package org.sap.commercemigration.strategy.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import org.apache.commons.collections.MapUtils;
import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.concurrent.DataWorkerExecutor;
import org.sap.commercemigration.concurrent.DataWorkerPoolFactory;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.concurrent.RetriableTask;
import org.sap.commercemigration.concurrent.impl.DefaultDataWorkerExecutor;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.performance.PerformanceUnit;
import org.sap.commercemigration.service.DatabaseCopyTask;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;
import org.sap.commercemigration.strategy.PipeWriterStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CopyPipeWriterStrategy implements PipeWriterStrategy<DataSet> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterStrategy.class);

	private final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;

	private final DatabaseCopyTaskRepository taskRepository;

	private final DataWorkerPoolFactory dataWriteWorkerPoolFactory;

	public CopyPipeWriterStrategy(DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService,
			DatabaseCopyTaskRepository taskRepository, DataWorkerPoolFactory dataWriteWorkerPoolFactory) {
		this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
		this.taskRepository = taskRepository;
		this.dataWriteWorkerPoolFactory = dataWriteWorkerPoolFactory;
	}

	@Override
	public void write(CopyContext context, DataPipe<DataSet> pipe, CopyContext.DataCopyItem item) throws Exception {
		String targetTableName = item.getTargetItem();
		PerformanceRecorder performanceRecorder = context.getPerformanceProfiler()
				.createRecorder(PerformanceCategory.DB_WRITE, targetTableName);
		performanceRecorder.start();
		Set<String> excludedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		if (context.getMigrationContext().getExcludedColumns().containsKey(targetTableName)) {
			excludedColumns.addAll(context.getMigrationContext().getExcludedColumns().get(targetTableName));
			LOG.info("Ignoring excluded column(s): {}", excludedColumns);
		}
		Set<String> nullifyColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		if (context.getMigrationContext().getNullifyColumns().containsKey(targetTableName)) {
			nullifyColumns.addAll(context.getMigrationContext().getNullifyColumns().get(targetTableName));
			LOG.info("Nullify column(s): {}", nullifyColumns);
		}

		List<String> columnsToCopy = new ArrayList<>();
		try (Connection sourceConnection = context.getMigrationContext().getDataSourceRepository().getConnection();
				Statement stmt = sourceConnection.createStatement();
				ResultSet metaResult = stmt
						.executeQuery(String.format("select * from %s where 0 = 1", item.getSourceItem()));) {
			ResultSetMetaData sourceMeta = metaResult.getMetaData();
			int columnCount = sourceMeta.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				String column = sourceMeta.getColumnName(i);
				if (!excludedColumns.contains(column)) {
					columnsToCopy.add(column);
				}
			}
		}

		if (columnsToCopy.isEmpty()) {
			throw new IllegalStateException(
					String.format("%s: source has no columns or all columns excluded", item.getPipelineName()));
		}
		ThreadPoolTaskExecutor taskExecutor = dataWriteWorkerPoolFactory.create(context);
		DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
		Connection targetConnection = null;
		AtomicLong totalCount = new AtomicLong(
				taskRepository.findPipeline(context, item).map(p -> p.getTargetrowcount()).orElseGet(() -> 0l));
		Optional<String> upsertId = Optional.empty();
		try {
			targetConnection = context.getMigrationContext().getDataTargetRepository().getConnection();
			boolean requiresIdentityInsert = requiresIdentityInsert(item.getTargetItem(), targetConnection);
			MaybeFinished<DataSet> sourcePage;
			boolean firstPage = true;
			do {
				sourcePage = pipe.get();
				if (sourcePage.isPoison()) {
					throw new IllegalStateException("Poison received; dying. Check the logs for further insights.");
				}
				DataSet dataSet = sourcePage.getValue();
				if (firstPage) {
					if (doTruncateIfNecessary(context, item)) {
						totalCount.set(0);
						updateProgress(context, item, totalCount.get());
					}
					doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), false);
					if (context.getMigrationContext().isIncrementalModeEnabled()) {
						upsertId = determineUpsertId(dataSet);
					}
					firstPage = false;
				}
				if (dataSet.isNotEmpty()) {
					DataWriterContext dataWriterContext = new DataWriterContext(context, item, dataSet, columnsToCopy,
							nullifyColumns, performanceRecorder, totalCount, upsertId, requiresIdentityInsert);
					RetriableTask writerTask = createWriterTask(dataWriterContext);
					workerExecutor.safelyExecute(writerTask);
				}
			} while (!sourcePage.isDone());
			workerExecutor.waitAndRethrowUncaughtExceptions();
		} catch (Exception e) {
			pipe.requestAbort(e);
			if (e instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw e;
		} finally {
			if (taskExecutor != null) {
				taskExecutor.shutdown();
			}
			if (targetConnection != null) {
				doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), true);
				targetConnection.close();
			}
			updateProgress(context, item, totalCount.get());
		}
	}

	private void switchIdentityInsert(Connection connection, final String tableName, boolean on) {
		try (Statement stmt = connection.createStatement()) {
			String onOff = on ? "ON" : "OFF";
			stmt.executeUpdate(String.format("SET IDENTITY_INSERT %s %s", tableName, onOff));
		} catch (final Exception e) {
			// TODO using brute force FIX
		}
	}

	protected void executeBatch(CopyContext.DataCopyItem item, PreparedStatement preparedStatement, long batchCount,
			PerformanceRecorder recorder) throws SQLException {
		final Stopwatch timer = Stopwatch.createStarted();
		preparedStatement.executeBatch();
		preparedStatement.clearBatch();
		LOG.debug("Batch written ({} items) for table '{}' in {}", batchCount, item.getTargetItem(),
				timer.stop().toString());
		recorder.record(PerformanceUnit.ROWS, batchCount);
	}

	private void updateProgress(CopyContext context, CopyContext.DataCopyItem item, long totalCount) {
		try {
			taskRepository.updateTaskProgress(context, item, totalCount);
		} catch (Exception e) {
			LOG.warn("Could not update progress", e);
		}
	}

	protected boolean doTruncateIfNecessary(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
		String targetTableName = item.getTargetItem();
		if (context.getMigrationContext().isSchedulerResumeEnabled()) {
			Optional<DatabaseCopyTask> pipeline = taskRepository.findPipeline(context, item);
			if (pipeline.isPresent()) {
				DatabaseCopyTask databaseCopyTask = pipeline.get();
				/*
				 * check if table was initially truncated. Could happen that batches are
				 * scheduled but migration was aborted before truncation.
				 */
				if (databaseCopyTask.isTruncated()) {
					return false;
				}
			}
		}
		if (context.getMigrationContext().isTruncateEnabled()) {
			if (!context.getMigrationContext().getTruncateExcludedTables().contains(targetTableName)) {
				assertTruncateAllowed(context, targetTableName);
				context.getMigrationContext().getDataTargetRepository().truncateTable(targetTableName);
				taskRepository.markTaskTruncated(context, item);
				return true;
			} else {
				taskRepository.markTaskTruncated(context, item);
			}
		}
		return false;
	}

	protected void doTurnOnOffIndicesIfNecessary(CopyContext context, String targetTableName, boolean on)
			throws Exception {
		if (context.getMigrationContext().isDropAllIndexesEnabled()) {
			if (!on) {
				LOG.debug("{} indexes for table '{}'", "Dropping", targetTableName);
				context.getMigrationContext().getDataTargetRepository().dropIndexesOfTable(targetTableName);
			}
		} else {
			if (context.getMigrationContext().isDisableAllIndexesEnabled()) {
				if (!context.getMigrationContext().getDisableAllIndexesIncludedTables().isEmpty()) {
					if (!context.getMigrationContext().getDisableAllIndexesIncludedTables().contains(targetTableName)) {
						return;
					}
				}
				LOG.debug("{} indexes for table '{}'", on ? "Rebuilding" : "Disabling", targetTableName);
				if (on) {
					context.getMigrationContext().getDataTargetRepository().enableIndexesOfTable(targetTableName);
				} else {
					context.getMigrationContext().getDataTargetRepository().disableIndexesOfTable(targetTableName);
				}
			}
		}
	}

	protected void assertTruncateAllowed(CopyContext context, String targetTableName) throws Exception {
		if (context.getMigrationContext().isIncrementalModeEnabled()) {
			throw new IllegalStateException("Truncating tables in incremental mode is illegal. Change the property "
					+ CommercemigrationConstants.MIGRATION_DATA_TRUNCATE_ENABLED + " to false");
		}
	}

	protected boolean isColumnOverride(CopyContext context, CopyContext.DataCopyItem item, String sourceColumnName) {
		return MapUtils.isNotEmpty(item.getColumnMap()) && item.getColumnMap().containsKey(sourceColumnName);
	}

	protected boolean isColumnOverride(CopyContext context, CopyContext.DataCopyItem item) {
		return MapUtils.isNotEmpty(item.getColumnMap());
	}

	private PreparedStatement createPreparedStatement(CopyContext context, String targetTableName,
			List<String> columnsToCopy, Optional<String> upsertId, Connection targetConnection) throws Exception {
		if (context.getMigrationContext().isIncrementalModeEnabled()) {
			if (upsertId.isPresent()) {
				return targetConnection
						.prepareStatement(getBulkUpsertStatement(targetTableName, columnsToCopy, upsertId.get()));
			} else {
				throw new RuntimeException(
						"The incremental approach can only be used on tables that have a valid identifier like PK or ID");
			}
		} else {
			return targetConnection.prepareStatement(getBulkInsertStatement(targetTableName, columnsToCopy,
					columnsToCopy.stream().map(column -> "?").collect(Collectors.toList())));
		}
	}

	private String getBulkInsertStatement(String targetTableName, List<String> columnsToCopy,
			List<String> columnsToCopyValues) {
		return "INSERT INTO " + targetTableName + " "
				+ getBulkInsertStatementParamList(columnsToCopy, columnsToCopyValues);
	}

	private String getBulkInsertStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
		return "(" + String.join(", ", columnsToCopy) + ") VALUES ("
				+ columnsToCopyValues.stream().collect(Collectors.joining(", ")) + ")";
	}

	private String getBulkUpdateStatementParamList(List<String> columnsToCopy, List<String> columnsToCopyValues) {
		return "SET " + IntStream.range(0, columnsToCopy.size())
				.mapToObj(idx -> String.format("%s = %s", columnsToCopy.get(idx), columnsToCopyValues.get(idx)))
				.collect(Collectors.joining(", "));
	}

	private Optional<String> determineUpsertId(DataSet dataSet) {
		if (dataSet.hasColumn("PK")) {
			return Optional.of("PK");
		} else if (dataSet.hasColumn("ID")) {
			return Optional.of("ID");
		} else {
			// should we support more IDs? In the hybris context there is hardly any other
			// with regards to transactional data.
			return Optional.empty();
		}
	}

	private String getBulkUpsertStatement(String targetTableName, List<String> columnsToCopy, String columnId) {
		/*
		 * https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-
		 * antipatterns/ We are not using a stored procedure here as CCv2 does not grant
		 * sp exec permission to the default db user
		 */
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append(String.format("MERGE %s WITH (HOLDLOCK) AS t", targetTableName));
		sqlBuilder.append("\n");
		sqlBuilder.append(String.format("USING (SELECT %s) AS s ON t.%s = s.%s",
				Joiner.on(',').join(columnsToCopy.stream().map(column -> "? " + column).collect(Collectors.toList())),
				columnId, columnId));
		sqlBuilder.append("\n");
		sqlBuilder.append("WHEN MATCHED THEN UPDATE"); // update
		sqlBuilder.append("\n");
		sqlBuilder.append(getBulkUpdateStatementParamList(columnsToCopy,
				columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
		sqlBuilder.append("\n");
		sqlBuilder.append("WHEN NOT MATCHED THEN INSERT"); // insert
		sqlBuilder.append("\n");
		sqlBuilder.append(getBulkInsertStatementParamList(columnsToCopy,
				columnsToCopy.stream().map(column -> "s." + column).collect(Collectors.toList())));
		sqlBuilder.append(";");
		return sqlBuilder.toString();
	}

	private boolean requiresIdentityInsert(String targetTableName, Connection targetConnection) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT \n");
		sqlBuilder.append("count(*)\n");
		sqlBuilder.append("FROM sys.columns\n");
		sqlBuilder.append("WHERE\n");
		sqlBuilder.append(String.format("object_id = object_id('%s')\n", targetTableName));
		sqlBuilder.append("AND\n");
		sqlBuilder.append("is_identity = 1\n");
		sqlBuilder.append(";\n");
		try (Statement statement = targetConnection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(sqlBuilder.toString());
			boolean requiresIdentityInsert = false;
			if (resultSet.next()) {
				requiresIdentityInsert = resultSet.getInt(1) > 0;
			}
			resultSet.close();
			return requiresIdentityInsert;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private RetriableTask createWriterTask(DataWriterContext dwc) {
		MigrationContext ctx = dwc.getContext().getMigrationContext();
		return new DataWriterTask(dwc);
	}

	private static class DataWriterContext {
		private CopyContext context;
		private CopyContext.DataCopyItem copyItem;
		private DataSet dataSet;
		private List<String> columnsToCopy;
		private Set<String> nullifyColumns;
		private PerformanceRecorder performanceRecorder;
		private AtomicLong totalCount;
		private Optional<String> upsertId;
		private boolean requiresIdentityInsert;

		public DataWriterContext(CopyContext context, CopyContext.DataCopyItem copyItem, DataSet dataSet,
				List<String> columnsToCopy, Set<String> nullifyColumns, PerformanceRecorder performanceRecorder,
				AtomicLong totalCount, Optional<String> upsertId, boolean requiresIdentityInsert) {
			this.context = context;
			this.copyItem = copyItem;
			this.dataSet = dataSet;
			this.columnsToCopy = columnsToCopy;
			this.nullifyColumns = nullifyColumns;
			this.performanceRecorder = performanceRecorder;
			this.totalCount = totalCount;
			this.upsertId = upsertId;
			this.requiresIdentityInsert = requiresIdentityInsert;
		}

		public CopyContext getContext() {
			return context;
		}

		public CopyContext.DataCopyItem getCopyItem() {
			return copyItem;
		}

		public DataSet getDataSet() {
			return dataSet;
		}

		public List<String> getColumnsToCopy() {
			return columnsToCopy;
		}

		public Set<String> getNullifyColumns() {
			return nullifyColumns;
		}

		public PerformanceRecorder getPerformanceRecorder() {
			return performanceRecorder;
		}

		public AtomicLong getTotalCount() {
			return totalCount;
		}

		public Optional<String> getUpsertId() {
			return upsertId;
		}

		public boolean isRequiresIdentityInsert() {
			return requiresIdentityInsert;
		}
	}

	private class DataWriterTask extends RetriableTask {

		private DataWriterContext ctx;

		public DataWriterTask(DataWriterContext ctx) {
			super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
			this.ctx = ctx;
		}

		@Override
		protected Boolean internalRun() {
			try {
				if (!ctx.getDataSet().getAllResults().isEmpty()) {
					process();
				}
				return Boolean.TRUE;
			} catch (Exception e) {
				// LOG.error("Error while executing table task " +
				// ctx.getCopyItem().getTargetItem(),e);
				throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
			}
		}

		private void process() throws Exception {
			Connection connection = null;
			Boolean originalAutoCommit = null;
			boolean requiresIdentityInsert = ctx.isRequiresIdentityInsert();
			try {
				connection = ctx.getContext().getMigrationContext().getDataTargetRepository().getConnection();
				originalAutoCommit = connection.getAutoCommit();
				try (PreparedStatement bulkWriterStatement = createPreparedStatement(ctx.getContext(),
						ctx.getCopyItem().getTargetItem(), ctx.getColumnsToCopy(), ctx.getUpsertId(), connection);
						Statement tempStmt = connection.createStatement();
						ResultSet tempTargetRs = tempStmt.executeQuery(
								String.format("select * from %s where 0 = 1", ctx.getCopyItem().getTargetItem()))) {
					connection.setAutoCommit(false);
					if (requiresIdentityInsert) {
						switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), true);
					}
					for (List<Object> row : ctx.getDataSet().getAllResults()) {
						int paramIdx = 1;
						for (String sourceColumnName : ctx.getColumnsToCopy()) {
							int targetColumnIdx = tempTargetRs.findColumn(sourceColumnName);
							int targetColumnType = tempTargetRs.getMetaData().getColumnType(targetColumnIdx);
							if (ctx.getNullifyColumns().contains(sourceColumnName)) {
								bulkWriterStatement.setNull(paramIdx, targetColumnType);
								LOG.trace("Column {} is nullified. Setting NULL value...", sourceColumnName);
							} else {
								if (isColumnOverride(ctx.getContext(), ctx.getCopyItem(), sourceColumnName)) {
									bulkWriterStatement.setObject(paramIdx,
											ctx.getCopyItem().getColumnMap().get(sourceColumnName), targetColumnType);
								} else {
									Object sourceColumnValue = ctx.getDataSet().getColumnValue(sourceColumnName, row);
									if (sourceColumnValue != null) {
										bulkWriterStatement.setObject(paramIdx, sourceColumnValue, targetColumnType);
									} else {
										bulkWriterStatement.setNull(paramIdx, targetColumnType);
									}
								}
							}
							paramIdx += 1;
						}
						bulkWriterStatement.addBatch();
					}
					int batchCount = ctx.getDataSet().getAllResults().size();
					executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
					bulkWriterStatement.clearParameters();
					bulkWriterStatement.clearBatch();
					taskRepository.markBatchCompleted(connection, ctx.getContext(), ctx.getCopyItem(),
							ctx.getDataSet().getBatchId());
					connection.commit();
					long totalCount = ctx.getTotalCount().addAndGet(batchCount);
					updateProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
				}
			} catch (Exception e) {
				if (connection != null) {
					connection.rollback();
				}
				throw e;
			} finally {
				if (connection != null && originalAutoCommit != null) {
					connection.setAutoCommit(originalAutoCommit);
				}
				if (connection != null && ctx != null) {
					if (requiresIdentityInsert) {
						switchIdentityInsert(connection, ctx.getCopyItem().getTargetItem(), false);
					}
					connection.close();
				}
			}
		}
	}

}
