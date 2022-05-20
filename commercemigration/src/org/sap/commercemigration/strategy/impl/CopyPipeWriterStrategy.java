/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.strategy.impl;

import org.sap.commercemigration.DataThreadPoolConfig;
import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.concurrent.DataThreadPoolConfigBuilder;
import org.sap.commercemigration.concurrent.DataWorkerExecutor;
import org.sap.commercemigration.concurrent.DataThreadPoolFactory;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.concurrent.impl.task.RetriableTask;
import org.sap.commercemigration.concurrent.impl.DefaultDataWorkerExecutor;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.service.DatabaseCopyTask;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.sap.commercemigration.strategy.PipeWriterStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class CopyPipeWriterStrategy implements PipeWriterStrategy<DataSet> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterStrategy.class);

	private final DatabaseCopyTaskRepository taskRepository;

	private final DataThreadPoolFactory dataWriteWorkerPoolFactory;

	public CopyPipeWriterStrategy(DatabaseCopyTaskRepository taskRepository,
			DataThreadPoolFactory dataWriteWorkerPoolFactory) {
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
						.executeQuery(String.format("select * from %s where 0 = 1", item.getSourceItem()))) {
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
		DataThreadPoolConfig threadPoolConfig = new DataThreadPoolConfigBuilder(context.getMigrationContext())
				.withPoolSize(context.getMigrationContext().getMaxParallelWriterWorkers()).build();
		ThreadPoolTaskExecutor taskExecutor = dataWriteWorkerPoolFactory.create(context, threadPoolConfig);
		DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
		Connection targetConnection = null;
		AtomicLong totalCount = new AtomicLong(
				taskRepository.findPipeline(context, item).map(p -> p.getTargetrowcount()).orElseGet(() -> 0l));
		String upsertId = null;
		try {
			targetConnection = context.getMigrationContext().getDataTargetRepository().getConnection();
			boolean requiresIdentityInsert = requiresIdentityInsert(item.getTargetItem(), targetConnection);
			MaybeFinished<DataSet> sourcePage;
			boolean firstPage = true;
			CopyPipeWriterContext copyPipeWriterContext = null;
			do {
				sourcePage = pipe.get();
				if (sourcePage.isPoison()) {
					throw new IllegalStateException("Poison received; dying. Check the logs for further insights.");
				}
				DataSet dataSet = sourcePage.getValue();
				if (firstPage) {
					if (doTruncateIfNecessary(context, item)) {
						totalCount.set(0);
						taskRepository.updateTaskProgress(context, item, totalCount.get());
					}
					doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), false);
					if (context.getMigrationContext().isIncrementalModeEnabled()) {
						upsertId = determineUpsertId(dataSet).orElse(null);
					}
					copyPipeWriterContext = new CopyPipeWriterContext(context, item, columnsToCopy, nullifyColumns,
							performanceRecorder, totalCount, upsertId, requiresIdentityInsert, taskRepository);
					firstPage = false;
				}
				if (dataSet.isNotEmpty()) {
					RetriableTask writerTask = createWriterTask(copyPipeWriterContext, dataSet);
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
				dataWriteWorkerPoolFactory.destroy(taskExecutor);
			}
			if (targetConnection != null) {
				doTurnOnOffIndicesIfNecessary(context, item.getTargetItem(), true);
				targetConnection.close();
			}
			if (taskRepository != null) {
				taskRepository.updateTaskProgress(context, item, totalCount.get());
			}
		}
	}

	private boolean doTruncateIfNecessary(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
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
				assertTruncateAllowed(context);
				context.getMigrationContext().getDataTargetRepository().truncateTable(targetTableName);
				taskRepository.markTaskTruncated(context, item);
				return true;
			} else {
				taskRepository.markTaskTruncated(context, item);
			}
		}
		return false;
	}

	private void doTurnOnOffIndicesIfNecessary(CopyContext context, String targetTableName, boolean on)
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

	private void assertTruncateAllowed(CopyContext context) {
		if (context.getMigrationContext().isIncrementalModeEnabled()) {
			throw new IllegalStateException("Truncating tables in incremental mode is illegal. Change the property "
					+ CommercemigrationConstants.MIGRATION_DATA_TRUNCATE_ENABLED + " to false");
		}
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

	private boolean requiresIdentityInsert(String targetTableName, Connection targetConnection) {
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("SELECT \n");
		sqlBuilder.append("count(*)\n");
		sqlBuilder.append("FROM sys.columns\n");
		sqlBuilder.append("WHERE\n");
		sqlBuilder.append(String.format("object_id = object_id('%s')%n", targetTableName));
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

	private RetriableTask createWriterTask(CopyPipeWriterContext dwc, DataSet dataSet) {
		return new CopyPipeWriterTask(dwc, dataSet);
	}

}
