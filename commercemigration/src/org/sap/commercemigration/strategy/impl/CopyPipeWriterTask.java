/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.strategy.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.concurrent.impl.task.RetriableTask;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.performance.PerformanceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CopyPipeWriterTask extends RetriableTask {

	private static final Logger LOG = LoggerFactory.getLogger(CopyPipeWriterTask.class);

	private CopyPipeWriterContext ctx;
	private DataSet dataSet;

	public CopyPipeWriterTask(CopyPipeWriterContext ctx, DataSet dataSet) {
		super(ctx.getContext(), ctx.getCopyItem().getTargetItem());
		this.ctx = ctx;
		this.dataSet = dataSet;
	}

	@Override
	protected Boolean internalRun() {
		try {
			if (!dataSet.getAllResults().isEmpty()) {
				process();
			}
			return Boolean.TRUE;
		} catch (Exception e) {
			throw new RuntimeException("Error processing writer task for " + ctx.getCopyItem().getTargetItem(), e);
		}
	}

	private boolean isColumnOverride(CopyContext.DataCopyItem item, String sourceColumnName) {
		return MapUtils.isNotEmpty(item.getColumnMap()) && item.getColumnMap().containsKey(sourceColumnName);
	}

	private void switchIdentityInsert(Connection connection, final String tableName, boolean on) {
		try (Statement stmt = connection.createStatement()) {
			String onOff = on ? "ON" : "OFF";
			stmt.executeUpdate(String.format("SET IDENTITY_INSERT %s %s", tableName, onOff));
		} catch (final Exception e) {
			throw new RuntimeException("Could not switch identity insert", e);
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

	private PreparedStatement createPreparedStatement(CopyContext context, String targetTableName,
			List<String> columnsToCopy, String upsertId, Connection targetConnection) throws Exception {
		if (context.getMigrationContext().isIncrementalModeEnabled()) {
			if (StringUtils.isNotEmpty(upsertId)) {
				return targetConnection
						.prepareStatement(getBulkUpsertStatement(targetTableName, columnsToCopy, upsertId));
			} else {
				throw new RuntimeException(
						"The incremental approach can only be used on tables that have a valid identifier like PK or ID");
			}
		} else {
			return targetConnection.prepareStatement(getBulkInsertStatement(targetTableName, columnsToCopy,
					columnsToCopy.stream().map(column -> "?").collect(Collectors.toList())));
		}
	}

	private void executeBatch(CopyContext.DataCopyItem item, PreparedStatement preparedStatement, long batchCount,
			PerformanceRecorder recorder) throws SQLException {
		final Stopwatch timer = Stopwatch.createStarted();
		preparedStatement.executeBatch();
		preparedStatement.clearBatch();
		LOG.debug("Batch written ({} items) for table '{}' in {}", batchCount, item.getTargetItem(), timer.stop());
		recorder.record(PerformanceUnit.ROWS, batchCount);
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
				for (List<Object> row : dataSet.getAllResults()) {
					int paramIdx = 1;
					for (String sourceColumnName : ctx.getColumnsToCopy()) {
						int targetColumnIdx = tempTargetRs.findColumn(sourceColumnName);
						int targetColumnType = tempTargetRs.getMetaData().getColumnType(targetColumnIdx);
						if (ctx.getNullifyColumns().contains(sourceColumnName)) {
							bulkWriterStatement.setNull(paramIdx, targetColumnType);
							LOG.trace("Column {} is nullified. Setting NULL value...", sourceColumnName);
						} else {
							if (isColumnOverride(ctx.getCopyItem(), sourceColumnName)) {
								bulkWriterStatement.setObject(paramIdx,
										ctx.getCopyItem().getColumnMap().get(sourceColumnName), targetColumnType);
							} else {
								Object sourceColumnValue = dataSet.getColumnValue(sourceColumnName, row);
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
				int batchCount = dataSet.getAllResults().size();
				executeBatch(ctx.getCopyItem(), bulkWriterStatement, batchCount, ctx.getPerformanceRecorder());
				bulkWriterStatement.clearParameters();
				bulkWriterStatement.clearBatch();
				ctx.getDatabaseCopyTaskRepository().markBatchCompleted(connection, ctx.getContext(), ctx.getCopyItem(),
						dataSet.getBatchId());
				connection.commit();
				long totalCount = ctx.getTotalCount().addAndGet(batchCount);
				ctx.getDatabaseCopyTaskRepository().updateTaskProgress(ctx.getContext(), ctx.getCopyItem(), totalCount);
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
