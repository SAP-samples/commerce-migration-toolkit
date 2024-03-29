/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.service.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import org.apache.commons.lang3.StringUtils;
import org.sap.commercemigration.MigrationProgress;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.performance.PerformanceUnit;
import org.sap.commercemigration.service.DatabaseCopyBatch;
import org.sap.commercemigration.service.DatabaseCopyTask;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

/**
 * Repository to manage the status on of the migration copy tasks across the
 * cluster
 */
public class DefaultDatabaseCopyTaskRepository implements DatabaseCopyTaskRepository {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseCopyTaskRepository.class);

	private ClusterService clusterService;

	@Override
	public void createMigrationStatus(CopyContext context) throws Exception {
		String insert = "INSERT INTO MIGRATIONTOOLKIT_TABLECOPYSTATUS (migrationId, total) VALUES (?, ?)";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
			stmt.setObject(1, context.getMigrationId());
			stmt.setObject(2, context.getCopyItems().size());
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public void resetMigration(CopyContext context) throws Exception {
		String update = "UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET completed = total - failed, status = ?, failed=?, lastUpdate=? WHERE migrationId = ?";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
			stmt.setObject(1, MigrationProgress.RUNNING.name());
			stmt.setObject(2, 0);
			stmt.setObject(3, now());
			stmt.setObject(4, context.getMigrationId());
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public void setMigrationStatus(CopyContext context, MigrationProgress progress) throws Exception {
		setMigrationStatus(context, MigrationProgress.RUNNING, progress);
	}

	@Override
	public void setMigrationStatus(CopyContext context, MigrationProgress from, MigrationProgress to) throws Exception {

		String update = "UPDATE MIGRATIONTOOLKIT_TABLECOPYSTATUS SET status = ? WHERE status = ? AND migrationId = ?";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(update)) {
			stmt.setObject(1, to.name());
			stmt.setObject(2, from.name());
			stmt.setObject(3, context.getMigrationId());
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public MigrationStatus getMigrationStatus(CopyContext context) throws Exception {
		String query = "SELECT * FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS WHERE migrationId = ?";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(query)) {
			stmt.setObject(1, context.getMigrationId());
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return convertToStatus(rs);
			}
		}
	}

	/**
	 * @param rs
	 *            result set to covert
	 * @return the equivalent Migration Status
	 * @throws Exception
	 */
	public MigrationStatus convertToStatus(ResultSet rs) throws Exception {
		MigrationStatus status = new MigrationStatus();
		status.setMigrationID(rs.getString("migrationId"));
		status.setStart(getDateTime(rs, "startAt"));
		status.setEnd(getDateTime(rs, "endAt"));
		status.setLastUpdate(getDateTime(rs, "lastUpdate"));
		status.setTotalTasks(rs.getInt("total"));
		status.setCompletedTasks(rs.getInt("completed"));
		status.setFailedTasks(rs.getInt("failed"));
		status.setStatus(MigrationProgress.valueOf(rs.getString("status")));

		status.setCompleted(status.getTotalTasks() == status.getCompletedTasks()
				|| MigrationProgress.STALLED == status.getStatus());
		status.setFailed(status.getFailedTasks() > 0 || MigrationProgress.STALLED == status.getStatus());
		status.setAborted(MigrationProgress.ABORTED == status.getStatus());
		status.setStatusUpdates(Collections.emptyList());

		return status;
	}

	private LocalDateTime getDateTime(ResultSet rs, String column) throws Exception {
		Timestamp ts = rs.getObject(column, Timestamp.class);
		return ts == null ? null : ts.toLocalDateTime();
	}

	@Override
	public void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long sourceRowCount,
			int targetNode) throws Exception {
		String insert = "INSERT INTO MIGRATIONTOOLKIT_TABLECOPYTASKS (targetnodeid, pipelinename, sourcetablename, targettablename, columnmap, migrationid, sourcerowcount, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
			stmt.setObject(1, targetNode);
			stmt.setObject(2, copyItem.getPipelineName());
			stmt.setObject(3, copyItem.getSourceItem());
			stmt.setObject(4, copyItem.getTargetItem());
			stmt.setObject(5, new Gson().toJson(copyItem.getColumnMap()));
			stmt.setObject(6, context.getMigrationId());
			stmt.setObject(7, sourceRowCount);
			setTimestamp(stmt, 8, now());
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public void rescheduleTask(CopyContext context, String pipelineName, int targetNode) throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET failure='0', duration=NULL, error='', "
				+ "targetnodeid=?, " + "lastupdate=? " + "WHERE migrationId=? " + "AND pipelinename=? ";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, targetNode);
			setTimestamp(stmt, 2, now());
			stmt.setObject(3, context.getMigrationId());
			stmt.setObject(4, pipelineName);
			stmt.executeUpdate();
			connection.commit();
		}
	}

	@Override
	public void scheduleBatch(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId, Object lowerBoundary,
			Object upperBoundary) throws Exception {
		LOG.debug("Schedule Batch for {} with ID {}", copyItem.getPipelineName(), batchId);
		String insert = "INSERT INTO MIGRATIONTOOLKIT_TABLECOPYBATCHES (migrationId, batchId, pipelinename, lowerBoundary, upperBoundary) VALUES (?, ?, ?, ?, ?)";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
			stmt.setObject(1, context.getMigrationId());
			stmt.setObject(2, batchId);
			stmt.setObject(3, copyItem.getPipelineName());
			stmt.setObject(4, lowerBoundary);
			stmt.setObject(5, upperBoundary);
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public void markBatchCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId)
			throws Exception {
		try (Connection conn = getConnection(context)) {
			markBatchCompleted(conn, context, copyItem, batchId);
			conn.commit();
		}
	}

	@Override
	public void markBatchCompleted(Connection connection, CopyContext context, CopyContext.DataCopyItem copyItem,
			int batchId) throws Exception {
		LOG.debug("Mark batch completed for {} with ID {}", copyItem.getPipelineName(), batchId);
		String insert = "DELETE FROM MIGRATIONTOOLKIT_TABLECOPYBATCHES WHERE migrationId=? AND batchId=? AND pipelinename=?";
		try (PreparedStatement stmt = connection.prepareStatement(insert)) {
			stmt.setObject(1, context.getMigrationId());
			stmt.setObject(2, batchId);
			stmt.setObject(3, copyItem.getPipelineName());
			// exactly one batch record should be affected
			if (stmt.executeUpdate() != 1) {
				throw new IllegalStateException("No (exact) match for batch with id '" + batchId + "' found.");
			}
		}
	}

	@Override
	public void resetPipelineBatches(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception {
		String insert = "DELETE FROM MIGRATIONTOOLKIT_TABLECOPYBATCHES WHERE migrationId=? AND pipelinename=?";
		try (Connection conn = getConnection(context); PreparedStatement stmt = conn.prepareStatement(insert)) {
			stmt.setObject(1, context.getMigrationId());
			stmt.setObject(2, copyItem.getPipelineName());
			stmt.executeUpdate();
			conn.commit();
		}
	}

	@Override
	public Set<DatabaseCopyBatch> findPendingBatchesForPipeline(CopyContext context, CopyContext.DataCopyItem item)
			throws Exception {
		String sql = "SELECT * from MIGRATIONTOOLKIT_TABLECOPYBATCHES WHERE migrationid=? AND pipelinename=? ORDER BY batchId ASC";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, context.getMigrationId());
			stmt.setObject(2, item.getPipelineName());
			try (ResultSet resultSet = stmt.executeQuery()) {
				return convertToBatch(resultSet);
			}
		}
	}

	private Timestamp now() {
		Instant now = java.time.Instant.now();
		Timestamp ts = new Timestamp(now.toEpochMilli());
		return ts;
	}

	private Connection getConnection(CopyContext context) throws Exception {
		return context.getMigrationContext().getDataTargetRepository().getConnection();
	}

	@Override
	public Optional<DatabaseCopyTask> findPipeline(CopyContext context, CopyContext.DataCopyItem dataCopyItem)
			throws Exception {
		String sql = "SELECT * from MIGRATIONTOOLKIT_TABLECOPYTASKS WHERE targetnodeid=? AND migrationid=? AND pipelinename=?";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, getTargetNodeId());
			stmt.setObject(2, context.getMigrationId());
			stmt.setObject(3, dataCopyItem.getPipelineName());
			try (ResultSet resultSet = stmt.executeQuery()) {
				Set<DatabaseCopyTask> databaseCopyTasks = convertToTask(resultSet);
				if (databaseCopyTasks.size() > 1) {
					throw new IllegalStateException(
							"Invalid scheduler table, cannot have same pipeline multiple times.");
				}
				return databaseCopyTasks.stream().findFirst();
			}
		}
	}

	@Override
	public Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception {
		String sql = "SELECT * from MIGRATIONTOOLKIT_TABLECOPYTASKS WHERE targetnodeid=? AND migrationid=? AND duration IS NULL ORDER BY sourcerowcount";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, getTargetNodeId());
			stmt.setObject(2, context.getMigrationId());
			try (ResultSet resultSet = stmt.executeQuery()) {
				return convertToTask(resultSet);
			}
		}
	}

	@Override
	public Set<DatabaseCopyTask> findFailedTasks(CopyContext context) throws Exception {
		String sql = "SELECT * from MIGRATIONTOOLKIT_TABLECOPYTASKS WHERE targetnodeid=? AND migrationid=? AND (duration = '-1' AND failure = '1') ORDER BY sourcerowcount";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, getTargetNodeId());
			stmt.setObject(2, context.getMigrationId());
			try (ResultSet resultSet = stmt.executeQuery()) {
				return convertToTask(resultSet);
			}
		}
	}

	@Override
	public void updateTaskProgress(CopyContext context, CopyContext.DataCopyItem copyItem, long itemCount)
			throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET targetrowcount=?, " + "lastupdate=?, "
				+ "avgwriterrowthroughput=?, " + "avgreaderrowthroughput=? " + "WHERE targetnodeid=? "
				+ "AND migrationid=? " + "AND pipelinename=?";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, itemCount);
			setTimestamp(stmt, 2, now());
			stmt.setObject(3, getAvgPerformanceValue(context, PerformanceCategory.DB_WRITE, copyItem.getTargetItem()));
			stmt.setObject(4, getAvgPerformanceValue(context, PerformanceCategory.DB_READ, copyItem.getSourceItem()));
			stmt.setObject(5, getTargetNodeId());
			stmt.setObject(6, context.getMigrationId());
			stmt.setObject(7, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
	}

	protected void setTimestamp(PreparedStatement stmt, int i, Timestamp ts) throws SQLException {
		stmt.setTimestamp(i, ts, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
	}

	@Override
	public void markTaskCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, String duration)
			throws Exception {
		Objects.requireNonNull(duration, "duration must not be null");
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET duration=?, " + "lastupdate=?, "
				+ "avgwriterrowthroughput=?, " + "avgreaderrowthroughput=? " + "WHERE targetnodeid=? "
				+ "AND migrationid=? " + "AND pipelinename=? " + "AND duration IS NULL";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, duration);
			setTimestamp(stmt, 2, now());
			stmt.setObject(3, getAvgPerformanceValue(context, PerformanceCategory.DB_WRITE, copyItem.getTargetItem()));
			stmt.setObject(4, getAvgPerformanceValue(context, PerformanceCategory.DB_READ, copyItem.getSourceItem()));
			stmt.setObject(5, getTargetNodeId());
			stmt.setObject(6, context.getMigrationId());
			stmt.setObject(7, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
		mutePerformanceRecorder(context, copyItem);
	}

	@Override
	public void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error)
			throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET failure='1', duration='-1', " + "error=?, "
				+ "lastupdate=? " + "WHERE targetnodeid=? " + "AND migrationId=? " + "AND pipelinename=? "
				+ "AND failure = '0'";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			String errorMsg = error.getMessage();
			if (StringUtils.isBlank(errorMsg)) {
				errorMsg = error.getClass().getName();
			}
			stmt.setObject(1, errorMsg.trim());
			setTimestamp(stmt, 2, now());
			stmt.setObject(3, getTargetNodeId());
			stmt.setObject(4, context.getMigrationId());
			stmt.setObject(5, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
		mutePerformanceRecorder(context, copyItem);
	}

	@Override
	public void markTaskTruncated(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET truncated = '1' " + "WHERE targetnodeid=? "
				+ "AND migrationId=? " + "AND pipelinename=? ";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, getTargetNodeId());
			stmt.setObject(2, context.getMigrationId());
			stmt.setObject(3, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
	}

	@Override
	public void updateTaskCopyMethod(CopyContext context, CopyContext.DataCopyItem copyItem, String copyMethod)
			throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET copymethod=? " + "WHERE targetnodeid=? "
				+ "AND migrationId=? " + "AND pipelinename=? ";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, copyMethod);
			stmt.setObject(2, getTargetNodeId());
			stmt.setObject(3, context.getMigrationId());
			stmt.setObject(4, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
	}

	@Override
	public void updateTaskKeyColumns(CopyContext context, CopyContext.DataCopyItem copyItem,
			Collection<String> keyColumns) throws Exception {
		String sql = "UPDATE MIGRATIONTOOLKIT_TABLECOPYTASKS " + "SET keycolumns=? " + "WHERE targetnodeid=? "
				+ "AND migrationId=? " + "AND pipelinename=? ";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setObject(1, Joiner.on(',').join(keyColumns));
			stmt.setObject(2, getTargetNodeId());
			stmt.setObject(3, context.getMigrationId());
			stmt.setObject(4, copyItem.getPipelineName());
			stmt.executeUpdate();
			connection.commit();
		}
	}

	@Override
	public Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception {
		String sql = "select * from MIGRATIONTOOLKIT_TABLECOPYTASKS WHERE migrationid=? AND lastupdate >= ?";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setObject(1, context.getMigrationId());
			setTimestamp(stmt, 2, toTimestamp(since));
			try (ResultSet resultSet = stmt.executeQuery()) {
				return convertToTask(resultSet);
			}
		}
	}

	private Timestamp toTimestamp(OffsetDateTime ts) {
		return new Timestamp(ts.toInstant().toEpochMilli());
	}

	@Override
	public Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception {
		String sql = "select * from MIGRATIONTOOLKIT_TABLECOPYTASKS WHERE migrationid=?";
		try (Connection connection = getConnection(context);
				PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setObject(1, context.getMigrationId());
			try (ResultSet resultSet = stmt.executeQuery()) {
				return convertToTask(resultSet);
			}
		}
	}

	private int getTargetNodeId() {
		return clusterService.getClusterId();
	}

	public void setClusterService(ClusterService clusterService) {
		this.clusterService = clusterService;
	}

	private Set<DatabaseCopyTask> convertToTask(ResultSet rs) throws Exception {
		Set<DatabaseCopyTask> copyTasks = new HashSet<>();
		while (rs.next()) {
			DatabaseCopyTask copyTask = new DatabaseCopyTask();
			copyTask.setTargetnodeId(rs.getInt("targetnodeId"));
			copyTask.setMigrationId(rs.getString("migrationId"));
			copyTask.setPipelinename(rs.getString("pipelinename"));
			copyTask.setSourcetablename(rs.getString("sourcetablename"));
			copyTask.setTargettablename(rs.getString("targettablename"));
			copyTask.setColumnmap(new Gson().fromJson(rs.getString("columnmap"), new TypeToken<Map<String, String>>() {
			}.getType()));
			copyTask.setDuration(rs.getString("duration"));
			copyTask.setCompleted(copyTask.getDuration() != null);
			copyTask.setSourcerowcount(rs.getLong("sourcerowcount"));
			copyTask.setTargetrowcount(rs.getLong("targetrowcount"));
			copyTask.setFailure(rs.getBoolean("failure"));
			copyTask.setError(rs.getString("error"));
			copyTask.setTruncated(rs.getBoolean("truncated"));
			copyTask.setLastUpdate(getDateTime(rs, "lastupdate"));
			copyTask.setAvgReaderRowThroughput(rs.getDouble("avgreaderrowthroughput"));
			copyTask.setAvgWriterRowThroughput(rs.getDouble("avgwriterrowthroughput"));
			copyTask.setCopyMethod(rs.getString("copymethod"));
			copyTask.setKeyColumns(Splitter.on(",")
					.splitToList(StringUtils.defaultIfEmpty(rs.getString("keycolumns"), StringUtils.EMPTY)));
			copyTasks.add(copyTask);
		}
		return copyTasks;
	}

	private Set<DatabaseCopyBatch> convertToBatch(ResultSet rs) throws Exception {
		Set<DatabaseCopyBatch> copyBatches = new LinkedHashSet<>();
		while (rs.next()) {
			DatabaseCopyBatch copyBatch = new DatabaseCopyBatch();
			copyBatch.setMigrationId(rs.getString("migrationId"));
			copyBatch.setBatchId(rs.getString("batchId"));
			copyBatch.setPipelinename(rs.getString("pipelinename"));
			copyBatch.setLowerBoundary(rs.getString("lowerBoundary"));
			copyBatch.setUpperBoundary(rs.getString("upperBoundary"));
			copyBatches.add(copyBatch);
		}
		return copyBatches;
	}

	private double getAvgPerformanceValue(CopyContext context, PerformanceCategory category, String tableName) {
		PerformanceRecorder recorder = context.getPerformanceProfiler().getRecorder(category, tableName);
		if (recorder != null) {
			PerformanceRecorder.PerformanceAggregation performanceAggregation = recorder.getRecords()
					.get(PerformanceUnit.ROWS);
			if (performanceAggregation != null) {
				return performanceAggregation.getAvgThroughput().get();
			}
		}
		return 0;
	}

	private void mutePerformanceRecorder(CopyContext context, CopyContext.DataCopyItem copyItem) {
		context.getPerformanceProfiler().muteRecorder(PerformanceCategory.DB_READ, copyItem.getSourceItem());
		context.getPerformanceProfiler().muteRecorder(PerformanceCategory.DB_WRITE, copyItem.getTargetItem());
	}

}
