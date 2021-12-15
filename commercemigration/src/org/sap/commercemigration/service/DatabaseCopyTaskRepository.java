package org.sap.commercemigration.service;

import org.sap.commercemigration.MigrationProgress;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;

import java.sql.Connection;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Repository to manage Migration Status and Tasks
 */
public interface DatabaseCopyTaskRepository {

	/**
	 * Creates a new DB Migration status record
	 *
	 * @param context
	 * @throws Exception
	 */
	void createMigrationStatus(CopyContext context) throws Exception;

	/**
	 * Resets the values of the current migration to start it again
	 *
	 * @param context
	 * @throws Exception
	 */
	void resetMigration(CopyContext context) throws Exception;

	/**
	 * Updates the Migration status record
	 *
	 * @param context
	 * @param progress
	 * @throws Exception
	 */
	void setMigrationStatus(CopyContext context, MigrationProgress progress) throws Exception;

	/**
	 * Updates the Migration status record from one status to another
	 *
	 * @param context
	 * @param from
	 * @param to
	 * @throws Exception
	 */
	void setMigrationStatus(CopyContext context, MigrationProgress from, MigrationProgress to) throws Exception;

	/**
	 * Retrieves the current migration status
	 *
	 * @param context
	 * @return
	 * @throws Exception
	 */
	MigrationStatus getMigrationStatus(CopyContext context) throws Exception;

	/**
	 * Schedules a copy Task
	 *
	 * @param context
	 *            the migration context
	 * @param copyItem
	 *            the item to copy
	 * @param sourceRowCount
	 * @param targetNode
	 *            the nodeId to perform the copy
	 * @throws Exception
	 */
	void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long sourceRowCount, int targetNode)
			throws Exception;

	void rescheduleTask(CopyContext context, String pipelineName, int targetNodeId) throws Exception;

	void scheduleBatch(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId, Object lowerBoundary,
			Optional<Object> upperBoundary) throws Exception;

	void markBatchCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, int batchId) throws Exception;

	void markBatchCompleted(Connection connection, CopyContext context, CopyContext.DataCopyItem copyItem, int batchId)
			throws Exception;

	void resetPipelineBatches(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception;

	Set<DatabaseCopyBatch> findPendingBatchesForPipeline(CopyContext context, CopyContext.DataCopyItem item)
			throws Exception;

	Optional<DatabaseCopyTask> findPipeline(CopyContext context, CopyContext.DataCopyItem dataCopyItem)
			throws Exception;

	/**
	 * Retrieves all pending tasks
	 *
	 * @param context
	 * @return
	 * @throws Exception
	 */
	Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception;

	Set<DatabaseCopyTask> findFailedTasks(CopyContext context) throws Exception;

	/**
	 * Updates progress on a Task
	 *
	 * @param context
	 * @param copyItem
	 * @param itemCount
	 * @throws Exception
	 */
	void updateTaskProgress(CopyContext context, CopyContext.DataCopyItem copyItem, long itemCount) throws Exception;

	/**
	 * Marks the Task as Completed
	 *
	 * @param context
	 * @param copyItem
	 * @param duration
	 * @throws Exception
	 */
	void markTaskCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, String duration) throws Exception;

	void markTaskTruncated(CopyContext context, CopyContext.DataCopyItem copyItem) throws Exception;

	/**
	 * Marks the Task as Failed
	 *
	 * @param context
	 * @param copyItem
	 * @param error
	 * @throws Exception
	 */
	void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error) throws Exception;

	void updateTaskCopyMethod(CopyContext context, CopyContext.DataCopyItem copyItem, String copyMethod)
			throws Exception;

	void updateTaskKeyColumns(CopyContext context, CopyContext.DataCopyItem copyItem, Collection<String> keyColumns)
			throws Exception;

	/**
	 * Gets all updated Tasks
	 *
	 * @param context
	 * @param since
	 *            offset
	 * @return
	 * @throws Exception
	 */
	Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception;

	Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception;
}
