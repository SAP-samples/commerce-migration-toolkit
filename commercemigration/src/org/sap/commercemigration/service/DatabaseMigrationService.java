package org.sap.commercemigration.service;

import org.sap.commercemigration.MigrationReport;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.MigrationContext;

import java.time.OffsetDateTime;

public interface DatabaseMigrationService {

	/**
	 * Asynchronously start a new database migration
	 *
	 * @param context
	 *            Migration configuration
	 * @return migrationID of the started migration
	 * @throws Exception
	 *             if anything goes wrong during start
	 */
	String startMigration(MigrationContext context) throws Exception;

	/**
	 * Asynchronously resumes the migrations and continuous with unfinished items.
	 *
	 * @param context
	 *            Migration configuration
	 * @param migrationID
	 *            The id of the migration to resume
	 * @throws Exception
	 *             if anything goes wrong during start
	 */
	void resumeUnfinishedMigration(MigrationContext context, String migrationID) throws Exception;

	/**
	 * Stops the the database migration process. The process is stopped on all
	 * nodes, in case clustering is used.
	 *
	 * @param context
	 *            Migration configuration
	 * @param migrationID
	 *            ID of the migration process that should be stopped
	 * @throws Exception
	 *             if anything goes wrong
	 */
	void stopMigration(MigrationContext context, String migrationID) throws Exception;

	/**
	 * Get current overall state without details
	 *
	 * @param context
	 * @param migrationID
	 * @return
	 * @throws Exception
	 */
	MigrationStatus getMigrationState(MigrationContext context, String migrationID) throws Exception;

	/**
	 * Get current state with details per copy task
	 *
	 * @param context
	 * @param migrationID
	 * @param since
	 *            Get all updates since this timestamp. Must be in UTC!
	 * @return
	 * @throws Exception
	 */
	MigrationStatus getMigrationState(MigrationContext context, String migrationID, OffsetDateTime since)
			throws Exception;

	MigrationReport getMigrationReport(MigrationContext context, String migrationID) throws Exception;

	/**
	 * Busy wait until migration is done. Use only for tests!
	 *
	 * @param context
	 * @param migrationID
	 * @return
	 * @throws Exception
	 *             when migration was not successful
	 */
	MigrationStatus waitForFinish(MigrationContext context, String migrationID) throws Exception;
}
