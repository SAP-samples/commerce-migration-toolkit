/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.service.impl;

import de.hybris.platform.task.TaskEngine;
import de.hybris.platform.task.TaskService;
import org.sap.commercemigration.MigrationReport;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.LaunchOptions;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.context.validation.MigrationContextValidator;
import org.sap.commercemigration.performance.PerformanceProfiler;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.scheduler.DatabaseCopyScheduler;
import org.sap.commercemigration.service.DatabaseMigrationReportService;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.slf4j.MDC;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;

import static org.sap.commercemigration.constants.CommercemigrationConstants.MDC_MIGRATIONID;

public class DefaultDatabaseMigrationService implements DatabaseMigrationService {

	private DatabaseCopyScheduler databaseCopyScheduler;
	private CopyItemProvider copyItemProvider;
	private PerformanceProfiler performanceProfiler;
	private DatabaseMigrationReportService databaseMigrationReportService;
	private DatabaseSchemaDifferenceService schemaDifferenceService;
	private MigrationContextValidator migrationContextValidator;
	private TaskService taskService;

	@Override
	public String startMigration(final MigrationContext context, LaunchOptions launchOptions) throws Exception {
		migrationContextValidator.validateContext(context);

		MigrationStatus migrationStatus = new MigrationStatus();
		String query = "SELECT * FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS WHERE status = 'RUNNING'";
		try (Connection conn = context.getDataTargetRepository().getConnection();
				PreparedStatement stmt = conn.prepareStatement(query)) {
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				migrationStatus = new DefaultDatabaseCopyTaskRepository().convertToStatus(rs);
			}
		} catch (Exception e) {
			e.getMessage();
		}
		if (migrationStatus.getStatus() != null && migrationStatus.getStatus().name().equals("RUNNING")) {
			return migrationStatus.getMigrationID();
		}

		TaskEngine engine = taskService.getEngine();
		boolean running = engine.isRunning();

		if (running) {

			throw new Exception("Task engine is activated - migration is blocked");
		}
		performanceProfiler.reset();
		if (context.isLogSql()) {
			context.getDataSourceRepository().clearJdbcQueriesStore();
			context.getDataTargetRepository().clearJdbcQueriesStore();
		}

		final String migrationId = UUID.randomUUID().toString();

		MDC.put(MDC_MIGRATIONID, migrationId);

		if (context.isSchemaMigrationEnabled() && context.isSchemaMigrationAutoTriggerEnabled()) {
			schemaDifferenceService.executeSchemaDifferences(context);
		}

		CopyContext copyContext = buildCopyContext(context, migrationId);
		copyContext.getPropertyOverrideMap().putAll(launchOptions.getPropertyOverrideMap());
		databaseCopyScheduler.schedule(copyContext);

		return migrationId;
	}

	@Override
	public void resumeUnfinishedMigration(MigrationContext context, LaunchOptions launchOptions, String migrationID)
			throws Exception {
		CopyContext copyContext = buildIdContext(context, migrationID);
		copyContext.getPropertyOverrideMap().putAll(launchOptions.getPropertyOverrideMap());
		databaseCopyScheduler.resumeUnfinishedItems(copyContext);
	}

	@Override
	public void stopMigration(MigrationContext context, String migrationID) throws Exception {
		CopyContext copyContext = buildIdContext(context, migrationID);
		databaseCopyScheduler.abort(copyContext);
	}

	private CopyContext buildCopyContext(MigrationContext context, String migrationID) throws Exception {
		Set<CopyContext.DataCopyItem> dataCopyItems = copyItemProvider.get(context);
		return new CopyContext(migrationID, context, dataCopyItems, performanceProfiler);
	}

	private CopyContext buildIdContext(MigrationContext context, String migrationID) throws Exception {
		// we use a lean implementation of the copy context to avoid calling the
		// provider which is not required for task management.
		return new CopyContext.IdCopyContext(migrationID, context, performanceProfiler);
	}

	@Override
	public MigrationStatus getMigrationState(MigrationContext context, String migrationID) throws Exception {
		return getMigrationState(context, migrationID, OffsetDateTime.MAX);
	}

	@Override
	public MigrationStatus getMigrationState(MigrationContext context, String migrationID, OffsetDateTime since)
			throws Exception {
		CopyContext copyContext = buildIdContext(context, migrationID);
		return databaseCopyScheduler.getCurrentState(copyContext, since);
	}

	@Override
	public MigrationReport getMigrationReport(MigrationContext context, String migrationID) throws Exception {
		CopyContext copyContext = buildIdContext(context, migrationID);
		return databaseMigrationReportService.getMigrationReport(copyContext);
	}

	@Override
	public String getMigrationID(MigrationContext context) {
		String query = "SELECT * FROM MIGRATIONTOOLKIT_TABLECOPYSTATUS";
		try (Connection conn = context.getDataTargetRepository().getConnection();
				PreparedStatement stmt = conn.prepareStatement(query)) {
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getString("migrationId");
			}
		} catch (Exception e) {
			e.getMessage();
		}
		return null;
	}

	@Override
	public MigrationStatus waitForFinish(MigrationContext context, String migrationID) throws Exception {
		MigrationStatus status;
		do {
			status = getMigrationState(context, migrationID);
			Thread.sleep(5000);
		} while (!status.isCompleted());

		if (status.isFailed()) {
			throw new Exception("Database migration failed");
		}

		return status;
	}

	public void setDatabaseCopyScheduler(DatabaseCopyScheduler databaseCopyScheduler) {
		this.databaseCopyScheduler = databaseCopyScheduler;
	}

	public void setCopyItemProvider(CopyItemProvider copyItemProvider) {
		this.copyItemProvider = copyItemProvider;
	}

	public void setPerformanceProfiler(PerformanceProfiler performanceProfiler) {
		this.performanceProfiler = performanceProfiler;
	}

	public void setDatabaseMigrationReportService(DatabaseMigrationReportService databaseMigrationReportService) {
		this.databaseMigrationReportService = databaseMigrationReportService;
	}

	public void setSchemaDifferenceService(DatabaseSchemaDifferenceService schemaDifferenceService) {
		this.schemaDifferenceService = schemaDifferenceService;
	}

	public void setMigrationContextValidator(MigrationContextValidator migrationContextValidator) {
		this.migrationContextValidator = migrationContextValidator;
	}

	public void setTaskService(TaskService taskService) {
		this.taskService = taskService;
	}
}
