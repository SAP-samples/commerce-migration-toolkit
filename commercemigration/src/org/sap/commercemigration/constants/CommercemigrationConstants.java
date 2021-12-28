/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.constants;

/**
 * Global class for all Commercemigration constants. You can add global
 * constants for your extension into this class.
 */
public final class CommercemigrationConstants extends GeneratedCommercemigrationConstants {
	public static final String EXTENSIONNAME = "commercemigration";
	public static final String PROPERTIES_PREFIX = "migration";
	public static final String MIGRATION_TRIGGER_UPDATESYSTEM = "migration.trigger.updatesystem";
	public static final String MIGRATION_SCHEMA_ENABLED = "migration.schema.enabled";
	public static final String MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED = "migration.schema.target.tables.add.enabled";
	public static final String MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED = "migration.schema.target.tables.remove.enabled";
	public static final String MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED = "migration.schema.target.columns.add.enabled";
	public static final String MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED = "migration.schema.target.columns.remove.enabled";
	public static final String MIGRATION_TARGET_MAX_STAGE_MIGRATIONS = "migration.ds.target.db.max.stage.migrations";
	public static final String MIGRATION_SCHEMA_AUTOTRIGGER_ENABLED = "migration.schema.autotrigger.enabled";
	public static final String MIGRATION_DATA_READER_BATCHSIZE = "migration.data.reader.batchsize";
	public static final String MIGRATION_DATA_TRUNCATE_ENABLED = "migration.data.truncate.enabled";
	public static final String MIGRATION_DATA_TRUNCATE_EXCLUDED = "migration.data.truncate.excluded";
	public static final String MIGRATION_DATA_WORKERS_READER_MAXTASKS = "migration.data.workers.reader.maxtasks";
	public static final String MIGRATION_DATA_WORKERS_WRITER_MAXTASKS = "migration.data.workers.writer.maxtasks";
	public static final String MIGRATION_DATA_WORKERS_RETRYATTEMPTS = "migration.data.workers.retryattempts";
	public static final String MIGRATION_DATA_MAXPRALLELTABLECOPY = "migration.data.maxparalleltablecopy";
	public static final String MIGRATION_DATA_FAILONEERROR_ENABLED = "migration.data.failonerror.enabled";
	public static final String MIGRATION_DATA_COLUMNS_EXCLUDED = "migration.data.columns.excluded";
	public static final String MIGRATION_DATA_COLUMNS_NULLIFY = "migration.data.columns.nullify";
	public static final String MIGRATION_DATA_INDICES_DROP_ENABLED = "migration.data.indices.drop.enabled";
	public static final String MIGRATION_DATA_INDICES_DISABLE_ENABLED = "migration.data.indices.disable.enabled";
	public static final String MIGRATION_DATA_INDICES_DISABLE_INCLUDED = "migration.data.indices.disable.included";
	public static final String MIGRATION_DATA_TABLES_AUDIT_ENABLED = "migration.data.tables.audit.enabled";
	public static final String MIGRATION_DATA_TABLES_CUSTOM = "migration.data.tables.custom";
	public static final String MIGRATION_DATA_TABLES_EXCLUDED = "migration.data.tables.excluded";
	public static final String MIGRATION_DATA_TABLES_INCLUDED = "migration.data.tables.included";
	public static final String MIGRATION_CLUSTER_ENABLED = "migration.cluster.enabled";
	public static final String MIGRATION_DATA_INCREMENTAL_ENABLED = "migration.data.incremental.enabled";
	public static final String MIGRATION_DATA_INCREMENTAL_TABLES = "migration.data.incremental.tables";
	public static final String MIGRATION_DATA_INCREMENTAL_TIMESTAMP = "migration.data.incremental.timestamp";
	public static final String MIGRATION_DATA_PIPE_TIMEOUT = "migration.data.pipe.timeout";
	public static final String MIGRATION_DATA_PIPE_CAPACITY = "migration.data.pipe.capacity";
	public static final String MIGRATION_STALLED_TIMEOUT = "migration.stalled.timeout";
	public static final String MIGRATION_DATA_REPORT_CONNECTIONSTRING = "migration.data.report.connectionstring";
	public static final String MIGRATION_DATATYPE_CHECK = "migration.datatype.check";
	public static final String MIGRATION_TABLESPREFIX = "MIGRATIONTOOLKIT_";
	public static final String MIGRATION_SCHEDULER_RESUME_ENABLED = "migration.scheduler.resume.enabled";

	public static final String MDC_MIGRATIONID = "migrationID";
	public static final String MDC_PIPELINE = "pipeline";
	public static final String MDC_CLUSTERID = "clusterID";

	public static final String DEPLOYMENTS_TABLE = "ydeployments";

	// Masking
	public static final String MIGRATION_REPORT_MASKED_PROPERTIES = "migration.properties.masked";
	public static final String MASKED_VALUE = "***";

	// Locale
	public static final String MIGRATION_LOCALE_DEFAULT = "migration.locale.default";

	private CommercemigrationConstants() {
		// empty to avoid instantiating this constant class
	}

}
