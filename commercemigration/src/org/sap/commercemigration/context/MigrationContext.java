/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.context;

import org.sap.commercemigration.repository.DataRepository;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * The MigrationContext contains all information needed to perform a Source ->
 * Target Migration
 */
public interface MigrationContext {
	DataRepository getDataSourceRepository();

	DataRepository getDataTargetRepository();

	boolean isMigrationTriggeredByUpdateProcess();

	boolean isSchemaMigrationEnabled();

	boolean isAddMissingTablesToSchemaEnabled();

	boolean isRemoveMissingTablesToSchemaEnabled();

	boolean isAddMissingColumnsToSchemaEnabled();

	boolean isRemoveMissingColumnsToSchemaEnabled();

	boolean isSchemaMigrationAutoTriggerEnabled();

	int getReaderBatchSize();

	boolean isTruncateEnabled();

	boolean isAuditTableMigrationEnabled();

	Set<String> getTruncateExcludedTables();

	int getMaxParallelReaderWorkers();

	int getMaxParallelWriterWorkers();

	int getMaxParallelTableCopy();

	int getMaxWorkerRetryAttempts();

	boolean isFailOnErrorEnabled();

	Map<String, Set<String>> getExcludedColumns();

	Map<String, Set<String>> getNullifyColumns();

	Set<String> getCustomTables();

	Set<String> getExcludedTables();

	Set<String> getIncludedTables();

	boolean isDropAllIndexesEnabled();

	boolean isDisableAllIndexesEnabled();

	Set<String> getDisableAllIndexesIncludedTables();

	boolean isClusterMode();

	boolean isIncrementalModeEnabled();

	Set<String> getIncrementalTables();

	Instant getIncrementalTimestamp();

	int getDataPipeTimeout();

	int getDataPipeCapacity();

	int getStalledTimeout();

	String getMigrationReportConnectionString();

	int getMaxTargetStagedMigrations();

	boolean isSchedulerResumeEnabled();
}
