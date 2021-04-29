package org.sap.commercemigration.service;

import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.service.impl.DefaultDatabaseSchemaDifferenceService;

/**
 * Calculates and applies Schema Differences between two Databases
 */
public interface DatabaseSchemaDifferenceService {

    String generateSchemaDifferencesSql(MigrationContext context) throws Exception;

    void executeSchemaDifferencesSql(MigrationContext context, String sql) throws Exception;

    void executeSchemaDifferences(MigrationContext context) throws Exception;

    /**
     * Calculates the differences between two schemas
     *
     * @param migrationContext
     * @return
     */
    DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult getDifference(MigrationContext migrationContext) throws Exception;
}
