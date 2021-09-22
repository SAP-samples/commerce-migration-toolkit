package org.sap.commercemigration.repository.impl;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

public class HsqlRepository extends AbstractDataRepository {

    public HsqlRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String createAllTableNamesQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.HSQL;
    }
}
