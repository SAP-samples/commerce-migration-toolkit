package org.sap.commercemigration.repository.impl;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

import java.util.Set;

public class HsqlRepository extends AbstractDataRepository {

    public HsqlRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(String table, Set<String> columns, long batchSize, long offset, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildValueBatchQuery(String table, String column, long batchSize, String... conditions) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    protected String buildBatchMarkersQuery(String table, String column, long batchSize, String... conditions) {
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
