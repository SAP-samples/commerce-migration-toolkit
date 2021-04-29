package org.sap.commercemigration.repository.impl;

import com.google.common.base.Joiner;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

import java.util.Set;

public class MySQLDataRepository extends AbstractDataRepository {
    public MySQLDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(String table, Set<String> columns, long batchSize, long offset, String... conditions) {
        String orderBy = Joiner.on(',').join(columns);
        return String.format("select * from %s where %s order by %s limit %s,%s", table, expandConditions(conditions), orderBy, offset, batchSize);
    }

    @Override
    protected String buildValueBatchQuery(String table, String column, long batchSize, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s", table, expandConditions(conditions), column, batchSize);
    }

    @Override
    protected String buildBatchMarkersQuery(String table, String column, long batchSize, String... conditions) {
        return String.format("SELECT %s,rownum\n" +
                "FROM ( \n" +
                "    SELECT \n" +
                "        @row := @row +1 AS rownum, %s \n" +
                "    FROM (SELECT @row :=-1) r, %s  WHERE %s ORDER BY %s) ranked \n" +
                "WHERE rownum %% %s = 0 ", column, column, table, expandConditions(conditions), column, batchSize);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format(
                "select TABLE_NAME from information_schema.tables where table_schema = '%s' and TABLE_TYPE = 'BASE TABLE'",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String tableName) {
        return String.format(
                "SELECT DISTINCT COLUMN_NAME from information_schema.columns where table_schema = '%s' AND TABLE_NAME = '%s'",
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        return String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS t1\n" +
                        "INNER JOIN \n" +
                        "(\n" +
                        "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, count(INDEX_NAME) as COL_COUNT \n" +
                        "FROM INFORMATION_SCHEMA.STATISTICS \n" +
                        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND NON_UNIQUE = 0\n" +
                        "GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME\n" +
                        "ORDER BY COL_COUNT ASC\n" +
                        "LIMIT 1\n" +
                        ") t2\n" +
                        "ON t1.TABLE_SCHEMA = t2.TABLE_SCHEMA AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.INDEX_NAME = t2.INDEX_NAME\n" +
                        ";\n",
                getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.MYSQL;
    }
}
