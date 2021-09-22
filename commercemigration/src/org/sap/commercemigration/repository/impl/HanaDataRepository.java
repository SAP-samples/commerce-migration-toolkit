package org.sap.commercemigration.repository.impl;

import com.google.common.base.Joiner;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

public class HanaDataRepository extends AbstractDataRepository {

    public HanaDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
        return String.format("select * from %s where %s order by %s limit %s offset %s", queryDefinition.getTable(), expandConditions(conditions), orderBy, queryDefinition.getBatchSize(), queryDefinition.getOffset());
    }

    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
    }

    @Override
    protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
        String column = queryDefinition.getColumn();
        return String.format("SELECT t.%s, t.rownr as \"rownum\" \n" +
                "FROM\n" +
                "(\n" +
                "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownr\n" +
                "    FROM %s\n WHERE %s" +
                ") t\n" +
                "WHERE mod(t.rownr,%s) = 0\n" +
                "ORDER BY t.%s", column, column, column, queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getBatchSize(), column);
    }

    @Override
    protected String createAllTableNamesQuery() {
        return String.format("select distinct table_name from table_columns where lower(schema_name) = lower('%s') order by table_name", getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        return String.format("select distinct column_name from table_columns where lower(schema_name) = lower('%s') and lower(table_name) = lower('%s')", getDataSourceConfiguration().getSchema(), table);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        return String.format("SELECT t2.\"COLUMN_NAME\"\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT * FROM (\n" +
                "    SELECT i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\", count(*) as \"COL_COUNT\"\n" +
                "    FROM INDEXES i\n" +
                "    INNER JOIN INDEX_COLUMNS c\n" +
                "    ON i.\"INDEX_NAME\" = c.\"INDEX_NAME\" AND i.\"SCHEMA_NAME\" = c.\"SCHEMA_NAME\" AND i.\"TABLE_NAME\" = c.\"TABLE_NAME\"\n" +
                "    WHERE \n" +
                "    lower(i.\"SCHEMA_NAME\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"TABLE_NAME\") = lower('%s')\n" +
                "    AND(\n" +
                "    lower(i.\"CONSTRAINT\") = lower('UNIQUE') OR \n" +
                "    lower(i.\"CONSTRAINT\") = lower('PRIMARY KEY'))\n" +
                "    GROUP BY i.\"SCHEMA_NAME\", i.\"TABLE_NAME\", i.\"INDEX_NAME\"\n" +
                "    ORDER BY COL_COUNT ASC  \n" +
                "  )\n" +
                "  LIMIT 1\n" +
                ") t1\n" +
                "INNER JOIN INDEX_COLUMNS t2\n" +
                "ON t1.\"INDEX_NAME\" = t2.\"INDEX_NAME\" AND t1.\"SCHEMA_NAME\" = t2.\"SCHEMA_NAME\" AND t1.\"TABLE_NAME\" = t2.\"TABLE_NAME\"", getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.HANA;
    }
}
