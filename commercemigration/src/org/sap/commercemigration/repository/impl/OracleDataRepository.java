package org.sap.commercemigration.repository.impl;

import com.google.common.base.Joiner;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.bootstrap.ddl.DatabaseSettings;
import de.hybris.bootstrap.ddl.HybrisOraclePlatform;
import de.hybris.bootstrap.ddl.HybrisPlatform;
import org.apache.ddlutils.Platform;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Collections;

public class OracleDataRepository extends AbstractDataRepository {
    public OracleDataRepository(DataSourceConfiguration dataSourceConfiguration, DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
        ensureJdbcCompliance();
    }

    private void ensureJdbcCompliance() {
        //without this types like timestamps may not be jdbc compliant
        System.getProperties().setProperty("oracle.jdbc.J2EE13Compliant", "true");
    }

    @Override
    protected DataSet convertToBatchDataSet(ResultSet resultSet) throws Exception {
        return convertToDataSet(resultSet, Collections.singleton("rn"));
    }

    @Override
    protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {
        String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
        return String.format(
                "select * " +
                        " from ( " +
                        " select /*+ first_rows(%s) */ " +
                        "  t.*, " +
                        "  row_number() " +
                        "  over (order by %s) rn " +
                        " from %s t where %s) " +
                        "where rn between %s and %s " +
                        "order by rn", queryDefinition.getBatchSize(), orderBy, queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getOffset() + 1, queryDefinition.getOffset() + queryDefinition.getBatchSize());
    }

    // https://blogs.oracle.com/oraclemagazine/on-top-n-and-pagination-queries
    // "Pagination in Getting Rows N Through M"
    @Override
    protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
        return String.format(
                "select * " +
                        " from ( " +
                        " select /*+ first_rows(%s) */ " +
                        "  t.*, " +
                        "  row_number() " +
                        "  over (order by t.%s) rn " +
                        " from %s t where %s) " +
                        "where rn <= %s " +
                        "order by rn", queryDefinition.getBatchSize(), queryDefinition.getColumn(), queryDefinition.getTable(), expandConditions(conditions), queryDefinition.getBatchSize());
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
        return String.format(
                "select distinct TABLE_NAME from ALL_TAB_COLUMNS where lower(OWNER) = lower('%s')",
                getDataSourceConfiguration().getSchema());
    }

    @Override
    protected String createAllColumnNamesQuery(String table) {
        return String.format(
                "select distinct COLUMN_NAME from ALL_TAB_COLUMNS where lower(OWNER) = lower('%s') AND lower(TABLE_NAME) = lower('%s')",
                getDataSourceConfiguration().getSchema(), table);
    }

    @Override
    protected String createUniqueColumnsQuery(String tableName) {
        return String.format("SELECT t2.\"COLUMN_NAME\"\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT * FROM (\n" +
                "    SELECT i.\"OWNER\", i.\"TABLE_NAME\", i.\"INDEX_NAME\", count(*) as \"COL_COUNT\"\n" +
                "    FROM ALL_INDEXES i\n" +
                "    INNER JOIN ALL_IND_COLUMNS c\n" +
                "    ON i.\"INDEX_NAME\" = c.\"INDEX_NAME\" AND i.\"OWNER\" = c.\"INDEX_OWNER\" AND i.\"TABLE_NAME\" = c.\"TABLE_NAME\"\n" +
                "    WHERE \n" +
                "    lower(i.\"OWNER\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"TABLE_NAME\") = lower('%s')\n" +
                "    AND\n" +
                "    lower(i.\"UNIQUENESS\") = lower('UNIQUE')\n" +
                "    GROUP BY i.\"OWNER\", i.\"TABLE_NAME\", i.\"INDEX_NAME\"\n" +
                "    ORDER BY COL_COUNT ASC  \n" +
                "  )\n" +
                "  WHERE ROWNUM = 1\n" +
                ") t1\n" +
                "INNER JOIN ALL_IND_COLUMNS t2\n" +
                "ON t1.\"INDEX_NAME\" = t2.\"INDEX_NAME\" AND t1.\"OWNER\" = t2.\"INDEX_OWNER\" AND t1.\"TABLE_NAME\" = t2.\"TABLE_NAME\"", getDataSourceConfiguration().getSchema(), tableName);
    }

    @Override
    protected Platform createPlatform(DatabaseSettings databaseSettings, DataSource dataSource) {
        HybrisPlatform platform = HybrisOraclePlatform.build(databaseSettings);
        platform.setDataSource(dataSource);
        return platform;
    }

    @Override
    public DataBaseProvider getDatabaseProvider() {
        return DataBaseProvider.ORACLE;
    }
}
