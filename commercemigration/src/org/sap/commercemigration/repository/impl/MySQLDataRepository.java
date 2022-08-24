/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.repository.impl;

import de.hybris.bootstrap.ddl.DataBaseProvider;

import java.util.Map;

import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.ValidationException;

public class MySQLDataRepository extends AbstractDataRepository {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLDataRepository.class);

	public MySQLDataRepository(final MigrationContext migrationContext,
			final DataSourceConfiguration dataSourceConfiguration,
			final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
		super(migrationContext, dataSourceConfiguration, databaseMigrationDataTypeMapperService);
	}

	@Override
	protected String buildOffsetBatchQuery(final OffsetQueryDefinition queryDefinition, final String... conditions) {
		final String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
		return String.format("select * from %s where %s order by %s limit %s,%s", queryDefinition.getTable(),
				expandConditions(conditions), orderBy, queryDefinition.getOffset(), queryDefinition.getBatchSize());
	}

	@Override
	protected String buildValueBatchQuery(final SeekQueryDefinition queryDefinition, final String... conditions) {
		return String.format("select * from %s where %s order by %s limit %s", queryDefinition.getTable(),
				expandConditions(conditions), queryDefinition.getColumn(), queryDefinition.getBatchSize());
	}

	@Override
	protected String buildBatchMarkersQuery(final MarkersQueryDefinition queryDefinition, final String... conditions) {
		final String column = queryDefinition.getColumn();
		return String.format(
				"SELECT %s,rownum\n" + "FROM ( \n" + "    SELECT \n" + "        @row := @row +1 AS rownum, %s \n"
						+ "    FROM (SELECT @row :=-1) r, %s  WHERE %s ORDER BY %s) ranked \n"
						+ "WHERE rownum %% %s = 0 ",
				column, column, queryDefinition.getTable(), expandConditions(conditions), column,
				queryDefinition.getBatchSize());
	}

	@Override
	protected String createAllTableNamesQuery() {
		return String.format(
				"select TABLE_NAME from information_schema.tables where table_schema = '%s' and TABLE_TYPE = 'BASE TABLE'",
				getDataSourceConfiguration().getSchema());
	}

	@Override
	protected String createAllColumnNamesQuery(final String tableName) {
		return String.format(
				"SELECT DISTINCT COLUMN_NAME from information_schema.columns where table_schema = '%s' AND TABLE_NAME = '%s'",
				getDataSourceConfiguration().getSchema(), tableName);
	}

	@Override
	protected String createUniqueColumnsQuery(final String tableName) {
		return String.format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS t1\n" + "INNER JOIN \n" + "(\n"
				+ "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, count(INDEX_NAME) as COL_COUNT \n"
				+ "FROM INFORMATION_SCHEMA.STATISTICS \n"
				+ "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND NON_UNIQUE = 0\n"
				+ "GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME\n" + "ORDER BY COL_COUNT ASC\n" + "LIMIT 1\n"
				+ ") t2\n"
				+ "ON t1.TABLE_SCHEMA = t2.TABLE_SCHEMA AND t1.TABLE_NAME = t2.TABLE_NAME AND t1.INDEX_NAME = t2.INDEX_NAME\n"
				+ ";\n", getDataSourceConfiguration().getSchema(), tableName);
	}

	@Override
	public DataBaseProvider getDatabaseProvider() {
		return DataBaseProvider.MYSQL;
	}

	@Override
	public boolean validateConnection() throws Exception {
		final Map<String, String> locationMap = getLocationMap();

		if (!locationMap.containsKey("useConfigs")) {
			LOG.info("Parameter useConfigs is missing");
			throw new ValidationException("Parameter useConfigs is missing");
		}

		return super.validateConnection();
	}

}
