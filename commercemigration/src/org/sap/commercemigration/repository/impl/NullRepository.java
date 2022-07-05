/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.repository.impl;

import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.TypeSystemTable;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.logging.JDBCQueriesStore;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.profile.impl.InvalidDataSourceConfigurationException;
import org.sap.commercemigration.repository.DataRepository;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;

/**
 * Represents a dummy implementation of a data repository that always throws an
 * exception
 */
public class NullRepository implements DataRepository {

	private final String message;
	private final DataSourceConfiguration dataSourceConfiguration;

	public NullRepository(String message, DataSourceConfiguration dataSourceConfiguration) {
		this.message = message;
		this.dataSourceConfiguration = dataSourceConfiguration;
	}

	@Override
	public Database asDatabase() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Database asDatabase(boolean reload) {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Set<String> getAllTableNames() throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Set<TypeSystemTable> getAllTypeSystemTables() throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public boolean isAuditTable(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Set<String> getAllColumnNames(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public long getRowCount(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public long getRowCountModifiedAfter(String table, Instant time) throws SQLException {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getAll(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getAllModifiedAfter(String table, Instant time) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSourceConfiguration getDataSourceConfiguration() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public int executeUpdateAndCommit(String updateStatement) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public void runSqlScript(Resource resource) {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public float getDatabaseUtilization() throws SQLException {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public int truncateTable(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public void disableIndexesOfTable(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public void enableIndexesOfTable(String table) throws SQLException {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public void dropIndexesOfTable(String table) throws SQLException {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Platform asPlatform() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Platform asPlatform(boolean reload) {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataBaseProvider getDatabaseProvider() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public Connection getConnection() throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSource getDataSource() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time)
			throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public DataSet getUniqueColumns(String table) throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public boolean validateConnection() throws Exception {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public JDBCQueriesStore getJdbcQueriesStore() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

	@Override
	public void clearJdbcQueriesStore() {
		throw new InvalidDataSourceConfigurationException(this.message, this.dataSourceConfiguration);
	}

}
