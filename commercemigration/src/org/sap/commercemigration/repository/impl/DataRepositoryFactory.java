/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.repository.impl;

import com.google.common.base.Strings;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.profile.DataSourceConfigurationFactory;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataRepositoryFactory {

	private static final Logger LOG = LoggerFactory.getLogger(DataRepositoryFactory.class);

	private final DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService;

	public DataRepositoryFactory(DataSourceConfigurationFactory dataSourceConfigurationFactory,
			DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
		this.databaseMigrationDataTypeMapperService = databaseMigrationDataTypeMapperService;
	}

	public DataRepository create(MigrationContext migrationContext,
			Set<DataSourceConfiguration> dataSourceConfigurations) throws Exception {
		Objects.requireNonNull(dataSourceConfigurations);
		if (dataSourceConfigurations.isEmpty()) {
			return new NullRepository("no datasource specified", null);
		}
		Set<DataRepository> repositories = new HashSet<>();
		for (DataSourceConfiguration dataSourceConfiguration : dataSourceConfigurations) {
			try {
				repositories.add(doCreate(dataSourceConfiguration, migrationContext));
			} catch (Exception e) {
				LOG.error("Error creating data repository", e);
				repositories.add(new NullRepository(e.getMessage(), dataSourceConfiguration));
			}
		}
		if (repositories.size() > 1) {
			// TODO implement a CompositeRepository to handle multiple inputs/outputs
			return new NullRepository("multiple data source profiles as input/output is currently not supported", null);
		} else {
			Optional<DataRepository> repositoryOptional = repositories.stream().findFirst();
			if (repositoryOptional.isPresent()) {
				return repositoryOptional.get();
			} else {
				throw new NoSuchElementException("The element being requested does not exist.");
			}
		}
	}

	protected DataRepository doCreate(DataSourceConfiguration dataSourceConfiguration,
			MigrationContext migrationContext) throws Exception {
		String connectionString = dataSourceConfiguration.getConnectionString();
		if (Strings.isNullOrEmpty(connectionString)) {
			throw new RuntimeException(
					"No connection string provided for data source '" + dataSourceConfiguration.getProfile() + "'");
		} else {
			String connectionStringLower = connectionString.toLowerCase();
			if (connectionStringLower.startsWith("jdbc:mysql")) {
				return new MySQLDataRepository(migrationContext, dataSourceConfiguration,
						databaseMigrationDataTypeMapperService);
			} else if (connectionStringLower.startsWith("jdbc:sqlserver")) {
				return new AzureDataRepository(migrationContext, dataSourceConfiguration,
						databaseMigrationDataTypeMapperService);
			} else if (connectionStringLower.startsWith("jdbc:oracle")) {
				return new OracleDataRepository(migrationContext, dataSourceConfiguration,
						databaseMigrationDataTypeMapperService);
			} else if (connectionStringLower.startsWith("jdbc:sap")) {
				return new HanaDataRepository(migrationContext, dataSourceConfiguration,
						databaseMigrationDataTypeMapperService);
			} else if (connectionStringLower.startsWith("jdbc:hsqldb")) {
				return new HsqlRepository(migrationContext, dataSourceConfiguration,
						databaseMigrationDataTypeMapperService);
			}
		}
		throw new RuntimeException("Cannot handle connection string for " + connectionString);
	}
}
