/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.datasource;

import org.sap.commercemigration.profile.DataSourceConfiguration;

import javax.sql.DataSource;

/**
 * Factory to create the DataSources used for Migration
 */
public interface MigrationDataSourceFactory {
	DataSource create(DataSourceConfiguration dataSourceConfiguration);
}
