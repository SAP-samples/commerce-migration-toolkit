/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.profile;

/**
 * Contains a DataSource Configuration
 */
public interface DataSourceConfiguration {
	String getProfile();

	String getDriver();

	String getConnectionString();

	String getUserName();

	String getPassword();

	String getSchema();

	String getTypeSystemName();

	String getTypeSystemSuffix();

	String getCatalog();

	String getTablePrefix();

	int getMaxActive();

	int getMaxIdle();

	int getMinIdle();

	boolean isRemoveAbandoned();
}
