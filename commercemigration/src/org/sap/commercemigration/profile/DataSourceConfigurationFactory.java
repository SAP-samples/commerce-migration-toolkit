/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.profile;

/**
 * Factory to create datasource configurations based on profiles
 */
public interface DataSourceConfigurationFactory {
	DataSourceConfiguration create(String profile);
}
