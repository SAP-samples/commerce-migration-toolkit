/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.profile.impl;

import org.sap.commercemigration.profile.DataSourceConfiguration;

public class InvalidDataSourceConfigurationException extends RuntimeException {
	public InvalidDataSourceConfigurationException(String message, DataSourceConfiguration dataSourceConfiguration) {
		super(message + ": " + String.valueOf(dataSourceConfiguration));
	}

}
