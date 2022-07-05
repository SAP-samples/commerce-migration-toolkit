/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.profile.impl;

import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.profile.DataSourceConfigurationFactory;

public class DefaultDataSourceConfigurationFactory implements DataSourceConfigurationFactory {

	private final ConfigurationService configurationService;

	public DefaultDataSourceConfigurationFactory(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}

	@Override
	public DataSourceConfiguration create(String profile) {
		return new DefaultDataSourceConfiguration(configurationService.getConfiguration(), profile);
	}
}
