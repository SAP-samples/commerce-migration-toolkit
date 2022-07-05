/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.DataThreadPoolConfig;
import org.sap.commercemigration.context.MigrationContext;

public class DataThreadPoolConfigBuilder {

	private DataThreadPoolConfig config;

	public DataThreadPoolConfigBuilder(MigrationContext context) {
		config = new DataThreadPoolConfig();
	}

	public DataThreadPoolConfigBuilder withPoolSize(int poolSize) {
		config.setPoolSize(poolSize);
		return this;
	}

	public DataThreadPoolConfig build() {
		return config;
	}
}
