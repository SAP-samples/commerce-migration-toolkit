/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.populator.impl;

import org.sap.commercemigration.context.MigrationContext;

import javax.sql.DataSource;

public class HikariSourceConnectionMetricPopulator extends HikariConnectionMetricPopulator {

	@Override
	protected String getMetricId(MigrationContext context) {
		return "hikari-source-pool";
	}

	@Override
	protected String getName(MigrationContext context) {
		return "Source DB Pool";
	}

	@Override
	protected DataSource getDataSource(MigrationContext context) {
		return context.getDataSourceRepository().getDataSource();
	}
}
