/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.populator.impl;

import org.sap.commercemigration.context.MigrationContext;

import javax.sql.DataSource;

public class HikariTargetConnectionMetricPopulator extends HikariConnectionMetricPopulator {

	@Override
	protected String getMetricId(MigrationContext context) {
		return "hikari-target-pool";
	}

	@Override
	protected String getName(MigrationContext context) {
		return "Target DB Pool";
	}

	@Override
	protected DataSource getDataSource(MigrationContext context) {
		return context.getDataTargetRepository().getDataSource();
	}
}
