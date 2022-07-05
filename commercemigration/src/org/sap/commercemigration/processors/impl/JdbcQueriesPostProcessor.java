/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.processors.impl;

import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.processors.MigrationPostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post-processor producing and storing reports on the JDBC queries that were
 * executed during a migration against the source and target data repositories.
 */
public class JdbcQueriesPostProcessor implements MigrationPostProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcQueriesPostProcessor.class.getName());

	@Override
	public void process(CopyContext context) {
		if (!context.getMigrationContext().isLogSql()) {
			return;
		}
		try {
			context.getMigrationContext().getDataSourceRepository().getJdbcQueriesStore()
					.writeToLogFileAndCompress(context.getMigrationId());
			context.getMigrationContext().getDataTargetRepository().getJdbcQueriesStore()
					.writeToLogFileAndCompress(context.getMigrationId());
			LOG.info("Finished writing jdbc entries report");
		} catch (Exception e) {
			LOG.error("Error executing post processor", e);
		}
	}

}
