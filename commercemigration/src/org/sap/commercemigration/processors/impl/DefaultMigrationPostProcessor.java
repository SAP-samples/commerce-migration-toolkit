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
 * Implements the {@link MigrationPostProcessor}
 */
public class DefaultMigrationPostProcessor implements MigrationPostProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultMigrationPostProcessor.class.getName());

	@Override
	public void process(CopyContext context) {
		LOG.info("DefaultMigrationPostProcessor Finished");
	}
}
