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
