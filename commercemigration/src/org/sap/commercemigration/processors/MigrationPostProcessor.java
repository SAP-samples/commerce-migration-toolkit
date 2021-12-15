package org.sap.commercemigration.processors;

import org.sap.commercemigration.context.CopyContext;

/**
 * Postprocessor activated after a migration has terminated
 */
public interface MigrationPostProcessor {

	void process(CopyContext context);
}
