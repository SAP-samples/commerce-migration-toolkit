/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.processors;

import org.sap.commercemigration.context.CopyContext;

/**
 * Postprocessor activated after a migration has terminated
 */
public interface MigrationPostProcessor {

	void process(CopyContext context);
}
