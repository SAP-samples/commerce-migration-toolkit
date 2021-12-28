/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.service;

import org.sap.commercemigration.context.CopyContext;

/**
 * Actual Service to perform the Migration
 */
public interface DatabaseMigrationCopyService {

	void copyAllAsync(CopyContext context);

}
