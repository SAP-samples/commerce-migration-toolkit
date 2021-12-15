package org.sap.commercemigration.service;

import org.sap.commercemigration.context.CopyContext;

/**
 * Actual Service to perform the Migration
 */
public interface DatabaseMigrationCopyService {

	void copyAllAsync(CopyContext context);

}
