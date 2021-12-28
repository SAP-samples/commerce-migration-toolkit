/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.provider;

import org.sap.commercemigration.TableCandidate;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;

import java.util.Set;

/**
 * Provides the means to copy an Item fro Source to Target
 */
public interface CopyItemProvider {
	Set<CopyContext.DataCopyItem> get(MigrationContext context) throws Exception;

	Set<TableCandidate> getSourceTableCandidates(MigrationContext context) throws Exception;

	Set<TableCandidate> getTargetTableCandidates(MigrationContext context) throws Exception;
}
