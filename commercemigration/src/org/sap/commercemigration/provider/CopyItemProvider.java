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
