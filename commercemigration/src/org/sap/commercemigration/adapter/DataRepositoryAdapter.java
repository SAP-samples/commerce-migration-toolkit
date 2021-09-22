package org.sap.commercemigration.adapter;

import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;

public interface DataRepositoryAdapter {
    long getRowCount(MigrationContext context, String table) throws Exception;

    DataSet getAll(MigrationContext context, String table) throws Exception;

    DataSet getBatchWithoutIdentifier(MigrationContext context, OffsetQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchOrderedByColumn(MigrationContext context, SeekQueryDefinition queryDefinition) throws Exception;

    DataSet getBatchMarkersOrderedByColumn(MigrationContext context, MarkersQueryDefinition queryDefinition) throws Exception;

}
