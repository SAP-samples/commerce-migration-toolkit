package org.sap.commercemigration.adapter;

import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;

import java.util.Set;

public interface DataRepositoryAdapter {
    long getRowCount(MigrationContext context, String table) throws Exception;

    DataSet getAll(MigrationContext context, String table) throws Exception;

    DataSet getBatchWithoutIdentifier(MigrationContext context, String table, Set<String> allColumns, long batchSize, long offset) throws Exception;

    DataSet getBatchOrderedByColumn(MigrationContext context, String table, String column, Object lastColumnValue, long batchSize) throws Exception;

    DataSet getBatchMarkersOrderedByColumn(MigrationContext context, String table, String column, long batchSize) throws Exception;

}
