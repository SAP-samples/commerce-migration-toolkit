package org.sap.commercemigration.adapter.impl;

import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.repository.DataRepository;

import java.time.Instant;
import java.util.Set;

/**
 * Controls the way the repository is accessed by adapting the most common reading
 * operations based on the configured context
 */
public class ContextualDataRepositoryAdapter implements DataRepositoryAdapter {

    private DataRepository repository;

    public ContextualDataRepositoryAdapter(DataRepository repository) {
        this.repository = repository;
    }

    @Override
    public long getRowCount(MigrationContext context, String table) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getRowCountModifiedAfter(table, getIncrementalTimestamp(context));
        } else {
            return repository.getRowCount(table);
        }
    }

    @Override
    public DataSet getAll(MigrationContext context, String table) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getAllModifiedAfter(table, getIncrementalTimestamp(context));
        } else {
            return repository.getAll(table);
        }
    }

    @Override
    public DataSet getBatchWithoutIdentifier(MigrationContext context, String table, Set<String> allColumns, long batchSize, long offset) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchWithoutIdentifier(table, allColumns, batchSize, offset, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchWithoutIdentifier(table, allColumns, batchSize, offset);
        }
    }

    @Override
    public DataSet getBatchOrderedByColumn(MigrationContext context, String table, String column, Object lastColumnValue, long batchSize) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchOrderedByColumn(table, column, lastColumnValue, batchSize, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchOrderedByColumn(table, column, lastColumnValue, batchSize);
        }
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MigrationContext context, String table, String column, long batchSize) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchMarkersOrderedByColumn(table, column, batchSize, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchMarkersOrderedByColumn(table, column, batchSize);
        }
    }

    private Instant getIncrementalTimestamp(MigrationContext context) {
        Instant incrementalTimestamp = context.getIncrementalTimestamp();
        if (incrementalTimestamp == null) {
            throw new IllegalStateException("Timestamp cannot be null in incremental mode. Set a timestamp using the property " + CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        }
        return incrementalTimestamp;
    }
}
