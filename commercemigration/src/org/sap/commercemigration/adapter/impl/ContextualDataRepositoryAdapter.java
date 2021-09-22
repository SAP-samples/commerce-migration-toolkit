package org.sap.commercemigration.adapter.impl;

import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.repository.DataRepository;

import java.time.Instant;

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
    public DataSet getBatchWithoutIdentifier(MigrationContext context, OffsetQueryDefinition queryDefinition) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchWithoutIdentifier(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchWithoutIdentifier(queryDefinition);
        }
    }

    @Override
    public DataSet getBatchOrderedByColumn(MigrationContext context, SeekQueryDefinition queryDefinition) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchOrderedByColumn(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchOrderedByColumn(queryDefinition);
        }
    }

    @Override
    public DataSet getBatchMarkersOrderedByColumn(MigrationContext context, MarkersQueryDefinition queryDefinition) throws Exception {
        if (context.isIncrementalModeEnabled()) {
            return repository.getBatchMarkersOrderedByColumn(queryDefinition, getIncrementalTimestamp(context));
        } else {
            return repository.getBatchMarkersOrderedByColumn(queryDefinition);
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
