package org.sap.commercemigration.concurrent.impl;

import org.apache.commons.lang3.tuple.Pair;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.concurrent.DataPipeFactory;
import org.sap.commercemigration.concurrent.DataWorkerExecutor;
import org.sap.commercemigration.concurrent.DataWorkerPoolFactory;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.concurrent.RetriableTask;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.performance.PerformanceUnit;
import org.sap.commercemigration.scheduler.DatabaseCopyScheduler;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultDataPipeFactory implements DataPipeFactory<DataSet> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataPipeFactory.class);

    private final DatabaseCopyTaskRepository taskRepository;
    private final DatabaseCopyScheduler scheduler;
    private final AsyncTaskExecutor executor;
    private final DataWorkerPoolFactory dataReadWorkerPoolFactory;

    public DefaultDataPipeFactory(DatabaseCopyScheduler scheduler, DatabaseCopyTaskRepository taskRepository, AsyncTaskExecutor executor, DataWorkerPoolFactory dataReadWorkerPoolFactory) {
        this.scheduler = scheduler;
        this.taskRepository = taskRepository;
        this.executor = executor;
        this.dataReadWorkerPoolFactory = dataReadWorkerPoolFactory;
    }

    @Override
    public DataPipe<DataSet> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
        int dataPipeTimeout = context.getMigrationContext().getDataPipeTimeout();
        int dataPipeCapacity = context.getMigrationContext().getDataPipeCapacity();
        DataPipe<DataSet> pipe = new DefaultDataPipe<>(scheduler, taskRepository, context, item, dataPipeTimeout, dataPipeCapacity);
        ThreadPoolTaskExecutor taskExecutor = dataReadWorkerPoolFactory.create(context);
        DataWorkerExecutor<Boolean> workerExecutor = new DefaultDataWorkerExecutor<>(taskExecutor);
        try {
            executor.submit(() -> {
                try {
                    scheduleWorkers(context, workerExecutor, pipe, item);
                    workerExecutor.waitAndRethrowUncaughtExceptions();
                    pipe.put(MaybeFinished.finished(DataSet.EMPTY));
                } catch (Exception e) {
                    LOG.error("Error scheduling worker tasks ", e);
                    try {
                        pipe.put(MaybeFinished.poison());
                    } catch (Exception p) {
                        LOG.error("Cannot contaminate pipe ", p);
                    }
                } finally {
                    if (taskExecutor != null) {
                        taskExecutor.shutdown();
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Error invoking reader tasks ", e);
        }
        return pipe;
    }

    private void scheduleWorkers(CopyContext context, DataWorkerExecutor<Boolean> workerExecutor, DataPipe<DataSet> pipe, CopyContext.DataCopyItem copyItem) throws Exception {
        DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(context.getMigrationContext().getDataSourceRepository());
        String table = copyItem.getSourceItem();
        long totalRows = copyItem.getRowCount();
        long pageSize = context.getMigrationContext().getReaderBatchSize();
        try {
            PerformanceRecorder recorder = context.getPerformanceProfiler().createRecorder(PerformanceCategory.DB_READ, table);
            recorder.start();

            PipeTaskContext pipeTaskContext = new PipeTaskContext(context, pipe, table, dataRepositoryAdapter, pageSize, recorder);

            String batchColumn = "";
            // help.sap.com/viewer/d0224eca81e249cb821f2cdf45a82ace/LATEST/en-US/08a27931a21441b59094c8a6aa2a880e.html
            if (context.getMigrationContext().getDataSourceRepository().isAuditTable(table) &&
                    context.getMigrationContext().getDataSourceRepository().getAllColumnNames(table).contains("ID")) {
                batchColumn = "ID";
            } else if (context.getMigrationContext().getDataSourceRepository().getAllColumnNames(table).contains("PK")) {
                batchColumn = "PK";
            }
            LOG.debug("Using batchColumn: {}", batchColumn.isEmpty() ? "NONE" : batchColumn);

            if (batchColumn.isEmpty()) {
                // trying offset queries with unique index columns
                Set<String> batchColumns;
                DataSet uniqueColumns = context.getMigrationContext().getDataSourceRepository().getUniqueColumns(table);
                if (uniqueColumns.isNotEmpty()) {
                    if (uniqueColumns.getColumnCount() == 0) {
                        throw new IllegalStateException("Corrupt dataset retrieved. Dataset should have information about unique columns");
                    }
                    batchColumns = uniqueColumns.getAllResults().stream().map(row -> String.valueOf(row.get(0))).collect(Collectors.toSet());
                    for (int offset = 0; offset < totalRows; offset += pageSize) {
                        DataReaderTask dataReaderTask = new BatchOffsetDataReaderTask(pipeTaskContext, offset, batchColumns);
                        workerExecutor.safelyExecute(dataReaderTask);
                    }
                } else {
                    //If no unique columns available to do batch sorting, fallback to read all
                    LOG.warn("Reading all rows at once without batching for table {}. Memory consumption might be negatively affected", table);
                    DataReaderTask dataReaderTask = new DefaultDataReaderTask(pipeTaskContext);
                    workerExecutor.safelyExecute(dataReaderTask);
                }
            } else {
                // do the pagination by value comparison
                MarkersQueryDefinition queryDefinition = new MarkersQueryDefinition();
                queryDefinition.setTable(table);
                queryDefinition.setColumn(batchColumn);
                queryDefinition.setBatchSize(pageSize);
                DataSet batchMarkers = dataRepositoryAdapter.getBatchMarkersOrderedByColumn(context.getMigrationContext(), queryDefinition);
                List<List<Object>> batchMarkersList = batchMarkers.getAllResults();
                if (batchMarkersList.isEmpty()) {
                    throw new RuntimeException("Could not retrieve batch values for table " + table);
                }
                for (int i = 0; i < batchMarkersList.size(); i++) {
                    List<Object> lastBatchMarkerRow = batchMarkersList.get(i);
                    Optional<List<Object>> nextBatchMarkerRow = Optional.empty();
                    int nextIndex = i + 1;
                    if (nextIndex < batchMarkersList.size()) {
                        nextBatchMarkerRow = Optional.of(batchMarkersList.get(nextIndex));
                    }
                    DataReaderTask dataReaderTask = new BatchMarkerDataReaderTask(pipeTaskContext, batchColumn, Pair.of(lastBatchMarkerRow, nextBatchMarkerRow));
                    workerExecutor.safelyExecute(dataReaderTask);
                }
            }
        } catch (Exception ex) {
            LOG.error("{{}}: Exception while preparing reader tasks", table, ex);
            pipe.requestAbort(ex);
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Exception while preparing reader tasks", ex);
        }
    }

    private static abstract class DataReaderTask extends RetriableTask {
        private static final Logger LOG = LoggerFactory.getLogger(DataReaderTask.class);

        private PipeTaskContext pipeTaskContext;

        public DataReaderTask(PipeTaskContext pipeTaskContext) {
            super(pipeTaskContext.getContext(), pipeTaskContext.getTable());
            this.pipeTaskContext = pipeTaskContext;
        }

        public PipeTaskContext getPipeTaskContext() {
            return pipeTaskContext;
        }
    }

    private static class DefaultDataReaderTask extends DataReaderTask {

        public DefaultDataReaderTask(PipeTaskContext pipeTaskContext) {
            super(pipeTaskContext);
        }

        @Override
        protected Boolean internalRun() throws Exception {
            process();
            return Boolean.TRUE;
        }

        private void process() throws Exception {
            MigrationContext migrationContext = getPipeTaskContext().getContext().getMigrationContext();
            DataSet all = getPipeTaskContext().getDataRepositoryAdapter().getAll(migrationContext, getPipeTaskContext().getTable());
            getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, all.getAllResults().size());
            getPipeTaskContext().getPipe().put(MaybeFinished.of(all));
        }
    }

    private static class BatchOffsetDataReaderTask extends DataReaderTask {

        private long offset = 0;
        private Set<String> batchColumns;

        public BatchOffsetDataReaderTask(PipeTaskContext pipeTaskContext, long offset, Set<String> batchColumns) {
            super(pipeTaskContext);
            this.offset = offset;
            this.batchColumns = batchColumns;
        }

        @Override
        protected Boolean internalRun() throws Exception {
            process();
            return Boolean.TRUE;
        }

        private void process() throws Exception {
            DataRepositoryAdapter adapter = getPipeTaskContext().getDataRepositoryAdapter();
            CopyContext context = getPipeTaskContext().getContext();
            String table = getPipeTaskContext().getTable();
            long pageSize = getPipeTaskContext().getPageSize();
            OffsetQueryDefinition queryDefinition = new OffsetQueryDefinition();
            queryDefinition.setTable(table);
            queryDefinition.setAllColumns(batchColumns);
            queryDefinition.setBatchSize(pageSize);
            queryDefinition.setOffset(offset);
            DataSet result = adapter.getBatchWithoutIdentifier(context.getMigrationContext(), queryDefinition);
            getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, result.getAllResults().size());
            getPipeTaskContext().getPipe().put(MaybeFinished.of(result));
        }
    }

    private static class BatchMarkerDataReaderTask extends DataReaderTask {

        private final String batchColumn;
        private final Pair<List<Object>, Optional<List<Object>>> batchMarkersPair;

        public BatchMarkerDataReaderTask(PipeTaskContext pipeTaskContext, String batchColumn, Pair<List<Object>, Optional<List<Object>>> batchMarkersPair) {
            super(pipeTaskContext);
            this.batchColumn = batchColumn;
            this.batchMarkersPair = batchMarkersPair;
        }

        @Override
        protected Boolean internalRun() throws Exception {
            List<Object> lastBatchMarker = batchMarkersPair.getLeft();
            Optional<List<Object>> nextBatchMarker = batchMarkersPair.getRight();
            if (lastBatchMarker != null && lastBatchMarker.size() == 2) {
                Object lastBatchValue = lastBatchMarker.get(0);
                process(lastBatchValue, nextBatchMarker.map(v -> v.get(0)));
                return Boolean.TRUE;
            } else {
                throw new IllegalArgumentException("Invalid batch marker passed to task");
            }
        }

        private void process(Object lastValue, Optional<Object> nextValue) throws Exception {
            CopyContext ctx = getPipeTaskContext().getContext();
            DataRepositoryAdapter adapter = getPipeTaskContext().getDataRepositoryAdapter();
            String table = getPipeTaskContext().getTable();
            long pageSize = getPipeTaskContext().getPageSize();
            SeekQueryDefinition queryDefinition = new SeekQueryDefinition();
            queryDefinition.setTable(table);
            queryDefinition.setColumn(batchColumn);
            queryDefinition.setLastColumnValue(lastValue);
            queryDefinition.setNextColumnValue(nextValue.orElseGet(() -> null));
            queryDefinition.setBatchSize(pageSize);
            DataSet page = adapter.getBatchOrderedByColumn(ctx.getMigrationContext(), queryDefinition);
            getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, pageSize);
            getPipeTaskContext().getPipe().put(MaybeFinished.of(page));
        }
    }

    private static class PipeTaskContext {
        private CopyContext context;
        private DataPipe<DataSet> pipe;
        private String table;
        private DataRepositoryAdapter dataRepositoryAdapter;
        private long pageSize;
        private PerformanceRecorder recorder;

        public PipeTaskContext(CopyContext context, DataPipe<DataSet> pipe, String table, DataRepositoryAdapter dataRepositoryAdapter, long pageSize, PerformanceRecorder recorder) {
            this.context = context;
            this.pipe = pipe;
            this.table = table;
            this.dataRepositoryAdapter = dataRepositoryAdapter;
            this.pageSize = pageSize;
            this.recorder = recorder;
        }

        public CopyContext getContext() {
            return context;
        }

        public DataPipe<DataSet> getPipe() {
            return pipe;
        }

        public String getTable() {
            return table;
        }

        public DataRepositoryAdapter getDataRepositoryAdapter() {
            return dataRepositoryAdapter;
        }

        public long getPageSize() {
            return pageSize;
        }

        public PerformanceRecorder getRecorder() {
            return recorder;
        }

    }

}


