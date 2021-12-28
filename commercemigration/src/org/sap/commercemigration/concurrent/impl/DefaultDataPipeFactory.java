/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.fest.util.Collections;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.concurrent.DataCopyMethod;
import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.concurrent.DataPipeFactory;
import org.sap.commercemigration.concurrent.DataWorkerExecutor;
import org.sap.commercemigration.concurrent.DataWorkerPoolFactory;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.concurrent.impl.task.BatchMarkerDataReaderTask;
import org.sap.commercemigration.concurrent.impl.task.BatchOffsetDataReaderTask;
import org.sap.commercemigration.concurrent.impl.task.DataReaderTask;
import org.sap.commercemigration.concurrent.impl.task.DefaultDataReaderTask;
import org.sap.commercemigration.concurrent.impl.task.PipeTaskContext;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.scheduler.DatabaseCopyScheduler;
import org.sap.commercemigration.service.DatabaseCopyBatch;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
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

	public DefaultDataPipeFactory(DatabaseCopyScheduler scheduler, DatabaseCopyTaskRepository taskRepository,
			AsyncTaskExecutor executor, DataWorkerPoolFactory dataReadWorkerPoolFactory) {
		this.scheduler = scheduler;
		this.taskRepository = taskRepository;
		this.executor = executor;
		this.dataReadWorkerPoolFactory = dataReadWorkerPoolFactory;
	}

	@Override
	public DataPipe<DataSet> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception {
		int dataPipeTimeout = context.getMigrationContext().getDataPipeTimeout();
		int dataPipeCapacity = context.getMigrationContext().getDataPipeCapacity();
		DataPipe<DataSet> pipe = new DefaultDataPipe<>(scheduler, taskRepository, context, item, dataPipeTimeout,
				dataPipeCapacity);
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
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
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

	private void scheduleWorkers(CopyContext context, DataWorkerExecutor<Boolean> workerExecutor,
			DataPipe<DataSet> pipe, CopyContext.DataCopyItem copyItem) throws Exception {
		DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(
				context.getMigrationContext().getDataSourceRepository());
		String table = copyItem.getSourceItem();
		long totalRows = copyItem.getRowCount();
		long pageSize = context.getMigrationContext().getReaderBatchSize();
		try {
			PerformanceRecorder recorder = context.getPerformanceProfiler().createRecorder(PerformanceCategory.DB_READ,
					table);
			recorder.start();

			PipeTaskContext pipeTaskContext = new PipeTaskContext(context, pipe, table, dataRepositoryAdapter, pageSize,
					recorder, taskRepository);

			String batchColumn = "";
			// help.sap.com/viewer/d0224eca81e249cb821f2cdf45a82ace/LATEST/en-US/08a27931a21441b59094c8a6aa2a880e.html
			if (context.getMigrationContext().getDataSourceRepository().isAuditTable(table) && context
					.getMigrationContext().getDataSourceRepository().getAllColumnNames(table).contains("ID")) {
				batchColumn = "ID";
			} else if (context.getMigrationContext().getDataSourceRepository().getAllColumnNames(table)
					.contains("PK")) {
				batchColumn = "PK";
			}
			LOG.debug("Using batchColumn: {}", batchColumn.isEmpty() ? "NONE" : batchColumn);

			if (batchColumn.isEmpty()) {
				// trying offset queries with unique index columns
				Set<String> batchColumns;
				DataSet uniqueColumns = context.getMigrationContext().getDataSourceRepository().getUniqueColumns(table);
				if (uniqueColumns.isNotEmpty()) {
					if (uniqueColumns.getColumnCount() == 0) {
						throw new IllegalStateException(
								"Corrupt dataset retrieved. Dataset should have information about unique columns");
					}
					batchColumns = uniqueColumns.getAllResults().stream().map(row -> String.valueOf(row.get(0)))
							.collect(Collectors.toSet());
					taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.OFFSET.toString());
					taskRepository.updateTaskKeyColumns(context, copyItem, batchColumns);

					List<Long> batches = null;
					if (context.getMigrationContext().isSchedulerResumeEnabled()) {
						Set<DatabaseCopyBatch> pendingBatchesForPipeline = taskRepository
								.findPendingBatchesForPipeline(context, copyItem);
						batches = pendingBatchesForPipeline.stream()
								.map(b -> Long.valueOf(b.getLowerBoundary().toString())).collect(Collectors.toList());
						taskRepository.resetPipelineBatches(context, copyItem);
					} else {
						batches = new ArrayList<>();
						for (long offset = 0; offset < totalRows; offset += pageSize) {
							batches.add(offset);
						}
					}

					for (int batchId = 0; batchId < batches.size(); batchId++) {
						long offset = batches.get(batchId);
						DataReaderTask dataReaderTask = new BatchOffsetDataReaderTask(pipeTaskContext, batchId, offset,
								batchColumns);
						taskRepository.scheduleBatch(context, copyItem, batchId, offset, offset + pageSize);
						workerExecutor.safelyExecute(dataReaderTask);
					}
				} else {
					// If no unique columns available to do batch sorting, fallback to read all
					LOG.warn(
							"Reading all rows at once without batching for table {}. Memory consumption might be negatively affected",
							table);
					taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.DEFAULT.toString());
					if (context.getMigrationContext().isSchedulerResumeEnabled()) {
						taskRepository.resetPipelineBatches(context, copyItem);
					}
					taskRepository.scheduleBatch(context, copyItem, 0, 0, totalRows);
					DataReaderTask dataReaderTask = new DefaultDataReaderTask(pipeTaskContext);
					workerExecutor.safelyExecute(dataReaderTask);
				}
			} else {
				// do the pagination by value comparison
				taskRepository.updateTaskCopyMethod(context, copyItem, DataCopyMethod.SEEK.toString());
				taskRepository.updateTaskKeyColumns(context, copyItem, Lists.newArrayList(batchColumn));

				List<List<Object>> batchMarkersList = null;
				if (context.getMigrationContext().isSchedulerResumeEnabled()) {
					batchMarkersList = new ArrayList<>();
					Set<DatabaseCopyBatch> pendingBatchesForPipeline = taskRepository
							.findPendingBatchesForPipeline(context, copyItem);
					batchMarkersList.addAll(pendingBatchesForPipeline.stream()
							.map(b -> Collections.list(b.getLowerBoundary())).collect(Collectors.toList()));
					taskRepository.resetPipelineBatches(context, copyItem);
				} else {
					MarkersQueryDefinition queryDefinition = new MarkersQueryDefinition();
					queryDefinition.setTable(table);
					queryDefinition.setColumn(batchColumn);
					queryDefinition.setBatchSize(pageSize);
					DataSet batchMarkers = dataRepositoryAdapter
							.getBatchMarkersOrderedByColumn(context.getMigrationContext(), queryDefinition);
					batchMarkersList = batchMarkers.getAllResults();
				}

				for (int i = 0; i < batchMarkersList.size(); i++) {
					List<Object> lastBatchMarkerRow = batchMarkersList.get(i);
					Optional<List<Object>> nextBatchMarkerRow = Optional.empty();
					int nextIndex = i + 1;
					if (nextIndex < batchMarkersList.size()) {
						nextBatchMarkerRow = Optional.of(batchMarkersList.get(nextIndex));
					}
					if (!Collections.isEmpty(lastBatchMarkerRow)) {
						Object lastBatchValue = lastBatchMarkerRow.get(0);
						Pair<Object, Object> batchMarkersPair = Pair.of(lastBatchValue,
								nextBatchMarkerRow.map(v -> v.get(0)).orElseGet(() -> null));
						DataReaderTask dataReaderTask = new BatchMarkerDataReaderTask(pipeTaskContext, i, batchColumn,
								batchMarkersPair);
						// After creating the task, we register the batch in the db for later use if
						// necessary
						taskRepository.scheduleBatch(context, copyItem, i, batchMarkersPair.getLeft(),
								batchMarkersPair.getRight());
						workerExecutor.safelyExecute(dataReaderTask);
					} else {
						throw new IllegalArgumentException("Invalid batch marker passed to task");
					}
				}
			}
		} catch (Exception ex) {
			pipe.requestAbort(ex);
			if (ex instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
			throw new RuntimeException("Exception while preparing reader tasks", ex);
		}
	}

}
