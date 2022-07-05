/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl.task;

import org.apache.commons.lang3.tuple.Pair;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceUnit;

public class BatchMarkerDataReaderTask extends DataReaderTask {

	private final String batchColumn;
	private final Pair<Object, Object> batchMarkersPair;
	private final int batchId;

	public BatchMarkerDataReaderTask(PipeTaskContext pipeTaskContext, int batchId, String batchColumn,
			Pair<Object, Object> batchMarkersPair) {
		super(pipeTaskContext);
		this.batchId = batchId;
		this.batchColumn = batchColumn;
		this.batchMarkersPair = batchMarkersPair;
	}

	@Override
	protected Boolean internalRun() throws Exception {
		process(batchMarkersPair.getLeft(), batchMarkersPair.getRight());
		return Boolean.TRUE;
	}

	private void process(Object lastValue, Object nextValue) throws Exception {
		CopyContext ctx = getPipeTaskContext().getContext();
		DataRepositoryAdapter adapter = getPipeTaskContext().getDataRepositoryAdapter();
		String table = getPipeTaskContext().getTable();
		long pageSize = getPipeTaskContext().getPageSize();
		SeekQueryDefinition queryDefinition = new SeekQueryDefinition();
		queryDefinition.setBatchId(batchId);
		queryDefinition.setTable(table);
		queryDefinition.setColumn(batchColumn);
		queryDefinition.setLastColumnValue(lastValue);
		queryDefinition.setNextColumnValue(nextValue);
		queryDefinition.setBatchSize(pageSize);
		DataSet page = adapter.getBatchOrderedByColumn(ctx.getMigrationContext(), queryDefinition);
		getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, pageSize);
		getPipeTaskContext().getPipe().put(MaybeFinished.of(page));
	}
}
