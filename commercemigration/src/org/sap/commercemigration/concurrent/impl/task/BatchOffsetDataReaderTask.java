/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl.task;

import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceUnit;

import java.util.Set;

public class BatchOffsetDataReaderTask extends DataReaderTask {

	private final long offset;
	private final Set<String> batchColumns;
	private final int batchId;

	public BatchOffsetDataReaderTask(PipeTaskContext pipeTaskContext, int batchId, long offset,
			Set<String> batchColumns) {
		super(pipeTaskContext);
		this.batchId = batchId;
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
		queryDefinition.setBatchId(batchId);
		queryDefinition.setTable(table);
		queryDefinition.setAllColumns(batchColumns);
		queryDefinition.setBatchSize(pageSize);
		queryDefinition.setOffset(offset);
		DataSet result = adapter.getBatchWithoutIdentifier(context.getMigrationContext(), queryDefinition);
		getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, result.getAllResults().size());
		getPipeTaskContext().getPipe().put(MaybeFinished.of(result));
	}
}
