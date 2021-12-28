/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl.task;

import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceUnit;

public class DefaultDataReaderTask extends DataReaderTask {

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
		DataSet all = getPipeTaskContext().getDataRepositoryAdapter().getAll(migrationContext,
				getPipeTaskContext().getTable());
		getPipeTaskContext().getRecorder().record(PerformanceUnit.ROWS, all.getAllResults().size());
		getPipeTaskContext().getPipe().put(MaybeFinished.of(all));
	}
}
