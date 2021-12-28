/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl.task;

public abstract class DataReaderTask extends RetriableTask {

	private PipeTaskContext pipeTaskContext;

	protected DataReaderTask(PipeTaskContext pipeTaskContext) {
		super(pipeTaskContext.getContext(), pipeTaskContext.getTable());
		this.pipeTaskContext = pipeTaskContext;
	}

	public PipeTaskContext getPipeTaskContext() {
		return pipeTaskContext;
	}
}
