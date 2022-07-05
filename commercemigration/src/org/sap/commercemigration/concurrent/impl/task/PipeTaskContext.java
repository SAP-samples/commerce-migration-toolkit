/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl.task;

import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;

public class PipeTaskContext {
	private CopyContext context;
	private DataPipe<DataSet> pipe;
	private String table;
	private DataRepositoryAdapter dataRepositoryAdapter;
	private long pageSize;
	private PerformanceRecorder recorder;
	private DatabaseCopyTaskRepository taskRepository;

	public PipeTaskContext(CopyContext context, DataPipe<DataSet> pipe, String table,
			DataRepositoryAdapter dataRepositoryAdapter, long pageSize, PerformanceRecorder recorder,
			DatabaseCopyTaskRepository taskRepository) {
		this.context = context;
		this.pipe = pipe;
		this.table = table;
		this.dataRepositoryAdapter = dataRepositoryAdapter;
		this.pageSize = pageSize;
		this.recorder = recorder;
		this.taskRepository = taskRepository;
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

	public DatabaseCopyTaskRepository getTaskRepository() {
		return taskRepository;
	}
}
