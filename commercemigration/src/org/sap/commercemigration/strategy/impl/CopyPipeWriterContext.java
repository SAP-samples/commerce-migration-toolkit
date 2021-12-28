/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.strategy.impl;

import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.performance.PerformanceRecorder;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class CopyPipeWriterContext {
	private CopyContext context;
	private CopyContext.DataCopyItem copyItem;
	private List<String> columnsToCopy;
	private Set<String> nullifyColumns;
	private PerformanceRecorder performanceRecorder;
	private AtomicLong totalCount;
	private String upsertId;
	private boolean requiresIdentityInsert;
	private DatabaseCopyTaskRepository databaseCopyTaskRepository;

	public CopyPipeWriterContext(CopyContext context, CopyContext.DataCopyItem copyItem, List<String> columnsToCopy,
			Set<String> nullifyColumns, PerformanceRecorder performanceRecorder, AtomicLong totalCount, String upsertId,
			boolean requiresIdentityInsert, DatabaseCopyTaskRepository databaseCopyTaskRepository) {
		this.context = context;
		this.copyItem = copyItem;
		this.columnsToCopy = columnsToCopy;
		this.nullifyColumns = nullifyColumns;
		this.performanceRecorder = performanceRecorder;
		this.totalCount = totalCount;
		this.upsertId = upsertId;
		this.requiresIdentityInsert = requiresIdentityInsert;
		this.databaseCopyTaskRepository = databaseCopyTaskRepository;
	}

	public CopyContext getContext() {
		return context;
	}

	public CopyContext.DataCopyItem getCopyItem() {
		return copyItem;
	}

	public List<String> getColumnsToCopy() {
		return columnsToCopy;
	}

	public Set<String> getNullifyColumns() {
		return nullifyColumns;
	}

	public PerformanceRecorder getPerformanceRecorder() {
		return performanceRecorder;
	}

	public AtomicLong getTotalCount() {
		return totalCount;
	}

	public String getUpsertId() {
		return upsertId;
	}

	public boolean isRequiresIdentityInsert() {
		return requiresIdentityInsert;
	}

	public DatabaseCopyTaskRepository getDatabaseCopyTaskRepository() {
		return databaseCopyTaskRepository;
	}
}
