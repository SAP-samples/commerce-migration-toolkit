/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.scheduler;

import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;

import java.time.OffsetDateTime;

/**
 * Scheduler for Cluster Migration
 */
public interface DatabaseCopyScheduler {
	void schedule(CopyContext context) throws Exception;

	void resumeUnfinishedItems(CopyContext copyContext) throws Exception;

	MigrationStatus getCurrentState(CopyContext context, OffsetDateTime since) throws Exception;

	boolean isAborted(CopyContext context) throws Exception;

	void abort(CopyContext context) throws Exception;
}
