/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.events;

/**
 * Cluster Event to notify a Cluster to start the copy process
 */
public class CopyDatabaseTableEvent extends CopyEvent {

	private final boolean resumeUnfinishedMigration;

	public CopyDatabaseTableEvent(final Integer sourceNodeId, final String migrationId,
			boolean resumeUnfinishedMigration) {
		super(sourceNodeId, migrationId);
		this.resumeUnfinishedMigration = resumeUnfinishedMigration;
	}

	public boolean isResumeUnfinishedMigration() {
		return resumeUnfinishedMigration;
	}
}
