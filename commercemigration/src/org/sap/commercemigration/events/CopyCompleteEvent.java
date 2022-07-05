/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.events;

/**
 * * ClusterAwareEvent to signal completion of the assigned copy ta
 */
public class CopyCompleteEvent extends CopyEvent {

	private Boolean copyResult = false;

	public CopyCompleteEvent(final Integer sourceNodeId, final String migrationId) {
		super(sourceNodeId, migrationId);
	}

	public Boolean getCopyResult() {
		return copyResult;
	}
}
