/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.events;

import de.hybris.platform.servicelayer.event.ClusterAwareEvent;
import de.hybris.platform.servicelayer.event.PublishEventContext;
import de.hybris.platform.servicelayer.event.events.AbstractEvent;

/**
 * ClusterAwareEvent to notify other Nodes to start the migration
 */
public abstract class CopyEvent extends AbstractEvent implements ClusterAwareEvent {

	private final int sourceNodeId;

	private final String migrationId;

	protected CopyEvent(final int sourceNodeId, final String migrationId) {
		super();
		this.sourceNodeId = sourceNodeId;
		this.migrationId = migrationId;
	}

	@Override
	public boolean canPublish(PublishEventContext publishEventContext) {
		return true;
	}

	/**
	 * @return the masterNodeId
	 */
	public int getSourceNodeId() {
		return sourceNodeId;
	}

	/**
	 * @return the migrationId
	 */
	public String getMigrationId() {
		return migrationId;
	}

}
