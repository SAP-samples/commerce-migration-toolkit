/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2019 SAP SE
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * Hybris ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the
 * terms of the license agreement you entered into with SAP Hybris.
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

    public CopyEvent(final int sourceNodeId, final String migrationId) {
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
