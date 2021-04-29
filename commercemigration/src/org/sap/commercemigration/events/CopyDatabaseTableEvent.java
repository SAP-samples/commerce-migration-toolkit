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

/**
 * Cluster Event to notify a Cluster to start the copy process
 */
public class CopyDatabaseTableEvent extends CopyEvent {
    public CopyDatabaseTableEvent(final Integer sourceNodeId, final String migrationId) {
        super(sourceNodeId, migrationId);
    }
}
