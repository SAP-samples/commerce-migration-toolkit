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
package org.sap.commercemigration.service;

import org.sap.commercemigration.context.CopyContext;


/**
 * Actual Service to perform the Migration
 */
public interface DatabaseMigrationCopyService {

    void copyAllAsync(CopyContext context);

}
