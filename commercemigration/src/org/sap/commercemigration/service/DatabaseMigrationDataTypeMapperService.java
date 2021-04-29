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

import java.io.IOException;
import java.sql.SQLException;


/**
 * Service to deal with Mapping different types between Databases
 */
public interface DatabaseMigrationDataTypeMapperService {

    /**
     * Converts BLOB, CLOB and NCLOB Data
     */
    Object dataTypeMapper(final Object sourceColumnValue, final int jdbcType) throws IOException, SQLException;
}
