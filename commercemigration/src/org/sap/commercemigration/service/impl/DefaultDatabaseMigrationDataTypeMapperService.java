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
package org.sap.commercemigration.service.impl;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Types;

/**
 *
 */
public class DefaultDatabaseMigrationDataTypeMapperService implements DatabaseMigrationDataTypeMapperService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationDataTypeMapperService.class);

    @Override
    public Object dataTypeMapper(final Object sourceColumnValue, final int jdbcType)
            throws IOException, SQLException {
        Object targetColumnValue = sourceColumnValue;
        if (sourceColumnValue == null) {
            // do nothing
        } else if (jdbcType == Types.BLOB) {
            targetColumnValue = new ByteArrayInputStream(ByteStreams.toByteArray(((Blob) sourceColumnValue).getBinaryStream()));
        } else if (jdbcType == Types.NCLOB) {
            targetColumnValue = getValue((NClob) sourceColumnValue);
        } else if (jdbcType == Types.CLOB) {
            targetColumnValue = getValue((Clob) sourceColumnValue);
        }
        return targetColumnValue;
    }

    private String getValue(final NClob nClob) throws SQLException, IOException {
        return getValue(nClob.getCharacterStream());
    }

    private String getValue(final Clob clob) throws SQLException, IOException {
        return getValue(clob.getCharacterStream());
    }

    private String getValue(final Reader in) throws SQLException, IOException {
        final StringWriter w = new StringWriter();
        IOUtils.copy(in, w);
        String value = w.toString();
        w.close();
        in.close();
        return value;
    }

}
