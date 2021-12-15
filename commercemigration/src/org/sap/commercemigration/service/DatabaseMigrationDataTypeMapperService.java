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
