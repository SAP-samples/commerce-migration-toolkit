/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.logging;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable value-based classes representing a JDBC query ran by the migration
 * tool against a {@link org.sap.commercemigration.repository.DataRepository}
 */
public class JdbcQueryLog {

	private final ZonedDateTime jdbcQueryTimestamp;
	private final String jdbcQuery;
	private final Map<Integer, Object> parameters;

	public JdbcQueryLog(final String jdbcQuery) {
		this(jdbcQuery, null);
	}
	public JdbcQueryLog(final String jdbcQuery, final Map<Integer, Object> parameters) {
		this.jdbcQueryTimestamp = ZonedDateTime.now();
		this.jdbcQuery = jdbcQuery;
		if (parameters == null || parameters.isEmpty()) {
			this.parameters = null;
		} else {
			this.parameters = Collections.unmodifiableMap(parameters);
		}
	}

	/**
	 * Timestamp when the JDBC query was executed
	 *
	 * @return timestamp of the JDBC query
	 */
	public ZonedDateTime getJdbcQueryTimestamp() {
		return jdbcQueryTimestamp;
	}

	/**
	 * String representation of the JDBC query
	 *
	 * @return string representation of the JDBC query
	 */
	public String getJdbcQuery() {
		return jdbcQuery;
	}

	/**
	 * If the JDBC query has parameters, this will return the map of the parameters
	 * with the parameter index as key and the parameter value as value
	 *
	 * @return the JDBC query parameters, if it has any; null otherwise.
	 */
	public Map<Integer, Object> getParameters() {
		return parameters;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("{timestamp=").append(jdbcQueryTimestamp).append(", query='")
				.append(jdbcQuery).append("'");
		if (parameters != null) {
			sb.append(", parameters=").append(parameters);
		}
		sb.append("}");
		return sb.toString();
	}
}
