/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.utils;

public class MaskUtil {

	private MaskUtil() {
	}

	@java.lang.SuppressWarnings("java:S2068")
	public static String stripJdbcPassword(final String jdbcConnectionString) {
		return jdbcConnectionString.replaceFirst("password=.*?;", "password=***;");
	}

}
