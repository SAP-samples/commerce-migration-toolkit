package org.sap.commercemigration.utils;

public class MaskUtil {

	@java.lang.SuppressWarnings("java:S2068")
	public static String stripJdbcPassword(final String jdbcConnectionString) {
		return jdbcConnectionString.replaceFirst("password=.*?;", "password=***;");
	}

}
