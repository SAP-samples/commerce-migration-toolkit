package org.sap.commercemigration.utils;

public class MaskUtil {

    public static String stripJdbcPassword(final String jdbcConnectionString) {
        return jdbcConnectionString.replaceFirst("password=.*?;", "password=***;");
    }

}
