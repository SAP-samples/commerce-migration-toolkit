package org.sap.commercemigrationhac.metric.populator;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;

public interface MetricPopulator {
    static String PRIMARY_STANDARD_COLOR = "#92cae4";
    static String PRIMARY_CRITICAL_COLOR = "#de5d70";
    static String SECONDARY_STANDARD_COLOR = "#d5edf8";
    static String SECONDARY_CRITICAL_COLOR = "#e8acb5";

    MetricData populate(MigrationContext context) throws Exception;

    default void populateColors(MetricData data) {
        data.setPrimaryValueStandardColor(PRIMARY_STANDARD_COLOR);
        data.setPrimaryValueCriticalColor(PRIMARY_CRITICAL_COLOR);
        data.setSecondaryValueStandardColor(SECONDARY_STANDARD_COLOR);
        data.setSecondaryValueCriticalColor(SECONDARY_CRITICAL_COLOR);
    }
}
