/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.populator.impl;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.performance.PerformanceCategory;
import org.sap.commercemigration.performance.PerformanceProfiler;
import org.sap.commercemigration.performance.PerformanceUnit;
import org.sap.commercemigrationhac.metric.populator.MetricPopulator;

public class IOMetricPopulator implements MetricPopulator {

	private PerformanceProfiler performanceProfiler;

	public IOMetricPopulator(PerformanceProfiler performanceProfiler) {
		this.performanceProfiler = performanceProfiler;
	}

	@Override
	public MetricData populate(MigrationContext context) throws Exception {
		MetricData data = new MetricData();
		int avgRowReading = (int) performanceProfiler.getAverageByCategoryAndUnit(PerformanceCategory.DB_READ,
				PerformanceUnit.ROWS);
		int avgRowWriting = (int) performanceProfiler.getAverageByCategoryAndUnit(PerformanceCategory.DB_WRITE,
				PerformanceUnit.ROWS);
		int totalIO = avgRowReading + avgRowWriting;
		if (avgRowReading < 1 && avgRowWriting < 1) {
			avgRowReading = -1;
			avgRowWriting = -1;
		}
		data.setMetricId("db-io");
		data.setName("Database I/O");
		data.setDescription("The proportion of items read from source compared to items written to target");
		data.setPrimaryValue((double) avgRowReading);
		data.setPrimaryValueLabel("Read");
		data.setPrimaryValueUnit("rows/s");
		data.setPrimaryValueThreshold(totalIO * 0.75);
		data.setSecondaryValue((double) avgRowWriting);
		data.setSecondaryValueLabel("Write");
		data.setSecondaryValueUnit("rows/s");
		data.setSecondaryValueThreshold(totalIO * 0.75);
		populateColors(data);
		return data;
	}
}
