package org.sap.commercemigrationhac.metric.populator.impl;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigrationhac.metric.populator.MetricPopulator;

public class MemoryMetricPopulator implements MetricPopulator {
	@Override
	public MetricData populate(MigrationContext context) throws Exception {
		MetricData data = new MetricData();
		Runtime runtime = Runtime.getRuntime();
		double freeMemory = runtime.freeMemory() / 1048576d;
		double totalMemory = runtime.totalMemory() / 1048576d;
		double usedMemory = totalMemory - freeMemory;
		data.setMetricId("memory");
		data.setName("Memory");
		data.setDescription("The proportion of free and used memory");
		data.setPrimaryValue(usedMemory);
		data.setPrimaryValueLabel("Used");
		data.setPrimaryValueUnit("MB");
		data.setPrimaryValueThreshold(totalMemory * 0.9);
		data.setSecondaryValue(freeMemory);
		data.setSecondaryValueLabel("Free");
		data.setSecondaryValueUnit("MB");
		data.setSecondaryValueThreshold(0d);
		populateColors(data);
		return data;
	}
}
