/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.populator.impl;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.concurrent.DataThreadPoolFactory;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigrationhac.metric.populator.MetricPopulator;

public class ThreadPoolMetricPopulator implements MetricPopulator {

	private DataThreadPoolFactory factory;
	private String name;

	public ThreadPoolMetricPopulator(DataThreadPoolFactory factory, String name) {
		this.factory = factory;
		this.name = name;
	}

	@Override
	public MetricData populate(MigrationContext context) throws Exception {
		MetricData data = new MetricData();
		double activeCount = factory.getMonitor().getActiveCount();
		double maxPoolSize = factory.getMonitor().getMaxPoolSize();
		if (maxPoolSize < 1) {
			// make primary and secondary value negative to indicate inactive widget
			activeCount = -1;
			maxPoolSize = -2;
		}
		data.setMetricId(name + "-executor");
		data.setName(StringUtils.capitalize(name) + " Tasks");
		data.setDescription("The workers running in parallel in the task executor");
		data.setPrimaryValue(activeCount);
		data.setPrimaryValueLabel("Running");
		data.setPrimaryValueUnit("#");
		data.setPrimaryValueThreshold(-1d);
		data.setSecondaryValue(maxPoolSize - activeCount);
		data.setSecondaryValueLabel("Free");
		data.setSecondaryValueUnit("#");
		data.setSecondaryValueThreshold(-1d);
		populateColors(data);
		return data;
	}
}
