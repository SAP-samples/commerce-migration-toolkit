/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.impl;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigrationhac.metric.MetricService;
import org.sap.commercemigrationhac.metric.populator.MetricPopulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultMetricService implements MetricService {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricService.class);

	private List<MetricPopulator> populators;

	public DefaultMetricService(List<MetricPopulator> populators) {
		this.populators = populators;
	}

	@Override
	public List<MetricData> getMetrics(MigrationContext context) {
		List<MetricData> dataList = new ArrayList<>();
		for (MetricPopulator populator : populators) {
			try {
				dataList.add(populator.populate(context));
			} catch (Exception e) {
				LOG.error("Error while populating metric. Populator: " + populator.getClass().getName(), e);
			}
		}
		return dataList;
	}

}
