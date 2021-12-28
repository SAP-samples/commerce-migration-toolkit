/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric.populator.impl;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigrationhac.metric.populator.MetricPopulator;

public class DTUMetricPopulator implements MetricPopulator {
	@Override
	public MetricData populate(MigrationContext context) throws Exception {
		MetricData data = new MetricData();
		int primaryValue = (int) context.getDataTargetRepository().getDatabaseUtilization();
		if (primaryValue > 100) {
			primaryValue = 100;
		}
		int secondaryValue = 100 - primaryValue;
		if (primaryValue < 0) {
			primaryValue = -1;
			secondaryValue = -1;
		}

		data.setMetricId("dtu");
		data.setName("DTU");
		data.setDescription("The current DTU utilization of the azure database");
		data.setPrimaryValue((double) primaryValue);
		data.setPrimaryValueLabel("Used");
		data.setPrimaryValueUnit("%");
		data.setPrimaryValueThreshold(90d);
		data.setSecondaryValue((double) secondaryValue);
		data.setSecondaryValueLabel("Idle");
		data.setSecondaryValueUnit("%");
		data.setSecondaryValueThreshold(0d);
		populateColors(data);
		return data;
	}

}
