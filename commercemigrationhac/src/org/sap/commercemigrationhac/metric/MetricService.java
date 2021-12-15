package org.sap.commercemigrationhac.metric;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;

import java.util.List;

public interface MetricService {
	List<MetricData> getMetrics(MigrationContext context);
}
