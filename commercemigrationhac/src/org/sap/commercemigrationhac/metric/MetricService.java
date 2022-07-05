/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigrationhac.metric;

import de.hybris.platform.commercemigrationhac.data.MetricData;
import org.sap.commercemigration.context.MigrationContext;

import java.util.List;

public interface MetricService {
	List<MetricData> getMetrics(MigrationContext context);
}
