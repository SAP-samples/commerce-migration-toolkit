/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.scheduler;

import java.util.List;

public interface DatabaseCopySchedulerAlgorithm {
	int getOwnNodeId();

	List<Integer> getNodeIds();

	int next();

	void reset();
}
