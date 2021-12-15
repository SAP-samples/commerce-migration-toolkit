package org.sap.commercemigration.scheduler;

import java.util.List;

public interface DatabaseCopySchedulerAlgorithm {
	int getOwnNodeId();

	List<Integer> getNodeIds();

	int next();

	void reset();
}
