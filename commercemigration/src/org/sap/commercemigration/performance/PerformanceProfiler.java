/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.performance;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public interface PerformanceProfiler {
	PerformanceRecorder createRecorder(PerformanceCategory category, String name);

	void muteRecorder(PerformanceCategory category, String name);

	ConcurrentMap<String, PerformanceRecorder> getRecorders();

	Collection<PerformanceRecorder> getRecordersByCategory(PerformanceCategory category);

	double getAverageByCategoryAndUnit(PerformanceCategory category, PerformanceUnit unit);

	PerformanceRecorder getRecorder(PerformanceCategory category, String name);

	void reset();
}
