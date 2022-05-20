/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataThreadPoolMonitor {
	void subscribe(ThreadPoolTaskExecutor executor);
	void unsubscribe(ThreadPoolTaskExecutor executor);

	int getActiveCount();

	int getMaxPoolSize();
}
