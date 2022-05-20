/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.DataThreadPoolConfig;
import org.sap.commercemigration.context.CopyContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataThreadPoolFactory {
	ThreadPoolTaskExecutor create(CopyContext context, DataThreadPoolConfig config);

	void destroy(ThreadPoolTaskExecutor executor);

	DataThreadPoolMonitor getMonitor();
}
