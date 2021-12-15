package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.context.CopyContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataWorkerPoolFactory {
	ThreadPoolTaskExecutor create(CopyContext context);
}
