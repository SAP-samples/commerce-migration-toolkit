/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.context.CopyContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public interface DataWorkerPoolFactory {
	ThreadPoolTaskExecutor create(CopyContext context);
}
