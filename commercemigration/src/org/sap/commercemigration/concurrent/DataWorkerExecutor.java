/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface DataWorkerExecutor<T> {
	Future<T> safelyExecute(Callable<T> callable) throws InterruptedException;

	void waitAndRethrowUncaughtExceptions() throws ExecutionException, InterruptedException;
}
