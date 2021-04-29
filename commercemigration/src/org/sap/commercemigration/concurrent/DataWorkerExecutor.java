package org.sap.commercemigration.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public interface DataWorkerExecutor<T> {
    Future<T> safelyExecute(Callable<T> callable) throws InterruptedException;

    void waitAndRethrowUncaughtExceptions() throws ExecutionException, InterruptedException;
}
