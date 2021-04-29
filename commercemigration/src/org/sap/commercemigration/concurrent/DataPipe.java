package org.sap.commercemigration.concurrent;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Used to separate database reading and writing operations, after reading data from the DB, the result
 * is put to the pipe and can be used by the database writer later on -> asynchronously
 *
 * @param <T>
 */
@ThreadSafe
public interface DataPipe<T> {
    void requestAbort(Exception e);

    void put(MaybeFinished<T> value) throws Exception;

    MaybeFinished<T> get() throws Exception;
}
