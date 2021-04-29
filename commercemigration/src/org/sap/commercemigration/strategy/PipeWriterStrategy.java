package org.sap.commercemigration.strategy;

import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.context.CopyContext;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main Strategy to Write Data to a target Database
 *
 * @param <T>
 */
@ThreadSafe
public interface PipeWriterStrategy<T> {
    /**
     * Performs the actual copying of Data Items
     *
     * @param context
     * @param pipe
     * @param item
     * @throws Exception
     */
    void write(CopyContext context, DataPipe<T> pipe, CopyContext.DataCopyItem item) throws Exception;
}
