package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface DataPipeFactory<T> {
    DataPipe<DataSet> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception;
}
