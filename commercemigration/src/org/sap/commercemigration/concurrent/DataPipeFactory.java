package org.sap.commercemigration.concurrent;

import org.sap.commercemigration.context.CopyContext;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface DataPipeFactory<T> {
	DataPipe<T> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception;
}
