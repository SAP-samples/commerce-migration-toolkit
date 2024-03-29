/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

import javax.annotation.concurrent.ThreadSafe;

import org.sap.commercemigration.context.CopyContext;

@ThreadSafe
public interface DataPipeFactory<T> {
	DataPipe<T> create(CopyContext context, CopyContext.DataCopyItem item) throws Exception;
}
