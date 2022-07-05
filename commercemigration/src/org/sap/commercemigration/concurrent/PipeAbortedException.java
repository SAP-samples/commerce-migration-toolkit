/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent;

public class PipeAbortedException extends Exception {
	public PipeAbortedException(String message) {
		super(message);
	}

	public PipeAbortedException(String message, Throwable cause) {
		super(message, cause);
	}
}
