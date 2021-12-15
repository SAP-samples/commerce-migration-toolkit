package org.sap.commercemigration.concurrent;

public class PipeAbortedException extends Exception {
	public PipeAbortedException(String message) {
		super(message);
	}

	public PipeAbortedException(String message, Throwable cause) {
		super(message, cause);
	}
}
