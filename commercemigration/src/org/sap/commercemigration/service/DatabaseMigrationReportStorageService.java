package org.sap.commercemigration.service;

import java.io.InputStream;

public interface DatabaseMigrationReportStorageService {
	void store(String fileName, InputStream inputStream) throws Exception;

	boolean validateConnection();
}
