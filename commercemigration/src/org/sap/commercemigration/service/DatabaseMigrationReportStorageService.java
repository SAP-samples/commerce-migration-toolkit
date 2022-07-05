/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.service;

import java.io.InputStream;

public interface DatabaseMigrationReportStorageService {
	void store(String fileName, InputStream inputStream) throws Exception;

	boolean validateConnection();
}
