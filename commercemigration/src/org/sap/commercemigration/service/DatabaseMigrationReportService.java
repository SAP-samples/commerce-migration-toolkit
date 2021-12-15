package org.sap.commercemigration.service;

import org.sap.commercemigration.MigrationReport;
import org.sap.commercemigration.context.CopyContext;

public interface DatabaseMigrationReportService {

	MigrationReport getMigrationReport(CopyContext copyContext) throws Exception;

}
