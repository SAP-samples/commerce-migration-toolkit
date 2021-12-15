package org.sap.commercemigration.processors.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.sap.commercemigration.MigrationReport;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.processors.MigrationPostProcessor;
import org.sap.commercemigration.service.DatabaseMigrationReportService;
import org.sap.commercemigration.service.DatabaseMigrationReportStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ReportMigrationPostProcessor implements MigrationPostProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(ReportMigrationPostProcessor.class.getName());

	private DatabaseMigrationReportService databaseMigrationReportService;
	private DatabaseMigrationReportStorageService databaseMigrationReportStorageService;

	@Override
	public void process(CopyContext context) {
		try {
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			MigrationReport migrationReport = databaseMigrationReportService.getMigrationReport(context);
			InputStream is = new ByteArrayInputStream(gson.toJson(migrationReport).getBytes(StandardCharsets.UTF_8));
			databaseMigrationReportStorageService.store(context.getMigrationId() + ".json", is);
			LOG.info("Finished writing database migration report");
		} catch (Exception e) {
			LOG.error("Error executing post processor", e);
		}
	}

	public void setDatabaseMigrationReportService(DatabaseMigrationReportService databaseMigrationReportService) {
		this.databaseMigrationReportService = databaseMigrationReportService;
	}

	public void setDatabaseMigrationReportStorageService(
			DatabaseMigrationReportStorageService databaseMigrationReportStorageService) {
		this.databaseMigrationReportStorageService = databaseMigrationReportStorageService;
	}
}
