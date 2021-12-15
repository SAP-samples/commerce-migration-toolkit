package org.sap.commercemigration.service.impl;

import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.configuration.Configuration;
import org.sap.commercemigration.MigrationReport;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.scheduler.DatabaseCopyScheduler;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.sap.commercemigration.service.DatabaseMigrationReportService;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.sap.commercemigration.constants.CommercemigrationConstants.MASKED_VALUE;
import static org.sap.commercemigration.constants.CommercemigrationConstants.MIGRATION_REPORT_MASKED_PROPERTIES;
import static org.sap.commercemigration.constants.CommercemigrationConstants.PROPERTIES_PREFIX;
import static org.sap.commercemigration.utils.MaskUtil.stripJdbcPassword;

public class DefaultDatabaseMigrationReportService implements DatabaseMigrationReportService {

	private DatabaseCopyScheduler databaseCopyScheduler;
	private DatabaseCopyTaskRepository databaseCopyTaskRepository;
	private ConfigurationService configurationService;

	@Override
	public MigrationReport getMigrationReport(CopyContext copyContext) throws Exception {
		final MigrationReport migrationReport = new MigrationReport();
		migrationReport.setMigrationID(copyContext.getMigrationId());
		populateConfiguration(migrationReport);
		migrationReport.setMigrationStatus(databaseCopyScheduler.getCurrentState(copyContext, OffsetDateTime.MAX));
		migrationReport.setDatabaseCopyTasks(databaseCopyTaskRepository.getAllTasks(copyContext));
		return migrationReport;
	}

	private void populateConfiguration(MigrationReport migrationReport) {
		final SortedMap<String, String> configuration = new TreeMap<>();
		final Configuration config = configurationService.getConfiguration();
		final Configuration subset = config.subset(PROPERTIES_PREFIX);
		final Set<String> maskedProperties = Arrays
				.stream(config.getString(MIGRATION_REPORT_MASKED_PROPERTIES).split(",")).collect(Collectors.toSet());

		final Iterator<String> keys = subset.getKeys();

		while (keys.hasNext()) {
			final String key = keys.next();
			final String prefixedKey = PROPERTIES_PREFIX + "." + key;

			if (MIGRATION_REPORT_MASKED_PROPERTIES.equals(prefixedKey)) {
				continue;
			}

			configuration.put(prefixedKey,
					maskedProperties.contains(prefixedKey) ? MASKED_VALUE : stripJdbcPassword(subset.getString(key)));
		}

		migrationReport.setConfiguration(configuration);
	}

	public void setDatabaseCopyScheduler(DatabaseCopyScheduler databaseCopyScheduler) {
		this.databaseCopyScheduler = databaseCopyScheduler;
	}

	public void setDatabaseCopyTaskRepository(DatabaseCopyTaskRepository databaseCopyTaskRepository) {
		this.databaseCopyTaskRepository = databaseCopyTaskRepository;
	}

	public void setConfigurationService(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}
}
