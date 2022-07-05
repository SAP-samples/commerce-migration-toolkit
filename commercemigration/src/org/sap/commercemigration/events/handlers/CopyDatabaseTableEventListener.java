/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.events.handlers;

import de.hybris.platform.servicelayer.cluster.ClusterService;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.events.CopyDatabaseTableEvent;
import org.sap.commercemigration.performance.PerformanceProfiler;
import org.sap.commercemigration.service.DatabaseCopyTask;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.sap.commercemigration.service.DatabaseMigrationCopyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.sap.commercemigration.constants.CommercemigrationConstants.MDC_CLUSTERID;
import static org.sap.commercemigration.constants.CommercemigrationConstants.MDC_MIGRATIONID;

/**
 * Listener that starts the Migration Process on a given node
 */
public class CopyDatabaseTableEventListener extends AbstractEventListener<CopyDatabaseTableEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyDatabaseTableEventListener.class.getName());

	private DatabaseMigrationCopyService databaseMigrationCopyService;

	private DatabaseCopyTaskRepository databaseCopyTaskRepository;

	private MigrationContext migrationContext;

	private PerformanceProfiler performanceProfiler;

	private ClusterService clusterService;

	private ConfigurationService configurationService;

	@Override
	protected void onEvent(final CopyDatabaseTableEvent event) {
		final String migrationId = event.getMigrationId();
		processPropertyOverrides(event.getPropertyOverrideMap());
		LOG.debug("Starting Migration with Id {}", migrationId);
		try (MDC.MDCCloseable ignored = MDC.putCloseable(MDC_MIGRATIONID, migrationId);
				MDC.MDCCloseable ignored2 = MDC.putCloseable(MDC_CLUSTERID,
						String.valueOf(clusterService.getClusterId()))) {
			CopyContext copyContext = new CopyContext(migrationId, migrationContext, new HashSet<>(),
					performanceProfiler);
			Set<DatabaseCopyTask> copyTableTasks = databaseCopyTaskRepository.findPendingTasks(copyContext);
			Set<CopyContext.DataCopyItem> items = copyTableTasks
					.stream().map(task -> new CopyContext.DataCopyItem(task.getSourcetablename(),
							task.getTargettablename(), task.getColumnmap(), task.getSourcerowcount()))
					.collect(Collectors.toSet());
			copyContext.getCopyItems().addAll(items);
			databaseMigrationCopyService.copyAllAsync(copyContext);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void processPropertyOverrides(Map<String, Serializable> propertyOverrideMap) {
		propertyOverrideMap.forEach((key, value) -> {
			configurationService.getConfiguration().setProperty(key, String.valueOf(value));
		});
	}

	public void setDatabaseMigrationCopyService(final DatabaseMigrationCopyService databaseMigrationCopyService) {
		this.databaseMigrationCopyService = databaseMigrationCopyService;
	}

	public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
		this.databaseCopyTaskRepository = databaseCopyTaskRepository;
	}

	public void setMigrationContext(final MigrationContext migrationContext) {
		this.migrationContext = migrationContext;
	}

	public void setPerformanceProfiler(final PerformanceProfiler performanceProfiler) {
		this.performanceProfiler = performanceProfiler;
	}

	@Override
	public void setClusterService(ClusterService clusterService) {
		super.setClusterService(clusterService);
		this.clusterService = clusterService;
	}

	public void setConfigurationService(ConfigurationService configurationService) {
		this.configurationService = configurationService;
	}
}
