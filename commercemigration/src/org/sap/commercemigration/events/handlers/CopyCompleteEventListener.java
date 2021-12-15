package org.sap.commercemigration.events.handlers;

import de.hybris.platform.servicelayer.event.impl.AbstractEventListener;
import de.hybris.platform.tx.Transaction;
import de.hybris.platform.tx.TransactionBody;
import org.sap.commercemigration.MigrationProgress;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.events.CopyCompleteEvent;
import org.sap.commercemigration.performance.PerformanceProfiler;
import org.sap.commercemigration.processors.MigrationPostProcessor;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;

/**
 * Receives an Event when a node has completed Copying Data Tasks
 */
public class CopyCompleteEventListener extends AbstractEventListener<CopyCompleteEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(CopyCompleteEventListener.class.getName());

	private MigrationContext migrationContext;

	private DatabaseCopyTaskRepository databaseCopyTaskRepository;

	private PerformanceProfiler performanceProfiler;

	private List<MigrationPostProcessor> postProcessors;

	@Override
	protected void onEvent(CopyCompleteEvent event) {
		final String migrationId = event.getMigrationId();

		LOG.info("Migration finished on Node {} with result {}", event.getSourceNodeId(), event.getCopyResult());
		CopyContext copyContext = new CopyContext(migrationId, migrationContext, new HashSet<>(), performanceProfiler);

		executePostProcessors(copyContext);
	}

	/**
	 * Runs through all the Post Processors in a transaction to avoid multiple
	 * executions
	 *
	 * @param copyContext
	 */
	private void executePostProcessors(final CopyContext copyContext) {
		try {
			Transaction.current().execute(new TransactionBody() {
				@Override
				public Object execute() throws Exception {

					MigrationStatus status = databaseCopyTaskRepository.getMigrationStatus(copyContext);

					if (status.isFailed()) {
						return null;
					}

					LOG.debug("Starting PostProcessor execution");

					if (status.getStatus() == MigrationProgress.PROCESSED) {
						postProcessors.forEach(p -> p.process(copyContext));
					}
					LOG.debug("Finishing PostProcessor execution");

					databaseCopyTaskRepository.setMigrationStatus(copyContext, MigrationProgress.PROCESSED,
							MigrationProgress.COMPLETED);
					return null;
				}
			});
		} catch (final Exception e) {
			LOG.error("Error during PostProcessor execution", e);
			throw new RuntimeException(e);
		}
	}

	public void setDatabaseCopyTaskRepository(final DatabaseCopyTaskRepository databaseCopyTaskRepository) {
		this.databaseCopyTaskRepository = databaseCopyTaskRepository;
	}

	public void setMigrationContext(final MigrationContext migrationContext) {
		this.migrationContext = migrationContext;
	}

	public void setPerformanceProfiler(PerformanceProfiler performanceProfiler) {
		this.performanceProfiler = performanceProfiler;
	}

	public void setPostProcessors(List<MigrationPostProcessor> postProcessors) {
		this.postProcessors = postProcessors;
	}
}
