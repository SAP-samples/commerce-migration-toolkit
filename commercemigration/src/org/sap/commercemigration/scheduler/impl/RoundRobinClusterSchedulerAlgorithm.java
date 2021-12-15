package org.sap.commercemigration.scheduler.impl;

import de.hybris.platform.cluster.PingBroadcastHandler;
import de.hybris.platform.servicelayer.cluster.ClusterService;
import org.apache.commons.collections4.CollectionUtils;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.scheduler.DatabaseCopySchedulerAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RoundRobinClusterSchedulerAlgorithm implements DatabaseCopySchedulerAlgorithm {

	private static final Logger LOG = LoggerFactory.getLogger(RoundRobinClusterSchedulerAlgorithm.class);

	private MigrationContext migrationContext;

	private ClusterService clusterService;

	private List<Integer> nodeIds = null;

	private int nodeIndex = 0;

	public RoundRobinClusterSchedulerAlgorithm(MigrationContext migrationContext, ClusterService clusterService) {
		this.migrationContext = migrationContext;
		this.clusterService = clusterService;
	}

	@Override
	public int getOwnNodeId() {
		return clusterService.getClusterId();
	}

	@Override
	public List<Integer> getNodeIds() {
		if (nodeIds == null) {
			nodeIds = detectClusterNodes();
		}
		return nodeIds;
	}

	@Override
	public int next() {
		if (nodeIndex >= (getNodeIds().size())) {
			nodeIndex = 0;
		}
		return getNodeIds().get(nodeIndex++);
	}

	public void reset() {
		nodeIds = null;
		nodeIndex = 0;
	}

	private List<Integer> detectClusterNodes() {
		if (!migrationContext.isClusterMode()) {
			return Collections.singletonList(clusterService.getClusterId());
		}
		final List<Integer> nodeIdList = new ArrayList<>();
		try {
			// Same code as the hac cluster overview page
			PingBroadcastHandler pingBroadcastHandler = PingBroadcastHandler.getInstance();
			pingBroadcastHandler.getNodes().forEach(i -> nodeIdList.add(i.getNodeID()));
		} catch (final Exception e) {
			LOG.warn(
					"Using single cluster node because an error was encountered while fetching cluster nodes information: {{}}",
					e.getMessage(), e);
		}
		if (CollectionUtils.isEmpty(nodeIdList)) {
			nodeIdList.add(clusterService.getClusterId());
		}
		return nodeIdList;
	}
}
