/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.concurrent.impl;

import org.sap.commercemigration.concurrent.DataThreadPoolMonitor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DefaultDataThreadPoolMonitor implements DataThreadPoolMonitor {

	private List<ThreadPoolTaskExecutor> executors;

	public DefaultDataThreadPoolMonitor() {
		this.executors = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void subscribe(ThreadPoolTaskExecutor executor) {
		executors.add(executor);
	}

	@Override
	public void unsubscribe(ThreadPoolTaskExecutor executor) {
		executors.remove(executor);
	}

	@Override
	public int getActiveCount() {
		return executors.stream().mapToInt(ThreadPoolTaskExecutor::getActiveCount).sum();
	}

	@Override
	public int getMaxPoolSize() {
		return executors.stream().mapToInt(ThreadPoolTaskExecutor::getMaxPoolSize).sum();
	}

}
