package org.sap.commercemigration.concurrent.impl;

import org.sap.commercemigration.concurrent.DataWorkerPoolFactory;
import org.sap.commercemigration.context.CopyContext;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class DefaultDataWorkerPoolFactory implements DataWorkerPoolFactory {

    private TaskDecorator taskDecorator;
    private String threadNamePrefix;
    private int corePoolSize;
    private int maxPoolSize;
    private int keepAliveSeconds;
    private int queueCapacity = 2147483647;

    public DefaultDataWorkerPoolFactory(TaskDecorator taskDecorator, String threadNamePrefix, int maxPoolSize, int keepAliveSeconds, boolean queueable) {
        this.taskDecorator = taskDecorator;
        this.threadNamePrefix = threadNamePrefix;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveSeconds = keepAliveSeconds;
        this.queueCapacity = queueable ? this.queueCapacity : 0;
        this.corePoolSize = maxPoolSize;
    }

    @Override
    public ThreadPoolTaskExecutor create(CopyContext context) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setTaskDecorator(taskDecorator);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setKeepAliveSeconds(keepAliveSeconds);
        executor.setAllowCoreThreadTimeOut(true);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(Integer.MAX_VALUE);
        executor.initialize();
        return executor;
    }

}
