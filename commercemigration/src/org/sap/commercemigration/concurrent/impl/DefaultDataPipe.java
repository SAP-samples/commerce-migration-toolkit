package org.sap.commercemigration.concurrent.impl;

import org.sap.commercemigration.concurrent.DataPipe;
import org.sap.commercemigration.concurrent.MaybeFinished;
import org.sap.commercemigration.concurrent.PipeAbortedException;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.scheduler.DatabaseCopyScheduler;
import org.sap.commercemigration.service.DatabaseCopyTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultDataPipe<T> implements DataPipe<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataPipe.class);

    private final BlockingQueue<MaybeFinished<T>> queue;
    private final int defaultTimeout;
    private final AtomicReference<Exception> abortException = new AtomicReference<>();
    private final CopyContext context;
    private final CopyContext.DataCopyItem copyItem;
    private final DatabaseCopyTaskRepository taskRepository;
    private final DatabaseCopyScheduler scheduler;

    public DefaultDataPipe(DatabaseCopyScheduler scheduler, DatabaseCopyTaskRepository taskRepository, CopyContext context, CopyContext.DataCopyItem copyItem, int timeoutInSeconds, int capacity) {
        this.taskRepository = taskRepository;
        this.scheduler = scheduler;
        this.context = context;
        this.copyItem = copyItem;
        this.queue = new ArrayBlockingQueue<>(capacity);
        defaultTimeout = timeoutInSeconds;
    }

    @Override
    public void requestAbort(Exception cause) {
        if (this.abortException.compareAndSet(null, cause)) {
            if (context.getMigrationContext().isFailOnErrorEnabled()) {
                try {
                    scheduler.abort(context);
                } catch (Exception ex) {
                    LOG.warn("could not abort", ex);
                }
            }
            try {
                taskRepository.markTaskFailed(context, copyItem, cause);
            } catch (Exception e) {
                LOG.warn("could not update error status!", e);
            }
            try {
                this.queue.offer(MaybeFinished.poison(), defaultTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Could not flush pipe with poison", e);
            }
        }
    }

    private boolean isAborted() throws Exception {
        if (this.abortException.get() == null && scheduler.isAborted(this.context)) {
            this.requestAbort(new PipeAbortedException("Migration aborted"));
        }
        return this.abortException.get() != null;
    }

    @Override
    public void put(MaybeFinished<T> value) throws Exception {
        if (isAborted()) {
            throw new PipeAbortedException("pipe aborted", this.abortException.get());
        }
        if (!queue.offer(value, defaultTimeout, TimeUnit.SECONDS)) {
            throw new RuntimeException("cannot put new item in time");
        }
    }

    @Override
    public MaybeFinished<T> get() throws Exception {
        if (isAborted()) {
            throw new PipeAbortedException("pipe aborted", this.abortException.get());
        }
        MaybeFinished<T> element = queue.poll(defaultTimeout, TimeUnit.SECONDS);
        if (isAborted()) {
            throw new PipeAbortedException("pipe aborted", this.abortException.get());
        }
        if (element == null) {
            throw new RuntimeException(String.format("cannot get new item in time. Consider increasing the value of the property '%s' or '%s'", CommercemigrationConstants.MIGRATION_DATA_PIPE_TIMEOUT, CommercemigrationConstants.MIGRATION_DATA_PIPE_CAPACITY));
        }
        return element;
    }
}
