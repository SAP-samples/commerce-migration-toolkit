package org.sap.commercemigration.events;

/**
 * * ClusterAwareEvent to signal completion of the assigned copy ta
 */
public class CopyCompleteEvent extends CopyEvent {

    private Boolean copyResult = false;

    public CopyCompleteEvent(final Integer sourceNodeId, final String migrationId) {
        super(sourceNodeId, migrationId);
    }

    public Boolean getCopyResult() {
        return copyResult;
    }
}
