package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SnapshotSupplier} for {#link
 * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}}
 * mode.
 */
public class ContinuousSourceSnapshotSupplier extends SnapshotSupplier {

    public ContinuousSourceSnapshotSupplier(DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        super(deltaLog, sourceConfiguration);
    }

    /**
     * This method returns a {@link Snapshot} instance acquired from {@link #deltaLog}. This
     * implementation tries to query the {@code Snapshot} in below order, stopping at first
     * non-empty result:
     * <ul>
     *     <li>If {@link DeltaSourceOptions#STARTING_VERSION} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForVersionAsOf(long)}.</li>
     *     <li>If {@link DeltaSourceOptions#STARTING_TIMESTAMP} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForTimestampAsOf(long)}.</li>
     *     <li>Get the head version using {@link DeltaLog#snapshot()}</li>
     * </ul>
     *
     * @return A {@link Snapshot} instance or throws {@link java.util.NoSuchElementException} if no
     * snapshot was found.
     */
    @Override
    public Snapshot getSnapshot() {
        //.or(this::getSnapshotFromStartingVersionOption) // TODO Add in PR 7
        //.or(this::getSnapshotFromStartingTimestampOption) // TODO Add in PR 7
        return getHeadSnapshot()
            .get();
    }
}
