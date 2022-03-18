package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.DeltaSourceConfiguration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

public class ContinuousSourceSnapshotSupplier extends SnapshotSupplier {

    public ContinuousSourceSnapshotSupplier(DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        super(deltaLog, sourceConfiguration);
    }

    @Override
    public Snapshot getSnapshot() {
        //.or(this::getSnapshotFromStartingVersionOption) // TODO Add in PR 7
        //.or(this::getSnapshotFromStartingTimestampOption) // TODO Add in PR 7
        return getHeadSnapshot()
            .get();
    }
}
