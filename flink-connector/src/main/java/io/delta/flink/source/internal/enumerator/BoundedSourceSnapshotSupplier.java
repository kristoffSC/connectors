package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.utils.TransitiveOptional;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

public class BoundedSourceSnapshotSupplier extends SnapshotSupplier {

    public BoundedSourceSnapshotSupplier(DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        super(deltaLog, sourceConfiguration);
    }

    public Snapshot getSnapshot() {
        return getSnapshotFromVersionAsOfOption()
            .or(this::getSnapshotFromTimestampAsOfOption)
            .or(this::getHeadSnapshot)
            .get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromVersionAsOfOption() {
        Long versionAsOf = getOptionValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption() {
        Long timestampAsOf = getOptionValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf));
        }
        return TransitiveOptional.empty();
    }
}
