package io.delta.flink.source;

import java.util.Collection;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaPendingSplitsCheckpoint<SplitT extends DeltaSourceSplit> {

    private final long initialSnapshotVersion;

    private final PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint;

    private DeltaPendingSplitsCheckpoint(long initialSnapshotVersion,
        PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint) {
        this.initialSnapshotVersion = initialSnapshotVersion;
        this.pendingSplitsCheckpoint = pendingSplitsCheckpoint;
    }

    public static <T extends DeltaSourceSplit> DeltaPendingSplitsCheckpoint<T>
        fromCollectionSnapshot(final long initialSnapshotVersion, final Collection<T> splits) {
        checkNotNull(initialSnapshotVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits);

        return new DeltaPendingSplitsCheckpoint<>(initialSnapshotVersion, splitsCheckpoint);
    }

    public static <T extends DeltaSourceSplit> DeltaPendingSplitsCheckpoint<T>
        fromCollectionSnapshot(
        final long initialSnapshotVersion, final Collection<T> splits,
        final Collection<Path> alreadyProcessedPaths) {
        checkNotNull(initialSnapshotVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, alreadyProcessedPaths);

        return new DeltaPendingSplitsCheckpoint<>(initialSnapshotVersion, splitsCheckpoint);
    }

    public long getInitialSnapshotVersion() {
        return initialSnapshotVersion;
    }

    public Collection<SplitT> getSplits() {
        return pendingSplitsCheckpoint.getSplits();
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    public Collection<Path> getAlreadyProcessedPaths() {
        return pendingSplitsCheckpoint.getAlreadyProcessedPaths();
    }

    // Package protected For (De)Serializer only
    PendingSplitsCheckpoint<SplitT> getPendingSplitsCheckpoint() {
        return pendingSplitsCheckpoint;
    }
}
