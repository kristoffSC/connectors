package io.delta.flink.source.state;

import java.util.Collection;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaEnumeratorStateCheckpoint<SplitT extends DeltaSourceSplit> {

    private final Path deltaTablePath;

    private final long initialSnapshotVersion;

    private final PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint;

    private DeltaEnumeratorStateCheckpoint(Path deltaTablePath, long initialSnapshotVersion,
        PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint) {
        this.deltaTablePath = deltaTablePath;
        this.initialSnapshotVersion = initialSnapshotVersion;
        this.pendingSplitsCheckpoint = pendingSplitsCheckpoint;
    }

    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpoint<T>
        fromCollectionSnapshot(Path deltaTablePath, long initialSnapshotVersion,
        Collection<T> splits) {
        checkNotNull(deltaTablePath);
        checkNotNull(initialSnapshotVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits);

        return new DeltaEnumeratorStateCheckpoint<>(deltaTablePath, initialSnapshotVersion,
            splitsCheckpoint);
    }

    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpoint<T>
        fromCollectionSnapshot(
        Path deltaTablePath, long initialSnapshotVersion, Collection<T> splits,
        Collection<Path> alreadyProcessedPaths) {

        checkNotNull(deltaTablePath);
        checkNotNull(initialSnapshotVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, alreadyProcessedPaths);

        return new DeltaEnumeratorStateCheckpoint<>(deltaTablePath, initialSnapshotVersion,
            splitsCheckpoint);
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

    public Path getDeltaTablePath() {
        return deltaTablePath;
    }
}
