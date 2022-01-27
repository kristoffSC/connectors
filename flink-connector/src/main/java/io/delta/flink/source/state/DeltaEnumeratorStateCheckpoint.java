package io.delta.flink.source.state;

import java.util.Collection;

import io.delta.flink.source.exceptions.DeltaSourceException;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaEnumeratorStateCheckpoint<SplitT extends DeltaSourceSplit> {

    private final Path deltaTablePath;

    private final long initialSnapshotVersion;

    private final long currentTableVersion;

    private final PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint;

    private DeltaEnumeratorStateCheckpoint(Path deltaTablePath,
        long initialSnapshotVersion, long currentTableVersion,
        PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint) {
        this.deltaTablePath = deltaTablePath;
        this.initialSnapshotVersion = initialSnapshotVersion;
        this.currentTableVersion = currentTableVersion;
        this.pendingSplitsCheckpoint = pendingSplitsCheckpoint;
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------
    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpoint<T>
        fromCollectionSnapshot(
        Path deltaTablePath, long initialSnapshotVersion, long currentTableVersion,
        Collection<T> splits) {

        checkArguments(deltaTablePath, initialSnapshotVersion, currentTableVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits);

        return new DeltaEnumeratorStateCheckpoint<>(
            deltaTablePath, initialSnapshotVersion, currentTableVersion, splitsCheckpoint);
    }

    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpoint<T>
        fromCollectionSnapshot(
        Path deltaTablePath, long initialSnapshotVersion, long currentTableVersion,
        Collection<T> splits, Collection<Path> alreadyProcessedPaths) {

        checkArguments(deltaTablePath, initialSnapshotVersion, currentTableVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, alreadyProcessedPaths);

        return new DeltaEnumeratorStateCheckpoint<>(
            deltaTablePath, initialSnapshotVersion, currentTableVersion, splitsCheckpoint);
    }

    // ------------------------------------------------------------------------

    private static void checkArguments(Path deltaTablePath, long initialSnapshotVersion,
        long currentTableVersion) {
        try {
            checkNotNull(deltaTablePath);
            checkNotNull(initialSnapshotVersion);
            checkNotNull(currentTableVersion);
            checkArgument(currentTableVersion >= initialSnapshotVersion,
                "CurrentTableVersion must be equal or higher than initialSnapshotVersion ");
        } catch (Exception e) {
            throw new DeltaSourceException(e);
        }
    }

    public long getInitialSnapshotVersion() {
        return initialSnapshotVersion;
    }

    public Collection<SplitT> getSplits() {
        return pendingSplitsCheckpoint.getSplits();
    }

    public Collection<Path> getAlreadyProcessedPaths() {
        return pendingSplitsCheckpoint.getAlreadyProcessedPaths();
    }

    public Path getDeltaTablePath() {
        return deltaTablePath;
    }

    public long getCurrentTableVersion() {
        return currentTableVersion;
    }

    // Package protected For (De)Serializer only
    PendingSplitsCheckpoint<SplitT> getPendingSplitsCheckpoint() {
        return pendingSplitsCheckpoint;
    }
}
