package io.delta.flink.source.internal.state;

import java.util.Collection;

import io.delta.flink.source.internal.exceptions.DeltaSourceExceptionUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint of the current state of
 * {@link SplitEnumerator}.
 *
 * <p>It contains all necessary information need by SplitEnumerator to resume work after
 * checkpoint recovery including currently pending splits that are not yet assigned and
 * resume changes discovery task on Delta Table in {@link Boundedness#CONTINUOUS_UNBOUNDED} mode</p>
 *
 * <p>During checkpoint, Flink will serialize this object and persist it in checkpoint location.
 * During the recovery, Flink will deserialize this object from Checkpoint/Savepoint
 * and will use it to recreate {@code SplitEnumerator}.
 */
public class DeltaEnumeratorStateCheckpoint<SplitT extends DeltaSourceSplit> {

    /**
     * {@link Path} to Delta Table used for this snapshot.
     */
    private final Path deltaTablePath;

    /**
     * The initial version of Delta Table from witch we started reading the Delta Table.
     */
    private final long initialSnapshotVersion;

    /**
     * The Delta Table snapshot version at moment when snapshot was taken.
     */
    private final long currentTableVersion;

    /**
     * Decorated {@link PendingSplitsCheckpoint} that keeps details about checkpointed splits in
     * enumerator.
     */
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
            DeltaSourceExceptionUtils.generalSourceException(e);
        }
    }

    /**
     * @return The initial version of Delta Table from witch we started reading the Delta Table.
     */
    public long getInitialSnapshotVersion() {
        return initialSnapshotVersion;
    }

    /**
     * @return The checkpointed {@link DeltaSourceSplit} that were not yet assigned to file readers.
     */
    public Collection<SplitT> getSplits() {
        return pendingSplitsCheckpoint.getSplits();
    }

    /**
     * @return The paths that are no longer in the enumerator checkpoint, but have been processed
     * before and should be ignored.
     */
    public Collection<Path> getAlreadyProcessedPaths() {
        return pendingSplitsCheckpoint.getAlreadyProcessedPaths();
    }

    /**
     * @return {@link Path} to Delta Table used for this snapshot.
     */
    public Path getDeltaTablePath() {
        return deltaTablePath;
    }

    /**
     * @return The Delta Table snapshot version at moment when snapshot was taken.
     */
    public long getCurrentTableVersion() {
        return currentTableVersion;
    }

    // Package protected For (De)Serializer only
    PendingSplitsCheckpoint<SplitT> getPendingSplitsCheckpoint() {
        return pendingSplitsCheckpoint;
    }
}
