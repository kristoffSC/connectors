package io.delta.flink.source.internal.state;

import java.util.Collection;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint of the current state of {@link SplitEnumerator}.
 *
 * <p>It contains all necessary information need by SplitEnumerator to resume work after
 * checkpoint recovery including currently pending splits that are not yet assigned and resume
 * changes discovery task on Delta Table in {@link Boundedness#CONTINUOUS_UNBOUNDED} mode</p>
 *
 * <p>During checkpoint, Flink will serialize this object and persist it in checkpoint location.
 * During the recovery, Flink will deserialize this object from Checkpoint/Savepoint and will use it
 * to recreate {@code SplitEnumerator}.
 *
 * @param <SplitT> The concrete type of {@link SourceSplit} that is kept in @param * splits
 *                 collection.
 */
public class DeltaEnumeratorStateCheckpoint<SplitT extends DeltaSourceSplit> {

    /**
     * {@link Path} to Delta Table used for this snapshot.
     */
    private final Path deltaTablePath;

    /**
     * The Delta Table snapshot version used to create this checkpoint.
     */
    private final long snapshotVersion;

    /**
     * Flag indicating that source start monitoring Delta Table for changes.
     * <p>
     * This field is mapped from
     * {@link io.delta.flink.source.internal.enumerator.ContinuousTableProcessor
     * #isMonitoringForChanges()} method.
     */
    private final boolean monitoringForChanges;

    /**
     * Decorated {@link PendingSplitsCheckpoint} that keeps details about checkpointed splits in
     * enumerator.
     */
    private final PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint;

    private DeltaEnumeratorStateCheckpoint(Path deltaTablePath,
        long snapshotVersion, boolean monitoringForChanges,
        PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint) {
        this.deltaTablePath = deltaTablePath;
        this.snapshotVersion = snapshotVersion;
        this.monitoringForChanges = monitoringForChanges;
        this.pendingSplitsCheckpoint = pendingSplitsCheckpoint;
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    /**
     * A factory method for creating {@code DeltaEnumeratorStateCheckpoint} from given parameters
     * including split and already process paths collections.
     *
     * @param deltaTablePath        A {@link Path} to Delta Table.
     * @param snapshotVersion       The initial version of Delta Table from which we started reading
     *                              the Delta Table.
     * @param monitoringForChanges  indicates whether source started monitoring Delta table for
     *                              changes.
     * @param splits                A collection of splits that were unassigned to any readers at
     *                              moment of taking the checkpoint.
     * @param alreadyProcessedPaths The paths to Parquet files that have already been processed and
     *                              thus can be ignored during recovery.
     * @return DeltaEnumeratorStateCheckpoint for given T Split type.
     */
    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpoint<T>
        fromCollectionSnapshot(
        Path deltaTablePath, long snapshotVersion, boolean monitoringForChanges,
        Collection<T> splits, Collection<Path> alreadyProcessedPaths) {

        checkNotNull(deltaTablePath);
        checkNotNull(snapshotVersion);

        PendingSplitsCheckpoint<T> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, alreadyProcessedPaths);

        return new DeltaEnumeratorStateCheckpoint<>(
            deltaTablePath, snapshotVersion, monitoringForChanges, splitsCheckpoint);
    }

    // ------------------------------------------------------------------------

    /**
     * @return The initial version of Delta Table from witch we started reading the Delta Table.
     */
    public long getSnapshotVersion() {
        return snapshotVersion;
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
     * @return Boolean flag indicating that {@code DeltaSourceSplitEnumerator} started monitoring
     * for changes on Delta Table.
     */
    public boolean isMonitoringForChanges() {
        return monitoringForChanges;
    }

    // Package protected For (De)Serializer only
    PendingSplitsCheckpoint<SplitT> getPendingSplitsCheckpoint() {
        return pendingSplitsCheckpoint;
    }
}
