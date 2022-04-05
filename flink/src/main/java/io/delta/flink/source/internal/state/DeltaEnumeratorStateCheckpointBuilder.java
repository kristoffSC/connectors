package io.delta.flink.source.internal.state;

import java.util.Collection;
import java.util.Collections;

import io.delta.flink.source.internal.enumerator.processor.ContinuousTableProcessor;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaEnumeratorStateCheckpointBuilder<SplitT extends DeltaSourceSplit> {

    /**
     * {@link Path} to Delta Table used for this snapshot.
     */
    private final Path deltaTablePath;

    /**
     * The Delta Table snapshot version used to create this checkpoint.
     */
    private final long snapshotVersion;

    private final Collection<SplitT> splits;

    private Collection<Path> processedPaths = Collections.emptySet();

    /**
     * Flag indicating that source start monitoring Delta Table for changes.
     * <p>
     * This field is mapped from {@link ContinuousTableProcessor #isMonitoringForChanges()} method.
     */
    private boolean monitoringForChanges;

    // TODO PR 7 javadoc
    private long changesOffset;

    public DeltaEnumeratorStateCheckpointBuilder(
        Path deltaTablePath, long snapshotVersion, Collection<SplitT> splits) {
        this.deltaTablePath = deltaTablePath;
        this.snapshotVersion = snapshotVersion;
        this.splits = splits;
        this.monitoringForChanges = false;
        this.changesOffset = 0;
    }

    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpointBuilder<T>
        builder(Path deltaTablePath, long snapshotVersion, Collection<T> splits) {

        checkNotNull(deltaTablePath);
        checkNotNull(snapshotVersion);

        return new DeltaEnumeratorStateCheckpointBuilder<>(deltaTablePath, snapshotVersion, splits);
    }

    public DeltaEnumeratorStateCheckpointBuilder<SplitT> withProcessedPaths(
        Collection<Path> processedPaths) {
        this.processedPaths = processedPaths;
        return this;
    }

    public DeltaEnumeratorStateCheckpointBuilder<SplitT> withChangesOffset(long changesOffset) {
        this.changesOffset = changesOffset;
        return this;
    }

    public DeltaEnumeratorStateCheckpointBuilder<SplitT> withMonitoringForChanges(
        boolean monitoringForChanges) {
        this.monitoringForChanges = monitoringForChanges;
        return this;
    }

    public DeltaEnumeratorStateCheckpoint<SplitT> build() {
        PendingSplitsCheckpoint<SplitT> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, processedPaths);

        return new DeltaEnumeratorStateCheckpoint<>(deltaTablePath, snapshotVersion,
            monitoringForChanges, changesOffset, splitsCheckpoint);
    }
}
