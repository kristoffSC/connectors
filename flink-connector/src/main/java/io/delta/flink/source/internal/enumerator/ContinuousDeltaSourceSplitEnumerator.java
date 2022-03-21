package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;

/**
 * A SplitEnumerator implementation for continuous {@link io.delta.flink.source.DeltaSource} mode.
 *
 * <p>This enumerator takes all files that are present in the configured Delta Table directory,
 * convert them to {@link DeltaSourceSplit} and assigns them to the readers. Once all files from
 * initial snapshot are processed, {@code ContinuousDeltaSourceSplitEnumerator} starts monitoring
 * Delta Table for changes. Each appending data change is converted to {@code DeltaSourceSplit} and
 * assigned to readers.
 * <p>
 * <p>
 * If {@link io.delta.flink.source.internal.DeltaSourceOptions#STARTING_VERSION} or {@link
 * io.delta.flink.source.internal.DeltaSourceOptions#STARTING_TIMESTAMP} option is defined, the
 * enumerator will not read the initial snapshot content but will read only changes, starting from
 * snapshot defined by those options.
 *
 * <p>The actual logic for creating the set of {@link DeltaSourceSplit} to process, and the logic
 * to decide which reader gets what split can be found {@link DeltaSourceSplitEnumerator} and in
 * {@link FileSplitAssigner}, respectively.
 */
public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private final ContinuousTableProcessor continuousTableProcessor;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, ContinuousTableProcessor continuousTableProcessor,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        super(deltaTablePath, splitAssigner, enumContext);

        this.continuousTableProcessor = continuousTableProcessor;
    }

    @Override
    public void start() {
        continuousTableProcessor.process(deltaSourceSplits -> {
            addSplits(deltaSourceSplits);
            assignSplits();
        });
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, continuousTableProcessor.getSnapshotVersion(),
            continuousTableProcessor.isMonitoringForChanges(), getRemainingSplits(),
            continuousTableProcessor.getAlreadyProcessedPaths());
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        // We should do nothing, since we are continuously monitoring Delta Table.
    }
}
