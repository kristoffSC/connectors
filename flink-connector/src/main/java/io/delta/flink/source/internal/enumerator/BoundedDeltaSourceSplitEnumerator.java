package io.delta.flink.source.internal.enumerator;

import java.util.List;

import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;

/**
 * A SplitEnumerator implementation for bounded/batch {@link io.delta.flink.source.DeltaSource}
 * mode.
 *
 * <p>This enumerator takes all files that are present in the configured Delta Table directory,
 * converts them to {@link DeltaSourceSplit} and assigns them to the readers. Once all files are
 * processed, the source is finished.
 *
 * <p>The actual logic for creating the set of
 * {@link DeltaSourceSplit} to process, and the logic to decide which reader gets what split can be
 * found {@link DeltaSourceSplitEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
public class BoundedDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private final SnapshotProcessor snapshotProcessor;

    private BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, SnapshotProcessor snapshotProcessor,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        super(deltaTablePath, splitAssigner, enumContext);
        this.snapshotProcessor = snapshotProcessor;
    }

    public static BoundedDeltaSourceSplitEnumerator create(
        Path deltaTablePath, SnapshotProcessor snapshotProcessor, FileSplitAssigner splitAssigner,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        return new BoundedDeltaSourceSplitEnumerator(deltaTablePath, snapshotProcessor,
            splitAssigner, enumContext);
    }

    @Override
    public void start() {
        List<DeltaSourceSplit> splits = snapshotProcessor.process();
        addSplits(splits);
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, snapshotProcessor.getSnapshotVersion(), false, getRemainingSplits(),
            snapshotProcessor.getAlreadyProcessedPaths());
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
