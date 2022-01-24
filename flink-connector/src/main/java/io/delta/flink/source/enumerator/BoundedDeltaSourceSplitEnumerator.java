package io.delta.flink.source.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.DeltaSourceException;
import io.delta.flink.source.DeltaSourceOptions;
import io.delta.flink.source.DeltaSourceSplitEnumerator;
import io.delta.flink.source.file.AddFileEnumerator;
import io.delta.flink.source.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.file.AddFileEnumeratorContext;
import io.delta.flink.source.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class BoundedDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            sourceOptions, NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions,
        long initialSnapshotVersion, Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, configuration, enumContext, sourceOptions,
            initialSnapshotVersion, alreadyDiscoveredPaths);
        this.fileEnumerator = fileEnumerator;
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        try {
            AddFileEnumeratorContext context = setUpEnumeratorContext(snapshot.getAllFiles());
            List<DeltaSourceSplit> splits = fileEnumerator
                .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
            addSplits(splits);
        } catch (Exception e) {
            throw new DeltaSourceException(e);
        }
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, initialSnapshotVersion, getRemainingSplits(),
            pathsAlreadyProcessed);
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
