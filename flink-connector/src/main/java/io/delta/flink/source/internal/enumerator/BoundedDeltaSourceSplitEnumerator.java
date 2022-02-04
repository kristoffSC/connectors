package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.Snapshot;

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
    protected Snapshot getInitialSnapshot(long checkpointSnapshotVersion) {

        // TODO test all those options
        // Prefer version from checkpoint over other ones.
        return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
            .or(this::getSnapshotFromVersionAsOfOption)
            .or(this::getSnapshotFromTimestampAsOfOption)
            .or(this::getHeadSnapshot)
            .get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromVersionAsOfOption() {
        Long versionAsOf = sourceOptions.getValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption() {
        Long timestampAsOf = sourceOptions.getValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf));
        }
        return TransitiveOptional.empty();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
