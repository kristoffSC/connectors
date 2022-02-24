package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptionUtils;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.Snapshot;

/**
 * A SplitEnumerator implementation for bounded/batch {@link io.delta.flink.source.DeltaSource}
 * mode.
 *
 * <p>This enumerator takes all files that are present in the configured input directory and
 * assigns them to the readers. Once all files are processed, the source is finished.
 *
 * <p>The implementation of this class is rather thin. The actual logic for creating the set of
 * {@link DeltaSourceSplit} to process, and the logic to decide which reader gets what split can be
 * found {@link DeltaSourceSplitEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
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
            DeltaSourceExceptionUtils.generalSourceException(e);
        }
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, initialSnapshotVersion, getRemainingSplits(),
            pathsAlreadyProcessed);
    }

    /**
     * The implementation of this method encapsulates the initial snapshot creation logic.
     * <p>
     * This method is called from {@code DeltaSourceSplitEnumerator} constructor during object
     * initialization.
     *
     * @param checkpointSnapshotVersion version of snapshot from checkpoint. If the value is equal
     *                                  to {@link #NO_SNAPSHOT_VERSION} it means that this is the
     *                                  first Source initialization and not a recovery from a
     *                                  Flink's checkpoint.
     * @return A {@link Snapshot} that will be used as an initial Delta Table {@code Snapshot} to
     * read data from.
     *
     * <p>
     * <p>
     * @implNote We have 2 cases:
     * <ul>
     *      <li>
     *          checkpointSnapshotVersion is -1. This is either the initial setup of the source,
     *          or we are recovering from failure yet no checkpoint was found.
     *      </li>
     *      <li>
     *          checkpointSnapshotVersion is not -1. We are recovering from failure and a
     *          checkpoint was found. Thus, this checkpointSnapshotVersion is the version we
     *          should load.
     *      </li>
     * </ul>
     * <p>
     * If a specific versionAsOf/timestampAsOf option is set, we will use that for initial setup
     * of the source. In case of recovery, if there is a checkpoint available to recover from,
     * the checkpointSnapshotVersion will be set to versionAsOf/timestampAsOf snapshot version
     * by Flink using {@link io.delta.flink.source.DeltaSource#restoreEnumerator(
     *SplitEnumeratorContext, DeltaEnumeratorStateCheckpoint)} method.
     * <p>
     * <p>
     * <p>
     * Option's mutual exclusion must be guaranteed by other classes like {@code DeltaSourceBuilder}
     * or {@code DeltaSourceOptions}
     */
    @Override
    protected Snapshot getInitialSnapshot(long checkpointSnapshotVersion) {

        // TODO test all those options PR 5
        // Prefer version from checkpoint over the other ones.
        return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
            //.or(this::getSnapshotFromVersionAsOfOption) // TODO Add in PR 5
            //.or(this::getSnapshotFromTimestampAsOfOption) // TODO Add in PR 5
            .or(this::getHeadSnapshot)
            .get();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
