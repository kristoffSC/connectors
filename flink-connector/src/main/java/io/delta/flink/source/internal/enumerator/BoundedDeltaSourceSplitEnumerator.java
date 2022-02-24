package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
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
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            sourceConfiguration, NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration,
        long initialSnapshotVersion, Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, configuration, enumContext, sourceConfiguration,
            initialSnapshotVersion, alreadyDiscoveredPaths);
        this.fileEnumerator = fileEnumerator;
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        AddFileEnumeratorContext context =
            setUpEnumeratorContext(snapshot.getAllFiles(), snapshot.getVersion());
        List<DeltaSourceSplit> splits = fileEnumerator
            .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
        addSplits(splits);
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
     * or {@code DeltaSourceConfiguration}
     */
    @Override
    protected Snapshot getInitialSnapshot(long checkpointSnapshotVersion) {

        // Prefer version from checkpoint over the other ones.
        return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
            .or(this::getSnapshotFromVersionAsOfOption)
            .or(this::getSnapshotFromTimestampAsOfOption)
            .or(this::getHeadSnapshot)
            .get();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }

    private TransitiveOptional<Snapshot> getSnapshotFromVersionAsOfOption() {
        Long versionAsOf = sourceConfiguration.getValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption() {
        Long timestampAsOf = sourceConfiguration.getValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf));
        }
        return TransitiveOptional.empty();
    }
}
