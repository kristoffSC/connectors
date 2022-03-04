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
 * <p>This enumerator takes all files that are present in the configured Delta Table directory,
 * converts them to {@link DeltaSourceSplit} and assigns them to the readers. Once all files are
 * processed, the source is finished.
 *
 * <p>The actual logic for creating the set of
 * {@link DeltaSourceSplit} to process, and the logic to decide which reader gets what split can be
 * found {@link DeltaSourceSplitEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
public class BoundedDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    /**
     * @param initialSnapshotVersionHint this parameter is used by {@link #getInitialSnapshot(long)}
     *                                   method to initilize the initial {@link Snapshot}. From that
     *                                   snapshot the {@link #initialSnapshotVersion} field is set
     *                                   by calling {@code snapshot.vetVersion()}.
     */
    private BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration,
        long initialSnapshotVersionHint, Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, fileEnumerator, configuration, enumContext,
            sourceConfiguration, initialSnapshotVersionHint, alreadyDiscoveredPaths);
    }

    /**
     * Method used to create a new {@link BoundedDeltaSourceSplitEnumerator} instance. The
     * instance created with this method will have its Delta {@link Snapshot} created from version:
     * <ul>
     *     <li>Specified by versionAsOf/timestampAsOf options.</li>
     *     <li>The Delta Table current head version at the time this object was created.</li>
     * </ul>
     */
    public static BoundedDeltaSourceSplitEnumerator create(Path deltaTablePath,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator, FileSplitAssigner splitAssigner,
        Configuration configuration, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        return new BoundedDeltaSourceSplitEnumerator(deltaTablePath, fileEnumerator, splitAssigner,
            configuration, enumContext, sourceConfiguration, NO_SNAPSHOT_VERSION,
            Collections.emptySet());
    }

    /**
     * Method used to create a new {@link BoundedDeltaSourceSplitEnumerator} instance using Flink's
     * checkpoint data from {@link DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>} such as, path
     * to Delta Table, initialSnapshotVersion or collection of already processed paths.
     * <p>
     * This method should be used when recovering from checkpoint, for example in {@link
     * org.apache.flink.api.connector.source.Source#restoreEnumerator(SplitEnumeratorContext,
     * Object)} method implementation.
     */
    public static BoundedDeltaSourceSplitEnumerator createForCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator, FileSplitAssigner splitAssigner,
        Configuration configuration, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        return new BoundedDeltaSourceSplitEnumerator(checkpoint.getDeltaTablePath(), fileEnumerator,
            splitAssigner, configuration, enumContext, sourceConfiguration,
            checkpoint.getInitialSnapshotVersion(), checkpoint.getAlreadyProcessedPaths());
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        AddFileEnumeratorContext context =
            setUpEnumeratorContext(initialSnapshot.getAllFiles(), initialSnapshot.getVersion());
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
     * The implementation of this method encapsulates the initial {@link Snapshot} creation logic.
     * <p>
     * This method is called from {@code DeltaSourceSplitEnumerator} constructor during object
     * initialization.
     *
     * @param initialSnapshotVersionHint version of snapshot from checkpoint. If the value is equal
     *                                   to {@link #NO_SNAPSHOT_VERSION} it means that this is the
     *                                   first Source initialization and not a recovery from a
     *                                   Flink's checkpoint.
     * @return A {@link Snapshot} that will be used as an initial Delta Table {@code Snapshot} to
     * read data from.
     *
     * <p>
     * <p>
     * We have 2 cases:
     * <ul>
     *      <li>
     *          {@code initialSnapshotVersionHint} is equal to
     *          {@link DeltaSourceSplitEnumerator#NO_SNAPSHOT_VERSION}. This is either the
     *          initial setup of the source or we are recovering from failure yet no checkpoint
     *          was found.
     *      </li>
     *      <li>
     *          {@code initialSnapshotVersionHint} is not equal to
     *          {@link DeltaSourceSplitEnumerator#NO_SNAPSHOT_VERSION}. We are recovering from
     *          failure and a checkpoint was found. Thus, this {@code initialSnapshotVersionHint}
     *          is the version we should load.
     *      </li>
     * </ul>
     * <p>
     * @implNote In case of recovery, if there is a checkpoint available to recover from, the logic
     * will try to recover snapshot using version coming from that checkpoint. We always prefer that
     * version over the other ones. If one of {@code versionAsOf} or {@code timestampAsOf} option
     * was used, we will use that for initial setup. Also in case of recovery the {@code
     * initialSnapshotVersionHint} value will be same as snapshot version resolved by
     * versionAsOf/timestampAsOf options during source first initialization.
     * <p>
     * <p>
     * <p>
     * Option's mutual exclusion must be guaranteed by other classes like {@code DeltaSourceBuilder}
     * or {@code DeltaSourceConfiguration}
     */
    @Override
    protected Snapshot getInitialSnapshot(long initialSnapshotVersionHint) {

        // Prefer version from checkpoint over the other ones.
        return getSnapshotFromCheckpoint(initialSnapshotVersionHint)
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
        Long versionAsOf = getOptionValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption() {
        Long timestampAsOf = getOptionValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf));
        }
        return TransitiveOptional.empty();
    }
}
