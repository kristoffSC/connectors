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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

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

    private static final Logger LOG =
        LoggerFactory.getLogger(ContinuousDeltaSourceSplitEnumerator.class);

    /**
     * The {@link TableMonitor} instance used to monitor Delta Table for changes.
     */
    // TODO PR 7 Will be used in monitor for changes.
    private final TableMonitor tableMonitor;

    // TODO PR 7 Add monitor for changes.
    // private final boolean ignoreChanges;

    // TODO PR 7 Add monitor for changes.
    // private final boolean ignoreDeletes;

    /**
     * The current {@link Snapshot} version that is used by this enumerator to read data from. This
     * field will be updated by enumerator while new changes will be added to Delta Table and
     * discovered by enumerator.
     */
    // TODO PR 7 this value will be updated by Work Discovery mechanism that will be added in PR 7
    private long currentSnapshotVersion;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            sourceConfiguration, NO_SNAPSHOT_VERSION, NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, long initialSnapshotVersion,
        long currentSnapshotVersion, Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, fileEnumerator, configuration, enumContext,
            sourceConfiguration, chooseVersion(initialSnapshotVersion, currentSnapshotVersion),
            alreadyDiscoveredPaths);

        this.currentSnapshotVersion = (initialSnapshotVersion == NO_SNAPSHOT_VERSION) ?
            this.initialSnapshotVersion : currentSnapshotVersion;

        this.tableMonitor =
            TableMonitor.create(
                deltaLog,
                this.currentSnapshotVersion + 1,
                getOptionValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL));
    }

    private static long chooseVersion(long initialSnapshotVersion,
        long currentSnapshotVersion) {
        return (initialSnapshotVersion == NO_SNAPSHOT_VERSION) ?
            initialSnapshotVersion : currentSnapshotVersion;
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        if (isNotChangeStreamOnly()) {
            readTableInitialContent();
        }

        // TODO PR 7 Add monitor for changes here.
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, currentSnapshotVersion, getRemainingSplits(),
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
     *          checkpointSnapshotVersion is equal to
     *          {@link DeltaSourceSplitEnumerator#NO_SNAPSHOT_VERSION}. This is either the
     *          initial setup of the source, or we are recovering from failure yet no checkpoint
     *          was found.
     *      </li>
     *      <li>
     *          checkpointSnapshotVersion is not equal to
     *          {@link DeltaSourceSplitEnumerator#NO_SNAPSHOT_VERSION}. We are recovering from
     *          failure and a checkpoint was found. Thus, this {@code checkpointSnapshotVersion}
     *          is the version we should load.
     *      </li>
     * </ul>
     * <p>
     * If a specific startingVersion or startingTimestamp option is set, we will use that for
     * initial setup of the source. In case of recovery, if there is a checkpoint available to
     * recover from, the {@code checkpointSnapshotVersion} will be set to version from checkpoint
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

        // TODO test all those options
        // Prefer version from checkpoint over other ones.
        return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
            //.or(this::getSnapshotFromStartingVersionOption) // TODO Add in PR 7
            //.or(this::getSnapshotFromStartingTimestampOption) // TODO Add in PR 7
            .or(this::getHeadSnapshot)
            .get();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        // We should do nothing, since we are continuously monitoring Delta Table.
    }

    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles, long snapshotVersion) {
        AddFileEnumeratorContext context = setUpEnumeratorContext(addFiles, snapshotVersion);
        return fileEnumerator.enumerateSplits(context,
            (SplitFilter<Path>) pathsAlreadyProcessed::add);
    }

    @VisibleForTesting
    void readTableInitialContent() {
        // get data for start version only if we did not already process it,
        // hence if currentSnapshotVersion is == initialSnapshotVersion;
        // So do not read the initial data if we recovered from checkpoint.
        if (this.initialSnapshotVersion == this.currentSnapshotVersion) {
            LOG.info("Getting data for start version - {}", snapshot.getVersion());
            List<DeltaSourceSplit> splits =
                prepareSplits(snapshot.getAllFiles(), snapshot.getVersion());
            addSplits(splits);
        }
    }

    private boolean isChangeStreamOnly() {
        return
            sourceConfiguration.hasOption(STARTING_VERSION) ||
                sourceConfiguration.hasOption(STARTING_TIMESTAMP);
    }

    private boolean isNotChangeStreamOnly() {
        return !isChangeStreamOnly();
    }
}
