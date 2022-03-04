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
     * The current {@link Snapshot} version that is used by this enumerator to read changes from.
     * This field will be updated by enumerator while new changes will be added to Delta Table and
     * discovered by enumerator.
     */
    // TODO PR 7 this value will be updated by Work Discovery mechanism that will be added in PR 7
    private long currentSnapshotVersion;

    /**
     * @param initialSnapshotVersionHint this parameter is used by {@link #getInitialSnapshot(long)}
     *                                   method to initialize the initial {@link Snapshot}. From
     *                                   that snapshot the {@link #initialSnapshotVersion} field is
     *                                   set by calling {@code snapshot.vetVersion()}.
     * @param currentSnapshotVersionHint this parameter is used by
     *                                   {@link #computeCurrentSnapshotVersion(long)}
     *                                   method to compute the value of
     *                                   {@link #currentSnapshotVersion}
     *                                   field.
     */
    private ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, long initialSnapshotVersionHint,
        long currentSnapshotVersionHint, Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, fileEnumerator, configuration, enumContext,
            sourceConfiguration, initialSnapshotVersionHint, alreadyDiscoveredPaths);

        this.currentSnapshotVersion = computeCurrentSnapshotVersion(currentSnapshotVersionHint);

        this.tableMonitor =
            TableMonitor.create(
                deltaLog, computeTableMonitorInitialSnapshotVersion(),
                getOptionValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL));
    }

    /**
     * Method used to create a new {@link ContinuousDeltaSourceSplitEnumerator} instance. The
     * instance created with this method will have its Delta {@link Snapshot} created from version:
     * <ul>
     *     <li>Specified by startingVersion/startingTimestamp options.</li>
     *     <li>The Delta Table current head version at the time this object was created.</li>
     * </ul>
     */
    public static ContinuousDeltaSourceSplitEnumerator create(Path deltaTablePath,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator, FileSplitAssigner splitAssigner,
        Configuration configuration, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        return new ContinuousDeltaSourceSplitEnumerator(deltaTablePath, fileEnumerator,
            splitAssigner, configuration, enumContext,
            sourceConfiguration, NO_SNAPSHOT_VERSION, NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    /**
     * Method used to create a new {@link ContinuousDeltaSourceSplitEnumerator} instance using
     * Flink's checkpoint data from {@link DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>} such
     * as, path to Delta Table, initialSnapshotVersion, currentSnapshotVersion or collection of
     * already processed paths.
     * <p>
     * This method should be used when recovering from checkpoint, for example in {@link
     * org.apache.flink.api.connector.source.Source#restoreEnumerator(SplitEnumeratorContext,
     * Object)} method implementation.
     */
    public static ContinuousDeltaSourceSplitEnumerator createForCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        return new ContinuousDeltaSourceSplitEnumerator(checkpoint.getDeltaTablePath(),
            fileEnumerator, splitAssigner, configuration, enumContext, sourceConfiguration,
            checkpoint.getInitialSnapshotVersion(), checkpoint.getCurrentTableVersion(),
            checkpoint.getAlreadyProcessedPaths());
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        if (isNotChangeStreamOnly() && isInitialVersionNotProcessed()) {
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
     *          initial setup of the source, or we are recovering from failure yet no checkpoint
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
     * version over the other ones. If one of {@code startingVersion} or {@code startingTimestamp}
     * option was used, we will use that for initial setup. Also in case of recovery the {@code
     * initialSnapshotVersionHint} value will be same as snapshot version resolved by
     * startingVersion/startingTimestamp options during source first initialization.
     * <p>
     * <p>
     * <p>
     * Option's mutual exclusion must be guaranteed by other classes like {@code DeltaSourceBuilder}
     * or {@code DeltaSourceConfiguration}
     */
    @Override
    protected Snapshot getInitialSnapshot(long initialSnapshotVersionHint) {

        // TODO test all those options
        // Prefer version from checkpoint over other ones.
        return getSnapshotFromCheckpoint(initialSnapshotVersionHint)
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
        LOG.info("Getting data for start version - {}", initialSnapshot.getVersion());
        List<DeltaSourceSplit> splits =
            prepareSplits(initialSnapshot.getAllFiles(), initialSnapshot.getVersion());
        addSplits(splits);
    }

    /**
     * Check if {@link #currentSnapshotVersion} is same as {@link #initialSnapshotVersion}. If yes,
     * then we should read the initial table content.
     * <p>
     * The {@code currentSnapshotVersion} field will be changed after we convert all Files to Splits
     * from initial snapshot, and we start monitoring for changes.
     *
     * @return true if Initial Snapshot was already converted to Splits, false if not.
     */
    private boolean isInitialVersionNotProcessed() {
        return this.initialSnapshotVersion == this.currentSnapshotVersion;
    }

    /**
     * This method is used to provide a value for {@link #currentSnapshotVersion} field. The {@code
     * ContinuousDeltaSourceSplitEnumerator}, since it reads table changes continuously, it needs an
     * additional field that will be mutable and will represent a current snapshot version that this
     * source currently uses, thus {@link #currentSnapshotVersion} field.
     * <p>
     * The {@link #initialSnapshotVersion} however, which is declared as final in a parent class,
     * and it is shared between all {@link DeltaSourceSplitEnumerator} implementations, represents
     * an "initial" Snapshot version that this Source starts reading from.
     * <p>
     * For initial Source setup or for recovery without a checkpoint, the condition in this method
     * will be true, setting  {@link #currentSnapshotVersion} to the same value as {@link
     * #initialSnapshotVersion}.
     *
     * <p>
     * During the recovery, in
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode we do not want to read the initial table content once again (if we did it already). We
     * can start monitor for changes immediately. When recovering from checkpoint, the condition
     * check will be false, meaning that we will set {@link #currentSnapshotVersion} field to {@code
     * currentSnapshotVersionHint}. The {@code currentSnapshotVersionHint} value will be coming from
     * checkpoint in this case.
     *
     * @param currentSnapshotVersionHint version of current snapshot.
     * @return The snapshot value that should be used as currentSnapshotVersionHint.
     * <p>
     * @implNote The logic that determines whether we have already processed the initial version is
     * implemented and described in {@link #isInitialVersionNotProcessed}.
     */
    private long computeCurrentSnapshotVersion(long currentSnapshotVersionHint) {
        return (currentSnapshotVersionHint == NO_SNAPSHOT_VERSION) ?
            this.initialSnapshotVersion : currentSnapshotVersionHint;
    }

    /**
     * This method is used to set the initial Snapshot version for {@link TableMonitor}. For initial
     * Source setup or recovery without a checkpoint, where source will read the initial table
     * content, the {@code TableMonitor} should monitor for changes starting from
     * currentSnapshotVersion + 1 version.
     * <p>
     * When recovering from a checkpoint however, the snapshot's content for {@link
     * #currentSnapshotVersion} should not be read, and we should read only changes starting from
     * this version instead.
     *
     * @return The Delta Snapshot's version number that should be used for {@code TableMonitor}.
     */
    // TODO PR 7 add tests for this.
    private long computeTableMonitorInitialSnapshotVersion() {
        return (isInitialVersionNotProcessed()) ?
            this.currentSnapshotVersion + 1 : this.currentSnapshotVersion;
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
