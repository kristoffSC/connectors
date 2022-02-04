package io.delta.flink.source.internal.enumerator;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.MonitorTableResult.ChangesPerVersion;
import io.delta.flink.source.internal.exceptions.DeltaSourceException;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptionUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_CHANGES;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_DELETES;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;


import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private static final Logger LOG =
        LoggerFactory.getLogger(ContinuousDeltaSourceSplitEnumerator.class);

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    private final TableMonitor tableMonitor;

    private final boolean ignoreChanges;

    private final boolean ignoreDeletes;

    private long currentSnapshotVersion;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            sourceOptions, NO_SNAPSHOT_VERSION, NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions,
        long initialSnapshotVersion, long currentSnapshotVersion,
        Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, configuration, enumContext, sourceOptions,
            initialSnapshotVersion, alreadyDiscoveredPaths);

        this.fileEnumerator = fileEnumerator;
        this.currentSnapshotVersion = (initialSnapshotVersion == NO_SNAPSHOT_VERSION) ?
            this.initialSnapshotVersion : currentSnapshotVersion;

        // Maybe we could inject it from the provider.
        this.tableMonitor =
            new TableMonitor(deltaLog, this.currentSnapshotVersion + 1, sourceOptions);

        this.ignoreChanges = sourceOptions.getValue(IGNORE_CHANGES);
        this.ignoreDeletes = this.ignoreChanges || sourceOptions.getValue(IGNORE_DELETES);
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        if (isNotChangeStreamOnly()) {
            readTableInitialContent();
        }

        // TODO add tests to check split creation//assignment granularity is in scope of VersionLog.
        //monitor for changes
        enumContext.callAsync(
            tableMonitor, // executed sequentially by ScheduledPool Thread.
            this::processDiscoveredVersions, // executed by Flink's Source-Coordinator Thread.
            sourceOptions.getValue(UPDATE_CHECK_INITIAL_DELAY),
            sourceOptions.getValue(UPDATE_CHECK_INTERVAL));
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, currentSnapshotVersion, getRemainingSplits(),
            pathsAlreadyProcessed);
    }

    @Override
    protected Snapshot getInitialSnapshot(long checkpointSnapshotVersion) {

        // TODO test all those options
        // Prefer version from checkpoint over other ones.
        return getSnapshotFromCheckpoint(checkpointSnapshotVersion)
            .or(this::getSnapshotFromStartingVersionOption)
            .or(this::getSnapshotFromStartingTimestampOption)
            .or(this::getHeadSnapshot)
            .get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromStartingVersionOption() {
        if (isChangeStreamOnly()) {
            String startingVersionValue = sourceOptions.getValue(STARTING_VERSION);
            if (startingVersionValue != null) {
                if (startingVersionValue.equalsIgnoreCase(
                    STARTING_VERSION.defaultValue())) {
                    return TransitiveOptional.ofNullable(deltaLog.snapshot());
                } else {
                    return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(
                        Long.parseLong(startingVersionValue)));
                }
            }
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromStartingTimestampOption() {
        if (isChangeStreamOnly()) {
            String startingTimestampValue = sourceOptions.getValue(STARTING_TIMESTAMP);
            if (startingTimestampValue != null) {
                return TransitiveOptional.ofNullable(deltaLog.getSnapshotForTimestampAsOf(
                    Timestamp.valueOf(startingTimestampValue).getTime()));
            }
        }
        return TransitiveOptional.empty();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        // We should do nothing, since we are continuously monitoring Delta Table.
    }

    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles) {
        try {
            AddFileEnumeratorContext context = setUpEnumeratorContext(addFiles);
            return fileEnumerator
                .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
        } catch (Exception e) {
            throw new DeltaSourceException("Exception wile preparing Splits.", e);
        }
    }

    private void processDiscoveredVersions(MonitorTableResult monitorTableResult, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            throw new DeltaSourceException(error);
        }

        this.currentSnapshotVersion = monitorTableResult.getHighestSeenVersion();
        List<ChangesPerVersion> newActions = monitorTableResult.getChanges();

        newActions.stream()
            .map(this::processActions)
            .map(this::prepareSplits)
            .forEachOrdered(this::addSplits);

        assignSplits();
    }

    private List<AddFile> processActions(ChangesPerVersion changes) {

        List<AddFile> addFiles = new ArrayList<>(changes.size());
        boolean seenAddFile = false;
        boolean seenRemovedFile = false;

        for (Action action : changes.getChanges()) {
            DeltaActions deltaActions = DeltaActions.instanceFrom(action.getClass());
            switch (deltaActions) {
                case ADD:
                    if (((AddFile) action).isDataChange()) {
                        seenAddFile = true;
                        addFiles.add((AddFile) action);
                    }
                    break;
                case REMOVE:
                    if (((RemoveFile) action).isDataChange()) {
                        seenRemovedFile = true;
                    }
                    break;
                case METADATA:
                    // TODO implement schema compatibility check similar as it is done in
                    //  https://github.com/delta-io/delta/blob/0d07d094ccd520c1adbe45dde4804c754c0a4baa/core/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala#L422
                default:
                    // Do nothing.
                    break;
            }
        }

        actionsSanityCheck(seenAddFile, seenRemovedFile, changes.getSnapshotVersion());

        return addFiles;
    }

    private void actionsSanityCheck(boolean seenFileAdd, boolean seenRemovedFile, long version) {
        if (seenRemovedFile) {
            if (seenFileAdd && !ignoreChanges) {
                DeltaSourceExceptionUtils.deltaSourceIgnoreChangesError(version);
            } else if (!seenFileAdd && !ignoreDeletes) {
                DeltaSourceExceptionUtils.deltaSourceIgnoreDeleteError(version);
            }
        }
    }

    private void readTableInitialContent() {
        // get data for start version only if we did not already process is,
        // hence if currentSnapshotVersion is == initialSnapshotVersion;
        // So do not read the initial data if we recovered from checkpoint.
        if (this.initialSnapshotVersion == this.currentSnapshotVersion) {
            try {
                LOG.info("Getting data for start version - {}", snapshot.getVersion());
                List<DeltaSourceSplit> splits = prepareSplits(snapshot.getAllFiles());
                addSplits(splits);
            } catch (Exception e) {
                throw new DeltaSourceException(e);
            }
        }
    }

    private boolean isChangeStreamOnly() {
        return
            sourceOptions.hasOption(STARTING_VERSION) ||
                sourceOptions.hasOption(STARTING_TIMESTAMP);
    }

    private boolean isNotChangeStreamOnly() {
        return !isChangeStreamOnly();
    }
}
