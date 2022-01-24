package io.delta.flink.source.enumerator;

import java.util.ArrayList;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);
    private static final int INITIAL_DELAY = 1000;

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    private final TableMonitor tableMonitor;

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
        this.tableMonitor = new TableMonitor(deltaLog, this.currentSnapshotVersion + 1);
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.

        // get data for start version only if we did not already process is,
        // hence if currentSnapshotVersion is == initialSnapshotVersion;
        if (this.initialSnapshotVersion == this.currentSnapshotVersion) {
            try {
                LOG.info("Getting data for start version - {}", snapshot.getVersion());
                List<DeltaSourceSplit> splits = prepareSplits(snapshot.getAllFiles());
                addSplits(splits);
            } catch (Exception e) {
                throw new DeltaSourceException(e);
            }
        }

        // TODO add tests to check split creation//assignment granularity is in scope of VersionLog.
        //monitor for changes
        enumContext.callAsync(
            tableMonitor, // executed sequentially by ScheduledPool Thread.
            this::processDiscoveredVersions, // executed by Flink's Source-Coordinator Thread.
            INITIAL_DELAY,
            sourceOptions.getOptionValue(DeltaSourceOptions.UPDATE_CHECK_INTERVAL));
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {
        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, currentSnapshotVersion, getRemainingSplits(),
            pathsAlreadyProcessed);
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

    private void processDiscoveredVersions(MonitorTableState monitorTableState, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            throw new DeltaSourceException(error);
        }

        this.currentSnapshotVersion = monitorTableState.getSnapshotVersion();
        List<List<Action>> newActions = monitorTableState.getResult();

        newActions.stream()
            .map(this::processActions)
            .map(this::prepareSplits)
            .forEachOrdered(this::addSplits);

        assignSplits();
    }

    private List<AddFile> processActions(List<Action> actions) {
        List<AddFile> addFiles = new ArrayList<>();
        for (Action action : actions) {
            DeltaActions deltaActions = DeltaActions.instanceBy(action.getClass().getSimpleName());
            switch (deltaActions) {
                case ADD:
                    addFiles.add((AddFile) action);
                    break;
                case REMOVE:
                    // TODO add support for ignoreDeletes and ignoreChanges options:
                    //  ignoreDeletes - if true and particular version had only Deletes
                    //  (no other actions) then do not throw an exception on RemoveFile
                    //  regardless of Action.isDataChange flag.
                    //  ignoreChanges - if true and particular version had combination of
                    //  deletes and other actions, then do not throw an error on RemoveFile
                    //  regardless of Action.isDataChange flag deletes.
                    if (((RemoveFile) action).isDataChange()) {
                        throwOnUnsupported();
                    } else {
                        ignoreAction(action);
                    }
                    break;
                case CDC:
                    throwOnUnsupported();
                    break;
                default:
                    ignoreAction(action);
            }
        }
        return addFiles;
    }

    private void throwOnUnsupported() {
        throw new DeltaSourceException(
            "Unsupported Action, table does not have only append changes.");
    }

    private void ignoreAction(Action action) {
        LOG.info("Ignoring action {}", action.getClass());
    }
}
