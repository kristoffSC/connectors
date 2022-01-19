package io.delta.flink.source.enumerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.delta.flink.source.DeltaSourceException;
import io.delta.flink.source.DeltaSourceSplitEnumerator;
import io.delta.flink.source.file.AddFileEnumerator;
import io.delta.flink.source.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.file.AddFileEnumeratorContext;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, long initialSnapshotVersion,
        Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, configuration, enumContext, initialSnapshotVersion,
            alreadyDiscoveredPaths);
        this.fileEnumerator = fileEnumerator;
    }

    @Override
    public void start() {

        // TODO pass continuousEnumerationSettings to
        //  DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER and ContinuousDeltaSourceSplitEnumerator
        //  and use them in start method.

        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.

        //get data for start version
        try {
            LOG.info("Getting data for start version - {}", snapshot.getVersion());
            List<DeltaSourceSplit> splits = prepareSplits(snapshot.getAllFiles());
            addSplits(splits);
        } catch (Exception e) {
            throw new DeltaSourceException(e);
        }

        // TODO Currently we are assigning new splits after processing all VersionLog elements.
        //  the crucial requirement is that we must assign splits at least on VersionLog element
        //  granularity.
        //  The possible performance improvement here could be to assign splits
        //  after each VersionLog element processing, so after processChangesForVersion method.
        //monitor for changes
        enumContext.callAsync(
            this::monitorForChanges,
            this::processDiscoveredSplits,
            1000,
            5000);
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        //Do nothing since readers have to continuously wait for new data.
    }

    // TODO Extract this to separate class - TableMonitor
    //  and add tests, especially for Action filters.
    //
    // TODO add tests to check split creation//assignment granularity is in scope of VersionLog.
    private List<DeltaSourceSplit> monitorForChanges() {
        long currentTableVersion = deltaLog.update().getVersion();
        if (currentTableVersion > initialSnapshotVersion) {
            long highestSeenVersion = initialSnapshotVersion;
            Iterator<VersionLog> changes = deltaLog.getChanges(initialSnapshotVersion, true);
            List<AddFile> addFiles = new ArrayList<>();
            while (changes.hasNext()) {
                VersionLog versionLog = changes.next();
                highestSeenVersion =
                    processVersion(highestSeenVersion, addFiles, versionLog);
            }
            initialSnapshotVersion = highestSeenVersion;
            return prepareSplits(addFiles);
        }
        return Collections.emptyList();
    }

    // The crucial requirement is that we must assign splits at least on VersionLog element
    // granularity, meaning that we cannot assign splits during VersionLog but after, when we are
    // sure that there were no breaking changes in this version, for which we could emit downstream
    // a corrupted data.
    private long processVersion(long highestSeenVersion, List<AddFile> addFiles,
        VersionLog versionLog) {
        long version = versionLog.getVersion();

        // track the highest version number for future use
        if (highestSeenVersion < version) {
            highestSeenVersion = version;
        }

        // create splits only for new versions
        if (version > initialSnapshotVersion) {
            List<Action> actions = versionLog.getActions();
            processActionsForVersion(addFiles, actions);
        }
        return highestSeenVersion;
    }

    private void processActionsForVersion(List<AddFile> addFiles, List<Action> actions) {
        for (Action action : actions) {
            DeltaActions deltaActions = DeltaActions.instanceBy(action.getClass().getSimpleName());
            switch (deltaActions) {
                case ADD:
                    addFiles.add((AddFile) action);
                    break;
                case REMOVE:
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
    }

    private void throwOnUnsupported() {
        throw new DeltaSourceException(
            "Unsupported Action, table does not have only append changes.");
    }

    private void ignoreAction(Action action) {
        LOG.info("Ignoring action {}", action.getClass());
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

    private void processDiscoveredSplits(List<DeltaSourceSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }

        addSplits(splits);

        // TODO this substskId makes no sense here. Refactor This.
        assignSplits(-1);
    }
}
