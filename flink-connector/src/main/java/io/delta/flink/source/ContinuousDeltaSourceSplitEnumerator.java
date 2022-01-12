package io.delta.flink.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.delta.flink.source.AddFileEnumerator.SplitFilter;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddCDCFile;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    private long currentLocalSnapshotVersion;

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
        this.currentLocalSnapshotVersion = this.initialSnapshotVersion;
    }

    @Override
    public void start() {

        //get data for start version
        try {
            List<DeltaSourceSplit> splits = prepareSplits(snapshot.getAllFiles());
            addSplits(splits);
        } catch (Exception e) {
            // TODO Create Delta Source Exception
            throw new RuntimeException(e);
        }

        //monitor for changes
        enumContext.callAsync(
            this::monitorForChanges,
            this::processDiscoveredSplits,
            1000,
            5000);
    }

    // TODO extract this to separate class - TableMonitor
    //  and add tests, especially for Action filters.
    private List<DeltaSourceSplit> monitorForChanges() {
        long currentTableVersion = deltaLog.update().getVersion();
        if (currentTableVersion > currentLocalSnapshotVersion) {
            long highestSeenVersion = currentLocalSnapshotVersion;
            Iterator<VersionLog> changes = deltaLog.getChanges(currentLocalSnapshotVersion, true);
            List<AddFile> addFiles = new ArrayList<>();
            while (changes.hasNext()) {
                VersionLog versionLog = changes.next();
                long version = versionLog.getVersion();

                // track the highest version number for future use
                if (highestSeenVersion < version) {
                    highestSeenVersion = version;
                }

                // create splits only for new versions
                if (version > currentLocalSnapshotVersion) {
                    List<Action> actions = versionLog.getActions();
                    for (Action action : actions) {
                        if (action instanceof RemoveFile || action instanceof AddCDCFile) {
                            throw new RuntimeException(
                                "Unsupported Action, table does not have only append changes.");
                        }

                        if (action instanceof AddFile) {
                            addFiles.add((AddFile) action);
                        } else {
                            LOG.info(
                                "Ignoring action {}, since this is not an ADDFile nor Remove/CDC",
                                action.getClass());
                        }
                    }
                }
            }
            currentLocalSnapshotVersion = highestSeenVersion;
            return prepareSplits(addFiles);
        }
        return Collections.emptyList();
    }

    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles) {
        try {
            AddFileEnumeratorContext context = setUpEnumeratorContext(addFiles);
            return fileEnumerator
                .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
        } catch (Exception e) {
            throw new RuntimeException("Exception wile preparing Splits.", e);
        }
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        //Do nothing since readers have to continuously wait for new data.
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
