package io.delta.flink.source.enumerator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import io.delta.flink.source.DeltaSourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;

public class TableMonitor implements Callable<MonitorTableState> {

    private static final Logger LOG = LoggerFactory.getLogger(TableMonitor.class);

    private final DeltaLog deltaLog;

    private long snapshotVersion;

    public TableMonitor(DeltaLog deltaLog, long initialSnapshotVersion) {
        this.deltaLog = deltaLog;
        this.snapshotVersion = initialSnapshotVersion;
    }

    @Override
    public MonitorTableState call() throws Exception {
        List<AddFile> result = new ArrayList<>();
        this.snapshotVersion = monitorForChanges(this.snapshotVersion, result);
        return new MonitorTableState(this.snapshotVersion, result);
    }

    // TODO and add tests, especially for Action filters.
    private long monitorForChanges(long startVersion, List<AddFile> result) {
        long currentTableVersion = deltaLog.update().getVersion();
        if (currentTableVersion > startVersion) {
            long highestSeenVersion = startVersion;
            Iterator<VersionLog> changes = deltaLog.getChanges(startVersion + 1, true);
            while (changes.hasNext()) {
                VersionLog versionLog = changes.next();
                highestSeenVersion =
                    processVersion(highestSeenVersion, result, versionLog);
            }
            return highestSeenVersion;
        }
        return startVersion;
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

        List<Action> actions = versionLog.getActions();
        processActionsForVersion(addFiles, actions);

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

}
