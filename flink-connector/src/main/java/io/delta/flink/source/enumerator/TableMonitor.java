package io.delta.flink.source.enumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import io.delta.flink.source.DeltaSourceOptions;
import org.apache.commons.lang3.tuple.Pair;
import static io.delta.flink.source.DeltaSourceOptions.ACTIONS_PER_MONITOR_BATCH_LIMIT;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;

public class TableMonitor implements Callable<MonitorTableState> {

    private final DeltaLog deltaLog;

    private final int changesPerBatchCountLimit;

    private long changesFromVersion;

    public TableMonitor(DeltaLog deltaLog, long initialMonitorSnapshotVersion,
        DeltaSourceOptions sourceOptions) {
        this.deltaLog = deltaLog;
        this.changesFromVersion = initialMonitorSnapshotVersion;
        this.changesPerBatchCountLimit = sourceOptions.getValue(ACTIONS_PER_MONITOR_BATCH_LIMIT);
    }

    @Override
    public MonitorTableState call() throws Exception {
        Pair<Long, List<List<Action>>> changes = monitorForChanges(this.changesFromVersion);
        Long highestSeenVersion = changes.getKey();
        if (!changes.getValue().isEmpty()) {
            this.changesFromVersion = highestSeenVersion + 1;
        }
        return new MonitorTableState(highestSeenVersion, changes.getValue());
    }

    /**
     * Monitor underlying Delta Table for changes. Returns a Pair of the highest snapshot version
     * found and List of actions grouped by each intermediate version.
     *
     * @param startVersion - Delta Snapshot version (inclusive) from which monitoring of changes
     *                     will begin.
     * @return - Pair of values - highest seen version and List of all table changes grouped by each
     * intermediate version.
     */
    private Pair<Long, List<List<Action>>> monitorForChanges(long startVersion) {
        // TODO and add tests, especially for Action filters.
        long currentTableVersion = deltaLog.update().getVersion();
        if (currentTableVersion >= startVersion) {
            Iterator<VersionLog> changes = deltaLog.getChanges(startVersion, true);
            return processChanges(startVersion, changes);
        }
        return Pair.of(startVersion, Collections.emptyList());
    }

    private Pair<Long, List<List<Action>>> processChanges(
        long startVersion, Iterator<VersionLog> changes) {

        List<List<Action>> actions = new ArrayList<>();
        long highestSeenVersion = startVersion;

        int changesPerBatchCount = 0;

        // TODO Discuss With DataBricks about the default value and implement after
        // long changesPerBatchSize = 0;

        while (changes.hasNext()) {
            VersionLog versionLog = changes.next();
            Pair<Long, List<Action>> version = processVersion(highestSeenVersion, versionLog);

            highestSeenVersion = version.getKey();
            actions.add(version.getValue());

            changesPerBatchCount += version.getValue().size();
            if (changesPerBatchCount >= changesPerBatchCountLimit) {
                break;
            }
        }
        return Pair.of(highestSeenVersion, actions);
    }

    // The crucial requirement is that we must assign splits at least on VersionLog element
    // granularity, meaning that we cannot assign splits during VersionLog but after, when we are
    // sure that there were no breaking changes in this version, for which we could emit downstream
    // a corrupted data.
    private Pair<Long, List<Action>> processVersion(
        long highestSeenVersion, VersionLog versionLog) {
        long version = versionLog.getVersion();

        // track the highest version number for future use
        if (highestSeenVersion < version) {
            highestSeenVersion = version;
        }

        return Pair.of(highestSeenVersion, versionLog.getActions());
    }
}
