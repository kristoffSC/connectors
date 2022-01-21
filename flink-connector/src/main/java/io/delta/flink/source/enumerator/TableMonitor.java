package io.delta.flink.source.enumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.tuple.Pair;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;

public class TableMonitor implements Callable<MonitorTableState> {

    private final DeltaLog deltaLog;

    private long snapshotVersion;

    public TableMonitor(DeltaLog deltaLog, long initialMonitorSnapshotVersion) {
        this.deltaLog = deltaLog;
        this.snapshotVersion = initialMonitorSnapshotVersion;
    }

    @Override
    public MonitorTableState call() throws Exception {
        Pair<Long, List<List<Action>>> changes = monitorForChanges(this.snapshotVersion);
        this.snapshotVersion = changes.getKey();
        return new MonitorTableState(this.snapshotVersion, changes.getValue());
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
            List<List<Action>> actions = new ArrayList<>();
            long highestSeenVersion = startVersion;
            Iterator<VersionLog> changes = deltaLog.getChanges(startVersion, true);
            while (changes.hasNext()) {
                VersionLog versionLog = changes.next();
                Pair<Long, List<Action>> version = processVersion(highestSeenVersion, versionLog);

                highestSeenVersion = version.getKey();
                actions.add(version.getValue());
            }
            return Pair.of(highestSeenVersion, actions);
        }
        return Pair.of(startVersion, Collections.emptyList());
    }

    // The crucial requirement is that we must assign splits at least on VersionLog element
    // granularity, meaning that we cannot assign splits during VersionLog but after, when we are
    // sure that there were no breaking changes in this version, for which we could emit downstream
    // a corrupted data.
    private Pair<Long, List<Action>> processVersion(long highestSeenVersion,
        VersionLog versionLog) {
        long version = versionLog.getVersion();

        // track the highest version number for future use
        if (highestSeenVersion < version) {
            highestSeenVersion = version;
        }

        return Pair.of(highestSeenVersion, versionLog.getActions());
    }
}
