package io.delta.flink.source.enumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import io.delta.flink.source.DeltaSourceOptions;
import io.delta.flink.source.enumerator.MonitorTableResult.ChangesPerVersion;
import org.apache.commons.lang3.tuple.Pair;
import static io.delta.flink.source.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;

public class TableMonitor implements Callable<MonitorTableResult> {

    private final DeltaLog deltaLog;

    private final long maxDurationMillis;

    private long changesFromVersion;

    public TableMonitor(DeltaLog deltaLog, long initialMonitorSnapshotVersion,
        DeltaSourceOptions sourceOptions) {
        this.deltaLog = deltaLog;
        this.changesFromVersion = initialMonitorSnapshotVersion;
        this.maxDurationMillis = sourceOptions.getValue(UPDATE_CHECK_INTERVAL);
    }

    @Override
    public MonitorTableResult call() throws Exception {
        MonitorTableResult monitorResult = monitorForChanges(this.changesFromVersion);
        long highestSeenVersion = monitorResult.getHighestSeenVersion();
        if (!monitorResult.getChanges().isEmpty()) {
            this.changesFromVersion = highestSeenVersion + 1;
        }
        return monitorResult;
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
    private MonitorTableResult monitorForChanges(long startVersion) {

        // TODO and add tests, especially for Action filters.
        Iterator<VersionLog> changes = deltaLog.getChanges(startVersion, true);
        if (changes.hasNext()) {
            return processChanges(startVersion, changes);
        }

        // Case if there were no changes.
        return new MonitorTableResult(startVersion, Collections.emptyList());
    }

    private MonitorTableResult processChanges(long startVersion, Iterator<VersionLog> changes) {

        List<ChangesPerVersion> actionsPerVersion = new ArrayList<>();
        long highestSeenVersion = startVersion;

        long endTime = System.currentTimeMillis() + maxDurationMillis;

        while (changes.hasNext()) {
            VersionLog versionLog = changes.next();
            Pair<Long, List<Action>> version = processVersion(highestSeenVersion, versionLog);

            highestSeenVersion = version.getKey();
            actionsPerVersion.add(
                new ChangesPerVersion(versionLog.getVersion(), version.getValue()));

            // TODO write unit test for this
            // Check if we still under task interval limit.
            if (System.currentTimeMillis() >= endTime) {
                break;
            }
        }
        return new MonitorTableResult(highestSeenVersion, actionsPerVersion);
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
