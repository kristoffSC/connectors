package io.delta.flink.source.internal.enumerator.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.tuple.Pair;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;

/**
 * This class implements a logic for monitoring Delta table for changes. The logic is implemented in
 * {@link #call()} method which should be called periodically.
 *
 * @implNote This class is stateful and mutable, meaning it keep {@link
 * io.delta.standalone.Snapshot} version to check as next. This class is also NOT Thread safe. Each
 * thread calling {@link #call()} method should have its own {@code TableMonitor} instance.
 */
public class TableMonitor implements Callable<TableMonitorResult> {

    /**
     * The Delta Log/Delta table that this instance monitor for changes.
     */
    private final DeltaLog deltaLog;

    /**
     * The "maximal" duration that each subsequent call to {@link #call()} method should take. This
     * is a soft limit, which means that implementation will try to guarantee that overall call is
     * no longer that this limit. See {@link #call()} method for details.
     */
    private final long maxDurationMillis;

    /**
     * The Delta table {@link io.delta.standalone.Snapshot} version that should be used to read data
     * in next {@link #call()} method call. This value is mutable.
     */
    private long monitorVersion;

    /**
     * Creates new instance of TableMonitor class to monitor Delta table Changes.
     *
     * @param deltaLog          The {@link DeltaLog} to monitor for changes from.
     * @param monitorVersion    The initial {@link io.delta.standalone.Snapshot} version from which
     *                          this instance will monitor for changes.
     * @param maxDurationMillis The "maximal" duration that each subsequent call to {@link #call()}
     *                          method should take. This is a soft limit, which means that
     *                          implementation will try to guarantee that overall call is * no
     *                          longer that this limit. See {@link #call()} method for details.
     */
    public TableMonitor(DeltaLog deltaLog, long monitorVersion,
        long maxDurationMillis) {
        this.deltaLog = deltaLog;
        this.monitorVersion = monitorVersion;
        this.maxDurationMillis = maxDurationMillis;
    }

    /**
     * Monitor underlying Delta table for changes. The {@link TableMonitor} will try to limit
     * execution time for this method to {@link #maxDurationMillis} value. Limit check will be done
     * per each {@link io.delta.standalone.Snapshot} version that was detected. If the {@link
     * #maxDurationMillis} limit is exceeded, logic will return.
     *
     * @return {@link TableMonitorResult} object that contains list of {@link
     * io.delta.standalone.actions.Action} per version.
     */
    @Override
    public TableMonitorResult call() throws Exception {
        // TODO PR 7 add tests
        TableMonitorResult monitorResult = monitorForChanges(this.monitorVersion);
        long highestSeenVersion = monitorResult.getHighestSeenVersion();
        if (!monitorResult.getChanges().isEmpty()) {
            this.monitorVersion = highestSeenVersion + 1;
        }
        return monitorResult;
    }

    public long getMonitorVersion() {
        return monitorVersion;
    }

    private TableMonitorResult monitorForChanges(long startVersion) {

        // TODO Add tests, especially for Action filters.
        Iterator<VersionLog> changes = deltaLog.getChanges(startVersion, true);
        if (changes.hasNext()) {
            return processChanges(startVersion, changes);
        }

        // Case if there were no changes.
        return new TableMonitorResult(startVersion, Collections.emptyList());
    }

    private TableMonitorResult processChanges(long startVersion, Iterator<VersionLog> changes) {

        List<ChangesPerVersion<Action>> actionsPerVersion = new ArrayList<>();
        long highestSeenVersion = startVersion;

        long endTime = System.currentTimeMillis() + maxDurationMillis;

        while (changes.hasNext()) {
            VersionLog versionLog = changes.next();
            Pair<Long, List<Action>> version = processVersion(highestSeenVersion, versionLog);

            highestSeenVersion = version.getKey();
            actionsPerVersion.add(
                new ChangesPerVersion<>(
                    deltaLog.getPath().toUri().normalize().toString(),
                    versionLog.getVersion(), version.getValue()));

            // TODO PR 7 write unit test for this
            // Check if we still under task interval limit.
            if (System.currentTimeMillis() >= endTime) {
                break;
            }
        }
        return new TableMonitorResult(highestSeenVersion, actionsPerVersion);
    }

    // We must assign splits at VersionLog element granularity, meaning that we cannot assign
    // splits while integrating through VersionLog changes. We must do it only when we are
    // sure that there were no breaking changes in this version. In other case we could emit
    // downstream a corrupted data or unsupported data change.
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
