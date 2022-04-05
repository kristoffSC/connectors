package io.delta.flink.source.internal.enumerator.monitor;

import java.util.List;

import io.delta.standalone.actions.Action;

/**
 * The result object for {@link TableMonitor#call()} method. It contains Lists of {@link Action} per
 * {@link io.delta.standalone.Snapshot} versions for monitored Delta table.
 */
public class TableMonitorResult {

    /**
     * The highest found {@link io.delta.standalone.Snapshot} version in given set of discovered
     * changes.
     */
    private final long highestSeenVersion;

    /**
     * An ordered list of {@link ChangesPerVersion}. Elements of this list represents Delta table
     * changes per version in ASC version order.
     */
    private final List<ChangesPerVersion<Action>> changesPerVersion;

    public TableMonitorResult(
        long snapshotVersion, List<ChangesPerVersion<Action>> changesPerVersion) {
        this.highestSeenVersion = snapshotVersion;
        this.changesPerVersion = changesPerVersion;
    }

    public long getHighestSeenVersion() {
        return highestSeenVersion;
    }

    public List<ChangesPerVersion<Action>> getChanges() {
        return changesPerVersion;
    }

}
