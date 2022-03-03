package io.delta.flink.source.internal.enumerator.monitor;

import java.util.List;

import io.delta.flink.source.internal.enumerator.ActionsPerVersion;

import io.delta.standalone.actions.Action;

/**
 * The Result object for {@link TableMonitor#call()} method. It contains Lists of {@link Action} per
 * {@link io.delta.standalone.Snapshot} versions for monitored Delta Table.
 */
public class MonitorTableResult {

    /**
     * The highest found {@link io.delta.standalone.Snapshot} version in given set of discovered
     * changes.
     */
    private final long highestSeenVersion;

    /**
     * An ordered list of {@link ActionsPerVersion}. Elements o this list represents Delta Table
     * changes per version in ASC version order.
     */
    private final List<ActionsPerVersion<Action>> actionsPerVersion;

    public MonitorTableResult(long snapshotVersion,
        List<ActionsPerVersion<Action>> actionsPerVersion) {
        this.highestSeenVersion = snapshotVersion;
        this.actionsPerVersion = actionsPerVersion;
    }

    public long getHighestSeenVersion() {
        return highestSeenVersion;
    }

    public List<ActionsPerVersion<Action>> getChanges() {
        return actionsPerVersion;
    }
}
