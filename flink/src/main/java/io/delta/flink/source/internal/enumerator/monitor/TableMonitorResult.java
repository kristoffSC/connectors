package io.delta.flink.source.internal.enumerator.monitor;

import java.util.List;

import io.delta.standalone.actions.Action;

/**
 * The result object for {@link TableMonitor#call()} method. It contains Lists of {@link Action} per
 * {@link io.delta.standalone.Snapshot} versions for monitored Delta table.
 */
public class TableMonitorResult {

    /**
     * An ordered list of {@link ChangesPerVersion}. Elements of this list represents Delta table
     * changes per version in ASC version order.
     */
    private final List<ChangesPerVersion<Action>> changesPerVersion;

    public TableMonitorResult(List<ChangesPerVersion<Action>> changesPerVersion) {
        this.changesPerVersion = changesPerVersion;
    }

    public List<ChangesPerVersion<Action>> getChanges() {
        return changesPerVersion;
    }

}
