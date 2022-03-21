package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
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
    private final List<ChangesPerVersion> changesPerVersion;

    public TableMonitorResult(long snapshotVersion, List<ChangesPerVersion> changesPerVersion) {
        this.highestSeenVersion = snapshotVersion;
        this.changesPerVersion = changesPerVersion;
    }

    public long getHighestSeenVersion() {
        return highestSeenVersion;
    }

    public List<ChangesPerVersion> getChanges() {
        return changesPerVersion;
    }

    /**
     * A container object that represents Delta table changes per one {@link
     * io.delta.standalone.Snapshot} version.
     */
    public static class ChangesPerVersion {

        /**
         * The {@link io.delta.standalone.Snapshot} version value for these changes.
         */
        private final long snapshotVersion;

        /**
         * The list of {@link Action}'s in scope of {@link #snapshotVersion}.
         */
        private final List<Action> changes;

        ChangesPerVersion(long snapshotVersion,
            List<Action> changes) {
            this.snapshotVersion = snapshotVersion;
            this.changes = changes;
        }

        public long getSnapshotVersion() {
            return snapshotVersion;
        }

        public List<Action> getChanges() {
            return Collections.unmodifiableList(changes);
        }

        /**
         * @return Number of changes for this version.
         */
        public int size() {
            return changes.size();
        }
    }
}
