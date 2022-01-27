package io.delta.flink.source.enumerator;

import java.util.Collections;
import java.util.List;

import io.delta.standalone.actions.Action;

public class MonitorTableResult {

    private final long highestSeenVersion;
    private final List<ChangesPerVersion> changesPerVersion;

    public MonitorTableResult(long snapshotVersion, List<ChangesPerVersion> changesPerVersion) {
        this.highestSeenVersion = snapshotVersion;
        this.changesPerVersion = changesPerVersion;
    }

    public long getHighestSeenVersion() {
        return highestSeenVersion;
    }

    public List<ChangesPerVersion> getChanges() {
        return changesPerVersion;
    }

    public static class ChangesPerVersion {

        private final long snapshotVersion;
        private final List<Action> changes;

        public ChangesPerVersion(long snapshotVersion,
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

        public int size() {
            return changes.size();
        }
    }
}
