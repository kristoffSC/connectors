package io.delta.flink.source.enumerator;

import java.util.List;

import io.delta.standalone.actions.Action;

public class MonitorTableState {

    private final long snapshotVersion;
    private final List<List<Action>> result;

    public MonitorTableState(long snapshotVersion, List<List<Action>> result) {
        this.snapshotVersion = snapshotVersion;
        this.result = result;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    public List<List<Action>> getResult() {
        return result;
    }
}
