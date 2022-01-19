package io.delta.flink.source.enumerator;

import java.util.List;

import io.delta.standalone.actions.AddFile;

public class MonitorTableState {

    private final long snapshotVersion;
    private final List<AddFile> result;

    public MonitorTableState(long snapshotVersion, List<AddFile> result) {
        this.snapshotVersion = snapshotVersion;
        this.result = result;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    public List<AddFile> getResult() {
        return result;
    }
}
