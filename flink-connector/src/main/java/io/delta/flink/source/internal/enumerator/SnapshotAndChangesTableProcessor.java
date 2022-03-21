package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;

public class SnapshotAndChangesTableProcessor implements ContinuousTableProcessor {

    private final SnapshotProcessor snapshotProcessor;

    private final ContinuousTableProcessor changesProcessor;

    private boolean monitoringForChanges;

    public SnapshotAndChangesTableProcessor(
        SnapshotProcessor snapshotProcessor, ContinuousTableProcessor changesProcessor) {
        this.snapshotProcessor = snapshotProcessor;
        this.changesProcessor = changesProcessor;
    }

    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        snapshotProcessor.process(processCallback);
        monitoringForChanges = true;
        changesProcessor.process(processCallback);
    }

    @Override
    public boolean isMonitoringForChanges() {
        return this.monitoringForChanges;
    }

    public long getSnapshotVersion() {
        return (monitoringForChanges) ? changesProcessor.getSnapshotVersion()
            : snapshotProcessor.getSnapshotVersion();
    }

    @Override
    public Collection<Path> getAlreadyProcessedPaths() {
        return (monitoringForChanges) ? changesProcessor.getAlreadyProcessedPaths()
            : snapshotProcessor.getAlreadyProcessedPaths();
    }
}
