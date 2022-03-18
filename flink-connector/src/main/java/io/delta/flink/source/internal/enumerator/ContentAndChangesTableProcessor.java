package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;

public class ContentAndChangesTableProcessor implements ContinuousTableProcessor {

    private final SnapshotProcessor snapshotProcessor;

    private final ContinuousTableProcessor changesProcessor;

    private boolean startedMonitoringForChanges;

    public ContentAndChangesTableProcessor(
        SnapshotProcessor snapshotProcessor, ContinuousTableProcessor changesProcessor) {
        this.snapshotProcessor = snapshotProcessor;
        this.changesProcessor = changesProcessor;
    }

    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {

        List<DeltaSourceSplit> snapshotContent = snapshotProcessor.process();
        processCallback.accept(snapshotContent);

        startedMonitoringForChanges = true;

        changesProcessor.process(processCallback);
    }

    @Override
    public boolean isStartedMonitoringForChanges() {
        return this.startedMonitoringForChanges;
    }

    public long getSnapshotVersion() {
        return (startedMonitoringForChanges) ? changesProcessor.getSnapshotVersion()
            : snapshotProcessor.getSnapshotVersion();
    }

    @Override
    public Collection<Path> getAlreadyProcessedPaths() {
        return (startedMonitoringForChanges) ? changesProcessor.getAlreadyProcessedPaths()
            : snapshotProcessor.getAlreadyProcessedPaths();
    }
}
