package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;

public interface ContinuousTableProcessor {

    void process(Consumer<List<DeltaSourceSplit>> processCallback);

    long getSnapshotVersion();

    Collection<Path> getAlreadyProcessedPaths();

    boolean isStartedMonitoringForChanges();
}
