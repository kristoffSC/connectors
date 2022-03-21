package io.delta.flink.source.internal.enumerator;

public interface ContinuousTableProcessor extends TableProcessor {
    boolean isMonitoringForChanges();
}
