package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import org.apache.flink.configuration.ConfigOption;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

public abstract class SnapshotSupplier {

    protected final DeltaLog deltaLog;

    protected final DeltaSourceConfiguration sourceConfiguration;

    protected SnapshotSupplier(
        DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        this.deltaLog = deltaLog;
        this.sourceConfiguration = sourceConfiguration;
    }

    public abstract Snapshot getSnapshot();

    protected TransitiveOptional<Snapshot> getHeadSnapshot() {
        return TransitiveOptional.ofNullable(deltaLog.snapshot());
    }

    protected <T> T getOptionValue(ConfigOption<T> option) {
        return this.sourceConfiguration.getValue(option);
    }
}
