package io.delta.flink.source.internal.enumerator;

import java.util.Collections;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner.Provider;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class BoundedSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    public BoundedSplitEnumeratorProvider(
        Provider splitAssignerProvider,
        AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions) {

        return new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumeratorProvider.create(),
            splitAssignerProvider.create(Collections.emptyList()), configuration, enumContext,
            sourceOptions);
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions) {

        return new BoundedDeltaSourceSplitEnumerator(
            checkpoint.getDeltaTablePath(), fileEnumeratorProvider.create(),
            splitAssignerProvider.create(Collections.emptyList()),
            configuration, enumContext, sourceOptions, checkpoint.getInitialSnapshotVersion(),
            checkpoint.getAlreadyProcessedPaths());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
}
