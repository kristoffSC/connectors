package io.delta.flink.source;

import java.util.Collections;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner.Provider;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class BoundedSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    public BoundedSplitEnumeratorProvider() {
        this(DeltaSource.DEFAULT_SPLIT_ASSIGNER,
            DeltaSource.DEFAULT_SPLITTABLE_FILE_ENUMERATOR);
    }

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
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        return new BoundedDeltaSourceSplitEnumerator(
            deltaTablePath, fileEnumeratorProvider.create(),
            splitAssignerProvider.create(Collections.emptyList()), configuration, enumContext
        );
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        return new BoundedDeltaSourceSplitEnumerator(
            checkpoint.getDeltaTablePath(), fileEnumeratorProvider.create(),
            splitAssignerProvider.create(Collections.emptyList()),
            configuration, enumContext, checkpoint.getInitialSnapshotVersion(),
            checkpoint.getAlreadyProcessedPaths());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }
}
