package io.delta.flink.source.internal.enumerator;

import java.util.Collections;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of {@link SplitEnumeratorProvider} that creates a {@code
 * BoundedSplitEnumerator} used for {@link Boundedness#BOUNDED} mode.
 */
public class BoundedSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    // TODO PR 4 add AddFileEnumerator in with IT bounded tests.
    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    /**
     * @param splitAssignerProvider  an instance of {@link FileSplitAssigner.Provider} that will be
     *                               used for building a {@code BoundedSplitEnumerator} by factory
     *                               methods.
     * @param fileEnumeratorProvider an instance of {@link AddFileEnumerator.Provider} that will be
     *                               used for building a {@code BoundedSplitEnumerator} by factory
     *                               methods.
     */
    public BoundedSplitEnumeratorProvider(
        FileSplitAssigner.Provider splitAssignerProvider,
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
