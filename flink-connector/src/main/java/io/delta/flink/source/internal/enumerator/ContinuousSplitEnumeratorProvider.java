package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
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
 * ContinuousSplitEnumerator} used for {@link Boundedness#CONTINUOUS_UNBOUNDED} mode.
 */
public class ContinuousSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    /**
     * @param splitAssignerProvider  an instance of {@link FileSplitAssigner.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     * @param fileEnumeratorProvider an instance of {@link AddFileEnumerator.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     */
    public ContinuousSplitEnumeratorProvider(
        FileSplitAssigner.Provider splitAssignerProvider,
        AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        // TODO add in PR 6
        return null;
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        // TODO add in PR 6
        return null;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
}
