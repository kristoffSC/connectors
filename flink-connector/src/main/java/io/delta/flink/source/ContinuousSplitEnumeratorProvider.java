package io.delta.flink.source;

import java.util.Collections;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner.Provider;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;

public class ContinuousSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final FileEnumerator.Provider fileEnumeratorProvider;

    private final ContinuousEnumerationSettings settings;

    public ContinuousSplitEnumeratorProvider(
        Provider splitAssignerProvider, FileEnumerator.Provider fileEnumeratorProvider,
        ContinuousEnumerationSettings settings) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
        this.settings = settings;
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaPendingSplitsCheckpoint<DeltaSourceSplit>>
            createEnumerator(Path deltaTablePath,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        return new ContinuousDeltaSourceFileEnumerator(
            deltaTablePath, fileEnumeratorProvider.create(),
            splitAssignerProvider.create(Collections.emptyList()), settings
        );
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }
}
