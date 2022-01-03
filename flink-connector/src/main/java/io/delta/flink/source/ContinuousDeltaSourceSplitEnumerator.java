package io.delta.flink.source;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousDeltaSourceSplitEnumerator implements
    SplitEnumerator<DeltaSourceSplit, DeltaPendingSplitsCheckpoint<DeltaSourceSplit>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(ContinuousDeltaSourceSplitEnumerator.class);

    private final Path deltaTablePath;
    private final FileEnumerator fileEnumerator;
    private final FileSplitAssigner splitAssigner;
    private final ContinuousEnumerationSettings settings;

    public ContinuousDeltaSourceSplitEnumerator(Path deltaTablePath, FileEnumerator fileEnumerator,
        FileSplitAssigner splitAssigner, ContinuousEnumerationSettings settings) {

        this.deltaTablePath = deltaTablePath;
        this.fileEnumerator = fileEnumerator;
        this.splitAssigner = splitAssigner;
        this.settings = settings;
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<DeltaSourceSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public DeltaPendingSplitsCheckpoint<DeltaSourceSplit> snapshotState() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
