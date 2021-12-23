package io.delta.flink.source;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundedDeltaSourceFileEnumerator
    implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceFileEnumerator.class);

    private final Path deltaTablePath;
    private final FileEnumerator fileEnumerator;
    private final FileSplitAssigner splitAssigner;

    public BoundedDeltaSourceFileEnumerator(Path deltaTablePath, FileEnumerator fileEnumerator,
        FileSplitAssigner splitAssigner) {
        this.deltaTablePath = deltaTablePath;
        this.fileEnumerator = fileEnumerator;
        this.splitAssigner = splitAssigner;
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public PendingSplitsCheckpoint<FileSourceSplit> snapshotState() throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
