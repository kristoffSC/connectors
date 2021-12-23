package io.delta.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;

public interface SplitEnumeratorProvider {

    SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
        createEnumerator(Path deltaTablePath);

    Boundedness getBoundedness();

}
