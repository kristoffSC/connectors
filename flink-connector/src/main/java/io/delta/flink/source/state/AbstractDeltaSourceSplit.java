package io.delta.flink.source.state;

import javax.annotation.Nullable;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;

/**
 * Marker abstract class for internal types, not meant for public use.
 */
public abstract class AbstractDeltaSourceSplit extends FileSourceSplit {

    public AbstractDeltaSourceSplit(String id, Path filePath, long offset, long length,
        String[] hostnames, @Nullable
        CheckpointedPosition readerPosition) {
        super(id, filePath, offset, length, hostnames, readerPosition);
    }
}
