package io.delta.flink.source;

import javax.annotation.Nullable;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaSourceSplit extends FileSourceSplit {

    private final long snapshotVersion;

    public DeltaSourceSplit(long snapshotVersion, String id, Path filePath, long offset,
        long length) {
        super(id, filePath, offset, length);
        this.snapshotVersion = checkNotNull(snapshotVersion, "snapshotVersion can not be null");
    }

    public DeltaSourceSplit(long snapshotVersion, String id, Path filePath, long offset,
        long length,
        String... hostnames) {
        super(id, filePath, offset, length, hostnames);
        this.snapshotVersion = checkNotNull(snapshotVersion, "snapshotVersion can not be null");
    }

    public DeltaSourceSplit(long snapshotVersion, String id, Path filePath, long offset,
        long length, String[] hostnames,
        @Nullable CheckpointedPosition readerPosition) {
        super(id, filePath, offset, length, hostnames, readerPosition);
        this.snapshotVersion = checkNotNull(snapshotVersion, "snapshotVersion can not be null");
    }

    @Override
    public DeltaSourceSplit updateWithCheckpointedPosition(
        @Nullable CheckpointedPosition position) {
        return new DeltaSourceSplit(snapshotVersion, splitId(), path(), offset(), length(),
            hostnames(), position);
    }
}
