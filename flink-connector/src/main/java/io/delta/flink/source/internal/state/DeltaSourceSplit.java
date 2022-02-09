package io.delta.flink.source.internal.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.source.state.AbstractDeltaSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

public class DeltaSourceSplit extends AbstractDeltaSourceSplit {

    private static final String[] NO_HOSTS = StringUtils.EMPTY_STRING_ARRAY;

    private final Map<String, String> partitionValues;

    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length) {
        this(partitionValues, id, filePath, offset, length, NO_HOSTS, null);
    }

    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length, String... hostnames) {
        this(partitionValues, id, filePath, offset, length, hostnames, null);
    }

    public DeltaSourceSplit(Map<String, String> partitionValues, String id,
        Path filePath, long offset, long length, String[] hostnames,
        CheckpointedPosition readerPosition) {
        super(id, filePath, offset, length, hostnames, readerPosition);

        // Make split Partition a new Copy of original map to prevent mutation.
        this.partitionValues =
            (partitionValues == null) ? Collections.emptyMap() : new HashMap<>(partitionValues);
    }

    @Override
    public DeltaSourceSplit updateWithCheckpointedPosition(CheckpointedPosition position) {
        return new DeltaSourceSplit(partitionValues, splitId(), path(), offset(), length(),
            hostnames(), position);
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }
}
