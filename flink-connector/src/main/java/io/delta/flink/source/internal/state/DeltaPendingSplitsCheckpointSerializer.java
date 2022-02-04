package io.delta.flink.source.internal.state;

import java.io.IOException;

import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class DeltaPendingSplitsCheckpointSerializer<SplitT extends DeltaSourceSplit> implements
    SimpleVersionedSerializer<DeltaEnumeratorStateCheckpoint<SplitT>> {

    private static final int VERSION = 1;
    private final PendingSplitsCheckpointSerializer<SplitT> decoratedSerDe;

    public DeltaPendingSplitsCheckpointSerializer(
        SimpleVersionedSerializer<SplitT> splitSerDe) {
        this.decoratedSerDe = new PendingSplitsCheckpointSerializer<>(splitSerDe);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DeltaEnumeratorStateCheckpoint<SplitT> state)
        throws IOException {
        // TODO Add in PR_2
        return new byte[0];
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<SplitT> deserialize(int version,
        byte[] serialized) throws IOException {
        // TODO Add in PR_2
        return null;
    }
}
