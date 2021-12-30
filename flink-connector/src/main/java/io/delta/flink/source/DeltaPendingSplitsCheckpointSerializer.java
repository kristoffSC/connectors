package io.delta.flink.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import static org.apache.flink.util.Preconditions.checkArgument;

public class DeltaPendingSplitsCheckpointSerializer implements
    SimpleVersionedSerializer<DeltaPendingSplitsCheckpoint<DeltaSourceSplit>> {

    private static final int VERSION = 1;
    private final PendingSplitsCheckpointSerializer<DeltaSourceSplit> decoratedSerDe;

    public DeltaPendingSplitsCheckpointSerializer(
        SimpleVersionedSerializer<DeltaSourceSplit> splitSerDe) {
        this.decoratedSerDe = new PendingSplitsCheckpointSerializer<>(splitSerDe);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DeltaPendingSplitsCheckpoint<DeltaSourceSplit> checkpoint)
        throws IOException {
        checkArgument(
            checkpoint.getClass() == DeltaPendingSplitsCheckpoint.class,
            "Only supports %s", DeltaPendingSplitsCheckpoint.class.getName());

        PendingSplitsCheckpoint<DeltaSourceSplit> decoratedCheckPoint =
            checkpoint.getPendingSplitsCheckpoint();

        byte[] decoratedBytes = decoratedSerDe.serialize(decoratedCheckPoint);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper outputWrapper =
            new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
            outputWrapper.writeInt(decoratedBytes.length);
            outputWrapper.write(decoratedBytes);
            outputWrapper.writeLong(checkpoint.getInitialSnapshotVersion());
        }

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DeltaPendingSplitsCheckpoint<DeltaSourceSplit> deserialize(int version,
        byte[] serialized) throws IOException {
        if (version == 1) {
            return tryDeserializeV1(serialized);
        }

        throw new IOException("Unknown version: " + version);
    }

    private DeltaPendingSplitsCheckpoint<DeltaSourceSplit> tryDeserializeV1(byte[] serialized)
        throws IOException {
        try (DataInputViewStreamWrapper inputWrapper =
            new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
            return deserializeV1(inputWrapper);
        }
    }

    private DeltaPendingSplitsCheckpoint<DeltaSourceSplit> deserializeV1(
        DataInputViewStreamWrapper inputWrapper) throws IOException {
        byte[] decoratedBytes = new byte[inputWrapper.readInt()];
        inputWrapper.readFully(decoratedBytes);
        PendingSplitsCheckpoint<DeltaSourceSplit> decoratedCheckPoint =
            decoratedSerDe.deserialize(decoratedSerDe.getVersion(), decoratedBytes);

        long initialSnapshotVersion = inputWrapper.readLong();

        return DeltaPendingSplitsCheckpoint.fromCollectionSnapshot(
            initialSnapshotVersion,
            decoratedCheckPoint.getSplits(), decoratedCheckPoint.getAlreadyProcessedPaths());
    }
}
