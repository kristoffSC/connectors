package io.delta.flink.source.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.delta.flink.source.SourceUtils;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import static org.apache.flink.util.Preconditions.checkArgument;

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
        checkArgument(
            state.getClass() == DeltaEnumeratorStateCheckpoint.class,
            "Only supports %s", DeltaEnumeratorStateCheckpoint.class.getName());

        PendingSplitsCheckpoint<SplitT> decoratedCheckPoint =
            state.getPendingSplitsCheckpoint();

        byte[] decoratedBytes = decoratedSerDe.serialize(decoratedCheckPoint);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper outputWrapper =
            new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
            outputWrapper.writeInt(decoratedBytes.length);
            outputWrapper.write(decoratedBytes);
            outputWrapper.writeLong(state.getInitialSnapshotVersion());

            final byte[] serPath =
                SourceUtils.pathToString(state.getDeltaTablePath())
                    .getBytes(StandardCharsets.UTF_8);

            outputWrapper.writeInt(serPath.length);
            outputWrapper.write(serPath);
        }

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<SplitT> deserialize(int version,
        byte[] serialized) throws IOException {
        if (version == 1) {
            return tryDeserializeV1(serialized);
        }

        throw new IOException("Unknown version: " + version);
    }

    private DeltaEnumeratorStateCheckpoint<SplitT> tryDeserializeV1(byte[] serialized)
        throws IOException {
        try (DataInputViewStreamWrapper inputWrapper =
            new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
            return deserializeV1(inputWrapper);
        }
    }

    private DeltaEnumeratorStateCheckpoint<SplitT> deserializeV1(
        DataInputViewStreamWrapper inputWrapper) throws IOException {
        byte[] decoratedBytes = new byte[inputWrapper.readInt()];
        inputWrapper.readFully(decoratedBytes);
        PendingSplitsCheckpoint<SplitT> decoratedCheckPoint =
            decoratedSerDe.deserialize(decoratedSerDe.getVersion(), decoratedBytes);

        long initialSnapshotVersion = inputWrapper.readLong();

        final byte[] bytes = new byte[inputWrapper.readInt()];
        inputWrapper.readFully(bytes);

        Path deltaTablePath = new Path(new String(bytes, StandardCharsets.UTF_8));

        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion,
            decoratedCheckPoint.getSplits(), decoratedCheckPoint.getAlreadyProcessedPaths());
    }
}
