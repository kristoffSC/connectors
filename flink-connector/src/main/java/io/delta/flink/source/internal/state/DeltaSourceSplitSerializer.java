package io.delta.flink.source.internal.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import static org.apache.flink.util.Preconditions.checkArgument;

public class DeltaSourceSplitSerializer implements SimpleVersionedSerializer<DeltaSourceSplit> {

    public static final DeltaSourceSplitSerializer INSTANCE = new DeltaSourceSplitSerializer();

    private static final MapSerializer<String, String> partitionSerDe = new MapSerializer<>(
        StringSerializer.INSTANCE, StringSerializer.INSTANCE);

    private static final int VERSION = 1;

    private DeltaSourceSplitSerializer() {
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DeltaSourceSplit split) throws IOException {
        checkArgument(
            split.getClass() == DeltaSourceSplit.class,
            "Only supports %s", DeltaSourceSplit.class.getName());

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper outputWrapper =
            new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
            serialize(outputWrapper, split);
        }

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DeltaSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return tryDeserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private DeltaSourceSplit tryDeserializeV1(byte[] serialized) throws IOException {
        try (DataInputViewStreamWrapper inputWrapper =
            new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
            return deserializeV1(inputWrapper);
        }
    }

    private DeltaSourceSplit deserializeV1(DataInputViewStreamWrapper inputWrapper) throws
        IOException {

        int superLen = inputWrapper.readInt();
        byte[] superBytes = new byte[superLen];
        inputWrapper.readFully(superBytes);
        FileSourceSplit superSplit =
            FileSourceSplitSerializer.INSTANCE.deserialize(
                FileSourceSplitSerializer.INSTANCE.getVersion(), superBytes);

        Map<String, String> partitionValues = partitionSerDe.deserialize(inputWrapper);

        return new DeltaSourceSplit(
            partitionValues,
            superSplit.splitId(),
            superSplit.path(),
            superSplit.offset(),
            superSplit.length(),
            superSplit.hostnames(),
            superSplit.getReaderPosition().orElse(null)
        );
    }

    private void serialize(DataOutputViewStreamWrapper outputWrapper, DeltaSourceSplit split)
        throws IOException {

        byte[] superBytes =
            FileSourceSplitSerializer.INSTANCE.serialize(
                new FileSourceSplit(
                    split.splitId(),
                    split.path(),
                    split.offset(),
                    split.length(),
                    split.hostnames(),
                    split.getReaderPosition().orElse(null)));

        outputWrapper.writeInt(superBytes.length);
        outputWrapper.write(superBytes);
        partitionSerDe.serialize(split.getPartitionValues(), outputWrapper);
    }
}
