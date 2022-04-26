package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;

public interface DeltaBulkFormat<T> extends BulkFormat<T, DeltaSourceSplit> {

}
