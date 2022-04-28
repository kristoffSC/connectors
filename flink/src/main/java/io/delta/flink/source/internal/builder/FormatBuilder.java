package io.delta.flink.source.internal.builder;

import java.util.List;

public interface FormatBuilder<T> {

    DeltaBulkFormat<T> build();

    FormatBuilder<T> partitions(List<String> partitions);
}
