package io.delta.flink.source;

import java.util.List;

import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

// TODO Convert this to some kind of builder with generic parameter
public class DeltaColumnarRowInputFormatFactory {

    public static ParquetColumnarRowInputFormat<DeltaSourceSplit>
        createPartitionedFormat(
        Configuration hadoopConfig, RowType producedRowType, List<String> partitionKeys,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {

        return ParquetColumnarRowInputFormat.createPartitionedFormat(hadoopConfig, producedRowType,
            partitionKeys, new DeltaPartitionFieldExtractor<>(), batchSize, isUtcTimestamp,
            isCaseSensitive);
    }

    public static ParquetColumnarRowInputFormat<DeltaSourceSplit>
        createFormat(
        Configuration hadoopConfig, RowType producedRowType,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {

        return new ParquetColumnarRowInputFormat<>(
            hadoopConfig, producedRowType, batchSize, isUtcTimestamp, isCaseSensitive);
    }
}
