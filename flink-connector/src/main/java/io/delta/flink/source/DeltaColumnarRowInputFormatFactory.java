package io.delta.flink.source;

import java.util.List;

import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

// TODO Convert this to some kind of builder
public class DeltaColumnarRowInputFormatFactory {

    private static final PartitionFieldExtractor<DeltaSourceSplit>
        DEFAULT_FIELD_EXTRACTOR = new DeltaPartitionFieldExtractor<>();

    public static <SplitT extends DeltaSourceSplit> ParquetColumnarRowInputFormat<?
        extends DeltaSourceSplit>
        createPartitionedFormat(
        Configuration hadoopConfig, RowType producedRowType, List<String> partitionKeys,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {

        return ParquetColumnarRowInputFormat.createPartitionedFormat(hadoopConfig, producedRowType,
            partitionKeys, DEFAULT_FIELD_EXTRACTOR, batchSize, isUtcTimestamp, isCaseSensitive);
    }

    public static <SplitT extends DeltaSourceSplit> ParquetColumnarRowInputFormat<SplitT>
        createPartitionedFormat(
        Configuration hadoopConfig, RowType producedRowType, List<String> partitionKeys,
        PartitionFieldExtractor<SplitT> extractor,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {

        return ParquetColumnarRowInputFormat.createPartitionedFormat(hadoopConfig, producedRowType,
            partitionKeys, extractor, batchSize, isUtcTimestamp, isCaseSensitive);
    }

    public static <SplitT extends DeltaSourceSplit> ParquetColumnarRowInputFormat<SplitT>
        createFormat(
        Configuration hadoopConfig, RowType producedRowType,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {

        return new ParquetColumnarRowInputFormat<>(
            hadoopConfig, producedRowType, batchSize, isUtcTimestamp, isCaseSensitive);
    }
}
