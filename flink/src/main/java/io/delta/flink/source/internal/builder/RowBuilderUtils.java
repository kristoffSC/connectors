package io.delta.flink.source.internal.builder;

import java.util.List;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.logical.RowType;
import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createVectorFromConstant;

public final class RowBuilderUtils {

    private RowBuilderUtils() {

    }

    /**
     * Create a partitioned {@link ParquetColumnarRowInputFormat}, the partition columns can be
     * generated by {@link Path}.
     */
    public static <SplitT extends DeltaSourceSplit> ColumnBatchFactory<SplitT>
        createPartitionedColumnFactory(
        RowType producedRowType,
        List<String> projectedNames,
        List<String> partitionKeys,
        PartitionFieldExtractor<SplitT> extractor,
        int batchSize) {

        return (SplitT split, ColumnVector[] parquetVectors) -> {
            // create and initialize the row batch
            ColumnVector[] vectors = new ColumnVector[producedRowType.getFieldCount()];
            for (int i = 0; i < vectors.length; i++) {
                RowType.RowField field = producedRowType.getFields().get(i);

                vectors[i] =
                    partitionKeys.contains(field.getName())
                        ? createVectorFromConstant(
                        field.getType(),
                        extractor.extract(
                            split, field.getName(), field.getType()),
                        batchSize)
                        : parquetVectors[projectedNames.indexOf(field.getName())];
            }
            return new VectorizedColumnBatch(vectors);
        };
    }
}
