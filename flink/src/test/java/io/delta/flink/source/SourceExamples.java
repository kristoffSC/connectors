package io.delta.flink.source;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

public class SourceExamples {

    protected static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    protected static final String[] COLUMN_NAMES = {"name", "surname", "age"};

    public void builderBounded() {
        DeltaSource<RowData> source = DeltaSource.forRowData(
                new Path("s3://some/path"),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .build();
    }

    public void stepBuilderBounded() {
        DeltaSource<RowData> source = DeltaSource.forRowDataStepBuilder()
            .tablePath(new Path("s3://some/path"))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .hadoopConfiguration(DeltaSinkTestUtils.getHadoopConf())
            .build();
    }

    public void builderContinuous() {
        DeltaSource<RowData> source = DeltaSource.forRowData(
                new Path("s3://some/path"),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .continuousMode()
            .build();
    }

    public void stepBuilderContinuous() {
        DeltaSource<RowData> source = DeltaSource.forRowDataStepBuilder()
            .tablePath(new Path("s3://some/path"))
            .columnNames(COLUMN_NAMES)
            .columnTypes(COLUMN_TYPES)
            .hadoopConfiguration(DeltaSinkTestUtils.getHadoopConf())
            .continuousMode()
            .build();
    }

    // Starting from here all examples are using DeltaSourceBuilder and not DeltaSourceStepBuilder
    // In both implementations, options are defined in the same way.
    public void builderBoundedPublicOption() {
        DeltaSource<RowData> source = DeltaSource.forRowData(
                new Path("s3://some/path"),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .ignoreChanges(true)
            .build();
    }

    public void builderContinuousPublicOption() {
        DeltaSource<RowData> source = DeltaSource.forRowData(
                new Path("s3://some/path"),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .ignoreChanges(true)
            .continuousMode()
            //.ignoreChanges(true)
            .build();
    }

    public void builderContinuousNonPublicOption() {
        DeltaSource<RowData> source = DeltaSource.forRowData(
                new Path("s3://some/path"),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .continuousMode()
            .option("parquetBatchSize", 1024)
            .build();
    }

}
