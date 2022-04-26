package io.delta.flink.source;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

public class SourceExamples {

    protected static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    protected static final String[] COLUMN_NAMES = {"name", "surname", "age"};

    public void builderBounded() {
        Configuration hadoopConf = new Configuration();

        RowDataFormat dataFormat = RowDataFormat
            .builder(COLUMN_NAMES, COLUMN_TYPES, hadoopConf)
            .build();

        DeltaSource<RowData> source = DeltaSource.boundedSourceBuilder(
            new Path("s3://some/path"),
            dataFormat,
            hadoopConf
        ).build();
    }

    public void builderContinuous() {
        Configuration hadoopConf = new Configuration();

        RowDataFormat dataFormat = RowDataFormat
            .builder(COLUMN_NAMES, COLUMN_TYPES, hadoopConf)
            .build();

        DeltaSource<RowData> source = DeltaSource.continuousSourceBuilder(
                new Path("s3://some/path"),
                dataFormat,
                hadoopConf
            )
            .build();
    }

    public void builderBoundedPublicOption() {
        Configuration hadoopConf = new Configuration();

        RowDataFormat dataFormat = RowDataFormat
            .builder(COLUMN_NAMES, COLUMN_TYPES, hadoopConf)
            .build();

        DeltaSource<RowData> source = DeltaSource.boundedSourceBuilder(
                new Path("s3://some/path"),
                dataFormat,
                hadoopConf
            )
            .versionAsOf(10)
            .build();
    }

    public void builderContinuousPublicOption() {
        Configuration hadoopConf = new Configuration();

        RowDataFormat dataFormat = RowDataFormat
            .builder(COLUMN_NAMES, COLUMN_TYPES, hadoopConf)
            .build();

        DeltaSource<RowData> source = DeltaSource.continuousSourceBuilder(
                new Path("s3://some/path"),
                dataFormat,
                hadoopConf
            )
            .updateCheckIntervalMillis(1000)
            .startingVersion(10)
            .build();
    }

    public void builderContinuousNonPublicOption() {

        Configuration hadoopConf = new Configuration();

        RowDataFormat dataFormat = RowDataFormat
            .builder(COLUMN_NAMES, COLUMN_TYPES, hadoopConf)
            .build();

        DeltaSource<RowData> source = DeltaSource.continuousSourceBuilder(
                new Path("s3://some/path"),
                dataFormat,
                hadoopConf
            )
            .startingVersion(10)
            .build();
    }

}
