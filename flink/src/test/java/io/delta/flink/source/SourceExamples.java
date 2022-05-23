package io.delta.flink.source;

import java.util.Arrays;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

public class SourceExamples {

    public void builderBounded() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .build();
    }

    public void builderBoundedUserSelectedColumns() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .columnNames(Arrays.asList("col1", "col2"))
            .build();
    }

    public void builderContinuous() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forContinuousRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .build();
    }

    public void builderContinuousWithPartitions() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forContinuousRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .build();
    }

    public void builderBoundedPublicOption() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .versionAsOf(10)
            .build();
    }

    public void builderContinuousPublicOption() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forContinuousRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .updateCheckIntervalMillis(1000)
            .startingVersion(10)
            .build();
    }
}
