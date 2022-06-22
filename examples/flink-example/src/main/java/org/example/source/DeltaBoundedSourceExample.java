package org.example.source;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.Utils;

public class DeltaBoundedSourceExample extends DeltaSourceExampleBase {

    static String TABLE_PATH =
        Utils.resolveExampleTableAbsolutePath("data/source_table_no_partitions");

    public static void main(String[] args) throws Exception {
        new DeltaBoundedSourceExample().run(TABLE_PATH);
    }

    @Override
    StreamExecutionEnvironment createPipeline(
            String tablePath,
            int sourceParallelism,
            int sinkParallelism) {

        DeltaSource<RowData> deltaSink = getDeltaSource(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        env
            .fromSource(deltaSink, WatermarkStrategy.noWatermarks(), "delta-source")
            .setParallelism(sourceParallelism)
            .addSink(new DeltaExampleSinkFunction())
            .setParallelism(1);

        return env;
    }

    @Override
    DeltaSource<RowData> getDeltaSource(String tablePath) {
        return DeltaSource.forBoundedRowData(
            new Path(tablePath),
            new Configuration()
        ).build();
    }

}
