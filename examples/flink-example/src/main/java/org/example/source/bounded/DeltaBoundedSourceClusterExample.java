package org.example.source.bounded;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.ConsoleSink;
import org.utils.Utils;
import org.utils.job.bounded.DeltaBoundedSourceClusterJobExampleBase;

public class DeltaBoundedSourceClusterExample extends DeltaBoundedSourceClusterJobExampleBase {

    private static final String TABLE_PATH = "/tmp/delta-flink-example/source_table";

    public static void main(String[] args) throws Exception {
        new DeltaBoundedSourceClusterExample().run(TABLE_PATH);
    }

    @Override
    public StreamExecutionEnvironment createPipeline(
            String tablePath,
            int sourceParallelism,
            int sinkParallelism) {

        DeltaSource<RowData> deltaSink = getDeltaSource(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        env
            .fromSource(deltaSink, WatermarkStrategy.noWatermarks(), "bounded-delta-source")
            .setParallelism(sourceParallelism)
            .addSink(new ConsoleSink(Utils.FULL_SCHEMA_ROW_TYPE))
            .setParallelism(1);

        return env;
    }

    @Override
    public DeltaSource<RowData> getDeltaSource(String tablePath) {
        return DeltaSource.forBoundedRowData(
            new Path(tablePath),
            new Configuration()
        ).build();
    }
}
