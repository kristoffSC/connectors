package org.example.source;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.example.DeltaExampleLocalJobRunner;

public abstract class DeltaSourceExampleBase implements DeltaExampleLocalJobRunner {

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);

        StreamExecutionEnvironment env = createPipeline(tablePath, 2, 3);
        runFlinkJobInBackground(env);
    }

    abstract StreamExecutionEnvironment createPipeline(
        String tablePath,
        int sourceParallelism,
        int sinkParallelism
    );


    abstract DeltaSource<RowData> getDeltaSource(String tablePath);

}
