package org.example.source.bounded;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.DeltaExampleLocalJobRunner;

public abstract class DeltaBoundedSourceExampleBase implements DeltaExampleLocalJobRunner {

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);

        StreamExecutionEnvironment env = createPipeline(tablePath, 2, 3);
        runFlinkJobInBackground(env);
    }

    protected abstract StreamExecutionEnvironment createPipeline(
        String tablePath,
        int sourceParallelism,
        int sinkParallelism
    );


    protected abstract DeltaSource<RowData> getDeltaSource(String tablePath);

}
