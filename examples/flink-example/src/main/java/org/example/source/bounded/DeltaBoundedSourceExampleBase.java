package org.example.source.bounded;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.DeltaExampleLocalJobRunner;
import org.utils.Utils;

public abstract class DeltaBoundedSourceExampleBase implements DeltaExampleLocalJobRunner {

    private final String workPath = Utils.resolveExampleTableAbsolutePath("example_table");

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);

        Utils.prepareDirs(tablePath, workPath);
        StreamExecutionEnvironment env = createPipeline(workPath, 2, 3);
        runFlinkJobInBackground(env);
    }

    protected abstract StreamExecutionEnvironment createPipeline(
        String tablePath,
        int sourceParallelism,
        int sinkParallelism
    );

    protected abstract DeltaSource<RowData> getDeltaSource(String tablePath);
}
