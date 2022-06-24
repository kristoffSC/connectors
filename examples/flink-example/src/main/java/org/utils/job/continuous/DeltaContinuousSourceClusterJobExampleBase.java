package org.utils.job.continuous;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.Utils;
import org.utils.job.DeltaExampleJobRunner;

public abstract class DeltaContinuousSourceClusterJobExampleBase implements DeltaExampleJobRunner {

    private static final String workPath = "/tmp/delta-flink-example/source_table_work";

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + workPath);
        Utils.prepareDirs(tablePath, workPath);
        StreamExecutionEnvironment env = createPipeline(tablePath, 1, 1);
        env.execute("Continuous Example Job");
        Utils.runSourceTableUpdater(workPath);
    }

    public abstract DeltaSource<RowData> getDeltaSource(String tablePath);
}
