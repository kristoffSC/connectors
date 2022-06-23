package org.example.source.continuous;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.utils.DeltaExampleLocalJobRunner;
import org.utils.DeltaTableUpdater;
import org.utils.Descriptor;
import org.utils.Utils;

public abstract class DeltaContinuousSourceExampleBase implements DeltaExampleLocalJobRunner {

    private final String workPath = Utils.resolveExampleTableAbsolutePath("example_table");

    @Override
    public void run(String tablePath) throws Exception {
        System.out.println("Will use table from path: " + tablePath);

        Utils.prepareDirs(tablePath, workPath);
        StreamExecutionEnvironment env = createPipeline(workPath, 2, 3);
        runFlinkJobInBackground(env);
        runSourceTableUpdater(workPath);
    }

    private void runSourceTableUpdater(String tablePath) {

        final DeltaTableUpdater tableUpdater = new DeltaTableUpdater(tablePath);

        AtomicInteger index = new AtomicInteger(0);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(
            () -> {
                int i = index.getAndIncrement();
                List<Row> rows = Collections.singletonList(
                    Row.of("f1_newVal_" + i, "f2_newVal_" + i, i));
                Descriptor descriptor = new Descriptor(tablePath, Utils.FULL_SCHEMA_ROW_TYPE, rows);
                tableUpdater.writeToTable(descriptor);
            },
            10,
            2,
            TimeUnit.SECONDS
        );
    }

    protected abstract StreamExecutionEnvironment createPipeline(
        String tablePath,
        int sourceParallelism,
        int sinkParallelism
    );


    protected abstract DeltaSource<RowData> getDeltaSource(String tablePath);

}
