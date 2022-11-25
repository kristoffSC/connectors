package io.delta.flink.e2e.sink;

import java.security.SecureRandom;
import java.util.UUID;

import io.delta.flink.e2e.utils.UuidUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomFailoverProcess extends ProcessFunction<RowData, RowData> {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RandomFailoverProcess.class);

    private SecureRandom random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        UUID uuid = UUID.randomUUID();
        String seedString = uuid.toString();
        this.random = new SecureRandom(UuidUtils.asBytes(uuid));

        LOG.info(
            String.format(
                "Seed used for random exception generator for subtask %s on attempt %s - %s",
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getAttemptNumber(),
                seedString
            )
        );
    }

    @Override
    public void processElement(
            RowData rowData, ProcessFunction<RowData,
            RowData>.Context context,
            Collector<RowData> collector) throws Exception {

        if (random.nextBoolean()) {
            throw new RuntimeException(
                String.format("Designed Random Exception from process function task %s.",
                    getRuntimeContext().getIndexOfThisSubtask())
            );
        } else {
            collector.collect(rowData);
        }
    }

}
