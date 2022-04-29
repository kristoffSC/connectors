package io.delta.flink.source;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class RowDataContinuousDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    @Test
    public void shouldCreateSource() {
        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {
        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.STARTING_TIMESTAMP.key(), 10)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forContinuousRowData(
            null,
            null,
            null,
            null
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderForColumns(
        String[] columnNames,
        LogicalType[] columnTypes) {
        return DeltaSource.forContinuousRowData(
            new Path(TABLE_PATH),
            columnNames,
            columnTypes,
            DeltaSinkTestUtils.getHadoopConf()
        );
    }
}
