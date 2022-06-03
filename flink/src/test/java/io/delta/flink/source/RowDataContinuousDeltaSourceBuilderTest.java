package io.delta.flink.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.DeltaConfigOption;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;

@ExtendWith(MockitoExtension.class)
class RowDataContinuousDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    @AfterEach
    public void afterEach() {
        closeDeltaLogStatic();
    }

    ////////////////////////////////
    // Continuous-only test cases //
    ////////////////////////////////

    @Test
    public void shouldCreateSource() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {

        when(deltaLog.getSnapshotForVersionAsOf(10)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.STARTING_VERSION.key(), "10")
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.STARTING_VERSION), equalTo("10"));
    }

    @Test
    public void shouldCreateSourceForStartingVersionParameter() {
        String startingVersion = "10";
        long long_startingVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(long_startingVersion)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().startingVersion(startingVersion),
            getBuilderAllColumns().startingVersion(long_startingVersion)
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                    .getValue(DeltaSourceOptions.STARTING_VERSION), equalTo(startingVersion));
            }
            // two calls because we are testing two builders
            verify(deltaLog, times(2)).getSnapshotForVersionAsOf(long_startingVersion);
        });
    }

    @Test
    public void shouldCreateSourceForStartingVersionOption() {
        long startingVersion = 10;

        when(deltaLog.getSnapshotForVersionAsOf(startingVersion)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String startingVersionKey = DeltaSourceOptions.STARTING_VERSION.key();
        List<RowDataContinuousDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().option(startingVersionKey, 10),
            getBuilderAllColumns().option(startingVersionKey, 10L),
            getBuilderAllColumns().option(startingVersionKey, "10")
        );

        assertAll(() -> {
            for (RowDataContinuousDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> source = builder.build();
                assertThat(source, notNullValue());
                assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
                assertThat(source.getSourceConfiguration()
                    .getValue(DeltaSourceOptions.STARTING_VERSION), equalTo(startingVersion));
            }
            // three calls because we are testing three builders
            verify(deltaLog, times(3)).getSnapshotForVersionAsOf(startingVersion);
        });
    }

    @Test
    public void shouldCreateSourceForStartingTimestampParameter() {
        String timestamp = "2022-02-24T04:55:00.001";
        long timestampAsOf = TimestampFormatConverter.convertToTimestamp(timestamp);
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);
        DeltaSource<RowData> source =DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .startingTimestamp(timestamp)
            .build();

        assertThat(source, notNullValue());
        assertThat(source.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
        assertThat(source.getSourceConfiguration()
            .getValue(DeltaSourceOptions.STARTING_TIMESTAMP), equalTo(timestampAsOf));
        verify(deltaLog).getSnapshotForTimestampAsOf(timestampAsOf);
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    public Collection<? extends DeltaSourceBuilderBase<?,?>> initBuildersWithInapplicableOptions() {
        return Arrays.asList(
            getBuilderWithOption(DeltaSourceOptions.VERSION_AS_OF, 10L),
            getBuilderWithOption(DeltaSourceOptions.TIMESTAMP_AS_OF, System.currentTimeMillis())
        );
    }

    @Override
    protected <T> RowDataContinuousDeltaSourceBuilder getBuilderWithOption(
        DeltaConfigOption<T> option,
        T value) {
        RowDataContinuousDeltaSourceBuilder builder =
            DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            );

        return (RowDataContinuousDeltaSourceBuilder) setOptionOnBuilder(option, value, builder);
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forContinuousRowData(
            null,
            null
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderForColumns(String[] columnNames) {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderAllColumns() {
        return DeltaSource.forContinuousRowData(
            new Path(TABLE_PATH),
            DeltaSinkTestUtils.getHadoopConf()
        );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .startingVersion(10)
            .startingTimestamp("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forContinuousRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10)
            .option(
                DeltaSourceOptions.STARTING_TIMESTAMP.key(),
                TimestampFormatConverter.convertToTimestamp("2022-02-24T04:55:00.001")
            );
    }

    @Override
    protected RowDataContinuousDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forContinuousRowData(
                null,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .startingTimestamp("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.STARTING_VERSION.key(), 10);
    }
}
