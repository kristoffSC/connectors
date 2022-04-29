package io.delta.flink.source;

import java.util.Optional;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class RowDataBoundedDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    @Test
    public void shouldCreateSource() {
        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    @Test
    public void shouldCreateSourceWithOptions() {
        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    @Test
    public void testMutualExclusiveOptions() {
        // using dedicated builder methods
        Optional<Exception> validation = testValidation(
            () -> DeltaSource.forBoundedRowData(
                    new Path(TABLE_PATH),
                    COLUMN_NAMES,
                    COLUMN_TYPES,
                    DeltaSinkTestUtils.getHadoopConf()
                )
                .versionAsOf(10)
                .timestampAsOf("2022-02-24T04:55:00.001")
                .build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception when using mutual excluded options."));

        assertThat(exception.getValidationMessages().size(), equalTo(1));
    }

    @Test
    public void testMutualExclusiveGenericOptions() {
        // using dedicated builder methods
        Optional<Exception> validation = testValidation(
            () -> DeltaSource.forBoundedRowData(
                    new Path(TABLE_PATH),
                    COLUMN_NAMES,
                    COLUMN_TYPES,
                    DeltaSinkTestUtils.getHadoopConf()
                )
                .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
                .option(DeltaSourceOptions.TIMESTAMP_AS_OF.key(), "2022-02-24T04:55:00.001")
                .build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception when using mutual excluded options."));

        assertThat(exception.getValidationMessages().size(), equalTo(1));
    }

    @Test
    public void testNullMandatoryFieldsAndExcludedOption() {

        Optional<Exception> validation = testValidation(
            () -> DeltaSource.forBoundedRowData(
                    null,
                    null,
                    null,
                    DeltaSinkTestUtils.getHadoopConf()
                )
                .timestampAsOf("2022-02-24T04:55:00.001")
                .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
                .build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError("Builder should throw validation exception."));

        // expected number is 5 because Hadoop is used and validated by Format and Source builders.
        assertThat(exception.getValidationMessages().size(), equalTo(4));
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forBoundedRowData(
            null,
            null,
            null,
            null
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderForColumns(
        String[] columnNames,
        LogicalType[] columnTypes) {
        return DeltaSource.forBoundedRowData(
            new Path(TABLE_PATH),
            columnNames,
            columnTypes,
            DeltaSinkTestUtils.getHadoopConf()
        );
    }
}
