package io.delta.flink.source;

import java.util.Optional;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.janino.util.Producer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;


public class RowDataDeltaSourceBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataDeltaSourceBuilderTest.class);

    private static final String[] COLUMN_NAMES = {"name", "surname", "age"};

    private static final String TABLE_PATH = "s3://some/path/";

    private static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    @Test
    public void shouldCreateSource() {
        DeltaSource<RowData> boundedSource = RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        DeltaSource<RowData> continuousSource = RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf())
            .continuousMode()
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));

        assertThat(continuousSource, notNullValue());
        assertThat(continuousSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Test
    public void shouldThrowOnNullArguments() {

        Optional<Class<? extends Exception>> nullOnPath = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                null,
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> nullOnColumnNames = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                null,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> nullOnColumnTypes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                null,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> nullOnHadoopConfig = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                null
            ).build()
        );

        assertException(
            nullOnPath.orElseThrow(() -> new AssertionError("Builder should throw on null Path.")),
            NullPointerException.class);

        assertException(
            nullOnColumnNames.orElseThrow(
                () -> new AssertionError("Builder should throw on null column names.")),
            NullPointerException.class);

        assertException(
            nullOnColumnTypes.orElseThrow(
                () -> new AssertionError("Builder should throw on null column types.")),
            NullPointerException.class);

        assertException(
            nullOnHadoopConfig.orElseThrow(
                () -> new AssertionError("Builder should throw on null hadoop config.")),
            NullPointerException.class);
    }

    @Test
    public void testColumnSizeValidation() {

        Optional<Class<? extends Exception>> emptyColumNames = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[0],
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> emptyColumnTypes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[0],
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> notMatchingSizes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[]{new CharType(), new IntType()},
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        assertException(
            emptyColumNames.orElseThrow(
                () -> new AssertionError("Builder should throw on empty column names.")),
            IllegalArgumentException.class);

        assertException(
            emptyColumnTypes.orElseThrow(
                () -> new AssertionError("Builder should throw on empty column types.")),
            IllegalArgumentException.class);

        assertException(
            notMatchingSizes.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw if colum types are different sizes.")),
            IllegalArgumentException.class);
    }

    @Test
    public void testColumnElementValidation() {

        Optional<Class<? extends Exception>> nullColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", null, "coll3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> emptyColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", null, "col3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> blankColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", " ", "col3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        Optional<Class<? extends Exception>> nullColumnType = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[]{new CharType(), null, new CharType()},
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        assertException(
            nullColumnName.orElseThrow(
                () -> new AssertionError("Builder should throw on null column name element.")),
            IllegalArgumentException.class);

        assertException(
            emptyColumnName.orElseThrow(
                () -> new AssertionError("Builder should throw on empty column name element.")),
            IllegalArgumentException.class);

        assertException(
            blankColumnName.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw on blank column name element.")),
            IllegalArgumentException.class);

        assertException(
            nullColumnType.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw null column type element.")),
            IllegalArgumentException.class);
    }

    @Test
    public void testMutualExclusiveOptions() {

        Optional<Class<? extends Exception>> boundedModeExclusions = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                    new Path(TABLE_PATH),
                    COLUMN_NAMES,
                    COLUMN_TYPES,
                    DeltaSinkTestUtils.getHadoopConf()
                ).versionAsOf(1)
                .timestampAsOf("2022-02-24 04:55:00.001")
                .build()
        );

        Optional<Class<? extends Exception>> continuousModeExclusions = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                    new Path(TABLE_PATH),
                    COLUMN_NAMES,
                    COLUMN_TYPES,
                    DeltaSinkTestUtils.getHadoopConf()
                )
                .continuousMode()
                .startingVersion(1)
                .startingTimestamp("2022-02-24 04:55:00.001")
                .build()
        );

        assertException(
            boundedModeExclusions.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw when mutual excluded options were used.")),
            IllegalArgumentException.class);

        assertException(
            continuousModeExclusions.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw when mutual excluded options were used.")),
            IllegalArgumentException.class);
    }

    // PR 8 ADD tests for partition validation null reference and null element.

    @Test
    public void shouldValidateOptionValue() {
        RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).option("startingTimestamp", "dsfgfdsfs")
            .build();
    }

    private Optional<Class<? extends Exception>> testValidation(Producer<DeltaSource<?>> builder) {
        try {
            builder.produce();
        } catch (Exception e) {
            LOG.info("Caught exception during builder validation tests", e);
            return Optional.of(e.getClass());
        }
        return Optional.empty();
    }

    private void assertException(
        Class<? extends Exception> validationException,
        Class<? extends Exception> exception) {
        assertThat("Got different Exception type from validation than expected.",
            validationException, equalTo(exception));
    }

}
