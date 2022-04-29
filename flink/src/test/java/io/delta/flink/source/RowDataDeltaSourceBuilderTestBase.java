package io.delta.flink.source;

import java.util.Optional;
import java.util.stream.Stream;

import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.janino.util.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class RowDataDeltaSourceBuilderTestBase {

    protected static final Logger LOG =
        LoggerFactory.getLogger(RowDataBoundedDeltaSourceBuilderTest.class);

    protected static final String[] COLUMN_NAMES = {"name", "surname", "age"};

    protected static final String TABLE_PATH = "s3://some/path/";

    protected static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    protected static Stream<Arguments> columnArrays() {
        return Stream.of(
            Arguments.of(
                new String[]{"col1", "col2"},
                new LogicalType[]{new CharType(), new CharType(), new CharType()},
                1),
            Arguments.of(
                new String[]{"col1", "col2", "col3"},
                new LogicalType[]{new CharType(), new CharType()},
                1),
            Arguments.of(
                new String[]{"col1", null, "col3"},
                new LogicalType[]{new CharType(), new CharType(), new CharType()},
                1),
            Arguments.of(
                new String[]{"col1", "col2", "col3"},
                new LogicalType[]{new CharType(), null, new CharType()},
                1),
            Arguments.of(
                new String[]{"col1", null, "col3"},
                new LogicalType[]{new CharType(), new CharType()},
                2),
            Arguments.of(
                new String[]{"col1", "col3"},
                new LogicalType[]{new CharType(), null, new CharType()},
                2),
            Arguments.of(
                null,
                new LogicalType[]{new CharType(), null, new CharType()},
                2),
            Arguments.of(
                null,
                new LogicalType[]{new CharType(), new CharType()},
                1),
            Arguments.of(new String[]{"col1", "col3"}, null, 1),
            Arguments.of(new String[]{"col1", null}, null, 2)
        );
    }

    @Test
    public void testNullArgumentsValidation() {

        Optional<Exception> validation = testValidation(() -> getBuilderWithNulls().build());

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError("Builder should throw exception on null arguments."));

        // expected number is 5 because Hadoop is used and validated by Format and Source builders.
        assertThat(exception.getValidationMessages().size(), equalTo(5));
    }

    @ParameterizedTest
    @MethodSource("columnArrays")
    public void testColumnArrays(
        String[] columnNames,
        LogicalType[] columnTypes,
        int expectedCount) {

        Optional<Exception> validation = testValidation(
            () -> getBuilderForColumns(columnNames, columnTypes).build()
        );

        DeltaSourceValidationException exception =
            (DeltaSourceValidationException) validation.orElseThrow(
                () -> new AssertionError(
                    "Builder should throw exception on invalid column names and column types "
                        + "arrays."));

        assertThat(exception.getValidationMessages().size(), equalTo(expectedCount));
    }

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderWithNulls();

    protected abstract DeltaSourceBuilderBase<?, ?> getBuilderForColumns(
        String[] columnNames,
        LogicalType[] columnTypes);

    protected Optional<Exception> testValidation(Producer<DeltaSource<?>> builder) {
        try {
            builder.produce();
        } catch (Exception e) {
            LOG.info("Caught exception during builder validation tests", e);
            return Optional.of(e);
        }
        return Optional.empty();
    }

}
