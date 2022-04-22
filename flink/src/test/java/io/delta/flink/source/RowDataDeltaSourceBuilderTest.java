package io.delta.flink.source;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.janino.util.Producer;
import org.junit.Test;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;


public class RowDataDeltaSourceBuilderTest {

    protected static final String[] COLUMN_NAMES = {"name", "surname", "age"};
    private static final String TABLE_PATH = "s3://some/path/";
    private static final LogicalType[] COLUMN_TYPES =
        {new CharType(), new CharType(), new IntType()};

    @Test
    public void shouldCreateSource() {
        DeltaSource<RowData> boundedSource = RowDataDeltaSourceBuilder.builder(
            new Path(TABLE_PATH),
            COLUMN_NAMES,
            COLUMN_TYPES,
            DeltaSinkTestUtils.getHadoopConf()
        ).build();

        DeltaSource<RowData> continuousSource = RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).continuousMode()
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));

        assertThat(continuousSource, notNullValue());
        assertThat(continuousSource.getBoundedness(), equalTo(Boundedness.CONTINUOUS_UNBOUNDED));
    }

    @Test
    public void shouldThrowOnNullArguments() {

        boolean nullOnPath = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                null,
                COLUMN_NAMES,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean nullOnColumnNames = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                null,
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean nullOnColumnTypes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                null,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean nullOnHadoopConfig = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                COLUMN_TYPES,
                null
            ).build()
        );

        assertThat("Builder should throw on null Path", nullOnPath, equalTo(true));
        assertThat("Builder should throw on null column names", nullOnColumnNames, equalTo(true));
        assertThat("Builder should throw on null column types", nullOnColumnTypes, equalTo(true));
        assertThat("Builder should throw on null hadoop config", nullOnHadoopConfig, equalTo(true));
    }

    @Test
    public void testColumnSizeValidation() {

        boolean emptyColumNames = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[0],
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean emptyColumnTypes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[0],
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean notMatchingSizes = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[]{new CharType(), new IntType()},
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        assertThat("Builder should throw on empty column names", emptyColumNames, equalTo(true));
        assertThat("Builder should throw on empty column types", emptyColumnTypes, equalTo(true));
        assertThat("Builder should throw if colum types are different sizes.", notMatchingSizes,
            equalTo(true));
    }

    @Test
    public void testColumnElementValidation() {

        boolean nullColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", null, "coll3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean emptyColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", null, "col3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean blankColumnName = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                new String[]{"col1", " ", "col3"},
                COLUMN_TYPES,
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        boolean nullColumnType = testValidation(
            () -> RowDataDeltaSourceBuilder.builder(
                new Path(TABLE_PATH),
                COLUMN_NAMES,
                new LogicalType[]{new CharType(), null, new CharType()},
                DeltaSinkTestUtils.getHadoopConf()
            ).build()
        );

        assertThat("Builder should throw on null column name element.", nullColumnName,
            equalTo(true));
        assertThat("Builder should throw on empty column name element.", emptyColumnName,
            equalTo(true));
        assertThat("Builder should throw on blank column name element.", blankColumnName,
            equalTo(true));
        assertThat("Builder should throw null column type element.", nullColumnType, equalTo(true));

    }

    private boolean testValidation(Producer<DeltaSource<?>> builder) {
        try {
            builder.produce();
        } catch (Exception e) {
            return true;
        }
        return false;
    }

}
