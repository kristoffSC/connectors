package io.delta.flink.source.internal.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.source.internal.DeltaPartitionFieldExtractor;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for {@link RowData} implementation io {@link FormatBuilder}
 */
public class RowDataFormatBuilder implements FormatBuilder<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataFormatBuilder.class);

    /**
     * Default reference value for partition column names list.
     */
    private static final List<String> DEFAULT_PARTITION_COLUMNS = new ArrayList<>(0);

    /**
     * Message prefix for validation exceptions.
     */
    private static final String EXCEPTION_PREFIX = "RowDataFormatBuilder - ";

    // -------------- Hardcoded Non Public Options ----------
    /**
     * Hardcoded option for {@link RowDataFormat} to threat timestamps as a UTC timestamps.
     */
    private static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * Hardcoded option for {@link RowDataFormat} to use case-sensitive in column name processing
     * for Parquet files.
     */
    private static final boolean PARQUET_CASE_SENSITIVE = false;
    // ------------------------------------------------------

    // TODO PR 11 get this from options.
    private static final int BATCH_SIZE = 2048;

    /**
     * An array with Delta table's column names that should be read.
     */
    private final String[] columnNames;

    /**
     * An array of {@link LogicalType} for column names tha should raed from Delta table.
     */
    private final LogicalType[] columnTypes;

    /**
     * An instance of Hadoop configuration used to read Parquet files.
     */
    private final Configuration hadoopConfiguration;

    /**
     * An array with Delta table partition columns.
     */
    private List<String> partitionColumns;

    RowDataFormatBuilder(
            String[] columnNames,
            LogicalType[] columnTypes,
            Configuration hadoopConfiguration) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.hadoopConfiguration = hadoopConfiguration;
        this.partitionColumns = DEFAULT_PARTITION_COLUMNS;
    }

    /**
     * Set list of partition columns.
     */
    public RowDataFormatBuilder partitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    @Override
    public FormatBuilder<RowData> partitionColumns(String... partitionColumns) {
        return partitionColumns(Arrays.asList(partitionColumns));
    }

    /**
     * Creates an instance of {@link RowDataFormat}.
     *
     * @throws DeltaSourceValidationException if invalid arguments were passed to {@link
     *                                        RowDataFormatBuilder}. For example null arguments.
     */
    public RowDataFormat build() {
        validateFormat();

        if (partitionColumns == DEFAULT_PARTITION_COLUMNS) {
            LOG.info("Building format data for none partitioned Delta table.");
            return buildFormatWithoutPartitionColumns(
                columnNames,
                columnTypes,
                hadoopConfiguration
            );
        } else {
            LOG.info("Building format data for partitioned Delta table.");
            return
                buildFormatWithPartitionColumns(
                    columnNames,
                    columnTypes,
                    hadoopConfiguration,
                    partitionColumns
                );
        }
    }

    private void validateFormat() {
        Validator mandatoryValidator = validateMandatoryOptions();
        Validator optionValidator = validatePartitionColumns();

        Set<String> validationMessages = new HashSet<>();
        validationMessages.addAll(mandatoryValidator.getValidationMessages());
        validationMessages.addAll(optionValidator.getValidationMessages());

        if (!validationMessages.isEmpty()) {
            // RowDataFormatBuilder does not know Delta's table path,
            // hence null argument in DeltaSourceValidationException
            throw new DeltaSourceValidationException(null, validationMessages);
        }
    }

    private RowDataFormat buildFormatWithoutPartitionColumns(
            String[] columnNames,
            LogicalType[] columnTypes,
            Configuration configuration) {

        return new RowDataFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            BATCH_SIZE,
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    private RowDataFormat buildFormatWithPartitionColumns(
            String[] columnNames,
            LogicalType[] columnTypes,
            Configuration hadoopConfig,
            List<String> partitionColumns) {

        RowType producedRowType = RowType.of(columnTypes, columnNames);
        RowType projectedRowType =
            new RowType(
                producedRowType.getFields().stream()
                    .filter(field -> !partitionColumns.contains(field.getName()))
                    .collect(Collectors.toList()));

        List<String> projectedNames = projectedRowType.getFieldNames();

        ColumnBatchFactory<DeltaSourceSplit> factory =
            RowBuilderUtils.createPartitionedColumnFactory(
                producedRowType,
                projectedNames,
                partitionColumns,
                new DeltaPartitionFieldExtractor<>(),
                BATCH_SIZE);

        return new RowDataFormat(
            hadoopConfig,
            projectedRowType,
            producedRowType,
            factory,
            BATCH_SIZE,
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    /**
     * Validates a mandatory options for {@link RowDataFormatBuilder} such as
     * <ul>
     *     <li>null check on arguments</li>
     *     <li>null values in arrays</li>
     *     <li>size mismatch for column name and type arrays.</li>
     * </ul>
     *
     * @return {@link Validator} instance containing validation error messages if any.
     */
    private Validator validateMandatoryOptions() {

        Validator validator = new Validator()
            // validate against null references
            .checkNotNull(columnNames, EXCEPTION_PREFIX + "missing Delta table column names.")
            .checkNotNull(columnTypes, EXCEPTION_PREFIX + "missing Delta table column types.")
            .checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");

        if (columnNames != null) {
            validator
                .checkArgument(columnNames.length > 0,
                    EXCEPTION_PREFIX + "empty array with column names.")
                // validate invalid array element
                .checkArgument(Stream.of(columnNames)
                        .noneMatch(StringUtils::isNullOrWhitespaceOnly),
                    EXCEPTION_PREFIX
                        + "Column names array contains at least one element that is null, "
                        + "empty, or contains only whitespace characters.");
        }

        if (columnTypes != null) {
            validator
                .checkArgument(columnTypes.length > 0,
                    EXCEPTION_PREFIX + "empty array with column names.")
                .checkArgument(Stream.of(columnTypes)
                    .noneMatch(Objects::isNull), EXCEPTION_PREFIX + "Column type array contains at "
                    + "least one null element.");
        }

        if (columnNames != null && columnTypes != null) {
            validator
                .checkArgument(columnNames.length == columnTypes.length,
                    EXCEPTION_PREFIX + "column names and column types size does not match.");
        }

        return validator;
    }

    private Validator validatePartitionColumns() {
        Validator validator = new Validator();

        if (this.partitionColumns != DEFAULT_PARTITION_COLUMNS) {
            validator.checkNotNull(
                partitionColumns,
                EXCEPTION_PREFIX + "Passed a null reference for partition column names.");

            if (partitionColumns != null) {
                validator.checkArgument(!partitionColumns.isEmpty(),
                    EXCEPTION_PREFIX + "Partition column names collection is empty.");

                if (!partitionColumns.isEmpty()) {
                    validator.checkArgument(
                        partitionColumns.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
                        EXCEPTION_PREFIX
                            + "Partition columns names contains at least one element that is null, "
                            + "empty, or contains only whitespace characters.");

                    if (columnNames != null && columnNames.length > 0) {
                        List<String> columnNamesList = Arrays.asList(columnNames);
                        validator.checkArgument(
                            partitionColumns.stream()
                                .filter(
                                    (partitionColName) ->
                                        !StringUtils.isNullOrWhitespaceOnly(partitionColName))
                                .anyMatch(columnNamesList::contains),
                            EXCEPTION_PREFIX
                                + "None of the partition columns were included in table column "
                                + "names definition.");
                    }
                }
            }
        }
        return validator;
    }
}
