package io.delta.flink.source.internal.builder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class RowDataFormatBuilder {

    /**
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "RowDataFormatBuilder - ";

    // -------------- Hardcoded Non Public Options ----------
    /**
     * Hardcoded option for {@link RowDataFormat} to threat timestamps as a UTC
     * timestamps.
     */
    protected static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * Hardcoded option for {@link RowDataFormat} to use case-sensitive in column
     * name processing for Parquet files.
     */
    protected static final boolean PARQUET_CASE_SENSITIVE = true;
    // ------------------------------------------------------

    private final String[] columnNames;

    private final LogicalType[] columnTypes;

    private final Configuration hadoopConfiguration;

    private List<String> partitions;

    public RowDataFormatBuilder(String[] columnNames,
        LogicalType[] columnTypes, Configuration hadoopConfiguration) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    /**
     * Set list of partition columns.
     */
    public RowDataFormatBuilder partitions(List<String> partitions) {
        checkNotNull(partitions, EXCEPTION_PREFIX + "partition list cannot be null.");
        checkArgument(partitions.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
            EXCEPTION_PREFIX
                + "List with partition columns contains at least one element that is null, "
                + "empty, or contains only whitespace characters.");

        this.partitions = partitions;
        return this;
    }

    public RowDataFormat build() {
        validateMandatoryOptions();
        RowDataFormat format;
        if (partitions == null || partitions.isEmpty()) {
            format = buildFormatWithoutPartitions(columnNames, columnTypes, hadoopConfiguration);
        } else {
            // TODO PR 8
            throw new UnsupportedOperationException("Partition support will be added later.");
            /*format =
                buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                    sourceConfiguration);*/
        }
        return format;
    }

    private RowDataFormat buildFormatWithoutPartitions(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration) {

        return new RowDataFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            2048, // get this from user...
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    // TODO PR 8
    private RowDataFormat buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        List<String> partitionKeys, DeltaSourceConfiguration sourceConfiguration) {

        // TODO PR 8
        /*return DeltaRowDataFormat.createPartitionedFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            partitionKeys, new DeltaPartitionFieldExtractor<>(),
            sourceConfiguration.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);*/
        return null;
    }

    private void validateMandatoryOptions() {

        // validate against null references
        checkNotNull(columnNames, EXCEPTION_PREFIX + "missing Delta table column names.");
        checkNotNull(columnTypes, EXCEPTION_PREFIX + "missing Delta table column types.");
        checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");

        // validate arrays size
        checkArgument(columnNames.length > 0, EXCEPTION_PREFIX + "empty array with column names.");
        checkArgument(columnTypes.length > 0, EXCEPTION_PREFIX + "empty array with column names.");
        checkArgument(columnNames.length == columnTypes.length,
            EXCEPTION_PREFIX + "column names and column types size does not match.");

        // validate invalid array element
        checkArgument(Stream.of(columnNames)
                .noneMatch(StringUtils::isNullOrWhitespaceOnly),
            EXCEPTION_PREFIX + "Column names array contains at least one element that is null, "
                + "empty, or contains only whitespace characters.");
        checkArgument(Stream.of(columnTypes)
            .noneMatch(Objects::isNull), EXCEPTION_PREFIX + "Column type array contains at "
            + "least one null element.");
    }

}
