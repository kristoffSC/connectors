package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.internal.BaseDeltaSourceBuilder;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;

/**
 * A builder class for {@link DeltaSource} for a stream of {@link RowData}.
 * <p>
 * To create an instance of {@link RowDataDeltaSourceBuilder} call {@link
 * DeltaSource#forRowData(Path, String[], LogicalType[], Configuration)} method providing mandatory
 * arguments. Call {@link RowDataDeltaSourceBuilder#build()} method to get the instance of a {@link
 * DeltaSource} or configure additional options using builder methods and then build the source
 * instance.
 */
public final class RowDataDeltaSourceBuilder
    extends BaseDeltaSourceBuilder<RowData, RowDataDeltaSourceBuilder> {

    private RowDataDeltaSourceBuilder(Path tablePath,
        String[] columnNames, LogicalType[] columnTypes,
        Configuration hadoopConfiguration) {
        super(tablePath, columnNames, columnTypes, hadoopConfiguration);
    }

    /**
     * Creates {@link RowDataDeltaSourceBuilder} for {@link DeltaSource} that produces elements of
     * type {@link RowData}
     */
    static RowDataDeltaSourceBuilder builder(
        Path tablePath, String[] columnNames, LogicalType[] columnTypes,
        Configuration hadoopConfiguration) {
        return new RowDataDeltaSourceBuilder(
            tablePath, columnNames, columnTypes, hadoopConfiguration);
    }

    /*
        We have to expose/override all methods from base class to include them in Javadoc
        generated by sbt-unidoc even though we're simply redirecting the call to super class.
    */

    /**
     * Sets value of "versionAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to given {@link io.delta.standalone.Snapshot}
     * version and read from it.
     *
     * <p>
     * This option is mutual exclusive with {@link #timestampAsOf(String)} option.
     *
     * @param snapshotVersion Delta {@link io.delta.standalone.Snapshot} version to time travel to.
     */
    @Override
    public RowDataDeltaSourceBuilder versionAsOf(long snapshotVersion) {
        return super.versionAsOf(snapshotVersion);
    }

    /**
     * Sets value of "timestampAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to the latest {@link
     * io.delta.standalone.Snapshot} that was generated at or before given timestamp.
     * <p>
     * This option is mutual exclusive with {@link #versionAsOf(long)} option.
     *
     * @param snapshotTimestamp The timestamp we should time travel to.
     */
    @Override
    public RowDataDeltaSourceBuilder timestampAsOf(String snapshotTimestamp) {
        return super.timestampAsOf(snapshotTimestamp);
    }

    /**
     * Sets value of "staringVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from. The values can be string numbers like "1", "10" etc. or
     *                        keyword "latest", where in that case, changes from the latest Delta
     *                        table version will be read.
     */
    @Override
    public RowDataDeltaSourceBuilder startingVersion(String startingVersion) {
        return super.startingVersion(startingVersion);
    }

    /**
     * Sets value of "staringVersion" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option specifies the {@link io.delta.standalone.Snapshot} version from which
     * we want to start reading the changes.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingTimestamp(String)} option.
     *
     * @param startingVersion Delta {@link io.delta.standalone.Snapshot} version to start reading
     *                        changes from.
     */
    public RowDataDeltaSourceBuilder startingVersion(long startingVersion) {
        return super.startingVersion(String.valueOf(startingVersion));
    }

    /**
     * Sets value of "startingTimestamp" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option is used to read only changes from {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp.
     *
     * <p>
     * This option is mutual exclusive with {@link #startingVersion(String)} and {@link
     * #startingVersion(long)} option.
     *
     * @param startingTimestamp The timestamp of {@link io.delta.standalone.Snapshot} that we start
     *                          reading changes from.
     */
    @Override
    public RowDataDeltaSourceBuilder startingTimestamp(String startingTimestamp) {
        return super.startingTimestamp(startingTimestamp);
    }

    /**
     * Sets the value for "updateCheckIntervalMillis" option. Applicable for {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode only. This
     * option to specify check interval (in milliseconds) used for periodic Delta table changes
     * checks.
     *
     * <p>
     * The default value for this option is 5000 ms.
     *
     * @param updateCheckInterval The update check internal in milliseconds.
     */
    @Override
    public RowDataDeltaSourceBuilder updateCheckIntervalMillis(long updateCheckInterval) {
        return super.updateCheckIntervalMillis(updateCheckInterval);
    }

    /**
     * Sets an "ignoreDeletes" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing only {@link
     * io.delta.standalone.actions.RemoveFile} actions.
     *
     * <p> If this option is set to true, Source connector will not throw an exception when
     * processing version containing only {@link io.delta.standalone.actions.RemoveFile} actions
     * regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     *
     * <p>
     * The default value for these options is false.
     */
    @Override
    public RowDataDeltaSourceBuilder ignoreDeletes(boolean ignoreDeletes) {
        return super.ignoreDeletes(ignoreDeletes);
    }

    /**
     * Sets "ignoreChanges" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only. This option allows processing Delta table versions containing both {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions. This option subsumes {@link #ignoreDeletes} option.
     *
     * <p> If this option is set to true, Source connector will not
     * throw an exception when processing version containing combination of {@link
     * io.delta.standalone.actions.RemoveFile} and {@link io.delta.standalone.actions.AddFile}
     * actions regardless of {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     */
    @Override
    public RowDataDeltaSourceBuilder ignoreChanges(boolean ignoreChanges) {
        return super.ignoreChanges(ignoreChanges);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    @Override
    public RowDataDeltaSourceBuilder option(String optionName, String optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     */
    @Override
    public RowDataDeltaSourceBuilder option(String optionName, boolean optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     */
    @Override
    public RowDataDeltaSourceBuilder option(String optionName, int optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     */
    @Override
    public RowDataDeltaSourceBuilder option(String optionName, long optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a list of Delta table partition columns. Value for each partition column will be
     * extracted from Delta table {@link io.delta.standalone.actions.AddFile#getPartitionValues()}
     * map.
     *
     * @param partitions A {@link List} of Delta partition columns.
     */
    @Override
    public RowDataDeltaSourceBuilder partitions(List<String> partitions) {
        return super.partitions(partitions);
    }

    /**
     * Switch source to
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode.
     */
    @Override
    public RowDataDeltaSourceBuilder continuousMode() {
        return super.continuousMode();
    }

    /**
     * @return an instance of {@link DeltaSource} from given builder configuration.
     */
    @Override
    @SuppressWarnings("unchecked")
    public DeltaSource<RowData> build() {

        validateMandatoryOptions();
        validateOptionExclusions();

        // TODO add option value validation. Check for null, empty values, numbers for
        //  "string" like values and string for numeric options.
        ParquetColumnarRowInputFormat<DeltaSourceSplit> format = buildFormat();

        return new DeltaSource<>(tablePath, format,
            (isContinuousMode())
                ? DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER
                : DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER,
            hadoopConfiguration, sourceConfiguration);
    }

    private ParquetColumnarRowInputFormat<DeltaSourceSplit> buildFormat() {
        ParquetColumnarRowInputFormat<DeltaSourceSplit> format;
        if (partitions == null || partitions.isEmpty()) {
            format = buildFormatWithoutPartitions(columnNames, columnTypes, hadoopConfiguration,
                sourceConfiguration);
        } else {
            // TODO PR 8
            throw new UnsupportedOperationException("Partition support will be added later.");
            /*format =
                buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                    sourceConfiguration);*/
        }
        return format;
    }

    // TODO PR 8
    private ParquetColumnarRowInputFormat<DeltaSourceSplit> buildPartitionedFormat(
        String[] columnNames, LogicalType[] columnTypes, Configuration configuration,
        List<String> partitionKeys, DeltaSourceConfiguration sourceConfiguration) {

        // TODO PR 8
        /*return ParquetColumnarRowInputFormat.createPartitionedFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            partitionKeys, new DeltaPartitionFieldExtractor<>(),
            sourceConfiguration.getValue(PARQUET_BATCH_SIZE),
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);*/
        return null;
    }

}
