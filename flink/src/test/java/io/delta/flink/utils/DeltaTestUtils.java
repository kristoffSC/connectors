package io.delta.flink.utils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class DeltaTestUtils {

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    ///////////////////////////////////////////////////////////////////////////
    // test data utils
    ///////////////////////////////////////////////////////////////////////////

    public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR =
        "/test-data/test-non-partitioned-delta-table-initial-state";
    public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_DIR =
        "/test-data/test-partitioned-delta-table-initial-state";
    public static final String TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR =
        "/test-data/test-non-partitioned-delta-table_1100_records";
    public static final String TEST_PARTITIONED_DELTA_TABLE_SOURCE =
        "/test-data/test-partitioned-delta-table-source";

    public static void initTestForNonPartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForSourcePartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_PARTITIONED_DELTA_TABLE_SOURCE;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForPartitionedTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_TABLE_INITIAL_STATE_P_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

    public static void initTestForNonPartitionedLargeTable(String targetTablePath)
        throws IOException {
        File resourcesDirectory = new File("src/test/resources");
        String initialTablePath =
            resourcesDirectory.getAbsolutePath() + TEST_DELTA_LARGE_TABLE_INITIAL_STATE_DIR;
        FileUtils.copyDirectory(
            new File(initialTablePath),
            new File(targetTablePath));
    }

}
