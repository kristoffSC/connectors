package org.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

public final class Utils {

    private Utils() {}

    public static final RowType FULL_SCHEMA_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("f1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f3", new IntType())
    ));

    public static String resolveExampleTableAbsolutePath(String resourcesTableDir) {
        String rootPath = Paths.get(".").toAbsolutePath().normalize().toString();
        return rootPath.endsWith("flink-example") ?
            rootPath + "/src/main/resources/" + resourcesTableDir :
            rootPath + "/examples/flink-example/src/main/resources/" + resourcesTableDir;
    }

    public static void prepareDirs(String tablePath) throws IOException {
        File tableDir = new File(tablePath);
        if (tableDir.exists()) {
            FileUtils.cleanDirectory(tableDir);
        } else {
            tableDir.mkdirs();
        }
    }

    public static void prepareDirs(String sourcePath, String workPath) throws IOException {
        prepareDirs(workPath);
        FileUtils.copyDirectory(new File(sourcePath), new File(workPath));
    }
}
