package io.delta.flink.source.internal.utils;

import java.util.Collection;

import io.delta.flink.source.internal.SchemaConverter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.LogicalType;
import static org.apache.flink.util.Preconditions.checkArgument;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * A utility class for Source connector
 */
public final class SourceUtils {

    private SourceUtils() {

    }

    /**
     * Converts Flink's {@link Path} to String
     *
     * @param path Flink's {@link Path}
     * @return String representation of {@link Path}
     */
    public static String pathToString(Path path) {
        checkArgument(path != null, "Path argument cannot be be null.");
        return path.toUri().normalize().toString();
    }

    public static SourceSchema buildSourceSchema(
            Collection<String> userColumnNames,
            StructType logSchema) {
        String[] columnNames;
        LogicalType[] columnTypes;
        if (userColumnNames != null && !userColumnNames.isEmpty()) {
            columnTypes = new LogicalType[userColumnNames.size()];
            int i = 0;
            for (String columnName : userColumnNames) {
                DataType dataType = logSchema.get(columnName).getDataType();
                columnTypes[i++] = SchemaConverter.toFlinkDataType(dataType);
            }
            columnNames = userColumnNames.toArray(new String[0]);
        } else {
            StructField[] fields = logSchema.getFields();
            columnNames = new String[fields.length];
            columnTypes = new LogicalType[fields.length];
            int i = 0;
            for (StructField field : fields) {
                columnNames[i] = field.getName();
                columnTypes[i] = SchemaConverter.toFlinkDataType(field.getDataType());
                i++;
            }
        }

        return new SourceSchema(columnNames, columnTypes);
    }

}
