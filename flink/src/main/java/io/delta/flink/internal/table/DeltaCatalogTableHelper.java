package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.internal.ConnectorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public final class DeltaCatalogTableHelper {

    private DeltaCatalogTableHelper() {}

    public static StructType resolveDeltaSchemaFromDdl(ResolvedCatalogTable table) {

        // contains physical, computed and metadata columns that were defined in DDL
        List<Column> columns = table.getResolvedSchema().getColumns();
        validateDuplicateColumns(columns);

        List<String> names = new LinkedList<>();
        List<LogicalType> types = new LinkedList<>();

        for (Column column : columns) {
            // We care only about physical columns. As stated in Flink doc - metadata columns and
            // computed columns are excluded from persisting. Therefore, a computed column cannot
            // be the target of an INSERT INTO statement.
            if (column instanceof PhysicalColumn) {
                names.add(column.getName());
                types.add(column.getDataType().getLogicalType());
            }
        }

        return io.delta.flink.sink.internal.SchemaConverter.toDeltaDataType(
            RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]))
        );
    }

    public static Pair<String[], DataType[]> resolveFlinkTypesFromDelta(StructType tableSchema) {
        StructField[] fields = tableSchema.getFields();
        String[] columnNames = new String[fields.length];
        DataType[] columnTypes = new DataType[fields.length];
        int i = 0;
        for (StructField field : fields) {
            columnNames[i] = field.getName();
            columnTypes[i] = LogicalTypeDataTypeConverter.toDataType(
                io.delta.flink.source.internal.SchemaConverter.toFlinkDataType(field.getDataType(),
                field.isNullable()));
            i++;
        }

        return Pair.of(columnNames, columnTypes);
    }

    public static void validateDuplicateColumns(List<Column> columns) {
        final List<String> names =
            columns.stream().map(Column::getName).collect(Collectors.toList());
        final List<String> duplicates =
            names.stream()
                .filter(name -> Collections.frequency(names, name) > 1)
                .distinct()
                .collect(Collectors.toList());
        if (duplicates.size() > 0) {
            throw new CatalogException(
                String.format(
                    "Schema must not contain duplicate column names. Found duplicates: %s",
                    duplicates));
        }
    }

    public static void commitToDeltaLog(
            DeltaLog deltaLog,
            Metadata updatedMetadata,
            Name setTableProperties) {

        OptimisticTransaction transaction = deltaLog.startTransaction();
        transaction.updateMetadata(updatedMetadata);
        Operation opName =
            prepareDeltaLogOperation(setTableProperties, updatedMetadata);
        transaction.commit(
            Collections.singletonList(updatedMetadata),
            opName,
            ConnectorUtils.ENGINE_INFO
        );
    }

    /**
     * Prepares {@link Operation} object for current transaction
     *
     * @param opName name of the operation.
     * @param metadata Delta Table Metadata action.
     * @return {@link Operation} object for current transaction.
     */
    public static Operation prepareDeltaLogOperation(Name opName, Metadata metadata) {
        Map<String, String> operationParameters = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // TODO DC - consult with Scott this "mode" thing. This is what Sink's
            //  GlobalCommitter does.
            //operationParameters.put("mode", objectMapper.writeValueAsString(APPEND_MODE));
            // we need to perform mapping to JSON object twice for partition columns. First to map
            // the list to string type and then again to make this string JSON encoded
            // e.g. java array of ["a", "b"] will be mapped as string "[\"a\",\"c\"]"
            operationParameters.put("isManaged", objectMapper.writeValueAsString(false));
            operationParameters.put("description",
                objectMapper.writeValueAsString(metadata.getDescription()));

            // TODO DC - consult this with Scott, Delta seems to expect "[]" and "{}" rather then
            //  [] and {}.
            operationParameters.put("properties",
                objectMapper.writeValueAsString(
                    objectMapper.writeValueAsString(metadata.getConfiguration())));
            operationParameters.put("partitionBy", objectMapper.writeValueAsString(
                objectMapper.writeValueAsString(metadata.getPartitionColumns())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot map object to JSON", e);
        }
        return new Operation(opName, operationParameters, Collections.emptyMap());
    }

}
