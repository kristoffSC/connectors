package io.delta.flink.source;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import io.delta.flink.DeltaTestUtils;
import io.delta.flink.source.ContinuousTestDescriptor.Descriptor;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.CommitResult;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;

public class DeltaTableUpdater {

    private static final String ENGINE_INFO = "local";
    private static final Configuration configuration = DeltaTestUtils.getHadoopConf();

    private final String deltaTablePath;

    public DeltaTableUpdater(String deltaTablePath) {
        this.deltaTablePath = deltaTablePath;
    }

    public CommitResult writeToTable(Descriptor descriptor) {
        return writeToTable(descriptor.getRowType(), descriptor.getRows());
    }

    public CommitResult writeToTable(RowType rowType, List<Row> rows) {
        try {
            long now = System.currentTimeMillis();
            DeltaLog log = DeltaLog.forTable(configuration, deltaTablePath);

            Operation op = new Operation(Operation.Name.WRITE);
            OptimisticTransaction txn = log.startTransaction();

            Path pathToParquet = writeParquet(deltaTablePath, rowType, rows);

            AddFile addFile =
                AddFile.builder(pathToParquet.getPath(), Collections.emptyMap(), rows.size(), now,
                        true)
                    .build();

            return txn.commit(Collections.singletonList(addFile), op, ENGINE_INFO);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Path writeParquet(String deltaTablePath, RowType rowType, List<Row> rows)
        throws IOException {

        ParquetWriterFactory<RowData> factory =
            ParquetRowDataBuilder.createWriterFactory(rowType, configuration, false);

        Path path = new Path(deltaTablePath, UUID.randomUUID().toString());
        BulkWriter<RowData> writer =
            factory.create(path.getFileSystem().create(path, WriteMode.OVERWRITE));

        DataFormatConverter<RowData, Row> converter = getConverter(rowType);
        for (Row row : rows) {
            writer.addElement(converter.toInternal(row));
        }

        writer.flush();
        writer.finish();

        return path;
    }

    @SuppressWarnings("unchecked")
    private DataFormatConverter<RowData, Row> getConverter(RowType rowType) {
        return (DataFormatConverter<RowData, Row>) DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(rowType));
    }

}
