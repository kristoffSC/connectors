package org.example.source;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

public class DeltaExampleSinkFunction extends RichSinkFunction<RowData> {

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        System.out.println(
            "Delta table row content - f1: " + row.getString(0)
            + ", f2: " + row.getString(1)
            + ", f3: " + row.getInt(2)
        );
    }
}
