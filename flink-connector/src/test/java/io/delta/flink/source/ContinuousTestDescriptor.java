package io.delta.flink.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

public class ContinuousTestDescriptor {

    private final int initialDataSize;

    private final List<Descriptor> updateDescriptors = new ArrayList<>();

    public ContinuousTestDescriptor(int initialDataSize) {
        this.initialDataSize = initialDataSize;
    }

    public void add(RowType rowType, List<Row> rows, int expectedCount) {
        updateDescriptors.add(new Descriptor(rowType, rows, expectedCount));
    }

    public List<Descriptor> getUpdateDescriptors() {
        return Collections.unmodifiableList(updateDescriptors);
    }

    public int getInitialDataSize() {
        return initialDataSize;
    }

    public static class Descriptor {

        private final RowType rowType;
        private final List<Row> rows;
        private final int expectedCount;

        public Descriptor(RowType rowType, List<Row> rows, int expectedCount) {
            this.rowType = rowType;
            this.rows = rows;
            this.expectedCount = expectedCount;
        }

        public RowType getRowType() {
            return rowType;
        }

        public List<Row> getRows() {
            return Collections.unmodifiableList(rows);
        }

        public int getExpectedCount() {
            return expectedCount;
        }
    }

}
