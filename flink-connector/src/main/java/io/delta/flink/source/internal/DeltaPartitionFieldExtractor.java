package io.delta.flink.source.internal;

import java.util.Map;

import io.delta.flink.source.internal.exceptions.DeltaSourceExceptionUtils;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.filesystem.RowPartitionComputer;
import org.apache.flink.table.types.logical.LogicalType;

public class DeltaPartitionFieldExtractor<SplitT extends DeltaSourceSplit>
    implements PartitionFieldExtractor<SplitT> {

    @Override
    public Object extract(SplitT split, String fieldName, LogicalType fieldType) {
        Map<String, String> partitionValues = split.getPartitionValues();

        sanityCheck(fieldName, partitionValues);

        return RowPartitionComputer.restorePartValueFromType(partitionValues.get(fieldName),
            fieldType);
    }

    private void sanityCheck(String fieldName, Map<String, String> partitionValues) {
        if (tableHasNoPartitions(partitionValues)) {
            DeltaSourceExceptionUtils.notPartitionedTableException(fieldName);
        }

        if (isNotAPartitionColumn(fieldName, partitionValues)) {
            DeltaSourceExceptionUtils.missingPartitionValueException(fieldName);
        }
    }

    private boolean tableHasNoPartitions(Map<String, String> partitionValues) {
        return partitionValues == null || partitionValues.isEmpty();
    }

    private boolean isNotAPartitionColumn(String fieldName, Map<String, String> partitionValues) {
        return !partitionValues.containsKey(fieldName);
    }
}
