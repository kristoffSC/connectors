package io.delta.flink.source.internal;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.DataType;

public class SchemaConverter {

    public static LogicalType toFlinkDataType(DataType deltaType) {

        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(deltaType.getClass());
        switch (deltaDataType) {
            case ARRAY:
                boolean containsNull = ((ArrayType) deltaType).containsNull();
                LogicalType elementType = toFlinkDataType(((ArrayType) deltaType).getElementType());
                return
                    new org.apache.flink.table.types.logical.ArrayType(containsNull, elementType);
            case BIGINT:
                return new BigIntType();
            case VAR_BINARY:
                return new BinaryType();
            case BOOLEAN:
                return new BooleanType();
            case DATE:
                return new DateType();
            case DECIMAL:
                return new DecimalType();
            case DOUBLE:
                return new DoubleType();
            case FLOAT:
                return new FloatType();
            case INTEGER:
                return new IntType();
            case MAP:
                // TODO
            case NULL:
                return new NullType();
            case SMALLINT:
                return new SmallIntType();
            case TIMESTAMP:
                return new TimestampType();
            case TINYINT:
                return new TinyIntType();
            case VAR_CHAR:
                return new CharType();

            default:
                throw new UnsupportedOperationException(
                    "Type not supported: " + deltaDataType);
        }


    }

}
