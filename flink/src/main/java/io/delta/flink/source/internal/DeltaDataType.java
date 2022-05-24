package io.delta.flink.source.internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;

public enum DeltaDataType {
    ARRAY(ArrayType.class),
    BIGINT(LongType.class),
    VAR_BINARY(BinaryType.class),
    BOOLEAN(BooleanType.class),
    DATE(DateType.class),
    DECIMAL(DecimalType.class),
    DOUBLE(DoubleType.class),
    FLOAT(FloatType.class),
    INTEGER(IntegerType.class),
    MAP(MapType.class),
    NULL(NullType.class),
    SMALLINT(ShortType.class),
    TIMESTAMP(TimestampType.class),
    TINYINT(ByteType.class),
    VAR_CHAR(StringType.class),
    ROW(StructType.class),
    OTHER(null);

    private static final Map<Class<?>, DeltaDataType> LOOKUP_MAP;

    static {
        Map<Class<?>, DeltaDataType> tmpMap = new HashMap<>();
        for (DeltaDataType type : DeltaDataType.values()) {
            tmpMap.put(type.deltaDataTypeClass, type);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final Class<? extends DataType> deltaDataTypeClass;

    DeltaDataType(Class<? extends DataType> deltaDataTypeClass) {
        this.deltaDataTypeClass = deltaDataTypeClass;
    }

    public static DeltaDataType instanceFrom(Class<? extends DataType> deltaActionName) {
        return LOOKUP_MAP.getOrDefault(deltaActionName, OTHER);
    }
}
