package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;

enum DeltaActions {

    ADD(AddFile.class),
    REMOVE(RemoveFile.class),
    METADATA(Metadata.class),
    OTHER(null);


    private static final Map<Class<?>, DeltaActions> LOOKUP_MAP;

    static {
        Map<Class<?>, DeltaActions> tmpMap = new HashMap<>();
        for (DeltaActions action : DeltaActions.values()) {
            tmpMap.put(action.deltaActionClass, action);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final Class<?> deltaActionClass;

    DeltaActions(Class<?> deltaActionClass) {
        this.deltaActionClass = deltaActionClass;
    }

    public static DeltaActions instanceFrom(Class<?> deltaActionName) {
        return LOOKUP_MAP.getOrDefault(deltaActionName, OTHER);
    }

}
