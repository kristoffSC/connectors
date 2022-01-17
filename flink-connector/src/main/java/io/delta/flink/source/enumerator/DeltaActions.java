package io.delta.flink.source.enumerator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

enum DeltaActions {

    ADD("AddFile"),
    REMOVE("RemoveFile"),
    CDC("AddCDCFile"),
    OTHER("");


    private static final Map<String, DeltaActions> LOOKUP_MAP;

    static {
        Map<String, DeltaActions> tmpMap = new HashMap<>();
        for (DeltaActions action : DeltaActions.values()) {
            tmpMap.put(action.deltaActionName, action);
        }
        LOOKUP_MAP = Collections.unmodifiableMap(tmpMap);
    }

    private final String deltaActionName;

    DeltaActions(String deltaActionName) {
        this.deltaActionName = deltaActionName;
    }

    public static DeltaActions instanceBy(String deltaActionName) {
        return LOOKUP_MAP.getOrDefault(deltaActionName, OTHER);
    }

}
