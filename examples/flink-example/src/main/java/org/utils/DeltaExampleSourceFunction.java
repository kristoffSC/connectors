/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.utils;

import java.util.Collections;
import java.util.Iterator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * Internal class providing mock implementation for example stream source.
 * <p>
 * This streaming source will be generating events of type {@link Utils#FULL_SCHEMA_ROW_TYPE} with
 * interval of {@link DeltaExampleSourceFunction#NEXT_ROW_INTERVAL_MILLIS} that will be further
 * fed to the Flink job until the parent process is stopped.
 */
public class DeltaExampleSourceFunction extends RichParallelSourceFunction<RowData> implements
    CheckpointedFunction {

    static int NEXT_ROW_INTERVAL_MILLIS = 800;

    private transient ListState<Integer> checkpointedState;

    private int bufferedElements;

    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(Utils.FULL_SCHEMA_ROW_TYPE)
            );

    private volatile boolean cancelled = false;

    @Override
    public void run(SourceContext<RowData> ctx) throws InterruptedException {
        while (!cancelled) {

            int currentVal = this.bufferedElements;
            this.bufferedElements = this.bufferedElements + 1;
            RowData row = CONVERTER.toInternal(
                Row.of(
                    String.valueOf(currentVal),
                    String.valueOf(currentVal),
                    currentVal)
            );
            ctx.collect(row);
            Thread.sleep(NEXT_ROW_INTERVAL_MILLIS);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedState.update(Collections.singletonList(bufferedElements));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Integer> descriptor =
            new ListStateDescriptor<>(
                    "buffered-elements",
                TypeInformation.of(new TypeHint<Integer>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Iterator<Integer> integers = checkpointedState.get().iterator();
            if (integers.hasNext()) {
                this.bufferedElements = integers.next();
            }
        }
    }
}
