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

package io.delta.flink.e2e.sink;

import java.util.Collections;

import io.delta.flink.e2e.datagenerator.TestDataGenerator;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each of the source operators outputs given number of records in given number of checkpoints.
 * In other words, between two consecutive checkpoints numberOfRecords / recordsPerCheckpoint
 * records are emitted per operator. When all records are emitted, the source waits for one more
 * checkpoint until it finishes.
 */
class CheckpointCountingSource
    extends RichParallelSourceFunction<RowData>
    implements CheckpointListener, CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCountingSource.class);

    private final int numberOfCheckpoints;

    private final int recordsPerCheckpoint;

    private final boolean isFailoverScenario;

    private final TestDataGenerator testDataGenerator;

    private ListState<Integer> nextValueState;

    private int nextValue;

    private volatile boolean isCanceled;

    private volatile boolean waitingForCheckpoint;

    CheckpointCountingSource(
            int numberOfRecords,
            int numberOfCheckpoints,
            boolean isFailoverScenario,
            TestDataGenerator testDataGenerator) {

        this.numberOfCheckpoints = numberOfCheckpoints;
        this.recordsPerCheckpoint = numberOfRecords / numberOfCheckpoints;
        this.isFailoverScenario = isFailoverScenario;
        this.testDataGenerator = testDataGenerator;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextValue = nextValueState.get().iterator().next();
        }
        waitingForCheckpoint = false;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        if (isFailoverScenario &&
            getRuntimeContext().getIndexOfThisSubtask() == 0 &&
            getRuntimeContext().getAttemptNumber() == 0) {
            runWithFailover(ctx);
        } else {
            runWithoutFailover(ctx);
        }
        LOG.info("Source task done; subtask={}.", getRuntimeContext().getIndexOfThisSubtask());
    }

    private void runWithFailover(SourceContext<RowData> ctx) throws InterruptedException {
        LOG.info("Run with failover; subtask={}; attempt={}.",
            getRuntimeContext().getIndexOfThisSubtask(),
            getRuntimeContext().getAttemptNumber());
        sendRecordsUntil((int) (numberOfCheckpoints * 0.5), ctx);
        synchronized (ctx.getCheckpointLock()) {
            // Let's ensure that the records emitted below are not committed.
            emitRecordsBatch(123, ctx);
            LOG.info("Throwing exception to cause job restart.");
            throw new RuntimeException("Designated Exception");
        }
    }

    private void runWithoutFailover(SourceContext<RowData> ctx) throws InterruptedException {
        LOG.info("Run without failover; subtask={}; attempt={}.",
            getRuntimeContext().getIndexOfThisSubtask(),
            getRuntimeContext().getAttemptNumber());
        sendRecordsUntil(numberOfCheckpoints, ctx);
        idleUntilNextCheckpoint(ctx);
    }

    private void sendRecordsUntil(int targetCheckpoints, SourceContext<RowData> ctx)
        throws InterruptedException {
        while (!isCanceled && nextValue < targetCheckpoints * recordsPerCheckpoint) {
            synchronized (ctx.getCheckpointLock()) {
                emitRecordsBatch(recordsPerCheckpoint, ctx);
                waitingForCheckpoint = true;
            }
            LOG.debug("Waiting for checkpoint to complete; subtask={}.",
                getRuntimeContext().getIndexOfThisSubtask());
            while (waitingForCheckpoint) {
                Thread.sleep(1);
            }
        }
    }

    private void emitRecordsBatch(int batchSize, SourceContext<RowData> ctx) {
        for (int i = 0; i < batchSize; ++i) {
            RowData row = testDataGenerator.get(i);
            ctx.collect(row);
            nextValue++;
        }
        LOG.debug("Emitted {} records (total {}); subtask={}.", batchSize, nextValue,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    private void idleUntilNextCheckpoint(SourceContext<RowData> ctx) throws InterruptedException {
        // Idle until the next checkpoint completes to avoid any premature job termination and
        // race conditions.
        LOG.info("Waiting for an additional checkpoint to complete; subtask={}.",
            getRuntimeContext().getIndexOfThisSubtask());
        synchronized (ctx.getCheckpointLock()) {
            waitingForCheckpoint = true;
        }
        while (waitingForCheckpoint) {
            Thread.sleep(1L);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
        LOG.debug("state snapshot done; checkpointId={}; subtask={}.",
            context.getCheckpointId(),
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        waitingForCheckpoint = false;
        LOG.debug("Checkpoint {} complete; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        LOG.debug("Checkpoint {} aborted; subtask={}.", checkpointId,
            getRuntimeContext().getIndexOfThisSubtask());
        CheckpointListener.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}
