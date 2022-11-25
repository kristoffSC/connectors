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

import java.time.Duration;
import java.util.UUID;

import io.delta.flink.e2e.DeltaConnectorEndToEndTestBase;
import io.delta.flink.e2e.client.parameters.JobParameters;
import io.delta.flink.e2e.client.parameters.JobParametersBuilder;
import io.delta.flink.e2e.data.UserDeltaTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static io.delta.flink.e2e.assertions.DeltaLogAssertions.assertThat;

@RunWith(Parameterized.class)
@DisplayNameGeneration(DisplayNameGenerator.IndicativeSentences.class)
class DeltaSinkStreamingStressEndToEndTest extends DeltaConnectorEndToEndTestBase {

    private static final int INPUT_RECORDS = 10_000;

    private static final String JOB_MAIN_CLASS =
        "io.delta.flink.e2e.sink.DeltaSinkStreamingRandomFailoverJob";

    @DisplayName(
        "Sink Stress test - Connector in streaming mode should add new records to the Delta Table"
    )
    @ParameterizedTest(name = "partitioned table: {0}")
    @ValueSource(booleans = {false})
    void shouldAddNewRecords_StressTest(boolean isPartitioned) throws Exception {

        // GIVEN
        UserDeltaTable userTable = isPartitioned
            ? UserDeltaTable.partitionedByCountryAndBirthYear(deltaTableLocation)
            : UserDeltaTable.nonPartitioned(deltaTableLocation);
        userTable.initializeTable();
        // AND
        long initialDeltaVersion = userTable.getDeltaLog().snapshot().getVersion();
        int initialRecordCount = parquetFileReader.readRecursively(deltaTableLocation).size();
        // AND
        JobParameters jobParameters = streamingJobParameters()
            .withDeltaTablePath(deltaTableLocation)
            .withTablePartitioned(isPartitioned)
            .withInputRecords(INPUT_RECORDS)
            .build();

        // WHEN
        jobID = flinkClient.run(jobParameters);
        wait(Duration.ofMinutes(3));

        // THEN
        assertThat(userTable.getDeltaLog())
            .sinceVersion(initialDeltaVersion)
            .hasRecordCountInParquetFiles(initialRecordCount + INPUT_RECORDS)
            .hasNewRecordCountInOperationMetrics(INPUT_RECORDS)
            .metricsNewFileCountMatchesSnapshotNewFileCount()
            .hasPositiveNumOutputBytesInEachVersion();
    }

    protected void initializeTestDataLocation() {
        testDataLocationPrefix = "flink-connector-e2e-tests/" + UUID.randomUUID();
        deltaTableLocation = "delta/" + testDataLocationPrefix;
    }

    private JobParametersBuilder streamingJobParameters() {
        return JobParametersBuilder.builder()
            .withName(getTestDisplayName())
            .withJarId(jarId)
            .withEntryPointClassName(JOB_MAIN_CLASS);
    }

}
