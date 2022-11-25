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

package io.delta.flink.e2e;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;

import io.delta.flink.e2e.client.FlinkClient;
import io.delta.flink.e2e.client.FlinkClientFactory;
import io.delta.flink.e2e.parquet.AvroParquetFileReader;
import io.delta.flink.e2e.utils.HadoopConfig;
import org.apache.flink.api.common.JobID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.e2e.TestParameters.getJobManagerHost;
import static io.delta.flink.e2e.TestParameters.getJobManagerPort;
import static io.delta.flink.e2e.TestParameters.getTestArtifactPath;
import static io.delta.flink.e2e.TestParameters.getTestS3BucketName;
import static io.delta.flink.e2e.TestParameters.preserveS3Data;
import static io.delta.flink.e2e.utils.AwsUtils.removeS3DirectoryRecursively;

public abstract class DeltaConnectorEndToEndTestBase {

    protected static final Logger LOGGER =
        LoggerFactory.getLogger(DeltaConnectorEndToEndTestBase.class);

    protected static FlinkClient flinkClient;

    protected static String jarId;

    protected static String bucketName;

    protected String testDataLocationPrefix;

    protected String deltaTableLocation;

    protected JobID jobID;

    protected TestInfo testInfo;

    protected AvroParquetFileReader parquetFileReader =
        new AvroParquetFileReader(HadoopConfig.get());


    @BeforeAll
    static void setUpClass() throws Exception {
        flinkClient = FlinkClientFactory.getCustomRestClient(
            getJobManagerHost(), getJobManagerPort());
        jarId = flinkClient.uploadJar(getTestArtifactPath());
        bucketName = getTestS3BucketName();
    }

    @BeforeEach
    protected void setUp(TestInfo testInfo) throws Exception {
        this.testInfo = testInfo;
        initializeTestDataLocation();
    }

    protected abstract void initializeTestDataLocation();

    @AfterEach
    void cleanUp() throws Exception {
        cancelJobIfRunning();
        removeTestDataIfNeeded();
    }

    private void cancelJobIfRunning() throws Exception {
        if (flinkClient != null && jobID != null && !flinkClient.isFinished(jobID)) {
            LOGGER.info("Cancelling job {}.", jobID);
            flinkClient.cancel(jobID);
            LOGGER.info("Job cancelled.");
        }
        jobID = null;
    }

    private void removeTestDataIfNeeded() {
        if (!preserveS3Data()) {
            LOGGER.info("Removing test data in S3 {}", deltaTableLocation);
            removeS3DirectoryRecursively(bucketName, testDataLocationPrefix);
            LOGGER.info("Test data removed.");
        } else {
            LOGGER.info("Skip test data removal.");
        }
    }

    @AfterAll
    static void cleanUpClass() throws Exception {
        if (flinkClient != null && jarId != null) {
            flinkClient.deleteJar(jarId);
        }
    }

    protected String getTestDisplayName() {
        return String.format(
            "%s -> %s -> %s",
            testInfo.getTestClass().map(Class::getSimpleName).orElse("<empty>"),
            testInfo.getTestMethod().map(Method::getName).orElse("<empty>"),
            testInfo.getDisplayName()
        );
    }

    protected void wait(Duration waitTime) throws Exception {
        Instant waitUntil = Instant.now().plus(waitTime);
        while (!flinkClient.isFinished(jobID) && Instant.now().isBefore(waitUntil)) {
            if (flinkClient.isFailed(jobID) || flinkClient.isCanceled(jobID)) {
                Assertions.fail(
                    String.format("Job has failed or has been cancelled; status=%s.",
                        flinkClient.getStatus(jobID))
                );
            }
            Thread.sleep(5_000L);
            LOGGER.info("Waiting until {}", waitUntil);
        }
        Assertions.assertTrue(flinkClient.isFinished(jobID),
            "Job has not finished in a timely manner.");
    }

}
