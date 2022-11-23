package io.delta.flink.e2e.client;

import java.time.Duration;
import java.time.Instant;

import io.delta.flink.e2e.client.parameters.JobParameters;
import io.delta.flink.e2e.client.parameters.JobParametersBuilder;
import org.apache.flink.api.common.JobID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class FlinkClientTest {
    private static final String host = "localhost";
    private static final int port = 8081;
    //private static final String flinkPath = "C:/GID/Dev/";
    private static final String flinkPath = "/home/kristoff/";
    private static final String jarPath = flinkPath + "BatchWordCount.jar";

    private final JobParametersBuilder wordCountJob = JobParametersBuilder
        .builder()
        .withParallelism(1)
        .withEntryPointClassName("org.apache.flink.examples.java.wordcount.WordCount")
        .withArgument("input", "delta/LICENSE.txt")
        .withArgument("output", "delta/flink_test_" + System.currentTimeMillis() + ".txt");

    @Test
    @DisplayName("CustomRestClient should run an example job")
    public void flinkCustomClientTest() throws Exception {
        FlinkClient flinkClient = FlinkClientFactory.getCustomRestClient(host, port);
        String jarId = flinkClient.uploadJar(jarPath);

        JobParameters jobParameters = wordCountJob
            .withJarId(jarId)
            .build();

        JobID jobID = flinkClient.run(jobParameters);

        wait(flinkClient, jobID, Duration.ofSeconds(20));
        flinkClient.deleteJar(jarId);
    }

    protected void wait(FlinkClient flinkClient, JobID jobID, Duration waitTime) throws Exception {
        Instant waitUntil = Instant.now().plus(waitTime);
        while (!flinkClient.isFinished(jobID) && Instant.now().isBefore(waitUntil)) {
            if (flinkClient.isFailed(jobID) || flinkClient.isCanceled(jobID)) {
                Assertions.fail(String.format("Job has failed or has been cancelled; status=%s.",
                    flinkClient.getStatus(jobID)));
            }
            Thread.sleep(1_000L);
        }
        Assertions.assertTrue(flinkClient.isFinished(jobID),
            "Job has not finished in a timely manner.");
    }
}
