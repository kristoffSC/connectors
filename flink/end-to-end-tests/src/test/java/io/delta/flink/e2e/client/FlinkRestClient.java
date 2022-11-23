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

package io.delta.flink.e2e.client;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.delta.flink.e2e.client.parameters.JobParameters;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper on {@link RestClusterClient}.
 */
class FlinkRestClient implements FlinkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkRestClient.class);

    private final String host;
    private final int port;

    FlinkRestClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, host);
        config.setInteger(RestOptions.PORT, port);
        return config;
    }

    @Override
    public String uploadJar(String jarPath) throws Exception {
        throw new UnsupportedOperationException("JAR upload not supported.");
    }

    @Override
    public void deleteJar(String jarId) throws Exception {
        throw new UnsupportedOperationException("JAR deleting not supported.");
    }

    @Override
    public JobID run(JobParameters parameters) throws Exception {
        Configuration config = getConfiguration();
        String[] args = getArguments(parameters.getArguments());

        PackagedProgram packagedProgram = PackagedProgram.newBuilder()
                .setJarFile(new File(parameters.getJarPath()))
                .setEntryPointClassName(parameters.getEntryPointClassName())
                .setConfiguration(config)
                .setArguments(args)
                .build();

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                packagedProgram,
                config,
                parameters.getParallelism(),
                false
        );

        try (RestClusterClient<StandaloneClusterId> client =
                     new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            LOGGER.info("Submitting flink job; parameters: {}", parameters);
            JobID jobId = client.submitJob(jobGraph).get(60, TimeUnit.SECONDS);
            LOGGER.info("Job submitted; jobId={}.", jobId);
            return jobId;
        }
    }

    private String[] getArguments(Map<String, String> arguments) {
        return arguments.entrySet().stream()
            .flatMap(entry -> Stream.of(String.format("--%s", entry.getKey()), entry.getValue()))
            .toArray(String[]::new);
    }

    @Override
    public void cancel(JobID jobID) throws Exception {
        Configuration config = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client =
                     new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            client.cancel(jobID).get(60, TimeUnit.SECONDS);
        }
    }

    @Override
    public JobStatus getStatus(JobID jobID) throws Exception {
        Configuration config = getConfiguration();
        try (RestClusterClient<StandaloneClusterId> client =
                     new RestClusterClient<>(config, StandaloneClusterId.getInstance())) {
            return client.getJobStatus(jobID).get(15, TimeUnit.SECONDS);
        }
    }

}
