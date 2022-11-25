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

import io.delta.flink.e2e.datagenerator.TestRowType;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;

class DeltaSinkFactory {

    static DeltaSinkInternal<RowData> createDeltaSink(
            String deltaTablePath,
            boolean isTablePartitioned) {

        if (isTablePartitioned) {
            return DeltaSink.forRowData(
                    new Path(deltaTablePath),
                    getHadoopConf(),
                    TestRowType.ROW_TYPE
                )
                .withPartitionColumns("country", "birthYear")
                .build();
        } else {
            return DeltaSink.forRowData(
                    new Path(deltaTablePath),
                    getHadoopConf(),
                    TestRowType.ROW_TYPE
                )
                .build();
        }
    }

    private static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
        return conf;
    }

}
