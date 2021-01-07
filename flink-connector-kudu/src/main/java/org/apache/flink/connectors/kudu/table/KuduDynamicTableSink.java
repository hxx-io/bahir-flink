/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.kudu.table;

import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class KuduDynamicTableSink implements DynamicTableSink {

    private final KuduWriterConfig.Builder writerConfigBuilder;
    private final KuduTableInfo tableInfo;
    private final TableSchema flinkSchema;

    public KuduDynamicTableSink(KuduWriterConfig.Builder writerConfigBuilder, KuduTableInfo tableInfo, TableSchema flinkSchema) {
        this.writerConfigBuilder = writerConfigBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduSink upsertKuduSink = new KuduSink(writerConfigBuilder.build(), tableInfo, new UpsertOperationMapper(flinkSchema.getFieldNames()));
        return SinkFunctionProvider.of(upsertKuduSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new KuduDynamicTableSink(writerConfigBuilder,tableInfo,flinkSchema);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }
}
