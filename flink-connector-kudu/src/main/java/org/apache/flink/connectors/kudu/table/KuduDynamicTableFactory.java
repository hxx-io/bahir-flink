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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.table.utils.KuduTableUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KuduDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "kudu-d";

    private static final ConfigOption<String> KUDU_MASTERS = ConfigOptions
            .key("kudu.masters")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of kudu masters");

    private static final ConfigOption<String> KUDU_TABLE = ConfigOptions
            .key("kudu.table")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of kudu table");

    private static final ConfigOption<String> KUDU_HASH_COLUMNS = ConfigOptions
            .key("kudu.hash-columns")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of kudu hash-columns");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        CatalogTable table = context.getCatalogTable();
        validateTable(table);
        String tableName = table.toProperties().get(KUDU_TABLE.key());
        return createTableSink(tableName, table.getSchema(), table.getOptions());
    }

    private DescriptorProperties validateTable(CatalogTable table) {
        Map<String, String> properties = table.toProperties();
        checkNotNull(properties.get(KUDU_MASTERS.key()), "Missing required property " + KUDU_MASTERS);

        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        return descriptorProperties;
    }

    private DynamicTableSink createTableSink(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS.key());
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);
        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);
        return new KuduDynamicTableSink(configBuilder, tableInfo, physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return "kudu-d";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(KUDU_TABLE);
        set.add(KUDU_MASTERS);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(KUDU_HASH_COLUMNS);
        return set;
    }
}
