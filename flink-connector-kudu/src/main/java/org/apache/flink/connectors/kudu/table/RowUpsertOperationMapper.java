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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.Optional;

@Internal
public class RowUpsertOperationMapper extends AbstractSingleOperationMapper<GenericRowData> {

    public RowUpsertOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    @Override
    public Object getField(GenericRowData input, int i) {
        if(input.getField(i) instanceof StringData){
            return input.getString(i).toString();
        }
        return input.getField(i);
    }

    @Override
    public Optional<Operation> createBaseOperation(GenericRowData input, KuduTable table) {
        RowKind kind = input.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            return Optional.of(table.newUpsert());
        }else{
            return Optional.of(table.newDelete());
        }
    }
}
