/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.connect.storage.partitioner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TimeSchemaBasedPartitioner<T> extends TimeBasedPartitioner<T> {

  private static final Pattern SCHEMA_NAME = Pattern.compile("__SCHEMA_NAME__");

  private final Map<String, String> mappedNames = new HashMap<String, String>();

  @Override
  public void configure(Map<String, Object> config) {
    super.configure(config);
    String mappings = (String)config.getOrDefault(PartitionerConfig.SCHEMA_MAPPING_CONFIG, "");
    for (String p : mappings.split(",")) {
      String[] pair = p.split(":");
      if (pair.length == 2) {
        mappedNames.put(pair[0], pair[1]);
      }
    }
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {
    String basePath = super.encodePartition(sinkRecord, nowInMillis);
    return replaceSchemaName(sinkRecord.valueSchema(), basePath);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return replaceSchemaName(sinkRecord.valueSchema(), super.encodePartition(sinkRecord));
  }

  private String replaceSchemaName(Schema schema, String basePath) {
    if (schema == null || schema.name() == null || basePath == null) {
      return basePath;
    }
    return SCHEMA_NAME.matcher(basePath).replaceAll(
            mappedNames.getOrDefault(schema.name(), schema.name()));
  }
}
