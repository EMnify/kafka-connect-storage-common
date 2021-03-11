package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TimeSchemaBasedPartitionerTest extends StorageSinkTestBase {

  private static final String TIME_ZONE = "America/Los_Angeles";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(TIME_ZONE);
  private static final String PATH_FORMAT = "'__SCHEMA_NAME__'/'year'=YYYY/'month'=M/'day'=d/'hour'=H/'second'=S";

  private static final int YEAR = 2015;
  private static final int MONTH = DateTimeConstants.APRIL;
  private static final int DAY = 2;
  private static final int HOUR = 1;
  public static final DateTime DATE_TIME =
          new DateTime(YEAR, MONTH, DAY, HOUR, 0, DATE_TIME_ZONE);

  @Test
  public void validateThatSchemaNameReplacedCorrectly() {
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    String timeStr = fmt.print(DATE_TIME);

    String timeFieldName = "timestamp";
    Schema schema = SchemaBuilder.struct().name("my.random.Schema")
            .field(timeFieldName, Schema.STRING_SCHEMA);
    Struct s = new Struct(schema).put(timeFieldName, timeStr);
    SinkRecord record = new SinkRecord(TOPIC, PARTITION, null, null, schema, s,
            0, DATE_TIME.getMillis(), TimestampType.LOG_APPEND_TIME);

    String encodedPartition = getEncodedPartition(timeFieldName, record);
    assertEquals("just_schema/year=2015/month=4/day=2/hour=1/second=0", encodedPartition);
  }

  private Map<String, Object> createConfig(String timeFieldName) {
    Map<String, Object> config = new HashMap<>();

    config.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    config.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record" +
            (timeFieldName == null ? "" : "Field"));
    config.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
    config.put(PartitionerConfig.PATH_FORMAT_CONFIG, PATH_FORMAT);
    config.put(PartitionerConfig.LOCALE_CONFIG, Locale.US.toString());
    config.put(PartitionerConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());
    if (timeFieldName != null) {
      config.put(PartitionerConfig.TIMESTAMP_FIELD_NAME_CONFIG, timeFieldName);
    }
    List<String> mappings = new ArrayList<>();
    mappings.add("my.random.Schema:just_schema");
    config.put(PartitionerConfig.SCHEMA_MAPPING_CONFIG, mappings);
    return config;
  }

  private <T> TimeSchemaBasedPartitioner<T> configurePartitioner(
          TimeSchemaBasedPartitioner<T> partitioner,
          String timeField,
          Map<String, Object> configOverride) {
    if (partitioner == null) {
      partitioner = new TimeSchemaBasedPartitioner<>();
    }
    Map<String, Object> config = createConfig(timeField);
    if (configOverride != null) {
      for (Map.Entry<String, Object> e : configOverride.entrySet()) {
        config.put(e.getKey(), e.getValue());
      }
    }
    partitioner.configure(config);
    return partitioner;
  }

  private String getEncodedPartition(String timeFieldName, SinkRecord r) {
    TimeSchemaBasedPartitioner<String> partitioner = configurePartitioner(
            new TimeSchemaBasedPartitioner<>(), timeFieldName, null);
    return partitioner.encodePartition(r);
  }

  private SinkRecord getSinkRecord() {
    long timestamp = new DateTime(2015, 4, 2, 1, 0,
            0, 0, DateTimeZone.forID(TIME_ZONE)).getMillis();
    return createSinkRecord(timestamp);
  }

  private SinkRecord createSinkRecord(Schema timestampSchema, Object timestamp) {
    Schema schema = createSchemaWithTimestampField(timestampSchema);
    Struct record = createRecordWithTimestampField(schema, timestamp);
    return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L,
            timestamp instanceof Long ? (Long) timestamp : Time.SYSTEM.milliseconds(), TimestampType.CREATE_TIME);
  }

}
