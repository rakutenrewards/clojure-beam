package curbside.beam.java;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public final class ClojureKafkaSupport {

  private ClojureKafkaSupport() {}

  /** ClojureKafkaDeserializer. */
  public static class ClojureKafkaDeserializer implements Deserializer<Object> {

    private Var deserializeFn;
    private Object deserializeConfig;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      this.deserializeFn = (Var) configs.get("clojure.kafka-deserializer.fn");
      this.deserializeConfig = configs.get("clojure.kafka-deserializer.config");
      ClojureRequire.require_(this.deserializeFn);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
      return this.deserializeFn.invoke(topic, data,
        PersistentHashMap.create(
          Keyword.intern("runtime-config"), this.deserializeConfig));
    }

    @Override
    public void close() { }

  }

  /** ClojureTimestampPolicyFactory. */
  public static final class ClojureTimestampPolicyFactory implements TimestampPolicyFactory<Object, Object> {

    private final Var getTsFn;
    private final Var getWatermarkFn;

    public ClojureTimestampPolicyFactory(Var getTsFn, Var getWatermarkFn) {
      this.getTsFn = getTsFn;
      this.getWatermarkFn = getWatermarkFn;
    }

    @Override
    public TimestampPolicy<Object, Object> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
      return new TimestampPolicy<Object, Object>() {

        private Instant lastTimestampForRecord;

        @Override
        public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<Object, Object> record) {
          this.lastTimestampForRecord = (Instant) getTsFn.invoke(
            this.lastTimestampForRecord != null ? this.lastTimestampForRecord
              : BoundedWindow.TIMESTAMP_MIN_VALUE, record.getKV().getValue());
          return this.lastTimestampForRecord;
        }

        @Override
        public Instant getWatermark(PartitionContext ctx) {
          if (lastTimestampForRecord == null) {
            return previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
          }
          return (Instant) getWatermarkFn.invoke(this.lastTimestampForRecord);
        }
      };
    }

    private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      ClojureRequire.require_(this.getTsFn);
      ClojureRequire.require_(this.getWatermarkFn);
    }

  }
}
