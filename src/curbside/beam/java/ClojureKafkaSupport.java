package curbside.beam.java;

import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public final class ClojureKafkaSupport {

    private ClojureKafkaSupport() {}

    /**
     * ClojureKafkaDeserializer.
     */
    public static class ClojureKafkaDeserializer implements Deserializer<Object> {

        private Var deserializeFn;
        private Object deserializeConfig;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.deserializeFn = (Var) configs.get("clojure.kafka-deserializer.fn");
            this.deserializeConfig = configs.get("clojure.kafka-deserializer.config");
            ISeq requireVars = RT.seq(configs.get("clojure.kafka-deserializer.require-vars"));
            for (; requireVars != null; requireVars = requireVars.next()) {
                ClojureRequire.require_((Var) requireVars.first());
            }
            ClojureRequire.require_(this.deserializeFn);
        }

        @Override
        public Object deserialize(String topic, byte[] data) {
            return this.deserializeFn.invoke(topic, data,
                PersistentHashMap.create(
                    Keyword.intern("runtime-config"), this.deserializeConfig));
        }

        @Override
        public void close() {}

    }

    /**
     * ClojureKafkaKeySerializer.
     */
    public static class KafkaKeySerializer implements Serializer<Object> {
        private Var serializeFn;
        private Object serializeConfig;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.serializeFn = (Var)configs.get("clojure.kafka-key-serializer.fn");
            this.serializeConfig = configs.get("clojure.kafka-serializer.config");

            ClojureRequire.require_(this.serializeFn);
        }

        @Override
        public byte[] serialize(String topic, Object data) {
            return (byte[]) this.serializeFn.invoke(topic, data,
                PersistentHashMap.create(
                    Keyword.intern("runtime-config"), this.serializeConfig));
        }

        @Override
        public void close() {
        }
    }

    /**
     * ClojureKafkaValueSerializer.
     */
    public static class KafkaValueSerializer implements Serializer<Object> {
        private Var serializeFn;
        private Object serializeConfig;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.serializeFn = (Var)configs.get("clojure.kafka-value-serializer.fn");
            this.serializeConfig = configs.get("clojure.kafka-serializer.config");

            ClojureRequire.require_(this.serializeFn);
        }

        @Override
        public byte[] serialize(String topic, Object data) {
            return (byte[]) this.serializeFn.invoke(topic, data,
                PersistentHashMap.create(
                    Keyword.intern("runtime-config"), this.serializeConfig));
        }

        @Override
        public void close() {
        }
    }

    /**
     * ClojureTimestampPolicyFactory.
     */
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
