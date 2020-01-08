package curbside.beam.examples.redis_key_inc;

import org.apache.beam.sdk.options.PipelineOptions;

/** Note: coming soon; we won't have to define Java options like such and we can drive off of clojure.spec. */
public interface RedisKeyIncOptions extends PipelineOptions {

  String getRedisUri();

  void setRedisUri(String val);

}
