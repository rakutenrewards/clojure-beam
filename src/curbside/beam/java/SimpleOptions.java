package curbside.beam.java;

import org.apache.beam.sdk.options.PipelineOptions;

public interface SimpleOptions extends PipelineOptions {

  String getCustomOptions();

  void setCustomOptions(String clojureOptions);

}
