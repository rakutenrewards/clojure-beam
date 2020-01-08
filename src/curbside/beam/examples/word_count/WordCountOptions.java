package curbside.beam.examples.word_count;

import org.apache.beam.sdk.options.PipelineOptions;

/** Note: coming soon; we won't have to define Java options like such and we can drive off of clojure.spec. */
public interface WordCountOptions extends PipelineOptions {

  String getInputFile();

  void setInputFile(String val);

  String getOutput();

  void setOutput(String val);

}
