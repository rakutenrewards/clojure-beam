package curbside.beam.java;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.IOException;
import java.util.Map;

public class ClojureSerializableFunction implements SerializableFunction {

  private final Var fn;
  // TODO: use ValueProvider to runtime parameter
  // See https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#pipeline-io-and-runtime-parameters
  private final Map runtimeParameters;

  public ClojureSerializableFunction(Var fn, Map runtimeParameters) {
    this.fn = fn;
    this.runtimeParameters = runtimeParameters;
  }

  @Override
  public Object apply(Object input) {
    return fn.invoke(PersistentHashMap.create(
      Keyword.intern("input"), input,
      Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  private void readObject(java.io.ObjectInputStream stream)
    throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    // -- "@Setup" --
    ClojureRequire.require_(this.fn);
  }

}
