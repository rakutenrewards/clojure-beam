package curbside.beam.java;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

import java.io.IOException;
import java.util.Map;

public final class ClojureCombineFn extends CombineFn<Object, Object, Object> {

  private static final long serialVersionUID = 0;

  private final Var createAccumulatorFn;
  private final Var extractOutputFn;
  private final Var addInputFn;
  private final Var mergeAccumulatorsFn;
  // TODO: use ValueProvider to runtime parameter
  // See https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#pipeline-io-and-runtime-parameters
  private final Map runtimeParameters;

  public ClojureCombineFn(Map<String, Var> fns_map, Map runtimeParameters) {
    super();
    this.createAccumulatorFn = fns_map.get("createAccumulatorFn");
    this.extractOutputFn = fns_map.get("extractOutputFn");
    this.addInputFn = fns_map.get("addInputFn");
    this.mergeAccumulatorsFn = fns_map.get("mergeAccumulatorsFn");
    this.runtimeParameters = runtimeParameters;
  }

  public Object createAccumulator() {
    return createAccumulatorFn.invoke(PersistentHashMap.create(Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  public Object addInput(Object acc, Object input) {
    return addInputFn.invoke(PersistentHashMap.create(
      Keyword.intern("accumulator"), acc,
      Keyword.intern("input"), input,
      Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  public Object mergeAccumulators(Iterable<Object> accs) {
    return mergeAccumulatorsFn.invoke(PersistentHashMap.create(
      Keyword.intern("accumulator-coll"), accs,
      Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  public Object extractOutput(Object acc) {
    return extractOutputFn.invoke(PersistentHashMap.create(
      Keyword.intern("accumulator"), acc,
      Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  private void readObject(java.io.ObjectInputStream stream)
    throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    // -- "@Setup" --
    ClojureRequire.require_(this.createAccumulatorFn);
    ClojureRequire.require_(this.addInputFn);
    ClojureRequire.require_(this.mergeAccumulatorsFn);
    ClojureRequire.require_(this.extractOutputFn);
  }

}
