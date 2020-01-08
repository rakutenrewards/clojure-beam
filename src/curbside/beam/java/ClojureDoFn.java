package curbside.beam.java;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class ClojureDoFn extends DoFn<Object, Object> {

  private static final long serialVersionUID = 0;

  private final Map runtimeParameters;
  private final Var processElement;
  private final Var startBundle;
  private final Var finishBundle;
  private final Var setup;
  private final Var teardown;
  private final Map<String, PCollectionView<?>> sideInputs;

  public ClojureDoFn(Map<String, Var> fns_map, Map runtimeParameters) {
    this(fns_map, runtimeParameters, Collections.emptyMap());
  }

  public ClojureDoFn(Map<String, Var> fns_map, Map runtimeParameters, Map<String, PCollectionView<?>> sideInputs) {
    super();
    this.processElement = fns_map.get("processElementFn");
    this.startBundle = fns_map.get("startBundleFn");
    this.finishBundle = fns_map.get("finishBundleFn");
    this.setup = fns_map.get("setupFn");
    this.teardown = fns_map.get("teardownFn");
    this.runtimeParameters = runtimeParameters;
    if (sideInputs != null && !(sideInputs instanceof Serializable))
      throw new RuntimeException("sideInputs must be Serializable map");
    this.sideInputs = sideInputs;
  }

  @Setup
  public void setup() {
    ClojureRequire.require_(processElement);
    ClojureRequire.require_(startBundle);
    ClojureRequire.require_(finishBundle);
    ClojureRequire.require_(setup);
    ClojureRequire.require_(teardown);

    if (setup != null)
      setup.invoke(PersistentHashMap.create(Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  @StartBundle
  public void startBundle() {
    if (startBundle != null)
      startBundle.invoke(PersistentHashMap.create(Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    invokeProcessElement(c, window, PersistentHashMap.create());
  }

  void invokeProcessElement(ProcessContext c, BoundedWindow window, IPersistentMap params) {
    processElement.invoke(params
      .assoc(Keyword.intern("process-context"), c)
      .assoc(Keyword.intern("current-window"), window)
      .assoc(Keyword.intern("runtime-parameters"), runtimeParameters)
      .assoc(Keyword.intern("side-inputs"), sideInputs));
  }

  @FinishBundle
  public void finishBundle() {
    if (finishBundle != null)
      finishBundle.invoke(PersistentHashMap.create(Keyword.intern("runtime-parameters"), runtimeParameters));
  }

  @Teardown
  public void teardown() {
    if (teardown != null)
      teardown.invoke(PersistentHashMap.create(Keyword.intern("runtime-parameters"), runtimeParameters));
  }
}
