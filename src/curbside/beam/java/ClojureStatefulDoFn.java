package curbside.beam.java;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class ClojureStatefulDoFn extends DoFn<Object, Object> {

  private static final long serialVersionUID = 0;

  private final ClojureDoFn cljDoFn;

  @StateId("dofnState")
  private final StateSpec<ValueState<IPersistentMap>> dofnStateSpec = StateSpecs.value(new NippyCustomCoder());

  public ClojureStatefulDoFn(Map<String, Var> fns_map, Map runtimeParameters) {
    this(fns_map, runtimeParameters, Collections.emptyMap());
  }

  public ClojureStatefulDoFn(Map<String, Var> fns_map, Map runtimeParameters, Map<String, PCollectionView<?>> sideInputs) {
    super();
    this.cljDoFn = new ClojureDoFn(fns_map, runtimeParameters, sideInputs);
  }

  @Setup
  public void setup() {
    cljDoFn.setup();
  }

  @StartBundle
  public void startBundle() {
    cljDoFn.startBundle();
  }

  @ProcessElement
  public void processElement(ProcessContext c,
                             BoundedWindow window,
                             @StateId("dofnState") ValueState<IPersistentMap> dofnState) {
    if (null == dofnState) {
      throw new IllegalStateException("dofnState is null");
    }

    cljDoFn.invokeProcessElement(c, window, PersistentHashMap.create(Keyword.intern("dofn-state"), dofnState));
  }

  @FinishBundle
  public void finishBundle() {
    cljDoFn.finishBundle();
  }

  @Teardown
  public void teardown() {
    cljDoFn.teardown();
  }
}
