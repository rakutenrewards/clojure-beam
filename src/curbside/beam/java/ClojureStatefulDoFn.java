package curbside.beam.java;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class ClojureStatefulDoFn extends DoFn<Object, Object> {

  private static final long serialVersionUID = 0;

  private final ClojureDoFn cljDoFn;
  @Nullable private final Var cljTimerFn;

  // Note: DataflowRunner does not yet support MapState.
  //    https://beam.apache.org/documentation/runners/capability-matrix/#cap-full-what

  @StateId("dofnState")
  private final StateSpec<ValueState<Object>> dofnStateSpec
      = StateSpecs.value(NippyCustomCoder.of());

  @StateId("dofnBagState")
  private final StateSpec<BagState<Object>> dofnBagStateSpec
      = StateSpecs.bag(NippyCustomCoder.of());

  @TimerId("dofnTimer")
  private final TimerSpec dofnTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  public ClojureStatefulDoFn(Map<String, Var> fns_map, Map runtimeParameters) {
    this(fns_map, runtimeParameters, Collections.emptyMap());
  }

  public ClojureStatefulDoFn(Map<String, Var> fns_map, Map runtimeParameters, Map<String, PCollectionView<?>> sideInputs) {
    super();
    this.cljDoFn = new ClojureDoFn(fns_map, runtimeParameters, sideInputs);
    this.cljTimerFn = fns_map.get("onTimerFn");
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
                             @StateId("dofnState") ValueState<Object> dofnState,
                             @StateId("dofnBagState") BagState<Object> dofnBagState,
                             @TimerId("dofnTimer") Timer dofnTimer) {
    if (null == dofnState) {
      throw new IllegalStateException("dofnState is null");
    }

    cljDoFn.invokeProcessElement(c, window, PersistentHashMap.create(
      Keyword.intern("dofn-state"), dofnState,
      Keyword.intern("dofn-bag-state"), dofnBagState,
      Keyword.intern("dofn-timer"), dofnTimer));
  }

  @OnTimer("dofnTimer")
  public void onTimer(OnTimerContext context,
                      @StateId("dofnState") ValueState<Object> dofnState,
                      @StateId("dofnBagState") BagState<Object> dofnBagState,
                      @TimerId("dofnTimer") Timer dofnTimer) {
    if (null == cljTimerFn) {
      throw new IllegalStateException("A timer was set but no onTimerFn was provided.");
    }

    cljTimerFn.invoke(PersistentHashMap.create(
      Keyword.intern("dofn-state"), dofnState,
      Keyword.intern("dofn-bag-state"), dofnBagState,
      Keyword.intern("dofn-timer"), dofnTimer,
      Keyword.intern("current-window"), context.window(),
      Keyword.intern("runtime-parameters"), cljDoFn.runtimeParameters,
      Keyword.intern("timer-context"), context));
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
