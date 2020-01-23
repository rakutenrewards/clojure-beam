package curbside.beam.java;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class ClojureDoFn extends DoFn<Object, Object> {

    private static final long serialVersionUID = 0;

    final Map runtimeParameters;
    private final Var processElement;
    private final Var startBundle;
    private final Var finishBundle;
    private final Var setup;
    private final Var teardown;
    @Nullable
    private final Map<String, PCollectionView<?>> sideInputs;
    @Nullable
    private final List<TupleTag<?>> tupleTags;

    public ClojureDoFn(Map<String, Var> fns_map, Map runtimeParameters) {
        this(fns_map, runtimeParameters, emptyMap());
    }

    public ClojureDoFn(Map<String, Var> fns_map, Map runtimeParameters, Map<String, PCollectionView<?>> sideInputs) {
        this(fns_map, runtimeParameters, sideInputs, emptyList());
    }

    public ClojureDoFn(Map<String, Var> fns_map, Map runtimeParameters, Map<String, PCollectionView<?>> sideInputs,
                       List<TupleTag<?>> tupleTags) {
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
        this.tupleTags = tupleTags;
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
            .assoc(Keyword.intern("side-inputs"), sideInputs)
            .assoc(Keyword.intern("tuple-tags"), tupleTags));
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
