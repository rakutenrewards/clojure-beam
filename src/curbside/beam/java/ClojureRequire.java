package curbside.beam.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import clojure.lang.Var;

/** Clojure require is not thread-safe; must globally synchronize on requires
 * or else hangs/exceptions during dataflow execution. */
public final class ClojureRequire {

  private static final IFn require = Clojure.var("clojure.core", "require");

  static synchronized void require(Symbol ns) {
    require.invoke(ns);
  }

  /** Require namespace/s for Var. */
  public static synchronized void require_(Var fn) {
    if (fn != null)
      require.invoke(fn.ns.name);
  }

}
