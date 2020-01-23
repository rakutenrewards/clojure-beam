package curbside.beam.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;


public final class NippyCustomCoder extends CustomCoder<Object> {

  public static NippyCustomCoder of() { return INSTANCE; }

  private static final NippyCustomCoder INSTANCE = new NippyCustomCoder();

  private static final long serialVersionUID = 0;
  private static final IFn FREEZE_TO_OUT = Clojure.var("taoensso.nippy", "freeze-to-out!");
  private static final IFn THAW_FROM_IN = Clojure.var("taoensso.nippy", "thaw-from-in!");

  static {
    ClojureRequire.require((Symbol) Clojure.read("taoensso.nippy"));
  }

  public NippyCustomCoder() {
    super();
  }

  public void encode(Object obj, OutputStream out) {
    FREEZE_TO_OUT.invoke(new DataOutputStream(out), obj);
  }

  public Object decode(InputStream in) {
    return THAW_FROM_IN.invoke(new DataInputStream(in));
  }


  public void verifyDeterministic() {
  }

  public boolean consistentWithEquals() {
    return true;
  }
}
