package curbside.beam.java;

import clojure.lang.Var;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class ClojureComparator implements Comparator<Object>, Serializable {

  private final Var fn;

  public ClojureComparator(Var fn) {
    this.fn = fn;
  }

  @Override
  public int compare(Object left, Object right) {
    return (int) fn.invoke(left, right);
  }

  private void readObject(java.io.ObjectInputStream stream)
    throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    // -- "@Setup" --
    ClojureRequire.require_(this.fn);
  }

}
