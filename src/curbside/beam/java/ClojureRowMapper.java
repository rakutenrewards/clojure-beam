package curbside.beam.java;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Map;

public final class ClojureRowMapper implements JdbcIO.RowMapper<Object>, Serializable {

    private final Var fn;
    private final Map runtimeParameters;

    public ClojureRowMapper(Var fn, Map runtimeParameters) {
        this.fn = fn;
        this.runtimeParameters = runtimeParameters;
    }

    @Override
    public Object mapRow(ResultSet resultSet) throws Exception {
        return fn.invoke(PersistentHashMap.create(
            Keyword.intern("result-set"), resultSet,
            Keyword.intern("runtime-parameters"), runtimeParameters));
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        ClojureRequire.require_(this.fn);
    }

}
