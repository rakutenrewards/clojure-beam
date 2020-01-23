(ns curbside.beam.nippy-coder
  (:import
   (curbside.beam.java NippyCustomCoder)
   (org.apache.beam.sdk.coders KvCoder)))

(defn make-custom-coder
  "Returns an instance of a CustomCoder using nippy for serialization"
  []
  (NippyCustomCoder/of))

(defn make-kv-coder
  "Returns an instance of a KvCoder using by default nippy for serialization."
  []
  (KvCoder/of (make-custom-coder) (make-custom-coder)))
