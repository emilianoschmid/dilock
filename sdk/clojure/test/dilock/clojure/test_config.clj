(ns dilock.clojure.test-config
  (:require [clojure.string :as string]))

(defn configured-value?
  "Return true only when an optional backend setting has a non-blank value."
  [value]
  (not (string/blank? value)))
