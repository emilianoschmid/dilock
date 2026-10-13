(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'org.dilock/dilock-clojure)
(def version "0.1.0")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(defn- project-basis [local-maven-repo]
  (b/create-basis
   (cond-> {:project "deps.edn"}
     local-maven-repo
     (assoc :extra {:mvn/repos {"dilock-local" {:url (str "file:" local-maven-repo)}}}))))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [{:keys [local-maven-repo]}]
  (let [basis (project-basis local-maven-repo)]
    (clean nil)
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  ;; A temporary monorepo repository must never leak into the
                  ;; distributable POM. Consumers use their configured repos.
                  :repos {}})
    (b/copy-dir {:src-dirs ["src"]
                 :target-dir class-dir})
    (b/copy-file {:src "../../LICENSE"
                  :target (str class-dir "/META-INF/LICENSE")})
    (b/jar {:class-dir class-dir
            :jar-file jar-file})))
