(defproject myproject "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "20.09-1.12.0-beta"] ;; added these two
                 [juxt/crux-rocksdb "20.09-1.12.0-beta"]
                 [juxt/crux-http-server "20.09-1.12.0-alpha"]] ;; but otherwise is vanilla "lein new app myproject"
  :main ^:skip-aot myproject.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
