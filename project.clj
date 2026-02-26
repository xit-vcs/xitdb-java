(defproject io.github.radarroark/xitdb "0.29.0"
  :url "https://github.com/xit-vcs/xitdb-java"
  :description "An immutable database"
  :license {:name "MIT License"
            :url "https://github.com/xit-vcs/xitdb-java/blob/master/LICENSE"}
  :repositories [["clojars" {:url "https://clojars.org/repo"
                             :sign-releases false}]]
  ;; everything in src is included by default, which causes duplicate
  ;; files in the jar since the :java-source-paths includes a sub dir.
  ;; the :jar-exclusions prevents this from happening...
  :jar-exclusions [#"main/.*" #"test/.*"]
  :java-source-paths ["src/main/java"]
  :javac-options ["--release" "17"])
