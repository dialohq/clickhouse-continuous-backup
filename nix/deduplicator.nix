{stdenvNoCC, lib, jdk, kotlin, apacheKafka}:
stdenvNoCC.mkDerivation {
  pname = "durable-clickhouse-deduplicator";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../deduplicator;
    fileset = ../deduplicator;
  };

  nativeBuildInputs = [jdk kotlin];

  buildPhase = ''
    runHook preBuild
    kafka_classpath="$(printf '%s:' ${apacheKafka}/libs/*.jar)"
    kotlinc -Werror \
      -cp "$kafka_classpath" \
      -include-runtime \
      -d deduplicator.jar \
      $(find src/main/kotlin -name '*.kt')
    jar --update --file deduplicator.jar -C src/main/resources .

    kotlinc -Werror \
      -cp "deduplicator.jar:$kafka_classpath" \
      -d deduplicator-test.jar \
      $(find src/test/kotlin -name '*.kt')
    test_state="$(mktemp -d)"
    java -cp "deduplicator.jar:deduplicator-test.jar:$kafka_classpath" \
      io.dialo.durableclickhouse.DeduplicatorTest "$test_state"
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    install -Dm644 deduplicator.jar $out/app/deduplicator.jar
    runHook postInstall
  '';
}

