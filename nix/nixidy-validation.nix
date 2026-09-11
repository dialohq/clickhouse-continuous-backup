{chartSource, ...}: {
  nixidy.target = {
    repository = "https://github.com/dialohq/durable-clickhouse-sink";
    branch = "main";
    rootPath = ".rendered";
  };

  applications.validation = {
    namespace = "durable-clickhouse-sink";
    helm.releases.sink = {
      chart = chartSource;
      values = {
        kafka.bootstrapServers = "kafka.example:9092";
        clickhouse = {
          host = "clickhouse.example";
          credentialsSecret.name = "clickhouse-credentials";
        };
        backup = {
          enabled = true;
          credentialsSecret.name = "clickhouse-backup-credentials";
        };
        recovery.credentialsSecret.name = "clickhouse-recovery-credentials";
        pipelines = [
          {
            name = "records";
            topic = "records.input";
            table = "records";
          }
        ];
      };
    };
  };
}
