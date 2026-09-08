{
  rustPlatform,
  pkg-config,
  rustfmt,
  clippy,
  openssl,
  cyrus_sasl,
  rdkafka,
}:
rustPlatform.buildRustPackage {
  pname = "durable-clickhouse-backup";
  version = "0.1.0";

  src = ../backup;
  cargoLock.lockFile = ../backup/Cargo.lock;
  nativeBuildInputs = [pkg-config rustfmt clippy];
  buildInputs = [openssl cyrus_sasl rdkafka];

  # E2E tests launch dnvr services and run separately in the development shell.
  cargoTestFlags = ["--lib" "--bins"];

  postCheck = ''
    cargo fmt --check
    cargo clippy --all-targets -- --deny warnings
  '';
}
