{rustPlatform, pkg-config, rustfmt, clippy, openssl, cyrus_sasl, rdkafka}:
rustPlatform.buildRustPackage {
  pname = "durable-clickhouse-recovery";
  version = "0.1.0";

  src = ../backup;
  cargoLock.lockFile = ../backup/Cargo.lock;
  nativeBuildInputs = [pkg-config rustfmt clippy];
  buildInputs = [openssl cyrus_sasl rdkafka];

  postCheck = ''
    cargo fmt --check
    cargo clippy --all-targets -- --deny warnings
  '';
}
