//! The `krabka` binary. Everything it does lives in the `krabka_cli` library.

#[tokio::main]
async fn main() {
    std::process::exit(krabka_cli::run().await);
}
