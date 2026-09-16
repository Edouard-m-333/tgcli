use crate::Cli;

pub fn run(cli: &Cli) {
    if cli.output.is_json() {
        println!(
            "{}",
            serde_json::json!({"version": env!("CARGO_PKG_VERSION"), "contract": crate::onboarding::CONTRACT})
        );
    } else {
        println!("tgcli {}", env!("CARGO_PKG_VERSION"));
    }
}
