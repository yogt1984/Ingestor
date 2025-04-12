use std::fs::OpenOptions;
use std::io::Write;
use serde::Serialize;

pub fn save_feature_as_json<T: Serialize>(feature: &T, filepath: &str) -> std::io::Result<()> {
    let json = serde_json::to_string(feature)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)  // Important: append to the file
        .open(filepath)?;
    writeln!(file, "{}", json)?;
    Ok(())
}
