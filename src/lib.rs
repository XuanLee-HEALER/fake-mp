use std::path::Path;

use anyhow::{Error, Result as AnyResult};
use tokio::{fs::File, io::AsyncWriteExt};

pub struct MapTask {
    pub file_name: String,
    pub file_data: String,
    pub status: TaskStatus,
}

pub enum TaskStatus {
    Idle,
    InProgress,
    Completed,
}

pub struct WcMapResult {
    pub k: String,
    pub v: String,
}

pub type Result<T> = AnyResult<T, Error>;

/// wc的map任务，读取文件内容，将其转换为每个词对应词频为1的Map返回
pub async fn wc_map(_: &str, contents: Vec<u8>) -> Result<Vec<WcMapResult>> {
    let mut result = Vec::new();
    String::from_utf8_lossy(&contents)
        .split(|c: char| !c.is_ascii_alphabetic())
        .filter(|&v| !v.is_empty())
        .for_each(|v| {
            result.push(WcMapResult {
                k: v.to_string(),
                v: "1".to_string(),
            });
        });
    Ok(result)
}

pub async fn wc_reduce(
    output: impl AsRef<Path>,
    key: String,
    values: impl IntoIterator<Item = String>,
) -> Result<()> {
    let count = values.into_iter().count();
    let mut f = File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(output)
        .await?;
    let _ = f
        .write_all(&format!("{} {}", key, count).as_bytes())
        .await?;
    Ok(())
}
