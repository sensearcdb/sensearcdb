//! compaction scan 链路逐步诊断：对 data/sac/0 下的 L0 文件
//! 逐环节跑 `parse_file_name → SacHeader::from_bytes → SacFooter::from_bytes`，
//! 定位「调度器 backlog=0、磁盘有文件」的断点。

use std::path::{Path, PathBuf};

use sensearcdb_common::constants::{SAC_FOOTER_SIZE, SAC_HEADER_SIZE};
use sensearcdb_engine::sac::footer::SacFooter;
use sensearcdb_engine::sac::header::SacHeader;
use sensearcdb_engine::sac::naming::SacNaming;

fn main() {
    let sac_dir = Path::new("data/sac");
    println!("sac_dir exists: {}", sac_dir.exists());

    let mut tenant_dirs = Vec::new();
    if let Ok(rd) = std::fs::read_dir(sac_dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                tenant_dirs.push(p.clone());
                println!("tenant dir: {}", p.display());
            }
        }
    }
    if tenant_dirs.is_empty() {
        println!("!! sac_dir 下没有任何 tenant 子目录 —— scan_sac_dir 将返回空");
        return;
    }

    let mut ok = 0u64;
    let mut fail = 0u64;
    for tenant_dir in &tenant_dirs {
        let Ok(files) = std::fs::read_dir(tenant_dir) else {
            println!("!! read_dir 失败: {}", tenant_dir.display());
            continue;
        };
        let mut paths: Vec<PathBuf> = files.flatten().map(|e| e.path()).collect();
        paths.sort();
        for fp in paths.iter().take(5) {
            let name = fp.file_name().unwrap_or_default().to_string_lossy().to_string();
            if fp.extension().and_then(|e| e.to_str()) != Some("sac") {
                println!("[skip] {name} (非 .sac)");
                continue;
            }
            let parsed = fp
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(SacNaming::parse_file_name);
            println!("[parse_file_name] {name} -> {:?}", parsed.is_some());

            let len = std::fs::metadata(fp).map(|m| m.len()).unwrap_or(0);
            if len < (SAC_HEADER_SIZE + SAC_FOOTER_SIZE) as u64 {
                println!("!! 文件过小 ({len}B) —— read_sac_meta 将返回 None");
                fail += 1;
                continue;
            }
            let hdr = std::fs::read(fp).map(|b| { let arr: [u8; SAC_HEADER_SIZE] = b[..SAC_HEADER_SIZE].try_into().expect("32B"); SacHeader::from_bytes(&arr) });
            match hdr {
                Ok(Some(h)) => {
                    println!(
                        "[header] OK level={:?} write_epoch={} tde={}",
                        h.level,
                        h.write_epoch,
                        h.tde_encrypted
                    );
                }
                Ok(None) => {
                    println!("!! [header] from_bytes 返回 None —— 这就是断点");
                    fail += 1;
                    continue;
                }
                Err(e) => {
                    println!("!! 读文件失败: {e}");
                    fail += 1;
                    continue;
                }
            }
            let tail = std::fs::OpenOptions::new()
                .read(true)
                .open(fp)
                .and_then(|mut f| {
                    use std::io::{Read, Seek, SeekFrom};
                    f.seek(SeekFrom::Start(len - SAC_FOOTER_SIZE as u64))?;
                    let mut buf = [0u8; 64];
                    // SAC_FOOTER_SIZE 常量为准
                    let n = SAC_FOOTER_SIZE as usize;
                    f.read_exact(&mut buf[..n])?;
                    Ok(buf[..n].to_vec())
                });
            match tail {
                Ok(bytes) => match SacFooter::from_bytes(bytes.as_slice().try_into().expect("64B")) {
                    Some(ft) => {
                        println!(
                            "[footer] OK blocks={} min_ts={} max_ts={}",
                            ft.block_count, ft.min_ts, ft.max_ts
                        );
                        ok += 1;
                    }
                    None => {
                        println!("!! [footer] from_bytes 返回 None —— 这就是断点");
                        fail += 1;
                    }
                },
                Err(e) => {
                    println!("!! 读 footer 失败: {e}");
                    fail += 1;
                }
            }
        }
        println!("（{} 共 {} 个文件，仅抽前 5 个诊断）", tenant_dir.display(), paths.len());
    }
    println!("诊断完成: OK={ok} FAIL={fail}");
}
