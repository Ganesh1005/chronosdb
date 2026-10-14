//! `chronos` — ChronosDB server and CLI.
//!
//! ```text
//! chronos serve --id 1 --client-addr 127.0.0.1:7001 --peer-addr 127.0.0.1:7101 \
//!     --peer 2=127.0.0.1:7102 --peer 3=127.0.0.1:7103 --data-dir ./data1
//! chronos put    --addrs 127.0.0.1:7001 key value
//! chronos get    --addrs 127.0.0.1:7001 key
//! chronos delete --addrs 127.0.0.1:7001 key
//! chronos get-at --addrs 127.0.0.1:7001 key version
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use chronos_net::client::Client;
use chronos_net::server::{run_server, ServerConfig};

fn usage() -> ! {
    eprintln!("usage:");
    eprintln!("  chronos serve --id ID --client-addr ADDR --peer-addr ADDR");
    eprintln!("      [--peer ID=ADDR]... --data-dir DIR [--snapshot-threshold N]");
    eprintln!("  chronos put --addrs A1,A2 key value");
    eprintln!("  chronos get --addrs A1,A2 key");
    eprintln!("  chronos delete --addrs A1,A2 key");
    eprintln!("  chronos get-at --addrs A1,A2 key version");
    eprintln!("  chronos bench --addrs A1,A2 [--n 10000] [--concurrency 8] [--label NAME]");
    std::process::exit(2);
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage();
    }
    match args[1].as_str() {
        "serve" => serve(&args[2..]),
        "put" | "get" | "delete" | "get-at" => cli(&args[1], &args[2..]),
        "bench" => bench(&args[2..]),
        _ => usage(),
    }
}

fn serve(args: &[String]) {
    let id: u64 = flag(args, "--id").unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
    let client_addr: SocketAddr =
        flag(args, "--client-addr").unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
    let peer_addr: SocketAddr =
        flag(args, "--peer-addr").unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
    let data_dir: PathBuf =
        flag(args, "--data-dir").unwrap_or_else(|| usage()).into();
    let snapshot_threshold: u64 =
        flag(args, "--snapshot-threshold").map(|s| s.parse().unwrap_or(1000)).unwrap_or(1000);
    let mut peers = HashMap::new();
    for w in args.windows(2) {
        if w[0] == "--peer" {
            let mut it = w[1].splitn(2, '=');
            let pid: u64 = it.next().unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
            let addr: SocketAddr = it.next().unwrap_or_else(|| usage()).parse().unwrap_or_else(|_| usage());
            if pid == id {
                eprintln!("--peer must not list self");
                usage();
            }
            peers.insert(pid, addr);
        }
    }
    let cfg = ServerConfig { id, client_addr, peer_addr, peers, data_dir, snapshot_threshold };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            // Never signaled; the server runs until killed.
            if let Err(e) = run_server(cfg, rx).await {
                eprintln!("server error: {e}");
                std::process::exit(1);
            }
        });
}

fn parse_addrs(args: &[String]) -> Vec<(u64, SocketAddr)> {
    flag(args, "--addrs")
        .unwrap_or_else(|| usage())
        .split(',')
        .enumerate()
        .map(|(i, s)| {
            let s = s.trim();
            if let Some((id, addr)) = s.split_once('=') {
                (
                    id.parse().unwrap_or_else(|_| usage()),
                    addr.parse().unwrap_or_else(|_| usage()),
                )
            } else {
                // Without explicit IDs the client tries every address; the
                // NotLeader hint can't be followed directly.
                (i as u64, s.parse().unwrap_or_else(|_| usage()))
            }
        })
        .collect()
}

fn bench(args: &[String]) {
    let addrs = parse_addrs(args);
    let n: usize = flag(args, "--n").map(|s| s.parse().unwrap_or(5000)).unwrap_or(5000);
    let concurrency: usize =
        flag(args, "--concurrency").map(|s| s.parse().unwrap_or(8)).unwrap_or(8);
    let label = flag(args, "--label").unwrap_or_else(|| "bench".to_string());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            // Warmup.
            {
                let mut c = Client::with_ids(addrs.clone());
                for i in 0..200 {
                    let k = format!("w{i}");
                    c.put(k.into_bytes(), b"warm".to_vec()).await.expect("warmup");
                }
            }
            let mut latencies: Vec<u128> = Vec::with_capacity(n);
            let start = std::time::Instant::now();
            // Bounded concurrency via chunks.
            let mut done = 0;
            while done < n {
                let batch = (n - done).min(concurrency);
                let mut tasks = Vec::new();
                for b in 0..batch {
                    let addrs = addrs.clone();
                    let label = label.clone();
                    let i = done + b;
                    tasks.push(tokio::spawn(async move {
                        let mut c = Client::with_ids(addrs);
                        let k = format!("{label}/k{i:06}");
                        let t0 = std::time::Instant::now();
                        c.put(k.into_bytes(), vec![0u8; 64]).await.expect("put");
                        t0.elapsed().as_micros()
                    }));
                }
                for t in tasks {
                    latencies.push(t.await.expect("task"));
                }
                done += batch;
            }
            let elapsed = start.elapsed();
            latencies.sort_unstable();
            let pct = |p: f64| latencies[((p * latencies.len() as f64) as usize).min(latencies.len() - 1)];
            println!("label={label} n={n} concurrency={concurrency}");
            println!("elapsed_ms={}", elapsed.as_millis());
            println!("throughput_puts_per_sec={:.1}", n as f64 / elapsed.as_secs_f64());
            println!("latency_us_p50={} p95={} p99={}", pct(0.50), pct(0.95), pct(0.99));
        });
}

fn cli(cmd: &str, args: &[String]) {
    let addrs = parse_addrs(args);
    let mut positional: Vec<&String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i].starts_with("--") {
            i += 2; // skip flag and its value
        } else {
            positional.push(&args[i]);
            i += 1;
        }
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async {
            let mut c = Client::with_ids(addrs);
            let res: Result<(), chronos_net::client::ClientError> = async {
                match cmd {
                    "put" => {
                        if positional.len() != 2 {
                            usage();
                        }
                        let v = c.put(positional[0].as_bytes().to_vec(), positional[1].as_bytes().to_vec()).await?;
                        println!("committed at version {v}");
                        Ok(())
                    }
                    "get" => {
                        if positional.len() != 1 {
                            usage();
                        }
                        match c.get(positional[0].as_bytes().to_vec()).await? {
                            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                            None => println!("(not found)"),
                        }
                        Ok(())
                    }
                    "delete" => {
                        if positional.len() != 1 {
                            usage();
                        }
                        let v = c.delete(positional[0].as_bytes().to_vec()).await?;
                        println!("committed at version {v}");
                        Ok(())
                    }
                    "get-at" => {
                        if positional.len() != 2 {
                            usage();
                        }
                        let version: u64 = positional[1].parse().unwrap_or_else(|_| usage());
                        match c.get_at_version(positional[0].as_bytes().to_vec(), version).await? {
                            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                            None => println!("(not found)"),
                        }
                        Ok(())
                    }
                    _ => usage(),
                }
            }
            .await;
            if let Err(e) = res {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        });
}
