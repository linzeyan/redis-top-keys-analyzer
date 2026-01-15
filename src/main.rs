use indicatif::{ProgressBar, ProgressStyle};
use redis::{self, Connection, Value};
use std::env;

const SCAN_COUNT: u64 = 5000; // 每次 SCAN 的 count hint
const BATCH_SIZE: usize = 2000; // 每批 pipeline key 數
const PROGRESS_EVERY: u64 = 50_000; // 每掃描多少 keys 更新一次進度條
const TOP_N: usize = 10; // 每類型 Top N

fn main() {
    if let Err(err) = run() {
        eprintln!("發生錯誤: {}", err);
        std::process::exit(1);
    }
}

/// Key 類型（只處理常見的六種）
#[derive(Copy, Clone, Debug)]
enum KeyTypeCode {
    String = 0,
    List = 1,
    Set = 2,
    ZSet = 3,
    Hash = 4,
    Stream = 5,
}

impl KeyTypeCode {
    fn all() -> &'static [KeyTypeCode] {
        use KeyTypeCode::*;
        &[String, List, Set, ZSet, Hash, Stream]
    }

    fn name(self) -> &'static str {
        match self {
            KeyTypeCode::String => "string",
            KeyTypeCode::List => "list",
            KeyTypeCode::Set => "set",
            KeyTypeCode::ZSet => "zset",
            KeyTypeCode::Hash => "hash",
            KeyTypeCode::Stream => "stream",
        }
    }

    fn title(self) -> &'static str {
        // 顯示用（大寫）
        match self {
            KeyTypeCode::String => "STRING",
            KeyTypeCode::List => "LIST",
            KeyTypeCode::Set => "SET",
            KeyTypeCode::ZSet => "ZSET",
            KeyTypeCode::Hash => "HASH",
            KeyTypeCode::Stream => "STREAM",
        }
    }
}

/// 將 Redis 回傳的 TYPE 結果(Value)轉成 KeyTypeCode（不分配 String）
///
/// redis 1.x / RESP3 會用 `BulkString(Vec<u8>)` 或 `SimpleString(String)` 表示 "string"/"hash" 等。
fn parse_type_code(v: &Value) -> Option<KeyTypeCode> {
    match v {
        Value::BulkString(b) => match b.as_slice() {
            b"string" => Some(KeyTypeCode::String),
            b"list" => Some(KeyTypeCode::List),
            b"set" => Some(KeyTypeCode::Set),
            b"zset" => Some(KeyTypeCode::ZSet),
            b"hash" => Some(KeyTypeCode::Hash),
            b"stream" => Some(KeyTypeCode::Stream),
            _ => None,
        },
        Value::SimpleString(s) => match s.as_str() {
            "string" => Some(KeyTypeCode::String),
            "list" => Some(KeyTypeCode::List),
            "set" => Some(KeyTypeCode::Set),
            "zset" => Some(KeyTypeCode::ZSet),
            "hash" => Some(KeyTypeCode::Hash),
            "stream" => Some(KeyTypeCode::Stream),
            _ => None,
        },
        _ => None,
    }
}

/// 單一類型的統計
#[derive(Clone, Default)]
struct TypeStats {
    top: Vec<(u64, String)>, // (mem_bytes, key)
    total_mem: u64,
    count: u64,
}

impl TypeStats {
    fn new() -> Self {
        Self::default()
    }

    /// 新增一個 key 的統計，只在進入 Top N 時才 clone key
    fn add_key(&mut self, mem: u64, key: &str) {
        self.count += 1;
        self.total_mem += mem;

        // Top N 還沒滿，直接塞
        if self.top.len() < TOP_N {
            self.top.push((mem, key.to_owned()));
            return;
        }

        // 找目前 Top 中 mem 最小的一筆
        let mut min_idx = 0;
        let mut min_mem = self.top[0].0;
        for (i, (m, _)) in self.top.iter().enumerate().skip(1) {
            if *m < min_mem {
                min_mem = *m;
                min_idx = i;
            }
        }

        // 只有新的 mem 比最小的大才換掉
        if mem > min_mem {
            self.top[min_idx] = (mem, key.to_owned());
        }
    }

    /// 回傳依 mem desc 排序後的 Top N
    fn sorted_top_desc(&self) -> Vec<(u64, String)> {
        let mut v = self.top.clone();
        v.sort_by(|a, b| b.0.cmp(&a.0));
        v
    }
}

/// 所有類型的統計，固定 6 個 slot，避免 HashMap + String type key
struct AllStats {
    inner: [TypeStats; 6],
}

impl AllStats {
    fn new() -> Self {
        Self {
            inner: [
                TypeStats::new(),
                TypeStats::new(),
                TypeStats::new(),
                TypeStats::new(),
                TypeStats::new(),
                TypeStats::new(),
            ],
        }
    }

    fn get_mut(&mut self, t: KeyTypeCode) -> &mut TypeStats {
        &mut self.inner[t as usize]
    }

    fn get(&self, t: KeyTypeCode) -> &TypeStats {
        &self.inner[t as usize]
    }

    fn total_mem(&self) -> u64 {
        self.inner.iter().map(|s| s.total_mem).sum()
    }
}

fn run() -> redis::RedisResult<()> {
    // ------------------------------------------------------------
    // CLI 參數處理：支援 host, host:port, host port
    // ------------------------------------------------------------
    let (host, port) = parse_host_port();
    let redis_url = format!("redis://{}:{}/", host, port);

    println!("嘗試連線 Redis: {}", redis_url);

    // ------------------------------------------------------------
    // 建立連線
    // ------------------------------------------------------------
    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_connection()?;

    println!("✔ Redis 連線成功\n");

    // ------------------------------------------------------------
    // 取得 key 總量（DBSIZE）
    // ------------------------------------------------------------
    let total_keys: u64 = redis::cmd("DBSIZE").query(&mut con)?;
    println!("資料庫共 {} keys\n", format_with_commas(total_keys));

    // ------------------------------------------------------------
    // 建立進度條
    // ------------------------------------------------------------
    let pb = ProgressBar::new(total_keys);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} keys ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    println!("開始 SCAN + PIPELINE MEMORY USAGE + TYPE...\n");

    // ------------------------------------------------------------
    // SCAN 全庫，搭配 pipeline 一次抓 MEMORY USAGE + TYPE
    // ------------------------------------------------------------
    let mut stats = AllStats::new();

    let mut cursor: u64 = 0;
    let mut scanned: u64 = 0;
    let mut errors: u64 = 0;

    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query(&mut con)?;

        cursor = next_cursor;

        if keys.is_empty() {
            if cursor == 0 {
                break;
            }
            continue;
        }

        // 每個 chunk 做一次 pipeline
        for chunk in keys.chunks(BATCH_SIZE) {
            match fetch_mem_and_type_batch(&mut con, chunk) {
                Ok(batch_results) => {
                    for (key, (mem_opt, type_opt)) in chunk.iter().zip(batch_results.into_iter()) {
                        match (mem_opt, type_opt) {
                            (Some(mem), Some(type_code)) => {
                                stats.get_mut(type_code).add_key(mem, key);
                                scanned += 1;
                            }
                            _ => {
                                errors += 1;
                            }
                        }

                        if scanned >= total_keys {
                            pb.set_position(total_keys);
                        } else if scanned.is_multiple_of(PROGRESS_EVERY) {
                            pb.set_position(scanned);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Pipeline 批次錯誤: {}", e);
                    errors += chunk.len() as u64;
                }
            }
        }

        if cursor == 0 {
            break;
        }
    }

    pb.set_position(scanned.min(total_keys));
    pb.finish_with_message("掃描完成");

    println!(
        "\n完成！共掃描 {} keys (錯誤: {})\n",
        format_with_commas(scanned),
        errors
    );
    println!("{}", "=".repeat(120));

    // ------------------------------------------------------------
    // 類型 Top N
    // ------------------------------------------------------------
    for t in KeyTypeCode::all() {
        let st = stats.get(*t);
        if st.count == 0 || st.top.is_empty() {
            continue;
        }

        let top = st.sorted_top_desc();

        println!("\n🔸 {} - Top {}", t.title(), TOP_N);
        println!("{}", "-".repeat(120));
        println!(
            "{:>6} {:>15} {:>20} Key",
            "排名", "記憶體 (MB)", "記憶體 (Bytes)"
        );
        println!("{}", "-".repeat(120));

        for (idx, (mem, key)) in top.iter().enumerate() {
            let mem_mb = *mem as f64 / 1024.0 / 1024.0;
            println!(
                "{:>6} {:>15.3} {:>20} {}",
                idx + 1,
                mem_mb,
                mem,
                truncate_key(key, 80)
            );
        }

        let total_type_mem = st.total_mem;
        let top_mem: u64 = top.iter().map(|(m, _)| *m).sum();
        let top_pct = if total_type_mem > 0 {
            (top_mem as f64 / total_type_mem as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "\n  統計: 此類型共 {} keys, 總記憶體 {:.2} MB",
            format_with_commas(st.count),
            total_type_mem as f64 / 1024.0 / 1024.0
        );
        println!(
            "  Top {} 佔比: {:.2}% ({:.2} MB)",
            TOP_N,
            top_pct,
            top_mem as f64 / 1024.0 / 1024.0
        );
    }

    // ------------------------------------------------------------
    // 總體摘要
    // ------------------------------------------------------------
    println!("\n{}", "=".repeat(120));
    println!("總體摘要");
    println!("{}", "=".repeat(120));
    println!(
        "{:<15} {:>15} {:>20} 佔比",
        "類型", "Keys 數量", "總記憶體 (MB)"
    );
    println!("{}", "-".repeat(120));

    let total_mem = stats.total_mem();

    for t in KeyTypeCode::all() {
        let st = stats.get(*t);
        if st.count == 0 {
            continue;
        }

        let pct = if total_mem > 0 {
            (st.total_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "{:<15} {:>15} {:>20.2} {:>6.2}%",
            t.name(),
            format_with_commas(st.count),
            st.total_mem as f64 / 1024.0 / 1024.0,
            pct
        );
    }

    println!(
        "\n總計: {} keys, {:.2} MB",
        format_with_commas(scanned),
        total_mem as f64 / 1024.0 / 1024.0
    );

    Ok(())
}

/// 針對一批 keys，用 pipeline 一次取得 (MEMORY USAGE, TYPE)
/// 回傳 Vec<(Option<mem_bytes>, Option<KeyTypeCode>)>
fn fetch_mem_and_type_batch(
    con: &mut Connection,
    keys: &[String],
) -> redis::RedisResult<Vec<(Option<u64>, Option<KeyTypeCode>)>> {
    let mut pipe = redis::pipe();

    for key in keys {
        // MEMORY USAGE key
        pipe.cmd("MEMORY").arg("USAGE").arg(key);
        // TYPE key
        pipe.cmd("TYPE").arg(key);
    }

    // Vec<Value> 長度 = 2 * keys.len()
    let values: Vec<Value> = pipe.query(con)?;

    if values.len() != keys.len() * 2 {
        return Err(redis::RedisError::from((
            redis::ErrorKind::TypeError,
            "Pipeline 回傳長度不匹配",
        )));
    }

    let mut result = Vec::with_capacity(keys.len());

    for idx in 0..keys.len() {
        let mem_val = &values[2 * idx];
        let type_val = &values[2 * idx + 1];

        // MEMORY USAGE，一般是 Int；保守多支援 BulkString / SimpleString
        let mem_opt = match mem_val {
            Value::Nil => None,
            Value::Int(i) => Some(*i as u64),
            Value::BulkString(b) => {
                let s = String::from_utf8_lossy(b);
                s.parse::<u64>().ok()
            }
            Value::SimpleString(s) => s.parse::<u64>().ok(),
            _ => None,
        };

        let type_opt = parse_type_code(type_val);

        result.push((mem_opt, type_opt));
    }

    Ok(result)
}

/// 解析 CLI host / port
///
/// 無參數: 127.0.0.1:6379
/// 1 參數: "host" 或 "host:port"
/// 2+ 參數: host port
fn parse_host_port() -> (String, u16) {
    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        return ("127.0.0.1".to_string(), 6379);
    }

    if args.len() == 2 {
        let arg = &args[1];
        if let Some((h, p)) = arg.split_once(':') {
            let port = p.parse::<u16>().unwrap_or(6379);
            (h.to_string(), port)
        } else {
            (arg.to_string(), 6379)
        }
    } else {
        let host = args[1].clone();
        let port = args[2].parse::<u16>().unwrap_or(6379);
        (host, port)
    }
}

/// 千分位格式
fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out_rev = String::new();

    for (i, ch) in s.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            out_rev.push(',');
        }
        out_rev.push(ch);
    }

    out_rev.chars().rev().collect()
}

/// 長 key 截斷
fn truncate_key(key: &str, max_chars: usize) -> String {
    if key.chars().count() <= max_chars {
        key.to_string()
    } else {
        let mut s: String = key.chars().take(max_chars - 3).collect();
        s.push_str("...");
        s
    }
}
