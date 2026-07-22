use axum::{
    extract::{Query, Json, DefaultBodyLimit},
    routing::{get, post},
    Router,
};
use base64::prelude::*;
use sled::Db;
use log::{info, warn, error};

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use lazy_static::lazy_static;
use reqwest;
use flate2::read::GzDecoder;

fn data_dir() -> String {
    std::env::var("DATA_DIR").unwrap_or_else(|_| "/opt".to_string())
}
fn output_file() -> String { format!("{}/sovereign_data.bin", data_dir()) }
fn tasks_state_file() -> String { format!("{}/tasks_state.json", data_dir()) }
fn dump_list_file() -> String { format!("{}/common_crawl_list.txt", data_dir()) }
fn db_path() -> String { format!("{}/sovereign_db", data_dir()) }

const TASK_TIMEOUT_SECONDS: u64 = 7200; // 2 hours
const BATCH_SIZE: i32 = 5;

lazy_static! {
    static ref SLED_DB: Db = sled::Config::new()
        .path(db_path())
        .cache_capacity(256 * 1024 * 1024)
        .open()
        .unwrap();
    static ref TASK_STATE: RwLock<TaskState> = RwLock::new(TaskState::default());
    static ref STATS: RwLock<Stats> = RwLock::new(Stats::new());
    static ref LIVE_WORKERS: RwLock<HashMap<String, WorkerStatus>> = RwLock::new(HashMap::new());
    
    // NEW: In-Memory Accumulator to prevent Sled lock contention
    static ref ACCUMULATOR: RwLock<HashMap<Vec<u8>, u32>> = RwLock::new(HashMap::new());
}

#[derive(Serialize, Deserialize, Clone)]
struct WorkerStatus {
    last_ping_time: u64,
    current_dump: Option<String>,
    current_file_index: Option<i32>,
    ram_usage_mb: Option<f64>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct BatchInfo {
    colab_id: Option<String>,
    dump_id: String,
    start_index: i32,
    end_index: i32,
    start_time: u64,
    last_ping: u64,
    resume_path_index: i32,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct TaskState {
    pending_dumps: Vec<String>,
    current_dump: Option<String>,
    current_dump_total_files: i32,
    next_path_index: i32,
    in_progress_batches: HashMap<String, BatchInfo>,
    completed_dumps: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Stats {
    start_time: u64,
    total_bytes_received: usize,
    total_bytes_written: usize,
    chunks_processed: usize,
    total_wet_files_processed: usize,
    active_colabs: HashSet<String>,
    chunks_per_colab: HashMap<String, usize>,
    last_save_time: u64,
    cached_unique_patterns: usize, // NEW: Cached length to avoid O(n) calls
}

impl Stats {
    fn new() -> Self {
        Stats {
            start_time: current_time(),
            total_bytes_received: 0,
            total_bytes_written: 0,
            chunks_processed: 0,
            total_wet_files_processed: 0,
            active_colabs: HashSet::new(),
            chunks_per_colab: HashMap::new(),
            last_save_time: current_time(),
            cached_unique_patterns: 0,
        }
    }
}

fn current_time() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

// --------------------------------------------------------
// Background Flusher Task
// --------------------------------------------------------
const ACCUMULATOR_FLUSH_LIMIT: usize = 5_000_000; // FIX #4: Force flush at 5M entries to prevent RAM crash

async fn flush_accumulator_to_sled(map_to_flush: HashMap<Vec<u8>, u32>) {
    if map_to_flush.is_empty() { return; }
    let num_patterns = map_to_flush.len();
    info!("Flusher: Writing {} patterns to Sled DB...", num_patterns);
    let start = Instant::now();
    tokio::task::spawn_blocking(move || {
        for (k, v) in map_to_flush {
            let _ = SLED_DB.fetch_and_update(&k, |old| {
                let old_val = match old {
                    Some(bytes) if bytes.len() >= 4 => {
                        let mut arr = [0u8; 4];
                        arr.copy_from_slice(&bytes[..4]);
                        u32::from_le_bytes(arr)
                    },
                    _ => 0,
                };
                Some((old_val + v).to_le_bytes().to_vec())
            });
        }
        let _ = SLED_DB.flush();
    }).await.unwrap();
    info!("Flusher: Sled write complete in {:.2?}", start.elapsed());
}

async fn background_flusher() {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        let map_to_flush = {
            let mut acc = ACCUMULATOR.write().await;
            std::mem::replace(&mut *acc, HashMap::new())
        };

        flush_accumulator_to_sled(map_to_flush).await;

        // Cache the unique pattern count to avoid O(n) on /status
        let count = tokio::task::spawn_blocking(|| SLED_DB.len()).await.unwrap();
        let mut s = STATS.write().await;
        s.cached_unique_patterns = count;
    }
}

use std::time::Instant;

// --------------------------------------------------------
// State Loaders & Dump Management
// --------------------------------------------------------
async fn load_state() {
    if let Ok(data) = fs::read_to_string(tasks_state_file()) {
        if let Ok(state) = serde_json::from_str::<TaskState>(&data) {
            let mut ts = TASK_STATE.write().await;
            *ts = state;
            info!("Loaded Task State. Current dump: {:?}", ts.current_dump);
            return;
        }
    }
    
    if let Ok(data) = fs::read_to_string(dump_list_file()) {
        let mut lines: Vec<String> = data.lines().filter(|l| !l.trim().is_empty()).map(|s| s.trim().to_string()).collect();
        lines.reverse();
        let mut ts = TASK_STATE.write().await;
        ts.pending_dumps = lines;
        info!("Initialized Task State from list. Total Dumps: {}", ts.pending_dumps.len());
        let _ = save_tasks(&ts).await;
    } else {
        warn!("Could not find dump_list_file at {}", dump_list_file());
    }
}

async fn save_tasks(ts: &TaskState) -> Result<(), std::io::Error> {
    let data = serde_json::to_string(ts)?;
    fs::write(tasks_state_file(), data)?;
    Ok(())
}

fn fetch_dump_file_count(dump_id: &str) -> i32 {
    let url = format!("https://data.commoncrawl.org/crawl-data/{}/wet.paths.gz", dump_id);
    info!("Fetching {} to get precise file count...", url);
    match reqwest::blocking::get(&url) {
        Ok(resp) => {
            let mut gz = GzDecoder::new(resp);
            let mut s = String::new();
            if gz.read_to_string(&mut s).is_ok() {
                let count = s.lines().count() as i32;
                info!("Dump {} has exactly {} WET files.", dump_id, count);
                return count;
            }
        },
        Err(e) => error!("Failed to download {}: {}", url, e),
    }
    0
}

async fn initialize_next_dump_if_needed(ts: &mut TaskState) {
    if ts.current_dump.is_none() {
        if !ts.pending_dumps.is_empty() {
            let next_dump = ts.pending_dumps.remove(0);
            let count = tokio::task::spawn_blocking({
                let d = next_dump.clone();
                move || fetch_dump_file_count(&d)
            }).await.unwrap_or(0);
            
            ts.current_dump = Some(next_dump);
            ts.current_dump_total_files = count;
            ts.next_path_index = 0;
            ts.in_progress_batches.clear();
            let _ = save_tasks(ts).await;
            info!("Initialized NEW Dump: {:?}", ts.current_dump);
        }
    }
}

async fn check_task_timeouts(ts: &mut TaskState) {
    let now = current_time();
    let mut timed_out = Vec::new();
    
    for (batch_id, info) in &ts.in_progress_batches {
        if now - info.last_ping > TASK_TIMEOUT_SECONDS {
            timed_out.push(batch_id.clone());
        }
    }
    
    for batch_id in &timed_out {
        warn!("Batch {} timed out. Releasing back to swarm.", batch_id);
        if let Some(info) = ts.in_progress_batches.get_mut(batch_id) {
            info.colab_id = None;
        }
    }
    
    if !timed_out.is_empty() {
        let _ = save_tasks(ts).await;
    }
}

// --------------------------------------------------------
// Binary Generation
// --------------------------------------------------------
fn save_binary_data() -> usize {
    let mut i8_count: u32 = 0;
    let mut i16_count: u32 = 0;
    let mut i32_count: u32 = 0;
    let mut i64_count: u32 = 0;
    let mut i128_count: u32 = 0;
    let mut total_len: u32 = 0;

    info!("Pass 1: Collecting all patterns from Sled DB...");
    let mut entries: Vec<(Vec<u8>, u32)> = Vec::new();
    for item in SLED_DB.iter() {
        if let Ok((k_bytes, v_bytes)) = item {
            if v_bytes.len() >= 4 {
                let mut v_arr = [0u8; 4];
                v_arr.copy_from_slice(&v_bytes[..4]);
                let freq = u32::from_le_bytes(v_arr);
                entries.push((k_bytes.to_vec(), freq));
            }
        }
    }

    if entries.is_empty() {
        warn!("No patterns in DB. Nothing to compile.");
        return 0;
    }

    // FIX #2: SPEC requires sorting by frequency DESCENDING before tier assignment
    entries.sort_unstable_by(|a, b| b.1.cmp(&a.1));

    // Count tiers AFTER sorting
    for (_, freq) in &entries {
        total_len += 1;
        if *freq >= 1000      { i8_count += 1; }
        else if *freq >= 100  { i16_count += 1; }
        else if *freq >= 10   { i32_count += 1; }
        else if *freq >= 2    { i64_count += 1; }
        else                  { i128_count += 1; } // FIX #3: freq==1 only (< 2 same but explicit)
    }

    if total_len == 0 {
        warn!("No patterns in DB. Nothing to compile.");
        return 0;
    }
    info!("Collected {} total patterns (sorted by freq desc). Building binary...", total_len);

    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&total_len.to_le_bytes());
    body.extend_from_slice(&i8_count.to_le_bytes());
    body.extend_from_slice(&i16_count.to_le_bytes());
    body.extend_from_slice(&i32_count.to_le_bytes());
    body.extend_from_slice(&i64_count.to_le_bytes());
    body.extend_from_slice(&i128_count.to_le_bytes());

    let mut pid8: u8 = 0;
    for (k, freq) in &entries {
        if *freq >= 1000 {
            body.push(pid8);
            pid8 = pid8.wrapping_add(1);
            let klen = k.len().min(255) as u8;
            body.push(klen);
            body.extend_from_slice(&k[..klen as usize]);
            body.extend_from_slice(&freq.to_le_bytes());
        }
    }

    let mut pid16: u16 = 0;
    for (k, freq) in &entries {
        if *freq >= 100 && *freq < 1000 {
            body.extend_from_slice(&pid16.to_le_bytes());
            pid16 = pid16.wrapping_add(1);
            let klen = k.len().min(255) as u8;
            body.push(klen);
            body.extend_from_slice(&k[..klen as usize]);
            body.extend_from_slice(&freq.to_le_bytes());
        }
    }

    let mut pid32: u32 = 0;
    for (k, freq) in &entries {
        if *freq >= 10 && *freq < 100 {
            body.extend_from_slice(&pid32.to_le_bytes());
            pid32 = pid32.wrapping_add(1);
            let klen = k.len().min(255) as u8;
            body.push(klen);
            body.extend_from_slice(&k[..klen as usize]);
            body.extend_from_slice(&freq.to_le_bytes());
        }
    }

    let mut pid64: u64 = 0;
    for (k, freq) in &entries {
        if *freq >= 2 && *freq < 10 {
            body.extend_from_slice(&pid64.to_le_bytes());
            pid64 = pid64.wrapping_add(1);
            let klen = k.len().min(255) as u8;
            body.push(klen);
            body.extend_from_slice(&k[..klen as usize]);
            body.extend_from_slice(&freq.to_le_bytes());
        }
    }

    let mut pid128: u64 = 0;
    for (k, freq) in &entries {
        if *freq == 1 { // FIX #3: SPEC says freq==1 exactly for i128
            body.extend_from_slice(&pid128.to_le_bytes());
            body.extend_from_slice(&0u64.to_le_bytes()); 
            pid128 = pid128.wrapping_add(1);
            let klen = k.len().min(255) as u8;
            body.push(klen);
            body.extend_from_slice(&k[..klen as usize]);
            body.extend_from_slice(&freq.to_le_bytes());
        }
    }

    let original_size = entries.iter().map(|(k, _)| k.len() as u64).sum::<u64>();
    let compressed_size = body.len() as u64;
    body.extend_from_slice(&original_size.to_le_bytes());
    body.extend_from_slice(&compressed_size.to_le_bytes());

    use std::io::{BufWriter, Write as IoWrite};
    if let Ok(file) = File::create(output_file()) {
        let mut out = BufWriter::new(file);
        // FIX #3: Write body directly — NO extra 4-byte prefix (not in SPEC!)
        // SPEC format: [HEADER 24B][BODY variable][FOOTER 16B] — nothing before header
        out.write_all(&body).unwrap();
        out.flush().unwrap();
        info!("Binary compiled: {} bytes ({} patterns)", body.len(), total_len);
    }
    
    total_len as usize
}

// --------------------------------------------------------
// API Handlers
// --------------------------------------------------------
#[derive(Deserialize)]
struct GetTaskQuery {
    colab_id: Option<String>,
}

#[derive(Serialize)]
struct GetTaskResponse {
    status: String,
    dump_id: Option<String>,
    start_index: i32,
    end_index: i32,
    resume_index: i32,
    message: Option<String>,
}

async fn handle_get_task(Query(query): Query<GetTaskQuery>) -> Json<GetTaskResponse> {
    let colab_id = query.colab_id.unwrap_or_else(|| "unknown".to_string());
    
    let mut ts = TASK_STATE.write().await;
    initialize_next_dump_if_needed(&mut ts).await;
    check_task_timeouts(&mut ts).await;
    
    let mut existing_batch = None;
    for (_, info) in ts.in_progress_batches.iter_mut() {
        if info.colab_id.as_deref() == Some(&colab_id) {
            info.last_ping = current_time();
            existing_batch = Some(info.clone());
            break;
        }
    }
    
    if let Some(info) = existing_batch {
        let _ = save_tasks(&ts).await;
        info!("Colab {} reconnected. Returning existing batch {}:{}", colab_id, info.dump_id, info.start_index);
        return Json(GetTaskResponse {
            status: "success".to_string(),
            dump_id: Some(info.dump_id),
            start_index: info.start_index,
            end_index: info.end_index,
            resume_index: info.resume_path_index,
            message: None,
        });
    }
    
    let mut resumed_batch = None;
    for (_, info) in ts.in_progress_batches.iter_mut() {
        if info.colab_id.is_none() {
            info.colab_id = Some(colab_id.clone());
            info.last_ping = current_time();
            info.start_time = current_time(); // BUG FIX: Reset start time on reassignment
            resumed_batch = Some(info.clone());
            break;
        }
    }
    
    if let Some(info) = resumed_batch {
        let _ = save_tasks(&ts).await;
        info!("Re-assigned batch {}:{} to Colab {}", info.dump_id, info.start_index, colab_id);
        return Json(GetTaskResponse {
            status: "success".to_string(),
            dump_id: Some(info.dump_id),
            start_index: info.start_index,
            end_index: info.end_index,
            resume_index: info.resume_path_index,
            message: None,
        });
    }
    
    if let Some(current_dump) = ts.current_dump.clone() {
        if ts.next_path_index < ts.current_dump_total_files {
            let start = ts.next_path_index;
            let mut end = start + BATCH_SIZE;
            if end > ts.current_dump_total_files {
                end = ts.current_dump_total_files;
            }
            
            let batch_id = format!("{}:{}", current_dump, start);
            let info = BatchInfo {
                colab_id: Some(colab_id.clone()),
                dump_id: current_dump.clone(),
                start_index: start,
                end_index: end,
                start_time: current_time(),
                last_ping: current_time(),
                resume_path_index: start,
            };
            
            ts.in_progress_batches.insert(batch_id, info.clone());
            ts.next_path_index = end;
            
            let _ = save_tasks(&ts).await;
            info!("Assigned NEW batch {}:{} to Colab {}", current_dump, start, colab_id);
            
            return Json(GetTaskResponse {
                status: "success".to_string(),
                dump_id: Some(current_dump),
                start_index: start,
                end_index: end,
                resume_index: start,
                message: None,
            });
        }
    }
    
    Json(GetTaskResponse {
        status: "empty".to_string(),
        dump_id: None,
        start_index: 0,
        end_index: 0,
        resume_index: 0,
        message: Some("No pending tasks".to_string()),
    })
}

#[derive(Deserialize)]
struct PostDataReq {
    colab_id: Option<String>,
    data_type: String,
    data: Option<String>,
    dump_id: Option<String>,
    start_index: i32,
    path_index: i32,
}

#[derive(Serialize)]
struct PostDataRes {
    status: String,
    message: Option<String>,
    chunk_id: Option<usize>,
    patterns_added: Option<usize>,
}

async fn handle_data(bytes: axum::body::Bytes) -> Result<Json<PostDataRes>, axum::http::StatusCode> {
    let payload: PostDataReq = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            error!("JSON Parse Error: {}", e);
            return Err(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
        }
    };

    if payload.data_type != "frequency_map" || payload.data.is_none() {
        return Ok(Json(PostDataRes {
            status: "error".to_string(),
            message: Some("Invalid payload".to_string()),
            chunk_id: None,
            patterns_added: None,
        }));
    }
    
    let b64_data = payload.data.unwrap();
    let payload_len = b64_data.len();
    let colab_id = payload.colab_id.unwrap_or_else(|| "unknown".to_string());
    let dump_id = payload.dump_id.unwrap_or_else(|| "unknown".to_string());
    
    let patterns_added = tokio::task::spawn_blocking(move || {
        let compressed = BASE64_STANDARD.decode(&b64_data).unwrap_or_default();
        let mut json_str = String::new();
        if let Ok(mut decoder) = zstd::stream::read::Decoder::new(&compressed[..]) {
            let _ = decoder.read_to_string(&mut json_str);
        }
        
        let local_map: HashMap<String, u32> = serde_json::from_str(&json_str).unwrap_or_default();
        local_map
    }).await.unwrap_or_default();
    
    let added_count = patterns_added.len();
    
    // FAST In-Memory Accumulator Update (No SLED I/O here!)
    let needs_force_flush = {
        let mut acc = ACCUMULATOR.write().await;
        for (k, v) in patterns_added {
            let key_bytes = k.as_bytes();
            let key_vec = if key_bytes.len() > 255 { key_bytes[..255].to_vec() } else { key_bytes.to_vec() };
            *acc.entry(key_vec).or_insert(0) += v;
        }
        acc.len() >= ACCUMULATOR_FLUSH_LIMIT // FIX #4: Check if we hit the RAM cap
    };

    // FIX #4: Force immediate flush if accumulator is too large
    if needs_force_flush {
        warn!("Accumulator hit {}M limit — forcing immediate Sled flush!", ACCUMULATOR_FLUSH_LIMIT / 1_000_000);
        let map_to_flush = {
            let mut acc = ACCUMULATOR.write().await;
            std::mem::replace(&mut *acc, HashMap::new())
        };
        tokio::spawn(flush_accumulator_to_sled(map_to_flush));
    }
    
    let mut ts = TASK_STATE.write().await;
    let batch_id = format!("{}:{}", dump_id, payload.start_index);
    if let Some(info) = ts.in_progress_batches.get_mut(&batch_id) {
        info.last_ping = current_time();
        if payload.path_index != -1 {
            info.resume_path_index = payload.path_index + 1;
        }
        let _ = save_tasks(&ts).await;
    }
    
    let mut s = STATS.write().await;
    s.total_bytes_received += payload_len;
    s.chunks_processed += 1;
    
    if payload.path_index != -1 {
        s.total_wet_files_processed += 1;
    }
    
    s.active_colabs.insert(colab_id.clone());
    *s.chunks_per_colab.entry(colab_id).or_insert(0) += 1;
    let chunk_id = s.chunks_processed;
    
    Ok(Json(PostDataRes {
        status: "success".to_string(),
        message: None,
        chunk_id: Some(chunk_id),
        patterns_added: Some(added_count),
    }))
}

#[derive(Deserialize)]
struct CompleteReq {
    dump_id: String,
    start_index: i32,
}

async fn handle_complete(Json(payload): Json<CompleteReq>) -> Json<serde_json::Value> {
    let mut ts = TASK_STATE.write().await;
    let batch_id = format!("{}:{}", payload.dump_id, payload.start_index);
    
    if ts.in_progress_batches.contains_key(&batch_id) {
        ts.in_progress_batches.remove(&batch_id);
        let _ = save_tasks(&ts).await;
        info!("Batch {} completed successfully!", batch_id);
        
        if let Some(ref current) = ts.current_dump {
            if current == &payload.dump_id && ts.next_path_index >= ts.current_dump_total_files && ts.in_progress_batches.is_empty() {
                info!("Dump {} is FULLY COMPLETE. Advancing to next dump.", payload.dump_id);
                ts.completed_dumps.push(payload.dump_id.clone());
                ts.current_dump = None;
                ts.current_dump_total_files = 0;
                let _ = save_tasks(&ts).await;
            }
        }
    }
    
    Json(serde_json::json!({"status": "success"}))
}

#[derive(Deserialize)]
struct PingReq {
    worker_id: String,
    current_dump: Option<String>,
    current_file_index: Option<i32>,
    ram_usage_mb: Option<f64>,
}

async fn handle_ping(Json(payload): Json<PingReq>) -> Json<serde_json::Value> {
    let mut workers = LIVE_WORKERS.write().await;
    workers.insert(payload.worker_id.clone(), WorkerStatus {
        last_ping_time: current_time(),
        current_dump: payload.current_dump,
        current_file_index: payload.current_file_index,
        ram_usage_mb: payload.ram_usage_mb,
    });
    
    let now = current_time();
    workers.retain(|_, status| now - status.last_ping_time < 900);
    
    Json(serde_json::json!({"status": "success"}))
}

async fn handle_status_req() -> Json<serde_json::Value> {
    let s = STATS.read().await;
    let ts = TASK_STATE.read().await;
    let workers = LIVE_WORKERS.read().await;
    let uptime = current_time() - s.start_time;
    
    let written_bytes = std::fs::metadata(output_file()).map(|m| m.len()).unwrap_or(0);
    
    let progress_pct = if ts.current_dump_total_files > 0 {
        (ts.next_path_index as f64 / ts.current_dump_total_files as f64) * 100.0
    } else {
        0.0
    };
    
    let global_total_files = 8_216_667_f64;
    let processed_files = s.total_wet_files_processed as f64;
    let mut eta_str = "Calculating...".to_string();
    let mut global_progress_pct = 0.0;
    
    if uptime > 10 && processed_files > 0.0 {
        global_progress_pct = (processed_files / global_total_files) * 100.0;
        let files_per_sec = processed_files / uptime as f64;
        let remaining_files = global_total_files - processed_files;
        let remaining_secs = remaining_files / files_per_sec;
        
        let days = (remaining_secs / 86400.0).floor();
        let hours = ((remaining_secs % 86400.0) / 3600.0).floor();
        let mins = ((remaining_secs % 3600.0) / 60.0).floor();
        
        eta_str = format!("{} Days, {} Hours, {} Mins", days, hours, mins);
    }
    
    let mut live_workers_report = HashMap::new();
    let now = current_time();
    for (worker_id, status) in workers.iter() {
        let mins_ago = (now - status.last_ping_time) as f64 / 60.0;
        live_workers_report.insert(worker_id.clone(), serde_json::json!({
            "mins_ago": format!("{:.1} mins", mins_ago),
            "current_dump": status.current_dump,
            "current_file_index": status.current_file_index,
            "ram_usage_mb": status.ram_usage_mb
        }));
    }
    
    Json(serde_json::json!({
        "status": "running",
        "uptime_seconds": uptime,
        "total_received_gb": (s.total_bytes_received as f64) / (1024.0 * 1024.0 * 1024.0),
        "total_written_gb": (written_bytes as f64) / (1024.0 * 1024.0 * 1024.0),
        "chunks_processed": s.chunks_processed,
        "unique_patterns": s.cached_unique_patterns, // FAST O(1) LOOKUP
        "live_workers": live_workers_report,
        "active_colabs": s.active_colabs,
        "chunks_per_colab": s.chunks_per_colab,
        "global_progress": {
            "total_wet_files_overall": 8216667,
            "wet_files_processed": s.total_wet_files_processed,
            "progress_pct": format!("{:.4}%", global_progress_pct),
            "estimated_time_remaining": eta_str
        },
        "current_dump": ts.current_dump,
        "dump_progress_pct": format!("{:.2}%", progress_pct),
        "total_files": ts.current_dump_total_files,
        "assigned_files": ts.next_path_index,
        "tasks": {
            "pending_dumps": ts.pending_dumps.len(),
            "in_progress_batches": ts.in_progress_batches.len(),
            "completed_dumps": ts.completed_dumps.len()
        },
        "encoding": "MapReduce Rust Axum Engine Phase 23 (In-Memory Accumulator)"
    }))
}

async fn handle_compile() -> Json<serde_json::Value> {
    info!("Manual binary compilation triggered!");
    tokio::task::spawn_blocking(move || {
        info!("Starting binary compilation to HDD...");
        let _ = SLED_DB.flush(); 
        save_binary_data();
        info!("Binary compilation complete.");
    });
    
    Json(serde_json::json!({
        "status": "success",
        "message": "Binary compilation started in background"
    }))
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting Sovereign AI Master VM in RUST! Phase 23 Batch Tasking");
    
    load_state().await;
    
    // START BACKGROUND FLUSHER
    tokio::spawn(background_flusher());
    
    let app = Router::new()
        .route("/get_task", get(handle_get_task))
        .route("/data", post(handle_data))
        .route("/complete_task", post(handle_complete))
        .route("/compile", post(handle_compile))
        .route("/status", get(handle_status_req))
        .route("/ping", post(handle_ping))
        .layer(DefaultBodyLimit::disable());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8085").await.unwrap();
    info!("Listening on 0.0.0.0:8085");
    axum::serve(listener, app).await.unwrap();
}
