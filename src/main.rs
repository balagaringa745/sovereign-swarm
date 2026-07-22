use std::collections::HashMap;
use std::io::Read;
use std::time::Instant;
use flate2::read::MultiGzDecoder;
use reqwest::Client;
use base64::{Engine as _, engine::general_purpose};
use tokio::sync::Semaphore;
use std::sync::Arc;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct GetTaskResponse {
    status: String,
    dump_id: Option<String>,
    start_index: i32,
    end_index: i32,
    resume_index: i32,
    message: Option<String>,
}

async fn flush_map(
    map: HashMap<String, u32>,
    client: &Client,
    worker_id: &str,
    dump_id: &str,
    start_index: i32,
    path_index: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json_bytes = serde_json::to_vec(&map)?;
    let mut compressed_payload = Vec::new();
    zstd::stream::copy_encode(&json_bytes[..], &mut compressed_payload, 3)?;
    let b64_payload = general_purpose::STANDARD.encode(&compressed_payload);

    let payload = serde_json::json!({
        "colab_id": worker_id,
        "data_type": "frequency_map",
        "dump_id": dump_id,
        "start_index": start_index,
        "path_index": path_index,
        "data": b64_payload
    });

    client
        .post(&format!("http://{}:8085/data", std::env::var("MASTER_VM_IP").unwrap_or_else(|_| "52.201.247.138".to_string())))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

const CHUNK_LIMIT: usize = 500_000;

/// Spawn an async upload task if the map is full, or always if force=true.
async fn maybe_flush(
    freq_map: &mut HashMap<String, u32>,
    upload_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    chunks_sent: &mut usize,
    semaphore: &Arc<Semaphore>,
    client: &Client,
    worker_id: &str,
    dump_id: &str,
    start_idx: i32,
    path_idx: i32,
    force: bool,
) {
    if freq_map.len() >= CHUNK_LIMIT || (force && !freq_map.is_empty()) {
        println!(
            "[*] Chunk {} ({} items). Spawning upload...",
            *chunks_sent + 1,
            freq_map.len()
        );
        let upload_map = std::mem::replace(freq_map, HashMap::with_capacity(CHUNK_LIMIT));
        let client_c = client.clone();
        let wid = worker_id.to_string();
        let did = dump_id.to_string();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let handle = tokio::spawn(async move {
            let _p = permit; // RAII: dropped on block exit even on panic
            match flush_map(upload_map, &client_c, &wid, &did, start_idx, path_idx).await {
                Err(e) => eprintln!("[!] Upload failed: {}", e),
                Ok(_) => println!("[+] Upload chunk done."),
            }
        });
        upload_tasks.push(handle);
        *chunks_sent += 1;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start_total = Instant::now();
    println!("==================================================");
    println!(" Sovereign Rust Swarm Worker — Phase 23           ");
    println!("==================================================");

    let args: Vec<String> = std::env::args().collect();
    let worker_id = if args.len() > 1 {
        args[1].clone()
    } else {
        "unknown-worker".to_string()
    };
    println!("[*] Worker ID: {}", worker_id);

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(450))
        .build()?;

    // --- 1. Get Task ---
    let task_resp = client
        .get(format!(
            "http://{}:8085/get_task?colab_id={}", std::env::var("MASTER_VM_IP").unwrap_or_else(|_| "52.201.247.138".to_string()),
            worker_id
        ))
        .send()
        .await?
        .json::<GetTaskResponse>()
        .await?;

    if task_resp.status != "success" || task_resp.dump_id.is_none() {
        println!("[!] No task: {:?}", task_resp.message);
        return Ok(());
    }

    let dump_id = task_resp.dump_id.unwrap();
    let start_idx = task_resp.start_index;
    let end_idx = task_resp.end_index;
    let resume_idx = task_resp.resume_index;
    println!(
        "[*] Task: Dump={} Files={}-{} Resume={}",
        dump_id, start_idx, end_idx, resume_idx
    );

    // --- 2. Fetch WET paths index ---
    let paths_gz = client
        .get(format!(
            "https://data.commoncrawl.org/crawl-data/{}/wet.paths.gz",
            dump_id
        ))
        .send()
        .await?
        .bytes()
        .await?;

    let mut paths_text = String::new();
    MultiGzDecoder::new(&paths_gz[..]).read_to_string(&mut paths_text)?;
    drop(paths_gz); // free immediately
    let all_paths: Vec<&str> = paths_text.lines().collect();

    // --- 3. Process each WET file in the batch ---
    for current_path_idx in resume_idx..end_idx {
        if current_path_idx as usize >= all_paths.len() {
            println!("[!] path_index {} out of bounds!", current_path_idx);
            break;
        }
        let wet_url = format!(
            "https://data.commoncrawl.org/{}",
            all_paths[current_path_idx as usize]
        );
        println!("[*] File {}/{}: {}", current_path_idx + 1, end_idx, wet_url);

        // Download + decompress, then free compressed bytes immediately
        let buffer = {
            let dl_start = Instant::now();
            let compressed = client.get(&wet_url).send().await?.error_for_status()?.bytes().await?.to_vec();
            println!("[*] Downloaded {} MB in {:.2?}", compressed.len() / 1_048_576, dl_start.elapsed());
            let mut buf = String::with_capacity(300 * 1_048_576);
            MultiGzDecoder::new(&compressed[..]).read_to_string(&mut buf)?;
            buf // compressed dropped here → RAM freed
        };

        let parse_start = Instant::now();
        let bytes = buffer.as_bytes(); // zero-copy slice

        let mut freq_map: HashMap<String, u32> = HashMap::with_capacity(CHUNK_LIMIT);
        let mut upload_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut chunks_sent = 0usize;
        let semaphore = Arc::new(Semaphore::new(3));

        // === SPEC-COMPLIANT PATTERN EXTRACTION ===

        // Pass 1: Character Unigrams (max ~256 unique — never triggers flush)
        for &b in bytes.iter() {
            let key = String::from_utf8_lossy(&[b]).into_owned(); // FIX #1: Safe for all byte values
            *freq_map.entry(key).or_insert(0) += 1;
        }
        println!("[*] Unigrams done ({} unique)", freq_map.len());

        // Pass 2: Character Bigrams (max ~65K unique — cannot hit 500K limit)
        for window in bytes.windows(2) {
            let key = String::from_utf8_lossy(window).into_owned();
            *freq_map.entry(key).or_insert(0) += 1;
        }
        println!("[*] Bigrams done ({} unique)", freq_map.len());

        // Pass 3: Character Trigrams (dominant — may flush multiple times)
        for window in bytes.windows(3) {
            let key = String::from_utf8_lossy(window).into_owned();
            *freq_map.entry(key).or_insert(0) += 1;
            maybe_flush(
                &mut freq_map, &mut upload_tasks, &mut chunks_sent,
                &semaphore, &client, &worker_id, &dump_id,
                start_idx, current_path_idx, false,
            ).await;
        }
        println!("[*] Trigrams done ({} unique)", freq_map.len());

        // Final flush for remaining patterns
        maybe_flush(
            &mut freq_map, &mut upload_tasks, &mut chunks_sent,
            &semaphore, &client, &worker_id, &dump_id,
            start_idx, current_path_idx, true,
        ).await;

        // Wait for all uploads for this file
        for h in upload_tasks { let _ = h.await; }

        println!(
            "[*] File {} done in {:.2?} ({} chunks sent)",
            current_path_idx,
            parse_start.elapsed(),
            chunks_sent
        );
        drop(buffer); // free decompressed text before next file
    }

    // --- 4. Mark batch complete ---
    println!("[*] Marking batch complete...");
    client
        .post(&format!("http://{}:8085/complete_task", std::env::var("MASTER_VM_IP").unwrap_or_else(|_| "52.201.247.138".to_string())))
        .json(&serde_json::json!({
            "dump_id": dump_id,
            "start_index": start_idx
        }))
        .send()
        .await?;

    println!("[*] TOTAL TIME: {:.2?}", start_total.elapsed());
    Ok(())
}
