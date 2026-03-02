use std::time::Instant;

use super::aes_gcm::Aes256GcmEngine;
use super::chacha20_poly1305::ChaCha20Poly1305Engine;
use super::CryptoEngine;

const SMALL_SIZE: usize = 4 * 1024;
const LARGE_SIZE: usize = 1024 * 1024;
const SMALL_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const LARGE_TOTAL_BYTES: usize = 32 * 1024 * 1024;
const SMALL_WEIGHT: f64 = 0.7;
const LARGE_WEIGHT: f64 = 0.3;
const TIE_BIAS_THRESHOLD: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoAeadMode {
    Aes256Gcm,
    Chacha20Poly1305,
}

struct CandidateScore {
    small_mibps: f64,
    large_mibps: f64,
    weighted: f64,
}

pub fn select_best_aead() -> AutoAeadMode {
    let gcm = benchmark_candidate(AutoAeadMode::Aes256Gcm);
    let chacha = benchmark_candidate(AutoAeadMode::Chacha20Poly1305);

    tracing::debug!(
        "AES-256-GCM:        small {:.0} MiB/s, large {:.0} MiB/s, weighted {:.0}",
        gcm.small_mibps,
        gcm.large_mibps,
        gcm.weighted
    );
    tracing::debug!(
        "ChaCha20-Poly1305:  small {:.0} MiB/s, large {:.0} MiB/s, weighted {:.0}",
        chacha.small_mibps,
        chacha.large_mibps,
        chacha.weighted
    );

    let chosen = choose_from_scores(gcm.weighted, chacha.weighted);
    tracing::info!("auto-selected cipher: {:?}", chosen);
    chosen
}

fn benchmark_candidate(candidate: AutoAeadMode) -> CandidateScore {
    let encryption_key = [0x3Au8; 32];
    let chunk_id_key = [0xC5u8; 32];

    let engine: Box<dyn CryptoEngine> = match candidate {
        AutoAeadMode::Aes256Gcm => Box::new(Aes256GcmEngine::new(&encryption_key, &chunk_id_key)),
        AutoAeadMode::Chacha20Poly1305 => {
            Box::new(ChaCha20Poly1305Engine::new(&encryption_key, &chunk_id_key))
        }
    };

    let aad = b"\x01";
    let small = benchmark_input(SMALL_SIZE);
    let large = benchmark_input(LARGE_SIZE);

    // Warm up both message sizes before timing.
    let _ = run_once(engine.as_ref(), &small, aad);
    let _ = run_once(engine.as_ref(), &large, aad);

    let small_iterations = SMALL_TOTAL_BYTES / SMALL_SIZE;
    let large_iterations = LARGE_TOTAL_BYTES / LARGE_SIZE;

    let small_mibps = measure_mib_per_sec(engine.as_ref(), &small, aad, small_iterations);
    let large_mibps = measure_mib_per_sec(engine.as_ref(), &large, aad, large_iterations);
    let weighted = weighted_score(small_mibps, large_mibps);

    CandidateScore {
        small_mibps,
        large_mibps,
        weighted,
    }
}

fn measure_mib_per_sec(
    engine: &dyn CryptoEngine,
    payload: &[u8],
    aad: &[u8],
    iterations: usize,
) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        let Ok(ciphertext) = engine.encrypt(payload, aad) else {
            return 0.0;
        };
        let Ok(plaintext) = engine.decrypt(&ciphertext, aad) else {
            return 0.0;
        };
        if plaintext != payload {
            return 0.0;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        return 0.0;
    }

    let total_mib = (payload.len() * iterations) as f64 / (1024.0 * 1024.0);
    total_mib / elapsed
}

fn run_once(engine: &dyn CryptoEngine, payload: &[u8], aad: &[u8]) -> bool {
    let Ok(ciphertext) = engine.encrypt(payload, aad) else {
        return false;
    };
    let Ok(plaintext) = engine.decrypt(&ciphertext, aad) else {
        return false;
    };
    plaintext == payload
}

fn benchmark_input(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn weighted_score(small_mibps: f64, large_mibps: f64) -> f64 {
    (small_mibps * SMALL_WEIGHT) + (large_mibps * LARGE_WEIGHT)
}

fn choose_from_scores(gcm: f64, chacha: f64) -> AutoAeadMode {
    if chacha > gcm * (1.0 + TIE_BIAS_THRESHOLD) {
        AutoAeadMode::Chacha20Poly1305
    } else {
        AutoAeadMode::Aes256Gcm
    }
}

#[cfg(test)]
mod tests {
    use super::{choose_from_scores, weighted_score, AutoAeadMode};

    #[test]
    fn chooser_prefers_chacha_on_clear_win() {
        let picked = choose_from_scores(1000.0, 1200.0);
        assert_eq!(picked, AutoAeadMode::Chacha20Poly1305);
    }

    #[test]
    fn chooser_prefers_gcm_on_near_tie() {
        let picked = choose_from_scores(1000.0, 1030.0);
        assert_eq!(picked, AutoAeadMode::Aes256Gcm);
    }

    #[test]
    fn weighted_score_favors_small_payloads() {
        let score = weighted_score(500.0, 100.0);
        assert_eq!(score, 380.0);
    }
}
