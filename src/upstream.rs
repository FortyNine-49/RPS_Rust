//! Upstream runtime state: smooth weighted round-robin and the keepalive
//! connection cache.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

use crate::config::UpstreamConfig;

#[derive(Debug)]
struct PeerState {
    current_weight: i64,
    effective_weight: i64,
    fails: u32,
    failed_at: Option<Instant>,
}

pub struct UpstreamGroup {
    pub cfg: UpstreamConfig,
    state: Mutex<Vec<PeerState>>,
}

impl UpstreamGroup {
    pub fn new(cfg: UpstreamConfig) -> Self {
        let state = cfg
            .peers
            .iter()
            .map(|p| PeerState {
                current_weight: 0,
                effective_weight: p.weight,
                fails: 0,
                failed_at: None,
            })
            .collect();
        Self {
            cfg,
            state: Mutex::new(state),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, Vec<PeerState>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Smooth weighted round-robin, mirroring the original RPS algorithm.
    /// `exclude` is used to retry another peer after a connect failure.
    pub fn select(&self, exclude: &[usize]) -> Option<usize> {
        let mut states = self.lock_state();
        let now = Instant::now();
        let n = self.cfg.peers.len();
        let mut best: Option<(usize, i64)> = None;
        let mut backup_best: Option<(usize, i64)> = None;
        let mut total_weight: i64 = 0;

        for i in 0..n {
            if exclude.contains(&i) {
                continue;
            }
            let cfg = &self.cfg.peers[i];
            let st = &mut states[i];
            if cfg.down {
                continue;
            }

            if cfg.max_fails > 0 && st.fails >= cfg.max_fails {
                if let Some(at) = st.failed_at {
                    if now.duration_since(at) < cfg.fail_timeout {
                        continue;
                    }
                }
                st.fails = 0;
                st.failed_at = None;
                st.effective_weight = cfg.weight;
            }

            if cfg.backup {
                let better = match backup_best {
                    None => true,
                    Some((_, w)) => st.current_weight > w,
                };
                if better {
                    backup_best = Some((i, st.current_weight));
                }
                continue;
            }

            st.current_weight += st.effective_weight;
            total_weight += st.effective_weight;
            let better = match best {
                None => true,
                Some((_, w)) => st.current_weight > w,
            };
            if better {
                best = Some((i, st.current_weight));
            }
        }

        if let Some((idx, _)) = best {
            if total_weight > 0 {
                states[idx].current_weight -= total_weight;
            }
            return Some(idx);
        }

        // No primary available: fall back to a backup.
        let (idx, _) = backup_best?;
        let eff = states[idx].effective_weight;
        states[idx].current_weight += eff;
        if eff > 0 {
            states[idx].current_weight -= eff;
        }
        Some(idx)
    }

    pub fn mark_failure(&self, idx: usize) {
        let mut states = self.lock_state();
        if let Some(st) = states.get_mut(idx) {
            st.fails = st.fails.saturating_add(1);
            st.failed_at = Some(Instant::now());
            if st.effective_weight > 1 {
                st.effective_weight /= 2;
            }
        }
    }

    pub fn mark_success(&self, idx: usize) {
        let mut states = self.lock_state();
        if let Some(st) = states.get_mut(idx) {
            st.fails = 0;
            st.failed_at = None;
            st.effective_weight = self.cfg.peers[idx].weight;
        }
    }

    pub fn address(&self, idx: usize) -> (&str, u16) {
        let p = &self.cfg.peers[idx];
        (&p.host, p.port)
    }
}

/* ------------------------------------------------------------------ */
/* Keepalive connection pool                                          */
/* ------------------------------------------------------------------ */

struct PooledConn {
    stream: TcpStream,
    /// Number of requests already sent on this connection.
    uses: usize,
    idle_since: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PoolKey {
    group: String,
    host: String,
    port: u16,
}

#[derive(Default)]
pub struct UpstreamPool {
    inner: Mutex<HashMap<PoolKey, Vec<PooledConn>>>,
}

impl UpstreamPool {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PoolKey, Vec<PooledConn>>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take a cached connection for this upstream address (LIFO), pruning
    /// entries that exceeded the idle timeout.
    pub fn take(
        &self,
        group: &str,
        host: &str,
        port: u16,
        idle_timeout: Duration,
    ) -> Option<(TcpStream, usize)> {
        let key = PoolKey {
            group: group.to_string(),
            host: host.to_string(),
            port,
        };
        let mut map = self.lock();
        let list = map.get_mut(&key)?;
        list.retain(|c| c.idle_since.elapsed() < idle_timeout);
        let entry = list.pop()?;
        if list.is_empty() {
            map.remove(&key);
        }
        Some((entry.stream, entry.uses))
    }

    /// Return a connection to the cache if the group still has capacity.
    /// A stream passed to this function is closed when it is not cached.
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &self,
        group: &str,
        host: &str,
        port: u16,
        stream: TcpStream,
        uses: usize,
        max_per_group: usize,
        idle_timeout: Duration,
    ) {
        if max_per_group == 0 {
            return;
        }
        let key = PoolKey {
            group: group.to_string(),
            host: host.to_string(),
            port,
        };
        let mut map = self.lock();
        if let Some(list) = map.get_mut(&key) {
            list.retain(|c| c.idle_since.elapsed() < idle_timeout);
        }
        map.retain(|_, list| !list.is_empty());
        let group_count: usize = map
            .iter()
            .filter(|(k, _)| k.group == group)
            .map(|(_, v)| v.len())
            .sum();
        if group_count >= max_per_group {
            return;
        }
        map.entry(key).or_default().push(PooledConn {
            stream,
            uses,
            idle_since: Instant::now(),
        });
    }

    /// Drop idle connections whose per-group timeout has expired.
    pub fn prune(&self, timeouts: &HashMap<String, Duration>) {
        let mut map = self.lock();
        let keys: Vec<PoolKey> = map.keys().cloned().collect();
        for key in keys {
            let timeout = match timeouts.get(&key.group) {
                Some(t) => *t,
                None => Duration::from_secs(60),
            };
            if let Some(list) = map.get_mut(&key) {
                let before = list.len();
                list.retain(|c| c.idle_since.elapsed() < timeout);
                if list.is_empty() && before > 0 {
                    map.remove(&key);
                }
            }
        }
    }

    pub fn idle_count(&self) -> usize {
        self.lock().values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerConfig;

    fn peer(weight: i64, backup: bool) -> PeerConfig {
        PeerConfig {
            host: "127.0.0.1".to_string(),
            port: 8080,
            weight,
            backup,
            ..PeerConfig::default()
        }
    }

    #[test]
    fn smooth_weighted_round_robin() {
        let group = UpstreamGroup::new(UpstreamConfig {
            name: "test".to_string(),
            peers: vec![peer(3, false), peer(1, false)],
            keepalive: 0,
            keepalive_timeout: Duration::from_secs(60),
            keepalive_requests: 100,
        });
        let mut counts = [0usize; 2];
        for _ in 0..400 {
            let idx = group.select(&[]).unwrap();
            counts[idx] += 1;
        }
        assert_eq!(counts[0], 300);
        assert_eq!(counts[1], 100);
    }

    #[test]
    fn backup_is_used_only_when_primary_is_unavailable() {
        let mut group = UpstreamGroup::new(UpstreamConfig {
            name: "test".to_string(),
            peers: vec![peer(1, false), peer(5, true)],
            keepalive: 0,
            keepalive_timeout: Duration::from_secs(60),
            keepalive_requests: 100,
        });
        for _ in 0..10 {
            assert_eq!(group.select(&[]), Some(0));
        }

        // Mark the primary down permanently.
        group.cfg.peers[0].down = true;
        for _ in 0..10 {
            assert_eq!(group.select(&[]), Some(1));
        }
    }

    #[test]
    fn failed_primary_is_skipped_during_cooldown() {
        let group = UpstreamGroup::new(UpstreamConfig {
            name: "test".to_string(),
            peers: vec![
                PeerConfig {
                    max_fails: 1,
                    fail_timeout: Duration::from_secs(60),
                    ..peer(1, false)
                },
                peer(1, false),
            ],
            keepalive: 0,
            keepalive_timeout: Duration::from_secs(60),
            keepalive_requests: 100,
        });
        group.mark_failure(0);
        for _ in 0..10 {
            assert_eq!(group.select(&[]), Some(1));
        }
    }
}
