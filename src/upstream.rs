//! upstream 运行时：平滑加权轮询、节点健康状态和 keepalive 连接池。
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

use crate::config::UpstreamConfig;

/// 节点的运行时状态：权重、失败次数和冷却时间。
#[derive(Debug)]
struct PeerState {
    current_weight: i64,
    effective_weight: i64,
    fails: u32,
    failed_at: Option<Instant>,
}

/// 一个 upstream 组：静态配置加可变节点状态。
pub struct UpstreamGroup {
    pub cfg: UpstreamConfig,
    state: Mutex<Vec<PeerState>>,
}

/// upstream 组的选择与健康状态维护。
impl UpstreamGroup {
    /// 根据配置创建节点运行时状态。
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

    /// 获取节点状态的互斥锁。
    fn lock_state(&self) -> std::sync::MutexGuard<'_, Vec<PeerState>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 平滑加权轮询选择一个节点，支持排除已重试节点。
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

        let (idx, _) = backup_best?;
        let eff = states[idx].effective_weight;
        states[idx].current_weight += eff;
        if eff > 0 {
            states[idx].current_weight -= eff;
        }
        Some(idx)
    }

    /// 记录节点失败，增加失败计数并降低有效权重。
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

    /// 记录节点成功，清零失败状态。
    pub fn mark_success(&self, idx: usize) {
        let mut states = self.lock_state();
        if let Some(st) = states.get_mut(idx) {
            st.fails = 0;
            st.failed_at = None;
            st.effective_weight = self.cfg.peers[idx].weight;
        }
    }

    /// 返回节点主机和端口。
    pub fn address(&self, idx: usize) -> (&str, u16) {
        let p = &self.cfg.peers[idx];
        (&p.host, p.port)
    }
}

/// 池中的空闲连接及其使用计数。
struct PooledConn {
    stream: TcpStream,

    uses: usize,
    idle_since: Instant,
}

/// 连接池键：upstream 组、主机和端口。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PoolKey {
    group: String,
    host: String,
    port: u16,
}

/// 全局 keepalive 连接池。
#[derive(Default)]
pub struct UpstreamPool {
    inner: Mutex<HashMap<PoolKey, Vec<PooledConn>>>,
}

/// 连接池的存取与清理。
impl UpstreamPool {
    /// 创建空连接池。
    pub fn new() -> Self {
        Self::default()
    }

    /// 获取连接池互斥锁。
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PoolKey, Vec<PooledConn>>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// LIFO 取出一个未过期的空闲连接。
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

    /// 归还连接；超出容量或过期则丢弃。
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

    /// 清理超过各 upstream 组空闲超时的连接。
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

    /// 返回当前空闲连接总数。
    pub fn idle_count(&self) -> usize {
        self.lock().values().map(|v| v.len()).sum()
    }
}

// upstream 调度与连接池单元测试。
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerConfig;

    /// 测试辅助：构造节点配置。
    fn peer(weight: i64, backup: bool) -> PeerConfig {
        PeerConfig {
            host: "127.0.0.1".to_string(),
            port: 8080,
            weight,
            backup,
            ..PeerConfig::default()
        }
    }

    /// 验证 3:1 平滑加权轮询的比例。
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

    /// 验证 backup 只在主节点不可用时生效。
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

        group.cfg.peers[0].down = true;
        for _ in 0..10 {
            assert_eq!(group.select(&[]), Some(1));
        }
    }

    /// 验证失败主节点在冷却期内被跳过。
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
