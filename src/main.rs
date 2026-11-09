//! RPS — a lightweight nginx-style HTTP reverse proxy, rewritten in Rust.
//!
//! The command line interface and configuration language are compatible with
//! the original C implementation:
//!
//!   rps -c <config> [-l 0..8] [-p <prefix>] [-t] [-s stop]

mod config;
mod http;
mod log;
mod proxy;
mod server;
mod upstream;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs2::FileExt;
use tokio::sync::watch;

struct Cli {
    conf: Option<String>,
    prefix: Option<String>,
    log_level: Option<u8>,
    test: bool,
    stop: bool,
}

fn main() -> ExitCode {
    match real_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log::emerg(e);
            ExitCode::FAILURE
        }
    }
}

fn real_main() -> Result<(), String> {
    let cli = parse_args()?;

    let config_path = match &cli.conf {
        Some(path) => resolve_path(Path::new(path))?,
        None => return Err("RPS requires a configuration file (use \"-c <file>\")".to_string()),
    };

    apply_log_level(cli.log_level);
    log::alert("RPS start");

    let prefix = match &cli.prefix {
        Some(p) => resolve_path(Path::new(p))?,
        None => config_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from(".")),
    };

    let cfg = config::parse_config(&config_path).map_err(|e| {
        format!(
            "failed to parse configuration \"{}\": {}",
            config_path.display(),
            e
        )
    })?;

    let pid_path = {
        let pid = PathBuf::from(&cfg.pid);
        if pid.is_absolute() {
            pid
        } else {
            prefix.join(pid)
        }
    };

    if cli.stop {
        stop_daemon(&pid_path)?;
        return Ok(());
    }

    if cli.test {
        log::alert(format!(
            "configuration file {} test is successful",
            config_path.display()
        ));
        return Ok(());
    }

    if cfg.daemon && std::env::var("RPS_DAEMONIZED").ok().as_deref() != Some("1") {
        daemonize()?;
        return Ok(());
    }

    let _pid_guard = PidGuard::acquire(&pid_path)?;

    let cfg = Arc::new(cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mode = cfg.use_mode;
    let workers = cfg.worker_processes.max(1);

    let runtime = match mode {
        config::UseMode::Reactor => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build(),
        config::UseMode::Thread => tokio::runtime::Builder::new_multi_thread()
            // Connection handling runs on dedicated OS threads; the runtime
            // only drives the accept loops and signal handling.
            .worker_threads(1)
            .enable_all()
            .build(),
    }
    .map_err(|e| format!("failed to create the async runtime: {}", e))?;

    match mode {
        config::UseMode::Reactor => log::info(format!(
            "execution model: reactor (event driven), {} worker(s), {} connections/worker",
            workers, cfg.worker_connections
        )),
        config::UseMode::Thread => log::info(format!(
            "execution model: multithreading (one thread per connection), {} accept loop(s)",
            workers
        )),
    }

    runtime.block_on(async {
        let signal_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            log::info("shutdown signal received");
            let _ = shutdown_tx.send(true);
        });

        server::run(cfg.clone(), signal_shutdown)
            .await
            .map_err(|e| format!("server failed: {}", e))
    })?;

    log::alert("RPS stopped");
    Ok(())
}

fn parse_args() -> Result<Cli, String> {
    let mut cli = Cli {
        conf: None,
        prefix: None,
        log_level: None,
        test: false,
        stop: false,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "-c" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "\"-c\" requires a file path".to_string())?;
                cli.conf = Some(v.clone());
            }
            "-l" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "\"-l\" requires a level from 0 to 8".to_string())?;
                // Match the original atoi() behavior: a non-numeric value
                // becomes 0; out-of-range values downgrade to DEBUG.
                let n = v.parse::<i32>().unwrap_or(0);
                cli.log_level = Some(if (0..=8).contains(&n) {
                    n as u8
                } else {
                    log::DEBUG
                });
            }
            "-p" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "\"-p\" requires a prefix path".to_string())?;
                cli.prefix = Some(v.clone());
            }
            "-t" => cli.test = true,
            "-s" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "\"-s\" requires an action (stop)".to_string())?;
                if v != "stop" {
                    return Err(format!("unsupported action for -s: \"{}\"", v));
                }
                cli.stop = true;
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "-v" | "--version" => {
                println!("rps {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument \"{}\"", other)),
        }
        i += 1;
    }
    Ok(cli)
}

fn print_help() {
    println!(
        "RPS - lightweight HTTP reverse proxy (Rust rewrite)\n\
         \n\
         Usage: rps -c <config> [-l <0-8>] [-p <prefix>] [-t] [-s stop]\n\
         \n\
           -c <file>    configuration file (required)\n\
           -l <0-8>     log level (0=quiet, 8=debug; default 8)\n\
           -p <path>    prefix used to resolve relative paths\n\
           -t           test the configuration and exit\n\
           -s stop      stop the daemon recorded in the pid file\n"
    );
}

fn apply_log_level(level: Option<u8>) {
    if let Some(level) = level {
        log::set_level(level);
    }
}

fn resolve_path(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| format!("failed to read the current directory: {}", e))?
            .join(path)
    };
    Ok(absolute)
}

/* ------------------------------------------------------------------ */
/* PID file                                                           */
/* ------------------------------------------------------------------ */

struct PidGuard {
    path: PathBuf,
    lock_path: PathBuf,
    lock_file: File,
    pid: u32,
}

impl PidGuard {
    fn acquire(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "failed to create pid directory \"{}\": {}",
                        parent.display(),
                        e
                    )
                })?;
            }
        }

        // Use a separate lock file. An exclusive Windows byte-range lock on the
        // pid file itself would also block `-s stop` from reading the pid.
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock_path = PathBuf::from(lock_name);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                format!(
                    "failed to open lock file \"{}\": {}",
                    lock_path.display(),
                    e
                )
            })?;
        lock_file.try_lock_exclusive().map_err(|e| {
            format!(
                "failed to lock \"{}\" (another instance running?): {}",
                lock_path.display(),
                e
            )
        })?;

        let pid = std::process::id();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| format!("failed to open pid file \"{}\": {}", path.display(), e))?;
        file.write_all(format!("{}\n", pid).as_bytes())
            .and_then(|_| file.sync_all())
            .map_err(|e| format!("failed to write pid file \"{}\": {}", path.display(), e))?;
        drop(file);

        log::info(format!("pid {} written to {}", pid, path.display()));

        Ok(Self {
            path: path.to_path_buf(),
            lock_path,
            lock_file,
            pid,
        })
    }
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        if current_pid_in_file(&self.path) == Some(self.pid) {
            let _ = fs::remove_file(&self.path);
        }
        let _ = FileExt::unlock(&self.lock_file);
        let _ = fs::remove_file(&self.lock_path);
    }
}

fn current_pid_in_file(path: &Path) -> Option<u32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
}

/* ------------------------------------------------------------------ */
/* Daemon / stop                                                      */
/* ------------------------------------------------------------------ */

fn daemonize() -> Result<(), String> {
    let exe = std::env::current_exe()
        .map_err(|e| format!("failed to locate the rps executable: {}", e))?;

    let mut cmd = Command::new(exe);
    cmd.args(std::env::args_os().skip(1));
    cmd.env("RPS_DAEMONIZED", "1");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to start the daemon process: {}", e))?;
    log::alert(format!("daemon started (pid {})", child.id()));
    Ok(())
}

fn stop_daemon(pid_path: &Path) -> Result<(), String> {
    let mut content = String::new();
    File::open(pid_path)
        .and_then(|mut f| f.read_to_string(&mut content))
        .map_err(|e| format!("failed to read pid file \"{}\": {}", pid_path.display(), e))?;
    let pid: u32 = content
        .trim()
        .parse()
        .map_err(|_| format!("invalid pid in \"{}\"", pid_path.display()))?;
    if pid == 0 {
        return Err(format!("invalid pid in \"{}\"", pid_path.display()));
    }

    log::info(format!("sending termination request to process {}", pid));

    #[cfg(unix)]
    {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    if wait_until_dead(pid, Duration::from_secs(3)) {
        log::info(format!("process {} exited", pid));
    } else {
        log::warn(format!(
            "process {} did not exit in time, forcing termination",
            pid
        ));
        #[cfg(unix)]
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
        #[cfg(windows)]
        {
            let _ = Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/F"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = wait_until_dead(pid, Duration::from_secs(2));
    }

    let _ = fs::remove_file(pid_path);
    Ok(())
}

fn wait_until_dead(pid: u32, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if !process_alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !process_alive(pid)
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    let filter = format!("PID eq {}", pid);
    match Command::new("tasklist")
        .args(["/FI", &filter, "/NH"])
        .output()
    {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            text.contains(&pid.to_string())
        }
        Err(_) => false,
    }
}

/* ------------------------------------------------------------------ */
/* Signals                                                            */
/* ------------------------------------------------------------------ */

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            log::error(format!("failed to install SIGTERM handler: {}", e));
            return;
        }
    };
    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            log::error(format!("failed to install SIGINT handler: {}", e));
            return;
        }
    };
    let mut sigquit = match signal(SignalKind::quit()) {
        Ok(s) => s,
        Err(e) => {
            log::error(format!("failed to install SIGQUIT handler: {}", e));
            return;
        }
    };

    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
        _ = sigquit.recv() => {}
    }
}

#[cfg(windows)]
async fn wait_for_signal() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        log::error(format!("failed to wait for Ctrl-C: {}", e));
    }
}
