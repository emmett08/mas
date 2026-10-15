//! Lifetime supervision of a trusted, explicitly registered worker executable.
//! No network or request-selected code is accepted here. Linux process groups
//! manage cooperative trusted workers; this module is not a security sandbox.

#[cfg(target_os = "linux")]
pub fn supervise(args: &[String]) -> Result<(), String> {
    use std::{io, os::unix::process::CommandExt, process::{Command, Stdio},
              sync::atomic::{AtomicBool, Ordering},
              time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

    static STOP: AtomicBool = AtomicBool::new(false);
    extern "C" fn stop(_signal: i32) { STOP.store(true, Ordering::SeqCst); }

    fn integer(value: &str) -> Result<u64, String> { value.parse().map_err(|_| "invalid supervisor limit".into()) }
    fn descriptor(value: &str) -> Result<i32, String> {
        let fd: i32 = value.parse().map_err(|_| "invalid supervisor descriptor")?;
        if fd <= 2 || unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
            return Err("invalid supervisor descriptor".into());
        }
        Ok(fd)
    }
    fn milliseconds() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }
    if args.len() < 9 || args[7] != "--" { return Err("invalid supervisor arguments".into()); }
    let lifetime = descriptor(&args[0])?;
    let channel = descriptor(&args[1])?;
    let deadline = integer(&args[2])?;
    let memory = integer(&args[3])?;
    let cpu = integer(&args[4])?;
    let file_size = integer(&args[5])?;
    if lifetime == channel || deadline <= milliseconds() || deadline > milliseconds().saturating_add(86_400_000)
        || !(64*1024*1024..=1024_u64*1024*1024*1024).contains(&memory)
        || !(1..=86_400).contains(&cpu) || !(1024..=1024_u64*1024*1024*1024).contains(&file_size) {
        return Err("invalid supervisor bounds".into());
    }
    let end = Instant::now() + Duration::from_millis(deadline.saturating_sub(milliseconds()));
    let mut ownership = Vec::new();
    if args[6] != "-" {
        for value in args[6].split(',') {
            let fd = descriptor(value)?;
            if fd == lifetime || fd == channel || ownership.contains(&fd) { return Err("invalid ownership descriptor".into()); }
            ownership.push(fd);
        }
    }
    if ownership.len() > 32 || !std::path::Path::new(&args[8]).is_absolute() {
        return Err("invalid worker registration".into());
    }
    // The guardian retains ownership locks until every owned descendant is
    // reaped. They and the lifetime pipe are never inherited by the worker.
    for fd in ownership.iter().chain(std::iter::once(&lifetime)) {
        if unsafe { libc::fcntl(*fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err("cannot restrict worker descriptors".into());
        }
    }
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        return Err("subreaper unavailable".into());
    }
    STOP.store(false, Ordering::SeqCst);
    for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        unsafe { libc::signal(signal, stop as *const () as libc::sighandler_t); }
    }
    let supervisor = unsafe { libc::getpid() };
    let mut command = Command::new(&args[8]);
    command.args(&args[9..]).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .env("MAS_WORKER_CHANNEL_FD", channel.to_string()).process_group(0);
    unsafe {
        command.pre_exec(move || {
            // pre_exec performs only async-signal-safe system calls.
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0
                || libc::getppid() != supervisor {
                return Err(io::Error::from_raw_os_error(libc::ECHILD));
            }
            for (resource, limit) in [(libc::RLIMIT_AS, memory), (libc::RLIMIT_CPU, cpu),
                                      (libc::RLIMIT_FSIZE, file_size)] {
                let value = libc::rlimit { rlim_cur:limit as libc::rlim_t, rlim_max:limit as libc::rlim_t };
                if libc::setrlimit(resource, &value) != 0 { return Err(io::Error::last_os_error()); }
            }
            let zero = libc::rlimit { rlim_cur:0, rlim_max:0 };
            if libc::setrlimit(libc::RLIMIT_CORE, &zero) != 0 { return Err(io::Error::last_os_error()); }
            Ok(())
        });
    }
    let child = command.spawn().map_err(|_| "worker executable unavailable")?;
    let worker = child.id() as i32;
    unsafe { libc::close(channel); }
    loop {
        if STOP.load(Ordering::SeqCst) { break; }
        // Observe exit without reaping the leader: its PID cannot be recycled
        // between detecting exit and terminating the remaining process group.
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let observed = unsafe { libc::waitid(libc::P_PID, worker as libc::id_t, &mut status,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT) };
        if observed < 0 || unsafe { status.si_pid() } != 0 { break; }
        let remaining = end.saturating_duration_since(Instant::now()).as_millis() as u64;
        if remaining == 0 { break; }
        let mut event = libc::pollfd { fd:lifetime, events:libc::POLLIN, revents:0 };
        let result = unsafe { libc::poll(&mut event, 1, remaining.min(50) as i32) };
        if result > 0 { break; } // Any byte, EOF or descriptor error revokes lifetime.
        if result < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted { break; }
    }
    // SIGKILL is deliberately unconditional: successful worker exit does not
    // grant its background descendants permission to outlive this invocation.
    unsafe { libc::kill(-worker, libc::SIGKILL); }
    loop {
        let mut status = 0;
        let result = unsafe { libc::waitpid(-1, &mut status, 0) };
        if result > 0 { continue; }
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) { continue; }
            if error.raw_os_error() == Some(libc::ECHILD) { break; }
            return Err("worker cleanup failed".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn supervise(_args: &[String]) -> Result<(), String> {
    Err("executable supervision requires Linux".into())
}
