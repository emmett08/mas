//! Private host stdio broker. It deliberately has no socket listener.
use mas_protocol_codec::{gateway::{normalise_config, Gateway}, parse_json, store::Store, MAX_BYTES};
use serde_json::{json, Value};
use std::{env, fs::{self, File, OpenOptions}, io::{self, BufReader, Read, Write}, path::Path};

fn read_json(path: &Path, private: bool) -> Result<Value, String> {
    let file = File::open(path).map_err(|_| "input file unavailable")?;
    let metadata = file.metadata().map_err(|_| "input file unavailable")?;
    if !metadata.is_file() { return Err("input must be a regular file".into()); }
    if private {
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 { return Err("credential file must deny group and other permissions".into()); }
        }
        #[cfg(not(unix))] { return Err("private credential files currently require Unix".into()); }
    }
    let mut source = String::new();
    file.take((MAX_BYTES + 1) as u64).read_to_string(&mut source).map_err(|_| "input file unavailable")?;
    parse_json(&source).map_err(|_| "invalid input configuration".into())
}

fn write_private(path: &Path, value: &Value) -> Result<(), String> {
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(path).map_err(|_| "cannot create private output file (existing files are never overwritten)")?;
        let bytes = serde_json::to_vec_pretty(value).map_err(|_| "cannot encode private output")?;
        file.write_all(&bytes).and_then(|_| file.write_all(b"\n")).and_then(|_| file.sync_all()).map_err(|_| "cannot write private output")?;
        Ok(())
    }
    #[cfg(not(unix))] { let _ = (path, value); Err("private credential files currently require Unix".into()) }
}

fn random_token() -> Result<String, String> {
    #[cfg(unix)] {
        let mut bytes = [0; 32];
        File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut bytes)).map_err(|_| "operating system randomness unavailable")?;
        Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }
    #[cfg(not(unix))] { Err("capability provisioning currently requires Unix /dev/urandom".into()) }
}

fn private_database_parent(path: &Path) -> Result<(), String> {
    if !path.is_absolute() { return Err("DB must be an absolute path".into()); }
    let parent = path.parent().ok_or("DB needs a parent directory")?;
    let metadata = fs::metadata(parent).map_err(|_| "create the private database directory first")?;
    if !metadata.is_dir() { return Err("database parent must be a directory".into()); }
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 { return Err("database directory must deny group and other permissions".into()); }
    }
    #[cfg(not(unix))] { return Err("gateway executable currently requires Unix".into()); }
    Ok(())
}

fn execute(args: &[String]) -> Result<Option<Value>, String> {
    match args.first().map(String::as_str) {
        Some("help" | "--help" | "-h") | None => Ok(Some(json!({"usage":[
            "mas-gateway provision POLICY.json PRIVATE_CONFIG.json",
            "mas-gateway export PRIVATE_CONFIG.json CREDENTIAL_ID PRIVATE_SESSION.json",
            "mas-gateway serve /absolute/private/state.sqlite PRIVATE_CONFIG.json",
            "mas-gateway host /absolute/private/state.sqlite PRIVATE_CONFIG.json RUN",
            "mas-gateway supervise LIFETIME_FD CHANNEL_FD DEADLINE_MS MEMORY_BYTES CPU_SECONDS FILE_BYTES LOCK_FDS -- /absolute/worker [ARGS...]"
        ],"transport":"JSONL on a private host broker's stdin/stdout; no socket listener","platform":"Unix, /dev/urandom; credential files mode 0600, database directory mode 0700"}))),
        Some("provision") if args.len() == 3 => {
            let policy = read_json(Path::new(&args[1]), false)?;
            let mut config = normalise_config(&policy, false).map_err(|_| "invalid provisioning policy")?;
            for credential in config["credentials"].as_array_mut().unwrap() { credential["token"] = json!(random_token()?); }
            normalise_config(&config, true).map_err(|_| "capability provisioning failed")?;
            write_private(Path::new(&args[2]), &config)?;
            Ok(Some(json!({"provisioned":true})))
        },
        Some("export") if args.len() == 4 => {
            let config = normalise_config(&read_json(Path::new(&args[1]), true)?, true).map_err(|_| "invalid gateway configuration")?;
            let credential = config["credentials"].as_array().unwrap().iter().find(|c| c["credential_id"].as_str() == Some(&args[2])).ok_or("unknown credential id")?;
            write_private(Path::new(&args[3]), &json!({"token":credential["token"],"run_id":credential["run_id"],"actor":credential["actor"]}))?;
            Ok(Some(json!({"exported":true})))
        },
        Some("serve") if args.len() == 3 => {
            let db = Path::new(&args[1]);
            private_database_parent(db)?;
            let config = read_json(Path::new(&args[2]), true)?;
            let store = Store::open(db).map_err(|_| "database unavailable")?;
            let mut gateway = Gateway::open(store, &config)?;
            let stdin = io::stdin();
            let stdout = io::stdout();
            gateway.serve(&mut BufReader::new(stdin.lock()), &mut stdout.lock())?;
            Ok(None)
        },
        Some("host") if args.len() == 4 => {
            let db = Path::new(&args[1]);
            private_database_parent(db)?;
            let config = read_json(Path::new(&args[2]), true)?;
            let mut session = mas_protocol_codec::host_ipc::HostSession::open(db, &config, &args[3])
                .map_err(|_| "host IPC unavailable")?;
            let stdin = io::stdin();
            let stdout = io::stdout();
            session.serve(&mut BufReader::new(stdin.lock()), &mut stdout.lock())?;
            Ok(None)
        },
        Some("supervise") => {
            mas_protocol_codec::worker_supervisor::supervise(&args[1..])?;
            Ok(None)
        },
        _ => Err("incorrect arguments; use mas-gateway help".into()),
    }
}

fn main() {
    match execute(&env::args().skip(1).collect::<Vec<_>>()) {
        Ok(Some(result)) => println!("{}", json!({"ok":true,"result":result})),
        Ok(None) => {},
        Err(message) => {
            // Errors contain only constant diagnostics, never a path, token,
            // request body, database diagnostic or credential configuration.
            eprintln!("{}", json!({"ok":false,"error":{"code":"gateway_unavailable","message":message}}));
            std::process::exit(2);
        },
    }
}
