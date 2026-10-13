//! Executable private-stdio capability demonstration; no live AI/provider calls.
//! Build binaries first, then `cargo run --release --example gateway`.
use mas_protocol_codec::store::Store;
use serde_json::{json, Value};
use std::{fs, io::Write, path::Path, process::{Command, Stdio}};

fn run_command(binary:&Path, args:&[&str]) -> Result<(),String> {
    let output = Command::new(binary).args(args).output().map_err(|_|"could not run gateway binary")?;
    if !output.status.success() { return Err("gateway provisioning command failed".into()); }
    Ok(())
}
fn exchange(binary:&Path, database:&Path, config:&Path, requests:&[Value]) -> Result<Vec<Value>,String> {
    let mut child = Command::new(binary).args(["serve",database.to_str().ok_or("invalid database path")?,config.to_str().ok_or("invalid config path")?])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|_|"gateway process unavailable")?;
    let mut stdin = child.stdin.take().ok_or("gateway stdin unavailable")?;
    for request in requests {
        serde_json::to_writer(&mut stdin,request).map_err(|_|"request write failed")?;
        stdin.write_all(b"\n").map_err(|_|"request write failed")?;
    }
    drop(stdin);
    let output = child.wait_with_output().map_err(|_|"gateway process unavailable")?;
    if !output.status.success() { return Err("gateway process failed".into()); }
    output.stdout.split(|b| *b==b'\n').filter(|line| !line.is_empty()).map(|line|serde_json::from_slice(line).map_err(|_|"invalid gateway response".into())).collect()
}
fn session(path:&Path) -> Result<Value,String> {
    serde_json::from_slice(&fs::read(path).map_err(|_|"private session unavailable")?).map_err(|_|"invalid private session".into())
}
fn request(session:&Value, mut value:Value) -> Value { value["token"] = session["token"].clone(); value }

#[cfg(unix)]
fn demonstrate(root:&Path,binary:&Path) -> Result<Value,String> {
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir(root).map_err(|_|"cannot create private example directory")?;
    fs::set_permissions(root,fs::Permissions::from_mode(0o700)).map_err(|_|"cannot protect example directory")?;
    let database=root.join("state.sqlite");
    let config=root.join("credentials.json");
    let policy=root.join("policy.json");
    let alpha=root.join("alpha-session.json");
    let beta=root.join("beta-session.json");
    let mut store=Store::open(&database)?;
    store.create_run("demo",&json!({"agents":["alpha","beta"],"edges":[["alpha","beta"],["beta","alpha"]]}))?;
    drop(store);
    fs::write(&policy,json!({"gateway_id":"demo","max_attempts":4,"credentials":[
        {"credential_id":"alpha","run_id":"demo","actor":"alpha","max_attempts":2},
        {"credential_id":"beta","run_id":"demo","actor":"beta","max_attempts":2}
    ]}).to_string()).map_err(|_|"cannot write example policy")?;
    run_command(binary,&["provision",policy.to_str().unwrap(),config.to_str().unwrap()])?;
    run_command(binary,&["export",config.to_str().unwrap(),"alpha",alpha.to_str().unwrap()])?;
    run_command(binary,&["export",config.to_str().unwrap(),"beta",beta.to_str().unwrap()])?;
    let alpha=session(&alpha)?;
    let beta=session(&beta)?;
    let legitimate="message \"m1\" run \"demo\" from \"alpha\" to \"beta\" version \"1\" kind \"Notice\" payload {\"text\":\"hello\"};";
    let forged="message \"m2\" run \"demo\" from \"beta\" to \"alpha\" version \"1\" kind \"Notice\" payload {};";
    let first=exchange(binary,&database,&config,&[
        request(&alpha,json!({"command":"send","wire":legitimate})),
        request(&alpha,json!({"command":"send","wire":forged}))
    ])?;
    if first.len()!=2 || first[0]["ok"]!=true || first[1]["error"]["code"]!="operation_rejected" { return Err("actor-binding example failed".into()); }
    // Every exchange launches a fresh process against the same durable ledger.
    let received=exchange(binary,&database,&config,&[request(&beta,json!({"command":"receive"}))])?;
    if received.len()!=1 || received[0]["result"][0]["message"]["message_id"]!="m1" { return Err("mailbox example failed".into()); }
    let acknowledged=exchange(binary,&database,&config,&[request(&beta,json!({"command":"ack","message_id":"m1"}))])?;
    if acknowledged.len()!=1 || acknowledged[0]["ok"]!=true { return Err("acknowledgement example failed".into()); }
    let exhausted=exchange(binary,&database,&config,&[request(&beta,json!({"command":"receive"}))])?;
    if exhausted.len()!=1 || exhausted[0]["error"]["code"]!="admission_closed" { return Err("durable-cap example failed".into()); }
    Ok(json!({"example":"private-stdio-gateway","authenticated_send":true,"forged_sender_rejected":true,
        "received_after_restart":true,"acknowledged":true,"attempts_including_rejection":4,
        "restart_preserves_exhaustion":true,"credentials_in_report":false,
        "scope":"Local deterministic fixture; the host provisions tokens and privately routes responses."}))
}

fn main() -> Result<(),String> {
    #[cfg(not(unix))] { return Err("gateway example requires Unix".into()); }
    #[cfg(unix)] {
        let executable=std::env::current_exe().map_err(|_|"example path unavailable")?;
        let binary=executable.parent().and_then(Path::parent).ok_or("example build directory unavailable")?.join("mas-gateway");
        if !binary.is_file() { return Err("build binaries first: cargo build --release --locked --manifest-path rust-protocol/Cargo.toml".into()); }
        let stamp=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|"clock unavailable")?.as_nanos();
        let root=std::env::temp_dir().join(format!("mas-gateway-example-{}-{stamp}",std::process::id()));
        let result=demonstrate(&root,&binary);
        let _=fs::remove_dir_all(&root);
        println!("{}",serde_json::to_string_pretty(&result?).map_err(|_|"report encoding failed")?);
        Ok(())
    }
}
