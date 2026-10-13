//! Linux SPIN orchestration, generation, resource budgets and evidence capture.
//! Protocol and verification automation use Rust; SPIN generates a C verifier.
use serde_json::{json, Value};
use std::{env, fs, io, path::{Path, PathBuf}, process::{Command, Stdio}, thread, time::{Duration, Instant}};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const PROPERTIES: [&str; 2] = ["message_0_progress", "message_1_progress"];

struct Options {
    root: PathBuf, examples: PathBuf, out: PathBuf, seconds: u64,
    memory: u64, depth: u64, only: Option<String>, generate_only: bool,
}

fn sha(path: &Path) -> Result<String> {
    let out = Command::new("sha256sum").arg(path).output()?;
    if !out.status.success() { return Err("sha256sum failed".into()); }
    let text = String::from_utf8(out.stdout)?;
    let value = text.split_whitespace().next().ok_or("missing digest")?;
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid SHA256 output".into());
    }
    Ok(value.to_owned())
}

fn run(command: &[String], cwd: &Path, name: &str, opt: &Options) -> Result<Value> {
    let logfile = cwd.join(format!("{name}.log"));
    let log = fs::File::create(&logfile)?;
    let started = Instant::now();
    // Arguments remain separate argv entries. The shell evaluates only this
    // constant script; filenames and model content never become shell code.
    let mut child = Command::new("sh")
        .args(["-c", "ulimit -v \"$1\" || exit 125; shift; exec \"$@\"", "verify-limit"])
        .arg((opt.memory * 1024).to_string()).args(command)
        .current_dir(cwd).stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?)).stderr(Stdio::from(log)).spawn()?;
    let (code, timeout) = loop {
        if let Some(status) = child.try_wait()? { break (status.code(), false); }
        if started.elapsed() >= Duration::from_secs(opt.seconds) {
            child.kill()?; let _ = child.wait(); break (None, true);
        }
        thread::sleep(Duration::from_millis(10));
    };
    Ok(json!({"command": command, "returncode": code, "timeout":timeout,
        "elapsed_seconds":started.elapsed().as_secs_f64(),
        "log":logfile.file_name().unwrap().to_string_lossy(), "log_sha256":sha(&logfile)?}))
}

fn invoke(commands: &mut Vec<Value>, command: Vec<String>, cwd: &Path, name: &str, opt: &Options) -> Result<Value> {
    let result = run(&command, cwd, name, opt)?;
    commands.push(result.clone()); Ok(result)
}

fn classify(record: &Value, cwd: &Path) -> Result<&'static str> {
    let text = fs::read_to_string(cwd.join(record["log"].as_str().ok_or("missing log")?))?;
    let lower = text.to_lowercase();
    if record["timeout"] == true || ["max search depth", "out of memory", "not enough memory", "vectorsz", "reached -dmemlim"]
        .iter().any(|needle| lower.contains(needle)) { return Ok("INCOMPLETE"); }
    let errors = text.split("errors:").nth(1).and_then(|s| s.split_whitespace().next()).and_then(|s| s.parse::<u64>().ok());
    if record["returncode"] != 0 || errors.is_none() { return Ok("EXECUTION_FAILED"); }
    if errors.unwrap() > 0 { return Ok("COUNTEREXAMPLE"); }
    if !text.contains("Full statespace search") || text.contains("Warning: Search not completed") {
        return Ok("INCOMPLETE");
    }
    Ok("COMPLETE_NO_ERRORS")
}

fn generate(path: &Path, root: &Path) -> Result<String> {
    let data = mas_protocol_codec::parse_json(&fs::read_to_string(path)?)
        .map_err(|diagnostic|format!("invalid topology JSON: {}",diagnostic.message))?;
    let agents = data["agents"].as_array().ok_or("agents must be array")?;
    if !(2..=6).contains(&agents.len()) { return Err("model requires 2..6 participants".into()); }
    let mut names = std::collections::BTreeSet::new();
    for agent in agents {
        let name=agent.as_str().ok_or("agent identity must be a string")?;
        if name.is_empty() || name.len()>128 || !name.as_bytes()[0].is_ascii_alphanumeric()
            || !name.bytes().all(|b|b.is_ascii_alphanumeric() || b"_.:/-".contains(&b))
            || !names.insert(name) { return Err("invalid or duplicate agent identity".into()); }
    }
    let edges = data["edges"].as_array().ok_or("edges must be array")?;
    let mut indexed = Vec::new();
    for edge in edges {
        let pair = edge.as_array().ok_or("invalid edge")?;
        if pair.len() != 2 { return Err("edge must contain two participants".into()); }
        if pair[0]==pair[1] {return Err("self edges are outside the model contract".into());}
        indexed.push((agents.iter().position(|a| a == &pair[0]).ok_or("unknown sender")?,
                      agents.iter().position(|a| a == &pair[1]).ok_or("unknown recipient")?));
    }
    indexed.sort(); indexed.dedup();
    if indexed.is_empty() { return Err("empty topology".into()); }
    let capacity = data["mailbox_capacity"].as_u64().ok_or("explicit integer mailbox_capacity required")?;
    if capacity == 0 || capacity > 100 { return Err("invalid model mailbox capacity".into()); }
    let allowed = indexed.iter().map(|(a,b)|format!("        allowed[{a} * AGENTS + {b}] = true;")).collect::<Vec<_>>().join("\n");
    let routes = (0..2).map(|m| format!("        if\n{}\n        fi;", indexed.iter()
        .map(|(a,b)|format!("        :: sender[{m}] = {a}; recipient[{m}] = {b}"))
        .collect::<Vec<_>>().join("\n"))).collect::<Vec<_>>().join("\n");
    let action = fs::read_to_string(root.join("formal/actions.pml.in"))?;
    let actions = (0..2).map(|m| action.replace("@M@", &m.to_string())).collect::<Vec<_>>().join("\n");
    let start = (0..agents.len()).map(|i|format!("        run Agent({i});")).collect::<Vec<_>>().join("\n");
    let mut model = fs::read_to_string(root.join("formal/protocol.pml.in"))?;
    for (key,value) in [("AGENT_COUNT",agents.len().to_string()),("CAPACITY",capacity.to_string()),
        ("ALLOWED",allowed),("ROUTES",routes),("ACTIONS",actions),("START",start)] {
        model = model.replace(&format!("@{key}@"), &value);
    }
    Ok(model)
}

fn strings(values: &[&str]) -> Vec<String> { values.iter().map(|s|s.to_string()).collect() }

#[derive(Default)]
struct CaseOptions {
    defect: Option<&'static str>, no_reduce: bool, capacity: Option<u64>,
    standalone: bool, unbounded_faults: bool, assumption_witness: bool,
    weak_fairness: bool,
}

fn expected_check_satisfied(check: &Value, counterexample: bool, cycle: bool) -> bool {
    if !counterexample { return check["status"] == "COMPLETE_NO_ERRORS"; }
    check["status"] == "COUNTEREXAMPLE" && check["counterexample_kind"] == if cycle {"acceptance_cycle"} else {"assertion"}
        && check["replay"]["returncode"] == 0 && check["replay"]["timeout"] == false
}

fn run_case(source: &Path, dest: &Path, opt: &Options, case: CaseOptions) -> Result<Value> {
    let CaseOptions {defect,no_reduce,capacity,standalone:task,unbounded_faults,assumption_witness,weak_fairness}=case;
    fs::create_dir_all(dest)?;
    // Reject stale verifier/trail output from a previous interrupted invocation.
    for entry in fs::read_dir(dest)? {
        let path = entry?.path();
        let name = path.file_name().unwrap().to_string_lossy();
        if path.is_file() && (name.starts_with("pan") || name.ends_with(".trail")) { fs::remove_file(path)?; }
    }
    let model_name = if task { source.file_name().and_then(|s|s.to_str()).ok_or("invalid standalone model filename")? } else { "protocol.pml" };
    let model = dest.join(model_name);
    fs::write(&model, if task {fs::read_to_string(source)?} else {generate(source, &opt.root)?})?;
    let mut defs = Vec::new();
    if let Some(bug) = defect { defs.push(format!("-D{bug}")); }
    if let Some(size) = capacity { defs.push(format!("-DCAPACITY={size}")); }
    if unbounded_faults {defs.push("-DUNBOUNDED_FAULTS".into());}
    let mut commands = Vec::new(); let mut checks = Vec::new();
    let mut simulations_ok = true;
    if task && source.file_name().is_some_and(|name| name == "host-recovery.pml") && defect.is_none() {
        for seed in [17, 29, 53] {
            let command = strings(&["spin", &format!("-n{seed}"), "-u10000", "-p", "-g", "-l", model_name]);
            let mut record = run(&command, dest, &format!("simulation-{seed}"), opt)?;
            let text = fs::read_to_string(dest.join(record["log"].as_str().unwrap()))?;
            let complete = record["returncode"] == 0 && record["timeout"] == false
                && !text.contains("depth-limit") && !text.contains("assertion violated");
            record["seed"] = json!(seed);
            record["status"] = json!(if complete {"SAMPLED_COMPLETE"} else {"SAMPLED_INCOMPLETE"});
            simulations_ok &= complete;
            commands.push(record);
        }
    }
    if defect.is_none() && !no_reduce && !task && !unbounded_faults {
        for seed in [17,29] {
            let mut cmd = strings(&["spin"]); cmd.extend(defs.clone());
            cmd.extend(strings(&[&format!("-n{seed}"),"-u10000","-p","-g","-l",model_name]));
            let mut record = run(&cmd, dest, &format!("simulation-{seed}"), opt)?;
            let text = fs::read_to_string(dest.join(record["log"].as_str().unwrap()))?;
            let accepted = (0..2).all(|m|text.contains(&format!("EVENT send {m} ")));
            let acked = (0..2).all(|m|text.contains(&format!("EVENT ack {m} ")));
            let complete = record["returncode"] == 0 && record["timeout"] == false
                && !text.contains("depth-limit") && !text.contains("timeout");
            record["seed"]=json!(seed); record["both_messages_accepted"]=json!(accepted);
            record["both_messages_acknowledged"]=json!(acked);
            record["status"]=json!(if complete {"SAMPLED_COMPLETE"} else {"SAMPLED_INCOMPLETE"});
            simulations_ok &= complete && accepted && acked; commands.push(record);
        }
    }
    let mut cmd=strings(&["spin"]);cmd.extend(defs.clone());cmd.extend(strings(&["-a",model_name]));
    let generated=invoke(&mut commands,cmd,dest,"generate",opt)?;
    if generated["returncode"] != 0 {
        return Ok(json!({"topology":source.file_name().unwrap().to_string_lossy(),"status":"GENERATION_FAILED","commands":commands}));
    }
    let modes = if assumption_witness || defect==Some("BUG_NO_ACK") {vec!["ltl"]}
        else if task || unbounded_faults || defect.is_some() {vec!["safety"]} else {vec!["safety","ltl"]};
    for mode in modes {
        let mut cmd=strings(&["cc","-O2",&format!("-DMEMLIM={}",opt.memory-32)]);
        if no_reduce { cmd.push("-DNOREDUCE".into()); }
        if mode=="safety" { cmd.extend(strings(&["-DSAFETY","-DNOCLAIM"])); }
        cmd.extend(strings(&["-o",&format!("pan-{mode}"),"pan.c"]));
        let compiled=invoke(&mut commands,cmd,dest,&format!("compile-{mode}"),opt)?;
        if compiled["returncode"]!=0 {checks.push(json!({"property":mode,"status":"COMPILATION_FAILED"}));continue;}
        let properties=if mode=="safety" {vec!["safety"]} else if defect.is_some() || assumption_witness {vec![PROPERTIES[0]]} else {PROPERTIES.to_vec()};
        for property in properties {
            let mut cmd=strings(&[&format!("./pan-{mode}"),"-b",&format!("-m{}",opt.depth),"-w20"]);
            if mode=="ltl" {
                cmd.extend(strings(&["-a","-N",property]));
                if weak_fairness {cmd.push("-f".into());}
            }
            let mut checked=invoke(&mut commands,cmd,dest,&format!("check-{property}"),opt)?;
            checked["status"]=json!(classify(&checked,dest)?); checked["property"]=json!(property);
            let raw=fs::read_to_string(dest.join(checked["log"].as_str().unwrap()))?;
            checked["counterexample_kind"]=json!(if raw.contains("acceptance cycle") {"acceptance_cycle"}
                else if raw.contains("assertion violated") {"assertion"} else {"none_or_other"});
            let trail=dest.join(format!("{model_name}.trail"));
            if trail.exists() {
                let saved=dest.join(format!("{property}.trail")); fs::rename(trail,&saved)?;
                checked["trail"]=json!(saved.file_name().unwrap().to_string_lossy());checked["trail_sha256"]=json!(sha(&saved)?);
                let mut cmd=strings(&["spin"]);cmd.extend(defs.clone());
                cmd.extend(strings(&["-t","-k",saved.file_name().unwrap().to_str().unwrap(),"-p","-g","-l",model_name]));
                checked["replay"]=invoke(&mut commands,cmd,dest,&format!("replay-{property}"),opt)?;
            }
            checks.push(checked);
        }
    }
    let counterexample=defect.is_some() || assumption_witness;
    let expected=if counterexample {"COUNTEREXAMPLE"}else{"COMPLETE_NO_ERRORS"};
    let cycle=assumption_witness || defect==Some("BUG_NO_ACK");
    let ok=simulations_ok && !checks.is_empty() && checks.iter().all(|c|expected_check_satisfied(c,counterexample,cycle));
    for entry in fs::read_dir(dest)? {
        let path=entry?.path();let name=path.file_name().unwrap().to_string_lossy();
        if path.is_file() && name.starts_with("pan") && !name.ends_with(".log") {fs::remove_file(path)?;}
    }
    let config:Value=if task{json!({})}else{mas_protocol_codec::parse_json(&fs::read_to_string(source)?)
        .map_err(|diagnostic|format!("invalid topology JSON: {}",diagnostic.message))?};
    Ok(json!({"topology":source.file_name().unwrap().to_string_lossy(),"source_sha256":sha(source)?,
        "model_sha256":sha(&model)?,"defect":defect,"capacity":capacity.unwrap_or(config["mailbox_capacity"].as_u64().unwrap_or(2)),
        "partial_order_reduction":!no_reduce,"weak_process_fairness":weak_fairness,
        "fault_repetition":if unbounded_faults {"unlimited"} else {"bounded_or_not_applicable"},
        "purpose":if assumption_witness {"assumption_counterexample"} else if defect.is_some() {"deliberate_defect"}
            else if task {"standalone_safety"} else if unbounded_faults {"unbounded_fault_safety"} else {"bounded_transport"},
        "expected_result":expected,"expectation_met":ok,
        "commands":commands,"checks":checks,"status":if ok{"PASS"}else{"FAIL_OR_INCONCLUSIVE"}}))
}

fn main() -> Result<()> {
    let root=PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().ok_or("crate needs repository parent")?.to_path_buf();
    let mut opt=Options{examples:root.join("examples"),out:root.join("formal/evidence"),root,
        seconds:30,memory:512,depth:100000,only:None,generate_only:false};
    let mut args=env::args().skip(1);
    while let Some(arg)=args.next() {
        match arg.as_str() {
            "--out"=>opt.out=PathBuf::from(args.next().ok_or("--out needs path")?),
            "--examples"=>opt.examples=PathBuf::from(args.next().ok_or("--examples needs path")?),
            "--seconds"=>opt.seconds=args.next().ok_or("--seconds needs integer")?.parse()?,
            "--memory-mb"=>opt.memory=args.next().ok_or("--memory-mb needs integer")?.parse()?,
            "--depth"=>opt.depth=args.next().ok_or("--depth needs integer")?.parse()?,
            "--only"=>opt.only=Some(args.next().ok_or("--only needs topology")?),
            "--generate-only"=>opt.generate_only=true,
            "--help"|"-h"=>{println!("verify [--out PATH] [--examples PATH] [--only TOPOLOGY] [--generate-only] [--seconds 30] [--memory-mb 512] [--depth 100000]\nRequires Linux, SPIN, cc, POSIX sh and sha256sum.");return Ok(());},
            _=>return Err(format!("unknown option {arg}").into()),
        }
    }
    if opt.seconds==0 || opt.memory<64 || opt.depth==0 {return Err("invalid positive resource budget".into());}
    fs::create_dir_all(&opt.out)?;opt.out=opt.out.canonicalize()?;
    let mut sources=fs::read_dir(&opt.examples)?.filter_map(|e|e.ok().map(|x|x.path()))
        .filter(|p|p.extension().is_some_and(|e|e=="json"))
        .filter(|p|opt.only.as_ref().is_none_or(|only|p.file_stem().is_some_and(|s|s==only.as_str())))
        .collect::<Vec<_>>();sources.sort();
    if sources.is_empty(){return Err("no topology examples found".into());}
    if opt.generate_only {
        for source in &sources {
            let target=opt.out.join(format!("{}.pml",source.file_stem().unwrap().to_string_lossy()));
            fs::write(&target,generate(source,&opt.root)?)?;println!("{}",target.display());
        }
        return Ok(());
    }
    // Invalidate a previous success before changing any checker output. An
    // interruption or propagation of an IO/tool error must never preserve PASS.
    fs::write(opt.out.join("summary.json"),b"{\"status\":\"RUNNING_OR_INTERRUPTED\"}\n")?;
    let mut versions=Vec::new();
    for (command,name) in [(strings(&["spin","-V"]),"spin-version"),(strings(&["cc","--version"]),"compiler-version")] {
        let version=run(&command,&opt.out,name,&opt)?;
        if version["returncode"]!=0 {
            fs::write(opt.out.join("summary.json"),serde_json::to_vec_pretty(&json!({"status":"NOT_EXECUTED","tool":command,"record":version}))?)?;
            return Err("required verification tool unavailable".into());
        }
        versions.push(version);
    }
    let mut cases=Vec::new();
    for source in &sources {
        let name=source.file_stem().unwrap().to_string_lossy();
        let case=run_case(source,&opt.out.join(name.as_ref()),&opt,CaseOptions::default())?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    for (defect,name,no_reduce) in [(Some("BUG_EARLY_ACK"),"negative-early-ack",false),
        (Some("BUG_ALLOW_FORBIDDEN"),"negative-forbidden-route",false),
        (Some("BUG_NO_ACK"),"negative-no-ack",false),(None,"no-reduction",true)] {
        let case=run_case(&sources[0],&opt.out.join(name),&opt,CaseOptions{defect,no_reduce,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let pressure=sources.iter().find(|p|p.file_stem().is_some_and(|s|s=="star")).unwrap_or(&sources[0]);
    let case=run_case(pressure,&opt.out.join("capacity-one"),&opt,CaseOptions{capacity:Some(1),..Default::default()})?;
    println!("capacity-one {}",case["status"]);cases.push(case);
    for (defect,name) in [(None,"task-lifecycle"),(Some("BUG_DOUBLE_TERMINAL"),"negative-task-terminal")] {
        let case=run_case(&opt.root.join("formal/task-lifecycle.pml"),&opt.out.join(name),&opt,CaseOptions{defect,standalone:true,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    // Safety does not require any upper bound on these repeatable faults.
    for source in &sources {
        let name=format!("unbounded-{}",source.file_stem().unwrap().to_string_lossy());
        let case=run_case(source,&opt.out.join(&name),&opt,CaseOptions{unbounded_faults:true,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    for (name,source,capacity,no_reduce) in [("unbounded-capacity-one",pressure,Some(1),false),
        ("unbounded-no-reduction",&sources[0],None,true)] {
        let case=run_case(source,&opt.out.join(name),&opt,CaseOptions{unbounded_faults:true,capacity,no_reduce,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let mesh=sources.iter().find(|p|p.file_stem().is_some_and(|s|s=="mesh")).unwrap_or(&sources[0]);
    for (name,weak_fairness) in [("assumption-unbounded-no-fairness",false),("assumption-unbounded-weak-fairness",true)] {
        let case=run_case(mesh,&opt.out.join(name),&opt,CaseOptions{unbounded_faults:true,assumption_witness:true,weak_fairness,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    for (model,name,defect) in [
        ("transaction-recovery.pml","transaction-recovery",None),
        ("transaction-recovery.pml","negative-partial-commit",Some("BUG_PARTIAL_COMMIT")),
        ("transaction-recovery.pml","negative-duplicate-insert",Some("BUG_DUPLICATE_INSERT")),
        ("task-expiry.pml","task-expiry",None),
        ("task-expiry.pml","negative-expiry-overwrite",Some("BUG_EXPIRY_OVERWRITE")),
        ("host-recovery.pml","host-recovery",None),
        ("host-recovery.pml","negative-host-uncharged",Some("BUG_HOST_UNCHARGED")),
        ("host-recovery.pml","negative-host-replay",Some("BUG_HOST_REPLAY_UNKNOWN")),
        ("host-recovery.pml","negative-host-stale-owner",Some("BUG_HOST_STALE_OWNER")),
        ("host-recovery.pml","negative-host-mutable-result",Some("BUG_HOST_MUTABLE_RESULT"))] {
        let case=run_case(&opt.root.join("formal").join(model),&opt.out.join(name),&opt,
            CaseOptions{standalone:true,defect,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let ok=cases.iter().all(|c|c["status"]=="PASS");
    let summary=json!({"status":if ok{"PASS"}else{"FAIL_OR_INCONCLUSIVE"},"versions":versions,"cases":cases,
        "bounds":{"messages":2,"durable_slots":2,"bounded_transport":{"lost_ack_per_message":1,"restart_per_message":1,"redelivery_per_message":1},
            "unbounded_fault_safety":"no bound on repeated lost acknowledgement, restart or redelivery transitions",
            "host_recovery":{"logical_calls":1,"attempts":2,"crashes":1,"owners":2}},
        "limits":{"seconds_per_command":opt.seconds,"memory_mb":opt.memory,"depth":opt.depth},
        "assumptions":["fixed membership and directed ACL","no scheduling fairness required by bounded transport model",
            "bounded faults for positive transport liveness only","SQLite transaction isolation and atomic recovery boundary",
            "host expiry is an explicit event, with no wall-clock or eventual host-execution guarantee","transport acknowledgement differs from task outcome",
            "host recovery administrative no-effect evidence is trusted; no remote exactly-once or implementation refinement claim"],
        "runner_sha256":sha(&opt.root.join("rust-protocol/src/bin/verify.rs"))?,
        "template_sha256":sha(&opt.root.join("formal/protocol.pml.in"))?,
        "actions_sha256":sha(&opt.root.join("formal/actions.pml.in"))?,
        "transaction_model_sha256":sha(&opt.root.join("formal/transaction-recovery.pml"))?,
        "expiry_model_sha256":sha(&opt.root.join("formal/task-expiry.pml"))?,
        "host_recovery_model_sha256":sha(&opt.root.join("formal/host-recovery.pml"))?});
    fs::write(opt.out.join("summary.json"),format!("{}\n",serde_json::to_string_pretty(&summary)?))?;
    if !ok{return Err(io::Error::other("model verification failed or incomplete; inspect summary.json").into());}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scratch() -> PathBuf {
        let path=env::temp_dir().join(format!("mas-verify-test-{}-{}",std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir_all(&path).unwrap();path
    }
    #[test]
    fn incomplete_logs_never_become_success() {
        let dir=scratch();let record=json!({"log":"check.log","returncode":0,"timeout":false});
        for text in ["errors: 0", "Full statespace search errors: 0\nWarning: Search not completed",
            "Full statespace search errors: 0\npan: reached -DMEMLIM bound", "max search depth too small errors: 1"] {
            fs::write(dir.join("check.log"),text).unwrap();
            assert_ne!(classify(&record,&dir).unwrap(),"COMPLETE_NO_ERRORS");
        }
        fs::write(dir.join("check.log"),"Full statespace search errors: 0").unwrap();
        assert_eq!(classify(&record,&dir).unwrap(),"COMPLETE_NO_ERRORS");
        fs::write(dir.join("check.log"),"Full statespace search errors: 1\nWarning: Search not completed").unwrap();
        assert_eq!(classify(&record,&dir).unwrap(),"COUNTEREXAMPLE");
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn invalid_examples_fail_closed() {
        let root=PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
        let dir=scratch();let path=dir.join("example.json");
        let valid:Value=serde_json::from_slice(&fs::read(root.join("examples/star.json")).unwrap()).unwrap();
        for change in [json!({"agents":["a","a"]}),json!({"agents":["a",2]}),
            json!({"edges":[["hub","hub"]]}),json!({"mailbox_capacity":101}),
            json!({"mailbox_capacity":null}),json!({"mailbox_capacity":"2"})] {
            let mut config=valid.clone();for (key,value) in change.as_object().unwrap(){config[key]=value.clone();}
            fs::write(&path,serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(generate(&path,&root).is_err(),"{config}");
        }
        fs::write(&path,r#"{"agents":["a","b"],"edges":[["a","b"]],"mailbox_capacity":1,"mailbox_capacity":2}"#).unwrap();
        assert!(generate(&path,&root).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn expected_failures_need_the_right_counterexample_and_replay() {
        let mut check=json!({"status":"COUNTEREXAMPLE","counterexample_kind":"acceptance_cycle",
            "replay":{"returncode":0,"timeout":false}});
        assert!(expected_check_satisfied(&check,true,true));
        assert!(!expected_check_satisfied(&check,true,false));
        assert!(!expected_check_satisfied(&check,false,false));
        check["counterexample_kind"]=json!("assertion");
        assert!(expected_check_satisfied(&check,true,false));
        check["replay"]["timeout"]=json!(true);
        assert!(!expected_check_satisfied(&check,true,false));
        check["status"]=json!("INCOMPLETE");
        assert!(!expected_check_satisfied(&check,true,false));
    }
}
