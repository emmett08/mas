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

fn trace_state(value: &Value) -> Result<&'static str> {
    Ok(match value.as_str().ok_or("trace state must be a string")? {
        "absent" => "Absent", "queued" => "Queued", "running" => "Running",
        "input_required" => "Input", "auth_required" => "Auth", "completed" => "Completed",
        "failed" => "Failed", "cancelled" => "Cancelled", "unknown" => "Unknown",
        _ => return Err("unknown trace state".into()),
    })
}

fn trace_model(source: &str, trace: &Value) -> Result<String> {
    let mut body = String::from("\ninit { initialise();\n");
    for (index, action) in trace["actions"].as_array().ok_or("trace actions must be an array")?.iter().enumerate() {
        let op = action["op"].as_str().ok_or("missing trace operation")?;
        let args = action["args"].as_array().ok_or("missing trace arguments")?;
        let arity = match op {"submit" | "inspect" | "cancel" => 2, "update" => 4,
            "begin" | "answer" => 1, "commit" | "crash" => 0, _ => return Err("unknown trace operation".into())};
        if args.len() != arity { return Err("invalid trace arity".into()); }
        let mut values = Vec::new();
        for value in args {
            values.push(if op == "answer" {trace_state(value)?.to_owned()} else {
                let number = value.as_u64().ok_or("trace argument must be an unsigned integer")?;
                if number > 2 {return Err("trace argument exceeds declared finite values".into());}
                number.to_string()
            });
        }
        body += &format!("    {op}({});\n", values.join(","));
        let expected = &action["expect"];
        if let Some(error) = expected["error"].as_str() {
            let status = match error {"request_conflict"=>"Conflict", "task_not_found"=>"Forbidden",
                "invalid_task_state"=>"Invalid", "revision_limit"=>"Limit", _=>return Err("unknown trace error".into())};
            body += &format!("    assert(result == {status}); /* TRACE_RESPONSE */\n");
        } else if expected["idle"] == true {
            body += "    assert(result == Invalid); /* TRACE_RESPONSE */\n";
        } else if !matches!(op,"answer"|"crash") {
            body += "    assert(result == Accepted || result == Duplicate); /* TRACE_RESPONSE */\n";
        }
        for field in ["state", "revision", "cancelled", "events"] {
            let observations = expected[field].as_array().ok_or("missing per-step trace observation")?;
            if observations.len()!=2 {return Err("trace observations require two principals".into());}
            for (owner,value) in observations.iter().enumerate() {
                let value = if field=="state" {trace_state(value)?.to_owned()}
                    else if field=="cancelled" {u8::from(value.as_bool().ok_or("trace cancellation must be boolean")?).to_string()}
                    else {value.as_u64().ok_or("trace observation must be unsigned")?.to_string()};
                body += &format!("    assert({field}[{owner}] == {value}); /* TRACE_OBSERVATION */\n");
            }
        }
        body += &format!("    printf(\"TRACE step {index} {op}\\n\");\n");
    }
    body += "    assert(!busy); /* TRACE_WORKER_FINISHED */\n}\n";
    Ok(format!("#define TRACE_ONLY\n{source}{body}"))
}

#[derive(Default)]
struct CaseOptions {
    defect: Option<&'static str>, no_reduce: bool, capacity: Option<u64>,
    standalone: bool, unbounded_faults: bool, assumption_witness: bool,
    weak_fairness: bool,
    reachability_witness: bool,
}

fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_EARLY_ACK" => "TRANSPORT_ACK_REQUIRES_RECEIPT",
        "BUG_ALLOW_FORBIDDEN" => "TRANSPORT_ROUTE_ALLOWED",
        "BUG_DOUBLE_TERMINAL" => "TASK_SINGLE_TERMINAL",
        "BUG_PARTIAL_COMMIT" | "BUG_DUPLICATE_INSERT" => "TRANSACTION_ROW_COUNT",
        "BUG_EXPIRY_OVERWRITE" => "EXPIRY_SINGLE_TERMINAL",
        "BUG_HOST_UNCHARGED" => "HOST_ADMITTED",
        "BUG_HOST_REPLAY_UNKNOWN" => "HOST_NO_REPLAY",
        "BUG_HOST_STALE_OWNER" => "HOST_CURRENT_OWNER",
        "BUG_HOST_MUTABLE_RESULT" => "HOST_RESULT_IMMUTABLE",
        "BUG_CURSOR_UNCHARGED" => "CURSOR_ADMITTED",
        "BUG_CURSOR_REPLAY_UNKNOWN" => "CURSOR_NO_RELAUNCH",
        "BUG_CURSOR_LOST_ID" => "CURSOR_ID_RETAINED",
        "BUG_CURSOR_WRONG_RUN" => "CURSOR_RUN_CORRELATED",
        "BUG_CURSOR_CANCEL_CONFIRMED" | "BUG_CURSOR_TIMEOUT_CANCELLED" => "CURSOR_TERMINAL_OBSERVED",
        "BUG_CURSOR_TERMINAL_OVERWRITE" => "CURSOR_TERMINAL_IMMUTABLE",
        "BUG_BRIDGE_DUPLICATE" => "BRIDGE_KEY_DEDUPLICATED",
        "BUG_BRIDGE_CONFLICT" => "BRIDGE_KEY_CONTENT_IMMUTABLE",
        "BUG_BRIDGE_UNADMITTED" => "BRIDGE_ADMISSION_BEFORE_EFFECT",
        "BUG_BRIDGE_UNKNOWN_REPLAY" => "BRIDGE_UNKNOWN_NOT_REPLAYED",
        "BUG_BRIDGE_CANCEL_CONFIRMED" => "BRIDGE_CANCEL_NOT_CONFIRMATION",
        "BUG_BRIDGE_STALE_OVERWRITE" => "BRIDGE_TERMINAL_IMMUTABLE",
        "BUG_BRIDGE_EVENT_LOST" => "BRIDGE_EVENTS_ATOMIC",
        "BUG_BRIDGE_INBOX_OVERFLOW" => "BRIDGE_INBOX_BOUND",
        "BUG_BRIDGE_UNCOMMITTED_EVENT" => "BRIDGE_EVENT_COMMITTED",
        "BUG_BRIDGE_CURSOR_BEFORE_DELIVERY" => "BRIDGE_CURSOR_OBSERVED",
        "BUG_SERVICE_CANCEL_PAUSE" => "SERVICE_CANCEL_CLOSES_PAUSE",
        "BUG_SERVICE_UNSCOPED_KEY" => "SERVICE_OWNER_ISOLATED",
        "BUG_SERVICE_STALE_REVISION" => "SERVICE_COMMIT_CURRENT_REVISION",
        "BUG_SERVICE_EVENT_LOST" => "SERVICE_EVENT_ATOMIC",
        "WITNESS_SERVICE_TWO_OWNERS" => "REACH_SERVICE_TWO_OWNERS",
        "WITNESS_SERVICE_CANCEL_PAUSE" => "REACH_SERVICE_CANCEL_PAUSE",
        "WITNESS_SERVICE_REVISION" => "REACH_SERVICE_REVISION",
        "WITNESS_SERVICE_UNKNOWN_RESTART" => "REACH_SERVICE_UNKNOWN_RESTART",
        "WITNESS_SERVICE_FOREIGN_REJECTED" => "REACH_SERVICE_FOREIGN_REJECTED",
        _ => return None,
    })
}

// The original models predate assertion markers. Bind their exact replayed
// expressions to assertions present in the source, including SPIN's inline
// parameter substitution. Source-line remapping must not select a different
// assertion. Unknown expression formats fail closed.
fn core_replay_assertion(model_name: &str, model: &str, expression: &str) -> Option<&'static str> {
    let source: String = model.chars().filter(|c| !c.is_whitespace()).collect();
    let assertions = [
        ("task-lifecycle.pml", "assert(terminal_count[t]==1)", "assert((terminal_count[task]==1))", "TASK_SINGLE_TERMINAL"),
        ("transaction-recovery.pml", "assert(stored==committed[0]+committed[1])", "assert((stored==(committed[0]+committed[1])))", "TRANSACTION_ROW_COUNT"),
        ("host-recovery.pml", "assert(charged[attempt]&&reserved[attempt])", "assert((charged[attempt]&&reserved[attempt]))", "HOST_ADMITTED"),
        ("host-recovery.pml", "assert(dispatches[attempt]==1)", "assert((dispatches[attempt]==1))", "HOST_NO_REPLAY"),
        ("host-recovery.pml", "assert(!result_written||result_owner==owner)", "assert((!(result_written)||(result_owner==owner)))", "HOST_CURRENT_OWNER"),
        ("host-recovery.pml", "assert(!result_written||result_value==first_result)", "assert((!(result_written)||(result_value==first_result)))", "HOST_RESULT_IMMUTABLE"),
    ];
    for (name, original, expanded, marker) in assertions {
        if model_name == name && expression == expanded && source.contains(original) { return Some(marker); }
    }
    for message in 0..2 {
        if model_name == "protocol.pml" && source.contains("assert(!acknowledged[m]||received[m])")
            && expression == format!("assert((!(acknowledged[{message}])||received[{message}]))") {
            return Some("TRANSPORT_ACK_REQUIRES_RECEIPT");
        }
        if model_name == "task-expiry.pml" && source.contains("assert(terminal_count[task]==1)")
            && expression == format!("assert((terminal_count[{message}]==1))") {
            return Some("EXPIRY_SINGLE_TERMINAL");
        }
        for agents in 2..=6 {
            if model_name == "protocol.pml"
                && model.lines().any(|line| line.trim() == format!("#define AGENTS {agents}"))
                && source.contains(&format!("assert(allowed[actor*AGENTS+recipient[{message}]])"))
                && expression == format!("assert(allowed[((actor*{agents})+recipient[{message}])])") {
                return Some("TRANSPORT_ROUTE_ALLOWED");
            }
        }
    }
    None
}

// SPIN's replay reports the original model line, including inline assertions.
// Bind a mutation to that line's stable property marker, not merely any failure.
fn replay_assertion(model_name: &str, model: &str, replay: &str) -> Option<String> {
    let prefix = format!("spin: {model_name}:");
    let mut lines = replay.lines();
    let failure = lines.find(|line| line.starts_with(&prefix) && line.contains(", Error: assertion violated"))?;
    // SPIN 6.5.2 can report a collapsed #ifdef line for the later witness
    // alternatives. Match their exact failed expression instead of guessing
    // another source line. This remains an assertion-specific, fail-closed gate.
    let expression = lines.next()?.strip_prefix("spin: text of failed assertion: ")?;
    if let Some(marker) = core_replay_assertion(model_name, model, expression) { return Some(marker.to_owned()); }
    let witness = match expression {
        "assert(!((receipts[0]==1)&&(receipts[1]==1)))" => Some("REACH_SERVICE_TWO_OWNERS"),
        "assert(!(cancelled_pause))" => Some("REACH_SERVICE_CANCEL_PAUSE"),
        "assert(!(reached_revision))" => Some("REACH_SERVICE_REVISION"),
        "assert(!(reached_unknown_restart))" => Some("REACH_SERVICE_UNKNOWN_RESTART"),
        "assert(!(foreign_rejected))" => Some("REACH_SERVICE_FOREIGN_REJECTED"),
        _ => None,
    };
    if let Some(marker) = witness {
        return model.contains(&format!("/* {marker} */")).then(|| marker.to_owned());
    }
    let number = failure.strip_prefix(&prefix)?.split(',').next()?.parse::<usize>().ok()?;
    let source = model.lines().nth(number.checked_sub(1)?)?;
    let marker = source.split("/*").nth(1)?.split("*/").next()?.trim();
    if marker.is_empty() || !marker.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') {
        return None;
    }
    Some(marker.to_owned())
}

fn replay_cycle(property: &str, checked: &str, replay: &str) -> Option<String> {
    let named_check = checked.lines().any(|line| line == format!("pan: ltl formula {property}"));
    let claim = format!("({property}:1)");
    let named_replay = replay.lines().any(|line| line.split_whitespace().collect::<Vec<_>>().windows(4)
        .any(|part| part[0] == "proc" && part[1] == "-" && part[2] == claim && part[3].starts_with("_spin_nvr.tmp:")));
    (named_check && named_replay && replay.contains("<<<<<START OF CYCLE>>>>>")
        && !replay.contains("Error: assertion violated")).then(|| property.to_owned())
}

fn expected_check_satisfied(check: &Value, counterexample: bool, cycle: bool) -> bool {
    if !counterexample { return check["status"] == "COMPLETE_NO_ERRORS"; }
    check["status"] == "COUNTEREXAMPLE" && check["counterexample_kind"] == if cycle {"acceptance_cycle"} else {"assertion"}
        && check["replay"]["returncode"] == 0 && check["replay"]["timeout"] == false
        && if cycle {
            check["expected_ltl_property"].is_string() && check["expected_ltl_property"] == check["replayed_ltl_property"]
        } else {
            check["expected_assertion_id"].is_string() && check["expected_assertion_id"] == check["replayed_assertion_id"]
        }
}

fn run_case(source: &Path, dest: &Path, opt: &Options, case: CaseOptions) -> Result<Value> {
    let CaseOptions {defect,no_reduce,capacity,standalone:task,unbounded_faults,assumption_witness,weak_fairness,reachability_witness}=case;
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
    if task && source.file_name().is_some_and(|name| name == "host-recovery.pml" || name == "cursor-jobs.pml"
        || name == "bridge-service.pml" || name == "bridge-events.pml" || name == "task-service.pml") && defect.is_none() {
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
            checked["expected_assertion_id"] = json!(expected_assertion(defect));
            checked["expected_ltl_property"] = json!(if assumption_witness || defect==Some("BUG_NO_ACK") {Some(property)} else {None});
            let trail=dest.join(format!("{model_name}.trail"));
            if trail.exists() {
                let saved=dest.join(format!("{property}.trail")); fs::rename(trail,&saved)?;
                checked["trail"]=json!(saved.file_name().unwrap().to_string_lossy());checked["trail_sha256"]=json!(sha(&saved)?);
                let mut cmd=strings(&["spin"]);cmd.extend(defs.clone());
                cmd.extend(strings(&["-t","-k",saved.file_name().unwrap().to_str().unwrap(),"-p","-g","-l",model_name]));
                checked["replay"]=invoke(&mut commands,cmd,dest,&format!("replay-{property}"),opt)?;
                let replay = fs::read_to_string(dest.join(checked["replay"]["log"].as_str().ok_or("missing replay log")?))?;
                checked["replayed_assertion_id"] = json!(replay_assertion(model_name, &fs::read_to_string(&model)?, &replay));
                checked["replayed_ltl_property"] = json!(replay_cycle(property, &raw, &replay));
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
        "purpose":if reachability_witness {"reachability_witness"} else if assumption_witness {"assumption_counterexample"} else if defect.is_some() {"deliberate_defect"}
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
        ("host-recovery.pml","negative-host-mutable-result",Some("BUG_HOST_MUTABLE_RESULT")),
        ("cursor-jobs.pml","cursor-jobs",None),
        ("cursor-jobs.pml","negative-cursor-uncharged",Some("BUG_CURSOR_UNCHARGED")),
        ("cursor-jobs.pml","negative-cursor-relaunch",Some("BUG_CURSOR_REPLAY_UNKNOWN")),
        ("cursor-jobs.pml","negative-cursor-lost-id",Some("BUG_CURSOR_LOST_ID")),
        ("cursor-jobs.pml","negative-cursor-wrong-run",Some("BUG_CURSOR_WRONG_RUN")),
        ("cursor-jobs.pml","negative-cursor-cancel-confirmed",Some("BUG_CURSOR_CANCEL_CONFIRMED")),
        ("cursor-jobs.pml","negative-cursor-timeout-cancelled",Some("BUG_CURSOR_TIMEOUT_CANCELLED")),
        ("cursor-jobs.pml","negative-cursor-terminal-overwrite",Some("BUG_CURSOR_TERMINAL_OVERWRITE")),
        ("bridge-service.pml","bridge-service",None),
        ("bridge-service.pml","negative-bridge-duplicate",Some("BUG_BRIDGE_DUPLICATE")),
        ("bridge-service.pml","negative-bridge-conflict",Some("BUG_BRIDGE_CONFLICT")),
        ("bridge-service.pml","negative-bridge-uncharged",Some("BUG_BRIDGE_UNADMITTED")),
        ("bridge-service.pml","negative-bridge-unknown-replay",Some("BUG_BRIDGE_UNKNOWN_REPLAY")),
        ("bridge-service.pml","negative-bridge-cancel-confirmed",Some("BUG_BRIDGE_CANCEL_CONFIRMED")),
        ("bridge-service.pml","negative-bridge-stale-overwrite",Some("BUG_BRIDGE_STALE_OVERWRITE")),
        ("bridge-service.pml","negative-bridge-event-lost",Some("BUG_BRIDGE_EVENT_LOST")),
        ("bridge-events.pml","bridge-events",None),
        ("bridge-events.pml","negative-bridge-inbox-overflow",Some("BUG_BRIDGE_INBOX_OVERFLOW")),
        ("bridge-events.pml","negative-bridge-uncommitted-event",Some("BUG_BRIDGE_UNCOMMITTED_EVENT")),
        ("bridge-events.pml","negative-bridge-cursor-before-delivery",Some("BUG_BRIDGE_CURSOR_BEFORE_DELIVERY")),
        ("task-service.pml","task-service",None),
        ("task-service.pml","negative-service-cancel-pause",Some("BUG_SERVICE_CANCEL_PAUSE")),
        ("task-service.pml","negative-service-unscoped-key",Some("BUG_SERVICE_UNSCOPED_KEY")),
        ("task-service.pml","negative-service-stale-revision",Some("BUG_SERVICE_STALE_REVISION")),
        ("task-service.pml","negative-service-event-lost",Some("BUG_SERVICE_EVENT_LOST"))] {
        let case=run_case(&opt.root.join("formal").join(model),&opt.out.join(name),&opt,
            CaseOptions{standalone:true,defect,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let case=run_case(&opt.root.join("formal/cursor-jobs.pml"),&opt.out.join("cursor-jobs-no-reduction"),&opt,
        CaseOptions{standalone:true,no_reduce:true,..Default::default()})?;
    println!("cursor-jobs-no-reduction {}",case["status"]);cases.push(case);
    for name in ["bridge-service", "bridge-events", "task-service"] {
        let case=run_case(&opt.root.join(format!("formal/{name}.pml")),&opt.out.join(format!("{name}-no-reduction")),&opt,
            CaseOptions{standalone:true,no_reduce:true,..Default::default()})?;
        println!("{name}-no-reduction {}",case["status"]);cases.push(case);
    }
    for (defect,name) in [
        ("WITNESS_SERVICE_TWO_OWNERS", "reach-service-two-owners"),
        ("WITNESS_SERVICE_CANCEL_PAUSE", "reach-service-cancel-pause"),
        ("WITNESS_SERVICE_REVISION", "reach-service-revision"),
        ("WITNESS_SERVICE_UNKNOWN_RESTART", "reach-service-unknown-restart"),
        ("WITNESS_SERVICE_FOREIGN_REJECTED", "reach-service-foreign-rejected")] {
        let case=run_case(&opt.root.join("formal/task-service.pml"),&opt.out.join(name),&opt,
            CaseOptions{standalone:true,defect:Some(defect),reachability_witness:true,..Default::default()})?;
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let corpus_path = opt.root.join("host/tests/fixtures/task-service-traces.json");
    let corpus: Value = serde_json::from_str(&fs::read_to_string(&corpus_path)?)?;
    if corpus["version"] != 1 {return Err("unsupported trace corpus version".into());}
    let trace_sources = opt.out.join("trace-sources");fs::create_dir_all(&trace_sources)?;
    let task_model = fs::read_to_string(opt.root.join("formal/task-service.pml"))?;
    for trace in corpus["traces"].as_array().ok_or("missing shared traces")? {
        let name=trace["name"].as_str().ok_or("trace needs name")?;
        if name.is_empty() || !name.bytes().all(|b|b.is_ascii_alphanumeric() || b==b'_') {
            return Err("invalid trace name".into());
        }
        let name=format!("trace-service-{name}");
        let source=trace_sources.join(format!("{name}.pml"));
        fs::write(&source,trace_model(&task_model,trace)?)?;
        let mut case=run_case(&source,&opt.out.join(&name),&opt,CaseOptions{standalone:true,..Default::default()})?;
        case["purpose"]=json!("shared_trace_conformance");
        println!("{name} {}",case["status"]);cases.push(case);
    }
    let ok=cases.iter().all(|c|c["status"]=="PASS");
    let summary=json!({"status":if ok{"PASS"}else{"FAIL_OR_INCONCLUSIVE"},"versions":versions,"cases":cases,
        "bounds":{"messages":2,"durable_slots":2,"bounded_transport":{"lost_ack_per_message":1,"restart_per_message":1,"redelivery_per_message":1},
            "unbounded_fault_safety":"no bound on repeated lost acknowledgement, restart or redelivery transitions",
            "host_recovery":{"logical_calls":1,"attempts":2,"crashes":1,"owners":2},
            "cursor_jobs":{"logical_jobs":1,"remote_run_id_values":2,"crashes":1,"polls":2,"cancellation_requests":1},
            "bridge_service":{"principals":1,"request_keys":1,"payload_values":2,"submissions":3,"crashes":1,"observations":2},
            "bridge_events":{"tasks":2,"event_slots":3,"reads":2,"client_restarts":1},
            "task_service":{"principals":2,"tasks":2,"scheduled_tasks":1,"revisions_per_task":2,"workers":1,"crashes":1,
                "shared_trace_count":corpus["traces"].as_array().unwrap().len()}},
        "limits":{"seconds_per_command":opt.seconds,"memory_mb":opt.memory,"depth":opt.depth},
        "assumptions":["fixed membership and directed ACL","no scheduling fairness required by bounded transport model",
            "bounded faults for positive transport liveness only","SQLite transaction isolation and atomic recovery boundary",
            "host expiry is an explicit event, with no wall-clock or eventual host-execution guarantee","transport acknowledgement differs from task outcome",
            "host recovery administrative no-effect evidence is trusted; no remote exactly-once or implementation refinement claim",
            "Cursor job host operations are serialised; durable commits and remote dispatch are separate transitions",
            "Cursor unknown submissions remain unresolved without replay; cancellation requires a correlated terminal observation",
            "Cursor provider availability, remote cancellation, billing and local SDK implementation are outside the finite model",
            "Bridge identity scope represents an already authenticated principal; network authentication is not modelled",
            "Bridge task/event commits are atomic; remote dispatch and receipt persistence are distinct transitions",
            "Bridge event consumers own their cursor; no server acknowledgement, guaranteed delivery or eventual remote completion is claimed",
            "Local TaskService checks serialize API transactions, split worker claim/reply/commit, and assume the separately tested host checkpoint contract",
            "Local remote-evidence separation is an environmental assumption, not a proof of the remote implementation",
            "Shared finite traces compare model transitions with real TaskService/SQLite and an injected executor, not unrestricted implementation refinement"],
        "runner_sha256":sha(&opt.root.join("rust-protocol/src/bin/verify.rs"))?,
        "template_sha256":sha(&opt.root.join("formal/protocol.pml.in"))?,
        "actions_sha256":sha(&opt.root.join("formal/actions.pml.in"))?,
        "transaction_model_sha256":sha(&opt.root.join("formal/transaction-recovery.pml"))?,
        "expiry_model_sha256":sha(&opt.root.join("formal/task-expiry.pml"))?,
        "host_recovery_model_sha256":sha(&opt.root.join("formal/host-recovery.pml"))?,
        "cursor_jobs_model_sha256":sha(&opt.root.join("formal/cursor-jobs.pml"))?,
        "bridge_service_model_sha256":sha(&opt.root.join("formal/bridge-service.pml"))?,
        "bridge_events_model_sha256":sha(&opt.root.join("formal/bridge-events.pml"))?,
        "task_service_model_sha256":sha(&opt.root.join("formal/task-service.pml"))?,
        "task_service_trace_corpus_sha256":sha(&corpus_path)?});
    fs::write(opt.out.join("summary.json"),format!("{}\n",serde_json::to_string_pretty(&summary)?))?;
    if !ok{return Err(io::Error::other("model verification failed or incomplete; inspect summary.json").into());}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scratch() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path=env::temp_dir().join(format!("mas-verify-test-{}-{}-{}",std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
        fs::create_dir(&path).unwrap();path
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
        assert!(!expected_check_satisfied(&check,true,true));
        check["expected_ltl_property"]=json!("message_0_progress");
        check["replayed_ltl_property"]=json!("message_1_progress");
        assert!(!expected_check_satisfied(&check,true,true));
        check["replayed_ltl_property"]=json!("message_0_progress");
        assert!(expected_check_satisfied(&check,true,true));
        assert!(!expected_check_satisfied(&check,true,false));
        assert!(!expected_check_satisfied(&check,false,false));
        check["counterexample_kind"]=json!("assertion");
        assert!(!expected_check_satisfied(&check,true,false));
        check["expected_assertion_id"]=json!("SERVICE_OWNER_ISOLATED");
        check["replayed_assertion_id"]=json!("SERVICE_EVENT_ATOMIC");
        assert!(!expected_check_satisfied(&check,true,false));
        check["replayed_assertion_id"]=json!("SERVICE_OWNER_ISOLATED");
        assert!(expected_check_satisfied(&check,true,false));
        check["replay"]["timeout"]=json!(true);
        assert!(!expected_check_satisfied(&check,true,false));
        check["status"]=json!("INCOMPLETE");
        assert!(!expected_check_satisfied(&check,true,false));
    }
    #[test]
    fn original_assertion_mutations_reject_unrelated_counterexamples() {
        let root=PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
        let transport=generate(&root.join("examples/star.json"),&root).unwrap();
        // Expressions are taken from the retained SPIN replays. Deliberately
        // invalid line numbers exercise expression-based inline/ifdef mapping.
        for (defect,name,expression) in [
            ("BUG_EARLY_ACK","protocol.pml","assert((!(acknowledged[0])||received[0]))"),
            ("BUG_ALLOW_FORBIDDEN","protocol.pml","assert(allowed[((actor*3)+recipient[0])])"),
            ("BUG_DOUBLE_TERMINAL","task-lifecycle.pml","assert((terminal_count[task]==1))"),
            ("BUG_PARTIAL_COMMIT","transaction-recovery.pml","assert((stored==(committed[0]+committed[1])))"),
            ("BUG_DUPLICATE_INSERT","transaction-recovery.pml","assert((stored==(committed[0]+committed[1])))"),
            ("BUG_EXPIRY_OVERWRITE","task-expiry.pml","assert((terminal_count[0]==1))"),
            ("BUG_HOST_UNCHARGED","host-recovery.pml","assert((charged[attempt]&&reserved[attempt]))"),
            ("BUG_HOST_REPLAY_UNKNOWN","host-recovery.pml","assert((dispatches[attempt]==1))"),
            ("BUG_HOST_STALE_OWNER","host-recovery.pml","assert((!(result_written)||(result_owner==owner)))"),
            ("BUG_HOST_MUTABLE_RESULT","host-recovery.pml","assert((!(result_written)||(result_value==first_result)))"),
        ] {
            let model=if name=="protocol.pml" {transport.clone()} else {fs::read_to_string(root.join("formal").join(name)).unwrap()};
            let replay=format!("spin: {name}:9999, Error: assertion violated\nspin: text of failed assertion: {expression}\n");
            let actual=replay_assertion(name,&model,&replay);
            let expected=expected_assertion(Some(defect));
            assert!(expected.is_some(),"{defect}");
            assert_eq!(actual.as_deref(),expected,"{defect}");
            assert_eq!(replay_assertion(name,"",&replay),None,"{defect}");
            let mut check=json!({"status":"COUNTEREXAMPLE","counterexample_kind":"assertion",
                "replay":{"returncode":0,"timeout":false},"expected_assertion_id":expected,
                "replayed_assertion_id":actual});
            assert!(expected_check_satisfied(&check,true,false),"{defect}");
            check["replayed_assertion_id"]=json!("UNRELATED_ASSERTION");
            assert!(!expected_check_satisfied(&check,true,false),"{defect}");
            let unrelated=format!("spin: {name}:9999, Error: assertion violated\nspin: text of failed assertion: assert(0)\n");
            assert_eq!(replay_assertion(name,&model,&unrelated),None,"{defect}");
        }
        assert_eq!(expected_assertion(Some("UNRECOGNISED_MUTATION")),None);
    }
    #[test]
    fn cycle_counterexample_requires_the_selected_ltl_claim_in_check_and_replay() {
        let checked="pan: ltl formula message_0_progress\npan:1: acceptance cycle (at depth 18)\n";
        let replay="  <<<<<START OF CYCLE>>>>>\n 22:\tproc  - (message_0_progress:1) _spin_nvr.tmp:7 (state 10)\n";
        assert_eq!(replay_cycle(PROPERTIES[0],checked,replay).as_deref(),Some(PROPERTIES[0]));
        assert_eq!(replay_cycle(PROPERTIES[0],checked,&replay.replace("proc  -","proc\t-")).as_deref(),Some(PROPERTIES[0]));
        assert_eq!(replay_cycle(PROPERTIES[1],checked,replay),None);
        assert_eq!(replay_cycle(PROPERTIES[0],checked,&replay.replace("message_0_progress","message_1_progress")),None);
        assert_eq!(replay_cycle(PROPERTIES[0],checked,&replay.replace("<<<<<START OF CYCLE>>>>>","")),None);
        assert_eq!(replay_cycle(PROPERTIES[0],checked,&format!("{replay}spin: protocol.pml:34, Error: assertion violated\n")),None);
    }
    #[test]
    fn replay_binds_the_actual_failed_source_assertion() {
        let source="assert(true); /* FIRST_PROPERTY */\nassert(false); /* EXPECTED_PROPERTY */\n";
        let replay="spin: local.pml:2, Error: assertion violated\nspin: text of failed assertion: assert(0)\n";
        assert_eq!(replay_assertion("local.pml",source,replay).as_deref(),Some("EXPECTED_PROPERTY"));
        assert_eq!(replay_assertion("different.pml",source,replay),None);
        assert_eq!(replay_assertion("local.pml",source,"exit code 0"),None);
        assert_eq!(replay_assertion("local.pml",source,"spin: local.pml:3, Error: assertion violated"),None);
        let conditional="/* REACH_SERVICE_UNKNOWN_RESTART */\n";
        let collapsed="spin: local.pml:62, Error: assertion violated\nspin: text of failed assertion: assert(!(reached_unknown_restart))\n";
        assert_eq!(replay_assertion("local.pml",conditional,collapsed).as_deref(),Some("REACH_SERVICE_UNKNOWN_RESTART"));
        assert_eq!(replay_assertion("local.pml",source,collapsed),None);
    }
    #[test]
    fn shared_trace_generation_checks_each_observation() {
        let trace=json!({"actions":[{"op":"submit","args":[0,1],"expect":{
            "state":["queued","absent"],"revision":[0,0],"cancelled":[false,false],"events":[1,0]}}]});
        let generated=trace_model("/* model */\n",&trace).unwrap();
        assert!(generated.starts_with("#define TRACE_ONLY\n"));
        assert!(generated.contains("submit(0,1)"));
        assert!(generated.contains("assert(state[0] == Queued)"));
        assert!(generated.contains("assert(events[1] == 0)"));
        let mut invalid=trace.clone();invalid["actions"][0]["op"]=json!("unknown");
        assert!(trace_model("",&invalid).is_err());
        invalid=trace.clone();invalid["actions"][0]["expect"]["state"][0]=json!("invalid");
        assert!(trace_model("",&invalid).is_err());
    }
}
