//! Administrative local adapter. Agent inputs belong at the authenticated gateway.
#[path = "mas/assessment.rs"]
mod assessment;
use mas_protocol_codec::{encode_message, parse_message, MAX_BYTES, MAX_DEPTH};
use mas_protocol_codec::store::Store;
use serde_json::{json, Value};
use std::{env, fs::File, io::{self, Read}, path::PathBuf};

fn read(path: &str) -> Result<String, String> {
    let mut source = String::new();
    if path == "-" { io::stdin().take((MAX_BYTES + 257) as u64).read_to_string(&mut source) }
    else { File::open(path).map_err(|e|e.to_string())?.take((MAX_BYTES + 257) as u64).read_to_string(&mut source) }
        .map_err(|e| e.to_string())?;
    if source.len() > MAX_BYTES + 256 { return Err("input exceeds limit".into()); }
    Ok(source)
}

fn arg<'a>(args: &'a [String], index: usize) -> Result<&'a str, String> {
    args.get(index).map(String::as_str).ok_or_else(|| "missing argument; use mas help".into())
}

fn execute(args: &[String]) -> Result<Value, String> {
    let command = arg(args, 0)?;
    if matches!(command, "help" | "--help" | "-h") {
        return Ok(json!({"usage": [
            "mas init DB RUN TOPOLOGY.json",
            "mas send DB ACTOR MESSAGE.txt",
            "mas receive DB RUN ACTOR [LIMIT]",
            "mas ack DB RUN MESSAGE_ID ACTOR",
            "mas status DB RUN",
            "mas reconcile DB RUN",
            "mas reserve DB RUN ACTOR CALL_ID",
            "mas finish DB RUN ACTOR CALL_ID EVIDENCE.json",
            "mas demo TOPOLOGY.json [OUTPUT.json]",
            "mas parse MESSAGE.txt",
            "mas encode MESSAGE.json",
            "mas assessment COMMAND.json"
        ], "db": "absolute SQLite path", "stdin": "use - as input filename", "identity": "actor is a trusted local host assertion, not authentication"}));
    }
    if command == "parse" {
        if args.len()!=2 { return Err("parse needs one file".into()); }
        return parse_message(&read(arg(args,1)?)?, MAX_BYTES, MAX_DEPTH).map(|m|m.to_value()).map_err(|e|e.message);
    }
    if command == "encode" {
        if args.len()!=2 { return Err("encode needs one file".into()); }
        return encode_message(&read(arg(args,1)?)?, MAX_BYTES, MAX_DEPTH).map(|s|json!({"text":s})).map_err(|e|e.message);
    }
    if command == "assessment" {
        if args.len()!=2 { return Err("assessment needs one file".into()); }
        return assessment::execute(&read(arg(args,1)?)?);
    }
    if command == "demo" {
        if !(2..=3).contains(&args.len()) { return Err("demo needs topology and optional output".into()); }
        let spec = mas_protocol_codec::parse_json(&read(arg(args,1)?)?).map_err(|e|e.message)?;
        let dir = env::temp_dir().join(format!("mas-demo-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|e|e.to_string())?.as_nanos()));
        std::fs::create_dir(&dir).map_err(|e|e.to_string())?;
        let result = mas_protocol_codec::examples::run_example(&spec, &dir.join("state.sqlite"));
        let _ = std::fs::remove_dir_all(&dir);
        let report = result?;
        if let Some(output) = args.get(2) {
            std::fs::write(output, serde_json::to_string_pretty(&report).map_err(|e|e.to_string())? + "\n").map_err(|e|e.to_string())?;
        }
        return Ok(report);
    }
    let arity = match command {"init"|"ack"|"reserve"=> if command=="init" {4} else {5},"send"=>4,"status"|"reconcile"=>3,"finish"=>6,"receive"=>args.len(),_=>return Err("unknown command; use mas help".into())};
    if args.len()!=arity || (command=="receive" && !(4..=5).contains(&args.len())) { return Err("incorrect arguments; use mas help".into()); }
    let path = PathBuf::from(arg(args,1)?);
    if !path.is_absolute() { return Err("DB must be an absolute path".into()); }
    let mut store = Store::open(&path)?;
    match command {
        "init" => {
            let spec = mas_protocol_codec::parse_json(&read(arg(args,3)?)?).map_err(|e|e.message)?;
            for field in ["agents","edges","mailbox_capacity","application"] {
                if spec.get(field).is_none() { return Err(format!("topology missing {field}")); }
            }
            if !spec["edges"].is_array() || spec["application"] != "agent-task/1" { return Err("topology requires explicit edges and agent-task/1".into()); }
            // Metadata is not configuration. A small explicit list prevents typo fallbacks.
            let allowed = ["name","description","agents","edges","mailbox_capacity","application","max_messages","max_bytes","max_calls","wall_seconds"];
            if spec.as_object().ok_or("topology must be an object")?.keys().any(|k|!allowed.contains(&k.as_str())) { return Err("unknown topology field".into()); }
            let mut config = spec;
            config.as_object_mut().unwrap().remove("name");
            config.as_object_mut().unwrap().remove("description");
            store.create_run(arg(args,2)?, &config)
        },
        "send" => { let m = parse_message(&read(arg(args,3)?)?, MAX_BYTES, MAX_DEPTH).map_err(|e|e.message)?; store.send(&m,arg(args,2)?) },
        "receive" => store.receive(arg(args,2)?,arg(args,3)?,args.get(4).map(|s|s.parse::<usize>()).transpose().map_err(|_|"limit must be an integer")?.unwrap_or(10)),
        "ack" => store.ack(arg(args,2)?,arg(args,3)?,arg(args,4)?),
        "status" => store.inspect(arg(args,2)?),
        "reconcile" => store.reconcile(arg(args,2)?),
        "reserve" => store.reserve_call(arg(args,2)?,arg(args,3)?,arg(args,4)?),
        "finish" => store.finish_call(arg(args,2)?,arg(args,3)?,arg(args,4)?, &mas_protocol_codec::parse_json(&read(arg(args,5)?)?).map_err(|e|e.message)?),
        _ => unreachable!()
    }
}

fn main() {
    match execute(&env::args().skip(1).collect::<Vec<_>>()) {
        Ok(value) => println!("{}",json!({"ok":true,"result":value})),
        Err(message) => { println!("{}",json!({"ok":false,"error":{"code":"operation_rejected","message":message}})); std::process::exit(2); }
    }
}
