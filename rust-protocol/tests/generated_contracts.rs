//! Reproducible generated boundary and gateway histories with independent oracles.
//! The seed and operation index identify the shortest executed failing prefix.
use mas_protocol_codec::{gateway::Gateway, parse_json, parse_message, store::Store, MAX_BYTES, MAX_DEPTH};
use serde_json::json;
use std::{collections::BTreeMap, path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

const SEEDS: [u64; 4] = [0, 17, 0x5a17, 0xc0dec];
static SERIAL: AtomicUsize = AtomicUsize::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("mas-generated-{}-{}", std::process::id(), SERIAL.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}
struct Generator(u64);
impl Generator {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}
fn wire(payload: &str) -> String {
    format!("message \"m\" run \"r\" from \"a\" to \"b\" version \"1\" kind \"Notice\" payload {payload};")
}

#[test]
fn generated_integer_boundaries_and_lexical_mutations_preserve_exact_values() {
    for seed in SEEDS {
        let mut rng = Generator(seed);
        for (case, digits) in [1, 2, 19, 20, 38, 39, 1023, 1024, 1025].into_iter().enumerate() {
            let magnitude: String = (0..digits).map(|i| {
                let digit = if i == 0 { 1 + rng.next() % 9 } else { rng.next() % 10 };
                char::from(b'0' + digit as u8)
            }).collect();
            for sign in ["", "-"] {
                let integer = format!("{sign}{magnitude}");
                let payload = format!("{{\"x\":{integer}}}");
                let result = parse_message(&wire(&payload), MAX_BYTES, MAX_DEPTH);
                if digits <= 1024 {
                    let message = result.unwrap_or_else(|e| panic!("seed={seed} case={case}: {e:?}"));
                    assert_eq!(message.payload["x"].to_string(), integer, "seed={seed} case={case}");
                    for suffix in [".0", "e0", "E+0", "_"] {
                        let changed = format!("{{\"x\":{integer}{suffix}}}");
                        assert!(parse_json(&changed).is_err(), "seed={seed} case={case}: {changed}");
                    }
                } else {
                    assert_eq!(result.unwrap_err().code, "INTEGER", "seed={seed} case={case}");
                }
                let duplicate = format!("{{\"x\":{integer},\"\\u0078\":0}}");
                assert!(parse_json(&duplicate).is_err(), "seed={seed} case={case}");
            }
        }
    }
}

#[test]
fn generated_utf8_and_nesting_limits_count_encoded_bytes_and_containers() {
    for text in ["a", "雪", "🧠", "\\n", "\\u0000"] {
        let overhead = wire("{\"x\":\"\"}").len();
        let repetitions = (MAX_BYTES - overhead) / text.len();
        let padding = (MAX_BYTES - overhead) % text.len();
        let payload = format!("{{\"x\":\"{}{}\"}}", text.repeat(repetitions), "a".repeat(padding));
        let source = wire(&payload);
        assert_eq!(source.len(), MAX_BYTES);
        assert!(parse_message(&source, MAX_BYTES, MAX_DEPTH).is_ok(), "text={text:?}");
        assert_eq!(parse_message(&(source + " "), MAX_BYTES, MAX_DEPTH).unwrap_err().code, "SIZE");
    }
    for containers in 30..=34 {
        let value = format!("{}0{}", "[".repeat(containers), "]".repeat(containers));
        assert_eq!(parse_json(&value).is_ok(), containers <= MAX_DEPTH, "containers={containers}");
    }
}

#[test]
fn generated_gateway_histories_conserve_call_authority_across_restarts() {
    for seed in SEEDS {
        let temporary = Temporary::new();
        let path = temporary.0.join("state.sqlite");
        let mut store = Store::open(&path).unwrap();
        store.create_run("r", &json!({"agents":["a","b"],"max_calls":9})).unwrap();
        let config = json!({"gateway_id":"generated","max_attempts":100,"credentials":[
            {"credential_id":"a","token":"a1".repeat(32),"run_id":"r","actor":"a","max_attempts":30},
            {"credential_id":"b","token":"b2".repeat(32),"run_id":"r","actor":"b","max_attempts":35}
        ]});
        let mut gateway = Gateway::open(store, &config).unwrap();
        let mut rng = Generator(seed);
        let mut charges = [0usize; 2];
        let mut calls: BTreeMap<String, (usize, bool)> = BTreeMap::new();
        let mut successful = 0;
        for step in 0..128 {
            let mut actor = (rng.next() % 2) as usize;
            let operation = if step < 9 { 4 } else if step < 18 { 5 } else { rng.next() % 7 };
            let call = format!("call-{}", if step < 18 { step % 9 } else { rng.next() as usize % 15 });
            if (9..18).contains(&step) { actor = calls[&call].0; }
            let token = ["a1", "b2"][actor].repeat(32);
            let request = match operation {
                0 => "{".to_owned(),
                1 => json!({"token":"ff".repeat(32),"command":"receive"}).to_string(),
                2 => json!({"token":token,"command":"receive"}).to_string(),
                3 => json!({"token":token,"command":"receive","actor":(["a","b"][1-actor])}).to_string(),
                4 => json!({"token":token,"command":"reserve","call_id":call}).to_string(),
                5 => json!({"token":token,"command":"finish","call_id":call,"evidence":{"observed":true}}).to_string(),
                _ => json!({"token":token,"command":"finish","call_id":call,"evidence":[]}).to_string(),
            };
            // This oracle has no database access: capabilities consume an attempt,
            // unique admitted calls consume authority, and finish never refunds it.
            let expected_error = if step >= 100 { Some("admission_closed") }
                else if operation == 0 { Some("invalid_frame") }
                else if operation == 1 { Some("unauthorised") }
                else if charges[actor] >= [30,35][actor] { Some("credential_admission_closed") }
                else {
                    charges[actor] += 1;
                    match operation {
                        2 => None,
                        4 if !calls.contains_key(&call) && calls.len() < 9 => {
                            calls.insert(call.clone(), (actor, false)); None
                        },
                        5 if calls.get(&call) == Some(&(actor, false)) => {
                            calls.insert(call.clone(), (actor, true)); None
                        },
                        _ => Some("operation_rejected"),
                    }
                };
            let reply = gateway.handle_frame(request.as_bytes());
            assert_eq!(reply.response["ok"], expected_error.is_none(), "seed={seed} step={step} op={operation}: {}", reply.response);
            if let Some(error) = expected_error {
                assert_eq!(reply.response["error"]["code"], error, "seed={seed} step={step}");
            } else { successful += 1; }
            assert_eq!(reply.close, step >= 99, "seed={seed} step={step}");
            // Opening an independent reader checks the actual retained history
            // after every accepted/rejected operation, including mutated frames.
            let state = Store::open(&path).unwrap().inspect("r").unwrap();
            assert_eq!(state["usage"]["calls"], calls.len(), "seed={seed} step={step}");
            assert_eq!(state["calls"].as_array().unwrap().len(), calls.len());
            for record in state["calls"].as_array().unwrap() {
                let (owner, finished) = calls[record["id"].as_str().unwrap()];
                assert_eq!(record["actor"], ["a","b"][owner]);
                assert_eq!(record["state"], if finished {"recorded"} else {"reserved"});
            }
            if step % 7 == 0 {
                drop(gateway);
                let mut reordered = config.clone();
                reordered["credentials"].as_array_mut().unwrap().reverse();
                gateway = Gateway::open(Store::open(&path).unwrap(), &reordered).unwrap();
            }
        }
        assert!(successful >= 18, "seed={seed}: generator must reach reservation and completion");
        assert_eq!(calls.len(), 9, "seed={seed}: generator must exercise exhausted call authority");
    }
}
