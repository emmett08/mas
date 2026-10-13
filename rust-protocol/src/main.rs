use mas_protocol_codec::{encode_message, parse_message, Diagnostic, ENCODE_ENVELOPE_ALLOWANCE, MAX_BYTES, MAX_DEPTH};
use std::io::{self, Read};

fn main() {
    let mut args=std::env::args().skip(1);
    let operation=args.next().unwrap_or_default();
    let mut max_bytes=MAX_BYTES;
    let mut max_depth=MAX_DEPTH;
    let mut valid=true;
    while let Some(name)=args.next() {
        let value=args.next().and_then(|text|text.parse::<usize>().ok());
        match (name.as_str(),value) {
            ("--max-bytes",Some(n))=>max_bytes=n,
            ("--max-depth",Some(n))=>max_depth=n,
            _=>valid=false,
        }
    }
    let result=if !valid || !matches!(operation.as_str(),"parse"|"encode") {
        Err(Diagnostic {code:"COMMAND",message:"Use parse|encode [--max-bytes N] [--max-depth N] with UTF-8 stdin".into(),line:None,column:None})
    } else {
        let mut source=String::new();
        let transport_limit=MAX_BYTES+if operation=="encode" {ENCODE_ENVELOPE_ALLOWANCE} else {0};
        match io::stdin().take((transport_limit+1) as u64).read_to_string(&mut source) {
            Err(_)=>Err(Diagnostic {code:"UNICODE",message:"stdin must be valid UTF-8".into(),line:None,column:None}),
            Ok(_)=>if operation=="parse" {
                parse_message(&source,max_bytes,max_depth).map(|message|serde_json::json!({"ok":true,"message":message.to_value()}))
            } else {
                encode_message(&source,max_bytes,max_depth).map(|text|serde_json::json!({"ok":true,"text":text}))
            },
        }
    };
    match result {
        Ok(value)=>println!("{value}"),
        Err(error)=>{
            println!("{}",serde_json::json!({"ok":false,"error":error.to_value()}));
            std::process::exit(2);
        }
    }
}
