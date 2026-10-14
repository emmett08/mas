//! The protocol grammar is composed from nom parsers; message contents cannot
//! install Rust code or change the envelope. serde_json decodes only an already
//! recognised JSON string/number token and serialises lowered typed values.

pub mod store;
pub mod repository;
#[cfg(feature = "postgres-repository")]
pub mod postgres_repository;
#[cfg(feature = "postgres-repository")]
pub mod shared_host;
#[cfg(feature = "postgres-repository")]
pub mod shared_leadership;
#[cfg(feature = "postgres-repository")]
pub mod shared_backup;
#[cfg(feature = "postgres-repository")]
pub mod shared_jobs;
#[cfg(feature = "postgres-repository")]
pub mod shared_commands;
#[cfg(feature = "postgres-repository")]
pub mod shared_archive;
#[cfg(feature = "postgres-repository")]
pub mod shared_transfers;
#[cfg(feature = "postgres-repository")]
pub mod shared_timing;
#[cfg(feature = "postgres-repository")]
pub mod shared_accounting;
#[cfg(feature = "postgres-repository")]
pub mod shared_tasks;
#[cfg(feature = "postgres-repository")]
pub mod shared_composition;
#[cfg(feature = "postgres-repository")]
pub mod shared_remote;
#[cfg(feature = "postgres-repository")]
pub mod shared_artifacts;
#[cfg(feature = "postgres-repository")]
pub mod shared_control;
mod store_schema;
pub mod tasks;
pub mod examples;
pub mod gateway;
pub mod host_ipc;
pub mod worker_supervisor;

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1, take_while_m_n},
    character::complete::{char, digit0, multispace0, multispace1, one_of},
    combinator::{all_consuming, map, opt, recognize, value as constant},
    error::{ErrorKind, ParseError},
    multi::{many0, separated_list0},
    sequence::{delimited, pair, preceded},
    IResult, Parser,
};
use serde_json::{Map, Number, Value};
use std::str::FromStr;

pub const MAX_BYTES: usize = 65_536;
pub const MAX_DEPTH: usize = 32;
pub const MAX_INTEGER_DIGITS: usize = 1_024;
// JSON API requests spell envelope field names longer than canonical wire text.
// Keep that transport overhead separate from the actual message size bound.
pub const ENCODE_ENVELOPE_ALLOWANCE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    pub code: &'static str,
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl Diagnostic {
    pub fn to_value(&self) -> Value {
        serde_json::json!({"code":self.code, "message":self.message,
                          "line":self.line, "column":self.column})
    }
}

#[derive(Debug, Clone)]
struct RecognitionError<'a> {
    input: &'a str,
    code: &'static str,
    message: String,
}

impl<'a> ParseError<&'a str> for RecognitionError<'a> {
    fn from_error_kind(input: &'a str, kind: ErrorKind) -> Self {
        Self { input, code: "SYNTAX", message: format!("Expected protocol grammar production ({kind:?})") }
    }
    fn append(_input: &'a str, _kind: ErrorKind, other: Self) -> Self { other }
    fn or(self, other: Self) -> Self {
        if self.input.len() < other.input.len() { self } else { other }
    }
}

type ResultAt<'a, T> = IResult<&'a str, T, RecognitionError<'a>>;

fn failure<'a, T>(input: &'a str, code: &'static str, message: impl Into<String>) -> ResultAt<'a, T> {
    Err(nom::Err::Failure(RecognitionError { input, code, message: message.into() }))
}

fn ws<'a, O, P>(parser: P) -> impl Parser<&'a str, Output = O, Error = RecognitionError<'a>>
where P: Parser<&'a str, Output = O, Error = RecognitionError<'a>> {
    delimited(multispace0, parser, multispace0)
}

fn quoted(input: &str) -> ResultAt<'_, String> {
    let escape = recognize(pair(char('\\'), alt((
        recognize(one_of("\"\\/bfnrt")),
        recognize(pair(char('u'), take_while_m_n(4, 4, |c: char| c.is_ascii_hexdigit()))),
    ))));
    let unescaped = take_while1(|c: char| c != '"' && c != '\\' && (c as u32) >= 0x20);
    let (remaining, token) = recognize(delimited(char('"'), many0(alt((unescaped, escape))), char('"'))).parse(input)?;
    match serde_json::from_str::<String>(token) {
        Ok(text) => Ok((remaining, text)),
        Err(_) => failure(input, "UNICODE", "String contains an invalid Unicode escape or unpaired surrogate"),
    }
}

fn integer(input: &str) -> ResultAt<'_, Value> {
    let (remaining, token) = recognize(pair(opt(char('-')), alt((
        recognize(char('0')),
        recognize(pair(one_of("123456789"), digit0)),
    )))).parse(input)?;
    if token.trim_start_matches('-').len() > MAX_INTEGER_DIGITS {
        return failure(input, "INTEGER", format!("Integer exceeds {MAX_INTEGER_DIGITS} decimal digits"));
    }
    // The nom lexeme excludes dots/exponents; arbitrary_precision therefore
    // keeps exact integers without crossing an f64 representation.
    let canonical = if token == "-0" { "0" } else { token };
    match Number::from_str(canonical) {
        Ok(number) => Ok((remaining, Value::Number(number))),
        Err(_) => failure(input, "INTEGER", "Invalid integer token"),
    }
}

fn json_value(input: &str, depth: usize, max_depth: usize) -> ResultAt<'_, Value> {
    ws(alt((
        |i| object(i, depth, max_depth),
        |i| array(i, depth, max_depth),
        map(quoted, Value::String),
        integer,
        constant(Value::Bool(true), tag("true")),
        constant(Value::Bool(false), tag("false")),
        constant(Value::Null, tag("null")),
    ))).parse(input)
}

fn object(input: &str, depth: usize, max_depth: usize) -> ResultAt<'_, Value> {
    if input.starts_with('{') && depth >= max_depth {
        return failure(input, "DEPTH", format!("Message exceeds nesting depth {max_depth}"));
    }
    let member = |i| {
        let (i, key) = ws(quoted).parse(i)?;
        let (i, _) = ws(char(':')).parse(i)?;
        let (i, value) = json_value(i, depth + 1, max_depth)?;
        Ok((i, (key, value)))
    };
    let (remaining, members) = delimited(ws(char('{')), separated_list0(ws(char(',')), member), ws(char('}'))).parse(input)?;
    let mut lowered = Map::new();
    for (key, value) in members {
        if lowered.contains_key(&key) {
            return failure(input, "DUPLICATE", format!("Repeated payload key {key:?}"));
        }
        lowered.insert(key, value);
    }
    Ok((remaining, Value::Object(lowered)))
}

fn array(input: &str, depth: usize, max_depth: usize) -> ResultAt<'_, Value> {
    if input.starts_with('[') && depth >= max_depth {
        return failure(input, "DEPTH", format!("Message exceeds nesting depth {max_depth}"));
    }
    map(delimited(ws(char('[')), separated_list0(ws(char(',')), |i| json_value(i, depth + 1, max_depth)), ws(char(']'))), Value::Array).parse(input)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub message_id: String,
    pub run_id: String,
    pub sender: String,
    pub recipient: String,
    pub version: String,
    pub kind: String,
    pub payload: Map<String, Value>,
}

fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.bytes().all(|c| c.is_ascii_alphanumeric() || b"_.:/-".contains(&c))
}

fn vocabulary(value: &str) -> bool {
    !value.is_empty() && value.len() <= 64
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value.bytes().all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
}

fn charge(budget:&mut usize,size:usize)->Result<(),Diagnostic> {
    *budget=budget.checked_sub(size).ok_or_else(||Diagnostic {code:"SIZE",message:format!("Payload exceeds {MAX_BYTES} bytes"),line:None,column:None})?;
    Ok(())
}

fn validate_value(value: &Value, depth: usize,budget:&mut usize) -> Result<(), Diagnostic> {
    let reject = |code, message| Diagnostic {code,message,line:None,column:None};
    match value {
        Value::Array(values) => {
            if depth >= MAX_DEPTH {return Err(reject("DEPTH",format!("Payload exceeds nesting depth {MAX_DEPTH}")));}
            charge(budget,2)?;
            for value in values {validate_value(value,depth+1,budget)?;}
        },
        Value::Object(values) => {
            if depth >= MAX_DEPTH {return Err(reject("DEPTH",format!("Payload exceeds nesting depth {MAX_DEPTH}")));}
            charge(budget,2)?;
            for (key,value) in values {charge(budget,key.len()+3)?;validate_value(value,depth+1,budget)?;}
        },
        Value::Number(number) => {
            let text=number.as_str();
            let digits=text.trim_start_matches('-');
            if digits.is_empty() || !digits.bytes().all(|b|b.is_ascii_digit()) {
                return Err(reject("TYPE","Payload numeric values must be integers".into()));
            }
            if digits.len()>MAX_INTEGER_DIGITS {
                return Err(reject("INTEGER",format!("Integer exceeds {MAX_INTEGER_DIGITS} decimal digits")));
            }
            charge(budget,text.len())?;
        },
        Value::String(text)=>charge(budget,text.len()+2)?,
        _=>charge(budget,4)?,
    }
    Ok(())
}

impl Message {
    pub fn validate(&self) -> Result<(), Diagnostic> {
        for (name, text) in [("message_id", &self.message_id), ("run_id", &self.run_id),
                              ("sender", &self.sender), ("recipient", &self.recipient), ("version", &self.version)] {
            if !identity(text) { return Err(Diagnostic { code:"IDENTITY", message:format!("{name} must contain 1–128 ASCII identity characters"), line:None, column:None }); }
        }
        if !vocabulary(&self.kind) { return Err(Diagnostic { code:"KIND", message:"kind must contain 1–64 ASCII vocabulary characters".into(), line:None, column:None }); }
        let mut budget=MAX_BYTES;
        charge(&mut budget,2)?;
        for (key,value) in &self.payload {charge(&mut budget,key.len()+3)?;validate_value(value,1,&mut budget)?;}
        Ok(())
    }

    pub fn to_value(&self) -> Value {
        serde_json::json!({"message_id":self.message_id, "run_id":self.run_id,
                          "sender":self.sender, "recipient":self.recipient,
                          "version":self.version, "kind":self.kind, "payload":self.payload})
    }

    fn from_value(value: Value) -> Result<Self, Diagnostic> {
        let error = |message: &str| Diagnostic { code:"ENVELOPE", message:message.into(), line:None, column:None };
        let Value::Object(mut fields) = value else { return Err(error("Envelope must be an object")); };
        let required = ["message_id", "run_id", "sender", "recipient", "version", "kind", "payload"];
        if fields.len() != required.len() || required.iter().any(|key| !fields.contains_key(*key)) {
            return Err(error("Envelope must contain exactly message_id, run_id, sender, recipient, version, kind, payload"));
        }
        let mut text = |key: &str| match fields.remove(key) {
            Some(Value::String(s)) => Ok(s),
            _ => Err(error("Envelope identity/kind fields must be strings")),
        };
        let message_id = text("message_id")?;
        let run_id = text("run_id")?;
        let sender = text("sender")?;
        let recipient = text("recipient")?;
        let version = text("version")?;
        let kind = text("kind")?;
        let Some(Value::Object(payload)) = fields.remove("payload") else { return Err(error("payload must be an object")); };
        let result = Self { message_id, run_id, sender, recipient, version, kind, payload };
        result.validate()?;
        Ok(result)
    }

    pub fn format(&self) -> Result<String, Diagnostic> {
        self.validate()?;
        let quote = |value: &str| serde_json::to_string(value).expect("String serialization cannot fail");
        let formatted=format!("message {} run {} from {} to {} version {} kind {} payload {};",
                quote(&self.message_id), quote(&self.run_id), quote(&self.sender), quote(&self.recipient),
                quote(&self.version), quote(&self.kind), Value::Object(self.payload.clone()));
        check_limits(&formatted,MAX_BYTES,MAX_DEPTH)?;
        Ok(formatted)
    }
}

fn envelope(input: &str, max_depth: usize) -> ResultAt<'_, Message> {
    fn field<'a>(keyword: &'static str) -> impl Parser<&'a str, Output=String, Error=RecognitionError<'a>> {
        preceded(pair(tag(keyword), multispace1), quoted)
    }
    let (i, message_id) = field("message").parse(input)?;
    let (i, run_id) = preceded(multispace1, field("run")).parse(i)?;
    let (i, sender) = preceded(multispace1, field("from")).parse(i)?;
    let (i, recipient) = preceded(multispace1, field("to")).parse(i)?;
    let (i, version) = preceded(multispace1, field("version")).parse(i)?;
    let (i, kind) = preceded(multispace1, field("kind")).parse(i)?;
    let (i, _) = preceded(multispace1, pair(tag("payload"), multispace1)).parse(i)?;
    let (i, payload) = object(i, 0, max_depth)?;
    let (i, _) = ws(char(';')).parse(i)?;
    let Value::Object(payload) = payload else { unreachable!() };
    Ok((i, Message { message_id, run_id, sender, recipient, version, kind, payload }))
}

fn check_limits(source: &str, max_bytes: usize, max_depth: usize) -> Result<(), Diagnostic> {
    if max_bytes == 0 || max_bytes > MAX_BYTES || max_depth == 0 || max_depth > MAX_DEPTH {
        return Err(Diagnostic {code:"LIMIT", message:format!("Limits require 1–{MAX_BYTES} bytes and 1–{MAX_DEPTH} nesting levels"),line:None,column:None});
    }
    if source.len() > max_bytes {
        return Err(Diagnostic {code:"SIZE", message:format!("Message exceeds {max_bytes} UTF-8 bytes"),line:None,column:None});
    }
    Ok(())
}

fn diagnostic(source: &str, error: nom::Err<RecognitionError<'_>>) -> Diagnostic {
    match error {
        nom::Err::Error(error) | nom::Err::Failure(error) => {
            let offset = source.len().saturating_sub(error.input.len());
            let before = &source[..offset];
            Diagnostic {code:error.code,message:error.message,
                        line:Some(before.bytes().filter(|b| *b == b'\n').count()+1),
                        column:Some(before.rsplit('\n').next().unwrap_or("").chars().count())}
        },
        nom::Err::Incomplete(_) => Diagnostic {code:"SYNTAX",message:"Incomplete message".into(),line:None,column:None},
    }
}

pub fn parse_message(source: &str, max_bytes: usize, max_depth: usize) -> Result<Message, Diagnostic> {
    check_limits(source, max_bytes, max_depth)?;
    let (_, message) = all_consuming(delimited(multispace0, |i| envelope(i,max_depth), multispace0))
        .parse(source).map_err(|e| diagnostic(source,e))?;
    message.validate()?;
    Ok(message)
}

pub fn encode_message(source: &str, max_bytes: usize, max_depth: usize) -> Result<String, Diagnostic> {
    check_limits("", max_bytes, max_depth)?;
    if source.len()>max_bytes+ENCODE_ENVELOPE_ALLOWANCE {
        return Err(Diagnostic {code:"SIZE",message:"Encode request exceeds the bounded envelope transport allowance".into(),line:None,column:None});
    }
    // The JSON envelope introduces one extra container around the payload.
    let (_, value) = all_consuming(ws(|i| json_value(i,0,max_depth+1)))
        .parse(source).map_err(|e| diagnostic(source,e))?;
    let message = Message::from_value(value)?;
    let formatted = message.format()?;
    parse_message(&formatted, max_bytes, max_depth)?;
    Ok(formatted)
}

/// Strict configuration/evidence JSON with the same exact integers, duplicate
/// rejection and resource limits as the wire parser. No permissive JSON fallback.
pub fn parse_json(source: &str) -> Result<Value, Diagnostic> {
    check_limits(source, MAX_BYTES, MAX_DEPTH)?;
    let (_, value) = all_consuming(ws(|i| json_value(i, 0, MAX_DEPTH)))
        .parse(source).map_err(|e| diagnostic(source, e))?;
    Ok(value)
}

/// Private host command transport budget. Wire messages retain MAX_BYTES.
#[cfg(feature = "postgres-repository")]
pub const SHARED_COMMAND_BYTES: usize = 1_048_576;
#[cfg(feature = "postgres-repository")]
pub fn parse_shared_command(source: &str) -> Result<Value, Diagnostic> {
    if source.len()>SHARED_COMMAND_BYTES {
        return Err(Diagnostic {code:"SIZE",message:"Shared command exceeds transport budget".into(),line:None,column:None});
    }
    let (_,value)=all_consuming(ws(|i|json_value(i,0,MAX_DEPTH))).parse(source).map_err(|e|diagnostic(source,e))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn wire(payload:&str)->String {format!("message \"m\" run \"r\" from \"a\" to \"b\" version \"1\" kind \"request\" payload {payload};")}
    #[test]
    fn exact_unicode_bigint_and_boolean_types() {
        let message = parse_message(&wire(r#"{"text":"∀x:🧠","n":340282366920938463463374607431768211456,"flag":true}"#),MAX_BYTES,MAX_DEPTH).unwrap();
        assert!(message.payload["n"].is_number());
        assert!(message.payload["flag"].is_boolean());
        assert_eq!(message.payload["n"].to_string(), "340282366920938463463374607431768211456");
        assert_eq!(parse_message(&message.format().unwrap(),MAX_BYTES,MAX_DEPTH).unwrap(),message);
        assert_eq!(encode_message(&message.to_value().to_string(),MAX_BYTES,MAX_DEPTH).unwrap(),message.format().unwrap());
    }
    #[test]
    fn duplicates_malformed_and_trailing_rejected() {
        for payload in [r#"{"x":1,"x":2}"#,r#"{"x":{"n":1,"\u006e":2}}"#] {
            assert_eq!(parse_message(&wire(payload),MAX_BYTES,MAX_DEPTH).unwrap_err().code,"DUPLICATE");
        }
        for payload in [r#"{"x":1.0}"#,r#"{"x":1e3}"#,r#"{"x":01}"#,r#"{"x":NaN}"#,r#"{"x":@1}"#,r#"{"x":1,}"#] {
            assert!(parse_message(&wire(payload),MAX_BYTES,MAX_DEPTH).is_err());
        }
        assert!(parse_message(&(wire("{}")+" garbage"),MAX_BYTES,MAX_DEPTH).is_err());
    }
    #[test]
    fn bounds_prevent_unbounded_recursion() {
        let nested=format!("{{\"x\":{}0{}}}","[".repeat(32),"]".repeat(32));
        assert_eq!(parse_message(&wire(&nested),MAX_BYTES,MAX_DEPTH).unwrap_err().code,"DEPTH");
        assert_eq!(parse_message(&wire("{}"),16,MAX_DEPTH).unwrap_err().code,"SIZE");
        let large=format!("{{\"n\":{}}}","9".repeat(1025));
        assert_eq!(parse_message(&wire(&large),MAX_BYTES,MAX_DEPTH).unwrap_err().code,"INTEGER");
    }
    #[test]
    fn empty_depth_boundary_and_unpaired_surrogate() {
        let payload=format!("{}{}", "{\"x\":".repeat(31), "{}".to_owned()+&"}".repeat(31));
        assert!(parse_message(&wire(&payload),MAX_BYTES,MAX_DEPTH).is_ok());
        assert_eq!(parse_message(&wire(r#"{"text":"\ud800"}"#),MAX_BYTES,MAX_DEPTH).unwrap_err().code,"UNICODE");
    }
    #[test]
    fn direct_library_validate_rejects_noninteger_payload() {
        let mut message=parse_message(&wire("{}"),MAX_BYTES,MAX_DEPTH).unwrap();
        message.payload.insert("fraction".into(),serde_json::json!(1.5));
        assert_eq!(message.validate().unwrap_err().code,"TYPE");
        assert_eq!(message.format().unwrap_err().code,"TYPE");
        message.payload.clear();
        message.payload.insert("huge".into(),Value::String("x".repeat(MAX_BYTES)));
        assert_eq!(message.format().unwrap_err().code,"SIZE");
    }
}
