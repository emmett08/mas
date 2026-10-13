//! Deterministic fixtures over the durable Rust bus; no model or human is invoked.
use crate::{parse_message, Message, MAX_BYTES, MAX_DEPTH};
use crate::store::Store;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

struct Exchange {
    store: Store,
    database: PathBuf,
    run: String,
    events: Vec<Value>,
    index: usize,
}

impl Exchange {
    fn enqueue(&mut self, message: &Message) -> Result<(), String> {
        let wire = message.format().map_err(|e| e.message)?;
        if parse_message(&wire, MAX_BYTES, MAX_DEPTH).map_err(|e| e.message)? != *message {
            return Err("wire round-trip mismatch".into());
        }
        let sent = self.store.send(message, &message.sender)?;
        let again = self.store.send(message, &message.sender)?;
        if again["duplicate"] != true || sent["sequence"] != again["sequence"] {
            return Err("identical resend was not idempotent".into());
        }
        Ok(())
    }

    fn consume(&mut self, expected: &Message) -> Result<Value, String> {
        let first = self.store.receive(&self.run, &expected.recipient, 1)?;
        if first[0]["message"] != expected.to_value() {
            return Err("unexpected FIFO delivery or envelope".into());
        }
        if expected.kind == "Request" && first[0]["actionable"] != true {
            return Err("request is closed; fixture must not start work".into());
        }
        // Reopen after delivery without ack to exercise durable recovery.
        self.store = Store::open(&self.database)?;
        let second = self.store.receive(&self.run, &expected.recipient, 1)?;
        if second[0]["message"] != expected.to_value() || second[0]["redelivered"] != true {
            return Err("restart did not redeliver the same unacknowledged message".into());
        }
        if expected.kind == "Request" && second[0]["actionable"] != true {
            return Err("request closed during restart; fixture must not start work".into());
        }
        self.store.ack(&self.run, &expected.message_id, &expected.recipient)?;
        self.events.push(json!({"wire":expected.format().map_err(|e|e.message)?,
            "message":second[0]["message"],"identical_resend":true,
            "restart_redelivery":true,"acknowledged":true}));
        Ok(second[0]["message"]["payload"].clone())
    }

    fn deliver(&mut self, message: &Message) -> Result<Value, String> {
        self.enqueue(message)?;
        self.consume(message)
    }

    fn pending(&mut self, sender: &str, recipient: &str, operation: &str, input: Value) -> Result<Message, String> {
        self.index += 1;
        let request = Message {
            message_id: format!("q{}",self.index), run_id:self.run.clone(),
            sender:sender.into(), recipient:recipient.into(), version:"1".into(), kind:"Request".into(),
            payload:json!({"task_id":format!("t{}",self.index),"operation":operation,"input":input}).as_object().unwrap().clone(),
        };
        self.enqueue(&request)?;
        Ok(request)
    }

    fn reply(&self, request: &Message, output: Value) -> Message {
        // Derive correlation from this request, never from the mutable sequence counter.
        Message {message_id:format!("r{}",request.message_id.trim_start_matches('q')),
            run_id:self.run.clone(),sender:request.recipient.clone(),recipient:request.sender.clone(),
            version:"1".into(),kind:"Result".into(),
            payload:json!({"task_id":request.payload["task_id"],"reply_to":request.message_id,
                "status":if output["status"]=="unresolved" {"unresolved"} else {"completed"},"output":output}).as_object().unwrap().clone()}
    }

    fn finish(&mut self, request: &Message, output: Value) -> Result<Value, String> {
        let reply = self.reply(request, output);
        Ok(self.deliver(&reply)?["output"].clone())
    }

    fn request<F>(&mut self, sender:&str, recipient:&str, operation:&str, input:Value, compute:F) -> Result<Value,String>
    where F:FnOnce(&Value)->Value {
        let request = self.pending(sender,recipient,operation,input)?;
        let received = self.consume(&request)?;
        self.finish(&request,compute(&received["input"]))
    }
}

fn sum(input:&Value)->i64 {
    input["values"].as_array().unwrap().iter().map(|v|v.as_i64().unwrap()).sum()
}

fn evaluator(ex:&mut Exchange, start:i64, rounds:usize, repair:bool)->Result<Value,String> {
    let mut candidate=start;
    for round in 1..=rounds {
        let response=ex.request("generator","evaluator","evaluate",json!({"candidate":candidate,"rounds_remaining":rounds-round}),|input| {
            let n=input["candidate"].as_i64().unwrap();
            let exhausted=n%2!=0 && input["rounds_remaining"]==0;
            json!({"accepted":n%2==0,"suggestion":n+1,
                "status":if exhausted {"unresolved"} else {"completed"}})
        })?;
        if response["accepted"]==true {
            return Ok(json!({"status":"completed","candidate":candidate,"iterations":round,"criterion":"even integer"}));
        }
        if repair {candidate=response["suggestion"].as_i64().unwrap();}
    }
    Ok(json!({"status":"unresolved","candidate":candidate,"iterations":rounds,"reason":"evaluation budget exhausted"}))
}

fn prompt_chain(ex:&mut Exchange,text:&str)->Result<Value,String> {
    let drafted=ex.request("coordinator","drafter","normalise",json!({"text":text}),|input| {
        let text=input["text"].as_str().unwrap().trim().to_uppercase();
        json!({"status":if text.is_empty(){"unresolved"}else{"completed"},"text":text})
    })?;
    // The gate is a deterministic application predicate over delivered output.
    if drafted["text"].as_str().unwrap().is_empty() {
        return Ok(json!({"status":"unresolved","gate":"rejected empty draft","second_stage_dispatched":false}));
    }
    let checked=ex.request("coordinator","reviewer","check-draft",drafted,|input| {
        json!({"accepted":!input["text"].as_str().unwrap().is_empty(),"text":input["text"]})
    })?;
    Ok(json!({"status":"completed","gate":"passed","second_stage_dispatched":true,"output":checked}))
}

fn vote(ex:&mut Exchange, ballots:&[&str])->Result<Value,String> {
    let mut requests=Vec::new();
    for (i,ballot) in ballots.iter().enumerate() {
        requests.push(ex.pending("coordinator",&format!("voter-{}",i+1),"vote",json!({"fixture_vote":ballot}))?);
    }
    let mut received=Vec::new();
    for request in &requests {
        let input=ex.consume(request)?;
        received.push((request, json!({"vote":input["input"]["fixture_vote"]})));
    }
    let mut replies=Vec::new();
    // Completion order differs from dispatch order. Correlation uses reply_to.
    for (request,output) in received.into_iter().rev() {
        let reply=ex.reply(request,output);
        ex.enqueue(&reply)?;
        replies.push(reply);
    }
    let mut counts=std::collections::BTreeMap::<String,usize>::new();
    let mut correlations=Vec::new();
    for reply in replies {
        let delivered=ex.consume(&reply)?;
        let ballot=delivered["output"]["vote"].as_str().unwrap();
        *counts.entry(ballot.into()).or_default()+=1;
        correlations.push(delivered["reply_to"].clone());
    }
    let winner=counts.iter().find(|(label,count)|label.as_str()!="abstain"&&**count>=2).map(|(label,_)|label.clone());
    Ok(json!({"status":if winner.is_some(){"completed"}else{"unresolved"},"selected":winner,
        "counts":counts,"required_votes":2,"electorate":3,"reply_order":correlations,
        "policy":"At least two matching votes; otherwise unresolved; abstention cannot win.",
        "claim":"Agreement under this fixture rule does not establish truth."}))
}

/// Execute a fresh deterministic workflow. Existing run IDs are deliberately rejected.
pub fn run_example(spec:&Value,database:&Path)->Result<Value,String> {
    let fields=spec.as_object().ok_or("topology must be an object")?;
    let allowed=["name","description","agents","edges","mailbox_capacity","application",
        "max_messages","max_bytes","max_calls","wall_seconds","input"];
    if fields.keys().any(|key|!allowed.contains(&key.as_str())) {
        return Err("unknown topology field".into());
    }
    let name=spec["name"].as_str().ok_or("example needs a name")?;
    if fields.contains_key("input") && name!="orchestrator-workers" {
        return Err("input is supported only by the orchestrator-workers fixture".into());
    }
    for field in ["agents","edges","mailbox_capacity","application"] {
        if spec.get(field).is_none() {return Err(format!("topology missing {field}"));}
    }
    if !spec["edges"].is_array() || spec["application"]!="agent-task/1" {
        return Err("topology requires explicit edges and agent-task/1".into());
    }
    let mut config=spec.clone();
    for metadata in ["name","description","input"] {config.as_object_mut().unwrap().remove(metadata);}
    // Caller-supplied bounds are enforced; defaults only fill absent budgets.
    if config.get("max_messages").is_none() {config["max_messages"]=json!(200);}
    if config.get("max_bytes").is_none() {config["max_bytes"]=json!(200000);}
    let mut store=Store::open(database)?;
    store.create_run(name,&config)?;
    let state=store.inspect(name)?;
    if state["messages"].as_array().is_some_and(|messages|!messages.is_empty()) {
        return Err("example requires a fresh database or unused run ID".into());
    }
    let mut ex=Exchange{store,database:database.into(),run:name.into(),events:vec![],index:0};
    let result=match name {
        "star"=>{
            let candidate=ex.request("hub","proposer","sum",json!({"values":[2,3]}),|x|json!({"candidate":sum(x)}))?;
            ex.request("hub","checker","check",json!({"candidate":candidate["candidate"],"values":[2,3]}),|x|json!({"accepted":x["candidate"]==sum(x)}))?
        },
        "mesh"=>{
            let candidate=ex.request("alice","bob","sum",json!({"values":[2,3]}),|x|json!({"candidate":sum(x)}))?;
            let checked=ex.request("bob","checker","check",json!({"candidate":candidate["candidate"],"values":[2,3]}),|x|json!({"accepted":x["candidate"]==sum(x)}))?;
            ex.request("checker","alice","report",checked,|x|json!({"recorded":x["accepted"]}))?
        },
        "tree"=>{
            let delegation=ex.pending("root","lead","delegate",json!({"values":[2,3]}))?;
            let delivered=ex.consume(&delegation)?;
            let candidate=ex.request("lead","worker","sum",delivered["input"].clone(),|x|json!({"candidate":sum(x),"values":x["values"]}))?;
            let checked=ex.request("lead","checker","check",candidate,|x|json!({"accepted":x["candidate"]==sum(x)}))?;
            ex.finish(&delegation,checked)?
        },
        "evaluator-optimiser"=>json!({"success":evaluator(&mut ex,3,3,true)?,"exhausted":evaluator(&mut ex,3,3,false)?}),
        "human-review"=>{
            let mut branches=Vec::new();
            for action in ["record-example","unapproved-action"] {
                let decision=ex.request("agent","human","review",json!({"action":action}),|x|json!({"approved":x["action"]=="record-example","action":x["action"],
                    "status":if x["action"]=="record-example" {"completed"} else {"unresolved"}}))?;
                let output=if decision["approved"]==true {
                    ex.request("human","executor","execute-fixture",decision,|x|json!({"recorded":x["action"],"external_effects":false}))?
                }else{json!({"status":"unresolved","reason":"simulated reviewer rejected action","executor_dispatched":false})};
                branches.push(output);
            }
            json!({"branches":branches,"reviewer":"deterministic simulation; no human approval obtained"})
        },
        "orchestrator-workers"=>{
            let documents=spec.get("input").and_then(|v|v.get("documents")).cloned().unwrap_or_else(||json!([
                {"id":"one","text":"alpha beta","required_operations":["word-count"]},
                {"id":"two","text":"Gamma Delta","required_operations":["uppercase","word-count"]}]));
            let mut plan=Vec::new();
            let mut outputs=Vec::new();
            for document in documents.as_array().ok_or("documents must be an array")? {
                let operations=document["required_operations"].as_array().ok_or("document needs required_operations")?;
                let text=document["text"].as_str().ok_or("document needs text")?;
                for operation in operations {
                    let operation=operation.as_str().ok_or("operation must be text")?;
                    if !matches!(operation,"word-count"|"uppercase") {return Err("unsupported document operation".into());}
                    let worker=if operation=="word-count"{"worker-a"}else{"worker-b"};
                    plan.push(json!({"document":document["id"],"operation":operation,"worker":worker}));
                    outputs.push(ex.request("orchestrator",worker,operation,json!({"document":document["id"],"text":text,"operation":operation}),|x|{
                        let text=x["text"].as_str().unwrap();
                        let output=if x["operation"]=="word-count" {json!(text.split_whitespace().count())}else{json!(text.to_uppercase())};
                        json!({"document":x["document"],"operation":x["operation"],"value":output})
                    })?);
                }
            }
            json!({"plan":plan,"outputs":outputs,"worker_pool":"fixed and pre-registered","planning":"required operations derived from each input document"})
        },
        "sequential-crew"|"swarm-handoff"=>{
            let agents:Vec<&str>=spec["agents"].as_array().ok_or("agents must be an array")?.iter().map(|a|a.as_str().ok_or("agent must be text")).collect::<Result<_,_>>()?;
            let mut context=json!({"context":["source"]});
            let count=if name=="swarm-handoff"{agents.len()}else{agents.len()-1};
            for i in 0..count {
                let recipient=agents[(i+1)%agents.len()];
                context=ex.request(agents[i],recipient,"handoff-context",context,|x|{
                    let mut history=x["context"].as_array().unwrap().clone();history.push(json!(recipient));
                    json!({"context":history})
                })?;
            }
            json!({"output":context,"control":"Scripted sequential execution; transport does not guarantee global exclusive control."})
        },
        "routing"=>{
            let mut outputs=Vec::new();
            for input in [json!({"category":"arithmetic","values":[2,3]}),json!({"category":"text","text":"hello"}),json!({"category":"unknown"})] {
                let category=input["category"].as_str().unwrap();
                let output=match category {
                    "arithmetic"=>ex.request("router","calculator","sum",input,|x|json!({"sum":sum(x)}))?,
                    "text"=>ex.request("router","writer","uppercase",input,|x|json!({"text":x["text"].as_str().unwrap().to_uppercase()}))?,
                    _=>json!({"status":"unresolved","reason":"unknown route","dispatched":false}),
                };
                outputs.push(output);
            }
            json!({"routes":outputs})
        },
        "parallel-sectioning"=>{
            let first=ex.pending("coordinator","worker-a","partial-sum",json!({"values":[1,2]}))?;
            let second=ex.pending("coordinator","worker-b","partial-sum",json!({"values":[3,4]}))?;
            // Both requests exist before either worker consumes one.
            let pending=ex.store.inspect(name)?;
            let first_input=ex.consume(&first)?;
            let second_input=ex.consume(&second)?;
            let replies=[ex.reply(&second,json!({"partial":sum(&second_input["input"])})),ex.reply(&first,json!({"partial":sum(&first_input["input"])}))];
            for reply in &replies {ex.enqueue(reply)?;}
            let mut partials=serde_json::Map::new();
            for reply in &replies {
                let delivered=ex.consume(reply)?;
                partials.insert(delivered["reply_to"].as_str().unwrap().into(),delivered["output"]["partial"].clone());
            }
            let total:i64=partials.values().map(|x|x.as_i64().unwrap()).sum();
            json!({"sum":total,"partials_by_request":partials,"reply_order":[second.message_id,first.message_id],
                "dispatched_before_consumption":pending["messages"].as_array().map(|x|x.len()).unwrap_or(0),
                "execution":"Scripted interleaving of independently dispatched work; no concurrent model execution."})
        },
        "parallel-voting"=>json!({"majority":vote(&mut ex,&["accept","accept","reject"])? ,
            "no_quorum":vote(&mut ex,&["accept","reject","abstain"])?}),
        "prompt-chaining"=>json!({"passed":prompt_chain(&mut ex,"  example draft  ")?,"rejected":prompt_chain(&mut ex,"  ")?}),
        _=>return Err(format!("unknown example {name}")),
    };
    let state=ex.store.inspect(name)?;
    if !state["messages"].as_array().ok_or("missing messages")?.iter().all(|message|message["acked"]==true) {
        return Err("example left unacknowledged messages".into());
    }
    Ok(json!({"pattern":name,"mode":"deterministic-fixture","result":result,"events":ex.events,
        "usage":state["usage"],"all_acknowledged":true,
        "claim":"Transport and scripted workflow only; no live model or human execution."}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize,Ordering};
    static SEQ:AtomicUsize=AtomicUsize::new(0);
    fn execute(name:&str, input:Option<Value>)->Value {
        let root=Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples");
        let mut spec:Value=serde_json::from_str(&std::fs::read_to_string(root.join(format!("{name}.json"))).unwrap()).unwrap();
        if let Some(input)=input {spec["input"]=input;}
        let path=std::env::temp_dir().join(format!("mas-example-{}-{}.sqlite",std::process::id(),SEQ.fetch_add(1,Ordering::Relaxed)));
        let result=run_example(&spec,&path).unwrap();
        let _=std::fs::remove_file(path);
        result
    }
    #[test]
    fn malformed_demo_configuration_is_rejected() {
        let original:Value=serde_json::from_str(include_str!("../../examples/star.json")).unwrap();
        let path=std::env::temp_dir().join(format!("mas-invalid-example-{}-{}.sqlite",std::process::id(),SEQ.fetch_add(1,Ordering::Relaxed)));
        for field in ["agents","edges","mailbox_capacity","application"] {
            let mut missing=original.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(run_example(&missing,&path).is_err(),"missing {field}");
        }
        for (field,value) in [("edges",Value::Null),("agents",json!("hub")),
            ("mailbox_capacity",json!(0)),("application",Value::Null),
            ("application",json!("other/1")),("mailbox_capcity",json!(2)),
            ("input",json!({})),("max_messages",json!(0))] {
            let mut malformed=original.clone();
            malformed[field]=value;
            assert!(run_example(&malformed,&path).is_err(),"malformed {field}");
        }
        let mut bounded=original;
        bounded["max_messages"]=json!(1);
        assert!(run_example(&bounded,&path).is_err(),"caller message budget must be enforced");
        let _=std::fs::remove_file(path);
    }
    #[test]
    fn all_patterns_recover_and_ack_wire_messages() {
        for name in ["star","mesh","tree","evaluator-optimiser","human-review","orchestrator-workers","sequential-crew","swarm-handoff","routing","parallel-sectioning","parallel-voting","prompt-chaining"] {
            let output=execute(name,None);
            assert_eq!(output["all_acknowledged"],true,"{name}");
            for event in output["events"].as_array().unwrap() {
                assert_eq!(event["restart_redelivery"],true);
                assert_eq!(event["identical_resend"],true);
                assert_eq!(parse_message(event["wire"].as_str().unwrap(),MAX_BYTES,MAX_DEPTH).unwrap().to_value(),event["message"]);
            }
        }
    }
    #[test]
    fn nested_tree_reply_retains_parent_correlation() {
        let output=execute("tree",None);
        let events=output["events"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["message"]["payload"]["reply_to"],"q1");
        assert_eq!(events.last().unwrap()["message"]["message_id"],"r1");
        assert_eq!(output["result"]["accepted"],true);
    }
    #[test]
    fn parallel_correlation_and_application_decisions_are_explicit() {
        let sections=execute("parallel-sectioning",None);
        assert_eq!(sections["result"]["sum"],10);
        assert_eq!(sections["result"]["dispatched_before_consumption"],2);
        assert_eq!(sections["result"]["reply_order"],json!(["q2","q1"]));
        let votes=execute("parallel-voting",None);
        assert_eq!(votes["result"]["majority"]["selected"],"accept");
        assert_eq!(votes["result"]["no_quorum"]["status"],"unresolved");
        let chain=execute("prompt-chaining",None);
        assert_eq!(chain["result"]["rejected"]["second_stage_dispatched"],false);
        let evaluation=execute("evaluator-optimiser",None);
        assert_eq!(evaluation["result"]["success"]["iterations"],2);
        assert_eq!(evaluation["result"]["exhausted"]["status"],"unresolved");
        assert_eq!(evaluation["events"].as_array().unwrap().last().unwrap()["message"]["payload"]["status"],"unresolved");
        assert_eq!(chain["events"].as_array().unwrap().last().unwrap()["message"]["payload"]["status"],"unresolved");
        let human=execute("human-review",None);
        assert_eq!(human["events"].as_array().unwrap().last().unwrap()["message"]["payload"]["status"],"unresolved");
        assert_eq!(human["result"]["branches"][1]["executor_dispatched"],false);
    }
    #[test]
    fn orchestrator_plan_changes_with_input_requirements() {
        let default=execute("orchestrator-workers",None);
        let changed=execute("orchestrator-workers",Some(json!({"documents":[{"id":"only","text":"one two three","required_operations":["word-count"]}]})));
        assert_eq!(default["result"]["plan"].as_array().unwrap().len(),3);
        assert_eq!(changed["result"]["plan"].as_array().unwrap().len(),1);
        assert_eq!(changed["result"]["outputs"][0]["value"],3);
    }
}
