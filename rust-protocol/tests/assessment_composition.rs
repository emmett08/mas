//! Shared traces exercise real SQLite/assessment boundaries. Dispatch and
//! workflow-stop decisions are a small explicit test adapter, not a provider
//! implementation or a proof of the separate mas-host journal.
use mas_protocol_codec::{assessment::*, store::Store, Message};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Database(PathBuf);
impl Drop for Database { fn drop(&mut self) { let _=std::fs::remove_file(&self.0); } }

fn message(id: &str, kind: &str, reverse: bool, payload: Value) -> Message {
    Message { message_id:id.into(),run_id:"r".into(),
        sender:if reverse {"reviewer"} else {"coordinator"}.into(),
        recipient:if reverse {"coordinator"} else {"reviewer"}.into(),
        version:"1".into(),kind:kind.into(),payload:payload.as_object().unwrap().clone() }
}

fn fixtures() -> (Candidate, Rubric, Evaluator, Evidence, Assessment) {
    let output=json!({"diagnosis":"configuration"});
    let candidate=Candidate::from_value(&json!({"schema_version":1,"id":"candidate-0",
        "output_sha256":canonical_digest(&output),"output":output,"parent":null,"feedback":[]})).unwrap();
    let rubric=Rubric::from_value(&json!({"schema_version":1,"id":"rubric-0",
        "criteria":[{"id":"diagnosis","measurement":"deterministic","minimum":0,"maximum":1}]})).unwrap();
    let evaluator=Evaluator::from_value(&json!({"id":"reviewer","version":"1","kind":"deterministic"})).unwrap();
    let content=json!({"observed":"configuration"});
    let evidence=Evidence::from_value(&json!({"schema_version":1,"id":"evidence-0","content_sha256":canonical_digest(&content),"content":content})).unwrap();
    let assessment=Assessment::from_value(&json!({"schema_version":1,"id":"assessment-0",
        "candidate":candidate.reference().to_value(),"evaluator":evaluator.to_value(),
        "rubric":rubric.reference().to_value(),"status":"completed","findings":[
            {"criterion_id":"diagnosis","verdict":"pass","score":1,"evidence":[evidence.reference().to_value()],"explanation":"Fixture"}],"error":null})).unwrap();
    (candidate,rubric,evaluator,evidence,assessment)
}

#[test]
fn shared_composed_traces_preserve_native_identity_terminals_and_reservations() {
    let corpus:Value=serde_json::from_str(include_str!("../../formal/assessment-composition-traces.json")).unwrap();
    for trace in corpus["traces"].as_array().unwrap() {
        let db=Database(std::env::temp_dir().join(format!("mas-composed-{}-{}.sqlite",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed))));
        let mut store=Store::open(&db.0).unwrap();
        store.create_run("r",&json!({"agents":["coordinator","reviewer","other"],
            "edges":[["coordinator","reviewer"],["reviewer","coordinator"]],
            "application":"agent-task/1","max_calls":2,"mailbox_capacity":8})).unwrap();
        let (candidate,rubric,evaluator,evidence,assessment)=fixtures();
        let request=message("request-0","Request",false,json!({"task_id":"evaluate-0","operation":"assess","input":candidate.to_value()}));
        let reply=message("assessment-0","Result",true,json!({"task_id":"evaluate-0","reply_to":"request-0","status":"completed","output":assessment.payload()}));
        let mut stopped=false;
        let mut expired=false;
        let mut unknown=false;
        let mut dispatched=false;
        let mut revision_dispatched=false;
        let mut retained=None::<Assessment>;
        for action in trace["actions"].as_array().unwrap() {
            match action.as_str().unwrap() {
                "Commit" | "Resume" => {
                    // Commit can be scheduled after an earlier absent observation.
                    // Discarding the result represents an unobserved response.
                    let old=store.reconcile_message("r","coordinator","request-0").unwrap();
                    let accepted=store.send_checked(&request,"coordinator");
                    if expired && old["state"]=="absent" { assert!(accepted.is_err()); }
                    else {
                        let accepted=accepted.unwrap();
                        assert_eq!(accepted["duplicate"],old["state"]=="present");
                        if old["state"]=="present" { assert_eq!(accepted["sequence"],old["sequence"]); }
                    }
                }
                "Observe" | "ForeignObserve" => {
                    let before=store.observe("r",true).unwrap();
                    let actor=if action=="Observe" {"coordinator"} else {"other"};
                    let observation=store.reconcile_message("r",actor,"request-0").unwrap();
                    if action=="ForeignObserve" { assert_eq!(observation["state"],"absent"); }
                    assert_eq!(before,store.observe("r",true).unwrap());
                }
                "Receive" => {
                    let deliveries=store.receive("r","reviewer",1).unwrap();
                    assert_eq!(deliveries[0]["message"]["message_id"],"request-0");
                }
                "Reserve" => { store.reserve_call("r","reviewer","evaluation-0").unwrap(); }
                "Dispatch" => {
                    assert!(!unknown && !dispatched);
                    let snapshot=store.observe("r",true).unwrap();
                    assert!(snapshot["calls"].as_array().unwrap().iter().any(|c|c["id"]=="evaluation-0" && c["state"]=="reserved"));
                    dispatched=true;
                }
                "Assess" => {
                    assert!(dispatched && !unknown);
                    validate_assessment(&assessment,&candidate,&rubric,std::slice::from_ref(&evidence),&evaluator).unwrap();
                    store.finish_call("r","reviewer","evaluation-0",&json!({"assessment":assessment.to_value()})).unwrap();
                    retained=Some(assessment.clone());
                }
                "Reply" => {
                    let result=store.send_checked(&reply,"reviewer");
                    if stopped || expired { assert!(result.is_err()); } else { result.unwrap(); }
                }
                "Revise" => {
                    let feedback=retained.as_ref().unwrap();
                    let mut revised=candidate.clone();
                    revised.id="candidate-1".into();revised.parent=Some(candidate.reference());
                    revised.feedback=vec![feedback.reference()];
                    validate_candidate_revision(&revised,Some(&candidate),std::slice::from_ref(feedback)).unwrap();
                    let mut wrong=feedback.clone();wrong.candidate.sha256="0".repeat(64);
                    assert!(validate_candidate_revision(&revised,Some(&candidate),&[wrong]).is_err());
                    // Host workflow cancellation is deliberately an adapter
                    // precondition. Store itself enforces expiry and call identity.
                    if !stopped && !expired { store.reserve_call("r","reviewer","revision-0").unwrap(); }
                    if expired { assert!(store.reserve_call("r","reviewer","forbidden-late").is_err()); }
                }
                "RevDispatch" => { assert!(!unknown && !stopped && !expired && !revision_dispatched); revision_dispatched=true; }
                "RevComplete" => {
                    assert!(revision_dispatched && !unknown);
                    store.finish_call("r","reviewer","revision-0",&json!({"candidate":"candidate-1"})).unwrap();
                }
                "Restart" => {
                    drop(store);store=Store::open(&db.0).unwrap();
                    let snapshot=store.observe("r",true).unwrap();
                    for call in snapshot["calls"].as_array().unwrap() {
                        // No existing execution identity can acquire another call.
                        assert!(store.reserve_call("r","reviewer",call["id"].as_str().unwrap()).is_err());
                        if call["state"]=="reserved" { unknown=true; }
                        if call["id"]=="evaluation-0" && call["state"]=="recorded" {
                            retained=Some(Assessment::from_value(&call["evidence"]["assessment"]).unwrap());
                            assert_eq!(retained.as_ref(),Some(&assessment));
                        }
                    }
                }
                "Cancel" => {
                    stopped=true;
                    let cancel=message("cancel-0","Cancel",false,json!({"task_id":"evaluate-0","reply_to":"request-0","reason":"workflow stopped"}));
                    let was_open=store.observe("r",false).unwrap()["tasks"][0]["state"]=="open";
                    let cancellation=store.send_checked(&cancel,"coordinator");
                    if was_open { cancellation.unwrap(); } else { assert!(cancellation.is_err()); }
                }
                "Expire" => {
                    expired=true;
                    Connection::open(&db.0).unwrap().execute("UPDATE runs SET deadline=0 WHERE id='r'",[]).unwrap();
                    store.reconcile("r").unwrap();
                }
                other => panic!("unknown trace action {other}"),
            }
        }
        let snapshot=store.observe("r",true).unwrap();
        let receipt=store.reconcile_message("r","coordinator","request-0").unwrap();
        let calls=snapshot["calls"].as_array().unwrap();
        let terminal=match snapshot["tasks"][0]["state"].as_str().unwrap() {
            "open"=>0,"completed"=>1,"cancelled"=>2,"expired_outcome_unknown"=>3,other=>panic!("unexpected {other}") };
        let actual=json!({"stored":receipt["state"]=="present","received":receipt["received"],"terminal":terminal,
            "reserved":calls.iter().any(|c|c["id"]=="evaluation-0"),"assessment":retained.is_some(),
            "revision":calls.iter().any(|c|c["id"]=="revision-0"),"unknown":unknown});
        assert_eq!(actual,trace["expected"],"trace {}",trace["name"]);
        assert_eq!(snapshot["messages"].as_array().unwrap().iter().filter(|m|m["message_id"]=="request-0").count(),1);
    }
}
