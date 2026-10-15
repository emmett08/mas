//! Whole-operation durable protocol repository contract.
//!
//! Implementations own the transaction containing each state transition and its
//! quota/message/task changes. Callers receive no unchecked SQL/transaction port.
//! A failed or lost acknowledgement never authorises a second external effect.
use crate::{
    store::{RunMembership, Store},
    Message,
};
use serde_json::Value;

pub type RepositoryResult<T> = Result<T, String>;

pub trait ProtocolRepository {
    fn register_admission(
        &mut self,
        scope: &str,
        fingerprint: &str,
        limit: i64,
    ) -> RepositoryResult<()>;
    fn reserve_admission(&mut self, scope: &str) -> RepositoryResult<i64>;
    fn reconcile(&self, id: &str) -> RepositoryResult<Value>;
    fn run_membership(&self, id: &str) -> RepositoryResult<RunMembership>;
    fn create_run(&mut self, id: &str, configuration: &Value) -> RepositoryResult<Value>;
    fn send(&mut self, message: &Message, actor: &str) -> RepositoryResult<Value>;
    fn receive(&mut self, id: &str, actor: &str, limit: usize) -> RepositoryResult<Value>;
    fn ack(&mut self, id: &str, message_id: &str, actor: &str) -> RepositoryResult<Value>;
    fn reserve_call(&mut self, id: &str, actor: &str, call_id: &str) -> RepositoryResult<Value>;
    fn finish_call(
        &mut self,
        id: &str,
        actor: &str,
        call_id: &str,
        evidence: &Value,
    ) -> RepositoryResult<Value>;
    fn inspect(&self, id: &str) -> RepositoryResult<Value>;
}

impl ProtocolRepository for Store {
    fn register_admission(
        &mut self,
        scope: &str,
        fingerprint: &str,
        limit: i64,
    ) -> RepositoryResult<()> {
        Store::register_admission(self, scope, fingerprint, limit)
    }
    fn reserve_admission(&mut self, scope: &str) -> RepositoryResult<i64> {
        Store::reserve_admission(self, scope)
    }
    fn reconcile(&self, id: &str) -> RepositoryResult<Value> {
        Store::reconcile(self, id)
    }
    fn run_membership(&self, id: &str) -> RepositoryResult<RunMembership> {
        Store::run_membership(self, id)
    }
    fn create_run(&mut self, id: &str, configuration: &Value) -> RepositoryResult<Value> {
        Store::create_run(self, id, configuration)
    }
    fn send(&mut self, message: &Message, actor: &str) -> RepositoryResult<Value> {
        Store::send(self, message, actor)
    }
    fn receive(&mut self, id: &str, actor: &str, limit: usize) -> RepositoryResult<Value> {
        Store::receive(self, id, actor, limit)
    }
    fn ack(&mut self, id: &str, message_id: &str, actor: &str) -> RepositoryResult<Value> {
        Store::ack(self, id, message_id, actor)
    }
    fn reserve_call(&mut self, id: &str, actor: &str, call_id: &str) -> RepositoryResult<Value> {
        Store::reserve_call(self, id, actor, call_id)
    }
    fn finish_call(
        &mut self,
        id: &str,
        actor: &str,
        call_id: &str,
        evidence: &Value,
    ) -> RepositoryResult<Value> {
        Store::finish_call(self, id, actor, call_id, evidence)
    }
    fn inspect(&self, id: &str) -> RepositoryResult<Value> {
        Store::inspect(self, id)
    }
}
