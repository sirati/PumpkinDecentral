use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::text::TextComponent;
use std::sync::Arc;

use crate::argument_types::entity_anchor::EntityAnchor;
use crate::errors::command_syntax_error::CommandSyntaxError;

pub trait ReturnValueCallable: Send + Sync {
    fn call(&self, value: ReturnValue);

    fn call_deferred(&self, _token: crate::deferred::DeferredCommandToken, value: ReturnValue) {
        self.call(value);
    }

    fn complete_deferred_branch(&self, _token: crate::deferred::DeferredCommandToken) {}

    fn reserve_deferred_terminal(&self, _token: crate::deferred::DeferredCommandToken) -> Result<Option<Box<dyn crate::deferred::DeferredCompletionTerminal>>, crate::deferred::DeferredCompletionUnavailable> {
        Ok(None)
    }

    fn deferred_completion_readiness(&self, _token: crate::deferred::DeferredCommandToken) -> crate::deferred::CompletionReadiness {
        crate::deferred::CompletionReadiness::Ready
    }
}

pub type ReturnValueCallback = Arc<dyn ReturnValueCallable>;

#[derive(Clone, Default)]
pub struct ResultValueTaker(pub Vec<ReturnValueCallback>);

impl ResultValueTaker {
    #[must_use]
    pub fn merge(taker_1: &Self, taker_2: &Self) -> Self {
        let mut takers = Vec::with_capacity(taker_1.0.len() + taker_2.0.len());
        for taker in &taker_1.0 {
            takers.push(taker.clone());
        }
        for taker in &taker_2.0 {
            takers.push(taker.clone());
        }
        Self(takers)
    }

    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn call(&self, return_value: ReturnValue) {
        for callback in &self.0 {
            callback.call(return_value);
        }
    }

    pub fn call_deferred(&self, token: crate::deferred::DeferredCommandToken, return_value: ReturnValue) {
        for callback in &self.0 {
            callback.call_deferred(token, return_value);
        }
    }

    pub fn complete_deferred_branch(&self, token: crate::deferred::DeferredCommandToken) {
        for callback in &self.0 {
            callback.complete_deferred_branch(token);
        }
    }

    pub fn reserve_deferred_terminal(&self, token: crate::deferred::DeferredCommandToken) -> Result<Option<Box<dyn crate::deferred::DeferredCompletionTerminal>>, crate::deferred::DeferredCompletionUnavailable> {
        let mut terminals = Vec::new();
        for callback in &self.0 {
            if let Some(terminal) = callback.reserve_deferred_terminal(token)? { terminals.push(terminal); }
        }
        Ok((!terminals.is_empty()).then(|| Box::new(crate::deferred::DeferredTerminalGroup(terminals)) as Box<dyn crate::deferred::DeferredCompletionTerminal>))
    }

    pub fn deferred_completion_readiness(&self, token: crate::deferred::DeferredCommandToken) -> crate::deferred::CompletionReadiness {
        use crate::deferred::CompletionReadiness;
        let mut readiness = CompletionReadiness::Ready;
        for callback in &self.0 {
            match callback.deferred_completion_readiness(token) {
                CompletionReadiness::Denied => return CompletionReadiness::Denied,
                CompletionReadiness::Pending => readiness = CompletionReadiness::Pending,
                CompletionReadiness::Ready => {},
            }
        }
        readiness
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ReturnValue {
    Success(i32),
    Failure,
}

impl ReturnValue {
    #[must_use]
    pub const fn success_value(self) -> bool {
        match self {
            Self::Success(_) => true,
            Self::Failure => false,
        }
    }

    #[must_use]
    pub const fn result_value(self) -> i32 {
        match self {
            Self::Success(value) => value,
            Self::Failure => 0,
        }
    }
}

/// Abstract command source trait for command dispatch and execution.
pub trait CommandSource: Clone + Send + Sync + 'static {
    /// Sends a feedback message to the command sender.
    fn send_message(&self, message: TextComponent);

    /// Sends an error message to the command sender.
    fn send_error(&self, error: TextComponent) {
        self.send_message(error);
    }

    /// Invokes callbacks for the command's return value.
    fn call_result(&self, _result: ReturnValue) {}

    fn call_deferred_result(&self, _token: crate::deferred::DeferredCommandToken, result: ReturnValue) {
        self.call_result(result);
    }

    fn complete_deferred_branch(&self, _token: crate::deferred::DeferredCommandToken) {}

    fn reserve_deferred_terminal(&self, _token: crate::deferred::DeferredCommandToken) -> Result<Option<Box<dyn crate::deferred::DeferredCompletionTerminal>>, crate::deferred::DeferredCompletionUnavailable> {
        Ok(None)
    }

    fn deferred_completion_readiness(&self, _token: crate::deferred::DeferredCommandToken) -> crate::deferred::CompletionReadiness {
        crate::deferred::CompletionReadiness::Ready
    }

    /// Checks if this command source has the specified permission.
    fn has_permission(&self, _permission: &str) -> bool {
        true
    }

    /// Returns the position of this command source in the world.
    fn position(&self) -> Vector3<f64> {
        Vector3::default()
    }

    /// Returns the rotation (pitch, yaw) of this command source.
    fn rotation(&self) -> Vector2<f32> {
        Vector2::default()
    }

    /// Checks if the block at the given position is loaded.
    fn check_block_loaded(&self, _pos: &BlockPos) -> Result<(), CommandSyntaxError> {
        Ok(())
    }

    /// Returns the anchor position for the given entity anchor.
    fn anchor_position(&self, _anchor: EntityAnchor) -> Vector3<f64> {
        self.position()
    }

    /// Returns the current entity anchor of this command source.
    fn entity_anchor(&self) -> EntityAnchor {
        EntityAnchor::Feet
    }
}

#[derive(Clone, Default)]
pub struct DummySource {
    pub position: Vector3<f64>,
    pub rotation: Vector2<f32>,
    pub entity_anchor: EntityAnchor,
    pub command_result_taker: ResultValueTaker,
}

impl DummySource {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn dummy() -> Self {
        Self::default()
    }
}

impl CommandSource for DummySource {
    fn send_message(&self, _message: TextComponent) {}
    fn send_error(&self, _error: TextComponent) {}
    fn position(&self) -> Vector3<f64> {
        self.position
    }
    fn rotation(&self) -> Vector2<f32> {
        self.rotation
    }
    fn entity_anchor(&self) -> EntityAnchor {
        self.entity_anchor
    }
    fn call_result(&self, result: ReturnValue) {
        self.command_result_taker.call(result);
    }

    fn call_deferred_result(&self, token: crate::deferred::DeferredCommandToken, result: ReturnValue) {
        self.command_result_taker.call_deferred(token, result);
    }

    fn complete_deferred_branch(&self, token: crate::deferred::DeferredCommandToken) {
        self.command_result_taker.complete_deferred_branch(token);
    }

    fn reserve_deferred_terminal(&self, token: crate::deferred::DeferredCommandToken) -> Result<Option<Box<dyn crate::deferred::DeferredCompletionTerminal>>, crate::deferred::DeferredCompletionUnavailable> {
        self.command_result_taker.reserve_deferred_terminal(token)
    }

    fn deferred_completion_readiness(&self, token: crate::deferred::DeferredCommandToken) -> crate::deferred::CompletionReadiness {
        self.command_result_taker.deferred_completion_readiness(token)
    }
}

impl CommandSource for () {
    fn send_message(&self, _message: TextComponent) {}
}

impl<S: CommandSource> CommandSource for Arc<S> {
    fn send_message(&self, message: TextComponent) {
        (**self).send_message(message);
    }

    fn send_error(&self, error: TextComponent) {
        (**self).send_error(error);
    }

    fn has_permission(&self, permission: &str) -> bool {
        (**self).has_permission(permission)
    }

    fn position(&self) -> Vector3<f64> {
        (**self).position()
    }

    fn rotation(&self) -> Vector2<f32> {
        (**self).rotation()
    }

    fn entity_anchor(&self) -> EntityAnchor {
        (**self).entity_anchor()
    }

    fn anchor_position(&self, anchor: EntityAnchor) -> Vector3<f64> {
        (**self).anchor_position(anchor)
    }

    fn call_result(&self, result: ReturnValue) {
        (**self).call_result(result);
    }

    fn call_deferred_result(&self, token: crate::deferred::DeferredCommandToken, result: ReturnValue) {
        (**self).call_deferred_result(token, result);
    }

    fn complete_deferred_branch(&self, token: crate::deferred::DeferredCommandToken) {
        (**self).complete_deferred_branch(token);
    }

    fn reserve_deferred_terminal(&self, token: crate::deferred::DeferredCommandToken) -> Result<Option<Box<dyn crate::deferred::DeferredCompletionTerminal>>, crate::deferred::DeferredCompletionUnavailable> {
        (**self).reserve_deferred_terminal(token)
    }

    fn deferred_completion_readiness(&self, token: crate::deferred::DeferredCommandToken) -> crate::deferred::CompletionReadiness {
        (**self).deferred_completion_readiness(token)
    }

    fn check_block_loaded(&self, pos: &BlockPos) -> Result<(), CommandSyntaxError> {
        (**self).check_block_loaded(pos)
    }
}
