use serde::{Deserialize, Serialize};

use crate::action_command::ActorReference;
use crate::actor_occupancy::ActorOccupancyCut;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuSource {
    pub actor: ActorReference,
    pub session: u64,
    pub cut: ActorOccupancyCut,
}

impl MenuSource {
    pub fn valid(&self) -> bool {
        self.cut.valid() && matches!(&self.actor, ActorReference::Player { lease, owner, .. }
            if lease.incarnation != 0 && !lease.key.0.is_nil() && owner.server == lease.gid.server && owner.generation != 0)
    }
    pub fn menu_target(&self) -> crate::canonical_action::Target {
        crate::canonical_action::Target::Chunk {
            key: self.actor.chunk(),
            part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: self.actor.actor(), property: crate::living_property::LivingProperty::MenuSession,
            },
            inventory: None,
        }
    }
    pub fn resolve<'a, S: crate::canonical_action::ActionState>(&self, context: &'a crate::action_command::CommandContext<'_, S>)
        -> Result<&'a crate::canonical_menu::MenuSessionState, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, ChunkPart, Target, Value};
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        let ActorReference::Player { lease, epoch, .. } = &self.actor else { return Err(ApplyFailure::InvalidAction); };
        let crate::action_identity::ActionIdentity::Issued(identity) = context.identity else { return Err(ApplyFailure::InvalidAction); };
        if !self.valid() || identity.actor != self.actor.actor() || identity.incarnation != lease.incarnation
            || self.cut.sample_tick.is_after(context.tick) || !context.state.validate_live_player_lease(*lease) {
            return Err(ApplyFailure::InvalidAction);
        }
        let body_target = self.actor.body_target();
        let Some(Value::Living(body)) = context.values.get(&body_target).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(body_target)); };
        if !body.belongs_to(self.actor.actor()) || body.avatar.as_ref().is_none_or(|avatar| avatar.spatial_epoch != *epoch) {
            return Err(ApplyFailure::Condition(body_target));
        }
        let (owner, cut) = context.state.actor_occupancy_cut(body_target, &context.values[&body_target].value)?;
        if owner != self.actor.owner() || cut != self.cut { return Err(ApplyFailure::Condition(body_target)); }
        let property = |property| Target::Chunk { key: self.actor.chunk(), part: ChunkPart::LivingProperty { actor: self.actor.actor(), property }, inventory: None };
        if !matches!(context.values.get(&property(P::Health)).map(|cell| &cell.value), Some(Value::LivingProperty(V::Unsigned(1..)))) {
            return Err(ApplyFailure::Condition(property(P::Health)));
        }
        if !matches!(context.values.get(&property(P::GameMode)).map(|cell| &cell.value), Some(Value::LivingProperty(V::Unsigned(0..=2)))) {
            return Err(ApplyFailure::Condition(property(P::GameMode)));
        }
        let target = self.menu_target();
        let Some(Value::LivingProperty(V::MenuSession(menu))) = context.values.get(&target).map(|cell| &cell.value) else { return Err(ApplyFailure::Condition(target)); };
        if !menu.valid() || menu.session != self.session { return Err(ApplyFailure::Condition(target)); }
        Ok(menu)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconSelection {
    pub source: MenuSource,
    pub primary: Option<u16>,
    pub secondary: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultTransfer {
    Cursor,
    QuickMove,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeResult {
    pub source: MenuSource,
    pub transfer: ResultTransfer,
}

impl BeaconSelection {
    pub fn resolve<'a, S: crate::canonical_action::ActionState>(&self, context: &'a crate::action_command::CommandContext<'_, S>)
        -> Result<&'a crate::canonical_menu::MenuSessionState, crate::canonical_action::ApplyFailure> {
        let menu = self.source.resolve(context)?;
        if menu.window_kind != 9 || !matches!(menu.source, crate::canonical_menu::MenuSource::Block { .. }) {
            return Err(crate::canonical_action::ApplyFailure::Condition(self.source.menu_target()));
        }
        Ok(menu)
    }
}

impl TakeResult {
    pub fn resolve<'a, S: crate::canonical_action::ActionState>(&self, context: &'a crate::action_command::CommandContext<'_, S>)
        -> Result<&'a crate::canonical_menu::MenuSessionState, crate::canonical_action::ApplyFailure> {
        let menu = self.source.resolve(context)?;
        if !matches!(menu.window_kind, 0 | 8 | 10 | 12 | 13 | 14 | 15 | 18 | 19 | 21 | 22 | 23 | 24) {
            return Err(crate::canonical_action::ApplyFailure::Condition(self.source.menu_target()));
        }
        Ok(menu)
    }
}
