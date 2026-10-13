use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MenuSource {
    PlayerCrafting,
    Virtual { kind: u16 },
    Block {
        chunk: crate::protocol::ChunkKey,
        position: crate::protocol::BlockPos,
        state: u16,
    },
    Blocks(Vec<MenuBlockContact>),
    Merchant {
        contact: crate::item_contact::ContactObservation,
        kind: u16,
    },
    Entity { contact: crate::item_contact::ContactObservation, kind: u16 },
    RemotePlayer { lease: crate::canonical_action::LivePlayerLease },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuBlockContact {
    pub chunk: crate::protocol::ChunkKey,
    pub position: crate::protocol::BlockPos,
    pub state: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum MenuSelection {
    #[default]
    None,
    TradeOffer(u32),
    AnvilName(String),
    Recipe(u32),
    LoomPattern(u16),
    Beacon { primary: u16, secondary: Option<u16> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuSessionState {
    pub closed_sessions: Vec<MenuClosedSession>,
    pub last_closed_session: u64,
    pub last_close: Option<crate::canonical_action::ActionRef>,
    pub session: u64,
    pub window_kind: u16,
    pub source: MenuSource,
    pub selection: MenuSelection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuClosedSession {
    pub session: u64,
    pub token: u64,
    pub window_kind: u16,
    pub action: crate::canonical_action::ActionRef,
}

impl Default for MenuSessionState {
    fn default() -> Self {
        Self { closed_sessions: Vec::new(), last_closed_session: 0, last_close: None,
            session: 0, window_kind: 0, source: MenuSource::PlayerCrafting, selection: MenuSelection::None }
    }
}

impl MenuSessionState {
    pub fn valid(&self) -> bool {
        if self.last_closed_session != 0 && self.last_close.is_none() { return false; }
        if self.closed_sessions.len() > 64 || self.closed_sessions.iter().any(|closed|
            closed.session == 0 || closed.token == 0 || closed.action.id.incarnation == 0)
            || self.closed_sessions.iter().map(|closed| closed.session).collect::<std::collections::BTreeSet<_>>().len() != self.closed_sessions.len() {
            return false;
        }
        if self.last_closed_session != 0 && !self.closed_sessions.iter().any(|closed| closed.session == self.last_closed_session) { return false; }
        if self.session == 0 { return self.window_kind == 0 && self.source == MenuSource::PlayerCrafting
            && self.selection == MenuSelection::None; }
        let source_valid = match &self.source {
            MenuSource::PlayerCrafting => false,
            MenuSource::Virtual { kind } => *kind != 0,
            MenuSource::Block { chunk, position, .. } => position.x.div_euclid(16) == chunk.position.x
                && position.z.div_euclid(16) == chunk.position.z,
            MenuSource::Blocks(blocks) => !blocks.is_empty() && blocks.len() <= 2
                && blocks.iter().all(|block| block.position.x.div_euclid(16) == block.chunk.position.x
                    && block.position.z.div_euclid(16) == block.chunk.position.z)
                && blocks.iter().map(|block| (block.chunk, block.position)).collect::<std::collections::BTreeSet<_>>().len() == blocks.len(),
            MenuSource::Merchant { contact, .. } | MenuSource::Entity { contact, .. } => contact.valid()
                && matches!(contact.identity, crate::item_contact::ContactIdentity::Entity { .. }),
            MenuSource::RemotePlayer { lease } => lease.incarnation != 0 && !lease.key.0.is_nil(),
        };
        source_valid && match &self.selection {
            MenuSelection::TradeOffer(offer) => *offer < 256 && matches!(self.source, MenuSource::Merchant { .. }),
            MenuSelection::AnvilName(name) => self.window_kind == 8 && name.chars().count() <= 50 && !name.chars().any(char::is_control),
            MenuSelection::LoomPattern(_) => self.window_kind == 18,
            MenuSelection::Recipe(_) => matches!(self.window_kind, 12 | 24),
            MenuSelection::Beacon { .. } => self.window_kind == 9,
            _ => true,
        }
    }

    pub fn belongs_to(&self, actor: crate::actor::ActorId) -> bool {
        self.valid() && match actor {
            crate::actor::ActorId::Player(_) => self.last_close.is_none_or(|reference| reference.id.actor == actor)
                && self.closed_sessions.iter().all(|closed| closed.action.id.actor == actor),
            crate::actor::ActorId::Entity(_) => *self == Self::default(),
            crate::actor::ActorId::Peer(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MenuSessionOperation {
    Open { state: MenuSessionState, viewer: crate::item_contact::ContactObservation },
    Select { session: u64, selection: MenuSelection },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MenuVirtualInventoryLayout {
    Crafting, Anvil, Beacon, Enchanting, Grindstone, Loom, Merchant, Smithing, Stonecutter, Cartography,
}

impl MenuVirtualInventoryLayout {
    pub fn window_kind(&self) -> u16 {
        match self { Self::Crafting => 12, Self::Anvil => 8, Self::Beacon => 9, Self::Enchanting => 13,
            Self::Grindstone => 15, Self::Loom => 18, Self::Merchant => 19, Self::Smithing => 21,
            Self::Stonecutter => 24, Self::Cartography => 23 }
    }

    pub fn slots(&self) -> u16 {
        match self { Self::Crafting => 10, Self::Beacon => 1, Self::Enchanting | Self::Stonecutter => 2,
            Self::Loom | Self::Smithing => 4, _ => 3 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuVirtualInventoryAdmission {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub session: u64,
    pub layout: MenuVirtualInventoryLayout,
}

impl MenuVirtualInventoryAdmission {
    pub fn snapshot(&self) -> crate::inventory::InventoryStateSnapshot {
        crate::inventory::InventoryStateSnapshot { target: self.inventory_target(), revision: 0,
            cells: self.entries().into_iter().map(|(_, cell)| cell).collect() }
    }
    pub fn inventory_target(&self) -> crate::inventory::InventoryTarget {
        crate::inventory::InventoryTarget::LeasedVirtual { host: self.lease.gid.server, key: self.lease.key.0,
            player: self.lease.gid, incarnation: self.lease.incarnation, session: self.session,
            kind: self.layout.window_kind() + 1 }
    }

    pub fn entries(&self) -> Vec<(crate::canonical_action::Target, crate::inventory::InventoryCell)> {
        (0..self.layout.slots()).map(|slot| {
            let location = crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, slot);
            (crate::canonical_action::Target::Player { key: self.lease.key,
                lease: Some(self.lease), inventory: Some(crate::canonical_action::InventoryId::Virtual {
                    host: self.lease.gid.server, kind: self.layout.window_kind() + 1,
                    session: self.session, inv: location.inv, slot: location.slot }) }, crate::inventory::InventoryCell::empty(location))
        }).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuSessionProposal {
    pub actor_chunk: crate::protocol::ChunkKey,
    pub operation: MenuSessionOperation,
    pub virtual_inventory: Option<MenuVirtualInventoryLayout>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MenuSessionIntent {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub target: crate::canonical_action::Target,
    pub operation: MenuSessionOperation,
    pub virtual_inventory: Option<MenuVirtualInventoryLayout>,
}

impl MenuSessionIntent {
    pub fn source_properties(&self) -> Vec<crate::canonical_action::Target> {
        use crate::canonical_action::{ChunkPart, Target};
        use crate::living_property::LivingProperty as P;
        let MenuSessionOperation::Open { state, viewer } = &self.operation else { return Vec::new(); };
        let property = |contact: &crate::item_contact::ContactObservation, property| Target::Chunk {
            key: contact.chunk, part: ChunkPart::LivingProperty { actor: contact.actor(), property }, inventory: None };
        match &state.source {
            MenuSource::Merchant { contact, .. } => vec![property(viewer, P::GameMode), property(contact, P::Health), property(contact, P::MerchantOffers)],
            MenuSource::Entity { .. } => vec![property(viewer, P::GameMode)],
            _ => Vec::new(),
        }
    }
    pub fn effect_targets(&self) -> Vec<crate::canonical_action::Target> {
        std::iter::once(self.target).chain(self.admission().into_iter().flat_map(|admission|
            admission.entries().into_iter().map(|(target, _)| target))).collect()
    }

    pub fn guards_valid(&self, action: &crate::canonical_action::Action) -> bool {
        use crate::canonical_action::{Cell, Expected, Value};
        if !self.valid() || action.id.actor != crate::actor::ActorId::Player(self.lease.gid)
            || action.id.incarnation != self.lease.incarnation || !action.player_leases.contains(&self.lease)
            || !self.targets().iter().all(|target| action.conditions.iter().any(|condition| condition.target == *target))
            || !action.conditions.iter().any(|condition| condition.target == self.health_target()
                && condition.expected == Expected::LivingUnsignedAtLeast(1)) {
            return false;
        }
        if !self.source_properties().iter().all(|target| action.conditions.iter().any(|condition|
            condition.target == *target && matches!(&condition.expected, Expected::Exact(Cell {
                value: Value::LivingProperty(value), .. }) if match (target, value) {
                    (crate::canonical_action::Target::Chunk { part: crate::canonical_action::ChunkPart::LivingProperty {
                        property: crate::living_property::LivingProperty::GameMode, .. }, .. },
                        crate::living_property::LivingPropertyValue::Unsigned(mode)) => *mode < 3,
                    (crate::canonical_action::Target::Chunk { part: crate::canonical_action::ChunkPart::LivingProperty {
                        property: crate::living_property::LivingProperty::Health, .. }, .. },
                        crate::living_property::LivingPropertyValue::Unsigned(health)) => *health > 0,
                    (crate::canonical_action::Target::Chunk { part: crate::canonical_action::ChunkPart::LivingProperty {
                        property: crate::living_property::LivingProperty::MerchantOffers, .. }, .. },
                        crate::living_property::LivingPropertyValue::MerchantOffers(Some(offers))) => offers.valid() && !offers.offers.is_empty(),
                    _ => false,
                }))) { return false; }
        let Some(previous) = action.conditions.iter().find_map(|condition| match &condition.expected {
            Expected::Exact(Cell { value: Value::LivingProperty(crate::living_property::LivingPropertyValue::MenuSession(menu)), .. })
                if condition.target == self.target => Some(menu), _ => None,
        }) else { return false; };
        if let Some(admission) = self.admission() && !admission.entries().iter().all(|(target, _)|
            action.conditions.iter().any(|condition| condition.target == *target && condition.expected == Expected::Missing)) {
            return false;
        }
        match &self.operation {
            MenuSessionOperation::Select { session, .. } => previous.session == *session,
            MenuSessionOperation::Open { state, viewer } => {
                if previous.session != 0 || previous.closed_sessions.iter().any(|closed| closed.session == state.session)
                    || !action.conditions.iter().any(|condition| condition.target == viewer.target()
                        && condition.expected == Expected::SpatialEpochEquals(viewer.epoch)) { return false; }
                match &state.source {
                    MenuSource::Merchant { contact, kind } | MenuSource::Entity { contact, kind } => {
                        let crate::item_contact::ContactIdentity::Entity { birth, .. } = &contact.identity else { return false; };
                        action.conditions.iter().any(|condition| condition.target == contact.target()
                            && condition.expected == Expected::SpatialEpochEquals(contact.epoch))
                            && action.conditions.iter().any(|condition| condition.target == contact.target()
                                && condition.expected == Expected::EntityBirthKind { birth: birth.clone(), owner: contact.owner, kind: *kind })
                    }
                    MenuSource::Blocks(blocks) => blocks.iter().all(|block| action.conditions.iter().any(|condition|
                        condition.target == crate::canonical_action::Target::Chunk { key: block.chunk,
                            part: crate::canonical_action::ChunkPart::Block(block.position), inventory: None }
                            && matches!(&condition.expected, Expected::Exact(Cell { value: Value::Block(state), .. }) if *state == block.state))),
                    MenuSource::Block { chunk, position, state } => action.conditions.iter().any(|condition|
                        condition.target == crate::canonical_action::Target::Chunk { key: *chunk,
                            part: crate::canonical_action::ChunkPart::Block(*position), inventory: None }
                            && matches!(&condition.expected, Expected::Exact(Cell { value: Value::Block(actual), .. }) if actual == state)),
                    MenuSource::RemotePlayer { .. } => action.edit_authority.is_some(),
                    _ => true,
                }
            }
        }
    }

    pub fn admission(&self) -> Option<MenuVirtualInventoryAdmission> {
        let MenuSessionOperation::Open { state, .. } = &self.operation else { return None; };
        self.virtual_inventory.clone().map(|layout| MenuVirtualInventoryAdmission { lease: self.lease, session: state.session, layout })
    }
    pub fn health_target(&self) -> crate::canonical_action::Target {
        let crate::canonical_action::Target::Chunk { key, .. } = self.target else { return self.target; };
        crate::canonical_action::Target::Chunk { key, part: crate::canonical_action::ChunkPart::LivingProperty {
            actor: crate::actor::ActorId::Player(self.lease.gid), property: crate::living_property::LivingProperty::Health }, inventory: None }
    }
    pub fn valid(&self) -> bool {
        use crate::canonical_action::{ChunkPart, Target};
        matches!(self.target, Target::Chunk { part: ChunkPart::LivingProperty {
            actor: crate::actor::ActorId::Player(gid), property: crate::living_property::LivingProperty::MenuSession,
        }, inventory: None, .. } if gid == self.lease.gid)
            && self.lease.incarnation != 0 && !self.lease.key.0.is_nil()
            && self.virtual_inventory.as_ref().is_none_or(|layout| matches!(&self.operation,
                MenuSessionOperation::Open { state, .. } if state.window_kind == layout.window_kind()))
            && match &self.operation {
                MenuSessionOperation::Open { state, viewer } => state.session != 0 && state.valid()
                    && viewer.valid()
                    && matches!(viewer.identity, crate::item_contact::ContactIdentity::Player(lease) if lease == self.lease)
                    && match &state.source {
                        MenuSource::Merchant { contact, .. } | MenuSource::Entity { contact, .. } => viewer.world == contact.world
                            && squared_distance(viewer.position, contact.position) <= 64.0,
                        MenuSource::Block { chunk, position, .. } => viewer.world == chunk.world
                            && squared_distance(viewer.position, [
                                (f64::from(position.x) + 0.5).to_bits(),
                                (f64::from(position.y) + 0.5).to_bits(),
                                (f64::from(position.z) + 0.5).to_bits(),
                            ]) <= 64.0,
                        MenuSource::Blocks(blocks) => blocks.iter().all(|block| viewer.world == block.chunk.world
                            && squared_distance(viewer.position, [(f64::from(block.position.x) + 0.5).to_bits(),
                                (f64::from(block.position.y) + 0.5).to_bits(), (f64::from(block.position.z) + 0.5).to_bits()]) <= 64.0),
                        MenuSource::RemotePlayer { lease } => *lease != self.lease,
                        MenuSource::Virtual { .. } => true,
                        MenuSource::PlayerCrafting => false,
                    },
                MenuSessionOperation::Select { session, .. } => *session != 0,
            }
    }

    pub fn targets(&self) -> Vec<crate::canonical_action::Target> {
        let mut targets = vec![self.target, self.health_target()];
        targets.extend(self.source_properties());
        if let Some(admission) = self.admission() { targets.extend(admission.entries().into_iter().map(|(target, _)| target)); }
        if let MenuSessionOperation::Open { state, viewer } = &self.operation {
            targets.push(viewer.target());
            match &state.source {
                MenuSource::Merchant { contact, .. } | MenuSource::Entity { contact, .. } => targets.push(contact.target()),
                MenuSource::Block { chunk, position, .. } => targets.push(crate::canonical_action::Target::Chunk {
                    key: *chunk, part: crate::canonical_action::ChunkPart::Block(*position), inventory: None,
                }),
                MenuSource::Blocks(blocks) => targets.extend(blocks.iter().map(|block| crate::canonical_action::Target::Chunk {
                    key: block.chunk, part: crate::canonical_action::ChunkPart::Block(block.position), inventory: None })),
                _ => {}
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    pub fn derive(
        &self,
        values: &std::collections::BTreeMap<crate::canonical_action::Target, crate::canonical_action::Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Cell, Effect, Value};
        use crate::living_property::LivingPropertyValue;
        if !self.valid() { return Err(ApplyFailure::InvalidAction); }
        for target in self.source_properties() {
            use crate::canonical_action::{ChunkPart, Target};
            use crate::living_property::LivingProperty as P;
            let valid = match target {
                Target::Chunk { part: ChunkPart::LivingProperty { property: P::GameMode, .. }, .. } => matches!(values.get(&target),
                    Some(Cell { value: Value::LivingProperty(LivingPropertyValue::Unsigned(mode)), .. }) if *mode < 3),
                Target::Chunk { part: ChunkPart::LivingProperty { property: P::Health, .. }, .. } => matches!(values.get(&target),
                    Some(Cell { value: Value::LivingProperty(LivingPropertyValue::Unsigned(health)), .. }) if *health > 0),
                Target::Chunk { part: ChunkPart::LivingProperty { property: P::MerchantOffers, .. }, .. } => matches!(values.get(&target),
                    Some(Cell { value: Value::LivingProperty(LivingPropertyValue::MerchantOffers(Some(offers))), .. }) if offers.valid() && !offers.offers.is_empty()),
                _ => false,
            };
            if !valid { return Err(ApplyFailure::Condition(target)); }
        }
        if let Some(admission) = self.admission() && admission.entries().iter().any(|(target, _)| values.contains_key(target)) {
            return Err(ApplyFailure::InvalidAction);
        }
        if !matches!(values.get(&self.health_target()), Some(Cell { value: Value::LivingProperty(
            LivingPropertyValue::Unsigned(health)), .. }) if *health > 0) {
            return Err(ApplyFailure::Condition(self.health_target()));
        }
        let Some(Cell { value: Value::LivingProperty(LivingPropertyValue::MenuSession(current)), .. }) = values.get(&self.target)
            else { return Err(ApplyFailure::Type(self.target)); };
        let next = match &self.operation {
            MenuSessionOperation::Open { state, viewer } => {
                if current.session != 0 || current.closed_sessions.iter().any(|closed| closed.session == state.session) {
                    return Err(ApplyFailure::Condition(self.target));
                }
                if !contact_matches(viewer, values.get(&viewer.target())) { return Err(ApplyFailure::Condition(viewer.target())); }
                match &state.source {
                    MenuSource::Merchant { contact, kind } | MenuSource::Entity { contact, kind } => {
                        if !contact_matches(contact, values.get(&contact.target()))
                            || !matches!(values.get(&contact.target()), Some(Cell { value: Value::Entity(body), .. }) if body.kind == *kind) {
                            return Err(ApplyFailure::Condition(contact.target()));
                        }
                    }
                    MenuSource::Block { chunk, position, state } => {
                        let target = crate::canonical_action::Target::Chunk { key: *chunk,
                            part: crate::canonical_action::ChunkPart::Block(*position), inventory: None };
                        if !matches!(values.get(&target), Some(Cell { value: Value::Block(actual), .. }) if actual == state) {
                            return Err(ApplyFailure::Condition(target));
                        }
                    }
                    MenuSource::Blocks(blocks) => for block in blocks {
                        let target = crate::canonical_action::Target::Chunk { key: block.chunk,
                            part: crate::canonical_action::ChunkPart::Block(block.position), inventory: None };
                        if !matches!(values.get(&target), Some(Cell { value: Value::Block(actual), .. }) if *actual == block.state) {
                            return Err(ApplyFailure::Condition(target));
                        }
                    },
                    _ => {}
                }
                let mut next = state.clone();
                next.last_closed_session = current.last_closed_session;
                next.last_close = current.last_close;
                next.closed_sessions = current.closed_sessions.clone();
                next
            }
            MenuSessionOperation::Select { session, selection } => {
                if current.session != *session { return Err(ApplyFailure::Condition(self.target)); }
                let mut next = current.clone();
                next.selection = selection.clone();
                next
            }
        };
        if !next.valid() { return Err(ApplyFailure::InvalidAction); }
        let mut effects = vec![Effect::Put { target: self.target,
            value: Value::LivingProperty(LivingPropertyValue::MenuSession(next)) }];
        if let Some(admission) = self.admission() { effects.push(Effect::MenuVirtualInventoryAdmission { admission }); }
        Ok(effects)
    }
}

pub(crate) fn contact_matches(contact: &crate::item_contact::ContactObservation, value: Option<&crate::canonical_action::Cell>) -> bool {
    use crate::canonical_action::{Cell, Value};
    match (&contact.identity, value) {
        (crate::item_contact::ContactIdentity::Player(_), Some(Cell { value: Value::Living(living), .. })) =>
            living.avatar.as_ref().is_some_and(|avatar| avatar.spatial_epoch == contact.epoch
                && avatar.position == contact.position && avatar.world == contact.world),
        (crate::item_contact::ContactIdentity::Entity { id, birth }, Some(Cell { value: Value::Entity(body), .. })) =>
            body.id == *id && body.birth == *birth && body.owner == contact.owner
                && body.spatial_epoch == contact.epoch && body.spatial.position == contact.position
                && body.spatial.world == contact.world,
        _ => false,
    }
}

fn squared_distance(first: [u64; 3], second: [u64; 3]) -> f64 {
    first.into_iter().zip(second).map(|(first, second)|
        (f64::from_bits(first) - f64::from_bits(second)).powi(2)).sum()
}

impl MenuSessionIntent {
    pub fn capture_action<S: crate::canonical_action::ActionState>(
        &self,
        owners: &S,
        capture: crate::canonical_inventory::InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{Action, ActionClass, ActionKind, ApplyFailure, Condition, Expected,
            SemanticIntent, Stakeholder, TruthView, UndoProcedure};
        if !self.valid() || !capture.player_leases.contains(&self.lease)
            || capture.id.actor != crate::actor::ActorId::Player(self.lease.gid)
            || capture.id.incarnation != self.lease.incarnation { return Err(ApplyFailure::InvalidAction); }
        let targets = self.targets();
        let admission_targets = self.admission().map(|admission| admission.entries().into_iter().map(|(target, _)| target)
            .collect::<std::collections::BTreeSet<_>>()).unwrap_or_default();
        let values = targets.iter().filter(|target| !admission_targets.contains(target)).map(|target| owners.read(TruthView::Local, *target)
            .map(|cell| (*target, cell)).ok_or(ApplyFailure::Condition(*target)))
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        let effects = self.derive(&values)?;
        let mut conditions = capture.guards;
        for target in self.source_properties() {
            conditions.push(Condition { target, expected: Expected::Exact(values[&target].clone()) });
        }
        for target in &admission_targets {
            if owners.read(TruthView::Local, *target).is_some() { return Err(ApplyFailure::Condition(*target)); }
            conditions.push(Condition { target: *target, expected: Expected::Missing });
        }
        conditions.push(Condition { target: self.target, expected: Expected::Exact(values[&self.target].clone()) });
        conditions.push(Condition { target: self.health_target(), expected: Expected::LivingUnsignedAtLeast(1) });
        if let MenuSessionOperation::Open { state, viewer } = &self.operation {
            conditions.push(Condition { target: viewer.target(), expected: Expected::SpatialEpochEquals(viewer.epoch) });
            match &state.source {
                MenuSource::Merchant { contact, kind } | MenuSource::Entity { contact, kind } => {
                    let crate::item_contact::ContactIdentity::Entity { birth, .. } = &contact.identity
                        else { return Err(ApplyFailure::InvalidAction); };
                    conditions.push(Condition { target: contact.target(), expected: Expected::EntityBirthKind {
                        birth: birth.clone(), owner: contact.owner, kind: *kind } });
                    conditions.push(Condition { target: contact.target(), expected: Expected::SpatialEpochEquals(contact.epoch) });
                }
                MenuSource::Block { chunk, position, .. } => {
                    let target = crate::canonical_action::Target::Chunk { key: *chunk,
                        part: crate::canonical_action::ChunkPart::Block(*position), inventory: None };
                    conditions.push(Condition { target, expected: Expected::Exact(values[&target].clone()) });
                }
                MenuSource::Blocks(blocks) => for block in blocks {
                    let target = crate::canonical_action::Target::Chunk { key: block.chunk,
                        part: crate::canonical_action::ChunkPart::Block(block.position), inventory: None };
                    conditions.push(Condition { target, expected: Expected::Exact(values[&target].clone()) });
                },
                MenuSource::RemotePlayer { .. } if capture.edit_authority.is_none() => return Err(ApplyFailure::InvalidAction),
                _ => {}
            }
        }
        let stakeholders = targets.iter().map(|target| target.stakeholder())
            .chain(conditions.iter().map(|condition| condition.target.stakeholder()))
            .chain(capture.actor_chunk.map(Stakeholder::Chunk))
            .chain(capture.player_chunks.iter().map(|(_, chunk)| Stakeholder::Chunk(*chunk)))
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let action = Action {
            id: capture.id, tick: capture.tick, class: ActionClass::Replayable, chunk_transition: false,
            actor_chunk: capture.actor_chunk, player_leases: capture.player_leases,
            player_chunks: capture.player_chunks, stakeholders, dependencies: Vec::new(), conditions, effects,
            counterfactual_conditions: targets, counterfactual_effects: std::iter::once(self.target).chain(admission_targets).collect(),
            semantic_intent: Some(SemanticIntent::MenuSessionChange(self.clone())), factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets], kind: ActionKind::Inventory,
            presentation: Vec::new(), client_origin: capture.client_origin, edit_authority: capture.edit_authority,
        };
        action.validate()?;
        Ok(action)
    }
}
