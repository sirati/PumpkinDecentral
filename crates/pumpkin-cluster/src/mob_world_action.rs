use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ActionState, ApplyFailure, ChunkPart, ClientOrigin,
    Condition, Effect, Expected, Stakeholder, Target, TruthView, UndoProcedure, Value,
};
use crate::entity_body::{EntityBody, EntityPayload};
use crate::living_property::{LivingProperty, LivingPropertyValue};
use crate::mob_continuation::MobSpecies;
use crate::mob_species_property::{MobSpeciesProperty, MobSpeciesPropertyValue};
use crate::protocol::{BlockPos, ChunkKey};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

pub fn graze<S: ActionState>(
    state: &S,
    body: &EntityBody,
    actor_chunk: ChunkKey,
    issuer: PeerGeneration,
    tick: TickStamp,
    id: ActionId,
    position: BlockPos,
    expected: u16,
    replacement: u16,
    modify_world: bool,
) -> Result<Action, ApplyFailure> {
    let actor = ActorId::Entity(body.id);
    let EntityPayload::Living(living) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Some(mob) = &living.mob else {
        return Err(ApplyFailure::InvalidAction);
    };
    let MobSpecies::Sheep { .. } = mob.species else {
        return Err(ApplyFailure::InvalidAction);
    };
    if issuer != body.owner
        || id.actor != actor
        || id.incarnation != u64::from(body.id.generation)
        || !body.belongs_to(body.id, actor_chunk)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let block_chunk = ChunkKey::new(
        actor_chunk.world,
        position.x.div_euclid(16),
        position.z.div_euclid(16),
    );
    let species = Target::Chunk {
        key: actor_chunk,
        part: ChunkPart::LivingProperty {
            actor,
            property: LivingProperty::MobSpeciesProperty(MobSpeciesProperty::SheepSheared),
        },
        inventory: None,
    };
    let block = Target::Chunk {
        key: block_chunk,
        part: ChunkPart::Block(position),
        inventory: None,
    };
    let body_target = Target::Chunk {
        key: actor_chunk,
        part: ChunkPart::Entity(body.id),
        inventory: None,
    };
    let mut conditions = vec![
        Condition {
            target: body_target,
            expected: Expected::ActorControlled {
                birth: body.birth.clone(),
                issuer,
            },
        },
        Condition {
            target: block,
            expected: Expected::BlockState(expected),
        },
    ];
    for target in [species] {
        let cell = state
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        conditions.push(Condition {
            target,
            expected: Expected::Exact(cell),
        });
    }
    let mut effects = vec![Effect::Put {
        target: species,
        value: Value::LivingProperty(LivingPropertyValue::MobSpeciesProperty(Some(
            MobSpeciesPropertyValue::Flag(false),
        ))),
    }];
    if modify_world {
        effects.push(Effect::Put {
            target: block,
            value: Value::Block(replacement),
        });
    }
    let mut stakeholders = vec![
        Stakeholder::Chunk(actor_chunk),
        Stakeholder::Chunk(block_chunk),
    ];
    stakeholders.sort_unstable();
    stakeholders.dedup();
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        actor_chunk: Some(actor_chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders,
        dependencies: Vec::new(),
        conditions,
        effects,
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        presentation: if modify_world {
            vec![
                crate::world_presentation::WorldPresentationIntent::BreakParticles {
                    chunk: block_chunk,
                    position,
                    state: expected,
                },
            ]
        } else {
            Vec::new()
        },
        kind: ActionKind::Actor,
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}
