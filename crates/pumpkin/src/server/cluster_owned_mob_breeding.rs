use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::mob_breeding::MobBreedingCredit;
use pumpkin_cluster::player_advancement::{PlayerAdvancementState, change};

pub(crate) fn advancement_outcome(kind: u16, credit: &MobBreedingCredit,
    current: &PlayerAdvancementState) -> Result<PlayerAdvancementState, ApplyFailure> {
    if credit.statistic_category != pumpkin_data::statistic::StatisticCategory::Custom as u16
        || credit.statistic_key != pumpkin_data::statistic::CustomStatistic::AnimalsBred as u32 {
        return Err(ApplyFailure::InvalidAction);
    }
    let kind = pumpkin_data::entity::EntityType::from_raw(kind).ok_or(ApplyFailure::InvalidAction)?;
    if !kind.mob { return Err(ApplyFailure::InvalidAction); }
    let definition = super::cluster_advancements::definition("minecraft:husbandry/breed_an_animal")
        .ok_or(ApplyFailure::InvalidAction)?;
    let next = change(current, &definition, Some("bred"), true, credit.timestamp)
        .ok_or(ApplyFailure::InvalidAction)?;
    let definition = super::cluster_advancements::definition("minecraft:husbandry/bred_all_animals")
        .ok_or(ApplyFailure::InvalidAction)?;
    let criterion = format!("minecraft:{}", kind.resource_name);
    if !definition.criteria.contains(&criterion) { return Ok(next); }
    change(&next, &definition, Some(&criterion), true, credit.timestamp)
        .ok_or(ApplyFailure::InvalidAction)
}

pub(super) fn prepare_intent(
    parents: [pumpkin_cluster::item_contact::ContactObservation; 2],
    source: &pumpkin_cluster::entity_body::EntityBody,
    reference: pumpkin_cluster::canonical_action::ActionRef,
    identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    continuation: pumpkin_cluster::mob_continuation::MobContinuation,
    experience_amount: u32,
    primary: pumpkin_cluster::realm_completion::PeerGeneration,
    credit: Option<MobBreedingCredit>,
) -> Result<pumpkin_cluster::mob_breeding::MobBreedingIntent, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{Cell, Value};
    use pumpkin_cluster::entity_inventory_admission::EntityInventoryAdmission;
    use pumpkin_cluster::mob_breeding::{MobBirthAdmission, MobBreedingIntent};
    let [child_identity, experience_identity] = identities else {
        return Err(ApplyFailure::InvalidAction);
    };
    if child_identity.source != source.owner || experience_identity.source != source.owner
        || reference.tick != parents[0].simulation_tick
        || source.id != match &parents[0].identity {
            pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth }
                if *birth == source.birth => *id,
            _ => return Err(ApplyFailure::InvalidAction),
        }
        || continuation.reproduction.growth_age != -24000
        || continuation.reproduction.love_ticks != 0
        || continuation.reproduction.breeding_cooldown != 0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut spatial = source.spatial.clone();
    spatial.position = parents[0].position;
    spatial.velocity_micro = [0; 3];
    let kind = pumpkin_data::entity::EntityType::from_raw(source.kind)
        .ok_or(ApplyFailure::InvalidAction)?;
    let child = super::cluster_owned_mob_birth::prepare_body(
        super::cluster_owned_mob_birth::MobBirthInput {
            tags: Default::default(), entity: child_identity.id, action: reference.id,
            owner: source.owner, tick: reference.tick, uuid: child_identity.uuid,
            kind: kind.clone(), spatial: spatial.clone(), continuation,
        }, parents[0].chunk)?;
    let cells = super::cluster_owned_mob_birth::inventory_layout(&child)?.into_iter()
        .map(|loc| Cell { revision: 0,
            value: Value::Inventory(pumpkin_cluster::inventory::InventoryCell::empty(loc)) })
        .collect();
    let experience = super::cluster_item_birth::reserved_experience_body(reference,
        experience_identity, primary, spatial, experience_amount)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let intent = MobBreedingIntent { issuer: source.owner,
        child: MobBirthAdmission { chunk: parents[0].chunk, body: child,
            inventory: EntityInventoryAdmission { imported_revision: 0, cells } },
        experience: MobBirthAdmission { chunk: parents[0].chunk, body: experience,
            inventory: EntityInventoryAdmission { imported_revision: 0, cells: Vec::new() } },
        parents, credit };
    if !intent.is_valid(reference.tick) { return Err(ApplyFailure::InvalidAction); }
    Ok(intent)
}

pub(super) fn child_continuation(
    source: &pumpkin_cluster::entity_body::EntityBody,
    mate: &pumpkin_cluster::entity_body::EntityBody,
    identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    random: &mut pumpkin_cluster::mob_continuation::MobBrain,
) -> Result<pumpkin_cluster::mob_continuation::MobContinuation, ApplyFailure> {
    use pumpkin_cluster::entity_body::EntityPayload;
    use pumpkin_cluster::mob_continuation::{MobBrain, MobContinuation, MobReproduction, MobSpecies};
    let EntityPayload::Living(source_living) = &source.payload else { return Err(ApplyFailure::InvalidAction); };
    let EntityPayload::Living(mate_living) = &mate.payload else { return Err(ApplyFailure::InvalidAction); };
    let source_mob = source_living.mob.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let mate_mob = mate_living.mob.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let kind = pumpkin_data::entity::EntityType::from_raw(source.kind)
        .filter(|kind| kind.id == mate.kind).ok_or(ApplyFailure::InvalidAction)?;
    let goals = super::cluster_owned_mob_goals::passive(kind).ok_or(ApplyFailure::InvalidAction)?;
    let raw = identity.uuid.as_u128();
    let mut brain = MobBrain::at_birth([raw as u64, (raw >> 64) as u64],
        source.spatial.position.map(f64::from_bits), f32::from_bits(source.spatial.yaw),
        source_mob.brain.navigation.kind).ok_or(ApplyFailure::InvalidAction)?;
    brain.goals = goals;
    brain.selector_phase = (identity.id.local_id & 1) as u8;
    let species = match (&source_mob.species, &mate_mob.species) {
        (MobSpecies::Sheep { color: source, .. }, MobSpecies::Sheep { color: mate, .. }) =>
            MobSpecies::Sheep { color: if random.next_random() & 1 == 0 { *source } else { *mate }, sheared: false },
        (MobSpecies::Horse { kind, .. }, MobSpecies::Horse { kind: mate_kind, .. }) if kind == mate_kind =>
            MobSpecies::Horse { kind: *kind, color: if matches!(kind, pumpkin_cluster::mob_continuation::HorseSpeciesKind::Horse) { (random.next_random() % 7) as u8 } else { 0 },
                markings: if matches!(kind, pumpkin_cluster::mob_continuation::HorseSpeciesKind::Horse) { (random.next_random() % 5) as u8 } else { 0 }, flags: 0, chested: false, temper: 0, owner: None },
        (MobSpecies::Generic, MobSpecies::Generic) => MobSpecies::Generic,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    Ok(MobContinuation { brain, species, reproduction: MobReproduction {
        growth_age: -24000, ..Default::default() } })
}
