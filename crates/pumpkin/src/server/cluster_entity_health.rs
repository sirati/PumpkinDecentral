use pumpkin_cluster::{canonical_action::LivePlayerLease, entity_body::{EntityBody, EntityPayload}, health_policy::{HealthMaskMode, required_health}, item_contact::ContactIdentity};

#[derive(Clone, Copy)]
pub(super) struct HealthRenderContext {
    pub mode: HealthMaskMode,
    pub viewer: Option<LivePlayerLease>,
}

impl Default for HealthRenderContext {
    fn default() -> Self { Self { mode: HealthMaskMode::Players, viewer: None } }
}

pub(super) fn values(body: &EntityBody, context: HealthRenderContext) -> Result<Option<(f32, f64)>, String> {
    let EntityPayload::Living(living) = &body.payload else { return Ok(None); };
    let visual = body.kind == pumpkin_data::entity::EntityType::WOLF.id || body.kind == pumpkin_data::entity::EntityType::IRON_GOLEM.id;
    let rider = context.viewer.is_some_and(|lease| body.passengers.values().any(|identity| *identity == ContactIdentity::Player(lease)));
    let health = required_health(context.mode, living.avatar.is_some(), visual, rider, living.combat.health_milli as f32 / 1000.0);
    let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
    let base = living.combat.effective_attributes.get(&attribute).copied().ok_or("canonical max health missing")?;
    let maximum = living.attribute(attribute, f64::from_bits(base));
    if !maximum.is_finite() || maximum < 0.0 || maximum > 1_000_000.0 { return Err("canonical max health invalid".into()); }
    let masked = !visual && !rider && match context.mode { HealthMaskMode::All => true, HealthMaskMode::Players => living.avatar.is_some(), HealthMaskMode::Off => false };
    Ok(Some((health, if masked { 20.0 } else { maximum })))
}

pub(super) fn project(body: &EntityBody, id: i32, client: &crate::net::ClientPlatform, context: HealthRenderContext) -> Result<Vec<bytes::Bytes>, String> {
    let Some((health, maximum)) = values(body, context)? else { return Ok(Vec::new()); };
    match client {
        crate::net::ClientPlatform::Java(_) => {
            let version = client.java_version();
            let field = pumpkin_data::tracked_data::living_entity::DATA_HEALTH_ID;
            if field.id.get(&version) == 255 { return Ok(Vec::new()); }
            let mut metadata = Vec::new();
            pumpkin_protocol::java::client::play::Metadata::new(field, health).write(&mut metadata, &version).map_err(|error| error.to_string())?;
            metadata.push(if version < pumpkin_util::version::JavaMinecraftVersion::V_1_9 { 127 } else { 255 });
            Ok(vec![
                crate::net::java::JavaClient::serialize_packet_for_version(
                    &pumpkin_protocol::java::client::play::CUpdateAttributes::new(id.into(), vec![
                        pumpkin_protocol::java::client::play::Property::new(i32::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id).into(), maximum, Vec::new()),
                    ]), version,
                ).map_err(|error| error.to_string())?,
                crate::net::java::JavaClient::serialize_packet_for_version(
                    &pumpkin_protocol::java::client::play::CSetEntityMetadata::new(id.into(), metadata.into_boxed_slice()), version,
                ).map_err(|error| error.to_string())?,
            ])
        }
        crate::net::ClientPlatform::Bedrock(client) => prepare_bedrock(body, id, context, &client.encoder_snapshot()),
    }
}

pub(super) fn prepare_bedrock(body: &EntityBody, id: i32, context: HealthRenderContext,
    encoder: &pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder) -> Result<Vec<bytes::Bytes>, String> {
    let Some((health, maximum)) = values(body, context)? else { return Ok(Vec::new()); };
    use pumpkin_protocol::{bedrock::client::update_attributes::{AttributeData, CUpdateAttributes}, codec::var_ulong::VarULong};
    Ok(vec![encoder.serialize_packet(&CUpdateAttributes {
        target_runtime_id: VarULong(id as u64), tick: VarULong(0), attribute_list: vec![AttributeData {
            name: "minecraft:health".into(), min_value: 0.0, max_value: maximum as f32, current_value: health,
            default_min_value: 0.0, default_max_value: maximum as f32, default_value: maximum as f32, modifiers: Vec::new(),
        }],
    }).map_err(|error| error.to_string())?])
}
