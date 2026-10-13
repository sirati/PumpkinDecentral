use crate::canonical_action::{
    AppliedWrite, ApplyFailure, Cell, ChunkPart, InventoryId, Target, Value,
};
use crate::entity_body::EntityPayload;
use crate::object_state::ObjectKind;

pub fn body_target(target: Target) -> Option<Target> {
    let Target::Chunk {
        key,
        part: ChunkPart::Entity(actor),
        inventory: Some(InventoryId::Entity { inv, slot }),
    } = target
    else {
        return None;
    };
    (inv == crate::inventory::INV_MAIN && slot == 0).then_some(Target::Chunk {
        key,
        part: ChunkPart::Entity(actor),
        inventory: None,
    })
}

pub fn project(
    target: Target,
    before: Option<&Cell>,
    projected: Option<Cell>,
    writes: &[AppliedWrite],
) -> Result<Option<Cell>, ApplyFailure> {
    let Some(mut projected) = projected else {
        return Ok(None);
    };
    let Value::Entity(body) = &mut projected.value else {
        return Ok(Some(projected));
    };
    let EntityPayload::Object(object) = &mut body.payload else {
        return Ok(Some(projected));
    };
    let ObjectKind::ItemFrame(frame) = &mut object.kind else {
        return Ok(Some(projected));
    };
    let mut changed = false;
    for write in writes
        .iter()
        .filter(|write| body_target(write.target) == Some(target))
    {
        let Some(Cell {
            value: Value::Inventory(item),
            ..
        }) = &write.after
        else {
            return Err(ApplyFailure::Type(write.target));
        };
        if item.loc != crate::inventory::InvLoc::new(crate::inventory::INV_MAIN, 0)
            || item.count > 1
        {
            return Err(ApplyFailure::Inventory(write.target));
        }
        if frame.item != *item {
            frame.item = item.clone();
            changed = true;
        }
    }
    if changed
        && !writes.iter().any(|write| write.target == target)
        && before.is_some_and(|cell| cell.revision == projected.revision)
    {
        projected.revision = projected
            .revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(target))?;
    }
    Ok(Some(projected))
}
