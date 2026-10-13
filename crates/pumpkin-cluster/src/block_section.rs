use crate::canonical_action::{AppliedWrite, ApplyFailure, Cell, ChunkPart, Target, Value};

pub fn project_writes(
    target: Target,
    before: &Cell,
    writes: &[AppliedWrite],
) -> Result<Cell, ApplyFailure> {
    let Target::Chunk {
        key,
        part: ChunkPart::BlockSection(section_y),
        inventory: None,
    } = target
    else {
        return Err(ApplyFailure::Type(target));
    };
    let Value::BlockSection(palette) = &before.value else {
        return Err(ApplyFailure::Type(target));
    };
    if palette.len() != 4096 {
        return Err(ApplyFailure::Type(target));
    }
    let mut next = palette.clone();
    let mut changed = false;
    for write in writes {
        let Target::Chunk {
            key: written_key,
            part: ChunkPart::Block(position),
            inventory: None,
        } = write.target
        else {
            continue;
        };
        if written_key != key || position.y.div_euclid(16) != section_y {
            continue;
        }
        let index = (position.y.rem_euclid(16) as usize * 16 + position.z.rem_euclid(16) as usize)
            * 16
            + position.x.rem_euclid(16) as usize;
        let Some(Cell {
            value: Value::Block(previous),
            ..
        }) = &write.before
        else {
            return Err(ApplyFailure::Type(write.target));
        };
        let Some(Cell {
            value: Value::Block(after),
            ..
        }) = &write.after
        else {
            return Err(ApplyFailure::Type(write.target));
        };
        if next[index] != *previous {
            return Err(ApplyFailure::Condition(write.target));
        }
        next[index] = *after;
        changed = true;
    }
    Ok(Cell {
        revision: if changed {
            before
                .revision
                .checked_add(1)
                .ok_or(ApplyFailure::InvalidAction)?
        } else {
            before.revision
        },
        value: Value::BlockSection(next),
    })
}

pub fn project_undo(
    target: Target,
    before: &Cell,
    writes: &[AppliedWrite],
) -> Result<Cell, ApplyFailure> {
    let mut projected = project_writes(target, before, writes)?;
    if projected.revision != before.revision {
        projected.revision = before
            .revision
            .checked_sub(1)
            .ok_or(ApplyFailure::InvalidAction)?;
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{BlockPos, ChunkKey, WorldId};

    #[test]
    fn one_action_updates_negative_section_once_and_rejects_stale_palette() {
        let key = ChunkKey::new(WorldId::new(0, 0), -1, -1);
        let target = Target::Chunk {
            key,
            part: ChunkPart::BlockSection(-1),
            inventory: None,
        };
        let before = Cell {
            revision: 9,
            value: Value::BlockSection(vec![0; 4096]),
        };
        let write = |x, y, z, after| AppliedWrite {
            target: Target::Chunk {
                key,
                part: ChunkPart::Block(BlockPos { x, y, z }),
                inventory: None,
            },
            before: Some(Cell {
                revision: 3,
                value: Value::Block(0),
            }),
            after: Some(Cell {
                revision: 4,
                value: Value::Block(after),
            }),
        };
        let writes = [write(-16, -16, -16, 1), write(-1, -1, -1, 2)];
        let projected = project_writes(target, &before, &writes).unwrap();
        assert_eq!(projected.revision, 10);
        let Value::BlockSection(palette) = &projected.value else {
            unreachable!();
        };
        assert_eq!(palette[0], 1);
        assert_eq!(palette[4095], 2);
        let undo = writes
            .iter()
            .rev()
            .map(|write| AppliedWrite {
                target: write.target,
                before: write.after.clone(),
                after: write.before.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(project_undo(target, &projected, &undo).unwrap(), before);
        assert_eq!(
            project_writes(target, &projected, &writes),
            Err(ApplyFailure::Condition(writes[0].target))
        );
        assert_eq!(before.revision, 9);
        assert_eq!(project_writes(target, &before, &[]).unwrap(), before);
    }
}
