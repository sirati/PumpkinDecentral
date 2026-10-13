use pumpkin_cluster::{
    object_property::ObjectPropertyValue,
    object_state::{FallingBlockState, ObjectKind, ObjectProperty, ObjectState, TntState},
    object_tick::{ObjectTerminal, capture},
    protocol::BlockPos,
    time::TickStamp,
};

fn object(kind: ObjectKind, previous: u16) -> ObjectState {
    ObjectState {
        kind,
        simulation_tick: Some(TickStamp(previous)),
        property_versions: Default::default(),
    }
}

#[test]
fn delayed_tnt_tick_explodes_once_at_wrapped_source_tick() {
    let state = object(
        ObjectKind::PrimedTnt(TntState {
            fuse: 3,
            power: 4.0f32.to_bits(),
            block_state: 1,
            igniter: None,
        }),
        u16::MAX - 1,
    );
    let plan = capture(&state, TickStamp(100), TickStamp(2), 64.0, -64, 320).unwrap();
    assert_eq!(plan.elapsed, 4);
    assert_eq!(
        plan.terminal,
        Some(ObjectTerminal::Explosion {
            power: 4.0f32.to_bits(),
            igniter: None
        })
    );
    assert!(
        plan.updates
            .contains(&(ObjectProperty::Fuse, ObjectPropertyValue::Fuse(0)))
    );
    let mut applied = state;
    applied.simulation_tick = Some(TickStamp(2));
    assert!(capture(&applied, TickStamp(100), TickStamp(2), 64.0, -64, 320).is_none());
}

fn falling(age: u32) -> ObjectState {
    object(
        ObjectKind::FallingBlock(FallingBlockState {
            block_state: 1,
            start: BlockPos { x: 0, y: 64, z: 0 },
            age,
            drop_item: true,
            hurt_entities: false,
            fall_distance: 0.0f32.to_bits(),
            damage_per_block: 2.0f32.to_bits(),
            max_damage: 40.0f32.to_bits(),
            tile_data: Vec::new(),
        }),
        10,
    )
}

#[test]
fn falling_blocks_have_distinct_unloaded_and_out_of_world_expiry_limits() {
    assert!(
        capture(&falling(599), TickStamp(0), TickStamp(11), 64.0, -64, 320)
            .unwrap()
            .terminal
            .is_none()
    );
    assert_eq!(
        capture(&falling(600), TickStamp(0), TickStamp(11), 64.0, -64, 320)
            .unwrap()
            .terminal,
        Some(ObjectTerminal::FallingExpired {
            block_state: 1,
            drop_item: true
        })
    );
    assert!(
        capture(&falling(99), TickStamp(0), TickStamp(11), -65.0, -64, 320)
            .unwrap()
            .terminal
            .is_none()
    );
    assert!(matches!(
        capture(&falling(100), TickStamp(0), TickStamp(11), -65.0, -64, 320)
            .unwrap()
            .terminal,
        Some(ObjectTerminal::FallingExpired { .. })
    ));
    assert!(
        capture(
            &falling(100),
            TickStamp(0),
            TickStamp(11),
            f64::NAN,
            -64,
            320
        )
        .is_none()
    );
}
