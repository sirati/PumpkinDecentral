use pumpkin_cluster::world_time::*;
fn time_update() -> TimeUpdate {
    TimeUpdate::new(String::from("minecraft:overworld"), 1000, 0.0, 1.0, false)
}
fn weather_update() -> WeatherUpdate {
    WeatherUpdate::new(
        String::from("minecraft:overworld"),
        12_000,
        0,
        0,
        false,
        false,
    )
}
fn environment(issuer: pumpkin_cluster::realm_completion::PeerGeneration) -> EnvironmentSnapshot {
    EnvironmentSnapshot {
        issuer,
        revision: 10,
        slot: 1000,
        world_age: 400,
        rain_level: 0.0,
        old_rain_level: 0.0,
        thunder_level: 0.0,
        old_thunder_level: 0.0,
        config_revision: 10,
        config_from_slot: 1000,
        time: time_update(),
        weather: weather_update(),
        morning: None,
        border: BorderState {
            center_x: 0.0,
            center_z: 0.0,
            old_diameter: 100.0,
            new_diameter: 200.0,
            speed: 5000,
            lerp_start_slot: 1000,
            warning_blocks: 5,
            warning_time: 15,
            damage_per_block: 0.2,
            buffer: 5.0,
        },
        spawn: SpawnState {
            x: 10,
            y: 64,
            z: -10,
            yaw: 90.0,
            pitch: 0.0,
        },
    }
}

#[test]
fn environment_rejects_spoofed_boot_replay_and_backward_slots() {
    use pumpkin_cluster::realm_completion::PeerGeneration;
    let elected = PeerGeneration {
        server: pumpkin_cluster::identity::ServerId(1),
        generation: 99,
    };
    let old_boot = PeerGeneration {
        generation: 98,
        ..elected
    };
    let old = environment(elected);
    let mut incoming = old.clone();
    incoming.revision += 1;
    incoming.slot += 1;
    assert!(accept_environment_snapshot(
        elected,
        elected,
        Some(&old),
        &incoming
    ));
    assert!(!accept_environment_snapshot(
        old_boot,
        elected,
        Some(&old),
        &incoming
    ));
    assert!(!accept_environment_snapshot(
        elected,
        elected,
        Some(&old),
        &old
    ));
    incoming.slot = old.slot - 1;
    assert!(!accept_environment_snapshot(
        elected,
        elected,
        Some(&old),
        &incoming
    ));
    incoming.slot = old.slot;
    incoming.issuer = old_boot;
    assert!(!accept_environment_snapshot(
        elected,
        elected,
        Some(&old),
        &incoming
    ));
}

#[test]
fn environment_wire_rejects_invalid_config_and_morning_provenance() {
    let issuer = pumpkin_cluster::realm_completion::PeerGeneration {
        server: pumpkin_cluster::identity::ServerId(1),
        generation: 99,
    };
    let mut value = environment(issuer);
    value.morning = Some(MorningTransition {
        issuer,
        revision: 8,
        slot: 990,
    });
    let bytes = encode_environment(&EnvironmentControl::Snapshot(value.clone())).unwrap();
    let EnvironmentControl::Snapshot(restored) = decode_environment(&bytes).unwrap() else {
        panic!()
    };
    assert_eq!(restored.border.new_diameter, 200.0);
    assert_eq!(restored.spawn.z, -10);
    assert_eq!(restored.morning.unwrap().slot, 990);
    value.morning.as_mut().unwrap().issuer.generation -= 1;
    assert!(
        decode_environment(
            &encode_environment(&EnvironmentControl::Snapshot(value.clone())).unwrap()
        )
        .is_err()
    );
    value.morning = None;
    value.border.damage_per_block = f32::NAN;
    assert!(
        decode_environment(&encode_environment(&EnvironmentControl::Snapshot(value)).unwrap())
            .is_err()
    );
}

#[test]
fn environment_wire_preserves_add_requests_and_rejects_nonfinite_values() {
    let request = EnvironmentControl::Request {
        dimension: String::from("minecraft:overworld"),
        mutation: EnvironmentMutation::Add(500),
    };
    assert_eq!(
        decode_environment(&encode_environment(&request).unwrap()).unwrap(),
        request
    );
    let invalid = EnvironmentControl::Request {
        dimension: String::from("minecraft:overworld"),
        mutation: EnvironmentMutation::Border(BorderMutation::Center(f64::INFINITY, 0.0)),
    };
    assert!(decode_environment(&encode_environment(&invalid).unwrap()).is_err());
}

#[test]
fn border_lerp_uses_shared_original_slot_for_late_joiners() {
    let issuer = pumpkin_cluster::realm_completion::PeerGeneration {
        server: pumpkin_cluster::identity::ServerId(1),
        generation: 99,
    };
    let mut state = environment(issuer).border;
    state.lerp_start_slot = 1000;
    state.speed = 5000;
    assert_eq!(state.current_diameter(1000), 100.0);
    assert_eq!(state.current_diameter(1050), 150.0);
    assert_eq!(state.remaining_millis(1050), 2500);
    assert_eq!(state.current_diameter(1101), 200.0);
    assert_eq!(state.remaining_millis(1101), 0);
}

#[test]
fn retained_border_history_proves_original_slots_and_rejects_forged_bounds() {
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use pumpkin_cluster::time::TickStamp;
    let elected = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let world = WorldId::new(0, 1);
    let environment = environment(elected);
    let first = WorldBorderSnapshot {
        world,
        issuer: elected,
        revision: 1,
        from_slot: 1000,
        border: environment.border,
        spawn: environment.spawn,
    };
    let original = first.witness(1050);
    let mut history = WorldBorderHistory::default();
    assert!(!history.insert(
        PeerGeneration {
            generation: 98,
            ..elected
        },
        elected,
        first.clone()
    ));
    assert!(history.resolve(original, TickStamp(1050)).is_none());
    assert!(history.insert(elected, elected, first.clone()));
    assert_eq!(history.revision(), 1);
    assert!(history.insert(elected, elected, first.clone()));
    assert_eq!(history.revision(), 1);
    assert_eq!(
        f64::from_bits(
            history
                .resolve(original, TickStamp(1050))
                .unwrap()
                .witness(1050)
                .diameter
        ),
        150.0
    );
    let forged = WorldBorderWitness {
        diameter: 1000.0_f64.to_bits(),
        ..original
    };
    assert!(history.resolve(forged, TickStamp(1050)).is_none());
    assert!(history.resolve(original, TickStamp(1051)).is_none());
    let mut second = first;
    second.revision = 2;
    second.from_slot = 1100;
    second.border.new_diameter = 400.0;
    assert!(history.insert(elected, elected, second));
    assert!(history.resolve(original, TickStamp(1050)).is_some());
    let expired = WorldBorderWitness {
        slot: 1100,
        diameter: 200.0_f64.to_bits(),
        ..original
    };
    assert!(history.resolve(expired, TickStamp(1100)).is_none());
    assert_eq!(history.current(world, elected, 1100).unwrap().revision, 2);
}

#[test]
fn exact_environment_capacity_preserves_source_pins_and_publication_barriers() {
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use std::sync::Arc;
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let world = WorldId::new(0, 1);
    let first = environment(issuer);
    let mut second = first.clone();
    second.slot += 1;
    second.revision += 1;
    second.rain_level = 0.25;
    let mut third = second.clone();
    third.slot += 1;
    third.revision += 1;
    third.rain_level = 0.5;
    let mut history = EnvironmentHistory::new(2);
    let mut roots = EnvironmentRoots::default();
    history
        .try_insert(issuer, issuer, world, Arc::new(first.clone()), &roots)
        .unwrap();
    history
        .try_insert(issuer, issuer, world, Arc::new(second.clone()), &roots)
        .unwrap();
    assert_eq!(
        history.try_insert(issuer, issuer, world, Arc::new(third.clone()), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    roots.published_through_slot = Some(2000);
    let pinned = history.get(world, issuer, first.slot).unwrap();
    assert_eq!(
        history.try_insert(issuer, issuer, world, Arc::new(third.clone()), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    drop(pinned);
    roots.pin_source(first.slot);
    assert_eq!(
        history.try_insert(issuer, issuer, world, Arc::new(third.clone()), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    roots.source_slots.clear();
    history
        .try_insert(issuer, issuer, world, Arc::new(third), &roots)
        .unwrap();
    assert!(history.get(world, issuer, first.slot).is_none());
    assert_eq!(
        history
            .get(world, issuer, second.slot)
            .unwrap()
            .snapshot
            .rain_level,
        0.25
    );
    assert_eq!(history.len(), 2);
}

#[test]
fn exact_environment_source_pin_keeps_transitive_border_and_morning_proofs() {
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    use std::sync::Arc;
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let world = WorldId::new(0, 1);
    let mut first = environment(issuer);
    let morning = MorningTransition {
        issuer,
        revision: 8,
        slot: 990,
    };
    first.morning = Some(morning);
    let mut history = EnvironmentHistory::new(3);
    history
        .try_insert(
            issuer,
            issuer,
            world,
            Arc::new(first.clone()),
            &EnvironmentRoots::default(),
        )
        .unwrap();
    let retained = history.get(world, issuer, first.slot).unwrap();
    let mut later = first.clone();
    later.revision += 1;
    later.config_revision += 1;
    later.config_from_slot = first.slot;
    later.border.center_x = 10.0;
    history
        .try_insert(
            issuer,
            issuer,
            world,
            Arc::new(later.clone()),
            &EnvironmentRoots::default(),
        )
        .unwrap();
    let mut roots = EnvironmentRoots::default();
    history.retain_required_roots(&mut roots);
    assert!(
        roots
            .borders
            .contains(&(world, issuer, first.config_revision))
    );
    assert!(roots.mornings.contains(&(world, issuer, morning.revision)));
    assert_eq!(retained.snapshot.border.center_x, 0.0);
    assert_eq!(
        history
            .get(world, issuer, first.slot)
            .unwrap()
            .snapshot
            .border
            .center_x,
        10.0
    );
}

#[test]
fn morning_archive_rejects_spoofed_boot_and_preserves_active_cause_roots() {
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::protocol::WorldId;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let world = WorldId::new(0, 1);
    let first = MorningTransition {
        issuer,
        revision: 8,
        slot: 1000,
    };
    let second = MorningTransition {
        issuer,
        revision: 9,
        slot: 2000,
    };
    let third = MorningTransition {
        issuer,
        revision: 10,
        slot: 3000,
    };
    let mut history = WorldMorningHistory::new(2);
    let mut roots = EnvironmentRoots::default();
    roots.published_through_slot = Some(4000);
    assert_eq!(
        history.try_insert(
            PeerGeneration {
                generation: 98,
                ..issuer
            },
            issuer,
            world,
            first,
            &roots
        ),
        Err(EnvironmentEvidenceError::Invalid)
    );
    history
        .try_insert(issuer, issuer, world, first, &roots)
        .unwrap();
    history
        .try_insert(issuer, issuer, world, second, &roots)
        .unwrap();
    let before = history.revision();
    assert_eq!(
        history.try_insert(
            issuer,
            issuer,
            world,
            MorningTransition {
                slot: 1500,
                ..third
            },
            &roots
        ),
        Err(EnvironmentEvidenceError::Invalid)
    );
    assert_eq!(history.revision(), before);
    assert!(history.resolve(world, first));
    assert!(history.resolve(world, second));
    assert_eq!(
        history.try_insert(
            issuer,
            issuer,
            world,
            MorningTransition {
                revision: 7,
                slot: 2500,
                ..first
            },
            &roots
        ),
        Err(EnvironmentEvidenceError::Invalid)
    );
    assert!(history.resolve(world, first));
    roots.pin_morning(world, first);
    assert_eq!(
        history.try_insert(issuer, issuer, world, third, &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    let forged = MorningTransition { slot: 999, ..first };
    assert!(history.contains(world, forged));
    assert!(!history.resolve(world, forged));
    roots.mornings.clear();
    history
        .try_insert(issuer, issuer, world, third, &roots)
        .unwrap();
    assert!(!history.contains(world, first));
    assert!(history.resolve(world, second));
    assert_eq!(history.records().count(), 2);
}

#[test]
fn grouped_frames_preserve_temporal_weather_and_reject_oversized_batches() {
    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::realm_completion::PeerGeneration;
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let first = environment(issuer);
    let mut second = first.clone();
    second.slot += 1;
    second.revision += 1;
    second.rain_level = 0.25;
    second.old_rain_level = 0.2;
    let control = EnvironmentControl::Frames(vec![first.clone(), second.clone()]);
    let bytes = encode_environment(&control).unwrap();
    assert_eq!(decode_environment(&bytes).unwrap(), control);
    let oversized = EnvironmentControl::Frames(vec![first; 9]);
    assert!(decode_environment(&encode_environment(&oversized).unwrap()).is_err());
    second.rain_level = f32::NAN;
    assert!(
        decode_environment(&encode_environment(&EnvironmentControl::Snapshot(second)).unwrap())
            .is_err()
    );
}

#[test]
fn original_environment_archive_rejects_retimed_versions_but_accepts_late_frames() {
    use pumpkin_cluster::{
        identity::ServerId, protocol::WorldId, realm_completion::PeerGeneration,
    };
    use std::sync::Arc;
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let world = WorldId::new(0, 1);
    let first = environment(issuer);
    let mut later = first.clone();
    later.revision += 2;
    later.slot += 2;
    let mut history = EnvironmentHistory::new(8);
    let roots = EnvironmentRoots::default();
    history
        .try_insert(issuer, issuer, world, Arc::new(later), &roots)
        .unwrap();
    history
        .try_insert(issuer, issuer, world, Arc::new(first.clone()), &roots)
        .unwrap();
    let mut retimed = first.clone();
    retimed.slot += 1;
    assert_eq!(
        history.try_insert(issuer, issuer, world, Arc::new(retimed), &roots),
        Err(EnvironmentEvidenceError::Invalid)
    );
    let mut backwards = first.clone();
    backwards.revision += 3;
    backwards.slot += 1;
    assert_eq!(
        history.try_insert(issuer, issuer, world, Arc::new(backwards), &roots),
        Err(EnvironmentEvidenceError::Invalid)
    );
    let mut middle = first.clone();
    middle.revision += 1;
    middle.slot += 1;
    history
        .try_insert(issuer, issuer, world, Arc::new(middle.clone()), &roots)
        .unwrap();
    assert_eq!(
        *history.get(world, issuer, middle.slot).unwrap().snapshot,
        middle
    );
    assert_eq!(
        *history.get(world, issuer, first.slot).unwrap().snapshot,
        first
    );
    assert_eq!(history.len(), 3);
}

#[test]
fn invalid_border_chronology_does_not_evict_retained_history() {
    use pumpkin_cluster::{
        identity::ServerId, protocol::WorldId, realm_completion::PeerGeneration,
    };
    let issuer = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let value = environment(issuer);
    let first = WorldBorderSnapshot {
        world: WorldId::new(0, 1),
        issuer,
        revision: 1,
        from_slot: 100,
        border: value.border,
        spawn: value.spawn,
    };
    let second = WorldBorderSnapshot {
        revision: 2,
        from_slot: 200,
        ..first.clone()
    };
    let mut history = WorldBorderHistory::new(2);
    assert!(history.insert(issuer, issuer, first.clone()));
    assert!(history.insert(issuer, issuer, second.clone()));
    let mut roots = EnvironmentRoots::default();
    roots.published_through_slot = Some(1000);
    let invalid = WorldBorderSnapshot {
        revision: 3,
        from_slot: 150,
        ..first.clone()
    };
    assert_eq!(
        history.try_insert(issuer, issuer, invalid, &roots),
        Err(EnvironmentEvidenceError::Invalid)
    );
    assert_eq!(history.revision(), 2);
    assert_eq!(history.get(first.world, issuer, 1), Some(&first));
    assert_eq!(history.get(second.world, issuer, 2), Some(&second));
    let conflicting = WorldBorderSnapshot {
        from_slot: 101,
        ..first.clone()
    };
    assert_eq!(
        history.try_insert(issuer, issuer, conflicting, &roots),
        Err(EnvironmentEvidenceError::Invalid)
    );
    assert_eq!(history.get(first.world, issuer, 1), Some(&first));
}

#[test]
fn qualified_retirement_releases_last_boot_records_only_after_all_roots_clear() {
    use pumpkin_cluster::{
        identity::ServerId, protocol::WorldId, realm_completion::PeerGeneration,
    };
    use std::sync::Arc;
    let old = PeerGeneration {
        server: ServerId(1),
        generation: 99,
    };
    let new = PeerGeneration {
        generation: 100,
        ..old
    };
    let world = WorldId::new(0, 1);
    let first = environment(old);
    let next = environment(new);
    let border = WorldBorderSnapshot {
        world,
        issuer: old,
        revision: 1,
        from_slot: 1000,
        border: first.border,
        spawn: first.spawn,
    };
    let successor = WorldBorderSnapshot {
        issuer: new,
        ..border.clone()
    };
    let mut roots = EnvironmentRoots::default();
    roots.published_through_slot = Some(1001);
    let mut borders = WorldBorderHistory::new(1);
    assert!(borders.insert(old, old, border.clone()));
    assert_eq!(
        borders.try_insert(new, new, successor.clone(), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    assert!(
        roots
            .retirements
            .try_insert(EnvironmentDomain::Borders, world, old, 999)
    );
    assert_eq!(
        borders.try_insert(new, new, successor.clone(), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    assert!(
        roots
            .retirements
            .try_insert(EnvironmentDomain::Borders, world, old, 1000)
    );
    roots.pin_border(border.witness(1000));
    assert_eq!(
        borders.try_insert(new, new, successor.clone(), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    roots.borders.clear();
    roots.source_slots.clear();
    assert_eq!(borders.try_insert(new, new, successor, &roots), Ok(true));
    assert!(!borders.contains(border.witness(1000)));
    let morning = MorningTransition {
        issuer: old,
        revision: 1,
        slot: 1000,
    };
    let next_morning = MorningTransition {
        issuer: new,
        ..morning
    };
    let mut mornings = WorldMorningHistory::new(1);
    assert_eq!(
        mornings.try_insert(old, old, world, morning, &roots),
        Ok(true)
    );
    assert_eq!(
        mornings.try_insert(new, new, world, next_morning, &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    assert!(
        roots
            .retirements
            .try_insert(EnvironmentDomain::Mornings, world, old, 1000)
    );
    roots.pin_morning(world, morning);
    assert_eq!(
        mornings.try_insert(new, new, world, next_morning, &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    roots.mornings.clear();
    assert_eq!(
        mornings.try_insert(new, new, world, next_morning, &roots),
        Ok(true)
    );
    let mut frames = EnvironmentHistory::new(1);
    assert_eq!(
        frames.try_insert(old, old, world, Arc::new(first.clone()), &roots),
        Ok(true)
    );
    let pin = frames.get(world, old, first.slot).unwrap();
    assert!(
        roots
            .retirements
            .try_insert(EnvironmentDomain::Frames, world, old, first.slot)
    );
    assert_eq!(
        frames.try_insert(new, new, world, Arc::new(next.clone()), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    drop(pin);
    roots.pin_source(first.slot);
    assert_eq!(
        frames.try_insert(new, new, world, Arc::new(next.clone()), &roots),
        Err(EnvironmentEvidenceError::Capacity)
    );
    roots.source_slots.clear();
    assert_eq!(
        frames.try_insert(new, new, world, Arc::new(next), &roots),
        Ok(true)
    );
    assert!(frames.get(world, old, first.slot).is_none());
}
