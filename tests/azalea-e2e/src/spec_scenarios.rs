use std::{collections::HashSet, env, hash::Hasher};

use anyhow::{Context, Result, bail};
use azalea::{
    BlockPos, Client, ClientInformation, Vec3, WalkDirection,
    core::direction::Direction,
    ecs::query::With,
    entity::{EntityDataValue, metadata::Zombie},
    registry::builtin::{BlockKind, EntityKind, ItemKind, MenuKind},
};
use azalea_inventory::{
    components::{ChargedProjectiles, CustomName, Damage, EquipmentSlot},
    operations::{ClickType, SwapClick},
};
use azalea_protocol::packets::game::ClientboundGamePacket;
use azalea_protocol::packets::game::s_container_click::{HashedStack, ServerboundContainerClick};
use azalea_protocol::packets::game::s_container_close::ServerboundContainerClose;
use azalea_protocol::packets::game::s_edit_book::ServerboundEditBook;
use azalea_protocol::packets::game::s_interact::{InteractionHand, ServerboundInteract};
use azalea_protocol::packets::game::s_player_action::{
    Action as PlayerAction, ServerboundPlayerAction,
};
use azalea_protocol::packets::game::s_player_command::{
    Action as PlayerCommand, ServerboundPlayerCommand,
};
use azalea_protocol::packets::game::s_set_carried_item::ServerboundSetCarriedItem;
use azalea_protocol::packets::game::s_swing::ServerboundSwing;
use azalea_protocol::packets::game::s_use_item::ServerboundUseItem;
use azalea_protocol::packets::game::s_use_item_on::{BlockHit, ServerboundUseItemOn};
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    BOT_A, Deadline, Event, offline_uuid, strip_ansi, structured_value, wait_chat_contains,
    wait_chat_exact, wait_for_position, wait_for_position_change, wait_inventory_slot_kind,
    wait_ticks, wait_until, within, zombie_uuids,
};

#[derive(Clone, Copy)]
enum BlockExpectation {
    Exact(BlockKind),
    Either(BlockKind, BlockKind),
}

#[derive(Clone, Copy)]
struct WorldScenario {
    id: &'static str,
    positions: &'static [(BlockPos, BlockExpectation)],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum FixtureActorId {
    Player(u16, u16),
    Entity(u16, i32, u64),
}

fn parse_fixture_actor(value: &str) -> Result<FixtureActorId> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["p", server, player] => Ok(FixtureActorId::Player(server.parse()?, player.parse()?)),
        ["e", origin, local, generation] => Ok(FixtureActorId::Entity(
            origin.parse()?,
            local.parse()?,
            generation.parse()?,
        )),
        _ => bail!("invalid action actor ID {value}"),
    }
}

fn fixture_actor_rank(seed: u64, tick: u16, actor: FixtureActorId) -> u64 {
    let mut hash = xxhash_rust::xxh64::Xxh64::new(seed);
    hash.write_u16(tick);
    match actor {
        FixtureActorId::Player(server, player) => {
            hash.write_u8(0);
            hash.write_u16(server);
            hash.write_u16(player);
        }
        FixtureActorId::Entity(origin, local, generation) => {
            hash.write_u8(1);
            hash.write_u16(origin);
            hash.write_i32(local);
            hash.write_u64(generation);
        }
    }
    hash.finish()
}

const CONFLICT_POSITION: BlockPos = BlockPos::new(0, 80, 4);
const SECOND_POSITION: BlockPos = BlockPos::new(17, 80, 4);
const NEAR_POSITION: BlockPos = BlockPos::new(1, 80, 4);
const WORLD_SCENARIOS: &[WorldScenario] = &[
    WorldScenario {
        id: "zero-action-manifest",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "subscriber-gap-during-transfer",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "relay-drop-newest-ground-election",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "unsolicited-update-unsubscribe",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "actor-local-overlay-discard",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "canonical-atomic-failure",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "conflict-two-place",
        positions: &[(
            CONFLICT_POSITION,
            BlockExpectation::Either(BlockKind::Stone, BlockKind::OakPlanks),
        )],
    },
    WorldScenario {
        id: "conflict-three-place",
        positions: &[(
            CONFLICT_POSITION,
            BlockExpectation::Either(BlockKind::Stone, BlockKind::OakPlanks),
        )],
    },
    WorldScenario {
        id: "finish-before-actions",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "dropped-peer-gap",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "stale-dirt-break",
        positions: &[(
            CONFLICT_POSITION,
            BlockExpectation::Exact(BlockKind::OakPlanks),
        )],
    },
    WorldScenario {
        id: "targeted-undo",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "late-dependent-replay",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "graph-shared-conditions",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "late-holder-unresolved-snapshot",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "undo-redo-cascade",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "counterfactual-ground-block",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "dropped-graph-recovery",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "tick-bank-backpressure",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "cross-chunk-joint",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (SECOND_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
        ],
    },
    WorldScenario {
        id: "no-coholder-entity-joint",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (SECOND_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
        ],
    },
    WorldScenario {
        id: "partial-ground-promotion",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone)),
            (
                SECOND_POSITION,
                BlockExpectation::Exact(BlockKind::OakPlanks),
            ),
        ],
    },
    WorldScenario {
        id: "sheep-graze-race",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Dirt))],
    },
    WorldScenario {
        id: "door-trapdoor",
        positions: &[
            (
                CONFLICT_POSITION,
                BlockExpectation::Exact(BlockKind::OakDoor),
            ),
            (
                NEAR_POSITION,
                BlockExpectation::Exact(BlockKind::OakTrapdoor),
            ),
        ],
    },
    WorldScenario {
        id: "end-frame-anchor",
        positions: &[
            (
                CONFLICT_POSITION,
                BlockExpectation::Exact(BlockKind::EndPortalFrame),
            ),
            (
                NEAR_POSITION,
                BlockExpectation::Exact(BlockKind::RespawnAnchor),
            ),
        ],
    },
    WorldScenario {
        id: "redstone-delayed-dependency",
        positions: &[(
            CONFLICT_POSITION,
            BlockExpectation::Exact(BlockKind::RedstoneLamp),
        )],
    },
    WorldScenario {
        id: "button-lever",
        positions: &[
            (
                CONFLICT_POSITION,
                BlockExpectation::Exact(BlockKind::OakButton),
            ),
            (NEAR_POSITION, BlockExpectation::Exact(BlockKind::Lever)),
        ],
    },
    WorldScenario {
        id: "explosion-deltas",
        positions: &[
            (CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Air)),
            (SECOND_POSITION, BlockExpectation::Exact(BlockKind::Air)),
        ],
    },
    WorldScenario {
        id: "primary-random-tick",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Dirt))],
    },
    WorldScenario {
        id: "holder-drop-reroute",
        positions: &[(CONFLICT_POSITION, BlockExpectation::Exact(BlockKind::Stone))],
    },
    WorldScenario {
        id: "entity-chunk-transition",
        positions: &[],
    },
];

pub(super) async fn run_world_scenarios(
    actor: &Client,
    observer: &Client,
    third: Option<&Client>,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect::<HashSet<_>>();
    for name in &selected {
        if !super::scenario_catalog::REQUIRED_SCENARIOS.contains(&name.as_str()) {
            bail!("unknown extended world scenario {name}");
        }
    }
    if [
        "handoff-callback-early-confirm",
        "handoff-revision-mismatch",
        "login-plugin-cancel",
        "login-plugin-delay",
    ]
    .into_iter()
    .filter(|id| selected.contains(*id))
    .count()
        > 1
    {
        bail!("handoff and login callback injections require separate fixture runs");
    }
    if selected.contains("login-plugin-cancel") && selected.contains("login-plugin-delay") {
        bail!("login-plugin-cancel and login-plugin-delay require separate fixture runs");
    }
    if selected.contains("handoff-revision-mismatch")
        && (selected.contains("login-plugin-cancel") || selected.contains("login-plugin-delay"))
    {
        bail!(
            "handoff revision mismatch and login callback injections require separate fixture runs"
        );
    }
    if selected.contains("chat-departure-delay") && selected.contains("command-departure-delay") {
        bail!("delayed chat and command departure require separate fixture runs");
    }
    if selected.contains("packet-preflight-disconnect")
        && (selected.contains("chat-departure-delay")
            || selected.contains("command-departure-delay"))
    {
        bail!("packet preflight and delayed chat/command disconnect require separate fixture runs");
    }
    if selected.contains("pinned-radius-view-change")
        && env::var_os("PUMPKIN_E2E_FULL_RADIUS").is_some()
    {
        bail!(
            "pinned-radius-view-change is a separate initial radius-8 fixture and cannot combine with PUMPKIN_E2E_FULL_RADIUS"
        );
    }
    for scenario in WORLD_SCENARIOS {
        if !selected.contains(scenario.id) {
            eprintln!(
                "untested {}: select PUMPKIN_E2E_SPEC_SCENARIOS={} after its server preparation hook exists",
                scenario.id, scenario.id
            );
            continue;
        }
        run_world_scenario(
            actor,
            observer,
            third,
            events,
            observer_events,
            deadline,
            scenario,
        )
        .await?;
        eprintln!(
            "passed {}: participating Java clients observed the required converged state after accepted completion",
            scenario.id
        );
    }
    if selected.contains("syncstats") {
        run_syncstats(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested syncstats: select PUMPKIN_E2E_SPEC_SCENARIOS=syncstats after its cluster-reachable lock path is removed"
        );
    }
    for id in ["datagram-loss-reorder", "datagram-multi-batch"] {
        if selected.contains(id) {
            run_datagram_loss_reorder(actor, observer, events, deadline, id).await?;
        } else {
            eprintln!(
                "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after the feature-only movement transport hook exists"
            );
        }
    }
    if selected.contains("health-mask") {
        run_health_mask(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested health-mask: select PUMPKIN_E2E_SPEC_SCENARIOS=health-mask after deterministic injury and mount fixture hooks exist"
        );
    }
    if selected.contains("health-mask-mount") {
        run_health_mask_mount(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested health-mask-mount: select PUMPKIN_E2E_SPEC_SCENARIOS=health-mask-mount after mounted low-health entity hook exists"
        );
    }
    if selected.contains("health-visual-exemption") {
        run_health_visual_exemption(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested health-visual-exemption: select PUMPKIN_E2E_SPEC_SCENARIOS=health-visual-exemption after injured golem policy hook exists"
        );
    }
    if selected.contains("invsee-number-swap") {
        run_invsee_number_swap(actor, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-number-swap: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-number-swap after canonical cross-player inventory settlement exists"
        );
    }
    if selected.contains("invsee-quickcraft") {
        run_invsee_quickcraft(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-quickcraft: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-quickcraft after canonical remote QuickCraft settlement exists"
        );
    }
    if selected.contains("invsee-stale-window") {
        run_invsee_stale_window(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-stale-window: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-stale-window after canonical screen-session correction exists"
        );
    }
    if selected.contains("invsee-refresh-r2-r3") {
        run_invsee_refresh(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-refresh-r2-r3: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-refresh-r2-r3 after two accepted target mutations can be staged"
        );
    }
    if selected.contains("invsee-wrap-window") {
        run_invsee_wrap_window(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-wrap-window: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-wrap-window after delayed old-session refresh hook exists"
        );
    }
    if selected.contains("initial-avatar-inventory") {
        run_initial_avatar_inventory(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested initial-avatar-inventory: canonical persisted loadout preparation hook required"
        );
    }
    if selected.contains("creative-clone-source-cursor") {
        run_creative_clone_source_cursor(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested creative-clone-source-cursor: canonical preparation and late same-tick graph delivery hooks required"
        );
    }
    if selected.contains("inventory-move-split-merge") {
        run_inventory_move_split_merge(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested inventory-move-split-merge: select PUMPKIN_E2E_SPEC_SCENARIOS=inventory-move-split-merge after canonical inventory setup exists"
        );
    }
    if selected.contains("inventory-component-identity") {
        run_inventory_components(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested inventory-component-identity: accepted differently named stack setup missing"
        );
    }
    if selected.contains("inventory-pickup-rollback-split") {
        run_inventory_pickup_rollback(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested inventory-pickup-rollback-split: delayed competing pickup/canonical split fixture hook missing"
        );
    }
    if selected.contains("untrusted-result-reject") {
        run_untrusted_result_reject(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested untrusted-result-reject: select PUMPKIN_E2E_SPEC_SCENARIOS=untrusted-result-reject after a crafting/anvil preview setup hook exists"
        );
    }
    if selected.contains("combat-effects") {
        run_combat_effects(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested combat-effects: select PUMPKIN_E2E_SPEC_SCENARIOS=combat-effects after canonical actor damage and statistics hooks exist"
        );
    }
    if selected.contains("sweeping-fire-effects") {
        run_sweeping_fire_effects(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested sweeping-fire-effects: canonical sword/nearby-entity combat setup missing"
        );
    }
    if selected.contains("visual-state-cycle") {
        run_visual_state_cycle(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested visual-state-cycle: select PUMPKIN_E2E_SPEC_SCENARIOS=visual-state-cycle after canonical visual state and prepared usable item hooks exist"
        );
    }
    if selected.contains("book-plugin-order") {
        run_book_plugin_order(actor, observer, events, deadline).await?;
    } else {
        eprintln!(
            "untested book-plugin-order: select PUMPKIN_E2E_SPEC_SCENARIOS=book-plugin-order after delayed EditBook plugin hook and canonical item setup exist"
        );
    }
    for id in [
        "membership-peer-replacement",
        "membership-quic-resume",
        "protocol-version-reject",
    ] {
        if selected.contains(id) {
            while events.try_recv().is_ok() {}
            actor.chat(&format!("/clustertest {id}"));
            tokio::time::timeout(
                deadline
                    .remaining(id)?
                    .min(std::time::Duration::from_secs(90)),
                async {
                    while let Some(event) = events.recv().await {
                        match event {
                            Event::Chat(chat)
                                if chat.message().to_string()
                                    == format!("cluster test {id} complete") =>
                            {
                                return Ok(());
                            }
                            Event::Disconnect(reason) => {
                                bail!("{id} disconnected Java client: {reason:?}")
                            }
                            _ => {}
                        }
                    }
                    bail!("{id} client stream closed")
                },
            )
            .await
            .with_context(|| format!("{id} did not complete within ninety seconds"))??;
            observer.chat("/list");
            wait_chat_contains(observer_events, "PumpkinE2eA", deadline, id).await?;
        }
    }
    for (id, swimming) in [("visual-swim", true), ("visual-glide", false)] {
        if selected.contains(id) {
            run_visual_environment(actor, observer, events, deadline, id, swimming).await?;
        } else {
            eprintln!(
                "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after its accepted water-lane or Elytra-fall preparation hook exists"
            );
        }
    }
    if selected.contains("inventory-conflict") {
        run_inventory_conflict(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested inventory-conflict: select PUMPKIN_E2E_SPEC_SCENARIOS=inventory-conflict after canonical shared-container conflict setup exists"
        );
    }
    if selected.contains("inventory-placement-conflict") {
        run_inventory_placement_conflict(actor, observer, events, observer_events, deadline)
            .await?;
    } else {
        eprintln!(
            "untested inventory-placement-conflict: select PUMPKIN_E2E_SPEC_SCENARIOS=inventory-placement-conflict after canonical joint item/world/inventory resolution exists"
        );
    }
    for id in ["projectile-effects", "crossbow-effects"] {
        if selected.contains(id) {
            run_projectile_effects(actor, observer, events, observer_events, deadline, id).await?;
        } else {
            eprintln!(
                "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after canonical weapon/projectile action hooks exist"
            );
        }
    }
    for id in ["food-completion", "food-abort"] {
        if selected.contains(id) {
            run_food_use(actor, observer, events, deadline, id).await?;
        } else {
            eprintln!(
                "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after canonical food/inventory setup exists"
            );
        }
    }
    if selected.contains("mining-break") {
        run_mining_break(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!("untested mining-break: canonical mining/tool/drop/statistic hook missing");
    }
    if selected.contains("visual-break-start-abort") {
        run_break_visuals(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested visual-break-start-abort: accepted survival barehand/stone preparation missing"
        );
    }
    if selected.contains("virtual-crafting") {
        run_virtual_crafting(actor, events, deadline).await?;
    } else {
        eprintln!(
            "untested virtual-crafting: select PUMPKIN_E2E_SPEC_SCENARIOS=virtual-crafting after canonical player crafting-grid result action exists"
        );
    }
    if selected.contains("acceptance-delay") {
        actor.chat("/clustertest acceptance-delay");
        tokio::time::timeout(
            deadline
                .remaining("accepted tick held past sixty seconds")?
                .min(std::time::Duration::from_secs(90)),
            async {
                while let Some(event) = events.recv().await {
                    match event {
                        Event::Chat(chat)
                            if chat.message().to_string()
                                == "cluster test acceptance-delay complete" =>
                        {
                            return Ok(());
                        }
                        Event::Disconnect(reason) => {
                            bail!("acceptance-delay disconnected Java client: {reason:?}")
                        }
                        _ => {}
                    }
                }
                bail!("acceptance-delay event stream ended before settlement")
            },
        )
        .await
        .context("acceptance-delay did not complete after the expected >60-second hold")??;
    } else {
        eprintln!(
            "untested acceptance-delay: select PUMPKIN_E2E_SPEC_SCENARIOS=acceptance-delay after a feature-only unresolved tick hook exists"
        );
    }
    if selected.contains("acceptance-cooldown") {
        actor.chat("/clustertest acceptance-cooldown");
        tokio::time::timeout(
            deadline
                .remaining("five-minute acceptance cooldown")?
                .min(std::time::Duration::from_secs(600)),
            async {
                while let Some(event) = events.recv().await {
                    match event {
                        Event::Chat(chat)
                            if chat.message().to_string()
                                == "cluster test acceptance-cooldown complete" =>
                        {
                            return Ok(());
                        }
                        Event::Disconnect(reason) => {
                            bail!("acceptance-cooldown disconnected Java client: {reason:?}")
                        }
                        _ => {}
                    }
                }
                bail!(
                    "acceptance-cooldown event stream ended before its real-time cooldown expired"
                )
            },
        )
        .await
        .context("acceptance-cooldown did not finish within the bounded six-minute stage")??;
    } else {
        eprintln!(
            "untested acceptance-cooldown: select PUMPKIN_E2E_SPEC_SCENARIOS=acceptance-cooldown after same-key real-time cooldown fixture hook exists"
        );
    }
    if selected.contains("holder-only-routing") {
        run_holder_only_routing(actor, observer, events, deadline).await?;
    } else {
        eprintln!(
            "untested holder-only-routing: select PUMPKIN_E2E_SPEC_SCENARIOS=holder-only-routing after holder release and snapshot hooks exist"
        );
    }
    if selected.contains("entity-holder-only-routing") {
        run_entity_holder_only_routing(actor, observer, events, deadline).await?;
    } else {
        eprintln!(
            "untested entity-holder-only-routing: select PUMPKIN_E2E_SPEC_SCENARIOS=entity-holder-only-routing after entity holder release/snapshot hook exists"
        );
    }
    if selected.contains("item-drop-pickup") {
        run_item_drop_pickup(actor, observer, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested item-drop-pickup: select PUMPKIN_E2E_SPEC_SCENARIOS=item-drop-pickup after canonical item entity setup exists"
        );
    }
    if selected.contains("invsee-armor-offhand") {
        run_invsee_armor_offhand(actor, events, observer_events, deadline).await?;
    } else {
        eprintln!(
            "untested invsee-armor-offhand: select PUMPKIN_E2E_SPEC_SCENARIOS=invsee-armor-offhand after canonical remote equipment editing exists"
        );
    }
    if selected.contains("container-transfer") {
        run_container_transfer(actor, observer, events, deadline).await?;
    } else {
        eprintln!(
            "untested container-transfer: select PUMPKIN_E2E_SPEC_SCENARIOS=container-transfer after canonical chest setup exists"
        );
    }
    if selected.contains("ender-chest-reopen") {
        run_ender_chest_reopen(actor, observer, events, deadline).await?;
    } else {
        eprintln!(
            "untested ender-chest-reopen: select PUMPKIN_E2E_SPEC_SCENARIOS=ender-chest-reopen after canonical Ender chest setup exists"
        );
    }
    if !selected.contains("cursor-crafting-departure")
        && !selected.contains("cursor-crafting-departure-saturated")
    {
        eprintln!(
            "untested cursor-crafting-departure: select PUMPKIN_E2E_SPEC_SCENARIOS=cursor-crafting-departure after exact-token Close setup exists"
        );
    }
    if !selected.contains("login-plugin-cancel") {
        eprintln!(
            "untested login-plugin-cancel: select PUMPKIN_E2E_SPEC_SCENARIOS=login-plugin-cancel after async login cancellation hook exists"
        );
    }
    if !selected.contains("login-plugin-delay") {
        eprintln!(
            "untested login-plugin-delay: select PUMPKIN_E2E_SPEC_SCENARIOS=login-plugin-delay after a 3-second async login callback hook exists"
        );
    }
    if !selected.contains("pinned-radius-view-change") {
        eprintln!(
            "untested pinned-radius-view-change: select PUMPKIN_E2E_SPEC_SCENARIOS=pinned-radius-view-change to change client view radius after immutable target pinning"
        );
    }
    for id in [
        "membership-direct-mesh",
        "membership-ack-ten-seconds",
        "membership-expand-set",
        "membership-peer-removal",
        "membership-peer-replacement",
        "membership-quic-resume",
        "protocol-version-reject",
    ] {
        if !selected.contains(id) {
            eprintln!(
                "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after admission-path and direct-peer isolation hooks exist"
            );
        }
    }
    Ok(())
}

async fn run_syncstats(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/syncstats all");
    within(deadline, "/syncstats all numeric totals", async {
        let mut totals = false;
        let mut ticks = false;
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) => {
                    let message = chat.message().to_string();
                    if let Some(values) = message.strip_prefix("Sync stats: ") {
                        let parts = values.split_whitespace().collect::<Vec<_>>();
                        if parts.len() < 9 {
                            bail!("/syncstats all omitted numeric counts: {message}");
                        }
                        let tracked = parts[0].parse::<usize>()?;
                        let pending = parts[3].parse::<usize>()?;
                        let affected = parts[7].parse::<usize>()?;
                        if tracked == 0 || affected > tracked || pending < affected {
                            bail!("/syncstats all counts are inconsistent: {message}");
                        }
                        totals = true;
                    }
                    if let Some(values) = message.strip_prefix("Disciplined global tick ") {
                        let first = values
                            .split(',')
                            .next()
                            .context("/syncstats global tick missing")?;
                        let _: u16 = first.parse()?;
                        let _: usize = message
                            .split(", ")
                            .last()
                            .context("/syncstats lag missing")?
                            .split_whitespace()
                            .next()
                            .context("/syncstats lag value missing")?
                            .parse::<usize>()?;
                        ticks = true;
                    }
                    if totals && ticks {
                        return Ok(());
                    }
                }
                Event::Disconnect(reason) => {
                    bail!("/syncstats all disconnected client: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("/syncstats all event stream ended before numeric values arrived")
    })
    .await?;
    actor.chat("/syncstats chunk 0 0");
    let explicit = wait_syncstats_chunk(events, deadline, "/syncstats chunk coordinate").await?;
    if !explicit.starts_with("Chunk [0, 0]:") {
        bail!("/syncstats explicit chunk reported the wrong coordinates: {explicit}");
    }
    actor.chat("/syncstats chunk");
    let current = wait_syncstats_chunk(events, deadline, "/syncstats current chunk").await?;
    if !current.starts_with("Chunk [") {
        bail!("/syncstats current chunk omitted coordinates: {current}");
    }
    eprintln!("passed syncstats: all and explicit/current chunk outputs reached the Java client");
    Ok(())
}

async fn wait_syncstats_chunk(
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<String> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) => {
                    let message = chat.message().to_string();
                    if let Some((_, values)) = message.split_once("]: ")
                        && message.starts_with("Chunk [")
                    {
                        let _: usize = values
                            .split_whitespace()
                            .next()
                            .context("chunk pending count absent")?
                            .parse()?;
                        return Ok(message);
                    }
                }
                Event::Disconnect(reason) => bail!("{stage} disconnected client: {reason:?}"),
                _ => {}
            }
        }
        bail!("{stage} event stream closed before numeric chunk statistics")
    })
    .await
}

pub(super) fn validate_world_scenario_records(logs: &[(u16, String)]) -> Result<()> {
    validate_health_policy_records(logs)?;
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect::<HashSet<_>>();
    for id in selected {
        if id == "initial-avatar-inventory" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = &logs
                    .iter()
                    .find(|(peer, _)| *peer == node)
                    .context("avatar inventory holder log missing")?
                    .1;
                let records = log
                    .lines()
                    .map(strip_ansi)
                    .filter(|line| line.contains("cluster e2e initial avatar inventory settled"))
                    .collect::<Vec<_>>();
                if records.len() != 1 {
                    bail!(
                        "node {node} must publish exactly one initial avatar lifecycle inventory settlement"
                    );
                }
                let record = &records[0];
                for (field, expected) in [
                    ("inventory_in_lifecycle", "true"),
                    ("observer_chunk_preheld", "true"),
                    ("initial_main_count", "2"),
                    ("initial_boots", "iron_boots"),
                    ("initial_offhand", "shield"),
                    ("first_selected", "1"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&record, field) != Some(expected) {
                        bail!("initial avatar inventory proof {field} differs on {node}: {record}");
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("initial avatar ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("initial avatar inventories did not converge");
            }
            continue;
        }
        if id == "creative-clone-source-cursor" {
            let mut hashes = HashSet::new();
            let mut capture_identities = HashSet::new();
            for node in [0, 1, 2] {
                let log = &logs
                    .iter()
                    .find(|(peer, _)| *peer == node)
                    .context("creative clone holder log absent")?
                    .1;
                let records = log
                    .lines()
                    .map(strip_ansi)
                    .filter(|line| line.contains("cluster e2e creative clone settled"))
                    .collect::<Vec<_>>();
                if records.len() != 1 {
                    bail!("node {node} must publish exactly one creative clone settled record");
                }
                for (field, expected) in [
                    ("source_at_replay", "oak_planks"),
                    ("first_cursor_count", "64"),
                    ("second_clone_rejected", "true"),
                    ("final_cursor", "stone:1"),
                    ("partial_writes", "0"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&records[0], field) != Some(expected) {
                        bail!(
                            "creative clone ground proof {field} differs on node {node}: {}",
                            records[0]
                        );
                    }
                }
                if structured_value(&records[0], "original_graph_captures") != Some("2") {
                    bail!("creative clone was not captured in both original-tick graphs on {node}");
                }
                let seed: u64 = structured_value(&records[0], "cluster_seed")
                    .context("creative clone cluster seed absent")?
                    .parse()?;
                for phase in ["source", "cursor"] {
                    let tick: u16 = structured_value(&records[0], &format!("{phase}_clone_tick"))
                        .context("creative clone original tick absent")?
                        .parse()?;
                    let competing_tick: u16 =
                        structured_value(&records[0], &format!("{phase}_competitor_tick"))
                            .context("creative competing tick absent")?
                            .parse()?;
                    let clone_actor = parse_fixture_actor(
                        structured_value(&records[0], &format!("{phase}_clone_actor"))
                            .context("creative clone actor absent")?,
                    )?;
                    let competitor_actor = parse_fixture_actor(
                        structured_value(&records[0], &format!("{phase}_competitor_actor"))
                            .context("creative competitor actor absent")?,
                    )?;
                    for (role, actor) in [("clone", clone_actor), ("competitor", competitor_actor)]
                    {
                        let id = structured_value(&records[0], &format!("{phase}_{role}_id"))
                            .context("creative original ActionId absent")?;
                        let parts = id.split('/').collect::<Vec<_>>();
                        if parts.len() != 4 || parse_fixture_actor(parts[0])? != actor {
                            bail!(
                                "creative {phase} {role} ActionId has wrong actor on {node}: {id}"
                            );
                        }
                        let _: u64 = parts[1].parse()?;
                        let _: u16 = parts[2].parse()?;
                        let _: u8 = parts[3].parse()?;
                        capture_identities.insert(format!("{seed}/{phase}/{tick}/{role}/{id}"));
                    }
                    if tick != competing_tick
                        || clone_actor == competitor_actor
                        || (
                            fixture_actor_rank(seed, tick, competitor_actor),
                            competitor_actor,
                        ) <= (fixture_actor_rank(seed, tick, clone_actor), clone_actor)
                    {
                        bail!(
                            "creative {phase} replay lacks a higher-priority same-tick competitor on {node}: {}",
                            records[0]
                        );
                    }
                }
                hashes.insert(
                    structured_value(&records[0], "ground_hash")
                        .context("creative clone ground hash missing")?
                        .to_owned(),
                );
            }
            if capture_identities.len() != 4 {
                bail!("creative original ActionRefs disagree between holders");
            }
            if hashes.len() != 1 {
                bail!("creative clone holders diverged");
            }
            continue;
        }
        if id == "item-drop-pickup" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = &logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .context("drop/pickup node log absent")?
                    .1;
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e item drop pickup settled"))
                    .context("drop/pickup ownership ground proof absent")?;
                for (field, expected) in [
                    ("drop_owner", "0"),
                    ("physics_owner", "0"),
                    ("pickup_capture_peer", "2"),
                    ("source_count", "1"),
                    ("collector_count", "1"),
                    ("spawned_items", "1"),
                    ("collected_items", "1"),
                    ("secondary_item_ticks", "0"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&record, field) != Some(expected) {
                        bail!(
                            "item drop/pickup ownership or conservation failed on node {node}: {record}"
                        );
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("drop/pickup ground hash missing")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("drop/pickup ground diverged across holders");
            }
            continue;
        }
        if id == "live-lease-reconnect" {
            let mut identities = HashSet::new();
            for node in [0, 1, 2] {
                let log = &logs
                    .iter()
                    .find(|(id, _)| *id == node)
                    .context("lease node log absent")?
                    .1;
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e live lease reconnect settled"))
                    .context("lease reconnect ground proof absent")?;
                let old_gid =
                    structured_value(&record, "old_gid").context("old lease GID absent")?;
                let new_gid =
                    structured_value(&record, "new_gid").context("new lease GID absent")?;
                let old_incarnation = structured_value(&record, "old_incarnation")
                    .context("old incarnation absent")?
                    .parse::<u64>()?;
                let new_incarnation = structured_value(&record, "new_incarnation")
                    .context("new incarnation absent")?
                    .parse::<u64>()?;
                if old_gid != new_gid || old_incarnation == new_incarnation {
                    bail!(
                        "lease fixture did not actually reuse GID across live incarnations: {record}"
                    );
                }
                if structured_value(&record, "uuid")
                    != Some(offline_uuid(super::BOT_C).to_string().as_str())
                    || structured_value(&record, "stale_action_rejected") != Some("true")
                    || structured_value(&record, "new_controller_mutations") != Some("0")
                    || structured_value(&record, "new_inventory_mutations") != Some("0")
                    || structured_value(&record, "new_ground_mutations") != Some("0")
                    || (structured_value(&record, "peer_action_reply_evidence")
                        != Some("static_schema")
                        || structured_value(&record, "peer_action_reply_protocol_variants")
                            != Some("0"))
                {
                    bail!(
                        "stale live lease mutated the replacement player on node {node}: {record}"
                    );
                }
                identities.insert((
                    old_gid.to_owned(),
                    old_incarnation,
                    new_incarnation,
                    structured_value(&record, "ground_hash")
                        .context("lease ground hash absent")?
                        .to_owned(),
                ));
            }
            if identities.len() != 1 {
                bail!("lease identity/ground evidence diverged across peers");
            }
            continue;
        }
        if id == "handoff-revision-mismatch" {
            let uuid = offline_uuid(super::BOT_E).to_string();
            let diagnostics = logs
                .iter()
                .flat_map(|(_, log)| log.lines().map(strip_ansi))
                .filter(|line| {
                    line.contains(&uuid)
                        && (line.contains("playerdata handoff revision mismatch")
                            || line.contains("unavailable playerdata revision mismatch"))
                })
                .collect::<Vec<_>>();
            let [diagnostic] = diagnostics.as_slice() else {
                bail!(
                    "revision mismatch must emit exactly one diagnostic, observed {}",
                    diagnostics.len()
                );
            };
            let expected = structured_value(diagnostic, "expected")
                .context("revision mismatch expected revision absent")?
                .parse::<u64>()?;
            let received = structured_value(diagnostic, "received")
                .context("revision mismatch received revision absent")?
                .parse::<u64>()?;
            if expected == received {
                bail!("revision fault did not actually change the handoff revision");
            }
            let cleanup = logs
                .iter()
                .flat_map(|(_, log)| log.lines().map(strip_ansi))
                .find(|line| {
                    line.contains("cluster e2e revision mismatch cleanup")
                        && structured_value(line, "uuid") == Some(uuid.as_str())
                })
                .context("revision mismatch has no cleanup observation")?;
            for field in [
                "player_registered",
                "inventory_installed",
                "replica_registered",
                "target_rechosen",
            ] {
                if structured_value(&cleanup, field) != Some("false") {
                    bail!("revision mismatch left partial state or retargeted: {cleanup}");
                }
            }
            continue;
        }
        if id == "inventory-pickup-rollback-split" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} pickup rollback log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e pickup rollback split settled"))
                    .with_context(|| format!("node {node} has no pickup/split ground outcome"))?;
                for (field, expected) in [
                    ("pickup_winner", "B"),
                    ("a_slot", "1"),
                    ("a_cursor", "1"),
                    ("b_count", "2"),
                    ("item_total", "4"),
                    ("dependent_split_rebased", "true"),
                    ("queue_replays", "0"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&record, field) != Some(expected) {
                        bail!(
                            "pickup rollback did not rebase dependent split {field} on node {node}: {record}"
                        );
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("pickup/split ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("pickup rollback ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "inventory-component-identity" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} component identity log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e inventory components settled"))
                    .with_context(|| {
                        format!("node {node} has no inventory component ground outcome")
                    })?;
                if structured_value(&record, "alpha_count") != Some("2")
                    || structured_value(&record, "beta_count") != Some("2")
                    || structured_value(&record, "cursor_count") != Some("0")
                    || (structured_value(&record, "peer_action_reply_evidence")
                        != Some("static_schema")
                        || structured_value(&record, "peer_action_reply_protocol_variants")
                            != Some("0"))
                {
                    bail!(
                        "inventory component identity was lost or merged on node {node}: {record}"
                    );
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("component identity ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("component identity ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "visual-break-start-abort" {
            for node in [1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} break visual log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e block breaking visuals settled"))
                    .with_context(|| format!("node {node} has no break visual route outcome"))?;
                for field in ["started", "aborted", "block_unchanged"] {
                    if structured_value(&record, field) != Some("true") {
                        bail!("break visual omitted {field} on node {node}: {record}");
                    }
                }
            }
            continue;
        }
        if id == "sweeping-fire-effects" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} sweep/fire log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e sweeping fire settled"))
                    .with_context(|| format!("node {node} has no sweep/fire ground outcome"))?;
                for (field, expected) in [
                    ("main_targets", "1"),
                    ("sweep_targets", "2"),
                    ("ignited_targets", "1"),
                    ("durability_consumed", "1"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&record, field) != Some(expected) {
                        bail!("sweep/fire omitted {field} on node {node}: {record}");
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("sweep/fire ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("sweep/fire ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "mining-break" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} mining log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e mining break settled"))
                    .with_context(|| {
                        format!("node {node} has no accepted mining ground outcome")
                    })?;
                for (field, expected) in [
                    ("broken", "1"),
                    ("drop_spawned", "1"),
                    ("drop_collected", "1"),
                    ("dirt_count", "1"),
                    ("tool_damage", "2"),
                    ("statistic_increment", "1"),
                    ("peer_action_reply_evidence", "static_schema"),
                    ("peer_action_reply_protocol_variants", "0"),
                ] {
                    if structured_value(&record, field) != Some(expected) {
                        bail!("mining ground outcome omits {field} on node {node}: {record}");
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("mining ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("mining ground diverged: {hashes:?}");
            }
            continue;
        }
        if matches!(
            id.as_str(),
            "packet-preflight-lobby" | "packet-preflight-disconnect"
        ) {
            let log = &logs
                .iter()
                .find(|(node, _)| *node == 1)
                .context("packet-preflight owner log absent")?
                .1;
            let record = log
                .lines()
                .map(strip_ansi)
                .find(|line| {
                    line.contains("cluster e2e packet preflight departure committed")
                        && structured_value(line, "scenario") == Some(id.as_str())
                })
                .with_context(|| {
                    format!("{id} has no accepted durable preflight departure record")
                })?;
            if structured_value(&record, "uuid") != Some(offline_uuid(BOT_A).to_string().as_str())
                || structured_value(&record, "action_server") != Some("1")
                || structured_value(&record, "action_family") != Some("7")
                || structured_value(&record, "action_incarnation")
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_none_or(|value| value == 0)
                || ["action_tick", "action_sequence", "action_player"]
                    .iter()
                    .any(|field| {
                        structured_value(&record, field)
                            .and_then(|value| value.parse::<u16>().ok())
                            .is_none()
                    })
            {
                bail!(
                    "packet preflight completion lacks A's exact canonical ActionRef identity: {record}"
                );
            }
            if id == "packet-preflight-lobby"
                && structured_value(&record, "departure_trigger") != Some("other_operator")
            {
                bail!("manual preflight test did not use an independent operator: {record}");
            }
            if structured_value(&record, "callback_started") != Some("true")
                || structured_value(&record, "departure_during_callback") != Some("true")
                || structured_value(&record, "callback_before_commit") != Some("true")
                || structured_value(&record, "action_accepted") != Some("true")
                || structured_value(&record, "revision")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_none_or(|v| v == 0)
                || structured_value(&record, "callback_ms")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_none_or(|v| v < 500)
            {
                bail!(
                    "packet preflight receiver was lost or logout preceded accepted callback state: {record}"
                );
            }
            continue;
        }
        if id == "health-visual-exemption" {
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} health visual log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e health visual exemption"))
                    .with_context(|| {
                        format!("node {node} did not publish health visual policy outcome")
                    })?;
                if structured_value(&record, "policy") != Some("all")
                    || structured_value(&record, "golem_actual") != Some("10")
                    || structured_value(&record, "golem_sent") != Some("10")
                    || structured_value(&record, "zombie_actual") != Some("6")
                    || structured_value(&record, "zombie_sent") != Some("20")
                    || structured_value(&record, "golem_visual_derived_from_health") != Some("true")
                {
                    bail!(
                        "health-driven visual was masked or ordinary mob health leaked on node {node}: {record}"
                    );
                }
            }
            continue;
        }
        if id == "book-plugin-order" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} book-order log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e book plugin ordered"))
                    .with_context(|| {
                        format!("node {node} did not publish accepted book/item order")
                    })?;
                if structured_value(&record, "book_page") != Some("plugin-edited")
                    || structured_value(&record, "item_after_book") != Some("true")
                    || structured_value(&record, "block_placed") != Some("true")
                    || (node == 1
                        && structured_value(&record, "callback_ms")
                            .and_then(|value| value.parse::<u64>().ok())
                            .is_none_or(|duration| duration < 3_000))
                {
                    bail!(
                        "book plugin preflight did not delay and precede item action on node {node}: {record}"
                    );
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("book/item accepted ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("book/item accepted ground differs across peers: {hashes:?}");
            }
            continue;
        }
        if matches!(
            id.as_str(),
            "membership-peer-replacement" | "membership-quic-resume" | "protocol-version-reject"
        ) {
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} {id} log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| {
                        line.contains("cluster e2e peer incarnation scenario")
                            && structured_value(line, "scenario") == Some(id.as_str())
                    })
                    .with_context(|| format!("node {node} has no {id} peer-incarnation record"))?;
                if id == "membership-peer-replacement" {
                    if structured_value(&record, "early_rejected") != Some("true")
                        || structured_value(&record, "old_keepalive_gap_ms")
                            .and_then(|value| value.parse::<u64>().ok())
                            .is_none_or(|gap| gap < 3_000)
                        || structured_value(&record, "old_peer_dropped") != Some("true")
                        || structured_value(&record, "gap_fill_complete") != Some("true")
                    {
                        bail!(
                            "{id} violated the three-second identity handoff or dropped-peer gap fill on node {node}: {record}"
                        );
                    }
                } else if id == "membership-quic-resume" {
                    if structured_value(&record, "same_connection") != Some("true")
                        || structured_value(&record, "drop_count") != Some("0")
                        || structured_value(&record, "sync_generation_unchanged") != Some("true")
                    {
                        bail!(
                            "QUIC resume was treated as a new peer or dropped sync state on node {node}: {record}"
                        );
                    }
                } else if structured_value(&record, "incompatible_rejected") != Some("true")
                    || structured_value(&record, "incompatible_admitted") != Some("false")
                    || structured_value(&record, "live_peers_unchanged") != Some("true")
                {
                    bail!(
                        "mixed cluster wire version was not rejected before membership/gameplay on node {node}: {record}"
                    );
                }
            }
            continue;
        }
        if id == "inventory-conflict" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} inventory conflict log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e inventory conflict settled"))
                    .with_context(|| {
                        format!("node {node} did not publish inventory conflict ground outcome")
                    })?;
                if structured_value(&record, "accepted") != Some("1")
                    || structured_value(&record, "rejected") != Some("1")
                    || structured_value(&record, "remaining") != Some("0")
                {
                    bail!(
                        "node {node} inventory conflict outcome violated single-winner conservation: {record}"
                    );
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("inventory conflict ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!(
                    "inventory conflict accepted ground differs between cluster nodes: {hashes:?}"
                );
            }
            continue;
        }
        if id == "inventory-placement-conflict" {
            let mut hashes = HashSet::new();
            let mut winner = None;
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} inventory-placement log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e inventory placement conflict settled"))
                    .with_context(|| {
                        format!("node {node} did not publish joint item/world conflict outcome")
                    })?;
                let result =
                    structured_value(&record, "winner").context("joint conflict winner absent")?;
                if !matches!(result, "place" | "move")
                    || structured_value(&record, "same_tick") != Some("true")
                    || structured_value(&record, "item_total") != Some("1")
                {
                    bail!(
                        "node {node} inventory/world conflict violated atomicity or conservation: {record}"
                    );
                }
                if winner.as_deref().is_some_and(|prior| prior != result) {
                    bail!("joint inventory/world winner diverged: {record}");
                }
                winner = Some(result.to_owned());
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("joint conflict ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("joint inventory/world ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "food-completion" || id == "food-abort" {
            let completed = id == "food-completion";
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} food log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| {
                        line.contains("cluster e2e food use settled")
                            && structured_value(line, "scenario") == Some(id.as_str())
                    })
                    .with_context(|| format!("node {node} has no {id} ground outcome"))?;
                if structured_value(&record, "apple_count")
                    != Some(if completed { "1" } else { "2" })
                    || structured_value(&record, "food")
                        != Some(if completed { "14" } else { "10" })
                    || structured_value(&record, "consumed")
                        != Some(if completed { "1" } else { "0" })
                    || (structured_value(&record, "peer_action_reply_evidence")
                        != Some("static_schema")
                        || structured_value(&record, "peer_action_reply_protocol_variants")
                            != Some("0"))
                {
                    bail!(
                        "{id} consumption/abort did not settle atomically on node {node}: {record}"
                    );
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("food ground hash missing")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("food/inventory ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "projectile-effects" || id == "crossbow-effects" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} projectile log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| {
                        line.contains("cluster e2e projectile effects settled")
                            && structured_value(line, "scenario") == Some(id.as_str())
                    })
                    .with_context(|| {
                        format!("node {node} did not publish projectile ground outcome")
                    })?;
                if structured_value(&record, "accepted_projectiles") != Some("1")
                    || structured_value(&record, "arrow_consumed") != Some("1")
                    || structured_value(&record, "target_damage") != Some("1")
                    || (structured_value(&record, "peer_action_reply_evidence")
                        != Some("static_schema")
                        || structured_value(&record, "peer_action_reply_protocol_variants")
                            != Some("0"))
                    || (id == "crossbow-effects"
                        && structured_value(&record, "charged_before_fire") != Some("true"))
                {
                    bail!("node {node} projectile action did not settle exactly once: {record}");
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("projectile ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("projectile ground diverged: {hashes:?}");
            }
            continue;
        }
        if id == "combat-effects" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} combat outcome log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e combat effects settled"))
                    .with_context(|| {
                        format!("node {node} did not publish accepted combat outcome")
                    })?;
                for field in [
                    "damage",
                    "knockback",
                    "durability",
                    "cooldown",
                    "statistics",
                    "advancement",
                ] {
                    if structured_value(&record, field) != Some("accepted") {
                        bail!("node {node} combat ground outcome omitted {field}: {record}");
                    }
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("combat ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("combat accepted ground state diverged across nodes: {hashes:?}");
            }
            continue;
        }
        if id == "virtual-crafting" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} virtual crafting log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e virtual crafting settled"))
                    .with_context(|| {
                        format!("node {node} did not publish accepted virtual crafting outcome")
                    })?;
                if structured_value(&record, "planks_consumed") != Some("2")
                    || structured_value(&record, "sticks_created") != Some("4")
                    || structured_value(&record, "accepted") != Some("true")
                {
                    bail!("node {node} virtual crafting failed semantic conservation: {record}");
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("virtual crafting ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("virtual crafting ground state diverged across peers: {hashes:?}");
            }
            continue;
        }
        if id == "invsee-quickcraft" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} remote QuickCraft log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e invsee quickcraft settled"))
                    .with_context(|| {
                        format!("node {node} did not publish accepted remote QuickCraft outcome")
                    })?;
                if structured_value(&record, "source_consumed") != Some("4")
                    || structured_value(&record, "destination_a") != Some("2")
                    || structured_value(&record, "destination_b") != Some("2")
                    || structured_value(&record, "accepted") != Some("true")
                {
                    bail!("node {node} remote QuickCraft violated conservation: {record}");
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("QuickCraft ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("remote QuickCraft ground diverged: {hashes:?}");
            }
            continue;
        }
        if matches!(
            id.as_str(),
            "chat-departure-delay" | "command-departure-delay"
        ) {
            let kind = if id == "chat-departure-delay" {
                "chat"
            } else {
                "command"
            };
            let log = logs
                .iter()
                .find(|(candidate, _)| *candidate == 1)
                .context("departing actor node log missing")?
                .1
                .as_str();
            let record = log
                .lines()
                .map(strip_ansi)
                .find(|line| {
                    line.contains("cluster e2e departure queued work settled")
                        && structured_value(line, "kind") == Some(kind)
                })
                .with_context(|| {
                    format!("missing accepted delayed {kind} before departure commit")
                })?;
            if structured_value(&record, "accepted") != Some("true")
                || structured_value(&record, "before_commit") != Some("true")
                || structured_value(&record, "callback_delay_ms")
                    .and_then(|ms| ms.parse::<u64>().ok())
                    .is_none_or(|ms| ms < 3_000)
            {
                bail!("delayed {kind} did not settle before logout commit: {record}");
            }
            validate_retirement_reconnect_barrier(log, &record)?;
            continue;
        }
        if id == "entity-owner-leave" {
            let mut hashes = HashSet::new();
            for node in [0, 1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} ownership transfer log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| line.contains("cluster e2e entity ownership transfer settled"))
                    .with_context(|| {
                        format!("node {node} did not publish entity transfer ground outcome")
                    })?;
                if structured_value(&record, "old_owner") != Some("1")
                    || structured_value(&record, "new_owner") != Some("2")
                    || structured_value(&record, "accepted") != Some("true")
                {
                    bail!("node {node} entity ownership transfer invalid: {record}");
                }
                hashes.insert(
                    structured_value(&record, "ground_hash")
                        .context("entity transfer ground hash absent")?
                        .to_owned(),
                );
            }
            if hashes.len() != 1 {
                bail!("entity transfer ground state diverged: {hashes:?}");
            }
            continue;
        }
        if matches!(
            id.as_str(),
            "membership-direct-mesh"
                | "membership-ack-ten-seconds"
                | "membership-expand-set"
                | "membership-peer-removal"
        ) {
            let log = logs
                .iter()
                .find(|(candidate, _)| *candidate == 2)
                .context("node2 membership admission log missing")?
                .1
                .as_str();
            let record = log
                .lines()
                .map(strip_ansi)
                .find(|line| {
                    line.contains("cluster e2e admission path")
                        && structured_value(line, "candidate") == Some("2")
                })
                .context("node2 did not publish dynamic membership admission path")?;
            let elapsed = structured_value(&record, "elapsed_ms")
                .context("admission elapsed duration absent")?
                .parse::<u64>()?;
            if id == "membership-direct-mesh" {
                if structured_value(&record, "path") != Some("direct") || elapsed >= 10_000 {
                    bail!("full direct QUIC mesh failed immediate admission: {record}");
                }
            } else if id == "membership-peer-removal" {
                if structured_value(&record, "path") != Some("required_peer_removed")
                    || structured_value(&record, "required_before") != Some("0,1")
                    || structured_value(&record, "required_after") != Some("0")
                    || structured_value(&record, "direct_blocked") != Some("true")
                    || elapsed >= 10_000
                {
                    bail!(
                        "removing a disconnected required peer stalled candidate admission: {record}"
                    );
                }
            } else if structured_value(&record, "path") != Some("ack_timeout")
                || elapsed < 10_000
                || elapsed > 15_000
                || structured_value(&record, "direct_blocked") != Some("true")
                || (id == "membership-expand-set"
                    && (structured_value(&record, "required_before") != Some("0")
                        || structured_value(&record, "required_after") != Some("0,1")
                        || structured_value(&record, "timer_restarted") != Some("false")))
            {
                bail!(
                    "acknowledged dynamic membership did not admit after ten seconds without full mesh: {record}"
                );
            }
            continue;
        }
        if id == "syncstats"
            || id == "health-mask"
            || id == "health-mask-mount"
            || id == "invsee-number-swap"
            || id == "invsee-stale-window"
            || id == "invsee-refresh-r2-r3"
            || id == "invsee-wrap-window"
            || id == "inventory-move-split-merge"
            || id == "untrusted-result-reject"
            || id == "acceptance-delay"
            || id == "acceptance-cooldown"
            || id == "item-drop-pickup"
            || id == "invsee-armor-offhand"
            || id == "container-transfer"
            || id == "ender-chest-reopen"
            || id == "cursor-crafting-departure"
            || id == "cursor-crafting-departure-saturated"
            || id == "login-plugin-cancel"
            || id == "login-plugin-delay"
            || id == "handoff-callback-early-confirm"
            || id == "pinned-radius-view-change"
            || id == "chat-departure-delay"
            || id == "command-departure-delay"
        {
            continue;
        }
        if id == "datagram-loss-reorder" || id == "datagram-multi-batch" {
            for node in [1, 2] {
                let log = logs
                    .iter()
                    .find(|(candidate, _)| *candidate == node)
                    .with_context(|| format!("missing node {node} datagram log"))?
                    .1
                    .as_str();
                let record = log
                    .lines()
                    .map(strip_ansi)
                    .find(|line| {
                        line.contains("cluster e2e datagram movement result")
                            && structured_value(line, "scenario") == Some(id.as_str())
                    })
                    .with_context(|| {
                        format!("node {node} did not report transport loss/reorder evidence")
                    })?;
                let dropped = structured_value(&record, "dropped")
                    .context("datagram drop count absent")?
                    .parse::<usize>()?;
                let reordered = structured_value(&record, "reordered")
                    .context("datagram reorder count absent")?
                    .parse::<usize>()?;
                let retransmits = structured_value(&record, "movement_retransmits")
                    .context("movement retransmit count absent")?
                    .parse::<usize>()?;
                let largest = structured_value(&record, "largest_udp_bytes")
                    .context("UDP size absent")?
                    .parse::<usize>()?;
                if id == "datagram-loss-reorder"
                    && (dropped == 0 || reordered == 0 || retransmits != 0 || largest > 1200)
                {
                    bail!("datagram loss/reorder invariant failed on node {node}: {record}");
                }
                if id == "datagram-multi-batch"
                    && (structured_value(&record, "same_tick") != Some("true")
                        || structured_value(
                            &record,
                            if node == 1 {
                                "datagrams"
                            } else {
                                "received_datagrams"
                            },
                        )
                        .and_then(|value| value.parse::<usize>().ok())
                        .is_none_or(|count| count < 2)
                        || structured_value(&record, "split_records") != Some("0")
                        || largest > 1200)
                {
                    bail!(
                        "movement records were not split into independent single-UDP datagrams on node {node}: {record}"
                    );
                }
            }
            continue;
        }
        if id == "holder-only-routing" || id == "entity-holder-only-routing" {
            let log = &logs
                .iter()
                .find(|(candidate, _)| *candidate == 2)
                .context("missing nonholder node2 log")?
                .1;
            let record = log
                .lines()
                .map(strip_ansi)
                .find(|line| {
                    line.contains("cluster e2e holder-only routing result")
                        && structured_value(line, "scenario") == Some(id.as_str())
                })
                .context("nonholder node did not report actor routing evidence")?;
            if structured_value(&record, "movement_while_nonholder") != Some("0")
                || structured_value(&record, "visual_while_nonholder") != Some("0")
                || structured_value(&record, "snapshot_before_new_datagram") != Some("true")
                || (id == "entity-holder-only-routing"
                    && structured_value(&record, "owner_unchanged") != Some("true"))
            {
                bail!("holder-only movement/visual or boundary snapshot ordering failed: {record}");
            }
            continue;
        }
        let mut agreed = None;
        let scenario_nodes: &[u16] = if id == "relay-drop-newest-ground-election" {
            &[0, 1, 2, 4]
        } else {
            &[0, 1, 2]
        };
        for &node in scenario_nodes {
            let log = logs
                .iter()
                .find(|(candidate, _)| *candidate == node)
                .with_context(|| format!("missing node {node} log for {id}"))?
                .1
                .as_str();
            let records = log
                .lines()
                .map(strip_ansi)
                .filter(|line| {
                    line.contains("cluster e2e action scenario settled")
                        && structured_value(line, "scenario") == Some(id.as_str())
                })
                .collect::<Vec<_>>();
            if records.len() != 1 {
                bail!(
                    "{id} must publish exactly one accepted-ground record on node {node}, got {}",
                    records.len()
                );
            }
            let record = &records[0];
            let ground_hash = structured_value(&record, "ground_hash")
                .context("ground hash absent from action scenario record")?;
            let tick = structured_value(&record, "tick")
                .context("tick absent from action scenario record")?;
            let actions = structured_value(&record, "actions")
                .context("action count absent from action scenario record")?
                .parse::<usize>()
                .context("invalid action count in action scenario record")?;
            let replies = if matches!(
                id.as_str(),
                "door-trapdoor" | "button-lever" | "end-frame-anchor"
            ) {
                if structured_value(&record, "peer_action_reply_evidence") != Some("static_schema")
                    || structured_value(&record, "peer_action_reply_protocol_variants") != Some("0")
                    || actions != 2
                {
                    bail!(
                        "interaction record must identify static protocol reply evidence: {record}"
                    );
                }
                0
            } else {
                if structured_value(&record, "peer_action_reply_evidence") != Some("static_schema")
                    || structured_value(&record, "peer_action_reply_protocol_variants") != Some("0")
                {
                    bail!(
                        "action scenario lacks explicit static protocol reply evidence: {record}"
                    );
                }
                0
            };
            if replies != 0 || actions < if id == "conflict-three-place" { 3 } else { 1 } {
                bail!("{id} on node {node} has invalid action/reply evidence: {record}");
            }
            if id == "conflict-two-place" || id == "conflict-three-place" {
                if structured_value(&record, "accepted_tick_batches") != Some("1") {
                    bail!("{id} failed one-batch tick acceptance on node {node}: {record}");
                }
                let seed = structured_value(&record, "cluster_seed")
                    .context("conflict cluster seed absent")?
                    .parse::<u64>()?;
                let tick_value = tick.parse::<u16>()?;
                let actors = structured_value(&record, "actor_ids")
                    .context("conflict actor IDs absent")?
                    .split(',')
                    .map(parse_fixture_actor)
                    .collect::<Result<Vec<_>>>()?;
                let expected_count = if id == "conflict-three-place" { 3 } else { 2 };
                if actors.len() != expected_count
                    || actors.iter().copied().collect::<HashSet<_>>().len() != expected_count
                {
                    bail!("{id} did not use {expected_count} distinct actors: {record}");
                }
                let expected_winner = actors
                    .iter()
                    .copied()
                    .max_by_key(|actor| (fixture_actor_rank(seed, tick_value, *actor), *actor))
                    .context("conflict actor set empty")?;
                let winner = parse_fixture_actor(
                    structured_value(&record, "winner_actor")
                        .context("conflict winner actor absent")?,
                )?;
                if winner != expected_winner {
                    bail!(
                        "{id} winner diverged from independent xxhash/tie-break calculation on node {node}: {record}"
                    );
                }
            }
            if id == "conflict-three-place" {
                let expected_arrivals = ["ab", "bc", "ca"][node as usize];
                if structured_value(&record, "initial_actions") != Some(expected_arrivals)
                    || structured_value(&record, "late_missing_actions") != Some("1")
                    || structured_value(&record, "ground_promoted") != Some("true")
                {
                    bail!(
                        "three-way conflict did not prove incomplete same-tick delivery and later local settlement on node {node}: {record}"
                    );
                }
            }
            if id == "finish-before-actions"
                && (structured_value(&record, "manifest_arrived_first") != Some("true")
                    || structured_value(&record, "ground_promoted") != Some("true"))
            {
                bail!(
                    "holistic finish ordering was not exercised before actions on node {node}: {record}"
                );
            }
            if id == "dropped-peer-gap"
                && (structured_value(&record, "peer_dropped") != Some("true")
                    || structured_value(&record, "gaps_filled") != Some("true")
                    || structured_value(&record, "ground_promoted") != Some("true"))
            {
                bail!("dropped-peer gap recovery did not complete on node {node}: {record}");
            }
            if id == "targeted-undo"
                && ((node == 1
                    && (structured_value(&record, "losing_local_actions") != Some("1")
                        || structured_value(&record, "targeted_undo") != Some("1")))
                    || structured_value(&record, "queue_replays") != Some("0")
                    || structured_value(&record, "full_chunk_repairs") != Some("0"))
            {
                bail!(
                    "targeted action undo replayed a queue or repaired a whole chunk on node {node}: {record}"
                );
            }
            if id == "zero-action-manifest"
                && node != 2
                && structured_value(&record, "ground_blocked_before_zero_manifest") != Some("true")
            {
                bail!(
                    "ground advanced before peer2's holistic zero-action envelope on node {node}: {record}"
                );
            }
            let graph_fields: &[(&str, &str)] = match id.as_str() {
                "zero-action-manifest" => &[
                    ("zero_peer", "2"),
                    ("zero_peer_action_count", "0"),
                    ("zero_chunk_entry_omitted", "true"),
                    ("holistic_empty_envelope_received", "true"),
                    ("remaining_expected_actions", "0"),
                    ("ground_promoted", "true"),
                    ("additional_resolution_messages", "0"),
                ],
                "subscriber-gap-during-transfer" => &[
                    ("actions_during_snapshot", "true"),
                    ("manifest_during_snapshot", "true"),
                    ("relayed_late_pre_cutoff_action", "true"),
                    ("missing_actions", "0"),
                    ("duplicate_applies", "0"),
                    ("hold_after_atomic_install", "true"),
                ],
                "relay-drop-newest-ground-election" => &[
                    ("relay_dropped_before_complete", "true"),
                    ("newest_ground_selected", "true"),
                    ("late_better_offer_cancels_old_sender", "true"),
                    ("only_ground_actions_always_win", "true"),
                    ("cross_realm_delta_propagated", "true"),
                    ("missing_actions", "0"),
                ],
                "unsolicited-update-unsubscribe" => &[
                    ("unsolicited_actions_applied", "0"),
                    ("first_unsubscribe_sent", "true"),
                    ("same_chunk_2s_suppressed", "true"),
                    ("other_chunk_independent", "true"),
                    ("no_unsubscribe_ack", "true"),
                ],
                "actor-local-overlay-discard" => &[
                    ("actor_read_own_block_overlay", "true"),
                    ("actor_read_own_entity_cow", "true"),
                    ("overlay_discarded_after_actor", "true"),
                    ("next_actor_read_canonical_state", "true"),
                    ("local_submission_same_receiver_path", "true"),
                    ("client_output_before_resolution", "0"),
                ],
                "canonical-atomic-failure" => &[
                    ("failed_last_leg_condition", "true"),
                    ("earlier_leg_mutations", "0"),
                    ("inventory_consumed", "0"),
                    ("local_partial_writes", "0"),
                    ("ground_partial_writes", "0"),
                    ("local_apply_interpreter", "canonical"),
                    ("ground_apply_interpreter", "canonical"),
                ],
                "late-holder-unresolved-snapshot" => &[
                    ("snapshot_ground_installed", "true"),
                    ("snapshot_pending_imported", "true"),
                    ("snapshot_inventories_installed", "true"),
                    ("hold_after_atomic_install", "true"),
                    ("late_holder_observer_only", "true"),
                    ("retroactive_votes", "0"),
                ],
                "graph-shared-conditions" => &[
                    ("shared_condition", "true"),
                    ("both_actions_accepted", "true"),
                    ("false_conflicts", "0"),
                ],
                "late-dependent-replay" => &[
                    ("late_tick", "true"),
                    ("dependency_cascade", "true"),
                    ("replay_relocated", "true"),
                    ("earlier_always_wins", "true"),
                    ("delta_current_to_past", "true"),
                    ("world_mutated_during_resolution", "false"),
                ],
                "undo-redo-cascade" => &[
                    ("undo_wrapped", "true"),
                    ("undo_retained_in_graph", "true"),
                    ("redo_after_winner_undo", "true"),
                    ("delta_current_to_past", "true"),
                ],
                "counterfactual-ground-block" => &[
                    ("counterfactual_locations_finite", "true"),
                    ("ground_blocked_until_all_realms_done", "true"),
                    ("settled_graph_retained_until_tick_drained", "true"),
                ],
                "dropped-graph-recovery" => &[
                    ("done_graphs_imported", "true"),
                    ("only_ground_actions_always_win", "true"),
                    ("undone_graphs_preserved", "true"),
                    ("delta_repaired_local_truth", "true"),
                ],
                _ => &[],
            };
            if graph_fields
                .iter()
                .any(|(field, value)| structured_value(&record, field) != Some(*value))
            {
                bail!(
                    "{id} did not prove the specified graph/cascade/ground behavior on node {node}: {record}"
                );
            }
            if id == "subscriber-gap-during-transfer" {
                let cutoff = structured_value(&record, "snapshot_cutoff_tick")
                    .context("snapshot cutoff absent")?
                    .parse::<u16>()?;
                let subscription = structured_value(&record, "subscription_start_tick")
                    .context("subscription start absent")?
                    .parse::<u16>()?;
                if subscription != cutoff.wrapping_add(1) {
                    bail!(
                        "subscriber snapshot/stream has an overlapping or missing tick on node {node}: {record}"
                    );
                }
            }
            if id == "relay-drop-newest-ground-election" {
                let errors = log
                    .lines()
                    .map(strip_ansi)
                    .filter(|line| {
                        line.contains("cluster peer dropped recovery reconciled")
                            && structured_value(line, "peer") == Some("3")
                    })
                    .collect::<Vec<_>>();
                let [diagnostic] = errors.as_slice() else {
                    bail!(
                        "survivor {node} must emit exactly one actual dropped-relay recovery error, got {}",
                        errors.len()
                    );
                };
                if !diagnostic.contains("ERROR")
                    || structured_value(diagnostic, "reason").is_none_or(str::is_empty)
                    || structured_value(diagnostic, "gaps_reconciled")
                        .and_then(|value| value.parse::<usize>().ok())
                        .is_none_or(|count| count == 0)
                {
                    bail!(
                        "dropped relay recovery diagnostic lacks severity/reason/reconciled gaps: {diagnostic}"
                    );
                }
                let offers = structured_value(&record, "ground_offers")
                    .context("recovery ground offers absent")?
                    .split(',')
                    .map(|offer| {
                        let (peer, tick) =
                            offer.split_once(':').context("invalid recovery offer")?;
                        Ok((peer.parse::<u16>()?, tick.parse::<u16>()?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if offers.iter().map(|(peer, _)| *peer).collect::<HashSet<_>>()
                    != HashSet::from([0, 1, 4])
                    || offers.len() != 3
                    || offers
                        .iter()
                        .map(|(peer, _)| *peer)
                        .collect::<HashSet<_>>()
                        .len()
                        != 3
                {
                    bail!("recovery did not collect distinct ground offers: {record}");
                }
                let anchor = offers[0].1;
                let newest = offers
                    .iter()
                    .max_by_key(|(_, tick)| tick.wrapping_sub(anchor) as i16)
                    .context("empty recovery offers")?;
                if offers.iter().filter(|(_, tick)| *tick == newest.1).count() != 1 {
                    bail!(
                        "newest-ground fixture must provide unequal final offers to test the priority independently: {record}"
                    );
                }
                let winner = structured_value(&record, "recovery_sender")
                    .context("recovery sender absent")?
                    .parse::<u16>()?;
                if winner != newest.0 {
                    bail!(
                        "recovery chose an older ground snapshot instead of the independently selected newest on node {node}: {record}"
                    );
                }
            }
            if id == "tick-bank-backpressure"
                && ((node == 1
                    && (structured_value(&record, "delayed_ticks")
                        .and_then(|value| value.parse::<usize>().ok())
                        .is_none_or(|count| count == 0)
                        || structured_value(&record, "high_water")
                            .and_then(|value| value.parse::<usize>().ok())
                            .is_none_or(|count| count == 0)))
                    || structured_value(&record, "banks_per_worker") != Some("2")
                    || structured_value(&record, "actions_lost") != Some("0")
                    || structured_value(&record, "waits") != Some("0"))
            {
                bail!(
                    "tick-bank backpressure lost actions, waited, or allocated beyond two banks on node {node}: {record}"
                );
            }
            if id == "partial-ground-promotion"
                && (structured_value(&record, "resolved_subset") != Some("abc")
                    || structured_value(&record, "unresolved") != Some("d"))
            {
                bail!(
                    "partial ground promotion did not prove lower-priority d remained unresolved: {record}"
                );
            }
            if id == "cross-chunk-joint"
                && (structured_value(&record, "joint") != Some("true")
                    || structured_value(&record, "dependency_settled") != Some("true"))
            {
                bail!("joint cross-chunk settlement evidence missing on node {node}: {record}");
            }
            if id == "no-coholder-entity-joint"
                && (structured_value(&record, "coholder_at_issue") != Some("false")
                    || structured_value(&record, "dependency_notice") != Some("true")
                    || structured_value(&record, "primary_resolution_owner") != Some("true")
                    || structured_value(&record, "both_legs_loaded") != Some("true")
                    || structured_value(&record, "both_legs_ground") != Some("true")
                    || structured_value(&record, "primary_transaction_log_preserved")
                        != Some("true")
                    || structured_value(&record, "secondary_pending_unload_safe") != Some("true")
                    || (node == 0
                        && structured_value(&record, "retained_ms")
                            .and_then(|value| value.parse::<u64>().ok())
                            .is_none_or(|duration| duration < 5_000)))
            {
                bail!(
                    "no-coholder entity transaction failed joint primary resolution or five-second retention on node {node}: {record}"
                );
            }
            if id == "entity-chunk-transition"
                && (structured_value(&record, "old_ref_relocated") != Some("true")
                    || structured_value(&record, "applied_after_tick_actions") != Some("true")
                    || structured_value(&record, "effective_next_tick") != Some("true")
                    || structured_value(&record, "owners") != Some("1"))
            {
                bail!(
                    "entity chunk transition did not preserve next-tick ownership and old-chunk references: {record}"
                );
            }
            if id == "holder-drop-reroute"
                && node == 1
                && (structured_value(&record, "initial_requests") != Some("1")
                    || structured_value(&record, "replacement_requests") != Some("1")
                    || structured_value(&record, "timer_retries") != Some("0"))
            {
                bail!(
                    "holder drop did not trigger exactly one replacement request without timer retries: {record}"
                );
            }
            let conflict_signature = if id == "conflict-two-place" || id == "conflict-three-place" {
                let mut actors = structured_value(&record, "actor_ids")
                    .context("conflict actor IDs absent")?
                    .split(',')
                    .map(parse_fixture_actor)
                    .collect::<Result<Vec<_>>>()?;
                actors.sort_unstable();
                format!(
                    "{}:{actors:?}:{:?}",
                    structured_value(&record, "cluster_seed")
                        .context("conflict cluster seed absent")?,
                    parse_fixture_actor(
                        structured_value(&record, "winner_actor")
                            .context("conflict winner absent")?
                    )?
                )
            } else {
                String::new()
            };
            let state = (ground_hash.to_owned(), tick.to_owned(), conflict_signature);
            if let Some(expected) = &agreed {
                if expected != &state {
                    bail!(
                        "{id} accepted ground differs on node {node}: expected={expected:?} observed={state:?}"
                    );
                }
            } else {
                agreed = Some(state);
            }
        }
    }
    Ok(())
}

pub(super) fn validate_acceptance_delay_records(logs: &[(u16, String)]) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected.split(',').any(|name| name == "acceptance-delay") {
        return Ok(());
    }
    for node in [0, 1, 2] {
        let log = &logs
            .iter()
            .find(|(candidate, _)| *candidate == node)
            .with_context(|| format!("missing node {node} acceptance-delay log"))?
            .1;
        let records = log
            .lines()
            .map(strip_ansi)
            .filter(|line| line.contains("cluster acceptance delayed"))
            .collect::<Vec<_>>();
        let mut bands = HashSet::new();
        let mut key = None;
        for line in &records {
            let observed_key =
                structured_value(line, "key").context("delayed acceptance key absent")?;
            if let Some(previous) = key {
                if previous != observed_key {
                    bail!(
                        "acceptance-delay emitted an unrelated failure key on node {node}: {line}"
                    );
                }
            } else {
                key = Some(observed_key);
            }
            let band = structured_value(line, "age_band_seconds")
                .context("acceptance age band absent")?
                .parse::<u64>()?;
            let age = structured_value(line, "actual_age_seconds")
                .context("acceptance actual age absent")?
                .parse::<u64>()?;
            if age <= band || band < 10 || band > 70 || band % 10 != 0 || !bands.insert(band) {
                bail!(
                    "acceptance diagnostic repeated, skipped or used an invalid age band on node {node}: {line}"
                );
            }
            let expected_level = if age > 60 { "ERROR" } else { "WARN" };
            if !line.contains(expected_level) {
                bail!(
                    "acceptance diagnostic used the wrong severity for actual age {age}s on node {node}: {line}"
                );
            }
        }
        if bands != HashSet::from([10, 20, 30, 40, 50, 60, 70]) {
            bail!("acceptance delay on node {node} emitted wrong 10-second bands: {bands:?}");
        }
    }
    Ok(())
}

pub(super) fn validate_acceptance_cooldown_records(logs: &[(u16, String)]) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected
        .split(',')
        .any(|name| name == "acceptance-cooldown")
    {
        return Ok(());
    }
    for node in [0, 1, 2] {
        let log = logs
            .iter()
            .find(|(candidate, _)| *candidate == node)
            .with_context(|| format!("missing node {node} acceptance cooldown log"))?
            .1
            .as_str();
        let records = log
            .lines()
            .map(strip_ansi)
            .filter(|line| line.contains("cluster acceptance delayed"))
            .collect::<Vec<_>>();
        let bands = records
            .iter()
            .map(|line| {
                Ok(structured_value(line, "age_band_seconds")
                    .context("cooldown diagnostic age band absent")?
                    .parse::<u64>()?)
            })
            .collect::<Result<Vec<_>>>()?;
        if bands != [10, 20, 30, 40, 50, 60, 70, 80, 70] {
            bail!(
                "node {node} did not suppress same/lower ages within five minutes or reemit after expiry: {bands:?}"
            );
        }
        let key = structured_value(&records[0], "key").context("cooldown diagnostic key absent")?;
        for line in &records {
            if structured_value(line, "key") != Some(key) {
                bail!("node {node} cooldown diagnostic changed key: {line}");
            }
            let age = structured_value(line, "actual_age_seconds")
                .context("cooldown actual age absent")?
                .parse::<u64>()?;
            let level = if age > 60 { "ERROR" } else { "WARN" };
            if !line.contains(level) {
                bail!("node {node} cooldown diagnostic severity wrong: {line}");
            }
        }
        let completion = log
            .lines()
            .map(strip_ansi)
            .find(|line| {
                line.contains("cluster e2e acceptance cooldown complete")
                    && structured_value(line, "key") == Some(key)
            })
            .with_context(|| format!("node {node} lacks cooldown expiry evidence"))?;
        let elapsed = structured_value(&completion, "elapsed_since_last_emission_ms")
            .context("cooldown elapsed time absent")?
            .parse::<u64>()?;
        let suppressed = structured_value(&completion, "suppressed_same_or_lower")
            .context("cooldown suppressed observation count absent")?
            .parse::<u64>()?;
        if elapsed < 300_000 || suppressed == 0 {
            bail!("node {node} did not observe actual five-minute cooldown: {completion}");
        }
    }
    Ok(())
}

pub(super) fn validate_saturated_departure_records(logs: &[(u16, String)]) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected
        .split(',')
        .any(|name| name == "cursor-crafting-departure-saturated")
    {
        return Ok(());
    }
    let log = &logs
        .iter()
        .find(|(id, _)| *id == 1)
        .context("missing departure owner log")?
        .1;
    let record = log
        .lines()
        .map(strip_ansi)
        .find(|line| line.contains("cluster e2e inventory saturated departure"))
        .context("saturated departure did not publish a bounded owner result")?;
    if structured_value(&record, "ingress_full") != Some("true")
        || structured_value(&record, "semantic_close_accepted") != Some("true")
        || structured_value(&record, "returned_items") != Some("2")
        || structured_value(&record, "duplicate_items") != Some("0")
    {
        bail!("saturated departure lost its exact-token Close or inventory conservation: {record}");
    }
    Ok(())
}

pub(super) async fn arm_packet_preflight_departure(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    if !env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|selected| selected == id)
    {
        eprintln!(
            "untested {id}: select PUMPKIN_E2E_SPEC_SCENARIOS={id} after delayed PacketReceivedEvent hook exists"
        );
        return Ok(());
    }
    actor.chat(&format!("/clustertest {id}"));
    wait_chat_exact(events, &format!("cluster test {id} armed"), deadline, id).await?;
    actor.write_packet(ServerboundSwing {
        hand: InteractionHand::OffHand,
    });
    wait_chat_exact(
        events,
        &format!("cluster test {id} callback started"),
        deadline,
        "actual PacketReceivedEvent in-flight confirmation",
    )
    .await?;
    Ok(())
}

pub(super) async fn verify_handoff_revision_mismatch(
    node: &super::Node,
    existing: &Client,
    observer: &Client,
    existing_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    if !env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "handoff-revision-mismatch")
    {
        eprintln!(
            "untested handoff-revision-mismatch: feature-only mismatched response revision hook missing"
        );
        return Ok(());
    }
    existing.chat("/clustertest handoff-revision-mismatch");
    wait_chat_exact(
        existing_events,
        "cluster test handoff-revision-mismatch armed",
        deadline,
        "arm mismatched playerdata response",
    )
    .await?;
    let (candidate, mut events) = super::connect(
        super::BOT_E,
        node.cancel_java
            .as_deref()
            .context("revision mismatch forwarding endpoint absent")?,
        deadline,
    )
    .await?;
    let login = super::wait_login(
        &mut events,
        deadline,
        "revision mismatch initial login",
        None,
    )
    .await?;
    let lobby = super::wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "revision mismatch virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    within(deadline, "revision mismatch aborts before any player creation", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Disconnect(reason) => {
                    let reason = reason.context("revision mismatch closed without a reason")?.to_string();
                    if !reason.contains("revision mismatch") { bail!("handoff aborted for the wrong reason: {reason}"); }
                    return Ok(());
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::PlayerPosition(position) if position.change.pos.x.abs() < 900_000.0
                        || position.change.pos.z.abs() < 900_000.0 => bail!("revision mismatch teleported a partial player into the real world"),
                    ClientboundGamePacket::ContainerSetContent(_)
                    | ClientboundGamePacket::Respawn(_)
                    | ClientboundGamePacket::Login(_) => bail!("revision mismatch emitted a real-player or second-login transition packet"),
                    _ => {}
                },
                _ => {}
            }
        }
        bail!("revision mismatch stream ended without an explicit disconnect");
    }).await?;
    candidate.disconnect();
    if existing
        .entity_by_uuid(offline_uuid(super::BOT_E))
        .is_some()
        || observer
            .entity_by_uuid(offline_uuid(super::BOT_E))
            .is_some()
    {
        bail!("revision mismatch left a registered world avatar");
    }
    existing.chat("/list");
    wait_chat_contains(
        existing_events,
        BOT_A,
        deadline,
        "existing client remains healthy after revision mismatch",
    )
    .await?;
    Ok(())
}

pub(super) async fn verify_login_plugin_cancel(
    node: &super::Node,
    existing: &Client,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected
        .split(',')
        .any(|name| name == "login-plugin-cancel")
    {
        return Ok(());
    }
    let endpoint = node
        .cancel_java
        .as_deref()
        .context("canceled-login forwarding endpoint missing")?;
    let (candidate, mut events) = super::connect(super::BOT_E, endpoint, deadline).await?;
    let login = super::wait_login(
        &mut events,
        deadline,
        "canceled-login initial Java login",
        None,
    )
    .await?;
    let lobby = super::wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "canceled-login virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    within(deadline, "async PlayerLoginEvent cancellation", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Disconnect(reason) => {
                    if existing
                        .entity_by_uuid(super::offline_uuid(super::BOT_E))
                        .is_some()
                    {
                        bail!("canceled login left a remote world avatar");
                    }
                    if reason.is_none() {
                        bail!("canceled login closed without a user-facing rejection reason");
                    }
                    return Ok(());
                }
                Event::Packet(packet)
                    if matches!(packet.as_ref(), ClientboundGamePacket::Respawn(_)) =>
                {
                    bail!("canceled login sent a real-world respawn packet");
                }
                Event::Spawn => bail!("canceled login produced a real-world spawn"),
                _ => {}
            }
        }
        bail!("canceled login event stream ended without a disconnect")
    })
    .await?;
    candidate.disconnect();
    eprintln!(
        "passed login-plugin-cancel: virtual lobby entered, then plugin rejection removed connection without an avatar"
    );
    Ok(())
}

pub(super) async fn verify_login_plugin_delay(
    node: &super::Node,
    existing: &Client,
    existing_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected.split(',').any(|name| name == "login-plugin-delay") {
        return Ok(());
    }
    let endpoint = node
        .cancel_java
        .as_deref()
        .context("delayed-login forwarding endpoint missing")?;
    let (candidate, mut events) = super::connect(super::BOT_E, endpoint, deadline).await?;
    let login = super::wait_login(
        &mut events,
        deadline,
        "delayed-login initial Java login",
        None,
    )
    .await?;
    let lobby = super::wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "delayed-login virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    let started = tokio::time::Instant::now();
    candidate.chat("/list");
    within(deadline, "lobby command during delayed login callback", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string().contains(super::BOT_A) => {
                    if started.elapsed() >= std::time::Duration::from_secs(3) {
                        bail!("lobby /list waited for the three-second login plugin callback");
                    }
                    return Ok(());
                }
                Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::PlayerPosition(position) if position.change.pos.x.abs() < 1000.0) => {
                    bail!("delayed login promoted before answering an entity-free lobby command");
                }
                Event::Disconnect(reason) => bail!("delayed login disconnected during lobby command: {reason:?}"),
                _ => {}
            }
        }
        bail!("delayed login closed before /list response")
    }).await?;
    candidate.chat("e2e-chat-during-login-plugin");
    wait_chat_contains(
        existing_events,
        "e2e-chat-during-login-plugin",
        deadline,
        "chat during async login plugin",
    )
    .await?;
    super::wait_for_player_transition(
        &candidate,
        &mut events,
        super::BOT_A,
        deadline,
        "delayed plugin handoff",
    )
    .await?;
    if started.elapsed() < std::time::Duration::from_secs(3) {
        bail!("delayed login completed before the configured callback duration");
    }
    if existing
        .entity_by_uuid(super::offline_uuid(super::BOT_E))
        .is_none()
    {
        bail!("delayed plugin login did not publish the joined player's real avatar");
    }
    candidate.disconnect();
    eprintln!(
        "passed login-plugin-delay: lobby /list/chat stayed responsive during three-second callback and promotion followed"
    );
    Ok(())
}

pub(super) fn validate_login_plugin_cancel_record(logs: &[(u16, String)]) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected
        .split(',')
        .any(|name| name == "login-plugin-cancel")
    {
        return Ok(());
    }
    let uuid = super::offline_uuid(super::BOT_E).to_string();
    let log = &logs
        .iter()
        .find(|(id, _)| *id == 1)
        .context("missing canceled-login owner log")?
        .1;
    let record = log
        .lines()
        .map(strip_ansi)
        .find(|line| {
            line.contains("cluster e2e canceled login cleanup")
                && structured_value(line, "uuid") == Some(uuid.as_str())
        })
        .context("canceled login did not publish a cleanup record")?;
    if structured_value(&record, "player_registered") != Some("false")
        || structured_value(&record, "replica_registered") != Some("false")
        || structured_value(&record, "inventory_preinstall_canceled") != Some("true")
    {
        bail!("canceled login retained a partial player or inventory install: {record}");
    }
    Ok(())
}

async fn run_world_scenario(
    actor: &Client,
    observer: &Client,
    third: Option<&Client>,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    scenario: &WorldScenario,
) -> Result<()> {
    let prior_zombies = if scenario.id == "entity-chunk-transition" {
        Some(zombie_uuids(observer)?)
    } else {
        None
    };
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat(&format!("/clustertest {}", scenario.id));
    drive_placement_sources(actor, observer, third, events, deadline, scenario.id).await?;
    if scenario.id == "stale-dirt-break" {
        drive_stale_break_sources(actor, observer, events, deadline).await?;
    }
    let replay_chest = drive_replay_sources(
        actor,
        observer,
        events,
        observer_events,
        deadline,
        scenario.id,
    )
    .await?;
    if scenario.id == "actor-local-overlay-discard" {
        within(deadline, "actor-only overlay never escapes to either client", async {
            let mut accepted = false;
            let mut settled_ticks = 0;
            let mut ticks = actor.get_tick_broadcaster();
            while !accepted || settled_ticks < 5 {
                tokio::select! {
                    tick = ticks.recv() => {
                        tick.context("overlay tick stream ended")?;
                        for client in [actor, observer] {
                            if client.world()?.read().get_block_state(CONFLICT_POSITION).is_some_and(|state| BlockKind::from(state) == BlockKind::OakPlanks) {
                                bail!("actor-private overlay appeared in decoded client world through a batched/chunk update");
                            }
                        }
                        if accepted { settled_ticks += 1; }
                    }
                    event = events.recv() => {
                        let event = event.context("overlay actor stream ended")?;
                        reject_overlay_leak(&event)?;
                        if let Event::Chat(chat) = event {
                            accepted |= chat.message().to_string() == "cluster test actor-local-overlay-discard accepted";
                        }
                    }
                    event = observer_events.recv() => {
                        reject_overlay_leak(&event.context("overlay observer stream ended")?)?;
                    }
                }
            }
            Ok(())
        }).await?;
    } else if scenario.id == "targeted-undo" {
        within(deadline, "provisional block and targeted client undo", async {
            let mut provisional = false;
            let mut corrected = false;
            let mut accepted = false;
            while !(provisional && corrected && accepted) {
                match events.recv().await.context("targeted undo client stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::BlockUpdate(update) if update.pos == CONFLICT_POSITION => {
                            match BlockKind::from(update.block_state) {
                                BlockKind::OakPlanks if corrected => bail!("targeted undo restored a losing block after correction"),
                                BlockKind::OakPlanks => provisional = true,
                                BlockKind::Stone if provisional => corrected = true,
                                _ => {}
                            }
                        }
                        ClientboundGamePacket::LevelChunkWithLight(_) if provisional => {
                            bail!("targeted undo repaired client state with a whole chunk instead of a block delta");
                        }
                        _ => {}
                    },
                    Event::Chat(chat) if chat.message().to_string() == "cluster test targeted-undo accepted" => accepted = true,
                    Event::Disconnect(reason) => bail!("targeted undo disconnected client: {reason:?}"),
                    _ => {}
                }
            }
            Ok(())
        }).await?;
    } else if matches!(
        scenario.id,
        "late-dependent-replay" | "counterfactual-ground-block"
    ) {
        wait_replay_acceptance(
            actor,
            observer,
            events,
            observer_events,
            deadline,
            scenario.id,
        )
        .await?;
    } else {
        wait_chat_exact(
            events,
            &format!("cluster test {} accepted", scenario.id),
            deadline,
            scenario.id,
        )
        .await?;
    }
    verify_replay_client_conservation(
        actor,
        observer,
        events,
        observer_events,
        deadline,
        scenario.id,
    )
    .await?;
    drop(replay_chest);
    if let Some(prior_zombies) = prior_zombies {
        let uuid = wait_until(
            observer,
            deadline,
            "entity crossed its chunk boundary",
            || {
                for entity in observer.nearest_entities::<With<Zombie>>()? {
                    let uuid = entity.uuid()?;
                    let position = entity.position()?;
                    if !prior_zombies.contains(&uuid) && position.x >= 16.0 {
                        return Ok(Some(uuid));
                    }
                }
                Ok(None)
            },
        )
        .await?;
        let actor_entity = actor
            .entity_by_uuid(uuid)
            .context("transitioning entity absent on source holder")?;
        let observer_entity = observer
            .entity_by_uuid(uuid)
            .context("transitioning entity absent on destination holder")?;
        if (actor_entity.position()?.x - observer_entity.position()?.x).abs() > 0.1 {
            bail!("entity chunk transition left holders with different actor positions");
        }
    }
    for (pos, expected) in scenario.positions {
        for (client, side) in [(actor, "actor"), (observer, "observer")] {
            wait_until(
                client,
                deadline,
                &format!("{} {side} block {pos:?}", scenario.id),
                || {
                    let state = client.world()?.read().get_block_state(*pos);
                    let matched = state.is_some_and(|state| match expected {
                        BlockExpectation::Exact(kind) => BlockKind::from(state) == *kind,
                        BlockExpectation::Either(first, second) => {
                            let actual = BlockKind::from(state);
                            actual == *first || actual == *second
                        }
                    });
                    Ok(matched.then_some(()))
                },
            )
            .await?;
        }
        let actor_state = actor
            .world()?
            .read()
            .get_block_state(*pos)
            .context("actor block absent")?;
        let observer_state = observer
            .world()?
            .read()
            .get_block_state(*pos)
            .context("observer block absent")?;
        if actor_state != observer_state {
            bail!(
                "{} did not converge at {pos:?}: actor={actor_state:?} observer={observer_state:?}",
                scenario.id
            );
        }
    }
    verify_placement_client_conservation(actor, observer, third, deadline, scenario.id).await?;
    if matches!(
        scenario.id,
        "door-trapdoor" | "end-frame-anchor" | "button-lever"
    ) {
        run_block_interactions(actor, observer, events, deadline, scenario).await?;
    }
    within(deadline, &format!("{} stable accepted state", scenario.id), async {
        let mut ticks = actor.get_tick_broadcaster();
        for _ in 0..5 {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed after extended world action")?;
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed after extended world action")? {
                        Event::Disconnect(reason) => bail!("{} disconnected the actor: {reason:?}", scenario.id),
                        Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::Login(_) | ClientboundGamePacket::Respawn(_)) => {
                            bail!("{} triggered a terrain transition", scenario.id)
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }).await?;
    Ok(())
}

async fn run_block_interactions(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    scenario: &WorldScenario,
) -> Result<()> {
    wait_for_position(
        actor,
        Vec3::new(0.5, 80.0, 3.5),
        deadline,
        "block interaction reach setup",
    )
    .await?;
    for (index, (position, _)) in scenario.positions.iter().enumerate() {
        let before = actor
            .world()?
            .read()
            .get_block_state(*position)
            .context("interaction block absent before Java input")?;
        let (property, initial, expected) = match scenario.id {
            "door-trapdoor" => ("open", "false", "true"),
            "button-lever" => ("powered", "false", "true"),
            "end-frame-anchor" if index == 0 => ("eye", "false", "true"),
            "end-frame-anchor" => ("charges", "0", "1"),
            _ => bail!("unsupported interaction scenario"),
        };
        if before.to_trait().get_property(property) != Some(initial) {
            bail!(
                "{} interaction initial {property} state was not {initial}",
                scenario.id
            );
        }
        if scenario.id == "end-frame-anchor" {
            actor.set_selected_hotbar_slot(index as u8);
            let inventory = actor
                .get_inventory()?
                .menu()?
                .context("interaction initial inventory absent")?;
            let item = if index == 0 {
                ItemKind::EnderEye
            } else {
                ItemKind::Glowstone
            };
            if !inventory
                .slots()
                .get(36 + index)
                .is_some_and(|stack| stack.kind() == item && stack.count() == 1)
            {
                bail!(
                    "interaction did not start with exactly one {item:?} in slot {}",
                    36 + index
                );
            }
        }
        let sequence = 0x780 + index as u32;
        actor.write_packet(ServerboundUseItemOn {
            hand: InteractionHand::MainHand,
            block_hit: BlockHit {
                block_pos: *position,
                direction: Direction::Up,
                location: Vec3::new(
                    position.x as f64 + 0.5,
                    position.y as f64 + 0.5,
                    position.z as f64 + 0.5,
                ),
                inside: false,
                world_border: false,
            },
            seq: sequence,
        });
        within(deadline, &format!("{} interaction at {position:?}", scenario.id), async {
            let mut acknowledged = false;
            let mut ticks = actor.get_tick_broadcaster();
            loop {
                tokio::select! {
                    tick = ticks.recv() => {
                        tick.context("Azalea tick stream closed during block interaction")?;
                        let actor_state = actor.world()?.read().get_block_state(*position);
                        let observer_state = observer.world()?.read().get_block_state(*position);
                        if acknowledged && actor_state.is_some_and(|state| state != before)
                            && actor_state == observer_state
                            && actor_state.is_some_and(|state| state.to_trait().get_property(property) == Some(expected)) {
                            return Ok(());
                        }
                    }
                    event = events.recv() => {
                        match event.context("Azalea event stream closed during block interaction")? {
                            Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == sequence) => acknowledged = true,
                            Event::Disconnect(reason) => bail!("{} disconnected actor on interaction: {reason:?}", scenario.id),
                            _ => {}
                        }
                    }
                }
            }
        }).await?;
        if scenario.id == "end-frame-anchor" {
            wait_until(
                actor,
                deadline,
                "interaction consumed exactly one inventory item",
                || {
                    let inventory = actor
                        .get_inventory()?
                        .menu()?
                        .context("interaction result inventory absent")?;
                    Ok(inventory
                        .slots()
                        .get(36 + index)
                        .is_some_and(|stack| stack.is_empty())
                        .then_some(()))
                },
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_datagram_loss_reorder(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat(&format!("/clustertest {id}"));
    wait_chat_exact(
        events,
        &format!("cluster test {id} accepted"),
        deadline,
        "datagram loss/reorder armed",
    )
    .await?;
    let uuid = super::offline_uuid(super::BOT_A);
    let before = observer
        .entity_by_uuid(uuid)
        .context("movement replica absent")?
        .position()?;
    actor.walk(WalkDirection::Forward);
    let outcome = wait_for_position_change(
        observer,
        uuid,
        before,
        deadline,
        "later movement superseded dropped datagram",
    )
    .await;
    actor.walk(WalkDirection::None);
    outcome?;
    Ok(())
}

async fn run_holder_only_routing(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest holder-only-routing");
    wait_chat_exact(
        events,
        "cluster test holder-only-routing accepted",
        deadline,
        "holder release setup",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(1000.5, 80.0, 0.5),
        deadline,
        "observer moved outside actor holder region",
    )
    .await?;
    let actor_uuid = super::offline_uuid(super::BOT_A);
    wait_until(observer, deadline, "nonholder replica removal", || {
        Ok(observer.entity_by_uuid(actor_uuid).is_none().then_some(()))
    })
    .await?;
    actor.walk(WalkDirection::Forward);
    wait_ticks(
        actor,
        5,
        deadline,
        "movement while observer node is not a holder",
    )
    .await?;
    actor.walk(WalkDirection::None);
    actor.chat("/clustertest holder-return");
    wait_chat_exact(
        events,
        "cluster test holder-return accepted",
        deadline,
        "holder return setup",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(1.5, 80.0, 0.5),
        deadline,
        "observer returned to actor holder region",
    )
    .await?;
    wait_until(
        observer,
        deadline,
        "full actor snapshot after holder return",
        || {
            let Some(remote) = observer.entity_by_uuid(actor_uuid) else {
                return Ok(None);
            };
            let actual = remote.position()?;
            let source = actor.position()?;
            Ok(((actual.x - source.x).abs() < 0.5
                && (actual.y - source.y).abs() < 0.5
                && (actual.z - source.z).abs() < 0.5)
                .then_some(()))
        },
    )
    .await?;
    Ok(())
}

async fn run_entity_holder_only_routing(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let before = zombie_uuids(observer)?;
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest entity-holder-only-routing");
    wait_chat_exact(
        events,
        "cluster test entity-holder-only-routing accepted",
        deadline,
        "entity holder setup",
    )
    .await?;
    let uuid = wait_until(
        observer,
        deadline,
        "owner entity visible on destination holder",
        || {
            for zombie in observer.nearest_entities::<With<Zombie>>()? {
                let uuid = zombie.uuid()?;
                if !before.contains(&uuid) {
                    return Ok(Some(uuid));
                }
            }
            Ok(None)
        },
    )
    .await?;
    actor.chat("/clustertest entity-holder-leave");
    wait_chat_exact(
        events,
        "cluster test entity-holder-leave accepted",
        deadline,
        "entity holder release",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(1000.5, 80.0, 0.5),
        deadline,
        "entity observer outside holder region",
    )
    .await?;
    wait_until(
        observer,
        deadline,
        "nonholder entity replica removed",
        || Ok(observer.entity_by_uuid(uuid).is_none().then_some(())),
    )
    .await?;
    wait_ticks(
        actor,
        5,
        deadline,
        "entity owner continued ticking while observer nonholder",
    )
    .await?;
    actor.chat("/clustertest entity-holder-return");
    wait_chat_exact(
        events,
        "cluster test entity-holder-return accepted",
        deadline,
        "entity holder reacquisition",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(1.5, 80.0, 0.5),
        deadline,
        "entity observer returned to holder region",
    )
    .await?;
    wait_until(
        observer,
        deadline,
        "full entity snapshot after holder return",
        || {
            let Some(source) = actor.entity_by_uuid(uuid) else {
                return Ok(None);
            };
            let Some(remote) = observer.entity_by_uuid(uuid) else {
                return Ok(None);
            };
            let source = source.position()?;
            let remote = remote.position()?;
            Ok(((source.x - remote.x).abs() < 0.5
                && (source.y - remote.y).abs() < 0.5
                && (source.z - remote.z).abs() < 0.5)
                .then_some(()))
        },
    )
    .await?;
    eprintln!(
        "passed entity-holder-only-routing: moving entity was absent at nonholder and resnapshot at reacquisition"
    );
    Ok(())
}

async fn run_item_drop_pickup(
    actor: &Client,
    collector: &Client,
    events: &mut UnboundedReceiver<Event>,
    collector_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while collector_events.try_recv().is_ok() {}
    actor.chat("/clustertest item-drop-pickup");
    wait_chat_exact(
        events,
        "cluster test item-drop-pickup accepted",
        deadline,
        "item entity action setup",
    )
    .await?;
    wait_until(actor, deadline, "two-stone drop source", || {
        Ok((stone_count(actor)? == 2).then_some(()))
    })
    .await?;
    wait_until(collector, deadline, "empty stone collector", || {
        Ok((stone_count(collector)? == 0).then_some(()))
    })
    .await?;
    actor.set_selected_hotbar_slot(4);
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::DropItem,
        pos: BlockPos::new(0, 0, 0),
        direction: Direction::Down,
        seq: 0x7a1,
    });
    within(deadline, "cross-holder item drop and pickup conservation", async {
        let mut source_item = None;
        let mut collector_item = None;
        let mut taken_item = None;
        let collector_id = *collector.ecs.read().get::<azalea::core::entity_id::MinecraftEntityId>(collector.entity).context("collector network entity id missing")?;
        let mut source_authoritative = false;
        let mut target_authoritative = false;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed during item pickup")?;
                    if source_item.is_some() && collector_item.is_some() && taken_item.is_some()
                        && source_item == collector_item.map(|(uuid, _)| uuid)
                        && taken_item == collector_item.map(|(_, id)| id)
                        && source_authoritative && target_authoritative
                        && stone_count(actor)? == 1 && stone_count(collector)? == 1 {
                        return Ok(());
                    }
                }
                event = events.recv() => {
                    match event.context("item drop source stream closed")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::AddEntity(packet) if packet.entity_type == EntityKind::Item => {
                                if source_item.is_some_and(|uuid| uuid != packet.uuid) { bail!("drop created more than one source item entity"); }
                                source_item = Some(packet.uuid);
                            },
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == 0 && packet.item_stack.kind() == ItemKind::Stone && packet.item_stack.count() == 1 => source_authoritative = true,
                            ClientboundGamePacket::ContainerSetContent(packet)
                                if packet.container_id == 0 && packet.items.iter().any(|item| item.kind() == ItemKind::Stone && item.count() == 1) => source_authoritative = true,
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("item drop disconnected source: {reason:?}"),
                        _ => {}
                    }
                }
                event = collector_events.recv() => {
                    match event.context("item collector stream closed")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::AddEntity(packet) if packet.entity_type == EntityKind::Item => {
                                if collector_item.is_some_and(|(uuid, _)| uuid != packet.uuid) { bail!("collector observed duplicate dropped item entities"); }
                                collector_item = Some((packet.uuid, packet.id.0 as u32));
                            },
                            ClientboundGamePacket::TakeItemEntity(packet) if packet.amount == 1 && packet.player_id == collector_id => taken_item = Some(packet.item_id),
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == 0 && packet.item_stack.kind() == ItemKind::Stone && packet.item_stack.count() == 1 => target_authoritative = true,
                            ClientboundGamePacket::ContainerSetContent(packet)
                                if packet.container_id == 0 && packet.items.iter().any(|item| item.kind() == ItemKind::Stone && item.count() == 1) => target_authoritative = true,
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("item pickup disconnected collector: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
    }).await?;
    eprintln!("passed item-drop-pickup: one stone transferred via item entity without duplication");
    Ok(())
}

fn stone_count(client: &Client) -> Result<i32> {
    let inventory = client
        .get_inventory()?
        .menu()?
        .context("player inventory menu absent")?;
    Ok(inventory
        .slots()
        .iter()
        .filter(|item| item.kind() == ItemKind::Stone)
        .map(|item| item.count())
        .sum())
}

async fn run_food_use(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat(&format!("/clustertest {id}"));
    wait_chat_exact(events, &format!("cluster test {id} accepted"), deadline, id).await?;
    actor.set_selected_hotbar_slot(4);
    wait_until(actor, deadline, "food setup reaches client", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("food inventory absent")?;
        let hunger = actor
            .ecs
            .read()
            .get::<azalea::local_player::Hunger>(actor.entity)
            .cloned()
            .context("food actor has no client hunger state")?;
        Ok((inventory
            .slots()
            .get(40)
            .is_some_and(|stack| stack.kind() == ItemKind::Apple && stack.count() == 2)
            && hunger.food == 10
            && hunger.saturation.abs() < 0.01)
            .then_some(()))
    })
    .await?;
    let uuid = offline_uuid(BOT_A);
    actor.write_packet(ServerboundUseItem {
        hand: InteractionHand::MainHand,
        seq: 0x7f0,
        y_rot: 0.0,
        x_rot: 0.0,
    });
    wait_until(observer, deadline, "remote eating begins", || {
        Ok(observer
            .entity_by_uuid(uuid)
            .context("food user replica absent")?
            .get_component::<azalea::entity::metadata::AbstractLivingUsingItem>()
            .is_some_and(|using| using.0)
            .then_some(()))
    })
    .await?;
    let abort = id == "food-abort";
    if abort {
        actor.write_packet(ServerboundPlayerAction {
            action: PlayerAction::ReleaseUseItem,
            pos: BlockPos::ZERO,
            direction: Direction::Down,
            seq: 0x7f1,
        });
    }
    wait_until(
        observer,
        deadline,
        "remote eating finishes or aborts",
        || {
            Ok(observer
                .entity_by_uuid(uuid)
                .context("food user replica disappeared")?
                .get_component::<azalea::entity::metadata::AbstractLivingUsingItem>()
                .is_some_and(|using| !using.0)
                .then_some(()))
        },
    )
    .await?;
    if abort {
        wait_ticks(actor, 40, deadline, "aborted food cannot complete later").await?;
    }
    wait_until(
        actor,
        deadline,
        "authoritative food and inventory outcome",
        || {
            let inventory = actor
                .get_inventory()?
                .menu()?
                .context("food outcome inventory absent")?;
            let hunger = actor
                .ecs
                .read()
                .get::<azalea::local_player::Hunger>(actor.entity)
                .cloned()
                .context("food outcome HUD state absent")?;
            Ok((inventory.slots().get(40).is_some_and(|stack| {
                stack.kind() == ItemKind::Apple && stack.count() == if abort { 2 } else { 1 }
            }) && hunger.food == if abort { 10 } else { 14 }
                && (hunger.saturation - if abort { 0.0 } else { 2.4 }).abs() < 0.01)
                .then_some(()))
        },
    )
    .await?;
    actor.chat("/list");
    wait_chat_contains(
        events,
        BOT_A,
        deadline,
        "food action keeps client responsive",
    )
    .await?;
    eprintln!(
        "passed {id}: normal Java use/abort changed remote eating, authoritative stack and self hunger together"
    );
    Ok(())
}

async fn run_mining_break(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat("/clustertest mining-break");
    wait_chat_exact(
        events,
        "cluster test mining-break accepted",
        deadline,
        "mining setup",
    )
    .await?;
    wait_for_position(actor, Vec3::new(0.5, 80.0, 3.5), deadline, "mining reach").await?;
    for client in [actor, observer] {
        wait_until(client, deadline, "dirt before mining", || {
            Ok(client
                .world()?
                .read()
                .get_block_state(CONFLICT_POSITION)
                .is_some_and(|state| BlockKind::from(state) == BlockKind::Dirt)
                .then_some(()))
        })
        .await?;
    }
    actor.set_selected_hotbar_slot(4);
    wait_until(actor, deadline, "mining tool before break", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("mining inventory absent")?;
        Ok((inventory.slots().get(40).is_some_and(|stack| {
            stack.kind() == ItemKind::IronShovel
                && stack
                    .get_component::<Damage>()
                    .is_some_and(|damage| damage.amount == 1)
        }) && inventory
            .slots()
            .iter()
            .all(|stack| stack.kind() != ItemKind::Dirt))
        .then_some(()))
    })
    .await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StartDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7f2,
    });
    wait_ticks(actor, 20, deadline, "survival mining duration").await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StopDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7f3,
    });
    within(deadline, "mining block, drop, pickup and statistics", async {
        let mut start_ack = false;
        let mut finish_ack = false;
        let mut statistics = false;
        let mut actor_air = false;
        let mut observer_air = false;
        let mut actor_item = None;
        let mut observer_item = None;
        let mut taken_item = None;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("mining client tick stream closed")?;
                    let inventory = actor.get_inventory()?.menu()?.context("mining outcome inventory absent")?;
                    let dirt_count: i32 = inventory.slots().iter().filter(|stack| stack.kind() == ItemKind::Dirt).map(|stack| stack.count()).sum();
                    let damaged = inventory.slots().get(40).is_some_and(|stack| stack.kind() == ItemKind::IronShovel
                        && stack.get_component::<Damage>().is_some_and(|damage| damage.amount == 2));
                    if start_ack && finish_ack && statistics && actor_air && observer_air && damaged && dirt_count == 1
                        && actor_item.is_some() && actor_item.as_ref().map(|(uuid, _)| uuid) == observer_item.as_ref()
                        && actor_item.as_ref().is_some_and(|(_, item)| Some(*item) == taken_item) {
                        return Ok(());
                    }
                }
                event = events.recv() => match event.context("mining actor stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == 0x7f2 => start_ack = true,
                        ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == 0x7f3 => finish_ack = true,
                        ClientboundGamePacket::BlockUpdate(update) if update.pos == CONFLICT_POSITION
                            && BlockKind::from(update.block_state) == BlockKind::Air => actor_air = true,
                        ClientboundGamePacket::AddEntity(add) if add.entity_type == EntityKind::Item => actor_item = Some((add.uuid, add.id.0 as u32)),
                        ClientboundGamePacket::TakeItemEntity(take) if take.amount == 1 => taken_item = Some(take.item_id),
                        ClientboundGamePacket::AwardStats(stats) if !stats.stats.is_empty() => statistics = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("normal survival mining disconnected actor: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("mining observer stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::BlockUpdate(update) if update.pos == CONFLICT_POSITION
                            && BlockKind::from(update.block_state) == BlockKind::Air => observer_air = true,
                        ClientboundGamePacket::AddEntity(add) if add.entity_type == EntityKind::Item => observer_item = Some(add.uuid),
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("mining observer disconnected: {reason:?}"),
                    _ => {}
                },
            }
        }
    }).await?;
    eprintln!(
        "passed mining-break: exact sequence ACKs, both-holder removal/drop, one collected dirt, tool durability and statistics"
    );
    Ok(())
}

async fn run_break_visuals(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat("/clustertest visual-break-start-abort");
    wait_chat_exact(
        events,
        "cluster test visual-break-start-abort accepted",
        deadline,
        "break visual setup",
    )
    .await?;
    wait_for_position(
        actor,
        Vec3::new(0.5, 80.0, 3.5),
        deadline,
        "break visual reach",
    )
    .await?;
    actor.set_selected_hotbar_slot(4);
    wait_until(actor, deadline, "empty mining hand", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("break visual inventory absent")?;
        Ok(inventory
            .slots()
            .get(40)
            .is_some_and(|stack| stack.kind() == ItemKind::Air)
            .then_some(()))
    })
    .await?;
    let remote_id = observer
        .entity_by_uuid(offline_uuid(BOT_A))
        .context("block breaker remote avatar absent")?
        .minecraft_id()?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StartDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7f4,
    });
    within(deadline, "remote block breaking started", async {
        while let Some(event) = observer_events.recv().await {
            match event {
                Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::BlockDestruction(animation)
                    if animation.id == remote_id && animation.pos == CONFLICT_POSITION && animation.progress <= 9) => return Ok(()),
                Event::Disconnect(reason) => bail!("block-breaking animation disconnected observer: {reason:?}"),
                _ => {}
            }
        }
        bail!("remote block-breaking stream ended before animation");
    }).await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::AbortDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7f5,
    });
    within(deadline, "remote break animation removed and exact Java ACKs", async {
        let mut removed = false;
        let mut started = false;
        let mut aborted = false;
        loop {
            tokio::select! {
                event = events.recv() => match event.context("block-breaking actor stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == 0x7f4 => started = true,
                        ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == 0x7f5 => aborted = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("block-breaking abort disconnected actor: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("block-breaking observer stream closed")? {
                    Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::BlockDestruction(animation)
                        if animation.id == remote_id && animation.pos == CONFLICT_POSITION && animation.progress > 9) => removed = true,
                    Event::Disconnect(reason) => bail!("block-breaking abort disconnected observer: {reason:?}"),
                    _ => {}
                },
            }
            if removed && started && aborted { return Ok(()); }
        }
    }).await?;
    for client in [actor, observer] {
        let state = client
            .world()?
            .read()
            .get_block_state(CONFLICT_POSITION)
            .context("aborted block chunk absent")?;
        if BlockKind::from(state) != BlockKind::Stone {
            bail!("aborted mining changed the block");
        }
    }
    eprintln!(
        "passed visual-break-start-abort: exact remote breaking/clear packets, both Java sequence ACKs, no block mutation"
    );
    Ok(())
}

async fn run_health_mask(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    health_policy_observer_permission(actor, events, observer_events, deadline, true).await?;
    let remote_id = actor
        .entity_by_uuid(super::offline_uuid(super::BOT_B))
        .context("health mask target replica absent")?
        .minecraft_id()?;
    let reciprocal_id = observer
        .entity_by_uuid(super::offline_uuid(super::BOT_A))
        .context("health mask reciprocal replica absent")?
        .minecraft_id()?;
    for (mode, remote_health, mob_health) in [
        ("off", 5.0_f32, 6.0_f32),
        ("players", 20.0_f32, 6.0_f32),
        ("all", 20.0_f32, 20.0_f32),
    ] {
        while events.try_recv().is_ok() {}
        while observer_events.try_recv().is_ok() {}
        set_fixture_health_policy(actor, observer, events, observer_events, deadline, mode).await?;
        actor.chat(&format!("/clustertest health-mask-{mode}"));
        within(deadline, &format!("health mask {mode} Java packets"), async {
            let mut accepted = false;
            let mut self_health = false;
            let mut remote_metadata = false;
            let mut mob_metadata = false;
            let mut mob_id = None;
            let mut mob_uuid = None;
            let mut observer_mob_id = None;
            let mut observer_mob_uuid = None;
            let mut observer_remote_metadata = false;
            let mut observer_mob_metadata = false;
            let reciprocal_health = if mode == "off" { 7.0_f32 } else { 20.0_f32 };
            let mut target_self_health = false;
            while !(accepted && self_health && target_self_health && remote_metadata && mob_metadata && observer_remote_metadata && observer_mob_metadata) {
                tokio::select! {
                    event = events.recv() => {
                        match event.context("health mask actor event stream closed")? {
                            Event::Chat(chat) if chat.message().to_string() == format!("cluster test health-mask-{mode} accepted") => accepted = true,
                            Event::Packet(packet) => match packet.as_ref() {
                                ClientboundGamePacket::SetHealth(packet) => {
                                    if (packet.health - 7.0).abs() < 0.01 { self_health = true; }
                                }
                                ClientboundGamePacket::AddEntity(packet) if packet.entity_type == EntityKind::Zombie => {
                                    mob_id = Some(packet.id);
                                    mob_uuid = Some(packet.uuid);
                                }
                                ClientboundGamePacket::SetEntityData(packet) => {
                                    for item in &packet.packed_items.0 {
                                        if item.index != 9 { continue; }
                                        if let EntityDataValue::Float(value) = &item.value {
                                            if packet.id == remote_id && mode != "off" && *value != 20.0 { bail!("health-mask {mode} sent unmasked player value {value} after policy readiness"); }
                                            if Some(packet.id) == mob_id && mode == "all" && *value != 20.0 { bail!("health-mask all sent unmasked mob value {value} after policy readiness"); }
                                            if packet.id == remote_id && (*value - remote_health).abs() < 0.01 {
                                                remote_metadata = true;
                                            }
                                            if Some(packet.id) == mob_id && (*value - mob_health).abs() < 0.01 {
                                                mob_metadata = true;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            },
                            Event::Disconnect(reason) => bail!("health mask {mode} disconnected actor: {reason:?}"),
                            _ => {}
                        }
                    }
                    event = observer_events.recv() => {
                        match event.context("health mask target event stream closed")? {
                            Event::Packet(packet) => match packet.as_ref() {
                                ClientboundGamePacket::SetHealth(health) if health.health == 5.0 => target_self_health = true,
                                ClientboundGamePacket::AddEntity(spawn) if spawn.entity_type == EntityKind::Zombie => { observer_mob_id = Some(spawn.id); observer_mob_uuid = Some(spawn.uuid); },
                                ClientboundGamePacket::SetEntityData(update) => {
                                    for item in &update.packed_items.0 {
                                        if item.index != 9 { continue; }
                                        if let EntityDataValue::Float(value) = &item.value {
                                            if update.id == reciprocal_id {
                                                if mode != "off" && *value != 20.0 { bail!("health-mask {mode} sent unmasked reciprocal player health {value}"); }
                                                observer_remote_metadata |= *value == reciprocal_health;
                                            }
                                            if Some(update.id) == observer_mob_id {
                                                if mode == "all" && *value != 20.0 { bail!("health-mask all sent unmasked reciprocal mob health {value}"); }
                                                observer_mob_metadata |= *value == mob_health;
                                            }
                                        }
                                    }
                                },
                                _ => {}
                            },
                            Event::Disconnect(reason) => bail!("health mask {mode} disconnected target: {reason:?}"),
                            _ => {}
                        }
                    }
                }
            }
            if mob_uuid.is_none() || mob_uuid != observer_mob_uuid { bail!("health-mask holders received different mob UUIDs"); }
            let mut ticks = actor.get_tick_broadcaster();
            let mut observed_ticks = 0;
            while observed_ticks < 10 {
                tokio::select! {
                    tick = ticks.recv() => {
                        tick.context("Azalea tick stream closed during health-mask leak check")?;
                        observed_ticks += 1;
                    }
                    event = events.recv() => {
                        match event.context("health-mask leak-check stream closed")? {
                            Event::Packet(packet) => {
                                if let ClientboundGamePacket::SetHealth(update) = packet.as_ref() && update.health != 7.0 { bail!("health policy masked actor self HUD"); }
                                if let ClientboundGamePacket::SetEntityData(packet) = packet.as_ref() {
                                    for item in &packet.packed_items.0 {
                                        if item.index != 9 { continue; }
                                        if let EntityDataValue::Float(value) = &item.value {
                                            if packet.id == remote_id && *value != remote_health {
                                                bail!("health-mask {mode} leaked remote player health {value}");
                                            }
                                            if Some(packet.id) == mob_id && *value != mob_health {
                                                bail!("health-mask {mode} leaked remote mob health {value}");
                                            }
                                        }
                                    }
                                }
                            }
                            Event::Disconnect(reason) => bail!("health-mask {mode} leak-check disconnected actor: {reason:?}"),
                            _ => {}
                        }
                    }
                    event = observer_events.recv() => match event.context("reciprocal health-mask stream closed")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::SetHealth(update) if update.health != 5.0 => bail!("health policy masked reciprocal self HUD"),
                            ClientboundGamePacket::SetEntityData(update) => {
                                for item in &update.packed_items.0 {
                                    if item.index == 9 && let EntityDataValue::Float(value) = &item.value {
                                        if update.id == reciprocal_id && *value != reciprocal_health { bail!("health-mask reciprocal player numeric leak {value}"); }
                                        if Some(update.id) == observer_mob_id && *value != mob_health { bail!("health-mask reciprocal mob numeric leak {value}"); }
                                    }
                                }
                            },
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("reciprocal health-mask disconnected: {reason:?}"),
                        _ => {}
                    }
                }
            }
            Ok(())
        }).await?;
    }
    actor.chat("/gamerule clusterHealthMask players");
    health_policy_observer_permission(actor, events, observer_events, deadline, false).await?;
    eprintln!(
        "passed health-mask: off/players/all metadata and accurate self HUD on both Java clients"
    );
    Ok(())
}

async fn run_health_mask_mount(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    health_policy_observer_permission(actor, events, observer_events, deadline, true).await?;
    let local_rider = *actor
        .ecs
        .read()
        .get::<azalea::core::entity_id::MinecraftEntityId>(actor.entity)
        .context("mount local rider network ID absent")?;
    let remote_rider = observer
        .entity_by_uuid(offline_uuid(BOT_A))
        .context("mount remote rider replica absent")?
        .minecraft_id()?;
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    set_fixture_health_policy(actor, observer, events, observer_events, deadline, "all").await?;
    actor.chat("/clustertest health-mask-mount");
    within(deadline, "health mask mount exception", async {
        let mut accepted = false;
        let mut actor_horse = None;
        let mut observer_horse = None;
        let mut mounted = false;
        let mut observer_mounted = false;
        let mut actor_mount_uuid = None;
        let mut observer_mount_uuid = None;
        let mut rider_health = false;
        let mut rider_sees_actual = false;
        let mut observer_sees_mask = false;
        while !(accepted && mounted && observer_mounted && rider_health && rider_sees_actual && observer_sees_mask) {
            tokio::select! {
                event = events.recv() => {
                    match event.context("rider stream closed during mount mask test")? {
                        Event::Chat(chat) if chat.message().to_string() == "cluster test health-mask-mount accepted" => accepted = true,
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::AddEntity(packet) if packet.entity_type == EntityKind::Horse => {
                                actor_horse = Some(packet.id);
                                actor_mount_uuid = Some(packet.uuid);
                                actor.write_packet(ServerboundInteract {
                                    entity_id: packet.id,
                                    hand: InteractionHand::MainHand,
                                    location: packet.position.into(),
                                    using_secondary_action: false,
                                });
                            },
                            ClientboundGamePacket::SetPassengers(packet) if Some(packet.vehicle) == actor_horse && packet.passengers == vec![local_rider] => mounted = true,
                            ClientboundGamePacket::SetHealth(packet) if (packet.health - 7.0).abs() < 0.01 => rider_health = true,
                            ClientboundGamePacket::SetEntityData(packet) if Some(packet.id) == actor_horse => {
                                rider_sees_actual |= packet.packed_items.0.iter().any(|item| {
                                    item.index == 9 && matches!(&item.value, EntityDataValue::Float(value) if (*value - 4.0).abs() < 0.01)
                                });
                            }
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("mount health mask disconnected rider: {reason:?}"),
                        _ => {}
                    }
                }
                event = observer_events.recv() => {
                    match event.context("observer stream closed during mount mask test")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::AddEntity(packet) if packet.entity_type == EntityKind::Horse => { observer_horse = Some(packet.id); observer_mount_uuid = Some(packet.uuid); },
                            ClientboundGamePacket::SetPassengers(packet) if Some(packet.vehicle) == observer_horse && packet.passengers == vec![remote_rider] => observer_mounted = true,
                            ClientboundGamePacket::SetEntityData(packet) if Some(packet.id) == observer_horse => {
                                for item in &packet.packed_items.0 {
                                    if item.index == 9 && let EntityDataValue::Float(value) = &item.value {
                                        if *value != 20.0 { bail!("mount observer received unmasked health {value} after policy readiness"); }
                                        observer_sees_mask = true;
                                    }
                                }
                            }
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("mount health mask disconnected observer: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
        if actor_mount_uuid.is_none() || actor_mount_uuid != observer_mount_uuid { bail!("mount viewers received different canonical UUIDs"); }
        observe_mount_health_window(actor, events, observer_events, deadline, actor_horse.unwrap(), observer_horse.unwrap(), local_rider, remote_rider).await?;
        Ok(())
    }).await?;
    actor.chat("/gamerule clusterHealthMask players");
    health_policy_observer_permission(actor, events, observer_events, deadline, false).await?;
    eprintln!(
        "passed health-mask-mount: rider saw actual four-health mount while another player saw masked twenty"
    );
    Ok(())
}

async fn run_health_visual_exemption(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    health_policy_observer_permission(actor, events, observer_events, deadline, true).await?;
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    set_fixture_health_policy(actor, observer, events, observer_events, deadline, "all").await?;
    actor.chat("/clustertest health-visual-exemption");
    within(deadline, "fresh health-driven golem visual and masked zombie on both holders", async {
        let mut accepted = false;
        let mut golems = [None, None];
        let mut zombies = [None, None];
        let mut golem_health = [false; 2];
        let mut zombie_health = [false; 2];
        let mut ticks = actor.get_tick_broadcaster();
        let mut stable_ticks = 0;
        loop {
            let (side, event) = tokio::select! {
                event = events.recv() => (0, event.context("visual actor event stream closed")?),
                event = observer_events.recv() => (1, event.context("visual observer event stream closed")?),
                tick = ticks.recv() => {
                    tick.context("visual health tick stream closed")?;
                    if accepted && golem_health.iter().all(|seen| *seen) && zombie_health.iter().all(|seen| *seen) {
                        stable_ticks += 1;
                        if stable_ticks == 10 { break; }
                    }
                    continue;
                }
            };
            match event {
                Event::Chat(chat) if side == 0 && chat.message().to_string() == "cluster test health-visual-exemption accepted" => accepted = true,
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::AddEntity(spawn) if spawn.entity_type == EntityKind::IronGolem => golems[side] = Some((spawn.id, spawn.uuid)),
                    ClientboundGamePacket::AddEntity(spawn) if spawn.entity_type == EntityKind::Zombie => zombies[side] = Some((spawn.id, spawn.uuid)),
                    ClientboundGamePacket::SetEntityData(update) => {
                        for item in &update.packed_items.0 {
                            if item.index != 9 { continue; }
                            if let EntityDataValue::Float(value) = &item.value {
                                if golems[side].is_some_and(|(id, _)| id == update.id) {
                                    if *value != 10.0 { bail!("injured golem lost actual numeric visual input: {value}"); }
                                    golem_health[side] |= *value == 10.0;
                                }
                                if zombies[side].is_some_and(|(id, _)| id == update.id) {
                                    if *value != 20.0 { bail!("visual exemption leaked ordinary zombie health: {value}"); }
                                    zombie_health[side] = true;
                                }
                            }
                        }
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!("visual health participant disconnected: {reason:?}"),
                _ => {}
            }
        }
        if golems[0].map(|(_, uuid)| uuid) != golems[1].map(|(_, uuid)| uuid)
            || zombies[0].map(|(_, uuid)| uuid) != zombies[1].map(|(_, uuid)| uuid) {
            bail!("health visual holders observed different canonical entity UUIDs");
        }
        Ok(())
    }).await?;
    actor.chat("/gamerule clusterHealthMask players");
    wait_chat_contains(
        events,
        "clusterHealthMask",
        deadline,
        "restore default health policy",
    )
    .await?;
    health_policy_observer_permission(actor, events, observer_events, deadline, false).await?;
    eprintln!(
        "passed health-visual-exemption: injured golem kept numeric visual input while ordinary mob remained masked under all policy"
    );
    Ok(())
}

async fn run_invsee_number_swap(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let window = within(deadline, "remote invsee number-key window", async {
        let mut open = None;
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(packet)
                        if packet.menu_type == MenuKind::Generic9x5 =>
                    {
                        open = Some(packet.container_id);
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if open == Some(packet.container_id)
                            && packet
                                .items
                                .get(37)
                                .is_some_and(|item| item.kind() == ItemKind::Bow)
                            && packet
                                .items
                                .get(72)
                                .is_some_and(|item| item.kind() == ItemKind::IronSword) =>
                    {
                        return Ok(packet.container_id);
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("remote invsee number-key setup disconnected: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("remote invsee number-key window closed before content")
    })
    .await?;
    let inventory = actor.get_inventory()?;
    if inventory.id() != window {
        bail!("remote invsee number-key targeted an obsolete client window");
    }
    inventory.click(SwapClick {
        source_slot: 37,
        target_slot: 0,
    });
    wait_inventory_slot_kind(
        target_events,
        0,
        37,
        ItemKind::IronSword,
        deadline,
        "number-key remote target slot",
    )
    .await?;
    wait_inventory_slot_kind(
        events,
        0,
        36,
        ItemKind::Bow,
        deadline,
        "number-key local hotbar slot",
    )
    .await?;
    inventory.click(SwapClick {
        source_slot: 37,
        target_slot: 0,
    });
    wait_inventory_slot_kind(
        target_events,
        0,
        37,
        ItemKind::Bow,
        deadline,
        "number-key remote restoration",
    )
    .await?;
    wait_inventory_slot_kind(
        events,
        0,
        36,
        ItemKind::IronSword,
        deadline,
        "number-key local restoration",
    )
    .await?;
    inventory.close();
    eprintln!(
        "passed invsee-number-swap: remote target and local hotbar exchanged through Java number-key input and restored"
    );
    Ok(())
}

async fn run_invsee_armor_offhand(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let window = wait_invsee_open(
        actor,
        events,
        deadline,
        "editable armor/offhand invsee window",
    )
    .await?;
    let inventory = actor.get_inventory()?;
    if inventory.id() != window {
        bail!("armor/offhand edit targeted an obsolete window");
    }
    for (view_slot, target_slot, restored) in [
        (0usize, 5u16, ItemKind::IronHelmet),
        (8usize, 45u16, ItemKind::Shield),
    ] {
        inventory.left_click(view_slot);
        wait_inventory_slot_kind(
            target_events,
            0,
            target_slot,
            ItemKind::Air,
            deadline,
            "remote equipment removal",
        )
        .await?;
        inventory.left_click(view_slot);
        wait_inventory_slot_kind(
            target_events,
            0,
            target_slot,
            restored,
            deadline,
            "remote equipment restoration",
        )
        .await?;
    }
    for filler in 4..8 {
        inventory.left_click(filler);
        within(deadline, "blocked invsee filler correction", async {
            while let Some(event) = events.recv().await {
                match event {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetSlot(packet)
                            if packet.container_id == window && packet.slot == filler as u16 =>
                        {
                            if packet.item_stack.kind() != ItemKind::GrayStainedGlassPane {
                                bail!("invsee filler slot {filler} became editable");
                            }
                            return Ok(());
                        }
                        ClientboundGamePacket::ContainerSetContent(packet)
                            if packet.container_id == window =>
                        {
                            if packet
                                .items
                                .get(filler)
                                .is_none_or(|item| item.kind() != ItemKind::GrayStainedGlassPane)
                            {
                                bail!("invsee filler slot {filler} became editable");
                            }
                            return Ok(());
                        }
                        _ => {}
                    },
                    Event::Disconnect(reason) => {
                        bail!("invsee filler click disconnected viewer: {reason:?}")
                    }
                    _ => {}
                }
            }
            bail!("invsee filler click had no authoritative correction")
        })
        .await?;
    }
    inventory.close();
    eprintln!(
        "passed invsee-armor-offhand: remote equipment edits restored and dark filler stayed blocked"
    );
    Ok(())
}

async fn run_invsee_quickcraft(
    viewer: &Client,
    target: &Client,
    viewer_events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while viewer_events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    viewer.chat("/clustertest invsee-quickcraft");
    wait_chat_exact(
        viewer_events,
        "cluster test invsee-quickcraft accepted",
        deadline,
        "remote QuickCraft setup",
    )
    .await?;
    wait_until(viewer, deadline, "four-stone drag source cursor", || {
        let ecs = viewer.ecs.read();
        let inventory = ecs
            .get::<azalea::entity::inventory::Inventory>(viewer.entity)
            .context("QuickCraft viewer inventory missing")?;
        Ok(
            (inventory.carried.kind() == ItemKind::Stone && inventory.carried.count() == 4)
                .then_some(()),
        )
    })
    .await?;
    viewer.chat(&format!("/invsee edit {}", super::BOT_B));
    let window = wait_invsee_open(
        viewer,
        viewer_events,
        deadline,
        "remote QuickCraft viewer screen",
    )
    .await?;
    for (button, slot) in [(0, -999), (1, 36), (1, 37), (2, -999)] {
        viewer.write_packet(ServerboundContainerClick {
            container_id: window,
            state_id: 0,
            slot_num: slot,
            button_num: button,
            click_type: ClickType::QuickCraft,
            changed_slots: Default::default(),
            carried_item: HashedStack(None),
        });
    }
    within(deadline, "remote QuickCraft semantic conservation", async {
        let mut viewer_authoritative = false;
        let mut target_authoritative = false;
        let mut ticks = viewer.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => { tick.context("QuickCraft tick stream closed")?; }
                event = viewer_events.recv() => match event.context("QuickCraft viewer event stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == window && content.items.len() > 37
                            && content.items[36].kind() == ItemKind::Stone && content.items[36].count() == 2
                            && content.items[37].kind() == ItemKind::Stone && content.items[37].count() == 2 => viewer_authoritative = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("remote QuickCraft disconnected viewer: {reason:?}"),
                    _ => {}
                },
                event = target_events.recv() => match event.context("QuickCraft target event stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == 0 && content.items.len() > 37
                            && content.items[36].kind() == ItemKind::Stone && content.items[36].count() == 2
                            && content.items[37].kind() == ItemKind::Stone && content.items[37].count() == 2 => target_authoritative = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("remote QuickCraft disconnected target: {reason:?}"),
                    _ => {}
                },
            }
            if viewer_authoritative && target_authoritative {
                let viewer_ecs = viewer.ecs.read();
                let viewer_inventory = viewer_ecs.get::<azalea::entity::inventory::Inventory>(viewer.entity)
                    .context("QuickCraft viewer inventory missing after commit")?;
                let target_inventory = target.get_inventory()?;
                let slots = target_inventory.menu()?.context("QuickCraft target menu missing")?.slots();
                if viewer_inventory.carried.is_empty()
                    && slots.get(36).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 2)
                    && slots.get(37).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 2) {
                    return Ok(());
                }
            }
        }
    }).await?;
    viewer.write_packet(ServerboundContainerClose {
        container_id: window,
    });
    eprintln!(
        "passed invsee-quickcraft: four viewer cursor stones became two in each remote target hotbar slot through normal Java drag"
    );
    Ok(())
}

async fn run_invsee_stale_window(
    actor: &Client,
    target: &Client,
    events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let old = wait_invsee_open(actor, events, deadline, "first invsee session").await?;
    actor.write_packet(ServerboundContainerClose { container_id: old });
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let current = wait_invsee_open(actor, events, deadline, "replacement invsee session").await?;
    if current == old {
        bail!("reopened invsee reused the stale Java window ID immediately");
    }
    while events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.write_packet(ServerboundContainerClick {
        container_id: old,
        state_id: 0,
        slot_num: 37,
        button_num: 0,
        click_type: ClickType::Pickup,
        changed_slots: Default::default(),
        carried_item: HashedStack(None),
    });
    within(deadline, "stale invsee packet cannot mutate replacement session", async {
        let mut ticks = actor.get_tick_broadcaster();
        for _ in 0..10 {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed after stale window input")?;
                }
                event = events.recv() => {
                    match event.context("viewer event stream closed after stale window input")? {
                        Event::Disconnect(reason) => bail!("stale window packet disconnected viewer: {reason:?}"),
                        Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == current && slot.slot == 37 && slot.item_stack.kind() != ItemKind::Bow) => {
                            bail!("stale window click mutated replacement invsee content");
                        }
                        _ => {}
                    }
                }
                event = target_events.recv() => {
                    match event.context("target event stream closed after stale window input")? {
                        Event::Disconnect(reason) => bail!("stale window packet disconnected target: {reason:?}"),
                        Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == 0 && slot.slot == 37 && slot.item_stack.kind() != ItemKind::Bow) => {
                            bail!("stale window click changed the target's authoritative slot");
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }).await?;
    if actor.get_inventory()?.id() != current {
        bail!("stale window click closed the replacement window");
    }
    let actual = target
        .get_inventory()?
        .menu()?
        .context("target inventory absent")?;
    if actual
        .slots()
        .get(37)
        .is_none_or(|stack| stack.kind() != ItemKind::Bow)
    {
        bail!("stale window click mutated target inventory despite no packet delta");
    }
    actor.write_packet(ServerboundContainerClose {
        container_id: current,
    });
    eprintln!(
        "passed invsee-stale-window: old session click left replacement screen and target inventory unchanged"
    );
    Ok(())
}

async fn wait_invsee_open(
    client: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<i32> {
    let id = within(deadline, stage, async {
        let mut open = None;
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(packet)
                        if packet.menu_type == MenuKind::Generic9x5 =>
                    {
                        open = Some(packet.container_id);
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if open == Some(packet.container_id)
                            && packet
                                .items
                                .get(37)
                                .is_some_and(|stack| stack.kind() == ItemKind::Bow) =>
                    {
                        return Ok(packet.container_id);
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!("{stage} disconnected: {reason:?}"),
                _ => {}
            }
        }
        bail!("{stage} did not open a complete screen")
    })
    .await?;
    if client.get_inventory()?.id() != id {
        bail!("{stage} was not Azalea's current window");
    }
    Ok(id)
}

async fn run_invsee_refresh(
    actor: &Client,
    target: &Client,
    events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let window = wait_invsee_open(actor, events, deadline, "remote invsee refresh window").await?;
    actor.chat("/clustertest invsee-refresh-r2-r3");
    within(deadline, "remote invsee rapid accepted revisions", async {
        let mut accepted = false;
        let mut final_view = false;
        let mut final_target = false;
        while !(accepted && final_view && final_target) {
            tokio::select! {
                event = events.recv() => {
                    match event.context("viewer stream closed during remote refresh")? {
                        Event::Chat(chat) if chat.message().to_string() == "cluster test invsee-refresh-r2-r3 accepted" => accepted = true,
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::OpenScreen(_) => bail!("remote invsee refresh reopened the viewer window"),
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == window && packet.slot == 37 && packet.item_stack.kind() == ItemKind::Shield => final_view = true,
                            ClientboundGamePacket::ContainerSetContent(packet)
                                if packet.container_id == window && packet.items.get(37).is_some_and(|item| item.kind() == ItemKind::Shield) => final_view = true,
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("remote invsee refresh disconnected viewer: {reason:?}"),
                        _ => {}
                    }
                }
                event = target_events.recv() => {
                    match event.context("target stream closed during remote refresh")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == 0 && packet.slot == 37 && packet.item_stack.kind() == ItemKind::Shield => final_target = true,
                            ClientboundGamePacket::ContainerSetContent(packet)
                                if packet.container_id == 0 && packet.items.get(37).is_some_and(|item| item.kind() == ItemKind::Shield) => final_target = true,
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("remote invsee refresh disconnected target: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }).await?;
    if actor.get_inventory()?.id() != window {
        bail!("remote invsee rapid refresh lost the open window");
    }
    if target
        .get_inventory()?
        .menu()?
        .context("target inventory absent after refresh")?
        .slots()
        .get(37)
        .is_none_or(|item| item.kind() != ItemKind::Shield)
    {
        bail!("target final inventory revision did not contain the accepted shield");
    }
    actor.write_packet(ServerboundContainerClose {
        container_id: window,
    });
    eprintln!(
        "passed invsee-refresh-r2-r3: latest target revision reached the same open Java screen"
    );
    Ok(())
}

async fn run_invsee_wrap_window(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat(&format!("/invsee edit {}", super::BOT_B));
    let old = wait_invsee_open(actor, events, deadline, "pre-wrap invsee session").await?;
    actor.chat("/clustertest invsee-delay-old-refresh");
    wait_chat_exact(
        events,
        "cluster test invsee-delay-old-refresh accepted",
        deadline,
        "old invsee refresh held",
    )
    .await?;
    actor.write_packet(ServerboundContainerClose { container_id: old });
    let mut wrapped = None;
    for _ in 0..256 {
        actor.chat(&format!("/invsee edit {}", super::BOT_B));
        let id = wait_invsee_open(actor, events, deadline, "invsee Java ID wrap").await?;
        if id == old {
            wrapped = Some(id);
            break;
        }
        actor.write_packet(ServerboundContainerClose { container_id: id });
    }
    let current =
        wrapped.context("Java invsee window ID did not wrap through the previous value")?;
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest invsee-release-old-refresh");
    within(deadline, "old invsee invalidation cannot affect wrapped window", async {
        let mut released = false;
        let mut ticks = actor.get_tick_broadcaster();
        let mut elapsed = 0;
        while !released || elapsed < 10 {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed during wrapped window test")?;
                    elapsed += 1;
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed during wrapped window test")? {
                        Event::Chat(chat) if chat.message().to_string() == "cluster test invsee-release-old-refresh accepted" => released = true,
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::ContainerClose(packet) if packet.container_id == current => {
                                bail!("delayed old-session refresh closed the wrapped new window");
                            }
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == current && packet.slot == 37 && packet.item_stack.kind() != ItemKind::Bow => {
                                bail!("delayed old-session refresh changed wrapped window content");
                            }
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("wrapped invsee window disconnected: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }).await?;
    if actor.get_inventory()?.id() != current {
        bail!("wrapped Java ID no longer refers to the new invsee window");
    }
    actor.write_packet(ServerboundContainerClose {
        container_id: current,
    });
    eprintln!(
        "passed invsee-wrap-window: delayed old refresh did not affect a reused Java window ID"
    );
    Ok(())
}

async fn run_inventory_move_split_merge(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest inventory-move-split-merge");
    wait_chat_exact(
        events,
        "cluster test inventory-move-split-merge accepted",
        deadline,
        "inventory semantic setup",
    )
    .await?;
    wait_until(actor, deadline, "four-stone inventory setup", || {
        let menu = actor
            .get_inventory()?
            .menu()?
            .context("player inventory menu absent")?;
        let slots = menu.slots();
        Ok((slots
            .get(36)
            .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 4)
            && slots.get(37).is_some_and(|stack| stack.is_empty()))
        .then_some(()))
    })
    .await?;
    let inventory = actor
        .open_inventory()?
        .context("player inventory did not open")?;
    inventory.left_click(36usize);
    inventory.right_click(37usize);
    inventory.left_click(37usize);
    within(deadline, "semantic move/split/merge conservation", async {
        let mut source_empty = false;
        let mut destination_four = false;
        let mut authoritative_source = false;
        let mut authoritative_destination = false;
        let mut ticks = actor.get_tick_broadcaster();
        while !(source_empty && destination_four && authoritative_source && authoritative_destination) {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed during semantic inventory test")?;
                    let menu = actor.get_inventory()?.menu()?.context("player inventory menu absent")?;
                    let slots = menu.slots();
                    source_empty = slots.get(36).is_some_and(|stack| stack.is_empty());
                    destination_four = slots.get(37).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 4);
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed during semantic inventory test")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == 0 && packet.slot == 36 && packet.item_stack.is_empty() => authoritative_source = true,
                            ClientboundGamePacket::ContainerSetSlot(packet)
                                if packet.container_id == 0 && packet.slot == 37 && packet.item_stack.kind() == ItemKind::Stone && packet.item_stack.count() == 4 => authoritative_destination = true,
                            ClientboundGamePacket::ContainerSetContent(packet)
                                if packet.container_id == 0 && packet.items.len() > 37 => {
                                    authoritative_source |= packet.items[36].is_empty();
                                    authoritative_destination |= packet.items[37].kind() == ItemKind::Stone && packet.items[37].count() == 4;
                                },
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("semantic inventory move disconnected client: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }).await?;
    inventory.close();
    eprintln!(
        "passed inventory-move-split-merge: normal Java clicks preserved four stone with authoritative slot updates"
    );
    Ok(())
}

async fn run_inventory_pickup_rollback(
    actor: &Client,
    winner: &Client,
    events: &mut UnboundedReceiver<Event>,
    winner_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while winner_events.try_recv().is_ok() {}
    actor.chat("/clustertest inventory-pickup-rollback-split");
    wait_chat_exact(
        events,
        "cluster test inventory-pickup-rollback-split armed",
        deadline,
        "provisional pickup and late competing action",
    )
    .await?;
    wait_until(
        actor,
        deadline,
        "provisional four-stone pickup visible",
        || {
            let menu = actor
                .get_inventory()?
                .menu()?
                .context("provisional pickup inventory absent")?;
            Ok(menu
                .slots()
                .get(36)
                .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 4)
                .then_some(()))
        },
    )
    .await?;
    let inventory = actor
        .open_inventory()?
        .context("pickup/split inventory did not open")?;
    inventory.right_click(36usize);
    within(deadline, "late pickup winner changes dependent half-stack result", async {
        let mut slot_corrected = false;
        let mut cursor_corrected = false;
        let mut winner_corrected = false;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("pickup/split client tick stream closed")?;
                    let menu = actor.get_inventory()?.menu()?.context("pickup/split menu disappeared")?;
                    let winner_menu = winner.get_inventory()?.menu()?.context("pickup winner menu disappeared")?;
                    let ecs = actor.ecs.read();
                    let cursor = &ecs.get::<azalea::entity::inventory::Inventory>(actor.entity)
                        .context("pickup/split cursor absent")?.carried;
                    let source_total: i32 = menu.slots().iter().filter(|stack| stack.kind() == ItemKind::Stone).map(|stack| stack.count()).sum();
                    let winner_total: i32 = winner_menu.slots().iter().filter(|stack| stack.kind() == ItemKind::Stone).map(|stack| stack.count()).sum();
                    if slot_corrected && cursor_corrected && winner_corrected
                        && menu.slots().get(36).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1)
                        && cursor.kind() == ItemKind::Stone && cursor.count() == 1
                        && source_total == 1 && winner_total == 2 {
                        return Ok(());
                    }
                }
                event = events.recv() => match event.context("pickup/split actor stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == 0 && update.slot == 36
                            && update.item_stack.kind() == ItemKind::Stone && update.item_stack.count() == 1 => slot_corrected = true,
                        ClientboundGamePacket::ContainerSetContent(update) if update.container_id == 0 => {
                            slot_corrected |= update.items.get(36).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1);
                            cursor_corrected |= update.carried_item.kind() == ItemKind::Stone && update.carried_item.count() == 1;
                        }
                        ClientboundGamePacket::SetCursorItem(update) if update.contents.kind() == ItemKind::Stone && update.contents.count() == 1 => cursor_corrected = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("dependent inventory rollback disconnected A: {reason:?}"),
                    _ => {}
                },
                event = winner_events.recv() => match event.context("pickup winner stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == 0
                            && update.item_stack.kind() == ItemKind::Stone && update.item_stack.count() == 2 => winner_corrected = true,
                        ClientboundGamePacket::ContainerSetContent(update) if update.container_id == 0
                            && update.items.iter().any(|stack| stack.kind() == ItemKind::Stone && stack.count() == 2) => winner_corrected = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("pickup winner disconnected B: {reason:?}"),
                    _ => {}
                },
            }
        }
    }).await?;
    inventory.close();
    eprintln!(
        "passed inventory-pickup-rollback-split: late pickup rejection corrected a real half-stack action from 2/2 to 1/1 while B kept the contested two items"
    );
    Ok(())
}

async fn run_inventory_components(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest inventory-component-identity");
    wait_chat_exact(
        events,
        "cluster test inventory-component-identity accepted",
        deadline,
        "named stack setup",
    )
    .await?;
    let matches = |stack: &azalea_inventory::ItemStack, name: &str, count| {
        stack.kind() == ItemKind::Stone
            && stack.count() == count
            && stack
                .get_component::<CustomName>()
                .is_some_and(|component| component.name.to_string() == name)
    };
    wait_until(
        actor,
        deadline,
        "distinct canonical component identities",
        || {
            let menu = actor
                .get_inventory()?
                .menu()?
                .context("named stack inventory absent")?;
            let slots = menu.slots();
            Ok((slots
                .get(40)
                .is_some_and(|stack| matches(stack, "alpha", 2))
                && slots.get(41).is_some_and(|stack| matches(stack, "beta", 2))
                && slots.get(42).is_some_and(|stack| stack.is_empty()))
            .then_some(()))
        },
    )
    .await?;
    let inventory = actor
        .open_inventory()?
        .context("named stack player inventory did not open")?;
    inventory.left_click(40usize);
    inventory.right_click(42usize);
    inventory.left_click(41usize);
    inventory.left_click(40usize);
    within(deadline, "named stack semantic split and incompatible swap", async {
        let mut authoritative = HashSet::new();
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("named stack client tick stream closed")?;
                    let menu = actor.get_inventory()?.menu()?.context("named stack result menu absent")?;
                    let slots = menu.slots();
                    let ecs = actor.ecs.read();
                    let cursor = &ecs.get::<azalea::entity::inventory::Inventory>(actor.entity)
                        .context("named stack cursor absent")?.carried;
                    if authoritative.len() == 3 && cursor.is_empty()
                        && slots.get(40).is_some_and(|stack| matches(stack, "beta", 2))
                        && slots.get(41).is_some_and(|stack| matches(stack, "alpha", 1))
                        && slots.get(42).is_some_and(|stack| matches(stack, "alpha", 1))
                        && slots.iter().filter(|stack| stack.kind() == ItemKind::Stone).map(|stack| stack.count()).sum::<i32>() == 4 {
                        return Ok(());
                    }
                }
                event = events.recv() => match event.context("named stack stream closed")? {
                    Event::Packet(packet) => {
                        for (slot, name, count) in [(40, "beta", 2), (41, "alpha", 1), (42, "alpha", 1)] {
                            let stack = match packet.as_ref() {
                                ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == 0 && update.slot == slot => Some(&update.item_stack),
                                ClientboundGamePacket::ContainerSetContent(update) if update.container_id == 0 => update.items.get(slot as usize),
                                _ => None,
                            };
                            if stack.is_some_and(|stack| matches(stack, name, count)) { authoritative.insert(slot); }
                        }
                    }
                    Event::Disconnect(reason) => bail!("component-preserving inventory operation disconnected: {reason:?}"),
                    _ => {}
                },
            }
        }
    }).await?;
    inventory.close();
    eprintln!(
        "passed inventory-component-identity: normal split/swap kept four named stones with distinct canonical identities and authoritative slots"
    );
    Ok(())
}

async fn run_book_plugin_order(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let clicked = BlockPos::new(0, 80, 2);
    let placed = BlockPos::new(0, 81, 2);
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest book-plugin-order");
    wait_chat_exact(
        events,
        "cluster test book-plugin-order accepted",
        deadline,
        "accepted book/item setup",
    )
    .await?;
    wait_for_position(
        actor,
        Vec3::new(0.5, 80.0, 3.5),
        deadline,
        "book/item placement reach",
    )
    .await?;
    actor.set_selected_hotbar_slot(4);
    actor.write_packet(ServerboundEditBook {
        slot: 4,
        pages: vec!["claimed page".to_owned()],
        title: None,
    });
    actor.set_selected_hotbar_slot(5);
    let sequence = 0x7e4;
    actor.write_packet(ServerboundUseItemOn {
        hand: InteractionHand::MainHand,
        block_hit: BlockHit {
            block_pos: clicked,
            direction: Direction::Up,
            location: Vec3::new(0.5, 80.5, 2.5),
            inside: false,
            world_border: false,
        },
        seq: sequence,
    });
    within(deadline, "delayed book edit before item action", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == sequence) => return Ok(()),
                Event::Disconnect(reason) => bail!("book plugin ordering disconnected client: {reason:?}"),
                _ => {}
            }
        }
        bail!("book plugin ordering stream ended without item sequence ACK")
    }).await?;
    for (client, side) in [(actor, "editor"), (observer, "remote holder")] {
        wait_until(
            client,
            deadline,
            &format!("{side} ordered item placement"),
            || {
                Ok(client
                    .world()?
                    .read()
                    .get_block_state(placed)
                    .is_some_and(|state| BlockKind::from(state) == BlockKind::Stone)
                    .then_some(()))
            },
        )
        .await?;
    }
    eprintln!(
        "passed book-plugin-order: normal Java EditBook and following item placement both reached the client-visible accepted state"
    );
    Ok(())
}

async fn run_visual_state_cycle(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat("/clustertest visual-state-cycle");
    wait_chat_exact(
        events,
        "cluster test visual-state-cycle accepted",
        deadline,
        "accepted visual equipment setup",
    )
    .await?;
    let uuid = offline_uuid(BOT_A);
    let entity_id = observer
        .entity_by_uuid(uuid)
        .context("visual actor absent on observer")?
        .minecraft_id()?;
    wait_until(
        observer,
        deadline,
        "remote skin layers initially enabled",
        || {
            let remote = observer
                .entity_by_uuid(uuid)
                .context("visual actor despawned before skin update")?;
            Ok(remote
                .get_component::<azalea::entity::metadata::PlayerModeCustomisation>()
                .is_some_and(|value| value.0 == 0x7f)
                .then_some(()))
        },
    )
    .await?;
    let mut client_information = ClientInformation {
        view_distance: super::client_view_distance(),
        allows_listing: true,
        ..ClientInformation::default()
    };
    client_information.model_customization.cape = false;
    client_information.model_customization.jacket = false;
    client_information.model_customization.left_sleeve = false;
    client_information.model_customization.right_sleeve = false;
    client_information.model_customization.left_pants = false;
    client_information.model_customization.right_pants = false;
    client_information.model_customization.hat = false;
    actor.set_client_information(client_information)?;
    wait_until(observer, deadline, "remote skin layers cleared", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned during skin update")?;
        Ok(remote
            .get_component::<azalea::entity::metadata::PlayerModeCustomisation>()
            .is_some_and(|value| value.0 == 0)
            .then_some(()))
    })
    .await?;
    let self_id = *actor
        .ecs
        .read()
        .get::<azalea::core::entity_id::MinecraftEntityId>(actor.entity)
        .context("visual actor has no Java entity ID")?;
    actor.write_packet(ServerboundPlayerCommand {
        id: self_id,
        action: PlayerCommand::StartSprinting,
        data: 0,
    });
    wait_until(observer, deadline, "remote sprint started", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned during sprint")?;
        Ok(remote
            .get_component::<azalea::entity::metadata::Sprinting>()
            .is_some_and(|value| value.0)
            .then_some(()))
    })
    .await?;
    actor.write_packet(ServerboundPlayerCommand {
        id: self_id,
        action: PlayerCommand::StopSprinting,
        data: 0,
    });
    wait_until(observer, deadline, "remote sprint stopped", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned during sprint stop")?;
        Ok(remote
            .get_component::<azalea::entity::metadata::Sprinting>()
            .is_some_and(|value| !value.0)
            .then_some(()))
    })
    .await?;
    actor.write_packet(ServerboundSwing {
        hand: InteractionHand::MainHand,
    });
    within(deadline, "remote mainhand swing animation", async {
        while let Some(event) = observer_events.recv().await {
            match event {
                Event::Packet(packet) => {
                    if let ClientboundGamePacket::Animate(animation) = packet.as_ref() {
                        if animation.id == entity_id
                            && animation.action == azalea_protocol::packets::game::c_animate::AnimationAction::SwingMainHand {
                            return Ok(());
                        }
                    }
                }
                Event::Disconnect(reason) => bail!("visual swing disconnected observer: {reason:?}"),
                _ => {}
            }
        }
        bail!("observer packet stream ended before remote mainhand swing")
    }).await?;
    actor.set_selected_hotbar_slot(1);
    wait_until(observer, deadline, "remote shield equipped", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned before shield use")?;
        let shield = remote
            .get_component::<azalea::entity::inventory::Inventory>()
            .and_then(|inventory| {
                inventory
                    .get_equipment(EquipmentSlot::Mainhand)
                    .is_some_and(|stack| stack.kind() == ItemKind::Shield)
                    .then_some(())
            });
        Ok(shield)
    })
    .await?;
    actor.write_packet(ServerboundUseItem {
        hand: InteractionHand::MainHand,
        seq: 0x7e2,
        y_rot: 0.0,
        x_rot: 0.0,
    });
    wait_until(observer, deadline, "remote item use started", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned during item use")?;
        Ok(remote
            .get_component::<azalea::entity::metadata::AbstractLivingUsingItem>()
            .is_some_and(|value| value.0)
            .then_some(()))
    })
    .await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::ReleaseUseItem,
        pos: BlockPos::ZERO,
        direction: Direction::Down,
        seq: 0x7e3,
    });
    wait_until(observer, deadline, "remote item use aborted", || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("visual actor despawned during item-use abort")?;
        Ok(remote
            .get_component::<azalea::entity::metadata::AbstractLivingUsingItem>()
            .is_some_and(|value| !value.0)
            .then_some(()))
    })
    .await?;
    actor.set_client_information(ClientInformation {
        view_distance: super::client_view_distance(),
        allows_listing: true,
        ..ClientInformation::default()
    })?;
    eprintln!(
        "passed visual-state-cycle: Java skin-layer flags, mainhand swing, and item-use start/abort reached the remote avatar"
    );
    Ok(())
}

async fn run_visual_environment(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
    swimming: bool,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat(&format!("/clustertest {id}"));
    wait_chat_exact(events, &format!("cluster test {id} accepted"), deadline, id).await?;
    let prepared_position = if swimming {
        Vec3::new(0.5, 80.0, 6.5)
    } else {
        Vec3::new(0.5, 120.0, 6.5)
    };
    wait_for_position(actor, prepared_position, deadline, id).await?;
    let uuid = offline_uuid(BOT_A);
    if swimming {
        actor.walk(WalkDirection::Forward);
    } else {
        actor.set_jumping(true)?;
    }
    let result = wait_until(observer, deadline, id, || {
        let remote = observer
            .entity_by_uuid(uuid)
            .context("environmental visual actor absent")?;
        let enabled = if swimming {
            remote
                .get_component::<azalea::entity::metadata::Swimming>()
                .is_some_and(|value| value.0)
        } else {
            remote
                .get_component::<azalea::entity::metadata::FallFlying>()
                .is_some_and(|value| value.0)
        };
        Ok(enabled.then_some(()))
    })
    .await;
    actor.walk(WalkDirection::None);
    actor.set_jumping(false)?;
    result?;
    eprintln!(
        "passed {id}: normal client movement produced the expected remote avatar visual state"
    );
    Ok(())
}

async fn run_combat_effects(
    actor: &Client,
    target: &Client,
    actor_events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while actor_events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.chat("/clustertest combat-effects");
    wait_chat_exact(
        actor_events,
        "cluster test combat-effects accepted",
        deadline,
        "combat action setup",
    )
    .await?;
    let uuid = super::offline_uuid(super::BOT_B);
    let attacker_entity = target
        .entity_id_by_uuid(super::offline_uuid(super::BOT_A))
        .context("combat observer has no registered attacker avatar")?;
    let attacker_id = *target
        .ecs
        .read()
        .get::<azalea::core::entity_id::MinecraftEntityId>(attacker_entity)
        .context("combat observer attacker has no network entity ID")?;
    let target_position = target
        .position()
        .context("combat target has no real position")?;
    let entity_id = actor
        .entity_id_by_uuid(uuid)
        .context("combat test target has no remote avatar")?;
    let entity = actor
        .entity_by_uuid(uuid)
        .context("combat test target is not interactable")?;
    actor.look_at(
        entity
            .position()
            .context("combat target position missing")?,
    );
    if super::position_changed(
        entity
            .position()
            .context("combat replica position missing")?,
        target_position,
    ) {
        bail!("combat target replica did not agree with its controller position before attack");
    }
    actor.attack(entity_id);
    within(deadline, "canonical combat effects", async {
        let mut actual_health = false;
        let mut damage = false;
        let mut motion = false;
        let mut equipment = false;
        let mut statistics = false;
        let mut advancement = false;
        loop {
            tokio::select! {
                event = actor_events.recv() => match event.context("attacker stream closed during combat")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::DamageEvent(_) => damage = true,
                        ClientboundGamePacket::SetEntityMotion(_) => motion = true,
                        ClientboundGamePacket::AwardStats(stats) if !stats.stats.is_empty() => statistics = true,
                        ClientboundGamePacket::UpdateAdvancements(update) if !update.progress.is_empty() => advancement = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("attacker disconnected on normal combat action: {reason:?}"),
                    _ => {}
                },
                event = target_events.recv() => match event.context("target stream closed during combat")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::SetHealth(health) if health.health > 0.0 && health.health < 5.0 => actual_health = true,
                        ClientboundGamePacket::SetEntityMotion(_) => motion = true,
                        ClientboundGamePacket::SetEquipment(update)
                            if update.entity_id == attacker_id
                                && update.slots.slots.iter().any(|(slot, stack)| *slot == EquipmentSlot::Mainhand
                                && stack.get_component::<Damage>().is_some_and(|damage| damage.amount == 2)) => equipment = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("target disconnected on normal combat action: {reason:?}"),
                    _ => {}
                },
            }
            if actual_health && damage && motion && equipment && statistics && advancement {
                return Ok(());
            }
        }
    }).await?;
    eprintln!(
        "passed combat-effects: normal attack delivered real self health, damage, knockback, equipment, statistics and advancement packets"
    );
    Ok(())
}

async fn run_sweeping_fire_effects(
    actor: &Client,
    target: &Client,
    actor_events: &mut UnboundedReceiver<Event>,
    target_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let before = zombie_uuids(actor)?;
    while actor_events.try_recv().is_ok() {}
    while target_events.try_recv().is_ok() {}
    actor.chat("/clustertest sweeping-fire-effects");
    wait_chat_exact(
        actor_events,
        "cluster test sweeping-fire-effects accepted",
        deadline,
        "sweep/fire setup",
    )
    .await?;
    let mobs = wait_until(actor, deadline, "two sweep victims", || {
        let new = zombie_uuids(actor)?
            .difference(&before)
            .copied()
            .collect::<Vec<_>>();
        Ok((new.len() == 2).then_some(new))
    })
    .await?;
    for uuid in &mobs {
        wait_until(target, deadline, "remote sweep victim replica", || {
            Ok(target.entity_by_uuid(*uuid).map(|_| ()))
        })
        .await?;
    }
    let target_uuid = offline_uuid(super::BOT_B);
    let mut actor_ids = mobs
        .iter()
        .map(|uuid| {
            actor
                .entity_by_uuid(*uuid)
                .context("sweep actor mob absent")?
                .minecraft_id()
                .context("sweep actor mob network ID absent")
        })
        .collect::<Result<HashSet<_>>>()?;
    actor_ids.insert(
        actor
            .entity_by_uuid(target_uuid)
            .context("sweep primary victim absent")?
            .minecraft_id()?,
    );
    let mut target_ids = mobs
        .iter()
        .map(|uuid| {
            target
                .entity_by_uuid(*uuid)
                .context("sweep observer mob absent")?
                .minecraft_id()
                .context("sweep observer mob network ID absent")
        })
        .collect::<Result<HashSet<_>>>()?;
    target_ids.insert(
        *target
            .ecs
            .read()
            .get::<azalea::core::entity_id::MinecraftEntityId>(target.entity)
            .context("sweep player victim ID absent")?,
    );
    let attacker_id = target
        .entity_by_uuid(offline_uuid(BOT_A))
        .context("sweep attacker replica absent")?
        .minecraft_id()?;
    actor.walk(WalkDirection::None);
    actor.set_jumping(false)?;
    actor.set_selected_hotbar_slot(4);
    wait_until(actor, deadline, "sweep sword authoritative", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("sweep inventory absent")?;
        Ok(inventory
            .slots()
            .get(40)
            .is_some_and(|stack| {
                stack.kind() == ItemKind::IronSword
                    && stack
                        .get_component::<Damage>()
                        .is_some_and(|damage| damage.amount == 1)
            })
            .then_some(()))
    })
    .await?;
    actor.look_at(target.position()?);
    wait_ticks(actor, 25, deadline, "full sword cooldown").await?;
    actor.attack(
        actor
            .entity_id_by_uuid(target_uuid)
            .context("sweep target not interactable")?,
    );
    within(deadline, "sweeping attack, fire and durability on both holders", async {
        let mut actor_damage = HashSet::new();
        let mut target_damage = HashSet::new();
        let mut self_health = false;
        let mut durability = false;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("sweep client tick stream closed")?;
                    let burning = actor.entity_by_uuid(target_uuid)
                        .is_some_and(|entity| entity.get_component::<azalea::entity::metadata::OnFire>()
                            .is_some_and(|fire| fire.0));
                    if actor_damage == actor_ids && target_damage == target_ids && self_health && durability && burning {
                        return Ok(());
                    }
                }
                event = actor_events.recv() => match event.context("sweep attacker stream closed")? {
                    Event::Packet(packet) => {
                        if let ClientboundGamePacket::DamageEvent(damage) = packet.as_ref() {
                            if actor_ids.contains(&damage.entity_id) { actor_damage.insert(damage.entity_id); }
                        }
                    }
                    Event::Disconnect(reason) => bail!("sweeping attack disconnected actor: {reason:?}"),
                    _ => {}
                },
                event = target_events.recv() => match event.context("sweep observer stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::DamageEvent(damage) if target_ids.contains(&damage.entity_id) => { target_damage.insert(damage.entity_id); },
                        ClientboundGamePacket::SetHealth(health) if health.health < 20.0 => self_health = true,
                        ClientboundGamePacket::SetEquipment(update) if update.entity_id == attacker_id
                            && update.slots.slots.iter().any(|(slot, stack)| *slot == EquipmentSlot::Mainhand
                                && stack.get_component::<Damage>().is_some_and(|damage| damage.amount == 2)) => durability = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("sweep victim disconnected: {reason:?}"),
                    _ => {}
                },
            }
        }
    }).await?;
    eprintln!(
        "passed sweeping-fire-effects: one normal attack damaged B and two mobs on both holders, ignited B and consumed one sword durability"
    );
    Ok(())
}

async fn run_projectile_effects(
    actor: &Client,
    observer: &Client,
    actor_events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    while actor_events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat(&format!("/clustertest {id}"));
    wait_chat_exact(
        actor_events,
        &format!("cluster test {id} accepted"),
        deadline,
        "projectile weapon and target setup",
    )
    .await?;
    let actor_target_id = actor
        .entity_by_uuid(offline_uuid(super::BOT_B))
        .context("projectile target replica absent")?
        .minecraft_id()?;
    let observer_target_id = *observer
        .ecs
        .read()
        .get::<azalea::core::entity_id::MinecraftEntityId>(observer.entity)
        .context("projectile target has no own network entity ID")?;
    let crossbow = id == "crossbow-effects";
    actor.set_selected_hotbar_slot(4);
    wait_until(actor, deadline, "projectile weapon equipped", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("weapon inventory menu absent")?;
        Ok(inventory
            .slots()
            .get(40)
            .is_some_and(|stack| {
                stack.kind()
                    == if crossbow {
                        ItemKind::Crossbow
                    } else {
                        ItemKind::Bow
                    }
            })
            .then_some(()))
    })
    .await?;
    actor.write_packet(ServerboundUseItem {
        hand: InteractionHand::MainHand,
        seq: 0x7d0,
        y_rot: 0.0,
        x_rot: 0.0,
    });
    wait_ticks(
        actor,
        if crossbow { 30 } else { 8 },
        deadline,
        "projectile charge",
    )
    .await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::ReleaseUseItem,
        pos: BlockPos::new(0, 0, 0),
        direction: Direction::Down,
        seq: 0x7d1,
    });
    if crossbow {
        wait_until(actor, deadline, "crossbow authoritative charge", || {
            let inventory = actor
                .get_inventory()?
                .menu()?
                .context("crossbow inventory absent")?;
            Ok(inventory
                .slots()
                .get(40)
                .is_some_and(|stack| {
                    stack.kind() == ItemKind::Crossbow
                        && stack
                            .get_component::<ChargedProjectiles>()
                            .is_some_and(|charged| {
                                charged.items.len() == 1
                                    && charged.items[0].kind() == ItemKind::Arrow
                            })
                })
                .then_some(()))
        })
        .await?;
        actor.write_packet(ServerboundUseItem {
            hand: InteractionHand::MainHand,
            seq: 0x7d2,
            y_rot: 0.0,
            x_rot: 0.0,
        });
    }
    within(deadline, "projectile spawn and damage on both clients", async {
        let mut actor_arrow = false;
        let mut observer_arrow = false;
        let mut actor_damage = false;
        let mut observer_damage = false;
        let mut target_health = false;
        loop {
            tokio::select! {
                event = actor_events.recv() => match event.context("bow user stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::AddEntity(add) if add.entity_type == EntityKind::Arrow => actor_arrow = true,
                        ClientboundGamePacket::DamageEvent(damage) if damage.entity_id == actor_target_id => actor_damage = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("bow user disconnected: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("projectile observer stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::AddEntity(add) if add.entity_type == EntityKind::Arrow => observer_arrow = true,
                        ClientboundGamePacket::DamageEvent(damage) if damage.entity_id == observer_target_id => observer_damage = true,
                        ClientboundGamePacket::SetHealth(health) if health.health < 20.0 => target_health = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("projectile observer disconnected: {reason:?}"),
                    _ => {}
                },
            }
            if actor_arrow && observer_arrow && actor_damage && observer_damage && target_health { return Ok(()); }
        }
    }).await?;
    eprintln!(
        "passed {id}: normal weapon use/release created an arrow, target-specific damage on both holders and actual target HUD damage"
    );
    Ok(())
}

async fn run_virtual_crafting(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest virtual-crafting");
    wait_chat_exact(
        events,
        "cluster test virtual-crafting accepted",
        deadline,
        "virtual grid setup",
    )
    .await?;
    wait_until(actor, deadline, "two-plank crafting preview", || {
        let inventory = actor.get_inventory()?;
        let menu = inventory.menu()?.context("player crafting grid absent")?;
        let slots = menu.slots();
        Ok((slots
            .get(0)
            .is_some_and(|stack| stack.kind() == ItemKind::Stick && stack.count() == 4)
            && slots
                .get(1)
                .is_some_and(|stack| stack.kind() == ItemKind::OakPlanks && stack.count() == 1)
            && slots
                .get(3)
                .is_some_and(|stack| stack.kind() == ItemKind::OakPlanks && stack.count() == 1))
        .then_some(()))
    })
    .await?;
    actor.get_inventory()?.left_click(0usize);
    within(deadline, "accepted virtual crafting transfer", async {
        let mut authoritative = false;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => { tick.context("crafting tick stream closed")?; }
                event = events.recv() => match event.context("crafting event stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == 0 && content.items.len() > 3
                            && content.items[1].is_empty() && content.items[3].is_empty() => authoritative = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("virtual crafting result click disconnected client: {reason:?}"),
                    _ => {}
                },
            }
            let ecs = actor.ecs.read();
            let inventory = ecs.get::<azalea::entity::inventory::Inventory>(actor.entity)
                .context("Azalea player inventory missing after crafting")?;
            let slots = inventory.inventory_menu.slots();
            if authoritative && inventory.carried.kind() == ItemKind::Stick && inventory.carried.count() == 4
                && slots.get(1).is_some_and(|stack| stack.is_empty())
                && slots.get(3).is_some_and(|stack| stack.is_empty()) {
                return Ok(());
            }
        }
    }).await?;
    eprintln!(
        "passed virtual-crafting: normal Java result click consumed two planks once and produced four sticks"
    );
    Ok(())
}

async fn run_untrusted_result_reject(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest untrusted-result-reject");
    let (window, mut state_id, input) = within(deadline, "untrusted anvil preview", async {
        let mut window = None;
        let mut accepted = false;
        let mut preview = None;
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat)
                    if chat.message().to_string()
                        == "cluster test untrusted-result-reject accepted" =>
                {
                    accepted = true
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(open)
                        if open.menu_type == MenuKind::Anvil =>
                    {
                        window = Some(open.container_id)
                    }
                    ClientboundGamePacket::ContainerSetContent(content)
                        if window == Some(content.container_id)
                            && content.items.len() > 2
                            && !content.items[0].is_empty()
                            && !content.items[2].is_empty() =>
                    {
                        preview = Some((
                            content.container_id,
                            content.state_id,
                            content.items[0].clone(),
                        ));
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("untrusted result setup disconnected client: {reason:?}")
                }
                _ => {}
            }
            if accepted && let Some(preview) = preview.take() {
                return Ok(preview);
            }
        }
        bail!("anvil preview was not presented after accepted setup")
    })
    .await?;
    for click_type in [ClickType::Pickup, ClickType::QuickMove] {
        actor.write_packet(ServerboundContainerClick {
            container_id: window,
            state_id,
            slot_num: 2,
            button_num: 0,
            click_type,
            changed_slots: Default::default(),
            carried_item: HashedStack(None),
        });
        state_id = within(deadline, "untrusted output correction", async {
            while let Some(event) = events.recv().await {
                match event {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetContent(content)
                            if content.container_id == window && content.items.len() > 2 =>
                        {
                            if content.items[0] != input || content.items[2].is_empty() {
                                bail!("untrusted result click consumed input or removed preview");
                            }
                            return Ok(content.state_id);
                        }
                        ClientboundGamePacket::ContainerSetSlot(slot)
                            if slot.container_id == window
                                && slot.slot == 0
                                && slot.item_stack != input =>
                        {
                            bail!("untrusted result click changed input");
                        }
                        _ => {}
                    },
                    Event::Disconnect(reason) => {
                        bail!("untrusted result click disconnected client: {reason:?}")
                    }
                    _ => {}
                }
            }
            bail!("untrusted result click received no authoritative correction")
        })
        .await?;
    }
    actor.write_packet(ServerboundContainerClose {
        container_id: window,
    });
    eprintln!(
        "passed untrusted-result-reject: pickup and shift-click were corrected without consuming anvil input"
    );
    Ok(())
}

async fn run_inventory_placement_conflict(
    placer: &Client,
    editor: &Client,
    placer_events: &mut UnboundedReceiver<Event>,
    editor_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let clicked = BlockPos::new(0, 80, 2);
    let placed = BlockPos::new(0, 81, 2);
    while placer_events.try_recv().is_ok() {}
    while editor_events.try_recv().is_ok() {}
    placer.chat("/clustertest inventory-placement-conflict");
    wait_chat_exact(
        placer_events,
        "cluster test inventory-placement-conflict accepted",
        deadline,
        "joint item/world conflict setup",
    )
    .await?;
    wait_for_position(
        placer,
        Vec3::new(0.5, 80.0, 3.5),
        deadline,
        "joint conflict placement reach",
    )
    .await?;
    editor.chat(&format!("/invsee edit {}", super::BOT_A));
    let window = wait_invsee_open(
        editor,
        editor_events,
        deadline,
        "joint conflict remote editor",
    )
    .await?;
    wait_until(editor, deadline, "remote editable source stack", || {
        let menu = editor
            .get_inventory()?
            .menu()?
            .context("remote editor menu missing")?;
        Ok(menu
            .slots()
            .get(40)
            .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1)
            .then_some(()))
    })
    .await?;
    placer.set_selected_hotbar_slot(4);
    let sequence = 0x7e1;
    placer.write_packet(ServerboundUseItemOn {
        hand: InteractionHand::MainHand,
        block_hit: BlockHit {
            block_pos: clicked,
            direction: Direction::Up,
            location: Vec3::new(0.5, 80.5, 2.5),
            inside: false,
            world_border: false,
        },
        seq: sequence,
    });
    editor.write_packet(ServerboundContainerClick {
        container_id: window,
        state_id: 0,
        slot_num: 40,
        button_num: 0,
        click_type: ClickType::Pickup,
        changed_slots: Default::default(),
        carried_item: HashedStack(None),
    });
    within(deadline, "same-tick inventory placement conflict", async {
        let mut acknowledged = false;
        let mut source_corrected = false;
        let mut editor_corrected = false;
        let mut ticks = placer.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => { tick.context("joint conflict tick stream closed")?; }
                event = placer_events.recv() => match event.context("placer event stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::BlockChangedAck(ack) if ack.seq == sequence => acknowledged = true,
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == 0 && content.items.get(40).is_some_and(|stack| stack.is_empty()) => source_corrected = true,
                        ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == 0 && slot.slot == 40 && slot.item_stack.is_empty() => source_corrected = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("joint conflict disconnected placer: {reason:?}"),
                    _ => {}
                },
                event = editor_events.recv() => match event.context("editor event stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == window && content.items.get(40).is_some_and(|stack| stack.is_empty()) => editor_corrected = true,
                        ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == window && slot.slot == 40 && slot.item_stack.is_empty() => editor_corrected = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("joint conflict disconnected editor: {reason:?}"),
                    _ => {}
                },
            }
            if !(acknowledged && source_corrected && editor_corrected) { continue; }
            let a = placer.world()?.read().get_block_state(placed).context("placed target absent on placer")?;
            let b = editor.world()?.read().get_block_state(placed).context("placed target absent on editor")?;
            if a != b { continue; }
            let editor_ecs = editor.ecs.read();
            let cursor = &editor_ecs.get::<azalea::entity::inventory::Inventory>(editor.entity)
                .context("editor inventory absent after conflict")?.carried;
            let block = BlockKind::from(a);
            if (block == BlockKind::Stone && cursor.is_empty())
                || (block == BlockKind::Air && cursor.kind() == ItemKind::Stone && cursor.count() == 1) {
                return Ok(());
            }
            if !matches!(block, BlockKind::Air | BlockKind::Stone) {
                bail!("joint conflict produced an invalid placed block: {block:?}");
            }
        }
    }).await?;
    editor.write_packet(ServerboundContainerClose {
        container_id: window,
    });
    eprintln!(
        "passed inventory-placement-conflict: normal placement and remote semantic item move settled with one conserved stone"
    );
    Ok(())
}

async fn run_inventory_conflict(
    a: &Client,
    b: &Client,
    a_events: &mut UnboundedReceiver<Event>,
    b_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let chest = BlockPos::new(2, 80, 4);
    while a_events.try_recv().is_ok() {}
    while b_events.try_recv().is_ok() {}
    a.chat("/clustertest inventory-conflict");
    wait_chat_exact(
        a_events,
        "cluster test inventory-conflict accepted",
        deadline,
        "shared inventory conflict setup",
    )
    .await?;
    for client in [a, b] {
        wait_for_position(
            client,
            Vec3::new(2.5, 80.0, 3.5),
            deadline,
            "shared inventory conflict reach",
        )
        .await?;
    }
    let (a_window, b_window) = tokio::try_join!(
        within(deadline, "A opens contested chest", async {
            a.open_container_at(chest)
                .await?
                .context("A contested chest absent")
        }),
        within(deadline, "B opens contested chest", async {
            b.open_container_at(chest)
                .await?
                .context("B contested chest absent")
        }),
    )?;
    for client in [a, b] {
        wait_until(client, deadline, "contested chest has one stone", || {
            let inventory = client.get_inventory()?;
            let menu = inventory.menu()?.context("contested chest menu absent")?;
            Ok(menu
                .slots()
                .first()
                .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1)
                .then_some(()))
        })
        .await?;
    }
    a_window.left_click(0usize);
    b_window.left_click(0usize);
    within(deadline, "contested chest single winner", async {
        let mut a_ticks = a.get_tick_broadcaster();
        let mut b_ticks = b.get_tick_broadcaster();
        let mut a_authoritative = false;
        let mut b_authoritative = false;
        loop {
            tokio::select! {
                tick = a_ticks.recv() => { tick.context("A tick stream closed during inventory conflict")?; }
                tick = b_ticks.recv() => { tick.context("B tick stream closed during inventory conflict")?; }
                event = a_events.recv() => match event.context("A event stream closed during inventory conflict")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == a_window.id() && slot.slot == 0 && slot.item_stack.is_empty() => a_authoritative = true,
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == a_window.id() && content.items.first().is_some_and(|stack| stack.is_empty()) => a_authoritative = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("A disconnected in inventory conflict: {reason:?}"),
                    _ => {}
                },
                event = b_events.recv() => match event.context("B event stream closed during inventory conflict")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::ContainerSetSlot(slot) if slot.container_id == b_window.id() && slot.slot == 0 && slot.item_stack.is_empty() => b_authoritative = true,
                        ClientboundGamePacket::ContainerSetContent(content) if content.container_id == b_window.id() && content.items.first().is_some_and(|stack| stack.is_empty()) => b_authoritative = true,
                        _ => {}
                    },
                    Event::Disconnect(reason) => bail!("B disconnected in inventory conflict: {reason:?}"),
                    _ => {}
                },
            }
            let ai = a.get_inventory()?;
            let bi = b.get_inventory()?;
            let ac = ai.menu()?.context("A chest menu absent")?.slots().first().cloned().context("A chest slot absent")?;
            let bc = bi.menu()?.context("B chest menu absent")?.slots().first().cloned().context("B chest slot absent")?;
            let a_ecs = a.ecs.read();
            let b_ecs = b.ecs.read();
            let a_carried = &a_ecs.get::<azalea::entity::inventory::Inventory>(a.entity).context("A inventory entity absent")?.carried;
            let b_carried = &b_ecs.get::<azalea::entity::inventory::Inventory>(b.entity).context("B inventory entity absent")?.carried;
            let carried = a_carried.count() + b_carried.count();
            if a_authoritative && b_authoritative && ac.is_empty() && bc.is_empty() && carried == 1 {
                if !matches!((a_carried.kind(), b_carried.kind()), (ItemKind::Stone, _) | (_, ItemKind::Stone)) {
                    bail!("inventory conflict winner did not carry the unique stone");
                }
                return Ok(());
            }
        }
    }).await?;
    a_window.close();
    b_window.close();
    eprintln!(
        "passed inventory-conflict: two normal Java clicks produced one cursor winner and one shared empty slot"
    );
    Ok(())
}

async fn run_container_transfer(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let chest = BlockPos::new(2, 80, 4);
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest container-transfer");
    wait_chat_exact(
        events,
        "cluster test container-transfer accepted",
        deadline,
        "shared chest setup",
    )
    .await?;
    for client in [actor, observer] {
        wait_until(client, deadline, "shared chest visible", || {
            Ok(client
                .world()?
                .read()
                .get_block_state(chest)
                .is_some_and(|state| BlockKind::from(state) == BlockKind::Chest)
                .then_some(()))
        })
        .await?;
    }
    wait_for_position(
        actor,
        Vec3::new(1.5, 80.0, 3.5),
        deadline,
        "chest actor reach",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(2.5, 80.0, 3.5),
        deadline,
        "chest observer reach",
    )
    .await?;
    let open = within(deadline, "normal Java chest open", async {
        actor
            .open_container_at(chest)
            .await?
            .context("actor chest did not open")
    })
    .await?;
    wait_until(actor, deadline, "chest source stone", || {
        let menu = actor
            .get_inventory()?
            .menu()?
            .context("actor chest menu absent")?;
        Ok((menu
            .slots()
            .get(0)
            .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1)
            && menu.slots().get(54).is_some_and(|stack| stack.is_empty()))
        .then_some(()))
    })
    .await?;
    open.left_click(0usize);
    open.left_click(54usize);
    within(deadline, "chest-to-player authoritative transfer", async {
        let mut chest_empty = false;
        let mut player_one = false;
        let mut ticks = actor.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("Azalea tick stream closed during chest transfer")?;
                    if chest_empty && player_one { return Ok(()); }
                }
                event = events.recv() => {
                    match event.context("Azalea chest transfer event stream closed")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::ContainerSetSlot(packet) if packet.container_id == open.id() && packet.slot == 0 && packet.item_stack.is_empty() => chest_empty = true,
                            ClientboundGamePacket::ContainerSetSlot(packet) if packet.container_id == 0 && packet.slot == 36 && packet.item_stack.kind() == ItemKind::Stone && packet.item_stack.count() == 1 => player_one = true,
                            ClientboundGamePacket::ContainerSetContent(packet) if packet.container_id == open.id() && packet.items.first().is_some_and(|stack| stack.is_empty()) => chest_empty = true,
                            ClientboundGamePacket::ContainerSetContent(packet) if packet.container_id == 0 && packet.items.get(36).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1) => player_one = true,
                            _ => {}
                        },
                        Event::Disconnect(reason) => bail!("chest transfer disconnected actor: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
    }).await?;
    open.close();
    let remote_open = within(deadline, "other holder opens same chest", async {
        observer
            .open_container_at(chest)
            .await?
            .context("observer chest did not open")
    })
    .await?;
    wait_until(
        observer,
        deadline,
        "other holder sees consumed chest slot",
        || {
            let menu = observer
                .get_inventory()?
                .menu()?
                .context("observer chest menu absent")?;
            Ok(menu
                .slots()
                .first()
                .is_some_and(|stack| stack.is_empty())
                .then_some(()))
        },
    )
    .await?;
    remote_open.close();
    eprintln!(
        "passed container-transfer: normal Java chest click moved one stone and the other holder saw the empty slot"
    );
    Ok(())
}

async fn run_ender_chest_reopen(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let ender_chest = BlockPos::new(3, 80, 4);
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest ender-chest-reopen");
    wait_chat_exact(
        events,
        "cluster test ender-chest-reopen accepted",
        deadline,
        "Ender chest canonical setup",
    )
    .await?;
    for client in [actor, observer] {
        wait_until(client, deadline, "Ender chest visible", || {
            Ok(client
                .world()?
                .read()
                .get_block_state(ender_chest)
                .is_some_and(|state| BlockKind::from(state) == BlockKind::EnderChest)
                .then_some(()))
        })
        .await?;
    }
    wait_for_position(
        actor,
        Vec3::new(2.5, 80.0, 3.5),
        deadline,
        "Ender owner reach",
    )
    .await?;
    wait_for_position(
        observer,
        Vec3::new(3.5, 80.0, 3.5),
        deadline,
        "Ender other-player reach",
    )
    .await?;
    for opening in 0..2 {
        let screen = within(deadline, "Ender chest owner open", async {
            actor
                .open_container_at(ender_chest)
                .await?
                .context("owner Ender chest did not open")
        })
        .await?;
        wait_until(
            actor,
            deadline,
            &format!("Ender owner item opening {opening}"),
            || {
                let menu = actor
                    .get_inventory()?
                    .menu()?
                    .context("owner Ender menu absent")?;
                Ok(menu
                    .slots()
                    .first()
                    .is_some_and(|item| item.kind() == ItemKind::Stone && item.count() == 1)
                    .then_some(()))
            },
        )
        .await?;
        screen.close();
    }
    let other = within(deadline, "other player opens private Ender chest", async {
        observer
            .open_container_at(ender_chest)
            .await?
            .context("other Ender chest did not open")
    })
    .await?;
    wait_until(
        observer,
        deadline,
        "private Ender inventory isolation",
        || {
            let menu = observer
                .get_inventory()?
                .menu()?
                .context("other Ender menu absent")?;
            Ok(menu
                .slots()
                .first()
                .is_some_and(|item| item.is_empty())
                .then_some(()))
        },
    )
    .await?;
    other.close();
    eprintln!(
        "passed ender-chest-reopen: owner item survived close/open and another player saw private empty contents"
    );
    Ok(())
}

pub(super) async fn verify_ender_chest_after_restart(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected.split(',').any(|name| name == "ender-chest-reopen") {
        return Ok(());
    }
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest ender-chest-world");
    wait_chat_exact(
        events,
        "cluster test ender-chest-world accepted",
        deadline,
        "Ender chest world-only restart setup",
    )
    .await?;
    let pos = BlockPos::new(3, 80, 4);
    wait_for_position(
        actor,
        Vec3::new(2.5, 80.0, 3.5),
        deadline,
        "Ender chest restart reach",
    )
    .await?;
    let screen = within(deadline, "Ender chest after full process restart", async {
        actor
            .open_container_at(pos)
            .await?
            .context("persisted Ender chest did not open")
    })
    .await?;
    wait_until(actor, deadline, "persisted private Ender item", || {
        let menu = actor
            .get_inventory()?
            .menu()?
            .context("persisted Ender menu absent")?;
        Ok(menu
            .slots()
            .first()
            .is_some_and(|item| item.kind() == ItemKind::Stone && item.count() == 1)
            .then_some(()))
    })
    .await?;
    screen.close();
    eprintln!(
        "passed ender-chest-persistence: private item survived a full primary/secondary restart"
    );
    Ok(())
}

pub(super) async fn prepare_cursor_crafting_departure(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    let saturated = selected
        .split(',')
        .any(|name| name == "cursor-crafting-departure-saturated");
    if !saturated
        && !selected
            .split(',')
            .any(|name| name == "cursor-crafting-departure")
    {
        return Ok(());
    }
    while events.try_recv().is_ok() {}
    actor.chat("/clustertest cursor-crafting-departure");
    wait_chat_exact(
        events,
        "cluster test cursor-crafting-departure accepted",
        deadline,
        "cursor/crafting departure seed",
    )
    .await?;
    wait_until(actor, deadline, "cursor/crafting two-stone seed", || {
        let inventory = actor
            .get_inventory()?
            .menu()?
            .context("player inventory absent")?;
        Ok(inventory
            .slots()
            .get(36)
            .is_some_and(|item| item.kind() == ItemKind::Stone && item.count() == 2)
            .then_some(()))
    })
    .await?;
    let inventory = actor.get_inventory()?;
    inventory.left_click(36usize);
    inventory.right_click(1usize);
    wait_until(
        actor,
        deadline,
        "held cursor and crafting input before disconnect",
        || {
            let ecs = actor.ecs.read();
            let inventory = ecs
                .get::<azalea::entity::inventory::Inventory>(actor.entity)
                .context("Azalea inventory entity missing")?;
            Ok((inventory.carried.kind() == ItemKind::Stone
                && inventory.carried.count() == 1
                && inventory
                    .inventory_menu
                    .slots()
                    .get(1)
                    .is_some_and(|item| item.kind() == ItemKind::Stone && item.count() == 1))
            .then_some(()))
        },
    )
    .await?;
    if saturated {
        actor.chat("/clustertest inventory-fifo-saturate");
        wait_chat_exact(
            events,
            "cluster test inventory-fifo-saturate accepted",
            deadline,
            "bounded ingress FIFO saturated before departure",
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn verify_cursor_crafting_after_reconnect(
    actor: &Client,
    deadline: &Deadline,
) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if !selected.split(',').any(|name| {
        matches!(
            name,
            "cursor-crafting-departure" | "cursor-crafting-departure-saturated"
        )
    }) {
        return Ok(());
    }
    wait_until(
        actor,
        deadline,
        "cursor/crafting exact-once departure return",
        || {
            let ecs = actor.ecs.read();
            let inventory = ecs
                .get::<azalea::entity::inventory::Inventory>(actor.entity)
                .context("reconnected Azalea inventory entity missing")?;
            let slots = inventory.inventory_menu.slots();
            let stone = slots
                .iter()
                .filter(|item| item.kind() == ItemKind::Stone)
                .map(|item| item.count())
                .sum::<i32>();
            Ok((stone == 2
                && inventory.carried.is_empty()
                && (1..=4).all(|slot| slots.get(slot).is_some_and(|item| item.is_empty())))
            .then_some(()))
        },
    )
    .await?;
    eprintln!(
        "passed cursor-crafting-departure: two stones returned exactly once, cursor/grid empty after relog"
    );
    Ok(())
}

fn reject_overlay_leak(event: &Event) -> Result<()> {
    match event {
        Event::Packet(packet) => match packet.as_ref() {
            ClientboundGamePacket::BlockUpdate(update)
                if update.pos == CONFLICT_POSITION
                    && BlockKind::from(update.block_state) == BlockKind::OakPlanks =>
            {
                bail!("actor-private oak overlay leaked to a client before canonical resolution");
            }
            ClientboundGamePacket::Login(_) | ClientboundGamePacket::Respawn(_) => {
                bail!("actor overlay test triggered a terrain transition")
            }
            _ => {}
        },
        Event::Disconnect(reason) => bail!("actor overlay disconnected client: {reason:?}"),
        _ => {}
    }
    Ok(())
}

pub(super) async fn verify_live_lease_reconnect(
    node: &super::Node,
    operator: &Client,
    operator_events: &mut UnboundedReceiver<Event>,
    old_client: &Client,
    deadline: &Deadline,
) -> Result<Option<(Client, UnboundedReceiver<Event>)>> {
    if !env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "live-lease-reconnect")
    {
        eprintln!(
            "untested live-lease-reconnect: delayed old-incarnation proposal and deterministic GID-reuse hook missing"
        );
        return Ok(None);
    }
    operator.chat("/clustertest live-lease-arm");
    wait_chat_exact(
        operator_events,
        "cluster test live-lease-reconnect armed",
        deadline,
        "capture old C live lease before disconnect",
    )
    .await?;
    old_client.disconnect();
    operator.chat("/clustertest live-lease-retired");
    wait_chat_exact(
        operator_events,
        "cluster test live-lease-reconnect retired",
        deadline,
        "old C lease closed and logout committed",
    )
    .await?;
    let (replacement, mut events) = super::connect(
        super::BOT_C,
        node.unrelated_java
            .as_deref()
            .context("C-specific lease forwarding endpoint absent")?,
        deadline,
    )
    .await?;
    let login = super::wait_login(&mut events, deadline, "lease replacement login", None).await?;
    let lobby = super::wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "lease replacement virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    super::wait_for_local_promotion(
        &replacement,
        &mut events,
        super::BOT_C,
        false,
        deadline,
        "lease replacement real promotion",
    )
    .await?;
    let before = replacement.position()?;
    operator.chat("/clustertest live-lease-release");
    wait_chat_exact(
        operator_events,
        "cluster test live-lease-reconnect rejected",
        deadline,
        "old-incarnation proposal rejects on reused GID",
    )
    .await?;
    within(deadline, "replacement stays intact after stale live lease", async {
        let mut ticks = replacement.get_tick_broadcaster();
        for _ in 0..20 {
            tokio::select! {
                tick = ticks.recv() => {
                    tick.context("lease replacement tick stream ended")?;
                    if super::position_changed(replacement.position()?, before) {
                        bail!("old lease action changed replacement player pose");
                    }
                }
                event = events.recv() => match event.context("lease replacement stream ended")? {
                    Event::Disconnect(reason) => bail!("stale lease disconnected replacement client: {reason:?}"),
                    Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::Respawn(_) | ClientboundGamePacket::Login(_)) => bail!("stale lease emitted replacement terrain transition"),
                    _ => {},
                }
            }
        }
        Ok(())
    }).await?;
    replacement.chat("/list");
    wait_chat_contains(
        &mut events,
        super::BOT_C,
        deadline,
        "replacement remains responsive after stale proposal",
    )
    .await?;
    Ok(Some((replacement, events)))
}

async fn run_creative_clone_source_cursor(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    for (phase, expected_kind, expected_count) in [
        ("source", ItemKind::OakPlanks, 64),
        ("cursor", ItemKind::Stone, 1),
    ] {
        while events.try_recv().is_ok() {}
        actor.chat(&format!("/clustertest creative-clone-{phase}-arm"));
        wait_chat_exact(
            events,
            &format!("cluster test creative-clone-{phase} armed"),
            deadline,
            "canonical creative conflict staging",
        )
        .await?;
        let inventory = actor
            .open_inventory()?
            .context("creative inventory absent")?;
        let state_id = actor
            .ecs
            .read()
            .get::<azalea::entity::inventory::Inventory>(actor.entity)
            .context("creative authoritative inventory missing")?
            .state_id;
        actor.write_packet(ServerboundContainerClick {
            container_id: 0,
            state_id,
            slot_num: 36,
            button_num: 2,
            click_type: ClickType::Clone,
            changed_slots: Default::default(),
            carried_item: HashedStack(None),
        });
        wait_chat_exact(
            events,
            &format!("cluster test creative-clone-{phase} captured"),
            deadline,
            "Java clone materialized in original-tick canonical graph",
        )
        .await?;
        actor.chat(&format!("/clustertest creative-clone-{phase}-release"));
        within(deadline, "authoritative creative cursor outcome", async {
            let mut authoritative = false;
            let mut released = false;
            let mut ticks = actor.get_tick_broadcaster();
            loop {
                tokio::select! {
                    event = events.recv() => match event.context("creative event stream closed")? {
                        Event::Packet(packet) => match packet.as_ref() {
                            ClientboundGamePacket::SetCursorItem(update) => {
                                authoritative |= update.contents.kind() == expected_kind
                                    && update.contents.count() == expected_count;
                            },
                            ClientboundGamePacket::ContainerSetContent(update) if update.container_id == 0 => {
                                authoritative |= update.carried_item.kind() == expected_kind
                                    && update.carried_item.count() == expected_count;
                            },
                            _ => {},
                        },
                        Event::Chat(chat) if chat.message().to_string() == format!("cluster test creative-clone-{phase} released") => released = true,
                        Event::Disconnect(reason) => bail!("creative clone disconnected: {reason:?}"),
                        _ => {},
                    },
                    tick = ticks.recv() => {
                        tick.context("creative tick stream closed")?;
                        let ecs = actor.ecs.read();
                        let state = ecs.get::<azalea::entity::inventory::Inventory>(actor.entity)
                            .context("creative inventory entity absent")?;
                        if released && authoritative && state.carried.kind() == expected_kind
                            && state.carried.count() == expected_count {
                            break;
                        }
                    },
                }
            }
            Ok(())
        }).await?;
        inventory.close();
    }
    eprintln!(
        "passed creative-clone-source-cursor: current source cloned and occupied cursor rejected with authoritative correction"
    );
    Ok(())
}

async fn run_initial_avatar_inventory(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    actor.chat("/clustertest initial-avatar-inventory");
    wait_chat_exact(
        events,
        "cluster test initial-avatar-inventory accepted",
        deadline,
        "canonical loadout preparation before persisted departure",
    )
    .await?;
    let uuid = offline_uuid(BOT_A);
    actor.chat("/lobby");
    super::wait_reentry_feedback(
        events,
        "You are now waiting in the lobby.",
        deadline,
        "initial inventory avatar retirement",
    )
    .await?;
    super::wait_observer_presence(
        observer,
        observer_events,
        uuid,
        false,
        deadline,
        "old avatar removed from already-held chunk",
    )
    .await?;
    while events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat("/exitlobby");
    within(deadline, "initial avatar inventory and remote gear before first input", async {
        let mut initial_content = false;
        let mut main = false;
        let mut boots = false;
        let mut offhand = false;
        let mut spawned = None;
        loop {
            tokio::select! {
                event = events.recv() => match event.context("initial inventory actor stream closed")? {
                    Event::Packet(packet) => if let ClientboundGamePacket::ContainerSetContent(content) = packet.as_ref() {
                        if content.container_id == 0 && content.items.len() > 45 {
                            initial_content |= content.items[36].kind() == ItemKind::Stone
                                && content.items[36].count() == 2
                                && content.items[37].kind() == ItemKind::OakPlanks
                                && content.items[37].count() == 3
                                && content.items[8].kind() == ItemKind::IronBoots
                                && content.items[45].kind() == ItemKind::Shield;
                        }
                    },
                    Event::Disconnect(reason) => bail!("initial inventory handoff disconnected: {reason:?}"),
                    _ => {},
                },
                event = observer_events.recv() => match event.context("initial gear observer stream closed")? {
                    Event::Packet(packet) => match packet.as_ref() {
                        ClientboundGamePacket::AddEntity(entity) if entity.uuid == uuid => spawned = Some(entity.id),
                        ClientboundGamePacket::SetEquipment(gear) if spawned == Some(gear.entity_id) => {
                            for (slot, stack) in &gear.slots.slots {
                                main |= *slot == EquipmentSlot::Mainhand && stack.kind() == ItemKind::Stone && stack.count() == 2;
                                boots |= *slot == EquipmentSlot::Feet && stack.kind() == ItemKind::IronBoots;
                                offhand |= *slot == EquipmentSlot::Offhand && stack.kind() == ItemKind::Shield;
                            }
                        },
                        _ => {},
                    },
                    Event::Disconnect(reason) => bail!("initial gear observer disconnected: {reason:?}"),
                    _ => {},
                },
            }
            if initial_content && spawned.is_some() && main && boots && offhand { break; }
        }
        Ok(())
    }).await?;
    actor.set_selected_hotbar_slot(1);
    within(
        deadline,
        "first post-handoff selection reaches remote avatar",
        async {
            loop {
                match observer_events
                    .recv()
                    .await
                    .context("initial inventory selection stream closed")?
                {
                    Event::Packet(packet) => {
                        if let ClientboundGamePacket::SetEquipment(gear) = packet.as_ref() {
                            if observer.entity_id_by_uuid(uuid).is_some_and(|entity| {
                                observer
                                    .ecs
                                    .read()
                                    .get::<azalea::core::entity_id::MinecraftEntityId>(entity)
                                    .is_some_and(|id| *id == gear.entity_id)
                            }) && gear.slots.slots.iter().any(|(slot, stack)| {
                                *slot == EquipmentSlot::Mainhand
                                    && stack.kind() == ItemKind::OakPlanks
                                    && stack.count() == 3
                            }) {
                                break;
                            }
                        }
                    }
                    Event::Disconnect(reason) => {
                        bail!("first inventory selection disconnected: {reason:?}")
                    }
                    _ => {}
                }
            }
            Ok(())
        },
    )
    .await?;
    eprintln!(
        "passed initial-avatar-inventory: lifecycle gear present on preheld chunk and first normal selection replicated"
    );
    Ok(())
}

pub(super) async fn verify_handoff_callback_early_confirm(
    node: &super::Node,
    actor: &Client,
    operator: &Client,
    actor_events: &mut UnboundedReceiver<Event>,
    operator_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    if !env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "handoff-callback-early-confirm")
    {
        return Ok(());
    }
    actor.chat(&format!("/op {}", super::BOT_B));
    wait_chat_exact(
        actor_events,
        &format!(
            "Queued /op for {}; pending primary durability.",
            super::BOT_B
        ),
        deadline,
        "early confirmation operator grant",
    )
    .await?;
    wait_chat_exact(
        actor_events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "early confirmation durable operator",
    )
    .await?;
    super::wait_permission_command_graph(operator_events, true, deadline).await?;
    let endpoint = node
        .cancel_java
        .as_deref()
        .context("early confirmation forwarding endpoint missing")?;
    let (candidate, mut events) = super::connect(super::BOT_E, endpoint, deadline).await?;
    let login = super::wait_login(&mut events, deadline, "early confirmation login", None).await?;
    let lobby = super::wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "early confirmation lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    within(deadline, "handoff callback and early teleport confirmation", async {
        let mut token = None;
        let mut started = false;
        let mut released = false;
        let mut confirmed = false;
        let mut teleport_id = None;
        let mut order = Vec::new();
        while let Some(event) = events.recv().await {
            match event {
                Event::Disconnect(reason) => bail!("early confirmation candidate disconnected: {reason:?}"),
                Event::Chat(chat) => {
                    let text = chat.message().to_string();
                    if let Some(value) = text.strip_prefix("cluster test handoff callback cue ") {
                        if token.is_some() { bail!("duplicate early confirmation gate cue"); }
                        let value = value.parse::<u64>().context("invalid early confirmation session token")?;
                        token = Some(value);
                        candidate.write_packet(azalea_protocol::packets::game::s_custom_payload::ServerboundCustomPayload {
                            identifier: "pumpkin:cluster_e2e_handoff".parse().map_err(anyhow::Error::msg)?,
                            data: value.to_be_bytes().to_vec().into(),
                        });
                    } else if let Some(value) = text.strip_prefix("cluster test handoff callback started ") {
                        if Some(value.parse::<u64>()?) != token { bail!("handoff callback started for another session"); }
                        started = true;
                    } else if let Some(value) = text.strip_prefix("cluster test handoff actual confirmation ") {
                        let fields = value.split_whitespace().collect::<Vec<_>>();
                        if fields.len() != 2 || Some(fields[0].parse::<u64>()?) != token
                            || Some(fields[1].parse::<u32>()?) != teleport_id || !started || confirmed
                        { bail!("handoff actual confirmation did not match active callback and real teleport"); }
                        confirmed = true;
                        operator.chat("/clustertest handoff-callback-release");
                    } else if let Some(value) = text.strip_prefix("cluster test handoff callback released ") {
                        if Some(value.parse::<u64>()?) != token || !confirmed { bail!("handoff callback released before exact real confirmation"); }
                        released = true;
                    }
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::Login(_) | ClientboundGamePacket::Respawn(_)
                        | ClientboundGamePacket::StartConfiguration(_) => bail!("early confirmation handoff sent terrain transition: {packet:?}"),
                    ClientboundGamePacket::GameEvent(packet) if packet.event == super::EventType::WaitForLevelChunks => bail!("early confirmation handoff reopened terrain loading"),
                    ClientboundGamePacket::PlayerPosition(packet) if packet.change.pos.x.abs() < 100_000.0 && packet.change.pos.z.abs() < 100_000.0 => {
                        if !started || teleport_id.replace(packet.id).is_some() { bail!("real teleport arrived outside single active callback"); }
                        order.push("PlayerPosition");
                    }
                    ClientboundGamePacket::ContainerSetContent(_) if teleport_id.is_some() => order.push("ContainerSetContent"),
                    ClientboundGamePacket::SetHealth(_) if teleport_id.is_some() => order.push("SetHealth"),
                    ClientboundGamePacket::PlayerAbilities(_) if teleport_id.is_some() => order.push("PlayerAbilities"),
                    ClientboundGamePacket::GameEvent(packet) if teleport_id.is_some() && packet.event == super::EventType::ChangeGameMode => order.push("ChangeGameMode"),
                    ClientboundGamePacket::SetCamera(_) if teleport_id.is_some() => order.push("SetCamera"),
                    ClientboundGamePacket::RemoveEntities(_) if teleport_id.is_some() => order.push("RemoveEntities"),
                    _ => {},
                },
                _ => {},
            }
            let required = ["PlayerPosition", "ContainerSetContent", "SetHealth", "PlayerAbilities", "ChangeGameMode", "SetCamera", "RemoveEntities"];
            if released && required.iter().all(|name| order.contains(name)) {
                let indices = required.map(|name| order.iter().position(|packet| *packet == name).unwrap());
                if !indices.windows(2).all(|pair| pair[0] < pair[1]) { bail!("early confirmation handoff packet order changed: {order:?}"); }
                return Ok(());
            }
        }
        bail!("early confirmation stream closed before callback release and complete handoff")
    }).await?;
    wait_chat_exact(
        operator_events,
        "cluster test handoff-callback-release submitted",
        deadline,
        "independent callback release submission",
    )
    .await?;
    wait_chat_exact(
        operator_events,
        "cluster test handoff-callback-release accepted",
        deadline,
        "independent callback release accepted",
    )
    .await?;
    wait_until(
        actor,
        deadline,
        "early confirmation remote avatar birth",
        || Ok(actor.entity_by_uuid(offline_uuid(super::BOT_E)).map(|_| ())),
    )
    .await?;
    let before = candidate.position()?;
    candidate.set_direction(0.0, 0.0)?;
    candidate.walk(WalkDirection::Forward);
    let movement = wait_until(
        actor,
        deadline,
        "movement after callback handoff without another teleport",
        || {
            let Some(entity) = actor.entity_by_uuid(offline_uuid(super::BOT_E)) else {
                return Ok(None);
            };
            let remote = entity.position()?;
            let local = candidate.position()?;
            let moved = (remote.x - before.x).abs() > 0.05 || (remote.z - before.z).abs() > 0.05;
            Ok(
                (moved && (remote.x - local.x).abs() < 0.5 && (remote.z - local.z).abs() < 0.5)
                    .then_some(()),
            )
        },
    )
    .await;
    candidate.walk(WalkDirection::None);
    movement?;
    candidate.disconnect();
    actor.chat(&format!("/deop {}", super::BOT_B));
    wait_chat_exact(
        actor_events,
        &format!(
            "Queued /deop for {}; pending primary durability.",
            super::BOT_B
        ),
        deadline,
        "early confirmation operator cleanup",
    )
    .await?;
    wait_chat_exact(
        actor_events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "early confirmation durable cleanup",
    )
    .await?;
    super::wait_observer_presence(
        actor,
        actor_events,
        offline_uuid(super::BOT_E),
        false,
        deadline,
        "early confirmation candidate departed",
    )
    .await?;
    eprintln!(
        "passed handoff-callback-early-confirm: exact real ACK during virtual callback and remote movement before another teleport"
    );
    Ok(())
}

fn validate_retirement_reconnect_barrier(log: &str, work_record: &str) -> Result<()> {
    let lines = log.lines().map(strip_ansi).collect::<Vec<_>>();
    let settled = lines
        .iter()
        .position(|line| line == work_record)
        .context("delayed work record index missing")?;
    let uuid = offline_uuid(BOT_A).to_string();
    let mut pairs = Vec::new();
    for (index, entered) in lines.iter().enumerate().take(settled) {
        if !entered.contains("cluster e2e retirement barrier entered")
            || structured_value(entered, "uuid") != Some(uuid.as_str())
        {
            continue;
        }
        let session = structured_value(entered, "new_session")
            .context("retirement barrier new session absent")?;
        for (released_index, released) in lines.iter().enumerate().skip(settled + 1) {
            if released.contains("cluster e2e retirement barrier released")
                && structured_value(released, "uuid") == Some(uuid.as_str())
                && structured_value(released, "new_session") == Some(session)
            {
                pairs.push((index, released_index, entered, released));
            }
        }
    }
    let [(_, _, entered, released)] = pairs.as_slice() else {
        bail!(
            "expected one exact reconnect barrier spanning delayed work, observed {}",
            pairs.len()
        );
    };
    let session = structured_value(entered, "new_session")
        .context("barrier new session missing")?
        .parse::<u64>()?;
    let old =
        structured_value(entered, "old_install").context("barrier old install field absent")?;
    let commit = structured_value(entered, "pending_commit")
        .context("barrier pending commit field absent")?;
    if session == 0 || old == "None" && commit == "None" {
        bail!("barrier did not identify actual old retirement/commit: {entered}");
    }
    if old
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<u64>().ok())
        == Some(session)
    {
        bail!("retirement barrier reused old install session identity");
    }
    let revision = structured_value(released, "actual_revision")
        .context("released durable revision absent")?
        .parse::<u64>()?;
    let expected = structured_value(entered, "expected_revision")
        .and_then(|value| value.strip_prefix("Some("))
        .and_then(|value| value.strip_suffix(')'))
        .and_then(|value| value.parse::<u64>().ok());
    if revision == 0
        || expected.is_some_and(|expected| revision <= expected)
        || structured_value(released, "old_install") != Some(old)
        || structured_value(released, "pending_commit") != Some(commit)
        || structured_value(released, "world").is_none_or(str::is_empty)
        || (super::structured_f64(released, "x")? - 0.5).abs() > 1e-9
        || (super::structured_f64(released, "y")? - 80.0).abs() > 1e-9
        || (super::structured_f64(released, "z")? - 4.5).abs() > 1e-9
    {
        bail!("reconnect barrier did not release exact fresh durable departure target: {released}");
    }
    Ok(())
}

async fn drive_placement_sources(
    actor: &Client,
    observer: &Client,
    third: Option<&Client>,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    if !matches!(
        id,
        "conflict-two-place" | "conflict-three-place" | "targeted-undo"
    ) {
        return Ok(());
    }
    wait_chat_exact(events, &format!("cluster test {id} prepared"), deadline, id).await?;
    let mut clients = vec![
        (actor, u8::from(id == "targeted-undo")),
        (observer, u8::from(id != "targeted-undo")),
    ];
    if id == "conflict-three-place" {
        clients.push((
            third.context("three-way conflict requires the third real client")?,
            0,
        ));
    }
    let support = BlockPos::new(0, 79, 4);
    let hit = Vec3::new(0.5, 80.0, 4.5);
    for (client, slot) in &clients {
        client.set_selected_hotbar_slot(*slot);
        client.look_at(hit);
    }
    wait_chat_exact(
        events,
        &format!("cluster test {id} source armed"),
        deadline,
        id,
    )
    .await?;
    for (index, (client, _)) in clients.into_iter().enumerate() {
        client.write_packet(ServerboundUseItemOn {
            hand: InteractionHand::MainHand,
            block_hit: BlockHit {
                block_pos: support,
                direction: Direction::Up,
                location: hit,
                inside: false,
                world_border: false,
            },
            seq: 0x790 + index as u32,
        });
    }
    Ok(())
}
async fn drive_replay_sources(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<Option<azalea::container::ContainerHandle>> {
    if !matches!(id, "late-dependent-replay" | "counterfactual-ground-block") {
        return Ok(None);
    }
    wait_chat_exact(events, &format!("cluster test {id} prepared"), deadline, id).await?;
    actor.chat("/op PumpkinE2eB");
    wait_chat_contains(
        events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "replay editor permission durable",
    )
    .await?;
    super::wait_permission_command_graph(observer_events, true, deadline).await?;
    let chest = if id == "counterfactual-ground-block" {
        let handle = within(deadline, "normal physical replay chest open", async {
            actor
                .open_container_at(BlockPos::new(17, 80, 5))
                .await?
                .context("replay physical chest did not open")
        })
        .await?;
        if handle
            .menu()?
            .context("physical replay chest menu absent")?
            .slots()
            .len()
            != 63
        {
            bail!("physical replay source is not a single chest with player inventory");
        }
        handle.left_click(54usize);
        handle.left_click(0usize);
        Some(handle)
    } else {
        None
    };
    observer.chat("/invsee edit PumpkinE2eA");
    within(deadline, "normal authorized replay editor open", async {
        let mut opened = None;
        while let Some(event) = observer_events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(screen)
                        if screen.menu_type == MenuKind::Generic9x5 =>
                    {
                        opened = Some(screen.container_id)
                    }
                    ClientboundGamePacket::ContainerSetContent(content)
                        if opened == Some(content.container_id)
                            && content.items.len() == 81
                            && content.items.get(73).is_some_and(|stack| {
                                stack.kind() == ItemKind::OakPlanks && stack.count() == 1
                            }) =>
                    {
                        return Ok(());
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!("replay editor disconnected: {reason:?}"),
                _ => {}
            }
        }
        bail!("replay editor did not receive complete authoritative inventory");
    })
    .await?;
    observer.get_inventory()?.left_click(73usize);
    wait_chat_exact(
        events,
        &format!("cluster test {id} source armed"),
        deadline,
        id,
    )
    .await?;
    if id == "late-dependent-replay" {
        observer.get_inventory()?.left_click(9usize);
        actor.get_inventory()?.shift_click(36usize);
    } else {
        actor.get_inventory()?.shift_click(0usize);
        observer.get_inventory()?.left_click(10usize);
    }
    Ok(chest)
}

async fn verify_replay_client_conservation(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    if !matches!(id, "late-dependent-replay" | "counterfactual-ground-block") {
        return Ok(());
    }
    wait_until(
        observer,
        deadline,
        "replayed inventory final client state",
        || {
            let a = actor
                .get_inventory()?
                .menu()?
                .context("replay A menu absent")?;
            let b = observer
                .get_inventory()?
                .menu()?
                .context("replay B menu absent")?;
            let a_slots = a.slots();
            let b_slots = b.slots();
            let own_offset = if id == "late-dependent-replay" { 0 } else { 18 };
            let (stone_slot, oak_slot) = if id == "late-dependent-replay" {
                (10, 9)
            } else {
                (9, 10)
            };
            let exact =
                |slots: &[azalea_inventory::ItemStack], slot: usize, kind: ItemKind, count: i32| {
                    slots
                        .get(slot)
                        .is_some_and(|stack| stack.kind() == kind && stack.count() == count)
                };
            let a_ecs = actor.ecs.read();
            let b_ecs = observer.ecs.read();
            let a_cursor = &a_ecs
                .get::<azalea::entity::inventory::Inventory>(actor.entity)
                .context("replay A cursor absent")?
                .carried;
            let b_cursor = &b_ecs
                .get::<azalea::entity::inventory::Inventory>(observer.entity)
                .context("replay B cursor absent")?
                .carried;
            Ok(
                (exact(&a_slots, stone_slot + own_offset, ItemKind::Stone, 2)
                    && exact(&a_slots, oak_slot + own_offset, ItemKind::OakPlanks, 1)
                    && a_slots
                        .get(36 + own_offset)
                        .is_some_and(|stack| stack.is_empty())
                    && exact(&a_slots, 37 + own_offset, ItemKind::OakPlanks, 1)
                    && exact(&b_slots, stone_slot, ItemKind::Stone, 2)
                    && exact(&b_slots, oak_slot, ItemKind::OakPlanks, 1)
                    && b_slots.get(36).is_some_and(|stack| stack.is_empty())
                    && exact(&b_slots, 37, ItemKind::OakPlanks, 1)
                    && exact(&b_slots, 72, ItemKind::Stone, 2)
                    && b_slots.get(73).is_some_and(|stack| stack.is_empty())
                    && (id != "counterfactual-ground-block"
                        || a_slots.first().is_some_and(|stack| stack.is_empty()))
                    && a_cursor.is_empty()
                    && b_cursor.is_empty())
                .then_some(()),
            )
        },
    )
    .await?;
    actor.get_inventory()?.close();
    observer.get_inventory()?.close();
    actor.chat("/deop PumpkinE2eB");
    wait_chat_contains(
        events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "replay editor permission restored",
    )
    .await?;
    super::wait_permission_command_graph(observer_events, false, deadline).await?;
    Ok(())
}

async fn wait_replay_acceptance(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    let a_window = actor.get_inventory()?.id();
    let b_window = observer.get_inventory()?.id();
    let offset = if id == "late-dependent-replay" { 0 } else { 18 };
    let (stone, oak) = if id == "late-dependent-replay" {
        (10usize, 9usize)
    } else {
        (9, 10)
    };
    within(deadline, "authoritative replay inventory deltas and Ground completion", async {
        let mut accepted = false;
        let mut provisional = id != "late-dependent-replay";
        let mut a_seen = [false; 3];
        let mut b_seen = [false; 3];
        let mut cursor = false;
        let capture = |packet: &ClientboundGamePacket, window: i32, offset: usize, seen: &mut [bool; 3]| {
            let mut slot = |index: usize, item: &azalea_inventory::ItemStack| {
                if index == stone + offset && item.kind() == ItemKind::Stone && item.count() == 2 { seen[0] = true; }
                if index == oak + offset && item.kind() == ItemKind::OakPlanks && item.count() == 1 { seen[1] = true; }
                if index == 36 + offset && item.is_empty() { seen[2] = true; }
            };
            match packet {
                ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == window => {
                    slot(usize::from(update.slot), &update.item_stack);
                }
                ClientboundGamePacket::ContainerSetContent(update) if update.container_id == window => {
                    for (index, item) in update.items.iter().enumerate() { slot(index, item); }
                }
                _ => {}
            }
        };
        while !(accepted && provisional && a_seen.into_iter().all(|seen| seen) && b_seen.into_iter().all(|seen| seen) && cursor) {
            tokio::select! {
                event = events.recv() => match event.context("replay actor stream ended")? {
                    Event::Chat(chat) => accepted |= chat.message().to_string() == format!("cluster test {id} accepted"),
                    Event::Packet(packet) => {
                        if id == "late-dependent-replay" {
                            match packet.as_ref() {
                                ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == a_window && update.slot == 9 && update.item_stack.kind() == ItemKind::Stone && update.item_stack.count() == 2 => provisional = true,
                                ClientboundGamePacket::ContainerSetContent(update) if update.container_id == a_window && update.items.get(9).is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 2) => provisional = true,
                                ClientboundGamePacket::LevelChunkWithLight(update) if provisional && update.x == 0 && update.z == 0 => bail!("inventory replay repaired the already-held conflict chunk with a full resend"),
                                _ => {}
                            }
                        }
                        capture(packet.as_ref(), a_window, offset, &mut a_seen);
                    },
                    Event::Disconnect(reason) => bail!("replay actor disconnected: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("replay editor stream ended")? {
                    Event::Packet(packet) => {
                        capture(packet.as_ref(), b_window, 0, &mut b_seen);
                        cursor |= match packet.as_ref() {
                            ClientboundGamePacket::SetCursorItem(update) => update.contents.is_empty(),
                            ClientboundGamePacket::ContainerSetContent(update) if update.container_id == b_window => update.carried_item.is_empty(),
                            _ => false
                        };
                    },
                    Event::Disconnect(reason) => bail!("replay editor disconnected: {reason:?}"),
                    _ => {}
                }
            }
        }
        Ok(())
    }).await
}
async fn drive_stale_break_sources(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let id = "stale-dirt-break";
    wait_chat_exact(events, &format!("cluster test {id} prepared"), deadline, id).await?;
    for (index, client) in [actor, observer].into_iter().enumerate() {
        client.set_selected_hotbar_slot(3);
        client.look_at(Vec3::new(0.5, 80.5, 4.5));
        client.write_packet(ServerboundPlayerAction {
            action: PlayerAction::StartDestroyBlock,
            pos: CONFLICT_POSITION,
            direction: Direction::North,
            seq: 0x7a0 + index as u32,
        });
    }
    wait_ticks(
        actor,
        20,
        deadline,
        "normal dirt mining progress before source gate",
    )
    .await?;
    wait_chat_exact(
        events,
        &format!("cluster test {id} source armed"),
        deadline,
        id,
    )
    .await?;
    actor.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StopDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7a2,
    });
    actor.write_packet(ServerboundSetCarriedItem { slot: 1 });
    actor.write_packet(ServerboundUseItemOn {
        hand: InteractionHand::MainHand,
        block_hit: BlockHit {
            block_pos: BlockPos::new(0, 79, 4),
            direction: Direction::Up,
            location: Vec3::new(0.5, 80.0, 4.5),
            inside: false,
            world_border: false,
        },
        seq: 0x7a3,
    });
    observer.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StopDestroyBlock,
        pos: CONFLICT_POSITION,
        direction: Direction::North,
        seq: 0x7a4,
    });
    Ok(())
}

async fn verify_placement_client_conservation(
    actor: &Client,
    observer: &Client,
    third: Option<&Client>,
    deadline: &Deadline,
    id: &str,
) -> Result<()> {
    if !matches!(
        id,
        "conflict-two-place" | "conflict-three-place" | "targeted-undo"
    ) {
        return Ok(());
    }
    let mut clients = vec![actor, observer];
    if id == "conflict-three-place" {
        clients.push(third.context("three-way inventory conservation requires real C")?);
    }
    wait_until(
        actor,
        deadline,
        "placement inventory consume and losing undo converge",
        || {
            let block = actor
                .world()?
                .read()
                .get_block_state(CONFLICT_POSITION)
                .context("placement result block absent")?;
            let winner_kind = match BlockKind::from(block) {
                BlockKind::Stone => ItemKind::Stone,
                BlockKind::OakPlanks => ItemKind::OakPlanks,
                _ => return Ok(None),
            };
            let mut stone_total = 0;
            let mut oak_total = 0;
            for (index, client) in clients.iter().enumerate() {
                let inventory = client.get_inventory()?;
                if inventory.id() != 0 {
                    bail!("placement conservation encountered an unexpected open menu");
                }
                let slots = inventory
                    .menu()?
                    .context("placement own inventory absent")?
                    .slots();
                let expected_choice = if id == "targeted-undo" {
                    if index == 0 {
                        ItemKind::OakPlanks
                    } else {
                        ItemKind::Stone
                    }
                } else if index == 1 {
                    ItemKind::OakPlanks
                } else {
                    ItemKind::Stone
                };
                for (slot, stack) in slots.iter().enumerate() {
                    let (kind, original) = match slot {
                        36 => (ItemKind::Stone, 2),
                        37 => (ItemKind::OakPlanks, 2),
                        38 => (ItemKind::Dirt, 2),
                        39 => (ItemKind::IronShovel, 1),
                        _ => {
                            if !stack.is_empty() {
                                return Ok(None);
                            }
                            continue;
                        }
                    };
                    if stack.kind() != kind {
                        return Ok(None);
                    }
                    let may_consume = kind == expected_choice && kind == winner_kind;
                    if stack.count() != original && !(may_consume && stack.count() == original - 1)
                    {
                        return Ok(None);
                    }
                    if kind == ItemKind::Stone {
                        stone_total += stack.count();
                    }
                    if kind == ItemKind::OakPlanks {
                        oak_total += stack.count();
                    }
                }
                let ecs = client.ecs.read();
                if !ecs
                    .get::<azalea::entity::inventory::Inventory>(client.entity)
                    .context("placement cursor absent")?
                    .carried
                    .is_empty()
                {
                    return Ok(None);
                }
            }
            let original = i32::try_from(clients.len())? * 2;
            Ok(
                (stone_total == original - i32::from(winner_kind == ItemKind::Stone)
                    && oak_total == original - i32::from(winner_kind == ItemKind::OakPlanks))
                .then_some(()),
            )
        },
    )
    .await
}

async fn health_policy_observer_permission(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    enabled: bool,
) -> Result<()> {
    actor.chat(&format!(
        "/{} {}",
        if enabled { "op" } else { "deop" },
        super::BOT_B
    ));
    wait_chat_contains(
        events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "health observer permission durable",
    )
    .await?;
    super::wait_permission_command_graph(observer_events, enabled, deadline).await
}

async fn set_fixture_health_policy(
    actor: &Client,
    observer: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    mode: &str,
) -> Result<()> {
    for client in [actor, observer] {
        client.chat(&format!("/clustertest health-policy-{mode}-arm"));
    }
    let ((a_prior, _), (b_prior, _)) = tokio::try_join!(
        wait_health_policy_cue(events, deadline, mode, false),
        wait_health_policy_cue(observer_events, deadline, mode, false),
    )?;
    actor.chat(&format!("/gamerule clusterHealthMask {mode}"));
    let (a, b) = tokio::try_join!(
        wait_health_policy_cue(events, deadline, mode, true),
        wait_health_policy_cue(observer_events, deadline, mode, true),
    )?;
    if a != b || a.0 <= a_prior.max(b_prior) || a.1.is_empty() {
        bail!(
            "health policy {mode} did not converge to the same new actual version/source on both holders: A={a:?}, B={b:?}, prior={a_prior}/{b_prior}"
        );
    }
    Ok(())
}

async fn wait_health_policy_cue(
    events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    mode: &str,
    ready: bool,
) -> Result<(u64, String)> {
    let phase = if ready { "ready" } else { "armed" };
    let prefix = format!("cluster test health-policy-{mode} {phase} ");
    within(deadline, &format!("actual health policy {mode} {phase}"), async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) => {
                    let text = chat.message().to_string();
                    if !text.starts_with(&prefix) { continue; }
                    let field = if ready { "version" } else { "prior_version" };
                    let version = structured_value(&text, field).context("health policy cue version absent")?.parse::<u64>()?;
                    let source = if ready {
                        structured_value(&text, "source").filter(|source| !source.is_empty() && source.len() <= 128).context("health policy source stamp absent/oversized")?.to_owned()
                    } else { String::new() };
                    if ready {
                        let (server, boot) = source.split_once(':').context("health policy source stamp malformed")?;
                        if server.parse::<u16>()? != 0 || boot.parse::<u64>()? == 0 { bail!("health policy source is not the actual admitted primary: {source}"); }
                    }
                    return Ok((version, source));
                }
                Event::Disconnect(reason) => bail!("health policy observer disconnected: {reason:?}"),
                _ => {}
            }
        }
        bail!("health policy observer stream ended before actual publication cue");
    }).await
}

async fn observe_mount_health_window(
    actor: &Client,
    events: &mut UnboundedReceiver<Event>,
    observer_events: &mut UnboundedReceiver<Event>,
    deadline: &Deadline,
    actor_horse: azalea::core::entity_id::MinecraftEntityId,
    observer_horse: azalea::core::entity_id::MinecraftEntityId,
    local_rider: azalea::core::entity_id::MinecraftEntityId,
    remote_rider: azalea::core::entity_id::MinecraftEntityId,
) -> Result<()> {
    within(deadline, "mount health remains accurate/masked for ten actual client ticks", async {
        let mut ticks = actor.get_tick_broadcaster();
        let mut observed_ticks = 0;
        let inspect = |packet: &ClientboundGamePacket, horse, rider, health: f32| -> Result<()> {
            match packet {
                ClientboundGamePacket::SetEntityData(update) if update.id == horse => {
                    for item in &update.packed_items.0 {
                        if item.index == 9 && let EntityDataValue::Float(value) = &item.value && *value != health {
                            bail!("mount numeric health changed from required {health} to {value}");
                        }
                    }
                }
                ClientboundGamePacket::SetPassengers(update) if update.vehicle == horse && update.passengers != vec![rider] => bail!("prepared mount lost its exact rider during numeric exception validation"),
                _ => {}
            }
            Ok(())
        };
        while observed_ticks < 10 {
            tokio::select! {
                tick = ticks.recv() => { tick.context("mount health tick stream ended")?; observed_ticks += 1; }
                event = events.recv() => match event.context("mount rider event stream ended")? {
                    Event::Packet(packet) => {
                        inspect(packet.as_ref(), actor_horse, local_rider, 4.0)?;
                        if let ClientboundGamePacket::SetHealth(update) = packet.as_ref() && update.health != 7.0 { bail!("mount rider self HUD lost actual seven health"); }
                    },
                    Event::Disconnect(reason) => bail!("mount rider disconnected: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("mount observer event stream ended")? {
                    Event::Packet(packet) => inspect(packet.as_ref(), observer_horse, remote_rider, 20.0)?,
                    Event::Disconnect(reason) => bail!("mount observer disconnected: {reason:?}"),
                    _ => {}
                }
            }
        }
        Ok(())
    }).await
}

fn validate_health_policy_records(logs: &[(u16, String)]) -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    let selected = selected.split(',').collect::<HashSet<_>>();
    let mut modes = Vec::new();
    if selected.contains("health-mask") {
        modes.extend(["off", "players", "all"]);
    }
    if selected.contains("health-mask-mount") {
        modes.push("all");
    }
    if selected.contains("health-visual-exemption") {
        modes.push("all");
    }
    if modes.is_empty() {
        return Ok(());
    }
    let primary = &logs
        .iter()
        .find(|(node, _)| *node == 0)
        .context("health policy primary log absent")?
        .1;
    let applied = primary
        .lines()
        .map(strip_ansi)
        .filter(|line| line.contains("cluster e2e health policy applied"))
        .collect::<Vec<_>>();
    let mut identities = Vec::new();
    for (node, participant) in [(1, super::BOT_A), (2, super::BOT_B)] {
        let log = &logs
            .iter()
            .find(|(peer, _)| *peer == node)
            .context("health policy participant log absent")?
            .1;
        let records = log
            .lines()
            .map(strip_ansi)
            .filter(|line| line.contains("cluster e2e health policy ready"))
            .collect::<Vec<_>>();
        if records.len() != modes.len() {
            bail!(
                "node {node} emitted {} health policy readiness records, expected {}",
                records.len(),
                modes.len()
            );
        }
        let mut current = Vec::new();
        for (record, mode) in records.iter().zip(&modes) {
            let expected_uuid = offline_uuid(participant).to_string();
            if structured_value(record, "participant") != Some(expected_uuid.as_str())
                || structured_value(record, "mode") != Some(*mode)
            {
                bail!("health policy readiness participant/mode mismatch on node {node}: {record}");
            }
            let session = structured_value(record, "session")
                .context("health policy session absent")?
                .parse::<u64>()?;
            let prior = structured_value(record, "prior_version")
                .context("health policy prior version absent")?
                .parse::<u64>()?;
            let version_text =
                structured_value(record, "version").context("health policy version absent")?;
            let version = version_text.parse::<u64>()?;
            let source =
                structured_value(record, "source").context("health policy source absent")?;
            let (server, boot) = source
                .split_once(':')
                .context("health policy source malformed")?;
            if session == 0
                || version <= prior
                || server.parse::<u16>()? != 0
                || boot.parse::<u64>()? == 0
            {
                bail!(
                    "health policy readiness lacks a new authenticated publication on node {node}: {record}"
                );
            }
            if applied
                .iter()
                .filter(|publication| {
                    structured_value(publication, "mode") == Some(*mode)
                        && structured_value(publication, "version") == Some(version_text)
                        && structured_value(publication, "source") == Some(source)
                })
                .count()
                != 1
            {
                bail!(
                    "health policy readiness has no unique actual primary publication on node {node}: {record}"
                );
            }
            current.push(((*mode).to_owned(), version, source.to_owned()));
        }
        identities.push(current);
    }
    if identities[0] != identities[1] {
        bail!("health policy participants observed different actual publications");
    }
    Ok(())
}
