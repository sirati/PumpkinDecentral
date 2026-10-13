use block::BlockBehaviour;

mod block {
    pub type BlockStateId = u16;
    pub struct PlacementState {
        pub id: BlockStateId,
    }
    pub struct PlacementBlock {
        pub default_state: PlacementState,
    }
    pub struct PlacementIntentArgs<'a> {
        pub block: &'a PlacementBlock,
    }
    pub mod registry {
        pub enum BlockActionResult {
            Pass,
        }
    }
    pub struct RandomTickIntentArgs<'a> {
        pub marker: std::marker::PhantomData<&'a ()>,
    }
    pub struct NormalUseIntentArgs<'a> {
        pub marker: std::marker::PhantomData<&'a ()>,
    }
    pub mod neighbor {
        #[derive(Clone, Copy)]
        pub enum BlockCallbackEvent {
            Scheduled,
            StateReplaced {},
            Placed {},
            Prepare {},
            Neighbor {},
        }
        pub struct BlockCallbackIntentArgs<'a> {
            pub world: &'a (),
            pub block: &'a (),
            pub position: &'a (),
            pub event: BlockCallbackEvent,
        }
        pub type BlockCallbackEffect = u8;
    }
    pub struct ScheduledTickIntentArgs<'a> {
        pub world: &'a (),
        pub block: &'a (),
        pub position: &'a (),
    }
    pub trait BlockBehaviour {
        fn placement_intent(&self, _: PlacementIntentArgs<'_>) -> Option<BlockStateId> {
            None
        }
        fn on_place(&self) {}
        fn random_tick_intent(&self, _: RandomTickIntentArgs<'_>) -> Option<Vec<u8>> {
            None
        }
        fn normal_use_intent(
            &self,
            _: NormalUseIntentArgs<'_>,
        ) -> Option<(registry::BlockActionResult, Vec<u8>)> {
            None
        }
        fn callback_intent(&self, _: neighbor::BlockCallbackIntentArgs<'_>) -> Option<Vec<u8>> {
            None
        }
        fn scheduled_tick_intent(&self, _: ScheduledTickIntentArgs<'_>) -> Option<Vec<u8>> {
            None
        }
        fn on_neighbor_update(&self) {}
        fn on_neighbour_update(&self) {}
    }
}

fn run(block: &impl BlockBehaviour, event: block::neighbor::BlockCallbackEvent) -> Option<Vec<u8>> {
    block.callback_intent(block::neighbor::BlockCallbackIntentArgs {
        world: &(),
        block: &(),
        position: &(),
        event,
    })
}

struct Defaults;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for Defaults {}

struct LegacyNeighbor;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for LegacyNeighbor {
    fn on_neighbor_update(&self) {}
}

struct PureScheduled;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for PureScheduled {
    fn scheduled_tick_intent(&self, args: block::ScheduledTickIntentArgs<'_>) -> Option<Vec<u8>> {
        let _ = (args.world, args.block, args.position);
        Some(vec![7])
    }
}

struct Explicit;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for Explicit {
    fn callback_intent(&self, _: block::neighbor::BlockCallbackIntentArgs<'_>) -> Option<Vec<u8>> {
        Some(vec![9])
    }
}

struct SimilarMethod;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for SimilarMethod {
    fn on_neighbour_update(&self) {}
}

struct LegacyPlacement;
#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for LegacyPlacement {
    fn on_place(&self) {}
}

#[test]
fn inherited_placement_is_available_but_legacy_override_is_not_bypassed() {
    let block = block::PlacementBlock {
        default_state: block::PlacementState { id: 42 },
    };
    assert_eq!(
        Defaults.placement_intent(block::PlacementIntentArgs { block: &block }),
        Some(42)
    );
    assert_eq!(
        LegacyPlacement.placement_intent(block::PlacementIntentArgs { block: &block }),
        None
    );
}

#[test]
fn legacy_side_effects_cannot_be_classified_as_inherited_noop() {
    use block::neighbor::BlockCallbackEvent;
    assert!(run(&LegacyNeighbor, BlockCallbackEvent::Neighbor {}).is_none());
    assert!(
        run(&LegacyNeighbor, BlockCallbackEvent::Placed {})
            .unwrap()
            .is_empty()
    );
    assert!(
        run(&Defaults, BlockCallbackEvent::StateReplaced {})
            .unwrap()
            .is_empty()
    );
    assert!(
        run(&Defaults, BlockCallbackEvent::Prepare {})
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        run(&PureScheduled, BlockCallbackEvent::Scheduled),
        Some(vec![7])
    );
    assert_eq!(
        run(&Explicit, BlockCallbackEvent::Neighbor {}),
        Some(vec![9])
    );
    assert!(
        run(&SimilarMethod, BlockCallbackEvent::Neighbor {})
            .unwrap()
            .is_empty()
    );
}
