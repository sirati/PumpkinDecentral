use std::{collections::HashMap, sync::Arc};

use pumpkin_cluster::{
    canonical_action::Cell,
    protocol::{BlockPos, ChunkKey},
};
use pumpkin_world::{level::OwnedChunkImage, lighting::owned::OwnedChunkLighting};

use super::{
    Server,
    cluster_chunk_state::{OwnedChunkBlocks, OwnedChunkStore},
};

pub struct ChunkPresentation {
    pub publication: u64,
    pub image: Arc<OwnedChunkImage>,
    pub blocks: Arc<OwnedChunkBlocks>,
    pub block_entities: Arc<Vec<(BlockPos, Cell)>>,
    pub lighting: Option<Arc<OwnedChunkLighting>>,
    pub admission_lighting_valid: bool,
}

pub fn publish(
    server: &Server,
    chunk: ChunkKey,
    store: &OwnedChunkStore,
    image: &OwnedChunkImage,
    lighting: Option<&OwnedChunkLighting>,
) {
    let Some(blocks) = store.snapshot(chunk) else {
        return;
    };
    let Some(projected_image) = image.with_palettes(
        blocks.min_y(),
        blocks.block_palettes(),
        blocks.biome_palettes(),
    ) else {
        return;
    };
    let admission_lighting_valid = image.document().root_tag.get_list("sections")
        == projected_image.document().root_tag.get_list("sections");
    let current = server.cluster_chunk_presentations.load_full();
    let publication = current.get(&chunk).map_or(1, |old| {
        old.publication
            .checked_add(1)
            .expect("chunk presentation publication exhausted")
    });
    let mut next = (*current).clone();
    next.insert(
        chunk,
        Arc::new(ChunkPresentation {
            publication,
            image: Arc::new(projected_image),
            blocks,
            block_entities: Arc::new(store.block_entities_for_chunk(false, chunk)),
            lighting: if admission_lighting_valid {
                lighting.cloned().map(Arc::new)
            } else {
                None
            },
            admission_lighting_valid,
        }),
    );
    server.cluster_chunk_presentations.store(Arc::new(next));
}

pub fn refresh(
    server: &Server,
    store: &OwnedChunkStore,
    chunks: impl IntoIterator<Item = ChunkKey>,
) {
    let current = server.cluster_chunk_presentations.load_full();
    let mut next: HashMap<_, _> = (*current).clone();
    let changed = chunks
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut affected = std::collections::BTreeSet::new();
    for chunk in changed.iter().copied() {
        for dx in -1..=1 {
            for dz in -1..=1 {
                if let (Some(x), Some(z)) = (chunk.x().checked_add(dx), chunk.z().checked_add(dz)) {
                    affected.insert(ChunkKey::new(chunk.world, x, z));
                }
            }
        }
    }
    for chunk in affected {
        let Some(old) = current.get(&chunk) else {
            continue;
        };
        let Some(blocks) = store.snapshot(chunk) else {
            continue;
        };
        let image = if changed.contains(&chunk) {
            let Some(image) = old.image.with_palettes(
                blocks.min_y(),
                blocks.block_palettes(),
                blocks.biome_palettes(),
            ) else {
                next.remove(&chunk);
                continue;
            };
            Arc::new(image)
        } else {
            old.image.clone()
        };
        next.insert(
            chunk,
            Arc::new(ChunkPresentation {
                publication: old
                    .publication
                    .checked_add(1)
                    .expect("chunk presentation publication exhausted"),
                image,
                blocks,
                block_entities: Arc::new(store.block_entities_for_chunk(false, chunk)),
                lighting: None,
                admission_lighting_valid: false,
            }),
        );
    }
    server.cluster_chunk_presentations.store(Arc::new(next));
}

pub fn retire(server: &Server, chunk: ChunkKey) {
    let current = server.cluster_chunk_presentations.load_full();
    if !current.contains_key(&chunk) {
        return;
    }
    let mut next = (*current).clone();
    next.remove(&chunk);
    server.cluster_chunk_presentations.store(Arc::new(next));
}

#[derive(Default)]
pub struct PresentationLighting {
    pending: Option<(
        ChunkKey,
        Arc<ChunkPresentation>,
        [Arc<OwnedChunkBlocks>; 9],
        pumpkin_world::lighting::owned::OwnedLightingWork,
    )>,
}

impl PresentationLighting {
    pub fn advance(
        &mut self,
        server: &Server,
        config: impl Fn(ChunkKey) -> Option<(pumpkin_config::lighting::LightingEngineConfig, bool)>,
    ) {
        let presentations = server.cluster_chunk_presentations.load_full();
        let palettes = server.cluster_owned_chunk_blocks.load_full();
        if self
            .pending
            .as_ref()
            .is_some_and(|(key, presentation, halo, _)| {
                !presentations
                    .get(key)
                    .is_some_and(|current| Arc::ptr_eq(current, presentation))
                    || halo.iter().enumerate().any(|(index, palette)| {
                        let Some(x) = key.x().checked_add(index as i32 % 3 - 1) else {
                            return true;
                        };
                        let Some(z) = key.z().checked_add(index as i32 / 3 - 1) else {
                            return true;
                        };
                        !palettes
                            .get(&ChunkKey::new(key.world, x, z))
                            .is_some_and(|current| Arc::ptr_eq(current, palette))
                    })
            })
        {
            self.pending = None;
        }
        if self.pending.is_none() {
            let mut candidates = presentations
                .iter()
                .filter(|(_, image)| !image.admission_lighting_valid && image.lighting.is_none())
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(key, _)| **key);
            for (key, presentation) in candidates {
                let Some((lighting_config, skylight)) = config(*key) else {
                    continue;
                };
                let mut halo = Vec::with_capacity(9);
                for index in 0..9 {
                    let Some(x) = key.x().checked_add(index % 3 - 1) else {
                        break;
                    };
                    let Some(z) = key.z().checked_add(index / 3 - 1) else {
                        break;
                    };
                    let Some(palette) = palettes.get(&ChunkKey::new(key.world, x, z)) else {
                        break;
                    };
                    halo.push(palette.clone());
                }
                let Ok(halo): Result<[Arc<OwnedChunkBlocks>; 9], _> = halo.try_into() else {
                    continue;
                };
                let Ok(work) = pumpkin_world::lighting::owned::OwnedLightingWork::start(
                    std::array::from_fn(|index| halo[index].block_palettes()),
                    lighting_config,
                    skylight,
                ) else {
                    continue;
                };
                self.pending = Some((*key, presentation.clone(), halo, work));
                break;
            }
        }
        if let Some((_, _, _, work)) = &mut self.pending
            && work.advance(250_000)
        {
            let (key, old, _, work) = self
                .pending
                .take()
                .expect("completed presentation lighting");
            let mut next = (*presentations).clone();
            next.insert(
                key,
                Arc::new(ChunkPresentation {
                    publication: old
                        .publication
                        .checked_add(1)
                        .expect("chunk presentation publication exhausted"),
                    image: old.image.clone(),
                    blocks: old.blocks.clone(),
                    block_entities: old.block_entities.clone(),
                    lighting: Some(Arc::new(
                        work.finish().expect("completed presentation lighting"),
                    )),
                    admission_lighting_valid: false,
                }),
            );
            server.cluster_chunk_presentations.store(Arc::new(next));
            let current = server.cluster_chunk_presentations.load_full();
            if let Some(presentation) = current.get(&key)
                && let Ok(light) = crate::net::java::chunk_data::owned::light_from_document(
                    &presentation.image.document().root_tag,
                    presentation.blocks.min_y(),
                    presentation.blocks.block_palettes().len(),
                    presentation.lighting.as_deref(),
                )
            {
                let packet = pumpkin_protocol::java::client::play::CLightUpdate::new(
                    key.x().into(),
                    key.z().into(),
                    light,
                );
                for player in super::cluster_chunk_watch::recipients(server, key) {
                    if let crate::net::ClientPlatform::Java(client) = player.client.as_ref()
                        && client.version.load()
                            >= pumpkin_util::version::JavaMinecraftVersion::V_1_14
                    {
                        player.try_send_client_packet(&packet);
                    }
                }
            }
        }
    }
}
