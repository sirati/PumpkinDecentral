use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl, parse_quote};

pub fn expand_item(mut item: ItemImpl) -> syn::Result<TokenStream> {
    if item
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
        .is_none_or(|segment| segment.ident != "ItemBehaviour")
    {
        return Err(syn::Error::new_spanned(
            item,
            "canonical_item_defaults requires ItemBehaviour",
        ));
    }
    let methods: BTreeSet<_> = item
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method.sig.ident.to_string()),
            _ => None,
        })
        .collect();
    if !methods.contains("can_mine_intent")
        && !methods.contains("can_mine")
        && !item
            .items
            .iter()
            .any(|item| matches!(item, ImplItem::Macro(_)))
    {
        item.items.push(parse_quote! { fn can_mine_intent(&self, _game_mode: u8) -> Option<bool> { Some(true) } });
    }
    Ok(quote!(#item))
}

pub fn expand(mut item: ItemImpl) -> syn::Result<TokenStream> {
    if item
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
        .is_none_or(|segment| segment.ident != "BlockBehaviour")
    {
        return Err(syn::Error::new_spanned(
            item,
            "canonical_callback_defaults requires BlockBehaviour",
        ));
    }
    let methods: BTreeSet<_> = item
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method.sig.ident.to_string()),
            _ => None,
        })
        .collect();
    let opaque = item
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Macro(_)));
    if !opaque
        && !methods.contains("placement_intent")
        && !["on_place", "can_place_at", "can_update_at"]
            .iter()
            .any(|method| methods.contains(*method))
    {
        item.items.push(parse_quote! {
            fn placement_intent(&self, args: crate::block::PlacementIntentArgs<'_>)
                -> Option<crate::block::BlockStateId> {
                Some(args.block.default_state.id)
            }
        });
    }
    if !opaque && !methods.contains("can_update_at") && !methods.contains("can_update_intent") {
        item.items.push(parse_quote! {
            fn can_update_intent(&self, _args: crate::block::CanUpdateIntentArgs<'_>) -> Option<bool> {
                Some(false)
            }
        });
    }
    if !opaque && !methods.contains("random_tick") && !methods.contains("random_tick_intent") {
        item.items.push(parse_quote! {
            fn random_tick_intent(&self, _args: crate::block::RandomTickIntentArgs<'_>)
                -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
                Some(Vec::new())
            }
        });
    }
    if !opaque && !methods.contains("normal_use") && !methods.contains("normal_use_intent") {
        item.items.push(parse_quote! {
            fn normal_use_intent(&self, _args: crate::block::NormalUseIntentArgs<'_>)
                -> Option<(crate::block::registry::BlockActionResult, Vec<crate::block::neighbor::BlockCallbackEffect>)> {
                Some((crate::block::registry::BlockActionResult::Pass, Vec::new()))
            }
        });
    }
    if !opaque && !methods.contains("broken") && !methods.contains("player_broken_intent") {
        item.items.push(parse_quote! {
            fn player_broken_intent(&self, _args: crate::block::neighbor::PlayerBrokenIntentArgs<'_>)
                -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> { Some(Vec::new()) }
        });
    }
    if methods.contains("callback_intent") {
        return Ok(quote!(#item));
    }
    let branch = |legacy: &str| {
        if opaque || methods.contains(legacy) {
            quote!(None)
        } else {
            quote!(Some(Vec::new()))
        }
    };
    let replaced = branch("on_state_replaced");
    let placed = branch("placed");
    let prepare = branch("prepare");
    let neighbor = branch("on_neighbor_update");
    let scheduled = if opaque
        || methods.contains("scheduled_tick_intent")
        || methods.contains("on_scheduled_tick")
    {
        quote!(
            self.scheduled_tick_intent(crate::block::ScheduledTickIntentArgs {
                world: args.world,
                block: args.block,
                position: args.position,
            })
        )
    } else {
        quote!(Some(Vec::new()))
    };
    item.items.push(parse_quote! {
        fn callback_intent(&self, args: crate::block::neighbor::BlockCallbackIntentArgs<'_>)
            -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
            match args.event {
                crate::block::neighbor::BlockCallbackEvent::Scheduled => #scheduled,
                crate::block::neighbor::BlockCallbackEvent::StateReplaced { .. } => #replaced,
                crate::block::neighbor::BlockCallbackEvent::Placed { .. } => #placed,
                crate::block::neighbor::BlockCallbackEvent::Prepare { .. } => #prepare,
                crate::block::neighbor::BlockCallbackEvent::Neighbor { .. } => #neighbor,
            }
        }
    });
    Ok(quote!(#item))
}
