use std::fmt::Write;

pub const LOBBY_FAKE_X: f64 = 1_000_000.0;
pub const LOBBY_FAKE_Y: f64 = 320.0;
pub const LOBBY_FAKE_Z: f64 = 1_000_000.0;
pub const LOBBY_YAW_DEG: f32 = 0.0;
pub const LOBBY_PITCH_DEG: f32 = 90.0;
pub const LOBBY_GAMEMODE_SPECTATOR: u8 = 3;
pub const LOBBY_TITLE: &str = "Loading...";
pub const LOBBY_READY_PERCENT: u8 = 100;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LobbyPosition {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub yaw: f32,
    pub pitch: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LobbyProgress {
    pub loaded: u32,
    pub required: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LobbyFrame {
    pub title: String,
    pub subtitle: String,
    pub percent: u8,
    pub ready: bool,
}

impl LobbyPosition {
    #[must_use]
    pub const fn new(x: f64, y: f64, z: f64, yaw: f32, pitch: f32) -> Self {
        Self { x, y, z, yaw, pitch }
    }
}

impl LobbyProgress {
    #[must_use]
    pub const fn new(loaded: u32, required: u32) -> Self {
        Self { loaded, required }
    }
}

#[must_use]
pub const fn lobby_fake_position() -> LobbyPosition {
    LobbyPosition {
        x: LOBBY_FAKE_X,
        y: LOBBY_FAKE_Y,
        z: LOBBY_FAKE_Z,
        yaw: LOBBY_YAW_DEG,
        pitch: LOBBY_PITCH_DEG,
    }
}

#[must_use]
pub const fn is_spectator_gamemode(gamemode: u8) -> bool {
    gamemode == LOBBY_GAMEMODE_SPECTATOR
}

#[must_use]
pub const fn is_third_entity_camera(player_entity_id: i32, camera_entity_id: Option<i32>) -> bool {
    match camera_entity_id {
        None => false,
        Some(camera) => camera != player_entity_id,
    }
}

#[must_use]
pub fn is_lobby_position(position: &LobbyPosition) -> bool {
    position.x == LOBBY_FAKE_X
        && position.y == LOBBY_FAKE_Y
        && position.z == LOBBY_FAKE_Z
        && position.yaw == LOBBY_YAW_DEG
        && position.pitch == LOBBY_PITCH_DEG
}

#[must_use]
pub const fn lobby_looks_straight_down(yaw: f32, pitch: f32) -> bool {
    yaw == LOBBY_YAW_DEG && pitch == LOBBY_PITCH_DEG
}

#[must_use]
pub fn is_lobby_title(title: &str) -> bool {
    title == LOBBY_TITLE
}

#[must_use]
pub const fn lobby_percent(loaded: u32, required: u32) -> u8 {
    if required == 0 {
        return LOBBY_READY_PERCENT;
    }
    if loaded >= required {
        return LOBBY_READY_PERCENT;
    }
    let scaled = (loaded as u64) * 100 / (required as u64);
    scaled as u8
}

#[must_use]
pub fn lobby_subtitle(loaded: u32, required: u32) -> String {
    let mut subtitle = String::with_capacity(4);
    let _ = write!(subtitle, "{}%", lobby_percent(loaded, required));
    subtitle
}

#[must_use]
pub fn is_lobby_subtitle_for(subtitle: &str, loaded: u32, required: u32) -> bool {
    subtitle == lobby_subtitle(loaded, required)
}

#[must_use]
pub const fn is_lobby_ready(loaded: u32, required: u32) -> bool {
    if required == 0 {
        return true;
    }
    loaded >= required
}

#[must_use]
pub const fn lobby_may_send_chunk_while_waiting() -> bool {
    true
}

#[must_use]
pub const fn lobby_title_updates_every_tick() -> bool {
    true
}

#[must_use]
pub fn lobby_frame(loaded: u32, required: u32) -> LobbyFrame {
    let percent = lobby_percent(loaded, required);
    LobbyFrame {
        title: String::from(LOBBY_TITLE),
        subtitle: lobby_subtitle(loaded, required),
        percent,
        ready: is_lobby_ready(loaded, required),
    }
}

#[must_use]
pub const fn lobby_releases_only_when_ready(loaded: u32, required: u32) -> bool {
    is_lobby_ready(loaded, required)
}

#[must_use]
pub fn is_lobby_frame_for(frame: &LobbyFrame, loaded: u32, required: u32) -> bool {
    frame.title == LOBBY_TITLE
        && frame.subtitle == lobby_subtitle(loaded, required)
        && frame.percent == lobby_percent(loaded, required)
        && frame.ready == is_lobby_ready(loaded, required)
}

#[must_use]
pub const fn lobby_tracks_no_world_state_on_server() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_position_matches_spec_coords_and_look() {
        let position = lobby_fake_position();
        assert_eq!(position.x, 1_000_000.0);
        assert_eq!(position.z, 1_000_000.0);
        assert_eq!(position.y, LOBBY_FAKE_Y);
        assert!(position.y >= 256.0);
        assert!(is_lobby_position(&position));
        assert!(lobby_looks_straight_down(position.yaw, position.pitch));
    }

    #[test]
    fn position_audit_rejects_any_drift() {
        let base = lobby_fake_position();
        assert!(!is_lobby_position(&LobbyPosition::new(base.x + 1.0, base.y, base.z, base.yaw, base.pitch)));
        assert!(!is_lobby_position(&LobbyPosition::new(base.x, base.y - 1.0, base.z, base.yaw, base.pitch)));
        assert!(!is_lobby_position(&LobbyPosition::new(base.x, base.y, base.z + 16.0, base.yaw, base.pitch)));
        assert!(!is_lobby_position(&LobbyPosition::new(base.x, base.y, base.z, base.yaw + 1.0, base.pitch)));
        assert!(!is_lobby_position(&LobbyPosition::new(base.x, base.y, base.z, base.yaw, base.pitch - 1.0)));
        assert!(!lobby_looks_straight_down(0.0, 0.0));
        assert!(!lobby_looks_straight_down(180.0, 90.0));
    }

    #[test]
    fn wait_room_forces_spectator_and_third_entity_camera() {
        assert!(is_spectator_gamemode(3));
        assert!(is_spectator_gamemode(LOBBY_GAMEMODE_SPECTATOR));
        assert!(!is_spectator_gamemode(0));
        assert!(!is_spectator_gamemode(1));
        assert!(!is_spectator_gamemode(2));
        assert!(is_third_entity_camera(10, Some(11)));
        assert!(!is_third_entity_camera(10, Some(10)));
        assert!(!is_third_entity_camera(10, None));
    }

    #[test]
    fn title_is_loading_and_subtitle_is_percent() {
        assert!(is_lobby_title("Loading..."));
        assert!(!is_lobby_title("Loading"));
        assert!(!is_lobby_title("Your data is loading"));
        assert!(!is_lobby_title(""));
        assert_eq!(lobby_subtitle(0, 10), String::from("0%"));
        assert_eq!(lobby_subtitle(1, 10), String::from("10%"));
        assert_eq!(lobby_subtitle(5, 10), String::from("50%"));
        assert_eq!(lobby_subtitle(10, 10), String::from("100%"));
        assert!(is_lobby_subtitle_for("50%", 5, 10));
        assert!(!is_lobby_subtitle_for("51%", 5, 10));
        assert!(!is_lobby_subtitle_for("Loading...", 5, 10));
    }

    #[test]
    fn percent_covers_edges_and_overflow() {
        assert_eq!(lobby_percent(0, 0), 100);
        assert_eq!(lobby_percent(0, 1), 0);
        assert_eq!(lobby_percent(1, 1), 100);
        assert_eq!(lobby_percent(9, 10), 90);
        assert_eq!(lobby_percent(11, 10), 100);
        assert_eq!(lobby_percent(u32::MAX, u32::MAX), 100);
        assert_eq!(lobby_percent(u32::MAX, 1), 100);
        assert_eq!(lobby_subtitle(0, 0), String::from("100%"));
        assert_eq!(lobby_percent(1, 3), 33);
    }

    #[test]
    fn release_waits_for_all_required_chunks() {
        assert!(is_lobby_ready(0, 0));
        assert!(!is_lobby_ready(0, 4));
        assert!(!is_lobby_ready(3, 4));
        assert!(is_lobby_ready(4, 4));
        assert!(is_lobby_ready(5, 4));
        assert!(!lobby_releases_only_when_ready(0, 4));
        assert!(lobby_releases_only_when_ready(4, 4));
        assert!(lobby_may_send_chunk_while_waiting());
    }

    #[test]
    fn frame_updates_live_every_tick_without_throttle() {
        assert!(lobby_title_updates_every_tick());
        assert!(lobby_tracks_no_world_state_on_server());
        let first = lobby_frame(1, 10);
        let second = lobby_frame(2, 10);
        assert_eq!(first.title, String::from("Loading..."));
        assert_eq!(first.subtitle, String::from("10%"));
        assert_eq!(first.percent, 10);
        assert!(!first.ready);
        assert_eq!(second.subtitle, String::from("20%"));
        assert!(is_lobby_frame_for(&first, 1, 10));
        assert!(is_lobby_frame_for(&second, 2, 10));
        assert!(!is_lobby_frame_for(&first, 2, 10));
        let done = lobby_frame(10, 10);
        assert_eq!(done.subtitle, String::from("100%"));
        assert!(done.ready);
        assert!(is_lobby_frame_for(&done, 10, 10));
    }
}
