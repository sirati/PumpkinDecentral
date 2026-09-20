/// Tick-batch and position-datagram wire codec.
///
///
/// Wire format is postcard encoding of [`TickBatch`] and of
/// `{ count: u16, updates: [PosUpdate] }`. The `count` header mirrors
/// `updates.len()` so UDP receivers can pre-size without scanning the body.
///
/// Zero-copy contract used by the hot paths:
/// - Encode borrows its input. [`encode_batch`] and [`encode_pos`] are thin
///   allocating wrappers; [`encode_batch_into`] and [`encode_pos_into`]
///   append directly onto a caller-owned `Vec<u8>` via
///   `postcard::to_extend`, so a tick loop reserves once and reuses the
///   buffer with no per-tick intermediate allocation. [`encode_batch_to_slice`]
///   and [`encode_pos_to_slice`] go one step further and write into a
///   caller-owned `&mut [u8]` with no heap at all.
/// - Decode borrows its input. `postcard::from_bytes` never copies the input
///   slice; only the decoded `Vec` payloads allocate. [`decode_batch_prefix`]
///   and [`decode_pos_prefix`] expose the trailing remainder via
///   `postcard::take_from_bytes` so framed streams decode in place without
///   splitting or copying frames first.
/// - [`encode_pos`] never clones the update slice. It serializes through a
///   borrowed `{ count, updates: &[PosUpdate] }` view whose postcard layout
///   is byte-identical to [`PosDatagram`].
///
/// Callers that send every tick should prefer the `_into` forms with a
/// buffer pre-sized by [`encoded_batch_len`] or [`encoded_pos_len`]; callers
/// that only need one buffer should use the allocating wrappers.
use serde::Serialize;

use crate::protocol::{PosUpdate, TickBatch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    pub message: String,
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CodecError {}

/// Position datagram: explicit `count` header plus the update list.
///
/// `count` mirrors `updates.len()` on encode. Decoders must tolerate a
/// mismatch (truncated `u16` range, mixed sender versions) and use
/// [`PosDatagram::is_consistent`] or [`decode_pos_strict`] to gate on it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PosDatagram {
    pub count: u16,
    pub updates: Vec<PosUpdate>,
}

impl PosDatagram {
    /// Length-mirror check: `count` matches the decoded update list.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        usize::from(self.count) == self.updates.len()
    }

    /// Number of updates in this datagram.
    #[must_use]
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    /// Whether this datagram carries no updates.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}

/// Borrowed encode view for position datagrams.
///
/// Layout matches [`PosDatagram`] field-for-field, so its postcard bytes are
/// identical while borrowing the caller's slice instead of cloning it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
struct PosDatagramRef<'borrow> {
    count: u16,
    updates: &'borrow [PosUpdate],
}

/// Saturating `u16` length header shared by every pos-datagram encoder.
///
/// Slices longer than `u16::MAX` truncate the header; the body still carries
/// every update, so receivers detect it via [`PosDatagram::is_consistent`].
fn pos_count(updates: &[PosUpdate]) -> u16 {
    u16::try_from(updates.len()).unwrap_or(u16::MAX)
}

/// Borrowed view for an update slice, with the truncated header attached.
fn pos_view(updates: &[PosUpdate]) -> PosDatagramRef<'_> {
    PosDatagramRef {
        count: pos_count(updates),
        updates,
    }
}

fn encode_error(context: &str, error: postcard::Error) -> CodecError {
    CodecError {
        message: format!("{context}: {error}"),
    }
}

/// Postcard-encodes a full tick batch into a fresh buffer.
pub fn encode_batch(batch: &TickBatch) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(batch).map_err(|error| encode_error("encode batch", error))
}

/// Decodes a full tick batch, borrowing `bytes` for the read.
///
/// Only the output `Vec` fields allocate; the input slice itself is never
/// copied.
pub fn decode_batch(bytes: &[u8]) -> Result<TickBatch, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| encode_error("decode batch", error))
}

/// Decodes a batch prefix, returning the batch plus the trailing remainder.
///
/// Lets framed readers walk `[batch | batch | ...]` in place: no split, no
/// copy, the remainder borrows from the same input slice.
pub fn decode_batch_prefix(bytes: &[u8]) -> Result<(TickBatch, &[u8]), CodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| encode_error("decode batch", error))
}

/// Appends a batch encoding onto caller-owned `out`, returning it.
///
/// Reuses the allocation across ticks: reserve once with
/// [`encoded_batch_len`], then hand the same buffer back each tick.
pub fn encode_batch_into(batch: &TickBatch, out: Vec<u8>) -> Result<Vec<u8>, CodecError> {
    postcard::to_extend(batch, out).map_err(|error| encode_error("encode batch", error))
}

/// Writes a batch encoding into `out` with no heap allocation.
///
/// Returns the used prefix of `out`; size the buffer with
/// [`encoded_batch_len`] first.
pub fn encode_batch_to_slice<'out>(
    batch: &TickBatch,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], CodecError> {
    postcard::to_slice(batch, out).map_err(|error| encode_error("encode batch", error))
}

/// Exact postcard length of `batch` without encoding it.
///
/// Pre-size-and-reuse pattern: `out.clear(); out.reserve(len); encode_into`.
pub fn encoded_batch_len(batch: &TickBatch) -> Result<usize, CodecError> {
    postcard::experimental::serialized_size(batch).map_err(|error| encode_error("size batch", error))
}

/// Postcard-encodes a position slice into a fresh buffer.
///
/// Zero-copy over the previous implementation: the slice is serialized
/// through [`PosDatagramRef`] without an intermediate `Vec` clone.
pub fn encode_pos(updates: &[PosUpdate]) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(&pos_view(updates)).map_err(|error| encode_error("encode pos", error))
}

/// Decodes a position datagram, borrowing `bytes` for the read.
///
/// Only the output `updates` vec allocates; the input slice is never copied.
/// Use [`decode_pos_strict`] when a mismatched `count` header must fail.
pub fn decode_pos(bytes: &[u8]) -> Result<PosDatagram, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| encode_error("decode pos", error))
}

/// Decodes a position datagram and rejects a mismatched `count` header.
pub fn decode_pos_strict(bytes: &[u8]) -> Result<PosDatagram, CodecError> {
    let datagram = decode_pos(bytes)?;
    if datagram.is_consistent() {
        Ok(datagram)
    } else {
        Err(CodecError {
            message: format!(
                "decode pos: count {} != updates {}",
                datagram.count,
                datagram.updates.len()
            ),
        })
    }
}

/// Decodes a pos-datagram prefix, returning it plus the trailing remainder.
///
/// Same in-place framing rationale as [`decode_batch_prefix`].
pub fn decode_pos_prefix(bytes: &[u8]) -> Result<(PosDatagram, &[u8]), CodecError> {
    postcard::take_from_bytes(bytes).map_err(|error| encode_error("decode pos", error))
}

/// Appends a pos-datagram encoding onto caller-owned `out`, returning it.
///
/// Borrowed counterpart to [`encode_pos`]: no intermediate `Vec` clone, the
/// bytes land directly in the reused buffer.
pub fn encode_pos_into(updates: &[PosUpdate], out: Vec<u8>) -> Result<Vec<u8>, CodecError> {
    postcard::to_extend(&pos_view(updates), out).map_err(|error| encode_error("encode pos", error))
}

/// Writes a pos-datagram encoding into `out` with no heap allocation.
///
/// Returns the used prefix of `out`; size the buffer with
/// [`encoded_pos_len`] first.
pub fn encode_pos_to_slice<'out>(
    updates: &[PosUpdate],
    out: &'out mut [u8],
) -> Result<&'out mut [u8], CodecError> {
    postcard::to_slice(&pos_view(updates), out).map_err(|error| encode_error("encode pos", error))
}

/// Exact postcard length of a position slice without encoding it.
pub fn encoded_pos_len(updates: &[PosUpdate]) -> Result<usize, CodecError> {
    postcard::experimental::serialized_size(&pos_view(updates))
        .map_err(|error| encode_error("size pos", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use crate::time::TickStamp;

    fn sample_gid() -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(3), PlayerSlot(7))
    }

    fn sample_pos() -> PosUpdate {
        PosUpdate {
            gid: sample_gid(),
            seq: PlayerSeq(12),
            tick: TickStamp(90),
            pos: [1.0, 64.0, -3.0],
            vel: [0.0, 0.0, 0.0],
            yaw: 45.0,
            pitch: 0.0,
        }
    }

    #[test]
    fn batch_roundtrip() {
        let mut batch = TickBatch::new(TickStamp(42));
        batch.pos.push(sample_pos());
        let bytes = encode_batch(&batch).unwrap();
        assert_eq!(decode_batch(&bytes).unwrap(), batch);
    }

    #[test]
    fn empty_batch_roundtrip() {
        let batch = TickBatch::new(TickStamp(1));
        let bytes = encode_batch(&batch).unwrap();
        let back = decode_batch(&bytes).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn pos_datagram_roundtrip() {
        let bytes = encode_pos(&[sample_pos(), sample_pos()]).unwrap();
        let back = decode_pos(&bytes).unwrap();
        assert!(back.is_consistent());
        assert_eq!(back.updates.len(), 2);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_batch(&[0xFF, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn into_reuses_buffer_without_realloc() {
        let mut batch = TickBatch::new(TickStamp(7));
        batch.pos.push(sample_pos());
        let scratch = Vec::with_capacity(encoded_batch_len(&batch).unwrap());
        let ptr = scratch.as_ptr();
        let bytes = encode_batch_into(&batch, scratch).unwrap();
        assert_eq!(decode_batch(&bytes).unwrap(), batch);
        assert_eq!(bytes.as_ptr(), ptr);

        let scratch = Vec::with_capacity(encoded_pos_len(&[sample_pos()]).unwrap());
        let bytes = encode_pos_into(&[sample_pos()], scratch).unwrap();
        assert!(decode_pos(&bytes).unwrap().is_consistent());
    }

    #[test]
    fn to_slice_matches_allocvec() {
        let mut batch = TickBatch::new(TickStamp(11));
        batch.pos.push(sample_pos());
        let mut buf = vec![0_u8; encoded_batch_len(&batch).unwrap()];
        let used_len = {
            let used = encode_batch_to_slice(&batch, &mut buf).unwrap();
            used.len()
        };
        assert_eq!(&buf[..used_len], encode_batch(&batch).unwrap());

        let updates = [sample_pos(), sample_pos()];
        let mut pos_buf = vec![0_u8; encoded_pos_len(&updates).unwrap()];
        let pos_len = {
            let used = encode_pos_to_slice(&updates, &mut pos_buf).unwrap();
            used.len()
        };
        assert_eq!(&pos_buf[..pos_len], encode_pos(&updates).unwrap());
    }

    #[test]
    fn prefix_decoders_leave_remainder() {
        let first = TickBatch::new(TickStamp(3));
        let mut second = TickBatch::new(TickStamp(4));
        second.pos.push(sample_pos());
        let mut framed = encode_batch(&first).unwrap();
        framed.extend_from_slice(&encode_batch(&second).unwrap());
        let (back, rest) = decode_batch_prefix(&framed).unwrap();
        assert_eq!(back, first);
        assert_eq!(decode_batch(rest).unwrap(), second);

        let mut pos_framed = encode_pos(&[sample_pos()]).unwrap();
        pos_framed.extend_from_slice(&encode_pos(&[sample_pos(), sample_pos()]).unwrap());
        let (datagram, rest) = decode_pos_prefix(&pos_framed).unwrap();
        assert_eq!(datagram.updates.len(), 1);
        assert_eq!(decode_pos(rest).unwrap().updates.len(), 2);
    }

    #[test]
    fn strict_pos_rejects_count_mismatch() {
        let datagram = PosDatagram {
            count: 9,
            updates: vec![sample_pos()],
        };
        assert!(!datagram.is_consistent());
        assert!(datagram.len() == 1);
        assert!(!datagram.is_empty());
        let bytes = postcard::to_allocvec(&datagram).unwrap();
        assert!(decode_pos(&bytes).unwrap().updates.len() == 1);
        assert!(decode_pos_strict(&bytes).is_err());
    }

    #[test]
    fn slice_too_small_is_an_error() {
        let batch = TickBatch::new(TickStamp(5));
        let len = encoded_batch_len(&batch).unwrap();
        if len > 1 {
            let mut tiny = [0_u8; 1];
            assert!(encode_batch_to_slice(&batch, &mut tiny).is_err());
        }
        let mut too_small = [0_u8; 1];
        assert!(encode_pos_to_slice(&[sample_pos()], &mut too_small).is_err());
    }
}
