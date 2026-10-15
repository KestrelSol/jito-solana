//! Optional tee of raw consensus-metrics events to a local UDP socket, for
//! external per-slot analysis (Alpenview). Enabled by setting
//! `ALPENVIEW_TEE_UDP=host:port`; otherwise inert.
//!
//! Runs on the metrics thread, never blocks: the socket is non-blocking and a
//! failed send is counted and dropped. Each datagram is one JSON line:
//! `{"t":<unix ns when the batch was received>,"e":[<events>]}` where an event is
//! `["v",kind,slot,block_id,vote_pubkey]`, `["r",slot,leader]` (block replay
//! completed), `["p",slot]` (parent ready) or `["f",slot]` (slot finalized).

use log::{info, warn};
use {
    agave_votor_messages::{metric_types::ConsensusMetricsEvent, vote::Vote},
    std::{
        fmt::Write as _,
        net::UdpSocket,
        time::{Instant, SystemTime, UNIX_EPOCH},
    },
};

const ENV_VAR: &str = "ALPENVIEW_TEE_UDP";
/// Keep datagrams comfortably under the 64 KiB UDP limit.
const MAX_DATAGRAM_BYTES: usize = 60_000;
const DROP_LOG_INTERVAL: u64 = 10_000;

pub struct AlpenviewTee {
    socket: UdpSocket,
    target: String,
    dropped: u64,
}

impl AlpenviewTee {
    /// Returns `None` unless `ALPENVIEW_TEE_UDP` is set and the socket can be created.
    pub fn from_env() -> Option<Self> {
        let target = std::env::var(ENV_VAR).ok()?;
        let socket = match UdpSocket::bind("0.0.0.0:0") {
            Ok(socket) => socket,
            Err(err) => {
                warn!("alpenview tee: cannot bind UDP socket: {err}");
                return None;
            }
        };
        if let Err(err) = socket.set_nonblocking(true) {
            warn!("alpenview tee: cannot set non-blocking: {err}");
            return None;
        }
        info!("alpenview tee: streaming consensus events to {target}");
        Some(Self {
            socket,
            target,
            dropped: 0,
        })
    }

    pub fn emit(&mut self, received: Instant, events: &[ConsensusMetricsEvent]) {
        if events.is_empty() {
            return;
        }
        let observed_ns = wall_clock_nanos(received);
        let header = format!("{{\"t\":{observed_ns},\"e\":[");
        let mut line = header.clone();
        let mut first = true;
        for event in events {
            let mut item = String::with_capacity(160);
            write_event(&mut item, event);
            if item.is_empty() { continue; }
            for piece in item.split("],[") {
                let piece = piece.trim_start_matches('[').trim_end_matches(']');
                let item = format!("[{piece}]");
            if line.len() + item.len() + 3 > MAX_DATAGRAM_BYTES && !first {
                line.push_str("]}");
                self.send(&line);
                line = header.clone();
                first = true;
            }
            if !first {
                line.push(',');
            }
            line.push_str(&item);
            first = false;
            }
        }
        line.push_str("]}");
        self.send(&line);
    }

    fn send(&mut self, line: &str) {
        if self.socket.send_to(line.as_bytes(), &self.target).is_err() {
            self.dropped += 1;
            if self.dropped % DROP_LOG_INTERVAL == 1 {
                warn!("alpenview tee: {} datagrams dropped so far", self.dropped);
            }
        }
    }
}

fn wall_clock_nanos(received: Instant) -> u128 {
    let elapsed = Instant::now().saturating_duration_since(received);
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|now| now.saturating_sub(elapsed).as_nanos())
        .unwrap_or(0)
}

fn write_event(out: &mut String, event: &ConsensusMetricsEvent) {
    match event {
        ConsensusMetricsEvent::Vote { ids, vote } => {
            let (kind, block_id) = match vote {
                Vote::Notarize(v) => ("notar", Some(v.block.block_id)),
                Vote::NotarizeFallback(v) => ("notar_fb", Some(v.block.block_id)),
                Vote::Finalize(_) => ("final", None),
                Vote::Skip(_) => ("skip", None),
                Vote::SkipFallback(_) => ("skip_fb", None),
                Vote::Genesis(v) => ("genesis", Some(v.block.block_id)),
            };
            let block_id = block_id.map(|h| h.to_string()).unwrap_or_default();
            for (i, id) in ids.as_slice().iter().enumerate() {
                if i > 0 { out.push(','); }
                let _ = write!(out, "[\"v\",\"{kind}\",{},\"{block_id}\",\"{id}\"]", vote.slot());
            }
        }
        ConsensusMetricsEvent::ReplayCompleted { leader, slot } => {
            let _ = write!(out, "[\"r\",{slot},\"{leader}\"]");
        }
        ConsensusMetricsEvent::ParentReadySeen { slot } => {
            let _ = write!(out, "[\"p\",{slot}]");
        }
        ConsensusMetricsEvent::SlotFinalized { slot } => {
            let _ = write!(out, "[\"f\",{slot}]");
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use agave_votor_messages::VoteAccountPubkeys;
    use solana_pubkey::Pubkey;
    use std::time::Duration;

    #[test]
    fn grouped_votes_split_into_bounded_udp_datagrams() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut tee = AlpenviewTee {
            socket: UdpSocket::bind("127.0.0.1:0").unwrap(),
            target: receiver.local_addr().unwrap().to_string(),
            dropped: 0,
        };
        let ids = (0..1000).map(|_| Pubkey::new_unique()).collect();
        tee.emit(Instant::now(), &[ConsensusMetricsEvent::Vote {
            ids: VoteAccountPubkeys::Owned(ids), vote: Vote::new_skip_vote(42),
        }]);
        let mut count = 0;
        let mut buf = [0u8; 65536];
        while count < 1000 {
            let n = receiver.recv(&mut buf).unwrap();
            assert!(n <= MAX_DATAGRAM_BYTES);
            let s = std::str::from_utf8(&buf[..n]).unwrap();
            assert!(s.starts_with("{\"t\":"));
            assert!(s.ends_with("]}"));
            assert!(!s.contains("],,["));
            count += s.matches("[\"v\",\"skip\",42,").count();
        }
        assert_eq!(count, 1000);
        assert_eq!(tee.dropped, 0);
    }

    #[test]
    fn new_timing_events_keep_distinct_names() {
        let mut out = String::new();
        write_event(&mut out, &ConsensusMetricsEvent::ParentReadySeen {slot: 42});
        assert_eq!(out, "[\"p\",42]");
        out.clear();
        write_event(&mut out, &ConsensusMetricsEvent::ReplayCompleted {slot: 42, leader: Pubkey::default()});
        assert!(out.starts_with("[\"r\",42,"));
    }
}
