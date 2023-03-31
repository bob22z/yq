use crate::ArcString;

// messages      - int
#[inline]
pub(crate) fn mid_seq_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:mid-seq").into()
}

// messages      - hash: {mid mcontent} ; Message content
#[inline]
pub(crate) fn messages_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:messages").into()
}

// lock-times    - hash: {mid lock-ms}  ; Optional mid-specific lock duration
#[inline]
pub(crate) fn lock_times_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:lock-times").into()
}

// locks         - hash: {mid    lock-expiry-time} ; Active locks
#[inline]
pub(crate) fn locks_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:locks").into()
}

// messages      - hash: {mid mcontent} ; Message content
#[inline]
pub(crate) fn err_messages_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:err-msgs").into()
}

// error          - mid set: awaiting gc, etc.
#[inline]
pub(crate) fn err_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:err").into()
}

// done          - mid set: awaiting gc, etc.
#[inline]
pub(crate) fn done_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:done").into()
}

// mids-ready    - list: mids for immediate handling     (push to left, pop from right)
#[inline]
pub(crate) fn mids_ready_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:mids-ready").into()
}

// mid-circle    - list: mids for maintenance processing (push to left, pop from right)
#[inline]
pub(crate) fn mid_circle_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:mid-circle").into()
}

// ndry-runs     - int: num times worker(s) have lapped queue w/o work to do
#[inline]
pub(crate) fn ndry_runs_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:ndry-runs").into()
}

// isleep-a      - list: 0/1 sentinel element for `interruptible-sleep`
#[inline]
pub(crate) fn isleep_a_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:isleep-a").into()
}

// isleep-b      - list: 0/1 sentinel element for `interruptible-sleep`
#[inline]
pub(crate) fn isleep_b_key(prefix: &str, queue_name: &str) -> ArcString {
    format!("{prefix}:{queue_name}:isleep-b").into()
}

// isleep-b      - list: 0/1 sentinel element for `interruptible-sleep`
#[inline]
pub(crate) fn schedule_key(prefix: &str) -> ArcString {
    format!("{prefix}:schedule").into()
}
