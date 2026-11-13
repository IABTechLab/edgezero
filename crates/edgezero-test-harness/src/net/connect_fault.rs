#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_proof_requires_the_owned_inode_and_exact_destination() {
        let table = "header\n0: 0100007F:9C40 0100007F:1234 02 0 0 0 0 0 17\n1: 0100007F:9C41 0100007F:1234 01 0 0 0 0 0 18\n";
        let target = "127.0.0.1:4660".parse().unwrap();
        let rows = owned_rows(table, &[17, 18], target).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].pending(target));
        assert!(!rows[1].pending(target));
        assert!(owned_rows(table, &[99], target).unwrap().is_empty());
        assert!(
            owned_rows(table, &[17], "127.0.0.1:4661".parse().unwrap())
                .unwrap()
                .is_empty()
        );
        assert!(owned_rows("header\nshort\n", &[17], target).is_err());
    }

    #[test]
    fn proc_identity_handles_parentheses_and_rejects_the_wrong_process() {
        let stat = format!("73 (spin (worker)) S {} 999 0", vec!["0"; 18].join(" "));
        assert_eq!(start_ticks(&stat, 73).unwrap(), 999);
        assert!(start_ticks(&stat, 74).is_err());
        assert!(start_ticks("73 (spin) S", 73).is_err());
    }
}

#[cfg(all(test, unix))]
mod resolver_tests {
    use super::*;

    const TCP: &str = "header\n0: 0100007F:1234 00000000:0000 0A 0 0 0 0 0 17\n";

    fn stat(pid: u32, group: u32, ticks: u64) -> String {
        format!(
            "{pid} (spin (worker)) S 0 {group} {} {ticks} 0",
            vec!["0"; 16].join(" ")
        )
    }

    fn process(root: &Path, pid: u32, group: u32, ticks: u64, inodes: &[u64], table: &str) {
        let path = root.join(pid.to_string());
        fs::create_dir_all(path.join("fd")).unwrap();
        fs::create_dir_all(path.join("net")).unwrap();
        fs::write(path.join("stat"), stat(pid, group, ticks)).unwrap();
        fs::write(path.join("net/tcp"), table).unwrap();
        for (fd, inode) in inodes.iter().enumerate() {
            std::os::unix::fs::symlink(
                format!("socket:[{inode}]"),
                path.join("fd").join(fd.to_string()),
            )
            .unwrap();
        }
    }

    fn endpoint() -> SocketAddr {
        "127.0.0.1:4660".parse().unwrap()
    }

    #[test]
    fn selects_launcher_or_child_listener_and_never_reads_foreign_fds() {
        for owner_pid in [100, 101] {
            let root = tempfile::tempdir().unwrap();
            process(
                root.path(),
                100,
                100,
                1000,
                if owner_pid == 100 { &[17] } else { &[] },
                TCP,
            );
            process(
                root.path(),
                101,
                100,
                1010,
                if owner_pid == 101 { &[17] } else { &[] },
                TCP,
            );
            fs::create_dir(root.path().join("200")).unwrap();
            fs::write(root.path().join("200/stat"), stat(200, 200, 2000)).unwrap();
            let owner = resolve_listener_at(root.path(), 100, endpoint()).unwrap();
            assert_eq!(owner.pid, owner_pid);
            assert_eq!(owner.process_group, 100);
            assert_eq!(owner.leader_start_ticks, 1000);
            assert_eq!(
                owner.start_ticks,
                if owner_pid == 100 { 1000 } else { 1010 }
            );
            assert_eq!(owner.listener_inode, 17);
        }
    }

    #[test]
    fn deduplicates_pid_inode_but_rejects_shared_listener_ownership() {
        let root = tempfile::tempdir().unwrap();
        let duplicate = format!("{TCP}1: 0100007F:1234 00000000:0000 0A 0 0 0 0 0 17\n");
        process(root.path(), 100, 100, 1000, &[17, 17], &duplicate);
        assert_eq!(
            resolve_listener_at(root.path(), 100, endpoint())
                .unwrap()
                .listener_inode,
            17
        );
        process(root.path(), 101, 100, 1010, &[17], TCP);
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
    }

    #[test]
    fn rejects_zero_wrong_endpoint_and_non_listening_candidates() {
        for table in [
            "header\n",
            "header\n0: 0200007F:1234 00000000:0000 0A 0 0 0 0 0 17\n",
            "header\n0: 0100007F:1234 0100007F:9C40 01 0 0 0 0 0 17\n",
        ] {
            let root = tempfile::tempdir().unwrap();
            process(root.path(), 100, 100, 1000, &[17], table);
            assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
        }
    }

    #[test]
    fn rejects_pid_tick_or_group_changes_during_identity_guard() {
        for replacement in [
            stat(99, 100, 1000),
            stat(100, 100, 1001),
            stat(100, 200, 1000),
        ] {
            let root = tempfile::tempdir().unwrap();
            process(root.path(), 100, 100, 1000, &[17], TCP);
            let expected = ProcessIdentity {
                pid: 100,
                process_group: 100,
                start_ticks: 1000,
            };
            assert!(
                with_identity(root.path(), 100, Some(expected), |_| {
                    fs::write(root.path().join("100/stat"), replacement)?;
                    Ok(())
                })
                .is_err()
            );
        }
    }

    #[test]
    fn pinned_observation_cannot_reselect_after_host_or_leader_changes() {
        for (pid, replacement) in [
            (101, stat(101, 200, 1010)),
            (101, stat(101, 100, 1011)),
            (100, stat(100, 100, 1001)),
            (100, stat(100, 200, 1000)),
        ] {
            let root = tempfile::tempdir().unwrap();
            process(root.path(), 100, 100, 1000, &[], TCP);
            process(root.path(), 101, 100, 1010, &[17], TCP);
            let owner = resolve_listener_at(root.path(), 100, endpoint()).unwrap();
            fs::write(root.path().join(format!("{pid}/stat")), replacement).unwrap();
            process(root.path(), 102, 100, 1020, &[17], TCP);
            assert!(observe_pinned_at(root.path(), &owner, endpoint()).is_err());
        }
    }

    #[test]
    fn enforces_exact_process_entry_limit_even_after_finding_owner() {
        let root = tempfile::tempdir().unwrap();
        process(root.path(), 100, 100, 1000, &[17], TCP);
        for pid in 1000..5095 {
            fs::create_dir(root.path().join(pid.to_string())).unwrap();
            fs::write(
                root.path().join(format!("{pid}/stat")),
                stat(pid, 200, u64::from(pid)),
            )
            .unwrap();
        }
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_ok());
        fs::create_dir(root.path().join("5095")).unwrap();
        fs::write(root.path().join("5095/stat"), stat(5095, 200, 5095)).unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
    }

    #[test]
    fn enforces_fd_stat_and_tcp_byte_limits() {
        let root = tempfile::tempdir().unwrap();
        process(root.path(), 100, 100, 1000, &vec![17; 512], TCP);
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_ok());
        std::os::unix::fs::symlink("socket:[17]", root.path().join("100/fd/512")).unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
        fs::remove_file(root.path().join("100/fd/512")).unwrap();
        let original = stat(100, 100, 1000);
        fs::write(
            root.path().join("100/stat"),
            format!("{original}{}", " ".repeat(16 * 1024 - original.len())),
        )
        .unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_ok());
        fs::write(root.path().join("100/stat"), "x".repeat(16 * 1024 + 1)).unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
        fs::write(root.path().join("100/stat"), original).unwrap();
        let table = format!("{}{TCP}", "h".repeat(64 * 1024 - TCP.len()));
        fs::write(root.path().join("100/net/tcp"), &table).unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_ok());
        fs::write(root.path().join("100/net/tcp"), format!("h{table}")).unwrap();
        assert!(resolve_listener_at(root.path(), 100, endpoint()).is_err());
    }
}
use crate::evidence::{Result, require};
use serde_json::{Value, json};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::BTreeSet, fs, io::Read, net::SocketAddr, path::Path, thread, time::Duration,
};

pub struct Fault {
    _listener: Socket,
    fillers: Vec<Socket>,
    target: SocketAddr,
}

#[derive(Debug)]
pub struct Row {
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub inode: u64,
    pub state: u8,
}

impl Row {
    pub fn pending(&self, target: SocketAddr) -> bool {
        self.remote == target && self.state == 2
    }

    pub fn json(&self) -> Value {
        json!({"inode":self.inode,"local":self.local.to_string(),
            "remote":self.remote.to_string(),"state":self.state})
    }
}

pub struct Observation {
    pub pid: u32,
    pub process_group: u32,
    pub start_ticks: u64,
    pub rows: Vec<Row>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessIdentity {
    pid: u32,
    process_group: u32,
    start_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ListenerOwner {
    pub pid: u32,
    pub process_group: u32,
    pub start_ticks: u64,
    pub leader_start_ticks: u64,
    pub listener_inode: u64,
}

fn identity(root: &Path, pid: u32) -> Result<ProcessIdentity> {
    let stat = bounded_file(root.join(pid.to_string()).join("stat"), 16 * 1024)?;
    let (_, fields) = stat.rsplit_once(')').ok_or("invalid process identity")?;
    Ok(ProcessIdentity {
        pid,
        process_group: fields
            .split_ascii_whitespace()
            .nth(2)
            .ok_or("missing process group")?
            .parse()?,
        start_ticks: start_ticks(&stat, pid)?,
    })
}

fn with_identity<T>(
    root: &Path,
    pid: u32,
    expected: Option<ProcessIdentity>,
    read: impl FnOnce(ProcessIdentity) -> Result<T>,
) -> Result<(ProcessIdentity, T)> {
    let before = identity(root, pid)?;
    if let Some(expected) = expected {
        require(
            before == expected,
            "pinned process identity or group changed",
        )?;
    }
    let value = read(before)?;
    require(
        identity(root, pid)? == before,
        "owned process identity or group changed during observation",
    )?;
    Ok((before, value))
}

fn resolve_listener_at(root: &Path, group: u32, target: SocketAddr) -> Result<ListenerOwner> {
    require(
        group > 0 && target.is_ipv4() && target.ip().is_loopback() && target.port() != 0,
        "invalid owned listener resolution target",
    )?;
    let (_, owner) = with_identity(root, group, None, |leader| {
        require(
            leader.process_group == group,
            "registered leader does not own its process group",
        )?;
        let mut entries = 0;
        let mut seen = BTreeSet::new();
        let mut owner = None;
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name
                .to_str()
                .filter(|name| !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_digit()))
            else {
                continue;
            };
            entries += 1;
            require(entries <= 4096, "process inspection exceeded 4096 entries")?;
            let pid = name.parse()?;
            let candidate = match identity(root, pid) {
                Ok(candidate) => candidate,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if candidate.process_group != group {
                continue;
            }
            let observed = observe_at(root, pid, target, Some(candidate))?;
            for row in observed.rows {
                if row.state != 10
                    || row.local != target
                    || row.inode == 0
                    || !row.remote.ip().is_unspecified()
                    || row.remote.port() != 0
                {
                    continue;
                }
                if !seen.insert((pid, row.inode)) {
                    continue;
                }
                require(
                    owner.is_none(),
                    "multiple owned listener PID/inode bindings",
                )?;
                owner = Some(ListenerOwner {
                    pid,
                    process_group: group,
                    start_ticks: observed.start_ticks,
                    leader_start_ticks: leader.start_ticks,
                    listener_inode: row.inode,
                });
            }
        }
        let owner = owner.ok_or("no unique listener in the owned process group")?;
        let pinned = observe_pinned_at(root, &owner, target)?;
        require(
            pinned.rows.iter().any(|row| {
                row.state == 10 && row.local == target && row.inode == owner.listener_inode
            }),
            "resolved listener changed before scan completed",
        )?;
        Ok(owner)
    })?;
    Ok(owner)
}

pub fn resolve_listener(group: u32, target: SocketAddr) -> Result<ListenerOwner> {
    resolve_listener_at(Path::new("/proc"), group, target)
}

fn observe_pinned_at(
    root: &Path,
    owner: &ListenerOwner,
    target: SocketAddr,
) -> Result<Observation> {
    let leader = ProcessIdentity {
        pid: owner.process_group,
        process_group: owner.process_group,
        start_ticks: owner.leader_start_ticks,
    };
    let host = ProcessIdentity {
        pid: owner.pid,
        process_group: owner.process_group,
        start_ticks: owner.start_ticks,
    };
    let (_, observed) = with_identity(root, leader.pid, Some(leader), |_| {
        observe_at(root, host.pid, target, Some(host))
    })?;
    Ok(observed)
}

pub fn observe_pinned(owner: &ListenerOwner, target: SocketAddr) -> Result<Observation> {
    observe_pinned_at(Path::new("/proc"), owner, target)
}

fn bounded_file(path: impl AsRef<Path>, limit: u64) -> Result<String> {
    let mut text = String::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_string(&mut text)?;
    require(
        text.len() as u64 <= limit,
        "bounded process metadata exceeded its limit",
    )?;
    Ok(text)
}

fn address(text: &str) -> Result<SocketAddr> {
    let (ip, port) = text.split_once(':').ok_or("invalid TCP address")?;
    require(ip.len() == 8 && port.len() == 4, "invalid IPv4 TCP field")?;
    Ok(SocketAddr::from((
        u32::from_str_radix(ip, 16)?.to_ne_bytes(),
        u16::from_str_radix(port, 16)?,
    )))
}

fn owned_rows(table: &str, owned: &[u64], target: SocketAddr) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for line in table.lines().skip(1) {
        let columns: Vec<_> = line.split_ascii_whitespace().collect();
        require(columns.len() >= 10, "invalid TCP process row")?;
        let inode = columns[9].parse()?;
        if !owned.contains(&inode) {
            continue;
        }
        let row = Row {
            local: address(columns[1])?,
            remote: address(columns[2])?,
            state: u8::from_str_radix(columns[3], 16)?,
            inode,
        };
        if row.local == target || row.remote == target {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn start_ticks(stat: &str, pid: u32) -> Result<u64> {
    let (prefix, fields) = stat.rsplit_once(')').ok_or("invalid process identity")?;
    require(
        prefix.split_whitespace().next() == Some(pid.to_string().as_str()),
        "process identity belongs to a different PID",
    )?;
    fields
        .split_ascii_whitespace()
        .nth(19)
        .ok_or("missing process start time")?
        .parse()
        .map_err(Into::into)
}

fn observe_at(
    root: &Path,
    pid: u32,
    target: SocketAddr,
    expected: Option<ProcessIdentity>,
) -> Result<Observation> {
    let (identity, rows) = with_identity(root, pid, expected, |_| {
        let mut owned = Vec::new();
        for (index, entry) in fs::read_dir(root.join(pid.to_string()).join("fd"))?.enumerate() {
            require(
                index < 512,
                "owned process FD inspection exceeded 512 entries",
            )?;
            let link = match fs::read_link(entry?.path()) {
                Ok(link) => link,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if let Some(inode) = link
                .to_str()
                .and_then(|s| s.strip_prefix("socket:["))
                .and_then(|s| s.strip_suffix(']'))
            {
                owned.push(inode.parse()?);
            }
        }
        owned_rows(
            &bounded_file(root.join(pid.to_string()).join("net/tcp"), 64 * 1024)?,
            &owned,
            target,
        )
    })?;
    Ok(Observation {
        pid,
        process_group: identity.process_group,
        start_ticks: identity.start_ticks,
        rows,
    })
}

pub fn observe(pid: u32, target: SocketAddr) -> Result<Observation> {
    observe_at(Path::new("/proc"), pid, target, None)
}

impl Fault {
    pub fn start() -> Result<Self> {
        require(
            std::env::consts::OS == "linux",
            "silent-connect qualification requires Linux /proc ownership evidence",
        )?;
        require(
            bounded_file("/proc/sys/net/ipv4/tcp_abort_on_overflow", 16)?.trim() == "0",
            "listener overflow policy resets rather than silently drops",
        )?;
        let listener = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        listener.bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
        listener.listen(1)?;
        let target = listener
            .local_addr()?
            .as_socket()
            .ok_or("missing IPv4 listener")?;
        let mut fillers = Vec::with_capacity(8);
        for _ in 0..8 {
            let filler = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
            filler.set_nonblocking(true)?;
            if let Err(error) = filler.connect(&target.into())
                && error.kind() != std::io::ErrorKind::WouldBlock
                && error.raw_os_error() != Some(115)
            {
                return Err(error.into());
            }
            fillers.push(filler);
            thread::sleep(Duration::from_millis(30));
        }
        let proof = observe(std::process::id(), target)?;
        require(
            proof
                .rows
                .iter()
                .any(|row| row.state == 1 && row.remote == target)
                && proof.rows.iter().any(|row| row.pending(target)),
            "fault did not establish both full-backlog and silently pending controls",
        )?;
        Ok(Self {
            _listener: listener,
            fillers,
            target,
        })
    }

    pub fn target(&self) -> SocketAddr {
        self.target
    }

    pub fn policy(&self) -> Value {
        json!({"kind":"linux-loopback-listen-backlog-saturation","listen_backlog":1,
            "fillers":self.fillers.len(),"accept_calls":0,"max_owned_sockets":9,
            "target":self.target.to_string(),"kernel":bounded_file("/proc/sys/kernel/osrelease",4096).ok().map(|v|v.trim().to_owned())})
    }
}
