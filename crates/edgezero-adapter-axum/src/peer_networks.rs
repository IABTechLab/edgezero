//! Shared bounded socket-peer networks for forwarding and protected Geo admission.
use crate::proxy::ProxyTrustConfigError;
use ipnet::{IpNet, Ipv4Net};
use std::net::{IpAddr, Ipv6Addr};
const MAX_BYTES: usize = 0x4000;
pub(crate) const MAX_NETWORKS: usize = 256;

#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct PeerNetworks(Vec<IpNet>);
impl PeerNetworks {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn new<I, S>(networks: I) -> Result<Self, ProxyTrustConfigError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = Vec::new();
        let mut bytes = 0_usize;
        for (index, entry) in networks.into_iter().enumerate() {
            if index >= MAX_NETWORKS {
                return Err(ProxyTrustConfigError);
            }
            let raw = entry.as_ref();
            bytes = bytes
                .checked_add(raw.len())
                .and_then(|size| size.checked_add(usize::from(index != 0)))
                .filter(|size| *size <= MAX_BYTES)
                .ok_or(ProxyTrustConfigError)?;
            if raw
                .bytes()
                .any(|byte| byte.is_ascii_control() && byte != b'\t')
            {
                return Err(ProxyTrustConfigError);
            }
            let literal = raw.trim_matches([' ', '\t']);
            let parsed = literal
                .parse::<IpNet>()
                .or_else(|_error| literal.parse::<IpAddr>().map(IpNet::from))
                .map_err(|_error| ProxyTrustConfigError)?;
            let network = normalize_network(parsed)?;
            if !normalized.contains(&network) {
                normalized.push(network);
            }
        }
        if covers_family(&normalized, false) || covers_family(&normalized, true) {
            return Err(ProxyTrustConfigError);
        }
        Ok(Self(normalized))
    }
    pub(crate) fn trusts(&self, address: IpAddr) -> bool {
        let canonical = canonical_ip(address);
        self.0.iter().any(|network| network.contains(&canonical))
    }
}
pub(crate) fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        IpAddr::V4(v4) => IpAddr::V4(v4),
    }
}

fn normalize_network(network: IpNet) -> Result<IpNet, ProxyTrustConfigError> {
    let truncated = network.trunc();
    if truncated.prefix_len() == 0 {
        return Err(ProxyTrustConfigError);
    }
    if let IpNet::V6(v6) = truncated {
        let mapped_start = Ipv6Addr::from(0xffff_u128 << 32_u32);
        if v6.prefix_len() < 96 && v6.contains(&mapped_start) {
            // A shorter overlapping prefix contains every mapped IPv4 address.
            return Err(ProxyTrustConfigError);
        }
        if let Some(v4) = v6.network().to_ipv4_mapped() {
            let prefix = v6
                .prefix_len()
                .checked_sub(96)
                .ok_or(ProxyTrustConfigError)?;
            if prefix == 0 {
                return Err(ProxyTrustConfigError);
            }
            return Ipv4Net::new(v4, prefix)
                .map(IpNet::V4)
                .map_err(|_error| ProxyTrustConfigError);
        }
    }
    Ok(truncated)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the maximum endpoint returns before end + 1; IPv4 endpoints fit u128 and the IPv6 maximum is handled explicitly"
)]
fn covers_family(networks: &[IpNet], ipv6: bool) -> bool {
    let mut intervals: Vec<(u128, u128)> = networks
        .iter()
        .filter_map(|network| match network {
            IpNet::V4(v4) if !ipv6 => Some((
                u128::from(u32::from(v4.network())),
                u128::from(u32::from(v4.broadcast())),
            )),
            IpNet::V6(v6) if ipv6 => Some((u128::from(v6.network()), u128::from(v6.broadcast()))),
            IpNet::V4(_) | IpNet::V6(_) => None,
        })
        .collect();
    intervals.sort_unstable();
    let maximum = if ipv6 {
        u128::MAX
    } else {
        u128::from(u32::MAX)
    };
    let mut next = 0_u128;
    for (start, end) in intervals {
        if start > next {
            return false;
        }
        if end == maximum {
            return true;
        }
        next = next.max(end + 1);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn family_interval_merging_checks_maximum_endpoints() {
        // Exercise interval merging's maximum endpoint without enumerating addresses.
        let v6 = ["::/1".parse().unwrap(), "8000::/1".parse().unwrap()];
        assert!(covers_family(&v6, true));
        assert!(!covers_family(&["ffff::/16".parse().unwrap()], true));
        assert!(!covers_family(
            &["0.0.0.0/1".parse().unwrap(), "192.0.0.0/2".parse().unwrap()],
            false
        ));
    }
}
