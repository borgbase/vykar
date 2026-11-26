//! The S3 services vykar knows the put-if-absent behaviour of: the tested
//! list `s3_conditional_writes: auto` trusts, and the known-bad list refused
//! even with `on` (backends.md, "Conditional writes").

/// What vykar knows about an S3 service's conditional writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    /// A recorded probe run passed every case, the concurrent race
    /// included: `auto` offers put-if-absent on its hosts.
    Tested,
    /// Offered by `auto` on the provider's documented conditional writes;
    /// no recorded probe run.
    Documented,
    /// A recorded probe run failed in a way the sequential check does not
    /// catch (two concurrent creates both win): put-if-absent is refused on
    /// its hosts whatever `s3_conditional_writes` says.
    KnownBad,
}

impl ServiceStatus {
    /// Whether `s3_conditional_writes: auto` offers put-if-absent.
    pub fn offered_by_auto(self) -> bool {
        matches!(self, Self::Tested | Self::Documented)
    }
}

/// One S3 service of the tested or known-bad list.
#[derive(Debug)]
pub struct S3Service {
    pub name: &'static str,
    /// Whether an endpoint host (lower-case, no port) belongs to the
    /// service. Software anyone hosts under any name (MinIO) has none: its
    /// entry records the run, and its users set `on`.
    pub host: fn(&str) -> bool,
    /// The hosts `host` accepts, for messages and docs.
    pub hosts: &'static str,
    pub status: ServiceStatus,
    /// What this entry rests on (date, version, where it is recorded).
    pub evidence: &'static str,
}

/// The services, first match wins.
///
/// IDrive e2's host forms (`*.idrivee2.com` and `*.idrivee2-<n>.com`) are
/// a reading of IDrive's public endpoint formats, not the endpoint of the
/// recorded probe run, which the repository does not keep.
pub const S3_SERVICES: &[S3Service] = &[
    S3Service {
        name: "AWS S3",
        host: is_aws_host,
        hosts: "*.amazonaws.com, *.amazonaws.com.cn",
        status: ServiceStatus::Documented,
        evidence: "trusted by auto since 04 step 8 on AWS's documented conditional writes; \
                   no vykar probe run recorded",
    },
    S3Service {
        name: "MinIO",
        host: no_host,
        hosts: "self-hosted, any name",
        status: ServiceStatus::Tested,
        evidence: "MinIO 2025-10-15, probe 21 Nov 2026; MinIO \
                   RELEASE.2025-09-07T16-13-09Z, probe 26 Nov 2026 (loopback): every case \
                   passed, the race and the list case included",
    },
    S3Service {
        name: "IDrive e2",
        host: is_idrive_e2_host,
        hosts: "*.idrivee2.com, *.idrivee2-<n>.com",
        status: ServiceStatus::KnownBad,
        evidence: "probe 21 Nov 2026 (fra1): 1 of 10 race rounds with a single winner, \
                   up to 8 winners",
    },
];

/// The service `endpoint`'s host belongs to: the first entry whose `host`
/// accepts it, one trailing `.` (a fully qualified name, which the URL
/// parser keeps) stripped first. `None` for an unknown host or an endpoint
/// that does not parse.
pub fn s3_service_of(endpoint: &str) -> Option<&'static S3Service> {
    let host = url::Url::parse(endpoint)
        .ok()?
        .host_str()?
        .to_ascii_lowercase();
    let host = host.strip_suffix('.').unwrap_or(&host);
    S3_SERVICES.iter().find(|s| (s.host)(host))
}

/// A host under `amazonaws.com` or `amazonaws.com.cn`. A custom domain in
/// front of AWS reads as non-AWS, which is the conservative answer for
/// `auto`.
fn is_aws_host(h: &str) -> bool {
    h.ends_with(".amazonaws.com") || h.ends_with(".amazonaws.com.cn")
}

fn no_host(_: &str) -> bool {
    false
}

/// Anchored at the registrable domain, as [`is_aws_host`] is: at least one
/// label, then `idrivee2` or `idrivee2-<digits>`, then `com`.
fn is_idrive_e2_host(h: &str) -> bool {
    let labels: Vec<&str> = h.split('.').collect();
    let [before @ .., domain, "com"] = labels.as_slice() else {
        return false;
    };
    if before.is_empty() || before.iter().any(|l| l.is_empty()) {
        return false;
    }
    match domain.strip_prefix("idrivee2") {
        Some("") => true,
        Some(rest) => rest
            .strip_prefix('-')
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name_of(endpoint: &str) -> Option<&'static str> {
        s3_service_of(endpoint).map(|s| s.name)
    }

    #[test]
    fn service_of_matches_hosts() {
        for host in [
            "https://s3.amazonaws.com",
            "https://s3.eu-west-1.amazonaws.com",
            "https://s3.dualstack.us-east-1.amazonaws.com",
            "https://s3.cn-north-1.amazonaws.com.cn",
            "https://S3.EU-WEST-1.AMAZONAWS.COM",
            "https://s3.eu-west-1.amazonaws.com.",
        ] {
            assert_eq!(name_of(host), Some("AWS S3"), "{host}");
        }
        for host in [
            "https://abcd.fra1.idrivee2-42.com",
            "https://s3.us-west-1.idrivee2.com",
            "https://ABCD.FRA1.IDRIVEE2-42.COM",
            "https://x.fra1.idrivee2-1.com.",
        ] {
            assert_eq!(name_of(host), Some("IDrive e2"), "{host}");
        }
        for host in [
            "http://minio.local:9000",
            "http://127.0.0.1:9000",
            "https://s3.example.com",
            "https://amazonaws.com.evil.com",
            "https://amazonaws.com",
            "https://idrivee2-42.evil.com",
            "https://x.idrivee2-42.evil.com",
            "https://x.notidrivee2.com",
            "https://x.notidrivee2-1.com",
            "https://x.idrivee2-.com",
            "https://x.idrivee2-1a.com",
            "https://x.idrivee2-42.com.evil.org",
            "https://idrivee2.com",
            "https://idrivee2-42.com",
            "https://x.fra1.idrivee2-1.com..",
            "",
            "not a url",
        ] {
            assert_eq!(name_of(host), None, "{host}");
        }
    }

    #[test]
    fn idrive_host_rule() {
        assert!(is_idrive_e2_host("abcd.fra1.idrivee2-42.com"));
        assert!(is_idrive_e2_host("s3.us-west-1.idrivee2.com"));
        assert!(!is_idrive_e2_host("idrivee2-42.evil.com"));
        assert!(!is_idrive_e2_host(".idrivee2.com"));
        assert!(!is_idrive_e2_host("x..idrivee2-1.com"));
    }

    #[test]
    fn auto_offers_on_tested_and_documented_only() {
        assert!(ServiceStatus::Tested.offered_by_auto());
        assert!(ServiceStatus::Documented.offered_by_auto());
        assert!(!ServiceStatus::KnownBad.offered_by_auto());
    }
}
