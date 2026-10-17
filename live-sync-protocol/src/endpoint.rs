/// Event-service credentials may only be sent to TLS origins, except loopback development.
/// A single origin is used by registration, ingestion and WebSocket routes. Reject embedded
/// credentials, queries and path prefixes so different clients cannot resolve different hosts.
pub fn gateway_endpoint(value: &str) -> Result<url::Url, &'static str> {
    let url = url::Url::parse(value).map_err(|_| "invalid event gateway URL")?;
    let loopback = match url.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if url.host().is_none() || !(url.scheme() == "https" || url.scheme() == "http" && loopback) {
        return Err("event gateway requires HTTPS except on loopback");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err("event gateway must be an origin without credentials, path, query or fragment");
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_tls_or_loopback_origins_can_receive_credentials() {
        for url in [
            "https://events.example.test",
            "http://127.0.0.1:9080",
            "http://[::1]:9080",
            "http://localhost:9080",
        ] {
            assert!(gateway_endpoint(url).is_ok(), "{url}");
        }
        for url in [
            "http://events.example.test",
            "http://127.0.0.1.evil.test",
            "https://user:pass@example.test",
            "https://example.test/?token=secret",
            "https://example.test/api",
            "ftp://example.test",
        ] {
            assert!(gateway_endpoint(url).is_err(), "{url}");
        }
    }
}
