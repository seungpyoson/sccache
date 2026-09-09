//! Tests for sccache args.
//!
//! Any copyright is dedicated to the Public Domain.
//! http://creativecommons.org/publicdomain/zero/1.0/
pub mod helpers;

use anyhow::Result;
use assert_cmd::prelude::*;
use helpers::{SCCACHE_BIN, stop_sccache};
use predicates::prelude::*;
use serial_test::serial;
use std::process::Command;

#[macro_use]
extern crate log;

#[test]
#[serial]
#[cfg(feature = "gcs")]
fn test_gcp_arg_check() -> Result<()> {
    trace!("sccache with log");
    stop_sccache()?;

    let mut cmd = Command::new(SCCACHE_BIN.as_os_str());
    cmd.arg("--start-server")
        .env("SCCACHE_LOG", "debug")
        .env("SCCACHE_GCS_KEY_PATH", "foo.json");

    cmd.assert().failure().stderr(predicate::str::contains(
        "If setting GCS credentials, SCCACHE_GCS_BUCKET",
    ));

    stop_sccache()?;

    let mut cmd = Command::new(SCCACHE_BIN.as_os_str());
    cmd.arg("--start-server")
        .env("SCCACHE_LOG", "debug")
        .env("SCCACHE_GCS_OAUTH_URL", "http://127.0.0.1");

    cmd.assert().failure().stderr(predicate::str::contains(
        "If setting GCS credentials, SCCACHE_GCS_BUCKET",
    ));

    stop_sccache()?;
    let mut cmd = Command::new(SCCACHE_BIN.as_os_str());
    cmd.arg("--start-server")
        .env("SCCACHE_LOG", "debug")
        .env("SCCACHE_GCS_BUCKET", "b")
        .env("SCCACHE_GCS_CREDENTIALS_URL", "not_valid_url//127.0.0.1")
        .env("SCCACHE_GCS_KEY_PATH", "foo.json");

    // This is just a warning
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("gcs credential url is invalid"));

    Ok(())
}

#[test]
#[serial]
#[cfg(feature = "s3")]
fn test_s3_invalid_args() -> Result<()> {
    stop_sccache()?;

    // A local provider refusal keeps this argument test independent of AWS.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let provider = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut request = [0u8; 4096];
        socket.read(&mut request).unwrap();
        socket
            .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
    });

    let mut cmd = Command::new(SCCACHE_BIN.as_os_str());
    cmd.arg("--start-server")
        .env("SCCACHE_LOG", "debug")
        .env("SCCACHE_BUCKET", "test")
        .env("SCCACHE_REGION", "us-east-1")
        .env("SCCACHE_ENDPOINT", endpoint)
        .env("SCCACHE_S3_USE_SSL", "false")
        .env("AWS_ACCESS_KEY_ID", "invalid_ak")
        .env("AWS_SECRET_ACCESS_KEY", "invalid_sk");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("cache storage failed to read"));

    provider.join().unwrap();

    Ok(())
}

fn isolated_command(directory: &std::path::Path, port: u16) -> Command {
    let mut command = Command::new(SCCACHE_BIN.as_os_str());
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("SCCACHE_")
            || name.to_string_lossy().starts_with("AWS_")
        {
            command.env_remove(name);
        }
    }
    command
        .env("SCCACHE_CONF", directory.join("empty.toml"))
        .env("SCCACHE_DIR", directory.join("cache"))
        .env("SCCACHE_SERVER_PORT", port.to_string())
        .env("SCCACHE_ERROR_LOG", directory.join("server.log"));
    command
}

#[test]
fn capability_invalid_endpoint_fails_before_bootstrap_side_effects() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("empty.toml"), "").unwrap();
    let cases = [
        (
            "SCCACHE_SERVER_PORT",
            "not-a-port",
            "invalid SCCACHE_SERVER_PORT",
        ),
        #[cfg(unix)]
        (
            "SCCACHE_SERVER_UDS",
            "relative.sock",
            "SCCACHE_SERVER_UDS must be an absolute path",
        ),
    ];
    for (name, value, reason) in cases {
        for internal in [false, true] {
            let mut command = isolated_command(directory.path(), 0);
            command.env(name, value);
            if internal {
                command
                    .env("SCCACHE_START_SERVER", "1")
                    .env("SCCACHE_NO_DAEMON", "1");
            } else {
                command.arg("--start-server");
            }
            command
                .assert()
                .failure()
                .stderr(predicate::str::contains(reason));
            assert!(!directory.path().join("server.log").exists());
            assert!(!directory.path().join("cache").exists());
        }
    }
}

#[test]
fn capability_startup_notification_roundtrips() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("empty.toml"), "").unwrap();
    for _ in 0..5 {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        drop(reservation);
        isolated_command(directory.path(), port)
            .arg("--start-server")
            .assert()
            .success();
        isolated_command(directory.path(), port)
            .arg("--stop-server")
            .assert()
            .success();
    }
}
