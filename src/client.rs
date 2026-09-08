// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::*;
use crate::net::Connection;
use crate::protocol::{Request, Response};
use crate::util;
use byteorder::{BigEndian, ByteOrder};
use std::io::{self, BufReader, BufWriter, Read};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// A connection to an sccache server.
pub struct ServerConnection {
    /// A reader for the socket connected to the server.
    reader: BufReader<Box<dyn Connection>>,
    /// A writer for the socket connected to the server.
    writer: BufWriter<Box<dyn Connection>>,
}

impl ServerConnection {
    /// Create a new connection using `stream`.
    pub fn new(conn: Box<dyn Connection>) -> io::Result<ServerConnection> {
        let write_conn = conn.try_clone()?;
        Ok(ServerConnection {
            reader: BufReader::new(conn),
            writer: BufWriter::new(write_conn),
        })
    }

    /// Send `request` to the server, read and return a `Response`.
    pub fn request(&mut self, request: Request) -> Result<Response> {
        trace!("ServerConnection::request");
        util::write_length_prefixed_bincode(&mut self.writer, request)?;
        trace!("ServerConnection::request: sent request");
        self.read_one_response()
    }

    /// Read a single `Response` from the server.
    pub fn read_one_response(&mut self) -> Result<Response> {
        trace!("ServerConnection::read_one_response");
        let mut bytes = [0; 4];
        self.reader
            .read_exact(&mut bytes)
            .context("Failed to read response header")?;
        let len = BigEndian::read_u32(&bytes);
        trace!("Should read {} more bytes", len);
        let mut data = vec![0; len as usize];
        self.reader.read_exact(&mut data)?;
        trace!("Done reading");
        Ok(bincode::deserialize(&data)?)
    }
}

/// Connect and establish daemon readiness within the caller deadline.
/// The returned blocking connection has no timeout on ordinary compilation.
pub fn connect_to_server(
    addr: &crate::net::SocketAddr,
    deadline: Instant,
) -> io::Result<ServerConnection> {
    use crate::net::SocketAddr;

    if deadline <= Instant::now() {
        return Err(io::ErrorKind::TimedOut.into());
    }
    let runtime = util::new_client_runtime()?;
    let connect = async {
        let conn: Box<dyn Connection> = match addr {
            SocketAddr::Net(addr) => {
                let mut stream = tokio::net::TcpStream::connect(addr).await?;
                wait_ready(&mut stream).await?;
                let stream = stream.into_std()?;
                stream.set_nonblocking(false)?;
                Box::new(stream)
            }
            #[cfg(unix)]
            SocketAddr::Unix(path) => {
                let mut stream = tokio::net::UnixStream::connect(path).await?;
                wait_ready(&mut stream).await?;
                let stream = stream.into_std()?;
                stream.set_nonblocking(false)?;
                Box::new(stream)
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            SocketAddr::UnixAbstract(name) => {
                use std::os::unix::ffi::OsStrExt;
                let mut path = vec![0];
                path.extend_from_slice(name);
                let mut stream =
                    tokio::net::UnixStream::connect(std::ffi::OsStr::from_bytes(&path)).await?;
                wait_ready(&mut stream).await?;
                let stream = stream.into_std()?;
                stream.set_nonblocking(false)?;
                Box::new(stream)
            }
        };
        ServerConnection::new(conn)
    };
    runtime.block_on(async {
        tokio::time::timeout_at(deadline.into(), connect)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Timed out waiting for server readiness",
                )
            })?
    })
}

async fn wait_ready<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> io::Result<()> {
    let mut request = Vec::new();
    util::write_length_prefixed_bincode(&mut request, Request::GetStats)
        .map_err(io::Error::other)?;
    stream.write_all(&request).await?;
    // Read exactly one frame; the blocking connection must not lose buffered bytes.
    let len = stream.read_u32().await? as usize;
    let mut response = vec![0; len];
    stream.read_exact(&mut response).await?;
    match bincode::deserialize::<Response>(&response).map_err(io::Error::other)? {
        Response::Stats(_) => (),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unexpected readiness response",
            ));
        }
    }
    Ok(())
}

/// Reconnect after competing daemon bootstraps without resetting the caller budget.
pub fn connect_with_retry(
    addr: &crate::net::SocketAddr,
    deadline: Instant,
) -> io::Result<ServerConnection> {
    loop {
        match connect_to_server(addr, deadline) {
            Ok(conn) => return Ok(conn),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) =>
            {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Timed out reconnecting to server",
                    ));
                }
                std::thread::sleep(remaining.min(Duration::from_millis(500)));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::SocketAddr;
    use crate::server::{ServerInfo, ServerStats};
    use std::io::Write;
    use std::net::TcpListener;

    fn read_request(socket: &mut impl Read) -> Request {
        let mut len = [0; 4];
        socket.read_exact(&mut len).unwrap();
        let mut bytes = vec![0; BigEndian::read_u32(&len) as usize];
        socket.read_exact(&mut bytes).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }

    fn ready_frame() -> Vec<u8> {
        let info = util::new_client_runtime()
            .unwrap()
            .block_on(ServerInfo::new(ServerStats::default(), None))
            .unwrap();
        let mut frame = Vec::new();
        util::write_length_prefixed_bincode(&mut frame, Response::Stats(Box::new(info))).unwrap();
        frame
    }

    #[test]
    fn capability_readiness_deadline_includes_partial_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = SocketAddr::Net(listener.local_addr().unwrap());
        let frame = ready_frame();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            assert!(matches!(read_request(&mut stream), Request::GetStats));
            stream.write_all(&frame[..4]).unwrap();
            // The socket exists and the response has started, but is not ready.
            std::thread::sleep(Duration::from_millis(400));
            let _ = stream.write_all(&frame[4..]);
        });
        let started = Instant::now();
        let result = connect_to_server(&address, started + Duration::from_millis(100));
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
        assert!(started.elapsed() < Duration::from_millis(350));
        worker.join().unwrap();
    }

    #[test]
    fn capability_readiness_deadline_does_not_limit_subsequent_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = SocketAddr::Net(listener.local_addr().unwrap());
        let frame = ready_frame();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            assert!(matches!(read_request(&mut stream), Request::GetStats));
            stream.write_all(&frame).unwrap();
            assert!(matches!(read_request(&mut stream), Request::ZeroStats));
            std::thread::sleep(Duration::from_millis(300));
            util::write_length_prefixed_bincode(&mut stream, Response::ZeroStats).unwrap();
        });
        let mut conn =
            connect_to_server(&address, Instant::now() + Duration::from_millis(100)).unwrap();
        assert!(matches!(
            conn.request(Request::ZeroStats).unwrap(),
            Response::ZeroStats
        ));
        worker.join().unwrap();
    }
}
