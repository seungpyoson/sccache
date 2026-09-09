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

use super::cache_io::*;
#[cfg(feature = "azure")]
use crate::cache::azure::AzureBlobCache;
#[cfg(feature = "cos")]
use crate::cache::cos::COSCache;
use crate::cache::disk::DiskCache;
#[cfg(feature = "gcs")]
use crate::cache::gcs::GCSCache;
#[cfg(feature = "gha")]
use crate::cache::gha::GHACache;
#[cfg(feature = "memcached")]
use crate::cache::memcached::MemcachedCache;
use crate::cache::multilevel::{MultiLevelStats, MultiLevelStorage};
#[cfg(feature = "oss")]
use crate::cache::oss::OSSCache;
#[cfg(feature = "redis")]
use crate::cache::redis::RedisCache;
#[cfg(feature = "s3")]
use crate::cache::s3::S3Cache;
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
use crate::cache::utils::normalize_key;
#[cfg(feature = "webdav")]
use crate::cache::webdav::WebdavCache;
use crate::compiler::PreprocessorCacheEntry;
use crate::config::Config;
use crate::config::{self, CacheType, PreprocessorCacheModeConfig};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::errors::*;

/// Result of [`Storage::get_path`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetPathResult {
    /// Cache hit: the entry lives at this filesystem path.
    Found(PathBuf),
    /// Cache miss: the key is not in the cache.
    Miss,
    /// This backend does not support direct file access; use `get`/`get_raw` instead.
    Unsupported,
}

/// An interface to cache storage.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get a cache entry by `key`.
    ///
    /// If an error occurs, this method should return a `Cache::Error`.
    /// If nothing fails but the entry is not found in the cache,
    /// it should return a `Cache::Miss`.
    /// If the entry is successfully found in the cache, it should
    /// return a `Cache::Hit`.
    async fn get(&self, key: &str) -> Result<Cache>;

    /// Put `entry` in the cache under `key`.
    ///
    /// Returns a `Future` that will provide the result or error when the put is
    /// finished.
    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration>;

    /// Get raw serialized cache entry bytes by `key` (for multi-level backfill).
    /// Returns `None` if the entry is not found, or if the implementation doesn't support raw access.
    /// This is used by multi-level caches to backfill faster levels.
    async fn get_raw(&self, _key: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }

    /// Put raw serialized cache entry bytes under `key` (for multi-level backfill).
    /// Returns an error if the implementation doesn't support raw access.
    /// This is used by multi-level caches to backfill faster levels.
    async fn put_raw(&self, _key: &str, _data: Bytes) -> Result<Duration> {
        Err(anyhow!("put_raw not implemented for this storage backend"))
    }

    /// Check the cache capability.
    ///
    /// - `Ok(CacheMode::ReadOnly)` means cache can only be used to `get`
    ///   cache.
    /// - `Ok(CacheMode::ReadWrite)` means cache can do both `get` and `put`.
    /// - `Err(err)` means cache is not setup correctly or not match with
    ///   users input (for example, user try to use `ReadWrite` but cache
    ///   is `ReadOnly`).
    ///
    /// We will provide a default implementation which returns
    /// `Ok(CacheMode::ReadWrite)` for service that doesn't
    /// support check yet.
    async fn check(&self) -> Result<CacheMode> {
        Ok(CacheMode::ReadWrite)
    }

    /// Get the storage location.
    fn location(&self) -> String;

    /// Get the cache backend type name (e.g., "disk", "redis", "s3").
    /// Used for statistics and display purposes.
    fn cache_type_name(&self) -> &'static str {
        "unknown"
    }

    /// Get the current storage usage, if applicable.
    async fn current_size(&self) -> Result<Option<u64>>;

    /// Get the maximum storage size, if applicable.
    async fn max_size(&self) -> Result<Option<u64>>;

    /// Get multi-level cache statistics, if this is a multi-level storage.
    fn multilevel_stats(&self) -> Option<MultiLevelStats> {
        None
    }

    /// Failed backend read operations, including reads recovered by another level.
    /// This is storage accounting, not a count of recompiled requests.
    fn read_error_count(&self) -> u64 {
        0
    }

    /// Reset completed storage-operation statistics alongside server statistics.
    fn reset_stats(&self) {}

    /// Return the config for preprocessor cache mode if applicable
    fn preprocessor_cache_mode_config(&self) -> PreprocessorCacheModeConfig {
        // Enable by default, only in local mode
        PreprocessorCacheModeConfig::default()
    }
    /// Return the base directories for path normalization if configured
    fn basedirs(&self) -> &[Vec<u8>] {
        &[]
    }
    /// Return the filesystem path of the cached entry for `key`.
    /// Default impl returns [`GetPathResult::Unsupported`].
    async fn get_path(&self, _key: &str) -> Result<GetPathResult> {
        Ok(GetPathResult::Unsupported)
    }

    /// Return the preprocessor cache entry for a given preprocessor key,
    /// if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn get_preprocessor_cache_entry(
        &self,
        _key: &str,
    ) -> Result<Option<Box<dyn crate::lru_disk_cache::ReadSeek>>> {
        Ok(None)
    }
    /// Insert a preprocessor cache entry at the given preprocessor key,
    /// overwriting the entry if it exists.
    /// Only applicable when using preprocessor cache mode.
    async fn put_preprocessor_cache_entry(
        &self,
        _key: &str,
        _preprocessor_cache_entry: PreprocessorCacheEntry,
    ) -> Result<()> {
        Ok(())
    }
}

/// The only provider error data retained by storage consumers. Provider bodies,
/// URLs and nested causes are discarded at the remote adapter boundary.
#[derive(Debug)]
struct RemoteStorageError {
    operation: &'static str,
    kind: String,
}

impl std::fmt::Display for RemoteStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.operation, self.kind)
    }
}

impl std::error::Error for RemoteStorageError {}

/// Wrapper for opendal::Operator that adds basedirs support
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
pub struct RemoteStorage {
    operator: opendal::Operator,
    basedirs: Vec<Vec<u8>>,
    rw_mode: CacheMode,
    /// Successful initialization belongs to this storage instance. Mode queries
    /// must not compete with one another by rewriting the capability probe.
    initialized: tokio::sync::OnceCell<()>,
    /// One marker per storage instance, retained under the normal cache prefix.
    /// The backend's cache lifecycle/eviction owns its retention, as for entries.
    /// Keep the identity across failed or cancelled initialization attempts.
    write_probe: String,
}

#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
impl RemoteStorage {
    pub(crate) fn error(operation: &'static str, error: opendal::Error) -> anyhow::Error {
        RemoteStorageError {
            operation,
            kind: error.kind().to_string(),
        }
        .into()
    }

    pub fn new(operator: opendal::Operator, basedirs: Vec<Vec<u8>>, rw_mode: CacheMode) -> Self {
        Self {
            operator,
            basedirs,
            rw_mode,
            initialized: tokio::sync::OnceCell::new(),
            write_probe: format!(".sccache_check-{}", uuid::Uuid::new_v4()),
        }
    }
}

#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
fn decode_remote_cache_read(result: opendal::Result<opendal::Buffer>) -> Result<Cache> {
    match result {
        Ok(res) => {
            let hit = CacheRead::from(io::Cursor::new(res.to_bytes()))?;
            Ok(Cache::Hit(hit))
        }
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(Cache::Miss),
        Err(e) => Err(RemoteStorage::error("failed to read remote cache entry", e)),
    }
}

/// Return the stable provider-operation message allowed to cross a process
/// boundary. The remote adapter has already discarded provider context.
pub(crate) fn classify_storage_error(operation: &str, error: &anyhow::Error) -> String {
    let kind = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<RemoteStorageError>())
        .map(|cause| &cause.kind);
    match kind {
        Some(kind) => format!("cache storage {operation} failed ({kind})"),
        None => format!("cache storage {operation} failed"),
    }
}

/// Implement storage for operator.
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
#[async_trait]
impl Storage for RemoteStorage {
    async fn get(&self, key: &str) -> Result<Cache> {
        decode_remote_cache_read(self.operator.read(&normalize_key(key)).await)
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        trace!("RemoteStorage::put({})", key);
        // Delegate to put_raw after serializing the entry
        let data = entry.finish()?;
        self.put_raw(key, data.into()).await
    }

    async fn check(&self) -> Result<CacheMode> {
        use opendal::ErrorKind;

        self.initialized
            .get_or_try_init(|| async {
                // Use OpenDAL's bounded retry policy for temporary probe errors.
                // The command's existing startup deadline still bounds enablement.
                // Cache operations retain their original error behavior.
                let operator =
                    self.operator
                        .clone()
                        .layer(opendal::layers::RetryLayer::new().with_notify(
                            |error: &opendal::Error, delay: Duration| {
                                warn!(
                                    "retrying cache capability check ({}) after {delay:?}",
                                    error.kind()
                                );
                            },
                        ));
                let path = ".sccache_check";

                match operator.read(path).await {
                    Ok(_) => (),
                    Err(err) if err.kind() == ErrorKind::NotFound => (),
                    Err(err) => {
                        return Err(Self::error("cache storage failed to read", err));
                    }
                }

                if self.rw_mode == CacheMode::ReadWrite {
                    match operator.write(&self.write_probe, "Hello, World!").await {
                        Ok(_) => (),
                        // Immutable backends acknowledge an existing probe.
                        // This does not report a fresh cache write.
                        Err(err) if err.kind() == ErrorKind::AlreadyExists => (),
                        Err(err) => {
                            return Err(Self::error(
                                "cache storage failed to provide requested write access",
                                err,
                            ));
                        }
                    }
                }
                Ok(())
            })
            .await?;

        debug!("storage check result: {:?}", self.rw_mode);
        Ok(self.rw_mode)
    }

    fn location(&self) -> String {
        let meta = self.operator.info();
        format!(
            "{}, name: {}, prefix: {}",
            meta.scheme(),
            meta.name(),
            meta.root()
        )
    }

    fn cache_type_name(&self) -> &'static str {
        // Use opendal's scheme as the cache type name
        // This returns "s3", "redis", "azure", "gcs", etc.
        self.operator.info().scheme()
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn basedirs(&self) -> &[Vec<u8>] {
        &self.basedirs
    }

    /// Get raw bytes from remote storage for multi-level backfill.
    ///
    /// Unlike `get()` which parses bytes into `CacheRead` (a `ZipArchive<Box<dyn ReadSeek>>`),
    /// this returns the raw bytes without parsing. `CacheRead` is a one-way transformation —
    /// there is no way to extract the original bytes back from the parsed ZIP archive.
    /// For backfill we need the raw bytes to write directly to another cache level.
    async fn get_raw(&self, key: &str) -> Result<Option<Bytes>> {
        trace!("opendal::Operator::get_raw({})", key);
        match self.operator.read(&normalize_key(key)).await {
            Ok(res) => {
                let data = res.to_bytes();
                trace!(
                    "opendal::Operator::get_raw({}): Found {} bytes",
                    key,
                    data.len()
                );
                Ok(Some(data))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                trace!("opendal::Operator::get_raw({}): NotFound", key);
                Ok(None)
            }
            Err(e) => Err(Self::error("failed to read raw cache bytes", e)),
        }
    }

    /// Write raw bytes to remote storage for multi-level backfill.
    ///
    /// Unlike `put()` which takes a `CacheWrite` and serializes it, this writes
    /// pre-serialized bytes directly. Paired with `get_raw()` for efficient
    /// level-to-level data transfer without a deserialize/reserialize round-trip.
    async fn put_raw(&self, key: &str, data: Bytes) -> Result<Duration> {
        trace!("opendal::Operator::put_raw({}, {} bytes)", key, data.len());
        let start = std::time::Instant::now();

        if self.rw_mode == CacheMode::ReadOnly {
            bail!("storage is read-only");
        }

        self.operator
            .write(&normalize_key(key), data)
            .await
            .map_err(|e| Self::error("failed to write remote cache entry", e))?;

        Ok(start.elapsed())
    }
}

/// Build a single cache storage from CacheType
/// Helper function used by storage_from_config for both single and multi-level caches
#[cfg(any(
    feature = "azure",
    feature = "gcs",
    feature = "gha",
    feature = "memcached",
    feature = "redis",
    feature = "s3",
    feature = "webdav",
    feature = "oss",
    feature = "cos"
))]
pub fn build_single_cache(
    cache_type: &CacheType,
    basedirs: &[Vec<u8>],
    _pool: &tokio::runtime::Handle,
) -> Result<Arc<dyn Storage>> {
    match cache_type {
        #[cfg(feature = "azure")]
        CacheType::Azure(config::AzureCacheConfig {
            connection_string,
            container,
            key_prefix,
            rw_mode,
        }) => {
            debug!("Init azure cache with container {container}, key_prefix {key_prefix}");
            let operator = AzureBlobCache::build(connection_string, container, key_prefix)
                .map_err(|err| anyhow!("create azure cache failed: {err:?}"))?;
            let storage = RemoteStorage::new(operator, basedirs.to_vec(), (*rw_mode).into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "gcs")]
        CacheType::GCS(config::GCSCacheConfig {
            bucket,
            key_prefix,
            cred_path,
            rw_mode,
            service_account,
            credential_url,
        }) => {
            debug!("Init gcs cache with bucket {bucket}, key_prefix {key_prefix}");

            let operator = GCSCache::build(
                bucket,
                key_prefix,
                cred_path.as_deref(),
                service_account.as_deref(),
                (*rw_mode).into(),
                credential_url.as_deref(),
            )
            .map_err(|err| anyhow!("create gcs cache failed: {err:?}"))?;
            let storage = RemoteStorage::new(operator, basedirs.to_vec(), (*rw_mode).into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "gha")]
        CacheType::GHA(config::GHACacheConfig {
            version, rw_mode, ..
        }) => {
            debug!("Init gha cache with version {version}");

            let operator = GHACache::build(version)
                .map_err(|err| anyhow!("create gha cache failed: {err:?}"))?;
            let storage = RemoteStorage::new(operator, basedirs.to_vec(), (*rw_mode).into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "memcached")]
        CacheType::Memcached(config::MemcachedCacheConfig {
            url,
            username,
            password,
            expiration,
            key_prefix,
            rw_mode,
        }) => {
            debug!("Init memcached cache");

            let operator = MemcachedCache::build(
                url,
                username.as_deref(),
                password.as_deref(),
                key_prefix,
                *expiration,
            )
            .map_err(|err| anyhow!("create memcached cache failed: {err:?}"))?;
            let storage = RemoteStorage::new(operator, basedirs.to_vec(), (*rw_mode).into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "redis")]
        CacheType::Redis(config::RedisCacheConfig {
            endpoint,
            cluster_endpoints,
            username,
            password,
            db,
            url,
            ttl,
            key_prefix,
            rw_mode,
        }) => {
            let storage = match (endpoint, cluster_endpoints, url) {
                (Some(url), None, None) => {
                    debug!("Init redis single-node cache");
                    RedisCache::build_single(
                        url,
                        username.as_deref(),
                        password.as_deref(),
                        *db,
                        key_prefix,
                        *ttl,
                    )
                }
                (None, Some(urls), None) => {
                    debug!("Init redis cluster cache");
                    RedisCache::build_cluster(
                        urls,
                        username.as_deref(),
                        password.as_deref(),
                        *db,
                        key_prefix,
                        *ttl,
                    )
                }
                (None, None, Some(url)) => {
                    warn!("Init redis single-node cache from deprecated URL API");
                    if username.is_some() || password.is_some() || *db != crate::config::DEFAULT_REDIS_DB {
                        bail!("`username`, `password` and `db` has no effect when `url` is set. Please use `endpoint` or `cluster_endpoints` for new API accessing");
                    }

                    RedisCache::build_from_url(url, key_prefix, *ttl)
                }
                _ => bail!("Only one of `endpoint`, `cluster_endpoints`, `url` must be set"),
            }
            .map_err(|err| anyhow!("create redis cache failed: {err:?}"))?;
            let storage = RemoteStorage::new(storage, basedirs.to_vec(), (*rw_mode).into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "s3")]
        CacheType::S3(c) => {
            debug!("Init s3 cache");
            let storage_builder =
                S3Cache::new(c.bucket.clone(), c.key_prefix.clone(), c.no_credentials);
            let operator = storage_builder
                .with_region(c.region.clone())
                .with_endpoint(c.endpoint.clone())
                .with_use_ssl(c.use_ssl)
                .with_server_side_encryption(c.server_side_encryption)
                .with_enable_virtual_host_style(c.enable_virtual_host_style)
                .build()
                .map_err(|err| anyhow!("create s3 cache failed: {err:?}"))?;

            let storage = RemoteStorage::new(operator, basedirs.to_vec(), c.rw_mode.into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "webdav")]
        CacheType::Webdav(c) => {
            debug!("Init webdav cache");

            let operator = WebdavCache::build(
                &c.endpoint,
                &c.key_prefix,
                c.username.as_deref(),
                c.password.as_deref(),
                c.token.as_deref(),
            )
            .map_err(|err| anyhow!("create webdav cache failed: {err:?}"))?;

            let storage = RemoteStorage::new(operator, basedirs.to_vec(), c.rw_mode.into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "oss")]
        CacheType::OSS(c) => {
            debug!("Init oss cache");

            let operator = OSSCache::build(
                &c.bucket,
                &c.key_prefix,
                c.endpoint.as_deref(),
                c.no_credentials,
            )
            .map_err(|err| anyhow!("create oss cache failed: {err:?}"))?;

            let storage = RemoteStorage::new(operator, basedirs.to_vec(), c.rw_mode.into());
            Ok(Arc::new(storage))
        }
        #[cfg(feature = "cos")]
        CacheType::COS(c) => {
            debug!("Init cos cache");

            let operator = COSCache::build(&c.bucket, &c.key_prefix, c.endpoint.as_deref())
                .map_err(|err| anyhow!("create cos cache failed: {err:?}"))?;

            let storage = RemoteStorage::new(operator, basedirs.to_vec(), c.rw_mode.into());
            Ok(Arc::new(storage))
        }
        #[allow(unreachable_patterns)]
        _ => {
            bail!("Cache type not supported with current feature configuration")
        }
    }
}

/// Get a suitable `Storage` implementation from configuration.
/// Supports both single-cache (backward compatible) and multi-level cache configurations.
pub fn storage_from_config(
    config: &Config,
    pool: &tokio::runtime::Handle,
) -> Result<Arc<dyn Storage>> {
    // Check for multi-level cache configuration
    if let Some(multilevel) = MultiLevelStorage::from_config(config, pool)? {
        return Ok(Arc::new(multilevel));
    }

    // Single cache or fallback to disk (backward compatible path)
    #[cfg(any(
        feature = "azure",
        feature = "gcs",
        feature = "gha",
        feature = "memcached",
        feature = "redis",
        feature = "s3",
        feature = "webdav",
        feature = "oss",
        feature = "cos"
    ))]
    if let Some(cache_type) = &config.cache {
        debug!("Configuring single cache from CacheType");
        return build_single_cache(cache_type, &config.basedirs, pool);
    }

    // No remote cache configured - use disk cache only
    let (dir, size) = (&config.fallback_cache.dir, config.fallback_cache.size);
    let preprocessor_cache_mode_config = config.fallback_cache.preprocessor_cache_mode;
    let rw_mode = config.fallback_cache.rw_mode.into();
    debug!("Init disk cache with dir {:?}, size {}", dir, size);
    Ok(Arc::new(DiskCache::new(
        dir,
        size,
        pool,
        preprocessor_cache_mode_config,
        rw_mode,
        config.basedirs.clone(),
    )))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::CacheModeConfig;
    use fs_err as fs;

    #[cfg(feature = "s3")]
    mod remote_storage {
        use super::*;
        use http::{Request, Response, StatusCode};
        use opendal::layers::HttpClientLayer;
        use opendal::raw::{HttpBody, HttpClient, HttpFetch, Operation};
        use opendal::{Error, ErrorKind};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct ProbeHttp {
            read_status: StatusCode,
            write_status: StatusCode,
            initial_write_statuses: Arc<std::sync::Mutex<std::collections::VecDeque<StatusCode>>>,
            reads: Arc<AtomicUsize>,
            writes: Arc<AtomicUsize>,
            write_paths: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl HttpFetch for ProbeHttp {
            async fn fetch(
                &self,
                request: Request<opendal::Buffer>,
            ) -> opendal::Result<Response<HttpBody>> {
                tokio::task::yield_now().await;
                let status = match request.extensions().get::<Operation>() {
                    Some(Operation::Read) => {
                        self.reads.fetch_add(1, Ordering::SeqCst);
                        self.read_status
                    }
                    Some(Operation::Write) => {
                        self.writes.fetch_add(1, Ordering::SeqCst);
                        self.write_paths
                            .lock()
                            .unwrap()
                            .push(request.uri().path().to_owned());
                        self.initial_write_statuses
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or(self.write_status)
                    }
                    _ => panic!("unexpected probe request: {}", request.method()),
                };
                Ok(Response::builder()
                    .status(status)
                    .header("Content-Length", "0")
                    .body(HttpBody::new(futures::stream::empty(), Some(0)))
                    .unwrap())
            }
        }

        fn probe_storage(
            mode: CacheMode,
            read_status: StatusCode,
            write_status: StatusCode,
        ) -> (RemoteStorage, ProbeHttp) {
            let http = ProbeHttp {
                read_status,
                write_status,
                initial_write_statuses: Arc::default(),
                reads: Arc::default(),
                writes: Arc::default(),
                write_paths: Arc::default(),
            };
            let operator = opendal::Operator::new(
                opendal::services::S3::default()
                    .bucket("probe-test")
                    .root("cache/")
                    .region("auto")
                    .endpoint("http://s3.invalid")
                    .disable_config_load()
                    .disable_ec2_metadata()
                    .allow_anonymous(),
            )
            .unwrap()
            .layer(HttpClientLayer::new(HttpClient::with(http.clone())))
            .finish();
            (RemoteStorage::new(operator, vec![], mode), http)
        }

        #[tokio::test]
        async fn capability_check_rejects_failed_required_reads() {
            for mode in [CacheMode::ReadOnly, CacheMode::ReadWrite] {
                for status in [
                    StatusCode::FORBIDDEN,
                    StatusCode::TOO_MANY_REQUESTS,
                    StatusCode::SERVICE_UNAVAILABLE,
                ] {
                    let (storage, http) = probe_storage(mode, status, StatusCode::OK);
                    let error = storage.check().await.unwrap_err();
                    assert!(error.is::<RemoteStorageError>(), "{mode:?}: {status}");
                    if status == StatusCode::FORBIDDEN {
                        assert_eq!(
                            error.to_string(),
                            "cache storage failed to read (PermissionDenied)"
                        );
                    }
                    assert_eq!(http.writes.load(Ordering::SeqCst), 0);
                }
            }
        }

        #[tokio::test]
        async fn capability_check_rejects_failed_required_writes() {
            for status in [
                StatusCode::FORBIDDEN,
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::SERVICE_UNAVAILABLE,
            ] {
                let (storage, http) =
                    probe_storage(CacheMode::ReadWrite, StatusCode::NOT_FOUND, status);
                let error = storage.check().await.unwrap_err();
                assert!(error.is::<RemoteStorageError>(), "{status}");
                assert!(storage.initialized.get().is_none());
                if status == StatusCode::FORBIDDEN {
                    assert_eq!(
                        error.to_string(),
                        "cache storage failed to provide requested write access (PermissionDenied)"
                    );
                    assert_eq!(http.writes.load(Ordering::SeqCst), 1);
                }
            }
        }

        #[tokio::test]
        async fn capability_check_preserves_explicit_read_only_mode() {
            for status in [StatusCode::OK, StatusCode::NOT_FOUND] {
                let (storage, http) =
                    probe_storage(CacheMode::ReadOnly, status, StatusCode::FORBIDDEN);
                assert_eq!(storage.check().await.unwrap(), CacheMode::ReadOnly);
                assert_eq!(http.writes.load(Ordering::SeqCst), 0);
            }
        }

        #[tokio::test]
        async fn capability_check_preserves_requested_read_write_mode() {
            for status in [StatusCode::OK, StatusCode::NOT_FOUND] {
                let (storage, http) = probe_storage(CacheMode::ReadWrite, status, StatusCode::OK);
                assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
                assert_eq!(http.writes.load(Ordering::SeqCst), 1);
            }
        }

        #[tokio::test]
        async fn capability_check_initializes_once_for_concurrent_callers() {
            let (storage, http) =
                probe_storage(CacheMode::ReadWrite, StatusCode::NOT_FOUND, StatusCode::OK);
            let results = futures::future::join_all((0..8).map(|_| storage.check())).await;
            assert!(
                results
                    .into_iter()
                    .all(|mode| mode.unwrap() == CacheMode::ReadWrite)
            );
            assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
            assert_eq!(http.reads.load(Ordering::SeqCst), 1);
            assert_eq!(http.writes.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn capability_independent_storage_instances_use_distinct_probe_keys() {
            let (storage, http) =
                probe_storage(CacheMode::ReadWrite, StatusCode::NOT_FOUND, StatusCode::OK);
            let instances: Vec<_> = (0..8)
                .map(|_| RemoteStorage::new(storage.operator.clone(), vec![], CacheMode::ReadWrite))
                .collect();
            for result in
                futures::future::join_all(instances.iter().map(|storage| storage.check())).await
            {
                assert_eq!(result.unwrap(), CacheMode::ReadWrite);
            }
            let paths = http.write_paths.lock().unwrap();
            assert_eq!(paths.len(), 8);
            assert_eq!(
                paths.iter().collect::<std::collections::HashSet<_>>().len(),
                8
            );
            assert!(
                paths
                    .iter()
                    .all(|path| path.starts_with("/probe-test/cache/"))
            );
        }

        #[tokio::test]
        async fn capability_failed_checks_reuse_the_owned_probe_identity() {
            let (storage, http) =
                probe_storage(CacheMode::ReadWrite, StatusCode::NOT_FOUND, StatusCode::OK);
            http.initial_write_statuses
                .lock()
                .unwrap()
                .push_back(StatusCode::FORBIDDEN);
            let error = storage.check().await.unwrap_err();
            assert!(error.to_string().contains("PermissionDenied"));
            assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
            let paths = http.write_paths.lock().unwrap();
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0], paths[1]);
        }

        #[tokio::test]
        async fn capability_check_recovers_from_temporary_initial_write_contention() {
            let (storage, http) =
                probe_storage(CacheMode::ReadWrite, StatusCode::NOT_FOUND, StatusCode::OK);
            http.initial_write_statuses
                .lock()
                .unwrap()
                .push_back(StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
            assert_eq!(http.writes.load(Ordering::SeqCst), 2);
            assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
            assert_eq!(http.writes.load(Ordering::SeqCst), 2);
        }

        #[cfg(feature = "gha")]
        #[tokio::test]
        async fn capability_check_accepts_an_existing_immutable_probe() {
            let http = ProbeHttp {
                read_status: StatusCode::NOT_FOUND,
                write_status: StatusCode::CONFLICT,
                initial_write_statuses: Arc::default(),
                reads: Arc::default(),
                writes: Arc::default(),
                write_paths: Arc::default(),
            };
            let operator = opendal::Operator::new(
                opendal::services::Ghac::default()
                    .root("cache/")
                    .endpoint("http://ghac.invalid/")
                    .runtime_token("synthetic-test-token"),
            )
            .unwrap()
            .layer(HttpClientLayer::new(HttpClient::with(http.clone())))
            .finish();
            let storage = RemoteStorage::new(operator, vec![], CacheMode::ReadWrite);
            assert_eq!(storage.check().await.unwrap(), CacheMode::ReadWrite);
            assert_eq!(http.writes.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn get_propagates_unexpected_backend_errors() {
            let error = decode_remote_cache_read(Err(Error::new(
                ErrorKind::Unexpected,
                "injected read failure",
            )))
            .unwrap_err();

            assert_eq!(
                error
                    .downcast_ref::<RemoteStorageError>()
                    .map(|e| e.kind.as_str()),
                Some("Unexpected")
            );
        }

        #[test]
        fn capability_error_summary_excludes_provider_context() {
            let provider = Error::new(ErrorKind::PermissionDenied, "synthetic-provider-detail");
            let error = RemoteStorage::error("failed to read raw cache bytes", provider);
            let summary = super::super::classify_storage_error("read raw entry", &error);
            assert_eq!(
                summary,
                "cache storage read raw entry failed (PermissionDenied)"
            );
            assert!(!summary.contains("synthetic-provider-detail"));
            assert_eq!(error.chain().count(), 1);
            assert!(!format!("{error:?}").contains("synthetic-provider-detail"));
        }
    }

    #[test]
    fn test_read_write_mode_local() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();

        // Use disk cache.
        let mut config = Config {
            cache: None,
            ..Default::default()
        };

        let tempdir = tempfile::Builder::new()
            .prefix("sccache_test_rust_cargo")
            .tempdir()
            .context("Failed to create tempdir")
            .unwrap();
        let cache_dir = tempdir.path().join("cache");
        fs::create_dir(&cache_dir).unwrap();

        config.fallback_cache.dir = cache_dir;

        // Test Read Write
        config.fallback_cache.rw_mode = CacheModeConfig::ReadWrite;

        {
            let cache = storage_from_config(&config, runtime.handle()).unwrap();

            runtime.block_on(async move {
                cache.put("test1", CacheWrite::default()).await.unwrap();
                cache
                    .put_preprocessor_cache_entry("test1", PreprocessorCacheEntry::default())
                    .await
                    .unwrap();
            });
        }

        // Test Read-only
        config.fallback_cache.rw_mode = CacheModeConfig::ReadOnly;

        {
            let cache = storage_from_config(&config, runtime.handle()).unwrap();

            runtime.block_on(async move {
                assert_eq!(
                    cache
                        .put("test1", CacheWrite::default())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to a read-only cache"
                );
                assert_eq!(
                    cache
                        .put_preprocessor_cache_entry("test1", PreprocessorCacheEntry::default())
                        .await
                        .unwrap_err()
                        .to_string(),
                    "Cannot write to a read-only cache"
                );
            });
        }
    }

    #[test]
    #[cfg(feature = "s3")]
    fn test_operator_storage_s3_with_basedirs() {
        // Create S3 operator (doesn't need real credentials for this test)
        let operator = crate::cache::s3::S3Cache::new(
            "test-bucket".to_string(),
            "test-prefix".to_string(),
            true, // no_credentials = true
        )
        .with_region(Some("us-east-1".to_string()))
        .build()
        .expect("Failed to create S3 cache operator");

        let basedirs = vec![b"/home/user/project".to_vec(), b"/opt/build".to_vec()];

        // Wrap with OperatorStorage
        let storage = RemoteStorage::new(operator, basedirs.clone(), CacheMode::ReadWrite);

        // Verify basedirs are stored and retrieved correctly
        assert_eq!(storage.basedirs(), basedirs.as_slice());
        assert_eq!(storage.basedirs().len(), 2);
        assert_eq!(storage.basedirs()[0], b"/home/user/project".to_vec());
        assert_eq!(storage.basedirs()[1], b"/opt/build".to_vec());
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_operator_storage_redis_with_basedirs() {
        // Create Redis operator
        let operator = crate::cache::redis::RedisCache::build_single(
            "redis://localhost:6379",
            None,
            None,
            0,
            "test-prefix",
            0,
        )
        .expect("Failed to create Redis cache operator");

        let basedirs = vec![b"/workspace".to_vec()];

        // Wrap with OperatorStorage
        let storage = RemoteStorage::new(operator, basedirs.clone(), CacheMode::ReadWrite);

        // Verify basedirs work
        assert_eq!(storage.basedirs(), basedirs.as_slice());
        assert_eq!(storage.basedirs().len(), 1);
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_operator_storage_redis_with_read_only() {
        // Create Redis operator

        use crate::test::utils::Waiter;
        let operator = crate::cache::redis::RedisCache::build_single(
            "redis://localhost:6379",
            None,
            None,
            0,
            "test-prefix",
            0,
        )
        .expect("Failed to create Redis cache operator");

        // Wrap with OperatorStorage
        let storage = RemoteStorage::new(operator, vec![], CacheMode::ReadOnly);

        // Verify put fails
        let result = storage.put("test", CacheWrite::default()).wait();
        match result {
            Ok(_) => panic!("expected error, got success {result:?}"),
            Err(err) => assert_eq!(err.to_string(), "storage is read-only"),
        }
    }
}
