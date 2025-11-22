use crate::cached_object_store::stats::CachedObjectStoreStats;
use crate::cached_object_store::storage::{
    LocalCacheHead, ObjectStoreCacheKey, ObjectStoreCacheValue, PartID,
};
use crate::error::SlateDBError;
use crate::utils::build_concurrent;
use bytes::{Bytes, BytesMut};
use foyer::HybridCache;
use futures::{future::BoxFuture, stream, stream::BoxStream, StreamExt};
use log::warn;
use object_store::{path::Path, GetOptions, GetResult, ObjectMeta, ObjectStore};
use object_store::{Attributes, GetRange, GetResultPayload, PutMultipartOptions, PutResult};
use object_store::{ListResult, MultipartUpload, PutOptions, PutPayload};
use std::{ops::Range, sync::Arc};

#[derive(Clone)]
pub(crate) struct CachedObjectStore {
    object_store: Arc<dyn ObjectStore>,
    pub(crate) part_size_bytes: usize,
    cache: Option<HybridCache<ObjectStoreCacheKey, ObjectStoreCacheValue>>,
    pub(crate) cache_puts: bool,
    stats: Arc<CachedObjectStoreStats>,
}

impl std::fmt::Debug for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedObjectStore")
            .field("part_size_bytes", &self.part_size_bytes)
            .field("has_cache", &self.cache.is_some())
            .field("cache_puts", &self.cache_puts)
            .finish()
    }
}

impl CachedObjectStore {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        cache: Option<HybridCache<ObjectStoreCacheKey, ObjectStoreCacheValue>>,
        part_size_bytes: usize,
        cache_puts: bool,
        stats: Arc<CachedObjectStoreStats>,
    ) -> Result<Arc<Self>, SlateDBError> {
        if part_size_bytes == 0 || !part_size_bytes.is_multiple_of(1024) {
            return Err(SlateDBError::InvalidCachePartSize);
        }

        Ok(Arc::new(Self {
            object_store,
            part_size_bytes,
            cache,
            stats,
            cache_puts,
        }))
    }

    /// Load files into cache up to a maximum number of bytes.
    /// This method fetches objects from the provided paths and stores them in the cache
    /// until the specified max_bytes limit is reached.
    pub async fn load_files_to_cache(
        &self,
        file_paths: Vec<Path>,
        max_bytes: usize,
    ) -> Result<(), SlateDBError> {
        if file_paths.is_empty() || max_bytes == 0 {
            return Ok(());
        }

        let mut remaining_bytes = max_bytes;
        let mut files_to_load = Vec::with_capacity(file_paths.len());

        // First pass: sequentially get metadata and select files that fit
        // This is done sequentially because the head calls should be very quick compared to files loading
        for path in file_paths {
            match self.object_store.head(&path).await {
                Ok(meta) => {
                    let file_size = meta.size as usize;
                    if remaining_bytes >= file_size {
                        remaining_bytes -= file_size;
                        files_to_load.push(path);
                    } else {
                        // We can't fit this file, so we stop here
                        break;
                    }
                }
                Err(e) => {
                    // If file doesn't exist or can't be accessed, we stop here
                    warn!("Failed to preload all SSTs to cache: {:?}", e);
                    break;
                }
            }
        }

        // Second pass: load the selected files in bouded parallelism and cache them.
        let degree_of_parallelism = 5;
        let _result = build_concurrent(files_to_load.into_iter(), degree_of_parallelism, |path| {
            let this = self.clone();
            async move {
                match this.cached_get_opts(&path, GetOptions::default()).await {
                    Ok(result) => {
                        // trigger caching
                        let _ = result.bytes().await;
                        Ok(Some(())) // success → Some
                    }
                    Err(e) => {
                        warn!(
                            "Failed to prefetch file into cache [path={}, error={:?}]",
                            path, e
                        );
                        Ok(None) // best-effort: skip errors
                    }
                }
            }
        })
        .await;

        Ok(())
    }

    pub async fn cached_head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        if let Some(cache) = &self.cache {
            let key = ObjectStoreCacheKey::Head {
                location: location.to_string(),
            };
            if let Ok(Some(entry)) = cache.get(&key).await {
                if let ObjectStoreCacheValue::Head(head) = entry.value() {
                    return Ok(head.meta());
                }
            }
        }

        let result = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: None,
                    head: true,
                    ..Default::default()
                },
            )
            .await?;
        let meta = result.meta.clone();
        self.save_get_result(result).await.ok();
        Ok(meta)
    }

    pub async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let get_range = opts.range.clone();
        let (meta, attributes) = self.maybe_prefetch_range(location, opts).await?;
        let range = self.canonicalize_range(get_range, meta.size)?;
        let parts = self.split_range_into_parts(range.clone());

        // read parts, and concatenate them into a single stream. please note that some of these part may not be cached,
        // we'll still fallback to the object store to get the missing parts.
        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| self.read_part(location, part_id, range_in_part))
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    async fn cached_put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        if !self.cache_puts {
            return self.object_store.put_opts(location, payload, opts).await;
        }

        let result = self
            .object_store
            .put_opts(location, payload.clone(), opts)
            .await?;

        match self.cached_head(location).await {
            Ok(_) => {
                let stream =
                    stream::iter(payload.into_iter()).map(Ok::<Bytes, object_store::Error>);
                self.save_parts_stream(location, stream, 0).await.ok();
            }
            Err(e) => {
                warn!(
                    "failed to save head to disk cache [location={}, error={:?}]",
                    location, e
                );
            }
        }

        Ok(result)
    }

    // if an object is not cached before, maybe_prefetch_range will try to prefetch the object from the
    // object store and save the parts into the local disk cache. the prefetching is helpful to reduce the
    // number of GET requests to the object store, it'll try to aggregate the parts among the range into a
    // single GET request, and save the related parts into local disks together.
    // when it sends GET requests to the object store, the range is expected to be ALIGNED with the part
    // size.
    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        mut opts: GetOptions,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        if let Some(cache) = &self.cache {
            let key = ObjectStoreCacheKey::Head {
                location: location.to_string(),
            };
            if let Ok(Some(entry)) = cache.get(&key).await {
                if let ObjectStoreCacheValue::Head(head) = entry.value() {
                    return Ok((head.meta(), head.attributes()));
                }
            }
        }

        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range));
        }

        let get_result = self.object_store.get_opts(location, opts).await?;
        let result_meta = get_result.meta.clone();
        let result_attrs = get_result.attributes.clone();
        self.save_get_result(get_result).await.ok();
        Ok((result_meta, result_attrs))
    }

    /// save the GetResult to the disk cache, a GetResult may be transformed into multiple part
    /// files and a meta file. please note that the `range` in the GetResult is expected to be
    /// aligned with the part size.
    async fn save_get_result(&self, result: GetResult) -> object_store::Result<u64> {
        let part_size_bytes_u64 = self.part_size_bytes as u64;
        assert!(result.range.start.is_multiple_of(part_size_bytes_u64));
        assert!(
            result.range.end.is_multiple_of(part_size_bytes_u64)
                || result.range.end == result.meta.size
        );

        let location = result.meta.location.clone();
        let object_size = result.meta.size;

        if let Some(cache) = &self.cache {
            let head: LocalCacheHead = (&result.meta, &result.attributes).into();
            let key = ObjectStoreCacheKey::Head {
                location: location.to_string(),
            };
            cache.insert(key, ObjectStoreCacheValue::Head(head));
        }

        let start_part_number = usize::try_from(result.range.start / part_size_bytes_u64)
            .expect("Part number exceeds u32 on a 32-bit system. Try increasing part size.");

        let stream = result.into_stream();

        self.save_parts_stream(&location, stream, start_part_number)
            .await?;

        Ok(object_size)
    }

    async fn save_parts_stream<S>(
        &self,
        location: &Path,
        mut stream: S,
        start_part_number: usize,
    ) -> object_store::Result<usize>
    where
        S: stream::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    {
        let mut buffer = BytesMut::new();
        let mut part_number = start_part_number;
        let mut total_bytes: usize = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            total_bytes += chunk.len();
            buffer.extend_from_slice(&chunk);

            while buffer.len() >= self.part_size_bytes {
                let to_write = buffer.split_to(self.part_size_bytes);
                self.save_part(location, part_number, to_write.into())
                    .await?;
                part_number += 1;
            }
        }

        if !buffer.is_empty() {
            self.save_part(location, part_number, buffer.into()).await?;
        }

        Ok(total_bytes)
    }

    async fn save_part(
        &self,
        location: &Path,
        part_id: PartID,
        bytes: Bytes,
    ) -> object_store::Result<()> {
        if let Some(cache) = &self.cache {
            let key = ObjectStoreCacheKey::Part {
                location: location.to_string(),
                part_id,
            };
            cache.insert(key, ObjectStoreCacheValue::Part(bytes));
        }
        Ok(())
    }

    // split the range into parts, and return the part id and the range inside the part.
    fn split_range_into_parts(&self, range: Range<u64>) -> Vec<(PartID, Range<usize>)> {
        let part_size_bytes_u64 = self.part_size_bytes as u64;
        let range_aligned = self.align_range(&range, self.part_size_bytes);
        let start_part = range_aligned.start / part_size_bytes_u64;
        let end_part = range_aligned.end / part_size_bytes_u64;
        let mut parts: Vec<_> = (start_part..end_part)
            .map(|part_id| {
                (
                    usize::try_from(part_id).expect("Number of parts exceeds usize"),
                    Range {
                        start: 0,
                        end: self.part_size_bytes,
                    },
                )
            })
            .collect();
        if parts.is_empty() {
            return vec![];
        }
        if let Some(first_part) = parts.first_mut() {
            first_part.1.start = usize::try_from(range.start % part_size_bytes_u64)
                .expect("Part size is too large to fit in a usize");
        }
        if let Some(last_part) = parts.last_mut() {
            if !range.end.is_multiple_of(part_size_bytes_u64) {
                last_part.1.end = usize::try_from(range.end % part_size_bytes_u64)
                    .expect("Part size is too large to fit in a usize");
            }
        }
        parts
    }

    /// get from disk if the parts are cached, otherwise start a new GET request.
    /// the io errors on reading the disk caches will be ignored, just fallback to
    /// the object store.
    fn read_part(
        &self,
        location: &Path,
        part_id: PartID,
        range_in_part: Range<usize>,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let part_size = self.part_size_bytes;
        let object_store = self.object_store.clone();
        let location = location.clone();
        let cache = self.cache.clone();
        let db_stats = self.stats.clone();
        Box::pin(async move {
            db_stats.object_store_cache_part_access.inc();

            let cache_key = ObjectStoreCacheKey::Part {
                location: location.to_string(),
                part_id,
            };

            if let Some(cache) = &cache {
                if let Ok(Some(entry)) = cache.get(&cache_key).await {
                    if let ObjectStoreCacheValue::Part(bytes) = entry.value() {
                        db_stats.object_store_cache_part_hits.inc();
                        let slice = bytes.slice(range_in_part);
                        return Ok(slice);
                    }
                }
            }

            let range = Range {
                start: (part_id * part_size) as u64,
                end: ((part_id + 1) * part_size) as u64,
            };
            let get_result = object_store
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(range)),
                        ..Default::default()
                    },
                )
                .await?;

            let meta = get_result.meta.clone();
            let attrs = get_result.attributes.clone();
            let bytes = get_result.bytes().await?;

            if let Some(cache) = &cache {
                let head: LocalCacheHead = (&meta, &attrs).into();
                let head_key = ObjectStoreCacheKey::Head {
                    location: location.to_string(),
                };
                cache.insert(head_key, ObjectStoreCacheValue::Head(head));
                cache.insert(cache_key, ObjectStoreCacheValue::Part(bytes.clone()));
            }

            Ok(Bytes::copy_from_slice(&bytes.slice(range_in_part)))
        })
    }

    // given the range and object size, return the canonicalized `Range<usize>` with concrete start and
    // end.
    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        object_size: u64,
    ) -> object_store::Result<Range<u64>> {
        let (start_offset, end_offset) = match range {
            None => (0, object_size),
            Some(range) => match range {
                GetRange::Bounded(range) => {
                    if range.start >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: range.start,
                                length: object_size,
                            }),
                        });
                    }
                    if range.start >= range.end {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::Inconsistent {
                                start: range.start,
                                end: range.end,
                            }),
                        });
                    }
                    (range.start, range.end.min(object_size))
                }
                GetRange::Offset(offset) => {
                    if offset >= object_size {
                        return Err(object_store::Error::Generic {
                            store: "cached_object_store",
                            source: Box::new(InvalidGetRange::StartTooLarge {
                                requested: offset,
                                length: object_size,
                            }),
                        });
                    }
                    (offset, object_size)
                }
                GetRange::Suffix(suffix) => (object_size.saturating_sub(suffix), object_size),
            },
        };
        Ok(Range {
            start: start_offset,
            end: end_offset,
        })
    }

    fn align_get_range(&self, range: &GetRange) -> GetRange {
        match range {
            GetRange::Bounded(bounded) => {
                let aligned = self.align_range(bounded, self.part_size_bytes);
                GetRange::Bounded(aligned)
            }
            GetRange::Suffix(suffix) => {
                let suffix_aligned = self.align_range(&(0..*suffix), self.part_size_bytes).end;
                GetRange::Suffix(suffix_aligned)
            }
            GetRange::Offset(offset) => {
                let offset_aligned = *offset - *offset % self.part_size_bytes as u64;
                GetRange::Offset(offset_aligned)
            }
        }
    }

    fn align_range(&self, range: &Range<u64>, alignment: usize) -> Range<u64> {
        let alignment = alignment as u64;
        let start_aligned = range.start - range.start % alignment;
        let end_aligned = range.end.div_ceil(alignment) * alignment;
        Range {
            start: start_aligned,
            end: end_aligned,
        }
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachedObjectStore({})", self.object_store)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.cached_get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.cached_head(location).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.cached_put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        // TODO: handle cache eviction
        self.object_store.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.object_store.rename_if_not_exists(from, to).await
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidGetRange {
    #[error("Range start too large, requested: {requested}, length: {length}")]
    StartTooLarge { requested: u64, length: u64 },

    #[error("Range started at {start} and ended at {end}")]
    Inconsistent { start: u64, end: u64 },
}
