With `music.download.storage.cache_bucket_name` set (#1188) the downloader uploads to `queue/<file>` instead of `cache/<file>`. The cache bucket is never used by this pod. Without it nothing changes.
