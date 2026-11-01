`ready_remove` accepts `evict_all`, which flags every video-cache row for deletion regardless of the count and size limits. It is the db side of `discord-broker --flush-cache`.
