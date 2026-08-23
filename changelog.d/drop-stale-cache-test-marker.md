### Changed

- **Removed a stale test marker that was the entire contents of `discord_bot/__init__.py`.** The two comment lines were added by !68 in June 2026 to generate a source change that would exercise the then-new buildkit S3 layer cache, and were meant to be reverted before that MR merged. They weren't, and have sat on `main` since. No behaviour change — the file stays as the package marker, now empty.
