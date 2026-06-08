#!/bin/sh
# Idempotently start the apt-installed postgres cluster for the test suite.
#
# Why: pytest-postgresql's postgresql_proc fixture runs pg_ctl initdb as the
# current user, and pg_ctl refuses to run as root. The GitLab CI tox runner
# is root inside python:*-slim, so we instead use Debian's pg_ctlcluster
# (which internally drops to the postgres user) to start the cluster, then
# point conftest at localhost via POSTGRES_TEST_HOST.
#
# Skips silently when not running as root, or when pg_ctlcluster isn't
# available — local dev is expected to bring its own postgres (docker
# compose or system install) and set POSTGRES_TEST_HOST.

set -e

# Already running? Nothing to do.
if command -v pg_isready >/dev/null 2>&1 && pg_isready -h localhost -q 2>/dev/null; then
    exit 0
fi

# Only root can use pg_ctlcluster; non-root devs must provide their own postgres.
if [ "$(id -u)" != "0" ]; then
    exit 0
fi

# Debian-blessed cluster manager required.
if ! command -v pg_ctlcluster >/dev/null 2>&1; then
    exit 0
fi

PG_VERSION=$(ls /etc/postgresql/ 2>/dev/null | sort -rn | head -1)
if [ -z "$PG_VERSION" ]; then
    echo "start_postgres_for_ci.sh: no cluster found under /etc/postgresql/" >&2
    exit 1
fi

pg_ctlcluster "$PG_VERSION" main start

# Wait for the cluster to accept connections.
for _ in 1 2 3 4 5 6 7 8 9 10; do
    if pg_isready -h localhost -q; then
        break
    fi
    sleep 1
done
pg_isready -h localhost -q

# Set a known password so tests can connect via TCP (Debian default
# requires scram-sha-256 on host entries).
su postgres -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
