#!/bin/bash
# Start the REST gateway in the background, then run the standalone master in the foreground
# so the container lives exactly as long as HBase does.
set -euo pipefail
mkdir -p "$HBASE_LOG_DIR" "$HBASE_PID_DIR"
HBASE_REST_OPTS="-Xmx192m" "$HBASE_HOME/bin/hbase-daemon.sh" --config "$HBASE_CONF_DIR" start rest -p 8080
exec "$HBASE_HOME/bin/hbase" --config "$HBASE_CONF_DIR" master start
