# Apache HBase 2.6.7 standalone (HMaster + RegionServer + ZooKeeper in one JVM) + REST gateway.
# Built from the official Eclipse Temurin 17 image and the official Apache release,
# verified against the publisher's SHA-512 before extraction.
FROM eclipse-temurin:17-jre-noble

ARG HBASE_VERSION=2.6.7
ARG HBASE_TGZ=hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz
ARG HBASE_URL=https://archive.apache.org/dist/hbase/${HBASE_VERSION}/${HBASE_TGZ}

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /tmp/hbase.tgz "${HBASE_URL}" \
 && curl -fsSL -o /tmp/hbase.sha512 "${HBASE_URL}.sha512" \
 && expected=$(cut -d: -f2 /tmp/hbase.sha512 | tr -d ' \n' | tr 'A-F' 'a-f') \
 && actual=$(sha512sum /tmp/hbase.tgz | cut -d' ' -f1) \
 && [ "$expected" = "$actual" ] || { echo "SHA-512 mismatch for HBase"; exit 1; } \
 && mkdir -p /opt/hbase && tar -xzf /tmp/hbase.tgz -C /opt/hbase --strip-components=1 \
 && rm /tmp/hbase.tgz /tmp/hbase.sha512 \
 && useradd --system --home /data hbase && mkdir -p /data && chown -R hbase /data /opt/hbase/logs 2>/dev/null || true

COPY hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY entrypoint.sh /entrypoint.sh
ENV HBASE_HOME=/opt/hbase HBASE_CONF_DIR=/opt/hbase/conf HBASE_LOG_DIR=/data/logs \
    HBASE_PID_DIR=/data/pids HBASE_HEAPSIZE=512m HBASE_MANAGES_ZK=true \
    HBASE_OPTS="-Djava.net.preferIPv4Stack=true -XX:+UseG1GC"
USER hbase
VOLUME /data
EXPOSE 8080 16010
HEALTHCHECK --interval=15s --timeout=5s --start-period=90s --retries=10 \
  CMD curl -fs -H 'Accept: application/json' http://127.0.0.1:8080/status/cluster | grep -q LiveNodes
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
