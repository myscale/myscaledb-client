#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

# Choose random timezone for this test run.
TZ="$(grep -v '#' /usr/share/zoneinfo/zone.tab | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" >/etc/timezone

source /etc/profile
arch="$(dpkg --print-architecture)"
if [[ "x$arch" = "xamd64" ]]; then
    echo "/opt/intel/oneapi/mkl/$INTEL_ONEAPI_VERSION/lib/intel64" >>/etc/ld.so.conf.d/libc.conf
    ldconfig
fi

dpkg -i /home/package/clickhouse-common-static_$arch.deb
dpkg -i /home/package/clickhouse-common-static-dbg_$arch.deb
dpkg -i /home/package/clickhouse-server_$arch.deb
dpkg -i /home/package/clickhouse-client_$arch.deb

# install test configs
mkdir -p /etc/clickhouse-server/
cp -rf  config/* /etc/clickhouse-server/.

clickhouse start
echo "clickhouse start finish, exist after 5s"
sleep 5
