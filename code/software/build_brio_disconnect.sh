#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_dir="$root/software/src/brio/bgprpki-io"
target_dir="${1:-$root/software/bin-brio-disconnect}"
sysroot_lib="$root/software/sysroot/usr/lib/x86_64-linux-gnu"

mkdir -p "$target_dir"
for executable in bird birdc bngblaster; do
    ln -sfn "../bin/$executable" "$target_dir/$executable"
done

gcc -std=gnu11 -fpermissive -O2 -DBRIO -D_GNU_SOURCE \
    -I"$root/software/include" \
    -I"$root/software/sysroot/usr/include" \
    -I"$source_dir" \
    -I"$root/software/src/brio/srx-crypto-api" \
    "$source_dir/rpki-rtr-protocol/rpkirtr_svr.c" \
    "$source_dir/srx-common/server_socket.c" \
    "$source_dir/srx-common/debug.c" \
    "$source_dir/srx-common/mutex.c" \
    "$source_dir/srx-common/rwlock.c" \
    "$source_dir/srx-common/slist.c" \
    "$source_dir/srx-common/socket.c" \
    "$source_dir/srx-common/str.c" \
    "$source_dir/srx-common/timer.c" \
    "$source_dir/antd-util/log.c" \
    "$source_dir/antd-util/prefix.c" \
    "$source_dir/antd-util/linked_list.c" \
    "$source_dir/antd-util/stack.c" \
    "$source_dir/antd-util/printer.c" \
    -L"$sysroot_lib" -Wl,-rpath,"$sysroot_lib" \
    -pthread -lreadline -lcrypto -lm -ldl -lrt \
    -o "$target_dir/brio_rc.new"
mv "$target_dir/brio_rc.new" "$target_dir/brio_rc"
