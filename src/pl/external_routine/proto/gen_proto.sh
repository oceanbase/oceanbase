#! /bin/env bash

set -e

MULAN_HEADER='/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
'

protoc ob_pl_java_udf.proto --c_out=.

for file in *.pb-c.*; do
    if [ -f "${file}" ]; then
        BUFFER=$(printf "%s\n%s" "${MULAN_HEADER}" "$(cat "${file}")")
        printf "%s\n" "${BUFFER}" > "${file}"
    fi
done
