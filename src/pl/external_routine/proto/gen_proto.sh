#! /bin/env bash

set -e

MULAN_HEADER='/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
'

protoc ob_pl_java_udf.proto --c_out=.

for file in *.pb-c.*; do
    if [ -f "${file}" ]; then
        BUFFER=$(printf "%s\n%s" "${MULAN_HEADER}" "$(cat "${file}")")
        printf "%s\n" "${BUFFER}" > "${file}"
    fi
done
