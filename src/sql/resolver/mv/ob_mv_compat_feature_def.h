/**
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

// MView refresh SQL generation feature definitions.
// DEF_MV_COMPAT_FEATURE(type, description, latest_version, patch_versions ...)

#ifdef DEF_MV_COMPAT_FEATURE

DEF_MV_COMPAT_FEATURE(ADAPTIVE_REFRESH_STEP,
    "Skip refresh DML for base tables without delta data (no insert/delete/update)",
    CLUSTER_VERSION_4_4_2_2)

DEF_MV_COMPAT_FEATURE(DISABLE_SEMI_TO_INNER_HINT,
    "Remove built-in SEMI_TO_INNER hint, rely on kernel semi to inner transform",
    CLUSTER_VERSION_4_4_2_2)

#endif
