/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// MView refresh SQL generation feature definitions.
// DEF_MV_COMPAT_FEATURE(type, description, latest_version, patch_versions ...)

#ifdef DEF_MV_COMPAT_FEATURE

DEF_MV_COMPAT_FEATURE(ADAPTIVE_REFRESH_STEP,
    "Skip refresh DML for base tables without delta data (no insert/delete/update)",
    MOCK_CLUSTER_VERSION_4_4_2_2, CLUSTER_VERSION_4_5_0_0,
    CLUSTER_VERSION_4_6_1_0)

DEF_MV_COMPAT_FEATURE(DISABLE_SEMI_TO_INNER_HINT,
    "Remove built-in SEMI_TO_INNER hint, rely on kernel semi to inner transform",
    MOCK_CLUSTER_VERSION_4_4_2_2, CLUSTER_VERSION_4_5_0_0,
    CLUSTER_VERSION_4_6_1_0)

#endif
