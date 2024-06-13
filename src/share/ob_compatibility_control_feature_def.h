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

// define the feature list here whose behavior is different in MySQL5.7, MySQL8.0 or OB
// DEF_COMPAT_CONTROL_FEATURE(type, id, is_dynanmic, description, lastest_version, patch_versions ...)

#ifdef DEF_COMPAT_CONTROL_FEATURE
DEF_COMPAT_CONTROL_FEATURE(FUNC_REPLACE_NULL,
    "The result of REPLACE('abd', '', null) is different in MySQL 5.7 and 8.0",
    MOCK_CLUSTER_VERSION_4_2_3_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)

DEF_COMPAT_CONTROL_FEATURE(UPD_LIMIT_OFFSET,
    "MySQL do not support the use of OFFSET in the LIMIT clause of UPDATE/DELETE statement",
    MOCK_CLUSTER_VERSION_4_2_3_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)

DEF_COMPAT_CONTROL_FEATURE(PROJECT_NULL,
    "MySQL will rename the projection item names with pure null values to `NULL`",
    MOCK_CLUSTER_VERSION_4_2_3_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)

DEF_COMPAT_CONTROL_FEATURE(VAR_NAME_LENGTH,
    "MySQL will limit the length of user-defined variable names to within 64 characters",
    MOCK_CLUSTER_VERSION_4_2_3_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)
#endif
