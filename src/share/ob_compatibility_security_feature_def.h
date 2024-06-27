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
DEF_COMPAT_CONTROL_FEATURE(MYSQL_PRIV_ENHANCE, "add privilege check to some command",
    MOCK_CLUSTER_VERSION_4_2_3_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)
DEF_COMPAT_CONTROL_FEATURE(MYSQL_SET_VAR_PRIV_ENHANCE, "check privilege for set var subquery",
    MOCK_CLUSTER_VERSION_4_2_4_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)
DEF_COMPAT_CONTROL_FEATURE(MYSQL_USER_REVOKE_ALL_ENHANCE, "use create_user to check privilege for revoke all from user",
    MOCK_CLUSTER_VERSION_4_2_4_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)
DEF_COMPAT_CONTROL_FEATURE(MYSQL_USER_REVOKE_ALL_WITH_PL_PRIV_CHECK, "revoke all on db.* need check pl privilege",
    MOCK_CLUSTER_VERSION_4_2_4_0, CLUSTER_VERSION_4_3_0_0,
    CLUSTER_VERSION_4_3_2_0)
#endif
