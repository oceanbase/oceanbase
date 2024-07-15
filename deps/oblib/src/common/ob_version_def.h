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

#ifndef OCEANBASE_OBSERVER_OB_VERSION_DEF_H_
#define OCEANBASE_OBSERVER_OB_VERSION_DEF_H_

#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{
#define OB_VSN_MAJOR_SHIFT 32
#define OB_VSN_MINOR_SHIFT 16
#define OB_VSN_MAJOR_PATCH_SHIFT 8
#define OB_VSN_MINOR_PATCH_SHIFT 0
#define OB_VSN_MAJOR_MASK 0xffffffff
#define OB_VSN_MINOR_MASK 0xffff
#define OB_VSN_MAJOR_PATCH_MASK 0xff
#define OB_VSN_MINOR_PATCH_MASK 0xff
#define OB_VSN_MAJOR(version) (static_cast<const uint32_t>((version >> OB_VSN_MAJOR_SHIFT) & OB_VSN_MAJOR_MASK))
#define OB_VSN_MINOR(version) (static_cast<const uint16_t>((version >> OB_VSN_MINOR_SHIFT) & OB_VSN_MINOR_MASK))
#define OB_VSN_MAJOR_PATCH(version) (static_cast<const uint8_t>((version >> OB_VSN_MAJOR_PATCH_SHIFT) & OB_VSN_MAJOR_PATCH_MASK))
#define OB_VSN_MINOR_PATCH(version) (static_cast<const uint8_t>(version & OB_VSN_MINOR_PATCH_MASK))

#define CALC_VERSION(major, minor, major_patch, minor_patch) \
        (((major) << OB_VSN_MAJOR_SHIFT) + \
         ((minor) << OB_VSN_MINOR_SHIFT) + \
         ((major_patch) << OB_VSN_MAJOR_PATCH_SHIFT) + \
         ((minor_patch)))
constexpr static inline uint64_t
cal_version(const uint64_t major, const uint64_t minor, const uint64_t major_patch, const uint64_t minor_patch)
{
  return CALC_VERSION(major, minor, major_patch, minor_patch);
}

#define DEF_MAJOR_VERSION 1
#define DEF_MINOR_VERSION 4
#define DEF_MAJOR_PATCH_VERSION 40
#define DEF_MINOR_PATCH_VERSION 0

#define CLUSTER_VERSION_140 (oceanbase::common::cal_version(1, 4, 0, 0))
#define CLUSTER_VERSION_141 (oceanbase::common::cal_version(1, 4, 0, 1))
#define CLUSTER_VERSION_142 (oceanbase::common::cal_version(1, 4, 0, 2))
#define CLUSTER_VERSION_143 (oceanbase::common::cal_version(1, 4, 0, 3))
#define CLUSTER_VERSION_1431 (oceanbase::common::cal_version(1, 4, 0, 31))
#define CLUSTER_VERSION_1432 (oceanbase::common::cal_version(1, 4, 0, 32))
#define CLUSTER_VERSION_144 (oceanbase::common::cal_version(1, 4, 0, 4))
#define CLUSTER_VERSION_1440 (oceanbase::common::cal_version(1, 4, 0, 40))
#define CLUSTER_VERSION_1450 (oceanbase::common::cal_version(1, 4, 0, 50))
#define CLUSTER_VERSION_1460 (oceanbase::common::cal_version(1, 4, 0, 60))
#define CLUSTER_VERSION_1461 (oceanbase::common::cal_version(1, 4, 0, 61))
#define CLUSTER_VERSION_1470 (oceanbase::common::cal_version(1, 4, 0, 70))
#define CLUSTER_VERSION_1471 (oceanbase::common::cal_version(1, 4, 0, 71))
#define CLUSTER_VERSION_1472 (oceanbase::common::cal_version(1, 4, 0, 72))
#define CLUSTER_VERSION_1500 (oceanbase::common::cal_version(1, 5, 0, 0))
#define CLUSTER_VERSION_2000 (oceanbase::common::cal_version(2, 0, 0, 0))
#define CLUSTER_VERSION_2100 (oceanbase::common::cal_version(2, 1, 0, 0))
#define CLUSTER_VERSION_2110 (oceanbase::common::cal_version(2, 1, 0, 1))
#define CLUSTER_VERSION_2200 (oceanbase::common::cal_version(2, 2, 0, 0))
#define CLUSTER_VERSION_2210 (oceanbase::common::cal_version(2, 2, 0, 1))
/*
 * FIXME: cluster_version目前最高是4位，此处定义要和CMakeLists.txt、tools/upgrade、src/share/parameter/ob_parameter_seed.ipp的定义保持一致
 *        当最后一位非0时，需要注意。比方说2.2.2版本实际上代表的是2.2.02版本，但实际我们想定义成2.2.20版本，和我们的意图不符。
 *        但2.2.1及之前的版本已经发版，为了避免引入兼容性问题，不改历史版本的cluster_version定义。
 */
#define CLUSTER_VERSION_2220 (oceanbase::common::cal_version(2, 2, 0, 20))
#define CLUSTER_VERSION_2230 (oceanbase::common::cal_version(2, 2, 0, 30))
#define CLUSTER_VERSION_2240 (oceanbase::common::cal_version(2, 2, 0, 40))
#define CLUSTER_VERSION_2250 (oceanbase::common::cal_version(2, 2, 0, 50))
#define CLUSTER_VERSION_2260 (oceanbase::common::cal_version(2, 2, 0, 60))
#define CLUSTER_VERSION_2270 (oceanbase::common::cal_version(2, 2, 0, 70))
#define CLUSTER_VERSION_2271 (oceanbase::common::cal_version(2, 2, 0, 71))
#define CLUSTER_VERSION_2272 (oceanbase::common::cal_version(2, 2, 0, 72))
#define CLUSTER_VERSION_2273 (oceanbase::common::cal_version(2, 2, 0, 73))
#define CLUSTER_VERSION_2274 (oceanbase::common::cal_version(2, 2, 0, 74))
#define CLUSTER_VERSION_2275 (oceanbase::common::cal_version(2, 2, 0, 75))
#define CLUSTER_VERSION_2276 (oceanbase::common::cal_version(2, 2, 0, 76))
#define CLUSTER_VERSION_2277 (oceanbase::common::cal_version(2, 2, 0, 77))
#define CLUSTER_VERSION_3000 (oceanbase::common::cal_version(3, 0, 0, 0))
#define CLUSTER_VERSION_3100 (oceanbase::common::cal_version(3, 1, 0, 0))
#define CLUSTER_VERSION_311 (oceanbase::common::cal_version(3, 1, 0, 1))
#define CLUSTER_VERSION_312 (oceanbase::common::cal_version(3, 1, 0, 2))
#define CLUSTER_VERSION_3200 (oceanbase::common::cal_version(3, 2, 0, 0))
#define CLUSTER_VERSION_321 (oceanbase::common::cal_version(3, 2, 0, 1))
#define CLUSTER_VERSION_322 (oceanbase::common::cal_version(3, 2, 0, 2))
// ATTENSION!!!!!!!!!!!!!!!!!
//
// Cluster Version which is less than "3.2.3":
// - 1. It's composed by 3 parts(major、minor、minor_patch)
// - 2. String: cluster version will be format as "major.minor.minor_patch[.0]", and string like "major.minor.x.minor_patch" is invalid.
// - 3. Integer: for compatibility, cluster version will be encoded into "major|minor|x|minor_patch". "x" must be 0, otherwise, it's invalid.
// - 4. Print: cluster version str will be still printed as 3 parts.
//
// Cluster Version which is not less than "3.2.3":
// - 1. It's composed by 4 parts(major、minor、major_patch、minor_patch)
// - 2. String: cluster version will be format as "major.minor.major_patch.minor_patch".
// - 3. Integer: cluster version will be encoded into "major|minor|major_patch|minor_patch".
// - 4. Print: cluster version str will be printed as 4 parts.
#define CLUSTER_VERSION_3_2_3_0 (oceanbase::common::cal_version(3, 2, 3, 0))
#define CLUSTER_VERSION_4_0_0_0 (oceanbase::common::cal_version(4, 0, 0, 0))
#define CLUSTER_VERSION_4_1_0_0 (oceanbase::common::cal_version(4, 1, 0, 0))
#define CLUSTER_VERSION_4_1_0_1 (oceanbase::common::cal_version(4, 1, 0, 1))
#define CLUSTER_VERSION_4_1_0_2 (oceanbase::common::cal_version(4, 1, 0, 2))
#define CLUSTER_VERSION_4_2_0_0 (oceanbase::common::cal_version(4, 2, 0, 0))
#define CLUSTER_VERSION_4_2_1_0 (oceanbase::common::cal_version(4, 2, 1, 0))
#define CLUSTER_VERSION_4_2_1_1 (oceanbase::common::cal_version(4, 2, 1, 1))
#define CLUSTER_VERSION_4_2_1_2 (oceanbase::common::cal_version(4, 2, 1, 2))
#define MOCK_CLUSTER_VERSION_4_2_1_3 (oceanbase::common::cal_version(4, 2, 1, 3))
#define MOCK_CLUSTER_VERSION_4_2_1_4 (oceanbase::common::cal_version(4, 2, 1, 4))
#define MOCK_CLUSTER_VERSION_4_2_1_6 (oceanbase::common::cal_version(4, 2, 1, 6))
#define MOCK_CLUSTER_VERSION_4_2_1_7 (oceanbase::common::cal_version(4, 2, 1, 7))
#define CLUSTER_VERSION_4_2_2_0 (oceanbase::common::cal_version(4, 2, 2, 0))
#define MOCK_CLUSTER_VERSION_4_2_2_1 (oceanbase::common::cal_version(4, 2, 2, 1))
#define MOCK_CLUSTER_VERSION_4_2_3_0 (oceanbase::common::cal_version(4, 2, 3, 0))
#define MOCK_CLUSTER_VERSION_4_2_3_1 (oceanbase::common::cal_version(4, 2, 3, 1))
#define MOCK_CLUSTER_VERSION_4_2_4_0 (oceanbase::common::cal_version(4, 2, 4, 0))
#define MOCK_CLUSTER_VERSION_4_2_5_0 (oceanbase::common::cal_version(4, 2, 5, 0))
// new data version before 4.3 cannot upgrade to master, must add "MOCK_" prefix
#define CLUSTER_VERSION_4_3_0_0 (oceanbase::common::cal_version(4, 3, 0, 0))
#define CLUSTER_VERSION_4_3_0_1 (oceanbase::common::cal_version(4, 3, 0, 1))
#define CLUSTER_VERSION_4_3_1_0 (oceanbase::common::cal_version(4, 3, 1, 0))
#define CLUSTER_VERSION_4_3_2_0 (oceanbase::common::cal_version(4, 3, 2, 0))
#define CLUSTER_VERSION_4_3_3_0 (oceanbase::common::cal_version(4, 3, 3, 0))
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//TODO: If you update the above version, please update CLUSTER_CURRENT_VERSION.
#define CLUSTER_CURRENT_VERSION CLUSTER_VERSION_4_3_3_0

// ATTENSION !!!!!!!!!!!!!!!!!!!!!!!!!!!
// 1. After 4.0, each cluster_version is corresponed to a data version.
// 2. cluster_version and data_version is not compariable.
// 3. TODO: If you update data_version below, please update DATA_CURRENT_VERSION & ObUpgradeChecker too.
#define DEFAULT_MIN_DATA_VERSION (oceanbase::common::cal_version(0, 0, 0, 1))
#define DATA_VERSION_4_0_0_0 (oceanbase::common::cal_version(4, 0, 0, 0))
#define DATA_VERSION_4_1_0_0 (oceanbase::common::cal_version(4, 1, 0, 0))
#define DATA_VERSION_4_1_0_1 (oceanbase::common::cal_version(4, 1, 0, 1))
#define DATA_VERSION_4_1_0_2 (oceanbase::common::cal_version(4, 1, 0, 2))
#define DATA_VERSION_4_2_0_0 (oceanbase::common::cal_version(4, 2, 0, 0))
#define DATA_VERSION_4_2_1_0 (oceanbase::common::cal_version(4, 2, 1, 0))
#define DATA_VERSION_4_2_1_1 (oceanbase::common::cal_version(4, 2, 1, 1))
#define DATA_VERSION_4_2_1_2 (oceanbase::common::cal_version(4, 2, 1, 2))
#define MOCK_DATA_VERSION_4_2_1_3 (oceanbase::common::cal_version(4, 2, 1, 3))
#define MOCK_DATA_VERSION_4_2_1_4 (oceanbase::common::cal_version(4, 2, 1, 4))
#define MOCK_DATA_VERSION_4_2_1_5 (oceanbase::common::cal_version(4, 2, 1, 5))
#define MOCK_DATA_VERSION_4_2_1_8 (oceanbase::common::cal_version(4, 2, 1, 8))
#define DATA_VERSION_4_2_2_0 (oceanbase::common::cal_version(4, 2, 2, 0))
#define MOCK_DATA_VERSION_4_2_2_1 (oceanbase::common::cal_version(4, 2, 2, 1))
#define MOCK_DATA_VERSION_4_2_3_0 (oceanbase::common::cal_version(4, 2, 3, 0))
#define MOCK_DATA_VERSION_4_2_3_1 (oceanbase::common::cal_version(4, 2, 3, 1))
#define MOCK_DATA_VERSION_4_2_4_0 (oceanbase::common::cal_version(4, 2, 4, 0))
#define MOCK_DATA_VERSION_4_2_5_0 (oceanbase::common::cal_version(4, 2, 5, 0))
// new data version before 4.3 cannot upgrade to master, must add "MOCK_" prefix
#define DATA_VERSION_4_3_0_0 (oceanbase::common::cal_version(4, 3, 0, 0))
#define DATA_VERSION_4_3_0_1 (oceanbase::common::cal_version(4, 3, 0, 1))
#define DATA_VERSION_4_3_1_0 (oceanbase::common::cal_version(4, 3, 1, 0))
#define DATA_VERSION_4_3_2_0 (oceanbase::common::cal_version(4, 3, 2, 0))
#define DATA_VERSION_4_3_3_0 (oceanbase::common::cal_version(4, 3, 3, 0))
#define DATA_CURRENT_VERSION DATA_VERSION_4_3_3_0
// ATTENSION !!!!!!!!!!!!!!!!!!!!!!!!!!!
// LAST_BARRIER_DATA_VERSION should be the latest barrier data version before DATA_CURRENT_VERSION
#define LAST_BARRIER_DATA_VERSION DATA_VERSION_4_2_1_0

class VersionUtil
{
public:
  static bool check_version_valid(const uint64_t version);
  static int64_t print_version_str(char *buf, const int64_t buf_len, uint64_t version);
};

} // namespace common
} // namespace oceanbase

#endif
