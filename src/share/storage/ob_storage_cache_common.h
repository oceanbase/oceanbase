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

#ifndef OCEANBASE_STORAGE_CAHCE_COMMON_DEFINE_H_
#define OCEANBASE_STORAGE_CAHCE_COMMON_DEFINE_H_

#include "lib/string/ob_string.h"
#include "share/ob_define.h"
namespace oceanbase
{
namespace storage
{
const char *const OB_DEFAULT_STORAGE_CACHE_POLICY_STR = "{\"GLOBAL\": \"AUTO\"}";
const char *const OB_DEFAULT_PART_STORAGE_CACHE_POLICY_STR = "NONE";

struct ObStorageCacheGlobalPolicy
{
enum PolicyType : uint8_t
{
  HOT_POLICY = 0,
  AUTO_POLICY = 1,
  TIME_POLICY = 2,
  NONE_POLICY = 3,
  MAX_POLICY
};
};
typedef ObStorageCacheGlobalPolicy::PolicyType ObStorageCachePolicyType;

int get_storage_cache_policy_type_from_str(const common::ObString &storage_cache_policy_str, ObStorageCachePolicyType &policy_type);
int get_storage_cache_policy_type_from_part_str(const common::ObString &storage_cache_policy_part_str, ObStorageCachePolicyType &policy_type);
} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_CAHCE_COMMON_DEFINE_H_