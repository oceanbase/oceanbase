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
#define USING_LOG_PREFIX STORAGE

#include "share/storage/ob_storage_cache_common.h"
namespace oceanbase
{
namespace storage
{
// todo(baonian.wcx): add details to convert str to policy type
int get_storage_cache_policy_type_from_str(const common::ObString &storage_cache_policy_str, ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  policy_type = ObStorageCachePolicyType::AUTO_POLICY;
  return ret;
}
// todo(baonian.wcx): add details to convert partstr to policy type
int get_storage_cache_policy_type_from_part_str(const common::ObString &storage_cache_policy_part_str, ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  policy_type = ObStorageCachePolicyType::AUTO_POLICY;
  return ret;
}

}
}