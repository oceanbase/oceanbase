/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"

namespace oceanbase
{
namespace storage
{

int ObTTLFilterInfoArray::sort_array()
{
  lib::ob_sort(mds_info_array_.begin(), mds_info_array_.end(), compare);
  return OB_SUCCESS;
}

bool ObTTLFilterInfoArray::compare(const ObTTLFilterInfo *lhs, const ObTTLFilterInfo *rhs)
{
  bool bret = true;

  if (lhs->commit_version_ == rhs->commit_version_) {
    bret = lhs->key_ < rhs->key_;
  } else {
    bret = lhs->commit_version_ < rhs->commit_version_;
  }

  return bret;
}

} // namespace storage
} // namespace oceanbase
