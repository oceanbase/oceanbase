/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
