// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE

#include "storage/compaction_ttl/ob_ttl_filter_val.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/ob_sort.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"

namespace oceanbase
{
namespace storage
{
int ObTTLFilterVal::init(
  const ObTTLFilterInfoArray &ttl_filter_info_array)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTTLFilterInfo *> &ttl_array = ttl_filter_info_array.get_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < ttl_array.count(); ++i) {
    const ObTTLFilterInfo *ttl_filter_info = ttl_array.at(i);
    if (OB_ISNULL(ttl_filter_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ttl_filter_info is null", K(ret), K(i));
    } else if (OB_FAIL(filter_pairs_.push_back(
        TTLFilterPair(ttl_filter_info->ttl_filter_col_idx_, ttl_filter_info->ttl_filter_value_)))) {
      LOG_WARN("failed to push back filter pair", K(ret), KPC(ttl_filter_info));
    }
  } // for
  if (OB_SUCC(ret) && filter_pairs_.count() > 1) {
    lib::ob_sort(filter_pairs_.begin(), filter_pairs_.end(), [](const TTLFilterPair &lhs, const TTLFilterPair &rhs) {
      return lhs.col_idx_ < rhs.col_idx_;
    });
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
