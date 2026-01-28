//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE

#include "storage/compaction_ttl/ob_ttl_filter_val.h"
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
  if (OB_SUCC(ret)) {
    // Sort the array manually to avoid template instantiation issues
    for (int64_t i = 0; i < filter_pairs_.count() - 1; ++i) {
      for (int64_t j = i + 1; j < filter_pairs_.count(); ++j) {
        if (filter_pairs_.at(j).col_idx_ < filter_pairs_.at(i).col_idx_) {
          TTLFilterPair temp = filter_pairs_.at(i);
          filter_pairs_.at(i) = filter_pairs_.at(j);
          filter_pairs_.at(j) = temp;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
