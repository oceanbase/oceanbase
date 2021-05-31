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

#define USING_LOG_PREFIX SHARE
#include "share/ob_i_data_access_service.h"
#include "lib/hash_func/ob_hash_func.h"
#include "common/ob_range.h"
#include "common/ob_store_range.h"

namespace oceanbase {

namespace common {

OB_SERIALIZE_MEMBER(ObLimitParam, offset_, limit_);
OB_SERIALIZE_MEMBER(SampleInfo, table_id_, method_, scope_, percent_, seed_);
OB_SERIALIZE_MEMBER(
    ObEstRowCountRecord, table_id_, table_type_, version_range_, logical_row_count_, physical_row_count_);
uint64_t SampleInfo::hash(uint64_t seed) const
{
  seed = do_hash(table_id_, seed);
  seed = do_hash(method_, seed);
  seed = do_hash(scope_, seed);
  seed = do_hash(percent_, seed);
  seed = do_hash(seed_, seed);

  return seed;
}
int ObIDataAccessService::query_range_to_macros(ObIAllocator& allocator, const ObPartitionKey& pkey,
    const ObIArray<ObNewRange>& ranges, const int64_t type, uint64_t* macros_count, const int64_t* total_task_count,
    ObIArray<ObNewRange>* splitted_ranges, ObIArray<int64_t>* split_index)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObArray<ObStoreRange> store_ranges;
  ObArray<ObStoreRange> store_splitted_ranges;
  ObIArray<ObStoreRange>* splitted_ptr = splitted_ranges == nullptr ? nullptr : &store_splitted_ranges;
  ObStoreRange store_range;

  int64_t origin_splitted_range_count = splitted_ranges == nullptr ? 0 : splitted_ranges->count();

  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i++) {
    store_range.assign(ranges.at(i));
    if (OB_FAIL(store_ranges.push_back(store_range))) {
      LOG_WARN("Failed to push back store range", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && splitted_ranges != nullptr && i < splitted_ranges->count(); i++) {
    store_range.assign(splitted_ranges->at(i));
    if (OB_FAIL(store_splitted_ranges.push_back(store_range))) {
      LOG_WARN("Failed to push back store range", K(ret));
    }
  }

  if (OB_SUCC(ret) &&
      OB_FAIL(query_range_to_macros(
          allocator, pkey, store_ranges, type, macros_count, total_task_count, splitted_ptr, split_index))) {
    LOG_WARN("Failed to call virtual query_range_to_macros", K(ret));
  }

  if (OB_SUCC(ret) && nullptr != splitted_ranges) {
    if (splitted_ranges->empty()) {
      if (OB_FAIL(splitted_ranges->reserve(store_splitted_ranges.count()))) {
        LOG_WARN("failed to prepare allocate ranges", K(ret));
      }
    }
    for (int64_t i = origin_splitted_range_count; OB_SUCC(ret) && i < store_splitted_ranges.count(); i++) {
      ObNewRange new_range;
      store_splitted_ranges.at(i).to_new_range(new_range);
      if (OB_INVALID_INDEX == new_range.table_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid table id", K(ret), K(new_range), K(store_splitted_ranges.at(i)));
      } else if (OB_FAIL(splitted_ranges->push_back(new_range))) {
        LOG_WARN("Failed to push back new range", K(ret));
      }
    }
  }
  return ret;
}

bool ObVTableScanParam::is_index_limit() const
{
  return (scan_flag_.is_index_back() && (filters_.count() <= 0 && (NULL == op_filters_ || op_filters_->empty())));
}
}  // namespace common
}  // namespace oceanbase
