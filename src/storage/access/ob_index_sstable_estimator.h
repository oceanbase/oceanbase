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

#ifndef OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H
#define OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H

#include "storage/ob_micro_block_handle_mgr.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
struct ObPartitionEst
{
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  OB_INLINE bool is_invalid_memtable_result() const { return logical_row_count_ <= 0 && physical_row_count_ > 1000; }
  TO_STRING_KV(K_(logical_row_count), K_(physical_row_count));

  ObPartitionEst();
  int add(const ObPartitionEst &pe);
  int deep_copy(const ObPartitionEst &src);
  void reset() { logical_row_count_ = physical_row_count_ = 0; }
};

struct ObIndexSSTableEstimateContext
{
public:
  ObIndexSSTableEstimateContext(
      blocksstable::ObSSTable &sstable,
      const ObTabletHandle &tablet_handle,
      const common::ObQueryFlag &query_flag,
      const blocksstable::ObDatumRange &datum_range)
      : sstable_(sstable),
      tablet_handle_(tablet_handle),
      query_flag_(query_flag),
      range_(datum_range) {}
  ~ObIndexSSTableEstimateContext() {}
  OB_INLINE bool is_valid() const
  {
    return  tablet_handle_.is_valid() && range_.is_valid();
  }

  TO_STRING_KV(K_(tablet_handle), K_(query_flag), K_(range));
  blocksstable::ObSSTable &sstable_;
  const ObTabletHandle &tablet_handle_;
  const common::ObQueryFlag &query_flag_;
  const blocksstable::ObDatumRange &range_;
};

struct ObEstimatedResult
{
  ObEstimatedResult() : total_row_count_(0), total_row_count_delta_(0),
    excluded_row_count_(0), excluded_row_count_delta_(0)
  {
  }
  int64_t total_row_count_;
  int64_t total_row_count_delta_;
  int64_t excluded_row_count_;
  int64_t excluded_row_count_delta_;
  TO_STRING_KV(K_(total_row_count), K_(total_row_count_delta),
    K_(excluded_row_count), K_(excluded_row_count_delta));
};

class ObIndexBlockScanEstimator
{
public:
  ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context);
  ~ObIndexBlockScanEstimator();
  int estimate_row_count(ObPartitionEst &part_est);
private:
  int cal_total_row_count(ObEstimatedResult &result);
  int estimate_excluded_border_row_count(bool is_left, ObEstimatedResult &result);
  int goto_next_level(
      const blocksstable::ObDatumRange &range,
      const blocksstable::ObMicroIndexInfo &micro_index_info,
      ObEstimatedResult &result);
  int prefetch_index_block_data(
      const blocksstable::ObMicroIndexInfo &micro_index_info,
      ObMicroBlockDataHandle &micro_handle);
  int estimate_data_block_row_count(
      const blocksstable::ObDatumRange &range,
      ObMicroBlockDataHandle &micro_handle,
      bool consider_multi_version,
       ObPartitionEst &est);
  ObMicroBlockDataHandle &get_read_handle()
  {
    return micro_handles_[level_++ % DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
  }
  static const int64_t DEFAULT_GET_MICRO_DATA_HANDLE_CNT = 2;
  uint64_t tenant_id_;
  blocksstable::ObMicroBlockData root_index_block_;
  blocksstable::ObIndexBlockRowScanner index_block_row_scanner_;
  blocksstable::ObMacroBlockReader macro_reader_;
  int64_t level_;
  ObMicroBlockDataHandle micro_handles_[DEFAULT_GET_MICRO_DATA_HANDLE_CNT];
  blocksstable::ObMicroBlockData index_block_data_;
  const ObIndexSSTableEstimateContext &context_;
  common::ObArenaAllocator allocator_;
};

}
}
#endif /* OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H */
