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

#include "storage/access/ob_micro_block_handle_mgr.h"
#include "storage/tablet/ob_tablet.h"

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
  OB_INLINE bool is_invalid_memtable_result() const { return logical_row_count_ <= 0 && physical_row_count_ > 1024; }
  TO_STRING_KV(K_(logical_row_count), K_(physical_row_count));

  ObPartitionEst();
  int add(const ObPartitionEst &pe);
  int deep_copy(const ObPartitionEst &src);
  void reset() { logical_row_count_ = physical_row_count_ = 0; }
  bool operator ==(const ObPartitionEst &other) const { return logical_row_count_ == other.logical_row_count_ &&
                                                               physical_row_count_ == other.physical_row_count_; }
};

struct ObIndexSSTableEstimateContext
{
public:
  ObIndexSSTableEstimateContext(
      const ObTabletHandle &tablet_handle,
      const common::ObQueryFlag &query_flag,
      const int64_t base_version = -1)
      : index_read_info_(tablet_handle.get_obj()->get_rowkey_read_info()),
      tablet_handle_(tablet_handle),
      query_flag_(query_flag),
      base_version_(base_version)
  {
  }

  ~ObIndexSSTableEstimateContext() = default;

  TO_STRING_KV(K_(tablet_handle), K_(query_flag));
  const ObITableReadInfo &index_read_info_;
  const ObTabletHandle &tablet_handle_;
  const common::ObQueryFlag &query_flag_;
  const int64_t base_version_;
};

struct ObEstimatedResult
{
  ObEstimatedResult(const bool only_block = false)
    : total_row_count_(0), total_row_count_delta_(0), macro_block_cnt_(0), micro_block_cnt_(0),
    only_block_(only_block)
  {
  }
  int64_t total_row_count_;
  int64_t total_row_count_delta_;
  uint64_t macro_block_cnt_;
  uint64_t micro_block_cnt_;
  bool only_block_;
  TO_STRING_KV(K_(total_row_count), K_(total_row_count_delta), K_(macro_block_cnt), K_(micro_block_cnt), K_(only_block));
};

class ObIndexBlockScanEstimator
{
public:
  ObIndexBlockScanEstimator(const ObIndexSSTableEstimateContext &context);
  ~ObIndexBlockScanEstimator() = default;
  void reuse() {}
  int estimate_row_count(blocksstable::ObSSTable &sstable,
                         const blocksstable::ObDatumRange &datum_range,
                         ObPartitionEst &part_est);
  int estimate_block_count(blocksstable::ObSSTable &sstable,
                           const blocksstable::ObDatumRange &datum_range,
                           int64_t &macro_block_cnt,
                           int64_t &micro_block_cnt);
private:
  int cal_total_estimate_result(blocksstable::ObSSTable &sstable,
      const blocksstable::ObDatumRange &datum_range,
      ObEstimatedResult &result);
  int estimate_data_block_row_count(
      const blocksstable::ObDatumRange &range,
      ObMicroBlockDataHandle &micro_handle,
      bool consider_multi_version,
       ObPartitionEst &est);
  uint64_t tenant_id_;
  const ObIndexSSTableEstimateContext &context_;
};

}
}
#endif /* OCEANBASE_STORAGE_OB_INDEX_SSTABLE_ESTIMATOR_H */
