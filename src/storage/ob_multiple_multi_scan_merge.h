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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_MULTI_SCAN_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_MULTI_SCAN_MERGE_
#include "storage/ob_multiple_scan_merge_impl.h"

namespace oceanbase {
namespace storage {
class ObMultipleMultiScanMerge : public ObMultipleScanMergeImpl {
public:
  ObMultipleMultiScanMerge();
  virtual ~ObMultipleMultiScanMerge();

public:
  int open(const common::ObIArray<common::ObExtStoreRange>& ranges);
  virtual void reset();
  virtual void reuse() override;
  static int estimate_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRange>& ranges, const common::ObIArray<ObITable*>& tables,
      ObPartitionEst& part_estimate, common::ObIArray<common::ObEstRowCountRecord>& est_records);

protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(ObStoreRow& row);
  virtual int prepare() override;
  virtual int is_range_valid() const override;
  virtual void collect_merge_stat(ObTableStoreStat& stat) const override;
  virtual int skip_to_range(const int64_t range_idx) override;

private:
  int is_get_data_ready(bool& is_ready);
  int is_scan_data_ready(bool& is_ready);
  int inner_get_next_row_for_get(ObStoreRow& row, bool& need_retry);
  int inner_get_next_row_for_scan(ObStoreRow& row, bool& need_retry);
  int supply_consume();
  static int to_collation_free_range_on_demand(
      const common::ObIArray<common::ObExtStoreRange>& ranges, common::ObIAllocator& allocator);

private:
  const ObStoreRow* get_items_[common::MAX_TABLE_CNT_IN_STORAGE];
  int64_t get_num_;
  const ObIArray<ObExtStoreRange>* ranges_;
  ScanRangeArray cow_ranges_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleMultiScanMerge);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_MULTIPLE_MULTI_SCAN_MERGE_
