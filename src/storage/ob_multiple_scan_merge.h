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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_
#include "storage/ob_multiple_scan_merge_impl.h"

namespace oceanbase {
namespace storage {
class ObMultipleScanMerge : public ObMultipleScanMergeImpl {
public:
  ObMultipleScanMerge();
  virtual ~ObMultipleScanMerge();

public:
  static int estimate_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRange& range, const common::ObIArray<ObITable*>& tables, ObPartitionEst& cost_estimate,
      common::ObIArray<common::ObEstRowCountRecord>& est_records);
  int open(const common::ObExtStoreRange& range);
  virtual void reset() override;
  virtual void reuse() override;
  inline void set_iter_del_row(const bool iter_del_row)
  {
    iter_del_row_ = iter_del_row;
  }

protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;

  virtual int inner_get_next_row(ObStoreRow& row) override;
  virtual int is_range_valid() const override;
  virtual int prepare() override;
  virtual void collect_merge_stat(ObTableStoreStat& stat) const override;

private:
  const common::ObExtStoreRange* range_;
  common::ObExtStoreRange cow_range_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleScanMerge);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_
