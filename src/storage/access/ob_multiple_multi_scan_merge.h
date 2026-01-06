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
#include "ob_multiple_scan_merge.h"
#include "storage/ob_storage_struct.h"


namespace oceanbase
{
namespace storage
{
class ObMultipleMultiScanMerge : public ObMultipleScanMerge
{
public:
  ObMultipleMultiScanMerge();
  virtual ~ObMultipleMultiScanMerge();
public:
  int open(const common::ObIArray<blocksstable::ObDatumRange> &ranges);
  virtual void reset() override;
  INHERIT_TO_STRING_KV("ObMultipleScanMerge", ObMultipleScanMerge, KPC_(ranges));

protected:
  virtual int calc_scan_range() override;
  int inner_calc_scan_range(const common::ObIArray<blocksstable::ObDatumRange> *&ranges,
                            common::ObIArray<blocksstable::ObDatumRange> &cow_ranges,
                            const int64_t curr_scan_index_,
                            const blocksstable::ObDatumRowkey &curr_rowkey,
                            const bool calc_di_base_range);
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
  virtual int pause(bool& do_pause) override final;
  virtual int get_current_range(ObDatumRange& current_range) const override;
  virtual int get_range_count() const override { return ranges_->count(); }
private:
  const common::ObIArray<blocksstable::ObDatumRange> *ranges_;
  common::ObSEArray<blocksstable::ObDatumRange, 32> cow_ranges_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleMultiScanMerge);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_MULTI_SCAN_MERGE_
