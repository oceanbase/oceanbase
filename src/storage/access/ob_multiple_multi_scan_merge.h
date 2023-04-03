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
  virtual void reset();
protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
  virtual int is_range_valid() const override;
  virtual void collect_merge_stat(ObTableStoreStat &stat) const override;
private:
  const ObIArray<blocksstable::ObDatumRange> *ranges_;
  common::ObSEArray<blocksstable::ObDatumRange, 32> cow_ranges_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleMultiScanMerge);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_MULTI_SCAN_MERGE_
