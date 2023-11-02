// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_STORAGE_MULTIPLE_MULTI_SKIP_SCAN_MERGE_H
#define OCEANBASE_STORAGE_MULTIPLE_MULTI_SKIP_SCAN_MERGE_H

#include "ob_multiple_skip_scan_merge.h"
namespace oceanbase
{
namespace storage
{

class ObMultipleMultiSkipScanMerge final : public ObMultipleSkipScanMerge
{
public:
  ObMultipleMultiSkipScanMerge();
  virtual ~ObMultipleMultiSkipScanMerge();
  virtual int init(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param) override;
  virtual void reset() override;
  virtual void reuse() override;
  int open(
      const common::ObIArray<blocksstable::ObDatumRange> &ranges,
      const common::ObIArray<blocksstable::ObDatumRange> &skip_scan_ranges);
protected:
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row) override;
  virtual int inner_get_next_rows() override;
private:
  int64_t cur_range_idx_;
  const ObIArray<blocksstable::ObDatumRange> *ranges_;
  const ObIArray<blocksstable::ObDatumRange> *skip_scan_ranges_;
};

}
}

#endif // OCEANBASE_STORAGE_MULTIPLE_MULTI_SKIP_SCAN_MERGE_H