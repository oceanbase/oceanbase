/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_DDL_BLOCK_SAMPLE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_DDL_BLOCK_SAMPLE_ITERATOR_H

#include "storage/ob_i_store.h"
#include "ob_i_sample_iterator.h"
#include "ob_multiple_scan_merge.h"
#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"
#include "storage/access/ob_block_sample_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObDDLBlockSampleIterator : public ObBlockSampleIterator
{
public:
  explicit ObDDLBlockSampleIterator(const common::SampleInfo &sample_info) :
    ObBlockSampleIterator(sample_info), is_opened_(false), reservoir_() { }
  virtual ~ObDDLBlockSampleIterator() = default;
  int open(ObMultipleScanMerge &scan_merge,
           ObTableAccessContext &access_ctx,
           const blocksstable::ObDatumRange &range,
           ObGetTableParam &get_table_param,
           const bool is_reverse_scan);
  virtual void reuse() override;
  virtual void reset() override;
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;

protected:
  virtual int open_range(blocksstable::ObDatumRange &range) override;

private:
  int reservoir_block_sample();

private:
  bool is_opened_;
  ObArray<blocksstable::ObDatumRange *> reservoir_;
};

}
}

#endif /* OCEANBASE_STORAGE_OB_DDL_BLOCK_SAMPLE_ITERATOR_H */
