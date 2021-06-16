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

#ifndef OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H

#include "storage/ob_i_store.h"
#include "storage/ob_i_sample_iterator.h"
#include "storage/ob_multiple_scan_merge.h"
#include "storage/ob_partition_store.h"
#include "storage/ob_all_micro_block_range_iterator.h"

namespace oceanbase {
namespace storage {

class ObBlockSampleIterator : public ObISampleIterator {
public:
  explicit ObBlockSampleIterator(const common::SampleInfo& sample_info);
  virtual ~ObBlockSampleIterator();
  int open(ObMultipleScanMerge& scan_merge, ObTableAccessContext& access_ctx, const common::ObExtStoreRange& range,
      const ObGetTableParam& get_table_param, const bool is_reverse_scan);
  void reuse();
  virtual int get_next_row(ObStoreRow*& row) override;
  virtual void reset() override;

private:
  int open_range(common::ObExtStoreRange& range);

private:
  static const int64_t ROW_CNT_LIMIT_PER_BLOCK = 5000;
  common::ObArenaAllocator allocator_;
  ObAllMicroBlockRangeIterator micro_block_iterator_;
  common::ObExtStoreRange range_;
  common::ObExtStoreRange micro_range_;
  ObMultipleScanMerge* scan_merge_;
  int64_t block_num_;
  int64_t row_cnt_from_cur_block_;
  bool has_open_block_;
  ObTableHandle table_handle_;
  ObTableAccessContext* access_ctx_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H */
