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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_IMPL_H_
#define OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_IMPL_H_

#include "lib/container/ob_loser_tree.h"
#include "storage/ob_i_store.h"
#include "storage/ob_row_fuse.h"
#include "storage/ob_multiple_merge.h"
#include "storage/ob_range_purger.h"
#include "storage/ob_range_skip.h"
#include "storage/ob_scan_merge_loser_tree.h"

namespace oceanbase {
namespace storage {
class ObQueryIteratorConsumer {
public:
  ObQueryIteratorConsumer()
  {
    reset();
  }
  ~ObQueryIteratorConsumer()
  {}
  void add_consumer(const int64_t iter_idx)
  {
    consumer_iters_[consumer_num_++] = iter_idx;
  }
  void set_consumer(const int64_t consumer_idx, const int64_t iter_idx)
  {
    consumer_iters_[consumer_idx] = iter_idx;
  }
  void set_base_iter_idx(const int64_t iter_idx)
  {
    base_sstable_iter_idx_ = iter_idx;
  }
  void reset()
  {
    consumer_num_ = 0;
    base_sstable_iter_idx_ = OB_INVALID_INDEX;
    memset(consumer_iters_, 0, MAX_TABLE_CNT_IN_STORAGE * sizeof(int64_t));
  }

  inline int64_t get_consumer_num()
  {
    return consumer_num_;
  }
  inline int64_t* get_consumer_iters()
  {
    return consumer_iters_;
  }
  inline void set_consumer_num(const int64_t consumer_num)
  {
    consumer_num_ = consumer_num;
  }
  OB_INLINE bool is_base_iter_only() const
  {
    return 1 == consumer_num_ && base_sstable_iter_idx_ == consumer_iters_[0];
  }

  // remove larger or equal to idx
  void remove_le(const int64_t idx)
  {
    int64_t valid_consume_num = 0;
    for (int64_t i = 0; i < consumer_num_; i++) {
      if (consumer_iters_[i] < idx) {
        consumer_iters_[valid_consume_num++] = consumer_iters_[i];
      }
    }
    consumer_num_ = valid_consume_num;
  }
  TO_STRING_KV(
      "consumers", ObArrayWrap<int64_t>(consumer_iters_, consumer_num_), K_(consumer_num), K_(base_sstable_iter_idx));

private:
  int64_t consumer_iters_[common::MAX_TABLE_CNT_IN_STORAGE];
  int64_t consumer_num_;
  int64_t base_sstable_iter_idx_;
};

class ObMultipleScanMergeImpl : public ObMultipleMerge {
public:
  ObMultipleScanMergeImpl();
  virtual ~ObMultipleScanMergeImpl();

  virtual int init(
      const ObTableAccessParam& param, ObTableAccessContext& context, const ObGetTableParam& get_table_param) override;
  virtual int inner_get_next_row(ObStoreRow& row) override;
  virtual void reset() override;
  virtual void reuse() override;
  inline bool is_scan_end() const
  {
    return loser_tree_.empty();
  }

protected:
  int reset_range(int idx, int64_t range_idx, const ObStoreRowkey* rowkey, const bool include_gap_key);
  int supply_consume();
  int prepare_range_skip();
  int inner_get_next_row(ObStoreRow& row, bool& need_retry);
  int prepare_loser_tree();

private:
  int try_skip_range(const ObStoreRow* row, int idx, uint8_t flag, bool first_pop, bool& skipped);

protected:
  ObScanMergeLoserTreeCmp tree_cmp_;
  ObScanMergeLoserTree loser_tree_;
  bool iter_del_row_;
  ObQueryIteratorConsumer consumer_;
  bool try_push_top_item_;
  ObRangePurger range_purger_;
  ObRangeSkip range_skip_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultipleScanMergeImpl);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_IMPL_H_
