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

#ifndef OCEANBASE_STORAGE_OB_RANGE_SKIP_H_
#define OCEANBASE_STORAGE_OB_RANGE_SKIP_H_
#include "ob_multiple_merge.h"

namespace oceanbase {
namespace storage {
class ObRangeSkip {
public:
  ObRangeSkip() : iters_(NULL)
  {}
  ~ObRangeSkip()
  {}
  void reset()
  {
    iters_ = NULL;
  }
  bool is_enabled() const
  {
    return NULL != iters_;
  }
  void init(ObMultipleMerge::MergeIterators* iters)
  {
    iters_ = iters;
  }
  int inspect_gap(int idx, const bool is_memtable, uint8_t in_gap_flag, int64_t& range_idx,
      const ObStoreRowkey*& rowkey, int64_t limit);

private:
  ObMultipleMerge::MergeIterators* iters_;
};

};  // end namespace storage
};  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_RANGE_SKIP_H_ */
