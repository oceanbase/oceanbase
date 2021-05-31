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

#include "ob_range_skip.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/mvcc/ob_query_engine.h"

namespace oceanbase {
namespace storage {
int ObRangeSkip::inspect_gap(int idx, const bool is_memtable, uint8_t in_gap_flag, int64_t& range_idx,
    const ObStoreRowkey*& rowkey, int64_t limit)
{
  int ret = OB_SUCCESS;
  bool in_gap = (in_gap_flag & STORE_ITER_ROW_IN_GAP);
  bool big_gap_hint = (in_gap_flag & STORE_ITER_ROW_BIG_GAP_HINT);
  int64_t gap_size = 0;
  if (!in_gap) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!big_gap_hint) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObStoreRowIterator* iter = (typeof(iter))(*iters_)[idx];
    if (OB_FAIL(iter->get_gap_end(range_idx, rowkey, gap_size))) {
      STORAGE_LOG(WARN, "RangeSkip: fail", K(ret));
    } else if (gap_size < limit) {
      STORAGE_LOG(TRACE, "RangeSkip: gap_size too small", K(is_memtable), K(*rowkey), K(gap_size), K(idx));
      ret = OB_ENTRY_NOT_EXIST;
      if (is_memtable) {
        EVENT_INC(MEMSTORE_SMALL_GAP_COUNT);
        EVENT_ADD(MEMSTORE_SMALL_GAP_ROW_COUNT, gap_size);
      } else {
        EVENT_INC(SSSTORE_SMALL_GAP_COUNT);
        EVENT_ADD(SSSTORE_SMALL_GAP_ROW_COUNT, gap_size);
      }
    } else {
      STORAGE_LOG(TRACE, "RangeSkip: skip range", K(idx), K(is_memtable), K(range_idx), K(*rowkey), K(gap_size));
      if (is_memtable) {
        EVENT_INC(MEMSTORE_BIG_GAP_COUNT);
        EVENT_ADD(MEMSTORE_BIG_GAP_ROW_COUNT, gap_size);
      } else {
        EVENT_INC(SSSTORE_BIG_GAP_COUNT);
        EVENT_ADD(SSSTORE_BIG_GAP_ROW_COUNT, gap_size);
      }
    }
  }
  return ret;
}

};  // end namespace storage
};  // end namespace oceanbase
