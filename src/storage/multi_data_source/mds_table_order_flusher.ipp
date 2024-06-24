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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_IPP
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_IPP

#include "lib/ob_errno.h"
#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H_IPP
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_HIGH_PRIORITY_FLUSHER_H_IPP
#include "mds_table_order_flusher.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{

// this is optimized for get_rec_scn() operation
template <>
inline bool MdsTableHighPriorityFlusher<FLUSH_FOR_ONE_SIZE>::try_record_mds_table(FlushKey new_key, FlushKey &eliminated_key) {
  if (OB_UNLIKELY(size_ == 0)) {
    size_ = 1;
    high_priority_mds_tables_[0] = new_key;
  } else {
    if (new_key < high_priority_mds_tables_[0]) {
      high_priority_mds_tables_[0] = new_key;
    }
  }
  return true;
}

template <int64_t STACK_QUEUE_SIZE>
inline bool MdsTableHighPriorityFlusher<STACK_QUEUE_SIZE>::try_record_mds_table(FlushKey new_key, FlushKey &eliminated_key) {
  bool need_insert = false;
  if (size_ < STACK_QUEUE_SIZE) {
    need_insert = true;
  } else if (new_key < high_priority_mds_tables_[STACK_QUEUE_SIZE - 1]) {
    need_insert = true;
  }
  if (need_insert) {
    FlushKey *begin = &high_priority_mds_tables_[0];
    FlushKey *end = &high_priority_mds_tables_[size_ >= STACK_QUEUE_SIZE ? STACK_QUEUE_SIZE : size_];
    FlushKey *iter = std::upper_bound(begin, end, new_key);
    if (end == &high_priority_mds_tables_[STACK_QUEUE_SIZE]) {
      eliminated_key = high_priority_mds_tables_[STACK_QUEUE_SIZE - 1];
      MDS_ASSERT(iter < end);
      memmove(iter + 1, iter, (end - iter - 1) * sizeof(FlushKey));
    } else {// maybe end == iter
      memmove(iter + 1, iter, (end - iter) * sizeof(FlushKey));
      ++size_;
    }
    *iter = new_key;
  }
  return need_insert;
}

template <int64_t STACK_QUEUE_SIZE>
inline void MdsTableHighPriorityFlusher<STACK_QUEUE_SIZE>::flush_by_order(MdsTableMap &map, share::SCN limit, share::SCN max_decided_scn) {
  int ret = OB_SUCCESS;
  int64_t flush_count = 0;
  FlushOp op(limit, flush_count, max_decided_scn);
  for (int64_t i = 0; i < size_ && !op.dag_full(); ++i) {// ignore ERROR
    if (OB_FAIL(map.operate(high_priority_mds_tables_[i].tablet_id_, op))) {
      MDS_LOG(WARN, "fail to operate mds table", K(i), K(high_priority_mds_tables_[i]), KR(ret), K(limit), K(max_decided_scn));
    }
  }
}

template <int64_t STACK_QUEUE_SIZE>
int64_t MdsTableHighPriorityFlusher<STACK_QUEUE_SIZE>::to_string(char *buf, const int64_t len) const {
  int64_t pos = 0;
  constexpr int64_t MAX_PRINT_NUMBER = 32L;
  databuff_printf(buf, len, pos, "size:%ld, ordered_tablets:{", size_);
  int64_t print_numnber = std::min(size_, MAX_PRINT_NUMBER);
  for (int64_t i = 0; i < print_numnber; ++i) {
    databuff_printf(buf, len, pos, "%s", to_cstring(high_priority_mds_tables_[i]));
    if (i != print_numnber) {
      databuff_printf(buf, len, pos, ", ");
    } else {
      if (print_numnber < size_) {
        databuff_printf(buf, len, pos, "%ld more...}", size_ - print_numnber);
      } else {
        databuff_printf(buf, len, pos, "}");
      }
    }
  }
  return pos;
}

template <int64_t STACK_QUEUE_SIZE>
void MdsTableOrderFlusher<STACK_QUEUE_SIZE, true>::reserve_memory(int64_t mds_table_total_size_likely) {// it'ok if failed
  int ret = OB_SUCCESS;
  constexpr int64_t max_tablet_number = 1_MB;//sizeof(100w FlushKey) = 16MB
  int64_t reserve_size = std::min(mds_table_total_size_likely * 2, max_tablet_number);
  if (OB_FAIL(extra_mds_tables_.reserve(reserve_size))) {
    MDS_LOG(WARN, "fail to reserve memory", KR(ret));
    array_err_ = ret;
  }
}

template <int64_t STACK_QUEUE_SIZE>
void MdsTableOrderFlusher<STACK_QUEUE_SIZE, true>::record_mds_table(FlushKey key) {// it's ok if failed
  int ret = OB_SUCCESS;
  FlushKey eliminated_key;
  if (!high_priority_flusher_.try_record_mds_table(key, eliminated_key)) {
    eliminated_key = key;
  }
  if (eliminated_key.is_valid() && !array_err_) {
    if (OB_FAIL(extra_mds_tables_.push_back(eliminated_key))) {
      MDS_LOG(WARN, "fail to push_back", KR(ret), K(eliminated_key));
      array_err_ = ret;
    }
  }
}

template <int64_t STACK_QUEUE_SIZE>
void MdsTableOrderFlusher<STACK_QUEUE_SIZE, false>::record_mds_table(FlushKey key) {// won't be failed
  FlushKey _;
  high_priority_flusher_.try_record_mds_table(key, _);
}

template <int64_t STACK_QUEUE_SIZE>
void MdsTableOrderFlusher<STACK_QUEUE_SIZE, true>::flush_by_order(MdsTableMap &map,
                                                                  share::SCN limit,
                                                                  share::SCN max_decided_scn) {
  int ret = OB_SUCCESS;
  int64_t flush_count = 0;
  FlushOp op(limit, flush_count, max_decided_scn);
  high_priority_flusher_.flush_by_order(map, limit, max_decided_scn);
  if (!array_err_) {// if array has error, just order flush STACK_QUEUE_SIZE mds tables
    try {
      lib::ob_sort(extra_mds_tables_.begin(), extra_mds_tables_.end());
    } catch (std::exception e) {
      MDS_LOG(WARN, "std::sort failed", K(e.what()));
      array_err_ = OB_ERR_UNEXPECTED;
    }
    if (!array_err_) {
      for (int64_t idx = 0; idx < extra_mds_tables_.count() && !op.dag_full(); ++idx) {
        if (OB_FAIL(map.operate(extra_mds_tables_[idx].tablet_id_, op))) {
          MDS_LOG(WARN, "fail to operate mds table", K(idx), K(extra_mds_tables_[idx]), KR(ret), K(limit), K(max_decided_scn));
        }
      }
    }
  }
}

template <int64_t STACK_QUEUE_SIZE>
void MdsTableOrderFlusher<STACK_QUEUE_SIZE, false>::flush_by_order(MdsTableMap &map,
                                                                   share::SCN limit,
                                                                   share::SCN max_decided_scn) {
  high_priority_flusher_.flush_by_order(map, limit, max_decided_scn);
}

}
}
}
#endif