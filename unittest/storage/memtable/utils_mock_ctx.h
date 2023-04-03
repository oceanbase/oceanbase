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

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_

#include "storage/memtable/ob_memtable_interface.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

struct ObMtCtxMembers
{
  char trace_log_buffer_[65536];
  int64_t tlb_pos_;
  CharArena allocator_;
  ObSEArray<ObIMvccCallback*, 64> scbs_;
  ObSEArray<ObIMvccCallback*, 64> tcbs_;
};

class ObMtCtx : public ObIMemtableCtx, public ObMtCtxMembers
{
public:
  ObMtCtx()
  {
  }
  ~ObMtCtx()
  {
  }
public:
  void fill_trace_log(const char *fmt, ...)
  {
    va_list args;
    va_start(args, fmt);
    tlb_pos_ += vsnprintf(&trace_log_buffer_[tlb_pos_], sizeof(trace_log_buffer_) - tlb_pos_, fmt, args);
    va_end(args);
  }
  void *alloc(const int64_t size) { return allocator_.alloc(size); }
  int register_sub_callback(ObIMvccCallback *callback)
  {
    int ret = OB_SUCCESS;
    scbs_.push_back(callback);
    return ret;
  }
  int register_end_callback(ObIMvccCallback *callback)
  {
    int ret = OB_SUCCESS;
    tcbs_.push_back(callback);
    return ret;
  }
  int fill_redo_log(
      const uint64_t table_id,
      const uint64_t index_id,
      const int64_t rowkey_len,
      const common::ObIArray<uint64_t> &column_ids,
      const storage::ObStoreRow &row)
  {
    int ret = OB_SUCCESS;
    UNUSED(table_id);
    UNUSED(index_id);
    UNUSED(rowkey_len);
    UNUSED(column_ids);
    UNUSED(row);
    return ret;
  }
public:
  int trans_begin()
  {
    return OB_SUCCESS;
  }
  int sub_trans_begin(const int64_t snapshot, const int64_t abs_expired_time)
  {
    assert(0 == scbs_.count());
    set_base_snapshot(INT64_MAX);
    set_start_snapshot(0);
    set_stop_snapshot(snapshot);
    set_abs_expired_time(abs_expired_time);
    return OB_SUCCESS;
  }
  void sub_trans_abort()
  {
    for (int64_t i = scbs_.count() - 1; i >= 0; i--) {
      if (NULL != scbs_.at(i)) {
        scbs_.at(i)->callback(false);
      }
    }
    scbs_.reset();
  }
  void trans_end(const bool commit, const int64_t redo_log_id)
  {
    set_redo_log_id(redo_log_id);
    for (int64_t i = scbs_.count() - 1; i >= 0; i--) {
      if (NULL != scbs_.at(i)) {
        scbs_.at(i)->callback(commit);
      }
    }
    scbs_.reset();
    for (int64_t i = tcbs_.count() - 1; i >= 0; i--) {
      if (NULL != tcbs_.at(i)) {
        tcbs_.at(i)->callback(commit);
      }
    }
    tcbs_.reset();
  }
  void trans_publish()
  {
  }
  void replay_end(const bool commit, const int64_t redo_log_id)
  {
    UNUSED(commit);
    UNUSED(redo_log_id);
  }
  int fill_redo_log(char *buf, const int64_t buf_len, const int64_t &buf_pos)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(buf_pos);
    return OB_SUCCESS;
  }
};

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_


