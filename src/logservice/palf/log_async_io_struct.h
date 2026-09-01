/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_STRUCT_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_STRUCT_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "log_define.h"
#include "lib/utility/ob_macro_utils.h"  // UNUSED
#include "share/ob_errno.h"              // OB_NOT_SUPPORTED
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
class IAsyncPalfIOCtx;

// A normal fragment can cover one complete follower group-buffer range. A
// wait-parent fragment is smaller to limit data blocked by one parent AIO.
static constexpr int64_t NORMAL_FRAGMENT_MAX_SIZE = FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE;
static constexpr int64_t WAIT_PARENT_FRAGMENT_MAX_SIZE = 2 * 1024 * 1024;
static constexpr int64_t FRAGMENT_SLOT_CNT_PER_PALF = 64;

// Stable reference to a fragment slot in the fixed pool. The generation
// number guards against stale completions reusing a recycled slot.
struct FragmentRef
{
  FragmentRef();
  FragmentRef(const int64_t slot_id, const int64_t generation)
    : slot_id(slot_id),
      generation(generation)
  {}
  ~FragmentRef() { reset(); }
  void reset();
  bool is_valid() const;
  bool is_equal(const FragmentRef &other) const;

  int64_t slot_id;
  int64_t generation;

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "%ld:%ld", slot_id, generation);
    return pos;
  }
};

// AsyncPalfIOCtx 传给底层异步写接口的只读请求. init() 后不再修改;
// source buffer 和 PALF ctx 都是借用关系, 其所有者必须保证二者存活到
// AIO 同步提交失败或异步完成.
class AsyncPwriteRequest
{
public:
  AsyncPwriteRequest();
  ~AsyncPwriteRequest() {}
  void reset();
  int init(const LSN &aligned_begin_lsn,
           const char *aligned_buf,
           int64_t aligned_buf_len,
           IAsyncPalfIOCtx *ctx,
           const FragmentRef &fragment_ref,
           int64_t submit_ts);
  bool is_valid() const;

  const LSN &get_aligned_begin_lsn() const { return aligned_begin_lsn_; }
  const char *get_aligned_buf() const { return aligned_buf_; }
  int64_t get_aligned_buf_len() const { return aligned_buf_len_; }
  IAsyncPalfIOCtx *get_ctx() const { return ctx_; }
  const FragmentRef &get_fragment_ref() const { return fragment_ref_; }
  int64_t get_submit_ts() const { return submit_ts_; }

  TO_STRING_KV(K_(aligned_begin_lsn), KP_(aligned_buf), K_(aligned_buf_len),
               KP_(ctx), K_(fragment_ref), K_(submit_ts));

private:
  LSN aligned_begin_lsn_;
  const char *aligned_buf_;
  int64_t aligned_buf_len_;
  IAsyncPalfIOCtx *ctx_;
  FragmentRef fragment_ref_;
  int64_t submit_ts_;
};

// 标识一次物理 AIO, 用于诊断和 fragment 完成处理. callback 对象单独 pin
// AsyncPalfIOCtx; FragmentRef generation 用于拒绝已经复用 slot 的旧 callback.
struct AsyncIOCallbackCtx
{
  AsyncIOCallbackCtx();
  ~AsyncIOCallbackCtx() { reset(); }
  void reset();
  bool is_valid() const;

  int64_t palf_id;          // diagnostic only
  FragmentRef fragment_ref;
  LSN begin_lsn;            // diagnostic only
  LSN end_lsn;              // diagnostic only
  int64_t submit_ts;        // diagnostic only

  TO_STRING_KV(K(palf_id), K(fragment_ref), K(begin_lsn), K(end_lsn), K(submit_ts));
};

// Completion event passed directly from the IOManager callback thread to the
// owning AsyncPalfIOCtx. The worker thread publishes logical tasks later.
struct AsyncIOCompletionEvent
{
  AsyncIOCompletionEvent();
  ~AsyncIOCompletionEvent() { reset(); }
  void reset();
  bool is_valid() const;

  AsyncIOCallbackCtx ctx;
  int ret_code;
  int64_t finish_ts;

  TO_STRING_KV(K(ctx), K(ret_code), K(finish_ts));
};


// State of a PhysicalWriteFragment in the fixed fragment pool.
enum class AsyncFragmentState
{
  FREE = 0,
  WAIT_PARENT = 1,
  READY = 2,
  SUBMITTED = 3,
  FINISHED = 4,
  FAILED = 5,
};

inline const char *async_fragment_state_to_string(const AsyncFragmentState state)
{
  const char *state_str = "UNKNOWN";
  switch (state) {
    case AsyncFragmentState::FREE:
      state_str = "FREE";
      break;
    case AsyncFragmentState::WAIT_PARENT:
      state_str = "WAIT_PARENT";
      break;
    case AsyncFragmentState::READY:
      state_str = "READY";
      break;
    case AsyncFragmentState::SUBMITTED:
      state_str = "SUBMITTED";
      break;
    case AsyncFragmentState::FINISHED:
      state_str = "FINISHED";
      break;
    case AsyncFragmentState::FAILED:
      state_str = "FAILED";
      break;
    default:
      break;
  }
  return state_str;
}

class IAsyncPalfIOCtx;


} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_STRUCT_
