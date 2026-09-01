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

#define USING_LOG_PREFIX PALF

#include "log_async_io_struct.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace palf
{

// ---- FragmentRef ----
FragmentRef::FragmentRef() : slot_id(-1), generation(-1)
{
  reset();
}

void FragmentRef::reset()
{
  slot_id = -1;
  generation = -1;
}

bool FragmentRef::is_valid() const
{
  return slot_id >= 0 && generation >= 0;
}

bool FragmentRef::is_equal(const FragmentRef &other) const
{
  return slot_id == other.slot_id && generation == other.generation;
}

// ---- AsyncPwriteRequest ----
AsyncPwriteRequest::AsyncPwriteRequest()
  : aligned_begin_lsn_(),
    aligned_buf_(NULL),
    aligned_buf_len_(0),
    ctx_(NULL),
    fragment_ref_(),
    submit_ts_(OB_INVALID_TIMESTAMP)
{
}

void AsyncPwriteRequest::reset()
{
  aligned_begin_lsn_.reset();
  aligned_buf_ = NULL;
  aligned_buf_len_ = 0;
  ctx_ = NULL;
  fragment_ref_.reset();
  submit_ts_ = OB_INVALID_TIMESTAMP;
}

int AsyncPwriteRequest::init(const LSN &aligned_begin_lsn,
                             const char *aligned_buf,
                             const int64_t aligned_buf_len,
                             IAsyncPalfIOCtx *ctx,
                             const FragmentRef &fragment_ref,
                             const int64_t submit_ts)
{
  int ret = OB_SUCCESS;
  reset();
  if (!aligned_begin_lsn.is_valid()
      || OB_ISNULL(aligned_buf)
      || aligned_buf_len <= 0
      || OB_ISNULL(ctx)
      || !fragment_ref.is_valid()
      || submit_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid async pwrite request init argument",
             K(ret), K(aligned_begin_lsn), KP(aligned_buf), K(aligned_buf_len),
             KP(ctx), K(fragment_ref), K(submit_ts));
  } else {
    aligned_begin_lsn_ = aligned_begin_lsn;
    aligned_buf_ = aligned_buf;
    aligned_buf_len_ = aligned_buf_len;
    ctx_ = ctx;
    fragment_ref_ = fragment_ref;
    submit_ts_ = submit_ts;
  }
  return ret;
}

bool AsyncPwriteRequest::is_valid() const
{
  return aligned_begin_lsn_.is_valid()
      && OB_NOT_NULL(aligned_buf_)
      && aligned_buf_len_ > 0
      && OB_NOT_NULL(ctx_)
      && fragment_ref_.is_valid()
      && submit_ts_ > 0;
}

// ---- AsyncIOCallbackCtx ----
AsyncIOCallbackCtx::AsyncIOCallbackCtx()
  : palf_id(-1),
    fragment_ref(),
    begin_lsn(),
    end_lsn(),
    submit_ts(OB_INVALID_TIMESTAMP)
{
  reset();
}

void AsyncIOCallbackCtx::reset()
{
  palf_id = -1;
  fragment_ref.reset();
  begin_lsn.reset();
  end_lsn.reset();
  submit_ts = OB_INVALID_TIMESTAMP;
}

bool AsyncIOCallbackCtx::is_valid() const
{
  return fragment_ref.is_valid();
}

// ---- AsyncIOCompletionEvent ----
AsyncIOCompletionEvent::AsyncIOCompletionEvent()
  : ctx(),
    ret_code(0),
    finish_ts(0)
{
  reset();
}

void AsyncIOCompletionEvent::reset()
{
  ctx.reset();
  ret_code = 0;
  finish_ts = 0;
}

bool AsyncIOCompletionEvent::is_valid() const
{
  // 失败 AIO 同样需要完成处理. 这里只要求 fragment 身份和完成时间有效;
  // ret_code 有意允许错误码.
  return ctx.is_valid() && finish_ts > 0;
}

} // end namespace palf
} // end namespace oceanbase
