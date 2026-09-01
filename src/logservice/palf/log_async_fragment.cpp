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

#include "log_async_fragment.h"
#include <cstdlib>
#include <cstring>
#include "share/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace palf
{

using namespace common;

//===----------------------------------------------------------------------===//
// PhysicalWriteFragment
//===----------------------------------------------------------------------===//

PhysicalWriteFragment::PhysicalWriteFragment()
  : slot_id_(-1),
    generation_(-1),
    lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    state_(AsyncFragmentState::FREE),
    buf_(NULL),
    begin_lsn_(),
    end_lsn_(),
    fragment_max_size_(0),
    parent_(),
    wait_parent_since_ts_(OB_INVALID_TIMESTAMP),
    ret_code_(OB_SUCCESS),
    next_retry_ts_(0),
    submit_ts_(OB_INVALID_TIMESTAMP),
    finish_ts_(OB_INVALID_TIMESTAMP)
{
}

PhysicalWriteFragment::~PhysicalWriteFragment()
{
  reset();
}

DEF_TO_STRING(PhysicalWriteFragment)
{
  int64_t pos = 0;
  const AsyncFragmentState state = get_state();
  J_OBJ_START();
  if (AsyncFragmentState::FREE == state) {
    J_KV(K_(slot_id), "generation_id", generation_);
  } else {
    J_KV(K_(slot_id), K_(generation), "state", async_fragment_state_to_string(state),
         KP_(buf), K_(begin_lsn), K_(end_lsn), K_(fragment_max_size), K_(parent),
         K_(wait_parent_since_ts), K_(ret_code), K_(next_retry_ts), K_(submit_ts), K_(finish_ts));
  }
  J_OBJ_END();
  return pos;
}

bool PhysicalWriteFragment::is_data_valid() const
{
  return OB_NOT_NULL(buf_) && begin_lsn_.is_valid() && end_lsn_.is_valid()
         && begin_lsn_ < end_lsn_ && fragment_max_size_ > 0
         && end_lsn_ - begin_lsn_ <= fragment_max_size_
         && 0 == lsn_2_offset(begin_lsn_, PALF_BLOCK_SIZE) % LOG_DIO_ALIGN_SIZE
         && 0 == static_cast<int64_t>(reinterpret_cast<uintptr_t>(buf_) % LOG_DIO_ALIGN_SIZE);
}

void PhysicalWriteFragment::reset()
{
  // reset() 保留 slot 身份; buf_ 只借用上层 log buffer, 每次 reset 都必须清空.
  buf_ = NULL;
  begin_lsn_.reset();
  end_lsn_.reset();
  fragment_max_size_ = 0;
  {
    common::ObSpinLockGuard guard(lock_);
    state_ = AsyncFragmentState::FREE;
    parent_.reset();
    wait_parent_since_ts_ = OB_INVALID_TIMESTAMP;
    ret_code_ = OB_SUCCESS;
    next_retry_ts_ = 0;
    submit_ts_ = OB_INVALID_TIMESTAMP;
    finish_ts_ = OB_INVALID_TIMESTAMP;
  }
  // reset 只能发生在 worker 已观察到 AIO 完成, 或 ctx 已排空 inflight 之后.
  // 此处释放 SUBMITTED 期间保留的 out-ref, 不能在 IOManager callback 线程调用.
  io_handle_.reset();
}

int PhysicalWriteFragment::get_data_len(int64_t &data_len) const
{
  int ret = OB_SUCCESS;
  data_len = 0;
  if (!is_data_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid fragment data",
             K(ret), K_(buf), K_(begin_lsn), K_(end_lsn), KPC(this));
  } else {
    data_len = end_lsn_ - begin_lsn_;
  }
  return ret;
}

int PhysicalWriteFragment::get_wait_parent_stat(const int64_t now_us,
                                                int64_t &wait_us,
                                                int64_t &data_len) const
{
  int ret = OB_SUCCESS;
  wait_us = 0;
  data_len = 0;
  common::ObSpinLockGuard guard(lock_);
  if (AsyncFragmentState::WAIT_PARENT != state_ || !is_data_valid() || wait_parent_since_ts_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid wait-parent fragment stat",
             K(ret), K(now_us), K_(slot_id), K_(generation), K_(state),
             K_(buf), K_(begin_lsn), K_(end_lsn), K_(wait_parent_since_ts));
  } else {
    wait_us = now_us > wait_parent_since_ts_ ? now_us - wait_parent_since_ts_ : 0;
    data_len = end_lsn_ - begin_lsn_;
  }
  return ret;
}

FragmentRef PhysicalWriteFragment::get_fragment_ref() const
{
  common::ObSpinLockGuard guard(lock_);
  return FragmentRef(slot_id_, generation_);
}

int PhysicalWriteFragment::check_append_source_(const LSN &begin_lsn, const int64_t len, const char *buf) const
{
  int ret = OB_SUCCESS;
  const LSN end_lsn = begin_lsn + static_cast<offset_t>(len);
  int64_t appendable_len = 0;
  int64_t data_len = 0;
  if (!begin_lsn.is_valid() || len <= 0 || OB_ISNULL(buf) || !end_lsn.is_valid()
      || !is_data_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid fragment append source",
             KR(ret), K(begin_lsn), K(len), KP(buf), K(end_lsn), KPC(this));
  } else if (OB_FAIL(get_appendable_data_len(appendable_len))) {
    PALF_LOG(ERROR, "get fragment appendable len failed", KR(ret), K(begin_lsn), K(len), KPC(this));
  } else if (OB_FAIL(get_data_len(data_len))) {
    PALF_LOG(ERROR, "get fragment data len failed", KR(ret), K(begin_lsn), K(len), KPC(this));
  } else if (!is_appendable()
             || begin_lsn != end_lsn_
             || len > appendable_len
             || buf_ + data_len != buf) {
    // 只有 WAIT_PARENT fragment 可以继续追加. 新数据必须同时满足 LSN 连续、
    // group buffer 地址连续, 且追加后不超过本 fragment 的容量.
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async fragment cannot append source",
             KR(ret), K(begin_lsn), K(len), K(appendable_len), K(data_len), KP(buf), KPC(this));
  }
  return ret;
}

int PhysicalWriteFragment::check_init_source_(const LSN &begin_lsn,
                                                   const char *buf,
                                                   const int64_t max_len,
                                                   const int64_t fragment_max_size) const
{
  int ret = OB_SUCCESS;
  const LSN end_lsn = begin_lsn + static_cast<offset_t>(max_len);
  const int64_t buf_off = static_cast<int64_t>(reinterpret_cast<uintptr_t>(buf) % LOG_DIO_ALIGN_SIZE);
  if (!begin_lsn.is_valid() || max_len <= 0 || OB_ISNULL(buf) || !end_lsn.is_valid()
      || fragment_max_size <= 0 || 0 != buf_off
      || 0 != lsn_2_offset(begin_lsn, PALF_BLOCK_SIZE) % LOG_DIO_ALIGN_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid fragment init source",
             KR(ret), K(begin_lsn), K(max_len), KP(buf), K(end_lsn), K(fragment_max_size), KPC(this));
  }
  return ret;
}

int PhysicalWriteFragment::append_source(const LSN &begin_lsn,
                                         const int64_t len,
                                         const char *buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_append_source_(begin_lsn, len, buf))) {
    PALF_LOG(ERROR, "check append fragment source failed",
             KR(ret), K(begin_lsn), K(len), KP(buf), KPC(this));
  } else {
    end_lsn_ = begin_lsn + static_cast<offset_t>(len);
  }
  return ret;
}

int PhysicalWriteFragment::transition_state_locked_(const FragmentRef &ref,
                                                    const AsyncFragmentState next_state)
{
  int ret = OB_SUCCESS;
  bool allowed = false;
  const AsyncFragmentState curr_state = state_;
  if (!ref.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(TRACE, "invalid physical fragment ref before state transition",
             K(ret), K(ref), K_(slot_id), K_(generation), K(curr_state), K(next_state));
  } else if (ref.slot_id != slot_id_ || ref.generation != generation_) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(TRACE, "stale physical fragment ref before state transition",
             K(ret), K(ref), K_(slot_id), K_(generation), K(curr_state), K(next_state));
  } else if (AsyncFragmentState::READY == next_state) {
    allowed = (AsyncFragmentState::WAIT_PARENT == curr_state);
  } else if (AsyncFragmentState::SUBMITTED == next_state) {
    allowed = (AsyncFragmentState::READY == curr_state || AsyncFragmentState::FAILED == curr_state);
  } else if (AsyncFragmentState::FAILED == next_state) {
    allowed = (AsyncFragmentState::SUBMITTED == curr_state);
  } else if (AsyncFragmentState::FINISHED == next_state) {
    allowed = (AsyncFragmentState::SUBMITTED == curr_state);
  } else {
    allowed = false;
  }
  if (OB_SUCC(ret) && !allowed) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "invalid physical fragment state transition",
             K(ret), K_(slot_id), K_(generation), K(curr_state), K(next_state),
             K_(parent));
  } else if (OB_SUCC(ret)) {
    state_ = next_state;
  }
  return ret;
}

int PhysicalWriteFragment::mark_ready(const FragmentRef &ref)
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_FAIL(transition_state_locked_(ref, AsyncFragmentState::READY))) {
    PALF_LOG(TRACE, "transition fragment to ready failed",
             K(ret), K(ref), K_(slot_id), K_(generation), K_(state));
  } else {
    parent_.reset();
    wait_parent_since_ts_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

int PhysicalWriteFragment::mark_submitted(const FragmentRef &ref, const int64_t submit_ts)
{
  common::ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (submit_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(TRACE, "invalid fragment submit timestamp",
             K(ret), K(ref), K(submit_ts), K_(slot_id), K_(generation), K_(state));
  } else if (OB_FAIL(transition_state_locked_(ref, AsyncFragmentState::SUBMITTED))) {
    PALF_LOG(TRACE, "transition fragment to submitted failed",
             K(ret), K(ref), K_(slot_id), K_(generation), K_(state));
  } else {
    submit_ts_ = submit_ts;
    finish_ts_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

int PhysicalWriteFragment::mark_failed(const FragmentRef &ref,
                                              const int ret_code,
                                              const int64_t next_retry_ts)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_FAIL(transition_state_locked_(ref, AsyncFragmentState::FAILED))) {
    PALF_LOG(TRACE, "transition fragment to failed failed",
             K(ret), K(ref), K(ret_code), K(next_retry_ts),
             K_(slot_id), K_(generation), K_(state));
  } else {
    ret_code_ = ret_code;
    next_retry_ts_ = next_retry_ts;
    finish_ts_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

int PhysicalWriteFragment::mark_io_completed(const FragmentRef &ref,
                                             const int ret_code,
                                             const int64_t next_retry_ts,
                                             const int64_t finish_ts,
                                             bool &completed_by_me,
                                             int64_t &completed_data_len,
                                             int64_t &submit_ts)
{
  int ret = OB_SUCCESS;
  completed_by_me = false;
  completed_data_len = 0;
  submit_ts = OB_INVALID_TIMESTAMP;
  common::ObSpinLockGuard guard(lock_);
  if (!ref.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(TRACE, "invalid physical fragment ref before mark io completed",
             K(ret), K(ref), K(ret_code), K(next_retry_ts),
             K_(slot_id), K_(generation), K_(state));
  } else if (finish_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid physical fragment finish timestamp",
             KR(ret), K(ref), K(ret_code), K(next_retry_ts), K(finish_ts),
             K_(slot_id), K_(generation), K_(state));
  } else if (ref.slot_id != slot_id_ || ref.generation != generation_) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(TRACE, "stale physical fragment ref before mark io completed",
             K(ret), K(ref), K(ret_code), K(next_retry_ts),
             K_(slot_id), K_(generation), K_(state));
  } else if (AsyncFragmentState::SUBMITTED != state_) {
    // callback 和 worker poll 可能同时观察到完成. 状态已不是 SUBMITTED 表示
    // 另一方已经赢得 close-once 竞争, 当前调用不能重复推进状态和 inflight.
  } else if (OB_SUCCESS == ret_code
             && OB_FAIL(transition_state_locked_(ref, AsyncFragmentState::FINISHED))) {
    PALF_LOG(TRACE, "transition fragment to finished failed",
             K(ret), K(ref), K(ret_code),
             K_(slot_id), K_(generation), K_(state));
  } else if (OB_SUCCESS != ret_code
             && OB_FAIL(transition_state_locked_(ref, AsyncFragmentState::FAILED))) {
    PALF_LOG(TRACE, "transition completed fragment to failed failed",
             K(ret), K(ref), K(ret_code), K(next_retry_ts),
             K_(slot_id), K_(generation), K_(state));
  } else {
    ret_code_ = ret_code;
    if (OB_SUCCESS != ret_code) {
      next_retry_ts_ = next_retry_ts;
      finish_ts_ = OB_INVALID_TIMESTAMP;
    } else {
      finish_ts_ = finish_ts;
    }
    completed_data_len = end_lsn_ - begin_lsn_;
    submit_ts = submit_ts_;
    completed_by_me = true;
  }
  return ret;
}

int PhysicalWriteFragment::get_remaining_finish_delay(
    const int64_t now_us,
    const int64_t aio_delay_us,
    int64_t &remaining_delay_us) const
{
  int ret = OB_SUCCESS;
  remaining_delay_us = 0;
  common::ObSpinLockGuard guard(lock_);
  if (now_us <= 0 || aio_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async fragment finish delay argument",
             KR(ret), K(now_us), K(aio_delay_us), K_(slot_id), K_(generation), K_(state));
  } else if (AsyncFragmentState::FINISHED != state_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(ERROR, "get finish delay from non-finished async fragment",
             KR(ret), K(now_us), K(aio_delay_us), K_(slot_id), K_(generation), K_(state));
  } else if (aio_delay_us > 0 && finish_ts_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "finished async fragment has invalid finish timestamp",
             KR(ret), K(now_us), K(aio_delay_us), K_(slot_id), K_(generation), K_(finish_ts));
  } else if (aio_delay_us > 0) {
    const int64_t elapsed_us = now_us > finish_ts_ ? now_us - finish_ts_ : 0;
    remaining_delay_us = elapsed_us >= aio_delay_us ? 0 : aio_delay_us - elapsed_us;
  }
  return ret;
}

int PhysicalWriteFragment::alloc_from_free(const int64_t slot_id, const LSN &begin_lsn, const char *buf,
                                           const int64_t max_len, const int64_t fragment_max_size,
                                           const FragmentRef &parent, FragmentRef &ref,
                                           int64_t &planned_len)
{
  int ret = OB_SUCCESS;
  int64_t prefix_len = 0;
  LSN fragment_begin_lsn;
  const char *fragment_buf = NULL;
  ref.reset();
  planned_len = 0;
  if (slot_id < 0 || !begin_lsn.is_valid() || OB_ISNULL(buf) || max_len <= 0 || fragment_max_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid fragment pool slot argument", K(ret), K(slot_id), K(begin_lsn), KP(buf),
             K(max_len), K(fragment_max_size));
  } else if (FALSE_IT(prefix_len = begin_lsn.val_ % LOG_DIO_ALIGN_SIZE)) {
  } else if (prefix_len < 0 || prefix_len >= fragment_max_size) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid fragment source prefix", K(ret), K(slot_id), K(prefix_len), K(fragment_max_size));
  } else if (FALSE_IT(fragment_begin_lsn = begin_lsn - static_cast<offset_t>(prefix_len))) {
  } else if (FALSE_IT(fragment_buf = buf - prefix_len)) {
  } else if (OB_FAIL(check_init_source_(fragment_begin_lsn, fragment_buf, max_len, fragment_max_size))) {
    PALF_LOG(WARN, "invalid fragment init source while allocating slot", KR(ret), K(slot_id));
  } else {
    // fragment 从 DIO 页首开始写, 因此 LSN 和指针都回退 prefix_len;
    // planned_len 只统计本轮新消费的数据, 不包含已经存在的页前缀.
    planned_len = MIN(max_len, fragment_max_size - prefix_len);
    buf_ = fragment_buf;
    begin_lsn_ = fragment_begin_lsn;
    end_lsn_ = begin_lsn + static_cast<offset_t>(planned_len);
    fragment_max_size_ = fragment_max_size;
    {
      common::ObSpinLockGuard guard(lock_);
      slot_id_ = slot_id;
      if (generation_ < 0) {
        generation_ = 0;
      }
      const FragmentRef new_ref(slot_id_, generation_);
      if (AsyncFragmentState::FREE != state_) {
        ret = OB_STATE_NOT_MATCH;
        PALF_LOG(WARN, "fragment slot is not free before allocation",
                 KR(ret), K(slot_id), K_(slot_id), K_(generation), K_(state));
      } else {
        if (parent.is_valid()) {
          parent_ = parent;
          wait_parent_since_ts_ = common::ObTimeUtility::current_time();
          state_ = AsyncFragmentState::WAIT_PARENT;
        } else {
          state_ = AsyncFragmentState::READY;
        }
        ref = new_ref;
      }
    }
    if (OB_SUCCESS != ret) {
      buf_ = NULL;
      begin_lsn_.reset();
      end_lsn_.reset();
      fragment_max_size_ = 0;
      planned_len = 0;
    }
  }
  return ret;
}

int PhysicalWriteFragment::recycle_slot()
{
  int ret = OB_SUCCESS;
  buf_ = NULL;
  begin_lsn_.reset();
  end_lsn_.reset();
  fragment_max_size_ = 0;
  {
    common::ObSpinLockGuard guard(lock_);
    parent_.reset();
    wait_parent_since_ts_ = OB_INVALID_TIMESTAMP;
    ret_code_ = OB_SUCCESS;
    next_retry_ts_ = 0;
    submit_ts_ = OB_INVALID_TIMESTAMP;
    finish_ts_ = OB_INVALID_TIMESTAMP;
    state_ = AsyncFragmentState::FREE;
    ++generation_;
  }
  io_handle_.reset();
  return ret;
}

int PhysicalWriteFragment::get_appendable_data_len(int64_t &appendable_len) const
{
  int ret = OB_SUCCESS;
  const LSN end_lsn = get_end_lsn();
  int64_t data_len = 0;
  appendable_len = 0;
  if (!is_data_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(TRACE, "invalid fragment range for append length",
             K(ret), K(end_lsn), K_(begin_lsn), KPC(this));
  } else if (OB_FAIL(get_data_len(data_len))) {
    PALF_LOG(TRACE, "get fragment data len failed",
             K(ret), K(end_lsn), K_(begin_lsn), KPC(this));
  } else {
    appendable_len = fragment_max_size_ - data_len;
    if (appendable_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid fragment appendable length",
               K(ret), K(end_lsn), K(appendable_len), KPC(this));
    }
  }
  return ret;
}

PhysicalWriteFragmentFilter::~PhysicalWriteFragmentFilter()
{
}

PhysicalWriteFragmentStateFilter::PhysicalWriteFragmentStateFilter(const AsyncFragmentState state)
  : state_(state)
{
}

PhysicalWriteFragmentStateFilter::~PhysicalWriteFragmentStateFilter()
{
}

bool PhysicalWriteFragmentStateFilter::operator()(const PhysicalWriteFragment &fragment) const
{
  return fragment.is_state(state_);
}

//===----------------------------------------------------------------------===//
// PhysicalWriteFragmentPool
//===----------------------------------------------------------------------===//

PhysicalWriteFragmentPool::PhysicalWriteFragmentPool()
  : is_inited_(false)
{
}

PhysicalWriteFragmentPool::~PhysicalWriteFragmentPool()
{
  destroy();
}

int PhysicalWriteFragmentPool::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "fragment pool init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void PhysicalWriteFragmentPool::reuse()
{
  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
    (void) slots_[i].recycle_slot();
  }
}

void PhysicalWriteFragmentPool::destroy()
{
  reuse();
  is_inited_ = false;
}

int PhysicalWriteFragmentPool::alloc_slot(const LSN &begin_lsn, const char *buf, const int64_t max_len,
                                          const int64_t fragment_max_size,
                                          const FragmentRef &parent, FragmentRef &ref,
                                          int64_t &planned_len)
{
  int ret = OB_SUCCESS;
  ref.reset();
  planned_len = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "alloc fragment slot before pool init", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      if (slots_[i].is_free()) {
        slots_[i].reset();   // clears write range and runtime state, keeps slot id + generation.
        if (OB_FAIL(slots_[i].alloc_from_free(i, begin_lsn, buf, max_len, fragment_max_size,
                                               parent, ref, planned_len))) {
          PALF_LOG(WARN, "alloc fragment slot from free slot failed", K(ret), K(i));
        } else {
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_SIZE_OVERFLOW;
      PALF_LOG(TRACE, "fragment pool has no free slot", K(ret));
    }
  }
  return ret;
}

int PhysicalWriteFragmentPool::free_slot(const FragmentRef &ref)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "free fragment slot before pool init", K(ret), K(ref));
  } else if (ref.slot_id < 0 || ref.slot_id >= FRAGMENT_SLOT_CNT_PER_PALF) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid fragment slot ref to free", K(ret), K(ref));
  } else {
    PhysicalWriteFragment &slot = slots_[ref.slot_id];
    if (slot.get_generation() != ref.generation) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(TRACE, "fragment slot generation changed before free",
               K(ret), K(ref), K(slot));
    } else if (OB_FAIL(slot.recycle_slot())) {
      PALF_LOG(WARN, "recycle fragment slot failed", K(ret), K(ref));
    }
  }
  return ret;
}

int PhysicalWriteFragmentPool::get_fragment(const FragmentRef &ref, PhysicalWriteFragment *&fragment)
{
  int ret = OB_SUCCESS;
  const PhysicalWriteFragment *const_fragment = NULL;
  fragment = NULL;
  ret = get_fragment_(ref.slot_id, ref.generation, const_fragment);
  if (OB_SUCCESS == ret) {
    fragment = const_cast<PhysicalWriteFragment *>(const_fragment);
  }
  return ret;
}

int PhysicalWriteFragmentPool::get_fragment(const FragmentRef &ref,
                                            const PhysicalWriteFragment *&fragment) const
{
  return get_fragment_(ref.slot_id, ref.generation, fragment);
}

int PhysicalWriteFragmentPool::get_fragment_(const int64_t slot_id,
                                             const int64_t generation,
                                             const PhysicalWriteFragment *&fragment) const
{
  int ret = OB_SUCCESS;
  fragment = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "get fragment before pool init",
             K(ret), K(slot_id), K(generation));
  } else if (slot_id < 0 || slot_id >= FRAGMENT_SLOT_CNT_PER_PALF) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(TRACE, "invalid fragment slot id",
             K(ret), K(slot_id), K(generation));
  } else {
    const PhysicalWriteFragment &slot = slots_[slot_id];
    if (slot.get_generation() != generation) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(TRACE, "fragment generation mismatch",
               K(ret), K(slot_id), K(generation), K(slot));
    } else {
      fragment = &slot;
    }
  }
  return ret;
}

int64_t PhysicalWriteFragmentPool::get_used_slot_count() const
{
  int64_t used_count = 0;
  if (is_inited_) {
    for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      if (!slots_[i].is_free()) {
        ++used_count;
      }
    }
  }
  return used_count;
}

void PhysicalWriteFragmentPool::get_stat(PhysicalWriteFragmentPoolStat &stat) const
{
  stat.reset();
  if (is_inited_) {
    for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      const PhysicalWriteFragment &slot = slots_[i];
      const AsyncFragmentState state = slot.get_state();
      const int tmp_ret = stat.inc_state(state);
      if (OB_SUCCESS != tmp_ret) {
        PALF_LOG_RET(ERROR, tmp_ret, "count async fragment state failed",
                     K(state), K(i), K(slot));
      }
    }
  }
}

int64_t PhysicalWriteFragmentPool::get_oldest_pending_io_start_ts() const
{
  int64_t oldest_ts = OB_INVALID_TIMESTAMP;
  if (is_inited_) {
    for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      const PhysicalWriteFragment &slot = slots_[i];
      if (slot.is_submitted() || slot.is_failed()) {
        const int64_t submit_ts = slot.get_submit_ts();
        if (submit_ts > 0 && (OB_INVALID_TIMESTAMP == oldest_ts || submit_ts < oldest_ts)) {
          oldest_ts = submit_ts;
        }
      }
    }
  }
  return oldest_ts;
}

DEF_TO_STRING(PhysicalWriteFragmentPool)
{
  int64_t pos = 0;
  PhysicalWriteFragmentPoolStat stat;
  get_stat(stat);
  J_OBJ_START();
  J_KV(K_(is_inited), K(stat));
  J_COMMA();
  J_NAME("slots");
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
    if (i > 0) {
      J_COMMA();
    }
    BUF_PRINTO(slots_[i]);
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int PhysicalWriteFragmentPool::collect_ready_fragments(
    common::ObIArray<PhysicalWriteFragment *> &fragments,
    const int64_t aio_delay_us,
    PalfPerfItem *wait_parent_wake_cnt,
    PalfPerfItem *wait_parent_wait_us,
    PalfPerfItem *wait_parent_data_bytes)
{
  int ret = OB_SUCCESS;
  const PhysicalWriteFragmentStateFilter filter(AsyncFragmentState::READY);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "collect ready fragments before pool init", K(ret));
  } else if (aio_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid AIO delay when collecting ready fragments",
             KR(ret), K(aio_delay_us));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      PhysicalWriteFragment &slot = slots_[i];
      if (slot.is_wait_parent()) {
        bool woken = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(try_wake_wait_parent_(slot,
                                              aio_delay_us,
                                              wait_parent_wake_cnt,
                                              wait_parent_wait_us,
                                              wait_parent_data_bytes,
                                              woken))) {
          ret = tmp_ret;
          if (OB_ERR_UNEXPECTED == tmp_ret) {
            PALF_LOG(ERROR, "try wake wait-parent fragment failed while collecting ready fragments",
                     K(ret), K(slot));
          } else {
            PALF_LOG(WARN, "try wake wait-parent fragment failed while collecting ready fragments",
                     K(ret), K(slot));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(collect_fragments(fragments, filter))) {
    PALF_LOG(WARN, "collect ready fragments by state failed", K(ret));
  }
  return ret;
}

int PhysicalWriteFragmentPool::free_all_finished_fragments(
    const int64_t aio_delay_us,
    PalfPerfItem *fragment_recycle_delay_us)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = common::ObTimeUtility::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "free finished fragments before pool init", K(ret));
  } else if (aio_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid AIO delay when freeing finished fragments",
             KR(ret), K(aio_delay_us));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      PhysicalWriteFragment &slot = slots_[i];
      if (slot.is_finished()) {
        int64_t remaining_delay_us = 0;
        if (OB_FAIL(slot.get_remaining_finish_delay(now_us, aio_delay_us, remaining_delay_us))) {
          PALF_LOG(WARN, "get finished fragment recycle delay failed", KR(ret), K(i), K(slot));
        } else if (0 == remaining_delay_us) {
          const int64_t finish_ts = slot.get_finish_ts();
          if (OB_FAIL(free_slot(slot.get_fragment_ref()))) {
            PALF_LOG(WARN, "free finished fragment failed", KR(ret), K(i), K(slot));
          } else if (OB_NOT_NULL(fragment_recycle_delay_us)) {
            const int64_t recycle_ts = common::ObTimeUtility::current_time();
            if (finish_ts <= 0 || recycle_ts < finish_ts) {
              PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                           "invalid fragment recycle timestamp",
                           K(i), K(finish_ts), K(recycle_ts));
            } else {
              fragment_recycle_delay_us->record(recycle_ts, recycle_ts - finish_ts);
            }
          }
        }
      }
    }
  }
  return ret;
}

int PhysicalWriteFragmentPool::get_next_drive_interval(
    const int64_t now_us,
    const int64_t aio_delay_us,
    int64_t &next_drive_interval_us) const
{
  int ret = OB_SUCCESS;
  next_drive_interval_us = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "get fragment drive interval before pool init", KR(ret));
  } else if (now_us <= 0 || aio_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid fragment drive interval argument",
             KR(ret), K(now_us), K(aio_delay_us));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && next_drive_interval_us > 0
         && i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      const PhysicalWriteFragment &slot = slots_[i];
      const AsyncFragmentState state = slot.get_state();
      int64_t interval_us = INT64_MAX;
      if (AsyncFragmentState::READY == state) {
        interval_us = 0;
      } else if (AsyncFragmentState::FAILED == state) {
        const int64_t next_retry_ts = slot.get_next_retry_ts();
        interval_us = next_retry_ts <= now_us ? 0 : next_retry_ts - now_us;
      } else if (AsyncFragmentState::FINISHED == state
                 && OB_FAIL(slot.get_remaining_finish_delay(now_us, aio_delay_us, interval_us))) {
        PALF_LOG(WARN, "get finished fragment drive interval failed", KR(ret), K(i), K(slot));
      }
      if (OB_SUCC(ret)) {
        next_drive_interval_us = MIN(next_drive_interval_us, interval_us);
      }
    }
  }
  return ret;
}

int PhysicalWriteFragmentPool::try_wake_wait_parent_(PhysicalWriteFragment &fragment,
                                                     const int64_t aio_delay_us,
                                                     PalfPerfItem *wait_parent_wake_cnt,
                                                     PalfPerfItem *wait_parent_wait_us,
                                                     PalfPerfItem *wait_parent_data_bytes,
                                                     bool &woken)
{
  int ret = OB_SUCCESS;
  woken = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "wake wait-parent before fragment pool init", K(ret), K(fragment));
  } else if (!fragment.get_fragment_ref().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid wait-parent fragment", K(ret), K(fragment));
  } else if (fragment.is_wait_parent()) {
    const FragmentRef parent_ref = fragment.get_parent_ref();
    const PhysicalWriteFragment *parent = NULL;
    bool can_wake = false;
    int tmp_ret = get_fragment(parent_ref, parent);
    if (OB_SUCCESS == tmp_ret) {
      if (parent->is_finished()) {
        int64_t remaining_delay_us = 0;
        if (OB_FAIL(parent->get_remaining_finish_delay(common::ObTimeUtility::current_time(),
                                                       aio_delay_us,
                                                       remaining_delay_us))) {
          PALF_LOG(WARN, "get wait-parent finish delay failed",
                   KR(ret), K(parent_ref), K(fragment));
        } else {
          can_wake = (0 == remaining_delay_us);
        }
      }
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // generation 已变化表示 parent 已完成并回收, child 不再需要等待.
      can_wake = true;
    } else {
      ret = tmp_ret;
      PALF_LOG(WARN, "get wait-parent fragment parent failed",
               K(ret), K(parent_ref), K(fragment));
    }
    if (OB_SUCC(ret) && can_wake) {
      const int64_t now_us = common::ObTimeUtility::current_time();
      int64_t wait_us = 0;
      int64_t data_len = 0;
      if (OB_FAIL(fragment.get_wait_parent_stat(now_us, wait_us, data_len))) {
        PALF_LOG(WARN, "get wait-parent perf stat failed before waking fragment",
                 K(ret), K(parent_ref), K(fragment));
      } else if (OB_FAIL(fragment.mark_ready(fragment.get_fragment_ref()))) {
        PALF_LOG(WARN, "wake wait-parent fragment failed",
                 K(ret), K(parent_ref), K(fragment), KPC(parent));
      } else {
        woken = true;
        if (OB_NOT_NULL(wait_parent_wake_cnt)
            && OB_NOT_NULL(wait_parent_wait_us)
            && OB_NOT_NULL(wait_parent_data_bytes)) {
          wait_parent_wake_cnt->record(now_us, 1);
          wait_parent_wait_us->record(now_us, wait_us);
          wait_parent_data_bytes->record(now_us, data_len);
        }
      }
    }
  }
  return ret;
}
int PhysicalWriteFragmentPool::collect_fragments(common::ObIArray<PhysicalWriteFragment *> &fragments,
                                                 const PhysicalWriteFragmentFilter &filter)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "collect fragments before pool init", K(ret));
  } else {
    fragments.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < FRAGMENT_SLOT_CNT_PER_PALF; ++i) {
      PhysicalWriteFragment &slot = slots_[i];
      if (filter(slot)) {
        if (OB_FAIL(fragments.push_back(&slot))) {
          PALF_LOG(WARN, "collect fragment pointer failed", K(ret), K(i), K(slot));
        }
      }
    }
  }
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
