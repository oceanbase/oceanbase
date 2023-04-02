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

#include "lib/queue/ob_lighty_queue.h"

#include <sys/syscall.h>
#include <unistd.h>

#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
void ObLightyCond::signal()
{
  (void)ATOMIC_FAA(&futex_.uval(), 1);
  if (ATOMIC_LOAD(&n_waiters_) > 0) {
    futex_.wake(INT32_MAX);
  }
}

void ObLightyCond::wait(const uint32_t cmp, const int64_t timeout)
{
  if (timeout > 0) {
    (void)ATOMIC_FAA(&n_waiters_, 1);
    (void)futex_.wait(cmp, timeout);
    (void)ATOMIC_FAA(&n_waiters_, -1);
  }
}

int ObLightyQueue::init(const uint64_t capacity,
                        const lib::ObLabel &label,
                        const uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  uint64_t n_cond = calc_n_cond(capacity);
  ObMemAttr attr;
  attr.tenant_id_ = tenant_id;
  attr.label_ = label;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
  } else if (NULL == (data_ = (void**)ob_malloc(capacity * sizeof(void*) + n_cond * sizeof(Cond), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    memset(data_, 0, capacity * sizeof(void*));
    capacity_ = capacity;
    n_cond_ = n_cond;
    cond_ = (Cond*)(data_ + capacity);
    for(int i = 0; i < n_cond; i++) {
      new(cond_ + i)Cond();
    }
  }
  return ret;
}

void ObLightyQueue::destroy()
{
  if (NULL != data_) {
    ob_free(data_);
    data_ = NULL;
    cond_ = NULL;
  }
}

void ObLightyQueue::clear()
{
  void* p = NULL;
  while(OB_SUCCESS == pop(p, 0))
    ;
}

int ObLightyQueue::push(void* p)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t limit = 0;
    uint64_t seq = inc_if_lt(&push_, &pop_, capacity_, limit);
    if (seq < limit) {
      store(seq, p);
      get_cond(seq).signal();
    } else {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObLightyQueue::pop(void*& p, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
  } else {
    void* DUMMY = (void*)(~0ULL);
    uint64_t seq = ATOMIC_FAA(&pop_, 1);
    uint64_t push_idx = ATOMIC_LOAD(&push_);
    if (push_idx <= seq) {
      int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
      while((push_idx = wait_push(seq, timeout)) <= seq && (timeout = abs_timeout - get_us()) > 0) {
        PAUSE();
      }
      while((push_idx = push_bounded(DUMMY, seq + 1)) < seq) {
        PAUSE();
      }
    }
    if (DUMMY == (p = fetch(seq))) {
      p = NULL;
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

uint64_t ObLightyQueue::push_bounded(void* p, uint64_t limit)
{
  uint64_t seq = inc_if_lt(&push_, limit);
  if (seq < limit) {
    store(seq, p);
    get_cond(seq).signal();
  }
  return seq;
}

uint64_t ObLightyQueue::inc_if_lt(uint64_t *addr, uint64_t *limit_addr, uint64_t delta, uint64_t &limit)
{
  uint64_t ov = 0;
  uint64_t nv = ATOMIC_LOAD(addr);
  limit = ATOMIC_LOAD(limit_addr) + delta;
  while (((ov = nv) < limit || ov < (limit = ATOMIC_LOAD(limit_addr) + delta))
         && ov != (nv = ATOMIC_VCAS(addr, ov, ov + 1))) {
    PAUSE();
  }
  return nv;
}

uint64_t ObLightyQueue::wait_push(uint64_t seq, int64_t timeout)
{
  uint32_t wait_id = get_cond(seq).get_seq();
  uint64_t push_idx = ATOMIC_LOAD(&push_);
  if (push_idx <= seq) {
    get_cond(seq).wait(wait_id, timeout);
  }
  return push_idx;
}

int64_t ObLightyQueue::get_us()
{
  return ::oceanbase::common::ObTimeUtility::current_time();
}

uint64_t ObLightyQueue::inc_if_lt(uint64_t* addr, uint64_t b)
{
  uint64_t ov = ATOMIC_LOAD(addr);
  uint64_t nv = 0;
  while(ov < b && ov != (nv = ATOMIC_VCAS(addr, ov, ov + 1))) {
    ov = nv;
  }
  return ov;
}

void* ObLightyQueue::fetch(uint64_t seq)
{
  void* p = NULL;
  void** addr = data_ + idx(seq);
  while(NULL == ATOMIC_LOAD(addr) || NULL == (p = ATOMIC_TAS(addr, NULL))) {
    PAUSE();
  }
  return p;
}

void ObLightyQueue::store(uint64_t seq, void* p)
{
  void** addr = data_ + idx(seq);
  while(!ATOMIC_BCAS(addr, NULL, p)) {
    PAUSE();
  }
}

static int64_t get_us() { return ::oceanbase::common::ObTimeUtility::current_time(); }

int LightyQueue::init(const uint64_t capacity, const lib::ObLabel &label, const uint64_t tenant_id)
{
  ObMemAttr attr(tenant_id, label);
  return queue_.init(capacity, global_default_allocator, attr);
}

int LightyQueue::push(void *data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
  int64_t wait_timeout = 0;
  while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.push(data))) {
      break;
    } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
      ret = OB_TIMEOUT;
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::pop(void *&data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
  int64_t wait_timeout = 0;
  while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.pop(data))) {
      break;
    } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::multi_pop(void **data, const int64_t data_count, int64_t &avail_count,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  avail_count = 0;
  if (data_count > 0) {
    void *curr_data = NULL;
    int64_t abs_timeout = (timeout > 0 ? (get_us() + timeout) : 0);
    int64_t wait_timeout = 0;
    while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
      uint32_t seq = cond_.get_seq();
      if (OB_SUCCESS == (ret = queue_.pop(curr_data))) {
        data[avail_count++] = curr_data;
        curr_data = NULL;
        cond_.signal();
        if (avail_count >= data_count) {
          //finish then break
          break;
        }
      } else if (avail_count > 0) {
        //not finish, but has already got one, break
        ret = OB_SUCCESS;
        break;
      } else if (timeout <= 0 || (wait_timeout = abs_timeout - get_us()) <= 0) {
        break;
      } else {
        cond_.wait(seq, wait_timeout);
      }
    }
  }
  return ret;
}

void LightyQueue::reset()
{
  void *p = NULL;
  while (0 == pop(p, 0))
    ;
}

}; // end namespace common
}; // end namespace oceanbase
