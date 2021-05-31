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

#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_thread_mempool.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase {
namespace common {
ObMemList::ObMemList(const int32_t fixed_size) : header_(NULL), size_(0), fixed_size_(fixed_size), ref_cnt_(0)
{
  pthread_spin_init(&spin_, PTHREAD_PROCESS_PRIVATE);
}

ObMemList::~ObMemList()
{
  MemBlock* iter = header_;
  while (NULL != iter) {
    MemBlock* tmp = iter;
    iter = iter->next;
    alloc_.free(tmp);
  }
  header_ = NULL;
  size_ = 0;
  pthread_spin_destroy(&spin_);
}

void* ObMemList::get()
{
  void* ret = NULL;
  pthread_spin_lock(&spin_);
  if (NULL != header_) {
    ret = header_;
    header_ = header_->next;
    size_--;
  }
  pthread_spin_unlock(&spin_);
  if (NULL == ret) {
    ret = alloc_.malloc(fixed_size_);
  }
  if (NULL != ret) {
    inc_ref_cnt();
    if (WARN_ALLOC_NUM < get_ref_cnt()) {
      _OB_LOG(
          WARN, "maybe alloc too many memory from mem_list=%p hold_num=%ld free_num=%d", this, get_ref_cnt(), size_);
    }
  }
  return ret;
}

void ObMemList::put(void* ptr, const int32_t max_free_num)
{
  if (NULL != ptr) {
    pthread_spin_lock(&spin_);
    if (max_free_num > size_) {
      MemBlock* tmp = (MemBlock*)ptr;
      tmp->next = header_;
      header_ = tmp;
      ptr = NULL;
      size_++;
    }
    pthread_spin_unlock(&spin_);
    if (NULL != ptr) {
      alloc_.free(ptr);
      _OB_LOG(DEBUG, "free ptr=%p", ptr);
    }
    dec_ref_cnt();
  }
  return;
}

int64_t ObMemList::inc_ref_cnt()
{
  return ATOMIC_AAF((uint64_t*)&ref_cnt_, 1);
}

int64_t ObMemList::dec_ref_cnt()
{
  return ATOMIC_AAF((uint64_t*)&ref_cnt_, 1);
}

int64_t ObMemList::get_ref_cnt()
{
  return ref_cnt_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObThreadMempool::ObThreadMempool() : key_(INVALID_THREAD_KEY), fixed_size_(0), max_free_num_(0)
{}

ObThreadMempool::~ObThreadMempool()
{}

void ObThreadMempool::destroy_thread_data_(void* ptr)
{
  if (NULL != ptr) {
    ObMemList* mem_list = (ObMemList*)ptr;
    if (0 == mem_list->dec_ref_cnt()) {
      _OB_LOG(INFO, "delete mem_list=%p", mem_list);
      delete mem_list;
      mem_list = NULL;
    } else {
      _OB_LOG(INFO, "will not delete mem_list=%p ref_cnt=%ld", mem_list, mem_list->get_ref_cnt());
    }
  }
}

int ObThreadMempool::init(const int32_t fixed_size, const int32_t max_free_num)
{
  int ret = OB_SUCCESS;
  if (0 >= fixed_size || 0 > max_free_num) {
    _OB_LOG(WARN, "invalid param fixed_size=%d max_free_num=%d", fixed_size, max_free_num);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != pthread_key_create(&key_, destroy_thread_data_)) {
    _OB_LOG(WARN, "pthread_key_create fail errno=%u", errno);
    ret = OB_ERROR;
  } else {
    fixed_size_ = static_cast<int32_t>(fixed_size + sizeof(ObMemList*));
    max_free_num_ = max_free_num;
  }
  return ret;
}

int ObThreadMempool::destroy()
{
  int ret = OB_SUCCESS;
  if (INVALID_THREAD_KEY != key_) {
    ObMemList* mem_list = (ObMemList*)pthread_getspecific(key_);
    if (NULL != mem_list) {
      if (0 == mem_list->dec_ref_cnt()) {
        _OB_LOG(INFO, "delete mem_list=%p", mem_list);
        delete mem_list;
        mem_list = NULL;
      } else {
        _OB_LOG(INFO, "will not delete mem_list=%p ref_cnt=%ld", mem_list, mem_list->get_ref_cnt());
      }
    }
    pthread_key_delete(key_);
    key_ = INVALID_THREAD_KEY;
  }
  fixed_size_ = 0;
  max_free_num_ = 0;
  return ret;
}

void* ObThreadMempool::alloc()
{
  void* ret = NULL;
  if (INVALID_THREAD_KEY != key_) {
    ObMemList* mem_list = (ObMemList*)pthread_getspecific(key_);
    if (NULL == mem_list) {
      mem_list = new (std::nothrow) ObMemList(fixed_size_);
      if (NULL != mem_list) {
        if (0 != pthread_setspecific(key_, mem_list)) {
          _OB_LOG(WARN, "pthread_setspecific fail errno=%u key=%d", errno, key_);
          delete mem_list;
          mem_list = NULL;
        } else {
          _OB_LOG(INFO, "new mem_list=%p", mem_list);
          mem_list->inc_ref_cnt();
        }
      }
    }
    if (NULL != mem_list) {
      ObMemList** buffer = (ObMemList**)mem_list->get();
      if (NULL != buffer) {
        *buffer = mem_list;
        ret = buffer + 1;
      }
    }
  }
  return ret;
}

void ObThreadMempool::free(void* ptr)
{
  if (NULL != ptr) {
    ObMemList** buffer = ((ObMemList**)ptr) - 1;
    ObMemList* mem_list = *buffer;
    if (NULL == mem_list) {
      _OB_LOG(ERROR, "mem_list null pointer cannot free ptr=%p", ptr);
    } else {
      mem_list->put(buffer, max_free_num_);
      if (0 == mem_list->get_ref_cnt()) {
        _OB_LOG(INFO, "delete mem_list=%p", mem_list);
        delete mem_list;
        mem_list = NULL;
      }
    }
  }
  return;
}

void ObThreadMempool::set_max_free_num(const int32_t max_free_num)
{
  if (0 > max_free_num) {
    _OB_LOG(WARN, "invalid max_free_num=%d", max_free_num);
  } else {
    _OB_LOG(INFO, "set max_free_num from %d to %d", max_free_num_, max_free_num);
    max_free_num_ = max_free_num;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

static ObThreadMempool& get_thread_mempool()
{
  static ObThreadMempool instance;
  return instance;
}

void thread_mempool_init()
{
  get_thread_mempool().init(OB_MAX_PACKET_LENGTH, ObThreadMempool::DEFAULT_MAX_FREE_NUM);
}

void thread_mempool_destroy()
{
  get_thread_mempool().destroy();
}

// void  __attribute__((constructor)) init_global_thread_mempool()
//{
//  get_thread_mempool().init(OB_MAX_PACKET_LENGTH, ObThreadMempool::DEFAULT_MAX_FREE_NUM);
//}

// void  __attribute__((destructor)) destroy_global_thread_mempool()
//{
//  get_thread_mempool().destroy();
//}

void thread_mempool_set_max_free_num(const int32_t max_free_num)
{
  get_thread_mempool().set_max_free_num(max_free_num);
}

void* thread_mempool_alloc()
{
  return get_thread_mempool().alloc();
}

void thread_mempool_free(void* ptr)
{
  get_thread_mempool().free(ptr);
}
}  // namespace common
}  // namespace oceanbase
