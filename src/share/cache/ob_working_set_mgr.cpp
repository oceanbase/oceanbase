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

#define USING_LOG_PREFIX COMMON

#include "ob_working_set_mgr.h"

#include "ob_kvcache_inst_map.h"

namespace oceanbase
{
using namespace lib;
namespace common
{
void WorkingSetMB::reset()
{
  mb_handle_ = NULL;
  seq_num_ = 0;
  block_size_ = 0;
  is_refed_ = false;
  dlink_.prev_ = NULL;
  dlink_.next_ = NULL;
  retire_link_.reset();
}

int WorkingSetMB::store(const ObIKVCacheKey &key, const ObIKVCacheValue &value, ObKVCachePair *&kvpair)
{
  int ret = OB_SUCCESS;
  if (NULL == mb_handle_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("mb_handle is null", K(ret), KP_(mb_handle));
  } else if (OB_FAIL(mb_handle_->store(key, value, kvpair))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("store failed", K(ret));
    }
  }
  return ret;
}

int WorkingSetMB::alloc(
    const int64_t key_size,
    const int64_t value_size,
    const int64_t align_kv_size,
    ObKVCachePair *&kvpair)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mb_handle_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("mb handle is null", K(ret), KP_(mb_handle));
  } else if (OB_FAIL(mb_handle_->alloc(key_size, value_size, align_kv_size, kvpair))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("alloc kvpair failed", K(ret));
    }
  }
  return ret;
}

void WorkingSetMB::set_full(const double base_mb_score)
{
  if (NULL == mb_handle_) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "mb_handle_ is null", KP_(mb_handle));
  } else {
    mb_handle_->set_full(base_mb_score);
  }
}

ObWorkingSet::ObWorkingSet()
{
  reset();
}

ObWorkingSet::~ObWorkingSet()
{
  reset();
}

int ObWorkingSet::init(
    const WSListKey &ws_list_key,
    int64_t limit, ObKVMemBlockHandle *mb_handle,
    ObFixedQueue<WorkingSetMB> &ws_mb_pool,
    ObIMBHandleAllocator &mb_handle_allocator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ws_list_key.is_valid() || limit <= 0 || NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ws_list_key), K(limit), KP(mb_handle));
  } else {
    ws_list_key_ = ws_list_key;
    used_ = 0;
    limit_ = limit;
    head_.prev_ = NULL;
    head_.next_ = NULL;
    ws_mb_pool_ = &ws_mb_pool;
    mb_handle_allocator_ = &mb_handle_allocator;
    inited_ = true;

    // check inited_, so set inited_ to true first
    if (OB_FAIL(build_ws_mb(mb_handle, cur_))) {
      LOG_WARN("build_ws_mb failed", K(ret));
      inited_ = false;
    } else {
      QClockGuard guard(get_qclock());
      dl_insert(&head_, &cur_->dlink_);
      ATOMIC_AAF(&used_, cur_->block_size_);
    }
  }
  return ret;
}

void ObWorkingSet::reset()
{
  ws_list_key_.reset();
  used_ = 0;
  limit_ = 0;
  head_.prev_ = NULL;
  head_.next_ = NULL;
  clear_mbs();
  cur_ = NULL;
  ws_mb_pool_ = NULL;
  mb_handle_allocator_ = NULL;
  inited_ = false;
}

bool ObWorkingSet::add_handle_ref(WorkingSetMB *ws_mb)
{
  bool added = false;
  if (NULL != ws_mb) {
    added = mb_handle_allocator_->add_handle_ref(ws_mb->mb_handle_, ws_mb->seq_num_);
  }
  return added;
}

uint32_t ObWorkingSet::de_handle_ref(WorkingSetMB *ws_mb, const bool do_retire)
{
  uint32_t ref_cnt = 0;
  if (NULL != ws_mb) {
    ref_cnt = mb_handle_allocator_->de_handle_ref(ws_mb->mb_handle_, do_retire);
  }
  return ref_cnt;
}

int ObWorkingSet::alloc(ObKVCacheInst &inst, const enum ObKVCachePolicy policy,
    const int64_t block_size, WorkingSetMB *&ws_mb)
{
  int ret = OB_SUCCESS;
  UNUSED(policy);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_size));
  } else {
    ws_mb = NULL;
    ObKVMemBlockHandle *mb_handle = NULL;
    if (used_ + block_size < limit_) {
      if (OB_FAIL(mb_handle_allocator_->alloc_mbhandle(inst, block_size, mb_handle))) {
        LOG_WARN("alloc_mbhandle failed", K(ret), K(block_size));
      }
    } else {
      if (OB_FAIL(reuse_mb(inst, block_size, mb_handle))) {
        LOG_WARN("reuse_mb failed", K(ret), K(block_size));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(build_ws_mb(mb_handle, ws_mb))) {
        LOG_WARN("build_ws_mb failed", K(ret));
      } else {
        QClockGuard guard(get_qclock());
        dl_insert(&head_, &ws_mb->dlink_);
        ATOMIC_AAF(&used_, ws_mb->block_size_);
      }
    }

    if (OB_FAIL(ret) && NULL != mb_handle) {
      // de ref cnt, mb_handle will be freed
      mb_handle_allocator_->de_handle_ref(mb_handle);
    }
  }
  return ret;
}

int ObWorkingSet::free(WorkingSetMB *ws_mb)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == ws_mb) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ws_mb is null", K(ret), KP(ws_mb));
  } else if (!ws_mb->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "ws_mb", *ws_mb);
  } else {
    const int64_t block_size = ws_mb->block_size_;
    bool freed = false;
    while (!freed) {
      if (ws_mb->try_inc_ref()) {
        mb_handle_allocator_->de_handle_ref(ws_mb->mb_handle_);
        HazardList retire_list;
        {
          QClockGuard guard(get_qclock());
          dl_del(&ws_mb->dlink_);
          ATOMIC_SAF(&used_, block_size);
        }
        ws_mb->dec_ref();
        retire_list.push(&ws_mb->retire_link_);
        retire_ws_mbs(*ws_mb_pool_, retire_list);
        freed = true;
      }
    }
  }
  return ret;
}

WorkingSetMB *&ObWorkingSet::get_curr_mb(ObKVCacheInst &inst, const enum ObKVCachePolicy policy)
{
  UNUSED(inst);
  UNUSED(policy);
  return cur_;
}

bool ObWorkingSet::mb_status_match(ObKVCacheInst &inst,
    const enum ObKVCachePolicy policy, WorkingSetMB *ws_mb)
{
  UNUSED(inst);
  UNUSED(policy);
  return this == ws_mb->mb_handle_->working_set_;
}

int ObWorkingSet::build_ws_mb(ObKVMemBlockHandle *mb_handle, WorkingSetMB *&ws_mb)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mb_handle));
  } else if (!mb_handle_allocator_->add_handle_ref(mb_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add_handle_ref failed", K(ret));
  } else {
    ws_mb = NULL;
    if (OB_FAIL(ws_mb_pool_->pop(ws_mb))) {
      LOG_ERROR("ws_mb_pool pop failed", K(ret));
    } else {
      ws_mb->reset();
      ws_mb->mb_handle_ = mb_handle;
      ws_mb->seq_num_ = mb_handle->get_seq_num();
      ws_mb->block_size_ = mb_handle->mem_block_->get_align_size();
      mb_handle->working_set_ = this;
    }
    mb_handle_allocator_->de_handle_ref(mb_handle);
  }
  return ret;
}

int ObWorkingSet::reuse_mb(ObKVCacheInst &inst, const int64_t block_size, ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    WorkingSetMB *reused_mb = NULL;
    HazardList retire_list;
    int64_t reused_size = 0;
    {
      QClockGuard guard(get_qclock());
      ObDLink *p = static_cast<ObDLink *>(link_next(&head_));
      while (NULL != p && reused_size < block_size && OB_SUCC(ret)) {
        WorkingSetMB *ws_mb = CONTAINER_OF(p, WorkingSetMB, dlink_);
        bool can_try_reuse = false;
        if (ws_mb->is_mark_delete()) {
          // ignore deleted ws_mb
        } else if (!ws_mb->try_inc_ref() || ws_mb->is_mark_delete()) {
          // try_inc_ref avoid threads concurrently try to reuse same ws_mb
          // if try_inc_ref failed, other thread hold the ref, ignore this ws_mb
          // should check ws_mb after inc_ref
        } else {
          if (mb_handle_allocator_->add_handle_ref(ws_mb->mb_handle_, ws_mb->seq_num_)) {
            enum ObKVMBHandleStatus status = ATOMIC_LOAD(&ws_mb->mb_handle_->status_);
            if (FULL == status && 2 == ws_mb->mb_handle_->get_ref_cnt()) {
              can_try_reuse = true;
            }
            mb_handle_allocator_->de_handle_ref(ws_mb->mb_handle_);
            if (can_try_reuse && try_reuse_mb(ws_mb, mb_handle)) {
              reused_mb = ws_mb;
            }
          } else {
            // this mb has been washed, can free
            reused_mb = ws_mb;
          }

          if (OB_SUCC(ret) && NULL != reused_mb) {
            reused_size += reused_mb->block_size_;
            dl_del(&reused_mb->dlink_);
            retire_list.push(&reused_mb->retire_link_);
            reused_mb = NULL;
          }
          ws_mb->dec_ref();
        }
        p = static_cast<ObDLink *>(link_next(p));
      }
    }
    retire_ws_mbs(*ws_mb_pool_, retire_list);

    if (OB_SUCC(ret) && reused_size < block_size) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no enough mb can reused found", K(ret), K(reused_size), K(block_size));
    }

    // should subtract even error occur
    if (reused_size > 0) {
      ATOMIC_SAF(&used_, reused_size);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(mb_handle_allocator_->alloc_mbhandle(inst, block_size, mb_handle))) {
        LOG_WARN("alloc_mbhandle failed", K(ret), K(block_size));
      }
    }
  }
  return ret;
}

bool ObWorkingSet::try_reuse_mb(WorkingSetMB *ws_mb, ObKVMemBlockHandle *&mb_handle)
{
  bool reused = false;
  if (NULL != ws_mb) {
    ObKVMemBlockHandle *reused_handle = ws_mb->mb_handle_;
    const uint32_t seq_num = ws_mb->seq_num_;
    // add_handle_ref before change status
    if (mb_handle_allocator_->add_handle_ref(reused_handle, seq_num)) {
      if (ATOMIC_BCAS((uint32_t*)(&reused_handle->status_), FULL, FREE)) {
        if (reused_handle->handle_ref_.try_check_and_inc_seq_num(seq_num)) {
          int ret = OB_SUCCESS;
          if (OB_FAIL(mb_handle_allocator_->free_mbhandle(reused_handle, true /* do_retire */))) {
            LOG_WARN("free_mbhandle failed", K(ret));
          } else {
            reused = true;
            mb_handle = reused_handle;
          }
          // try_check_and_inc_seq_num will set ref_cnt to 0, no need to de_handle_ref any more
        } else {
          if (!ATOMIC_BCAS((uint32_t*)(&reused_handle->status_), FREE, FULL)) {
            COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "change mb_handle status back to FULL failed");
          }
          mb_handle_allocator_->de_handle_ref(reused_handle);
        }
      }
    }
  }
  return reused;
}

void ObWorkingSet::clear_mbs()
{
  if (NULL != ws_mb_pool_) {
    HazardList retire_list;
    {
      QClockGuard guard(get_qclock());
      ObDLink *p = static_cast<ObDLink *>(link_next(&head_));
      while (NULL != p) {
        WorkingSetMB *ws_mb = CONTAINER_OF(p, WorkingSetMB, dlink_);
        if (ws_mb->is_mark_delete()) {
          // ignore deleted ws_mb
        } else {
          dl_del(&ws_mb->dlink_);
          retire_list.push(&ws_mb->retire_link_);
        }
        p = static_cast<ObDLink *>(link_next(p));
      }
    }
    retire_ws_mbs(*ws_mb_pool_, retire_list);
  }
}

int ObWorkingSet::insert_ws_mb(ObDLink &head, WorkingSetMB *ws_mb)
{
  int ret = OB_SUCCESS;
  if (NULL == ws_mb) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ws_mb));
  } else {
    QClockGuard guard(get_qclock());
    dl_insert(&head, &ws_mb->dlink_);
  }
  return ret;
}

int ObWorkingSet::delete_ws_mb(ObFixedQueue<WorkingSetMB> &ws_mb_pool, WorkingSetMB *ws_mb)
{
  int ret = OB_SUCCESS;
  if (NULL == ws_mb) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ws_mb));
  } else {
    HazardList retire_list;
    {
      QClockGuard guard(get_qclock());
      dl_del(&ws_mb->dlink_);
    }
    retire_list.push(&ws_mb->retire_link_);
    retire_ws_mbs(ws_mb_pool, retire_list);
  }
  return ret;
}

void ObWorkingSet::retire_ws_mbs(ObFixedQueue<WorkingSetMB> &ws_mb_pool, HazardList &retire_list)
{
  if (retire_list.size() > 0) {
    HazardList reclaim_list;
    get_retire_station().retire(reclaim_list, retire_list);
    reuse_ws_mbs(ws_mb_pool, reclaim_list);
  }
}

void ObWorkingSet::reuse_ws_mbs(ObFixedQueue<WorkingSetMB> &ws_mb_pool, HazardList &reclaim_list)
{
  int ret = OB_SUCCESS;
  ObLink *p = NULL;
  // should continue even error occur
  while (NULL != (p = reclaim_list.pop())) {
    WorkingSetMB *ws_mb = CONTAINER_OF(p, WorkingSetMB, retire_link_);
    ws_mb->reset();
    if (OB_FAIL(ws_mb_pool.push(ws_mb))) {
      COMMON_LOG(ERROR, "push ws_mb to pool failed", K(ret));
    }
  }
}

ObWorkingSetMgr::WorkingSetList::WorkingSetList()
{
  reset();
}

ObWorkingSetMgr::WorkingSetList::~WorkingSetList()
{
  reset();
}

int ObWorkingSetMgr::WorkingSetList::init(const WSListKey &ws_list_key, ObIMBHandleAllocator &mb_handle_allocator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ws_list_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ws_list_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < FREE_ARRAY_SIZE; ++i) {
      FreeArrayMB mb;
      if (OB_FAIL(free_array_mbs_.push_back(mb))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      key_ = ws_list_key;
      list_.reset();
      free_array_.reset();
      limit_sum_ = 0;
      mb_handle_allocator_ = &mb_handle_allocator;
      inited_ = true;
    }
  }
  return ret;
}

void ObWorkingSetMgr::WorkingSetList::reset()
{
  key_.reset();
  list_.reset();
  free_array_mbs_.reset();
  free_array_.reset();
  limit_sum_ = 0;
  mb_handle_allocator_ = NULL;
  inited_ = false;
}

int ObWorkingSetMgr::WorkingSetList::add_ws(ObWorkingSet *ws)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == ws) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ws is null", K(ret), KP(ws));
  } else if (!ws->is_valid() || key_ != ws->get_ws_list_key()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid working set", K(ret), "working set", *ws, K_(key));
  } else if (!list_.add_last(ws)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add_last failed", K(ret), "working set", *ws);
  } else {
    limit_sum_ += ws->get_limit();
  }
  return ret;
}

int ObWorkingSetMgr::WorkingSetList::del_ws(ObWorkingSet *ws)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == ws) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ws is null", K(ret), KP(ws));
  } else if (!ws->is_valid() || key_ != ws->get_ws_list_key()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid working set", K(ret), "working set", *ws, K_(key));
  } else if (ws != list_.remove(ws)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remove failead", K(ret), "working set", *ws);
  } else {
    limit_sum_ -= ws->get_limit();
  }
  return ret;
}

int ObWorkingSetMgr::WorkingSetList::pop_mb_handle(ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    mb_handle = NULL;
    if (free_array_.count() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
      // don't print log here
    } else {
      while (OB_SUCC(ret) && free_array_.count() > 0 && NULL == mb_handle) {
        FreeArrayMB *free_mb = NULL;
        if (OB_FAIL(free_array_.pop_back(free_mb))) {
          LOG_WARN("pop_back failed", K(ret));
        } else {
          if (mb_handle_allocator_->add_handle_ref(free_mb->mb_handle_, free_mb->seq_num_)) {
            if (ATOMIC_BCAS((uint32_t*)(&free_mb->mb_handle_->status_), FULL, USING)) {
              mb_handle = free_mb->mb_handle_;
            }
            mb_handle_allocator_->de_handle_ref(free_mb->mb_handle_);
          }
          free_mb->reset();
        }
      }
      if (OB_SUCC(ret) && NULL == mb_handle) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::WorkingSetList::push_mb_handle(ObKVMemBlockHandle *mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mb_handle is null", K(ret), KP(mb_handle));
  } else if (free_array_.count() >= FREE_ARRAY_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    // don't print log here
  } else {
    bool pushed = false;
    for (int64_t i = 0; OB_SUCC(ret) && !pushed && i < free_array_mbs_.count(); ++i) {
      if (!free_array_mbs_.at(i).in_array_) {
        if (mb_handle_allocator_->add_handle_ref(mb_handle)) {
          free_array_mbs_.at(i).mb_handle_ = mb_handle;
          free_array_mbs_.at(i).seq_num_ = mb_handle->get_seq_num();
          free_array_mbs_.at(i).in_array_ = true;
          // change status to full so that wash thread can wash it
          ATOMIC_STORE((uint32_t *)(&mb_handle->status_), FULL);
          if (OB_FAIL(free_array_.push_back(&free_array_mbs_.at(i)))) {
            LOG_WARN("push_back failed", K(ret));
          } else {
            pushed = true;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add_handle_ref failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !pushed) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not available free_array_mb found", K(ret));
    }
  }

  return ret;
}

ObWorkingSetMgr::ObWorkingSetMgr()
  : inited_(false), lock_(), ws_list_map_(),
    list_pool_(), ws_pool_(), ws_mb_pool_(),
    mb_handle_allocator_(NULL), allocator_(ObNewModIds::OB_KVSTORE_CACHE)
{
}

ObWorkingSetMgr::~ObWorkingSetMgr()
{
  destroy();
}

int ObWorkingSetMgr::init(ObIMBHandleAllocator &mb_handle_allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  const int64_t list_num = MAX_CACHE_NUM * MAX_TENANT_NUM_PER_SERVER;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ws_list_map_.create(list_num,
        ObNewModIds::OB_KVSTORE_CACHE, ObNewModIds::OB_KVSTORE_CACHE))) {
    LOG_WARN("create ws_list_map failed", K(ret), K(list_num));
  } else if (NULL == (buf = static_cast<char *>(allocator_.alloc(
      (sizeof(WorkingSetList) + sizeof(WorkingSetList *)) * list_num)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(list_num));
  } else if (OB_FAIL(list_pool_.init(list_num, buf + sizeof(WorkingSetList) * list_num))) {
    LOG_WARN("list_pool init failed", K(ret), K(list_num));
  } else {
    WorkingSetList *list = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < list_num; ++i) {
      list = new (buf + sizeof(WorkingSetList) * i) WorkingSetList();
      if (OB_FAIL(list_pool_.push(list))) {
        LOG_WARN("list_pool push failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t ws_num = MAX_WORKING_SET_COUNT;
    if (NULL == (buf = static_cast<char *>(allocator_.alloc(
        (sizeof(ObWorkingSet) + sizeof(ObWorkingSet *)) * ws_num)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(ws_num));
    } else if (OB_FAIL(ws_pool_.init(ws_num, buf + sizeof(ObWorkingSet) * ws_num))) {
      LOG_WARN("ws_pool init failed", K(ret), K(ws_num));
    } else {
      ObWorkingSet *ws = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < ws_num; ++i) {
        ws = new (buf + sizeof(ObWorkingSet) * i) ObWorkingSet();
        if (OB_FAIL(ws_pool_.push(ws))) {
          LOG_WARN("ws_pool push failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t ws_mb_num = MAX_WORKING_SET_MB_COUNT;
    if (NULL == (buf = static_cast<char *>(allocator_.alloc(
        (sizeof(WorkingSetMB) + sizeof(WorkingSetMB *)) * ws_mb_num)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(ws_mb_num));
    } else if (OB_FAIL(ws_mb_pool_.init(ws_mb_num, buf + sizeof(WorkingSetMB) * ws_mb_num))) {
      LOG_WARN("ws_mb_pool init failed", K(ret), K(ws_mb_num));
    } else {
      WorkingSetMB *ws_mb = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < ws_mb_num; ++i) {
        ws_mb = new (buf + sizeof(WorkingSetMB) * i) WorkingSetMB();
        if (OB_FAIL(ws_mb_pool_.push(ws_mb))) {
          LOG_WARN("ws_mb_pool push failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    mb_handle_allocator_ = &mb_handle_allocator;
    inited_ = true;
  }
  return ret;
}

void ObWorkingSetMgr::destroy()
{
  if (inited_) {
    ws_list_map_.destroy();
    list_pool_.destroy();
    ws_pool_.destroy();

    // free all ws mb cached by threads
    HazardList reclaim_list;
    ObWorkingSet::get_retire_station().purge(reclaim_list);
    ObWorkingSet::reuse_ws_mbs(ws_mb_pool_, reclaim_list);

    ws_mb_pool_.destroy();
    mb_handle_allocator_ = NULL;
    allocator_.reset();
    inited_ = false;
  }
}

int ObWorkingSetMgr::create_working_set(const WSListKey &ws_list_key,
    const int64_t limit, ObWorkingSet *&working_set)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ws_list_key.is_valid() || limit < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ws_list_key));
  } else {
    ObKVMemBlockHandle *mb_handle = NULL;
    WorkingSetList *list = NULL;
    const bool create_not_exist = true;
    DRWLock::WRLockGuard wr_guard(lock_);
    working_set = NULL;
    if (OB_FAIL(get_ws_list(ws_list_key, create_not_exist, list))) {
      LOG_WARN("get_ws_list failed", K(ret), K(ws_list_key), K(create_not_exist));
    } else if (OB_FAIL(ws_pool_.pop(working_set))) {
      LOG_WARN("ws_pool pop failed", K(ret));
    } else if (OB_FAIL(alloc_mb(list, mb_handle))) {
      LOG_WARN("alloc_mb failed", K(ret));
    } else if (OB_FAIL(working_set->init(
        ws_list_key, limit, mb_handle, ws_mb_pool_, *mb_handle_allocator_))) {
      LOG_WARN("working_set init failed", K(ret), K(ws_list_key), K(limit));
    } else if (OB_FAIL(list->add_ws(working_set))) {
      LOG_WARN("add_ws failed", K(ret), KP(working_set));
    }

    if (OB_FAIL(ret) && NULL != working_set) {
      int temp_ret = OB_SUCCESS;
      working_set->reset();
      if (OB_SUCCESS != (temp_ret = ws_pool_.push(working_set))) {
        LOG_ERROR("ws_pool push failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::delete_working_set(ObWorkingSet *working_set)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == working_set) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(working_set));
  } else if (!working_set->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid working_set", K(ret), "working_set", *working_set);
  } else {
    WorkingSetList *list = NULL;
    const bool create_not_exist = false;
    const WSListKey &ws_list_key = working_set->get_ws_list_key();
    DRWLock::WRLockGuard wr_guard(lock_);
    if (OB_FAIL(get_ws_list(ws_list_key, create_not_exist, list))) {
      LOG_WARN("get_ws_list failed", K(ret), K(ws_list_key), K(create_not_exist));
    } else if (OB_FAIL(free_mb(list, working_set->get_curr_mb()))) {
      LOG_WARN("free_mb failed", K(ret));
    } else if (OB_FAIL(list->del_ws(working_set))) {
      LOG_WARN("del_ws failed", K(ret), KP(working_set));
    } else {
      working_set->reset();
      if (OB_FAIL(ws_pool_.push(working_set))) {
        LOG_ERROR("ws_pool push failed", K(ret));
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::get_ws_list(const WSListKey &ws_list_key,
                                 const bool create_not_exist, WorkingSetList *&list)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ws_list_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ws_list_key));
  } else {
    list = NULL;
    if (OB_FAIL(ws_list_map_.get_refactored(ws_list_key, list))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("ws_list_map get_refactored failed", K(ret), K(ws_list_key));
      } else if (!create_not_exist) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("ws lit not exist", K(ret), K(ws_list_key));
      } else if (OB_FAIL(create_ws_list(ws_list_key, list))) {
        LOG_WARN("create_ws_list failed", K(ret), K(ws_list_key));
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::create_ws_list(const WSListKey &ws_list_key, WorkingSetList *&list)
{
  int ret = OB_SUCCESS;
  list = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ws_list_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ws_list_key));
  } else if (OB_FAIL(list_pool_.pop(list))) {
    LOG_WARN("list_pool pop failed", K(ret));
  } else {
    list->reset();
    if (OB_FAIL(list->init(ws_list_key, *mb_handle_allocator_))) {
      LOG_WARN("list init failed", K(ret), K(ws_list_key));
    } else if (OB_FAIL(ws_list_map_.set_refactored(ws_list_key, list))) {
      LOG_WARN("ws_list_map set failed", K(ret), K(ws_list_key));
    }

    if (OB_FAIL(ret)) {
      list->reset();
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = list_pool_.push(list))) {
        LOG_ERROR("list_pool push failed", K(temp_ret));
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::alloc_mb(WorkingSetList *list, ObKVMemBlockHandle *&mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == list) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), KP(list));
  } else {
    mb_handle = NULL;
    ret = list->pop_mb_handle(mb_handle);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pop_mb_handle failed", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(mb_handle_allocator_->alloc_mbhandle(list->get_key(), mb_handle))) {
        LOG_WARN("alloc_mbhandle failed", K(ret), "ws_list_key", list->get_key());
      }
    }
  }
  return ret;
}

int ObWorkingSetMgr::free_mb(WorkingSetList *list, ObKVMemBlockHandle *mb_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == list || NULL == mb_handle) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), KP(list));
  } else {
    ret = list->push_mb_handle(mb_handle);
    if (OB_SUCCESS != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("push_mb_handle failed", K(ret));
    } else if (OB_SIZE_OVERFLOW == ret) {
      if (OB_FAIL(mb_handle_allocator_->mark_washable(mb_handle))) {
        LOG_WARN("mark_washable failed", K(ret));
      }
    }
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase
