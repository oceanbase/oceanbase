/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/tablet/ob_full_tablet_creator.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ObFullTabletCreator::ObFullTabletCreator()
  : is_inited_(false),
    mstx_allocator_(),
    tiny_allocator_(),
    transform_head_(),
    transform_tail_(),
    wait_create_tablets_cnt_(0),
    created_tablets_cnt_(0),
    persist_queue_cnt_(0),
    persist_tablets_cnt_(0),
    gc_tablets_cnt_(0),
    mutex_()
{
}

int ObFullTabletCreator::init(const uint64_t tenant_id)
{
  /* We use two fifo allocators to minimize the memory hole caused by tablets of different lifetime.
    1. MSTXAllocator is used to alloc tablets. To ensure that each tablet can occupy one whole page, we set the page_size = 8K.
    2. TinyAllocator is used to alloc ObArenaAllocator. We set the page_size = 4K to minimize the memory hole. */
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantMetaMemMgr has been initialized", K(ret));
  } else if (OB_FAIL(mstx_allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE, ObMemAttr(tenant_id, "MSTXAllocator", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init tenant mstx allocator", K(ret));
  } else if (OB_FAIL(tiny_allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE/2, ObMemAttr(tenant_id, "TinyAllocator", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init tenant tiny allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObFullTabletCreator::reset()
{
  transform_head_.reset();
  transform_tail_.reset();
  persist_queue_cnt_ = 0;
  wait_create_tablets_cnt_ = 0;
  persist_tablets_cnt_ = 0;
  gc_tablets_cnt_ = 0;
  created_tablets_cnt_ = 0;
  mstx_allocator_.reset();
  tiny_allocator_.reset();
  is_inited_ = false;
}

void ObFullTabletCreator::free_tablet(ObTablet *tablet)
{
  if (OB_NOT_NULL(tablet)) {
    ObIAllocator *allocator = tablet->get_allocator();
    tablet->~ObTablet();
    allocator->~ObIAllocator();
    tiny_allocator_.free(allocator);
    ATOMIC_INC(&gc_tablets_cnt_);
  }
  return;
}

int ObFullTabletCreator::throttle_tablet_creation()
{
  int ret = OB_SUCCESS;

  bool need_wait = false;
  const int64_t limit_size = get_tenant_memory_limit(MTL_ID()) / 10; // 10%
  const int64_t timeout = 5 * 1000L; // 5ms for effective replay
  const int64_t log_timeout = 1 * 1000 * 1000L; // 1s
  const int64_t start_time = ObTimeUtility::fast_current_time();

  ATOMIC_INC(&wait_create_tablets_cnt_);
  do {
    if (need_wait) {
      ob_usleep(10); // sleep 10us, do not get mutex here
    }
    lib::ObMutexGuard guard(mutex_);
    if (mstx_allocator_.total() + tiny_allocator_.total() < limit_size) {
      need_wait = false;
      ATOMIC_DEC(&wait_create_tablets_cnt_);
    } else if (ObTimeUtility::fast_current_time() - start_time >= timeout) {
      ret = OB_EAGAIN;
      LOG_WARN("throttle tablet creation timeout", K(ret));
      break;
    } else {
      need_wait = true;
      if (REACH_TENANT_TIME_INTERVAL(log_timeout)) {
        const int64_t hanging_tablets_cnt = ATOMIC_LOAD(&created_tablets_cnt_) - ATOMIC_LOAD(&gc_tablets_cnt_);
        const int64_t wait_create_tablets_cnt = ATOMIC_LOAD(&wait_create_tablets_cnt_);
        LOG_WARN("prepare create tablet timeout",
            K_(persist_queue_cnt), K(wait_create_tablets_cnt), K(hanging_tablets_cnt), K(limit_size),
            K(mstx_allocator_.total()), K(mstx_allocator_.used()),
            K(tiny_allocator_.total()), K(tiny_allocator_.used()));
      }
    }
  } while (need_wait);
  return ret;
}

int ObFullTabletCreator::create_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObArenaAllocator *allocator = nullptr;
  ObMetaDiskAddr mem_addr;
  const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE - FIFO_START_OFFSET;

  if (OB_ISNULL(allocator = OB_NEWx(ObArenaAllocator, (&tiny_allocator_), mstx_allocator_, page_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new arena allocator", K(ret));
  /* TODO(zhuixin.gsy) rm these set_xx after merge master*/
  } else if (FALSE_IT(allocator->set_label("MSTXAllocator"))) {
  } else if (FALSE_IT(allocator->set_tenant_id(MTL_ID()))) {
  } else if (FALSE_IT(allocator->set_ctx_id(ObCtxIds::DEFAULT_CTX_ID))) {
  } else if (OB_ISNULL(tablet = OB_NEWx(ObTablet, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new tablet", K(ret));
  } else if (OB_FAIL(mem_addr.set_mem_addr(0, sizeof(ObTablet)))) {
    LOG_WARN("fail to set memory address", K(ret));
  } else {
    tablet->set_allocator(allocator);
    tablet->set_tablet_addr(mem_addr);
    ATOMIC_INC(&created_tablets_cnt_);
  }
  if (OB_SUCC(ret)) {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
    tablet_handle.set_obj(tablet, allocator, t3m);
    tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
  } else {
    if (OB_NOT_NULL(tablet)) {
      tablet->~ObTablet();
      tablet = nullptr;
    }
    if (OB_NOT_NULL(allocator)) {
      allocator->~ObArenaAllocator();
      tiny_allocator_.free(allocator);
      allocator = nullptr;
    }
  }
  return ret;
}

int ObFullTabletCreator::persist_tablet()
{
  int ret = OB_SUCCESS;
  int64_t persist_tablets_cnt = 0;
  int64_t error_tablets_cnt = 0;
  ObTabletHandle old_handle;
  const int64_t per_round_time = ObTenantMetaMemMgr::TABLET_TRANSFORM_INTERVAL_US;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  while (OB_SUCC(ret) && ObTimeUtility::fast_current_time() - start_time < per_round_time && OB_SUCC(pop_tablet(old_handle))) {
    const ObTablet *old_tablet = old_handle.get_obj();
    const ObMetaDiskAddr old_addr = old_tablet->get_tablet_addr();
    const ObTabletMeta &tablet_meta = old_tablet->get_tablet_meta();
    ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
    ObMetaDiskAddr addr;
    ObTabletHandle new_handle;
    ObLSHandle ls_handle;
    ObLSTabletService *ls_tablet_svr = nullptr;
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTabletCreateDeleteMdsUserData mds_data;
    ObTimeGuard single_guard("try persist tablet", 5 * 1000); // 5ms
    if (OB_UNLIKELY(!old_tablet->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected old tablet", K(ret), K(key), K(old_handle), KPC(old_tablet));
    } else if (FALSE_IT(single_guard.click("start persist"))) {
    } else if (OB_FAIL(t3m->get_tablet_addr(key, addr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // deleted, skip
      } else {
        LOG_ERROR("fail to get meta addr", K(ret), K(key), K(old_handle), K(old_tablet->is_empty_shell()));
      }
    } else if (!addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected not memory tablet addr", K(ret), K(key), K(addr), K(old_handle), K(old_tablet->is_empty_shell()));
    } else if (addr != old_addr) {
      if (addr.is_block()) {
        LOG_INFO("full tablet has been persisted, skip this", K(ret), K(key), K(old_addr), K(addr));
      } else {
        ret = OB_NOT_THE_OBJECT; // create_memtable may change the addr, push back to queue
        LOG_INFO("memory addr changed, push back to queue", K(ret), K(key), K(old_addr), K(addr));
      }
    } else if (OB_FAIL(old_tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), mds_data, 0))) {
      if (OB_EMPTY_RESULT != ret && OB_ERR_SHARED_LOCK_CONFLICT != ret) {
        LOG_ERROR("fail to get tablet status", K(ret), K(key), K(addr), K(old_handle), K(old_tablet->is_empty_shell()));
      }
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*old_tablet, new_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // deleted, skip
      } else {
        LOG_WARN("fail to persist old tablet", K(ret), K(key), K(old_handle), K(old_tablet->is_empty_shell()));
      }
    } else if (FALSE_IT(single_guard.click("end persist"))) {
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(tablet_meta.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_ERROR("fail to get ls", K(ret), K(tablet_meta.ls_id_));
    } else if (OB_ISNULL(ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_ERROR("null ls tablet svr", K(ret), K(ls_handle));
    } else if (OB_FAIL(ls_tablet_svr->update_tablet_mstx(key, old_addr, old_handle, new_handle))) {
      LOG_WARN("fail to update tablet mstx", K(ret), K(key), K(old_addr), K(old_handle), K(new_handle), K(old_tablet->is_empty_shell()));
    } else {
      single_guard.click("end update mstx");
    }

    if (OB_FAIL(ret)) {
      ++error_tablets_cnt;
      if (OB_FAIL(push_tablet_to_queue(old_handle))) {
        LOG_ERROR("fail to push tablet, wrong tablet may be leaked", K(ret), K(key), K(old_handle), K(old_tablet->is_empty_shell()));
      }
      ret = OB_SUCCESS; // continue to persist other tablet
    } else {
      ++persist_tablets_cnt;
      LOG_DEBUG("succeed to persist one tablet", KP(old_tablet), K(single_guard));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  // persist_tablets_cnt: the cnt of tablets that have been persisted in this round
  // error_tablets_cnt:   the cnt of tablets that couldn't be persisted in this round
  // tablets_cnt:         the cnt of tablets left in queue (including error_tablets_cnt)
  if (persist_tablets_cnt + error_tablets_cnt > 0) {
    lib::ObMutexGuard guard(mutex_);
    persist_tablets_cnt_ += persist_tablets_cnt;
    const int64_t hanging_tablets_cnt = ATOMIC_LOAD(&created_tablets_cnt_) - ATOMIC_LOAD(&gc_tablets_cnt_);
    const int64_t wait_create_tablets_cnt = ATOMIC_LOAD(&wait_create_tablets_cnt_);
    FLOG_INFO("Finish persist task one round", K(persist_tablets_cnt), K(error_tablets_cnt), K_(persist_queue_cnt),
        K(wait_create_tablets_cnt), K(hanging_tablets_cnt),
        K(mstx_allocator_.total()), K(mstx_allocator_.used()),
        K(tiny_allocator_.total()), K(tiny_allocator_.used()));
  }
  return ret;
}

int ObFullTabletCreator::pop_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(0 > persist_queue_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected <0 tablets cnt", K(ret), K_(persist_queue_cnt));
  } else if (0 == persist_queue_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!transform_head_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tablet handle to pop", K(ret), K_(persist_queue_cnt), K_(transform_head));
  } else {
    tablet_handle = transform_head_;
    transform_head_ = transform_head_.get_obj()->get_next_full_tablet();
    ObTabletHandle empty_handle;
    tablet_handle.get_obj()->set_next_full_tablet(empty_handle);
    --persist_queue_cnt_;
    if (!persist_queue_cnt_) {
      transform_tail_.reset();
    }
  }
  return ret;
}

int ObFullTabletCreator::push_tablet_to_queue(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  bool hdl_valid, tablet_valid, addr_valid;
  hdl_valid = tablet_valid = addr_valid = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("full tablet creator not inited", K(ret));
  } else if (OB_UNLIKELY(!(hdl_valid = tablet_handle.is_valid())
                      || !(tablet_valid = tablet_handle.get_obj()->is_valid())
                      || !(addr_valid = tablet_handle.get_obj()->get_tablet_addr().is_valid())
                      || tablet_handle.get_obj()->get_tablet_addr().is_block())) {
    // TODO (@chenqingxiang.cqx) use !is_memory() to skip tablet if empty shell is allocated from pool
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle or not memory tablet addr", K(ret), K(hdl_valid), K(tablet_valid), K(addr_valid),
        K(tablet_handle), KPC(tablet_handle.get_obj()));
  } else if (0 == persist_queue_cnt_) {
    tablet_handle.get_obj()->set_next_full_tablet(transform_head_);
    transform_head_ = transform_tail_ = tablet_handle;
    ++persist_queue_cnt_;
  } else {
    transform_tail_.get_obj()->set_next_full_tablet(tablet_handle);
    transform_tail_ = tablet_handle;
    ++persist_queue_cnt_;
  }
  return ret;
}

int ObFullTabletCreator::remove_tablet_from_queue(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  bool hdl_valid, tablet_valid, addr_valid;
  hdl_valid = tablet_valid = addr_valid = true;
  ObMetaDiskAddr tablet_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("full tablet creator not inited", K(ret));
  } else if (OB_UNLIKELY(!(hdl_valid = tablet_handle.is_valid())
                      || !(tablet_valid = tablet_handle.get_obj()->is_valid())
                      || !(addr_valid = (tablet_addr = tablet_handle.get_obj()->get_tablet_addr()).is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to remove invalid tablet", K(ret), K(hdl_valid), K(tablet_valid), K(addr_valid),
        K(tablet_handle), K(tablet_addr), KPC(tablet_handle.get_obj()));
  } else if (tablet_addr.is_block()
          || tablet_addr.is_none()
          || 0 == persist_queue_cnt_) {
    // skip persisted or none-addr tablet
    // TODO (@chenqingxiang.cqx) use !is_memory() to skip tablet if empty shell is allocated from pool
  } else {
    ObTabletHandle curr_handle = transform_head_;
    if (OB_UNLIKELY(!curr_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tranform head", K(ret), K_(transform_head));
    } else if (curr_handle.get_obj() == tablet_handle.get_obj()) {
      transform_head_ = transform_head_.get_obj()->get_next_full_tablet();
      --persist_queue_cnt_;
      if (!persist_queue_cnt_) {
        transform_tail_.reset();
      }
    } else {
      ObTabletHandle prev_handle = curr_handle;
      while (curr_handle.is_valid()) {
        if (curr_handle.get_obj() == tablet_handle.get_obj()) {
          prev_handle.get_obj()->set_next_full_tablet(curr_handle.get_obj()->get_next_full_tablet());
          if (curr_handle.get_obj() == transform_tail_.get_obj()) {
            transform_tail_ = prev_handle;
          }
          --persist_queue_cnt_;
          break;
        }
        prev_handle = curr_handle;
        curr_handle = curr_handle.get_obj()->get_next_full_tablet();
      }
    }
  }
  return ret;
}

void ObFullTabletCreator::destroy_queue()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  while (transform_head_.is_valid()) {
    transform_head_ = transform_head_.get_obj()->get_next_full_tablet();
    --persist_queue_cnt_;
  }
  transform_tail_.reset();
  if (OB_UNLIKELY(0 != persist_queue_cnt_)) {
    LOG_ERROR("unexpected tablets cnt", K_(persist_queue_cnt));
  }
}

} // namespace storage
} // namespace oceanbase
