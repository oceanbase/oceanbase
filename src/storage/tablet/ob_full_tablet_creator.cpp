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
    tiny_allocator_(),
    wait_create_tablets_cnt_(0),
    created_tablets_cnt_(0),
    mstx_mem_ctx_(nullptr)
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
  } else if (OB_FAIL(tiny_allocator_.init(lib::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE/2, ObMemAttr(tenant_id, "TinyAllocator", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init tenant tiny allocator", K(ret));
  } else {
    ContextParam param;
    param.set_mem_attr(tenant_id, "MSTXCTX", common::ObCtxIds::DEFAULT_CTX_ID)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE)
      .set_properties(ALLOC_THREAD_SAFE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mstx_mem_ctx_, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (nullptr == mstx_mem_ctx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memory entity is null", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObFullTabletCreator::reset()
{
  wait_create_tablets_cnt_ = 0;
  created_tablets_cnt_ = 0;
  tiny_allocator_.reset();
  if (NULL != mstx_mem_ctx_) {
    DESTROY_CONTEXT(mstx_mem_ctx_);
    mstx_mem_ctx_ = nullptr;
  }
  is_inited_ = false;
}

void ObFullTabletCreator::free_tablet(ObTablet *tablet)
{
  if (OB_NOT_NULL(tablet)) {
    ObIAllocator *allocator = tablet->get_allocator();
    tablet->~ObTablet();
    allocator->~ObIAllocator();
    tiny_allocator_.free(allocator);
    ATOMIC_DEC(&created_tablets_cnt_);
  }
  return;
}

int ObFullTabletCreator::throttle_tablet_creation()
{
  int ret = OB_SUCCESS;

  bool need_wait = false;
  const int64_t limit_size = get_tenant_memory_limit(MTL_ID()) / 20; // 5%
  const int64_t timeout = 5 * 1000L; // 5ms for effective replay
  const int64_t log_timeout = 1 * 1000 * 1000L; // 1s
  const int64_t start_time = ObTimeUtility::fast_current_time();

  ATOMIC_INC(&wait_create_tablets_cnt_);
  do {
    if (need_wait) {
      ob_usleep(10); // sleep 10us, do not get mutex here
    }
    if (total() < limit_size) {
      need_wait = false;
    } else if (ObTimeUtility::fast_current_time() - start_time >= timeout) {
      ret = OB_EAGAIN;
      LOG_WARN("throttle tablet creation timeout", K(ret));
      break;
    } else {
      need_wait = true;
      if (REACH_TENANT_TIME_INTERVAL(log_timeout)) {
        const int64_t wait_create_tablets_cnt = ATOMIC_LOAD(&wait_create_tablets_cnt_);
        LOG_WARN("prepare create tablet timeout",
            K_(created_tablets_cnt), K(wait_create_tablets_cnt), K(limit_size),
            K(tiny_allocator_.total()), K(tiny_allocator_.used()),
            K(mstx_mem_ctx_->hold()), K(mstx_mem_ctx_->used()));
      }
    }
  } while (need_wait);
  ATOMIC_DEC(&wait_create_tablets_cnt_);
  return ret;
}

int ObFullTabletCreator::create_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObArenaAllocator *allocator = nullptr;
  ObMetaDiskAddr mem_addr;
  const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE;

  if (OB_ISNULL(allocator = OB_NEWx(
      ObArenaAllocator, (&tiny_allocator_), mstx_mem_ctx_->get_malloc_allocator(), page_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new arena allocator", K(ret));
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

} // namespace storage
} // namespace oceanbase
