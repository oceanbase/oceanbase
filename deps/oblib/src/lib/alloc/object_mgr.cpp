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

#include "object_mgr.h"
#include "lib/allocator/ob_ctx_define.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/memory_sanity.h"

using namespace oceanbase;
using namespace lib;

SubObjectMgr::SubObjectMgr(const bool for_logger, const int64_t tenant_id, const int64_t ctx_id,
                           const uint32_t ablock_size,
                           const bool enable_dirty_list,
                           IBlockMgr *blk_mgr)
  : IBlockMgr(tenant_id, ctx_id), mutex_(common::ObLatchIds::ALLOC_OBJECT_LOCK),
    normal_locker_(mutex_), no_log_locker_(mutex_),
    locker_(!for_logger ? static_cast<ISetLocker&>(normal_locker_) :
            static_cast<ISetLocker&>(no_log_locker_)),
    bs_(), os_(NULL, ablock_size, enable_dirty_list)
{
  bs_.set_locker(&locker_);
  os_.set_locker(&locker_);
  NULL == blk_mgr ? os_.set_block_mgr(this) : os_.set_block_mgr(blk_mgr);
#ifndef ENABLE_SANITY
  mutex_.enable_record_stat(false);
#endif
}

void SubObjectMgr::free_object(AObject *object)
{
  ABlock *block = object->block();
  abort_unless(block->is_valid());
  abort_unless(block->in_use_);
  abort_unless(block->obj_set_ != NULL);
  ObjectSet *os = block->obj_set_;
  abort_unless(&os_ == os);
  os->free_object(object);
}

int64_t SubObjectMgr::sync_wash(int64_t wash_size)
{
  return bs_.sync_wash(wash_size);
}

void SubObjectMgr::free_block(ABlock *block)
{
  abort_unless(block);
  abort_unless(block->is_valid());
  AChunk *chunk = block->chunk();
  abort_unless(chunk);
  abort_unless(chunk->is_valid());
  abort_unless(&bs_ == chunk->block_set_);
  bs_.free_block(block);
}

ObjectMgr::ObjectMgr(ObTenantCtxAllocator &allocator, uint64_t tenant_id, uint64_t ctx_id,
                     uint32_t ablock_size, int parallel, bool enable_dirty_list, IBlockMgr *blk_mgr)
  : IBlockMgr(tenant_id, ctx_id), ta_(allocator),
    ablock_size_(ablock_size), parallel_(parallel), enable_dirty_list_(enable_dirty_list),
    blk_mgr_(blk_mgr), sub_cnt_(1),
    root_mgr_(CTX_ATTR(ctx_id).enable_no_log_, tenant_id, ctx_id, ablock_size_,
              enable_dirty_list, blk_mgr_),
    last_wash_ts_(0), last_washed_size_(0)
{
  root_mgr_.set_tenant_ctx_allocator(allocator);
  MEMSET(sub_mgrs_, 0, sizeof(sub_mgrs_));
  sub_mgrs_[0] = &root_mgr_;
}

ObjectMgr::~ObjectMgr()
{
  reset();
}

void ObjectMgr::reset() {
  for (int i = 1; i < ATOMIC_LOAD(&sub_cnt_); i++) {
    if (sub_mgrs_[i] != nullptr) {
      destroy_sub_mgr(sub_mgrs_[i]);
      ATOMIC_STORE(&sub_mgrs_[i], nullptr);
    }
  }
  ATOMIC_STORE(&sub_cnt_, 1);
}

AObject *ObjectMgr::alloc_object(uint64_t size, const ObMemAttr &attr)
{
  AObject *obj = NULL;
  const uint64_t start = common::get_itid();
  SubObjectMgr *sub_mgr = nullptr;
  for (uint64_t i = 0; NULL == obj && i < ATOMIC_LOAD(&sub_cnt_); i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else if (sub_mgr->trylock()) {
      obj = sub_mgr->alloc_object(size, attr);
      sub_mgr->unlock();
    }
  }
  if (OB_ISNULL(obj)) {
    auto cnt = ATOMIC_LOAD(&sub_cnt_);
    if (cnt < parallel_) {
      if (OB_NOT_NULL(sub_mgr = create_sub_mgr())) {
        if (ATOMIC_BCAS(&sub_mgrs_[cnt], nullptr, sub_mgr)) {
          obj = sub_mgr->alloc_object(size, attr);
          ATOMIC_INC(&sub_cnt_);
        } else {
          destroy_sub_mgr(sub_mgr);
        }
      }
    }
    if (OB_ISNULL(obj)) {
      root_mgr_.lock();
      obj = root_mgr_.alloc_object(size, attr);
      root_mgr_.unlock();
    }
  }
  return obj;
}

AObject *ObjectMgr::realloc_object(
    AObject *obj, const uint64_t size, const ObMemAttr &attr)
{
  AObject *new_obj = NULL;

  if (NULL != obj) {
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);

    ABlock *block = obj->block();

    abort_unless(block->is_valid());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    ObjectSet *os = block->obj_set_;
    abort_unless(os);
    if (os != NULL) {
      os->lock();
      new_obj = os->realloc_object(obj, size, attr);
      os->unlock();
    }
  } else {
    new_obj = alloc_object(size, attr);
  }

  return new_obj;
}

void ObjectMgr::free_object(AObject *obj)
{
  ABlock *block = obj->block();
  abort_unless(block->is_valid());
  abort_unless(block->in_use_);
  abort_unless(block->obj_set_ != NULL);

  ObjectSet *set = block->obj_set_;
  set->free_object(obj);
  // TODO by fengshuo.fs: when object_set is empty, try free the sub_mgr of it.
}

ABlock *ObjectMgr::alloc_block(uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = NULL;
  const uint64_t start = common::get_itid();
  SubObjectMgr *sub_mgr = nullptr;
  for (uint64_t i = 0; NULL == block && i < ATOMIC_LOAD(&sub_cnt_); i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else if (sub_mgr->trylock()) {
      block = sub_mgr->alloc_block(size, attr);
      sub_mgr->unlock();
    }
  }
  if (OB_ISNULL(block)) {
    auto cnt = ATOMIC_LOAD(&sub_cnt_);
    if (cnt < parallel_) {
      if (OB_NOT_NULL(sub_mgr = create_sub_mgr())) {
        if (ATOMIC_BCAS(&sub_mgrs_[cnt], nullptr, sub_mgr)) {
          block = sub_mgr->alloc_block(size, attr);
          ATOMIC_INC(&sub_cnt_);
        } else {
          destroy_sub_mgr(sub_mgr);
        }
      }
    }
    if (OB_ISNULL(block)) {
      root_mgr_.lock();
      block = root_mgr_.alloc_block(size, attr);
      root_mgr_.unlock();
    }
  }
  return block;
}

void ObjectMgr::free_block(ABlock *block)
{
  abort_unless(block);
  abort_unless(block->is_valid());
  AChunk *chunk = block->chunk();
  abort_unless(chunk);
  abort_unless(chunk->is_valid());
  BlockSet *bs = chunk->block_set_;
  bs->lock();
  bs->free_block(block);
  bs->unlock();
  // TODO by fengshuo.fs: when block_set is empty, try free the sub_mgr of it.
}

SubObjectMgr *ObjectMgr::create_sub_mgr()
{
  SubObjectMgr *sub_mgr = nullptr;
  auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID,
                                                                        ObCtxIds::DEFAULT_CTX_ID);
  auto &root_mgr = static_cast<ObjectMgr&>(ta->get_block_mgr()).root_mgr_;
  ObMemAttr attr;
  attr.tenant_id_ = OB_SERVER_TENANT_ID;
  attr.label_ = common::ObModIds::OB_TENANT_CTX_ALLOCATOR;
  attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
  root_mgr.lock();
  auto *obj = root_mgr.alloc_object(sizeof(SubObjectMgr), attr);
  root_mgr.unlock();
  if (OB_NOT_NULL(obj)) {
    SANITY_UNPOISON(obj->data_, obj->alloc_bytes_);
    sub_mgr = new (obj->data_) SubObjectMgr(CTX_ATTR(ctx_id_).enable_no_log_, tenant_id_, ctx_id_,
                                            ablock_size_, enable_dirty_list_, blk_mgr_);
    sub_mgr->set_tenant_ctx_allocator(ta_);
  }
  return sub_mgr;
}

void ObjectMgr::destroy_sub_mgr(SubObjectMgr *sub_mgr)
{
  if (sub_mgr != nullptr) {
    auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID,
                                                                          ObCtxIds::DEFAULT_CTX_ID);
    auto &root_mgr = static_cast<ObjectMgr&>(ta->get_block_mgr()).root_mgr_;
    sub_mgr->~SubObjectMgr();
    auto *obj = reinterpret_cast<AObject*>((char*)sub_mgr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    SANITY_POISON(obj->data_, obj->alloc_bytes_);
    root_mgr.free_object(obj);
  }
}

int64_t ObjectMgr::sync_wash(int64_t wash_size)
{
  int64_t washed_size = 0;
  const uint64_t start = common::get_itid();
  for (uint64_t i = 0; washed_size < wash_size && i < ATOMIC_LOAD(&sub_cnt_); i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    auto sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else {
      sub_mgr->lock();
      washed_size += sub_mgr->sync_wash(wash_size - washed_size);
      sub_mgr->unlock();
    }
  }
  UNUSED(ATOMIC_STORE(&last_washed_size_, washed_size));
  UNUSED(ATOMIC_STORE(&last_wash_ts_, common::ObTimeUtility::current_time()));
  return washed_size;
}

ObjectMgr::Stat ObjectMgr::get_stat()
{
  int64_t hold, payload, used;
  hold = payload = used = 0;
  const uint64_t start = common::get_itid();
  for (uint64_t i = 0; i < ATOMIC_LOAD(&sub_cnt_); i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    auto sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else {
      hold += sub_mgr->get_hold();
      payload += sub_mgr->get_payload();
      used += sub_mgr->get_used();
    }
  }
  return Stat{
      .hold_ = hold,
      .payload_ = payload,
      .used_ = used,
      .last_washed_size_ = ATOMIC_LOAD(&last_washed_size_),
      .last_wash_ts_ = ATOMIC_LOAD(&last_wash_ts_)
      };
}

bool ObjectMgr::check_has_unfree()
{
  bool has_unfree = false;
  for (uint64_t idx = 0; idx < ATOMIC_LOAD(&sub_cnt_) && !has_unfree; idx++) {
    auto sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else {
      sub_mgr->lock();
      DEFER(sub_mgr->unlock());
      has_unfree = sub_mgr->check_has_unfree();
    }
  }
  return has_unfree;
}

bool ObjectMgr::check_has_unfree(char *first_label)
{
  bool has_unfree = false;
  for (uint64_t idx = 0; idx < ATOMIC_LOAD(&sub_cnt_) && !has_unfree; idx++) {
    auto sub_mgr = ATOMIC_LOAD(&sub_mgrs_[idx]);
    if (OB_ISNULL(sub_mgr)) {
      // do nothing
    } else {
      sub_mgr->lock();
      DEFER(sub_mgr->unlock());
      has_unfree = sub_mgr->check_has_unfree(first_label);
    }
  }
  return has_unfree;
}
