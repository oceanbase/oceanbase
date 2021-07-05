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

#ifndef _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_
#define _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_

#include "lib/allocator/ob_ctx_parallel_define.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/random/ob_random.h"
#include "lib/ob_define.h"
#include "lib/alloc/alloc_interface.h"
#include "object_set.h"

namespace oceanbase {
namespace lib {
// object_set needs to be lightweight, and some large or logically optional members need to be stripped out
// SubObjectMgr is a combination of object_set and attributes stripped from object_set, such as block_set, mutex, etc.
class SubObjectMgr : public IBlockMgr {
  friend class ObTenantCtxAllocator;

public:
  SubObjectMgr(const bool for_logger)
      : mutex_(common::ObLatchIds::ALLOC_OBJECT_LOCK),
        mod_set_(),
        normal_locker_(mutex_),
        logger_locker_(mutex_),
        locker_(!for_logger ? static_cast<ISetLocker&>(normal_locker_) : static_cast<ISetLocker&>(logger_locker_)),
        os_(),
        bs_()
  {
    bs_.set_locker(&locker_);
    os_.set_locker(&locker_);
    os_.set_mod_set(&mod_set_);
    os_.set_block_mgr(this);
  }
  void set_tenant_ctx_allocator(ObTenantCtxAllocator& allocator, const ObMemAttr& attr)
  {
    bs_.set_tenant_ctx_allocator(allocator, attr);
  }
  void lock()
  {
    locker_.lock();
  }
  void unlock()
  {
    locker_.unlock();
  }
  bool trylock()
  {
    return locker_.trylock();
  }
  common::ObModItem get_mod_usage(int mod_id) const
  {
    return mod_set_.get_mod(mod_id);
  }
  AObject* alloc_object(uint64_t size, const ObMemAttr& attr)
  {
    return os_.alloc_object(size, attr);
  }
  ABlock* alloc_block(uint64_t size, const ObMemAttr& attr) override
  {
    return bs_.alloc_block(size, attr);
  }
  void free_block(ABlock* block) override
  {
    abort_unless(block);
    abort_unless(block->check_magic_code());
    AChunk* chunk = block->chunk();
    abort_unless(chunk);
    abort_unless(chunk->check_magic_code());
    abort_unless(&bs_ == chunk->block_set_);
    bs_.free_block(block);
  }
  ObTenantCtxAllocator& get_tenant_ctx_allocator() override
  {
    return bs_.get_tenant_ctx_allocator();
  }

private:
  lib::ObMutex mutex_;
  common::ObLocalModSet mod_set_;
  SetLocker normal_locker_;
  SetLockerForLogger logger_locker_;
  ISetLocker& locker_;
  ObjectSet os_;
  BlockSet bs_;
};

template <int N>
class ObjectMgr : public IBlockMgr {
public:
  ObjectMgr(ObTenantCtxAllocator& allocator, uint64_t tenant_id, uint64_t ctx_id);
  ~ObjectMgr();

  AObject* alloc_object(uint64_t size, const ObMemAttr& attr);
  AObject* realloc_object(AObject* obj, const uint64_t size, const ObMemAttr& attr);
  ABlock* alloc_block(uint64_t size, const ObMemAttr& attr) override;
  void free_block(ABlock* block) override;
  ObTenantCtxAllocator& get_tenant_ctx_allocator() override
  {
    return ta_;
  }

  common::ObModItem get_mod_usage(int mod_id) const;
  void print_usage() const;

private:
  SubObjectMgr* create_sub_mgr();
  void destroy_sub_mgr(SubObjectMgr* sub_mgr);

public:
  ObTenantCtxAllocator& ta_;
  const ObMemAttr attr_;
  const int sub_cnt_;
  SubObjectMgr root_mgr_;
  SubObjectMgr* sub_mgrs_[N];
};  // end of class ObjectMgr

template <int N>
ObjectMgr<N>::ObjectMgr(ObTenantCtxAllocator& allocator, uint64_t tenant_id, uint64_t ctx_id)
    : ta_(allocator),
      attr_(tenant_id, nullptr, ctx_id),
      sub_cnt_(common::ObCtxParallel::instance().parallel_of_ctx(ctx_id)),
      root_mgr_(common::ObCtxIds::LOGGER_CTX_ID == attr_.ctx_id_)
{
  root_mgr_.set_tenant_ctx_allocator(allocator, attr_);
  MEMSET(sub_mgrs_, 0, sizeof(sub_mgrs_));
  sub_mgrs_[0] = &root_mgr_;
}

template <int N>
ObjectMgr<N>::~ObjectMgr()
{
  for (int i = 1; i < sub_cnt_; i++) {
    if (sub_mgrs_[i] != nullptr) {
      destroy_sub_mgr(sub_mgrs_[i]);
      sub_mgrs_[i] = nullptr;
    }
  }
}

template <int N>
SubObjectMgr* ObjectMgr<N>::create_sub_mgr()
{
  SubObjectMgr* sub_mgr = nullptr;
  ObMemAttr attr = attr_;
  attr.label_ = common::ObModIds::OB_TENANT_CTX_ALLOCATOR;
  AObject* obj = root_mgr_.alloc_object(sizeof(SubObjectMgr), attr);
  if (obj != nullptr) {
    sub_mgr = new (obj->data_) SubObjectMgr(common::ObCtxIds::LOGGER_CTX_ID == attr_.ctx_id_);
    sub_mgr->set_tenant_ctx_allocator(ta_, attr_);
  }
  return sub_mgr;
}

template <int N>
void ObjectMgr<N>::destroy_sub_mgr(SubObjectMgr* sub_mgr)
{
  if (sub_mgr != nullptr) {
    sub_mgr->~SubObjectMgr();
    AObject* obj = reinterpret_cast<AObject*>((char*)sub_mgr - AOBJECT_HEADER_SIZE);
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
    ABlock* block = obj->block();
    abort_unless(block->check_magic_code());
    abort_unless(block->obj_set_ != nullptr);
    ObjectSet* os = block->obj_set_;
    os->free_object(obj);
  }
}

template <int N>
AObject* ObjectMgr<N>::alloc_object(uint64_t size, const ObMemAttr& attr)
{
  AObject* obj = NULL;
  bool found = false;
  const uint64_t start = common::get_itid();
  for (uint64_t i = 0; NULL == obj && i < sub_cnt_ && !found; i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    if (OB_UNLIKELY(nullptr == sub_mgrs_[idx])) {
      root_mgr_.lock();
      if (OB_UNLIKELY(nullptr == sub_mgrs_[idx])) {
        auto* sub_mgr = create_sub_mgr();
        if (sub_mgr != nullptr) {
          sub_mgrs_[idx] = sub_mgr;
        }
      }
      root_mgr_.unlock();
    }
    if (OB_ISNULL(sub_mgrs_[idx])) {
      continue;
    }
    if (sub_mgrs_[idx]->trylock()) {
      obj = sub_mgrs_[idx]->alloc_object(size, attr);
      sub_mgrs_[idx]->unlock();
      found = true;
      break;
    }
  }
  if (!found && NULL == obj) {
    root_mgr_.lock();
    obj = root_mgr_.alloc_object(size, attr);
    root_mgr_.unlock();
  }
  return obj;
}

template <int N>
AObject* ObjectMgr<N>::realloc_object(AObject* obj, const uint64_t size, const ObMemAttr& attr)
{
  AObject* new_obj = NULL;

  if (NULL != obj) {
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);

    ABlock* block = obj->block();

    abort_unless(block->check_magic_code());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    ObjectSet* os = block->obj_set_;
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

template <int N>
ABlock* ObjectMgr<N>::alloc_block(uint64_t size, const ObMemAttr& attr)
{
  ABlock* block = NULL;
  bool found = false;
  const uint64_t start = common::get_itid();
  for (uint64_t i = 0; NULL == block && i < sub_cnt_ && !found; i++) {
    uint64_t idx = (start + i) % sub_cnt_;
    if (OB_UNLIKELY(nullptr == sub_mgrs_[idx])) {
      root_mgr_.lock();
      if (OB_UNLIKELY(nullptr == sub_mgrs_[idx])) {
        auto* sub_mgr = create_sub_mgr();
        if (sub_mgr != nullptr) {
          sub_mgrs_[idx] = sub_mgr;
        }
      }
      root_mgr_.unlock();
    }
    if (OB_ISNULL(sub_mgrs_[idx])) {
      continue;
    }
    if (sub_mgrs_[idx]->trylock()) {
      block = sub_mgrs_[idx]->alloc_block(size, attr);
      sub_mgrs_[idx]->unlock();
      found = true;
      break;
    }
  }
  if (!found && NULL == block) {
    root_mgr_.lock();
    block = root_mgr_.alloc_block(size, attr);
    root_mgr_.unlock();
  }
  return block;
}

template <int N>
void ObjectMgr<N>::free_block(ABlock* block)
{
  abort_unless(block);
  abort_unless(block->check_magic_code());
  AChunk* chunk = block->chunk();
  abort_unless(chunk);
  abort_unless(chunk->check_magic_code());
  BlockSet* bs = chunk->block_set_;
  bs->lock();
  bs->free_block(block);
  bs->unlock();
}

template <int N>
common::ObModItem ObjectMgr<N>::get_mod_usage(int mod_id) const
{
  common::ObModItem item;
  common::ObModItem one_item;
  for (int i = 0; i < sub_cnt_; ++i) {
    if (sub_mgrs_[i] != nullptr) {
      one_item = sub_mgrs_[i]->get_mod_usage(mod_id);
      item += one_item;
      one_item.reset();
    }
  }
  return item;
}

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_ */
