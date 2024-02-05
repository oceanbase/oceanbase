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

#ifndef  OCEANBASE_COMMON_PAGE_MANAGER_H_
#define  OCEANBASE_COMMON_PAGE_MANAGER_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_interface.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_rbtree.h"

namespace oceanbase
{
namespace common
{
using lib::BlockSet;
using lib::AChunk;
using lib::ABlock;
using lib::ObMemAttr;
using lib::ObMallocAllocator;
using lib::ObTenantCtxAllocator;

class ObPageManager : public lib::IBlockMgr
{
  friend class Thread;
public:
  ObPageManager();
  ~ObPageManager() {}
  static ObPageManager *thread_local_instance() { return tl_instance_; }
  int set_tenant_ctx(const int64_t tenant_id, const int64_t ctx_id);
  void reset();
  int64_t get_hold() const;
  int64_t get_tid() const { return tid_; }
  // IBlockMgr interface
  virtual ABlock *alloc_block(uint64_t size, const ObMemAttr &attr) override;
  virtual void free_block(ABlock *block) override;
  virtual int64_t sync_wash(int64_t wash_size) override
  {
    UNUSED(wash_size);
    return 0;
  }
  int64_t get_used() const { return used_; }
  static void set_thread_local_instance(ObPageManager &instance) { tl_instance_ = &instance; }
private:
  int init();
  RLOCAL_STATIC(ObPageManager *,tl_instance_);
private:
  lib::ObTenantCtxAllocatorGuard ta_;
  lib::BlockSet bs_;
  int64_t used_;
  const int64_t tid_;
  const int64_t itid_;
  bool has_register_;
  bool is_inited_;
};

inline ObPageManager::ObPageManager()
  : bs_(),
    used_(0),
    tid_(GETTID()),
    itid_(get_itid()),
    has_register_(false),
    is_inited_(false)
{
}

inline int ObPageManager::set_tenant_ctx(const int64_t tenant_id, const int64_t ctx_id)
{
  int ret = OB_SUCCESS;
  if (tenant_id != tenant_id_ || ctx_id != ctx_id_) {
    tenant_id_ = tenant_id;
    ctx_id_ = ctx_id;
    is_inited_ = false;
    if (OB_FAIL(init())) {
    }
  }
  return ret;
}

inline int ObPageManager::init()
{
  int ret = OB_SUCCESS;
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "init twice", K(ret));
  } else if (OB_ISNULL(ma)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "null ptr", K(ret));
  } else if (OB_ISNULL(ta_ = ma->get_tenant_ctx_allocator(tenant_id_, ctx_id_))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "null ptr", K(ret));
  } else {
    bs_.set_tenant_ctx_allocator(*ta_.ref_allocator());
    bs_.set_chunk_mgr(&ta_->get_req_chunk_mgr());
    is_inited_ = true;
  }
  return ret;
}

inline int64_t ObPageManager::get_hold() const
{
  return bs_.get_total_hold();
}

inline ABlock *ObPageManager::alloc_block(uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_itid() != itid_)) {
    _OB_LOG(ERROR, "cross thread not supported, pm_tid: %ld, cur_tid: %ld", itid_, get_itid());
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    ObMemAttr inner_attr(tenant_id_, nullptr, ctx_id_);
    inner_attr.label_ = attr.label_;
    block = bs_.alloc_block(size, inner_attr);
    if (OB_UNLIKELY(nullptr == block)) {
      _OB_LOG(WARN, "oops, alloc failed, tenant_id=%ld ctx_id=%ld hold=%ld limit=%ld",
              tenant_id_,
              ctx_id_,
              ta_->get_hold(),
              ta_->get_limit());
    } else {
      used_ += size;
    }
  }
  return block;
}

inline void ObPageManager::free_block(ABlock *block)
{
  if (OB_UNLIKELY(get_itid() != itid_)) {
    _OB_LOG_RET(ERROR, OB_ERROR, "cross thread not supported, pm_tid: %ld, cur_tid: %ld", itid_, get_itid());
  } else if (OB_LIKELY(block != nullptr)) {
    abort_unless(block);
    abort_unless(block->is_valid());
    AChunk *chunk = block->chunk();
    abort_unless(chunk);
    abort_unless(chunk->is_valid());
    abort_unless(&bs_ == chunk->block_set_);
    used_ -= block->alloc_bytes_;
    bs_.free_block(block);
  }
}

} // end of namespace common
} // end of namespace oceanbase

#endif //OCEANBASE_COMMON_PAGE_MANAGER_H_
