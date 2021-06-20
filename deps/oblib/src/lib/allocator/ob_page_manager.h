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

#ifndef OCEANBASE_COMMON_PAGE_MANAGER_H_
#define OCEANBASE_COMMON_PAGE_MANAGER_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_interface.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_rbtree.h"

namespace oceanbase {
namespace common {
using lib::ABlock;
using lib::AChunk;
using lib::BlockSet;
using lib::ObMallocAllocator;
using lib::ObMemAttr;
using lib::ObTenantCtxAllocator;
using oceanbase::common::default_memattr;

class ObPageManager : public lib::IBlockMgr {
public:
  constexpr static int DEFAULT_CHUNK_CACHE_CNT = 2;
  constexpr static int MINI_MODE_CHUNK_CACHE_CNT = 0;
  RBNODE(ObPageManager, rblink);
  int compare(const ObPageManager* node) const
  {
    int ret = 0;
    ret = (attr_.tenant_id_ > node->attr_.tenant_id_) - (attr_.tenant_id_ < node->attr_.tenant_id_);
    if (ret == 0) {
      ret = (id_ > node->id_) - (id_ < node->id_);
    }
    return ret;
  }

private:
  friend class ObPageManagerCenter;
  friend class Thread;

public:
  ObPageManager();
  ~ObPageManager();
  static ObPageManager* thread_local_instance()
  {
    return tl_instance_;
  }
  bool less_than(const ObPageManager& other) const
  {
    return less_than(other.attr_.tenant_id_, other.id_);
  }
  bool less_than(int64_t tenant_id, int64_t id) const
  {
    return attr_.tenant_id_ < tenant_id || (attr_.tenant_id_ == tenant_id && id_ < id);
  }
  int set_tenant_ctx(const uint64_t tenant_id, const uint64_t ctx_id);
  void set_max_chunk_cache_cnt(const int cnt)
  {
    bs_.set_max_chunk_cache_cnt(cnt);
  }
  void reset();
  int64_t get_hold() const;
  int64_t get_tid() const
  {
    return tid_;
  }
  int64_t get_tenant_id() const
  {
    return attr_.tenant_id_;
  }
  int64_t get_ctx_id() const
  {
    return attr_.ctx_id_;
  }
  // IBlockMgr interface
  ABlock* alloc_block(uint64_t size, const ObMemAttr& attr = default_memattr) override;
  void free_block(ABlock* block) override;
  ObTenantCtxAllocator& get_tenant_ctx_allocator() override;
  int64_t get_used() const
  {
    return used_;
  }
  static void set_thread_local_instance(ObPageManager& instance)
  {
    tl_instance_ = &instance;
  }

private:
  int init();
  static __thread ObPageManager* tl_instance_;
  static int64_t global_id_;

private:
  int64_t id_;
  lib::ObMemAttr attr_;
  lib::BlockSet bs_;
  int64_t used_;
  const int64_t tid_;
  const int64_t itid_;
  bool has_register_;
  bool is_inited_;
};

class ObPageManagerCenter {
public:
  static ObPageManagerCenter& get_instance();
  int register_pm(ObPageManager& pm);
  void unregister_pm(ObPageManager& pm);
  bool has_register(ObPageManager& pm) const;
  int print_tenant_stat(int64_t tenant_id, char* buf, int64_t len, int64_t& pos);

private:
  ObPageManagerCenter();
  int print_tenant_stat(int64_t tenant_id, int64_t& sum_used, int64_t& sum_hold, char* buf, int64_t len, int64_t& pos);

private:
  lib::ObMutex mutex_;
  container::ObRbTree<ObPageManager, container::ObDummyCompHelper<ObPageManager>> rb_tree_;
};

inline ObPageManager::ObPageManager()
    : id_(ATOMIC_FAA(&global_id_, 1)),
      bs_(),
      used_(0),
      tid_(GETTID()),
      itid_(get_itid()),
      has_register_(false),
      is_inited_(false)
{}

inline ObPageManager::~ObPageManager()
{
  auto& pmc = ObPageManagerCenter::get_instance();
  if (pmc.has_register(*this)) {
    pmc.unregister_pm(*this);
  }
}

inline int ObPageManager::set_tenant_ctx(const uint64_t tenant_id, const uint64_t ctx_id)
{
  int ret = OB_SUCCESS;
  auto& pmc = ObPageManagerCenter::get_instance();
  if (tenant_id != attr_.tenant_id_ || ctx_id != attr_.ctx_id_) {
    if (pmc.has_register(*this)) {
      pmc.unregister_pm(*this);
    }
    attr_ = ObMemAttr(tenant_id, ObModIds::OB_MOD_DO_NOT_USE_ME, ctx_id);
    is_inited_ = false;
    if (OB_FAIL(init())) {
    } else {
      ret = pmc.register_pm(*this);
    }
  }
  return ret;
}

inline int ObPageManager::init()
{
  int ret = OB_SUCCESS;
  ObMallocAllocator* ma = ObMallocAllocator::get_instance();
  lib::ObTenantCtxAllocator* ta = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "init twice", K(ret));
  } else if (OB_ISNULL(ma)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "null ptr", K(ret));
  } else if (OB_ISNULL(ta = ma->get_tenant_ctx_allocator(attr_.tenant_id_, attr_.ctx_id_))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "null ptr", K(ret));
  } else {
    bs_.set_tenant_ctx_allocator(*ta, attr_);
    is_inited_ = true;
  }
  return ret;
}

inline int64_t ObPageManager::get_hold() const
{
  return bs_.get_total_hold();
}

inline ABlock* ObPageManager::alloc_block(uint64_t size, const ObMemAttr& attr)
{
  ABlock* block = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_itid() != itid_)) {
    _OB_LOG(ERROR, "cross thread not supported, pm_tid: %ld, cur_tid: %ld", itid_, get_itid());
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    ObMemAttr inner_attr = attr_;
    inner_attr.label_ = attr.label_;
    block = bs_.alloc_block(size, inner_attr);
    if (OB_UNLIKELY(nullptr == block)) {
      _OB_LOG(WARN,
          "oops, alloc failed, tenant_id=%ld ctx_id=%ld hold=%ld limit=%ld",
          attr_.tenant_id_,
          attr_.ctx_id_,
          bs_.get_tenant_ctx_allocator().get_hold(),
          bs_.get_tenant_ctx_allocator().get_limit());
    } else {
      used_ += size;
    }
  }
  return block;
}

inline void ObPageManager::free_block(ABlock* block)
{
  if (OB_UNLIKELY(get_itid() != itid_)) {
    _OB_LOG(ERROR, "cross thread not supported, pm_tid: %ld, cur_tid: %ld", itid_, get_itid());
  } else if (OB_LIKELY(block != nullptr)) {
    abort_unless(block);
    abort_unless(block->check_magic_code());
    AChunk* chunk = block->chunk();
    abort_unless(chunk);
    abort_unless(chunk->check_magic_code());
    abort_unless(&bs_ == chunk->block_set_);
    used_ -= block->alloc_bytes_;
    bs_.free_block(block);
  }
}

inline ObTenantCtxAllocator& ObPageManager::get_tenant_ctx_allocator()
{
  return bs_.get_tenant_ctx_allocator();
}

}  // end of namespace common
}  // end of namespace oceanbase

#endif  // OCEANBASE_COMMON_PAGE_MANAGER_H_
