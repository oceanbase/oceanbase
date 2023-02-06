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

#ifndef OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_
#define OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_

#include "lib/objectpool/ob_resource_pool.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace storage
{

class ObTenantMetaMemMgr;

class RPMetaObjLabel
{
public:
  static constexpr const char LABEL[] = "MetaObj";
};

class TryWashTabletFunc final
{
public:
  explicit TryWashTabletFunc(ObTenantMetaMemMgr &t3m);
  ~TryWashTabletFunc();

  int operator()();

private:
  ObTenantMetaMemMgr &t3m_;
};

class ObITenantMetaObjPool
{
public:
  ObITenantMetaObjPool() = default;
  virtual ~ObITenantMetaObjPool() = default;

  virtual void free_obj(void *obj) = 0;
};

template <typename T>
class ObTenantMetaObjPool
    : public common::ObBaseResourcePool<T, RPMetaObjLabel>,
      public ObITenantMetaObjPool
{
private:
  typedef common::ObBaseResourcePool<T, RPMetaObjLabel> BasePool;
public:
  ObTenantMetaObjPool(
      const uint64_t tenant_id,
      const int64_t max_free_list_num,
      const lib::ObLabel &label,
      const uint64_t ctx_id,
      TryWashTabletFunc &wash_func_);
  virtual ~ObTenantMetaObjPool();

  int64_t used() const { return allocator_.used(); }
  int64_t total() const { return allocator_.total(); }
  int64_t get_used_obj_cnt() const { return ATOMIC_LOAD(&used_obj_cnt_); }
  int64_t get_free_obj_cnt() const { return this->get_free_num(); }
  int64_t get_obj_size() const { return sizeof(T); }
  virtual void free_obj(void *obj) override;
  virtual void free_node_(typename BasePool::Node *ptr) override;

  int acquire(T *&t);
  void release(T *t);

  TO_STRING_KV(K(typeid(T).name()), K(sizeof(T)), K_(used_obj_cnt), "free_obj_hold_cnt",
      this->get_free_num(), "allocator used", allocator_.used(), "allocator total",
      allocator_.total());
private:
  TryWashTabletFunc &wash_func_;
  common::ObFIFOAllocator allocator_;
  int64_t used_obj_cnt_;
};

template <class T>
void ObTenantMetaObjPool<T>::free_obj(void *obj)
{
  release(static_cast<T *>(obj));
}

template <class T>
int ObTenantMetaObjPool<T>::acquire(T *&t)
{
  int ret = OB_SUCCESS;
  t = nullptr;
  const int64_t max_wait_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 3L; // 3s
  while (OB_SUCC(ret)
         && OB_ISNULL(t = BasePool::alloc())
         && max_wait_ts - ObTimeUtility::fast_current_time() >= 0) {
    ob_usleep(1);
    if (OB_FAIL(wash_func_())) {
      STORAGE_LOG(WARN, "wash function fail", K(ret));
    }
  }
  if (OB_ALLOCATE_MEMORY_FAILED == ret && OB_NOT_NULL(t = BasePool::alloc())) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(t)) {
      BasePool::free(t);
      t = nullptr;
    }
  } else {
    if (OB_ISNULL(t)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "no object could be acquired", K(ret));
    } else {
      (void)ATOMIC_AAF(&used_obj_cnt_, 1);
    }
  }
  return ret;
}

template <class T>
void ObTenantMetaObjPool<T>::release(T *t)
{
  BasePool::free(t);
  (void)ATOMIC_AAF(&used_obj_cnt_, -1);
}

template <class T>
void ObTenantMetaObjPool<T>::free_node_(typename BasePool::Node *ptr)
{
  if (NULL != ptr) {
    ptr->data.reset();
    ptr->next = NULL;
    if (BasePool::ALLOC_BY_INNER_ALLOCATOR == ptr->flag) {
      if (common::OB_SUCCESS != BasePool::free_list_.push(ptr)) {
        _COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "free node to list fail, size=%ld ptr=%p", BasePool::free_list_.get_total(), ptr);
      }
      (void)ATOMIC_AAF(&(BasePool::inner_used_num_), -1);
    } else if (BasePool::ALLOC_BY_OBMALLOC == ptr->flag) {
      ptr->~Node();
      common::ob_free(ptr);
    } else {
      _COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid flag=%lu ptr=%p", ptr->flag, ptr);
    }
  }
}

template <class T>
ObTenantMetaObjPool<T>::ObTenantMetaObjPool(
    const uint64_t tenant_id,
    const int64_t max_free_list_num,
    const lib::ObLabel &label,
    const uint64_t ctx_id,
    TryWashTabletFunc &wash_func_)
  : ObBaseResourcePool<T, RPMetaObjLabel>(max_free_list_num, &allocator_,
      lib::ObMemAttr(tenant_id, label, ctx_id)),
    wash_func_(wash_func_),
    used_obj_cnt_(0)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  const int64_t mem_limit = tenant_config.is_valid()
      ? tenant_config->_storage_meta_memory_limit_percentage : OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT;
  if (ObCtxIds::META_OBJ_CTX_ID == ctx_id && OB_FAIL(lib::set_meta_obj_limit(tenant_id, mem_limit))) {
    STORAGE_LOG(WARN, "fail to set meta object memory limit", K(ret), K(tenant_id), K(mem_limit));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, label, ctx_id)))) {
    STORAGE_LOG(WARN, "fail to initialize pool FIFO allocator", K(ret));
  }
  abort_unless(OB_SUCCESS == ret);
}

template <class T>
ObTenantMetaObjPool<T>::~ObTenantMetaObjPool()
{
  ObBaseResourcePool<T, RPMetaObjLabel>::destroy();
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_ */
