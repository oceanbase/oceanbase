/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_COMMON_H_
#define OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_COMMON_H_

#include "share/table/ob_table.h"
#include "share/table/ob_table_rpc_struct.h"

namespace oceanbase
{
namespace table
{

template <typename T>
class ObTableObjectPool final
{
public:
  ObTableObjectPool(uint64_t tenant_id, const char *pool_name, const int64_t max_num, const int64_t retire_time)
      : allocator_(pool_name, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
        is_inited_(false),
        max_num_(max_num),
        attr_(tenant_id, pool_name),
        retire_time_(retire_time)
  {}
  virtual ~ObTableObjectPool() { destroy(); }
  TO_STRING_KV(K_(is_inited),
               K_(attr),
               K_(max_num),
               K_(retire_time),
               K(obj_pool_.capacity()),
               K(obj_pool_.get_free()),
               K(obj_pool_.get_curr_total())
               );
public:
  int get_object(T *&obj)
  {
    int ret = get_object_inner(obj);

    if (OB_SUCC(ret) && OB_NOT_NULL(obj)) {
      obj->set_last_active_ts(ObTimeUtility::fast_current_time());
    }

    return ret;
  }
  int free_object(T *obj)
  {
    int ret = OB_SUCCESS;

    if (is_inited_ && OB_NOT_NULL(obj)) {
      obj->reuse();
      if (OB_FAIL(obj_pool_.push(obj))) {
        SERVER_LOG(DEBUG, "fail to push back object to object pool", K(ret), K(*this));
        obj->~T();
        if (OB_NOT_NULL(mem_ctx_)) {
          mem_ctx_->free(obj);
        }
      } else {
        SERVER_LOG(DEBUG, "succeed to push back object to object pool", K(ret), K(*this));
      }
    }

    return ret;
  }
  OB_INLINE bool empty() const { return obj_pool_.get_curr_total() == 0; }
  void destroy()
  {
    int ret = OB_SUCCESS;
    T *obj = nullptr;
    while (OB_SUCC(obj_pool_.pop(obj))) {
      if (OB_NOT_NULL(obj)) {
        obj->~T();
        if (OB_NOT_NULL(mem_ctx_)) {
          mem_ctx_->free(obj);
        }
      }
    }
    obj_pool_.destroy();
    max_num_ = 0;
    if (OB_NOT_NULL(mem_ctx_)) {
      DESTROY_CONTEXT(mem_ctx_);
      mem_ctx_ = nullptr;
    }
    allocator_.reset();
    is_inited_ = false;
  }
  int recycle_retired_object()
  {
    int ret = OB_SUCCESS;

    if (IS_INIT) {
      T *obj = nullptr;
      int64_t total = obj_pool_.get_curr_total();
      while (OB_SUCC(ret) && total > 0) {
        if (OB_FAIL(obj_pool_.pop(obj))) {
          if (ret != OB_ENTRY_NOT_EXIST) {
            SERVER_LOG(WARN, "fail to pop object", K(ret), KPC(this));
          }
        } else if (OB_NOT_NULL(obj)) {
          if (ObTimeUtility::fast_current_time() - obj->get_last_active_ts() >= retire_time_) {
            obj->~T();
            if (OB_NOT_NULL(mem_ctx_)) {
              mem_ctx_->free(obj);
            }
            SERVER_LOG(DEBUG, "free object to object pool", K(ret), K(*this), K(retire_time_));
          } else if (OB_FAIL(obj_pool_.push(obj))) {
            SERVER_LOG(WARN, "fail to push back object to object pool", K(ret), K(*this));
            obj->~T();
            if (OB_NOT_NULL(mem_ctx_)) {
              mem_ctx_->free(obj);
            }
          } else {
            SERVER_LOG(DEBUG, "push object to object pool", K(ret), K(*this), K(retire_time_));
          }
        }
        total--;
      }
    }

    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
    }

    return ret;
  }
private:
  int get_object_inner(T *&obj)
  {
    int ret = OB_SUCCESS;
    T *tmp_obj = nullptr;

    if (IS_NOT_INIT) {
      ObLockGuard<ObSpinLock> guard(lock_);
      if (IS_NOT_INIT) {
        if (OB_FAIL(obj_pool_.init(max_num_, &allocator_, attr_))) {
          SERVER_LOG(WARN, "fail to init obj pool", K(ret), K(max_num_), K(attr_));
        } else {
          lib::MemoryContext tmp_mem_ctx = nullptr;
          lib::ContextParam param;
          param.set_mem_attr(MTL_ID(), "TbObj", ObCtxIds::DEFAULT_CTX_ID)
               .set_properties(lib::ALLOC_THREAD_SAFE);
          if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(tmp_mem_ctx, param))) {
            SERVER_LOG(WARN, "fail to create mem context", K(ret));
          } else if (OB_ISNULL(tmp_mem_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected null mem context ", K(ret));
          } else {
            mem_ctx_ = tmp_mem_ctx;
            is_inited_ = true;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(obj_pool_.pop(tmp_obj))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          SERVER_LOG(WARN, "fail to pop object", K(ret), K(max_num_), K(attr_));
        } else {
          ret = OB_SUCCESS;
          SERVER_LOG(DEBUG, "obj pool is empty", K(ret), K(max_num_), K(attr_));
        }
      } else {
        SERVER_LOG(DEBUG, "pop obj for pool", K(ret), K(max_num_), K(attr_));
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(tmp_obj)) { // has no object in pool, need extend
          void *buf = nullptr;
          ObMemAttr attr(MTL_ID(), "TbObjVal", ObCtxIds::DEFAULT_CTX_ID);
          if (OB_ISNULL(mem_ctx_)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "memory context is null", K(ret));
          } else if (OB_ISNULL(buf = mem_ctx_->allocf(sizeof(T), attr))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "fail to alloc memory", K(ret), K(sizeof(T)));
          } else {
            tmp_obj = new (buf) T ();
            SERVER_LOG(DEBUG, "alloc obj from memctx", K(ret), K(sizeof(T)), K(*this));
          }
        }
      }

      if (OB_SUCC(ret)) {
        obj = tmp_obj;
      }
    }

    return ret;
  }
private:
  common::ObArenaAllocator allocator_; // for init obj_pool_
  lib::MemoryContext mem_ctx_; // for alloc T
  bool is_inited_;
  int64_t max_num_;
  lib::ObMemAttr attr_;
  int64_t retire_time_; // us
  common::ObSpinLock lock_;
  common::ObFixedQueue<T> obj_pool_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableObjectPool);
};


} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_COMMON_H_ */
