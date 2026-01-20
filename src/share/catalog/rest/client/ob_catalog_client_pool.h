/**
* Copyright (c) 2023 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#ifndef _SHARE_OB_CATALOG_CLIENT_POOL_H
#define _SHARE_OB_CATALOG_CLIENT_POOL_H

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/rc/ob_rc.h"
#include "lib/rc/context.h"
#include "observer/ob_server.h"
#include "share/catalog/rest/client/ob_curl_rest_client.h"
#include "share/catalog/hive/ob_hive_metastore.h"

namespace oceanbase
{

namespace observer
{
  class ObExternalCatalogClientPoolGetter;  // Forward declaration
}

namespace share
{
struct ObCatalogClientPoolKey
{
  public:
  ObCatalogClientPoolKey() : tenant_id_(OB_INVALID_TENANT_ID), catalog_id_(OB_INVALID_ID) {}
  ObCatalogClientPoolKey(const uint64_t &tenant_id, const uint64_t &catalog_id)
    : tenant_id_(tenant_id), catalog_id_(catalog_id) {}
  ~ObCatalogClientPoolKey() {}

  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    hash_ret = murmurhash(&catalog_id_, sizeof(catalog_id_), hash_ret);
    return hash_ret;
  }

  int hash(uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    hash_val = hash();
    return ret;
  }

  bool operator==(const ObCatalogClientPoolKey &other) const
  {
    return tenant_id_ == other.tenant_id_ && catalog_id_ == other.catalog_id_;
  }
  int assign(const ObCatalogClientPoolKey &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      tenant_id_ = other.tenant_id_;
      catalog_id_ = other.catalog_id_;
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(catalog_id));

private:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
};

template <typename ClientInstance>
class ObCatalogClientPool
{
public:
  ObCatalogClientPool(ObIAllocator *allocator)
   : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
     catalog_id_(OB_INVALID_ID), allocator_(allocator),
     idle_clients_(*allocator), cond_(), size_(0), capacity_(0),
     waiting_cnt_(0), ref_cnt_(0), last_access_ts_(0)
  {}

  ~ObCatalogClientPool()
  {
    destroy();
  }

  int resolve_properties(const ObString &properties);

  int init(const uint64_t &tenant_id,
           const uint64_t &catalog_id,
           const ObString &properties)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      SHARE_LOG(WARN, "object pool already inited", K(ret));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "allocator is null", K(ret));
    } else if (OB_FAIL(cond_.init(ObWaitEventIds::CATALOG_CLIENT_POOL_COND_WAIT))) {
      SHARE_LOG(WARN, "failed to init condition", K(ret));
    } else if (OB_FAIL(resolve_properties(properties))) {
      SHARE_LOG(WARN, "fail to resolve properties", K(ret), K(properties));
    } else {
      tenant_id_ = tenant_id;
      catalog_id_ = catalog_id;
      is_inited_ = true;
      refresh_last_access_ts();
    }
    return ret;
  }

  int destroy()
  {
    int ret = OB_SUCCESS;
    {
      ObThreadCondGuard guard(cond_);
      if (!is_inited_) {
        // do nothing
      } else {
        ClientInstance *obj = nullptr;
        if (idle_clients_.size() != ATOMIC_LOAD(&size_)) {
          SHARE_LOG(WARN, "destroy pool, but still have in use clients", K(ret), K(size_), K(idle_clients_.size()));
        }
        int64_t idle_clients_cnt = idle_clients_.size();
        for (int64_t i = 0; i < idle_clients_cnt && OB_SUCC(ret); ++i) {
          if (OB_FAIL(idle_clients_.pop_front(obj))) {
            SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
          } else if (OB_ISNULL(obj)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "obj is null", K(ret));
          } else if (OB_FAIL(destroy_client(obj))) {
            SHARE_LOG(WARN, "failed to destroy client", K(ret));
          }
        }
        is_inited_ = false;
        allocator_ = nullptr;
        SHARE_LOG(INFO, "object pool destroyed");
      }
    }
    return ret;
  }

  int get_client(ClientInstance *&obj)
  {
    int ret = OB_SUCCESS;
    obj = nullptr;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "object pool not inited", K(ret));
    } else if (capacity_ <= 0) {
      if (OB_FAIL(create_client(obj))) {
        SHARE_LOG(WARN, "failed to create object", K(ret));
      }
    } else {
      {
        ObThreadCondGuard guard(cond_);  // 以下操作都加上了cond_.mutex_
        refresh_last_access_ts();
        if (idle_clients_.size() > 0) {  // 有空闲的client
          if (OB_FAIL(idle_clients_.pop_front(obj))) {
            SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
          } else if (OB_ISNULL(obj)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "obj is null", K(ret));
          }
        } else if (ATOMIC_LOAD(&size_) < capacity_) {  // 没有空闲的client，但是pool还有容量
          if (OB_FAIL(create_client(obj))) {
            SHARE_LOG(WARN, "failed to create object", K(ret));
          }
        } else {  // 没有空闲的client，也没有容量
          int64_t remain_time_us = THIS_WORKER.get_timeout_remain();
          while (OB_SUCC(ret)
                 && remain_time_us > 0
                 && idle_clients_.size() <= 0
                 && ATOMIC_LOAD(&size_) >= capacity_) {
            ATOMIC_INC(&waiting_cnt_);
            if (OB_FAIL(cond_.wait_us(remain_time_us))) {
              SHARE_LOG(WARN, "failed to wait for condition", K(ret));
            }
            ATOMIC_DEC(&waiting_cnt_);
            remain_time_us = THIS_WORKER.get_timeout_remain();
            refresh_last_access_ts();
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (remain_time_us <= 0) {
            ret = OB_TIMEOUT;
            SHARE_LOG(WARN, "timeout waiting for client", K(ret));
          } else if (idle_clients_.size() > 0) {
            if (OB_FAIL(idle_clients_.pop_front(obj))) {
              SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
            }
          } else if (ATOMIC_LOAD(&size_) < capacity_) {
            if (OB_FAIL(create_client(obj))) {
              SHARE_LOG(WARN, "failed to create object", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "unexpected error", K(ret), K(remain_time_us), K(idle_clients_.size()), K(size_), K(capacity_));
          }
        }
      }
    }
    return ret;
  }

  int return_client(ClientInstance *&obj)
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "object pool not inited", K(ret));
    } else if (OB_ISNULL(obj)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "obj is null", K(ret));
    } else if (capacity_ <= 0) {
      if (OB_FAIL(destroy_client(obj))) {
        SHARE_LOG(WARN, "failed to destroy client", K(ret));
      }
      obj = nullptr;
    } else {
      {
        ObThreadCondGuard guard(cond_);
        refresh_last_access_ts();
        if (OB_FAIL(idle_clients_.push_back(obj))) {  // todo@lekou: 支持alter catalog后需要考虑调整pool容量的情况
          SHARE_LOG(WARN, "failed to push back to idle_objects", K(ret));
          destroy_client(obj);
        } else {
          obj = nullptr;
        }
        cond_.signal();
      }
    }
    return ret;
  }

  int64_t get_size() const { return ATOMIC_LOAD(&size_); }

  int64_t get_capacity() const { return capacity_; }

  bool is_inited() const { return is_inited_; }

  int64_t get_ref_cnt() const {
    return ATOMIC_LOAD(&ref_cnt_);
  }
  void inc_ref_cnt() {
    ATOMIC_INC(&ref_cnt_);
  }
  void dec_ref_cnt() {
    ATOMIC_DEC(&ref_cnt_);
  }

  int64_t get_waiting_cnt() const {
    return ATOMIC_LOAD(&waiting_cnt_);
  }

  int64_t get_last_access_ts() const { return last_access_ts_; }

  int64_t get_idle_clients_cnt() const { return idle_clients_.size(); }

  int64_t get_in_use_clients_cnt() const { return ATOMIC_LOAD(&size_) - idle_clients_.size(); }

  int64_t get_tenant_id() const { return tenant_id_; }

  int64_t get_catalog_id() const { return catalog_id_; }
  const ObString &get_uri() const { return uri_; }

  int clean() {
    int ret = OB_SUCCESS;
    {
      ObThreadCondGuard guard(cond_);
      if (!is_inited_) {
        // maybe destroyed
        SHARE_LOG(WARN, "object pool is not inited", K(ret));
      } else {
        const int64_t idle_time = ObTimeUtility::current_time() - last_access_ts_;
        if (idle_time >= IDLE_THRESHOLD_US) {
          ClientInstance *obj = nullptr;
          int64_t idle_clients_cnt = idle_clients_.size();
          for (int64_t i = 0; i < idle_clients_cnt && OB_SUCC(ret); ++i) {
            if (OB_FAIL(idle_clients_.pop_front(obj))) {
              SHARE_LOG(WARN, "failed to pop front from idle_clients", K(ret));
            } else if (OB_ISNULL(obj)) {
              ret = OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "obj is null", K(ret));
            } else if (OB_FAIL(destroy_client(obj))) {
              SHARE_LOG(WARN, "failed to destroy client", K(ret));
            }
          }
        }
      }
    }
    return ret;
  }
  int init_client(void *ptr, ObCurlRestClient *&obj)
  {
    int ret = OB_SUCCESS;
    obj = new(ptr) ClientInstance(*allocator_);
    if (OB_FAIL(obj->init(rest_properties_))) {
      allocator_->free(ptr);
      SHARE_LOG(WARN, "failed to init object", K(ret));
    }
    return ret;
  }
  int init_client(void *ptr, ObHiveMetastoreClient *&obj)
  {
    int ret = OB_SUCCESS;
    obj = new(ptr) ClientInstance(allocator_);
    if (OB_FAIL(obj->init(hms_properties_))) {
      allocator_->free(ptr);
      SHARE_LOG(WARN, "failed to init object", K(ret));
    }
    return ret;
  }
private:
  int create_client(ClientInstance *&obj)
  {
    int ret = OB_SUCCESS;
    obj = nullptr;
    if (OB_ISNULL(allocator_)) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "allocator is null", K(ret));
    } else {
      void *ptr = allocator_->alloc(sizeof(ClientInstance));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to allocate memory for object", K(ret), K(sizeof(ClientInstance)));
      } else if (OB_FAIL(init_client(ptr, obj))) {
        SHARE_LOG(WARN, "failed to init client", K(ret));
      } else {
        obj->set_client_pool(this);
        ATOMIC_INC(&size_);
      }
    }
    return ret;
  }

  int destroy_client(ClientInstance *obj)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(obj)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "obj is null", K(ret));
    } else if (OB_FALSE_IT(obj->~ClientInstance())) {
      // do nothing
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "allocator is null", K(ret));
    } else {
      allocator_->free(obj);
      ATOMIC_DEC(&size_);
    }
    return ret;
  }

  void refresh_last_access_ts() {
    last_access_ts_ = ObTimeUtility::current_time();
  }

  // 对象池自身机制
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObIAllocator *allocator_;
  ObList<ClientInstance*, ObIAllocator> idle_clients_;
  ObThreadCond cond_;
  volatile int64_t size_;
  int64_t capacity_;
  volatile int64_t waiting_cnt_;  // 等待线程的数量
  volatile int64_t ref_cnt_;      // pool对象的引用计数
  int64_t last_access_ts_;        // 最后一次从pool取/还client的时间

  union {
    ObRestCatalogProperties rest_properties_;
    ObHMSCatalogProperties hms_properties_;
  };
  ObString uri_;  // 深拷贝为c-style字符串, 避免展示池化信息时乱码
  static constexpr int64_t IDLE_THRESHOLD_US = 5LL * 60LL * 1000LL * 1000LL;  // 5 minutes, 2 minutes for debug
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPool);
};

template<typename ClientInstance>
inline int ObCatalogClientPool<ClientInstance>::resolve_properties(const ObString &properties) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(rest_properties_.load_from_string(properties, *allocator_))) {
    SHARE_LOG(WARN, "fail to init rest properties", K(ret), K(properties));
  } else if (OB_FAIL(rest_properties_.decrypt(*allocator_))) {
    SHARE_LOG(WARN, "fail to decrypt rest properties", K(ret));
  } else {
    capacity_ = rest_properties_.max_client_pool_size_;
    if (OB_FAIL(ob_write_string(*allocator_, rest_properties_.uri_, uri_, true /*c_style*/))) {
      SHARE_LOG(WARN, "failed to write uri", K(ret), K(rest_properties_.uri_));
    }
  }
  return ret;
}

template<>
inline int ObCatalogClientPool<ObHiveMetastoreClient>::resolve_properties(const ObString &properties) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(hms_properties_.load_from_string(properties, *allocator_))) {
    SHARE_LOG(WARN, "fail to init hms properties", K(ret), K(properties));
  } else if (OB_FAIL(hms_properties_.decrypt(*allocator_))) {
    SHARE_LOG(WARN, "fail to decrypt hms properties", K(ret));
  } else {
    capacity_ = hms_properties_.max_client_pool_size_;
    if (OB_FAIL(ob_write_string(*allocator_, hms_properties_.uri_, uri_, true /*c_style*/))) {
      SHARE_LOG(WARN, "failed to write uri", K(ret), K(hms_properties_.uri_));
    }
  }
  return ret;
}

template <typename ClientInstance>
class ObCatalogClientPoolClearTask : public common::ObTimerTask
{
public:
  ObCatalogClientPoolClearTask() : pool_mgr_(NULL) {}
  void runTimerTask(void) override
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(pool_mgr_)) {
      SHARE_LOG(INFO, "[clean_rest_client] run timer task to clean rest client");
      if (OB_FAIL(pool_mgr_->do_clean_task())) {
        SHARE_LOG(WARN, "[clean_rest_client] failed to clean rest client", K(ret));
      }
    }
  }
  ObCatalogClientPoolMgr<ClientInstance> *pool_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPoolClearTask);
};

template <typename ClientInstance>
class ObCatalogClientPoolMgr
{
public:
  ObCatalogClientPoolMgr()
  : is_inited_(false), allocator_(nullptr),
    mem_context_(nullptr), tg_id_(-1),
    pool_map_(), bucket_num_(0),
    pool_map_lock_(ObLatchIds::OBJECT_DEVICE_LOCK)
  {}

  ~ObCatalogClientPoolMgr()
  {
    destroy();
  }

  int init_memory_context(const uint64_t &tenant_id)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(tenant_id, CLIENT_POOL_ALLOCATOR, ObCtxIds::DEFAULT_CTX_ID)
          .set_properties(lib::ALLOC_THREAD_SAFE)
          .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
      if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        SHARE_LOG(WARN, "failed to create memory entity", K(ret));
      } else if (OB_ISNULL(mem_context_)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to create memory entity", K(ret));
      } else {
        allocator_ = &mem_context_->get_malloc_allocator();
      }
    }
    return ret;
  }

  int init(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      SHARE_LOG(WARN, "object pool manager already inited", K(ret));
    } else if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_INVALID_TENANT_ID;
      SHARE_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
      SHARE_LOG(WARN, "failed to create tg", K(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      SHARE_LOG(WARN, "failed to start tg", K(ret));
    } else if (OB_FAIL(init_memory_context(tenant_id))) {
      SHARE_LOG(WARN, "failed to init memory context", K(ret));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "allocator is not init", K(ret));
    } else {
      clear_task_.pool_mgr_ = this;
      const ObMemAttr attr(tenant_id, CLIENT_POOL_ALLOCATOR);
      if (OB_FAIL(pool_map_.create(MAX_CLIENT_POOL_CNT, attr))) {
        SHARE_LOG(WARN, "failed to create pool map", K(ret));
      } else if (OB_FAIL(TG_SCHEDULE(tg_id_, clear_task_, CLIENT_POOL_CLEANUP_INTERVAL_US, true))) {
        SHARE_LOG(WARN, "failed to schedule clear task", K(ret));
      } else {
        is_inited_ = true;
        bucket_num_ = MAX_CLIENT_POOL_CNT;
        SHARE_LOG(INFO, "object pool manager inited successfully");
      }
    }
    return ret;
  }

  int destroy()
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      // do nothing
    } else {
      TG_DESTROY(tg_id_);
      {
        SpinWLockGuard guard(pool_map_lock_);
        typename common::hash::ObHashMap<ObCatalogClientPoolKey, ObCatalogClientPool<ClientInstance> *>::iterator iter = pool_map_.begin();
        for (; OB_SUCC(ret) && iter != pool_map_.end(); ++iter) {
          ObCatalogClientPool<ClientInstance> *pool = iter->second;
          if (OB_NOT_NULL(pool)) {
            if (OB_FAIL(pool->destroy())) {
              SHARE_LOG(WARN, "failed to destroy pool", K(ret));
            }
            if (OB_NOT_NULL(allocator_)) {
              allocator_->free(pool);
            }
          }
        }
        pool_map_.destroy();
        is_inited_ = false;
        allocator_ = nullptr;
        bucket_num_ = 0;
        if (OB_LIKELY(nullptr != mem_context_)) {
          DESTROY_CONTEXT(mem_context_);
          mem_context_ = nullptr;
        }
      }
      SHARE_LOG(INFO, "object pool manager releaseed");
    }
    return ret;
  }

  int get_client(const int64_t tenant_id, const int64_t catalog_id, const ObString &properties, ClientInstance *&obj)
  {
    int ret = OB_SUCCESS;
    obj = nullptr;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "object pool manager not inited", K(ret));
    } else {
      ObCatalogClientPool<ClientInstance> *pool = nullptr;
      if (OB_FAIL(find_pool(tenant_id, catalog_id, properties, pool))) {  // 获取pool指针并增加引用计数
        SHARE_LOG(WARN, "failed to get or create pool", K(ret), K(tenant_id), K(catalog_id));
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "pool is null", K(ret));
      } else if (OB_FAIL(pool->get_client(obj))) {  // 刚取出指针，还未获取client，clean线程可能清除pool？增加ref_cnt_
        pool->dec_ref_cnt();
        SHARE_LOG(WARN, "failed to get object from pool", K(ret), K(tenant_id), K(catalog_id));
      } else {
        pool->dec_ref_cnt();
      }
    }
    return ret;
  }

  int do_clean_task()
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "[clean_rest_client] object pool manager not inited", K(ret));
    } else {
      // clean each pool
      ObArray<ObCatalogClientPoolKey> pools_to_remove;
      {
        SpinRLockGuard guard(pool_map_lock_);
        typename common::hash::ObHashMap<ObCatalogClientPoolKey, ObCatalogClientPool<ClientInstance> *>::iterator iter = pool_map_.begin();
        for (; OB_SUCC(ret) && iter != pool_map_.end(); ++iter) {
          const ObCatalogClientPoolKey &key = iter->first;
          ObCatalogClientPool<ClientInstance> *pool = iter->second;
          const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();
          if (OB_ISNULL(pool)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "[clean_rest_client] pool is null", K(ret), K(key));
          } else if (OB_FAIL(pool->clean())) {
            SHARE_LOG(WARN, "[clean_rest_client] failed to clean pool", K(ret), K(key));
          } else if (pool->get_size() == 0            // client数量为0
                     && pool->get_ref_cnt() == 0      // 持有该pool指针的线程数量为0
                     && pool->get_waiting_cnt() == 0  // 等待获取client的线程数量为0
                     && idle_time >= CLIENT_POOL_REMOVE_THRESHOLD_US  // pool空闲时间大于阈值
                     && OB_FAIL(pools_to_remove.push_back(key))) {
            SHARE_LOG(WARN, "[clean_rest_client] failed to push back key", K(ret), K(key));
          }
          SHARE_LOG(INFO, "[clean_rest_client] clean pool", K(ret), K(key), K(idle_time), K(CLIENT_POOL_REMOVE_THRESHOLD_US), K(pool->get_size()), K(pool->get_ref_cnt()), K(pool->get_waiting_cnt()));
          ret = OB_SUCCESS; // reset ret
        }
      }
      SHARE_LOG(INFO, "[clean_rest_client] clean pool", K(ret), K(pools_to_remove.count()));
      // check if should remove pool
      {
        SpinWLockGuard guard(pool_map_lock_);
        for (int i = 0; OB_SUCC(ret) && i < pools_to_remove.count(); ++i) {
          const ObCatalogClientPoolKey &key = pools_to_remove[i];
          ObCatalogClientPool<ClientInstance> *pool = nullptr;
          if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
            SHARE_LOG(WARN, "[clean_rest_client] failed to get pool", K(ret), K(key));
          } else if (OB_ISNULL(pool)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "[clean_rest_client] pool is null", K(ret));
          } else {
            const int64_t idle_time = ObTimeUtility::current_time() - pool->get_last_access_ts();
            SHARE_LOG(INFO, "[clean_rest_client] clean pool", K(ret), K(key), K(idle_time), K(CLIENT_POOL_REMOVE_THRESHOLD_US), K(pool->get_size()), K(pool->get_ref_cnt()), K(pool->get_waiting_cnt()));
            if (pool->get_size() == 0            // client数量为0
                && pool->get_ref_cnt() == 0      // 持有该pool指针的线程数量为0
                && pool->get_waiting_cnt() == 0  // 等待获取client的线程数量为0
                && idle_time >= CLIENT_POOL_REMOVE_THRESHOLD_US  // pool空闲时间大于阈值
                ) {
              if (OB_FAIL(pool_map_.erase_refactored(key))) {
                SHARE_LOG(WARN, "[clean_rest_client] failed to erase pool from map", K(ret), K(key));
              } else if (OB_FAIL(pool->destroy())) {
                SHARE_LOG(WARN, "[clean_rest_client] failed to destroy pool", K(ret), K(key));
              } else if (OB_ISNULL(allocator_)) {
                ret = OB_ERR_UNEXPECTED;
                SHARE_LOG(WARN, "[clean_rest_client] allocator is null", K(ret));
              } else {
                allocator_->free(pool);
                pool = nullptr;
                SHARE_LOG(INFO, "[clean_rest_client] pool removed successfully", K(key));
              }
            }
          }
          ret = OB_SUCCESS; // reset ret
        }
      }
    }
    return ret;
  }

  template <typename T>
  int get_pool_stat_info(T &getter)
  {
    int ret = OB_SUCCESS;
    {
      SpinRLockGuard guard(pool_map_lock_);
      if (!is_inited_) {
        ret = OB_NOT_INIT;
        SHARE_LOG(WARN, "object pool manager not inited", K(ret));
      } else if (OB_FAIL(pool_map_.foreach_refactored(getter))) {
        SHARE_LOG(WARN, "fail to get pool stat info", K(ret));
      }
    }
    return ret;
  }

  int64_t get_pool_size() const { return pool_map_.size(); }

  int get_tg_id() const { return tg_id_; }

private:
  int find_pool(const int64_t tenant_id, const int64_t catalog_id,
                const ObString &properties, ObCatalogClientPool<ClientInstance> *&pool)
  {
    int ret = OB_SUCCESS;
    pool = nullptr;
    bool find_pool = true;
    ObCatalogClientPoolKey key(tenant_id, catalog_id);
    {
      SpinRLockGuard guard(pool_map_lock_);
      if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          find_pool = false;
        } else {
          SHARE_LOG(WARN, "failed to get pool from map", K(ret), K(key));
        }
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "pool is null", K(ret));
      } else {
        pool->inc_ref_cnt();
      }
    }

    if (!find_pool && OB_SUCC(ret)) {
      SpinWLockGuard guard(pool_map_lock_);
      if (OB_FAIL(pool_map_.get_refactored(key, pool))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(create_pool(tenant_id, catalog_id, properties, pool))) {
            SHARE_LOG(WARN, "failed to create pool", K(ret), K(key));
          } else if (OB_ISNULL(pool)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "pool is null", K(ret));
          } else {
            pool->inc_ref_cnt();
          }
        } else {
          SHARE_LOG(WARN, "failed to get pool from map", K(ret), K(key));
        }
      } else if (OB_ISNULL(pool)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "pool is null", K(ret));
      } else {
        pool->inc_ref_cnt();
      }
    }
    return ret;
  }

  int create_pool(const int64_t tenant_id, const int64_t catalog_id,
                  const ObString &properties, ObCatalogClientPool<ClientInstance> *&pool)
  {
    int ret = OB_SUCCESS;
    pool = nullptr;
    if (OB_ISNULL(allocator_)) {
      ret = OB_NOT_INIT;
      SHARE_LOG(WARN, "allocator is null", K(ret));
    } else if (pool_map_.size() >= bucket_num_) {
      ret = OB_SIZE_OVERFLOW;
      SHARE_LOG(WARN, "pool map size overflow", K(ret), K(pool_map_.size()), K(bucket_num_));
    } else {
      ObCatalogClientPoolKey key(tenant_id, catalog_id);
      void *ptr = allocator_->alloc(sizeof(ObCatalogClientPool<ClientInstance>));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "failed to allocate memory for pool", K(ret), K(sizeof(ObCatalogClientPool<ClientInstance>)));
      } else {
        pool = new(ptr) ObCatalogClientPool<ClientInstance>(allocator_);
        if (OB_FAIL(pool->init(tenant_id, catalog_id, properties))) {
          SHARE_LOG(WARN, "failed to init pool", K(ret));
          pool->~ObCatalogClientPool<ClientInstance>();
          allocator_->free(pool);
          pool = nullptr;
        } else if (OB_FAIL(pool_map_.set_refactored(key, pool))) {
          SHARE_LOG(WARN, "failed to set pool to map", K(ret), K(key));
          pool->destroy();
          pool->~ObCatalogClientPool<ClientInstance>();
          allocator_->free(pool);
          pool = nullptr;
        }
      }
    }
    return ret;
  }

private:
  bool is_inited_;
  ObIAllocator *allocator_;
  lib::MemoryContext mem_context_;
  int tg_id_;
  common::hash::ObHashMap<ObCatalogClientPoolKey, ObCatalogClientPool<ClientInstance> *> pool_map_;
  int64_t bucket_num_;
  SpinRWLock pool_map_lock_;
  ObCatalogClientPoolClearTask<ClientInstance> clear_task_;
  static constexpr int64_t MAX_CLIENT_POOL_CNT = 100;  // 100, 2 for debug
  static constexpr const char *CLIENT_POOL_ALLOCATOR = "ClientPoolAlloc";
  static constexpr int64_t CLIENT_POOL_CLEANUP_INTERVAL_US = 5LL * 60LL * 1000LL * 1000LL;  // 5 minutes, 2 minutes for debug
  static constexpr int64_t CLIENT_POOL_REMOVE_THRESHOLD_US = 10LL * 60LL * 1000LL * 1000LL;  // 10 minutes, 3 minutes for debug
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogClientPoolMgr);
};

}
}

#endif /* _SHARE_OB_CATALOG_CLIENT_POOL_H */