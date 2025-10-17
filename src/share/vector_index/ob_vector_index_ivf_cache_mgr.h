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

#ifndef OCEANBASE_SHARE_IVF_CACHE_MGR_H_
#define OCEANBASE_SHARE_IVF_CACHE_MGR_H_
#include "share/datum/ob_datum.h"
#include "storage/ob_i_store.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_vector_index_util.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "ob_plugin_vector_index_adaptor.h"

namespace oceanbase
{
namespace share
{
using ObIvfCacheMgrKey = ObTabletID;

enum IvfCacheType {
  IVF_CENTROID_CACHE,
  IVF_PQ_CENTROID_CACHE,
  IVF_PQ_PRECOMPUTE_TABLE_CACHE,
  IVF_CACHE_MAX
};

enum IvfCacheStatus {
  IVF_CACHE_IDLE,
  IVF_CACHE_WRITING,
  IVF_CACHE_COMPLETED
};

struct ObIvfAuxTableInfo
{
  ObIvfAuxTableInfo()
      : centroid_table_id_(OB_INVALID_ID),
        pq_centroid_table_id_(OB_INVALID_ID),
        data_table_id_(OB_INVALID_ID),
        centroid_tablet_ids_(),
        pq_centroid_tablet_ids_(),
        type_(ObVectorIndexAlgorithmType::VIAT_MAX)
  {
    centroid_tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "IvfAuxInfo"));
    pq_centroid_tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "IvfAuxInfo"));
  }
  ~ObIvfAuxTableInfo() {
    reset();
  }
  bool is_valid() const;
  void reset();
  int64_t count() const { return centroid_tablet_ids_.count(); }
  int copy_ith_tablet(int64_t idx, ObIvfAuxTableInfo &dst) const;

  TO_STRING_KV(K_(centroid_table_id), K_(pq_centroid_table_id), K_(centroid_tablet_ids),
               K_(pq_centroid_tablet_ids), K_(data_table_id), K_(type));

  uint64_t centroid_table_id_;
  uint64_t pq_centroid_table_id_;
  uint64_t data_table_id_;
  ObSEArray<ObTabletID, 1> centroid_tablet_ids_;
  ObSEArray<ObTabletID, 1> pq_centroid_tablet_ids_;
  ObVectorIndexAlgorithmType type_;
};

struct IvfCacheKey final
{
public:
  IvfCacheKey() : type_(IvfCacheType::IVF_CACHE_MAX) {}
  IvfCacheKey(const IvfCacheType &type) : type_(type) {}
  ~IvfCacheKey() = default;
  uint64_t hash() const { return murmurhash(&type_, sizeof(type_), 0); }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool is_valid() const
  {
    return type_ >= IvfCacheType::IVF_CENTROID_CACHE && type_ < IvfCacheType::IVF_CACHE_MAX;
  }
  bool is_centroid_cache() const
  {
    return type_ == IvfCacheType::IVF_CENTROID_CACHE
           || type_ == IvfCacheType::IVF_PQ_CENTROID_CACHE;
  }
  bool is_pq_precompute_table() const
  {
    return type_ == IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE;
  }
  bool operator==(const IvfCacheKey &other) const { return type_ == other.type_; }
  TO_STRING_KV(K_(type));

public:
  IvfCacheType type_;
};

class ObIvfICache
{
public:
  friend class ObIvfCacheMgr;

  explicit ObIvfICache(ObIAllocator &allocator, int64_t tenant_id)
      : is_inited_(false),
        self_allocator_(allocator),
        tenant_id_(tenant_id),
        key_(IvfCacheType::IVF_CACHE_MAX),
        sub_mem_ctx_(nullptr),
        rwlock_(),
        status_(IvfCacheStatus::IVF_CACHE_IDLE)
  {}
  virtual ~ObIvfICache();
  virtual void reset();
  virtual void reuse();
  ObIAllocator &get_self_allocator() { return self_allocator_; }

  uint64_t get_actual_memory_used() { return sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->used(); }
  uint64_t get_memory_hold() { return sub_mem_ctx_ == nullptr ? 0 : sub_mem_ctx_->hold(); }
  virtual int64_t get_expect_memory_used(const IvfCacheKey &key, const ObVectorIndexParam &param) = 0;
  RWLock &get_lock() { return rwlock_; }
  OB_INLINE bool is_writing() { return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_WRITING; }
  OB_INLINE bool set_writing_if_idle()
  {
    return ATOMIC_BCAS(&status_, IvfCacheStatus::IVF_CACHE_IDLE, IvfCacheStatus::IVF_CACHE_WRITING);
  }
  OB_INLINE void set_completed() { ATOMIC_STORE(&status_, IvfCacheStatus::IVF_CACHE_COMPLETED); }
  OB_INLINE bool is_completed()
  {
    return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_COMPLETED;
  }
  OB_INLINE bool is_idle() { return ATOMIC_LOAD(&status_) == IvfCacheStatus::IVF_CACHE_IDLE; }
  OB_INLINE void set_idle() { ATOMIC_STORE(&status_, IvfCacheStatus::IVF_CACHE_IDLE); }

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(key_), K(status_));

protected:
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;

  int inner_init(ObIvfMemContext *parent_mem_ctx, uint64_t* all_vsag_use_mem);
  virtual int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
                   const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem) = 0;

  bool is_inited_;
  ObIAllocator &self_allocator_;  // allocator for alloc ObIvfICache self
  int64_t tenant_id_;
  IvfCacheKey key_;
  ObIvfMemContext *sub_mem_ctx_;
  RWLock rwlock_;
  IvfCacheStatus status_;  // atomic
};

class ObIvfCentCache : public ObIvfICache
{
public:
  friend class ObIvfCacheMgr;
  explicit ObIvfCentCache(ObIAllocator &allocator, int64_t tenant_id)
      : ObIvfICache(allocator, tenant_id), centroids_(nullptr), cent_vec_dim_(0), nlist_(0), capacity_(0), count_(0)
  {}
  virtual ~ObIvfCentCache();
  // NOTE(liyao): attention! read_centroid and write_centroid are not thread-safe
  //              if need deep copy, allocator should not be null
  int read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                    ObIAllocator *allocator = nullptr);
  int read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec,
                       bool deep_copy = false, ObIAllocator *allocator = nullptr);
  int write_centroid(const int64_t center_idx, const float *centroid, const int64_t length);
  int write_pq_centroid(int64_t m_idx, const int64_t center_idx, const float *centroid,
                        const int64_t length);
  OB_INLINE int64_t get_capacity() { return capacity_; }
  OB_INLINE int64_t get_count() { return count_; }
  OB_INLINE bool is_full_cache() { return count_ == capacity_; }
  OB_INLINE float* get_centroids() { return centroids_; }
  void reuse() override;
  int64_t get_expect_memory_used(const IvfCacheKey &key, const ObVectorIndexParam &param) override;
  int write_centroid_with_real_idx(const int64_t real_idx, const float *centroid,
                                   const int64_t length);

  TO_STRING_KV(K(centroids_), K(cent_vec_dim_), K(capacity_), K(nlist_));

protected:
  int inner_read_centroid(int64_t centroid_idx, float *&centroid_vec, bool deep_copy = false,
                          ObIAllocator *allocator = nullptr);
  int init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
           const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem) override;
  // NOTE(liyao): to avoid ObIArray using sizeof(float*) * capacity_ extra memory,
  //              we use float* to save centroids_
  float *centroids_;
  int cent_vec_dim_; // dim for flat, dim/m for pq
  int nlist_; // 0 for flat
  int32_t capacity_;
  int32_t count_; // maybe not enough vector for nlist
};

using ObIvfCacheMap = common::hash::ObHashMap<IvfCacheKey, ObIvfICache *>;

class ObIvfCacheMgr
{
public:
  explicit ObIvfCacheMgr(ObIAllocator &allocator, int64_t tenant_id)
      : ref_cnt_(0),
        mem_ctx_(nullptr),
        cache_objs_(),
        is_reach_limit_(false),
        reach_limit_cnt_(0),
        is_inited_(false),
        self_allocator_(allocator),
        tenant_id_(tenant_id),
        cache_mgr_key_(ObTabletID::INVALID_TABLET_ID),
        vec_param_(),
        table_id_(OB_INVALID_ID),
        all_vsag_use_mem_(nullptr)
  {}
  ~ObIvfCacheMgr();
  void reset();
  int init(lib::MemoryContext &parent_mem_ctx, const ObVectorIndexParam &vec_index_param,
           const ObIvfCacheMgrKey &key, int64_t dim, int64_t table_id, uint64_t* all_vsag_use_mem);
  void inc_ref();
  bool dec_ref_and_check_release();
  // only used to alloc/free ObIvfCacheMgr self
  ObIAllocator &get_self_allocator() { return self_allocator_; }
  uint64_t get_actual_memory_used();
  uint64_t get_memory_hold();
  template<typename CacheType>
  int get_or_create_cache_node(const IvfCacheKey &key, CacheType *&cache);

  int get_centroid_cache_size(const IvfCacheKey &);
  int write_pq_centroid_cache(const IvfCacheKey &key, float *centroid_vec, int64_t length);
  int check_memory_limit(int64_t base);
  int fill_cache_info(ObVectorIndexInfo &info);
  OB_INLINE int64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObIvfCacheMgrKey get_cache_mgr_key() const { return cache_mgr_key_; }
  OB_INLINE int64_t get_table_id() const { return table_id_; }

  TO_STRING_KV(K(ref_cnt_), K(is_inited_), K(is_reach_limit_), K(reach_limit_cnt_), K(tenant_id_),
               K(cache_mgr_key_), K(vec_param_), K(table_id_));

private:
  static const int64_t DEFAULT_IVF_CACHE_HASH_SIZE = 10;

  int create_cache_obj(const IvfCacheKey &key, ObIvfICache *&cache_obj);
  void release_cache_obj(ObIvfICache *&cache_obj);

  int64_t ref_cnt_;
  ObIvfMemContext *mem_ctx_;
  ObIvfCacheMap cache_objs_;
  bool is_reach_limit_;
  int reach_limit_cnt_;  // check memory every reach_limit_cnt_ % 10 after is_reach_limit_ = true
  bool is_inited_;
  ObIAllocator &self_allocator_;  // allocator for alloc ObIvfCacheMgr self
  int64_t tenant_id_;
  ObIvfCacheMgrKey cache_mgr_key_;  // equal to index tablet id currently
  ObVectorIndexParam vec_param_;
  int64_t table_id_;
  uint64_t *all_vsag_use_mem_;
};

class ObIvfCacheMgrGuard
{
public:
  ObIvfCacheMgrGuard(ObIvfCacheMgr *cache_mgr = nullptr) : cache_mgr_(cache_mgr) {}
  ~ObIvfCacheMgrGuard();

  bool is_valid() { return cache_mgr_ != nullptr; }
  ObIvfCacheMgr *get_ivf_cache_mgr() { return cache_mgr_; }
  int set_cache_mgr(ObIvfCacheMgr *cache_mgr);
  TO_STRING_KV(KPC_(cache_mgr));

private:
  ObIvfCacheMgr *cache_mgr_;
};

template<typename CacheType>
int ObIvfCacheMgr::get_or_create_cache_node(const IvfCacheKey &key, CacheType *&cache)
{
  int ret = OB_SUCCESS;
  ObIvfICache *icache = nullptr;
  if (OB_FAIL(cache_objs_.get_refactored(key, icache))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      OB_LOG(INFO, "cache obj not exist, create new one", K(key), K(cache_objs_.size()));
      if (OB_FAIL(create_cache_obj(key, icache))) {
        OB_LOG(WARN, "fail to create cache obj", K(ret), K(key));
      } else if (OB_FAIL(cache_objs_.set_refactored(key, icache))) {
        release_cache_obj(icache);
        if (ret == OB_HASH_EXIST) {
          // other thread may already created, try get again.
          if (OB_FAIL(cache_objs_.get_refactored(key, icache))) {
            OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
          }
        } else {
          OB_LOG(WARN, "fail to set cache obj", K(ret), K(key));
        }
      }
    } else {
      OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(icache)) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "fail to get cache obj", K(ret), K(key));
  } else if (OB_ISNULL(cache = reinterpret_cast<CacheType *>(icache))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid cache type", K(ret), K(key));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_SHARE_IVF_CACHE_MGR_H_
