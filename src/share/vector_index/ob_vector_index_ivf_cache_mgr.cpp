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

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
/////////////////////////////////////
// implement of ObIvfCacheMgrGuard //
/////////////////////////////////////

ObIvfCacheMgrGuard::~ObIvfCacheMgrGuard()
{
  reset();
}

void ObIvfCacheMgrGuard::reset()
{
  if (is_valid()) {
    if (cache_mgr_->dec_ref_and_check_release()) {
      ObIAllocator &allocator = cache_mgr_->get_self_allocator();
      LOG_INFO("ivf cache mgr released", KPC(cache_mgr_), K(lbt()));
      cache_mgr_->~ObIvfCacheMgr();
      allocator.free(cache_mgr_);
    }
    cache_mgr_ = nullptr;
  }
}

int ObIvfCacheMgrGuard::set_cache_mgr(ObIvfCacheMgr *cache_mgr)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index cache_mgr guard can only set once", KPC(cache_mgr_), KPC(cache_mgr));
  } else {
    cache_mgr_ = cache_mgr;
    cache_mgr_->inc_ref();
  }
  return ret;
}

/////////////////////////////////
// implement of ObIvfCacheMgr //
////////////////////////////////

void ObIvfCacheMgr::reset()
{
  ref_cnt_ = 0;
  is_reach_limit_ = false;
  reach_limit_cnt_ = 0;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cache_mgr_key_.reset();
  vec_param_.reset();
  ObIAllocator &allocator = get_self_allocator();

  FOREACH(iter, cache_objs_) { release_cache_obj(iter->second); }
  if (OB_NOT_NULL(mem_ctx_)) {
    mem_ctx_->~ObIvfMemContext();
    allocator.free(mem_ctx_);
    mem_ctx_ = nullptr;
  }
}

ObIvfCacheMgr::~ObIvfCacheMgr()
{
  reset();
}

int ObIvfCacheMgr::init(lib::MemoryContext &parent_mem_ctx,
                        const ObVectorIndexParam &vec_index_param, const ObIvfCacheMgrKey &key,
                        int64_t dim, int64_t table_id, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "IvfCacheCtx");
  SET_IGNORE_MEM_VERSION(attr);
  if (!key.is_valid() || dim <= 0 || OB_ISNULL(all_vsag_use_mem)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id or dim", K(ret), K(key), K(dim), KP(all_vsag_use_mem));
  } else if (OB_ISNULL(mem_ctx_ = OB_NEWx(ObIvfMemContext, &get_self_allocator(), all_vsag_use_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem_ctx", K(ret));
  } else if (OB_FAIL(mem_ctx_->init(parent_mem_ctx, all_vsag_use_mem, tenant_id_))) {
    LOG_WARN("failed to init memory context", K(ret));
    reset();
  } else if (OB_FAIL(cache_objs_.create(DEFAULT_IVF_CACHE_HASH_SIZE, attr, attr))) {
    LOG_WARN("fail to create full index adapter map", KR(ret), K(attr));
    reset();
  } else {
    vec_param_ = vec_index_param;
    vec_param_.dim_ = dim;
    cache_mgr_key_ = key;
    table_id_ = table_id;
    all_vsag_use_mem_ = all_vsag_use_mem;
    is_inited_ = true;
  }
  return ret;
}

void ObIvfCacheMgr::inc_ref()
{
  int64_t ref_count = ATOMIC_AAF(&ref_cnt_, 1);
  // LOG_INFO("inc ref count", K(ref_count), KP(this), KPC(this), K(lbt())); // remove later
}

bool ObIvfCacheMgr::dec_ref_and_check_release()
{
  int64_t ref_count = ATOMIC_SAF(&ref_cnt_, 1);
  // LOG_INFO("dec ref count", K(ref_count), KP(this), KPC(this), K(lbt()));
  return (ref_count == 0);
}

int ObIvfCacheMgr::check_memory_limit(int64_t base)
{
  int ret = OB_SUCCESS;
  int64_t tenant_mem_size = 0;
  int64_t curr_used = ATOMIC_LOAD(all_vsag_use_mem_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfCacheMgr not init", K(ret));
  } else if (OB_ISNULL(mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem ctx is null", K(ret));
  } else if (!is_reach_limit_) {
    if (OB_FAIL(
            ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, tenant_mem_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
    } else if (curr_used + base > tenant_mem_size) {
      is_reach_limit_ = true;
    }
  } else if (reach_limit_cnt_ >= 10) {
    // check is memory limit changed
    reach_limit_cnt_ = 0;
    if (OB_FAIL(
            ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id_, tenant_mem_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id_));
    } else if (curr_used + base < tenant_mem_size) {
      is_reach_limit_ = false;
    }
  } else {
    ++reach_limit_cnt_;
  }
  if (OB_SUCC(ret) && is_reach_limit_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Memory usage exceeds user limit.",
              K(ret),
              K(tenant_mem_size),
              K(curr_used),
              K(base));
  }
  return ret;
}

void ObIvfCacheMgr::release_cache_obj(ObIvfICache *&cache_obj)
{
  if (OB_NOT_NULL(cache_obj)) {
    cache_obj->~ObIvfICache();
    if (OB_NOT_NULL(mem_ctx_)) {
      mem_ctx_->Deallocate(cache_obj);
    }
    cache_obj = nullptr;
  }
}

int ObIvfCacheMgr::create_cache_obj(const IvfCacheKey &key, ObIvfICache *&cache_obj)
{
  int ret = OB_SUCCESS;
  switch (key.type_) {
  case IvfCacheType::IVF_PQ_CENTROID_CACHE:
  case IvfCacheType::IVF_CENTROID_CACHE:
  case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
    ObIvfCentCache *tmp_cent_cache = nullptr;
    void *tmp_buf = nullptr;
    if (OB_ISNULL(tmp_buf = mem_ctx_->Allocate(sizeof(ObIvfCentCache)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc tmp_buf", K(ret));
    } else {
      tmp_cent_cache = new(tmp_buf) ObIvfCentCache(get_self_allocator(), tenant_id_);
      cache_obj = tmp_cent_cache;
    }
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IvfCacheKey", K(ret), K(key));
  }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_memory_limit(cache_obj->get_expect_memory_used(key, vec_param_)))) {
    LOG_WARN("fail to check memory limit", K(ret));
  } else if (OB_FAIL(cache_obj->init(mem_ctx_, key, vec_param_, all_vsag_use_mem_))) {
    LOG_WARN("fail to init cache obj", K(ret));
  } else if (OB_FAIL(check_memory_limit(0))) {
    LOG_WARN("fail to check memory limit", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cache_obj)) {
    release_cache_obj(cache_obj);
  }

  return ret;
}

int ObIvfCacheMgr::fill_cache_info(ObVectorIndexInfo &info){
  int ret = OB_SUCCESS;
  info.rowkey_vid_tablet_id_ = cache_mgr_key_.id();
  info.rowkey_vid_table_id_ = table_id_;
  info.index_type_ = vec_param_.type_;
  ObVectorIndexParam *param;
  int64_t pos = 0;
  #define STAT_PRINT(...) \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(databuff_printf(info.statistics_, sizeof(info.statistics_), pos, __VA_ARGS__))) { \
      LOG_WARN("failed to fill statistics", K(ret)); \
    } \
  }
  STAT_PRINT("{");
  STAT_PRINT("\"actual_memory_used\":%ld", get_actual_memory_used());
  STAT_PRINT(",\"ref_cnt\":%ld", ref_cnt_);
  STAT_PRINT(",\"is_reach_limit\":%d", is_reach_limit_);
  STAT_PRINT(",\"reach_limit_cnt\":%d", reach_limit_cnt_);
  STAT_PRINT(",\"is_inited\":%d", is_inited_);
  STAT_PRINT(",\"param\":\"");
  if (OB_SUCC(ret)) {
    if (OB_FAIL(vec_param_.print_to_string(info.statistics_, sizeof(info.statistics_), pos))) {
      LOG_WARN("failed to print param", K(ret));
    }
  }
  STAT_PRINT("\"");
  // print cache_objs_info
  STAT_PRINT(",\"cache_objs_info\":[");
  if (OB_SUCC(ret)) {
    bool first_obj = true;
    FOREACH_X(iter, cache_objs_, OB_SUCC(ret))
    {
      IvfCacheType cache_type = iter->first.type_;
      if (!first_obj) {
        STAT_PRINT(",");
      }
      first_obj = false;
      STAT_PRINT("{\"cache_type\":%d", cache_type);
      STAT_PRINT(",\"mem_used\":%lu", iter->second->get_actual_memory_used());
      STAT_PRINT(",\"mem_hold\":%lu", iter->second->get_memory_hold());
      switch (cache_type) {
        case IvfCacheType::IVF_CENTROID_CACHE:
        case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE:
        case IvfCacheType::IVF_PQ_CENTROID_CACHE:
        {
          ObIvfCentCache *cache = dynamic_cast<ObIvfCentCache*>(iter->second);
          STAT_PRINT(",\"cache_info\":{");
          STAT_PRINT("\"capacity\":%ld", cache->get_capacity());
          STAT_PRINT(",\"cent_vec_dim\":%d", cache->cent_vec_dim_);
          STAT_PRINT(",\"count\":%d", cache->count_);
          STAT_PRINT(",\"nlist\":%d", cache->nlist_);
          STAT_PRINT("}");
          break;
        }
        case IvfCacheType::IVF_CACHE_MAX:
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported cache for __all_virtual_vector_index_info", K(cache_type));
          break;
      }
      STAT_PRINT("}");
    }
  }
  STAT_PRINT("]");
  STAT_PRINT("}");
  #undef STAT_PRINT
  return ret;
}

uint64_t ObIvfCacheMgr::get_actual_memory_used()
{
  uint64_t used = 0;
  FOREACH(iter, cache_objs_)
  {
    ObIvfICache *cache = dynamic_cast<ObIvfICache *>(iter->second);
    used += cache->get_actual_memory_used();
  }
  used += mem_ctx_->used();
  return used;
}

uint64_t ObIvfCacheMgr::get_memory_hold()
{
  uint64_t hold = 0;
  FOREACH(iter, cache_objs_)
  {
    ObIvfICache *cache = dynamic_cast<ObIvfICache *>(iter->second);
    hold += cache->get_memory_hold();
  }
  hold += (mem_ctx_ == nullptr ? 0 : mem_ctx_->hold());
  return hold;
}

///////////////////////////////
// implement of ObIvfICache //
//////////////////////////////

void ObIvfICache::reset()
{
  is_inited_ = false;
  key_ = IvfCacheType::IVF_CACHE_MAX;
  status_ = IvfCacheStatus::IVF_CACHE_IDLE;
  ObIAllocator &allocator = get_self_allocator();
  if (OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->~ObIvfMemContext();
    allocator.free(sub_mem_ctx_);
    sub_mem_ctx_ = nullptr;
  }
}

void ObIvfICache::reuse()
{
  status_ = IvfCacheStatus::IVF_CACHE_IDLE;
}

ObIvfICache::~ObIvfICache()
{
  ObIvfICache::reset();
}

int ObIvfICache::inner_init(ObIvfMemContext *parent_mem_ctx, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_mem_ctx_ = OB_NEWx(ObIvfMemContext, &get_self_allocator(), all_vsag_use_mem))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create mem_ctx", K(ret));
  } else if (OB_FAIL(sub_mem_ctx_->init(parent_mem_ctx->get_mem_context(), all_vsag_use_mem, tenant_id_))) {
    LOG_WARN("failed to init memory context", K(ret));
    reset();
  }
  return ret;
}

/////////////////////////////////
// implement of ObIvfCentCache //
////////////////////////////////

void ObIvfCentCache::reuse()
{
  MEMSET(centroids_, 0, sizeof(float) * capacity_ * cent_vec_dim_);
  count_ = 0;
  ObIvfICache::reuse();
}

ObIvfCentCache::~ObIvfCentCache()
{
  if (OB_NOT_NULL(centroids_) && OB_NOT_NULL(sub_mem_ctx_)) {
    sub_mem_ctx_->Deallocate(centroids_);
    centroids_ = nullptr;
  }
  cent_vec_dim_ = 0;
  capacity_ = 0;
  count_ = 0;
}

int ObIvfCentCache::init(ObIvfMemContext *parent_mem_ctx, const IvfCacheKey &key,
                         const ObVectorIndexParam &param, uint64_t* all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIvfICache::inner_init(parent_mem_ctx, all_vsag_use_mem))) {
    LOG_WARN("fail to do ObIvfICache inner init", K(ret));
  } else {
    switch (key.type_) {
      case IvfCacheType::IVF_CENTROID_CACHE: {
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * param.nlist_ * param.dim_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(param.nlist_), K(param.dim_), K(key));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * param.nlist_ * param.dim_);
          capacity_ = param.nlist_;
          cent_vec_dim_ = param.dim_;
          nlist_ = param.nlist_;
          count_ = 0;
        }
        break;
      }
      case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
        int64_t pqnlist = 1L << param.nbits_;
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * pqnlist * param.dim_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(pqnlist), K(param.dim_), K(key));
        } else if (param.m_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected m value", K(ret), K(param.m_));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * pqnlist * param.dim_);
          capacity_ = pqnlist * param.m_;
          cent_vec_dim_ = param.dim_ / param.m_;
          nlist_ = pqnlist;
          count_ = 0;
        }
        break;
      }
      case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
        int64_t ksub = 1L << param.nbits_;
        if (OB_ISNULL(centroids_ = static_cast<float *>(
                          sub_mem_ctx_->Allocate(sizeof(float) * param.nlist_ * param.m_ * ksub)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to init centroids", K(ret), K(param.nlist_), K(param.m_), K(ksub), K(key));
        } else {
          MEMSET(centroids_, 0, sizeof(float) * param.nlist_ * param.m_ * ksub);
          capacity_ = param.nlist_ * param.m_ * ksub;
          cent_vec_dim_ = 1; // just a float distance
          nlist_ = 1;
          count_ = 0;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid IvfCacheKey", K(ret), K(key));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    key_ = key;
    is_inited_ = true;
  }

  return ret;
}

int ObIvfCentCache::write_centroid_with_real_idx(const int64_t real_idx, const float *centroid,
                                                 const int64_t length)
{
  int ret = OB_SUCCESS;
  if ((length != cent_vec_dim_ * sizeof(float)) || OB_ISNULL(centroid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid centroid vec length", K(ret), K(length), K(cent_vec_dim_), KP(centroid));
  } else if (real_idx >= capacity_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid vec is full", K(ret), K(real_idx), K(capacity_));
  } else {
    MEMCPY(centroids_ + real_idx * cent_vec_dim_, centroid, cent_vec_dim_ * sizeof(float));
    count_++;
  }
  return ret;
}

int ObIvfCentCache::write_pq_centroid(int64_t m_idx, const int64_t center_idx,

                                      const float *centroid, const int64_t length)
{
  return write_centroid_with_real_idx((m_idx - 1) * nlist_ + (center_idx - 1), centroid, length);
}

int ObIvfCentCache::write_centroid(const int64_t center_idx, const float *centroid,
                                   const int64_t length)
{
  return write_centroid_with_real_idx(center_idx - 1, centroid, length);
}

int ObIvfCentCache::inner_read_centroid(int64_t centroid_idx, float *&centroid_vec,
                                        bool deep_copy /* = false*/,
                                        ObIAllocator *allocator /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (!is_completed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not read cache that not completed", K(ret), K(capacity_));
  } else if (centroid_idx >= get_capacity()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("centroid idx is out of range", K(ret), K(centroid_idx));
  } else if (OB_ISNULL(centroids_ + centroid_idx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null centroid", K(ret), K(centroid_idx));
  } else if (deep_copy) {
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null allocator", K(ret));
    } else if (OB_ISNULL(centroid_vec = static_cast<float *>(allocator->alloc(cent_vec_dim_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(cent_vec_dim_));
    } else {
      MEMCPY(
          centroid_vec, centroids_ + centroid_idx * cent_vec_dim_, cent_vec_dim_ * sizeof(float));
    }
  } else {
    centroid_vec = centroids_ + centroid_idx * cent_vec_dim_;
  }
  return ret;
}

int ObIvfCentCache::read_pq_centroid(int64_t m_idx, int64_t centroid_idx, float *&centroid_vec,
                                     bool deep_copy /* = false*/,
                                     ObIAllocator *allocator /* = nullptr*/)
{
  return inner_read_centroid(
      (m_idx - 1) * nlist_ + (centroid_idx - 1), centroid_vec, deep_copy, allocator);
}

int ObIvfCentCache::read_centroid(int64_t centroid_idx, float *&centroid_vec,
                                  bool deep_copy /* = false*/,
                                  ObIAllocator *allocator /* = nullptr*/)
{
  return inner_read_centroid(centroid_idx - 1, centroid_vec, deep_copy, allocator);
}

int64_t ObIvfCentCache::get_expect_memory_used(
    const IvfCacheKey &key,
    const ObVectorIndexParam &param)
{
  int64_t usage = 0;
  switch (key.type_) {
    case IvfCacheType::IVF_CENTROID_CACHE: {
      usage = sizeof(float) * param.nlist_ * param.dim_;
      break;
    }
    case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
      int64_t pqnlist = 1L << param.nbits_;
      usage = sizeof(float) * pqnlist * param.dim_;
      break;
    }
    case IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE: {
      int64_t ksub = 1L << param.nbits_;
      usage = sizeof(float) * param.nlist_ * param.m_ * ksub;
      break;
    }
    default: {
      break;
    }
  }
  return usage;
}

///////////////////////
// ObIvfAuxTableInfo //
///////////////////////
bool ObIvfAuxTableInfo::is_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_FLAT || type_ == VIAT_IVF_SQ8) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid();
    }
  } else if (type_ == VIAT_IVF_PQ) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && pq_centroid_table_id_ != OB_INVALID_ID
               && data_table_id_ != OB_INVALID_ID
               && centroid_tablet_ids_.count() == pq_centroid_tablet_ids_.count()
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid() && pq_centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

bool ObIvfAuxTableInfo::is_ivf_centroid_table_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_FLAT || type_ == VIAT_IVF_SQ8 || type_ == VIAT_IVF_PQ) {
    is_valid = centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < centroid_tablet_ids_.count(); ++i) {
      is_valid = centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

bool ObIvfAuxTableInfo::is_ivf_pq_centroid_table_valid() const
{
  bool is_valid = false;
  if (type_ == VIAT_IVF_PQ) {
    is_valid = pq_centroid_table_id_ != OB_INVALID_ID && data_table_id_ != OB_INVALID_ID
               && !pq_centroid_tablet_ids_.empty();
    for (int i = 0; is_valid && i < pq_centroid_tablet_ids_.count(); ++i) {
      is_valid = pq_centroid_tablet_ids_[i].is_valid();
    }
  }
  return is_valid;
}

void ObIvfAuxTableInfo::reset()
{
  centroid_table_id_ = OB_INVALID_ID;
  pq_centroid_table_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  centroid_tablet_ids_.reset();
  pq_centroid_tablet_ids_.reset();
  type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
}

int ObIvfAuxTableInfo::copy_ith_tablet(int64_t idx, ObIvfAuxTableInfo &dst) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(idx), K(count()));
  } else {
    dst.centroid_table_id_ = this->centroid_table_id_;
    dst.centroid_tablet_ids_.push_back(this->centroid_tablet_ids_[idx]);
    dst.data_table_id_ = this->data_table_id_;
    dst.type_ = this->type_;
    if (dst.type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
      // Try to copy pq_centroid_tablet_ids_ if available
      if (idx >= pq_centroid_tablet_ids_.count()) {
        // PQ centroid tablets not ready yet, but still allow centroid-only load
        LOG_WARN("pq centroid tablet not ready yet, will load centroid only", K(idx),
                 "pq_count", pq_centroid_tablet_ids_.count(),
                 "centroid_count", centroid_tablet_ids_.count());
      } else {
        dst.pq_centroid_table_id_ = this->pq_centroid_table_id_;
        dst.pq_centroid_tablet_ids_.push_back(this->pq_centroid_tablet_ids_[idx]);
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
