/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "share/vector_index/ob_ivfflat_index_sample_cache.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{
/*
* ObMysqlResultIterator Impl
*/
int ObMysqlResultIterator::get_next_vector(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  ObObjMeta type;
  const ObNewRow *row = nullptr;
  if (OB_FAIL(result_.next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next result failed", K(ret));
    }
  } else if (OB_ISNULL(row = result_.get_row())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect nullptr row", K(ret));
  } else if (OB_FAIL(result_.get_type(row->count_ - 1, type))) {
    LOG_WARN("failed to get type", "vector_column_idx", row->count_ - 1);
  } else if (ObVectorType != type.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect index column type", K(ret), K(type));
  } else if (OB_FAIL(result_.get_vector(row->count_ - 1, vector))) {
    LOG_WARN("get obj failed", K(ret), "vector_column_idx", row->count_ - 1);
  }
  return ret;
}

/*
* ObIvfflatFixSampleCache Impl
*/
int ObIvfflatFixSampleCache::init(
    const int64_t tenant_id,
    const int64_t lists,
    ObSqlString &select_sql_string)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(select_sql_string));
  } else if (OB_FAIL(select_str_.assign(select_sql_string))) {
    LOG_WARN("failed to assign sql string", K(ret), K(select_sql_string));
  } else {
    tenant_id_ = tenant_id;
    allocator_.set_attr(ObMemAttr(tenant_id, "IvfflatCache"));
    samples_.set_attr(ObMemAttr(tenant_id, "IvfflatSamps"));
    limit_memory_size_ = (double)MAX_CACHE_MEMORY_RATIO / 100 * lib::get_tenant_memory_limit(tenant_id);
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      sample_cnt_ = tenant_config->vector_ivfflat_sample_count;
    }
    if (0 == sample_cnt_) { // only cache some samples
      if (OB_FAIL(init_cache())) {
        LOG_WARN("failed to init cache", K(ret));
      }
    } else {
      sample_cnt_ = MAX(lists * 50, sample_cnt_);
      if (OB_FAIL(init_reservoir_samples())) {
        LOG_WARN("failed to init reservoir_samples", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_INFO("success to init sample cache", K(ret), KPC(this));
    }
  }
  return ret;
}

int64_t ObIvfflatFixSampleCache::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(is_inited), K_(total_cnt), K_(sample_cnt), "cached_sample_count", samples_.count(), K_(cur_idx), K_(select_str), K_(limit_memory_size));
    J_OBJ_END();
  }
  return pos;
}

int ObIvfflatFixSampleCache::init_reservoir_samples()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_proxy_->read(result_, tenant_id_, select_str_.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K_(select_str));
  } else if (OB_ISNULL(res_ = result_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get mysql result failed", KR(ret), K_(select_str));
  } else {
    // 目前依赖特定的sql string
    ObMysqlResultIterator iter(*res_);
    ObTypeVector vector;
    ObTypeVector *new_vector = nullptr;
    int64_t random = 0;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_vector(vector))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next vector", K(ret));
        }
        break;
      } else if (sample_cnt_ > samples_.count()) { // just push back
        if (OB_FAIL(alloc_and_copy_vector(vector, new_vector))) {
          LOG_WARN("failed to alloc and copy vector", K(ret));
        } else if (OB_FAIL(samples_.push_back(new_vector))) {
          LOG_WARN("failed to push back array", K(ret), KPC(new_vector));
        }
      } else {
        random = ObRandom::rand(0, total_cnt_ - 1);
        if (random < samples_.count()) {
          if (OB_FAIL(samples_.at(random)->deep_copy(vector))) {
            LOG_WARN("failed to replace sample", K(ret));
          }
        }
      }
      ++total_cnt_;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to init reservoir samples", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObIvfflatFixSampleCache::init_cache()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(inner_conn_)
      && OB_FAIL(inner_conn_->execute_read(tenant_id_, select_str_.ptr(), result_))) {
    LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K_(select_str));
  } else if (OB_ISNULL(inner_conn_)
             && OB_FAIL(sql_proxy_->read(result_, tenant_id_, select_str_.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K_(select_str));
  } else if (OB_ISNULL(res_ = result_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get mysql result failed", KR(ret), K_(select_str));
  } else {
    // 目前依赖特定的sql string
    ObMysqlResultIterator iter(*res_);
    ObTypeVector vector;
    ObTypeVector *new_vector = nullptr;
    bool need_cache = true;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_vector(vector))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next vector", K(ret));
        }
        break;
      } else if (need_cache) {
        if (OB_FAIL(alloc_and_copy_vector(vector, new_vector))) {
          if (OB_ALLOCATE_MEMORY_FAILED == ret) {
            ret = OB_SUCCESS;
            need_cache = false;
          } else {
            LOG_WARN("failed to alloc and copy vector", K(ret));
          }
        } else if (OB_FAIL(samples_.push_back(new_vector))) {
          LOG_WARN("failed to push back array", K(ret), KPC(new_vector));
        }
      }
      ++total_cnt_;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (FAILEDx(select_str_.append_fmt(" limit %ld,%ld", samples_.count(), INT64_MAX-2))) {
      LOG_WARN("failed to append to sql string", K(ret), K_(select_str));
    } else {
      LOG_INFO("success to init cache", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObIvfflatFixSampleCache::alloc_and_copy_vector(const ObTypeVector& other, ObTypeVector *&vector)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  void *buf = nullptr;
  if (allocator_.total() >= limit_memory_size_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc", K(ret), K_(limit_memory_size), "total", allocator_.total());
  } else if (nullptr == (buf = allocator_.alloc(sizeof(ObTypeVector)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObTypeVector", K(ret));
  } else if (FALSE_IT(vector = new (buf) ObTypeVector())) {
  } else if (OB_FAIL(vector->deep_copy(other, allocator_))) {
    LOG_WARN("failed to deep copy vector", K(ret), K(vector));
  }
  if (OB_FAIL(ret)) {
    destory_vector(vector);
  }
  return ret;
}

void ObIvfflatFixSampleCache::destory_vector(ObTypeVector *&vector)
{
  if (OB_NOT_NULL(vector)) {
    vector->destroy(allocator_); // free ptr
    allocator_.free(vector);
    vector = nullptr;
  }
}

void ObIvfflatFixSampleCache::destroy()
{
  is_inited_ = false;
  allocator_.reset();
}

int ObIvfflatFixSampleCache::read()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfflatFixSampleCache is not inited", K(ret));
  } else if (sample_cnt_ > 0 || total_cnt_ == samples_.count()) {
    // do nothing
  } else if (OB_NOT_NULL(inner_conn_)
             && OB_FAIL(inner_conn_->execute_read(tenant_id_, select_str_.ptr(), result_))) {
    LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K_(select_str));
  } else if (OB_ISNULL(inner_conn_)
             && OB_FAIL(sql_proxy_->read(result_, tenant_id_, select_str_.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K_(tenant_id), K_(select_str));
  } else if (OB_ISNULL(res_ = result_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get mysql result failed", KR(ret), K_(select_str));
  }
  cur_idx_ = 0;
  return ret;
}

int ObIvfflatFixSampleCache::get_next_vector(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIvfflatFixSampleCache is not inited", K(ret));
  } else if (OB_FAIL(get_next_vector_by_cache(vector))) {
    if (OB_ITER_END == ret) {
      if (sample_cnt_ > 0) {
        // do nothing // only access cache
      } else if (cur_idx_ < total_cnt_ && OB_FAIL(get_next_vector_by_sql(vector))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next vector", K(ret));
        }
      }
    } else {
      LOG_WARN("failed to get next vector", K(ret));
    }
  }
  return ret;
}

int ObIvfflatFixSampleCache::get_random_vector(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  // TODO(@jingshui): random
  if (!samples_.empty()) {
    ObTypeVector *sample = samples_.at(0);
    if (OB_FAIL(vector.shallow_copy(*sample))) {
      LOG_WARN("failed to shallow copy vector", K(ret));
    }
  } else if (OB_FAIL(read())) {
    LOG_WARN("failed to read", K(ret));
  }
  return ret;
}

int ObIvfflatFixSampleCache::get_next_vector_by_sql(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  // 目前依赖特定的sql string
  ObMysqlResultIterator iter(*res_);
  if (OB_FAIL(iter.get_next_vector(vector))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next vector", K(ret));
    }
  }
  return ret;
}

int ObIvfflatFixSampleCache::get_next_vector_by_cache(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  if (cur_idx_ >= samples_.count()) {
    ret = OB_ITER_END;
  } else {
    ObTypeVector *sample = samples_.at(cur_idx_);
    if (OB_FAIL(vector.shallow_copy(*sample))) {
      LOG_WARN("failed to shallow copy vector", K(ret));
    } else {
      ++cur_idx_;
    }
  }
  return ret;
}

} // share
} // oceanbase