/**
 * Copyright (c) 2024 OceanBase
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

#include "share/vector_index/ob_index_sample_cache.h"

namespace oceanbase
{
namespace share
{

/*
* ObIndexSampleCache Impl
*/
int ObIndexSampleCache::init(
    const int64_t tenant_id,
    ObArray<ObTypeVector*> samples,
    lib::ObLabel samples_label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    tenant_id_ = tenant_id;
    samples_ = samples;
    samples_.set_attr(ObMemAttr(tenant_id, samples_label));
    total_cnt_ = samples_.count();
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_TRACE("success to init sample cache", K(ret), KPC(this));
    }
  }
  return ret;
}

int64_t ObIndexSampleCache::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(is_inited), K_(total_cnt), K_(sample_cnt), "cached_sample_count", samples_.count(), K_(cur_idx));
    for (int64_t i = 0; i < samples_.count(); ++i) {
      J_COMMA();
      BUF_PRINTO(samples_.at(i));
    }
    J_OBJ_END();
  }
  return pos;
}

void ObIndexSampleCache::destroy()
{
  is_inited_ = false;
}

int ObIndexSampleCache::read()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexSampleCache is not inited", K(ret));
  }
  cur_idx_ = 0;
  return ret;
}

int ObIndexSampleCache::get_next_vector(ObTypeVector &vector)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexSampleCache is not inited", K(ret));
  } else if (OB_FAIL(get_next_vector_by_cache(vector))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("failed to get next vector", K(ret));
    }
  }
  return ret;
}

int ObIndexSampleCache::get_random_vector(ObTypeVector &vector)
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

int ObIndexSampleCache::get_vector(const int64_t offset, ObTypeVector *&next_vector) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIndexSampleCache is not inited", K(ret));
  } else if (offset < 0 || offset >= samples_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    next_vector = samples_.at(offset);
  }
  return ret;
}

int ObIndexSampleCache::get_next_vector_by_cache(ObTypeVector &vector)
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
