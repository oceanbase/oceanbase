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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_id_rpc.h"
#include "ob_das_id_cache.h"
#include "ob_das_id_service.h"
#include "share/location_cache/ob_location_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql
{
int ObDASIDCache::init(const common::ObAddr &server, rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;
  void *proxy_buf = nullptr;
  void *request_buf = nullptr;
  alloc_.set_attr(ObMemAttr(MTL_ID(), "DASIDCache"));
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || OB_ISNULL(req_transport))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), KP(req_transport));
  } else if (OB_ISNULL(proxy_buf = alloc_.alloc(sizeof(obrpc::ObDASIDRpcProxy)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc rpc proxy failed", KR(ret));
  } else if (OB_ISNULL(request_buf = alloc_.alloc(sizeof(ObDASIDRequestRpc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc request rpc failed", KR(ret));
  }
  if (OB_SUCC(ret)) {
    id_rpc_proxy_ = new(proxy_buf) obrpc::ObDASIDRpcProxy();
    id_request_rpc_ = new(request_buf) ObDASIDRequestRpc();
    if (OB_FAIL(id_rpc_proxy_->init(req_transport, server))) {
      LOG_WARN("init rpc proxy failed", KR(ret), K(server));
    } else if (OB_FAIL(id_request_rpc_->init(id_rpc_proxy_, server, this))) {
      LOG_WARN("init response rpc failed", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != id_rpc_proxy_) {
      alloc_.free(id_rpc_proxy_);
      id_rpc_proxy_ = NULL;
    }
    if (NULL != id_request_rpc_) {
      alloc_.free(id_request_rpc_);
      id_request_rpc_ = NULL;
    }
  } else {
    server_ = server;
    is_inited_ = true;
  }
  return ret;
}

void ObDASIDCache::destroy()
{
  if (NULL != id_rpc_proxy_) {
    alloc_.free(id_rpc_proxy_);
    id_rpc_proxy_ = NULL;
  }
  if (NULL != id_request_rpc_) {
    alloc_.free(id_request_rpc_);
    id_request_rpc_ = NULL;
  }
  alloc_.reset();
  is_inited_ = false;
}

void ObDASIDCache::reset()
{
  is_inited_ = false;
  is_requesting_ = false;
  for (int i=0; i<MAX_CACHE_NUM; i++) {
    id_cache_[i].start_id = 0;
    id_cache_[i].end_id = 0;
  }
  cur_idx_ = 0;
  cache_idx_ = 0;
  server_.reset();
  id_rpc_proxy_ = NULL;
  id_request_rpc_ = NULL;
  id_service_leader_.reset();
  retry_request_cnt_ = 0;
  preallocate_count_ = MIN_PREALLOCATE_COUNT;
  last_update_ts_ = ObTimeUtility::current_time();
  alloc_.reset();
}

int ObDASIDCache::refresh_id_service_location()
{
  int ret = OB_SUCCESS;
  id_service_leader_.reset();
  if (OB_FAIL(GCTX.location_service_->nonblock_renew(GCONF.cluster_id, MTL_ID(), DAS_ID_LS))) {
    LOG_WARN("das id cache nonblock renew failed", KR(ret));
  }
  return ret;
}

int ObDASIDCache::update_das_id(const int64_t start_id, const int64_t end_id)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  const int64_t cache_idx = ATOMIC_LOAD(&cache_idx_);
  const int64_t cur_idx = ATOMIC_LOAD(&cur_idx_);
  if (cache_idx - cur_idx >= MAX_CACHE_NUM - 1) {
    LOG_TRACE("drop das id", K(MTL_ID()), K(start_id), K(end_id));
  } else {
    IdCache *id_cache = &(id_cache_[cache_idx % MAX_CACHE_NUM]);
    inc_update(&(id_cache->start_id), start_id);
    inc_update(&(id_cache->end_id), end_id);
    (void)ATOMIC_FAA(&cache_idx_, 1);
    retry_request_cnt_ = 0;
  }
  return ret;
}

int ObDASIDCache::get_das_id(int64_t &das_id, const bool force_renew)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  while (OB_SUCCESS == ret) {
    const int64_t cur_idx = ATOMIC_LOAD(&cur_idx_);
    IdCache *id_cache = &(id_cache_[cur_idx % MAX_CACHE_NUM]);
    const int64_t tmp_end_id = ATOMIC_LOAD(&(id_cache->end_id));
    const int64_t tmp_start_id = ATOMIC_LOAD(&(id_cache->start_id));
    int64_t tmp_das_id = 0;
    if (tmp_start_id < tmp_end_id &&
         (tmp_das_id = ATOMIC_FAA(&(id_cache->start_id), 1)) < tmp_end_id) {
      das_id = tmp_das_id;
      break;
    } else {
      if (OB_UNLIKELY(cur_idx >= ATOMIC_LOAD(&cache_idx_))) {
        ret = OB_EAGAIN;
      } else {
        (void)ATOMIC_CAS(&cur_idx_, cur_idx, cur_idx + 1);
      }
    }
  }
  save_ret = ret;
  const int64_t left_cache_count = cache_idx_ - cur_idx_;
  if (left_cache_count < PRE_CACHE_NUM && !ATOMIC_LOAD(&is_requesting_)) {
    const int64_t cur_ts = ObTimeUtility::current_time();
    const int64_t retry_timeout = min(OB_DAS_ID_RPC_TIMEOUT_MIN * max(retry_request_cnt_, 1),
                                      OB_DAS_ID_RPC_TIMEOUT_MAX);
    if (ATOMIC_BCAS(&is_requesting_, false, true)) {
      ObDASIDRequest req;
      obrpc::ObDASIDRpcResult res;
      retry_request_cnt_++;
      if (OB_FAIL(req.init(MTL_ID(), get_preallocate_count_()))) {
        LOG_WARN("ObDASIDRequest init fail", KR(ret), K(MTL_ID()));
      } else if (OB_FAIL(id_request_rpc_->fetch_new_range(req, res, retry_timeout, force_renew))) {
        LOG_WARN("fetch new range failed", KR(ret));
      } else if (OB_UNLIKELY(!res.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("das id rpc result is invalid", KR(ret), K(res));
      } else if (OB_SUCCESS != res.get_status()) {
        if (OB_NOT_MASTER == res.get_status() || OB_TENANT_NOT_IN_SERVER == res.get_status()) {
          ret = OB_EAGAIN;
          LOG_INFO("das id rpc need retry", KR(res.get_status()), KR(ret));
        } else {
          ret = res.get_status();
          LOG_WARN("das id rpc failed", KR(ret), K(res));
        }
      } else if (OB_FAIL(update_das_id(res.get_start_id(), res.get_end_id()))) {
        LOG_WARN("update das id failed", KR(ret));
      }
      ATOMIC_STORE(&is_requesting_, false);
      if (OB_SUCC(ret)) {
        LOG_TRACE("das id rpc succeeded", K(req), K(res));
      }
    }
    if (left_cache_count <= PRE_CACHE_NUM / 2) {
      if (cache_idx_ < PRE_CACHE_NUM) {
        // do not update preallocate count at boot time
      } else {
        update_preallocate_count_();
      }
    }
  }
  return save_ret;
}

void ObDASIDCache::update_preallocate_count_()
{
  const int64_t cur_ts = ObTimeUtility::current_time();
  if (cur_ts - last_update_ts_ > UPDATE_PREALLOCATE_COUNT_INTERVAL) {
    const int64_t tmp_preallocate_count = ATOMIC_LOAD(&preallocate_count_);
    int64_t new_preallocate_count = tmp_preallocate_count * UPDATE_FACTOR;
    if (new_preallocate_count > MAX_PREALLOCATE_COUNT) {
      new_preallocate_count = MAX_PREALLOCATE_COUNT;
    }
    if (ATOMIC_BCAS(&preallocate_count_, tmp_preallocate_count, new_preallocate_count)) {
      last_update_ts_ = cur_ts;
    }
  }
}

int64_t ObDASIDCache::get_preallocate_count_()
{
  const int64_t tmp_preallocate_count = ATOMIC_LOAD(&preallocate_count_);
  int64_t new_preallocate_count = tmp_preallocate_count - MIN_PREALLOCATE_COUNT;
  if (new_preallocate_count < MIN_PREALLOCATE_COUNT) {
    new_preallocate_count = MIN_PREALLOCATE_COUNT;
  }
  (void)ATOMIC_BCAS(&preallocate_count_, tmp_preallocate_count, new_preallocate_count);
  return tmp_preallocate_count;
}
} // namespace sql
} // namespace oceanbase
