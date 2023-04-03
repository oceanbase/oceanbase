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

#include "ob_gti_source.h"
#include "ob_trans_service.h"
#include "ob_gti_rpc.h"
#include "ob_trans_factory.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace transaction
{

int ObGtiSource::init(const ObAddr &server, rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", KR(ret));
  } else if (OB_UNLIKELY(!server.is_valid() || OB_ISNULL(req_transport))) {
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(server), KP(req_transport));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(gti_request_rpc_proxy_ = ObGtiRpcProxyFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gti_reqeust_rpc_proxy fail", KR(ret));
  } else if (OB_ISNULL(gti_request_rpc_ = ObGtiRequestRpcFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc gti_reqeust_rpc fail", KR(ret));
  } else if (OB_FAIL(gti_request_rpc_proxy_->init(req_transport, server))) {
    TRANS_LOG(WARN, "rpc proxy init failed", KR(ret), KP(req_transport), K(server));
  } else if (OB_FAIL(gti_request_rpc_->init(gti_request_rpc_proxy_, server, this))) {
    TRANS_LOG(WARN, "response rpc init failed", KR(ret), K(server));
  } else {
    server_ = server;
    is_inited_ = true;
    TRANS_LOG(INFO, "gti source init success", K(server), KP(this));
  }
  if (OB_FAIL(ret)) {
    if (NULL != gti_request_rpc_proxy_) {
      ObGtiRpcProxyFactory::release(gti_request_rpc_proxy_);
      gti_request_rpc_proxy_ = NULL;
    }
    if (NULL != gti_request_rpc_) {
      ObGtiRequestRpcFactory::release(gti_request_rpc_);
      gti_request_rpc_ = NULL;
    }
  }

  return ret;
}

int ObGtiSource::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGtiSource is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObGtiSource is already running", KR(ret));
  } else if (OB_FAIL(gti_request_rpc_->start())) {
    TRANS_LOG(WARN, "gti request rpc start", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObGtiSource start success");
  }
  return ret;
}

void ObGtiSource::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGtiSource is not inited", KR(ret));
  } else if (OB_FAIL(gti_request_rpc_->stop())) {
    TRANS_LOG(WARN, "gti request rpc stop", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObGtiSource stop success");
  }
}

void ObGtiSource::wait()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObGtiSource is not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ObGtiSource is running", KR(ret));
  } else if (OB_FAIL(gti_request_rpc_->wait())) {
    TRANS_LOG(WARN, "gti request rpc wait", KR(ret));
  } else {
    TRANS_LOG(INFO, "ObGtiSource wait success");
  }
}

void ObGtiSource::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
  }
  if (NULL != gti_request_rpc_proxy_) {
    ObGtiRpcProxyFactory::release(gti_request_rpc_proxy_);
    gti_request_rpc_proxy_ = NULL;
  }
  if (NULL != gti_request_rpc_) {
    ObGtiRequestRpcFactory::release(gti_request_rpc_);
    gti_request_rpc_ = NULL;
  }
  TRANS_LOG(INFO, "ObGtiSource destroyed");
}

void ObGtiSource::reset()
{
  is_inited_ = false;
  is_running_ = false;
  is_requesting_ = false;
  for (int i=0; i<MAX_CACHE_NUM; i++) {
    id_cache_[i].start_id = 0;
    id_cache_[i].end_id = 0;
  }
  cur_idx_ = 0;
  cache_idx_ = 0;
  server_.reset();
  gti_request_rpc_ = NULL;
  gti_request_rpc_proxy_ = NULL;
  gti_cache_leader_.reset();
  retry_request_cnt_ = 0;
  last_request_ts_ = 0;
  preallocate_count_ = MIN_PREALLOCATE_COUNT;
  last_update_ts_ = ObTimeUtility::current_time();
}

int ObGtiSource::refresh_gti_location()
{
  int ret = OB_SUCCESS;
  gti_cache_leader_.reset();
  transaction::ObTransService *txs = MTL(transaction::ObTransService*);
  if (OB_FAIL(txs->get_location_adapter()->nonblock_renew(GCONF.cluster_id, MTL_ID(), GTI_LS))) {
    TRANS_LOG(WARN, "gti nonblock renew error", KR(ret), K(MTL_ID()), K(GTI_LS));
  } else {
    TRANS_LOG(INFO, "refresh gti location success", K(MTL_ID()));
  }
  return ret;
}

int ObGtiSource::update_trans_id(const int64_t start_id, const int64_t end_id)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::GTI_SOURCE_LOCK);
  const int64_t cache_idx = ATOMIC_LOAD(&cache_idx_);
  const int64_t cur_idx = ATOMIC_LOAD(&cur_idx_);
  if (cache_idx - cur_idx >= MAX_CACHE_NUM - 1) {
    TRANS_LOG(WARN, "drop trans id", K(MTL_ID()), K(start_id), K(end_id));
  } else {
    IdCache *id_cache = &(id_cache_[cache_idx % MAX_CACHE_NUM]);
    inc_update(&(id_cache->start_id), start_id);
    inc_update(&(id_cache->end_id), end_id);
    (void)ATOMIC_FAA(&cache_idx_, 1);
    retry_request_cnt_ = 0;
    TRANS_LOG(INFO, "update trans id", K(MTL_ID()), K(start_id), K(end_id));
  }

  return ret;
}

int ObGtiSource::get_trans_id(int64_t &trans_id)
{
  int ret = OB_SUCCESS;
  int save_ret = OB_SUCCESS;
  while (OB_SUCCESS == ret) {
    const int64_t cur_idx = ATOMIC_LOAD(&cur_idx_);
    IdCache *id_cache = &(id_cache_[cur_idx % MAX_CACHE_NUM]);
    const int64_t tmp_end_id = ATOMIC_LOAD(&(id_cache->end_id));
    const int64_t tmp_start_id = ATOMIC_LOAD(&(id_cache->start_id));
    int64_t tmp_trans_id = 0;
    if (tmp_start_id < tmp_end_id &&
         (tmp_trans_id = ATOMIC_FAA(&(id_cache->start_id), 1)) < tmp_end_id) {
      trans_id = tmp_trans_id;
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
    const int64_t retry_interval = min(retry_request_cnt_ * RETRY_REQUEST_INTERVAL, MAX_RETRY_REQUEST_INTERVAL);
    if (cur_ts - last_request_ts_ > retry_interval && ATOMIC_BCAS(&is_requesting_, false, true)) {
      ObGtiRequest req;
      if (OB_FAIL(req.init(MTL_ID(), get_preallocate_count_()))) {
        TRANS_LOG(WARN, "ObGtiRequest init fail", KR(ret), K(MTL_ID()));
      } else if (OB_FAIL(gti_request_rpc_->post(req))) {
        TRANS_LOG(WARN, "ObGtiRequest post fail", KR(ret), K(MTL_ID()));
      } else {
        last_request_ts_ = cur_ts;
        retry_request_cnt_++;
      }
      ATOMIC_STORE(&is_requesting_, false);
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

void ObGtiSource::update_preallocate_count_()
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

int64_t ObGtiSource::get_preallocate_count_()
{
  const int64_t tmp_preallocate_count = ATOMIC_LOAD(&preallocate_count_);
  int64_t new_preallocate_count = tmp_preallocate_count - MIN_PREALLOCATE_COUNT;
  if (new_preallocate_count < MIN_PREALLOCATE_COUNT) {
    new_preallocate_count = MIN_PREALLOCATE_COUNT;
  }
  (void)ATOMIC_BCAS(&preallocate_count_, tmp_preallocate_count, new_preallocate_count);
  return tmp_preallocate_count;
}

}
}
