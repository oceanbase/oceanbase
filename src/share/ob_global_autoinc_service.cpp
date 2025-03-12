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

#include "share/ob_global_autoinc_service.h"
#include "src/share/sequence/ob_sequence_cache.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace share
{

OB_SERIALIZE_MEMBER(ObAutoIncCacheNode,  // FARM COMPAT WHITELIST
                    start_, end_, sync_value_, autoinc_version_);

int ObAutoIncCacheNode::init(const uint64_t start,
                             const uint64_t end,
                             const uint64_t sync_value,
                             const int64_t autoinc_version)
{
  int ret = OB_SUCCESS;
  if (start <= 0 || end < start || sync_value > start || autoinc_version < OB_INVALID_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(end), K(sync_value));
  } else {
    start_ = start;
    end_ = end;
    sync_value_ = sync_value;
    autoinc_version_ = autoinc_version;
  }
  return ret;
}

bool ObAutoIncCacheNode::need_fetch_next_node(const uint64_t base_value,
                                              const uint64_t desired_cnt,
                                              const uint64_t max_value) const
{
  bool bret = false;
  if (OB_UNLIKELY(end_ == max_value)) {
    bret = false;
  } else if (OB_LIKELY(end_ >= desired_cnt)) {
    uint64_t new_base_value = std::max(base_value, start_);
    bret = new_base_value > (end_ - desired_cnt + 1);
  } else {
    bret = true;
  }
  return bret;
}

int ObAutoIncCacheNode::with_new_start(const uint64_t new_start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(new_start > end_ || new_start < start_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_start), K_(start), K_(end));
  } else {
    start_ = new_start;
  }
  return ret;
}

int ObAutoIncCacheNode::with_new_end(const uint64_t new_end)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(new_end < end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_end), K_(end));
  } else {
    end_ = new_end;
  }
  return ret;
}

int ObAutoIncCacheNode::with_sync_value(const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(sync_value > start_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sync_value), K_(start));
  } else {
    sync_value_ = sync_value;
  }
  return ret;
}

int ObGlobalAutoIncService::init(const ObAddr &addr, ObMySQLProxy *mysql_proxy)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), ObModIds::OB_AUTOINCREMENT);
  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(inner_table_proxy_.init(mysql_proxy))) {
    LOG_WARN("init inner table proxy failed", K(ret));
  } else if (OB_FAIL(autoinc_map_.create(ObGlobalAutoIncService::INIT_HASHMAP_SIZE,
                                         attr,
                                         attr))) {
    LOG_WARN("init autoinc_map_ failed", K(ret));
  } else if (OB_ISNULL(gais_request_rpc_ =
      ObAutoincrementService::get_instance().get_gais_request_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gais request rpc is null", K(ret), KP(gais_request_rpc_));
  } else {
    for (int64_t i = 0; i < MUTEX_NUM; ++i) {
      op_mutex_[i].set_latch_id(common::ObLatchIds::AUTO_INCREMENT_GAIS_LOCK);
    }
    self_ = addr;
    is_inited_ = true;
  }
  return ret;
}

int ObGlobalAutoIncService::mtl_init(ObGlobalAutoIncService *&gais)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  const ObAddr &self = GCTX.self_addr();
  ret = gais->init(self, mysql_proxy);
  return ret;
}

void ObGlobalAutoIncService::destroy()
{
  autoinc_map_.destroy();
  inner_table_proxy_.reset();
  ObSpinLockGuard lock(cache_ls_lock_);
  cache_ls_ = NULL;
  is_leader_ = false;
  gais_request_rpc_ = NULL;
  is_switching_ = false;
  is_inited_ = false;
}

int ObGlobalAutoIncService::clear()
{
  int ret = OB_SUCCESS;
  if (autoinc_map_.size() > 0) {
    ret = autoinc_map_.clear();
  }
  return ret;
}

int ObGlobalAutoIncService::handle_next_autoinc_request(
    const ObGAISNextAutoIncValReq &request,
    obrpc::ObGAISNextValRpcResult &result)
{
  int ret = OB_SUCCESS;
  const AutoincKey &key = request.autoinc_key_;
  const uint64_t desired_count = request.desired_cnt_;
  bool is_leader = false;
  lib::ObMutex &mutex = op_mutex_[key.hash() % MUTEX_NUM];
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("global service is not init", K(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else if (OB_FAIL(check_leader_(key.tenant_id_, is_leader))) {
    LOG_WARN("check leader failed", K(ret), K(request.sender_), K(self_));
  } else if (OB_UNLIKELY(!is_leader)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("gais service is not leader", K(ret));
  } else if (OB_FAIL(mutex.lock())) {
    LOG_WARN("fail to get lock", K(ret));
  } else {
    ObAutoIncCacheNode cache_node;
    int err = autoinc_map_.get_refactored(key.table_id_, cache_node);
    const int64_t tenant_id = key.tenant_id_;
    const int64_t request_version = request.autoinc_version_;
    LOG_TRACE("begin handle req autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key));
    } else if (OB_UNLIKELY(cache_node.is_received())) {
      ret = read_and_push_inner_table(key, request.max_value_, cache_node);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!cache_node.is_valid()
                          || (request_version == cache_node.autoinc_version_
                            && cache_node.need_fetch_next_node(
                              request.base_value_, desired_count, request.max_value_)))) {
      OZ(fetch_next_node_(request, cache_node));
    } else if (OB_UNLIKELY(request_version > cache_node.autoinc_version_)) {
      LOG_INFO("start to reset old global table node", K(key), K(request_version),
                K(cache_node.autoinc_version_));
      cache_node.reset();
      OZ(fetch_next_node_(request, cache_node));
    } else if (OB_UNLIKELY(request_version < cache_node.autoinc_version_)) {
      ret = OB_AUTOINC_CACHE_NOT_EQUAL;
      LOG_WARN("request autoinc_version is less than autoinc_version_ in table_node,"
               "it should retry", KR(ret), K(tenant_id), K(key), K(request_version), K(cache_node));
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!cache_node.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cache node", K(ret), K(cache_node));
      } else {
        const uint64_t start_inclusive = std::max(cache_node.start_, request.base_value_);
        const uint64_t max_value = request.max_value_;
        uint64_t end_inclusive = 0;
        if (max_value >= request.desired_cnt_ &&
             start_inclusive <= max_value - request.desired_cnt_ + 1) {
          end_inclusive = start_inclusive + request.desired_cnt_ - 1;
          if (OB_UNLIKELY(end_inclusive > cache_node.end_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(ret), K(end_inclusive), K(cache_node));
          } else if (OB_UNLIKELY(end_inclusive == cache_node.end_)) {
            // the cache node is run out
            cache_node.reset();
          } else if (OB_FAIL(cache_node.with_new_start(end_inclusive + 1))) {
            LOG_WARN("fail to update sequence value", K(ret), K(cache_node), K(end_inclusive));
          }
        } else if (OB_FAIL(cache_node.with_new_start(max_value))) {
          LOG_WARN("fail to update sequence value", K(ret), K(cache_node), K(max_value));
        } else {
          end_inclusive = max_value;
        }
        if (OB_SUCC(ret)) {
          uint64_t sync_value = cache_node.sync_value_;
          if (request.base_value_ != 0 && request.base_value_ - 1 > sync_value) {
            sync_value = request.base_value_ - 1;
          }
          if (OB_FAIL(result.init(start_inclusive, end_inclusive, sync_value))) {
            LOG_WARN("init result failed", K(ret), K(cache_node));
          } else if (OB_FAIL(autoinc_map_.set_refactored(key.table_id_, cache_node, 1))) {
            LOG_WARN("set autoinc_map_ failed", K(ret));
          }
        }
        LOG_TRACE("after handle req autoinc request", K(request), K(cache_node));
      }
    }
    mutex.unlock();
  }
  return ret;
}

int ObGlobalAutoIncService::handle_curr_autoinc_request(const ObGAISAutoIncKeyArg &request,
                                                        obrpc::ObGAISCurrValRpcResult &result)
{
  int ret = OB_SUCCESS;
  const AutoincKey &key = request.autoinc_key_;
  uint64_t sequence_value = 0;
  uint64_t sync_value = 0;
  bool is_leader = false;
  lib::ObMutex &mutex = op_mutex_[key.hash() % MUTEX_NUM];
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("global service is not init", K(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else if (OB_FAIL(check_leader_(key.tenant_id_, is_leader))) {
    LOG_WARN("check leader failed", K(ret), K(request.sender_), K(self_));
  } else if (OB_FAIL(mutex.lock())) {
    LOG_WARN("fail to get lock", K(ret));
  } else {
    ObAutoIncCacheNode cache_node;
    const int64_t tenant_id = key.tenant_id_;
    int err = autoinc_map_.get_refactored(key.table_id_, cache_node);
    const int64_t request_version = request.autoinc_version_;
    LOG_TRACE("start handle get autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key));
    } else if (is_leader
              && OB_LIKELY(cache_node.is_valid() && !cache_node.is_received())
              && request_version == cache_node.autoinc_version_) {
      // get autoinc values from cache
      sequence_value = cache_node.start_;
      sync_value = cache_node.sync_value_;
      // hash not exist, cache node is non-valid or service is not leader,
      // read value from inner table
    } else if (OB_FAIL(read_value_from_inner_table_(key, request_version, sequence_value,
                                                    sync_value))) {
      LOG_WARN("fail to read value from inner table", KR(ret), K(tenant_id), K_(key.table_id));
    } else if (OB_UNLIKELY(cache_node.is_received()) && (sequence_value - 1 == cache_node.end_)) {
      sequence_value = cache_node.start_;
      sync_value = cache_node.sync_value_;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.init(sequence_value, sync_value))) {
        LOG_WARN("failed to init result", KR(ret), K(tenant_id), K_(key.table_id),
                  K(request_version), K(cache_node));
      }
    }
    mutex.unlock();
  }
  return ret;
}

int ObGlobalAutoIncService::handle_push_autoinc_request(
    const ObGAISPushAutoIncValReq &request,
    uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  const AutoincKey &key = request.autoinc_key_;
  bool is_leader = false;
  lib::ObMutex &mutex = op_mutex_[key.hash() % MUTEX_NUM];
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("global service is not init", K(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else if (OB_FAIL(check_leader_(key.tenant_id_, is_leader))) {
    LOG_WARN("check leader failed", K(ret), K(request.sender_), K(self_));
  } else if (OB_UNLIKELY(!is_leader)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("gais service is not leader", K(ret));
  } else if (OB_FAIL(mutex.lock())) {
    LOG_WARN("fail to get lock", K(ret));
  } else {
    ObAutoIncCacheNode cache_node;
    const int64_t tenant_id = key.tenant_id_;
    int err = autoinc_map_.get_refactored(key.table_id_, cache_node);
    const int64_t request_version = request.autoinc_version_;
    const uint64_t insert_value = request.base_value_;
    LOG_TRACE("start handle push global autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key), K(err));
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST == err
                        || (request_version == cache_node.autoinc_version_
                            && cache_node.need_sync(insert_value))
                        // cache node is expired
                        || (request_version > cache_node.autoinc_version_))) {
      cache_node.reset();
      if (OB_FAIL(sync_value_to_inner_table_(request, cache_node, sync_value))) {
        LOG_WARN("sync to inner table failed", K(ret));
      } else if (OB_FAIL(autoinc_map_.set_refactored(key.table_id_, cache_node, 1))) {
        LOG_WARN("set autoinc_map_ failed", K(ret));
      }
    // old request just ignore
    } else if (OB_UNLIKELY(request_version < cache_node.autoinc_version_)) {
      ret = OB_AUTOINC_CACHE_NOT_EQUAL;
      LOG_WARN("request autoinc_version is less than cache_node autoinc_version", KR(ret),
               K(key), K(request_version), K(cache_node.autoinc_version_));
    } else if (OB_LIKELY(request_version == cache_node.autoinc_version_)) {
      if (insert_value < cache_node.start_ && insert_value < cache_node.sync_value_) {
        // insert value is too small and no need to update node
      } else {
        sync_value = MAX(MAX(insert_value, cache_node.sync_value_), cache_node.start_ - 1);
        if (OB_UNLIKELY(cache_node.is_received()) &&
              OB_FAIL(read_and_push_inner_table(key, request.max_value_, cache_node))) {
          LOG_WARN("fail to read and push inner table", K(ret), K(key), K(cache_node));
        } else if (cache_node.is_valid()) {
          cache_node.start_ = sync_value + 1;
          cache_node.sync_value_ = sync_value;
        }
        if (OB_SUCC(ret) && OB_FAIL(autoinc_map_.set_refactored(key.table_id_, cache_node, 1))) {
          LOG_WARN("set autoinc_map_ failed", K(ret));
        }
      }
    }
    mutex.unlock();
  }
  return ret;
}

int ObGlobalAutoIncService::handle_clear_autoinc_cache_request(const ObGAISAutoIncKeyArg &request)
{
  int ret = OB_SUCCESS;
  const AutoincKey &key = request.autoinc_key_;
  bool is_leader = false;
  lib::ObMutex &mutex = op_mutex_[key.hash() % MUTEX_NUM];
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("global service is not init", K(ret));
  } else if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else if (OB_FAIL(check_leader_(key.tenant_id_, is_leader))) {
    LOG_WARN("check leader failed", K(ret), K(request.sender_), K(self_));
  } else if (OB_UNLIKELY(!is_leader)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("gais service is not leader", K(ret));
  } else if (OB_FAIL(mutex.lock())) {
    LOG_WARN("fail to get lock", K(ret));
  } else {
    LOG_TRACE("start clear autoinc cache request", K(request));
    if (OB_FAIL(autoinc_map_.erase_refactored(key.table_id_))) {
      LOG_WARN("fail to erase autoinc cache map key", K(ret));
    }
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    }
    mutex.unlock();
  }
  return ret;
}

int ObGlobalAutoIncService::handle_next_sequence_request(
  const ObGAISNextSequenceValReq &request,
  obrpc::ObGAISNextSequenceValRpcResult &result)
{
  int ret = OB_SUCCESS;
  ObSequenceCache *sequence_cache = &share::ObSequenceCache::get_instance();
  ObArenaAllocator allocator;
  return sequence_cache->nextval(request.schema_, allocator ,result.nextval_);
}

int ObGlobalAutoIncService::check_leader_(const uint64_t tenant_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  is_leader = ATOMIC_LOAD(&is_leader_);
  if (OB_LIKELY(is_leader)) {
  } else if (ATOMIC_LOAD(&is_switching_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("service is switching to leader", K(ret), KP(this), K(*this));
  } else {
    // try to get role from logstream
    ObRole role = ObRole::INVALID_ROLE;
    int64_t proposal_id = 0;
    ObSpinLockGuard lock(cache_ls_lock_);
    if (OB_ISNULL(cache_ls_)) {
      ret = OB_NOT_MASTER;
      LOG_WARN("cache ls is null", K(ret));
    } else if (OB_FAIL(cache_ls_->get_log_handler()->get_role(role, proposal_id))) {
      int tmp_ret = ret;
      ret = OB_NOT_MASTER;
      LOG_WARN("get ls role fail", K(ret), K(tmp_ret));
    } else if (common::ObRole::LEADER == role) {
      is_leader = true;
    } else {
      is_leader = false;
    }
  }

  return ret;
}

int ObGlobalAutoIncService::fetch_next_node_(const ObGAISNextAutoIncValReq &request,
                                             ObAutoIncCacheNode &node)
{
  int ret = OB_SUCCESS;
  uint64_t desired_count = std::max(request.cache_size_, request.desired_cnt_);
  uint64_t start_inclusive = 0;
  uint64_t end_inclusive = 0;
  uint64_t sync_value = 0;
  const int64_t autoinc_version =  request.autoinc_version_;
  if (OB_FAIL(inner_table_proxy_.next_autoinc_value(request.autoinc_key_,
                                                    request.offset_,
                                                    request.increment_,
                                                    request.base_value_,
                                                    request.max_value_,
                                                    desired_count,
                                                    autoinc_version,
                                                    start_inclusive,
                                                    end_inclusive,
                                                    sync_value))) {
    LOG_WARN("fail to require autoinc value from inner table", K(ret));
  } else if (OB_LIKELY(node.is_valid() && (node.end_ == start_inclusive - request.increment_))) {
    if (OB_FAIL(node.with_new_end(end_inclusive))) {
      LOG_WARN("fail to update available value", K(ret), K(node), K(end_inclusive));
    } else {
      LOG_TRACE("fetch next node done", K(request), K(node));
    }
  } else if (OB_FAIL(node.init(start_inclusive, end_inclusive, sync_value, autoinc_version))){
    LOG_WARN("fail to init node", K(ret), K(start_inclusive), K(end_inclusive), K(sync_value));
  } else {
    LOG_TRACE("fetch next node done", K(request), K(node));
  }
  return ret;
}

int ObGlobalAutoIncService::read_value_from_inner_table_(const share::AutoincKey &key,
                                                         const int64_t &autoinc_version,
                                                         uint64_t &sequence_val,
                                                         uint64_t &sync_val)
{
  return inner_table_proxy_.get_autoinc_value(key, autoinc_version, sequence_val, sync_val);
}

int ObGlobalAutoIncService::sync_value_to_inner_table_(
    const ObGAISPushAutoIncValReq &request,
    ObAutoIncCacheNode &node,
    uint64_t &sync_value)
{
  int ret = OB_SUCCESS;
  const uint64_t insert_value = request.base_value_;
  const int64_t autoinc_version = request.autoinc_version_;
  const uint64_t next_cache_boundary =
    calc_next_cache_boundary(insert_value, request.cache_size_, request.max_value_);
  uint64_t seq_value = insert_value;
  if (OB_FAIL(inner_table_proxy_.sync_autoinc_value(request.autoinc_key_,
                                                    next_cache_boundary,
                                                    request.max_value_,
                                                    autoinc_version,
                                                    seq_value,
                                                    sync_value))) {
    LOG_WARN("fail to sync autoinc value to inner table", K(ret));
  } else if (insert_value == request.max_value_) {
    if (OB_FAIL(node.init(request.max_value_, request.max_value_,
                          request.max_value_, autoinc_version))) {
      LOG_WARN("fail to init node", K(ret), K(request.max_value_));
    }
  } else {
    // updates directly without checking, this node may be invalid.
    node.start_ = seq_value;
    node.end_ = sync_value;
    node.sync_value_ = seq_value - 1;
    node.autoinc_version_ = autoinc_version;
  }
  return ret;
}

int ObGlobalAutoIncService::inner_switch_to_follower()
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::current_time();
  ObMutexGuard lock(switching_mutex_);
  LOG_INFO("start to switch to follower", KP(this), K(*this));
  ATOMIC_STORE(&is_switching_, true);
  ATOMIC_STORE(&is_leader_, false);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("global service is not init", K(ret));
  } else if (OB_FAIL(broadcast_global_autoinc_cache())) {
    LOG_WARN("fail to broadcast global autoinc cache", K(ret));
  }
  if (OB_SUCC(ret)) {
    // If the broadcast is successful, all cache nodes will be updated to is_received,
    // otherwise the map will be cleared directly.
  } else {
    // ob failed
    int tmp_ret = clear();
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("fail to clear auto inc map", K(ret), K(tmp_ret), K(autoinc_map_.size()));
    }
  }
  ATOMIC_STORE(&is_switching_, false);
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  LOG_INFO("global_autoinc service: switch_to_follower", K(*this), K(cost_us));
  return ret;
}

int ObGlobalAutoIncService::broadcast_global_autoinc_cache()
{
  int ret = OB_SUCCESS;
  if (autoinc_map_.size() > 0) {
    if (OB_FAIL(wait_all_requests_to_finish())) {
      LOG_WARN("fail to wait all requests to finish", K(ret));
    } else if (autoinc_map_.size() > 0) {
      const int64_t size = serialize_size_autoinc_cache();
      const uint64_t tenant_id = MTL_ID();
      ObMemAttr attr(tenant_id, ObModIds::OB_AUTOINCREMENT);
      char *buffer = NULL;
      int64_t pos = 0;
      ObGAISBroadcastAutoIncCacheReq msg;
      if (OB_ISNULL(buffer = static_cast<char*>(ob_malloc(size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc cache buffer", K(ret), K(size));
      } else if (OB_FAIL(serialize_autoinc_cache(buffer, size, pos))) {
        LOG_WARN("fail to serialize global autoinc cache", K(ret));
      } else if (OB_FAIL(msg.init(tenant_id, buffer, pos))) {
        LOG_WARN("fail to init msg", K(ret), K(tenant_id), K(buffer), K(pos));
      } else if (OB_UNLIKELY(!msg.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(msg));
      } else if (OB_ISNULL(gais_request_rpc_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("global service request rpc is not init", K(ret));
      } else if (OB_FAIL(gais_request_rpc_->broadcast_global_autoinc_cache(msg))) {
        LOG_WARN("broadcast gais request failed", K(ret), K(msg));
      } else {
        LOG_INFO("succ to broadcast global autoinc cache", K(msg));
      }
      if (NULL != buffer) {
        ob_free(buffer);
        buffer = NULL;
      }
    }
  }
  return ret;
}

int ObGlobalAutoIncService::receive_global_autoinc_cache(
    const ObGAISBroadcastAutoIncCacheReq &request)
{
  int ret = OB_SUCCESS;
  ObMutexGuard lock(switching_mutex_);
  ATOMIC_STORE(&is_switching_, true);
  ATOMIC_STORE(&is_leader_, false);
  int64_t pos = 0;
  if (OB_UNLIKELY(!request.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request));
  } else if (OB_UNLIKELY(request.tenant_id_ != MTL_ID())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(request), K(MTL_ID()));
  } else if (OB_FAIL(wait_all_requests_to_finish())) {
    LOG_WARN("fail to wait all requests to finish", K(ret));
  } else if (OB_FAIL(deserialize_autoinc_cache(request.buf_, request.buf_size_, pos))) {
    LOG_WARN("fail to deserialize global auto inc cache", K(ret), K(request));
  }
  ATOMIC_STORE(&is_switching_, false);
  return ret;
}

int64_t ObGlobalAutoIncService::serialize_size_autoinc_cache()
{
  int64_t len = 0;
  common::hash::ObHashMap<uint64_t, ObAutoIncCacheNode>::iterator iter = autoinc_map_.begin();
  const uint64_t count = autoinc_map_.size();
  OB_UNIS_ADD_LEN(count);
  for (; iter != autoinc_map_.end(); ++iter) {
    const uint64_t &table_id = iter->first;
    const ObAutoIncCacheNode &cache_node = iter->second;
    OB_UNIS_ADD_LEN(table_id);
    OB_UNIS_ADD_LEN(cache_node);
  }
  return len;
}

int ObGlobalAutoIncService::serialize_autoinc_cache(SERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<uint64_t, ObAutoIncCacheNode>::iterator iter = autoinc_map_.begin();
  const uint64_t count = autoinc_map_.size();
  OB_UNIS_ENCODE(count);
  for (; OB_SUCC(ret) && iter != autoinc_map_.end(); ++iter) {
    const uint64_t &table_id = iter->first;
    ObAutoIncCacheNode &cache_node = iter->second;
    OB_UNIS_ENCODE(table_id);
    OB_UNIS_ENCODE(cache_node);
    cache_node.is_received_ = true; // mark local cache as received
  }
  return ret;
}

int ObGlobalAutoIncService::deserialize_autoinc_cache(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  uint64_t table_id = 0;
  ObAutoIncCacheNode cache_node;
  ret = autoinc_map_.clear();
  OB_UNIS_DECODE(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    cache_node.reset();
    OB_UNIS_DECODE(table_id);
    OB_UNIS_DECODE(cache_node);
    cache_node.is_received_ = true;
    if (OB_SUCC(ret) && OB_FAIL(autoinc_map_.set_refactored(table_id, cache_node, 1))) {
      LOG_WARN("fail to set map", K(ret), K(table_id), K(cache_node));
    }
  }
  return ret;
}

int ObGlobalAutoIncService::wait_all_requests_to_finish()
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + BROADCAST_OP_TIMEOUT;
  for (int64_t i = 0; OB_SUCC(ret) && i < MUTEX_NUM; i++) {
    // wait for all working threads to finish
    if (OB_FAIL(op_mutex_[i].lock(abs_timeout_us))) {
      LOG_WARN("fail to lock mutex", K(ret), K(i));
    } else {
      op_mutex_[i].unlock();
    }
  }
  return ret;
}

int ObGlobalAutoIncService::read_and_push_inner_table(const AutoincKey &key,
                                                      const uint64_t max_value,
                                                      ObAutoIncCacheNode &received_node)
{
  int ret = OB_SUCCESS;
  if (received_node.is_valid()) {
    bool is_valid = false;
    uint64_t new_end = received_node.end_;
    if (OB_FAIL(inner_table_proxy_.read_and_push_inner_table(key,
                                                             max_value,
                                                             received_node.end_,
                                                             received_node.autoinc_version_,
                                                             is_valid,
                                                             new_end))) {
      LOG_WARN("fail to read and push inner table", K(ret), K(key), K(received_node));
    } else if (!is_valid) {
      received_node.reset();
    } else if (OB_FAIL(received_node.with_new_end(new_end))) {
      LOG_WARN("fail to update node end", K(ret), K(new_end), K(received_node));
    } else {
      received_node.is_received_ = false;
    }
  }
  return ret;
}

} // share
} // oceanbase
