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

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_define.h"
#include "share/ob_global_autoinc_service.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace share
{

int ObAutoIncCacheNode::init(const uint64_t sequence_value,
                             const uint64_t last_available_value,
                             const uint64_t sync_value,
                             const int64_t autoinc_version)
{
  int ret = OB_SUCCESS;
  if (sequence_value <= 0 || last_available_value < sequence_value || sync_value > sequence_value || autoinc_version < OB_INVALID_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence_value), K(last_available_value), K(sync_value));
  } else {
    sequence_value_ = sequence_value;
    last_available_value_ = last_available_value;
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
  if (OB_UNLIKELY(last_available_value_ == max_value)) {
    bret = false;
  } else if (OB_LIKELY(last_available_value_ >= desired_cnt)) {
    uint64_t new_base_value = std::max(base_value, sequence_value_);
    bret = new_base_value > last_available_value_ - desired_cnt + 1;
  } else {
    bret = true;
  }
  return bret;
}

int ObAutoIncCacheNode::update_sequence_value(const uint64_t sequence_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(sequence_value > last_available_value_ ||
                         sequence_value < sequence_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence_value),
                                 K(sequence_value_), K(last_available_value_));
  } else {
    sequence_value_ = sequence_value;
  }
  return ret;
}

int ObAutoIncCacheNode::update_available_value(const uint64_t available_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(available_value < last_available_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(available_value), K(last_available_value_));
  } else {
    last_available_value_ = available_value;
  }
  return ret;
}

int ObAutoIncCacheNode::update_sync_value(const uint64_t sync_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("update invalid cache is not allowed", K(ret));
  } else if (OB_UNLIKELY(sync_value > sequence_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sync_value), K(sequence_value_));
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
    LOG_WARN("gloabl service is not init", K(ret));
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
    int err = autoinc_map_.get_refactored(key, cache_node);
    const int64_t tenant_id = key.tenant_id_;
    const int64_t request_version = request.autoinc_version_;
    LOG_TRACE("begin handle req autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key));
    } else if (OB_UNLIKELY(!cache_node.is_valid()
                          || (request_version == cache_node.autoinc_version_
                            && cache_node.need_fetch_next_node(
                              request.base_value_, desired_count, request.max_value_)))) {
      OZ(fetch_next_node_(request, cache_node));
    } else if (OB_UNLIKELY(request_version > cache_node.autoinc_version_)) {
      LOG_INFO("start to reset old global table node", K(tenant_id), K(key.table_id_),
                                                       K(request_version), K(cache_node.autoinc_version_));
      cache_node.reset();
      OZ(fetch_next_node_(request, cache_node));
    } else if (OB_UNLIKELY(request_version < cache_node.autoinc_version_)) {
      ret = OB_AUTOINC_CACHE_NOT_EQUAL;
      LOG_WARN("request autoinc_version is less than autoinc_version_ in table_node, it should retry", KR(ret), K(tenant_id), K(key.table_id_),
                                                                                                       K(request_version), K(cache_node.autoinc_version_));
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!cache_node.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cache node", K(ret), K(cache_node));
      } else {
        const uint64_t start_inclusive = std::max(cache_node.sequence_value_, request.base_value_);
        const uint64_t max_value = request.max_value_;
        uint64_t end_inclusive = 0;
        if (max_value >= request.desired_cnt_ &&
             start_inclusive <= max_value - request.desired_cnt_ + 1) {
          end_inclusive = start_inclusive + request.desired_cnt_ - 1;
          if (OB_UNLIKELY(end_inclusive > cache_node.last_available_value_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(ret), K(end_inclusive), K(cache_node));
          } else if (OB_UNLIKELY(end_inclusive == cache_node.last_available_value_)) {
            // the cache node is run out
            cache_node.reset();
          } else if (OB_FAIL(cache_node.update_sequence_value(end_inclusive + 1))) {
            LOG_WARN("fail to update sequence value", K(ret), K(cache_node), K(end_inclusive));
          }
        } else if (OB_FAIL(cache_node.update_sequence_value(max_value))) {
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
          } else if (OB_FAIL(autoinc_map_.set_refactored(key, cache_node, 1))) {
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
    LOG_WARN("gloabl service is not init", K(ret));
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
    int err = autoinc_map_.get_refactored(key, cache_node);
    const int64_t request_version = request.autoinc_version_;
    LOG_TRACE("start handle get autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key));
    } else if (is_leader
              && OB_LIKELY(cache_node.is_valid())
              && request_version == cache_node.autoinc_version_) {
      // get autoinc values from cache
      sequence_value = cache_node.sequence_value_;
      sync_value = cache_node.sync_value_;
      // hash not exist, cache node is non-valid or service is not leader,
      // read value from inner table
    } else if (OB_FAIL(read_value_from_inner_table_(key, request_version, sequence_value, sync_value))) {
      LOG_WARN("fail to read value from inner table", KR(ret), K(tenant_id), K_(key.table_id));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.init(sequence_value, sync_value))) {
        LOG_WARN("failed to init result", KR(ret), K(tenant_id), K_(key.table_id), K(request_version), K(cache_node));
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
    LOG_WARN("gloabl service is not init", K(ret));
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
    int err = autoinc_map_.get_refactored(key, cache_node);
    const int64_t request_version = request.autoinc_version_;
    LOG_TRACE("start handle push global autoinc request", K(request), K(cache_node));
    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get seq value", K(ret), K(key));
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST == err
                        || (request_version == cache_node.autoinc_version_
                            && cache_node.need_sync(request.base_value_))
                        // cache node is expired
                        || (request_version > cache_node.autoinc_version_))) {
      if (request_version > cache_node.autoinc_version_) {
        cache_node.reset();
      }
      if (OB_FAIL(sync_value_to_inner_table_(request, cache_node, sync_value))) {
        LOG_WARN("sync to inner table failed", K(ret));
      } else if (OB_FAIL(autoinc_map_.set_refactored(key, cache_node, 1))) {
        LOG_WARN("set autoinc_map_ failed", K(ret));
      }
    // old request just ignore
    } else if (OB_UNLIKELY(request_version < cache_node.autoinc_version_)) {
      ret = OB_AUTOINC_CACHE_NOT_EQUAL;
      LOG_WARN("request autoinc_version is less than cache_node autoinc_version", KR(ret), K(tenant_id), K_(key.table_id),
                                                                                  K(request_version), K(cache_node.autoinc_version_));
    } else if (OB_LIKELY(request_version == cache_node.autoinc_version_)) {
      sync_value = cache_node.sync_value_;
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
    LOG_WARN("gloabl service is not init", K(ret));
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
    if (OB_FAIL(autoinc_map_.erase_refactored(key))) {
      LOG_WARN("fail to erase autoinc cache map key", K(ret));
    }
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    }
    mutex.unlock();
  }
  return ret;
}

int ObGlobalAutoIncService::check_leader_(const uint64_t tenant_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  is_leader = ATOMIC_LOAD(&is_leader_);
  if (OB_LIKELY(is_leader)) {
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
  } else if (OB_LIKELY(node.is_valid() && node.last_available_value_ == start_inclusive - 1)) {
    if (OB_FAIL(node.update_available_value(end_inclusive))) {
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
  uint64_t seq_value = node.is_valid() ? node.sequence_value_ : 0;
  const int64_t autoinc_version = request.autoinc_version_;
  if (OB_FAIL(inner_table_proxy_.sync_autoinc_value(request.autoinc_key_,
                                                    insert_value,
                                                    request.max_value_,
                                                    autoinc_version,
                                                    seq_value,
                                                    sync_value))) {
    LOG_WARN("fail to sync autoinc value to inner table", K(ret));
  } else if (OB_LIKELY(node.is_valid())) {
    if (seq_value > node.last_available_value_) {
      // the node is expired.
      node.reset();
      node.sync_value_ = sync_value; // update sync value for next sync
    } else if (sync_value == request.max_value_) {
      if (node.last_available_value_ != request.max_value_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(node), K(request.max_value_));
      } else if (OB_FAIL(node.init(request.max_value_, request.max_value_, request.max_value_, autoinc_version))) {
        LOG_WARN("fail to init node", K(ret), K(request.max_value_));
      }
    } else if (OB_FAIL(node.update_sequence_value(seq_value))) {
      LOG_WARN("fail to update sequence value", K(ret), K(seq_value));
    } else if (OB_FAIL(node.update_sync_value(sync_value))) {
      LOG_WARN("fail to update sync value", K(ret), K(sync_value));
    }
  } else {
    node.reset();
    node.sync_value_ = sync_value; // update sync value for next sync
  }
  if (OB_SUCC(ret)) {
    node.autoinc_version_ = autoinc_version > node.autoinc_version_ ? autoinc_version : node.autoinc_version_;
  }

  return ret;
}

} // share
} // oceanbase
