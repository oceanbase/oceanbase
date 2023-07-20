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

#include "observer/net/ob_ingress_bw_alloc_service.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#define USING_LOG_PREFIX RS

namespace oceanbase
{
using namespace common;

namespace rootserver
{
int ObNetEndpointIngressManager::init()
{
  int ret = OB_SUCCESS;
  if (ingress_plan_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("endpoint ingress manager should not init multiple times", K(ret));
  } else if (OB_FAIL(ingress_plan_map_.create(7, "INGRESS_MAP"))) {
    LOG_WARN("fail create ingress_plan_map", K(ret));
  } else {
    total_bw_limit_ = 0;
    LOG_INFO("endpoint ingress manager init ok");
  }
  return ret;
}

void ObNetEndpointIngressManager::destroy()
{
  ObIngressPlanMap::iterator iter;
  ObSpinLockGuard guard(lock_);
  for (iter = ingress_plan_map_.begin(); iter != ingress_plan_map_.end(); ++iter) {
    if (OB_NOT_NULL(iter->second)) {
      ob_free(iter->second);
    }
  }
  ingress_plan_map_.destroy();
}
int ObNetEndpointIngressManager::register_endpoint(const ObNetEndpointKey &endpoint_key, const int64_t expire_time)
{
  int ret = OB_SUCCESS;
  ObNetEndpointValue *endpoint_value = nullptr;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!endpoint_key.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid config", K(ret), K(endpoint_key));
  } else if (OB_FAIL(ingress_plan_map_.get_refactored(endpoint_key, endpoint_value))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // initialize
      endpoint_value = (ObNetEndpointValue *)ob_malloc(sizeof(ObNetEndpointValue), "INGRESS_SERVICE");
      if (OB_ISNULL(endpoint_value)) {
        LOG_WARN("failed to alloc memory for objs");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        endpoint_value->expire_time_ = expire_time;
      }
      if (OB_FAIL(ingress_plan_map_.set_refactored(endpoint_key, endpoint_value))) {
        ob_free(endpoint_value);
        LOG_WARN("endpoint register failed", K(ret), K(endpoint_key), K(expire_time));
      } else {
        LOG_INFO("endpoint register first time success", K(endpoint_key), K(expire_time), KP(endpoint_value));
      }
    } else {
      LOG_WARN("endpoint register failed", K(ret), K(endpoint_key));
    }
  } else {
    endpoint_value->expire_time_ = expire_time;
  }
  return ret;
}

int ObNetEndpointIngressManager::collect_predict_bw(ObNetEndpointKVArray &update_kvs)
{
  int ret = OB_SUCCESS;
  ObNetEndpointPredictIngressProxy proxy_batch(
      *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::net_endpoint_predict_ingress);
  const int64_t timeout_ts = GCONF.rpc_timeout;
  common::ObSEArray<ObNetEndpointKey, 16> delete_keys;
  const int64_t current_time = ObTimeUtility::current_time();
  {
    ObSpinLockGuard guard(lock_);
    for (ObIngressPlanMap::iterator iter = ingress_plan_map_.begin(); iter != ingress_plan_map_.end(); ++iter) {
      const ObNetEndpointKey &endpoint_key = iter->first;
      ObNetEndpointValue *endpoint_value = iter->second;
      if (endpoint_value->expire_time_ < current_time) {
        LOG_INFO("endpoint expired", K(endpoint_key), K(endpoint_value->expire_time_), K(current_time));
        if (OB_FAIL(delete_keys.push_back(endpoint_key))) {
          LOG_WARN("fail to push back arrays", K(ret), K(endpoint_key));
        } else {
          ob_free(endpoint_value);
        }
      } else {
        if (OB_FAIL(update_kvs.push_back(ObNetEndpointKeyValue(endpoint_key, endpoint_value)))) {
          LOG_WARN("fail to push back arrays", K(ret), K(endpoint_key));
        } else {
          endpoint_value->predicted_bw_ = -1;
        }
      }
    }

    for (int64_t i = 0; i < delete_keys.count(); i++) {
      const ObNetEndpointKey &endpoint_key = delete_keys[i];
      if (OB_FAIL(ingress_plan_map_.erase_refactored(endpoint_key))) {
        LOG_ERROR("failed to erase endpoint", K(ret), K(endpoint_key));
        ret = OB_SUCCESS;  // ignore error
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < update_kvs.count(); i++) {
    const ObNetEndpointKey &endpoint_key = update_kvs[i].key_;
    obrpc::ObNetEndpointPredictIngressArg arg;
    arg.endpoint_key_.assign(endpoint_key);
    if (OB_FAIL(proxy_batch.call(endpoint_key.addr_, timeout_ts, arg))) {
      LOG_WARN("fail to call async batch rpc", KR(ret), K(endpoint_key.addr_), K(arg));
      ret = OB_SUCCESS;  // ignore error
    }
  }

  ObArray<int> return_code_array;
  if (OB_FAIL(proxy_batch.wait_all(return_code_array))) {
    LOG_WARN("wait batch result failed", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (return_code_array.count() != update_kvs.count() ||
             return_code_array.count() != proxy_batch.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cnt not match",
        KR(ret),
        "return_cnt",
        return_code_array.count(),
        "result_cnt",
        proxy_batch.get_results().count(),
        "server_cnt",
        ingress_plan_map_.size());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < update_kvs.count(); i++) {
    if (OB_FAIL(return_code_array.at(i))) {
      const ObAddr &addr = proxy_batch.get_dests().at(i);
      LOG_WARN("rpc execute failed", KR(ret), K(addr), K(update_kvs[i]));
      ret = OB_SUCCESS;  // ignore error
    } else {
      const obrpc::ObNetEndpointPredictIngressRes *result = proxy_batch.get_results().at(i);
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
        ret = OB_SUCCESS;  // ignore error
      } else {
        ObNetEndpointValue *endpoint_value = update_kvs[i].value_;
        endpoint_value->predicted_bw_ = result->predicted_bw_;
      }
    }
  }
  return ret;
}

int ObNetEndpointIngressManager::update_ingress_plan(ObNetEndpointKVArray &update_kvs)
{
  int ret = OB_SUCCESS;
  int64_t *predicted_bws = nullptr;
  if (update_kvs.count() == 0) {
    // do nothing
  } else {
    predicted_bws = (int64_t *)ob_malloc(sizeof(int64_t) * update_kvs.count(), "INGRESS_SERVICE");
    if (OB_ISNULL(predicted_bws)) {
      LOG_WARN("failed to alloc memory for objs");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t remain_bw_limit = total_bw_limit_;
      int64_t valid_count = 0;
      for (int64_t i = 0; i < update_kvs.count(); i++) {
        ObNetEndpointValue *endpoint_value = update_kvs[i].value_;
        if (OB_UNLIKELY(endpoint_value->predicted_bw_ == -1)) {
          if (endpoint_value->assigned_bw_ != -1) {
            remain_bw_limit -= endpoint_value->assigned_bw_;
          }
        } else {
          predicted_bws[valid_count++] = endpoint_value->predicted_bw_;
        }
      }

      int64_t baseline_bw = 0;
      int64_t extra_bw = 0;
      if (remain_bw_limit <= 0) {
        remain_bw_limit = 0;
      }
      std::sort(predicted_bws, predicted_bws + valid_count);
      int64_t average = (int64_t)remain_bw_limit / valid_count;
      for (int i = 0; i < valid_count; i++) {
        average = (int64_t)remain_bw_limit / (valid_count - i);
        if (average <= predicted_bws[i]) {
          remain_bw_limit = 0;
          break;
        } else {
          remain_bw_limit -= predicted_bws[i];
        }
      }
      baseline_bw = average;
      extra_bw = (int64_t)remain_bw_limit / valid_count;

      for (int64_t i = 0; i < update_kvs.count(); i++) {
        ObNetEndpointValue *endpoint_value = update_kvs[i].value_;
        if (OB_UNLIKELY(endpoint_value->predicted_bw_ == -1)) {
          // do nothing, remain old assigned_bw
        } else {
          int64_t predicted_bw = endpoint_value->predicted_bw_;
          int64_t assigned_bw = -1;
          if (predicted_bw > baseline_bw) {
            assigned_bw = baseline_bw;
          } else {
            assigned_bw = predicted_bw;
          }
          assigned_bw += extra_bw;
          endpoint_value->assigned_bw_ = assigned_bw;
        }
      }
    }
  }

  if (OB_NOT_NULL(predicted_bws)) {
    ob_free(predicted_bws);
  }
  return ret;
}

int ObNetEndpointIngressManager::commit_bw_limit_plan(ObNetEndpointKVArray &update_kvs)
{
  int ret = OB_SUCCESS;
  ObNetEndpointSetIngressProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::net_endpoint_set_ingress);

  for (int64_t i = 0; i < update_kvs.count(); i++) {
    const ObNetEndpointKey &endpoint_key = update_kvs[i].key_;
    ObNetEndpointValue *endpoint_value = update_kvs[i].value_;
    obrpc::ObNetEndpointSetIngressArg arg;
    arg.endpoint_key_.assign(endpoint_key);
    arg.assigned_bw_ = endpoint_value->assigned_bw_;
    const int64_t timeout_ts = GCONF.rpc_timeout;
    if (OB_FAIL(proxy_batch.call(endpoint_key.addr_, timeout_ts, arg))) {
      LOG_WARN("fail to call async batch rpc", KR(ret), K(endpoint_key.addr_), K(arg));
      ret = OB_SUCCESS;  // ignore error
    }
  }

  ObArray<int> return_code_array;
  if (OB_FAIL(proxy_batch.wait_all(return_code_array))) {
    LOG_WARN("wait batch result failed", KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (return_code_array.count() != update_kvs.count() ||
             return_code_array.count() != proxy_batch.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cnt not match",
        KR(ret),
        "return_cnt",
        return_code_array.count(),
        "result_cnt",
        proxy_batch.get_results().count(),
        "server_cnt",
        ingress_plan_map_.size());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
    if (OB_FAIL(return_code_array.at(i))) {
      const ObAddr &addr = proxy_batch.get_dests().at(i);
      LOG_WARN("rpc execute failed", KR(ret), K(addr));
      ret = OB_SUCCESS;  // ignore error
    }
  }
  return ret;
}
int ObNetEndpointIngressManager::set_total_bw_limit(int64_t total_bw_limit)
{
  int ret = OB_SUCCESS;
  if (total_bw_limit >= 0) {
    if (total_bw_limit_ != total_bw_limit) {
      total_bw_limit_ = total_bw_limit;
      LOG_INFO("total_bw_limit update success", K(total_bw_limit));
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("totall bandwidth limit must be greater than 0", K(ret), K(total_bw_limit));
  }
  return ret;
}
int64_t ObNetEndpointIngressManager::get_map_size()
{
  return ingress_plan_map_.size();
}

int ObIngressBWAllocService::init(const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;
  tg_id_ = lib::TGDefIDs::IngressService;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(ingress_manager_.init())) {
    LOG_ERROR("failed to init ingress manager", KR(ret));
  } else if (OB_FAIL(endpoint_ingress_rpc_proxy_.init(GCTX.net_frame_->get_req_transport(), GCTX.self_addr()))) {
    LOG_WARN("failed to init rpc proxy", K(ret));
  } else {
    is_inited_ = true;
    cluster_id_ = cluster_id;
    LOG_INFO("[INGRESS_SERVICE] ObIngressBWAllocService init success", K(tg_id_));
  }
  return ret;
}

int ObIngressBWAllocService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("ObIngressBWAllocService TG_START failed", K(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, INGRESS_SERVICE_INTERVAL_US, true))) {
    LOG_WARN("ObIngressBWAllocService TG_SCHEDULE failed", K(ret), K(tg_id_));
  } else {
    ATOMIC_SET(&is_stop_, false);
    LOG_INFO("[INGRESS_SERVICE] ObIngressBWAllocService start success", K(tg_id_));
  }
  return ret;
}
void ObIngressBWAllocService::stop()
{
  ATOMIC_SET(&is_stop_, true);
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    LOG_INFO("[INGRESS_SERVICE] ObIngressBWAllocService stop success", K(tg_id_));
  }
}

void ObIngressBWAllocService::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
    LOG_INFO("[INGRESS_SERVICE] ObIngressBWAllocService wait success", K(tg_id_));
  }
}

void ObIngressBWAllocService::destroy()
{
  is_inited_ = false;
  ATOMIC_SET(&is_leader_, false);
  ATOMIC_SET(&is_stop_, true);
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
    ingress_manager_.destroy();
    LOG_INFO("[INGRESS_SERVICE] ObIngressBWAllocService destroy success", K(tg_id_));
  }
}

int ObIngressBWAllocService::register_endpoint(const obrpc::ObNetEndpointKey &endpoint_key, const int64_t expire_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_leader())) {
    ret = OB_RPC_PACKET_INVALID;
    LOG_WARN("[INGRESS_SERVICE] ObIngressBWAllocService not leader", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_stop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[INGRESS_SERVICE] ObIngressBWAllocService thread is stopped", KR(ret), KP(this));
  } else if (OB_FAIL(ingress_manager_.register_endpoint(endpoint_key, expire_time))) {
    LOG_WARN("[INGRESS_SERVICE] ObIngressBWAllocService register endpoint failed", KR(ret), KP(this));
  }
  return ret;
}
void ObIngressBWAllocService::follower_task()
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  obrpc::ObNetEndpointRegisterArg arg;
  arg.endpoint_key_.addr_ = GCTX.self_addr();
  arg.endpoint_key_.group_id_ = 0;  // not used now
  arg.expire_time_ = current_time + ENDPOINT_EXPIRE_INTERVAL_US;
  ObAddr rs_addr;
  int64_t server_standby_bw = GCONF._server_standby_fetch_log_bandwidth_limit;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret), K(is_inited_));
  } else if (server_standby_bw > 0) {
    if (OB_FAIL(obrpc::global_poc_server.update_server_standby_fetch_log_bandwidth_limit(server_standby_bw))) {
      COMMON_LOG(WARN, "Failed to set server-level standby fetchlog bandwidth limit");
    }
  } else if (GCONF.standby_fetch_log_bandwidth_limit == 0) {
    // unlimited
    if (OB_FAIL(obrpc::global_poc_server.update_server_standby_fetch_log_bandwidth_limit(RATE_UNLIMITED))) {
      COMMON_LOG(WARN, "Failed to set server-level standby fetchlog bandwidth limit");
    }
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(cluster_id_, rs_addr))) {
    COMMON_LOG(ERROR, "failed to get root server address", K(ret));
  } else if (OB_FAIL(endpoint_ingress_rpc_proxy_.to(rs_addr).timeout(500 * 1000).net_endpoint_register(arg))) {
    COMMON_LOG(WARN, "net endpoint register failed", K(ret), K(rs_addr), K(arg));
    if (expire_time_ != 0 && current_time > expire_time_) {
      COMMON_LOG(INFO, "net endpoint register expired", K(ret), K(rs_addr), K(arg));
      if (OB_FAIL(obrpc::global_poc_server.update_server_standby_fetch_log_bandwidth_limit(0))) {
        COMMON_LOG(WARN, "limit bandwitdh to zero failed", K(ret), K(rs_addr), K(arg));
      }
    }
  } else {
    expire_time_ = arg.expire_time_;
  }
}

void ObIngressBWAllocService::leader_task()
{
  int ret = OB_SUCCESS;
  // reload GCONF.standby_fetchlog_bandwidth_limit
  int64_t total_bw_limit = GCONF.standby_fetch_log_bandwidth_limit;
  ingress_manager_.set_total_bw_limit(total_bw_limit);

  if (total_bw_limit == 0) {
    // do nothing
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (ingress_manager_.get_map_size() > 0) {
    ObNetEndpointKVArray update_kvs;
    if (OB_FAIL(ingress_manager_.collect_predict_bw(update_kvs))) {
      LOG_ERROR("update observer predicted bandwidth failed", K(ret));
    } else if (OB_FAIL(ingress_manager_.update_ingress_plan(update_kvs))) {
      LOG_ERROR("update ingress plan failed", K(ret));
    } else if (OB_FAIL(ingress_manager_.commit_bw_limit_plan(update_kvs))) {
      LOG_ERROR("assign ingress bandwidth failed", K(ret));
    }
  }
}
void ObIngressBWAllocService::runTimerTask()
{
  // for follower register task
  follower_task();

  // for leader
  if (is_leader()) {
    leader_task();
  }
}

void ObIngressBWAllocService::switch_to_follower_forcedly()
{
  ATOMIC_SET(&is_leader_, false);
}
int ObIngressBWAllocService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&is_leader_, true);
  return ret;
}
int ObIngressBWAllocService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&is_leader_, false);
  return ret;
}
int ObIngressBWAllocService::resume_leader()
{
  int ret = OB_SUCCESS;
  if (!is_leader()) {
    ATOMIC_SET(&is_leader_, true);
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase