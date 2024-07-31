/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS
#include "ob_heartbeat_service.h"

#include "share/ob_define.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_version.h"
#include "share/ob_zone_table_operation.h"
#include "lib/thread/threads.h"               // set_run_wrapper
#include "lib/mysqlclient/ob_mysql_transaction.h"  // ObMySQLTransaction
#include "lib/utility/ob_unify_serialize.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"              // GCTX
#include "logservice/ob_log_base_header.h"          // ObLogBaseHeader
#include "logservice/ob_log_handler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "rootserver/ob_root_utils.h"            // get_proposal_id_from_sys_ls
#include "rootserver/ob_rs_event_history_table_operator.h" // ROOTSERVICE_EVENT_ADD
#include "rootserver/ob_root_service.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
#define HBS_LOG_INFO(fmt, args...) FLOG_INFO("[HEARTBEAT_SERVICE] " fmt, ##args)
#define HBS_LOG_WARN(fmt, args...) FLOG_WARN("[HEARTBEAT_SERVICE] " fmt, ##args)
#define HBS_LOG_ERROR(fmt, args...) FLOG_ERROR("[HEARTBEAT_SERVICE] " fmt, ##args)
ObHeartbeatService::ObHeartbeatService()
    : is_inited_(false),
      sql_proxy_(NULL),
      srv_rpc_proxy_(NULL),
      epoch_id_(palf::INVALID_PROPOSAL_ID),
      whitelist_epoch_id_(palf::INVALID_PROPOSAL_ID),
      hb_responses_epoch_id_(palf::INVALID_PROPOSAL_ID),
      hb_responses_rwlock_(ObLatchIds::HB_RESPONSES_LOCK),
      all_servers_info_in_table_rwlock_(ObLatchIds::ALL_SERVERS_INFO_IN_TABLE_LOCK),
      all_servers_hb_info_(),
      all_servers_info_in_table_(),
      inactive_zone_list_(),
      hb_responses_(),
      need_process_hb_responses_(false)
{
}
ObHeartbeatService::~ObHeartbeatService()
{
}
bool ObHeartbeatService::is_service_enabled_ = false;
int ObHeartbeatService::init()
{
  int ret = OB_SUCCESS;
  int BUCKET_NUM  = 1024; // ** FIXME: (linqiucen.lqc) temp. value
  sql_proxy_ = GCTX.sql_proxy_;
  srv_rpc_proxy_ = GCTX.srv_rpc_proxy_;
  lib::ObMemAttr attr(MTL_ID(), "HB_SERVICE");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has already inited", KR(ret), K(is_inited_));
  } else if (MTL_ID() != OB_SYS_TENANT_ID) {
    // only create hb service threads in sys tenant
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("srv_rpc_proxy_ is null", KR(ret), KP(srv_rpc_proxy_));
  } else if (OB_FAIL(ObTenantThreadHelper::create(
      "HBService",
      lib::TGDefIDs::HeartbeatService,
      *this))) {
    LOG_WARN("fail to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else if (OB_FAIL(all_servers_hb_info_.create(BUCKET_NUM, attr))) {
    LOG_WARN("fail to create all_servers_hb_info_", KR(ret));
  } else {
    {
      SpinWLockGuard guard_for_hb_responses(hb_responses_rwlock_);
      hb_responses_.reset();
      hb_responses_epoch_id_ = palf::INVALID_PROPOSAL_ID;
      need_process_hb_responses_ = false;
    }
    {
      SpinWLockGuard guard_for_servers_info(all_servers_info_in_table_rwlock_);
      all_servers_info_in_table_.reset();
      inactive_zone_list_.reset();
      whitelist_epoch_id_ = palf::INVALID_PROPOSAL_ID;
    }
    all_servers_hb_info_.clear();
    all_servers_info_in_table_.set_attr(attr);
    inactive_zone_list_.set_attr(attr);
    hb_responses_.set_attr(attr);
    set_epoch_id_(palf::INVALID_PROPOSAL_ID);
    is_inited_ = true;
    HBS_LOG_INFO("ObHeartbeatService is inited");
  }
  // we do not need the returned error code when init
  // only try to confirm whether the heartbeat service is enabled as early as possible,
  (void) check_is_service_enabled_();
  return ret;
}
int ObHeartbeatService::check_is_service_enabled_()
{
  int ret = OB_SUCCESS;
  uint64_t sys_tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_tenant_data_version))) {
    LOG_WARN("fail to get sys tenant's min data version", KR(ret));
  } else if (sys_tenant_data_version >= DATA_VERSION_4_2_0_0) {
    is_service_enabled_ = true;
    HBS_LOG_INFO("the heartbeart service is enabled now", K(sys_tenant_data_version), K(is_service_enabled_));
  }
  return ret;
}
void ObHeartbeatService::destroy()
{
  {
    SpinWLockGuard guard_for_hb_responses(hb_responses_rwlock_);
    hb_responses_.reset();
    hb_responses_epoch_id_ = palf::INVALID_PROPOSAL_ID;
    need_process_hb_responses_ = false;
  }
  {
    SpinWLockGuard guard_for_servers_info(all_servers_info_in_table_rwlock_);
    all_servers_info_in_table_.reset();
    inactive_zone_list_.reset();
    whitelist_epoch_id_ = palf::INVALID_PROPOSAL_ID;
  }
  is_inited_ = false;
  sql_proxy_ = NULL;
  srv_rpc_proxy_ = NULL;
  set_epoch_id_(palf::INVALID_PROPOSAL_ID);
  all_servers_hb_info_.destroy();
  HBS_LOG_INFO("ObHeartbeatService is destroyed");
  ObTenantThreadHelper::destroy();
}

int ObHeartbeatService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  int64_t epoch_id = palf::INVALID_PROPOSAL_ID;
  ObRole role;
  if (OB_FAIL(ObRootUtils::get_proposal_id_from_sys_ls(epoch_id, role))) {
    LOG_WARN("fail to get proposal id from sys ls", KR(ret));
  } else if (ObRole::LEADER != role) {
    ret = OB_NOT_MASTER;
    HBS_LOG_WARN("not master ls", KR(ret), K(epoch_id), K(role));
  } else {
    if (OB_LIKELY((palf::INVALID_PROPOSAL_ID == epoch_id_ || epoch_id_ < epoch_id)
        && palf::INVALID_PROPOSAL_ID != epoch_id)) {
      set_epoch_id_(epoch_id);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid epoch id", KR(ret), K(epoch_id), K(epoch_id_));
    }
  }
  if (FAILEDx(ObTenantThreadHelper::switch_to_leader())) {
    HBS_LOG_WARN("fail to switch to leader", KR(ret));
  } else {
    HBS_LOG_INFO("switch to leader", KR(ret), K(epoch_id_));
  }
  return ret;
}
void ObHeartbeatService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(check_upgrade_compat_())) {
    LOG_WARN("fail to check upgrade compatibility", KR(ret));
  } else {
    while (!has_set_stop()) {
      uint64_t thread_idx = get_thread_idx();
      int64_t thread_cnt = THREAD_COUNT;
      if (OB_UNLIKELY(thread_idx >= thread_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        HBS_LOG_ERROR("unexpected thread_idx", KR(ret), K(thread_idx), K(thread_cnt));
      } else {
        if (0 == thread_idx) {
          ObCurTraceId::init(GCONF.self_addr_);
          if (OB_FAIL(send_heartbeat_())) {
            LOG_WARN("fail to send heartbeat", KR(ret));
          }
        } else { // 1 == thread_idx
          ObCurTraceId::init(GCONF.self_addr_);
          if (OB_FAIL(manage_heartbeat_())) {
            LOG_WARN("fail to manage heartbeat", KR(ret));
          }
        }
        if(OB_FAIL(ret)) {
          idle(HB_FAILED_IDLE_TIME_US);
        } else {
          idle(HB_IDLE_TIME_US);
        }
      }
    } // end while
  }
}
int ObHeartbeatService::check_upgrade_compat_()
{
  int ret = OB_SUCCESS;
  while (!is_service_enabled_ && !has_set_stop()) {
    if (OB_FAIL(check_is_service_enabled_())) {
      LOG_WARN("fail to check whether the heartbeat service is enabled", KR(ret));
    }
    idle(HB_IDLE_TIME_US);
  }
  if (has_set_stop()) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not leader", KR(ret));
  }
  return ret;
}
int ObHeartbeatService::send_heartbeat_()
{
  int ret = OB_SUCCESS;
  ObHBRequestArray hb_requests;
  int64_t tmp_whitelist_epoch_id = palf::INVALID_PROPOSAL_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("srv_rpc_proxy_ is null", KR(ret), KP(srv_rpc_proxy_));
  } else {
    ObTimeGuard time_guard("ObHeartbeatService::send_heartbeat_", 2 * 1000 * 1000);
    // step 1: prepare hb_requests based on the whitelist
    if (OB_FAIL(prepare_hb_requests_(hb_requests, tmp_whitelist_epoch_id))) {
      LOG_WARN("fail to prepare heartbeat requests", KR(ret));
    } else if (hb_requests.count() <= 0) {
      LOG_INFO("no heartbeat request needs to be sent");
    } else {
      time_guard.click("end prepare_hb_requests");
      ObSendHeartbeatProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::handle_heartbeat);
      int64_t timeout = GCONF.rpc_timeout;  // default value is 2s
      int tmp_ret = OB_SUCCESS;
      ObArray<int> return_ret_array;
      // step 2: send hb_requests to all servers in the whitelist
      for (int64_t i = 0; i < hb_requests.count(); i++) {
        if (OB_TMP_FAIL(proxy.call(
            hb_requests.at(i).get_server(),
            timeout,
            GCONF.cluster_id,
            OB_SYS_TENANT_ID,
            share::OBCG_HB_SERVICE,
            hb_requests.at(i)))) {
          // error code will be ignored here.
          // send rpc to some offline servers will return error, however, it's acceptable
          LOG_WARN("fail to send heartbeat rpc",  KR(ret), KR(tmp_ret), K(hb_requests.at(i)));
        }
      }
      // step 3: wait hb_responses
      if (OB_TMP_FAIL(proxy.wait_all(return_ret_array))) {
        LOG_WARN("fail to wait all batch result", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      time_guard.click("end wait_hb_responses");
      // step 4: save hb_responses
      if (FAILEDx(set_hb_responses_(tmp_whitelist_epoch_id, &proxy))) {
        LOG_WARN("fail to set hb_responses", KR(ret));
      }
      time_guard.click("end set_hb_responses");
    }
  }
  FLOG_INFO("send_heartbeat_ has finished one round", KR(ret));
  return ret;
}
int ObHeartbeatService::set_hb_responses_(const int64_t whitelist_epoch_id, ObSendHeartbeatProxy *proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_UNLIKELY(proxy->get_dests().count() != proxy->get_results().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest addr count != result count", KR(ret), "dest addr count", proxy->get_dests().count(),
        "result count", proxy->get_results().count());
  } else {
    int tmp_ret = OB_SUCCESS;
    SpinWLockGuard guard_for_hb_responses(hb_responses_rwlock_);
    need_process_hb_responses_ = true;
    hb_responses_epoch_id_ = whitelist_epoch_id;
    hb_responses_.reset();
    // don't use arg/dest here because call() may has failue.
    ARRAY_FOREACH_X(proxy->get_results(), idx, cnt, OB_SUCC(ret)) {
      const ObHBResponse *hb_response = proxy->get_results().at(idx);
      const ObAddr &dest_addr = proxy->get_dests().at(idx);
      if (OB_ISNULL(hb_response)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hb_response is null", KR(ret), KR(tmp_ret), KP(hb_response));
      } else if (OB_UNLIKELY(!hb_response->is_valid())) {
        // if an observer does not reply the rpc, we will get an invalid hb_response.
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("There exists a server not responding to the hb service",
            KR(ret), KR(tmp_ret), KPC(hb_response), K(dest_addr));
      } else if (OB_FAIL(hb_responses_.push_back(*hb_response))) {
        LOG_WARN("fail to push an element into hb_responses_", KR(ret), KPC(hb_response));
      } else {
        LOG_TRACE("receive a heartbeat response", KPC(hb_response));
      }
    }
  }
  return ret;
}
int ObHeartbeatService::get_and_reset_hb_responses_(
    ObHBResponseArray &hb_responses,
    int64_t &hb_responses_epoch_id)
{
  int ret = OB_SUCCESS;
  // set hb_responses = hb_responses_
  // locking hb_responses_ too long will block send_heartbeat()
  // therefore we process hb_responses rather than hb_responses_
  hb_responses.reset();
  hb_responses_epoch_id = palf::INVALID_PROPOSAL_ID;
  SpinWLockGuard guard_for_hb_responses(hb_responses_rwlock_);
  if (need_process_hb_responses_) {
    if (OB_FAIL(hb_responses.assign(hb_responses_))) {
      LOG_WARN("fail to assign tmp_hb_responses", KR(ret), K(hb_responses_));
    } else {
      need_process_hb_responses_ = false;
      hb_responses_epoch_id = hb_responses_epoch_id_;
      hb_responses_epoch_id_ = palf::INVALID_PROPOSAL_ID;
      hb_responses_.reset();
    }
  } else {
    ret = OB_NEED_WAIT;
    LOG_WARN("currently there are no hb_responses need to be proccessed", KR(ret));
  }
  return ret;
}
int ObHeartbeatService::prepare_hb_requests_(ObHBRequestArray &hb_requests, int64_t &whitelist_epoch_id)
{
  int ret = OB_SUCCESS;
  hb_requests.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    // ensure when we prepare hb_requests,
    // we should mark these hb_requests are based on which whitelist.
    // In other words, we should mark the whitelist's corresponding whitelist_epoch_id_.
    SpinRLockGuard guard_for_servers_info(all_servers_info_in_table_rwlock_);
    ObHBRequest hb_request;
    whitelist_epoch_id = whitelist_epoch_id_;
    ARRAY_FOREACH_X(all_servers_info_in_table_, idx, cnt, OB_SUCC(ret)) {
      const ObServerInfoInTable &server_info = all_servers_info_in_table_.at(idx);
      bool is_stopped = false;
      if (OB_UNLIKELY(!server_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        HBS_LOG_WARN("invalid server info in table", KR(ret), K(server_info));
      } else {
        if (server_info.is_stopped() || has_exist_in_array(inactive_zone_list_, server_info.get_zone())) {
          is_stopped = true;
        }
      }
      if (OB_SUCC(ret)) {
        hb_request.reset();
        if (OB_FAIL(hb_request.init(
            server_info.get_server(),
            server_info.get_server_id(),
            GCTX.self_addr(),
            is_stopped ? RSS_IS_STOPPED : RSS_IS_WORKING,
            whitelist_epoch_id))) {
          LOG_WARN("fail to init hb_request", KR(ret), K(server_info), K(is_stopped),
              K(GCTX.self_addr()), K(whitelist_epoch_id));
        } else if (OB_FAIL(hb_requests.push_back(hb_request))) {
          LOG_WARN("fail to push an element into hb_requests", KR(ret), K(hb_request));
        } else {}
      }
    }
  }
  return ret;
}
int ObHeartbeatService::manage_heartbeat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    ObTimeGuard time_guard("ObHeartbeatService::manage_heartbeat_", 2 * 1000 * 1000);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(prepare_whitelist_())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to prepare whitelist", KR(ret), KR(tmp_ret));
    }
    time_guard.click("end prepare_whitelist");
    if (OB_TMP_FAIL(process_hb_responses_())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to prepare heartbeat response", KR(ret), KR(tmp_ret));
    }
    time_guard.click("end process_hb_responses");
  }
  FLOG_INFO("manage_heartbeat_ has finished one round", KR(ret));
  return ret;
}
int ObHeartbeatService::prepare_whitelist_()
{
  int ret = OB_SUCCESS;
  int64_t epoch_id = get_epoch_id_();
  int64_t persistent_epoch_id = palf::INVALID_PROPOSAL_ID;
  ObServerInfoInTableArray tmp_all_servers_info_in_table;
  ObArray<ObZone> tmp_inactive_zone_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(check_or_update_service_epoch_(epoch_id))) {
    LOG_WARN("fail to check or update service epoch", KR(ret), K(epoch_id));
  } else if (OB_FAIL(ObServerTableOperator::get(*sql_proxy_, tmp_all_servers_info_in_table))) {
    // It is possible that heartbeat_service_epoch is changed while we are reading __all_server table
    // It's acceptable, since we cannot update __all_server table when we hold the old heartbeat_service_epoch
    LOG_WARN("fail to read __all_server table", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObZoneTableOperation::get_inactive_zone_list(*sql_proxy_, tmp_inactive_zone_list))) {
    LOG_WARN("fail to get inactive zone list", KR(ret), KP(sql_proxy_));
  } else {
    SpinWLockGuard guard_for_servers_info(all_servers_info_in_table_rwlock_);
    whitelist_epoch_id_ = epoch_id;
    if (OB_FAIL(all_servers_info_in_table_.assign(tmp_all_servers_info_in_table))) {
      all_servers_info_in_table_.reset();
      whitelist_epoch_id_ = palf::INVALID_PROPOSAL_ID;
      LOG_WARN("fail to assign all_servers_info_in_table_", KR(ret), K(tmp_all_servers_info_in_table));
    } else if (OB_FAIL(inactive_zone_list_.assign(tmp_inactive_zone_list))) {
      LOG_WARN("fail to assign inactive_zone_list_",KR(ret), K(tmp_inactive_zone_list));
    }
  }
  return ret;
}
int ObHeartbeatService::check_or_update_service_epoch_(const int64_t epoch_id)
{
  // if persistent_epoch_id == epoch_id: check ok.
  // if persistent_epoch_id < epoch_id: update heartbeat_service_epoch in __all_service_epoch table
  //                                    if the updation is successful, check ok.
  // if persistent_epoch_id > epoch_id: return error OB_NOT_MASTER
  int ret = OB_SUCCESS;
  int64_t persistent_epoch_id = palf::INVALID_PROPOSAL_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObServiceEpochProxy::get_service_epoch(
      *sql_proxy_,
      OB_SYS_TENANT_ID,
      ObServiceEpochProxy::HEARTBEAT_SERVICE_EPOCH,
      persistent_epoch_id))) {
    LOG_WARN("fail to get heartbeat service epoch", KR(ret),KP(sql_proxy_));
  } else if (palf::INVALID_PROPOSAL_ID == persistent_epoch_id || palf::INVALID_PROPOSAL_ID == epoch_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("epoch id is unexpectedly invalid", KR(ret), K(persistent_epoch_id), K(epoch_id));
  } else if (persistent_epoch_id > epoch_id) {
    ret = OB_NOT_MASTER;
    HBS_LOG_WARN("persistent_epoch_id is greater than epoch_id, which means this server is not leader",
        KR(ret), K(persistent_epoch_id), K(epoch_id));
  } else if (persistent_epoch_id < epoch_id) {
    HBS_LOG_INFO("persistent_epoch_id is smaller than epoch_id", K(persistent_epoch_id), K(epoch_id));
    common::ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_service_epoch(
        trans,
        OB_SYS_TENANT_ID,
        ObServiceEpochProxy::HEARTBEAT_SERVICE_EPOCH,
        epoch_id))) {
      LOG_WARN("fail to check and update service epoch", KR(ret), KP(sql_proxy_), K(epoch_id));
    }
    if (OB_UNLIKELY(!trans.is_started())) {
      LOG_WARN("the transaction is not started");
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit the transaction", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to update __all_service_epoch table", KR(ret));
      }
    }
    // we do not care whether the table is updated successfully
    // we always reset all_servers_info_in_table_ and all_servers_hb_info_
    SpinWLockGuard guard_for_servers_info(all_servers_info_in_table_rwlock_);
    all_servers_info_in_table_.reset();
    whitelist_epoch_id_ = palf::INVALID_PROPOSAL_ID;
    all_servers_hb_info_.clear();
  } else {} // persistent_epoch_id = epoch_id, do nothing.
  return ret;
}
int ObHeartbeatService::process_hb_responses_()
{
  int ret = OB_SUCCESS;
  ObHBResponseArray tmp_hb_responses;
  const int64_t now = ObTimeUtility::current_time();
  int64_t tmp_hb_responses_epoch_id = palf::INVALID_PROPOSAL_ID;
  common::ObArray<common::ObZone> zone_list;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(get_and_reset_hb_responses_(tmp_hb_responses, tmp_hb_responses_epoch_id))) {
    LOG_WARN("fail to get and reset hb_responses", KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_zone_list(*sql_proxy_, zone_list))) {
    LOG_WARN("fail to get zone list", KR(ret));
  } else {
    // Here we do not need to lock all_servers_info_in_table_.
    // There are two threads in heartbeat service.
    // Prepare_whitelist() will modify all_servers_info_in_table_,
    // But prepare_whitelist() and this func. are in the same thread.
    // In another thread, send_heartbeat() only reads server_ and server_id_ in all_servers_info_in_table_
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < all_servers_info_in_table_.count(); i++) {
      // note: we can only update __all_server table successfully when hb_responses_epoch_id is
      //       equal to current heartbeat_service_epoch in __all_service_epoch table.
      //       It means that our whitelist (all_servers_info_in_table_) is not outdated.
      if (OB_TMP_FAIL(check_server_(
          tmp_hb_responses,
          all_servers_info_in_table_.at(i),
          zone_list,
          now,
          tmp_hb_responses_epoch_id))) {
        LOG_WARN("fail to check server", KR(ret), KR(tmp_ret),
            K(all_servers_info_in_table_.at(i)), K(now), K(tmp_hb_responses_epoch_id));
      }
    }
    if (FAILEDx(clear_deleted_servers_in_all_servers_hb_info_())) {
      LOG_WARN("fail to clear deleted servers in all_servers_hb_info_", KR(ret));
    }
  }
  return ret;
}
int ObHeartbeatService::check_server_(
    const ObHBResponseArray &hb_responses,
    const share::ObServerInfoInTable &server_info_in_table,
    const common::ObArray<common::ObZone> &zone_list,
    const int64_t now,
    const int64_t hb_responses_epoch_id)
{
  int ret = OB_SUCCESS;
  ObServerHBInfo server_hb_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server_info_in_table.is_valid()
      || now <= 0
      || palf::INVALID_PROPOSAL_ID == hb_responses_epoch_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalied argument", KR(ret), K(server_info_in_table), K(now), K(hb_responses_epoch_id));
  } else if (OB_FAIL(all_servers_hb_info_.get_refactored(server_info_in_table.get_server(), server_hb_info))) {
    LOG_WARN("fail to get server_hb_info, or get an old server_hb_info", KR(ret),
        K(server_info_in_table.get_server()), K(server_hb_info));
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(init_server_hb_info_(now, server_info_in_table, server_hb_info))) {
        LOG_WARN("fail to init server_hb_info", KR(ret), K(server_info_in_table), K(now));
      } else if (OB_FAIL(all_servers_hb_info_.set_refactored(
          server_hb_info.get_server(),
          server_hb_info,
          0 /* flag:  0 shows that not cover existing object. */))) {
        LOG_WARN("fail to push an element into all_servers_hb_info_", KR(ret), K(server_hb_info));
      } else {}
    }
  }
  if (OB_SUCC(ret)) {
    // check whether the heartbeat response from server_info_in_table.get_server() is received
    int64_t idx = OB_INVALID_INDEX_INT64;
    if (!has_server_exist_in_array_(hb_responses, server_info_in_table.get_server(), idx)) {
      //  heartbeat response is not received
      if (OB_FAIL(check_server_without_hb_response_(
          now,
          server_info_in_table,
          hb_responses_epoch_id,
          server_hb_info))) {
        LOG_WARN("fail to check the server without heartbeat response", KR(ret),
            K(server_info_in_table), K(now), K(hb_responses_epoch_id));
      }
    } else if (OB_UNLIKELY(!hb_responses.at(idx).is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      HBS_LOG_WARN("there exists an invalid element in hb_responses", KR(ret),
          K(hb_responses.at(idx)));
    } else if (OB_FAIL(check_server_with_hb_response_(
        hb_responses.at(idx),
        server_info_in_table,
        zone_list,
        now,
        hb_responses_epoch_id,
        server_hb_info))) { //  heartbeat response is received
      LOG_WARN("fail to check the server with heartbeat response", KR(ret),
          K(hb_responses.at(idx)), K(server_info_in_table), K(now), K(hb_responses_epoch_id));
    }
  }
  return ret;
}
int ObHeartbeatService::check_server_without_hb_response_(
    const int64_t now,
    const share::ObServerInfoInTable &server_info_in_table,
    const int64_t hb_responses_epoch_id,
    ObServerHBInfo &server_hb_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(now <= 0
      || !server_info_in_table.is_valid()
      || !server_hb_info.is_valid()
      || palf::INVALID_PROPOSAL_ID == hb_responses_epoch_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),  K(now), K(server_info_in_table),
        K(server_hb_info), K(hb_responses_epoch_id));
  } else if (OB_FAIL(update_server_hb_info_(
      now,
      false, /* hb_response_exists */
      server_hb_info))) {
    LOG_WARN("fail to update server_hb_info", KR(ret), K(now), K(server_info_in_table),
        K(server_hb_info));
  } else if ((now - server_hb_info.get_last_hb_time() > GCONF.lease_time
          && 0 == server_info_in_table.get_last_offline_time())) {
    if (OB_FAIL(update_table_for_online_to_offline_server_(
        server_info_in_table,
        now,
        hb_responses_epoch_id))) {
      LOG_WARN("fail to update table for online to offline server",
          KR(ret), K(server_info_in_table), K(now), K(hb_responses_epoch_id));
    } else {
      const ObAddr &server = server_info_in_table.get_server();
      ROOTSERVICE_EVENT_ADD("server", "last_offline_time set", "server", server);
    }
  } else {}
  return ret;
}
int ObHeartbeatService::update_table_for_online_to_offline_server_(
    const share::ObServerInfoInTable &server_info_in_table,
    const int64_t now,
    const int64_t hb_responses_epoch_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  bool is_match = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!server_info_in_table.is_valid()
      || now <= 0
      || palf::INVALID_PROPOSAL_ID == hb_responses_epoch_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_info_in_table), K(now), K(hb_responses_epoch_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch_with_trans(
      trans,
      OB_SYS_TENANT_ID,
      ObServiceEpochProxy::HEARTBEAT_SERVICE_EPOCH,
      hb_responses_epoch_id,
      is_match))) {
    LOG_WARN("fail to check and update service epoch", KR(ret), K(hb_responses_epoch_id));
  } else if (OB_UNLIKELY(!is_match)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("hb_responses_epoch_id is not the same as persistent heartbeat service epoch id", KR(ret));
  } else {
    if (OB_FAIL(ObServerTableOperator::update_table_for_online_to_offline_server(
        trans,
        server_info_in_table.get_server(),
        ObServerStatus::OB_SERVER_DELETING == server_info_in_table.get_status(), /* is_deleting */
        now /*last_offline_time */))) {
      LOG_WARN("fail to update __all_server table for online to offline server", KR(ret),
          K(server_info_in_table), K(now));
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_and_refresh_server_(server_info_in_table.get_server(),
          OB_SUCC(ret), trans))) {
    LOG_WARN("failed to end trans", KR(ret), K(tmp_ret), K(server_info_in_table));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObHeartbeatService::end_trans_and_refresh_server_(
      const ObAddr &server,
      const bool commit,
      common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server is invalid", KR(ret), K(server));
  } else if (!trans.is_started()) {
    LOG_WARN("the transaction is not started");
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(trans.end(commit))) {
      HBS_LOG_WARN("fail to commit the transaction", KR(ret),
          K(server), K(commit));
    }
    //ignore error of refresh and on server_status_change
    if (OB_TMP_FAIL(SVR_TRACER.refresh())) {
      LOG_WARN("fail to refresh server tracer", KR(ret), KR(tmp_ret));
    }
    if (OB_ISNULL(GCTX.root_service_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.root_service_ is null", KR(ret), KR(tmp_ret), KP(GCTX.root_service_));
    } else if (OB_TMP_FAIL(GCTX.root_service_->get_status_change_cb().on_server_status_change(server))) {
      LOG_WARN("fail to execute on_server_status_change", KR(ret), KR(tmp_ret), K(server));
    }
  }
  return ret;
}

int ObHeartbeatService::init_server_hb_info_(
    const int64_t now,
    const share::ObServerInfoInTable &server_info_in_table,
    ObServerHBInfo &server_hb_info)
{
  int ret = OB_SUCCESS;
  const ObServerStatus::DisplayStatus &display_status = server_info_in_table.get_status();
  const int64_t last_offline_time = server_info_in_table.get_last_offline_time();
  const ObAddr &server = server_info_in_table.get_server();
  int64_t last_hb_time = 0;
  ObServerStatus::HeartBeatStatus hb_status = ObServerStatus::OB_HEARTBEAT_MAX;

  server_hb_info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server_info_in_table.is_valid()
      || now - last_offline_time < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(now), K(server_info_in_table));
  } else {
    if (0 == last_offline_time) { // online, the status is active or deleting
      last_hb_time = now;
      hb_status = ObServerStatus::OB_HEARTBEAT_ALIVE;
    } else { // last_offline_time > 0, offline, the status is inactive or deleting
      last_hb_time = last_offline_time - GCONF.lease_time;
      hb_status = ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
      if (now - last_hb_time >= GCONF.server_permanent_offline_time) {
        hb_status = ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE;
      }
    }
    if (FAILEDx(server_hb_info.init(server, last_hb_time, hb_status))) {
      LOG_WARN("fail to init server_hb_info", KR(ret), K(server), K(last_hb_time), K(hb_status));
    } else {
      LOG_INFO("new server_hb_info is generated", K(server_hb_info));
    }
  }
  return ret;
}
int ObHeartbeatService::check_server_with_hb_response_(
    const ObHBResponse &hb_response,
    const share::ObServerInfoInTable &server_info_in_table,
    const common::ObArray<common::ObZone> &zone_list,
    const int64_t now,
    const int64_t hb_responses_epoch_id,
    ObServerHBInfo &server_hb_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(now <= 0
      || !server_hb_info.is_valid())
      || palf::INVALID_PROPOSAL_ID == hb_responses_epoch_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_info_in_table), K(hb_response),
        K(now), K(server_hb_info), K(hb_responses_epoch_id));
  } else if (OB_FAIL(check_if_hb_response_can_be_processed_(
      hb_response,
      server_info_in_table,
      zone_list))) {
    // the validity of hb_response and server_info_in_table is also checked here
    LOG_WARN("hb_response cannot be processed", KR(ret), K(hb_response),
        K(server_info_in_table), K(zone_list));
  }
  if (OB_SUCC(ret)) {
    if ((!server_info_in_table.get_with_rootserver() && hb_response.get_server() == GCTX.self_addr())
        || 0 != server_info_in_table.get_last_offline_time()
        || server_info_in_table.get_build_version() != hb_response.get_build_version()
        || server_info_in_table.get_start_service_time() != hb_response.get_start_service_time()) {
      if (OB_FAIL(update_table_for_server_with_hb_response_(
          hb_response,
          server_info_in_table,
          hb_responses_epoch_id))) {
        LOG_WARN("fail to update table for server with hb_response", KR(ret), K(hb_response),
            K(server_info_in_table), K(hb_responses_epoch_id));
      }
    }
    if (FAILEDx(check_and_execute_start_or_stop_server_(
        hb_response,
        server_hb_info,
        server_info_in_table))) {
      LOG_WARN("fail to check and execute start or stop server", KR(ret),
          K(hb_response), K(server_info_in_table));
    }
    if (FAILEDx(server_hb_info.set_server_health_status(hb_response.get_server_health_status()))) {
        LOG_WARN("fail to set server_health_status", KR(ret), K(hb_response.get_server_health_status()));
    } else if (OB_FAIL(update_server_hb_info_(
        now,
        true, /* hb_response_exists*/
        server_hb_info))) {
      LOG_WARN("fail to get and update server_hb_info", KR(ret), K(hb_response),
          K(server_info_in_table), K(now));
    }
  }
  return ret;
}
int ObHeartbeatService::check_if_hb_response_can_be_processed_(
    const ObHBResponse &hb_response,
    const share::ObServerInfoInTable &server_info_in_table,
    const common::ObArray<common::ObZone> &zone_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server_info_in_table.is_valid()
      || !hb_response.is_valid()
      || server_info_in_table.get_server() != hb_response.get_server())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_info_in_table), K(hb_response));
  } else if (server_info_in_table.get_zone() != hb_response.get_zone()) {
    ret = OB_SERVER_ZONE_NOT_MATCH;
    HBS_LOG_ERROR("server's zone does not match", KR(ret), K(server_info_in_table.get_zone()),
        K(hb_response.get_zone()));
  } else if (server_info_in_table.get_sql_port() != hb_response.get_sql_port()) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("unexpexted error: server's sql port has changed!", KR(ret),
        K(server_info_in_table), K(hb_response));
  } else {
    bool zone_exists = false;
    for (int64_t i = 0; !zone_exists && i < zone_list.count(); i++) {
      if (zone_list.at(i) == hb_response.get_zone()) {
        zone_exists = true;
      }
    }
    if (OB_UNLIKELY(!zone_exists)) {
      ret = OB_ZONE_INFO_NOT_EXIST;
      HBS_LOG_ERROR("zone info not exist", KR(ret), K(hb_response.get_zone()), K(zone_list));
    }
  }
  return ret;
}
int ObHeartbeatService::check_and_execute_start_or_stop_server_(
    const ObHBResponse &hb_response,
    const ObServerHBInfo &server_hb_info,
    const share::ObServerInfoInTable &server_info_in_table)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const ObAddr &server = hb_response.get_server();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!hb_response.is_valid()
      || !server_info_in_table.is_valid()
      || hb_response.get_server() != server_info_in_table.get_server()
      || !server.is_valid()
      || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(hb_response), K(server_info_in_table));
  } else {
    const ObServerHealthStatus &health_status = hb_response.get_server_health_status();
    bool need_start_or_stop_server = false;
    bool is_start = false;
    int64_t affected_rows = 0;
    ObSqlString sql;
    if (server_hb_info.get_server_health_status() != hb_response.get_server_health_status()) {
      if (0 == server_info_in_table.get_stop_time() && !health_status.is_healthy()) {
        is_start = false;
        need_start_or_stop_server = true;
      }
      if (0 != server_info_in_table.get_stop_time() && health_status.is_healthy()) {
        is_start = true;
        need_start_or_stop_server = true;
      }
    }
    if (OB_SUCC(ret) && need_start_or_stop_server) {
      if (is_start) {
        ROOTSERVICE_EVENT_ADD("server", "disk error repaired, start server", "server", server);
        HBS_LOG_INFO("disk error repaired, try to start server", K(server), K(health_status));
        ret = sql.assign_fmt("ALTER SYSTEM START SERVER '%s:%d'", ip, server.get_port());
      } else {
        ROOTSERVICE_EVENT_ADD("server", "disk error, stop server", "server", server);
        HBS_LOG_INFO("disk error, try to stop server", K(server), K(health_status));
        ret = sql.assign_fmt("ALTER SYSTEM STOP SERVER '%s:%d'", ip, server.get_port());
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to assign fmt", KR(ret), K(server), K(is_start));
      } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("fail to write sql", KR(ret),K(server), K(sql));
      } else {
        HBS_LOG_INFO("start or stop server successfully", K(server), K(is_start));
      }
    }
  }
  return ret;
}
int ObHeartbeatService::update_server_hb_info_(
      const int64_t now,
      const bool hb_response_exists,
      ObServerHBInfo &server_hb_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(now <= 0
      || !server_hb_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(now), K(server_hb_info));
  } else {
    const ObServerStatus::HeartBeatStatus& hb_status = server_hb_info.get_hb_status();
    const ObAddr& server = server_hb_info.get_server();
    // step 1: update last_hb_time
    if (hb_response_exists) {
      server_hb_info.set_last_hb_time(now);
    }
    int64_t time_diff = now - server_hb_info.get_last_hb_time();
    // step 2: update hb_status
    if (time_diff >= GCONF.server_permanent_offline_time
        && ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE != hb_status) {
      server_hb_info.set_hb_status(ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE);
      ROOTSERVICE_EVENT_ADD("server", "permanent_offline", "server", server);
      HBS_LOG_INFO("the server becomes permanent offline", K(server), K(time_diff));
    } else if (time_diff >= GCONF.lease_time
        && ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED != hb_status
        && ObServerStatus::OB_HEARTBEAT_PERMANENT_OFFLINE != hb_status) {
      server_hb_info.set_hb_status(ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED);
      ROOTSERVICE_EVENT_ADD("server", "lease_expire", "server", server);
      HBS_LOG_INFO("the server's lease becomes expired'", K(server), K(time_diff));
    } else if (time_diff < GCONF.lease_time
        && ObServerStatus::OB_HEARTBEAT_ALIVE != hb_status) {
      server_hb_info.set_hb_status(ObServerStatus::OB_HEARTBEAT_ALIVE);
      ROOTSERVICE_EVENT_ADD("server", "online", "server", server);
      HBS_LOG_INFO("the server's lease becomes online'", K(server), K(time_diff));
    } else {}
    // step 3: update server_hb_info
    if (FAILEDx(all_servers_hb_info_.set_refactored(
        server_hb_info.get_server(),
        server_hb_info,
        1 /* flag:  0 shows that not cover existing object. */))) {
      LOG_WARN("fail to push an element into all_servers_hb_info_", KR(ret), K(server_hb_info));
    }
  }
  return ret;
}

int ObHeartbeatService::clear_deleted_servers_in_all_servers_hb_info_()
{
  int ret = OB_SUCCESS;
  ObAddr server;
  hash::ObHashMap<ObAddr, ObServerHBInfo>::iterator iter = all_servers_hb_info_.begin();
  if (OB_UNLIKELY(!is_inited_)) { // return false
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    while (OB_SUCC(ret) && iter != all_servers_hb_info_.end()) {
      int64_t idx = OB_INVALID_INDEX_INT64;
      server.reset();
      server = iter->first;
      iter++;
      if (!has_server_exist_in_array_(all_servers_info_in_table_, server, idx)) {
        HBS_LOG_INFO("the server is deleted, it can be removed from all_servers_hb_info", K(server));
        if (OB_FAIL(all_servers_hb_info_.erase_refactored(server))) {
          LOG_WARN("fail to remove the server from all_servers_hb_info", KR(ret), K(server));
        }
      }
    }
  }
  return ret;
}

int ObHeartbeatService::update_table_for_server_with_hb_response_(
    const ObHBResponse &hb_response,
    const share::ObServerInfoInTable &server_info_in_table,
    const int64_t hb_responses_epoch_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  bool is_match = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    HBS_LOG_ERROR("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!hb_response.is_valid()
      || !server_info_in_table.is_valid()
      || hb_response.get_server() != server_info_in_table.get_server()
      || palf::INVALID_PROPOSAL_ID == hb_responses_epoch_id)) {
    ret = OB_INVALID_ARGUMENT;
    // return false
    LOG_WARN("invalid argument", KR(ret), K(hb_response), K(server_info_in_table), K(hb_responses_epoch_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch_with_trans(
      trans,
      OB_SYS_TENANT_ID,
      ObServiceEpochProxy::HEARTBEAT_SERVICE_EPOCH,
      hb_responses_epoch_id,
      is_match))) {
    LOG_WARN("fail to check heartbeat service epoch", KR(ret), K(hb_responses_epoch_id));
  } else if (OB_UNLIKELY(!is_match)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("hb_responses_epoch_id is not the same as persistent heartbeat service epoch id", KR(ret));
  } else {
    const ObAddr &server = server_info_in_table.get_server();
    // *********  check with_rootserver ********* //
    if (OB_SUCC(ret)
        && !server_info_in_table.get_with_rootserver() && server == GCTX.self_addr()) {
      if (OB_FAIL(ObServerTableOperator::update_with_rootserver(trans, server))) {
        HBS_LOG_WARN("fail to update_with_rootserver", KR(ret), K(server));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "rootserver", "server", server);
        HBS_LOG_INFO("server becomes rootserver",  K(server));
      }
    }

    // ********* check if offline to online, then update last_offline_time and status ********* //
    if (OB_SUCC(ret) && 0 != server_info_in_table.get_last_offline_time()) {
      if (OB_FAIL(ObServerTableOperator::update_table_for_offline_to_online_server(
          trans,
          ObServerStatus::OB_SERVER_DELETING == server_info_in_table.get_status(), /* is_deleting */
          server))) {
        HBS_LOG_WARN("fail to reset last_offline_time", KR(ret), K(server));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "last_offline_time reset", "server", server);
        HBS_LOG_INFO("server becomes online",  K(server));
      }
    }
    // *********  check build_version ********* //
    if (OB_SUCC(ret) && server_info_in_table.get_build_version() != hb_response.get_build_version()) {
      if (OB_FAIL(ObServerTableOperator::update_build_version(
          trans,
          server,
          server_info_in_table.get_build_version(), // old value
          hb_response.get_build_version()))) {
        HBS_LOG_WARN("fail to update build_version", KR(ret),
            K(server_info_in_table), K(hb_response));
      } else {
        ROOTSERVICE_EVENT_ADD("server", hb_response.get_build_version().ptr(), "server", server);
        HBS_LOG_INFO("build_version is updated", K(server),
            K(hb_response.get_build_version()), K(hb_response.get_build_version()));
      }
    }
    // *********  check start_service_time ********* //
    if (OB_SUCC(ret) && server_info_in_table.get_start_service_time() != hb_response.get_start_service_time()) {
      if (OB_FAIL(ObServerTableOperator::update_start_service_time(
          trans,
          server,
          server_info_in_table.get_start_service_time(), // old value
          hb_response.get_start_service_time()))) {
        HBS_LOG_WARN("fail to update start service time", KR(ret), K(server),
            K(server_info_in_table.get_start_service_time()), K(hb_response.get_start_service_time()));
      } else {
        ROOTSERVICE_EVENT_ADD("server", "start_service", "server", server);
        HBS_LOG_INFO("start service time is updated", K(server),
            K(server_info_in_table.get_start_service_time()), K(hb_response.get_start_service_time()));
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(end_trans_and_refresh_server_(server_info_in_table.get_server(),
          OB_SUCC(ret), trans))) {
    LOG_WARN("failed to end trans", KR(ret), K(tmp_ret), K(server_info_in_table));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  return ret;
}
template <typename T>
bool ObHeartbeatService::has_server_exist_in_array_(
    const ObIArray<T> &array,
    const common::ObAddr &server,
    int64_t &idx)
{
  bool bret = false;
  idx = OB_INVALID_INDEX_INT64;
  for (int64_t i = 0; i < array.count(); i++) {
    if (server == array.at(i).get_server()) {
      bret = true;
      idx = i;
      break;
    }
  }
  return bret;
}
}
}
