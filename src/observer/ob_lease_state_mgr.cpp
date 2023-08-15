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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_lease_state_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
#include "share/ob_zone_merge_info.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
namespace observer
{
ObRefreshSchemaStatusTimerTask::ObRefreshSchemaStatusTimerTask()
{}

void ObRefreshSchemaStatusTimerTask::destroy()
{
}

void ObRefreshSchemaStatusTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema status proxy", KR(ret));
  } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
    LOG_WARN("fail to load refresh schema status", KR(ret));
  } else {
    LOG_INFO("refresh schema status success");
  }
}

//////////////////////////////////////

ObLeaseStateMgr::ObLeaseStateMgr()
  : inited_(false), stopped_(false), lease_response_(), lease_expire_time_(0),
    hb_timer_(), cluster_info_timer_(), merge_timer_(), rs_mgr_(NULL), rpc_proxy_(NULL), heartbeat_process_(NULL),
    hb_(), renew_timeout_(RENEW_TIMEOUT), ob_service_(NULL),
    baseline_schema_version_(0), heartbeat_expire_time_(0)
{
}

ObLeaseStateMgr::~ObLeaseStateMgr()
{
  destroy();
}

void ObLeaseStateMgr::destroy()
{
  if (inited_) {
    stopped_ = false;
    hb_timer_.destroy();
    cluster_info_timer_.destroy();
    merge_timer_.destroy();
    rs_mgr_ = NULL;
    rpc_proxy_ = NULL;
    heartbeat_process_ = NULL;
    inited_ = false;
  }
}

// ObRsMgr should be inited by local config before call ObLeaseStateMgr.init
int ObLeaseStateMgr::init(
    ObCommonRpcProxy *rpc_proxy, ObRsMgr *rs_mgr,
    IHeartBeatProcess *heartbeat_process,
    ObService &service,
    const int64_t renew_timeout) //default RENEW_TIMEOUT = 2s
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == rpc_proxy || NULL == rs_mgr
      || NULL == heartbeat_process || renew_timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(rpc_proxy), KP(rs_mgr),
        KP(heartbeat_process), K(renew_timeout), K(ret));
  } else if (!rs_mgr->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_mgr not inited", "rs_mgr inited", rs_mgr->is_inited(), K(ret));
  } else if (OB_FAIL(hb_timer_.init("LeaseHB"))) {
    LOG_WARN("hb_timer_ init failed", KR(ret));
  } else if (OB_FAIL(cluster_info_timer_.init("ClusterTimer"))) {
    LOG_WARN("cluster_info_timer_ init failed", KR(ret));
  } else if (OB_FAIL(merge_timer_.init("MergeTimer"))) {
    LOG_WARN("merge_timer_ init failed", KR(ret));
  } else {
    rs_mgr_ = rs_mgr;
    rpc_proxy_ = rpc_proxy;
    heartbeat_process_ = heartbeat_process;
    renew_timeout_ = renew_timeout;
    ob_service_ = &service;
    if (OB_FAIL(hb_.init(this))) {
      LOG_WARN("hb_.init failed", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObLeaseStateMgr::register_self()
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin register_self");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(do_renew_lease())) {
      LOG_WARN("do_renew_lease failed", K(ret));
    }

    LOG_INFO("start_heartbeat anyway");
    // ignore ret overwrite
    if (OB_FAIL(start_heartbeat())) {
      LOG_ERROR("start_heartbeat failed", K(ret));
    }
  }
  return ret;
}

int ObLeaseStateMgr::register_self_busy_wait()
{
  int ret = OB_SUCCESS;

  ObCurTraceId::init(GCONF.self_addr_);
  LOG_INFO("begin register_self_busy_wait");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (!stopped_) {
      if (OB_FAIL(try_report_sys_ls())) {
        LOG_WARN("fail to try report sys log stream");
      } else if (OB_FAIL(do_renew_lease())) {
        LOG_WARN("fail to do_renew_lease", KR(ret));
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("register failed, will try again", KR(ret),
            "retry latency", REGISTER_TIME_SLEEP / 1000000);
        ob_usleep(static_cast<useconds_t>(REGISTER_TIME_SLEEP));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = rs_mgr_->renew_master_rootserver())) {
          LOG_WARN("renew_master_rootserver failed", K(tmp_ret));
          if (OB_SUCCESS != (tmp_ret = ob_service_->refresh_sys_tenant_ls())) {
            LOG_WARN("fail to refresh core partition", K(tmp_ret));
          }
        } else {
          LOG_INFO("renew_master_rootserver successfully, try register again");
        }
      } else {
        LOG_INFO("register self successfully!");
        if (OB_FAIL(start_heartbeat())) {
          LOG_ERROR("start_heartbeat failed", K(ret));
        }
        break;
      }
    }
  }
  if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("fail to register_self_busy_wait", KR(ret));
  }
  LOG_INFO("end register_self_busy_wait");
  return ret;
}

int ObLeaseStateMgr::try_report_sys_ls()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("lease manager is stopped", KR(ret));
  } else {
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    const ObLSID ls_id = SYS_LS;
    MTL_SWITCH(tenant_id) {
      bool ls_exist = false;
      ObLSService *ls_svr = NULL;
      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant storage ptr is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ls_svr->check_ls_exist(ls_id, ls_exist))) {
        LOG_WARN("fail to check log stream exist", KR(ret), K(ls_id));
      } else if (!ls_exist) {
        // core log stream not exist
      } else {
        share::ObLSTableOperator *lst_operator = GCTX.lst_operator_;
        share::ObLSReplica ls_replica;
        if (OB_ISNULL(ob_service_) || OB_ISNULL(lst_operator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ob_service or lst_operator ptr is null",
                   KR(ret), KP(ob_service_), KP(lst_operator));
        } else if (OB_FAIL(ob_service_->fill_ls_replica(
                   tenant_id, ls_id, ls_replica))) {
          LOG_WARN("fail to fill log stream replica", KR(ret),
                   K(tenant_id), K(ls_replica));
        } else if (OB_FAIL(lst_operator->update(ls_replica, false/*inner_table_only*/))) {
          LOG_WARN("fail to report sys log stream", KR(ret), K(ls_replica));
        } else if (OB_FAIL(ob_service_->submit_ls_update_task(tenant_id, ls_id))) {
          LOG_WARN("fail to add async update task", KR(ret), K(tenant_id), K(ls_id));
        } else {
          LOG_INFO("try report sys log stream succeed");
        }
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObLeaseStateMgr::renew_lease()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(THE_TRACE)) {
    THE_TRACE->reset();
  }
  NG_TRACE(renew_lease_begin);
  const int64_t start = ObTimeUtility::fast_current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("lease manager is stopped", K(ret));
  } else {
    if (OB_FAIL(do_renew_lease())) {
      LOG_WARN("do_renew_lease failed", K(ret));
      NG_TRACE(renew_master_rs_begin);
      if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
        LOG_WARN("renew_master_rootserver failed", K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ob_service_->refresh_sys_tenant_ls())) {
          LOG_WARN("fail to refresh core partition", K(tmp_ret));
        }
      } else {
        NG_TRACE(renew_lease_end);
        LOG_INFO("renew_master_rootserver successfully, try renew lease again");
        if (OB_FAIL(try_report_sys_ls())) {
          LOG_WARN("fail to try report all core table partition");
        } else if (OB_FAIL(do_renew_lease())) {
          LOG_WARN("try do_renew_lease again failed, will do it no next heartbeat", K(ret));
        }
      }
   }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("renew_lease successfully!");
    }
    NG_TRACE_EXT(renew_lease_end, OB_ID(ret), ret);
    const int64_t cost = ObTimeUtility::fast_current_time() - start;
    if (OB_UNLIKELY(cost > DELAY_TIME || OB_FAIL(ret))
        && OB_NOT_NULL(THE_TRACE)) {
      FORCE_PRINT_TRACE(THE_TRACE, "[slow heartbeat]");
    }
    const bool repeat = false;
    if (OB_FAIL(hb_timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("schedule failed", LITERAL_K(DELAY_TIME), K(repeat), K(ret));
    }
  }
  return ret;
}

int ObLeaseStateMgr::start_heartbeat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool repeat = false;
    if (OB_FAIL(hb_timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("schedule failed", LITERAL_K(DELAY_TIME), K(repeat), K(ret));
    }
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObLeaseStateMgr::update_master_key_info(
    const share::ObLeaseResponse &lease_response)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const common::ObIArray<std::pair<uint64_t, ObLeaseResponse::TLRpKeyVersion> > &master_key_array
      = lease_response.tenant_max_key_version_;
    common::ObArray<std::pair<uint64_t, uint64_t> > max_key_version_array;
    common::ObArray<std::pair<uint64_t, uint64_t> > max_available_key_version_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < master_key_array.count(); ++i) {
      const std::pair<uint64_t, ObLeaseResponse::TLRpKeyVersion> &key = master_key_array.at(i);
      std::pair<uint64_t, uint64_t> max_key_version
        = std::pair<uint64_t, uint64_t>(key.first, key.second.max_key_version_);
      std::pair<uint64_t, uint64_t> available_key_version
        = std::pair<uint64_t, uint64_t>(key.first, key.second.max_available_key_version_);
      if (OB_FAIL(max_key_version_array.push_back(max_key_version))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        if (available_key_version.second > 0) {
          if (OB_FAIL(max_available_key_version_array.push_back(available_key_version))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMasterKeyGetter::instance().got_versions(
              max_key_version_array))) {
        LOG_WARN("fail to update got versions", KR(ret));
      } else if (OB_FAIL(ObMasterKeyGetter::instance().update_active_versions(
              max_available_key_version_array))) {
        LOG_WARN("fail to update active versions", KR(ret));
      }
    }
  }
  return ret;
}
#endif

int ObLeaseStateMgr::do_renew_lease()
{
  int ret = OB_SUCCESS;
  ObLeaseRequest lease_request;
  ObLeaseResponse lease_response;
  ObAddr rs_addr;
  NG_TRACE(do_renew_lease_begin);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(heartbeat_process_->init_lease_request(lease_request))) {
    LOG_WARN("init lease request failed", K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else {
    NG_TRACE(send_heartbeat_begin);
    ret = rpc_proxy_->to(rs_addr).timeout(renew_timeout_)
        .renew_lease(lease_request, lease_response);
    if (lease_response.lease_expire_time_ > 0) {
      // for compatible with old version
      lease_response.heartbeat_expire_time_ = lease_response.lease_expire_time_;
    }
    NG_TRACE_EXT(send_heartbeat_end, OB_ID(ret), ret);
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(!lease_response.is_valid())) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(tmp_ret), K(lease_response));
      }

      if (baseline_schema_version_ < lease_response.baseline_schema_version_) {
        if (OB_SUCCESS != (tmp_ret = GCTX.schema_service_->update_baseline_schema_version(
            OB_SYS_TENANT_ID, lease_response.baseline_schema_version_))) {
          LOG_WARN("fail to update baseline schema version", KR(ret), KR(tmp_ret), K(lease_response));
        } else {
          LOG_INFO("update baseline schema version", KR(ret), "old_version", baseline_schema_version_,
                   "new_version", lease_response.baseline_schema_version_);
          baseline_schema_version_ = lease_response.baseline_schema_version_;
        }
      }
#ifdef OB_BUILD_TDE_SECURITY
      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != (tmp_ret = update_master_key_info(lease_response))) {
          LOG_WARN("fail to update master key info", KR(ret), K(tmp_ret), K(lease_response));
        }
      }
      NG_TRACE_EXT(update_master_key_info, OB_ID(ret), tmp_ret);
#endif
      const int64_t now = ObTimeUtility::current_time();
      if (OB_SUCC(ret) && lease_response.heartbeat_expire_time_ > now) {
        LOG_DEBUG("renew_lease from  master_rs successfully", K(rs_addr));
        if (OB_FAIL(set_lease_response(lease_response))) {
          LOG_WARN("fail to set lease response", K(ret));
        } else if (OB_FAIL(heartbeat_process_->do_heartbeat_event(lease_response_))) {
          LOG_WARN("fail to process new lease info", K_(lease_response), K(ret));
        }
        NG_TRACE_EXT(do_heartbeat_event, OB_ID(ret), ret);
      }
    } else {
      LOG_WARN("can't get lease from rs", K(rs_addr), K(ret));
    }
  }
  NG_TRACE_EXT(do_renew_lease_end, OB_ID(ret), ret);
  return ret;
}

ObLeaseStateMgr::HeartBeat::HeartBeat()
  : inited_(false), lease_state_mgr_(NULL)
{
}

ObLeaseStateMgr::HeartBeat::~HeartBeat()
{
}

int ObLeaseStateMgr::HeartBeat::init(ObLeaseStateMgr *lease_state_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == lease_state_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(lease_state_mgr), K(ret));
  } else {
    lease_state_mgr_ = lease_state_mgr;
    inited_ = true;
  }
  return ret;
}

void ObLeaseStateMgr::HeartBeat::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(lease_state_mgr_->renew_lease())) {
    LOG_WARN("fail to renew lease", K(ret));
  }
}

}//end namespace observer
}//end namespace oceanbase
