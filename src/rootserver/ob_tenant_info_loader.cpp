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

#define USING_LOG_PREFIX RS

#include "lib/trace/ob_trace_event.h"
#include "share/rc/ob_tenant_base.h"    // MTL_ID
#include "share/scn.h"//SCN
#include "share/ob_all_server_tracer.h"       // ObAllServerTracer
#include "share/ob_global_stat_proxy.h"
#include "observer/ob_server_struct.h"          // GCTX
#include "rootserver/ob_tenant_info_loader.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "logservice/ob_log_service.h"          // ObLogService
#include "storage/tx/ob_ts_mgr.h" // OB_TS_MGR

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{
int ObTenantInfoLoader::mtl_init(ObTenantInfoLoader *&ka)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ka)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ka is null", KR(ret));
  } else if (OB_FAIL(ka->init())) {
    LOG_WARN("failed to init", KR(ret), KP(ka));
  }

  return ret;
}

int ObTenantInfoLoader::init()
{
  int ret = OB_SUCCESS;
  lib::ThreadPool::set_run_wrapper(MTL_CTX());
  const int64_t thread_cnt = 1;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    tenant_id_ = MTL_ID();
    tenant_info_cache_.reset();
    ATOMIC_STORE(&broadcast_times_, 0);
    ATOMIC_STORE(&rpc_update_times_, 0);
    ATOMIC_STORE(&sql_update_times_, 0);
    ATOMIC_STORE(&last_rpc_update_time_us_, OB_INVALID_TIMESTAMP);

    if (!is_user_tenant(tenant_id_)) {
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret));
    } else if (OB_FAIL(create(thread_cnt, "TenantInf"))) {
      LOG_WARN("failed to create tenant info loader thread", KR(ret), K(thread_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantInfoLoader::destroy()
{
  LOG_INFO("tenant info loader destory", KPC(this));
  stop();
  wait();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_info_cache_.reset();
  sql_proxy_ = NULL;
  ATOMIC_STORE(&broadcast_times_, 0);
  ATOMIC_STORE(&rpc_update_times_, 0);
  ATOMIC_STORE(&sql_update_times_, 0);
  ATOMIC_STORE(&last_rpc_update_time_us_, OB_INVALID_TIMESTAMP);
}

int ObTenantInfoLoader::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_user_tenant(tenant_id_)) {
    //meta and sys tenant is primary
    MTL_SET_TENANT_ROLE_CACHE(ObTenantRole::PRIMARY_TENANT);
    LOG_INFO("not user tenant no need load", K(tenant_id_));
  } else if (OB_FAIL(logical_start())) {
    LOG_WARN("failed to start", KR(ret));
  } else {
    LOG_INFO("tenant info loader start", KPC(this));
  }
  return ret;
}

void ObTenantInfoLoader::stop()
{
  logical_stop();
}
void ObTenantInfoLoader::wait()
{
  logical_wait();
}

void ObTenantInfoLoader::wakeup()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_TRACE("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    get_cond().broadcast();
  }
}

void ObTenantInfoLoader::run2()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tenant info loader run", KPC(this));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(get_cond());
    int64_t last_dump_time_us = ObTimeUtility::current_time();

    while (!stop_) {
      const int64_t start_time_us = ObTimeUtility::current_time();
      share::ObAllTenantInfo tenant_info;
      bool content_changed = false;
      bool is_sys_ls_leader = is_sys_ls_leader_();
      const int64_t refresh_time_interval_us = act_as_standby_() && is_sys_ls_leader ?
                ObTenantRoleTransitionConstants::STS_TENANT_INFO_REFRESH_TIME_US :
                ObTenantRoleTransitionConstants::DEFAULT_TENANT_INFO_REFRESH_TIME_US;

      if (need_refresh(refresh_time_interval_us)
          && OB_FAIL(tenant_info_cache_.refresh_tenant_info(tenant_id_, sql_proxy_, content_changed))) {
        LOG_WARN("failed to update tenant info", KR(ret), K_(tenant_id), KP(sql_proxy_));
      }

      const int64_t now_us = ObTimeUtility::current_time();
      const int64_t sql_update_cost_time = now_us - start_time_us;
      if (OB_FAIL(ret)) {
      } else if (content_changed && is_sys_ls_leader) {
        (void)ATOMIC_AAF(&sql_update_times_, 1);
        (void)broadcast_tenant_info_content_();
      }
      const int64_t broadcast_cost_time = ObTimeUtility::current_time() - now_us;

      const int64_t end_time_us = ObTimeUtility::current_time();
      const int64_t cost_time_us = end_time_us - start_time_us;
      if (content_changed) {
        (void)dump_tenant_info_(sql_update_cost_time, is_sys_ls_leader, broadcast_cost_time, end_time_us, last_dump_time_us);
      }
      const int64_t idle_time = max(10 * 1000, refresh_time_interval_us - cost_time_us);
      //At least sleep 10ms, allowing the thread to release the lock
      if (!stop_) {
        get_cond().wait_us(idle_time);
      }
    }//end while
  }
}

void ObTenantInfoLoader::dump_tenant_info_(
      const int64_t sql_update_cost_time,
      const bool is_sys_ls_leader,
      const int64_t broadcast_cost_time,
      const int64_t end_time_us,
      int64_t &last_dump_time_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (0 >= last_dump_time_us || end_time_us < last_dump_time_us) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(last_dump_time_us), K(end_time_us));
  } else {
    const int64_t dump_interval_s = (end_time_us - last_dump_time_us) / 1000000; // 1s unit;
    if (dump_tenant_info_cache_update_action_interval_.reach() && 1 <= dump_interval_s) {
      const uint64_t broadcast_times = ATOMIC_LOAD(&broadcast_times_);
      const uint64_t rpc_update_times = ATOMIC_LOAD(&rpc_update_times_);
      const uint64_t sql_update_times = ATOMIC_LOAD(&sql_update_times_);
      const int64_t broadcast_per_sec = broadcast_times / dump_interval_s; // per second
      const int64_t rpc_update_per_sec = rpc_update_times / dump_interval_s; // per second
      const int64_t sql_update_per_sec = sql_update_times / dump_interval_s; // per second
      if (OB_NOT_NULL(THE_TRACE)) {
        THE_TRACE->reset();
      }
      NG_TRACE_EXT(ob_tenant_info_loader, OB_ID(tenant_id), tenant_id_,
                   OB_ID(is_sys_ls_leader), is_sys_ls_leader,
                   OB_ID(broadcast_cost_time), broadcast_cost_time,
                   OB_ID(broadcast_times), broadcast_times_,
                   OB_ID(broadcast_per_sec), broadcast_per_sec,
                   OB_ID(rpc_update_times), rpc_update_times,
                   OB_ID(rpc_update_per_sec), rpc_update_per_sec,
                   OB_ID(last_rpc_update_time_us), last_rpc_update_time_us_,
                   OB_ID(sql_update_cost_time), sql_update_cost_time,
                   OB_ID(sql_update_times), sql_update_times,
                   OB_ID(tenant_info_cache), tenant_info_cache_, OB_ID(is_inited), is_inited_);
      FORCE_PRINT_TRACE(THE_TRACE, "[dump tenant_info_loader]");
      ATOMIC_STORE(&broadcast_times_, 0);
      ATOMIC_STORE(&rpc_update_times_, 0);
      ATOMIC_STORE(&sql_update_times_, 0);
      last_dump_time_us = ObTimeUtility::current_time();
    }
  }
}

bool ObTenantInfoLoader::need_refresh(const int64_t refresh_time_interval_us)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  bool need_refresh = true;
  const int64_t now = ObTimeUtility::current_time();
  int64_t last_sql_update_time = OB_INVALID_TIMESTAMP;
  int64_t ora_rowscn = 0;

  if (ObTenantRoleTransitionConstants::DEFAULT_TENANT_INFO_REFRESH_TIME_US < refresh_time_interval_us) {
    LOG_WARN("unexpected refresh_time_interval_us");
    need_refresh = true;
  } else if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info, last_sql_update_time, ora_rowscn))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else if (now - last_sql_update_time < refresh_time_interval_us) {
    need_refresh = false;
  }

  return need_refresh;
}

bool ObTenantInfoLoader::is_sys_ls_leader_()
{
  int ret = OB_SUCCESS;
  bool is_sys_ls_leader = false;
  logservice::ObLogService *log_svr = MTL(logservice::ObLogService*);
  int64_t proposal_id = 0;
  common::ObRole role;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(log_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl pointer is null", KR(ret), KP(log_svr));
  } else if (OB_FAIL(log_svr->get_palf_role(share::SYS_LS, role, proposal_id))) {
    LOG_WARN("failed to get palf role", KR(ret));
  } else if (is_strong_leader(role)) {
    is_sys_ls_leader = true;
  }

  return is_sys_ls_leader;
}

int ObTenantInfoLoader::get_max_ls_id(uint64_t &tenant_id, ObLSID &max_ls_id)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  const uint64_t mtl_tenant_id = MTL_ID();
  tenant_id = OB_INVALID_TENANT_ID;
  max_ls_id.reset();
  if (OB_SYS_TENANT_ID == mtl_tenant_id || is_meta_tenant(mtl_tenant_id)) {
    tenant_id = mtl_tenant_id;
    max_ls_id = ObLSID::SYS_LS_ID;
  } else if (OB_FAIL(get_tenant_info(tenant_info))) {
    LOG_WARN("get_tenant_info failed", KR(ret));
  } else {
    tenant_id = tenant_info.get_tenant_id();
    max_ls_id = tenant_info.get_max_ls_id();
  }
  return ret;
}

bool ObTenantInfoLoader::act_as_standby_()
{
  int ret = OB_SUCCESS;
  bool act_as_standby = true;
  ObAllTenantInfo tenant_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret));
  } else if (tenant_info.is_primary() && tenant_info.is_normal_status()) {
    act_as_standby = false;
  }

  return act_as_standby;
}

void ObTenantInfoLoader::broadcast_tenant_info_content_()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT_US = 200 * 1000;  // 200ms
  const int64_t begin_time = ObTimeUtility::current_time();
  ObArray<int> return_code_array;
  share::ObAllTenantInfo tenant_info;
  int64_t last_sql_update_time = OB_INVALID_TIMESTAMP;
  int64_t ora_rowscn = 0;
  uint64_t finish_data_version = 0;
  share::SCN data_version_barrier_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else {
    ObUpdateTenantInfoCacheProxy proxy(
      *GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::update_tenant_info_cache);
    int64_t rpc_count = 0;

    if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info, last_sql_update_time, ora_rowscn, finish_data_version, data_version_barrier_scn))) {
      LOG_WARN("failed to get tenant info", KR(ret));
    } else {
      struct UpdateTenantInfoCacheFunc {
        int64_t &rpc_count;
        share::ObAllTenantInfo &tenant_info;
        ObUpdateTenantInfoCacheProxy &proxy;
        int64_t ora_rowscn;
        uint64_t finish_data_version;
        share::SCN data_version_barrier_scn;

        UpdateTenantInfoCacheFunc(int64_t &rpc_count, share::ObAllTenantInfo &tenant_info,
                                  ObUpdateTenantInfoCacheProxy &proxy, int64_t ora_rowscn,
                                  uint64_t finish_data_version, share::SCN data_version_barrier_scn)
            : rpc_count(rpc_count), tenant_info(tenant_info), proxy(proxy), ora_rowscn(ora_rowscn),
              finish_data_version(finish_data_version),
              data_version_barrier_scn(data_version_barrier_scn)
        {
        }

        int operator()(const share::ObServerInfoInTable &server_info)
        {
          int ret = OB_SUCCESS;
          obrpc::ObUpdateTenantInfoCacheArg arg;
          if (!server_info.is_valid()) {
            LOG_WARN("skip invalid server_info", KR(ret), K(server_info));
          } else if (!server_info.is_alive()) {
            // not send to alive
          } else if (OB_FAIL(arg.init(tenant_info.get_tenant_id(), tenant_info, ora_rowscn,
                                      finish_data_version, data_version_barrier_scn))) {
            LOG_WARN("failed to init arg", KR(ret), K(tenant_info), K(ora_rowscn));
            // use meta rpc process thread
          } else if (OB_FAIL(proxy.call(server_info.get_server(), DEFAULT_TIMEOUT_US,
                                        gen_meta_tenant_id(tenant_info.get_tenant_id()), arg))) {
            LOG_WARN("failed to send rpc", KR(ret), K(server_info), K(tenant_info), K(arg));
          } else {
            rpc_count++;
          }
          return ret;
        }
      };
      ObFunction<int(const ObServerInfoInTable &server_info)> functor(
          UpdateTenantInfoCacheFunc(rpc_count, tenant_info, proxy, ora_rowscn, finish_data_version,
                                    data_version_barrier_scn));
      if (OB_FAIL(share::ObAllServerTracer::get_instance().for_each_server_info(functor))) {
        LOG_WARN("for each server_info failed", KR(ret));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else {
      (void)ATOMIC_AAF(&broadcast_times_, 1);
      ObUpdateTenantInfoCacheRes res;
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); ++i) {
        ret = return_code_array.at(i);
        const ObAddr &dest = proxy.get_dests().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("send rpc is failed", KR(ret), K(i), K(dest));
        } else {
          LOG_INFO("refresh tenant info content success", KR(ret), K(i), K(dest));
        }
      }
    }
  }
  const int64_t cost_time_us = ObTimeUtility::current_time() - begin_time;
  const int64_t PRINT_INTERVAL = 3 * 1000 * 1000L;
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    LOG_INFO("broadcast_tenant_info_content_ finished", KR(ret), K_(tenant_id), K(cost_time_us),
           K(return_code_array), K_(broadcast_times));
  }
  return ;
}

int ObTenantInfoLoader::get_valid_sts_after(const int64_t specified_time_us, share::SCN &standby_scn)
{
  int ret = OB_SUCCESS;
  standby_scn.set_min();
  share::ObAllTenantInfo tenant_info;
  int64_t last_sql_update_time = OB_INVALID_TIMESTAMP;
  int64_t ora_rowscn = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_INVALID_TIMESTAMP == specified_time_us) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(specified_time_us));
  } else if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info, last_sql_update_time, ora_rowscn))) {
    if (OB_NEED_WAIT == ret) {
      LOG_TRACE("tenant info cache is not refreshed, need wait", KR(ret));
    } else {
      LOG_WARN("failed to get tenant info", KR(ret));
    }
  } else if (last_sql_update_time <= specified_time_us) {
    ret = OB_NEED_WAIT;
    LOG_TRACE("tenant info cache is old, need wait", KR(ret), K(last_sql_update_time), K(specified_time_us), K(tenant_info));
    wakeup();
  } else if (!tenant_info.is_sts_ready()) {
    ret = OB_NEED_WAIT;
    LOG_TRACE("sts can not work for current tenant status", KR(ret), K(tenant_info));
  } else {
    standby_scn = tenant_info.get_standby_scn();
  }

  const int64_t PRINT_INTERVAL = 3 * 1000 * 1000L;
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    LOG_INFO("get_valid_sts_after", KR(ret), K(specified_time_us), K(last_sql_update_time), K(tenant_info));
  }

  return ret;
}

int ObTenantInfoLoader::get_readable_scn(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  readable_scn.set_min();

  if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, readable_scn))) {
    LOG_WARN("failed to get gts as readable_scn", KR(ret));
  }

  return ret;
}

int ObTenantInfoLoader::check_is_standby_normal_status(bool &is_standby_normal_status)
{
  int ret = OB_SUCCESS;
  is_standby_normal_status = false;

  if (OB_SYS_TENANT_ID == MTL_ID() || is_meta_tenant(MTL_ID())) {
    is_standby_normal_status = false;
  } else {
    // user tenant
    share::ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info))) {
      LOG_WARN("failed to get tenant info", KR(ret));
    } else {
      is_standby_normal_status = tenant_info.is_standby() && tenant_info.is_normal_status();
    }
  }
  return ret;
}

int ObTenantInfoLoader::check_is_primary_normal_status(bool &is_primary_normal_status)
{
  int ret = OB_SUCCESS;
  is_primary_normal_status = false;

  if (OB_SYS_TENANT_ID == MTL_ID() || is_meta_tenant(MTL_ID())) {
    is_primary_normal_status = true;
  } else {
    // user tenant
    share::ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info))) {
      LOG_WARN("failed to get tenant info", KR(ret));
    } else {
      is_primary_normal_status = tenant_info.is_primary() && tenant_info.is_normal_status();
    }
  }
  return ret;
}

int ObTenantInfoLoader::get_global_replayable_scn(share::SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  replayable_scn.set_min();

  if (OB_SYS_TENANT_ID == MTL_ID() || is_meta_tenant(MTL_ID())) {
    // there isn't replayable_scn for SYS/META tenant
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("there isn't replayable_scn for SYS/META tenant", KR(ret));
  } else {
    // user tenant
    share::ObAllTenantInfo tenant_info;
    if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info))) {
      LOG_WARN("failed to get tenant info", KR(ret));
    } else {
      replayable_scn = tenant_info.get_replayable_scn();
    }
  }
  return ret;
}

int ObTenantInfoLoader::get_local_replayable_scn(share::SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  replayable_scn.set_min();
  share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();

  if (OB_FAIL(get_global_replayable_scn(replayable_scn))) {
    LOG_WARN("failed to get replayable scn", KR(ret));
  } else if (!is_primary_tenant(tenant_role)) {
    bool is_data_version_crossed = false;
    share::SCN data_version_barrier_scn;
    if (OB_FAIL(tenant_info_cache_.is_data_version_crossed(is_data_version_crossed,
                                                           data_version_barrier_scn))) {
      LOG_WARN("failed to get is_data_version_crossed", KR(ret));
    } else if (is_data_version_crossed) {

    } else if (!data_version_barrier_scn.is_valid_and_not_min()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_version_barrier_scn is invalid", K(ret), K(data_version_barrier_scn));
    } else {
      replayable_scn = data_version_barrier_scn;
    }
  }

  return ret;
}

int ObTenantInfoLoader::get_sync_scn(share::SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo tenant_info;
  sync_scn.set_invalid();
  if (OB_FAIL(get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else {
    sync_scn = tenant_info.get_sync_scn();
  }
  return ret;
}

int ObTenantInfoLoader::get_recovery_until_scn(share::SCN &recovery_until_scn)
{
  int ret = OB_SUCCESS;
  share::ObAllTenantInfo tenant_info;
  recovery_until_scn.set_invalid();
  if (OB_FAIL(get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  } else {
    recovery_until_scn = tenant_info.get_recovery_until_scn();
  }
  return ret;
}

int ObTenantInfoLoader::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();

  if (OB_SYS_TENANT_ID == MTL_ID() || is_meta_tenant(MTL_ID())) {
    // there isn't tenant info for SYS/META tenant
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("there isn't tenant info for SYS/META tenant", KR(ret));
  } else {
    // user tenant
    if (OB_FAIL(tenant_info_cache_.get_tenant_info(tenant_info))) {
      LOG_WARN("failed to get tenant info", KR(ret));
    }
  }

  return ret;
}

int ObTenantInfoLoader::refresh_tenant_info()
{
  int ret = OB_SUCCESS;
  bool content_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tenant_info_cache_.refresh_tenant_info(tenant_id_, sql_proxy_, content_changed))) {
    LOG_WARN("failed to refresh_tenant_info", KR(ret), K_(tenant_id), KP(sql_proxy_));
  }
  return ret;
}

int ObTenantInfoLoader::update_tenant_info_cache(const int64_t new_ora_rowscn,
                                                 const ObAllTenantInfo &new_tenant_info,
                                                 const uint64_t new_finish_data_version,
                                                 const share::SCN &new_data_version_barrier_scn)
{
  int ret = OB_SUCCESS;
  bool refreshed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tenant_info_cache_.update_tenant_info_cache(
                 new_ora_rowscn, new_tenant_info, new_finish_data_version,
                 new_data_version_barrier_scn, refreshed))) {
    LOG_WARN("failed to update_tenant_info_cache", KR(ret), K(new_ora_rowscn), K(new_tenant_info));
  } else if (refreshed) {
    (void)ATOMIC_AAF(&rpc_update_times_, 1);
    (void)ATOMIC_STORE(&last_rpc_update_time_us_, ObTimeUtility::current_time());
  }
  return ret;
}

DEFINE_TO_YSON_KV(ObAllTenantInfoCache,
                  OB_ID(tenant_info), tenant_info_,
                  OB_ID(last_sql_update_time), last_sql_update_time_,
                  OB_ID(ora_rowscn), ora_rowscn_,
                  OB_ID(is_data_version_crossed), is_data_version_crossed_,
                  OB_ID(finish_data_version), finish_data_version_,
                  OB_ID(data_version_barrier_scn), data_version_barrier_scn_);

void ObAllTenantInfoCache::reset()
{
  SpinWLockGuard guard(lock_);
  tenant_info_.reset();
  last_sql_update_time_ = OB_INVALID_TIMESTAMP;
  ora_rowscn_ = 0;
  is_data_version_crossed_ = false;
  // finish_data_version and data_version_barrier_scn_ may be 0 and min_value for a long time,
  // until the data_version barrier log is iterated
  finish_data_version_ = 0;
  data_version_barrier_scn_.set_min();
}

ERRSIM_POINT_DEF(ERRSIM_UPDATE_TENANT_INFO_CACHE_ERROR);
int ObAllTenantInfoCache::refresh_tenant_info(const uint64_t tenant_id,
                                              common::ObMySQLProxy *sql_proxy,
                                              bool &content_changed)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo new_tenant_info;
  int64_t ora_rowscn = 0;
  uint64_t finish_data_version = 0;
  share::SCN data_version_barrier_scn;
  const int64_t new_refresh_time_us = ObClockGenerator::getClock();
  content_changed = false;
  if (OB_ISNULL(sql_proxy) || !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(sql_proxy));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id,
             sql_proxy, false /* for_update */, ora_rowscn, new_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (INT64_MAX == ora_rowscn || 0 == ora_rowscn) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ora_rowscn", KR(ret), K(ora_rowscn), K(tenant_id), K(new_tenant_info), K(lbt()));
  } else if (OB_FAIL(query_new_finish_data_version_(tenant_id, sql_proxy, finish_data_version, data_version_barrier_scn))) {
    LOG_WARN("failed to query new finish data version", KR(ret), K(tenant_id));
  } else {
    /**
    * Only need to refer to tenant role, no need to refer to switchover status.
    * tenant_role is primary in <primary, normal switchoverstatus> or <primary, prep switching_to_standby switchover_status>.
    * When switch to standby starts, it will change to <standby, switch to standby>.
    * During the switch to standby process, some LS may be in RO state. GTS & STS may not work.
    * This also ensures the consistency of tenant_role cache and the tenant role field in all_tenant_info
    */
    SpinWLockGuard guard(lock_);
    if (OB_UNLIKELY(ERRSIM_UPDATE_TENANT_INFO_CACHE_ERROR)) {
      ret = ERRSIM_UPDATE_TENANT_INFO_CACHE_ERROR;
    } else if (ora_rowscn >= ora_rowscn_) {
      if (OB_FAIL(assign_new_tenant_info_(ora_rowscn, new_tenant_info, finish_data_version,
                                          data_version_barrier_scn, content_changed))) {
        LOG_WARN("failed to assign new tenant info", KR(ret), K(ora_rowscn), K(new_tenant_info),
                 K(finish_data_version), K(data_version_barrier_scn));
      }
      // In order to provide sts an accurate time of tenant info refresh time, it is necessary to
      // update last_sql_update_time_ after sql refresh
      last_sql_update_time_ = new_refresh_time_us;
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("refresh tenant info conflict", KR(ret), K(new_tenant_info), K(new_refresh_time_us),
                                      K(tenant_id), K(tenant_info_), K(last_sql_update_time_), K(ora_rowscn_), K(ora_rowscn));
    }
  }

  if (dump_tenant_info_interval_.reach()) {
    LOG_INFO("refresh tenant info", KR(ret), K(new_tenant_info), K(new_refresh_time_us),
                                    K(tenant_id), K(tenant_info_), K(last_sql_update_time_),
                                    K(ora_rowscn_), K(content_changed));
  }

  return ret;
}

int ObAllTenantInfoCache::update_tenant_info_cache(const int64_t new_ora_rowscn,
                                                   const ObAllTenantInfo &new_tenant_info,
                                                   const uint64_t new_finish_data_version,
                                                   const share::SCN &new_data_version_barrier_scn,
                                                   bool &refreshed)
{
  int ret = OB_SUCCESS;
  refreshed = false;
  SpinWLockGuard guard(lock_);
  if (!is_tenant_info_valid_()) {
    ret = OB_EAGAIN;
    LOG_WARN("my tenant_info is invalid, don't refresh", KR(ret), K_(tenant_info), K_(ora_rowscn));
  } else if (OB_UNLIKELY(ERRSIM_UPDATE_TENANT_INFO_CACHE_ERROR)) {
    ret = ERRSIM_UPDATE_TENANT_INFO_CACHE_ERROR;
  } else if (OB_FAIL(assign_new_tenant_info_(new_ora_rowscn, new_tenant_info,
                                             new_finish_data_version, new_data_version_barrier_scn,
                                             refreshed))) {
    LOG_WARN("failed to assign new tenant info", KR(ret), K(new_ora_rowscn), K(new_tenant_info),
             K(new_finish_data_version), K(new_data_version_barrier_scn));
  }

  return ret;
}

// caller should acquire lock before call this function
bool ObAllTenantInfoCache::is_tenant_info_valid_()
{
  return tenant_info_.is_valid() && OB_INVALID_TIMESTAMP != last_sql_update_time_ &&
      0 != ora_rowscn_;
}

// caller should acquire lock before call this function
int ObAllTenantInfoCache::assign_new_tenant_info_(
    const int64_t new_ora_rowscn,
    const ObAllTenantInfo &new_tenant_info,
    const uint64_t new_finish_data_version,
    const share::SCN &new_data_version_barrier_scn,
    bool &assigned)
{
  int ret = OB_SUCCESS;
  assigned = false;

  if (0 == new_ora_rowscn || INT64_MAX == new_ora_rowscn || !new_tenant_info.is_valid() ||
      !new_data_version_barrier_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_ora_rowscn), K(new_tenant_info),
             K(new_finish_data_version), K(new_data_version_barrier_scn));
  } else if (new_ora_rowscn > ora_rowscn_) {
    MTL_SET_TENANT_ROLE_CACHE(new_tenant_info.get_tenant_role().value());
    (void)tenant_info_.assign(new_tenant_info);
    ora_rowscn_ = new_ora_rowscn;
    assigned = true;
    LOG_TRACE("assign new tenant info", K(new_tenant_info), K(new_ora_rowscn), K(tenant_info_),
              K(ora_rowscn_));
    if (new_finish_data_version > finish_data_version_) {
      finish_data_version_ = new_finish_data_version;
      data_version_barrier_scn_ = new_data_version_barrier_scn;
      is_data_version_crossed_ = false;
      LOG_INFO("update finish data version cache", K(finish_data_version_),
               K(data_version_barrier_scn_));
    }
  }

  return ret;
}

int ObAllTenantInfoCache::query_new_finish_data_version_(const uint64_t tenant_id,
                                                         common::ObMySQLProxy *sql_proxy,
                                                         uint64_t &finish_data_version,
                                                         share::SCN &data_version_barrier_scn)
{
  int ret = OB_SUCCESS;
  uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  finish_data_version = 0;
  data_version_barrier_scn.set_min();

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id), K(sql_proxy));
  } else {
    ObGlobalStatProxy proxy(*sql_proxy, exec_tenant_id);
    if (OB_FAIL(proxy.get_finish_data_version(finish_data_version, data_version_barrier_scn))) {
      if (OB_ERR_NULL_VALUE == ret) {
        // have not iterated over the barrier log
        finish_data_version = 0;
        data_version_barrier_scn.set_min();
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get finish data version", KR(ret), K(exec_tenant_id));
      }
    }
  }

  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DATA_VERSION_BARRIER_ERROR);
int ObAllTenantInfoCache::is_data_version_crossed(bool &result,
                                                  share::SCN &data_version_barrier_scn)
{
  int ret = OB_SUCCESS;
  uint64_t current_data_version = 0;
  result = false;
  data_version_barrier_scn.set_min();
  SpinRLockGuard guard(lock_);

  if (!is_tenant_info_valid_()) {
    ret = OB_NEED_WAIT;
    const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_WARN("finish_data_version is invalid, need wait", KR(ret), K(*this));
    }
  } else if (is_data_version_crossed_) {
    result = true;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), current_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (current_data_version < finish_data_version_ || ERRSIM_DATA_VERSION_BARRIER_ERROR) {
    result = false;
    data_version_barrier_scn = data_version_barrier_scn_;
    LOG_INFO("local data version has not crossed", KR(ret), K(data_version_barrier_scn),
             "has_injected_error", ERRSIM_DATA_VERSION_BARRIER_ERROR);
  } else {
    result = true;
    is_data_version_crossed_ = true;
  }

  return ret;
}

int ObAllTenantInfoCache::get_tenant_info(share::ObAllTenantInfo &tenant_info,
                                          int64_t &last_sql_update_time,
                                          int64_t &ora_rowscn,
                                          uint64_t &finish_data_version,
                                          share::SCN &data_version_barrier_scn)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  last_sql_update_time = OB_INVALID_TIMESTAMP;
  ora_rowscn = 0;
  SpinRLockGuard guard(lock_);

  if (!is_tenant_info_valid_()) {
    ret = OB_NEED_WAIT;
    const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_WARN("tenant info is invalid, need wait", KR(ret), K(last_sql_update_time_),
               K(tenant_info_), K(ora_rowscn_), K(data_version_barrier_scn_));
    }
  } else {
    (void)tenant_info.assign(tenant_info_);
    last_sql_update_time = last_sql_update_time_;
    ora_rowscn = ora_rowscn_;
    finish_data_version = finish_data_version_;
    data_version_barrier_scn = data_version_barrier_scn_;
  }
  return ret;
}

int ObAllTenantInfoCache::get_tenant_info(
    share::ObAllTenantInfo &tenant_info,
    int64_t &last_sql_update_time,
    int64_t &ora_rowscn)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  last_sql_update_time = OB_INVALID_TIMESTAMP;
  ora_rowscn = 0;
  uint64_t finish_data_version = 0;
  share::SCN data_version_barrier_scn;

  if (OB_FAIL(get_tenant_info(tenant_info, last_sql_update_time, ora_rowscn, finish_data_version,
                              data_version_barrier_scn))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  }
  return ret;
}

int ObAllTenantInfoCache::get_tenant_info(share::ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  int64_t last_sql_update_time = OB_INVALID_TIMESTAMP;
  int64_t ora_rowscn = 0;
  uint64_t finish_data_version = 0;
  share::SCN data_version_barrier_scn;

  if (OB_FAIL(get_tenant_info(tenant_info, last_sql_update_time, ora_rowscn, finish_data_version,
                              data_version_barrier_scn))) {
    LOG_WARN("failed to get tenant info", KR(ret));
  }
  return ret;
}

}
}
