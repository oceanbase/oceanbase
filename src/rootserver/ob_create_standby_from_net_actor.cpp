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
#include "ob_create_standby_from_net_actor.h"
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "observer/ob_server_struct.h"        // GCTX
#include "observer/ob_service.h" // ObService
#include "share/rc/ob_tenant_base.h"    // MTL_ID
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "share/ob_rpc_struct.h" // ObCreateTenantEndArg
#include "rootserver/restore/ob_restore_scheduler.h" //reset_schema_status
#include "rootserver/ob_rs_async_rpc_proxy.h" // ObSwitchSchemaProxy
#include "share/ob_common_rpc_proxy.h" // create_tenant_end
#include "share/ob_schema_status_proxy.h"//ObSchemaStatusProxy
#include "share/ob_rpc_struct.h" // ObBroadcastSchemaArg
#include "share/schema/ob_multi_version_schema_service.h" // for GSCHEMASERVICE

#define STAT(level, fmt, args...) RS_LOG(level, "[NET_STANDBY_TNT_SERVICE] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define WSTAT(fmt, args...) STAT(WARN, fmt, ##args)
#define TSTAT(fmt, args...) STAT(TRACE, fmt, ##args)

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{

int ObCreateStandbyFromNetActor::set_idle_interval_us_(const int64_t idle_time)
{
  int ret = OB_SUCCESS;
  if (idle_time <= 0 || idle_time > MAX_IDLE_TIME) {
    ret = OB_INVALID_ARGUMENT;
    WSTAT("invalid idle_time", KR(ret), K(idle_time));
  } else {
    (void)ATOMIC_STORE(&idle_time_, idle_time);
  }
  return ret;
}


int ObCreateStandbyFromNetActor::init()
{
  int ret = OB_SUCCESS;
  sql_proxy_ = GCTX.sql_proxy_;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    WSTAT("init twice", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("NetStandbyCT",
         lib::TGDefIDs::ObCreateStandbyFromNetActor, *this))) {
    WSTAT("failed to create NET_STANDBY_TNT_SERVICE", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    WSTAT("failed to start NET_STANDBY_TNT_SERVICE", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    schema_broadcasted_ = false;
    is_inited_ = true;
  }

  return ret;
}

void ObCreateStandbyFromNetActor::do_work()
{
  ISTAT("create standby from net actor start");
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    WSTAT("inner stat error", KR(ret), K_(is_inited));
  } else {
    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      if (OB_FAIL(do_creating_standby_tenant())) {
        WSTAT("create standby from net actor failed", KR(ret));
      }

      ISTAT("finish one round", KR(ret));
      idle(get_idle_interval_us_());
    }
  }
}

void ObCreateStandbyFromNetActor::destroy()
{
  ISTAT("create standby from net actor destory", KPC(this));
  ObTenantThreadHelper::destroy();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
  schema_broadcasted_ = false;
}

int ObCreateStandbyFromNetActor::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    WSTAT("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("Member variables is NULL", KR(ret), KP(sql_proxy_));
  } else if (!is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("shoule use meta tenant", K(tenant_id_));
  }
  return ret;
}

int ObCreateStandbyFromNetActor::check_has_user_ls(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy, bool &has_user_ls)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator status_op;
  ObLSStatusInfoArray ls_array;
  has_user_ls = false;
  if (!is_user_tenant(tenant_id) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    WSTAT("invalid argument", K(tenant_id), KP(sql_proxy));
  } else if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array, *sql_proxy))) {
    WSTAT("failed to get all ls status", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; !has_user_ls && OB_SUCC(ret) && i < ls_array.count(); ++i) {
    const ObLSStatusInfo &info = ls_array.at(i);
    if (info.is_user_ls()) {
      has_user_ls = true;
    }
  }// end for
  return ret;
}

int ObCreateStandbyFromNetActor::finish_restore_if_possible_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSRecoveryStat recovery_stat;
  ObLSRecoveryStatOperator ls_recovery_operator;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  obrpc::ObCreateTenantEndArg arg;
  arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
  arg.tenant_id_ = tenant_id_;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy = GCTX.rs_rpc_proxy_;
  SCN min_user_ls_create_scn = SCN::base_scn();
  SCN readable_scn = SCN::base_scn();

  if (OB_ISNULL(rs_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("pointer is null", KP(rs_rpc_proxy));
  } else if (OB_FAIL(check_inner_stat_())) {
    WSTAT("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_));
  } else if (OB_FAIL(ls_recovery_operator.get_tenant_min_user_ls_create_scn(tenant_id_, *sql_proxy_,
                                                                        min_user_ls_create_scn))) {
    WSTAT("failed to get tenant min_user_ls_create_scn", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("tenant info loader should not be NULL", KR(ret), KP(tenant_info_loader));
  } else {
    ISTAT("start to wait whether can finish restore", K_(tenant_id), K(min_user_ls_create_scn));
    DEBUG_SYNC(BLOCK_CREATE_STANDBY_TENANT_END);

    int64_t retry_cnt_after_sync_user_ls = 0;
    // wait 1 minute, sleep 1s and retry 60 times
    for (int64_t retry_cnt = 60; OB_SUCC(ret) && retry_cnt > 0 && !has_set_stop(); --retry_cnt) {
      bool is_dropped = false;
      if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(tenant_id_, is_dropped))) {
        LOG_WARN("tenant has been dropped", KR(ret), K_(tenant_id));
      } else if (is_dropped) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("tenant has been dropped", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(tenant_info_loader->get_readable_scn(readable_scn))) {
        WSTAT("failed to get readable_scn", KR(ret));
      } else if (readable_scn >= min_user_ls_create_scn) {
        retry_cnt_after_sync_user_ls++;
        ISTAT("tenant readable scn can read inner table", K(readable_scn), K(min_user_ls_create_scn),
                                                          K(retry_cnt_after_sync_user_ls));

        bool is_refreshed = false;
        if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_schema_has_been_refreshed(tenant_id_, is_refreshed))) {
          LOG_WARN("fail to check tenant schema has been refreshed", KR(ret), K_(tenant_id), K(retry_cnt_after_sync_user_ls));
        } else if (!is_refreshed || !schema_broadcasted_) {
          if (50 < retry_cnt_after_sync_user_ls) {
            WSTAT("schema has not refreshed", KR(ret), KR(tmp_ret), K(is_refreshed),
                                        K_(schema_broadcasted), K(retry_cnt_after_sync_user_ls));
          }
          if (OB_TMP_FAIL(refresh_schema_())) {
            WSTAT("failed to refresh schema", KR(ret), KR(tmp_ret), K(is_refreshed), K_(schema_broadcasted),
                                              K(retry_cnt_after_sync_user_ls));
          }
        } else if (OB_FAIL(rs_rpc_proxy->create_tenant_end(arg))) {
          WSTAT("fail to execute create tenant end", KR(ret), K_(tenant_id), K(arg), K(retry_cnt_after_sync_user_ls));
        } else {
          ISTAT("execute create_tenant_end", KR(ret), K_(tenant_id), K(arg), K(retry_cnt_after_sync_user_ls));
          break;
        }
      }
      usleep(1000 * 1000); // 1s
    }
  }

  ISTAT("finish_restore_if_possible", K(ret), K_(tenant_id), K(min_user_ls_create_scn), K(arg), K(readable_scn));

  return ret;
}

int ObCreateStandbyFromNetActor::refresh_schema_()
{
  int ret = OB_SUCCESS;
  observer::ObService *ob_service = GCTX.ob_service_;
  int tmp_ret = OB_SUCCESS;
  ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
  ObRefreshSchemaStatus refresh_schema_status;

  if (OB_ISNULL(ob_service) || OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("pointer is null", KP(ob_service), KP(schema_status_proxy));
  } else if (OB_FAIL(check_inner_stat_())) {
    WSTAT("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_));
  } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id_, refresh_schema_status))) {
    LOG_WARN("fail to get refresh schema status", KR(ret), K_(tenant_id));
  } else if (refresh_schema_status.snapshot_timestamp_ == 0) {
    if (OB_FAIL(ObRestoreScheduler::reset_schema_status(tenant_id_, sql_proxy_))) {
      WSTAT("failed to reset schema status", KR(ret), K_(tenant_id));
    }
  }

  if (OB_SUCC(ret) && !schema_broadcasted_) {
    obrpc::ObBroadcastSchemaArg arg;
    arg.tenant_id_ = tenant_id_;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pointer is null", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.rs_rpc_proxy_));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to_rs(*GCTX.rs_mgr_).broadcast_schema(arg))) {
      LOG_WARN("failed to broadcast schema", KR(ret), K(arg));
    } else {
      schema_broadcasted_ = true;
    }
  }

  ISTAT("refresh_schema finished", KR(ret), K_(tenant_id), K_(schema_broadcasted), K(refresh_schema_status));
  return ret;
}

int ObCreateStandbyFromNetActor::do_creating_standby_tenant()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  bool has_user_ls = false;
  if (OB_FAIL(check_inner_stat_())) {
    WSTAT("error unexpected", KR(ret), K(tenant_id_), KP(sql_proxy_));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    WSTAT("failed to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    WSTAT("failed to get tenant info", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    WSTAT("tenant_schema is null", KR(ret), K_(tenant_id));
  } else if (tenant_schema->is_creating_standby_tenant_status()) {
    if (OB_FAIL(set_idle_interval_us_(DEFAULT_IDLE_TIME))) {
      WSTAT("failed to set_idle_interval_us_", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(check_has_user_ls(tenant_id_, sql_proxy_, has_user_ls))) {
      WSTAT("failed to check_has_user_ls", KR(ret), K_(tenant_id), KP(sql_proxy_));
    } else if (!has_user_ls) {
      TSTAT("has not recover user ls, keep wait", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(finish_restore_if_possible_())) {
      WSTAT("failed to finish_restore_if_possible", KR(ret), K_(tenant_id));
    }
  } else {
    ISTAT("not creating standby status, do nothing", KR(ret), K(tenant_id_), K(tenant_schema));
    if (OB_FAIL(set_idle_interval_us_(MAX_IDLE_TIME))) {
      WSTAT("failed to set_idle_interval_us_", KR(ret), K(tenant_id_));
    }
  }
  return ret;
}

}
}

#undef STAT
#undef ISTAT
#undef WSTAT
#undef TSTAT
