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
#include "ob_recovery_ls_service.h"

#include "lib/thread/threads.h"               //set_run_wrapper
#include "lib/mysqlclient/ob_mysql_transaction.h"  //ObMySQLTransaction
#include "lib/profile/ob_trace_id.h"
#include "logservice/ob_log_base_header.h"          //ObLogBaseHeader
#include "logservice/ob_log_handler.h"              //ObLogHandler
#include "logservice/palf/log_entry.h"              //LogEntry
#include "logservice/palf/log_define.h"
#include "logservice/ob_log_service.h"
#include "share/scn.h"//SCN
#include "observer/ob_server_struct.h"              //GCTX
#include "share/ob_share_util.h"                           //ObShareUtil
#include "share/schema/ob_multi_version_schema_service.h"  //ObMultiSchemaService
#include "share/ob_standby_upgrade.h"  // ObStandbyUpgrade
#include "share/ob_upgrade_utils.h"  // ObUpgradeChecker
#include "share/ob_global_stat_proxy.h" // ObGlobalStatProxy
#include "share/ob_cluster_version.h"//GET_TENANT_DATA_VERSION
#include "rootserver/ob_ls_service_helper.h"//ObRecoveryLSHelper

namespace oceanbase
{
using namespace logservice;
using namespace transaction;
using namespace share;
using namespace storage;
using namespace palf;
namespace rootserver
{

int ObRecoveryLSService::mtl_init(ObRecoveryLSService*&ka)
{
  return ka->init();
}
int ObRecoveryLSService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("RecLSSer",
         lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    proxy_ = GCTX.sql_proxy_;
    inited_ = true;
  }

  return ret;
}

void ObRecoveryLSService::destroy()
{
  LOG_INFO("recovery ls service destory", KPC(this));
  ObTenantThreadHelper::destroy();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  proxy_ = NULL;
}

void ObRecoveryLSService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_), KP(proxy_));
  } else if (OB_FAIL(wait_tenant_data_version_ready_(tenant_id_, DATA_VERSION_4_1_0_0))) {
    LOG_WARN("failed to wait tenant and version ready", KR(ret), K(tenant_id_));
  } else {
    palf::PalfBufferIterator iterator;
    int64_t idle_time_us = 100 * 1000L;
    SCN start_scn;

    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      // two thread for seed log and recovery_ls_manager
      if (!is_user_tenant(tenant_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls recovery thread must run on user tenant", KR(ret), K(tenant_id_));
      } else {
        ObRecoveryLSHelper recovery_ls(tenant_id_, proxy_);
        if (OB_FAIL(recovery_ls.do_work(iterator, start_scn))) {
          LOG_WARN("failed to recovery ls", KR(ret), K(start_scn));
        }
        if (OB_ITER_STOP == ret) {
          //standby or recovery may restore finish,
          //so when the error code is OB_ITER_STOP, adjust idle_time to 10s
          idle_time_us = 10 * 1000 * 1000;//10s
        }
      }
      if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {  // every 10 second
        (void)try_tenant_upgrade_end_();
      }
      LOG_INFO("[RECOVERY_LS_SERVICE] finish one round", KR(ret),
               K(start_scn));
      idle(idle_time_us);
    }
  }
}

int ObRecoveryLSService::get_min_data_version_(uint64_t &compatible)
{
  int ret = OB_SUCCESS;
  compatible = 0;
  if (OB_ISNULL(proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt("select value from %s where tenant_id = '%lu' and name = 'compatible' ",
                                 OB_TENANT_PARAMETER_TNAME, tenant_id_))) {
        LOG_WARN("fail to generate sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(proxy_->read(result, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("read config from __tenant_parameter failed",
                KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config result is null", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        LOG_WARN("get result next failed", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
      } else {
        ObString compatible_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result.get_result(), "compatible", compatible_str);

        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get result", KR(ret), K_(tenant_id), K(exec_tenant_id), K(sql));
        } else if (OB_FAIL(ObClusterVersion::get_version(compatible_str, compatible))) {
          LOG_WARN("parse version failed", KR(ret), K(compatible_str));
        }
      }
    }
  }
  return ret;
}

void ObRecoveryLSService::try_tenant_upgrade_end_()
{
  int ret = OB_SUCCESS;
  uint64_t min_data_version = 0;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_ = GCTX.rs_rpc_proxy_;
  if (OB_ISNULL(proxy_) || !is_user_tenant(tenant_id_) || OB_ISNULL(rs_rpc_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", KR(ret), KP(proxy_), K_(tenant_id), KP(rs_rpc_proxy_));
  } else if (OB_FAIL(get_min_data_version_(min_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
  } else if (min_data_version == DATA_CURRENT_VERSION) {
    // already upgrade end
  } else {
    ObGlobalStatProxy proxy(*proxy_, tenant_id_);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    if (OB_FAIL(proxy.get_target_data_version(false /* for_update */, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K_(tenant_id));
    } else if (!(target_data_version == current_data_version
              && target_data_version != min_data_version
              && min_data_version <= DATA_CURRENT_VERSION)) {
      ret = EAGAIN;
      LOG_WARN("data_version not match, run upgrade end later",
               KR(ret), K_(tenant_id), K(target_data_version), K(current_data_version),
               K(DATA_CURRENT_VERSION));
    } else {
      HEAP_VAR(obrpc::ObAdminSetConfigItem, item) {
      ObSchemaGetterGuard guard;
      const ObSimpleTenantSchema *tenant = NULL;
      obrpc::ObAdminSetConfigArg arg;
      item.exec_tenant_id_ = OB_SYS_TENANT_ID;
      const int64_t timeout = GCONF.internal_sql_execute_timeout;
      int64_t pos = ObClusterVersion::print_version_str(
                    item.value_.ptr(), item.value_.capacity(),
                    current_data_version);
      if (pos <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("current_data_version is invalid",
                  KR(ret), K_(tenant_id), K(current_data_version));
      } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("fail to get schema guard", KR(ret));
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant))) {
        LOG_WARN("fail to get tenant info", KR(ret), K_(tenant_id));
      } else if (OB_ISNULL(tenant)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(item.tenant_name_.assign(tenant->get_tenant_name()))) {
        LOG_WARN("fail to assign tenant name", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(item.name_.assign("compatible"))) {
        LOG_WARN("fail to assign config name", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(arg.items_.push_back(item))) {
        LOG_WARN("fail to push back item", KR(ret), K(item));
      } else if (OB_FAIL(rs_rpc_proxy_->timeout(timeout).admin_set_config(arg))) {
        LOG_WARN("fail to set config", KR(ret), K(arg), K(timeout));
      }
      } // end HEAP_VAR
    }
  }
}

}//end of namespace rootserver
}//end of namespace oceanbase


