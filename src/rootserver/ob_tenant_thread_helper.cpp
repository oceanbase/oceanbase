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
#include "ob_tenant_thread_helper.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_mgr.h"//TG
#include "share/ob_errno.h"
#include "share/schema/ob_schema_struct.h"//ObTenantInfo
#include "share/schema/ob_schema_service.h"//ObMultiSchemaService
#include "share/schema/ob_schema_getter_guard.h"//ObSchemaGetterGuard
#include "share/ob_share_util.h"//ObShareUtil
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "share/restore/ob_physical_restore_table_operator.h"//restore_job
#include "share/restore/ob_tenant_clone_table_operator.h" // clone_job
#include "share/restore/ob_physical_restore_info.h"//restore_info
#include "share/ob_primary_zone_util.h"//get_ls_primary_zone_priority
#include "observer/ob_server_struct.h"//GCTX
#include "share/ls/ob_ls_recovery_stat_operator.h" //ObLSRecoveryStatOperator
#include "logservice/ob_log_service.h"//get_palf_role
#include "src/logservice/palf_handle_guard.h"//palf_handle
#include "share/scn.h"//SCN

#include "ob_tenant_info_loader.h"  // ObTenantInfoLoader


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
using namespace lib;
namespace rootserver
{
//////////////ObTenantThreadHelper
int ObTenantThreadHelper::create(
    const char* thread_name, int tg_def_id, ObTenantThreadHelper &tenant_thread)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_created_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_ISNULL(thread_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("thread name is null", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_def_id, tg_id_))) {
    LOG_ERROR("create tg failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_ERROR("set thread runable fail", KR(ret));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::REENTRANT_THREAD_COND_WAIT))) {
    LOG_WARN("fail to init cond, ", KR(ret));
  } else {
    thread_name_ = thread_name;
    is_created_ = true;
    is_first_time_to_start_ = true;
  }
  return ret;
}

int ObTenantThreadHelper::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (is_first_time_to_start_) {
    if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("fail ed to start at first time", KR(ret), K(tg_id_), K(thread_name_));
    } else {
      is_first_time_to_start_ = false;
    }
  } else if (OB_FAIL(TG_REENTRANT_LOGICAL_START(tg_id_))) {
    LOG_WARN("failed to start", KR(ret));
  }
  LOG_INFO("[TENANT THREAD] thread start", KR(ret), K(tg_id_), K(thread_name_));
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SKIP_TENANT_THREAD_STOP);
void ObTenantThreadHelper::stop()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[TENANT THREAD] thread stop start", K(tg_id_), K(thread_name_));
  ret = ERRSIM_SKIP_TENANT_THREAD_STOP;
  if (OB_UNLIKELY(ERRSIM_SKIP_TENANT_THREAD_STOP)) {
    LOG_ERROR("[TENANT THREAD] skip tenant thread stop");
  } else if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_STOP(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread stop finish", K(tg_id_), K(thread_name_), KR(ret));
}

void ObTenantThreadHelper::wait()
{
  LOG_INFO("[TENANT THREAD] thread wait start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_REENTRANT_LOGICAL_WAIT(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread wait finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::mtl_thread_stop()
{
  LOG_INFO("[TENANT THREAD] thread stop start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("[TENANT THREAD] thread stop finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::mtl_thread_wait()
{
  LOG_INFO("[TENANT THREAD] thread wait start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
    is_first_time_to_start_ = true;
  }
  LOG_INFO("[TENANT THREAD] thread wait finish", K(tg_id_), K(thread_name_));
}
void ObTenantThreadHelper::destroy()
{
  LOG_INFO("[TENANT THREAD] thread destory start", K(tg_id_), K(thread_name_));
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    {
      ObThreadCondGuard guard(thread_cond_);
      thread_cond_.broadcast();
    }
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_created_ = false;
  is_first_time_to_start_ = true;
  LOG_INFO("[TENANT THREAD] thread destory finish", K(tg_id_), K(thread_name_));
}

void ObTenantThreadHelper::switch_to_follower_forcedly()
{
  stop();
}
int ObTenantThreadHelper::switch_to_leader()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[TENANT THREAD] thread start", K(tg_id_), K(thread_name_));
  if (OB_FAIL(start())) {
    LOG_WARN("failed to start thread", KR(ret));
  } else {
    ObThreadCondGuard guard(thread_cond_);
    if (OB_FAIL(thread_cond_.broadcast())) {
      LOG_WARN("failed to weakup thread cond", KR(ret));
    }
  }
  LOG_INFO("[TENANT THREAD] thread start finish", K(tg_id_), K(thread_name_));
  return ret;
}
int ObTenantThreadHelper::wait_tenant_data_version_ready_(
    const uint64_t tenant_id, const uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  bool is_ready = false;
  uint64_t tenant_data_version = 0;
  while (!is_ready && !has_set_stop()) {
    ret = OB_SUCCESS;

    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
    } else if (tenant_data_version < data_version) {
      ret = OB_NEED_WAIT;
      LOG_WARN("tenant version not target, need wait", KR(ret),
            K(tenant_data_version), K(data_version));
    } else {
      is_ready = true;
    }

    if (!is_ready) {
      idle(10 * 1000 * 1000);
    }
  }

  if (has_set_stop()) {
    LOG_WARN("thread has been stopped", K(is_ready), K(tenant_id));
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObTenantThreadHelper::wait_tenant_schema_and_version_ready_(
    const uint64_t tenant_id, const uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema ptr is null", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(wait_tenant_data_version_ready_(tenant_id, data_version))) {
    LOG_WARN("failed to wait tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else {
    bool is_ready = false;
    share::schema::ObTenantSchema tenant_schema;
    while (!is_ready && !has_set_stop()) {
      ret = OB_SUCCESS;
      if (OB_FAIL(get_tenant_schema(tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
      } else if (tenant_schema.is_creating()) {
        ret = OB_NEED_WAIT;
        LOG_WARN("tenant schema not ready, no need tenant balance", KR(ret), K(tenant_schema));
      } else {
        is_ready = true;
      }

      if (!is_ready) {
        idle(10 * 1000 *1000);
      }
    }

    if (has_set_stop()) {
      LOG_WARN("thread has been stopped", K(is_ready), K(tenant_id));
      ret = OB_IN_STOP_STATE;
    }
  }
  return ret;
}

void ObTenantThreadHelper::run1() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lib::set_thread_name(thread_name_);
    LOG_INFO("thread run", K(thread_name_));
    do_work();
  }
}
void ObTenantThreadHelper::idle(const int64_t idle_time_us)
{
  ObThreadCondGuard guard(thread_cond_);
  thread_cond_.wait_us(idle_time_us);
}

int ObTenantThreadHelper::get_tenant_schema(const uint64_t tenant_id,
  share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *cur_tenant_schema = NULL;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id,
          cur_tenant_schema))) {
    LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(cur_tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_schema.assign(*cur_tenant_schema))) {
    LOG_WARN("failed to get cur tenant schema", KR(ret), KP(cur_tenant_schema));
  }
  return ret;
}

int ObTenantThreadHelper::get_zone_priority(const ObZone &primary_zone,
                                 const share::schema::ObTenantSchema &tenant_schema,
                                 ObSqlString &primary_zone_str)
{
  int ret = OB_SUCCESS;
  primary_zone_str.reset();
  if (OB_UNLIKELY(!tenant_schema.is_valid() || primary_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone), K(tenant_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_ls_primary_zone_priority(primary_zone,
          tenant_schema, primary_zone_str))) {
    LOG_WARN("failed to get ls primary zone priority", KR(ret), K(primary_zone), K(tenant_schema));
  }
  LOG_DEBUG("get zone priority", KR(ret), K(primary_zone_str), K(tenant_schema));
  return ret;
}

//TODO meta tenant and user tenant maybe not in same observer
int ObTenantThreadHelper::check_can_do_recovery_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only user tenant need check recovery", KR(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      share::ObTenantRole::Role tenant_role = MTL_GET_TENANT_ROLE_CACHE();
      if (is_primary_tenant(tenant_role) || is_standby_tenant(tenant_role)) {
      } else if (is_restore_tenant(tenant_role)) {
        //need to check success to create init ls
        share::ObPhysicalRestoreTableOperator restore_table_operator;
        share::ObPhysicalRestoreJob job_info;
        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql can't null", K(ret), K(GCTX.sql_proxy_));
        } else if (OB_FAIL(restore_table_operator.init(GCTX.sql_proxy_, tenant_id, share::OBCG_STORAGE /*group_id*/))) {
          LOG_WARN("fail to init restore table operator", KR(ret), K(tenant_id));
        } else if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id,
                job_info))) {
          LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
        } else if (job_info.is_valid_status_to_recovery()) {
          //can do recovery
        } else {
          ret = OB_NEED_WAIT;
          LOG_WARN("restore tenant not valid to recovery", KR(ret), K(job_info));
        }
      } else if (is_clone_tenant(tenant_role)) {
        //need to check success to create init ls
        share::ObTenantCloneTableOperator clone_table_operator;
        ObArray<share::ObCloneJob> job_arr;
        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql can't null", KR(ret), K(GCTX.sql_proxy_));
        } else if (OB_FAIL(clone_table_operator.init(tenant_id, GCTX.sql_proxy_))) {
          LOG_WARN("fail to init clone table operator", KR(ret), K(tenant_id));
        } else if (OB_FAIL(clone_table_operator.get_all_clone_jobs(job_arr))) {
          LOG_WARN("fail to get clone job", KR(ret), K(tenant_id));
        } else if (job_arr.empty()) {
          ret = OB_NEED_WAIT;
          LOG_WARN("clone job is empty", KR(ret), K(tenant_id));
        } else if (job_arr.count()!=1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("clone job's count is unexpected", KR(ret), K(job_arr));
        } else if (job_arr.at(0).is_valid_status_allows_user_tenant_to_do_ls_recovery()) {
          //can do recovery
        } else {
          ret = OB_NEED_WAIT;
          LOG_WARN("clone tenant not valid to recovery", KR(ret), K(job_arr));
        }
      } else if (is_invalid_tenant(tenant_role)) {
        ret = OB_NEED_WAIT;
        LOG_WARN("tenant role not ready, need wait", KR(ret), K(tenant_role));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tenant role", KR(ret), K(tenant_role));
      }
    }
  }
  return ret;
}

void ObTenantThreadHelper::wakeup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObThreadCondGuard guard(thread_cond_);
    thread_cond_.broadcast();
  }
}

}//end of rootserver
}
