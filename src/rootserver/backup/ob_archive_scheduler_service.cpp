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
#include "rootserver/backup/ob_archive_scheduler_service.h"
#include "rootserver/backup/ob_tenant_archive_scheduler.h"
#include "share/ob_ddl_common.h"

using namespace oceanbase;
using namespace rootserver;
using namespace common;
using namespace share;
using namespace schema;
using namespace obrpc;

/**
 * ------------------------------ObArchiveSchedulerService---------------------
 */
ObArchiveSchedulerService::ObArchiveSchedulerService()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), rpc_proxy_(nullptr), sql_proxy_(nullptr), schema_service_(nullptr)
{
}

int ObArchiveSchedulerService::init()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = nullptr;
  obrpc::ObSrvRpcProxy *rpc_proxy = nullptr;
  share::schema::ObMultiVersionSchemaService *schema_service = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("archive scheduler init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not be NULL", K(ret), KP(sql_proxy));
  } else if (OB_ISNULL(rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy should not be NULL", K(ret), KP(rpc_proxy));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service should not be NULL", K(ret), KP(schema_service));
  } else if (OB_FAIL(create("ArchiveSvr", *this, ObWaitEventIds::BACKUP_ARCHIVE_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to create log archive thread", K(ret));
  } else {
    schema_service_ = schema_service;
    rpc_proxy_ = rpc_proxy;
    sql_proxy_ = sql_proxy;
    tenant_id_ = gen_user_tenant_id(MTL_ID());
    is_inited_ = true;
  }

  return ret;
}

void ObArchiveSchedulerService::run2()
{
  int tmp_ret = OB_SUCCESS;
  int64_t round = 0;
  ObArchiveRoundState round_state;

  FLOG_INFO("ObArchiveSchedulerService run start");
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR_RET(tmp_ret, "not init", K(tmp_ret));
  } else {
    while (true) {
      ++round;
      round_state.set_invalid();
      ObCurTraceId::init(GCONF.self_addr_);
      FLOG_INFO("start do ObArchiveSchedulerService round", K(round));
      if (has_set_stop()) {
        tmp_ret = OB_IN_STOP_STATE;
        LOG_WARN_RET(tmp_ret, "exit for stop state", K(tmp_ret));
        break;
      } else if (OB_SUCCESS != (tmp_ret = process_(round_state))) {
        LOG_WARN_RET(tmp_ret, "failed to do process", K(tmp_ret));
      }

      int64_t checkpoint_interval = 120 * 1000 * 1000L;
      set_checkpoint_interval_(checkpoint_interval, round_state);
      idle();
    }
  }
  FLOG_INFO("ObArchiveSchedulerService run finish");
}

int ObArchiveSchedulerService::force_cancel(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> archive_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive scheduler not init", K(ret));
  } else if (tenant_id != OB_SYS_TENANT_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only sys tenant can force cancel archive", K(ret));
  } else if (OB_FAIL(get_all_tenant_ids_(archive_tenant_ids))) {
    LOG_WARN("failed to get all tenant id", K(ret));
  } else if (OB_FAIL(stop_tenant_archive_(archive_tenant_ids, false /* force_stop */))) {
    LOG_WARN("failed to stop tenant archive", K(ret), K(archive_tenant_ids));
  }

  FLOG_WARN("force_cancel archive", K(ret));

  return ret;
}

int ObArchiveSchedulerService::start_archive(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> bak_archive_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive scheduler not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // If archive_tenant_ids is empty, then start archive for all tenants. Otherwise, just
    // start archive for these tenants in archive_tenant_ids.
    if (archive_tenant_ids.empty()) {
      // No tenants indicated, start archive all tenants, and ignore all error code.
      if (OB_FAIL(get_all_tenant_ids_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to get all tenant ids", K(ret), K(tenant_id));
      } else if (OB_FAIL(start_tenant_archive_(bak_archive_tenant_ids, false /* force_start */))) {
        LOG_WARN("failed to start archive all tenants", K(ret), K(bak_archive_tenant_ids));
      }
    } else {
      // Tenants are indicated, return the first error code which tenant failed to start archive.
      if (OB_FAIL(start_tenant_archive_(archive_tenant_ids, true /* force_start */))) {
        LOG_WARN("failed to start archive indicated tenants", K(ret), K(archive_tenant_ids));
      }
    }
  } else {
    // If start archive is lauched by normal tenant, then archive_tenant_ids must be empty.
    if (!archive_tenant_ids.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("normal tenant can only archive for itself.", K(ret), K(tenant_id), K(archive_tenant_ids));
    } else if (OB_FAIL(start_tenant_archive_(tenant_id))) {
      LOG_WARN("failed to start archive", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObArchiveSchedulerService::stop_archive(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> bak_archive_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive scheduler not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // If archive_tenant_ids is empty, then stop archive for all tenants. Otherwise, just
    // stop archive for these tenants in archive_tenant_ids.
    if (archive_tenant_ids.empty()) {
      // No tenants indicated, stop archive all tenants, and ignore all error code.
      if (OB_FAIL(get_all_tenant_ids_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to get all tenant ids", K(ret), K(tenant_id));
      } else if (OB_FAIL(stop_tenant_archive_(bak_archive_tenant_ids, false /* force_stop */))) {
        LOG_WARN("failed to stop archive all tenants", K(ret), K(bak_archive_tenant_ids));
      }
    } else {
      // Tenants are indicated, return the first error code which tenant failed to stop archive.
      if (OB_FAIL(stop_tenant_archive_(archive_tenant_ids, true /* force_stop */))) {
        LOG_WARN("failed to stop archive indicated tenants", K(ret), K(archive_tenant_ids));
      }
    }
  } else {
    // If stop archive is lauched by normal tenant, then archive_tenant_ids must be empty.
    if (!archive_tenant_ids.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("normal tenant can only archive for itself.", K(ret), K(tenant_id), K(archive_tenant_ids));
    } else if (OB_FAIL(stop_tenant_archive_(tenant_id))) {
      LOG_WARN("failed to stop archive", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObArchiveSchedulerService::start_tenant_archive_(const ObIArray<uint64_t> &tenant_ids_array, const bool force_start) 
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tenant_ids_array.count(); i++) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = start_tenant_archive_(tenant_ids_array.at(i)))) {
      // set first error code
      if (force_start && OB_SUCC(ret)) {
        ret = tmp_ret;
        LOG_WARN("failed to start archive, set first error code", K(ret));
      }
      LOG_WARN("failed to start archive", K(tmp_ret), K(i));
    } 
  }
  return ret;
}

int ObArchiveSchedulerService::stop_tenant_archive_(const ObIArray<uint64_t> &tenant_ids_array, const bool force_stop) 
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tenant_ids_array.count(); i++) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = stop_tenant_archive_(tenant_ids_array.at(i)))) {
      // set first error code
      if (force_stop && OB_SUCC(ret)) {
        ret = tmp_ret;
        LOG_WARN("failed to stop archive, set first error code", K(ret));
      }
      LOG_WARN("failed to stop archive", K(tmp_ret), K(i));
    } 
  }
  return ret;
}

int ObArchiveSchedulerService::start_tenant_archive_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArchiveHandler archive_handler;
  // Only one dest is supported.
  const int64_t dest_no = 0;
  if (OB_FAIL(archive_handler.init(tenant_id, schema_service_, *rpc_proxy_, *sql_proxy_))) {
    LOG_WARN("failed to init archive_handler", K(ret));
  } else if (OB_FAIL(archive_handler.enable_archive(dest_no))) {
    LOG_WARN("failed to enable archive tenant", K(ret), K(tenant_id), K(dest_no));
  } else {
    LOG_INFO("enable archive", K(tenant_id), K(dest_no));
  }

  return ret;
}

int ObArchiveSchedulerService::stop_tenant_archive_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArchiveHandler archive_handler;
  // Only one dest is supported.
  const int64_t dest_no = 0;
  if (OB_FAIL(archive_handler.init(tenant_id, schema_service_, *rpc_proxy_, *sql_proxy_))) {
    LOG_WARN("failed to init archive_handler", K(ret), K(tenant_id));
  } else if (OB_FAIL(archive_handler.disable_archive(dest_no))) {
    LOG_WARN("failed to disable tenant archive", K(ret), K(tenant_id), K(dest_no));
  } else {
    LOG_INFO("disable tenant archive", K(tenant_id), K(dest_no));
  }

  return ret;
}

int ObArchiveSchedulerService::process_(ObArchiveRoundState &round_state)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArchivePersistHelper archive_op;
  ObArchiveMode archive_mode;
  ObLogArchiveDestState dest_state;
  ObTenantArchiveRoundAttr round;
  const int64_t dest_no = 0;
  const bool lock = false;
  bool no_dest = false;
  bool no_round = false;
  round_state.set_invalid();

  ObArchiveHandler tenant_scheduler;
  const uint64_t tenant_id = tenant_id_;
  if (OB_FAIL(tenant_scheduler.init(tenant_id, schema_service_, *rpc_proxy_, *sql_proxy_))) {
    LOG_WARN("failed to init tenant archive scheduler", K(ret), K(tenant_id));
  } else if (OB_TMP_FAIL(tenant_scheduler.checkpoint())) {
    LOG_WARN("failed to checkpoint", K(tmp_ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(archive_op.init(tenant_id))) {
    LOG_WARN("failed to init archive_op", K(ret));
  } else if (OB_FAIL(archive_op.get_archive_mode(*sql_proxy_, archive_mode))) {
    LOG_WARN("failed to get archive mode", K(ret), K(tenant_id));
  } else if (!archive_mode.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("archive mode not valid", K(ret), K(tenant_id), K(archive_mode));
  } else if (OB_FAIL(archive_op.get_dest_state(*sql_proxy_, lock, dest_no, dest_state))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
       LOG_WARN("failed to get dest state", K(ret), K(tenant_id));
    } else {
      // no dest exist
      no_dest =  true;
      ret = OB_SUCCESS;
    }
  } else if (!dest_state.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest state not valid", K(ret), K(tenant_id), K(dest_state));
  } else if (OB_FAIL(archive_op.get_round(*sql_proxy_, dest_no, false, round))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
       LOG_WARN("failed to get round", K(ret), K(tenant_id));
    } else {
      // no round exist
      no_round =  true;
      ret = OB_SUCCESS;
    }
  } else {
    round_state = round.state_;
  }

  if (OB_FAIL(ret)) {
  } else if (no_dest) {
  } else if (archive_mode.is_noarchivelog()) {
    if (no_round || round.state_.is_stop() || round.state_.is_stopping()) {
    } else if (OB_FAIL(tenant_scheduler.disable_archive(dest_no))) {
      LOG_WARN("failed to disable archive", K(ret), K(tenant_id), K(dest_no), K(dest_state));
    } else {
      round_state.set_stopping();
    }
  } else if (dest_state.is_defer()) {
    if (no_round || round.state_.is_stop() || round.state_.is_suspend() || round.state_.is_suspending()) {
    } else if (OB_FAIL(tenant_scheduler.defer_archive(dest_no))) {
      LOG_WARN("failed to defer archive", K(ret), K(tenant_id), K(dest_no), K(dest_state));
    } else {
      round_state.set_suspending();
    }
  } else {
    // dest is enable
    if (no_round || round.state_.is_suspend() || round.state_.is_stop()) {
      if (OB_FAIL(tenant_scheduler.enable_archive(dest_no))) {
        LOG_WARN("failed to enable archive", K(ret), K(tenant_id), K(dest_no), K(dest_state));
      } else {
        round_state.set_beginning();
      }
    }
  }
  return ret;
}

int ObArchiveSchedulerService::get_all_tenant_ids_(common::ObIArray<uint64_t> &tenantid_array)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tmp_tenantid_array;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tmp_tenantid_array))) {
    LOG_WARN("failed to get tenant ids", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenantid_array.count(); ++i) {
    const uint64_t tenant_id = tmp_tenantid_array.at(i);
    const ObTenantSchema *tenant_info = nullptr;
    if (tenant_id < OB_USER_TENANT_ID) {
      // do nothing
    } else if (is_meta_tenant(tenant_id)) {
      // do nothing
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
      LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_info)) { //skip
      LOG_INFO("tenant schema is null, skip tenant which may has been dropped", K(tenant_id));
    } else if (tenant_info->is_restore()) {
      // skip restoring tenant
      LOG_INFO("skip tenant which is doing restore", K(tenant_id));
    } else if (tenant_info->is_creating()) {
      LOG_INFO("skip tenant which is creating", K(tenant_id));
    } else if (tenant_info->is_dropping()) {
      LOG_INFO("skip tenant which is dropping", K(tenant_id));
    } else if (tenant_info->is_in_recyclebin()) {
      LOG_INFO("skip tenant which is recyclebin", K(tenant_id));
    } else if (OB_FAIL(tenantid_array.push_back(tenant_id))) {
      LOG_WARN("failed to push back tenant id", K(ret), K(tenant_id));
    }
  }

  return ret;
}

void ObArchiveSchedulerService::set_checkpoint_interval_(const int64_t interval_us, const share::ObArchiveRoundState &round_state)
{
  int64_t max_idle_us = interval_us / 2 - RESERVED_FETCH_US;
  int64_t idle_time_us = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t lag_target = tenant_config.is_valid() ? tenant_config->archive_lag_target : 0L;

  if (interval_us <= 0) {
    idle_time_us = MAX_IDLE_INTERVAL_US;
  } else {
    if (max_idle_us <= MIN_IDLE_INTERVAL_US) {
      idle_time_us = max(MIN_IDLE_INTERVAL_US, lag_target / 2);
    } else if (max_idle_us > MAX_IDLE_INTERVAL_US) {
      idle_time_us = min(MAX_IDLE_INTERVAL_US, max(lag_target / 2, MIN_IDLE_INTERVAL_US));
    } else {
      idle_time_us = max_idle_us;
    }
  }

  if (idle_time_us > FAST_IDLE_INTERVAL_US
    && (round_state.is_prepare()
      || round_state.is_beginning()
      || round_state.is_suspending()
      || round_state.is_stopping())) {
    idle_time_us = FAST_IDLE_INTERVAL_US;
  }

  if (idle_time_us != get_idle_time()) {
    FLOG_INFO("change idle_time_us", "idle_time_ts_", get_idle_time(), K(idle_time_us));
    set_idle_time(idle_time_us);
  }
}

int ObArchiveSchedulerService::open_archive_mode(const uint64_t tenant_id, const common::ObIArray<uint64_t> &archive_tenant_ids)
{

  // TODO(wangxiaohui.wxh):4.3, print error trace to user
  int ret = OB_SUCCESS;
  ObArray<uint64_t> bak_archive_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive scheduler not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // If archive_tenant_ids is empty, then open archive mode for all tenants. Otherwise, just
    // open archive mode for these tenants in archive_tenant_ids.
    if (archive_tenant_ids.empty()) {
      // No tenants indicated, open archive mode for all tenants, and ignore all error code.
      if (OB_FAIL(get_all_tenant_ids_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to get all tenant ids", K(ret), K(tenant_id));
      } else if (OB_FAIL(open_tenant_archive_mode_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to open archive mode for all tenants", K(ret), K(bak_archive_tenant_ids));
      }
    } else {
      if (OB_FAIL(open_tenant_archive_mode_(archive_tenant_ids))) {
        LOG_WARN("failed to open archive mode for indicated tenants", K(ret), K(archive_tenant_ids));
      } else if (1 == archive_tenant_ids.count()) {
        notify_start_archive_(archive_tenant_ids.at(0));
      }
    }
  } else {
    // If open archive mode is lauched by normal tenant, then archive_tenant_ids must be empty.
    if (!archive_tenant_ids.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("normal tenant can only open archive mode for itself.", K(ret), K(tenant_id), K(archive_tenant_ids));
    } else if (OB_FAIL(open_tenant_archive_mode_(tenant_id))) {
      LOG_WARN("failed to open archive mode", K(ret), K(tenant_id));
    } else {
      notify_start_archive_(tenant_id);
    }
  }

  return ret;
}

int ObArchiveSchedulerService::open_tenant_archive_mode_(
    const common::ObIArray<uint64_t> &tenant_ids_array)
{

  // TODO(wangxiaohui.wxh):4.3, return failed if any tenant failed
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_array.count(); i++) {
    bool is_no_logging = false;
    const uint64_t &tenant_id = tenant_ids_array.at(i);
    if (OB_FAIL(ObDDLUtil::get_no_logging_param(tenant_id, is_no_logging))) {
      LOG_WARN("fail to check tenant no logging param", K(ret), K(tenant_id));
    } else if (is_no_logging) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("is not allow to set archive when in no logging mode", K(ret), K(tenant_id));
    }
  }

  for (int64_t i = 0; i < tenant_ids_array.count(); i++) {
    int tmp_ret = OB_SUCCESS;
    const uint64_t &tenant_id = tenant_ids_array.at(i);
    if (OB_TMP_FAIL(open_tenant_archive_mode_(tenant_id))) {
      LOG_WARN("failed to open archive mode", K(tmp_ret), K(i));
    }
  }
  return ret;
}

int ObArchiveSchedulerService::open_tenant_archive_mode_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArchiveHandler tenant_scheduler;
  bool is_no_logging = false;
  if (OB_FAIL(ObDDLUtil::get_no_logging_param(tenant_id, is_no_logging))) {
    LOG_WARN("fail to check tenant no logging param", K(ret), K(tenant_id));
  } else if (is_no_logging) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("is not allow to set archive when in no logging mode", K(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tenant_scheduler.init(tenant_id, schema_service_, *rpc_proxy_, *sql_proxy_))) {
    LOG_WARN("failed to init tenant archive scheduler", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_scheduler.open_archive_mode())) {
    LOG_WARN("failed to open archive mode", K(ret), K(tenant_id));
  }
  return ret;
}

int ObArchiveSchedulerService::close_archive_mode(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &archive_tenant_ids)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> bak_archive_tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("archive scheduler not init", K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // If archive_tenant_ids is empty, then close archive mode for all tenants. Otherwise, just
    // close archive mode for these tenants in archive_tenant_ids.
    if (archive_tenant_ids.empty()) {
      // No tenants indicated, close archive mode for all tenants, and ignore all error code.
      if (OB_FAIL(get_all_tenant_ids_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to get all tenant ids", K(ret), K(tenant_id));
      } else if (OB_FAIL(close_tenant_archive_mode_(bak_archive_tenant_ids))) {
        LOG_WARN("failed to close archive mode for all tenants", K(ret), K(bak_archive_tenant_ids));
      }
    } else {
      if (OB_FAIL(close_tenant_archive_mode_(archive_tenant_ids))) {
        LOG_WARN("failed to close archive mode for indicated tenants", K(ret), K(archive_tenant_ids));
      } else if (1 == archive_tenant_ids.count()) {
        notify_start_archive_(archive_tenant_ids.at(0));
      }
    }
  } else {
    // If close archive mode is lauched by normal tenant, then archive_tenant_ids must be empty.
    if (!archive_tenant_ids.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("normal tenant can only close archive mode for itself.", K(ret), K(tenant_id), K(archive_tenant_ids));
    } else if (OB_FAIL(close_tenant_archive_mode_(tenant_id))) {
      LOG_WARN("failed to close archive mode", K(ret), K(tenant_id));
    } else {
      notify_start_archive_(tenant_id);
    }
  }

  return ret;
}

int ObArchiveSchedulerService::close_tenant_archive_mode_(const common::ObIArray<uint64_t> &tenant_ids_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tenant_ids_array.count(); i++) {
    int tmp_ret = OB_SUCCESS;
    const uint64_t &tenant_id = tenant_ids_array.at(i);
    if (OB_FAIL(close_tenant_archive_mode_(tenant_id))) {
      LOG_WARN("failed to close archive mode", K(tmp_ret), K(i));
    }
  }
  return ret;
}

int ObArchiveSchedulerService::close_tenant_archive_mode_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArchiveHandler tenant_scheduler;
  if (OB_FAIL(tenant_scheduler.init(tenant_id, schema_service_, *rpc_proxy_, *sql_proxy_))) {
    LOG_WARN("failed to init tenant archive scheduler", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_scheduler.close_archive_mode())) {
    LOG_WARN("failed to close archive mode", K(ret), K(tenant_id));
  }
  return ret;
}

void ObArchiveSchedulerService::notify_start_archive_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObNotifyStartArchiveArg arg;
  arg.set_tenant_id(tenant_id);

  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy or location service is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
              GCONF.cluster_id, gen_meta_tenant_id(tenant_id), ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get meta tenant leader address", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id).notify_start_archive(arg))) {
    LOG_WARN("failed to notify tenant archive scheduler service", KR(ret), K(leader_addr), K(tenant_id), K(arg));
  }
}