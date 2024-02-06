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

#include "observer/ob_server.h"
#include "rootserver/ob_root_service.h"

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_preload.h"
#include "lib/worker.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/file/file_directory_utils.h"
#include "lib/encrypt/ob_encrypted_helper.h"

#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_thread_mgr.h"
#include "common/ob_timeout_ctx.h"
#include "common/object/ob_object.h"
#include "share/ob_cluster_version.h"

#include "share/ob_define.h"
#include "share/ob_version.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_lease_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_config_helper.h"
#include "share/config/ob_config_manager.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_time_zone_info_manager.h"
#include "share/ob_server_status.h"
#include "share/ob_index_builder_util.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_upgrade_utils.h"
#include "share/deadlock/ob_deadlock_inner_table_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#ifdef OB_BUILD_ARBITRATION
#include "share/arbitration_service/ob_arbitration_service_utils.h" // ObArbitrationServiceUtils
#endif
#include "share/ob_max_id_fetcher.h" // ObMaxIdFetcher
#include "share/backup/ob_backup_config.h"
#include "share/backup/ob_backup_helper.h"
#include "share/scheduler/ob_sys_task_stat.h"

#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/dbms_job/ob_dbms_job_master.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_master.h"

#include "rootserver/ob_bootstrap.h"
#include "rootserver/ob_schema2ddl_sql.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_update_rs_list_task.h"
#include "rootserver/ob_resource_weight_parser.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/restore/ob_restore_util.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_vertical_partition_builder.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_constraint_task.h"
#include "storage/ob_file_system_router.h"
#include "storage/tx/ob_ts_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "rootserver/ob_cluster_event.h"        // CLUSTER_EVENT_ADD_CONTROL
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_global_context_operator.h"

#include "share/ls/ob_ls_table_operator.h"  // for ObLSTableOperator
#include "share/ls/ob_ls_status_operator.h"//ls_status_operator
#include "share/ob_max_id_fetcher.h" //ObMaxIdFetcher
#include "observer/ob_service.h"
#include "storage/ob_file_system_router.h"
#include "storage/ddl/ob_ddl_heart_beat_task.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "share/restore/ob_physical_restore_table_operator.h"//ObPhysicalRestoreTableOperator
#include "share/ob_cluster_event_history_table_operator.h"//CLUSTER_EVENT_INSTANCE
#include "share/scn.h"
#include "share/restore/ob_recover_table_util.h"
#include "rootserver/backup/ob_backup_proxy.h" //ObBackupServiceProxy
#include "logservice/palf_handle_guard.h"
#include "logservice/ob_log_service.h"
#include "rootserver/restore/ob_recover_table_initiator.h"
#include "rootserver/ob_heartbeat_service.h"

#include "parallel_ddl/ob_create_table_helper.h" // ObCreateTableHelper
#include "parallel_ddl/ob_create_view_helper.h"  // ObCreateViewHelper

namespace oceanbase
{

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace dbms_job;

namespace rootserver
{

#define PUSH_BACK_TO_ARRAY_AND_SET_RET(array, msg)                              \
  do {                                                                          \
    if (OB_FAIL(array.push_back(msg))) {                                        \
      LOG_WARN("push reason array error", KR(ret), K(array), K(msg));           \
    }                                                                           \
  } while(0)

ObRootService::ObStatusChangeCallback::ObStatusChangeCallback(ObRootService &root_service)
    : root_service_(root_service)
{
}

ObRootService::ObStatusChangeCallback::~ObStatusChangeCallback()
{
}

int ObRootService::ObStatusChangeCallback::wakeup_balancer()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service not init", K(ret));
  } else {
    root_service_.get_root_balancer().wakeup();
  }
  return ret;
}

int ObRootService::ObStatusChangeCallback::wakeup_daily_merger()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service not init", K(ret));
  } else {
    //root_service_.get_daily_merge_scheduler().wakeup();
  }
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_server_status_change(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(root_service_.submit_update_all_server_task(server))) {
    LOG_WARN("root service commit task failed", K(server), K(ret));
  }
  LOG_INFO("on_server_status_change finish", KR(ret), K(server));
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_start_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else if (OB_FAIL(root_service_.submit_start_server_task(server))) {
    LOG_WARN("fail to submit start server task", K(ret));
  }
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_stop_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else if (OB_FAIL(root_service_.submit_stop_server_task(server))) {
    LOG_WARN("fail to submit stop server task", K(ret));
  }
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_offline_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else if (OB_FAIL(root_service_.submit_offline_server_task(server))) {
    LOG_WARN("fail to submit stop server task", K(ret));
  }
  return ret;
}

int ObRootService::ObServerChangeCallback::on_server_change()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (OB_FAIL(root_service_.submit_update_all_server_config_task())) {
    LOG_WARN("root service commit task failed", K(ret));
  }
  return ret;
}

int ObRootService::ObStartStopServerTask::process()
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (!server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_));
  // still use server manager here, since this func. will be called only in version < 4.2
  } else if (OB_FAIL(root_service_.get_server_mgr().is_server_exist(server_, exist))) {
    LOG_WARN("fail to check server exist", KR(ret), K(server_));
  } else if (!exist) {
    // server does not exist, ignore
  } else {
    obrpc::ObAdminServerArg arg;
    if (OB_FAIL(arg.servers_.push_back(server_))) {
      LOG_WARN("fail to push back", K(ret), K(server_));
    } else if (start_) {
      if (OB_FAIL(root_service_.start_server(arg))) {
        LOG_WARN("fail to start server", K(ret), K(arg));
      }
    } else {
      if (OB_FAIL(root_service_.stop_server(arg))) {
        LOG_WARN("fail to stop server", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int64_t ObRootService::ObStartStopServerTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObRootService::ObStartStopServerTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObStartStopServerTask(root_service_, server_, start_);
  }
  return task;
}

int ObRootService::ObOfflineServerTask::process()
{
  int ret = OB_SUCCESS;
  ObRsListArg arg;
  ObLSInfo ls_info;
  const int64_t cluster_id = GCONF.cluster_id;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  if (!server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_));
  } else if (OB_FAIL(root_service_.get_lst_operator().get(
                                      cluster_id,
                                      tenant_id,
                                      SYS_LS,
                                      share::ObLSTable::DEFAULT_MODE,
                                      ls_info))) {
    LOG_WARN("fail to get", KR(ret));
  } else {
    arg.master_rs_ = GCONF.self_addr_;
    FOREACH_CNT_X(replica, ls_info.get_replicas(), OB_SUCCESS == ret) {
      if (replica->get_server() == GCONF.self_addr_
          || (replica->is_in_service()
          && ObReplicaTypeCheck::is_paxos_replica_V2(replica->get_replica_type()))) {
        if (OB_FAIL(arg.rs_list_.push_back(replica->get_server()))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(root_service_.get_rpc_proxy().to(server_).broadcast_rs_list(arg))) {
      LOG_DEBUG("fail to broadcast rs list", KR(ret));
    } else {
      LOG_INFO("broadcast rs list success", K(arg), K_(server));
    }
  }
  return ret;
}

int64_t ObRootService::ObOfflineServerTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObRootService::ObOfflineServerTask::deep_copy(
    char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObOfflineServerTask(root_service_, server_);
  }
  return task;
}

int ObRootService::ObMinorFreezeTask::process()
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  DEBUG_SYNC(BEFORE_DO_MINOR_FREEZE);
  if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rootservice address failed", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).timeout(GCONF.rpc_timeout)
                     .root_minor_freeze(arg_))) {
    LOG_WARN("minor freeze rpc failed", K(ret), K_(arg));
  } else {
    LOG_INFO("minor freeze rpc success", K(ret), K_(arg));
  }
  return ret;
}

int64_t ObRootService::ObMinorFreezeTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask *ObRootService::ObMinorFreezeTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObAsyncTask *task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new(buf) ObMinorFreezeTask(arg_);
  }
  return task;
}

////////////////////////////////////////////////////////////////

bool ObRsStatus::can_start_service() const
{
  bool bret = false;
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus rs_status = ATOMIC_LOAD(&rs_status_);
  if (status::INIT == rs_status) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::is_start() const
{
  bool bret = false;
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  if (status::STARTING == stat || status::IN_SERVICE == stat
      || status::FULL_SERVICE == stat || status::STARTED == stat) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::is_stopping() const
{
  bool bret = false;
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  if (status::STOPPING == stat) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::need_do_restart() const
{
  bool bret = false;
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  if (status::IN_SERVICE == stat) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::is_full_service() const
{
  SpinRLockGuard guard(lock_);
  bool bret = false;
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  if (status::FULL_SERVICE == stat || status::STARTED == stat) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::in_service() const
{
  bool bret = false;
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  if (status::IN_SERVICE == stat
      || status::FULL_SERVICE == stat
      || status::STARTED == stat) {
    bret = true;
  }
  return bret;
}

bool ObRsStatus::is_need_stop() const
{
  SpinRLockGuard guard(lock_);
  status::ObRootServiceStatus stat = ATOMIC_LOAD(&rs_status_);
  return status::NEED_STOP == stat;
}

status::ObRootServiceStatus ObRsStatus::get_rs_status() const
{
  SpinRLockGuard guard(lock_);
  return ATOMIC_LOAD(&rs_status_);
}

//RS need stop after leader revoke
int ObRsStatus::revoke_rs()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[ROOTSERVICE_NOTICE] try to revoke rs");
  SpinWLockGuard guard(lock_);
  if (status::IN_SERVICE == rs_status_ || status::FULL_SERVICE == rs_status_) {
    rs_status_ = status::NEED_STOP;
    FLOG_INFO("[ROOTSERVICE_NOTICE] rs_status is setted to need_stop", K_(rs_status));
  } else if (status::STOPPING != rs_status_) {
    rs_status_ = status::STOPPING;
    FLOG_INFO("[ROOTSERVICE_NOTICE] rs_status is setted to stopping", K_(rs_status));
  }
  return ret;
}

int ObRsStatus::try_set_stopping()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[ROOTSERVICE_NOTICE] try set rs_status to stopping");
  SpinWLockGuard guard(lock_);
  if (status::NEED_STOP == rs_status_) {
    rs_status_ = status::STOPPING;
    FLOG_INFO("[ROOTSERVICE_NOTICE] rs_status is setted to stopping");
  }
  return ret;
}

int ObRsStatus::set_rs_status(const status::ObRootServiceStatus status)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const char* new_status_str = NULL;
  const char* old_status_str = NULL;
  if (OB_FAIL(get_rs_status_str(status, new_status_str))) {
    FLOG_WARN("fail to get rs status", KR(ret), K(status));
  } else if (OB_FAIL(get_rs_status_str(rs_status_, old_status_str))) {
    FLOG_WARN("fail to get rs status", KR(ret), K(rs_status_));
  } else if (OB_ISNULL(new_status_str) || OB_ISNULL(old_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_WARN("error unexpect", KR(ret), K(new_status_str), K(old_status_str));
  }
  if (OB_SUCC(ret)) {
    switch(rs_status_) {
      case status::INIT:
        {
          if (status::STARTING == status
              || status::STOPPING == status) {
            //rs.stop() will be executed while obs exit
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::STARTING:
        {
          if (status::IN_SERVICE == status
              || status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::IN_SERVICE:
        {
          if (status::FULL_SERVICE == status
              || status::NEED_STOP == status
              || status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::FULL_SERVICE:
        {
          if (status::STARTED == status
              || status::NEED_STOP == status
              || status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::STARTED:
        {
          if (status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::NEED_STOP:
        {
          if (status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      case status::STOPPING:
        {
          if (status::INIT == status
              || status::STOPPING == status) {
            rs_status_ = status;
            FLOG_INFO("[ROOTSERVICE_NOTICE] success to set rs status",
                      K(new_status_str), K(old_status_str), K(rs_status_));
          } else {
            ret = OB_OP_NOT_ALLOW;
            FLOG_WARN("can't set rs status", KR(ret));
          }
          break;
        }
      default:
        ret = OB_ERR_UNEXPECTED;
        FLOG_WARN("invalid rs status", KR(ret), K(rs_status_));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObRootService::ObRootService()
: inited_(false), server_refreshed_(false),
    debug_(false),
    self_addr_(), config_(NULL), config_mgr_(NULL),
    rpc_proxy_(), common_proxy_(), sql_proxy_(), restore_ctx_(NULL), rs_mgr_(NULL),
    schema_service_(NULL), status_change_cb_(*this),
    server_change_callback_(*this),
    server_manager_(), hb_checker_(),
    server_checker_(),
    rs_list_change_cb_(*this),
    server_zone_op_service_(),
    root_minor_freeze_(),
    lst_operator_(NULL),
    zone_manager_(), ddl_service_(), unit_manager_(server_manager_, zone_manager_),
    root_balancer_(), empty_server_checker_(), lost_replica_checker_(), thread_checker_(),
    vtable_location_getter_(unit_manager_),
    addr_agent_(NULL), root_inspection_(),
    upgrade_executor_(),
    upgrade_storage_format_executor_(), create_inner_schema_executor_(),
    bootstrap_lock_(), broadcast_rs_list_lock_(ObLatchIds::RS_BROADCAST_LOCK),
    task_queue_(),
    inspect_task_queue_(),
    restart_task_(*this),
    refresh_server_task_(*this),
    check_server_task_(task_queue_, server_checker_),
    self_check_task_(*this),
    load_ddl_task_(*this),
    refresh_io_calibration_task_(*this),
    event_table_clear_task_(ROOTSERVICE_EVENT_INSTANCE,
                            SERVER_EVENT_INSTANCE,
                            DEALOCK_EVENT_INSTANCE,
                            task_queue_),
    inspector_task_(*this),
    purge_recyclebin_task_(*this),
    ddl_scheduler_(),
    snapshot_manager_(),
    core_meta_table_version_(0),
    update_rs_list_timer_task_(*this),
    update_all_server_config_task_(*this),
    baseline_schema_version_(0),
    start_service_time_(0),
    rs_status_(),
    fail_count_(0),
    schema_history_recycler_(),
#ifdef OB_BUILD_TDE_SECURITY
    master_key_mgr_(),
#endif
    disaster_recovery_task_executor_(),
    disaster_recovery_task_mgr_(),
    global_ctx_task_(*this)
{
}

ObRootService::~ObRootService()
{
  if (inited_) {
    destroy();
  }
}

int ObRootService::fake_init(ObServerConfig &config,
                             ObConfigManager &config_mgr,
                             ObSrvRpcProxy &srv_rpc_proxy,
                             ObCommonRpcProxy &common_proxy,
                             ObAddr &self,
                             ObMySQLProxy &sql_proxy,
                             ObRsMgr &rs_mgr,
                             ObMultiVersionSchemaService *schema_service,
                             ObLSTableOperator &lst_operator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rootservice already inited", K(ret));
  } else if (!self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid self address", K(self), K(ret));
  } else if (NULL == schema_service) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service must not null", KP(schema_service), K(ret));
  } else {
    LOG_INFO("start to init rootservice");
    config_ = &config;
    config_mgr_ = &config_mgr;

    rpc_proxy_ = srv_rpc_proxy;
    common_proxy_ = common_proxy;

    const bool rpc_active = false;
    common_proxy_.active(rpc_active);
    rpc_proxy_.active(rpc_active);

    self_addr_ = self;

    sql_proxy_.assign(sql_proxy);
    sql_proxy_.set_inactive();
    oracle_sql_proxy_.set_inactive();

    rs_mgr_ = &rs_mgr;
    addr_agent_ = &rs_mgr.get_addr_agent();
    schema_service_ = schema_service;
    lst_operator_ = &lst_operator;
  }

  // init server management related
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_manager_.init(
                status_change_cb_, server_change_callback_, sql_proxy_, unit_manager_, zone_manager_,
                config, self, rpc_proxy_))) {
      LOG_WARN("init server manager failed", K(ret));
    } else if (OB_FAIL(hb_checker_.init(server_manager_))) {
      LOG_WARN("init heartbeat checker failed", K(ret));
    } else if (OB_FAIL(server_checker_.init(server_manager_, self))) {
      LOG_WARN("init server checker failed", K(self), K(ret));
    }
  }

  // init ddl service
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_.init(rpc_proxy_, common_proxy_, sql_proxy_, *schema_service,
                                  lst_operator, zone_manager_, unit_manager_,
                                  snapshot_manager_))) {
      LOG_WARN("ddl_service_ init failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dbms_job::ObDBMSJobMaster::get_instance()
          .init(&sql_proxy_, schema_service_))) {
      LOG_WARN("failed to init ObDBMSJobMaster", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobMaster::get_instance()
          .init(&unit_manager_, &sql_proxy_, schema_service_))) {
      LOG_WARN("failed to init ObDBMSSchedJobMaster", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  LOG_INFO("init rootservice", K(ret));
  return ret;
}
int ObRootService::init(ObServerConfig &config,
                        ObConfigManager &config_mgr,
                        ObSrvRpcProxy &srv_rpc_proxy,
                        ObCommonRpcProxy &common_proxy,
                        ObAddr &self,
                        ObMySQLProxy &sql_proxy,
                        observer::ObRestoreCtx &restore_ctx,
                        ObRsMgr &rs_mgr,
                        ObMultiVersionSchemaService *schema_service,
			                  ObLSTableOperator &lst_operator)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[ROOTSERVICE_NOTICE] begin to init rootservice");
  if (inited_) {
    ret = OB_INIT_TWICE;
    FLOG_WARN("rootservice already inited", KR(ret));
  } else if (!self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FLOG_WARN("invalid self address", K(self), KR(ret));
  } else if (NULL == schema_service) {
    ret = OB_INVALID_ARGUMENT;
    FLOG_WARN("schema_service must not null", KP(schema_service), KR(ret));
  } else {
    config_ = &config;
    config_mgr_ = &config_mgr;

    rpc_proxy_ = srv_rpc_proxy;
    common_proxy_ = common_proxy;

    const bool rpc_active = false;
    common_proxy_.active(rpc_active);
    rpc_proxy_.active(rpc_active);

    self_addr_ = self;

    restore_ctx_ = &restore_ctx;

    sql_proxy_.assign(sql_proxy);
    sql_proxy_.set_inactive();

    if (OB_FAIL(oracle_sql_proxy_.init(sql_proxy.get_pool()))) {
      FLOG_WARN("init oracle sql proxy failed", KR(ret));
    } else {
      oracle_sql_proxy_.set_inactive();
    }

    rs_mgr_ = &rs_mgr;
    addr_agent_ = &rs_mgr.get_addr_agent();
    schema_service_ = schema_service;
    lst_operator_ = &lst_operator;
  }

  // init inner queue
  if (FAILEDx(task_queue_.init(
              config_->rootservice_async_task_thread_count,
              config_->rootservice_async_task_queue_size,
              "RSAsyncTask"))) {
    FLOG_WARN("init inner queue failed",
             "thread_count", static_cast<int64_t>(config_->rootservice_async_task_thread_count),
             "queue_size", static_cast<int64_t>(config_->rootservice_async_task_queue_size), KR(ret));
  } else if (OB_FAIL(inspect_task_queue_.init(1/*only for the inspection of RS*/,
                                              config_->rootservice_async_task_queue_size,
                                              "RSInspectTask"))) {
    FLOG_WARN("init inner queue failed",
              "thread_count", 1,
              "queue_size", static_cast<int64_t>(config_->rootservice_async_task_queue_size), KR(ret));
  } else if (OB_FAIL(zone_manager_.init(sql_proxy_))) {
    // init zone manager
    FLOG_WARN("init zone manager failed", KR(ret));
  } else if (OB_FAIL(server_manager_.init(status_change_cb_, server_change_callback_, sql_proxy_,
                                          unit_manager_, zone_manager_, config, self, rpc_proxy_))) {
    // init server management related
    FLOG_WARN("init server manager failed", KR(ret));
  } else if (OB_FAIL(server_zone_op_service_.init(
      server_change_callback_,
      rpc_proxy_,
      lst_operator,
      unit_manager_,
      sql_proxy_
#ifdef OB_BUILD_TDE_SECURITY
      , &master_key_mgr_
#endif
      ))) {
    FLOG_WARN("init server zone op service failed", KR(ret));
  } else if (OB_FAIL(hb_checker_.init(server_manager_))) {
    FLOG_WARN("init heartbeat checker failed", KR(ret));
  } else if (OB_FAIL(server_checker_.init(server_manager_, self))) {
    FLOG_WARN("init server checker failed", KR(ret), K(self));
  } else if (OB_FAIL(root_minor_freeze_.init(rpc_proxy_, unit_manager_))) {
    // init root minor freeze
    FLOG_WARN("init root_minor_freeze_ failed", KR(ret));
  } else if (OB_FAIL(ddl_service_.init(rpc_proxy_, common_proxy_, sql_proxy_, *schema_service,
                                       lst_operator, zone_manager_, unit_manager_,
                                       snapshot_manager_))) {
    // init ddl service
    FLOG_WARN("init ddl_service_ failed", KR(ret));
  } else if (OB_FAIL(unit_manager_.init(sql_proxy_, *config_, rpc_proxy_, *schema_service,
                                        root_balancer_))) {
    // init unit manager
    FLOG_WARN("init unit_manager failed", KR(ret));
  } else if (OB_FAIL(snapshot_manager_.init(self_addr_))) {
    FLOG_WARN("init snapshot manager failed", KR(ret));
  } else if (OB_FAIL(root_inspection_.init(*schema_service_, zone_manager_, sql_proxy_, &common_proxy_))) {
    FLOG_WARN("init root inspection failed", KR(ret));
  } else if (OB_FAIL(upgrade_executor_.init(*schema_service_,
             root_inspection_, sql_proxy_, rpc_proxy_, common_proxy_))) {
    FLOG_WARN("init upgrade_executor failed", KR(ret));
  } else if (OB_FAIL(upgrade_storage_format_executor_.init(*this, ddl_service_))) {
    FLOG_WARN("init upgrade storage format executor failed", KR(ret));
  } else if (OB_FAIL(create_inner_schema_executor_.init(*schema_service_, sql_proxy_, common_proxy_))) {
    FLOG_WARN("init create inner role executor failed", KR(ret));
  } else if (OB_FAIL(thread_checker_.init())) {
    FLOG_WARN("init thread checker failed", KR(ret));
  } else if (OB_FAIL(empty_server_checker_.init(
      server_manager_,
      unit_manager_,
      *lst_operator_,
      *schema_service_,
      server_zone_op_service_))) {
    FLOG_WARN("init empty server checker failed", KR(ret));
  } else if (OB_FAIL(lost_replica_checker_.init(*lst_operator_, *schema_service_))) {
    FLOG_WARN("init empty server checker failed", KR(ret));
  } else if (OB_FAIL(root_balancer_.init(*config_, *schema_service_, unit_manager_,
                                           server_manager_, zone_manager_, rpc_proxy_,
                                           self_addr_, sql_proxy, disaster_recovery_task_mgr_))) {
    FLOG_WARN("init root balancer failed", KR(ret));
  } else if (OB_FAIL(ROOTSERVICE_EVENT_INSTANCE.init(sql_proxy, self_addr_))) {
    FLOG_WARN("init rootservice event history failed", KR(ret));
  } else if (OB_FAIL(THE_RS_JOB_TABLE.init(&sql_proxy, self_addr_))) {
    FLOG_WARN("init THE_RS_JOB_TABLE failed", KR(ret));
  } else if (OB_FAIL(ddl_scheduler_.init(this))) {
    FLOG_WARN("init ddl task scheduler failed", KR(ret));
  } else if (OB_FAIL(schema_history_recycler_.init(*schema_service_,
                                                   zone_manager_,
                                                   sql_proxy_))) {
    FLOG_WARN("fail to init schema history recycler failed", KR(ret));
  } else if (OB_FAIL(dbms_job::ObDBMSJobMaster::get_instance().init(&sql_proxy_,
                                                                    schema_service_))) {
    FLOG_WARN("init ObDBMSJobMaster failed", KR(ret));
  } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobMaster::get_instance().init(&unit_manager_,
                                                                               &sql_proxy_,
                                                                               schema_service_))) {
    FLOG_WARN("init ObDBMSSchedJobMaster failed", KR(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(master_key_mgr_.init(&zone_manager_, schema_service_))) {
    FLOG_WARN("init master key mgr failed", KR(ret));
#endif
  } else if (OB_FAIL(disaster_recovery_task_executor_.init(lst_operator,
                                                           rpc_proxy_))) {
    FLOG_WARN("init disaster recovery task executor failed", KR(ret));
  } else if (OB_FAIL(disaster_recovery_task_mgr_.init(self,
                                                      *config_,
                                                      disaster_recovery_task_executor_,
                                                      &rpc_proxy_,
                                                      &sql_proxy_,
                                                      schema_service_))) {
    FLOG_WARN("init disaster recovery task mgr failed", KR(ret));
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
    FLOG_INFO("[ROOTSERVICE_NOTICE] init rootservice success", KR(ret), K_(inited));
  } else {
    LOG_ERROR("[ROOTSERVICE_NOTICE] fail to init root service", KR(ret));
    LOG_DBA_ERROR(OB_ERR_ROOTSERVICE_START, "msg", "rootservice init() has failure", KR(ret));
  }

  return ret;
}

void ObRootService::destroy()
{
  int ret = OB_SUCCESS;
  int fail_ret = OB_SUCCESS;
  FLOG_INFO("[ROOTSERVICE_NOTICE] start to destroy rootservice");
  if (in_service()) {
    if (OB_FAIL(stop_service())) {
      FLOG_WARN("stop service failed", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    }
  }

  if (OB_FAIL(lost_replica_checker_.destroy())) {
    FLOG_WARN("lost replica checker failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("lost replica checker destroy");
  }

  // continue executing while error happen
  if (OB_FAIL(root_balancer_.destroy())) {
    FLOG_WARN("root balance destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("root balance destroy");
  }

  if (OB_FAIL(empty_server_checker_.destroy())) {
    FLOG_WARN("empty server checker destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("empty server checker destroy");
  }

  if (OB_FAIL(thread_checker_.destroy())) {
    FLOG_WARN("rs_monitor_check : thread checker destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("rs_monitor_check : thread checker destroy");
  }
  if (OB_FAIL(schema_history_recycler_.destroy())) {
    FLOG_WARN("schema history recycler destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("schema history recycler destroy");
  }

  task_queue_.destroy();
  FLOG_INFO("inner queue destroy");
  inspect_task_queue_.destroy();
  FLOG_INFO("inspect queue destroy");
  ddl_builder_.destroy();
  FLOG_INFO("ddl builder destroy");
  if (OB_FAIL(hb_checker_.destroy())) {
    FLOG_WARN("heartbeat checker destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("heartbeat checker destroy");
  }

  ROOTSERVICE_EVENT_INSTANCE.destroy();
  FLOG_INFO("event table operator destroy");

  dbms_job::ObDBMSJobMaster::get_instance().destroy();
  FLOG_INFO("ObDBMSJobMaster destroy");

  ddl_scheduler_.destroy();
  FLOG_INFO("ddl task scheduler destroy");

#ifdef OB_BUILD_TDE_SECURITY
  if (OB_FAIL(master_key_mgr_.destroy())) {
    FLOG_WARN("master key mgr destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("master key mgr destroy");
  }
#endif

  if (OB_FAIL(disaster_recovery_task_mgr_.destroy())) {
    FLOG_WARN("disaster recovery task mgr destroy failed", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("disaster recovery task mgr destroy");
  }

  dbms_scheduler::ObDBMSSchedJobMaster::get_instance().destroy();
  FLOG_INFO("ObDBMSSchedJobMaster destroy");
  TG_DESTROY(lib::TGDefIDs::GlobalCtxTimer);
  FLOG_INFO("global ctx timer destroyed");


  if (OB_SUCC(ret)) {
    if (inited_) {
      inited_ = false;
    }
  }

  FLOG_INFO("[ROOTSERVICE_NOTICE] destroy rootservice end", KR(ret));
  if (OB_SUCCESS != fail_ret) {
    LOG_DBA_WARN(OB_ERR_ROOTSERVICE_STOP, "msg", "rootservice destroy() has failure", KR(fail_ret));
  }
}

int ObRootService::start_service()
{
  int ret = OB_SUCCESS;
  start_service_time_ = ObTimeUtility::current_time();
  ROOTSERVICE_EVENT_ADD("root_service", "start_rootservice", K_(self_addr));
  FLOG_INFO("[ROOTSERVICE_NOTICE] start to start rootservice", K_(start_service_time));
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("rootservice not inited", KR(ret));
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STARTING))) {
    FLOG_WARN("fail to set rs status", KR(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver()) {
    ret = OB_NOT_MASTER;
    FLOG_WARN("not master", KR(ret));
  } else {
    sql_proxy_.set_active();
    oracle_sql_proxy_.set_active();
    const bool rpc_active = true;
    common_proxy_.active(rpc_active);
    rpc_proxy_.active(rpc_active);
    ddl_service_.restart();
    server_manager_.reset();
    zone_manager_.reset();
    OTC_MGR.reset_version_has_refreshed();

    if (OB_FAIL(hb_checker_.start())) {
      FLOG_WARN("hb checker start failed", KR(ret));
    } else if (OB_FAIL(task_queue_.start())) {
      FLOG_WARN("inner queue start failed", KR(ret));
    } else if (OB_FAIL(inspect_task_queue_.start())) {
      FLOG_WARN("inspect queue start failed", KR(ret));
    } else if (OB_FAIL(ddl_builder_.start())) {
      FLOG_WARN("start ddl builder failed", KR(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::GlobalCtxTimer))) {
      FLOG_WARN("init global ctx timer fail", KR(ret));
    } else if (OB_FAIL(global_ctx_task_.schedule(lib::TGDefIDs::GlobalCtxTimer))) {
      FLOG_WARN("failed to schedule global ctx task", KR(ret));
    } else if (OB_FAIL(lst_operator_->set_callback_for_rs(rs_list_change_cb_))) {
      FLOG_WARN("lst_operator set as rs leader failed", KR(ret));
    } else if (OB_FAIL(rs_status_.set_rs_status(status::IN_SERVICE))) {
      FLOG_WARN("fail to set rs status", KR(ret));
    } else if (OB_FAIL(schedule_refresh_server_timer_task(0))) {
      FLOG_WARN("failed to schedule refresh_server task", KR(ret));
    } else if (OB_FAIL(schedule_restart_timer_task(0))) {
      FLOG_WARN("failed to schedule restart task", KR(ret));
    } else if (OB_FAIL(schema_service_->get_ddl_epoch_mgr().remove_all_ddl_epoch())) {
      FLOG_WARN("fail to remove ddl epoch", KR(ret));
    } else if (debug_) {
      if (OB_FAIL(init_debug_database())) {
        FLOG_WARN("init_debug_database failed", KR(ret));
      }
    }
  }

  ROOTSERVICE_EVENT_ADD("root_service", "finish_start_rootservice",
                        "result", ret, K_(self_addr));

  if (OB_FAIL(ret)) {
    // increase fail count for self checker and print log.
    update_fail_count(ret);
    FLOG_WARN("start service failed, do stop service", KR(ret));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rs_status_.set_rs_status(status::STOPPING))) {
      FLOG_WARN("fail to set status", KR(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = stop_service())) {
      FLOG_WARN("stop service failed", KR(tmp_ret));
    }
  }

  FLOG_INFO("[ROOTSERVICE_NOTICE] rootservice start_service finished", KR(ret));
  return ret;
}

int ObRootService::stop_service()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[ROOTSERVICE_NOTICE] stop service begin");
  if (OB_FAIL(stop())) {
    FLOG_WARN("fail to stop thread", KR(ret));
  } else {
    wait();
  }
  if (FAILEDx(rs_status_.set_rs_status(status::INIT))) {
    FLOG_WARN("fail to set rs status", KR(ret));
  }
  FLOG_INFO("[ROOTSERVICE_NOTICE] stop service finished", KR(ret));
  return ret;
}

int ObRootService::stop()
{
  int ret = OB_SUCCESS;
  int fail_ret = OB_SUCCESS;
  start_service_time_ = 0;
  int64_t start_time = ObTimeUtility::current_time();
  ROOTSERVICE_EVENT_ADD("root_service", "stop_rootservice", K_(self_addr));
  FLOG_INFO("[ROOTSERVICE_NOTICE] start to stop rootservice", K(start_time));
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("rootservice not inited", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STOPPING))) {
    FLOG_WARN("fail to set rs status", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    // set to rpc ls table as soon as possible
    if (OB_FAIL(lst_operator_->set_callback_for_obs(
        common_proxy_,
        rpc_proxy_,
        *rs_mgr_,
        sql_proxy_))) {
      FLOG_WARN("set as rs follower failed", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("set old rs to follower finished");
    }
    //full_service_ = false;
    server_refreshed_ = false;
    //in_service_ = false;
    sql_proxy_.set_inactive();
    FLOG_INFO("sql_proxy set inactive finished");
    oracle_sql_proxy_.set_inactive();
    FLOG_INFO("oracle_sql_proxy set inactive finished");
    const bool rpc_active = false;
    common_proxy_.active(rpc_active);
    FLOG_INFO("commom_proxy set inactive finished");
    rpc_proxy_.active(rpc_active);
    FLOG_INFO("rpc_proxy set inactive finished");

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = upgrade_executor_.stop())) {
      FLOG_WARN("upgrade_executor stop failed", KR(tmp_ret));
      fail_ret = OB_SUCCESS == fail_ret ? tmp_ret : fail_ret;
    } else {
      FLOG_INFO("upgrade_executor stop finished");
    }
    if (OB_SUCCESS != (tmp_ret = upgrade_storage_format_executor_.stop())) {
      FLOG_WARN("fail to stop upgrade storage format executor", KR(tmp_ret));
      fail_ret = OB_SUCCESS == fail_ret ? tmp_ret : fail_ret;
    } else {
      FLOG_INFO("upgrade_storage_format_executor stop finished");
    }

    if (OB_SUCCESS != (tmp_ret = create_inner_schema_executor_.stop())) {
      FLOG_WARN("fail to stop create inner schema executor", KR(tmp_ret));
      fail_ret = OB_SUCCESS == fail_ret ? tmp_ret : fail_ret;
    } else {
      FLOG_INFO("create_inner_schema_executor stop finished");
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(stop_timer_tasks())) {
        FLOG_WARN("stop timer tasks failed", KR(ret));
        fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
      } else {
        FLOG_INFO("stop timer tasks success");
      }
    }

    if (OB_SUCC(ret)) {
      // ddl_service may be trying refresh schema, stop it
      ddl_service_.stop();
      FLOG_INFO("ddl service stop");
      root_minor_freeze_.stop();
      FLOG_INFO("minor freeze stop");
      root_inspection_.stop();
      FLOG_INFO("root inspection stop");
    }
    if (OB_SUCC(ret)) {
      ddl_builder_.stop();
      FLOG_INFO("ddl builder stop");
      task_queue_.stop();
      FLOG_INFO("task_queue stop");
      inspect_task_queue_.stop();
      FLOG_INFO("inspect queue stop");
      root_balancer_.stop();
      FLOG_INFO("root_balancer stop");
      empty_server_checker_.stop();
      FLOG_INFO("empty_server_checker stop");
      lost_replica_checker_.stop();
      FLOG_INFO("lost_replica_checker stop");
      thread_checker_.stop();
      FLOG_INFO("rs_monitor_check : thread_checker stop");
      schema_history_recycler_.stop();
      FLOG_INFO("schema_history_recycler stop");
      hb_checker_.stop();
      FLOG_INFO("hb_checker stop");
      ddl_scheduler_.stop();
      FLOG_INFO("ddl task scheduler stop");
      dbms_job::ObDBMSJobMaster::get_instance().stop();
      FLOG_INFO("dbms job master stop");
#ifdef OB_BUILD_TDE_SECURITY
      master_key_mgr_.stop();
      FLOG_INFO("master key mgr stop");
#endif
      disaster_recovery_task_mgr_.stop();
      FLOG_INFO("disaster_recovery_task_mgr stop");
      dbms_scheduler::ObDBMSSchedJobMaster::get_instance().stop();
      FLOG_INFO("dbms sched job master stop");
      TG_STOP(lib::TGDefIDs::GlobalCtxTimer);
      FLOG_INFO("global ctx timer stop");
    }
  }

  ROOTSERVICE_EVENT_ADD("root_service", "finish_stop_thread", KR(ret), K_(self_addr));
  FLOG_INFO("[ROOTSERVICE_NOTICE] finish stop rootservice", KR(ret));
  if (OB_SUCCESS != fail_ret) {
    LOG_DBA_WARN(OB_ERR_ROOTSERVICE_STOP, "msg", "rootservice stop() has failure", KR(fail_ret));
  }
  return ret;
}

void ObRootService::wait()
{
  FLOG_INFO("[ROOTSERVICE_NOTICE] wait rootservice begin");
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("start to wait all thread exit");
  root_balancer_.wait();
  FLOG_INFO("root balancer exit success");
  empty_server_checker_.wait();
  FLOG_INFO("empty server checker exit success");
  lost_replica_checker_.wait();
  FLOG_INFO("lost replica checker exit success");
  thread_checker_.wait();
  FLOG_INFO("rs_monitor_check : thread checker exit success");
  schema_history_recycler_.wait();
  FLOG_INFO("schema_history_recycler exit success");
  hb_checker_.wait();
  FLOG_INFO("hb checker exit success");
  task_queue_.wait();
  FLOG_INFO("task queue exit success");
  inspect_task_queue_.wait();
  FLOG_INFO("inspect queue exit success");
  ddl_scheduler_.wait();
  FLOG_INFO("ddl task scheduler exit success");
#ifdef OB_BUILD_TDE_SECURITY
  master_key_mgr_.wait();
  FLOG_INFO("master key mgr exit success");
#endif
  disaster_recovery_task_mgr_.wait();
  FLOG_INFO("rebalance task mgr exit success");
  TG_WAIT(lib::TGDefIDs::GlobalCtxTimer);
  FLOG_INFO("global ctx timer exit success");
  ddl_service_.get_index_name_checker().reset_all_cache();
  FLOG_INFO("reset index name checker success");
  ddl_service_.get_non_partitioned_tablet_allocator().reset_all_cache();
  FLOG_INFO("reset non partitioned tablet allocator success");
  ObUpdateRsListTask::clear_lock();
  THE_RS_JOB_TABLE.reset_max_job_id();
  int64_t cost = ObTimeUtility::current_time() - start_time;
  ROOTSERVICE_EVENT_ADD("root_service", "finish_wait_stop", K(cost));
  FLOG_INFO("[ROOTSERVICE_NOTICE] rootservice wait finished", K(start_time), K(cost));
  if (cost > 10 * 60 * 1000 * 1000L) { // 10min
    int ret = OB_ERROR;
    LOG_ERROR("cost too much time to wait rs stop", KR(ret), K(start_time), K(cost));
  }
}

int ObRootService::reload_config()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(addr_agent_->reload())) {
      LOG_WARN("root address agent reload failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_update_all_server_config_task()
{
  int ret = OB_SUCCESS;
  ObUpdateAllServerConfigTask task(*this);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("fail to add async task", K(ret));
  } else {
    LOG_INFO("ass async task for update all server config");
  }
  return ret;
}

int ObRootService::submit_ddl_single_replica_build_task(ObAsyncTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService has not been inited", K(ret));
  } else if (OB_FAIL(ddl_builder_.push_task(task))) {
    LOG_WARN("add task to ddl builder failed", K(ret));
  }
  return ret;
}

int ObRootService::submit_update_all_server_task(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else {
    const bool with_rootserver = (server == self_addr_);
    if (!ObHeartbeatService::is_service_enabled()) {
      ObAllServerTask task(server_manager_, disaster_recovery_task_mgr_, server, with_rootserver);
      if (OB_FAIL(task_queue_.add_async_task(task))) {
        LOG_WARN("inner queue push task failed", K(ret));
      }
    }
  }

  // FIXME: @wanhong.wwh: If self is RS and self status change, submit_update_rslist_task
  if (OB_SUCC(ret)) {
    if (!in_service()) {
      LOG_INFO("self is not RS, need not submit update rslist task in update_all_server_task",
          K(server));
    } else {
      LOG_INFO("self is RS and self status change, submit update rslist task", K(server));
      if (OB_FAIL(submit_update_rslist_task())) {
        LOG_WARN("submit update rslist task failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObRootService::submit_start_server_task(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else {
    const bool start = true;
    ObStartStopServerTask task(*this, server, start);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_stop_server_task(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else {
    const bool start = false;
    ObStartStopServerTask task(*this, server, start);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_offline_server_task(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(ret), K(server));
  } else {
    ObOfflineServerTask task(*this, server);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_upgrade_task(
    const obrpc::ObUpgradeJobArg &arg)
{
  int ret = OB_SUCCESS;
  ObUpgradeTask task(upgrade_executor_);
  task.set_retry_times(0); //not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(task.init(arg))) {
    LOG_WARN("task init failed", KR(ret), K(arg));
  } else if (OB_FAIL(upgrade_executor_.can_execute())) {
    LOG_WARN("can't run task now", KR(ret), K(arg));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit upgrade task fail", KR(ret), K(arg));
  } else {
    LOG_INFO("submit upgrade task success", KR(ret), K(arg));
  }
  return ret;
}

int ObRootService::submit_upgrade_storage_format_version_task()
{
  int ret = OB_SUCCESS;
  ObUpgradeStorageFormatVersionTask task(upgrade_storage_format_executor_);
  task.set_retry_times(0);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService has not been inited", K(ret));
  } else if (OB_FAIL(upgrade_storage_format_executor_.can_execute())) {
    LOG_WARN("cannot run task now", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit upgrade storage format version", K(ret));
  } else {
    LOG_INFO("submit upgrade storage format version success", K(ret), K(common::lbt()));
  }
  return ret;
}

int ObRootService::submit_create_inner_schema_task()
{
  int ret = OB_SUCCESS;
  ObCreateInnerSchemaTask task(create_inner_schema_executor_);
  task.set_retry_times(0);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService has not been inited", K(ret));
  } else if (OB_FAIL(create_inner_schema_executor_.can_execute())) {
    LOG_WARN("cannot run task now", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit create inner role task", K(ret));
  } else {
    LOG_INFO("submit create inner role task success", K(ret), K(common::lbt()));
  }
  return ret;
}

int ObRootService::schedule_check_server_timer_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObHeartbeatService::is_service_enabled()) {
    if (OB_FAIL(task_queue_.add_timer_task(check_server_task_, config_->server_check_interval, true))) {
      LOG_WARN("failed to add check_server task", K(ret));
    }
  } else {
    LOG_TRACE("no need to schedule ObCheckServerTask in version >= 4.2");
  }
  return ret;
}

int ObRootService::schedule_recyclebin_task(int64_t delay)
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;

  if (OB_FAIL(get_inspect_task_queue().add_timer_task(
              purge_recyclebin_task_, delay, did_repeat))) {
    if (OB_CANCELED != ret) {
      LOG_ERROR("schedule purge recyclebin task failed", KR(ret), K(delay), K(did_repeat));
    } else {
      LOG_WARN("schedule purge recyclebin task failed", KR(ret), K(delay), K(did_repeat));
    }
  }

  return ret;
}

int ObRootService::schedule_inspector_task()
{
  int ret = OB_SUCCESS;
  int64_t inspect_interval = ObInspector::INSPECT_INTERVAL;
  int64_t delay = 1 * 60 * 1000 * 1000;
  int64_t purge_interval = GCONF._recyclebin_object_purge_frequency;
  int64_t expire_time = GCONF.recyclebin_object_expire_time;
  if (purge_interval > 0 && expire_time > 0) {
    delay = purge_interval;
  }
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!inspect_task_queue_.exist_timer_task(inspector_task_)
             && OB_FAIL(inspect_task_queue_.add_timer_task(inspector_task_, inspect_interval, true))) {
    LOG_WARN("failed to add inspect task", KR(ret));
  } else if (!inspect_task_queue_.exist_timer_task(purge_recyclebin_task_)
             && OB_FAIL(inspect_task_queue_.add_timer_task(purge_recyclebin_task_,
                                                           delay, false))) {
    LOG_WARN("failed to add purge recyclebin task", KR(ret));
  } else {
    LOG_INFO("schedule inspector task", K(inspect_interval), K(purge_interval));
  }
  return ret;
}

int ObRootService::schedule_self_check_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;
  const int64_t delay = 5L * 1000L * 1000L; //5s
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(self_check_task_)) {
    // ignore error
    LOG_WARN("already have one self_check_task, ignore this");
  } else if (OB_FAIL(task_queue_.add_timer_task(self_check_task_, delay, did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("add self_check task success");
  }
  return ret;
}

int ObRootService::schedule_update_rs_list_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(update_rs_list_timer_task_)) {
    // ignore error
    LOG_WARN("already have one update rs list timer task , ignore this");
  } else if (OB_FAIL(task_queue_.add_timer_task(update_rs_list_timer_task_,
                                                ObUpdateRsListTimerTask::RETRY_INTERVAL,
                                                did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("add update rs list task success");
  }
  return ret;
}
ERRSIM_POINT_DEF(ALL_SERVER_SCHEDULE_ERROR);
int ObRootService::schedule_update_all_server_config_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (task_queue_.exist_timer_task(update_all_server_config_task_)) {
    // ignore error
    LOG_WARN("already have one update_all_server_config task , ignore this");
  } else if (OB_FAIL(task_queue_.add_timer_task(
      update_all_server_config_task_,
      ALL_SERVER_SCHEDULE_ERROR ? (ObUpdateAllServerConfigTask::RETRY_INTERVAL / 2) : ObUpdateAllServerConfigTask::RETRY_INTERVAL,
      did_repeat))) {
    LOG_WARN("fail to add timer task", KR(ret));
  } else {
    LOG_INFO("add update server config task success");
  }
  return ret;
}

int ObRootService::schedule_load_ddl_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;
  const int64_t delay = 5L * 1000L * 1000L; //5s
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(load_ddl_task_)) {
    // ignore error
    LOG_WARN("load ddl task already exist", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(load_ddl_task_, delay, did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("succeed to add load ddl task");
  }
  return ret;
}

int ObRootService::schedule_refresh_io_calibration_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;
  const int64_t delay = 5L * 1000L * 1000L; //5s
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(refresh_io_calibration_task_)) {
    // ignore error
    LOG_WARN("refresh io calibration task already exist", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(refresh_io_calibration_task_, delay, did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("succeed to add refresh io calibration task");
  }
  return ret;
}

int ObRootService::submit_update_rslist_task(const bool force_update)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (ObUpdateRsListTask::try_lock()) {
      bool task_added = false;
      ObUpdateRsListTask task;
      if (OB_FAIL(task.init(*lst_operator_, addr_agent_, zone_manager_,
                            broadcast_rs_list_lock_,
                            force_update, self_addr_))) {
        LOG_WARN("task init failed", KR(ret));
      } else if (OB_FAIL(task_queue_.add_async_task(task))) {
        LOG_WARN("inner queue push task failed", K(ret));
      } else {
        task_added = true;
        LOG_INFO("added async task to update rslist", K(force_update));
      }
      if (!task_added) {
        ObUpdateRsListTask::unlock();
      }
    } else {
      LOG_WARN("fail to submit update rslist task, need retry", K(force_update));
    }
  }
  return ret;
}

int ObRootService::submit_report_core_table_replica_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObReportCoreTableReplicaTask task(*this);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObRootService::submit_reload_unit_manager_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObReloadUnitManagerTask task(*this, unit_manager_);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push reload_unit task failed", K(ret));
    } else {
      LOG_INFO("submit reload unit task success", K(ret));
    }
  }
  return ret;
}

int ObRootService::schedule_restart_timer_task(const int64_t delay)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool did_repeat = false;
    if (OB_FAIL(task_queue_.add_timer_task(restart_task_,
                                           delay, did_repeat))) {
      LOG_WARN("schedule restart task failed", K(ret), K(delay), K(did_repeat));
    } else {
      LOG_INFO("submit restart task success", K(delay));
    }
  }
  return ret;
}

int ObRootService::schedule_refresh_server_timer_task(const int64_t delay)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool did_repeat = false;
    if (OB_FAIL(task_queue_.add_timer_task(refresh_server_task_,
                                           delay, did_repeat))) {
      LOG_WARN("schedule restart task failed", K(ret), K(delay), K(did_repeat));
    } else {
      LOG_INFO("schedule refresh server timer task", K(delay));
    }
  }
  return ret;
}

int ObRootService::update_rslist()
{
  int ret = OB_SUCCESS;
  ObUpdateRsListTask task;
  ObTimeoutCtx ctx;
  ctx.set_timeout(config_->rpc_timeout);
  const bool force_update = true;
  if (OB_FAIL(task.init(*lst_operator_, addr_agent_,
                        zone_manager_, broadcast_rs_list_lock_, force_update, self_addr_))) {
    LOG_WARN("task init failed", K(ret), K(force_update));
  } else if (OB_FAIL(task.process_without_lock())) {
    LOG_WARN("failed to update rslist", K(ret));
  } else {
    LOG_INFO("broadcast root address succeed");
  }
  return ret;
}

//only used in bootstrap
int ObRootService::update_all_server_and_rslist()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard rs_list_guard(broadcast_rs_list_lock_);
    ret = update_rslist();
    if (OB_FAIL(ret)) {
      LOG_INFO("fail to update rslist, ignore it", KR(ret));
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<ObAddr> servers;
    ObZone empty_zone; // empty zone for all servers
    if (OB_FAIL(SVR_TRACER.get_servers_of_zone(empty_zone, servers))) {
      LOG_WARN("get server list failed", K(ret));
    } else {
      FOREACH_X(s, servers, OB_SUCC(ret)) {
        const bool with_rootserver = (*s == self_addr_);
        ObAllServerTask task(server_manager_, disaster_recovery_task_mgr_, *s, with_rootserver);
        if (OB_FAIL(task.process())) {
          LOG_WARN("sync server status to __all_server table failed",
                   K(ret), "server", *s);
        }
      }
    }
  }
  return ret;
}

int ObRootService::request_heartbeats()
{
  int ret = OB_SUCCESS;
  ObServerManager::ObServerStatusArray statuses;
  ObZone zone; // empty zone means all zone
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_server_statuses(zone, statuses))) {
    LOG_WARN("get_server_statuses failed", K(zone), K(ret));
  } else {
    const int64_t rpc_timeout = 250 * 1000; // 250ms
    ObLeaseRequest lease_request;
    // should continue even some failed, so don't look at condition OB_SUCCESS == ret
    FOREACH_CNT(status, statuses) {
      if (ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED == status->hb_status_
          || (ObServerStatus::OB_SERVER_ADMIN_DELETING == status->admin_status_
              && !status->is_alive())) {
        uint64_t server_id = OB_INVALID_ID;
        lease_request.reset();
        int temp_ret = OB_SUCCESS;
        bool to_alive = false;
        if (OB_SUCCESS != (temp_ret = rpc_proxy_.to(status->server_).timeout(rpc_timeout)
                           .request_heartbeat(lease_request))) {
          LOG_WARN("request_heartbeat failed", "server", status->server_,
                   K(rpc_timeout), K(temp_ret));
        } else if (OB_SUCCESS != (temp_ret = server_manager_.receive_hb(
                    lease_request, server_id, to_alive))) {
          LOG_WARN("receive hb failed", K(lease_request), K(temp_ret));
        }
        ret = (OB_SUCCESS != ret) ? ret : temp_ret;
      }
    }
  }
  return ret;
}

int ObRootService::self_check()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!in_service()) {
    // nothing todo
  } else if (GCONF.in_upgrade_mode()) {
    // nothing todo
  } else if (OB_FAIL(root_inspection_.check_all())) {  //ignore failed
    LOG_WARN("root_inspection check_all failed", K(ret));
    if (OB_FAIL(schedule_self_check_task())) {
      if (OB_CANCELED != ret) {
        LOG_ERROR("fail to schedule self check task", K(ret));
      }
    }
  }
  return ret;
}

int ObRootService::after_restart()
{
  ObCurTraceId::init(GCONF.self_addr_);

  // avoid concurrent with bootstrap
  FLOG_INFO("[ROOTSERVICE_NOTICE] try to get lock for bootstrap in after_restart");
  ObLatchRGuard guard(bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);

  // NOTE: Following log print after lock
  FLOG_INFO("[ROOTSERVICE_NOTICE] start to do restart task");

  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("rootservice not init", KR(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver()) {
    ret = OB_NOT_MASTER;
    FLOG_WARN("not master", KR(ret));
  } else if (need_do_restart() && OB_FAIL(do_restart())) {
    FLOG_WARN("do restart failed, retry again", KR(ret));
  } else if (OB_FAIL(do_after_full_service())) {
    FLOG_WARN("fail to do after full service", KR(ret));
  }

  int64_t cost = ObTimeUtility::current_time() - start_service_time_;
  if (OB_FAIL(ret)) {
    FLOG_WARN("do restart task failed, retry again", KR(ret), K(cost));
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STARTED))) {
    FLOG_WARN("fail to set rs status", KR(ret));
  } else {
    FLOG_INFO("do restart task success, finish restart", KR(ret), K(cost), K_(start_service_time));
  }

  if (OB_FAIL(ret)) {
    rs_status_.try_set_stopping();
    if (rs_status_.is_stopping()) {
      // need stop
      FLOG_INFO("rs_status_ is set to stopping");
    } else {
      const int64_t RETRY_TIMES = 3;
      int64_t tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < RETRY_TIMES; ++i) {
        if (OB_SUCCESS != (tmp_ret = schedule_restart_timer_task(config_->rootservice_ready_check_interval))) {
          FLOG_WARN("fail to schedule_restart_timer_task at this retry", KR(tmp_ret), K(i));
        } else {
          FLOG_INFO("success to schedule_restart_timer_task");
          break;
        }
      }
      if (OB_SUCCESS != tmp_ret) {
        LOG_ERROR("fatal error, fail to add restart task", KR(tmp_ret));
        if (OB_FAIL(rs_status_.set_rs_status(status::STOPPING))) {
          LOG_ERROR("fail to set rs status", KR(ret));
        }
      }
    }
  }

  // NOTE: Following log print after lock
  FLOG_INFO("[ROOTSERVICE_NOTICE] finish do restart task", KR(ret));
  return ret;
}

int ObRootService::do_after_full_service() {
  int ret = OB_SUCCESS;
  ObGlobalStatProxy global_proxy(sql_proxy_, OB_SYS_TENANT_ID);
  if (OB_FAIL(global_proxy.get_baseline_schema_version(baseline_schema_version_))) {
    LOG_WARN("fail to get baseline schema version", KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schedule_self_check_task())) {
    LOG_WARN("fail to schedule self check task", K(ret));
  } else {
    LOG_INFO("schedule self check to root_inspection success");
  }

  // force broadcast rs list again to make sure rootserver list be updated
  if (OB_SUCC(ret)) {
    int tmp_ret = update_rslist();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("broadcast root address failed", K(tmp_ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObRootService::execute_bootstrap(const obrpc::ObBootstrapArg &arg)
{
  int ret = OB_SUCCESS;
  const obrpc::ObServerInfoList &server_list = arg.server_list_;
  BOOTSTRAP_LOG(INFO, "STEP_1.1:execute_bootstrap start to executor.");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service not inited", K(ret));
  } else if (!sql_proxy_.is_inited() || !sql_proxy_.is_active()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy not inited or not active", "sql_proxy inited",
             sql_proxy_.is_inited(), "sql_proxy active", sql_proxy_.is_active(), K(ret));
  } else if (server_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list is empty", K(server_list), K(ret));
  } else if (OB_UNLIKELY(nullptr == lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator_ ptr is null", KR(ret), KP(lst_operator_));
  } else {
    update_cpu_quota_concurrency_in_memory_();
    // avoid bootstrap and do_restart run concurrently
    FLOG_INFO("[ROOTSERVICE_NOTICE] try to get lock for bootstrap in execute_bootstrap");
    ObLatchWGuard guard(bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);
    FLOG_INFO("[ROOTSERVICE_NOTICE] success to get lock for bootstrap in execute_bootstrap");
    ObBootstrap bootstrap(rpc_proxy_, *lst_operator_, ddl_service_, unit_manager_,
                          *config_, arg, common_proxy_);
    if (OB_FAIL(bootstrap.execute_bootstrap(server_zone_op_service_))) {
      LOG_ERROR("failed to execute_bootstrap", K(server_list), K(ret));
    }

    BOOTSTRAP_LOG(INFO, "start to do_restart");
    ObGlobalStatProxy global_proxy(sql_proxy_, OB_SYS_TENANT_ID);
    ObArray<ObAddr> self_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_restart())) {
      LOG_WARN("do restart task failed", K(ret));
    } else if (OB_FAIL(check_ddl_allowed())) {
      LOG_WARN("fail to check ddl allowed", K(ret));
    } else if (OB_FAIL(update_all_server_and_rslist())) {
      LOG_WARN("failed to update all_server and rslist", K(ret));
    } else if (OB_FAIL(zone_manager_.reload())) {
      LOG_WARN("failed to reload zone manager", K(ret));
    } else if (OB_FAIL(set_cluster_version())) {
      LOG_WARN("set cluster version failed", K(ret));
    } else if (OB_FAIL(pl::ObPLPackageManager::load_all_sys_package(sql_proxy_))) {
      LOG_WARN("load all system package failed", K(ret));
    } else if (OB_FAIL(finish_bootstrap())) {
      LOG_WARN("failed to finish bootstrap", K(ret));
    } else if (OB_FAIL(update_baseline_schema_version())) {
      LOG_WARN("failed to update baseline schema version", K(ret));
    } else if (OB_FAIL(global_proxy.get_baseline_schema_version(
                       baseline_schema_version_))) {
      LOG_WARN("fail to get baseline schema version", KR(ret));
    } else if (OB_FAIL(set_cpu_quota_concurrency_config_())) {
      LOG_WARN("failed to update cpu_quota_concurrency", K(ret));
    }

    if (OB_SUCC(ret)) {
      char ori_min_server_version[OB_SERVER_VERSION_LENGTH] = {'\0'};
      uint64_t ori_cluster_version = GET_MIN_CLUSTER_VERSION();
      share::ObServerInfoInTable::ObBuildVersion build_version;
      if (OB_INVALID_INDEX == ObClusterVersion::print_version_str(
          ori_min_server_version, OB_SERVER_VERSION_LENGTH, ori_cluster_version)) {
         ret = OB_INVALID_ARGUMENT;
         LOG_WARN("fail to print version str", KR(ret), K(ori_cluster_version));
      } else if (OB_FAIL(observer::ObService::get_build_version(build_version))) {
        LOG_WARN("fail to get build version", KR(ret));
      } else {
        CLUSTER_EVENT_SYNC_ADD("BOOTSTRAP", "BOOTSTRAP_SUCCESS",
                               "cluster_version", ori_min_server_version,
                               "build_version", build_version.ptr());
      }
    }

    //clear bootstrap flag, regardless failure or success
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = clear_special_cluster_schema_status())) {
      LOG_WARN("failed to clear special cluster schema status",
                KR(ret), K(tmp_ret));
    }
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  BOOTSTRAP_LOG(INFO, "execute_bootstrap finished", K(ret));
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRootService::check_sys_tenant_initial_master_key_valid()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  const int64_t MAX_WAIT_US = 120L * 1000L * 1000L; //120s
  const int64_t end = start + MAX_WAIT_US;
  const int64_t IDLING_US = 100L * 1000L; // 100ms
  while (OB_SUCC(ret)) {
    if (ObTimeUtility::current_time() >= end) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait sys tenant initial master key valid timeout", KR(ret));
    } else {
      bool has_available_master_key = false;
      if (OB_FAIL(master_key_mgr_.check_if_tenant_has_available_master_keys(
              OB_SYS_TENANT_ID, has_available_master_key))) {
        LOG_WARN("fail to check if tenant has available master key", KR(ret));
      } else if (!has_available_master_key) {
        ob_usleep(std::min(IDLING_US, end - ObTimeUtility::current_time()));
      } else {
        break;
      }
    }
  }
  return ret;
}
#endif

int ObRootService::check_config_result(const char *name, const char* value)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  const uint64_t DEFAULT_WAIT_US = 120 * 1000 * 1000L; //120s
  int64_t timeout = DEFAULT_WAIT_US;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    timeout = MAX(DEFAULT_WAIT_US, THIS_WORKER.get_timeout_remain());
  }
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT count(*) as count FROM %s "
                               "WHERE name = '%s' and value != '%s'",
                               "__all_virtual_tenant_parameter_stat", name, value))) {
      LOG_WARN("fail to append sql", K(ret));
    }
    while(OB_SUCC(ret) || OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret /* remote schema not ready, return -4029 on remote */) {
      if (ObTimeUtility::current_time() - start > timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("sync config info use too much time", K(ret), K(name), K(value),
                 "cost_us", ObTimeUtility::current_time() - start);
      } else {
        if (OB_FAIL(sql_proxy_.read(res, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get result", K(ret));
        } else {
          int32_t count = OB_INVALID_COUNT;
          EXTRACT_INT_FIELD_MYSQL(*result, "count", count, int32_t);
          if (OB_SUCC(ret)) {
            if (count == 0) { break; }
          }
        }
      }
    } // while end
  }
  return ret;
}

// DDL exection depends on full_service & major_freeze_done state. the sequence of these two status in bootstrap is:
// 1.rs do_restart: major_freeze_launcher start
// 2.rs do_restart success: full_service is true
// 3.root_major_freeze success: major_freeze_done is true (need full_service is true)
// the success of do_restart does not mean to execute DDL, therefor, add wait to bootstrap, to avoid bootstrap failure cause by DDL failure
int ObRootService::check_ddl_allowed()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL_US = 1 * 1000 * 1000; //1s
  while (OB_SUCC(ret) && !is_ddl_allowed()) {
    if (!in_service() && !is_start()) {
      ret = OB_RS_SHUTDOWN;
      LOG_WARN("rs shutdown", K(ret));
    } else if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait too long", K(ret));
    } else {
      ob_usleep(SLEEP_INTERVAL_US);
    }
  }
  return ret;
}

// used by bootstrap
int ObRootService::update_baseline_schema_version()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("trans start failed", K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().
                     get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID,
                                                         baseline_schema_version))) {
    LOG_WARN("fail to get refreshed schema version", K(ret));
  } else {
    ObGlobalStatProxy proxy(trans, OB_SYS_TENANT_ID);
    if (OB_FAIL(proxy.set_baseline_schema_version(baseline_schema_version))) {
      LOG_WARN("set_baseline_schema_version failed", K(baseline_schema_version), K(ret));
    }
  }
  int temp_ret = OB_SUCCESS;
  if (!trans.is_started()) {
  } else if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
    LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, K(temp_ret));
    ret = (OB_SUCCESS == ret) ? temp_ret : ret;
  }
    LOG_DEBUG("update_baseline_schema_version finish", K(ret), K(temp_ret),
              K(baseline_schema_version));
  return ret;
}

int ObRootService::finish_bootstrap()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t tenant_id = OB_SYS_TENANT_ID;
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
    share::schema::ObSchemaService *tmp_schema_service = multi_schema_service.get_schema_service();
    if (OB_ISNULL(tmp_schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret), KP(tmp_schema_service));
    } else {
      ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
      share::schema::ObDDLSqlService ddl_sql_service(*tmp_schema_service);
      share::schema::ObSchemaOperation schema_operation;
      schema_operation.op_type_ = share::schema::OB_DDL_FINISH_BOOTSTRAP;
      schema_operation.tenant_id_ = tenant_id;
      if (OB_FAIL(multi_schema_service.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id), K(new_schema_version));
      } else if (OB_FAIL(ddl_sql_service.log_nop_operation(schema_operation,
                                                           new_schema_version,
                                                           schema_operation.ddl_stmt_str_,
                                                           sql_proxy))) {
        LOG_WARN("log finish bootstrap operation failed", K(ret), K(schema_operation));
      } else if (OB_FAIL(ddl_service_.refresh_schema(OB_SYS_TENANT_ID))) {
        LOG_WARN("failed to refresh_schema", K(ret));
      } else {
        LOG_INFO("finish bootstrap", K(ret), K(new_schema_version));
      }
    }
  }
  return ret;
}

void ObRootService::construct_lease_expire_time(
     const ObLeaseRequest &lease_request,
     share::ObLeaseResponse &lease_response,
     const share::ObServerStatus &server_status)
{
  UNUSED(lease_request);
  const int64_t now = ObTimeUtility::current_time();
  // if force_stop_hb is true,
  // then lease_expire_time_ won't be changed
  lease_response.heartbeat_expire_time_ = is_full_service() && server_status.force_stop_hb_
                                          ? server_status.last_hb_time_ + config_->lease_time
                                          : now + config_->lease_time;
  lease_response.lease_expire_time_ = lease_response.heartbeat_expire_time_;
}

int ObRootService::renew_lease(const ObLeaseRequest &lease_request, ObLeaseResponse &lease_response)
{
  int ret = OB_SUCCESS;
  ObServerStatus server_stat;
  uint64_t server_id = OB_INVALID_ID;
  bool to_alive = false;
  DEBUG_SYNC(HANG_HEART_BEAT_ON_RS);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request", K(lease_request), K(ret));
  } else if (OB_FAIL(server_manager_.receive_hb(lease_request, server_id, to_alive))) {
    LOG_WARN("server manager receive hb failed", K(lease_request), K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else {
    // before __all_zone load, it may fail, ignore it
    int temp_ret = OB_SUCCESS;
    int64_t lease_info_version = 0;
    bool is_stopped = false;
    lease_response.rs_server_status_ = RSS_INVALID;
    if (is_full_service()) {
      if (OB_FAIL(zone_manager_.get_lease_info_version(lease_info_version))) {
        LOG_WARN("get_lease_info_version failed", K(ret));
      } else if (OB_FAIL(server_manager_.get_server_status(
          lease_request.server_, server_stat))) {
        // get server_stat for construct_lease_expire_time only!
        LOG_WARN("get server status failed", K(ret), "server", lease_request.server_);
      }
      if (!ObHeartbeatService::is_service_enabled()) {
        if (FAILEDx(server_manager_.is_server_stopped(lease_request.server_, is_stopped))) {
          LOG_WARN("check_server_stopped failed", KR(ret), "server", lease_request.server_);
        } else {
          lease_response.rs_server_status_ = is_stopped ? RSS_IS_STOPPED : RSS_IS_WORKING;
        }
      }
#ifdef OB_BUILD_TDE_SECURITY
      if (OB_SUCCESS != (temp_ret = master_key_mgr_.input_server_master_key(
              lease_request.server_, lease_request.tenant_max_flushed_key_version_))) {
        LOG_WARN("fail to input server master key", KR(temp_ret), K(lease_request));
      }
#endif
    }
    if (OB_SUCC(ret)) {
      lease_response.version_ = ObLeaseResponse::LEASE_VERSION;
      construct_lease_expire_time(lease_request, lease_response, server_stat);
      lease_response.lease_info_version_ = lease_info_version;
      lease_response.server_id_ = server_id;
      lease_response.force_frozen_status_ = to_alive;
      lease_response.baseline_schema_version_ = baseline_schema_version_;
      (void)OTC_MGR.get_lease_response(lease_response);

      // after split schema, the schema_version is not used, but for the legality detection, set schema_version to sys's schema_version
      if (OB_SUCCESS != (temp_ret = schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, lease_response.schema_version_))) {
        LOG_WARN("fail to get tenant schema version", K(temp_ret));
      }

      if (OB_SUCCESS != (temp_ret = schema_service_->get_refresh_schema_info(
              lease_response.refresh_schema_info_))) {
        LOG_WARN("fail to get refresh_schema_info", K(temp_ret));
      }

#ifdef OB_BUILD_TDE_SECURITY
      if (OB_SUCCESS != (temp_ret = master_key_mgr_.get_all_tenant_master_key(
              lease_request.zone_,
              lease_response.tenant_max_key_version_))) {
        LOG_WARN("fail to get all tenant master key", KR(temp_ret),
                 "server", lease_request.server_, "zone", lease_request.zone_);
      }
#endif
      LOG_TRACE("lease_request", K(lease_request), K(lease_response));
    }
  }
  return ret;
}

int ObRootService::report_sys_ls(const share::ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  ObInMemoryLSTable *inmemory_ls = NULL;
  ObRole role = FOLLOWER;
  bool inner_table_only = false;
  LOG_INFO("receive request to report sys ls", K(replica));
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", KR(ret));
  } else if (OB_FAIL(lst_operator_->get_role(OB_SYS_TENANT_ID, SYS_LS, role))) {
    LOG_WARN("fail to get local role by lst_operator", KR(ret));
  } else if (OB_UNLIKELY(!is_strong_leader(role))) {
    ret = OB_RS_NOT_MASTER;
    LOG_WARN("local role is not leader", KR(ret), K(role));
  } else if (OB_UNLIKELY(!replica.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", KR(ret), K(replica));
  } else if (OB_ISNULL(inmemory_ls = lst_operator_->get_inmemory_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get inmemory ls", KR(ret), K(replica));
  } else if (OB_FAIL(inmemory_ls->update(replica, inner_table_only))) {
    LOG_WARN("update sys ls failed", KR(ret), K(replica));
  } else {
    LOG_INFO("update sys ls on rs success", K(replica));
  }
  return ret;
}

int ObRootService::remove_sys_ls(const obrpc::ObRemoveSysLsArg &arg)
{
  int ret = OB_SUCCESS;
  ObInMemoryLSTable *inmemory_ls = NULL;
  ObRole role = FOLLOWER;
  bool inner_table_only = false;
  LOG_INFO("receive request to remove sys ls", K(arg));
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", KR(ret));
  } else if (OB_FAIL(lst_operator_->get_role(OB_SYS_TENANT_ID, SYS_LS, role))) {
    LOG_WARN("fail to get local role by lst_operator", KR(ret));
  } else if (OB_UNLIKELY(!is_strong_leader(role))) {
    ret = OB_RS_NOT_MASTER;
    LOG_WARN("local role is not leader", KR(ret), K(role));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(arg));
  } else if (OB_ISNULL(inmemory_ls = lst_operator_->get_inmemory_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get inmemory ls", KR(ret), K(arg));
  } else if (OB_FAIL(inmemory_ls->remove(
      OB_SYS_TENANT_ID,
      SYS_LS,
      arg.server_,
      inner_table_only))) {
    LOG_WARN("remove sys ls failed", KR(ret), K(arg));
  } else {
    LOG_INFO("remove sys ls on rs success", K(arg));
  }
  return ret;
}

int ObRootService::fetch_location(
    const obrpc::ObFetchLocationArg &arg,
    obrpc::ObFetchLocationResult &res)
{
  int ret = OB_SUCCESS;
  const ObVtableLocationType &vtable_type = arg.get_vtable_type();
  ObSArray<ObAddr> servers;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(vtable_location_getter_.get(vtable_type, servers))) {
    LOG_WARN("vtable_location_getter get failed", KR(ret), K(arg));
  } else if (OB_FAIL(res.set_servers(servers))) {
    LOG_WARN("fail to assign servers", KR(ret), K(servers), K(arg));
  }
  return ret;
}

////////////////////////////////////////////////////////////////

int ObRootService::create_resource_unit(const obrpc::ObCreateResourceUnitArg &arg)
{
  int ret = OB_SUCCESS;
  const bool if_not_exist = arg.get_if_not_exist();
  LOG_INFO("receive create_resource_unit request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(unit_manager_.create_unit_config(arg.get_unit_config(), if_not_exist))) {
    LOG_WARN("create_unit_config failed", K(arg), K(if_not_exist), KR(ret));
    int mysql_error = -common::ob_mysql_errno(ret);
    if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
        if (OB_CANCELED != tmp_ret) {
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit', please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
  }

  LOG_INFO("finish create_resource_unit", K(arg), KR(ret));
  ROOTSERVICE_EVENT_ADD("root_service", "create_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_resource_unit(const obrpc::ObAlterResourceUnitArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("receive alter_resource_unit request", K(arg));
    if (OB_FAIL(unit_manager_.alter_unit_config(arg.get_unit_config()))) {
      LOG_WARN("alter_unit_config failed", K(arg), KR(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit', please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish alter_resource_unit", K(arg), KR(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "alter_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::drop_resource_unit(const obrpc::ObDropResourceUnitArg &arg)
{
  int ret = OB_SUCCESS;
  const bool if_exist = arg.if_exist_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    LOG_INFO("receive drop_resource_unit request", K(arg));
    if (OB_FAIL(unit_manager_.drop_unit_config(arg.unit_name_, if_exist))) {
      LOG_WARN("drop_unit_config failed", "unit_config", arg.unit_name_, K(if_exist), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish drop_resource_unit", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "drop_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::create_resource_pool(const obrpc::ObCreateResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  const bool if_not_exist = arg.if_not_exist_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (arg.pool_name_.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource pool name");
    } else if (arg.unit_.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "unit");
    } else if (arg.unit_num_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "unit_num");
    }
    LOG_WARN("missing arg to create resource pool", K(arg), K(ret));
  } else if (REPLICA_TYPE_LOGONLY != arg.replica_type_
             && REPLICA_TYPE_FULL != arg.replica_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only full/logonly pool are supported", K(ret), K(arg));
  } else if (REPLICA_TYPE_LOGONLY == arg.replica_type_
             && arg.unit_num_> 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("logonly resource pool should only have one unit on one zone", K(ret), K(arg));
  } else if (0 == arg.unit_.case_compare(OB_STANDBY_UNIT_CONFIG_TEMPLATE_NAME)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not create resource pool use standby unit config template", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "create resource pool use stanby unit config template");
  } else {
    LOG_INFO("receive create_resource_pool request", K(arg));
    share::ObResourcePool pool;
    pool.name_ = arg.pool_name_;
    pool.unit_count_ = arg.unit_num_;
    pool.replica_type_ = arg.replica_type_;
    if (OB_FAIL(pool.zone_list_.assign(arg.zone_list_))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(unit_manager_.create_resource_pool(pool, arg.unit_, if_not_exist))) {
      LOG_WARN("create_resource_pool failed", K(pool),
               "unit_config", arg.unit_, K(if_not_exist), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish create_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "create_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::split_resource_pool(const obrpc::ObSplitResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    LOG_INFO("receive split resource pool request", K(arg));
    share::ObResourcePoolName pool_name = arg.pool_name_;
    const common::ObIArray<common::ObString> &split_pool_list = arg.split_pool_list_;
    const common::ObIArray<common::ObZone> &zone_list = arg.zone_list_;
    if (OB_FAIL(unit_manager_.split_resource_pool(pool_name, split_pool_list, zone_list))) {
      LOG_WARN("fail to split resource pool", K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish split_resource_pool", K(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "split_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_resource_tenant(const obrpc::ObAlterResourceTenantArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_UNLIKELY(nullptr == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service ptr is null", KR(ret));
  } else {
    LOG_INFO("receive alter resource tenant request", K(arg));
    const ObString &target_tenant_name = arg.tenant_name_;
    const int64_t new_unit_num = arg.unit_num_;
    const common::ObIArray<uint64_t> &delete_unit_group_id_array = arg.unit_group_id_array_;
    share::schema::ObSchemaGetterGuard schema_guard;
    uint64_t target_tenant_id = OB_INVALID_ID;
    int tmp_ret = OB_SUCCESS;

    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), "tenant_id", OB_SYS_TENANT_ID);
    } else if (OB_FAIL(schema_guard.get_tenant_id(target_tenant_name, target_tenant_id))) {
      LOG_WARN("fail to get tenant id", KR(ret), K(target_tenant_name));
    } else if (OB_UNLIKELY(OB_INVALID_ID == target_tenant_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target_tenant_id value unexpected", KR(ret), K(target_tenant_name), K(target_tenant_id));
    } else if (OB_FAIL(unit_manager_.alter_resource_tenant(
            target_tenant_id, new_unit_num, delete_unit_group_id_array, arg.ddl_stmt_str_))) {
      LOG_WARN("fail to alter resource tenant", KR(ret), K(target_tenant_id),
               K(new_unit_num), K(delete_unit_group_id_array));
      if (OB_TMP_FAIL(submit_reload_unit_manager_task())) {
        if (OB_CANCELED != tmp_ret) {
          LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", KR(ret), KR(tmp_ret));
        }
      }
    }
    LOG_INFO("finish alter_resource_tenant", KR(ret), K(arg));
  }

  ROOTSERVICE_EVENT_ADD("root_service", "alter_resource_tenant", K(ret), K(arg));
  return ret;
}

int ObRootService::merge_resource_pool(const obrpc::ObMergeResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    LOG_INFO("receive merge resource pool request", K(arg));
    const common::ObIArray<common::ObString> &old_pool_list = arg.old_pool_list_;
    const common::ObIArray<common::ObString> &new_pool_list = arg.new_pool_list_;
    if (OB_FAIL(unit_manager_.merge_resource_pool(old_pool_list, new_pool_list))) {
      LOG_WARN("fail to merge resource pool", K(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {//ensure submit task all case
        if (OB_CANCELED != tmp_ret) {
          LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", KR(ret), K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish merge_resource_pool", K(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "merge_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_resource_pool(const obrpc::ObAlterResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (arg.pool_name_.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource pool name");
    }
    LOG_WARN("missing arg to alter resource pool", K(arg), K(ret));
  } else if (0 == arg.unit_.case_compare(OB_STANDBY_UNIT_CONFIG_TEMPLATE_NAME)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not alter resource pool use standby unit config template", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter resource pool use stanby unit config template");
  } else {
    LOG_INFO("receive alter_resource_pool request", K(arg));
    share::ObResourcePool pool;
    pool.name_ = arg.pool_name_;
    pool.unit_count_ = arg.unit_num_;
    if (OB_FAIL(pool.zone_list_.assign(arg.zone_list_))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(unit_manager_.alter_resource_pool(
            pool, arg.unit_, arg.delete_unit_id_array_))) {
      LOG_WARN("alter_resource_pool failed", K(pool), K(arg), "resource unit", arg.unit_, K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish alter_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "alter_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::drop_resource_pool(const obrpc::ObDropResourcePoolArg &arg)
{
  int ret = OB_SUCCESS;
  const bool if_exist = arg.if_exist_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (arg.pool_name_.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource pool name");
    }
    LOG_WARN("missing arg to drop resource pool", K(arg), K(ret));
  } else {
    LOG_INFO("receive drop_resource_pool request", K(arg));
    if (OB_FAIL(unit_manager_.drop_resource_pool(arg.pool_name_, if_exist))) {
      LOG_WARN("drop_resource_pool failed", "pool", arg.pool_name_, K(if_exist), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          if (OB_CANCELED != tmp_ret) {
            LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
          }
        }
      }
    }
    LOG_INFO("finish drop_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "drop_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::check_tenant_in_alter_locality(
    const uint64_t tenant_id,
    bool &in_alter_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ddl_service_.check_tenant_in_alter_locality(tenant_id, in_alter_locality))) {
    LOG_WARN("fail to check tenant in alter locality", K(ret));
  } else {} // no more to do
  return ret;
}

int ObRootService::create_tenant(const ObCreateTenantArg &arg, UInt64 &tenant_id)
{
  LOG_INFO("receive create tenant arg", K(arg), "timeout_ts", THIS_WORKER.get_timeout_ts());
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObString &tenant_name = arg.tenant_schema_.get_tenant_name_str();
  // when recovering table, it needs to create tmp tenant
  const bool tmp_tenant = arg.is_tmp_tenant_for_recover_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!tmp_tenant && OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(tenant_name))) {
    LOG_WARN("unsupported tenant name", KR(ret), K(tenant_name));
  } else if (OB_FAIL(ddl_service_.create_tenant(arg, tenant_id))) {
    LOG_WARN("fail to create tenant", KR(ret), K(arg));
    if (OB_TMP_FAIL(submit_reload_unit_manager_task())) {
      if (OB_CANCELED != tmp_ret) {
        LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", KR(ret), KR(tmp_ret));
      }
    }
  } else {}
  LOG_INFO("finish create tenant", KR(ret), K(tenant_id), K(arg), "timeout_ts", THIS_WORKER.get_timeout_ts());
  return ret;
}

int ObRootService::create_tenant_end(const ObCreateTenantEndArg &arg)
{
  LOG_DEBUG("receive create tenant end arg", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else if (OB_FAIL(ddl_service_.create_tenant_end(arg.tenant_id_))) {
    LOG_WARN("fail to create tenant end", K(ret), K(arg));
  } else {
    LOG_INFO("success to create tenant end", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::commit_alter_tenant_locality(
    const rootserver::ObCommitAlterTenantLocalityArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("commit alter tenant locality", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.commit_alter_tenant_locality(arg))) {
    LOG_WARN("fail to commit alter tenant locality", K(ret));
  } else {
    LOG_INFO("commit alter tenant locality succeed", K(ret));
  }
  return ret;
}


int ObRootService::drop_tenant(const ObDropTenantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_tenant(arg))) {
    LOG_WARN("ddl_service_ drop_tenant failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::flashback_tenant(const ObFlashBackTenantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(arg.new_tenant_name_))) {
    LOG_WARN("unsupported tenant name", KR(ret), "new_tenant_name", arg.new_tenant_name_);
  } else if (OB_FAIL(ddl_service_.flashback_tenant(arg))) {
    LOG_WARN("failed to flash back tenant", K(ret));
  }
  LOG_INFO("flashback tenant success");
  return ret;
}

int ObRootService::purge_tenant(const ObPurgeTenantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_tenant(arg))) {
    LOG_WARN("failed to purge tenant", K(ret));
  }
  LOG_INFO("purge tenant success");
  return ret;
}

int ObRootService::modify_tenant(const ObModifyTenantArg &arg)
{
  LOG_DEBUG("receive modify tenant arg", K(arg));
  int ret = OB_NOT_SUPPORTED;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(arg.new_tenant_name_))) {
    LOG_WARN("unsupported tenant name", KR(ret), "new_tenant_name", arg.new_tenant_name_);
  } else if (OB_FAIL(ddl_service_.modify_tenant(arg))) {
    LOG_WARN("ddl service modify tenant failed", K(arg), K(ret));
  } else {
    root_balancer_.wakeup();
  }
  // weak leader coordinator while modify primary zone
  //if (OB_SUCC(ret)
  //    && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE)) {
  //  leader_coordinator_.signal();
  //}
  return ret;
}

int ObRootService::lock_tenant(const obrpc::ObLockTenantArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive lock tenant request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.lock_tenant(arg.tenant_name_, arg.is_locked_))) {
    LOG_WARN("ddl_service lock_tenant failed", K(arg), K(ret));
  }
  LOG_INFO("finish lock tenant", K(arg), K(ret));
  return ret;
}

int ObRootService::add_system_variable(const ObAddSysVarArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sysvar arg", K(arg));
  } else if (OB_FAIL(ddl_service_.add_system_variable(arg))) {
    LOG_WARN("add system variable failed", K(ret));
  }
  return ret;
}

int ObRootService::modify_system_variable(const obrpc::ObModifySysVarArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sysvar arg", K(arg));
  } else if (OB_FAIL(ddl_service_.modify_system_variable(arg))) {
    LOG_WARN("modify system variable failed", K(ret));
  }
  return ret;
}

int ObRootService::create_database(const ObCreateDatabaseArg &arg, UInt64 &db_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObDatabaseSchema copied_db_schema = arg.database_schema_;
    if (OB_FAIL(ddl_service_.create_database(arg.if_not_exist_,
                                             copied_db_schema, &arg.ddl_stmt_str_))) {
      LOG_WARN("create_database failed", "if_not_exist", arg.if_not_exist_,
               K(copied_db_schema), "ddl_stmt_str", arg.ddl_stmt_str_, K(ret));
    } else {
      db_id = copied_db_schema.get_database_id();
    }
  }
  return ret;
}

int ObRootService::alter_database(const ObAlterDatabaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (common::STANDBY_CLUSTER == ObClusterInfoGetter::get_cluster_role_v2()) {
    const int64_t tenant_id = arg.database_schema_.get_tenant_id();
    ObSchemaGetterGuard schema_guard;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
            tenant_id, schema_guard))) {
      LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_database_id(tenant_id,
            arg.database_schema_.get_database_name_str(), database_id))) {
      LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(arg));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_service_.alter_database(arg))) {
    LOG_WARN("alter database failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::create_tablegroup(const ObCreateTablegroupArg &arg, UInt64 &tg_id)
{
  LOG_INFO("receive create tablegroup arg", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObTablegroupSchema copied_tg_schema;
    if (OB_FAIL(copied_tg_schema.assign(arg.tablegroup_schema_))) {
      LOG_WARN("failed to assign tablegroup schema", K(ret), K(arg));
    } else if (OB_FAIL(ddl_service_.create_tablegroup(
            arg.if_not_exist_, copied_tg_schema, &arg.ddl_stmt_str_))) {
      LOG_WARN("create_tablegroup failed", "if_not_exist", arg.if_not_exist_,
               K(copied_tg_schema), "ddl_stmt_str", arg.ddl_stmt_str_, K(ret));
    } else {
      tg_id = copied_tg_schema.get_tablegroup_id();
    }
  }
  return ret;
}

int ObRootService::handle_security_audit(const ObSecurityAuditArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_security_audit(arg))) {
    LOG_WARN("handle audit request failed", K(ret), K(arg));
  }
  return ret;
}


int ObRootService::parallel_ddl_pre_check_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_dropped = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else if (is_dropped) {
    ret = OB_TENANT_HAS_BEEN_DROPPED;
    LOG_WARN("tenant has been dropped", KR(ret), K(tenant_id));
  } else if (!schema_service_->is_tenant_refreshed(tenant_id)) {
    // use this err to trigger DDL retry and release current thread.
    ret = OB_ERR_PARALLEL_DDL_CONFLICT;
    LOG_WARN("tenant' schema not refreshed yet, need retry", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObRootService::parallel_create_table(const ObCreateTableArg &arg, ObCreateTableRes &res)
{
  LOG_TRACE("receive create table arg", K(arg));
  int64_t begin_time = ObTimeUtility::current_time();
  const uint64_t tenant_id = arg.exec_tenant_id_;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(parallel_ddl_pre_check_(tenant_id))) {
    LOG_WARN("pre check failed before parallel ddl execute", KR(ret), K(tenant_id));
  } else if (arg.schema_.is_view_table()) {
    ObCreateViewHelper create_view_helper(schema_service_, tenant_id, arg, res);
    if (OB_FAIL(create_view_helper.init(ddl_service_))) {
      LOG_WARN("fail to init create view helper", KR(ret), K(tenant_id));
    } else if (OB_FAIL(create_view_helper.execute())) {
      LOG_WARN("fail to execute create view", KR(ret), K(tenant_id));
    }
  } else {
    ObCreateTableHelper create_table_helper(schema_service_, tenant_id, arg, res);
    if (OB_FAIL(create_table_helper.init(ddl_service_))) {
      LOG_WARN("fail to init create table helper", KR(ret), K(tenant_id));
    } else if (OB_FAIL(create_table_helper.execute())) {
      LOG_WARN("fail to execute create table", KR(ret), K(tenant_id));
    }
  }
  int64_t cost = ObTimeUtility::current_time() - begin_time;
  LOG_TRACE("finish create table", KR(ret), K(arg), K(cost));
  ROOTSERVICE_EVENT_ADD("ddl", "parallel_create_table",
                        K(ret), K(tenant_id),
                        "table_id", res.table_id_, K(cost));
  return ret;
}

int ObRootService::create_table(const ObCreateTableArg &arg, ObCreateTableRes &res)
{
  LOG_DEBUG("receive create table arg", K(arg));
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("receive create table ddl", K(begin_time));
  RS_TRACE(create_table_begin);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObArray<ObTableSchema> table_schemas;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    schema_guard.set_session_id(arg.schema_.get_session_id());
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    ObTableSchema table_schema;
    bool is_oracle_mode = false;
    // generate base table schema
    if (OB_FAIL(table_schema.assign(arg.schema_))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KP(schema_service), K(ret));
    } else if (OB_FAIL(generate_table_schema_in_tenant_space(arg, table_schema))) {
      LOG_WARN("fail to generate table schema in tenant space", K(ret), K(arg));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
               table_schema.get_tenant_id(), schema_guard))) {
      LOG_WARN("get_schema_guard with version in inner table failed", K(ret));
    } else if (OB_FAIL(check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                table_schema.get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else if (OB_INVALID_ID == table_schema.get_database_id()) {
      ObString database_name = arg.db_name_;
      if (OB_FAIL(schema_guard.get_database_schema(table_schema.get_tenant_id(),
                                                   database_name,
                                                   db_schema))) {
        LOG_WARN("get databas schema failed", K(arg));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
      } else if (!arg.is_inner_ && db_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("Can't not create table of db in recyclebin", K(ret), K(arg), K(*db_schema));
      } else if (OB_INVALID_ID == db_schema->get_database_id()) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database id is invalid", "tenant_id",
                 table_schema.get_tenant_id(), K(database_name), K(*db_schema), K(ret));
      } else {
        table_schema.set_database_id(db_schema->get_database_id());
      }
    } else {
      // for view, database_id must be filled
    }
    if (OB_SUCC(ret)) {
      bool table_exist = false;
      bool object_exist = false;
      uint64_t synonym_id = OB_INVALID_ID;
      ObSchemaGetterGuard::CheckTableType check_type = ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES;
      if (table_schema.is_mysql_tmp_table()) {
        check_type = ObSchemaGetterGuard::TEMP_TABLE_TYPE;
      } else if (0 == table_schema.get_session_id()) {
        //if session_id <> 0 during create table, need to exclude the existence of temporary table with the same table_name,
        //if there is, need to throw error.
        check_type = ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE;
      }
      if (OB_SUCC(ret)) {
        ObArray<ObSchemaType> conflict_schema_types;
        if (!arg.is_alter_view_
            && OB_FAIL(schema_guard.check_oracle_object_exist(table_schema.get_tenant_id(),
                   table_schema.get_database_id(), table_schema.get_table_name_str(),
                   TABLE_SCHEMA, INVALID_ROUTINE_TYPE, arg.if_not_exist_, conflict_schema_types))) {
          LOG_WARN("fail to check oracle_object exist", K(ret), K(table_schema));
        } else if (conflict_schema_types.count() > 0) {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by an existing object",
                   K(ret), K(table_schema), K(conflict_schema_types));
        }
      }
      if (FAILEDx(schema_guard.check_synonym_exist_with_name(table_schema.get_tenant_id(),
                                                             table_schema.get_database_id(),
                                                             table_schema.get_table_name_str(),
                                                             object_exist,
                                                             synonym_id))) {
        LOG_WARN("fail to check synonym exist", K(table_schema), K(ret));
      } else if (object_exist) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object", K(table_schema), K(ret));
      } else if (OB_FAIL(schema_guard.check_table_exist(table_schema.get_tenant_id(),
                                                        table_schema.get_database_id(),
                                                        table_schema.get_table_name_str(),
                                                        false, /*is index*/
                                                        check_type,
                                                        table_exist))) {
        LOG_WARN("check table exist failed", K(ret), K(table_schema));
      } else if (table_exist) {
        if (table_schema.is_view_table() && arg.if_not_exist_) {
          //create or replace view ...
          //create user table will drop the old view and recreate it in trans
          const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
          if (OB_FAIL(schema_guard.get_simple_table_schema(
                      table_schema.get_tenant_id(),
                      table_schema.get_database_id(),
                      table_schema.get_table_name_str(),
                      false, /*is index*/
                      simple_table_schema))) {
            LOG_WARN("failed to get table schema", K(ret));
          } else if (OB_ISNULL(simple_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("simple_table_schema is null", K(ret));
          } else if (simple_table_schema->get_table_type() == SYSTEM_VIEW
                     || simple_table_schema->get_table_type() == USER_VIEW
                     || simple_table_schema->get_table_type() == MATERIALIZED_VIEW) {
            ret = OB_SUCCESS;
          } else {
            if (is_oracle_mode) {
              ret = OB_ERR_EXIST_OBJECT;
              LOG_WARN("name is already used by an existing object",
                       K(ret), K(table_schema.get_table_name_str()));
            } else { // mysql mode
              const ObDatabaseSchema *db_schema = nullptr;
              if (OB_FAIL(schema_guard.get_database_schema(
                          table_schema.get_tenant_id(),
                          table_schema.get_database_id(),
                          db_schema))) {
                LOG_WARN("get db schema failed", K(ret), K(table_schema.get_database_id()));
              } else if (OB_ISNULL(db_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("db schema is null", K(ret));
              } else {
                ret = OB_ERR_WRONG_OBJECT;
                LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
                    to_cstring(db_schema->get_database_name_str()),
                    to_cstring(table_schema.get_table_name_str()), "VIEW");
                LOG_WARN("table exist", K(ret), K(table_schema));
              }
            }
          }
        } else {
          ret = OB_ERR_TABLE_EXIST;
          LOG_WARN("table exist", K(ret), K(table_schema), K(arg.if_not_exist_));
        }
      } else if (!table_exist && table_schema.is_view_table() && arg.is_alter_view_) {
        // the origin view must exist while alter view
        const ObSimpleDatabaseSchema *simple_db_schema = nullptr;
        if (OB_FAIL(schema_guard.get_database_schema(
                    table_schema.get_tenant_id(),
                    table_schema.get_database_id(),
                    simple_db_schema))) {
          LOG_WARN("get db schema failed", K(ret), K(table_schema.get_database_id()));
        } else if (OB_ISNULL(simple_db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("db schema is null", K(ret));
        } else {
          ret = OB_TABLE_NOT_EXIST;
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                         to_cstring(simple_db_schema->get_database_name_str()),
                         to_cstring(table_schema.get_table_name_str()));
          LOG_WARN("table not exist", K(ret), K(table_schema));
        }
      }
    }
    RS_TRACE(generate_schema_start);
    //bool can_hold_new_table = false;
    common::hash::ObHashMap<ObString, uint64_t> mock_fk_parent_table_map; // name, count
    ObArray<ObMockFKParentTableSchema> tmp_mock_fk_parent_table_schema_array;
    ObArray<ObMockFKParentTableSchema> mock_fk_parent_table_schema_array;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(mock_fk_parent_table_map.create(16, "MockFKParentTbl"))) {
      LOG_WARN("fail to create mock_fk_parent_table_map", K(ret));
    } else if (OB_FAIL(ddl_service_.generate_schema(arg, table_schema))) {
      LOG_WARN("generate_schema for table failed", K(ret));
      //} else if (OB_FAIL(check_rs_capacity(table_schema, can_hold_new_table))) {
      //  LOG_WARN("fail to check rs capacity", K(ret), K(table_schema));
      //} else if (!can_hold_new_table) {
      //  ret = OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT;
      //  LOG_WARN("reach rs's limits, rootserver can only hold limited replicas");
    } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
      LOG_WARN("push_back failed", K(ret));
    } else {
      RS_TRACE(generate_schema_index);
      res.table_id_ = table_schema.get_table_id();
      // generate index schemas
      ObIndexBuilder index_builder(ddl_service_);
      ObTableSchema index_schema;
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_arg_list_.size(); ++i) {
        index_schema.reset();
        ObCreateIndexArg &index_arg = const_cast<ObCreateIndexArg&>(arg.index_arg_list_.at(i));
        //if we pass the table_schema argument, the create_index_arg can not set database_name
        //and table_name, which will used from get data table schema in generate_schema
        if (!index_arg.index_schema_.is_partitioned_table()
            && !table_schema.is_partitioned_table()) {
          if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
          } else if (INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
          } else if (INDEX_TYPE_SPATIAL_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE;
          }
        }
        // the global index has generated column schema during resolve, RS no need to generate index schema,
        // just assign column schema
        if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_
            || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_
            || INDEX_TYPE_SPATIAL_GLOBAL == index_arg.index_type_) {
          if (OB_FAIL(index_schema.assign(index_arg.index_schema_))) {
            LOG_WARN("fail to assign schema", K(ret));
          }
        }
        const bool global_index_without_column_info = false;
        ObSEArray<ObColumnSchemaV2 *, 1> gen_columns;
        ObIAllocator *allocator = index_arg.index_schema_.get_allocator();
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(allocator)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid allocator", K(ret));
        } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(index_arg, table_schema, *allocator, gen_columns))) {
            LOG_WARN("fail to adjust expr index args", K(ret));
        } else if (OB_FAIL(index_builder.generate_schema(index_arg,
                                                         table_schema,
                                                         global_index_without_column_info,
                                                         true, /*generate_id*/
                                                         index_schema))) {
          LOG_WARN("generate_schema for index failed", K(index_arg), K(table_schema), K(ret));
        }
        if (OB_SUCC(ret)) {
          uint64_t new_table_id = OB_INVALID_ID;
          if (OB_FAIL(schema_service->fetch_new_table_id(table_schema.get_tenant_id(), new_table_id))) {
            LOG_WARN("failed to fetch_new_table_id", "tenant_id", table_schema.get_tenant_id(), K(ret));
          } else {
            index_schema.set_table_id(new_table_id);
            //index_schema.set_data_table_id(table_id);
            if (OB_FAIL(table_schemas.push_back(index_schema))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
      RS_TRACE(generate_schema_lob);
      if (OB_FAIL(ret) || table_schema.is_view_table() || table_schema.is_external_table()) {
        // do nothing
      } else if (OB_FAIL(ddl_service_.build_aux_lob_table_schema_if_need(table_schema, table_schemas))) {
        LOG_WARN("fail to build_aux_lob_table_schema_if_need", K(ret), K(table_schema));
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.foreign_key_arg_list_.count(); i++) {
          const ObCreateForeignKeyArg &foreign_key_arg = arg.foreign_key_arg_list_.at(i);
          ObForeignKeyInfo foreign_key_info;
          // check for duplicate constraint names of foregin key
          if (foreign_key_arg.foreign_key_name_.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fk name is empty", K(ret));
          } else {
            bool is_foreign_key_name_exist = true;
            if (OB_FAIL(ddl_service_.check_constraint_name_is_exist(
                        schema_guard, table_schema, foreign_key_arg.foreign_key_name_, true, is_foreign_key_name_exist))) {
              LOG_WARN("fail to check foreign key name is exist or not", K(ret), K(foreign_key_arg.foreign_key_name_));
            } else if(is_foreign_key_name_exist) {
              if (is_oracle_mode) {
                ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
                LOG_WARN("fk name is duplicate", K(ret), K(foreign_key_arg.foreign_key_name_));
              } else { // mysql mode
                ret = OB_ERR_DUP_KEY;
                LOG_USER_ERROR(OB_ERR_DUP_KEY,
                    table_schema.get_table_name_str().length(),
                    table_schema.get_table_name_str().ptr());
              }
            }
          }
          // end of check for duplicate constraint names of foregin key
          const ObTableSchema *parent_schema = NULL;
          if (OB_SUCC(ret)) {
            // get parent table schema.
            // TODO: is it necessory to determine whether it is case sensitive by check sys variable
            // check whether it belongs to self reference, if so, the parent schema is child schema.
            if (0 == foreign_key_arg.parent_table_.case_compare(table_schema.get_table_name_str())
                  && 0 == foreign_key_arg.parent_database_.case_compare(arg.db_name_)) {
              parent_schema = &table_schema;
              if (CONSTRAINT_TYPE_PRIMARY_KEY == foreign_key_arg.ref_cst_type_) {
                if (is_oracle_mode) {
                  for (ObTableSchema::const_constraint_iterator iter = parent_schema->constraint_begin(); iter != parent_schema->constraint_end(); ++iter) {
                    if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
                      foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
                      foreign_key_info.ref_cst_id_ = (*iter)->get_constraint_id();
                      break;
                    }
                  }
                } else {
                  foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
                  foreign_key_info.ref_cst_id_ = common::OB_INVALID_ID;
                }
              } else if (CONSTRAINT_TYPE_UNIQUE_KEY == foreign_key_arg.ref_cst_type_) {
                if (OB_FAIL(ddl_service_.get_uk_cst_id_for_self_ref(table_schemas, foreign_key_arg, foreign_key_info))) {
                  LOG_WARN("failed to get uk cst id for self ref", K(ret), K(foreign_key_arg));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid foreign key ref cst type", K(ret), K(foreign_key_arg));
              }
            } else if (OB_FAIL(schema_guard.get_table_schema(table_schema.get_tenant_id(),
                                                             foreign_key_arg.parent_database_,
                                                             foreign_key_arg.parent_table_,
                                                             false, parent_schema))) {
              LOG_WARN("failed to get parent table schema", K(ret), K(foreign_key_arg));
            } else {
              foreign_key_info.ref_cst_type_ = foreign_key_arg.ref_cst_type_;
              foreign_key_info.ref_cst_id_ = foreign_key_arg.ref_cst_id_;
            }
          }
          const ObMockFKParentTableSchema *tmp_mock_fk_parent_table_ptr = NULL;
          ObMockFKParentTableSchema mock_fk_parent_table_schema;
          if (OB_SUCC(ret)) {
            if (foreign_key_arg.is_parent_table_mock_) {
              uint64_t dup_name_mock_fk_parent_table_count = 0;
              if (NULL != parent_schema) {
                ret = OB_ERR_PARALLEL_DDL_CONFLICT;
                LOG_WARN("the mock parent table is conflict with the real parent table, need retry",
                    K(ret), K(foreign_key_arg), K(parent_schema->get_table_id()));
              } else if (OB_FAIL(mock_fk_parent_table_map.get_refactored(foreign_key_arg.parent_table_, dup_name_mock_fk_parent_table_count))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  ret = OB_SUCCESS;
                  if (OB_FAIL(mock_fk_parent_table_map.set_refactored(foreign_key_arg.parent_table_, ++dup_name_mock_fk_parent_table_count))) {
                    LOG_WARN("failed to insert into mock_fk_parent_table_map", K(ret), K(foreign_key_arg), K(dup_name_mock_fk_parent_table_count));
                  }
                } else {
                  LOG_WARN("get_refactored from mock_fk_parent_table_map failed", K(ret), K(foreign_key_arg));
                }
              } else {
                //already had dup name mock_fk_parent_table in tmp_mock_fk_parent_table_schema_array
                int64_t count = 0;
                for (int64_t i = 0; i < tmp_mock_fk_parent_table_schema_array.count(); ++i) {
                  if (0 == tmp_mock_fk_parent_table_schema_array.at(i).get_mock_fk_parent_table_name().case_compare(foreign_key_arg.parent_table_)) {
                    if (++count == dup_name_mock_fk_parent_table_count) {
                      tmp_mock_fk_parent_table_ptr = &tmp_mock_fk_parent_table_schema_array.at(i);
                      break;
                    }
                  }
                }
                if (OB_ISNULL(tmp_mock_fk_parent_table_ptr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("tmp_mock_fk_parent_table_ptr is null", K(ret), K(foreign_key_arg), K(tmp_mock_fk_parent_table_schema_array));
                } else if (OB_FAIL(mock_fk_parent_table_map.set_refactored(foreign_key_arg.parent_table_, ++dup_name_mock_fk_parent_table_count, true/*overwrite*/))) {
                  LOG_WARN("failed to insert into mock_fk_parent_table_map", K(ret), K(foreign_key_arg), K(dup_name_mock_fk_parent_table_count));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ddl_service_.gen_mock_fk_parent_table_for_create_fk(
                         schema_guard, table_schema.get_tenant_id(), foreign_key_arg, tmp_mock_fk_parent_table_ptr, foreign_key_info, mock_fk_parent_table_schema))) {
                LOG_WARN("failed to generate_mock_fk_parent_table_schema", K(ret), K(table_schema.get_tenant_id()), K(foreign_key_arg));
              }
            } else if (OB_ISNULL(parent_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("parent table is not exist", K(ret), K(foreign_key_arg));
            } else if (false == parent_schema->is_tmp_table()
                           && 0 != parent_schema->get_session_id()
                           && OB_INVALID_ID != schema_guard.get_session_id()) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(foreign_key_arg.parent_database_), to_cstring(foreign_key_arg.parent_table_));
            } else if (!arg.is_inner_ && parent_schema->is_in_recyclebin()) {
              ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
              LOG_WARN("parent table is in recyclebin", K(ret), K(foreign_key_arg));
            } else if (parent_schema->get_table_id() != table_schema.get_table_id()) {
              // no need to update sync_versin_for_cascade_table while the refrence table is itself
              if (OB_FAIL(table_schema.add_depend_table_id(parent_schema->get_table_id()))) {
                LOG_WARN("failed to add depend table id", K(ret), K(foreign_key_arg));
              }
            }
          }
          // get child column schema.
          if (OB_SUCC(ret)) {
            foreign_key_info.child_table_id_ = res.table_id_;
            foreign_key_info.parent_table_id_ = foreign_key_arg.is_parent_table_mock_ ? mock_fk_parent_table_schema.get_mock_fk_parent_table_id() : parent_schema->get_table_id();
            for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.child_columns_.count(); j++) {
              const ObString &column_name = foreign_key_arg.child_columns_.at(j);
              const ObColumnSchemaV2 *column_schema = table_schema.get_column_schema(column_name);
              if (OB_ISNULL(column_schema)) {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                LOG_WARN("child column is not exist", K(ret), K(column_name));
              } else if (OB_FAIL(foreign_key_info.child_column_ids_.push_back(column_schema->get_column_id()))) {
                LOG_WARN("failed to push child column id", K(ret), K(column_name));
              }
            }
          }
          // get parent column schema.
          if (OB_SUCC(ret) && !foreign_key_arg.is_parent_table_mock_) {
            for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.parent_columns_.count(); j++) {
              const ObString &column_name = foreign_key_arg.parent_columns_.at(j);
              const ObColumnSchemaV2 *column_schema = parent_schema->get_column_schema(column_name);
              if (OB_ISNULL(column_schema)) {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                LOG_WARN("parent column is not exist", K(ret), K(column_name));
              } else if (OB_FAIL(foreign_key_info.parent_column_ids_.push_back(column_schema->get_column_id()))) {
                LOG_WARN("failed to push parent column id", K(ret), K(column_name));
              }
            }
          }
          // get reference option and foreign key name.
          if (OB_SUCC(ret)) {
            foreign_key_info.update_action_ = foreign_key_arg.update_action_;
            foreign_key_info.delete_action_ = foreign_key_arg.delete_action_;
            foreign_key_info.foreign_key_name_ = foreign_key_arg.foreign_key_name_;
            foreign_key_info.enable_flag_ = foreign_key_arg.enable_flag_;
            foreign_key_info.validate_flag_ = foreign_key_arg.validate_flag_;
            foreign_key_info.rely_flag_ = foreign_key_arg.rely_flag_;
            foreign_key_info.is_parent_table_mock_ = foreign_key_arg.is_parent_table_mock_;
            foreign_key_info.name_generated_type_ = foreign_key_arg.name_generated_type_;
          }
          // add foreign key info.
          if (OB_SUCC(ret)) {
            if (OB_FAIL(schema_service->fetch_new_constraint_id(table_schema.get_tenant_id(),
                                                                foreign_key_info.foreign_key_id_))) {
              LOG_WARN("failed to fetch new foreign key id", K(ret), K(foreign_key_arg));
            } else if (OB_FAIL(table_schema.add_foreign_key_info(foreign_key_info))) {
              LOG_WARN("failed to push foreign key info", K(ret), K(foreign_key_info));
            } else if (foreign_key_info.is_parent_table_mock_
                       && MOCK_FK_PARENT_TABLE_OP_INVALID != mock_fk_parent_table_schema.get_operation_type()) {
              if (OB_FAIL(mock_fk_parent_table_schema.add_foreign_key_info(foreign_key_info))) {
                LOG_WARN("failed to push foreign key info", K(ret), K(foreign_key_info));
              } else if (ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE == mock_fk_parent_table_schema.get_operation_type()) {
                if (OB_FAIL(tmp_mock_fk_parent_table_schema_array.push_back(mock_fk_parent_table_schema))) {
                  LOG_WARN("failed to push mock_fk_parent_table_schema to tmp_mock_fk_parent_table_schema_array", K(ret), K(mock_fk_parent_table_schema));
                }
              } else { // ObMockFKParentTableOperationType::MOCK_FK_PARENT_TABLE_OP_CREATE_TABLE_BY_ADD_FK_IN_CHILD_TBALE != mock_fk_parent_table_schema.get_operation_type()
                if (OB_FAIL(mock_fk_parent_table_schema_array.push_back(mock_fk_parent_table_schema))) {
                  LOG_WARN("failed to push mock_fk_parent_table_schema to mock_fk_parent_table_schema_array", K(ret), K(mock_fk_parent_table_schema));
                } else if (OB_FAIL(mock_fk_parent_table_map.erase_refactored(mock_fk_parent_table_schema.get_mock_fk_parent_table_name()))) {
                  LOG_WARN("failed to delete from mock_fk_parent_table_map", K(ret), K(mock_fk_parent_table_schema.get_mock_fk_parent_table_name()));
                }
              }
            }
          }
        } // for
        if (OB_SUCC(ret)) {
          // push back to mock_fk_parent_table_schema_array with the last one of all dup name mock_fk_parent_table_schema
          if (!tmp_mock_fk_parent_table_schema_array.empty()) {
            for (int64_t i = 0; OB_SUCC(ret) && i < tmp_mock_fk_parent_table_schema_array.count(); ++i) {
              uint64_t dup_name_mock_fk_parent_table_count = 0;
              ObString mock_fk_parent_table_name;
              if (OB_FAIL(mock_fk_parent_table_map.get_refactored(tmp_mock_fk_parent_table_schema_array.at(i).get_mock_fk_parent_table_name(), dup_name_mock_fk_parent_table_count))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  ret = OB_SUCCESS;
                  continue;
                } else {
                  LOG_WARN("get_refactored from mock_fk_parent_table_map failed", K(ret), K(tmp_mock_fk_parent_table_schema_array.at(i)));
                }
              } else {
                mock_fk_parent_table_name = tmp_mock_fk_parent_table_schema_array.at(i).get_mock_fk_parent_table_name();
                int64_t j = i;
                uint64_t count = 0;
                for (; count < dup_name_mock_fk_parent_table_count && j < tmp_mock_fk_parent_table_schema_array.count(); ++j) {
                  if (0 == mock_fk_parent_table_name.case_compare(tmp_mock_fk_parent_table_schema_array.at(j).get_mock_fk_parent_table_name())) {
                    ++count;
                  }
                }
                if (--j >= tmp_mock_fk_parent_table_schema_array.count()) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("j >= tmp_mock_fk_parent_table_schema_array.count()", K(ret), K(j), K(tmp_mock_fk_parent_table_schema_array.count()));
                } else {
                  for (int64_t k = 0; OB_SUCC(ret) && k < tmp_mock_fk_parent_table_schema_array.at(j).get_foreign_key_infos().count(); ++k) {
                    tmp_mock_fk_parent_table_schema_array.at(j).get_foreign_key_infos().at(k).parent_table_id_ = tmp_mock_fk_parent_table_schema_array.at(j).get_mock_fk_parent_table_id();
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(mock_fk_parent_table_schema_array.push_back(tmp_mock_fk_parent_table_schema_array.at(j)))) {
                  LOG_WARN("fail to push back to mock_fk_parent_table_schema_array", K(ret), K(tmp_mock_fk_parent_table_schema_array.at(j)));
                } else if (OB_FAIL(mock_fk_parent_table_map.erase_refactored(mock_fk_parent_table_name))) {
                  LOG_WARN("failed to delete from mock_fk_parent_table_map", K(mock_fk_parent_table_name), K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          // deal with new table name which is the same to mock_fk_parent_table_name, replace mock_parent_table with this new table
          const ObMockFKParentTableSchema *ori_mock_parent_table_schema_ptr = NULL;
          if (OB_FAIL(schema_guard.get_mock_fk_parent_table_schema_with_name(
              table_schema.get_tenant_id(),
              table_schema.get_database_id(),
              table_schema.get_table_name_str(),
              ori_mock_parent_table_schema_ptr))) {
            LOG_WARN("failed to check_mock_fk_parent_table_exist_with_name");
          } else if (OB_NOT_NULL(ori_mock_parent_table_schema_ptr)) {
            ObMockFKParentTableSchema mock_fk_parent_table_schema;
            ObArray<const share::schema::ObTableSchema*> index_schemas;
            for (int64_t i = 1; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
              if (table_schemas.at(i).is_unique_index()
                  && OB_FAIL(index_schemas.push_back(&table_schemas.at(i)))) {
                LOG_WARN("failed to push back index_schemas", K(ret));
              }
            }
            if (FAILEDx(ddl_service_.gen_mock_fk_parent_table_for_replacing_mock_fk_parent_table(
                schema_guard, ori_mock_parent_table_schema_ptr->get_mock_fk_parent_table_id(), table_schema, index_schemas,
                mock_fk_parent_table_schema))) {
              LOG_WARN("failed to gen_mock_fk_parent_table_for_replacing_mock_fk_parent_table", K(ret));
            } else if (OB_FAIL(mock_fk_parent_table_schema_array.push_back(mock_fk_parent_table_schema))) {
              LOG_WARN("failed to push mock_fk_parent_table_schema", K(ret), K(mock_fk_parent_table_schema));
            }
          }
        }
      } // check foreign key info end.
    }
    RS_TRACE(generate_schema_finish);
    if (OB_SUCC(ret)) {
      //table schema may be updated during analyse index schema, so reset table_schema
      const bool is_standby = PRIMARY_CLUSTER != ObClusterInfoGetter::get_cluster_role_v2();
      if (OB_FAIL(table_schemas.at(0).assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else if (OB_FAIL(ddl_service_.create_user_tables(
                                      arg.if_not_exist_,
                                      arg.ddl_stmt_str_,
                                      arg.error_info_,
                                      table_schemas,
                                      schema_guard,
                                      arg.sequence_ddl_arg_,
                                      arg.last_replay_log_id_,
                                      &arg.dep_infos_,
                                      mock_fk_parent_table_schema_array))) {
        LOG_WARN("create_user_tables failed", "if_not_exist", arg.if_not_exist_,
                 "ddl_stmt_str", arg.ddl_stmt_str_, K(ret));
      }
    }
    if (OB_ERR_TABLE_EXIST == ret) {
      //create table xx if not exist (...)
      //create or replace view xx as ...
      if (arg.if_not_exist_) {
        ret = OB_SUCCESS;
        LOG_INFO("table is exist, no need to create again, ",
                 "tenant_id", table_schema.get_tenant_id(),
                 "database_id", table_schema.get_database_id(),
                 "table_name", table_schema.get_table_name());
      } else {
        ret = OB_ERR_TABLE_EXIST;
        LOG_USER_ERROR(OB_ERR_TABLE_EXIST, table_schema.get_table_name_str().length(),
            table_schema.get_table_name_str().ptr());
        LOG_WARN("table is exist, cannot create it twice,",
                 "tenant_id", table_schema.get_tenant_id(),
                 "database_id", table_schema.get_database_id(),
                 "table_name", table_schema.get_table_name(), K(ret));
      }
    }
    // check vertical partition
    // is_primary_vp_table()
    // get_aux_vp_tid_array()
    // is_aux_vp_table()
    // get_vp_store_column_ids
    // get_vp_column_ids_without_rowkey
    if (OB_SUCC(ret)) {
      ObSchemaGetterGuard new_schema_guard;
      const ObTableSchema *new_table_schema = NULL;
      const uint64_t arg_vp_cnt = arg.vertical_partition_arg_list_.count();

      if (arg_vp_cnt == 0) {
        LOG_INFO("avg_vp_cnt is 0");
        // do-nothing
      } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                         table_schema.get_tenant_id(), new_schema_guard))) {
        LOG_WARN("fail to get schema guard with version in inner table",
                 K(ret), K(table_schema.get_tenant_id()));
      } else if (OB_FAIL(new_schema_guard.get_table_schema(table_schema.get_tenant_id(),
                                                           table_schema.get_table_id(),
                                                           new_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(table_schema));
      } else if (NULL == new_table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret));
      } else if (!new_table_schema->is_primary_vp_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is_primary_vp_table is invalid", K(ret), K(arg_vp_cnt), K(new_table_schema->is_primary_vp_table()));
      } else {
        ObSEArray<uint64_t, 16> aux_vp_tid_array;
        if (OB_FAIL(new_table_schema->get_aux_vp_tid_array(aux_vp_tid_array))) {
          LOG_WARN("failed to get_aux_vp_tid_array", K(*new_table_schema));
        } else if (!((arg_vp_cnt == (aux_vp_tid_array.count()+ 1)
                      || (arg_vp_cnt == aux_vp_tid_array.count())))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arg_vp_cnt is not equal to aux_vp_cnt_ or (aux_vp_cnt_+1)",
                   K(ret), K(arg_vp_cnt), K(aux_vp_tid_array.count()));
        } else {
          // check primary partition table include get_vp_store_column_ids and vertical partition column information
          ObArray<share::schema::ObColDesc> columns;
          const ObColumnSchemaV2 *column_schema = NULL;
          const ObCreateVertialPartitionArg primary_vp_arg = arg.vertical_partition_arg_list_.at(0);
          int64_t arg_pri_vp_col_cnt = primary_vp_arg.vertical_partition_columns_.count();
          if (OB_FAIL(new_table_schema->get_vp_store_column_ids(columns))) {
            LOG_WARN("get_vp_store_column_ids failed", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
            LOG_INFO("column info", K(columns.at(i).col_id_), K(columns.at(i).col_type_));
            if (NULL == (column_schema = new_table_schema->get_column_schema(columns.at(i).col_id_))) {
              ret = OB_ERR_BAD_FIELD_ERROR;
              LOG_WARN("get_column_schema failed", K(columns.at(i)), K(ret));
            } else {
              ObString column_name = column_schema->get_column_name();
              LOG_INFO("column info", K(column_name),
                  K(column_schema->get_column_id()), K(column_schema->get_table_id()));
              if (column_schema->is_primary_vp_column()) {
                for (int64_t j = 0; OB_SUCC(ret) && j < primary_vp_arg.vertical_partition_columns_.count(); ++j) {
                  ObString pri_vp_col = primary_vp_arg.vertical_partition_columns_.at(j);
                  if (0 == column_name.case_compare(pri_vp_col)) {
                    arg_pri_vp_col_cnt--;
                    LOG_INFO("primary vp", K(column_name));
                    break;
                  }
                }
              } else {
                LOG_INFO("non-primary vp", K(column_name));
              }
            }
          }
          if (OB_SUCC(ret) && (0 != arg_pri_vp_col_cnt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mismatch primary vp column", K(ret));
            for (int64_t j = 0; j < arg_pri_vp_col_cnt; ++j) {
              ObString pri_vp_col = primary_vp_arg.vertical_partition_columns_.at(j);
              LOG_INFO("arg primary vp", K(pri_vp_col));
            }
          }

          // verify secondary partition table
          if (OB_SUCC(ret)) {
            int64_t N = aux_vp_tid_array.count();
            for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
              const ObTableSchema *aux_vp_table_schema = NULL;
              ObArray<share::schema::ObColDesc> vp_columns;
              ObArray<share::schema::ObColDesc> store_columns;
              if (OB_FAIL(new_schema_guard.get_table_schema(table_schema.get_tenant_id(),
                          aux_vp_tid_array.at(i), aux_vp_table_schema))) {
                LOG_WARN("get_table_schema failed", "table id", aux_vp_tid_array.at(i), K(ret));
              } else if (NULL == aux_vp_table_schema) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("aux vp table is null", K(ret));
              } else if (!aux_vp_table_schema->is_aux_vp_table()
                  || AUX_VERTIAL_PARTITION_TABLE != aux_vp_table_schema->get_table_type()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("aux vp table type is incorrect", K(ret), K(aux_vp_table_schema->is_aux_vp_table()));
              } else if (OB_FAIL(aux_vp_table_schema->get_vp_column_ids(vp_columns))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get aux vp table columns", K(ret), K(*aux_vp_table_schema));
              } else if (OB_FAIL(aux_vp_table_schema->get_vp_store_column_ids(store_columns))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get aux vp table columns", K(ret), K(*aux_vp_table_schema));
              } else {
                LOG_INFO("table info", K(aux_vp_table_schema->get_table_name()), K(aux_vp_table_schema->get_table_id()),
                        K(aux_vp_table_schema->get_data_table_id()), K(ret));
                const ObColumnSchemaV2 *column_schema = NULL;

                for (int64_t k = 0; OB_SUCC(ret) && k < vp_columns.count(); ++k) {
                  LOG_INFO("column info", K(vp_columns.at(k).col_id_), K(vp_columns.at(k).col_type_));
                  if (NULL == (column_schema = aux_vp_table_schema->get_column_schema(vp_columns.at(k).col_id_))) {
                    ret = OB_ERR_BAD_FIELD_ERROR;
                    LOG_WARN("get_column_schema failed", K(vp_columns.at(k)), K(ret));
                  } else {
                    LOG_INFO("column info", K(column_schema->get_column_name()), K(column_schema->get_column_id()),
                        K(column_schema->get_table_id()), K(ret));
                  }
                }
                // verify get_vp_store_column_ids return all vertical partition columns,
                // include vertical partition columns of primary key.
                for (int64_t k = 0; OB_SUCC(ret) && k < store_columns.count(); ++k) {
                  LOG_INFO("column info", K(store_columns.at(k).col_id_), K(store_columns.at(k).col_type_));
                  if (NULL == (column_schema = aux_vp_table_schema->get_column_schema(store_columns.at(k).col_id_))) {
                    ret = OB_ERR_BAD_FIELD_ERROR;
                    LOG_WARN("get_column_schema failed", K(store_columns.at(k)), K(ret));
                  } else {
                    LOG_INFO("column info", K(column_schema->get_column_name()), K(column_schema->get_column_id()),
                        K(column_schema->get_table_id()), K(ret));
                  }
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      uint64_t tenant_id = table_schema.get_tenant_id();
      if (OB_FAIL(schema_service_->get_tenant_schema_version(tenant_id, res.schema_version_))) {
        LOG_WARN("failed to get tenant schema version", K(ret));
      }
    }
  }

  RS_TRACE(create_table_end);
  FORCE_PRINT_TRACE(THE_RS_TRACE, "[create table]");
  int64_t cost = ObTimeUtility::current_time() - begin_time;
  ROOTSERVICE_EVENT_ADD("ddl", "create_table", K(ret), "table_id", res.table_id_, K(cost));
  return ret;
}

// create sys_table by specify table_id for tenant:
// 1. can not create table cross tenant except sys tenant.
// 2. part_type of sys table only support non-partition or only level hash_like part type.
// 3. sys table's tablegroup and database must be oceanbase
int ObRootService::generate_table_schema_in_tenant_space(
    const ObCreateTableArg &arg,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  const uint64_t table_id = table_schema.get_table_id();
  const ObPartitionLevel part_level = table_schema.get_part_level();
  const ObPartitionFuncType part_func_type = table_schema.get_part_option().get_part_func_type();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_inner_table(table_id)) {
    // skip
  } else if (OB_SYS_TENANT_ID != arg.exec_tenant_id_) {
    //FIXME: this restriction should be removed later.
    // only enable sys tenant create sys table
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only sys tenant can create tenant space table", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "non-sys tenant creating system tables");
  } else if (table_schema.is_view_table()) {
    // no need specify tenant_id while specify table_id creating sys table
    if (OB_SYS_TENANT_ID != tenant_id) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create sys view with ordinary tenant not allowed", K(ret), K(table_schema));
    }
  } else if (part_level > ObPartitionLevel::PARTITION_LEVEL_ONE
             || !is_hash_like_part(part_func_type)) {
    // sys tables do not write __all_part table, so sys table only support non-partition or only level hash_like part type.
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys table's partition option is invalid", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "invalid partition option to system table");
  } else if (0 != table_schema.get_tablegroup_name().case_compare(OB_SYS_TABLEGROUP_NAME)) {
    // sys tables's tablegroup must be oceanbase
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys table's tablegroup should be oceanbase", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "invalid tablegroup to system table");
  } else if (0 != arg.db_name_.case_compare(OB_SYS_DATABASE_NAME)) {
    // sys tables's database  must be oceanbase
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("sys table's database should be oceanbase", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "invalid database to sys table");
  } else {
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_table_id(table_id);
    table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
    table_schema.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
    table_schema.set_database_id(OB_SYS_DATABASE_ID);
  }
  return ret;
}

int ObRootService::maintain_obj_dependency_info(const obrpc::ObDependencyObjDDLArg &arg)
{
  LOG_DEBUG("receive maintain obj dependency info arg", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.maintain_obj_dependency_info(arg))) {
    LOG_WARN("failed to maintain obj dependency info", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::execute_ddl_task(const obrpc::ObAlterTableArg &arg,
                                    common::ObSArray<uint64_t> &obj_ids)
{
  LOG_DEBUG("receive execute ddl task arg", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    switch (arg.ddl_task_type_) {
      case share::REBUILD_INDEX_TASK: {
        if (OB_FAIL(ddl_service_.rebuild_hidden_table_index_in_trans(
            const_cast<obrpc::ObAlterTableArg &>(arg), obj_ids))) {
          LOG_WARN("failed to rebuild hidden table index in trans", K(ret));
        }
        break;
      }
      case share::REBUILD_CONSTRAINT_TASK: {
        if (OB_FAIL(ddl_service_.rebuild_hidden_table_constraints_in_trans(
            const_cast<obrpc::ObAlterTableArg &>(arg), obj_ids))) {
          LOG_WARN("failed to rebuild hidden table constraints in trans", K(ret));
        }
        break;
      }
      case share::REBUILD_FOREIGN_KEY_TASK: {
        if (OB_FAIL(ddl_service_.rebuild_hidden_table_foreign_key_in_trans(
            const_cast<obrpc::ObAlterTableArg &>(arg), obj_ids))) {
          LOG_WARN("failed to rebuild hidden table foreign key in trans", K(ret));
        }
        break;
      }
      case share::MAKE_DDL_TAKE_EFFECT_TASK: {
        if (OB_FAIL(ddl_service_.swap_orig_and_hidden_table_state(
            const_cast<obrpc::ObAlterTableArg &>(arg)))) {
          LOG_WARN("failed to swap orig and hidden table state", K(ret));
        }
        break;
      }
      case share::CLEANUP_GARBAGE_TASK: {
        if (OB_FAIL(ddl_service_.cleanup_garbage(
            const_cast<obrpc::ObAlterTableArg &>(arg)))) {
          LOG_WARN("failed to cleanup garbage", K(ret));
        }
        break;
      }
      case share::MODIFY_FOREIGN_KEY_STATE_TASK: {
        if (OB_FAIL(ddl_service_.modify_hidden_table_fk_state(
            const_cast<obrpc::ObAlterTableArg &>(arg)))) {
          LOG_WARN("failed to modify hidden table fk state", K(ret));
        }
        break;
      }
      case share::DELETE_COLUMN_FROM_SCHEMA: {
        if (OB_FAIL(ddl_service_.delete_column_from_schema(const_cast<ObAlterTableArg &>(arg)))) {
          LOG_WARN("fail to set column to no minor status", K(ret), K(arg));
        }
        break;
      }
      // remap all index tables to hidden table and take effect concurrently.
      case share::REMAP_INDEXES_AND_TAKE_EFFECT_TASK: {
        if (OB_FAIL(ddl_service_.remap_index_tablets_and_take_effect(
            const_cast<obrpc::ObAlterTableArg &>(arg)))) {
          LOG_WARN("fail to remap index tables to hidden table and take effect", K(ret));
        }
        break;
      }
      case share::UPDATE_AUTOINC_SCHEMA: {
        if (OB_FAIL(ddl_service_.update_autoinc_schema(const_cast<ObAlterTableArg &>(arg)))) {
          LOG_WARN("fail to update autoinc schema", K(ret), K(arg));
        }
        break;
      }
      case share::MODIFY_NOT_NULL_COLUMN_STATE_TASK: {
        if (OB_FAIL(ddl_service_.modify_hidden_table_not_null_column_state(arg))) {
          LOG_WARN("failed to modify hidden table cst state", K(ret));
        }
        break;
      }
      case share::MAKE_RECOVER_RESTORE_TABLE_TASK_TAKE_EFFECT: {
        if (OB_FAIL(ddl_service_.make_recover_restore_tables_visible(const_cast<ObAlterTableArg &>(arg)))) {
          LOG_WARN("make recovert restore task visible failed", K(ret), K(arg));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown ddl task type", K(ret), K(arg.ddl_task_type_));
    }
  }
  return ret;
}

int ObRootService::precheck_interval_part(const obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObAlterTableArg::AlterPartitionType op_type = arg.alter_part_type_;
  const ObSimpleTableSchemaV2 *simple_table_schema = NULL;
  const AlterTableSchema &alter_table_schema = arg.alter_table_schema_;
  int64_t tenant_id = alter_table_schema.get_tenant_id();

  if (!alter_table_schema.is_interval_part()
      || obrpc::ObAlterTableArg::ADD_PARTITION != op_type) {
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
             alter_table_schema.get_table_id(), simple_table_schema))) {
    LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(alter_table_schema));
  } else if (OB_ISNULL(simple_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("simple_table_schema is null", K(ret), K(alter_table_schema));
  } else if (simple_table_schema->get_schema_version() < alter_table_schema.get_schema_version()) {
  } else if (simple_table_schema->get_interval_range() != alter_table_schema.get_interval_range()
             || simple_table_schema->get_transition_point() != alter_table_schema.get_transition_point()) {
    ret = OB_ERR_INTERVAL_PARTITION_ERROR;
    LOG_WARN("interval_range or transition_point is changed", KR(ret), \
             KPC(simple_table_schema), K(alter_table_schema));
  } else {
    int64_t j = 0;
    const ObRowkey *rowkey_orig= NULL;
    bool is_all_exist = true;
    ObPartition **inc_part_array = alter_table_schema.get_part_array();
    ObPartition **orig_part_array = simple_table_schema->get_part_array();
    if (OB_ISNULL(inc_part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
    } else if (OB_ISNULL(orig_part_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
    }
    for (int64_t i = 0; is_all_exist && OB_SUCC(ret) && i < alter_table_schema.get_part_option().get_part_num(); ++i) {
      const ObRowkey *rowkey_cur = NULL;
      if (OB_ISNULL(inc_part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
      } else if (OB_UNLIKELY(NULL == (rowkey_cur = &inc_part_array[i]->get_high_bound_val()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
      }
      while (is_all_exist && OB_SUCC(ret) && j < simple_table_schema->get_part_option().get_part_num()) {
        if (OB_ISNULL(orig_part_array[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
        } else if (OB_UNLIKELY(NULL == (rowkey_orig = &orig_part_array[j]->get_high_bound_val()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptr is null", K(ret), K(alter_table_schema), KPC(simple_table_schema));
        } else if (*rowkey_orig < *rowkey_cur) {
          j++;
        } else {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (*rowkey_orig != *rowkey_cur) {
        is_all_exist = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_all_exist) {
      LOG_INFO("all interval part for add is exist", K(alter_table_schema), KPC(simple_table_schema));
      ret = OB_ERR_INTERVAL_PARTITION_EXIST;
    }
  }
  return ret;
}

int ObRootService::create_hidden_table(const obrpc::ObCreateHiddenTableArg &arg,
                                       obrpc::ObCreateHiddenTableRes &res)
{
  LOG_DEBUG("receive create hidden table arg", K(arg));
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.create_hidden_table(arg, res))) {
    LOG_WARN("do create hidden table in trans failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::update_ddl_task_active_time(const obrpc::ObUpdateDDLTaskActiveTimeArg &arg)
{
  LOG_DEBUG("receive recv ddl task status arg", K(arg));
  int ret = OB_SUCCESS;
  const int64_t task_id = arg.task_id_;
  const uint64_t tenant_id = arg.tenant_id_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.update_ddl_task_active_time(ObDDLTaskID(tenant_id, task_id)))) {
    LOG_WARN("fail to set RegTaskTime map", K(ret), K(tenant_id), K(task_id));
  }
  return ret;
}

int ObRootService::abort_redef_table(const obrpc::ObAbortRedefTableArg &arg)
{
  LOG_DEBUG("receive abort redef table arg", K(arg));
  int ret = OB_SUCCESS;
  const int64_t task_id = arg.task_id_;
  const uint64_t tenant_id = arg.tenant_id_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.abort_redef_table(ObDDLTaskID(tenant_id, task_id)))) {
    LOG_WARN("cancel task failed", K(ret), K(tenant_id), K(task_id));
  }
  return ret;
}

int ObRootService::finish_redef_table(const obrpc::ObFinishRedefTableArg &arg)
{
  LOG_DEBUG("receive finish redef table arg", K(arg));
  int ret = OB_SUCCESS;
  const int64_t task_id = arg.task_id_;
  const uint64_t tenant_id = arg.tenant_id_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.finish_redef_table(ObDDLTaskID(tenant_id, task_id)))) {
    LOG_WARN("failed to finish redef table", K(ret), K(task_id), K(tenant_id));
  }
  return ret;
}

int ObRootService::copy_table_dependents(const obrpc::ObCopyTableDependentsArg &arg)
{
  LOG_INFO("receive copy table dependents arg", K(arg));
  int ret = OB_SUCCESS;
  const int64_t task_id = arg.task_id_;
  const uint64_t tenant_id = arg.tenant_id_;
  const bool is_copy_indexes = arg.copy_indexes_;
  const bool is_copy_triggers = arg.copy_triggers_;
  const bool is_copy_constraints = arg.copy_constraints_;
  const bool is_copy_foreign_keys = arg.copy_foreign_keys_;
  const bool is_ignore_errors = arg.ignore_errors_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.copy_table_dependents(ObDDLTaskID(tenant_id, task_id),
                                                          is_copy_constraints,
                                                          is_copy_indexes,
                                                          is_copy_triggers,
                                                          is_copy_foreign_keys,
                                                          is_ignore_errors))) {
    LOG_WARN("failed to copy table dependents", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::start_redef_table(const obrpc::ObStartRedefTableArg &arg, obrpc::ObStartRedefTableRes &res)
{
  LOG_DEBUG("receive start redef table arg", K(arg));
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.orig_tenant_id_;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.start_redef_table(arg, res))) {
    LOG_WARN("start redef table failed", K(ret));
  }
  return ret;
}

int ObRootService::recover_restore_table_ddl(const obrpc::ObRecoverRestoreTableDDLArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(arg.src_tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(arg));
  } else if (compat_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version 4.0 does not support this operation", K(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.recover_restore_table_ddl_task(arg))) {
    LOG_WARN("recover restore table ddl task failed", K(ret), K(arg));
  }
  LOG_INFO("recover restore table ddl finish", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_table(const obrpc::ObAlterTableArg &arg, obrpc::ObAlterTableRes &res)
{
  LOG_DEBUG("receive alter table arg", K(arg));
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = arg.alter_table_schema_.get_tenant_id();
  ObAlterTableArg &nonconst_arg = const_cast<ObAlterTableArg &>(arg);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(precheck_interval_part(arg))) {
    if (ret != OB_ERR_INTERVAL_PARTITION_EXIST) {
      LOG_WARN("fail to precheck_interval_part", K(arg), KR(ret));
    }
  } else {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(table_allow_ddl_operation(arg))) {
      LOG_WARN("table can't do ddl now", K(ret));
    } else if (nonconst_arg.is_add_to_scheduler_) {
      ObDDLTaskRecord task_record;
      ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
      ObDDLType ddl_type = ObDDLType::DDL_INVALID;
      const ObTableSchema *orig_table_schema = nullptr;
      schema_guard.set_session_id(arg.session_id_);
      if (obrpc::ObAlterTableArg::DROP_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_DROP_PARTITION;
      } else if (obrpc::ObAlterTableArg::DROP_SUB_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_DROP_SUB_PARTITION;
      } else if (obrpc::ObAlterTableArg::TRUNCATE_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_TRUNCATE_PARTITION;
      } else if (obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_TRUNCATE_SUB_PARTITION;
      } else if (obrpc::ObAlterTableArg::RENAME_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_RENAME_PARTITION;
      } else if (obrpc::ObAlterTableArg::RENAME_SUB_PARTITION == nonconst_arg.alter_part_type_) {
        ddl_type = ObDDLType::DDL_RENAME_SUB_PARTITION;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ddl type", K(ret), K(nonconst_arg.alter_part_type_), K(nonconst_arg));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                        nonconst_arg.alter_table_schema_.get_database_name(),
                                                        nonconst_arg.alter_table_schema_.get_origin_table_name(),
                                                        false  /* is_index*/,
                                                        orig_table_schema))) {
        LOG_WARN("fail to get and check table schema", K(ret));
      } else if (OB_ISNULL(orig_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(tenant_id), K(nonconst_arg.alter_table_schema_));
      } else {
        ObCreateDDLTaskParam param(tenant_id,
                                   ddl_type,
                                   nullptr,
                                   nullptr,
                                   orig_table_schema->get_table_id(),
                                   orig_table_schema->get_schema_version(),
                                   arg.parallelism_,
                                   arg.consumer_group_id_,
                                   &allocator,
                                   &arg,
                                   0 /*parent task id*/);
        if (OB_FAIL(ddl_scheduler_.create_ddl_task(param, sql_proxy_, task_record))) {
          LOG_WARN("submit ddl task failed", K(ret), K(arg));
        } else if (OB_FAIL(ddl_scheduler_.schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
        } else {
          res.ddl_type_ = ddl_type;
          res.task_id_ = task_record.task_id_;
        }
      }
    } else if (OB_FAIL(ddl_service_.alter_table(nonconst_arg, res))) {
      LOG_WARN("alter_user_table failed", K(arg), K(ret));
    } else {
      const ObSimpleTableSchemaV2 *simple_table_schema = NULL;
      // there are multiple DDL except alter table, ctas, comment on, eg.
      // but only alter_table specify table_id, so if no table_id, it indicates DDL is not alter table, skip.
      if (OB_INVALID_ID == arg.alter_table_schema_.get_table_id()) {
        // skip
      } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard in inner table failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, arg.alter_table_schema_.get_table_id(), simple_table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(arg.alter_table_schema_.get_table_id()));
      } else if (OB_ISNULL(simple_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("simple_table_schema is NULL ptr", K(ret), K(simple_table_schema), K(ret));
      } else {
        res.schema_version_ = simple_table_schema->get_schema_version();
      }
    }
  }
  return ret;
}

int ObRootService::create_index(const ObCreateIndexArg &arg, obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  LOG_DEBUG("receive create index arg", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObIndexBuilder index_builder(ddl_service_);
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(index_builder.create_index(arg, res))) {
      LOG_WARN("create_index failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::drop_table(const obrpc::ObDropTableArg &arg, obrpc::ObDDLRes &res)
{
  int ret = OB_SUCCESS;
  uint64_t target_object_id = OB_INVALID_ID;
  int64_t schema_version = OB_INVALID_SCHEMA_VERSION;
  bool need_add_to_ddl_scheduler = arg.is_add_to_scheduler_;
  const uint64_t tenant_id = arg.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret), K(arg));
  } else if (need_add_to_ddl_scheduler) {
    // to decide wherther to add to ddl scheduler.
    // 1. do not add to scheduler if all tables do not exist.
    // 2. do not add to scheduler if all existed tables are temporary tables.
    need_add_to_ddl_scheduler = arg.tables_.count() == 0 ? false : true;
    for (int64_t i = 0; OB_SUCC(ret) && need_add_to_ddl_scheduler && i < arg.tables_.count(); ++i) {
      int tmp_ret = OB_SUCCESS;
      const ObTableItem &table_item = arg.tables_.at(i);
      const ObTableSchema *table_schema = nullptr;
      if (OB_SUCCESS != (tmp_ret = ddl_service_.check_table_exists(tenant_id,
                                                                   table_item,
                                                                   arg.table_type_,
                                                                   schema_guard,
                                                                   &table_schema))) {
        LOG_INFO("check table exist failed, generate error msg in ddl service later", K(ret), K(tmp_ret));
      }
      if (OB_FAIL(ret)) {
      } else if (nullptr != table_schema) {
        if (table_schema->is_tmp_table()) {
          // do nothing.
        } else if (OB_INVALID_ID == target_object_id || OB_INVALID_SCHEMA_VERSION == schema_version) {
          // regard table_id, schema_version of the the first table as the tag to submit ddl task.
          target_object_id = table_schema->get_table_id();
          schema_version = table_schema->get_schema_version();
        }
      }
    }
    // all tables do not exist, or all existed tables are temporary tables.
    if (OB_INVALID_ID == target_object_id || OB_INVALID_SCHEMA_VERSION == schema_version) {
      need_add_to_ddl_scheduler = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_add_to_ddl_scheduler) {
    ObDDLTaskRecord task_record;
    ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
    ObCreateDDLTaskParam param(tenant_id,
                               ObDDLType::DDL_DROP_TABLE,
                               nullptr,
                               nullptr,
                               target_object_id,
                               schema_version,
                               arg.parallelism_,
                               arg.consumer_group_id_,
                               &allocator,
                               &arg,
                               0 /* parent task id*/);
    if (OB_UNLIKELY(OB_INVALID_ID == target_object_id || OB_INVALID_SCHEMA_VERSION == schema_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", K(ret), K(arg), K(target_object_id), K(schema_version));
    } else if (OB_FAIL(ddl_scheduler_.create_ddl_task(param, sql_proxy_, task_record))) {
      LOG_WARN("submit ddl task failed", K(ret), K(arg));
    } else if (OB_FAIL(ddl_scheduler_.schedule_ddl_task(task_record))) {
      LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
    } else {
      res.tenant_id_ = tenant_id;
      res.schema_id_ = target_object_id;
      res.task_id_ = task_record.task_id_;
    }
  } else if (OB_FAIL(ddl_service_.drop_table(arg, res))) {
    LOG_WARN("ddl service failed to drop table", K(ret), K(arg), K(res));
  }
  return ret;
}

int ObRootService::drop_database(const obrpc::ObDropDatabaseArg &arg, ObDropDatabaseRes &drop_database_res)
{
  int ret = OB_SUCCESS;
  uint64_t database_id = 0;
  int64_t schema_version = 0;
  bool need_add_to_scheduler = arg.is_add_to_scheduler_;
  const uint64_t tenant_id = arg.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (need_add_to_scheduler) {
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
      LOG_WARN("fail to get schema version", K(ret), K(arg));
    } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, arg.database_name_, database_id))) {
      LOG_WARN("fail to get database id");
    } else if (OB_INVALID_ID == database_id) {
      // drop database if exists xxx.
      need_add_to_scheduler = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_add_to_scheduler) {
    ObDDLTaskRecord task_record;
    ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
    ObCreateDDLTaskParam param(tenant_id,
                                ObDDLType::DDL_DROP_DATABASE,
                                nullptr,
                                nullptr,
                                database_id,
                                schema_version,
                                arg.parallelism_,
                                arg.consumer_group_id_,
                                &allocator,
                                &arg,
                                0 /* parent task id*/);
    if (OB_FAIL(ddl_scheduler_.create_ddl_task(param, sql_proxy_, task_record))) {
      LOG_WARN("submit ddl task failed", K(ret), K(arg));
    } else if (OB_FAIL(ddl_scheduler_.schedule_ddl_task(task_record))) {
      LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
    } else {
      drop_database_res.ddl_res_.tenant_id_ = tenant_id;
      drop_database_res.ddl_res_.schema_id_ = database_id;
      drop_database_res.ddl_res_.task_id_ = task_record.task_id_;
    }
  } else if (OB_FAIL(ddl_service_.drop_database(arg, drop_database_res))) {
    LOG_WARN("ddl_service_ drop_database failed", K(arg), K(ret));
  }
  return ret;
}


int ObRootService::drop_tablegroup(const obrpc::ObDropTablegroupArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_tablegroup(arg))) {
    LOG_WARN("ddl_service_ drop_tablegroup failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::alter_tablegroup(const obrpc::ObAlterTablegroupArg &arg)
{
  LOG_DEBUG("receive alter tablegroup arg", K(arg));
  const ObTablegroupSchema *tablegroup_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  uint64_t tablegroup_id = OB_INVALID_ID;
  const uint64_t tenant_id = arg.tenant_id_;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id,
                                                    arg.tablegroup_name_,
                                                    tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("get invalid tablegroup schema", KR(ret), K(arg));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tenant_id), K(ret));
  } else if (OB_ISNULL(tablegroup_schema)) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("get invalid tablegroup schema", K(ret));
  } else if (tablegroup_schema->is_in_splitting()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tablegroup is splitting, refuse to alter now", K(ret), K(tablegroup_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "tablegroup is splitting, alter tablegroup");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ddl_service_.alter_tablegroup(arg))) {
    LOG_WARN("ddl_service_ alter tablegroup failed", K(arg), K(ret));
  } else {
  }
  return ret;
}

int ObRootService::drop_index(const obrpc::ObDropIndexArg &arg, obrpc::ObDropIndexRes &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObIndexBuilder index_builder(ddl_service_);
    if (OB_FAIL(index_builder.drop_index(arg, res))) {
      LOG_WARN("index_builder drop_index failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::rebuild_index(const obrpc::ObRebuildIndexArg &arg, obrpc::ObAlterTableRes &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.rebuild_index(arg, res))) {
    LOG_WARN("ddl_service rebuild index failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::flashback_index(const ObFlashBackIndexArg &arg) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.flashback_index(arg))) {
    LOG_WARN("failed to flashback index", K(ret));
  }

  return ret;
}

int ObRootService::purge_index(const ObPurgeIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_index(arg))) {
    LOG_WARN("failed to purge index", K(ret));
  }

  return ret;
}

int ObRootService::rename_table(const obrpc::ObRenameTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.rename_table(arg))){
    LOG_WARN("rename table failed", K(ret));
  }
  return ret;
}

int ObRootService::truncate_table(const obrpc::ObTruncateTableArg &arg, obrpc::ObDDLRes &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    SCN frozen_scn;
    if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(arg.tenant_id_, frozen_scn))) {
      LOG_WARN("get_frozen_scn failed", K(ret));
    } else if (arg.is_add_to_scheduler_) {
      ObDDLTaskRecord task_record;
      ObArenaAllocator allocator(lib::ObLabel("DdlTaskTmp"));
      ObSchemaGetterGuard schema_guard;
      const ObTableSchema *table_schema = nullptr;
      const uint64_t tenant_id = arg.tenant_id_;
      if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard in inner table failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, arg.database_name_,
                                                       arg.table_name_, false /* is_index */,
                                                       table_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(arg));
      } else {
        ObCreateDDLTaskParam param(tenant_id,
                                   ObDDLType::DDL_TRUNCATE_TABLE,
                                   nullptr,
                                   nullptr,
                                   table_schema->get_table_id(),
                                   table_schema->get_schema_version(),
                                   arg.parallelism_,
                                   arg.consumer_group_id_,
                                   &allocator,
                                   &arg,
                                   0 /* parent task id*/);
        if (OB_FAIL(GCTX.root_service_->get_ddl_scheduler().create_ddl_task(param, sql_proxy_, task_record))) {
          LOG_WARN("submit ddl task failed", K(ret), K(arg));
        } else if (OB_FAIL(ddl_scheduler_.schedule_ddl_task(task_record))) {
          LOG_WARN("fail to schedule ddl task", K(ret), K(task_record));
        } else {
          res.tenant_id_ = tenant_id;
          res.schema_id_ = table_schema->get_table_id();
          res.task_id_ = task_record.task_id_;
        }
      }
    } else if (OB_FAIL(ddl_service_.truncate_table(arg, res, frozen_scn))) {
      LOG_WARN("ddl service failed to truncate table", K(arg), K(ret), K(frozen_scn));
    }
  }
  return ret;
}

/*
 * new parallel truncate table
 */
int ObRootService::truncate_table_v2(const obrpc::ObTruncateTableArg &arg, obrpc::ObDDLRes &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    SCN frozen_scn;
    if (OB_FAIL(ObMajorFreezeHelper::get_frozen_scn(arg.tenant_id_, frozen_scn))) {
      LOG_WARN("get_frozen_scn failed", K(ret));
    } else if (OB_FAIL(ddl_service_.new_truncate_table(arg, res, frozen_scn))) {
      LOG_WARN("ddl service failed to truncate table", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::create_table_like(const ObCreateTableLikeArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    if (OB_FAIL(ddl_service_.create_table_like(arg))) {
      if (OB_ERR_TABLE_EXIST == ret) {
        //create table xx if not exist like
        if (arg.if_not_exist_) {
          LOG_USER_NOTE(OB_ERR_TABLE_EXIST,
                        arg.new_table_name_.length(), arg.new_table_name_.ptr());
          LOG_WARN("table is exist, no need to create again", K(arg), K(ret));
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_TABLE_EXIST;
          LOG_USER_ERROR(OB_ERR_TABLE_EXIST, arg.new_table_name_.length(), arg.new_table_name_.ptr());
          LOG_WARN("table is exist, cannot create it twice", K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * recyclebin related
 */
int ObRootService::flashback_table_from_recyclebin(const ObFlashBackTableFromRecyclebinArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.flashback_table_from_recyclebin(arg))) {
    LOG_WARN("failed to flash back table", K(ret));
  }
  return ret;
}

int ObRootService::flashback_table_to_time_point(const obrpc::ObFlashBackTableToScnArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive flashback table arg", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.flashback_table_to_time_point(arg))) {
    LOG_WARN("failed to flash back table", K(ret));
  }
  return ret;
}

int ObRootService::purge_table(const ObPurgeTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_table(arg))) {
    LOG_WARN("failed to purge table", K(ret));
  }
  return ret;
}

int ObRootService::flashback_database(const ObFlashBackDatabaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.flashback_database(arg))) {
    LOG_WARN("failed to flash back database", K(ret));
  }
  return ret;
}

int ObRootService::purge_database(const ObPurgeDatabaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_database(arg))) {
    LOG_WARN("failed to purge database", K(ret));
  }
  return ret;
}

int ObRootService::purge_expire_recycle_objects(const ObPurgeRecycleBinArg &arg, Int64 &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t purged_objects = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.purge_tenant_expire_recycle_objects(arg, purged_objects))) {
    LOG_WARN("failed to purge expire recyclebin objects", K(ret), K(arg));
  } else {
    affected_rows = purged_objects;
  }
  return ret;
}

int ObRootService::optimize_table(const ObOptimizeTableArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  LOG_INFO("receive optimize table request", K(arg));
  lib::Worker::CompatMode mode;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(arg.tenant_id_, mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    const int64_t all_core_table_id = OB_ALL_CORE_TABLE_TID;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tables_.count(); ++i) {
      SMART_VAR(obrpc::ObAlterTableArg, alter_table_arg) {
        ObSqlString sql;
        const obrpc::ObTableItem &table_item = arg.tables_.at(i);
        const ObTableSchema *table_schema = nullptr;
        alter_table_arg.is_alter_options_ = true;
        alter_table_arg.alter_table_schema_.set_origin_database_name(table_item.database_name_);
        alter_table_arg.alter_table_schema_.set_origin_table_name(table_item.table_name_);
        alter_table_arg.alter_table_schema_.set_tenant_id(arg.tenant_id_);
        alter_table_arg.skip_sys_table_check_ = true;
        //exec_tenant_id_ is used in standby cluster
        alter_table_arg.exec_tenant_id_ = arg.exec_tenant_id_;
        if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
          LOG_WARN("fail to get tenant schema guard", K(ret));
        } else if (OB_FAIL(schema_guard.get_table_schema(arg.tenant_id_, table_item.database_name_, table_item.table_name_, false/*is index*/, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret));
        } else if (nullptr == table_schema) {
          // skip deleted table
        } else if (all_core_table_id == table_schema->get_table_id()) {
          // do nothing
        } else {
          if (lib::Worker::CompatMode::MYSQL == mode) {
            if (OB_FAIL(sql.append_fmt("OPTIMIZE TABLE `%.*s`",
                table_item.table_name_.length(), table_item.table_name_.ptr()))) {
              LOG_WARN("fail to assign sql stmt", K(ret));
            }
          } else if (lib::Worker::CompatMode::ORACLE == mode) {
            if (OB_FAIL(sql.append_fmt("ALTER TABLE \"%.*s\" SHRINK SPACE",
                table_item.table_name_.length(), table_item.table_name_.ptr()))) {
              LOG_WARN("fail to append fmt", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, unknown mode", K(ret), K(mode));
          }
          if (OB_SUCC(ret)) {
            alter_table_arg.ddl_stmt_str_ = sql.string();
            obrpc::ObAlterTableRes res;
            if (OB_FAIL(alter_table_arg.alter_table_schema_.alter_option_bitset_.add_member(ObAlterTableArg::PROGRESSIVE_MERGE_ROUND))) {
              LOG_WARN("fail to add member", K(ret));
            } else if (OB_FAIL(alter_table(alter_table_arg, res))) {
              LOG_WARN("fail to alter table", K(ret), K(alter_table_arg));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRootService::calc_column_checksum_repsonse(const obrpc::ObCalcColumnChecksumResponseArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.on_column_checksum_calc_reply(
          arg.tablet_id_, ObDDLTaskKey(arg.tenant_id_, arg.target_table_id_, arg.schema_version_), arg.ret_code_))) {
    LOG_WARN("handle column checksum calc response failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::refresh_config()
{
  int ret = OB_SUCCESS;
  int64_t local_config_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_manager_.get_config_version(local_config_version))) {
    LOG_WARN("get_config_version failed", K(ret));
  } else {
    LOG_INFO("receive refresh config");
    const int64_t now = ObTimeUtility::current_time();
    const int64_t new_config_version = max(local_config_version + 1, now);
    if (OB_FAIL(zone_manager_.update_config_version(new_config_version))) {
      LOG_WARN("update_config_version failed", K(new_config_version), K(ret));
    } else if (OB_FAIL(config_mgr_->got_version(new_config_version))) {
      LOG_WARN("got_version failed", K(new_config_version), K(ret));
    } else {
      LOG_INFO("root service refresh_config succeed", K(new_config_version));
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "refresh_config", K(ret));
  return ret;
}

int ObRootService::root_minor_freeze(const ObRootMinorFreezeArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive minor freeze request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(root_minor_freeze_.try_minor_freeze(arg))) {
    LOG_WARN("minor freeze failed", K(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "root_minor_freeze", K(ret), K(arg));
  return ret;
}

int ObRootService::update_index_status(const obrpc::ObUpdateIndexStatusArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.update_index_status(arg))) {
    LOG_WARN("update index table status failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::init_debug_database()
{
  const schema_create_func *creator_ptr_array[] = {
    core_table_schema_creators,
    sys_table_schema_creators,
    NULL};

  int ret = OB_SUCCESS;
  HEAP_VAR(char[OB_MAX_SQL_LENGTH], sql) {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    }

    ObTableSchema table_schema;
    ObSqlString create_func_sql;
    ObSqlString del_sql;
    for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
         OB_SUCCESS == ret && NULL != *creator_ptr_ptr; ++creator_ptr_ptr) {
      for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
           OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
        table_schema.reset();
        create_func_sql.reset();
        del_sql.reset();
        if (OB_FAIL((*creator_ptr)(table_schema))) {
          LOG_WARN("create table schema failed", K(ret));
          ret = OB_SCHEMA_ERROR;
        } else {
          int64_t affected_rows = 0;
          // ignore create function result
          int temp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (temp_ret = create_func_sql.assign(
                      "create function time_to_usec(t timestamp) "
                      "returns bigint(20) deterministic begin return unix_timestamp(t); end;"))) {
            LOG_WARN("create_func_sql assign failed", K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = sql_proxy_.write(
                      create_func_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(create_func_sql), K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = create_func_sql.assign(
                      "create function usec_to_time(u bigint(20)) "
                      "returns timestamp deterministic begin return from_unixtime(u); end;"))) {
            LOG_WARN("create_func_sql assign failed", K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = sql_proxy_.write(
                      create_func_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(create_func_sql), K(temp_ret));
          }

          memset(sql, 0, sizeof(sql));
          if (OB_FAIL(del_sql.assign_fmt(
                      "DROP table IF EXISTS %s", table_schema.get_table_name()))) {
            LOG_WARN("assign sql failed", K(ret));
          } else if (OB_FAIL(sql_proxy_.write(del_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(ret));
          } else if (OB_FAIL(ObSchema2DDLSql::convert(
                      table_schema, sql, sizeof(sql)))) {
            LOG_WARN("convert table schema to create table sql failed", K(ret));
          } else if (OB_FAIL(sql_proxy_.write(sql, affected_rows))) {
            LOG_WARN("execute sql failed", K(ret), K(sql));
          }
        }
      }
    }

    LOG_INFO("init debug database finish.", K(ret));
  }
  return ret;
}

int ObRootService::do_restart()
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = OB_SYS_TENANT_ID;
  SpinWLockGuard rs_list_guard(broadcast_rs_list_lock_);

  // NOTE: following log print after lock
  FLOG_INFO("[ROOTSERVICE_NOTICE] start do_restart");

  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("not init", KR(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver()) {
    ret = OB_NOT_MASTER;
    FLOG_WARN("not master", KR(ret));
  }

  // renew master rootservice, ignore error
  if (OB_SUCC(ret)) {
    int tmp_ret = rs_mgr_->renew_master_rootserver();
    if (OB_SUCCESS != tmp_ret) {
      FLOG_WARN("renew master rootservice failed", KR(tmp_ret));
    }
  }

  //fetch root partition info
  if (FAILEDx(fetch_sys_tenant_ls_info())) {
    FLOG_WARN("fetch root partition info failed", KR(ret));
  } else {
    FLOG_INFO("fetch root partition info succeed", KR(ret));
  }

  // broadcast root server address, ignore error
  if (OB_SUCC(ret)) {
    int tmp_ret = update_rslist();
    if (OB_SUCCESS != tmp_ret) {
      FLOG_WARN("failed to update rslist but ignored", KR(tmp_ret));
    }
  }

  if (OB_SUCC(ret)) {
    //standby cluster trigger load_refresh_schema_status by heartbeat.
    //due to switchover, primary cluster need to load schema_status too.
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      FLOG_WARN("schema_status_proxy is null", KR(ret));
    } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
      FLOG_WARN("fail to load refresh schema status", KR(ret));
    } else {
      FLOG_INFO("load schema status success");
    }
  }

  bool load_frozen_status = true;
  const bool refresh_server_need_retry = false; // no need retry
  // try fast recover
  if (OB_SUCC(ret)) {
    int tmp_ret = refresh_server(load_frozen_status, refresh_server_need_retry);
    if (OB_SUCCESS != tmp_ret) {
      FLOG_WARN("refresh server failed", KR(tmp_ret), K(load_frozen_status));
    }
    tmp_ret = refresh_schema(load_frozen_status);
    if (OB_SUCCESS != tmp_ret) {
      FLOG_WARN("refresh schema failed", KR(tmp_ret), K(load_frozen_status));
    }
  }
  load_frozen_status = false;
  // refresh schema
  if (FAILEDx(refresh_schema(load_frozen_status))) {
    FLOG_WARN("refresh schema failed", KR(ret), K(load_frozen_status));
  } else {
    FLOG_INFO("success to refresh schema", K(load_frozen_status));
  }

  // refresh server manager
  if (FAILEDx(refresh_server(load_frozen_status, refresh_server_need_retry))) {
    FLOG_WARN("refresh server failed", KR(ret), K(load_frozen_status));
  } else {
    FLOG_INFO("success to refresh server", K(load_frozen_status));
  }

  // add other reload logic here
  if (FAILEDx(zone_manager_.reload())) {
    FLOG_WARN("zone_manager_ reload failed", KR(ret));
  } else {
    FLOG_INFO("success to reload zone_manager_");
  }

  // start timer tasks
  if (FAILEDx(start_timer_tasks())) {
    FLOG_WARN("start timer tasks failed", KR(ret));
  } else {
    FLOG_INFO("success to start timer tasks");
  }

  DEBUG_SYNC(BEFORE_UNIT_MANAGER_LOAD);
  if (FAILEDx(unit_manager_.load())) {
    FLOG_WARN("unit_manager_ load failed", KR(ret));
  } else {
    FLOG_INFO("load unit_manager success");
  }

  /*
   * FIXME: wanhong.wwh: need re-implement
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_core_table_unit_id())) {
      LOG_WARN("START_SERVICE: set core table partition unit failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: set core table unit id success");
    }
  }
  */

  if (FAILEDx(schema_history_recycler_.start())) {
    FLOG_WARN("schema_history_recycler start failed", KR(ret));
  } else {
    FLOG_INFO("success to start schema_history_recycler");
  }

  if (FAILEDx(root_balancer_.start())) {
    FLOG_WARN("root balancer start failed", KR(ret));
  } else {
    FLOG_INFO("success to start root balancer");
  }

  if (FAILEDx(thread_checker_.start())) {
    FLOG_WARN("rs_monitor_check: start thread checker failed", KR(ret));
  } else {
    FLOG_INFO("success to start thread checker");
  }

  if (FAILEDx(empty_server_checker_.start())) {
    FLOG_WARN("start empty server checker failed", KR(ret));
  } else {
    FLOG_INFO("success to start empty server checker");
  }

  if (FAILEDx(lost_replica_checker_.start())) {
    FLOG_WARN("start lost replica checker failed", KR(ret));
  } else {
    FLOG_INFO("start lost replica checker success");
  }

  // broadcast root server address again, this task must be in the end part of do_restart,
  // because system may work properly without it.
  if (FAILEDx(update_rslist())) {
    FLOG_WARN("broadcast root address failed but ignored", KR(ret));
    // it's ok ret be overwritten, update_rslist_task will retry until succeed
    if (OB_FAIL(submit_update_rslist_task(true))) {
      FLOG_WARN("submit_update_rslist_task failed", KR(ret));
    } else {
      FLOG_INFO("submit_update_rslist_task succeed");
    }
  } else {
    FLOG_INFO("broadcast root address succeed");
  }

  if (FAILEDx(report_single_replica(tenant_id, SYS_LS))) {
    FLOG_WARN("report all_core_table replica failed, but ignore",
              KR(ret), K(tenant_id), K(SYS_LS));
    // it's ok ret be overwritten, report single all_core will retry until succeed
    if (OB_FAIL(submit_report_core_table_replica_task())) {
      FLOG_WARN("submit all core table replica task failed", KR(ret));
    } else {
      FLOG_INFO("submit all core table replica task succeed");
    }
  } else {
    FLOG_INFO("report all_core_table replica finish");
  }

  if (FAILEDx(ddl_scheduler_.start())) {
    FLOG_WARN("fail to start ddl task scheduler", KR(ret));
  } else {
    FLOG_INFO("success to start ddl task scheduler");
  }

  if (FAILEDx(dbms_job::ObDBMSJobMaster::get_instance().start())) {
    FLOG_WARN("failed to start dbms job master", KR(ret));
  } else {
    FLOG_INFO("success to start dbms job master");
  }

  if (FAILEDx(dbms_scheduler::ObDBMSSchedJobMaster::get_instance().start())) {
    FLOG_WARN("failed to start dbms sched job master", KR(ret));
  } else {
    FLOG_INFO("success to start dbms sched job mstart");
  }

  if (OB_SUCC(ret)) {
    upgrade_executor_.start();
    FLOG_INFO("success to start upgrade_executor_", KR(ret));
    upgrade_storage_format_executor_.start();
    FLOG_INFO("success to start upgrade_storage_format_executor_", KR(ret));
    create_inner_schema_executor_.start();
    FLOG_INFO("success to start create_inner_schema_executor_", KR(ret));
  }

  // to avoid increase rootservice_epoch while fail to restart RS,
  // put it and the end of restart RS.
  if (FAILEDx(init_sequence_id())) {
    FLOG_WARN("fail to init sequence id", KR(ret));
  } else {
    FLOG_INFO("success to init sequenxe id");
  }


#ifdef OB_BUILD_TDE_SECURITY
  if (FAILEDx(master_key_mgr_.start())) {
    FLOG_WARN("fail to start master key manager", KR(ret));
  } else {
    FLOG_INFO("success to start master key manager");
  }
#endif

  if (FAILEDx(disaster_recovery_task_mgr_.start())) {
    FLOG_WARN("disaster recovery task manager start failed", KR(ret));
  } else {
    FLOG_INFO("success to start disaster recovery task manager");
  }

  if (FAILEDx(rs_status_.set_rs_status(status::FULL_SERVICE))) {
    FLOG_WARN("fail to set rs status", KR(ret));
  } else {
    FLOG_INFO("full_service !!! start to work!!");
    ROOTSERVICE_EVENT_ADD("root_service", "full_rootservice",
                          "result", ret, K_(self_addr));
    root_balancer_.set_active();
    root_minor_freeze_.start();
    FLOG_INFO("root_minor_freeze_ started");
    root_inspection_.start();
    FLOG_INFO("root_inspection_ started");
    int64_t now = ObTimeUtility::current_time();
    core_meta_table_version_ = now;
    EVENT_SET(RS_START_SERVICE_TIME, now);
    // reset fail count for self checker and print log.
    reset_fail_count();
  }

  if (OB_FAIL(ret)) {
    update_fail_count(ret);
  }

  FLOG_INFO("[ROOTSERVICE_NOTICE] finish do_restart", KR(ret));
  return ret;
}

bool ObRootService::in_service() const
{
  return rs_status_.in_service();
}

bool ObRootService::is_full_service() const
{
  return rs_status_.is_full_service();
}

bool ObRootService::is_start() const
{
  return rs_status_.is_start();
}

bool ObRootService::is_stopping() const
{
  return rs_status_.is_stopping();
}

bool ObRootService::is_need_stop() const
{
  return rs_status_.is_need_stop();
}

bool ObRootService::can_start_service() const
{
  return rs_status_.can_start_service();
}

int ObRootService::set_rs_status(const status::ObRootServiceStatus status)
{
  return rs_status_.set_rs_status(status);
}

bool ObRootService::need_do_restart() const
{
  return rs_status_.need_do_restart();
}

int ObRootService::revoke_rs()
{
  return rs_status_.revoke_rs();
}
int ObRootService::check_parallel_ddl_conflict(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObDDLArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;

  if (arg.is_need_check_based_schema_objects()) {
    for (int64_t i = 0; OB_SUCC(ret) && (i < arg.based_schema_object_infos_.count()); ++i) {
      const ObBasedSchemaObjectInfo &info = arg.based_schema_object_infos_.at(i);
      if (OB_FAIL(schema_guard.get_schema_version(
          info.schema_type_,
          info.schema_tenant_id_ == OB_INVALID_TENANT_ID ? arg.exec_tenant_id_: info.schema_tenant_id_,
          info.schema_id_,
          schema_version))) {
        LOG_WARN("failed to get_schema_version", K(ret), K(arg.exec_tenant_id_), K(info));
      } else if (OB_INVALID_VERSION == schema_version) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("schema_version is OB_INVALID_VERSION", K(ret), K(info));
      } else if (schema_version != info.schema_version_) {
        ret = OB_ERR_PARALLEL_DDL_CONFLICT;
        LOG_WARN("schema_version is not equal to info.schema_version_", K(ret), K(schema_version), K(info));
      }
    }
  }

  return ret;
}

int ObRootService::init_sequence_id()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), K(schema_service_));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("trans start failed", K(ret));
  } else {
    ObGlobalStatProxy proxy(trans, OB_SYS_TENANT_ID);
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    int64_t rootservice_epoch = 0;
    int64_t schema_version = OB_INVALID_VERSION;
    ObRefreshSchemaInfo schema_info;
    //increase sequence_id can trigger every observer refresh schema while restart RS
    if (OB_FAIL(proxy.inc_rootservice_epoch())) {
      LOG_WARN("fail to increase rootservice_epoch", K(ret));
    } else if (OB_FAIL(proxy.get_rootservice_epoch(rootservice_epoch))) {
      LOG_WARN("fail to get rootservice start times", K(ret), K(rootservice_epoch));
    } else if (rootservice_epoch <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rootservice_epoch", K(ret), K(rootservice_epoch));
    } else if (OB_FAIL(schema_service->init_sequence_id(rootservice_epoch))) {
      LOG_WARN("init sequence id failed", K(ret), K(rootservice_epoch));
    } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, schema_version))) {
      LOG_WARN("fail to get sys tenant refreshed schema version", K(ret));
    } else if (schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
      // in bootstrap and new schema mode, to avoid write failure while schema_version change,
      // only actively refresh schema at the end of bootstrap, and make heartbeat'srefresh_schema_info effective.
    } else if (OB_FAIL(schema_service->set_refresh_schema_info(schema_info))) {
      LOG_WARN("fail to set refresh schema info", K(ret), K(schema_info));
    }

    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObRootService::load_server_manager()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.load_server_manager())) {
    LOG_WARN("fail to load server manager", K(ret));
  } else {} // no more to do
  return ret;
}

ERRSIM_POINT_DEF(ERROR_EVENT_TABLE_CLEAR_INTERVAL);
int ObRootService::start_timer_tasks()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  if (OB_SUCCESS == ret && !task_queue_.exist_timer_task(event_table_clear_task_)) {
    const int64_t delay = ERROR_EVENT_TABLE_CLEAR_INTERVAL ? 10 * 1000 * 1000 :
      ObEventHistoryTableOperator::EVENT_TABLE_CLEAR_INTERVAL;
    if (OB_FAIL(task_queue_.add_repeat_timer_task_schedule_immediately(event_table_clear_task_, delay))) {
      LOG_WARN("start event table clear task failed", K(delay), K(ret));
    } else {
      LOG_INFO("added event_table_clear_task", K(delay));
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(update_rs_list_timer_task_)) {
    if (OB_FAIL(schedule_update_rs_list_task())) {
      LOG_WARN("failed to schedule update rs list task", K(ret));
    } else {
      LOG_INFO("add update rs list timer task");
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(update_all_server_config_task_)) {
    if (OB_FAIL(schedule_update_all_server_config_task())) {
      LOG_WARN("fail to schedule update_all_server_config_task", KR(ret));
    } else {
      LOG_INFO("add update_all_server_config_task");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_inspector_task())) {
      LOG_WARN("start inspector fail", K(ret));
    } else {
      LOG_INFO("start inspector success");
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(check_server_task_)) {
    if (OB_FAIL(schedule_check_server_timer_task())) {
      LOG_WARN("start all server checker fail", K(ret));
    } else {
      LOG_INFO("start all server checker success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_load_ddl_task())) {
      LOG_WARN("schedule load ddl task failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_refresh_io_calibration_task())) {
      LOG_WARN("schedule refresh io calibration task failed", K(ret));
    }
  }

  LOG_INFO("start all timer tasks finish", K(ret));
  return ret;
}

int ObRootService::stop_timer_tasks()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    task_queue_.cancel_timer_task(restart_task_);
    task_queue_.cancel_timer_task(check_server_task_);
    task_queue_.cancel_timer_task(event_table_clear_task_);
    task_queue_.cancel_timer_task(self_check_task_);
    task_queue_.cancel_timer_task(update_rs_list_timer_task_);
    task_queue_.cancel_timer_task(update_all_server_config_task_);
    inspect_task_queue_.cancel_timer_task(inspector_task_);
    inspect_task_queue_.cancel_timer_task(purge_recyclebin_task_);
  }

  //stop other timer tasks here
  LOG_INFO("stop all timer tasks finish", K(ret));
  return ret;
}

int ObRootService::fetch_sys_tenant_ls_info()
{
  int ret = OB_SUCCESS;
  ObLSReplica replica;
  ObRole role = FOLLOWER;
  bool inner_table_only = false;
  ObMemberList member_list;
  // TODO: automatically decide to use rpc or inmemory
  ObLSTable* inmemory_ls;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.ob_service_) || OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", KR(ret));
  } else {
    inmemory_ls = GCTX.lst_operator_->get_inmemory_ls();
    if (OB_ISNULL(inmemory_ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inmemory ls_table is null", KR(ret), KP(inmemory_ls));
    } else if (OB_FAIL(inmemory_ls->get_role(
          OB_SYS_TENANT_ID,
          SYS_LS,
          role))) {
      LOG_WARN("get role from ObLS failed", KR(ret));
    } else if (OB_FAIL(inmemory_ls->get_member_list(
          OB_SYS_TENANT_ID,
          SYS_LS,
          member_list))) {
      LOG_WARN("get sys_tenant ls member list failed", KR(ret));
    } else if (!is_strong_leader(role)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("local role should be leader now", KR(ret), K(role));
    } else if (OB_FAIL(GCTX.ob_service_->fill_ls_replica(
          OB_SYS_TENANT_ID,
          SYS_LS,
          replica))) {
      LOG_WARN("fail to fill log stream replica", KR(ret), K(replica));
    } else if (OB_FAIL(inmemory_ls->update(replica, inner_table_only))) {
      LOG_WARN("fail to update ls replica", KR(ret), K(replica));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); i++) {
    ObAddr addr;
    if (OB_FAIL(member_list.get_server_by_index(i, addr))) {
      LOG_WARN("fail to get server", KR(ret), K(i), K(member_list));
    } else if (self_addr_ == addr){
      continue;
    } else {
      replica.reset();
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = rpc_proxy_.to(addr).timeout(config_->rpc_timeout)
                         .by(OB_SYS_TENANT_ID).fetch_sys_ls(replica))) {
        LOG_WARN("fetch sys_ls failed", K(temp_ret), "server", addr);
      } else if (replica.is_strong_leader()) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("role should be follower", KR(ret), K(replica));
      } else if (OB_FAIL(inmemory_ls->update(replica, inner_table_only))) {
        LOG_WARN("update sys_ls info failed", KR(ret), K(replica));
      } else {
        LOG_INFO("update sys_tenant ls replica succeed", K(replica), "server", addr);
      }
    }
  }
  return ret;
}

int ObRootService::not_implement()
{
  int ret = OB_NOT_IMPLEMENT;
  bt("not implement");
  LOG_WARN("rpc not implemented", K(ret));
  return ret;
}

ObRootService::ObRestartTask::ObRestartTask(ObRootService &root_service)
:ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service)
{
  set_retry_times(0);  // don't retry when failed
}

ObRootService::ObRestartTask::~ObRestartTask()
{
}

int ObRootService::ObRestartTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("after_restart task begin to process");
  if (OB_FAIL(root_service_.after_restart())) {
    LOG_WARN("root service after restart failed", K(ret));
  }
  FLOG_INFO("after_restart task process finish", KR(ret));
  return ret;
}

ObAsyncTask *ObRootService::ObRestartTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObRestartTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObRestartTask(root_service_);
  }
  return task;
}

ObRootService::ObRefreshServerTask::ObRefreshServerTask(ObRootService &root_service)
:ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObRootService::ObRefreshServerTask::process()
{
  int ret = OB_SUCCESS;
  const bool load_frozen_status = true;
  const bool need_retry = true;
  FLOG_INFO("refresh server task process");
  ObLatchRGuard guard(root_service_.bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);
  if (OB_FAIL(root_service_.refresh_server(load_frozen_status, need_retry))) {
    FLOG_WARN("refresh server failed", K(ret), K(load_frozen_status));
  } else {}
  return ret;
}

ObAsyncTask *ObRootService::ObRefreshServerTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObRefreshServerTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObRefreshServerTask(root_service_);
  }
  return task;
}

//-----Functions for managing privileges------
int ObRootService::create_user(obrpc::ObCreateUserArg &arg,
                               common::ObSArray<int64_t> &failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.create_user(arg, failed_index))){
    LOG_WARN("create user failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::drop_user(const ObDropUserArg &arg,
                             common::ObSArray<int64_t> &failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_user(arg, failed_index))) {
    LOG_WARN("drop user failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::rename_user(const obrpc::ObRenameUserArg &arg,
                               common::ObSArray<int64_t> &failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.rename_user(arg, failed_index))){
    LOG_WARN("rename user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::alter_role(const obrpc::ObAlterRoleArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if(!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_role(arg))) {
    LOG_WARN("alter role failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::set_passwd(const obrpc::ObSetPasswdArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.set_passwd(arg))){
    LOG_WARN("set passwd failed",  K(arg), K(ret));
  }
  return ret;
}

int ObRootService::grant(const ObGrantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    if (OB_FAIL(ddl_service_.grant(arg))) {
      LOG_WARN("Grant user failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::revoke_user(const ObRevokeUserArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.revoke(arg))) {
    LOG_WARN("revoke privilege failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::lock_user(const ObLockUserArg &arg, ObSArray<int64_t> &failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.lock_user(arg, failed_index))){
    LOG_WARN("lock user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::alter_user_profile(const ObAlterUserProfileArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_user_profile(arg))){
    LOG_WARN("lock user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::create_directory(const obrpc::ObCreateDirectoryArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.create_directory(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("create directory failed", K(arg.schema_), K(ret));
  }
  return ret;
}

int ObRootService::drop_directory(const obrpc::ObDropDirectoryArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.drop_directory(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("drop directory failed", K(arg.directory_name_), K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// row level security
////////////////////////////////////////////////////////////////

int ObRootService::handle_rls_policy_ddl(const obrpc::ObRlsPolicyDDLArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dbms_rls");
  } else if (OB_FAIL(ddl_service_.handle_rls_policy_ddl(arg))) {
    LOG_WARN("do rls policy ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_rls_group_ddl(const obrpc::ObRlsGroupDDLArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dbms_rls");
  } else if (OB_FAIL(ddl_service_.handle_rls_group_ddl(arg))) {
    LOG_WARN("do rls group ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_rls_context_ddl(const obrpc::ObRlsContextDDLArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(arg.exec_tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dbms_rls");
  } else if (OB_FAIL(ddl_service_.handle_rls_context_ddl(arg))) {
    LOG_WARN("do rls context ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::revoke_database(const ObRevokeDBArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObOriginalDBKey db_key(arg.tenant_id_, arg.user_id_, arg.db_);
    if (OB_FAIL(ddl_service_.revoke_database(db_key, arg.priv_set_))) {
      LOG_WARN("Revoke db failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::revoke_table(const ObRevokeTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObTablePrivSortKey table_priv_key(arg.tenant_id_, arg.user_id_, arg.db_, arg.table_);
    ObObjPrivSortKey obj_priv_key(arg.tenant_id_,
                                  arg.obj_id_,
                                  arg.obj_type_,
                                  OBJ_LEVEL_FOR_TAB_PRIV,
                                  arg.grantor_id_,
                                  arg.user_id_);
    OZ (ddl_service_.revoke_table(table_priv_key,
                                  arg.priv_set_,
                                  obj_priv_key,
                                  arg.obj_priv_array_,
                                  arg.revoke_all_ora_));
  }
  return ret;
}

int ObRootService::revoke_syspriv(const ObRevokeSysPrivArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.revoke_syspriv(arg.tenant_id_,
                                                 arg.grantee_id_,
                                                 arg.sys_priv_array_,
                                                 arg.role_ids_,
                                                 &arg.ddl_stmt_str_))) {
    LOG_WARN("revoke privilege failed", K(ret), K(arg));
  }
  return ret;
}

//-----End of functions for managing privileges-----

//-----Functions for managing outlines-----
int ObRootService::create_outline(const ObCreateOutlineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObOutlineInfo outline_info = arg.outline_info_;
    const bool is_or_replace = arg.or_replace_;
    uint64_t tenant_id = outline_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (database_name == OB_OUTLINE_DEFAULT_DATABASE_NAME) {
      // if not specify database, set default database name and database id;
      outline_info.set_database_id(OB_OUTLINE_DEFAULT_DATABASE_ID);
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create outline of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      outline_info.set_database_id(db_schema->get_database_id());
    }

    bool is_update = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.check_outline_exist(outline_info, is_or_replace, is_update))) {
        LOG_WARN("failed to check_outline_exist", K(outline_info), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.create_outline(outline_info, is_update, &arg.ddl_stmt_str_, schema_guard))) {
        LOG_WARN("create_outline failed", K(outline_info), K(is_update), K(ret));
      }
    }
  }
  return ret;
}

int ObRootService::create_user_defined_function(const obrpc::ObCreateUserDefinedFunctionArg &arg)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  uint64_t udf_id = OB_INVALID_ID;
  ObUDF udf_info_ = arg.udf_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.check_udf_exist(arg.udf_.get_tenant_id(), arg.udf_.get_name_str(), exist, udf_id))) {
    LOG_WARN("failed to check_udf_exist", K(arg.udf_.get_tenant_id()), K(arg.udf_.get_name_str()), K(exist), K(ret));
  } else if (exist) {
    ret = OB_UDF_EXISTS;
    LOG_USER_ERROR(OB_UDF_EXISTS, arg.udf_.get_name_str().length(), arg.udf_.get_name_str().ptr());
  } else if (OB_FAIL(ddl_service_.create_user_defined_function(udf_info_, arg.ddl_stmt_str_))) {
    LOG_WARN("failed to create udf", K(arg), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObRootService::drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_user_defined_function(arg))) {
    LOG_WARN("failed to alter udf", K(arg), K(ret));
  } else {/*do nothing*/}

  return ret;
}

bool ObRootService::is_sys_tenant(const ObString &tenant_name)
{
  return (0 == tenant_name.case_compare(OB_SYS_TENANT_NAME)
          || 0 == tenant_name.case_compare(OB_DIAG_TENANT_NAME)
          || 0 == tenant_name.case_compare(OB_GTS_TENANT_NAME));
}

int ObRootService::alter_outline(const ObAlterOutlineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_outline(arg))) {
    LOG_WARN("failed to alter outline", K(arg), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObRootService::drop_outline(const obrpc::ObDropOutlineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    if (OB_FAIL(ddl_service_.drop_outline(arg))) {
      LOG_WARN("ddl service failed to drop outline", K(arg), K(ret));
    }
  }
  return ret;
}
//-----End of functions for managing outlines-----

int ObRootService::create_routine(const ObCreateRoutineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObRoutineInfo routine_info = arg.routine_info_;
    const ObRoutineInfo* old_routine_info = NULL;
    uint64_t tenant_id = routine_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    bool is_or_replace = lib::is_oracle_mode() ? arg.is_or_replace_ : arg.is_need_alter_;
    bool is_inner = lib::is_mysql_mode() ? arg.is_or_replace_ : false;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (!is_inner && db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create routine of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      routine_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)
        && database_name.case_compare(OB_SYS_DATABASE_NAME) != 0
        && lib::is_oracle_mode()) {
      if (OB_FAIL(schema_guard.get_user_info(
          tenant_id, database_name, ObString(OB_DEFAULT_HOST_NAME), user_info))) {
        LOG_WARN("failed to get user info", K(ret), K(database_name));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user is does not exist", K(ret), K(database_name));
      } else if (OB_INVALID_ID == user_info->get_user_id()) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user id is invalid", K(ret), K(database_name));
      } else {
        routine_info.set_owner_id(user_info->get_user_id());
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id,
          db_schema->get_database_id(), routine_info.get_routine_name(), ROUTINE_SCHEMA,
          routine_info.get_routine_type(), is_or_replace, conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(routine_info.get_routine_name()));
      } else if (conflict_schema_types.count() > 0) {
        // 这里检查 oracle 模式下新对象的名字是否已经被其他对象占用了
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object in oralce mode",
                 K(ret), K(routine_info.get_routine_name()),
                 K(conflict_schema_types));
      }
    }
    bool exist = false;
    if (OB_SUCC(ret)) {
      if (routine_info.get_routine_type() == ROUTINE_PROCEDURE_TYPE) {
        if (OB_FAIL(schema_guard.check_standalone_procedure_exist(tenant_id, db_schema->get_database_id(),
                                                                  routine_info.get_routine_name(), exist))) {
          LOG_WARN("failed to check procedure info exist", K(routine_info), K(ret));
        } else if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "PROCEDURE",
                         routine_info.get_routine_name().length(), routine_info.get_routine_name().ptr());
        } else if (exist && is_or_replace) {
          if (OB_FAIL(schema_guard.get_standalone_procedure_info(tenant_id, db_schema->get_database_id(),
                                                                 routine_info.get_routine_name(), old_routine_info))) {
            LOG_WARN("failed to get standalone procedure info", K(routine_info), K(ret));
          } else if (OB_ISNULL(old_routine_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old routine info is NULL", K(ret));
          }
        }
      } else {
        if (OB_FAIL(schema_guard.check_standalone_function_exist(tenant_id, db_schema->get_database_id(),
                                                                 routine_info.get_routine_name(), exist))) {
          LOG_WARN("failed to check function info exist", K(routine_info), K(ret));
        } else if (exist && !is_or_replace) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "FUNCTION",
                         routine_info.get_routine_name().length(), routine_info.get_routine_name().ptr());
        } else if (exist && is_or_replace) {
          if (OB_FAIL(schema_guard.get_standalone_function_info(tenant_id, db_schema->get_database_id(),
                                                                routine_info.get_routine_name(), old_routine_info))) {
            LOG_WARN("failed to get standalone function info", K(routine_info), K(ret));
          } else if (OB_ISNULL(old_routine_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old routine info is NULL", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObErrorInfo error_info = arg.error_info_;
        ObSArray<ObDependencyInfo> &dep_infos =
                      const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
        if (OB_FAIL(ddl_service_.create_routine(routine_info,
                                                old_routine_info,
                                                (exist && is_or_replace),
                                                error_info,
                                                dep_infos,
                                                &arg.ddl_stmt_str_,
                                                schema_guard))) {
          LOG_WARN("failed to replace routine", K(routine_info), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::alter_routine(const ObCreateRoutineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObErrorInfo error_info = arg.error_info_;
    const ObRoutineInfo *routine_info = NULL;
    ObSchemaGetterGuard schema_guard;
    const uint64_t tenant_id = arg.routine_info_.get_tenant_id();
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                                  tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_routine_info(
               tenant_id, arg.routine_info_.get_routine_id(), routine_info))) {
      LOG_WARN("failed to get routine info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(routine_info)) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("routine info is not exist!", K(ret), K(arg.routine_info_));
    }
    if (OB_FAIL(ret)) {
    } else if ((lib::is_oracle_mode() && arg.is_or_replace_) ||
               (lib::is_mysql_mode() && arg.is_need_alter_)) {
      if (OB_FAIL(create_routine(arg))) {
        LOG_WARN("failed to alter routine with create", K(ret));
      }
    } else {
      if (OB_FAIL(ddl_service_.alter_routine(*routine_info, error_info, &arg.ddl_stmt_str_,
                                             schema_guard))) {
        LOG_WARN("alter routine failed", K(ret), K(arg.routine_info_), K(error_info));
      }
    }
  }
  return ret;
}

int ObRootService::drop_routine(const ObDropRoutineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &routine_name = arg.routine_name_;
    ObRoutineType routine_type = arg.routine_type_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    /*!
     * 兼容mysql行为:
     * create database test;
     * use test;
     * drop database test;
     * drop function if exists no_such_func; -- warning 1035
     * drop procedure if exists no_such_proc; -- error 1046
     * drop function no_such_func; --error 1035
     * drop procedure no_such_proc; --error 1046
     */
    if (db_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no database selected", K(ret), K(db_name));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create procedure of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }

    if (OB_SUCC(ret)) {
      bool exist = false;
      const ObRoutineInfo *routine_info = NULL;
      if (ROUTINE_PROCEDURE_TYPE == routine_type) {
        if (OB_FAIL(schema_guard.check_standalone_procedure_exist(tenant_id, db_schema->get_database_id(),
                                                                  routine_name, exist))) {
          LOG_WARN("failed to check standalone procedure info exist", K(routine_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_standalone_procedure_info(tenant_id, db_schema->get_database_id(),
                                                                 routine_name, routine_info))) {
            LOG_WARN("get procedure info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "PROCEDURE", db_name.length(), db_name.ptr(),
                         routine_name.length(), routine_name.ptr());
        }
      } else {
        if (OB_FAIL(schema_guard.check_standalone_function_exist(tenant_id, db_schema->get_database_id(),
                                                                 routine_name, exist))) {
          LOG_WARN("failed to check standalone function info exist", K(routine_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_standalone_function_info(tenant_id, db_schema->get_database_id(),
                                                                routine_name, routine_info))) {
            LOG_WARN("get function info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION", db_name.length(), db_name.ptr(),
                         routine_name.length(), routine_name.ptr());
        }
      }

      if (OB_SUCC(ret) && !OB_ISNULL(routine_info)) {
        ObErrorInfo error_info = arg.error_info_;
        if (OB_FAIL(ddl_service_.drop_routine(*routine_info,
                                              error_info,
                                              &arg.ddl_stmt_str_,
                                              schema_guard))) {
          LOG_WARN("drop routine failed", K(ret), K(routine_name), K(routine_info));
        }
      }
    }
    if (OB_ERR_NO_DB_SELECTED == ret && ROUTINE_FUNCTION_TYPE == routine_type) {
      if (arg.if_exist_) {
        ret = OB_SUCCESS;
        LOG_USER_WARN(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION (UDF)",
                      db_name.length(), db_name.ptr(),
                      routine_name.length(), routine_name.ptr());
      } else {
        ret = OB_ERR_SP_DOES_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION (UDF)",
                      db_name.length(), db_name.ptr(),
                      routine_name.length(), routine_name.ptr());
        LOG_WARN("FUNCTION (UDF) does not exists", K(ret), K(routine_name), K(db_name));
      }
    }
  }
  return ret;
}

int ObRootService::create_udt(const ObCreateUDTArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObUDTTypeInfo udt_info = arg.udt_info_;
    const ObUDTTypeInfo* old_udt_info = NULL;
    uint64_t tenant_id = udt_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    bool is_or_replace = arg.is_or_replace_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create udt of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      udt_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id, db_schema->get_database_id(),
          udt_info.get_type_name(), UDT_SCHEMA, INVALID_ROUTINE_TYPE, is_or_replace,
          conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(udt_info.get_type_name()));
      } else if (1 == conflict_schema_types.count()
                 && UDT_SCHEMA == conflict_schema_types.at(0) && udt_info.is_object_type()) {
        // skip
      } else if (conflict_schema_types.count() > 0) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object", K(ret), K(udt_info.get_type_name()),
            K(conflict_schema_types));
      }
    }
    bool exist = false;
    ObUDTTypeCode type_code = udt_info.is_object_body_ddl()
      ? UDT_TYPE_OBJECT_BODY : static_cast<ObUDTTypeCode>(udt_info.get_typecode());
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.check_udt_exist(tenant_id,
                                              db_schema->get_database_id(),
                                              OB_INVALID_ID,
                                              type_code,
                                              udt_info.get_type_name(),
                                              exist))) {
        LOG_WARN("failed to check udt info exist", K(udt_info), K(ret));
      } else if (exist && !is_or_replace) {
        if (!udt_info.is_object_type()) {
          ret = OB_ERR_SP_ALREADY_EXISTS;
          LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "UDT",
                        udt_info.get_type_name().length(), udt_info.get_type_name().ptr());
        }
      } else {
        // do nothing
      }
      if (exist && OB_SUCC(ret)) {
        if (OB_FAIL(schema_guard.get_udt_info(tenant_id,
                                              db_schema->get_database_id(),
                                              OB_INVALID_ID,
                                              udt_info.get_type_name(),
                                              type_code,
                                              old_udt_info))) {
          LOG_WARN("failed to get udt info", K(udt_info), K(ret));
        } else if (OB_ISNULL(old_udt_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("old udt info is NULL", K(ret));
        }
      }
      if (OB_SUCC(ret)
          && is_inner_pl_udt_id(udt_info.get_type_id())
          && type_code != UDT_TYPE_OBJECT_BODY) {
        if (!exist) {
          if (OB_FAIL(schema_guard.get_udt_info(tenant_id, udt_info.get_type_id(), old_udt_info))) {
            LOG_WARN("failed to get udt info", K(ret), K(tenant_id), K(udt_info.get_type_id()));
          } else if (OB_NOT_NULL(old_udt_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type id already used by other udt", K(ret), KPC(old_udt_info));
          }
        } else {
          CK (OB_NOT_NULL(old_udt_info));
          OV (old_udt_info->get_type_id() == udt_info.get_type_id(), OB_ERR_UNEXPECTED, KPC(old_udt_info), K(udt_info));
        }
      }
      // 这儿检查object是否能插入，当name相同，且不是replace的时候，只有一种情况可以插入
      // 新的body不为空，但是老的body为空，这说明需要增加body信息
      if (OB_SUCC(ret) && exist && !is_or_replace) {
        ret = OB_ERR_SP_ALREADY_EXISTS;
        LOG_USER_ERROR(OB_ERR_SP_ALREADY_EXISTS, "UDT",
                    udt_info.get_type_name().length(), udt_info.get_type_name().ptr());
      }

      if (OB_SUCC(ret)) {
        ObErrorInfo error_info = arg.error_info_;
        ObSArray<ObRoutineInfo> &public_routine_infos =
                      const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
        ObSArray<ObDependencyInfo> &dep_infos =
                     const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
        if (OB_FAIL(ddl_service_.create_udt(udt_info,
                                            old_udt_info,
                                            (exist && is_or_replace),
                                            public_routine_infos,
                                            error_info,
                                            schema_guard,
                                            dep_infos,
                                            &arg.ddl_stmt_str_))) {
          LOG_WARN("failed to create udt", K(udt_info), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::drop_udt(const ObDropUDTArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &udt_name = arg.udt_name_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    if (db_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("no database selected", K(ret), K(db_name));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create udt of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (!arg.is_type_body_) {
        bool exist = false;
        const ObUDTTypeInfo *udt_info = NULL;
        ObUDTTypeInfo udt;
        const ObUDTTypeCode type_code = ObUDTTypeCode::UDT_TYPE_OBJECT;
        if (OB_FAIL(schema_guard.check_udt_exist(tenant_id,
                                                db_schema->get_database_id(),
                                                OB_INVALID_ID, type_code,
                                                udt_name, exist))) {
            LOG_WARN("failed to check udt info exist", K(udt_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_udt_info(tenant_id,
                                                db_schema->get_database_id(),
                                                OB_INVALID_ID,
                                                udt_name, type_code, udt_info))) {
            LOG_WARN("get udt info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "UDT", db_name.length(), db_name.ptr(),
                        udt_name.length(), udt_name.ptr());
        }

        if (OB_SUCC(ret) && !OB_ISNULL(udt_info)) {
          if (OB_FAIL(udt.assign(*udt_info))) {
            LOG_WARN("assign udt info failed", K(ret), KPC(udt_info));
          } else if (udt.is_object_type()) {
              udt.clear_property_flag(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_BODY);
              udt.set_object_ddl_type(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_SPEC);
          }
          if (OB_SUCC(ret)
           && OB_FAIL(ddl_service_.drop_udt(udt, schema_guard, &arg.ddl_stmt_str_))) {
            LOG_WARN("drop udt failed", K(ret), K(udt_name), K(*udt_info));
          }
        }
      } else {
        bool exist = false;
        const ObUDTTypeInfo *udt_info = NULL;
        ObUDTTypeInfo udt;
        const ObUDTTypeCode type_code = ObUDTTypeCode::UDT_TYPE_OBJECT_BODY;
        if (OB_FAIL(schema_guard.check_udt_exist(tenant_id,
                                                db_schema->get_database_id(),
                                                OB_INVALID_ID, type_code,
                                                udt_name, exist))) {
            LOG_WARN("failed to check udt info exist", K(udt_name), K(ret));
        } else if (exist) {
          if (OB_FAIL(schema_guard.get_udt_info(tenant_id,
                                                db_schema->get_database_id(),
                                                OB_INVALID_ID,
                                                udt_name, type_code, udt_info))) {
            LOG_WARN("get udt info failed", K(ret));
          }
        } else if (!arg.if_exist_) {
          ret = OB_ERR_SP_DOES_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "UDT", db_name.length(), db_name.ptr(),
                        udt_name.length(), udt_name.ptr());
        }
        if (OB_SUCC(ret) && exist && !OB_ISNULL(udt_info)) {
          if (OB_FAIL(udt.assign(*udt_info))) {
            LOG_WARN("assign udt info failed", K(ret), KPC(udt_info));
          } else if (udt.is_object_type()) {
              udt.clear_property_flag(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_SPEC);
              udt.set_object_ddl_type(ObUDTTypeFlag::UDT_FLAG_OBJECT_TYPE_BODY);
          }
          if (OB_SUCC(ret)
            && OB_FAIL(ddl_service_.drop_udt(udt, schema_guard, &arg.ddl_stmt_str_))) {
            LOG_WARN("drop udt failed", K(ret), K(udt_name), K(*udt_info));
          }
        }
      }
    }
  }
  return ret;
}

//----Functions for managing dblinks----

int ObRootService::create_dblink(const obrpc::ObCreateDbLinkArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_service_.create_dblink(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("create_dblink failed", K(arg.dblink_info_), K(ret));
  }
  return ret;
}

int ObRootService::drop_dblink(const obrpc::ObDropDbLinkArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_service_.drop_dblink(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("drop_dblink failed", K(arg.tenant_id_), K(arg.dblink_name_), K(ret));
  }
  return ret;
}


//----End of functions for managing dblinks----

//-----Functions for managing synonyms-----
int ObRootService::create_synonym(const ObCreateSynonymArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSynonymInfo synonym_info = arg.synonym_info_;
    uint64_t tenant_id = synonym_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    ObString obj_database_name = arg.obj_db_name_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    const ObDatabaseSchema *obj_db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(database_name), K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, obj_database_name, obj_db_schema))) {
      LOG_WARN("get database schema failed", K(obj_database_name), K(ret));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (OB_ISNULL(obj_db_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, obj_database_name.length(), obj_database_name.ptr());
    } else if (OB_UNLIKELY(db_schema->is_in_recyclebin())
               || OB_UNLIKELY(obj_db_schema->is_in_recyclebin())) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("can't create synonym of db in recyclebin", K(arg), KPC(db_schema), KPC(obj_db_schema), K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == db_schema->get_database_id())
               || OB_UNLIKELY(OB_INVALID_ID == obj_db_schema->get_database_id())) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), KPC(db_schema), KPC(obj_db_schema), K(ret));
    } else {
      synonym_info.set_database_id(db_schema->get_database_id());
      synonym_info.set_object_database_id(obj_db_schema->get_database_id());
    }
    bool is_update = false;
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id, synonym_info.get_database_id(),
          synonym_info.get_synonym_name_str(), SYNONYM_SCHEMA, INVALID_ROUTINE_TYPE,
          arg.or_replace_, conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(synonym_info));
      } else if (conflict_schema_types.count() > 0) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object",
                 K(ret), K(synonym_info), K(conflict_schema_types));
      }
    }
    if (OB_SUCC(ret)) {
      // can not delete, it will update synonym_info between check_synonym_exist.
      if (OB_FAIL(ddl_service_.check_synonym_exist(synonym_info, arg.or_replace_, is_update))) {
        LOG_WARN("failed to check_synonym_exist", K(synonym_info), K(arg.or_replace_), K(is_update), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.create_synonym(synonym_info, arg.dependency_info_, &arg.ddl_stmt_str_, is_update, schema_guard))) {
        LOG_WARN("create_synonym failed", K(synonym_info), K(ret));
      }
    }
  }
  return ret;
}

int ObRootService::drop_synonym(const obrpc::ObDropSynonymArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.drop_synonym(arg))) {
    LOG_WARN("ddl service failed to drop synonym", K(arg), K(ret));
  }
  return ret;
}
//-----End of functions for managing synonyms-----

#ifdef OB_BUILD_SPM
//-----Functions for managing plan_baselines-----
int ObRootService::accept_plan_baseline(const ObModifyPlanBaselineArg &arg)
{
  int ret = OB_SUCCESS;
  ObZone null_zone;
  ObSEArray<ObAddr, 8> server_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      if (OB_FAIL(rpc_proxy_.to(server_list.at(i))
                            .by(arg.tenant_id_)
                            .as(arg.tenant_id_)
                            .svr_accept_plan_baseline(arg))) {
        LOG_WARN("fail to accept plan baseline", K(ret), K(server_list.at(i)));
        ret = OB_SUCCESS;
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObRootService::cancel_evolve_task(const ObModifyPlanBaselineArg &arg)
{
  int ret = OB_SUCCESS;
  ObZone null_zone;
  ObSEArray<ObAddr, 8> server_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      if (OB_FAIL(rpc_proxy_.to(server_list.at(i))
                            .by(arg.tenant_id_)
                            .as(arg.tenant_id_)
                            .svr_cancel_evolve_task(arg))) {
        LOG_WARN("fail to accept plan baseline", K(ret), K(server_list.at(i)));
        ret = OB_SUCCESS;
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObRootService::admin_load_baseline(const obrpc::ObLoadPlanBaselineArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminLoadBaseline admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("dispatch flush cache failed", K(arg), K(ret));
      }
      ROOTSERVICE_EVENT_ADD("root_service", "admin_load_baseline", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::admin_load_baseline_v2(const obrpc::ObLoadPlanBaselineArg &arg,
                                          obrpc::ObLoadBaselineRes &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    uint64_t load_count = 0;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminLoadBaselineV2 admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg, load_count))) {
        LOG_WARN("dispatch flush cache failed", K(arg), K(ret));
      } else {
        res.load_count_ = load_count;
      }
      ROOTSERVICE_EVENT_ADD("root_service", "admin_load_baseline", K(ret), K(arg));
    }
  }
  return ret;
}

//-----End of functions for managing plan_baselines-----
#endif

int ObRootService::admin_sync_rewrite_rules(const obrpc::ObSyncRewriteRuleArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminSyncRewriteRules admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("dispatch sync rewrite rules failed", K(arg), K(ret));
      }
      ROOTSERVICE_EVENT_ADD("root_service", "admin_sync_rewrite_rules", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::create_package(const obrpc::ObCreatePackageArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObPackageInfo new_package_info = arg.package_info_;
    const ObPackageInfo *old_package_info = NULL;
    uint64_t tenant_id = new_package_info.get_tenant_id();
    ObString database_name = arg.db_name_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    const ObUserInfo *user_info = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    } else {
      new_package_info.set_database_id(db_schema->get_database_id());
    }
    if (OB_SUCC(ret)
        && database_name.case_compare(OB_SYS_DATABASE_NAME) != 0
        && lib::is_oracle_mode()) {
      if (OB_FAIL(schema_guard.get_user_info(
        tenant_id, database_name, ObString(OB_DEFAULT_HOST_NAME), user_info))) {
        LOG_WARN("failed to get user info", K(ret), K(database_name));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user is does not exist", K(ret), K(database_name));
      } else if (OB_INVALID_ID == user_info->get_user_id()) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user id is invalid", K(ret), K(database_name));
      } else {
        new_package_info.set_owner_id(user_info->get_user_id());
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      // package body 查重只比较有没有重复的 package body，package 查重需要比较除 package body 以外的对象
      if (PACKAGE_TYPE == new_package_info.get_type()
          && OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id,
             db_schema->get_database_id(), new_package_info.get_package_name(), PACKAGE_SCHEMA,
             INVALID_ROUTINE_TYPE, arg.is_replace_, conflict_schema_types))) {
        LOG_WARN("fail to check object exist", K(ret), K(new_package_info.get_package_name()));
      } else if (conflict_schema_types.count() > 0) {
        // 这里检查 oracle 模式下新对象的名字是否已经被其他对象占用了
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object in oralce mode",
                 K(ret), K(new_package_info.get_package_name()),
                 K(conflict_schema_types));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), new_package_info.get_package_name(),
                                                new_package_info.get_type(), new_package_info.get_compatibility_mode(),
                                                old_package_info))) {
        LOG_WARN("failed to check package info exist", K(new_package_info), K(ret));
      } else if (OB_ISNULL(old_package_info) || arg.is_replace_) {
        bool need_create = true;
        // 对于系统包, 为了避免多次重建, 比较下新的系统包与已经存在的系统包是否相同
        if (OB_NOT_NULL(old_package_info) && OB_SYS_TENANT_ID == tenant_id) {
          if (old_package_info->get_source().length() == new_package_info.get_source().length()
              && (0 == MEMCMP(old_package_info->get_source().ptr(),
                              new_package_info.get_source().ptr(),
                              old_package_info->get_source().length()))
              && old_package_info->get_exec_env() == new_package_info.get_exec_env()) {
            need_create = false;
            LOG_INFO("do not recreate package with same source",
                     K(ret),
                     K(old_package_info->get_source()),
                     K(new_package_info.get_source()), K(need_create));
          } else {
            LOG_INFO("recreate package with diff source",
                     K(ret),
                     K(old_package_info->get_source()),
                     K(new_package_info.get_source()), K(need_create));
          }
        }
        if (need_create) {
          ObSArray<ObRoutineInfo> &public_routine_infos = const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
          ObErrorInfo error_info = arg.error_info_;
          ObSArray<ObDependencyInfo> &dep_infos =
                               const_cast<ObSArray<ObDependencyInfo> &>(arg.dependency_infos_);
          if (OB_FAIL(ddl_service_.create_package(schema_guard,
                                                  old_package_info,
                                                  new_package_info,
                                                  public_routine_infos,
                                                  error_info,
                                                  dep_infos,
                                                  &arg.ddl_stmt_str_))) {
            LOG_WARN("create package failed", K(ret), K(new_package_info));
          }
        }
      } else {
        ret = OB_ERR_PACKAGE_ALREADY_EXISTS;
        const char *type = (new_package_info.get_type() == PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_ALREADY_EXISTS, type,
                       database_name.length(), database_name.ptr(),
                       new_package_info.get_package_name().length(), new_package_info.get_package_name().ptr());
      }
    }
  }
  return ret;
}

int ObRootService::alter_package(const obrpc::ObAlterPackageArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &package_name = arg.package_name_;
    ObPackageType package_type = arg.package_type_;
    int64_t compatible_mode =  arg.compatible_mode_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(db_name), K(ret));
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    }
    if (OB_SUCC(ret)) {
      bool exist = false;
      ObSArray<ObRoutineInfo> &public_routine_infos = const_cast<ObSArray<ObRoutineInfo> &>(arg.public_routine_infos_);
      if (OB_FAIL(schema_guard.check_package_exist(tenant_id, db_schema->get_database_id(),
                                                   package_name, package_type, compatible_mode, exist))) {
        LOG_WARN("failed to check package info exist", K(package_name), K(ret));
      } else if (exist) {
        const ObPackageInfo *package_info = NULL;
        if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), package_name, package_type,
                                                  compatible_mode, package_info))) {
          LOG_WARN("get package info failed", K(ret));
        } else if (OB_ISNULL(package_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package info is null", K(db_schema->get_database_id()), K(package_name), K(package_type), K(ret));
        } else if (OB_FAIL(ddl_service_.alter_package(schema_guard,
                                                      *package_info,
                                                      public_routine_infos,
                                                      const_cast<ObErrorInfo &>(arg.error_info_),
                                                      &arg.ddl_stmt_str_))) {
          LOG_WARN("drop package failed", K(ret), K(package_name));
        }
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        const char *type = (package_type == PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, type,
                       db_schema->get_database_name_str().length(), db_schema->get_database_name(),
                       package_name.length(), package_name.ptr());
      }
    }
  }

  return ret;
}

int ObRootService::drop_package(const obrpc::ObDropPackageArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    uint64_t tenant_id = arg.tenant_id_;
    const ObString &db_name = arg.db_name_;
    const ObString &package_name = arg.package_name_;
    ObPackageType package_type = arg.package_type_;
    int64_t compatible_mode = arg.compatible_mode_;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, db_name, db_schema))) {
      LOG_WARN("get database schema failed", K(ret));
    } else if (NULL == db_schema) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
    } else if (db_schema->is_in_recyclebin()) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("Can't not create package of db in recyclebin", K(ret), K(arg), K(*db_schema));
    } else if (OB_INVALID_ID == db_schema->get_database_id()) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), K(*db_schema), K(ret));
    }
    if (OB_SUCC(ret)) {
      bool exist = false;
      if (OB_FAIL(schema_guard.check_package_exist(tenant_id, db_schema->get_database_id(),
          package_name, package_type, compatible_mode, exist))) {
        LOG_WARN("failed to check package info exist", K(package_name), K(ret));
      } else if (exist) {
        const ObPackageInfo *package_info = NULL;
        ObErrorInfo error_info = arg.error_info_;
        if (OB_FAIL(schema_guard.get_package_info(tenant_id, db_schema->get_database_id(), package_name, package_type, compatible_mode, package_info))) {
          LOG_WARN("get package info failed", K(ret));
        } else if (OB_FAIL(ddl_service_.drop_package(*package_info,
                                                     error_info,
                                                     &arg.ddl_stmt_str_))) {
          LOG_WARN("drop package failed", K(ret), K(package_name));
        }
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        const char *type = (package_type == PACKAGE_TYPE ? "PACKAGE" : "PACKAGE BODY");
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, type,
                       db_name.length(), db_name.ptr(),
                       package_name.length(), package_name.ptr());
      }
    }
  }
  return ret;
}

int ObRootService::create_trigger(const obrpc::ObCreateTriggerArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.create_trigger(arg))) {
    LOG_WARN("failed to create trigger", K(ret));
  }
  return ret;
}

int ObRootService::alter_trigger(const obrpc::ObAlterTriggerArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_trigger(arg))) {
    LOG_WARN("failed to alter trigger", K(ret));
  }
  return ret;
}

int ObRootService::drop_trigger(const obrpc::ObDropTriggerArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_trigger(arg))) {
    LOG_WARN("failed to drop trigger", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// sequence
////////////////////////////////////////////////////////////////
int ObRootService::do_sequence_ddl(const obrpc::ObSequenceDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.do_sequence_ddl(arg))) {
    LOG_WARN("do sequence ddl failed", K(arg), K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// context
////////////////////////////////////////////////////////////////
int ObRootService::do_context_ddl(const obrpc::ObContextDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.do_context_ddl(arg))) {
    LOG_WARN("do context ddl failed", K(arg), K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// schema revise
////////////////////////////////////////////////////////////////
int ObRootService::schema_revise(const obrpc::ObSchemaReviseArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.do_schema_revise(arg))) {
    LOG_WARN("schema revise failed", K(arg), K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// keystore
////////////////////////////////////////////////////////////////

int ObRootService::do_keystore_ddl(const obrpc::ObKeystoreDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifndef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(ddl_service_.do_keystore_ddl(arg))) {
    LOG_WARN("do sequence ddl failed", K(arg), K(ret));
  }
#else
  } else {
    // exclude add server
    common::ObArray<uint64_t> tenant_id_array;
    common::ObArray<std::pair<uint64_t, uint64_t> > max_key_version;
    SpinRLockGuard sync_guard(master_key_mgr_.sync());
    if (OB_FAIL(ddl_service_.do_keystore_ddl(arg))) {
      LOG_WARN("do sequence ddl failed", K(arg), K(ret));
    } else if (arg.type_ == ObKeystoreDDLArg::ALTER_KEYSTORE_SET_KEY) {
      if (OB_FAIL(tenant_id_array.push_back(arg.exec_tenant_id_))) {
        LOG_WARN("fail to push back", KR(ret));
      } else if (OB_FAIL(ObMasterKeyGetter::instance().get_latest_key_versions(
            tenant_id_array, max_key_version))) {
        LOG_WARN("fail to get latest key versions", KR(ret));
      } else if (max_key_version.count() != 1 || max_key_version.at(0).second <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max key version unexpected", KR(ret), K(arg), K(max_key_version));
      } else if (OB_FAIL(master_key_mgr_.forward_tenant_max_key_version(
              arg.exec_tenant_id_, max_key_version.at(0).second))) {
        LOG_WARN("fail to forward tenant max key version", KR(ret), K(arg), K(max_key_version));
      }
    } else {
      // no new master key generated, ignore
    }
  }
#endif
  return ret;
}

////////////////////////////////////////////////////////////////
// label security policy
////////////////////////////////////////////////////////////////

int ObRootService::handle_label_se_policy_ddl(const obrpc::ObLabelSePolicyDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_label_se_policy_ddl(arg))) {
    LOG_WARN("do label security policy ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_label_se_component_ddl(const obrpc::ObLabelSeComponentDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_label_se_component_ddl(arg))) {
    LOG_WARN("do label security policy ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_label_se_label_ddl(const obrpc::ObLabelSeLabelDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_label_se_label_ddl(arg))) {
    LOG_WARN("do label security policy ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_label_se_user_level_ddl(const obrpc::ObLabelSeUserLevelDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_label_se_user_level_ddl(arg))) {
    LOG_WARN("do label security policy ddl failed", K(arg), K(ret));
  }
  return ret;
}
// tablespace
////////////////////////////////////////////////////////////////
int ObRootService::do_tablespace_ddl(const obrpc::ObTablespaceDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.do_tablespace_ddl(arg))) {
    LOG_WARN("do sequence ddl failed", K(arg), K(ret));
  }

  return ret;
}
////////////////////////////////////////////////////////////////
// server & zone management
////////////////////////////////////////////////////////////////
int ObRootService::add_server_for_bootstrap_in_version_smaller_than_4_2_0(
      const common::ObAddr &server,
      const common::ObZone &zone)
{
  return server_manager_.add_server(server, zone);
}
int ObRootService::add_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {}
  if (OB_SUCC(ret)) {
    if (!ObHeartbeatService::is_service_enabled()) { // the old logic
      LOG_INFO("sys tenant data version < 4.2, add_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(old_add_server(arg))) {
        LOG_WARN("fail to add server by using old logic", KR(ret), K(arg));
      }
    } else { // the new logic
      LOG_INFO("sys tenant data version >= 4.2, add_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.add_servers(arg.servers_, arg.zone_))) {
        LOG_WARN("fail to add servers", KR(ret), K(arg));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(load_server_manager())) {
        // ** FIXME (linqiucen.lqc): temp. solution.
        // ** This will be removed if we do not need whitelist in server_manager
        LOG_WARN("fail to load server_manager, please try 'ALTER SYSTEM RELOAD SERVER;'", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  FLOG_INFO("add server", KR(ret), K(arg));
  return ret;
}
int ObRootService::old_add_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  // argument
#ifdef OB_BUILD_TDE_SECURITY
  ObWaitMasterKeyInSyncArg wms_in_sync_arg;
  // master key mgr sync
#endif
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("fail to get sys data version", KR(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(server_zone_op_service_.construct_rs_list_arg(wms_in_sync_arg.rs_list_arg_))) {
    LOG_WARN("fail to construct rs list arg", KR(ret));
#endif
  } else {
    LOG_INFO("add_server", K(arg), "timeout_ts", THIS_WORKER.get_timeout_ts());
    ObCheckServerEmptyArg new_arg(ObCheckServerEmptyArg::ADD_SERVER,
                                  sys_data_version);
    ObCheckDeploymentModeArg dp_arg;
    dp_arg.single_zone_deployment_on_ = OB_FILE_SYSTEM_ROUTER.is_single_zone_deployment_on();

#ifdef OB_BUILD_TDE_SECURITY
    SpinRLockGuard sync_guard(master_key_mgr_.sync());
#endif
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.servers_.count(); ++i) {
      const ObAddr &addr = arg.servers_[i];
      Bool is_empty(false);
      Bool is_deployment_mode_match(false);
      if (OB_FAIL(rpc_proxy_.to(addr).is_empty_server(new_arg, is_empty))) {
        LOG_WARN("fail to check is server empty", K(ret), K(addr));
      } else if (!is_empty) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("add non-empty server not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add non-empty server");
      } else if (OB_FAIL([&](){
                           int ret = OB_SUCCESS;
                           ret = rpc_proxy_.to(addr).check_deployment_mode_match(
                             dp_arg, is_deployment_mode_match);
                           return ret;}())) {
        LOG_WARN("fail to check deployment mode match", K(ret));
      } else if (!is_deployment_mode_match) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("add server deployment mode mot match not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add server deployment mode not match");
#ifdef OB_BUILD_TDE_SECURITY
      } else if (OB_FAIL(server_zone_op_service_.master_key_checking_for_adding_server(
            addr,
            arg.zone_,
            wms_in_sync_arg))) {
        LOG_WARN("master key checking for adding server is failed", KR(ret), K(addr), K(arg.zone_));
#endif
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server_manager_.add_server(addr, arg.zone_))) {
          LOG_WARN("add_server failed", "server", addr,  "zone", arg.zone_, K(ret));
        }
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "add_server", K(ret), K(arg));
  return ret;
}
int ObRootService::delete_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {}
  if (OB_SUCC(ret)) {
    if (!ObHeartbeatService::is_service_enabled()) { // the old logic
      LOG_INFO("sys tenant data version < 4.2, delete_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(old_delete_server(arg))) {
        LOG_WARN("fail to delete server by using the old logic", KR(ret), K(arg));
      }
    } else { // the new logic
      LOG_INFO("sys tenant data version >= 4.2, delete_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.delete_servers(arg.servers_, arg.zone_))) {
        LOG_WARN("fail to delete servers", KR(ret), K(arg));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(load_server_manager())) {
        // ** FIXME (linqiucen.lqc): temp. solution.
        // ** This will be removed if we do not need whitelist in server_manager
        LOG_WARN("fail to load server_manager, please try 'ALTER SYSTEM RELOAD SERVER;'", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else {
        root_balancer_.wakeup();
        empty_server_checker_.wakeup();
        lost_replica_checker_.wakeup();
        LOG_INFO("delete server and load server manager successfully", K(arg));
      }
    }
  }
  FLOG_INFO("delete server", KR(ret), K(arg));
  return ret;
}
int ObRootService::old_delete_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  bool has_enough = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(check_server_have_enough_resource_for_delete_server(arg.servers_, arg.zone_))) {
    LOG_WARN("not enough resource, cannot delete servers", K(ret), K(arg));
  } else if (OB_FAIL(check_all_ls_has_leader("delete server"))) {
    LOG_WARN("fail to check all ls has leader", KR(ret), K(arg));
  } else if (OB_FAIL(server_manager_.delete_server(arg.servers_, arg.zone_))) {
    LOG_WARN("delete_server failed", "servers", arg.servers_, "zone", arg.zone_, K(ret));
  } else {
    root_balancer_.wakeup();
    empty_server_checker_.wakeup();
    lost_replica_checker_.wakeup();
  }
  ROOTSERVICE_EVENT_ADD("root_service", "delete_server", K(ret), K(arg));
  return ret;
}

int ObRootService::check_server_have_enough_resource_for_delete_server(
                   const ObIArray<ObAddr> &servers,
                   const ObZone &zone)
{
  int ret = OB_SUCCESS;
  common::ObZone tmp_zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(server, servers, OB_SUCC(ret)) {
      if (zone.is_empty()) {
        // still use server manager here, since this func. will be called only in version < 4.2
        if (OB_FAIL(server_manager_.get_server_zone(*server, tmp_zone))) {
          LOG_WARN("fail to get server zone", K(ret));
        }
      } else {
        if (OB_FAIL(server_manager_.get_server_zone(*server, tmp_zone))) {
          LOG_WARN("fail to get server zone", K(ret));
        } else if (tmp_zone != zone) {
          ret = OB_SERVER_ZONE_NOT_MATCH;
          LOG_WARN("delete server not in zone", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(unit_manager_.check_enough_resource_for_delete_server(
                *server, tmp_zone))) {
          LOG_WARN("fail to check enouch resource", K(ret));
        }
      }
    }//end for each
  }
  return ret;
}

int ObRootService::cancel_delete_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {}
  if (OB_SUCC(ret)) {
    if (!ObHeartbeatService::is_service_enabled()) { // the old logic
      LOG_INFO("sys tenant data version < 4.2, cancel_delete_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(old_cancel_delete_server(arg))) {
        LOG_WARN("fail to cancel delete server by using the old logic", KR(ret), K(arg));
      }
    } else { // the new logic
      LOG_INFO("sys tenant data version >= 4.2, cancel_delete_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.cancel_delete_servers(arg.servers_, arg.zone_))) {
        LOG_WARN("fail to cancel delete servers", KR(ret), K(arg));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(load_server_manager())) {
        // ** FIXME (linqiucen.lqc): temp. solution.
        // ** This will be removed if we do not need whitelist in server_manager
        LOG_WARN("fail to load server_manager, please try 'ALTER SYSTEM RELOAD SERVER;'", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else {
        root_balancer_.wakeup();
      }
    }
  }
  FLOG_INFO("cancel delete server", KR(ret), K(arg));
  return ret;
}

int ObRootService::old_cancel_delete_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.servers_.count(); ++i) {
      const bool commit = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(server_manager_.end_delete_server(arg.servers_[i], arg.zone_, commit))) {
        LOG_WARN("delete_server failed", "server", arg.servers_[i],
                 "zone", arg.zone_, K(commit), K(ret));
      } else {
        // TODO: @wanhong.wwh NEED support cancel DR task
        root_balancer_.wakeup();
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "cancel_delete_server", K(ret), K(arg));
  return ret;
}

int ObRootService::start_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {}
  if (OB_SUCC(ret)) {
    if (!ObHeartbeatService::is_service_enabled()) { // the old logic
      LOG_INFO("sys tenant data version < 4.2, start_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_manager_.start_server_list(arg.servers_, arg.zone_))) {
        LOG_WARN("fail to start server by using old logic", KR(ret), K(arg));
      }
    } else { // the new logic
      LOG_INFO("sys tenant data version >= 4.2, start_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.start_servers(arg.servers_, arg.zone_))) {
        LOG_WARN("fail to start servers", KR(ret), K(arg));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(load_server_manager())) {
        // ** FIXME (linqiucen.lqc): temp. solution.
        // ** This will be removed if we do not need whitelist in server_manager
        LOG_WARN("fail to load server_manager, please try 'ALTER SYSTEM RELOAD SERVER;'", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  FLOG_INFO("start server", KR(ret), K(arg));
  return ret;
}

int ObRootService::stop_server(const obrpc::ObAdminServerArg &arg)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {
    if (!ObHeartbeatService::is_service_enabled()) { // the old logic
      LOG_INFO("sys tenant data version < 4.2, stop_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.stop_server_precheck(arg.servers_, arg.op_))) {
        LOG_WARN("fail to precheck stop server", KR(ret), K(arg));
      } else if (OB_FAIL(server_manager_.stop_server_list(arg.servers_, arg.zone_))) {
        LOG_WARN("stop server failed", "server", arg.servers_, "zone", arg.zone_, KR(ret));
      }
    } else {
      LOG_INFO("sys tenant data version >= 4.2, stop_server", K(arg),
          "timeout_ts", ctx.get_timeout());
      if (OB_FAIL(server_zone_op_service_.stop_servers(arg.servers_, arg.zone_, arg.op_))) {
        LOG_WARN("stop server failed", KR(ret), K(arg));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(load_server_manager())) {
        // ** FIXME (linqiucen.lqc): temp. solution.
        // ** This will be removed if we do not need whitelist in server_manager
        LOG_WARN("fail to load server_manager, please try 'ALTER SYSTEM RELOAD SERVER;'", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      if (OB_TMP_FAIL(try_notify_switch_leader(obrpc::ObNotifySwitchLeaderArg::STOP_SERVER))) {
        LOG_WARN("failed to notify switch leader", KR(ret), KR(tmp_ret));
      }
    }
  }
  FLOG_INFO("stop server", KR(ret), K(arg));
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRootService::try_check_encryption_zone_cond(
    const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  bool has_available_master_key = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (arg.zone_type_ != ZONE_TYPE_ENCRYPTION) {
    // good, no need to check
  } else if (OB_FAIL(master_key_mgr_.check_if_tenant_has_available_master_keys(
          OB_SYS_TENANT_ID, has_available_master_key))) {
    LOG_WARN("fail to check if tenant has available master key", KR(ret));
  } else if (!has_available_master_key) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("add encryption zone without available master key in sys tenant is not allowed", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add encryption zone without available master key in sys tenant");
  }
  return ret;
}
#endif

int ObRootService::add_zone(const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(try_check_encryption_zone_cond(arg))) {
    LOG_WARN("fail to check encryption zone", KR(ret), K(arg));
#endif
  } else if (OB_FAIL(zone_manager_.add_zone(arg.zone_, arg.region_, arg.idc_, arg.zone_type_))) {
    LOG_WARN("failed to add zone", K(ret), K(arg));
  } else {
    LOG_INFO("add zone ok", K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "add_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  return ret;
}

int ObRootService::delete_zone(const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    // @note to avoid deadlock risk, put it beside ZoneManager::write_lock_. ObServerManager::add_server also call the interfaces of zone_mgr.
    // it does not matter while add server after check.
    int64_t alive_count = 0;
    int64_t not_alive_count = 0;
    ObArray<ObServerInfoInTable> servers_info;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, servers_info))) {
      LOG_WARN("fail to get servers_info", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ObRootUtils::get_server_count(servers_info, arg.zone_, alive_count, not_alive_count))) {
      LOG_WARN("failed to get server count of the zone", KR(ret), K(arg.zone_), K(servers_info));
    } else {
      LOG_INFO("current server count of zone",
               "zone", arg.zone_, K(alive_count), K(not_alive_count));
      if (alive_count > 0 || not_alive_count > 0) {
        ret = OB_ERR_ZONE_NOT_EMPTY;
        LOG_USER_ERROR(OB_ERR_ZONE_NOT_EMPTY, alive_count, not_alive_count);
      } else if (OB_FAIL(zone_manager_.delete_zone(arg.zone_))) {
        LOG_WARN("delete zone failed", K(ret), K(arg));
      } else {
        LOG_INFO("delete zone ok", K(arg));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "delete_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  return ret;
}

int ObRootService::start_zone(const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(zone_manager_.start_zone(arg.zone_))) {
    LOG_WARN("failed to start zone", K(ret), K(arg));
  } else {
    LOG_INFO("start zone ok", K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "start_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  return ret;
}

//check whether has a object that all primary_zones were stopped after this time stop.
//basic idea:
//1. get the intersection tenant of to_stop_list and stopped_server_list. if tenant is only on stopp_list, it is no matter with this time stop.
//   therefor, only need to consider the intersection of two sets;
//2. set the union of to_stop_list and stopped_list to zone_list. set the intersection tenant of to_stop_list and stopped_server_list to tenant_ids,
//   check whether the primary zone of every tenant in tenant_ids is the subset of zone_list. if it is the subset, then the leader of the object may switch
//   to non_primary_zone observer, this situation is not allowed; if it is not the subset, than still has replica in primary zone can be leader.
int ObRootService::check_can_stop(const ObZone &zone,
                                  const ObIArray<ObAddr> &servers,
                                  const bool is_stop_zone)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObArray<ObAddr> to_stop_list;
  ObArray<ObZone> stopped_zone_list;
  ObArray<ObAddr> stopped_server_list;
  ObArray<ObServerInfoInTable> servers_info_in_table;
  if ((!is_stop_zone && (0 == servers.count() || zone.is_empty()))
      || (is_stop_zone && zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(servers), K(zone));
  } else if (OB_FAIL(ObRootUtils::get_stopped_zone_list(stopped_zone_list,
                                                        stopped_server_list))) {
    LOG_WARN("fail to get stopped zone list", KR(ret));
  } else if (0 >= stopped_server_list.count()) {
    //nothing todo
  } else {
    if (!is_stop_zone) {
      if (OB_FAIL(to_stop_list.assign(servers))) {
        LOG_WARN("fail to push back", KR(ret), K(servers));
      }
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, servers_info_in_table))) {
      LOG_WARN("fail to get servers_info_in_table", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ObRootUtils::get_servers_of_zone(servers_info_in_table, zone, to_stop_list))) {
      LOG_WARN("fail to get servers of zone", KR(ret), K(zone));
    }
    ObArray<uint64_t> tenant_ids;
    bool is_in = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRootUtils::get_tenant_intersection(unit_manager_,  //get intersection tenant of to_stop_list and stopped_server_list
                                                            to_stop_list,
                                                            stopped_server_list,
                                                            tenant_ids))) {
      LOG_WARN("fail to get tenant intersections", KR(ret));
    } else if (!has_exist_in_array(stopped_zone_list, zone)
               && OB_FAIL(stopped_zone_list.push_back(zone))) {
      LOG_WARN("fail to push back", KR(ret), K(zone));
    } else if (OB_FAIL(ObRootUtils::check_primary_region_in_zonelist(schema_service_, //check whether stop primary region of the tenant
                                                                     &ddl_service_,
                                                                     unit_manager_,
                                                                     zone_manager_,
                                                                     tenant_ids,
                                                                     stopped_zone_list,
                                                                     is_in))) {
      LOG_WARN("fail to check tenant stop primary region", KR(ret));
    } else if (!is_in) {
      //nothing todo
    } else {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("stop all primary region is not allowed", KR(ret), K(zone), K(servers));
    }
  }
  int64_t cost = ObTimeUtility::current_time() - start_time;
  LOG_INFO("check can stop zone/server", KR(ret), K(zone), K(servers), K(cost));
  return ret;
}

int ObRootService::stop_zone(const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> empty_server;
  HEAP_VAR(ObZoneInfo, zone_info) {
    bool zone_active = false;
    bool zone_exist = false;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(arg), K(ret));
    } else if (OB_FAIL(zone_manager_.check_zone_exist(arg.zone_, zone_exist))) {
      LOG_WARN("fail to check zone exist", K(ret));
    } else if (!zone_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("zone not exist");
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "zone not exist");
    } else if (OB_FAIL(zone_manager_.check_zone_active(arg.zone_, zone_active))) {
      LOG_WARN("fail to check zone active", KR(ret), K(arg.zone_));
    } else if (!zone_active) {
      //nothing todo
    } else if (ObAdminZoneArg::ISOLATE == arg.op_) {
      if (OB_FAIL(check_can_stop(arg.zone_, empty_server, true/*is_stop_zone*/))) {
        LOG_WARN("fail to check zone can stop", KR(ret), K(arg));
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Stop all server in primary region is disabled");
        }
      }
    } else {
      //stop zone/force stop zone
      if (ObRootUtils::have_other_stop_task(arg.zone_)) {
        ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
        LOG_WARN("cannot stop zone when other stop task already exist", KR(ret), K(arg));
        LOG_USER_ERROR(OB_STOP_SERVER_IN_MULTIPLE_ZONES,
                       "cannot stop server or stop zone in multiple zones");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(zone_manager_.get_zone(arg.zone_, zone_info))) {
      LOG_WARN("fail to get zone info", K(ret));
    } else {
      common::ObZoneType zone_type = static_cast<ObZoneType>(zone_info.zone_type_.value_);
      if (ObAdminZoneArg::ISOLATE == arg.op_) {
        // isolate server no need to check count and status of replicas, it can not kill observer savely
      } else if (common::ZONE_TYPE_READONLY == zone_type) {
        // do not need to check anything for readonly zone
      } else if (common::ZONE_TYPE_READWRITE == zone_type
                 || common::ZONE_TYPE_ENCRYPTION == zone_type) {
        ObArray<ObAddr> server_list;
        ObArray<ObServerInfoInTable> servers_info;
        if (OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
        } else if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, servers_info))) {
          LOG_WARN("fail to get servers_info", KR(ret), KP(GCTX.sql_proxy_));
        } else if (OB_FAIL(ObRootUtils::get_servers_of_zone(servers_info, arg.zone_, server_list))) {
          LOG_WARN("get servers of zone failed", KR(ret), K(arg.zone_), K(servers_info));
        } else if (server_list.count() <= 0) {
          //do not need to check anyting while zone is empty
        } else if (OB_FAIL(check_majority_and_log_in_sync(
            server_list,
            arg.force_stop_,/*skip_log_sync_check*/
            "stop zone"))) {
          LOG_WARN("fail to check majority and log in-sync", KR(ret), K(arg));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone type", K(ret), "zone", arg.zone_, K(zone_type));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_manager_.stop_zone(arg.zone_))) {
          LOG_WARN("stop zone failed", K(arg), K(ret));
        } else {
          LOG_INFO("stop zone ok", K(arg));
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(try_notify_switch_leader(obrpc::ObNotifySwitchLeaderArg::STOP_ZONE))) {
            LOG_WARN("failed to notify switch leader", KR(ret), KR(tmp_ret));
          }
        }
      } else {
        //set other error code to 4179
      }
    }
    ROOTSERVICE_EVENT_ADD("root_service", "stop_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  }
  return ret;
}

int ObRootService::alter_zone(const obrpc::ObAdminZoneArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(zone_manager_.alter_zone(arg))) {
    LOG_WARN("failed to alter zone", K(ret), K(arg));
  } else {
    LOG_INFO("alter zone ok", K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "alter_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  return ret;
}


int ObRootService::generate_stop_server_log_in_sync_dest_server_array(
    const common::ObIArray<common::ObAddr> &alive_server_array,
    const common::ObIArray<common::ObAddr> &excluded_server_array,
    common::ObIArray<common::ObAddr> &dest_server_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    dest_server_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < alive_server_array.count(); ++i) {
      const common::ObAddr &server = alive_server_array.at(i);
      if (has_exist_in_array(excluded_server_array, server)) {
        // in this excluded server array
      } else {
        if (OB_FAIL(dest_server_array.push_back(server))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::try_notify_switch_leader(const obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment &comment)
{
  int ret = OB_SUCCESS;
  ObServerManager::ObServerArray server_list;
  ObZone zone;
  obrpc::ObNotifySwitchLeaderArg arg;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(zone, server_list))) {
    LOG_WARN("failed to get server list", KR(ret), K(zone));
  } else if (OB_FAIL(arg.init(OB_INVALID_TENANT_ID, ObLSID(), ObAddr(), comment))) {
    LOG_WARN("failed to init switch leader arg", KR(ret), K(comment));
  } else if (OB_FAIL(ObRootUtils::notify_switch_leader(&rpc_proxy_, OB_SYS_TENANT_ID, arg, server_list))) {
    LOG_WARN("failed to notify switch leader", KR(ret), K(arg), K(server_list));
  }
  return ret;
}
////////////////////////////////////////////////////////////////
// system admin command (alter system ...)
////////////////////////////////////////////////////////////////
int ObRootService::init_sys_admin_ctx(ObSystemAdminCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ctx.rs_status_ = &rs_status_;
    ctx.rpc_proxy_ = &rpc_proxy_;
    ctx.sql_proxy_ = &sql_proxy_;
    ctx.server_mgr_ = &server_manager_;
    ctx.zone_mgr_ = &zone_manager_;
    ctx.schema_service_ = schema_service_;
    ctx.ddl_service_ = &ddl_service_;
    ctx.config_mgr_ = config_mgr_;
    ctx.unit_mgr_ = &unit_manager_;
    ctx.root_inspection_ = &root_inspection_;
    ctx.root_service_ = this;
    ctx.root_balancer_ = &root_balancer_;
    ctx.upgrade_storage_format_executor_ = &upgrade_storage_format_executor_;
    ctx.create_inner_schema_executor_ = &create_inner_schema_executor_;
    ctx.inited_ = true;
  }
  return ret;
}

int ObRootService::admin_switch_replica_role(const obrpc::ObAdminSwitchReplicaRoleArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminSwitchReplicaRole admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute switch replica role failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_switch_replica_role", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_switch_rs_role(const obrpc::ObAdminSwitchRSRoleArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

int ObRootService::admin_change_replica(const obrpc::ObAdminChangeReplicaArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

int ObRootService::admin_drop_replica(const obrpc::ObAdminDropReplicaArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

int ObRootService::admin_migrate_replica(const obrpc::ObAdminMigrateReplicaArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

int ObRootService::admin_report_replica(const obrpc::ObAdminReportReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminReportReplica admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute report replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_report_replica", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_flush_cache(const obrpc::ObAdminFlushCacheArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminFlushCache admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("dispatch flush cache failed", K(arg), K(ret));
      }
      ROOTSERVICE_EVENT_ADD("root_service", "admin_flush_cache", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::admin_recycle_replica(const obrpc::ObAdminRecycleReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRecycleReplica admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute recycle replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_recycle_replica", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_merge(const obrpc::ObAdminMergeArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminMerge admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute merge control failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_merge", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_recovery(const obrpc::ObAdminRecoveryArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminZoneFastRecovery admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute merge control failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_recovery", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_clear_roottable(const obrpc::ObAdminClearRoottableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminClearRoottable admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute clear root table failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_clear_roottable", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_refresh_schema(const obrpc::ObAdminRefreshSchemaArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRefreshSchema admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute refresh schema failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_refresh_schema", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_set_config(obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (arg.is_backup_config_) {
    if (OB_FAIL(admin_set_backup_config(arg))) {
      LOG_WARN("fail to set backup config", K(ret), K(arg));
    }
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObLatchWGuard guard(set_config_lock_, ObLatchIds::CONFIG_LOCK);
      ObAdminSetConfig admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute set config failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_set_config", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_clear_location_cache(
    const obrpc::ObAdminClearLocationCacheArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminClearLocationCache admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute clear location cache failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_clear_location_cache", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_refresh_memory_stat(const ObAdminRefreshMemStatArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRefreshMemStat admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute refresh memory stat failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_refresh_memory_stat", K(ret));
  return ret;
}

int ObRootService::admin_wash_memory_fragmentation(const ObAdminWashMemFragmentationArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminWashMemFragmentation admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute refresh memory stat failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_wash_memory_fragmentation", K(ret));
  return ret;
}

int ObRootService::admin_refresh_io_calibration(const obrpc::ObAdminRefreshIOCalibrationArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRefreshIOCalibration admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute refresh io calibration failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_refresh_io_calibration", K(ret));
  return ret;
}

int ObRootService::admin_reload_unit()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminReloadUnit admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("execute reload unit failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_reload_unit", K(ret));
  return ret;
}

int ObRootService::admin_reload_server()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminReloadServer admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("execute reload server failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_reload_server", K(ret));
  return ret;
}

int ObRootService::admin_reload_zone()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminReloadZone admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("execute reload zone failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_reload_zone", K(ret));
  return ret;
}

int ObRootService::admin_clear_merge_error(const obrpc::ObAdminMergeArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("admin receive clear_merge_error request");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", KR(ret));
    } else {
      ObAdminClearMergeError admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute clear merge error failed", KR(ret), K(arg));
      }
      ROOTSERVICE_EVENT_ADD("root_service", "clear_merge_error", KR(ret), K(arg));
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObRootService::admin_add_arbitration_service(const obrpc::ObAdminAddArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(share::ObArbitrationServiceUtils::add_arbitration_service(sql_proxy_, arg))) {
    LOG_WARN("fail to add arbitration service", KR(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("arb_service", "admin_add_arbitration_service", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_remove_arbitration_service(const obrpc::ObAdminRemoveArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(share::ObArbitrationServiceUtils::remove_arbitration_service(sql_proxy_, arg))) {
    LOG_WARN("fail to remove arbitration service", KR(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("arb_service", "admin_remove_arbitration_service", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_replace_arbitration_service(const obrpc::ObAdminReplaceArbitrationServiceArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(share::ObArbitrationServiceUtils::replace_arbitration_service(sql_proxy_, arg))) {
    LOG_WARN("fail to replace arbitration service", KR(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("arb_service", "admin_replace_arbitration_service", K(ret), K(arg));
  return ret;
}

int ObRootService::remove_cluster_info_from_arb_server(const obrpc::ObRemoveClusterInfoFromArbServerArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(ObArbitrationServiceUtils::remove_cluster_info_from_arb_server(arg.get_arbitration_service()))) {
    LOG_WARN("fail to remove cluster info from arb server", KR(ret), K(arg));
  }
  return ret;
}
#endif

int ObRootService::admin_migrate_unit(const obrpc::ObAdminMigrateUnitArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminMigrateUnit admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute migrate unit failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_migrate_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_upgrade_virtual_schema()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminUpgradeVirtualSchema admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("upgrade virtual schema failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_upgrade_virtual_schema", K(ret));
  return ret;
}

int ObRootService::admin_upgrade_cmd(const obrpc::Bool &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminUpgradeCmd admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("begin upgrade failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_upgrade_cmd", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_rolling_upgrade_cmd(const obrpc::ObAdminRollingUpgradeArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRollingUpgradeCmd admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("begin upgrade failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_rolling_upgrade_cmd", K(ret), K(arg));
  return ret;
}

int ObRootService::physical_restore_tenant(const obrpc::ObPhysicalRestoreTenantArg &arg, obrpc::Int64 &res_job_id)
{
  int ret = OB_SUCCESS;
  bool has_standby_cluster = false;
  res_job_id = OB_INVALID_ID;
  int64_t current_timestamp = ObTimeUtility::current_time();
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t job_id = OB_INVALID_ID;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  int64_t restore_concurrency = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    restore_concurrency = tenant_config->ha_high_thread_score;
  }
  if (0 == restore_concurrency) {
    restore_concurrency = OB_DEFAULT_RESTORE_CONCURRENCY;
  }

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (GCTX.is_standby_cluster() || GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore tenant while in standby cluster or "
             "in upgrade mode is not allowed", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW,
                   "restore tenant while in standby cluster or in upgrade mode");
  } else if (0 == restore_concurrency) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore tenant when restore_concurrency is 0 not allowed", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore tenant when restore_concurrency is 0");
  } else if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(arg.tenant_name_))) {
    LOG_WARN("unsupported tenant name", KR(ret), "tenant_name", arg.tenant_name_);
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant's schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, refreshed_schema_version))) {
    LOG_WARN("fail to get sys schema version", KR(ret));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      // just to check sys tenant's schema with latest schema version
      ObDDLSQLTransaction trans(schema_service_, false /*end_signal*/);
      if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID, refreshed_schema_version))) {
        LOG_WARN("failed to start trans, ", K(ret));
      } else if (OB_FAIL(RS_JOB_CREATE_EXT(job_id, RESTORE_TENANT, trans,
                         "sql_text", ObHexEscapeSqlStr(arg.get_sql_stmt())))) {
        LOG_WARN("fail create rs job", K(ret), K(arg));
      } else if (job_id < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid job_id", K(ret), K(job_id));
      } else if (OB_FAIL(ObRestoreUtil::fill_physical_restore_job(job_id, arg, job_info))) {
        LOG_WARN("fail to fill physical restore job", K(ret), K(job_id), K(arg));
      } else {
        job_info.set_restore_start_ts(start_ts);
        res_job_id = job_id;
      }
      if (FAILEDx(check_restore_tenant_valid(job_info, schema_guard))) {
        LOG_WARN("failed to check restore tenant vailid", KR(ret), K(job_info));
      }

      if (FAILEDx(ObRestoreUtil::record_physical_restore_job(trans, job_info))) {
        LOG_WARN("fail to record physical restore job", K(ret), K(job_id), K(arg));
      } else {
        //TODO wakeup restore scheduler
      }

      if (trans.is_started()) {
        int temp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
          ret = (OB_SUCC(ret)) ? temp_ret : ret;
        }
      }
    }
  }
  LOG_INFO("[RESTORE] physical restore tenant start", K(arg), K(ret));
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_start", K(ret),
                        "tenant_name", arg.tenant_name_);
  if (OB_SUCC(ret)) {
    const char *status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(
                             static_cast<PhysicalRestoreStatus>(PHYSICAL_RESTORE_CREATE_TENANT));
    ROOTSERVICE_EVENT_ADD("physical_restore", "change_restore_status",
                          "job_id", job_id,
                          "tenant_name", arg.tenant_name_,
                          "status", status_str);
  }
  return ret;
}

int ObRootService::check_restore_tenant_valid(const share::ObPhysicalRestoreJob &job_info,
    share::schema::ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!job_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(job_info));
  } else {
    //check tenant if exist
    const ObTenantSchema *tenant_schema = NULL;
    const ObString &tenant_name = job_info.get_tenant_name();
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(job_info));
    } else if (OB_NOT_NULL(tenant_schema)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("restore tenant with existed tenant name is not allowed", KR(ret), K(tenant_name));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore tenant with existed tenant name is");
    } else {
      // check if resource pool contains any E zone
      ObSqlString pool_list_str;
      ObArray<ObString> pool_list;
      ObArray<ObResourcePoolName> pools;
      ObArray<ObZone> zones;
      if (OB_FAIL(pool_list_str.assign(job_info.get_pool_list()))) {
        LOG_WARN("failed to assign pool list", KR(ret), K(job_info));
      } else if (OB_FAIL(ObRestoreScheduler::assign_pool_list(pool_list_str.ptr(), pool_list))) {
        LOG_WARN("failed to assgin pool list", KR(ret), K(pool_list_str));
      } else if (OB_FAIL(ObUnitManager::convert_pool_name_list(pool_list, pools))) {
         LOG_WARN("fail to convert pools", KR(ret), K(pool_list));
      } else if (OB_FAIL(unit_manager_.get_zones_of_pools(pools, zones))) {
        LOG_WARN("fail to get zones of pools", KR(ret), K(pools));
      } else {
        HEAP_VAR(ObZoneInfo, info) {
          for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); i++) {
            const ObZone &zone = zones.at(i);
            info.reset();
            if (OB_FAIL(zone_manager_.get_zone(zone, info))) {
              LOG_WARN("fail to get zone info", KR(ret), K(zone));
            } else if (static_cast<int64_t>(ObZoneType::ZONE_TYPE_ENCRYPTION) == info.zone_type_.value_) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("restore tenant with encrypted zone is not supported", KR(ret), K(info));
            }
          }
        }
      }
    }
  }
  //TODO check if need check R replica
  return ret;
}

int ObRootService::run_job(const obrpc::ObRunJobArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminRunJob admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("run job failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_run_job", K(ret), K(arg));
  return ret;
}

int ObRootService::run_upgrade_job(const obrpc::ObUpgradeJobArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t version = arg.version_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(arg));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (ObUpgradeJobArg::UPGRADE_POST_ACTION == arg.action_
             && !ObUpgradeChecker::check_data_version_exist(version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported version to run upgrade job", KR(ret), K(version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "run upgrade job with such version is");
  } else if (ObUpgradeJobArg::STOP_UPGRADE_JOB == arg.action_) {
    if (OB_FAIL(upgrade_executor_.stop())) {
      LOG_WARN("fail to stop upgrade task", KR(ret));
    } else {
      upgrade_executor_.start();
    }
  } else if (OB_FAIL(submit_upgrade_task(arg))) {
    LOG_WARN("fail to submit upgrade task", KR(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_run_upgrade_job", KR(ret), K(arg));
  return ret;
}

int ObRootService::upgrade_table_schema(const obrpc::ObUpgradeTableSchemaArg &arg)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  FLOG_INFO("[UPGRADE] start to upgrade table", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (!arg.upgrade_virtual_schema()) {
    // upgrade single system table
    if (OB_FAIL(ddl_service_.upgrade_table_schema(arg))) {
      LOG_WARN("fail to upgrade table schema", KR(ret), K(arg));
    }
  } else {
    // upgrade all virtual table/sys view
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminUpgradeVirtualSchema admin_util(ctx);
      int64_t upgrade_cnt = 0;
      if (OB_FAIL(admin_util.execute(arg.get_tenant_id(), upgrade_cnt))) {
        LOG_WARN("upgrade virtual schema failed", KR(ret), K(arg));
      }
    }
  }
  FLOG_INFO("[UPGRADE] finish upgrade table", KR(ret), K(arg),
            "cost_us", ObTimeUtility::current_time() - start);
  return ret;
}

int ObRootService::broadcast_ds_action(const obrpc::ObDebugSyncActionArg &arg)
{
  LOG_INFO("receive broadcast debug sync actions", K(arg));
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  const ObZone all_zone;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(all_zone, server_list))) {
    LOG_WARN("get all alive servers failed", K(all_zone), K(ret));
  } else {
    FOREACH_X(s, server_list, OB_SUCCESS == ret) {
      if (OB_FAIL(rpc_proxy_.to(*s).timeout(config_->rpc_timeout).set_debug_sync_action(arg))) {
        LOG_WARN("set server's global sync action failed", K(ret), "server", *s, K(arg));
      }
    }
  }
  return ret;
}

int ObRootService::check_dangling_replica_finish(const obrpc::ObCheckDanglingReplicaFinishArg &arg)
{
  UNUSED(arg);
  return OB_NOT_SUPPORTED;
}

int ObRootService::fetch_alive_server(const ObFetchAliveServerArg &arg,
                                      ObFetchAliveServerResult &result)
{
  LOG_DEBUG("receive fetch alive server request");
  ObZone empty_zone; // for all server
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (!server_refreshed_) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("RS is initializing, server not refreshed, can not process this request");
  } else if (arg.cluster_id_ != config_->cluster_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster id mismatch",
             K(ret), K(arg), "cluster_id", static_cast<int64_t>(config_->cluster_id));
  } else if (OB_FAIL(SVR_TRACER.get_servers_by_status(empty_zone, result.active_server_list_,
                                                           result.inactive_server_list_))) {
    LOG_WARN("get alive servers failed", K(ret));
  }
  return ret;
}

int ObRootService::refresh_server(const bool load_frozen_status, const bool need_retry)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!in_service()|| (server_refreshed_ && load_frozen_status)) {
    // no need to refresh again for fast recover
  } else {
    {
      ObTimeoutCtx ctx;
      if (load_frozen_status) {
        ctx.set_timeout(config_->rpc_timeout);
      }
      if (OB_FAIL(load_server_manager())) {
        LOG_WARN("build server manager failed", K(ret), K(load_frozen_status));
      } else {
        LOG_INFO("build server manager succeed", K(load_frozen_status));
      }
      if (FAILEDx(SVR_TRACER.refresh())) {
        LOG_WARN("fail to refresh all server tracer", KR(ret));
      }
    }
    // request heartbeats from observers
    if (OB_SUCC(ret) && !ObHeartbeatService::is_service_enabled()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = request_heartbeats())) {
        LOG_WARN("request heartbeats failed", K(temp_ret));
      } else {
        LOG_INFO("request heartbeats succeed");
      }
    }
    if (OB_SUCC(ret)) {
      server_refreshed_ = true;
      LOG_INFO("refresh server success", K(load_frozen_status));
    } else if (need_retry) {
      LOG_INFO("refresh server failed, retry", K(ret), K(load_frozen_status));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schedule_refresh_server_timer_task(
                  ObRefreshServerTask::REFRESH_SERVER_INTERVAL))) {
        if (OB_CANCELED != ret) {
          LOG_ERROR("schedule refresh server task failed", K(ret));
        } else {
          LOG_WARN("schedule refresh server task failed, because the server is stopping", K(ret));
        }
      } else {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("schedule refresh server task again", KR(tmp_ret), KR(ret));
      }
    } else {} // no more to do
  }
  return ret;
}

int ObRootService::refresh_schema(const bool load_frozen_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObTimeoutCtx ctx;
    int64_t schema_version = OB_INVALID_VERSION;
    if (load_frozen_status) {
      ctx.set_timeout(config_->rpc_timeout);
    }
    ObArray<uint64_t> tenant_ids; //depend on sys schema while start RS
    if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to refresh sys schema", K(ret));
    } else if (OB_FAIL(schema_service_->refresh_and_add_schema(tenant_ids))) {
      LOG_WARN("refresh schema failed", K(ret), K(load_frozen_status));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, schema_version))) {
      LOG_WARN("fail to get max schema version", K(ret));
    } else {
      LOG_INFO("refresh schema with new mode succeed", K(load_frozen_status), K(schema_version));
    }
    if (OB_SUCC(ret)) {
      ObSchemaService *schema_service = schema_service_->get_schema_service();
      if (NULL == schema_service) {
        ret = OB_ERR_SYS;
        LOG_WARN("schema_service can't be null", K(ret), K(schema_version));
      } else {
        schema_service->set_refreshed_schema_version(schema_version);
        LOG_INFO("set schema version succeed", K(ret), K(schema_service), K(schema_version));
      }
    }
  }
  return ret;
}

int ObRootService::set_cluster_version()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  char sql[1024] = {0};
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();

  snprintf(sql, sizeof(sql), "alter system set min_observer_version = '%s'", PACKAGE_VERSION);
  if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql, affected_rows))) {
    LOG_WARN("execute sql failed", K(sql));
  }

  return ret;
}

int ObRootService::admin_set_tracepoint(const obrpc::ObAdminSetTPArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminSetTP admin_util(ctx, arg);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute report replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_set_tracepoint", K(ret), K(arg));
  return ret;
}

// RS may receive refresh time zone from observer with old binary during upgrade.
// do notiong
int ObRootService::refresh_time_zone_info(const obrpc::ObRefreshTimezoneArg &arg)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  ROOTSERVICE_EVENT_ADD("root_service", "refresh_time_zone_info", K(ret), K(arg));
  return ret;
}

int ObRootService::request_time_zone_info(const ObRequestTZInfoArg &arg, ObRequestTZInfoResult &result)
{
  UNUSED(arg);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SYS_TENANT_ID;

  ObTZMapWrap tz_map_wrap;
  ObTimeZoneInfoManager *tz_info_mgr = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id, tz_map_wrap, tz_info_mgr))) {
    LOG_WARN("get tenant timezone failed", K(ret));
  } else if (OB_ISNULL(tz_info_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_tz_mgr failed", K(ret), K(tz_info_mgr));
  } else if (OB_FAIL(tz_info_mgr->response_time_zone_info(result))) {
    LOG_WARN("fail to response tz_info", K(ret));
  } else {
    LOG_INFO("rs success to response lastest tz_info to server", "server", arg.obs_addr_, "last_version", result.last_version_);
  }
  return ret;
}

bool ObRootService::check_config(const ObConfigItem &item, const char *&err_info)
{
  bool bret = true;
  err_info = NULL;
  if (!inited_) {
    bret = false;
    LOG_WARN_RET(OB_NOT_INIT, "service not init");
  } else if (0 == STRCMP(item.name(), MIN_OBSERVER_VERSION)) {
    if (OB_SUCCESS != ObClusterVersion::is_valid(item.str())) {
      LOG_WARN_RET(OB_INVALID_ERROR, "fail to parse min_observer_version value");
      bret = false;
    }
  } else if (0 == STRCMP(item.name(), __BALANCE_CONTROLLER)) {
    ObString balance_troller_str(item.str());
    ObRootBalanceHelp::BalanceController switch_info;
    int tmp_ret = ObRootBalanceHelp::parse_balance_info(balance_troller_str, switch_info);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "fail to parse balance switch", K(balance_troller_str));
      bret = false;
    }
  }
  return bret;
}

int ObRootService::report_replica()
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  ObZone null_zone;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    FOREACH_CNT(server, server_list) {
      if (OB_ISNULL(server)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid server", K(ret));
      } else if (OB_FAIL(rpc_proxy_.to(*server).report_replica())) {
        LOG_WARN("fail to force observer report replica", K(ret), K(*server));
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootService::report_single_replica(
    const int64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  ObZone null_zone;
  obrpc::ObReportSingleReplicaArg arg;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    FOREACH_CNT(server, server_list) {
      if (OB_ISNULL(server)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid server", KR(ret));
//      } else if (OB_FAIL(rpc_proxy_.to(*server).report_single_replica(arg))) {//TODO(xiuming):delete it
//        LOG_WARN("fail to force observer report replica", KR(ret), K(server), K(arg));
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootService::update_all_server_config()
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  ObArray<ObAddr> server_list;
  ObArray<ObAddr> config_all_server_list;
  ObArray<ObAddr> empty_excluded_server_list;
  bool need_update = true;
  HEAP_VAR(ObAdminSetConfigItem, all_server_config) {
    auto &value = all_server_config.value_;
    int64_t pos = 0;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(!SVR_TRACER.has_build())) {
      need_update = false;
    } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(empty_zone, server_list))) {
      LOG_WARN("fail to get server", K(ret));
    } else if (OB_UNLIKELY(0 == server_list.size())) {
      need_update = false;
      LOG_WARN("no servers in all_server_tracer");
    } else if (OB_FAIL(all_server_config.name_.assign(config_->all_server_list.name()))) {
      LOG_WARN("fail to assign name", K(ret));
    } else if (OB_FAIL(ObShareUtil::parse_all_server_list(empty_excluded_server_list, config_all_server_list))) {
      LOG_WARN("fail to parse all_server_list from GCONF", KR(ret));
    } else if (ObRootUtils::is_subset(server_list, config_all_server_list)
        && ObRootUtils::is_subset(config_all_server_list, server_list)) {
      need_update = false;
      LOG_TRACE("server_list is the same as config_all_server_list, no need to update GCONF.all_server_list",
          K(server_list), K(config_all_server_list));
    } else {
      LOG_INFO("GCONF.all_server_list should be updated", K(config_all_server_list), K(server_list));
      char ip_port_buf[MAX_IP_PORT_LENGTH];
      for (int64_t i = 0; i < server_list.count() - 1; i++) {
        if (OB_FAIL(server_list.at(i).ip_port_to_string(ip_port_buf, MAX_IP_PORT_LENGTH))) {
          LOG_WARN("fail to print ip port", K(ret), "server", server_list.at(i));
        } else if (OB_FAIL(databuff_printf(value.ptr(), OB_MAX_CONFIG_VALUE_LEN, pos, "%s", ip_port_buf))) {
          LOG_WARN("fail to databuff_printf", K(ret), K(i), K(server_list));
        } else if (OB_FAIL(databuff_printf(value.ptr(), OB_MAX_CONFIG_VALUE_LEN, pos, "%c", ','))) {
          LOG_WARN("fail to print char", K(ret), K(i), K(server_list));
        }
      }
      if (OB_SUCC(ret) && 0 < server_list.count()) {
        if (OB_FAIL(server_list.at(server_list.count() - 1).ip_port_to_string(ip_port_buf, MAX_IP_PORT_LENGTH))) {
          LOG_WARN("fail to print ip port", K(ret), "server", server_list.at(server_list.count() - 1));
        } else if (OB_FAIL(databuff_printf(value.ptr(), OB_MAX_CONFIG_VALUE_LEN, pos, "%s", ip_port_buf))) {
          LOG_WARN("fail to databuff_printf", K(ret), K(server_list), K(ip_port_buf));
        }
      }
    }
    if (OB_SIZE_OVERFLOW == ret) {
      LOG_ERROR("can't print server addr to buffer, size overflow", K(ret), K(server_list));
    }
    if (need_update && OB_SUCC(ret)) {
      ObAdminSetConfigArg arg;
      arg.is_inner_ = true;
      if (OB_FAIL(arg.items_.push_back(all_server_config))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(admin_set_config(arg))) {
        LOG_WARN("fail to set config", K(ret));
      } else {
        LOG_INFO("update all server config success", K(arg));
      }
    }
  }
  return ret;
}
/////////////////////////
ObRootService::ObReportCoreTableReplicaTask::ObReportCoreTableReplicaTask(ObRootService &root_service)
: ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service)
{
  set_retry_times(INT64_MAX); //retry until success
}

int ObRootService::ObReportCoreTableReplicaTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = OB_SYS_TENANT_ID;
  if (OB_FAIL(root_service_.report_single_replica(tenant_id, SYS_LS))) {
    LOG_WARN("fail to report single replica", K(ret), K(tenant_id), K(SYS_LS));
  } else {
    LOG_INFO("report all_core table succeed");
  }
  return ret;
}

ObAsyncTask *ObRootService::ObReportCoreTableReplicaTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObReportCoreTableReplicaTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObReportCoreTableReplicaTask(root_service_);
  }
  return task;
}
//////////////ObReloadUnitManagerTask
ObRootService::ObReloadUnitManagerTask::ObReloadUnitManagerTask(ObRootService &root_service,
                                                                ObUnitManager &unit_manager)
: ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service),
    unit_manager_(unit_manager)
{
  set_retry_times(INT64_MAX); // retry until success
}

int ObRootService::ObReloadUnitManagerTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(unit_manager_.load())) {
    LOG_WARN("fail to reload unit_manager", K(ret));
  } else {
    LOG_INFO("reload unit_manger succeed");
  }
  return ret;
}

ObAsyncTask *ObRootService::ObReloadUnitManagerTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObReloadUnitManagerTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObReloadUnitManagerTask(root_service_, unit_manager_);
  }
  return task;
}

ObRootService::ObLoadDDLTask::ObLoadDDLTask(ObRootService &root_service)
  : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(INT64_MAX);
}

int ObRootService::ObLoadDDLTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLScheduler &ddl_scheduler = root_service_.get_ddl_scheduler();
  if (OB_FAIL(ddl_scheduler.recover_task())) {
    LOG_WARN("load ddl task failed", K(ret));
  }
  return ret;
}

ObAsyncTask *ObRootService::ObLoadDDLTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObLoadDDLTask *task = nullptr;
  if (nullptr == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buf is not enough", K(buf_size), "request_size", sizeof(*this));
  } else {
    task = new (buf) ObLoadDDLTask(root_service_);
  }
  return task;
}

ObRootService::ObRefreshIOCalibrationTask::ObRefreshIOCalibrationTask(ObRootService &root_service)
  : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(INT64_MAX);
}

int ObRootService::ObRefreshIOCalibrationTask::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObAdminRefreshIOCalibrationArg arg;
  arg.only_refresh_ = true;
  if (OB_FAIL(root_service_.admin_refresh_io_calibration(arg))) {
    LOG_WARN("refresh io calibration failed", K(ret), K(arg));
  } else {
    LOG_INFO("refresh io calibration succeeded");
  }
  return ret;
}

ObAsyncTask *ObRootService::ObRefreshIOCalibrationTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObRefreshIOCalibrationTask *task = nullptr;
  if (nullptr == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buf is not enough", K(buf_size), "request_size", sizeof(*this));
  } else {
    task = new (buf) ObRefreshIOCalibrationTask(root_service_);
  }
  return task;
}

////////////////////
ObRootService::ObSelfCheckTask::ObSelfCheckTask(ObRootService &root_service)
:ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service)
{
  set_retry_times(0);  // don't retry when failed
}

int ObRootService::ObSelfCheckTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.self_check())) {
    LOG_WARN("fail to do root inspection check, please check it", K(ret));
  } else {
    LOG_INFO("self check success!");
  }
  return ret;
}

ObAsyncTask *ObRootService::ObSelfCheckTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObSelfCheckTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size));
  } else {
    task = new(buf) ObSelfCheckTask(root_service_);
  }
  return task;
}

/////////////////////////
ObRootService::ObUpdateAllServerConfigTask::ObUpdateAllServerConfigTask(ObRootService &root_service)
: ObAsyncTimerTask(root_service.task_queue_),
    root_service_(root_service)
{
  set_retry_times(INT64_MAX); // retry until success
}

int ObRootService::ObUpdateAllServerConfigTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.update_all_server_config())) {
    LOG_WARN("fail to update all server config", K(ret));
  } else {
    LOG_INFO("update all server config success");
  }
  return ret;
}

ObAsyncTask *ObRootService::ObUpdateAllServerConfigTask::deep_copy(char *buf, const int64_t buf_size) const
{
  ObUpdateAllServerConfigTask *task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN_RET(OB_BUF_NOT_ENOUGH, "buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObUpdateAllServerConfigTask(root_service_);
  }
  return task;
}

/////////////////////////
int ObRootService::admin_clear_balance_task(const obrpc::ObAdminClearBalanceTaskArg &args)
{
  // TODO: @wanhong.wwh NEED SUPPORT
  UNUSEDx(args);
  return 0;
}

status::ObRootServiceStatus ObRootService::get_status() const
{
  return rs_status_.get_rs_status();
}

int ObRootService::table_allow_ddl_operation(const obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const AlterTableSchema &alter_table_schema = arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const ObString &origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString &origin_table_name = alter_table_schema.get_origin_table_name();
  schema_guard.set_session_id(arg.session_id_);
  if (arg.is_refresh_sess_active_time()) {
    //do nothing
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invali argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, origin_database_name,
                                                   origin_table_name, false, schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
  } else if (OB_ISNULL(schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("invalid schema", K(ret));
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(origin_database_name), to_cstring(origin_table_name));
  } else if (schema->is_in_splitting()) {
    //TODO ddl must not execute on splitting table due to split not unstable
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is physical or logical split can not split", K(ret), K(schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "table is in physial or logical split, ddl operation");
  } else if (schema->is_ctas_tmp_table()) {
    if (!alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::SESSION_ID)) {
      //to prevet alter table after failed to create table, the table is invisible.
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("try to alter invisible table schema", K(schema->get_session_id()), K(arg));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "try to alter invisible table");
    }
  }
  return ret;
}

// ask each server to update statistic
int ObRootService::update_stat_cache(const obrpc::ObUpdateStatCacheArg &arg)
{
  int ret = OB_SUCCESS;
  ObZone null_zone;
  ObSEArray<ObAddr, 8> server_list;
  bool evict_plan_failed = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      if (OB_FAIL(rpc_proxy_.to(server_list.at(i)).update_local_stat_cache(arg))) {
        LOG_WARN("fail to update table statistic", K(ret), K(server_list.at(i)));
        // OB_SQL_PC_NOT_EXIST represent evict plan failed
        if (OB_SQL_PC_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          evict_plan_failed = true;
        }
      } else { /*do nothing*/}
    }
  }
  if (OB_SUCC(ret) && evict_plan_failed) {
    ret = OB_SQL_PC_NOT_EXIST;
  }
  return ret;
}

int ObRootService::check_weak_read_version_refresh_interval(int64_t refresh_interval, bool &valid)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard sys_schema_guard;
  ObArray<uint64_t> tenant_ids;
  valid = true;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_schema_guard))) {
    LOG_WARN("get sys schema guard failed", KR(ret));
  } else if (OB_FAIL(sys_schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get tenant ids failed", KR(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObSimpleTenantSchema *tenant_schema = NULL;
    const ObSysVarSchema *var_schema = NULL;
    ObObj obj;
    int64_t session_max_stale_time = 0;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    for (int64_t i = 0; OB_SUCC(ret) && valid && i < tenant_ids.count(); i++) {
      tenant_id = tenant_ids[i];
      if (OB_FAIL(sys_schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_SUCCESS;
        LOG_WARN("tenant schema is null, skip and continue", KR(ret), K(tenant_id));
      } else if (!tenant_schema->is_normal()) {
        ret = OB_SUCCESS;
        LOG_WARN("tenant schema is not normal, skip and continue", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id,
                         OB_SV_MAX_READ_STALE_TIME, var_schema))) {
        LOG_WARN("get tenant system variable failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(var_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("var schema is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(var_schema->get_value(NULL, NULL, obj))) {
        LOG_WARN("get value failed", KR(ret), K(tenant_id), K(obj));
      } else if (OB_FAIL(obj.get_int(session_max_stale_time))) {
        LOG_WARN("get int failed", KR(ret), K(tenant_id), K(obj));
      } else if (session_max_stale_time != share::ObSysVarFactory::INVALID_MAX_READ_STALE_TIME
                 && refresh_interval > session_max_stale_time) {
        valid = false;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "weak_read_version_refresh_interval is larger than ob_max_read_stale_time");
      }
    }
  }
  return ret;
}

int ObRootService::set_config_pre_hook(obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret) {
    bool valid = true;
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, K(ret));
    } else if (0 == STRCMP(item->name_.ptr(), FREEZE_TRIGGER_PERCENTAGE)) {
      // check write throttle percentage
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        valid = valid && ObConfigFreezeTriggerIntChecker::check(item->tenant_ids_.at(i), *item);
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant freeze_trigger_percentage which should smaller than writing_throttling_trigger_percentage");
          LOG_WARN("config invalid", "item", *item, K(ret), K(i), K(item->tenant_ids_.at(i)));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), WRITING_THROTTLEIUNG_TRIGGER_PERCENTAGE)) {
      // check freeze trigger
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        valid = valid && ObConfigWriteThrottleTriggerIntChecker::check(item->tenant_ids_.at(i), *item);
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "tenant writing_throttling_trigger_percentage which should greater than freeze_trigger_percentage");
          LOG_WARN("config invalid", "item", *item, K(ret), K(i), K(item->tenant_ids_.at(i)));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), WEAK_READ_VERSION_REFRESH_INTERVAL)) {
      int64_t refresh_interval = ObConfigTimeParser::get(item->value_.ptr(), valid);
      if (valid && OB_FAIL(check_weak_read_version_refresh_interval(refresh_interval, valid))) {
        LOG_WARN("check refresh interval failed ", KR(ret), K(*item));
      } else if (!valid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("config invalid", KR(ret), K(*item));
      }
    } else if (0 == STRCMP(item->name_.ptr(), PARTITION_BALANCE_SCHEDULE_INTERVAL)) {
      const int64_t DEFAULT_BALANCER_IDLE_TIME = 10 * 1000 * 1000L; // 10s
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        const uint64_t tenant_id = item->tenant_ids_.at(i);
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
        int64_t balancer_idle_time = tenant_config.is_valid() ? tenant_config->balancer_idle_time : DEFAULT_BALANCER_IDLE_TIME;
        int64_t interval = ObConfigTimeParser::get(item->value_.ptr(), valid);
        if (valid) {
          if (0 == interval) {
            valid = true;
          } else if (interval >= balancer_idle_time) {
            valid = true;
          } else {
            valid = false;
            char err_msg[DEFAULT_BUF_LENGTH];
            (void)snprintf(err_msg, sizeof(err_msg), "partition_balance_schedule_interval of tenant %ld, "
                "it should not be less than balancer_idle_time", tenant_id);
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, err_msg);
          }
        }
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("config invalid", KR(ret), K(*item), K(balancer_idle_time), K(tenant_id));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), BALANCER_IDLE_TIME)) {
      const int64_t DEFAULT_PARTITION_BALANCE_SCHEDULE_INTERVAL = 2 * 3600 * 1000 * 1000L; // 2h
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        const uint64_t tenant_id = item->tenant_ids_.at(i);
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
        int64_t interval = tenant_config.is_valid()
            ? tenant_config->partition_balance_schedule_interval
            : DEFAULT_PARTITION_BALANCE_SCHEDULE_INTERVAL;
        int64_t idle_time = ObConfigTimeParser::get(item->value_.ptr(), valid);
        if (valid && (idle_time > interval)) {
          valid = false;
          char err_msg[DEFAULT_BUF_LENGTH];
          (void)snprintf(err_msg, sizeof(err_msg), "balancer_idle_time of tenant %ld, "
              "it should not be longer than partition_balance_schedule_interval", tenant_id);
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, err_msg);
        }
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("config invalid", KR(ret), K(*item), K(interval), K(tenant_id));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), LOG_DISK_UTILIZATION_LIMIT_THRESHOLD)) {
      // check log_disk_utilization_limit_threshold
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        valid = valid && ObConfigLogDiskLimitThresholdIntChecker::check(item->tenant_ids_.at(i), *item);
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log_disk_utilization_limit_threshold should be greater than log_disk_throttling_percentage "
                        "when log_disk_throttling_percentage is not equal to 100");
          LOG_WARN("config invalid", "item", *item, K(ret), K(i), K(item->tenant_ids_.at(i)));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), LOG_DISK_THROTTLING_PERCENTAGE)) {
      // check log_disk_throttling_percentage
      for (int i = 0; i < item->tenant_ids_.count() && valid; i++) {
        valid = valid && ObConfigLogDiskThrottlingPercentageIntChecker::check(item->tenant_ids_.at(i), *item);
        if (!valid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log_disk_throttling_percentage should be equal to 100 or smaller than log_disk_utilization_limit_threshold");
          LOG_WARN("config invalid", "item", *item, K(ret), K(i), K(item->tenant_ids_.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObRootService::set_config_post_hook(const obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret) {
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, K(ret));
    } else if (0 == STRCMP(item->name_.ptr(), ENABLE_REBALANCE)
               || 0 == STRCMP(item->name_.ptr(), ENABLE_REREPLICATION)) {
      // TODO: @wanhong.wwh SUPPORT clear DR task after disable rebalance and rereplication
    } else if (0 == STRCMP(item->name_.ptr(), MERGER_CHECK_INTERVAL)) {
      //daily_merge_scheduler_.wakeup();
    } else if (0 == STRCMP(item->name_.ptr(), ENABLE_AUTO_LEADER_SWITCH)) {
      //wake_up leader_cooridnator
    } else if (0 == STRCMP(item->name_.ptr(), OBCONFIG_URL)) {
      int tmp_ret = OB_SUCCESS;
      bool force_update = true;
      if (OB_SUCCESS != (tmp_ret = submit_update_rslist_task(force_update))) {
        LOG_WARN("fail to submit update rs list task", KR(ret), K(tmp_ret));
      }
      LOG_INFO("obconfig_url parameters updated, force submit update rslist task", KR(tmp_ret),
          KPC(item));
    } else if (0 == STRCMP(item->name_.ptr(), SCHEMA_HISTORY_RECYCLE_INTERVAL)) {
      schema_history_recycler_.wakeup();
      LOG_INFO("schema_history_recycle_interval parameters updated, wakeup schema_history_recycler",
               KPC(item));
    }
  }
  return ret;
}

//ensure execute on DDL thread
int ObRootService::force_create_sys_table(const obrpc::ObForceCreateSysTableArg &arg)
{
  return OB_NOT_SUPPORTED;
}

// set tenant's locality
// TODO
//  1. set all locality of normal tenant to DEFAULT first.
//  2. verify that replica distribution satifies the new locality
int ObRootService::force_set_locality(const obrpc::ObForceSetLocalityArg &arg)
{
  LOG_INFO("receive force set locality arg", K(arg));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  if (!inited_) {
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()
             || is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret));
  } else {
    ObTenantSchema new_tenant;
    if (OB_FAIL(new_tenant.assign(*tenant_schema))) {
      LOG_WARN("fail to assgin tenant schema", KR(ret), KPC(tenant_schema));
    } else if (OB_FAIL(new_tenant.set_locality(arg.locality_))) {
      LOG_WARN("fail to set locality", KR(ret), K(arg));
    } else if (OB_FAIL(new_tenant.set_previous_locality(ObString("")))) {
      LOG_WARN("fail to reset previous locality", KR(ret), K(arg));
    } else if (OB_FAIL(ddl_service_.force_set_locality(schema_guard, new_tenant))) {
      LOG_WARN("fail to force set locality", K(ret), K(new_tenant));
    }
  }
  LOG_INFO("force set locality", K(arg));
  return ret;
}

int ObRootService::clear_special_cluster_schema_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else {
      schema_service->set_cluster_schema_status(
          ObClusterSchemaStatus::NORMAL_STATUS);
    }
  }
  return ret;
}

int ObRootService::get_is_in_bootstrap(bool &is_bootstrap) const
{
  int ret = OB_SUCCESS;
  is_bootstrap = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else if (ObClusterSchemaStatus::BOOTSTRAP_STATUS
        == schema_service->get_cluster_schema_status()) {
      is_bootstrap = true;
    }
  }
  return ret;
}

int ObRootService::log_nop_operation(const obrpc::ObDDLNopOpreatorArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to log nop operation", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.log_nop_operation(arg))) {
    LOG_WARN("failed to log nop operation", K(ret), K(arg));
  }
  return ret;
}

// if tenant_id =  OB_INVALID_TENANT_ID, indicates refresh all tenants's schema;
// otherwise, refresh specify tenant's schema. ensure schema_version not fallback by outer layer logic.
int ObRootService::broadcast_schema(const obrpc::ObBroadcastSchemaArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receieve broadcast_schema request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP_(schema_service));
  } else {
    ObRefreshSchemaInfo schema_info;
    ObSchemaService *schema_service = schema_service_->get_schema_service();
    if (OB_INVALID_TENANT_ID != arg.tenant_id_) {
      // tenant_id is valid, just refresh specify tenant's schema.
      schema_info.set_tenant_id(arg.tenant_id_);
      schema_info.set_schema_version(arg.schema_version_);
    } else {
      // tenant_id =  OB_INVALID_TENANT_ID, indicates refresh all tenants's schema;
      if (OB_FAIL(schema_service->inc_sequence_id())) {
        LOG_WARN("increase sequence_id failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_service->inc_sequence_id())) {
      LOG_WARN("increase sequence_id failed", K(ret));
    } else if (OB_FAIL(schema_service->set_refresh_schema_info(schema_info))) {
      LOG_WARN("fail to set refresh schema info", K(ret), K(schema_info));
    }
  }
  LOG_INFO("end broadcast_schema request", K(ret), K(arg));
  return ret;
}

/*
 * standby_cluster, will return local tenant's schema_version
 * primary_cluster, will return tenant's newest schema_version
 *   - schema_version = OB_CORE_SCHEMA_VERSION, indicate the tenant is garbage.
 *   - schema_version = OB_INVALID_VERSION, indicate that it is failed to get schame_version.
 *   - schema_version > OB_CORE_SCHEMA_VERSION, indicate that the schema_version is valid.
 */
int ObRootService::get_tenant_schema_versions(
    const obrpc::ObGetSchemaArg &arg,
    obrpc::ObTenantSchemaVersions &tenant_schema_versions)
{
  int ret = OB_SUCCESS;
  tenant_schema_versions.reset();
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    int64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t schema_version = 0;
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
      ObSchemaGetterGuard tenant_schema_guard;
      tenant_id = tenant_ids.at(i);
      schema_version = 0;
      if (OB_SYS_TENANT_ID == tenant_id
          || STANDBY_CLUSTER == ObClusterInfoGetter::get_cluster_role_v2()) {
        // 对于备库，由于schema_status不在DDL线程推进，且能接受最终一致，
        // 故只需取本地schema版本即可
        if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                    tenant_id, schema_version))) {
          LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
        }
      } else {
        // for primary cluster, need to get newest schema_version from inner table.
        ObRefreshSchemaStatus schema_status;
        schema_status.tenant_id_ = tenant_id;
        int64_t version_in_inner_table = OB_INVALID_VERSION;
        bool is_restore = false;
        if (OB_FAIL(schema_service_->check_tenant_is_restore(&schema_guard, tenant_id, is_restore))) {
          LOG_WARN("fail to check tenant is restore", KR(ret), K(tenant_id));
        } else if (is_restore) {
          ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
          if (OB_ISNULL(schema_status_proxy)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema_status_proxy is null", KR(ret));
          } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, schema_status))) {
            LOG_WARN("failed to get tenant refresh schema status", KR(ret), K(tenant_id));
          } else if (OB_INVALID_VERSION != schema_status.readable_schema_version_) {
            ret = OB_EAGAIN;
            LOG_WARN("tenant's sys replicas are not restored yet, try later", KR(ret), K(tenant_id));
          }
        }
        if (FAILEDx(schema_service_->get_schema_version_in_inner_table(
                    sql_proxy_, schema_status, version_in_inner_table))) {
          // failed tenant creation, inner table is empty, return OB_CORE_SCHEMA_VERSION
          if (OB_EMPTY_RESULT == ret) {
            LOG_INFO("create tenant maybe failed", K(ret), K(tenant_id));
            schema_version = OB_CORE_SCHEMA_VERSION;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get latest schema version in inner table", K(ret));
          }
        } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                           tenant_id, schema_version))) {
          LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
        } else if (schema_version < version_in_inner_table) {
          ObArray<uint64_t> tenant_ids;
          if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push back tenant_id", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service_->refresh_and_add_schema(tenant_ids))) {
            LOG_WARN("fail to refresh schema", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(
                             tenant_id, schema_version))) {
            LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
          } else if (schema_version < version_in_inner_table) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("local version is still less than version in table",
                     K(ret), K(tenant_id), K(schema_version), K(version_in_inner_table));
          } else {}
        } else {}
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tenant_schema_versions.add(tenant_id, schema_version))) {
        LOG_WARN("fail to add tenant schema version", KR(ret), K(tenant_id), K(schema_version));
      }
      if (OB_FAIL(ret) && arg.ignore_fail_ && OB_SYS_TENANT_ID != tenant_id) {
        int64_t invalid_schema_version = OB_INVALID_SCHEMA_VERSION;
        if (OB_FAIL(tenant_schema_versions.add(tenant_id, invalid_schema_version))) {
          LOG_WARN("fail to add tenant schema version", KR(ret), K(tenant_id), K(schema_version));
        }
      }
    } // end for
  }
  return ret;
}

int ObRootService::generate_user(const ObClusterRole &cluster_role,
                                 const char* user_name,
                                 const char* user_passwd)
{
  int ret = OB_SUCCESS;
  ObSqlString ddl_stmt_str;
  int64_t affected_row = 0;
  ObString passwd(user_passwd);
  ObString encry_passwd;
  char enc_buf[ENC_BUF_LEN] = {0};
  if (OB_ISNULL(user_name) || OB_ISNULL(user_passwd)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_name), K(user_passwd));
  } else if (PRIMARY_CLUSTER != cluster_role) {
    LOG_INFO("standby cluster, no need to create user", K(cluster_role));
  } else if (OB_FAIL(sql::ObCreateUserExecutor::encrypt_passwd(passwd, encry_passwd, enc_buf, ENC_BUF_LEN))) {
    LOG_WARN("Encrypt passwd failed", K(ret));
  } else if (OB_FAIL(ObDDLSqlGenerator::gen_create_user_sql(ObAccountArg(user_name, OB_SYS_HOST_NAME),
                                                     encry_passwd, ddl_stmt_str))) {
    LOG_WARN("fail to gen create user sql", KR(ret));
  } else if (OB_FAIL(sql_proxy_.write(ddl_stmt_str.ptr(), affected_row))) {
    LOG_WARN("execute sql failed", K(ret), K(ddl_stmt_str));
  } else {
    LOG_INFO("create user success", K(user_name), K(affected_row));
  }
  ddl_stmt_str.reset();
  if (OB_FAIL(ret) || PRIMARY_CLUSTER != cluster_role) {
    //nothing todo
  } else if (OB_FAIL(ddl_stmt_str.assign_fmt("grant select on *.* to '%s'",
                                             user_name))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(sql_proxy_.write(ddl_stmt_str.ptr(), affected_row))) {
    LOG_WARN("fail to write", KR(ret), K(ddl_stmt_str));
  } else {
    LOG_INFO("grant privilege success", K(ddl_stmt_str));
  }
  return ret;
}

int ObRootService::get_recycle_schema_versions(
    const obrpc::ObGetRecycleSchemaVersionsArg &arg,
    obrpc::ObGetRecycleSchemaVersionsResult &result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive get recycle schema versions request", K(arg));
  bool is_standby = GCTX.is_standby_cluster();
  bool in_service = is_full_service();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (!is_standby || !in_service) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("should be standby cluster and rs in service",
             KR(ret), K(is_standby), K(in_service));
  } else if (OB_FAIL(schema_history_recycler_.get_recycle_schema_versions(arg, result))) {
    LOG_WARN("fail to get recycle schema versions", KR(ret), K(arg));
  }
  LOG_INFO("get recycle schema versions", KR(ret), K(arg), K(result));
  return ret;
}
int ObRootService::do_profile_ddl(const obrpc::ObProfileDDLArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.handle_profile_ddl(arg))) {
    LOG_WARN("handle ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::rebuild_index_in_restore(
    const obrpc::ObRebuildIndexInRestoreArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  return ret;
}

int ObRootService::handle_archive_log(const obrpc::ObArchiveLogArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("handle_archive_log", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObBackupServiceProxy::handle_archive_log(arg))) {
    LOG_WARN("failed to handle archive log", K(ret));
  }
  return ret;
}

int ObRootService::handle_backup_database(const obrpc::ObBackupDatabaseArg &in_arg)
{
  int ret = OB_SUCCESS;
	if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObBackupServiceProxy::handle_backup_database(in_arg))) {
    LOG_WARN("failed to handle backup database", K(ret), K(in_arg));
  }
  FLOG_INFO("handle_backup_database", K(ret), K(in_arg));
  return ret;
}

int ObRootService::handle_validate_database(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("not supported now", K(ret), K(arg));
  return ret;
}

int ObRootService::handle_validate_backupset(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("not supported now", K(ret), K(arg));
  return ret;
}

int ObRootService::handle_cancel_validate(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("not supported now", K(ret), K(arg));
  return ret;
}


int ObRootService::disaster_recovery_task_reply(
    const obrpc::ObDRTaskReplyResult &arg)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RS_DEAL_WITH_RPC);
  FLOG_INFO("[DRTASK_NOTICE] receive disaster recovery task reply", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(disaster_recovery_task_mgr_.deal_with_task_reply(arg))) {
    LOG_WARN("fail to execute over", KR(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_backup_manage(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (arg.type_) {
    case ObBackupManageArg::CANCEL_BACKUP: {
      if (OB_FAIL(handle_backup_database_cancel(arg))) {
        LOG_WARN("failed to handle backup database cancel", K(ret), K(arg));
      }
      break;
    };
    case ObBackupManageArg::VALIDATE_DATABASE: {
      if (OB_FAIL(handle_validate_database(arg))) {
        LOG_WARN("failed to handle validate database", K(ret), K(arg));
      }
      break;
    };
    case ObBackupManageArg::VALIDATE_BACKUPSET: {
      if (OB_FAIL(handle_validate_backupset(arg))) {
        LOG_WARN("failed to handle validate backupset", K(ret), K(arg));
      }
      break;
    };
    case ObBackupManageArg::CANCEL_VALIDATE: {
      if (OB_FAIL(handle_cancel_validate(arg))) {
        LOG_WARN("failed to handle cancel validate", K(ret), K(arg));
      }
      break;
    };
    case ObBackupManageArg::CANCEL_BACKUP_BACKUPSET: {
      if (OB_FAIL(handle_cancel_backup_backup(arg))) {
        LOG_WARN("failed to handle cancel backup backup", K(ret), K(arg));
      }
      break;
    }
    case ObBackupManageArg::CANCEL_BACKUP_BACKUPPIECE: {
      if (OB_FAIL(handle_cancel_backup_backup(arg))) {
        LOG_WARN("failed to handle cancel backup backup", K(ret), K(arg));
      }
      break;
    }
    case ObBackupManageArg::CANCEL_ALL_BACKUP_FORCE: {
      if (OB_FAIL(handle_cancel_all_backup_force(arg))) {
        LOG_WARN("failed to handle cancel all backup force", K(ret), K(arg));
      }
      break;
    };
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid backup manage arg", K(ret), K(arg));
      break;
    }
    }
  }

  FLOG_INFO("finish handle_backup_manage", K(ret), K(arg));
  return ret;
}

int ObRootService::handle_backup_delete(const obrpc::ObBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
	if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObBackupServiceProxy::handle_backup_delete(arg))) {
    LOG_WARN("failed to handle backup delete", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_delete_policy(const obrpc::ObDeletePolicyArg &arg)
{
  int ret = OB_SUCCESS;
	if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
	} else if (OB_FAIL(ObBackupServiceProxy::handle_delete_policy(arg))) {
    LOG_WARN("failed to handle delete policy", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_backup_database_cancel(
    const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObBackupManageArg::CANCEL_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup database cancel get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ObBackupServiceProxy::handle_backup_database_cancel(arg))) {
    LOG_WARN("failed to start schedule backup cancel", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::check_backup_scheduler_working(Bool &is_working)
{
  int ret = OB_NOT_SUPPORTED;
  is_working = true;

  FLOG_INFO("not support check backup scheduler working, should not use anymore", K(ret), K(is_working));
  return ret;
}

ObRootService::ObTenantGlobalContextCleanTimerTask::ObTenantGlobalContextCleanTimerTask(
                                               ObRootService &root_service)
  : root_service_(root_service)
{
}

int ObRootService::ObTenantGlobalContextCleanTimerTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, SCHEDULE_PERIOD, true);
}

void ObRootService::ObTenantGlobalContextCleanTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.clean_global_context())) {
    LOG_WARN("failed to clean global context", K(ret));
  }
}

int ObRootService::handle_cancel_backup_backup(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("not supported now", K(ret), K(arg));
  return ret;
}

int ObRootService::handle_cancel_all_backup_force(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_ERROR("not support now", K(ret), K(arg));
  return ret;
}

void ObRootService::reset_fail_count()
{
  ATOMIC_STORE(&fail_count_, 0);
}

void ObRootService::update_fail_count(int ret)
{
  int64_t count = ATOMIC_AAF(&fail_count_, 1);
  if (count > OB_ROOT_SERVICE_START_FAIL_COUNT_UPPER_LIMIT
      && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    LOG_ERROR("rs_monitor_check : fail to start root service", KR(ret), K(count));
  } else {
    LOG_WARN("rs_monitor_check : fail to start root service", KR(ret), K(count));
  }
  LOG_DBA_WARN(OB_ERR_ROOTSERVICE_START, "msg", "rootservice start()/do_restart() has failure",
               KR(ret), "fail_cnt", count);
}

int ObRootService::send_physical_restore_result(const obrpc::ObPhysicalRestoreResult &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(res));
  } else {
    ret = OB_NOT_SUPPORTED;
    //TODO set physical restore result
  }
  LOG_INFO("get physical restore job's result", K(ret), K(res));
  return ret;
}

int ObRootService::create_restore_point(const obrpc::ObCreateRestorePointArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  LOG_WARN("craete restpre point is not supported now", K(ret));
  return ret;
}

int ObRootService::drop_restore_point(const obrpc::ObDropRestorePointArg &arg)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(arg);
  LOG_WARN("drop restpre point is not supported now", K(ret));
  return ret;
}

int ObRootService::build_ddl_single_replica_response(const obrpc::ObDDLBuildSingleReplicaResponseArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive build ddl single replica response", K(arg));
  ObDDLTaskInfo info;
  info.row_scanned_ = arg.row_scanned_;
  info.row_inserted_ = arg.row_inserted_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(ddl_scheduler_.on_sstable_complement_job_reply(
      arg.tablet_id_/*source tablet id*/, ObDDLTaskKey(arg.dest_tenant_id_, arg.dest_schema_id_, arg.dest_schema_version_), arg.snapshot_version_, arg.execution_id_, arg.ret_code_, info))) {
    LOG_WARN("handle column checksum calc response failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::purge_recyclebin_objects(int64_t purge_each_time)
{
  int ret = OB_SUCCESS;
  // always passed
  int64_t expire_timeval = GCONF.recyclebin_object_expire_time;
  ObSEArray<uint64_t, 16> tenant_ids;
  ObSchemaGetterGuard guard;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_serviece_ is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get sys schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get all tenants failed", KR(ret));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    obrpc::Int64 expire_time = current_time - expire_timeval;
    const int64_t SLEEP_INTERVAL = 100 * 1000;  //100ms interval of send rpc
    const int64_t PURGE_EACH_RPC = 10;          //delete count per rpc
    obrpc::Int64 affected_rows = 0;
    obrpc::ObPurgeRecycleBinArg arg;
    int64_t purge_sum = purge_each_time;
    const bool is_standby = PRIMARY_CLUSTER != ObClusterInfoGetter::get_cluster_role_v2();
    const ObSimpleTenantSchema *simple_tenant = NULL;
    //ignore ret
    for (int i = 0; i < tenant_ids.count() && in_service() && purge_sum > 0; ++i) {
      int64_t purge_time = GCONF._recyclebin_object_purge_frequency;
      const uint64_t tenant_id = tenant_ids.at(i);
      if (purge_time <= 0) {
        break;
      }
      if (OB_SYS_TENANT_ID != tenant_id && is_standby) {
        // standby cluster won't purge recyclebin automacially.
        LOG_TRACE("user tenant won't purge recyclebin automacially in standby cluster", K(tenant_id));
        continue;
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id, simple_tenant))) {
        LOG_WARN("fail to get simple tenant schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(simple_tenant)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("simple tenant schema not exist", KR(ret), K(tenant_id));
      } else if (!simple_tenant->is_normal()) {
        // only deal with normal tenant.
        LOG_TRACE("tenant which isn't normal won't purge recyclebin automacially", K(tenant_id));
        continue;
      }
      // ignore error code of different tenant
      ret = OB_SUCCESS;
      affected_rows = 0;
      arg.tenant_id_ = tenant_id;
      arg.expire_time_ = expire_time;
      arg.auto_purge_ = true;
      arg.exec_tenant_id_ = tenant_id;
      LOG_INFO("start purge recycle objects of tenant", K(arg), K(purge_sum));
      while (OB_SUCC(ret) && in_service() && purge_sum > 0) {
        int64_t cal_timeout = 0;
        int64_t start_time = ObTimeUtility::current_time();
        arg.purge_num_ = purge_sum > PURGE_EACH_RPC ? PURGE_EACH_RPC : purge_sum;
        if (OB_FAIL(schema_service_->cal_purge_need_timeout(arg, cal_timeout))) {
          LOG_WARN("fail to cal purge need timeout", KR(ret), K(arg));
        } else if (0 == cal_timeout) {
          purge_sum = 0;
        } else if (OB_FAIL(common_proxy_.timeout(cal_timeout).purge_expire_recycle_objects(arg, affected_rows))) {
          LOG_WARN("purge reyclebin objects failed", KR(ret),
              K(current_time), K(expire_time), K(affected_rows), K(arg));
        } else {
          purge_sum -= affected_rows;
          if (arg.purge_num_ != affected_rows) {
            int64_t cost_time = ObTimeUtility::current_time() - start_time;
            LOG_INFO("purge recycle objects", KR(ret), K(cost_time),
                K(expire_time), K(current_time), K(affected_rows));
            if (OB_SUCC(ret) && in_service()) {
              ob_usleep(SLEEP_INTERVAL);
            }
            break;
          }
        }
        int64_t cost_time = ObTimeUtility::current_time() - start_time;
        LOG_INFO("purge recycle objects", KR(ret), K(cost_time),
            K(expire_time), K(current_time), K(affected_rows));
        if (OB_SUCC(ret) && in_service()) {
          ob_usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}

int ObRootService::flush_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg)
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  ObSEArray<ObAddr, 8> server_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(empty_zone, server_list))) {
    LOG_WARN("fail to get alive server", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      if (OB_FAIL(rpc_proxy_.to(server_list.at(i)).flush_local_opt_stat_monitoring_info(arg))) {
        LOG_WARN("fail to update table statistic", K(ret), K(server_list.at(i)));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObRootService::clean_global_context()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(tenant_ids));
  } else {
    ObGlobalContextOperator ctx_operator;
    if (OB_FAIL(ctx_operator.clean_global_context(tenant_ids, sql_proxy_, *schema_service_))) {
      LOG_WARN("failed to clean global context", K(ret));
    }
  }
  return ret;
}

int ObRootService::admin_set_backup_config(const obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid backup config arg", K(ret));
  } else if (!arg.is_backup_config_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("admin set config type not backup config", K(ret), K(arg));
  }
  share::BackupConfigItemPair config_item;
  share::ObBackupConfigParserMgr config_parser_mgr;
  ARRAY_FOREACH_X(arg.items_, i , cnt, OB_SUCC(ret)) {
    const ObAdminSetConfigItem &item = arg.items_.at(i);
    uint64_t exec_tenant_id = OB_INVALID_TENANT_ID;
    ObMySQLTransaction trans;
    config_parser_mgr.reset();
    if ((common::is_sys_tenant(item.exec_tenant_id_) && item.tenant_name_.is_empty())
        || (common::is_user_tenant(item.exec_tenant_id_) && !item.tenant_name_.is_empty())
        || common::is_meta_tenant(item.exec_tenant_id_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("backup config only support user tenant", K(ret));
    } else if (!item.tenant_name_.is_empty()) {
      schema::ObSchemaGetterGuard guard;
      if (OB_ISNULL(schema_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service must not be null", K(ret));
      } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret));
      } else if (OB_FAIL(guard.get_tenant_id(ObString(item.tenant_name_.ptr()), exec_tenant_id))) {
        LOG_WARN("fail to get tenant id", K(ret));
      }
    } else {
      exec_tenant_id = item.exec_tenant_id_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(&sql_proxy_, gen_meta_tenant_id(exec_tenant_id)))) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      common::ObSqlString name;
      common::ObSqlString value;
      if (OB_FAIL(name.assign(item.name_.ptr()))) {
        LOG_WARN("fail to assign name", K(ret));
      } else if (OB_FAIL(value.assign(item.value_.ptr()))) {
        LOG_WARN("fail to assign value", K(ret));
      } else if (OB_FAIL(config_parser_mgr.init(name, value, exec_tenant_id))) {
        LOG_WARN("fail to init backup config parser mgr", K(ret), K(item));
      } else if (OB_FAIL(config_parser_mgr.update_inner_config_table(rpc_proxy_, trans))) {
        LOG_WARN("fail to update inner config table", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true))) {
          LOG_WARN("fail to commit trans", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("fail to rollback trans", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::cancel_ddl_task(const ObCancelDDLTaskArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive cancel ddl task", K(arg));
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.cancel_task(arg.get_task_id()))) {
    LOG_WARN("cancel task failed", K(ret));
  } else {
    LOG_INFO("succeed to cancel ddl task", K(arg));
  }
  return ret;
}

int ObRootService::check_majority_and_log_in_sync(
    const ObIArray<ObAddr> &to_stop_servers,
    const bool skip_log_sync_check,
    const char *print_str)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator ls_status_op;
  bool need_retry = false;
  const int64_t CHECK_RETRY_INTERVAL = 100 * 1000; // 100ms
  const int64_t RESERVED_TIME = 500 * 1000; // 500ms
  int64_t start_time = ObTimeUtility::current_time();
  int64_t abs_timeout_us = OB_INVALID_TIMESTAMP;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_RETRY_TIMEOUT = GCONF.internal_sql_execute_timeout;
  LOG_INFO("check majority and log in sync start",
      K(to_stop_servers), K(skip_log_sync_check), K(DEFAULT_RETRY_TIMEOUT));

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_RETRY_TIMEOUT))) {
    LOG_WARN("failed to set default timeout ctx", KR(ret), K(DEFAULT_RETRY_TIMEOUT));
  } else {
    abs_timeout_us = ctx.get_abs_timeout() - RESERVED_TIME;
    if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
      LOG_WARN("fail to set abs timeout", KR(ret), K(abs_timeout_us));
    } else {
      do {
        if (need_retry) {
          ob_usleep(CHECK_RETRY_INTERVAL);
        }
        if (OB_FAIL(ls_status_op.check_all_ls_has_majority_and_log_sync(
            to_stop_servers,
            skip_log_sync_check,
            print_str,
            *schema_service_,
            sql_proxy_,
            need_retry))) {
          LOG_WARN("fail to get and check all ls_paxos_info", KR(ret),
              K(to_stop_servers), K(skip_log_sync_check));
        }
      } while ((OB_OP_NOT_ALLOW == ret) && need_retry);
    }
  }
  LOG_INFO("check majority and log in sync finish", K(to_stop_servers),
      K(skip_log_sync_check), "cost_time", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObRootService::check_all_ls_has_leader(const char *print_str)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator ls_status_op;
  const int64_t CHECK_RETRY_INTERVAL = 100 * 1000; // 100ms
  const int64_t RESERVED_TIME = 500 * 1000; // 500ms
  int64_t start_time = ObTimeUtility::current_time();
  int64_t abs_timeout_us = OB_INVALID_TIMESTAMP;
  bool has_ls_without_leader = false;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_RETRY_TIMEOUT = GCONF.internal_sql_execute_timeout;
  ObSqlString last_error_msg;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_RETRY_TIMEOUT))) {
    LOG_WARN("failed to set default timeout ctx", KR(ret), K(DEFAULT_RETRY_TIMEOUT));
  } else {
    abs_timeout_us = ctx.get_abs_timeout() - RESERVED_TIME;
    if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_us))) {
      LOG_WARN("fail to set abs timeout", KR(ret), K(abs_timeout_us));
    } else {
      do {
        if (has_ls_without_leader) {
          ob_usleep(CHECK_RETRY_INTERVAL);
        }
        if (OB_FAIL(ls_status_op.check_all_ls_has_leader(
            sql_proxy_,
            print_str,
            has_ls_without_leader,
            last_error_msg))) {
          LOG_WARN("fail to check all ls has leader", KR(ret), K(print_str));
        }
      } while (OB_OP_NOT_ALLOW == ret && has_ls_without_leader);
    }
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_OP_NOT_ALLOW;
    if (!last_error_msg.empty()) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, last_error_msg.ptr());
    } else {
      LOG_WARN("fail to check all ls has leader because inner sql timeout", KR(ret), K(print_str));
      char err_msg[OB_TMP_BUF_SIZE_256];
      (void)snprintf(err_msg, sizeof(err_msg), "check leader for all LS timeout, %s", print_str);
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, err_msg);
    }
  }
  LOG_INFO("check all ls has leader finish", KR(ret), K(abs_timeout_us), K(start_time),
      "cost_time", ObTimeUtility::current_time() - start_time);
  return ret;
}

void ObRootService::update_cpu_quota_concurrency_in_memory_()
{
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
    tenant_config->cpu_quota_concurrency = MAX(10, tenant_config->cpu_quota_concurrency);
  }
}

int ObRootService::set_cpu_quota_concurrency_config_()
{
  int64_t affected_rows = 0;
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_proxy_.write("ALTER SYSTEM SET cpu_quota_concurrency = 10;", affected_rows))) {
    LOG_WARN("update cpu_quota_concurrency failed", K(ret));
  } else if (OB_FAIL(check_config_result("cpu_quota_concurrency", "10"))) {
    LOG_WARN("failed to check config same", K(ret));
  }
  return ret;
}

int ObRootService::handle_recover_table(const obrpc::ObRecoverTableArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("succeed received recover table arg", K(arg));
  uint64_t data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (GCTX.is_standby_cluster()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover table in standby tenant is not allowed", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover table in standby tenant");
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("recover table in upgrade mode is not allowed", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Cluster is in upgrade mode, recover table is");
  } else if (OB_FAIL(ObRecoverTableUtil::check_compatible(arg.tenant_id_))) {
    LOG_WARN("check recover table compatible failed", K(ret), K(arg));
  } else {
    ObRecoverTableInitiator initiator;
    bool is_exist = false;
    if (OB_FAIL(initiator.init(schema_service_, &sql_proxy_))) {
      LOG_WARN("failed to init ObRecoverTableInitiator", K(ret));
    } else if (ObRecoverTableArg::Action::INITIATE == arg.action_
        && OB_FAIL(initiator.is_recover_job_exist(arg.tenant_id_, is_exist))) {
      LOG_WARN("failed to check recover job exist", K(ret), K(arg));
    } else if (is_exist) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("recover job is exist", K(ret), K(arg));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "recover table when recover table job exists is");
    } else if (OB_FAIL(initiator.initiate_recover_table(arg))) {
      LOG_WARN("failed to initiate table recover", K(ret), K(arg));
    } else {
      LOG_INFO("[RECOVER_TABLE] initiate recover table succeed", K(arg));
    }
  }
  return ret;
}

int ObRootService::recompile_all_views_batch(const obrpc::ObRecompileAllViewsBatchArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.recompile_all_views_batch(arg.tenant_id_, arg.view_ids_))) {
    LOG_WARN("failed to recompile all views", K(ret), K(arg.tenant_id_));
  }
  LOG_INFO("recompile all views batch finish", KR(ret), K(start_time),
      "cost_time", ObTimeUtility::current_time() - start_time);
  return ret;
}

int ObRootService::try_add_dep_infos_for_synonym_batch(const obrpc::ObTryAddDepInofsForSynonymBatchArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.try_add_dep_info_for_all_synonyms_batch(arg.tenant_id_, arg.synonym_ids_))) {
    LOG_WARN("failed to add synonym dep info", K(ret), K(arg.tenant_id_));
  }
  LOG_INFO("add dep infos for synonym batch finish", KR(ret), K(start_time),
      "cost_time", ObTimeUtility::current_time() - start_time);
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObRootService::handle_get_root_key(const obrpc::ObRootKeyArg &arg,
                                       obrpc::ObRootKeyResult &result)
{
  int ret = OB_SUCCESS;
  ObRootKey root_key;
  if (OB_UNLIKELY(arg.is_set_ || !arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(arg.tenant_id_, root_key))) {
    LOG_WARN("failed to get root key", K(ret));
  } else if (obrpc::RootKeyType::INVALID != root_key.key_type_) {
    result.key_type_ = root_key.key_type_;
    result.root_key_ = root_key.key_;
  } else if (OB_FAIL(get_root_key_from_obs(arg, result))) {
    LOG_WARN("failed to get root key from obs", K(ret));
  }
  return ret;
}

int ObRootService::get_root_key_from_obs(const obrpc::ObRootKeyArg &arg,
                                         obrpc::ObRootKeyResult &result)
{
  int ret = OB_SUCCESS;
  ObZone empty_zone;
  ObArray<ObAddr> active_server_list;
  ObArray<ObAddr> inactive_server_list;
  const ObSimpleTenantSchema *simple_tenant = NULL;
  ObSchemaGetterGuard guard;
  const uint64_t tenant_id = arg.tenant_id_;
  bool enable_default = false;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_serviece_ is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get sys schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, simple_tenant))) {
    LOG_WARN("fail to get simple tenant schema", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(simple_tenant) && simple_tenant->is_normal()) {
    enable_default = true;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(SVR_TRACER.get_servers_by_status(empty_zone, active_server_list,
                                                      inactive_server_list))) {
    LOG_WARN("get alive servers failed", K(ret));
  } else if (OB_FAIL(ObDDLService::notify_root_key(rpc_proxy_, arg, active_server_list, result,
                                                   enable_default))) {
    LOG_WARN("failed to notify root key");
  }
  return ret;
}
#endif

} // end namespace rootserver
} // end namespace oceanbase
