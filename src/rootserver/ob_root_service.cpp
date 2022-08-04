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

#include "rootserver/ob_root_service.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_preload.h"
#include "share/ob_worker.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/file/file_directory_utils.h"
#include "lib/encrypt/ob_encrypted_helper.h"

#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_thread_mgr.h"
#include "common/ob_partition_key.h"
#include "common/ob_timeout_ctx.h"
#include "common/object/ob_object.h"
#include "share/ob_cluster_version.h"

#include "share/ob_define.h"
#include "share/ob_version.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_lease_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_config_manager.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
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
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_upgrade_utils.h"
#include "share/ob_multi_cluster_util.h"

#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"
#include "sql/engine/px/ob_px_util.h"

#include "rootserver/ob_bootstrap.h"
#include "rootserver/ob_schema2ddl_sql.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_update_rs_list_task.h"
#include "rootserver/ob_resource_weight_parser.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/restore/ob_restore_util.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_ddl_sql_generator.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/backup/ob_backup_scheduler.h"
#include "share/backup/ob_validate_scheduler.h"
#include "rootserver/ob_root_backup.h"
#include "rootserver/ob_backup_cancel_scheduler.h"
#include "rootserver/ob_backup_data_clean_scheduler.h"
#include "rootserver/backup/ob_cancel_validate_scheduler.h"
#include "rootserver/backup/ob_backup_backupset_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/ob_service.h"
#include "rootserver/backup/ob_cancel_delete_backup_scheduler.h"
#include "rootserver/backup/ob_cancel_backup_backup_scheduler.h"
#include "share/backup/ob_backup_backuppiece_operator.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace rootserver {

#define PUSH_BACK_TO_ARRAY_AND_SET_RET(array, msg)                    \
  do {                                                                \
    if (OB_FAIL(array.push_back(msg))) {                              \
      LOG_WARN("push reason array error", KR(ret), K(array), K(msg)); \
    }                                                                 \
  } while (0)

ObRootService::ObStatusChangeCallback::ObStatusChangeCallback(ObRootService& root_service) : root_service_(root_service)
{}

ObRootService::ObStatusChangeCallback::~ObStatusChangeCallback()
{}

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
    root_service_.get_daily_merge_scheduler().wakeup();
  }
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_server_status_change(const common::ObAddr& server)
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
  return ret;
}

int ObRootService::ObStatusChangeCallback::on_start_server(const common::ObAddr& server)
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

int ObRootService::ObStatusChangeCallback::on_stop_server(const common::ObAddr& server)
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

int ObRootService::ObStatusChangeCallback::on_offline_server(const common::ObAddr& server)
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

ObAsyncTask* ObRootService::ObStartStopServerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
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
  ObPartitionInfo partition_info;
  if (!server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_));
  } else if (OB_FAIL(root_service_.get_pt_operator().get(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 partition_info))) {
    LOG_WARN("fail to get", KR(ret));
  } else {
    arg.master_rs_ = GCONF.self_addr_;
    FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (replica->server_ == GCONF.self_addr_ ||
          (replica->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(replica->replica_type_))) {
        if (OB_FAIL(arg.rs_list_.push_back(replica->server_))) {
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

ObAsyncTask* ObRootService::ObOfflineServerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
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

int ObRootService::ObMergeErrorTask::process()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (OB_FAIL(root_service_.handle_merge_error())) {
    LOG_WARN("fail to handle merge error", K(ret));
  }
  return ret;
}

int64_t ObRootService::ObMergeErrorTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObRootService::ObMergeErrorTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObMergeErrorTask(root_service_);
  }
  return task;
}

int ObRootService::ObStatisticPrimaryZoneCountTask::process()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not inited", K(ret));
  } else if (OB_FAIL(root_service_.statistic_primary_zone_entity_count())) {
    LOG_WARN("fail to statistic primary zone entity count", K(ret));
  }
  return ret;
}

int64_t ObRootService::ObStatisticPrimaryZoneCountTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObRootService::ObStatisticPrimaryZoneCountTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = nullptr;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (nullptr == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(ret), K(need_size), K(buf_size));
  } else {
    task = new (buf) ObStatisticPrimaryZoneCountTask(root_service_);
  }
  return task;
}

int ObRootService::ObCreateHaGtsUtilTask::process()
{
  int ret = OB_SUCCESS;
  if (!root_service_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("root service not init", K(ret));
  } else if (OB_FAIL(root_service_.upgrade_cluster_create_ha_gts_util())) {
    LOG_WARN("fail to create ha gts util", K(ret));
  }
  return ret;
}

int64_t ObRootService::ObCreateHaGtsUtilTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObRootService::ObCreateHaGtsUtilTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = nullptr;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (nullptr == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(ret), K(need_size), K(buf_size));
  } else {
    task = new (buf) ObCreateHaGtsUtilTask(root_service_);
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
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).timeout(GCONF.rpc_timeout).root_minor_freeze(arg_))) {
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

ObAsyncTask* ObRootService::ObMinorFreezeTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObMinorFreezeTask(arg_);
  }
  return task;
}

ObRootService::ObInnerTableMonitorTask::ObInnerTableMonitorTask(ObRootService& rs)
    : ObAsyncTimerTask(rs.task_queue_), rs_(rs)
{}

int ObRootService::ObInnerTableMonitorTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rs_.inner_table_monitor_.purge_inner_table_history())) {
    LOG_WARN("failed to purge inner table history", K(ret));
  }
  return ret;
}

ObAsyncTask* ObRootService::ObInnerTableMonitorTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObInnerTableMonitorTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObInnerTableMonitorTask(rs_);
  }
  return task;
}

/////////////////////////////////////////////////////////////
int ObRootService::ObLostReplicaCheckerTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rs_.lost_replica_checker_.check_lost_replicas())) {
    LOG_WARN("failed to check_lost_replicas", K(ret));
  }
  return ret;
}

ObAsyncTask* ObRootService::ObLostReplicaCheckerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObLostReplicaCheckerTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObLostReplicaCheckerTask(rs_);
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
  if (status::STARTING == stat || status::IN_SERVICE == stat || status::FULL_SERVICE == stat ||
      status::STARTED == stat) {
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
  if (status::IN_SERVICE == stat || status::FULL_SERVICE == stat || status::STARTED == stat) {
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

// RS need stop after leader revoke
int ObRsStatus::revoke_rs()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (status::IN_SERVICE == rs_status_ || status::FULL_SERVICE == rs_status_) {
    rs_status_ = status::NEED_STOP;
  } else {
    rs_status_ = status::STOPPING;
  }
  return ret;
}

int ObRsStatus::try_set_stopping()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (status::NEED_STOP == rs_status_) {
    rs_status_ = status::STOPPING;
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
    LOG_WARN("fail to get rs status", KR(ret), K(status));
  } else if (OB_FAIL(get_rs_status_str(rs_status_, old_status_str))) {
    LOG_WARN("fail to get rs status", KR(ret), K(rs_status_));
  } else if (OB_ISNULL(new_status_str) || OB_ISNULL(old_status_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpect", KR(ret), K(new_status_str), K(old_status_str));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("start to set rs status", K(new_status_str), K(old_status_str), K(status), K(rs_status_));
    switch (rs_status_) {
      case status::INIT: {
        if (status::STARTING == status || status::STOPPING == status) {
          // rs.stop() will be executed while obs exit
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::STARTING: {
        if (status::IN_SERVICE == status || status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::IN_SERVICE: {
        if (status::FULL_SERVICE == status || status::NEED_STOP == status || status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::FULL_SERVICE: {
        if (status::STARTED == status || status::NEED_STOP == status || status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::STARTED: {
        if (status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::NEED_STOP: {
        if (status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      case status::STOPPING: {
        if (status::INIT == status || status::STOPPING == status) {
          rs_status_ = status;
        } else {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can't set rs status", KR(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid rs status", KR(ret), K(rs_status_));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to set rs status", KR(ret), K(new_status_str), K(old_status_str));
    } else {
      LOG_INFO("finish set rs status", K(ret), K(new_status_str), K(old_status_str), K(status), K(rs_status_));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObRootService::ObRootService()
    : inited_(false),
      server_refreshed_(false),
      debug_(false),
      self_addr_(),
      config_(NULL),
      config_mgr_(NULL),
      rpc_proxy_(),
      common_proxy_(),
      sql_proxy_(),
      remote_sql_proxy_(NULL),
      restore_ctx_(NULL),
      rs_mgr_(NULL),
      schema_service_(NULL),
      status_change_cb_(*this),
      server_change_callback_(*this),
      server_manager_(),
      hb_checker_(),
      server_checker_(),
      rs_list_change_cb_(*this),
      freeze_info_manager_(),
      root_major_freeze_v2_(freeze_info_manager_),
      root_minor_freeze_(),
      pt_operator_(NULL),
      remote_pt_operator_(NULL),
      location_cache_(NULL),
      zone_manager_(),
      ddl_service_(),
      unit_manager_(server_manager_, zone_manager_),
      leader_coordinator_(),
      pt_util_(),
      daily_merge_scheduler_(),
      merge_error_cb_(*this),
      major_freeze_launcher_(),
      freeze_info_updater_(),
      global_trans_version_mgr_(),
      rebalance_task_executor_(),
      rebalance_task_mgr_(),
      root_balancer_(),
      empty_server_checker_(),
      lost_replica_checker_(),
      thread_checker_(),
      vtable_location_getter_(server_manager_, unit_manager_),
      addr_agent_(),
      root_inspection_(),
      schema_split_executor_(),
      upgrade_executor_(),
      upgrade_storage_format_executor_(),
      create_inner_schema_executor_(),
      schema_revise_executor_(),
      bootstrap_lock_(),
      broadcast_rs_list_lock_(ObLatchIds::RS_BROADCAST_LOCK),
      inner_table_monitor_(),
      task_queue_(),
      inspect_task_queue_(),
      restart_task_(*this),
      refresh_server_task_(*this),
      check_server_task_(task_queue_, server_checker_),
      self_check_task_(*this),
      load_building_index_task_(*this, global_index_builder_),
      event_table_clear_task_(ROOTSERVICE_EVENT_INSTANCE, SERVER_EVENT_INSTANCE, task_queue_),
      inner_table_monitor_task_(*this),
      inspector_task_(*this),
      purge_recyclebin_task_(*this),
      force_drop_schema_task_(*this),
      global_index_builder_(),
      partition_spliter_(),
      snapshot_manager_(),
      core_meta_table_version_(0),
      rs_gts_manager_(),
      update_rs_list_timer_task_(*this),
      rs_gts_task_mgr_(unit_manager_, server_manager_, zone_manager_),
      rs_gts_monitor_(unit_manager_, server_manager_, zone_manager_, rs_gts_task_mgr_),
      baseline_schema_version_(0),
      upgrade_cluster_create_ha_gts_lock_(),
      restore_scheduler_(),
      log_archive_scheduler_(),
      root_backup_(),
      root_validate_(),
      start_service_time_(0),
      rs_status_(),
      fail_count_(0),
      schema_history_recycler_(),
      backup_data_clean_(),
      backup_auto_delete_(),
      reload_unit_replica_counter_task_(*this),
      single_part_balance_(),
      restore_point_service_(),
      backup_archive_log_(),
      backup_backupset_(),
      backup_lease_service_(),
      ttl_scheduler_(*this)
{}

ObRootService::~ObRootService()
{
  if (inited_) {
    destroy();
  }
}

int ObRootService::fake_init(ObServerConfig& config, ObConfigManager& config_mgr, ObSrvRpcProxy& srv_rpc_proxy,
    ObCommonRpcProxy& common_proxy, ObAddr& self, ObMySQLProxy& sql_proxy, ObRsMgr& rs_mgr,
    ObMultiVersionSchemaService* schema_service, ObPartitionTableOperator& pt_operator,
    share::ObRemotePartitionTableOperator& remote_pt_operator, ObIPartitionLocationCache& location_cache)
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
    schema_service_ = schema_service;
    pt_operator_ = &pt_operator;
    remote_pt_operator_ = &remote_pt_operator;
    location_cache_ = &location_cache;
  }

  // init server management related
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_manager_.init(status_change_cb_,
            server_change_callback_,
            sql_proxy_,
            unit_manager_,
            zone_manager_,
            leader_coordinator_,
            config,
            self,
            rpc_proxy_))) {
      LOG_WARN("init server manager failed", K(ret));
    } else if (OB_FAIL(hb_checker_.init(server_manager_))) {
      LOG_WARN("init heartbeat checker failed", K(ret));
    } else if (OB_FAIL(server_checker_.init(server_manager_, self))) {
      LOG_WARN("init server checker failed", K(self), K(ret));
    }
  }

  // init ddl service
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_.init(rpc_proxy_,
            common_proxy_,
            sql_proxy_,
            *schema_service,
            pt_operator,
            server_manager_,
            zone_manager_,
            unit_manager_,
            root_balancer_,
            freeze_info_manager_,
            snapshot_manager_,
            rebalance_task_mgr_))) {
      LOG_WARN("ddl_service_ init failed", K(ret));
    }
  }

  // init partition table util
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pt_util_.init(zone_manager_, *schema_service_, *pt_operator_, server_manager_, sql_proxy_))) {
      LOG_WARN("init partition table util failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_backup_.init(*config_,
            *schema_service_,
            sql_proxy_,
            root_balancer_,
            freeze_info_manager_,
            server_manager_,
            rebalance_task_mgr_,
            zone_manager_,
            rpc_proxy_,
            backup_lease_service_,
            restore_point_service_))) {
      LOG_WARN("fail to init root backup", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_validate_.init(*config_,
            sql_proxy_,
            root_balancer_,
            server_manager_,
            rebalance_task_mgr_,
            rpc_proxy_,
            backup_lease_service_))) {
      LOG_WARN("failed to init root validate", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_data_clean_.init(*schema_service_, sql_proxy_, backup_lease_service_))) {
      LOG_WARN("fail to init backup data clean", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_auto_delete_.init(
            *config_, sql_proxy_, *schema_service_, backup_data_clean_, backup_lease_service_))) {
      LOG_WARN("fail to init backup auto delete", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_archive_log_.init(
            server_manager_, rpc_proxy_, sql_proxy_, backup_lease_service_, *schema_service_))) {
      LOG_WARN("failed to init backup archive log scheduler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_backupset_.init(sql_proxy_,
            zone_manager_,
            server_manager_,
            root_balancer_,
            rebalance_task_mgr_,
            rpc_proxy_,
            *schema_service_,
            backup_lease_service_))) {
      LOG_WARN("failed to init backup backupset", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ttl_scheduler_.init())) {
      LOG_WARN("failed to init ttl scheduler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  LOG_INFO("init rootservice", K(ret));
  return ret;
}
int ObRootService::init(ObServerConfig& config, ObConfigManager& config_mgr, ObSrvRpcProxy& srv_rpc_proxy,
    ObCommonRpcProxy& common_proxy, ObAddr& self, ObMySQLProxy& sql_proxy, ObRemoteSqlProxy& remote_sql_proxy,
    observer::ObRestoreCtx& restore_ctx, ObRsMgr& rs_mgr, ObMultiVersionSchemaService* schema_service,
    ObPartitionTableOperator& pt_operator, share::ObRemotePartitionTableOperator& remote_pt_operator,
    ObIPartitionLocationCache& location_cache)
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

    restore_ctx_ = &restore_ctx;

    sql_proxy_.assign(sql_proxy);
    sql_proxy_.set_inactive();

    if (OB_FAIL(oracle_sql_proxy_.init(sql_proxy.get_pool()))) {
      LOG_WARN("init oracle sql proxy failed", K(ret));
    } else {
      oracle_sql_proxy_.set_inactive();
    }

    remote_sql_proxy_ = &remote_sql_proxy;
    remote_sql_proxy_->set_active();

    rs_mgr_ = &rs_mgr;
    schema_service_ = schema_service;
    pt_operator_ = &pt_operator;
    remote_pt_operator_ = &remote_pt_operator;
    location_cache_ = &location_cache;
  }

  // init inner queue
  if (OB_SUCC(ret)) {
    if (OB_FAIL(task_queue_.init(
            config_->rootservice_async_task_thread_count, config_->rootservice_async_task_queue_size, "RSAsyncTask"))) {
      LOG_WARN("init inner queue failed",
          "thread_count",
          static_cast<int64_t>(config_->rootservice_async_task_thread_count),
          "queue_size",
          static_cast<int64_t>(config_->rootservice_async_task_queue_size),
          K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inspect_task_queue_.init(1,  // only for the inspection of RS
            config_->rootservice_async_task_queue_size,
            "RSInspectTask"))) {
      LOG_WARN("init inner queue failed",
          "thread_count",
          1,
          "queue_size",
          static_cast<int64_t>(config_->rootservice_async_task_queue_size),
          K(ret));
    }
  }

  // init zone manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(zone_manager_.init(sql_proxy_, leader_coordinator_))) {
      LOG_WARN("init zone manager failed", K(ret));
    }
  }

  // init server management related
  if (OB_SUCC(ret)) {
    if (OB_FAIL(server_manager_.init(status_change_cb_,
            server_change_callback_,
            sql_proxy_,
            unit_manager_,
            zone_manager_,
            leader_coordinator_,
            config,
            self,
            rpc_proxy_))) {
      LOG_WARN("init server manager failed", K(ret));
    } else if (OB_FAIL(hb_checker_.init(server_manager_))) {
      LOG_WARN("init heartbeat checker failed", K(ret));
    } else if (OB_FAIL(server_checker_.init(server_manager_, self))) {
      LOG_WARN("init server checker failed", K(self), K(ret));
    }
  }
  // init freeze info manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_manager_.init(&sql_proxy_, &zone_manager_, &common_proxy_, *this))) {
      LOG_WARN("freeze_info_manager_ init failed", K(ret));
    }
  }

  // init root major freeze
  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_major_freeze_v2_.init(zone_manager_))) {
      LOG_WARN("fail to init root major freeze", K(ret));
    }
  }

  // init root minor freeze
  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_minor_freeze_.init(rpc_proxy_, server_manager_, unit_manager_, pt_operator))) {
      LOG_WARN("root_minor_freeze_ init failed", K(ret));
    }
  }

  // init ddl service
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ddl_service_.init(rpc_proxy_,
            common_proxy_,
            sql_proxy_,
            *schema_service,
            pt_operator,
            server_manager_,
            zone_manager_,
            unit_manager_,
            root_balancer_,
            freeze_info_manager_,
            snapshot_manager_,
            rebalance_task_mgr_))) {
      LOG_WARN("ddl_service_ init failed", K(ret));
    }
  }

  // init unit manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(unit_manager_.init(sql_proxy_, *config_, leader_coordinator_, *schema_service, root_balancer_))) {
      LOG_WARN("init unit_manager failed", K(ret));
    }
  }
  // init leader coordinator
  if (OB_SUCC(ret)) {
    LOG_INFO("start to init leader coordinator");
    if (OB_FAIL(leader_coordinator_.init(*pt_operator_,
            *schema_service_,
            server_manager_,
            rpc_proxy_,
            zone_manager_,
            rebalance_task_mgr_,
            unit_manager_,
            *config_,
            daily_merge_scheduler_))) {
      LOG_WARN("init leader coordinator failed", K(ret));
    } else {
      LOG_INFO("init leader coordinator succ");
    }
  }

  // init partition table util
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pt_util_.init(zone_manager_, *schema_service_, *pt_operator_, server_manager_, sql_proxy_))) {
      LOG_WARN("init partition table util failed", K(ret));
    }
  }

  // init checksum checker
  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition_checksum_checker_.init(
            &sql_proxy_, remote_sql_proxy_, zone_manager_, freeze_info_manager_, &merge_error_cb_))) {
      LOG_WARN("fail to init partition checksum checker", K(ret));
    }
  }
  // init daily merge scheduler
  if (OB_SUCC(ret)) {
    if (OB_FAIL(daily_merge_scheduler_.init(zone_manager_,
            *config_,
            config_->zone.str(),
            leader_coordinator_,
            pt_util_,
            ddl_service_,
            server_manager_,
            *pt_operator_,
            *remote_pt_operator_,
            *schema_service_,
            sql_proxy_,
            freeze_info_manager_,
            root_balancer_))) {
      LOG_WARN("init daily merge scheduler failed", "zone", config_->zone.str(), K(ret));
    } else {
      daily_merge_scheduler_.set_checksum_checker(partition_checksum_checker_);
    }
  }
  // init restore scheduler
  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_scheduler_.init(*schema_service_,
            sql_proxy_,
            oracle_sql_proxy_,
            common_proxy_,
            rpc_proxy_,
            freeze_info_manager_,
            *pt_operator_,
            rebalance_task_mgr_,
            server_manager_,
            zone_manager_,
            unit_manager_,
            ddl_service_,
            self_addr_))) {
      LOG_WARN("init restore scheduler failed", K(ret));
    }
  }

  // init major freeze launcher
  if (OB_SUCC(ret)) {
    if (OB_FAIL(major_freeze_launcher_.init(*this, common_proxy_, *config_, self_addr_, freeze_info_manager_))) {
      LOG_WARN("init major_freeze_launcher failed", K_(self_addr), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_updater_.init(freeze_info_manager_))) {
      LOG_WARN("init freeze_info_updater failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition_spliter_.init(common_proxy_, self_addr_, schema_service_, this))) {
      LOG_WARN("fail to init partition spliter", K(ret));
    } else if (OB_FAIL(snapshot_manager_.init(self_addr_))) {
      LOG_WARN("fail to init snapshot manager", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(global_trans_version_mgr_.init(&rpc_proxy_, pt_operator_, schema_service_, self_addr_))) {
      LOG_WARN("init global_trans_version_mgr_ failed", K(self_addr_), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(addr_agent_.init(sql_proxy_, *config_))) {
      LOG_WARN("init addr agent failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_inspection_.init(*schema_service_, zone_manager_, sql_proxy_, &common_proxy_))) {
      LOG_WARN("init root inspection failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_split_executor_.init(*schema_service_, &sql_proxy_, rpc_proxy_, server_manager_))) {
      LOG_WARN("init schema_split_executor failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(upgrade_executor_.init(*schema_service_, sql_proxy_, rpc_proxy_))) {
      LOG_WARN("init upgrade_executor failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(upgrade_storage_format_executor_.init(*this, ddl_service_))) {
      LOG_WARN("fail to init upgrade storage format executor", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_inner_schema_executor_.init(*schema_service_, sql_proxy_, common_proxy_))) {
      LOG_WARN("fail to create inner role executor", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            schema_revise_executor_.init(*schema_service_, ddl_service_, sql_proxy_, rpc_proxy_, server_manager_))) {
      LOG_WARN("fail to revise schema executor", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(empty_server_checker_.init(server_manager_, rpc_proxy_, *pt_operator_, *schema_service_))) {
      LOG_WARN("init empty server checker failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(thread_checker_.init())) {
      LOG_WARN("rs_monitor_check : init thread checker failed", KR(ret));
    }
  }

  // init single partition balance
  if (OB_SUCC(ret)) {
    if (OB_FAIL(single_part_balance_.init(unit_manager_, zone_manager_, pt_operator))) {
      LOG_WARN("single_part_balance_ init failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rebalance_task_executor_.init(*pt_operator_,
            rpc_proxy_,
            server_manager_,
            zone_manager_,
            leader_coordinator_,
            *schema_service_,
            unit_manager_))) {
      LOG_WARN("init rebalance task executor failed", K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_.init(*config_,
                   rebalance_task_executor_,
                   &server_manager_,
                   &rpc_proxy_,
                   &root_balancer_,
                   &global_index_builder_))) {
      LOG_WARN("init rebalance task manager failed", K(ret));
    } else if (OB_FAIL(root_balancer_.init(*config_,
                   *schema_service_,
                   ddl_service_,
                   unit_manager_,
                   server_manager_,
                   *pt_operator_,
                   *remote_pt_operator_,
                   leader_coordinator_,
                   zone_manager_,
                   empty_server_checker_,
                   rebalance_task_mgr_,
                   rpc_proxy_,
                   restore_ctx,
                   self_addr_,
                   sql_proxy,
                   single_part_balance_))) {
      LOG_WARN("init root balancer failed", K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_.set_self(self_addr_))) {
      LOG_WARN("set_self failed", K_(self_addr), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(lost_replica_checker_.init(server_manager_, *pt_operator_, *schema_service_))) {
      LOG_WARN("init lost replica checker failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_table_monitor_.init(sql_proxy, common_proxy_, *this))) {
      LOG_WARN("init inner table monitor failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ROOTSERVICE_EVENT_INSTANCE.init(sql_proxy, self_addr_))) {
      LOG_WARN("init rootservice event history failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(THE_RS_JOB_TABLE.init(&sql_proxy, self_addr_))) {
      LOG_WARN("failed to init THE_RS_JOB_TABLE", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRSBuildIndexScheduler::get_instance().init(&ddl_service_))) {
      LOG_WARN("fail to init ObRSBuildIndexScheduler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(global_index_builder_.init(&srv_rpc_proxy,
            &sql_proxy,
            &server_manager_,
            pt_operator_,
            &rebalance_task_mgr_,
            &ddl_service_,
            schema_service_,
            &zone_manager_,
            &freeze_info_manager_))) {
      LOG_WARN("fail to init ObGlobalIndexBuilder", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_manager_.init(&sql_proxy, &zone_manager_, &unit_manager_, &server_manager_))) {
      LOG_WARN("fail to init rs gts manager", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_task_mgr_.init(&rpc_proxy_, &sql_proxy))) {
      LOG_WARN("fail to init rs gts task mgr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_monitor_.init(&sql_proxy))) {
      LOG_WARN("fail to init rs gts monitor", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_lease_service_.init(self_addr_, sql_proxy))) {
      LOG_WARN("failed to init backup lease service", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_archive_scheduler_.init(
            server_manager_, zone_manager_, schema_service_, rpc_proxy_, sql_proxy_, backup_lease_service_))) {
      LOG_WARN("failed to init log_archive_scheduler_", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(log_archive_scheduler_))) {
      LOG_WARN("failed to register log archive scheduler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_backup_.init(*config_,
            *schema_service_,
            sql_proxy_,
            root_balancer_,
            freeze_info_manager_,
            server_manager_,
            rebalance_task_mgr_,
            zone_manager_,
            rpc_proxy_,
            backup_lease_service_,
            restore_point_service_))) {
      LOG_WARN("fail to init root backup", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(root_backup_))) {
      LOG_WARN("failed to register root_backup_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_validate_.init(*config_,
            sql_proxy_,
            root_balancer_,
            server_manager_,
            rebalance_task_mgr_,
            rpc_proxy_,
            backup_lease_service_))) {
      LOG_WARN("failed to init root validate", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(root_validate_))) {
      LOG_WARN("failed to register root_validate_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_data_clean_.init(*schema_service_, sql_proxy_, backup_lease_service_))) {
      LOG_WARN("fail to init backup data clean", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(backup_data_clean_))) {
      LOG_WARN("failed to register root_validate_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_archive_log_.init(
            server_manager_, rpc_proxy_, sql_proxy_, backup_lease_service_, *schema_service_))) {
      LOG_WARN("failed to init backup archive log scheduler", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(backup_archive_log_))) {
      LOG_WARN("failed to register backup_archive_log_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_backupset_.init(sql_proxy,
            zone_manager_,
            server_manager_,
            root_balancer_,
            rebalance_task_mgr_,
            rpc_proxy_,
            *schema_service_,
            backup_lease_service_))) {
      LOG_WARN("failed to init backup backupset", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(backup_backupset_))) {
      LOG_WARN("failed to register backup_backupset_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_history_recycler_.init(
            *schema_service_, freeze_info_manager_, zone_manager_, sql_proxy_, server_manager_))) {
      LOG_WARN("fail to init schema history recycler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_auto_delete_.init(
            *config_, sql_proxy_, *schema_service_, backup_data_clean_, backup_lease_service_))) {
      LOG_WARN("fail to init backup auto delete", K(ret));
    } else if (OB_FAIL(backup_lease_service_.register_scheduler(backup_auto_delete_))) {
      LOG_WARN("failed to register backup_auto_delete_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_lease_service_.start())) {
      LOG_WARN("failed to start backup lease task", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_point_service_.init(ddl_service_, freeze_info_manager_))) {
      LOG_WARN("failed to init restore point service", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ttl_scheduler_.init())) {
      LOG_WARN("failed to init ttl task scheduler", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  LOG_INFO("init rootservice", K(ret));
  return ret;
}

void ObRootService::destroy()
{
  int ret = OB_SUCCESS;
  if (in_service()) {
    if (OB_FAIL(stop_service())) {
      LOG_WARN("stop service failed", K(ret));
    }
  }

  FLOG_INFO("start destroy log_archive_scheduler_");
  log_archive_scheduler_.destroy();
  FLOG_INFO("finish destroy log_archive_scheduler_");

  // continue executing while error happen
  if (OB_FAIL(root_balancer_.destroy())) {
    LOG_WARN("root balance destroy failed", K(ret));
  } else {
    LOG_INFO("root balance destroy");
  }
  if (OB_FAIL(rebalance_task_mgr_.destroy())) {
    LOG_WARN("rebalance task mgr destroy failed", K(ret));
  } else {
    LOG_INFO("rebalance task mgr destroy");
  }
  if (OB_FAIL(major_freeze_launcher_.destroy())) {
    LOG_WARN("major freeze launcher destroy failed", K(ret));
  } else {
    LOG_INFO("major freeze launcher destroy");
  }
  if (OB_FAIL(freeze_info_updater_.destroy())) {
    LOG_WARN("freeze info updater destroy failed", K(ret));
  } else {
    LOG_INFO("freeze info updater destroy");
  }
  if (OB_FAIL(partition_spliter_.destroy())) {
    LOG_WARN("partition spliter destory failed", K(ret));
  } else {
    LOG_INFO("partition spliter destory success");
  }
  if (OB_FAIL(global_trans_version_mgr_.destroy())) {
    LOG_WARN("global_trans_version_mgr_ destroy failed", K(ret));
  } else {
    LOG_INFO("global_trans_version_mgr_ destroy");
  }
  if (OB_FAIL(empty_server_checker_.destroy())) {
    LOG_WARN("empty server checker destroy failed", K(ret));
  } else {
    LOG_INFO("empty server checker destroy");
  }
  if (OB_FAIL(thread_checker_.destroy())) {
    LOG_WARN("rs_monitor_check : thread checker destroy failed", KR(ret));
  } else {
    LOG_INFO("rs_monitor_check : thread checker destroy");
  }
  if (OB_FAIL(daily_merge_scheduler_.destroy())) {
    LOG_WARN("daily merge scheduler destroy failed", K(ret));
  } else {
    LOG_INFO("daily merge scheduler destroy");
  }
  if (OB_FAIL(restore_scheduler_.destroy())) {
    LOG_WARN("restore scheduler scheduler destroy failed", K(ret));
  } else {
    LOG_INFO("restore scheduler destroy");
  }
  if (OB_FAIL(leader_coordinator_.destroy())) {
    LOG_WARN("leader coordinator destroy failed", K(ret));
  } else {
    LOG_INFO("leader coordinator destroy");
  }

  if (OB_FAIL(schema_history_recycler_.destroy())) {
    LOG_WARN("schema history recycler destroy failed", K(ret));
  } else {
    LOG_INFO("schema history recycler destroy");
  }

  ttl_scheduler_.destroy();
  task_queue_.destroy();
  LOG_INFO("inner queue destroy");
  inspect_task_queue_.destroy();
  LOG_INFO("inspect queue destroy");
  TG_DESTROY(lib::TGDefIDs::IdxBuild);
  LOG_INFO("index build thread destroy");
  if (OB_FAIL(hb_checker_.destroy())) {
    LOG_INFO("heartbeat checker destroy failed", K(ret));
  } else {
    LOG_INFO("heartbeat checker destroy");
  }

  ROOTSERVICE_EVENT_INSTANCE.destroy();
  LOG_INFO("event table operator destroy");

  ObRSBuildIndexScheduler::get_instance().destroy();
  LOG_INFO("ObRSBuildIndexScheduler destory");

  // ignore ret code here
  if (OB_FAIL(rs_gts_task_mgr_.destroy())) {
    LOG_INFO("rs gts task mgr destroy failed", K(ret));
  } else {
    LOG_INFO("rs gts task mgr destroy");
  }

  if (OB_FAIL(rs_gts_monitor_.destroy())) {
    LOG_INFO("rs gts monitor destroy failed", K(ret));
  } else {
    LOG_INFO("rs gts monitor destroy");
  }

  if (OB_FAIL(root_backup_.destroy())) {
    LOG_INFO("root backup destroy failed", K(ret));
  } else {
    LOG_INFO("root backup destroy");
  }

  if (OB_FAIL(root_validate_.destroy())) {
    LOG_INFO("root validate destroy failed", K(ret));
  } else {
    LOG_INFO("root validate destroy");
  }

  if (OB_FAIL(backup_data_clean_.destroy())) {
    LOG_INFO("backup data clean destroy failed", K(ret));
  } else {
    LOG_INFO("backup data clean destroy");
  }

  if (OB_FAIL(backup_auto_delete_.destroy())) {
    LOG_INFO("backup auto delete destroy failed", K(ret));
  } else {
    LOG_INFO("backup auoto delete destroy");
  }

  if (OB_FAIL(backup_archive_log_.destroy())) {
    LOG_WARN("backup archive log scheduler destroy failed", K(ret));
  } else {
    LOG_INFO("backup archive log scheduler destory");
  }

  if (OB_FAIL(backup_backupset_.destroy())) {
    LOG_INFO("backup backupset destroy failed", K(ret));
  } else {
    LOG_INFO("backup backupset destroy");
  }

  FLOG_INFO("start destroy backup_lease_service_");
  backup_lease_service_.destroy();
  FLOG_INFO("finish destroy backup_lease_service_");

  if (OB_SUCC(ret)) {
    if (inited_) {
      inited_ = false;
    }
  }

  LOG_INFO("destroy rootservice", K(ret));
}

int ObRootService::start_service()
{
  int ret = OB_SUCCESS;
  start_service_time_ = ObTimeUtility::current_time();
  ROOTSERVICE_EVENT_ADD("root_service", "start_rootservice", K_(self_addr));
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("[NOTICE] START_SERVICE: start to start rootservice", K_(start_service_time));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("START_SERVICE: rootservice not inited", K(ret));
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STARTING))) {
    LOG_WARN("fail to set rs status", K(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver(static_cast<ObIPartPropertyGetter*>(GCTX.ob_service_))) {
    ret = OB_NOT_MASTER;
    LOG_WARN("START_SERVICE: not master", K(ret));
  } else {
    sql_proxy_.set_active();
    oracle_sql_proxy_.set_active();
    const bool rpc_active = true;
    common_proxy_.active(rpc_active);
    rpc_proxy_.active(rpc_active);

    bool is_exist = false;
    if (OB_FAIL(check_other_rs_exist(is_exist))) {
      LOG_WARN("fail to check other rs exist, may no other rs exist", KR(ret));
      ret = OB_SUCCESS;
    } else if (!is_exist) {
      // nothing todo
    } else {
      ret = OB_ROOTSERVICE_EXIST;
      LOG_WARN("START_SERVICE: another root service in service", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ddl_service_.restart();
    server_manager_.reset();

    if (OB_FAIL(hb_checker_.start())) {
      LOG_WARN("START_SERVICE: hb checker start failed", K(ret));
    } else if (OB_FAIL(task_queue_.start())) {
      LOG_WARN("START_SERVICE: inner queue start failed", K(ret));
    } else if (OB_FAIL(inspect_task_queue_.start())) {
      LOG_WARN("START_SERVICE: inspect queue start failed", K(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::IdxBuild))) {
      LOG_WARN("START_SERVICE: index build thread start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERSVICE: start some queue success");
    }
  }

  if (OB_SUCC(ret)) {
    // partition table related
    if (OB_FAIL(pt_operator_->set_callback_for_rs(rs_list_change_cb_, merge_error_cb_))) {
      LOG_WARN("START_SERVICE: pt_operator set as rs leader failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_status_.set_rs_status(status::IN_SERVICE))) {
      LOG_WARN("fail to set rs status", KR(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: rootservice IN SERVICE");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_refresh_server_timer_task(0))) {
      LOG_WARN("failed to schedule refresh_server task", K(ret));
    } else if (OB_FAIL(schedule_restart_timer_task(0))) {
      LOG_WARN("failed to schedule restart task", K(ret));
    } else if (debug_) {
      if (OB_FAIL(init_debug_database())) {
        LOG_WARN("START_SERVICE: init_debug_database failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "finish_start_rootservice", "result", ret, K_(self_addr));

  if (OB_FAIL(ret)) {
    // increase fail count for self checker and print log.
    update_fail_count(ret);
    ObTaskController::get().allow_next_syslog();
    LOG_WARN("START_SERVICE: start service failed, do stop service", K(ret));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rs_status_.set_rs_status(status::STOPPING))) {
      LOG_WARN("fail to set status", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = stop_service())) {
      ObTaskController::get().allow_next_syslog();
      LOG_WARN("START_SERVICE: stop service failed", K(tmp_ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: stop service finished", K(tmp_ret));
    }
  }

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("[NOTICE] START_SERVICE: rootservice start_service finished", K(ret));
  return ret;
}

int ObRootService::check_other_rs_exist(bool& is_other_rs_exist)
{
  int ret = OB_SUCCESS;
  is_other_rs_exist = false;
  ObAddr master_rs;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
    if (OB_RS_NOT_MASTER == ret) {
      LOG_INFO("no other rs exist", KR(ret));
    } else {
      LOG_WARN("do detect  master rs failed", K(ret));
    }
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(master_rs))) {
    LOG_WARN("START_SERVICE: get master root service failed", KR(ret));
  } else if (master_rs == GCTX.self_addr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid rs", KR(ret), K(master_rs));
  } else {
    is_other_rs_exist = true;
    LOG_WARN("START_SERVICE: another root service in service", K(master_rs));
  }
  return ret;
}

int ObRootService::stop_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stop())) {
    LOG_WARN("fail to stop thread", KR(ret));
  } else {
    wait();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rs_status_.set_rs_status(status::INIT))) {
    LOG_WARN("fail to set rs status", KR(ret));
  }
  return ret;
}

int ObRootService::stop()
{
  int ret = OB_SUCCESS;
  start_service_time_ = 0;
  int64_t start_time = ObTimeUtility::current_time();
  ROOTSERVICE_EVENT_ADD("root_service", "stop_rootservice", K_(self_addr));
  LOG_INFO("[NOTICE] start to stop rootservice", K(start_time));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STOPPING))) {
    LOG_WARN("fail to set rs status", KR(ret));
  } else {
    // full_service_ = false;
    server_refreshed_ = false;
    // in_service_ = false;
    sql_proxy_.set_inactive();
    oracle_sql_proxy_.set_inactive();
    const bool rpc_active = false;
    common_proxy_.active(rpc_active);
    rpc_proxy_.active(rpc_active);

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = schema_split_executor_.stop())) {
      LOG_WARN("schema_split_executor stop failed", KR(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = upgrade_executor_.stop())) {
      LOG_WARN("upgrade_executor stop failed", KR(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = upgrade_storage_format_executor_.stop())) {
      LOG_WARN("fail to stop upgrade storage format executor", KR(tmp_ret));
    }

    if (OB_SUCCESS != (tmp_ret = create_inner_schema_executor_.stop())) {
      LOG_WARN("fail to stop create inner schema executor", KR(tmp_ret));
    }

    if (OB_SUCCESS != (tmp_ret = schema_revise_executor_.stop())) {
      LOG_WARN("fail to stop schema revise executor", KR(tmp_ret));
    }

    // set to rpc partition table as soon as possible
    if (OB_FAIL(pt_operator_->set_callback_for_obs(common_proxy_, *rs_mgr_, *config_))) {
      LOG_WARN("set as rs follower failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(stop_timer_tasks())) {
        LOG_WARN("stop timer tasks failed", K(ret));
      } else {
        LOG_INFO("[NOTICE] stop timer tasks success");
      }
    }
    if (OB_SUCC(ret)) {
      // ddl_service may be trying refresh schema, stop it
      ddl_service_.stop();
      LOG_INFO("ddl service stop");
      root_minor_freeze_.stop();
      LOG_INFO("minor freeze stop");
      freeze_info_manager_.unload();
      LOG_INFO("freeze info manater stop");
      root_inspection_.stop();
      LOG_INFO("root inspection stop");
    }
    if (OB_SUCC(ret)) {
      TG_STOP(lib::TGDefIDs::IdxBuild);
      LOG_INFO("index build queue stop");
      task_queue_.stop();
      LOG_INFO("task_queue stop");
      inspect_task_queue_.stop();
      LOG_INFO("inspect queue stop");
      rebalance_task_mgr_.stop();
      LOG_INFO("rebalance_task_mgr stop");
      root_balancer_.stop();
      LOG_INFO("root_balancer stop");
      backup_lease_service_.stop_lease();
      LOG_INFO("backup lease service stop");
      empty_server_checker_.stop();
      LOG_INFO("empty_server_checker stop");
      thread_checker_.stop();
      LOG_INFO("rs_monitor_check : thread_checker stop");
      daily_merge_scheduler_.stop();
      LOG_INFO("daily_merge_scheduler stop");
      restore_scheduler_.stop();
      LOG_INFO("restore_scheduler stop");
      schema_history_recycler_.stop();
      LOG_INFO("schema_history_recycler stop");
      leader_coordinator_.stop();
      LOG_INFO("leader_coordinator stop");
      major_freeze_launcher_.stop();
      LOG_INFO("major_freeze_launcher stop");
      freeze_info_updater_.stop();
      LOG_INFO("freeze_info_updater stop");
      global_trans_version_mgr_.stop();
      LOG_INFO("global_trans_version_mgr stop");
      hb_checker_.stop();
      LOG_INFO("hb_checker stop");
      global_index_builder_.stop();
      LOG_INFO("global_index_builder stop");
      partition_spliter_.stop();
      LOG_INFO("partition_spliter stop");
      rs_gts_task_mgr_.stop();
      LOG_INFO("rs_gts_task_mgr stop");
      rs_gts_monitor_.stop();
      LOG_INFO("rs_gts_monitor stop");
      root_backup_.stop();
      LOG_INFO("root_backup stop");
      root_validate_.stop();
      LOG_INFO("root_validate stop");
      backup_data_clean_.stop();
      LOG_INFO("backup_data_clean stop");
      backup_auto_delete_.stop();
      LOG_INFO("backup auto delete stop");
      backup_archive_log_.stop();
      LOG_INFO("backup archive log stop");
      backup_backupset_.stop();
      LOG_INFO("backup backupset stop");
      ttl_scheduler_.stop();
      LOG_INFO("ttl task scheduler stop");
    }
  }

  ROOTSERVICE_EVENT_ADD("root_service", "finish_stop_thread", K(ret), K_(self_addr));
  LOG_INFO("[NOTICE] finish stop rootserver", K(ret));
  return ret;
}

void ObRootService::wait()
{
  int64_t start_time = ObTimeUtility::current_time();
  LOG_INFO("start to wait all thread exit");
  rebalance_task_mgr_.wait();
  LOG_INFO("rebalance task mgr exit success");
  root_balancer_.wait();
  LOG_INFO("root balancer exit success");
  backup_lease_service_.wait_lease();
  LOG_INFO("log archive exit success");
  empty_server_checker_.wait();
  LOG_INFO("empty server checker exit success");
  thread_checker_.wait();
  LOG_INFO("rs_monitor_check : thread checker exit success");
  daily_merge_scheduler_.wait();
  LOG_INFO("daily merge scheduler exit success");
  restore_scheduler_.wait();
  LOG_INFO("restore scheduler exit success");
  schema_history_recycler_.wait();
  LOG_INFO("schema_history_recycler exit success");
  leader_coordinator_.wait();
  LOG_INFO("leader coordinate exit success");
  major_freeze_launcher_.wait();
  LOG_INFO("major freeze launcher exit success");
  freeze_info_updater_.wait();
  LOG_INFO("freeze info update exit success");
  global_trans_version_mgr_.wait();
  LOG_INFO("global trans version exit success");
  hb_checker_.wait();
  LOG_INFO("hb checker exit success");
  task_queue_.wait();
  LOG_INFO("task queue exit success");
  inspect_task_queue_.wait();
  LOG_INFO("inspect queue exit success");
  TG_WAIT(lib::TGDefIDs::IdxBuild);
  LOG_INFO("index build queue exit success");
  global_index_builder_.wait();
  LOG_INFO("global index build exit success");
  partition_spliter_.wait();
  LOG_INFO("parttion spliter exit success");
  rs_gts_task_mgr_.wait();
  LOG_INFO("rs gts task mgr exit success");
  rs_gts_monitor_.wait();
  LOG_INFO("rs gts monitor exit success");
  root_backup_.wait();
  LOG_INFO("root backup exit success");
  root_validate_.wait();
  LOG_INFO("root validate exit success");
  backup_data_clean_.wait();
  LOG_INFO("backup data clean exit success");
  backup_auto_delete_.wait();
  LOG_INFO("backup auto delete exit success");
  backup_archive_log_.wait();
  LOG_INFO("backup archive log exit success");
  backup_backupset_.wait();
  LOG_INFO("backup backupset exit success");
  ttl_scheduler_.wait();
  LOG_INFO("ttl task scheduler exit success");

  rebalance_task_mgr_.reuse();
  ObUpdateRsListTask::clear_lock();
  THE_RS_JOB_TABLE.reset_max_job_id();
  int64_t cost = ObTimeUtility::current_time() - start_time;
  ROOTSERVICE_EVENT_ADD("root_service", "finish_wait_stop", K(cost));
  LOG_INFO("[NOTICE] rootservice stop", K(start_time), K(cost));
}

int ObRootService::reload_config()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {

    rebalance_task_mgr_.clear_reach_concurrency_limit();

    if (OB_FAIL(addr_agent_.reload())) {
      LOG_WARN("root address agent reload failed", K(ret));
    } else if (OB_FAIL(get_daily_merge_scheduler().reset_merger_warm_up_duration_time(
                   GCONF.merger_warm_up_duration_time))) {
      LOG_ERROR("failed to reset merge warm up duration time", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_statistic_primary_zone_count()
{
  int ret = OB_SUCCESS;
  ObStatisticPrimaryZoneCountTask task(*this);
  task.set_retry_times(0);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("fail to submit statistic primary zone count", K(ret));
  } else {
    LOG_INFO("submit statistic primary zone count finish", K(ret));
  }
  return ret;
}

int ObRootService::submit_create_ha_gts_util()
{
  int ret = OB_SUCCESS;
  ObCreateHaGtsUtilTask task(*this);
  task.set_retry_times(0);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("fail to submit create ha gts util", K(ret));
  } else {
    LOG_INFO("submit create ha gts util succeed", K(ret));
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

int ObRootService::submit_index_sstable_build_task(const sql::ObIndexSSTableBuilder::BuildIndexJob& job,
    sql::ObIndexSSTableBuilder::ReplicaPicker& replica_picker, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job));
  } else {
    ObIndexSSTableBuildTask task(
        job, replica_picker, global_index_builder_, sql_proxy_, oracle_sql_proxy_, abs_timeout_us);
    if (OB_FAIL(TG_PUSH_TASK(lib::TGDefIDs::IdxBuild, task))) {
      LOG_WARN("add task to queue failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_update_all_server_task(const ObAddr& server)
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
    ObAllServerTask task(server_manager_, rebalance_task_mgr_, server, with_rootserver);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // update rs_list if server is rs
    ObArray<ObAddr> rs_list;
    if (OB_FAIL(get_root_partition_table(rs_list))) {
      LOG_WARN("get rs_list failed", K(ret));
    } else {
      bool found = false;
      FOREACH_CNT_X(rs, rs_list, !found)
      {
        if (*rs == server) {
          found = true;
        }
      }
      if (found) {
        if (OB_FAIL(submit_update_rslist_task())) {
          LOG_WARN("submit update rslist task failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::submit_start_server_task(const common::ObAddr& server)
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

int ObRootService::submit_stop_server_task(const common::ObAddr& server)
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

int ObRootService::submit_offline_server_task(const common::ObAddr& server)
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

int ObRootService::submit_merge_error_task()
{
  int ret = OB_SUCCESS;
  ObMergeErrorTask task(*this);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit merge error task fail", K(ret));
  }
  return ret;
}

int ObRootService::submit_black_list_task(const int64_t count, const common::ObIArray<uint64_t>& table_ids,
    const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<common::ObAddr>& servers,
    const share::ObPartitionReplica::FailList& fail_msgs)
{
  int ret = OB_SUCCESS;
  ObBlacklistProcess task;
  task.set_retry_times(0);  // not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task.init(count, *pt_operator_, table_ids, partition_ids, servers, fail_msgs))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit black list task fail", K(ret));
  } else {
    LOG_INFO(
        "submit blacklist task success", K(ret), K(count), K(table_ids), K(partition_ids), K(servers), K(fail_msgs));
  }
  return ret;
}

int ObRootService::submit_schema_split_task()
{
  int ret = OB_SUCCESS;
  ObSchemaSplitTask task(schema_split_executor_, ObRsJobType::JOB_TYPE_SCHEMA_SPLIT);
  task.set_retry_times(0);  // not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_split_executor_.can_execute())) {
    LOG_WARN("can't run task now", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit schema split task fail", K(ret));
  } else {
    LOG_INFO("submit schema split task success", K(ret));
  }
  return ret;
}

int ObRootService::submit_schema_split_task_v2()
{
  int ret = OB_SUCCESS;
  ObSchemaSplitTask task(schema_split_executor_, ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2);
  task.set_retry_times(0);  // not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_split_executor_.can_execute())) {
    LOG_WARN("can't run task now", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit schema split task v2 fail", K(ret));
  } else {
    LOG_INFO("submit schema split task v2 success", K(ret));
  }
  return ret;
}

int ObRootService::submit_upgrade_task(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObUpgradeTask task(upgrade_executor_, version);
  task.set_retry_times(0);  // not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(upgrade_executor_.can_execute())) {
    LOG_WARN("can't run task now", KR(ret), K(version));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit upgrade task fail", KR(ret), K(version));
  } else {
    LOG_INFO("submit upgrade task success", KR(ret), K(version));
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

int ObRootService::submit_schema_revise_task()
{
  int ret = OB_SUCCESS;
  ObSchemaReviseTask task(schema_revise_executor_);
  task.set_retry_times(0);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService has not been inited", K(ret));
  } else if (OB_FAIL(schema_revise_executor_.can_execute())) {
    LOG_WARN("cannot run task now", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit revise schema task", K(ret));
  } else {
    LOG_INFO("submit revise schema task success", K(ret), K(common::lbt()));
  }
  return ret;
}

int ObRootService::submit_async_minor_freeze_task(const ObRootMinorFreezeArg& arg)
{
  int ret = OB_SUCCESS;
  ObMinorFreezeTask task(arg);
  task.set_retry_times(0);  // not repeat
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_async_task(task))) {
    LOG_WARN("submit async minor freeze task fail", K(ret));
  } else {
    LOG_INFO("submit async minor freeze task success", K(ret));
  }
  return ret;
}

int ObRootService::schedule_check_server_timer_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(check_server_task_, config_->server_check_interval, true))) {
    LOG_WARN("failed to add check_server task", K(ret));
  } else {
  }
  return ret;
}

int ObRootService::schedule_inner_table_monitor_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(
                 inner_table_monitor_task_, ObInnerTableMonitorTask::PURGE_INTERVAL, true))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("schedule inner_table_monitor task");
  }
  return ret;
}

int ObRootService::schedule_recyclebin_task(int64_t delay)
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;

  if (OB_FAIL(get_inspect_task_queue().add_timer_task(purge_recyclebin_task_, delay, did_repeat))) {
    LOG_ERROR("schedule purge recyclebin task failed", KR(ret), K(delay), K(did_repeat));
  }

  return ret;
}

int ObRootService::schedule_force_drop_schema_task(int64_t delay)
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;

  if (OB_FAIL(get_inspect_task_queue().add_timer_task(force_drop_schema_task_, delay, did_repeat))) {
    LOG_ERROR("schedule force drop schema task failed", KR(ret), K(delay), K(did_repeat));
  }

  return ret;
}

int ObRootService::schedule_inspector_task()
{
  int ret = OB_SUCCESS;
  int64_t inspect_interval = ObInspector::INSPECT_INTERVAL;
#ifdef DEBUG
  inspect_interval = ObServerConfig::get_instance().schema_drop_gc_delay_time;
#endif

#ifdef ERRSIM
  inspect_interval = ObServerConfig::get_instance().schema_drop_gc_delay_time;
#endif

  int64_t delay = 1 * 60 * 1000 * 1000;
  int64_t purge_interval = GCONF._recyclebin_object_purge_frequency;
  int64_t expire_time = GCONF.recyclebin_object_expire_time;
  if (purge_interval > 0 && expire_time > 0) {
    delay = purge_interval;
  }
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!inspect_task_queue_.exist_timer_task(inspector_task_) &&
             OB_FAIL(inspect_task_queue_.add_timer_task(inspector_task_, inspect_interval, true))) {
    LOG_WARN("failed to add inspect task", KR(ret));
  } else if (!inspect_task_queue_.exist_timer_task(force_drop_schema_task_) &&
             OB_FAIL(inspect_task_queue_.add_timer_task(force_drop_schema_task_, inspect_interval, false))) {
    LOG_WARN("failed to add force drop schema task", KR(ret));
  } else if (!inspect_task_queue_.exist_timer_task(purge_recyclebin_task_) &&
             OB_FAIL(inspect_task_queue_.add_timer_task(purge_recyclebin_task_, delay, false))) {
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
  const int64_t delay = 5L * 1000L * 1000L;  // 5s
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(self_check_task_)) {
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
    LOG_WARN("already have one update rs list timer task , ignore this");
  } else if (OB_FAIL(task_queue_.add_timer_task(
                 update_rs_list_timer_task_, ObUpdateRsListTimerTask::RETRY_INTERVAL, did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("add update rs list task success");
  }
  return ret;
}

int ObRootService::schedule_load_building_index_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = false;
  const int64_t delay = 5L * 1000L * 1000L;  // 5s
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_queue_.exist_timer_task(load_building_index_task_)) {
    LOG_WARN("load local index task already exist", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(load_building_index_task_, delay, did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("succeed to add load building index task");
  }
  return ret;
}

int ObRootService::submit_lost_replica_checker_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObLostReplicaCheckerTask task(*this);
    if (OB_FAIL(task_queue_.add_async_task(task))) {
      LOG_WARN("inner queue push task failed", K(ret));
    } else {
      LOG_INFO("submit lost_replica_checker task");
    }
  }
  return ret;
}

int ObRootService::submit_update_rslist_task(const bool force_update)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObUpdateRsListTask::try_lock()) {
      bool task_added = false;
      ObUpdateRsListTask task;
      if (OB_FAIL(task.init(*pt_operator_,
              &addr_agent_,
              server_manager_,
              zone_manager_,
              broadcast_rs_list_lock_,
              force_update,
              self_addr_))) {
        LOG_WARN("task init failed", K(ret));
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
    } else {
    }  // no more to do
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
    if (OB_FAIL(task_queue_.add_timer_task(restart_task_, delay, did_repeat))) {
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
    if (OB_FAIL(task_queue_.add_timer_task(refresh_server_task_, delay, did_repeat))) {
      LOG_WARN("schedule restart task failed", K(ret), K(delay), K(did_repeat));
    } else {
      LOG_INFO("schedule restart task", K(delay));
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
  if (OB_FAIL(task.init(*pt_operator_,
          &addr_agent_,
          server_manager_,
          zone_manager_,
          broadcast_rs_list_lock_,
          force_update,
          self_addr_))) {
    LOG_WARN("task init failed", K(ret), K(force_update));
  } else if (OB_FAIL(task.process_without_lock())) {
    LOG_WARN("failed to update rslist", K(ret));
  } else {
    LOG_INFO("broadcast root address succeed");
  }
  return ret;
}

// only used in bootstrap
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
    ObZone empty_zone;  // empty zone for all servers
    if (OB_FAIL(server_manager_.get_servers_of_zone(empty_zone, servers))) {
      LOG_WARN("get server list failed", K(ret));
    } else {
      FOREACH_X(s, servers, OB_SUCC(ret))
      {
        const bool with_rootserver = (*s == self_addr_);
        ObAllServerTask task(server_manager_, rebalance_task_mgr_, *s, with_rootserver);
        if (OB_FAIL(task.process())) {
          LOG_WARN("sync server status to __all_server table failed", K(ret), "server", *s);
        }
      }
    }
  }
  return ret;
}

int ObRootService::get_root_partition_table(ObArray<ObAddr>& rs_list)
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  const int64_t partition_id = ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_operator_->get(table_id, partition_id, partition_info))) {
    LOG_WARN("pt_operator get failed", K(table_id), K(partition_id), K(ret));
  } else {
    FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), OB_SUCCESS == ret)
    {
      if (replica->is_in_service()) {
        if (OB_FAIL(rs_list.push_back(replica->server_))) {
          LOG_WARN("add rs failed", K(ret));
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
  ObZone zone;  // empty zone means all zone
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_server_statuses(zone, statuses))) {
    LOG_WARN("get_server_statuses failed", K(zone), K(ret));
  } else {
    const int64_t rpc_timeout = 250 * 1000;  // 250ms
    ObLeaseRequest lease_request;
    // should continue even some failed, so don't look at condition OB_SUCCESS == ret
    FOREACH_CNT(status, statuses)
    {
      if (ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED == status->hb_status_ ||
          (ObServerStatus::OB_SERVER_ADMIN_DELETING == status->admin_status_ && !status->is_alive())) {
        uint64_t server_id = OB_INVALID_ID;
        lease_request.reset();
        int temp_ret = OB_SUCCESS;
        bool to_alive = false;
        bool update_delay_time_flag = false;
        if (OB_SUCCESS !=
            (temp_ret = rpc_proxy_.to(status->server_).timeout(rpc_timeout).request_heartbeat(lease_request))) {
          LOG_WARN("request_heartbeat failed", "server", status->server_, K(rpc_timeout), K(temp_ret));
        } else if (OB_SUCCESS != (temp_ret = server_manager_.receive_hb(
                                      lease_request, server_id, to_alive, update_delay_time_flag))) {
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
  } else if (OB_FAIL(root_inspection_.check_all())) {  // ignore failed
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
  // avoid concurrent with bootstrap
  ObCurTraceId::init(GCONF.self_addr_);
  LOG_INFO("START_SERVICE: start to do restart task");
  ObLatchRGuard guard(bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("START_SERVICE: not init", K(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver(static_cast<ObIPartPropertyGetter*>(GCTX.ob_service_))) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not master", K(ret));
  } else if (need_do_restart() && OB_FAIL(do_restart())) {
    LOG_WARN("START_SERVICE: do restart failed, retry again", K(ret));
  } else if (OB_FAIL(do_after_full_service())) {
    LOG_WARN("fail to do after full service", KR(ret));
  }

  int64_t cost = ObTimeUtility::current_time() - start_service_time_;
  if (OB_FAIL(ret)) {
    LOG_WARN("START_SERVICE: do restart task failed, retry again", K(ret), K(cost));
  } else if (OB_FAIL(rs_status_.set_rs_status(status::STARTED))) {
    LOG_WARN("fail to set rs status", KR(ret));
  } else {
    LOG_WARN("START_SERVICE: do restart task success, finish restart", K(ret), K(cost), K_(start_service_time));
  }

  if (OB_FAIL(ret)) {
    rs_status_.try_set_stopping();
    if (rs_status_.is_stopping()) {
      // need stop
    } else {
      const int64_t RETRY_TIMES = 3;
      int64_t tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < RETRY_TIMES; ++i) {
        if (OB_SUCCESS != (tmp_ret = schedule_restart_timer_task(config_->rootservice_ready_check_interval))) {
        } else {
          break;
        }
      }
      if (OB_SUCCESS != tmp_ret) {
        LOG_ERROR("fatal error, fail to add restart task", K(tmp_ret));
        if (OB_FAIL(rs_status_.set_rs_status(status::STOPPING))) {
          LOG_ERROR("fail to set rs status", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootService::do_after_full_service()
{
  int ret = OB_SUCCESS;
  ObGlobalStatProxy global_proxy(sql_proxy_);
  int64_t frozen_version = 0;
  if (OB_FAIL(global_proxy.get_baseline_schema_version(frozen_version, baseline_schema_version_))) {
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
int ObRootService::execute_bootstrap(const obrpc::ObBootstrapArg& arg)
{
  int ret = OB_SUCCESS;
  const obrpc::ObServerInfoList& server_list = arg.server_list_;
  BOOTSTRAP_LOG(INFO, "STEP_1.1:start_execute start to executor.");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service not inited", K(ret));
  } else if (!sql_proxy_.is_inited() || !sql_proxy_.is_active()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy not inited or not active",
        "sql_proxy inited",
        sql_proxy_.is_inited(),
        "sql_proxy active",
        sql_proxy_.is_active(),
        K(ret));
  } else if (server_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server_list is empty", K(server_list), K(ret));
  } else if (OB_FAIL(freeze_info_manager_.reload())) {
    BOOTSTRAP_LOG(WARN, "STEP_1.2:reload_freeze_info execute fail.");
  } else {
    BOOTSTRAP_LOG(INFO, "STEP_1.2:reload_freeze_info execute success.");
    // avoid bootstrap and do_restart run concurrently
    ObLatchWGuard guard(bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);
    ObBootstrap bootstrap(
        rpc_proxy_, ddl_service_, unit_manager_, leader_coordinator_, *config_, arg, rs_gts_manager_, common_proxy_);
    if (OB_FAIL(bootstrap.execute_bootstrap())) {
      LOG_ERROR("failed to execute_bootstrap", K(server_list), K(ret));
    }

    BOOTSTRAP_LOG(INFO, "start to do_restart");
    ObGlobalStatProxy global_proxy(sql_proxy_);
    int64_t frozen_version = 0;
    ObArray<ObAddr> self_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_restart())) {
      LOG_WARN("do restart task failed", K(ret));
    } else if (OB_FAIL(check_ddl_allowed())) {
      LOG_WARN("fail to check ddl allowed", K(ret));
    } else if (OB_FAIL(update_all_server_and_rslist())) {
      LOG_WARN("failed to update all_server and rslist", K(ret));
    } else if (OB_FAIL(zone_manager_.reload())) {
      LOG_ERROR("failed to reload zone manager", K(ret));
    } else if (OB_FAIL(leader_coordinator_.coordinate())) {
      LOG_WARN("leader_coordinator coordinate failed", K(ret));
    } else if (OB_FAIL(set_cluster_version())) {
      LOG_ERROR("set cluster version failed", K(ret));
    } else if (OB_FAIL(finish_bootstrap())) {
      LOG_WARN("failed to finish bootstrap", K(ret));
    } else if (OB_FAIL(update_baseline_schema_version())) {
      LOG_WARN("failed to update baseline schema version", K(ret));
    } else if (OB_FAIL(global_proxy.get_baseline_schema_version(frozen_version, baseline_schema_version_))) {
      LOG_WARN("fail to get baseline schema version", KR(ret));
    // } else if (OB_FAIL(set_max_trx_size_config())) {
    // LOG_WARN("fail to set max trx size config", K(ret));
    } else if (OB_FAIL(set_1pc_config())) {
      LOG_WARN("fail to set one phase commit config", K(ret));
    } else if (OB_FAIL(set_enable_oracle_priv_check())) {
      LOG_WARN("fail to set enable oracle priv check", K(ret));
    } else if (OB_FAIL(set_balance_strategy_config())) {
      LOG_WARN("fail to set balance strategy config", K(ret));
    }

    // clear bootstrap flag, regardless failure or success
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = clear_bootstrap())) {
      LOG_WARN("failed to clear bootstrap", K(ret), K(tmp_ret));
    }
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  BOOTSTRAP_LOG(INFO, "execute_bootstrap finished", K(ret));
  return ret;
}

int ObRootService::set_balance_strategy_config()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const char* CHANGE_SQL = "ALTER SYSTEM SET _partition_balance_strategy = 'standard'";
  if (OB_FAIL(sql_proxy_.write(CHANGE_SQL, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(CHANGE_SQL));
  }

  return ret;
}

int ObRootService::set_1pc_config()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const char* CHANGE_MAX_TRX_SIZE_SQL = "ALTER SYSTEM SET enable_one_phase_commit = false";
  if (OB_FAIL(sql_proxy_.write(CHANGE_MAX_TRX_SIZE_SQL, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(CHANGE_MAX_TRX_SIZE_SQL));
  } else if (OB_FAIL(check_config_result("enable_one_phase_commit", "False" /*expected value*/))) {
    LOG_WARN("fail to check enable one phase commit config same", K(ret));
  }

  return ret;
}

int ObRootService::set_max_trx_size_config()
{
  int ret = OB_SUCCESS;
  // ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  int64_t affected_rows = 0;
  const char* CHANGE_MAX_TRX_SIZE_SQL = "ALTER SYSTEM SET _max_trx_size = '100M'";
  if (OB_FAIL(sql_proxy_.write(CHANGE_MAX_TRX_SIZE_SQL, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(CHANGE_MAX_TRX_SIZE_SQL));
  } else if (OB_FAIL(check_config_result("_max_trx_size", "100M" /*expected value*/))) {
    LOG_WARN("fail to check all server config same", K(ret));
  }

  return ret;
}

int ObRootService::set_disable_ps_config()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  int64_t affected_rows = 0;
  const char* DISABLE_PS_SQL = "ALTER SYSTEM SET _ob_enable_prepared_statement = false";
  if (OB_FAIL(sql_proxy_.write(DISABLE_PS_SQL, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(DISABLE_PS_SQL));
  } else if (OB_FAIL(check_config_result("_ob_enable_prepared_statement", "False" /*expected value*/))) {
    LOG_WARN("fail to diable ps config", K(ret));
  }

  return ret;
}

int ObRootService::set_enable_oracle_priv_check()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  int64_t affected_rows = 0;
  const char* ENABLE_ORACLE_PRIV_SQL = "ALTER SYSTEM SET _enable_oracle_priv_check = true";
  if (OB_FAIL(sql_proxy_.write(ENABLE_ORACLE_PRIV_SQL, affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(ENABLE_ORACLE_PRIV_SQL));
  } else if (OB_FAIL(check_config_result("_enable_oracle_priv_check", "True" /*expected value*/))) {
    LOG_WARN("fail to set enable orace priv check", K(ret));
  }

  return ret;
}

int ObRootService::check_config_result(const char* name, const char* value)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  const int64_t start = ObTimeUtility::current_time();
  const uint64_t DEFAULT_WAIT_US = 120 * 1000 * 1000L;  // 120s
  int64_t timeout = DEFAULT_WAIT_US;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    timeout = max(DEFAULT_WAIT_US, THIS_WORKER.get_timeout_remain());
  }
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT count(*) as count FROM %s "
                               "WHERE name = '%s' and value != '%s'",
            OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TNAME,
            name,
            value))) {
      LOG_WARN("fail to append sql", K(ret));
    }
    while (OB_SUCC(ret)) {
      if (ObTimeUtility::current_time() - start > timeout) {
        ret = OB_TIMEOUT;
        LOG_WARN("sync config info use too much time",
            K(ret),
            K(name),
            K(value),
            "cost_us",
            ObTimeUtility::current_time() - start);
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
            if (count == 0) {
              break;
            }
          }
        }
      }
    }  // while end
  }
  return ret;
}

// DDL exection depends on full_service & major_freeze_done state. the sequence of these two status in bootstrap is:
// 1.rs do_restart: major_freeze_launcher start
// 2.rs do_restart success: full_service is true
// 3.root_major_freeze success: major_freeze_done is true (need full_service is true)
// the success of do_restart does not mean to execute DDL, therefor, add wait to bootstrap, to avoid bootstrap failure
// cause by DDL failure
int ObRootService::check_ddl_allowed()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_INTERVAL_US = 1 * 1000 * 1000;  // 1s
  while (OB_SUCC(ret) && !is_ddl_allowed()) {
    if (!in_service() && !is_start()) {
      ret = OB_RS_SHUTDOWN;
      LOG_WARN("rs shutdown", K(ret));
    } else if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait too long", K(ret));
    } else {
      usleep(SLEEP_INTERVAL_US);
    }
  }
  return ret;
}

int ObRootService::update_baseline_schema_version()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy_))) {
    LOG_WARN("trans start failed", K(ret));
  } else if (OB_FAIL(ddl_service_.get_schema_service().get_tenant_refreshed_schema_version(
                 OB_SYS_TENANT_ID, baseline_schema_version))) {
    LOG_WARN("fail to get refreshed schema version", K(ret));
  } else {
    ObGlobalStatProxy proxy(trans);
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
  LOG_DEBUG("update_baseline_schema_version finish", K(ret), K(temp_ret), K(baseline_schema_version));
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
    ObMultiVersionSchemaService& multi_schema_service = ddl_service_.get_schema_service();
    share::schema::ObSchemaService* tmp_schema_service = multi_schema_service.get_schema_service();
    if (OB_ISNULL(tmp_schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret), KP(tmp_schema_service));
    } else {
      ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
      share::schema::ObDDLSqlService ddl_sql_service(*tmp_schema_service);
      share::schema::ObSchemaOperation schema_operation;
      schema_operation.op_type_ = share::schema::OB_DDL_FINISH_BOOTSTRAP;
      schema_operation.tenant_id_ = tenant_id;
      if (OB_FAIL(multi_schema_service.gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id), K(new_schema_version));
      } else if (OB_FAIL(ddl_sql_service.log_nop_operation(
                     schema_operation, new_schema_version, schema_operation.ddl_stmt_str_, sql_proxy))) {
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

void ObRootService::construct_lease_expire_time(const ObLeaseRequest& lease_request,
    share::ObLeaseResponse& lease_response, const share::ObServerStatus& server_status)
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

int ObRootService::renew_lease(const ObLeaseRequest& lease_request, ObLeaseResponse& lease_response)
{
  int ret = OB_SUCCESS;
  ObServerStatus server_stat;
  uint64_t server_id = OB_INVALID_ID;
  bool to_alive = false;
  bool update_delay_time_flag = true;
  DEBUG_SYNC(HANG_HEART_BEAT_ON_RS);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!lease_request.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lease_request", K(lease_request), K(ret));
  } else if (OB_FAIL(server_manager_.receive_hb(lease_request, server_id, to_alive, update_delay_time_flag))) {
    LOG_WARN("server manager receive hb failed", K(lease_request), K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else {
    // before __all_zone load, it may fail, ignore it
    int temp_ret = OB_SUCCESS;
    int64_t lease_info_version = 0;
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    bool is_stopped = false;
    int64_t leader_cnt = -1;
    bool has_leader = true;
    int64_t global_max_decided_trans_version = 0;
    if (is_full_service()) {
      if (OB_FAIL(zone_manager_.get_lease_info_version(lease_info_version))) {
        LOG_WARN("get_lease_info_version failed", K(ret));
      } else if (OB_FAIL(zone_manager_.get_try_frozen_version(frozen_version, frozen_timestamp))) {
        LOG_WARN("get_try_frozen_version failed", K(ret));
      } else if (OB_FAIL(server_manager_.get_server_status(lease_request.server_, server_stat))) {
        LOG_WARN("get server status failed", K(ret), "server", lease_request.server_);
      } else if (OB_FAIL(server_manager_.is_server_stopped(lease_request.server_, is_stopped))) {
        LOG_WARN("check_server_stopped failed", K(ret), "server", lease_request.server_);
      } else if (OB_FAIL(server_manager_.get_server_leader_cnt(lease_request.server_, leader_cnt))) {
        LOG_WARN("fail to get server leader cnt", K(ret));
      } else if (OB_FAIL(global_trans_version_mgr_.get_global_max_decided_trans_version(
                     global_max_decided_trans_version))) {
        LOG_WARN("fail to get_global_max_decided_trans_version failed", K(ret));
      } else {
        if (leader_cnt > 0) {
          has_leader = true;
        } else if (0 == leader_cnt) {
          has_leader = false;
        } else if (leader_cnt < 0) {
          // leader_cnt has not reported to server manager by leader_coordinator
          has_leader = true;
        }
      }
    } else {
      LOG_DEBUG("heart beat before rootservice started", K(lease_request));
    }

    if (OB_SUCC(ret)) {
      lease_response.version_ = ObLeaseResponse::LEASE_VERSION;
      construct_lease_expire_time(lease_request, lease_response, server_stat);
      lease_response.lease_info_version_ = lease_info_version;
      lease_response.frozen_version_ = frozen_version;
      lease_response.server_id_ = server_id;
      lease_response.force_frozen_status_ = to_alive;
      lease_response.baseline_schema_version_ = baseline_schema_version_;
      // set observer stopped after has no leader
      lease_response.rs_server_status_ = (is_stopped && !has_leader) ? RSS_IS_STOPPED : RSS_IS_WORKING;
      lease_response.global_max_decided_trans_version_ = global_max_decided_trans_version;
      int64_t input_version = 0;
      ObSimpleFrozenStatus frozen_status;
      if (OB_SUCCESS != (temp_ret = freeze_info_manager_.get_freeze_info(input_version, frozen_status))) {
        LOG_WARN("fail to get frozen status, rs may not complete restart", K(temp_ret));
      } else {
        lease_response.frozen_status_.frozen_version_ = frozen_status.frozen_version_;
        lease_response.frozen_status_.frozen_timestamp_ = frozen_status.frozen_timestamp_;
        lease_response.frozen_status_.cluster_version_ = frozen_status.cluster_version_;
        lease_response.frozen_status_.status_ = common::COMMIT_SUCCEED;
        OTC_MGR.get_lease_response(lease_response);
      }

      // after split schema, the schema_version is not used, but for the legality detection, set schema_version to sys's
      // schema_version
      if (OB_SUCCESS !=
          (temp_ret = schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, lease_response.schema_version_))) {
        LOG_WARN("fail to get tenant schema version", K(temp_ret));
      }

      if (OB_SUCCESS != (temp_ret = schema_service_->get_refresh_schema_info(lease_response.refresh_schema_info_))) {
        LOG_WARN("fail to get refresh_schema_info", K(temp_ret));
      }
      LOG_TRACE("lease_request", K(lease_request), K(lease_response));
    }
  }
  return ret;
}

int ObRootService::get_root_partition(share::ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (OB_FAIL(pt_operator_->get(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 partition_info))) {
    LOG_WARN("get root partition failed",
        "table_id",
        combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        "partition_id",
        static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
        K(ret));
  }
  return ret;
}

int ObRootService::report_root_partition(const share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive request to report root partition", K(replica));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(replica), K(ret));
  } else {
    if (OB_FAIL(pt_operator_->update(replica))) {
      LOG_WARN("update all root partition failed", K(replica), K(ret));
    } else if (OB_ALL_CORE_TABLE_TID == extract_pure_id(replica.table_id_)) {
      core_meta_table_version_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObRootService::remove_root_partition(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive request to remove root partition", K(server));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(pt_operator_->remove(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 server))) {
    LOG_WARN("remove __all_core_table replica failed",
        "table_id",
        combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        "partition_id",
        static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
        K(server),
        K(ret));
  } else {
    core_meta_table_version_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObRootService::clear_rebuild_root_partition(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  const bool rebuild = false;
  LOG_INFO("receive clear rebuild root partition request", K(server));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(pt_operator_->update_rebuild_flag(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 server,
                 rebuild))) {
    LOG_WARN("rebuild __all_core_table replica failed",
        "table_id",
        combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        "partition_id",
        static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
        K(server),
        K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObRootService::rebuild_root_partition(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  const bool rebuild = true;
  LOG_INFO("receive rebuild root partition request", K(server));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not inited", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (OB_FAIL(pt_operator_->update_rebuild_flag(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 server,
                 rebuild))) {
    LOG_WARN("rebuild __all_core_table replica failed",
        "table_id",
        combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        "partition_id",
        static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
        K(server),
        K(ret));
  }
  return ret;
}

int ObRootService::fetch_location(const UInt64& table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rootservice not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", KT(table_id), K(ret));
  } else if (OB_FAIL(vtable_location_getter_.get(table_id, locations))) {
    LOG_WARN("vtable_location_getter get failed", KT(table_id), K(ret));
  }
  return ret;
}

int ObRootService::try_block_server(int rc, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (rc > 0 || rc <= -OB_MAX_ERROR_CODE || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rc), K(server), K(ret));
  } else if (OB_SERVER_MIGRATE_IN_DENIED == rc || OB_TOO_MANY_PARTITIONS_ERROR == rc) {
    LOG_INFO("receive server deny migrate in, try to block server migrate in", K(server));
    if (OB_FAIL(server_manager_.block_migrate_in(server))) {
      LOG_WARN("block migrate in failed", K(ret), K(server));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObRootService::create_resource_unit(const obrpc::ObCreateResourceUnitArg& arg)
{
  int ret = OB_SUCCESS;
  const bool if_not_exist = arg.if_not_exist_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (arg.unit_name_.empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "unit name");
    } else if (arg.max_cpu_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "max_cpu");
    } else if (arg.max_memory_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "max_memory");
    } else if (arg.max_iops_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "max_iops");
    } else if (arg.max_disk_size_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "max_disk_size");
    } else if (arg.max_session_num_ <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "max_session_num");
    }
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    LOG_INFO("receive create_resource_unit request", K(arg));
    ObUnitConfig unit_config;
    unit_config.name_ = arg.unit_name_;
    unit_config.min_cpu_ = arg.min_cpu_;
    unit_config.min_iops_ = arg.min_iops_;
    unit_config.min_memory_ = arg.min_memory_;
    unit_config.max_cpu_ = arg.max_cpu_;
    unit_config.max_memory_ = arg.max_memory_;
    unit_config.max_disk_size_ = arg.max_disk_size_;
    unit_config.max_iops_ = arg.max_iops_;
    unit_config.max_session_num_ = arg.max_session_num_;
    if (OB_FAIL(unit_manager_.create_unit_config(unit_config, if_not_exist))) {
      LOG_WARN("create_unit_config failed", K(unit_config), K(if_not_exist), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit', please try 'alter system "
                    "reload unit'",
              K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish create_resource_unit", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "create_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_resource_unit(const obrpc::ObAlterResourceUnitArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    LOG_INFO("receive alter_resource_unit request", K(arg));
    ObUnitConfig unit_config;
    unit_config.name_ = arg.unit_name_;
    unit_config.min_cpu_ = arg.min_cpu_;
    unit_config.min_iops_ = arg.min_iops_;
    unit_config.min_memory_ = arg.min_memory_;
    unit_config.max_cpu_ = arg.max_cpu_;
    unit_config.max_memory_ = arg.max_memory_;
    unit_config.max_disk_size_ = arg.max_disk_size_;
    unit_config.max_iops_ = arg.max_iops_;
    unit_config.max_session_num_ = arg.max_session_num_;
    if (OB_FAIL(unit_manager_.alter_unit_config(unit_config))) {
      LOG_WARN("alter_unit_config failed", K(unit_config), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit', please try 'alter system "
                    "reload unit'",
              K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish alter_resource_unit", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "alter_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::drop_resource_unit(const obrpc::ObDropResourceUnitArg& arg)
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
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish drop_resource_unit", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "drop_resource_unit", K(ret), K(arg));
  return ret;
}

int ObRootService::create_resource_pool(const obrpc::ObCreateResourcePoolArg& arg)
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
  } else if (REPLICA_TYPE_LOGONLY != arg.replica_type_ && REPLICA_TYPE_FULL != arg.replica_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only full/logonly pool are supported", K(ret), K(arg));
  } else if (REPLICA_TYPE_LOGONLY == arg.replica_type_ && arg.unit_num_ > 1) {
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
      LOG_WARN("create_resource_pool failed", K(pool), "unit_config", arg.unit_, K(if_not_exist), K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish create_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "create_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::split_resource_pool(const obrpc::ObSplitResourcePoolArg& arg)
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
    const common::ObIArray<common::ObString>& split_pool_list = arg.split_pool_list_;
    const common::ObIArray<common::ObZone>& zone_list = arg.zone_list_;
    if (OB_FAIL(unit_manager_.split_resource_pool(pool_name, split_pool_list, zone_list))) {
      LOG_WARN("fail to split resource pool", K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish split_resource_pool", K(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "split_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::merge_resource_pool(const obrpc::ObMergeResourcePoolArg& arg)
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
    const common::ObIArray<common::ObString>& old_pool_list = arg.old_pool_list_;
    const common::ObIArray<common::ObString>& new_pool_list = arg.new_pool_list_;
    if (OB_FAIL(unit_manager_.merge_resource_pool(old_pool_list, new_pool_list))) {
      LOG_WARN("fail to merge resource pool", K(ret));
      if (OB_SUCCESS != submit_reload_unit_manager_task()) {  // ensure submit task all case
        LOG_ERROR("fail to reload unit_mgr, please try 'alter system reload unit'", K(ret));
      }
    }
    LOG_INFO("finish merge_resource_pool", K(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "merge_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::alter_resource_pool(const obrpc::ObAlterResourcePoolArg& arg)
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
    } else if (OB_FAIL(unit_manager_.alter_resource_pool(pool, arg.unit_, arg.delete_unit_id_array_))) {
      LOG_WARN("alter_resource_pool failed", K(pool), K(arg), "resource unit", arg.unit_, K(ret));
      int mysql_error = -common::ob_mysql_errno(ret);
      if (OB_TIMEOUT == ret || OB_TIMEOUT == mysql_error) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = submit_reload_unit_manager_task())) {
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish alter_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "alter_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::drop_resource_pool(const obrpc::ObDropResourcePoolArg& arg)
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
          LOG_ERROR("fail to reload unit_manager, please try 'alter system reload unit'", K(tmp_ret));
        }
      }
    }
    LOG_INFO("finish drop_resource_pool", K(arg), K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "drop_resource_pool", K(ret), K(arg));
  return ret;
}

int ObRootService::check_tenant_in_alter_locality(const uint64_t tenant_id, bool& in_alter_locality)
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
  } else {
  }  // no more to do
  return ret;
}

int ObRootService::create_tenant(const ObCreateTenantArg& arg, UInt64& tenant_id)
{
  ObCurTraceId::init(GCONF.self_addr_);
  LOG_DEBUG("receive create tenant arg", K(arg));
  // standby cluster must not create tenant by self except sync from primary cluster
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_MISS_ARGUMENT;
    if (arg.tenant_schema_.get_tenant_name_str().empty()) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "tenant name");
    } else if (arg.pool_list_.count() <= 0) {
      LOG_USER_ERROR(OB_MISS_ARGUMENT, "resource_pool_list");
    }
    LOG_WARN("missing arg to create tenant", K(arg), K(ret));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tenant when cluster is upgrading not allowed", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "create tenant when cluster is upgrading");
  } else {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    LOG_INFO("receive create tenant request", K(arg));
    ObTenantSchema tenant_schema = arg.tenant_schema_;
    if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(ddl_service_.create_tenant(arg, tenant_schema, frozen_version))) {
      LOG_WARN("create_tenant failed", K(arg), K(tenant_schema), K(frozen_version), K(ret));
    } else if (OB_FAIL(ddl_service_.init_help_tables(tenant_schema))) {
      LOG_WARN("init help table failed", K(tenant_schema), K(ret));
    } else {
      tenant_id = tenant_schema.get_tenant_id();
    }
    LOG_INFO("finish create tenant", K(arg), K(tenant_id), K(ret));
  }
  return ret;
}

int ObRootService::create_tenant_end(const ObCreateTenantEndArg& arg)
{
  ObCurTraceId::init(GCONF.self_addr_);
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

int ObRootService::commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg& arg)
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

int ObRootService::commit_alter_tablegroup_locality(const rootserver::ObCommitAlterTablegroupLocalityArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("commit alter tablegroup locality", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.commit_alter_tablegroup_locality(arg))) {
    LOG_WARN("fail to commit alter tablegroup locality", K(ret));
  } else {
    LOG_INFO("commit alter tablegroup locality succeed", K(ret));
  }
  return ret;
}

int ObRootService::commit_alter_table_locality(const rootserver::ObCommitAlterTableLocalityArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("commit alter table locality", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.commit_alter_table_locality(arg))) {
    LOG_WARN("fail to commit alter table locality", K(ret));
  } else {
    LOG_INFO("commit alter table locality succeed", K(ret));
  }
  return ret;
}

int ObRootService::drop_tenant(const ObDropTenantArg& arg)
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

int ObRootService::flashback_tenant(const ObFlashBackTenantArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.flashback_tenant(arg))) {
    LOG_WARN("failed to flash back tenant", K(ret));
  }
  LOG_INFO("flashback tenant success");
  return ret;
}

int ObRootService::purge_tenant(const ObPurgeTenantArg& arg)
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

int ObRootService::modify_tenant(const ObModifyTenantArg& arg)
{
  LOG_DEBUG("receive modify tenant arg", K(arg));
  int ret = OB_NOT_SUPPORTED;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.modify_tenant(arg))) {
    LOG_WARN("ddl service modify tenant failed", K(arg), K(ret));
  } else {
    root_balancer_.wakeup();
  }
  // weak leader coordinator while modify primary zone
  if (OB_SUCC(ret) && arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE)) {
    leader_coordinator_.signal();
  }
  return ret;
}

int ObRootService::lock_tenant(const obrpc::ObLockTenantArg& arg)
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

int ObRootService::add_system_variable(const ObAddSysVarArg& arg)
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

int ObRootService::modify_system_variable(const obrpc::ObModifySysVarArg& arg)
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

int ObRootService::create_database(const ObCreateDatabaseArg& arg, UInt64& db_id)
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
    if (OB_FAIL(ddl_service_.create_database(arg.if_not_exist_, copied_db_schema, &arg.ddl_stmt_str_))) {
      LOG_WARN("create_database failed",
          "if_not_exist",
          arg.if_not_exist_,
          K(copied_db_schema),
          "ddl_stmt_str",
          arg.ddl_stmt_str_,
          K(ret));
    } else {
      db_id = copied_db_schema.get_database_id();
    }
  }
  return ret;
}

int ObRootService::alter_database(const ObAlterDatabaseArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_database(arg))) {
    LOG_WARN("alter database failed", K(arg), K(ret));
  }
  // weak leader coordinator while modify primary zone
  if (OB_SUCC(ret) && arg.alter_option_bitset_.has_member(obrpc::ObAlterDatabaseArg::PRIMARY_ZONE)) {
    leader_coordinator_.signal();
  }
  return ret;
}

int ObRootService::create_tablegroup(const ObCreateTablegroupArg& arg, UInt64& tg_id)
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
                   arg.if_not_exist_, copied_tg_schema, &arg.ddl_stmt_str_, arg.create_mode_))) {
      LOG_WARN("create_tablegroup failed",
          "if_not_exist",
          arg.if_not_exist_,
          K(copied_tg_schema),
          "ddl_stmt_str",
          arg.ddl_stmt_str_,
          K(ret));
    } else {
      tg_id = copied_tg_schema.get_tablegroup_id();
    }
  }
  return ret;
}

int ObRootService::get_frozen_status(const obrpc::Int64& arg, storage::ObFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  int64_t major_version = arg;
  UNUSED(frozen_status);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (major_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_version));
    //} else if (OB_FAIL(freeze_info_manager_.get_freeze_info(major_version, frozen_status))) {
    //  LOG_WARN("fail to get freeze info", K(ret), K(major_version));
    //} else if (!frozen_status.is_valid()) {
    //  ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not support function", KR(ret));
  }
  return ret;
}

int ObRootService::restore_partitions(const obrpc::ObRestorePartitionsArg& arg)
{
  LOG_INFO("receieve create table partitions request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_CREATE_TABLE_MODE_PHYSICAL_RESTORE == arg.mode_) {
    if (OB_FAIL(physical_restore_partitions(arg))) {
      LOG_WARN("physical restore partitions failed", K(ret), K(arg));
    }
  } else {
    if (OB_FAIL(logical_restore_partitions(arg))) {
      LOG_WARN("logical restore partitions failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::physical_restore_partitions(const obrpc::ObRestorePartitionsArg& arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t schema_id = arg.schema_id_;
  const uint64_t tenant_id = extract_tenant_id(schema_id);
  int64_t last_schema_version = OB_INVALID_VERSION;
  ObPhysicalRestoreJob job;
  ObPhysicalRestoreTableOperator table_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret), K(arg));
  } else if (is_inner_table(schema_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("restore sys table partitions not allowed", K(ret), K(schema_id));
  } else if (OB_FAIL(table_op.init(&sql_proxy_))) {
    LOG_WARN("failed to init table op", K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(arg));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, last_schema_version))) {
    LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_op.get_job_by_tenant_id(tenant_id, job))) {
    LOG_WARN("fail to get job", K(ret), K(tenant_id));
  } else {
    share::ObSimpleFrozenStatus frozen_status;
    frozen_status.frozen_version_ = job.restore_data_version_;
    frozen_status.frozen_timestamp_ = job.frozen_snapshot_version_;
    common::hash::ObHashSet<int64_t> base_part_id_set;
    if (OB_FAIL(base_part_id_set.create(hash::cal_next_prime(arg.partition_ids_.count() + 1),
            ObModIds::OB_PHYSICAL_RESTORE_PARTITIONS,
            ObModIds::OB_PHYSICAL_RESTORE_PARTITIONS))) {
      LOG_WARN("failed to create list value", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.partition_ids_.count(); i++) {
        const int64_t partition_id = arg.partition_ids_.at(i);
        if (OB_FAIL(base_part_id_set.set_refactored(partition_id))) {
          LOG_WARN("fail to set partition_id", K(ret), K(partition_id));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_new_tablegroup_id(schema_id)) {
      if (OB_FAIL(ddl_service_.create_tablegroup_partitions_for_physical_restore(
              arg, base_part_id_set, frozen_status, last_schema_version, schema_guard))) {
        LOG_WARN("fail to create table restore partitions", K(ret), K(arg));
      }
    } else {
      if (OB_FAIL(ddl_service_.create_table_partitions_for_physical_restore(
              arg, base_part_id_set, frozen_status, last_schema_version, schema_guard))) {
        LOG_WARN("fail to create table restore partitions", K(ret), K(arg));
      }
    }
  }
  return ret;
}

// baseline meta recovery adjustment:
// 1. at first, create schema.
// 2. later, create partition use the newest schema_version.
//    1) if id is tablegroup_id(high 40 bit 1), create partition of pg
//    2) if id is date_table_id, create partition of table and local index
//    3) if id is global index table_id and not binding, create partition of global index
int ObRootService::logical_restore_partitions(const obrpc::ObRestorePartitionsArg& arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObCreateTableMode create_mode = OB_CREATE_TABLE_MODE_RESTORE;
  int64_t frozen_version = 0;
  int64_t frozen_timestamp = 0;
  const uint64_t schema_id = arg.schema_id_;
  const uint64_t tenant_id = extract_tenant_id(schema_id);
  int64_t last_schema_version = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret), K(arg));
  } else if (is_inner_table(schema_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("restore sys table partitions not allowed", K(ret), K(schema_id));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(arg));
  } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, last_schema_version))) {
    LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
  } else {
    ObMySQLTransaction trans;
    ObGlobalStatProxy proxy(trans);
    if (OB_FAIL(trans.start(&sql_proxy_))) {
      LOG_WARN("failed to start trans, ", K(ret));
    } else if (is_new_tablegroup_id(schema_id)) {
      // PG
      const uint64_t tablegroup_id = schema_id;
      const ObTablegroupSchema* tablegroup_schema = NULL;
      if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
      } else if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(tablegroup_id));
      } else if (!tablegroup_schema->get_binding()) {
        // no need create partition while not binding tablegroup
      } else if (OB_FAIL(ddl_service_.create_tablegroup_partitions(create_mode, *tablegroup_schema))) {
        LOG_WARN("fail to create tablegroup partitions", K(ret), K(tablegroup_id));
      }
    } else {
      // standalone partition
      const uint64_t table_id = schema_id;
      const ObTableSchema* table_schema = NULL;
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      ObArray<ObTableSchema> table_schemas;
      if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
        LOG_WARN("get_frozen_info failed", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(table_id));
      } else if (OB_FAIL(table_schemas.push_back(*table_schema))) {
        LOG_WARN("fail to push back table schema", K(ret), K(table_id));
      } else if (table_schema->is_user_table()) {
        // only user table need add local index
        if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
          LOG_WARN("get_index_tid_array failed", K(ret), K(table_id));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
            const ObTableSchema* index_schema = NULL;
            if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
              LOG_WARN("get_table_schema failed", K(ret), K(table_id), "index_id", simple_index_infos.at(i).table_id_);
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("table schema should not be null",
                  K(ret),
                  K(table_id),
                  "index_id",
                  simple_index_infos.at(i).table_id_);
            } else if (index_schema->has_partition()) {
              // skip
              LOG_INFO("schema has partition, just skip",
                  K(ret),
                  K(table_id),
                  "index_id",
                  simple_index_infos.at(i).table_id_);
            } else if (OB_FAIL(table_schemas.push_back(*index_schema))) {
              LOG_WARN(
                  "fail o push back table schema", K(ret), K(table_id), "index_id", simple_index_infos.at(i).table_id_);
            } else {
              LOG_INFO("add index schema", K(ret), K(table_id), "index_id", simple_index_infos.at(i).table_id_);
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_service_.create_or_bind_tables_partitions(
                     schema_guard, *table_schema, last_schema_version, create_mode, frozen_version, table_schemas))) {
        LOG_WARN("create_table_partitions failed",
            K(ret),
            K(frozen_version),
            K(last_schema_version),
            K(create_mode),
            K(table_schemas));
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }
  return ret;
}

int ObRootService::create_table(const ObCreateTableArg& arg, ObCreateTableRes& res)
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
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    ObSchemaGetterGuard schema_guard;
    const ObDatabaseSchema* db_schema = NULL;
    schema_guard.set_session_id(arg.schema_.get_session_id());
    ObSchemaService* schema_service = schema_service_->get_schema_service();
    ObTableSchema table_schema;
    // generate base table schema
    if (OB_FAIL(table_schema.assign(arg.schema_))) {
      LOG_WARN("fail to assign schema", K(ret));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KP(schema_service), K(ret));
    } else if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(generate_table_schema_in_tenant_space(arg, table_schema))) {
      LOG_WARN("fail to generate table schema in tenant space", K(ret), K(arg));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                   table_schema.get_tenant_id(), schema_guard))) {
      LOG_WARN("get_schema_guard with version in inner table failed", K(ret));
    } else if (OB_INVALID_ID == table_schema.get_database_id()) {
      ObString database_name = arg.db_name_;
      if (OB_FAIL(schema_guard.get_database_schema(table_schema.get_tenant_id(), database_name, db_schema))) {
        LOG_WARN("get database schema failed", K(arg));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, database_name.length(), database_name.ptr());
      } else if (!arg.is_inner_ && db_schema->is_in_recyclebin()) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("Can't not create table of db in recyclebin", K(ret), K(arg), K(*db_schema));
      } else if (OB_INVALID_ID == db_schema->get_database_id()) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database id is invalid",
            "tenant_id",
            table_schema.get_tenant_id(),
            K(database_name),
            K(*db_schema),
            K(ret));
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
      ObSchemaGetterGuard::CheckTableType check_type = ObSchemaGetterGuard::ALL_TYPES;
      if (table_schema.is_mysql_tmp_table()) {
        check_type = ObSchemaGetterGuard::TEMP_TABLE_TYPE;
      } else if (0 == table_schema.get_session_id()) {
        // if session_id <> 0 during create table, need to exclude the existence of temporary table with the same
        // table_name, if there is, need to throw error.
        check_type = ObSchemaGetterGuard::NON_TEMP_TABLE_TYPE;
      }
      if (OB_SUCC(ret)) {
        ObArray<ObSchemaType> conflict_schema_types;
        if (OB_FAIL(schema_guard.check_oracle_object_exist(table_schema.get_tenant_id(),
                table_schema.get_database_id(),
                table_schema.get_table_name_str(),
                TABLE_SCHEMA,
                arg.if_not_exist_,
                conflict_schema_types))) {
          LOG_WARN("fail to check oracle_object exist", K(ret), K(table_schema));
        } else if (conflict_schema_types.count() > 0) {
          ret = OB_ERR_EXIST_OBJECT;
          LOG_WARN("Name is already used by an existing object", K(ret), K(table_schema));
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
          // create or replace view ...
          // create user table will drop the old view and recreate it in trans
          const ObSimpleTableSchemaV2* simple_table_schema = nullptr;
          if (OB_FAIL(schema_guard.get_simple_table_schema(table_schema.get_tenant_id(),
                  table_schema.get_database_id(),
                  table_schema.get_table_name_str(),
                  false, /*is index*/
                  simple_table_schema))) {
            LOG_WARN("failed to get table schema", K(ret));
          } else if (OB_ISNULL(simple_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("simple_table_schema is null", K(ret));
          } else if (simple_table_schema->get_table_type() == SYSTEM_VIEW ||
                     simple_table_schema->get_table_type() == USER_VIEW ||
                     simple_table_schema->get_table_type() == MATERIALIZED_VIEW) {
            ret = OB_SUCCESS;
          } else {
            bool is_oracle_mode = false;
            if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                    table_schema.get_tenant_id(), is_oracle_mode))) {
              LOG_WARN("fail to check is oracle mode", K(ret));
            } else if (is_oracle_mode) {
              ret = OB_ERR_EXIST_OBJECT;
              LOG_WARN("name is already used by an existing object", K(ret), K(table_schema.get_table_name_str()));
            } else {  // mysql mode
              const ObDatabaseSchema* db_schema = nullptr;
              if (OB_FAIL(schema_guard.get_database_schema(table_schema.get_database_id(), db_schema))) {
                LOG_WARN("get db schema failed", K(ret), K(table_schema.get_database_id()));
              } else if (OB_ISNULL(db_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("db schema is null", K(ret));
              } else {
                ret = OB_ERR_WRONG_OBJECT;
                LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
                    to_cstring(db_schema->get_database_name_str()),
                    to_cstring(table_schema.get_table_name_str()),
                    "VIEW");
                LOG_WARN("table exist", K(ret), K(table_schema));
              }
            }
          }
        } else {
          ret = OB_ERR_TABLE_EXIST;
          LOG_WARN("table exist", K(ret), K(table_schema), K(arg.if_not_exist_));
        }
      }
    }
    RS_TRACE(generate_schema_start);
    // bool can_hold_new_table = false;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ddl_service_.generate_schema(arg, table_schema, frozen_version))) {
      LOG_WARN("generate_schema for table failed", K(frozen_version), K(ret));
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
        ObCreateIndexArg& index_arg = const_cast<ObCreateIndexArg&>(arg.index_arg_list_.at(i));
        // if we pass the table_schema argument, the create_index_arg can not set database_name
        // and table_name, which will used from get data table schema in generate_schema
        if (!index_arg.index_schema_.is_partitioned_table() && !table_schema.is_partitioned_table()) {
          if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE;
          } else if (INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
            index_arg.index_type_ = INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE;
          }
        }
        // the global index has generated column schema during resolve, RS no need to generate index schema,
        // just assign column schema
        if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_ || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
          if (OB_FAIL(index_schema.assign(index_arg.index_schema_))) {
            LOG_WARN("fail to assign schema", K(ret));
          }
        }
        const bool global_index_without_column_info = false;
        ObSEArray<ObColumnSchemaV2*, 1> gen_columns;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObIndexBuilderUtil::adjust_expr_index_args(index_arg, table_schema, gen_columns))) {
          LOG_WARN("fail to adjust expr index args", K(ret));
        } else if (OB_FAIL(index_builder.generate_schema(
                       index_arg, frozen_version, table_schema, global_index_without_column_info, index_schema))) {
          LOG_WARN("generate_schema for index failed", K(index_arg), K(frozen_version), K(table_schema), K(ret));
        } else if (INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_ ||
                   INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
          if (OB_FAIL(ddl_service_.generate_global_index_locality_and_primary_zone(table_schema, index_schema))) {
            LOG_WARN("fail to generate global index locality and primary zone", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          uint64_t new_table_id = OB_INVALID_ID;
          if (OB_FAIL(schema_service->fetch_new_table_id(table_schema.get_tenant_id(), new_table_id))) {
            LOG_WARN("failed to fetch_new_table_id", "tenant_id", table_schema.get_tenant_id(), K(ret));
          } else {
            index_schema.set_table_id(new_table_id);
            // index_schema.set_data_table_id(table_id);
            if (OB_FAIL(table_schemas.push_back(index_schema))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
        }
      }
      bool is_oracle_mode = false;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
                table_schema.get_tenant_id(), is_oracle_mode))) {
          LOG_WARN("fail to check is oracle mode", K(ret));
        }
      }
      // check foreign key info.
      // addforeign_key_info to table_schema
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.foreign_key_arg_list_.count(); i++) {
          const ObCreateForeignKeyArg& foreign_key_arg = arg.foreign_key_arg_list_.at(i);
          ObForeignKeyInfo foreign_key_info;
          // check for duplicate constraint names of foregin key
          if (!foreign_key_arg.foreign_key_name_.empty()) {
            bool is_foreign_key_name_exist = true;
            if (OB_FAIL(ddl_service_.check_constraint_name_is_exist(
                    schema_guard, table_schema, foreign_key_arg.foreign_key_name_, is_foreign_key_name_exist))) {
              LOG_WARN("fail to check foreign key name is exist or not", K(ret), K(foreign_key_arg.foreign_key_name_));
            } else if (is_foreign_key_name_exist) {
              if (is_oracle_mode) {
                ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
                LOG_WARN("fk name is duplicate", K(ret), K(foreign_key_arg.foreign_key_name_));
              } else {  // mysql mode
                ret = OB_ERR_DUP_KEY;
                LOG_USER_ERROR(OB_ERR_DUP_KEY,
                    table_schema.get_table_name_str().length(),
                    table_schema.get_table_name_str().ptr());
              }
            }
          }
          // end of check for duplicate constraint names of foregin key
          const ObTableSchema* parent_schema = NULL;
          if (OB_SUCC(ret)) {
            // get parent table schema.
            // TODO: is it necessory to determine whether it is case sensitive by check sys variable
            // check whether it belongs to self reference, if so, the parent schema is child schema.
            if (0 == foreign_key_arg.parent_table_.case_compare(table_schema.get_table_name_str()) &&
                0 == foreign_key_arg.parent_database_.case_compare(arg.db_name_)) {
              parent_schema = &table_schema;
              if (CONSTRAINT_TYPE_PRIMARY_KEY == foreign_key_arg.ref_cst_type_) {
                for (ObTableSchema::const_constraint_iterator iter = parent_schema->constraint_begin();
                     iter != parent_schema->constraint_end();
                     ++iter) {
                  if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
                    foreign_key_info.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
                    foreign_key_info.ref_cst_id_ = (*iter)->get_constraint_id();
                    break;
                  }
                }
              } else {
                if (OB_FAIL(
                        ddl_service_.get_uk_cst_id_for_self_ref(table_schemas, foreign_key_arg, foreign_key_info))) {
                  LOG_WARN("failed to get uk cst id for self ref", K(ret), K(foreign_key_arg));
                }
              }
            } else if (OB_FAIL(schema_guard.get_table_schema(table_schema.get_tenant_id(),
                           foreign_key_arg.parent_database_,
                           foreign_key_arg.parent_table_,
                           false,
                           parent_schema))) {
              LOG_WARN("failed to get parent table schema", K(ret), K(foreign_key_arg));
            } else {
              foreign_key_info.ref_cst_type_ = foreign_key_arg.ref_cst_type_;
              foreign_key_info.ref_cst_id_ = foreign_key_arg.ref_cst_id_;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(parent_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              LOG_WARN("parent table is not exist", K(ret), K(foreign_key_arg));
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
            foreign_key_info.parent_table_id_ = parent_schema->get_table_id();
            for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.child_columns_.count(); j++) {
              const ObString& column_name = foreign_key_arg.child_columns_.at(j);
              const ObColumnSchemaV2* column_schema = table_schema.get_column_schema(column_name);
              if (OB_ISNULL(column_schema)) {
                ret = OB_ERR_COLUMN_NOT_FOUND;
                LOG_WARN("child column is not exist", K(ret), K(column_name));
              } else if (OB_FAIL(foreign_key_info.child_column_ids_.push_back(column_schema->get_column_id()))) {
                LOG_WARN("failed to push child column id", K(ret), K(column_name));
              }
            }
          }
          // get parent column schema.
          if (OB_SUCC(ret)) {
            for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_arg.parent_columns_.count(); j++) {
              const ObString& column_name = foreign_key_arg.parent_columns_.at(j);
              const ObColumnSchemaV2* column_schema = parent_schema->get_column_schema(column_name);
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
          }
          // add foreign key info.
          if (OB_SUCC(ret)) {
            if (OB_FAIL(schema_service->fetch_new_constraint_id(
                    table_schema.get_tenant_id(), foreign_key_info.foreign_key_id_))) {
              LOG_WARN("failed to fetch new foreign key id", K(ret), K(foreign_key_arg));
            } else if (OB_FAIL(table_schema.add_foreign_key_info(foreign_key_info))) {
              LOG_WARN("failed to push foreign key info", K(ret), K(foreign_key_info));
            }
          }
        }  // for
      }    // check foreign key info end.
    }
    RS_TRACE(generate_schema_finish);
    if (OB_SUCC(ret)) {
      obrpc::ObCreateTableMode create_mode = arg.create_mode_;
      if (OB_FAIL(table_schemas.at(0).assign(table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else if (OB_FAIL(ddl_service_.create_user_tables(arg.if_not_exist_,
                     arg.ddl_stmt_str_,
                     table_schemas,
                     frozen_version,
                     create_mode,
                     schema_guard,
                     arg.last_replay_log_id_))) {
        LOG_WARN("create_user_tables failed",
            "if_not_exist",
            arg.if_not_exist_,
            "ddl_stmt_str",
            arg.ddl_stmt_str_,
            K(frozen_version),
            K(ret));
      }
    }
    if (OB_ERR_TABLE_EXIST == ret) {
      // create table xx if not exist (...)
      // create or replace view xx as ...
      if (arg.if_not_exist_) {
        ret = OB_SUCCESS;
        LOG_INFO("table is exist, no need to create again, ",
            "tenant_id",
            table_schema.get_tenant_id(),
            "database_id",
            table_schema.get_database_id(),
            "table_name",
            table_schema.get_table_name());
      } else {
        ret = OB_ERR_TABLE_EXIST;
        LOG_USER_ERROR(
            OB_ERR_TABLE_EXIST, table_schema.get_table_name_str().length(), table_schema.get_table_name_str().ptr());
        LOG_WARN("table is exist, cannot create it twice,",
            "tenant_id",
            table_schema.get_tenant_id(),
            "database_id",
            table_schema.get_database_id(),
            "table_name",
            table_schema.get_table_name(),
            K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      uint64_t tenant_id = table_schema.get_tenant_id();
      if (is_inner_table(res.table_id_)) {
        tenant_id = OB_SYS_TENANT_ID;
      }
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
int ObRootService::generate_table_schema_in_tenant_space(const ObCreateTableArg& arg, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = table_schema.get_table_id();
  const ObPartitionLevel part_level = table_schema.get_part_level();
  const ObPartitionFuncType part_func_type = table_schema.get_part_option().get_part_func_type();
  uint64_t tenant_id = extract_tenant_id(table_id);
  if (OB_INVALID_TENANT_ID == tenant_id) {
    // compatible with the behavior of creating a specify pure_table_id table
    tenant_id = OB_SYS_TENANT_ID;
  }
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id || !is_inner_table(table_id)) {
    // skip
  } else if (OB_SYS_TENANT_ID != arg.exec_tenant_id_) {
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
  } else if (part_level > ObPartitionLevel::PARTITION_LEVEL_ONE || !is_hash_like_part(part_func_type)) {
    // sys tables do not write __all_part table, so sys table only support non-partition or only level hash_like part
    // type.
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
    table_schema.set_tablegroup_id(combine_id(tenant_id, OB_SYS_TABLEGROUP_ID));
    table_schema.set_tablegroup_name(OB_SYS_TABLEGROUP_NAME);
    table_schema.set_database_id(combine_id(tenant_id, OB_SYS_DATABASE_ID));
  }
  return ret;
}

int ObRootService::alter_table(const obrpc::ObAlterTableArg& arg, obrpc::ObAlterTableRes& res)
{
  LOG_DEBUG("receive alter table arg", K(arg));
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = arg.alter_table_schema_.get_tenant_id();
  const ObSimpleTableSchemaV2* simple_table_schema = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(table_allow_ddl_operation(arg))) {
      LOG_WARN("table can't do ddl now", K(ret));
    } else if (OB_FAIL(arg.alter_table_schema_.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check if tenant mode is oracle mode", K(ret), K(is_oracle_mode));
    } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(ddl_service_.alter_table((const_cast<obrpc::ObAlterTableArg&>(arg)), frozen_version))) {
      LOG_WARN("alter_user_table failed", K(arg), K(frozen_version), K(ret));
    } else {
      partition_spliter_.wakeup();
      obrpc::ObCreateIndexArg* create_index_arg = NULL;
      const ObSArray<obrpc::ObIndexArg*>& index_arg_list = arg.index_arg_list_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg_list.size(); ++i) {
        obrpc::ObIndexArg* index_arg = index_arg_list.at(i);
        if (OB_ISNULL(index_arg)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("index arg should not be null", K(ret));
        } else if (obrpc::ObIndexArg::ADD_INDEX == index_arg->index_action_type_
                   // REBUILD_INDEX is triggered by drop/truncate partition.
                   // normal operation will not set the flat.
                   || obrpc::ObIndexArg::REBUILD_INDEX == index_arg->index_action_type_) {
          if (NULL == (create_index_arg = static_cast<obrpc::ObCreateIndexArg*>(index_arg))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index arg is null", K(ret));
          } else {
            /*
             *  some illustration of ObAlterTableRes:
             *  the return is used for observer do some checking after RS end processing alter table and return to
             * observer.
             *    1. before 3.1, it guarantees that each RPC only send one request to RS, the RPC only has a create
             * index operator, return the index_table_id_ and schema version.
             *    2. after 3.1, RPC will send all operations of alter table to RS. the RPC may has multiple create index
             * operations. retrun res_arg_array_. each element of the array include index_table_id_ and schema_version_
             * of each index.
             * */
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
              res.index_table_id_ = create_index_arg->index_schema_.get_table_id();
              res.schema_version_ = create_index_arg->index_schema_.get_schema_version();
            } else {
              obrpc::ObAlterTableResArg arg(TABLE_SCHEMA,
                  create_index_arg->index_schema_.get_table_id(),
                  create_index_arg->index_schema_.get_schema_version());
              if (OB_FAIL(res.res_arg_array_.push_back(arg))) {
                LOG_WARN("push back to res_arg_array failed", K(ret), K(arg));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        const ObSimpleTableSchemaV2* simple_table_schema = NULL;
        // there are multiple DDL except alter table, ctas, comment on, eg.
        // but only alter_table specify table_id, so if no table_id, it indicates DDL is not alter table, skip.
        if (OB_INVALID_ID == arg.alter_table_schema_.get_table_id()) {
          // skip
        } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
          LOG_WARN("get schema guard in inner table failed", K(ret));
        } else if (OB_FAIL(
                       schema_guard.get_table_schema(arg.alter_table_schema_.get_table_id(), simple_table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), K(arg.alter_table_schema_.get_table_id()));
        } else if (OB_ISNULL(simple_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("simple_table_schema is NULL ptr", K(ret), K(simple_table_schema), K(ret));
        } else if (is_oracle_mode &&
                   (arg.alter_constraint_type_ == obrpc::ObAlterTableArg::ADD_CONSTRAINT ||
                       arg.alter_constraint_type_ == obrpc::ObAlterTableArg::ALTER_CONSTRAINT_STATE)) {
          ObTableSchema::const_constraint_iterator iter = arg.alter_table_schema_.constraint_begin();
          if (iter + 1 != arg.alter_table_schema_.constraint_end()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alter table could only add one constraint once", K(ret));
          } else {
            /*
             * observer need return while create index, create constraint or modify constraint.
             * in the resolver phase, it ensures that no other actions will happen at the same time while alter table.
             * check constraint need return constriant_id_ and schema_version_. other constraint return schema_version_.
             * the schema version is data table after finish alter table.
             */
            res.constriant_id_ = (*iter)->get_constraint_id();
            res.schema_version_ = simple_table_schema->get_schema_version();
          }
        } else {
          res.schema_version_ = simple_table_schema->get_schema_version();
        }
      }
    }
  }
  // weak up leader coordinator while modify primary zone
  if (OB_SUCC(ret) && arg.alter_table_schema_.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::PRIMARY_ZONE)) {
    leader_coordinator_.signal();
  }

  return ret;
}

int ObRootService::create_index(const ObCreateIndexArg& arg, obrpc::ObAlterTableRes& res)
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
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    ObIndexBuilder index_builder(ddl_service_);
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (OB_FAIL(check_parallel_ddl_conflict(schema_guard, arg))) {
      LOG_WARN("check parallel ddl conflict failed", K(ret));
    } else if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(index_builder.create_index(arg, frozen_version))) {
      LOG_WARN("create_index failed", K(arg), K(frozen_version), K(ret));
    } else {
      res.index_table_id_ = arg.index_schema_.get_table_id();
      res.schema_version_ = arg.index_schema_.get_schema_version();
    }
  }
  return ret;
}

int ObRootService::drop_table(const obrpc::ObDropTableArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_table(arg))) {
    LOG_WARN("ddl service failed to drop table", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::drop_database(const obrpc::ObDropDatabaseArg& arg, UInt64& affected_row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.drop_database(arg, affected_row))) {
    LOG_WARN("ddl_service_ drop_database failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::drop_tablegroup(const obrpc::ObDropTablegroupArg& arg)
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

int ObRootService::alter_tablegroup(const obrpc::ObAlterTablegroupArg& arg)
{
  LOG_DEBUG("receive alter tablegroup arg", K(arg));
  const ObTablegroupSchema* tablegroup_schema = NULL;
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
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, arg.tablegroup_name_, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret));
  } else if (OB_INVALID_ID == tablegroup_id) {
    ret = OB_TABLEGROUP_NOT_EXIST;
    LOG_WARN("get invalid tablegroup schema", KR(ret), K(arg));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(ret));
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
    partition_spliter_.wakeup();
  }
  // weak up leader coordinator while modify primary zone
  if (OB_SUCC(ret) && arg.alter_option_bitset_.has_member(obrpc::ObAlterTablegroupArg::PRIMARY_ZONE)) {
    leader_coordinator_.signal();
  }
  return ret;
}

// skip feasibility verification while force drop index
int ObRootService::force_drop_index(const obrpc::ObDropIndexArg& arg)
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
    if (OB_FAIL(index_builder.drop_index(arg))) {
      LOG_WARN("index_builder drop_index failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::drop_index(const obrpc::ObDropIndexArg& arg)
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
    if (OB_FAIL(index_builder.drop_index(arg))) {
      LOG_WARN("index_builder drop_index failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::rebuild_index(const obrpc::ObRebuildIndexArg& arg, obrpc::ObAlterTableRes& res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(ddl_service_.rebuild_index(arg, frozen_version, res))) {
      LOG_WARN("ddl_service rebuild index failed", K(arg), K(ret));
    }
  }
  return ret;
}

int ObRootService::submit_build_index_task(const obrpc::ObSubmitBuildIndexTaskArg &arg)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  const int64_t alloc_size = sizeof(ObTableSchema);
  char *buf = nullptr;
  ObTableSchema *deep_copy_index_schema = nullptr;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for deep copy schema", K(ret), K(alloc_size));
  } else {
    const uint64_t index_tid = arg.index_tid_;
    const ObTableSchema *index_schema = nullptr;
    deep_copy_index_schema = new (buf) ObTableSchema(&allocator);
    ObSchemaGetterGuard schema_guard;
    int64_t latest_schema_version = 0;
    // In DDL task, the schema version should be the latest, otherwise the schema can be already recycled
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_refreshed_schema_version(
            extract_tenant_id(index_tid), latest_schema_version))) {
      LOG_WARN("fail to get latest schema version", K(ret), K(arg));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                   extract_tenant_id(index_tid), schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(index_tid));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_tid, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(index_tid));
    } else if (OB_ISNULL(index_schema)) {
      LOG_INFO("index schema is deleted, skip it");
    } else if (INDEX_STATUS_UNAVAILABLE != index_schema->get_index_status()) {
      LOG_INFO("index build is already completed, skip it", K(ret), K(index_tid));
    } else if (OB_FAIL(deep_copy_index_schema->assign(*index_schema))) {
      LOG_WARN("fail to assign index schema", K(ret), K(*index_schema));
    } else if (FALSE_IT(deep_copy_index_schema->set_schema_version(latest_schema_version))) {
    } else if (deep_copy_index_schema->is_global_index_table() &&
               OB_FAIL(global_index_builder_.submit_build_global_index_task(deep_copy_index_schema))) {
      LOG_WARN("fail to submit build global index task", K(ret), K(*deep_copy_index_schema));
    } else if (deep_copy_index_schema->is_index_local_storage()) {
      ObIndexBuilder index_builder(ddl_service_);
      if (OB_FAIL(index_builder.submit_build_local_index_task(*deep_copy_index_schema))) {
        LOG_WARN("fail to submit build local index task", K(ret), K(*deep_copy_index_schema));
      }
    }
  }
  if (deep_copy_index_schema != nullptr) {
    deep_copy_index_schema->~ObTableSchema();
    allocator.free(deep_copy_index_schema);
  }
  return ret;
}

int ObRootService::flashback_index(const ObFlashBackIndexArg &arg)
{
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

int ObRootService::purge_index(const ObPurgeIndexArg& arg)
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

int ObRootService::rename_table(const obrpc::ObRenameTableArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    const bool is_sync_primary_query = arg.is_sync_primary_query();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.rename_table_items_.count(); ++i) {
      const ObString& origin_database_name = arg.rename_table_items_.at(i).origin_db_name_;
      const ObString& origin_table_name = arg.rename_table_items_.at(i).origin_table_name_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service_.rename_table(arg))) {
      LOG_WARN("rename table failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::truncate_table(const obrpc::ObTruncateTableArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(table_is_split(arg.tenant_id_, arg.database_name_, arg.table_name_))) {
    LOG_WARN("table is split", K(ret), K(arg));
  } else {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(ddl_service_.truncate_table(arg, frozen_version))) {
      LOG_WARN("truncate table failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::create_table_like(const ObCreateTableLikeArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_timestamp))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else if (OB_FAIL(ddl_service_.create_table_like(arg, frozen_version))) {
      if (OB_ERR_TABLE_EXIST == ret) {
        // create table xx if not exist like
        if (arg.if_not_exist_) {
          LOG_USER_NOTE(OB_ERR_TABLE_EXIST, arg.new_table_name_.length(), arg.new_table_name_.ptr());
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
int ObRootService::flashback_table_from_recyclebin(const ObFlashBackTableFromRecyclebinArg& arg)
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

int ObRootService::purge_table(const ObPurgeTableArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t dummy_cnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_table(arg, dummy_cnt))) {
    LOG_WARN("failed to purge table", K(ret));
  }
  return ret;
}

int ObRootService::flashback_database(const ObFlashBackDatabaseArg& arg)
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

int ObRootService::purge_database(const ObPurgeDatabaseArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t dummy_cnt = -1;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.purge_database(arg, dummy_cnt))) {
    LOG_WARN("failed to purge database", K(ret));
  }
  return ret;
}

int ObRootService::purge_expire_recycle_objects(const ObPurgeRecycleBinArg& arg, Int64& affected_rows)
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

int ObRootService::optimize_table(const ObOptimizeTableArg& arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  LOG_INFO("receive optimize table request", K(arg));
  ObWorker::CompatMode mode;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, schema service must not be NULL", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(arg.tenant_id_, mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else {
    const int64_t all_core_table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tables_.count(); ++i) {
      obrpc::ObAlterTableArg alter_table_arg;
      ObSqlString sql;
      const obrpc::ObTableItem& table_item = arg.tables_.at(i);
      const ObTableSchema* table_schema = nullptr;
      alter_table_arg.is_alter_options_ = true;
      alter_table_arg.alter_table_schema_.set_origin_database_name(table_item.database_name_);
      alter_table_arg.alter_table_schema_.set_origin_table_name(table_item.table_name_);
      alter_table_arg.alter_table_schema_.set_tenant_id(arg.tenant_id_);
      alter_table_arg.skip_sys_table_check_ = true;
      // exec_tenant_id_ is used in standby cluster
      alter_table_arg.exec_tenant_id_ = arg.exec_tenant_id_;
      if (OB_FAIL(alter_table_arg.primary_schema_versions_.assign(arg.primary_schema_versions_))) {
        LOG_WARN("failed to assign primary schema version", K(ret), K(arg));
      } else if (OB_FAIL(
                     ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret));
      } else if (OB_FAIL(schema_guard.get_table_schema(arg.tenant_id_,
                     table_item.database_name_,
                     table_item.table_name_,
                     false /*is index*/,
                     table_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (nullptr == table_schema) {
        // skip deleted table
      } else if (all_core_table_id == table_schema->get_table_id()) {
        // do nothing
      } else if (OB_SYS_TENANT_ID != arg.tenant_id_ && table_schema->is_sys_table()) {
        // do nothing
      } else {
        if (ObWorker::CompatMode::MYSQL == mode) {
          if (OB_FAIL(sql.append_fmt(
                  "OPTIMIZE TABLE `%.*s`", table_item.table_name_.length(), table_item.table_name_.ptr()))) {
            LOG_WARN("fail to assign sql stmt", K(ret));
          }
        } else if (ObWorker::CompatMode::ORACLE == mode) {
          if (OB_FAIL(sql.append_fmt("ALTER TABLE \"%.*s\" SHRINK SPACE",
                  table_item.table_name_.length(),
                  table_item.table_name_.ptr()))) {
            LOG_WARN("fail to append fmt", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, unknown mode", K(ret), K(mode));
        }
        if (OB_SUCC(ret)) {
          alter_table_arg.ddl_stmt_str_ = sql.string();
          obrpc::ObAlterTableRes res;
          if (OB_FAIL(alter_table_arg.alter_table_schema_.alter_option_bitset_.add_member(
                  ObAlterTableArg::PROGRESSIVE_MERGE_ROUND))) {
            LOG_WARN("fail to add member", K(ret));
          } else if (OB_FAIL(alter_table(alter_table_arg, res))) {
            LOG_WARN("fail to alter table", K(ret), K(alter_table_arg));
          }
        }
      }
    }
  }
  return ret;
}

int ObRootService::check_unique_index_response(const obrpc::ObCheckUniqueIndexResponseArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(global_index_builder_.on_check_unique_index_reply(arg.pkey_, arg.ret_code_, arg.is_valid_))) {
    LOG_WARN("fail to reply uniqe index check", K(ret), K(arg));
  } else {
  }  // no more to do
  return ret;
}

int ObRootService::calc_column_checksum_repsonse(const obrpc::ObCalcColumnChecksumResponseArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(
                 global_index_builder_.on_col_checksum_calculation_reply(arg.index_id_, arg.pkey_, arg.ret_code_))) {
    LOG_WARN("fail to reply column checksum calculation", K(ret));
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

int ObRootService::wait_refresh_config()
{
  int ret = OB_SUCCESS;
  int64_t lastest_config_version = 0;
  const int64_t retry_time_limit = 3;
  int64_t retry_time = 0;
  const int64_t sleep_us = 1000 * 1000LL;  // 1s

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_manager_.get_config_version(lastest_config_version))) {
    LOG_WARN("get_config_version failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      const int64_t current_version = config_mgr_->get_current_version();
      ++retry_time;
      if (current_version >= lastest_config_version) {
        break;
      } else if (retry_time > retry_time_limit) {
        ret = OB_CONFIG_NOT_SYNC;
        LOG_ERROR("failed to wait refresh config, config version is too old",
            K(ret),
            K(retry_time),
            K(retry_time_limit),
            K(lastest_config_version),
            K(current_version));
      } else {
        LOG_INFO("config version too old, retry after 1s",
            K(retry_time),
            K(retry_time_limit),
            K(lastest_config_version),
            K(current_version));
        usleep(sleep_us);
      }
    }
  }

  return ret;
}

int ObRootService::get_frozen_version(obrpc::Int64& frozen_version)
{
  int ret = OB_SUCCESS;
  int64_t inner_frozen_version = 0;
  int64_t inner_frozen_time = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_manager_.get_frozen_info(inner_frozen_version, inner_frozen_time))) {
    LOG_WARN("get_frozen_info failed", K(ret));
  } else {
    frozen_version = inner_frozen_version;
  }
  return ret;
}

int ObRootService::root_major_freeze(const ObRootMajorFreezeArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive major freeze request", K(arg));

  // major freeze will retry to success, so ignore the request timeout.
  int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (!GCONF.enable_major_freeze && arg.launch_new_round_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("enable_major_freeze is off, refuse to to major_freeze");
  } else if (arg.launch_new_round_) {
    if (OB_FAIL(
            root_major_freeze_v2_.global_major_freeze(arg.try_frozen_version_, arg.svr_, arg.tenant_id_, self_addr_))) {
      LOG_WARN("fail to lauch major freeze", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObRootMinorFreezeArg arg;
      if (OB_SUCCESS != (tmp_ret = submit_async_minor_freeze_task(arg))) {
        LOG_WARN("fail to try minor freeze", K(ret), K(tmp_ret));
      }
    }
  }  // end lauch_new_round
  if (OB_SUCC(ret) || (OB_MAJOR_FREEZE_NOT_ALLOW == ret && !arg.launch_new_round_)) {
    // major_freeze_done_ indicate major freeze is done before RS full service.
    // then ddl can be executed.
    // if any observer in cluster is down at this time,  will return OB_MAJOR_FREEZE_NOT_ALLOW,
    // but DDL can be executed
    // major_freeze_done_ = true;
  }
  if (OB_SUCC(ret)) {
    int64_t frozen_version = 0;
    share::ObSimpleFrozenStatus frozen_status;
    if (OB_FAIL(freeze_info_manager_.get_freeze_info(frozen_version, frozen_status))) {
      LOG_WARN("get_frozen_info failed", K(ret));
    } else {
      LOG_INFO("major_freeze succeed", K(frozen_status));
    }
    daily_merge_scheduler_.wakeup();
  }
  THIS_WORKER.set_timeout_ts(timeout_ts);
  return ret;
}

int ObRootService::root_minor_freeze(const ObRootMinorFreezeArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive minor freeze request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(root_minor_freeze_.try_minor_freeze(
                 arg.tenant_ids_, arg.partition_key_, arg.server_list_, arg.zone_))) {
    LOG_WARN("minor freeze failed", K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "root_minor_freeze", K(ret), K(arg));
  return ret;
}

int ObRootService::update_index_status(const obrpc::ObUpdateIndexStatusArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    int64_t frozen_time = 0;
    int64_t frozen_version = arg.create_mem_version_;
    if (arg.create_mem_version_ <= 0) {
      if (OB_FAIL(zone_manager_.get_frozen_info(frozen_version, frozen_time))) {
        LOG_WARN("get_frozen_info failed", K(ret));
      }
    }
    LOG_INFO("update index table status", K(arg), K(frozen_version));

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_service_.update_index_status(arg, frozen_version))) {
      LOG_WARN("update index table status failed", K(ret), K(arg), K(frozen_version));
    }

    // in recovery tenant rebuild_index stage, try to wake up restore_scheduler
    // while update index status.
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      const int64_t tenant_id = extract_tenant_id(arg.index_table_id_);
      bool is_restore = false;
      if (OB_ISNULL(schema_service_)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema service is null", KR(tmp_ret), K(arg));
      } else if (OB_SUCCESS != (tmp_ret = schema_service_->check_tenant_is_restore(NULL, tenant_id, is_restore))) {
        LOG_WARN("fail to check tenant is restore", KR(tmp_ret), K(tenant_id));
      } else if (is_restore) {
        restore_scheduler_.wakeup();
      }
    }
  }
  return ret;
}

int ObRootService::init_debug_database()
{
  const schema_create_func* creator_ptr_array[] = {core_table_schema_creators, sys_table_schema_creators, NULL};

  int ret = OB_SUCCESS;
  HEAP_VAR(char[OB_MAX_SQL_LENGTH], sql)
  {
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    }

    ObTableSchema table_schema;
    ObSqlString create_func_sql;
    ObSqlString del_sql;
    for (const schema_create_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
         ++creator_ptr_ptr) {
      for (const schema_create_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
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
          } else if (OB_SUCCESS != (temp_ret = sql_proxy_.write(create_func_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(create_func_sql), K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = create_func_sql.assign(
                                        "create function usec_to_time(u bigint(20)) "
                                        "returns timestamp deterministic begin return from_unixtime(u); end;"))) {
            LOG_WARN("create_func_sql assign failed", K(temp_ret));
          } else if (OB_SUCCESS != (temp_ret = sql_proxy_.write(create_func_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(create_func_sql), K(temp_ret));
          }

          memset(sql, 0, sizeof(sql));
          if (OB_FAIL(del_sql.assign_fmt("DROP table IF EXISTS %s", table_schema.get_table_name()))) {
            LOG_WARN("assign sql failed", K(ret));
          } else if (OB_FAIL(sql_proxy_.write(del_sql.ptr(), affected_rows))) {
            LOG_WARN("execute sql failed", K(ret));
          } else if (OB_FAIL(ObSchema2DDLSql::convert(table_schema, sql, sizeof(sql)))) {
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
  SpinWLockGuard rs_list_guard(broadcast_rs_list_lock_);

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("START_SERVICE: start do_restart");

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("START_SERVICE: not init", K(ret));
  } else if (!ObRootServiceRoleChecker::is_rootserver(static_cast<ObIPartPropertyGetter*>(GCTX.ob_service_))) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not master", K(ret));
  }

  // renew master rootservice, ignore error
  if (OB_SUCC(ret)) {
    int tmp_ret = rs_mgr_->renew_master_rootserver();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("START_SERVICE: renew master rootservice failed", K(tmp_ret));
    }
  }

  // fetch root partition info
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fetch_root_partition_info())) {
      LOG_WARN("START_SERVICE: fetch root partition info failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: fetch root partition info succeed", K(ret));
    }
  }

  // broadcast root server address, ignore error
  if (OB_SUCC(ret)) {
    int tmp_ret = update_rslist();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to update rslist but ignored", K(tmp_ret));
    }
  }

  // reload freeze_info_manager, then freeze_info_manager->recover_major_freeze.
  // !!! cannot change the sequence of reload and recover_major_freeze !!!
  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_manager_.load_frozen_status())) {
      LOG_WARN("START_SERVICE: freeze info manager reload failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: freeze info manager reload success");
    }
  }

  // set split_schema_version before refresh schema while restart RS
  if (OB_SUCC(ret)) {
    ObGlobalStatProxy proxy(sql_proxy_);
    int64_t split_schema_version = OB_INVALID_VERSION;
    int64_t split_schema_version_v2 = OB_INVALID_VERSION;
    if (OB_FAIL(proxy.get_split_schema_version(split_schema_version))) {
      LOG_WARN("fail to get split schema_version", K(ret), K(split_schema_version));
    } else if (OB_FAIL(proxy.get_split_schema_version_v2(split_schema_version_v2))) {
      LOG_WARN("fail to get split schema_version", K(ret), K(split_schema_version_v2));
    } else {
      (void)GCTX.set_split_schema_version(split_schema_version);
      (void)GCTX.set_split_schema_version_v2(split_schema_version_v2);
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: load split schema version success");
    }
  }

  if (OB_SUCC(ret)) {
    // standby cluster trigger load_refresh_schema_status by heartbeat.
    // due to switchover, primary cluster need to load schema_status too.
    ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret));
    } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
      LOG_WARN("fail to load refresh schema status", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: load schema status success");
    }
  }

  bool load_frozen_status = true;
  const bool refresh_server_need_retry = false;  // no need retry
  // try fast recover
  if (OB_SUCC(ret)) {
    int tmp_ret = refresh_server(load_frozen_status, refresh_server_need_retry);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("START_SERVICE: refresh server failed", K(tmp_ret), K(load_frozen_status));
    }
    tmp_ret = refresh_schema(load_frozen_status);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("START_SERVICE: refresh schema failed", K(tmp_ret), K(load_frozen_status));
    }
  }
  load_frozen_status = false;
  // refresh schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_schema(load_frozen_status))) {
      LOG_WARN("START_SERVICE: refresh schema failed", K(ret), K(load_frozen_status));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: refresh schema success", K(load_frozen_status));
    }
  }

  // refresh server manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_server(load_frozen_status, refresh_server_need_retry))) {
      LOG_WARN("START_SERVICE: refresh server failed", K(ret), K(load_frozen_status));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: refresh server success", K(load_frozen_status));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_manager_.reload())) {
      LOG_WARN("START_SERVICE: freeze info manager reload failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: freeze info manager reload success");
    }
  }
  // add other reload logic here
  if (OB_SUCC(ret)) {
    if (OB_FAIL(zone_manager_.reload())) {
      LOG_WARN("START_SERVICE: zone_manager_ reload failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: reload zone manager success", K(ret));
    }
  }

  // go on doing not finished major freeze
  if (OB_SUCC(ret)) {
    if (OB_FAIL(major_freeze_launcher_.start())) {
      LOG_WARN("START_SERVICE: major freeze launcher start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start to major freeze lancher success");
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(freeze_info_updater_.start())) {
      LOG_WARN("START_SERVICE: freeze info updater start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start to freeze info updater success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition_spliter_.start())) {
      LOG_WARN("START_SERVICE: partition spliter start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: partition spliter start success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(global_trans_version_mgr_.start())) {
      LOG_WARN("START_SERVICE: global_trans_version_mgr_ start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start global_trans_version_mgr_ success");
    }
  }

  // start timer tasks
  if (OB_SUCC(ret)) {
    if (OB_FAIL(start_timer_tasks())) {
      LOG_WARN("START_SERVICE: start timer tasks failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start timer task success");
    }
  }

  DEBUG_SYNC(BEFORE_UNIT_MANAGER_LOAD);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(unit_manager_.load())) {
      LOG_WARN("START_SERVICE: unit_manager_ load failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: load unit_manager success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_manager_.load())) {
      LOG_WARN("START_SERVICE: rs gts manager load failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: load rs_gts_manager success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_core_table_unit_id())) {
      LOG_WARN("START_SERVICE: set core table partition unit failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: set core table unit id success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(leader_coordinator_.start())) {
      LOG_WARN("START_SERVICE: leader coordinator start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start leader coordinator success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(daily_merge_scheduler_.start())) {
      LOG_WARN("START_SERVICE: daily merge scheduler start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start daily merge scheduler success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_scheduler_.start())) {
      LOG_WARN("START_SERVICE: restore scheduler start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start restore scheduler success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_history_recycler_.start())) {
      LOG_WARN("START_SERVICE: schema_history_recycler start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start schema_history_recycler success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rebalance_task_mgr_.start())) {
      LOG_WARN("START_SERVICE: rebalance task manager start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start rebalance task mgr succes");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root_balancer_.start())) {
      LOG_WARN("START_SERVICE: root balancer start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start root balance success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(empty_server_checker_.start())) {
      LOG_WARN("START_SERVICE: start empty server checker failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start empty server checker success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(thread_checker_.start())) {
      LOG_WARN("rs_monitor_check : START_SERVICE: start thread checker failed", KR(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("rs_monitor_check : START_SERVICE: start thread checker success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(submit_lost_replica_checker_task())) {
      LOG_WARN("START_SERVICE: lost replica checker start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start lost replica checker success");
    }
  }

  // broadcast root server address again, this task must be in the end part of do_restart,
  // because system may work properly without it.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_rslist())) {
      LOG_WARN("START_SERVICE: broadcast root address failed but ignored", K(ret));
      // it's ok ret be overwritten, update_rslist_task will retry until succeed
      if (OB_FAIL(submit_update_rslist_task(true))) {
        LOG_WARN("START_SERVICE: submit_update_rslist_task failed", K(ret));
      } else {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("START_SERVICE: submit_update_rslist_task succeed");
      }
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: broadcast root address succeed");
    }
  }
  if (OB_SUCC(ret)) {
    const ObPartitionKey pkey(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
    if (OB_FAIL(report_single_replica(pkey))) {
      LOG_WARN("START_SERVICE: report all_core_table replica failed, but ignore", K(ret));
      // it's ok ret be overwritten, report single all_core will retry until succeed
      if (OB_FAIL(submit_report_core_table_replica_task())) {
        LOG_WARN("START_SERVICE: submit all core table replica task failed", K(ret));
      } else {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("START_SERVICE: submit all core table replica task succeed");
      }
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: report all_core_table replica finish");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_task_mgr_.start())) {
      LOG_WARN("fail to start rs gts task mgr", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start rs gts task mgr succeed");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rs_gts_monitor_.start())) {
      LOG_WARN("fail to start rs gts monitor", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start rs gts monitor succeed");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(global_index_builder_.start())) {
      LOG_WARN("fail to start global index builder", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start global index builder succeed");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ttl_scheduler_.start())) {
      LOG_WARN("fail to start ttl scheduler", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start ttl scheduler succeed");
    }
  }

  if (OB_SUCC(ret)) {
    schema_split_executor_.start();
    upgrade_executor_.start();
    upgrade_storage_format_executor_.start();
    create_inner_schema_executor_.start();
    schema_revise_executor_.start();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_sequence_id())) {
      LOG_WARN("fail to init sequence id", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: init sequence id success", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_lease_service_.start_lease())) {
      LOG_WARN("START_SERVICE: backup_lease_service_ start failed", K(ret));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("START_SERVICE: start backup_lease_service_ success");
    }
  }

  if (OB_SUCC(ret)) {
    // destroy all containers, create sys tenant's containers again
    if (OB_FAIL(root_balancer_.destory_tenant_unit_array())) {
      LOG_WARN("START_SERVICE: fail to destory all tenant map", K(ret));
    } else if (OB_FAIL(root_balancer_.create_unit_replica_counter(OB_SYS_TENANT_ID))) {
      LOG_WARN("START_SERVICE: fail to create unit replica counter", K(ret));
    } else {
      LOG_INFO("START_SERVICE: create sys tenant unit replica container success", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rs_status_.set_rs_status(status::FULL_SERVICE))) {
    LOG_WARN("fail to set rs status", KR(ret));
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("START_SERVICE: set rs status success");
  }

  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("[NOTICE] START_SERVICE: full_service !!! start to work!!");
    ROOTSERVICE_EVENT_ADD("root_service", "full_rootservice", "result", ret, K_(self_addr));
    root_balancer_.set_active();
    root_minor_freeze_.start();
    root_inspection_.start();
    int64_t now = ObTimeUtility::current_time();
    core_meta_table_version_ = now;
    EVENT_SET(RS_START_SERVICE_TIME, now);
    // reset fail count for self checker and print log.
    reset_fail_count();
  } else {
    // increase fail count for self checker and print log.
    update_fail_count(ret);
  }

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("START_SERVICE: finish do_restart", K(ret));
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
    share::schema::ObSchemaGetterGuard& schema_guard, const obrpc::ObDDLArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;

  if (arg.is_need_check_based_schema_objects()) {
    for (int64_t i = 0; OB_SUCC(ret) && (i < arg.based_schema_object_infos_.count()); ++i) {
      const ObBasedSchemaObjectInfo& info = arg.based_schema_object_infos_.at(i);
      if (OB_FAIL(schema_guard.get_schema_version_v2(info.schema_type_, info.schema_id_, schema_version))) {
        LOG_WARN("failed to get_schema_version_v2", K(ret), K(info));
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
  } else if (OB_FAIL(trans.start(&sql_proxy_))) {
    LOG_WARN("trans start failed", K(ret));
  } else {
    ObGlobalStatProxy proxy(trans);
    ObSchemaService* schema_service = schema_service_->get_schema_service();
    int64_t rootservice_epoch = 0;
    int64_t schema_version = OB_INVALID_VERSION;
    ObRefreshSchemaInfo schema_info;
    // increase sequence_id can trigger every observer refresh schema while restart RS
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
  } else {
  }  // no more to do
  return ret;
}

int ObRootService::start_timer_tasks()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  if (OB_SUCCESS == ret && !task_queue_.exist_timer_task(event_table_clear_task_)) {
    const int64_t delay = ObEventHistoryTableOperator::EVENT_TABLE_CLEAR_INTERVAL;
    if (OB_FAIL(task_queue_.add_repeat_timer_task_schedule_immediately(event_table_clear_task_, delay))) {
      LOG_WARN("start event table clear task failed", K(delay), K(ret));
    } else {
      LOG_INFO("added event_table_clear_task");
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(update_rs_list_timer_task_)) {
    if (OB_FAIL(schedule_update_rs_list_task())) {
      LOG_WARN("failed to schedule update rs list task", K(ret));
    } else {
      LOG_INFO("add update rs list timer task");
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(inner_table_monitor_task_)) {
    // remove purge inner table task, we may fail to get history schema after purge.
    // if (OB_FAIL(schedule_inner_table_monitor_task())) {
    //   LOG_WARN("start inner table monitor service fail", K(ret));
    // } else {
    //  LOG_INFO("start inner table monitor success");
    // }
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
    if (OB_FAIL(schedule_load_building_index_task())) {
      LOG_WARN("fail to schedule load building index task", K(ret));
    }
  }

  if (OB_SUCC(ret) && !task_queue_.exist_timer_task(reload_unit_replica_counter_task_)) {
    if (OB_FAIL(reload_unit_replica_counter_task())) {
      LOG_WARN("fail to reload unit replica counter task", K(ret));
    } else {
      LOG_INFO("start reload unit replica success");
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
    task_queue_.cancel_timer_task(inner_table_monitor_task_);
    task_queue_.cancel_timer_task(self_check_task_);
    task_queue_.cancel_timer_task(update_rs_list_timer_task_);
    inspect_task_queue_.cancel_timer_task(inspector_task_);
    inspect_task_queue_.cancel_timer_task(purge_recyclebin_task_);
    inspect_task_queue_.cancel_timer_task(force_drop_schema_task_);
  }

  // stop other timer tasks here
  LOG_INFO("stop all timer tasks finish", K(ret));
  return ret;
}

int ObRootService::fetch_root_partition_info()
{
  int ret = OB_SUCCESS;
  ObPartitionReplica replica;
  ObSEArray<ObAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> member_list;
  ObIPartPropertyGetter* prop_getter = pt_operator_->get_prop_getter();
  const ObPartitionKey part_key(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == prop_getter) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null partition property getter", K(ret));
  } else if (OB_FAIL(prop_getter->get_leader_member(part_key, member_list))) {
    LOG_WARN("get __all_core_table member list failed", K(part_key), K(ret));
  } else {
    LOG_INFO("got __all_core_table member list", K(member_list));
    if (OB_FAIL(rpc_proxy_.to(self_addr_).by(OB_LOC_CORE_TENANT_ID).fetch_root_partition(replica))) {
      LOG_WARN("fetch root partition failed", K(ret), K_(self_addr));
    } else if (OB_FAIL(pt_operator_->update(replica))) {
      LOG_WARN("update root partition info failed", K(ret), K(replica));
    } else {
      LOG_INFO("update __all_core_table replica succeed", K(replica), "server", self_addr_);
    }
  }

  FOREACH_X(addr, member_list, OB_SUCCESS == ret)
  {
    if (self_addr_ == *addr) {
      continue;
    }

    replica.reset();
    int temp_ret = OB_SUCCESS;

    if (OB_SUCCESS != (temp_ret = rpc_proxy_.to(*addr)
                                      .timeout(config_->rpc_timeout)
                                      .by(OB_LOC_CORE_TENANT_ID)
                                      .fetch_root_partition(replica))) {
      LOG_WARN("fetch root partition failed", K(temp_ret), "server", *addr);
    } else if (OB_FAIL(pt_operator_->update(replica))) {
      LOG_WARN("update root partition info failed", K(ret), K(replica));
    } else {
      LOG_INFO("update __all_core_table replica succeed", K(replica), "server", *addr);
    }
  }

  return ret;
}

int ObRootService::set_core_table_unit_id()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo core_partition;
  core_partition.set_allocator(&allocator);
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  const int64_t partition_id = ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObArray<ObUnitInfo> unit_infos;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema* sys_tenant = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_operator_->get(table_id, partition_id, core_partition))) {
    LOG_WARN("get core table partition failed", KT(table_id), K(partition_id), K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, sys_tenant))) {
    LOG_WARN("fail to get tenant schema", K(ret));
  } else if (OB_UNLIKELY(NULL == sys_tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tenant schema is null", K(ret));
  } else if (OB_FAIL(unit_manager_.get_active_unit_infos_by_tenant(*sys_tenant, unit_infos))) {
    LOG_WARN("get_unit_infos_by_tenant failed", "sys_tenant", *sys_tenant, K(ret));
  } else {
    FOREACH_CNT_X(r, core_partition.get_replicas_v2(), OB_SUCCESS == ret)
    {
      FOREACH_CNT_X(unit_info, unit_infos, OB_SUCCESS == ret)
      {
        if (r->server_ == unit_info->unit_.server_) {
          if (OB_FAIL(pt_operator_->set_unit_id(
                  core_partition.get_table_id(), partition_id, r->server_, unit_info->unit_.unit_id_))) {
            LOG_WARN("set core table replica unit failed", K(ret), "core_table_replica", *r, "unit_info", *unit_info);
          } else {
            LOG_INFO("set core table replica unit succeed", K(ret), "core_table_replica", *r, "unit_info", *unit_info);
          }
          break;
        }
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

ObRootService::ObRestartTask::ObRestartTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(0);  // don't retry when failed
}

ObRootService::ObRestartTask::~ObRestartTask()
{}

int ObRootService::ObRestartTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.after_restart())) {
    LOG_WARN("START_SERVICE: root service after restart failed", K(ret));
  } else {
    LOG_INFO("START_SERVICE: execute after_restart task success");
  }
  return ret;
}

ObAsyncTask* ObRootService::ObRestartTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObRestartTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObRestartTask(root_service_);
  }
  return task;
}

ObRootService::ObRefreshServerTask::ObRefreshServerTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(0);  // don't retry when process failed
}

int ObRootService::ObRefreshServerTask::process()
{
  int ret = OB_SUCCESS;
  const bool load_frozen_status = true;
  const bool need_retry = true;
  ObLatchRGuard guard(root_service_.bootstrap_lock_, ObLatchIds::RS_BOOTSTRAP_LOCK);
  if (OB_FAIL(root_service_.refresh_server(load_frozen_status, need_retry))) {
    LOG_WARN("START_SERVICE: refresh server failed", K(ret), K(load_frozen_status));
  } else {
  }
  return ret;
}

ObAsyncTask* ObRootService::ObRefreshServerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObRefreshServerTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObRefreshServerTask(root_service_);
  }
  return task;
}

//-----Functions for managing privileges------
int ObRootService::create_user(obrpc::ObCreateUserArg& arg, common::ObSArray<int64_t>& failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.create_user(arg, failed_index))) {
    LOG_WARN("create user failed", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::drop_user(const ObDropUserArg& arg, common::ObSArray<int64_t>& failed_index)
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

int ObRootService::rename_user(const obrpc::ObRenameUserArg& arg, common::ObSArray<int64_t>& failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.rename_user(arg, failed_index))) {
    LOG_WARN("rename user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::set_passwd(const obrpc::ObSetPasswdArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.set_passwd(arg))) {
    LOG_WARN("set passwd failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::grant(const ObGrantArg& arg)
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

int ObRootService::revoke_user(const ObRevokeUserArg& arg)
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

int ObRootService::lock_user(const ObLockUserArg& arg, ObSArray<int64_t>& failed_index)
{
  int ret = OB_SUCCESS;
  failed_index.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.lock_user(arg, failed_index))) {
    LOG_WARN("lock user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::alter_user_profile(const ObAlterUserProfileArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.alter_user_profile(arg))) {
    LOG_WARN("lock user failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::revoke_database(const ObRevokeDBArg& arg)
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

int ObRootService::revoke_table(const ObRevokeTableArg& arg)
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
    ObObjPrivSortKey obj_priv_key(
        arg.tenant_id_, arg.obj_id_, arg.obj_type_, COL_ID_FOR_TAB_PRIV, arg.grantor_id_, arg.user_id_);
    OZ(ddl_service_.revoke_table(
        table_priv_key, arg.priv_set_, obj_priv_key, arg.obj_priv_array_, arg.revoke_all_ora_));
  }
  return ret;
}

int ObRootService::revoke_syspriv(const ObRevokeSysPrivArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.revoke_syspriv(
                 arg.tenant_id_, arg.grantee_id_, arg.sys_priv_array_, &arg.ddl_stmt_str_))) {
    LOG_WARN("revoke privilege failed", K(ret), K(arg));
  }
  return ret;
}

//-----End of functions for managing privileges-----

//-----Functions for managing outlines-----
int ObRootService::create_outline(const ObCreateOutlineArg& arg)
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
    const ObDatabaseSchema* db_schema = NULL;
    if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard in inner table failed", K(ret));
    } else if (database_name == OB_OUTLINE_DEFAULT_DATABASE_NAME) {
      // if not specify database, set default database name and database id;
      outline_info.set_database_id(combine_id(tenant_id, OB_OUTLINE_DEFAULT_DATABASE_ID));
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
      if (OB_FAIL(ddl_service_.create_outline(outline_info, is_update, &arg.ddl_stmt_str_))) {
        LOG_WARN("create_outline failed", K(outline_info), K(is_update), K(ret));
      }
    }
  }
  return ret;
}

int ObRootService::create_user_defined_function(const obrpc::ObCreateUserDefinedFunctionArg& arg)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObUDF udf_info_ = arg.udf_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.check_udf_exist(arg.udf_.get_tenant_id(), arg.udf_.get_name_str(), exist))) {
    LOG_WARN("failed to check_udf_exist", K(arg.udf_.get_tenant_id()), K(arg.udf_.get_name_str()), K(exist), K(ret));
  } else if (exist) {
    ret = OB_UDF_EXISTS;
    LOG_USER_ERROR(OB_UDF_EXISTS, arg.udf_.get_name_str().length(), arg.udf_.get_name_str().ptr());
  } else if (OB_FAIL(ddl_service_.create_user_defined_function(udf_info_, arg.ddl_stmt_str_))) {
    LOG_WARN("failed to create udf", K(arg), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObRootService::drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg& arg)
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
  } else { /*do nothing*/
  }

  return ret;
}

bool ObRootService::is_sys_tenant(const ObString& tenant_name)
{
  return (0 == tenant_name.case_compare(OB_SYS_TENANT_NAME) || 0 == tenant_name.case_compare(OB_MONITOR_TENANT_NAME) ||
          0 == tenant_name.case_compare(OB_DIAG_TENANT_NAME) || 0 == tenant_name.case_compare(OB_GTS_TENANT_NAME));
}

int ObRootService::alter_outline(const ObAlterOutlineArg& arg)
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
  } else { /*do nothing*/
  }
  return ret;
}

int ObRootService::drop_outline(const obrpc::ObDropOutlineArg& arg)
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

//----Functions for managing dblinks----

int ObRootService::create_dblink(const obrpc::ObCreateDbLinkArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_service_.create_dblink(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("create_dblink failed", K(arg.dblink_info_), K(ret));
  }
  return ret;
}

int ObRootService::drop_dblink(const obrpc::ObDropDbLinkArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_service_.drop_dblink(arg, &arg.ddl_stmt_str_))) {
    LOG_WARN("drop_dblink failed", K(arg.tenant_id_), K(arg.dblink_name_), K(ret));
  }
  return ret;
}

//----End of functions for managing dblinks----

//-----Functions for managing synonyms-----
int ObRootService::create_synonym(const ObCreateSynonymArg& arg)
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
    const ObDatabaseSchema* db_schema = NULL;
    const ObDatabaseSchema* obj_db_schema = NULL;
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
    } else if (OB_UNLIKELY(db_schema->is_in_recyclebin()) || OB_UNLIKELY(obj_db_schema->is_in_recyclebin())) {
      ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
      LOG_WARN("can't create synonym of db in recyclebin", K(arg), KPC(db_schema), KPC(obj_db_schema), K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == db_schema->get_database_id()) ||
               OB_UNLIKELY(OB_INVALID_ID == obj_db_schema->get_database_id())) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("database id is invalid", K(tenant_id), KPC(db_schema), KPC(obj_db_schema), K(ret));
    } else {
      synonym_info.set_database_id(db_schema->get_database_id());
      synonym_info.set_object_database_id(obj_db_schema->get_database_id());
    }
    bool is_update = false;
    if (OB_SUCC(ret)) {
      ObArray<ObSchemaType> conflict_schema_types;
      if (OB_FAIL(schema_guard.check_oracle_object_exist(tenant_id,
              synonym_info.get_database_id(),
              synonym_info.get_synonym_name_str(),
              SYNONYM_SCHEMA,
              arg.or_replace_,
              conflict_schema_types))) {
        LOG_WARN("fail to check oracle_object exist", K(ret), K(synonym_info));
      } else if (conflict_schema_types.count() > 0) {
        ret = OB_ERR_EXIST_OBJECT;
        LOG_WARN("Name is already used by an existing object", K(ret), K(synonym_info), K(conflict_schema_types));
      }
    }
    if (OB_SUCC(ret)) {
      // can not delete, it will update synonym_info between check_synonym_exist.
      if (OB_FAIL(ddl_service_.check_synonym_exist(synonym_info, arg.or_replace_, is_update))) {
        LOG_WARN("failed to check_synonym_exist", K(synonym_info), K(arg.or_replace_), K(is_update), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.create_synonym(synonym_info, &arg.ddl_stmt_str_, is_update))) {
        LOG_WARN("create_synonym failed", K(synonym_info), K(ret));
      }
    }
  }
  return ret;
}

int ObRootService::drop_synonym(const obrpc::ObDropSynonymArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(ddl_service_.drop_synonym(arg))) {
      LOG_WARN("ddl service failed to drop synonym", K(arg), K(ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// sequence
////////////////////////////////////////////////////////////////
int ObRootService::do_sequence_ddl(const obrpc::ObSequenceDDLArg& arg)
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
// server & zone management
////////////////////////////////////////////////////////////////
int ObRootService::add_server(const obrpc::ObAdminServerArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObCheckServerEmptyArg new_arg;
    new_arg.mode_ = ObCheckServerEmptyArg::ADD_SERVER;
    ObCheckDeploymentModeArg dp_arg;
    dp_arg.single_zone_deployment_on_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.servers_.count(); ++i) {
      const ObAddr& addr = arg.servers_[i];
      Bool is_empty(false);
      Bool is_deployment_mode_match(false);
      if (OB_FAIL(rpc_proxy_.to(addr).is_empty_server(new_arg, is_empty))) {
        LOG_WARN("fail to check is server empty", K(ret), K(addr));
      } else if (!is_empty) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("add non-empty server not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add non-empty server");
      } else if (rpc_proxy_.to(addr).check_deployment_mode_match(dp_arg, is_deployment_mode_match)) {
        LOG_WARN("fail to check deployment mode match", K(ret));
      } else if (!is_deployment_mode_match) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("add server deployment mode mot match not allowed", K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add server deployment mode not match");
      } else if (OB_FAIL(server_manager_.add_server(addr, arg.zone_))) {
        LOG_WARN("add_server failed", "server", addr, "zone", arg.zone_, K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "add_server", K(ret), K(arg));
  return ret;
}

int ObRootService::delete_server(const obrpc::ObAdminServerArg& arg)
{
  int ret = OB_SUCCESS;
  bool has_enough = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(check_has_enough_member(arg.servers_, has_enough))) {
    LOG_WARN("fail to check has enough member", K(ret), K(arg));
  } else if (!has_enough) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not enough member, cannot delete servers", K(ret), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not enough member or quorum mismatch, delete servers");
  } else if (OB_FAIL(check_server_have_enough_resource_for_delete_server(arg.servers_, arg.zone_))) {
    LOG_WARN("not enough resource, cannot delete servers", K(ret), K(arg));
  } else if (OB_FAIL(server_manager_.delete_server(arg.servers_, arg.zone_))) {
    LOG_WARN("delete_server failed", "servers", arg.servers_, "zone", arg.zone_, K(ret));
  } else {
    root_balancer_.wakeup();
  }
  ROOTSERVICE_EVENT_ADD("root_service", "delete_server", K(ret), K(arg));
  return ret;
}

int ObRootService::check_server_have_enough_resource_for_delete_server(
    const ObIArray<ObAddr>& servers, const ObZone& zone)
{
  int ret = OB_SUCCESS;
  common::ObZone tmp_zone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(server, servers, OB_SUCC(ret))
    {
      if (zone.is_empty()) {
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
        if (OB_FAIL(unit_manager_.check_enough_resource_for_delete_server(*server, tmp_zone))) {
          LOG_WARN("fail to check enouch resource", K(ret));
        }
      }
    }  // end for each
  }
  return ret;
}

int ObRootService::cancel_delete_server(const obrpc::ObAdminServerArg& arg)
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
      const bool force_stop_hb = false;
      int tmp_ret = OB_SUCCESS;
      // resume heardbeat
      if (OB_FAIL(server_manager_.set_force_stop_hb(arg.servers_[i], force_stop_hb))) {
        LOG_WARN("set force stop hb failed", K(ret), "server", arg.servers_[i], K(force_stop_hb));
      } else if (OB_SUCCESS != (tmp_ret = request_heartbeats())) {
        LOG_WARN("request heartbeats failed", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(server_manager_.end_delete_server(arg.servers_[i], arg.zone_, commit))) {
        LOG_WARN("delete_server failed", "server", arg.servers_[i], "zone", arg.zone_, K(commit), K(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        ObSEArray<uint64_t, 1> tenant_ids;
        ObAdminClearBalanceTaskArg::TaskType type = ObAdminClearBalanceTaskArg::ALL;
        if (OB_SUCCESS != (tmp_ret = rebalance_task_mgr_.clear_task(tenant_ids, type))) {
          LOG_WARN("fail to clear task after cancel delete server", K(tmp_ret));
        } else {
          LOG_INFO("clear balance task after cancel delete server", K(arg));
        }
        root_balancer_.wakeup();
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "cancel_delete_server", K(ret), K(arg));
  return ret;
}

int ObRootService::start_server(const obrpc::ObAdminServerArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(server_manager_.start_server_list(arg.servers_, arg.zone_))) {
    LOG_WARN("start servers failed", "server", arg.servers_, "zone", arg.zone_, K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "start_server", K(ret), K(arg));
  return ret;
}

int ObRootService::get_readwrite_servers(
    const common::ObIArray<common::ObAddr>& input_servers, common::ObIArray<common::ObAddr>& readwrite_servers)
{
  int ret = OB_SUCCESS;
  readwrite_servers.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < input_servers.count(); ++i) {
    const ObAddr& server = input_servers.at(i);
    HEAP_VAR(ObZoneInfo, zone_info)
    {
      if (OB_FAIL(server_manager_.get_server_zone(server, zone_info.zone_))) {
        LOG_WARN("fail to get server zone", K(ret));
      } else if (OB_FAIL(zone_manager_.get_zone(zone_info))) {
        LOG_WARN("fail to get zone", K(ret));
      } else {
        ObZoneType zone_type = static_cast<ObZoneType>(zone_info.zone_type_.value_);
        if (common::ZONE_TYPE_READWRITE == zone_type) {
          if (OB_FAIL(readwrite_servers.push_back(server))) {
            LOG_WARN("fail to push back", K(ret));
          } else {
          }  // no more to do
        } else if (common::ZONE_TYPE_READONLY == zone_type) {
          // ignore read-only zone
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid zone type", K(ret), K(zone_type), K(server), "zone", zone_info.zone_);
        }
      }
    }
  }
  return ret;
}

int ObRootService::check_zone_and_server(const ObIArray<ObAddr>& servers, bool& is_same_zone, bool& is_all_stopped)
{
  int ret = OB_SUCCESS;
  ObZone zone;
  is_same_zone = true;
  is_all_stopped = true;
  ObServerStatus server_status;
  for (int64_t i = 0; i < servers.count() && OB_SUCC(ret) && is_same_zone && is_all_stopped; i++) {
    if (OB_FAIL(server_manager_.get_server_status(servers.at(i), server_status))) {
      LOG_WARN("fail to get server zone", K(ret), K(servers), K(i));
    } else if (i == 0) {
      zone = server_status.zone_;
    } else if (zone != server_status.zone_) {
      is_same_zone = false;
      LOG_WARN("server zone not same", K(zone), K(server_status), K(servers));
    }
    if (OB_FAIL(ret)) {
    } else if (server_status.is_stopped()) {
      // nothing todo
    } else {
      is_all_stopped = false;
    }
  }
  return ret;
}

int ObRootService::stop_server(const obrpc::ObAdminServerArg& arg)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> readwrite_servers;
  ObZone zone;
  bool is_same_zone = false;
  bool is_all_stopped = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(get_readwrite_servers(arg.servers_, readwrite_servers))) {
    LOG_WARN("fail to get readwrite servers", K(ret));
  } else if (readwrite_servers.count() <= 0) {
    // no need to do check, stop servers directly
  } else if (OB_FAIL(check_zone_and_server(arg.servers_, is_same_zone, is_all_stopped))) {
    LOG_WARN("fail to check stop server zone", K(ret), K(arg.servers_));
  } else if (is_all_stopped) {
    // nothing todo
  } else if (!is_same_zone) {
    ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
    LOG_WARN("can not stop servers in multiple zones", K(ret));
  } else if (OB_FAIL(server_manager_.get_server_zone(readwrite_servers.at(0), zone))) {
    LOG_WARN("fail to get server zone", K(ret), K(readwrite_servers));
  } else {
    if (ObAdminServerArg::ISOLATE == arg.op_) {
      //"Isolate server" does not need to check the total number and status of replicas; it cannot be restarted later;
      if (OB_FAIL(check_can_stop(zone, arg.servers_, false /*is_stop_zone*/))) {
        LOG_WARN("fail to check can stop", KR(ret), K(zone), K(arg));
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Stop all server in primary region is disabled");
        }
      }
    } else {
      bool has_enough_member = false;
      bool is_log_sync = false;
      if (have_other_stop_task(zone)) {
        ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
        LOG_WARN("can not stop servers in multiple zones", KR(ret), K(arg), K(zone));
        LOG_USER_ERROR(OB_STOP_SERVER_IN_MULTIPLE_ZONES, "cannot stop server or stop zone in multiple zones");
      } else if (OB_FAIL(check_has_enough_member(readwrite_servers, has_enough_member))) {
        LOG_WARN("check has enough member failed", K(ret));
      } else if (!has_enough_member) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("not enough member, cannot stop servers", K(ret), K(arg));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not enough member or quorum mismatch, stop servers");
      } else if (arg.force_stop_) {
        // nothing todo
        // force stop no need to check log sync
      } else if (OB_FAIL(check_is_log_sync_twice(is_log_sync, readwrite_servers))) {
        LOG_WARN("check is log sync failed", K(arg), K(ret));
      } else if (!is_log_sync) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("log is not in-sync, cannot stop servers", K(ret), K(arg));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log is not in-sync, stop servers");
      } else {
      }  // good
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_manager_.stop_server_list(arg.servers_, arg.zone_))) {
        LOG_WARN("stop server failed", "server", arg.servers_, "zone", arg.zone_, K(ret));
      } else {
        LOG_INFO("stop server ok", K(arg));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "stop_server", K(ret), K(arg));
  return ret;
}

int ObRootService::check_gts_replica_enough_when_stop_server(
    const obrpc::ObCheckGtsReplicaStopServer& server_need_stopped, obrpc::Bool& can_stop)
{
  int ret = OB_SUCCESS;
  bool my_can_stop = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_gts_monitor_.check_gts_replica_enough_when_stop_server(
                 server_need_stopped.servers_, my_can_stop))) {
    LOG_WARN("fail to check gts replica enough when stop server", K(ret), K(server_need_stopped));
  } else {
    can_stop = my_can_stop;
  }
  return ret;
}

int ObRootService::check_gts_replica_enough_when_stop_zone(
    const obrpc::ObCheckGtsReplicaStopZone& zone_need_stopped, obrpc::Bool& can_stop)
{
  int ret = OB_SUCCESS;
  bool my_can_stop = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(rs_gts_monitor_.check_gts_replica_enough_when_stop_zone(zone_need_stopped.zone_, my_can_stop))) {
    LOG_WARN("fail to check gts replica enough when stop zone", K(ret), K(zone_need_stopped));
  } else {
    can_stop = my_can_stop;
  }
  return ret;
}

int ObRootService::add_zone(const obrpc::ObAdminZoneArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(zone_manager_.add_zone(arg.zone_, arg.region_, arg.idc_, arg.zone_type_))) {
    LOG_WARN("failed to add zone", K(ret), K(arg));
  } else {
    LOG_INFO("add zone ok", K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "add_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  return ret;
}

int ObRootService::delete_zone(const obrpc::ObAdminZoneArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    // @note to avoid deadlock risk, put it beside ZoneManager::write_lock_. ObServerManager::add_server also call the
    // interfaces of zone_mgr. it does not matter while add server after check.
    int64_t alive_count = 0;
    int64_t not_alive_count = 0;
    if (OB_FAIL(server_manager_.get_server_count(arg.zone_, alive_count, not_alive_count))) {
      LOG_WARN("failed to get server count of the zone", K(ret), "zone", arg.zone_);
    } else {
      LOG_INFO("current server count of zone", "zone", arg.zone_, K(alive_count), K(not_alive_count));
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

int ObRootService::start_zone(const obrpc::ObAdminZoneArg& arg)
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

// check whether has a object that all primary_zones were stopped after this time stop.
// basic idea:
// 1. get the intersection tenant of to_stop_list and stopped_server_list. if tenant is only on stopp_list, it is no
// matter with this time stop.
//   therefor, only need to consider the intersection of two sets;
// 2. set the union of to_stop_list and stopped_list to zone_list. set the intersection tenant of to_stop_list and
// stopped_server_list to tenant_ids,
//   check whether the primary zone of every tenant in tenant_ids is the subset of zone_list. if it is the subset, then
//   the leader of the object may switch to non_primary_zone observer, this situation is not allowed; if it is not the
//   subset, than still has replica in primary zone can be leader. normally, it need to iter all schemas of the tenant,
//   here are something optimizations:
//     2.1 OB_SV__PRIMARY_ZONE_ENTITY_COUNT = 0, indicate that only tenant has primary zone, only check tenant.
//     2.2 if tenant's timeservice is GTS, tenant and all schemas of tenant' primary_zone must be in same region. only
//     check tenant. 2.3 other, iter all schemas of tenant, do check if has primary zone.
int ObRootService::check_can_stop(const ObZone& zone, const ObIArray<ObAddr>& servers, const bool is_stop_zone)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObArray<ObAddr> to_stop_list;
  ObArray<ObZone> stopped_zone_list;
  ObArray<ObAddr> stopped_server_list;

  if ((!is_stop_zone && (0 == servers.count() || zone.is_empty())) || (is_stop_zone && zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(servers), K(zone));
  } else if (OB_FAIL(ObRootUtils::get_stopped_zone_list(
                 zone_manager_, server_manager_, stopped_zone_list, stopped_server_list))) {
    LOG_WARN("fail to get stopped zone list", KR(ret));
  } else if (0 >= stopped_server_list.count()) {
    // nothing todo
  } else {
    if (!is_stop_zone) {
      if (OB_FAIL(to_stop_list.assign(servers))) {
        LOG_WARN("fail to push back", KR(ret), K(servers));
      }
    } else if (OB_FAIL(server_manager_.get_servers_of_zone(zone, to_stop_list))) {
      LOG_WARN("fail to get servers of zone", KR(ret), K(zone));
    }
    ObArray<uint64_t> tenant_ids;
    bool is_in = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRootUtils::get_tenant_intersection(
                   unit_manager_,  // get intersection tenant of to_stop_list and stopped_server_list
                   to_stop_list,
                   stopped_server_list,
                   tenant_ids))) {
      LOG_WARN("fail to get tenant intersections", KR(ret));
    } else if (!has_exist_in_array(stopped_zone_list, zone) && OB_FAIL(stopped_zone_list.push_back(zone))) {
      LOG_WARN("fail to push back", KR(ret), K(zone));
    } else if (OB_FAIL(ObRootUtils::check_primary_region_in_zonelist(
                   schema_service_,  // check whether stop primary region of the tenant
                   &ddl_service_,
                   unit_manager_,
                   zone_manager_,
                   tenant_ids,
                   stopped_zone_list,
                   is_in))) {
      LOG_WARN("fail to check tenant stop primary region", KR(ret));
    } else if (!is_in) {
      // nothing todo
    } else {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("stop all primary region is not allowed", KR(ret), K(zone), K(servers));
    }
  }
  int64_t cost = ObTimeUtility::current_time() - start_time;
  LOG_INFO("check can stop zone/server", KR(ret), K(zone), K(servers), K(cost));
  return ret;
}

// Multiple stop tasks are allowed on a zone; they cannot cross zones;
bool ObRootService::have_other_stop_task(const ObZone& zone)
{
  bool bret = false;
  int64_t ret = OB_SUCCESS;
  ObArray<ObZoneInfo> zone_infos;
  bool stopped = false;
  // Check if there are other servers in the stopped state on other zones
  if (OB_FAIL(server_manager_.check_other_zone_stopped(zone, stopped))) {
    LOG_WARN("fail to check other zone stopped", KR(ret), K(zone));
    bret = true;
  } else if (stopped) {
    bret = true;
    LOG_WARN("have other server stop in other zone", K(bret), K(zone));
  } else if (OB_FAIL(zone_manager_.get_zone(zone_infos))) {
    LOG_WARN("fail to get zone", KR(ret), K(zone));
    bret = true;
  } else {
    // Check whether other zones are in the stopped state
    FOREACH_CNT_X(zone_info, zone_infos, OB_SUCC(ret) && !bret)
    {
      if (OB_ISNULL(zone_info)) {
        ret = OB_ERR_UNEXPECTED;
        bret = true;
        LOG_WARN("zone info is null", KR(ret), K(zone_infos));
      } else if (zone_info->status_ != ObZoneStatus::ACTIVE && zone != zone_info->zone_) {
        bret = true;
        LOG_WARN("have other zone in inactive status", K(bret), K(zone), "other_zone", zone_info->zone_);
      }
    }
  }
  return bret;
}

int ObRootService::stop_zone(const obrpc::ObAdminZoneArg& arg)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> empty_server;
  HEAP_VAR(ObZoneInfo, zone_info)
  {
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
      // nothing todo
    } else if (ObAdminZoneArg::ISOLATE == arg.op_) {
      if (OB_FAIL(check_can_stop(arg.zone_, empty_server, true /*is_stop_zone*/))) {
        LOG_WARN("fail to check zone can stop", KR(ret), K(arg));
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Stop all server in primary region is disabled");
        }
      }
    } else {
      // stop zone/force stop zone
      if (have_other_stop_task(arg.zone_)) {
        ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
        LOG_WARN("cannot stop zone when other stop task already exist", KR(ret), K(arg));
        LOG_USER_ERROR(OB_STOP_SERVER_IN_MULTIPLE_ZONES, "cannot stop server or stop zone in multiple zones");
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
      } else if (common::ZONE_TYPE_READWRITE == zone_type) {
        bool has_enough_member = false;
        bool is_log_sync = false;
        ObArray<ObAddr> server_list;
        if (OB_FAIL(server_manager_.get_servers_of_zone(arg.zone_, server_list))) {
          LOG_WARN("get servers of zone failed", K(ret), "zone", arg.zone_);
        } else if (server_list.count() <= 0) {
          // do not need to check anyting while zone is empty
        } else if (OB_FAIL(check_has_enough_member(server_list, has_enough_member))) {
          LOG_WARN("check has enough member failed", K(ret));
        } else if (!has_enough_member) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("not enough member, cannot stop zone", K(ret), K(arg));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "not enough member or quorum mismatch, stop zone");
        } else if (arg.force_stop_) {
          // skip check log is sync while force stop
        } else if (OB_FAIL(check_is_log_sync_twice(is_log_sync, server_list))) {
          LOG_WARN("check is log sync failed", K(arg), K(ret));
        } else if (!is_log_sync) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("log is not sync, cannot stop zone", K(ret), K(arg));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "log is not sync, cannot stop zone");
        } else {
        }  // good
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone type", K(ret), "zone", arg.zone_, K(zone_type));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_manager_.stop_zone(arg.zone_))) {
          LOG_WARN("stop zone failed", K(arg), K(ret));
        } else {
          LOG_INFO("stop zone ok", K(arg));
        }
      } else {
        // set other error code to 4179
      }
    }
    ROOTSERVICE_EVENT_ADD("root_service", "stop_zone", K(ret), "sql_text", ObHexEscapeSqlStr(arg.sql_stmt_str_));
  }
  return ret;
}


int ObRootService::alter_zone(const obrpc::ObAdminZoneArg& arg)
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

int ObRootService::check_has_enough_member(const ObIArray<ObAddr>& server_list, bool& has_enough_member)
{
  int ret = OB_SUCCESS;
  has_enough_member = true;
  ObSchemaGetterGuard schema_guard;
  ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
  ObArray<ObAddr> invalid_server_list;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(ObRootUtils::get_invalid_server_list(zone_manager_, server_manager_, invalid_server_list))) {
    LOG_WARN("fail to get stopped server list", KR(ret));
  } else if (FALSE_IT(cluster_type = ObClusterInfoGetter::get_cluster_type_v2())) {
    // nothing todo
  } else {
    bool is_standby = PRIMARY_CLUSTER != cluster_type;
    ObPartitionTableIterator iter;
    bool ignore_row_checksum = true;
    if (OB_FAIL(iter.init(*pt_operator_, *schema_service_, ignore_row_checksum))) {
      LOG_WARN("partition table iterator init failed", K(ret));
    } else if (OB_FAIL(iter.get_filters().set_replica_status(REPLICA_STATUS_NORMAL))) {
      LOG_WARN("set filter failed", K(ret));
    } else if (OB_FAIL(
                   iter.get_filters().filter_delete_server(server_manager_))) {  // filter replicas on deleted observer
      LOG_WARN("set filter failed", K(ret));
    } else if (OB_FAIL(iter.get_filters().filter_restore_replica())) {  // filter replicas in restore status.
      LOG_WARN("set filter failed", K(ret));
    } else if (invalid_server_list.count() > 0 &&
               OB_FAIL(iter.get_filters().filter_invalid_server(invalid_server_list))) {
      LOG_WARN("fail to filter stopped server", KR(ret));
    } else if (OB_FAIL(iter.get_filters().set_in_member_list())) {
      LOG_WARN("set filter failed", K(ret));
    } else {
      ObPartitionInfo partition;
      uint64_t last_id = 0;
      int64_t paxos_replica_num = 0;
      const ObTenantSchema* tenant_schema = NULL;
      const ObSimpleTableSchemaV2* table_schema = NULL;
      const ObTablegroupSchema* tablegroup_schema = NULL;
      bool need_have_leader = true;
      bool table_exist = true;
      const bool single_zone_deployment_on = false;
      while (OB_SUCC(ret) && has_enough_member) {
        partition.reuse();
        if (!in_service()) {
          ret = OB_CANCELED;
          LOG_WARN("rs is not in service", K(ret));
        } else if (OB_FAIL(iter.next(partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("iterate partition table failed", K(ret), K(iter));
          }
          break;
        } else if (last_id != partition.get_table_id()) {
          // previous table iter end, calculate paxos_num again.
          last_id = partition.get_table_id();
          table_exist = true;
          need_have_leader =
              ObMultiClusterUtil::need_create_partition(
                  partition.get_tenant_id(), partition.get_table_id(), true /*has_partition*/, is_standby) &&
              !partition.in_physical_restore();
          if (!need_have_leader) {
            // non_private table's paxos_num equal to tenant_schema's paxos_num
            if (OB_FAIL(schema_guard.get_tenant_info(partition.get_tenant_id(), tenant_schema))) {
              LOG_WARN("fail to get tenant schema", KR(ret), K(partition));
            } else if (OB_ISNULL(tenant_schema)) {
              table_exist = false;
              LOG_INFO("tenant not exist", K(ret), K(partition));
            } else if (OB_FAIL(tenant_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
              LOG_WARN("failed to get paxos replica num", K(ret), K(partition));
            }
          } else if (is_new_tablegroup_id(last_id)) {
            // pg
            if (OB_FAIL(schema_guard.get_tablegroup_schema(last_id, tablegroup_schema))) {
              LOG_WARN("failed to get tablegroup schema", K(ret), "tablegroup_id", last_id, K(partition));
            } else if (OB_ISNULL(tablegroup_schema)) {
              table_exist = false;
              LOG_INFO("tablegroup not exist", K(ret), K(partition));
            } else if (OB_FAIL(tablegroup_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
              LOG_WARN("fail to get paxos replica num", K(ret), "tablegroup_schema", *tablegroup_schema);
            }
          } else {
            // private_table get paxos_num from table_schema
            if (OB_FAIL(schema_guard.get_table_schema(partition.get_table_id(), table_schema))) {
              LOG_WARN("get table schema failed", K(ret), "table id", partition.get_table_id());
            } else if (OB_ISNULL(table_schema)) {
              // table not exist
              table_exist = false;
              LOG_INFO("table not exist", K(ret), K(partition));
            } else if (OB_FAIL(table_schema->get_paxos_replica_num(schema_guard, paxos_replica_num))) {
              LOG_WARN("fail to get paxos replica num", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(paxos_replica_num <= 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num), K(partition));
            }
          }
        }
        if (OB_SUCC(ret) && table_exist && single_zone_deployment_on) {
          // stop server can migrate table in signle zone multi replicas.
          has_enough_member = true;
        } else if (OB_SUCC(ret) && table_exist) {
          // it is no need to check while table not exist
          int64_t rep_count = 0;
          int64_t full_replica_num = 0;
          bool leader_quorum_match = false;
          bool quorum_match = true;
          FOREACH_CNT_X(r, partition.get_replicas_v2(), has_enough_member)
          {
            bool found = false;
            FOREACH_CNT(server, server_list)
            {
              if (r->server_ == *server) {
                found = true;
                break;
              }
            }
            // do not count the replica in stopped server list
            if (r->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) && !found) {
              bool is_stopped = false;
              if (OB_FAIL(server_manager_.check_server_stopped(r->server_, is_stopped))) {
                LOG_WARN("fail to check server stopped", K(ret), KPC(r));
              } else if (!is_stopped) {
                // filter stopped_server while calculate majority to avoid failed to get leader.
                ++rep_count;
                if (REPLICA_TYPE_FULL == r->replica_type_) {
                  ++full_replica_num;
                }
              }
            }
            // "check paxos replica quorum" is a strong check. modify quorum need to wait majority success.
            // if has leader, check leader quorum match, if has no leader, check all paxos members's quorum match.
            if (need_have_leader  // in primary cluster or standby cluster's private table
                && r->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
              if (r->quorum_ == paxos_replica_num && r->is_leader_by_election()) {
                leader_quorum_match = true;
              }
              if (r->quorum_ != paxos_replica_num && quorum_match) {
                quorum_match = false;
              }
            }
          }
          if (!has_enough_member) {
            // no need to check anymore
          } else if (rep_count < majority(paxos_replica_num) && !single_zone_deployment_on) {
            has_enough_member = false;
            LOG_WARN("replica count is less than majority", K(partition), K(rep_count), K(paxos_replica_num));
          } else if (full_replica_num <= 0 && !single_zone_deployment_on) {
            has_enough_member = false;
            LOG_WARN(
                "no enough full replica, can't stop zone", K(partition), K(full_replica_num), K(paxos_replica_num));
          } else if (!leader_quorum_match && !quorum_match && !single_zone_deployment_on) {
            has_enough_member = false;
            LOG_WARN("quorum not match", K(partition), K(paxos_replica_num));
          }
        }
      }
    }
  }
  return ret;
}

// the reason for check twice:
// location cache of inner table is updated async, the first check will trigger location cache refresh async,
// it will miss the new observer.
int ObRootService::check_is_log_sync_twice(bool& is_log_sync, const common::ObIArray<ObAddr>& stop_server)
{
  int ret = OB_SUCCESS;
  is_log_sync = true;
  if (OB_FAIL(check_is_log_sync(is_log_sync, stop_server))) {
    LOG_WARN("fail to check is log sync", K(ret), K(stop_server));
  } else if (!is_log_sync) {
    LOG_WARN("log is not sync", K(ret), K(is_log_sync));
  } else {
    const int64_t SLEEP_TIME = 5L * 1000 * 1000;
    usleep(SLEEP_TIME);
    if (OB_FAIL(check_is_log_sync(is_log_sync, stop_server))) {
      LOG_WARN("fail to check is log sync", K(ret), K(stop_server));
    }
  }
  return ret;
}

int ObRootService::generate_stop_server_log_in_sync_dest_server_array(
    const common::ObIArray<common::ObAddr>& alive_server_array,
    const common::ObIArray<common::ObAddr>& excluded_server_array, common::ObIArray<common::ObAddr>& dest_server_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    dest_server_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < alive_server_array.count(); ++i) {
      const common::ObAddr& server = alive_server_array.at(i);
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

int ObRootService::check_is_log_sync(bool& is_log_sync, const common::ObIArray<ObAddr>& stop_server)
{
  int ret = OB_SUCCESS;
  is_log_sync = true;
  common::ObZone zone; /*empty zone*/
  common::ObArray<common::ObAddr> alive_server_array;
  common::ObArray<common::ObAddr> dest_server_array;
  ObSqlString sql;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_alive_servers(zone, alive_server_array))) {
    LOG_WARN("fail to get alive server array", K(ret));
  } else if (OB_FAIL(generate_stop_server_log_in_sync_dest_server_array(
                 alive_server_array, stop_server, dest_server_array))) {
    LOG_WARN("fail to generate stop server log in sync dest server array",
        K(ret),
        K(alive_server_array),
        K(stop_server),
        K(dest_server_array));
  } else if (OB_FAIL(generate_log_in_sync_sql(dest_server_array, sql))) {
    LOG_WARN("fail to generate log in sync dest server buff", K(ret), K(dest_server_array), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      share::schema::ObSchemaGetterGuard schema_guard;

      // pay attention partition_idx actually is partition_id
      if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else if (OB_FAIL(sql_proxy_.read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next()) && is_log_sync) {
          int64_t table_id = -1;
          int64_t partition_idx = -1;
          const ObSimpleTableSchemaV2* table_schema = NULL;
          int64_t index = 0;
          ObAddr server_addr;
          bool table_exist = true;
          bool partition_exist = true;
          if (OB_FAIL(result->get_int(index++, table_id))) {
            LOG_WARN("fail to get table_id", K(ret));
          } else if (OB_FAIL(result->get_int(index++, partition_idx))) {
            LOG_WARN("fail to partition_idx", K(ret));
          } else if (OB_UNLIKELY(table_id < 0 || partition_idx < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result unexpected", K(ret), K(table_id), K(partition_idx));
          } else {
            bool is_cluster_private_table = (PRIMARY_CLUSTER == ObClusterInfoGetter::get_cluster_type_v2()) ||
                                            ObMultiClusterUtil::is_cluster_private_table(table_id);
            const uint64_t table_id_standard = static_cast<uint64_t>(table_id);
            bool check_dropped_partition = true;
            if (!is_cluster_private_table) {
              // no need to check private table in standby cluster
              continue;
            } else {
              bool is_restore = true;
              const uint64_t tenant_id = extract_tenant_id(table_id);
              if (OB_FAIL(GSCHEMASERVICE.check_tenant_is_restore(&schema_guard, tenant_id, is_restore))) {
                LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
              } else if (is_restore) {
                continue;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(schema_guard.check_table_exist(table_id_standard, table_exist))) {
              LOG_WARN("fail to check table exist", K(ret), K(table_id_standard));
            } else if (!table_exist) {
              // good, not exist table, ignore and go on to check next;
            } else if (OB_FAIL(schema_guard.get_table_schema(table_id_standard, table_schema))) {
              LOG_WARN("fail to get table schema", K(ret), K(table_id_standard));
            } else if (OB_UNLIKELY(NULL == table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table schema null", K(ret), KP(table_schema));
            } else if (OB_FAIL(ObPartMgrUtils::check_part_exist(
                           *table_schema, partition_idx, check_dropped_partition, partition_exist))) {
              LOG_WARN("fail to check part exist", K(ret), K(table_id_standard), K(partition_idx));
            } else if (!partition_exist) {
              // good, not exsit partition, ignore and go on to check next
            } else {  // partition exist, however log not in sync
              is_log_sync = false;
              LOG_WARN("log is not sync", K(ret), K(table_id_standard), K(partition_idx));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// system admin command (alter system ...)
////////////////////////////////////////////////////////////////
int ObRootService::init_sys_admin_ctx(ObSystemAdminCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ctx.rs_status_ = &rs_status_;
    ctx.rpc_proxy_ = &rpc_proxy_;
    ctx.pt_ = pt_operator_;
    ctx.sql_proxy_ = &sql_proxy_;
    ctx.server_mgr_ = &server_manager_;
    ctx.rebalance_task_mgr_ = &rebalance_task_mgr_;
    ctx.daily_merge_scheduler_ = &daily_merge_scheduler_;
    ctx.zone_mgr_ = &zone_manager_;
    ctx.schema_service_ = schema_service_;
    ctx.leader_coordinator_ = &leader_coordinator_;
    ctx.ddl_service_ = &ddl_service_;
    ctx.config_mgr_ = config_mgr_;
    ctx.unit_mgr_ = &unit_manager_;
    ctx.root_inspection_ = &root_inspection_;
    ctx.root_service_ = this;
    ctx.root_balancer_ = &root_balancer_;
    ctx.schema_split_executor_ = &schema_split_executor_;
    ctx.upgrade_storage_format_executor_ = &upgrade_storage_format_executor_;
    ctx.create_inner_schema_executor_ = &create_inner_schema_executor_;
    ctx.schema_revise_executor_ = &schema_revise_executor_;
    ctx.rs_gts_manager_ = &rs_gts_manager_;
    ctx.inited_ = true;
  }
  return ret;
}

int ObRootService::observer_copy_local_index_sstable(const obrpc::ObServerCopyLocalIndexSSTableArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to copy local index sstable", K(ret), K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_UNLIKELY(NULL == pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator is null", K(ret));
  } else {
    const common::ObPartitionKey& pkey = arg.pkey_;
    const common::ObAddr& data_src = arg.data_src_;
    const common::ObAddr& dst = arg.dst_;
    const uint64_t index_table_id = arg.index_table_id_;
    ObCopySSTableTaskInfo task_info;
    ObCopySSTableTask task;
    common::ObArray<ObCopySSTableTaskInfo> task_info_array;
    OnlineReplica dest_replica;
    const char* comment = balancer::COPY_LOCAL_INDEX_SSTABLE;
    const ObCopySSTableType type = OB_COPY_SSTABLE_TYPE_LOCAL_INDEX;
    int64_t data_size = 0;
    ObReplicaMember src_member;
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition_info;
    partition_info.set_allocator(&allocator);
    if (OB_FAIL(pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), partition_info))) {
      LOG_WARN("fail to get partition info", K(ret));
    } else {
      FOREACH_CNT_X(r, partition_info.get_replicas_v2(), OB_SUCC(ret))
      {
        bool alive = false;
        if (r->server_ == dst) {
          dest_replica.member_ =
              ObReplicaMember(r->server_, ObTimeUtility::current_time(), r->replica_type_, r->get_memstore_percent());
          dest_replica.unit_id_ = r->unit_id_;
          data_size = arg.data_size_ != 0 ? arg.data_size_ : r->data_size_;
        } else if (r->server_ != data_src) {
          // by pass
        } else if (OB_FAIL(server_manager_.check_server_alive(r->server_, alive))) {
          LOG_WARN("fail to check server alive", K(ret));
        } else if (!alive || !r->is_in_service()) {
          ret = OB_EAGAIN;
          // observer of data source in not valid, here just return OB_EAGAIN.
          // local index builder will eventually perceive it and choose a new data source.
          LOG_WARN("data src server state not match", K(ret), K(data_src), K(*r));
        } else {
          src_member = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
        }
      }
      if (OB_FAIL(ret)) {
        // nothing todo
      } else if (src_member.is_valid()) {
        // nothing todo
      } else if (STANDBY_CLUSTER == ObClusterInfoGetter::get_cluster_type_v2() && arg.data_src_.is_valid()) {
        src_member = ObReplicaMember(arg.data_src_, 0, REPLICA_TYPE_FULL);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!src_member.is_valid())) {
        // the error code must be OB_EAGAIN, because the index builder need to retry according to the code
        // even in standby cluster, the src_member must be valid
        ret = OB_EAGAIN;
        LOG_WARN("data source replica not found", KR(ret), K(partition_info), K(arg));
      } else if (!dest_replica.is_valid()) {
        ret = OB_EAGAIN;
        LOG_WARN("dest replica not found", K(ret), K(partition_info), K(arg));
      } else if (OB_FAIL(task_info.build(pkey, dest_replica, src_member, index_table_id))) {
        LOG_WARN("fail to build copy local index sstable task info",
            K(ret),
            K(pkey),
            K(index_table_id),
            K(src_member),
            K(dest_replica));
      } else if (FALSE_IT(task_info.set_transmit_data_size(data_size))) {
        // would never be here
      } else if (FALSE_IT(task_info.set_cluster_id(arg.cluster_id_))) {
        // would never be here
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(task_info_array, type, dest_replica.member_.get_server(), comment))) {
        LOG_WARN("fail to build copy local index sstable task", K(ret));
      } else if (OB_FAIL(rebalance_task_mgr_.add_task(task))) {
        LOG_WARN("fail to add task", K(ret), K(task));
      } else {
        ROOTSERVICE_EVENT_ADD("balancer", "add_copy_sstable_task",
                              "partition", ReplicaInfo(pkey, data_size),
                              "source", src_member,
                              "destination", dest_replica);
      }
    }
  }
  return ret;
}

int ObRootService::admin_switch_replica_role(const obrpc::ObAdminSwitchReplicaRoleArg& arg)
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

int ObRootService::generate_log_in_sync_sql(
    const common::ObIArray<common::ObAddr> &dest_server_array, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(dest_server_array.count() <= 0)) {
    ret = OB_SERVER_NOT_ALIVE;  // all server not alive
    LOG_WARN("all server not alive", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT table_id, partition_idx FROM %s WHERE is_in_sync = 0 and "
                                    "is_offline = 0 and replica_type != 16 "
                                    "and (svr_ip, svr_port) in (",
                 OB_ALL_VIRTUAL_CLOG_STAT_TNAME))) {
    LOG_WARN("assign_fmt failed", KR(ret), K(sql));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_server_array.count(); ++i) {
      const common::ObAddr &addr = dest_server_array.at(i);
      char ip_str[MAX_IP_PORT_LENGTH] = "";
      if (0 != i) {
        if (OB_FAIL(sql.append(","))) {
          LOG_WARN("failed to append fmt sql", KR(ret), K(i), K(sql));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!addr.ip_to_string(ip_str, MAX_IP_PORT_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to ip to string", KR(ret), K(ip_str), K(addr));
      } else if (OB_FAIL(sql.append_fmt("(\"%s\", %d)", ip_str, addr.get_port()))) {
        LOG_WARN("failed to append fmt sql", KR(ret), K(i), K(ip_str), K(addr), K(sql));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to append fmt sql", KR(ret), K(sql));
  }
  return ret;
}


int ObRootService::admin_switch_rs_role(const obrpc::ObAdminSwitchRSRoleArg& arg)
{
  int ret = OB_SUCCESS;
  obrpc::ObAdminSwitchReplicaRoleArg arg_new;
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
      const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
      if (OB_FAIL(copy_assign(arg_new.role_, arg.role_))) {
        LOG_WARN("copy role failed", K(ret));
      } else if (OB_FAIL(arg_new.partition_key_.init(table_id,
                     ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                     ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM))) {
        LOG_WARN("partition_key_ init failed",
            KT(table_id),
            "partition_id",
            static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
            "partition_num",
            static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM),
            K(ret));
      } else if (false == arg.zone_.is_empty()) {
        ObPartitionInfo partition_info;
        bool zone_exist = false;
        if (OB_FAIL(zone_manager_.check_zone_exist(arg.zone_, zone_exist))) {
          LOG_WARN("fail check zone exits", K_(arg.zone), K(ret));
        } else if (!zone_exist) {
          ret = OB_ZONE_INFO_NOT_EXIST;
          LOG_WARN("zone not exits", K_(arg.zone), K(zone_exist), K(ret));
        } else if (OB_FAIL(get_root_partition(partition_info))) {
          LOG_WARN("get root partition failed", K(ret));
        } else {
          bool found_server = false;
          FOREACH_CNT_X(replica, partition_info.get_replicas_v2(), !found_server)
          {
            if (true == replica->is_in_service() && ObReplicaTypeCheck::is_paxos_replica_V2(replica->replica_type_)) {
              if (replica->zone_ == arg.zone_) {
                arg_new.server_ = replica->server_;
                found_server = true;
              }
            }
          }
          if (false == found_server) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("cannot get rootserver", K(ret), K(arg.zone_));
          }
        }
      } else if (true == arg.server_.is_valid()) {
        ObArray<ObAddr> rs_list;
        if (OB_FAIL(get_root_partition_table(rs_list))) {
          LOG_WARN("get rs_list failed", K(ret));
        } else {
          bool found = false;
          FOREACH_CNT_X(rs, rs_list, !found)
          {
            if (*rs == arg.server_) {
              found = true;
              arg_new.server_ = arg.server_;
            }
          }
          if (false == found) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("server_ is not in rs_list", K(ret), K(arg.server_), K(rs_list));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("arg is invalid", K(ret), K(arg));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(admin_util.execute(arg_new))) {
          LOG_WARN("execute switch rootserver role failed", K(arg_new), K(ret));
        }
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_switch_rs_role", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_change_replica(const obrpc::ObAdminChangeReplicaArg& arg)
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
      ObAdminChangeReplica admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute change replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_change_replica", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_drop_replica(const obrpc::ObAdminDropReplicaArg& arg)
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
      ObAdminDropReplica admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute drop replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_drop_replica", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_migrate_replica(const obrpc::ObAdminMigrateReplicaArg& arg)
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
      ObAdminMigrateReplica admin_util(ctx);
      if (OB_FAIL(admin_util.execute(arg))) {
        LOG_WARN("execute migrate replica failed", K(arg), K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_migrate_replica", K(ret), K(arg));
  return ret;
}

int ObRootService::admin_report_replica(const obrpc::ObAdminReportReplicaArg& arg)
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

int ObRootService::admin_load_baseline(const obrpc::ObAdminLoadBaselineArg& arg)
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

int ObRootService::admin_flush_cache(const obrpc::ObAdminFlushCacheArg& arg)
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

int ObRootService::admin_recycle_replica(const obrpc::ObAdminRecycleReplicaArg& arg)
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

int ObRootService::admin_merge(const obrpc::ObAdminMergeArg& arg)
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

int ObRootService::admin_clear_roottable(const obrpc::ObAdminClearRoottableArg& arg)
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

int ObRootService::admin_refresh_schema(const obrpc::ObAdminRefreshSchemaArg& arg)
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

int ObRootService::admin_set_config(obrpc::ObAdminSetConfigArg& arg)
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

int ObRootService::admin_clear_location_cache(const obrpc::ObAdminClearLocationCacheArg& arg)
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

int ObRootService::admin_reload_gts()
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
      ObAdminReloadGts admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("execute reload gts failed", K(ret));
      }
    }
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_reload_gts", K(ret));
  return ret;
}

int ObRootService::admin_refresh_memory_stat(const ObAdminRefreshMemStatArg& arg)
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

int ObRootService::admin_clear_merge_error()
{
  int ret = OB_SUCCESS;
  LOG_INFO("admin receive clear_merge_error request");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSystemAdminCtx ctx;
    if (OB_FAIL(init_sys_admin_ctx(ctx))) {
      LOG_WARN("init_sys_admin_ctx failed", K(ret));
    } else {
      ObAdminClearMergeError admin_util(ctx);
      if (OB_FAIL(admin_util.execute())) {
        LOG_WARN("execute clear merge error failed", K(ret));
      }
      ROOTSERVICE_EVENT_ADD("daily_merge", "clear_merge_error", "result", ret);
    }
  }
  return ret;
}

int ObRootService::admin_migrate_unit(const obrpc::ObAdminMigrateUnitArg& arg)
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

int ObRootService::admin_upgrade_cmd(const obrpc::Bool& arg)
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

int ObRootService::admin_rolling_upgrade_cmd(const obrpc::ObAdminRollingUpgradeArg& arg)
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

int ObRootService::restore_tenant(const obrpc::ObRestoreTenantArg& arg)
{
  int ret = OB_SUCCESS;
  ObSystemAdminCtx admin_ctx;
  bool has_job = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3000) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("restore tenant logically while cluster version is not less than 3.0 not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "restore tenant logically while cluster version is not less than 3.0");
  } else if (OB_ISNULL(restore_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(init_sys_admin_ctx(admin_ctx))) {
    LOG_WARN("init_sys_admin_ctx failed", K(ret));
  } else if (OB_FAIL(ObRestoreUtil::check_has_job(restore_ctx_->sql_client_, arg, has_job))) {
    LOG_WARN("fail check if any job in progress", K(ret));
  } else if (has_job) {
    ret = OB_RESTORE_IN_PROGRESS;
    LOG_WARN("Another job is in progress", K(ret));
  } else {
    ObRestoreUtil restore_util;
    int64_t job_id = -1;
    if (OB_FAIL(
            RS_JOB_CREATE_EXT(job_id, RESTORE_TENANT, sql_proxy_, "sql_text", ObHexEscapeSqlStr(arg.get_sql_stmt())))) {
      LOG_WARN("fail create rs job", K(ret));
    } else if (OB_FAIL(restore_util.init(*restore_ctx_, job_id))) {
      LOG_WARN("fail init restore util", K(ret));
    } else if (OB_FAIL(restore_util.execute(arg))) {
      LOG_WARN("restore tenant fail", K(arg), K(ret));
    } else {
      root_balancer_.wakeup();
    }
    LOG_INFO("restore tenant first phase done", K(job_id), K(arg), K(ret));
  }
  return ret;
}

int ObRootService::physical_restore_tenant(const obrpc::ObPhysicalRestoreTenantArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t gc_snapshot_ts = OB_INVALID_TIMESTAMP;
  int64_t current_timestamp = ObTimeUtility::current_time();
  const int64_t RESTORE_TIMESTAMP_DETA = 10 * 1000 * 1000L;
  int64_t job_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (GCTX.is_standby_cluster() || GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore tenant while in standby cluster or "
             "in upgrade mode is not allowed",
        KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore tenant while in standby cluster or in upgrade mode");
  } else if (0 == GCONF.restore_concurrency) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore tenant when restore_concurrency is 0 not allowed", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore tenant when restore_concurrency is 0");
  } else if (OB_FAIL(freeze_info_manager_.get_latest_snapshot_gc_ts(gc_snapshot_ts))) {
    LOG_WARN("fail to get latest snapshot gc ts", K(ret));
  } else if (gc_snapshot_ts < arg.restore_timestamp_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("local gc_snapshot_ts is old", K(ret), K(gc_snapshot_ts), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "need retry later, local gc snapshot ts is too old");
  } else if (arg.restore_timestamp_ + RESTORE_TIMESTAMP_DETA >= current_timestamp) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore_timestamp is too new", K(ret), K(current_timestamp), K(arg));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "need retry later, restore timestamp is too new");
  } else if (OB_FAIL(
                 ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant's schema guard", KR(ret));
  } else {
    ObMySQLTransaction trans;
    ObPhysicalRestoreJob job_info;
    if (OB_FAIL(trans.start(&sql_proxy_))) {
      LOG_WARN("failed to start trans, ", K(ret));
    } else if (OB_FAIL(RS_JOB_CREATE_EXT(
                   job_id, RESTORE_TENANT, trans, "sql_text", ObHexEscapeSqlStr(arg.get_sql_stmt())))) {
      LOG_WARN("fail create rs job", K(ret), K(arg));
    } else if (job_id < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid job_id", K(ret), K(job_id));
    } else if (OB_FAIL(ObRestoreUtil::fill_physical_restore_job(job_id, arg, job_info))) {
      LOG_WARN("fail to fill physical restore job", K(ret), K(job_id), K(arg));
    } else {
      job_info.restore_start_ts_ = current_timestamp;
      // check if tenant exists
      const ObTenantSchema* tenant_schema = NULL;
      ObString tenant_name(job_info.tenant_name_);
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", KR(ret), K(job_info));
      } else if (OB_NOT_NULL(tenant_schema)) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("restore tenant with existed tenant name is not allowed", KR(ret), K(tenant_name));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "restore tenant with existed tenant name is");
      }
    }
    if (FAILEDx(ObRestoreUtil::record_physical_restore_job(trans, job_info))) {
      LOG_WARN("fail to record physical restore job", K(ret), K(job_id), K(arg));
    } else {
      restore_scheduler_.wakeup();
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
  }
  LOG_INFO("[RESTORE] physical restore tenant start", K(arg), K(ret));
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_start", K(ret), "tenant_name", arg.tenant_name_);
  if (OB_SUCC(ret)) {
    const char* status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(
        static_cast<PhysicalRestoreStatus>(PHYSICAL_RESTORE_CREATE_TENANT));
    ROOTSERVICE_EVENT_ADD("physical_restore",
        "change_restore_status",
        "job_id",
        job_id,
        "tenant_name",
        arg.tenant_name_,
        "status",
        status_str);
  }
  return ret;
}

int ObRootService::run_job(const obrpc::ObRunJobArg& arg)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
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

int ObRootService::run_upgrade_job(const obrpc::ObUpgradeJobArg& arg)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  int64_t version = arg.version_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), KR(ret));
  } else if (version < CLUSTER_VERSION_2270 || !ObUpgradeChecker::check_cluster_version_exist(version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported version to run upgrade job", KR(ret), K(version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "run upgrade job with such version is");
  } else if (ObUpgradeJobArg::RUN_UPGRADE_JOB == arg.action_) {
    if (OB_FAIL(submit_upgrade_task(arg.version_))) {
      LOG_WARN("fail to submit upgrade task", KR(ret), K(arg));
    }
  } else if (ObUpgradeJobArg::STOP_UPGRADE_JOB == arg.action_) {
    if (OB_FAIL(upgrade_executor_.stop())) {
      LOG_WARN("fail to stop upgrade task", KR(ret));
    } else {
      upgrade_executor_.start();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid action type", KR(ret), K(arg));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_run_upgrade_job", KR(ret), K(arg));
  return ret;
}

int ObRootService::handle_merge_error()
{
  int ret = OB_SUCCESS;
  bool merge_error_set = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_manager_.is_merge_error(merge_error_set))) {
    LOG_WARN("is_merge_error failed", K(ret));
  } else if (!merge_error_set) {
    const int64_t merge_error = 1;
    if (OB_FAIL(zone_manager_.set_merge_error(merge_error))) {
      LOG_WARN("set merge error failed", K(merge_error), K(ret));
    }
    LOG_ERROR("rootservice found checksum error!");
  }
  ROOTSERVICE_EVENT_ADD("daily_merge", "merge_process", "merge_error", ret);
  return ret;
}

int ObRootService::statistic_primary_zone_entity_count()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.statistic_primary_zone_entity_count())) {
    LOG_WARN("fail to statistic primary zone entity count", K(ret));
  }
  return ret;
}

int ObRootService::upgrade_cluster_create_ha_gts_util()
{
  int ret = OB_SUCCESS;
  ObRsJobInfo job_info;
  ObRsJobType job_type = ObRsJobType::JOB_TYPE_CREATE_HA_GTS_UTIL;
  int64_t job_id = OB_INVALID_ID;
  SpinWLockGuard guard(upgrade_cluster_create_ha_gts_lock_);  // flow lock
  bool upgrade_cluster_ha_gts_task_done = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int tmp_ret = RS_JOB_FIND(job_info, sql_proxy_, "job_type", "CREATE_HA_GTS_UTIL", "job_status", "SUCCESS");
    if (OB_SUCCESS == tmp_ret) {
      upgrade_cluster_ha_gts_task_done = true;
    } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      upgrade_cluster_ha_gts_task_done = false;
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to find job", K(ret));
    }
  }

  if (OB_SUCC(ret) && !upgrade_cluster_ha_gts_task_done) {
    // finish the previous unfinished task
    if (OB_FAIL(RS_JOB_FIND(job_info, sql_proxy_, "job_type", "CREATE_HA_GTS_UTIL", "job_status", "INPROGRESS"))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to find job", K(ret));
      }
    } else if (job_info.job_id_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find a job id unexpected", K(ret));
    } else if (OB_FAIL(RS_JOB_COMPLETE(job_info.job_id_, -1, sql_proxy_))) {
      LOG_WARN("fail to complete job", K(ret));
    }

    if (OB_SUCC(ret)) {
      // create a new job
      if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, sql_proxy_, "tenant_id", 0))) {
        LOG_WARN("fail to create rs job", K(ret));
      } else if (OB_FAIL(unit_manager_.upgrade_cluster_create_ha_gts_util())) {
        LOG_WARN("unit mgr fail to upgrade cluster create ha gts units", K(ret));
      } else if (OB_FAIL(rs_gts_manager_.upgrade_cluster_create_ha_gts_util())) {
      } else if (OB_FAIL(ddl_service_.upgrade_cluster_create_ha_gts_util())) {
        LOG_WARN("ddl service fail to upgrade cluster create ha gts util", K(ret));
      }
      int tmp_ret = OB_SUCCESS;
      if (job_id > 0) {
        if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, sql_proxy_))) {
          LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
          ret = (OB_FAIL(ret)) ? ret : tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObRootService::merge_error(const obrpc::ObMergeErrorArg& arg)
{
  LOG_ERROR("receive merge error", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(handle_merge_error())) {
    LOG_WARN("fail to handle merge error", K(ret));
  }
  return ret;
}

int ObRootService::merge_finish(const obrpc::ObMergeFinishArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive merge finish", K(arg));
  bool zone_merged = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(server_manager_.update_merged_version(arg.server_, arg.frozen_version_, zone_merged))) {
  } else {
    if (zone_merged) {
      LOG_INFO("zone merged, wakeup daily merge thread");
      daily_merge_scheduler_.wakeup();
    }
  }
  return ret;
}

int ObRootService::schedule_sql_bkgd_task(const sql::ObSchedBKGDDistTask& bkgd_task)
{
  int ret = OB_SUCCESS;
  if (!is_full_service()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("RS is initializing", K(ret), K(bkgd_task));
  } else {
    ObSqlBKGDDistTaskInfo task_info;
    ObRebalanceSqlBKGTask task;
    const char* comment = balancer::SQL_BACKGROUND_DIST_TASK_COMMENT;
    if (OB_FAIL(task_info.build(bkgd_task))) {
      LOG_WARN("get build sql background task failed", K(ret));
    } else if (OB_FAIL(task.build(task_info, comment))) {
      LOG_WARN("get rebalance task failed", K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_.add_task(task))) {
      LOG_WARN("add task failed", K(ret));
    }
  }

  return ret;
}

int ObRootService::sql_bkgd_task_execute_over(const sql::ObBKGDTaskCompleteArg& arg)
{
  int ret = OB_SUCCESS;
  if (!is_full_service()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("RS is initializing", K(ret), K(arg));
  } else {
    ObRebalanceSqlBKGTask task;
    ObSEArray<int, 1> rc_array;
    if (OB_FAIL(task.build_by_task_result(arg))) {
      LOG_WARN("init execute over task failed", K(ret));
    } else if (OB_FAIL(rc_array.push_back(arg.return_code_))) {
      LOG_WARN("array push back failed", K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_.execute_over(task, rc_array))) {
      LOG_WARN("background task execute over failed", K(ret));
    }
  }
  return ret;
}

int ObRootService::admin_rebuild_replica(const obrpc::ObAdminRebuildReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive rebuild replica", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObRebuildTaskInfo task_info;
    ObRebuildReplicaTask task;
    common::ObArray<ObRebuildTaskInfo> task_info_array;
    OnlineReplica dest_replica;
    const char* comment = balancer::REBUILD_REPLICA;
    int64_t data_size = 0;
    ObReplicaMember src_member;

    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition;
    partition.set_allocator(&allocator);
    if (OB_FAIL(pt_operator_->get(arg.key_.get_table_id(), arg.key_.get_partition_id(), partition))) {
      LOG_WARN("get partition info failed", K(ret), K(arg));
    } else {
      bool alive = false;
      // try to set replica with max data_version to source
      int64_t data_version = 0;
      FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
      {
        alive = false;
        if (r->server_ == arg.server_) {
          dest_replica.member_ =
              ObReplicaMember(r->server_, ObTimeUtility::current_time(), r->replica_type_, r->get_memstore_percent());
          dest_replica.unit_id_ = r->unit_id_;
          data_size = r->data_size_;
        } else if (OB_FAIL(server_manager_.check_server_alive(r->server_, alive))) {
          LOG_WARN("check server alive failed", K(ret), "server", r->server_);
        } else if (alive && r->data_version_ > data_version && r->is_in_service() &&
                   ObReplicaTypeCheck::can_as_data_source(dest_replica.member_.get_replica_type(), r->replica_type_)) {
          src_member = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_);
          data_version = r->data_version_;
        }
      }

      if (OB_SUCC(ret)) {
        if (!src_member.is_valid()) {
          ret = OB_DATA_SOURCE_NOT_EXIST;
          LOG_WARN("choose rebuild task source failed", K(ret), K(partition), K(arg));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(task_info.build(dest_replica, arg.key_, src_member))) {
        LOG_WARN("fail to build rebuild task info", K(ret), K(arg.key_));
      } else if (FALSE_IT(task_info.set_transmit_data_size(data_size))) {
        // nop
      } else if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task.build(task_info_array, dest_replica.member_.get_server(), comment))) {
        LOG_WARN("fail to build rebuild task", K(ret), K(arg), K(partition));
      } else if (OB_FAIL(rebalance_task_mgr_.add_task(task))) {
        LOG_WARN("add task failed", K(ret), K(task));
      } else {
        LOG_INFO("add rebuild replica task", K(task));
      }
    }
  }
  return ret;
}

int ObRootService::broadcast_ds_action(const obrpc::ObDebugSyncActionArg& arg)
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
  } else if (OB_FAIL(server_manager_.get_alive_servers(all_zone, server_list))) {
    LOG_WARN("get all alive servers failed", K(all_zone), K(ret));
  } else {
    FOREACH_X(s, server_list, OB_SUCCESS == ret)
    {
      if (OB_FAIL(rpc_proxy_.to(*s).timeout(config_->rpc_timeout).set_debug_sync_action(arg))) {
        LOG_WARN("set server's global sync action failed", K(ret), "server", *s, K(arg));
      }
    }
  }
  return ret;
}

int ObRootService::sync_pg_pt_finish(const obrpc::ObSyncPGPartitionMTFinishArg& arg)
{
  LOG_INFO("receive sync pg partition meta table finish rpc", K(arg));
  return OB_NOT_SUPPORTED;
}

int ObRootService::sync_pt_finish(const obrpc::ObSyncPartitionTableFinishArg& arg)
{
  LOG_INFO("receive sync partition table finish rpc", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(empty_server_checker_.pt_sync_finish(arg.server_, arg.version_))) {
    LOG_WARN("partition table sync finish failed", K(ret), K(arg));
  }
  return ret;
}
int ObRootService::check_dangling_replica_finish(const obrpc::ObCheckDanglingReplicaFinishArg& arg)
{
  UNUSED(arg);
  return OB_NOT_SUPPORTED;
}

int ObRootService::fetch_alive_server(const ObFetchAliveServerArg& arg, ObFetchAliveServerResult& result)
{
  LOG_DEBUG("receive fetch alive server request");
  ObZone empty_zone;  // for all server
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
    LOG_WARN("cluster id mismatch", K(ret), K(arg), "cluster_id", static_cast<int64_t>(config_->cluster_id));
  } else if (OB_FAIL(server_manager_.get_servers_by_status(
                 empty_zone, result.active_server_list_, result.inactive_server_list_))) {
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
  } else if (!in_service() || (server_refreshed_ && load_frozen_status)) {
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
    }

    // request heartbeats from observers
    if (OB_SUCC(ret)) {
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
      if (OB_SUCCESS != (tmp_ret = schedule_refresh_server_timer_task(ObRefreshServerTask::REFRESH_SERVER_INTERVAL))) {
        if (OB_CANCELED != ret) {
          LOG_ERROR("schedule refresh server task failed", K(ret));
        } else {
          LOG_WARN("schedule refresh server task failed, because the server is stopping", K(ret));
        }
      } else {
        LOG_INFO("schedule refresh server task again");
      }
    } else {
    }  // no more to do
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
    ObArray<uint64_t> tenant_ids;  // depend on sys schema while start RS
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
      ObSchemaService* schema_service = schema_service_->get_schema_service();
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
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();

  snprintf(sql, sizeof(sql), "alter system set min_observer_version = '%s'", PACKAGE_VERSION);
  if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql, affected_rows))) {
    LOG_WARN("execute sql failed", K(sql));
  }

  return ret;
}

int ObRootService::admin_set_tracepoint(const obrpc::ObAdminSetTPArg& arg)
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
int ObRootService::refresh_time_zone_info(const obrpc::ObRefreshTimezoneArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  ROOTSERVICE_EVENT_ADD("root_service", "refresh_time_zone_info", K(ret), K(arg));
  return ret;
}

int ObRootService::request_time_zone_info(const ObRequestTZInfoArg& arg, ObRequestTZInfoResult& result)
{
  UNUSED(arg);
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SYS_TENANT_ID;

  ObTZMapWrap tz_map_wrap;
  ObTimeZoneInfoManager* tz_info_mgr = NULL;
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
    LOG_INFO("rs success to response lastest tz_info to server",
        "server",
        arg.obs_addr_,
        "last_version",
        result.last_version_);
  }
  return ret;
}

int ObRootService::check_tenant_group_config_legality(
    common::ObIArray<ObTenantGroupParser::TenantNameGroup>& tenant_groups, bool& legal)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(root_balancer_.check_tenant_group_config_legality(tenant_groups, legal))) {
    LOG_WARN("fail to check tenant group config legality", K(ret));
  } else {
  }  // no more to do
  return ret;
}

bool ObRootService::check_config(const ObConfigItem& item, const char*& err_info)
{
  bool bret = true;
  err_info = NULL;
  if (!inited_) {
    bret = false;
    LOG_WARN("service not init");
  } else if (0 == STRCMP(item.name(), ZONE_MERGE_ORDER)) {
    ObString merge_order(item.str());
    int64_t zone_merge_concurrency = GCONF.zone_merge_concurrency;
    if (OB_SUCCESS != zone_manager_.check_merge_order(merge_order)) {
      LOG_WARN("fail to check merge order");
      bret = false;
    } else if (!merge_order.empty() && zone_merge_concurrency == 0) {
      bret = false;
      err_info = config_error::INALID_ZONE_MERGE_ORDER;
      LOG_WARN("can't set zone_merge_order while zone_merge_concurrency = 0");
    }
  } else if (0 == STRCMP(item.name(), UNIT_BALANCE_RESOURCE_WEIGHT)) {
    ObResourceWeight weight;
    if (OB_SUCCESS != ObResourceWeightParser::parse(item.str(), weight)) {
      LOG_WARN("fail to check unit_balance_resource_weight value");
      bret = false;
    }
  } else if (0 == STRCMP(item.name(), ZONE_MERGE_CURRENCNCY)) {
    ObString merge_order_str = config_->zone_merge_order.str();
    char* end_ptr = NULL;
    int64_t zone_merge_concurrency = strtol(item.str(), &end_ptr, 10);
    if (!merge_order_str.empty() && zone_merge_concurrency == 0) {
      bret = false;
      err_info = config_error::INVALID_ZONE_MERGE_CONCURRENCY;
      LOG_WARN("can't set zone_merge_concurrency= 0 while zone_merg_order is not null");
    }
  } else if (0 == STRCMP(item.name(), MIN_OBSERVER_VERSION)) {
    if (OB_SUCCESS != ObClusterVersion::is_valid(item.str())) {
      LOG_WARN("fail to parse min_observer_version value");
      bret = false;
    }
  } else if (0 == STRCMP(item.name(), __BALANCE_CONTROLLER)) {
    ObString balance_troller_str(item.str());
    ObRootBalanceHelp::BalanceController switch_info;
    if (OB_SUCCESS != ObRootBalanceHelp::parse_balance_info(balance_troller_str, switch_info)) {
      LOG_WARN("fail to parse balance switch", K(balance_troller_str));
      bret = false;
    }
  } else if (0 == STRCMP(item.name(), TENANT_GROUPS)) {
    ObString tenant_groups_str(item.str());
    ObTenantGroupParser ttg_parser;
    common::ObArray<ObTenantGroupParser::TenantNameGroup> tenant_groups;
    bool legal = false;
    if (OB_SUCCESS != ttg_parser.parse_tenant_groups(tenant_groups_str, tenant_groups)) {
      bret = false;
      err_info = config_error::INVALID_TENANT_GROUPS;
      LOG_WARN("invalid tenant groups, please check tenant group matrix, zone list and unit num");
    } else if (OB_SUCCESS != check_tenant_group_config_legality(tenant_groups, legal)) {
      bret = false;
      err_info = config_error::INVALID_TENANT_GROUPS;
      LOG_WARN("invalid tenant groups, please check tenant group matrix, zone list and unit num");
    } else if (!legal) {  // invalid tenant groups setting
      bret = false;
      err_info = config_error::INVALID_TENANT_GROUPS;
      LOG_WARN("invalid tenant groups, please check tenant group matrix, zone list and unit num");
    } else if (tenant_groups.count() > 0) {
      // if tenant_groups's count > 0, than server_balance_critical_disk_waterlevel's value != 0;
      int64_t disk_waterlevel = GCONF.server_balance_critical_disk_waterlevel;
      if (disk_waterlevel <= 0) {
        bret = false;
        err_info = config_error::INVALID_TENANT_GROUPS;
        LOG_WARN("cannot specify tenant group when disk waterlevel is specified to zero");
      }
    } else {
    }  // good, legal tenant groups config
  } else if (0 == STRCMP(item.name(), SERVER_BALANCE_CRITICAL_DISK_WATERLEVEL)) {
    char* end_ptr = NULL;
    int64_t disk_waterlevel = strtol(item.str(), &end_ptr, 10);
    ObTenantGroupParser ttg_parser;
    ObString ttg_string = GCONF.tenant_groups.str();
    common::ObArray<ObTenantGroupParser::TenantNameGroup> tenant_groups;
    if (disk_waterlevel > 0) {
      // when disk waterlevel is greater than 0, no need to check
    } else if (OB_SUCCESS != ttg_parser.parse_tenant_groups(ttg_string, tenant_groups)) {
      // will never be here
    } else if (tenant_groups.count() > 0) {
      bret = false;
      err_info = config_error::INVALID_DISK_WATERLEVEL;
      LOG_WARN("cannot specify disk waterlevel to zero when tenant groups matrix is specified");
    } else {
    }  // tenant groups count less than or equals to 0, good
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
  } else if (OB_FAIL(server_manager_.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    FOREACH_CNT(server, server_list)
    {
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

int ObRootService::report_single_replica(const ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> server_list;
  ObZone null_zone;
  obrpc::ObReportSingleReplicaArg arg;
  arg.partition_key_ = key;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    FOREACH_CNT(server, server_list)
    {
      if (OB_ISNULL(server)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid server", K(ret));
      } else if (OB_FAIL(rpc_proxy_.to(*server).report_single_replica(arg))) {
        LOG_WARN("fail to force observer report replica", K(ret), K(server), K(key));
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
  ObAdminSetConfigItem all_server_config;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> value;
  int64_t pos = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_servers_of_zone(empty_zone, server_list))) {
    LOG_WARN("fail to get server", K(ret));
  } else if (OB_FAIL(all_server_config.name_.assign(config_->all_server_list.name()))) {
    LOG_WARN("fail to assign name", K(ret));
  } else {
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
  if (OB_SUCC(ret)) {
    ObAdminSetConfigArg arg;
    arg.is_inner_ = true;
    int32_t buf_len = OB_MAX_CONFIG_VALUE_LEN;
    ObString tmp_string(buf_len, static_cast<int32_t>(pos), value.ptr());
    if (OB_FAIL(all_server_config.value_.assign(tmp_string))) {
      LOG_WARN("fail to assign", K(ret), K(tmp_string));
    } else if (OB_FAIL(arg.items_.push_back(all_server_config))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(admin_set_config(arg))) {
      LOG_WARN("fail to set config", K(ret));
    } else {
      LOG_INFO("update all server config success", K(arg));
    }
  }
  return ret;
}

/////////////////////////
ObRootService::ObReportCoreTableReplicaTask::ObReportCoreTableReplicaTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(INT64_MAX);  // retry until success
}

int ObRootService::ObReportCoreTableReplicaTask::process()
{
  int ret = OB_SUCCESS;
  const ObPartitionKey pkey(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
  if (OB_FAIL(root_service_.report_single_replica(pkey))) {
    LOG_WARN("fail to report single replica", K(ret), K(pkey));
  } else {
    LOG_INFO("report all_core table succeed");
  }
  return ret;
}

ObAsyncTask* ObRootService::ObReportCoreTableReplicaTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObReportCoreTableReplicaTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObReportCoreTableReplicaTask(root_service_);
  }
  return task;
}
//////////////ObReloadUnitManagerTask
ObRootService::ObReloadUnitManagerTask::ObReloadUnitManagerTask(
    ObRootService& root_service, ObUnitManager& unit_manager)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service), unit_manager_(unit_manager)
{
  set_retry_times(INT64_MAX);  // retry until success
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

ObAsyncTask* ObRootService::ObReloadUnitManagerTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObReloadUnitManagerTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObReloadUnitManagerTask(root_service_, unit_manager_);
  }
  return task;
}

ObRootService::ObLoadIndexBuildTask::ObLoadIndexBuildTask(
    ObRootService& root_service, ObGlobalIndexBuilder& global_index_builder)
    : ObAsyncTimerTask(root_service.task_queue_),
      root_service_(root_service),
      global_index_builder_(global_index_builder)
{
  set_retry_times(INT64_MAX);
}

int ObRootService::ObLoadIndexBuildTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(global_index_builder_.reload_building_indexes())) {
    LOG_WARN("fail to reload building indexes", K(ret));
  }
  return ret;
}

ObAsyncTask* ObRootService::ObLoadIndexBuildTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObLoadIndexBuildTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer is not enought", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObLoadIndexBuildTask(root_service_, global_index_builder_);
  }
  return task;
}

////////////////////
ObRootService::ObSelfCheckTask::ObSelfCheckTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(0);  // don't retry when failed
}

int ObRootService::ObSelfCheckTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.self_check())) {
    LOG_ERROR("fail to do root inspection check, please check it", K(ret));
  } else {
    LOG_INFO("self check success!");
  }
  return ret;
}

ObAsyncTask* ObRootService::ObSelfCheckTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObSelfCheckTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size));
  } else {
    task = new (buf) ObSelfCheckTask(root_service_);
  }
  return task;
}

/////////////////////////
int ObRootService::ObIndexSSTableBuildTask::process()
{
  int ret = OB_SUCCESS;
  sql::ObIndexSSTableBuilder builder;
  if (OB_UNLIKELY(!job_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(builder.init(sql_proxy_, oracle_sql_proxy_, job_, replica_picker_, abs_timeout_us_))) {
    LOG_WARN("init index sstable builder failed", K(ret));
  } else if (OB_FAIL(builder.build())) {
    LOG_WARN("build failed", K(ret));
  }

  LOG_INFO("build global index sstable finish", K(ret), K(job_));
  int tmp_ret = global_index_builder_.on_build_single_replica_reply(job_.index_table_id_, job_.snapshot_version_, ret);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("report build finish failed", K(ret), K(tmp_ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

ObAsyncTask* ObRootService::ObIndexSSTableBuildTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObIndexSSTableBuildTask* task = NULL;
  if (NULL == buf || buf_size < (sizeof(*task))) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size));
  } else {
    task = new (buf) ObIndexSSTableBuildTask(
        job_, replica_picker_, global_index_builder_, sql_proxy_, oracle_sql_proxy_, abs_timeout_us_);
  }
  return task;
}

/////////////////////////
ObRootService::ObUpdateAllServerConfigTask::ObUpdateAllServerConfigTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(INT64_MAX);  // retry until success
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

ObAsyncTask* ObRootService::ObUpdateAllServerConfigTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObUpdateAllServerConfigTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer not large enough", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObUpdateAllServerConfigTask(root_service_);
  }
  return task;
}

/////////////////////////
int ObRootService::admin_clear_balance_task(const obrpc::ObAdminClearBalanceTaskArg& args)
{
  LOG_INFO("receive admin to clear balance task", K(args));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (args.zone_names_.count() > 0) {                                  // need clear zone's task
    for (int64_t i = 0; OB_SUCC(ret) && i < args.zone_names_.count(); ++i) {  // check zone exist
      ObZone zone = args.zone_names_.at(i);
      bool zone_exist = false;
      if (OB_FAIL(zone_manager_.check_zone_exist(zone, zone_exist))) {
        LOG_WARN("failed to check zone exists", K(ret), K(zone));
      } else if (!zone_exist) {
        ret = OB_ZONE_INFO_NOT_EXIST;
        LOG_WARN("zone not exists", K(ret), K(zone));
      }
    }
    if (OB_SUCC(ret) && args.tenant_ids_.count() > 0) {  // get the intersection task of zone and tenant.
      for (int64_t j = 0; OB_SUCC(ret) && j < args.tenant_ids_.count(); ++j) {
        if (OB_FAIL(rebalance_task_mgr_.clear_task(args.zone_names_, args.type_, args.tenant_ids_.at(j)))) {
          LOG_WARN("fail to flush balance info", K(ret));
        }
      }
    } else if (OB_SUCC(ret) &&
               OB_FAIL(rebalance_task_mgr_.clear_task(args.zone_names_, args.type_))) {  // clear task by zone
      LOG_INFO("fail to flush balance info", K(ret));
    }
  } else if (args.tenant_ids_.count() >= 0 &&
             OB_FAIL(rebalance_task_mgr_.clear_task(args.tenant_ids_, args.type_))) {  // clear task by tenant
    LOG_INFO("fail to flush balance info", K(ret));
  }
  ROOTSERVICE_EVENT_ADD("root_service", "admin_clear_balance_task", K(ret), K(args));
  return ret;
}

status::ObRootServiceStatus ObRootService::get_status() const
{
  return rs_status_.get_rs_status();
}

////////
int ObRootService::root_split_partition(const ObRootSplitPartitionArg& arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* schema = NULL;
  const uint64_t tenant_id = extract_tenant_id(arg.table_id_);
  LOG_INFO("start to split partition", K(ret), K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (is_tablegroup_id(arg.table_id_)) {
    if (OB_FAIL(batch_root_split_partition(arg))) {
      LOG_WARN("fail to split partition for tablegroup", K(ret), K(arg));
    }
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(arg.table_id_, schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(schema) || !schema->is_in_splitting()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("schema error", K(ret), K(arg), KP(schema));
  } else if (OB_FAIL(ddl_service_.split_partition(schema))) {
    LOG_WARN("fail to split partition", K(ret));
  }
  return ret;
}

int ObRootService::batch_root_split_partition(const ObRootSplitPartitionArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to split tablegroup partition", K(ret), K(arg));
  const ObTablegroupSchema* tablegroup_schema = NULL;
  const uint64_t tablegroup_id = arg.table_id_;
  const uint64_t tenant_id = extract_tenant_id(tablegroup_id);
  ObSchemaGetterGuard schema_guard;
  if (!is_tablegroup_id(tablegroup_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
  } else if (OB_ISNULL(tablegroup_schema) || !tablegroup_schema->is_in_splitting()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("schema error", K(ret), K(arg), KP(tablegroup_schema));
  } else if (OB_FAIL(ddl_service_.split_tablegroup_partition(schema_guard, tablegroup_schema))) {
    LOG_WARN("fail to split partition", K(ret), K(tablegroup_schema));
  }
  return ret;
}

int ObRootService::table_is_split(
    uint64_t tenant_id, const ObString& database_name, const ObString& table_name, const bool is_index)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_name, table_name, is_index, schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_name), K(table_name));
  } else if (OB_ISNULL(schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("invalid schema", K(ret));
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(table_name));
  } else if (schema->is_in_splitting()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is splitting", K(ret), K(schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter table while table is splitting");
  }
  return ret;
}

int ObRootService::table_allow_ddl_operation(const obrpc::ObAlterTableArg& arg)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* schema = NULL;
  ObSchemaGetterGuard schema_guard;
  const AlterTableSchema& alter_table_schema = arg.alter_table_schema_;
  const uint64_t tenant_id = alter_table_schema.get_tenant_id();
  const ObString& origin_database_name = alter_table_schema.get_origin_database_name();
  const ObString& origin_table_name = alter_table_schema.get_origin_table_name();
  schema_guard.set_session_id(arg.session_id_);
  if (arg.is_refresh_sess_active_time()) {
    // do nothing
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invali argument", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard in inner table failed", K(ret));
  } else if (OB_FAIL(
                 schema_guard.get_table_schema(tenant_id, origin_database_name, origin_table_name, false, schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(origin_database_name), K(origin_table_name));
  } else if (OB_ISNULL(schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("invalid schema", K(ret));
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(origin_database_name), to_cstring(origin_table_name));
  } else if (schema->is_in_splitting()) {
    // TODO ddl must not execute on splitting table due to split not unstable
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is physical or logical split can not split", K(ret), K(schema));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "table is in physial or logical split, ddl operation");
  } else if (schema->is_ctas_tmp_table()) {
    if (!alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::SESSION_ID)) {
      // to prevet alter table after failed to create table, the table is invisible.
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("try to alter invisible table schema", K(schema->get_session_id()), K(arg));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "try to alter invisible table");
    }
  }
  return ret;
}

// ask each server to update statistic
int ObRootService::update_stat_cache(const obrpc::ObUpdateStatCacheArg& arg)
{
  int ret = OB_SUCCESS;
  ObZone null_zone;
  ObSEArray<ObAddr, 8> server_list;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_manager_.get_alive_servers(null_zone, server_list))) {
    LOG_WARN("fail to get alive server", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); i++) {
      if (OB_FAIL(rpc_proxy_.to(server_list.at(i)).update_local_stat_cache(arg))) {
        LOG_WARN("fail to update table statistic", K(ret), K(server_list.at(i)));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObRootService::set_config_pre_hook(obrpc::ObAdminSetConfigArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret)
  {
    bool valid;
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, K(ret));
    } else if (0 == STRCMP(item->name_.ptr(), _RECYCLEBIN_OBJECT_PURGE_FREQUENCY)) {
      int64_t purge_interval = ObConfigTimeParser::get(item->value_.ptr(), valid);
      if (!valid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("set time error", K(item->value_.ptr()), K(valid), KR(ret));
      } else if (GCONF.major_freeze_duty_time.disable() && 0 == purge_interval) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("it is not allow that purge_interval is zero when daily merge is off", "item", *item, K(ret));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "_recyclebin_object_purge_frequency is zero when daily merge is off");
      }
    } else if (0 == STRCMP(item->name_.ptr(), CLOG_DISK_USAGE_LIMIT_PERCENTAGE)) {
      int64_t clog_disk_usage_limit_percent = ObConfigIntParser::get(item->value_.ptr(), valid);
      const int64_t clog_disk_utilization_threshold = ObServerConfig::get_instance().clog_disk_utilization_threshold;
      if (!valid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("clog_disk_usage_limit_percentage is invalid", K(item->value_.ptr()), K(valid), KR(ret));
      } else if (clog_disk_usage_limit_percent <= clog_disk_utilization_threshold) {
        // clog_disk_usage_limit_percent need larger than clog_disk_utilization_threshold
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("clog_disk_usage_limit_percentage should be greater than clog_disk_utilization_threshold",
            K(clog_disk_usage_limit_percent),
            K(clog_disk_utilization_threshold),
            KR(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
            "clog_disk_usage_limit_percentage, it should be greater than clog_disk_utilization_threshold");
      } else {
      }
    } else if (0 == STRCMP(item->name_.ptr(), CLOG_DISK_UTILIZATION_THRESHOLD)) {
      int64_t clog_disk_utilization_threshold = ObConfigIntParser::get(item->value_.ptr(), valid);
      const int64_t clog_disk_usage_limit_percent = ObServerConfig::get_instance().clog_disk_usage_limit_percentage;
      if (!valid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("clog_disk_utilization_threshold is invalid", K(item->value_.ptr()), K(valid), KR(ret));
      } else if (clog_disk_utilization_threshold >= clog_disk_usage_limit_percent) {
        // clog_disk_utilization_threshold need smaller than clog_disk_usage_limit_percentage
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("clog_disk_utilization_threshold should be less than clog_disk_usage_limit_percentage",
            K(clog_disk_usage_limit_percent),
            K(clog_disk_utilization_threshold),
            KR(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
            "clog_disk_utilization_threshold, it shoud be less than clog_disk_usage_limit_percentage");
      } else {
      }
    }
  }
  return ret;
}

	
int ObRootService::wakeup_auto_delete(const obrpc::ObAdminSetConfigItem *item)
{
  int ret = OB_SUCCESS;
  ObBackupDestOpt new_opt;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == item) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item is null", K(ret)); 
  } else if (0 == STRCMP(item->name_.ptr(), OB_STR_BACKUP_DEST_OPT)) {
    if (OB_FAIL(new_opt.init(false/* is_backup_backup */))) {
      LOG_WARN("failed init backup dest option", K(ret), KPC(item));
    } else if (true == new_opt.auto_delete_obsolete_backup_ && 0 != new_opt.recovery_window_) {
      backup_auto_delete_.wakeup();
      LOG_INFO("backup_dest_option parameters updated, wakeup backup_auto_delete", KPC(item)); 
    }
  } else if (0 == STRCMP(item->name_.ptr(), OB_STR_BACKUP_BACKUP_DEST_OPT)) {
    if (OB_FAIL(new_opt.init(true/* is_backup_backup */))) {
      LOG_WARN("failed init backup backup dest option", K(ret), KPC(item));
    } else if (true == new_opt.auto_delete_obsolete_backup_ && 0 != new_opt.recovery_window_) {
      backup_auto_delete_.wakeup();
      LOG_INFO("backup_backup_dest_option parameters updated, wakeup backup_auto_delete", KPC(item)); 
    }
  } else if (0 == STRCMP(item->name_.ptr(), OB_STR_AUTO_DELETE_EXPIRED_BACKUP)) {
    ObString value(item->value_.ptr());
    ObString false_str("FALSE");
    if (0 != value.case_compare(false_str)) {
      backup_auto_delete_.wakeup();
      LOG_INFO("auto_delete_expired_backup parameters updated, wakeup backup_auto_delete", KPC(item));
    } 
  } else if (0 == STRCMP(item->name_.ptr(), OB_STR_BACKUP_RECORVERTY_WINDOW)) {
    ObString value(item->value_.ptr());
    ObString zero_str("0");
    if (0 != value.case_compare(zero_str)) {
      backup_auto_delete_.wakeup();
      LOG_INFO("backup_recovery_window parameters updated, wakeup backup_auto_delete", KPC(item));
    }  
  }
  return ret;
}

int ObRootService::set_config_post_hook(const obrpc::ObAdminSetConfigArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  }
  FOREACH_X(item, arg.items_, OB_SUCCESS == ret)
  {
    if (item->name_.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty config name", "item", *item, K(ret));
    } else if (0 == STRCMP(item->name_.ptr(), ENABLE_REBALANCE) ||
               0 == STRCMP(item->name_.ptr(), ENABLE_REREPLICATION)) {
      ObString value(item->value_.ptr());
      ObString false_str("FALSE");
      ObString zero_str("0");
      ObAdminClearBalanceTaskArg::TaskType type = ObAdminClearBalanceTaskArg::ALL;
      ObSEArray<uint64_t, 1> tenant_ids;
      if (0 != value.case_compare(false_str) && 0 != value.case_compare(zero_str)) {
        // wake immediately
        root_balancer_.wakeup();
      } else if (OB_FAIL(rebalance_task_mgr_.clear_task(tenant_ids, type))) {
        LOG_WARN("fail to clear balance task", K(ret));
      } else {
        if (0 == STRCMP(item->name_.ptr(), ENABLE_REREPLICATION)) {
          root_balancer_.reset_task_count();
          LOG_INFO("clear balance task after modify balance config", K(ret), K(arg));
        }
      }
    } else if (0 == STRCMP(item->name_.ptr(), MERGER_CHECK_INTERVAL)) {
      daily_merge_scheduler_.wakeup();
    } else if (0 == STRCMP(item->name_.ptr(), ENABLE_AUTO_LEADER_SWITCH)) {
      ObString value(item->value_.ptr());
      ObString false_str("FALSE");
      ObString zero_str("0");
      if (0 != value.case_compare(false_str) && 0 != value.case_compare(zero_str)) {
        leader_coordinator_.wakeup();
      }
    } else if (0 == STRCMP(item->name_.ptr(), OBCONFIG_URL)) {
      int tmp_ret = OB_SUCCESS;
      bool force_update = true;
      if (OB_SUCCESS != (tmp_ret = submit_update_rslist_task(force_update))) {
        LOG_WARN("fail to submit update rs list task", KR(ret), K(tmp_ret));
      }
      LOG_INFO("obconfig_url parameters updated, force submit update rslist task", KR(tmp_ret), KPC(item));
    } else if (0 == STRCMP(item->name_.ptr(), SCHEMA_HISTORY_RECYCLE_INTERVAL)) {
      schema_history_recycler_.wakeup();
      LOG_INFO("schema_history_recycle_interval parameters updated, wakeup schema_history_recycler", KPC(item));
    } else if (OB_FAIL(wakeup_auto_delete(item))) {
      LOG_WARN("failed to wakeup auto delete", K(ret), KPC(item));
    }
  }
  return ret;
}

int ObRootService::create_sys_index_table()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to create sys_index");
  int64_t affected_rows = 0;
  ObSqlString sql_enable_sys_table_ddl;
  ObSqlString sql_disable_ddl;
  ObSqlString sql_create_index;
  ObSqlString sql_enable_ddl;

  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();

  // the first index of sys table's table_id is scheduled to decrease from OB_MAX_SYS_TABLE_ID
  const uint64_t index_table_id = (OB_SYS_TENANT_ID << OB_TENANT_ID_SHIFT) | (OB_MAX_SYS_TABLE_ID - 1);
  if (OB_FAIL(sql_enable_sys_table_ddl.assign_fmt("alter system set enable_sys_table_ddl=true"))) {
    LOG_WARN("sql_enable_sys_table_ddl assign failed", K(ret));
  } else if (OB_FAIL(sql_disable_ddl.assign_fmt("alter system set enable_sys_table_ddl=false"))) {
    LOG_WARN("sql_disable_ddl assign failed", K(ret));
  } else if (OB_FAIL(
                 sql_create_index.assign_fmt("create index if not exists idx_data_table_id on "
                                             "oceanbase.__all_table_history(data_table_id) index_table_id=%lu local",
                     index_table_id))) {
    LOG_WARN("sql_create_index assign failed", K(ret));
  } else if (OB_FAIL(sql_enable_ddl.assign_fmt("alter system set enable_ddl=true"))) {
    LOG_WARN("sql_enable_ddl assign failed", K(ret));
  }

  uint32_t retry_time = 0;
  const uint32_t MAX_RETRY_TIMES = 60;                 // the default timeout of bootstrap is 300s
  const uint64_t SLEEP_INTERVAL_US = 5 * 1000 * 1000;  // 5s
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql_enable_sys_table_ddl.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql_enable_sys_table_ddl.ptr()));
    } else if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql_enable_ddl.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql_enable_ddl.ptr()));
    } else {
      usleep(SLEEP_INTERVAL_US);  // 5s, for waiting enable_sys_table_ddl/enable_ddl effective
      if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, sql_create_index.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql_create_index.ptr()));
      }
    }

    if (OB_SUCC(ret)) {
      break;
    } else if (OB_SERVER_IS_INIT == ret || OB_OP_NOT_ALLOW == ret) {
      // OB_OP_NOT_ALLOW: enable_sys_table_ddl is not valid
      // OB_SERVER_IS_INIT: major_freeze_done_ has not updated
      LOG_WARN("Can not process ddl request, and retry to create index", K(ret), K(sql_create_index.ptr()));
      ret = OB_SUCCESS;
      usleep(SLEEP_INTERVAL_US);
      if (++retry_time >= MAX_RETRY_TIMES) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to check enable_sys_table_ddl or major_freeze_done_ over 5min", K(ret));
        break;
      }
    } else {
      LOG_WARN("execute sql failed", K(ret), K(sql_create_index.ptr()));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = sql_proxy.write(OB_SYS_TENANT_ID, sql_disable_ddl.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(tmp_ret), K(sql_disable_ddl.ptr()));
  } else {
    LOG_INFO("sql disable ddl succ", K(sql_disable_ddl.ptr()));
  }

  if (OB_SUCC(ret)) {
    ret = tmp_ret;
  }
  LOG_INFO("wmy debug: finish to create sys_index", K(ret));
  return ret;
}

// ensure execute on DDL thread
int ObRootService::force_create_sys_table(const obrpc::ObForceCreateSysTableArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(
                 ObUpgradeUtils::force_create_tenant_table(arg.tenant_id_, arg.table_id_, arg.last_replay_log_id_))) {
    LOG_WARN("fail to force create tenant table", K(ret), K(arg));
  }
  LOG_INFO("force create sys table", K(ret), K(arg));
  return ret;
}

// set tenant's locality
// TODO
//  1. set all locality of normal tenant to DEFAULT first.
//  2. verify that replica distribution satifies the new locality
int ObRootService::force_set_locality(const obrpc::ObForceSetLocalityArg& arg)
{
  LOG_INFO("receive force set locality arg", K(arg));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema* tenant_schema = NULL;
  const uint64_t tenant_id = arg.exec_tenant_id_;
  if (!inited_) {
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(
                 ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard with version in inner table", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret));
  } else {
    ObTenantSchema new_tenant;
    if (OB_FAIL(new_tenant.assign(*tenant_schema))) {
      LOG_WARN("fail to assgin tenant schema", K(ret), KPC(tenant_schema));
    } else if (OB_FAIL(new_tenant.set_locality(arg.locality_))) {
      LOG_WARN("fail to set locality", K(ret), K(arg));
    } else if (OB_FAIL(new_tenant.set_previous_locality(ObString("")))) {
      LOG_WARN("fail to reset previous locality", K(ret), K(arg));
    } else if (OB_FAIL(ddl_service_.force_set_locality(schema_guard, new_tenant))) {
      LOG_WARN("fail to force set locality", K(ret), K(new_tenant));
    }
  }
  LOG_INFO("force set locality", K(arg));
  return ret;
}

int ObRootService::reload_unit_replica_counter_task()
{
  int ret = OB_SUCCESS;
  const bool did_repeat = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_queue_.add_timer_task(reload_unit_replica_counter_task_,
                 ObReloadUnitReplicaCounterTask::REFRESH_UNIT_INTERVAL,
                 did_repeat))) {
    LOG_WARN("fail to add timer task", K(ret));
  } else {
    LOG_INFO("succeed to add unit replica couter task");
  }
  return ret;
}

int ObRootService::clear_bootstrap()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSchemaService* schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else {
      schema_service->set_in_bootstrap(false);
    }
  }
  return ret;
}

int ObRootService::get_is_in_bootstrap(bool& is_bootstrap) const
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
    ObSchemaService* schema_service = schema_service_->get_schema_service();
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else {
      is_bootstrap = schema_service->is_in_bootstrap();
    }
  }
  return ret;
}

int ObRootService::log_nop_operation(const obrpc::ObDDLNopOpreatorArg& arg)
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

int ObRootService::get_schema_split_version(int64_t& version_in_core_table, int64_t& version_in_ddl_table)
{
  int ret = OB_SUCCESS;
  version_in_core_table = OB_INVALID_VERSION;
  version_in_ddl_table = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // get version from __all_core_table
    ObGlobalStatProxy proxy(sql_proxy_);
    if (OB_FAIL(proxy.get_split_schema_version(version_in_core_table))) {
      LOG_WARN("fail to get split schema_version", K(ret), K(version_in_core_table));
    }
    // get version from __all_ddl_operation
    if (OB_SUCC(ret)) {
      ObSqlString sql;
      HEAP_VAR(ObMySQLProxy::MySQLResult, res)
      {
        common::sqlclient::ObMySQLResult* result = NULL;
        if (OB_FAIL(sql.assign_fmt("SELECT schema_version FROM %s "
                                   "WHERE operation_type = %d",
                OB_ALL_DDL_OPERATION_TNAME,
                OB_DDL_FINISH_SCHEMA_SPLIT))) {
          LOG_WARN("fail to append sql", K(ret));
        } else if (OB_FAIL(sql_proxy_.read(res, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(sql));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get sql result", K(ret));
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get result", K(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", version_in_ddl_table, int64_t);
          if (OB_SUCC(ret) && OB_ITER_END != (ret = result->next())) {
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
            LOG_WARN("fail to get next row or more than onw row", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    // check result
    if (OB_SUCC(ret)) {
      if (version_in_ddl_table < 0 && version_in_core_table >= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("version_in_ddl_table invalid", K(ret), K(version_in_ddl_table), K(version_in_core_table));
      }
    }
  }
  return ret;
}
int ObRootService::finish_schema_split(const obrpc::ObFinishSchemaSplitArg& arg)
{
  int ret = OB_SUCCESS;
  if (ObRsJobType::JOB_TYPE_SCHEMA_SPLIT == arg.type_) {
    if (OB_FAIL(finish_schema_split_v1(arg))) {
      LOG_WARN("fail to finish schema split", K(ret), K(arg));
    }
  } else if (ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2 == arg.type_) {
    if (OB_FAIL(finish_schema_split_v2(arg))) {
      LOG_WARN("fail to finish schema split", K(ret), K(arg));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

// the interface is reentrant, after migrate data completely, do the following in turn:
//  1. write special DDL on sys tenant's __all_ddl_operation, it indicates split finish(for liboblog)
//  2. write split_schema_version on __all_core_table, it indicates split finish(for observer)
//  3. RS refresh schema with new mode.
//  4. set GCTX.split_schema_version_, heartbeat will trigger observer refresh schema with new mode.
int ObRootService::finish_schema_split_v1(const obrpc::ObFinishSchemaSplitArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else {
    int64_t version_in_memory = GCTX.split_schema_version_;
    int64_t version_in_core_table = OB_INVALID_VERSION;
    int64_t version_in_ddl_table = OB_INVALID_VERSION;
    if (OB_FAIL(get_schema_split_version(version_in_core_table, version_in_ddl_table))) {
      LOG_WARN("fail to check schema split", K(ret));
    } else if (version_in_ddl_table >= version_in_core_table && version_in_core_table >= version_in_memory) {
      // write ddl operation
      if (OB_SUCC(ret)) {
        if (version_in_ddl_table >= 0) {
          // skip
        } else if (OB_FAIL(ddl_service_.finish_schema_split(version_in_ddl_table))) {
          LOG_WARN("fail to set schema split finish", K(ret), K(version_in_ddl_table));
        } else {
          LOG_INFO("log schema split finish", K(ret), K(version_in_ddl_table));
        }
      }

      // write split schema verison in __all_core_table
      if (OB_SUCC(ret)) {
        ObGlobalStatProxy global_stat_proxy(sql_proxy_);
        if (version_in_core_table >= 0 && version_in_core_table != version_in_ddl_table) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid version", K(ret), K(version_in_ddl_table), K(version_in_core_table), K(version_in_memory));
        } else if (version_in_core_table >= 0 && version_in_core_table == version_in_ddl_table) {
          // skip
        } else if (OB_FAIL(global_stat_proxy.set_split_schema_version(version_in_ddl_table))) {
          LOG_WARN("fail to set split schema version", K(ret), K(version_in_ddl_table));
        } else {
          version_in_core_table = version_in_ddl_table;
          LOG_INFO("set split_schema_verison in table", K(ret), K(version_in_core_table));
        }
      }

      // refresh schema in new mode & set GCTX.split_schema_version
      if (OB_SUCC(ret)) {
        if (version_in_memory >= 0 && version_in_core_table != version_in_memory) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid version", K(ret), K(version_in_ddl_table), K(version_in_core_table), K(version_in_memory));
        } else if (version_in_memory >= 0 && version_in_core_table == version_in_memory) {
          // skip
        } else {
          ObSEArray<uint64_t, 1> tenant_ids;
          if (OB_ISNULL(schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema_service is null", K(ret));
          } else if (OB_FAIL(schema_service_->refresh_and_add_schema(tenant_ids, true /*use new mode*/))) {
            LOG_WARN("fail to refresh schema with new mode", K(ret));
          } else {
            GCTX.set_split_schema_version(version_in_core_table);
          }
        }
      }

      // always publish schema
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_service_.publish_schema(OB_SYS_TENANT_ID))) {
          LOG_WARN("fail to publish schema", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K(version_in_ddl_table), K(version_in_core_table), K(version_in_memory));
    }
  }
  return ret;
}

int ObRootService::finish_schema_split_v2(const obrpc::ObFinishSchemaSplitArg& arg)
{
  int ret = OB_SUCCESS;
  bool enable_ddl = GCONF.enable_ddl;
  bool enable_sys_table_ddl = GCONF.enable_sys_table_ddl;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = arg.tenant_id_;
  bool pass = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (enable_ddl || enable_sys_table_ddl) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("ddl should disable now", K(ret), K(enable_ddl), K(enable_sys_table_ddl));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(arg));
    // refresh schema split status by schema
  } else if (OB_FAIL(schema_service_->check_tenant_can_use_new_table(tenant_id, pass))) {
    LOG_WARN("fail to check tenant can use new table", K(ret), K(tenant_id));
  } else if (pass) {
    // do nothing
  } else {
    ObDDLSQLTransaction trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, sql_proxy_);
    if (OB_FAIL(trans.start(&sql_proxy_))) {
      LOG_WARN("failed to start trans, ", K(ret));
    } else if (OB_FAIL(ddl_operator.finish_schema_split_v2(trans, tenant_id))) {
      LOG_WARN("fail to log ddl operation", K(ret), K(tenant_id));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      // sys tenant writes __all_core_table additionally
      ObGlobalStatProxy global_stat_proxy(trans);
      int64_t schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_->get_new_schema_version(tenant_id, schema_version))) {
        LOG_WARN("fail to get new schema version", K(ret), K(tenant_id));
      } else if (OB_FAIL(global_stat_proxy.set_split_schema_version_v2(schema_version))) {
        LOG_WARN("fail to set split schema version", K(ret), K(schema_version));
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }
    // always publish schema
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.publish_schema(tenant_id))) {
        LOG_WARN("fail to publish schema", K(ret));
      }
    }
  }
  return ret;
}

// if tenant_id =  OB_INVALID_TENANT_ID, indicates refresh all tenants's schema;
// otherwise, refresh specify tenant's schema. ensure schema_version not fallback by outer layer logic.
int ObRootService::broadcast_schema(const obrpc::ObBroadcastSchemaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receieve broadcast_schema request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret), KP_(schema_service));
  } else {
    ObRefreshSchemaInfo schema_info;
    ObSchemaService* schema_service = schema_service_->get_schema_service();
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
    const obrpc::ObGetSchemaArg& arg, obrpc::ObTenantSchemaVersions& tenant_schema_versions)
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
  } else if (OB_FAIL(
                 ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    int64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t schema_version = 0;
    const ObTenantSchema* tenant_schema = NULL;
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
      ObSchemaGetterGuard tenant_schema_guard;
      tenant_id = tenant_ids.at(i);
      schema_version = 0;
      if (OB_SYS_TENANT_ID == tenant_id) {
        if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, schema_version))) {
          LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
        }
      } else {
        // get newest schema_version from inner table.
        ObRefreshSchemaStatus schema_status;
        schema_status.tenant_id_ = GCTX.is_schema_splited() ? tenant_id : OB_INVALID_TENANT_ID;
        int64_t version_in_inner_table = OB_INVALID_VERSION;
        bool is_restore = false;
        if (OB_FAIL(schema_service_->check_tenant_is_restore(&schema_guard, tenant_id, is_restore))) {
          LOG_WARN("fail to check tenant is restore", KR(ret), K(tenant_id));
        } else if (is_restore) {
          ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
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
        } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, schema_version))) {
          LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
        } else if (schema_version < version_in_inner_table) {
          ObArray<uint64_t> tenant_ids;
          if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push back tenant_id", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service_->refresh_and_add_schema(tenant_ids))) {
            LOG_WARN("fail to refresh schema", K(ret), K(tenant_id));
          } else if (OB_FAIL(schema_service_->get_tenant_refreshed_schema_version(tenant_id, schema_version))) {
            LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
          } else if (schema_version < version_in_inner_table) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("local version is still less than version in table",
                K(ret),
                K(tenant_id),
                K(schema_version),
                K(version_in_inner_table));
          } else {
          }
        } else {
        }
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
    }  // end for
  }
  return ret;
}

int ObRootService::generate_user(const ObClusterType& cluster_type, const char* user_name, const char* user_passwd)
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
  } else if (PRIMARY_CLUSTER != cluster_type) {
    LOG_INFO("slave cluster, no need to create user", K(cluster_type));
  } else if (OB_FAIL(sql::ObCreateUserExecutor::encrypt_passwd(passwd, encry_passwd, enc_buf, ENC_BUF_LEN))) {
    LOG_WARN("Encrypt passwd failed", K(ret));
  } else if (OB_FAIL(ObDDLSqlGenerator::gen_create_user_sql(
                 ObAccountArg(user_name, OB_SYS_HOST_NAME), encry_passwd, ddl_stmt_str))) {
    LOG_WARN("fail to gen create user sql", KR(ret));
  } else if (OB_FAIL(sql_proxy_.write(ddl_stmt_str.ptr(), affected_row))) {
    LOG_WARN("execute sql failed", K(ret), K(ddl_stmt_str));
  } else {
    LOG_INFO("create user success", K(user_name), K(affected_row));
  }
  ddl_stmt_str.reset();
  if (OB_FAIL(ret) || PRIMARY_CLUSTER != cluster_type) {
    // nothing todo
  } else if (OB_FAIL(ddl_stmt_str.assign_fmt("grant select on *.* to '%s'", user_name))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(sql_proxy_.write(ddl_stmt_str.ptr(), affected_row))) {
    LOG_WARN("fail to write", KR(ret), K(ddl_stmt_str));
  } else {
    LOG_INFO("grant privilege success", K(ddl_stmt_str));
  }
  return ret;
}

int ObRootService::get_recycle_schema_versions(
    const obrpc::ObGetRecycleSchemaVersionsArg& arg, obrpc::ObGetRecycleSchemaVersionsResult& result)
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
    LOG_WARN("should be standby cluster and rs in service", KR(ret), K(is_standby), K(in_service));
  } else if (OB_FAIL(schema_history_recycler_.get_recycle_schema_versions(arg, result))) {
    LOG_WARN("fail to get recycle schema versions", KR(ret), K(arg));
  }
  LOG_INFO("get recycle schema versions", KR(ret), K(arg), K(result));
  return ret;
}

int ObRootService::check_merge_finish(const obrpc::ObCheckMergeFinishArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive check_merge_finish request", K(arg));
  int64_t last_merged_version = 0;
  share::ObSimpleFrozenStatus frozen_status;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(zone_manager_.get_global_last_merged_version(last_merged_version))) {
    LOG_WARN("fail to get last merged version", K(ret));
  } else if (OB_FAIL(freeze_info_manager_.get_freeze_info(0, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret));
  } else if (frozen_status.frozen_version_ != last_merged_version) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't alter column when major freeze is not finished", K(ret));
  } else if (arg.frozen_version_ != last_merged_version) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("frozen_version is not new enough", K(ret), K(arg), K(last_merged_version), K(frozen_status));
  } else if (OB_FAIL(ddl_service_.check_all_server_frozen_version(last_merged_version))) {
    LOG_WARN("fail to check all servers's frozen version", K(ret), K(last_merged_version));
  }
  LOG_INFO("check_merge_finish finish", K(ret), K(last_merged_version), K(frozen_status), K(arg));
  return ret;
}

int ObRootService::do_profile_ddl(const obrpc::ObProfileDDLArg& arg)
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

// backup and restore
int ObRootService::force_drop_schema(const obrpc::ObForceDropSchemaArg& arg)
{
  LOG_INFO("receive force drop schema request", K(arg));
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  int64_t tenant_id = arg.exec_tenant_id_;
  // recycle_schema_version = 0 indicates close backup, delete all schema in delay delete
  int64_t recycle_schema_version = arg.recycle_schema_version_;
  int64_t reserved_schema_version = OB_INVALID_VERSION;
  bool is_delay_delete = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root_service not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(arg));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_delay_delete_schema_version(
                 tenant_id, *schema_service_, is_delay_delete, reserved_schema_version))) {
    LOG_WARN("fail to get delay delete schema version", K(ret), K(arg));
  } else if (0 == recycle_schema_version && is_delay_delete) {
    ret = OB_STOP_DROP_SCHEMA;
    LOG_WARN("physical backup switch is on, try later", K(ret), K(arg), K(reserved_schema_version));
  } else if (OB_FAIL(ddl_service_.force_drop_schema(arg))) {
    LOG_WARN("ddl service failed to drop table", K(arg), K(ret));
  }
  LOG_INFO("force drop schema finish", K(ret), K(arg));
  ROOTSERVICE_EVENT_ADD("ddl",
      "force_drop_schema",
      "tenant_id",
      tenant_id,
      "schema_id",
      arg.schema_id_,
      "partition_cnt",
      arg.partition_ids_.count(),
      "recycle_schema_version",
      recycle_schema_version,
      "result",
      ret);
  return ret;
}

int ObRootService::rebuild_index_in_restore(const obrpc::ObRebuildIndexInRestoreArg& arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema* tenant_schema = NULL;
  ObArray<uint64_t> index_ids;
  uint64_t tenant_id = arg.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), K(tenant_id));
  } else if (!tenant_schema->is_restore()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("restore tenant use only", K(ret), KPC(tenant_schema));
  } else if (OB_FAIL(schema_guard.get_tenant_unavailable_index(tenant_id, index_ids))) {
    LOG_WARN("fail to get tenant unavailable index", K(ret), K(tenant_id));
  } else {
    const ObTableSchema* index_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); i++) {
      const uint64_t index_id = index_ids.at(i);
      if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(index_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("fail to get index schema", K(ret), K(index_id));
      } else if (INDEX_STATUS_UNAVAILABLE != index_schema->get_index_status()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index status is invalid", K(ret), KPC(index_schema));
      } else if (index_schema->is_index_local_storage()) {
        ObRSBuildIndexTask task;
        if (OB_FAIL(task.init(index_schema->get_table_id(),
                index_schema->get_data_table_id(),
                index_schema->get_schema_version(),
                &ddl_service_))) {
          LOG_WARN("fail to init ObRSBuildIndexTask", K(ret), "index_id", index_schema->get_table_id());
        } else if (OB_FAIL(ObRSBuildIndexScheduler::get_instance().push_task(task))) {
          if (OB_ENTRY_EXIST == ret) {
            ret = OB_SUCCESS;  // ensure reentry
          } else {
            LOG_WARN("fail to add task into ObRSBuildIndexScheduler", K(ret));
          }
        }
      } else if (index_schema->is_global_index_table()) {
        if (OB_FAIL(global_index_builder_.submit_build_global_index_task(index_schema))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_SUCCESS;  // ensure reentry
          } else {
            LOG_WARN("fail to add task into ObGlobalIndexBuilder", K(ret));
          }
        }
      } else {
        // not support, just skip
      }
    }
  }
  return ret;
}

int ObRootService::handle_archive_log(const obrpc::ObArchiveLogArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("handle_archive_log", K(arg));
    if (OB_FAIL(wait_refresh_config())) {
      LOG_WARN("Failed to wait refresh schema", K(ret));
    } else if (OB_FAIL(log_archive_scheduler_.handle_enable_log_archive(arg.enable_))) {
      LOG_WARN("failed to handle_enable_log_archive", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::handle_user_ttl(const obrpc::ObTableTTLArg& arg)
{
  int ret = OB_SUCCESS;
  ObTTLTaskType user_ttl_req_type = static_cast<ObTTLTaskType>(arg.cmd_code_);

  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(TTLMGR.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  }

  LOG_INFO("handle user ttl cmd.", K(user_ttl_req_type), K(tenant_ids.count()), K(ret));

  for (size_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); ++i) {
    uint64_t tenant_id = tenant_ids.at(i);
    if (tenant_id == OB_SYS_TENANT_ID) {
      // do nothing
    } else if (OB_FAIL(TTLMGR.add_ttl_task(tenant_ids.at(i), static_cast<ObTTLTaskType>(arg.cmd_code_)))) {
      LOG_WARN("failed add ttl task", K(tenant_ids.at(i)), K(ret), K(user_ttl_req_type));
    } else {
      LOG_INFO("handle user ttl cmd.", K(user_ttl_req_type), K(tenant_id));
    }
  }

  return ret;
}

int ObRootService::ttl_response(const obrpc::ObTTLResponseArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t status = static_cast<int64_t>(arg.task_status_);
  LOG_INFO("recieve ttl response begin", K(arg));
  if (OB_FAIL(TTLMGR.process_tenant_task_rsp(arg.tenant_id_, arg.task_id_, status, arg.server_addr_))) {
    LOG_WARN("fail to process ttl rsp", K(arg), K(ret));
  } else {
    LOG_INFO("success to process ttl response", K(arg), K(ret));
  }
  LOG_INFO("recieve ttl response end", K(ret));
  return ret;
}

int ObRootService::handle_backup_database(const obrpc::ObBackupDatabaseArg& in_arg)
{
  int ret = OB_SUCCESS;
  ObBackupScheduler backup_scheduler;
  obrpc::ObBackupDatabaseArg arg = in_arg;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(wait_refresh_config())) {
    LOG_WARN("failed to wait_refresh_schema", K(ret));
  } else if (arg.tenant_id_ != OB_SYS_TENANT_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support cluster backup now", K(ret), K(arg));
  } else if (FALSE_IT(arg.tenant_id_ = 0)) {
  } else if (OB_FAIL(backup_scheduler.init(
                 arg, *schema_service_, sql_proxy_, root_backup_, freeze_info_manager_, restore_point_service_))) {
    LOG_WARN("failed to init backup scheduler", K(ret));
  } else if (OB_FAIL(backup_scheduler.start_schedule_backup())) {
    LOG_WARN("failed to schedule backup", K(ret));
  }
  FLOG_INFO("handle_backup_database", K(ret), K(arg));
  return ret;
}

int ObRootService::handle_validate_database(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObValidateScheduler validate_scheduler;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t backup_set_id = 0;
  // TODO concurrency is not allowed

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support tenant level validate", K(ret));
  } else if (ObBackupManageArg::VALIDATE_DATABASE != arg.type_ || 0 != backup_set_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(validate_scheduler.init(tenant_id, backup_set_id, sql_proxy_, root_validate_))) {
    LOG_WARN("failed to init validate scheduler", K(ret));
  } else if (OB_FAIL(validate_scheduler.start_schedule_validate())) {
    LOG_WARN("failed to schedule validate", K(ret));
  } else {
    LOG_INFO("handle_validate_database", K(arg));
  }
  return ret;
}

int ObRootService::handle_validate_backupset(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObValidateScheduler validate_scheduler;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t backup_set_id = arg.value_;
  // TODO : concurrency is not allowed

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support tenant level validate", K(ret));
  } else if (ObBackupManageArg::VALIDATE_BACKUPSET != arg.type_ || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(validate_scheduler.init(tenant_id, backup_set_id, sql_proxy_, root_validate_))) {
    LOG_WARN("failed to init validate scheduler", K(ret));
  } else if (OB_FAIL(validate_scheduler.start_schedule_validate())) {
    LOG_WARN("failed to start schedule validate", K(ret));
  } else {
    LOG_INFO("handle_validate_backupset", K(arg));
  }
  return ret;
}

int ObRootService::handle_cancel_validate(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObCancelValidateScheduler cancel_scheduler;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t job_id = arg.value_;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support tenant level cancel", K(ret));
  } else if (ObBackupManageArg::CANCEL_VALIDATE != arg.type_ || job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(cancel_scheduler.init(tenant_id, job_id, sql_proxy_, root_validate_))) {
    LOG_WARN("failed to init cancel validate scheduler", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(cancel_scheduler.start_schedule_cancel_validate())) {
    LOG_WARN("failed to start schedule cancel validate", KR(ret));
  } else {
    LOG_INFO("handle_cancel_validate success", KR(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_backup_archive_log(const obrpc::ObBackupArchiveLogArg& arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupInfoManager manager;
  ObLogArchiveBackupInfoMgr archive_mgr;
  ObBackupDest backup_dest;
  char backup_backup_dest_buf[OB_MAX_BACKUP_DEST_LENGTH];
  ObArray<ObBackupBackupPieceJobInfo> job_list;
  bool has_piece_mode = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(wait_refresh_config())) {
    LOG_WARN("failed to wait_refresh_schema", K(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_buf, sizeof(backup_backup_dest_buf)))) {
    LOG_WARN("failed to set backup dest buf", K(ret));
  } else if (0 == strlen(backup_backup_dest_buf)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("handle backup archive log with empty backup dest is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "handle backup archive log with empty backup dest is");
  } else if (OB_FAIL(backup_dest.set(backup_backup_dest_buf))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_backup_dest_buf));
  } else if (OB_FAIL(trans.start(&sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(
            archive_mgr.check_has_piece_mode_archive_in_dest(trans, backup_dest, OB_SYS_TENANT_ID, has_piece_mode))) {
      LOG_WARN("failed to check has piece mode archive in dest", KR(ret), K(backup_dest));
    } else if (has_piece_mode) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cannot start backup backup in round mode", KR(ret), K(backup_backup_dest_buf));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup archive log in piece data directory");
    } else if (OB_FAIL(ObBackupBackupPieceJobOperator::get_one_job(trans, job_list))) {
      LOG_WARN("failed to get backup backuppiece job", KR(ret));
    } else if (OB_UNLIKELY(!job_list.empty())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can not start backup archivelog if backup backuppiece job exist", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup archive log if backup backuppiece job exist");
    } else if (OB_FAIL(manager.init(OB_SYS_TENANT_ID, sql_proxy_))) {
      LOG_WARN("failed to init backup info manager", KR(ret));
    } else if (OB_FAIL(manager.update_enable_auto_backup_archivelog(OB_SYS_TENANT_ID, arg.enable_, trans))) {
      LOG_WARN("failed to update enable auto backup archivelog", KR(ret));
    }
  }
  if (trans.is_started()) {
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "commit", OB_SUCC(ret), K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_archive_log_.handle_enable_auto_backup(arg.enable_))) {
      LOG_WARN("failed to handle enable auto backup", KR(ret), K(arg));
    } else {
      backup_archive_log_.wakeup();
      LOG_INFO("handle backup archivelog", K(arg));
    }
  }
  return ret;
}

int ObRootService::handle_backup_archive_log_batch_res(const obrpc::ObBackupArchiveLogBatchRes& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive backup archive log batch res", K(arg));
  ObArray<ObPGKey> pg_list;
  ObArray<ObPGKey> failed_list;
  bool interrupted = false;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t archive_round = arg.archive_round_;
  const int64_t checkpoint_ts = arg.checkpoint_ts_;
  const int64_t piece_id = arg.piece_id_;
  const int64_t job_id = arg.job_id_;
  const common::ObAddr& server = arg.server_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is not valid", K(ret), K(arg));
  } else if (OB_FAIL(backup_archive_log_.set_server_free(server))) {
    LOG_WARN("failed to set server free", KR(ret), K(server));
  } else if (FALSE_IT(interrupted = arg.is_interrupted())) {
  } else if (interrupted && OB_FAIL(backup_archive_log_.set_round_interrupted(archive_round))) {
    LOG_WARN("failed to set current round interrupted", KR(ret), K(archive_round));
  } else if (interrupted) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    LOG_INFO("backup archive log interrupted", KR(ret), K(tenant_id), K(archive_round));
  } else if (OB_FAIL(arg.get_failed_pg_list(failed_list))) {
    LOG_WARN("failed to get failed pg list", KR(ret), K(piece_id));
  } else if (OB_FAIL(backup_archive_log_.redo_failed_pg_tasks(tenant_id, piece_id, job_id, failed_list))) {
    LOG_WARN("failed to redo failed pg tasks", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.get_finished_pg_list(pg_list))) {
    LOG_WARN("failed to get finished pg list", KR(ret), K(piece_id));
  } else if (pg_list.empty()) {
    LOG_INFO("no finished pg list", K(arg));
  } else {
    if (0 == piece_id) {
      if (OB_FAIL(backup_archive_log_.set_pg_finish(archive_round, checkpoint_ts, tenant_id, pg_list))) {
        LOG_WARN("failed to set pg finish move", KR(ret), K(archive_round), K(tenant_id));
      } else {
        LOG_INFO("set pg finish", K(archive_round), K(checkpoint_ts), K(tenant_id));
      }
    } else {
      if (OB_FAIL(backup_archive_log_.mark_piece_pg_task_finished(tenant_id, piece_id, job_id, pg_list))) {
        LOG_WARN("failed to set pg finish", KR(ret), K(tenant_id), K(job_id), K(piece_id));
      } else {
        LOG_INFO("mark piece pg task finished", K(tenant_id), K(piece_id), K(pg_list));
      }
    }
  }

  if (OB_SUCC(ret)) {
    backup_archive_log_.wakeup();
    LOG_INFO("handle backup archive log batch res", K(arg));
  }
  return ret;
}

int ObRootService::handle_backup_backupset(const obrpc::ObBackupBackupsetArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupBackupsetScheduler scheduler;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t backup_set_id = arg.backup_set_id_;
  const char* backup_dest = arg.backup_backup_dest_;
  const int64_t max_backup_times = arg.max_backup_times_;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup backupset get invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(wait_refresh_config())) {
    LOG_WARN("failed to wait_refresh_schema", K(ret));
  } else {
    if (OB_SUCC(ret) && OB_SYS_TENANT_ID != tenant_id) {
      if (0 == STRNCMP(arg.backup_backup_dest_, "", OB_MAX_BACKUP_DEST_LENGTH)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant level backup backup and backup_backup_dest is not empty is not supported", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant level backup backup and backup_backup_dest is not empty");
      } else {
        char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
        char tmp_backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
        if (OB_FAIL(GCONF.backup_dest.copy(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
          LOG_WARN("failed to copy backup dest", KR(ret));
        } else if (OB_FAIL(GCONF.backup_backup_dest.copy(tmp_backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
          LOG_WARN("failed to copy backup dest", KR(ret));
        } else if (0 == STRNCMP(tmp_backup_dest_str, arg.backup_backup_dest_, OB_MAX_BACKUP_DEST_LENGTH) ||
                   0 == STRNCMP(tmp_backup_backup_dest_str, arg.backup_backup_dest_, OB_MAX_BACKUP_DEST_LENGTH)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant level backup backup use same backup backup dest as gconf not supported", KR(ret));
          LOG_USER_ERROR(
              OB_NOT_SUPPORTED, "tenant level backup backup use same backup backup dest as gconf not supported");
        } else {
          share::ObBackupDest dest;
          bool is_empty_dir = false;
          ObStorageUtil util(false /*need_retry*/);
          if (OB_FAIL(dest.set(arg.backup_backup_dest_))) {
            LOG_WARN("failed to set backup backup dest", KR(ret));
          } else if (OB_FAIL(util.is_empty_directory(dest.root_path_, dest.storage_info_, is_empty_dir))) {
            LOG_WARN("failed to check is empty directory", K(ret), K(dest));
          } else if (!is_empty_dir) {
            ret = OB_INVALID_BACKUP_DEST;
            LOG_ERROR("cannot use backup backup dest with non empty directory", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 == STRNCMP(arg.backup_backup_dest_, "", OB_MAX_BACKUP_DEST_LENGTH)) {
      if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to copy backup dest", KR(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(backup_dest_str, sizeof(backup_dest_str), "%s", backup_dest))) {
        LOG_WARN("failed to databuff printf", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(scheduler.init(
                   tenant_id, backup_set_id, max_backup_times, backup_dest_str, sql_proxy_, backup_backupset_))) {
      LOG_WARN("failed to init backup backupset scheduler", KR(ret), K(arg));
    } else if (OB_FAIL(scheduler.start_schedule_backup_backupset())) {
      LOG_WARN("failed to start schedule backup backupset", KR(ret));
    } else {
      LOG_INFO("handle backup backupset", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObRootService::handle_backup_backuppiece(const obrpc::ObBackupBackupPieceArg& arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupBackupPieceJobInfo job_info;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t piece_id = arg.piece_id_;
  const int64_t max_backup_times = arg.max_backup_times_;
  const bool with_active_piece = arg.with_active_piece_;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObBackupInfoManager manager;
  ObLogArchiveBackupInfoMgr archive_mgr;
  bool has_round_mode = false;
  bool is_enable_auto = false;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup backup piece get invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(wait_refresh_config())) {
    LOG_WARN("failed to wait_refresh_schema", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy_))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else {
    if (OB_FAIL(manager.init(OB_SYS_TENANT_ID, sql_proxy_))) {
      LOG_WARN("failed to init backup info manager", KR(ret));
    } else if (OB_FAIL(manager.get_enable_auto_backup_archivelog(OB_SYS_TENANT_ID, trans, is_enable_auto))) {
      LOG_WARN("failed to get enable auto backup archivelog", KR(ret), K(tenant_id));
    } else if (is_enable_auto) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support backup backuppiece in auto mode", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup backuppiece in auto mode");
    } else {
      job_info.tenant_id_ = tenant_id;
      job_info.incarnation_ = OB_START_INCARNATION;
      job_info.piece_id_ = piece_id;
      job_info.max_backup_times_ = max_backup_times;
      job_info.status_ = ObBackupBackupPieceJobInfo::SCHEDULE;
      job_info.comment_ = "";
      job_info.type_ =
          with_active_piece ? ObBackupBackupPieceJobInfo::WITH_ACTIVE_PIECE : ObBackupBackupPieceJobInfo::ONLY_FROZEN;
      if (OB_SUCC(ret) && OB_SYS_TENANT_ID != tenant_id) {
        if (0 == STRNCMP(arg.backup_backup_dest_, "", OB_MAX_BACKUP_DEST_LENGTH)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant level backup backup and backup_backup_dest is not empty is not supported", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant level backup backup and backup_backup_dest is not empty");
        } else {
          char tmp_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
          char tmp_backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
          if (OB_FAIL(GCONF.backup_dest.copy(tmp_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
            LOG_WARN("failed to copy backup dest", KR(ret));
          } else if (OB_FAIL(GCONF.backup_backup_dest.copy(tmp_backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
            LOG_WARN("failed to copy backup dest", KR(ret));
          } else if (0 == STRNCMP(tmp_backup_dest_str, arg.backup_backup_dest_, OB_MAX_BACKUP_DEST_LENGTH) ||
                     0 == STRNCMP(tmp_backup_backup_dest_str, arg.backup_backup_dest_, OB_MAX_BACKUP_DEST_LENGTH)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("tenant level backup backup use same backup backup dest as gconf not supported", KR(ret));
            LOG_USER_ERROR(
                OB_NOT_SUPPORTED, "tenant level backup backup use same backup backup dest as gconf not supported");
          } else {
            share::ObBackupDest dest;
            bool is_empty_dir = false;
            ObStorageUtil util(false /*need_retry*/);
            if (OB_FAIL(dest.set(arg.backup_backup_dest_))) {
              LOG_WARN("failed to set backup backup dest", KR(ret));
            } else if (OB_FAIL(util.is_empty_directory(dest.root_path_, dest.storage_info_, is_empty_dir))) {
              LOG_WARN("failed to check is empty directory", K(ret), K(dest));
            } else if (!is_empty_dir) {
              ret = OB_INVALID_BACKUP_DEST;
              LOG_ERROR("cannot use backup backup dest with non empty directory", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == STRNCMP(arg.backup_backup_dest_, "", OB_MAX_BACKUP_DEST_LENGTH)) {
          if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
            LOG_WARN("failed to copy backup dest", KR(ret));
          } else if (OB_FAIL(job_info.backup_dest_.set(backup_dest_str))) {
            LOG_WARN("failed to set backup dest", KR(ret));
          }
        } else {
          if (OB_FAIL(job_info.backup_dest_.set(arg.backup_backup_dest_))) {
            LOG_WARN("failed to set backup dest", KR(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(archive_mgr.check_has_round_mode_archive_in_dest(
                     trans, job_info.backup_dest_, OB_SYS_TENANT_ID, has_round_mode))) {
        LOG_WARN("failed to check has round mode archive in dest", KR(ret), K(backup_dest_str));
      } else if (has_round_mode) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("cannot start backup backup in piece mode", KR(ret), K(backup_dest_str));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup backup piece in round mode directory");
      } else if (OB_FAIL(manager.get_job_id(job_info.job_id_))) {
        LOG_WARN("failed to get job id", KR(ret));
      } else if (OB_FAIL(ObBackupBackupPieceJobOperator::insert_job_item(job_info, trans))) {
        LOG_WARN("failed to insert job item", KR(ret), K(job_info));
      } else {
        backup_archive_log_.wakeup();
      }
    }
  }
  if (trans.is_started()) {
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "commit", OB_SUCC(ret), K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObRootService::handle_backup_manage(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    switch (arg.type_) {
      case ObBackupManageArg::CANCEL_BACKUP: {
        // TODO: compelte it
        if (OB_FAIL(handle_backup_database_cancel(arg))) {
          LOG_WARN("failed to handle backup database cancel", K(ret), K(arg));
        }
        break;
      };
      case ObBackupManageArg::SUSPEND_BACKUP: {
        // TODO: compelte it
        ret = OB_NOT_SUPPORTED;
        break;
      };
      case ObBackupManageArg::RESUME_BACKUP: {
        // TODO: compelte it
        ret = OB_NOT_SUPPORTED;
        break;
      };
        //    case ObBackupManageArg::DELETE_EXPIRED_BACKUP: {
        //      ret = OB_NOT_SUPPORTED;
        //      break;
        //    };
      case ObBackupManageArg::DELETE_BACKUP:
      case ObBackupManageArg::DELETE_BACKUPPIECE:
      case ObBackupManageArg::DELETE_BACKUPROUND: {
        if (OB_FAIL(handle_backup_delete_backup_data(arg))) {
          LOG_WARN("failed to handle backup delete backup set", K(ret), K(arg));
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
      case ObBackupManageArg::DELETE_OBSOLETE_BACKUP: {
        if (OB_FAIL(handle_backup_delete_obsolete_backup(arg))) {
          LOG_WARN("failed to handle backup delete expried backup", K(ret), K(arg));
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
      case ObBackupManageArg::CANCEL_DELETE_BACKUP: {
        if (OB_FAIL(handle_cancel_delete_backup(arg))) {
          LOG_WARN("failed to handle cancel delete backup", K(ret), K(arg));
        }
        break;
      };
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

int ObRootService::update_all_sys_tenant_schema_version()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObTableSchema*> table_schemas;
  ObRsJobInfo job_info;
  ObRsJobType job_type = ObRsJobType::JOB_TYPE_UPDATE_SYS_TABLE_SCHEMA_VERSION;
  int64_t job_id = OB_INVALID_ID;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(RS_JOB_FIND(
                 job_info, sql_proxy_, "job_type", "UPDATE_SYS_TABLE_SCHEMA", "job_status", "INPROGRESS"))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to find job", K(ret));
    }
  } else if (job_info.job_id_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find a job id unexpected", K(ret));
  } else if (OB_FAIL(RS_JOB_COMPLETE(job_info.job_id_, -1, sql_proxy_))) {
    LOG_WARN("fail to complete job", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(job_id, job_type, sql_proxy_, "tenant_id", 0))) {
    LOG_WARN("fail to create rs job", K(ret));
  } else if (OB_FAIL(
                 ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(ddl_service_.publish_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to publish schema", KR(ret));
  }
  int tmp_ret = OB_SUCCESS;
  if (job_id > 0) {
    if (OB_SUCCESS != (tmp_ret = RS_JOB_COMPLETE(job_id, ret, sql_proxy_))) {
      LOG_ERROR("fail to complete job", K(tmp_ret), K(ret), K(job_id));
      ret = (OB_FAIL(ret)) ? ret : tmp_ret;
    }
  }
  return ret;
}

// standby cluster update schema_version while specify schema_version and table_id
// primary cluster increase schema_version while do not specify schema_version and table_id, it used to add standby
// cluster.
int ObRootService::update_table_schema_version(const ObUpdateTableSchemaVersionArg& arg)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObTableSchema* new_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObSchemaService* schema_service = schema_service_->get_schema_service();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (ObUpdateTableSchemaVersionArg::UPDATE_SYS_TABLE_IN_TENANT_SPACE == arg.action_) {
    // update sys tables in tenant space for physical restore
    if (OB_FAIL(ddl_service_.update_sys_table_schema_version_in_tenant_space())) {
      LOG_WARN("fail to update sys table schema version in tenant space", KR(ret));
    }
  } else if (OB_SYS_TENANT_ID == arg.tenant_id_ && 0 == arg.table_id_ &&
             OB_INVALID_SCHEMA_VERSION == arg.schema_version_) {
    if (OB_FAIL(update_all_sys_tenant_schema_version())) {
      LOG_WARN("fail to update all sys tenant schema version", KR(ret));
    } else {
      LOG_INFO("update all sys tenant schema version success");
    }
  } else {
    if (OB_SYS_TENANT_ID != arg.tenant_id_ || !is_sys_table(arg.table_id_)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not allow to update table schema", KR(ret), K(arg));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("invalid schema service", KR(ret), KP(schema_service));
    } else if (!schema_service->is_in_bootstrap()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("not allow to update table schema while not in bootstrap");
    } else if (OB_FAIL(
                   ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
      LOG_WARN("get_schema_guard with version in inner table failed", K(ret), K(arg));
    } else if (OB_FAIL(schema_guard.get_table_schema(arg.table_id_, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(arg));
    } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, *table_schema, new_schema))) {
      LOG_WARN("fail to alloc schema", KR(ret));
    } else if (OB_ISNULL(new_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid schema", KR(ret), KP(new_schema));
    } else {
      ObDDLOperator ddl_operator(*schema_service_, sql_proxy_);
      new_schema->set_schema_version(arg.schema_version_);
      // in bootstrap, no need to write transaction end signal.
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(&sql_proxy_))) {
        LOG_WARN("fail to start transaction", KR(ret));
      } else if (OB_FAIL(ddl_operator.update_table_schema_version(trans, *new_schema))) {
        LOG_WARN("fail to update table schema version", KR(ret), K(arg));
      } else {
        LOG_INFO("update table schema version success", KR(ret), K(arg));
      }
      bool commit = (OB_SUCCESS == ret);
      if (trans.is_started()) {
        int tmp_ret = trans.end(commit);
        if (OB_SUCCESS != tmp_ret) {
          ret = (ret == OB_SUCCESS) ? tmp_ret : ret;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.publish_schema(arg.tenant_id_))) {
        LOG_WARN("fail to publish schema", KR(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObRootService::modify_schema_in_restore(const obrpc::ObRestoreModifySchemaArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ddl_service_.modify_schema_in_restore(arg))) {
    LOG_WARN("do sequence ddl failed", K(arg), K(ret));
  }
  return ret;
}

int ObRootService::handle_backup_database_cancel(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupCancelScheduler backup_cancel_scheduler;
  const uint64_t tenant_id = arg.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObBackupManageArg::CANCEL_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handle backup database cancel get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_cancel_scheduler.init(tenant_id, sql_proxy_, &root_backup_))) {
    LOG_WARN("failed to init backup cancel scheduler", K(ret), K(arg));
  } else if (OB_FAIL(backup_cancel_scheduler.start_schedule_backup_cancel())) {
    LOG_WARN("failed to start schedule backup cancel", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::check_backup_scheduler_working(Bool& is_working)
{
  int ret = OB_NOT_SUPPORTED;
  is_working = true;

  FLOG_INFO("not support check backup scheduler working, should not use anymore", K(ret), K(is_working));
  return ret;
}

ObRootService::ObReloadUnitReplicaCounterTask::ObReloadUnitReplicaCounterTask(ObRootService& root_service)
    : ObAsyncTimerTask(root_service.task_queue_), root_service_(root_service)
{
  set_retry_times(INT64_MAX);
}
int ObRootService::ObReloadUnitReplicaCounterTask::process()  // refresh by tenant
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(root_service_.refresh_unit_replica_counter())) {
    LOG_WARN("fail to refrush unit replica count", K(ret));
  } else {
    LOG_INFO("refrush unit replica count success");
  }
  return ret;
}
ObAsyncTask* ObRootService::ObReloadUnitReplicaCounterTask::deep_copy(char* buf, const int64_t buf_size) const
{
  ObReloadUnitReplicaCounterTask* task = NULL;
  if (NULL == buf || buf_size < static_cast<int64_t>(sizeof(*this))) {
    LOG_WARN("buffer is not enought", K(buf_size), KP(buf));
  } else {
    task = new (buf) ObReloadUnitReplicaCounterTask(root_service_);
  }
  return task;
}
int ObRootService::refresh_unit_replica_counter()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard tenant_schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, tenant_schema_guard))) {
    LOG_WARN("fail to get schema_guard", KR(ret));
  } else if (OB_FAIL(tenant_schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    std::sort(tenant_ids.begin(), tenant_ids.end(), cmp_tenant_id);
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); ++i) {
      uint64_t tenant_id = tenant_ids.at(i);
      if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(tenant_id));
      } else if (OB_FAIL(root_balancer_.refresh_unit_replica_counter(tenant_id))) {
        LOG_WARN("fail to create unit replica counter", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObRootService::handle_backup_delete_obsolete_backup(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupDataCleanScheduler data_clean_scheduler;
  const int64_t now_ts = ObTimeUtil::current_time();
  ObBackupManageArg new_arg = arg;
  ObBackupDestOpt backup_dest_option;
  const bool is_backup_backup = false;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObBackupManageArg::DELETE_OBSOLETE_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup delete arg type is not expried backup arg", K(ret), K(arg));
  } else if (OB_FAIL(backup_dest_option.init(is_backup_backup))) {
    LOG_WARN("failed to init backup dest option", K(ret));
  } else if (backup_dest_option.recovery_window_ <= 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("backup recovery window is unepxected", K(ret), K(backup_dest_option));
    const int64_t ERROR_MSG_LENGTH = 1024;
    char error_msg[ERROR_MSG_LENGTH] = "";
    int64_t pos = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                           ERROR_MSG_LENGTH,
                           pos,
                           "can not execute delete obsolete, because recovery window unexpected."
                           " recovery window : %ld .",
                           backup_dest_option.recovery_window_))) {
      LOG_WARN("failed to set error msg", K(tmp_ret), K(error_msg), K(pos));
    } else {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, error_msg);
    }
  } else {
    new_arg.value_ = now_ts - backup_dest_option.recovery_window_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_clean_scheduler.init(new_arg, *schema_service_, sql_proxy_, &backup_data_clean_))) {
    LOG_WARN("failed ot init backup data clean scheduler", K(ret), K(arg));
  } else if (OB_FAIL(data_clean_scheduler.start_schedule_backup_data_clean())) {
    LOG_WARN("failed to start schedule backup data clean", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_backup_delete_backup_data(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupDataCleanScheduler data_clean_scheduler;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObBackupManageArg::DELETE_BACKUP != arg.type_ && ObBackupManageArg::DELETE_BACKUPPIECE != arg.type_ &&
             ObBackupManageArg::DELETE_BACKUPROUND != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup delete arg type is not delete backup set arg", K(ret), K(arg));
  } else if (OB_FAIL(data_clean_scheduler.init(arg, *schema_service_, sql_proxy_, &backup_data_clean_))) {
    LOG_WARN("failed ot init backup data clean scheduler", K(ret), K(arg));
  } else if (OB_FAIL(data_clean_scheduler.start_schedule_backup_data_clean())) {
    LOG_WARN("failed to start schedule backup data clean", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_cancel_delete_backup(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObCancelDeleteBackupScheduler cancel_delete_backup_scheduler;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObBackupManageArg::CANCEL_DELETE_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cancel delete backup arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(cancel_delete_backup_scheduler.init(arg.tenant_id_, sql_proxy_, &backup_data_clean_))) {
    LOG_WARN("failed ot init backup data clean scheduler", K(ret), K(arg));
  } else if (OB_FAIL(cancel_delete_backup_scheduler.start_schedule_cacel_delete_backup())) {
    LOG_WARN("failed to start schedule backup data clean", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::handle_cancel_backup_backup(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObCancelBackupBackupScheduler cancel_scheduler;
  ObCancelBackupBackupType cancel_type =
      arg.type_ == ObBackupManageArg::CANCEL_BACKUP_BACKUPSET ? CANCEL_BACKUP_BACKUPSET : CANCEL_BACKUP_BACKUPPIECE;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObBackupManageArg::CANCEL_BACKUP_BACKUPSET != arg.type_ &&
             ObBackupManageArg::CANCEL_BACKUP_BACKUPPIECE != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(arg));
  } else if (OB_FAIL(cancel_scheduler.init(
                 arg.tenant_id_, sql_proxy_, cancel_type, &backup_backupset_, &backup_archive_log_))) {
    LOG_WARN("failed to init cancel backup backup scheduler", K(ret));
  } else if (OB_FAIL(cancel_scheduler.start_schedule_cancel_backup_backup())) {
    LOG_WARN("failed to start schedule cancel backup backup", K(ret));
  }
  return ret;
}

int ObRootService::handle_cancel_all_backup_force(const obrpc::ObBackupManageArg &arg)
{
  int ret = OB_SUCCESS;

  FLOG_WARN("start cancel all backup force");
  // clear backup_dest and backup_backup_dest in GCONF
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (OB_FAIL(sql.assign_fmt("alter system set backup_dest=''"))) {
    LOG_WARN("failed to clear backup dest", K(ret));
  } else if (OB_FAIL(sql_proxy_.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to clear backup dest", K(ret), K(sql));
  } else {
    LOG_WARN("succeed to clear backup dest");
  }

  ROOTSERVICE_EVENT_ADD("root_service", "clear_backup_dest", "result", ret, K_(self_addr));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt("alter system set backup_backup_dest=''"))) {
      LOG_WARN("failed to clear backup backup dest", K(ret));
    } else if (OB_FAIL(sql_proxy_.write(sql.ptr(), affected_rows))) {
      LOG_WARN("failed to clear backup backup dest", K(ret), K(sql));
    } else {
      LOG_WARN("succeed to clear backup backup dest");
    }

    ROOTSERVICE_EVENT_ADD("root_service", "clear_backup_backup_dest", "result", ret, K_(self_addr));
  }

  ROOTSERVICE_EVENT_ADD("root_service", "force_cancel_backup", "result", ret, K_(self_addr));

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(wait_refresh_config())) {
    LOG_WARN("failed to wait refresh schema", K(ret), K(arg));
  } else if (OB_FAIL(backup_lease_service_.force_cancel(arg.tenant_id_))) {
    LOG_WARN("failed to force stop", K(ret), K(arg));
  }

  FLOG_WARN("end cancel all backup force", K(ret));

  return ret;
}

int ObRootService::update_freeze_schema_versions(
    const int64_t frozen_version, obrpc::ObTenantSchemaVersions& tenant_schema_versions)
{
  int ret = OB_SUCCESS;
  obrpc::ObGetSchemaArg get_arg;
  ObFreezeInfoProxy freeze_info_proxy;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_schema_versions(get_arg, tenant_schema_versions))) {
    LOG_WARN("fail to get tenant schema versions", KR(ret));
  } else if (OB_FAIL(freeze_info_proxy.update_frozen_schema_v2(
                 sql_proxy_, frozen_version, tenant_schema_versions.tenant_schema_versions_))) {
    LOG_WARN("fail to update frozen schema version", KR(ret));
  }
  return ret;
}

void ObRootService::reset_fail_count()
{
  ATOMIC_STORE(&fail_count_, 0);
}

void ObRootService::update_fail_count(int ret)
{
  int64_t count = ATOMIC_AAF(&fail_count_, 1);
  if (count > OB_ROOT_SERVICE_START_FAIL_COUNT_UPPER_LIMIT && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    LOG_ERROR("rs_monitor_check : fail to start root service", KR(ret), K(count));
  } else {
    LOG_WARN("rs_monitor_check : fail to start root service", KR(ret), K(count));
  }
}

int ObRootService::send_physical_restore_result(const obrpc::ObPhysicalRestoreResult& res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(res));
  } else if (OB_FAIL(restore_scheduler_.mark_job_failed(
                 res.job_id_, res.return_ret_, res.mod_, res.trace_id_, res.addr_))) {
    LOG_WARN("fail to mark job failed", K(ret), K(res));
  }
  LOG_INFO("get physical restore job's result", K(ret), K(res));
  return ret;
}

int ObRootService::check_has_restore_tenant(bool& has_restore_tenant)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  has_restore_tenant = false;
  if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard with version in inner table failed", KR(ret));
  } else {
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
      LOG_WARN("fail to get tenant ids", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has_restore_tenant && i < tenant_ids.count(); i++) {
      const ObSimpleTenantSchema* tenant = NULL;
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant))) {
        LOG_WARN("fail to get tenant", KR(tenant_id));
      } else if (OB_ISNULL(tenant)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant not found", KR(ret), K(tenant_id));
      } else if (tenant->is_restore()) {
        has_restore_tenant = true;
      }
    }
  }
  return ret;
}

int ObRootService::build_range_part_split_arg(
    const common::ObPartitionKey& partition_key, const common::ObRowkey& rowkey, AlterTableSchema& alter_table_schema)
{
  int ret = OB_SUCCESS;

  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema* table_schema = nullptr;
  const share::schema::ObDatabaseSchema* db_schema = nullptr;

  const ObPartition* original_partition = nullptr;
  ObPartition split_partition1;
  ObPartition split_partition2;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService is not inited", K(ret));
  } else if (!partition_key.is_valid() || !rowkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(partition_key), K(rowkey));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("feature will be supported after ver 3.1", K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(
                 partition_key.get_tenant_id(), schema_guard))) {
    LOG_WARN("get_tenant_schema_guard_with_version_in_inner_table failed", K(ret), K(partition_key));
  } else if (OB_FAIL(schema_guard.get_table_schema(partition_key.get_table_id(), table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(partition_key));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_BAD_TABLE;
    LOG_WARN("table_schema is NULL", K(ret), K(partition_key));
  } else if (!table_schema->is_auto_partitioned_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this table is not auto partitioned table, unexpected", K(ret), K(partition_key));
  } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), db_schema))) {
    LOG_WARN("get database schema failed", K(ret), K(partition_key));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("db_schema is NULL", K(ret), K(partition_key));
    // at present, the implementation assumes that split_key is row_key,
    // calculate split_key according to schema
  } else if (OB_FAIL(alter_table_schema.assign(*table_schema))) {
    LOG_WARN("alter_table_schema assign failed", K(ret), K(partition_key));
    // at present, only one level partition table is supported as automatic partition.
  } else if (OB_FAIL(table_schema->get_partition_by_part_id(partition_key.get_partition_id(),
                 false, /*check_dropped_partition*/
                 original_partition))) {
    LOG_WARN("get_partition_by_partition_id failed", K(ret), K(partition_key));
  } else {
    alter_table_schema.reset_partition_schema();
    split_partition1.set_is_empty_partition_name(true);
    split_partition2.set_is_empty_partition_name(true);

    if (OB_FAIL(split_partition1.set_high_bound_val(rowkey))) {
      LOG_WARN("split_partition1 set_high_bound_val failed", K(ret), K(partition_key));
    } else if (OB_FAIL(split_partition2.set_high_bound_val(original_partition->get_high_bound_val()))) {
      LOG_WARN("split_partition2 set_high_bound_val failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.set_origin_table_name(table_schema->get_table_name_str()))) {
      LOG_WARN("alter_table_schema set_origin_table_name failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.set_origin_database_name(db_schema->get_database_name_str()))) {
      LOG_WARN("alter_table_schema set_origin_database_name failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.set_split_partition_name(original_partition->get_part_name()))) {
      LOG_WARN("alter_table_schema set_split_partition_name failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.set_split_high_bound_value(original_partition->get_high_bound_val()))) {
      LOG_WARN("alter_table_schema set_split_high_bound_value failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.get_part_option().set_part_expr(
                   table_schema->get_part_option().get_part_func_expr_str()))) {
      LOG_WARN("alter_table_schema set_part_expr failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.add_partition(split_partition1))) {
      LOG_WARN("alter_table_schema add_partition split_partition1 failed", K(ret), K(partition_key));
    } else if (OB_FAIL(alter_table_schema.add_partition(split_partition2))) {
      LOG_WARN("alter_table_schema add_partition split_partition2 failed", K(ret), K(partition_key));
    } else {
      alter_table_schema.set_part_level(PARTITION_LEVEL_ONE);
      alter_table_schema.set_part_num(2);
      alter_table_schema.get_part_option().set_part_func_type(table_schema->get_part_option().get_part_func_type());
      alter_table_schema.get_part_option().set_auto_part(table_schema->get_part_option().is_auto_range_part());
      alter_table_schema.get_part_option().set_auto_part_size(table_schema->get_auto_part_size());
    }
  }

  return ret;
}

int ObRootService::execute_range_part_split(const obrpc::ObExecuteRangePartSplitArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive execute_range_part_split request", K(ret), K(arg));
  const int64_t begin_time = ObTimeUtility::current_time();

  const common::ObPartitionKey& partition_key = arg.partition_key_;
  obrpc::ObAlterTableArg alter_table_arg;
  alter_table_arg.is_alter_partitions_ = true;
  alter_table_arg.alter_part_type_ = ObAlterTableArg::SPLIT_PARTITION;
  AlterTableSchema& alter_table_schema = alter_table_arg.alter_table_schema_;
  obrpc::ObAlterTableRes alter_table_res;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootService is not inited", K(ret), K(arg));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (is_inner_table(partition_key.get_table_id())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("inner table does not support to execute split", K(ret));
  } else if (OB_FAIL(build_range_part_split_arg(partition_key, arg.rowkey_, alter_table_schema))) {
    LOG_WARN("build_range_part_split_arg failed", K(ret), K(partition_key));
  } else if (OB_FAIL(alter_table(alter_table_arg, alter_table_res))) {
    LOG_WARN("alter_table failed", K(ret), K(partition_key));
  }

  const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
  LOG_INFO("execute_range_part_split finished", K(ret), K(arg), K(cost_time));

  return ret;
}

int ObRootService::handle_backup_delete_backup_set(const obrpc::ObBackupManageArg& arg)
{
  int ret = OB_SUCCESS;
  ObBackupDataCleanScheduler data_clean_scheduler;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObBackupManageArg::DELETE_BACKUP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup delete arg type is not delete backup set arg", K(ret), K(arg));
  } else if (OB_FAIL(data_clean_scheduler.init(arg, *schema_service_, sql_proxy_, &backup_data_clean_))) {
    LOG_WARN("failed ot init backup data clean scheduler", K(ret), K(arg));
  } else if (OB_FAIL(data_clean_scheduler.start_schedule_backup_data_clean())) {
    LOG_WARN("failed to start schedule backup data clean", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::create_restore_point(const obrpc::ObCreateRestorePointArg& arg)
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> index_ids;
  uint64_t tenant_id = arg.tenant_id_;

  LOG_DEBUG("receive create restore point arg", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ddl_service_.get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_unavailable_index(tenant_id, index_ids))) {
    LOG_WARN("fail to get tenant unavailable index", K(ret), K(tenant_id));
  } else if (0 != index_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unavailable index exists, cannot create restore point", KR(ret), K(index_ids));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "unavailable index exists, create restore point");
  } else if (OB_FAIL(restore_point_service_.create_restore_point(arg.tenant_id_, arg.name_.ptr()))) {
    LOG_WARN("failed to create restore point", K(ret), K(arg));
  }
  return ret;
}

int ObRootService::drop_restore_point(const obrpc::ObDropRestorePointArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("receive drop restore point arg", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(restore_point_service_.drop_restore_point(arg.tenant_id_, arg.name_.ptr()))) {
    LOG_WARN("failed to drop restore point", K(ret), K(arg));
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
    const int64_t SLEEP_INTERVAL = 100 * 1000;  // 100ms interval of send rpc
    const int64_t PURGE_EACH_RPC = 10;          // delete count per rpc
    obrpc::Int64 affected_rows = 0;
    obrpc::ObPurgeRecycleBinArg arg;
    int64_t purge_sum = purge_each_time;
    const bool is_standby = PRIMARY_CLUSTER != ObClusterInfoGetter::get_cluster_type_v2();
    const ObSimpleTenantSchema *simple_tenant = NULL;
    // ignore ret
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
        int64_t start_time = ObTimeUtility::current_time();
        arg.purge_num_ = purge_sum > PURGE_EACH_RPC ? PURGE_EACH_RPC : purge_sum;
        if (OB_FAIL(common_proxy_.purge_expire_recycle_objects(arg, affected_rows))) {
          LOG_WARN(
              "purge reyclebin objects failed", KR(ret), K(current_time), K(expire_time), K(affected_rows), K(arg));
        } else {
          purge_sum -= affected_rows;
          if (arg.purge_num_ != affected_rows) {
            int64_t cost_time = ObTimeUtility::current_time() - start_time;
            LOG_INFO("purge recycle objects", KR(ret), K(cost_time), K(expire_time), K(current_time), K(affected_rows));
            if (OB_SUCC(ret) && in_service()) {
              usleep(SLEEP_INTERVAL);
            }
            break;
          }
        }
        int64_t cost_time = ObTimeUtility::current_time() - start_time;
        LOG_INFO("purge recycle objects", KR(ret), K(cost_time), K(expire_time), K(current_time), K(affected_rows));
        if (OB_SUCC(ret) && in_service()) {
          usleep(SLEEP_INTERVAL);
        }
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////
int ObRootService::switch_cluster(const obrpc::ObAlterClusterInfoArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::alter_cluster_attr(const obrpc::ObAlterClusterInfoArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::alter_cluster_info(const obrpc::ObAlterClusterInfoArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::cluster_regist(const obrpc::ObRegistClusterArg& arg, obrpc::ObRegistClusterRes& res)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg, res);
  return ret;
}
int ObRootService::alter_standby(const obrpc::ObAdminClusterArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::get_cluster_info(const obrpc::ObGetClusterInfoArg& arg, share::ObClusterInfo& cluster_info)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg, cluster_info);
  return ret;
}
int ObRootService::get_switchover_status(obrpc::ObGetSwitchoverStatusRes& res)
{
  int ret = OB_SUCCESS;
  UNUSEDx(res);
  return ret;
}
int ObRootService::check_cluster_valid_to_add(const obrpc::ObCheckAddStandbyArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::check_standby_can_access(const obrpc::ObCheckStandbyCanAccessArg& arg, obrpc::Bool& can_access)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg, can_access);
  return ret;
}
int ObRootService::get_schema_snapshot(const obrpc::ObSchemaSnapshotArg& arg, obrpc::ObSchemaSnapshotRes& res)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg, res);
  return ret;
}
int ObRootService::cluster_action_verify(const obrpc::ObClusterActionVerifyArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::gen_next_schema_version(const obrpc::ObDDLArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::get_cluster_stats(obrpc::ObClusterTenantStats& tenant_stats)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tenant_stats);
  return ret;
}
int ObRootService::standby_upgrade_virtual_schema(const obrpc::ObDDLNopOpreatorArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::update_standby_cluster_info(const share::ObClusterAddr& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::cluster_heartbeat(const share::ObClusterAddr& arg, obrpc::ObStandbyHeartBeatRes& res)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg, res);
  return ret;
}

int ObRootService::standby_grant(const obrpc::ObStandbyGrantArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
int ObRootService::finish_replay_schema(const obrpc::ObFinishReplayArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}
///////////////////////////////////////////////
}  // end namespace rootserver
}  // end namespace oceanbase
