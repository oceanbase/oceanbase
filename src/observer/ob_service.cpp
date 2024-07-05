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

#include "observer/ob_service.h"

#include <new>
#include <string.h>
#include <cmath>

#include "share/ob_define.h"
#include "lib/ob_running_mode.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/utility/utility.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "lib/alloc/memory_dump.h"

#include "common/ob_member_list.h"
#include "common/ob_zone.h"
#include "share/ob_version.h"

#include "share/ob_version.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/deadlock/ob_deadlock_inner_table_service.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_zone_table_operation.h"
#include "share/tablet/ob_tablet_info.h" // for ObTabletReplica
#include "share/ob_tablet_replica_checksum_operator.h" // ObTabletReplicaChecksumItem
#include "share/rc/ob_tenant_base.h"

#include "storage/ob_i_table.h"
#include "storage/tx/ob_trans_service.h"
#include "sql/optimizer/ob_storage_estimator.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "rootserver/ob_bootstrap.h"
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "observer/ob_server.h"
#include "observer/ob_dump_task_generator.h"
#include "observer/ob_server_schema_updater.h"
#include "ob_server_event_history_table_operator.h"
#include "share/ob_alive_server_tracer.h"
#include "storage/ddl/ob_complement_data_task.h" // complement data for drop column
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_build_index_task.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_service.h"        // ObLogService
#include "logservice/palf_handle_guard.h"     // PalfHandleGuard
#include "logservice/archiveservice/ob_archive_service.h"
#include "share/scn.h"     // PalfHandleGuard
#include "storage/backup/ob_backup_handler.h"
#include "storage/backup/ob_ls_backup_clean_mgr.h"
#include "storage/ob_file_system_router.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/ob_ddl_sim_point.h" // for DDL_SIM
#include "storage/backup/ob_backup_utils.h"
#include "observer/report/ob_tenant_meta_checker.h"//ObTenantMetaChecker
#include "rootserver/backup/ob_backup_task_scheduler.h" // ObBackupTaskScheduler
#include "rootserver/backup/ob_backup_schedule_task.h" // ObBackupScheduleTask
#include "rootserver/ob_ls_recovery_stat_handler.h"//get_all_ls_replica_readbable_scn
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/ob_cluster_event_history_table_operator.h"//CLUSTER_EVENT_INSTANCE
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_heartbeat_handler.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/high_availability/ob_transfer_lock_utils.h"

namespace oceanbase
{

using namespace common;
using namespace rootserver;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace backup;
using namespace palf;
using namespace archive;

namespace observer
{


ObSchemaReleaseTimeTask::ObSchemaReleaseTimeTask()
: schema_updater_(nullptr), is_inited_(false)
{}

int ObSchemaReleaseTimeTask::init(ObServerSchemaUpdater &schema_updater, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSchemaReleaseTimeTask has already been inited", K(ret));
  } else {
    schema_updater_ = &schema_updater;
    is_inited_ = true;
    if (OB_FAIL(schedule_())) {
      LOG_WARN("fail to schedule ObSchemaReleaseTimeTask in init", KR(ret));
    }
  }
  return ret;
}

void ObSchemaReleaseTimeTask::destroy()
{
  is_inited_ = false;
  schema_updater_ = nullptr;
}

int ObSchemaReleaseTimeTask::schedule_()
{
  int ret = OB_SUCCESS;
  int64_t memory_recycle_interval = GCONF._schema_memory_recycle_interval;
  if (0 == memory_recycle_interval) {
    memory_recycle_interval = 15L * 60L * 1000L * 1000L; //15mins
  }
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, *this, memory_recycle_interval, false /*not schedule repeatly*/))) {
    LOG_ERROR("fail to schedule task ObSchemaReleaseTimeTask", KR(ret));
  }
  return ret;
}

void ObSchemaReleaseTimeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSchemaReleaseTimeTask has not been inited", K(ret));
  } else if (OB_ISNULL(schema_updater_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSchemaReleaseTimeTask task got null ptr", K(ret));
  } else if (OB_FAIL(schema_updater_->try_release_schema())) {
    LOG_WARN("ObSchemaReleaseTimeTask failed", K(ret));
  }
  if (OB_FAIL(schedule_())) {
    // overwrite ret
    LOG_WARN("fail to schedule ObSchemaReleaseTimeTask in runTimerTask", KR(ret));
  }
}

ObRemoteMasterRsUpdateTask::ObRemoteMasterRsUpdateTask(const ObGlobalContext &gctx)
  : gctx_(gctx), is_inited_(false)
{}

int ObRemoteMasterRsUpdateTask::init(int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRemoteMasterRsUpdateTask has already been inited", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
    LOG_WARN("fail to schedule task ObSchemaReleaseTimeTask", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRemoteMasterRsUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(gctx_.rs_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rs_mgr is not init yet", KR(ret), K_(is_inited));
  } else if (OB_FAIL(gctx_.rs_mgr_->renew_remote_master_rootserver())) {
    LOG_WARN("renew remote rs master failed", KR(ret));
  }
}

//////////////////////////////////////

// here gctx may hasn't been initialized already
ObService::ObService(const ObGlobalContext &gctx)
    : inited_(false), in_register_process_(false),
    service_started_(false), stopped_(false),
    schema_updater_(),
    lease_state_mgr_(), heartbeat_process_(gctx, schema_updater_, lease_state_mgr_),
    gctx_(gctx), server_trace_task_(), schema_release_task_(),
    schema_status_task_(), remote_master_rs_update_task_(gctx), ls_table_updater_(),
    meta_table_checker_()
  {
  }

ObService::~ObService()
{
}

int ObService::init(common::ObMySQLProxy &sql_proxy,
                    share::ObIAliveServerTracer &server_tracer)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[OBSERVICE_NOTICE] init ob_service begin");
  const static int64_t REBUILD_FLAG_REPORT_THREAD_CNT = 1;

  if (inited_) {
    ret = OB_INIT_TWICE;
    FLOG_WARN("Oceanbase service has already init", KR(ret));
  } else if (!gctx_.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    FLOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), KR(ret));
  } else if (OB_FAIL(heartbeat_process_.init())) {
    FLOG_WARN("heartbeat_process_.init failed", KR(ret));
  } else if (OB_FAIL(schema_updater_.init(gctx_.self_addr(), gctx_.schema_service_))) {
    FLOG_WARN("client_manager_.initialize failed", "self_addr", gctx_.self_addr(), KR(ret));
  } else if (OB_FAIL(CLUSTER_EVENT_INSTANCE.init(sql_proxy))) {
    FLOG_WARN("init cluster event history table failed", KR(ret));
  } else if (OB_FAIL(SERVER_EVENT_INSTANCE.init(sql_proxy, gctx_.self_addr()))) {
    FLOG_WARN("init server event history table failed", KR(ret));
  } else if (OB_FAIL(DEALOCK_EVENT_INSTANCE.init(sql_proxy))) {
    FLOG_WARN("init deadlock event history cleaner failed", KR(ret));
  } else if (OB_FAIL(ObAllServerTracer::get_instance().init(lib::TGDefIDs::ServerGTimer, server_trace_task_))) {
    FLOG_WARN("init ObAllServerTracer failed", KR(ret));
  } else if (OB_FAIL(OB_TSC_TIMESTAMP.init())) {
    FLOG_WARN("init tsc timestamp failed", KR(ret));
  } else if (OB_FAIL(schema_release_task_.init(schema_updater_, lib::TGDefIDs::ServerGTimer))) {
    FLOG_WARN("init schema release task failed", KR(ret));
  } else if (OB_FAIL(remote_master_rs_update_task_.init(lib::TGDefIDs::ServerGTimer))) {
    FLOG_WARN("init remote master rs update task failed", KR(ret));
  } else if (OB_FAIL(ls_table_updater_.init())) {
    FLOG_WARN("init log stream table updater failed", KR(ret));
  } else if (OB_FAIL(meta_table_checker_.init(
      gctx_.lst_operator_,
      gctx_.tablet_operator_,
      gctx_.omt_,
      gctx_.schema_service_))) {
    FLOG_WARN("init meta table checker failed", KR(ret));
  } else {
    inited_ = true;
  }
  FLOG_INFO("[OBSERVICE_NOTICE] init ob_service finish", KR(ret), K_(inited));
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR(OB_ERR_OBSERVICE_START, "msg", "observice init() has failure", KR(ret));
  }
  return ret;
}

int ObService::register_self()
{
  int ret = OB_SUCCESS;
  in_register_process_ = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("service not initialized, can't register self", KR(ret));
  } else if (!lease_state_mgr_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("lease_state_mgr_ not init", KR(ret));
  } else if (OB_FAIL(lease_state_mgr_.register_self_busy_wait())) {
    LOG_WARN("register self failed", KR(ret));
  } else if (!lease_state_mgr_.is_valid_heartbeat()) {
    ret = OB_ERROR;
    LOG_ERROR("can't renew lease, the time difference between local and RS may be more than 2s",
        KR(ret), "heartbeat_expire_time", lease_state_mgr_.get_heartbeat_expire_time());
  } else {
    in_register_process_ = false;
    service_started_ = true;
    SERVER_EVENT_ADD("observice", "register");
  }
  return ret;
}

int ObService::start()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[OBSERVICE_NOTICE] start ob_service begin");
  if (!inited_) {
    ret = OB_NOT_INIT;
    FLOG_WARN("ob_service is not inited", KR(ret), K_(inited));
  } else if (OB_FAIL(lease_state_mgr_.init(gctx_.rs_rpc_proxy_,
                                           gctx_.rs_mgr_,
                                           &heartbeat_process_,
                                           *this))) {
    LOG_ERROR("init lease_state_mgr_ failed", KR(ret));
  } else if (OB_FAIL(register_self())) {
    LOG_ERROR("register self failed", KR(ret));
  } else {
    FLOG_INFO("regist to rs success");
  }
  if (FAILEDx(meta_table_checker_.start())) {
    LOG_ERROR("start meta table checker failed", KR(ret));
  }
  FLOG_INFO("[OBSERVICE_NOTICE] start ob_service end", KR(ret));
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR(OB_ERR_OBSERVICE_START, "msg", "observice start() has failure", KR(ret));
  }
  return ret;
}


void ObService::set_stop()
{
  LOG_INFO("[OBSERVICE_NOTICE] observice need stop now");
  lease_state_mgr_.set_stop();
}

void ObService::stop()
{
  FLOG_INFO("[OBSERVICE_NOTICE] start to stop observice");
  if (!inited_) {
    FLOG_WARN_RET(OB_NOT_INIT, "ob_service not init", K_(inited));
  } else {
    FLOG_INFO("begin to add server event");
    SERVER_EVENT_ADD("observer", "stop");
    FLOG_INFO("add server event success");

    service_started_ = false;
    stopped_ = true;

    FLOG_INFO("begin to stop schema updater");
    schema_updater_.stop();
    FLOG_INFO("schema updater stopped");

    FLOG_INFO("begin to stop ls table updater");
    ls_table_updater_.stop();
    FLOG_INFO("ls table updater stopped");

    FLOG_INFO("begin to stop meta table checker");
    meta_table_checker_.stop();
    FLOG_INFO("meta table checker stopped");

    FLOG_INFO("begin to stop heartbeat process");
    heartbeat_process_.stop();
    FLOG_INFO("heartbeat process stopped");

    FLOG_INFO("begin to stop deadlock event service");
    DEALOCK_EVENT_INSTANCE.stop();
    FLOG_INFO("deadlock event service stopped");

    FLOG_INFO("begin to stop server event instance");
    SERVER_EVENT_INSTANCE.stop();
    FLOG_INFO("server event instance stopped");

    FLOG_INFO("begin to stop cluster event instance");
    CLUSTER_EVENT_INSTANCE.stop();
    FLOG_INFO("cluster event instance stopped");
  }
  FLOG_INFO("[OBSERVICE_NOTICE] observice finish stop", K_(stopped));
}

void ObService::wait()
{
  FLOG_INFO("[OBSERVICE_NOTICE] wait ob_service begin");
  if (!inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "ob_service not init", K_(inited));
  } else {
    FLOG_INFO("begin to wait schema updater");
    schema_updater_.wait();
    FLOG_INFO("wait schema updater success");

    FLOG_INFO("begin to wait ls table updater");
    ls_table_updater_.wait();
    FLOG_INFO("wait ls table updater success");

    FLOG_INFO("begin to wait meta table checker");
    meta_table_checker_.wait();
    FLOG_INFO("wait meta table checker success");

    FLOG_INFO("begin to wait heartbeat process");
    heartbeat_process_.wait();
    FLOG_INFO("wait heartbeat process success");

    FLOG_INFO("begin to wait deadlock event service");
    DEALOCK_EVENT_INSTANCE.wait();
    FLOG_INFO("wait deadlock event service success");

    FLOG_INFO("begin to wait server event instance");
    SERVER_EVENT_INSTANCE.wait();
    FLOG_INFO("wait server event instance success");

    FLOG_INFO("begin to wait cluster event instance");
    CLUSTER_EVENT_INSTANCE.wait();
    FLOG_INFO("wait cluster event instance success");
  }
  FLOG_INFO("[OBSERVICE_NOTICE] wait ob_service end");
}

int ObService::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[OBSERVICE_NOTICE] destroy ob_service begin");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob_service not init", KR(ret), K_(inited));
  } else {
    FLOG_INFO("begin to destroy schema updater");
    schema_updater_.destroy();
    FLOG_INFO("schema updater destroyed");

    FLOG_INFO("begin to destroy lease state manager");
    lease_state_mgr_.destroy();
    FLOG_INFO("lease state manager destroyed");

    FLOG_INFO("begin to destroy cluster event instance");
    CLUSTER_EVENT_INSTANCE.destroy();
    FLOG_INFO("cluster event instance destroyed");

    FLOG_INFO("begin to destroy server event instance");
    SERVER_EVENT_INSTANCE.destroy();
    FLOG_INFO("server event instance destroyed");

    FLOG_INFO("begin to destroy meta table checker");
    meta_table_checker_.destroy();
    FLOG_INFO("meta table checker destroyed");

    FLOG_INFO("begin to destroy heartbeat process");
    heartbeat_process_.destroy();
    FLOG_INFO("heartbeat process destroyed");

    FLOG_INFO("begin to destroy deadlock event service");
    DEALOCK_EVENT_INSTANCE.destroy();
    FLOG_INFO("deadlock event service destroyed");
  }
  FLOG_INFO("[OBSERVICE_NOTICE] destroy ob_service end", KR(ret));
  return ret;
}


// used by standby cluster
int ObService::update_baseline_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
    ObMultiVersionSchemaService *schema_service = gctx_.schema_service_;
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_version));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", KR(ret));
  } else if (OB_FAIL(schema_service->update_baseline_schema_version(
             OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to update baseline schema version", KR(ret), K(schema_version));
  } else {
    LOG_INFO("update baseline schema version success", K(schema_version));
  }
  return ret;
}

const ObAddr &ObService::get_self_addr()
{
  return gctx_.self_addr();
}

int ObService::submit_ls_update_task(
    const uint64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_table_updater_.async_update(tenant_id, ls_id))) {
    LOG_WARN("fail to async update log stream", KR(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObService::submit_async_refresh_schema_task(
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(schema_updater_.async_refresh_schema(tenant_id, schema_version))) {
    LOG_WARN("fail to async refresh schema", KR(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

// should return success if all partition have merge to specific frozen_version
int ObService::check_frozen_scn(const obrpc::ObCheckFrozenScnArg &arg)
{
  LOG_INFO("receive check frozen SCN request", K(arg));
  int ret = OB_SUCCESS;
  SCN last_merged_scn = SCN::min_scn();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (arg.frozen_scn_ != last_merged_scn) {
    ret = OB_ERR_CHECK_DROP_COLUMN_FAILED;
    LOG_WARN("last merged version not match", KR(ret), K(arg), K(last_merged_scn));
  }
  return ret;
}

int ObService::get_min_sstable_schema_version(
    const obrpc::ObGetMinSSTableSchemaVersionArg &arg,
    obrpc::ObGetMinSSTableSchemaVersionRes &result)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = gctx_.schema_service_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < arg.tenant_id_arg_list_.size(); ++i) {
      // The minimum schema_version used by storage will increase with the major version,
      // storage only need to keep schema history used by a certain number major version.
      // For storage, there is no need to the server level statistics.
      // min_schema_version = scheduler.get_min_schema_version(arg.tenant_id_arg_list_.at(i));
      int tmp_ret = OB_SUCCESS;
      const uint64_t tenant_id = arg.tenant_id_arg_list_.at(i);
      int64_t min_schema_version = 0;
      int64_t tmp_min_schema_version = 0;
      if (OB_TMP_FAIL(schema_service->get_recycle_schema_version(
                         tenant_id, min_schema_version))) {
        min_schema_version = OB_INVALID_VERSION;
        LOG_WARN("fail to get recycle schema version", KR(tmp_ret), K(tenant_id));
      } else {
        MTL_SWITCH(tenant_id) {
          if (OB_TMP_FAIL(MTL(compaction::ObTenantTabletScheduler *)->get_min_dependent_schema_version(tmp_min_schema_version))) {
            min_schema_version = OB_INVALID_VERSION;
            if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              LOG_WARN("failed to get min dependent schema version", K(tmp_ret));
            }
          } else if (tmp_min_schema_version != OB_INVALID_VERSION) {
            min_schema_version = MIN(min_schema_version, tmp_min_schema_version);
          }
        } else {
          if (OB_TENANT_NOT_IN_SERVER != ret) {
            STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result.ret_list_.push_back(min_schema_version))) {
        LOG_WARN("push error", KR(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObService::calc_column_checksum_request(const obrpc::ObCalcColumnChecksumRequestArg &arg, obrpc::ObCalcColumnChecksumRequestRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(arg));
  } else {
    // schedule unique checking task
    const uint64_t tenant_id = arg.tenant_id_;
    int saved_ret = OB_SUCCESS;
    MTL_SWITCH(tenant_id) {
      ObGlobalUniqueIndexCallback *callback = NULL;
      ObTenantDagScheduler* dag_scheduler = nullptr;
      if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret));
      } else if (OB_FAIL(res.ret_codes_.reserve(arg.calc_items_.count()))) {
        LOG_WARN("reserve return code array failed", K(ret), K(arg.calc_items_.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.calc_items_.count(); ++i) {
          const ObCalcColumnChecksumRequestArg::SingleItem &calc_item = arg.calc_items_.at(i);
          ObUniqueCheckingDag *dag = NULL;
          int tmp_ret = OB_SUCCESS;
          saved_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(DDL_SIM(tenant_id, arg.task_id_, CALC_COLUMN_CHECKSUM_RPC_SLOW))) {
            LOG_WARN("ddl sim failure: calcualte column checksum rpc slow", K(tmp_ret), K(tenant_id), K(arg.task_id_));
          } else if (OB_TMP_FAIL(dag_scheduler->alloc_dag(dag))) {
            STORAGE_LOG(WARN, "fail to alloc dag", KR(tmp_ret));
          } else if (OB_TMP_FAIL(dag->init(arg.tenant_id_,
                                           calc_item.ls_id_,
                                           calc_item.tablet_id_,
                                           calc_item.calc_table_id_ == arg.target_table_id_,
                                           arg.target_table_id_,
                                           arg.schema_version_,
                                           arg.task_id_,
                                           arg.execution_id_,
                                           arg.snapshot_version_))) {
            STORAGE_LOG(WARN, "fail to init ObUniqueCheckingDag", KR(tmp_ret));
          } else if (OB_TMP_FAIL(dag->alloc_global_index_task_callback(calc_item.tablet_id_,
                                                                       arg.target_table_id_,
                                                                       arg.source_table_id_,
                                                                       arg.schema_version_,
                                                                       arg.task_id_,
                                                                       callback))) {
            STORAGE_LOG(WARN, "fail to alloc global index task callback", KR(tmp_ret));
          } else if (OB_TMP_FAIL(dag->alloc_unique_checking_prepare_task(callback))) {
            STORAGE_LOG(WARN, "fail to alloc unique checking prepare task", KR(tmp_ret));
          } else if (OB_TMP_FAIL(dag_scheduler->add_dag(dag))) {
            saved_ret = tmp_ret;
            if (OB_EAGAIN == tmp_ret) {
              tmp_ret = OB_SUCCESS;
            } else if (OB_SIZE_OVERFLOW == tmp_ret) {
              tmp_ret = OB_EAGAIN;
            } else {
              STORAGE_LOG(WARN, "fail to add dag to queue", KR(tmp_ret));
            }
          }
          saved_ret = OB_SUCCESS != saved_ret ? saved_ret : tmp_ret;
          if (OB_SUCCESS != saved_ret && NULL != dag) {
            dag_scheduler->free_dag(*dag);
            dag = NULL;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(res.ret_codes_.push_back(tmp_ret))) {
              LOG_WARN("push back return code failed", K(ret), K(tmp_ret));
            }
          }
        }
      }
    }
    LOG_INFO("receive column checksum request", K(arg));
  }
  return ret;
}

int ObService::fetch_sys_ls(share::ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(fill_ls_replica(OB_SYS_TENANT_ID, SYS_LS, replica))) {
    LOG_WARN("fetch_sys_ls failed", KR(ret), K(replica));
  } else {
    LOG_INFO("fetch sys_ls succeed", K(replica));
  }
  return ret;
}

int ObService::backup_ls_data(const obrpc::ObBackupDataArg &arg)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP] receive backup ls data rpc", K(arg));
  ObBackupJobDesc job_desc;
  job_desc.job_id_ = arg.job_id_;
  job_desc.task_id_ = arg.task_id_;
  job_desc.trace_id_ = arg.trace_id_;
  share::ObBackupDest backup_dest;
  uint64_t tenant_id = arg.tenant_id_;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = arg.backup_set_id_;
  backup_set_desc.backup_type_.type_ = arg.backup_type_;
  const ObLSID &ls_id = arg.ls_id_;
  const int64_t turn_id = arg.turn_id_;
  const int64_t retry_id = arg.retry_id_;
  const ObBackupDataType &backup_data_type = arg.backup_data_type_;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (!arg.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, arg.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupHandler::schedule_backup_data_dag(job_desc,
      backup_dest, tenant_id, backup_set_desc, ls_id, turn_id, retry_id, backup_data_type))) {
    LOG_WARN("failed to schedule backup data dag", K(ret), K(arg));
  } else {
    const char *backup_event_str = NULL;
    if (backup_data_type.is_sys_backup()) {
      backup_event_str = "schedule_backup_ls_sys_data";
    } else if (backup_data_type.is_minor_backup()) {
      backup_event_str = "schedule_backup_ls_minor_data";
    } else if (backup_data_type.is_major_backup()) {
      backup_event_str = "schedule_backup_ls_major_data";
    } else {
      backup_event_str = "unknown";
    }
    SERVER_EVENT_ADD("backup_data", backup_event_str,
      "tenant_id", arg.tenant_id_,
      "backup_set_id", arg.backup_set_id_,
      "ls_id", arg.ls_id_.id(),
      "turn_id", arg.turn_id_,
      "retry_id", arg.retry_id_,
      "trace_id", arg.trace_id_);
    LOG_INFO("success recevied backup ls data rpc", K(arg));
  }
  return ret;
}

int ObService::backup_completing_log(const obrpc::ObBackupComplLogArg &arg)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP] receive backup completing log rpc", K(arg));
  ObBackupJobDesc job_desc;
  job_desc.job_id_ = arg.job_id_;
  job_desc.task_id_ = arg.task_id_;
  job_desc.trace_id_ = arg.trace_id_;
  share::ObBackupDest backup_dest;
  uint64_t tenant_id = arg.tenant_id_;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = arg.backup_set_id_;
  backup_set_desc.backup_type_.type_ = arg.backup_type_;
  SCN start_scn = arg.start_scn_;
  SCN end_scn = arg.end_scn_;
  ObLSID ls_id = arg.ls_id_;
  const bool is_only_calc_stat = arg.is_only_calc_stat_;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (!arg.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, arg.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupHandler::schedule_backup_complement_log_dag(
      job_desc, backup_dest, tenant_id, backup_set_desc, ls_id, start_scn, end_scn, is_only_calc_stat))) {
    LOG_WARN("failed to schedule backup data dag", KR(ret), K(arg));
  } else {
    SERVER_EVENT_ADD("backup_data", "schedule_backup_complement_log",
      "tenant_id", arg.tenant_id_,
      "backup_set_id", arg.backup_set_id_,
      "ls_id", arg.ls_id_.id(),
      "start_scn", arg.start_scn_,
      "end_scn", arg.end_scn_,
      "trace_id", arg.trace_id_);
    LOG_INFO("success recevied backup compl log rpc", K(arg));
  }
  return ret;
}

int ObService::backup_build_index(const obrpc::ObBackupBuildIdxArg &arg)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP] receive backup build index rpc", K(arg));
  ObBackupJobDesc job_desc;
  job_desc.job_id_ = arg.job_id_;
  job_desc.task_id_ = arg.task_id_;
  job_desc.trace_id_ = arg.trace_id_;
  share::ObBackupDest backup_dest;
  uint64_t tenant_id = arg.tenant_id_;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = arg.backup_set_id_;
  backup_set_desc.backup_type_.type_ = arg.backup_type_;
  const int64_t turn_id = arg.turn_id_;
  const int64_t retry_id = arg.retry_id_;
  const share::ObBackupDataType backup_data_type = arg.backup_data_type_;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (!arg.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, arg.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(arg));
  } else if (OB_FAIL(ObBackupHandler::schedule_build_tenant_level_index_dag(job_desc,
      backup_dest, tenant_id, backup_set_desc, turn_id, retry_id, backup_data_type))) {
    LOG_WARN("failed to schedule backup data dag", K(ret), K(arg));
  } else {
    SERVER_EVENT_ADD("backup_data", "schedule_build_tenant_level_index",
      "tenant_id", arg.tenant_id_,
      "backup_set_id", arg.backup_set_id_,
      "turn_id", arg.turn_id_,
      "backup_data_type", backup_data_type.type_,
      "job_id", arg.job_id_,
      "trace_id", arg.trace_id_);
  }
  LOG_INFO("success recevied backup build index rpc", K(ret), K(arg));
  return ret;
}

int ObService::backup_meta(const obrpc::ObBackupMetaArg &arg)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP] receive backup meta rpc", K(arg));
  ObBackupJobDesc job_desc;
  job_desc.job_id_ = arg.job_id_;
  job_desc.task_id_ = arg.task_id_;
  job_desc.trace_id_ = arg.trace_id_;
  share::ObBackupDest backup_dest;
  uint64_t tenant_id = arg.tenant_id_;
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = arg.backup_set_id_;
  backup_set_desc.backup_type_.type_ = arg.backup_type_;
  const ObLSID &ls_id = arg.ls_id_;
  const int64_t turn_id = arg.turn_id_;
  const int64_t retry_id = arg.retry_id_;
  const SCN start_scn = arg.start_scn_;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (!arg.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(arg));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy, tenant_id, arg.backup_path_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(arg));
  } else if (OB_FAIL(ObBackupHandler::schedule_backup_meta_dag(job_desc,
      backup_dest, tenant_id, backup_set_desc, ls_id, turn_id, retry_id, start_scn))) {
    LOG_WARN("failed to schedule backup data dag", KR(ret), K(arg));
  } else {
    SERVER_EVENT_ADD("backup_data", "schedule_backup_ls_meta",
      "tenant_id", arg.tenant_id_,
      "backup_set_id", arg.backup_set_id_,
      "ls_id", arg.ls_id_.id(),
      "turn_id", arg.turn_id_,
      "retry_id", arg.retry_id_,
      "trace_id", arg.trace_id_);
    LOG_INFO("success recevied backup ls meta rpc", K(arg));
  }
  return ret;
}

int ObService::check_backup_task_exist(const ObBackupCheckTaskArg &arg, bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      ObTenantDagScheduler* dag_scheduler = nullptr;
      if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, dag scheduler must not be nullptr", K(ret));
      } else if (OB_FAIL(dag_scheduler->check_dag_net_exist(arg.trace_id_, res))) {
        LOG_WARN("failed to check dag net exist", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObService::delete_backup_ls_task(const obrpc::ObLSBackupCleanArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive delete backup ls task request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ObLSBackupCleanScheduler::schedule_backup_clean_dag(arg))) {
    LOG_WARN("failed to schedule backup clean dag", K(ret), K(arg));
  } else {
    LOG_INFO("success receive delete backup ls task rpc", K(arg));
  }

  return ret;
}

int ObService::notify_archive(const obrpc::ObNotifyArchiveArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("recieve notify archive request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_){
      archive::ObArchiveService *archive_service = MTL(ObArchiveService*);
      if (OB_ISNULL(archive_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null MTL scheduler", K(ret), KP(archive_service));
      } else {
        archive_service->wakeup();
        LOG_INFO("succeed to notify archive service", K(arg));
      }
    }
  }

  return ret;
}

int ObService::check_sys_task_exist(
    const share::ObTaskId &arg, bool &res)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (arg.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.task_exist(arg, res))) {
    LOG_WARN("failed to check task exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_migrate_task_exist(
    const share::ObTaskId &arg, bool &res)
{
  int ret = OB_NOT_SUPPORTED;

  LOG_ERROR("not supported", K(ret), K(arg), K(res));
  return ret;
}

int ObService::minor_freeze(const obrpc::ObMinorFreezeArg &arg,
                            obrpc::Int64 &result)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("receive minor freeze request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (arg.ls_id_.is_valid() || arg.tablet_id_.is_valid()) {
    ret = handle_ls_freeze_req_(arg);
  } else if (arg.tenant_ids_.count() > 0) {
    ret = handle_tenant_freeze_req_(arg);
  } else {
    ret = handle_server_freeze_req_(arg);
  }

  result = ret;
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish minor freeze request", K(ret), K(arg), K(cost_ts));
  return ret;
}

int ObService::handle_server_freeze_req_(const obrpc::ObMinorFreezeArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "failed to get multi tenant from GCTX", K(ret));
  } else {
    omt::TenantIdList all_tenants;
    GCTX.omt_->get_tenant_ids(all_tenants);
    for (int i = 0; i < all_tenants.size(); ++i) {
      int tmp_ret = OB_SUCCESS;
      uint64_t tenant_id = all_tenants[i];
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = tenant_freeze_(tenant_id)))) {
        if (OB_TENANT_NOT_IN_SERVER == tmp_ret) {
          LOG_INFO("skip freeze stopped tenant", K(tmp_ret), K(tenant_id));
          tmp_ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to freeze tenant", K(tmp_ret), K(tenant_id));
        }
      }
      // record the first error code
      if (OB_SUCCESS != tmp_ret && OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObService::handle_tenant_freeze_req_(const obrpc::ObMinorFreezeArg &arg)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < arg.tenant_ids_.count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    uint64_t tenant_id = arg.tenant_ids_.at(i);
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = tenant_freeze_(tenant_id)))) {
      LOG_WARN("fail to freeze tenant", K(tmp_ret), K(tenant_id));
    }
    // record the first error code
    if (OB_SUCCESS != tmp_ret && OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObService::handle_ls_freeze_req_(const obrpc::ObMinorFreezeArg &arg)
{
  int ret = OB_SUCCESS;
  if (1 != arg.tenant_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only one tenant is needed", K(ret), K(arg.tenant_ids_), K(arg.tablet_id_));
  } else if (OB_FAIL(ls_freeze_(arg.tenant_ids_.at(0), arg.ls_id_, arg.tablet_id_))) {
    LOG_WARN("fail to freeze tablet", K(ret), K(arg));
  }
  return ret;
}

int ObService::ls_freeze_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (is_virtual_tenant_id(tenant_id)) {
    LOG_INFO("no need to freeze virtual tenant", K(ret), K(tenant_id), K(tablet_id));
  } else {
    MTL_SWITCH(tenant_id) {
      storage::ObTenantFreezer* freezer = nullptr;
      if (OB_ISNULL(freezer = MTL(storage::ObTenantFreezer*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObTenantFreezer shouldn't be null", K(ret), K(tenant_id));
      } else if (tablet_id.is_valid()) {
        // tablet freeze
        if (OB_FAIL(freezer->tablet_freeze(ls_id, tablet_id))) {
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to freeze tablet", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
          }
        } else {
          LOG_INFO("succeed to freeze tablet", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        }
      } else {
        // logstream freeze
        if (OB_FAIL(freezer->ls_freeze(ls_id))) {
          if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to freeze ls", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
          }
        } else {
          LOG_INFO("succeed to freeze ls", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        }
      }
    }
  }

  return ret;
}

int ObService::tenant_freeze_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (is_virtual_tenant_id(tenant_id)) {
    LOG_INFO("no need to freeze virtual tenant", K(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      storage::ObTenantFreezer* freezer = nullptr;
      if (OB_ISNULL(freezer = MTL(storage::ObTenantFreezer*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObTenantFreezer shouldn't be null", K(ret), K(tenant_id));
      } else if (freezer->exist_ls_freezing()) {
        LOG_INFO("exist running ls_freeze", K(ret), K(tenant_id));
      } else if (OB_FAIL(freezer->tenant_freeze())) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to freeze tenant", K(tenant_id), K(ret));
        }
      } else {
        LOG_INFO("succeed to freeze tenant", K(tenant_id), K(ret));
      }
    } else {
      LOG_WARN("fail to switch tenant", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObService::tablet_major_freeze(const obrpc::ObTabletMajorFreezeArg &arg,
                            obrpc::Int64 &result)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  LOG_INFO("receive tablet major freeze request", K(arg));

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      if (OB_FAIL(MTL(compaction::ObTenantTabletScheduler *)->try_schedule_tablet_medium_merge(
        arg.ls_id_, arg.tablet_id_, arg.is_rebuild_column_group_))) {
        LOG_WARN("failed to try schedule tablet major freeze", K(ret), K(arg));
      }
    }
  }

  result = ret;
  const int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
  LOG_INFO("finish tablet major freeze request", K(ret), K(arg), K(cost_ts));
  return ret;
}

int ObService::check_modify_time_elapsed(
    const obrpc::ObCheckModifyTimeElapsedArg &arg,
    obrpc::ObCheckModifyTimeElapsedResult &result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive get checksum cal snapshot", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      ObLSHandle ls_handle;
      SCN tmp_scn;
      transaction::ObTransService *txs = MTL(transaction::ObTransService *);
      ObLSService *ls_service = MTL(ObLSService *);
      if (OB_FAIL(result.results_.reserve(arg.tablets_.count()))) {
        LOG_WARN("reserve result array failed", K(ret), K(arg.tablets_.count()));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
        ObTabletHandle tablet_handle;
        ObLSHandle ls_handle;
        const ObLSID &ls_id = arg.tablets_.at(i).ls_id_;
        const ObTabletID &tablet_id = arg.tablets_.at(i).tablet_id_;
        SCN snapshot_version;
        ObCheckTransElapsedResult single_result;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(DDL_SIM(arg.tenant_id_, arg.ddl_task_id_, CHECK_MODIFY_TIME_ELAPSED_SLOW))) {
          LOG_WARN("ddl sim failure: check modify time elapsed slow", K(tmp_ret), K(arg.tenant_id_), K(arg.ddl_task_id_));
        } else if (OB_TMP_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
          LOG_WARN("get ls failed", K(tmp_ret), K(ls_id));
        } else if (OB_TMP_FAIL(ls_handle.get_ls()->check_modify_time_elapsed(tablet_id,
                                                                             arg.sstable_exist_ts_,
                                                                             single_result.pending_tx_id_))) {
          if (OB_EAGAIN != tmp_ret) {
            LOG_WARN("check schema version elapsed failed", K(tmp_ret), K(arg));
          }
        } else if (OB_TMP_FAIL(txs->get_max_commit_version(snapshot_version))) {
          LOG_WARN("fail to get max commit version", K(tmp_ret));
        } else {
          single_result.snapshot_ = snapshot_version.get_val_for_tx();
        }
        if (OB_SUCC(ret)) {
          single_result.ret_code_ = tmp_ret;
          if (OB_FAIL(result.results_.push_back(single_result))) {
            LOG_WARN("push back single result failed", K(ret), K(i), K(single_result));
          }
        }
      }
    }
  }
  return ret;
}

int ObService::check_schema_version_elapsed(
    const obrpc::ObCheckSchemaVersionElapsedArg &arg,
    obrpc::ObCheckSchemaVersionElapsedResult &result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive check schema version elapsed", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    MTL_SWITCH(arg.tenant_id_) {
      ObLSService *ls_service = nullptr;
      if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, get ls service failed", K(ret));
      } else if (OB_FAIL(result.results_.reserve(arg.tablets_.count()))) {
        LOG_WARN("reserve result array failed", K(ret), K(arg.tablets_.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablets_.count(); ++i) {
        ObTabletHandle tablet_handle;
        ObLSHandle ls_handle;
        const ObLSID &ls_id = arg.tablets_.at(i).ls_id_;
        const ObTabletID &tablet_id = arg.tablets_.at(i).tablet_id_;
        ObCheckTransElapsedResult single_result;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(DDL_SIM(arg.tenant_id_, arg.ddl_task_id_, CHECK_SCHEMA_TRANS_END_SLOW))) {
          LOG_WARN("ddl sim failure: check schema version elapsed slow", K(tmp_ret), K(arg));
        } else if (OB_TMP_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
          LOG_WARN("get ls failed", K(tmp_ret), K(i), K(ls_id));
        } else if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
          LOG_WARN("fail to get tablet", K(tmp_ret), K(i), K(ls_id), K(tablet_id));
        } else if (OB_TMP_FAIL(tablet_handle.get_obj()->check_schema_version_elapsed(arg.schema_version_,
                                                                                     arg.need_wait_trans_end_,
                                                                                     single_result.snapshot_,
                                                                                     single_result.pending_tx_id_))) {
          LOG_WARN("check schema version elapsed failed", K(tmp_ret), K(arg), K(ls_id), K(tablet_id));
        }
        if (OB_SUCC(ret)) {
          single_result.ret_code_ = tmp_ret;
          if (OB_FAIL(result.results_.push_back(single_result))) {
            LOG_WARN("push back single result failed", K(ret), K(i), K(single_result));
          }
        }
      }
    }
  }
  return ret;
}

int ObService::check_ddl_tablet_merge_status(
    const obrpc::ObDDLCheckTabletMergeStatusArg &arg,
    obrpc::ObDDLCheckTabletMergeStatusResult &result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    result.reset();
    MTL_SWITCH(arg.tenant_id_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
        const common::ObTabletID &tablet_id = arg.tablet_ids_.at(i);
        ObTabletHandle tablet_handle;
        ObLSHandle ls_handle;
        ObDDLKvMgrHandle ddl_kv_mgr_handle;
        ObLSService *ls_service = nullptr;
        bool status = false;

        if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, get ls service failed", K(ret));
        } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid arguments", K(ret), K(arg));
        } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
          LOG_WARN("get ls failed", K(ret), K(arg));
        } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
          LOG_WARN("get tablet failed", K(ret));
        }
        // check and update major status
        if (OB_SUCC(ret)) {
          ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
          if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
            LOG_WARN("fail to fetch table store", K(ret));
          } else {
            ObSSTable *latest_major_sstable = static_cast<ObSSTable *>(
              table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/));
            status = nullptr != latest_major_sstable;
            if (OB_FAIL(result.merge_status_.push_back(status))) {
              LOG_WARN("fail to push back to array", K(ret), K(status), K(tablet_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObService::batch_switch_rs_leader(const ObAddr &arg)
{
  UNUSEDx(arg);
  int ret = OB_NOT_SUPPORTED;
  // LOG_INFO("receive batch switch rs leader request", K(arg));

  // int64_t start_timestamp = ObTimeUtility::current_time();
  // if (OB_UNLIKELY(!inited_)) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("not init", KR(ret));
  // } else if (OB_ISNULL(gctx_.par_ser_)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("gctx par_ser is NULL", K(arg));
  // } else if (!arg.is_valid()) {
  //   if (OB_FAIL(gctx_.par_ser_->auto_batch_change_rs_leader())) {
  //     LOG_WARN("fail to auto batch change rs leader", KR(ret));
  //   }
  // } else if (OB_FAIL(gctx_.par_ser_->batch_change_rs_leader(arg))) {
  //   LOG_WARN("fail to batch change rs leader", K(arg), KR(ret));
  // }

  // int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  // SERVER_EVENT_ADD("election", "batch_switch_rs_leader", K(ret),
  //                  "leader", arg,
  //                  K(cost));
  return ret;
}

int ObService::switch_schema(
    const obrpc::ObSwitchSchemaArg &arg,
    obrpc::ObSwitchSchemaResult &result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to switch schema", K(arg));
  const ObRefreshSchemaInfo &schema_info = arg.schema_info_;
  const int64_t schema_version = schema_info.get_schema_version();
  const uint64_t tenant_id = schema_info.get_tenant_id();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",  KR(ret), K(schema_version));
  } else if (arg.is_async_) {
    const bool set_received_schema_version = true;
    if (OB_FAIL(schema_updater_.try_reload_schema(
        schema_info, set_received_schema_version))) {
      LOG_WARN("reload schema failed", KR(ret), K(schema_info));
    }
  } else {
    ObSEArray<uint64_t, 1> tenant_ids;
    ObMultiVersionSchemaService *schema_service = gctx_.schema_service_;
    int64_t local_schema_version = OB_INVALID_VERSION;
    int64_t abs_timeout = OB_INVALID_TIMESTAMP;
    if (OB_UNLIKELY(!schema_info.is_valid() || !is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid schema info", KR(ret), K(schema_info), K(tenant_id));
    } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret));
    } else if (OB_FAIL(ObShareUtil::get_abs_timeout(GCONF.rpc_timeout, abs_timeout))) {
      LOG_WARN("fail to get abs timeout", KR(ret), "default_timeout", static_cast<int64_t>(GCONF.rpc_timeout));
    } else {
      // To set the received_schema_version period in advance,
      // let refresh_schema can execute before analyze_dependencies logic;
      int64_t LEFT_TIME = 200 * 1000;// 200ms
      int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
      if (INT64_MAX != origin_timeout_ts
          && origin_timeout_ts >= ObTimeUtility::current_time() + LEFT_TIME) {
        THIS_WORKER.set_timeout_ts(origin_timeout_ts - LEFT_TIME);
      }
      if (OB_FAIL(schema_service->async_refresh_schema(tenant_id, schema_version))) {
        LOG_WARN("fail to async schema version", KR(ret), K(tenant_id), K(schema_version));
      }
      THIS_WORKER.set_timeout_ts(origin_timeout_ts);
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = schema_service->set_tenant_received_broadcast_version(tenant_id, schema_version))) {
        LOG_WARN("failt to update received schema version", KR(tmp_ret), K(tenant_id), K(schema_version));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      if (THIS_WORKER.is_timeout_ts_valid()
          && !THIS_WORKER.is_timeout()
          && OB_TIMEOUT == ret) {
        // To set set_tenant_received_broadcast_version in advance, we reduce the abs_time,
        // if not timeout after first async_refresh_schema, we should execute async_refresh_schema again and overwrite the ret code
        if (OB_FAIL(schema_service->async_refresh_schema(tenant_id, schema_version))) {
          LOG_WARN("fail to async schema version", KR(ret), K(tenant_id), K(schema_version));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (schema_info.get_schema_version() <= 0) {
        // skip
      } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(
                         tenant_id, local_schema_version))) {
        LOG_WARN("fail to get local tenant schema_version", KR(ret), K(tenant_id));
      } else if (OB_UNLIKELY(schema_info.get_schema_version() > local_schema_version)) {
        ret = OB_EAGAIN;
        LOG_WARN("schema is not new enough", KR(ret), K(schema_info), K(local_schema_version));
      }
    }
  }
  LOG_INFO("switch schema", K(ret), K(schema_info));
  //SERVER_EVENT_ADD("schema", "switch_schema", K(ret), K(schema_info));
  result.set_ret(ret);
  return ret;
}

int ObService::broadcast_consensus_version(
    const obrpc::ObBroadcastConsensusVersionArg &arg,
    obrpc::ObBroadcastConsensusVersionRes &result)
{
  int ret = OB_SUCCESS;
  int64_t local_consensus_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = arg.get_tenant_id();
  const int64_t consensus_version = arg.get_consensus_version();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(consensus_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",  KR(ret), K(consensus_version));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_broadcast_consensus_version(tenant_id, local_consensus_version))) {
    LOG_WARN("fail to get local tenant consensus_version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(consensus_version < local_consensus_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("consensus version is less than local consensus version", KR(ret), K(consensus_version), K(local_consensus_version));
  } else if (OB_FAIL(gctx_.schema_service_->set_tenant_broadcast_consensus_version(tenant_id, consensus_version))) {
    LOG_WARN("failt to update received schema version", KR(ret), K(tenant_id), K(consensus_version));
  }
  result.set_ret(ret);
  return OB_SUCCESS;
}

int ObService::bootstrap(const obrpc::ObBootstrapArg &arg)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 600 * 1000 * 1000LL; // 10 minutes
  const obrpc::ObServerInfoList &rs_list = arg.server_list_;
  LOG_INFO("bootstrap timeout", K(timeout), "worker_timeout_ts", THIS_WORKER.get_timeout_ts());
  if (!inited_) {
    ret = OB_NOT_INIT;
    BOOTSTRAP_LOG(WARN, "not init", K(ret));
  } else if (rs_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    BOOTSTRAP_LOG(WARN, "rs_list is empty", K(rs_list), K(ret));
  } else {
    ObPreBootstrap pre_bootstrap(*gctx_.srv_rpc_proxy_,
                                 rs_list,
                                 *gctx_.lst_operator_,
                                 *gctx_.config_,
                                 arg,
                                 *gctx_.rs_rpc_proxy_);
    ObAddr master_rs;
    bool server_empty = false;
    ObCheckServerEmptyArg new_arg;
    new_arg.mode_ = ObCheckServerEmptyArg::BOOTSTRAP;
    if (OB_FAIL(check_server_empty(server_empty))) {
      BOOTSTRAP_LOG(WARN, "check_server_empty failed", K(ret), K(new_arg));
    } else if (!server_empty) {
      ret = OB_ERR_SYS;
      BOOTSTRAP_LOG(WARN, "this observer is not empty", KR(ret), K(GCTX.self_addr()));
    } else if (OB_FAIL(pre_bootstrap.prepare_bootstrap(master_rs))) {
      BOOTSTRAP_LOG(ERROR, "failed to prepare boot strap", K(rs_list), K(ret));
    } else {
      const ObCommonRpcProxy &rpc_proxy = *gctx_.rs_rpc_proxy_;
      bool boot_done = false;
      const int64_t MAX_RETRY_COUNT = 30;
      for (int i = 0; !boot_done && i < MAX_RETRY_COUNT; i++) {
        ret = OB_SUCCESS;
        int64_t rpc_timeout = timeout;
        if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
          rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
        }
        if (OB_FAIL(rpc_proxy.to_addr(master_rs).timeout(rpc_timeout)
                    .execute_bootstrap(arg))) {
          if (OB_RS_NOT_MASTER == ret) {
            BOOTSTRAP_LOG(INFO, "master root service not ready",
                          K(master_rs), "retry_count", i, K(rpc_timeout), K(ret));
            USLEEP(200 * 1000);
          } else {
            const ObAddr rpc_svr = rpc_proxy.get_server();
            BOOTSTRAP_LOG(ERROR, "execute bootstrap fail", KR(ret), K(rpc_svr), K(master_rs), K(rpc_timeout));
            break;
          }
        } else {
          boot_done = true;
        }
      }
      if (boot_done) {
        BOOTSTRAP_LOG(INFO, "succeed to do_boot_strap", K(rs_list), K(master_rs));
      }
    }
  }

  return ret;
}

int ObService::check_deployment_mode_match(
    const obrpc::ObCheckDeploymentModeArg &arg,
    obrpc::Bool &match)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool single_zone_deployment_on = OB_FILE_SYSTEM_ROUTER.is_single_zone_deployment_on();
    if (single_zone_deployment_on == arg.single_zone_deployment_on_) {
      match = true;
    } else {
      match = false;
      LOG_INFO("deployment mode not match", K(single_zone_deployment_on), K(arg));
    }
  }
  return ret;
}

int ObService::is_empty_server(const obrpc::ObCheckServerEmptyArg &arg, obrpc::Bool &is_empty)
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("fail to get sys data version", KR(ret));
  } else if (arg.sys_data_version_ > 0
             && sys_data_version > arg.sys_data_version_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("add server with larger sys data version is not supported",
             KR(ret), K(arg), K(sys_data_version));
  } else {
    bool server_empty = false;
    if (OB_FAIL(check_server_empty(server_empty))) {
      LOG_WARN("check_server_empty failed", K(ret));
    } else {
      is_empty = server_empty;
    }
  }
  return ret;
}
int ObService::check_server_for_adding_server(
    const obrpc::ObCheckServerForAddingServerArg &arg,
    obrpc::ObCheckServerForAddingServerResult &result)
{
  int ret = OB_SUCCESS;
  uint64_t sys_tenant_data_version = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_tenant_data_version))) {
    LOG_WARN("fail to get sys tenant data version", KR(ret));
  } else if (arg.get_sys_tenant_data_version() > 0
      && sys_tenant_data_version > arg.get_sys_tenant_data_version()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("adding server with larger sys tenant data version is not supported",
        KR(ret), K(arg), K(sys_tenant_data_version), K(arg.get_sys_tenant_data_version()));
  } else {
    bool server_empty = false;
    char build_version[common::OB_SERVER_VERSION_LENGTH] = {'\0'};
    if (OB_FAIL(check_server_empty(server_empty))) {
      LOG_WARN("check_server_empty failed", KR(ret));
    } else if (OB_FAIL(get_package_and_svn(build_version, sizeof(build_version)))) {
      LOG_WARN("fail to get build_version", KR(ret));
    } else {
      ObServerInfoInTable::ObBuildVersion build_version_string;
      ObZone zone;
      int64_t sql_port = GCONF.mysql_port;

      if (OB_SUCC(ret) && server_empty) {
        uint64_t server_id = arg.get_server_id();
        GCTX.server_id_ = server_id;
        GCONF.observer_id = server_id;
        if (OB_ISNULL(GCTX.config_mgr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("GCTX.config_mgr_ is null", KR(ret));
        } else if (OB_FAIL(GCTX.config_mgr_->dump2file())) {
          LOG_ERROR("fail to execute dump2file, this server cannot be added, "
              "please clear it and try again", KR(ret));
        }
      }
      if (FAILEDx(zone.assign(GCONF.zone.str()))) {
        LOG_WARN("fail to assign zone", KR(ret), K(GCONF.zone.str()));
      } else if (OB_FAIL(build_version_string.assign(build_version))) {
        LOG_WARN("fail to assign build version", KR(ret), K(build_version));
      } else if (OB_FAIL(result.init(
          server_empty,
          zone,
          sql_port,
          build_version_string))) {
        LOG_WARN("fail to init result", KR(ret), K(server_empty), K(zone), K(sql_port),
            K(build_version_string));
      } else {}
    }
  }
  FLOG_INFO("[CHECK_SERVER_EMPTY] generate result", KR(ret), K(arg), K(result));
  return ret;
}

int ObService::get_server_resource_info(
    const obrpc::ObGetServerResourceInfoArg &arg,
    obrpc::ObGetServerResourceInfoResult &result)
{
  int ret = OB_SUCCESS;
  const ObAddr &my_addr = GCONF.self_addr_;
  share::ObServerResourceInfo resource_info;
  result.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(get_server_resource_info(resource_info))) {
    LOG_WARN("fail to get server resource info", KR(ret));
  } else if (OB_FAIL(result.init(my_addr, resource_info))) {
    LOG_WARN("fail to init result", KR(ret), K(my_addr), K(resource_info));
  }
  FLOG_INFO("get server resource info", KR(ret), K(arg), K(result));
  return ret;
}
int ObService::get_server_resource_info(share::ObServerResourceInfo &resource_info)
{
  int ret = OB_SUCCESS;
  omt::ObTenantNodeBalancer::ServerResource svr_res_assigned;
  int64_t clog_in_use_size_byte = 0;
  int64_t clog_total_size_byte = 0;
  logservice::ObServerLogBlockMgr *log_block_mgr = GCTX.log_block_mgr_;
  resource_info.reset();
  int64_t reserved_size = 4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(log_block_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log_block_mgr is null", KR(ret), K(GCTX.log_block_mgr_));
  } else if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().get_server_allocated_resource(svr_res_assigned))) {
    LOG_WARN("fail to get server allocated resource", KR(ret));
  } else if (OB_FAIL(log_block_mgr->get_disk_usage(clog_in_use_size_byte, clog_total_size_byte))) {
    LOG_WARN("Failed to get clog stat ", KR(ret));
  } else if (OB_FAIL(SLOGGERMGR.get_reserved_size(reserved_size))) {
    LOG_WARN("Failed to get reserved size ", KR(ret));
  } else {
    resource_info.cpu_ = get_cpu_count();
    resource_info.report_cpu_assigned_ = svr_res_assigned.min_cpu_;
    resource_info.report_cpu_max_assigned_ = svr_res_assigned.max_cpu_;
    resource_info.report_mem_assigned_ = svr_res_assigned.memory_size_;
    resource_info.mem_in_use_ = 0;
    resource_info.mem_total_ = GMEMCONF.get_server_memory_avail();
    resource_info.disk_total_
        = OB_SERVER_BLOCK_MGR.get_max_macro_block_count(reserved_size) * OB_SERVER_BLOCK_MGR.get_macro_block_size();
    resource_info.disk_in_use_
        = OB_SERVER_BLOCK_MGR.get_used_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size();
    resource_info.log_disk_total_ = clog_total_size_byte;
    resource_info.report_log_disk_assigned_ = svr_res_assigned.log_disk_size_;

  }
  return ret;
}
int ObService::get_build_version(share::ObServerInfoInTable::ObBuildVersion &build_version)
{
  int ret = OB_SUCCESS;
  char build_version_char_array[common::OB_SERVER_VERSION_LENGTH] = {0};
  build_version.reset();
  if (OB_FAIL(get_package_and_svn(build_version_char_array, sizeof(build_version_char_array)))) {
    LOG_WARN("fail to get build_version", KR(ret));
  } else if (OB_FAIL(build_version.assign(build_version_char_array))) {
    LOG_WARN("fail to assign build_version", KR(ret), K(build_version_char_array));
  }
  return ret;
}
int ObService::get_partition_count(obrpc::ObGetPartitionCountResult &result)
{
  UNUSEDx(result);
  int ret = OB_NOT_SUPPORTED;
  // result.reset();

  // if (!inited_) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("not inited", K(ret));
  // } else if (OB_FAIL(gctx_.par_ser_->get_partition_count(result.partition_count_))) {
  //   LOG_WARN("failed to get partition count", K(ret));
  // }
  return ret;
}

int ObService::do_add_ls_replica(const obrpc::ObLSAddReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  ObLSService *ls_service = nullptr;
  bool is_exist = false;
  ObMigrationOpArg migration_op_arg;
  if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRpcLSAddReplicaP::process tenant not match", KR(ret), K(tenant_id));
  }
  ObCurTraceId::set(arg.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_add start", "tenant_id", arg.tenant_id_, "ls_id", arg.ls_id_.id(),
                     "data_src", arg.data_source_.get_server(), "dest", arg.dst_.get_server());
    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_service->check_ls_exist(arg.ls_id_, is_exist))) {
      LOG_WARN("failed to check ls exist", KR(ret), K(arg));
    } else if (is_exist) {
      ret = OB_LS_EXIST;
      LOG_WARN("can not add ls which local ls is exist", KR(ret), K(arg), K(is_exist));
    } else {
      migration_op_arg.cluster_id_ = GCONF.cluster_id;
      migration_op_arg.data_src_ = arg.force_data_source_;
      migration_op_arg.dst_ = arg.dst_;
      migration_op_arg.ls_id_ = arg.ls_id_;
      //TODO(muwei.ym) need check priority in 4.2 RC3
      migration_op_arg.priority_ = ObMigrationOpPriority::PRIO_HIGH;
      migration_op_arg.paxos_replica_number_ = arg.new_paxos_replica_number_;
      migration_op_arg.src_ = arg.dst_;
      migration_op_arg.type_ = ObMigrationOpType::ADD_LS_OP;
      if (OB_FAIL(ls_service->create_ls_for_ha(arg.task_id_, migration_op_arg))) {
        LOG_WARN("failed to create ls for ha", KR(ret), K(arg), K(migration_op_arg));
      }
    }
  }
  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "schedule_ls_add failed", "tenant_id", arg.tenant_id_,
        "ls_id", arg.ls_id_, "result", ret);
  }
  return ret;
}

int ObService::do_remove_ls_paxos_replica(const obrpc::ObLSDropPaxosReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  ObCurTraceId::set(arg.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_paxos_member start", "tenant_id", arg.tenant_id_, "ls_id", arg.ls_id_.id(),
                     "dest", arg.remove_member_.get_server());
    LOG_INFO("start do remove ls paxos member", K(arg));
    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(arg));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(arg));
    } else if (OB_ISNULL(ls->get_ls_remove_member_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls remove handler should not be NULL", KR(ret), K(arg));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->remove_paxos_member(arg))) {
      LOG_WARN("failed to remove paxos member", KR(ret), K(arg));
    }
  }
  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_paxos_member failed", "tenant_id",
        arg.tenant_id_, "ls_id", arg.ls_id_.id(), "result", ret);
  }
  return ret;
}

int ObService::do_remove_ls_nonpaxos_replica(const obrpc::ObLSDropNonPaxosReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = arg.tenant_id_;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (tenant_id != MTL_ID()) {
    ret = guard.switch_to(tenant_id);
  }
  ObCurTraceId::set(arg.task_id_);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_learner_member start", "tenant_id", arg.tenant_id_, "ls_id", arg.ls_id_.id(),
                     "dest", arg.remove_member_.get_server());
    LOG_INFO("start do remove ls learner member", K(arg));
    ls_service = MTL(ObLSService*);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(arg));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", KR(ret), K(arg));
    } else if (OB_ISNULL(ls->get_ls_remove_member_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls remove handler should not be NULL", KR(ret), K(arg));
    } else if (OB_FAIL(ls->get_ls_remove_member_handler()->remove_learner_member(arg))) {
      LOG_WARN("failed to remove paxos member", KR(ret), K(arg));
    }
  }
  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("storage_ha", "remove_ls_learner_member failed", "tenant_id",
        arg.tenant_id_, "ls_id", arg.ls_id_.id(), "result", ret);
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObService::convert_tenant_max_key_version(
    const ObIArray<std::pair<uint64_t, ObLeaseResponse::TLRpKeyVersion> > &max_key_version,
    ObIArray<std::pair<uint64_t, uint64_t> > &got_version_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_key_version.count(); ++i) {
      const std::pair<uint64_t, ObLeaseResponse::TLRpKeyVersion> &key_version
        = max_key_version.at(i);
      std::pair<uint64_t, uint64_t> got_version;
      got_version.first = key_version.first;
      got_version.second = key_version.second.max_key_version_;
      if (OB_FAIL(got_version_array.push_back(got_version))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObService::do_wait_master_key_in_sync(
    const common::ObIArray<std::pair<uint64_t, uint64_t> > &got_version_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t abs_time = THIS_WORKER.get_timeout_ts();
    int64_t finished_idx = -1;
    const int64_t SLEEP_INTERVAL = 50L * 1000L;
    LOG_INFO("do wait master key in sync", K(abs_time));
    while (ObTimeUtility::current_time() < abs_time && OB_SUCC(ret)) {
      for (int64_t i = finished_idx + 1; OB_SUCC(ret) && i < got_version_array.count(); ++i) {
        uint64_t max_stored_key_version = 0;
        const std::pair<uint64_t, uint64_t> &got_version = got_version_array.at(i);
        if (OB_FAIL(ObMasterKeyGetter::instance().get_max_stored_version(
                got_version.first, max_stored_key_version))) {
          LOG_WARN("fail to get max active version", KR(ret),
                   "tenant_id", got_version.first);
        } else if (max_stored_key_version >= got_version.second) {
          finished_idx = i;
        } else {
          // TODO: wenduo
          // remove got_versions after renqing make got_versions has a retry logic in it
          (void)ObMasterKeyGetter::instance().got_versions(got_version_array);
          ob_usleep(std::min(SLEEP_INTERVAL, abs_time - ObTimeUtility::current_time()));
          break;
        }
      }
      if (OB_FAIL(ret)) {
        // failed
      } else if (finished_idx >= got_version_array.count() - 1) {
        break; // succ
        LOG_INFO("wait master key in sync succ");
      } else if (ObTimeUtility::current_time() >= abs_time) {
        ret = OB_TIMEOUT;
        LOG_WARN("fail master key in sync timeout", KR(ret), K(finished_idx));
      } else {
        // still need wait
      }
    }
  }
  return ret;
}

int ObService::trigger_tenant_config(
    const obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(ret));
  } else {
    // ignore ret in for condition
    for (int64_t i = 0; i < wms_in_sync_arg.tenant_config_version_.count(); ++i) {
      const uint64_t tenant_id = wms_in_sync_arg.tenant_config_version_.at(i).first;
      const int64_t version = wms_in_sync_arg.tenant_config_version_.at(i).second;
      OTC_MGR.add_tenant_config(tenant_id); // ignore ret
      OTC_MGR.got_version(tenant_id, version); // ignore ret
    }
  }
  return ret;
}

int ObService::wait_master_key_in_sync(
    const obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg)
{
  int ret = OB_SUCCESS;
  common::ObArray<std::pair<uint64_t, uint64_t> > got_version_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(broadcast_rs_list(wms_in_sync_arg.rs_list_arg_))) {
    LOG_WARN("fail to broadcast rs list", KR(ret), "rs_list_arg", wms_in_sync_arg.rs_list_arg_);
  } else if (wms_in_sync_arg.tenant_max_key_version_.count() <= 0) {
    // bypass, since tenant max key version is empty
  } else if (OB_FAIL(convert_tenant_max_key_version(
          wms_in_sync_arg.tenant_max_key_version_, got_version_array))) {
    LOG_WARN("fail to convert tenant max key version", KR(ret), K(wms_in_sync_arg));
  } else {
    ObRefreshSchemaInfo schema_info;
    const bool set_received_schema_version = false;
    if (OB_FAIL(schema_updater_.try_reload_schema(
        schema_info, set_received_schema_version))) {
      LOG_WARN("fail to try reload schema", KR(ret));
    } else if (OB_FAIL(trigger_tenant_config(wms_in_sync_arg))) {
      LOG_WARN("fail to got versions", KR(ret));
    } else if (OB_FAIL(ObMasterKeyGetter::instance().got_versions(got_version_array))) {
      LOG_WARN("fail to get versions", KR(ret));
    } else if (OB_FAIL(do_wait_master_key_in_sync(got_version_array))) {
      LOG_WARN("fail to do wait master key in sync", KR(ret));
    }
  }
  return ret;
}
#endif

int ObService::check_server_empty(bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t server_id_in_GCONF = GCONF.observer_id;
    if (is_empty) {
      if (is_valid_server_id(GCTX.server_id_) || is_valid_server_id(server_id_in_GCONF)) {
        is_empty = false;
        FLOG_WARN("[CHECK_SERVER_EMPTY] server_id exists", K(GCTX.server_id_), K(server_id_in_GCONF));
      }
    }
    if (is_empty) {
      if (!OBSERVER.is_log_dir_empty()) {
        FLOG_WARN("[CHECK_SERVER_EMPTY] log dir is not empty");
        is_empty = false;
      }
    }
#ifdef OB_BUILD_TDE_SECURITY
    if (is_empty) {
      if (ObMasterKeyGetter::instance().is_wallet_exist()) {
        FLOG_WARN("[CHECK_SERVER_EMPTY] master_key file exists");
        is_empty = false;
      }
    }
#endif
  }
  return ret;
}

int ObService::report_replica()
{
  // TODO: yanyuan.cxf this maybe need at 4.0
  // return OB_SUCCESS just for bootstrap.
  int ret = OB_SUCCESS;
  // LOG_INFO("receive report all replicas request");
  // if (!inited_) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("not init", K(ret));
  // } else {
  //   ObIPartitionGroupIterator *partition_iter = NULL;
  //   ObIPartitionGroup *partition = NULL;
  //   int64_t replica_count = 0;

  //   if (NULL == (partition_iter = gctx_.par_ser_->alloc_pg_iter())) {
  //     ret = OB_ALLOCATE_MEMORY_FAILED;
  //     LOG_WARN("Fail to alloc partition iter, ", K(ret));
  //   } else {
  //     while (OB_SUCC(ret)) {
  //       ObPartitionArray pkeys;
  //       if (OB_FAIL(partition_iter->get_next(partition))) {
  //         if (OB_ITER_END != ret) {
  //           LOG_WARN("Fail to get next partition, ", K(ret));
  //         }
  //       } else if (NULL == partition) {
  //         ret = OB_ERR_UNEXPECTED;
  //         LOG_WARN("The partition is NULL, ", K(ret));
  //       } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
  //         LOG_WARN("get all pg partition keys error", "pg_key", partition->get_partition_key());
  //         if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
  //           LOG_WARN("get partition failed", K(ret));
  //         } else {
  //           // The partition has been deleted. There is no need to trigger the report
  //           ret = OB_SUCCESS;
  //         }
  //       } else if (OB_FAIL(submit_pt_update_task(
  //               partition->get_partition_key(),
  //               true/*need report checksum*/))) {
  //         if (OB_PARTITION_NOT_EXIST == ret) {
  //           // The GC thread is already working,
  //           // and deleted during traversal, the replica has been deleted needs to be avoided blocking the start process
  //           ret = OB_SUCCESS;
  //           LOG_INFO("this partition is already not exist",
  //               K(ret), "partition_key", partition->get_partition_key());
  //         } else {
  //           LOG_WARN("submit partition table update task failed",
  //               K(ret), "partition_key", partition->get_partition_key());
  //         }
  //       } else if (OB_FAIL(submit_pt_update_role_task(
  //               partition->get_partition_key()))) {
  //         LOG_WARN("fail to submit pt update role task", K(ret),
  //                  "pkey", partition->get_partition_key());
  //       } else {
  //         //Update partition meta table without concern for error codes
  //         submit_pg_pt_update_task(pkeys);
  //         ++replica_count;
  //       }
  //     }

  //     if (OB_ITER_END == ret) {
  //       ret = OB_SUCCESS;
  //     }
  //   }

  //   if (NULL != partition_iter) {
  //     gctx_.par_ser_->revert_pg_iter(partition_iter);
  //   }
  //   LOG_INFO("submit all replicas report", K(ret));
  //   SERVER_EVENT_ADD("storage", "report_replica", K(ret), "replica_count", replica_count);
  // }
  return ret;
}

int ObService::recycle_replica()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    //gctx_.par_ser_->garbage_clean();
  }
  return ret;
}

int ObService::clear_location_cache()
{
  // TODO baihua: implement after kvcache support clear interface
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObService::set_ds_action(const obrpc::ObDebugSyncActionArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(GDS.set_global_action(arg.reset_, arg.clear_, arg.action_))) {
    LOG_WARN("set debug sync global action failed", K(ret), K(arg));
  }
  return ret;
}
int ObService::request_heartbeat(ObLeaseRequest &lease_request)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(heartbeat_process_.init_lease_request(lease_request))) {
    LOG_WARN("init_lease_request failed", K(ret));
  }
  return ret;
}

// used by bootstrap/create_tenant
int ObService::batch_broadcast_schema(
    const obrpc::ObBatchBroadcastSchemaArg &arg,
    ObBatchBroadcastSchemaResult &result)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = gctx_.schema_service_;
  const int64_t sys_schema_version = arg.get_sys_schema_version();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->async_refresh_schema(
             OB_SYS_TENANT_ID, sys_schema_version))) {
    LOG_WARN("fail to refresh sys schema", KR(ret), K(sys_schema_version));
  } else if (OB_FAIL(schema_service->broadcast_tenant_schema(
             arg.get_tenant_id(), arg.get_tables()))) {
    LOG_WARN("fail to broadcast tenant schema", KR(ret), K(arg));
  }
  result.set_ret(ret);
  return ret;
}

// get tenant's refreshed schema version in new mode
int ObService::get_tenant_refreshed_schema_version(
    const obrpc::ObGetTenantSchemaVersionArg &arg,
    obrpc::ObGetTenantSchemaVersionResult &result)
{
  int ret = OB_SUCCESS;
  result.schema_version_ = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
             arg.tenant_id_, result.schema_version_, false/*core_version*/))) {
    LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(arg));
  }
  return ret;
}

int ObService::sync_partition_table(const obrpc::Int64 &arg)
{
  return OB_NOT_SUPPORTED;
}

int ObService::set_tracepoint(const obrpc::ObAdminSetTPArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    EventItem item;
    item.error_code_ = arg.error_code_;
    item.occur_ = arg.occur_;
    item.trigger_freq_ = arg.trigger_freq_;
    item.cond_ = arg.cond_;
    if (arg.event_name_.length() > 0) {
      ObSqlString str;
      if (OB_FAIL(str.assign(arg.event_name_))) {
        LOG_WARN("string assign failed", K(ret));
      } else if (OB_FAIL(EventTable::instance().set_event(str.ptr(), item))) {
        LOG_WARN("Failed to set tracepoint event, tp_name does not exist.", K(ret), K(arg.event_name_));
      }
    } else if (OB_FAIL(EventTable::instance().set_event(arg.event_no_, item))) {
      LOG_WARN("Failed to set tracepoint event, tp_no does not exist.", K(ret), K(arg.event_no_));
    }
    LOG_INFO("set event", K(arg));
  }
  return ret;
}

int ObService::cancel_sys_task(
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_id));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.cancel_task(task_id))) {
    LOG_WARN("failed to cancel sys task", K(ret), K(task_id));
  }
  return ret;
}

int ObService::get_all_partition_status(int64_t &inactive_num, int64_t &total_num) const
{
  UNUSEDx(inactive_num, total_num);
  int ret = OB_NOT_SUPPORTED;
  // if (!inited_) {
  //   ret = OB_NOT_INIT;
  //   LOG_WARN("not init", K(ret));
  // } else if (OB_FAIL(gctx_.par_ser_->get_all_partition_status(inactive_num, total_num))) {
  //   LOG_WARN("fail to get all partition status", K(ret));
  // }
  return ret;
}

int ObService::detect_master_rs_ls(
    const ObDetectMasterRsArg &arg,
    obrpc::ObDetectMasterRsLSResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t local_cluster_id = GCONF.cluster_id;
  const ObAddr &self_addr = gctx_.self_addr();
  ObAddr master_rs;
  ObLSReplica replica;
  ObLSInfo ls_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(gctx_.root_service_) || OB_ISNULL(gctx_.rs_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("rs or rs_mgr is null", KR(ret));
  } else if (!arg.is_valid()
             || arg.get_cluster_id() != local_cluster_id
             || arg.get_addr() != self_addr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg), K(local_cluster_id), K(self_addr));
  } else if (OB_FAIL(gctx_.rs_mgr_->get_master_root_server(master_rs))) {
    LOG_WARN("fail to get master rs", KR(ret));
  } else if (OB_FAIL(fill_ls_replica(OB_SYS_TENANT_ID, SYS_LS, replica))) {
    if (OB_LS_NOT_EXIST == ret) {
      // case 1 : replica not exist
      ret = OB_SUCCESS;
      result.reset(); // only master_rs valid
      result.set_master_rs(master_rs);
    } else {
      LOG_WARN("fail to fill ls replica", KR(ret), K(SYS_LS));
    }
  } else if (replica.is_strong_leader()) {
    // case 2 : replica is leader, do not use in_service to check whether it is leader or not
    //          use in_service could lead to bad case:
    //          For more info, to see docs on yuque rootservice/pbw2qw
    if (OB_FAIL(generate_master_rs_ls_info_(replica, ls_info))) {
      LOG_WARN("generate master rs ls info failed", KR(ret), K(replica), K(ls_info));
    } else if (OB_FAIL(result.init(ObRole::LEADER, master_rs, replica, ls_info))) {
      LOG_WARN("fail to init result", KR(ret), K(master_rs), K(replica), K(ls_info));
    }
  } else {
    // case 3 : replica is follower
    replica.set_role(ObRole::FOLLOWER);
    ls_info.reset();
    result.reset(); // only ls info is invalid
    if (OB_FAIL(result.init(ObRole::FOLLOWER, master_rs, replica, ls_info))) {
      LOG_WARN("fail to init result", KR(ret), K(master_rs), K(replica), K(ls_info));
    }
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  const int64_t DETECT_THRESHOLD = 2 * 1000 * 1000L; // 2s
  if (cost_ts >= DETECT_THRESHOLD) {
    FLOG_WARN("detect rs too much time", KR(ret), K(cost_ts), K(result));
  }
  LOG_TRACE("detect rs cost", KR(ret), K(cost_ts), K(result));
  return ret;
}

// Use the local leader replica as ls_info by default, while trying to get full ls info from inmemory ls table.
// If proposal_id and server of inmemory leader and cur_leader are same, ls_info = cur_leader + inmemory followers.
int ObService::generate_master_rs_ls_info_(
    const share::ObLSReplica &cur_leader,
    share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  ObInMemoryLSTable *inmemory_ls_table = NULL;
  const ObLSReplica *inmemory_leader = NULL;
  ObLSInfo inmemory_ls_info;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(gctx_.lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!cur_leader.is_valid()
      || !cur_leader.is_strong_leader()
      || cur_leader.get_server() != gctx_.self_addr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current leader", KR(ret), K(cur_leader), "self_addr", gctx_.self_addr());
  } else if (OB_FAIL(ls_info.init_by_replica(cur_leader))) {
    LOG_WARN("init by replica failed", KR(ret), K(cur_leader));
  } else if (OB_ISNULL(inmemory_ls_table = gctx_.lst_operator_->get_inmemory_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inmemory ls_table is null", KR(ret), KP(inmemory_ls_table));
  } else if (OB_UNLIKELY(!inmemory_ls_table->is_inited())) {
    // if RS is not started, inmemory_ls_table may be uninitialized
  } else if (OB_FAIL(inmemory_ls_table->get(
      GCONF.cluster_id,
      OB_SYS_TENANT_ID,
      SYS_LS,
      share::ObLSTable::DEFAULT_MODE,
      inmemory_ls_info))) {
    LOG_WARN("failed to get in memory sys tenant ls info", KR(ret), K(inmemory_ls_info));
  } else if (OB_FAIL(inmemory_ls_info.find_leader(inmemory_leader))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // ls replica hasn't been reported to memory
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to find leader in inmemory_ls_info", KR(ret), K(inmemory_ls_info));
    }
  } else if (OB_ISNULL(inmemory_leader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica can not be null", KR(ret), K(inmemory_ls_info));
  } else if (inmemory_leader->get_proposal_id() != cur_leader.get_proposal_id()
      || inmemory_leader->get_server() != cur_leader.get_server()) {
    // do not use unreliable inmemory ls info
  } else {
    ARRAY_FOREACH(inmemory_ls_info.get_replicas(), idx) {
      const ObLSReplica &replica = inmemory_ls_info.get_replicas().at(idx);
      if (!replica.is_strong_leader()) {
        if (OB_FAIL(ls_info.add_replica(replica))) {
          LOG_WARN("add replica failed", KR(ret), K(replica), K(ls_info), K(inmemory_ls_info));
        }
      }
    }
    if (FAILEDx(ls_info.update_replica_status())) {
      LOG_WARN("update replica status failed", KR(ret),
          K(ls_info), K(cur_leader), K(inmemory_ls_info));
    }
  }
  return ret;
}

int ObService::get_root_server_status(ObGetRootserverRoleResult &get_role_result)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  ObLSReplica replica;
  ObRole role = FOLLOWER;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(gctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid root_service", KR(ret));
  } else if (OB_FAIL(fill_ls_replica(tenant_id, SYS_LS, replica))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to fill log_stream replica", KR(ret), K(tenant_id), K(SYS_LS));
    }
  } else {
    role = replica.get_role();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_role_result.init(role, gctx_.root_service_->get_status()))) {
      LOG_WARN("fail to init a ObGetRootserverRoleResult", KR(ret), K(replica));
    }
  }
  return ret;
}

int ObService::refresh_sys_tenant_ls()
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  int64_t expire_renew_time = INT64_MAX;
  bool is_cache_hit = false;
  share::ObLSLocation location;
  if (OB_UNLIKELY(nullptr == GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service ptr is null", KR(ret));
  } else if (OB_FAIL(GCTX.location_service_->get(
          cluster_id, tenant_id, SYS_LS, expire_renew_time, is_cache_hit, location))) {
    LOG_WARN("fail to refresh sys tenant log stream",
             KR(ret), K(cluster_id), K(tenant_id), K(SYS_LS));
  } else {
#if !defined(NDEBUG)
    LOG_INFO("refresh sys tenant log stream success", K(location));
#endif
  }
  return ret;
}

int ObService::stop_partition_write(const obrpc::Int64 &switchover_timestamp, obrpc::Int64 &result)
{
  //TODO for switchover
  int ret = OB_SUCCESS;
  result = switchover_timestamp;
  return ret;
}

int ObService::check_partition_log(const obrpc::Int64 &switchover_timestamp, obrpc::Int64 &result)
{
  UNUSEDx(switchover_timestamp, result);
  // Check that the log of all replicas in local have reached synchronization status
  // The primary has stopped writing
  int ret = OB_NOT_SUPPORTED;

  // ObTenantDagScheduler is at tenant level now, check all tenants task
  // int64_t balance_task = 0;
  // omt::TenantIdList all_tenants;
  // all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  // if (OB_ISNULL(GCTX.omt_)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("failed to get multi tenant from GCTX", K(ret));
  // } else {
  //   GCTX.omt_->get_tenant_ids(all_tenants);
  //   for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
  //     uint64_t tenant_id = all_tenants[i];
  //     if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
  //       if (OB_SUCC(guard.switch_to(tenant_id))) {
  //         ObTenantDagScheduler *scheduler = nullptr;
  //         if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
  //           ret = OB_ERR_UNEXPECTED;
  //           FLOG_WARN("MTL ObTenantDagScheduler is NULL", K(ret), K(scheduler), K(tenant_id));
  //         } else {
  //           balance_task += scheduler->get_dag_count(ObDagType::DAG_TYPE_MIGRATE);
  //         }
  //       }
  //     }
  //   }
  // }

  // if (OB_SUCC(ret)) {
  //   if (balance_task > 0) {
  //     ret = OB_EAGAIN;
  //     result = switchover_timestamp;
  //     LOG_INFO("observer already has task to do", K(switchover_timestamp), K(balance_task));
  //   } else if (OB_FAIL(gctx_.par_ser_->check_all_partition_sync_state(switchover_timestamp))) {
  //     LOG_WARN("fail to check_all_partition_sync_state", K(ret));
  //   } else {
  //     result = switchover_timestamp;
  //   }
  // }
  return ret;
}

int ObService::estimate_partition_rows(const obrpc::ObEstPartArg &arg,
                                       obrpc::ObEstPartRes &res) const
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("receive estimate rows request", K(arg));
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is not inited", K(ret));
  } else if (OB_FAIL(sql::ObStorageEstimator::estimate_row_count(arg, res))) {
    LOG_WARN("failed to estimate partition rowcount", K(ret));
  }
  return ret;
}

int ObService::get_wrs_info(const obrpc::ObGetWRSArg &arg,
                            obrpc::ObGetWRSResult &result)
{
  UNUSEDx(arg, result);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObService::refresh_memory_stat()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMemoryDump::get_instance().check_sql_memory_leak())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "there has sql memory leak");
  }
  return ret;
}

int ObService::wash_memory_fragmentation()
{
  ObMallocAllocator::get_instance()->sync_wash();
  return OB_SUCCESS;
}

int ObService::renew_in_zone_hb(
    const share::ObInZoneHbRequest &arg,
    share::ObInZoneHbResponse &result)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  UNUSED(result);
  return ret;
}

int ObService::broadcast_rs_list(const ObRsListArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service do not init", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr is null", KR(ret), K(arg));
  } else if (OB_FAIL(GCTX.rs_mgr_->force_set_master_rs(arg.master_rs_))) {
    LOG_WARN("fail to set master rs", KR(ret), K(arg));
  } else {
    LOG_INFO("observer set master rs success", K(arg));
  }
  return ret;
}

int ObService::build_ddl_single_replica_request(const ObDDLBuildSingleReplicaRequestArg &arg,
                                                ObDDLBuildSingleReplicaRequestResult &res)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive build single replica request", K(arg));
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else {
    if (is_complement_data_relying_on_dag(ObDDLType(arg.ddl_type_))) {
      int saved_ret = OB_SUCCESS;
      ObTenantDagScheduler *dag_scheduler = nullptr;
      ObComplementDataDag *dag = nullptr;
      if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag scheduler is null", K(ret));
      } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
        LOG_WARN("fail to alloc dag", K(ret));
      } else if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, dag is null", K(ret), KP(dag));
      } else if (OB_FAIL(dag->init(arg))) {
        LOG_WARN("fail to init complement data dag", K(ret), K(arg));
      } else if (OB_FAIL(dag->create_first_task())) {
        LOG_WARN("create first task failed", K(ret));
      } else if (OB_FAIL(dag_scheduler->add_dag(dag))) {
        saved_ret = ret;
        LOG_WARN("add dag failed", K(ret), K(arg));
        if (OB_EAGAIN == saved_ret) {
          dag_scheduler->get_complement_data_dag_progress(dag, res.row_scanned_, res.row_inserted_);
        }
      } else {
        dag = nullptr;
      }
      if (OB_NOT_NULL(dag)) {
        (void) dag->handle_init_failed_ret_code(ret);
        dag_scheduler->free_dag(*dag);
        dag = nullptr;
      }
      if (OB_FAIL(ret)) {
        // RS does not retry send RPC to tablet leader when the dag exists.
        ret = OB_EAGAIN == saved_ret ? OB_SUCCESS : ret;
        ret = OB_SIZE_OVERFLOW == saved_ret ? OB_EAGAIN : ret;
      }
      LOG_INFO("obs get rpc to build drop column dag", K(ret));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported ddl type", K(ret), K(arg));
    }

  }
  return ret;
}

int ObService::check_and_cancel_ddl_complement_data_dag(const ObDDLBuildSingleReplicaRequestArg &arg, bool &is_dag_exist)
{
  int ret = OB_SUCCESS;
  is_dag_exist = true;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_UNLIKELY(!is_complement_data_relying_on_dag(ObDDLType(arg.ddl_type_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ddl type", K(ret), K(arg));
  } else {
    ObTenantDagScheduler *dag_scheduler = nullptr;
    ObComplementDataDag *dag = nullptr;
    if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag scheduler is null", K(ret));
    } else if (OB_FAIL(dag_scheduler->alloc_dag(dag))) {
      LOG_WARN("fail to alloc dag", K(ret));
    } else if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, dag is null", K(ret), KP(dag));
    } else if (OB_FAIL(dag->init(arg))) {
      LOG_WARN("fail to init complement data dag", K(ret), K(arg));
    } else if (OB_FAIL(dag_scheduler->check_dag_exist(dag, is_dag_exist))) {
      LOG_WARN("check dag exist failed", K(ret));
    } else if (is_dag_exist && OB_FAIL(dag_scheduler->cancel_dag(dag))) {
      // sync to cancel ready dag only, not including running dag.
      LOG_WARN("cancel dag failed", K(ret));
    }
    if (OB_NOT_NULL(dag)) {
      (void) dag->handle_init_failed_ret_code(ret);
      dag_scheduler->free_dag(*dag);
      dag = nullptr;
    }
  }
  if (REACH_COUNT_INTERVAL(1000L)) {
    LOG_INFO("receive cancel ddl complement dag request", K(ret), K(is_dag_exist), K(arg));
  }
  return ret;
}

int ObService::inner_fill_tablet_info_(
    const int64_t tenant_id,
    const ObTabletID &tablet_id,
    storage::ObLS *ls,
    ObTabletReplica &tablet_replica,
    share::ObTabletReplicaChecksumItem &tablet_checksum,
    const bool need_checksum)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", KR(ret));
  } else if (!tablet_id.is_valid()
             || OB_INVALID_TENANT_ID == tenant_id
             || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument or nullptr", KR(ret), K(tablet_id), K(tenant_id));
  } else if (OB_ISNULL(ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_tablet_svr is null", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(
      tablet_id,
      tablet_handle,
      0,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("get tablet failed", KR(ret), K(tenant_id), K(tablet_id));
    }
  } else if (OB_ISNULL(gctx_.config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gctx_.config_ is null", KR(ret), K(tenant_id), K(tablet_id));
  } else {
    const common::ObAddr &addr = gctx_.self_addr();
    const int64_t snapshot_version = tablet_handle.get_obj()->get_tablet_meta().report_status_.merge_snapshot_version_;
    const ObLSID &ls_id = ls->get_ls_id();
    int64_t data_size = 0;
    int64_t required_size = 0;
    ObArray<int64_t> column_checksums;
    if (OB_FAIL(tablet_handle.get_obj()->get_tablet_report_info(snapshot_version, column_checksums,
        data_size, required_size, need_checksum))) {
      LOG_WARN("fail to get tablet report info from tablet", KR(ret), K(tenant_id), K(tablet_id));
    } else if (OB_FAIL(tablet_replica.init(
        tenant_id,
        tablet_id,
        ls_id,
        addr,
        snapshot_version,
        data_size,
        required_size,
        0/*report_scn*/,
        ObTabletReplica::SCN_STATUS_IDLE))) {
      LOG_WARN("fail to init a tablet replica", KR(ret), K(tenant_id),
          K(tablet_id), K(tablet_replica));
    } else if (!need_checksum) {
    } else if (OB_FAIL(tablet_checksum.set_tenant_id(tenant_id))) {
      LOG_WARN("failed to set tenant id", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tablet_checksum.column_meta_.init(column_checksums))) {
      LOG_WARN("fail to init report column meta with column_checksums", KR(ret), K(column_checksums));
    } else if (OB_FAIL(tablet_checksum.compaction_scn_.convert_for_tx(snapshot_version))) {
      LOG_WARN("failed to convert scn", KR(ret), K(snapshot_version));
    } else {
      tablet_checksum.ls_id_ = ls->get_ls_id();
      tablet_checksum.tablet_id_ = tablet_id;
      tablet_checksum.server_ = gctx_.self_addr();
      tablet_checksum.row_count_ = tablet_handle.get_obj()->get_tablet_meta().report_status_.row_count_;
      tablet_checksum.data_checksum_ = tablet_handle.get_obj()->get_tablet_meta().report_status_.data_checksum_;
    }
  }
  return ret;
}

int ObService::fill_tablet_report_info(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletReplica &tablet_replica,
    share::ObTabletReplicaChecksumItem &tablet_checksum,
    const bool need_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", KR(ret));
  } else if (!tablet_id.is_valid() || !ls_id.is_valid() || OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(ls_id), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      ObSharedGuard<ObLSIterator> ls_iter;
      ObTabletHandle tablet_handle;
      ObLSHandle ls_handle;
      storage::ObLS *ls = nullptr;
      ObLSService* ls_svr = nullptr;
      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        if (OB_LS_NOT_EXIST != ret) {
          LOG_WARN("fail to get log_stream's ls_handle", KR(ret), K(tenant_id), K(ls_id));
        } else {
          LOG_TRACE("log stream not exist in this tenant", KR(ret), K(tenant_id), K(ls_id));
        }
      } else if (FALSE_IT(ls = ls_handle.get_ls())) {
      } else if (OB_FAIL(inner_fill_tablet_info_(tenant_id,
                                                 tablet_id,
                                                 ls,
                                                 tablet_replica,
                                                 tablet_checksum,
                                                 need_checksum))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to inner fill tenant's tablet replica", KR(ret),
                    K(tenant_id), K(tablet_id), K(ls), K(tablet_replica), K(tablet_checksum), K(need_checksum));
        } else {
          LOG_TRACE("tablet not exist in this log stream", KR(ret),
                    K(tenant_id), K(tablet_id), K(ls), K(tablet_replica), K(tablet_checksum), K(need_checksum));
        }
      }
    }
  }
  return ret;
}

int ObService::get_role_from_palf_(
    logservice::ObLogService &log_service,
    const share::ObLSID &ls_id,
    common::ObRole &role,
    int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  role = FOLLOWER;
  proposal_id = 0;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(log_service.open_palf(ls_id, palf_handle_guard))) {
    LOG_WARN("open palf failed", KR(ret), K(ls_id));
  } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
    LOG_WARN("get role failed", KR(ret), K(ls_id));
  }
  return ret;
}

int ObService::fill_ls_replica(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    share::ObLSReplica &replica)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t unit_id = common::OB_INVALID_ID;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", KR(ret));
  } else if (!ls_id.is_valid()
             || OB_INVALID_TENANT_ID == tenant_id
             || OB_ISNULL(gctx_.config_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(GCTX.omt_->get_unit_id(tenant_id, unit_id))) {
    LOG_WARN("get tenant unit id failed", KR(ret), K(tenant_id), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      ObLSService *ls_svr = nullptr;
      logservice::ObLogService *log_service = nullptr;
      common::ObRole role = FOLLOWER;
      ObMemberList ob_member_list;
      ObLSReplica::MemberList member_list;
      GlobalLearnerList learner_list;
      int64_t proposal_id = 0;
      int64_t paxos_replica_number = 0;
      ObLSRestoreStatus restore_status;
      ObReplicaStatus replica_status = REPLICA_STATUS_NORMAL;
      ObReplicaType replica_type = REPLICA_TYPE_FULL;
      bool is_compatible_with_readonly_replica = false;
      ObMigrationStatus migration_status = OB_MIGRATION_STATUS_MAX;
      if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLSService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ls_svr->get_ls(
            ObLSID(ls_id),
            ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("get ls handle failed", KR(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_paxos_member_list_and_learner_list(ob_member_list, paxos_replica_number, learner_list))) {
        LOG_WARN("get member list and learner list from ObLS failed", KR(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_restore_status(restore_status))) {
        LOG_WARN("get restore status failed", KR(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_migration_status(migration_status))) {
        LOG_WARN("get migration status failed", KR(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_replica_status(replica_status))) {
        LOG_WARN("get replica status failed", KR(ret));
      } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(get_role_from_palf_(*log_service, ls_id, role, proposal_id))) {
        LOG_WARN("failed to get role from palf", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_SUCCESS != (tmp_ret = ObShareUtil::check_compat_version_for_readonly_replica(
                                          tenant_id, is_compatible_with_readonly_replica))) {
        LOG_WARN("fail to check data version for read-only replica", KR(ret), K(tenant_id));
      }

      if (OB_FAIL(ret)) {
      } else if (!is_compatible_with_readonly_replica) {
        replica_type = REPLICA_TYPE_FULL;
      } else if (learner_list.contains(gctx_.self_addr())) {
        // if replica exists in learner_list, report it as R-replica.
        // Otherwise, report as F-replica
        replica_type = REPLICA_TYPE_READONLY;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObLSReplica::transform_ob_member_list(ob_member_list, member_list))) {
        LOG_WARN("fail to transfrom ob_member_list into member_list", KR(ret), K(ob_member_list));
      } else if (OB_FAIL(replica.init(
            0,          /*create_time_us*/
            0,          /*modify_time_us*/
            tenant_id,  /*tenant_id*/
            ls_id,      /*ls_id*/
            gctx_.self_addr(),         /*server*/
            gctx_.config_->mysql_port, /*sql_port*/
            role,                      /*role*/
            replica_type,         /*replica_type*/
            proposal_id,              /*proposal_id*/
            is_strong_leader(role) ? REPLICA_STATUS_NORMAL : replica_status,/*replica_status*/
            restore_status,            /*restore_status*/
            100,                       /*memstore_percent*/
            unit_id,                   /*unit_id*/
            gctx_.config_->zone.str(), /*zone*/
            paxos_replica_number,                    /*paxos_replica_number*/
            0,                         /*data_size*/
            0,                         /*required_size*/
            member_list,
            learner_list,
            OB_MIGRATION_STATUS_REBUILD == migration_status /*is_rebuild*/))) {
        LOG_WARN("fail to init a ls replica", KR(ret), K(tenant_id), K(ls_id), K(role),
                 K(proposal_id), K(unit_id), K(paxos_replica_number), K(member_list), K(learner_list));
      } else {
        LOG_TRACE("finish fill ls replica", KR(ret), K(tenant_id), K(ls_id), K(replica));
      }
    }
  }
  return ret;
}

int ObService::report_backup_over(const obrpc::ObBackupTaskRes &res)
{
  int ret = OB_SUCCESS;
  ObBackupDataLSTask task;
  ObBackupTaskScheduler *task_scheduler = nullptr;
  FLOG_INFO("receive backup over", K(res));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObHAResultInfo result_info(ObHAResultInfo::BACKUP_DATA,
                               res.ls_id_,
                               res.src_server_,
                               res.dag_id_,
                               res.result_);
    MTL_SWITCH(gen_meta_tenant_id(res.tenant_id_)) {
      if (nullptr == (task_scheduler = MTL(ObBackupTaskScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup task scheduler can't be nullptr", K(ret));
      } else if (OB_FAIL(task.build_from_res(res, BackupJobType::BACKUP_DATA_JOB))) {
        LOG_WARN("failed to build task from res rpc", K(ret), K(res));
      } else if (OB_FAIL(task_scheduler->execute_over(task, result_info))) {
        LOG_WARN("failed to remove task from scheduler", K(ret), K(res), K(task));
      }
    }
  }
  return ret;
}

int ObService::report_backup_clean_over(const obrpc::ObBackupTaskRes &res)
{
  int ret = OB_SUCCESS;
  ObBackupCleanLSTask task;
  ObBackupTaskScheduler *task_scheduler = nullptr;
  FLOG_INFO("report backup clean over", K(res));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObHAResultInfo result_info(ObHAResultInfo::BACKUP_CLEAN,
                               res.ls_id_,
                               res.src_server_,
                               res.dag_id_,
                               res.result_);
    MTL_SWITCH(gen_meta_tenant_id(res.tenant_id_)) {
      if (nullptr == (task_scheduler = MTL(ObBackupTaskScheduler *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup task scheduler can't be nullptr", K(ret));
      } else if (OB_FAIL(task.build_from_res(res, BackupJobType::BACKUP_CLEAN_JOB))) {
        LOG_WARN("failed to build task from res rpc", K(ret), K(res));
      } else if (OB_FAIL(task_scheduler->execute_over(task, result_info))) {
        LOG_WARN("failed to remove task from scheduler", K(ret), K(res), K(task));
      }
    }
  }
  return ret;
}

int ObService::get_leader_locations(
    const obrpc::ObGetLeaderLocationsArg &arg,
    obrpc::ObGetLeaderLocationsResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.omt_) || OB_ISNULL(GCTX.config_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.omt_), KP(GCTX.config_));
  } else {
    omt::TenantIdList all_tenants;
    (void) GCTX.omt_->get_tenant_ids(all_tenants);
    int actual_ret = OB_SUCCESS;
    // ignore ret to return more ls leader infos as possible
    for (int64_t i = 0; OB_SUCCESS == actual_ret && i < all_tenants.size(); i++) {
      const uint64_t tenant_id = all_tenants[i];
      if (!is_virtual_tenant_id(tenant_id)) {
      const int64_t tenant_start_ts = ObTimeUtility::current_time();
      MTL_SWITCH(tenant_id) {
        ObLSService *ls_svr = NULL;
        logservice::ObLogService *log_service = NULL;
        ObSharedGuard<storage::ObLSIterator> ls_iter_guard;
        common::ObRole role = FOLLOWER;
        int64_t proposal_id = 0;
        ObReplicaProperty property; // unused now
        ObLSRestoreStatus restore_status;
        ObLSID ls_id;
        ObLSLeaderLocation leader_location;
        ObLS *ls = NULL;
        if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("MTL ObLSService is null", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ls_svr->get_ls_iter(ls_iter_guard, ObLSGetMod::OBSERVER_MOD))) {
          LOG_WARN("fail to get ls iter guard", KR(ret), K(tenant_id));
        }
        while (OB_SUCC(ret) && OB_SUCCESS == actual_ret) {
          if (OB_FAIL(ls_iter_guard->get_next(ls))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next ls", KR(ret));
            }
          } else {
            palf::PalfHandleGuard palf_handle_guard;
            ls_id = ls->get_ls_id();
            if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
              LOG_WARN("open palf failed", KR(ret), K(tenant_id), K(ls_id));
            } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
              LOG_WARN("get role failed", KR(ret), K(tenant_id), K(ls_id));
            } else if (LEADER != role) {
              // skip
            /*
             * This function may be blocked when dick is hang,
             * we consider leader's restore status is always NONE
             * } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
             *   LOG_WARN("get restore status failed", KR(ret));
             */
            } else if (FALSE_IT(restore_status = ObLSRestoreStatus::NONE)) {
            } else if (OB_FAIL(leader_location.init(
                  GCTX.config_->cluster_id,  /*cluster_id*/
                  tenant_id,                 /*tenant_id*/
                  ls_id,                     /*ls_id*/
                  GCTX.self_addr(),          /*server*/
                  role,                      /*role*/
                  GCTX.config_->mysql_port,  /*sql_port*/
                  REPLICA_TYPE_FULL,         /*replica_type*/
                  property,                  /*property*/
                  restore_status,            /*restore_status*/
                  proposal_id                /*proposal_id*/
                  ))) {
              LOG_WARN("fail to init a ls replica", KR(ret), K(tenant_id), K(ls_id), K(role), K(proposal_id));
            } else if (OB_SUCCESS != (actual_ret = result.add_leader_replica(leader_location))) {
              LOG_WARN("fail to add leader replica", KR(actual_ret), K(leader_location));
            }
            ret = OB_SUCCESS; // ignore get leader location error
          }
        } // end iter ls
      } // end MTL
      LOG_TRACE("get tenant leader locations cost", KR(ret), K(tenant_id),
                "cost", ObTimeUtility::current_time() - tenant_start_ts);
      }
    } // end iter tenant
    ret = actual_ret; // overwrite ret
    result.set_addr(GCTX.self_addr());
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  const int64_t FETCH_THRESHOLD = 2 * 1000 * 1000L; // 2s
  if (cost_ts >= FETCH_THRESHOLD) {
    FLOG_WARN("get leader locations cost too much time", KR(ret), K(cost_ts), K(result));
  }
  LOG_TRACE("get leader locations cost", KR(ret), K(cost_ts), K(result));
  return ret;
}

int ObService::check_backup_dest_connectivity(const obrpc::ObCheckBackupConnectivityArg &arg)
{
  int ret = OB_SUCCESS;
  share::ObBackupDestCheck backup_check;
  share::ObBackupPath path;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(path.init(arg.check_path_))) {
    LOG_WARN("failed to init path", K(ret), K(arg));
  } else if (OB_FAIL(backup_check.check_backup_dest_connectivity(arg.tenant_id_, arg.backup_path_, path))) {
    LOG_WARN("failed to check backup dest connectivity", K(ret), K(arg));
  }
  return ret;
}

int ObService::estimate_tablet_block_count(const obrpc::ObEstBlockArg &arg,
                                           obrpc::ObEstBlockRes &res) const
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("receive estimate tablet block count request", K(arg));
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is not inited", K(ret));
  } else if (OB_FAIL(sql::ObStorageEstimator::estimate_block_count_and_row_count(arg, res))) {
    LOG_WARN("failed to estimate block count and row count", K(ret));
  }
  return ret;
}

int ObService::get_ls_sync_scn(
    const ObGetLSSyncScnArg &arg,
    ObGetLSSyncScnRes &result)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_svr = nullptr;
  SCN cur_sync_scn = SCN::min_scn();
  SCN cur_restore_source_max_scn = SCN::min_scn();
  bool restore_to_newest = false;
  LOG_INFO("start get_ls_sync_scn", K(arg));

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.get_tenant_id() != MTL_ID() && OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    logservice::ObLogService *log_ls_svr = MTL(logservice::ObLogService*);
    ObLS *ls = nullptr;
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    logservice::ObLogRestoreHandler *restore_handler = NULL;
    ObLSID ls_id = arg.get_ls_id();
    common::ObRole role;
    int64_t first_leader_epoch = 0;
    int64_t second_leader_epoch = 0;
    if (OB_ISNULL(ls_svr) || OB_ISNULL(log_ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "should not be null", KR(ret), KP(ls_svr), KP(log_ls_svr));
    } else if (OB_FAIL(log_ls_svr->get_palf_role(ls_id, role, first_leader_epoch))) {
      COMMON_LOG(WARN, "failed to get palf role", KR(ret), K(ls_id));
    } else if (!is_strong_leader(role)) {
      ret = OB_NOT_MASTER;
      LOG_WARN("ls on this server is not master", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
      COMMON_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be null", KR(ret), K(ls_id));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), K(ls_id), KP(ls));
    } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get restore_handler failed", KR(ret), K(ls_id), KP(ls));
    // scn get order, sync_scn from leader before archive_scn
    } else if (OB_FAIL(log_handler->get_end_scn(cur_sync_scn))) {
      LOG_WARN("failed to get ls cur_sync_scn", KR(ret), K(ls_id), KPC(ls));
    } else if (arg.check_sync_to_latest()
               && OB_FAIL(restore_handler->check_restore_to_newest(cur_sync_scn, cur_restore_source_max_scn))) {
      LOG_WARN("failed to check_restore_to_newest", KR(ret), K(arg), KPC(ls));
    } else if (OB_FAIL(result.init(arg.get_tenant_id(), ls_id, cur_sync_scn, cur_restore_source_max_scn))) {
      LOG_WARN("failed to init res", KR(ret), K(arg.get_tenant_id()), K(ls_id), K(cur_sync_scn),
                                     K(cur_restore_source_max_scn));
    } else if (OB_FAIL(log_ls_svr->get_palf_role(ls_id, role, second_leader_epoch))) {
      COMMON_LOG(WARN, "failed to get palf role", KR(ret), K(ls_id));
    } else if (first_leader_epoch != second_leader_epoch || !is_strong_leader(role)) {
      ret = OB_NOT_MASTER;
      LOG_WARN("the ls not master", KR(ret), K(ls_id), K(first_leader_epoch),
          K(second_leader_epoch), K(role));
    }
  }
  LOG_INFO("finish get_ls_sync_scn", KR(ret), K(cur_sync_scn), K(cur_restore_source_max_scn), K(arg), K(result));
  return ret;
}

int ObService::force_set_ls_as_single_replica(
    const ObForceSetLSAsSingleReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_svr = nullptr;
  LOG_INFO("force_set_ls_as_single_replica", K(arg));

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.get_tenant_id() != MTL_ID() && OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    ls_svr = MTL(ObLSService*);
    logservice::ObLogService *log_ls_svr = MTL(logservice::ObLogService*);
    ObLS *ls = nullptr;
    ObLSHandle handle;
    logservice::ObLogHandler *log_handler = NULL;
    logservice::ObLogRestoreHandler *restore_handler = NULL;
    ObLSID ls_id = arg.get_ls_id();
    if (OB_ISNULL(ls_svr) || OB_ISNULL(log_ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "should not be null", KR(ret), KP(ls_svr), KP(log_ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::OBSERVER_MOD))) {
      COMMON_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "ls should not be null", KR(ret), K(ls_id));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is null", KR(ret), K(ls_id), KP(ls));
    } else if (OB_FAIL(log_handler->force_set_as_single_replica())) {
      LOG_WARN("failed to force_set_as_single_replica", KR(ret), K(ls_id), KPC(ls));
    }
  }
  LOG_INFO("finish force_set_ls_as_single_replica", KR(ret), K(arg));
  return ret;
}

int ObService::refresh_tenant_info(
    const ObRefreshTenantInfoArg &arg,
    ObRefreshTenantInfoRes &result)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.get_tenant_id() != MTL_ID() && OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

    if (OB_ISNULL(tenant_info_loader)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "tenant_info_loader should not be null", KR(ret));
    } else if (OB_FAIL(tenant_info_loader->refresh_tenant_info())) {
      COMMON_LOG(WARN, "refresh_tenant_info failed", KR(ret), K(arg));
    } else if (OB_FAIL(result.init(arg.get_tenant_id()))) {
      LOG_WARN("failed to init res", KR(ret), K(arg.get_tenant_id()));
    } else {
      MTL(transaction::ObTransService *)->register_standby_cleanup_task();
      LOG_INFO("finish refresh_tenant_info", KR(ret), K(arg), K(result));
    }
  }
  return ret;
}

int ObService::init_tenant_config(
    const obrpc::ObInitTenantConfigArg &arg,
    obrpc::ObInitTenantConfigRes &result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.get_tenant_configs().count(); i++) {
      const ObTenantConfigArg &config = arg.get_tenant_configs().at(i);
      if (OB_FAIL(OTC_MGR.init_tenant_config(config)))  {
        LOG_WARN("fail to init tenant config", KR(ret), K(config));
      }
    } // end for
  }
  (void) result.set_ret(ret);
  FLOG_INFO("init tenant config", KR(ret), K(arg));
  // use result to pass ret
  return OB_SUCCESS;
}

int ObService::get_ls_replayed_scn(
    const ObGetLSReplayedScnArg &arg,
    ObGetLSReplayedScnRes &result)
{
  LOG_INFO("start get_ls_replayed_scn", K(arg));
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  share::SCN cur_readable_scn = SCN::min_scn();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.get_tenant_id() != MTL_ID() && OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }
  if (OB_SUCC(ret)) {
    ObLSService *ls_svr = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    share::SCN offline_scn;
    if (OB_ISNULL(ls_svr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("pointer is null", KR(ret), KP(ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(arg.get_ls_id(), ls_handle, ObLSGetMod::RS_MOD))) {
      LOG_WARN("get log stream failed", KR(ret), K(arg));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream is null", KR(ret), K(arg), K(ls_handle));
    } else if (OB_FAIL(ls->get_max_decided_scn(cur_readable_scn))) {
      LOG_WARN("failed to get_max_decided_scn", KR(ret), K(arg), KPC(ls));
    } else if (arg.is_all_replica()) {
      if (OB_FAIL(ls->get_all_replica_min_readable_scn(cur_readable_scn))) {
        LOG_WARN("failed to get all replica readable scn", KR(ret));
      }
    }
    if (FAILEDx(ls->get_offline_scn(offline_scn))) {
      LOG_WARN("failed to get offline scn", KR(ret), K(arg), KPC(ls));
    } else if (OB_FAIL(result.init(arg.get_tenant_id(), arg.get_ls_id(),
            cur_readable_scn, offline_scn, get_self_addr()))) {
      LOG_WARN("failed to init res", KR(ret), K(arg), K(cur_readable_scn), K(offline_scn));
    }
    LOG_INFO("finish get_ls_replayed_scn", KR(ret), K(cur_readable_scn), K(arg), K(result),
        K(offline_scn));
  }

  return ret;
}

int ObService::handle_heartbeat(
    const share::ObHBRequest &hb_request,
    share::ObHBResponse &hb_response)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive a heartbeat request from heartbeat service", K(hb_request));
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService is not inited", KR(ret), K(inited_));
  } else if (OB_FAIL(ObHeartbeatHandler::handle_heartbeat(hb_request, hb_response))) {
    LOG_WARN("fail to handle heartbeat", KR(ret), K(hb_request));
  }
  const int64_t time_cost = ::oceanbase::common::ObTimeUtility::current_time() - now;
  FLOG_INFO("handle_heartbeat", KR(ret), K(hb_request), K(hb_response), K(time_cost));
  return ret;
}

int ObService::update_tenant_info_cache(
    const ObUpdateTenantInfoCacheArg &arg,
    ObUpdateTenantInfoCacheRes &result)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.get_tenant_id() != MTL_ID() && OB_FAIL(guard.switch_to(arg.get_tenant_id()))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

    if (OB_ISNULL(tenant_info_loader)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "tenant_info_loader should not be null", KR(ret));
    } else if (OB_FAIL(tenant_info_loader->update_tenant_info_cache(
                   arg.get_ora_rowscn(), arg.get_tenant_info(), arg.get_finish_data_version(),
                   arg.get_data_version_barrier_scn()))) {
      COMMON_LOG(WARN, "update_tenant_info_cache failed", KR(ret), K(arg));
    } else if (OB_FAIL(result.init(arg.get_tenant_id()))) {
      LOG_WARN("failed to init res", KR(ret), K(arg.get_tenant_id()));
    } else {
      LOG_TRACE("finish update_tenant_info_cache", KR(ret), K(arg), K(result));
    }
  }
  return ret;
}

int ObService::ob_admin_unlock_member_list(
    const obrpc::ObAdminUnlockMemberListOpArg &arg)
{
  LOG_INFO("start ob_admin_unlock_member_list", K(arg));
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invaild", KR(ret), K(arg));
  } else if (arg.tenant_id_ != MTL_ID() && OB_FAIL(guard.switch_to(arg.tenant_id_))) {
    LOG_WARN("switch tenant failed", KR(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMemberListLockUtils::unlock_for_ob_admin(arg.tenant_id_, arg.ls_id_, arg.lock_id_))) {
      LOG_WARN("failed to unlock member list", K(ret), K(arg));
    } else {
      LOG_INFO("finish ob_admin_unlock_member_list", K(arg));
    }
  }
  return ret;
}


}// end namespace observer
}// end namespace oceanbase
