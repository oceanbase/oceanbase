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

#define USING_LOG_PREFIX SERVER_OMT

#include "ob_multi_tenant.h"

#include "lib/oblog/ob_log.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/ob_running_mode.h"
#include "lib/file/file_directory_utils.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "share/ob_tenant_mgr.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "ob_tenant.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_sql_nio_server.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/ob_disk_usage_reporter.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/mysql/obsm_conn_callback.h"
#include "sql/dtl/ob_dtl_fc_server.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/das/ob_das_id_service.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/px/ob_px_admission.h"
#include "share/ob_get_compat_mode.h"
#include "storage/tx/wrs/ob_tenant_weak_read_service.h"   // ObTenantWeakReadService
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "share/ob_global_autoinc_service.h"
#include "lib/thread/ob_thread_name.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"    // ObArchiveService
#include "logservice/data_dictionary/ob_data_dict_service.h" // ObDataDictService
#include "ob_tenant_mtl_helper.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tx/ob_xa_service.h"
#include "storage/tx/ob_tx_loop_worker.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_standby_timestamp_service.h"
#include "storage/tx/ob_timestamp_access.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_unique_id_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_file_system_router.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_tenant_memory_printer.h"
#include "storage/tx/ob_id_service.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/ob_file_system_router.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h" // ObTenantSSTableMergeInfoMgr
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/io/ob_io_manager.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_tenant_srs.h"
#include "observer/report/ob_tenant_meta_checker.h"
#include "storage/high_availability/ob_storage_ha_service.h"
#include "rootserver/ob_tenant_info_loader.h"//ObTenantInfoLoader
#include "rootserver/ob_tenant_balance_service.h"//ObTenantBalanceService
#include "rootserver/ob_ls_recovery_reportor.h"//ObLSRecoveryReportor
#include "rootserver/ob_standby_schema_refresh_trigger.h"//ObStandbySchemaRefreshTrigger
#include "rootserver/ob_tenant_info_loader.h"//ObTenantInfoLoader
#include "rootserver/ob_create_standby_from_net_actor.h" // ObCreateStandbyFromNetActor
#include "rootserver/ob_primary_ls_service.h"//ObLSService
#include "rootserver/ob_recovery_ls_service.h"//ObRecoveryLSService
#include "rootserver/ob_common_ls_service.h"//ObCommonLSService
#include "rootserver/restore/ob_restore_service.h" //ObRestoreService
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "rootserver/ob_balance_task_execute_service.h" //ObBalanceTaskExecuteService
#include "rootserver/backup/ob_backup_service.h" //ObBackupDataService and ObBackupCleanService
#include "rootserver/backup/ob_backup_task_scheduler.h" // ObBackupTaskScheduler
#include "rootserver/backup/ob_archive_scheduler_service.h" // ObArchiveSchedulerService
#include "logservice/leader_coordinator/ob_leader_coordinator.h"
#include "storage/lob/ob_lob_manager.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_plan_baseline_mgr.h"
#endif
#ifdef OB_BUILD_ARBITRATION
#include "rootserver/ob_arbitration_service.h"
#endif
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oci_environment.h"
#endif
#include "lib/mysqlclient/ob_tenant_oci_envs.h"
#include "sql/udr/ob_udr_mgr.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/tx_storage/ob_tablet_gc_service.h"
#include "share/ob_occam_time_guard.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "observer/table_load/ob_table_load_service.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_cache.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_heartbeat_service.h"
#include "share/detect/ob_detect_manager.h"
#include "observer/table/ttl/ob_ttl_service.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#ifdef ERRSIM
#include "share/errsim_module/ob_tenant_errsim_module_mgr.h"
#include "share/errsim_module/ob_tenant_errsim_event_mgr.h"
#endif
#include "observer/table/ob_htable_lock_mgr.h"
#include "observer/table/ob_table_session_pool.h"
#include "observer/ob_server_event_history_table_operator.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::storage::checkpoint;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::concurrency_control;
using namespace oceanbase::transaction;
using namespace oceanbase::transaction::tablelock;
using namespace oceanbase::logservice;
using namespace oceanbase::archive;
using namespace oceanbase::observer;
using namespace oceanbase::rootserver;
using namespace oceanbase::blocksstable;

#define OB_TENANT_LOCK_BUCKET_NUM 10000L

namespace oceanbase
{
namespace share
{
// Declared in share/ob_context.h
// Obtain tenant_ctx according to tenant_id (obtained from omt)
int __attribute__ ((weak)) get_tenant_ctx_with_tenant_lock(const uint64_t tenant_id,
                                                           ObLDHandle &handle,
                                                           ObTenantSpace *&tenant_ctx)
{
  int ret = OB_SUCCESS;
  tenant_ctx = nullptr;

  omt::ObTenant *tenant = nullptr;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(tenant_id, handle, tenant))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_WARN("get tenant from omt failed", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(tenant_id));
  } else {
    tenant_ctx = &tenant->ctx();;
  }

  return ret;
}

int __attribute__ ((weak)) get_tenant_base_with_lock(
    uint64_t tenant_id, ObLDHandle &handle, ObTenantBase *&tenant_base, ReleaseCbFunc &release_cb)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = nullptr;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_with_tenant_lock(tenant_id, handle, tenant))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      LOG_WARN("get tenant from omt failed", K(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(tenant_id));
  } else {
    tenant_base = static_cast<ObTenantBase*>(tenant);
    release_cb = [tenant] (ObLDHandle &h) {
      return tenant->unlock(h);
    };
  }
  return ret;
}
} // end of namespace share
} // end of namespace oceanbase

bool compare_tenant(const ObTenant *lhs,
                    const ObTenant *rhs)
{
  return lhs->id() < rhs->id();
}

bool equal_tenant(const ObTenant *lhs,
                  const ObTenant *rhs)
{
  return lhs->id() == rhs->id();
}

bool compare_with_tenant_id(const ObTenant *lhs,
                            const uint64_t &tenant_id)
{
  return NULL != lhs ? (lhs->id() < tenant_id) : false;
}

bool equal_with_tenant_id(const ObTenant *lhs,
                          const uint64_t &tenant_id)
{
  return NULL != lhs ? (lhs->id() == tenant_id) : false;
}

int ObCtxMemConfigGetter::get(int64_t tenant_id, int64_t tenant_limit, common::ObIArray<ObCtxMemConfig> &configs)
{
  int64_t ret = OB_SUCCESS;
  if (tenant_id > OB_USER_TENANT_ID) {
    ObCtxMemConfig cfg;
    cfg.ctx_id_ = ObCtxIds::WORK_AREA;
    cfg.idle_size_ = 0;
    cfg.limit_ = 5 * tenant_limit / 100;
    ret = configs.push_back(cfg);
  }
  return ret;
}

ObCtxMemConfigGetter g_default_mcg;
ObICtxMemConfigGetter *ObMultiTenant::mcg_ = &g_default_mcg;

ObMultiTenant::ObMultiTenant()
    : is_inited_(false),
      server_slogger_(nullptr),
      bucket_lock_(),
      lock_(ObLatchIds::MULTI_TENANT_LOCK),
      tenants_(0, nullptr, ObModIds::OMT),
      balancer_(nullptr),
      myaddr_(),
      cpu_dump_(false),
      has_synced_(false)
{
}

static int init_compat_mode(lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;

  ObTenant *tenant = static_cast<ObTenant*>(share::ObTenantEnv::get_tenant());
  const uint64_t tenant_id = MTL_ID();

  if (is_virtual_tenant_id(tenant_id) || OB_SYS_TENANT_ID == tenant_id) {
    compat_mode = lib::Worker::CompatMode::MYSQL;
  } else {
    compat_mode = tenant->get_compat_mode();
    if (lib::Worker::CompatMode::INVALID == compat_mode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get compat mode failed", K(ret));
    }
  }
  LOG_INFO("finish init compatibility mode", K(tenant_id), K(compat_mode));
  return ret;
}

static int start_sql_nio_server(ObSqlNioServer *&sql_nio_server)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObSrvNetworkFrame *net_frame = GCTX.net_frame_;
  if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    sql_nio_server = OB_NEW(obmysql::ObSqlNioServer, "SqlNio",
                            obmysql::global_sm_conn_callback,
                            net_frame->get_mysql_handler(), tenant_id);
    if (OB_ISNULL(sql_nio_server)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to new sql_nio_server", K(ret));
    } else {
      int net_thread_count = 0;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        net_thread_count = tenant_config->tenant_sql_net_thread_count;
      }
      if (0 == net_thread_count) {
        ObTenantBase *tenant = MTL_CTX();
        net_thread_count = tenant ? std::max((int)tenant->unit_min_cpu(), 1) : 1;
      }
      sql_nio_server->get_nio()->set_run_wrapper(MTL_CTX());
      if (OB_FAIL(sql_nio_server->start(-1, &net_frame->get_deliver(),
                                        net_thread_count))) {
        LOG_WARN("sql nio server start failed", K(ret));
      } else {
        LOG_INFO("tenant sql_nio_server mtl_start success", K(ret),
                 K(tenant_id), K(net_thread_count));
      }
    }
  }
  return ret;
}

template<typename T>
static int server_obj_pool_mtl_new(common::ObServerObjectPool<T> *&pool)
{
  int ret = common::OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  pool = MTL_NEW(common::ObServerObjectPool<T>, "TntSrvObjPool", tenant_id, false/*regist*/,
                 MTL_IS_MINI_MODE(), MTL_CPU_COUNT());
  if (OB_ISNULL(pool)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ret = pool->init();
  }
  return ret;
}

template<typename T>
static void server_obj_pool_mtl_destroy(common::ObServerObjectPool<T> *&pool)
{
  using Pool = common::ObServerObjectPool<T>;
  MTL_DELETE(Pool, "TntSrvObjPool", pool);
  pool = nullptr;
}

static int start_mysql_queue(QueueThread *&qthread)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    qthread = OB_NEW(QueueThread, ObMemAttr(tenant_id, ObModIds::OB_RPC), "MysqlQueueTh", tenant_id);
    if (OB_ISNULL(qthread)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new qthread", K(ret), K(tenant_id));
    } else if (OB_FAIL(qthread->init())) {
      LOG_WARN("init qthread failed", K(tenant_id), K(ret));
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MysqlQueueTh,
                                        qthread->tg_id_))) {
      LOG_WARN("mysql queue init failed", K(ret), K(tenant_id),
               K(qthread->tg_id_));
    } else {
      qthread->queue_.set_qhandler(&GCTX.net_frame_->get_deliver().get_qhandler());

      int sql_thread_count = 0;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        sql_thread_count = tenant_config->tenant_sql_login_thread_count;
      }
      if (0 == sql_thread_count) {
        ObTenantBase *tenant = MTL_CTX();
        sql_thread_count = tenant ? std::max((int)tenant->unit_min_cpu(), 1) : 1;
      }

      if (OB_FAIL(TG_SET_RUNNABLE(qthread->tg_id_, qthread->thread_))) {
        LOG_WARN("fail to set runnable", K(ret), K(tenant_id), K(qthread->tg_id_));
      } else if (OB_FAIL(qthread->set_thread_count(sql_thread_count))) {
        LOG_WARN("fail to set thread count", K(ret), K(tenant_id), K(qthread->tg_id_));
      } else if(OB_FAIL(TG_START(qthread->tg_id_))) {
        LOG_ERROR("fail to start qthread", K(ret), K(tenant_id), K(qthread->tg_id_));
      } else {
        LOG_INFO("tenant mysql_queue mtl_start success", K(ret),
                  K(tenant_id), K(qthread->tg_id_), K(sql_thread_count));
      }
    }
  }
  return ret;
}

int ObMultiTenant::init(ObAddr myaddr,
                        ObMySQLProxy *sql_proxy,
                        bool mtl_bind_flag)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMultiTenant has been inited", K(ret));
  } else if (OB_FAIL(SLOGGERMGR.get_server_slogger(server_slogger_))) {
    LOG_WARN("fail to get server slogger", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(OB_TENANT_LOCK_BUCKET_NUM))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    myaddr_ = myaddr;
    if (NULL != sql_proxy) {
      if (OB_FAIL(ObTenantNodeBalancer::get_instance().init(this, *sql_proxy, myaddr))) {

        LOG_WARN("failed to init tenant node balancer", K(ret));
      }
    } else {
      // unset sql_proxy to disable quota balance among nodes
    }
  }

  if (OB_SUCC(ret) && mtl_bind_flag) {
    MTL_BIND(ObTenantIOManager::mtl_init, ObTenantIOManager::mtl_destroy);

    // base mtl
    MTL_BIND2(mtl_new_default, storage::mds::ObTenantMdsService::mtl_init, storage::mds::ObTenantMdsService::mtl_start, storage::mds::ObTenantMdsService::mtl_stop, storage::mds::ObTenantMdsService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObStorageLogger::mtl_init, ObStorageLogger::mtl_start, ObStorageLogger::mtl_stop, ObStorageLogger::mtl_wait, mtl_destroy_default);
    MTL_BIND2(ObTenantMetaMemMgr::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTransService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObLogService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, ObLogService::mtl_destroy);
    MTL_BIND2(mtl_new_default, logservice::ObGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObLSService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantCheckpointSlogHandler::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);

    // other mtl
    MTL_BIND2(mtl_new_default, ObArchiveService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, datadict::ObDataDictService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantTabletScheduler::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantDagScheduler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantFreezeInfoMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTxLoopWorker::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default); // ObTxLoopWorker
    MTL_BIND2(mtl_new_default, compaction::ObTenantCompactionProgressMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, compaction::ObServerCompactionEventHistory::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, storage::ObTenantTabletStatMgr::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, storage::ObTenantSSTableMergeInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, share::ObDagWarningHistoryManager::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, compaction::ObScheduleSuspectInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, memtable::ObLockWaitMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTableLockService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObPrimaryMajorFreezeService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObRestoreMajorFreezeService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantMetaChecker::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObLSRecoveryReportor::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObStandbySchemaRefreshTrigger::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObTenantInfoLoader::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObCreateStandbyFromNetActor::mtl_init, nullptr, rootserver::ObCreateStandbyFromNetActor::mtl_stop, rootserver::ObCreateStandbyFromNetActor::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObPrimaryLSService::mtl_init, nullptr, rootserver::ObPrimaryLSService::mtl_stop, rootserver::ObPrimaryLSService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObCommonLSService::mtl_init, nullptr, rootserver::ObCommonLSService::mtl_stop, rootserver::ObCommonLSService::mtl_wait, mtl_destroy_default);
#ifdef OB_BUILD_ARBITRATION
    MTL_BIND2(mtl_new_default, rootserver::ObArbitrationService::mtl_init, mtl_start_default, rootserver::ObArbitrationService::mtl_stop, rootserver::ObArbitrationService::mtl_wait, mtl_destroy_default);
#endif
    MTL_BIND2(mtl_new_default, rootserver::ObBalanceTaskExecuteService::mtl_init, nullptr, rootserver::ObBalanceTaskExecuteService::mtl_stop, rootserver::ObBalanceTaskExecuteService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObTenantBalanceService::mtl_init, nullptr, rootserver::ObTenantBalanceService::mtl_stop, rootserver::ObTenantBalanceService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObRecoveryLSService::mtl_init, nullptr, rootserver::ObRecoveryLSService::mtl_stop, rootserver::ObRecoveryLSService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObRestoreService::mtl_init, nullptr, rootserver::ObRestoreService::mtl_stop, rootserver::ObRestoreService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, coordinator::ObLeaderCoordinator::mtl_init, coordinator::ObLeaderCoordinator::mtl_start, coordinator::ObLeaderCoordinator::mtl_stop, coordinator::ObLeaderCoordinator::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, coordinator::ObFailureDetector::mtl_init, coordinator::ObFailureDetector::mtl_start, coordinator::ObFailureDetector::mtl_stop, coordinator::ObFailureDetector::mtl_wait, mtl_destroy_default);
    MTL_BIND2(ObLobManager::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObStorageHAService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObBackupTaskScheduler::mtl_init, nullptr, rootserver::ObBackupTaskScheduler::mtl_stop, rootserver::ObBackupTaskScheduler::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObBackupDataService::mtl_init, nullptr, rootserver::ObBackupDataService::mtl_stop, rootserver::ObBackupDataService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObBackupCleanService::mtl_init, nullptr, rootserver::ObBackupCleanService::mtl_stop, rootserver::ObBackupCleanService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObArchiveSchedulerService::mtl_init, nullptr, rootserver::ObArchiveSchedulerService::mtl_stop, rootserver::ObArchiveSchedulerService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObGlobalAutoIncService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, share::detector::ObDeadLockDetectorMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
#ifdef OB_BUILD_ARBITRATION
    MTL_BIND2(mtl_new_default, ObPlanBaselineMgr::mtl_init, nullptr, ObPlanBaselineMgr::mtl_stop, ObPlanBaselineMgr::mtl_wait, mtl_destroy_default);
#endif
    MTL_BIND2(mtl_new_default, ObTenantSchemaService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTimestampService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObStandbyTimestampService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTimestampAccess::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTransIDService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObUniqueIDService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObXAService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTabletGCService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantFreezer::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObDataAccessService::mtl_init, nullptr, nullptr, nullptr, ObDataAccessService::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObDASIDService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObAccessService::mtl_init, nullptr, mtl_stop_default, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObCheckPointService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTransferService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, rootserver::ObTenantTransferService::mtl_init, nullptr, rootserver::ObTenantTransferService::mtl_stop, rootserver::ObTenantTransferService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObRebuildService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObMultiVersionGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObUDRMgr::mtl_init, nullptr, ObUDRMgr::mtl_stop, nullptr, mtl_destroy_default);

    MTL_BIND(ObPxPools::mtl_init, ObPxPools::mtl_destroy);
    MTL_BIND(ObTenantDfc::mtl_init, ObTenantDfc::mtl_destroy);
    MTL_BIND(init_compat_mode, nullptr);
    MTL_BIND2(ObMySQLRequestManager::mtl_new, ObMySQLRequestManager::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, ObMySQLRequestManager::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObTenantWeakReadService::mtl_init, mtl_start_default,
              mtl_stop_default,
              mtl_wait_default,
              mtl_destroy_default);
    //MTL_BIND(ObTransAuditRecordMgr::mtl_init, ObTransAuditRecordMgr::mtl_destroy);
    MTL_BIND(ObTenantSqlMemoryManager::mtl_init, ObTenantSqlMemoryManager::mtl_destroy);
    MTL_BIND(ObPlanMonitorNodeList::mtl_init, ObPlanMonitorNodeList::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObTableLoadService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObSharedMacroBlockMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND(ObFLTSpanMgr::mtl_init, ObFLTSpanMgr::mtl_destroy);
    MTL_BIND(common::sqlclient::ObTenantOciEnvs::mtl_init, common::sqlclient::ObTenantOciEnvs::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObPlanCache::mtl_init, nullptr, ObPlanCache::mtl_stop, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObPsCache::mtl_init, nullptr, ObPsCache::mtl_stop, nullptr, mtl_destroy_default);
    MTL_BIND2(server_obj_pool_mtl_new<ObPartTransCtx>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<ObPartTransCtx>);
    MTL_BIND2(server_obj_pool_mtl_new<ObTableScanIterator>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<ObTableScanIterator>);
    MTL_BIND(ObDetectManager::mtl_init, ObDetectManager::mtl_destroy);
    MTL_BIND(ObTenantSQLSessionMgr::mtl_init, ObTenantSQLSessionMgr::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObDTLIntermResultManager::mtl_init, ObDTLIntermResultManager::mtl_start,
    ObDTLIntermResultManager::mtl_stop, ObDTLIntermResultManager::mtl_wait, ObDTLIntermResultManager::mtl_destroy);
    if (GCONF._enable_new_sql_nio && GCONF._enable_tenant_sql_net_thread) {
      MTL_BIND2(nullptr, nullptr, start_mysql_queue, mtl_stop_default,
                mtl_wait_default, mtl_destroy_default);
      // MTL_BIND2(nullptr, nullptr, start_sql_nio_server, mtl_stop_default,
      //           mtl_wait_default, mtl_destroy_default);
    }
    MTL_BIND2(mtl_new_default, rootserver::ObHeartbeatService::mtl_init, nullptr, rootserver::ObHeartbeatService::mtl_stop, rootserver::ObHeartbeatService::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, table::ObTTLService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);

#ifdef ERRSIM
    MTL_BIND2(mtl_new_default, ObTenantErrsimModuleMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantErrsimEventMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
#endif

    MTL_BIND(table::ObHTableLockMgr::mtl_init, table::ObHTableLockMgr::mtl_destroy);
    MTL_BIND2(mtl_new_default, ObSharedTimer::mtl_init, ObSharedTimer::mtl_start, ObSharedTimer::mtl_stop, ObSharedTimer::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObOptStatMonitorManager::mtl_init, ObOptStatMonitorManager::mtl_start, ObOptStatMonitorManager::mtl_stop, ObOptStatMonitorManager::mtl_wait, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, ObTenantSrs::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    MTL_BIND2(mtl_new_default, table::ObTableApiSessPoolMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    LOG_INFO("succ to init multi tenant");
  }
  return ret;
}

int ObMultiTenant::start()
{
  int ret = OB_SUCCESS;

  ObTenantMemoryPrinter &printer = ObTenantMemoryPrinter::get_instance();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(create_virtual_tenants())) {
    LOG_ERROR("create virtual tenants failed", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    LOG_ERROR("start multi tenant thread fail", K(ret));
  } else if (OB_FAIL(ObTenantNodeBalancer::get_instance().start())) {
    LOG_ERROR("start tenant node balancer thread failed", K(ret));
  // start memstore print timer.
  } else if (OB_FAIL(printer.register_timer_task(lib::TGDefIDs::MemDumpTimer))) {
    LOG_ERROR("Fail to register timer task", K(ret));
  } else {
    LOG_INFO("succ to start multi tenant");
  }


  if (OB_FAIL(ret)) {
    stop();
  }
  return ret;
}

void ObMultiTenant::stop()
{
  // Stop balancer so that tenants' quota will fixed. It's not
  // necessary to put ahead, but it isn't harmful and can exclude
  // affection for balancer.
  ObTenantNodeBalancer::get_instance().stop();
  // Stop workers of all tenants thus no request of tenant would be
  // processed any more. All tenants will be removed indeed.
  {
    TenantIdList ids;
    ids.set_label(ObModIds::OMT);
    get_tenant_ids(ids);
    bool lock_succ = false;
    while (ids.size() > 0) {
      LOG_INFO("there're some tenants need destroy", "count", ids.size());

      for (TenantIdList::iterator it = ids.begin(); it != ids.end(); it++) {
        uint64_t id = *it;
        remove_tenant(id, lock_succ);
      }
      get_tenant_ids(ids);
    }
  }
  // No tenant exist right now, so we just stop the scheduler.
  ObThreadPool::stop();
}

void ObMultiTenant::wait()
{
  ObTenantNodeBalancer::get_instance().wait();
  ObThreadPool::wait();
}


void ObMultiTenant::destroy()
{
  {
    SpinWLockGuard guard(lock_);
    tenants_.clear();
    is_inited_ = false;
  }
}

int ObMultiTenant::construct_meta_for_hidden_sys(ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenantSuperBlock super_block(tenant_id, true/*is_hidden*/);
  share::ObUnitInfoGetter::ObTenantConfig unit;
  const bool has_memstore = true;
  const int64_t create_timestamp = ObTimeUtility::current_time();
  uint64_t unit_id = 1000;

  share::ObUnitConfig unit_config;
  const bool is_hidden_sys = true;
  if (OB_FAIL(unit_config.gen_sys_tenant_unit_config(is_hidden_sys))) {
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else if (OB_FAIL(unit.init(tenant_id,
                        unit_id,
                        share::ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL,
                        unit_config,
                        lib::Worker::CompatMode::MYSQL,
                        create_timestamp,
                        has_memstore,
                        false /*is_removed*/))) {
    LOG_WARN("fail to init hidden sys tenant unit", K(ret), K(tenant_id));
  } else if (OB_FAIL(meta.build(unit, super_block))) {
    LOG_WARN("fail to build tenant meta", K(ret), K(tenant_id));
  }

  return ret;
}

int ObMultiTenant::construct_meta_for_virtual_tenant(const uint64_t tenant_id,
                                                     const double min_cpu,
                                                     const double max_cpu,
                                                     const int64_t mem_limit,
                                                     ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;

  ObTenantSuperBlock super_block(tenant_id, true/*is_hidden*/);
  share::ObUnitInfoGetter::ObTenantConfig unit;
  uint64_t unit_id = 1000;

  share::ObUnitConfig unit_config;
  const bool has_memstore = true;
  const int64_t create_timestamp = ObTimeUtility::current_time();
  if (OB_FAIL(unit_config.gen_virtual_tenant_unit_config(max_cpu, min_cpu, mem_limit))) {
    LOG_WARN("generate virtual tenant unit config fail", KR(ret), K(max_cpu), K(min_cpu),
        K(mem_limit));
  } else if (OB_FAIL(unit.init(tenant_id,
                        unit_id,
                        share::ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL,
                        unit_config,
                        lib::Worker::CompatMode::MYSQL,
                        create_timestamp,
                        has_memstore,
                        false /*is_removed*/))) {
    LOG_WARN("fail to init virtual tenant unit", K(ret), K(tenant_id));
  } else if (OB_FAIL(meta.build(unit, super_block))) {
    LOG_WARN("fail to build tenant meta", K(ret), K(tenant_id));
  }

  return ret;
}

int ObMultiTenant::create_hidden_sys_tenant()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  omt::ObTenant *tenant = nullptr;
  ObTenantMeta meta;
  if (OB_FAIL(construct_meta_for_hidden_sys(meta))) {
    LOG_ERROR("fail to construct meta", K(ret));
  } else if (OB_FAIL(create_tenant(meta, true /* write_slog */))) {
    LOG_ERROR("create hidden sys tenant failed", K(ret));
  }
  return ret;
}

int ObMultiTenant::update_hidden_sys_tenant()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  omt::ObTenant *tenant = nullptr;
  ObTenantMeta meta;
  if (OB_FAIL(get_tenant(tenant_id, tenant))) { // sys tenant will not be deleted
    LOG_WARN("failed to get sys tenant", K(ret));
  } else if (OB_FAIL(construct_meta_for_hidden_sys(meta))) {
    LOG_ERROR("fail to construct meta", K(ret));
  } else {
    int64_t bucket_lock_idx = -1;
    bool lock_succ = false;
    if (OB_FAIL(bucket_lock_.wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id)))) {
      LOG_WARN("fail to try_wrlock for update tenant unit", K(ret), K(tenant_id), K(bucket_lock_idx));
    } else if (FALSE_IT(lock_succ = true)) {
    } else if (!tenant->is_hidden() || meta.unit_ == tenant->get_unit()) {
      // do nothing
    } else if (OB_FAIL(update_tenant_unit_no_lock(meta.unit_))) {
      LOG_WARN("fail to update tenant unit", K(ret), K(tenant_id));
    }
    if (lock_succ) {
      bucket_lock_.unlock(bucket_lock_idx);
    }
  }
  return ret;
}

int ObMultiTenant::create_virtual_tenants()
{
  int ret = OB_SUCCESS;
  const int64_t phy_cpu_cnt = sysconf(_SC_NPROCESSORS_ONLN);
  const double data_cpu = (phy_cpu_cnt <= 4) ? 1.0 : OB_DATA_CPU;
  const double dtl_cpu = (phy_cpu_cnt <= 4) ? 1.0 : OB_DTL_CPU;

  if (OB_FAIL(create_tenant_without_unit(
                         OB_DATA_TENANT_ID,
                         data_cpu,
                         data_cpu))) {
    LOG_ERROR("add data tenant fail", K(ret));

  } else if (OB_FAIL(create_tenant_without_unit(
                         OB_DTL_TENANT_ID,
                         dtl_cpu,
                         dtl_cpu))) {
    LOG_ERROR("add DTL tenant fail", K(ret));

  } else {
    // init allocator for OB_SERVER_TENANT_ID
    ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
    if (!OB_ISNULL(allocator)) {
      allocator->set_tenant_limit(OB_SERVER_TENANT_ID, INT64_MAX);
    }

    // set tenant mem limits
    ObVirtualTenantManager &omti = ObVirtualTenantManager::get_instance();
    if (OB_FAIL(omti.add_tenant(OB_SERVER_TENANT_ID))) {
      LOG_ERROR("Fail to add server tenant to tenant manager, ", K(ret));
    } else if (OB_FAIL(omti.set_tenant_mem_limit(OB_SERVER_TENANT_ID, 0, INT64_MAX))) {
      LOG_ERROR("Fail to set tenant mem limit, ", K(ret));
    }
  }

  return ret;
}


int ObMultiTenant::create_tenant_without_unit(const uint64_t tenant_id,
                                              const double min_cpu,
                                              const double max_cpu)
{
  int ret = OB_SUCCESS;
  ObTenantMeta meta;
  uint64_t mem_limit = 0;

  if (OB_SERVER_TENANT_ID == tenant_id) {
    mem_limit = INT64_MAX;
  } else {
    static const int64_t VIRTUAL_TENANT_MEMORY_LIMTI = 1L << 30;
    mem_limit = VIRTUAL_TENANT_MEMORY_LIMTI;
  }
  if (OB_FAIL(construct_meta_for_virtual_tenant(tenant_id, min_cpu, max_cpu, mem_limit, meta))) {
    LOG_WARN("fail to construct_meta_for_virtual_tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(create_tenant(meta, false))) {
    LOG_WARN("fail to create virtual tenant", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret) && is_virtual_tenant_id(tenant_id)) {
    ObVirtualTenantManager &omti = ObVirtualTenantManager::get_instance();
    if (OB_FAIL(omti.add_tenant(tenant_id))) {
      LOG_ERROR("Fail to add virtual tenant to tenant manager, ", K(ret));
    } else if (OB_FAIL(omti.set_tenant_mem_limit(tenant_id, 0, mem_limit))) {
      LOG_ERROR("Fail to set virtual tenant mem limit, ", K(ret));
    }
  }
  return ret;
}

int ObMultiTenant::convert_hidden_to_real_sys_tenant(const ObUnitInfoGetter::ObTenantConfig &unit,
                                                     const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;

  ObTenant *tenant = nullptr;
  const double min_cpu = static_cast<double>(unit.config_.min_cpu());
  const double max_cpu = static_cast<double>(unit.config_.max_cpu());
  const uint64_t tenant_id = unit.tenant_id_;
  int64_t allowed_mem_limit = 0;
  ObTenantSuperBlock new_super_block;
  bool lock_succ = false;
  int64_t bucket_lock_idx = -1;
  int64_t lock_timeout_ts = abs_timeout_us - 3000000; // reserve 3s for converting tenant

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(bucket_lock_.wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id), lock_timeout_ts))) {
    LOG_WARN("fail to wrlock for convert_hidden_to_real_sys_tenant", K(ret), K(bucket_lock_idx), K(lock_timeout_ts));
  } else if (FALSE_IT(lock_succ = true)) {
  } else if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get sys tenant", K(tenant_id), K(ret));
  } else if (!tenant->is_hidden()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must be hidden sys tenant", K(ret));
  } else if (FALSE_IT(new_super_block = tenant->get_super_block())) {
  } else if (FALSE_IT(new_super_block.is_hidden_ = false)) {
  } else if (OB_FAIL(update_tenant_unit_no_lock(unit))) {
    LOG_WARN("fail to update_tenant_unit_no_lock", K(ret), K(unit));
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance()
      .write_tenant_super_block_slog(new_super_block))) {
    LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(new_super_block));
  } else {
    tenant->set_tenant_super_block(new_super_block);
  }

  if (lock_succ) {
    bucket_lock_.unlock(bucket_lock_idx);
  }

  FLOG_INFO("finish convert_hidden_to_real_sys_tenant", K(ret), K(new_super_block), K(bucket_lock_idx));

  return ret;
}

int ObMultiTenant::create_tenant(const ObTenantMeta &meta, bool write_slog, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const double min_cpu = static_cast<double>(meta.unit_.config_.min_cpu());
  const double max_cpu = static_cast<double>(meta.unit_.config_.max_cpu());
  const uint64_t tenant_id = meta.unit_.tenant_id_;
  ObTenant *tenant = nullptr;
  int64_t allowed_mem_limit = 0;
  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
  ObTenantCreateStep create_step = ObTenantCreateStep::STEP_BEGIN;  // step0
  bool lock_succ = false;
  int64_t bucket_lock_idx = -1;
  const int64_t log_disk_size = meta.unit_.config_.log_disk_size();
  int64_t lock_timeout_ts = abs_timeout_us - 5000000; // reserve 5s for creating tenant

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (!meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(meta));
  } else if (OB_ISNULL(malloc_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("malloc allocator is NULL", K(ret));
  } else if (OB_FAIL(bucket_lock_.wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id), lock_timeout_ts))) {
    LOG_WARN("fail to wrlock for create tenant", K(ret), K(tenant_id), K(bucket_lock_idx), K(lock_timeout_ts));
  } else if (FALSE_IT(lock_succ = true)) {
  } else if (OB_SUCC(get_tenant(tenant_id, tenant))) {
    ret = OB_TENANT_EXIST;
    LOG_WARN("tenant exist", K(ret), K(tenant_id));
  } else {
    ret = OB_SUCCESS;
  }

  tenant = nullptr;

  bool tenant_allocator_created = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(malloc_allocator->create_and_add_tenant_allocator(tenant_id))) {
      LOG_ERROR("create and add tenant allocator failed", K(ret), K(tenant_id));
    } else {
      tenant_allocator_created = true;
    }
    if (OB_SUCC(ret)) {
      int64_t memory_size = meta.unit_.config_.memory_size();
      if (is_sys_tenant(tenant_id) && !meta.super_block_.is_hidden_) {
        memory_size += GMEMCONF.get_extra_memory();
      }
      if (OB_FAIL(update_tenant_memory(tenant_id, memory_size, allowed_mem_limit))) {
        LOG_WARN("fail to update tenant memory", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObCtxMemConfig, ObCtxIds::MAX_CTX_ID> configs;
    if (OB_FAIL(mcg_->get(tenant_id, allowed_mem_limit, configs))) {
      LOG_ERROR("get ctx mem config failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < configs.count(); i++) {
      const uint64_t ctx_id = configs.at(i).ctx_id_;
      const int64_t idle_size = configs.at(i).idle_size_;
      const int64_t limit = configs.at(i).limit_;
      const bool reserve = true;
      if (OB_FAIL(malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id, idle_size, reserve))) {
        LOG_ERROR("set tenant ctx idle failed", K(ret));
      } else if (OB_FAIL(set_ctx_limit(tenant_id, ctx_id, limit))) {
        LOG_ERROR("set tenant ctx limit failed", K(ret), K(limit));
      }
      LOG_INFO("init ctx memory finish", K(ret), K(tenant_id), K(i), K(configs.at(i)));
    }
    if (OB_SUCC(ret)) {
      create_step = ObTenantCreateStep::STEP_CTX_MEM_CONFIG_SETTED; // step1
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_virtual_tenant_id(tenant_id)
        && OB_FAIL(GCTX.log_block_mgr_->create_tenant(log_disk_size))) {
      LOG_ERROR("create_tenant in ObServerLogBlockMgr failed", KR(ret));
    }
    // if create_tenant in ObServerLogBlockMGR success, the log disk size need by this tenant has been pinned,
    // otherwise, the assigned log disk size of ObServerLogBlockMGR is origin.
    if (OB_SUCC(ret)) {
      create_step = ObTenantCreateStep::STEP_LOG_DISK_SIZE_PINNED;  // step2
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(GCTX.cgroup_ctrl_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("group ctrl not init", K(ret));
  } else if (OB_ISNULL(tenant = OB_NEW(
    ObTenant, ObModIds::OMT, tenant_id, GCONF.workers_per_cpu_quota.get_value(), *GCTX.cgroup_ctrl_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("new tenant fail", K(ret));
  } else if (FALSE_IT(create_step = ObTenantCreateStep::STEP_TENANT_NEWED)) { //step3

  } else if (OB_FAIL(tenant->init_ctx())) {
    LOG_WARN("init ctx fail", K(tenant_id), K(ret));
  } else if (write_slog) {
    if (OB_FAIL(write_create_tenant_prepare_slog(meta))) {
      LOG_ERROR("fail to write create tenant prepare slog", K(ret));
    } else {
      create_step = ObTenantCreateStep::STEP_WRITE_PREPARE_SLOG; // step4
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OTC_MGR.add_tenant_config(tenant_id))) {
      LOG_ERROR("add tenant config fail", K(tenant_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, tenant->id()) {
      WITH_ENTITY(&tenant->ctx()) {
        if (OB_FAIL(tenant->init(meta))) {
          LOG_ERROR("init tenant fail", K(tenant_id), K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
#ifdef OMT_UNITTEST
   } else if (!is_virtual_tenant_id(tenant_id) &&
       OB_FAIL(OTC_MGR.got_version(tenant_id, common::ObSystemConfig::INIT_VERSION))) {
     LOG_ERROR("failed to got version", K(tenant_id), K(ret));
#endif
  } else if (!is_virtual_tenant_id(tenant_id)) {
    ObTenantSwitchGuard guard(tenant);
    if (OB_FAIL(MTL(ObTenantFreezer *)->set_tenant_mem_limit(meta.unit_.config_.memory_size(), allowed_mem_limit))) {
      LOG_WARN("fail to set_tenant_mem_limit", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (write_slog && OB_FAIL(write_create_tenant_commit_slog(tenant_id))) {
      LOG_ERROR("fail to write create tenant commit slog", K(ret), K(tenant_id));
    } else {
      tenant->set_create_status(ObTenantCreateStatus::CREATE_COMMIT);
      create_step = ObTenantCreateStep::STEP_FINISH; // step5
    }
  }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(lock_);
    ObTenant *tmp_tenant = NULL;
    TenantIterator iter;
    if (OB_SUCC(get_tenant_unsafe(tenant_id, tmp_tenant))) {
      ret = OB_TENANT_EXIST;
      LOG_ERROR("tenant exist", K(ret), K(tenant_id));
    } else if (OB_FAIL(tenants_.insert(tenant, iter, compare_tenant))) {
      LOG_ERROR("fail to insert tenant", K(ret), K(tenant_id));
    }
  }
  // TODO: @lingyang 预期不能失败
  if (!is_virtual_tenant_id(tenant_id) && OB_TMP_FAIL(update_tenant_config(tenant_id))) {
    LOG_WARN("update tenant config fail", K(tenant_id), K(tmp_ret));
  }

  if (OB_FAIL(ret)) {
    do {
      tmp_ret = OB_SUCCESS;
      if (create_step >= ObTenantCreateStep::STEP_TENANT_NEWED) {
        if (OB_NOT_NULL(tenant)) {
          tenant->stop();
          while (OB_SUCCESS != tenant->try_wait()) {
            ob_usleep(100 * 1000);
          }
          tenant->destroy();
          ob_delete(tenant);
          tenant = nullptr;
        }
        // no need rollback when replaying slog and creating a virtual tenant,
        // in which two case the write_slog flag is set to false
        if (write_slog && OB_SUCCESS != (tmp_ret = clear_persistent_data(tenant_id))) {
          LOG_ERROR("fail to clear persistent data", K(tenant_id), K(tmp_ret));
          SLEEP(1);
        }
      }
    } while (OB_SUCCESS != tmp_ret);

    do {
      tmp_ret = OB_SUCCESS;
      if (create_step >= ObTenantCreateStep::STEP_CTX_MEM_CONFIG_SETTED) {
        for (uint64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
          if (NULL == malloc_allocator->get_tenant_ctx_allocator(tenant_id, ctx_id)) {
            // do-nothing
          } else if (OB_SUCCESS != (tmp_ret = malloc_allocator->set_tenant_ctx_idle(tenant_id, ctx_id, 0))) {
            LOG_ERROR("fail to cleanup ctx mem config", K(tmp_ret), K(tenant_id), K(ctx_id));
            SLEEP(1);
          }
        }
      }
    } while (OB_SUCCESS != tmp_ret);

    do {
      tmp_ret = OB_SUCCESS;
      if (create_step >= ObTenantCreateStep::STEP_LOG_DISK_SIZE_PINNED) {
        if (!is_virtual_tenant_id(tenant_id)) {
          GCTX.log_block_mgr_->abort_create_tenant(log_disk_size);
        }
      }
    } while (OB_SUCCESS != tmp_ret);

    if (create_step >= ObTenantCreateStep::STEP_WRITE_PREPARE_SLOG) {
      if (OB_SUCCESS != (tmp_ret = write_create_tenant_abort_slog(tenant_id))) {
        LOG_ERROR("fail to write create tenant abort slog", K(tmp_ret));
      }
    }
  }
  if (OB_FAIL(ret) && tenant_allocator_created) {
    malloc_allocator->recycle_tenant_allocator(tenant_id);
  }
  if (lock_succ) {
    bucket_lock_.unlock(bucket_lock_idx);
  }

  FLOG_INFO("finish create new tenant", K(ret), K(tenant_id), K(write_slog), K(create_step), K(bucket_lock_idx));

  return ret;
}

int ObMultiTenant::update_tenant_unit_no_lock(const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;

  ObTenant *tenant = nullptr;
  const double min_cpu = static_cast<double>(unit.config_.min_cpu());
  const double max_cpu = static_cast<double>(unit.config_.max_cpu());
  const uint64_t tenant_id = unit.tenant_id_;
  ObUnitInfoGetter::ObTenantConfig allowed_new_unit;
  ObUnitInfoGetter::ObTenantConfig old_unit;
  int64_t allowed_new_log_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is nullptr", K(tenant_id));
  } else if (OB_FAIL(old_unit.assign(tenant->get_unit()))) {
    LOG_ERROR("fail to assign old unit failed", K(tenant_id), K(unit));
  } else if (OB_FAIL(update_tenant_log_disk_size(tenant_id,
                                                 old_unit.config_.log_disk_size(),
                                                 unit.config_.log_disk_size(),
                                                 allowed_new_log_disk_size))) {
    LOG_WARN("fail to update tenant log disk size", K(ret), K(tenant_id));
  } else if (OB_FAIL(construct_allowed_unit_config(allowed_new_log_disk_size,
                                                   unit,
                                                   allowed_new_unit))) {
    LOG_WARN("fail to construct_allowed_unit_config", K(allowed_new_log_disk_size),
             K(allowed_new_unit));
  } else if (OB_FAIL(write_update_tenant_unit_slog(allowed_new_unit))) {
    LOG_WARN("fail to write tenant meta slog", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant->update_thread_cnt(max_cpu))) {
    LOG_WARN("fail to update mtl module thread_cnt", K(ret), K(tenant_id));
  } else {
    if (tenant->unit_min_cpu() != min_cpu) {
      tenant->set_unit_min_cpu(min_cpu);
    }
    if (tenant->unit_max_cpu() != max_cpu) {
      tenant->set_unit_max_cpu(max_cpu);
    }
    tenant->set_tenant_unit(allowed_new_unit);
    LOG_INFO("succecc to set tenant unit config", K(unit));
  }

  return ret;
}

int ObMultiTenant::update_tenant_memory(const ObUnitInfoGetter::ObTenantConfig &unit,
                                        const int64_t extra_memory /* = 0 */)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  const uint64_t tenant_id = unit.tenant_id_;
  int64_t allowed_mem_limit = 0;
  int64_t memory_size = unit.config_.memory_size() + extra_memory;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is nullptr", K(tenant_id));
  } else if (OB_FAIL(update_tenant_memory(tenant_id, memory_size, allowed_mem_limit))) {
    LOG_WARN("fail to update tenant memory", K(ret), K(tenant_id));
  } else if (OB_FAIL(update_tenant_freezer_mem_limit(tenant_id, memory_size, allowed_mem_limit))) {
    LOG_WARN("fail to update_tenant_freezer_mem_limit", K(ret), K(tenant_id));
  } else if (FALSE_IT(tenant->set_unit_memory_size(allowed_mem_limit))) {
    // unreachable
  }
  return ret;
}

int ObMultiTenant::construct_allowed_unit_config(const int64_t allowed_new_log_disk_size,
                                                 const ObUnitInfoGetter::ObTenantConfig &expected_unit_config,
                                                 ObUnitInfoGetter::ObTenantConfig &allowed_new_unit)
{
  int ret = OB_SUCCESS;
  if (0 > allowed_new_log_disk_size
      || !expected_unit_config.is_valid()) {
    ret= OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(allowed_new_unit.assign(expected_unit_config))) {
    LOG_ERROR("fail to assign new unit", K(allowed_new_log_disk_size), K(expected_unit_config));
  } else {
    // construct allowed resource.
    ObUnitResource allowed_resource(
        expected_unit_config.config_.max_cpu(),
        expected_unit_config.config_.min_cpu(),
        expected_unit_config.config_.memory_size(),
        allowed_new_log_disk_size,
        expected_unit_config.config_.max_iops(),
        expected_unit_config.config_.min_iops(),
        expected_unit_config.config_.iops_weight());
    if (OB_FAIL(allowed_new_unit.config_.update_unit_resource(allowed_resource))) {
      LOG_WARN("update_unit_resource failed", K(allowed_new_log_disk_size), K(allowed_new_unit),
               K(allowed_resource));
    }
  }
  return ret;
}

int ObMultiTenant::update_tenant_unit(const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = unit.tenant_id_;
  int64_t bucket_lock_idx = -1;
  bool lock_succ = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(bucket_lock_.wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id)))) {
    LOG_WARN("fail to try_wrlock for update tenant unit", K(ret), K(tenant_id), K(bucket_lock_idx));
  } else if (FALSE_IT(lock_succ = true)) {
  } else if (OB_FAIL(update_tenant_unit_no_lock(unit))) {
    LOG_WARN("fail to update_tenant_unit_no_lock", K(ret), K(unit));
  }

  if (lock_succ) {
    bucket_lock_.unlock(bucket_lock_idx);
  }

  LOG_INFO("OMT finish update tenant unit config", K(ret), K(unit), K(bucket_lock_idx));

  return ret;
}

int ObMultiTenant::update_tenant_memory(const uint64_t tenant_id, const int64_t mem_limit, int64_t &allowed_mem_limit)
{
  int ret = OB_SUCCESS;

  ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();

  allowed_mem_limit = mem_limit;
  const int64_t pre_mem_limit = malloc_allocator->get_tenant_limit(tenant_id);
  const int64_t mem_hold = malloc_allocator->get_tenant_hold(tenant_id);
  const int64_t target_mem_limit = mem_limit;

  if (OB_SUCC(ret)) {
    // make sure half reserve memory available
    if (target_mem_limit < pre_mem_limit) {
      allowed_mem_limit = mem_hold + static_cast<int64_t>(
          static_cast<double>(target_mem_limit) * TENANT_RESERVE_MEM_RATIO / 2.0);
      if (allowed_mem_limit < target_mem_limit) {
        allowed_mem_limit = target_mem_limit;
      }
      if (allowed_mem_limit < pre_mem_limit) {
        LOG_INFO("reduce memory quota", K(mem_limit), K(pre_mem_limit), K(target_mem_limit), K(mem_hold));
      } else {
        allowed_mem_limit = pre_mem_limit;
        LOG_WARN("try to reduce memory quota, but free memory not enough",
                 K(allowed_mem_limit), K(pre_mem_limit), K(target_mem_limit), K(mem_hold));
      }
    }

    if (allowed_mem_limit != pre_mem_limit) {
      malloc_allocator->set_tenant_limit(tenant_id, allowed_mem_limit);
    }
  }

  return ret;
}

int ObMultiTenant::update_tenant_log_disk_size(const uint64_t tenant_id,
                                               const int64_t old_log_disk_size,
                                               const int64_t new_log_disk_size,
                                               int64_t &allowed_new_log_disk_size)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_SUCC(guard.switch_to(tenant_id))) {
    ObLogService *log_service = MTL(ObLogService *);
    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.log_block_mgr_->update_tenant(old_log_disk_size, new_log_disk_size,
                                                          allowed_new_log_disk_size, log_service))) {
      LOG_WARN("fail to update_tenant", K(tenant_id), K(old_log_disk_size), K(new_log_disk_size),
               K(allowed_new_log_disk_size));
    } else {
      LOG_INFO("update_tenant_log_disk_size success", K(tenant_id), K(old_log_disk_size),
               K(new_log_disk_size), K(allowed_new_log_disk_size));
    }
  }
  return ret;
}

int ObMultiTenant::update_tenant_config(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (false == tenant_config.is_valid()) {
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_SUCC(guard.switch_to(tenant_id))) {
      if (OB_TMP_FAIL(update_palf_config())) {
        LOG_WARN("failed to update palf disk config", K(tmp_ret), K(tenant_id));
      }
      if (OB_TMP_FAIL(update_tenant_dag_scheduler_config())) {
        LOG_WARN("failed to update tenant dag scheduler config", K(tmp_ret), K(tenant_id));
      }
      if (OB_TMP_FAIL(update_tenant_freezer_config_())) {
        LOG_WARN("failed to update tenant tenant freezer config", K(tmp_ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("update_tenant_config success", K(tenant_id));
  return ret;
}

int ObMultiTenant::update_palf_config()
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService *);
  if (NULL == log_service) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = log_service->update_palf_options_except_disk_usage_limit_size();
  }
  return ret;
}

int ObMultiTenant::update_tenant_dag_scheduler_config()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler*);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler should not be null", K(ret));
  } else {
    dag_scheduler->reload_config();
  }
  return ret;
}

int ObMultiTenant::update_tenant_freezer_config_()
{
  int ret = OB_SUCCESS;
  ObTenantFreezer *freezer = MTL(ObTenantFreezer*);
  if (NULL == freezer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant freezer should not be null", K(ret));
  } else if (OB_FAIL(freezer->reload_config())) {
    LOG_WARN("tenant freezer config update failed", K(ret));
  }
  return ret;
}

int ObMultiTenant::update_tenant_freezer_mem_limit(const uint64_t tenant_id,
                                                   const int64_t tenant_min_mem,
                                                   const int64_t tenant_max_mem)
{
  int ret = OB_SUCCESS;

  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObTenantFreezer *freezer = nullptr;
  if (tenant_id != MTL_ID() && OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id));
  } else if (FALSE_IT(freezer = MTL(ObTenantFreezer *))) {
  } else if (freezer->is_tenant_mem_changed(tenant_min_mem, tenant_max_mem)) {
    if (OB_FAIL(freezer->set_tenant_mem_limit(tenant_min_mem, tenant_max_mem))) {
      LOG_WARN("set tenant mem limit failed", K(ret));
    }
  }
  return ret;
}


int ObMultiTenant::get_tenant_unit(const uint64_t tenant_id, ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;

  SpinRLockGuard guard(lock_);
  if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(tenant_id), K(ret));
  } else {
    unit = tenant->get_unit();
  }

  return ret;
}

int ObMultiTenant::get_unit_id(const uint64_t tenant_id, uint64_t &unit_id)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;

  SpinRLockGuard guard(lock_);
  if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(tenant_id), K(ret));
  } else {
    unit_id = tenant->get_unit_id();
  }
  return ret;
}

int ObMultiTenant::get_tenant_units(share::TenantUnits &units, bool include_hidden_sys)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCC(ret); it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is nullptr", K(ret));
    } else if (is_virtual_tenant_id((*it)->id()) || (!include_hidden_sys && (*it)->is_hidden())) {
      // skip
    } else if (OB_FAIL(units.push_back((*it)->get_unit()))) {
      LOG_WARN("fail to push back unit", K(ret));
    }
  }
  return ret;
}

int ObMultiTenant::get_tenant_metas(common::ObIArray<ObTenantMeta> &metas)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCC(ret); it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant is nullptr", K(ret));
    } else if (is_virtual_tenant_id((*it)->id()) || (*it)->is_hidden()) {
      // skip
    } else if (OB_FAIL(metas.push_back((*it)->get_tenant_meta()))) {
      LOG_WARN("fail to push back tenant meta", K(ret));
    }
  }
  return ret;
}

int ObMultiTenant::get_tenant_metas_for_ckpt(common::ObIArray<ObTenantMeta> &metas)
{
  int ret = OB_SUCCESS;
  // Ensure that no tenants are being created or deleted
  ObBucketTryRLockAllGuard all_tenant_guard(bucket_lock_);
  if (OB_FAIL(all_tenant_guard.get_ret())) {
    LOG_WARN("fail to try rlock all tenant for ckpt", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCC(ret); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant is nullptr", K(ret));
      } else if (is_virtual_tenant_id((*it)->id())) {
        // skip
      } else if (OB_FAIL(metas.push_back((*it)->get_tenant_meta()))) {
        LOG_WARN("fail to push back tenant meta", K(ret));
      }
    }
  }

  return ret;
}


//Don't call this, please call ObCompatModeGetter::get_tenant_compat_mode
int ObMultiTenant::get_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(tenant_id), K(ret));
  } else {
    compat_mode = tenant->get_compat_mode();
  }

  return ret;
}

int ObMultiTenant::update_tenant_cpu(const uint64_t tenant_id, const double min_cpu, const double max_cpu)
{
  int ret = OB_SUCCESS;

  ObTenant *tenant = NULL;
  bool do_update = false;
  SpinRLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("can't modify tenant which doesn't exist", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected condition, tenant is NULL", K(tenant));
  } else {
    if (tenant->unit_min_cpu() != min_cpu) {
      tenant->set_unit_min_cpu(min_cpu);
      do_update = true;
    }
    if (tenant->unit_max_cpu() != max_cpu) {
      tenant->set_unit_max_cpu(max_cpu);
      do_update = true;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("update tenant cpu failed", K(tenant_id), K(ret));
  } else if (do_update) {
    LOG_INFO("update tenant cpu", K(tenant_id), K(min_cpu), K(max_cpu), K(ret));
  }

  return ret;
}

int ObMultiTenant::modify_tenant_io(const uint64_t tenant_id, const ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = NULL;

  if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("can't modify tenant which doesn't exist", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected condition, tenant is NULL", K(tenant));
  } else {
    ObTenantIOConfig io_config;
    io_config.memory_limit_ = unit_config.memory_size();
    io_config.unit_config_.min_iops_ = unit_config.min_iops();
    io_config.unit_config_.max_iops_ = unit_config.max_iops();
    io_config.unit_config_.weight_ = unit_config.iops_weight();
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", K(ret), K(tenant_id));
    } else {
      io_config.callback_thread_count_ = tenant_config->_io_callback_thread_count;
      static const char *trace_mod_name = "io_tracer";
      io_config.enable_io_tracer_ = 0 == strncasecmp(trace_mod_name, GCONF.leak_mod_to_check.get_value(), strlen(trace_mod_name));
      if (OB_FAIL(OB_IO_MANAGER.refresh_tenant_io_config(tenant_id, io_config))) {
        LOG_WARN("refresh tenant io config failed", K(ret), K(tenant_id), K(io_config));
      }
    }
  }
  return ret;
}

bool ObMultiTenant::has_tenant(uint64_t tenant_id) const
{
  ObTenant *tenant = NULL;
  int ret = get_tenant(tenant_id, tenant);
  return OB_SUCCESS == ret && NULL != tenant;
}

bool ObMultiTenant::is_available_tenant(uint64_t tenant_id) const
{
  ObTenant *tenant = NULL;
  bool available = false;
  SpinRLockGuard guard(lock_);
  int ret = get_tenant_unsafe(tenant_id, tenant);
  if (OB_SUCCESS == ret && NULL != tenant) {
    if (tenant->get_create_status() == ObTenantCreateStatus::CREATE_COMMIT) {
      ObUnitInfoGetter::ObUnitStatus unit_status = tenant->get_unit().unit_status_;
      available = share::ObUnitInfoGetter::is_valid_tenant(unit_status);
    }
  }
  return available;
}

int ObMultiTenant::check_if_hidden_sys(const uint64_t tenant_id, bool &is_hidden_sys)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;

  SpinRLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    is_hidden_sys = false;
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("fail to get tennat", K(ret), K(tenant_id));
  } else {
    is_hidden_sys = tenant->is_hidden();
  }

  return ret;
}

int ObMultiTenant::mark_del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = NULL;

  SpinRLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("fail to get tennat", K(ret), K(tenant_id));
  } else {
    tenant->mark_tenant_is_removed();
    // only write slog when del tenant, no need to write here
  }

  return ret;
}

// 确保remove_tenant函数可以重复调用, 因为在删除租户时失败会不断重试,
// 这里只是删除内存结构，持久化的数据还在。
int ObMultiTenant::remove_tenant(const uint64_t tenant_id, bool &remove_tenant_succ)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenant *removed_tenant = nullptr;
  remove_tenant_succ = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant(tenant_id, removed_tenant))) {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      LOG_WARN("tenant has been removed", K(tenant_id), K(ret));
      removed_tenant = nullptr;
      remove_tenant_succ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("remove tenant failed", K(tenant_id), K(ret));
    }
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected condition", K(ret));
  } else {
    LOG_INFO("removed_tenant begin to stop", K(tenant_id));
    {
      SpinWLockGuard guard(lock_); //add a lock when set tenant stop, omt will check tenant has stop before calling timeup()
      removed_tenant->stop();
    }
    if (!is_virtual_tenant_id(tenant_id)) {
      LOG_INFO("removed_tenant begin to kill tenant session", K(tenant_id));
      if (OB_FAIL(GCTX.session_mgr_->kill_tenant(tenant_id))) {
        LOG_ERROR("fail to kill tenant session", K(ret), K(tenant_id));
        {
          SpinWLockGuard guard(lock_);
          removed_tenant->start();
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(removed_tenant)) {
    ObLDHandle handle;
    if (OB_FAIL(removed_tenant->try_wait())) {
      LOG_WARN("remove tenant try_wait failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(removed_tenant->try_wrlock(handle))) {
      LOG_WARN("can't get tenant wlock to remove tenant", K(ret), K(tenant_id),
          KP(removed_tenant), K(removed_tenant->lock_));
      removed_tenant->lock_.ld_.print();
    } else {
      ObTenant *removed_tenant_tmp = nullptr;
      SpinWLockGuard guard(lock_);
      // This locking should be held after tenant->wait
      // because there maybe locking during tenant thread stopping.

      if (OB_FAIL(tenants_.remove_if(tenant_id, compare_with_tenant_id, equal_with_tenant_id, removed_tenant_tmp))) {
        LOG_WARN("fail to remove tenant", K(tenant_id), K(ret));
      } else if (removed_tenant_tmp != removed_tenant) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must be same tenant", K(tenant_id), K(ret));
      } else {
        remove_tenant_succ = true;
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(GCTX.dblink_proxy_)) {
      if (OB_FAIL(GCTX.dblink_proxy_->clean_dblink_connection(tenant_id))) {
        LOG_WARN("failed to clean dblink connection", K(ret), K(tenant_id));
      }
    }

    if (OB_SUCC(ret)) {
      const share::ObUnitInfoGetter::ObTenantConfig &config = removed_tenant->get_unit();
      const int64_t log_disk_size = config.config_.log_disk_size();
      if (!is_virtual_tenant_id(tenant_id)) {
        GCTX.log_block_mgr_->remove_tenant(log_disk_size);
      }
      removed_tenant->destroy();
      ob_delete(removed_tenant);
      LOG_INFO("remove tenant success", K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("malloc allocator is NULL", K(ret));
    } else {
      auto& cache_washer = ObKVGlobalCache::get_instance();
      if (OB_FAIL(cache_washer.sync_flush_tenant(tenant_id))) {
        LOG_WARN("Fail to sync flush tenant cache", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_virtual_tenant_id(tenant_id) &&
        OB_FAIL(ObVirtualTenantManager::get_instance().del_tenant(tenant_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("virtual tenant manager delete tenant failed", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(GCTX.disk_reporter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("disk reporter is null", K(ret));
    } else if (OB_FAIL(GCTX.disk_reporter_->delete_tenant_usage_stat(tenant_id))) {
      LOG_WARN("failed to delete_tenant_usage_stat", K(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    // only report event when ret = success
    ROOTSERVICE_EVENT_ADD("remove_tenant", "remove_tenant",
        "tenant_id", tenant_id,
        "addr", GCTX.self_addr(),
        "result", ret);
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(GCTX.dblink_proxy_)) {
    if (OB_FAIL(GCTX.dblink_proxy_->clean_dblink_connection(tenant_id))) {
      LOG_WARN("failed to clean dblink connection", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMultiTenant::clear_persistent_data(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_clog_dir[MAX_PATH_SIZE] = {0};
  char tenant_slog_dir[MAX_PATH_SIZE] = {0};
  bool exist = true;

  if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_tenant_clog_dir(tenant_id, tenant_clog_dir))) {
    LOG_WARN("fail to get tenant clog dir", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_clog_dir, exist))) {
    LOG_WARN("fail to check exist", K(ret));
  } else if (exist) {
    if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(tenant_clog_dir))) {
      LOG_WARN("fail to delete clog dir", K(ret), K(tenant_clog_dir));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(SLOGGERMGR.get_tenant_slog_dir(tenant_id, tenant_slog_dir))) {
    LOG_WARN("fail to get tenant slog dir", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_slog_dir, exist))) {
    LOG_WARN("fail to check exist", K(ret));
  } else if (exist) {
    if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(tenant_slog_dir))) {
      LOG_WARN("fail to delete slog dir", K(ret), K(tenant_slog_dir));
    }
  }

  return ret;
}

int ObMultiTenant::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[DELETE_TENANT] OMT begin to delete tenant", K(tenant_id));

  ObTenant *tenant = nullptr;
  bool lock_succ = false;
  int64_t bucket_lock_idx = -1;
  TIMEGUARD_INIT(SERVER_OMT, 60_s, 120_s); // report hung cost more than 120s

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(bucket_lock_.try_wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id)))) {
    LOG_WARN("fail to try_wrlock for delete tenant", K(ret), K(tenant_id), K(bucket_lock_idx));
  } else if (FALSE_IT(lock_succ = true)) {
  } else if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(ret), K(tenant_id));
  } else if (tenant->is_hidden()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hidden tenant can't be deleted", K(ret), K(tenant_id));
  } else  {
    const ObUnitInfoGetter::ObTenantConfig local_unit = tenant->get_unit();
    const ObUnitInfoGetter::ObUnitStatus local_unit_status = local_unit.unit_status_;
    // add a event when try to gc for the first time
    if (local_unit_status != ObUnitInfoGetter::ObUnitStatus::UNIT_WAIT_GC_IN_OBSERVER &&
        local_unit_status != ObUnitInfoGetter::ObUnitStatus::UNIT_DELETING_IN_OBSERVER) {
      SERVER_EVENT_ADD("unit", "start unit gc", "tenant_id", tenant_id,
          "unit_id", local_unit.unit_id_, "unit_status", "DELETING");
    }

    // Ensure to write delete_tenant_prepare_slog only once
    if (local_unit_status != ObUnitInfoGetter::UNIT_DELETING_IN_OBSERVER) {
      tenant->set_unit_status(ObUnitInfoGetter::UNIT_DELETING_IN_OBSERVER);
      tenant->set_create_status(ObTenantCreateStatus::DELETING);
      if (OB_FAIL(write_delete_tenant_prepare_slog(tenant_id))) {
        LOG_WARN("fail to write delete tenant slog", K(ret), K(tenant_id), K(local_unit_status));
        tenant->set_unit_status(local_unit_status);
      }
    }

    if (OB_SUCC(ret)) {
      do {
        // 保证remove_tenant, clear_persistent_data可以幂等重试,
        // 如果失败会但不是加锁失败会一直无限重试, 保证如果prepare log写成功一定会有commit日志，
        // 即使这个过程中宕机重启, 重启回放日志时会继续删除并且补一条delete commit log
        bool remove_tenant_succ = false;
        if (OB_FAIL(remove_tenant(tenant_id, remove_tenant_succ))) {
          LOG_WARN("fail to remove tenant", K(ret), K(tenant_id));
          // If lock failed, the tenant is not removed from tenants_list,
          // Here can break and leave ObTenantNodeBalancer::check_del_tenant to retry again,
          // in this case, the deletion of other tenants does not get stuck.
          // Otherwise it will have to retry indefinitely here, because the tenant cannot be obtained
          if (false == remove_tenant_succ) {
            break;
          } else {
            SLEEP(1);
          }
        } else if (OB_FAIL(clear_persistent_data(tenant_id))) {
          LOG_ERROR("fail to clear persistent_data", K(ret), K(tenant_id));
          SLEEP(1);
        } else if (OB_FAIL(write_delete_tenant_commit_slog(tenant_id))) {
          LOG_WARN("fail to write delete tenant commit slog", K(ret), K(tenant_id));
        }
      } while (OB_FAIL(ret));

      if (OB_SUCC(ret)) {
        lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id);
        // add a event when finish gc unit
        SERVER_EVENT_ADD("unit", "finish unit gc", "tenant_id", tenant_id,
            "unit_id", local_unit.unit_id_, "unit_status", "DELETED");
      }
    }
  }

  if (lock_succ) {
    bucket_lock_.unlock(bucket_lock_idx);
  }

  LOG_INFO("[DELETE_TENANT] OMT finish delete tenant", KR(ret), K(tenant_id), K(bucket_lock_idx));

  return ret;
}

int ObMultiTenant::convert_real_to_hidden_sys_tenant()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObTenant *tenant = nullptr;
  int64_t bucket_lock_idx = -1;
  bool lock_succ = false;
  ObTenantMeta tenant_meta;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", K(ret), K(tenant_id));
  } else if (tenant->is_hidden()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has been hidden sys", K(ret));
  } else if (OB_FAIL(construct_meta_for_hidden_sys(tenant_meta))) {
    LOG_WARN("fail to construct_meta_for_hidden_sys", K(ret));
  } else if (OB_FAIL(bucket_lock_.try_wrlock(bucket_lock_idx = get_tenant_lock_bucket_idx(tenant_id)))) {
    LOG_WARN("fail to try_wrlock for delete tenant", K(ret), K(tenant_id), K(bucket_lock_idx));
  } else if (FALSE_IT(lock_succ = true)) {
  } else if (OB_FAIL(update_tenant_unit_no_lock(tenant_meta.unit_))) {
    LOG_WARN("fail to update_tenant_unit_no_lock", K(ret), K(tenant_meta));
  } else {
    ObTenantSwitchGuard guard(tenant);
    if (OB_FAIL(MTL(ObStorageLogger *)->get_active_cursor(tenant_meta.super_block_.replay_start_point_))) {
      LOG_WARN("get slog current cursor fail", K(ret));
    } else if (OB_UNLIKELY(!tenant_meta.super_block_.replay_start_point_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_cursor is invalid", K(ret), K(tenant_meta));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObServerCheckpointSlogHandler::get_instance()
      .write_tenant_super_block_slog(tenant_meta.super_block_)) {
    LOG_WARN("fail to write_tenant_super_block_slog", K(ret), K(tenant_meta));
  } else {
    tenant->set_tenant_super_block(tenant_meta.super_block_);
  }

  if (lock_succ) {
    bucket_lock_.unlock(bucket_lock_idx);
  }

  LOG_INFO("[DELETE_TENANT] OMT finish convert_real_to_hidden_sys_tenant", K(ret), K(tenant_meta), K(bucket_lock_idx));

  return ret;
}

int ObMultiTenant::update_tenant(uint64_t tenant_id, std::function<int(ObTenant&)> &&func)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  SpinRLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_WARN("get tenant by tenant id fail", K(ret));
  } else {
    ret = func(*tenant);
  }
  return ret;
}

int ObMultiTenant::get_tenant(
    const uint64_t tenant_id, ObTenant *&tenant) const
{
  SpinRLockGuard guard(lock_);
  return get_tenant_unsafe(tenant_id, tenant);
}

int ObMultiTenant::get_tenant_with_tenant_lock(
  const uint64_t tenant_id, ObLDHandle &handle, ObTenant *&tenant) const
{
  SpinRLockGuard guard(lock_);
  ObTenant *tenant_tmp = nullptr;
  int ret = get_tenant_unsafe(tenant_id, tenant_tmp);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_tmp->try_rdlock(handle))) {
      if (tenant_tmp->has_stopped()) {
        // in some cases this error code is handled specially
        ret = OB_TENANT_NOT_IN_SERVER;
        LOG_WARN("fail to try rdlock tenant", K(ret), K(tenant_id));
      }
    } else {
      // assign tenant when get rdlock succ
      tenant = tenant_tmp;
    }
    if (OB_UNLIKELY(tenant_tmp->has_stopped())) {
      LOG_WARN("get rdlock when tenant has stopped", K(tenant_id), K(lbt()));
    }
  }
  return ret;
}

int ObMultiTenant::get_active_tenant_with_tenant_lock(
  const uint64_t tenant_id, ObLDHandle &handle, ObTenant *&tenant) const
{
  SpinRLockGuard guard(lock_);
  ObTenant *tenant_tmp = nullptr;
  int ret = get_tenant_unsafe(tenant_id, tenant_tmp);
  if (OB_SUCC(ret)) {
    if (tenant_tmp->has_stopped()) {
      ret = OB_TENANT_NOT_IN_SERVER;
    } else if (OB_FAIL(tenant_tmp->try_rdlock(handle))) {
      if (tenant_tmp->has_stopped()) {
        // in some cases this error code is handled specially
        ret = OB_TENANT_NOT_IN_SERVER;
        LOG_WARN("fail to try rdlock tenant", K(ret), K(tenant_id));
      }
    } else {
      // assign tenant when get rdlock succ
      tenant = tenant_tmp;
    }
    if (OB_UNLIKELY(tenant_tmp->has_stopped())) {
      LOG_WARN("get rdlock when tenant has stopped", K(tenant_id), K(lbt()));
    }
  }
  return ret;
}

int ObMultiTenant::get_tenant_unsafe( const uint64_t tenant_id, ObTenant *&tenant) const
{
  int ret = OB_SUCCESS;

  tenant = NULL;
  for (TenantList::iterator it = tenants_.begin();
       it != tenants_.end() && NULL == tenant;
       it++) {
    if (OB_ISNULL(*it)) {
      // process the remains anyway
      LOG_ERROR("unexpected condition");
    } else if ((*it)->id() == tenant_id) {
      tenant = *it;
    }
  }

  if (NULL == tenant) {
    ret = OB_TENANT_NOT_IN_SERVER;
  }

  return ret;
}

int ObMultiTenant::write_create_tenant_prepare_slog(const ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_PREPARE);
  ObCreateTenantPrepareLog log_entry(*const_cast<ObTenantMeta*>(&meta));
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write put tenant slog", K(ret), K(log_param));
  }

  return ret;
}
int ObMultiTenant::write_create_tenant_commit_slog(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_COMMIT);
  ObCreateTenantCommitLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}
int ObMultiTenant::write_create_tenant_abort_slog(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_CREATE_TENANT_ABORT);
  ObCreateTenantAbortLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}

int ObMultiTenant::write_delete_tenant_prepare_slog(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_PREPARE);
  ObDeleteTenantPrepareLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}
int ObMultiTenant::write_delete_tenant_commit_slog(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_DELETE_TENANT_COMMIT);
  ObDeleteTenantCommitLog log_entry(tenant_id);
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write slog", K(ret), K(log_param));
  }

  return ret;
}

int ObMultiTenant::write_update_tenant_unit_slog(const ObUnitInfoGetter::ObTenantConfig &unit)
{
  int ret = OB_SUCCESS;
  ObStorageLogParam log_param;
  int32_t cmd = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_SERVER_TENANT,
      ObRedoLogSubType::OB_REDO_LOG_UPDATE_TENANT_UNIT);
  ObUpdateTenantUnitLog log_entry(*const_cast<ObUnitInfoGetter::ObTenantConfig*>(&unit));
  log_param.data_ = &log_entry;
  log_param.cmd_ = cmd;
  if (OB_FAIL(server_slogger_->write_log(log_param))) {
    LOG_WARN("failed to write tenant unit slog", K(ret), K(log_param));
  }

  return ret;
}


int ObMultiTenant::recv_request(const uint64_t tenant_id, ObRequest &req)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = NULL;
  SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    LOG_ERROR("get tenant failed", K(ret), K(tenant_id));
  } else if (NULL == tenant) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant->recv_request(req))) {
    LOG_ERROR("recv request failed", K(ret), K(tenant_id));
  } else {
    // do nothing
  }
  return ret;
}

void ObMultiTenant::get_tenant_ids(TenantIdList &id_list)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  id_list.clear();
  for (TenantList::iterator it = tenants_.begin();
       it != tenants_.end() && OB_SUCCESS == ret;
       it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret), K(*it));
    } else if (OB_FAIL(id_list.push_back((*it)->id()))) {
      LOG_ERROR("push tenant id to id list fail", K(ret));
    }
    ret = OB_SUCCESS;  // process anyway
  }
}

int ObMultiTenant::get_mtl_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin();
       it != tenants_.end() && OB_SUCC(ret);
       it++) {
    if (OB_ISNULL(*it)) {
      LOG_ERROR("unexpected condition", K(*it));
    } else if (is_virtual_tenant_id((*it)->id())) {
      // do nothing
    } else if (OB_FAIL(tenant_ids.push_back((*it)->id()))) {
      LOG_ERROR("push tenant id to tenant_ids fail", K(ret));
    }
  }
  return ret;
}

int ObMultiTenant::for_each(std::function<int(ObTenant &)> func)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin();
       it != tenants_.end() && OB_SUCCESS == ret;
       it++) {
    if (OB_ISNULL(*it)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret), K(*it));
    } else if (OB_FAIL(func(**it))) {
      LOG_ERROR("invoke func failed", K(ret), K(**it));
    }
  }
  return ret;
}

int ObMultiTenant::operate_in_each_tenant(const std::function<int()> &func, bool skip_virtual_tenant)
{
  int ret = OB_SUCCESS;
  TenantIdList id_list;
  get_tenant_ids(id_list);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  for (int64_t i = 0; i < id_list.size(); i++) {
    auto id = id_list[i];
    int tmp_ret = OB_SUCCESS;
    if (skip_virtual_tenant && is_virtual_tenant_id(id)) {
      continue;
    }
    if (OB_SUCCESS != (tmp_ret = guard.switch_to(id))) {
      LOG_WARN("switch to tenant failed", K(tmp_ret), K(id));
    } else if (OB_FAIL(func())) {
      LOG_WARN("execute func failed", K(ret), K(id));
    } else {
    }
  }
  return ret;
}

int ObMultiTenant::operate_each_tenant_for_sys_or_self(const std::function<int()> &func, bool skip_virtual_tenant)
{
  int ret = OB_SUCCESS;
  if (MTL_ID() == OB_SYS_TENANT_ID) {
    ret = operate_in_each_tenant(func, skip_virtual_tenant);
  } else {
    int id = MTL_ID();
    if (skip_virtual_tenant && is_virtual_tenant_id(id)) {
    } else if (OB_FAIL(func())) {
      LOG_WARN("execute func failed", K(ret), K(id));
    }
  }
  return ret;
}

int ObMultiTenant::get_tenant_cpu_usage(const uint64_t tenant_id, double &usage) const
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  usage = 0.;
  if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    } else {
      usage = tenant->get_token_usage() * tenant->unit_min_cpu();
    }
    lock_.unlock();
  }

  return ret;
}

int ObMultiTenant::get_tenant_worker_time(const uint64_t tenant_id, int64_t &worker_time) const
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  worker_time = 0.;
  if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    } else {
      worker_time = tenant->get_worker_time();
    }
    lock_.unlock();
  }

  return ret;
}

int ObMultiTenant::get_tenant_cpu_time(const uint64_t tenant_id, int64_t &cpu_time) const
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = nullptr;
  cpu_time = 0;
  if (OB_NOT_NULL(GCTX.cgroup_ctrl_) && GCTX.cgroup_ctrl_->is_valid()) {
    ret = GCTX.cgroup_ctrl_->get_cpu_time(tenant_id, cpu_time);
  } else {
    if (!lock_.try_rdlock()) {
      ret = OB_EAGAIN;
    } else {
      if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
      } else {
        cpu_time = tenant->get_cpu_time();
      }
      lock_.unlock();
    }
  }
  return ret;
}


int ObMultiTenant::get_tenant_cpu(
    const uint64_t tenant_id, double &min_cpu, double &max_cpu) const
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = NULL;

  if (!lock_.try_rdlock()) {
    ret = OB_EAGAIN;
  } else {
    if (OB_FAIL(get_tenant_unsafe(tenant_id, tenant))) {
    } else if (NULL != tenant) {
      min_cpu = tenant->unit_min_cpu();
      max_cpu = tenant->unit_max_cpu();
    }
    lock_.unlock();
  }

  return ret;
}

void ObMultiTenant::run1()
{
  lib::set_thread_name("MultiTenant");
  while (!has_set_stop()) {
    {
      SpinRLockGuard guard(lock_);
      for (TenantList::iterator it = tenants_.begin(); it != tenants_.end(); it++) {
        if (OB_ISNULL(*it)) {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected condition");
        } else if ((*it)->has_stopped()) {
          // skip stopped tenant
        } else {
          (*it)->timeup();
        }
      }
    }
    ob_usleep(TIME_SLICE_PERIOD);

    if (REACH_TIME_INTERVAL(10000000L)) {  // every 10s
      SpinRLockGuard guard(lock_);
      for (TenantList::iterator it = tenants_.begin(); it != tenants_.end(); it++) {
        if (!OB_ISNULL(*it)) {
          ObTaskController::get().allow_next_syslog();
          LOG_INFO("dump tenant info", "tenant", **it);
        }
      }
    }
  }
  LOG_INFO("OMT quit");
}

uint32_t ObMultiTenant::get_tenant_lock_bucket_idx(const uint64_t tenant_id)
{
  uint64_t hash_tenant_id = tenant_id * 13;
  return common::murmurhash(&hash_tenant_id, sizeof(uint64_t), 0) % OB_TENANT_LOCK_BUCKET_NUM;
}
int ObMultiTenant::check_if_unit_id_exist(const uint64_t unit_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  SpinRLockGuard guard(lock_);
  for (TenantList::iterator it = tenants_.begin(); it != tenants_.end() && OB_SUCCESS == ret; it++) {
    ObTenant *tenant = *it;
    if (OB_ISNULL(tenant)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected condition", K(ret), K(tenant));
    } else if (tenant->is_hidden() || is_virtual_tenant_id(tenant->id())) {
      // do nothing
    } else if (tenant->get_unit_id() == unit_id) {
      exist = true;
      break;
    }
  }
  return ret;
}

int obmysql::sql_nio_add_cgroup(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_new_sql_nio && GCONF._enable_tenant_sql_net_thread &&
      nullptr != GCTX.cgroup_ctrl_ &&
      OB_LIKELY(GCTX.cgroup_ctrl_->is_valid())) {
    ret = GCTX.cgroup_ctrl_->add_self_to_cgroup(tenant_id, OBCG_SQL_NIO);
  }
  return ret;
}

int ObSrvNetworkFrame::reload_tenant_sql_thread_config(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  // reload tenant_sql_login_thread_count
  int sql_login_thread_count = 0;
  if (tenant_config.is_valid()) {
    sql_login_thread_count = tenant_config->tenant_sql_login_thread_count;
  }

  ObTenantBase *tenant = NULL;
  MTL_SWITCH(tenant_id) {
    if (0 == sql_login_thread_count) {
      tenant = MTL_CTX();
      sql_login_thread_count = tenant ? std::max((int)tenant->unit_min_cpu(), 1) : 1;
    }

    QueueThread *mysql_queue = MTL(QueueThread *);
    if (OB_NOT_NULL(mysql_queue) && mysql_queue->set_thread_count(sql_login_thread_count)) {
      LOG_WARN("update tenant_sql_login_thread_count fail", K(ret));
    }
  }

  // // reload tenant_sql_net_thread_count
  // int sql_net_thread_count = 0;
  // if (tenant_config.is_valid()) {
  //   sql_net_thread_count = tenant_config->tenant_sql_net_thread_count;

  //   MTL_SWITCH(tenant_id) {
  //   if (0 == sql_net_thread_count) {
  //     sql_net_thread_count =
  //         NULL == tenant ? 1 : std::max((int)tenant->unit_min_cpu(), 1);
  //   }
  //     ObSqlNioServer *sql_nio_server = MTL(ObSqlNioServer *);
  //     int cur_sql_net_thread_count =
  //         sql_nio_server->get_nio()->get_thread_count();
  //     if (sql_net_thread_count < cur_sql_net_thread_count) {
  //       LOG_WARN("decrease tenant_sql_net_thread_count not allowed", K(ret),
  //                K(sql_net_thread_count), K(cur_sql_net_thread_count));
  //       tenant_config->tenant_sql_net_thread_count = cur_sql_net_thread_count;
  //     } else if (OB_FAIL(
  //                    sql_nio_server->set_thread_count(sql_net_thread_count))) {
  //       LOG_WARN("update tenant_sql_net_thread_count fail", K(ret),
  //                K(sql_net_thread_count));
  //     }
  //   }

  return ret;
}

int ObSrvNetworkFrame::reload_sql_thread_config()
{
  int ret = OB_SUCCESS;
  int cnt = deliver_.get_mysql_login_thread_count_to_set(
      GCONF.sql_login_thread_count);
  if (OB_FAIL(deliver_.set_mysql_login_thread_count(cnt))) {
    LOG_WARN("update sql_login_thread_count error", K(ret));
  }

  int sql_net_thread_count = (int)GCONF.sql_net_thread_count;
  if (sql_net_thread_count == 0) {
    if (GCONF.net_thread_count == 0) {
      sql_net_thread_count = get_default_net_thread_count();
    } else {
      sql_net_thread_count = GCONF.net_thread_count;
    }
  }

  if (OB_NOT_NULL(obmysql::global_sql_nio_server)) {
    int cur_sql_net_thread_count =
        obmysql::global_sql_nio_server->get_nio()->get_thread_count();
    if (sql_net_thread_count < cur_sql_net_thread_count) {
      LOG_WARN("decrease sql_net_thread_count not allowed", K(ret),
               K(sql_net_thread_count), K(cur_sql_net_thread_count));
      GCONF.sql_net_thread_count = cur_sql_net_thread_count;
    } else if (OB_FAIL(obmysql::global_sql_nio_server->set_thread_count(
                   sql_net_thread_count))) {
      LOG_WARN("update sql_net_thread_count error", K(ret));
    }
  }

  if (GCONF._enable_new_sql_nio && GCONF._enable_tenant_sql_net_thread) {
    omt::TenantIdList ids;
    if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr", K(ret));
    } else {
      GCTX.omt_->get_tenant_ids(ids);
      for (int64_t i = 0; i < ids.size(); i++) {
        int tenant_id = ids[i];
        if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
          reload_tenant_sql_thread_config(tenant_id);
        }
      }
    }
  }
  return ret;
}

int ObSharedTimer::mtl_init(ObSharedTimer *&st)
{
  int ret = common::OB_SUCCESS;
  if (st != NULL) {
    int &tg_id = st->tg_id_;
    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TntSharedTimer, tg_id))) {
      LOG_WARN("init shared timer failed", K(ret));
    }
  }
  return ret;
}

int ObSharedTimer::mtl_start(ObSharedTimer *&st)
{
  int ret = common::OB_SUCCESS;
  if (st != NULL) {
    int &tg_id = st->tg_id_;
    if (OB_FAIL(TG_START(tg_id))) {
      LOG_WARN("init shared timer failed", K(ret), K(tg_id));
    }
  }
  return ret;
}

void ObSharedTimer::mtl_stop(ObSharedTimer *&st)
{
  if (st != NULL) {
    int &tg_id = st->tg_id_;
    if (tg_id > 0) {
      TG_STOP(tg_id);
    }
  }
}

void ObSharedTimer::mtl_wait(ObSharedTimer *&st)
{
  if (st != NULL) {
    int &tg_id = st->tg_id_;
    if (tg_id > 0) {
      TG_WAIT_ONLY(tg_id);
    }
  }
}

void ObSharedTimer::destroy()
{
  if (tg_id_ > 0) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
}
