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

#include <sys/statvfs.h>
#include "lib/ob_running_mode.h"
#include "lib/oblog/ob_base_log_buffer.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "lib/file/file_directory_utils.h"
#include "lib/thread/thread_mgr.h"
#include "lib/lock/ob_latch.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_define.h"
#include "lib/profile/ob_active_resource_list.h"
#include "lib/profile/ob_profile_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/string/ob_sql_string.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "lib/io/ob_io_benchmark.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/net/ob_net_util.h"
#include "lib/alloc/memory_dump.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "share/cache/ob_cache_name_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_tenant_mgr.h"
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_cluster_version.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_task.h"
#include "observer/ob_server.h"
#include "observer/table/ob_table_rpc_processor.h"
#include "sql/ob_sql_init.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/ob_sql_init.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_store.h"
#include "storage/ob_long_ops_monitor.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "ob_rpc_extra_payload.h"
#include "share/ob_task_define.h"
#include "share/sequence/ob_sequence_cache.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/transaction/ob_gc_partition_adapter.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_table_store_stat_mgr.h"
#include "storage/ob_sstable_merge_info_mgr.h"
#include "storage/ob_partition_scheduler.h"
#include "sql/engine/px/ob_px_worker.h"
#include "share/ob_get_compat_mode.h"
#include "lib/task/ob_timer_monitor.h"
#include "share/ob_force_print_log.h"
#include "share/backup/ob_backup_file_lock_mgr.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "observer/ob_server_memory_cutter.h"
#include "share/ob_bg_thread_monitor.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "lib/oblog/ob_log_compressor.h"
#include "observer/table/ob_table_ttl_task.h"
#include "observer/table/ob_table_ttl_manager.h"
//#include "share/ob_ofs.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace observer {
ObServer::ObServer()
    : need_ctas_cleanup_(true),
      stop_(true),
      has_stopped_(true),
      has_destroy_(false),
      net_frame_(gctx_),
      sql_conn_pool_(),
      dblink_conn_pool_(),
      restore_ctx_(),
      srv_rpc_proxy_(),
      rs_rpc_proxy_(),
      sql_proxy_(),
      remote_server_provider_(),
      remote_sql_proxy_(),
      dblink_proxy_(),
      executor_proxy_(),
      executor_rpc_(),
      interrupt_proxy_(),
      config_(ObServerConfig::get_instance()),
      reload_config_(config_, gctx_),
      config_mgr_(config_, reload_config_),
      tenant_config_mgr_(omt::ObTenantConfigMgr::get_instance()),
      tenant_timezone_mgr_(omt::ObTenantTimezoneMgr::get_instance()),
      schema_service_(schema::ObMultiVersionSchemaService::get_instance()),
      pt_operator_(ob_service_),
      remote_pt_operator_(ob_service_),
      location_fetcher_(),
      server_tracer_(),
      location_cache_(location_fetcher_),
      partition_cfy_(),
      gts_response_rpc_(),
      gts_(),
      bandwidth_throttle_(),
      sys_bkgd_net_percentage_(0),
      ethernet_speed_(0),
      session_mgr_(&ObPartitionService::get_instance()),
      query_ctx_mgr_(),
      root_service_monitor_(root_service_, ObPartitionService::get_instance()),
      ob_service_(gctx_),
      procor_(net_frame_.get_xlator(), self_addr_),
      multi_tenant_(procor_),
      vt_data_service_(root_service_, self_addr_, &config_),
      cache_size_calculator_(),
      weak_read_service_(),
      table_service_(),
      cgroup_ctrl_(),
      start_time_(ObTimeUtility::current_time()),
      zone_merged_version_(OB_MERGED_VERSION_INIT),
      global_last_merged_version_(OB_MERGED_VERSION_INIT),
      warm_up_start_time_(0),
      sort_dir_(),
      diag_(),
      scramble_rand_(),
      duty_task_(),
      sql_mem_task_(),
      long_ops_task_(),
      ctas_clean_up_task_(),
      refresh_active_time_task_(),
      refresh_network_speed_task_(),
      schema_status_proxy_(sql_proxy_),
      is_log_dir_empty_(false),
      conn_res_mgr_()
{
  memset(&gctx_, 0, sizeof(gctx_));
  lib::g_mem_cutter = &observer::g_server_mem_cutter;
}

ObServer::~ObServer()
{
  destroy();
}

int ObServer::init(const ObServerOptions &opts, const ObPLogWriterCfg &log_cfg)
{
  int ret = OB_SUCCESS;
  opts_ = opts;
  scramble_rand_.init(static_cast<uint64_t>(start_time_), static_cast<uint64_t>(start_time_ / 2));

  // server parameters be inited here.
  if (OB_FAIL(init_config())) {
    LOG_ERROR("init config fail", K(ret));
  }

  // set large page param
  ObLargePageHelper::set_param(config_.use_large_pages);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_compressor_.init())) {
      LOG_ERROR("log compressor init error.", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_LOGGER.init(log_cfg))) {
      LOG_ERROR("async log init error.", K(ret));
      ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    } else if (OB_FAIL(OB_LOGGER.set_log_compressor(&log_compressor_))) {
      LOG_ERROR("set log compressor error.", K(ret));
      ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_tz_info_mgr())) {
      LOG_ERROR("init tz_info_mgr fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSqlTaskFactory::get_instance().init())) {
      LOG_ERROR("sql task factory init failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ::oceanbase::sql::init_sql_factories();
    ::oceanbase::sql::init_sql_executor_singletons();
    ::oceanbase::sql::init_sql_expr_static_var();

    if (OB_SUCCESS != (ret = ObPreProcessSysVars::init_sys_var())) {
      LOG_ERROR("PreProcessing init system variable failed !", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableApiProcessorBase::init_session())) {
        LOG_WARN("failed to init static session", K(ret));
      } else if (OB_FAIL(init_loaddata_global_stat())) {
        LOG_WARN("fail to init global load data stat map", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_pre_setting())) {
    LOG_ERROR("init pre setting fail", K(ret));
  } else if (OB_FAIL(init_global_context())) {
    LOG_ERROR("init global context fail", K(ret));
  } else if (OB_FAIL(init_cluster_version())) {
    LOG_ERROR("init cluster version fail", K(ret));
  } else if (OB_FAIL(init_sql_proxy())) {
    LOG_ERROR("init sql connection pool fail", K(ret));
  } else if (OB_FAIL(init_io())) {
    LOG_ERROR("init io fail, ", K(ret));
  } else if (FALSE_IT(cgroup_ctrl_.init())) {
    LOG_ERROR("should never reach here!", K(ret));
  } else if (OB_FAIL(init_restore_ctx())) {
    LOG_ERROR("init restore context fail", K(ret));
#ifndef OB_USE_ASAN
  } else if (OB_FAIL(ObMemoryDump::get_instance().init())) {
    LOG_ERROR("init memory dumper fail", K(ret));
#endif
  } else if (OB_FAIL(ObDagScheduler::get_instance().init(OBSERVER.get_self()))) {
    LOG_ERROR("init scheduler fail, ", K(ret));
  } else if (OB_FAIL(init_global_kvcache())) {
    LOG_ERROR("init global kvcache failed", K(ret));
  } else if (OB_FAIL(schema_status_proxy_.init())) {
    LOG_ERROR("fail to init schema status proxy", K(ret));
  } else if (OB_FAIL(init_schema())) {
    LOG_ERROR("init schema fail", K(ret));
  } else if (OB_FAIL(init_network())) {
    LOG_ERROR("init network fail", K(ret));
  } else if (OB_FAIL(init_interrupt())) {
    LOG_ERROR("init interrupt fail", K(ret));
  } else if (OB_FAIL(rs_mgr_.init(&rs_rpc_proxy_, &config_, &sql_proxy_))) {
    LOG_ERROR("init rs_mgr_ failed", K(ret));
  } else if (OB_FAIL(server_tracer_.init(rs_rpc_proxy_, sql_proxy_))) {
    LOG_WARN("init server tracer failed", K(ret));
  } else if (OB_FAIL(init_ob_service())) {
    LOG_ERROR("init ob service fail", K(ret));
  } else if (OB_FAIL(init_root_service())) {
    LOG_ERROR("init root service fail", K(ret));
  } else if (OB_FAIL(root_service_monitor_.init())) {
    LOG_ERROR("init root service monitor failed", K(ret));
  } else if (OB_FAIL(init_sql())) {
    LOG_ERROR("init sql fail", K(ret));
  } else if (OB_FAIL(init_sql_runner())) {
    LOG_ERROR("init sql runner fail", K(ret));
  } else if (OB_FAIL(init_sequence())) {
    LOG_ERROR("init sequence fail", K(ret));
  } else if (OB_FAIL(init_pl())) {
    LOG_ERROR("init pl fail", K(ret));
  } else if (OB_FAIL(pt_operator_.init(sql_proxy_, &config_))) {
    LOG_WARN("partition table operator init failed", K(ret));
  } else if (OB_FAIL(init_gc_partition_adapter())) {
    LOG_WARN("gc partition adpater init failed", K(ret));
  } else if (OB_FAIL(location_fetcher_.init(config_,
                 pt_operator_,
                 remote_pt_operator_,
                 rs_mgr_,
                 rs_rpc_proxy_,
                 srv_rpc_proxy_,
                 ObPartitionService::get_instance().get_locality_manager(),
                 config_.cluster_id))) {
    LOG_WARN("location fetcher init failed", K(ret));
  } else if (OB_FAIL(location_cache_.init(schema_service_,
                 config_,
                 server_tracer_,
                 OB_LOCATION_CACHE_NAME,
                 config_.location_cache_priority,
                 ObPartitionService::get_instance().get_locality_manager(),
                 config_.cluster_id,
                 &remote_server_provider_))) {
    LOG_WARN("location cache init failed", K(ret));
  } else if (OB_FAIL(init_autoincrement_service())) {
    LOG_WARN("auto-increment service init failed", K(ret));
  } else if (OB_FAIL(init_bandwidth_throttle())) {
    LOG_WARN("init_bandwidth_throttle failed", K(ret));
  } else if (OB_FAIL(init_storage())) {
    LOG_WARN("init storage fail", K(ret));
  } else if (OB_FAIL(init_gts())) {
    LOG_ERROR("init gts fail", K(ret));
  } else if (OB_FAIL(init_ts_mgr())) {
    LOG_ERROR("init ts mgr fail", K(ret));
  } else if (OB_FAIL(weak_read_service_.init(net_frame_.get_req_transport()))) {
    LOG_WARN("weak_read_service init fail", K(ret));
  }
#ifdef _WITH_OSS
  else if (OB_FAIL(ObOssEnvIniter::get_instance().global_init())) {
    LOG_WARN("init oss storage fail", K(ret));
  }
#endif
  else if (OB_FAIL(ObTenantMutilAllocatorMgr::get_instance().init())) {
    LOG_WARN("ObTenantMutilAllocatorMgr init failed", K(ret));
  } else if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().init())) {
    LOG_WARN("init tenant config mgr fail", K(ret));
  } else if (OB_FAIL(init_multi_tenant())) {
    LOG_ERROR("init multi tenant fail", K(ret));
  } else if (OB_FAIL(init_ctas_clean_up_task())) {
    LOG_ERROR("init ctas clean up task fail", K(ret));
  } else if (OB_FAIL(init_refresh_active_time_task())) {
    LOG_ERROR("init refresh active time task fail", K(ret));
  } else if (OB_FAIL(init_refresh_network_speed_task())) {
    LOG_ERROR("init refresh network speed task fail", K(ret));
  } else if (OB_FAIL(user_col_stat_service_.init(&sql_proxy_, &config_))) {
    LOG_WARN("init user table column stat service failed");
  } else if (OB_FAIL(user_table_stat_service_.init(&sql_proxy_, &ObPartitionService::get_instance(), &config_))) {
    LOG_WARN("init user table stat service failed");
  } else if (OB_FAIL(ObStatManager::get_instance().init(&user_col_stat_service_, &user_table_stat_service_))) {
    LOG_WARN("init user data stat manager failed");
  } else if (OB_FAIL(opt_stat_service_.init(&sql_proxy_, &config_))) {
    LOG_WARN("init optimizer stat service failed");
  } else if (OB_FAIL(ObOptStatManager::get_instance().init(&opt_stat_service_, &sql_proxy_, &config_))) {
  } else if (OB_FAIL(pt_operator_.set_callback_for_obs(rs_rpc_proxy_, rs_mgr_, config_))) {
    LOG_WARN("set_use_rpc_table failed", K(ret));
  } else if (OB_FAIL(cache_size_calculator_.init(ObPartitionService::get_instance(), schema_service_))) {
    LOG_WARN("cache_size_calculator_ init failed", K(ret));
  } else if (OB_FAIL(ObSysTaskStatMgr::get_instance().set_self_addr(self_addr_))) {
    LOG_WARN("set sys task status self addr failed", K(ret));
  } else if (OB_FAIL(ObSSTableMergeInfoMgr::get_instance().init())) {
    LOG_WARN("init merge info mgr failed", K(ret));
  } else if (OB_FAIL(ObTableStoreStatMgr::get_instance().init())) {
    LOG_WARN("init table store stat mgr failed", K(ret));
  } else if (OB_FAIL(LONG_OPS_MONITOR_INSTANCE.init())) {
    LOG_WARN("fail to init long ops monitor instance", K(ret));
  } else if (OB_FAIL(ObCompatModeGetter::instance().init(&sql_proxy_))) {
    LOG_WARN("fail to init get compat mode server");
  } else if (OB_FAIL(table_service_.init(gctx_))) {
    LOG_WARN("failed to init table service", K(ret));
  } else if (OB_FAIL(ObTimerMonitor::get_instance().init())) {
    LOG_WARN("failed to init timer monitor", K(ret));
  } else if (OB_FAIL(ObBGThreadMonitor::get_instance().init())) {
    LOG_WARN("failed to init bg thread monitor", K(ret));
  } else if (OB_FAIL(ObLogArchiveInfoMgr::get_instance().init(sql_proxy_))) {
    LOG_WARN("failed to init ObLogArchiveInfoMgr", K(ret));
  } else if (OB_FAIL(ObBackupDestDetector::get_instance().init())) {
    LOG_WARN("failed to init ObBackupDestDetector", K(ret));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().init(sql_proxy_, ObBackupDestDetector::get_instance()))) {
    LOG_WARN("failed to init ObBackupInfo", K(ret));
  } else if (OB_FAIL(ObRestoreFatalErrorReporter::get_instance().init(rs_rpc_proxy_, rs_mgr_))) {
    LOG_WARN("failed to init ObRestoreFatalErrorReporter", K(ret));
  } else if (OB_FAIL(ObBackupFileLockMgr::get_instance().init())) {
    LOG_WARN("failed to init backup file lock mgr", K(ret));
  } else if (OB_FAIL(G_RES_MGR.init())) {
    LOG_WARN("failed to init resource plan", K(ret));
  } else if (OB_FAIL(ObTTLManager::get_instance().init())) {
    LOG_WARN("failed to init ttl manager", K(ret));
  } else {
    GDS.set_rpc_proxy(&rs_rpc_proxy_);
    LOG_INFO("ob server instance init succeed");
  }

  if (!OB_SUCC(ret)) {
    set_stop();
    destroy();
  }
  return ret;
}

void ObServer::destroy()
{
  // observer.destroy() be called under two scenarios:
  // 1. main() exit
  // 2. ObServer destruction.
  // ObServer itself is a static instance
  // during process exit, The destruction order of Observer and many other static instance are undefined.
  // this fact may cause double destruction.
  // If ObBackupInfo precedes ObServer destruction, ObServer destruction triggers the destruction of ObBackupInfo,
  // Cause ObBackupInfo to lock the mutex that has been destroyed by itself, and finally trigger the core
  // This is essentially an implementation problem of repeated destruction of ObBackupInfo (or one of its members).
  // ObServer also adds a layer of defense here.
  if (!has_destroy_ && has_stopped_) {
    LOG_WARN("destroy observer begin");

    ObBackupInfoMgr::get_instance().destroy();
    LOG_WARN("backup info destroyed");
    ObBackupDestDetector::get_instance().destroy();
    LOG_WARN("ObBackupDestDetector destroyed");
    ObBackupFileLockMgr::get_instance().destroy();
    LOG_WARN("backup file lock mgr detroy");
    ObTimerMonitor::get_instance().destroy();
    ObBGThreadMonitor::get_instance().destroy();
    TG_DESTROY(lib::TGDefIDs::ServerGTimer);
    LOG_WARN("timer destroyed");
    TG_DESTROY(lib::TGDefIDs::FreezeTimer);
    LOG_WARN("freeze timer destroyed");
    TG_DESTROY(lib::TGDefIDs::SqlMemTimer);
    LOG_WARN("sql memory manager timer destroyed");
    TG_DESTROY(lib::TGDefIDs::ServerTracerTimer);
    LOG_WARN("server trace timer destroyed");
    TG_DESTROY(lib::TGDefIDs::CTASCleanUpTimer);
    LOG_WARN("ctas clean up timer destroyed");
    root_service_.destroy();
    LOG_WARN("root service destroyed");
    ob_service_.destroy();
    LOG_WARN("ob service destroyed");
    sql_engine_.destroy();
    LOG_WARN("sql engine destroyed");
    ObDagScheduler::get_instance().destroy();
    LOG_WARN("ob dag scheduler destroyed");
    ObPartitionService::get_instance().destroy();
    LOG_WARN("partition service destroyed");
    GC_PARTITION_ADAPTER.destroy();
    LOG_WARN("gc partition adapter destroyed");
    location_cache_.destroy();
    LOG_WARN("location cache destroyed");
    weak_read_service_.destroy();
    LOG_WARN("weak read service destroyed");
    net_frame_.destroy();
    LOG_WARN("net frame destroyed");
#ifdef _WITH_OSS
    ObOssEnvIniter::get_instance().global_destroy();
    LOG_WARN("oss storage destroyed");
#endif
    ObIOManager::get_instance().destroy();
    LOG_WARN("io manager destroyed");
    ObMemoryDump::get_instance().destroy();
    LOG_WARN("memory dump destroyed");
    tenant_timezone_mgr_.destroy();
    LOG_WARN("tenant timezone manager destroyed");
    log_compressor_.destroy();
    LOG_WARN("log compressor destroyed");
    LOG_WARN("destroy observer end");
    has_destroy_ = true;
  }
}

int ObServer::start()
{
  int ret = OB_SUCCESS;
  gctx_.status_ = SS_STARTING;

  if (OB_FAIL(sig_worker_.start())) {
    LOG_ERROR("Start signal worker error", K(ret));
  } else if (OB_FAIL(signal_handle_.start())) {
    LOG_ERROR("Start signal handle error", K(ret));
  } else if (GCTX.is_in_phy_fb_mode()) {
    if (OB_FAIL(ObPartitionService::get_instance().start_physical_flashback())) {
      LOG_ERROR("failed to start physical flashback", K(ret));
    } else {
      LOG_INFO("[PHY_FLASHBACK]physical flashback succeed", K(ret));
      set_stop();
    }
  } else if (GCTX.is_in_phy_fb_verify_mode()) {
    if (OB_FAIL(ObPartitionService::get_instance().check_can_physical_flashback())) {
      LOG_ERROR("check_can_physical_flashback failed", K(ret));
    } else {
      LOG_INFO("[PHY_FLASHBACK_VERIFY]check_can_physical_flashback success", K(ret));
      set_stop();
    }
  } else if (OB_FAIL(start_gts())) {
    LOG_ERROR("start gts fail", K(ret));
  } else if (OB_FAIL(OB_TS_MGR.start())) {
    LOG_ERROR("start ts mgr failed", K(ret));
  } else if (OB_FAIL(multi_tenant_.start())) {
    LOG_ERROR("start muti tenant fail", K(ret));
  } else if (OB_FAIL(net_frame_.start())) {
    LOG_ERROR("start net frame fail", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().start())) {
    LOG_ERROR("start partition service fail", K(ret));
  } else if (OB_FAIL(weak_read_service_.start())) {
    LOG_ERROR("start weak read service fail", K(ret));
    // do not wait clog replay over, avoid blocking other module
  } else if (OB_FAIL(GC_PARTITION_ADAPTER.start())) {
    LOG_ERROR("gc partition adapter start failed", K(ret));
  } else if (OB_FAIL(root_service_monitor_.start())) {
    LOG_ERROR("start root service monitor fail", K(ret));
  } else if (OB_FAIL(ob_service_.start())) {
    LOG_ERROR("start oceanbase service fail", K(ret));
  } else if (OB_FAIL(cache_size_calculator_.start())) {
    LOG_ERROR("start cache size calculator failed", K(ret));
  } else if (OB_FAIL(reload_config_())) {
    LOG_ERROR("Reload configuration failed.", K(ret));
  } else if (OB_FAIL(ObTimerMonitor::get_instance().start())) {
    LOG_ERROR("failed to start timer monitor", K(ret));
  } else if (OB_FAIL(ObBGThreadMonitor::get_instance().start())) {
    LOG_ERROR("failed to start bg thread monitor", K(ret));
  } else if (OB_FAIL(ObBackupDestDetector::get_instance().start())) {
    LOG_ERROR("failed to start ObBackupDestDetector", K(ret));
  } else if (OB_FAIL(ObBackupInfoMgr::get_instance().start())) {
    LOG_ERROR("failed to start backup info", K(ret));
  } else if (OB_FAIL(ObRestoreFatalErrorReporter::get_instance().start())) {
    LOG_ERROR("failed to start ObRestoreFatalErrorReporter", K(ret));
  } else if (OB_FAIL(ObTTLManager::get_instance().start())) {
    LOG_ERROR("failed to start ObTTLManager", K(ret));
  } else {
    LOG_INFO("[NOTICE] server instance start succeed");
    stop_ = false;
    has_stopped_ = false;
  }

  // refresh server configure
  //
  // The version should be greater than original version but less
  // than normal one.
  if (OB_SUCC(ret)) {
    config_mgr_.got_version(ObSystemConfig::INIT_VERSION);
  }

  bool synced = false;
  while (OB_SUCC(ret) && !stop_ && !synced) {
    synced = multi_tenant_.has_synced();
    if (!synced) {
      SLEEP(1);
    }
  }
  LOG_INFO("[NOTICE] check if multi tenant synced", K(ret), K(stop_), K(synced));

  if (OB_SUCC(ret)) {
    do {
      if (stop_) {
        ret = OB_SERVER_IS_STOPPING;
      } else if (OB_FAIL(ObPartitionService::get_instance().wait_start_finish())) {
        if (OB_EAGAIN == ret) {
          usleep(100 * 1000);
        } else {
          LOG_WARN("wait scan inner table failed", K(ret));
        }
      } else {
        LOG_INFO("[NOTICE] wait scan inner table success");
      }
    } while (OB_EAGAIN == ret);
  }

  bool schema_ready = false;
  while (OB_SUCC(ret) && !stop_ && !schema_ready) {
    schema_ready = schema_service_.is_sys_full_schema();
    if (!schema_ready) {
      SLEEP(1);
    }
  }
  LOG_INFO("[NOTICE] check if schema ready", K(ret), K(stop_), K(schema_ready));

  bool timezone_usable = false;
  tenant_timezone_mgr_.set_start_refresh(true);
  while (OB_SUCC(ret) && !stop_ && !timezone_usable) {
    timezone_usable = tenant_timezone_mgr_.is_usable();
    if (!timezone_usable) {
      SLEEP(1);
    }
  }
  LOG_INFO("[NOTICE] check if timezone usable", K(ret), K(stop_), K(timezone_usable));

  if (OB_SUCC(ret)) {
    if (stop_) {
      ret = OB_SERVER_IS_STOPPING;
    } else if (OB_FAIL(check_server_can_start_service())) {
      LOG_ERROR("failed to check server can start service", KR(ret));
    }
  }

  if (!OB_SUCC(ret)) {
    set_stop();
    wait();
  } else if (!stop_) {
    GCTX.status_ = SS_SERVING;
    GCTX.start_service_time_ = ObTimeUtility::current_time();
    LOG_INFO("[NOTICE] observer start service", "start_service_time", GCTX.start_service_time_);
  }

  return ret;
}

void ObServer::set_stop()
{
  stop_ = true;
  ob_service_.set_stop();
  gctx_.status_ = SS_STOPPING;
}

int ObServer::stop()
{
  int ret = OB_SUCCESS;
  LOG_WARN("stop observer begin");

  LOG_INFO("begin stop cache size calcucator");
  cache_size_calculator_.stop();
  LOG_INFO("cache size calcucator has stopped");

  LOG_INFO("begin stop distributed scheduler manager");
  ObDistributedSchedulerManager *dist_sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(dist_sched_mgr)) {
    LOG_ERROR("distributed scheduler manager instance is NULL");
  } else if (OB_FAIL(dist_sched_mgr->stop())) {
    LOG_WARN("stop distributed scheduler manager fail", K(ret));
  }
  LOG_WARN("distributed scheduler manager has stopped");

  if (OB_NOT_NULL(dtl::ObDtl::instance())) {
    DTL.stop();
  }
  LOG_INFO("sqldtl stop");

  LOG_INFO("begin stop GDS");
  GDS.stop();
  LOG_WARN("GDS stopped");

  LOG_INFO("begin stop signal handle");
  signal_handle_.stop();
  LOG_WARN("signal handle stopped");

  LOG_INFO("begin stop signal worker");
  sig_worker_.stop();
  LOG_WARN("signal worker stopped");

  LOG_INFO("begin shutdown network");
  if (OB_FAIL(net_frame_.mysql_shutdown())) {
    LOG_WARN("mysql shutdown network fail", K(ret));
  }
  LOG_WARN("network has shutdown");

  ObBackupDestDetector::get_instance().stop();
  FLOG_INFO("ObBackupDestDetector stopped");
  ObBackupInfoMgr::get_instance().stop();
  FLOG_INFO("backup info stopped");
  ObRestoreFatalErrorReporter::get_instance().stop();
  FLOG_INFO("ObRestoreFatalErrorReporter stopped");

  LOG_INFO("begin stop ob_service");
  ob_service_.stop();
  LOG_WARN("ob service stopped");
  location_cache_.stop();
  LOG_WARN("location cache stopped");
  ObTimerMonitor::get_instance().stop();
  ObBGThreadMonitor::get_instance().stop();
  LOG_INFO("begin stop timer");
  TG_STOP(lib::TGDefIDs::ServerGTimer);
  LOG_WARN("timer stopped");
  TG_STOP(lib::TGDefIDs::FreezeTimer);
  LOG_WARN("freeze timer stopped");
  TG_STOP(lib::TGDefIDs::SqlMemTimer);
  LOG_WARN("sql memory manager timer stopped");
  TG_STOP(lib::TGDefIDs::ServerTracerTimer);
  LOG_WARN("server trace timer stopped");
  TG_STOP(lib::TGDefIDs::CTASCleanUpTimer);
  LOG_WARN("ctas clean up timer stopped");
  LOG_INFO("begin stop sql conn pool");
  sql_conn_pool_.stop();
  LOG_WARN("sql connection pool stopped");
  LOG_INFO("begin stop dblink conn pool");
  dblink_conn_pool_.stop();
  LOG_WARN("dblink connection pool stopped");

  LOG_INFO("begin stop root service monitor");
  root_service_monitor_.stop();
  LOG_WARN("root service monitor stopped", K(ret));
  LOG_INFO("begin stop root service");
  if (OB_FAIL(root_service_.stop())) {
    LOG_WARN("stop root service failed", K(ret));
  }
  LOG_WARN("root service stopped", K(ret));

  FLOG_INFO("begin stop io manager");
  ObIOManager::get_instance().set_no_working();
  FLOG_INFO("io manager stopped");

  FLOG_INFO("begin stop partition service");
  if (OB_FAIL(ObPartitionService::get_instance().stop())) {
    LOG_WARN("partition service monitor failed", K(ret));
  }
  LOG_WARN("partition service stopped", K(ret));

  LOG_INFO("begin stop gc partition adapter");
  GC_PARTITION_ADAPTER.stop();
  LOG_INFO("gc partition adapter stopped", K(ret));

  weak_read_service_.stop();

  LOG_INFO("begin stop ts mgr");
  OB_TS_MGR.stop();
  LOG_INFO("stop ts mgr success");

  LOG_INFO("begin stop gts");
  if (OB_FAIL(stop_gts())) {
    LOG_WARN("stop gts failed", K(ret));
  }
  LOG_INFO("gts stopped", K(ret));

  LOG_INFO("begin stop partition scheduler");
  ObPartitionScheduler::get_instance().stop_merge();
  LOG_WARN("partition scheduler stopped", K(ret));

  // It will wait for all requests done.
  LOG_INFO("begin stop multi tenant");
  multi_tenant_.stop();
  LOG_WARN("multi tenant stoppted");

  if (OB_FAIL(net_frame_.rpc_shutdown())) {
    LOG_WARN("rpc shutdown network fail", K(ret));
  }

  {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = net_frame_.high_prio_rpc_shutdown())) {
      LOG_WARN("rpc shutdown high priority rpc fail", K(tmp_ret));
    }
    if (OB_SUCCESS != (tmp_ret = net_frame_.batch_rpc_shutdown())) {
      ret = tmp_ret;
      LOG_WARN("clog rpc shutdown network fail", K(ret));
    }
  }
  // net frame, ensure net_frame should stop after multi_tenant_
  // stopping.
  LOG_INFO("begin stop net frame");
  if (OB_FAIL(net_frame_.stop())) {
    LOG_WARN("net frame stop failed", K(ret));
  }
  LOG_WARN("net frame stopped");

  has_stopped_ = true;

  LOG_WARN("stop observer end", K(ret));

  return ret;
}

int ObServer::wait()
{
  int ret = OB_SUCCESS;

  // wait for stop flag
  while (!stop_) {
    SLEEP(3);
  }

  LOG_INFO("stop signal detected, begin stopping");
  if (OB_FAIL(stop())) {
    LOG_WARN("stop observer fail");
  }

  ObTimerMonitor::get_instance().wait();
  ObBGThreadMonitor::get_instance().wait();

  // cache size calculator
  cache_size_calculator_.wait();

  // timer
  TG_WAIT(lib::TGDefIDs::ServerGTimer);
  TG_WAIT(lib::TGDefIDs::FreezeTimer);
  TG_WAIT(lib::TGDefIDs::SqlMemTimer);
  TG_WAIT(lib::TGDefIDs::ServerTracerTimer);
  TG_WAIT(lib::TGDefIDs::CTASCleanUpTimer);
  LOG_INFO("wait timer success");
  root_service_.wait();
  LOG_INFO("wait root service success");
  root_service_monitor_.wait();
  LOG_WARN("wait root service monitor success");
  ObBackupDestDetector::get_instance().wait();
  LOG_INFO("wait ObBackupDestDetector success");
  ObBackupInfoMgr::get_instance().wait();
  LOG_INFO("wait backup info success");
  ObRestoreFatalErrorReporter::get_instance().wait();
  LOG_INFO("wait ObRestoreFatalErrorReporter success");

  // omt
  multi_tenant_.wait();
  multi_tenant_.destroy();

  FLOG_INFO("waiting for net_frame");
  net_frame_.wait();
  FLOG_INFO("waiting for sql_conn_pool");
  // over write previous ret.
  if (OB_FAIL(sql_conn_pool_.wait())) {
    LOG_WARN("wait inner sql connection release failed", K(ret));
  }
  FLOG_INFO("waiting for signal_handle");
  signal_handle_.wait();
  FLOG_INFO("waiting for signal worker");
  sig_worker_.wait();
  FLOG_INFO("waiting for ob_service");
  ob_service_.wait();
  FLOG_INFO("waiting for location cache");
  location_cache_.wait();
  FLOG_INFO("waiting for partition_service");
  ObPartitionService::get_instance().wait();
  FLOG_INFO("waiting for gc partition adapter");
  GC_PARTITION_ADAPTER.wait();
  FLOG_INFO("waiting for ts mgr");
  OB_TS_MGR.wait();
  wait_gts();
  FLOG_INFO("wait for weak read service");
  weak_read_service_.wait();
  FLOG_INFO("set gctx status stopped");
  gctx_.status_ = SS_STOPPED;

  return ret;
}

int ObServer::init_tz_info_mgr()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tenant_timezone_mgr_.init(sql_proxy_, self_addr_, schema_service_))) {
    LOG_WARN("tenant_timezone_mgr_ init failed", K_(self_addr), K(ret));
  }
  return ret;
}

int ObServer::init_config()
{
  int ret = OB_SUCCESS;
  bool has_config_file = true;

  // set dump path
  const char *dump_path = "etc/observer.config.bin";
  config_mgr_.set_dump_path(dump_path);
  if (OB_FILE_NOT_EXIST == (ret = config_mgr_.load_config())) {
    has_config_file = false;
    ret = OB_SUCCESS;
  }

  if (opts_.rpc_port_) {
    config_.rpc_port = opts_.rpc_port_;
    config_.rpc_port.set_version(start_time_);
  }

  if (opts_.mysql_port_) {
    config_.mysql_port = opts_.mysql_port_;
    config_.mysql_port.set_version(start_time_);
  }

  if (opts_.devname_ && strlen(opts_.devname_) > 0) {
    config_.devname.set_value(opts_.devname_);
    config_.devname.set_version(start_time_);
  } else {
    if (!has_config_file) {
      const char *devname = get_default_if();
      if (devname && '\0' != devname[0]) {
        LOG_INFO("guess interface name", K(devname));
        config_.devname.set_value(devname);
        config_.devname.set_version(start_time_);
      } else {
        LOG_INFO("can't guess interface name, use default bond0");
      }
    }
  }

  if (opts_.zone_ && strlen(opts_.zone_) > 0) {
    config_.zone.set_value(opts_.zone_);
    config_.zone.set_version(start_time_);
  }

  if (opts_.rs_list_ && strlen(opts_.rs_list_) > 0) {
    config_.rootservice_list.set_value(opts_.rs_list_);
    config_.rootservice_list.set_version(start_time_);
  }

  config_.syslog_level.set_value(OB_LOGGER.get_level_str());

  if (opts_.optstr_ && strlen(opts_.optstr_) > 0) {
    if (OB_FAIL(config_.add_extra_config(opts_.optstr_, start_time_))) {
      LOG_ERROR("invalid config from cmdline options", K(opts_.optstr_), K(ret));
    }
  }

  if (opts_.appname_ && strlen(opts_.appname_) > 0) {
    config_.cluster.set_value(opts_.appname_);
    config_.cluster.set_version(start_time_);
  }

  if (opts_.cluster_id_ >= 0) {
    // Strictly here, everything that is less than zero is ignored, not when it is equal to -1.
    config_.cluster_id = opts_.cluster_id_;
    config_.cluster_id.set_version(start_time_);
  }
  if (config_.cluster_id.get_value() >= 0) {
    obrpc::ObRpcNetHandler::CLUSTER_ID = config_.cluster_id.get_value();
    LOG_INFO("set CLUSTER_ID for rpc", "cluster_id", config_.cluster_id.get_value());
  }

  if (opts_.data_dir_ && strlen(opts_.data_dir_) > 0) {
    config_.data_dir.set_value(opts_.data_dir_);
    config_.data_dir.set_version(start_time_);
  }

  // The command line is specified, subject to the command line
  if (opts_.use_ipv6_) {
    config_.use_ipv6 = opts_.use_ipv6_;
    config_.use_ipv6.set_version(start_time_);
  }

  config_.print();

  if (OB_FAIL(ret)) {
    // nop
  } else if (OB_FAIL(config_.strict_check_special())) {
    LOG_ERROR("some config setting is not valid", K(ret));
  } else if (OB_FAIL(config_.check_and_refresh_major_compact_trigger())) {
    LOG_WARN("Failed to check and refresh major_compact_trigger", K(ret));
  } else if (OB_FAIL(set_running_mode())) {
    LOG_ERROR("set running mode failed", K(ret));
  } else {
    int32_t local_port = static_cast<int32_t>(config_.rpc_port);
    if (config_.use_ipv6) {
      char ipv6[MAX_IP_ADDR_LENGTH] = {'\0'};
      obsys::ObNetUtil::get_local_addr_ipv6(config_.devname, ipv6, sizeof(ipv6));
      self_addr_.set_ip_addr(ipv6, local_port);
    } else {
      int32_t ipv4 = ntohl(obsys::ObNetUtil::get_local_addr_ipv4(config_.devname));
      self_addr_.set_ipv4_addr(ipv4, local_port);
    }

    // initialize self address
    obrpc::ObRpcProxy::myaddr_ = self_addr_;
    LOG_INFO("my addr", K_(self_addr));
    config_.self_addr_ = self_addr_;

    // initialize configure module
    if (!self_addr_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("local address isn't valid", K(self_addr_), K(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerGTimer))) {
      LOG_ERROR("init timer fail", K(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::FreezeTimer))) {
      LOG_ERROR("init freeze timer fail", K(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::SqlMemTimer))) {
      LOG_ERROR("init sql memory manger timer fail", K(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerTracerTimer))) {
      LOG_WARN("fail to init server trace timer", KR(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::CTASCleanUpTimer))) {
      LOG_ERROR("fail to init ctas clean up timer", KR(ret));
    } else if (OB_FAIL(config_mgr_.base_init())) {
      LOG_WARN("config_mgr_ base_init failed", K(ret));
    } else if (OB_FAIL(config_mgr_.init(sql_proxy_, self_addr_))) {
      LOG_WARN("config_mgr_ init failed", K_(self_addr), K(ret));
    } else if (OB_FAIL(tenant_config_mgr_.init(sql_proxy_, self_addr_, &config_mgr_))) {
      LOG_WARN("tenant_config_mgr_ init failed", K_(self_addr), K(ret));
    }
  }

  get_unis_global_compat_version() = GET_MIN_CLUSTER_VERSION();
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100) {
    lib::g_runtime_enabled = true;
  }

  return ret;
}

int ObServer::set_running_mode()
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = GCONF.get_server_memory_limit();
  if (memory_limit < lib::ObRunningModeConfig::instance().MINI_MEM_LOWER) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_ERROR("memory limit too small", K(ret), K(memory_limit));
  } else if (memory_limit < lib::ObRunningModeConfig::instance().MINI_MEM_UPPER) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("observer start with mini_mode", K(memory_limit));
    lib::set_mini_mode(true);
  } else {
    lib::set_mini_mode(false);
  }
  _OB_LOG(INFO, "mini mode: %s", lib::is_mini_mode() ? "true" : "false");
  return ret;
}

int ObServer::init_pre_setting()
{
  int ret = OB_SUCCESS;

  reset_mem_leak_checker_label(GCONF.leak_mod_to_check.str());

  // oblog configuration
  if (OB_SUCC(ret)) {
    const int max_log_cnt = static_cast<int32_t>(config_.max_syslog_file_count);
    const int64_t max_log_time = config_.max_syslog_file_time;
    const bool enable_log_compress = config_.enable_syslog_file_compress;
    const bool record_old_log_file = config_.enable_syslog_recycle;
    const bool log_warn = config_.enable_syslog_wf;
    const bool enable_async_syslog = config_.enable_async_syslog;
    OB_LOGGER.set_max_file_index(max_log_cnt);
    OB_LOGGER.set_max_file_time(max_log_time);
    OB_LOGGER.set_enable_file_compress(enable_log_compress);
    OB_LOGGER.set_record_old_log_file(record_old_log_file);
    LOG_INFO("Whether record old log file", K(record_old_log_file));
    OB_LOGGER.set_log_warn(log_warn);
    LOG_INFO("Whether log warn", K(log_warn));
    OB_LOGGER.set_enable_async_log(enable_async_syslog);
    LOG_INFO("init log config", K(record_old_log_file), K(log_warn), K(enable_async_syslog));
    if (0 == max_log_cnt) {
      LOG_INFO("won't recycle log file");
    } else {
      LOG_INFO("recycle log file", "count", max_log_cnt);
    }
  }

  // task controller(log rate limiter)
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTaskController::get().init())) {
      LOG_ERROR("init task controller fail", K(ret));
    } else {
      ObTaskController::get().set_log_rate_limit(config_.syslog_io_bandwidth_limit);
      ObTaskController::get().switch_task(share::ObTaskType::GENERIC);
    }
  }

  // total memory limit
  if (OB_SUCC(ret)) {
    const int64_t limit_memory = config_.get_server_memory_limit();
    const int64_t reserved_memory = std::min(config_.cache_wash_threshold.get_value(),
        static_cast<int64_t>(static_cast<double>(limit_memory) * KVCACHE_FACTOR));
    const int64_t reserved_urgent_memory = config_.memory_reserved;
    if (LEAST_MEMORY_SIZE >= limit_memory) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("memory limit for oceanbase isn't sufficient",
          "need",
          LEAST_MEMORY_SIZE,
          "limit to",
          limit_memory,
          "sys mem",
          get_phy_mem_size(),
          K(reserved_memory),
          K(reserved_urgent_memory),
          K(ret));
    } else {
      LOG_INFO("set limit memory", K(limit_memory));
      set_memory_limit(limit_memory);
      LOG_INFO("set reserved memory", K(reserved_memory));
      ob_set_reserved_memory(reserved_memory);
      LOG_INFO("set urgent memory", K(reserved_urgent_memory));
      ob_set_urgent_memory(reserved_urgent_memory);
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t stack_size = std::max(512L << 10, static_cast<int64_t>(GCONF.stack_size));
    LOG_INFO("set stack_size", K(stack_size));
    lib::coro::config().stack_size_ = stack_size;
  }

  return ret;
}

int ObServer::init_sql_proxy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_conn_pool_.init(&schema_service_,
          &sql_engine_,
          &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator(),
          &pt_operator_,
          &config_))) {
    LOG_WARN("init sql connection pool failed", K(ret));
  } else if (OB_FAIL(sql_proxy_.init(&sql_conn_pool_))) {
    LOG_WARN("init sql proxy failed", K(ret));
  } else if (OB_FAIL(dblink_proxy_.init(&dblink_conn_pool_))) {
    LOG_WARN("init dblink proxy failed", K(ret));
  }
  return ret;
}

int ObServer::init_io()
{
  int ret = OB_SUCCESS;
  char ip_port_str[MAX_PATH_SIZE] = {};

  if (OB_FAIL(GCTX.self_addr_.ip_port_to_string(ip_port_str, MAX_PATH_SIZE))) {
    LOG_WARN("get server ip port fail", K(ret), K(GCTX.self_addr_));
  } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.init(
                 GCONF.data_dir, GCONF.cluster.str(), GCONF.cluster_id.get_value(), GCONF.zone.str(), ip_port_str))) {
    LOG_WARN("init OB_FILE_SYSTEM_ROUTER fail", K(ret));
  } else {
    MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_LOCAL;
  }

  if (OB_SUCC(ret)) {
    static const double IO_MEMORY_RATIO = 0.2;
    if (OB_FAIL(ObIOManager::get_instance().init(GCONF.get_reserved_server_memory() * IO_MEMORY_RATIO,
            ObDiskManager::MAX_DISK_NUM,
            ObIOManager::DEFAULT_IO_QUEUE_DEPTH))) {
      LOG_ERROR("init io manager fail, ", K(ret));
    } else {
      ObIOConfig io_config;
      int64_t cpu_cnt = GCONF.cpu_count;
      if (cpu_cnt <= 0) {
        cpu_cnt = common::get_cpu_num();
      }
      io_config.sys_io_low_percent_ = GCONF.sys_bkgd_io_low_percentage;
      io_config.sys_io_high_percent_ = GCONF.sys_bkgd_io_high_percentage;
      io_config.user_iort_up_percent_ = GCONF.user_iort_up_percentage;
      io_config.cpu_high_water_level_ = GCONF.sys_cpu_limit_trigger * cpu_cnt;
      io_config.disk_io_thread_count_ = GCONF.disk_io_thread_count;
      io_config.callback_thread_count_ = GCONF._io_callback_thread_count;
      if (OB_FAIL(ObIOManager::get_instance().set_io_config(io_config))) {
        LOG_ERROR("config io manager fail, ", K(ret));
      } else {
        // allow load benchmark fail
        if (OB_FAIL(ObIOBenchmark::get_instance().init("etc"))) {
          LOG_WARN("init io benchmark fail, ", K(ret));
        }
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObServer::init_restore_ctx()
{
  int ret = OB_SUCCESS;
  restore_ctx_.schema_service_ = &schema_service_;
  restore_ctx_.sql_client_ = &sql_proxy_;
  restore_ctx_.ob_sql_ = &sql_engine_;
  restore_ctx_.vt_iter_creator_ = &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator();
  restore_ctx_.partition_table_operator_ = &pt_operator_;
  restore_ctx_.server_config_ = &config_;
  restore_ctx_.rs_rpc_proxy_ = &rs_rpc_proxy_;
  return ret;
}

int ObServer::init_interrupt()
{
  int ret = OB_SUCCESS;
  ObGlobalInterruptManager *mgr = ObGlobalInterruptManager::getInstance();
  if (OB_ISNULL(mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get interrupt mgr instance", K(ret));
  } else if (OB_FAIL(mgr->init(get_self(), &interrupt_proxy_))) {
    LOG_WARN("fail init interrupt mgr", K(ret));
  }
  return ret;
}

int ObServer::init_loaddata_global_stat()
{
  int ret = OB_SUCCESS;
  ObGlobalLoadDataStatMap *map = ObGlobalLoadDataStatMap::getInstance();
  if (OB_ISNULL(map)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail allocate load data map for status", K(ret));
  } else if (OB_FAIL(map->init())) {
    LOG_WARN("fail init load data map", K(ret));
  }
  return ret;
}

int ObServer::init_network()
{
  int ret = OB_SUCCESS;

  obrpc::ObIRpcExtraPayload::set_extra_payload(ObRpcExtraPayload::extra_payload_instance());

  if (OB_FAIL(net_frame_.init())) {
    LOG_ERROR("init server network fail");
  } else if (OB_FAIL(net_frame_.get_proxy(srv_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", K(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(rs_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", K(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(executor_proxy_))) {
    LOG_WARN("get rpc proxy fail");
  } else if (OB_FAIL(net_frame_.get_proxy(load_data_proxy_))) {
    LOG_WARN("get rpc proxy fail", K(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(interrupt_proxy_))) {
    LOG_WARN("get rpc proxy fail");
  } else if (OB_FAIL(batch_rpc_.init(
                 net_frame_.get_batch_rpc_req_transport(), net_frame_.get_high_prio_req_transport(), self_addr_))) {
    LOG_WARN("init batch rpc failed", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::BRPC, batch_rpc_))) {
    STORAGE_LOG(WARN, "fail to start batch rpc proxy", K(ret));
  } else {
    srv_rpc_proxy_.set_server(get_self());
    rs_rpc_proxy_.set_rs_mgr(rs_mgr_);
  }

  return ret;
}

int ObServer::init_multi_tenant()
{
  int ret = OB_SUCCESS;

  // get node cpu count from sysconf if node_cpu_count config isn't
  // specified.
  int64_t node_cpu_count = get_cpu_num();
  if (GCONF.cpu_count > "0") {
    node_cpu_count = GCONF.cpu_count;
    LOG_INFO("calc NODE CPU COUNT using config(node_cpu_count) for test", K(node_cpu_count));
  } else {
    LOG_INFO("calc NODE CPU COUNT using sysconf function", K(node_cpu_count));
  }

  const int64_t cpu_count = node_cpu_count - GCONF.cpu_reserved;
  const int64_t times_of_workers = GCONF.workers_per_cpu_quota;

  multi_tenant_.set_quota2token(config_.cpu_quota_concurrency);
  if (OB_FAIL(multi_tenant_.init(self_addr_, static_cast<double>(cpu_count), times_of_workers, &sql_proxy_))) {
    LOG_ERROR("init multi tenant fail", K(ret));

    // Because system_cpu_quota is used for clog operations, so here
    // use server_cpu_quota_min/max.  This "System" tenant is
    // different from user recognized "system tenant", which
    // represents for RootService.  This tenant is a shared tenant
    // that used to process miscellaneous request for this server,
    // just as 500 tenant for memory management. It's better to use
    // 500 tenant as well for cpu usage, but since compatibility
    // problem we choose system tenant instead, which tenant id is 1.
    //
    // From outsider's point of view, each server should reserved some
    // resources for internal usage, including meomry and cpu. For
    // memory its name is "server_momory_limit", and
    // "server_cpu_quota_min/max" for CPU management.
  } else if (OB_FAIL(
                 multi_tenant_.add_tenant(OB_SYS_TENANT_ID, GCONF.server_cpu_quota_min, GCONF.server_cpu_quota_max))) {
    LOG_ERROR("add sys tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_SERVER_TENANT_ID, GCONF.system_cpu_quota, GCONF.system_cpu_quota))) {
    LOG_ERROR("add server tenant fail", K(ret));

  } else if (OB_FAIL(
                 multi_tenant_.add_tenant(OB_ELECT_TENANT_ID, GCONF.election_cpu_quota, GCONF.election_cpu_quota))) {
    LOG_ERROR("add elect tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(
                 OB_LOC_CORE_TENANT_ID, GCONF.core_location_cpu_quota(), GCONF.core_location_cpu_quota()))) {
    LOG_ERROR("add location core tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(
                 OB_LOC_ROOT_TENANT_ID, GCONF.root_location_cpu_quota(), GCONF.root_location_cpu_quota()))) {
    LOG_ERROR("add location root tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(
                 OB_LOC_SYS_TENANT_ID, GCONF.sys_location_cpu_quota(), GCONF.sys_location_cpu_quota()))) {
    LOG_ERROR("add elect tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(
                 OB_LOC_USER_TENANT_ID, GCONF.user_location_cpu_quota(), GCONF.user_location_cpu_quota()))) {
    LOG_ERROR("add elect tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_EXT_LOG_TENANT_ID, EXT_LOG_TENANT_CPU, EXT_LOG_TENANT_CPU))) {
    LOG_ERROR("add ext_log tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_MONITOR_TENANT_ID, OB_MONITOR_CPU, OB_MONITOR_CPU))) {
    LOG_ERROR("add monitor tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_DATA_TENANT_ID, OB_DATA_CPU, OB_DATA_CPU))) {
    LOG_ERROR("add data tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_DTL_TENANT_ID, OB_DTL_CPU, OB_DTL_CPU))) {
    LOG_ERROR("add DTL tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_RS_TENANT_ID, OB_RS_CPU, OB_RS_CPU))) {
    LOG_ERROR("add RS tenant fail", K(ret));

  } else if (OB_FAIL(multi_tenant_.add_tenant(OB_DIAG_TENANT_ID, OB_DIAG_CPU, OB_DIAG_CPU))) {
    LOG_ERROR("add diag tenant fail", K(ret));

  } else if (OB_FAIL(
                 multi_tenant_.add_tenant(OB_SVR_BLACKLIST_TENANT_ID, OB_SVR_BLACKLIST_CPU, OB_SVR_BLACKLIST_CPU))) {
    LOG_ERROR("add server balcklist tenant fail", K(ret));
  } else {
    // do nothing
  }

  // init allocator for OB_SYS_TENANT_ID and OB_SERVER_TENANT_ID
  int64_t min_sys_tenant_memory = config_.get_min_sys_tenant_memory();
  int64_t max_sys_tenant_memory = config_.get_max_sys_tenant_memory();
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (OB_SUCC(ret)) {
    if (!OB_ISNULL(allocator)) {
      allocator->set_tenant_limit(OB_SYS_TENANT_ID, max_sys_tenant_memory);
      allocator->set_tenant_limit(OB_SERVER_TENANT_ID, INT64_MAX);
    }
  }

  // set tenant mem limits
  ObTenantManager &omti = ObTenantManager::get_instance();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            omti.init(self_addr_, srv_rpc_proxy_, rs_rpc_proxy_, rs_mgr_, net_frame_.get_req_transport(), &config_))) {
      LOG_ERROR("Fail to init ObTenantManager, ", K(ret));
    } else if (OB_FAIL(omti.add_tenant(common::OB_SYS_TENANT_ID))) {
      LOG_ERROR("Fail to add sys tenant to tenant manager, ", K(ret));
    } else if (OB_FAIL(omti.set_tenant_mem_limit(OB_SYS_TENANT_ID, min_sys_tenant_memory, max_sys_tenant_memory))) {
      LOG_ERROR("Fail to set tenant mem limit", K(ret));
    } else if (OB_FAIL(omti.add_tenant(OB_SERVER_TENANT_ID))) {
      LOG_ERROR("Fail to add server tenant to tenant manager, ", K(ret));
    } else if (OB_FAIL(omti.set_tenant_mem_limit(OB_SERVER_TENANT_ID, 0, INT64_MAX))) {
      LOG_ERROR("Fail to set tenant mem limit, ", K(ret));
    } else if (OB_FAIL(omti.register_timer_task(lib::TGDefIDs::ServerGTimer))) {
      LOG_ERROR("Fail to register timer task", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(duty_task_.schedule(lib::TGDefIDs::ServerGTimer))) {
      LOG_WARN("schedule tenant duty task fail", K(ret));
    } else if (OB_FAIL(long_ops_task_.init(&schema_service_, lib::TGDefIDs::ServerGTimer))) {
      LOG_WARN("fail to init create index task", K(ret));
    } else if (OB_FAIL(sql_mem_task_.schedule(lib::TGDefIDs::SqlMemTimer))) {
      LOG_WARN("schedule tenant sql memory manager task fail", K(ret));
    }
  }

  return ret;
}

int ObServer::init_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_service_.init(
          &sql_proxy_, &dblink_proxy_, &config_, OB_MAX_VERSION_COUNT, OB_MAX_VERSION_COUNT_FOR_MERGE, false))) {
    LOG_WARN("init schema_service_ fail", K(ret));
  }

  return ret;
}

int ObServer::init_autoincrement_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAutoincrementService::get_instance().init(self_addr_,
          &sql_proxy_,
          &srv_rpc_proxy_,
          &location_cache_,
          &schema_service_,
          &ObPartitionService::get_instance()))) {
    LOG_WARN("init autoincrement_service_ fail", K(ret));
  }
  return ret;
}

int ObServer::init_global_kvcache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(ObKVGlobalCache::get_instance().get_suitable_bucket_num()))) {
    LOG_WARN("Fail to init ObKVGlobalCache, ", K(ret));
  } else if (OB_FAIL(ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance()))) {
    LOG_WARN("Fail to set_cache_washer", K(ret));
  }

  return ret;
}

int ObServer::init_ob_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_service_.init(sql_proxy_, server_tracer_))) {
    LOG_ERROR("oceanbase service init failed", K(ret));
  }
  return ret;
}

int ObServer::init_root_service()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(root_service_.init(config_,
          config_mgr_,
          srv_rpc_proxy_,
          rs_rpc_proxy_,
          self_addr_,
          sql_proxy_,
          remote_sql_proxy_,
          restore_ctx_,
          rs_mgr_,
          &schema_service_,
          pt_operator_,
          remote_pt_operator_,
          location_cache_))) {
    LOG_WARN("init root service failed", K(ret));
  }

  return ret;
}

int ObServer::init_sql()
{
  int ret = OB_SUCCESS;

  LOG_INFO("init sql");
  if (OB_FAIL(session_mgr_.init())) {
    LOG_WARN("init sql session mgr fail");
  } else if (OB_FAIL(query_ctx_mgr_.init())) {
    LOG_WARN("init query ctx mgr failed", K(ret));
  } else if (OB_FAIL(conn_res_mgr_.init(schema_service_))) {
    LOG_WARN("init user resource mgr failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, session_mgr_, ObSQLSessionMgr::SCHEDULE_PERIOD, true))) {
    LOG_WARN("tier schedule fail");
  } else {
    LOG_INFO("init sql session mgr done");
    LOG_INFO("init sql location cache done");
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_engine_.init(&ObStatManager::get_instance(),
            &ObOptStatManager::get_instance(),
            net_frame_.get_req_transport(),
            &ObPartitionService::get_instance(),
            &vt_data_service_,
            &location_cache_,
            self_addr_,
            rs_mgr_))) {
      LOG_WARN("init sql engine failed", K(ret));
    } else {
      LOG_INFO("init sql engine done");
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == dtl::ObDtl::instance()) {
      ret = OB_INIT_FAIL;
      LOG_WARN("allocate DTL service fail", K(ret));
    } else if (OB_FAIL(net_frame_.get_proxy(DTL.get_rpc_proxy()))) {
      LOG_WARN("initialize DTL RPC proxy fail", K(ret));
    } else if (OB_FAIL(DTL.init())) {
      LOG_WARN("fail initialize DTL instance", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("init sql done");
  } else {
    LOG_ERROR("init sql fail", K(ret));
  }
  return ret;
}

int ObServer::init_sql_runner()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(executor_rpc_.init(&executor_proxy_, &batch_rpc_))) {
    LOG_WARN("init executor rpc fail", K(ret));
  } else if (OB_ISNULL(ObIntermResultPool::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init IntermResult pool failed", K(ret));
  } else {
    LOG_INFO("init sql runner done");
  }

  return ret;
}

int ObServer::init_sequence()
{
  int ret = OB_SUCCESS;
  ObSequenceCache &cache = ObSequenceCache::get_instance();
  if (OB_FAIL(cache.init(schema_service_, sql_proxy_))) {
    LOG_ERROR("init sequence engine failed", K(ret));
  } else {
    LOG_INFO("init sequence engine done");
  }
  return ret;
}

int ObServer::init_pl()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObServer::init_global_context()
{
  int ret = OB_SUCCESS;

  gctx_.root_service_ = &root_service_;
  gctx_.ob_service_ = &ob_service_;
  gctx_.schema_service_ = &schema_service_;
  gctx_.config_ = &config_;
  gctx_.config_mgr_ = &config_mgr_;
  gctx_.pt_operator_ = &pt_operator_;
  gctx_.remote_pt_operator_ = &remote_pt_operator_;
  gctx_.srv_rpc_proxy_ = &srv_rpc_proxy_;
  gctx_.rs_rpc_proxy_ = &rs_rpc_proxy_;
  gctx_.load_data_proxy_ = &load_data_proxy_;
  gctx_.sql_proxy_ = &sql_proxy_;
  gctx_.remote_sql_proxy_ = &remote_sql_proxy_;
  gctx_.dblink_proxy_ = &dblink_proxy_;
  gctx_.executor_rpc_ = &executor_rpc_;
  gctx_.self_addr_ = self_addr_;
  gctx_.rs_mgr_ = &rs_mgr_;
  gctx_.par_ser_ = &ObPartitionService::get_instance();
  gctx_.gts_ = &gts_;
  gctx_.bandwidth_throttle_ = &bandwidth_throttle_;
  gctx_.vt_par_ser_ = &vt_data_service_;
  gctx_.session_mgr_ = &session_mgr_;
  gctx_.query_ctx_mgr_ = &query_ctx_mgr_;
  gctx_.sql_engine_ = &sql_engine_;
  gctx_.conn_res_mgr_ = &conn_res_mgr_;
  gctx_.omt_ = &multi_tenant_;
  gctx_.vt_iter_creator_ = &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator();
  gctx_.location_cache_ = &location_cache_;
  gctx_.start_time_ = start_time_;
  gctx_.merged_version_ = &zone_merged_version_;
  gctx_.global_last_merged_version_ = &global_last_merged_version_;
  gctx_.warm_up_start_time_ = &warm_up_start_time_;
  gctx_.status_ = SS_INIT;
  gctx_.rs_server_status_ = RSS_IS_WORKING;
  gctx_.start_service_time_ = 0;
  gctx_.ssl_key_expired_time_ = 0;
  gctx_.sort_dir_ = &sort_dir_;
  gctx_.diag_ = &diag_;
  gctx_.scramble_rand_ = &scramble_rand_;
  gctx_.init();
  (void)gctx_.set_split_schema_version(OB_INVALID_VERSION);
  (void)gctx_.set_split_schema_version_v2(OB_INVALID_VERSION);
  gctx_.weak_read_service_ = &weak_read_service_;
  gctx_.table_service_ = &table_service_;
  gctx_.cgroup_ctrl_ = &cgroup_ctrl_;
  gctx_.schema_status_proxy_ = &schema_status_proxy_;
  (void)gctx_.set_upgrade_stage(obrpc::OB_UPGRADE_STAGE_INVALID);

  if (opts_.mode_ && strlen(opts_.mode_) > 0) {
    if (0 == STRCASECMP(opts_.mode_, FLASHBACK_MODE_STR)) {
      gctx_.mode_ = PHY_FLASHBACK_MODE;
      LOG_INFO("set physical_flashback mode");
    } else if (0 == STRCASECMP(opts_.mode_, FLASHBACK_VERIFY_MODE_STR)) {
      gctx_.mode_ = PHY_FLASHBACK_VERIFY_MODE;
      LOG_INFO("set physical_flashback_verify mode");
    } else {
      gctx_.mode_ = NORMAL_MODE;
      LOG_INFO("set normal mode", "mode_", opts_.mode_);
    }
  }
  gctx_.flashback_scn_ = opts_.flashback_scn_;
  if ((PHY_FLASHBACK_MODE == gctx_.mode_ || PHY_FLASHBACK_VERIFY_MODE == gctx_.mode_) && 0 >= gctx_.flashback_scn_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid flashback scn in flashback mode",
        K(ret),
        "server_mode",
        gctx_.mode_,
        "flashback_scn",
        gctx_.flashback_scn_);
  } else {
    gctx_.inited_ = true;
  }

  return ret;
}

int ObServer::init_cluster_version()
{
  return ObClusterVersion::get_instance().init(&config_);
}

int ObServer::init_gts()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gts_response_rpc_.init(net_frame_.get_req_transport(), self_addr_))) {
    LOG_ERROR("gts response rpc init failed", K(ret));
  } else if (OB_FAIL(
                 gts_.init(ObPartitionService::get_instance().get_election_mgr(), &gts_response_rpc_, self_addr_))) {
    LOG_ERROR("gts init failed", K(ret));
  } else {
    LOG_INFO("gts init success");
  }
  return ret;
}

int ObServer::start_gts()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gts_response_rpc_.start())) {
    LOG_ERROR("gts response rpc start failed", K(ret));
  } else if (OB_FAIL(gts_.start())) {
    LOG_ERROR("gts start failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObServer::stop_gts()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gts_.stop())) {
    LOG_ERROR("gts stop failed", K(ret));
  } else if (OB_FAIL(gts_response_rpc_.stop())) {
    LOG_ERROR("gts response rpc stop failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObServer::wait_gts()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gts_.wait())) {
    LOG_ERROR("gts wait failed", K(ret));
  } else if (OB_FAIL(gts_response_rpc_.wait())) {
    LOG_ERROR("gts response rpc wait failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObServer::init_ts_mgr()
{
  int ret = OB_SUCCESS;
  ObILocationAdapter *location_adapter = ObPartitionService::get_instance().get_trans_service()->get_location_adapter();

  if (OB_FAIL(OB_TS_MGR.init(self_addr_, location_adapter, net_frame_.get_req_transport(), &gts_))) {
    LOG_ERROR("gts cache mgr init failed", K_(self_addr), KP(location_adapter), K(ret));
  } else {
    LOG_INFO("gts cache mgr init success");
  }

  return ret;
}

int ObServer::init_storage()
{
  int ret = OB_SUCCESS;

  bool clogdir_is_empty = false;
  bool ilogdir_is_empty = false;

  if (OB_SUCC(ret)) {
    // Check if the clog directory is empty
    if (OB_FAIL(is_dir_empty(OB_FILE_SYSTEM_ROUTER.get_clog_dir(), clogdir_is_empty))) {
      LOG_ERROR("is_dir_empty fail", K(ret));
    } else if (clogdir_is_empty) {
      LOG_INFO("clog dir is empty");
    } else {
      LOG_INFO("clog dir is not empty");
    }
  }

  if (OB_SUCC(ret)) {
    // Check if the ilog directory is empty
    if (OB_FAIL(is_dir_empty(OB_FILE_SYSTEM_ROUTER.get_ilog_dir(), ilogdir_is_empty))) {
      LOG_ERROR("is_dir_empty fail", K(ret));
    } else if (ilogdir_is_empty) {
      LOG_INFO("ilog dir is empty");
    } else {
      LOG_INFO("ilog dir is not empty");
    }
  }

  if (OB_SUCC(ret)) {
    is_log_dir_empty_ = clogdir_is_empty && ilogdir_is_empty;
  }

  if (OB_SUCC(ret)) {
    const char *redundancy_level = config_.redundancy_level;
    if (0 == strcasecmp(redundancy_level, "EXTERNAL")) {
      storage_env_.redundancy_level_ = ObStorageEnv::EXTERNAL_REDUNDANCY;
    } else if (0 == strcasecmp(redundancy_level, "NORMAL")) {
      storage_env_.redundancy_level_ = ObStorageEnv::NORMAL_REDUNDANCY;
    } else if (0 == strcasecmp(redundancy_level, "HIGH")) {
      storage_env_.redundancy_level_ = ObStorageEnv::HIGH_REDUNDANCY;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid redundancy level", K(ret), K(redundancy_level));
    }
  }

  if (OB_SUCC(ret)) {
    storage_env_.data_dir_ = OB_FILE_SYSTEM_ROUTER.get_data_dir();
    storage_env_.sstable_dir_ = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
    storage_env_.default_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;  // 2MB
    storage_env_.disk_avail_space_ = config_.datafile_size;
    storage_env_.datafile_disk_percentage_ = config_.datafile_disk_percentage;

    // log
    storage_env_.log_spec_.log_dir_ = OB_FILE_SYSTEM_ROUTER.get_slog_dir();
    storage_env_.log_spec_.max_log_size_ = 256 << 20;
    storage_env_.clog_dir_ = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
    storage_env_.ilog_dir_ = OB_FILE_SYSTEM_ROUTER.get_ilog_dir();
    storage_env_.clog_shm_path_ = OB_FILE_SYSTEM_ROUTER.get_clog_shm_dir();
    storage_env_.ilog_shm_path_ = OB_FILE_SYSTEM_ROUTER.get_ilog_shm_dir();

    // cache
    storage_env_.index_cache_priority_ = config_.index_cache_priority;
    storage_env_.user_block_cache_priority_ = config_.user_block_cache_priority;
    storage_env_.user_row_cache_priority_ = config_.user_row_cache_priority;
    storage_env_.fuse_row_cache_priority_ = config_.fuse_row_cache_priority;
    storage_env_.bf_cache_priority_ = config_.bf_cache_priority;
    storage_env_.clog_cache_priority_ = config_.clog_cache_priority;
    storage_env_.index_clog_cache_priority_ = config_.index_clog_cache_priority;
    storage_env_.bf_cache_miss_count_threshold_ = config_.bf_cache_miss_count_threshold;

    storage_env_.ethernet_speed_ = ethernet_speed_;
    storage_env_.disk_type_ = ObIOBenchmark::get_instance().get_disk_type();

    if (OB_FAIL(ObPartitionService::get_instance().init(storage_env_,
            self_addr_,
            &partition_cfy_,
            &schema_service_,
            &location_cache_,
            &rs_mgr_,
            &ob_service_,
            net_frame_.get_req_transport(),
            &batch_rpc_,
            rs_rpc_proxy_,
            srv_rpc_proxy_,
            sql_proxy_,
            remote_sql_proxy_,
            server_tracer_,
            bandwidth_throttle_))) {
      LOG_ERROR("init partition service fail", K(ret), K(storage_env_));
    }
  }
  return ret;
}

int ObServer::init_gc_partition_adapter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GC_PARTITION_ADAPTER.init(&sql_proxy_))) {
    LOG_WARN("gc partition adapter init failed", K(ret));
  } else {
    LOG_INFO("gc partition adapter init success");
  }
  return ret;
}

int ObServer::get_network_speed_from_sysfs(int64_t &network_speed)
{
  int ret = OB_SUCCESS;
  // sys_bkgd_net_percentage_ = config_.sys_bkgd_net_percentage;

  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(get_ethernet_speed(config_.devname.str(), network_speed))) {
    LOG_WARN("cannot get Ethernet speed, use default", K(tmp_ret), "devname", config_.devname.str());
  } else if (network_speed < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid Ethernet speed, use default", "devname", config_.devname.str());
  }

  return ret;
}

char *strtrim(char *str)
{
  char *ptr;

  if (str == NULL) {
    return NULL;
  }

  ptr = str + strlen(str) - 1;
  while (isspace(*str)) {
    str++;
  }

  while ((ptr > str) && isspace(*ptr)) {
    *ptr-- = '\0';
  }

  return str;
}

static int64_t nic_rate_parse(const char *str, bool &valid)
{
  char *p_unit = nullptr;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = strtol(str, &p_unit, 0);
    p_unit = strtrim(p_unit);

    if (OB_ISNULL(p_unit)) {
      valid = false;
    } else if (value <= 0) {
      valid = false;
    } else if (0 == STRCASECMP("bit", p_unit) || 0 == STRCASECMP("b", p_unit)) {
      // do nothing
    } else if (0 == STRCASECMP("kbit", p_unit) || 0 == STRCASECMP("kb", p_unit) || 0 == STRCASECMP("k", p_unit)) {
      value <<= 10;
    } else if ('\0' == *p_unit || 0 == STRCASECMP("mbit", p_unit) || 0 == STRCASECMP("mb", p_unit) ||
               0 == STRCASECMP("m", p_unit)) {
      // default is meta bit
      value <<= 20;
    } else if (0 == STRCASECMP("gbit", p_unit) || 0 == STRCASECMP("gb", p_unit) || 0 == STRCASECMP("g", p_unit)) {
      value <<= 30;
    } else {
      valid = false;
      LOG_ERROR("parse nic rate error", K(str), K(p_unit));
    }
  }
  return value;
}

int ObServer::get_network_speed_from_config_file(int64_t &network_speed)
{
  int ret = OB_SUCCESS;
  const char *nic_rate_path = "etc/nic.rate.config";
  const int64_t MAX_NIC_CONFIG_FILE_SIZE = 1 << 10;  // 1KB
  FILE *fp = nullptr;
  char *buf = nullptr;
  static int nic_rate_file_exist = 1;

  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(MAX_NIC_CONFIG_FILE_SIZE + 1, ObModIds::OB_BUFFER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buffer failed", LITERAL_K(MAX_NIC_CONFIG_FILE_SIZE), K(ret));
  } else if (OB_ISNULL(fp = fopen(nic_rate_path, "r"))) {
    if (ENOENT == errno) {
      ret = OB_FILE_NOT_EXIST;
      if (nic_rate_file_exist) {
        LOG_WARN("NIC Config file doesn't exist, auto detecting", K(nic_rate_path), K(ret));
        nic_rate_file_exist = 0;
      }
    } else {
      ret = OB_IO_ERROR;
      if (EAGAIN == errno) {
        LOG_WARN("Can't open NIC Config file", K(nic_rate_path), K(errno), K(ret));
      } else {
        LOG_ERROR("Can't open NIC Config file", K(nic_rate_path), K(errno), K(ret));
      }
    }
  } else {
    if (!nic_rate_file_exist) {
      LOG_INFO("Reading NIC Config file", K(nic_rate_path));
      nic_rate_file_exist = 1;
    }
    memset(buf, 0, MAX_NIC_CONFIG_FILE_SIZE + 1);
    fread(buf, 1, MAX_NIC_CONFIG_FILE_SIZE, fp);
    char *prate = nullptr;

    if (OB_UNLIKELY(0 != ferror(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Read NIC Config file error", K(nic_rate_path), K(ret));
    } else if (OB_UNLIKELY(0 == feof(fp))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("NIC Config file is too long", K(nic_rate_path), K(ret));
    } else {
      prate = strchr(buf, '=');
      if (nullptr != prate) {
        prate++;
        bool valid = false;
        int64_t nic_rate = nic_rate_parse(prate, valid);
        if (valid) {
          network_speed = nic_rate / 8;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid NIC Rate Config", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid NIC Config file", K(ret));
      }
    }  // else

    if (OB_UNLIKELY(0 != fclose(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Close NIC Config file failed", K(ret));
    }
  }  // else
  if (OB_LIKELY(nullptr != buf)) {
    ob_free(buf);
    buf = nullptr;
  }
  return ret;
}

int ObServer::init_bandwidth_throttle()
{
  int ret = OB_SUCCESS;
  int64_t network_speed = 0;

  if (OB_SUCC(get_network_speed_from_config_file(network_speed))) {
    LOG_DEBUG("got network speed from config file", K(network_speed));
  } else if (OB_SUCC(get_network_speed_from_sysfs(network_speed))) {
    LOG_DEBUG("got network speed from sysfs", K(network_speed));
  } else {
    network_speed = DEFAULT_ETHERNET_SPEED;
    LOG_DEBUG("using default network speed", K(network_speed));
  }

  sys_bkgd_net_percentage_ = config_.sys_bkgd_net_percentage;
  if (network_speed > 0) {
    int64_t rate = network_speed * sys_bkgd_net_percentage_ / 100;
    if (OB_FAIL(bandwidth_throttle_.init(rate))) {
      LOG_WARN("failed to init bandwidth throttle", K(ret), K(rate), K(network_speed));
    } else {
      LOG_INFO("succeed to init_bandwidth_throttle", K(sys_bkgd_net_percentage_), K(network_speed), K(rate));
      ethernet_speed_ = network_speed;
    }
  }
  return ret;
}

int ObServer::reload_config()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(reload_bandwidth_throttle_limit(ethernet_speed_))) {
    LOG_WARN("failed to reload_bandwidth_throttle_limit", K(ret));
  }

  return ret;
}

int ObServer::reload_bandwidth_throttle_limit(int64_t network_speed)
{
  int ret = OB_SUCCESS;
  const int64_t sys_bkgd_net_percentage = config_.sys_bkgd_net_percentage;

  if ((sys_bkgd_net_percentage_ != sys_bkgd_net_percentage) || (ethernet_speed_ != network_speed)) {
    if (network_speed <= 0) {
      LOG_WARN("wrong network speed.", K(ethernet_speed_));
      network_speed = DEFAULT_ETHERNET_SPEED;
    }

    int64_t rate = network_speed * sys_bkgd_net_percentage / 100;
    if (OB_FAIL(bandwidth_throttle_.set_rate(rate))) {
      LOG_WARN("failed to reset bandwidth throttle", K(ret), K(rate), K(ethernet_speed_));
    } else {
      LOG_INFO("succeed to reload_bandwidth_throttle_limit",
          "old_percentage",
          sys_bkgd_net_percentage_,
          "new_percentage",
          sys_bkgd_net_percentage,
          K(network_speed),
          K(rate));
      sys_bkgd_net_percentage_ = sys_bkgd_net_percentage;
      ethernet_speed_ = network_speed;
    }
  }
  return ret;
}

int ObServer::check_server_can_start_service()
{
  int ret = OB_SUCCESS;
  int64_t min_wrs = INT64_MAX;
  // On the standby database, it is very likely that the minimum standby machine-readable timestamp cannot be pushed
  // because the main database does not exist. Do not stop the server from starting, otherwise you may not be able to
  // create a connection
  int64_t get_min_wrs_ts = ObTimeUtility::current_time();
  do {
    bool can_start_service = true;
    if (stop_) {
      ret = OB_SERVER_IS_STOPPING;
    } else {
      // Check whether the lagging amount of all partitions of the machine is greater than
      // max_stale_time_for_weak_consistency. If true, it cannot be restarted
      int64_t tmp_min_wrs = INT64_MAX;
      weak_read_service_.check_server_can_start_service(can_start_service, tmp_min_wrs);
      if (!can_start_service) {
        const int64_t STANDBY_WAIT_WRS_DURATION = 60 * 1000 * 1000;
        const int64_t current_time = ObTimeUtility::current_time();
        if (min_wrs != tmp_min_wrs) {
          get_min_wrs_ts = current_time;
          min_wrs = tmp_min_wrs;
        }
        ret = OB_EAGAIN;
        usleep(1000 * 1000);
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_INFO("[NOTICE] clog is behind, service starting need to wait !");
        }
      } else {
        ret = OB_SUCCESS;
      }
    }
  } while (OB_EAGAIN == ret);

  return ret;
}

storage::ObPartitionService &ObServer::get_partition_service()
{
  return ObPartitionService::get_instance();
}

ObServer::ObCTASCleanUpTask::ObCTASCleanUpTask() : obs_(nullptr), is_inited_(false)
{}

int ObServer::ObCTASCleanUpTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCTASCleanUpTask has already been inited", K(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObCTASCleanUpTask init with null ptr", K(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    disable_timeout_check();
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, CLEANUP_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule task ObCTASCleanUpTask", K(ret));
    }
  }
  return ret;
}

void ObServer::ObCTASCleanUpTask::destroy()
{
  is_inited_ = false;
  obs_ = nullptr;
}

void ObServer::ObCTASCleanUpTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_ctas_cleanup = ATOMIC_BCAS(&obs_->need_ctas_cleanup_, true, false);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCTASCleanUpTask has not been inited", K(ret));
  } else if (false == need_ctas_cleanup) {
    LOG_DEBUG("CTAS cleanup task skipped this time");
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("CTAS cleanup task got null ptr", K(ret));
  } else if (OB_FAIL(obs_->clean_up_invalid_tables())) {
    LOG_WARN("CTAS clean up task failed", K(ret));
    ATOMIC_STORE(&obs_->need_ctas_cleanup_, true);
  } else {
    LOG_DEBUG("CTAS clean up task succeed");
  }
}

// Traverse the current session and determine whether the given table schema needs to be deleted according to the
// session id and last active time
bool ObServer::ObCTASCleanUp::operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if ((ObCTASCleanUp::TEMP_TAB_PROXY_RULE == get_cleanup_type() && get_drop_flag()) ||
      (ObCTASCleanUp::TEMP_TAB_PROXY_RULE != get_cleanup_type() && false == get_drop_flag())) {
    // do nothing
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (static_cast<uint64_t>(key.sessid_) == get_session_id() || key.proxy_sessid_ == get_session_id()) {
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", K(ret));
      } else {
        ret = OB_SUCCESS;
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true);  // 1, The current session is in use, there is suspicion, need to
                                                        // continue to check in the next scheduling
        LOG_DEBUG(
            "try lock query fail with code OB_EGAIN", K(sess_info->get_sessid()), K(sess_info->get_sessid_for_table()));
      }
      set_drop_flag(false);
    } else if (ObCTASCleanUp::CTAS_RULE == get_cleanup_type()) {  // 2, Query build table cleanup
      if (sess_info->get_last_active_time() <
          get_schema_version() + 100) {  // The reason for +100 is to allow a certain error in the time stamp comparison
        (void)sess_info->unlock_query();
        set_drop_flag(false);
        ATOMIC_STORE(&obs_->need_ctas_cleanup_,
            true);  // The current session is creating a table and needs to continue to check in the next schedule
        LOG_INFO("current table is in status of creating", K(sess_info->get_last_active_time()));
      } else {
        (void)sess_info->unlock_query();
        LOG_INFO("current table was in status of creating", K(sess_info->get_last_active_time()));
      }
    } else if (ObCTASCleanUp::TEMP_TAB_RULE == get_cleanup_type()) {  // 3, Directly connected temporary table cleanup
      if (sess_info->get_sess_create_time() < get_schema_version() + 100) {
        (void)sess_info->unlock_query();
        set_drop_flag(false);
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true);  // The session that created the temporary table is still alive
                                                        // and needs to be checked in the next schedule
        LOG_DEBUG("session that creates temporary table is still alive");
      } else {
        (void)sess_info->unlock_query();
        LOG_DEBUG(
            "current session reusing session id that created temporary table", K(sess_info->get_sess_create_time()));
      }
    } else {  // 4. Proxy temporary table cleanup
      if (sess_info->get_sess_create_time() < get_schema_version() + 100) {
        (void)sess_info->unlock_query();
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true);  // The session that created the temporary table is still alive
                                                        // and needs to be checked in the next schedule
        LOG_DEBUG("session that creates temporary table is still alive");
      } else {
        set_drop_flag(true);
        (void)sess_info->unlock_query();
        LOG_DEBUG(
            "current session reusing session id that created temporary table", K(sess_info->get_sess_create_time()));
      }
    }
  }
  return OB_SUCCESS == ret;
}

// Traverse the current session, if the session has updated sess_active_time recently, execute alter system refresh
// tables in session xxx Synchronously update the last active time of all temporary tables under the current session
bool ObServer::ObRefreshTime::operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(sess_info->try_lock_query())) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to try lock query", K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_WARN(
          "try lock query fail with code OB_EGAIN", K(sess_info->get_sessid()), K(sess_info->get_sessid_for_table()));
    }
  } else {
    sess_info->refresh_temp_tables_sess_active_time();
    (void)sess_info->unlock_query();
  }
  return OB_SUCCESS == ret;
}

ObServer::ObRefreshTimeTask::ObRefreshTimeTask() : obs_(nullptr), is_inited_(false)
{}

int ObServer::ObRefreshTimeTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRefreshTimeTask has already been inited", K(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObRefreshTimeTask init with null ptr", K(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule task ObRefreshTimeTask", K(ret));
    }
  }
  return ret;
}

void ObServer::ObRefreshTimeTask::destroy()
{
  is_inited_ = false;
  obs_ = nullptr;
}

void ObServer::ObRefreshTimeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRefreshTimeTask has not been inited", K(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObRefreshTimeTask cleanup task got null ptr", K(ret));
  } else if (OB_FAIL(obs_->refresh_temp_table_sess_active_time())) {
    LOG_WARN("ObRefreshTimeTask clean up task failed", K(ret));
  }

  LOG_WARN("LICQ, ObRefreshTimeTask::runTimerTask", K(ret));
}

int ObServer::refresh_temp_table_sess_active_time()
{
  int ret = OB_SUCCESS;
  ObRefreshTime refesh_time(this);
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session mgr is null", K(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(refesh_time))) {
    LOG_WARN("failed to traverse each session to check table need be dropped", K(ret));
  }
  return ret;
}

ObServer::ObRefreshNetworkSpeedTask::ObRefreshNetworkSpeedTask() : obs_(nullptr), is_inited_(false)
{}

int ObServer::ObRefreshNetworkSpeedTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRefreshNetworkSpeedTask has already been inited", K(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObRefreshNetworkSpeedTask init with null ptr", K(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule task ObRefreshNetworkSpeedTask", K(ret));
    }
  }
  return ret;
}

void ObServer::ObRefreshNetworkSpeedTask::destroy()
{
  is_inited_ = false;
  obs_ = nullptr;
}

void ObServer::ObRefreshNetworkSpeedTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRefreshNetworkSpeedTask has not been inited", K(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObRefreshNetworkSpeedTask cleanup task got null ptr", K(ret));
  } else if (OB_FAIL(obs_->refresh_network_speed())) {
    LOG_WARN("ObRefreshNetworkSpeedTask reload bandwidth throttle limit failed", K(ret));
  }
}

int ObServer::refresh_network_speed()
{
  int ret = OB_SUCCESS;
  int64_t network_speed = 0;

  if (OB_SUCC(get_network_speed_from_config_file(network_speed))) {
    LOG_DEBUG("got network speed from config file", K(network_speed));
  } else if (OB_SUCC(get_network_speed_from_sysfs(network_speed))) {
    LOG_DEBUG("got network speed from sysfs", K(network_speed));
  } else {
    network_speed = DEFAULT_ETHERNET_SPEED;
    LOG_DEBUG("using default network speed", K(network_speed));
  }

  if ((network_speed > 0) && (network_speed != ethernet_speed_)) {
    LOG_INFO("network speed changed", "from", ethernet_speed_, "to", network_speed);
    if (OB_FAIL(reload_bandwidth_throttle_limit(network_speed))) {
      LOG_WARN("ObRefreshNetworkSpeedTask reload bandwidth throttle limit failed", K(ret));
    }
  }

  return ret;
}

int ObServer::init_refresh_active_time_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_active_time_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("fail to init refresh active time task", K(ret));
  }
  return ret;
}

int ObServer::init_ctas_clean_up_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctas_clean_up_task_.init(this, lib::TGDefIDs::CTASCleanUpTimer))) {
    LOG_WARN("fail to init ctas clean up task", K(ret));
  }
  return ret;
}

int ObServer::init_refresh_network_speed_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_network_speed_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("fail to init refresh network speed task", K(ret));
  }
  return ret;
}

// @@Query cleanup rules for built tables and temporary tables:
// 1, Traverse all table_schema, if the session_id of table T <> 0 means that the table is being created or the previous
// creation failed or the temporary table is to be cleared, then enter 2#; 2, Create a table for the query: traverse the
// session, and determine whether T should be DROP according to the session_id and time of the session and table T; 2.1,
// there is session->id = T->session_id,
//     a), the last active time of the session <the creation time of T, the table T is in the process of being created
//     and cannot be DROP; b), the last active time of the session >= the creation time of T, sess_id is reused, the
//     ession of the original table T has been disconnected, and you can DROP;
// 2.2, there is no session, its id = T->session_id, T can be DROP;
// 3. For temporary tables: distinguish between direct connection creation and ob proxy creation;
// 3.1 Direct connection mode, the judgment deletion condition is the same as 2#, the difference is that the last active
// time of the session needs to be replaced with the session creation time; 3.2 ob proxy mode, a), the interval between
// the current time and the sess_active_time of the table schema exceeds the maximum timeout of the session, and DROP is
// required;
//                 b), when a# is not met, all sessions need to be traversed to determine whether there is a session
//                 with the same id s1, s1->sess creation time> T creation time (same as rule 2.1#),
//                     When s1 exists, it is considered that session id reuse has occurred, and T still needs to be
//                     DROP;
// It has been optimized before calling this interface, only need_ctas_cleanup_=true will be here
// The cleanup of the oracle temporary table is performed in the dml resolve phase of the temporary table for the first
// time after the session is created for performance reasons, to avoid frequent delete operations in the background
int ObServer::clean_up_invalid_tables()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = NULL;
  ObSEArray<const ObTableSchema *, 512> table_schemas;
  const int64_t CONNECT_TIMEOUT_VALUE = 50L * 60L * 60L * 1000L * 1000L;  // default value is 50hrs
  obrpc::ObDropTableArg drop_table_arg;
  obrpc::ObTableItem table_item;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  char create_host_str[OB_MAX_HOST_NAME_LENGTH];
  if (OB_FAIL(schema_service_.get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schemas(table_schemas))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else {
    ObCTASCleanUp ctas_cleanup(this, true);
    drop_table_arg.if_exist_ = true;
    drop_table_arg.to_recyclebin_ = false;
    common_rpc_proxy = GCTX.rs_rpc_proxy_;
    MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH);
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(tmp_ret); i++) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got invalid schema", K(ret), K(i));
      } else if (0 == table_schema->get_session_id()) {
        // do nothing
      } else if (0 != table_schema->get_create_host_str().compare(create_host_str)) {
        LOG_DEBUG("current observer is not the one created the table, just skip",
            "current session",
            create_host_str,
            K(*table_schema));
      } else {
        LOG_DEBUG("table is creating or encountered error or is temporary one", K(*table_schema));
        if (table_schema->is_obproxy_create_tmp_tab()) {  // 1, Temporary tables, proxy table creation, cleanup rules
                                                          // see 3.2#
          LOG_DEBUG("clean_up_invalid_tables::ob proxy created",
              K(i),
              K(ObTimeUtility::current_time()),
              K(table_schema->get_sess_active_time()),
              K(CONNECT_TIMEOUT_VALUE));
          if (ObTimeUtility::current_time() - table_schema->get_sess_active_time() > CONNECT_TIMEOUT_VALUE) {
            ctas_cleanup.set_drop_flag(true);
          } else {
            ctas_cleanup.set_drop_flag(false);
          }
          ctas_cleanup.set_cleanup_type(ObCTASCleanUp::TEMP_TAB_PROXY_RULE);
        } else {
          ctas_cleanup.set_drop_flag(false);
          if (table_schema->is_tmp_table()) {  // 2, Temporary tables, directly connected tables, cleanup rules
                                               // see 3.1~3.2#
            ctas_cleanup.set_cleanup_type(ObCTASCleanUp::TEMP_TAB_RULE);
          } else {  // 3, Query and build tables, see 2# for cleaning rules
            ctas_cleanup.set_cleanup_type(ObCTASCleanUp::CTAS_RULE);
          }
        }
        if (false == ctas_cleanup.get_drop_flag()) {
          ctas_cleanup.set_session_id(table_schema->get_session_id());
          ctas_cleanup.set_schema_version(table_schema->get_schema_version());
          if (ObCTASCleanUp::TEMP_TAB_PROXY_RULE ==
              ctas_cleanup.get_cleanup_type()) {  // The proxy connection method is not deleted by default, and it may
                                                  // need to be dropped when the reused session is found.
            ctas_cleanup.set_drop_flag(false);
          } else {
            ctas_cleanup.set_drop_flag(true);
          }
          if (OB_ISNULL(GCTX.session_mgr_)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session mgr is null", K(ret));
          } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(ctas_cleanup))) {
            LOG_WARN("failed to traverse each session to check table need be dropped", K(ret), K(*table_schema));
          }
        }
        if (ctas_cleanup.get_drop_flag()) {
          LOG_INFO("a table will be dropped!", K(*table_schema));
          database_schema = NULL;
          drop_table_arg.tables_.reset();
          drop_table_arg.if_exist_ = true;
          drop_table_arg.tenant_id_ = table_schema->get_tenant_id();
          drop_table_arg.exec_tenant_id_ = table_schema->get_tenant_id();
          drop_table_arg.table_type_ = table_schema->get_table_type();
          drop_table_arg.session_id_ = table_schema->get_session_id();
          drop_table_arg.to_recyclebin_ = false;
          table_item.table_name_ = table_schema->get_table_name_str();
          table_item.mode_ = table_schema->get_name_case_mode();
          if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
            LOG_WARN("failed to get database schema", K(ret));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database schema is null", K(ret));
          } else if (database_schema->is_in_recyclebin() || table_schema->is_in_recyclebin()) {
            LOG_DEBUG("skip table schema in recyclebin", K(*table_schema));
          } else if (FALSE_IT(table_item.database_name_ = database_schema->get_database_name_str())) {
            // impossible
          } else if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
            LOG_WARN("failed to add table item!", K(table_item), K(ret));
          } else if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg))) {
            LOG_WARN("failed to drop table", K(drop_table_arg), K(table_item), K(ret));
          } else {
            LOG_INFO("a table is dropped due to previous error or is a temporary one",
                K(i),
                "table_name",
                table_item.table_name_);
          }
        } else {
          LOG_DEBUG("no need to drop table", K(i));
        }
      }
    }
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
