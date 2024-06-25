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

#include "observer/ob_server.h"
#include <sys/statvfs.h>
#include "common/log/ob_log_constants.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "lib/alloc/memory_dump.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/file/file_directory_utils.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/lock/ob_latch.h"
#include "lib/net/ob_net_util.h"
#include "lib/oblog/ob_base_log_buffer.h"
#include "lib/oblog/ob_log_compressor.h"
#include "lib/ob_running_mode.h"
#include "lib/profile/ob_active_resource_list.h"
#include "lib/profile/ob_profile_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/restore/ob_storage_oss_base.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/string/ob_sql_string.h"
#include "lib/task/ob_timer_monitor.h"
#include "lib/thread/thread_mgr.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/compress/zlib_lite/ob_zlib_lite_compressor.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_rpc_extra_payload.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/omt/ob_tenant_srs.h"
#include "observer/table/ob_table_rpc_processor.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/cache/ob_cache_name_define.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "share/ob_bg_thread_monitor.h"
#include "share/ob_cluster_version.h"
#include "share/ob_define.h"
#include "share/ob_force_print_log.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_global_autoinc_service.h"
#include "share/ob_io_device_helper.h"
#include "share/io/ob_io_calibration.h"
#include "share/ob_task_define.h"
#include "share/ob_tenant_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/sequence/ob_sequence_cache.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "sql/engine/px/ob_px_worker.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/ob_sql_init.h"
#include "sql/ob_sql_task.h"
#include "storage/ob_i_store.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#include "storage/ob_file_system_router.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "common/log/ob_log_constants.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "sql/engine/px/ob_px_target_mgr.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "share/ob_device_manager.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/ob_tenant_mem_limit_getter.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_tenant_memory_printer.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/ob_file_system_router.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/tablelock/ob_table_lock_rpc_client.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_leak_checker.h"
#include "share/ash/ob_active_sess_hist_task.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "share/ob_server_blacklist.h"
#include "share/ob_primary_standby_service.h" // ObPrimaryStandbyService
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/longops_mgr/ob_longops_mgr.h"
#include "logservice/palf/election/interface/election.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/fts/ob_fts_plugin_mgr.h"
#include "observer/ob_server_utils.h"
#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/virtual_table/ob_mds_event_buffer.h"
#include "observer/ob_startup_accel_task_handler.h"
#include "share/detect/ob_detect_manager.h"
#include "observer/table/ttl/ob_table_ttl_task.h"
#include "storage/high_availability/ob_storage_ha_diagnose_service.h"
#include "logservice/palf/log_cache.h"
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/palf_env_lite_mgr.h"
#include "logservice/arbserver/ob_arb_srv_network_frame.h"
#include "logservice/arbserver/ob_arb_cluster_white_list.h"
#include "logservice/arbserver/ob_arb_server_config.h"
#endif
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "lib/xml/ob_libxml2_sax_handler.h"
#include "ob_check_params.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::logservice;

extern "C" void ussl_stop();
extern "C" void ussl_wait();

namespace oceanbase
{
namespace obrpc
{
void keepalive_init_data(ObNetKeepAliveData &ka_data)
{
  ka_data.rs_server_status_ = RSS_IS_WORKING;
  ka_data.start_service_time_ = 0;
}

void keepalive_make_data(ObNetKeepAliveData &ka_data)
{
  ka_data.rs_server_status_ = GCTX.rs_server_status_;
  ka_data.start_service_time_ = GCTX.start_service_time_;
}
}
}

namespace oceanbase
{
namespace observer
{
ObServer::ObServer()
  : need_ctas_cleanup_(true), sig_worker_(new (sig_buf_) ObSignalWorker()),
    signal_handle_(new (sig_worker_ + 1) ObSignalHandle()),
    prepare_stop_(true), stop_(true), has_stopped_(true), has_destroy_(false),
    net_frame_(gctx_), sql_conn_pool_(), ddl_conn_pool_(), dblink_conn_pool_(),
    res_inner_conn_pool_(), restore_ctx_(), srv_rpc_proxy_(),
    storage_rpc_proxy_(), rs_rpc_proxy_(), sql_proxy_(),
    dblink_proxy_(),
    executor_proxy_(), executor_rpc_(), dbms_job_rpc_proxy_(), dbms_sched_job_rpc_proxy_(), interrupt_proxy_(),
    config_(ObServerConfig::get_instance()),
    reload_config_(config_, gctx_), config_mgr_(config_, reload_config_),
    tenant_config_mgr_(omt::ObTenantConfigMgr::get_instance()),
    tenant_timezone_mgr_(omt::ObTenantTimezoneMgr::get_instance()),
    schema_service_(share::schema::ObMultiVersionSchemaService::get_instance()),
    lst_operator_(), tablet_operator_(),
    server_tracer_(),
    location_service_(),
    bandwidth_throttle_(),
    sys_bkgd_net_percentage_(0),
    ethernet_speed_(0),
    cpu_frequency_(DEFAULT_CPU_FREQUENCY),
    session_mgr_(),
    root_service_monitor_(root_service_, rs_mgr_),
    ob_service_(gctx_),
    multi_tenant_(), vt_data_service_(root_service_, self_addr_, &config_),
    weak_read_service_(),
    bl_service_(ObBLService::get_instance()),
    table_service_(),
    cgroup_ctrl_(),
    start_time_(ObTimeUtility::current_time()),
    warm_up_start_time_(0),
    diag_(),
    scramble_rand_(),
    duty_task_(),
    sql_mem_task_(),
    ctas_clean_up_task_(),
    refresh_active_time_task_(),
    refresh_network_speed_task_(),
    refresh_cpu_frequency_task_(),
    refresh_io_calibration_task_(),
    schema_status_proxy_(sql_proxy_),
    is_log_dir_empty_(false),
    conn_res_mgr_(),
    unix_domain_listener_(),
    disk_usage_report_task_(),
    log_block_mgr_()
#ifdef OB_BUILD_ARBITRATION
    ,arb_gcs_(),
    arb_timer_()
#endif
    ,wr_service_()
{
  memset(&gctx_, 0, sizeof (gctx_));
}

ObServer::~ObServer()
{
  destroy();
}

int ObServer::parse_mode()
{
  int ret = OB_SUCCESS;
  const char *mode_str = GCONF.ob_startup_mode;

  if (mode_str && strlen(mode_str) > 0) {
    if (0 == STRCASECMP(mode_str, NORMAL_MODE_STR)) {
      gctx_.startup_mode_ = NORMAL_MODE;
      LOG_INFO("set normal mode");
#ifdef OB_BUILD_ARBITRATION
    } else if (0 == STRCASECMP(mode_str, ARBITRATION_MODE_STR)) {
      gctx_.startup_mode_ = ARBITRATION_MODE;
      LOG_INFO("set arbitration mode");
#endif
    } else if (0 == STRCASECMP(mode_str, FLASHBACK_MODE_STR)) {
      gctx_.startup_mode_ = PHY_FLASHBACK_MODE;
      LOG_INFO("set physical_flashback mode");
    } else if (0 == STRCASECMP(mode_str, FLASHBACK_VERIFY_MODE_STR)) {
      gctx_.startup_mode_ = PHY_FLASHBACK_VERIFY_MODE;
      LOG_INFO("set physical_flashback_verify mode");
    } else if (0 == STRCASECMP(mode_str, DISABLED_CLUSTER_MODE_STR)) {
      gctx_.startup_mode_ = DISABLED_CLUSTER_MODE;
      LOG_INFO("set disabled_cluster mode");
    } else if (0 == STRCASECMP(mode_str, DISABLED_WITH_READONLY_CLUSTER_MODE_STR)) {
      gctx_.startup_mode_ = DISABLED_WITH_READONLY_CLUSTER_MODE;
      LOG_INFO("set disabled_with_readonly_cluster mode");
    } else {
      LOG_INFO("invalid mode value", "startup_mode_", mode_str);
    }
  } else {
    gctx_.startup_mode_ = NORMAL_MODE;
    LOG_INFO("set normal mode");
  }
  return ret;
}

int ObServer::init(const ObServerOptions &opts, const ObPLogWriterCfg &log_cfg)
{
  FLOG_INFO("[OBSERVER_NOTICE] start to init observer");
  DBA_STEP_RESET(server_start);
  int ret = OB_SUCCESS;
  opts_ = opts;
  scramble_rand_.init(static_cast<uint64_t>(start_time_), static_cast<uint64_t>(start_time_ / 2));

  // server parameters be inited here.
  if (OB_FAIL(init_config())) {
    LOG_ERROR("init config failed", KR(ret));
  }
  // set alert log level earlier
  OB_LOGGER.set_alert_log_level(config_.alert_log_level);
  LOG_DBA_INFO_V2(OB_SERVER_INIT_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "observer init begin.");

  //check os params
  if (OB_SUCC(ret) && OB_FAIL(check_os_params(GCONF.strict_check_os_params))) {
    LOG_ERROR("check OS params failed", K(GCONF.strict_check_os_params));
  }
  // set large page param
  ObLargePageHelper::set_param(config_.use_large_pages);

  if (is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    FLOG_INFO("begin init observer in arbitration mode", KR(ret));
    if (FAILEDx(OB_LOGGER.init(log_cfg, true))) {
      LOG_ERROR("async log init error.", KR(ret));
      ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    } else if (OB_FAIL(OB_LOG_COMPRESSOR.init())) {
      LOG_ERROR("log compressor init error.", KR(ret));
    } else if (OB_FAIL(OB_LOGGER.set_log_compressor(&OB_LOG_COMPRESSOR))) {
      LOG_ERROR("set log compressor error.", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_pre_setting())) {
      LOG_ERROR("init pre setting failed", KR(ret));
    } else if (OB_FAIL(ObClockGenerator::init())) {
      LOG_ERROR("init create clock generator failed", KR(ret));
    } else if (OB_FAIL(init_version())) {
      LOG_ERROR("init version failed", KR(ret));
    } else if (OB_FAIL(init_server_in_arb_mode())) {
      LOG_ERROR("init_server_in_arb_mode failed", KR(ret));
    }
    lib::g_runtime_enabled = true;
    FLOG_INFO("end init observer in arbitration mode", KR(ret));
#endif
  } else {
    if (FAILEDx(OB_LOGGER.init(log_cfg, false))) {
      LOG_ERROR("async log init error.", KR(ret));
      ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    } else if (OB_FAIL(OB_LOG_COMPRESSOR.init())) {
      LOG_ERROR("log compressor init error.", KR(ret));
    } else if (OB_FAIL(OB_LOGGER.set_log_compressor(&OB_LOG_COMPRESSOR))) {
      LOG_ERROR("set log compressor error.", KR(ret));
    } else if (OB_FAIL(init_tz_info_mgr())) {
      LOG_ERROR("init tz_info_mgr failed", KR(ret));
    } else if (OB_FAIL(ObSqlTaskFactory::get_instance().init())) {
      LOG_ERROR("init sql task factory failed", KR(ret));
    } else if (OB_FAIL(ObTabletHandleIndexMap::get_instance()->init())) {
      LOG_ERROR("init leak checker hash map and qsync lock failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      ::oceanbase::sql::init_sql_factories();
      ::oceanbase::sql::init_sql_executor_singletons();
      ::oceanbase::sql::init_sql_expr_static_var();

      if (OB_FAIL(ObPreProcessSysVars::init_sys_var())) {
        LOG_ERROR("init PreProcessing system variable failed !", KR(ret));
      } else if (OB_FAIL(ObBasicSessionInfo::init_sys_vars_cache_base_values())) {
        LOG_ERROR("init session base values failed", KR(ret));
      }
    }

    if (FAILEDx(ObQueryRetryCtrl::init())) {
      LOG_ERROR("init retry ctrl failed", KR(ret));
    } else if (OB_FAIL(ObMdsEventBuffer::init())) {
      LOG_WARN("init MDS event buffer failed", KR(ret));
    } else if (OB_FAIL(ObTableApiProcessorBase::init_session())) {
      LOG_ERROR("init static session failed", KR(ret));
    } else if (OB_FAIL(init_loaddata_global_stat())) {
      LOG_ERROR("init global load data stat map failed", KR(ret));
    } else if (OB_FAIL(init_pre_setting())) {
      LOG_ERROR("init pre setting failed", KR(ret));
    } else if (OB_FAIL(init_global_context())) {
      LOG_ERROR("init global context failed", KR(ret));
    } else if (OB_FAIL(init_version())) {
      LOG_ERROR("init version failed", KR(ret));
    } else if (OB_FAIL(init_sql_proxy())) {
      LOG_ERROR("init sql connection pool failed", KR(ret));
    } else if (OB_FAIL(init_io())) {
      LOG_ERROR("init io failed", KR(ret));
    } else if (FALSE_IT(cgroup_ctrl_.init())) {
      LOG_ERROR("should never reach here!", KR(ret));
    } else if (OB_FAIL(init_restore_ctx())) {
      LOG_ERROR("init restore context failed", KR(ret));
    #ifndef OB_USE_ASAN
    } else if (OB_FAIL(ObMemoryDump::get_instance().init())) {
      LOG_ERROR("init memory dumper failed", KR(ret));
    #endif
    } else if (OB_FAIL(init_global_kvcache())) {
      LOG_ERROR("init global kvcache failed", KR(ret));
    } else if (OB_FAIL(schema_status_proxy_.init())) {
      LOG_ERROR("fail to init schema status proxy", KR(ret));
    } else if (OB_FAIL(init_schema())) {
      LOG_ERROR("init schema failed", KR(ret));
    } else if (OB_FAIL(init_network())) {
      LOG_ERROR("init network failed", KR(ret));
    } else if (OB_FAIL(init_interrupt())) {
      LOG_ERROR("init interrupt failed", KR(ret));
    } else if (OB_FAIL(init_zlib_lite_compressor())) {
      LOG_ERROR("init zlib lite compressor failed", KR(ret));
    } else if (OB_FAIL(rs_mgr_.init(&srv_rpc_proxy_, &config_, &sql_proxy_))) {
      LOG_ERROR("init rs_mgr_ failed", KR(ret));
    } else if (OB_FAIL(server_tracer_.init(rs_rpc_proxy_, sql_proxy_))) {
      LOG_ERROR("init server tracer failed", KR(ret));
    } else if (OB_FAIL(init_ob_service())) {
      LOG_ERROR("init ob service failed", KR(ret));
    } else if (OB_FAIL(init_root_service())) {
      LOG_ERROR("init root service failed", KR(ret));
    } else if (OB_FAIL(root_service_monitor_.init())) {
      LOG_ERROR("init root service monitor failed", KR(ret));
    } else if (OB_FAIL(init_sql())) {
      LOG_ERROR("init sql failed", KR(ret));
    } else if (OB_FAIL(init_sql_runner())) {
      LOG_ERROR("init sql failed", KR(ret));
    } else if (OB_FAIL(init_sequence())) {
      LOG_ERROR("init sequence failed", KR(ret));
    } else if (OB_FAIL(init_pl())) {
      LOG_ERROR("init pl failed", K(ret));
    } else if (OB_FAIL(lst_operator_.init(sql_proxy_, &config_))) {
      LOG_ERROR("ls table operator init failed", KR(ret));
    } else if (OB_FAIL(tablet_operator_.init(sql_proxy_))) {
      LOG_ERROR("tablet table operator init failed", KR(ret));
    } else if (OB_FAIL(location_service_.init(lst_operator_,
                                              schema_service_,
                                              sql_proxy_,
                                              server_tracer_,
                                              rs_mgr_,
                                              rs_rpc_proxy_,
                                              srv_rpc_proxy_))) {
      LOG_ERROR("init location service failed", KR(ret));
    } else if (OB_FAIL(init_autoincrement_service())) {
      LOG_ERROR("init auto-increment service failed", KR(ret));
    } else if (OB_FAIL(init_table_lock_rpc_client())) {
      LOG_ERROR("init table_lock_rpc_client failed", KR(ret));
    } else if (OB_FAIL(init_tablet_autoincrement_service())) {
      LOG_ERROR("init auto-increment service failed", KR(ret));
    } else if (OB_FAIL(init_bandwidth_throttle())) {
      LOG_ERROR("init bandwidth_throttle failed", KR(ret));
    } else if (OB_FAIL(ObClockGenerator::init())) {
      LOG_ERROR("init create clock generator failed", KR(ret));
    } else if (OB_FAIL(ObTenantFTPluginMgr::register_plugins())) {
      LOG_ERROR("init fulltext plugins failed", K(ret));
    } else if (OB_FAIL(init_storage())) {
      LOG_ERROR("init storage failed", KR(ret));
    } else if (OB_FAIL(init_tx_data_cache())) {
      LOG_ERROR("init tx data cache failed", KR(ret));
    } else if (OB_FAIL(init_log_kv_cache())) {
      LOG_ERROR("init log kv cache failed", KR(ret));
    } else if (OB_FAIL(locality_manager_.init(self_addr_,
                                              &sql_proxy_))) {
      LOG_ERROR("init locality manager failed", KR(ret));
    } else if (OB_FAIL(init_ts_mgr())) {
      LOG_ERROR("init ts mgr failed", KR(ret));
    } else if (OB_FAIL(weak_read_service_.init(net_frame_.get_req_transport()))) {
      LOG_ERROR("init weak_read_service failed", KR(ret));
    } else if (OB_FAIL(bl_service_.init())) {
      LOG_ERROR("init bl_service_ failed", KR(ret));
    } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
      LOG_ERROR("init device manager failed", KR(ret));
    } else if (OB_FAIL(ObTenantMutilAllocatorMgr::get_instance().init())) {
      LOG_ERROR("init ObTenantMutilAllocatorMgr failed", KR(ret));
    } else if (OB_FAIL(ObExternalTableFileManager::get_instance().init())) {
      LOG_ERROR("init external table file manager failed", KR(ret));
    } else if (OB_FAIL(SLOGGERMGR.init(storage_env_.log_spec_.log_dir_,
        storage_env_.sstable_dir_, storage_env_.log_spec_.max_log_file_size_,
        storage_env_.slog_file_spec_))) {
      LOG_ERROR("init ObStorageLoggerManager failed", KR(ret));
    } else if (OB_FAIL(ObVirtualTenantManager::get_instance().init())) {
      LOG_ERROR("init tenant manager failed", KR(ret));
    } else if (OB_FAIL(startup_accel_handler_.init(SERVER_ACCEL))) {
      LOG_ERROR("init server startup task handler failed", KR(ret));
    } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().init())) {
      LOG_ERROR("init server checkpoint slog handler failed", KR(ret));
    } else if (OB_FAIL(common::occam::ObThreadHungDetector::get_instance().init())) {
      LOG_ERROR("init sObThreadHungDetector failed", KR(ret));
    } else if (OB_FAIL(palf::election::GLOBAL_INIT_ELECTION_MODULE())) {
      LOG_ERROR("init election module failed", KR(ret));
    } else if (OB_FAIL(init_multi_tenant())) {
      LOG_ERROR("init multi tenant failed", KR(ret));
    } else if (OB_FAIL(init_ctas_clean_up_task())) {
      LOG_ERROR("init ctas clean up task failed", KR(ret));
    } else if (OB_FAIL(init_ddl_heart_beat_task_container())) {
      LOG_ERROR("init ddl heart beat task container failed", KR(ret));
    } else if (OB_FAIL(init_redef_heart_beat_task())) {
      LOG_ERROR("init redef heart beat task failed", KR(ret));
    } else if (OB_FAIL(init_refresh_active_time_task())) {
      LOG_ERROR("init refresh active time task failed", KR(ret));
    } else if (OB_FAIL(init_refresh_network_speed_task())) {
      LOG_ERROR("init refresh network speed task failed", KR(ret));
    } else if (OB_FAIL(init_refresh_cpu_frequency())) {
      LOG_ERROR("init refresh cpu frequency failed", KR(ret));
    } else if (OB_FAIL(init_refresh_io_calibration())) {
      LOG_ERROR("init refresh io calibration failed", KR(ret));
    } else if (OB_FAIL(ObOptStatManager::get_instance().init(
                         &sql_proxy_, &config_))) {
      LOG_ERROR("init opt stat manager failed", KR(ret));
    } else if (OB_FAIL(lst_operator_.set_callback_for_obs(
                rs_rpc_proxy_, srv_rpc_proxy_, rs_mgr_, sql_proxy_))) {
      LOG_ERROR("set_use_rpc_table failed", KR(ret));
    } else if (OB_FAIL(ObSysTaskStatMgr::get_instance().set_self_addr(self_addr_))) {
      LOG_ERROR("set sys task status self addr failed", KR(ret));
    } else if (OB_FAIL(ObTableStoreStatMgr::get_instance().init())) {
      LOG_ERROR("init table store stat mgr failed", KR(ret));
    } else if (OB_FAIL(ObCompatModeGetter::instance().init(&sql_proxy_))) {
      LOG_ERROR("init get compat mode server failed",KR(ret));
    } else if (OB_FAIL(table_service_.init())) {
      LOG_ERROR("init table service failed", KR(ret));
#ifdef OB_BUILD_TDE_SECURITY
    } else if (OB_FAIL(ObMasterKeyGetter::instance().init(&sql_proxy_))) {
      LOG_ERROR("init get master key server failed", KR(ret));
#endif
    } else if (OB_FAIL(ObTimerMonitor::get_instance().init())) {
      LOG_ERROR("init timer monitor failed", KR(ret));
    } else if (OB_FAIL(ObBGThreadMonitor::get_instance().init())) {
      LOG_ERROR("init bg thread monitor failed", KR(ret));
    } else if (OB_FAIL(ObPxBloomFilterManager::instance().init())) {
      LOG_ERROR("init px blomm filter manager failed", KR(ret));
    } else if (OB_FAIL(PX_P2P_DH.init())) {
      LOG_ERROR("init px p2p datahub failed", KR(ret));
    } else if (OB_FAIL(G_RES_MGR.init())) {
      LOG_ERROR("failed to init resource plan", KR(ret));
#ifdef ENABLE_IMC
    } else if (OB_FAIL(imc_tasks_.init())) {
      LOG_ERROR("init imc tasks failed", KR(ret));
#endif
    } else if (OB_FAIL(OB_PRIMARY_STANDBY_SERVICE.init(&sql_proxy_, &schema_service_))) {
      LOG_ERROR("init OB_PRIMARY_STANDBY_SERVICE failed", KR(ret));
    } else if (OB_FAIL(init_px_target_mgr())) {
      LOG_ERROR("init px target mgr failed", KR(ret));
    } else if (OB_FAIL(OB_BACKUP_INDEX_CACHE.init())) {
      LOG_ERROR("init backup index cache failed", KR(ret));
    } else if (OB_FAIL(ObActiveSessHistList::get_instance().init())) {
      LOG_ERROR("init ASH failed", KR(ret));
    } else if (OB_FAIL(ObServerBlacklist::get_instance().init(self_addr_, net_frame_.get_req_transport()))) {
      LOG_ERROR("init server blacklist failed", KR(ret));
    } else if (OB_FAIL(ObLongopsMgr::get_instance().init())) {
      LOG_WARN("init longops mgr fail", KR(ret));
    } else if (OB_FAIL(ObDDLRedoLock::get_instance().init())) {
      LOG_WARN("init ddl redo lock failed", K(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(ObDDLSimPointMgr::get_instance().init())) {
      LOG_WARN("init ddl sim point mgr fail", KR(ret));
#endif
    }
#ifdef OB_BUILD_ARBITRATION
    else if (OB_FAIL(arb_gcs_.init(GCTX.self_addr(),
                                   lib::TGDefIDs::ArbGCSTh,
                                    GCTX.srv_rpc_proxy_, GCTX.sql_proxy_))) {
      LOG_ERROR("init arb_gcs_ failed", KR(ret));
    }
#endif
    else if (OB_FAIL(ObDetectManagerThread::instance().init(GCTX.self_addr(), net_frame_.get_req_transport()))) {
      LOG_WARN("init ObDetectManagerThread failed", KR(ret));
    } else if (OB_FAIL(wr_service_.init())) {
      LOG_WARN("failed to init wr service", K(ret));
    } else if (OB_FAIL(ObStorageHADiagService::instance().init(GCTX.sql_proxy_))) {
      LOG_WARN("init storage ha diagnose service failed", K(ret));
    } else {
      GDS.set_rpc_proxy(&rs_rpc_proxy_);
    }
  }

  if (OB_FAIL(ret)) {
    set_stop();
    destroy();
    LOG_ERROR("[OBSERVER_NOTICE] fail to init observer", KR(ret));
    LOG_DBA_FORCE_PRINT(DBA_ERROR, OB_SERVER_INIT_FAIL, ret,
                        DBA_STEP_INC_INFO(server_start),
                        "observer init fail. "
                        "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    FLOG_INFO("[OBSERVER_NOTICE] success to init observer", "cluster_id", obrpc::ObRpcNetHandler::CLUSTER_ID,
        "lib::g_runtime_enabled", lib::g_runtime_enabled);
    LOG_DBA_INFO_V2(OB_SERVER_INIT_SUCCESS,
                    DBA_STEP_INC_INFO(server_start),
                    "observer init success.");
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
  // This is essentially an implementation problem of repeated destruction of ObBackupInfo (or one of its members). ObServer also adds a layer of defense here.
  FLOG_INFO("[OBSERVER_NOTICE] destroy observer begin");

  FLOG_INFO("begin to destroy config manager");
  config_mgr_.destroy();
  FLOG_INFO("destroy config manager success");

  if (is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    destroy_server_in_arb_mode();
#endif
  } else if (!has_destroy_ && has_stopped_) {

    FLOG_INFO("begin to destroy OB_LOGGER");
    OB_LOGGER.destroy();
    FLOG_INFO("OB_LOGGER destroyed");

    FLOG_INFO("begin to destroy OB_LOG_COMPRESSOR");
    OB_LOG_COMPRESSOR.destroy();
    FLOG_INFO("OB_LOG_COMPRESSOR destroyed");

    FLOG_INFO("begin to destroy task controller");
    ObTaskController::get().destroy();
    FLOG_INFO("task controller destroyed");

    FLOG_INFO("begin destroy signal worker");
    sig_worker_->destroy();
    FLOG_INFO("signal worker destroyed");

    FLOG_INFO("begin destroy signal handle");
    signal_handle_->destroy();
    FLOG_INFO("signal handle destroyed");

    FLOG_INFO("opt stat manager destroyed");
    ObOptStatManager::get_instance().destroy();
    FLOG_INFO("opt stat manager destroyed");

    FLOG_INFO("begin to destroy active session history task");
    ObActiveSessHistTask::get_instance().destroy();
    FLOG_INFO("active session history task destroyed");

    FLOG_INFO("begin to destroy timer monitor");
    ObTimerMonitor::get_instance().destroy();
    FLOG_INFO("timer monitor destroyed");

    FLOG_INFO("begin to destroy background thread monitor");
    ObBGThreadMonitor::get_instance().destroy();
    FLOG_INFO("background thread monitor destroyed");

    FLOG_INFO("begin to destroy thread hung detector");
    common::occam::ObThreadHungDetector::get_instance().destroy();
    FLOG_INFO("thread hung detector destroyed");

    FLOG_INFO("begin to destroy table store stat mgr");
    ObTableStoreStatMgr::get_instance().destroy();
    FLOG_INFO("table store stat mgr destroyed");

#ifdef OB_BUILD_TDE_SECURITY
    FLOG_INFO("begin to destroy master key getter");
    ObMasterKeyGetter::instance().destroy();
    FLOG_INFO("master key getter destroyed");
#endif

    FLOG_INFO("begin to destroy unix domain listener");
    unix_domain_listener_.destroy();
    FLOG_INFO("unix domain listener destroyed");

    FLOG_INFO("begin to destroy table service");
    table_service_.destroy();
    FLOG_INFO("table service destroyed");

    FLOG_INFO("begin to destroy batch rpc");
    batch_rpc_.destroy();
    FLOG_INFO("batch rpc destroyed");

    FLOG_INFO("begin to destroy schema service");
    schema_service_.destroy();
    FLOG_INFO("schema service destroyed");

    FLOG_INFO("begin to destroy table auto increment service");
    ObTabletAutoincrementService::get_instance().destroy();
    FLOG_INFO("table auto increment service destroyed");

    FLOG_INFO("begin to destroy server gtimer");
    TG_DESTROY(lib::TGDefIDs::ServerGTimer);
    FLOG_INFO("server gtimer destroyed");

    FLOG_INFO("begin to destroy freeze timer");
    TG_DESTROY(lib::TGDefIDs::FreezeTimer);
    FLOG_INFO("freeze timer destroyed");

    FLOG_INFO("begin to destroy sql memory manager timer");
    TG_DESTROY(lib::TGDefIDs::SqlMemTimer);
    FLOG_INFO("sql memory manager timer destroyed");

    FLOG_INFO("begin to destroy server trace timer");
    TG_DESTROY(lib::TGDefIDs::ServerTracerTimer);
    FLOG_INFO("server trace timer destroyed");

    FLOG_INFO("begin to destroy ctas clean up timer");
    TG_DESTROY(lib::TGDefIDs::CTASCleanUpTimer);
    FLOG_INFO("ctas clean up timer destroyed");

    FLOG_INFO("begin to destroy redef heart beat task");
    TG_DESTROY(lib::TGDefIDs::RedefHeartBeatTask);
    FLOG_INFO("redef heart beat task destroyed");

    FLOG_INFO("begin to destroy root service");
    root_service_.destroy();
    FLOG_INFO("root service destroyed");

    FLOG_INFO("begin to destroy ob service");
    ob_service_.destroy();
    FLOG_INFO("ob service destroyed");

    FLOG_INFO("begin to destroy session manager");
    session_mgr_.destroy();
    FLOG_INFO("session manager destroyed");

    FLOG_INFO("begin to destroy locality manager");
    locality_manager_.destroy();
    FLOG_INFO("locality manager destroyed");

    FLOG_INFO("begin to destroy sql engine");
    sql_engine_.destroy();
    FLOG_INFO("sql engine destroyed");

    FLOG_INFO("begin to destroy xml ctx");
    ObLibXml2SaxHandler::destroy();
    FLOG_INFO("xml ctx destroyed");

    FLOG_INFO("begin to destroy pl engine");
    pl_engine_.destory();
    FLOG_INFO("pl engine destroyed");

    FLOG_INFO("begin to destroy tenant disk usage report task");
    disk_usage_report_task_.destroy();
    FLOG_INFO("tenant disk usage report task destroyed");

    FLOG_INFO("begin to destroy tmp file manager");
    ObTmpFileManager::get_instance().destroy();
    FLOG_INFO("tmp file manager destroyed");

    FLOG_INFO("begin to destroy disk usage report task");
    TG_DESTROY(lib::TGDefIDs::DiskUseReport);
    FLOG_INFO("disk usage report task destroyed");

    FLOG_INFO("begin to destroy store cache");
    OB_STORE_CACHE.destroy();
    FLOG_INFO("store cache destroyed");

    FLOG_INFO("begin to destroy tx data kv cache");
    OB_TX_DATA_KV_CACHE.destroy();
    FLOG_INFO("tx data kv cache destroyed");

    FLOG_INFO("begin to destroy log kv cache");
    OB_LOG_KV_CACHE.destroy();
    FLOG_INFO("log kv cache destroyed");

    FLOG_INFO("begin to destroy location service");
    location_service_.destroy();
    FLOG_INFO("location service destroyed");

    FLOG_INFO("begin to destroy ts mgr");
    OB_TS_MGR.destroy();
    FLOG_INFO("ts mgr destroyed");

    FLOG_INFO("begin to destroy weak read service");
    weak_read_service_.destroy();
    FLOG_INFO("weak read service destroyed");

    FLOG_INFO("begin to destroy blacklist service");
    bl_service_.destroy();
    FLOG_INFO("blacklist service destroyed");

    FLOG_INFO("begin to destroy net frame");
    net_frame_.destroy();
    FLOG_INFO("net frame destroyed");

    FLOG_INFO("begin to destroy oss storage");
    fin_oss_env();
    FLOG_INFO("oss storage destroyed");

    FLOG_INFO("begin to destroy io manager");
    ObIOManager::get_instance().destroy();
    FLOG_INFO("io manager destroyed");

    FLOG_INFO("begin to destroy io device");
    ObIODeviceWrapper::get_instance().destroy();
    FLOG_INFO("io device destroyed");

    FLOG_INFO("begin to destroy memory dump");
    ObMemoryDump::get_instance().destroy();
    FLOG_INFO("memory dump destroyed");

    FLOG_INFO("begin to destroy tenant timezone manager");
    tenant_timezone_mgr_.destroy();
    FLOG_INFO("tenant timezone manager destroyed");

    FLOG_INFO("begin to destroy ObMdsEventBuffer");
    ObMdsEventBuffer::destroy();
    FLOG_INFO("ObMdsEventBuffer destroyed");

    FLOG_INFO("begin to wait destroy multi tenant");
    multi_tenant_.destroy();
    FLOG_INFO("wait destroy multi tenant success");

    FLOG_INFO("begin to unregister fulltext plugins");
    ObTenantFTPluginMgr::unregister_plugins();
    FLOG_INFO("fulltext plugins unregistered");

    FLOG_INFO("begin to destroy query retry ctrl");
    ObQueryRetryCtrl::destroy();
    FLOG_INFO("query retry ctrl destroy");

    FLOG_INFO("begin to destroy server checkpoint slog handler");
    ObServerCheckpointSlogHandler::get_instance().destroy();
    FLOG_INFO("server checkpoint slog handler destroyed");

    FLOG_INFO("begin to destroy ob server block mgr");
    OB_SERVER_BLOCK_MGR.destroy();
    FLOG_INFO("ob server block mgr destroyed");

    FLOG_INFO("begin to destroy server startup task handler");
    startup_accel_handler_.destroy();
    FLOG_INFO("server startup task handler destroyed");

    FLOG_INFO("begin to destroy backup index cache");
    OB_BACKUP_INDEX_CACHE.destroy();
    FLOG_INFO("backup index cache destroyed");

    FLOG_INFO("begin to destroy log block mgr");
    log_block_mgr_.destroy();
    FLOG_INFO("log block mgr destroy");

    FLOG_INFO("begin to destroy server blacklist");
    ObServerBlacklist::get_instance().destroy();
    FLOG_INFO("server blacklist destroy");

    FLOG_INFO("begin to destroy global election report timer");
    palf::election::GLOBAL_REPORT_TIMER.destroy();
    FLOG_INFO("global election report timer destroyed");

    ObStorageHADiagService::instance().destroy();
    FLOG_INFO("storage ha diagnose destroy");

    FLOG_INFO("begin to destroy virtual tenant manager");
    ObVirtualTenantManager::get_instance().destroy();
    FLOG_INFO("virtual tenant manager destroyed");

    FLOG_INFO("begin to destroy OB_PRIMARY_STANDBY_SERVICE");
    OB_PRIMARY_STANDBY_SERVICE.destroy();
    FLOG_INFO("OB_PRIMARY_STANDBY_SERVICE destroyed");

    FLOG_INFO("begin to destroy rootservice event history");
    ROOTSERVICE_EVENT_INSTANCE.destroy();
    FLOG_INFO("rootservice event history destroyed");

    FLOG_INFO("begin to destroy kv global cache");
    ObKVGlobalCache::get_instance().destroy();
    FLOG_INFO("kv global cache destroyed");

    FLOG_INFO("begin to destroy clock generator");
    ObClockGenerator::destroy();
    FLOG_INFO("clock generator destroyed");

#ifdef OB_BUILD_ARBITRATION
    arb_gcs_.destroy();
    FLOG_INFO("ArbGarbageCollectSerivce destroyed");
#endif

    FLOG_INFO("begin to destroy WR service");
    wr_service_.destroy();
    FLOG_INFO("WR service destroyed");

    deinit_zlib_lite_compressor();

    has_destroy_ = true;
    FLOG_INFO("[OBSERVER_NOTICE] destroy observer end");
  }
}

int ObServer::start_sig_worker_and_handle()
{
  int ret = OB_SUCCESS;
  if (sig_worker_ != nullptr && OB_FAIL(sig_worker_->start())) {
    LOG_ERROR("fail to start signal worker", KR(ret));
  } else if (signal_handle_ != nullptr && OB_FAIL(signal_handle_->start())) {
    LOG_ERROR("fail to start signal handle", KR(ret));
  } else {
    FLOG_INFO("success to start signal worker and handle");
  }
  return ret;
}

int ObServer::start()
{
  int ret = OB_SUCCESS;
  int64_t reserved_size = 0;
  gctx_.status_ = SS_STARTING;
  // begin to start a observer
  FLOG_INFO("[OBSERVER_NOTICE] start observer begin");
  LOG_DBA_INFO_V2(OB_SERVER_START_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "observer start begin.");

  if (is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    if (OB_FAIL(start_sig_worker_and_handle())) {
      LOG_ERROR("fail to start signal worker and handle", KR(ret));
    } else if (OB_FAIL(start_server_in_arb_mode())) {
      LOG_ERROR("start_server_in_arb_mode failed", K(ret));
    } else {
      FLOG_INFO("[OBSERVER_NOTICE] server instance start succeed");
      prepare_stop_ = false;
      stop_ = false;
      has_stopped_ = false;
    }
#endif
  } else {
    LOG_DBA_INFO_V2(OB_SERVER_INSTANCE_START_BEGIN,
                    DBA_STEP_INC_INFO(server_start),
                    "observer instance start begin.");
    if (OB_FAIL(start_sig_worker_and_handle())) {
      LOG_ERROR("fail to start signal worker", KR(ret));
    }

    if (FAILEDx(startup_accel_handler_.start())) {
      LOG_ERROR("fail to start server startup task handler", KR(ret));
    } else {
      FLOG_INFO("success to start server startup task handler");
    }

    if (FAILEDx(OB_TS_MGR.start())) {
      LOG_ERROR("fail to start ts mgr", KR(ret));
    } else {
      FLOG_INFO("success to start ts mgr");
    }

    if (FAILEDx(net_frame_.start())) {
      LOG_ERROR("fail to start net frame", KR(ret));
    } else {
      FLOG_INFO("success to start net frame");
    }

    if (FAILEDx(SLOGGERMGR.get_reserved_size(reserved_size))) {
      LOG_ERROR("fail to get reserved size", KR(ret), K(reserved_size));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.start(reserved_size))) {
      LOG_ERROR("start block manager fail", KR(ret));
    } else {
      FLOG_INFO("success to start block manager");
    }

    if (FAILEDx(ObMdsSchemaHelper::get_instance().init())) {
      LOG_ERROR("fail to init mds schema helper", K(ret));
    } else {
      FLOG_INFO("success to init mds schema helper");
    }

    if (FAILEDx(ObIOManager::get_instance().start())) {
      LOG_ERROR("fail to start io manager", KR(ret));
    } else {
      FLOG_INFO("success to start io manager");
    }

    if (FAILEDx(multi_tenant_.start())) {
      LOG_ERROR("fail to start multi tenant", KR(ret));
    } else {
      FLOG_INFO("success to start multi tenant");
    }

    if (FAILEDx(wr_service_.start())) {
      LOG_ERROR("failed to start wr service", K(ret));
    } else {
      LOG_INFO("success to start wr service");
    }

    if (FAILEDx(ObServerCheckpointSlogHandler::get_instance().start())) {
      LOG_ERROR("fail to start server checkpoint slog handler", KR(ret));
    } else {
      FLOG_INFO("success to start server checkpoint slog handler");
    }

    if (FAILEDx(log_block_mgr_.start(storage_env_.log_disk_size_))) {
      LOG_ERROR("fail to start log pool", KR(ret));
    } else {
      FLOG_INFO("success to start log pool");
    }

    if (FAILEDx(try_update_hidden_sys())) {
      LOG_ERROR("fail to update hidden sys tenant", KR(ret));
    } else {
      FLOG_INFO("success to update hidden sys tenant");
    }

    if (FAILEDx(weak_read_service_.start())) {
      LOG_ERROR("fail to start weak read service", KR(ret));
    } else {
      FLOG_INFO("success to start weak read service");
    }

    if (FAILEDx(bl_service_.start())) {
      LOG_ERROR("fail to start blacklist service", KR(ret));
    } else {
      FLOG_INFO("success to start blacklist service");
    }

    // do not wait clog replay over, avoid blocking other module
    if (FAILEDx(root_service_monitor_.start())) {
      LOG_ERROR("fail to start root service monitor", KR(ret));
    } else {
      FLOG_INFO("success to start root service monitor");
    }

    if (FAILEDx(ob_service_.start())) {
      LOG_ERROR("fail to start oceanbase service", KR(ret));
    } else {
      FLOG_INFO("success to start oceanbase service");
    }

    if (FAILEDx(locality_manager_.start())) {
      LOG_ERROR("fail to start locality manager", K(ret));
    } else {
      FLOG_INFO("success to start locality manager");
    }

    if (FAILEDx(reload_config_())) {
      LOG_ERROR("fail to reload configuration", KR(ret));
    } else {
      FLOG_INFO("success to reload configuration");
    }

    if (FAILEDx(ObTimerMonitor::get_instance().start())) {
      LOG_ERROR("fail to start timer monitor", KR(ret));
    } else {
      FLOG_INFO("success to start timer monitor");
    }

    if (FAILEDx(ObBGThreadMonitor::get_instance().start())) {
      LOG_ERROR("fail to start bg thread monitor", KR(ret));
    } else {
      FLOG_INFO("success to start bg thread monitor");
    }
#ifdef ENABLE_IMC
    if (FAILEDx(imc_tasks_.start())) {
      LOG_ERROR("fail to start imc tasks", KR(ret));
    } else {
      FLOG_INFO("success to start imc tasks");
    }
#endif

    if (FAILEDx(ObStorageHADiagService::instance().start())) {
      LOG_ERROR("fail to start storage ha diagnose service", KR(ret));
    } else {
      FLOG_INFO("success to start storage ha diagnose service");
    }

    if (FAILEDx(unix_domain_listener_.start())) {
      LOG_ERROR("fail to start unix domain listener", KR(ret));
    } else {
      FLOG_INFO("success to start unix domain listener");
    }

    if (FAILEDx(OB_PX_TARGET_MGR.start())) {
      LOG_ERROR("fail to start ObPxTargetMgr", KR(ret));
    } else {
      FLOG_INFO("success to start ObPxTargetMgr");
    }

    if (FAILEDx(TG_SCHEDULE(lib::TGDefIDs::DiskUseReport,
        disk_usage_report_task_, DISK_USAGE_REPORT_INTERVAL, true))) {
      LOG_ERROR("fail to schedule disk_usage_report_task_ task", KR(ret));
    } else {
      FLOG_INFO("success to schedule disk_usage_report_task_ task");
    }

    if (FAILEDx(ObActiveSessHistTask::get_instance().start())) {
      LOG_ERROR("fail to init active session history task", KR(ret));
    } else {
      FLOG_INFO("success to init active session history task");
    }

    if (FAILEDx(location_service_.start())) {
      LOG_ERROR("fail to start location service", KR(ret));
    } else {
      FLOG_INFO("success to start location service");
    }

#ifdef OB_BUILD_ARBITRATION
    if (FAILEDx(arb_gcs_.start())) {
      LOG_ERROR("start arb_gcs_ failed", KR(ret));
    } else {
      FLOG_INFO("success to start ArbGarbageCollectSerivce");
    }
#endif

    if (OB_SUCC(ret)) {
      FLOG_INFO("[OBSERVER_NOTICE] server instance start succeed");
      LOG_DBA_INFO_V2(OB_SERVER_INSTANCE_START_SUCCESS,
                      DBA_STEP_INC_INFO(server_start),
                      "observer instance start success.");
      prepare_stop_ = false;
      stop_ = false;
      has_stopped_ = false;
    } else {
      LOG_DBA_ERROR_V2(OB_SERVER_INSTANCE_START_FAIL, ret,
                       DBA_STEP_INC_INFO(server_start),
                       "observer instance start fail. "
                       "you may find solutions in previous error logs or seek help from official technicians.");
    }
    // this handler is only used to process tasks during startup. so it can be destroied here.
    startup_accel_handler_.destroy();

    // refresh server configure
    //
    // The version should be greater than original version but less
    // than normal one.
    if (FAILEDx(config_mgr_.got_version(ObSystemConfig::INIT_VERSION))) {
      FLOG_WARN("fail to refresh server configure", KR(ret));
    } else {
      FLOG_INFO("success to refresh server configure");
    }

    // check if multi tenant synced
    if (FAILEDx(check_if_multi_tenant_synced())) {
      LOG_ERROR("fail to check if multi tenant synced", KR(ret));
    } else {
      FLOG_INFO("success to check if multi tenant synced");
    }

    // check if schema ready
    if (FAILEDx(check_if_schema_ready())) {
      LOG_ERROR("fail to check if schema ready", KR(ret));
    } else {
      FLOG_INFO("success to check if schema ready");
    }

    // check if timezone usable
    if (FAILEDx(check_if_timezone_usable())) {
      LOG_ERROR("fail to check if timezone usable", KR(ret));
    } else {
      FLOG_INFO("success to check if timezone usable");
    }

    // check log replay and user tenant schema refresh status
    if (OB_SUCC(ret)) {
      if (stop_) {
        ret = OB_SERVER_IS_STOPPING;
        FLOG_WARN("server is in stopping status", KR(ret));
      } else {
        ObSEArray<uint64_t, 16> tenant_ids;
        const int64_t MAX_CHECK_TIME = 15 * 60 * 1000 * 1000L; // 15min
        const int64_t start_ts = ObTimeUtility::current_time();
        int64_t schema_refreshed_ts = 0;
        const int64_t expire_time = start_ts + MAX_CHECK_TIME;
        tenant_ids.set_max_print_count(512);

        if (OB_FAIL(multi_tenant_.get_mtl_tenant_ids(tenant_ids))) {
          FLOG_ERROR("get mtl tenant ids fail", KR(ret));
        } else if (tenant_ids.count() <= 0) {
          // do nothing
        } else {
          // check user tenant schema refresh
          check_user_tenant_schema_refreshed(tenant_ids, expire_time);
          schema_refreshed_ts = ObTimeUtility::current_time();
          // check log replay status
          check_log_replay_over(tenant_ids, expire_time);
        }
        FLOG_INFO("[OBSERVER_NOTICE] check log replay and user tenant schema finished",
            KR(ret),
            K(tenant_ids),
            "refresh_schema_cost_us", schema_refreshed_ts - start_ts,
            "replay_log_cost_us", ObTimeUtility::current_time() - schema_refreshed_ts);
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("failure occurs, try to set stop and wait", KR(ret));
    LOG_DBA_FORCE_PRINT(DBA_ERROR, OB_SERVER_START_FAIL, ret,
                        DBA_STEP_INC_INFO(server_start),
                        "observer start fail, the stop status is ", stop_, ". "
                        "you may find solutions in previous error logs or seek help from official technicians.");
    set_stop();
    wait();
  } else if (!stop_) {
    GCTX.status_ = SS_SERVING;
    GCTX.start_service_time_ = ObTimeUtility::current_time();
    FLOG_INFO("[OBSERVER_NOTICE] observer start service", "start_service_time", GCTX.start_service_time_);
    LOG_DBA_INFO_V2(OB_SERVER_START_SUCCESS,
                    DBA_STEP_INC_INFO(server_start),
                    "observer start success.");
  } else {
    FLOG_INFO("[OBSERVER_NOTICE] observer is set to stop", KR(ret), K_(stop));
    LOG_DBA_FORCE_PRINT(DBA_ERROR, OB_SERVER_START_FAIL, ret,
                        DBA_STEP_INC_INFO(server_start),
                        "observer start fail, the stop status is ", stop_, ". "
                        "you may find solutions in previous error logs or seek help from official technicians.");
  }

  return ret;
}

// try create hidden sys tenant must after ObServerCheckpointSlogHandler start,
// update hidden sys tenant unit if it exists
int ObServer::try_update_hidden_sys()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  omt::ObTenant *tenant = nullptr;
  if (OB_FAIL(multi_tenant_.get_tenant(tenant_id, tenant))) {
    if (OB_TENANT_NOT_IN_SERVER == ret) { // only when adding a new server
      ret = OB_SUCCESS;
      if (OB_FAIL(multi_tenant_.create_hidden_sys_tenant())) {
        LOG_ERROR("fail to create hidden sys tenant", KR(ret));
      }
      LOG_INFO("finish create hidden sys", KR(ret));
    } else {
      LOG_ERROR("fail to get tenant", KR(ret));
    }
  } else if (OB_FAIL(multi_tenant_.update_hidden_sys_tenant())) {
    LOG_WARN("fail to update hidden sys tenant unit", KR(ret));
  }
  return ret;
}

int ObServer::check_if_multi_tenant_synced()
{
  int ret = OB_SUCCESS;
  bool synced = false;
  LOG_DBA_INFO_V2(OB_SERVER_WAIT_MULTI_TENANT_SYNCED_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "wait multi tenant synced begin.");
  while (OB_SUCC(ret) && !stop_ && !synced) {
    synced = multi_tenant_.has_synced();
    if (!synced) {
      SLEEP(1);
    }
  }
  FLOG_INFO("check if multi tenant synced", KR(ret), K(stop_), K(synced));
  if (!stop_ && synced) {
    LOG_DBA_INFO_V2(OB_SERVER_WAIT_MULTI_TENANT_SYNCED_SUCCESS,
                    DBA_STEP_INC_INFO(server_start),
                    "wait multi tenant synced success.");
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_WAIT_MULTI_TENANT_SYNCED_FAIL, ret,
                     DBA_STEP_INC_INFO(server_start),
                     "wait multi tenant synced fail, server stop status is ", stop_, ". "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  }
  return ret;
}

int ObServer::check_if_schema_ready()
{
  int ret = OB_SUCCESS;
  bool schema_ready = false;
  LOG_DBA_INFO_V2(OB_SERVER_WAIT_SCHEMA_READY_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "wait schema ready begin.");
  while (OB_SUCC(ret) && !stop_ && !schema_ready) {
    schema_ready = schema_service_.is_sys_full_schema();
    if (!schema_ready) {
      SLEEP(1);
    }
  }
  FLOG_INFO("check if schema ready", KR(ret), K(stop_), K(schema_ready));
  if (!stop_ && schema_ready) {
    LOG_DBA_INFO_V2(OB_SERVER_WAIT_SCHEMA_READY_SUCCESS,
                    DBA_STEP_INC_INFO(server_start),
                    "wait schema ready success.");
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_WAIT_SCHEMA_READY_FAIL, ret,
                     DBA_STEP_INC_INFO(server_start),
                     "wait schema ready fail, server stop status is ", stop_, ". "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  }
  return ret;
}

int ObServer::check_if_timezone_usable()
{
  int ret = OB_SUCCESS;
  bool timezone_usable = false;
  if (FAILEDx(tenant_timezone_mgr_.start())) {
    LOG_ERROR("fail to start tenant timezone mgr", KR(ret));
  } else {
    FLOG_INFO("success to start tenant timezone mgr");
  }
  while (OB_SUCC(ret) && !stop_ && !timezone_usable) {
    timezone_usable = tenant_timezone_mgr_.is_usable();
    if (!timezone_usable) {
      SLEEP(1);
    }
  }
  FLOG_INFO("check if timezone usable", KR(ret), K(stop_), K(timezone_usable));
  return ret;
}
void ObServer::prepare_stop()
{
  prepare_stop_ = true;
  // reserve some time to switch leader
  ob_usleep(5 * 1000 * 1000);
}

bool ObServer::is_prepare_stopped()
{
  return prepare_stop_;
}

bool ObServer::is_stopped()
{
  return stop_;
}

void ObServer::set_stop()
{
  stop_ = true;
  ob_service_.set_stop();
  gctx_.status_ = SS_STOPPING;
  FLOG_INFO("[OBSERVER_NOTICE] observer is setted to stop");
}

int ObServer::stop()
{
  int ret = OB_SUCCESS;
  int fail_ret = OB_SUCCESS;
  FLOG_INFO("[OBSERVER_NOTICE] stop observer begin");
  LOG_DBA_INFO_V2(OB_SERVER_STOP_BEGIN, "observer stop begin.");

  FLOG_INFO("begin to stop OB_LOGGER");
  OB_LOGGER.stop();
  FLOG_INFO("stop OB_LOGGER success");

  FLOG_INFO("begin to stop OB_LOG_COMPRESSOR");
  OB_LOG_COMPRESSOR.stop();
  FLOG_INFO("stop OB_LOG_COMPRESSOR success");

  FLOG_INFO("begin to stop task controller");
  ObTaskController::get().stop();
  FLOG_INFO("stop task controller success");

  FLOG_INFO("begin to stop config manager");
  config_mgr_.stop();
  FLOG_INFO("stop config manager success");

  if (is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    (void) stop_server_in_arb_mode();
#endif
  } else {
#ifdef ENABLE_IMC
    FLOG_INFO("begin to stop imc tasks", KR(ret));
    ret = imc_tasks_.stop();
    FLOG_INFO("end to stop imc tasks", KR(ret));
#endif

    FLOG_INFO("begin stop signal worker");
    sig_worker_->stop();
    FLOG_INFO("stop signal worker success");

    FLOG_INFO("begin stop signal handle");
    signal_handle_->stop();
    FLOG_INFO("stop signal handle success");

    FLOG_INFO("begin to stop server blacklist");
    TG_STOP(lib::TGDefIDs::Blacklist);
    FLOG_INFO("server blacklist stopped");

    FLOG_INFO("begin to stop detect manager detect thread");
    TG_STOP(lib::TGDefIDs::DetectManager);
    FLOG_INFO("detect manager detect thread stopped");

    FLOG_INFO("begin to stop ObNetKeepAlive");
    ObNetKeepAlive::get_instance().stop();
    FLOG_INFO("ObNetKeepAlive stopped");

    FLOG_INFO("begin to stop GDS");
    GDS.stop();
    FLOG_INFO("GDS stopped");

    FLOG_INFO("begin to mysql shutdown network");
    if (OB_FAIL(net_frame_.mysql_shutdown())) {
      FLOG_WARN("fail to mysql shutdown network", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("mysql shutdown network stopped");
    }

    FLOG_INFO("begin to sql nio stop");
    net_frame_.sql_nio_stop();
    FLOG_INFO("sql nio stopped");

    FLOG_INFO("begin to stop rpc listen and io threads");
    net_frame_.rpc_stop();
    FLOG_INFO("rpc stopped");

    FLOG_INFO("begin to stop active session history task");
    ObActiveSessHistTask::get_instance().stop();
    FLOG_INFO("active session history task stopped");

    FLOG_INFO("begin to stop table store stat mgr");
    ObTableStoreStatMgr::get_instance().stop();
    FLOG_INFO("table store stat mgr stopped");

#ifdef OB_BUILD_TDE_SECURITY
    FLOG_INFO("begin to stop master key getter");
    ObMasterKeyGetter::instance().stop();
    FLOG_INFO("master key getter stopped");
#endif

    FLOG_INFO("begin to stop unix domain listener");
    unix_domain_listener_.stop();
    FLOG_INFO("unix domain listener stopped");

    FLOG_INFO("begin to stop table service");
    table_service_.stop();
    FLOG_INFO("table service stopped");

    FLOG_INFO("begin to stop batch rpc");
    batch_rpc_.stop();
    FLOG_INFO("batch rpc stopped");

    FLOG_INFO("begin to stop schema service");
    schema_service_.stop();
    FLOG_INFO("schema service stopped");

    FLOG_INFO("begin to stop disk usage report task");
    TG_STOP(lib::TGDefIDs::DiskUseReport);
    FLOG_INFO("disk usage report task stopped");

    FLOG_INFO("begin to stop ob server block mgr");
    OB_SERVER_BLOCK_MGR.stop();
    FLOG_INFO("ob server block mgr stopped");

    FLOG_INFO("begin to stop locality manager");
    locality_manager_.stop();
    FLOG_INFO("locality manager stopped");

    FLOG_INFO("begin to stop location service");
    location_service_.stop();
    FLOG_INFO("location service stopped");

    FLOG_INFO("begin to stop timer monitor");
    ObTimerMonitor::get_instance().stop();
    FLOG_INFO("timer monitor stopped");

    FLOG_INFO("begin to stop bgthread monitor");
    ObBGThreadMonitor::get_instance().stop();
    FLOG_INFO("bgthread monitor stopped");

    FLOG_INFO("begin to stop thread hung detector");
    common::occam::ObThreadHungDetector::get_instance().stop();
    FLOG_INFO("thread hung detector stopped");

    FLOG_INFO("begin to stop timer");
    TG_STOP(lib::TGDefIDs::ServerGTimer);
    FLOG_INFO("timer stopped");

    FLOG_INFO("begin to stop freeze timer");
    TG_STOP(lib::TGDefIDs::FreezeTimer);
    FLOG_INFO("freeze timer stopped");

    FLOG_INFO("begin to stop sql memory manager timer");
    TG_STOP(lib::TGDefIDs::SqlMemTimer);
    FLOG_INFO("sql memory manager timer stopped");

    FLOG_INFO("begin to stop server trace timer");
    TG_STOP(lib::TGDefIDs::ServerTracerTimer);
    FLOG_INFO("server trace timer stopped");

    FLOG_INFO("begin to stop ctas clean up timer");
    TG_STOP(lib::TGDefIDs::CTASCleanUpTimer);
    FLOG_INFO("ctas clean up timer stopped");

    FLOG_INFO("begin to stop ctas clean up timer");
    TG_STOP(lib::TGDefIDs::HeartBeatCheckTask);
    FLOG_INFO("ctas clean up timer stopped");

    FLOG_INFO("begin to stop sql conn pool");
    sql_conn_pool_.stop();
    FLOG_INFO("sql connection pool stopped");

    FLOG_INFO("begin to stop ddl connection pool");
    ddl_conn_pool_.stop();
    FLOG_INFO("ddl connection pool stopped");

    FLOG_INFO("begin to stop dblink conn pool");
    dblink_conn_pool_.stop();
    FLOG_INFO("dblink connection pool stopped");

    FLOG_INFO("begin to stop resource inner connection pool");
    res_inner_conn_pool_.get_inner_sql_conn_pool().stop();
    FLOG_INFO("resource inner connection pool stopped");

    FLOG_INFO("begin to stop root service monitor");
    root_service_monitor_.stop();
    FLOG_INFO("root service monitor stopped");

    FLOG_INFO("begin to stop root service");
    if (OB_FAIL(root_service_.stop())) {
      FLOG_WARN("fail to stop root service", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("root service stopped");
    }

    FLOG_INFO("begin to stop weak read service");
    weak_read_service_.stop();
    FLOG_INFO("weak read service stopped");

    FLOG_INFO("begin to stop ts mgr");
    OB_TS_MGR.stop();
    FLOG_INFO("ts mgr stopped");

    FLOG_INFO("begin to stop px target mgr");
    OB_PX_TARGET_MGR.stop();
    FLOG_INFO("px target mgr stopped");

    FLOG_INFO("begin to stop blacklist service");
    bl_service_.stop();
    FLOG_INFO("blacklist service stopped");

    FLOG_INFO("begin to stop memory dump");
    ObMemoryDump::get_instance().stop();
    FLOG_INFO("memory dump stopped");

    FLOG_INFO("begin to stop tenant timezone manager");
    tenant_timezone_mgr_.stop();
    FLOG_INFO("tenant timezone manager stopped");
    //FLOG_INFO("begin stop partition scheduler");
    //ObPartitionScheduler::get_instance().stop_merge();
    //FLOG_INFO("partition scheduler stopped", KR(ret));

    FLOG_INFO("begin to stop opt stat manager ");
    ObOptStatManager::get_instance().stop();
    FLOG_INFO("opt stat manager  stopped");

    FLOG_INFO("begin to stop server checkpoint slog handler");
    ObServerCheckpointSlogHandler::get_instance().stop();
    FLOG_INFO("server checkpoint slog handler stopped");

    FLOG_INFO("begin to stop server startup task handler");
    startup_accel_handler_.stop();
    FLOG_INFO("server startup task handler stopped");

    // It will wait for all requests done.
    FLOG_INFO("begin to stop multi tenant");
    multi_tenant_.stop();
    FLOG_INFO("multi tenant stopped");
    FLOG_INFO("begin to stop ob_service");
    ob_service_.stop();
    FLOG_INFO("ob_service stopped");

    FLOG_INFO("begin to stop slogger manager");
    SLOGGERMGR.destroy();
    FLOG_INFO("slogger manager stopped");

    FLOG_INFO("begin to stop io manager");
    ObIOManager::get_instance().stop();
    FLOG_INFO("io manager stopped");

    FLOG_INFO("begin to stop ratelimit manager");
    rl_mgr_.stop();
    FLOG_INFO("ratelimit manager stopped");

#ifdef OB_BUILD_ARBITRATION
      arb_gcs_.stop();
      FLOG_INFO("ArbGarbageCollectSerivce stopped");
#endif

    FLOG_INFO("begin to stop WR service");
    wr_service_.stop();
    FLOG_INFO("WR service stopped");

    FLOG_INFO("begin to shutdown rpc network");
    if (OB_FAIL(net_frame_.rpc_shutdown())) {
      FLOG_WARN("fail to rpc shutdown network", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("rpc network shutdowned");
    }

    FLOG_INFO("begin to stop storage ha diagnose service");
    ObStorageHADiagService::instance().stop();
    FLOG_INFO("storage ha diagnose service stopped");

    FLOG_INFO("begin to shutdown high priority rpc");
    if (OB_FAIL(net_frame_.high_prio_rpc_shutdown())) {
      FLOG_WARN("fail to shutdown high priority rpc", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("high priority rpc shutdowned");
    }

    FLOG_INFO("begin to shutdown clog rpc network");
    if (OB_FAIL(net_frame_.batch_rpc_shutdown())) {
      FLOG_WARN("fail to shutdown clog rpc network", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("clog rpc network shutdowned");
    }

    FLOG_INFO("begin to shutdown unix rpc");
    if (OB_FAIL(net_frame_.unix_rpc_shutdown())) {
      FLOG_WARN("fail to shutdown unix rpc", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("unix rpc network shutdowned");
    }

    // net frame, ensure net_frame should stop after multi_tenant_
    // stopping.
    FLOG_INFO("begin to stop net frame");
    if (OB_FAIL(net_frame_.stop())) {
      FLOG_WARN("fail to stop net frame", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("net frame stopped");
    }

    FLOG_INFO("begin to stop rootservice event history");
    ROOTSERVICE_EVENT_INSTANCE.stop();
    FLOG_INFO("rootservice event history stopped");

    FLOG_INFO("begin to stop global election report timer");
    palf::election::GLOBAL_REPORT_TIMER.stop();
    FLOG_INFO("global election report timer stopped");

    FLOG_INFO("begin to stop kv global cache");
    ObKVGlobalCache::get_instance().stop();
    FLOG_INFO("kv global cache stopped");

    FLOG_INFO("begin to stop clock generator");
    ObClockGenerator::get_instance().stop();
    FLOG_INFO("clock generator stopped");

  }

  has_stopped_ = true;
  FLOG_INFO("[OBSERVER_NOTICE] stop observer end", KR(ret));
  if (OB_SUCCESS != fail_ret) {
    LOG_DBA_ERROR_V2(OB_SERVER_STOP_FAIL, fail_ret, "observer stop fail. "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    LOG_DBA_INFO_V2(OB_SERVER_STOP_SUCCESS, "observer stop success.");
  }

  return ret;
}

int ObServer::wait()
{
  int ret = OB_SUCCESS;
  int fail_ret = OB_SUCCESS;
  FLOG_INFO("[OBSERVER_NOTICE] wait observer begin");
  LOG_DBA_INFO_V2(OB_SERVER_WAIT_BEGIN, "observer process wait begin.");
  // wait for stop flag

  FLOG_INFO("begin to wait observer setted to stop");
  while (!stop_) {
    SLEEP(3);
  }

  FLOG_INFO("wait observer setted to stop success");

  FLOG_INFO("begin to stop observer");
  if (OB_FAIL(stop())) {
    FLOG_WARN("stop observer fail", KR(ret));
    fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
  } else {
    FLOG_INFO("observer stopped");
  }

  FLOG_INFO("begin to wait config manager");
  config_mgr_.wait();
  FLOG_INFO("wait config manager success");

  if (is_arbitration_mode()) {
#ifdef OB_BUILD_ARBITRATION
    (void) wait_server_in_arb_mode();
#endif
  } else {

    FLOG_INFO("begin to wait OB_LOGGER");
    OB_LOGGER.wait();
    FLOG_INFO("wait OB_LOGGER success");

    FLOG_INFO("begin to wait OB_LOG_COMPRESSOR");
    OB_LOG_COMPRESSOR.wait();
    FLOG_INFO("wait OB_LOG_COMPRESSOR success");

    FLOG_INFO("begin to wait task controller");
    ObTaskController::get().wait();
    FLOG_INFO("wait task controller success");

    FLOG_INFO("begin wait signal worker");
    sig_worker_->wait();
    FLOG_INFO("wait signal worker success");

    FLOG_INFO("begin wait signal handle");
    signal_handle_->wait();
    FLOG_INFO("wait signal handle success");

    FLOG_INFO("begin to wait active session hist task");
    ObActiveSessHistTask::get_instance().wait();
    FLOG_INFO("wait active session hist task success");

    FLOG_INFO("begin to wait timer monitor");
    ObTimerMonitor::get_instance().wait();
    FLOG_INFO("wait timer monitor success");

    FLOG_INFO("begin to wait table store stat mgr");
    ObTableStoreStatMgr::get_instance().wait();
    FLOG_INFO("wait table store stat mgr success");

#ifdef OB_BUILD_TDE_SECURITY
    FLOG_INFO("begin to wait master key getter");
    ObMasterKeyGetter::instance().wait();
    FLOG_INFO("wait master key getter success");
#endif

    FLOG_INFO("begin to wait unix domain listener");
    unix_domain_listener_.wait();
    FLOG_INFO("wait unix domain listener success");

    FLOG_INFO("begin to wait table service");
    table_service_.wait();
    FLOG_INFO("wait table service success");

    FLOG_INFO("begin to wait batch rpc");
    batch_rpc_.wait();
    FLOG_INFO("wait batch rpc success");

    FLOG_INFO("begin to wait schema service");
    schema_service_.wait();
    FLOG_INFO("wait schema service success");

    FLOG_INFO("begin to wait bg thread monitor");
    ObBGThreadMonitor::get_instance().wait();
    FLOG_INFO("wait bg thread monitor success");

    FLOG_INFO("begin to wait thread hung detector");
    common::occam::ObThreadHungDetector::get_instance().wait();
    FLOG_INFO("wait thread hung detector success");

#ifdef ENABLE_IMC
    FLOG_INFO("begin to wait imc tasks");
    imc_tasks_.wait();
    FLOG_INFO("wait imc tasks success");

    FLOG_INFO("begin to wait destroy imc tasks");
    imc_tasks_.destroy();
    FLOG_INFO("wait destroy imc tasks success");
#endif

    // timer
    FLOG_INFO("begin to wait server gtimer");
    TG_WAIT(lib::TGDefIDs::ServerGTimer);
    FLOG_INFO("wait server gtimer success");

    FLOG_INFO("begin to wait freeze timer");
    TG_WAIT(lib::TGDefIDs::FreezeTimer);
    FLOG_INFO("wait freeze timer success");

    FLOG_INFO("begin to wait sqlmem timer");
    TG_WAIT(lib::TGDefIDs::SqlMemTimer);
    FLOG_INFO("wait sqlmem timer success");

    FLOG_INFO("begin to wait server tracer timer");
    TG_WAIT(lib::TGDefIDs::ServerTracerTimer);
    FLOG_INFO("wait server tracer timer success");

    FLOG_INFO("begin to wait ctas clean up timer");
    TG_WAIT(lib::TGDefIDs::CTASCleanUpTimer);
    FLOG_INFO("wait ctas clean up timer success");

    FLOG_INFO("begin to wait root service");
    root_service_.wait();
    FLOG_INFO("wait root service success");

    FLOG_INFO("begin to wait root service");
    root_service_monitor_.wait();
    FLOG_INFO("wait root service monitor success");

    //omt
    FLOG_INFO("begin to wait multi tenant");
    multi_tenant_.wait();
    FLOG_INFO("wait multi tenant success");

    FLOG_INFO("begin to wait ratelimit manager");
    rl_mgr_.wait();
    FLOG_INFO("wait ratelimit manager success");

    FLOG_INFO("begin to wait net_frame");
    net_frame_.wait();
    FLOG_INFO("wait net_frame success");

    // over write previous ret.
    FLOG_INFO("begin to wait sql_conn_pool");
    if (OB_FAIL(sql_conn_pool_.wait())) {
      FLOG_WARN("fail to wait inner sql connection release", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("wait sql_conn_pool success");
    }

    FLOG_INFO("begin to wait ddl_conn_pool");
    if (OB_FAIL(ddl_conn_pool_.wait())) {
      FLOG_WARN("fail to wait ddl sql connection release", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("wait ddl_conn_pool success");
    }

    FLOG_INFO("begin to wait inner_sql_conn_pool");
    if (OB_FAIL(res_inner_conn_pool_.get_inner_sql_conn_pool().wait())) {
      FLOG_WARN("fail to wait resource inner connection release", KR(ret));
      fail_ret = OB_SUCCESS == fail_ret ? ret : fail_ret;
    } else {
      FLOG_INFO("wait inner_sql_conn_pool success");
    }

    FLOG_INFO("begin to wait ob_service");
    ob_service_.wait();
    FLOG_INFO("wait ob_service success");

    FLOG_INFO("begin to wait disk usage report task");
    TG_WAIT(lib::TGDefIDs::DiskUseReport);
    FLOG_INFO("wait disk usage report task success");

    FLOG_INFO("begin to wait ob_server_block_mgr");
    OB_SERVER_BLOCK_MGR.wait();
    FLOG_INFO("wait ob_server_block_mgr success");

    FLOG_INFO("begin to wait locality_manager");
    locality_manager_.wait();
    FLOG_INFO("wait locality_manager success");

    FLOG_INFO("begin to wait location service");
    location_service_.wait();
    FLOG_INFO("wait location service success");

    FLOG_INFO("begin to wait ts mgr");
    OB_TS_MGR.wait();
    FLOG_INFO("wait ts mgr success");

    FLOG_INFO("begin to wait px target mgr");
    OB_PX_TARGET_MGR.wait();
    FLOG_INFO("wait px target success");

    FLOG_INFO("begin to wait weak read service");
    weak_read_service_.wait();
    FLOG_INFO("wait weak read service success");

    FLOG_INFO("begin to wait blacklist service");
    bl_service_.wait();
    FLOG_INFO("wait blacklist service success");

    FLOG_INFO("begin to wait memory dump");
    ObMemoryDump::get_instance().wait();
    FLOG_INFO("wait memory dump success");

    FLOG_INFO("begin to wait tenant timezone manager");
    tenant_timezone_mgr_.wait();
    FLOG_INFO("wait tenant timezone manager success");

    FLOG_INFO("begin to wait opt stat manager");
    ObOptStatManager::get_instance().wait();
    FLOG_INFO("wait opt stat manager success");

    FLOG_INFO("begin to wait server checkpoint slog handler");
    ObServerCheckpointSlogHandler::get_instance().wait();
    FLOG_INFO("wait server checkpoint slog handler success");

    FLOG_INFO("begin to wait server startup task handler");
    startup_accel_handler_.wait();
    FLOG_INFO("wait server startup task handler success");

    FLOG_INFO("begin to wait global election report timer");
    palf::election::GLOBAL_REPORT_TIMER.wait();
    FLOG_INFO("wait global election report timer success");

   #ifdef OB_BUILD_ARBITRATION
     arb_gcs_.wait();
     FLOG_INFO("wait ArbGarbageCollectSerivce success");
   #endif

    FLOG_INFO("begin to wait WR service");
    wr_service_.wait();
    FLOG_INFO("wait WR service success");

    FLOG_INFO("begin to wait rootservice event history");
    ROOTSERVICE_EVENT_INSTANCE.wait();
    FLOG_INFO("wait rootservice event history success");

    FLOG_INFO("begin to wait kv global cache");
    ObKVGlobalCache::get_instance().wait();
    FLOG_INFO("wait kv global cache success");

    FLOG_INFO("begin to wait clock generator");
    ObClockGenerator::get_instance().wait();
    FLOG_INFO("wait clock generator success");

    FLOG_INFO("begin to wait storage ha diagnose");
    ObStorageHADiagService::instance().wait();
    FLOG_INFO("wait storage ha diagnose success");

    gctx_.status_ = SS_STOPPED;
    FLOG_INFO("[OBSERVER_NOTICE] wait observer end", KR(ret));
    if (OB_SUCCESS != fail_ret) {
      LOG_DBA_ERROR_V2(OB_SERVER_WAIT_FAIL, fail_ret, "observer process wait fail. "
                       "you may find solutions in previous error logs or seek help from official technicians.");
    } else {
      LOG_DBA_INFO_V2(OB_SERVER_WAIT_SUCCESS, "observer process wait succcess.");
    }
  }

  return ret;
}

int ObServer::init_tz_info_mgr()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tenant_timezone_mgr_.init(sql_proxy_, self_addr_, schema_service_))) {
    LOG_ERROR("tenant_timezone_mgr_ init failed", K_(self_addr), KR(ret));
  }
  return ret;
}

int ObServer::init_config()
{
  int ret = OB_SUCCESS;
  bool has_config_file = true;

  if (OB_FAIL(ODV_MGR.init(true /*enable_compatible_monotonic*/))) {
    LOG_ERROR("fail to init data_version_mgr", KR(ret));
  } else if (OB_FAIL(ODV_MGR.load_from_file())) {
    LOG_ERROR("failed to load data_version_mgr file", KR(ret));
  } else {
    // set dump path
    const char *dump_path = "etc/observer.config.bin";
    config_mgr_.set_dump_path(dump_path);
    if (OB_FILE_NOT_EXIST == (ret = config_mgr_.load_config())) {
      has_config_file = false;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_ERROR("load config from file failed", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_opts_config(has_config_file))) {
    LOG_ERROR("init opts config failed", KR(ret));
  } else {
    config_.print();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_local_ip_and_devname())) {
    LOG_ERROR("init local_ip and devname failed", KR(ret));
  } else if (!is_arbitration_mode() && OB_FAIL(config_.strict_check_special())) {
    LOG_ERROR("some config setting is not valid", KR(ret));
  } else if (OB_FAIL(GMEMCONF.reload_config(config_))) {
    LOG_ERROR("reload memory config failed", KR(ret));
  } else if (!is_arbitration_mode() && OB_FAIL(set_running_mode())) {
    LOG_ERROR("set running mode failed", KR(ret));
  } else if (OB_FAIL(init_self_addr())) {
    LOG_ERROR("init self_addr failed", KR(ret));
  } else if (is_arbitration_mode()) {
    // arbitration mode, dump config params to file directly
    if (OB_FAIL(config_mgr_.dump2file())) {
      LOG_ERROR("config_mgr_ dump2file failed", KR(ret));
    } else {
      LOG_INFO("config_mgr_ dump2file success", KR(ret));
    }
  } else if (OB_FAIL(init_config_module())) {
    LOG_ERROR("init config module failed", KR(ret));
  } else {
    lib::g_runtime_enabled = true;
  }

  return ret;
}

int ObServer::init_opts_config(bool has_config_file)
{
  int ret = OB_SUCCESS;

  if (opts_.rpc_port_) {
    config_.rpc_port = opts_.rpc_port_;
    config_.rpc_port.set_version(start_time_);
  }

  if (opts_.mysql_port_) {
    config_.mysql_port = opts_.mysql_port_;
    config_.mysql_port.set_version(start_time_);
  }

  if (opts_.local_ip_ && strlen(opts_.local_ip_) > 0) {
    config_.local_ip.set_value(opts_.local_ip_);
    config_.local_ip.set_version(start_time_);
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

  if (opts_.startup_mode_) {
    config_.ob_startup_mode.set_value(opts_.startup_mode_);
    config_.ob_startup_mode.set_version(start_time_);
    LOG_INFO("mode is not null", "mode", opts_.startup_mode_);
  }
  // update gctx_.startup_mode_
  if (FAILEDx(parse_mode())) {
    LOG_ERROR("parse_mode failed", KR(ret));
  }

  config_.syslog_level.set_value(OB_LOGGER.get_level_str());

  if (opts_.optstr_ && strlen(opts_.optstr_) > 0) {
    if (FAILEDx(config_.add_extra_config(opts_.optstr_, start_time_))) {
      LOG_ERROR("invalid config from cmdline options", K(opts_.optstr_), KR(ret));
    }
  }

  if (opts_.appname_ && strlen(opts_.appname_) > 0) {
    config_.cluster.set_value(opts_.appname_);
    config_.cluster.set_version(start_time_);
    if (FAILEDx(set_cluster_name_hash(ObString::make_string(opts_.appname_)))) {
      LOG_WARN("failed to set_cluster_name_hash", KR(ret), "cluster_name", opts_.appname_,
                                                  "cluster_name_len", strlen(opts_.appname_));
    }
  }

  if (opts_.cluster_id_ >= 0) {
    // Strictly here, everything that is less than zero is ignored, not when it is equal to -1.
    config_.cluster_id = opts_.cluster_id_;
    config_.cluster_id.set_version(start_time_);
  }
  if (config_.cluster_id.get_value() > 0) {
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

  return ret;
}

int ObServer::init_local_ip_and_devname()
{
  int ret = OB_SUCCESS;

  // local_ip is a critical parameter, if it is set, then verify it; otherwise, set it via devname.
  if (strlen(config_.local_ip) > 0) {
    char if_name[MAX_IFNAME_LENGTH] = { '\0' };
    bool has_found = false;
    if (OB_SUCCESS != obsys::ObNetUtil::get_ifname_by_addr(config_.local_ip, if_name, sizeof(if_name), has_found)) {
      // if it is incorrect, then ObServer start but log a error.
      LOG_DBA_WARN_V2(OB_SERVER_GET_IFNAME_FAIL, OB_ERR_OBSERVER_START,
                        "get ifname by local_ip failed. ",
                        "local_ip is ", config_.local_ip.get_value(),
                        ". [suggestion] Verify if your local IP address is a virtual one.");
    } else if (false == has_found) {
      LOG_DBA_ERROR_V2(OB_SERVER_SET_LOCAL_IP_FAIL, OB_ERR_OBSERVER_START,
                        "local_ip set failed, please check your local_ip. ",
                        "local_ip is ", config_.local_ip.get_value(),
                        ". [suggestion] Verify if your local IP is right. ");
    } else if (0 != strcmp(config_.devname, if_name)) {
      config_.devname.set_value(if_name);
      config_.devname.set_version(start_time_);
      // this is done to ensure the consistency of local_ip and devname.
      LOG_DBA_WARN_V2(OB_SERVER_DEVICE_NAME_MISMATCH, OB_ITEM_NOT_MATCH,
          "the devname has been rewritten, and the new value comes from local_ip, old value: ",
          config_.devname.get_value(), " new value: ", if_name, " local_ip: ", config_.local_ip.get_value());
    }
  } else {
    if (config_.use_ipv6) {
      char ipv6[MAX_IP_ADDR_LENGTH] = { '\0' };
      if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv6(config_.devname, ipv6, sizeof(ipv6)))) {
        LOG_ERROR("get ipv6 address by devname failed", "devname",
            config_.devname.get_value(), KR(ret));
      } else {
        config_.local_ip.set_value(ipv6);
        config_.local_ip.set_version(start_time_);
        _LOG_INFO("set local_ip via devname, local_ip:%s, devname:%s.", ipv6, config_.devname.get_value());
      }
    } else {
      uint32_t ipv4_net = 0;
      char ipv4[INET_ADDRSTRLEN] = { '\0' };
      if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv4(config_.devname, ipv4_net))) {
        LOG_ERROR("get ipv4 address by devname failed", "devname",
            config_.devname.get_value(), KR(ret));
      } else if (nullptr == inet_ntop(AF_INET, (void *)&ipv4_net, ipv4, sizeof(ipv4))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("call inet_ntop failed", K(ipv4_net), K(errno), KERRMSG, KR(ret));
      } else {
        config_.local_ip.set_value(ipv4);
        config_.local_ip.set_version(start_time_);
        _LOG_INFO("set local_ip via devname, local_ip:%s, devname:%s.", ipv4, config_.devname.get_value());
      }
    }
  }

  return ret;
}

int ObServer::init_self_addr()
{
  int ret = OB_SUCCESS;

  int32_t local_port = static_cast<int32_t>(config_.rpc_port);
  if (strlen(config_.local_ip) > 0) {
    self_addr_.set_ip_addr(config_.local_ip, local_port);
  } else {
    if (config_.use_ipv6) {
      char ipv6[MAX_IP_ADDR_LENGTH] = { '\0' };
      if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv6(config_.devname, ipv6, sizeof(ipv6)))) {
        LOG_ERROR("get ipv6 address by devname failed", "devname",
            config_.devname.get_value(), KR(ret));
      } else {
        self_addr_.set_ip_addr(ipv6, local_port);
      }
    } else {
      uint32_t ipv4_net = 0;
      if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv4(config_.devname, ipv4_net))) {
        LOG_ERROR("get ipv4 address by devname failed", "devname",
            config_.devname.get_value(), KR(ret));
      } else {
        int32_t ipv4 = ntohl(ipv4_net);
        self_addr_.set_ipv4_addr(ipv4, local_port);
      }
    }
  }

  if (OB_SUCC(ret)) {
    const char *syslog_file_info = ObServerUtils::build_syslog_file_info(self_addr_);
    OB_LOGGER.set_new_file_info(syslog_file_info);
    LOG_INFO("Build basic information for each syslog file", "info", syslog_file_info);

    // initialize self address
    obrpc::ObRpcProxy::myaddr_ = self_addr_;
    LOG_INFO("my addr", K_(self_addr));
    config_.self_addr_ = self_addr_;
  }

  return ret;
}

int ObServer::init_config_module()
{
  int ret = OB_SUCCESS;

  omt::UpdateTenantConfigCb update_tenant_config_cb =
    [&](uint64_t tenant_id)-> void
  {
    multi_tenant_.update_tenant_config(tenant_id);
  };
  // initialize configure module
  if (!self_addr_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("local address isn't valid", K(self_addr_), KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("init timer fail", KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::FreezeTimer))) {
    LOG_ERROR("init freeze timer fail", KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::SqlMemTimer))) {
    LOG_ERROR("init sql memory manger timer fail", KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerTracerTimer))) {
    LOG_ERROR("fail to init server trace timer", KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::CTASCleanUpTimer))) {
    LOG_ERROR("fail to init ctas clean up timer", KR(ret));
  } else if (OB_FAIL(config_mgr_.base_init())) {
    LOG_ERROR("config_mgr_ base_init failed", KR(ret));
  } else if (OB_FAIL(config_mgr_.init(sql_proxy_, self_addr_))) {
    LOG_ERROR("config_mgr_ init failed", K_(self_addr), KR(ret));
  } else if (OB_FAIL(tenant_config_mgr_.init(sql_proxy_, self_addr_,
                      &config_mgr_, update_tenant_config_cb))) {
    LOG_ERROR("tenant_config_mgr_ init failed", K_(self_addr), KR(ret));
  } else if (OB_FAIL(tenant_config_mgr_.add_config_to_existing_tenant(opts_.optstr_))) {
    LOG_ERROR("tenant_config_mgr_ add_config_to_existing_tenant failed", KR(ret));
  }

  return ret;
}

int ObServer::set_running_mode()
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = GMEMCONF.get_server_memory_limit();
  const int64_t cnt = GCONF.cpu_count;
  const int64_t cpu_cnt = cnt > 0 ? cnt : common::get_cpu_num();
  if (memory_limit < lib::ObRunningModeConfig::MINI_MEM_LOWER) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_ERROR("memory limit too small", KR(ret), K(memory_limit));
  } else if (memory_limit < lib::ObRunningModeConfig::MINI_MEM_UPPER) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("observer start with mini_mode", K(memory_limit));
    lib::update_mini_mode(memory_limit, cpu_cnt);
  } else {
    lib::update_mini_mode(memory_limit, cpu_cnt);
  }
  _OB_LOG(INFO, "mini mode: %s", lib::is_mini_mode() ? "true" : "false");
  return ret;
}

int ObServer::init_pre_setting()
{
  int ret = OB_SUCCESS;

  reset_mem_leak_checker_label(GCONF.leak_mod_to_check.str());
  ObMallocSampleLimiter::set_interval(GCONF._max_malloc_sample_interval,
                                      GCONF._min_malloc_sample_interval);
  enable_memleak_light_backtrace(GCONF._enable_memleak_light_backtrace);

  // oblog configuration
  if (OB_SUCC(ret)) {
    const int max_log_cnt = static_cast<int32_t>(config_.max_syslog_file_count);
    const bool record_old_log_file = config_.enable_syslog_recycle;
    const bool log_warn = config_.enable_syslog_wf;
    const bool enable_async_syslog = config_.enable_async_syslog;
    const int64_t max_disk_size = config_.syslog_disk_size;
    const int64_t min_uncompressed_count = config_.syslog_file_uncompressed_count;
    const char *compress_func_ptr = config_.syslog_compress_func.str();
    OB_LOGGER.set_max_file_index(max_log_cnt);
    OB_LOGGER.set_record_old_log_file(record_old_log_file);
    LOG_INFO("Whether record old log file", K(record_old_log_file));
    OB_LOGGER.set_log_warn(log_warn);
    LOG_INFO("Whether log warn", K(log_warn));
    OB_LOGGER.set_enable_async_log(enable_async_syslog);
    OB_LOG_COMPRESSOR.set_max_disk_size(max_disk_size);
    LOG_INFO("Whether compress syslog file", K(compress_func_ptr));
    OB_LOG_COMPRESSOR.set_compress_func(compress_func_ptr);
    OB_LOG_COMPRESSOR.set_min_uncompressed_count(min_uncompressed_count);
    LOG_INFO("init log config", K(record_old_log_file), K(log_warn), K(enable_async_syslog),
             K(max_disk_size), K(compress_func_ptr), K(min_uncompressed_count));
    if (0 == max_log_cnt) {
      LOG_INFO("won't recycle log file");
    } else {
      LOG_INFO("recycle log file", "count", max_log_cnt);
    }
  }

  // task controller(log rate limiter)
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTaskController::get().init())) {
      LOG_ERROR("init task controller fail", KR(ret));
    } else {
      ObTaskController::get().set_log_rate_limit(config_.syslog_io_bandwidth_limit);
      ObTaskController::get().set_diag_per_error_limit(config_.diag_syslog_per_error_limit);
      ObTaskController::get().switch_task(share::ObTaskType::GENERIC);
    }
  }

  // total memory limit
  if (OB_SUCC(ret)) {
    const int64_t limit_memory = GMEMCONF.get_server_memory_limit();
    const int64_t reserved_memory = std::min(config_.cache_wash_threshold.get_value(),
        static_cast<int64_t>(static_cast<double>(limit_memory) * KVCACHE_FACTOR));
    const int64_t reserved_urgent_memory = config_.memory_reserved;
    const int64_t need_memory_size = is_arbitration_mode() ? LEAST_MEMORY_SIZE : LEAST_MEMORY_SIZE_FOR_NORMAL_MODE;
    if (need_memory_size > limit_memory) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("memory limit for oceanbase isn't sufficient",
                K(need_memory_size),
                "limit to", limit_memory,
                "sys mem", get_phy_mem_size(),
                K(reserved_memory),
                K(reserved_urgent_memory),
                KR(ret));
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
    const int64_t stack_size = std::max(1L << 19, static_cast<int64_t>(GCONF.stack_size));
    LOG_INFO("set stack_size", K(stack_size));
    global_thread_stack_size = stack_size - SIG_STACK_SIZE - ACHUNK_PRESERVE_SIZE;
  }
  if (OB_SUCC(ret) && GCONF.use_ipv6) {
    enable_use_ipv6();
  }
  return ret;
}

int ObServer::init_sql_proxy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_conn_pool_.init(&schema_service_,
                                  &sql_engine_,
                                  &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator(),
                                  &config_))) {
    LOG_ERROR("init sql connection pool failed", KR(ret));
  } else if (OB_FAIL(ddl_conn_pool_.init(&schema_service_,
                                  &sql_engine_,
                                  &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator(),
                                  &config_,
                                  true/*use static type engine*/))) {
    LOG_ERROR("init sql connection pool failed", KR(ret));
  } else if (OB_FAIL(sql_proxy_.init(&sql_conn_pool_))) {
    LOG_ERROR("init sql proxy failed", KR(ret));
  } else if (OB_FAIL(res_inner_conn_pool_.init(&schema_service_,
                                  &sql_engine_,
                                  &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator(),
                                  &config_))) {
    LOG_WARN("init res inner connection pool failed", KR(ret));
  } else if (OB_FAIL(ddl_sql_proxy_.init(&ddl_conn_pool_))) {
    LOG_ERROR("init ddl sql proxy failed", KR(ret));
  } else if (OB_FAIL(ddl_oracle_sql_proxy_.init(&ddl_conn_pool_))) {
    LOG_ERROR("init ddl oracle sql proxy failed", KR(ret));
  } else if (OB_FAIL(dblink_proxy_.init(&dblink_conn_pool_))) {
    LOG_ERROR("init dblink proxy failed", KR(ret));
  }
  return ret;
}

int ObServer::init_io()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.init(GCONF.data_dir,
                                         GCONF.cluster.str(),
                                         GCONF.cluster_id.get_value(),
                                         GCONF.zone.str(),
                                         GCTX.self_addr()))) {
    LOG_ERROR("init OB_FILE_SYSTEM_ROUTER fail", KR(ret));
  }

  if (OB_SUCC(ret)) {
    static const double IO_MEMORY_RATIO = 0.2;
    if (OB_FAIL(ObIOManager::get_instance().init(GMEMCONF.get_reserved_server_memory() * IO_MEMORY_RATIO))) {
      LOG_ERROR("init io manager fail, ", KR(ret));
    } else {
      ObIOConfig io_config;
      int64_t cpu_cnt = GCONF.cpu_count;
      if (cpu_cnt <= 0) {
        cpu_cnt = common::get_cpu_num();
      }
      io_config.disk_io_thread_count_ = GCONF.disk_io_thread_count;
      const int64_t max_io_depth = 256;
      if (OB_FAIL(ObIOManager::get_instance().set_io_config(io_config))) {
        LOG_ERROR("config io manager fail, ", KR(ret));
      } else {
        //allow load benchmark fail, please ignore return code.
        if (OB_FAIL(ObIOCalibration::get_instance().init())) {
          LOG_WARN("init io benchmark fail, ", KR(ret));
        }

        storage_env_.data_dir_ = OB_FILE_SYSTEM_ROUTER.get_data_dir();
        storage_env_.sstable_dir_ = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
        storage_env_.default_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;  // 2MB

        // log
        storage_env_.log_spec_.log_dir_ = OB_FILE_SYSTEM_ROUTER.get_slog_dir();
        storage_env_.log_spec_.max_log_file_size_ = ObLogConstants::MAX_LOG_FILE_SIZE;
        storage_env_.clog_dir_ = OB_FILE_SYSTEM_ROUTER.get_clog_dir();

        // cache
        storage_env_.index_block_cache_priority_ = config_.index_block_cache_priority;
        storage_env_.user_block_cache_priority_ = config_.user_block_cache_priority;
        storage_env_.user_row_cache_priority_ = config_.user_row_cache_priority;
        storage_env_.fuse_row_cache_priority_ = config_.fuse_row_cache_priority;
        storage_env_.bf_cache_priority_ = config_.bf_cache_priority;
        storage_env_.storage_meta_cache_priority_ = config_.storage_meta_cache_priority;
        storage_env_.bf_cache_miss_count_threshold_ = config_.bf_cache_miss_count_threshold;

        // policy
        storage_env_.clog_file_spec_ = OB_FILE_SYSTEM_ROUTER.get_clog_file_spec();
        storage_env_.slog_file_spec_ = OB_FILE_SYSTEM_ROUTER.get_slog_file_spec();

        int64_t data_disk_size = 0;
        int64_t log_disk_size = 0;
        int64_t data_disk_percentage = 0;
        int64_t log_disk_percentage = 0;

        if (OB_SUCC(ret) && OB_FAIL(log_block_mgr_.init(storage_env_.clog_dir_))) {
          LOG_ERROR("log block mgr init failed", KR(ret));
        } else if (OB_FAIL(ObServerUtils::cal_all_part_disk_size(config_.datafile_size,
                                                  config_.log_disk_size,
                                                  config_.datafile_disk_percentage,
                                                  config_.log_disk_percentage,
                                                  data_disk_size,
                                                  log_disk_size,
                                                  data_disk_percentage,
                                                  log_disk_percentage))) {
          LOG_ERROR("cal_all_part_disk_size failed", KR(ret));
        } else {
          if (log_block_mgr_.is_reserved()) {
            int64_t clog_pool_in_use = 0;
            int64_t clog_pool_total_size = 0;
            if (OB_FAIL(log_block_mgr_.get_disk_usage(clog_pool_in_use, clog_pool_total_size))) {
               LOG_ERROR("get clog disk size failed", KR(ret));
            } else {
              log_disk_size = clog_pool_total_size;
            }
          }
        }
        if (OB_SUCC(ret)) {
          storage_env_.data_disk_size_ = data_disk_size;
          storage_env_.data_disk_percentage_ = data_disk_percentage;
          storage_env_.log_disk_size_ = log_disk_size;
          storage_env_.log_disk_percentage_ = log_disk_percentage;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObIODeviceWrapper::get_instance().init(
                storage_env_.data_dir_,
                storage_env_.sstable_dir_,
                storage_env_.default_block_size_,
                storage_env_.data_disk_percentage_,
                storage_env_.data_disk_size_))) {
            LOG_ERROR("fail to init io device wrapper", KR(ret), K_(storage_env));
          } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(THE_IO_DEVICE,
                                                                            io_config.disk_io_thread_count_,
                                                                            io_config.disk_io_thread_count_ / 2,
                                                                            max_io_depth))) {
            LOG_ERROR("add device channel failed", KR(ret));
          }
        }
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
  restore_ctx_.ls_table_operator_ = &lst_operator_;
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
    LOG_ERROR("fail get interrupt mgr instance", KR(ret));
  } else if (OB_FAIL(mgr->init(get_self(), &interrupt_proxy_))) {
    LOG_ERROR("fail init interrupt mgr", KR(ret));
  }
  return ret;
}

int ObServer::init_zlib_lite_compressor()
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor = nullptr;
  ZLIB_LITE::ObZlibLiteCompressor *zlib_lite_compressor = nullptr;
  ret = ObCompressorPool::get_instance().get_compressor(ZLIB_LITE_COMPRESSOR, compressor);
  if (OB_FAIL(ret) || OB_ISNULL(compressor)) {
    LOG_ERROR("failed to get zlib lite compressor");
  } else if (FALSE_IT(zlib_lite_compressor = static_cast<ZLIB_LITE::ObZlibLiteCompressor *>(compressor))) {
  } else if (OB_FAIL(zlib_lite_compressor->init(0))) { // 0 means preserve 0 qpl job
    LOG_ERROR("failed to init zlib lite compressor", K(ret));
  } else {
    const char *zlib_lite_compress_method = zlib_lite_compressor->compression_method();
    LOG_INFO("zlib lite compressor init success", KCSTRING(zlib_lite_compress_method));
  }
  return ret;
}

void ObServer::deinit_zlib_lite_compressor()
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor = nullptr;
  ZLIB_LITE::ObZlibLiteCompressor *zlib_lite_compressor = nullptr;
  ret = ObCompressorPool::get_instance().get_compressor(ZLIB_LITE_COMPRESSOR, compressor);
  if (OB_FAIL(ret) || OB_ISNULL(compressor)) {
    LOG_ERROR("failed to get zlib lite compressor");
  } else if (FALSE_IT(zlib_lite_compressor = static_cast<ZLIB_LITE::ObZlibLiteCompressor *>(compressor))) {
  } else {
    zlib_lite_compressor->deinit();
  }
  LOG_INFO("zlib lite compressor deinit done");
}

int ObServer::init_loaddata_global_stat()
{
  int ret = OB_SUCCESS;
  ObGlobalLoadDataStatMap *map = ObGlobalLoadDataStatMap::getInstance();
  if (OB_ISNULL(map)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail allocate load data map for status", KR(ret));
  } else if (OB_FAIL(map->init())) {
    LOG_ERROR("fail init load data map", KR(ret));
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
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(storage_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(rs_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(executor_proxy_))) {
    LOG_ERROR("get rpc proxy fail");
  } else if (OB_FAIL(net_frame_.get_proxy(load_data_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(external_table_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(interrupt_proxy_))) {
    LOG_ERROR("get rpc proxy fail");
  } else if (OB_FAIL(net_frame_.get_proxy(dbms_job_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(inner_sql_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(dbms_sched_job_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(net_frame_.get_proxy(table_rpc_proxy_))) {
    LOG_ERROR("get rpc proxy fail", KR(ret));
  } else if (OB_FAIL(batch_rpc_.init(net_frame_.get_batch_rpc_req_transport(),
                                     net_frame_.get_high_prio_req_transport(),
                                     self_addr_))) {
    LOG_ERROR("init batch rpc failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::BRPC, batch_rpc_))) {
    STORAGE_LOG(WARN, "fail to start batch rpc proxy", KR(ret));
  } else if (OB_FAIL(rl_mgr_.init(self_addr_, &net_frame_, &gctx_))) {
    LOG_ERROR("init rl_mgr failed", KR(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::RLMGR, rl_mgr_))) {
    STORAGE_LOG(ERROR, "fail to start rl_mgr", KR(ret));
  } else {
    srv_rpc_proxy_.set_server(get_self());
    rs_rpc_proxy_.set_rs_mgr(rs_mgr_);
  }

  return ret;
}

int ObServer::init_multi_tenant()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(multi_tenant_.init(self_addr_,
                                 &sql_proxy_))) {
    LOG_ERROR("init multi tenant fail", KR(ret));

  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(duty_task_.schedule(lib::TGDefIDs::ServerGTimer))) {
      LOG_ERROR("schedule tenant duty task fail", KR(ret));
    } else if (OB_FAIL(sql_mem_task_.schedule(lib::TGDefIDs::SqlMemTimer))) {
      LOG_ERROR("schedule tenant sql memory manager task fail", KR(ret));
    }
  }

  return ret;
}

int ObServer::init_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_service_.init(&sql_proxy_, &dblink_proxy_, &config_,
                                   OB_MAX_VERSION_COUNT,
                                   OB_MAX_VERSION_COUNT_FOR_MERGE))) {
    LOG_WARN("init schema_service_ fail", KR(ret));
  }

  return ret;
}

int ObServer::init_autoincrement_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAutoincrementService::get_instance().init(self_addr_,
                                                         &sql_proxy_,
                                                         &srv_rpc_proxy_,
                                                         &schema_service_,
                                                         net_frame_.get_req_transport()))) {
    LOG_ERROR("init autoincrement_service_ fail", KR(ret));
  }
  return ret;
}

int ObServer::init_table_lock_rpc_client()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLockRpcClient::get_instance().init())) {
    LOG_ERROR("init table_lock_rpc_client fail", KR(ret));
  }
  return ret;
}

int ObServer::init_tablet_autoincrement_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletAutoincrementService::get_instance().init())) {
    LOG_WARN("init tablet_autoincrement_service_ fail", KR(ret));
  } else if (OB_FAIL(ObTabletAutoincSeqRpcHandler::get_instance().init())) {
    LOG_WARN("init tablet autoinc seq rpc handler fail", K(ret));
  }
  return ret;
}

int ObServer::init_global_kvcache()
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = ObKVGlobalCache::get_instance().get_suitable_bucket_num();
  const int64_t max_cache_size = MIN(sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE), ObKVGlobalCache::DEFAULT_MAX_CACHE_SIZE);
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(&ObTenantMemLimitGetter::get_instance(),
                                                   bucket_num,
                                                   max_cache_size))) {
    LOG_WARN("Fail to init ObKVGlobalCache, ", KR(ret));
  } else if (OB_FAIL(ObResourceMgr::get_instance().set_cache_washer(
      ObKVGlobalCache::get_instance()))) {
    LOG_ERROR("Fail to set_cache_washer", KR(ret));
  }

  return ret;
}

int ObServer::init_ob_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_service_.init(sql_proxy_, server_tracer_))) {
    LOG_ERROR("oceanbase service init failed", KR(ret));
  }
  return ret;
}

int ObServer::init_root_service()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(root_service_.init(
                 config_, config_mgr_, srv_rpc_proxy_,
                 rs_rpc_proxy_, self_addr_, sql_proxy_,
                 restore_ctx_, rs_mgr_, &schema_service_, lst_operator_))) {
    LOG_ERROR("init root service failed", K(ret));
  }

  return ret;
}

int ObServer::init_sql()
{
  int ret = OB_SUCCESS;

  LOG_INFO("init sql");
  if (OB_FAIL(session_mgr_.init())) {
    LOG_ERROR("init sql session mgr fail");
  } else if (OB_FAIL(conn_res_mgr_.init(schema_service_))) {
    LOG_ERROR("init user resource mgr failed", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, session_mgr_,
                                 ObSQLSessionMgr::SCHEDULE_PERIOD, true))) {
    LOG_ERROR("tier schedule fail");
  } else {
    LOG_INFO("init sql session mgr done");
    LOG_INFO("init sql location cache done");
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql_engine_.init(
                    &ObOptStatManager::get_instance(),
                    net_frame_.get_req_transport(),
                    &vt_data_service_,
                    self_addr_,
                    rs_mgr_))) {
      LOG_ERROR("init sql engine failed", KR(ret));
    } else {
      LOG_INFO("init sql engine done");
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == dtl::ObDtl::instance()) {
      ret = OB_INIT_FAIL;
      LOG_ERROR("allocate DTL service fail", KR(ret));
    } else if (OB_FAIL(net_frame_.get_proxy(DTL.get_rpc_proxy()))) {
      LOG_ERROR("initialize DTL RPC proxy fail", KR(ret));
    } else if (OB_FAIL(DTL.init())) {
      LOG_ERROR("fail initialize DTL instance", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLibXml2SaxHandler::init();
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("init sql done");
  } else {
    LOG_ERROR("init sql fail", KR(ret));
  }
  return ret;
}

int ObServer::init_sql_runner()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(executor_rpc_.init(&executor_proxy_, &batch_rpc_))) {
    LOG_ERROR("init executor rpc fail", K(ret));
  } else if (OB_FAIL(ObDASTaskResultGCRunner::schedule_timer_task())) {
    LOG_WARN("schedule das result gc runner failed", KR(ret));
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
    LOG_ERROR("init sequence engine failed", KR(ret));
  } else {
    LOG_INFO("init sequence engine done");
  }
  return ret;
}

int ObServer::init_pl()
{
  int ret = OB_SUCCESS;

  LOG_INFO("init pl");
  if (OB_FAIL(pl_engine_.init(sql_proxy_))) {
    LOG_ERROR("init pl engine failed", KR(ret));
  } else {
    LOG_INFO("init pl engine done");
  }
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
  gctx_.lst_operator_ = &lst_operator_;
  gctx_.tablet_operator_ = &tablet_operator_;
  gctx_.srv_rpc_proxy_ = &srv_rpc_proxy_;
  gctx_.storage_rpc_proxy_ = &storage_rpc_proxy_;
  gctx_.dbms_job_rpc_proxy_ = &dbms_job_rpc_proxy_;
  gctx_.inner_sql_rpc_proxy_ = &inner_sql_rpc_proxy_;
  gctx_.dbms_sched_job_rpc_proxy_ = &dbms_sched_job_rpc_proxy_;
  gctx_.rs_rpc_proxy_ = &rs_rpc_proxy_;
  gctx_.load_data_proxy_ = &load_data_proxy_;
  gctx_.external_table_proxy_ = &external_table_proxy_;
  gctx_.sql_proxy_ = &sql_proxy_;
  gctx_.ddl_sql_proxy_ = &ddl_sql_proxy_;
  gctx_.ddl_oracle_sql_proxy_ = &ddl_oracle_sql_proxy_;
  gctx_.dblink_proxy_ = &dblink_proxy_;
  gctx_.res_inner_conn_pool_ = &res_inner_conn_pool_;
  gctx_.executor_rpc_ =  &executor_rpc_;
  gctx_.self_addr_seq_.set_addr(self_addr_);
  gctx_.rs_mgr_ = &rs_mgr_;
  gctx_.bandwidth_throttle_ = &bandwidth_throttle_;
  gctx_.vt_par_ser_ = &vt_data_service_;
  gctx_.et_access_service_ = &et_access_service_;
  gctx_.session_mgr_ = &session_mgr_;
  gctx_.sql_engine_ = &sql_engine_;
  gctx_.pl_engine_ = &pl_engine_;
  gctx_.conn_res_mgr_ = &conn_res_mgr_;
  gctx_.omt_ = &multi_tenant_;
  gctx_.vt_iter_creator_ = &vt_data_service_.get_vt_iter_factory().get_vt_iter_creator();
  gctx_.location_service_ = &location_service_;
  gctx_.start_time_ = start_time_;
  gctx_.warm_up_start_time_ = &warm_up_start_time_;
  gctx_.status_ = SS_INIT;
  gctx_.rs_server_status_ = RSS_INVALID;
  gctx_.start_service_time_ = 0;
  gctx_.ssl_key_expired_time_ = 0;
  gctx_.diag_ = &diag_;
  gctx_.scramble_rand_ = &scramble_rand_;
  gctx_.init();
  gctx_.weak_read_service_ = &weak_read_service_;
  gctx_.table_service_ = &table_service_;
  gctx_.cgroup_ctrl_ = &cgroup_ctrl_;
  gctx_.schema_status_proxy_ = &schema_status_proxy_;
  gctx_.net_frame_ = &net_frame_;
  gctx_.rl_mgr_ = &rl_mgr_;
  gctx_.batch_rpc_ = &batch_rpc_;
  gctx_.server_tracer_ = &server_tracer_;
  gctx_.locality_manager_ = &locality_manager_;
  gctx_.disk_reporter_ = &disk_usage_report_task_;
  gctx_.log_block_mgr_ = &log_block_mgr_;
#ifdef OB_BUILD_ARBITRATION
  gctx_.arb_gcs_ = &arb_gcs_;
#endif
  (void)gctx_.set_upgrade_stage(obrpc::OB_UPGRADE_STAGE_INVALID);
  gctx_.wr_service_ = &wr_service_;
  gctx_.startup_accel_handler_ = &startup_accel_handler_;

  gctx_.flashback_scn_ = opts_.flashback_scn_;
  gctx_.server_id_ = config_.observer_id;
  if (is_valid_server_id(gctx_.server_id_)) {
    LOG_INFO("this observer has had a valid server_id", K(gctx_.server_id_));
  }
  if ((PHY_FLASHBACK_MODE == gctx_.startup_mode_ || PHY_FLASHBACK_VERIFY_MODE == gctx_.startup_mode_)
      && 0 >= gctx_.flashback_scn_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid flashback scn in flashback mode", KR(ret),
              "server_mode", gctx_.startup_mode_,
              "flashback_scn", gctx_.flashback_scn_);
  } else {
    gctx_.inited_ = true;
  }

  return ret;
}

int ObServer::init_version()
{
  return ObClusterVersion::get_instance().init(&config_, &tenant_config_mgr_);
}

int ObServer::init_ts_mgr()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_TS_MGR.init(self_addr_,
                             schema_service_,
                             location_service_,
                             net_frame_.get_req_transport()))) {
    LOG_ERROR("gts cache mgr init failed", K_(self_addr), KR(ret));
  } else {
    LOG_INFO("gts cache mgr init success");
  }

  return ret;
}

int ObServer::init_px_target_mgr()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_PX_TARGET_MGR.init(self_addr_, server_tracer_))) {
    LOG_ERROR("px target mgr init failed", K(self_addr_), KR(ret));
  } else {
    LOG_INFO("px target mgr init success");
  }
  return ret;
}

int ObServer::init_storage()
{
  int ret = OB_SUCCESS;

  bool clogdir_is_empty = false;

  if (OB_SUCC(ret)) {
    int64_t total_log_disk_size = 0;
    int64_t log_disk_in_use = 0;
    // Check if the clog directory is empty
    if (OB_FAIL(log_block_mgr_.get_disk_usage(
            log_disk_in_use, total_log_disk_size))) {
      LOG_ERROR("ObServerLogBlockMgr get_disk_usage failed", K(ret));
    } else if (0 == log_disk_in_use
        && OB_FAIL(logservice::ObServerLogBlockMgr::check_clog_directory_is_empty(
            OB_FILE_SYSTEM_ROUTER.get_clog_dir(), clogdir_is_empty))) {
      LOG_ERROR("is_dir_empty fail", K(ret));
    } else if (clogdir_is_empty) {
      LOG_INFO("clog dir is empty");
    } else {
      clogdir_is_empty = log_disk_in_use == 0;
    }
  }

  if (OB_SUCC(ret)) {
    is_log_dir_empty_ = clogdir_is_empty;
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
      LOG_ERROR("invalid redundancy level", KR(ret), K(redundancy_level));
    }
  }

  if (OB_SUCC(ret)) {
    storage_env_.ethernet_speed_ = ethernet_speed_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_STORE_CACHE.init(storage_env_.index_block_cache_priority_,
                                    storage_env_.user_block_cache_priority_,
                                    storage_env_.user_row_cache_priority_,
                                    storage_env_.fuse_row_cache_priority_,
                                    storage_env_.bf_cache_priority_,
                                    storage_env_.bf_cache_miss_count_threshold_,
                                    storage_env_.storage_meta_cache_priority_))) {
      LOG_WARN("Fail to init OB_STORE_CACHE, ", KR(ret), K(storage_env_.data_dir_));
    } else if (OB_FAIL(ObTmpFileManager::get_instance().init())) {
      LOG_WARN("fail to init temp file manager", KR(ret));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(THE_IO_DEVICE,
                                                storage_env_.default_block_size_))) {
      LOG_ERROR("init server block mgr fail", KR(ret));
    } else if (OB_FAIL(disk_usage_report_task_.init(sql_proxy_))) {
      LOG_WARN("fail to init disk usage report task", KR(ret));
    } else if (OB_FAIL(TG_START(lib::TGDefIDs::DiskUseReport))) {
      LOG_WARN("fail to initialize disk usage report timer", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDDLCtrlSpeedHandle::get_instance().init())) {
      LOG_WARN("fail to init ObDDLCtrlSpeedHandle", KR(ret));
    }
  }
  return ret;
}

int ObServer::init_tx_data_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_TX_DATA_KV_CACHE.init("tx_data_kv_cache", 2 /* cache priority */))) {
    LOG_WARN("init OB_TX_DATA_KV_CACHE failed", KR(ret));
  }
  return ret;
}

int ObServer::init_log_kv_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_LOG_KV_CACHE.init(palf::OB_LOG_KV_CACHE_NAME, 1, palf::LOG_CACHE_MEMORY_LIMIT))) {
    LOG_WARN("init OB_LOG_KV_CACHE failed", KR(ret));
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

char* strtrim(char* str)
{
  char* ptr;

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
    } else if (0 == STRCASECMP("bit", p_unit)
               || 0 == STRCASECMP("b", p_unit)) {
      // do nothing
    } else if (0 == STRCASECMP("kbit", p_unit)
               || 0 == STRCASECMP("kb", p_unit)
               || 0 == STRCASECMP("k", p_unit)) {
      value <<= 10;
    } else if ('\0' == *p_unit
               || 0 == STRCASECMP("mbit", p_unit)
               || 0 == STRCASECMP("mb", p_unit)
               || 0 == STRCASECMP("m", p_unit)) {
      // default is meta bit
      value <<= 20;
    } else if (0 == STRCASECMP("gbit", p_unit)
               || 0 == STRCASECMP("gb", p_unit)
               || 0 == STRCASECMP("g", p_unit)) {
      value <<= 30;
    } else {
      valid = false;
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "parse nic rate error", K(str), K(p_unit));
    }
  }
  return value;
}

int ObServer::get_network_speed_from_config_file(int64_t &network_speed)
{
  int ret = OB_SUCCESS;
  const char *nic_rate_path = "etc/nic.rate.config";
  const int64_t MAX_NIC_CONFIG_FILE_SIZE = 1 << 10; // 1KB
  FILE *fp = nullptr;
  char *buf = nullptr;
  static int nic_rate_file_exist = 1;

  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(MAX_NIC_CONFIG_FILE_SIZE + 1,
                                                           ObModIds::OB_BUFFER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buffer failed", LITERAL_K(MAX_NIC_CONFIG_FILE_SIZE), KR(ret));
  } else if (OB_ISNULL(fp = fopen(nic_rate_path, "r"))) {
    if (ENOENT == errno) {
      ret = OB_FILE_NOT_EXIST;
      if (nic_rate_file_exist) {
        LOG_WARN("NIC Config file doesn't exist, auto detecting", K(nic_rate_path), KR(ret));
        nic_rate_file_exist = 0;
      }
    } else {
      ret = OB_IO_ERROR;
      if (EAGAIN == errno) {
        LOG_WARN("Can't open NIC Config file", K(nic_rate_path), K(errno), KR(ret));
      } else {
        LOG_ERROR("Can't open NIC Config file", K(nic_rate_path), K(errno), KR(ret));
      }
    }
  } else {
    if (!nic_rate_file_exist) {
      LOG_INFO("Reading NIC Config file", K(nic_rate_path));
      nic_rate_file_exist = 1;
    }
    memset(buf, 0, MAX_NIC_CONFIG_FILE_SIZE + 1);
    // ignore return value of fread, because ferror can get fread state
    IGNORE_RETURN fread(buf, 1, MAX_NIC_CONFIG_FILE_SIZE, fp);
    char *prate = nullptr;

    if (OB_UNLIKELY(0 != ferror(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Read NIC Config file error", K(nic_rate_path), KR(ret));
    } else if (OB_UNLIKELY(0 == feof(fp))) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("NIC Config file is too long", K(nic_rate_path), KR(ret));
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
          LOG_ERROR("invalid NIC Rate Config", KR(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid NIC Config file", KR(ret));
      }
    } // else

    if (OB_UNLIKELY(0 != fclose(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Close NIC Config file failed", KR(ret));
    }
  } // else
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
      LOG_ERROR("failed to init bandwidth throttle", KR(ret), K(rate), K(network_speed));
    } else {
      LOG_INFO("succeed to init_bandwidth_throttle",
          K(sys_bkgd_net_percentage_),
          K(network_speed),
          K(rate));
      ethernet_speed_ = network_speed;
    }
  }
  return ret;
}

int ObServer::reload_config()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(OB_STORE_CACHE.set_bf_cache_miss_count_threshold(GCONF.bf_cache_miss_count_threshold))) {
    LOG_WARN("set bf_cache_miss_count_threshold fail", KR(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.reset_priority(GCONF.index_block_cache_priority,
                                                   GCONF.user_block_cache_priority,
                                                   GCONF.user_row_cache_priority,
                                                   GCONF.fuse_row_cache_priority,
                                                   GCONF.bf_cache_priority,
                                                   GCONF.storage_meta_cache_priority))) {
    LOG_WARN("set cache priority fail, ", KR(ret));
  } else if (OB_FAIL(reload_bandwidth_throttle_limit(ethernet_speed_))) {
    LOG_WARN("failed to reload_bandwidth_throttle_limit", KR(ret));
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
      LOG_WARN("failed to reset bandwidth throttle", KR(ret), K(rate), K(ethernet_speed_));
    } else {
      LOG_INFO("succeed to reload_bandwidth_throttle_limit",
          "old_percentage", sys_bkgd_net_percentage_,
          "new_percentage", sys_bkgd_net_percentage,
          K(network_speed),
          K(rate));
      sys_bkgd_net_percentage_ = sys_bkgd_net_percentage;
      ethernet_speed_ = network_speed;
    }
  }
  return ret;
}

void ObServer::check_user_tenant_schema_refreshed(const ObIArray<uint64_t> &tenant_ids, const int64_t expire_time)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  LOG_DBA_INFO_V2(OB_SERVER_CHECK_USER_TENANT_SCHEMA_REFRESHED_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "observer check user tenant schema refreshed begin.");

  for (int64_t i = 0; i < tenant_ids.count()
                      && ObTimeUtility::current_time() < expire_time; ++i) {
    tenant_id = tenant_ids.at(i);
    if (OB_ISNULL(gctx_.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is NULL", KR(ret));
    } else {
      bool is_dropped = false;
      bool tenant_schema_refreshed = false;
      while (!tenant_schema_refreshed
          && !stop_
          && !is_dropped
          && ObTimeUtility::current_time() < expire_time) {

        tenant_schema_refreshed = is_user_tenant(tenant_id) ?
                                  gctx_.schema_service_->is_tenant_refreshed(tenant_id) : true;
        if (OB_FAIL(gctx_.schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
          LOG_WARN("fail to check tenant has been dropped at observer startup", KR(ret), K(tenant_id));
        } else if (is_dropped) {
          // ignore
        } else if (!tenant_schema_refreshed) {
          // check wait and retry
          usleep(1000 * 1000);
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            FLOG_INFO("[OBSERVER_NOTICE] Refreshing user tenant schema, need to wait ", K(tenant_id));
          }
          // check success
        } else if (i == tenant_ids.count() - 1) {
          FLOG_INFO("[OBSERVER_NOTICE] Refresh all user tenant schema successfully ", K(tenant_ids));
          // check timeout
        } else if (ObTimeUtility::current_time() > expire_time) {
          FLOG_INFO("[OBSERVER_NOTICE] Refresh user tenant schema timeout ", K(tenant_id));
        } else {
          FLOG_INFO("[OBSERVER_NOTICE] Refresh user tenant schema successfully ", K(tenant_id));
        }
      }
    }
  }
  LOG_DBA_INFO_V2(OB_SERVER_CHECK_USER_TENANT_SCHEMA_REFRESHED_FINISH,
                  DBA_STEP_INC_INFO(server_start),
                  "observer check user tenant schema refreshed finish.");
}

void ObServer::check_log_replay_over(const ObIArray<uint64_t> &tenant_ids, const int64_t expire_time)
{
  LOG_DBA_INFO_V2(OB_SERVER_CHECK_LOG_REPLAY_OVER_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "observer check log replay over begin.");
  for (int64_t i = 0; i < tenant_ids.count()
                      && ObTimeUtility::current_time() < expire_time; ++i) {
    SCN min_version;
    uint64_t tenant_id = tenant_ids.at(i);
    bool can_start_service = false;
    while (!can_start_service
          && !stop_
          && ObTimeUtility::current_time() < expire_time) {
      weak_read_service_.check_tenant_can_start_service(tenant_id, can_start_service, min_version);
        // check wait and retry
      if (!can_start_service) {
        usleep(1000 * 1000);
        // check success
      } else if (i == tenant_ids.count() -1) {
        FLOG_INFO("[OBSERVER_NOTICE] all tenant replay log finished, start to service ", K(tenant_ids));
        // check timeout
      } else if (ObTimeUtility::current_time() > expire_time) {
        FLOG_INFO("[OBSERVER_NOTICE] replay log timeout and force to start service ", K(tenant_id));
      } else {
        // do nothing
      }
    }
  }
  LOG_DBA_INFO_V2(OB_SERVER_CHECK_LOG_REPLAY_OVER_FINISH,
                  DBA_STEP_INC_INFO(server_start),
                  "observer check log replay over finish.");
}

ObServer::ObCTASCleanUpTask::ObCTASCleanUpTask()
: obs_(nullptr), is_inited_(false)
{}

int ObServer::ObCTASCleanUpTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCTASCleanUpTask has already been inited", KR(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObCTASCleanUpTask init with null ptr", KR(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    disable_timeout_check();
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, CLEANUP_INTERVAL, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObCTASCleanUpTask", KR(ret));
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
    LOG_ERROR("ObCTASCleanUpTask has not been inited", KR(ret));
  } else if (false == need_ctas_cleanup) {
    LOG_DEBUG("CTAS cleanup task skipped this time");
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("CTAS cleanup task got null ptr", KR(ret));
  } else if (OB_FAIL(obs_->clean_up_invalid_tables())) {
    LOG_WARN("CTAS clean up task failed", KR(ret));
    ATOMIC_STORE(&obs_->need_ctas_cleanup_, true);
  } else {
    LOG_DEBUG("CTAS clean up task succeed");
  }
}

//Traverse the current session and determine whether the given table schema needs to be deleted according to the session id and last active time
bool ObServer::ObCTASCleanUp::operator()(sql::ObSQLSessionMgr::Key key,
                                         sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if ((ObCTASCleanUp::TEMP_TAB_PROXY_RULE == get_cleanup_type() && get_drop_flag())
      || (ObCTASCleanUp::TEMP_TAB_PROXY_RULE != get_cleanup_type() && false == get_drop_flag())) {
    //do nothing
  } else if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", KR(ret));
  } else if (static_cast<uint64_t>(key.sessid_) == get_session_id()
             || key.proxy_sessid_ == get_session_id()) {
    if (OB_FAIL(sess_info->try_lock_query())) {
      if (OB_UNLIKELY(OB_EAGAIN != ret)) {
        LOG_WARN("fail to try lock query", KR(ret));
      } else {
        ret = OB_SUCCESS;
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true); //1, The current session is in use, there is suspicion, need to continue to check in the next scheduling
        LOG_DEBUG("try lock query fail with code OB_EGAIN",
            K(sess_info->get_sessid()), K(sess_info->get_sessid_for_table()));
      }
      set_drop_flag(false);
    } else if (ObCTASCleanUp::CTAS_RULE == get_cleanup_type()) { //2, Query build table cleanup
      if (sess_info->get_last_active_time() < get_schema_version() + 100) { //The reason for +100 is to allow a certain error in the time stamp comparison
        (void)sess_info->unlock_query();
        set_drop_flag(false);
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true); //The current session is creating a table and needs to continue to check in the next schedule
        LOG_INFO("current table is in status of creating", K(sess_info->get_last_active_time()));
      } else {
        (void)sess_info->unlock_query();
        LOG_INFO("current table was in status of creating", K(sess_info->get_last_active_time()));
      }
    } else if (ObCTASCleanUp::TEMP_TAB_RULE == get_cleanup_type()) { //3, Directly connected temporary table cleanup
      if (sess_info->get_sess_create_time() < get_schema_version() + 100) {
        (void)sess_info->unlock_query();
        set_drop_flag(false);
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true); //The session that created the temporary table is still alive and needs to be checked in the next schedule
        LOG_DEBUG("session that creates temporary table is still alive");
      } else {
        (void)sess_info->unlock_query();
        LOG_DEBUG("current session reusing session id that created temporary table", K(sess_info->get_sess_create_time()));
      }
    } else { //4. Proxy temporary table cleanup
      if (sess_info->get_sess_create_time() < get_schema_version() + 100) {
        (void)sess_info->unlock_query();
        ATOMIC_STORE(&obs_->need_ctas_cleanup_, true); //The session that created the temporary table is still alive and needs to be checked in the next schedule
        LOG_DEBUG("session that creates temporary table is still alive");
      } else {
        set_drop_flag(true);
        (void)sess_info->unlock_query();
        LOG_DEBUG("current session reusing session id that created temporary table", K(sess_info->get_sess_create_time()));
      }
    }
  }
  return OB_SUCCESS == ret;
}

//Traverse the current session, if the session has updated sess_active_time recently, execute alter system refresh tables in session xxx
//Synchronously update the last active time of all temporary tables under the current session
bool ObServer::ObRefreshTime::operator()(sql::ObSQLSessionMgr::Key key,
                                             sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", KR(ret));
  } else if (OB_FAIL(sess_info->try_lock_query())) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("fail to try lock query", KR(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_WARN("try lock query fail with code OB_EGAIN",
          K(sess_info->get_sessid()), K(sess_info->get_sessid_for_table()));
    }
  } else {
    sess_info->refresh_temp_tables_sess_active_time();
    (void)sess_info->unlock_query();
  }
  return OB_SUCCESS == ret;
}

ObServer::ObRefreshTimeTask::ObRefreshTimeTask()
: obs_(nullptr), is_inited_(false)
{}

int ObServer::ObRefreshTimeTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRefreshTimeTask has already been inited", KR(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshTimeTask init with null ptr", KR(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObRefreshTimeTask", KR(ret));
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
    LOG_ERROR("ObRefreshTimeTask has not been inited", KR(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshTimeTask cleanup task got null ptr", KR(ret));
  } else if (OB_FAIL(obs_->refresh_temp_table_sess_active_time())) {
    LOG_ERROR("ObRefreshTimeTask clean up task failed", KR(ret));
  }

  LOG_WARN("LICQ, ObRefreshTimeTask::runTimerTask", KR(ret));
}

int ObServer::refresh_temp_table_sess_active_time()
{
  int ret = OB_SUCCESS;
  ObRefreshTime refesh_time(this);
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session mgr is null", KR(ret));
  } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(refesh_time))) {
    LOG_WARN("failed to traverse each session to check table need be dropped", KR(ret));
  }
  return ret;
}

ObServer::ObRefreshNetworkSpeedTask::ObRefreshNetworkSpeedTask()
: obs_(nullptr), is_inited_(false)
{}

int ObServer::ObRefreshNetworkSpeedTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRefreshNetworkSpeedTask has already been inited", KR(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshNetworkSpeedTask init with null ptr", KR(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObRefreshNetworkSpeedTask", KR(ret));
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
    LOG_ERROR("ObRefreshNetworkSpeedTask has not been inited", KR(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshNetworkSpeedTask cleanup task got null ptr", KR(ret));
  } else if (OB_FAIL(obs_->refresh_network_speed())) {
    LOG_WARN("ObRefreshNetworkSpeedTask reload bandwidth throttle limit failed", KR(ret));
  }
}

ObServer::ObRefreshCpuFreqTimeTask::ObRefreshCpuFreqTimeTask()
: obs_(nullptr), is_inited_(false)
{}

int ObServer::ObRefreshCpuFreqTimeTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRefreshCpuFreqTimeTask has already been inited", KR(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshCpuFreqTimeTask init with null ptr", KR(ret), K(obs));
  } else {
    obs_ = obs;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObRefreshCpuFreqTimeTask", KR(ret));
    }
  }
  return ret;
}

void ObServer::ObRefreshCpuFreqTimeTask::destroy()
{
  is_inited_ = false;
  obs_ = nullptr;
}

void ObServer::ObRefreshCpuFreqTimeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRefreshCpuFreqTimeTask has not been inited", KR(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshCpuFreqTimeTask task got null ptr", KR(ret));
  } else if (OB_FAIL(obs_->refresh_cpu_frequency())) {
    LOG_ERROR("ObRefreshCpuFreqTimeTask task failed", KR(ret));
  }
}

int ObServer::refresh_cpu_frequency()
{
  int ret = OB_SUCCESS;
  uint64_t cpu_frequency = get_cpufreq_khz();

  cpu_frequency = cpu_frequency < 1 ? 1 : cpu_frequency;
  if (cpu_frequency != cpu_frequency_) {
    LOG_INFO("Cpu frequency changed", "from", cpu_frequency_, "to", cpu_frequency);
    cpu_frequency_ = cpu_frequency;
  }

  return ret;
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
      LOG_WARN("ObRefreshNetworkSpeedTask reload bandwidth throttle limit failed", KR(ret));
    }
  }

  return ret;
}

ObServer::ObRefreshIOCalibrationTimeTask::ObRefreshIOCalibrationTimeTask()
: obs_(nullptr), tg_id_(-1), is_inited_(false)
{}

int ObServer::ObRefreshIOCalibrationTimeTask::init(ObServer *obs, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRefreshIOCalibrationTimeTask has already been inited", KR(ret));
  } else if (OB_ISNULL(obs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshIOCalibrationTimeTask init with null ptr", KR(ret), K(obs));
  } else {
    obs_ = obs;
    tg_id_ = tg_id;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_ERROR("fail to schedule task ObRefreshIOCalibrationTimeTask", KR(ret));
    }
  }
  return ret;
}

void ObServer::ObRefreshIOCalibrationTimeTask::destroy()
{
  is_inited_ = false;
  tg_id_ = -1;
  obs_ = nullptr;
}

void ObServer::ObRefreshIOCalibrationTimeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRefreshIOCalibrationTimeTask has not been inited", KR(ret));
  } else if (OB_ISNULL(obs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObRefreshIOCalibrationTimeTask task got null ptr", KR(ret));
  } else if (OB_FAIL(obs_->refresh_io_calibration())) {
    LOG_WARN("ObRefreshIOCalibrationTimeTask task failed", KR(ret));
  } else {
    TG_CANCEL(tg_id_, *this);
  }
}

int ObServer::refresh_io_calibration()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIOCalibration::get_instance().read_from_table())) {
    LOG_WARN("fail to refresh io calibration from table", KR(ret));
  }
  return ret;
}

int ObServer::init_refresh_active_time_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_active_time_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("fail to init refresh active time task", KR(ret));
  }
  return ret;
}

int ObServer::init_ctas_clean_up_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctas_clean_up_task_.init(this, lib::TGDefIDs::CTASCleanUpTimer))) {
    LOG_ERROR("fail to init ctas clean up task", KR(ret));
  }
  return ret;
}

int ObServer::init_redef_heart_beat_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(redef_table_heart_beat_task_.init(lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("fail to init redef heart beat task", KR(ret));
  }
  return ret;
}

int ObServer::init_ddl_heart_beat_task_container()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DDL_HEART_BEAT_TASK_CONTAINER.init())) {
    LOG_ERROR("fail to init ddl heart beat task container", K(ret));
  }
  return ret;
}

int ObServer::init_refresh_network_speed_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_network_speed_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("fail to init refresh network speed task", KR(ret));
  }
  return ret;
}

int ObServer::init_refresh_cpu_frequency()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_cpu_frequency_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("fail to init refresh cpu frequency task", KR(ret));
  }
  return ret;
}

int ObServer::init_refresh_io_calibration()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_io_calibration_task_.init(this, lib::TGDefIDs::ServerGTimer))) {
    LOG_ERROR("fail to init refresh io calibration task", KR(ret));
  }
  return ret;
}

// @@Query cleanup rules for built tables and temporary tables:
//1, Traverse all table_schema, if the session_id of table T <> 0 means that the table is being created or the previous creation failed or the temporary table is to be cleared, then enter 2#;
//2, Create a table for the query: traverse the session, and determine whether T should be DROP according to the session_id and time of the session and table T;
//2.1, there is session->id = T->session_id,
//     a), the last active time of the session <the creation time of T, the table T is in the process of being created and cannot be DROP;
//     b), the last active time of the session >= the creation time of T, sess_id is reused, the ession of the original table T has been disconnected, and you can DROP;
//2.2, there is no session, its id = T->session_id, T can be DROP;
//3. For temporary tables: distinguish between direct connection creation and ob proxy creation;
//3.1 Direct connection mode, the judgment deletion condition is the same as 2#, the difference is that the last active time of the session needs to be replaced with the session creation time;
//3.2 ob proxy mode, a), the interval between the current time and the sess_active_time of the table schema exceeds the maximum timeout of the session, and DROP is required;
//                 b), when a# is not met, all sessions need to be traversed to determine whether there is a session with the same id s1, s1->sess creation time> T creation time (same as rule 2.1#),
//                     When s1 exists, it is considered that session id reuse has occurred, and T still needs to be DROP;
//It has been optimized before calling this interface, only need_ctas_cleanup_=true will be here
//The cleanup of the oracle temporary table is performed in the dml resolve phase of the temporary table for the first time after the session is created for performance reasons, to avoid frequent delete operations in the background
int ObServer::clean_up_invalid_tables()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_FAIL(schema_service_.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant_ids", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    FOREACH(tenant_id, tenant_ids) {
      if (OB_SUCCESS != (tmp_ret = clean_up_invalid_tables_by_tenant(*tenant_id))) {
        LOG_WARN("fail to clean up invalid tables by tenant", KR(tmp_ret), "tenant_id", *tenant_id);
      }
      ret = OB_FAIL(ret) ? ret : tmp_ret;
    }
  }
  return ret;
}

int ObServer::clean_up_invalid_tables_by_tenant(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObDatabaseSchema *database_schema = NULL;
  const int64_t CONNECT_TIMEOUT_VALUE = 50L * 60L * 60L * 1000L * 1000L; //default value is 50hrs
  ObArray<uint64_t> table_ids;
  obrpc::ObDropTableArg drop_table_arg;
  obrpc::ObTableItem table_item;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  char create_host_str[OB_MAX_HOST_NAME_LENGTH];
  if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_ids))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id));
  } else {
    ObCTASCleanUp ctas_cleanup(this, true);
    drop_table_arg.if_exist_ = true;
    drop_table_arg.to_recyclebin_ = false;
    common_rpc_proxy = GCTX.rs_rpc_proxy_;
    MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH);
    // only OB_ISNULL(GCTX.session_mgr_) will exit the loop
    for (int64_t i = 0; i < table_ids.count() && OB_SUCC(tmp_ret); i++) {
      bool is_oracle_mode = false;
      const ObTableSchema *table_schema = NULL;
      const uint64_t table_id = table_ids.at(i);
      // schema guard cannot be used repeatedly in iterative logic,
      // otherwise it will cause a memory hike in schema cache
      if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), KT(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("got invalid schema", KR(ret), K(i));
      } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        LOG_WARN("fail to check table if oracle compat mode", KR(ret));
      } else if (0 == table_schema->get_session_id()) {
        //do nothing
      } else if (0 != table_schema->get_create_host_str().compare(create_host_str)) {
        LOG_DEBUG("current observer is not the one created the table, just skip", "current session", create_host_str, K(*table_schema));
      } else {
        LOG_DEBUG("table is creating or encountered error or is temporary one", K(*table_schema));
        if (table_schema->is_obproxy_create_tmp_tab()) { //1, Temporary tables, proxy table creation, cleanup rules see 3.2#
          LOG_DEBUG("clean_up_invalid_tables::ob proxy created", K(i), K(ObTimeUtility::current_time()),
                                                                 K(table_schema->get_sess_active_time()), K(CONNECT_TIMEOUT_VALUE));
          if (ObTimeUtility::current_time() - table_schema->get_sess_active_time() > CONNECT_TIMEOUT_VALUE) {
            ctas_cleanup.set_drop_flag(true);
          } else {
            ctas_cleanup.set_drop_flag(false);
          }
          ctas_cleanup.set_cleanup_type(ObCTASCleanUp::TEMP_TAB_PROXY_RULE);
        } else {
          ctas_cleanup.set_drop_flag(false);
          if (table_schema->is_tmp_table()) { // 2, Temporary tables, directly connected tables, cleanup rules see 3.1~3.2#
            ctas_cleanup.set_cleanup_type(ObCTASCleanUp::TEMP_TAB_RULE);
          } else { //3, Query and build tables, see 2# for cleaning rules
            ctas_cleanup.set_cleanup_type(ObCTASCleanUp::CTAS_RULE);
          }
        }
        if (false == ctas_cleanup.get_drop_flag()) {
          ctas_cleanup.set_session_id(table_schema->get_session_id());
          ctas_cleanup.set_schema_version(table_schema->get_schema_version());
          if (ObCTASCleanUp::TEMP_TAB_PROXY_RULE == ctas_cleanup.get_cleanup_type()) { //The proxy connection method is not deleted by default, and it may need to be dropped when the reused session is found.
            ctas_cleanup.set_drop_flag(false);
          } else {
            ctas_cleanup.set_drop_flag(true);
          }
          if (OB_ISNULL(GCTX.session_mgr_)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("session mgr is null", KR(ret));
          } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(ctas_cleanup))) {
            LOG_WARN("failed to traverse each session to check table need be dropped", KR(ret), K(*table_schema));
          }
        }
        if (ctas_cleanup.get_drop_flag()) {
          LOG_INFO("a table will be dropped!", K(*table_schema));
          obrpc::ObDDLRes res;
          database_schema = NULL;
          drop_table_arg.tables_.reset();
          drop_table_arg.if_exist_ = true;
          drop_table_arg.tenant_id_ = table_schema->get_tenant_id();
          drop_table_arg.exec_tenant_id_ = table_schema->get_tenant_id();
          drop_table_arg.table_type_ = table_schema->get_table_type();
          drop_table_arg.session_id_ = table_schema->get_session_id();
          drop_table_arg.to_recyclebin_ = false;
          drop_table_arg.compat_mode_ = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
          table_item.table_name_ = table_schema->get_table_name_str();
          table_item.mode_ = table_schema->get_name_case_mode();
          if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
            LOG_WARN("failed to get database schema", K(ret), K(tenant_id));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database schema is null", KR(ret));
          } else if (database_schema->is_in_recyclebin() || table_schema->is_in_recyclebin()) {
            LOG_DEBUG("skip table schema in recyclebin", K(*table_schema));
          } else if (FALSE_IT(table_item.database_name_ = database_schema->get_database_name_str())) {
            //impossible
          } else if (OB_FAIL(drop_table_arg.tables_.push_back(table_item))) {
            LOG_WARN("failed to add table item!", K(table_item), K(ret));
          } else if (OB_FAIL(common_rpc_proxy->drop_table(drop_table_arg, res))) {
            LOG_WARN("failed to drop table", K(drop_table_arg), K(table_item), KR(ret));
          } else {
            LOG_INFO("a table is dropped due to previous error or is a temporary one", K(i), "table_name", table_item.table_name_);
          }
        } else {
          LOG_DEBUG("no need to drop table", K(i));
        }
      }
    }
  }
  return ret;
}

// ---------------------------------- arb server start -------------------------------
#ifdef OB_BUILD_ARBITRATION
int ObServer::init_server_in_arb_mode()
{
  int ret = OB_SUCCESS;
  // init GCTX's config members.
  gctx_.config_ = &config_;
  gctx_.config_mgr_ = &config_mgr_;
  // set_extra_payload() call is necessary, or dest may return -4007 when deserialie rpc packet.
  obrpc::ObIRpcExtraPayload::set_extra_payload(ObRpcExtraPayload::extra_payload_instance());
  palflite::PalfEnvLiteMgr &palf_env_mgr = palflite::PalfEnvLiteMgr::get_instance();
  arbserver::ObArbSrvNetworkFrame &net_work_farme = arbserver::ObArbSrvNetworkFrame::get_instance();
  rpc::frame::ObNetOptions opts;
  arbserver::ArbNetOptions arb_opts;
  const uint32_t rpc_port = static_cast<uint32_t>(GCONF.rpc_port);

  int rpc_io_cnt = static_cast<int>(GCONF.net_thread_count);
  // make net thread count adaptive
  if (0 == rpc_io_cnt) {
    rpc_io_cnt = get_default_net_thread_count();
  }
  const int hp_io_cnt = static_cast<int>(GCONF.high_priority_net_thread_count);
  opts.rpc_io_cnt_ = rpc_io_cnt;                     // rpc io thread count
  opts.high_prio_rpc_io_cnt_ = hp_io_cnt;
  // Do not need set mysql/batch io cnt for arb server
  // opts.mysql_io_cnt_ = io_cnt;
  // opts.batch_rpc_io_cnt_ = io_cnt;
  opts.use_ipv6_ = GCONF.use_ipv6;
  //TODO(tony.wzh): fix opts.tcp_keepidle  negative
  opts.tcp_user_timeout_ = static_cast<int>(GCONF.dead_socket_detection_timeout);
  opts.tcp_keepidle_     = static_cast<int>(GCONF.tcp_keepidle);
  opts.tcp_keepintvl_    = static_cast<int>(GCONF.tcp_keepintvl);
  opts.tcp_keepcnt_      = static_cast<int>(GCONF.tcp_keepcnt);

  if (GCONF.enable_tcp_keepalive) {
    opts.enable_tcp_keepalive_ = 1;
  } else {
    opts.enable_tcp_keepalive_ = 0;
  }

  arb_opts.opts_ = opts;
  arb_opts.self_ = self_addr_;

  LOG_INFO("io thread connection negotiation enabled!");
  arb_opts.negotiation_enable_ = 1;          // enable negotiation
  arb_opts.rpc_port_ = rpc_port;

  if (OB_FAIL(net_work_farme.init(arb_opts, &palf_env_mgr))) {
    LOG_ERROR("init ObArbSrvNetworkFrame failed", K(ret), K(arb_opts));
  } else if (OB_FAIL(palf_env_mgr.init(GCONF.data_dir, self_addr_, net_work_farme.get_req_transport()))) {
    LOG_ERROR("init PalfEnvLiteMgr failed", K(ret), K(arb_opts));
  } else if (OB_FAIL(arb_timer_.init(lib::TGDefIDs::ArbServerTimer, &palf_env_mgr))) {
    LOG_ERROR("init ArbServerTimer failed", K(ret));
  #ifndef OB_USE_ASAN
  } else if (OB_FAIL(ObMemoryDump::get_instance().init())) {
    LOG_ERROR("init memory dumper failed", KR(ret));
  #endif
  } else if (OB_FAIL(arbserver::ObArbWhiteList::get_instance().init())) {
    LOG_ERROR("init ObArbWhiteList failed", K(ret));
  } else if (OB_FAIL(ASCONF.init())){
    LOG_ERROR("init ObArbServerConfig failed", K(ret));
  } else if (OB_FAIL(ASCONF.init_config_with_file())) {
    LOG_ERROR("init config with file failed", K(ret));
  } else {
    LOG_INFO("init_server_in_arb_mode success", K(ret), K(arb_opts));
  }

  return ret;
}

int ObServer::start_server_in_arb_mode()
{
  int ret = OB_SUCCESS;
  arbserver::ObArbSrvNetworkFrame &net_work_farme = arbserver::ObArbSrvNetworkFrame::get_instance();
  if (OB_FAIL(net_work_farme.start())) {
    LOG_ERROR("start ObArbSrvNetworkFrame failed", K(ret));
  } else if (OB_FAIL(arb_timer_.start())) {
    LOG_ERROR("start ObArbServerTimer failed", K(ret));
  } else {
    LOG_INFO("start_server_in_arb_mode success", K(ret));
  }
  return ret;
}

int ObServer::stop_server_in_arb_mode()
{
  int ret = OB_SUCCESS;
  palf::election::GLOBAL_REPORT_TIMER.stop();
  FLOG_INFO("global election report timer stopped");

  arbserver::ObArbSrvNetworkFrame &net_work_farme = arbserver::ObArbSrvNetworkFrame::get_instance();
  FLOG_INFO("begin to stop net_frame");
  if (OB_FAIL(net_work_farme.rpc_shutdown())) {
    FLOG_WARN("rpc_shutdown failed");
  } else if (OB_FAIL(net_work_farme.stop())) {
    FLOG_WARN("stop net_frame failed");
  } else if (OB_FAIL(arb_timer_.stop())) {
    LOG_ERROR("stop ObArbServerTimer failed", K(ret));
  } else {
    FLOG_INFO("stop net_frame success");
    FLOG_INFO("begin to stop OB_LOGGER");
    OB_LOGGER.stop();
    FLOG_INFO("stop OB_LOGGER success");

    FLOG_INFO("begin to stop OB_LOG_COMPRESSOR");
    OB_LOG_COMPRESSOR.stop();
    FLOG_INFO("stop OB_LOG_COMPRESSOR success");

    FLOG_INFO("begin to stop task controller");
    ObTaskController::get().stop();
    FLOG_INFO("stop task controller success");

    FLOG_INFO("begin stop arbserver config");
    ASCONF.stop();
    FLOG_INFO("stop arbserver config success");

    FLOG_INFO("begin stop signal worker");
    sig_worker_->stop();
    FLOG_INFO("stop signal worker success");

    FLOG_INFO("begin stop signal handle");
    signal_handle_->stop();
    FLOG_INFO("stop signal handle success");

    FLOG_INFO("begin to stop memory dump");
    ObMemoryDump::get_instance().stop();
    FLOG_INFO("memory dump stopped");

  }

  FLOG_INFO("stop_server_in_arb_mode success", K(ret));
  return ret;
}

int ObServer::wait_server_in_arb_mode()
{
  int ret = OB_SUCCESS;
  palf::election::GLOBAL_REPORT_TIMER.wait();
  FLOG_INFO("wait global election report timer success");

  FLOG_INFO("begin to wait OB_LOGGER");
  OB_LOGGER.wait();
  FLOG_INFO("wait OB_LOGGER success");

  FLOG_INFO("begin to wait OB_LOG_COMPRESSOR");
  OB_LOG_COMPRESSOR.wait();
  FLOG_INFO("wait OB_LOG_COMPRESSOR success");

  FLOG_INFO("begin to wait task controller");
  ObTaskController::get().wait();
  FLOG_INFO("wait task controller success");

  arbserver::ObArbSrvNetworkFrame &net_work_farme = arbserver::ObArbSrvNetworkFrame::get_instance();
  FLOG_INFO("begin to wait net_frame");
  net_work_farme.wait();
  FLOG_INFO("wait net_frame success");

  FLOG_INFO("begin to wait arb_server");
  arb_timer_.wait();
  FLOG_INFO("wait arb_server success");

  FLOG_INFO("begin wait arbserver config");
  ASCONF.wait();
  FLOG_INFO("wait arbserver config success");

  FLOG_INFO("begin wait signal worker");
  sig_worker_->wait();
  FLOG_INFO("wait signal worker success");

  FLOG_INFO("begin wait signal handle");
  signal_handle_->wait();
  FLOG_INFO("wait signal handle success");

  FLOG_INFO("begin to wait memory dump");
  ObMemoryDump::get_instance().wait();
  FLOG_INFO("wait memory dump success");

  FLOG_INFO("wait_server_in_arb_mode success", K(ret));
  return ret;
}

int ObServer::destroy_server_in_arb_mode()
{
  int ret = OB_SUCCESS;
  OB_LOGGER.destroy();
  OB_LOG_COMPRESSOR.destroy();
  ObTaskController::get().destroy();
  sig_worker_->destroy();
  signal_handle_->destroy();
  palflite::PalfEnvLiteMgr &palf_env_mgr = palflite::PalfEnvLiteMgr::get_instance();
  arbserver::ObArbSrvNetworkFrame &net_work_farme = arbserver::ObArbSrvNetworkFrame::get_instance();
  palf_env_mgr.destroy();
  net_work_farme.destroy();
  arb_timer_.destroy();
  ObMemoryDump::get_instance().destroy();
  ASCONF.destroy();
  palf::election::GLOBAL_REPORT_TIMER.destroy();
  LOG_WARN("destroy_server_in_arb_mode success", K(ret));
  return ret;
}
#endif

bool ObServer::is_arbitration_mode() const
{
#ifdef OB_BUILD_ARBITRATION
  return (ARBITRATION_MODE == gctx_.startup_mode_) ? true : false;
#else
  return false;
#endif

}


// ------------------------------- arb server end -------------------------------------------
} // end of namespace observer
} // end of namespace oceanbase
