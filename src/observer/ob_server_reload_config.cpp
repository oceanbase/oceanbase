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

#include "ob_server_reload_config.h"
#include "lib/alloc/alloc_func.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/ob_malloc_sample_struct.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/signal/ob_signal_handlers.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "share/ob_cluster_version.h"
#include "share/ob_task_define.h"
#include "share/ob_resource_limit.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_service.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "share/ob_ddl_sim_point.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase
{
namespace observer
{

int set_cluster_name_hash(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  uint64_t cluster_name_hash = obrpc::ObRpcPacket::INVALID_CLUSTER_NAME_HASH;

  if (OB_FAIL(calc_cluster_name_hash(cluster_name, cluster_name_hash))) {
    LOG_WARN("failed to calc_cluster_name_hash", KR(ret), K(cluster_name));
  } else {
    obrpc::ObRpcNetHandler::CLUSTER_NAME_HASH = cluster_name_hash;
    LOG_INFO("set cluster_name_hash", KR(ret), K(cluster_name), K(cluster_name_hash));
  }
  return ret;
}

int calc_cluster_name_hash(const ObString &cluster_name, uint64_t &cluster_name_hash)
{
  int ret = OB_SUCCESS;
  cluster_name_hash = obrpc::ObRpcPacket::INVALID_CLUSTER_NAME_HASH;

  if (0 == cluster_name.length()) {
    cluster_name_hash = obrpc::ObRpcPacket::INVALID_CLUSTER_NAME_HASH;
    LOG_INFO("set cluster_name_hash to invalid", K(cluster_name));
  } else {
    cluster_name_hash = common::murmurhash(cluster_name.ptr(), cluster_name.length(), 0);
    LOG_INFO("calc cluster_name_hash for rpc", K(cluster_name), K(cluster_name_hash));
  }

  return ret;
}
}
}
ObServerReloadConfig::ObServerReloadConfig(ObServerConfig &config, ObGlobalContext &gctx)
  : ObReloadConfig(&config),
    gctx_(gctx)
{
}

ObServerReloadConfig::~ObServerReloadConfig()
{

}

int ObServerReloadConfig::operator()()
{
  int tmp_ret = OB_SUCCESS;
  int ret = tmp_ret;
  const bool is_arbitration_mode = OBSERVER.is_arbitration_mode();

  if (!gctx_.is_inited()) {
    ret = tmp_ret = OB_INNER_STAT_ERROR;
    LOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), K(tmp_ret));
  } else {
    if (OB_TMP_FAIL(ObReloadConfig::operator()())) {
      LOG_WARN("ObReloadConfig operator() failed", K(tmp_ret));
    }
    if (OB_TMP_FAIL(gctx_.root_service_->reload_config())) {
      LOG_WARN("root_service_ reload_config failed", K(tmp_ret));
    }
    if (OB_TMP_FAIL(gctx_.location_service_->reload_config())) {
      LOG_WARN("location service reload config failed", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(ObClusterVersion::get_instance().reload_config())) {
      LOG_WARN("cluster version reload config failed", K(tmp_ret));
    }

    if (OB_TMP_FAIL(OBSERVER.reload_config())) {
      LOG_WARN("reload configuration for ob service fail", K(tmp_ret));
    }
    if (OB_TMP_FAIL(OBSERVER.get_net_frame().reload_config())) {
      LOG_WARN("reload configuration for net frame fail", K(tmp_ret));
    }
    if (OB_TMP_FAIL(OBSERVER.get_net_frame().reload_ssl_config())) {
      LOG_WARN("reload ssl config for net frame fail", K(tmp_ret));
    }
    if (OB_TMP_FAIL(OBSERVER.get_rl_mgr().reload_config())) {
      LOG_WARN("reload config for ratelimit manager fail", K(tmp_ret));
    }
    if (OB_TMP_FAIL(ObTdeEncryptEngineLoader::get_instance().reload_config())) {
      LOG_WARN("reload config for tde encrypt engine fail", K(tmp_ret));
    }
    if (OB_TMP_FAIL(ObSrvNetworkFrame::reload_rpc_auth_method())) {
      LOG_WARN("reload config for rpc auth method fail", K(tmp_ret));
    }
  }
  {
    GMEMCONF.reload_config(GCONF);
    const int64_t limit_memory = GMEMCONF.get_server_memory_limit();
    OB_LOGGER.set_info_as_wdiag(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_1_0_0);
    // reload log config again after get MIN_CLUSTER_VERSION
    if (OB_TMP_FAIL(ObReloadConfig::operator()())) {
      LOG_WARN("ObReloadConfig operator() failed", K(tmp_ret));
    }
    const int64_t reserved_memory = GCONF.cache_wash_threshold;
    const int64_t reserved_urgent_memory = GCONF.memory_reserved;
    LOG_INFO("set limit memory", K(limit_memory));
    set_memory_limit(limit_memory);
    LOG_INFO("set reserved memory", K(reserved_memory));
    ob_set_reserved_memory(reserved_memory);
    LOG_INFO("set urgent memory", K(reserved_urgent_memory));
    ob_set_urgent_memory(reserved_urgent_memory);
#ifdef OB_USE_ASAN
    __MemoryContext__::set_enable_asan_allocator(GCONF.enable_asan_for_memory_context);
#endif
    ObMallocSampleLimiter::set_interval(GCONF._max_malloc_sample_interval,
                                     GCONF._min_malloc_sample_interval);
    enable_memleak_light_backtrace(GCONF._enable_memleak_light_backtrace);
    if (!is_arbitration_mode) {
      ObIOConfig io_config;
      int64_t cpu_cnt = GCONF.cpu_count;
      if (cpu_cnt <= 0) {
        cpu_cnt = common::get_cpu_num();
      }
      io_config.disk_io_thread_count_ = GCONF.disk_io_thread_count;
      // In the 2.x version, reuse the sys_bkgd_io_timeout configuration item to indicate the data disk io timeout time
      // After version 3.1, use the data_storage_io_timeout configuration item.
      io_config.data_storage_io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      io_config.data_storage_warning_tolerance_time_ = GCONF.data_storage_warning_tolerance_time;
      io_config.data_storage_error_tolerance_time_ = GCONF.data_storage_error_tolerance_time;
      if (!is_arbitration_mode
          && OB_TMP_FAIL(ObIOManager::get_instance().set_io_config(io_config))) {
        LOG_WARN("reload io manager config fail, ", K(tmp_ret));
      }

      (void)reload_diagnose_info_config(GCONF.enable_perf_event);
      (void)reload_trace_log_config(GCONF.enable_record_trace_log);

      reload_tenant_freezer_config_();
      reload_tenant_scheduler_config_();
    }
  }
  {
    ObMallocAllocator *malloc_allocator = ObMallocAllocator::get_instance();
    const bool reserve = true;
    malloc_allocator->set_tenant_ctx_idle(OB_SERVER_TENANT_ID, ObCtxIds::LIBEASY,
        (GCONF.__easy_memory_limit * GCONF.__easy_memory_reserved_percentage) / 100,
        reserve);
  }

  int64_t cache_size = GCONF.memory_chunk_cache_size;
  bool use_large_chunk_cache = 1 != cache_size;
  if (0 == cache_size || 1 == cache_size) {
    cache_size = GMEMCONF.get_server_memory_limit();
    if (cache_size >= (32L<<30)) {
      cache_size -= (4L<<30);
    }
  }
  lib::AChunkMgr::instance().set_max_chunk_cache_size(cache_size, use_large_chunk_cache);

  if (!is_arbitration_mode) {
    // Refresh cluster_id, cluster_name_hash for non arbitration mode
    if (GCONF.cluster_id.get_value() > 0) {
      obrpc::ObRpcNetHandler::CLUSTER_ID = GCONF.cluster_id.get_value();
      LOG_INFO("set CLUSTER_ID for rpc", "cluster_id", GCONF.cluster_id.get_value());
    }

    if (FAILEDx(set_cluster_name_hash(GCONF.cluster.str()))) {
      LOG_WARN("failed to set_cluster_name_hash", KR(ret), "cluster_name", GCONF.cluster.str(),
                                                "cluster_name_len", strlen(GCONF.cluster.str()));
    }
  }

  // reset mem leak
  {
    static common::ObMemLeakChecker::TCharArray last_value;
    static bool do_once __attribute__((unused)) = [&]() {
                            STRNCPY(&last_value[0], GCONF.leak_mod_to_check.str(), sizeof(last_value));
                            return false;
                          }();
    if (0 == STRNCMP(last_value, GCONF.leak_mod_to_check.str(), sizeof(last_value))) {
      // At the end of the observer startup, the config will be reloaded once. If the status is not judged, the trace caught during the startup process will be flushed.
      // do-nothing
    } else {
      reset_mem_leak_checker_label(GCONF.leak_mod_to_check.str());

      STRNCPY(last_value, GCONF.leak_mod_to_check.str(), sizeof(last_value));
      last_value[sizeof(last_value) - 1] = '\0';
    }
  }

  {
    static char last_storage_check_mod[MAX_CACHE_NAME_LENGTH];
    if (0 != STRNCMP(last_storage_check_mod, GCONF._storage_leak_check_mod.str(), sizeof(last_storage_check_mod))) {
      ObKVGlobalCache::get_instance().set_storage_leak_check_mod(GCONF._storage_leak_check_mod.str());
      STRNCPY(last_storage_check_mod, GCONF._storage_leak_check_mod.str(), sizeof(last_storage_check_mod));
    }
  }
#ifndef ENABLE_SANITY
  {
    ObMallocAllocator::get_instance()->force_explict_500_malloc_ =
      GCONF._force_explict_500_malloc;
  }
#else
  {
    sanity_set_whitelist(GCONF.sanity_whitelist.str());
    ObMallocAllocator::get_instance()->enable_tenant_leak_memory_protection_ =
      GCONF._enable_tenant_leak_memory_protection;
  }
#endif
  {
    ObResourceLimit rl;
    int tmp_ret = rl.load_config(GCONF._resource_limit_spec.str());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("load _resource_limit_spec failed", K(tmp_ret), K(GCONF._resource_limit_spec.str()));
    } else {
      LOG_INFO("load _resource_limit_spec succeed", "origin", RL_CONF, "current", rl,
               K(GCONF._resource_limit_spec.str()));
      RL_CONF.assign(rl);
    }
    ObResourceLimit::IS_ENABLED = GCONF._enable_resource_limit_spec;
  }

  {
    auto new_level = obrpc::get_rpc_checksum_check_level_from_string(GCONF._rpc_checksum.str());
    auto orig_level = obrpc::get_rpc_checksum_check_level();
    if (new_level != orig_level) {
      LOG_INFO("rpc_checksum_check_level changed",
               "orig", orig_level,
               "new", new_level);
    }
    obrpc::set_rpc_checksum_check_level(new_level);
  }

  if (!is_arbitration_mode) {
    auto new_upgrade_stage = obrpc::get_upgrade_stage(GCONF._upgrade_stage.str());
    auto orig_upgrade_stage = GCTX.get_upgrade_stage();
    if (new_upgrade_stage != orig_upgrade_stage) {
      LOG_INFO("_upgrade_stage changed", K(new_upgrade_stage), K(orig_upgrade_stage));
    }
    (void)GCTX.set_upgrade_stage(new_upgrade_stage);
  }

  // syslog bandwidth limitation
  share::ObTaskController::get().set_log_rate_limit(
      GCONF.syslog_io_bandwidth_limit.get_value());
  share::ObTaskController::get().set_diag_per_error_limit(
      GCONF.diag_syslog_per_error_limit.get_value());

  lib::g_runtime_enabled = true;

  if (!is_arbitration_mode) {
    common::ObKVGlobalCache::get_instance().reload_wash_interval();
    int tmp_ret = OB_SUCCESS;
    int64_t data_disk_size = 0;
    int64_t data_disk_percentage = 0;
    int64_t reserved_size = 0;
    if (OB_TMP_FAIL(ObServerUtils::get_data_disk_info_in_config(data_disk_size,
                                                                data_disk_percentage))) {
      LOG_ERROR("cal_all_part_disk_size failed", KR(tmp_ret));
    } else if (OB_TMP_FAIL(SLOGGERMGR.get_reserved_size(reserved_size))) {
      LOG_WARN("fail to get reserved size", KR(tmp_ret), K(reserved_size));
    } else if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.resize_file(data_disk_size,
                                                           data_disk_percentage,
                                                           reserved_size))) {
      LOG_WARN("fail to resize file", KR(tmp_ret),
          K(data_disk_size), K(data_disk_percentage), K(reserved_size));
    }
  }

  {
    ObSysVariables::set_value("datadir", GCONF.data_dir);
  }

  {
    common::g_enable_backtrace = GCONF._enable_backtrace_function;
  }

  {
    ObMallocAllocator::get_instance()->force_malloc_for_absent_tenant_ = GCONF._force_malloc_for_absent_tenant;
  }

  {
    ObSigFaststack::get_instance().set_min_interval(GCONF._faststack_min_interval.get_value());
  }
  return ret;
}

void ObServerReloadConfig::reload_tenant_scheduler_config_()
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt should not be null", K(ret));
  } else {
    auto f = [] () {
      (void)MTL(ObTenantDagScheduler *)->reload_config();
      (void)MTL(compaction::ObTenantTabletScheduler *)->reload_tenant_config();
      return OB_SUCCESS;
    };
    omt->operate_in_each_tenant(f);
  }
}

int ObServerReloadConfig::ObReloadTenantFreezerConfOp::operator()()
{
  int ret = OB_SUCCESS;
  // NOTICE: tenant freezer should update before ObSharedMemAllocMgr.
  MTL(ObTenantFreezer *)->reload_config();
  MTL(ObSharedMemAllocMgr*)->update_throttle_config();
  return ret;
}

void ObServerReloadConfig::reload_tenant_freezer_config_()
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt should not be null", K(ret));
  } else {
    ObReloadTenantFreezerConfOp f;
    omt->operate_in_each_tenant(f);
  }
}
