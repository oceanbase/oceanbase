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
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "lib/io/ob_io_benchmark.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_task_define.h"
#include "rootserver/ob_root_service.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_partition_migrator.h"
#include "observer/ob_service.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ObServerReloadConfig::ObServerReloadConfig(ObServerConfig& config, ObGlobalContext& gctx)
    : ObReloadConfig(&config), gctx_(gctx)
{}

ObServerReloadConfig::~ObServerReloadConfig()
{}

int ObServerReloadConfig::operator()()
{
  int ret = OB_SUCCESS;
  int real_ret = ret;
  if (!gctx_.is_inited()) {
    real_ret = ret = OB_INNER_STAT_ERROR;
    LOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), K(ret));
  } else {
    if (OB_FAIL(ObReloadConfig::operator()())) {
      real_ret = ret;
      LOG_WARN("ObReloadConfig operator() failed", K(ret));
    }
    if (OB_FAIL(gctx_.root_service_->reload_config())) {
      real_ret = ret;
      LOG_WARN("root_service_ reload_config failed", K(ret));
    }
    if (OB_FAIL(gctx_.location_cache_->reload_config())) {
      real_ret = ret;
      LOG_WARN("location cache reload config failed", K(ret));
    }
    if (OB_FAIL(ObClusterVersion::get_instance().reload_config())) {
      real_ret = ret;
      LOG_WARN("cluster version reload config failed", K(ret));
    }
    if (OB_FAIL(gctx_.par_ser_->reload_config())) {
      real_ret = ret;
      LOG_WARN("reload configuration for partition service fail", K(ret));
    }

    if (OB_FAIL(OBSERVER.reload_config())) {
      real_ret = ret;
      LOG_WARN("reload configuration for ob service fail", K(ret));
    }
    if (OB_FAIL(OBSERVER.get_net_frame().reload_config())) {
      real_ret = ret;
      LOG_WARN("reload configuration for net frame fail", K(ret));
    }
    if (OB_FAIL(OBSERVER.get_net_frame().reload_ssl_config())) {
      real_ret = ret;
      LOG_WARN("reload ssl config for net frame fail", K(ret));
    }
  }
  {
    const int64_t limit_memory = GCONF.get_server_memory_limit();
    const int64_t reserved_memory = GCONF.cache_wash_threshold;
    const int64_t reserved_urgent_memory = GCONF.memory_reserved;
    LOG_INFO("set limit memory", K(limit_memory));
    set_memory_limit(limit_memory);
    LOG_INFO("set reserved memory", K(reserved_memory));
    ob_set_reserved_memory(reserved_memory);
    LOG_INFO("set urgent memory", K(reserved_urgent_memory));
    ob_set_urgent_memory(reserved_urgent_memory);

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
    io_config.large_query_io_percent_ = GCONF._large_query_io_percentage;
    // In the 2.x version, reuse the sys_bkgd_io_timeout configuration item to indicate the data disk io timeout time
    // After version 3.1, use the data_storage_io_timeout configuration item.
    io_config.data_storage_io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    io_config.data_storage_warning_tolerance_time_ = GCONF.data_storage_warning_tolerance_time;
    io_config.data_storage_error_tolerance_time_ = GCONF.data_storage_error_tolerance_time;
    if (OB_FAIL(ObIOManager::get_instance().set_io_config(io_config))) {
      real_ret = ret;
      LOG_WARN("reload io manager config fail, ", K(ret));
    }
    if (OB_FAIL(ObIOBenchmark::get_instance().reload_io_bench_res())) {
      // DO NOT overwtie real_ret, allow reaload fail
      LOG_WARN("reload io bench result fail, ", K(ret));
    }

    (void)reload_diagnose_info_config(GCONF.enable_perf_event);
    (void)reload_trace_log_config(GCONF.enable_sql_audit);

    ObTenantManager::get_instance().reload_config();
  }
  {
    ObMallocAllocator* malloc_allocator = ObMallocAllocator::get_instance();
    const bool reserve = true;
    malloc_allocator->set_tenant_ctx_idle(OB_SERVER_TENANT_ID,
        ObCtxIds::LIBEASY,
        (GCONF.__easy_memory_limit * GCONF.__easy_memory_reserved_percentage) / 100,
        reserve);
  }
  if (OB_FAIL(ObPartitionScheduler::get_instance().reload_config())) {
    real_ret = ret;
    LOG_WARN("reload configuration for ObPartitionSchedule fail", K(ret));
  }

  {
    const int64_t merge_thread_cnt = common::ObServerConfig::get_instance().merge_thread_count;
    if (merge_thread_cnt < 0 || merge_thread_cnt > INT32_MAX) {
      real_ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid merge thread cnt", K(real_ret));
    } else if (OB_FAIL(ObDagScheduler::get_instance().set_major_merge_concurrency(
                   static_cast<int32_t>(merge_thread_cnt)))) {
      real_ret = ret;
      LOG_WARN("set scheduler work thread num failed", K(ret), K(merge_thread_cnt));
    }
  }
  {
    const int64_t minor_merge_concurrency = common::ObServerConfig::get_instance().minor_merge_concurrency;
    if (OB_FAIL(ObDagScheduler::get_instance().set_minor_merge_concurrency(
            static_cast<int32_t>(minor_merge_concurrency)))) {
      real_ret = ret;
      LOG_WARN("set scheduler minor_merge_concurrency failed", K(ret), K(minor_merge_concurrency));
    }
  }
  {
    const int64_t mini_merge_concurrency = common::ObServerConfig::get_instance()._mini_merge_concurrency;
    if (OB_FAIL(
            ObDagScheduler::get_instance().set_mini_merge_concurrency(static_cast<int32_t>(mini_merge_concurrency)))) {
      real_ret = ret;
      LOG_WARN("set scheduler mini_merge_concurrency failed", K(ret), K(mini_merge_concurrency));
    }
  }

  {
    const int64_t restore_concurrency = common::ObServerConfig::get_instance().restore_concurrency;
    if (OB_FAIL(ObPartitionMigrator::get_instance().get_task_pool().set_task_thread_num(restore_concurrency))) {
      real_ret = ret;
      LOG_WARN("set scheduler restore_concurrency failed", K(ret), K(restore_concurrency));
    }
  }

  {
    const int64_t migrate_concurrency = common::ObServerConfig::get_instance().migrate_concurrency;
    if (OB_FAIL(ObDagScheduler::get_instance().set_migrate_concurrency(static_cast<int32_t>(migrate_concurrency)))) {
      LOG_WARN("set migrate concurrency failed", K(ret), K(migrate_concurrency));
    }
  }

  {
    const int64_t group_migrate_concurrency = common::ObServerConfig::get_instance().server_data_copy_in_concurrency;
    if (OB_FAIL(ObDagScheduler::get_instance().set_group_migrate_concurrency(
            static_cast<int32_t>(group_migrate_concurrency)))) {
      LOG_WARN("set group migrate concurrency failed", K(ret), K(group_migrate_concurrency));
    }
  }

  {
    const int64_t backup_concurrency = common::ObServerConfig::get_instance().backup_concurrency;
    if (OB_FAIL(ObDagScheduler::get_instance().set_backup_concurrency(static_cast<int32_t>(backup_concurrency)))) {
      LOG_WARN("set backup concurrency failed", K(ret), K(backup_concurrency));
    }
  }

  {
    // backup zone and region support
    if (OB_FAIL(ObServer::get_instance().get_partition_service().enable_backup_white_list())) {
      ObString backup_zone_str = GCONF.backup_zone.get_value_string();
      ObString backup_region_str = GCONF.backup_region.get_value_string();
      LOG_WARN("failed to enable backup white list", K(ret), K(backup_region_str), K(backup_zone_str));
    }
  }

  // backup zone and region support
  {
    ObString backup_zone_str = GCONF.backup_zone.get_value_string();
    ObString backup_region_str = GCONF.backup_region.get_value_string();

    bool prev_io_stat = ObStorageGlobalIns::get_instance().is_io_prohibited();
    bool prohibited = true;
    LOG_INFO("backup zone and region", K(backup_zone_str), K(backup_region_str));

    if (backup_zone_str.empty() && backup_region_str.empty()) {
      // Both backup region and zone are not set, IO operation is allowed.
      prohibited = false;
    } else if (!backup_zone_str.empty() && !backup_region_str.empty()) {
      // Both backup region and zone exist, not allowed, something wrong unexpected.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup region and zone both are set, which are unexpected.", K(ret), K(backup_zone_str), K(backup_region_str));
    } else if (!backup_zone_str.empty()) {
      // Backup zone is set.
      ObArray<ObBackupZone> backup_zone;
      if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_zone_str, MAX_ZONE_LENGTH, backup_zone))) {
        LOG_WARN("failed to parse backup zone format", K(ret), K(backup_zone_str));
      } else if (backup_zone.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup zone is empty", K(ret), K(backup_zone_str));
      } else {
        const ObZone zone = GCONF.zone.str();
        LOG_INFO("set backup zone", K(zone), K(backup_zone_str), K(backup_zone));
        for (int64_t i = 0; i < backup_zone.count(); i++) {
          // I am in black list, IO operations are allowed.
          if (zone == backup_zone[i].zone_) {
            prohibited = false;
            break;
          }
        }
      }
    } else {
      // Backup region is set.
      ObArray<ObBackupRegion> backup_region;
      ObRegion region;
      if (OB_FAIL(ObBackupUtils::parse_backup_format_input(backup_region_str, MAX_REGION_LENGTH, backup_region))) {
        LOG_WARN("failed to parse backup region format", K(ret), K(backup_region_str));
      } else if (backup_region.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup region is empty", K(ret), K(backup_region_str));
      } else if (OB_FAIL(ObServer::get_instance().get_partition_service().get_locality_manager()->load_region())) {
        LOG_WARN("get local region failed", K(ret), K(backup_region_str));
      } else if (OB_FAIL(ObServer::get_instance().get_partition_service().get_locality_manager()->get_local_region(region))) {
        LOG_WARN("get local region failed", K(ret), K(backup_region_str));
      } else {
        LOG_INFO("set backup region", K(region), K(backup_region_str), K(backup_region));
        for (int64_t i = 0; i < backup_region.count(); i++) {
          // I am in black list, IO operations are allowed.
          if (region == backup_region[i].region_) {
            prohibited = false;
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret) && prev_io_stat != prohibited) {
      ObStorageGlobalIns::get_instance().set_io_prohibited(prohibited);
      if (prev_io_stat != prohibited) {
        FLOG_WARN("backup set_io_prohibited", K(ret), K(prohibited), K(prev_io_stat));
      }
    }
  }

  const int64_t cache_size = GCONF.memory_chunk_cache_size;
  const int cache_cnt = cache_size > 0 ? cache_size / INTACT_ACHUNK_SIZE : INT32_MAX;
  lib::AChunkMgr::instance().set_max_chunk_cache_cnt(cache_cnt);
  if (NULL != gctx_.omt_) {
    gctx_.omt_->set_quota2token(GCONF.cpu_quota_concurrency);
  }
  if (GCONF.cluster_id.get_value() >= 0) {
    obrpc::ObRpcNetHandler::CLUSTER_ID = GCONF.cluster_id.get_value();
    LOG_INFO("set CLUSTER_ID for rpc", "cluster_id", GCONF.cluster_id.get_value());
  }

  // reset mem leak
  {
    static common::ObMemLeakChecker::TCharArray last_value;
    static bool do_once __attribute__((unused)) = [&]() {
      STRNCPY(&last_value[0], GCONF.leak_mod_to_check.str(), sizeof(last_value));
      return false;
    }();
    if (0 == STRNCMP(last_value, GCONF.leak_mod_to_check.str(), sizeof(last_value))) {
      // At the end of the observer startup, the config will be reloaded once. If the status is not judged, the trace
      // caught during the startup process will be flushed. do-nothing
    } else {
      reset_mem_leak_checker_label(GCONF.leak_mod_to_check.str());

      STRNCPY(last_value, GCONF.leak_mod_to_check.str(), sizeof(last_value));
      last_value[sizeof(last_value) - 1] = '\0';
    }
  }

  {
    auto new_level = obrpc::get_rpc_checksum_check_level_from_string(GCONF._rpc_checksum.str());
    auto orig_level = obrpc::get_rpc_checksum_check_level();
    if (new_level != orig_level) {
      LOG_INFO("rpc_checksum_check_level changed", "orig", orig_level, "new", new_level);
    }
    obrpc::set_rpc_checksum_check_level(new_level);
  }

  {
    auto new_upgrade_stage = obrpc::get_upgrade_stage(GCONF._upgrade_stage.str());
    auto orig_upgrade_stage = GCTX.get_upgrade_stage();
    if (new_upgrade_stage != orig_upgrade_stage) {
      LOG_INFO("_upgrade_stage changed", K(new_upgrade_stage), K(orig_upgrade_stage));
    }
    (void)GCTX.set_upgrade_stage(new_upgrade_stage);
  }

  // syslog bandwidth limitation
  share::ObTaskController::get().set_log_rate_limit(GCONF.syslog_io_bandwidth_limit.get_value());

  if (nullptr != GCTX.omt_) {
    GCTX.omt_->set_workers_per_cpu(GCONF.workers_per_cpu_quota.get_value());
  }

  get_unis_global_compat_version() = GET_MIN_CLUSTER_VERSION();
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100) {
    lib::g_runtime_enabled = true;
  }
  common::ObKVGlobalCache::get_instance().reload_wash_interval();
  ObPartitionScheduler::get_instance().reload_minor_merge_schedule_interval();
  {
    OB_STORE_FILE.resize_file(GCONF.datafile_size, GCONF.datafile_disk_percentage);
  }
  {
    const int64_t location_thread_cnt = GCONF.location_refresh_thread_count;
    if (OB_NOT_NULL(GCTX.ob_service_) &&
        OB_FAIL(GCTX.ob_service_->get_partition_location_updater().set_thread_cnt(location_thread_cnt))) {
      real_ret = ret;
      LOG_WARN("fail to set location updater's thread cnt", KR(ret), K(location_thread_cnt));
    }
  }
  return real_ret;
}
