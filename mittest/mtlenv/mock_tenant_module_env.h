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

#pragma once
#include <sys/stat.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include <unistd.h>
#define protected public
#define private public
#include "share/rc/ob_tenant_base.h"
#include "common/storage/ob_io_device.h"
#include "lib/file/file_directory_utils.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/palf_options.h"
#include "logservice/ob_server_log_block_mgr.h"
#include "observer/ob_service.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_tenant_mtl_helper.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_tenant_meta.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant_srs.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_alive_server_tracer.h"
#include "share/ob_device_manager.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "sql/ob_sql.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "storage/blocksstable/ob_decode_resource_pool.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/compaction/ob_compaction_tablet_diagnose.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_compaction_suggestion.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_xa_service.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/ob_file_system_router.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/lob/ob_lob_manager.h"
#include "mittest/ob_mittest_utils.h"
#include "storage/mock_disk_usage_report.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/ob_relative_table.h"
#include "storage/ob_locality_manager.h"
#include "share/scn.h"
#include "mock_gts_source.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tx/wrs/ob_tenant_weak_read_service.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"   // ObSharedMemAllocMgr
#include "logservice/palf/log_define.h"
#include "storage/access/ob_empty_read_bucket.h"
#include "storage/high_availability/ob_rebuild_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"
#include "share/ob_master_key_getter.h"
#endif
#include "share/object_storage/ob_device_config_mgr.h"
#include "observer/table/ob_htable_lock_mgr.h"
#include "observer/table/ob_table_session_pool.h"
#include "share/index_usage/ob_index_usage_info_mgr.h"
#include "observer/ob_startup_accel_task_handler.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/tmp_file/ob_tmp_file_manager.h" // ObTenantTmpFileManager
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/table/group/ob_table_tenant_group.h"
#include "observer/table/ob_table_client_info_mgr.h"
#include "observer/table/ob_table_query_async_processor.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"

namespace oceanbase
{
using namespace common;

namespace storage
{
using namespace transaction;
using namespace logservice;
using namespace concurrency_control;

int64_t ObTenantMetaMemMgr::cal_adaptive_bucket_num()
{
  return 1000;
}

template<typename T>
static int server_obj_pool_mtl_new(common::ObServerObjectPool<T> *&pool)
{
  int ret = common::OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  pool = MTL_NEW(common::ObServerObjectPool<T>, "TntSrvObjPool", tenant_id, false,
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

class MockObService : public observer::ObService
{
public:
  MockObService(const oceanbase::observer::ObGlobalContext &gctx):observer::ObService(gctx)
  {}
  int submit_ls_update_task(const uint64_t tenant_id,
    const share::ObLSID &ls_id)
  {
    UNUSED(tenant_id);
    UNUSED(ls_id);
    return OB_SUCCESS;
  }
};

class MockObTsMgr : public ObTsMgr
{
public:
  MockObTsMgr(MockObGtsSource &source) : source_(source) {}
  virtual ~MockObTsMgr() {}
public:
  virtual int update_gts(const uint64_t tenant_id, const int64_t gts, bool &update)
  {
    UNUSED(tenant_id);
    return source_.update_gts(gts, update);
  }
  virtual int update_local_trans_version(const uint64_t tenant_id, const int64_t gts, bool &update)
  {
    UNUSED(tenant_id);
    return source_.update_local_trans_version(gts, update);
  }
  virtual int get_gts(const uint64_t tenant_id,
                      const MonotonicTs stc,
                      ObTsCbTask *task,
                      share::SCN &gts,
                      MonotonicTs &receive_gts_ts)
  {
    UNUSED(tenant_id);
    return source_.get_gts(stc, task, gts, receive_gts_ts);
  }

  virtual int get_gts_sync(const uint64_t tenant_id,
                           const MonotonicTs stc,
                           int64_t timeout_us,
                           share::SCN &gts,
                           MonotonicTs &receive_gts_ts)
  {
    UNUSED(tenant_id);
    UNUSED(timeout_us);
    return source_.get_gts(stc, NULL, gts, receive_gts_ts);
  }

  virtual int get_gts(const uint64_t tenant_id, ObTsCbTask *task, share::SCN &gts)
  {
    UNUSED(tenant_id);
    return source_.get_gts(task, gts);
  }
  virtual int get_ts_sync(const uint64_t tenant_id,
                          const int64_t timeout_us,
                          share::SCN &scn,
                          bool &is_external_consistent)
  {
    UNUSED(tenant_id);
    UNUSED(timeout_us);
    source_.get_gts(NULL, scn);
    is_external_consistent = false;
    return common::OB_SUCCESS;
  }
  virtual int get_local_trans_version(const uint64_t tenant_id,
                                      const MonotonicTs stc,
                                      ObTsCbTask *task,
                                      int64_t &gts,
                                      MonotonicTs &receive_gts_ts)
  {
    UNUSED(tenant_id);
    UNUSED(stc);
    UNUSED(task);
    UNUSED(gts);
    UNUSED(receive_gts_ts);
    return common::OB_SUCCESS;
  }
  virtual int get_local_trans_version(const uint64_t tenant_id,
                                      ObTsCbTask *task,
                                      int64_t &gts)
  {
    UNUSED(tenant_id);
    UNUSED(task);
    UNUSED(gts);
    return common::OB_SUCCESS;
  }
  virtual int wait_gts_elapse(const uint64_t tenant_id, const share::SCN &scn, ObTsCbTask *task,
                              bool &need_wait)
  {
    UNUSED(tenant_id);
    return source_.wait_gts_elapse(scn, task, need_wait);
  }
  virtual int wait_gts_elapse(const uint64_t tenant_id, const share::SCN &scn)
  {
    UNUSED(tenant_id);
    return source_.wait_gts_elapse(scn);
  }
  virtual int refresh_gts(const uint64_t tenant_id, const bool need_refresh)
  {
    UNUSED(tenant_id);
    return source_.refresh_gts(need_refresh);
  }
  virtual int update_base_ts(const int64_t base_ts)
  {
    return source_.update_base_ts(base_ts);
  }
  virtual int get_base_ts(int64_t &base_ts)
  {
    return source_.get_base_ts(base_ts);
  }
  virtual bool is_external_consistent(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return source_.is_external_consistent();
  }
  virtual int get_gts_and_type(const uint64_t tenant_id, const MonotonicTs stc, share::SCN &gts,
                               int64_t &ts_type)
  {
    UNUSED(tenant_id);
    UNUSED(ts_type);
    MonotonicTs unused;
    return source_.get_gts(stc, NULL, gts, unused);
  }
  virtual int remove_dropped_tenant(const uint64_t tenant_id)
  {
    UNUSED(tenant_id);
    return OB_SUCCESS;
  }
  virtual int interrupt_gts_callback_for_ls_offline(const uint64_t tenant_id, const share::ObLSID ls_id)
  {
    UNUSED(tenant_id);
    UNUSED(ls_id);
    return OB_SUCCESS;
  }
private:
  MockObGtsSource &source_;
};

std::string _executeShellCommand(std::string command)
{
  char buffer[256];
  std::string result = "";
  const char * cmd = command.c_str();
  FILE* pipe = popen(cmd, "r");
  if (!pipe) throw std::runtime_error("popen() failed!");
    try {
        while (!feof(pipe))
            if (fgets(buffer, 128, pipe) != NULL)
                result += buffer;
    } catch (...) {
        pclose(pipe);
        throw;
    }
  pclose(pipe);
  return result;
}

common::ObIODevice* get_device_inner(const common::ObString &storage_type_prefix)
{
  int ret = OB_SUCCESS;
  common::ObIODevice* device = NULL;
  const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_DATA);
  if(OB_FAIL(common::ObDeviceManager::get_local_device(storage_type_prefix, storage_id_mod, device))) {
    STORAGE_LOG(WARN, "get_device_inner", K(ret));
  }
  return device;
}

class MockTenantModuleEnv
{
public:
  MockTenantModuleEnv() : rpc_port_(unittest::get_rpc_port(server_fd_)),
                          mysql_port_(rpc_port_ + 1),
                          self_addr_(ObAddr::IPV4, "127.0.0.1", int32_t(rpc_port_)),
                          net_frame_(GCTX),
                          multi_tenant_(),
                          ob_service_(GCTX),
                          schema_service_(share::schema::ObMultiVersionSchemaService::get_instance()),
                          session_mgr_(),
                          ts_mgr_(gts_source_),
                          config_(common::ObServerConfig::get_instance()),
                          mock_disk_reporter_(),
                          inited_(false),
                          destroyed_(false)
  {}
  ~MockTenantModuleEnv()
  {
    destroy();
  }
  static MockTenantModuleEnv &get_instance()
  {
    static MockTenantModuleEnv env;
    return env;
  }
  static int construct_default_tenant_meta(const uint64_t tenant_id, omt::ObTenantMeta &meta);

  void init_gctx_gconf();
  int init_before_start_mtl();
  int init();
  int start_();
  int remove_sys_tenant();
  void release_guard();
  void destroy();
  bool is_inited() const { return inited_; }

public:
#ifdef OB_BUILD_SHARED_STORAGE
  int encrypt_access_key(const char *access_key, char *encrypt_key, int64_t length) const;
  int init_device_config();
#endif

public:
  static const int64_t TENANT_WORKER_COUNT = 5;

private:
  int init_dir();
  int prepare_io();

private:
  // env
  int64_t rpc_port_;
  int64_t mysql_port_;
  ObAddr self_addr_;
  observer::ObSrvNetworkFrame net_frame_;
  share::ObCgroupCtrl cgroup_ctrl_;
  obrpc::ObBatchRpc batch_rpc_;
  omt::ObMultiTenant multi_tenant_;
  MockObService ob_service_;
  share::ObLocationService location_service_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  share::ObAliveServerTracer server_tracer_;
  sql::ObSql sql_engine_;
  ObSQLSessionMgr session_mgr_;
  common::ObMysqlRandom scramble_rand_;
  ObLocalityManager locality_manager_;
  common::ObMySQLProxy sql_proxy_;
  MockObGtsSource gts_source_;
  MockObTsMgr ts_mgr_;
  char *curr_dir_;
  std::string run_dir_;
  std::string env_dir_;
  std::string sstable_dir_;
  std::string clog_dir_;
  std::string slog_dir_;
  obrpc::ObCommonRpcProxy rs_rpc_proxy_;
  obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  share::ObRsMgr rs_mgr_;
  common::ObServerConfig &config_;
  MockDiskUsageReport mock_disk_reporter_;
  logservice::ObServerLogBlockMgr log_block_mgr_;
  ObLogCursor start_cursor_;
  ObInOutBandwidthThrottle bandwidth_throttle_;
  // param
  palf::PalfDiskOptions disk_options_;

  blocksstable::ObStorageEnv storage_env_;

  // switch tenant thread local
  share::ObTenantSwitchGuard guard_;

  common::ObSimpleMemLimitGetter getter_;
  observer::ObStartupAccelTaskHandler startup_accel_handler_;

  bool inited_;
  bool destroyed_;

  int server_fd_;
};

int MockTenantModuleEnv::remove_sys_tenant()
{
  int ret = OB_SUCCESS;
  bool lock_succ = false;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  guard_.release();
  ret = multi_tenant_.remove_tenant(tenant_id, lock_succ);

  return ret;
}

void MockTenantModuleEnv::release_guard()
{
  guard_.release();
}


int MockTenantModuleEnv::construct_default_tenant_meta(const uint64_t tenant_id, omt::ObTenantMeta &meta)
{
  int ret = OB_SUCCESS;

  ObTenantSuperBlock super_block(tenant_id, false/*is_hidden*/);
  share::ObUnitInfoGetter::ObTenantConfig unit;
  uint64_t unit_id = 1000;
  const bool has_memstore = true;
  const int64_t create_timestamp = ObTimeUtility::current_time();

  share::ObUnitConfig unit_config;
  share::ObUnitConfigName name(std::to_string(tenant_id).c_str());
  const uint64_t unit_config_id = 1000;
  share::ObUnitResource ur(
      4, // max_cpu
      2, // min_cpu
      4L << 30, // memory_size
      4L << 30, // log_disk_size
      GCTX.is_shared_storage_mode() ?
        (4L << 30) : ObUnitResource::DEFAULT_DATA_DISK_SIZE,    // data_disk_size
      10000, // max_iops
      10000, // min_iops,
      0, //iops_weight
      INT64_MAX, // max_net_bandwidth
      0  /*net_bandwidth_weight*/);
  int64_t hidden_sys_data_disk_config_size = 0;
#ifdef OB_BUILD_SHARED_STORAGE
  if ((OB_SYS_TENANT_ID == tenant_id) &&
      GCTX.is_shared_storage_mode()) {  // only sys_tenant_unit_meta record hidden_sys_data_disk_config_size value
    hidden_sys_data_disk_config_size = OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
  }
#endif
  if (OB_FAIL(unit_config.init(unit_config_id, name, ur))) {
    STORAGE_LOG(WARN, "fail to init unit config unit", KR(ret), K(unit_config_id), K(name), K(ur));
  } else if (OB_FAIL(unit.init(tenant_id,
                        unit_id,
                        share::ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL,
                        unit_config,
                        lib::Worker::CompatMode::MYSQL,
                        create_timestamp,
                        has_memstore,
                        false /*is_removed*/,
                        hidden_sys_data_disk_config_size))) {
    STORAGE_LOG(WARN, "fail to init tenant unit", K(ret), K(tenant_id));
  } else if (OB_FAIL(meta.build(unit, super_block))) {
    STORAGE_LOG(WARN, "fail to build tenant meta", K(ret), K(tenant_id));
  }

  return ret;
}


int MockTenantModuleEnv::init_dir()
{
  system(("rm -rf " + run_dir_).c_str());

  curr_dir_ = get_current_dir_name();

  int ret = OB_SUCCESS;
  sstable_dir_ = env_dir_ + "/sstable";
  clog_dir_ = env_dir_ + "/clog";
  slog_dir_ = env_dir_ + "/slog";
  if (OB_FAIL(mkdir(run_dir_.c_str(), 0777))) {
  } else if (OB_FAIL(chdir(run_dir_.c_str()))) {
  } else if (OB_FAIL(mkdir("./run", 0777))) {
  } else if (OB_FAIL(mkdir(env_dir_.c_str(), 0777))) {
  } else if (OB_FAIL(mkdir(clog_dir_.c_str(), 0777))) {
  } else if (OB_FAIL(mkdir(sstable_dir_.c_str(), 0777))) {
  } else if (OB_FAIL(mkdir(slog_dir_.c_str(), 0777))) {
  }

  // 因为改变了工作目录，设置为绝对路径
  for (int i=0;i<MAX_FD_FILE;i++) {
    int len = strlen(OB_LOGGER.log_file_[i].filename_);
    if (len > 0) {
      std::string ab_file = std::string(curr_dir_) + "/" + std::string(OB_LOGGER.log_file_[i].filename_);
      SERVER_LOG(INFO, "convert ab file", K(ab_file.c_str()));
      MEMCPY(OB_LOGGER.log_file_[i].filename_, ab_file.c_str(), ab_file.size());
    }
  }
  return ret;
}

int MockTenantModuleEnv::prepare_io()
{
  int ret = OB_SUCCESS;

  ObIODOpt iod_opt_array[5];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;
  int64_t macro_block_count = 5 * 1024;
  if (GCTX.is_shared_storage_mode()) {
    // In shared storage mode, total_data_disk_size set to 20GB, because hidden_sys has data_disk_size in shared storage mode
    macro_block_count = 10 * 1024;
  }
  int64_t macro_block_size = 64 * 1024;
  char* data_dir = (char*)env_dir_.c_str();
  char file_dir[OB_MAX_FILE_NAME_LENGTH];
  char clog_dir[OB_MAX_FILE_NAME_LENGTH];
  char slog_dir[OB_MAX_FILE_NAME_LENGTH];
  if (OB_FAIL(databuff_printf(file_dir, OB_MAX_FILE_NAME_LENGTH, "%s/sstable/", data_dir))) {
    STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/clog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen clog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(slog_dir, OB_MAX_FILE_NAME_LENGTH, "%s/slog/", data_dir))) {
    STORAGE_LOG(WARN, "failed to gen slog dir", K(ret));
  } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    STORAGE_LOG(WARN, "init device manager failed", KR(ret));
  }

  storage_env_.data_dir_ = data_dir;
  storage_env_.sstable_dir_ = file_dir;
  storage_env_.clog_dir_ = clog_dir;
  storage_env_.default_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_size_ = macro_block_count * common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_percentage_ = 0;
  storage_env_.log_disk_size_ = 20 * 1024 * 1024 * 1024ll;
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    common::ObString storage_type_prefix(OB_LOCAL_CACHE_PREFIX);
    storage::ObLocalCacheDevice *local_cache_device = static_cast<storage::ObLocalCacheDevice*>(
                                                      get_device_inner(storage_type_prefix));
    // for use LOCAL_DEVICE_INSTANCE in share storage mode
    ObIODeviceWrapper::get_instance().set_local_cache_device(local_cache_device);
  } else {
#endif
    common::ObString storage_type_prefix(OB_LOCAL_PREFIX);
    share::ObLocalDevice *local_device = static_cast<share::ObLocalDevice*>(get_device_inner(storage_type_prefix));
    // for unifying init/add_device_channel/destroy local_device and local_cache_device code below
    ObIODeviceWrapper::get_instance().set_local_device(local_device);
#ifdef OB_BUILD_SHARED_STORAGE
  }
#endif
  iod_opt_array[0].set("data_dir", storage_env_.data_dir_);
  iod_opt_array[1].set("sstable_dir", storage_env_.sstable_dir_);
  iod_opt_array[2].set("block_size", storage_env_.default_block_size_);
  iod_opt_array[3].set("datafile_disk_percentage", storage_env_.data_disk_percentage_);
  iod_opt_array[4].set("datafile_size", storage_env_.data_disk_size_);
  iod_opts.opt_cnt_ = 5;
  ObTenantIOConfig io_config = ObTenantIOConfig::default_instance();
  const int64_t async_io_thread_count = 8;
  const int64_t sync_io_thread_count = 2;
  const int64_t max_io_depth = 256;
  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init io manager", K(ret));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.init(iod_opts))) {
    STORAGE_LOG(WARN, "fail to init io device", K(ret), K_(storage_env));
  } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(&LOCAL_DEVICE_INSTANCE,
                                                                    async_io_thread_count,
                                                                    sync_io_thread_count,
                                                                    max_io_depth))) {
    STORAGE_LOG(WARN, "add device channel failed", K(ret));
  } else if (OB_FAIL(log_block_mgr_.init(storage_env_.clog_dir_))) {
    SERVER_LOG(ERROR, "init log pool fail", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    STORAGE_LOG(WARN, "fail to start io manager", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(&getter_,
      bucket_num,
      max_cache_size,
      block_size))) {
    STORAGE_LOG(WARN, "fail to init kv global cache ", K(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.init(10, 1, 1, 1, 1, 10000, 10))) {
    STORAGE_LOG(WARN, "fail to init OB_STORE_CACHE, ", K(ret));
  } else {
  }
  return ret;
}

void MockTenantModuleEnv::init_gctx_gconf()
{
  GCONF.rpc_port = rpc_port_;
  GCONF.mysql_port = mysql_port_;
  GCONF.self_addr_ = self_addr_;
  GCONF.__min_full_resource_pool_memory = 2 * 1024 * 1024 * 1024ul;
  GCONF.observer_id = 1;
  GCONF.cluster_id = 1;
  GCTX.self_addr_seq_.set_addr(self_addr_);
  GCTX.location_service_ = &location_service_;
  GCTX.batch_rpc_ = &batch_rpc_;
  GCTX.schema_service_ = &schema_service_;
  GCTX.server_tracer_ = &server_tracer_;
  GCTX.net_frame_ = &net_frame_;
  GCTX.ob_service_ = &ob_service_;
  GCTX.omt_ = &multi_tenant_;
  GCTX.sql_engine_ = &sql_engine_;
  GCTX.cgroup_ctrl_ = &cgroup_ctrl_;
  GCTX.session_mgr_ = &session_mgr_;
  GCTX.scramble_rand_ = &scramble_rand_;
  GCTX.locality_manager_ = &locality_manager_;
  (void) GCTX.set_server_id(1);
  GCTX.rs_rpc_proxy_ = &rs_rpc_proxy_;
  GCTX.srv_rpc_proxy_ = &srv_rpc_proxy_;
  GCTX.rs_mgr_ = &rs_mgr_;
  GCTX.config_ = &config_;
  GCTX.disk_reporter_ = &mock_disk_reporter_;
  GCTX.bandwidth_throttle_ = &bandwidth_throttle_;
  GCTX.log_block_mgr_ = &log_block_mgr_;
  GCTX.startup_accel_handler_ = &startup_accel_handler_;
}

#ifdef OB_BUILD_SHARED_STORAGE
int MockTenantModuleEnv::encrypt_access_key(
    const char *access_key,
    char *encrypt_key,
    int64_t length) const
{
  int ret = OB_SUCCESS;
  char encrypted_key[OB_MAX_BACKUP_ENCRYPTKEY_LENGTH] = { 0 };
  char serialize_buf[OB_MAX_BACKUP_SERIALIZEKEY_LENGTH] = { 0 };
  int64_t serialize_pos = 0;
  int64_t key_len = 0;
  if (OB_ISNULL(access_key) || OB_ISNULL(encrypt_key) || OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(access_key), KP(encrypt_key), K(length));
  } else if (OB_UNLIKELY(0 == strlen(access_key))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "access_key is empty, shouldn't encrypt", K(ret));
  } else if (OB_UNLIKELY(0 != strncmp(ACCESS_KEY, access_key, strlen(ACCESS_KEY)))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "parameter is not access_key", K(ret));
  } else if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(OB_SYS_TENANT_ID,
      access_key + strlen(ACCESS_KEY), strlen(access_key) - strlen(ACCESS_KEY),
      encrypted_key, OB_MAX_BACKUP_ENCRYPTKEY_LENGTH, key_len))) {
    STORAGE_LOG(WARN, "failed to encrypt authorization key", K(ret));
  } else if (OB_FAIL(hex_print(encrypted_key, key_len, serialize_buf, sizeof(serialize_buf), serialize_pos))) {
    STORAGE_LOG(WARN, "failed to serialize encrypted key", K(ret), K(encrypted_key));
  } else if (OB_UNLIKELY((serialize_pos >= sizeof(serialize_buf)) || (serialize_pos >= length))) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "encode error", K(ret), K(serialize_pos), K(sizeof(serialize_buf)), K(length));
  } else if (FALSE_IT(serialize_buf[serialize_pos] = '\0')) {
  } else if (OB_FAIL(databuff_printf(encrypt_key, length, "%s%s", ENCRYPT_KEY, serialize_buf))) {
    STORAGE_LOG(WARN, "failed to get encrypted key", K(ret), K(serialize_buf));
  }
  return ret;
}

int MockTenantModuleEnv::init_device_config()
{
  int ret = OB_SUCCESS;
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  char access_key_buf[OB_MAX_BACKUP_ACCESSKEY_LENGTH] = { 0 };
  char encrypt_key_buf[OB_MAX_BACKUP_SERIALIZEKEY_LENGTH] = { 0 };
  char object_storage_root_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_FAIL(share::ObMasterKeyGetter::instance().init(NULL))) {
    CLOG_LOG(WARN, "init ObMasterKeyGetter failed");
  } else if (OB_FAIL(share::ObMasterKeyGetter::instance().set_root_key(OB_SYS_TENANT_ID,
                                                      obrpc::RootKeyType::DEFAULT, ObString()))) {
    CLOG_LOG(WARN, "set_root_key failed");
  } else if (OB_FAIL(databuff_printf(access_key_buf, sizeof(access_key_buf),
      "%s%s", ACCESS_KEY, unittest::S3_SK))) {
    STORAGE_LOG(WARN, "fail to databuff printf", K(ret));
  } else if (OB_FAIL(encrypt_access_key(access_key_buf, encrypt_key_buf, sizeof(encrypt_key_buf)))) {
    STORAGE_LOG(WARN, "fail to encrypt access key", K(ret));
  } else if (OB_FAIL(databuff_printf(object_storage_root_path, sizeof(object_storage_root_path),
                     "%s/%lu", unittest::S3_BUCKET, cur_time_ns))) {
    STORAGE_LOG(WARN, "fail to databuff printf", K(ret));
  } else if (OB_FAIL(OB_DIR_MGR.set_object_storage_root_dir(object_storage_root_path))) {
    STORAGE_LOG(WARN, "fail to set object storage root dir", K(ret), K(object_storage_root_path));
  } else if (OB_FAIL(OB_DEVICE_CONF_MGR.init(sstable_dir_.c_str()))) {
    STORAGE_LOG(WARN, "fail to init device config", K(ret));
  } else if (OB_FAIL(OB_DEVICE_CONF_MGR.load_configs())) {
    STORAGE_LOG(WARN, "fail to load device configs", KR(ret));
  } else {
    ObDeviceConfig device_config;
    STRCPY(device_config.used_for_, ObStorageUsedType::get_str(ObStorageUsedType::USED_TYPE_ALL));
    if (OB_FAIL(databuff_printf(device_config.path_, sizeof(device_config.path_),
                "%s/%lu", unittest::S3_BUCKET, cur_time_ns))) {
      STORAGE_LOG(WARN, "fail to databuff printf", KR(ret));
    } else if (OB_FAIL(databuff_printf(device_config.endpoint_, sizeof(device_config.endpoint_),
                "%s%s", HOST, unittest::S3_ENDPOINT))) {
      STORAGE_LOG(WARN, "fail to databuff printf", KR(ret));
    } else if (OB_FAIL(databuff_printf(device_config.access_info_, sizeof(device_config.access_info_),
                "%s%s&%s", ACCESS_ID, unittest::S3_AK, encrypt_key_buf))) {
      STORAGE_LOG(WARN, "fail to databuff printf", KR(ret));
    } else if (OB_FAIL(databuff_printf(device_config.extension_,
               sizeof(device_config.extension_), "%s%s", REGION, unittest::S3_REGION))) {
      STORAGE_LOG(WARN, "fail to databuff printf", KR(ret));
    } else {
      STRCPY(device_config.state_, ObZoneStorageState::get_str(ObZoneStorageState::ADDED));
      device_config.create_timestamp_ = common::ObTimeUtility::fast_current_time();
      device_config.last_check_timestamp_ = common::ObTimeUtility::fast_current_time();
      device_config.op_id_ = 1;
      device_config.sub_op_id_ = 2;
      device_config.storage_id_ = 3;
      device_config.max_iops_ = 0;
      device_config.max_bandwidth_ = 0;
      if (OB_FAIL(OB_DEVICE_CONF_MGR.add_device_config(device_config))) {
        STORAGE_LOG(WARN, "fail to add device config", KR(ret));
      }
    }
  }
  return ret;
}
#endif

int MockTenantModuleEnv::init_before_start_mtl()
{
  int ret = OB_SUCCESS;
  const int64_t ts_ns = ObTimeUtility::current_time_ns();
  env_dir_ = "./env_" + std::to_string(rpc_port_) + "_" + std::to_string(ts_ns);
  run_dir_ = "./run_" + std::to_string(rpc_port_) + "_" + std::to_string(ts_ns);
  GCONF.cpu_count = 2;
  uint64_t start_time = 10000000;
  scramble_rand_.init(static_cast<uint64_t>(start_time), static_cast<uint64_t>(start_time / 2));
  if (OB_FAIL(init_dir())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(locality_manager_.init(self_addr_, &sql_proxy_))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(prepare_io())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(session_mgr_.init())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(ObVirtualTenantManager::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.init())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_instance().init(env_dir_.c_str()))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(init_device_config())) {
    STORAGE_LOG(WARN, "fail to init device config", K(ret));
#endif
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.init(GCTX.is_shared_storage_mode(), 2*1024*1024UL))) {
    STORAGE_LOG(WARN, "fail to init server object manager", K(ret));
  } else if (OB_FAIL(net_frame_.init())) {
    STORAGE_LOG(WARN, "net", "ss", _executeShellCommand("ss -antlp").c_str());
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(net_frame_.start())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(batch_rpc_.init(net_frame_.get_batch_rpc_req_transport(),
                                   net_frame_.get_high_prio_req_transport(),
                                   self_addr_))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::BRPC, batch_rpc_))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(startup_accel_handler_.init(observer::SERVER_ACCEL))) {
    STORAGE_LOG(WARN, "init server startup task handler failed", KR(ret));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.init(GCTX.is_shared_storage_mode()))) {
    STORAGE_LOG(ERROR, "init server checkpoint slog handler fail", K(ret));
  } else if (OB_FAIL(multi_tenant_.init(self_addr_, nullptr, false))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(ObTsMgr::get_instance().init(self_addr_,
                         schema_service_, location_service_, net_frame_.get_req_transport()))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(ObTsMgr::get_instance().start())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (!GCTX.is_shared_storage_mode() && OB_FAIL(tmp_file::ObTmpBlockCache::get_instance().init("tmp_block_cache", 1))) {
    STORAGE_LOG(WARN, "init tmp block cache failed", KR(ret));
  } else if (OB_FAIL(tmp_file::ObTmpPageCache::get_instance().init("tmp_page_cache", 1))) {
    STORAGE_LOG(WARN, "init sn tmp page cache failed", KR(ret));
  } else if (OB_SUCCESS != (ret = bandwidth_throttle_.init(1024 * 1024 * 60))) {
    STORAGE_LOG(ERROR, "failed to init bandwidth_throttle_", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerGTimer))) {
    STORAGE_LOG(ERROR, "init timer fail", KR(ret));
  } else if (OB_FAIL(ObMdsSchemaHelper::get_instance().init())) {
    STORAGE_LOG(ERROR, "fail to init mds schema helper", K(ret));
  } else if (OB_FAIL(LOG_IO_DEVICE_WRAPPER.init(clog_dir_.c_str(), 8, 128, &OB_IO_MANAGER, &ObDeviceManager::get_instance()))) {
    STORAGE_LOG(ERROR, "init log_io_device_wrapper fail", KR(ret));
  } else {
    obrpc::ObRpcNetHandler::CLUSTER_ID = 1;
    oceanbase::palf::election::INIT_TS = 1;
    // 忽略cgroup的报错
    cgroup_ctrl_.init();
    ObTsMgr::get_instance_inner() = &ts_mgr_;
    GCTX.sql_proxy_ = &sql_proxy_;
    ObRunningModeConfig::instance().mini_mode_ = true; // make startup_accel_handler_ use only one thread
  }
  return ret;
}

int MockTenantModuleEnv::init()
{
    STORAGE_LOG(INFO, "mock env init begin", K(this));
    int ret = OB_SUCCESS;

    if (inited_) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(ERROR, "init twice", K(ret));
    } else if (OB_FAIL(ObClockGenerator::init())) {
      STORAGE_LOG(ERROR, "init ClockGenerator failed", K(ret));
    } else if (FALSE_IT(init_gctx_gconf())) {
    } else if (OB_FAIL(init_before_start_mtl())) {
      STORAGE_LOG(ERROR, "init_before_start_mtl failed", K(ret));
    } else {
      oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
      MTL_BIND2(ObTenantIOManager::mtl_new, ObTenantIOManager::mtl_init, mtl_start_default, mtl_stop_default, nullptr, ObTenantIOManager::mtl_destroy);
      MTL_BIND2(mtl_new_default, omt::ObSharedTimer::mtl_init, omt::ObSharedTimer::mtl_start, omt::ObSharedTimer::mtl_stop, omt::ObSharedTimer::mtl_wait, mtl_destroy_default);
      MTL_BIND2(ObTimerService::mtl_new, nullptr, ObTimerService::mtl_start, ObTimerService::mtl_stop, ObTimerService::mtl_wait, ObTimerService::mtl_destroy);
      MTL_BIND2(mtl_new_default, ObTenantSchemaService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(ObTenantMetaMemMgr::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::ObSharedMemAllocMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, common::ObRbMemMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTransService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, logservice::ObGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTimestampService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTransIDService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObXAService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObLSService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObAccessService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantFreezer::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, checkpoint::ObCheckPointService::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, checkpoint::ObTabletGCService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObLogService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObTenantTabletScheduler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObTenantMediumChecker::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, observer::ObTabletTableUpdater::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::ObTenantDagScheduler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantStorageMetaService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, tmp_file::ObTenantTmpFileManager::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, coordinator::ObLeaderCoordinator::mtl_init, coordinator::ObLeaderCoordinator::mtl_start, coordinator::ObLeaderCoordinator::mtl_stop, coordinator::ObLeaderCoordinator::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, coordinator::ObFailureDetector::mtl_init, coordinator::ObFailureDetector::mtl_start, coordinator::ObFailureDetector::mtl_stop, coordinator::ObFailureDetector::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObDiagnoseTabletMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(ObLobManager::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::detector::ObDeadLockDetectorMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::ObTenantTabletStatMgr::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default)
      MTL_BIND2(mtl_new_default, storage::ObTenantCompactionMemPool::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default)
      MTL_BIND2(mtl_new_default, storage::ObTenantSSTableMergeInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::ObDagWarningHistoryManager::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObScheduleSuspectInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObCompactionSuggestionMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::ObTenantFreezeInfoMgr::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObSharedMacroBlockMgr::mtl_init,  mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::mds::ObTenantMdsService::mtl_init, storage::mds::ObTenantMdsService::mtl_start, storage::mds::ObTenantMdsService::mtl_stop, storage::mds::ObTenantMdsService::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObMultiVersionGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTableLockService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(server_obj_pool_mtl_new<transaction::ObPartTransCtx>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<transaction::ObPartTransCtx>);
      MTL_BIND2(server_obj_pool_mtl_new<ObTableScanIterator>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<ObTableScanIterator>);
      MTL_BIND2(ObTenantSQLSessionMgr::mtl_new, ObTenantSQLSessionMgr::mtl_init, nullptr, nullptr, nullptr, ObTenantSQLSessionMgr::mtl_destroy);
      MTL_BIND2(mtl_new_default, ObTenantCGReadInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObDecodeResourcePool::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantDirectLoadMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObEmptyReadBucket::mtl_init, nullptr, nullptr, nullptr, ObEmptyReadBucket::mtl_destroy);
      MTL_BIND2(mtl_new_default, ObRebuildService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
#ifdef OB_BUILD_SHARED_STORAGE
      if (GCTX.is_shared_storage_mode()) {
        MTL_BIND2(mtl_new_default, ObTenantDiskSpaceManager::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
        MTL_BIND2(mtl_new_default, ObTenantFileManager::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
        MTL_BIND2(mtl_new_default, ObSSMicroCachePrewarmService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
        MTL_BIND2(mtl_new_default, ObSSMicroCache::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      }
#else
#endif
      MTL_BIND2(mtl_new_default, table::ObHTableLockMgr::mtl_init, nullptr, nullptr, nullptr, table::ObHTableLockMgr::mtl_destroy);
      MTL_BIND2(mtl_new_default, omt::ObTenantSrs::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, omt::ObSharedTimer::mtl_init, omt::ObSharedTimer::mtl_start, omt::ObSharedTimer::mtl_stop, omt::ObSharedTimer::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, table::ObTableApiSessPoolMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(ObTenantFTPluginMgr::mtl_new, mtl_init_default, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObIndexUsageInfoMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::ObTabletMemtableMgrPool::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantSnapshotService::mtl_init, mtl_start_default, mtl_stop_default, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObOptStatMonitorManager::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, memtable::ObLockWaitMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObGlobalIteratorPool::mtl_init, nullptr, nullptr, nullptr, ObGlobalIteratorPool::mtl_destroy);
      MTL_BIND2(mtl_new_default, observer::ObTenantQueryRespTimeCollector::mtl_init,nullptr, nullptr, nullptr, observer::ObTenantQueryRespTimeCollector::mtl_destroy);
      MTL_BIND2(mtl_new_default, table::ObTableClientInfoMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, observer::ObTableQueryASyncMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(GMEMCONF.reload_config(config_))) {
      STORAGE_LOG(ERROR, "reload memory config failed", K(ret));
    } else if (OB_FAIL(start_())) {
      STORAGE_LOG(ERROR, "mock env start failed", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode() &&
               OB_FAIL(OB_SERVER_DISK_SPACE_MGR.reload_config(config_))) {
      STORAGE_LOG(ERROR, "reload data disk size config failed", K(ret));
#endif
    } else {
      inited_ = true;
    }

    STORAGE_LOG(INFO, "mock env init finish", K(ret));

    return ret;
}

int MockTenantModuleEnv::start_()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  omt::ObTenantMeta meta;
  omt::ObTenant *tenant = nullptr;
  int64_t succ_num = 0;


  if (OB_FAIL(log_block_mgr_.start(storage_env_.log_disk_size_))) {
    SERVER_LOG(ERROR, "log pool start failed", KR(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.start(0/*reserved_size*/))) {
    STORAGE_LOG(WARN, "fail to start object manager", K(ret));
  } else if (OB_FAIL(startup_accel_handler_.start())) {
    STORAGE_LOG(WARN, "fail to start server startup task handler", KR(ret));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.start())) {
    STORAGE_LOG(ERROR, "server storage meta service fail", K(ret));
  } else if (OB_FAIL(multi_tenant_.create_hidden_sys_tenant())) {
    STORAGE_LOG(WARN, "fail to create hidden sys tenant", K(ret));
  } else if (OB_FAIL(construct_default_tenant_meta(tenant_id, meta))) {
    STORAGE_LOG(WARN, "fail to construct_default_tenant_meta", K(ret));
  } else if (OB_FAIL(multi_tenant_.convert_hidden_to_real_sys_tenant(meta.unit_))) {
    STORAGE_LOG(WARN, "fail to create_real_sys_tenant", K(ret));
  }
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_FAIL(ret)) {
  } else if (GCTX.is_shared_storage_mode()) {
    int64_t data_disk_size = meta.unit_.config_.data_disk_size();
    if (is_sys_tenant(tenant_id)) { // real_sys_tenant's data_disk_size = sys_unit_config + hidden_sys_data_disk_size
      data_disk_size += OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }
    if (OB_FAIL(multi_tenant_.update_tenant_data_disk_size(tenant_id, data_disk_size))) {
      STORAGE_LOG(WARN, "fail to update tenant data disk size", K(ret), K(tenant_id), K(data_disk_size));
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(multi_tenant_.get_tenant(tenant_id, tenant))) {
    STORAGE_LOG(WARN, "fail to get tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant->acquire_more_worker(TENANT_WORKER_COUNT, succ_num))) {
  } else if (OB_FAIL(guard_.switch_to(tenant_id))) { // switch mtl context
    STORAGE_LOG(ERROR, "fail to switch to sys tenant", K(ret));
  } else {
    ObLogService *log_service = MTL(logservice::ObLogService*);
    ObGarbageCollector *gc_svr = MTL(logservice::ObGarbageCollector*);
    if (OB_ISNULL(log_service) || OB_ISNULL(log_service->palf_env_) || OB_ISNULL(gc_svr)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "fail to switch to sys tenant", KP(log_service));
    } else {
      palf::PalfEnvImpl *palf_env_impl = &log_service->palf_env_->palf_env_impl_;
      palf::LogIOWorkerWrapper &log_iow_wrapper = palf_env_impl->log_io_worker_wrapper_;
      palf::LogIOWorkerConfig new_config;
      const int64_t mock_tenant_id = 1;
      gc_svr->stop_create_new_gc_task_ = true;
      palf_env_impl->init_log_io_worker_config_(1, mock_tenant_id, new_config);
      new_config.io_worker_num_ = 4;
      log_iow_wrapper.destory_and_free_log_io_workers_();
      if (OB_FAIL(log_iow_wrapper.create_and_init_log_io_workers_(
        new_config, mock_tenant_id, palf_env_impl->cb_thread_pool_.get_tg_id(), palf_env_impl->log_alloc_mgr_, palf_env_impl))) {
        STORAGE_LOG(WARN, "failed to create_and_init_log_io_workers_", K(new_config));
      } else if (FALSE_IT(log_iow_wrapper.log_writer_parallelism_ = new_config.io_worker_num_)) {
      } else if (FALSE_IT(log_iow_wrapper.is_user_tenant_ = true)) {
      } else if (OB_FAIL(log_iow_wrapper.start_()))  {
        STORAGE_LOG(WARN, "failed to start_ log_iow_wrapper", K(new_config));
      } else {
      }
    }
  }
  return ret;
}

void MockTenantModuleEnv::destroy()
{
  STORAGE_LOG(INFO, "destroy", K(destroyed_));

  if (server_fd_ > 0) {
    close(server_fd_);
  }
  // 没有解决模块退出顺序问题，直接强退
  //int fail_cnt= ::testing::UnitTest::GetInstance()->failed_test_case_count();
  //_Exit(fail_cnt);
  if (destroyed_) {
    return;
  }
  // 释放租户上下文
  guard_.release();

  startup_accel_handler_.destroy();

  multi_tenant_.stop();
  multi_tenant_.wait();
  multi_tenant_.destroy();
  ObKVGlobalCache::get_instance().destroy();
  SERVER_STORAGE_META_SERVICE.destroy();

  OB_STORAGE_OBJECT_MGR.stop();
  OB_STORAGE_OBJECT_MGR.wait();
  OB_STORAGE_OBJECT_MGR.destroy();

  ObTsMgr::get_instance().stop();
  ObTsMgr::get_instance().wait();
  ObTsMgr::get_instance().destroy();

  net_frame_.sql_nio_stop();
  net_frame_.stop();
  net_frame_.wait();
  net_frame_.destroy();
  if (!GCTX.is_shared_storage_mode()) {
    tmp_file::ObTmpBlockCache::get_instance().destroy();
  }
  tmp_file::ObTmpPageCache::get_instance().destroy();
  TG_STOP(lib::TGDefIDs::ServerGTimer);
  TG_WAIT(lib::TGDefIDs::ServerGTimer);
  TG_DESTROY(lib::TGDefIDs::ServerGTimer);

#ifdef OB_BUILD_SHARED_STORAGE
  share::ObMasterKeyGetter::instance().stop();
  share::ObMasterKeyGetter::instance().wait();
  share::ObMasterKeyGetter::instance().reset();
  OB_DEVICE_CONF_MGR.destroy();
#endif

  destroyed_ = true;

  chdir(curr_dir_);
  system(("rm -rf " + run_dir_).c_str());
}

} // namespace storage

// just for override HOOK
namespace transaction
{
int ObGtiSource::get_trans_id(int64_t &trans_id)
{
  static int64_t trans_id_start = 1000;
  trans_id = ATOMIC_FAA(&trans_id_start, 1 );
  return OB_SUCCESS;
}

} // end transaction

} // namespace oceanbase
