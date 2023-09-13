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
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "sql/ob_sql.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/lob/ob_lob_manager.h"
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
#include "ob_mittest_utils.h"
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
#include "logservice/palf/log_define.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "observer/table/ob_htable_lock_mgr.h"
#include "observer/table/ob_table_session_pool.h"

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
  int submit_tablet_update_task(const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
  {
    UNUSED(tenant_id);
    UNUSED(ls_id);
    UNUSED(tablet_id);
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

common::ObIODevice* get_device_inner()
{
  int ret = OB_SUCCESS;
  common::ObIODevice* device = NULL;
  common::ObString storage_info(OB_LOCAL_PREFIX);
  //for the local and nfs, storage_prefix and storage info are same
  if(OB_FAIL(common::ObDeviceManager::get_instance().get_device(storage_info, storage_info, device))) {
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
  static int init_slogger_mgr(const char *log_dir);

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
      10000, // max_iops
      10000, // min_iops,
      0 /*iops_weight*/);

  if (OB_FAIL(unit_config.init(unit_config_id, name, ur))) {
    STORAGE_LOG(WARN, "fail to init unit config unit", KR(ret), K(unit_config_id), K(name), K(ur));
  } else if (OB_FAIL(unit.init(tenant_id,
                        unit_id,
                        share::ObUnitInfoGetter::ObUnitStatus::UNIT_NORMAL,
                        unit_config,
                        lib::Worker::CompatMode::MYSQL,
                        create_timestamp,
                        has_memstore,
                        false /*is_removed*/))) {
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
  }

  storage_env_.data_dir_ = data_dir;
  storage_env_.sstable_dir_ = file_dir;
  storage_env_.clog_dir_ = clog_dir;
  storage_env_.default_block_size_ = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_size_ = macro_block_count * common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_env_.data_disk_percentage_ = 0;
  storage_env_.log_disk_size_ = 20 * 1024 * 1024 * 1024ll;
  share::ObLocalDevice *local_device = static_cast<share::ObLocalDevice*>(get_device_inner());
  THE_IO_DEVICE = local_device;
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
  } else if (OB_FAIL(THE_IO_DEVICE->init(iod_opts))) {
    STORAGE_LOG(WARN, "fail to init io device", K(ret), K_(storage_env));
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    STORAGE_LOG(WARN, "fail to init io manager", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(THE_IO_DEVICE,
        async_io_thread_count,
        sync_io_thread_count,
        max_io_depth))) {
    STORAGE_LOG(WARN, "add device channel failed", K(ret));
  } else if (OB_FAIL(log_block_mgr_.init(storage_env_.clog_dir_))) {
    SERVER_LOG(ERROR, "init log pool fail", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    STORAGE_LOG(WARN, "fail to start io manager", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_tenant_io_manager(OB_SERVER_TENANT_ID, io_config))) {
    STORAGE_LOG(WARN, "add tenant io config failed", K(ret));
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
  GCTX.server_id_ = 1;
  GCTX.rs_rpc_proxy_ = &rs_rpc_proxy_;
  GCTX.srv_rpc_proxy_ = &srv_rpc_proxy_;
  GCTX.rs_mgr_ = &rs_mgr_;
  GCTX.config_ = &config_;
  GCTX.disk_reporter_ = &mock_disk_reporter_;
  GCTX.bandwidth_throttle_ = &bandwidth_throttle_;
  GCTX.log_block_mgr_ = &log_block_mgr_;
}

int MockTenantModuleEnv::init_slogger_mgr(const char *log_dir)
{
  int ret = OB_SUCCESS;

  const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;
  blocksstable::ObLogFileSpec log_file_spec;
  log_file_spec.retry_write_policy_ = "normal";
  log_file_spec.log_create_policy_ = "normal";
  log_file_spec.log_write_policy_ = "truncate";
  if (OB_FAIL(SLOGGERMGR.init(log_dir, MAX_FILE_SIZE, log_file_spec))) {
    STORAGE_LOG(WARN, "fail to init SLOGGERMGR", K(ret));
  }

  return ret;
}


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
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(THE_IO_DEVICE, 2 * 1024 * 1024))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.init())) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_instance().init(env_dir_.c_str(), env_dir_.c_str(), 1, env_dir_.c_str(), self_addr_))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
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
  } else if (OB_FAIL(init_slogger_mgr(slog_dir_.c_str()))) {
    STORAGE_LOG(WARN, "fail to init env", K(ret));
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().init())) {
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
  } else if (OB_SUCCESS != (ret = bandwidth_throttle_.init(1024 *1024 * 60))) {
    STORAGE_LOG(ERROR, "failed to init bandwidth_throttle_", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ServerGTimer))) {
    STORAGE_LOG(ERROR, "init timer fail", KR(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::MemDumpTimer))) {
    STORAGE_LOG(ERROR, "init memory dump timer fail", KR(ret));
  } else {
    obrpc::ObRpcNetHandler::CLUSTER_ID = 1;
    oceanbase::palf::election::INIT_TS = 1;
    // 忽略cgroup的报错
    cgroup_ctrl_.init();
    ObTsMgr::get_instance_inner() = &ts_mgr_;
    GCTX.sql_proxy_ = &sql_proxy_;
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
    } else if (FALSE_IT(init_gctx_gconf())) {
    } else if (OB_FAIL(init_before_start_mtl())) {
      STORAGE_LOG(ERROR, "init_before_start_mtl failed", K(ret));
    } else {
      oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
      MTL_BIND(ObTenantIOManager::mtl_init, ObTenantIOManager::mtl_destroy);
      MTL_BIND2(mtl_new_default, omt::ObSharedTimer::mtl_init, omt::ObSharedTimer::mtl_start, omt::ObSharedTimer::mtl_stop, omt::ObSharedTimer::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantSchemaService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObStorageLogger::mtl_init, ObStorageLogger::mtl_start, ObStorageLogger::mtl_stop, ObStorageLogger::mtl_wait, mtl_destroy_default);
      MTL_BIND2(ObTenantMetaMemMgr::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTransService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, logservice::ObGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTimestampService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTransIDService::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObXAService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObLSService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObAccessService::mtl_init, nullptr, mtl_stop_default, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantFreezer::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, checkpoint::ObCheckPointService::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, checkpoint::ObTabletGCService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObLogService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantTabletScheduler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::ObTenantDagScheduler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTenantCheckpointSlogHandler::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, coordinator::ObLeaderCoordinator::mtl_init, coordinator::ObLeaderCoordinator::mtl_start, coordinator::ObLeaderCoordinator::mtl_stop, coordinator::ObLeaderCoordinator::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, coordinator::ObFailureDetector::mtl_init, coordinator::ObFailureDetector::mtl_start, coordinator::ObFailureDetector::mtl_stop, coordinator::ObFailureDetector::mtl_wait, mtl_destroy_default);
      MTL_BIND2(ObLobManager::mtl_new, mtl_init_default, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::detector::ObDeadLockDetectorMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::ObTenantTabletStatMgr::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default)
      MTL_BIND2(mtl_new_default, storage::ObTenantSSTableMergeInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, share::ObDagWarningHistoryManager::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, compaction::ObScheduleSuspectInfoMgr::mtl_init, nullptr, nullptr, nullptr, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::ObTenantFreezeInfoMgr::mtl_init, nullptr, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObSharedMacroBlockMgr::mtl_init,  mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, storage::mds::ObTenantMdsService::mtl_init, storage::mds::ObTenantMdsService::mtl_start, storage::mds::ObTenantMdsService::mtl_stop, storage::mds::ObTenantMdsService::mtl_wait, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObMultiVersionGarbageCollector::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, ObTableLockService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(server_obj_pool_mtl_new<transaction::ObPartTransCtx>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<transaction::ObPartTransCtx>);
      MTL_BIND2(server_obj_pool_mtl_new<ObTableScanIterator>, nullptr, nullptr, nullptr, nullptr, server_obj_pool_mtl_destroy<ObTableScanIterator>);
      MTL_BIND(ObTenantSQLSessionMgr::mtl_init, ObTenantSQLSessionMgr::mtl_destroy);
      MTL_BIND2(mtl_new_default, ObRebuildService::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND(table::ObHTableLockMgr::mtl_init, table::ObHTableLockMgr::mtl_destroy);
      MTL_BIND2(mtl_new_default, omt::ObTenantSrs::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
      MTL_BIND2(mtl_new_default, table::ObTableApiSessPoolMgr::mtl_init, mtl_start_default, mtl_stop_default, mtl_wait_default, mtl_destroy_default);
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(GMEMCONF.reload_config(config_))) {
      STORAGE_LOG(ERROR, "reload memory config failed", K(ret));
    } else if (OB_FAIL(start_())) {
      STORAGE_LOG(ERROR, "mock env start failed", K(ret));
    } else if (OB_FAIL(ObTmpFileManager::get_instance().init())) {
      STORAGE_LOG(WARN, "init_tmp_file_manager failed", K(ret));
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
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.start(0))) {
    STORAGE_LOG(WARN, "fail to start block manager", K(ret));
  } else if (OB_FAIL(ObServerCheckpointSlogHandler::get_instance().mock_start())) {
    STORAGE_LOG(ERROR, "server checkpoint slog handler mock start fail", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(multi_tenant_.create_hidden_sys_tenant())) {
    STORAGE_LOG(WARN, "fail to create hidden sys tenant", K(ret));
  } else if (OB_FAIL(construct_default_tenant_meta(tenant_id, meta))) {
    STORAGE_LOG(WARN, "fail to construct_default_tenant_meta", K(ret));
  } else if (OB_FAIL(multi_tenant_.convert_hidden_to_real_sys_tenant(meta.unit_))) {
    STORAGE_LOG(WARN, "fail to create_real_sys_tenant", K(ret));
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

  if (OB_SUCC(ret)) {
    if (OB_FAIL(MTL(ObStorageLogger*)->start_log(meta.super_block_.replay_start_point_))) {
      STORAGE_LOG(WARN, "tenant start_log failed", K(ret), K(tenant_id));
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


  multi_tenant_.stop();
  multi_tenant_.wait();
  multi_tenant_.destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObServerCheckpointSlogHandler::get_instance().destroy();
  SLOGGERMGR.destroy();
  ObTmpFileManager::get_instance().destroy();

  OB_SERVER_BLOCK_MGR.stop();
  OB_SERVER_BLOCK_MGR.wait();
  OB_SERVER_BLOCK_MGR.destroy();

  ObTsMgr::get_instance().stop();
  ObTsMgr::get_instance().wait();
  ObTsMgr::get_instance().destroy();

  net_frame_.sql_nio_stop();
  net_frame_.stop();
  net_frame_.wait();
  net_frame_.destroy();

  TG_STOP(lib::TGDefIDs::ServerGTimer);
  TG_WAIT(lib::TGDefIDs::ServerGTimer);
  TG_DESTROY(lib::TGDefIDs::ServerGTimer);

  TG_STOP(lib::TGDefIDs::MemDumpTimer);
  TG_WAIT(lib::TGDefIDs::MemDumpTimer);
  TG_DESTROY(lib::TGDefIDs::MemDumpTimer);

  THE_IO_DEVICE->destroy();


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
