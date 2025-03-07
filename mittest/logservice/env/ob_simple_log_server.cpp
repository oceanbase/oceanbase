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
#define private public
#define protected public
#include "ob_simple_log_server.h"

#define USING_LOG_PREFIX RPC_TEST
#include <net/if.h>
#include <sys/ioctl.h>
#include "share/rpc/ob_batch_processor.h"
#include "share/ob_device_manager.h"                                // ObDeviceManager
#undef protected
#undef private

namespace oceanbase
{

namespace unittest
{
using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace palf;
using namespace palf::election;
using namespace logservice;

int MockNetKeepAliveAdapter::init(unittest::ObLogDeliver *log_deliver)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_deliver)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_deliver_ = log_deliver;
  }
  return ret;
}

bool MockNetKeepAliveAdapter::in_black_or_stopped(const common::ObAddr &server)
{
  return log_deliver_->need_filter_packet_by_blacklist(server);
}

bool MockNetKeepAliveAdapter::is_server_stopped(const common::ObAddr &server)
{
  UNUSED(server);
  return false;
}

bool MockNetKeepAliveAdapter::in_black(const common::ObAddr &server)
{
  return log_deliver_->need_filter_packet_by_blacklist(server);
}

int MockNetKeepAliveAdapter::get_last_resp_ts(const common::ObAddr &server,
                                              int64_t &last_resp_ts)
{
  if (log_deliver_->need_filter_packet_by_blacklist(server)) {
    last_resp_ts = 1;
  } else {
    last_resp_ts = common::ObTimeUtility::current_time();
  }
  return OB_SUCCESS;
}

uint32_t get_local_addr(const char *dev_name)
{
  int fd, intrface;
  struct ifreq buf[16];
  struct ifconf ifc;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return 0;
  }

  ifc.ifc_len = sizeof(buf);
  ifc.ifc_buf = (caddr_t)buf;
  if (ioctl(fd, SIOCGIFCONF, (char *)&ifc) != 0) {
    close(fd);
    return 0;
  }

  intrface = static_cast<int>(ifc.ifc_len / sizeof(struct ifreq));
  while (intrface-- > 0) {
    if (ioctl(fd, SIOCGIFFLAGS, (char *)&buf[intrface]) != 0) {
      continue;
    }
    if ((buf[intrface].ifr_flags & IFF_LOOPBACK) != 0)
      continue;
    if (!(buf[intrface].ifr_flags & IFF_UP))
      continue;
    if (dev_name != NULL && strcmp(dev_name, buf[intrface].ifr_name))
      continue;
    if (!(ioctl(fd, SIOCGIFADDR, (char *)&buf[intrface]))) {
      close(fd);
      return ((struct sockaddr_in *)(&buf[intrface].ifr_addr))->sin_addr.s_addr;
    }
  }
  close(fd);
  return 0;
}

std::string get_local_ip()
{
  uint32_t ip = get_local_addr("bond0");
 if (ip == 0) {
    ip = get_local_addr("eth0");
  }
  if (ip == 0) {
    return "";
  }
  return inet_ntoa(*(struct in_addr *)(&ip));
}

int ObSimpleLogServer::simple_init(
    const std::string &cluster_name,
    const common::ObAddr &addr,
    const int64_t node_id,
    ObTenantIOManager *tio_manager,
    LogMemberRegionMap *region_map,
    const bool is_bootstrap = false)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("simple_init", 0);
  SERVER_LOG(INFO, "simple_log_server simple_init start", K(node_id), K(addr_));
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(node_id, 0)) {
    malloc->create_and_add_tenant_allocator(node_id);
  }
  if (is_bootstrap) {
    // mittest的打印日志采用仲裁的方式
    OB_LOGGER.is_arb_replica_ = true;
    // ObTenantIOManager depend static instance, need init it in ObSimpleLogCluster
    ls_service_ = OB_NEW(ObLSService, "mittest");
    // use tenant_id_ to init ObTenantBase
    tenant_base_ = OB_NEW(ObLogMittestTenantBase, "TestBase", cluster_id_, tenant_id_, node_id);
    timer_service_ = OB_NEW(ObTimerService, "TimerService", node_id);
    tenant_base_->init();
    tenant_base_->set(&log_service_);
    tenant_base_->set(ls_service_);
    tenant_base_->set(&detector_);
    tenant_base_->set(&shared_timer_);
    tenant_base_->set(tio_manager);
    tenant_base_->set(timer_service_);
    tenant_base_->unit_max_cpu_ = 100;
    tenant_base_->unit_min_cpu_ = 100;
  }
  ObTenantEnv::set_tenant(tenant_base_);
  assert(&log_service_ == MTL(logservice::ObLogService*));
  if (is_bootstrap) {
    assert(nullptr != timer_service_ && timer_service_ == MTL(ObTimerService*));
  }
  guard.click("init tenant_base");
  node_id_ = node_id;

  if (is_bootstrap && OB_FAIL(timer_service_->start())) {
    SERVER_LOG(ERROR, "start timer service failed", K(ret), K_(node_id));
  } else if (is_bootstrap && OB_FAIL(init_memory_dump_timer_())) {
    SERVER_LOG(ERROR, "init_memory_dump_timer_ failed", K(ret), K_(node_id));
  } else if (is_bootstrap && OB_FAIL(mock_locality_manager_.init(region_map))) {
    SERVER_LOG(ERROR, "mock_locality_manager_ init fail", K(ret));
  } else if (FALSE_IT(guard.click("init_memory_dump_timer_"))
      || OB_FAIL(init_network_(addr, is_bootstrap))) {
    SERVER_LOG(WARN, "init_network failed", K(ret), K(addr));
  } else if (FALSE_IT(guard.click("init_network_")) || OB_FAIL(init_io_(cluster_name))) {
    SERVER_LOG(WARN, "init_io failed", K(ret), K(addr));
  } else if (FALSE_IT(guard.click("init_io_")) || OB_FAIL(init_log_service_(is_bootstrap))) {
    SERVER_LOG(WARN, "init_log_service failed", K(ret), K(addr));
  } else if (FALSE_IT(guard.click("init_log_service")) || OB_FAIL(init_ls_service_(is_bootstrap))) {
    SERVER_LOG(ERROR, "init_ls_service_ failed", K(ret));
  } else if (FALSE_IT(guard.click("init_ls_service_")) || OB_FAIL(looper_.init(this))) {
    SERVER_LOG(WARN, "init ObLooper failed", K(ret), K(addr));
  } else {
    guard.click("init_log_service_");
    SERVER_LOG(INFO, "simple_log_server init success", KPC(this), K(guard));
  }
  return ret;
}

int ObSimpleLogServer::update_tenant_log_disk_size_(const uint64_t tenant_id,
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
    } else if (OB_FAIL(log_block_pool_.update_tenant(old_log_disk_size, new_log_disk_size, allowed_new_log_disk_size, log_service))) {
      LOG_WARN("failed to update teannt int ObServerLogBlockMGR", K(ret), K(tenant_id), K(new_log_disk_size),
               K(old_log_disk_size), K(allowed_new_log_disk_size));
    } else {
      disk_opts_.log_disk_usage_limit_size_ = allowed_new_log_disk_size;
      LOG_INFO("update_log_disk_usage_limit_size success", K(ret), K(tenant_id), K(new_log_disk_size),
               K(old_log_disk_size), K(allowed_new_log_disk_size), K(disk_opts_));
    }
  }
  return ret;
}

int ObSimpleLogServer::update_disk_opts_no_lock_(const PalfDiskOptions &opts)
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "begin update_disk_opts_no_lock_", K(opts), K(disk_opts_), K(inner_table_disk_opts_));
  int64_t old_log_disk_size = disk_opts_.log_disk_usage_limit_size_;
  int64_t new_log_disk_size = opts.log_disk_usage_limit_size_;
  int64_t allowed_new_log_disk_size = 0;
  // 内部表中的disk_opts立马生效
  inner_table_disk_opts_ = opts;
  // disk_opts_表示本地持久化最新的disk_opts，log_disk_percentage_延迟生效
  disk_opts_ = opts;
  disk_opts_.log_disk_usage_limit_size_ = old_log_disk_size;
  if (!opts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(opts));
  } else if (OB_FAIL(update_tenant_log_disk_size_(tenant_id_,
                                                  old_log_disk_size,
                                                  new_log_disk_size,
                                                  allowed_new_log_disk_size))) {
    CLOG_LOG(WARN, "update_tenant_log_disk_size_ failed", K(new_log_disk_size), K(old_log_disk_size),
             K(allowed_new_log_disk_size));
  } else {
    CLOG_LOG(INFO, "update_disk_opts success", K(opts), K(disk_opts_), K(new_log_disk_size), K(old_log_disk_size),
             K(allowed_new_log_disk_size));
  }
  return ret;
}

int ObSimpleLogServer::add_ls_to_ls_map_(const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;
  ObLSGetMod mod = ObLSGetMod::LOG_MOD;

  if (OB_SUCCESS == ls_service_->get_ls(ObLSID(palf_id), ls_handle, mod)) {
    ls = ls_handle.get_ls();
    LOG_INFO("ls exists", K(palf_id));
  } else if (OB_ISNULL(buf = ls_service_->ls_allocator_.alloc(sizeof(ObLS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ls", K(ret));
  } else {
    LOG_INFO("ls not exists", K(palf_id));
    ls = new (buf) ObLS();
    ls->ls_meta_.ls_id_ = ObLSID(palf_id);
    if (OB_FAIL(ls_service_->add_ls_to_map_(ls))) {
      LOG_WARN("failed to add_ls_to_map_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to do", KR(ret));
  } else if (FALSE_IT(ls->log_handler_.id_ = palf_id)) {
  } else if (FALSE_IT(ls->log_handler_.palf_env_ = palf_env_)) {
  } else if (FALSE_IT(ls->log_handler_.is_inited_ = true)) {
    // don't release ObLS when add it into map failed, the destroy function of ObLS
    // depend too many modules.
  } else {
    LOG_INFO("add ls success", K(ret), K(palf_id));
  }
  return ret;
}


int ObSimpleLogServer::update_disk_opts(const PalfDiskOptions &opts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_disk_lock_);
  ret = update_disk_opts_no_lock_(opts);
  return ret;
}

int ObSimpleLogServer::try_resize()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(log_disk_lock_);
  if (disk_opts_ != inner_table_disk_opts_) {
    if (OB_FAIL(update_disk_opts_no_lock_(inner_table_disk_opts_))) {

    }
  }
  return ret;
}

int ObSimpleLogServer::update_server_log_disk(const int64_t log_disk_size)
{
  ObSpinLockGuard guard(log_disk_lock_);
  return log_block_pool_.resize_(log_disk_size);
}

int ObSimpleLogServer::init_memory_dump_timer_()
{
  int ret = OB_SUCCESS;
  common::ObFunction<bool()> print_memory_info = [=](){
      // 内存分配仍采取不同的租户id
      ObMallocAllocator::get_instance()->print_tenant_memory_usage(node_id_);
      ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(node_id_);
      ObMallocAllocator::get_instance()->print_tenant_memory_usage(OB_SERVER_TENANT_ID);
      ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(OB_SERVER_TENANT_ID);
      return false;};
  if (OB_FAIL(timer_.init_and_start(1, 1_s, "test timer"))) {
    SERVER_LOG(ERROR, "ObOccamTimer init failed", K(ret), K_(node_id));
  } else if (OB_FAIL(timer_.schedule_task_repeat(timer_handle_, 1_s, print_memory_info))) {
    SERVER_LOG(ERROR, "schedule_task failed", K(ret), K_(node_id));
  }
  return ret;
}

int ObSimpleLogServer::init_network_(const common::ObAddr &addr, const bool is_bootstrap = false)
{
  int ret = OB_SUCCESS;
  ObNetOptions opts;
  opts.rpc_io_cnt_ = 10;
  opts.high_prio_rpc_io_cnt_ = 10;
  opts.batch_rpc_io_cnt_ = 10;
  opts.tcp_user_timeout_ = 10 * 1000 * 1000; // 10s
  addr_ = addr;
  obrpc::ObRpcNetHandler::CLUSTER_ID = 1;
  if (is_bootstrap && OB_FAIL(net_.init(opts, 0))) {
    SERVER_LOG(ERROR, "net init fail", K(ret));
  } else if (OB_FAIL(deliver_.init(addr, is_bootstrap, this))) {
    SERVER_LOG(ERROR, "deliver_ init failed", K(ret));
  } else if (is_bootstrap && OB_FAIL(net_.add_rpc_listen(addr_.get_port(), handler_, transport_))) {
//  } else if (is_bootstrap &&(
//             OB_FAIL(net_.set_rpc_port(addr_.get_port()))
//             || OB_FAIL(net_.rpc_net_register(handler_, transport_))
//             || OB_FAIL(net_.net_keepalive_register()))) {
    SERVER_LOG(ERROR, "net_ listen failed", K(ret));
  } else if (is_bootstrap && OB_FAIL(srv_proxy_.init(transport_))) {
    SERVER_LOG(ERROR, "init srv_proxy_ failed");
  } else if (is_bootstrap && OB_FAIL(net_.add_high_prio_rpc_listen(addr_.get_port(), handler_, high_prio_rpc_transport_))) {
    SERVER_LOG(ERROR, "net_ listen failed", K(ret));
  } else if (is_bootstrap && OB_FAIL(net_.batch_rpc_net_register(handler_, batch_rpc_transport_))) {
    SERVER_LOG(ERROR, "batch_rpc_ init failed", K(ret));
  } else if (FALSE_IT(batch_rpc_transport_->set_bucket_count(10))) {
  } else if (is_bootstrap && OB_FAIL(batch_rpc_.init(batch_rpc_transport_, high_prio_rpc_transport_, addr))) {
    SERVER_LOG(ERROR, "batch_rpc_ init failed", K(ret));
  // } else if (is_bootstrap && OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::BRPC, batch_rpc_))) {
  } else if (is_bootstrap && OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::BRPC, batch_rpc_tg_id_))) {
    SERVER_LOG(ERROR, "batch_rpc_ init failed", K(ret));
  } else if (is_bootstrap && OB_FAIL(TG_SET_RUNNABLE_AND_START(batch_rpc_tg_id_, batch_rpc_))) {
    SERVER_LOG(ERROR, "batch_rpc_ start failed", K(ret));
  } else {
    deliver_.node_id_ = node_id_;
    SERVER_LOG(INFO, "init_network success", K(ret), K(addr_), K(node_id_), K(opts));
  }
  return ret;
}

int ObSimpleLogServer::init_io_(const std::string &cluster_name)
{
  int ret = OB_SUCCESS;
  ObTimeGuard guard("init_io_", 0);
  const std::string logserver_dir = cluster_name + "/port_" + std::to_string(addr_.get_port());
  std::string data_dir = logserver_dir;
  std::string clog_dir = logserver_dir + "/clog";
  std::string slog_dir = logserver_dir + "/slog";
  std::string sstable_dir = logserver_dir + "/sstable";
  std::vector<std::string> dirs{data_dir, slog_dir, clog_dir, sstable_dir};

  for (auto &dir : dirs) {
    if (-1 == mkdir(dir.c_str(), 0777)) {
      if (errno == EEXIST) {
        ret = OB_SUCCESS;
        SERVER_LOG(INFO, "for restart");
      } else {
        SERVER_LOG(WARN, "mkdir failed", K(errno));
        ret = OB_IO_ERROR;
        break;
      }
    }
  }
  guard.click("mkdir");
  if (OB_SUCC(ret)) {
    io_device_ = OB_NEW(ObLocalDevice, "TestBase");

    blocksstable::ObStorageEnv storage_env;
    storage_env.data_dir_ = data_dir.c_str();
    storage_env.sstable_dir_ = sstable_dir.c_str();
    storage_env.default_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    storage_env.data_disk_size_ = 1024 * 1024 * 1024;
    storage_env.data_disk_percentage_ = 0;
    // 当disk_opts_有效时，使用disk_opts_中记录的log_disk_usage_limit_size_作为log_block_pool_的初始值，否则重启会失败
    storage_env.log_disk_size_ = disk_opts_.is_valid() ? disk_opts_.log_disk_usage_limit_size_ : 2LL * 1024 * 1024 * 1024;
    storage_env.log_disk_percentage_ = 0;

    storage_env.log_spec_.log_dir_ = slog_dir.c_str();
    storage_env.log_spec_.max_log_file_size_ = ObLogConstants::MAX_LOG_FILE_SIZE;
    storage_env.clog_dir_ = clog_dir.c_str();
    iod_opts_.opts_ = iod_opt_array_;
    iod_opt_array_[0].set("data_dir", storage_env.data_dir_);
    iod_opt_array_[1].set("sstable_dir", storage_env.sstable_dir_);
    iod_opt_array_[2].set("block_size", reinterpret_cast<const char*>(&storage_env.default_block_size_));
    iod_opt_array_[3].set("datafile_disk_percentage", storage_env.data_disk_percentage_);
    iod_opt_array_[4].set("datafile_size", storage_env.data_disk_size_);
    iod_opts_.opt_cnt_ = MAX_IOD_OPT_CNT;

    if (OB_FAIL(io_device_->init(iod_opts_))) {
      SERVER_LOG(ERROR, "init io device fail", K(ret));
    } else if (OB_FAIL(log_block_pool_.init(storage_env.clog_dir_))) {
      SERVER_LOG(ERROR, "init log pool fail", K(ret));
    } else {
      guard.click("init io_device");
      clog_dir_ = clog_dir;
      SERVER_LOG(INFO, "init_io_ successs", K(ret), K(guard));
    }
    if (OB_SUCC(ret)) {
      log_block_pool_.get_tenants_log_disk_size_func_ = [](int64_t &log_disk_size) -> int
      {
        // ObServerLogBlockMGR 率先于 ObLogService加载，此时租户使用的log_disk_size为0.
        log_disk_size = 0;
        return OB_SUCCESS;
      };
      if (OB_FAIL(log_block_pool_.start(storage_env.log_disk_size_))) {
        LOG_ERROR("log pool start failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSimpleLogServer::init_log_service_(const bool is_bootstrap)
{
  int ret = OB_SUCCESS;
  // init deps of log_service
  palf::PalfOptions opts;
  if (disk_opts_.is_valid()) {
    opts.disk_options_ = disk_opts_;
    opts.enable_log_cache_ = true;
  } else {
    opts.disk_options_.log_disk_usage_limit_size_ = 2 * 1024 * 1024 * 1024ul;
    opts.disk_options_.log_disk_utilization_threshold_ = 80;
    opts.disk_options_.log_disk_utilization_limit_threshold_ = 95;
    opts.disk_options_.log_disk_throttling_percentage_ = 100;
    opts.disk_options_.log_disk_throttling_maximum_duration_ = 2 * 3600 * 1000 * 1000L;
    opts.disk_options_.log_writer_parallelism_ = 2;
    disk_opts_ = opts.disk_options_;
    inner_table_disk_opts_ = disk_opts_;
    opts.enable_log_cache_ = true;
  }
  std::string clog_dir = clog_dir_ + "/tenant_" + std::to_string(tenant_id_);
  // 内存分配器仍使用不同的tenant_id
  allocator_ = OB_NEW(ObTenantMutilAllocator, "TestBase", node_id_);
  ObMemAttr attr(1, "SimpleLog");
  ObMemAttr ele_attr(1, ObNewModIds::OB_ELECTION);
  net_keepalive_ = MTL_NEW(MockNetKeepAliveAdapter, "SimpleLog");
  omt::ObSharedTimer *shared_timer_ptr = &shared_timer_;
  if (OB_FAIL(net_keepalive_->init(&deliver_))) {
  } else if (OB_FAIL(omt::ObSharedTimer::mtl_init(shared_timer_ptr))) {
    SERVER_LOG(ERROR, "init_log_service_ fail", K(ret));
  } else if (OB_FAIL(init_log_kv_cache_())) {
  } else if (OB_FAIL(log_service_.init(opts, clog_dir.c_str(), addr_, allocator_, transport_, &batch_rpc_, ls_service_,
      &location_service_, &reporter_, &log_block_pool_, &sql_proxy_, net_keepalive_, &mock_locality_manager_))) {
    SERVER_LOG(ERROR, "init_log_service_ fail", K(ret));
  } else if (OB_FAIL(log_block_pool_.create_tenant(opts.disk_options_.log_disk_usage_limit_size_))) {
    SERVER_LOG(ERROR, "crete tenant failed", K(ret));
  } else if (OB_FAIL(mock_election_map_.init(ele_attr))) {
    SERVER_LOG(ERROR, "mock_election_map_ init fail", K(ret));
  } else if (is_bootstrap && OB_FAIL(ckpt_map_.init("CKPTMap", OB_SERVER_TENANT_ID))) {
    SERVER_LOG(ERROR, "ckpt_map_ init fail", K(ret));
  } else if (OB_FAIL(log_service_.get_palf_env()->start())) {
    SERVER_LOG(ERROR, "palf_env start fail", K(ret));
  } else {
    palf_env_ = log_service_.get_palf_env();
    palf_env_->palf_env_impl_.log_rpc_.tenant_id_ = tenant_id_;
    SERVER_LOG(INFO, "init_log_service_ success", K(ret), K(opts), K(disk_opts_));
  }
  return ret;
}

int ObSimpleLogServer::init_ls_service_(const bool is_bootstrap)
{
  int ret = OB_SUCCESS;
  if (!is_bootstrap) {
  } else {
    if (OB_FAIL(ls_service_->ls_allocator_.init(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                              "mittest",
                                              node_id_,
                                              1024*1024*1024))) {
      LOG_WARN("fail to init ls allocator, ", K(ret));
    } else if (OB_FAIL(ls_service_->ls_map_.init(node_id_, &ls_service_->ls_allocator_))) {
      LOG_WARN("fail to init ls map", K(ret));
    } else {
      ls_service_->is_inited_ = true;
    }
  }
  auto func_iterate_palf = [this](const palf::PalfHandle &palf_handle) -> int {
    int ret = OB_SUCCESS;
    int64_t palf_id = INVALID_PALF_ID;
    palf_handle.palf_handle_impl_->get_palf_id(palf_id);
    if (OB_FAIL(add_ls_to_ls_map_(palf_id))) {
      LOG_WARN("failed to add_ls_to_map_", K(ret));
    } else {
      LOG_INFO("add ls success", K(ret), K(palf_id));
    }
    return ret;
  };
  return log_service_.iterate_palf(func_iterate_palf);
}

int ObSimpleLogServer::simple_start(const bool is_bootstrap = false)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(tenant_base_);
  omt::ObSharedTimer *shared_timer = &shared_timer_;
  if (is_bootstrap && nullptr == timer_service_) {
    ret = OB_ERR_NULL_VALUE;
    SERVER_LOG(ERROR, "timer_service_ is NULL", K(ret));
  } else if (is_bootstrap && OB_FAIL(timer_service_->start())) {
    SERVER_LOG(ERROR, "ObTimerService start failed", K(ret));
  } else if (is_bootstrap && OB_FAIL(net_.start())) {
    SERVER_LOG(ERROR, "net start fail", K(ret));
  } else if (OB_FAIL(deliver_.start())) {
    SERVER_LOG(ERROR, "deliver_ start failed", K(ret));
  } else if (OB_FAIL(log_service_.arb_service_.start())) {
    SERVER_LOG(ERROR, "arb_service start failed", K(ret));
  #ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(omt::ObSharedTimer::mtl_start(shared_timer))) {
    SERVER_LOG(ERROR, "shared_log_service start failed", K(ret));
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(log_service_.shared_log_service_.start())) {
    SERVER_LOG(ERROR, "shared_log_service start failed", K(ret));
  #endif
  } else if (OB_FAIL(looper_.start())) {
    SERVER_LOG(ERROR, "ObLooper start failed", K(ret));
  }
  #ifdef OB_BUILD_SHARED_STORAGE
  if (OB_SUCC(ret) && GCTX.is_shared_storage_mode())
  {
    //add ls to shared_log_service
    common::ObFunction<int(const palf::PalfHandle&)> add_ls = [&](const palf::PalfHandle &palf_handle) {
    int ret = OB_SUCCESS;
    int64_t palf_id = -1;
    palf_handle.get_palf_id(palf_id);
    if (OB_FAIL(log_service_.shared_log_service_.add_ls(ObLSID(palf_id)))) {
      SERVER_LOG(WARN, "ls_array push_back failed", K(palf_id));
    } else {
      SERVER_LOG(INFO, "add ls to shared_log_service success", K(palf_id));
    }
    return ret;
    };
    if (OB_FAIL(log_service_.iterate_palf(add_ls))) {
      SERVER_LOG(ERROR, "iterate_palf failed");
    }
  }
  #endif
  // do not start entire log_service_ for now, it will
  // slow down cases running
  return ret;
}

int ObSimpleLogServer::simple_close(const bool is_shutdown = false)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(tenant_base_);
  looper_.destroy();
  ObTimeGuard guard("simple_close", 0);
  deliver_.destroy(is_shutdown);
  guard.click("destroy");
  log_service_.stop();
  log_service_.wait();
  log_service_.destroy();
  // don't destroy ls_service_, otherwise the destroy function of ObLS may be executed.
  //ls_service_->destroy();
  log_block_pool_.destroy();

  if (GCTX.is_shared_storage_mode()) {
    omt::ObSharedTimer *shared_timer = &shared_timer_;
    omt::ObSharedTimer::mtl_stop(shared_timer);
    omt::ObSharedTimer::mtl_wait(shared_timer);
  }
  guard.click("destroy_palf_env");
  if (OB_LOG_KV_CACHE.inited_) {
    OB_LOG_KV_CACHE.destroy();
  }


  if (is_shutdown) {
    TG_STOP(batch_rpc_tg_id_);
    TG_WAIT(batch_rpc_tg_id_);
    TG_DESTROY(batch_rpc_tg_id_);
    batch_rpc_tg_id_ = -1;

    net_.rpc_shutdown();
    net_.stop();
    net_.wait();
    net_.destroy();

    timer_handle_.stop_and_wait();
    timer_.destroy();
    mock_locality_manager_.destroy();

    timer_service_->stop();
    timer_service_->wait();
    timer_service_->destroy();
  }
  SERVER_LOG(INFO, "stop LogService success", K(ret), K(is_shutdown), K(guard));
  return ret;
}

int ObSimpleLogServer::simple_restart(const std::string &cluster_name,
                                      const int64_t node_idx,
                                      ObTenantIOManager *tio_manager)
{
  int ret = OB_SUCCESS;
  ObTenantEnv::set_tenant(tenant_base_);
  ObTimeGuard guard("simple_restart", 0);
  if (OB_FAIL(simple_close())) {
    SERVER_LOG(ERROR, "simple_close failed", K(ret));
  } else if (FALSE_IT(guard.click("simple_close")) || OB_FAIL(simple_init(cluster_name, addr_, node_idx, tio_manager, NULL))) {
    SERVER_LOG(ERROR, "simple_init failed", K(ret));
  } else if (FALSE_IT(guard.click("simple_init")) || OB_FAIL(simple_start())) {
    SERVER_LOG(ERROR, "simple_start failed", K(ret));
  } else {
    guard.click("simple_start");
    SERVER_LOG(INFO, "simple_restart success", K(ret), K(guard));
  }
  return ret;
}

int ObSimpleLogServer::init_log_kv_cache_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_LOG_KV_CACHE.init(OB_LOG_KV_CACHE_NAME, 1))) {
    if (OB_INIT_TWICE == ret || OB_INVALID_ARGUMENT == ret) {
      ret = OB_SUCCESS;
    } else {
      PALF_LOG(WARN, "OB_LOG_KV_CACHE init failed", KR(ret));
    }
  }
  return ret;
}

int ObMittestBlacklist::init(const common::ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blacklist_.create(1024))) {
    SERVER_LOG(WARN, "create blacklist_ failed", K(ret));
  } else if (false == blacklist_.created()) {
    SERVER_LOG(WARN, "blacklist_ created failed");
  } else if (OB_FAIL(pcode_blacklist_.create(1024))) {
    SERVER_LOG(WARN, "create pcode_blacklist_ failed", K(ret));
  } else if (false == pcode_blacklist_.created()) {
    SERVER_LOG(WARN, "pcode_blacklist_ created failed");
  } else {
    self_ = self;
  }
  return ret;
}

void ObMittestBlacklist::block_net(const ObAddr &src)
{
  blacklist_.set_refactored(src);
  SERVER_LOG(INFO, "block_net", K(src), K(blacklist_));
}

void ObMittestBlacklist::unblock_net(const ObAddr &src)
{
  blacklist_.erase_refactored(src);
  SERVER_LOG(INFO, "unblock_net", K(src), K(blacklist_));
}

void ObMittestBlacklist::block_pcode(const ObRpcPacketCode &pcode)
{
  pcode_blacklist_.set_refactored((int64_t)pcode);
  SERVER_LOG(INFO, "block_pcode", K(pcode), K(pcode_blacklist_));
}

void ObMittestBlacklist::unblock_pcode(const ObRpcPacketCode &pcode)
{
  pcode_blacklist_.erase_refactored((int64_t)pcode);
  SERVER_LOG(INFO, "unblock_pcode", K(pcode), K(pcode_blacklist_));
}

bool ObMittestBlacklist::need_filter_packet_by_blacklist(const ObAddr &addr)
{
  return OB_HASH_EXIST == blacklist_.exist_refactored(addr);
}

bool ObMittestBlacklist::need_filter_packet_by_pcode_blacklist(const ObRpcPacketCode &pcode)
{
  return OB_HASH_EXIST == pcode_blacklist_.exist_refactored((int64_t)pcode);
}

void ObMittestBlacklist::set_rpc_loss(const ObAddr &src, const int loss_rate)
{
  // not thread safe
  int old_loss_rate = -1;
  bool is_exist = false;
  for (int64_t i = 0; i < rpc_loss_config_.count(); ++i) {
    LossConfig &tmp_config = rpc_loss_config_[i];
    if (tmp_config.src_ == src) {
      is_exist = true;
      // src exists, update its loss_rate
      old_loss_rate = tmp_config.loss_rate_;
      tmp_config.loss_rate_ = loss_rate;
    }
  }
  if (!is_exist) {
    LossConfig loss_config(src, loss_rate);
    rpc_loss_config_.push_back(loss_config);
  }
  SERVER_LOG(INFO, "set_rpc_loss", K(src), K(loss_rate), K(old_loss_rate));
}

void ObMittestBlacklist::reset_rpc_loss(const ObAddr &src)
{
  // not thread safe
  int64_t idx = -1;
  for (int64_t i = 0; i < rpc_loss_config_.count(); ++i) {
    LossConfig tmp_config;
    int ret = rpc_loss_config_.at(i, tmp_config);
    if (OB_SUCCESS != ret) {
      SERVER_LOG(WARN, "at failed", K(ret), K(i), "count", rpc_loss_config_.count());
    } else if (tmp_config.src_ == src) {
      // src exists, update its loss_rate
      idx = i;
    }
  }
  if (idx >= 0) {
    rpc_loss_config_.remove(idx);
  }
  SERVER_LOG(INFO, "reset_rpc_loss", K(src), K(idx));
}

void ObMittestBlacklist::get_loss_config(const ObAddr &src, bool &exist, LossConfig &loss_config)
{
  // not thread safe
  exist = false;
  for (int64_t i = 0; i < rpc_loss_config_.count(); ++i) {
    LossConfig tmp_config;
    int ret = rpc_loss_config_.at(i, tmp_config);
    if (OB_SUCCESS != ret) {
      SERVER_LOG(WARN, "at failed", K(ret));
    } else if (tmp_config.src_ == src) {
      exist = true;
      loss_config = tmp_config;
    }
  }
  SERVER_LOG(INFO, "get_loss_config", K(src), K(loss_config));
}

bool ObMittestBlacklist::need_drop_by_loss_config(const ObAddr &addr)
{
  bool bool_ret = false;
  bool exist = false;
  LossConfig loss_config;
  get_loss_config(addr, exist, loss_config);
  if (exist && loss_config.loss_rate_ > 0) {
    const int64_t random_num = ObRandom::rand(1, 100);
    bool_ret = random_num <= loss_config.loss_rate_ ? true : false;
    if (bool_ret) {
      SERVER_LOG(INFO, "need drop req by loss config", K(addr));
    }
  }
  return bool_ret;
}

int ObLogDeliver::init(const common::ObAddr &self,
                       const bool is_bootstrap,
                       ObSimpleLogServer *log_server)
{
  int ret = OB_SUCCESS;
  if (!self.is_valid() || OB_ISNULL(log_server)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(self), KP(log_server));
  } else if (is_bootstrap && OB_FAIL(ObMittestBlacklist::init(self))) {
    SERVER_LOG(WARN, "ObMittestBlacklist init failed", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::COMMON_QUEUE_THREAD, tg_id_))) {
    SERVER_LOG(WARN, "ObSimpleThreadPool::init failed", K(ret));
  } else {
    log_server_ = log_server;
    is_inited_ = true;
    SERVER_LOG(INFO, "ObLogDeliver init success", KP(&blacklist_));
  }
  return ret;
}

void ObLogDeliver::destroy(const bool is_shutdown)
{
  if (IS_NOT_INIT) {
    SERVER_LOG(INFO, "ObLogDeliver not init");
  } else {
    RWLock::WLockGuard guard(lock_);
    stop();
    wait();
    is_inited_ = false;
    TG_DESTROY(tg_id_);
    if (is_shutdown) {
      blacklist_.destroy();
      pcode_blacklist_.destroy();
    }
    tg_id_ = 0;
    log_server_ = NULL;
    SERVER_LOG(INFO, "destroy ObLogDeliver");
  }
}

int ObLogDeliver::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(ERROR, "ObLogDeliver not init");
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    SERVER_LOG(ERROR, "start ObLogDeliver failed");
  } else {
    is_stopped_ = false;
    SERVER_LOG(INFO, "start ObLogDeliver success");
  }
  return ret;
}

void ObLogDeliver::stop()
{
  if (IS_NOT_INIT) {
    SERVER_LOG_RET(WARN, OB_NOT_INIT, "ObLogDeliver stop failed");
  } else {
    is_stopped_ = true;
    TG_STOP(tg_id_);
    SERVER_LOG(INFO, "stop ObLogDeliver success");
  }
}

int ObLogDeliver::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(ERROR, "ObLogDeliver not inited!!!", K(ret));
  } else {
    TG_WAIT(tg_id_);
    SERVER_LOG(INFO, "wait ObLogDeliver success", K(tg_id_));
  }
  return ret;
}

int ObLogDeliver::deliver(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req.get_packet());
  SERVER_LOG(INFO, "deliver request", K(pkt), K(req.ez_req_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    do {
      RWLock::RLockGuard guard(lock_);
      if (false == is_stopped_) {
        ret = TG_PUSH_TASK(tg_id_, &req);
        if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
          SERVER_LOG(ERROR, "deliver request failed", K(ret), K(pkt), K(req.ez_req_));
        }
      } else {
        break;
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

void ObLogDeliver::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObLogDeliver not init", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid argument", KP(task));
  } else {
    rpc::ObRequest *req = static_cast<rpc::ObRequest *>(task);
    SERVER_LOG(INFO, "handle request", KP(req->ez_req_));
    (void) handle_req_(*req);
  }
}

int ObLogDeliver::handle_req_(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  // tenant_id of log servers are different for clear log record,
  // so set tenant_id of RpcPacket to OB_INVALID_TENANT_ID, this
  // will make the PALF_ENV_ID to OB_SERVER_TENANT_ID
  const ObRpcPacket &pkt = dynamic_cast<const ObRpcPacket&>(req.get_packet());
  //ObRpcPacket &modify_pkt = const_cast<ObRpcPacket&>(pkt);
  //modify_pkt.set_tenant_id(OB_SERVER_TENANT_ID);
  SERVER_LOG(TRACE, "handle_req_ trace", K(pkt), K(node_id_));
  const ObRpcPacketCode pcode = pkt.get_pcode();
  ObFunction<bool(const ObAddr &src)> filter = [&](const ObAddr &src) -> bool {
    if (this->need_filter_packet_by_blacklist(src)) {
      SERVER_LOG(WARN, "need_filter_packet_by_blacklist", K(ret), K(pcode), K(src), K(blacklist_));
      return true;
    } else if (this->need_drop_by_loss_config(src)) {
      SERVER_LOG(WARN, "need_drop_by_loss_config", K(ret), K(pcode), K(src), K(rpc_loss_config_));
      return true;
    }
    return false;
  };
  if (this->need_filter_packet_by_pcode_blacklist(pcode)) {
    SERVER_LOG(WARN, "need_filter_packet_by_pcode_blacklist", K(ret), K(pcode), K(pcode_blacklist_));
    return OB_SUCCESS;
  }
  switch (pkt.get_pcode()) {
    #define BATCH_RPC_PROCESS() \
      ObBatchP p;\
      p.init(); \
      p.set_ob_request(req);\
      p.run();\
      break;
    #define PROCESS(processer) \
    processer p;\
    p.init();\
    p.set_ob_request(req);\
    p.set_filter(&filter);  \
    p.run();      \
    break;
    case obrpc::OB_LOG_PUSH_REQ: {
      PROCESS(LogPushReqP);
    }
    case obrpc::OB_LOG_PUSH_RESP: {
      PROCESS(LogPushRespP);
    }
    case obrpc::OB_LOG_FETCH_REQ: {
      PROCESS(LogFetchReqP);
    }
    case obrpc::OB_LOG_BATCH_FETCH_RESP: {
      PROCESS(LogBatchFetchRespP);
    }
    case obrpc::OB_LOG_PREPARE_REQ: {
      PROCESS(LogPrepareReqP);
    }
    case obrpc::OB_LOG_PREPARE_RESP: {
      PROCESS(LogPrepareRespP);
    }
    case obrpc::OB_LOG_CHANGE_CONFIG_META_REQ: {
      PROCESS(LogChangeConfigMetaReqP);
    }
    case obrpc::OB_LOG_CHANGE_CONFIG_META_RESP: {
      PROCESS(LogChangeConfigMetaRespP);
    }
    case obrpc::OB_LOG_CHANGE_MODE_META_REQ: {
      PROCESS(LogChangeModeMetaReqP);
    }
    case obrpc::OB_LOG_CHANGE_MODE_META_RESP: {
      PROCESS(LogChangeModeMetaRespP);
    }
    case obrpc::OB_LOG_NOTIFY_REBUILD_REQ: {
      PROCESS(LogNotifyRebuildReqP)
    }
    case obrpc::OB_LOG_COMMITTED_INFO: {
      PROCESS(CommittedInfoP)
    }
    case obrpc::OB_LOG_LEARNER_REQ: {
      PROCESS(LogLearnerReqP)
    }
    case obrpc::OB_LOG_REGISTER_PARENT_REQ: {
      PROCESS(LogRegisterParentReqP)
    }
    case obrpc::OB_LOG_REGISTER_PARENT_RESP: {
      PROCESS(LogRegisterParentRespP)
    }
    case obrpc::OB_LOG_ELECTION_PREPARE_REQUEST : {
      PROCESS(ElectionPrepareRequestMsgP)
    }
    case obrpc::OB_LOG_ELECTION_PREPARE_RESPONSE : {
      PROCESS(ElectionPrepareResponseMsgP)
    }
    case obrpc::OB_LOG_ELECTION_ACCEPT_REQUEST : {
      PROCESS(ElectionAcceptRequestMsgP)
    }
    case obrpc::OB_LOG_ELECTION_ACCEPT_RESPONSE : {
      PROCESS(ElectionAcceptResponseMsgP)
    }
    case obrpc::OB_LOG_ELECTION_CHANGE_LEADER_REQUEST : {
      PROCESS(ElectionChangeLeaderMsgP)
    }
    case obrpc::OB_LOG_GET_MC_ST: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogGetMCStP)
    }
    case obrpc::OB_LOG_ARB_PROBE_MSG: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(logservice::LogServerProbeP)
    }
    case obrpc::OB_LOG_CONFIG_CHANGE_CMD: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogMembershipChangeP)
    }
    case obrpc::OB_LOG_GET_PALF_STAT: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogGetPalfStatReqP)
    }
    case obrpc::OB_LOG_CHANGE_ACCESS_MODE_CMD: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogChangeAccessModeP)
    }
    case obrpc::OB_LOG_FLASHBACK_CMD: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogFlashbackMsgP)
    }
    case obrpc::OB_LOG_GET_STAT: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogGetStatP)
    }
    case obrpc::OB_LOG_NOTIFY_FETCH_LOG: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogNotifyFetchLogReqP)
    }
    case obrpc::OB_LOG_GET_LS_CKPT: {
      ObFunction<int(const share::ObLSID &ls_id, share::SCN &scn, palf::LSN &lsn)> ckpt_functor =
        [&](const share::ObLSID &ls_id, share::SCN &scn, palf::LSN &lsn) -> int {
        int ret = OB_SUCCESS;
        if (!ls_id.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(log_server_->get_mock_ls_ckpt(ls_id.id(), scn, lsn))) {
          SERVER_LOG(WARN, "allocate memory failed");
        }
        return ret;
      };
      //modify_pkt.set_tenant_id(node_id_);
      MockLogGetCkptReqP p;
      p.init();
      p.set_ob_request(req);
      p.set_filter(&filter);
      p.set_ckpt_functor(&ckpt_functor);
      p.run();
      break;
    }
    case obrpc::OB_BATCH: {
      // modify_pkt.set_tenant_id(node_id_);
      BATCH_RPC_PROCESS()
    }
    case obrpc::OB_LOG_PROBE_RS: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogProbeRsP);
    }
    case obrpc::OB_LOG_SYNC_BASE_LSN_REQ: {
      PROCESS(logservice::LogSyncBaseLSNReqP)
    }
    case obrpc::OB_LOG_ACQUIRE_REBUILD_INFO: {
      //modify_pkt.set_tenant_id(node_id_);
      PROCESS(LogAcquireRebuildInfoP)
    }
    default:
      SERVER_LOG(ERROR, "invalid req type", K(pkt.get_pcode()));
      break;
  }
  return ret;
}

ObLooper::ObLooper() : log_server_(nullptr),
                       run_interval_(0),
                       is_inited_(false) { }

ObLooper::~ObLooper()
{
  destroy();
}

int ObLooper::init(ObSimpleLogServer *log_server)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "ObLooper has been inited", K(ret));
  } else if (NULL == log_server) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(log_server));
  } else {
    log_server_ = log_server;
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    run_interval_ = ObLooper::INTERVAL_US;
    is_inited_ = true;
  }

  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    destroy();
  }
  PALF_LOG(INFO, "ObLooper init finished", K(ret));
  return ret;
}

void ObLooper::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  log_server_ = NULL;
}

void ObLooper::run1()
{
  lib::set_thread_name("ObLooper");
  log_loop_();
}

void ObLooper::log_loop_()
{

  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    const int64_t start_ts = ObTimeUtility::current_time();

    if (OB_FAIL(log_server_->try_resize())) {
      PALF_LOG(WARN, "try_resize failed", K(ret));
    }
    const int64_t round_cost_time = ObTimeUtility::current_time() - start_ts;
    int32_t sleep_ts = run_interval_ - static_cast<const int32_t>(round_cost_time);
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    ob_usleep(sleep_ts);

    if (REACH_THREAD_TIME_INTERVAL(5 * 1000 * 1000)) {
      PALF_LOG(INFO, "ObLooper round_cost_time(us)", K(round_cost_time));
    }
  }
}

int ObSimpleLogServer::set_mock_ls_ckpt(const int64_t palf_id, const SCN scn, const LSN lsn)
{
  int ret = OB_SUCCESS;
  std::pair<share::SCN, palf::LSN> ckpt;
  ckpt.first = scn;
  ckpt.second = lsn;
  if (OB_FAIL(ckpt_map_.insert_or_update(ObLSID(palf_id), ckpt))) {
    CLOG_LOG(WARN, "insert_or_update failed", KR(ret), K(palf_id));
  } else {
    CLOG_LOG(INFO, "set_mock_ls_ckpt", KR(ret), K(palf_id), K(scn), K(lsn));
  }
  return ret;
}

int ObSimpleLogServer::get_mock_ls_ckpt(const int64_t palf_id, share::SCN &scn, LSN &lsn) const
{
  int ret = OB_SUCCESS;
  std::pair<share::SCN, palf::LSN> ckpt;
  if (OB_FAIL(ckpt_map_.get(ObLSID(palf_id), ckpt))) {
    CLOG_LOG(WARN, "insert_or_update failed", KR(ret), K(palf_id));
  } else {
    scn = ckpt.first;
    lsn = ckpt.second;
    CLOG_LOG(INFO, "set_mock_ls_ckpt", KR(ret), K(palf_id), K(scn), K(lsn));
  }
  return ret;
}

int ObSimpleLogServer::create_ls(const int64_t palf_id,
                                 const AccessMode &access_mode,
                                 const PalfBaseInfo &base_info,
                                 IPalfHandleImpl *&palf_handle_impl)
{
  int ret = OB_SUCCESS;

  IPalfEnvImpl *palf_env_impl = get_palf_env();
  if (OB_ISNULL(palf_env_impl)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_env_impl->create_palf_handle_impl(palf_id, palf::AccessMode::APPEND, base_info, palf_handle_impl))) {
    CLOG_LOG(WARN, "create palf handle impll failed", K(ret), K(palf_id));
  } else if (OB_FAIL(add_ls_to_ls_map_(palf_id))) {
    CLOG_LOG(WARN, "add_ls_to_ls_map_ failed", K(ret), K(palf_id));
  } else {
    LOG_INFO("add ls success", K(ret), K(palf_id));
  }
  return ret;
}

int ObSimpleLogServer::remove_ls(const int64_t palf_id)
{
  int ret = OB_SUCCESS;

  IPalfEnvImpl *palf_env_impl = get_palf_env();
  if (OB_ISNULL(palf_env_impl)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error", K(ret), K(palf_id));
  } else if (OB_FAIL(palf_env_impl->remove_palf_handle_impl(palf_id))) {
    CLOG_LOG(WARN, "create palf handle impll failed", K(ret), K(palf_id));
//  Don't remove ls, otherwise the destroy function of ObLS may be executed.
//  } else if (OB_FAIL(ls_service_->remove_ls_from_map_(ObLSID(palf_id)))) {
//    CLOG_LOG(WARN, "failed to remove_ls_from_map_", K(ret));
  } else {
    CLOG_LOG(INFO, "remove ls success", K(ret), K(palf_id));
  }
  return ret;
}

} // unittest
} // oceanbase
