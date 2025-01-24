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

#include "ob_simple_log_cluster_testbase.h"
#include "ob_mittest_utils.h"
#include "lib/alloc/memory_dump.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#define private public
#define protected public
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "src/share/ob_device_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_shared_log_utils.h"
#endif
#undef protected
#undef private
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager

namespace oceanbase
{
using namespace logservice;
namespace unittest
{
// int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
// int64_t ObSimpleLogClusterTestBase::node_cnt_ = 7;
std::vector<ObISimpleLogServer*> ObSimpleLogClusterTestBase::cluster_;
bool ObSimpleLogClusterTestBase::is_started_ = false;
common::ObMemberList ObSimpleLogClusterTestBase::member_list_ = ObMemberList();
common::hash::ObHashMap<common::ObAddr, common::ObRegion> ObSimpleLogClusterTestBase::member_region_map_;
common::ObMemberList ObSimpleLogClusterTestBase::node_list_ = ObMemberList();
int64_t ObSimpleLogClusterTestBase::node_idx_base_ = 1002;
char ObSimpleLogClusterTestBase::sig_buf_[sizeof(ObSignalWorker) + sizeof(observer::ObSignalHandle)];
ObSignalWorker *ObSimpleLogClusterTestBase::sig_worker_ = new (sig_buf_) ObSignalWorker();
observer::ObSignalHandle *ObSimpleLogClusterTestBase::signal_handle_ = new (sig_worker_ + 1) observer::ObSignalHandle();
bool ObSimpleLogClusterTestBase::disable_hot_cache_ = false;
int64_t ObSimpleLogClusterTestBase::tenant_id_ = ObISimpleLogServer::DEFAULT_TENANT_ID;
ObTenantIOManager *ObSimpleLogClusterTestBase::tio_manager_ = nullptr;

void ObSimpleLogClusterTestBase::SetUpTestCase()
{
  SERVER_LOG(INFO, "SetUpTestCase", K(member_cnt_), K(node_cnt_));
  ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  int ret = OB_SUCCESS;
  if (!is_started_) {
    ret = start();
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

void ObSimpleLogClusterTestBase::TearDownTestCase()
{
  SERVER_LOG(INFO, "TearDownTestCase", K(member_cnt_), K(node_cnt_));
  int ret = OB_SUCCESS;

  if (cluster_.size() != 0) {
    ret = close();
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  for (auto svr : cluster_) {
    if (OB_NOT_NULL(svr)) {
      ob_delete(svr);
    }
  }
  ObTimerService::get_instance().stop();
  ObTimerService::get_instance().wait();
  ObTimerService::get_instance().destroy();
}

int ObSimpleLogClusterTestBase::init_log_shared_storage_()
{
  int ret = OB_SUCCESS;
  if (!need_shared_storage_) {
    return ret;
  }
  char base_dir_c[1024];
  getcwd(base_dir_c, 1024);
  std::string base_dir = std::string(base_dir_c) + "/" + test_name_ + "/bucket";
  ObDeviceConfig device_config;
  std::string mkdir = "mkdir " + base_dir;
  std::string rmdir = "rm -rf " + base_dir;
  system(rmdir.c_str());
  system(mkdir.c_str());
  std::string root_path = "file://" + base_dir;
  const char *used_for = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_LOG);
  const char *state = "ADDED";
  MEMCPY(device_config.path_, root_path.c_str(), root_path.size());
  MEMCPY(device_config.used_for_, used_for, strlen(used_for));
  device_config.sub_op_id_ = 1;
  STRCPY(device_config.state_, state);
  device_config.op_id_ = 1;
  device_config.last_check_timestamp_ = 0;
  device_config.storage_id_ = 1;
  device_config.max_iops_ = 0;
  device_config.max_bandwidth_ = 0;
  const uint64_t test_memory = 6L * 1024L * 1024L * 1024L;
  char tenant_uri[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  uint64_t storage_id = OB_INVALID_ID;
  common::ObBackupIoAdapter io_adapter;
  if (OB_FAIL(ObDeviceConfigMgr::get_instance().init(base_dir.c_str()))) {
    SERVER_LOG(ERROR, "init ObDeviceConfigMgr failed");
  } else if (OB_FAIL(ObDeviceConfigMgr::get_instance().add_device_config(device_config))) {
    SERVER_LOG(ERROR, "add_device_config failed");
  } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
    SERVER_LOG(ERROR, "get_storage_dest_ failed");
  } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.construct_tenant_str_(
      dest, tenant_id_, tenant_uri, OB_MAX_URI_LENGTH))) {
    SERVER_LOG(ERROR, "construct_tenant_str_ failed");
  } else if (OB_FAIL(io_adapter.mkdir(tenant_uri, dest.get_storage_info()))) {
    SERVER_LOG(ERROR, "mkdir failed", K(tenant_uri));
  } else {
    GCTX.startup_mode_ = ObServerMode::SHARED_STORAGE_MODE;
    SERVER_LOG(INFO, "init_log_shared_storage_ success", K(tenant_uri));
  }

  return ret;
}

int ObSimpleLogClusterTestBase::start()
{
  int ret = OB_SUCCESS;
  int64_t member_cnt = 0;
  ObTenantMutilAllocatorMgr::get_instance().init();
  auto malloc = ObMallocAllocator::get_instance();
  malloc->create_and_add_tenant_allocator(tenant_id_);
  ObMemoryDump::get_instance().init();
  // set easy allocator for watching easy memory holding
  easy_pool_set_allocator(ob_easy_realloc);
  ev_set_allocator(ob_easy_realloc);
  lib::set_memory_limit(10L * 1000L * 1000L * 1000L);
  const uint64_t mittest_memory = 6L * 1024L * 1024L * 1024L;
  ObTenantBase *tmp_base = OB_NEW(ObTenantBase, "mittest", tenant_id_);
  share::ObTenantEnv::set_tenant(tmp_base);
  const std::string clog_dir = test_name_;
  const int64_t disk_io_thread_count = 8;
  const int64_t max_io_depth = 256;
  if (sig_worker_ != nullptr && OB_FAIL(sig_worker_->start())) {
    SERVER_LOG(ERROR, "Start signal worker error", K(ret));
  } else if (signal_handle_ != nullptr && OB_FAIL(signal_handle_->start())) {
    SERVER_LOG(ERROR, "Start signal handle error", K(ret));
  } else if (OB_FAIL(member_region_map_.create(OB_MAX_MEMBER_NUMBER,
      ObMemAttr(MTL_ID(), ObModIds::OB_HASH_NODE, ObCtxIds::DEFAULT_CTX_ID)))) {
  } else if (OB_FAIL(generate_sorted_server_list_(node_cnt_))) {
    SERVER_LOG(ERROR, "generate_sorted_server_list_ failed", K(ret));
  } else if (OB_FAIL(G_RES_MGR.init())) {
    SERVER_LOG(ERROR, "init ObResourceManager failed", K(ret));
  } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    STORAGE_LOG(WARN, "init device manager failed", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init(mittest_memory))) {
    SERVER_LOG(ERROR, "init ObIOManager failed");
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    SERVER_LOG(ERROR, "start ObIOManager failed");
  } else if (OB_FAIL(init_log_shared_storage_())) {
    SERVER_LOG(ERROR, "init_for_shared_storage failed", K(ret));
  } else if (OB_FAIL(ObTenantIOManager::mtl_new(tio_manager_))) {
    SERVER_LOG(ERROR, "new tenant io manager failed", K(ret));
  } else if (OB_FAIL(ObTenantIOManager::mtl_init(tio_manager_))) {
    SERVER_LOG(ERROR, "init tenant io manager failed", K(ret));
  } else if (OB_FAIL(tio_manager_->start())) {
    SERVER_LOG(ERROR, "start tenant io manager failed", K(ret));
  } else if (OB_FAIL(init_global_kv_cache_())) {
  } else if (OB_FAIL(LOG_IO_DEVICE_WRAPPER.init(clog_dir.c_str(), disk_io_thread_count, max_io_depth, &OB_IO_MANAGER, &ObDeviceManager::get_instance()))) {
    SERVER_LOG(ERROR, "LOG_IO_DEVICE_WRAPPER init failed", K(ret));
  } else {
    ObTenantIOConfig io_config;
    io_config.unit_config_.max_iops_ = 10000000;
    io_config.unit_config_.min_iops_ = 10000000;
    io_config.unit_config_.weight_ = 10000000;
    tio_manager_->update_basic_io_config(io_config);
    // 如果需要新增arb server，将其作为memberlist最后一项
    // TODO by runlin, 这个是暂时的解决方法，以后可以走加减成员的流程
    const int64_t arb_idx = member_cnt_ - 1;
    int64_t node_id = node_idx_base_;
    for (int i = 0; OB_SUCC(ret) && i < node_cnt_; i++) {
      ObISimpleLogServer *svr = NULL;
      if (i == arb_idx && true == need_add_arb_server_) {
        svr = OB_NEW(ObSimpleArbServer, "TestBase");
      } else {
        svr = OB_NEW(ObSimpleLogServer, "TestBase");
      }
      common::ObAddr server;
      if (OB_FAIL(node_list_.get_server_by_index(i, server))) {
      } else if (OB_FAIL(svr->simple_init(test_name_, server, node_id, tio_manager_, &member_region_map_, true))) {
        SERVER_LOG(WARN, "simple_init failed", K(ret), K(i), K_(node_list));
      } else if (OB_FAIL(svr->simple_start(true))) {
        SERVER_LOG(WARN, "simple_start failed", K(ret), K(i), K_(node_list));
      } else {
        node_id += 2;
        cluster_.push_back(svr);
      }
      if (i < ObSimpleLogClusterTestBase::member_cnt_ && OB_SUCC(ret)) {
        common::ObMember member;
        if (OB_FAIL(member_list_.add_member(ObMember(server, 1)))) {
        } else if (OB_FAIL(member_region_map_.set_refactored(server, DEFAULT_REGION_NAME))) {
          SERVER_LOG(WARN, "member_region_map_.insert failed", K(ret), K(server), K_(node_list));
        }
      }
      usleep(500);
      SERVER_LOG(INFO, "ObSimpleLogClusterTestBase start success", K(node_id), K(server));
    }
    if (OB_SUCC(ret)) {
      is_started_ = true;
    }
    SERVER_LOG(INFO, "ObSimpleLogClusterTestBase started", K(ret), K_(member_cnt), K_(node_cnt), K_(node_list), K(member_list_));
  }
  return ret;
}

int ObSimpleLogClusterTestBase::close()
{
  int ret = OB_SUCCESS;
  for (auto svr : cluster_) {
    ret = svr->simple_close(true);
    if (OB_FAIL(ret)) {
      SERVER_LOG(WARN, "simple_close failed", K(ret));
      break;
    }
  }

  OB_LOG_KV_CACHE.destroy();
  ObKVGlobalCache::get_instance().destroy();

  LOG_IO_DEVICE_WRAPPER.destroy();
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  ObDeviceManager::get_instance().destroy();
  return ret;
}

int ObSimpleLogClusterTestBase::generate_sorted_server_list_(const int64_t node_cnt)
{
  int ret = OB_SUCCESS;
  // each get_rpc_port calling will get two available ports,
  // so just get node_cnt / 2 + 1 times
  const int64_t get_port_cnt = node_cnt / 2 + 1;
  for (int i = 0; i < get_port_cnt; i++) {
    int server_fd = 0;
    const std::string local_ip = get_local_ip();
    const int64_t port = get_rpc_port(server_fd);
    common::ObAddr addr;
    if (0 == port) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "get_rpc_port failed", K(ret), K(port));
      break;
    } else if (local_ip == "") {
      ret = OB_ERR_UNEXPECTED;
    } else if (false == addr.set_ip_addr(local_ip.c_str(), port)) {
      SERVER_LOG(ERROR, "set_ip_addr failed", K(local_ip.c_str()), K(port), K(addr));
    } else if (OB_FAIL(node_list_.add_server(addr))) {
      PALF_LOG(WARN, "add_server failed", K(ret));
    } else if (false == addr.set_ip_addr(local_ip.c_str(), port + 1)) {
      SERVER_LOG(ERROR, "set_ip_addr failed", K(local_ip.c_str()), K(port), K(addr));
    } else if (node_list_.get_member_number() < node_cnt && OB_FAIL(node_list_.add_server(addr))) {
      PALF_LOG(WARN, "add_server failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    SERVER_LOG(INFO, "simple log cluster node_list", K_(node_list));
  }
  return ret;
}

int ObSimpleLogClusterTestBase::init_global_kv_cache_()
{
  int ret = OB_SUCCESS;
  const int64_t KV_CACHE_WASH_TIMER_INTERVAL_US = 60 * 1000L * 1000L;
  const int64_t DEFAULT_BUCKET_NUM = 10000000L;
  const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(
          &ObTenantMemLimitGetter::get_instance(), DEFAULT_BUCKET_NUM,
          DEFAULT_MAX_CACHE_SIZE, lib::ACHUNK_SIZE,
          KV_CACHE_WASH_TIMER_INTERVAL_US))) {
    if (OB_INIT_TWICE == ret) {
      ret = OB_SUCCESS;
    } else {
      PALF_LOG(WARN, "ObKVGlobalCache init failed", KR(ret));
    }
  }
  return ret;
}

} // end unittest
} // end oceanbase
