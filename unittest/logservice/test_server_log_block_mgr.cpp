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

#include <gtest/gtest.h>
#include <atomic>
#include <functional>
#include <set>
#include <thread>

#define private public
#define protected public
#include "logservice/ob_server_log_block_mgr.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_io_adapter.h"
#include "logservice/palf/palf_env.h"
#include "observer/omt/ob_multi_tenant.h"
#undef protected
#undef private

#include "logservice/ob_log_io_adapter.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant.h"
#include "src/share/ob_device_manager.h"
#include "share/ob_local_device.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_cluster_version.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace logservice;
using namespace common;
using namespace palf;
namespace unittest
{
static const int64_t DEFAULT_LS_COUNT = 10;
static int ls_id_array_[DEFAULT_LS_COUNT] = {1,    1001, 1002, 1003, 1004,
                                             1005, 1006, 1007, 1008, 1009};
static const char *log_or_meta_[2] = {"log", "meta"};
void init_io()
{
  int ret = OB_SUCCESS;
  // set easy allocator for watching easy memory holding
  lib::set_memory_limit(10L * 1000L * 1000L * 1000L);
  const uint64_t mittest_memory = 6L * 1024L * 1024L * 1024L;
  const std::string clog_dir = "test_server_log_block_mgr_dir";
  const int64_t disk_io_thread_count = 8;
  const int64_t max_io_depth = 256;
  logservice::ObLogIOMode mode= logservice::ObLogIOMode::LOCAL;
  logservice::ObLogIOInfo io_info;
  if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    STORAGE_LOG(WARN, "init device manager failed", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init(mittest_memory))) {
    SERVER_LOG(ERROR, "init ObIOManager failed");
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    SERVER_LOG(ERROR, "start ObIOManager failed");
  } else if (OB_FAIL(io_info.init(mode, NULL, 1))) {
  } else if (OB_FAIL(LOG_IO_ADAPTER.init(clog_dir.c_str(), disk_io_thread_count, max_io_depth, io_info, &OB_IO_MANAGER, &ObDeviceManager::get_instance()))) {
    SERVER_LOG(ERROR, "LOG_IO_ADAPTER init failed", K(ret));
  }
}
class TestServerLogBlockMgr : public ::testing::Test
{
public:
  static void SetUpTestCase();

  static void TearDownTestCase();
  TestServerLogBlockMgr()
  {}
  virtual ~TestServerLogBlockMgr()
  {}

  static int create_ls_in_tenant(const int ls_count, const char *tenant_dir);

  static int remove_ls_in_tenant(const char *tenant_dir);

  int create_new_blocks_at(const int64_t ls_id, const std::string &dir_path,
                           const palf::block_id_t start_block_id, const int64_t block_cnt)
  {
    int ret = OB_SUCCESS;
    char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    for (int i = 0; i < block_cnt && OB_SUCC(ret); i++) {
      char create_block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      int64_t pos = 0;
      bool result = false;
      const palf::block_id_t block_id = start_block_id + i;
      databuff_printf(block_path, OB_MAX_FILE_NAME_LENGTH, pos, "%s/%lu",
                      dir_path.c_str(), block_id);
      if (OB_FAIL(log_block_mgr_.create_block(block_path, ObServerLogBlockMgr::BLOCK_SIZE))) {
        CLOG_LOG(ERROR, "create_block_at failed", K(ret), K(ls_id), K(start_block_id), K(block_id));
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(block_path, result))) {
        CLOG_LOG(ERROR, "is_exists failed", K(ret), K(block_path), K(result), K(i),
                 K(start_block_id));
      } else if (false == result) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "file not exist, unexpected error", K(ret), K(block_path),
                 K(start_block_id), K(i));
      } else {
      }
    }
    return ret;
  }

  int delete_blocks_at(const int64_t ls_id, const std::string &path,
                       const palf::block_id_t start_block_id, const int64_t block_cnt)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; i < block_cnt && OB_SUCC(ret); i++) {
      int64_t pos = 0;
      bool result = false;
      const palf::block_id_t block_id = start_block_id + i;
      char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
      snprintf(block_path, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", path.c_str(), block_id);
      if (OB_FAIL(log_block_mgr_.remove_block(block_path))) {
        CLOG_LOG(ERROR, "delete_new_block_at failed", K(ret), K(ls_id),
                 K(start_block_id), K(i));
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(block_path, result))) {
        CLOG_LOG(ERROR, "is_exists failed", K(ret), K(path.c_str()), K(result), K(i),
                 K(start_block_id));
      } else if (true == result) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "file exist, unexpected error", K(ret), K(path.c_str()),
                 K(start_block_id), K(start_block_id), K(i));
      } else {
      }
    }
    return ret;
  }

  static constexpr int64_t GB = 1024 * 1024 * 1024ul;
  int create_tenant(int key, int64_t size) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() != map_.find(key)) {
      ret = OB_ENTRY_EXIST;
    } else {
      ret = this->log_block_mgr_.create_tenant(size);
      if (OB_SUCC(ret)) {
        map_[key] = size;
      } else {
      }
    }
    return ret;
  };
  int remove_tenant(int key) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() == map_.find(key)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int64_t size = map_[key];
      map_.erase(key);
      this->log_block_mgr_.remove_tenant(size);
    }
    return ret;
  };
  int update_tenant(int key, int64_t new_size) {
    ObSpinLockGuard guard(this->lock_);
    int ret = OB_SUCCESS;
    if (map_.end() == map_.find(key)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      int64_t old_size = map_[key];
      int64_t tmp_min_log_disk_size_for_all_tenants = this->log_block_mgr_.min_log_disk_size_for_all_tenants_;
      tmp_min_log_disk_size_for_all_tenants -= old_size;
      tmp_min_log_disk_size_for_all_tenants += new_size;
      if (tmp_min_log_disk_size_for_all_tenants > this->log_block_mgr_.get_total_size_()) {
        ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      }
      if (OB_SUCCESS != ret) {
        map_[key] = old_size;
      } else {
        this->log_block_mgr_.min_log_disk_size_for_all_tenants_ = tmp_min_log_disk_size_for_all_tenants;
        map_[key] = new_size;
      }
    }
    return ret;
  };
public:
  virtual void SetUp();
  virtual void TearDown();
  static const char *log_pool_base_path_;
  static const char *log_disk_path_;
  static const char *tenant_string_;
  static std::map<int64_t, std::string> tenant_ls_map_;
  static std::vector<int> tenant_1_ls_fd_;
  uint64_t original_cluster_version_;
  bool original_enable_upgrade_mode_;
  ObServerLogBlockMgr log_block_mgr_;
  ObSpinLock lock_;
  std::map<int, int64_t> map_;
};

const char *TestServerLogBlockMgr::log_pool_base_path_ = "clog_disk/clog";
const char *TestServerLogBlockMgr::tenant_string_ = "clog_disk/clog/tenant_1";
std::map<int64_t, std::string> TestServerLogBlockMgr::tenant_ls_map_;
std::vector<int> TestServerLogBlockMgr::tenant_1_ls_fd_;

void TestServerLogBlockMgr::SetUpTestCase()
{
  bool result = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::create_directory(tenant_string_))) {
    CLOG_LOG(ERROR, "FileDirectoryUtils create_directory failed", K(ret),
             K(tenant_string_));
  } else if (OB_FAIL(create_ls_in_tenant(DEFAULT_LS_COUNT, tenant_string_))) {
    CLOG_LOG(ERROR, "create_ls_in_tenant failed", K(ret));
  } else {
    CLOG_LOG(INFO, "SetUpTestSuite success", K(log_pool_base_path_));
  }
}

void TestServerLogBlockMgr::TearDownTestCase()
{
  remove_ls_in_tenant(tenant_string_);
  system("rm -rf clog_disk");
}

int TestServerLogBlockMgr::create_ls_in_tenant(const int ls_count, const char *tenant_dir)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < ls_count && OB_SUCC(ret); i++) {
    char ls_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    snprintf(ls_path, OB_MAX_FILE_NAME_LENGTH, "%s/%d/%s", tenant_dir, ls_id_array_[i], log_or_meta_[i%2]);
    std::string mkdir_str = "mkdir -p "; mkdir_str += ls_path;
    system(mkdir_str.c_str());
    tenant_ls_map_.insert(std::make_pair(ls_id_array_[i], ls_path));
    CLOG_LOG(INFO, "create_ls_in_tenant_success", K(i), K(tenant_dir), K(ls_path));

  }
  return ret;
}

int TestServerLogBlockMgr::remove_ls_in_tenant(const char *tenant_dir)
{
  int ret = OB_SUCCESS;
  for (auto iter = tenant_ls_map_.begin(); iter != tenant_ls_map_.end(); iter++) {
    if (-1 == FileDirectoryUtils::delete_directory_rec(iter->second.c_str())) {
      ret = palf::convert_sys_errno();
      CLOG_LOG(ERROR, "::rmdir failed", K(ret));
    } else {
      CLOG_LOG(INFO, "remove_ls_in_tenant_success", K(iter->second.c_str()), K(tenant_dir));
    }
  }
  return ret;
}

void TestServerLogBlockMgr::SetUp()
{
  original_cluster_version_ = GET_MIN_CLUSTER_VERSION();
  original_enable_upgrade_mode_ = GCONF.enable_upgrade_mode;
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_8);
  GCONF.enable_upgrade_mode = false;
  OB_ASSERT(OB_SUCCESS == log_block_mgr_.init(log_pool_base_path_));
  log_block_mgr_.get_tenants_log_disk_size_func_ = [this](int64_t &out) -> int
  {
    for(auto pair : map_)
    {
      out += pair.second;
      CLOG_LOG(INFO, "current pair", K(pair.first), K(pair.second));
    }
    CLOG_LOG(INFO, "get_tenants_log_disk_size_func_ success", K(out), K(log_block_mgr_));
    return OB_SUCCESS;
  };
}

void TestServerLogBlockMgr::TearDown()
{
  log_block_mgr_.destroy();
  ObClusterVersion::get_instance().update_cluster_version(original_cluster_version_);
  GCONF.enable_upgrade_mode = original_enable_upgrade_mode_;
  GCONF.server_log_disk_utilization_limit_threshold.set_value("95");
  GCONF.server_log_disk_utilization_threshold.set_value("0");
}

using namespace palf;
TEST_F(TestServerLogBlockMgr, log_pool_meta_version_compatibility)
{
  char log_pool_meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char log_pool_meta_tmp_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(log_pool_meta_path, OB_MAX_FILE_NAME_LENGTH,
                                       "%s/meta", log_block_mgr_.log_pool_path_));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(log_pool_meta_tmp_path, OB_MAX_FILE_NAME_LENGTH,
                                       "%s.tmp", log_pool_meta_path));
  ObServerLogBlockMgr::LogPoolMeta meta = log_block_mgr_.log_pool_meta_;
  ObServerLogBlockMgr::LogPoolMetaEntry v1_entry(meta);
  v1_entry.version_ = ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V1;
  v1_entry.update_checksum();
  EXPECT_EQ(static_cast<int64_t>(ob_crc64(
                &v1_entry,
                offsetof(ObServerLogBlockMgr::LogPoolMetaEntry,
                         cross_tenant_reclaim_enabled_))),
            v1_entry.checksum_);

  int64_t pos = 0;
  memset(log_block_mgr_.log_pool_meta_serialize_buf_, '\0',
         ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE);
  ASSERT_EQ(OB_SUCCESS, v1_entry.serialize(log_block_mgr_.log_pool_meta_serialize_buf_,
                                           ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE,
                                           pos));
  ASSERT_EQ(OB_SUCCESS, write_until_success(
                            log_pool_meta_path,
                            log_block_mgr_.log_pool_meta_serialize_buf_,
                            ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE, 0));

  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
  EXPECT_EQ(meta, log_block_mgr_.log_pool_meta_);

  ASSERT_EQ(OB_SUCCESS,
            log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
                log_block_mgr_.log_pool_path_, log_pool_meta_path, meta,
                /* cross_tenant_reclaim_enabled */ true));
  bool tmp_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(log_pool_meta_tmp_path, tmp_file_exist));
  EXPECT_FALSE(tmp_file_exist);
  ObIOFd tmp_fd;
  ASSERT_EQ(OB_SUCCESS, LOG_IO_ADAPTER.open(
                            log_pool_meta_tmp_path, CREATE_FILE_FLAG, CREATE_FILE_MODE, tmp_fd));
  ASSERT_EQ(OB_SUCCESS, LOG_IO_ADAPTER.close(tmp_fd));
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_TRUE(log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
  EXPECT_EQ(meta, log_block_mgr_.log_pool_meta_);
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(log_pool_meta_tmp_path, tmp_file_exist));
  EXPECT_FALSE(tmp_file_exist);

  ObServerLogBlockMgr::LogPoolMetaEntry v2_entry(meta, true);
  v2_entry.update_checksum();
  uint64_t v2_checksum = ob_crc64(
      &v2_entry,
      offsetof(ObServerLogBlockMgr::LogPoolMetaEntry, cross_tenant_reclaim_enabled_));
  v2_checksum = ob_crc64(v2_checksum, &v2_entry.cross_tenant_reclaim_enabled_,
                         sizeof(v2_entry.cross_tenant_reclaim_enabled_));
  EXPECT_EQ(static_cast<int64_t>(v2_checksum), v2_entry.checksum_);
  EXPECT_TRUE(v2_entry.check_integrity());
  v2_entry.cross_tenant_reclaim_enabled_ = false;
  EXPECT_FALSE(v2_entry.check_integrity());

  char meta_buf[ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE] = {'\0'};
  v2_entry.cross_tenant_reclaim_enabled_ = true;
  v2_entry.update_checksum();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, v2_entry.serialize(meta_buf, sizeof(meta_buf), pos));
  meta_buf[2] = 0;
  meta_buf[3] = 3;
  ObServerLogBlockMgr::LogPoolMetaEntry unsupported_entry;
  pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            unsupported_entry.deserialize(meta_buf, sizeof(meta_buf), pos));
  for (const int64_t version : {ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V1,
                                ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V2}) {
    ObServerLogBlockMgr::LogPoolMetaEntry entry(meta, true);
    entry.version_ = version;
    entry.update_checksum();
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, entry.serialize(meta_buf, sizeof(meta_buf), pos));
    EXPECT_EQ(entry.get_serialize_size(), pos);
    const int64_t serialized_size = pos;
    for (int64_t size = 1; size < serialized_size; ++size) {
      ObServerLogBlockMgr::LogPoolMetaEntry truncated;
      pos = 0;
      EXPECT_NE(OB_SUCCESS, truncated.deserialize(meta_buf, size, pos));
      EXPECT_EQ(0, pos);
    }
    ObServerLogBlockMgr::LogPoolMetaEntry restored(meta, true);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, restored.deserialize(meta_buf, serialized_size, pos));
    EXPECT_TRUE(restored.check_integrity());
    EXPECT_EQ(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V2 == version,
              restored.cross_tenant_reclaim_enabled_);
    restored.magic_ = 0;
    restored.update_checksum();
    EXPECT_FALSE(restored.check_integrity());
  }
  ASSERT_EQ(OB_SUCCESS,
            log_block_mgr_.reclaim_service_.persist_cross_tenant_reclaim_enabled_(false));
}

TEST_F(TestServerLogBlockMgr, meta_version_follows_upgrade_gate)
{
  char meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(meta_path, sizeof(meta_path),
                                       "%s/meta", log_block_mgr_.log_pool_path_));
  const ObServerLogBlockMgr::LogPoolMeta meta = log_block_mgr_.log_pool_meta_;
  ObServerLogBlockMgr::LogPoolMetaEntry entry(meta);
  alignas(LOG_DIO_ALIGN_SIZE) char buf[ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE] = {'\0'};
  int64_t pos = 0;
  entry.version_ = ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V1;
  entry.update_checksum();
  ASSERT_EQ(OB_SUCCESS, entry.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, write_until_success(meta_path, buf, sizeof(buf), 0));
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_FALSE(log_block_mgr_.has_v2_meta_);

  const auto expect_meta_version = [&](const int64_t version) {
    EXPECT_EQ(OB_SUCCESS, read_until_success(meta_path, buf, sizeof(buf), 0));
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, entry.deserialize(buf, sizeof(buf), pos));
    EXPECT_TRUE(entry.check_integrity());
    EXPECT_EQ(version, entry.version_);
    EXPECT_FALSE(entry.cross_tenant_reclaim_enabled_);
  };
  GCONF.enable_upgrade_mode = true;
  for (const uint64_t version : {CLUSTER_VERSION_4_2_5_7, CLUSTER_VERSION_4_2_5_8}) {
    ObClusterVersion::get_instance().update_cluster_version(version);
    EXPECT_FALSE(log_block_mgr_.is_shared_log_pool_supported());
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
        log_block_mgr_.log_pool_path_, meta_path, meta, false));
    expect_meta_version(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V1);
    EXPECT_FALSE(log_block_mgr_.has_v2_meta_);
    EXPECT_EQ(OB_ERR_UNEXPECTED, log_block_mgr_.persist_cross_tenant_reclaim_enabled(true));
    EXPECT_FALSE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
  }

  GCONF.enable_upgrade_mode = false;
  EXPECT_TRUE(log_block_mgr_.is_shared_log_pool_supported());
  // A failed write must not publish the V2 marker or replace the existing V1 meta.
  EXPECT_NE(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
      log_block_mgr_.log_pool_path_, "missing_log_pool_parent/meta", meta, false));
  EXPECT_FALSE(log_block_mgr_.has_v2_meta_);
  expect_meta_version(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V1);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
      log_block_mgr_.log_pool_path_, meta_path, meta, false));
  EXPECT_TRUE(log_block_mgr_.has_v2_meta_);
  expect_meta_version(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V2);

  // Recover V2 while the live version gate is closed, without enabling sharing.
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_2_5_7);
  GCONF.enable_upgrade_mode = true;
  log_block_mgr_.destroy();
  EXPECT_FALSE(log_block_mgr_.has_v2_meta_);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_TRUE(log_block_mgr_.has_v2_meta_);
  EXPECT_TRUE(log_block_mgr_.is_shared_log_pool_supported());
  EXPECT_FALSE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
      log_block_mgr_.log_pool_path_, meta_path, meta, false));
  expect_meta_version(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V2);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(true));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
  EXPECT_TRUE(log_block_mgr_.has_v2_meta_);
  expect_meta_version(ObServerLogBlockMgr::LogPoolMetaEntry::VERSION_V2);
}

TEST_F(TestServerLogBlockMgr, meta_marker_is_published_before_resize)
{
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  const int64_t old_total_size = log_block_mgr_.log_pool_meta_.curr_total_size_;
  for (const bool enabled : {true, false}) {
    // Call the manager directly: its caller has not published any service state yet.
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(enabled));
    EXPECT_EQ(enabled, log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
    const int64_t resized_total_size = enabled
        ? old_total_size + ObServerLogBlockMgr::BLOCK_SIZE : old_total_size;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(resized_total_size));
    log_block_mgr_.destroy();
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
    EXPECT_EQ(enabled, log_block_mgr_.is_cross_tenant_reclaim_enabled());
    EXPECT_EQ(enabled ? LogReclaimState::DRAINING : LogReclaimState::DISABLED,
              log_block_mgr_.reclaim_service_.get_reclaim_state());
    EXPECT_EQ(resized_total_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  }
}

TEST_F(TestServerLogBlockMgr, server_reclaim_state_and_write_stop)
{
  ObServerLogBlockReclaimService &reclaim_service = log_block_mgr_.reclaim_service_;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  EXPECT_EQ(LogReclaimState::DISABLED, reclaim_service.get_reclaim_state());
  EXPECT_EQ(OB_ERR_UNEXPECTED, reclaim_service.plan_reclaim_(1));

  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("80"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ASSERT_EQ(OB_SUCCESS, reclaim_service.try_advance_reclaim(1));
  EXPECT_EQ(LogReclaimState::ENABLED, reclaim_service.get_reclaim_state());
  EXPECT_TRUE(reclaim_service.is_cross_tenant_reclaim_enabled());

  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  ASSERT_EQ(OB_SUCCESS,
            reclaim_service.try_advance_reclaim(
                1 + ObServerLogBlockReclaimService::RUN_INTERVAL));
  EXPECT_EQ(LogReclaimState::DRAINING, reclaim_service.get_reclaim_state());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ASSERT_EQ(OB_SUCCESS,
            reclaim_service.try_advance_reclaim(
                1 + 2 * ObServerLogBlockReclaimService::RUN_INTERVAL));
  EXPECT_EQ(LogReclaimState::ENABLED, reclaim_service.get_reclaim_state());

  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  ASSERT_EQ(OB_SUCCESS,
            reclaim_service.try_advance_reclaim(
                1 + 3 * ObServerLogBlockReclaimService::RUN_INTERVAL));
  ASSERT_EQ(OB_SUCCESS, reclaim_service.persist_cross_tenant_reclaim_enabled_(false));
  reclaim_service.reset();
  ASSERT_EQ(OB_SUCCESS, reclaim_service.init(&log_block_mgr_));
  EXPECT_EQ(LogReclaimState::DISABLED, reclaim_service.get_reclaim_state());

  ATOMIC_STORE(&reclaim_service.last_periodic_check_ts_, 0);
  ASSERT_EQ(OB_SUCCESS, reclaim_service.try_advance_reclaim(10));
  EXPECT_EQ(10, ATOMIC_LOAD(&reclaim_service.last_periodic_check_ts_));
  ASSERT_EQ(OB_SUCCESS, reclaim_service.try_advance_reclaim(11));
  EXPECT_EQ(10, ATOMIC_LOAD(&reclaim_service.last_periodic_check_ts_));
  ASSERT_EQ(OB_SUCCESS,
            reclaim_service.try_advance_reclaim(10 + ObServerLogBlockReclaimService::RUN_INTERVAL));
  EXPECT_EQ(10 + ObServerLogBlockReclaimService::RUN_INTERVAL,
            ATOMIC_LOAD(&reclaim_service.last_periodic_check_ts_));
}

TEST_F(TestServerLogBlockMgr, recover_reclaim_state_combinations)
{
  ObServerLogBlockReclaimService service;
  EXPECT_EQ(OB_NOT_INIT, service.try_advance_reclaim(1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, service.init(NULL));
  for (const bool meta_enabled : {false, true}) {
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(meta_enabled));
    for (const char *threshold : {"0", "80"}) {
      ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value(threshold));
      ASSERT_EQ(OB_SUCCESS, service.init(&log_block_mgr_));
      const LogReclaimState expected = !meta_enabled ? LogReclaimState::DISABLED
          : ('0' == threshold[0] ? LogReclaimState::DRAINING : LogReclaimState::ENABLED);
      EXPECT_EQ(expected, service.get_reclaim_state());
      EXPECT_EQ(meta_enabled, service.is_cross_tenant_reclaim_enabled());
      EXPECT_EQ(OB_INIT_TWICE, service.init(&log_block_mgr_));
      EXPECT_EQ(OB_INVALID_ARGUMENT, service.try_advance_reclaim(0));
      service.reset();
      EXPECT_EQ(LogReclaimState::INITIALIZING, service.get_reclaim_state());
      EXPECT_FALSE(service.is_cross_tenant_reclaim_enabled());
      EXPECT_FALSE(service.is_pool_write_stopped());
    }
  }
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
}

TEST_F(TestServerLogBlockMgr, draining_requires_all_tenants)
{
  ObServerLogBlockReclaimService &service = log_block_mgr_.reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.persist_cross_tenant_reclaim_enabled_(true));
  service.reset();
  ASSERT_EQ(OB_SUCCESS, service.init(&log_block_mgr_));
  ASSERT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());
  share::ObCgroupCtrl cgroup_ctrl;
  omt::ObTenant tenant1(1001, 1, cgroup_ctrl);
  omt::ObTenant tenant2(1002, 1, cgroup_ctrl);
  omt::ObMultiTenant omt;
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  // A complete scan before tenant replay must keep the recovered durable marker.
  ASSERT_FALSE(omt.has_synced());
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, true, omt));
  EXPECT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());
  EXPECT_TRUE(service.is_cross_tenant_reclaim_enabled());
  omt.set_synced();
  ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&tenant1));
  ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&tenant2));
  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.init(1001, 0, 0, 0, false));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, true, omt));
  EXPECT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());

  ASSERT_EQ(OB_SUCCESS, snapshot.init(1002, 1, 0, 0, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, true, omt));
  EXPECT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());
  ASSERT_EQ(OB_SUCCESS, snapshots.at(1).init(1002, 0, 0, 0, false));
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, false, omt));
  EXPECT_TRUE(service.is_cross_tenant_reclaim_enabled());

  // A concurrent config re-enable prevents completion using the old scan.
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, true, omt));
  EXPECT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  ASSERT_EQ(OB_SUCCESS, service.try_finish_draining_(snapshots, true, omt));
  EXPECT_EQ(LogReclaimState::DISABLED, service.get_reclaim_state());
  EXPECT_FALSE(service.is_cross_tenant_reclaim_enabled());
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(LogReclaimState::DISABLED, service.get_reclaim_state());
}

TEST_F(TestServerLogBlockMgr, incomplete_tenant_scan_keeps_draining_marker)
{
  const int64_t original_size = log_block_mgr_.log_pool_meta_.curr_total_size_;
  const bool original_marker = log_block_mgr_.is_cross_tenant_reclaim_enabled();
  // The suite reuses its directory; a failed scan must not leak these files to later cases.
  DEFER({
    for (int64_t block = 0; block < 6; ++block) {
      const std::string path = tenant_ls_map_[1] + "/" + std::to_string(block);
      bool exists = false;
      EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(path.c_str(), exists));
      if (exists) {
        EXPECT_EQ(OB_SUCCESS, log_block_mgr_.remove_block(path.c_str()));
      }
    }
    EXPECT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(original_marker));
    EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(original_size));
  });
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * ObServerLogBlockMgr::BLOCK_SIZE));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(1, tenant_ls_map_[1], 0, 6));
  for (const bool missing_log_service : {true, false}) {
    SCOPED_TRACE(missing_log_service);
    share::ObCgroupCtrl cgroup_ctrl;
    share::ObTenantBase caller(2002);
    omt::ObTenant sys_tenant(OB_SYS_TENANT_ID, 1, cgroup_ctrl);
    omt::ObTenant unavailable(2004, 1, cgroup_ctrl);
    omt::ObTenant healthy(2006, 1, cgroup_ctrl);
    omt::ObMultiTenant omt;
    PalfEnv sys_env;
    PalfEnv unavailable_env;
    PalfEnv healthy_env;
    ObLogService sys_log;
    ObLogService unavailable_log;
    ObLogService healthy_log;
    TenantLogBlockReclaimService &healthy_service = healthy_env.palf_env_impl_.tenant_reclaim_service_;
    sys_log.palf_env_ = &sys_env;
    unavailable_log.palf_env_ = &unavailable_env;
    healthy_log.palf_env_ = &healthy_env;
    // Every observer has a sys tenant, including observers with only hidden sys.
    sys_tenant.set_tenant_super_block(storage::ObTenantSuperBlock(OB_SYS_TENANT_ID, true /* is_hidden */));
    sys_tenant.set<ObLogService*>(&sys_log);
    unavailable.set<ObLogService*>(missing_log_service ? nullptr : &unavailable_log);
    healthy.set<ObLogService*>(&healthy_log);
    ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&sys_tenant));
    ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&unavailable));
    ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&healthy));
    omt.set_synced();
    omt::ObMultiTenant *saved_omt = GCTX.omt_;
    GCTX.omt_ = &omt;
    DEFER({
      GCTX.omt_ = saved_omt;
      sys_tenant.set<ObLogService*>(nullptr);
      unavailable.set<ObLogService*>(nullptr);
      healthy.set<ObLogService*>(nullptr);
      sys_log.palf_env_ = nullptr;
      unavailable_log.palf_env_ = nullptr;
      healthy_log.palf_env_ = nullptr;
    });
    share::ObTenantSwitchGuard caller_guard(&caller);
    {
      share::ObTenantSwitchGuard guard(&sys_tenant);
      ASSERT_TRUE(sys_tenant.is_hidden());
      ASSERT_EQ(OB_SUCCESS, sys_env.palf_env_impl_.tenant_reclaim_service_.init(false));
    }
    {
      share::ObTenantSwitchGuard guard(&healthy);
      ASSERT_EQ(OB_SUCCESS, healthy_service.init(true));
      healthy_service.update_reclaim_snapshot(3, 0);
    }
    ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("1"));
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(true));
    ObServerLogBlockReclaimService &service = log_block_mgr_.reclaim_service_;
    service.reset();
    ASSERT_EQ(OB_SUCCESS, service.init(&log_block_mgr_));
    ASSERT_EQ(LogReclaimState::ENABLED, service.get_reclaim_state());
    ASSERT_TRUE(service.need_reclaim());
    // Both a missing MTL service and a failed snapshot must let the next tenant receive work.
    ASSERT_EQ(OB_SUCCESS, service.plan_reclaim_(ObTimeUtility::current_monotonic_time()));
    EXPECT_EQ(3, healthy_service.pending_blocks_);
    EXPECT_EQ(caller.id(), MTL_ID());

    ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
    service.reset();
    ASSERT_EQ(OB_SUCCESS, service.init(&log_block_mgr_));
    healthy_service.sync_reclaim_mode(LogReclaimState::DRAINING, false, true);
    ASSERT_EQ(OB_SUCCESS, service.plan_reclaim_(ObTimeUtility::current_monotonic_time()));
    EXPECT_EQ(caller.id(), MTL_ID());
    EXPECT_EQ(LogReclaimState::DRAINING, service.get_reclaim_state());
    EXPECT_TRUE(log_block_mgr_.is_cross_tenant_reclaim_enabled());

    unavailable.set<ObLogService*>(&unavailable_log);
    {
      share::ObTenantSwitchGuard guard(&unavailable);
      ASSERT_EQ(OB_SUCCESS, unavailable_env.palf_env_impl_.tenant_reclaim_service_.init(false));
    }
    ASSERT_EQ(OB_SUCCESS, service.plan_reclaim_(ObTimeUtility::current_monotonic_time()));
    EXPECT_EQ(caller.id(), MTL_ID());
    EXPECT_EQ(LogReclaimState::DISABLED, service.get_reclaim_state());
    EXPECT_FALSE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
    log_block_mgr_.destroy();
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
    EXPECT_EQ(LogReclaimState::DISABLED, service.get_reclaim_state());
    EXPECT_FALSE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
  }
}

TEST_F(TestServerLogBlockMgr, publish_reclaim_quota_by_snapshot_tenant)
{
  ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  ATOMIC_STORE(&pool.cross_tenant_reclaim_enabled_, true);
  pool.block_cnt_in_use_ = 4;
  ATOMIC_STORE(&pool.total_block_cnt_, 4);
  pool.min_block_id_ = 1;
  pool.max_block_id_ = 1;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("1"));
  ObServerLogBlockReclaimService &service = pool.reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.init(&pool));
  share::ObCgroupCtrl cgroup_ctrl;
  share::ObTenantBase caller(2002);
  omt::ObTenant tenant(2004, 1, cgroup_ctrl);
  omt::ObMultiTenant omt;
  ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&tenant));
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  TenantReclaimSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot.init(2004, 3, 0, 0, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, snapshot.init(2006, 1, 0, 0, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  PalfEnv palf_env;
  TenantLogBlockReclaimService &tenant_service = palf_env.palf_env_impl_.tenant_reclaim_service_;
  {
    share::ObTenantSwitchGuard guard(&tenant);
    ASSERT_EQ(OB_SUCCESS, tenant_service.init(true));
    tenant_service.update_reclaim_snapshot(3, 0);
  }
  ObLogService log_service;
  log_service.palf_env_ = &palf_env;
  tenant.set<ObLogService*>(&log_service);
  omt::ObMultiTenant *saved_omt = GCTX.omt_;
  GCTX.omt_ = &omt;
  {
    share::ObTenantSwitchGuard guard(&caller);
    // A user tenant's GC thread can publish directly to a different user tenant.
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(3, tenant_service.pending_blocks_);
    EXPECT_EQ(caller.id(), MTL_ID());

    // Do not publish zero quota to a tenant that had no reclaim candidates in this scan.
    EXPECT_EQ(OB_SUCCESS, snapshots.at(0).init(2004, 0, 0, 0, true));
    EXPECT_EQ(OB_SUCCESS, snapshots.at(1).init(2006, 1, 0, 0, true));
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(3, tenant_service.pending_blocks_);

    // A missing tenant must not prevent delivery of the next snapshot's quota.
    EXPECT_EQ(OB_SUCCESS, snapshots.at(0).init(2006, 1, 0, 0, true));
    EXPECT_EQ(OB_SUCCESS, snapshots.at(1).init(2004, 3, 0, 0, true));
    tenant_service.pending_blocks_ = 0;
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(3, tenant_service.pending_blocks_);
    EXPECT_EQ(caller.id(), MTL_ID());

    // Keep the previous quota when the snapshot still reports it in progress.
    EXPECT_EQ(OB_SUCCESS, snapshots.at(1).init(2004, 3, 0, 1, true));
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(
        snapshots, 1 + ObServerLogBlockReclaimService::RUN_INTERVAL));
    EXPECT_EQ(3, tenant_service.pending_blocks_);

    // With no server demand, skip quota delivery and let tenant GC clear old quotas.
    EXPECT_EQ(OB_SUCCESS, snapshots.at(1).init(2004, 3, 0, 0, true));
    pool.block_cnt_in_use_ = 0;
    pool.max_block_id_ = 5;
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(3, tenant_service.pending_blocks_);
    pool.block_cnt_in_use_ = 4;
    pool.max_block_id_ = 1;
    EXPECT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(3, tenant_service.pending_blocks_);
    EXPECT_EQ(caller.id(), MTL_ID());

    // An active shrink plans against the target size even below the current watermark.
    ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
    ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("90"));
    pool.block_cnt_in_use_ = 80;
    ATOMIC_STORE(&pool.total_block_cnt_, 100);
    pool.min_block_id_ = 0;
    pool.max_block_id_ = 20;
    pool.log_pool_meta_ = ObServerLogBlockMgr::LogPoolMeta(
        100 * ObServerLogBlockMgr::BLOCK_SIZE,
        80 * ObServerLogBlockMgr::BLOCK_SIZE,
        ObServerLogBlockMgr::SHRINKING_STATUS);
    snapshots.reuse();
    ASSERT_EQ(OB_SUCCESS, snapshot.init(2004, 8, 0, 0, true));
    ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
    tenant_service.update_reclaim_snapshot(8, 0);
    tenant_service.pending_blocks_ = 0;
    EXPECT_TRUE(service.need_reclaim());
    EXPECT_EQ(OB_SUCCESS, service.plan_enabled_reclaim_(snapshots, 1));
    EXPECT_EQ(8, tenant_service.pending_blocks_);
    EXPECT_EQ(caller.id(), MTL_ID());
  }
  GCTX.omt_ = saved_omt;
  tenant.set<ObLogService*>(NULL);
  log_service.palf_env_ = NULL;
}

TEST_F(TestServerLogBlockMgr, reclaim_plan_skips_scan_without_demand)
{
  ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  ATOMIC_STORE(&pool.cross_tenant_reclaim_enabled_, true);
  ATOMIC_STORE(&pool.total_block_cnt_, 100);
  pool.min_block_id_ = 0;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  ObServerLogBlockReclaimService &service = pool.reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.init(&pool));
  omt::ObMultiTenant *saved_omt = GCTX.omt_;
  GCTX.omt_ = NULL;
  // No tenant service is needed below or at the watermark, but pressure still needs a scan.
  for (const int64_t used : {79, 80, 81}) {
    pool.block_cnt_in_use_ = used;
    pool.max_block_id_ = 100 - used;
    EXPECT_EQ(used > 80 ? OB_ERR_UNEXPECTED : OB_SUCCESS, service.plan_reclaim_(1));
  }
  // A disabled config also cancels planning before the next lifecycle transition.
  EXPECT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("0"));
  EXPECT_EQ(OB_SUCCESS, service.plan_reclaim_(1));
  // Draining must still scan tenant acknowledgements even without reclaim demand.
  ATOMIC_STORE64(&service.reclaim_state_, LogReclaimState::DRAINING);
  EXPECT_EQ(OB_ERR_UNEXPECTED, service.plan_reclaim_(1));
  GCTX.omt_ = saved_omt;
}

TEST_F(TestServerLogBlockMgr, unpublished_meta_is_ignored_on_restart)
{
  char tmp_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_path, sizeof(tmp_path),
                                       "%s/meta.tmp", log_block_mgr_.log_pool_path_));
  for (const bool meta_enabled : {false, true}) {
    ASSERT_EQ(OB_SUCCESS,
              log_block_mgr_.reclaim_service_.persist_cross_tenant_reclaim_enabled_(meta_enabled));
    ObServerLogBlockMgr::LogPoolMetaEntry unpublished(log_block_mgr_.log_pool_meta_, !meta_enabled);
    unpublished.update_checksum();
    char buf[ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE] = {'\0'};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, unpublished.serialize(buf, sizeof(buf), pos));
    // Before rename, neither a torn header nor a complete new entry is committed.
    for (const int64_t size : {int64_t(8), pos}) {
      const int fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
      ASSERT_GE(fd, 0);
      EXPECT_EQ(size, ::write(fd, buf, size));
      EXPECT_EQ(0, ::fsync(fd));
      EXPECT_EQ(0, ::close(fd));
      log_block_mgr_.destroy();
      ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
      EXPECT_EQ(meta_enabled, log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
      EXPECT_EQ(meta_enabled ? LogReclaimState::DRAINING : LogReclaimState::DISABLED,
                log_block_mgr_.reclaim_service_.get_reclaim_state());
      bool exists = true;
      ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(tmp_path, exists));
      EXPECT_FALSE(exists);
    }
  }
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.reclaim_service_.persist_cross_tenant_reclaim_enabled_(false));
}

TEST_F(TestServerLogBlockMgr, failed_meta_update_does_not_enable_reclaim)
{
  char meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char tmp_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(meta_path, sizeof(meta_path),
                                       "%s/meta", log_block_mgr_.log_pool_path_));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_path, sizeof(tmp_path),
                                       "%s/meta.tmp", log_block_mgr_.log_pool_path_));
  // A directory at the temp-file path makes replacement fail before touching meta.
  ASSERT_EQ(0, ::mkdir(tmp_path, 0700));
  const ObServerLogBlockMgr::LogPoolMeta old_meta = log_block_mgr_.log_pool_meta_;
  ObServerLogBlockMgr::LogPoolMeta new_meta = old_meta;
  new_meta.next_total_size_ += ObServerLogBlockMgr::BLOCK_SIZE;
  new_meta.status_ = ObServerLogBlockMgr::EXPANDING_STATUS;
  {
    ObSpinLockGuard guard(log_block_mgr_.resize_lock_);
    EXPECT_NE(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
        log_block_mgr_.log_pool_path_, meta_path, new_meta,
        /* cross_tenant_reclaim_enabled */ true));
  }
  EXPECT_EQ(old_meta, log_block_mgr_.log_pool_meta_);
  ObServerLogBlockReclaimService &service = log_block_mgr_.reclaim_service_;
  EXPECT_NE(OB_SUCCESS, service.advance_reclaim_state_guarded_by_lock_(80));
  EXPECT_EQ(LogReclaimState::DISABLED, service.get_reclaim_state());
  EXPECT_FALSE(service.is_cross_tenant_reclaim_enabled());
  ASSERT_EQ(0, ::rmdir(tmp_path));
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(old_meta, log_block_mgr_.log_pool_meta_);
  EXPECT_FALSE(service.is_cross_tenant_reclaim_enabled());
}

TEST_F(TestServerLogBlockMgr, hard_stop_in_every_reclaim_state)
{
  ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  ObServerLogBlockReclaimService &service = pool.reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.init(&pool));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("80"));
  for (const LogReclaimState state : {LogReclaimState::INITIALIZING, LogReclaimState::DISABLED,
                                    LogReclaimState::ENABLED, LogReclaimState::DRAINING}) {
    ATOMIC_STORE64(&service.reclaim_state_, state);
    ATOMIC_STORE(&service.pool_write_stop_flip_count_, 0);
    for (const int64_t used : {94, 95, 96, 94}) {
      pool.block_cnt_in_use_ = used;
      ATOMIC_STORE(&pool.total_block_cnt_, 100);
      pool.min_block_id_ = 0;
      pool.max_block_id_ = 100 - used;
      service.refresh_pool_write_stopped();
      EXPECT_EQ(LogReclaimState::DISABLED != state && used >= 95,
                service.is_pool_write_stopped());
      EXPECT_EQ(LogReclaimState::ENABLED == state, service.need_reclaim());
    }
    EXPECT_EQ(LogReclaimState::DISABLED == state ? 0 : 2,
              ATOMIC_LOAD(&service.pool_write_stop_flip_count_));
  }
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("100"));
  pool.block_cnt_in_use_ = 100;
  pool.max_block_id_ = 0;
  service.refresh_pool_write_stopped();
  EXPECT_TRUE(service.is_pool_write_stopped());
  pool.block_cnt_in_use_ = 99;
  pool.max_block_id_ = 1;
  service.refresh_pool_write_stopped();
  EXPECT_FALSE(service.is_pool_write_stopped());
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  EventItem enable_errsim_threshold;
  enable_errsim_threshold.error_code_ = -100;
  enable_errsim_threshold.trigger_freq_ = 1;
  EXPECT_EQ(OB_SUCCESS, EventTable::set_event(
      "ERRSIM_SERVER_LOG_POOL_WRITE_STOP_THRESHOLD", enable_errsim_threshold));
  pool.block_cnt_in_use_ = 96;
  pool.max_block_id_ = 4;
  service.refresh_pool_write_stopped();
  EXPECT_FALSE(service.is_pool_write_stopped());
  EventItem disable_errsim_threshold;
  EXPECT_EQ(OB_SUCCESS, EventTable::set_event(
      "ERRSIM_SERVER_LOG_POOL_WRITE_STOP_THRESHOLD", disable_errsim_threshold));
  service.refresh_pool_write_stopped();
  EXPECT_TRUE(service.is_pool_write_stopped());
  // Completing draining must bypass a stale stop flag before the next refresh.
  ATOMIC_STORE64(&service.reclaim_state_, LogReclaimState::DISABLED);
  EXPECT_FALSE(service.is_pool_write_stopped());
  service.refresh_pool_write_stopped();
  EXPECT_FALSE(ATOMIC_LOAD(&service.pool_write_stopped_));
  pool.block_cnt_in_use_ = 0;
  ATOMIC_STORE(&pool.total_block_cnt_, 0);
  pool.max_block_id_ = 0;
  service.refresh_pool_write_stopped();
  EXPECT_FALSE(service.is_pool_write_stopped());
  EXPECT_FALSE(service.need_reclaim());
}

TEST_F(TestServerLogBlockMgr, concurrent_reservations_cannot_exceed_pool_capacity)
{
  ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  pool.total_block_cnt_ = 100;
  pool.block_cnt_in_use_ = 94;
  pool.min_block_id_ = 0;
  pool.max_block_id_ = 6;
  ASSERT_EQ(OB_SUCCESS, pool.reclaim_service_.init(&pool));
  ATOMIC_STORE64(&pool.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  pool.reclaim_service_.refresh_pool_write_stopped();
  ASSERT_FALSE(pool.reclaim_service_.is_pool_write_stopped());

  // Model requests already admitted before the next stop-state refresh.
  std::atomic<int64_t> reserved(0);
  std::atomic<int64_t> exhausted(0);
  std::atomic<int64_t> ready(0);
  std::atomic<bool> start(false);
  std::thread workers[12];
  for (auto &worker : workers) {
    worker = std::thread([&pool, &reserved, &exhausted, &ready, &start]() {
      ready.fetch_add(1);
      while (!start.load()) {
        std::this_thread::yield();
      }
      block_id_t block_id = LOG_INVALID_BLOCK_ID;
      const int ret = pool.get_and_inc_min_block_id_guarded_by_lock_(block_id, true);
      if (OB_SUCCESS == ret) {
        reserved.fetch_add(1);
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        exhausted.fetch_add(1);
      } else {
        ADD_FAILURE() << "unexpected reservation result: " << ret;
      }
    });
  }
  while (ready.load() < ARRAYSIZEOF(workers)) {
    std::this_thread::yield();
  }
  start.store(true);
  for (auto &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(6, reserved.load());
  EXPECT_EQ(6, exhausted.load());
  EXPECT_EQ(100, pool.block_cnt_in_use_);
  EXPECT_EQ(pool.min_block_id_, pool.max_block_id_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, pool.create_block("clog_disk/exhausted", PALF_PHY_BLOCK_SIZE));
  EXPECT_EQ(100, pool.block_cnt_in_use_);
  pool.reclaim_service_.refresh_pool_write_stopped();
  EXPECT_TRUE(pool.reclaim_service_.is_pool_write_stopped());

  // Returning one block admits exactly one more reservation, not unbounded allocation.
  block_id_t block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_SUCCESS, pool.get_and_inc_max_block_id_guarded_by_lock_(block_id, true));
  EXPECT_EQ(99, pool.block_cnt_in_use_);
  EXPECT_EQ(OB_SUCCESS, pool.get_and_inc_min_block_id_guarded_by_lock_(block_id, true));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, pool.get_and_inc_min_block_id_guarded_by_lock_(block_id, true));
  EXPECT_EQ(100, pool.block_cnt_in_use_);
}

TEST_F(TestServerLogBlockMgr, write_stop_refresh_uses_dedicated_lock)
{
  ObServerLogBlockMgr pool;
  pool.is_inited_ = true;
  ATOMIC_STORE(&pool.total_block_cnt_, 100);
  pool.min_block_id_ = 0;
  ObServerLogBlockReclaimService &service = pool.reclaim_service_;
  ASSERT_EQ(OB_SUCCESS, service.init(&pool));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));

  auto refresh_after_usage_change = [&pool, &service](
      const int64_t old_used, const int64_t new_used, const bool expected_stopped) {
    pool.block_cnt_in_use_ = old_used;
    pool.max_block_id_ = 100 - old_used;
    service.refresh_pool_write_stopped();
    service.pool_write_stop_refresh_lock_.lock();
    std::atomic<bool> started(false);
    std::atomic<bool> finished(false);
    std::thread worker([&service, &started, &finished]() {
      started.store(true);
      service.refresh_pool_write_stopped();
      finished.store(true);
    });
    while (!started.load()) {
      std::this_thread::yield();
    }
    usleep(10 * 1000);
    EXPECT_FALSE(finished.load());
    pool.block_cnt_in_use_ = new_used;
    pool.max_block_id_ = 100 - new_used;
    service.pool_write_stop_refresh_lock_.unlock();
    worker.join();
    EXPECT_EQ(expected_stopped, service.is_pool_write_stopped());
  };

  for (const LogReclaimState state : {LogReclaimState::DISABLED,
                                    LogReclaimState::ENABLED, LogReclaimState::DRAINING}) {
    ATOMIC_STORE64(&service.reclaim_state_, state);
    refresh_after_usage_change(94, 96, LogReclaimState::DISABLED != state);
    refresh_after_usage_change(96, 94, false);
  }
}

TEST_F(TestServerLogBlockMgr, periodic_check_skips_busy_planner)
{
  ObServerLogBlockReclaimService &service = log_block_mgr_.reclaim_service_;
  service.reclaim_plan_lock_.lock();
  EXPECT_EQ(OB_SUCCESS, service.try_advance_reclaim(1));
  EXPECT_EQ(0, ATOMIC_LOAD(&service.last_periodic_check_ts_));
  service.reclaim_plan_lock_.unlock();
  std::thread workers[8];
  for (auto &worker : workers) {
    worker = std::thread([&service]() { EXPECT_EQ(OB_SUCCESS, service.try_advance_reclaim(1)); });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(1, ATOMIC_LOAD(&service.last_periodic_check_ts_));
  EXPECT_FALSE(service.need_periodic_check_(ObServerLogBlockReclaimService::RUN_INTERVAL));
  EXPECT_TRUE(service.need_periodic_check_(1 + ObServerLogBlockReclaimService::RUN_INTERVAL));
}

TEST_F(TestServerLogBlockMgr, allocate_reclaim_quota)
{
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  ObServerLogBlockReclaimService::ReclaimQuotaArray quotas;
  TenantReclaimSnapshot snapshot;
  EXPECT_FALSE(snapshot.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            snapshot.init(OB_INVALID_TENANT_ID, 0, 0, 0,
                          /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            snapshot.init(1001, -1, 0, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            snapshot.init(1001, 0, 0, -1, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1001, 2, 0, 0, /* shared_reclaim_enabled */ true));
  EXPECT_EQ(1001, snapshot.get_tenant_id());
  EXPECT_EQ(2, snapshot.get_retention_expired_blocks());
  EXPECT_EQ(0, snapshot.get_retention_unexpired_blocks());
  EXPECT_EQ(0, snapshot.get_last_reclaim_progress_ts());
  EXPECT_TRUE(snapshot.is_shared_reclaim_enabled());
  EXPECT_TRUE(snapshot.is_valid());
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1002, 2, 0, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));

  int64_t cursor = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                snapshots, 1, quotas, cursor));
  ASSERT_EQ(2, quotas.count());
  EXPECT_EQ(1, quotas.at(0));
  EXPECT_EQ(0, quotas.at(1));
  ASSERT_EQ(OB_SUCCESS,
            ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                snapshots, 1, quotas, cursor));
  EXPECT_EQ(0, quotas.at(0));
  EXPECT_EQ(1, quotas.at(1));

  snapshots.reuse();
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1001, 2, 0, 100, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1002, 2, 0, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  EXPECT_FALSE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(
      snapshots, 100 + ObServerLogBlockReclaimService::RUN_INTERVAL - 1));
  EXPECT_TRUE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(
      snapshots, 100 + ObServerLogBlockReclaimService::RUN_INTERVAL));
  ASSERT_EQ(OB_SUCCESS,
            ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                snapshots, 1, quotas, cursor));
  EXPECT_EQ(0, quotas.at(0));
  EXPECT_EQ(1, quotas.at(1));

  snapshots.reuse();
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1001, 1, 0, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1002, 0, 2, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                snapshots, 2, quotas, cursor));
  EXPECT_EQ(1, quotas.at(0));
  EXPECT_EQ(1, quotas.at(1));

  // Once a frozen tenant's history ages out, it is preferred over an active
  // tenant whose candidates are still inside the retention window.
  snapshots.reuse();
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1001, 3, 0, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1002, 0, 3, 0, /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                snapshots, 2, quotas, cursor));
  EXPECT_EQ(2, quotas.at(0));
  EXPECT_EQ(0, quotas.at(1));
}

TEST_F(TestServerLogBlockMgr, stage_quota_conserves_blocks)
{
  ObServerLogBlockReclaimService::ReclaimQuotaArray candidates;
  ObServerLogBlockReclaimService::ReclaimQuotaArray quotas;
  int64_t cursor = 0;
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_stage_quota_(
                            candidates, 1, quotas, cursor));
  EXPECT_EQ(0, quotas.count());
  // Exhaust small asymmetric pools, including zero demand and insufficient supply.
  for (int64_t a = 0; a <= 4; ++a) {
    for (int64_t b = 0; b <= 4; ++b) {
      candidates.reuse();
      ASSERT_EQ(OB_SUCCESS, candidates.push_back(a));
      ASSERT_EQ(OB_SUCCESS, candidates.push_back(b));
      ASSERT_EQ(OB_SUCCESS, candidates.push_back(0));
      for (int64_t target = 0; target <= a + b + 1; ++target) {
        for (int64_t start = 0; start < 3; ++start) {
          cursor = start;
          ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                    candidates, target, quotas, cursor));
          ASSERT_EQ(candidates.count(), quotas.count());
          int64_t assigned = 0;
          for (int64_t i = 0; i < quotas.count(); ++i) {
            EXPECT_GE(quotas.at(i), 0);
            EXPECT_LE(quotas.at(i), candidates.at(i));
            if (a + b > 0) {
              const int64_t floor = candidates.at(i) * common::min(target, a + b) / (a + b);
              EXPECT_GE(quotas.at(i), floor);
              EXPECT_LE(quotas.at(i), floor + 1);
            }
            assigned += quotas.at(i);
          }
          EXPECT_EQ(common::min(target, a + b), assigned);
        }
      }
    }
  }
  cursor = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                    candidates, -1, quotas, cursor));
  EXPECT_EQ(1, cursor);
  EXPECT_EQ(0, quotas.count());
  cursor = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                    candidates, 1, quotas, cursor));
  EXPECT_EQ(-1, cursor);
  EXPECT_EQ(0, quotas.count());
  cursor = 0;
  candidates.at(0) = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                    candidates, 1, quotas, cursor));
  EXPECT_EQ(0, cursor);
  // Preserve the first error even after accumulating positive candidates.
  candidates.at(0) = 4;
  candidates.at(1) = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                    candidates, 1, quotas, cursor));
  EXPECT_EQ(0, cursor);
  ASSERT_EQ(candidates.count(), quotas.count());
  for (int64_t i = 0; i < quotas.count(); ++i) {
    EXPECT_EQ(0, quotas.at(i));
  }
  candidates.reuse();
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(
                            ObServerLogBlockReclaimService::
                                MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND));
  cursor = 0;
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_stage_quota_(
                            candidates,
                            ObServerLogBlockReclaimService::
                                MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND,
                            quotas,
                            cursor));
  EXPECT_EQ(ObServerLogBlockReclaimService::MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND,
            quotas.at(0));
  candidates.at(0) =
      ObServerLogBlockReclaimService::MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND + 1;
  EXPECT_EQ(OB_SIZE_OVERFLOW, ObServerLogBlockReclaimService::allocate_stage_quota_(
                                  candidates, 1, quotas, cursor));
  EXPECT_EQ(0, cursor);
}

TEST(TestServerLogBlockReclaimService, stalled_tenant_does_not_block_ready_tenant)
{
  share::ObTenantBase tenant_a(1001);
  share::ObTenantBase tenant_b(1002);
  share::ObTenantBase tenant_c(1003);
  TenantLogBlockReclaimService service_a;
  TenantLogBlockReclaimService service_b;
  TenantLogBlockReclaimService service_c;
  TenantReclaimSnapshot snapshot_a;
  TenantReclaimSnapshot snapshot_b;
  TenantReclaimSnapshot snapshot_c;
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  ObServerLogBlockReclaimService::ReclaimQuotaArray quotas;
  int64_t replan_ts = 0;
  int64_t cursor = 0;

  {
    share::ObTenantSwitchGuard guard(&tenant_a);
    ASSERT_EQ(OB_SUCCESS, service_a.init(/* shared_reclaim_enabled */ true));
    service_a.update_reclaim_snapshot(2, 0);
    ASSERT_EQ(OB_SUCCESS, service_a.apply_reclaim_quota(2));
  }
  {
    share::ObTenantSwitchGuard guard(&tenant_b);
    ASSERT_EQ(OB_SUCCESS, service_b.init(/* shared_reclaim_enabled */ true));
    service_b.update_reclaim_snapshot(2, 0);
    ASSERT_EQ(OB_SUCCESS, service_b.apply_reclaim_quota(2));
    replan_ts = service_b.last_progress_ts_
        + TenantLogBlockReclaimService::SUMMARY_INTERVAL_US;
  }
  {
    share::ObTenantSwitchGuard guard(&tenant_c);
    ASSERT_EQ(OB_SUCCESS, service_c.init(/* shared_reclaim_enabled */ true));
    service_c.update_reclaim_snapshot(3, 0);
  }

  // A is still returning its old quota while B becomes suspended.
  {
    share::ObTenantSwitchGuard guard(&tenant_a);
    service_a.record_gc_progress();
    service_a.last_progress_ts_ = replan_ts;
    ASSERT_EQ(OB_SUCCESS, service_a.get_reclaim_snapshot(
                              replan_ts, /* server_need_reclaim */ true, snapshot_a));
  }
  {
    share::ObTenantSwitchGuard guard(&tenant_b);
    ASSERT_EQ(OB_SUCCESS, service_b.get_reclaim_snapshot(
                              replan_ts, /* server_need_reclaim */ true, snapshot_b));
    EXPECT_TRUE(service_b.reclaim_suspended_);
    EXPECT_EQ(2, service_b.pending_blocks_);
  }
  {
    share::ObTenantSwitchGuard guard(&tenant_c);
    ASSERT_EQ(OB_SUCCESS, service_c.get_reclaim_snapshot(
                              replan_ts, /* server_need_reclaim */ true, snapshot_c));
  }
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot_a));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot_b));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot_c));
  EXPECT_FALSE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(
      snapshots, replan_ts));

  // A completes normally. B keeps its old quota but no longer blocks new work,
  // so the next server demand is assigned to C.
  {
    share::ObTenantSwitchGuard guard(&tenant_a);
    service_a.record_gc_progress();
    ASSERT_EQ(OB_SUCCESS, service_a.get_reclaim_snapshot(
                              replan_ts + 1, /* server_need_reclaim */ true, snapshot_a));
  }
  snapshots.at(0) = snapshot_a;
  EXPECT_TRUE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(
      snapshots, replan_ts + 1));
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                            snapshots, 3, quotas, cursor));
  ASSERT_EQ(3, quotas.count());
  EXPECT_EQ(0, quotas.at(0));
  EXPECT_EQ(0, quotas.at(1));
  EXPECT_EQ(3, quotas.at(2));
  {
    share::ObTenantSwitchGuard guard(&tenant_c);
    ASSERT_EQ(OB_SUCCESS, service_c.apply_reclaim_quota(quotas.at(2)));
    EXPECT_EQ(3, service_c.pending_blocks_);
  }
  EXPECT_TRUE(service_b.reclaim_suspended_);
  EXPECT_EQ(2, service_b.pending_blocks_);
}

TEST(TestServerLogBlockReclaimService, reclaim_quota_caps_each_tenant_per_round)
{
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  ObServerLogBlockReclaimService::ReclaimQuotaArray quotas;
  TenantReclaimSnapshot snapshot;
  int64_t cursor = 0;
  const int64_t max_per_tenant =
      ObServerLogBlockReclaimService::MAX_RECLAIM_BLOCKS_PER_TENANT_PER_ROUND;
  const int64_t max_total_candidates =
      max_per_tenant * ObServerLogBlockReclaimService::MAX_RECLAIM_TENANT_COUNT;
  ASSERT_FALSE(IS_MULTI_OVERFLOW64(max_per_tenant, max_total_candidates));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1001, max_per_tenant - 100, 200, 0,
                          /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS,
            snapshot.init(1002, 0, max_per_tenant * 2, 0,
                          /* shared_reclaim_enabled */ true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                            snapshots, max_per_tenant * 4, quotas, cursor));
  EXPECT_EQ(max_per_tenant, quotas.at(0));
  EXPECT_EQ(max_per_tenant, quotas.at(1));
}

TEST_F(TestServerLogBlockMgr, reclaim_quota_skips_local_and_active_tenants)
{
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  ObServerLogBlockReclaimService::ReclaimQuotaArray quotas;
  TenantReclaimSnapshot snapshot;
  int64_t cursor = 0;
  ASSERT_EQ(OB_SUCCESS, snapshot.init(1001, 100, 100, 0, false));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, snapshot.init(1002, 100, 100, 100, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, snapshot.init(1003, 1, 2, 0, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                            snapshots, 10, quotas, cursor));
  EXPECT_EQ(0, quotas.at(0));
  EXPECT_EQ(0, quotas.at(1));
  EXPECT_EQ(3, quotas.at(2));
  EXPECT_FALSE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(snapshots, 0));
  EXPECT_FALSE(ObServerLogBlockReclaimService::can_start_next_reclaim_round_(snapshots, 99));
  ASSERT_EQ(OB_SUCCESS, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                            snapshots, 0, quotas, cursor));
  for (int64_t i = 0; i < quotas.count(); ++i) {
    EXPECT_EQ(0, quotas.at(i));
  }
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                                    snapshots, -1, quotas, cursor));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(TenantReclaimSnapshot()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObServerLogBlockReclaimService::allocate_reclaim_quota_(
                                    snapshots, 1, quotas, cursor));
}

TEST_F(TestServerLogBlockMgr, basic_func)
{
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  // resize
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.start(aligned_reserved_size));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(2 * aligned_reserved_size));
  EXPECT_EQ(2 * aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);

  const int64_t ls_id = 1;
  // Ten 64 MiB blocks use 31.25% of 2 GiB and 15.625% after expanding to 4 GiB.
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("20"));
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id].c_str(), 0, 10));
  EXPECT_EQ(LogReclaimState::DISABLED, log_block_mgr_.reclaim_service_.get_reclaim_state());
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.is_pool_write_stopped());
  ATOMIC_STORE64(&log_block_mgr_.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  log_block_mgr_.reclaim_service_.refresh_pool_write_stopped();
  EXPECT_TRUE(log_block_mgr_.reclaim_service_.is_pool_write_stopped());
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(2 * aligned_reserved_size));
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.is_pool_write_stopped());
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.is_pool_write_stopped());
  EXPECT_EQ(ObServerLogBlockMgr::SHRINKING_STATUS, log_block_mgr_.log_pool_meta_.status_);
  int64_t in_use_size_byte, total_size_byte;
  int64_t in_use_block_cnt = 0;
  int64_t total_block_cnt = 0;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(10*ObServerLogBlockMgr::BLOCK_SIZE, in_use_size_byte);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(in_use_block_cnt, total_block_cnt));
  EXPECT_EQ(10, in_use_block_cnt);
  EXPECT_EQ(64, total_block_cnt);
  // Completed block IO may lead the committed metadata during resize.
  const int64_t saved_total_size = log_block_mgr_.log_pool_meta_.curr_total_size_;
  log_block_mgr_.log_pool_meta_.curr_total_size_ = ObServerLogBlockMgr::BLOCK_SIZE;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(in_use_block_cnt, total_block_cnt));
  EXPECT_EQ(64, total_block_cnt);
  EXPECT_LE(in_use_block_cnt, total_block_cnt);
  log_block_mgr_.log_pool_meta_.curr_total_size_ = saved_total_size;
  // DISABLED ignores shared headroom and reaches the target with available blocks.
  ATOMIC_STORE64(&log_block_mgr_.reclaim_service_.reclaim_state_, LogReclaimState::DISABLED);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(in_use_block_cnt, total_block_cnt));
  EXPECT_EQ(32, total_block_cnt);
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id].c_str(), 0, 10));
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.is_pool_write_stopped());
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(0, in_use_size_byte);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(in_use_block_cnt, total_block_cnt));
  EXPECT_EQ(0, in_use_block_cnt);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  CLOG_LOG(INFO, "after basic_func", K(log_block_mgr_));
}

TEST_F(TestServerLogBlockMgr, restart_for_empty_log_disk)
{
  log_block_mgr_.destroy();
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  int64_t in_use_size_byte, total_size_byte;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.next_total_size_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_disk_usage(in_use_size_byte, total_size_byte));
  EXPECT_EQ(0, in_use_size_byte);
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.status_);
  CLOG_LOG(INFO, "after restart_for_empty_log_disk ", K(log_block_mgr_));
}

TEST_F(TestServerLogBlockMgr, partial_shrink_and_restart)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const int64_t target_size = 16 * block_size;
  const int64_t ls_id = 1;
  auto check_progress = [&](const int64_t total_blocks, const int64_t status) {
    int64_t used = 0;
    int64_t total = 0;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used, total));
    EXPECT_EQ(total_blocks, total);
    EXPECT_LE(used, total);
    EXPECT_EQ(total_blocks * block_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
    EXPECT_EQ(target_size, log_block_mgr_.log_pool_meta_.next_total_size_);
    EXPECT_EQ(status, log_block_mgr_.log_pool_meta_.status_);
  };
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(true));
  ATOMIC_STORE64(&log_block_mgr_.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 24));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_size));
  check_progress(27, ObServerLogBlockMgr::SHRINKING_STATUS);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_size));
    check_progress(27, ObServerLogBlockMgr::SHRINKING_STATUS);
  }

  // Simulate a failed unlink after shrink reserved an ID but left its file in the pool.
  block_id_t reserved_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_SUCCESS,
            log_block_mgr_.get_and_inc_min_block_id_guarded_by_lock_(reserved_block_id));

  // Startup must recover the reserved block, preserve progress and stop at the utilization limit.
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(LogReclaimState::DRAINING, log_block_mgr_.reclaim_service_.get_reclaim_state());
  log_block_mgr_.get_tenants_log_disk_size_func_ = [](int64_t &size) -> int {
    size = 0;
    return OB_SUCCESS;
  };
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.start(target_size));
  EXPECT_TRUE(log_block_mgr_.is_started_);
  check_progress(27, ObServerLogBlockMgr::SHRINKING_STATUS);
  EXPECT_EQ(reserved_block_id, log_block_mgr_.min_block_id_);

  // Changing the target to the current size cancels the unfinished shrink.
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(27 * block_size));
  EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_EQ(27 * block_size, log_block_mgr_.log_pool_meta_.next_total_size_);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_size));

  // Simulate a crash after unlink/fsync but before saving the new current size.
  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 4));
  int64_t freed_blocks = 0;
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.free_blocks_at_log_pool_(2, freed_blocks));
  EXPECT_EQ(2, freed_blocks);
  ASSERT_EQ(OB_SUCCESS, fsync_with_retry(log_block_mgr_.log_pool_path_));
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(LogReclaimState::DRAINING, log_block_mgr_.reclaim_service_.get_reclaim_state());
  check_progress(23, ObServerLogBlockMgr::SHRINKING_STATUS);
  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 4, 6));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_size));
  check_progress(16, ObServerLogBlockMgr::NORMAL_STATUS);

  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 10, 14));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
}

TEST_F(TestServerLogBlockMgr, partial_ls_return_recovers_inventory_once)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const std::string ls_path = std::string(tenant_string_) + "/2001";
  const std::string removed_path = ls_path + ".tmp";
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(true));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(ls_path.c_str()));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory((ls_path + "/log").c_str()));
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory((ls_path + "/meta").c_str()));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(2001, ls_path + "/log", 0, 4));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(2001, ls_path + "/meta", 0, 2));

  // Track physical files across all directories, including blocks already returned to the pool.
  using Inventory = std::set<std::pair<dev_t, ino_t>>;
  auto inventory = [&]() {
    Inventory blocks;
    std::function<void(const std::string &)> scan = [&](const std::string &path) {
      DIR *dir = ::opendir(path.c_str());
      ASSERT_NE(nullptr, dir);
      struct dirent *entry = nullptr;
      while (nullptr != (entry = ::readdir(dir))) {
        if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
          continue;
        }
        const std::string child = path + "/" + entry->d_name;
        struct stat st;
        if (0 != ::stat(child.c_str(), &st)) {
          ADD_FAILURE() << child;
        } else if (S_ISDIR(st.st_mode)) {
          scan(child);
        } else if (block_size == st.st_size) {
          EXPECT_TRUE(blocks.insert(std::make_pair(st.st_dev, st.st_ino)).second);
        }
      }
      EXPECT_EQ(0, ::closedir(dir));
    };
    scan(log_pool_base_path_);
    return blocks;
  };
  auto check_usage = [&](const int64_t expected_used) {
    int64_t used = -1;
    int64_t total = -1;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used, total));
    EXPECT_EQ(expected_used, used);
    EXPECT_EQ(32, total);
    EXPECT_EQ((32 - expected_used) * block_size, log_block_mgr_.get_free_size_guarded_by_lock_());
  };
  const Inventory original = inventory();
  ASSERT_EQ(32, original.size());
  check_usage(6);
  ASSERT_EQ(OB_SUCCESS, rename_with_retry(ls_path.c_str(), removed_path.c_str()));
  ASSERT_EQ(OB_SUCCESS, fsync_with_retry(tenant_string_));

  class PartialReturnPool : public ILogBlockPool
  {
  public:
    explicit PartialReturnPool(ObServerLogBlockMgr &pool) : pool_(pool), returned_(0) {}
    int create_block(const char *path, const int64_t size) override
    { return pool_.create_block(path, size); }
    int remove_block(const char *path) override
    {
      int ret = 2 == returned_ ? OB_IO_ERROR : pool_.remove_block(path);
      if (OB_SUCCESS == ret) {
        ++returned_;
      }
      return ret;
    }
    ObServerLogBlockMgr &pool_;
    int64_t returned_;
  } partial_pool(log_block_mgr_);
  ASSERT_EQ(OB_IO_ERROR, palf::remove_directory_rec(removed_path.c_str(), &partial_pool));
  EXPECT_EQ(2, partial_pool.returned_);
  check_usage(4);
  EXPECT_EQ(original, inventory());

  // Reload the interrupted disk state. A second reload must not return any block twice.
  for (int64_t restart = 0; restart < 2; ++restart) {
    log_block_mgr_.destroy();
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
    check_usage(0);
    EXPECT_EQ(original, inventory());
    EXPECT_EQ(LogReclaimState::DRAINING, log_block_mgr_.reclaim_service_.get_reclaim_state());
    EXPECT_TRUE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
    bool exists = true;
    ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(removed_path.c_str(), exists));
    EXPECT_FALSE(exists);
  }
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
}

TEST_F(TestServerLogBlockMgr, shrink_reclaim_converges_without_writes)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const int64_t ls_id = 1;
  const int64_t target_block_cnt = 16;
  int64_t reclaim_block_cnt = 0;
  int64_t used_block_cnt = 0;
  int64_t total_block_cnt = 0;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("90"));
  ATOMIC_STORE64(&log_block_mgr_.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(20 * block_size));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(
                            ls_id, tenant_ls_map_[ls_id], 0, target_block_cnt));

  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_block_cnt * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used_block_cnt, total_block_cnt));
  EXPECT_EQ(target_block_cnt, used_block_cnt);
  EXPECT_EQ(18, total_block_cnt);
  EXPECT_EQ(ObServerLogBlockMgr::SHRINKING_STATUS, log_block_mgr_.log_pool_meta_.status_);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_reclaim_block_count(90, reclaim_block_cnt));
  EXPECT_EQ(2, reclaim_block_cnt);
  EXPECT_TRUE(log_block_mgr_.reclaim_service_.need_reclaim());

  // Tenant GC returns the shrink demand; no new write is needed to resume resize.
  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, reclaim_block_cnt));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(target_block_cnt * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used_block_cnt, total_block_cnt));
  EXPECT_EQ(14, used_block_cnt);
  EXPECT_EQ(target_block_cnt, total_block_cnt);
  EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_FALSE(log_block_mgr_.reclaim_service_.need_reclaim());

  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(
                            ls_id, tenant_ls_map_[ls_id], reclaim_block_cnt,
                            target_block_cnt - reclaim_block_cnt));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
}

TEST_F(TestServerLogBlockMgr, physical_return_records_shared_progress_once)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const int64_t ls_id = 1;
  share::ObCgroupCtrl cgroup_ctrl;
  omt::ObTenant tenant(2004, 1, cgroup_ctrl);
  PalfEnv palf_env;
  ObLogService log_service;
  TenantLogBlockReclaimService &service = palf_env.palf_env_impl_.tenant_reclaim_service_;
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char conflict_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 3));
  log_service.palf_env_ = &palf_env;
  tenant.set<ObLogService*>(&log_service);
  // Detach stack-owned services even when a fatal assertion returns early.
  struct ServiceGuard {
    ServiceGuard(omt::ObTenant &tenant, ObLogService &log_service)
        : tenant_(tenant), log_service_(log_service) {}
    ~ServiceGuard()
    {
      tenant_.set<ObLogService*>(NULL);
      log_service_.palf_env_ = NULL;
    }
    omt::ObTenant &tenant_;
    ObLogService &log_service_;
  } service_guard(tenant, log_service);
  share::ObTenantSwitchGuard tenant_guard(&tenant);
  ASSERT_EQ(&log_service, MTL(ObLogService*));
  ASSERT_EQ(OB_SUCCESS, service.init(true));
  service.update_reclaim_snapshot(2, 1);
  ASSERT_EQ(OB_SUCCESS, service.apply_reclaim_quota(3));
  service.reclaim_suspended_ = true;
  service.last_progress_ts_ = 1;
  service.gc_stall_start_ts_ = 1;
  auto check_inventory = [&](const int64_t used, const int64_t pending,
                             const int64_t expired, const int64_t unexpired) {
    int64_t actual_used = -1;
    int64_t actual_total = -1;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(actual_used, actual_total));
    EXPECT_EQ(used, actual_used);
    EXPECT_EQ(32, actual_total);
    EXPECT_EQ((32 - used) * block_size, log_block_mgr_.get_free_size_guarded_by_lock_());
    EXPECT_EQ(pending, service.pending_blocks_);
    EXPECT_EQ(expired, service.retention_expired_blocks_);
    EXPECT_EQ(unexpired, service.retention_unexpired_blocks_);
  };
  check_inventory(3, 3, 2, 1);
  ASSERT_EQ(OB_SUCCESS, databuff_printf(block_path, sizeof(block_path),
                                       "%s/0", tenant_ls_map_[ls_id].c_str()));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.remove_block(block_path));
  check_inventory(2, 2, 1, 1);
  EXPECT_TRUE(service.reclaim_suspended_);
  EXPECT_EQ(1, service.gc_stall_start_ts_);
  const int64_t progress_after_return = service.last_progress_ts_;
  EXPECT_GT(progress_after_return, 1);

  // The lower layer reports missing; LogEngine normalizes this to success.
  // Neither path may consume another quota or renew the stall/progress timer.
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, log_block_mgr_.remove_block(block_path));
  check_inventory(2, 2, 1, 1);
  EXPECT_EQ(progress_after_return, service.last_progress_ts_);
  EXPECT_EQ(1, service.gc_stall_start_ts_);
  EXPECT_TRUE(service.reclaim_suspended_);

  // Rename retries until the conflicting directory is removed. While the return
  // is blocked after pre-accounting, it must not consume any shared quota.
  ASSERT_EQ(OB_SUCCESS, databuff_printf(block_path, sizeof(block_path),
                                       "%s/1", tenant_ls_map_[ls_id].c_str()));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(conflict_path, sizeof(conflict_path),
                                       "%s/%lu", log_block_mgr_.log_pool_path_,
                                       log_block_mgr_.max_block_id_));
  ASSERT_EQ(0, ::mkdir(conflict_path, 0755));
  int return_ret = OB_SUCCESS;
  std::atomic<bool> return_finished(false);
  std::thread return_thread([&]() {
    share::ObTenantSwitchGuard worker_guard(&tenant);
    return_ret = log_block_mgr_.remove_block(block_path);
    return_finished.store(true);
  });
  int64_t observed_used = -1;
  int64_t observed_total = -1;
  const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
  do {
    EXPECT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(observed_used, observed_total));
    if (1 != observed_used) {
      ob_usleep(1000);
    }
  } while (1 != observed_used && ObTimeUtility::current_time() < deadline);
  EXPECT_FALSE(return_finished.load());
  check_inventory(1, 2, 1, 1);
  EXPECT_EQ(progress_after_return, service.last_progress_ts_);
  EXPECT_EQ(1, service.gc_stall_start_ts_);
  EXPECT_TRUE(service.reclaim_suspended_);
  bool source_exists = false;
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists(block_path, source_exists));
  EXPECT_TRUE(source_exists);
  EXPECT_EQ(0, ::rmdir(conflict_path));
  return_thread.join();
  ASSERT_EQ(OB_SUCCESS, return_ret);
  check_inventory(1, 1, 0, 1);
  EXPECT_TRUE(service.reclaim_suspended_);
  const int64_t progress_after_retry = service.last_progress_ts_;
  EXPECT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, log_block_mgr_.remove_block(block_path));
  check_inventory(1, 1, 0, 1);
  EXPECT_EQ(progress_after_retry, service.last_progress_ts_);
  EXPECT_EQ(1, service.gc_stall_start_ts_);
  EXPECT_TRUE(service.reclaim_suspended_);

  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 2, 1));
  check_inventory(0, 0, 0, 0);
  EXPECT_EQ(0, service.last_progress_ts_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, service.gc_stall_start_ts_);
  EXPECT_FALSE(service.reclaim_suspended_);
}

TEST_F(TestServerLogBlockMgr, expansion_cancels_shrink_reclaim)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const int64_t original_block_cnt = 20;
  const int64_t shrink_target_block_cnt = 16;
  const int64_t ls_id = 1;
  int64_t used_block_cnt = 0;
  int64_t total_block_cnt = 0;
  share::ObCgroupCtrl cgroup_ctrl;
  share::ObTenantBase caller(2002);
  omt::ObTenant tenant(2004, 1, cgroup_ctrl);
  omt::ObMultiTenant omt;
  PalfEnv palf_env;
  ObLogService log_service;
  TenantLogBlockReclaimService &tenant_service =
      palf_env.palf_env_impl_.tenant_reclaim_service_;
  ObServerLogBlockReclaimService &reclaim_service = log_block_mgr_.reclaim_service_;
  ObServerLogBlockReclaimService::ReclaimSnapshotArray snapshots;
  TenantReclaimSnapshot snapshot;
  omt::ObMultiTenant *saved_omt = GCTX.omt_;

  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_TRUE(GCONF.server_log_disk_utilization_threshold.set_value("90"));
  ATOMIC_STORE64(&reclaim_service.reclaim_state_, LogReclaimState::ENABLED);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(original_block_cnt * block_size));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(
                            ls_id, tenant_ls_map_[ls_id], 0, shrink_target_block_cnt));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(shrink_target_block_cnt * block_size));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used_block_cnt, total_block_cnt));
  EXPECT_EQ(shrink_target_block_cnt, used_block_cnt);
  EXPECT_EQ(18, total_block_cnt);
  EXPECT_TRUE(reclaim_service.need_reclaim());

  ASSERT_EQ(OB_SUCCESS, snapshot.init(tenant.id(), 2, 0, 0, true));
  ASSERT_EQ(OB_SUCCESS, snapshots.push_back(snapshot));
  ASSERT_EQ(OB_SUCCESS, omt.tenants_.push_back(&tenant));
  {
    share::ObTenantSwitchGuard guard(&tenant);
    ASSERT_EQ(OB_SUCCESS, tenant_service.init(/* shared_reclaim_enabled */ true));
    tenant_service.update_reclaim_snapshot(2, 0);
  }
  log_service.palf_env_ = &palf_env;
  tenant.set<ObLogService*>(&log_service);
  GCTX.omt_ = &omt;
  {
    share::ObTenantSwitchGuard guard(&caller);
    ASSERT_EQ(OB_SUCCESS, reclaim_service.plan_enabled_reclaim_(snapshots, 1));
  }
  EXPECT_EQ(2, tenant_service.pending_blocks_);

  // One block is reclaimed while the shrink still needs another block.
  {
    share::ObTenantSwitchGuard guard(&tenant);
    ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 1));
  }
  EXPECT_EQ(1, tenant_service.pending_blocks_);
  EXPECT_TRUE(reclaim_service.need_reclaim());

  // Expanding to the original size removes the shrink demand and retires its quota.
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(original_block_cnt * block_size));
  EXPECT_FALSE(reclaim_service.need_reclaim());
  tenant_service.sync_reclaim_mode(
      LogReclaimState::ENABLED, /* server_need_reclaim */ false,
      /* local_reclaim_done */ false);
  EXPECT_EQ(0, tenant_service.pending_blocks_);
  EXPECT_FALSE(tenant_service.can_recycle());
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used_block_cnt, total_block_cnt));
  EXPECT_EQ(shrink_target_block_cnt - 1, used_block_cnt);
  EXPECT_EQ(original_block_cnt, total_block_cnt);

  GCTX.omt_ = saved_omt;
  tenant.set<ObLogService*>(NULL);
  log_service.palf_env_ = NULL;
  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(
                            ls_id, tenant_ls_map_[ls_id], 1,
                            shrink_target_block_cnt - 1));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
}

TEST_F(TestServerLogBlockMgr, shrink_stops_at_utilization_limit)
{
  ObServerLogBlockMgr pool;
  const char *dir_path = "clog_disk/shrink_watermark";
  char block_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ObIOFd fd;
  int64_t freed_blocks = 0;
  ASSERT_TRUE(GCONF.server_log_disk_utilization_limit_threshold.set_value("95"));
  ASSERT_EQ(0, ::mkdir(dir_path, 0700));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(pool.log_pool_path_, sizeof(pool.log_pool_path_), "%s", dir_path));
  pool.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, pool.reclaim_service_.init(&pool));
  ATOMIC_STORE64(&pool.reclaim_service_.reclaim_state_, LogReclaimState::ENABLED);
  // Empty files exercise real unlinks without allocating the modeled capacity.
  for (block_id_t block_id = 0; block_id < 3; ++block_id) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(block_path, sizeof(block_path), "%s/%lu", dir_path, block_id));
    ASSERT_EQ(OB_SUCCESS, LOG_IO_ADAPTER.open(block_path,
        ObServerLogBlockMgr::CREATE_FILE_FLAG, ObServerLogBlockMgr::CREATE_FILE_MODE, fd));
    ASSERT_EQ(OB_SUCCESS, LOG_IO_ADAPTER.close(fd));
  }

  // Normal shrink keeps five percentage points below the hard stop.
  pool.block_cnt_in_use_ = 17;
  pool.max_block_id_ = 3;
  ATOMIC_STORE(&pool.total_block_cnt_, 20);
  ASSERT_EQ(OB_SUCCESS, pool.free_blocks_at_log_pool_(3, freed_blocks));
  ASSERT_EQ(1, freed_blocks);
  EXPECT_EQ(1, pool.min_block_id_);
  EXPECT_EQ(19, ATOMIC_LOAD(&pool.total_block_cnt_));
  EXPECT_LE(17 * 100, 19 * 90);
  EXPECT_GT(17 * 100, 18 * 90);

  // ERRSIM restores the legacy policy that shrinks whenever current usage is below 95%.
  EventItem enable_ignore_headroom;
  enable_ignore_headroom.error_code_ = 1;
  enable_ignore_headroom.trigger_freq_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event(
      "ERRSIM_IGNORE_LOG_POOL_SHRINK_HEADROOM", enable_ignore_headroom));
  pool.block_cnt_in_use_ = 336;
  pool.max_block_id_ = 19;
  ATOMIC_STORE(&pool.total_block_cnt_, 355);
  const int shrink_ret = pool.free_blocks_at_log_pool_(3, freed_blocks);
  EventItem disable_ignore_headroom;
  EXPECT_EQ(OB_SUCCESS, EventTable::set_event(
      "ERRSIM_IGNORE_LOG_POOL_SHRINK_HEADROOM", disable_ignore_headroom));
  ASSERT_EQ(OB_SUCCESS, shrink_ret);
  ASSERT_EQ(2, freed_blocks);
  EXPECT_EQ(3, pool.min_block_id_);
  EXPECT_EQ(353, ATOMIC_LOAD(&pool.total_block_cnt_));
  EXPECT_LT(336 * 100, 354 * 95);
  EXPECT_GE(336 * 100, 353 * 95);
  ASSERT_EQ(OB_SUCCESS, pool.free_blocks_at_log_pool_(1, freed_blocks));
  EXPECT_EQ(0, freed_blocks);
  EXPECT_EQ(3, pool.min_block_id_);
  EXPECT_EQ(0, ::rmdir(dir_path));
}

TEST_F(TestServerLogBlockMgr, shrink_progress_survives_failed_meta_commit)
{
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const int64_t ls_id = 1;
  char meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char tmp_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(meta_path, sizeof(meta_path),
                                       "%s/meta", log_block_mgr_.log_pool_path_));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_path, sizeof(tmp_path),
                                       "%s.tmp", meta_path));
  auto check_blocks = [&](const int64_t expected_used, const int64_t expected_total) {
    int64_t used = 0;
    int64_t total = 0;
    ObServerLogBlockMgr::GetBlockIdListFunctor pool_blocks;
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.get_block_usage(used, total));
    EXPECT_EQ(expected_used, used);
    EXPECT_EQ(expected_total, total);
    EXPECT_EQ(expected_total * block_size, log_block_mgr_.get_total_size_());
    ASSERT_EQ(OB_SUCCESS, palf::scan_dir(log_block_mgr_.log_pool_path_, pool_blocks));
    EXPECT_EQ(expected_total - expected_used, pool_blocks.block_id_array_.count());
  };
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  ASSERT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 8));
  check_blocks(8, 32);

  // Follow the resize phases so only the final metadata commit fails.
  const ObServerLogBlockMgr::LogPoolMeta shrinking_meta(
      32 * block_size, 24 * block_size, ObServerLogBlockMgr::SHRINKING_STATUS);
  ObServerLogBlockMgr::LogPoolMeta progress_meta = shrinking_meta;
  {
    ObSpinLockGuard guard(log_block_mgr_.resize_lock_);
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
        log_block_mgr_.log_pool_path_, meta_path, shrinking_meta,
        log_block_mgr_.is_cross_tenant_reclaim_enabled()));
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.do_shrink_(progress_meta, 8));
    ASSERT_EQ(24 * block_size, progress_meta.curr_total_size_);
    check_blocks(8, 24);
    EXPECT_EQ(shrinking_meta, log_block_mgr_.log_pool_meta_);

    // A directory at meta.tmp rejects replacement after unlink and directory fsync.
    ASSERT_EQ(0, ::mkdir(tmp_path, 0700));
    progress_meta.status_ = ObServerLogBlockMgr::NORMAL_STATUS;
    const int ret = log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
        log_block_mgr_.log_pool_path_, meta_path, progress_meta,
        log_block_mgr_.is_cross_tenant_reclaim_enabled());
    EXPECT_EQ(0, ::rmdir(tmp_path));
    ASSERT_NE(OB_SUCCESS, ret);
    EXPECT_EQ(shrinking_meta, log_block_mgr_.log_pool_meta_);

    ObServerLogBlockMgr::LogPoolMetaEntry persisted;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, read_until_success(meta_path,
        log_block_mgr_.log_pool_meta_serialize_buf_,
        ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE, 0));
    ASSERT_EQ(OB_SUCCESS, persisted.deserialize(log_block_mgr_.log_pool_meta_serialize_buf_,
        ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE, pos));
    EXPECT_TRUE(persisted.check_integrity());
    EXPECT_EQ(shrinking_meta, persisted.log_pool_meta_);
  }

  // Retry in the same process: the eight completed deletions must not repeat.
  const block_id_t min_after_shrink = log_block_mgr_.min_block_id_;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(24 * block_size));
    EXPECT_EQ(min_after_shrink, log_block_mgr_.min_block_id_);
    EXPECT_EQ(progress_meta, log_block_mgr_.log_pool_meta_);
    check_blocks(8, 24);
  }
  log_block_mgr_.destroy();
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  check_blocks(8, 24);
  EXPECT_EQ(progress_meta, log_block_mgr_.log_pool_meta_);
  ASSERT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 8));
  check_blocks(0, 24);
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
  check_blocks(0, 32);
}

TEST_F(TestServerLogBlockMgr, meta_commit_fault_phases_preserve_publication_order)
{
  class FaultDevice : public share::ObLocalDevice
  {
  public:
    enum Phase { WRITE, RENAME, DIRECTORY_FSYNC };
    explicit FaultDevice(const Phase phase) : phase_(phase), fail_(true), failures_(0) {}
    bool inject(const Phase phase)
    {
      const bool fail = phase_ == phase && fail_.load();
      if (fail) {
        ++failures_;
      }
      return fail;
    }
    int pwrite(const ObIOFd &fd, const int64_t offset, const int64_t size,
               const void *buf, int64_t &written) override
    {
      return inject(WRITE) ? OB_IO_ERROR : ObLocalDevice::pwrite(fd, offset, size, buf, written);
    }
    int rename(const char *from, const char *to) override
    { return inject(RENAME) ? OB_IO_ERROR : ObLocalDevice::rename(from, to); }
    int fsync(const ObIOFd &fd) override
    {
      ObIODFileStat st;
      const int ret = ObLocalDevice::fstat(fd, st);
      return OB_SUCCESS != ret ? ret
          : (S_ISDIR(st.mode_) && inject(DIRECTORY_FSYNC) ? OB_IO_ERROR : ObLocalDevice::fsync(fd));
    }
    const Phase phase_;
    std::atomic<bool> fail_;
    std::atomic<int64_t> failures_;
  };
  const int64_t block_size = ObServerLogBlockMgr::BLOCK_SIZE;
  const std::string meta_path = std::string(log_block_mgr_.log_pool_path_) + "/meta";
  for (const auto phase : {FaultDevice::WRITE, FaultDevice::RENAME, FaultDevice::DIRECTORY_FSYNC}) {
    SCOPED_TRACE(phase);
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
    const auto old_meta = log_block_mgr_.log_pool_meta_;
    auto new_meta = old_meta;
    new_meta.next_total_size_ = 24 * block_size;
    new_meta.status_ = ObServerLogBlockMgr::SHRINKING_STATUS;
    FaultDevice device(phase);
    ObIODOpts device_options;
    ASSERT_EQ(OB_SUCCESS, device.init(device_options));
    ObIODevice *saved_device = LOG_IO_ADAPTER.using_device_;
    LOG_IO_ADAPTER.using_device_ = &device;
    DEFER({ LOG_IO_ADAPTER.using_device_ = saved_device; });
    int commit_ret = OB_SUCCESS;
    std::atomic<bool> finished(false);
    std::thread writer([&]() {
      ObSpinLockGuard guard(log_block_mgr_.resize_lock_);
      commit_ret = log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
          log_block_mgr_.log_pool_path_, meta_path.c_str(), new_meta, true);
      finished.store(true);
    });
    // Always release retrying I/O and join before stack-owned state is destroyed.
    DEFER({ device.fail_.store(false); if (writer.joinable()) { writer.join(); } });
    const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
    while (device.failures_.load() < 2 && !finished.load()
           && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    ASSERT_GT(device.failures_.load(), 0);
    EXPECT_EQ(old_meta, log_block_mgr_.get_log_pool_meta_guarded_by_lock_());
    EXPECT_FALSE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
    ObServerLogBlockMgr::LogPoolMetaEntry persisted;
    char buf[ObServerLogBlockMgr::LOG_POOL_META_SERIALIZE_SIZE];
    const int fd = ::open(meta_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    const int64_t bytes = ::read(fd, buf, sizeof(buf));
    EXPECT_EQ(0, ::close(fd));
    ASSERT_EQ(sizeof(buf), bytes);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, persisted.deserialize(buf, sizeof(buf), pos));
    EXPECT_TRUE(persisted.check_integrity());
    const bool renamed = FaultDevice::DIRECTORY_FSYNC == phase;
    EXPECT_EQ(renamed ? new_meta : old_meta, persisted.log_pool_meta_);
    EXPECT_EQ(renamed, persisted.cross_tenant_reclaim_enabled_);
    if (FaultDevice::WRITE == phase) {
      writer.join();
      EXPECT_EQ(OB_IO_ERROR, commit_ret);
    } else {
      EXPECT_GE(device.failures_.load(), 2);
      EXPECT_FALSE(finished.load());
    }
    device.fail_.store(false);
    if (writer.joinable()) {
      writer.join();
      ASSERT_EQ(OB_SUCCESS, commit_ret);
    } else {
      ObSpinLockGuard guard(log_block_mgr_.resize_lock_);
      ASSERT_EQ(OB_SUCCESS, log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
          log_block_mgr_.log_pool_path_, meta_path.c_str(), new_meta, true));
    }
    EXPECT_EQ(new_meta, log_block_mgr_.log_pool_meta_);
    EXPECT_TRUE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
    LOG_IO_ADAPTER.using_device_ = saved_device;
    log_block_mgr_.destroy();
    ASSERT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
    EXPECT_TRUE(log_block_mgr_.is_cross_tenant_reclaim_enabled());
    EXPECT_EQ(LogReclaimState::DRAINING, log_block_mgr_.reclaim_service_.get_reclaim_state());
    EXPECT_EQ(24 * block_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
    EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  }
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.persist_cross_tenant_reclaim_enabled(false));
  ASSERT_EQ(OB_SUCCESS, log_block_mgr_.resize_(32 * block_size));
}

TEST_F(TestServerLogBlockMgr, allocate_blocks_in_tenant)
{
  const int64_t ls_id = 1001;
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 10));
  EXPECT_EQ(OB_SUCCESS,
            create_new_blocks_at(ls_id + 1, tenant_ls_map_[ls_id + 1], 0, 5));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, 3));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id + 1, tenant_ls_map_[ls_id + 1], 0, 3));
  CLOG_LOG(INFO, "after allocate_blocks_in_tenant", K(log_block_mgr_));
}

TEST_F(TestServerLogBlockMgr, restart_for_non_empty_log_disk)
{
  CLOG_LOG(INFO, "restart_for_non_empty_log_disk", K(log_block_mgr_));
  log_block_mgr_.destroy();
  const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.next_total_size_);
  EXPECT_EQ(0, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_EQ(9 * ObServerLogBlockMgr::BLOCK_SIZE, log_block_mgr_.get_in_use_size_guarded_by_lock_());
  const int64_t ls_id = 1001;
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 3, 7));
  EXPECT_EQ(OB_SUCCESS, delete_blocks_at(ls_id + 1, tenant_ls_map_[ls_id + 1], 3, 2));
  EXPECT_EQ(0, log_block_mgr_.get_in_use_size_guarded_by_lock_());
}

TEST_F(TestServerLogBlockMgr, concurrent_create_delete_resize)
{
  const int64_t reserved_size = 4 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size));
  const int64_t free_block_size = log_block_mgr_.get_free_size_guarded_by_lock_();
  std::vector<std::thread> threads;
  std::atomic<int64_t> total_allocate_block_count(0);
  auto create_func = [&](int64_t ls_id) -> int {
    int64_t block_cnt = 6;
    total_allocate_block_count += block_cnt;
    return create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, block_cnt);
  };
  auto delete_func = [&](int64_t ls_id) -> int {
    int64_t block_cnt = 6;
    create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, block_cnt);
    total_allocate_block_count += 2;
    return delete_blocks_at(ls_id, tenant_ls_map_[ls_id], 0, block_cnt-2);
  };
  std::vector< std::function<int(int64_t) > > funcs;
  funcs.push_back(create_func);
  funcs.push_back(delete_func);

  std::thread resize_thread([&]() -> int
  {
    const int64_t reserved_size = 10 * ObServerLogBlockMgr::GB;
    const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
    return log_block_mgr_.resize_(reserved_size);
  });

  for (int i = 0; i < DEFAULT_LS_COUNT; i++) {
    threads.emplace_back(std::thread(funcs[i%2], ls_id_array_[i]));
  }

  for (int i = 0; i < DEFAULT_LS_COUNT; i++) {
    threads[i].join();
  }
  resize_thread.join();

  const int64_t curr_free_zie = log_block_mgr_.get_free_size_guarded_by_lock_();
  int64_t total_allocate_block_count_int = total_allocate_block_count;
  const int64_t total_block_size = log_block_mgr_.log_pool_meta_.curr_total_size_;
  EXPECT_EQ(total_allocate_block_count_int*ObServerLogBlockMgr::BLOCK_SIZE + curr_free_zie, total_block_size);
  int64_t ls_id = ls_id_array_[0];
  EXPECT_EQ(OB_SUCCESS, create_new_blocks_at(ls_id, tenant_ls_map_[ls_id], 6, 3));
}

TEST_F(TestServerLogBlockMgr, dirty_ls_dir_and_log_pool_file)
{
  system("touch clog_disk/clog/log_pool/0.tmp");
  system("mkdir clog_disk/clog/tenant_1");
  system("mkdir clog_disk/clog/tenant_1/1/");
  system("mkdir clog_disk/clog/tenant_1/1/meta/");
  system("touch clog_disk/clog/tenant_1/1/meta/10000.tmp");
  system("fallocate -l 67108863 clog_disk/clog/tenant_1/1/meta/10000.tmp ");
  system("mkdir clog_disk/clog/log_pool/1.tmp");
  log_block_mgr_.destroy();
  bool result = false;
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists("clog_disk/clog/tenant_1/1/meta/10000.tmp",result));
  EXPECT_EQ(false, result);
}

TEST_F(TestServerLogBlockMgr, resize_failed_and_restar)
{
  CLOG_LOG(INFO, "begin resize_failed_and_restar");
  const int64_t reserved_size = 15 * ObServerLogBlockMgr::GB;
  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
  ObServerLogBlockMgr::LogPoolMeta meta = log_block_mgr_.get_log_pool_meta_guarded_by_lock_();
  meta.status_ = ObServerLogBlockMgr::EXPANDING_STATUS;
  meta.next_total_size_ = aligned_reserved_size;
  char log_pool_meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  EXPECT_EQ(OB_SUCCESS, databuff_printf(log_pool_meta_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",log_block_mgr_.log_pool_path_, "meta"));
  log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
      log_block_mgr_.log_pool_path_, log_pool_meta_path, meta,
      log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
  // 触发trim
  system("touch clog_disk/clog/log_pool/100050");
  system("touch clog_disk/clog/log_pool/100000");

  log_block_mgr_.destroy();
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));

  EXPECT_EQ(false, log_block_mgr_.log_pool_meta_.resizing());
  EXPECT_EQ(aligned_reserved_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
}

// This case will cause disk space not enough, ignore it.
//TEST_F(TestServerLogBlockMgr, expand_when_disk_space_not_enough)
//{
//  const int64_t reserved_size = 100 * 1024  * ObServerLogBlockMgr::GB;
//  const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
//  ObServerLogBlockMgr::LogPoolMeta origin_meta = log_block_mgr_.get_log_pool_meta_guarded_by_lock_();
//  EXPECT_EQ(OB_ALLOCATE_DISK_SPACE_FAILED, log_block_mgr_.resize_(aligned_reserved_size));
//  EXPECT_EQ(origin_meta, log_block_mgr_.get_log_pool_meta_guarded_by_lock_());
//}

// TEST_F(TestServerLogBlockMgr, performance)
// {
//   const int64_t reserved_size = 2 * ObServerLogBlockMgr::GB;
//   const int64_t free_size = log_block_mgr_.get_free_size_guarded_by_lock_();
//   const int64_t aligned_reserved_size = log_block_mgr_.lower_align_(reserved_size);
//   EXPECT_EQ(OB_NOT_SUPPORTED, log_block_mgr_.resize_(aligned_reserved_size));
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(log_block_mgr_.get_total_size_() - free_size));
//  EXPECT_EQ(OB_ENTRY_NOT_EXIST, create_new_blocks_at(1, tenant_ls_fd_map_[1], 100000, 10));
//
//   system("mkdir clog_disk/tmp.dir");
//   const int64_t reserved_size1 = 1024 * ObServerLogBlockMgr::GB;
//   const int64_t aligned_reserved_size1 = log_block_mgr_.lower_align_(reserved_size1);
//   int64_t start_ts = ObTimeUtility::current_time();
//   int fd = ::open("clog_disk/tmp.dir", ObServerLogBlockMgr::OPEN_DIR_FLAG);
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.allocate_blocks_at_tmp_dir_(fd, 0, aligned_reserved_size1/ObServerLogBlockMgr::BLOCK_SIZE));
//   int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
//   ::close(fd);
//   EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec("clog_disk/tmp.dir"));
//   CLOG_LOG(INFO, "allocate cost", K(cost_ts));
//   EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(aligned_reserved_size1));
// }

TEST_F(TestServerLogBlockMgr, check_dir_is_empty)
{
  system("mkdir clog_disk/test");
  system("mkdir clog_disk/test/log_pool");
  bool result = false;
  EXPECT_EQ(OB_SUCCESS, ObServerLogBlockMgr::check_clog_directory_is_empty("clog_disk/test", result));
  EXPECT_EQ(true, result);
  system("mkdir clog_disk/test/log_pool1");
  EXPECT_EQ(OB_SUCCESS, ObServerLogBlockMgr::check_clog_directory_is_empty("clog_disk/test", result));
  EXPECT_EQ(false, result);
}
TEST_F(TestServerLogBlockMgr, create_tenant_resize)
{
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(4 * GB));
  EXPECT_EQ(0, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, create_tenant(OB_MAX_RESERVED_TENANT_ID + 1, 2 * GB));
  EXPECT_EQ(OB_SUCCESS, create_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 2 * GB));
  EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, create_tenant(OB_MAX_RESERVED_TENANT_ID + 3, 2 * GB));
  EXPECT_EQ(OB_SUCCESS, remove_tenant(OB_MAX_RESERVED_TENANT_ID + 1));
  EXPECT_EQ(2 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, update_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 3 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, update_tenant(OB_MAX_RESERVED_TENANT_ID + 1, 3 * GB));
  EXPECT_EQ(OB_MACHINE_RESOURCE_NOT_ENOUGH, update_tenant(OB_MAX_RESERVED_TENANT_ID + 2, 5 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.resize_(10 * GB));
  EXPECT_EQ(3 * GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  EXPECT_EQ(10 * GB, log_block_mgr_.get_total_size_());
  constexpr int64_t MB = 1024*1024;
  std::thread t_create1(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        create_tenant(i, 32*MB);
      };
    }
  );
  std::thread t_create2(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        create_tenant(i, 32*MB);
      };
    }
  );
  std::thread t_update1(
    [&](){
      usleep(10);
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        update_tenant(i, 128*MB);
      };
    }
  );
  std::thread t_update2(
    [&](){
      usleep(10);
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        update_tenant(i, 128*MB);
      };
    }
  );
  t_create1.join();
  t_create2.join();
  t_update1.join();
  t_update2.join();
  EXPECT_EQ(10*GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
  std::thread t_remove1(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        remove_tenant(i);
      };
    }
  );
  std::thread t_remove2(
    [&](){
      for (int i = 4+OB_MAX_RESERVED_TENANT_ID; i < 1000+OB_MAX_RESERVED_TENANT_ID; i++) {
        remove_tenant(i);
      };
    }
  );
  t_remove1.join();
  t_remove2.join();
  EXPECT_EQ(3*GB, log_block_mgr_.min_log_disk_size_for_all_tenants_);
}

TEST_F(TestServerLogBlockMgr, resize_log_loop_and_restart)
{
  ObServerLogBlockMgr::LogPoolMeta meta;
  meta.curr_total_size_ = log_block_mgr_.log_pool_meta_.curr_total_size_;
  int64_t next_total_size = meta.next_total_size_ = meta.curr_total_size_ + 1*1024*1024*1024;
  meta.status_ = ObServerLogBlockMgr::EXPANDING_STATUS;
  char log_pool_meta_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  EXPECT_EQ(OB_SUCCESS, databuff_printf(log_pool_meta_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",log_block_mgr_.log_pool_path_, "meta"));
  log_block_mgr_.update_log_pool_meta_guarded_by_lock_(
      log_block_mgr_.log_pool_path_, log_pool_meta_path, meta,
      log_block_mgr_.reclaim_service_.is_cross_tenant_reclaim_enabled());
  char tmp_dir_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(tmp_dir_path, OB_MAX_FILE_NAME_LENGTH, "%s/expanding.tmp", log_block_mgr_.log_pool_path_);
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.make_resizing_tmp_dir_(tmp_dir_path));
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.remove_resizing_tmp_dir_(tmp_dir_path));
  char file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  auto touch_file_in_log_pool = [](const char *file_path) {
    char cmd_touch[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    char cmd_fallocate[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    snprintf(cmd_touch, OB_MAX_FILE_NAME_LENGTH, "touch %s", file_path);
    snprintf(cmd_fallocate, OB_MAX_FILE_NAME_LENGTH, "fallocate -l %lu %s", PALF_PHY_BLOCK_SIZE, file_path);
    system(cmd_touch);
    system(cmd_fallocate);
  };
  for (int i = 0; i < 10; i++) {
    int64_t file_id = i + 10000;
    snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%ld", log_block_mgr_.log_pool_path_, file_id);
    touch_file_in_log_pool(file_path);
  }
  log_block_mgr_.destroy();
  EXPECT_EQ(OB_SUCCESS, log_block_mgr_.init(log_pool_base_path_));
  EXPECT_EQ(ObServerLogBlockMgr::NORMAL_STATUS, log_block_mgr_.log_pool_meta_.status_);
  EXPECT_EQ(next_total_size, log_block_mgr_.log_pool_meta_.curr_total_size_);
}

class DummyBlockPool : public palf::ILogBlockPool {
public:
  virtual int create_block(const char *block_path,
                           const int64_t block_size)
  {
    if (-1 == ::open(block_path, palf::LOG_WRITE_FLAG | O_CREAT)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
  virtual int remove_block(const char *block_path)
  {
    if (-1 == ::unlink(block_path)) {
      return OB_IO_ERROR;
    }
    return OB_SUCCESS;
  }
};

TEST_F(TestServerLogBlockMgr, basic_func_test)
{
  const char *test_path = "clog_disk/basic_func_test";
  const char *file_path = "clog_disk/basic_func_test/1.tmp";
  char cmd_mkdir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char cmd_touch[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char cmd_fallocate[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(cmd_mkdir, OB_MAX_FILE_NAME_LENGTH, "mkdir %s", test_path);
  snprintf(cmd_touch, OB_MAX_FILE_NAME_LENGTH, "touch %s", file_path);
  snprintf(cmd_fallocate, OB_MAX_FILE_NAME_LENGTH, "fallocate -l %lu %s", PALF_PHY_BLOCK_SIZE, file_path);
  system(cmd_mkdir);
  system(cmd_touch);
  int dir_fd = ::open(test_path, O_DIRECTORY | O_RDONLY);
  bool result = false;
  EXPECT_EQ(OB_SUCCESS, is_block_used_for_palf(file_path, result));
  EXPECT_EQ(false, result);
  system(cmd_fallocate);
  EXPECT_EQ(OB_SUCCESS, is_block_used_for_palf(file_path, result));
  EXPECT_EQ(true, result);
  DummyBlockPool block_pool;
  EXPECT_EQ(OB_SUCCESS, remove_tmp_file_or_directory_at(test_path, &block_pool));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_empty_directory(test_path, result));
  EXPECT_EQ(true, result);
  EXPECT_EQ(OB_SUCCESS, remove_directory_rec("clog_disk", &block_pool));
  EXPECT_EQ(OB_SUCCESS, FileDirectoryUtils::is_exists("clog_disk", result));
  EXPECT_EQ(false, result);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_server_log_block_mgr.log");
  system("rm -rf clog_disk");
  system("rm -rf clog_disk/clog");
  system("mkdir clog_disk");
  system("mkdir clog_disk/clog");
  OB_LOGGER.set_file_name("test_server_log_block_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_server_log_block_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::unittest::init_io();
  return RUN_ALL_TESTS();
}
