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

#ifndef OB_DATA_FILE_PREPARE_H_
#define OB_DATA_FILE_PREPARE_H_

#include <gtest/gtest.h>
#include <cctype>
#include "share/ob_define.h"
#include "lib/stat/ob_session_stat.h"
#define private public
#define protected public
#include "lib/file/file_directory_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_file_system_util.h"
#include "share/ob_force_print_log.h"
#include "ob_macro_meta_generate.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
namespace blocksstable {

class TestDataFilePrepareUtil {
public:
  TestDataFilePrepareUtil();
  virtual ~TestDataFilePrepareUtil()
  {
    destory();
  }
  int init(const char* test_name, const int64_t macro_block_size = 64 * 1024, const int64_t macro_block_count = 100,
      const int64_t disk_num = 0);
  virtual int open();
  virtual int restart();
  virtual void destory();
  ObStorageEnv& get_storage_env()
  {
    return storage_env_;
  }
  ObStoreFileSystem& get_file_system()
  {
    return OB_FILE_SYSTEM;
  }

private:
  static const int64_t SLOG_MAX_SIZE = 64 * 1024 * 1024;
  bool is_inited_;
  char data_dir_[OB_MAX_FILE_NAME_LENGTH];
  char file_dir_[OB_MAX_FILE_NAME_LENGTH];
  char slog_dir_[OB_MAX_FILE_NAME_LENGTH];
  char clog_dir_[OB_MAX_FILE_NAME_LENGTH];
  char ilog_dir_[OB_MAX_FILE_NAME_LENGTH];
  char clog_shm_name_[OB_MAX_FILE_NAME_LENGTH];
  char ilog_shm_name_[OB_MAX_FILE_NAME_LENGTH];
  char test_name_[OB_MAX_FILE_NAME_LENGTH];
  ObStorageEnv storage_env_;
  storage::MockObIPartitionService partition_service_;
  blocksstable::ObNotReplaySuperBlockAndConfigMetaSLogFilter slog_filter_;
  ObArenaAllocator allocator_;
  int64_t disk_num_;
};

class TestDataFilePrepare : public ::testing::Test {
public:
  TestDataFilePrepare(
      const char* test_name, const int64_t macro_block_size = 64 * 1024, const int64_t macro_block_count = 100);
  virtual ~TestDataFilePrepare();
  virtual void SetUp();
  virtual void TearDown();
  virtual void fake_freeze_mgr();
  const ObStorageEnv& get_storage_env()
  {
    return util_.get_storage_env();
  }
  ObStoreFileSystem& get_file_system()
  {
    return util_.get_file_system();
  }
  const ObStorageFileHandle& get_storage_file_handle();

protected:
  static const int64_t TENANT_ID = 1;
  static const int64_t TABLE_ID = 3001;
  TestDataFilePrepareUtil util_;
  ObArenaAllocator allocator_;
  const char* test_name_;
  const int64_t macro_block_size_;
  const int64_t macro_block_count_;
};

TestDataFilePrepare::TestDataFilePrepare(
    const char* test_name, const int64_t macro_block_size, const int64_t macro_block_count)
    : util_(),
      allocator_(ObModIds::TEST),
      test_name_(test_name),
      macro_block_size_(macro_block_size),
      macro_block_count_(macro_block_count)
{}

TestDataFilePrepare::~TestDataFilePrepare()
{}

void TestDataFilePrepare::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, util_.init(test_name_, macro_block_size_, macro_block_count_));
  ASSERT_EQ(OB_SUCCESS, util_.open());
  fake_freeze_mgr();
}

void TestDataFilePrepare::TearDown()
{
  util_.destory();
}

void TestDataFilePrepare::fake_freeze_mgr()
{
  common::ObArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite> freeze_info;
  common::ObArray<ObFreezeInfoSnapshotMgr::SchemaPair> gc_schema_version;
  common::ObArray<share::ObSnapshotInfo> snapshots;
  common::ObMySQLProxy sql_proxy;
  ObFreezeInfoSnapshotMgr* freeze_mgr;
  freeze_mgr = &ObFreezeInfoMgrWrapper::get_instance();
  if (!freeze_mgr->is_inited()) {
    ASSERT_EQ(OB_SUCCESS, freeze_mgr->init(sql_proxy, false));

    const int64_t snapshot_gc_ts = 500;
    const int64_t backup_snapshot_version = snapshot_gc_ts - 100;
    const int64_t delay_delete_snapshot_version = backup_snapshot_version;
    bool changed = false;

    ASSERT_EQ(OB_SUCCESS, gc_schema_version.push_back(ObFreezeInfoSnapshotMgr::SchemaPair(1, 210)));
    ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObFreezeInfoSnapshotMgr::FreezeInfoLite(1, 1, 0)));
    ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObFreezeInfoSnapshotMgr::FreezeInfoLite(2, 100, 0)));
    ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObFreezeInfoSnapshotMgr::FreezeInfoLite(3, 200, 0)));
    ASSERT_EQ(OB_SUCCESS, freeze_info.push_back(ObFreezeInfoSnapshotMgr::FreezeInfoLite(4, 400, 0)));

    ASSERT_EQ(OB_SUCCESS,
        freeze_mgr->update_info(snapshot_gc_ts,
            gc_schema_version,
            freeze_info,
            snapshots,
            backup_snapshot_version,
            delay_delete_snapshot_version,
            INT64_MAX,
            changed));
  }
}

const ObStorageFileHandle& TestDataFilePrepare::get_storage_file_handle()
{
  ObPGKey pg_key(combine_id(1, 1001), 1, 0);
  ObIPartitionGroupGuard pg_guard;
  util_.partition_service_.get_partition(pg_key, pg_guard);
  return util_.partition_service_.mock_pg_.get_storage_file_handle();
}

TestDataFilePrepareUtil::TestDataFilePrepareUtil()
    : is_inited_(), storage_env_(), partition_service_(), slog_filter_(), allocator_(ObModIds::TEST), disk_num_(0)
{}

int TestDataFilePrepareUtil::init(
    const char* test_name, const int64_t macro_block_size, const int64_t macro_block_count, const int64_t disk_num)
{
  int ret = OB_SUCCESS;
  char cur_dir[OB_MAX_FILE_NAME_LENGTH];

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(test_name) || disk_num < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid args", K(ret), KP(test_name), K(disk_num));
  } else if (NULL == getcwd(cur_dir, OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "canot get cur dir", K(ret));
  } else if (OB_FAIL(databuff_printf(test_name_, OB_MAX_FILE_NAME_LENGTH, "%s", test_name))) {
    STORAGE_LOG(WARN, "failed to gen test name", K(ret));
  } else if (OB_FAIL(databuff_printf(data_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/data_%s", cur_dir, test_name))) {
    STORAGE_LOG(WARN, "failed to gen data dir", K(ret));
  } else if (OB_FAIL(databuff_printf(file_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/sstable/", data_dir_))) {
    STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(slog_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/slog/", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen slog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/clog/", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen clog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(ilog_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/ilog/", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen ilog dir", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_shm_name_, OB_MAX_FILE_NAME_LENGTH, "%s/clog_shm", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen clog shm name", K(ret));
  } else if (OB_FAIL(databuff_printf(ilog_shm_name_, OB_MAX_FILE_NAME_LENGTH, "%s/ilog_shm", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen ilog shm name", K(ret));
  } else {
    storage_env_.data_dir_ = data_dir_;
    storage_env_.sstable_dir_ = file_dir_;
    storage_env_.default_block_size_ = macro_block_size;

    storage_env_.log_spec_.log_dir_ = slog_dir_;
    storage_env_.log_spec_.max_log_size_ = 64 * 1024 * 1024;

    storage_env_.clog_dir_ = clog_dir_;
    storage_env_.ilog_dir_ = ilog_dir_;
    storage_env_.clog_shm_path_ = clog_shm_name_;
    storage_env_.ilog_shm_path_ = ilog_shm_name_;

    storage_env_.bf_cache_miss_count_threshold_ = 10000;
    storage_env_.bf_cache_priority_ = 1;
    storage_env_.index_cache_priority_ = 10;
    storage_env_.user_block_cache_priority_ = 1;
    storage_env_.user_row_cache_priority_ = 1;
    storage_env_.fuse_row_cache_priority_ = 1;
    storage_env_.clog_cache_priority_ = 1;
    storage_env_.index_clog_cache_priority_ = 1;
    storage_env_.ethernet_speed_ = 1000000;
    storage_env_.redundancy_level_ = ObStorageEnv::NORMAL_REDUNDANCY;

    storage_env_.disk_avail_space_ = macro_block_count * macro_block_size;
    GCONF.micro_block_merge_verify_level = 0;
    GCTX.self_addr_.set_ip_addr("100.1.2.3", 456);
    disk_num_ = disk_num;

    is_inited_ = true;
  }
  return ret;
}

int TestDataFilePrepareUtil::open()
{
  int ret = OB_SUCCESS;
  char cmd[OB_MAX_FILE_NAME_LENGTH];
  char dirname[MAX_PATH_SIZE];
  char link_name[MAX_PATH_SIZE];

  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  const int64_t mem_limit = 10 * 1024L * 1024L * 1024L;
  lib::set_memory_limit(mem_limit);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(databuff_printf(cmd, OB_MAX_FILE_NAME_LENGTH, "rm -rf %s", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen cmd", K(ret));
  } else if (0 != system(cmd)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "failed to exec cmd", K(ret), K(cmd), K(errno), KERRMSG);
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size))) {
    STORAGE_LOG(WARN, "failed to init kv cache", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    STORAGE_LOG(WARN, "failed to init io manager", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(file_dir_))) {
    STORAGE_LOG(WARN, "failed to create file dir", K(ret), K(file_dir_));
  } else if (disk_num_ > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_num_; ++i) {
      if (OB_FAIL(databuff_printf(dirname, sizeof(dirname), "%s/%ld", data_dir_, i))) {
        STORAGE_LOG(WARN, "Failed to init dirname", K(ret));
      } else if (OB_FAIL(databuff_printf(link_name, sizeof(link_name), "%s/%ld", file_dir_, i))) {
        STORAGE_LOG(WARN, "Failed to init link_name", K(ret));
      } else if (OB_FAIL(FileDirectoryUtils::create_full_path(dirname))) {
        STORAGE_LOG(WARN, "failed to create sstable sub dir", K(ret), K(dirname));
      } else if (OB_FAIL(FileDirectoryUtils::symlink(dirname, link_name))) {
        STORAGE_LOG(WARN, "failed to create sstable sub dir", K(ret), K(dirname));
      } else {
        STORAGE_LOG(INFO, "succeed to create full path", K(ret), K(dirname));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(SLOGGER.init(storage_env_.log_spec_.log_dir_, storage_env_.log_spec_.max_log_size_, &slog_filter_))) {
      STORAGE_LOG(WARN, "Fail to init ObBaseStorageLogger, ", K(ret));
    } else if (OB_FAIL(OB_STORE_CACHE.init(storage_env_.index_cache_priority_,
                   storage_env_.user_block_cache_priority_,
                   storage_env_.user_row_cache_priority_,
                   storage_env_.fuse_row_cache_priority_,
                   storage_env_.bf_cache_priority_,
                   storage_env_.bf_cache_miss_count_threshold_))) {
      STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.init())) {
      STORAGE_LOG(WARN, "fail to init server file mgr", K(ret));
    } else if (OB_FAIL(ObStoreFileSystemWrapper::init(storage_env_, partition_service_))) {
      STORAGE_LOG(WARN, "failed to init store file system", K(ret), K(storage_env_));
    } else if (OB_FAIL(ObStoreFileSystemWrapper::get_instance().start())) {
      STORAGE_LOG(WARN, "failed to open store file", K(ret));
    } else if (OB_FAIL(ObClusterVersion::get_instance().init(CLUSTER_VERSION_2100))) {
      STORAGE_LOG(WARN, "failed to init cluster version", K(ret));
    }
  }
  return ret;
}

int TestDataFilePrepareUtil::restart()
{
  int ret = OB_SUCCESS;
  partition_service_.clear();
  OB_FILE_SYSTEM.stop();
  OB_FILE_SYSTEM.wait();
  ObStoreFileSystemWrapper::destroy();
  OB_STORE_CACHE.destroy();
  SLOGGER.destroy();
  OB_SERVER_FILE_MGR.destroy();

  if (OB_FAIL(SLOGGER.init(storage_env_.log_spec_.log_dir_, storage_env_.log_spec_.max_log_size_, &slog_filter_))) {
    STORAGE_LOG(WARN, "Fail to init ObBaseStorageLogger, ", K(ret));
  } else if (OB_FAIL(OB_STORE_CACHE.init(storage_env_.index_cache_priority_,
                 storage_env_.user_block_cache_priority_,
                 storage_env_.user_row_cache_priority_,
                 storage_env_.fuse_row_cache_priority_,
                 storage_env_.bf_cache_priority_,
                 storage_env_.bf_cache_miss_count_threshold_))) {
    STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
  } else if (OB_FAIL(ObStoreFileSystemWrapper::init(storage_env_, partition_service_))) {
    STORAGE_LOG(WARN, "failed to init store file", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.start())) {
    STORAGE_LOG(WARN, "failed to open store file", K(ret));
  }
  return ret;
}

void TestDataFilePrepareUtil::destory()
{
  partition_service_.clear();
  OB_SERVER_FILE_MGR.destroy();
  ObStoreFileSystemWrapper::destroy();
  OB_STORE_CACHE.destroy();
  SLOGGER.destroy();
  ObIOManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObClusterVersion::get_instance().destroy();
  allocator_.reuse();

  if (is_inited_) {
    char cmd[OB_MAX_FILE_NAME_LENGTH];
    if (OB_SUCCESS != databuff_printf(cmd, OB_MAX_FILE_NAME_LENGTH, "rm -rf %s", data_dir_)) {
      STORAGE_LOG(ERROR, "failed to gen cmd", K(data_dir_));
    } else if (0 != system(cmd)) {
      STORAGE_LOG(ERROR, "failed to  rm data dir", K(cmd), K(errno), KERRMSG);
    }
  }
  is_inited_ = false;
}
}  // namespace blocksstable
}  // namespace oceanbase
#endif /* OB_DATA_FILE_PREPARE_H_ */
