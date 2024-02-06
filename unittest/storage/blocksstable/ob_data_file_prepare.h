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
#include "common/log/ob_log_cursor.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_io_device_helper.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/ob_file_system_router.h"
#include "share/ob_force_print_log.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/slog/ob_storage_logger_manager.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
namespace blocksstable
{
class TestDataFilePrepareUtil
{
public:
  TestDataFilePrepareUtil();
  virtual ~TestDataFilePrepareUtil() { destory(); }
  int init(ObITenantMemLimitGetter *getter,
           const char *test_name,
           const int64_t macro_block_size = 64 * 1024,
           const int64_t macro_block_count = 100,
           const int64_t disk_num = 0);
  virtual int open();
  virtual void destory();
  ObStorageEnv &get_storage_env() { return storage_env_; }
  ObLogFileHandler *get_clog_file_handler() { return &clog_file_handler_; }
private:
  static const int64_t SLOG_MAX_SIZE = 64 * 1024 * 1024;
  bool is_inited_;
  char data_dir_[OB_MAX_FILE_NAME_LENGTH];
  char file_dir_[OB_MAX_FILE_NAME_LENGTH];
  char slog_dir_[OB_MAX_FILE_NAME_LENGTH];
  char clog_dir_[OB_MAX_FILE_NAME_LENGTH];
  char test_name_[OB_MAX_FILE_NAME_LENGTH];
  ObStorageEnv storage_env_;
  ObArenaAllocator allocator_;
  int64_t disk_num_;
  ObLogFileHandler clog_file_handler_;
  ObITenantMemLimitGetter *getter_;
};

class TestDataFilePrepare : public ::testing::Test
{
public:
  TestDataFilePrepare(ObITenantMemLimitGetter *getter,
                      const char *test_name,
                      const int64_t macro_block_size = OB_DEFAULT_MACRO_BLOCK_SIZE,
                      const int64_t macro_block_count = 100);
  virtual ~TestDataFilePrepare();
  virtual void SetUp();
  virtual void TearDown();
  const ObStorageEnv &get_storage_env() { return util_.get_storage_env(); }
protected:
  static const int64_t TENANT_ID = 1;
  static const int64_t TABLE_ID = 50001;
  TestDataFilePrepareUtil util_;
  ObArenaAllocator allocator_;
  const char *test_name_;
  const int64_t macro_block_size_;
  const int64_t macro_block_count_;
  ObITenantMemLimitGetter *getter_;
};

TestDataFilePrepare::TestDataFilePrepare(ObITenantMemLimitGetter *getter,
                                         const char *test_name,
                                         const int64_t macro_block_size,
                                         const int64_t macro_block_count)
  : util_(),
    allocator_(ObModIds::TEST),
    test_name_(test_name),
    macro_block_size_(macro_block_size),
    macro_block_count_(macro_block_count),
    getter_(getter)
{
}

TestDataFilePrepare::~TestDataFilePrepare()
{
}


void TestDataFilePrepare::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, util_.init(getter_, test_name_, macro_block_size_, macro_block_count_));
  ASSERT_EQ(OB_SUCCESS, util_.open());
}

void TestDataFilePrepare::TearDown()
{
  util_.destory();
}


TestDataFilePrepareUtil::TestDataFilePrepareUtil()
  : is_inited_(),
    storage_env_(),
    allocator_(ObModIds::TEST),
    disk_num_(0),
    clog_file_handler_(),
    getter_(nullptr)
{
}

int TestDataFilePrepareUtil::init(
    ObITenantMemLimitGetter *getter,
    const char *test_name,
    const int64_t macro_block_size,
    const int64_t macro_block_count,
    const int64_t disk_num)
{
  int ret = OB_SUCCESS;
  char cur_dir[OB_MAX_FILE_NAME_LENGTH];

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(getter) || OB_ISNULL(test_name) || disk_num < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid args", K(ret), KP(getter), KP(test_name), K(disk_num));
  } else if (NULL == getcwd(cur_dir,  OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "cannot get cur dir", K(ret));
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
  } else {
    storage_env_.data_dir_ = data_dir_;
    storage_env_.sstable_dir_ = file_dir_;
    storage_env_.default_block_size_ = macro_block_size;

    storage_env_.log_spec_.log_dir_ = slog_dir_;
    storage_env_.log_spec_.max_log_file_size_ = 64 * 1024 * 1024;

    storage_env_.clog_dir_ = clog_dir_;

    storage_env_.bf_cache_miss_count_threshold_ = 10000;
    storage_env_.bf_cache_priority_ = 1;
    storage_env_.index_block_cache_priority_ = 10;
    storage_env_.user_block_cache_priority_ = 1;
    storage_env_.user_row_cache_priority_ = 1;
    storage_env_.fuse_row_cache_priority_ = 1;
    storage_env_.tablet_ls_cache_priority_ = 1;
    storage_env_.storage_meta_cache_priority_ = 10;
    storage_env_.ethernet_speed_ = 1000000;
    storage_env_.redundancy_level_ = ObStorageEnv::NORMAL_REDUNDANCY;

    storage_env_.clog_file_spec_.retry_write_policy_ = "normal";
    storage_env_.clog_file_spec_.log_create_policy_ = "normal";
    storage_env_.clog_file_spec_.log_write_policy_ = "truncate";

    storage_env_.slog_file_spec_.retry_write_policy_ = "normal";
    storage_env_.slog_file_spec_.log_create_policy_ = "normal";
    storage_env_.slog_file_spec_.log_write_policy_ = "truncate";

    storage_env_.data_disk_size_ = macro_block_count * macro_block_size;
    GCONF.micro_block_merge_verify_level = 0;
    ObAddr tmp_addr = GCTX.self_addr();
    tmp_addr.set_ip_addr("100.1.2.3", 456);
    GCTX.self_addr_seq_.set_addr(tmp_addr);
    disk_num_ = disk_num;
    getter_ = getter;
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

  const int64_t default_partition_cnt = 1000;
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(databuff_printf(cmd, OB_MAX_FILE_NAME_LENGTH, "rm -rf %s", data_dir_))) {
    STORAGE_LOG(WARN, "failed to gen cmd", K(ret));
  } else if (0 != system(cmd)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "failed to exec cmd", K(ret), K(cmd), K(errno), KERRMSG);
  } else if (OB_FAIL(SLOGGERMGR.init(storage_env_.log_spec_.log_dir_,
      storage_env_.log_spec_.max_log_file_size_, storage_env_.slog_file_spec_))) {
    STORAGE_LOG(WARN, "ObStorageLoggerManager init failed", KR(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(getter_,
                                                          bucket_num,
                                                          max_cache_size,
                                                          block_size))) {
    STORAGE_LOG(WARN, "failed to init kv cache", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(clog_dir_))) {
    STORAGE_LOG(WARN, "failed to create clog dir", K(ret), K(clog_dir_));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(slog_dir_))) {
    STORAGE_LOG(WARN, "failed to create slog dir", K(ret), K(slog_dir_));
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(file_dir_))) {
    STORAGE_LOG(WARN, "failed to create file dir", K(ret), K(file_dir_));
  } else if (disk_num_ > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_num_; ++i) {
      if (OB_FAIL(databuff_printf(dirname, sizeof(dirname), "%s/%ld", data_dir_, i))) {
        STORAGE_LOG(WARN, "Failed to init dir name", K(ret));
      } else if (OB_FAIL(databuff_printf(link_name, sizeof(link_name), "%s/%ld", file_dir_, i))) {
        STORAGE_LOG(WARN, "Failed to init link_name", K(ret));
      } else if (OB_FAIL(FileDirectoryUtils::create_full_path(dirname))) {
        STORAGE_LOG(WARN, "failed to create sstable sub dir", K(ret), K(dirname));
      }else if (OB_FAIL(FileDirectoryUtils::symlink(dirname, link_name))) {
        STORAGE_LOG(WARN, "failed to create sstable sub dir", K(ret), K(dirname));
      } else {
        STORAGE_LOG(INFO, "succeed to create full path", K(ret), K(dirname));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTenantIOConfig io_config = ObTenantIOConfig::default_instance();
    const int64_t async_io_thread_count = 8;
    const int64_t sync_io_thread_count = 2;
    const int64_t max_io_depth = 256;
    static ObTenantBase tenant_ctx(OB_SERVER_TENANT_ID);
    ObTenantEnv::set_tenant(&tenant_ctx);
    if (OB_FAIL(ObIODeviceWrapper::get_instance().init(
          storage_env_.data_dir_,
          storage_env_.sstable_dir_,
          storage_env_.default_block_size_,
          storage_env_.data_disk_percentage_,
          storage_env_.data_disk_size_))) {
      STORAGE_LOG(WARN, "init io device fail", K(ret), K(storage_env_));
    } else if (OB_FAIL(ObIOManager::get_instance().init())) {
      STORAGE_LOG(WARN, "failed to init io manager", K(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(THE_IO_DEVICE,
                                                                      async_io_thread_count,
                                                                      sync_io_thread_count,
                                                                      max_io_depth))) {
      STORAGE_LOG(WARN, "add device channel failed", K(ret));
    } else {
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(THE_IO_DEVICE,
          storage_env_.default_block_size_))) {
        STORAGE_LOG(WARN, "init block manager fail", K(ret));
      } else if (OB_FAIL(OB_STORE_CACHE.init(
          storage_env_.index_block_cache_priority_,
          storage_env_.user_block_cache_priority_,
          storage_env_.user_row_cache_priority_,
          storage_env_.fuse_row_cache_priority_,
          storage_env_.bf_cache_priority_,
          storage_env_.bf_cache_miss_count_threshold_,
          storage_env_.storage_meta_cache_priority_))) {
        STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
      } else if (OB_FAIL(ObIOManager::get_instance().start())) {
        STORAGE_LOG(WARN, "Fail to star io mgr", K(ret));
      } else if (OB_FAIL(ObIOManager::get_instance().add_tenant_io_manager(OB_SERVER_TENANT_ID, io_config))) {
        STORAGE_LOG(WARN, "add tenant io config failed", K(ret));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.start(0/*reserver_size*/))) {
        STORAGE_LOG(WARN, "Fail to start server block mgr", K(ret));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.first_mark_device())) {
        STORAGE_LOG(WARN, "Fail to start first mark device", K(ret));
      } else if (OB_FAIL(ObClusterVersion::get_instance().init(CLUSTER_VERSION_2100))) {
        STORAGE_LOG(WARN, "Fail to init cluster version", K(ret));
      } else if (OB_FAIL(clog_file_handler_.init(storage_env_.clog_dir_, storage_env_.log_spec_.max_log_file_size_))) {
        STORAGE_LOG(WARN, "failed to create clog file handler", K(ret));
      }
    }
  }
  return ret;
}

void TestDataFilePrepareUtil::destory()
{
  OB_SERVER_BLOCK_MGR.destroy();
  OB_STORE_CACHE.destroy();
  ObIOManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  SLOGGERMGR.destroy();
  ObClusterVersion::get_instance().destroy();
  ObIODeviceWrapper::get_instance().destroy();
  allocator_.reuse();

  if (is_inited_) {
    char cmd[OB_MAX_FILE_NAME_LENGTH];
    if (OB_SUCCESS != databuff_printf(cmd, OB_MAX_FILE_NAME_LENGTH, "rm -rf %s", data_dir_)) {
      STORAGE_LOG_RET(ERROR, OB_ERROR, "failed to gen cmd", K(data_dir_));
    } else if (0 != system(cmd)) {
      STORAGE_LOG_RET(ERROR, OB_ERR_SYS, "failed to  rm data dir", K(cmd), K(errno), KERRMSG);
    }
  }
  getter_ = nullptr;
  is_inited_ = false;
}
}
}
#endif /* OB_DATA_FILE_PREPARE_H_ */
