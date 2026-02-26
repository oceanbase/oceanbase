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
#include "logservice/ob_log_external_storage_handler.h"
#include "logservice/ob_log_external_storage_utils.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/io/ob_io_manager.h"
#undef protected
#undef private
#include "share/ob_device_manager.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;


int delete_oss_object(const common::ObString &uri,
                   const ObString &oss_info)
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter io_adapter;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(OB_STORAGE_FILE, oss_info.ptr()))) {
    CLOG_LOG(WARN, "set ObBackupStorageInfo failed", K(oss_info));
  } else if (OB_FAIL(io_adapter.del_file(uri, &storage_info))) {
    CLOG_LOG(WARN, "del dir failed", K(oss_info));
  } else {}
  return ret;
}

int generate_oss_data(const common::ObString &uri,
                      const ObString &oss_info,
                      const char *buf,
                      const int64_t size)
{

  common::ObBackupIoAdapter io_adapter;
  share::ObBackupStorageInfo storage_info;
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_FAIL(storage_info.set(OB_STORAGE_FILE, oss_info.ptr()))) {
    CLOG_LOG(WARN, "set ObBackupStorageInfo failed", K(oss_info));
  } else if (OB_FAIL(io_adapter.is_exist(uri, &storage_info, exist))) {
    CLOG_LOG(WARN, "is_exist failed", K(oss_info));
  } else if (!exist && OB_FAIL(io_adapter.write_single_file(uri, &storage_info, buf, size,
                                          ObStorageIdMod::get_default_id_mod()))) {
    CLOG_LOG(WARN, "ObBackupStorageInfo write_single_file failed", K(oss_info));
  } else {
    CLOG_LOG(INFO, "ObBackupStorageInfo write_single_file success", K(oss_info), K(uri), K(size));
  }
  return ret;
}

TEST(TestLogExternalStorageHandler, test_log_external_storage_handler)
{
  ObLogExternalStorageHandler handler;
  // 测试异常内存状态——没有init
  EXPECT_EQ(false, handler.is_inited_);
  EXPECT_EQ(OB_NOT_INIT, handler.start(10));
  const int64_t MB = 1*1024*1024;
  std::string curr_dir(get_current_dir_name());
  std::string test_file = "file://" + curr_dir + "/runlin_test";
  ObString uri = test_file.c_str();
  ObString storage_info = "runlin_test";
  int64_t offset = 0;
  int64_t read_buf_size = 15*MB;
  char *read_buf = reinterpret_cast<char*>(ob_malloc(palf::PALF_PHY_BLOCK_SIZE, "unittest"));
  ASSERT_NE(nullptr, read_buf);
  int64_t real_read_size = 0;
  uint64_t storage_id = 2;
  palf::LogIOContext io_ctx;
  EXPECT_EQ(OB_NOT_INIT, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
  EXPECT_EQ(OB_NOT_INIT, handler.resize(16, 0));

  // 测试异常内存状态——没有start
  EXPECT_EQ(OB_SUCCESS, handler.init());
  EXPECT_EQ(true, handler.is_inited_);
  EXPECT_EQ(OB_NOT_RUNNING, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
  EXPECT_EQ(OB_NOT_RUNNING, handler.resize(16, 100));


  // 测试invalid argument
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.start(-1));
  // 当concurrency超过最大并发度时，以最大并发度为准
  EXPECT_NE(OB_INVALID_ARGUMENT, handler.start(ObLogExternalStorageHandler::CONCURRENCY_LIMIT+1));
  EXPECT_EQ(ObLogExternalStorageHandler::CONCURRENCY_LIMIT, handler.concurrency_);
  EXPECT_EQ(ObLogExternalStorageHandler::CONCURRENCY_LIMIT*ObLogExternalStorageHandler::CAPACITY_COEFFICIENT,
            handler.capacity_);
  EXPECT_EQ(true, handler.is_running_);

  // 重复start
  const int64_t concurrency = 16;
  EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));

  EXPECT_EQ(OB_SUCCESS, generate_oss_data(uri, storage_info, read_buf, palf::PALF_PHY_BLOCK_SIZE));

  // 验证读取——invalid argument
  {
    ObString empty_uri;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(empty_uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
    // NFS的storage info为empty
    ObString empty_storage_info;
    EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
    int64_t invalid_offset = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, storage_id, invalid_offset, read_buf, read_buf_size, real_read_size, io_ctx));
    // 读偏移等于文件长度，返回成功，real_read_size=0
    int64_t valid_offset = 64*1024*1024;
    EXPECT_NE(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, storage_id, valid_offset, read_buf, read_buf_size, real_read_size, io_ctx));
    EXPECT_EQ(0, real_read_size);
    char *invalid_read_buf = NULL;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, storage_id, offset, invalid_read_buf, read_buf_size, real_read_size, io_ctx));
    int64_t invalid_read_buf_size = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, storage_id, offset, read_buf, invalid_read_buf_size, real_read_size, io_ctx));
  }

  // 验证resize——invalid argument
  {
    int64_t invalid_concurrency = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.resize(invalid_concurrency, 0));
    int64_t invalid_timeout_us = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.resize(concurrency, invalid_timeout_us));

  }

  // 验证私有函数
  {
    EXPECT_EQ(OB_SUCCESS, handler.resize_(16));
    // 验证is_valid_concurrency_为false
    int64_t invalid_concurrency = -1;
    EXPECT_EQ(false, handler.is_valid_concurrency_(invalid_concurrency));

    // 当并发度超过128时，会将concurrency_设置为128.
    invalid_concurrency = ObLogExternalStorageHandler::CONCURRENCY_LIMIT + 1;
    EXPECT_EQ(true, handler.is_valid_concurrency_(invalid_concurrency));

    // 验证get_async_task_count_
    // 单个任务最小2M, 在concurrency足够的情况下，最多存在8个异步任务
    int64_t total_size = 15 * MB;
    EXPECT_EQ(8, handler.get_async_task_count_(total_size));
    // 最多存在51个异步任务，由于并发度问题，只能存在16个
    total_size = 101 * MB;
    EXPECT_EQ(16, handler.get_async_task_count_(total_size));

    ObLogExternalStorageCtx pread_ctx;
    // 验证construct_async_tasks_and_push_them_into_thread_pool_
    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.construct_async_pread_tasks_
              (uri, storage_info, storage_id, offset, read_buf, read_buf_size, pread_ctx));
    EXPECT_EQ(OB_SUCCESS, pread_ctx.wait(real_read_size));
    ASSERT_EQ(real_read_size, read_buf_size);
    CLOG_LOG(INFO, "after test private interface", K(real_read_size), K(read_buf_size));
  }

  // 验证公有函数
  {
    real_read_size = 0;
    int64_t invalid_offset = 100*1024*1024;
    EXPECT_EQ(OB_FILE_LENGTH_INVALID, handler.pread(uri, storage_info, storage_id, invalid_offset, read_buf, read_buf_size, real_read_size, io_ctx));

    {
      int64_t start_ts = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
      EXPECT_EQ(read_buf_size, real_read_size);
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      CLOG_LOG(INFO, "after first read", K(read_buf_size), K(real_read_size), K(cost_ts), K(concurrency));
    }
    int64_t new_concurrency = 8;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));
    EXPECT_EQ(new_concurrency, handler.concurrency_);
    EXPECT_EQ(new_concurrency*ObLogExternalStorageHandler::CAPACITY_COEFFICIENT,
              handler.capacity_);

    new_concurrency = 129;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));
    EXPECT_EQ(ObLogExternalStorageHandler::CONCURRENCY_LIMIT, handler.concurrency_);
    EXPECT_EQ(ObLogExternalStorageHandler::CONCURRENCY_LIMIT*ObLogExternalStorageHandler::CAPACITY_COEFFICIENT,
              handler.capacity_);
    new_concurrency = 0;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));

    real_read_size = 0;
    {
      int64_t start_ts = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
      EXPECT_EQ(read_buf_size, real_read_size);
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      CLOG_LOG(INFO, "after second read", K(read_buf_size), K(real_read_size), K(cost_ts), K(concurrency));
    }

    new_concurrency = 32;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));

    real_read_size = 0;
    {
      int64_t start_ts = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
      EXPECT_EQ(read_buf_size, real_read_size);
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      CLOG_LOG(INFO, "after third read", K(read_buf_size), K(real_read_size), K(cost_ts), K(concurrency));
    }


    real_read_size = 0;
    {
      read_buf_size = palf::PALF_PHY_BLOCK_SIZE;
      int64_t start_ts = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx));
      EXPECT_EQ(read_buf_size, real_read_size);
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      CLOG_LOG(INFO, "after fourth read", K(read_buf_size), K(real_read_size), K(cost_ts), K(concurrency));
    }

  }

  // 并发场景验证
  {
    const int64_t pread_thread_count = 2;
    std::vector<std::thread> pread_thread;
    auto read_func = [&](){
      for (int i = 0; i < 8; i++) {
        srandom(ObTimeUtility::current_time());
        const int64_t tmp_read_buf_size = random()%64*1024*1024 + 1;
        int64_t tmp_real_read_size = 0;
        int64_t tmp_offset = 64*1024*1024 - tmp_read_buf_size;
        handler.pread(uri, storage_info, storage_id, tmp_offset, read_buf, tmp_read_buf_size, tmp_real_read_size, io_ctx);
        EXPECT_EQ(tmp_real_read_size, tmp_read_buf_size);
        CLOG_LOG(INFO, "pread in thread success", K(tmp_real_read_size), K(tmp_read_buf_size));
      }
    };
    auto resize_func = [&](){
      for (int i = 0; i < 8; i++) {
        srandom(ObTimeUtility::current_time());
        handler.resize(random()%7);
        usleep(1000 * 1000);
      }
    };
    for (int i = 0; i < pread_thread_count; i++) {
      pread_thread.emplace_back(std::thread(read_func));
    }
    std::thread resize_thread(resize_func);

    for (int i = 0; i < pread_thread_count; i++) {
      pread_thread[i].join();
    }
    resize_thread.join();
  }
  delete_oss_object(uri, storage_info);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_log_external_storage_handler.log*");
  OB_LOGGER.set_file_name("test_log_external_storage_handler.log", true);
  OB_LOGGER.set_log_level("TRACE");
  srandom(ObTimeUtility::current_time());
  PALF_LOG(INFO, "begin unittest::test_log_external_storage_handler");
  EXPECT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  EXPECT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(1000000000));
  EXPECT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOConfig tenant_io_config = ObTenantIOConfig::default_instance();
  oceanbase::share::ObTenantBase *tenant_base = MTL_NEW(oceanbase::share::ObTenantBase, "unittest", OB_SERVER_TENANT_ID);
  ObTenantIOManager *tio_manager = NULL;
  ObTenantIOManager::mtl_new(tio_manager);
  ObTenantIOManager::mtl_init(tio_manager);
  tenant_base->set(tio_manager);
  tenant_base->unit_min_cpu_ = 100;
  tenant_base->unit_min_cpu_ = 100;
  oceanbase::share::ObTenantEnv::set_tenant(tenant_base);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
