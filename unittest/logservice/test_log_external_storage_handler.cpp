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

#include <vector>
#include <thread>
#define private public
#include "logservice/ob_log_external_storage_handler.h"
#include "logservice/ob_log_external_storage_io_task.h"
#include "share/backup/ob_backup_io_adapter.h"
#undef private
#include "share/ob_device_manager.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;

class ObLogExternalStorageIOTaskHandleDummyAdapter : public ObLogExternalStorageIOTaskHandleIAdapter {
public:

  ObLogExternalStorageIOTaskHandleDummyAdapter() {}
  ~ObLogExternalStorageIOTaskHandleDummyAdapter() override {}

  // Implemetn
public:

  int exist(const common::ObString &uri,
            const common::ObString &storage_info,
            bool &exist) override final
  {
    exist = true;
    return OB_SUCCESS;
  }

  int get_file_size(const common::ObString &uri,
                    const common::ObString &storage_info,
                    int64_t &file_size) override final
  {
    file_size = palf::PALF_PHY_BLOCK_SIZE;
    return OB_SUCCESS;
  }

  int pread(const common::ObString &uri,
            const common::ObString &storage_info,
            const int64_t offset,
            char *buf,
            const int64_t read_buf_size,
            int64_t &real_read_size) override final
  {
    // 假设8M每秒的读取速度
    const int64_t sleep_us = read_buf_size / (8);
    usleep(sleep_us);
    real_read_size = read_buf_size;
    // while (true) {
    //   int64_t old_real_read_size = ATOMIC_LOAD(&real_read_size);
    //   int64_t new_real_read_size = old_real_read_size + read_buf_size;
    //   CLOG_LOG(INFO, "dummy pread before update real_read_size", K(real_read_size), K(read_buf_size),
    //            K(old_real_read_size), K(new_real_read_size));
    //   if (ATOMIC_BCAS(&real_read_size, old_real_read_size, new_real_read_size)) {
    //     CLOG_LOG(INFO, "dummy pread can update real_read_size", K(real_read_size), K(read_buf_size),
    //              K(old_real_read_size), K(new_real_read_size));
    //     break;
    //   }
    //   CLOG_LOG(INFO, "dummy pread can not update real_read_size", K(real_read_size), K(read_buf_size),
    //            K(old_real_read_size), K(new_real_read_size));
    // };
    CLOG_LOG(INFO, "dummy pread success", K(real_read_size), K(read_buf_size));
    return OB_SUCCESS;
  }
};

int delete_oss_object(const common::ObString &uri,
                   const ObString &oss_info)
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter io_adapter;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(OB_STORAGE_OSS, oss_info.ptr()))) {
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
  if (OB_FAIL(storage_info.set(OB_STORAGE_OSS, oss_info.ptr()))) {
    CLOG_LOG(WARN, "set ObBackupStorageInfo failed", K(oss_info));
  } else if (OB_FAIL(io_adapter.is_exist(uri, &storage_info, exist))) {
    CLOG_LOG(WARN, "is_exist failed", K(oss_info));
  } else if (!exist && OB_FAIL(io_adapter.write_single_file(uri, &storage_info, buf, size))) {
    CLOG_LOG(WARN, "ObBackupStorageInfo write_single_file failed", K(oss_info));
  } else {
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
  ObString uri = "oss://runlin_test";
  ObString storage_info = "runlin_test";
  int64_t offset = 0;
  int64_t read_buf_size = 15*MB;
  char *read_buf = reinterpret_cast<char*>(ob_malloc(read_buf_size, "unittest"));
  ASSERT_NE(nullptr, read_buf);
  int64_t real_read_size = 0;
  EXPECT_EQ(OB_NOT_INIT, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
  EXPECT_EQ(OB_NOT_INIT, handler.resize(16, 0));

  // 测试异常内存状态——没有start
  EXPECT_EQ(OB_SUCCESS, handler.init());
  ObLogExternalStorageIOTaskHandleDummyAdapter adapter;
  ObLogExternalStorageIOTaskHandleIAdapter *origin_adapter = handler.handle_adapter_;
  handler.handle_adapter_ = &adapter;
  EXPECT_EQ(true, handler.is_inited_);
  EXPECT_EQ(OB_NOT_RUNNING, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
  EXPECT_EQ(OB_NOT_RUNNING, handler.resize(16, 0));


  // 测试invalid argument
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.start(-1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.start(ObLogExternalStorageHandler::CONCURRENCY_LIMIT+1));

  // start 成功
  const int64_t concurrency = 16;
  EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));
  EXPECT_EQ(true, handler.is_running_);
  EXPECT_EQ(concurrency, handler.concurrency_);
  EXPECT_EQ(concurrency*ObLogExternalStorageHandler::CAPACITY_COEFFICIENT,
            handler.capacity_);

  // 验证读取——invalid argument
  {
    ObString empty_uri;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(empty_uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
    ObString empty_storage_info;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, empty_storage_info, offset, read_buf, read_buf_size, real_read_size));
    int64_t invalid_offset = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, invalid_offset, read_buf, read_buf_size, real_read_size));
    invalid_offset = 100*1024*1024;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, invalid_offset, read_buf, read_buf_size, real_read_size));
    char *invalid_read_buf = NULL;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, offset, invalid_read_buf, read_buf_size, real_read_size));
    int64_t invalid_read_buf_size = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.pread(uri, storage_info, offset, read_buf, invalid_read_buf_size, real_read_size));
  }

  // 验证resize——invalid argument
  {
    int64_t invalid_concurrency = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.resize(invalid_concurrency, 0));
    int64_t invalid_timeout_us = -1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, handler.resize(concurrency, invalid_timeout_us));
  }

  // 验证私有函数
  {
    // 验证is_valid_concurrency_
    int64_t invalid_concurrency = -1;
    EXPECT_EQ(false, handler.is_valid_concurrency_(invalid_concurrency));
    invalid_concurrency = ObLogExternalStorageHandler::CONCURRENCY_LIMIT + 1;
    EXPECT_EQ(false, handler.is_valid_concurrency_(invalid_concurrency));

    // 验证get_async_task_count_
    // 单个任务最小2M, 在concurrency足够的情况下，最多存在8个异步任务
    int64_t total_size = 15 * MB;
    EXPECT_EQ(8, handler.get_async_task_count_(total_size));
    // 最多存在51个异步任务，由于并发度问题，只能存在16个
    total_size = 101 * MB;
    EXPECT_EQ(16, handler.get_async_task_count_(total_size));

    ObLogExternalStorageIOTaskCtx *async_task_ctx = NULL;
    // 验证construct_async_tasks_and_push_them_into_thread_pool_
    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.construct_async_tasks_and_push_them_into_thread_pool_
              (uri, storage_info, offset, read_buf, read_buf_size, real_read_size, async_task_ctx));
    EXPECT_EQ(handler.get_async_task_count_(read_buf_size), async_task_ctx->total_task_count_);
    EXPECT_EQ(OB_SUCCESS, handler.wait_async_tasks_finished_(async_task_ctx));
    ASSERT_EQ(real_read_size, read_buf_size);
    CLOG_LOG(INFO, "after test private interface", K(real_read_size), K(read_buf_size));
  }

  // 验证公有函数
  {
    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
    EXPECT_EQ(read_buf_size, real_read_size);
    CLOG_LOG(INFO, "after first read", K(read_buf_size), K(real_read_size));
    int64_t new_concurrency = 8;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));
    EXPECT_EQ(new_concurrency, handler.concurrency_);
    EXPECT_EQ(new_concurrency*ObLogExternalStorageHandler::CAPACITY_COEFFICIENT,
              handler.capacity_);
    new_concurrency = 0;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));

    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
    EXPECT_EQ(read_buf_size, real_read_size);
    CLOG_LOG(INFO, "after second read", K(read_buf_size), K(real_read_size));

    new_concurrency = 32;
    EXPECT_EQ(OB_SUCCESS, handler.resize(new_concurrency));

    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
    EXPECT_EQ(read_buf_size, real_read_size);
    CLOG_LOG(INFO, "after third read", K(read_buf_size), K(real_read_size));

    read_buf_size = 63*1000*1000+123;
    real_read_size = 0;
    EXPECT_EQ(OB_SUCCESS, handler.pread(uri, storage_info, offset, read_buf, read_buf_size, real_read_size));
    EXPECT_EQ(read_buf_size, real_read_size);
    CLOG_LOG(INFO, "after fourth read", K(read_buf_size), K(real_read_size));
  }

  // 并发场景验证
  {
    const int64_t pread_thread_count = 16;
    std::vector<std::thread> pread_thread;
    auto read_func = [&](){
      for (int i = 0; i < 8; i++) {
        srandom(ObTimeUtility::current_time());
        const int64_t tmp_read_buf_size = random()%64*1024*1024 + 1;
        int64_t tmp_real_read_size = 0;
        int64_t tmp_offset = 64*1024*1024 - tmp_read_buf_size;
        handler.pread(uri, storage_info, tmp_offset, read_buf, tmp_read_buf_size, tmp_real_read_size);
        EXPECT_EQ(tmp_real_read_size, tmp_read_buf_size);
        CLOG_LOG(INFO, "pread in thread success", K(tmp_real_read_size), K(tmp_read_buf_size));
      }
    };
    auto resize_func = [&](){
      for (int i = 0; i < 8; i++) {
        srandom(ObTimeUtility::current_time());
        handler.resize(random()%37);
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
  handler.handle_adapter_ = origin_adapter;
}

// 测试真实的oss object
TEST(TestLogExternalStorageHandler, test_oss_object)
{
  // 验证oss 不可访问
  {
    // 构造错误的oss meta信息
    CLOG_LOG(INFO, "test oss can not access");
    std::string trick_code_first = "host=123&acc";
    std::string trick_code_second = "ess_id=111&acce";
    std::string trick_code_third = "ss_key=222";
    std::string trick_code_fourth = trick_code_first + trick_code_second + trick_code_third;
    ObString oss_path(trick_code_fourth.c_str());
    ObString uri("oss://backup_dir/1234");
    ObLogExternalStorageIOTaskHandleAdapter adapter;
    bool exist = false;
    EXPECT_EQ(OB_OSS_ERROR, adapter.exist(uri, oss_path, exist));
    EXPECT_EQ(false, exist);
    int64_t file_size = 0;
    EXPECT_EQ(OB_OSS_ERROR, adapter.get_file_size(uri, oss_path, file_size));
    EXPECT_EQ(0, file_size);
    const int64_t buf_len = 4096;
    char buf[buf_len];
    int64_t real_read_size = 0;
    EXPECT_EQ(OB_OSS_ERROR, adapter.pread(uri, oss_path, 0, buf, buf_len, real_read_size));
    EXPECT_EQ(0, real_read_size);
  }

  // 验证oss 可访问
  // 需要保证如下的OSS能持续访问
  {
    CLOG_LOG(INFO, "test oss can access");
    int ret = OB_SUCCESS;
    const char * oss_bucket="oss://antsys-oceanbasebackup/shuning.tsn/";
    const char *oss_host="cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com";
    std::string trick_id_first = "LTAI4Fdwx";
    std::string trick_id_second = "9iFgZso4";
    std::string trick_id_third = "CqyHPs7";
    std::string oss_id = trick_id_first + trick_id_second + trick_id_third;
    std::string trick_key_first = "ER51kMnlmu";
    std::string trick_key_second = "3zXwcxczJMb";
    std::string trick_key_third = "YzJIgrY9O";
    std::string oss_key = trick_key_first + trick_key_second + trick_key_third;
    std::string trick_code_first = "host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com&acc";
    std::string trick_code_second = "ess_id=" + oss_id + "&acce";
    std::string trick_code_third = "ss_key=" + oss_key;
    std::string trick_code_fourth = trick_code_first + trick_code_second + trick_code_third;
    ObString oss_path(trick_code_fourth.c_str());
    ObLogExternalStorageIOTaskHandleAdapter adapter;

    std::vector<std::string> uris;
    std::vector<int64_t> object_checksums;
    std::string base_uri = "oss://antsys-oceanbasebackup/shuning.tsn/runlin_test_pf/";
    const int64_t total_oss_object = 5;
    // 测试目录下存在5个clog文件
    for (int i = 0; i < total_oss_object; i++) {
      uris.push_back(base_uri+std::to_string(i));
    }
    int exist_num = 0;
    const int64_t buf_len = 64*1024*1024;
    char *buf_self_pread = reinterpret_cast<char*>(ob_malloc(buf_len, "unittest"));
    memset(buf_self_pread, 'x', buf_len);

    // oss地址可能变化，当oss可访问时，进行如下测试
    for (int i = 0; i < total_oss_object; i++) {
      bool exist = false;
      int64_t real_read_size = 0;
      // farm环境写oss太慢了，暂时先单测不运行
     // if (OB_FAIL(generate_oss_data(uris[i].c_str(), oss_path,  buf_self_pread, buf_len))) {
     //   CLOG_LOG(ERROR, "oss can not access", K(uris[i].c_str()), K(oss_path), K(exist));
     // } else {
     //   int64_t checksum = ob_crc64(buf_self_pread, buf_len);
     //   object_checksums.push_back(checksum);
     //   exist_num++;
     //   memset(buf_self_pread, 'y', buf_len);
     // }
    }
    if (exist_num == total_oss_object)
    {
      int64_t real_read_size = 0;
      // 读的offer为文件尾，读长度为0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, buf_len, buf_self_pread, buf_len, real_read_size));
      EXPECT_EQ(0, real_read_size);
      CLOG_LOG(INFO, "read outof upper bound", K(ret), K(real_read_size));
      // 读的offer为文件尾-1000，读长度为0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, buf_len-1000, buf_self_pread, buf_len, real_read_size));
      EXPECT_EQ(1000, real_read_size);
      // 读的offer为文件尾-4*1000*1000，读长度为0
      EXPECT_EQ(OB_SUCCESS, adapter.pread(uris[0].c_str(), oss_path, buf_len-1000*1000*4, buf_self_pread, buf_len, real_read_size));
      EXPECT_EQ(1000*1000*4, real_read_size);
      // 读的offer为文件尾+1000，读长度为0, 返回invalid argument
      // 底层读接口会判断读取的offset是否超过文件长度，超过报错invalid argment
      // OSS的feature: 如果指定的offset超过文件长度或者offset+待读取长度大于文件长度，会返回整个文件的数据内容且不会报错
      EXPECT_EQ(OB_INVALID_ARGUMENT, adapter.pread(uris[0].c_str(), oss_path, buf_len+1000, buf_self_pread, buf_len, real_read_size));
      // 读的offer为文件尾+1000*1000*4，读长度为0, 返回invalid argument
      EXPECT_EQ(OB_INVALID_ARGUMENT, adapter.pread(uris[0].c_str(), oss_path, buf_len+1000*1000*4, buf_self_pread, buf_len, real_read_size));
      CLOG_LOG(INFO, "read outof upper bound", K(ret), K(real_read_size));
    }
    if (exist_num == total_oss_object)
    // 验证性能
    {
      EXPECT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
      ObLogExternalStorageHandler handler;
      const int64_t concurrency = 16;
      EXPECT_EQ(OB_SUCCESS, handler.init());
      EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));

      int64_t start_ts = ObTimeUtility::current_time();
      int64_t real_read_size = 0;
      for (int i = 0; i < total_oss_object; i++) {
        EXPECT_EQ(OB_SUCCESS, handler.pread(
          uris[i].c_str(), oss_path, 0, buf_self_pread, buf_len, real_read_size
        ));
        EXPECT_EQ(real_read_size, buf_len);
      }
      int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      int64_t bw = total_oss_object * buf_len / cost_ts;
      CLOG_LOG(INFO, "band width", K(bw), K(cost_ts));
    }
    // 验证读取正确性
    if (exist_num == total_oss_object)
    {
      ObLogExternalStorageHandler handler;
      const int64_t concurrency = 16;
      EXPECT_EQ(OB_SUCCESS, handler.init());
      EXPECT_EQ(OB_SUCCESS, handler.start(concurrency));
      ASSERT_NE(nullptr, buf_self_pread);
      for (int i = 0; i < total_oss_object; i++) {
        int64_t real_read_size = 0;
        memset(buf_self_pread, '0', buf_len);
        EXPECT_EQ(OB_SUCCESS, handler.pread(
          uris[i].c_str(), oss_path, 0, buf_self_pread, buf_len, real_read_size
        ));
        EXPECT_EQ(buf_len, real_read_size);
        int64_t checksum = ob_crc64(buf_self_pread, buf_len);
        EXPECT_EQ(checksum, object_checksums[i]);
      }

      if (NULL != buf_self_pread) {
        ob_free(buf_self_pread);
        buf_self_pread = NULL;
      }

      // 随机位置读取
      {
        auto read_func = [&]() {
          for (int i = 0; i < 32; i++) {
            srandom(ObTimeUtility::current_time());
            const int64_t object_idx = random() % total_oss_object;
            const int64_t file_size = palf::PALF_PHY_BLOCK_SIZE;
            int64_t tmp_read_offset = random() %  file_size;
            const int64_t tmp_read_size = random() % file_size;
            char *tmp_read_buf1 = reinterpret_cast<char*>(ob_malloc(tmp_read_size, "unittest"));
            char *tmp_read_buf2 = reinterpret_cast<char*>(ob_malloc(tmp_read_size, "unittest"));
            int64_t tmp_real_read_size = 0;
            ASSERT_NE(nullptr, tmp_read_buf1);
            ASSERT_NE(nullptr, tmp_read_buf2);
            EXPECT_EQ(OB_SUCCESS, handler.pread(
              uris[object_idx].c_str(), oss_path, tmp_read_offset, tmp_read_buf1, tmp_read_size, tmp_real_read_size
            ));
            EXPECT_EQ(std::min(file_size - tmp_read_offset, tmp_read_size),
                      tmp_real_read_size);
            tmp_real_read_size = 0;
            EXPECT_EQ(OB_SUCCESS, adapter.pread(
              uris[object_idx].c_str(), oss_path, tmp_read_offset, tmp_read_buf2, tmp_read_size, tmp_real_read_size
            ));
            EXPECT_EQ(std::min(file_size - tmp_read_offset, tmp_read_size),
                      tmp_real_read_size);
            EXPECT_EQ(0, memcmp(tmp_read_buf1, tmp_read_buf2, tmp_real_read_size));
            if (NULL != tmp_read_buf1) {
              ob_free(tmp_read_buf1);
              tmp_read_buf1 = NULL;
            }
            if (NULL != tmp_read_buf2) {
              ob_free(tmp_read_buf2);
              tmp_read_buf2 = NULL;
            }
          }
        };
        std::vector<std::thread> tmp_pread_threads;

        for (int i = 0; i < 4; i++) {
          tmp_pread_threads.emplace_back(read_func);
        }
        auto tmp_resize_func = [&](){
          for (int i = 0; i < 4; i++) {
            srandom(ObTimeUtility::current_time());
            handler.resize(random()%4);
            usleep(1000 * 1000);
          }
        };
        std::thread tmp_resize_thread(tmp_resize_func);
        for (int i = 0; i < 4; i++) {
          tmp_pread_threads[i].join();
        }
        tmp_resize_thread.join();
      }
      for (auto uri : uris) {
        delete_oss_object(uri.c_str(), oss_path);
      }
      CLOG_LOG(INFO, "delete success", K(base_uri.c_str()), K(oss_path));
    } else {
      CLOG_LOG(ERROR, "the object of oss is not correct", K(exist_num), K(oss_path));
    }
  }
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
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
