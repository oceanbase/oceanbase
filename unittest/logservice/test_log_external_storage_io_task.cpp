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
#include "logservice/ob_log_external_storage_io_task.h"
#undef protected
#undef private
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
    file_size = 0;
    return OB_SUCCESS;
  }

  int pread(const common::ObString &uri,
            const common::ObString &storage_info,
            const int64_t offset,
            char *buf,
            const int64_t read_buf_size,
            int64_t &real_read_size) override final
  {
    real_read_size = 0;
    return OB_SUCCESS;
  }
};

TEST(TestLogExternalStorageIOTaskCtx, test_io_ctx)
{
  CLOG_LOG_RET(INFO, OB_SUCCESS, "start test_io_ctx");
  ObLogExternalStorageIOTaskCtx io_ctx;
  EXPECT_EQ(OB_INVALID_ARGUMENT, io_ctx.init(-1));
  EXPECT_EQ(false, io_ctx.is_inited_);
  EXPECT_EQ(-1, io_ctx.flying_task_count_);
  EXPECT_EQ(-1, io_ctx.total_task_count_);
  EXPECT_EQ(nullptr, io_ctx.running_status_);
  const int64_t test_task_count = 16;
  EXPECT_EQ(OB_SUCCESS, io_ctx.init(test_task_count));
  EXPECT_EQ(test_task_count, io_ctx.flying_task_count_);
  EXPECT_EQ(test_task_count, io_ctx.total_task_count_);
  for (int i = 0; i < test_task_count; i++) {
    RunningStatus *ptr = NULL;
    EXPECT_EQ(OB_SUCCESS, io_ctx.get_running_status(i, ptr));
    EXPECT_NE(nullptr, ptr);
    EXPECT_EQ(-1, ptr->ret_);
    EXPECT_EQ(-1, ptr->thread_id_);
    EXPECT_EQ(i, ptr->logical_thread_id_);
    EXPECT_EQ(ptr->status_, EnumRunningStatus::INVALID_STATUS);
  }
  // 唤醒test_task_count - 1次，wait依旧报错OB_TIMEOUT
  for (int i = 0; i < test_task_count - 1; i++) {
    EXPECT_EQ(OB_TIMEOUT, io_ctx.wait(1000));
    io_ctx.signal();
    EXPECT_EQ(true, io_ctx.has_flying_async_task());
    CLOG_LOG(INFO, "io_ctx wait success", K(i), K(io_ctx));
  }
  // 唤醒test_task_count次后，wait返回成功
  EXPECT_EQ(OB_TIMEOUT, io_ctx.wait(1000));
  io_ctx.signal();
  EXPECT_EQ(OB_SUCCESS, io_ctx.wait(1000));
  EXPECT_EQ(false, io_ctx.has_flying_async_task());
  EXPECT_EQ(-1, io_ctx.get_ret_code());

  // 无唤醒，再次wait也能应为flying_task_count为0返回成功
  EXPECT_EQ(0, io_ctx.flying_task_count_);
  EXPECT_EQ(OB_SUCCESS, io_ctx.wait(1000));

  // 验证destroy后，内存状态被重置
  io_ctx.destroy();
  EXPECT_EQ(false, io_ctx.is_inited_);
  EXPECT_EQ(-1, io_ctx.flying_task_count_);
  EXPECT_EQ(-1, io_ctx.total_task_count_);
  EXPECT_EQ(NULL, io_ctx.running_status_);

  CLOG_LOG(INFO, "test_io_ctx success", K(io_ctx));
}

TEST(TestLogExternalStorageIOTask, test_pread_task)
{
  CLOG_LOG_RET(INFO, OB_SUCCESS, "start test_pread_task");
  ObLogExternalStorageIOTaskCtx io_ctx;
  const int64_t test_task_count = 16;
  EXPECT_EQ(OB_SUCCESS, io_ctx.init(16));
  ObString uri = "oss://runlin_test";
  ObString storage_info = "runlin_test";

  ObLogExternalStoragePreadTask *pread_task_array = NULL;
  pread_task_array = reinterpret_cast<ObLogExternalStoragePreadTask*>(
    ob_malloc(sizeof(ObLogExternalStoragePreadTask) * test_task_count, "unittest"));
  ASSERT_NE(nullptr, pread_task_array);

  char buff[test_task_count];
  int64_t real_read_size = 0;
  ObLogExternalStorageIOTaskHandleDummyAdapter adapter;
  for (int i = 0; i < test_task_count; i++) {
    RunningStatus *running_status = NULL;
    EXPECT_EQ(OB_SUCCESS, io_ctx.get_running_status(i, running_status));
    ObLogExternalStoragePreadTask *tmp_ptr =
      new(pread_task_array+i) ObLogExternalStoragePreadTask
      (uri, storage_info, running_status, &io_ctx, &adapter, i, 1, buff+i, real_read_size);
    ASSERT_NE(nullptr, tmp_ptr);
    EXPECT_EQ(ObLogExternalStorageIOTaskType::PREAD_TYPE, tmp_ptr->type_);
    EXPECT_EQ(OB_SUCCESS, tmp_ptr->do_task());
    if (i != test_task_count - 1) {
      EXPECT_EQ(OB_TIMEOUT, io_ctx.wait(1000));
    }
  }
  EXPECT_EQ(OB_SUCCESS, io_ctx.wait(1000));
  io_ctx.signal();
  EXPECT_EQ(0, io_ctx.flying_task_count_);
  EXPECT_EQ(OB_SUCCESS, io_ctx.wait(1000));
  CLOG_LOG(INFO, "test_pread_task success", K(io_ctx));
  ob_free(pread_task_array);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_log_external_storage_io_task.log*");
  OB_LOGGER.set_file_name("test_log_external_storage_io_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_external_storage_io_task");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
