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

#ifndef OCEANBASE_MITTEST_SHARED_STORAGE_PCACHED_EXTERNAL_FILE_SERVICE_H_
#define OCEANBASE_MITTEST_SHARED_STORAGE_PCACHED_EXTERNAL_FILE_SERVICE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "sql/engine/table/ob_external_file_access.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace storage
{

extern const bool is_shared_storage_mode_test;

template<typename KeyType_, typename ValueType_>
void print_map(const common::hash::ObHashMap<KeyType_, ValueType_> &map)
{
  for (auto it = map.begin(); it != map.end(); ++it) {
    OB_LOG(INFO, "map entry", K(it->first), K(it->second));
  }
}

class TestPCachedExternalFileService : public ::testing::Test
{
public:
  TestPCachedExternalFileService() : info_base_(), enable_test_(false) {}
  virtual ~TestPCachedExternalFileService() {}

  static void SetUpTestCase()
  {
    if (is_shared_storage_mode_test) {
      GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    }
    OK(MockTenantModuleEnv::get_instance().init());
    if (is_shared_storage_mode_test) {
      ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    }

    OB_EXTERNAL_FILE_DISK_SPACE_MGR.stop();
    OB_EXTERNAL_FILE_DISK_SPACE_MGR.wait();
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    // stop pipeline thread but start pipeline
    accesser->timer_task_scheduler_.stop();
    if (is_shared_storage_mode_test) {
      accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.is_stopped_ = false;
    } else {
      accesser->timer_task_scheduler_.sn_prefetch_timer_.pipeline_.is_stopped_ = false;
    }
  }
  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    MockTenantModuleEnv::get_instance().destroy();
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      OB_LOG(WARN, "failed to clean residual data", KR(ret));
    }
  }

  virtual void SetUp() override
  {
    const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    OB_LOG(INFO, "================= 测试用例开始 =================", K(test_info->test_case_name()), K(test_info->name()));

    if (ObObjectStorageUnittestCommon::need_skip_test(S3_SK)) {
      enable_test_ = false;
    } else {
      enable_test_ = true;
    }

    if (enable_test_) {
      OK(ObObjectStorageUnittestCommon::set_storage_info(
          S3_BUCKET, S3_ENDPOINT, S3_AK, S3_SK,
          nullptr/*appid_*/, S3_REGION, nullptr/*extension_*/,
          info_base_));

      ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
      ASSERT_NE(nullptr, accesser);
      // clean resources
      accesser->path_map_.path_id_map_.clear();
      accesser->path_map_.macro_id_map_.clear();
    }
  }
  virtual void TearDown() override
  {
    if (enable_test_) {
      OK(wait_prefetch_task_finish());
    }
    info_base_.reset();
    const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    OB_LOG(INFO, "================= 测试用例结束 =================", K(test_info->test_case_name()), K(test_info->name()));
  }

  int wait_prefetch_task_finish()
  {
    int ret = OB_SUCCESS;
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "accesser is null", KR(ret));
    } else {
      const int64_t FINISH_TIME_LIMIT_US = ObTimeUtility::current_time() + 5 * S_US;
      while (0 < accesser->running_prefetch_tasks_map_.size()
          && ObTimeUtility::current_time() < FINISH_TIME_LIMIT_US) {
        ob_usleep(10 * MS_US); // 10ms
        accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(0 < accesser->running_prefetch_tasks_map_.size())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "prefetch task not finished", KR(ret),
            K(accesser->timer_task_scheduler_), K(accesser->running_prefetch_tasks_map_.size()));
      }
      OB_LOG(INFO, "wait_prefetch_task_finish", K(accesser->timer_task_scheduler_));
    }
    return ret;
  }

  int generate_external_file(
      ObIAllocator &allocator,
      const int64_t content_size,
      char *external_file_name,
      const int64_t file_name_buf_size,
      char *&write_buf)
  {
    int ret = OB_SUCCESS;
    write_buf = nullptr;
    const int64_t ts = ObTimeUtility::current_time();
    const ::testing::TestInfo *test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    if (OB_ISNULL(external_file_name) || OB_ISNULL(test_info)
        || OB_UNLIKELY(content_size <= 0 || file_name_buf_size <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret), KP(test_info),
          KP(external_file_name), K(file_name_buf_size), K(content_size));
    } else if (OB_FAIL(databuff_printf(external_file_name, file_name_buf_size,
        "%s/test_pcached_external_file_service/%s_%ld", S3_BUCKET, test_info->name(), ts))) {
      OB_LOG(WARN, "failed to generate external file name", KR(ret), K(ts), K(test_info->name()),
          KP(external_file_name), K(file_name_buf_size), K(content_size));
    } else if (OB_FAIL(ObObjectStorageUnittestUtil::generate_random_data(
        allocator, content_size, write_buf))) {
      OB_LOG(WARN, "failed to generate random data", KR(ret), K(ts), K(test_info->name()),
          KP(external_file_name), K(file_name_buf_size), K(content_size));
    } else if (OB_FAIL(ObBackupIoAdapter::write_single_file(
        external_file_name, &info_base_, write_buf, content_size,
        ObStorageIdMod::get_default_external_id_mod()))) {
      OB_LOG(WARN, "failed to write single file", KR(ret), K(ts), K(test_info->name()),
          KP(external_file_name), K(file_name_buf_size), K(content_size));
    }
    return ret;
  }

  int init_external_info(
      ObIAllocator &allocator,
      const ObString &external_file_name,
      char *read_buf, const int64_t read_buf_size, const int64_t offset,
      ObExternalAccessFileInfo &external_file_info,
      ObExternalReadInfo &external_read_info)
  {
    int ret = OB_SUCCESS;
    external_file_info.reset_();
    external_read_info.reset();
    ObIOFd fd;
    ObIODevice *io_device = nullptr;
    int64_t file_length = -1;
    if (OB_ISNULL(read_buf) || OB_UNLIKELY(external_file_name.empty())
        || OB_UNLIKELY(read_buf_size <= 0 || offset < 0)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret),
          K(external_file_name), KP(read_buf), K(read_buf_size), K(offset));
    } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(
        io_device, fd, &info_base_, external_file_name,
        ObStorageAccessType::OB_STORAGE_ACCESS_READER, // for test
        ObStorageIdMod::get_default_external_id_mod()))) {
      OB_LOG(WARN, "failed to open with access type", KR(ret),
          K(external_file_name), K(read_buf_size), K(offset));
    } else if (OB_FAIL(ObBackupIoAdapter::get_file_size(io_device, fd, file_length))) {
      OB_LOG(WARN, "failed to get file length", KR(ret),
          K(external_file_name), K(read_buf_size), K(offset), K(info_base_));
    } else if (OB_FAIL(external_file_info.set_access_info(&info_base_, &allocator))) {
      OB_LOG(WARN, "failed to set access info", KR(ret),
          K(external_file_name), K(read_buf_size), K(offset), K(info_base_));
    } else if (OB_FAIL(external_file_info.set_basic_file_info(
        external_file_name, "", 1/*modify_time*/, 512, file_length, allocator))) {
      OB_LOG(WARN, "failed to set basic file info", KR(ret),
          K(external_file_name), K(read_buf_size), K(offset), K(info_base_));
    } else {
      external_file_info.device_handle_ = io_device;

      external_read_info.offset_ = offset;
      external_read_info.buffer_ = read_buf;
      external_read_info.size_ = read_buf_size;
      external_read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
      external_read_info.io_desc_.set_read();
      external_read_info.io_desc_.set_buffered_read();
      external_read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);

      if (OB_UNLIKELY(!external_read_info.is_valid() || !external_file_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "invalid external read info", KR(ret),
            K(external_file_name), K(read_buf_size), K(offset), K(info_base_),
            K(external_read_info), K(external_file_info));
      }
    }
    return ret;
  }

  int read_and_check(
      ObExternalAccessFileInfo &external_file_info,
      ObExternalReadInfo &external_read_info,
      const char *file_content, const int64_t expect_read_size)
  {
    int ret = OB_SUCCESS;
    ObPCachedExternalFileService *accesser = nullptr;
    ObStorageObjectHandle io_handle;
    if (OB_ISNULL(file_content) || OB_UNLIKELY(expect_read_size <= 0)
        || OB_UNLIKELY(!external_file_info.is_valid() || !external_read_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret),
          KP(file_content), K(expect_read_size), K(external_file_info), K(external_read_info));
    } else if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "accesser is null", KR(ret),
          K(expect_read_size), K(external_file_info), K(external_read_info));
    }
    OB_LOG(INFO, "read_and_check before", KR(ret), K(accesser->timer_task_scheduler_),
        K(external_file_info), K(external_read_info), K(expect_read_size));
    if (OB_NOT_NULL(accesser)) {
      print_map(accesser->path_map_.path_id_map_);
      print_map(accesser->path_map_.macro_id_map_);
    }

    if (FAILEDx(accesser->async_read(external_file_info, external_read_info, io_handle))) {
      OB_LOG(WARN, "failed to async read", KR(ret),
          K(expect_read_size), K(external_file_info), K(external_read_info));
    }
    OB_LOG(INFO, "read_and_check after async read", KR(ret), K(accesser->timer_task_scheduler_),
        K(external_file_info), K(external_read_info), K(expect_read_size));
    if (OB_NOT_NULL(accesser)) {
      print_map(accesser->path_map_.path_id_map_);
      print_map(accesser->path_map_.macro_id_map_);
    }

    if (FAILEDx(io_handle.wait())) {
      OB_LOG(WARN, "failed to wait for read completion", KR(ret),
          K(expect_read_size), K(external_file_info), K(external_read_info));
    } else if (OB_UNLIKELY(expect_read_size != io_handle.get_data_size())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected read size", KR(ret), K(expect_read_size), K(io_handle.get_data_size()),
          K(external_file_info), K(external_read_info), K(io_handle));
    } else if (OB_UNLIKELY(0 != MEMCMP(
        io_handle.get_buffer(), file_content + external_read_info.offset_, expect_read_size))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected read size", KR(ret), K(expect_read_size),
          K(external_file_info), K(external_read_info), K(io_handle));
    }
    OB_LOG(INFO, "read_and_check after read completion", KR(ret), K(accesser->timer_task_scheduler_),
        K(external_file_info), K(external_read_info), K(expect_read_size));
    if (OB_NOT_NULL(accesser)) {
      print_map(accesser->path_map_.path_id_map_);
      print_map(accesser->path_map_.macro_id_map_);
    }
    return ret;
  }

  ObBackupStorageInfo info_base_;
  bool enable_test_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_MITTEST_SHARED_STORAGE_PCACHED_EXTERNAL_FILE_SERVICE_H_ */
