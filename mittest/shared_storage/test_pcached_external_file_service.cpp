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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

class TestExternalDataAccesser : public ::testing::Test
{
public:
  TestExternalDataAccesser() : enable_test_(false) {}
  virtual ~TestExternalDataAccesser() {}

  virtual void SetUp() override
  {
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
    }
  }
  virtual void TearDown() override
  {
    info_base_.reset();
  }

  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    OK(MockTenantModuleEnv::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
        LOG_WARN("failed to clean residual data", KR(ret));
    }
    MockTenantModuleEnv::get_instance().destroy();
  }

  ObObjectStorageInfo info_base_;
  bool enable_test_;
};

TEST_F(TestExternalDataAccesser, basic_test)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    const int64_t ts = ObTimeUtility::current_time();
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    ASSERT_EQ(OB_SUCCESS, databuff_printf(
        external_file_name, sizeof(external_file_name),
        "%s/test_external_data_accesser/basic_test_%ld",
        S3_BUCKET, ts));

    // generate external table file
    const int64_t content_size = 4 * 1024 * 1024L + 7;
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    OK(ObObjectStorageUnittestUtil::generate_random_data(write_buf, content_size));
    OK(ObBackupIoAdapter::write_single_file(
        external_file_name, &info_base_, write_buf, content_size,
        ObStorageIdMod::get_default_external_id_mod()));

    // read external table file
    const int64_t read_buf_size = 100;
    char read_buf[read_buf_size] = { 0 };
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    ObIOFd fd;
    ObIODevice *io_device = nullptr;
    OK(ObBackupIoAdapter::open_with_access_type(
        io_device, fd, &info_base_, external_file_name,
        ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
        ObStorageIdMod::get_default_external_id_mod()));
    ObExternalAccessFileInfo external_file_info;
    external_file_info.url_ = external_file_name;
    external_file_info.modify_time_ = 0;
    OK(external_file_info.access_info_->assign(info_base_));
    external_file_info.device_handle_ = io_device;
    ASSERT_TRUE(external_file_info.is_valid());

    ObIOFlag io_flag;
    io_flag.set_read();
    io_flag.set_buffered_read();
    io_flag.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    ObExternalReadInfo external_read_info(
        0/*offset*/, read_buf, read_buf_size,
        OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID()));
    external_read_info.io_desc_ = io_flag;
    ASSERT_TRUE(external_read_info.is_valid());

    ObStorageObjectHandle io_handle;
    OK(accesser->async_read(external_file_info, external_read_info, io_handle));
    // wait for read completion
    OK(io_handle.wait());
    // check read result
    ASSERT_EQ(read_buf_size, io_handle.get_data_size());
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf, read_buf_size));

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_external_data_accesser.log*");
  OB_LOGGER.set_file_name("test_external_data_accesser.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}