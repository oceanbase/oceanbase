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
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestOpenClose : public ::testing::Test
{
public:
  class TestReadThread : public Threads
  {
  public:
    TestReadThread(ObTenantBase *tenant_base,
                   const int64_t file_num_per_thread,
                   const bool get_fd_from_cache)
      : tenant_base_(tenant_base), file_num_per_thread_(file_num_per_thread),
        get_fd_from_cache_(get_fd_from_cache)
    {}

    virtual void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);

      char buf[file_size_];
      buf[0] = '\0';

      ObIOInfo io_info;
      io_info.tenant_id_ = MTL_ID();
      io_info.offset_ = 0;
      io_info.size_ = file_size_;
      io_info.flag_.set_wait_event(1);
      io_info.timeout_us_ = 5 * 1000 * 1000L; // 5s
      io_info.user_data_buf_ = buf;
      io_info.flag_.set_read();

      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      macro_id.set_second_id(tablet_id_); // tablet_id
      macro_id.set_third_id(server_id_);  //tenant_seq
      for (int64_t i = 0; i < read_times_; ++i) {
        macro_id.set_macro_transfer_seq(0); // transfer_seq
        macro_id.set_tenant_seq(idx * file_num_per_thread_ + 1 + (i % file_num_per_thread_));  //seq_id
        get_read_device_and_fd(get_fd_from_cache_, fd_cache_, macro_id, io_info);
        ObIOHandle io_handle;
        ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().aio_read(io_info, io_handle));
        ASSERT_EQ(OB_SUCCESS, io_handle.wait());
      }
    }

  private:
    ObTenantBase *tenant_base_;
    int64_t file_num_per_thread_;
    bool get_fd_from_cache_;
  };

  TestOpenClose() {}
  virtual ~TestOpenClose() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void prepare();
  static void prepare_fd_cache();
  static void get_read_device_and_fd(const bool get_fd_from_cache,
                                     const common::hash::ObHashMap<MacroBlockId, int> &fd_cache,
                                     const MacroBlockId &macro_id,
                                     ObIOInfo &io_info);

public:
  static const uint64_t tablet_id_ = 200001;
  static const uint64_t server_id_ = 1;
  static const int64_t file_num_ = 1000; // 1k
  static const int64_t file_size_ = 4096;
  static const int64_t read_times_ = 1000;
  static const int64_t thread_cnt_ = 100;
  static common::hash::ObHashMap<MacroBlockId, int> fd_cache_;
};

common::hash::ObHashMap<MacroBlockId, int> TestOpenClose::fd_cache_;

void TestOpenClose::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestOpenClose::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestOpenClose::SetUp()
{
}

void TestOpenClose::TearDown()
{
}

void TestOpenClose::prepare()
{
  char write_buf[file_size_];
  MEMSET(write_buf, 0, file_size_);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = file_size_;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  for (int64_t i = 0; i < file_num_; ++i) {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id_, 0/*transfer_seq*/));
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
    macro_id.set_second_id(tablet_id_); // tablet_id
    macro_id.set_third_id(server_id_); // server_id
    macro_id.set_macro_transfer_seq(0); // transfer_seq
    macro_id.set_tenant_seq(i + 1);  //seq_id
    ASSERT_TRUE(macro_id.is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

    ObSSPrivateMacroWriter private_macro_writer;
    ASSERT_EQ(OB_SUCCESS, private_macro_writer.aio_write(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  }
}

void TestOpenClose::prepare_fd_cache()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, fd_cache_.create(100000, ObModIds::OB_HASH_BUCKET));
  ObBaseFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id_); // tablet_id
  macro_id.set_third_id(server_id_); // server_id
  for (int64_t i = 0; i < file_num_; ++i) {
    macro_id.set_macro_transfer_seq(0); // transfer_seq
    macro_id.set_tenant_seq(i + 1);  //seq_id
    int fd = INVALID_FD;
    ObPathContext ctx;
    ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(macro_id, 0/*ls_epoch_id*/, true/*is_local_cache*/));
    const int open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
    RETRY_ON_EINTR(fd, ::open(ctx.get_path(), open_flag, ObSSIOCommonOp::SS_FILE_OPEN_MODE));
    ASSERT_LE(0, fd);
    ASSERT_EQ(OB_SUCCESS, fd_cache_.set_refactored(macro_id, fd));
  }
}

void TestOpenClose::get_read_device_and_fd(
    const bool get_fd_from_cache,
    const common::hash::ObHashMap<MacroBlockId, int> &fd_cache,
    const MacroBlockId &macro_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(macro_id.is_valid());
  int fd = INVALID_FD;
  ObBaseFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(macro_id, 0/*ls_epoch_id*/, true/*is_local_cache*/));
  const int open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
  if (get_fd_from_cache) {
    ASSERT_EQ(OB_SUCCESS, fd_cache.get_refactored(macro_id, fd));
  } else {
    RETRY_ON_EINTR(fd, ::open(ctx.get_path(), open_flag, ObSSIOCommonOp::SS_FILE_OPEN_MODE));
  }
  ASSERT_LE(0, fd);
  io_info.fd_.first_id_ = macro_id.first_id(); // first_id is not used in shared storage mode
  io_info.fd_.second_id_ = fd;
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
  if (get_fd_from_cache) {
    // do nothing. do not close cached fd
  } else {
    io_info.flag_.set_need_close_dev_and_fd(); // close not cached fd
  }
}

TEST_F(TestOpenClose, cost_time)
{
  // 1. prepare file
  prepare();

  const int64_t file_num_per_thread = file_num_ / thread_cnt_;

  // 2. read with open/close
  TestOpenClose::TestReadThread read_with_open_threads(ObTenantEnv::get_tenant(),
                                                       file_num_per_thread,
                                                       false/*get_fd_from_cache*/);
  read_with_open_threads.set_thread_count(thread_cnt_);
  int64_t start_us = ObTimeUtility::current_time();
  read_with_open_threads.start();
  read_with_open_threads.wait();
  const int64_t read_with_open_cost_us = ObTimeUtility::current_time() - start_us;
  read_with_open_threads.destroy();

  // 3. prepare fd cache, and read without open/close
  prepare_fd_cache();
  TestOpenClose::TestReadThread read_without_open_threads(ObTenantEnv::get_tenant(),
                                                          file_num_per_thread,
                                                          true/*get_fd_from_cache*/);
  read_without_open_threads.set_thread_count(thread_cnt_);
  start_us = ObTimeUtility::current_time();
  read_without_open_threads.start();
  read_without_open_threads.wait();
  const int64_t read_without_open_cost_us = ObTimeUtility::current_time() - start_us;
  OB_LOG(INFO, "read cost", K(read_with_open_cost_us), K(read_without_open_cost_us),
         "diff_us", read_with_open_cost_us - read_without_open_cost_us);
  read_without_open_threads.destroy();
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_open_close.log*");
  OB_LOGGER.set_file_name("test_open_close.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
