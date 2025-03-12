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

class TestSSFdCache : public ::testing::Test
{
public:
  class TestReadThread : public Threads
  {
  public:
    TestReadThread(ObTenantBase *tenant_base,
                   const int64_t file_num_per_thread)
      : tenant_base_(tenant_base), file_num_per_thread_(file_num_per_thread)
    {}

    virtual void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);

      char buf[read_size_];
      buf[0] = '\0';

      ObStorageObjectReadInfo read_info;
      read_info.size_ = read_size_;
      read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
      read_info.io_desc_.set_wait_event(1);
      read_info.buf_ = buf;
      read_info.mtl_tenant_id_ = MTL_ID();

      ObLogicMicroBlockId logic_micro_id;
      logic_micro_id.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
      logic_micro_id.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
      logic_micro_id.logic_macro_id_.logic_version_ = 100;
      logic_micro_id.logic_macro_id_.tablet_id_ = tablet_id_;

      MacroBlockId macro_id;
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      macro_id.set_second_id(tablet_id_); // tablet_id
      macro_id.set_macro_transfer_seq(0); // transfer_seq
      macro_id.set_tenant_seq(server_id_); // server_id

      for (int64_t i = 0; i < read_times_; ++i) {
        read_info.offset_ = std::rand() % (file_size_ - read_size_);
        logic_micro_id.offset_ = read_info.offset_;
        macro_id.set_third_id(idx * file_num_per_thread_ + 1 + (i % file_num_per_thread_)); // seq_id
        read_info.logic_micro_id_ = logic_micro_id;
        read_info.macro_block_id_ = macro_id;

        ObSSPrivateMacroReader private_macro_reader;
        ObStorageObjectHandle object_handle;
        ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info, object_handle));
        ASSERT_EQ(OB_SUCCESS, object_handle.wait());
      }
    }

  private:
    ObTenantBase *tenant_base_;
    int64_t file_num_per_thread_;
  };

  TestSSFdCache() {}
  virtual ~TestSSFdCache() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void prepare();

public:
  static const uint64_t tablet_id_ = 200001;
  static const uint64_t server_id_ = 1;
  static const int64_t file_num_ = 1000; // 1k
  static const int64_t file_size_ = 2 * 1024 * 1024L; // 2MB
  static const int64_t read_times_ = 5000;
  static const int64_t read_size_ = 4096; // 4K
  static const int64_t thread_cnt_ = 100;
};

void TestSSFdCache::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSFdCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSFdCache::SetUp()
{
}

void TestSSFdCache::TearDown()
{
}

void TestSSFdCache::prepare()
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
    macro_id.set_third_id(i + 1); // seq_id
    macro_id.set_macro_transfer_seq(0); // transfer_seq
    macro_id.set_tenant_seq(server_id_); // server_id
    ASSERT_TRUE(macro_id.is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

    ObSSPrivateMacroWriter private_macro_writer;
    ASSERT_EQ(OB_SUCCESS, private_macro_writer.aio_write(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  }
}

TEST_F(TestSSFdCache, cost_time)
{
  // adjust tenant disk space to ensure private data macro write local. incremental space: 20GB * 0.4 = 8GB
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size));
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // prepare file
  prepare();

  const int64_t file_num_per_thread = file_num_ / thread_cnt_;
  TestSSFdCache::TestReadThread read_threads(ObTenantEnv::get_tenant(), file_num_per_thread);
  read_threads.set_thread_count(thread_cnt_);
  int64_t start_us = ObTimeUtility::current_time();
  read_threads.start();
  read_threads.wait();
  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  const int64_t per_read_cost_us = cost_us / read_times_;
  OB_LOG(INFO, "read cost", LITERAL_K_(file_num), LITERAL_K_(file_size), LITERAL_K_(thread_cnt),
         LITERAL_K_(read_times), LITERAL_K_(read_size), K(cost_us), K(per_read_cost_us));
  read_threads.destroy();
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_fd_cache_perf.log*");
  OB_LOGGER.set_file_name("test_ss_fd_cache_perf.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
