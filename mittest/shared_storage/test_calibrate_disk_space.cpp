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
#include "storage/tmp_file/ob_tmp_file_manager.h"
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

class TestCalibrateDisk : public ::testing::Test
{
public:
  TestCalibrateDisk() {}
  virtual ~TestCalibrateDisk() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  static void prepare(const int64_t segment_id_pos);
  static void write_dir();
public:
  class TestThread : public Threads
  {
  public:
    TestThread(ObTenantBase *tenant_base)
      : tenant_base_(tenant_base), is_gc_tmp_file_(false), is_delete_dir_(false)
    {}

    virtual void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);
      ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
      ASSERT_NE(nullptr, tenant_file_mgr);
      if (idx % 2 == 0) {
        if (is_delete_dir_) {
          const int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
          ob_usleep(1L * 1000L * 1000L); //1s
          LOG_INFO("start to calibrate tmp file alloc disk size");
          ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, tenant_file_mgr->calibrate_disk_space_task_.calibrate_alloc_disk_size(
                    ObStorageObjectType::TMP_FILE, start_calc_size_time_s, 0, 0));
          LOG_INFO("finish to calibrate tmp file alloc disk size");
        } else {
          if (is_gc_tmp_file_) {
            //because delete remote files need 9ms, delete local files need 5s
            ob_usleep(3L * 1000L * 1000L); //3s
          } else {
            // because write 10w files need 10s
            ob_usleep(5L * 1000L * 1000L); //5s
          }
          LOG_INFO("start to calibrate tmp file");
          int64_t start_us = ObTimeUtility::current_time();
          ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
          const int64_t cost_us = ObTimeUtility::current_time() - start_us;
          LOG_INFO("finish to calibrate tmp file", K(cost_us));
        }
      } else {
        if (is_delete_dir_) {
          LOG_INFO("start to delete tmp file dir");
          int64_t start_us = ObTimeUtility::current_time();
          for (int64_t i = dir_num_; i >= 0; --i) {
            MacroBlockId tmp_file;
            tmp_file.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
            tmp_file.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
            tmp_file.set_second_id(i);
            ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_local_tmp_file(tmp_file));
          }
          const int64_t cost_us = ObTimeUtility::current_time() - start_us;
          LOG_INFO("finish to delete tmp file dir", K(cost_us));
        } else {
          if (is_gc_tmp_file_) {
            LOG_INFO("start to delete tmp file");
            int64_t start_us = ObTimeUtility::current_time();
            const int64_t DELETE_FILE_TIMEOUT = 10L * 1000L * 1000L;
            MacroBlockId tmp_file;
            tmp_file.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
            tmp_file.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
            tmp_file.set_second_id(tmp_file_id_); // tmp_file_id
            ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(tmp_file));
            const int64_t cost_us = ObTimeUtility::current_time() - start_us;
            LOG_INFO("finish to delete tmp file", K(cost_us));
          } else {
            LOG_INFO("start to write tmp file");
            int64_t start_us = ObTimeUtility::current_time();
            const int64_t segment_id_pos = file_num_;
            TestCalibrateDisk::prepare(segment_id_pos);
            const int64_t cost_us = ObTimeUtility::current_time() - start_us;
            LOG_INFO("finish to write tmp file", K(cost_us));
          }
        }
      }
    }
    void set_is_gc_tmp_file(const bool is_gc_tmp_file) { is_gc_tmp_file_ = is_gc_tmp_file; }
    void set_is_delete_dir(const bool is_delete_dir) { is_delete_dir_ = is_delete_dir; }

  private:
    ObTenantBase *tenant_base_;
    bool is_gc_tmp_file_;
    bool is_delete_dir_;
  };

public:
  static const uint64_t tmp_file_id_ = 1001;
  static const int64_t file_num_ = 100000; // 10w
  static const int64_t dir_num_ = 30000; // 3w
  static const int64_t file_size_ = 8 * 1024L; // 8KB
  static const int64_t thread_cnt_ = 2;
};

void TestCalibrateDisk::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
}

void TestCalibrateDisk::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCalibrateDisk::SetUp()
{
}

void TestCalibrateDisk::TearDown()
{
}

void TestCalibrateDisk::prepare(const int64_t segment_id_pos)
{
  char write_buf[file_size_];
  MEMSET(write_buf, 0, file_size_);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = file_size_;
  write_info.tmp_file_valid_length_ = file_size_;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  for (int64_t i = segment_id_pos; i < segment_id_pos + file_num_; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_id.set_second_id(tmp_file_id_); // tmp_file_id
    macro_id.set_third_id(i);             // segment_file_id
    ASSERT_TRUE(macro_id.is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
    ASSERT_NE(nullptr, tenant_file_mgr);
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->append_file(write_info, write_object_handle));
  }
}

void TestCalibrateDisk::write_dir()
{
  char write_buf[file_size_];
  MEMSET(write_buf, 0, file_size_);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = file_size_;
  write_info.tmp_file_valid_length_ = file_size_;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  for (int64_t i = 0; i < dir_num_; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_id.set_second_id(i); // tmp_file_id
    macro_id.set_third_id(0);  // segment_file_id
    ASSERT_TRUE(macro_id.is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
    ASSERT_NE(nullptr, tenant_file_mgr);
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->append_file(write_info, write_object_handle));
  }
}

TEST_F(TestCalibrateDisk, calibrate_and_write_file)
{
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size));
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // prepare file
  const int64_t segment_id_pos = 0;
  TestCalibrateDisk::prepare(segment_id_pos);
  // test write files and calibrate
  TestCalibrateDisk::TestThread calibrate_write_threads(ObTenantEnv::get_tenant());
  calibrate_write_threads.set_is_gc_tmp_file(false);
  calibrate_write_threads.set_thread_count(thread_cnt_);
  calibrate_write_threads.start();
  calibrate_write_threads.wait();
  calibrate_write_threads.destroy();
  int64_t tmp_file_write_cache_alloc_size = tenant_disk_space_mgr->get_tmp_file_write_cache_alloc_size();
  const int64_t delta_alloc_size = std::abs(tmp_file_write_cache_alloc_size - TestCalibrateDisk::file_num_ * 2 * TestCalibrateDisk::file_size_);
  // because statbuf.mtime_s_ is second level, maybe at start_calc_time_s has written some files
  // ASSERT_EQ(TestCalibrateDisk::file_num_ * 2 * TestCalibrateDisk::file_size_, tmp_file_write_cache_alloc_size);
  ASSERT_TRUE(delta_alloc_size < tmp_file_write_cache_alloc_size * 0.05);
}

TEST_F(TestCalibrateDisk, calibrate_and_delete_file)
{
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size));
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // need calibrate disk space before calibrate_and_delete_file test,
  // because calibrate_and_write_file statbuf.mtime_s_ is second level, maybe at start_calc_time_s has written some files,
  // lead to calibrate disk size is inaccurate
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  int64_t tmp_file_write_cache_alloc_size = tenant_disk_space_mgr->get_tmp_file_write_cache_alloc_size();
  int64_t expected_disk_size = TestCalibrateDisk::file_num_ * 2 * TestCalibrateDisk::file_size_;
  ObIODFileStat statbuf;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tmp_file_id_));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  LOG_INFO("local tmp file dir size", K(statbuf.size_), K(expected_disk_size), K(tmp_file_write_cache_alloc_size));
  ASSERT_EQ(expected_disk_size, tmp_file_write_cache_alloc_size);

  // test gc files and calibrate
  TestCalibrateDisk::TestThread calibrate_gc_threads(ObTenantEnv::get_tenant());
  calibrate_gc_threads.set_is_gc_tmp_file(true);
  calibrate_gc_threads.set_thread_count(thread_cnt_);
  calibrate_gc_threads.start();
  calibrate_gc_threads.wait();
  calibrate_gc_threads.destroy();
  // because xfs dir size will become smaller as sub_files are deleted, so need calibrate disk size, otherwise tmp_file_write_cache_alloc_size is not 0, maybe dir size
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
  tmp_file_write_cache_alloc_size = tenant_disk_space_mgr->get_tmp_file_write_cache_alloc_size();
  ASSERT_EQ(0, tmp_file_write_cache_alloc_size);
}

TEST_F(TestCalibrateDisk, calibrate_and_delete_dir)
{
  ObTenantDiskSpaceManager* tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  int64_t total_disk_size = 20L * 1024L * 1024L * 1024L; // 20GB
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->resize_total_disk_size(total_disk_size));
  ASSERT_EQ(total_disk_size, tenant_disk_space_mgr->get_total_disk_size());

  // prepare file
  TestCalibrateDisk::write_dir();
  // test write files and calibrate
  TestCalibrateDisk::TestThread calibrate_delete_threads(ObTenantEnv::get_tenant());
  calibrate_delete_threads.set_is_delete_dir(true);
  calibrate_delete_threads.set_thread_count(thread_cnt_);
  calibrate_delete_threads.start();
  calibrate_delete_threads.wait();
  calibrate_delete_threads.destroy();
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_calibrate_disk_space.log*");
  OB_LOGGER.set_file_name("test_calibrate_disk_space.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
