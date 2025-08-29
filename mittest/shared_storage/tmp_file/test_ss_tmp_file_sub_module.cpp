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

#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/tmp_file/ob_ss_tmp_file_test_helper.h"
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "mittest/shared_storage/clean_residual_data.h"
#undef private
#undef protected

namespace oceanbase
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::tmp_file;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

/* ------------------------------ TestSSTmpFileSubModule ------------------------------ */

class TestSSTmpFileSubModule : public ::testing::Test
{
public:
  TestSSTmpFileSubModule() {}
  virtual ~TestSSTmpFileSubModule() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestSSTmpFileSubModule::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSTmpFileSubModule::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestSSTmpFileSubModule, test_async_remove_task)
{
  int ret = OB_SUCCESS;
  ObSSTmpFileRemoveManager &remove_mgr = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_;
  remove_mgr.stop();
  ATOMIC_SET(&remove_mgr.is_running_, true);
  // all data in wbp, remove with length_ = 0
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOHandle handle;
    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t write_buffer_size = 2 * 1024 * 1024;  // 2MB
    char * write_buffer = new char [write_buffer_size];
    MEMSET(write_buffer, 1, write_buffer_size);
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));

    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());

    ASSERT_EQ(write_buffer_size / ObTmpFileGlobal::PAGE_SIZE,
              ss_tmp_file->get_data_page_nums(false /* all pages */));
    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
    ASSERT_EQ(0, remove_mgr.get_task_num());
  }

  // all data in disk, remove with length_ = 2MB
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t write_buffer_size = 2 * 1024 * 1024;  // 2MB
    char * write_buffer = new char [write_buffer_size];
    MEMSET(write_buffer, 1, write_buffer_size);
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);

    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());

    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));
    tmp_file_handle.reset();

    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
    ASSERT_EQ(1, remove_mgr.get_task_num());
    ObTmpFileAsyncRemoveTask *remove_task;
    ASSERT_EQ(OB_SUCCESS, remove_mgr.remove_task_queue_.top(remove_task));
    ASSERT_EQ(fd, remove_task->tmp_file_id_.second_id());
    ASSERT_EQ(write_buffer_size, remove_task->length_);
    ASSERT_EQ(OB_SUCCESS, remove_mgr.exec_remove_task_once());
  }

  // data size 2MB, half in wbp, half in disk
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    // write 1MB
    const int64_t write_buffer_size = 1 * 1024 * 1024;  // 1MB
    char * write_buffer = new char [write_buffer_size];
    MEMSET(write_buffer, 1, write_buffer_size);
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);

    // flush 1MB
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());
    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));

    // write 1MB
    MEMSET(write_buffer, 2, write_buffer_size);
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);
    ASSERT_EQ(write_buffer_size / ObTmpFileGlobal::PAGE_SIZE,
              ss_tmp_file->get_data_page_nums(false /* all pages */));

    // remove file, half in memory, half in disk
    tmp_file_handle.reset();

    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
    ASSERT_EQ(1, remove_mgr.get_task_num());
    ObTmpFileAsyncRemoveTask *remove_task;
    ASSERT_EQ(OB_SUCCESS, remove_mgr.remove_task_queue_.top(remove_task));
    ASSERT_EQ(fd, remove_task->tmp_file_id_.second_id());
    ASSERT_EQ(write_buffer_size * 2, remove_task->length_);
    ASSERT_EQ(OB_SUCCESS, remove_mgr.exec_remove_task_once());
  }
  remove_mgr.start();
}

TEST_F(TestSSTmpFileSubModule, test_reboot_gc)
{
  int ret = OB_SUCCESS;

  const int64_t write_buffer_size = 1 * 1024 * 1024;  // 1MB
  char * write_buffer = new char [write_buffer_size];
  MEMSET(write_buffer, 1, write_buffer_size);

  int64_t before_init_fd = -1;
  int64_t after_init_fd = -1;

  // before mock init, open tmp file and flush
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    before_init_fd = fd;
    ASSERT_EQ(OB_SUCCESS, ret);

    // write 1MB
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);

    // flush 1MB
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());
    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));
  }

  // mock init
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.get_first_tmp_file_id_());
  ASSERT_NE(before_init_fd, -1);
  ASSERT_EQ(before_init_fd + 1, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.first_tmp_file_id_);

  // after mock init, open tmp file and flush
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    after_init_fd = fd;
    ASSERT_EQ(OB_SUCCESS, ret);

    // write 1MB
    io_info.fd_ = fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);

    // flush 1MB
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());
    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));
  }

  std::cout << "first_tmp_file_id: "
            << MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.first_tmp_file_id_
            << ", before_init_tmp_file_id: " << before_init_fd
            << ", after_init_tmp_file_id: " << after_init_fd << "\n";

  // mock reboot gc
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.exec_remove_task_once());

  char * read_buffer = new char [write_buffer_size];

  // read after init tmp file, should success
  {
    ObTmpFileIOInfo io_info;
    ObTmpFileIOHandle handle;
    MEMSET(read_buffer, 0, write_buffer_size);
    io_info.fd_ = after_init_fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = read_buffer;
    io_info.disable_page_cache_ = true;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle));
    ASSERT_EQ(0, MEMCMP(read_buffer, write_buffer, write_buffer_size));
  }

  // read before init tmp file, should fail
  {
    ObTmpFileIOInfo io_info;
    ObTmpFileIOHandle handle;
    MEMSET(read_buffer, 0, write_buffer_size);
    io_info.fd_ = before_init_fd;
    io_info.size_ = write_buffer_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = read_buffer;
    io_info.disable_page_cache_ = true;
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle));
  }

  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(before_init_fd));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(after_init_fd));
  sleep(1);
}

// ATTENTION!!!
// this ut will destroy some MTL modules. it must be the last test case
TEST_F(TestSSTmpFileSubModule, test_remove_in_destroy)
{
  int ret = OB_SUCCESS;

  const int64_t write_buffer_size = 3 * 1024 * 1024;  // 3MB
  const int64_t disk_size = write_buffer_size / 2;
  const int64_t mem_size = write_buffer_size - disk_size;
  char *write_buffer = new char [write_buffer_size];
  MEMSET(write_buffer, 1, write_buffer_size);

  int64_t fd = -1;
  // before mock remove, open tmp file and write 1.5MB disk data and 1.5MB mem data
  {
    int64_t dir = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);

    // write 1.5MB
    io_info.fd_ = fd;
    io_info.size_ = disk_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);

    // flush
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile * ss_tmp_file = tmp_file_handle.get();
    ASSERT_EQ(fd, tmp_file_handle.get()->get_fd());
    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, flush_size, wait_task_handle));
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));

    // write 1.5MB
    io_info.fd_ = fd;
    io_info.size_ = mem_size;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buffer;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
    sleep(1);
  }
  MTL(ObTenantTmpFileManager *)->stop();
  MTL(ObTenantFileManager *)->stop();
  MTL(ObTenantTmpFileManager *)->wait();
  MTL(ObTenantFileManager *)->wait();
  ATOMIC_SET(&(MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.is_running_), true);
  ASSERT_EQ(1, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().files_.count());
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.get_task_num());
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().files_.count());
  ASSERT_EQ(1, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().remove_mgr_.get_task_num());
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().destroy_sub_module_());
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_tmp_file_sub_module.log*");
  OB_LOGGER.set_file_name("test_ss_tmp_file_sub_module.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
