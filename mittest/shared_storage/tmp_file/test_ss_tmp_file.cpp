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

#include "mittest/mtlenv/storage/tmp_file/ob_tmp_file_test_helper.h"
#include "mittest/mtlenv/storage/tmp_file/mock_ob_tmp_file_util.h"
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;
/* ------------------------------ Mock Parameter ---------------------------- */
static const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L /* 16 GB */;
static constexpr int64_t IO_WAIT_TIME_MS = 60 * 1000L; // 60s
/********************************* Mock WBP *************************** */
static const int64_t WBP_BLOCK_SIZE = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; // each wbp block has 253 pages (253 * 8KB == 2024KB)
static const int64_t SMALL_WBP_BLOCK_COUNT = 3;
static const int64_t SMALL_WBP_MEM_LIMIT = SMALL_WBP_BLOCK_COUNT * WBP_BLOCK_SIZE; // the wbp mem size is 5.93MB
static const int64_t BIG_WBP_BLOCK_COUNT = 40;
static const int64_t BIG_WBP_MEM_LIMIT = BIG_WBP_BLOCK_COUNT * WBP_BLOCK_SIZE; // the wbp mem size is 79.06MB
/********************************* Mock WBP Index Cache*************************** */
// each bucket could indicate a 256KB data in wbp.
// SMALL_WBP_IDX_CACHE_MAX_CAPACITY will indicate 4MB data in wbp
static const int64_t SMALL_WBP_IDX_CACHE_MAX_CAPACITY = ObTmpFileWBPIndexCache::INIT_BUCKET_ARRAY_CAPACITY * 2;
/* ---------------------------- Unittest Class ----------------------------- */

class TestTmpFile : public ::testing::Test
{
public:
  TestTmpFile() = default;
  virtual ~TestTmpFile() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
private:
  void check_final_status();
};

static ObSimpleMemLimitGetter getter;

void TestTmpFile::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);

  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
}

void TestTmpFile::SetUp()
{
  int ret = OB_SUCCESS;

  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  if (!ObKVGlobalCache::get_instance().inited_) {
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(&getter,
        bucket_num,
        max_cache_size,
        block_size));
  }
  GCONF._data_storage_io_timeout = IO_WAIT_TIME_MS * 1000;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_EQ(true, tenant_config.is_valid());
  tenant_config->_object_storage_io_timeout = IO_WAIT_TIME_MS * 1000;
}

void TestTmpFile::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTmpFile::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

void TestTmpFile::check_final_status()
{
  ASSERT_LE(MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_, SMALL_WBP_MEM_LIMIT);
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.print_stat_info();
  bool is_over = false;
  const int64_t max_wait_cnt = 50;
  int cnt = 0;
  while (!is_over) {
    is_over = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.wait_task_queue_.queue_length_ == 0;
    if (!is_over) {
      if (cnt++ > max_wait_cnt) {
        is_over = true;
      } else {
        usleep(1000 * 1000); // 1s
      }
    }
  }
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.print_stat_info();
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.flush_prio_mgr_.get_file_size());
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.wait_task_queue_.queue_length_);
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.f1_cnt_);
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.f2_cnt_);
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.f3_cnt_);
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_.total_flushing_page_num_);
}

// generate 2MB random data (will not trigger flush and evict logic)
// 1. test write pages and append write tail page
// 2. test write after reading
TEST_F(TestTmpFile, test_unaligned_data_read_write)
{
  STORAGE_LOG(INFO, "=======================test_unaligned_data_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 2 * 1024 * 1024;
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.get_memory_limit();
  ASSERT_LT(write_size, wbp_mem_limit);
  char * write_buffer = new char[write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buffer[i + j] = random_int;
    }
    i += random_length;
  }
  int64_t dir = -1;
  int64_t fd = -1;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;
  file_handle.reset();
  // dump random data
  {
    std::string r_file_name = std::to_string(fd) + "_raw_write_data.txt";
    dump_hex_data(write_buffer, write_size, r_file_name);
  }

  // random write, read, and check
  int64_t already_write = 0;
  std::vector<int64_t> turn_write_size = generate_random_sequence(1, write_size / 3, write_size, 3);
  for (int i = 0; i < turn_write_size.size(); ++i) {
    int64_t this_turn_write_size = turn_write_size[i];
    std::cout << "random write and read " << this_turn_write_size << std::endl;
    // write data
    {
      ObTmpFileIOInfo io_info;
      io_info.fd_ = fd;
      io_info.io_desc_.set_wait_event(2);
      io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
      io_info.buf_ = write_buffer + already_write;
      if (this_turn_write_size % ObTmpFileGlobal::PAGE_SIZE == 0 && i == 0) {
        io_info.size_ = this_turn_write_size - 2 * 1024;
        ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));

        io_info.size_ = 2 * 1024;
        io_info.buf_ = write_buffer + already_write + this_turn_write_size - 2 * 1024;
        ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
      } else {
        io_info.size_ = this_turn_write_size;
        ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info));
      }
    }
    // read data
    char * read_check_buffer = new char[this_turn_write_size];
    {
      ObTmpFileIOInfo io_info;
      ObTmpFileIOHandle handle;
      io_info.fd_ = fd;
      io_info.size_ = this_turn_write_size;
      io_info.io_desc_.set_wait_event(2);
      io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
      io_info.buf_ = read_check_buffer;
      ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle));
    }
    // check data
    {
      std::string compare_file_name = std::to_string(fd) + "_compare_result.txt";
      bool is_equal = compare_and_print_hex_data(
          write_buffer + already_write, read_check_buffer,
          this_turn_write_size, 200, compare_file_name);
      if (!is_equal) {
        // dump write data
        std::string w_file_name = std::to_string(fd) + "_write_data.txt";
        dump_hex_data(write_buffer + already_write, this_turn_write_size, w_file_name);
        // dump read check data
        std::string r_file_name = std::to_string(fd) + "_read_data.txt";
        dump_hex_data(read_check_buffer, this_turn_write_size, r_file_name);
        // abort
        std::cout << "not equal in random data test"
                  << "\nwrite dumped file: " << w_file_name
                  << "\nread check dumped file: " << r_file_name
                  << "\ncompare result file: " << compare_file_name << std::endl;
        ob_abort();
      }
    }
    // update already_write
    delete [] read_check_buffer;
    already_write += this_turn_write_size;
  }

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  STORAGE_LOG(INFO, "=======================test_unaligned_data_read_write end=======================");
}

// generate 7MB random data
// this test will trigger flush and evict logic for data pages.
// meta tree will not be evicted in this test.
// 1. test pread
// 1.1 read disk data
// 1.2 read memory data
// 1.3 read both disk and memory data
// 1.4 read OB_ITER_END
// 2. test read
// 2.1 read aligned data
// 2.2 read unaligned data
TEST_F(TestTmpFile, test_read)
{
  STORAGE_LOG(INFO, "=======================test_read begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 7 * 1024 * 1024; // 7MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.get_memory_limit();
  ASSERT_GT(write_size, wbp_mem_limit);
  char *write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
  // Write data
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_GT(wbp_begin_offset, 0);
  file_handle.reset();

  int64_t read_time = ObTimeUtility::current_time();
  /************** test pread **************/
  // 1. read memory data
  char *read_buf = new char [write_size - wbp_begin_offset];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = write_size - wbp_begin_offset;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, wbp_begin_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + wbp_begin_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 2. read disk data
  read_buf = new char [wbp_begin_offset];
  io_info.buf_ = read_buf;
  io_info.size_ = wbp_begin_offset;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  read_buf = new char [wbp_begin_offset];
  io_info.buf_ = read_buf;
  io_info.size_ = wbp_begin_offset;
  io_info.disable_block_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  read_buf = new char [wbp_begin_offset];
  io_info.buf_ = read_buf;
  io_info.size_ = wbp_begin_offset;
  io_info.disable_block_cache_ = false;
  io_info.disable_page_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3. read both disk and memory data
  int64_t read_size = wbp_begin_offset / 2 + 9 * 1024;
  int64_t read_offset = wbp_begin_offset / 2 + 1024;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  read_size = wbp_begin_offset / 2 + 9 * 1024;
  read_offset = wbp_begin_offset / 2 + 1024;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_block_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 4. read OB_ITER_END
  read_buf = new char [200];
  io_info.buf_ = read_buf;
  io_info.size_ = 200;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, write_size - 100, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(100, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + write_size - 100, 100);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  /************** test read **************/
  // 1. read aligned data
  read_buf = new char [3 * ObTmpFileGlobal::PAGE_SIZE];
  io_info.buf_ = read_buf;
  io_info.size_ = 3 * ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  // 2. read unaligned data
  read_buf = new char [ObTmpFileGlobal::PAGE_SIZE];
  io_info.buf_ = read_buf;
  io_info.size_ = 100;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 3 * ObTmpFileGlobal::PAGE_SIZE, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();

  io_info.buf_ = read_buf + 100;
  io_info.size_ = ObTmpFileGlobal::PAGE_SIZE - 100;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + 3 * ObTmpFileGlobal::PAGE_SIZE + 100, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  read_time = ObTimeUtility::current_time() - read_time;

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  LOG_INFO("io time", K(write_time), K(read_time));
  STORAGE_LOG(INFO, "=======================test_read end=======================");
}

// generate 27206784 random data
// this test will check whether kv_cache caches correct pages in disk
TEST_F(TestTmpFile, test_cached_read)
{
  STORAGE_LOG(INFO, "=======================test_cached_read begin=======================");
  int ret = OB_SUCCESS;
  // 27206784: 24MB + 249 * 8KB + 1152B (12 * 256 + 250 pages)
  const int64_t write_size = 24 * 1024 * 1024 + 249 * 8 * 1024 + 1152;
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.get_memory_limit();
  ASSERT_GT(write_size, wbp_mem_limit);
  char *write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;

  // 1. Write data and wait flushing over
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  // sleep(2);

  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_GT(wbp_begin_offset, 0);
  ASSERT_EQ(wbp_begin_offset % ObTmpFileGlobal::PAGE_SIZE, 0);

  // 2. check rightness of reading
  int64_t read_size = write_size;
  int64_t read_offset = 0;
  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3. read disk data and puts them into kv_cache
  int64_t read_time = ObTimeUtility::current_time();
  read_size = wbp_begin_offset - ObTmpFileGlobal::PAGE_SIZE;
  read_offset = ObTmpFileGlobal::PAGE_SIZE / 2;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = false;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 4. read disk data to check whether kv_cache caches correct pages
  read_size = wbp_begin_offset;
  read_offset = 0;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = false;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  read_time = ObTimeUtility::current_time() - read_time;

  // 5. check pages in kv_cache
  int64_t begin_block_id = 0;
  int64_t end_block_id = common::upper_align(wbp_begin_offset, ObTmpFileGlobal::SS_BLOCK_SIZE) / ObTmpFileGlobal::SS_BLOCK_SIZE - 1 ;
  for (int64_t seg_id = begin_block_id; seg_id <= end_block_id && OB_SUCC(ret); ++seg_id) {
    const int64_t end_page_id = MIN((seg_id + 1) * ObTmpFileGlobal::SS_BLOCK_SIZE,
                                    common::upper_align(wbp_begin_offset, ObTmpFileGlobal::PAGE_SIZE)) / ObTmpFileGlobal::PAGE_SIZE
                                - seg_id * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS;
    for (int64_t page_id = 0; page_id < end_page_id && OB_SUCC(ret); page_id++) {
      const int64_t virtual_page_id = seg_id * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS + page_id;
      tmp_file::ObTmpPageCacheKey key(fd, 0, virtual_page_id, MTL_ID());
      tmp_file::ObTmpPageValueHandle handle;
      ret = tmp_file::ObTmpPageCache::get_instance().get_page(key, handle);
      if (OB_FAIL(ret)) {
        std::cout << "get cached page failed\n"
                  << "wbp_begin_offset: " << wbp_begin_offset << " "
                  << "block_id: " << seg_id << " "
                  << "page_id: " << page_id << " "
                  << "end_page_id: " << end_page_id << " "
                  << "begin_block_id: " << begin_block_id << " "
                  << "end_block_id: " << end_block_id << " "
                  << std::endl;
        ob_abort();
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      cmp = memcmp(handle.value_->get_buffer(), write_buf + virtual_page_id * ObTmpFileGlobal::PAGE_SIZE, ObTmpFileGlobal::PAGE_SIZE);
      ASSERT_EQ(0, cmp);
    }
  }

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  LOG_INFO("io time", K(write_time), K(read_time));
  STORAGE_LOG(INFO, "=======================test_cached_read end=======================");
}

TEST_F(TestTmpFile, test_prefetch_read)
{
  STORAGE_LOG(INFO, "=======================test_prefetch_read begin=======================");
  int ret = OB_SUCCESS;
  ObTmpWriteBufferPool &wbp = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_;
  // const int64_t write_size = 8 * 1024 * 1024; // 8MB
  const int64_t write_size = 9 * 1024 * 1024 + 37 * 1024; // 8MB
  const int64_t wbp_mem_limit = wbp.get_memory_limit();
  wbp.default_wbp_memory_limit_ = 10 * WBP_BLOCK_SIZE;
  char *write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

  // 1. Write data and wait flushing over
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  int64_t wash_size = 0;
  ObSSTmpFileFlushManager &flush_mgr = MTL(ObTenantTmpFileManager *)->ss_file_manager_.flush_mgr_;
  ObSSTmpFileAsyncFlushWaitTaskHandle task_handle;
  common::ObIOFlag io_desc;
  io_desc.set_wait_event(ObWaitEventIds::TMP_FILE_WRITE);
  ASSERT_EQ(OB_SUCCESS, flush_mgr.wash(INT64_MAX, io_desc, task_handle, wash_size));
  task_handle.wait(30 * 1000);
  while (file_handle.get()->cached_page_nums_ > 0) {
    task_handle.reset();
    ASSERT_EQ(OB_SUCCESS, flush_mgr.wash(INT64_MAX, io_desc, task_handle, wash_size)); // flush twice to make sure all pages are flushed
    task_handle.wait(30 * 1000);
  }
  EXPECT_EQ(file_handle.get()->cached_page_nums_, 0);
  LOG_INFO("wash all page complete");

  // 2. enable prefetch and read data from disk
  ObTmpFileIOHandle handle;
  int64_t read_size = 0;
  int64_t read_offset = 0;
  char *read_buf = nullptr;
  const int64_t BLOCK_NUM = upper_align(write_size, ObTmpFileGlobal::SN_BLOCK_SIZE) / ObTmpFileGlobal::SN_BLOCK_SIZE;
  for (int i = 0; i < BLOCK_NUM + 1; ++i) {
    if (i != BLOCK_NUM) {
      read_size = ObTmpFileGlobal::PAGE_SIZE / 2;
      read_offset = i * ObTmpFileGlobal::SN_BLOCK_SIZE;
    } else {
      read_size = 10;
      read_offset = write_size - 20;
    }
    LOG_INFO("test_prefetch_read pread", K(i), K(read_offset));
    read_buf = new char [read_size];
    io_info.buf_ = read_buf;
    io_info.size_ = read_size;
    io_info.disable_page_cache_ = false;
    io_info.disable_block_cache_ = true;
    io_info.prefetch_ = true;
    ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(io_info.size_, handle.get_done_size());
    int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
    ASSERT_EQ(0, cmp);
    handle.reset();
    delete[] read_buf;
  }

  // 3. check ALL pages are present in the kv_cache
  int64_t begin_block_id = 0;
  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  int64_t end_block_id = common::upper_align(wbp_begin_offset, ObTmpFileGlobal::SS_BLOCK_SIZE) / ObTmpFileGlobal::SS_BLOCK_SIZE - 1 ;
  for (int64_t seg_id = begin_block_id; seg_id <= end_block_id && OB_SUCC(ret); ++seg_id) {
    const int64_t end_page_id = MIN((seg_id + 1) * ObTmpFileGlobal::SS_BLOCK_SIZE,
                                    common::upper_align(wbp_begin_offset, ObTmpFileGlobal::PAGE_SIZE)) / ObTmpFileGlobal::PAGE_SIZE
                                - seg_id * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS;
    for (int64_t page_id = 0; page_id < end_page_id && OB_SUCC(ret); page_id++) {
      const int64_t virtual_page_id = seg_id * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS + page_id;
      tmp_file::ObTmpPageCacheKey key(fd, 0, virtual_page_id, MTL_ID());
      if (virtual_page_id == write_size / ObTmpFileGlobal::PAGE_SIZE) {
        key.unfilled_page_length_ = write_size % ObTmpFileGlobal::PAGE_SIZE;
      }
      tmp_file::ObTmpPageValueHandle handle;
      ret = tmp_file::ObTmpPageCache::get_instance().get_page(key, handle);
      if (OB_FAIL(ret)) {
        std::cout << "get cached page failed\n"
                  << "wbp_begin_offset: " << wbp_begin_offset << " "
                  << "block_id: " << seg_id << " "
                  << "page_id: " << page_id << " "
                  << "end_page_id: " << end_page_id << " "
                  << "begin_block_id: " << begin_block_id << " "
                  << "end_block_id: " << end_block_id << " "
                  << std::endl;
        ob_abort();
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      int64_t cmp_size = MIN(ObTmpFileGlobal::PAGE_SIZE, ObTmpFileGlobal::PAGE_SIZE - key.unfilled_page_length_);
      int cmp = memcmp(handle.value_->get_buffer(), write_buf + virtual_page_id * ObTmpFileGlobal::PAGE_SIZE, cmp_size);
      ASSERT_EQ(0, cmp);
    }
  }
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  STORAGE_LOG(INFO, "=======================test_prefetch_read end=======================");
}

// 1. append write a uncompleted tail page in memory
// 2. append write a uncompleted tail page in disk
TEST_F(TestTmpFile, test_write_tail_page)
{
  STORAGE_LOG(INFO, "=======================test_write_tail_page begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 10 * 1024; // 10KB
  int64_t already_write_size = 0;
  char *write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObSSTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  // 1. write 2KB data and check rightness of writing
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 2 * 1024; // 2KB
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  int64_t read_size = 2 * 1024; // 2KB
  int64_t read_offset = 0;
  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 2. append write 2KB data in memory and check rightness of writing
  io_info.buf_ = write_buf + 2 * 1024; // 2KB
  io_info.size_ = 2 * 1024; // 2KB
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  read_size = 4 * 1024; // 4KB
  read_offset = 0;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3. forcibly evict current page
  int64_t flush_size = 0;
  ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
  ObSharedStorageTmpFile *ss_tmp_file = file_handle.get();
  ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
  ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, io_info.io_desc_, ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS, flush_size, wait_task_handle));
  ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
  wait_task_handle.reset();
  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));
  ASSERT_EQ(wbp_begin_offset, already_write_size);

  // 4. read disk page and add it into kv_cache
  read_offset = 5;
  read_size = already_write_size - 2 * read_offset; // 4KB - 10B
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 5. append write 6KB data in memory and check rightness of writing
  io_info.buf_ = write_buf + already_write_size;
  io_info.size_ = write_size - already_write_size; // 6KB
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  read_size = write_size;
  read_offset = 0;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 6. forcibly evict all pages and read them from disk to check whether hit old cached page in kv_cache
  ASSERT_EQ(OB_SUCCESS, set_ss_tmp_file_flushing(*ss_tmp_file));
  ASSERT_EQ(OB_SUCCESS, ss_tmp_file->flush(true, io_info.io_desc_, ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS, flush_size, wait_task_handle));
  ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
  wait_task_handle.reset();
  wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_EQ(0, ss_tmp_file->get_data_page_nums(false /* all pages */));
  ASSERT_EQ(wbp_begin_offset, already_write_size);

  read_offset = 20;
  read_size = write_size - read_offset; // 10KB - 20B
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  STORAGE_LOG(INFO, "=======================test_write_tail_page end=======================");
}

// 1. truncate special cases
// 2. truncate disk data (truncate_offset < wbp begin offset)
// 3. truncate memory data and disk data (wbp begin offset < truncate_offset < file_size_)
// 4. truncate() do nothing (truncate_offset < file's truncate_offset_)
// 5. invalid truncate_offset checking
TEST_F(TestTmpFile, test_tmp_file_truncate)
{
  STORAGE_LOG(INFO, "=======================test_tmp_file_truncate begin=======================");
  int ret = OB_SUCCESS;
  const int64_t data_size = 30 * 1024 * 1024; // 30MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.get_memory_limit();
  ASSERT_GT(data_size, wbp_mem_limit);
  char *write_buf = new char [data_size];
  int64_t already_write_size = 0;
  for (int64_t i = 0; i < data_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < data_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  // 1. truncate special cases
  // 1.1 truncate a file with several pages
  // 1.1.1 write two pages and check rightness of writing
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  io_info.buf_ = write_buf;
  io_info.size_ = 2 * ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  int64_t read_offset = 0;
  int64_t read_size = already_write_size;
  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.disable_block_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  MEMSET(read_buf, 0, read_size);

  // 1.1.2 truncate to the middle offset of the first page
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 2);
  uint32_t begin_page_id = file_handle.get()->begin_page_id_;
  uint32_t end_page_id = file_handle.get()->end_page_id_;
  int64_t truncate_offset = ObTmpFileGlobal::PAGE_SIZE / 2;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(file_handle.get()->begin_page_id_, begin_page_id);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  MEMSET(read_buf, 0, read_size);

  // 1.1.3 truncate the first page
  truncate_offset = ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(file_handle.get()->begin_page_id_, end_page_id);
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 1);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  MEMSET(read_buf, 0, read_size);

  // 1.1.4 truncate whole pages
  truncate_offset = already_write_size;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(file_handle.get()->begin_page_id_, ObTmpFileGlobal::INVALID_PAGE_ID);
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 0);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 1.2 truncate a offset of a page whose page index is not in index cache (to mock the sparsify case of index cache)
  // 1.2.1 write three pages and check rightness of writing
  read_offset = already_write_size;
  io_info.buf_ = write_buf + already_write_size;
  io_info.size_ = 3 * ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  read_size = io_info.size_;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  delete[] read_buf;

  // 1.2.2 pop the first page index of index cache of file
  ASSERT_NE(file_handle.get()->page_idx_cache_.page_buckets_, nullptr);
  ASSERT_EQ(file_handle.get()->page_idx_cache_.size(), 1);
  ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket *bucket = file_handle.get()->page_idx_cache_.page_buckets_->at(0);
  ASSERT_NE(bucket, nullptr);
  ASSERT_EQ(bucket->size(), 3);
  begin_page_id = file_handle.get()->begin_page_id_;
  end_page_id = file_handle.get()->end_page_id_;
  ASSERT_EQ(bucket->page_indexes_.at(bucket->left_), begin_page_id);
  ASSERT_EQ(bucket->page_indexes_.at(bucket->right_), end_page_id);
  ret = bucket->pop_();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(bucket->size(), 2);
  ASSERT_NE(bucket->page_indexes_.at(bucket->left_), begin_page_id);
  ASSERT_EQ(bucket->page_indexes_.at(bucket->right_), end_page_id);

  // 1.2.3 truncate the first page
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 3);
  truncate_offset = read_offset + ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 2);

  read_size = already_write_size - read_offset;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 2. truncate disk data (truncate_offset < wbp begin offset)
  read_offset = already_write_size;
  io_info.buf_ = write_buf + already_write_size;
  io_info.size_ = data_size - already_write_size;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_GT(wbp_begin_offset, 0);

  truncate_offset = wbp_begin_offset/2;
  read_size = wbp_begin_offset - read_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3. truncate memory data (truncate_offset < file_size_)
  // 3.1 truncate_offset is unaligned
  read_offset = truncate_offset;
  truncate_offset = (wbp_begin_offset + data_size) / 2 - ObTmpFileGlobal::PAGE_SIZE / 2;
  read_size = data_size - read_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  read_offset = truncate_offset;
  truncate_offset = upper_align(truncate_offset, ObTmpFileGlobal::PAGE_SIZE) + ObTmpFileGlobal::PAGE_SIZE;
  read_size = data_size - read_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3.2 truncate_offset is aligned
  ASSERT_EQ(truncate_offset % ObTmpFileGlobal::PAGE_SIZE, 0);
  read_offset = truncate_offset;
  truncate_offset = truncate_offset + 5 * ObTmpFileGlobal::PAGE_SIZE;
  read_size = data_size - read_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  read_offset = truncate_offset;
  truncate_offset = data_size;
  read_size = data_size - read_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  MEMSET(write_buf, 0, truncate_offset);
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 4. truncate() do nothing (truncate_offset < file's truncate_offset_)
  int64_t old_truncate_offset = truncate_offset;
  ASSERT_EQ(old_truncate_offset, file_handle.get()->truncated_offset_);
  truncate_offset = wbp_begin_offset;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(old_truncate_offset, file_handle.get()->truncated_offset_);

  // 5. invalid truncate_offset checking
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, -1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, data_size + 10);
  ASSERT_NE(OB_SUCCESS, ret);

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_tmp_file_truncate end=======================");
}

// this test will trigger flush and evict logic for both data and meta pages.
void test_big_file(const int64_t write_size, const int64_t wbp_mem_limit, ObTmpFileIOInfo io_info)
{
  int ret = OB_SUCCESS;
  ASSERT_GT(write_size, wbp_mem_limit);
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_ = wbp_mem_limit;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  int cmp = 0;
  char *write_buf = (char *)malloc(write_size);
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << " tenant_id:"<< MTL_ID() << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;

  // 1. write data
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  write_time = ObTimeUtility::current_time() - write_time;

  // 2. read data
  ObTmpFileIOHandle handle;
  int64_t read_size = write_size;
  char *read_buf = new char [read_size];
  int64_t read_time = ObTimeUtility::current_time();
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  cmp = memcmp(handle.get_buffer(), write_buf, handle.get_done_size());
  ASSERT_EQ(read_size, handle.get_done_size());
  handle.reset();
  ASSERT_EQ(0, cmp);
  memset(read_buf, 0, read_size);

  // 3. attempt to read data when reach the end of file
  io_info.size_ = 10;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  handle.reset();

  // 4. pread 2MB
  int64_t read_offset = 100;
  read_size = macro_block_size;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, handle.get_done_size());
  handle.reset();
  ASSERT_EQ(0, cmp);
  memset(read_buf, 0, read_size);

  // 5. attempt to read data when reach the end of file (after pread)
  io_info.size_ = 10;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_ITER_END, ret);
  handle.reset();

  // 6. pread data which has been read to use kv_cache
  const int64_t begin_block_id = upper_align(file_handle.get()->cal_wbp_begin_offset(), ObTmpFileGlobal::SS_BLOCK_SIZE)
                                 / ObTmpFileGlobal::SS_BLOCK_SIZE / 4;
  const int64_t end_block_id = upper_align(file_handle.get()->cal_wbp_begin_offset(), ObTmpFileGlobal::SS_BLOCK_SIZE)
                               / ObTmpFileGlobal::SS_BLOCK_SIZE / 4 * 3;
  const int block_num = end_block_id - begin_block_id;
  LOG_INFO("start to pread disk data", K(begin_block_id), K(end_block_id), K(block_num),
                                       K(file_handle.get()->file_size_), K(file_handle.get()->cal_wbp_begin_offset()));
  for (int i = 0; i < block_num; ++i) {
    read_offset = MIN(macro_block_size * (begin_block_id + i), file_handle.get()->file_size_-1);
    read_size = MIN(macro_block_size * 2, file_handle.get()->file_size_ - read_offset);
    io_info.size_ = read_size;
    ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(read_size, handle.get_done_size());
    // cmp = memcmp(handle.get_buffer(), write_buf + read_offset, handle.get_done_size());
    // handle.reset();
    // ASSERT_EQ(0, cmp);
    std::string filename = "check_result";
    bool is_equal = compare_and_print_hex_data(write_buf + read_offset, handle.get_buffer(), read_size, 300, filename);
    if (!is_equal) {
      LOG_INFO("different data", K(i), K(read_offset), K(read_size));
      std::string file_name = "write_buffer";
      dump_hex_data(write_buf + read_offset, read_size, file_name);
      file_name = "read_buffer";
      dump_hex_data(handle.get_buffer(), read_size, file_name);
      ob_abort();
    }
    memset(read_buf, 0, read_size);
  }
  read_time = ObTimeUtility::current_time() - read_time;

  free(write_buf);
  free(read_buf);

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  STORAGE_LOG(INFO, "test_big_file", K(io_info.disable_page_cache_), K(io_info.disable_block_cache_));
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
}

TEST_F(TestTmpFile, test_big_file_with_small_wbp)
{
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp begin=======================");
  const int64_t write_size = 150 * 1024 * 1024;  // write 150MB data
  const int64_t wbp_mem_limit = SMALL_WBP_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = true;
  io_info.disable_block_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp end=======================");
}

TEST_F(TestTmpFile, test_big_file_with_small_wbp_disable_page_cache)
{
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp_disable_page_cache begin=======================");
  const int64_t write_size = 150 * 1024 * 1024;  // write 150MB data
  const int64_t wbp_mem_limit = SMALL_WBP_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = false;
  io_info.disable_block_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp_disable_page_cache end=======================");
}

// generate 16MB random data for four files. (total 64MB)
// 1. the first three files write and read 1020KB data (will not trigger flushing)
// 2. the 4th file writes and reads 3MB+1020KB data (will trigger flushing in the processing of writing)
// 3. the first three files write and read 1MB data 3 times (total 3MB)
// 4. each file read and write 12MB+4KB data
void test_multi_file_single_thread_read_write(bool disable_block_cache)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = 64 * 1024 * 1024; // 64MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.get_memory_limit();
  ASSERT_GT(buf_size, wbp_mem_limit);
  char *random_buf = new char [buf_size];
  for (int64_t i = 0; i < buf_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < buf_size; ++j) {
      random_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir1 = -1;
  int64_t dir2 = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir2);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t file_num = 4;
  char *write_bufs[file_num] = {nullptr};
  int64_t already_write_sizes[file_num] = {0};
  int64_t fds[file_num] = {-1};
  for (int i = 0; i < file_num; ++i) {
    int64_t dir = i % 2 == 0 ? dir1 : dir2;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    fds[i] = fd;
    write_bufs[i] = random_buf + i * buf_size / file_num;
    tmp_file::ObITmpFileHandle file_handle;
    ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;
    file_handle.reset();
  }
  ObTmpFileIOInfo io_info;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
  io_info.disable_block_cache_ = disable_block_cache;
  ObTmpFileIOHandle handle;
  int cmp = 0;

  // 1. the first three files write and read 1020KB data (will not trigger flushing)
  int64_t write_size = 1020;
  io_info.size_ = write_size;
  for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
    io_info.fd_ = fds[i];
    io_info.buf_ = write_bufs[i] + already_write_sizes[i];
    ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  char *read_buf = new char [write_size];
  io_info.buf_ = read_buf;
  io_info.size_ = write_size;
  for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
    io_info.fd_ = fds[i];
    ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(io_info.size_, handle.get_done_size());
    cmp = memcmp(handle.get_buffer(), write_bufs[i] + already_write_sizes[i], io_info.size_);
    ASSERT_EQ(0, cmp);
    handle.reset();
    memset(read_buf, 0, write_size);
  }
  delete[] read_buf;

  for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
    already_write_sizes[i] += write_size;
  }
  // 2. the 4th file writes and reads 3MB+1020KB data (will trigger flushing in the processing of writing)
  write_size = 1020 + 3 * 1024 * 1024;
  io_info.size_ = write_size;
  io_info.fd_ = fds[file_num - 1];
  io_info.buf_ = write_bufs[file_num - 1] + already_write_sizes[file_num - 1];
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  read_buf = new char [write_size];
  io_info.buf_ = read_buf;
  io_info.size_ = write_size;
  io_info.fd_ = fds[file_num - 1];
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_bufs[file_num - 1] + already_write_sizes[file_num - 1], io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  already_write_sizes[file_num - 1] += write_size;

  // 3. the first three files write and read 1MB data 3 times
  write_size = 1024 * 1024;
  io_info.size_ = write_size;
  const int loop_cnt = 3;
  read_buf = new char [write_size];
  for (int cnt = 0; OB_SUCC(ret) && cnt < loop_cnt; cnt++) {
    for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
      io_info.fd_ = fds[i];
      io_info.buf_ = write_bufs[i] + already_write_sizes[i];
      ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    io_info.buf_ = read_buf;
    io_info.size_ = write_size;
    for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
      io_info.fd_ = fds[i];
      ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(io_info.size_, handle.get_done_size());
      cmp = memcmp(handle.get_buffer(), write_bufs[i] + already_write_sizes[i], io_info.size_);
      ASSERT_EQ(0, cmp);
      handle.reset();
      memset(read_buf, 0, write_size);
    }
    for (int i = 0; OB_SUCC(ret) && i < file_num - 1; i++) {
      already_write_sizes[i] += write_size;
    }
  }
  delete[] read_buf;
  // 4. each file read and write 12MB+4KB data
  write_size = 12 * 1024 * 1024 + 4 * 1024;
  io_info.size_ = write_size;
  read_buf = new char [write_size];
  for (int i = 0; OB_SUCC(ret) && i < file_num; i++) {
    io_info.fd_ = fds[i];
    io_info.buf_ = write_bufs[i] + already_write_sizes[i];
    ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  io_info.buf_ = read_buf;
  io_info.size_ = write_size;
  for (int i = 0; OB_SUCC(ret) && i < file_num; i++) {
    io_info.fd_ = fds[i];
    ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(io_info.size_, handle.get_done_size());
    cmp = memcmp(handle.get_buffer(), write_bufs[i] + already_write_sizes[i], io_info.size_);
    ASSERT_EQ(0, cmp);
    handle.reset();
    memset(read_buf, 0, write_size);
  }
  delete[] read_buf;
  for (int i = 0; OB_SUCC(ret) && i < file_num; i++) {
    already_write_sizes[i] += write_size;
  }

  for (int i = 0; OB_SUCC(ret) && i < file_num; i++) {
    ret = MTL(ObTenantTmpFileManager *)->remove(fds[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestTmpFile, test_multi_file_single_thread_read_write_with_disable_block_cache)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write_with_disable_block_cache begin=======================");
  test_multi_file_single_thread_read_write(true);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write_with_disable_block_cache end=======================");
}

TEST_F(TestTmpFile, test_single_file_multi_thread_read_write)
{
  STORAGE_LOG(INFO, "=======================test_single_file_multi_thread_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t read_thread_cnt = 4;
  const int64_t file_cnt = 1;
  const int64_t batch_size = 64 * 1024 * 1024; // 64MB
  const int64_t batch_num = 4;
  const bool disable_block_cache = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num, disable_block_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;
  check_final_status();

  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_single_file_multi_thread_read_write end=======================");
}

TEST_F(TestTmpFile, test_multi_file_multi_thread_read_write)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write begin=======================");
  int ret = OB_SUCCESS;
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.set_max_data_page_usage_ratio_(0.99);
  const int64_t read_thread_cnt = 4;
  const int64_t file_cnt = 4;
  const int64_t batch_size = 16 * 1024 * 1024; // 16MB
  const int64_t batch_num = 4;
  const bool disable_block_cache = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num, disable_block_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.set_max_data_page_usage_ratio_(0.90);
  check_final_status();
  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write end=======================");
}

TEST_F(TestTmpFile, test_more_files_more_threads_read_write)
{
  STORAGE_LOG(INFO, "=======================test_more_files_more_threads_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t read_thread_cnt = 2;
  const int64_t file_cnt = 128;
  const int64_t batch_size = 3 * 1024 * 1024;
  const int64_t batch_num = 2; // total 128 * 3MB * 2 = 768MB
  const bool disable_block_cache = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num, disable_block_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;

  check_final_status();
  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_more_files_more_threads_read_write end=======================");
}

TEST_F(TestTmpFile, test_multiple_small_files)
{
  STORAGE_LOG(INFO, "=======================test_multiple_small_files begin=======================");
  int ret = OB_SUCCESS;
  const int64_t read_thread_cnt = 2;
  const int64_t file_cnt = 256;
  const int64_t batch_size = 1 * 1024 * 1024 + 54 * 1024 + 1023; // 1MB + 54KB + 1023B
  const int64_t batch_num = 3;
  const bool disable_block_cache = true;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num, disable_block_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;

  check_final_status();
  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multiple_small_files end=======================");
}

// ATTENTION
// the case after this will increase wbp_mem_limit to BIG_WBP_MEM_LIMIT.
// And it will never be decreased as long as it has been increased
TEST_F(TestTmpFile, test_big_file)
{
  STORAGE_LOG(INFO, "=======================test_big_file begin=======================");
  const int64_t write_size = 750 * 1024 * 1024;  // write 750MB data
  const int64_t wbp_mem_limit = BIG_WBP_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = false;
  io_info.disable_block_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  usleep(1 * 1000 * 1000); // 1s
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file end=======================");
}

TEST_F(TestTmpFile, test_big_file_disable_page_cache)
{
  STORAGE_LOG(INFO, "=======================test_big_file_disable_page_cache begin=======================");
  const int64_t write_size = 750 * 1024 * 1024;  // write 750MB data
  const int64_t wbp_mem_limit = BIG_WBP_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = true;
  io_info.disable_block_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  usleep(1 * 1000 * 1000); // 1s
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_disable_page_cache end=======================");
}

TEST_F(TestTmpFile, test_aio_pread)
{
  STORAGE_LOG(INFO, "=======================test_aio_pread begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 10 * 1024 * 1024; // 10MB
  char *write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }

  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;

  // 1. Write data
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. check aio_pread
  int64_t read_size = 9 * 1024 * 1024; // 9MB
  int64_t read_offset = 0;
  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->aio_pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, handle.get_done_size());
  ret = handle.wait();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 3. execute two aio_pread, but io_handle doesn't not call wait()
  read_size = 5 * 1024 * 1024; // 5MB
  read_offset = 0;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->aio_pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, handle.get_done_size());

  int read_offset2 = read_offset + read_size;
  ret = MTL(ObTenantTmpFileManager *)->aio_pread(MTL_ID(), io_info, read_offset2, handle);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = handle.wait();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  STORAGE_LOG(INFO, "=======================test_aio_pread begin=======================");
}

void rand_shrink_or_expand_wbp(ObTmpWriteBufferPool &wbp, bool &has_stop)
{
  LOG_INFO("begin setting random wbp mem limit", K(ATOMIC_LOAD(&wbp.default_wbp_memory_limit_)), K(ATOMIC_LOAD(&wbp.capacity_)));
  for (int32_t cnt = 0; !ATOMIC_LOAD(&has_stop); ++cnt) {
    int64_t rand_wbp_size = 0;
    if (cnt == 0) {
      rand_wbp_size = ObRandom::rand(SMALL_WBP_MEM_LIMIT, BIG_WBP_MEM_LIMIT / 2);
    } else {
      rand_wbp_size = ObRandom::rand(BIG_WBP_MEM_LIMIT / 2, BIG_WBP_MEM_LIMIT * 2);
    }
    rand_wbp_size = (rand_wbp_size + WBP_BLOCK_SIZE - 1) / WBP_BLOCK_SIZE * WBP_BLOCK_SIZE;
    ATOMIC_SET(&wbp.default_wbp_memory_limit_, rand_wbp_size);
    LOG_INFO("wbp mem limit change", K(rand_wbp_size), K(ATOMIC_LOAD(&wbp.capacity_)));
    int64_t rand_sleep_time = ObRandom::rand(2, 5);
    sleep(rand_sleep_time);
  }
  LOG_INFO("stop setting random wbp mem limit", K(ATOMIC_LOAD(&wbp.default_wbp_memory_limit_)));
}

TEST_F(TestTmpFile, test_multi_file_wr_when_wbp_shrink_and_expand)
{
  int ret = OB_SUCCESS;
  ObTmpWriteBufferPool &wbp = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  TestMultiTmpFileStress test_0(MTL_CTX());
  ret = test_0.init(1/*file_cnt*/, dir, 1/*read_thread*/, IO_WAIT_TIME_MS, 100 * 1024 * 1024/*MB*/, 1/*batch_num*/, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  test_0.start();
  test_0.wait();

  bool has_stop = false;
  std::thread t(rand_shrink_or_expand_wbp, std::ref(wbp), std::ref(has_stop));

  const int64_t read_thread_cnt = 2;
  const int64_t file_cnt = 128;
  const int64_t batch_size = 3 * 1024 * 1024;
  const int64_t batch_num = 2; // total 128 * 3MB * 2 = 768MB
  const bool disable_block_cache = true;
  TestMultiTmpFileStress test(MTL_CTX());
  ret = test.init(file_cnt, dir, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num, disable_block_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;

  ATOMIC_STORE(&has_stop, true);
  t.join();
  LOG_INFO("manually abort current shrinking operation", K(wbp.shrink_ctx_));
  if (wbp.shrink_ctx_.is_valid()) {
    wbp.finish_shrinking();
  }
  // if wbp current size is larger than expected, wait for new shrinking operation complete
  if (wbp.default_wbp_memory_limit_ < wbp.capacity_) {
    sleep(5);
  }
  MockIO.check_wbp_free_list(wbp);
  LOG_INFO("wbp target shrink size info", K(wbp.default_wbp_memory_limit_), K(wbp.capacity_));
  ASSERT_EQ(wbp.default_wbp_memory_limit_, wbp.capacity_);
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  usleep(1 * 1000 * 1000); // 1s
  LOG_INFO("test_multi_file_wr_when_wbp_shrink_and_expand");
  LOG_INFO("io time", K(io_time));
}

int mock_flush_file(const int64_t flush_page_num, ObSharedStorageTmpFile &file, ObSSTmpFileAsyncFlushWaitTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(flush_page_num > file.cached_page_nums_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush page num is larger than cached page num", KR(ret), K(flush_page_num), K(file.cached_page_nums_));
  } else {
    ObTmpWriteBufferPool &wbp = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_;
    int64_t fd = file.fd_;
    int64_t flush_page_virtual_id = file.begin_page_virtual_id_;
    uint32_t flush_page_id = file.begin_page_id_;
    for(int64_t i = 0; OB_SUCC(ret) && i < flush_page_num; ++i) {
      if (OB_FAIL(wbp.notify_write_back(fd, flush_page_id, ObTmpFilePageUniqKey(flush_page_virtual_id)))) {
        LOG_WARN("fail to notify write back", KR(ret), K(flush_page_id), K(flush_page_virtual_id));
      } else if (OB_FAIL(wbp.get_next_page_id(fd, flush_page_id, ObTmpFilePageUniqKey(flush_page_virtual_id), flush_page_id))) {
        LOG_WARN("fail to get next page id", KR(ret), K(flush_page_id), K(flush_page_virtual_id));
      } else {
        flush_page_virtual_id++;
      }
    }
    int64_t flush_level = file.data_page_flush_level_;
    if (FAILEDx(file.flush_prio_mgr_->remove_file(false, file))) {
      LOG_WARN("fail to remove file from flush prio mgr", KR(ret), K(file));
    } else {
      file.data_page_flush_level_ = flush_level; // to mock flushing state
      file.write_back_data_page_num_ += flush_page_num;
      task.current_begin_page_id_ = file.begin_page_id_;
      task.current_begin_page_virtual_id_ = file.begin_page_virtual_id_;
      task.current_length_ = file.file_size_;
      task.expected_flushed_page_num_ = flush_page_num;
      task.fd_ = fd;
      task.flush_mgr_ = file.flush_mgr_;
      task.flushed_offset_ = MIN((file.begin_page_virtual_id_ + flush_page_num) * ObTmpFileGlobal::PAGE_SIZE, file.file_size_);
    }
  }
  return ret;
}

// we assume the error reason is OB_TIMEOUT
int mock_flush_wait(const int64_t succ_page_num, ObSSTmpFileAsyncFlushWaitTask& task, const bool is_tail_page_failed=false)
{
  int ret = OB_SUCCESS;
  task.succeed_wait_page_nums_ = succ_page_num;
  const bool has_flushed_unfinished_tail_page = task.flushed_offset_ == task.current_length_ &&
                                                task.current_length_ % ObTmpFileGlobal::PAGE_SIZE != 0;
  bool flush_breakdown = has_flushed_unfinished_tail_page && succ_page_num != task.expected_flushed_page_num_ &&
                         is_tail_page_failed;
  if (flush_breakdown) {
    if (OB_FAIL(task.flush_mgr_->notify_flush_breakdown(task.fd_))) {
      LOG_WARN("fail to notify flush breakdown", KR(ret), K(task));
    }
  } else if (OB_FAIL(task.flush_mgr_->update_meta_after_flush(task))){
    LOG_WARN("fail to update meta after flush", KR(ret), K(task));
  }
  return ret;
}

int check_flush_over(ObSharedStorageTmpFile &file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file.write_back_data_page_num_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file write back data page num is not 0", KR(ret), K(file));
  } else if (file.cached_page_nums_ == 0) {
    if (OB_UNLIKELY(file.begin_page_id_ != ObTmpFileGlobal::INVALID_PAGE_ID ||
                    file.begin_page_virtual_id_ != ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file begin page id is not invalid", KR(ret), K(file));
    }
  } else {
    ObTmpWriteBufferPool &wbp = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().wbp_;
    int64_t page_virtual_id = file.begin_page_virtual_id_;
    uint32_t page_id = file.begin_page_id_;
    for(int64_t i = 0; OB_SUCC(ret) && i < file.cached_page_nums_ ; ++i) {
      if (OB_UNLIKELY(!wbp.is_dirty(file.fd_, page_id, ObTmpFilePageUniqKey(page_virtual_id)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page is not dirty", KR(ret), K(page_id), K(page_virtual_id), K(file));
      } else if (OB_FAIL(wbp.get_next_page_id(file.fd_, page_id, ObTmpFilePageUniqKey(page_virtual_id), page_id))) {
        LOG_WARN("fail to get next page id", KR(ret), K(page_virtual_id), K(page_id), K(file));
      } else {
        page_virtual_id++;
      }
    }
  }
  return ret;
}

TEST_F(TestTmpFile, test_flush_with_write_tail_page_and_truncate)
{
  STORAGE_LOG(INFO, "=======================test_flush_with_write_tail_page_and_truncate begin=======================");
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
  std::cout << "open temporary file: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSSTmpFileHandle tmp_file_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
  ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
  ASSERT_EQ(fd, ss_tmp_file.get_fd());

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
  int64_t write_size = 0;
  char * write_buff = nullptr;
  ObTmpFileIOHandle io_handle;
  ObSSTmpFileAsyncFlushWaitTask wait_task;
  int64_t truncate_offset = 0;
  // 1. write and flush full pages, then wait
  write_size = 10 * ObTmpFileGlobal::PAGE_SIZE;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);
  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

  // 2. write and flush 4.5 pages; then write 0.5 page and exec wait logic
  write_size = 4 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));

  write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 1);

  // 3. write 4 pages(totally 5 pages in file) and flush 5 pages; then truncate 4.5 pages and exec wait logic
  write_size = 4 * ObTmpFileGlobal::PAGE_SIZE;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));

  truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 4) * ObTmpFileGlobal::PAGE_SIZE +
                    ObTmpFileGlobal::PAGE_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));

  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

  // 4. write 4.5 pages and flush 4.5 pages; then write 3.5 pages and truncate 6.5 pages; finally exec wait logic
  write_size = 4 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));

  write_size = 3 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 6) * ObTmpFileGlobal::PAGE_SIZE +
                    ObTmpFileGlobal::PAGE_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));

  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 2);

  // 5. append write file to 4.5 pages and flush 4.5 pages; then write 3.5 pages and truncate 2.5 pages; finally exec wait logic
  write_size = 2 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));

  write_size = 3 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 2) * ObTmpFileGlobal::PAGE_SIZE +
                    ObTmpFileGlobal::PAGE_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));

  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 4);

  // 6. write 6 pages(totally 10 pages in file) and flush 5 pages and exec wait; then flush and wait 5 pages again.
  write_size = 6 * ObTmpFileGlobal::PAGE_SIZE;
  write_buff = new char [write_size];
  io_info.size_ = write_size;
  io_info.buf_ = write_buff;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
  delete [] write_buff;
  io_handle.reset();

  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);
  ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
  ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
  ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
  ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

  tmp_file_handle.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));

  check_final_status();
  STORAGE_LOG(INFO, "=======================test_flush_with_write_tail_page_and_truncate end=======================");
}

TEST_F(TestTmpFile, test_flush_with_io_error)
{
  STORAGE_LOG(INFO, "=======================test_flush_with_io_error begin=======================");
  int ret = OB_SUCCESS;
  // 1. write and flush pages with uncompleted tail page; fill page to full (try to cancel the flushing for tail page);
  //    then, exec wait with io timeout for tail page, leading to file discard (although we try to give up flushing it)
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());

    ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
    int64_t write_size = 0;
    char * write_buff = nullptr;
    ObTmpFileIOHandle io_handle;
    ObSSTmpFileAsyncFlushWaitTask wait_task;

    write_size = 4 * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));

    write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(4, wait_task, true));
    ASSERT_EQ(ss_tmp_file.is_deleting_, true);
    write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, io_handle));
    delete [] write_buff;

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  // 2. some cases with flush timeout
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());

    ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
    int64_t write_size = 0;
    char * write_buff = nullptr;
    ObTmpFileIOHandle io_handle;
    ObSSTmpFileAsyncFlushWaitTask wait_task;

    // 2.1. write and flush 5 full pages; then, all of them are failed
    write_size = 5 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(0, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    // 2.2 flush pages, but 3 of them are successful
    ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(3, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 2);

    // 2.3 write 0.5 pages(totally 2.5 pages in file) and flush 2.5 pages;
    //     then write 1.5 pages;
    //     then exec wait and the first page is failed
    //     (we assume the tail page has been waited successful, thus we don't need to discard the file;
    //      this case is used to test whether the tail page is changed state to dirty twice)
    write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(3, ss_tmp_file, wait_task));
    write_size = ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(0, wait_task, false));
    ASSERT_EQ(ss_tmp_file.is_deleting_, false);
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 4);

    // 2.4 write 0.5 pages(totally 4.5 pages in file) and flush 4.5 pages;
    //     then exec wait and the tail page is failed (discarding the file)
    write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(5, ss_tmp_file, wait_task));
    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(4, wait_task, true));
    ASSERT_EQ(ss_tmp_file.is_deleting_, true);
    write_size = ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, io_handle));
    delete [] write_buff;

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  // 3. some cases with truncate and flush timeout
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());

    ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
    int64_t write_size = 0;
    char * write_buff = nullptr;
    ObTmpFileIOHandle io_handle;
    ObSSTmpFileAsyncFlushWaitTask wait_task;
    int64_t truncate_offset = 0;

    // 3.1. write and flush 10 pages; then, truncate 5 pages;
    //      exec wait and five pages are successful
    write_size = 10 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 5) * ObTmpFileGlobal::PAGE_SIZE;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 5);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    // 3.2. append write file to 10 pages and flush 10 pages; then, truncate 4 pages;
    //      exec wait and 3 pages are successful
    write_size = 5 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 4) * ObTmpFileGlobal::PAGE_SIZE;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 6);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 6);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(3, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 6);

    // 3.3. append write file to 10 pages and flush 10 pages; then, truncate 4 pages;
    //      exec wait and 5 pages are successful
    write_size = 4 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 4) * ObTmpFileGlobal::PAGE_SIZE;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 6);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 6);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(5, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    // 3.4. append write file to 10 pages and flush 10 pages; then, truncate 10 pages;
    //      exec wait and 0 pages are successful
    write_size = 5 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 10) * ObTmpFileGlobal::PAGE_SIZE;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 0);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(0, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

    // 3.5. append write file to 10 pages and flush 10 pages; then, truncate 5 pages;
    //      exec wait and 0 pages are successful
    write_size = 10 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 5) * ObTmpFileGlobal::PAGE_SIZE;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 5);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(0, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    // 3.6. append write file to 10 pages and flush 10 pages; then, truncate 5.5 pages;
    //      exec wait and 0 pages are successful
    write_size = 5 * ObTmpFileGlobal::PAGE_SIZE;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, mock_flush_file(10, ss_tmp_file, wait_task));

    truncate_offset = (ss_tmp_file.begin_page_virtual_id_ + 5) * ObTmpFileGlobal::PAGE_SIZE +
                      ObTmpFileGlobal::PAGE_SIZE / 2;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, 5);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    ASSERT_EQ(OB_SUCCESS, mock_flush_wait(0, wait_task));
    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 5);

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  check_final_status();
  STORAGE_LOG(INFO, "=======================test_flush_with_io_error end=======================");
}

int check_file_is_sealed(ObSharedStorageTmpFile &ss_tmp_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ss_tmp_file.is_sealed())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file is not sealed", K(ret), K(ss_tmp_file));
  } else {
    blocksstable::MacroBlockId block_id = ss_tmp_file.id_;
    block_id.set_third_id(ss_tmp_file.get_block_virtual_id_(ss_tmp_file.file_size_, true));
    int64_t ls_epoch_id = 0;
    bool is_exist = true;
    int cnt = 0;
    while (OB_SUCC(ret) && is_exist && cnt++ < 10) {
      if (OB_FAIL(MTL(ObTenantFileManager*)->is_exist_local_file(block_id, ls_epoch_id, is_exist))) {
        STORAGE_LOG(WARN, "check file is exist in local failed", K(ret), K(block_id));
      } else {
        usleep(1 * 1000 * 1000); // 1s
      }
    }
    if (OB_SUCC(ret)) {
      is_exist = false;
      cnt = 0;
      while (OB_SUCC(ret) && !is_exist && cnt++ < 10) {
        if (OB_FAIL(MTL(ObTenantFileManager*)->is_exist_remote_file(block_id, ls_epoch_id, is_exist))) {
          STORAGE_LOG(WARN, "check file is exist in remote failed", K(ret), K(block_id));
        } else {
          usleep(1 * 1000 * 1000); // 1s
        }
      }
    }
  }

  return ret;
}

TEST_F(TestTmpFile, test_seal)
{
  STORAGE_LOG(INFO, "=======================test_seal begin=======================");
  int ret = OB_SUCCESS;
  ObTmpFileIOInfo io_info;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = IO_WAIT_TIME_MS;
  int64_t write_size = 0;
  char * write_buff = nullptr;
  ObTmpFileIOHandle io_handle;
  ObSSTmpFileAsyncFlushWaitTask wait_task;
  ObSSTmpFileFlushManager &flush_mgr = MTL(ObTenantTmpFileManager *)->get_ss_file_manager().flush_mgr_;
  flush_mgr.stop();
  // 1. seal file before flushing
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());
    io_info.fd_ = fd;

    // 4MB + 3.5 pages
    int64_t write_page_num = 2 * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS + 4;
    write_size = write_page_num * ObTmpFileGlobal::PAGE_SIZE
                 - ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    io_handle.reset();

    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->seal(fd));
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;

    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    set_ss_tmp_file_flushing(ss_tmp_file);
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file.flush(true, io_info.io_desc_, ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS, flush_size, wait_task_handle));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, write_page_num);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, write_page_num);
    ASSERT_EQ(flush_mgr.get_wait_task_queue().queue_length_, 1);
    ASSERT_EQ(OB_SUCCESS, flush_mgr.exec_wait_task_once());
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    wait_task_handle.reset();

    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

    ASSERT_EQ(OB_SUCCESS, check_file_is_sealed(ss_tmp_file));

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  // 2. seal file between flushing and callback
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());
    io_info.fd_ = fd;

    // 4MB + 3.5 pages
    int64_t write_page_num = 2 * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS + 4;
    write_size = write_page_num * ObTmpFileGlobal::PAGE_SIZE
                 - ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    io_handle.reset();

    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    set_ss_tmp_file_flushing(ss_tmp_file);
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file.flush(true, io_info.io_desc_, ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS, flush_size, wait_task_handle));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, write_page_num);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, write_page_num);
    ASSERT_EQ(flush_mgr.get_wait_task_queue().queue_length_, 1);

    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->seal(fd));
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;

    ASSERT_EQ(OB_SUCCESS, flush_mgr.exec_wait_task_once());
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    wait_task_handle.reset();

    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

    ASSERT_EQ(OB_SUCCESS, check_file_is_sealed(ss_tmp_file));

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  // 3. seal file after callback
  {
    int64_t dir = -1;
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    std::cout << "open temporary file: " << fd << std::endl;
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSSTmpFileHandle tmp_file_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->get_ss_file_manager().get_tmp_file(fd, tmp_file_handle));
    ObSharedStorageTmpFile &ss_tmp_file = *static_cast<ObSharedStorageTmpFile *>(tmp_file_handle.get());
    ASSERT_EQ(fd, ss_tmp_file.get_fd());
    io_info.fd_ = fd;

    // 4MB + 3.5 pages
    int64_t write_page_num = 2 * ObTmpFileGlobal::SS_BLOCK_PAGE_NUMS + 4;
    write_size = write_page_num * ObTmpFileGlobal::PAGE_SIZE
                 - ObTmpFileGlobal::PAGE_SIZE / 2;
    write_buff = new char [write_size];
    io_info.size_ = write_size;
    io_info.buf_ = write_buff;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    io_handle.reset();

    int64_t flush_size = 0;
    ObSSTmpFileAsyncFlushWaitTaskHandle wait_task_handle;
    set_ss_tmp_file_flushing(ss_tmp_file);
    ASSERT_EQ(OB_SUCCESS, ss_tmp_file.flush(true, io_info.io_desc_, ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS, flush_size, wait_task_handle));
    ASSERT_EQ(ss_tmp_file.write_back_data_page_num_, write_page_num);
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, write_page_num);
    ASSERT_EQ(flush_mgr.get_wait_task_queue().queue_length_, 1);

    ASSERT_EQ(OB_SUCCESS, flush_mgr.exec_wait_task_once());
    ASSERT_EQ(OB_SUCCESS, wait_task_handle.wait(ObTmpFileGlobal::SS_TMP_FILE_FLUSH_WAIT_TIMEOUT_MS));
    wait_task_handle.reset();

    ASSERT_EQ(OB_SUCCESS, check_flush_over(ss_tmp_file));
    ASSERT_EQ(ss_tmp_file.cached_page_nums_, 0);

    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->seal(fd));
    ASSERT_NE(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->aio_write(MTL_ID(), io_info, io_handle));
    delete [] write_buff;

    ASSERT_EQ(OB_SUCCESS, check_file_is_sealed(ss_tmp_file));

    tmp_file_handle.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantTmpFileManager *)->remove(fd));
  }

  flush_mgr.start();
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_seal end=======================");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_tmp_file.log*");
  system("rm -rf ./run*");
  OB_LOGGER.set_file_name("test_ss_tmp_file.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}