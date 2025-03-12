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

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;
/* ------------------------------ Mock Parameter ---------------------------- */
static const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L /* 16 GB */;
static constexpr int64_t IO_WAIT_TIME_MS = 5 * 1000L; // 5s
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
/********************************* Mock Meta Tree *************************** */
static const int64_t MAX_DATA_ITEM_ARRAY_COUNT = 2;
static const int64_t MAX_PAGE_ITEM_COUNT = 4;   // MAX_PAGE_ITEM_COUNT * ObTmpFileGlobal::PAGE_SIZE means
                                                // the max representation range of a meta page (4 * 2MB == 8MB).
                                                // according to the formula of summation for geometric sequence
                                                // (S_n = a_1 * (1-q^n)/(1-q), where a_1 = 8MB, q = 4),
                                                // a two-level meta tree could represent at most 40MB disk data of tmp file
                                                // a three-level meta tree could represent at most 168MB disk data of tmp file
                                                // a four-level meta tree could represent at most 680MB disk data of tmp file

/* ---------------------------- Unittest Class ----------------------------- */

class TestTmpFile : public ::testing::Test
{
public:
  TestTmpFile() = default;
  virtual ~TestTmpFile() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
private:
  void check_final_status();
};
static ObSimpleMemLimitGetter getter;

// ATTENTION!
// currently, we only initialize modules about tmp file at the beginning of unit test and
// never restart them in the end of test case.
// please make sure that all test cases will not affect the others.
void TestTmpFile::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);

  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  ObSharedNothingTmpFileMetaTree::set_max_array_item_cnt(MAX_DATA_ITEM_ARRAY_COUNT);
  ObSharedNothingTmpFileMetaTree::set_max_page_item_cnt(MAX_PAGE_ITEM_COUNT);
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
//  if (!MTL(ObTenantTmpFileManager *)->is_inited_) {
//    ret = MTL(ObTenantTmpFileManager *)->init();
//    ASSERT_EQ(OB_SUCCESS, ret);
//    ret = MTL(ObTenantTmpFileManager *)->start();
//    ASSERT_EQ(OB_SUCCESS, ret);
//    MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
//  }
  GCONF._data_storage_io_timeout = IO_WAIT_TIME_MS * 1000;
}

void TestTmpFile::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTmpFile::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
//  if (MTL(ObTenantTmpFileManager *)->is_inited_) {
//    MTL(ObTenantTmpFileManager *)->stop();
//    MTL(ObTenantTmpFileManager *)->wait();
//    MTL(ObTenantTmpFileManager *)->destroy();
//  }
}

void TestTmpFile::check_final_status()
{
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.flush_priority_mgr_.get_file_size());
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.evict_mgr_.get_file_size());
}

// generate 2MB random data (will not trigger flush and evict logic)
// 1. test write pages and append write tail page
// 2. test write after reading
TEST_F(TestTmpFile, test_unaligned_data_read_write)
{
  STORAGE_LOG(INFO, "=======================test_unaligned_data_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 2 * 1024 * 1024;
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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
      io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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
      io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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

// generate 8MB random data
// this test will check whether kv_cache caches correct pages in disk
TEST_F(TestTmpFile, test_cached_read)
{
  STORAGE_LOG(INFO, "=======================test_cached_read begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 8 * 1024 * 1024; // 8MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
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

  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_GT(wbp_begin_offset, 0);
  ASSERT_EQ(wbp_begin_offset % ObTmpFileGlobal::PAGE_SIZE, 0);

  // 2. check block kv cache
  common::ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ret = file_handle.get()->meta_tree_.search_data_items(0,wbp_begin_offset, data_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, data_items.empty());
  for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count(); i++) {
    const int64_t block_index = data_items[i].block_index_;
    tmp_file::ObTmpBlockValueHandle block_value_handle;
    ret = tmp_file::ObTmpBlockCache::get_instance().get_block(tmp_file::ObTmpBlockCacheKey(block_index, MTL_ID()),
                                                    block_value_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // 3. read data from block kv cache
  int64_t read_size = write_size;
  int64_t read_offset = 0;
  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = true;
  io_info.disable_block_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 4. read disk data and puts them into kv_cache
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

  // 6. read disk data to check whether kv_cache caches correct pages
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

  // 7. check pages in kv_cache
  int64_t previous_virtual_page_id = data_items.at(0).virtual_page_id_;
  for (int64_t i = 0; i < data_items.count() && OB_SUCC(ret); ++i) {
    const ObSharedNothingTmpFileDataItem &data_item = data_items.at(i);
    if (i > 0) {
      ASSERT_GT(data_item.virtual_page_id_, previous_virtual_page_id);
      previous_virtual_page_id = data_item.virtual_page_id_;
    }
    for (int64_t j = 0; j < data_item.physical_page_num_; j++) {
      int64_t physical_page_id = data_item.physical_page_id_ + j;
      tmp_file::ObTmpPageCacheKey key(data_item.block_index_, physical_page_id, MTL_ID());
      tmp_file::ObTmpPageValueHandle handle;
      ret = tmp_file::ObTmpPageCache::get_instance().get_page(key, handle);
      if (OB_FAIL(ret)) {
        std::cout << "get cached page failed" << i <<" "<< data_item.block_index_<<" "<< physical_page_id << std::endl;
        ob_abort();
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      cmp = memcmp(handle.value_->get_buffer(), write_buf + (data_item.virtual_page_id_ + j) * ObTmpFileGlobal::PAGE_SIZE, ObTmpFileGlobal::PAGE_SIZE);
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
  const int64_t write_size = 9 * 1024 * 1024 + 37 * 1024; // 9MB + 37KB, that is, 256 * 4 + 132 + 0.625 pages
  ObTmpWriteBufferPool &wbp = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_;
  wbp.default_wbp_memory_limit_ = 10 * WBP_BLOCK_SIZE;
  const int64_t wbp_mem_limit = wbp.get_memory_limit();
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
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

  ObTmpFilePageCacheController &pc_ctrl = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_page_cache_controller();
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true);

  // 1. Write data and wait flushing over
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  ObTmpFileEvictionManager &evict_mgr = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.evict_mgr_;
  const int64_t page_num = write_size / ObTmpFileGlobal::PAGE_SIZE;
  int64_t actual_evict_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, evict_mgr.evict(page_num * 2, actual_evict_page_num)); // evict all pages
  ASSERT_EQ(file_handle.get()->cached_page_nums_, 0);

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
    } else { // last unfinished page occupies 1 data item, and co-occupy the same macro block with prev data item
      read_size = 10;
      read_offset = write_size - 20;
    }
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
  common::ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ret = file_handle.get()->meta_tree_.search_data_items(0, write_size, data_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, data_items.empty());
  int64_t previous_virtual_page_id = data_items.at(0).virtual_page_id_;
  for (int64_t i = 0; i < data_items.count() && OB_SUCC(ret); ++i) {
    const ObSharedNothingTmpFileDataItem &data_item = data_items.at(i);
    LOG_INFO("data item in checking kv cache", K(i), K(data_item));
    if (i > 0) {
      ASSERT_GT(data_item.virtual_page_id_, previous_virtual_page_id);
      previous_virtual_page_id = data_item.virtual_page_id_;
    }
    for (int64_t j = 0; j < data_item.physical_page_num_; j++) {
      int64_t physical_page_id = data_item.physical_page_id_ + j;
      tmp_file::ObTmpPageCacheKey key(data_item.block_index_, physical_page_id, MTL_ID());
      tmp_file::ObTmpPageValueHandle handle;
      ret = tmp_file::ObTmpPageCache::get_instance().get_page(key, handle);
      if (OB_FAIL(ret)) {
        _OB_LOG(INFO, "get cached page failed, i:%ld, block_index:%ld, physical_page_id:%ld\n", i, data_item.block_index_, physical_page_id);
        printf("get cached page failed, i:%ld, block_index:%ld, physical_page_id:%ld\n", i, data_item.block_index_, physical_page_id);
        ob_abort();
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      int64_t cmp_size = (i == data_items.count() - 1 && j == data_item.physical_page_num_ - 1 ?
                          write_size % ObTmpFileGlobal::PAGE_SIZE :
                          ObTmpFileGlobal::PAGE_SIZE);
      int cmp = memcmp(handle.value_->get_buffer(), write_buf + (data_item.virtual_page_id_ + j) * ObTmpFileGlobal::PAGE_SIZE, cmp_size);
      EXPECT_EQ(0, cmp);
      if (0 != cmp) {
        LOG_ERROR("cached page data not match", K(i), K(j), K(data_items.count() - 1), K(data_item.physical_page_num_ - 1), K(cmp_size), K(data_item));
        ob_abort();
      }
    }
  }

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
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;
  file_handle.reset();

  // 1. write 2KB data and check rightness of writing
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 2 * 1024; // 2KB
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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
  ObTmpFilePageCacheController &pc_ctrl = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_page_cache_controller();
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true);
  pc_ctrl.flush_tg_.notify_doing_flush();
  sleep(2);
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = file_handle.get()->page_cache_controller_->invoke_swap_and_wait(write_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
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
  pc_ctrl.flush_tg_.notify_doing_flush();
  sleep(2);
  ret = file_handle.get()->page_cache_controller_->invoke_swap_and_wait(write_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  wbp_begin_offset = file_handle.get()->cal_wbp_begin_offset();
  ASSERT_EQ(wbp_begin_offset, already_write_size);
  ATOMIC_SET(&pc_ctrl.flush_all_data_, false);

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
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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

TEST_F(TestTmpFile, test_truncate_to_flushed_page_id)
{
  STORAGE_LOG(INFO, "=======================test_truncate_to_flushed_page_id end=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 4 * 1024 * 1024 + 12 * 1024;
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  // Write data
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  sleep(2); // waits for flushing 4MB data pages, 12KB(2 pages) left
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, 4 * 1024 * 1024);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4 * 1024 * 1024, file_handle.get()->truncated_offset_);

  int64_t block_index = -1;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_block_manager_.create_tmp_file_block(0/*begin_page_id*/, ObTmpFileGlobal::BLOCK_PAGE_NUMS, block_index);
  int64_t flush_sequence = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.flush_mgr_.flush_ctx_.get_flush_sequence();
  ObTmpFileDataFlushContext data_flush_ctx;
  ObTmpFileFlushTask flush_task;
  flush_task.get_flush_infos().push_back(ObTmpFileFlushInfo());
  flush_task.set_block_index(block_index);
  ret = flush_task.prealloc_block_buf();
  EXPECT_EQ(OB_SUCCESS, ret);
  // copy first page after flush and truncate
  int64_t last_info_idx = flush_task.get_flush_infos().count() - 1;
  ret = file_handle.get()->generate_data_flush_info(flush_task, flush_task.get_flush_infos().at(last_info_idx),
                                                    data_flush_ctx, flush_sequence, false/*flush tail*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("checking flush task", K(flush_task));
  LOG_INFO("checking data flush ctx", K(data_flush_ctx));
  int64_t PAGE_SIZE = 8 * 1024;
  EXPECT_EQ(PAGE_SIZE, flush_task.get_data_length());

  // simulate we have push 1 task to TFFT_INSERT_META_TREE and release truncate lock
  // so that another truncate operation can come in.
  file_handle.get()->truncate_lock_.unlock();

  // truncate again
  int64_t truncate_offset = 4 * 1024 * 1024 + PAGE_SIZE + PAGE_SIZE / 2;
  ret = MTL(ObTenantTmpFileManager *)->truncate(fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(truncate_offset, file_handle.get()->truncated_offset_);

  // append 8KB, 12KB left in wbp
  io_info.size_ = PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4 * 1024 * 1024 + 20 * 1024, file_handle.get()->file_size_);

  // copy second page
  flush_task.get_flush_infos().push_back(ObTmpFileFlushInfo());
  last_info_idx = flush_task.get_flush_infos().count() - 1;
  ret = file_handle.get()->generate_data_flush_info(flush_task, flush_task.get_flush_infos().at(last_info_idx),
                                                    data_flush_ctx, flush_sequence, false/*flush tail*/);
  LOG_INFO("checking flush task", K(flush_task));
  LOG_INFO("checking data flush ctx", K(data_flush_ctx));
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(PAGE_SIZE * 2, flush_task.get_data_length());

  // copy third page
  flush_task.get_flush_infos().push_back(ObTmpFileFlushInfo());
  last_info_idx = flush_task.get_flush_infos().count() - 1;
  ret = file_handle.get()->generate_data_flush_info(flush_task, flush_task.get_flush_infos().at(last_info_idx),
                                                    data_flush_ctx, flush_sequence, true/*flush tail*/);
  LOG_INFO("checking flush task", K(flush_task));
  LOG_INFO("checking data flush ctx", K(data_flush_ctx));
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(PAGE_SIZE * 3, flush_task.get_data_length());

  int64_t first_virtual_page_id = flush_task.get_flush_infos().at(0).flush_virtual_page_id_;
  int64_t second_virtual_page_id = flush_task.get_flush_infos().at(1).flush_virtual_page_id_;
  int64_t third_virtual_page_id = flush_task.get_flush_infos().at(2).flush_virtual_page_id_;
  EXPECT_EQ(first_virtual_page_id + 1, second_virtual_page_id);
  EXPECT_EQ(second_virtual_page_id + 1, third_virtual_page_id);

  file_handle.reset();
  flush_task.~ObTmpFileFlushTask();
  free(write_buf);

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  STORAGE_LOG(INFO, "=======================test_truncate_to_flushed_page_id end=======================");
}

TEST_F(TestTmpFile, test_write_last_page_during_flush)
{
  STORAGE_LOG(INFO, "=======================test_write_last_page_during_flush begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 64 * 1024 + 100;
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
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  printf("generate_data_flush_info\n");
  // hard code generate_data_flush_info to flush last page
  int64_t block_index = -1;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_block_manager_.create_tmp_file_block(0/*begin_page_id*/, ObTmpFileGlobal::BLOCK_PAGE_NUMS, block_index);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t flush_sequence = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.flush_mgr_.flush_ctx_.get_flush_sequence();
  ObTmpFileDataFlushContext data_flush_ctx;
  ObTmpFileFlushTask flush_task;
  flush_task.get_flush_infos().push_back(ObTmpFileFlushInfo());
  flush_task.set_block_index(block_index);
  ret = flush_task.prealloc_block_buf();
  EXPECT_EQ(OB_SUCCESS, ret);
  // copy first page after flush and truncate
  int64_t last_info_idx = flush_task.get_flush_infos().count() - 1;
  ret = file_handle.get()->generate_data_flush_info(flush_task, flush_task.get_flush_infos().at(last_info_idx),
                                                    data_flush_ctx, flush_sequence, true/*flush tail*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert_meta_tree
  ret = file_handle.get()->insert_meta_tree_item(flush_task.get_flush_infos().at(0), block_index);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write before IO complete
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // assume io complete, update file meta
  bool reset_ctx = false;
  ret = file_handle.get()->update_meta_after_flush(flush_task.get_flush_infos().at(0).batch_flush_idx_, false/*is_meta*/, reset_ctx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(8, file_handle.get()->flushed_data_page_num_);
  ASSERT_EQ(0, file_handle.get()->write_back_data_page_num_);

  flush_task.~ObTmpFileFlushTask();
  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();

  delete [] write_buf;
  STORAGE_LOG(INFO, "=======================test_write_last_page_during_flush end=======================");
}

// this test will trigger flush and evict logic for both data and meta pages.
void test_big_file(const int64_t write_size, const int64_t wbp_mem_limit, ObTmpFileIOInfo io_info)
{
  int ret = OB_SUCCESS;
  ASSERT_GT(write_size, wbp_mem_limit);
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = wbp_mem_limit;
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
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

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
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  cmp = memcmp(handle.get_buffer(), write_buf, handle.get_done_size());
  ASSERT_EQ(read_size, handle.get_done_size());
  handle.reset();
  ASSERT_EQ(0, cmp);
  memset(read_buf, 0, read_size);

  // 3. attempt to read data when reach the end of file
  int64_t read_time = ObTimeUtility::current_time();
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
  const int64_t begin_block_id = upper_align(file_handle.get()->cal_wbp_begin_offset(), macro_block_size)
                                 / macro_block_size / 4;
  const int64_t end_block_id = upper_align(file_handle.get()->cal_wbp_begin_offset(), macro_block_size)
                               / macro_block_size / 4 * 3;
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
    cmp = memcmp(handle.get_buffer(), write_buf + read_offset, handle.get_done_size());
    handle.reset();
    ASSERT_EQ(0, cmp);
    memset(read_buf, 0, read_size);
  }
  read_time = ObTimeUtility::current_time() - read_time;

  free(write_buf);
  free(read_buf);

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.flush_priority_mgr_.get_file_size());
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.evict_mgr_.get_file_size());

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
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.get_memory_limit();
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
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.flush_priority_mgr_.get_file_size());
  ASSERT_EQ(0, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.evict_mgr_.get_file_size());
}

TEST_F(TestTmpFile, test_multi_file_single_thread_read_write)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write begin=======================");
  test_multi_file_single_thread_read_write(false);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write end=======================");
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
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.set_max_data_page_usage_ratio_(0.99);
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
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.set_max_data_page_usage_ratio_(0.90);
  check_final_status();
  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write end=======================");
}

TEST_F(TestTmpFile, test_multi_file_multi_thread_read_write_with_block_cache)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write_with_block_cache begin=======================");
  int ret = OB_SUCCESS;
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.set_max_data_page_usage_ratio_(0.99);
  const int64_t read_thread_cnt = 4;
  const int64_t file_cnt = 4;
  const int64_t batch_size = 16 * 1024 * 1024; // 16MB
  const int64_t batch_num = 4;
  const bool disable_block_cache = false;
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
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.set_max_data_page_usage_ratio_(0.90);
  check_final_status();
  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write_with_block_cache end=======================");
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
  const int64_t batch_size = 10 * 1024; // 10KB
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
  io_info.disable_block_cache_ = false;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file end=======================");
}

TEST_F(TestTmpFile, test_big_file_disable_block_cache)
{
  STORAGE_LOG(INFO, "=======================test_big_file_disable_block_cache begin=======================");
  const int64_t write_size = 750 * 1024 * 1024;  // write 750MB data
  const int64_t wbp_mem_limit = BIG_WBP_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = false;
  io_info.disable_block_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_disable_block_cache end=======================");
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
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

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
  STORAGE_LOG(INFO, "=======================test_aio_pread end=======================");
}

void rand_shrink_or_expand_wbp(ObTmpWriteBufferPool &wbp, bool &has_stop)
{
  LOG_INFO("begin setting random wbp mem limit", K(ATOMIC_LOAD(&wbp.default_wbp_memory_limit_)), K(ATOMIC_LOAD(&wbp.capacity_)));
  for (int32_t cnt = 0; !ATOMIC_LOAD(&has_stop); ++cnt) {
    int64_t rand_wbp_size = 0;
    if (cnt == 0) {
      rand_wbp_size = ObRandom::rand(SMALL_WBP_MEM_LIMIT, BIG_WBP_MEM_LIMIT / 2);
    } else {
      rand_wbp_size = ObRandom::rand(BIG_WBP_MEM_LIMIT / 2, BIG_WBP_MEM_LIMIT);
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
  STORAGE_LOG(INFO, "=======================test_multi_file_wr_when_wbp_shrink_and_expand begin=======================");
  int ret = OB_SUCCESS;
  ObTmpFilePageCacheController &pc_ctrl = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_page_cache_controller();
  ObTmpWriteBufferPool &wbp = pc_ctrl.write_buffer_pool_;
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

  const int64_t read_thread_cnt = 1;
  const int64_t file_cnt = 32;
  const int64_t batch_size = 3 * 1024 * 1024;
  const int64_t batch_num = 2; // total 32 * 3MB * 2 = 192MB
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

  LOG_INFO("io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multi_file_wr_when_wbp_shrink_and_expand end=======================");
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_sn_tmp_file.log*");
  system("rm -rf ./run*");
  OB_LOGGER.set_file_name("test_sn_tmp_file.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
