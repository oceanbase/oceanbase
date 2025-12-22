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

#define USING_LOG_PREFIX STORAGE
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/mtlenv/storage/tmp_file/ob_sn_tmp_file_test_helper.h"
#include "lib/alloc/memory_dump.h"
#define protected public
#define private public

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
static const int64_t WRITE_CACHE_BLOCK_SIZE = ObTmpFileWriteCache::WBP_BLOCK_SIZE; // each wbp block has 253 pages (253 * 8KB == 2024KB)
static const int64_t SMALL_WRITE_CACHE_BLOCK_COUNT = 4;
static const int64_t SMALL_WRITE_CACHE_MEM_LIMIT = SMALL_WRITE_CACHE_BLOCK_COUNT * WRITE_CACHE_BLOCK_SIZE; // the wbp mem size is 7.906MB
static const int64_t BIG_WRITE_CACHE_BLOCK_COUNT = 40;
static const int64_t BIG_WRITE_CACHE_MEM_LIMIT = BIG_WRITE_CACHE_BLOCK_COUNT * WRITE_CACHE_BLOCK_SIZE; // the wbp mem size is 79.06MB
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
  ObMallocAllocator::get_instance()->set_tenant_max_min(MTL_ID(), TENANT_MEMORY, 0);

  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.default_memory_limit_ = SMALL_WRITE_CACHE_MEM_LIMIT;
  ObSharedNothingTmpFileMetaTree::set_max_array_item_cnt(MAX_DATA_ITEM_ARRAY_COUNT);
  ObSharedNothingTmpFileMetaTree::set_max_page_item_cnt(MAX_PAGE_ITEM_COUNT);

  ObMemoryDump().get_instance().init();
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
}

void TestTmpFile::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTmpFile::TearDown()
{
  ObKVGlobalCache::get_instance().erase_cache();
}

void TestTmpFile::check_final_status()
{
  ObSNTenantTmpFileManager &file_mgr = MTL(ObTenantTmpFileManager *)->get_sn_file_manager();
  ObTmpFileBlockManager &block_mgr = file_mgr.tmp_file_block_manager_;
  ASSERT_EQ(0, file_mgr.write_cache_.page_map_.count());
  block_mgr.print_blocks();
  ASSERT_EQ(0, block_mgr.block_map_.count());
  block_mgr.alloc_priority_mgr_.print_blocks();
  ASSERT_EQ(0, block_mgr.alloc_priority_mgr_.get_block_count());
  block_mgr.flush_priority_mgr_.print_blocks();
  ASSERT_EQ(0, block_mgr.flush_priority_mgr_.get_block_count());
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(1);
  ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(1);
}

// generate 4MB random data (will not trigger flush logic)
// 1. test write pages and append write tail page
// 2. test write after reading
TEST_F(TestTmpFile, test_unaligned_data_read_write)
{
  STORAGE_LOG(INFO, "=======================test_unaligned_data_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 4 * 1024 * 1024;
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_memory_limit();
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
  file_handle.reset();

  // random write, read, and check
  int64_t already_write = 0;
  std::vector<int64_t> turn_write_size = generate_random_sequence(1, write_size / 4, write_size);
  for (int i = 0; i < turn_write_size.size(); ++i) {
    int64_t this_turn_write_size = turn_write_size[i];
    std::cout << "random write and read " << i << " "<< this_turn_write_size << std::endl;
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
      std::string r_file_name = std::to_string(fd) + "_raw_write_data.txt";
      dump_hex_data(write_buffer, write_size, r_file_name);

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
  delete[] write_buffer;
  STORAGE_LOG(INFO, "=======================test_unaligned_data_read_write end=======================");
}

// generate 12MB random data
// this test will trigger flush logic.
// 1. test pread
// 1.1 read whole data (disable page cache, read some page from disk)
// 1.2 read whole data (enable page cache, cached some page from disk)
// 1.3 read whole data (enable page cache, hit pages in cache)
// 1.4 read OB_ITER_END
// 2. test read
// 2.1 read aligned data
// 2.2 read unaligned data
TEST_F(TestTmpFile, test_read)
{
  STORAGE_LOG(INFO, "=======================test_read begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 12 * 1024 * 1024; // 12MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_memory_limit();
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

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS * 5;
  // Write data
  int64_t write_time = ObTimeUtility::current_time();
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  write_time = ObTimeUtility::current_time() - write_time;
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArray<int64_t> disk_pages;
  ObTmpFileWriteCacheKey write_cache_key;
  write_cache_key.type_ = PageType::DATA;
  write_cache_key.fd_ = fd;
  int64_t end_virtual_page_id = common::upper_align(write_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1;
  for (int64_t virtual_page_id = 0; virtual_page_id <= end_virtual_page_id; ++virtual_page_id) {
    write_cache_key.virtual_page_id_ = virtual_page_id;
    ObTmpFilePageHandle page_handle;
    if (OB_FAIL(MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_page(write_cache_key, page_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = disk_pages.push_back(virtual_page_id);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_GT(disk_pages.count(), 0);

  ObTmpPageCacheKey kv_cache_key;
  kv_cache_key.set_tenant_id(MTL_ID());
  ObTmpPageValueHandle p_handle;
  for (int64_t i = 0; i < disk_pages.count(); ++i) {
    const int64_t virtual_page_id = disk_pages[i];
    write_cache_key.virtual_page_id_ = virtual_page_id;
    kv_cache_key.set_page_key(write_cache_key);
    p_handle.reset();
    // TODO: wanyue.wy
    // when support the file attribute for disable page cache, remove the 'erase()'
    ObTmpPageCache::get_instance().erase(kv_cache_key);
    ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
  int64_t read_time = ObTimeUtility::current_time();
  /************** test pread **************/
  // // 1. read whole data (disable page cache, read some page from disk)
  char *read_buf = new char [write_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = write_size;
  io_info.disable_page_cache_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  memset(read_buf, 0, write_size);

  for (int64_t i = 0; i < disk_pages.count(); ++i) {
    const int64_t virtual_page_id = disk_pages[i];
    write_cache_key.virtual_page_id_ = virtual_page_id;
    kv_cache_key.set_page_key(write_cache_key);
    p_handle.reset();
    ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }

  // 2. read whole data (enable page cache, cached some page from disk)
  io_info.disable_page_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  memset(read_buf, 0, write_size);

  for (int64_t i = 0; i < disk_pages.count(); ++i) {
    const int64_t virtual_page_id = disk_pages[i];
    write_cache_key.virtual_page_id_ = virtual_page_id;
    kv_cache_key.set_page_key(write_cache_key);
    p_handle.reset();
    ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // 3. read whole data (enable page cache, hit pages in cache)
  io_info.disable_page_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 4. read OB_ITER_END
  read_buf = new char [200];
  io_info.buf_ = read_buf;
  io_info.size_ = 200;
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
  delete[] write_buf;

  LOG_INFO("io time", K(write_time), K(read_time));
  STORAGE_LOG(INFO, "=======================test_read end=======================");
}

void flush_all_page()
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = 10 * 1000;
  const int64_t start_time = ObTimeUtility::current_time();
  ObTmpFileWriteCache &write_cache = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_;
  ATOMIC_SET(&write_cache.is_flush_all_, true);
  LOG_DEBUG("flush all pages begin",
      K(ATOMIC_LOAD(&write_cache.used_page_cnt_)), K(ATOMIC_LOAD(&write_cache.is_flush_all_)));
  write_cache.swap_page_(timeout_ms);
  while (ATOMIC_LOAD(&write_cache.used_page_cnt_) > 0) {
    sleep(2);
    std::cout << "used page cnt: " << ATOMIC_LOAD(&write_cache.used_page_cnt_)
              << " is_flush_all: "<< ATOMIC_LOAD(&write_cache.is_flush_all_) << std::endl;
    PAUSE();

    if (ObTimeUtility::current_time() - start_time > 30 * 1000 * 1000) {
      bool is_all_meta = true; // DEBUGtest_tmp_file.cpp:437
      for (int64_t i = 0; i < write_cache.pages_.size(); ++i) {
        if (write_cache.pages_[i].is_valid()) {
          LOG_INFO("page not flushed", K(write_cache.pages_[i]));
          if (PageType::META != write_cache.pages_[i].get_page_key().type_) {
            is_all_meta = false;
          }
        }
      }
      if (!is_all_meta) {
        printf("flush all page timeout, used_page_cnt:%d\n", write_cache.used_page_cnt_);
        for (int64_t i = 0; i < write_cache.pages_.size(); ++i) {
          if (write_cache.pages_[i].is_valid()) {
            char page_str[1024];
            write_cache.pages_[i].to_string(page_str, 1024);
            ADD_FAILURE() << "write cache resource release failed, page:" << i
                          << ", is_valid, " << page_str;
            break;
          }
        }
      }
      break;
    }
  }
  ATOMIC_SET(&write_cache.is_flush_all_, false);
}

// 1. Write 7MB data
// 2. flush all page and remove them from write cache and kv cache
// 3. read data to check prefetched data in kv cache
TEST_F(TestTmpFile, test_prefetch_read)
{
  STORAGE_LOG(INFO, "=======================test_prefetch_read begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_size = 7 * 1024 * 1024; // 7MB
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

  // 2. flush all page and remove them from write cache and kv cache
  flush_all_page();
  ObTmpFileWriteCacheKey write_cache_key;
  write_cache_key.type_ = PageType::DATA;
  write_cache_key.fd_ = fd;
  int64_t end_virtual_page_id = common::upper_align(write_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1;
  for (int64_t virtual_page_id = 0; virtual_page_id <= end_virtual_page_id; ++virtual_page_id) {
    write_cache_key.virtual_page_id_ = virtual_page_id;
    ObTmpFilePageHandle page_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_page(write_cache_key, page_handle));
  }

  ObTmpPageCacheKey kv_cache_key;
  kv_cache_key.set_tenant_id(MTL_ID());
  ObTmpPageValueHandle p_handle;
  // TODO: wanyue.wy
  // when support the file attribute for disable page cache,
  // remove the 'erase()' and use the attribute to avoid putting data into cache,
  for (int64_t virtual_page_id = 0; virtual_page_id <= end_virtual_page_id; ++virtual_page_id) {
    write_cache_key.virtual_page_id_ = virtual_page_id;
    kv_cache_key.set_page_key(write_cache_key);
    p_handle.reset();
    ObTmpPageCache::get_instance().erase(kv_cache_key);
    ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }

  // 3. read data
  // add "padding_in_shared_block" for start and end offset to
  // make the read interval is hit the middle part of exclusive block
  const int64_t padding_in_shared_block = ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM * ObTmpFileGlobal::PAGE_SIZE;
  const int64_t start_read_offset = 1 * 1024 * 1024 + padding_in_shared_block;
  const int64_t start_read_page_virtual_id = start_read_offset / ObTmpFileGlobal::PAGE_SIZE;
  const int64_t start_read_block_virtual_id = start_read_offset / ObTmpFileGlobal::SN_BLOCK_SIZE;
  const int64_t end_read_offset = 5 * 1024 * 1024 + padding_in_shared_block;
  const int64_t end_page_virtual_id_of_last_read_block = common::upper_align(end_read_offset - padding_in_shared_block, ObTmpFileGlobal::SN_BLOCK_SIZE)
                                                         / ObTmpFileGlobal::PAGE_SIZE - 1
                                                         + ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM;
  // close interval
  const int64_t end_read_block_virtual_id = common::upper_align(end_read_offset, ObTmpFileGlobal::SN_BLOCK_SIZE) / ObTmpFileGlobal::SN_BLOCK_SIZE - 1;
  const int64_t read_size = end_read_offset - start_read_offset;
  ASSERT_LT(start_read_offset, write_size);
  ASSERT_LT(end_read_offset, write_size);
  ASSERT_LT(start_read_offset, end_read_offset);
  ASSERT_GT(read_size, 0);
  ASSERT_EQ(end_read_block_virtual_id - start_read_block_virtual_id + 1, 3);

  char *read_buf = new char [read_size];
  ObTmpFileIOHandle handle;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = false;
  io_info.prefetch_ = true;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, start_read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + start_read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  memset(read_buf, 0, io_info.size_);
  delete[] read_buf;

  for (int64_t virtual_page_id = 0; virtual_page_id <= end_virtual_page_id; ++virtual_page_id) {
    write_cache_key.virtual_page_id_ = virtual_page_id;
    kv_cache_key.set_page_key(write_cache_key);
    p_handle.reset();
    ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
    if (start_read_page_virtual_id <= virtual_page_id && virtual_page_id <= end_page_virtual_id_of_last_read_block) {
      ASSERT_EQ(OB_SUCCESS, ret);
    } else {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
    }
  }

  read_buf = new char [write_size];
  io_info.buf_ = read_buf;
  io_info.size_ = write_size;
  io_info.disable_page_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, 0, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  memset(read_buf, 0, io_info.size_);
  delete[] read_buf;

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();
  delete[] write_buf;
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

  // 1. write 2KB data and check rightness of writing
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 2 * 1024; // 2KB
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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

  // 3. forcibly evict current page from write cache and kv cache
  flush_all_page();
  ObTmpFileWriteCacheKey write_cache_key;
  write_cache_key.type_ = PageType::DATA;
  write_cache_key.fd_ = fd;
  write_cache_key.virtual_page_id_ = 0;
  ObTmpFilePageHandle page_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_page(write_cache_key, page_handle));

  ObTmpPageCacheKey kv_cache_key;
  kv_cache_key.set_tenant_id(MTL_ID());
  kv_cache_key.set_page_key(write_cache_key);
  // TODO: wanyue.wy
  // when support the file attribute for disable page cache,
  // remove the 'erase()' and use the attribute to avoid putting data into cache,
  ObTmpPageCache::get_instance().erase(kv_cache_key);
  ObTmpPageValueHandle p_handle;
  ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  p_handle.reset();

  // 4. read disk page and add it into kv_cache
  read_offset = 5;
  read_size = already_write_size - 2 * read_offset; // 4KB - 10B
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  // 5. append write 6KB data in memory and check rightness of writing.
  //    then, check the first page in write cache and kv cache whether is different
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

  ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = memcmp(p_handle.value_->get_buffer(), read_buf, ObTmpFileGlobal::PAGE_SIZE);
  ASSERT_NE(0, cmp);
  p_handle.reset();
  handle.reset();
  delete[] read_buf;

  // 6. forcibly evict all pages and read them from kv cache to check whether hit old cached page in kv_cache
  // TODO: wanyue.wy
  // when we disable file attribute for disable page cache, enable it before flush all pages
  flush_all_page();
  page_handle.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_page(write_cache_key, page_handle));

  ret = ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  cmp = memcmp(p_handle.value_->get_buffer(), write_buf, ObTmpFileGlobal::PAGE_SIZE);
  ASSERT_EQ(0, cmp);

  read_offset = 20;
  read_size = write_size - read_offset; // 10KB - 20B
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  io_info.disable_page_cache_ = false;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();
  delete[] write_buf;
  STORAGE_LOG(INFO, "=======================test_write_tail_page end=======================");
}

int check_page_existence(const int64_t fd, const int64_t virtual_page_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObTmpFileWriteCacheKey write_cache_key;
  write_cache_key.type_ = PageType::DATA;
  write_cache_key.fd_ = fd;
  write_cache_key.virtual_page_id_ = virtual_page_id;
  ObTmpFilePageHandle page_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_page(write_cache_key, page_handle);
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    // exist = false;
  } else if (OB_SUCCESS == ret) {
    exist = true;
  }
  return ret;
}

// 1. truncate special cases
// 2. truncate disk data (write some pages and flush them, then truncate all)
// 3. truncate memory data and disk data (write pages util trigger flushing, then truncate all)
// 4. truncate() do nothing (truncate_offset < file's truncate_offset_)
// 5. invalid truncate_offset checking
TEST_F(TestTmpFile, test_tmp_file_truncate)
{
  STORAGE_LOG(INFO, "=======================test_tmp_file_truncate begin=======================");
  int ret = OB_SUCCESS;
  const int64_t data_size = 30 * 1024 * 1024; // 30MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_memory_limit();
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

  // 1. truncate special cases
  // 1.1 write two pages and check rightness of writing
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
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  MEMSET(read_buf, 0, read_size);
  LOG_INFO("============================= test_tmp_file_truncate 1.1 end =============================");
  // 1.2 truncate to the middle offset of the first page
  int64_t truncate_offset = ObTmpFileGlobal::PAGE_SIZE / 2;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  bool exist = false;
  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 0, exist));
  ASSERT_EQ(exist, true);
  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 1, exist));
  ASSERT_EQ(exist, true);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  MEMSET(read_buf, 0, read_size);

  LOG_INFO("============================= test_tmp_file_truncate 1.2 end =============================");
  // 1.3 truncate the first page
  truncate_offset = ObTmpFileGlobal::PAGE_SIZE;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 0, exist));
  ASSERT_EQ(exist, false);
  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 1, exist));
  ASSERT_EQ(exist, true);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  MEMSET(read_buf, 0, read_size);

  LOG_INFO("============================= test_tmp_file_truncate 1.3 end =============================");
  // 1.4 truncate whole pages
  truncate_offset = already_write_size;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 0, exist));
  ASSERT_EQ(exist, false);
  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 1, exist));
  ASSERT_EQ(exist, false);

  // read_offset = 0;
  // read_size = already_write_size;
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  LOG_INFO("============================= test_tmp_file_truncate 1.4 end =============================");
  // 2. truncate disk data (write some pages and flush them, then truncate all)
  read_offset = already_write_size;
  io_info.buf_ = write_buf + already_write_size;
  io_info.size_ = 4 * 1024 * 1024;    // 4MB
  ASSERT_LT(already_write_size + io_info.size_, data_size);
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  flush_all_page();

  truncate_offset = already_write_size;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  read_size = io_info.size_;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  LOG_INFO("============================= test_tmp_file_truncate 2 end =============================");
  // 3. truncate memory data and disk data (write pages util trigger flushing, then truncate all)
  // 3.1 write remain data and acquire page distribution
  io_info.buf_ = write_buf + already_write_size;
  io_info.size_ = data_size - already_write_size;
  ASSERT_GT(io_info.size_, 0);
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  already_write_size += io_info.size_;

  ObArray<int64_t> special_pages;// the page is in write cache, but the previous page is in disk
  int64_t end_virtual_page_id = common::upper_align(data_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1;
  bool previous_page_in_cache = false;
  bool cur_page_in_cache = false;
  ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, 0, previous_page_in_cache));
  for (int64_t virtual_page_id = 1; virtual_page_id <= end_virtual_page_id; ++virtual_page_id) {
    ASSERT_EQ(OB_SUCCESS, check_page_existence(fd, virtual_page_id, cur_page_in_cache));
    if (cur_page_in_cache == true && previous_page_in_cache == false) {
      ASSERT_EQ(OB_SUCCESS, special_pages.push_back(virtual_page_id));
    }
    previous_page_in_cache = cur_page_in_cache;
  }

  LOG_INFO("============================= test_tmp_file_truncate 3.1 end =============================");
  // 3.2 truncate_offset is unaligned
  truncate_offset = special_pages.at(special_pages.count()/2) * ObTmpFileGlobal::PAGE_SIZE + ObTmpFileGlobal::PAGE_SIZE / 2;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  read_offset = truncate_offset / 2;
  read_size = data_size - read_offset;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  LOG_INFO("============================= test_tmp_file_truncate 3.2 end =============================");
  // 3.3 truncate_offset is aligned
  read_offset = truncate_offset;
  truncate_offset = (truncate_offset + data_size) / 2;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset);
  ASSERT_EQ(OB_SUCCESS, ret);
  MEMSET(write_buf, 0, truncate_offset);

  read_size = data_size - read_offset;
  read_buf = new char [read_size];
  io_info.buf_ = read_buf;
  io_info.size_ = read_size;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_size, handle.get_done_size());
  cmp = memcmp(handle.get_buffer(), write_buf + read_offset, read_size);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;

  LOG_INFO("============================= test_tmp_file_truncate 3.3 end =============================");
  // 4. truncate() do nothing (truncate_offset < file's truncate_offset_)
  tmp_file::ObITmpFileHandle file_handle;
  ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, truncate_offset / 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(truncate_offset, file_handle.get()->truncated_offset_);
  file_handle.reset();

  LOG_INFO("============================= test_tmp_file_truncate 4 end =============================");
  // 5. invalid truncate_offset checking
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, -1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd, data_size + 10);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_final_status();
  delete[] write_buf;
  STORAGE_LOG(INFO, "=======================test_tmp_file_truncate end=======================");
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

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  check_final_status();
  delete[] write_buf;
  STORAGE_LOG(INFO, "=======================test_aio_pread end=======================");
}

// generate 16MB random data for four files. (total 64MB)
// 1. the first three files write and read 1020KB data (will not trigger flushing)
// 2. the 4th file writes and reads 3MB+1020KB data (will trigger flushing in the processing of writing)
// 3. the first three files write and read 1MB data 3 times (total 3MB)
// 4. each file read and write 12MB+4KB data
void test_multi_file_single_thread_read_write()
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = 64 * 1024 * 1024; // 64MB
  const int64_t wbp_mem_limit = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.get_memory_limit();
  ASSERT_GT(buf_size, wbp_mem_limit);
  char *write_buf = new char [buf_size];
  for (int64_t i = 0; i < buf_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < buf_size; ++j) {
      write_buf[i + j] = random_int;
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
    write_bufs[i] = write_buf + i * buf_size / file_num;
    tmp_file::ObITmpFileHandle file_handle;
    ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    file_handle.reset();
  }
  ObTmpFileIOInfo io_info;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
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
  delete[] write_buf;
}

TEST_F(TestTmpFile, test_multi_file_single_thread_read_write)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write begin=======================");
  test_multi_file_single_thread_read_write();
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_multi_file_single_thread_read_write end=======================");
}

TEST_F(TestTmpFile, test_single_file_multi_thread_read_write)
{
  STORAGE_LOG(INFO, "=======================test_single_file_multi_thread_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_thread_cnt = 1;
  const int64_t read_thread_cnt = 4;
  const int64_t file_cnt = 1;
  const int64_t batch_size = 64 * 1024 * 1024; // 64MB
  const int64_t batch_num = 4;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, write_thread_cnt, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num);
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
  const int64_t write_thread_cnt = 1;
  const int64_t read_thread_cnt = 4;
  const int64_t file_cnt = 4;
  const int64_t batch_size = 16 * 1024 * 1024; // 16MB
  const int64_t batch_num = 4;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, write_thread_cnt, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;
  check_final_status();

  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multi_file_multi_thread_read_write end=======================");
}

TEST_F(TestTmpFile, test_more_files_more_threads_read_write)
{
  STORAGE_LOG(INFO, "=======================test_more_files_more_threads_read_write begin=======================");
  int ret = OB_SUCCESS;
  const int64_t write_thread_cnt = 1;
  const int64_t read_thread_cnt = 2;
  const int64_t file_cnt = 128;
  const int64_t batch_size = 3 * 1024 * 1024;
  const int64_t batch_num = 2; // total 128 * 3MB * 2 = 768MB
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, write_thread_cnt, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num);
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
  const int64_t write_thread_cnt = 1;
  const int64_t read_thread_cnt = 2;
  const int64_t file_cnt = 256;
  const int64_t batch_size = 10 * 1024; // 10KB
  const int64_t batch_num = 3;
  TestMultiTmpFileStress test(MTL_CTX());
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = test.init(file_cnt, dir, write_thread_cnt, read_thread_cnt, IO_WAIT_TIME_MS, batch_size, batch_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t io_time = ObTimeUtility::current_time();
  test.start();
  test.wait();
  io_time = ObTimeUtility::current_time() - io_time;
  check_final_status();

  STORAGE_LOG(INFO, "io time", K(io_time));
  STORAGE_LOG(INFO, "=======================test_multiple_small_files end=======================");
}

TEST_F(TestTmpFile, test_tmp_file_disk_fragmentation)
{
  STORAGE_LOG(INFO, "=======================test_tmp_file_disk_fragmentation begin=======================");
  int ret = OB_SUCCESS;
  const int64_t file_cnt = 1024;
  const int64_t write_size = 12 * 1024; // 12KB
  char *write_buf = new char[write_size];
  for (int64_t i = 0; i < write_size; ++i) {
    write_buf[i] = i % 256;
  }
  ObArray<int64_t> fds;
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < file_cnt; ++i) {
    int64_t fd = -1;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    fds.push_back(fd);
    ASSERT_EQ(OB_SUCCESS, ret);
    tmp_file::ObITmpFileHandle file_handle;
    ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.io_desc_.set_wait_event(2);
    io_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    io_info.buf_ = write_buf;
    io_info.size_ = write_size;
    ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  flush_all_page();

  ObSNTenantTmpFileManager &file_mgr = MTL(ObTenantTmpFileManager *)->get_sn_file_manager();
  ObTmpFileBlockManager &block_mgr = file_mgr.tmp_file_block_manager_;
  block_mgr.print_block_usage();

  int64_t flushed_page_num = 0;
  int64_t macro_block_count = 0;
  block_mgr.get_block_usage_stat(flushed_page_num, macro_block_count);
  // we pre-allocate 4 pages for each 12KB file
  // therefore block count should be no more than actual_block_count * 2
  ASSERT_LE(macro_block_count, 32);
  ASSERT_LE(block_mgr.block_map_.count(), 32);

  delete[] write_buf;
  for (int64_t i = 0; i < fds.count(); ++i) {
    ret = MTL(ObTenantTmpFileManager *)->remove(fds.at(i));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  STORAGE_LOG(INFO, "=======================test_tmp_file_disk_fragmentation end=======================");
}

void test_big_file(const int64_t write_size, const int64_t wbp_mem_limit, ObTmpFileIOInfo io_info)
{
  int ret = OB_SUCCESS;
  ASSERT_GT(write_size, wbp_mem_limit);
  MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.default_memory_limit_ = wbp_mem_limit;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  int cmp = 0;
  char *write_buf = new char[write_size];
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
  // const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  // int64_t page_cnt = write_size / PAGE_SIZE;
  // int64_t read_size = ObTmpFileGlobal::PAGE_SIZE;
  // EXPECT_EQ(0, write_size % PAGE_SIZE);
  // for (int i = 0; i < page_cnt; ++i) {
  //   char *read_buf = new char [PAGE_SIZE];
  //   io_info.buf_ = read_buf;
  //   io_info.size_ = read_size;
  //   ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  //   cmp = memcmp(handle.get_buffer(), write_buf + i * PAGE_SIZE, handle.get_done_size());
  //   ASSERT_EQ(read_size, handle.get_done_size());
  //   if (cmp) {
  //     LOG_INFO("data not match", K(i));
  //     dump_hex_data(write_buf, write_size, "write_buffer.txt");
  //     dump_hex_data(handle.get_buffer(), read_size, "read_data_" + std::to_string(i) + ".txt");
  //     printf("data not match, abort\n");
  //     ob_abort();
  //   }
  //   ASSERT_EQ(0, cmp);
  //   handle.reset();
  //   memset(read_buf, 0, read_size);
  //   delete[] read_buf;
  // }
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
  delete[] read_buf;

  // 4. pread 2MB
  int64_t read_offset = 100;
  read_size = macro_block_size;
  read_buf = new char [macro_block_size];
  io_info.buf_ = read_buf;
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
  delete[] read_buf;

  // 6. pread data which has been read to use kv_cache
  if (!io_info.disable_page_cache_) {
    flush_all_page();
    const int64_t begin_block_id = upper_align(write_size, macro_block_size) / macro_block_size / 4;
    const int64_t end_block_id = upper_align(write_size, macro_block_size) / macro_block_size / 4 * 3;
    const int block_num = end_block_id - begin_block_id;
    LOG_INFO("start to pread disk data", K(begin_block_id), K(end_block_id), K(block_num),
                                         K(file_handle.get()->file_size_), K(write_size));
    for (int i = 0; i < block_num; ++i) {
      read_offset = MIN(macro_block_size * (begin_block_id + i), file_handle.get()->file_size_-1);
      read_size = MIN(macro_block_size * 2, file_handle.get()->file_size_ - read_offset);
      read_buf = new char [read_size];
      io_info.buf_ = read_buf;
      io_info.size_ = read_size;
      ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(read_size, handle.get_done_size());
      cmp = memcmp(handle.get_buffer(), write_buf + read_offset, handle.get_done_size());
      handle.reset();
      ASSERT_EQ(0, cmp);
      memset(read_buf, 0, read_size);
      delete[] read_buf;
    }
  }
  read_time = ObTimeUtility::current_time() - read_time;

  delete[] write_buf;

  file_handle.reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  STORAGE_LOG(INFO, "test_big_file", K(io_info.disable_page_cache_));
  STORAGE_LOG(INFO, "io time", K(write_time), K(read_time));
}

TEST_F(TestTmpFile, test_big_file_with_small_wbp)
{
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp begin=======================");
  const int64_t write_size = 150 * 1024 * 1024;  // write 150MB data
  const int64_t wbp_mem_limit = SMALL_WRITE_CACHE_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp end=======================");
}

TEST_F(TestTmpFile, test_big_file_with_small_wbp_disable_page_cache)
{
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp_disable_page_cache begin=======================");
  const int64_t write_size = 150 * 1024 * 1024;  // write 150MB data
  const int64_t wbp_mem_limit = SMALL_WRITE_CACHE_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = false;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_with_small_wbp_disable_page_cache end=======================");
}

// ATTENTION
// the case after this will increase wbp_mem_limit to BIG_WRITE_CACHE_MEM_LIMIT.
// And it will never be decreased as long as it has been increased
TEST_F(TestTmpFile, test_big_file)
{
  STORAGE_LOG(INFO, "=======================test_big_file begin=======================");
  const int64_t write_size = 750 * 1024 * 1024;  // write 750MB data
  const int64_t wbp_mem_limit = BIG_WRITE_CACHE_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = false;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file end=======================");
}

TEST_F(TestTmpFile, test_big_file_disable_page_cache)
{
  STORAGE_LOG(INFO, "=======================test_big_file_disable_page_cache begin=======================");
  const int64_t write_size = 750 * 1024 * 1024;  // write 750MB data
  const int64_t wbp_mem_limit = BIG_WRITE_CACHE_MEM_LIMIT;
  ObTmpFileIOInfo io_info;
  io_info.disable_page_cache_ = true;
  test_big_file(write_size, wbp_mem_limit, io_info);
  check_final_status();
  STORAGE_LOG(INFO, "=======================test_big_file_disable_page_cache end=======================");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_sn_tmp_file.log*");
  OB_LOGGER.set_file_name("test_sn_tmp_file.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
