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
#define protected public
#define private public
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_meta_tree.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_compress_tmp_file_manager.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;
/* ------------------------------ Mock Parameter ---------------------------- */
static const int64_t TENANT_MEMORY = 8L * 1024L * 1024L * 1024L /* 8 GB */;
/********************************* Mock WBP *************************** */
// static const int64_t WBP_BLOCK_SIZE = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; // each wbp block has 253 pages (253 * 8KB == 2024KB)
// static const int64_t SMALL_WBP_BLOCK_COUNT = 3;
// static const int64_t SMALL_WBP_MEM_LIMIT = SMALL_WBP_BLOCK_COUNT * WBP_BLOCK_SIZE; // the wbp mem size is 5.93MB
// static const int64_t BIG_WBP_BLOCK_COUNT = 40;
// static const int64_t BIG_WBP_MEM_LIMIT = BIG_WBP_BLOCK_COUNT * WBP_BLOCK_SIZE; // the wbp mem size is 79.06MB
/********************************* Mock WBP Index Cache*************************** */
// each bucket could indicate a 256KB data in wbp.
// SMALL_WBP_IDX_CACHE_MAX_CAPACITY will indicate 4MB data in wbp
// static const int64_t SMALL_WBP_IDX_CACHE_MAX_CAPACITY = ObTmpFileWBPIndexCache::INIT_BUCKET_ARRAY_CAPACITY * 2;
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

class TestCompressTmpFile : public ::testing::Test
{
public:
  TestCompressTmpFile() = default;
  virtual ~TestCompressTmpFile() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};
static ObSimpleMemLimitGetter getter;

// ATTENTION!
// currently, we only initialize modules about tmp file at the beginning of unit test and
// never restart them in the end of test case.
// please make sure that all test cases will not affect the others.
void TestCompressTmpFile::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_max_min(MTL_ID(), TENANT_MEMORY, 0);

  // MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.default_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  ObSharedNothingTmpFileMetaTree::set_max_array_item_cnt(MAX_DATA_ITEM_ARRAY_COUNT);
  ObSharedNothingTmpFileMetaTree::set_max_page_item_cnt(MAX_PAGE_ITEM_COUNT);
}

void TestCompressTmpFile::SetUp()
{
  int ret = OB_SUCCESS;

  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  if (!ObKVGlobalCache::get_instance().inited_) {
    // ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(&getter,
    //     bucket_num,
    //     max_cache_size,
    //     block_size));
  }
//  if (!MTL(ObTenantTmpFileManager *)->is_inited_) {
//    ret = MTL(ObTenantTmpFileManager *)->init();
//    ASSERT_EQ(OB_SUCCESS, ret);
//    ret = MTL(ObTenantTmpFileManager *)->start();
//    ASSERT_EQ(OB_SUCCESS, ret);
//    MTL(ObTenantTmpFileManager *)->get_sn_file_manager().write_cache_.default_memory_limit_ = SMALL_WBP_MEM_LIMIT;
//  }
}

void TestCompressTmpFile::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCompressTmpFile::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
//  if (MTL(ObTenantTmpFileManager *)->is_inited_) {
//    MTL(ObTenantTmpFileManager *)->stop();
//    MTL(ObTenantTmpFileManager *)->wait();
//    MTL(ObTenantTmpFileManager *)->destroy();
//  }
}

TEST_F(TestCompressTmpFile, test_aligned_read_write)
{
  STORAGE_LOG(INFO, "=======================test_aligned_read_write begin=======================");

  STORAGE_LOG(INFO, "=======================test_aligned_read_write end=======================");
}

TEST_F(TestCompressTmpFile, test_multi_aligned_read_write)
{
  STORAGE_LOG(INFO, "=======================test_multi_aligned_read_write begin=======================");

  STORAGE_LOG(INFO, "=======================test_multi_aligned_read_write end=======================");
}

TEST_F(TestCompressTmpFile, test_unaligned_read_write)
{
  STORAGE_LOG(INFO, "=======================test_unaligned_read_write begin=======================");

  STORAGE_LOG(INFO, "=======================test_unaligned_read_write end=======================");
}

TEST_F(TestCompressTmpFile, test_multi_file_read_write)
{
  STORAGE_LOG(INFO, "=======================test_multi_file_read_write begin=======================");

  STORAGE_LOG(INFO, "=======================test_multi_file_read_write end=======================");
}


} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_bunch_comp_tmp_file.log*");
  OB_LOGGER.set_file_name("test_bunch_comp_tmp_file.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
