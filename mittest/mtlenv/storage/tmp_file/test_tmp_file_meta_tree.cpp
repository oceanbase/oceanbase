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

#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include <random>

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;

static const int64_t WRITE_CACHE_MEMORY_LIMIT = 20 * 1024L * 1024L;

static const int64_t timeout_ms = 10 * 1000; /*10s*/

/* --------------------------------- Mock META TREE---------------------------------- */
class ObTmpFileTestMetaTree : public ObSharedNothingTmpFileMetaTree
{
public:
  ObTmpFileTestMetaTree() : release_pages_(),
                            read_cache_rightmost_pages_(),
                            alloc_range_index_(0) {}
  ~ObTmpFileTestMetaTree();
  void reset();
private:
  virtual int release_tmp_file_page_(const int64_t block_index,
                                     const int64_t begin_page_id, const int64_t page_num);
  virtual int cache_page_for_write_(const uint32_t parent_page_id,
                                    ObSharedNothingTmpFileMetaItem &page_info);
  int alloc_page_entry(ObIArray<ObTmpFileBlockRange> &page_entry_arr);
public:
  ObArray<uint32_t> release_pages_;
  //mock rightmost pages in read_cache, page_level is the index
  //first means page buffer in read_cache
  //second means read count in read_cache
  ObArray<std::pair<char *, int16_t>> read_cache_rightmost_pages_;
  int64_t alloc_range_index_;
};

ObTmpFileTestMetaTree::~ObTmpFileTestMetaTree()
{
  reset();
}

void ObTmpFileTestMetaTree::reset()
{
  release_pages_.reset();
  alloc_range_index_ = 0;
}

int ObTmpFileTestMetaTree::alloc_page_entry(ObIArray<ObTmpFileBlockRange> &page_range_arr)
{
  int ret = OB_SUCCESS;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int64_t> dis(1, 3);
  int64_t random_cnt = dis(gen);
  for (int64_t i = 0; i < random_cnt; i++) {
    if (alloc_range_index_ > 20) {
      alloc_range_index_ = 0;
    }
    ObTmpFileBlockRange range;
    range.block_index_ = alloc_range_index_;
    range.page_index_ = alloc_range_index_;
    range.page_cnt_ = alloc_range_index_ + 1;
    if (OB_FAIL(page_range_arr.push_back(range))) {
      STORAGE_LOG(WARN, "fail to push back", KR(ret), K(fd_), K(range));
    } else {
      alloc_range_index_++;
    }
  }
  return ret;
}

int ObTmpFileTestMetaTree::release_tmp_file_page_(const int64_t block_index,
                                                  const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  stat_info_.all_type_page_released_cnt_ += page_num;
  return ret;
}

int ObTmpFileTestMetaTree::cache_page_for_write_(
    const uint32_t parent_page_id,
    ObSharedNothingTmpFileMetaItem &page_info)
{
  int ret = OB_SUCCESS;
  return ret;
}

static const int64_t SN_BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;
/* ---------------------------- Unittest Class ----------------------------- */
class TestSNTmpFileMetaTree : public ::testing::Test
{
public:
  TestSNTmpFileMetaTree() = default;
  virtual ~TestSNTmpFileMetaTree() = default;
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase()
  {
    const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

    CHUNK_MGR.set_limit(TENANT_MEMORY);
    ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);
    ObMallocAllocator::get_instance()->set_tenant_max_min(MTL_ID(), TENANT_MEMORY, 0);
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
public:
  void generate_data_items(const int64_t item_num,
                           const int64_t start_virtual_page_id,
                           ObArray<ObSharedNothingTmpFileDataItem> &data_items);
  void generate_wrong_data_items(const int64_t item_num,
                                 const int64_t start_virtual_page_id,
                                 ObArray<ObSharedNothingTmpFileDataItem> &data_items);
  void test_tree_flush_with_truncate_occurs_before_update_meta(
       int64_t truncate_offset, bool insert_after_truncate);
};

//mock data items
void TestSNTmpFileMetaTree::generate_data_items(
     const int64_t item_num,
     const int64_t start_virtual_page_id,
     ObArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int64_t block_index = 0;
  int16_t physical_page_id = 0;
  int16_t physical_page_num = 128; //(SN_BLOCK_SIZE / ObTmpFileGlobal::PAGE_SIZE) / 2
  int64_t virtual_page_id = start_virtual_page_id;
  ObSharedNothingTmpFileDataItem data_item;
  for (int64_t i = block_index; i < item_num; i++) {
    data_item.reset();
    data_item.block_index_ = block_index;
    data_item.physical_page_id_ = physical_page_id;
    data_item.physical_page_num_ = physical_page_num;
    data_item.virtual_page_id_ = virtual_page_id;
    block_index++;
    virtual_page_id += physical_page_num;
    ASSERT_EQ(OB_SUCCESS, data_items.push_back(data_item));
  }
}

void TestSNTmpFileMetaTree::generate_wrong_data_items(
     const int64_t item_num,
     const int64_t start_virtual_page_id,
     ObArray<ObSharedNothingTmpFileDataItem> &data_items)
{
  int64_t block_index = 0;
  int16_t physical_page_id = 0;
  int16_t physical_page_num = 128; //(SN_BLOCK_SIZE / ObTmpFileGlobal::PAGE_SIZE) / 2
  int64_t virtual_page_id = start_virtual_page_id;
  ObSharedNothingTmpFileDataItem data_item;
  for (int64_t i = block_index; i < item_num; i++) {
    data_item.reset();
    data_item.block_index_ = block_index;
    data_item.physical_page_id_ = physical_page_id;
    data_item.physical_page_num_ = physical_page_num;
    data_item.virtual_page_id_ = virtual_page_id > 0 ? virtual_page_id - std::rand() % 50  - 1 : 0;
    block_index++;
    virtual_page_id += physical_page_num;
    ASSERT_EQ(OB_SUCCESS, data_items.push_back(data_item));
  }
}

TEST_F(TestSNTmpFileMetaTree, test_tree_insert)
{
  STORAGE_LOG(INFO, "=======================test_tree_insert begin=======================");
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileWriteCache wbp;
  // wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init(&block_manager));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  int64_t item_num = 30;
  STORAGE_LOG(INFO, "=======================first insert=======================");
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  //insert 30 items
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items, timeout_ms));
  STORAGE_LOG(INFO, "level_page_num_array_", K(meta_tree_.level_page_num_array_));
  ASSERT_EQ(4, meta_tree_.level_page_num_array_.count());
  STORAGE_LOG(INFO, "=======================second insert=======================");
  data_items.reset();
  item_num = 20;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 30 * 128, data_items);
  for (int64_t i = 0; i < item_num; i++) {
    data_items_1.reset();
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1, timeout_ms));
  }
  ASSERT_EQ(4, meta_tree_.level_page_num_array_.count());
  ASSERT_EQ(17, meta_tree_.level_page_num_array_.at(0));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 50 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_insert end=======================");
}

// TEST_F(TestSNTmpFileMetaTree, test_tree_insert_fail)
// {
//   STORAGE_LOG(INFO, "=======================test_tree_insert_fail begin=======================");
//   common::ObFIFOAllocator callback_allocator;
//   ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
//                                                 OB_MALLOC_MIDDLE_BLOCK_SIZE,
//                                                 ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
//   ObTmpFileBlockManager block_manager;
//   ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
//   ObTmpFileWriteCache wbp;
//   wbp.default_memory_limit_ = ObTmpFileWriteCache::WBP_BLOCK_SIZE; //253 pages
//   ASSERT_EQ(OB_SUCCESS, wbp.init(&block_manager));
//   ObTmpFileTestMetaTree meta_tree_;
//   ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
//   meta_tree_.set_max_array_item_cnt(2);
//   meta_tree_.set_max_page_item_cnt(3);
//   const int64_t item_num = 750;
//   ObArray<ObSharedNothingTmpFileDataItem> data_items;
//   ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
//   generate_data_items(item_num, 0/*start virtual page id*/, data_items);
//   ASSERT_EQ(item_num, data_items.count());
//   STORAGE_LOG(INFO, "=======================first insert=======================");
//   //insert 750 items (insert a array)
//   ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, meta_tree_.insert_items(data_items));
//   ASSERT_EQ(0, meta_tree_.level_page_num_array_.count());
//   ASSERT_EQ(false, meta_tree_.root_item_.is_valid());
//   ASSERT_EQ(false, meta_tree_.prealloc_info_.is_empty());

//   //we assume that there are some disk flushing and other operations here.

//   STORAGE_LOG(INFO, "=======================second insert=======================");
//   //insert 90 items (insert one by one)
//   for (int64_t i = 0; i < 90; i++) {
//     ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
//   }
//   ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
//   ASSERT_EQ(5, meta_tree_.level_page_num_array_.count());
//   ASSERT_EQ(30, meta_tree_.level_page_num_array_.at(0));
//   ASSERT_EQ(10, meta_tree_.level_page_num_array_.at(1));
//   ASSERT_EQ(4, meta_tree_.level_page_num_array_.at(2));
//   ASSERT_EQ(2, meta_tree_.level_page_num_array_.at(3));
//   ASSERT_EQ(1, meta_tree_.level_page_num_array_.at(4));

//   ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 90 * 128 * ObTmpFileGlobal::PAGE_SIZE));
//   meta_tree_.reset();
//   STORAGE_LOG(INFO, "=======================test_tree_insert_fail end=======================");
// }

TEST_F(TestSNTmpFileMetaTree, test_tree_get_last_data_item)
{
  STORAGE_LOG(INFO, "=======================test_tree_get_last_data_item begin=======================");
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileWriteCache wbp;
  // wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init(&block_manager));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  int64_t item_num = 27;
  STORAGE_LOG(INFO, "=======================insert=======================");
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  //insert 27 items
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items, timeout_ms));
  ASSERT_EQ(3, meta_tree_.level_page_num_array_.count());

  STORAGE_LOG(INFO, "=======================get last data item=======================");
  ObSharedNothingTmpFileDataItem last_data_item;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_last_data_item_write_tail(timeout_ms, last_data_item));
  ASSERT_EQ(26 * 128, last_data_item.virtual_page_id_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 27 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_get_last_data_item end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_read)
{
  STORAGE_LOG(INFO, "=======================test_tree_read begin=======================");
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileWriteCache wbp;
  wbp.default_memory_limit_ = ObTmpFileWriteCache::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init(&block_manager));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  const int64_t item_num = 25;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================tree insert=======================");
  //insert 25 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items, timeout_ms));
  ASSERT_EQ(3, meta_tree_.level_page_num_array_.count());

  //we assume that there are some disk flushing and other operations here.
  //After simulating the insert, we can see that the tmp file offset is [0, 25 * 128 * 8K],
  //  with each data item occupying 128 * 8K.
  STORAGE_LOG(INFO, "=======================first tree read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(1, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE - 2, timeout_ms, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(2, get_data_items.count());

  STORAGE_LOG(INFO, "=======================second tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(7 * 128 * ObTmpFileGlobal::PAGE_SIZE, 5 * 128 * ObTmpFileGlobal::PAGE_SIZE, timeout_ms, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(5, get_data_items.count());

  STORAGE_LOG(INFO, "=======================third tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(0, 4 * 128 * ObTmpFileGlobal::PAGE_SIZE, timeout_ms, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(4, get_data_items.count());

  STORAGE_LOG(INFO, "=======================forth tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE, timeout_ms, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(25, get_data_items.count());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_read end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_truncate)
{
  STORAGE_LOG(INFO, "=======================test_tree_truncate begin=======================");
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileWriteCache wbp;
  wbp.default_memory_limit_ = ObTmpFileWriteCache::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init(&block_manager));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  const int64_t item_num = 75;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 5 items (insert a array)
  for (int64_t i = 0; i < 5; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1, timeout_ms));
  ASSERT_EQ(0, meta_tree_.level_page_num_array_.count());
  ASSERT_EQ(5, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "data_item_array", K(meta_tree_.data_item_array_));

  STORAGE_LOG(INFO, "=======================first tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, 3 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "data_item_array", K(meta_tree_.data_item_array_));
  ASSERT_EQ(2, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  //insert 70 items (insert a array)
  for (int64_t i = 5; i < 75; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2, timeout_ms));
  ASSERT_EQ(3, meta_tree_.level_page_num_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_num_array_));

  STORAGE_LOG(INFO, "=======================second tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(3 * 128 * ObTmpFileGlobal::PAGE_SIZE, (25 * 128 + 64) * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_num_array_));
  ASSERT_EQ(25 * 128 + 4, meta_tree_.stat_info_.all_type_page_released_cnt_);

  STORAGE_LOG(INFO, "=======================third tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((25 * 128 + 64) * ObTmpFileGlobal::PAGE_SIZE, 28 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_num_array_));
  ASSERT_EQ(28 * 128 + 6, meta_tree_.stat_info_.all_type_page_released_cnt_);

  STORAGE_LOG(INFO, "=======================first tree read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(28 * 128 * ObTmpFileGlobal::PAGE_SIZE, 20 * 128 * ObTmpFileGlobal::PAGE_SIZE, timeout_ms, get_data_items));
  ASSERT_EQ(20, get_data_items.count());
  ASSERT_EQ(28 * 128, get_data_items.at(0).virtual_page_id_);

  STORAGE_LOG(INFO, "=======================forth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(28 * 128 * ObTmpFileGlobal::PAGE_SIZE, 73 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_num_array_));
  ASSERT_EQ(73 * 128 + 16, meta_tree_.stat_info_.all_type_page_released_cnt_);

  STORAGE_LOG(INFO, "=======================fifth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(73 * 128 * ObTmpFileGlobal::PAGE_SIZE, 75 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_num_array_));
  ASSERT_EQ(75 * 128 + 19, meta_tree_.stat_info_.all_type_page_released_cnt_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(75 * 128 * ObTmpFileGlobal::PAGE_SIZE, 75 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_truncate end=======================");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_sn_tmp_file_meta_tree.log*");
  OB_LOGGER.set_file_name("test_sn_tmp_file_meta_tree.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
