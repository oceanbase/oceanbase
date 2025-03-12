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

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;

/* --------------------------------- Mock META TREE---------------------------------- */
class ObTmpFileTestMetaTree : public ObSharedNothingTmpFileMetaTree
{
public:
  ObTmpFileTestMetaTree() : release_pages_() {}
  ~ObTmpFileTestMetaTree();
  void reset();
private:
  virtual int release_meta_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                 const int32_t page_index_in_level);
  virtual int release_tmp_file_page_(const int64_t block_index,
                                     const int64_t begin_page_id, const int64_t page_num);
  virtual int cache_page_for_write_(const uint32_t parent_page_id,
                                    ObSharedNothingTmpFileMetaItem &page_info);
public:
  ObArray<uint32_t> release_pages_;
  //mock rightmost pages in read_cache, page_level is the index
  //first means page buffer in read_cache
  //second means read count in read_cache
  ObArray<std::pair<char *, int16_t>> read_cache_rightmost_pages_;
};

ObTmpFileTestMetaTree::~ObTmpFileTestMetaTree()
{
  reset();
}

void ObTmpFileTestMetaTree::reset()
{
  release_pages_.reset();
}

int ObTmpFileTestMetaTree::release_meta_page_(const ObSharedNothingTmpFileMetaItem &page_info,
                                              const int32_t page_index_in_level)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(level_page_range_array_.count() <= page_info.page_level_
                  || 0 >  page_index_in_level)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected level_page_range_array_ or page_info", KR(ret), K(fd_), K(level_page_range_array_),
                                                                          K(page_info), K(page_index_in_level));
  } else if (is_page_in_write_cache(page_info)) {
    const uint32_t start_page_id_in_array = level_page_range_array_.at(page_info.page_level_).start_page_id_;
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (OB_FAIL(wbp_->free_page(fd_, page_info.buffer_page_id_,
                ObTmpFilePageUniqKey(page_info.page_level_, page_index_in_level), next_page_id))) {
      STORAGE_LOG(WARN, "fail to free meta page in write cache", KR(ret), K(fd_), K(page_info), K(page_index_in_level));
    } else if (FALSE_IT(stat_info_.meta_page_free_cnt_++)) {
    } else if (OB_UNLIKELY(start_page_id_in_array != page_info.buffer_page_id_)) {
      //NOTE: pages must be released sequentially (from front to back in array)
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected level_page_range_array_", KR(ret), K(fd_), K(level_page_range_array_), K(page_info));
    } else {
      level_page_range_array_.at(page_info.page_level_).start_page_id_ = next_page_id;
      if (start_page_id_in_array == level_page_range_array_.at(page_info.page_level_).end_page_id_) {
        //next_page_id must be invalid
        level_page_range_array_.at(page_info.page_level_).end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      }
      if (ObTmpFileGlobal::INVALID_PAGE_ID == level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_) {
        level_page_range_array_.at(page_info.page_level_).flushed_page_num_ += 1;
      }
      if (start_page_id_in_array == level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_) {
        level_page_range_array_.at(page_info.page_level_).flushed_end_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
      }
      level_page_range_array_.at(page_info.page_level_).cached_page_num_ -= 1;
      level_page_range_array_.at(page_info.page_level_).evicted_page_num_ += 1;
    }
  }
  if (OB_SUCC(ret) && is_page_flushed(page_info)) {
    if (OB_FAIL(release_tmp_file_page_(page_info.block_index_, page_info.physical_page_id_, 1))) {
      STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(page_info));
    }
  }
  if (FAILEDx(release_pages_.push_back(page_info.buffer_page_id_))) {
    STORAGE_LOG(WARN, "fail to push_back", KR(ret), K(page_info));
  }
  return ret;
}

int ObTmpFileTestMetaTree::release_tmp_file_page_(const int64_t block_index,
                                                  const int64_t begin_page_id, const int64_t page_num)
{
  int ret = OB_SUCCESS;
  stat_info_.all_type_flush_page_released_cnt_ += page_num;
  return ret;
}

int ObTmpFileTestMetaTree::cache_page_for_write_(
    const uint32_t parent_page_id,
    ObSharedNothingTmpFileMetaItem &page_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!page_info.is_valid()
                  || page_info.page_level_ >= level_page_range_array_.count()
                  || (ObTmpFileGlobal::INVALID_PAGE_ID != parent_page_id
                      && page_info.page_level_ + 1 >= level_page_range_array_.count()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(fd_), K(page_info), K(level_page_range_array_), K(parent_page_id));
  } else {
    if (!is_page_in_write_cache(page_info)) {
      uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      char *new_page_buff = NULL;
      int32_t level_page_index = level_page_range_array_[page_info.page_level_].evicted_page_num_ - 1;
      ObTmpFilePageUniqKey page_key(page_info.page_level_, level_page_index);
      if (OB_UNLIKELY(!is_page_flushed(page_info)
                      || 0 < level_page_range_array_[page_info.page_level_].cached_page_num_
                      || 0 > level_page_index
                      || ObTmpFileGlobal::INVALID_PAGE_ID != level_page_range_array_[page_info.page_level_].end_page_id_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected page_info", KR(ret), K(fd_), K(page_info), K(level_page_range_array_));
      } else if (OB_FAIL(wbp_->alloc_page(fd_, page_key, new_page_id, new_page_buff))) {
        STORAGE_LOG(WARN, "fail to alloc meta page", KR(ret), K(fd_), K(page_info), K(level_page_range_array_));
      } else if (FALSE_IT(stat_info_.meta_page_alloc_cnt_++)) {
      } else if (OB_ISNULL(new_page_buff)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null page buff", KR(ret), K(fd_), KP(new_page_buff));
      } else if (OB_FAIL(wbp_->notify_load(fd_, new_page_id, page_key))) {
        STORAGE_LOG(WARN, "fail to notify load for meta", KR(ret), K(fd_), K(new_page_id), K(page_key));
      } else {
        if (OB_UNLIKELY(read_cache_rightmost_pages_.count() <= page_info.page_level_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected read_cache_rightmost_pages_", KR(ret), K(read_cache_rightmost_pages_.count()), K(page_info));
        } else {
          MEMCPY(new_page_buff, read_cache_rightmost_pages_.at(page_info.page_level_).first, ObTmpFileGlobal::PAGE_SIZE);
          read_cache_rightmost_pages_.at(page_info.page_level_).second++;
        }
        if (FAILEDx(check_page_(new_page_buff))) {
          STORAGE_LOG(WARN, "the page is invalid or corrupted", KR(ret), K(fd_), KP(new_page_buff));
        }
        if (OB_SUCC(ret)) {
          //change page state to cached
          if (OB_FAIL(wbp_->notify_load_succ(fd_, new_page_id, page_key))) {
            STORAGE_LOG(WARN, "fail to notify load succ for meta", KR(ret), K(fd_), K(new_page_id), K(page_key));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          //change page state to invalid
          if (OB_TMP_FAIL(wbp_->notify_load_fail(fd_, new_page_id, page_key))) {
            STORAGE_LOG(WARN, "fail to notify load fail for meta", KR(tmp_ret), K(fd_), K(new_page_id), K(page_key));
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t origin_block_index = page_info.block_index_;
        int16_t origin_physical_page_id = page_info.physical_page_id_;
        page_info.buffer_page_id_ = new_page_id;
        page_info.block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
        page_info.physical_page_id_ = -1;
        if (ObTmpFileGlobal::INVALID_PAGE_ID == parent_page_id) {
          root_item_ = page_info;
        } else {
          char *parent_page_buff = NULL;
          uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
          ObSharedNothingTmpFileTreePageHeader page_header;
          int32_t parent_level_page_index = level_page_range_array_[page_info.page_level_ + 1].evicted_page_num_
                                              + level_page_range_array_[page_info.page_level_ + 1].cached_page_num_ - 1;
          ObTmpFilePageUniqKey parent_page_offset(page_info.page_level_ + 1, parent_level_page_index);
          if (OB_FAIL(wbp_->read_page(fd_, parent_page_id, parent_page_offset, parent_page_buff, next_page_id))) {
            STORAGE_LOG(WARN, "fail to read from write cache", KR(ret), K(fd_), K(parent_page_id), K(parent_page_offset));
          } else if (OB_FAIL(read_page_header_(parent_page_buff, page_header))) {
            STORAGE_LOG(WARN, "fail to read page header", KR(ret), K(fd_), KP(parent_page_buff));
          } else if (OB_FAIL(rewrite_item_(parent_page_buff, page_header.item_num_ - 1, page_info))) {
            STORAGE_LOG(WARN, "fail to rewrite item", KR(ret), K(fd_), K(page_header), K(page_info), KP(parent_page_buff));
          } else if (OB_FAIL(wbp_->notify_dirty(fd_, parent_page_id, parent_page_offset))) {
            STORAGE_LOG(WARN, "fail to notify dirty for meta", KR(ret), K(fd_), K(parent_page_id), K(parent_page_offset));
          }
        }
        if (OB_SUCC(ret)) {
          int16_t page_level = page_info.page_level_;
          if (OB_FAIL(wbp_->notify_dirty(fd_, new_page_id, page_key))) {
            STORAGE_LOG(WARN, "fail to notify dirty", KR(ret), K(fd_), K(new_page_id), K(page_key));
          } else if (OB_FAIL(release_tmp_file_page_(origin_block_index, origin_physical_page_id, 1))) {
            STORAGE_LOG(WARN, "fail to release tmp file page", KR(ret), K(fd_), K(origin_block_index), K(origin_physical_page_id));
          } else {
            level_page_range_array_[page_level].start_page_id_ = new_page_id;
            level_page_range_array_[page_level].end_page_id_ = new_page_id;
            level_page_range_array_[page_level].cached_page_num_++;
            level_page_range_array_[page_level].evicted_page_num_--;
            level_page_range_array_[page_level].flushed_page_num_--;
          }
        }
      } else if (ObTmpFileGlobal::INVALID_PAGE_ID != new_page_id) { //fail
        uint32_t unused_next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(wbp_->free_page(fd_, new_page_id, page_key, unused_next_page_id))) {
          STORAGE_LOG(WARN, "fail to free meta page", KR(tmp_ret), K(fd_), K(new_page_id), K(page_key));
        } else {
          stat_info_.meta_page_free_cnt_++;
        }
      }
      STORAGE_LOG(INFO, "load page to write cache", KR(ret), K(fd_), K(page_info), K(page_key));
    } else {
      //still in write cache
      //do nothing
    }
  }
  return ret;
}

static const int64_t SN_BLOCK_SIZE = ObTmpFileGlobal::SN_BLOCK_SIZE;
/* ---------------------------- Unittest Class ----------------------------- */
class TestSNTmpFileMetaTree : public ::testing::Test
{
public:
  TestSNTmpFileMetaTree() = default;
  virtual ~TestSNTmpFileMetaTree() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
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

static ObSimpleMemLimitGetter getter;

//TODO: test data_item_array
void TestSNTmpFileMetaTree::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}
void TestSNTmpFileMetaTree::SetUp()
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
}

void TestSNTmpFileMetaTree::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSNTmpFileMetaTree::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

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
  ObTmpWriteBufferPool wbp;
  // wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  const int64_t item_num = 30;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_3;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first insert=======================");
  //insert 9 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 9; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  //we assume that there are some disk flushing and other operations here.

  STORAGE_LOG(INFO, "=======================second insert=======================");
  //insert 18 items (insert one by one)
  for (int64_t i = 9; i < 27; i++) {
    data_items_2.reset();
    ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(data_items.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  }
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  //we assume that there are some disk flushing and other operations here.

  STORAGE_LOG(INFO, "=======================third insert=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 27; i < item_num; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_3.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_3));
  ASSERT_EQ(4, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 30 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_insert end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_insert_fail)
{
  STORAGE_LOG(INFO, "=======================test_tree_insert_fail begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  const int64_t item_num = 750;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first insert=======================");
  //insert 750 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, meta_tree_.insert_items(data_items));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(false, meta_tree_.root_item_.is_valid());

  //we assume that there are some disk flushing and other operations here.

  STORAGE_LOG(INFO, "=======================second insert=======================");
  //insert 90 items (insert one by one)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 90; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(5, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(30, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(10, meta_tree_.level_page_range_array_.at(1).cached_page_num_);
  ASSERT_EQ(4, meta_tree_.level_page_range_array_.at(2).cached_page_num_);
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.at(3).cached_page_num_);
  ASSERT_EQ(1, meta_tree_.level_page_range_array_.at(4).cached_page_num_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 90 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_insert_fail end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_read)
{
  STORAGE_LOG(INFO, "=======================test_tree_read begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  //we assume that there are some disk flushing and other operations here.
  //After simulating the insert, we can see that the tmp file offset is [0, 25 * 128 * 8K],
  //  with each data item occupying 128 * 8K.
  STORAGE_LOG(INFO, "=======================first tree read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(1, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE - 2, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(2, get_data_items.count());

  STORAGE_LOG(INFO, "=======================second tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(7 * 128 * ObTmpFileGlobal::PAGE_SIZE, 5 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(5, get_data_items.count());

  STORAGE_LOG(INFO, "=======================third tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(0, 4 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(4, get_data_items.count());

  STORAGE_LOG(INFO, "=======================forth tree read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(25, get_data_items.count());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_read end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush)
{
  STORAGE_LOG(INFO, "=======================test_tree_flush begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  const int64_t item_num = 28;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 25 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 25; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(13, total_need_flush_page_num);
  ASSERT_EQ(3, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================first tree flush=======================");
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context_1;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_1,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array_1));
  ASSERT_EQ(3, tree_io_array_1.count());
  ASSERT_EQ(0 + 13 * ObTmpFileGlobal::PAGE_SIZE, write_offset_1);
  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  //insert 3 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 25; i < 28; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(4, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(5, total_need_flush_page_num);
  ASSERT_EQ(4, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================second tree flush=======================");
  //NOTE: We will not flush the tree again before io returns successfully.
  //So here we assume that io is successful and call this function "update_after_flush".
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context_2;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  int64_t write_offset_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_2,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array_2));
  ASSERT_EQ(4, tree_io_array_2.count());
  //flush 7 dirty pages.
  //During the flushing process,
  //  disk information will be changed to the upper-level pages, so upper-level pages will become dirty pages during this process.
  //  So you shouldn’t be surprised that the number of dirty pages changed from 5 to 7
  ASSERT_EQ(0 + 7 * ObTmpFileGlobal::PAGE_SIZE, write_offset_2);
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));
  for (int64_t i = 0; i < 4; i++) {
    ObTmpFileTreeIOInfo& tree_io = tree_io_array_2.at(i);
    ASSERT_EQ(tree_io.flush_end_page_id_, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
  }
  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 28 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  delete[] block_buff_1;
  delete[] block_buff_2;
  STORAGE_LOG(INFO, "=======================test_tree_flush end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_multi_io)
{
  STORAGE_LOG(INFO, "=======================test_tree_flush_with_multi_io begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(13, total_need_flush_page_num);
  ASSERT_EQ(3, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================tree flush=======================");
  STORAGE_LOG(INFO, "=======================first block=======================");
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - 3 * ObTmpFileGlobal::PAGE_SIZE; //this block can only accommodate 3 pages
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array_1));
  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);

  STORAGE_LOG(INFO, "=======================second block=======================");
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  int64_t write_offset_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array_2));
  ASSERT_EQ(3, tree_io_array_2.count());
  ASSERT_EQ(0 + 10 * ObTmpFileGlobal::PAGE_SIZE, write_offset_2);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 28 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  delete[] block_buff_1;
  delete[] block_buff_2;
  STORAGE_LOG(INFO, "=======================test_tree_flush_with_multi_io end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_major_flush)
{
  STORAGE_LOG(INFO, "=======================test_tree_major_flush begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================tree flush=======================");
  char *block_buff = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array;
  int64_t write_offset = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::MAJOR,
                                                              block_buff,
                                                              write_offset,
                                                              flush_context,
                                                              tree_io_array));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array));
  ASSERT_EQ(2, tree_io_array.count());
  ASSERT_EQ(0 + 10 * ObTmpFileGlobal::PAGE_SIZE, write_offset); //flush 10 pages
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(3, total_need_flush_page_num);
  ASSERT_EQ(3, total_need_flush_rightmost_page_num);
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  delete[] block_buff;
  STORAGE_LOG(INFO, "=======================test_tree_major_flush end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_evict)
{
  STORAGE_LOG(INFO, "=======================test_tree_evict begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================first tree flush=======================");
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context_1;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::MAJOR,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_1,
                                                              tree_io_array_1));
  ASSERT_EQ(2, tree_io_array_1.count());
  ASSERT_EQ(2, tree_io_array_1.at(1).flush_nums_);
  ASSERT_EQ(0 + 10 * ObTmpFileGlobal::PAGE_SIZE, write_offset_1);
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));

  STORAGE_LOG(INFO, "=======================first tree evict=======================");
  int64_t total_need_evict_page_num = 0;
  int64_t total_need_evict_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_evict_page_num(total_need_evict_page_num, total_need_evict_rightmost_page_num));
  ASSERT_EQ(total_need_evict_page_num, 10);
  ASSERT_EQ(total_need_evict_rightmost_page_num, 0);
  int64_t actual_evict_page_num_1 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.evict_meta_pages(13,
                                                    ObTmpFileTreeEvictType::FULL,
                                                    actual_evict_page_num_1));
  ASSERT_EQ(actual_evict_page_num_1, 10);

  STORAGE_LOG(INFO, "=======================second tree flush=======================");
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  ObTmpFileTreeFlushContext flush_context_2;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  int64_t write_offset_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_2,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array", K(tree_io_array_2));
  ASSERT_EQ(3, tree_io_array_2.count());
  ASSERT_EQ(0 + 3 * ObTmpFileGlobal::PAGE_SIZE, write_offset_2);
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));

  STORAGE_LOG(INFO, "=======================second tree evict=======================");
  int64_t actual_evict_page_num_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.evict_meta_pages(3,
                                                    ObTmpFileTreeEvictType::FULL,
                                                    actual_evict_page_num_2));
  ASSERT_EQ(actual_evict_page_num_2, 3);
  for (int64_t i = 0; i < 3; i++) {
    ASSERT_EQ(0, meta_tree_.level_page_range_array_.at(i).cached_page_num_);
    ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(i).start_page_id_);
    ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(i).end_page_id_);
    ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(i).flushed_end_page_id_);
  }

  meta_tree_.reset();
  delete[] block_buff_1;
  delete[] block_buff_2;
  STORAGE_LOG(INFO, "=======================test_tree_evict end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_clear)
{
  STORAGE_LOG(INFO, "=======================test_tree_clear begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================tree clear=======================");
  ObSharedNothingTmpFileMetaItem origin_root_item = meta_tree_.root_item_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 25 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(13, meta_tree_.release_pages_.count());
  ASSERT_EQ(origin_root_item.buffer_page_id_, meta_tree_.release_pages_.at(12));

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_clear end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_truncate)
{
  STORAGE_LOG(INFO, "=======================test_tree_truncate begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 5; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(5, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "data_item_array", K(meta_tree_.data_item_array_));

  STORAGE_LOG(INFO, "=======================first tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, 3 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "data_item_array", K(meta_tree_.data_item_array_));
  ASSERT_EQ(2, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  //insert 70 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 5; i < 75; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================second tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(3 * 128 * ObTmpFileGlobal::PAGE_SIZE, 26 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));
  ASSERT_EQ(4, meta_tree_.release_pages_.count());

  STORAGE_LOG(INFO, "=======================third tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(26 * 128 * ObTmpFileGlobal::PAGE_SIZE, 28 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));
  ASSERT_EQ(6, meta_tree_.release_pages_.count());

  STORAGE_LOG(INFO, "=======================first tree read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(28 * 128 * ObTmpFileGlobal::PAGE_SIZE, 20 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  ASSERT_EQ(20, get_data_items.count());
  ASSERT_EQ(28 * 128, get_data_items.at(0).virtual_page_id_);

  STORAGE_LOG(INFO, "=======================forth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(28 * 128 * ObTmpFileGlobal::PAGE_SIZE, 73 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));
  ASSERT_EQ(16, meta_tree_.release_pages_.count());

  STORAGE_LOG(INFO, "=======================fifth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(73 * 128 * ObTmpFileGlobal::PAGE_SIZE, 75 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));
  ASSERT_EQ(19, meta_tree_.release_pages_.count());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(75 * 128 * ObTmpFileGlobal::PAGE_SIZE, 75 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_truncate end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_truncate_with_unfilled_page)
{
  STORAGE_LOG(INFO, "=======================test_tree_truncate_with_unfilled_page begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
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
  //insert 25 items (insert a array)
  //each data item contains 128 pages, we assume that the last page of the last data item is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 25; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================first tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, (24 * 128 + 1 * 127.5) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(24 * 128 * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(4, meta_tree_.release_pages_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================load unfilled page to write cache=======================");
  ObSharedNothingTmpFileDataItem last_data_item;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true/*release_tail_in_disk*/));

  //then, we write some data in write buffer

  STORAGE_LOG(INFO, "=======================second tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((24 * 128 + 1 * 127.5) * ObTmpFileGlobal::PAGE_SIZE, (24 * 128 + 1 * 127.8) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ((24 * 128 + 1 *127) * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(6, meta_tree_.release_pages_.count());

  STORAGE_LOG(INFO, "=======================third tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((24 * 128 + 1 * 127.8) * ObTmpFileGlobal::PAGE_SIZE, 28 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  ASSERT_EQ((24 * 128 + 1 *127) * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(6, meta_tree_.release_pages_.count());


  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  //insert 47 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 28; i < 75; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(10, meta_tree_.level_page_range_array_[0].evicted_page_num_ + meta_tree_.level_page_range_array_[0].cached_page_num_);
  ASSERT_EQ(2, meta_tree_.level_page_range_array_[1].evicted_page_num_ + meta_tree_.level_page_range_array_[1].cached_page_num_);
  ASSERT_EQ(1, meta_tree_.level_page_range_array_[2].evicted_page_num_ + meta_tree_.level_page_range_array_[2].cached_page_num_);

  STORAGE_LOG(INFO, "=======================forth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(28 * 128 * ObTmpFileGlobal::PAGE_SIZE, 75 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));
  ASSERT_EQ(6 + 13, meta_tree_.release_pages_.count());
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(false, meta_tree_.root_item_.is_valid());
  ASSERT_EQ(75 * 128 * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(75 * 128 * ObTmpFileGlobal::PAGE_SIZE, 80 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_truncate_with_unfilled_page end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_truncate_with_data_item_remove)
{
  STORAGE_LOG(INFO, "=======================test_tree_truncate_with_data_item_remove begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 9;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  //generate last data item, we assume that the page of the last data item is an unfilled page.
  ObSharedNothingTmpFileDataItem last_data_item;
  last_data_item.block_index_ = item_num;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = item_num * 128;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 10 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  for (int64_t i = 0; i < 10; i++) {
    ASSERT_EQ(OB_SUCCESS, data_items_1.push_back(data_items.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  meta_tree_.print_meta_tree_total_info();

  STORAGE_LOG(INFO, "=======================first tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, (9 * 128 + 1 * 0.5) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(9 * 128 * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(1, meta_tree_.release_pages_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================load unfilled page to write cache=======================");
  last_data_item.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true/*release_tail_in_disk*/));

  //then, we write some data in write buffer

  STORAGE_LOG(INFO, "=======================second tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((9 * 128 + 1 * 0.5) * ObTmpFileGlobal::PAGE_SIZE, (9 * 128 + 1 * 0.8) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(9 * 128 * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(3, meta_tree_.release_pages_.count());
  ASSERT_EQ(false, meta_tree_.root_item_.is_valid());

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  item_num = 10;
  generate_data_items(item_num, 9 * 128, data_items_2);
  //generate last data item, we assume that the page of the last data item is an unfilled page.
  last_data_item.reset();
  last_data_item.block_index_ = 30;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = (9 + 10) * 128;
  ASSERT_EQ(OB_SUCCESS, data_items_2.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items_2.count());
  //insert 11 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  // The third leaf page has only one data item (with an unfilled page)
  ASSERT_EQ(3, meta_tree_.level_page_range_array_[0].evicted_page_num_ + meta_tree_.level_page_range_array_[0].cached_page_num_);
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  meta_tree_.print_meta_tree_total_info();

  STORAGE_LOG(INFO, "=======================third tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((9 * 128 + 1 * 0.8) * ObTmpFileGlobal::PAGE_SIZE, (9 * 128 + 10 * 128 + 1 * 0.5) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ((9 * 128 + 10 * 128) * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(3 + 2, meta_tree_.release_pages_.count());
  meta_tree_.print_meta_tree_total_info();

  STORAGE_LOG(INFO, "=======================load unfilled page to write cache=======================");
  last_data_item.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true/*release_tail_in_disk*/));

  STORAGE_LOG(INFO, "=======================forth tree truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate((9 * 128 + 10 * 128 + 1 * 0.5) * ObTmpFileGlobal::PAGE_SIZE, (9 * 128 + 10 * 128 + 1 * 0.6) * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ((9 * 128 + 10 * 128) * ObTmpFileGlobal::PAGE_SIZE, meta_tree_.released_offset_);
  ASSERT_EQ(3 + 2 + 2, meta_tree_.release_pages_.count());
  ASSERT_EQ(false, meta_tree_.root_item_.is_valid());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear((9 * 128 + 10 * 128 + 1 * 0.6) * ObTmpFileGlobal::PAGE_SIZE, 30 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_truncate_with_data_item_remove end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_occurs_between_buf_generation_opers)
{
  STORAGE_LOG(INFO, "====test_tree_flush_with_truncate_occurs_between_buf_generation_opers begin==");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 19;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  //generate last data item, we assume that the page of the last data item is an unfilled page.
  ObSharedNothingTmpFileDataItem last_data_item;
  last_data_item.block_index_ = item_num;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = item_num * 128;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 10 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(5, total_need_flush_page_num);
  ASSERT_EQ(2, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================first round tree flush ============================");
  ObTmpFileTreeFlushContext flush_context_first;

  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - ObTmpFileGlobal::PAGE_SIZE;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));

  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);

  int64_t truncate_offset = 5 * 128 * ObTmpFileGlobal::PAGE_SIZE; //truncate one meta page
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, truncate_offset));
  ASSERT_EQ(1, meta_tree_.release_pages_.count());

  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_2 = SN_BLOCK_SIZE - 4 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_first,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array_2", K(tree_io_array_2));

  ASSERT_EQ(0, tree_io_array_2.count());
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(4, total_need_flush_page_num);
  ASSERT_EQ(2, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  for (int64_t i = 0; i < tree_io_array_1.count(); i++) {
    ObTmpFileTreeIOInfo& tree_io = tree_io_array_1.at(i);
    ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
  }

  STORAGE_LOG(INFO, "=======================second round tree flush ============================");
  ObTmpFileTreeFlushContext flush_context_second;

  char *block_buff_3 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_3 = SN_BLOCK_SIZE - 4 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_3;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(2/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_3,
                                                              write_offset_3,
                                                              flush_context_second,
                                                              tree_io_array_3));
  STORAGE_LOG(INFO, "tree_io_array_3", K(tree_io_array_3));

  ASSERT_EQ(2, tree_io_array_3.count());
  ASSERT_EQ(true, flush_context_second.is_meta_reach_end_);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_3));
  for (int64_t i = 0; i < tree_io_array_3.count(); i++) {
    ObTmpFileTreeIOInfo& tree_io = tree_io_array_3.at(i);
    ASSERT_EQ(tree_io.flush_end_page_id_, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
  }

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(truncate_offset, 30 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  delete[] block_buff_1;
  delete[] block_buff_2;
  delete[] block_buff_3;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "====test_tree_flush_with_truncate_occurs_between_buf_generation_opers end==");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_occurs_between_buf_generation_opers_2)
{
  STORAGE_LOG(INFO, "==test_tree_flush_with_truncate_occurs_between_buf_generation_opers_2 begin===");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 19;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  //generate last data item, we assume that the page of the last data item is an unfilled page.
  ObSharedNothingTmpFileDataItem last_data_item;
  last_data_item.block_index_ = item_num;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = item_num * 128;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 10 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(5, total_need_flush_page_num);
  ASSERT_EQ(2, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================first tree flush ============================");
  ObTmpFileTreeFlushContext flush_context_first;

  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - 3 * ObTmpFileGlobal::PAGE_SIZE;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));

  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);

  int64_t truncate_offset = 5 * 1 * 128 * ObTmpFileGlobal::PAGE_SIZE;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, truncate_offset));

  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_2 = SN_BLOCK_SIZE - 2 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_first,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array_2", K(tree_io_array_2));
  ASSERT_EQ(2, tree_io_array_2.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_2);
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));

  for (int64_t i = 0; i < tree_io_array_2.count(); i++) {
    ObTmpFileTreeIOInfo& tree_io = tree_io_array_2.at(i);
    ASSERT_EQ(tree_io.flush_end_page_id_, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
  }

  delete[] block_buff_1;
  delete[] block_buff_2;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "==test_tree_flush_with_truncate_occurs_between_buf_generation_opers_2 end===");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_occurs_between_buf_generation_opers_3)
{
  STORAGE_LOG(INFO, "====test_tree_flush_with_truncate_occurs_between_buf_generation_opers_3 begin==");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 19;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  //generate last data item, we assume that the page of the last data item is an unfilled page.
  ObSharedNothingTmpFileDataItem last_data_item;
  last_data_item.block_index_ = item_num;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = item_num * 128;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 10 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(5, total_need_flush_page_num);
  ASSERT_EQ(2, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "=======================first tree flush ============================");
  ObTmpFileTreeFlushContext flush_context_first;

  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - ObTmpFileGlobal::PAGE_SIZE;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));

  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);

  int64_t truncate_offset = 5 * 4 * 128 * ObTmpFileGlobal::PAGE_SIZE;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, truncate_offset));

  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_2 = SN_BLOCK_SIZE - 4 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_first,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array_2", K(tree_io_array_2));

  ASSERT_EQ(0, tree_io_array_2.count());
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);
  ASSERT_NE(flush_context_first.tree_epoch_, meta_tree_.tree_epoch_);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  for (int64_t i = 0; i < tree_io_array_1.count(); i++) {
    ObTmpFileTreeIOInfo& tree_io = tree_io_array_1.at(i);
    ASSERT_NE(tree_io.tree_epoch_, meta_tree_.tree_epoch_);
  }

  delete[] block_buff_1;
  delete[] block_buff_2;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "====test_tree_flush_with_truncate_occurs_between_buf_generation_opers_3 end==");
}

void TestSNTmpFileMetaTree::test_tree_flush_with_truncate_occurs_before_update_meta(
    int64_t truncate_offset, bool insert_after_truncate)
{
  STORAGE_LOG(INFO, "===============test_tree_flush_with_truncate_occurs_before_update_meta begin ====");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 70;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  //generate last data item, we assume the last data item only contain one page.
  ObSharedNothingTmpFileDataItem last_data_item;
  last_data_item.block_index_ = item_num;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = item_num * 128;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item));
  item_num++;
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  //insert 10 items (insert a array)
  //  last data item has only one page, and the page is an unfilled page.
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================check tree flush pages=======================");
  int64_t total_need_flush_page_num = 0;
  int64_t total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(15 + 3 + 1, total_need_flush_page_num);
  ASSERT_EQ(3, total_need_flush_rightmost_page_num);

  STORAGE_LOG(INFO, "======================= tree flush ============================");
  ObTmpFileTreeFlushContext flush_context;

  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - ObTmpFileGlobal::PAGE_SIZE;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));

  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);

  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_2 = SN_BLOCK_SIZE - 18 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array_2", K(tree_io_array_2));

  ASSERT_EQ(3, tree_io_array_2.count());
  ASSERT_EQ(true, flush_context.is_meta_reach_end_);

  total_need_flush_page_num = 0;
  total_need_flush_rightmost_page_num = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.get_need_flush_page_num(total_need_flush_page_num, total_need_flush_rightmost_page_num));
  ASSERT_EQ(0, total_need_flush_page_num);
  ASSERT_EQ(0, total_need_flush_rightmost_page_num);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, truncate_offset));

  if (insert_after_truncate) {
    STORAGE_LOG(INFO, "======================= second tree insert =======================");
    int64_t item_num_2 = 20;
    ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
    //even if truncate_offset is not an integer multiple of PAGE_SIZE, this result is reasonable.
    //the start_virtual_page_id we insert into the tree may be smaller than the actual truncate_offset of the tmp file.
    int64_t start_virtual_page_id =
      MAX(truncate_offset / ObTmpFileGlobal::PAGE_SIZE, last_data_item.virtual_page_id_ + last_data_item.physical_page_num_);
    generate_data_items(item_num_2, start_virtual_page_id, data_items_1);
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  }

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));

  if (insert_after_truncate) {
    ASSERT_EQ(true, meta_tree_.root_item_.is_valid());
    if (truncate_offset >= (last_data_item.virtual_page_id_ + last_data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE) {
      for (int64_t i = 0; i < tree_io_array_2.count(); i++) {
        ASSERT_NE(tree_io_array_2.at(i).tree_epoch_, meta_tree_.tree_epoch_);
      }
    } else {
      for (int64_t i = 0; i < tree_io_array_2.count(); i++) {
        ObTmpFileTreeIOInfo& tree_io = tree_io_array_2.at(i);
        ASSERT_EQ(tree_io.flush_end_page_id_, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
      }
    }
  } else {
    if (truncate_offset >= (last_data_item.virtual_page_id_ + last_data_item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE) {
      ASSERT_EQ(false, meta_tree_.root_item_.is_valid());
    } else {
      for (int64_t i = 0; i < tree_io_array_2.count(); i++) {
        ObTmpFileTreeIOInfo& tree_io = tree_io_array_2.at(i);
        ASSERT_EQ(tree_io.flush_end_page_id_, meta_tree_.level_page_range_array_.at(tree_io.page_level_).flushed_end_page_id_);
      }
    }
  }

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(truncate_offset, 100 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  delete[] block_buff_1;
  delete[] block_buff_2;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=================test_tree_flush_with_truncate_occurs_before_update_meta end=======");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_to_somewhere_in_the_middle_before_update_meta)
{
  test_tree_flush_with_truncate_occurs_before_update_meta(5 * 128 * ObTmpFileGlobal::PAGE_SIZE, false);
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_to_end_before_update_meta)
{
  test_tree_flush_with_truncate_occurs_before_update_meta(30 * 128 * ObTmpFileGlobal::PAGE_SIZE, false);
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_to_somewhere_in_the_middle_before_update_meta_2)
{
  test_tree_flush_with_truncate_occurs_before_update_meta(8 * 128 * ObTmpFileGlobal::PAGE_SIZE, true);
}

TEST_F(TestSNTmpFileMetaTree, test_tree_flush_with_truncate_to_end_before_update_meta_2)
{
  test_tree_flush_with_truncate_occurs_before_update_meta(10 * 128 * ObTmpFileGlobal::PAGE_SIZE, true);
}

//================More detailed tests of meta tree involve more test points==================

//=========================================insert============================================
TEST_F(TestSNTmpFileMetaTree, test_array_insert)
{
  STORAGE_LOG(INFO, "=======================test_array_insert begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(100);
  meta_tree_.set_max_page_item_cnt(100);
  int64_t item_num = 30;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first insert=======================");
  //insert 30 items (insert a array)
  ObSEArray<ObSharedNothingTmpFileDataItem, 1> tmp_data_item_arr;
  for (int64_t i = 0; i < data_items.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(30, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second insert=======================");
  item_num = 71;
  generate_data_items(item_num, 30 * 128, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  //insert 71 items (insert one by one)
  for (int64_t i = 0; i < data_items_1.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items_1.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_array_insert end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_prepare_for_insert)
{
  STORAGE_LOG(INFO, "=================test_tree_prepare_for_insert begin===============");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  int64_t item_num = 9;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  ObSEArray<ObSharedNothingTmpFileDataItem, 1> tmp_data_item_arr;
  for (int64_t i = 0; i < data_items.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================first tree flush and evict=========================");
  ObTmpFileTreeFlushContext flush_context_first;

  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - 2 * ObTmpFileGlobal::PAGE_SIZE;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));

  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);
  char *rightmost_page_buf = block_buff_1 + SN_BLOCK_SIZE - ObTmpFileGlobal::PAGE_SIZE;
  meta_tree_.read_cache_rightmost_pages_.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf, 0)));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  int64_t actual_evict_page_num = -1;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.evict_meta_pages(3, ObTmpFileTreeEvictType::FULL, actual_evict_page_num));
  ASSERT_EQ(2, actual_evict_page_num);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(0).end_page_id_);

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  item_num = 6;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 9 * 128, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  for (int64_t i = 0; i < data_items_1.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items_1.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(1, meta_tree_.read_cache_rightmost_pages_.at(0).second);
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(1, meta_tree_.level_page_range_array_.at(0).evicted_page_num_);
  STORAGE_LOG(INFO, "level_page_range_array", K(meta_tree_.level_page_range_array_));

  STORAGE_LOG(INFO, "=======================second tree flush and evict=========================");
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  int64_t write_offset_2 = SN_BLOCK_SIZE - 3 * ObTmpFileGlobal::PAGE_SIZE;
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  ObTmpFileTreeFlushContext flush_context_second;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_second,
                                                              tree_io_array_2));
  STORAGE_LOG(INFO, "tree_io_array_2", K(tree_io_array_2));
  ASSERT_EQ(2, tree_io_array_2.count());
  ASSERT_EQ(true, flush_context_second.is_meta_reach_end_);

  char *rightmost_page_buf_0 = block_buff_2 + SN_BLOCK_SIZE - 2 * ObTmpFileGlobal::PAGE_SIZE;
  char *rightmost_page_buf_1 = block_buff_2 + SN_BLOCK_SIZE - 1 * ObTmpFileGlobal::PAGE_SIZE;
  meta_tree_.read_cache_rightmost_pages_.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_0, 0)));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_1, 0)));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_2));
  actual_evict_page_num = -1;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.evict_meta_pages(3, ObTmpFileTreeEvictType::FULL, actual_evict_page_num));
  ASSERT_EQ(3, actual_evict_page_num);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(1).end_page_id_);

  STORAGE_LOG(INFO, "=======================third tree insert=======================");
  item_num = 20;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 15 * 128, data_items_2);
  ASSERT_EQ(item_num, data_items_2.count());
  for (int64_t i = 0; i < data_items_2.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items_2.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(1, meta_tree_.read_cache_rightmost_pages_.at(0).second);
    ASSERT_EQ(1, meta_tree_.read_cache_rightmost_pages_.at(1).second);
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(5, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.at(0).evicted_page_num_);

  delete[] block_buff_1;
  delete[] block_buff_2;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=================test_tree_prepare_for_insert begin===============");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_prepare_for_insert_fail)
{
  STORAGE_LOG(INFO, "=======================test_tree_prepare_for_insert_fail begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(2);

  STORAGE_LOG(INFO, "=======================first insert=======================");
  int64_t item_num = 10;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ObSEArray<ObSharedNothingTmpFileDataItem, 1> tmp_data_item_arr;
  for (int64_t i = 0; i < data_items.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(4, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================first tree flush and evict=========================");
  ObTmpFileTreeFlushContext flush_context_first;
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = 0;

  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  ASSERT_EQ(4, tree_io_array_1.count());
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  char *rightmost_page_buf_0 = block_buff_1 + 4 * ObTmpFileGlobal::PAGE_SIZE;
  char *rightmost_page_buf_1 = block_buff_1 + 7 * ObTmpFileGlobal::PAGE_SIZE;
  char *rightmost_page_buf_2 = block_buff_1 + 9 * ObTmpFileGlobal::PAGE_SIZE;
  char *rightmost_page_buf_3 = block_buff_1 + 10 * ObTmpFileGlobal::PAGE_SIZE;
  meta_tree_.read_cache_rightmost_pages_.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_0, 0)));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_1, 0)));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_2, 0)));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.read_cache_rightmost_pages_.push_back(std::make_pair(rightmost_page_buf_3, 0)));

  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  int64_t actual_evict_page_num = -1;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.evict_meta_pages(12, ObTmpFileTreeEvictType::FULL, actual_evict_page_num));
  ASSERT_EQ(11, actual_evict_page_num);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(3).end_page_id_);

  STORAGE_LOG(INFO, "=======================build new meta tree=======================");
  //We build a new meta tree to take up most of the write cache memory
  ObTmpFileTestMetaTree meta_tree_1_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_1_.init(2, &wbp, &callback_allocator, &block_manager));
  meta_tree_1_.set_max_array_item_cnt(2);
  meta_tree_1_.set_max_page_item_cnt(2);

  STORAGE_LOG(INFO, "=======================new meta tree insert=======================");
  item_num = 252;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  for (int64_t i = 0; i < data_items_1.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items_1.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_1_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_1_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(8, meta_tree_1_.level_page_range_array_.count());//252 pages

  STORAGE_LOG(INFO, "=======================second insert=======================");
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, meta_tree_.prepare_for_insert_items());
  ASSERT_NE(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(3).end_page_id_);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(2).end_page_id_);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(1).end_page_id_);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, meta_tree_.level_page_range_array_.at(0).end_page_id_);

  STORAGE_LOG(INFO, "=======================new meta tree clear=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_1_.clear(0, 252 * 128 * ObTmpFileGlobal::PAGE_SIZE));

  STORAGE_LOG(INFO, "=======================third insert=======================");
  item_num = 10;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 10 * 128/*start virtual page id*/, data_items_2);
  ASSERT_EQ(item_num, data_items_2.count());
  for(int64_t i = 0; i < data_items_2.count(); i++) {
    tmp_data_item_arr.reset();
    ASSERT_EQ(OB_SUCCESS, tmp_data_item_arr.push_back(data_items_2.at(i)));
    ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
    ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(tmp_data_item_arr));
  }
  ASSERT_EQ(5, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(6, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(4, meta_tree_.level_page_range_array_.at(0).evicted_page_num_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 20 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  delete[] block_buff_1;
  meta_tree_.reset();
  meta_tree_1_.reset();
  STORAGE_LOG(INFO, "=======================test_tree_prepare_for_insert_fail end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_insert_fail_after_array_used)
{
  STORAGE_LOG(INFO, "================test_tree_insert_fail_after_array_used begin================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(3);
  meta_tree_.set_max_page_item_cnt(3);
  STORAGE_LOG(INFO, "=======================first insert=======================");
  int64_t item_num = 2;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  //insert 750 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(2, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second insert=======================");
  item_num = 10;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 1 * 128/*start virtual page id*/, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_NE(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(2, meta_tree_.data_item_array_.count());
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================third insert=======================");
  item_num = 600;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 2 * 128/*start virtual page id*/, data_items_2);
  ASSERT_EQ(item_num, data_items_2.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(2, meta_tree_.data_item_array_.count());
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "================test_tree_insert_fail_after_array_used begin================");
}

TEST_F(TestSNTmpFileMetaTree, test_tree_insert_fail_after_tree_build)
{
  STORAGE_LOG(INFO, "================test_tree_insert_fail_after_tree_build begin================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(2);
  meta_tree_.set_max_page_item_cnt(3);
  STORAGE_LOG(INFO, "=======================first insert=======================");
  int64_t item_num = 361;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  //insert 361 items (insert a array)
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(6, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================second insert=======================");
  item_num = 361;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 361 * 128/*start virtual page id*/, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(121, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(41, meta_tree_.level_page_range_array_.at(1).cached_page_num_);

  STORAGE_LOG(INFO, "=======================third insert=======================");
  item_num = 2;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  generate_data_items(item_num, 361 * 128/*start virtual page id*/ - 10, data_items_2);
  ASSERT_EQ(item_num, data_items_2.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_NE(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  data_items_2.reset();
  generate_data_items(item_num, 361 * 128/*start virtual page id*/, data_items_2);
  ASSERT_EQ(item_num, data_items_2.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_2));
  ASSERT_EQ(121, meta_tree_.level_page_range_array_.at(0).cached_page_num_);
  ASSERT_EQ(41, meta_tree_.level_page_range_array_.at(1).cached_page_num_);

  ASSERT_EQ(OB_SUCCESS, meta_tree_.clear(0, 363 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  meta_tree_.reset();
  STORAGE_LOG(INFO, "================test_tree_insert_fail_after_tree_build begin================");
}

TEST_F(TestSNTmpFileMetaTree, test_array_read)
{
  STORAGE_LOG(INFO, "=======================test_array_read begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(100);
  meta_tree_.set_max_page_item_cnt(100);
  STORAGE_LOG(INFO, "=======================first array insert=======================");
  int64_t item_num = 50;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(50, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second array insert=======================");
  item_num = 50;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 50 * 128 - 1/*start virtual page id*/, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_NE(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  data_items_1.reset();
  generate_data_items(item_num, 50 * 128/*start virtual page id*/, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(100, meta_tree_.data_item_array_.count());

  //After simulating the insert, we can see that the tmp file offset is [0, 100 * 128 * 8K],
  //  with each data item occupying 128 * 8K.
  STORAGE_LOG(INFO, "=======================first array read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(10, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE - 5, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(3, get_data_items.count());

  STORAGE_LOG(INFO, "=======================second array read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(7 * 128 * ObTmpFileGlobal::PAGE_SIZE, 5 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  ASSERT_EQ(5, get_data_items.count());

  STORAGE_LOG(INFO, "=======================third array read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(0, 100 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  ASSERT_EQ(100, get_data_items.count());

  STORAGE_LOG(INFO, "=======================forth array read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(91 * 128 * ObTmpFileGlobal::PAGE_SIZE + 2, 3 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(4, get_data_items.count());
  ASSERT_EQ(91 * 128, get_data_items.at(0).virtual_page_id_);

  STORAGE_LOG(INFO, "=======================fifth array read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(1, 0.5 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(1, get_data_items.count());
  ASSERT_EQ(0, get_data_items.at(0).virtual_page_id_);

  STORAGE_LOG(INFO, "=======================fifth array read=======================");
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(1, 0.5 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  STORAGE_LOG(INFO, "data_items", K(get_data_items));
  ASSERT_EQ(1, get_data_items.count());
  ASSERT_EQ(0, get_data_items.at(0).virtual_page_id_);

  STORAGE_LOG(INFO, "=======================fifth array read=======================");
  get_data_items.reset();
  ASSERT_NE(OB_SUCCESS, meta_tree_.search_data_items(1, 100 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_array_read end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_read_fail)
{
  STORAGE_LOG(INFO, "=======================test_read_fail begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(3);
  meta_tree_.set_max_page_item_cnt(5);
  STORAGE_LOG(INFO, "=======================tree insert=======================");
  int64_t item_num = 3;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_wrong_data_items(item_num, 0, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(3, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================first read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  ASSERT_NE(OB_SUCCESS, meta_tree_.search_data_items(1 * 128 * ObTmpFileGlobal::PAGE_SIZE, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));

  STORAGE_LOG(INFO, "=======================second insert=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, 3 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  item_num = 5;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_wrong_data_items(item_num, 4 * 128, data_items_1);
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());
  ASSERT_EQ(1, meta_tree_.level_page_range_array_.count());

  STORAGE_LOG(INFO, "=======================second read=======================");
  get_data_items.reset();
  ASSERT_NE(OB_SUCCESS, meta_tree_.search_data_items(4 * 128 * ObTmpFileGlobal::PAGE_SIZE, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_read_fail end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_array_read_after_truncate)
{
  STORAGE_LOG(INFO, "=======================test_array_read_after_truncate begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(100);
  meta_tree_.set_max_page_item_cnt(100);
  STORAGE_LOG(INFO, "=======================array insert=======================");
  int64_t item_num = 100;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(100, meta_tree_.data_item_array_.count());
  //After simulating the insert, we can see that the tmp file offset is [0, 100 * 128 * 8K],
  //  with each data item occupying 128 * 8K.
  STORAGE_LOG(INFO, "=======================array truncate=======================");
  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(0, 2 * 128 * ObTmpFileGlobal::PAGE_SIZE - 5));
  ASSERT_EQ(99, meta_tree_.data_item_array_.count());

  ASSERT_EQ(OB_SUCCESS, meta_tree_.truncate(2 * 128 * ObTmpFileGlobal::PAGE_SIZE - 5, 20 * 128 * ObTmpFileGlobal::PAGE_SIZE));
  ASSERT_EQ(80, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================array read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> get_data_items;
  get_data_items.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items(20 * 128 * ObTmpFileGlobal::PAGE_SIZE, 5 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));
  ASSERT_EQ(5, get_data_items.count());
  STORAGE_LOG(INFO, "data_items", K(get_data_items));

  get_data_items.reset();
  ASSERT_NE(OB_SUCCESS, meta_tree_.search_data_items(18 * 128 * ObTmpFileGlobal::PAGE_SIZE, 5 * 128 * ObTmpFileGlobal::PAGE_SIZE, get_data_items));

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_array_read_after_truncate end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_write_tail)
{
  STORAGE_LOG(INFO, "=======================test_write_tail begin=======================");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  STORAGE_LOG(INFO, "=======================first insert=======================");
  int64_t item_num = 4;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(0, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(4, meta_tree_.data_item_array_.count());
  //After simulating the insert, we can see that the tmp file offset is [0, 100 * 128 * 8K],
  //  with each data item occupying 128 * 8K.
  //  we assume that the last page of the last data item is an unfilled page.
  STORAGE_LOG(INFO, "=======================first write tail=======================");
  ObSharedNothingTmpFileDataItem last_data_item;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  //write tail fail
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, false));
  ASSERT_EQ(128, meta_tree_.data_item_array_.at(meta_tree_.data_item_array_.count() - 1).physical_page_num_);
  last_data_item.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true));
  ASSERT_EQ(127, meta_tree_.data_item_array_.at(meta_tree_.data_item_array_.count() - 1).physical_page_num_);
  ASSERT_EQ(4, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second insert=======================");
  data_items.reset();
  last_data_item.reset();
  last_data_item.block_index_ = 30;
  last_data_item.physical_page_id_ = 0;
  last_data_item.physical_page_num_ = 1;
  last_data_item.virtual_page_id_ = 3 * 128 + 127;
  ASSERT_EQ(OB_SUCCESS, data_items.push_back(last_data_item)); //unfilled page
  ASSERT_EQ(1, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(5, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================second write tail=======================");
  last_data_item.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true));
  ASSERT_EQ(4, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================third insert=======================");
  item_num = 2;
  ObArray<ObSharedNothingTmpFileDataItem> data_items_1;
  generate_data_items(item_num, 3 * 128 + 127/*start virtual page id*/, data_items_1);
  //we assume that the last page of the last data item is an unfilled page.
  ASSERT_EQ(item_num, data_items_1.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items_1));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(0, meta_tree_.data_item_array_.count());

  STORAGE_LOG(INFO, "=======================third write tail=======================");
  last_data_item.reset();
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true));

  STORAGE_LOG(INFO, "=======================first read=======================");
  ObArray<ObSharedNothingTmpFileDataItem> data_items_2;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.search_data_items((3 * 128 + 127) * ObTmpFileGlobal::PAGE_SIZE, 240 * ObTmpFileGlobal::PAGE_SIZE, data_items_2));
  ASSERT_EQ(2, data_items_2.count());
  ASSERT_EQ(127, data_items_2.at(1).physical_page_num_);

  meta_tree_.reset();
  STORAGE_LOG(INFO, "=======================test_write_tail end=======================");
}

TEST_F(TestSNTmpFileMetaTree, test_page_is_dirty_again_during_flush)
{
  STORAGE_LOG(INFO, "=================test_page_is_dirty_again_during_flush begin===============");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  int64_t item_num = 10;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.at(0).cached_page_num_);

  STORAGE_LOG(INFO, "=======================first tree flush=========================");
  ObTmpFileTreeFlushContext flush_context_first;
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - 2 * ObTmpFileGlobal::PAGE_SIZE;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  STORAGE_LOG(INFO, "tree_io_array_1", K(tree_io_array_1));
  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(2, tree_io_array_1.at(0).flush_nums_);
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);
  ASSERT_EQ(false, flush_context_first.is_meta_reach_end_);

  STORAGE_LOG(INFO, "=======================first write tail=======================");
  ObSharedNothingTmpFileDataItem last_data_item;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_write_tail(last_data_item));
  ASSERT_EQ(OB_SUCCESS, meta_tree_.finish_write_tail(last_data_item, true));

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  item_num = 1;
  data_items.reset();
  generate_data_items(item_num, 9 * 128 + 127/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.at(0).cached_page_num_);

  STORAGE_LOG(INFO, "=======================second tree flush=========================");
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  int64_t write_offset_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_first,
                                                              tree_io_array_2));
  ASSERT_EQ(0, tree_io_array_2.count());
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  STORAGE_LOG(INFO, "=======================third tree flush=========================");
  //flush successfully
  ASSERT_EQ(OB_SUCCESS, meta_tree_.update_after_flush(tree_io_array_1));
  ASSERT_EQ(tree_io_array_1.at(0).flush_end_page_id_, meta_tree_.level_page_range_array_.at(0).flushed_end_page_id_);

  ObTmpFileTreeFlushContext flush_context_second;
  char *block_buff_3 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_3;
  int64_t write_offset_3 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(2/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_3,
                                                              write_offset_3,
                                                              flush_context_second,
                                                              tree_io_array_3));
  ASSERT_EQ(2, tree_io_array_3.count());
  ASSERT_EQ(2, tree_io_array_3.at(0).flush_nums_);
  ASSERT_EQ(1, tree_io_array_3.at(1).flush_nums_);
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  delete[] block_buff_1;
  delete[] block_buff_2;
  delete[] block_buff_3;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=================test_page_is_dirty_again_during_flush end===============");
}

TEST_F(TestSNTmpFileMetaTree, test_insert_items_during_flush)
{
  STORAGE_LOG(INFO, "=================test_insert_items_during_flush begin===============");
  ObTmpWriteBufferPool wbp;
  wbp.default_wbp_memory_limit_ = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; //253 pages
  ASSERT_EQ(OB_SUCCESS, wbp.init());
  common::ObFIFOAllocator callback_allocator;
  ASSERT_EQ(OB_SUCCESS, callback_allocator.init(lib::ObMallocAllocator::get_instance(),
                                                OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                ObMemAttr(MTL_ID(), "TmpFileCallback", ObCtxIds::DEFAULT_CTX_ID)));
  ObTmpFileBlockManager block_manager;
  ASSERT_EQ(OB_SUCCESS, block_manager.init(MTL_ID()));
  ObTmpFileTestMetaTree meta_tree_;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.init(1, &wbp, &callback_allocator, &block_manager));
  meta_tree_.set_max_array_item_cnt(5);
  meta_tree_.set_max_page_item_cnt(5);
  STORAGE_LOG(INFO, "=======================first tree insert=======================");
  int64_t item_num = 10;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  generate_data_items(item_num, 0/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.at(0).cached_page_num_);

  STORAGE_LOG(INFO, "=======================first tree flush=========================");
  ObTmpFileTreeFlushContext flush_context_first;
  char *block_buff_1 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_1;
  int64_t write_offset_1 = SN_BLOCK_SIZE - 2 * ObTmpFileGlobal::PAGE_SIZE;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(0/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_1,
                                                              write_offset_1,
                                                              flush_context_first,
                                                              tree_io_array_1));
  ASSERT_EQ(1, tree_io_array_1.count());
  ASSERT_EQ(2, tree_io_array_1.at(0).flush_nums_);
  ASSERT_EQ(SN_BLOCK_SIZE, write_offset_1);
  ASSERT_EQ(false, flush_context_first.is_meta_reach_end_);

  STORAGE_LOG(INFO, "=======================second tree insert=======================");
  item_num = 1;
  data_items.reset();
  generate_data_items(item_num, 10 * 128/*start virtual page id*/, data_items);
  ASSERT_EQ(item_num, data_items.count());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.prepare_for_insert_items());
  ASSERT_EQ(OB_SUCCESS, meta_tree_.insert_items(data_items));
  ASSERT_EQ(2, meta_tree_.level_page_range_array_.count());
  ASSERT_EQ(3, meta_tree_.level_page_range_array_.at(0).cached_page_num_);

  STORAGE_LOG(INFO, "=======================second tree flush=========================");
  char *block_buff_2 = new char[SN_BLOCK_SIZE];
  ObArray<ObTmpFileTreeIOInfo> tree_io_array_2;
  int64_t write_offset_2 = 0;
  ASSERT_EQ(OB_SUCCESS, meta_tree_.flush_meta_pages_for_block(1/*block_index*/,
                                                              ObTmpFileTreeEvictType::FULL,
                                                              block_buff_2,
                                                              write_offset_2,
                                                              flush_context_first,
                                                              tree_io_array_2));
  ASSERT_EQ(2, tree_io_array_2.count());
  ASSERT_EQ(1, tree_io_array_2.at(0).flush_nums_);
  ASSERT_EQ(true, flush_context_first.is_meta_reach_end_);

  delete[] block_buff_1;
  delete[] block_buff_2;
  meta_tree_.reset();
  STORAGE_LOG(INFO, "=================test_insert_items_during_flush end===============");
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
