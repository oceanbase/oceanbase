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

namespace oceanbase
{
using namespace tmp_file;

static const int64_t INIT_BUCKET_ARRAY_CAPACITY = ObTmpFileWBPIndexCache::INIT_BUCKET_ARRAY_CAPACITY;
static const int64_t MAX_BUCKET_ARRAY_CAPACITY = ObTmpFileWBPIndexCache::MAX_BUCKET_ARRAY_CAPACITY;
static const int64_t BUCKET_CAPACITY = ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket::BUCKET_CAPACITY;
static const int64_t fd = 1;
static uint64_t tenant_id = OB_SYS_TENANT_ID;
static const int64_t TENANT_MEMORY = 8L * 1024L * 1024L * 1024L /* 8 GB */;
static const double TMP_FILE_WBP_MEM_LIMIT_PROP = 50; //[0, 100]
/* ----------------------------- Mock Class -------------------------------- */
class MockWBPIndexCache
{
public:
  MockWBPIndexCache(ObTmpWriteBufferPool &wbp) : wbp_(wbp), sparsify_count_(0), ignored_push_count_(0) {}
  int push(uint32_t page_index);
  int truncate(const int64_t truncate_page_virtual_id);
  int compare(const ObTmpFileWBPIndexCache &index_cache);
private:
  int sparsify_(const int64_t sparsify_modulus);
public:
  ObArray<uint32_t> mock_index_cache_;
  ObTmpWriteBufferPool &wbp_;
  int64_t sparsify_count_;
  int64_t ignored_push_count_;
};

int MockWBPIndexCache::push(uint32_t page_index)
{
  int ret = OB_SUCCESS;
  if (mock_index_cache_.count() >= MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY) {
    if (OB_FAIL(sparsify_(2))) {
      LOG_WARN("failed to sparsify", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ignored_push_count_ < ((1 << sparsify_count_) - 1)) {
    ignored_push_count_ += 1;
  } else if (FALSE_IT(ignored_push_count_ = 0)) {
  } else if (OB_FAIL(mock_index_cache_.push_back(page_index))) {
    LOG_WARN("failed to push back page index", KR(ret), K(page_index));
  }
  return ret;
}

int MockWBPIndexCache::truncate(const int64_t truncate_page_virtual_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> new_mock_index_cache;
  if (truncate_page_virtual_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_virtual_id));
  } else {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < mock_index_cache_.count(); ++i) {
      int64_t virtual_page_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      if (OB_FAIL(wbp_.get_page_virtual_id(fd, mock_index_cache_.at(i), virtual_page_id))) {
        LOG_WARN("fail to get page virtual id", KR(ret), K(mock_index_cache_.at(i)));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid virtual page id", KR(ret), K(virtual_page_id));
      } else if (virtual_page_id >= truncate_page_virtual_id) {
        break;
      }
    }
    for (int64_t j = i; j < mock_index_cache_.count(); ++j) {
      if (OB_FAIL(new_mock_index_cache.push_back(mock_index_cache_.at(j)))) {
        LOG_WARN("fail to push back page index", KR(ret), K(mock_index_cache_.at(j)));
      }
    }
    mock_index_cache_ = new_mock_index_cache;
  }
  return ret;
}

int MockWBPIndexCache::sparsify_(const int64_t sparsify_modulus)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> new_mock_index_cache;
  if (OB_UNLIKELY(sparsify_modulus < 2 ||
      mock_index_cache_.count() != MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(sparsify_modulus), K(mock_index_cache_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mock_index_cache_.count(); ++i) {
      if (i % sparsify_modulus != 0 &&
          OB_FAIL(new_mock_index_cache.push_back(mock_index_cache_.at(i)))) {
        LOG_WARN("fail to push back page index", KR(ret), K(mock_index_cache_.at(i)));
      }
    }
    mock_index_cache_ = new_mock_index_cache;
    sparsify_count_ += 1;
    ignored_push_count_ = 0;
  }
  return ret;
}

int MockWBPIndexCache::compare(const ObTmpFileWBPIndexCache &index_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_cache.is_empty() && mock_index_cache_.empty())) {
    // do nothing
  } else {
    if (OB_UNLIKELY(index_cache.is_empty() || mock_index_cache_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("one cache is empty", KR(ret), K(index_cache.is_empty()), K(mock_index_cache_.empty()));
    } else if (OB_ISNULL(index_cache.page_buckets_) || OB_ISNULL(index_cache.page_buckets_->at(index_cache.left_)) ||
               OB_ISNULL(index_cache.page_buckets_->at(index_cache.right_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KR(ret), KP(index_cache.page_buckets_), K(index_cache.left_), K(index_cache.right_));
    } else if (OB_UNLIKELY(index_cache.page_buckets_->at(index_cache.left_)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty bucket", KR(ret), K(index_cache.left_));
    } else if (OB_UNLIKELY(index_cache.page_buckets_->at(index_cache.right_)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty bucket", KR(ret), K(index_cache.right_));
    } else if (index_cache.size() == 1) {
      if (mock_index_cache_.count() != index_cache.page_buckets_->at(index_cache.left_)->size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", KR(ret), K(index_cache.size()), K(mock_index_cache_.count()));
      }
    } else {
      int64_t real_cache_size = index_cache.page_buckets_->at(index_cache.left_)->size() +
                                index_cache.page_buckets_->at(index_cache.right_)->size() +
                                (index_cache.get_logic_tail_() - index_cache.left_ - 1) *
                                BUCKET_CAPACITY;
      if (mock_index_cache_.count() != real_cache_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", KR(ret), K(real_cache_size), K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                 K(index_cache.page_buckets_->at(index_cache.right_)->size()), K(index_cache.size()), K(mock_index_cache_.count()));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t logic_end_bkt_pos = index_cache.get_logic_tail_();
      int64_t cur_logic_bkt_pos = index_cache.left_;
      int64_t cur_logic_index_pos = index_cache.page_buckets_->at(cur_logic_bkt_pos)->left_;
      int64_t i = 0;
      for (;OB_SUCC(ret) && i < mock_index_cache_.count() && cur_logic_bkt_pos <= logic_end_bkt_pos; ++i) {
        int64_t real_bkt_pos = cur_logic_bkt_pos % index_cache.capacity_;
        int64_t real_index_pos = cur_logic_index_pos % BUCKET_CAPACITY;
        const ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket *bkt = index_cache.page_buckets_->at(real_bkt_pos);
        uint32_t page_index_in_real_cache = ObTmpFileGlobal::INVALID_PAGE_ID;
        uint32_t page_index_in_mock_cache = mock_index_cache_.at(i);

        if (OB_ISNULL(bkt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret), KP(bkt));
        } else if (FALSE_IT(page_index_in_real_cache = bkt->page_indexes_.at(real_index_pos))) {
        } else if (OB_UNLIKELY(page_index_in_mock_cache != page_index_in_real_cache)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("page index is not same", KR(ret), K(page_index_in_mock_cache), K(page_index_in_real_cache),
                   K(i), K(cur_logic_bkt_pos), K(cur_logic_index_pos), K(index_cache.left_), K(logic_end_bkt_pos));
        } else if (OB_UNLIKELY(cur_logic_index_pos > bkt->get_logic_tail_())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", KR(ret), K(cur_logic_index_pos), K(bkt->get_logic_tail_()));
        } else if (cur_logic_index_pos < bkt->get_logic_tail_()) {
          cur_logic_index_pos += 1;
        } else { // cur_logic_index_pos == bkt->get_logic_tail_()
          cur_logic_bkt_pos += 1;
          if (cur_logic_bkt_pos <= logic_end_bkt_pos) {
            const ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket *next_bkt = index_cache.page_buckets_->at(cur_logic_bkt_pos % index_cache.capacity_);
            if (OB_ISNULL(next_bkt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null", KR(ret), K(mock_index_cache_.count()),
                       K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                       K(index_cache.page_buckets_->at(index_cache.right_)->size()),
                       K(index_cache.size()), K(index_cache.capacity_),
                       K(i), K(cur_logic_bkt_pos),
                       K(cur_logic_index_pos), K(index_cache.left_),
                       K(logic_end_bkt_pos), KP(next_bkt));
            } else if (OB_UNLIKELY(next_bkt->is_empty())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected empty", KR(ret), K(mock_index_cache_.count()),
                       K(index_cache.page_buckets_->at(index_cache.left_)->size()),
                       K(index_cache.page_buckets_->at(index_cache.right_)->size()),
                       K(index_cache.size()), K(index_cache.capacity_),
                       K(i), K(cur_logic_bkt_pos), K(cur_logic_index_pos),
                       K(index_cache.left_), K(logic_end_bkt_pos),
                       KPC(next_bkt));
            } else {
              cur_logic_index_pos = next_bkt->left_;
            }
          }
        }
      } // end for
    }
  }

  return ret;
}

/* ---------------------------- Unittest Class ----------------------------- */
class TestTmpFileWBPIndexCache : public ::testing::Test
{
public:
  TestTmpFileWBPIndexCache() = default;
  virtual ~TestTmpFileWBPIndexCache() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  int write_and_push_pages(const uint32_t end_page_id,const int64_t page_num,
                           MockWBPIndexCache &mock_cache, uint32_t &new_end_page_id);
  int truncate_pages(const int64_t truncate_page_num, MockWBPIndexCache &mock_cache);
  int truncate_and_free_pages(const uint32_t wbp_begin_page_id, const int64_t truncate_page_num_in_cache,
                              MockWBPIndexCache &mock_cache, uint32_t &new_begin_page_id);
private:
  int write_pages_(const int64_t page_num, const int64_t begin_page_virtual_id, ObArray<uint32_t> &page_indexes);
  int free_pages_(const uint32_t begin_page_id, const int64_t end_page_virtual_id, uint32_t &new_begin_page_id);
  int check_cache_status_after_truncate_(MockWBPIndexCache &mock_cache);
public:
  ObTmpWriteBufferPool wbp_;
  ObTmpFileWBPIndexCache wbp_index_cache_;
  common::ObFIFOAllocator wbp_index_cache_allocator_;
  common::ObFIFOAllocator wbp_index_cache_bkt_allocator_;
};

void TestTmpFileWBPIndexCache::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                        OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                        ObMemAttr(tenant_id, "TmpFileIndCache",
                                                                  ObCtxIds::DEFAULT_CTX_ID)));
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_bkt_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                            OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                            ObMemAttr(tenant_id, "TmpFileIndCBkt",
                                                                      ObCtxIds::DEFAULT_CTX_ID)));
  ASSERT_EQ(OB_SUCCESS, wbp_.init());
  ASSERT_EQ(OB_SUCCESS, wbp_index_cache_.init(fd, &wbp_, &wbp_index_cache_allocator_, &wbp_index_cache_bkt_allocator_));
}

void TestTmpFileWBPIndexCache::TearDown()
{
  wbp_index_cache_allocator_.reset();
  wbp_index_cache_bkt_allocator_.reset();
  wbp_.destroy();
  wbp_index_cache_.destroy();
}

void TestTmpFileWBPIndexCache::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(1, TENANT_MEMORY);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_EQ(true, tenant_config.is_valid());
  tenant_config->_temporary_file_io_area_size = TMP_FILE_WBP_MEM_LIMIT_PROP;
}

void TestTmpFileWBPIndexCache::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

// end_page_id: the page_id of the last page of wbp
// page_num: the number of pages to be alloced.
// new_end_page_id: page_id of the last page of alloced pages.
// this function will alloc 'page_num' pages and link them after the page of end_page_id,
// then put them into mock_cache and wbp_index_cache_.
int TestTmpFileWBPIndexCache::write_and_push_pages(const uint32_t end_page_id, const int64_t page_num,
                                                   MockWBPIndexCache &mock_cache, uint32_t &new_end_page_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  int64_t end_page_virtual_id = 0;
  int64_t new_begin_page_virtual_id = 0;
  if (OB_UNLIKELY(page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_num));
  } else if (end_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, end_page_id, end_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(end_page_id), K(end_page_virtual_id));
    } else if (OB_UNLIKELY(end_page_virtual_id < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual id", KR(ret), K(end_page_virtual_id));
    } else {
      new_begin_page_virtual_id = end_page_virtual_id + 1;
    }
  }

  if (FAILEDx(write_pages_(page_num, new_begin_page_virtual_id, page_indexes))) {
    LOG_WARN("fail to write pages", KR(ret), K(page_num));
  } else if (OB_UNLIKELY(page_indexes.count() != page_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(page_num), K(page_indexes.count()));
  } else if (end_page_id != ObTmpFileGlobal::INVALID_PAGE_ID &&
             OB_FAIL(wbp_.link_page(fd, page_indexes.at(0), end_page_id, ObTmpFilePageUniqKey(end_page_virtual_id)))) {
    LOG_WARN("fail to link page", KR(ret), K(page_indexes.at(0)), K(end_page_id), K(end_page_virtual_id));
  } else if (FALSE_IT(new_end_page_id = page_indexes.at(page_num - 1))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < page_num; ++i) {
      if (OB_FAIL(mock_cache.push(page_indexes.at(i)))) {
        LOG_WARN("fail to push", KR(ret), K(i), K(page_indexes.at(i)));
      } else if (OB_FAIL(wbp_index_cache_.push(page_indexes.at(i)))) {
        LOG_WARN("fail to push", KR(ret), K(i), K(page_indexes.at(i)));
      }
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::truncate_pages(const int64_t truncate_page_num, MockWBPIndexCache &mock_cache)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  if (OB_UNLIKELY(truncate_page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_num));
  } else {
    int64_t truncate_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    uint32_t truncate_page_id = mock_cache.mock_index_cache_.at(truncate_page_num - 1);
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, truncate_page_id, truncate_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(truncate_page_id));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == truncate_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual page id", KR(ret), K(truncate_page_virtual_id));
    } else if (FALSE_IT(truncate_page_virtual_id += 1)) {
    } else if (OB_FAIL(mock_cache.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(wbp_index_cache_.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(check_cache_status_after_truncate_(mock_cache))) {
      LOG_WARN("fail to check cache status after truncate", KR(ret));
    }
  }
  return ret;
}

// wbp_begin_page_id: the page_id of the first page of wbp, the pos of it in the wbp list should be
//                    less than or equal to the first cached index in wbp_index_cache_.
// truncate_page_num_in_cache: the number of page indexes to be truncated in mock_cache,
//                             we will free pages between [wbp_begin_page_id, begin_page_id_in_cache + truncate_page_num_in_cache]
// new_begin_page_id: page_id of the first page after free pages in wbp.
int TestTmpFileWBPIndexCache::truncate_and_free_pages(const uint32_t wbp_begin_page_id, const int64_t truncate_page_num_in_cache,
                                                      MockWBPIndexCache &mock_cache, uint32_t &new_begin_page_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint32_t> page_indexes;
  if (OB_UNLIKELY(truncate_page_num_in_cache <= 0 || ObTmpFileGlobal::INVALID_PAGE_ID == wbp_begin_page_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(truncate_page_num_in_cache), K(wbp_begin_page_id));
  } else {
    int64_t truncate_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    uint32_t truncate_page_id = mock_cache.mock_index_cache_.at(truncate_page_num_in_cache - 1);
    if (OB_FAIL(wbp_.get_page_virtual_id(fd, truncate_page_id, truncate_page_virtual_id))) {
      LOG_WARN("fail to get page virtual id", KR(ret), K(truncate_page_id));
    } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == truncate_page_virtual_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid virtual page id", KR(ret), K(truncate_page_virtual_id));
    } else if (FALSE_IT(truncate_page_virtual_id += 1)) {
    } else if (OB_FAIL(mock_cache.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(wbp_index_cache_.truncate(truncate_page_virtual_id))) {
      LOG_WARN("fail to truncate", KR(ret), K(truncate_page_virtual_id));
    } else if (OB_FAIL(check_cache_status_after_truncate_(mock_cache))) {
      LOG_WARN("fail to check cache status after truncate", KR(ret));
    } else if (OB_FAIL(free_pages_(wbp_begin_page_id, truncate_page_virtual_id, new_begin_page_id))) {
      LOG_WARN("fail to free pages", KR(ret), K(wbp_begin_page_id), K(truncate_page_virtual_id));
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::write_pages_(const int64_t page_num, const int64_t begin_page_virtual_id, ObArray<uint32_t> &page_indexes)
{
  int ret = OB_SUCCESS;
  page_indexes.reset();
  if (OB_UNLIKELY(page_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(page_num));
  } else{
    for (int64_t i = 0; OB_SUCC(ret) && i < page_num; ++i) {
      uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      char *unused_buf = nullptr;
      int64_t new_page_begin_page_id = begin_page_virtual_id + i;
      if (OB_FAIL(wbp_.alloc_page(fd, ObTmpFilePageUniqKey(new_page_begin_page_id), new_page_id, unused_buf))) {
        LOG_WARN("fail to alloc data page", KR(ret), K(i));
      } else if (OB_FAIL(wbp_.notify_dirty(fd, new_page_id, ObTmpFilePageUniqKey(new_page_begin_page_id)))) {
        LOG_WARN("fail to notify dirty", KR(ret), K(new_page_id), K(new_page_begin_page_id));
      } else if (!page_indexes.empty() &&
                 OB_FAIL(wbp_.link_page(fd, new_page_id, page_indexes.at(page_indexes.count()-1),
                                        ObTmpFilePageUniqKey(new_page_begin_page_id - 1)))) {
        LOG_WARN("fail to link page", KR(ret), K(i), K(new_page_id),
                 K(page_indexes.at(page_indexes.count()-1)), K(new_page_begin_page_id));
      } else if (OB_FAIL(page_indexes.push_back(new_page_id))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
  }
  return ret;
}


int TestTmpFileWBPIndexCache::free_pages_(const uint32_t begin_page_id, const int64_t end_page_virtual_id, uint32_t &new_begin_page_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end_page_virtual_id <= 0 || begin_page_id == ObTmpFileGlobal::INVALID_PAGE_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(end_page_virtual_id), K(begin_page_id));
  } else {
    bool free_over = false;
    uint32_t cur_page_id = begin_page_id;
    int64_t cnt = 0;
    while(!free_over && OB_SUCC(ret) && cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      int64_t virtual_page_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      if (OB_FAIL(wbp_.get_page_virtual_id(fd, cur_page_id, virtual_page_id))) {
        LOG_WARN("fail to get page virtual id", KR(ret), K(cur_page_id), K(cnt));
      } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID == virtual_page_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid virtual page id", KR(ret), K(virtual_page_id), K(cnt));
      } else if (virtual_page_id >= end_page_virtual_id) {
        free_over = true;
        new_begin_page_id = cur_page_id;
      } else if (OB_FAIL(wbp_.free_page(fd, cur_page_id, ObTmpFilePageUniqKey(virtual_page_id), next_page_id))) {
        LOG_WARN("fail to alloc data page", KR(ret), K(cur_page_id), K(cnt));
      } else {
        cur_page_id = next_page_id;
        cnt += 1;
      }
    }
  }
  return ret;
}

int TestTmpFileWBPIndexCache::check_cache_status_after_truncate_(MockWBPIndexCache &mock_cache)
{
  int ret = OB_SUCCESS;
  int64_t bkt_num = wbp_index_cache_.get_logic_tail_() - wbp_index_cache_.left_ + 1;
  int64_t page_num = 0;
  if (OB_UNLIKELY(bkt_num != wbp_index_cache_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket num is invalid", KR(ret), K(bkt_num), K(wbp_index_cache_.size()));
  }
  for (int64_t bkt_arr_logic_idx = wbp_index_cache_.left_;
       OB_SUCC(ret) && bkt_arr_logic_idx <= wbp_index_cache_.get_logic_tail_();
       bkt_arr_logic_idx++) {
    int64_t bkt_arr_idx = bkt_arr_logic_idx % wbp_index_cache_.capacity_;
    int64_t bkt_left_idx = 0;
    if (OB_ISNULL(wbp_index_cache_.page_buckets_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket array is null", KR(ret));
    } else if (OB_ISNULL(wbp_index_cache_.page_buckets_->at(bkt_arr_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket is null", KR(ret), K(bkt_arr_idx));
    } else if (OB_UNLIKELY(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bucket is empty", KR(ret), K(bkt_arr_idx));
    } else if (FALSE_IT(bkt_left_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->left_)) {
    } else if (OB_UNLIKELY(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->min_page_index_ !=
                           wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_left_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min page index is not equal to left page index", KR(ret),
               K(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->min_page_index_),
               K(wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_left_idx)));
    } else {
      for (int64_t bkt_logic_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->left_;
           OB_SUCC(ret) && bkt_logic_idx <= wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->get_logic_tail_();
           bkt_logic_idx++) {
        int64_t bkt_idx = bkt_logic_idx % wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->capacity_;
        uint32_t page_idx = wbp_index_cache_.page_buckets_->at(bkt_arr_idx)->page_indexes_.at(bkt_idx);
        if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_PAGE_ID == page_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("page index is invalid", KR(ret), K(bkt_arr_idx), K(bkt_idx));
        } else {
          page_num += 1;
        }
      } // end for
    }
  } // end for
  if (OB_UNLIKELY(page_num != mock_cache.mock_index_cache_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page num is invalid", KR(ret), K(page_num), K(mock_cache.mock_index_cache_.count()));
  }
  return ret;
}

TEST_F(TestTmpFileWBPIndexCache, test_push_and_pop)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes
  int64_t page_num = BUCKET_CAPACITY / 2;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. fill a bucket of wbp_index_cache
  page_num = BUCKET_CAPACITY - page_num;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.size_);
  ASSERT_EQ(true, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 3. continue to push some index when bucket is full
  page_num = BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, wbp_index_cache_.size_);
  ASSERT_EQ(false, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 4. push more than a bucket number of indexes
  page_num = BUCKET_CAPACITY * 2 + BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, wbp_index_cache_.size_);
  ASSERT_EQ(false, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->is_full());

  // 5. free indexes of wbp_index_cache and mock_cache
  page_num = BUCKET_CAPACITY / 4;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. push some indexes after freeing
  page_num = 2 * BUCKET_CAPACITY / 4;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. free more than a bucket number of indexes
  page_num = BUCKET_CAPACITY * 2 + BUCKET_CAPACITY / 2;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. free all indexes of wbp_index_cache and mock_cache
  page_num = mock_cache.mock_index_cache_.count();
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  // 9. push indexes into both wbp_index_cache and mock_cache
  page_num = BUCKET_CAPACITY / 3;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("test_push_and_pop");
}

TEST_F(TestTmpFileWBPIndexCache, test_expand_and_sparsify)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes into both wbp_index_cache and mock_cache
  int64_t page_num = INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, wbp_index_cache_.is_full());
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. push indexes to trigger expand
  page_num = 30;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY * 2, wbp_index_cache_.capacity_);

  // 3. push indexes to trigger expand until reach the max capacity
  page_num = MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY
             - mock_cache.mock_index_cache_.count();
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  int64_t actual_write_page_num = 0;
  int64_t bucket_num = 0;
  // 4. push indexes to trigger sparsify
  page_num = BUCKET_CAPACITY / 2;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  actual_write_page_num = page_num / (1 << wbp_index_cache_.sparsify_count_); // in order to keep same sparse interval,
                                                                              // the other push callings are ignored
  bucket_num = wbp_index_cache_.size_;
  ASSERT_EQ(wbp_index_cache_.capacity_ / 2 + 1, bucket_num);
  ASSERT_EQ(actual_write_page_num % BUCKET_CAPACITY, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_);

  // 5. push indexes to trigger sparsify again
  page_num = ((1 << wbp_index_cache_.sparsify_count_) - wbp_index_cache_.ignored_push_count_ - 1) + 1 +
             (1 << wbp_index_cache_.sparsify_count_) *
             (BUCKET_CAPACITY - wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_ +
              BUCKET_CAPACITY * (MAX_BUCKET_ARRAY_CAPACITY - wbp_index_cache_.size_) - 1);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(true, wbp_index_cache_.is_full());

  page_num = BUCKET_CAPACITY / 2;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, wbp_index_cache_.sparsify_count_);
  ASSERT_EQ(false, wbp_index_cache_.is_full());
  ASSERT_EQ(MAX_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  actual_write_page_num = page_num / (1 << wbp_index_cache_.sparsify_count_);
  bucket_num = wbp_index_cache_.size_;
  ASSERT_EQ(wbp_index_cache_.capacity_ / 2 + 1, bucket_num);
  ASSERT_EQ(actual_write_page_num % BUCKET_CAPACITY, wbp_index_cache_.page_buckets_->at(wbp_index_cache_.right_)->size_);

  LOG_INFO("test_expand_and_sparsify");
}

TEST_F(TestTmpFileWBPIndexCache, test_shrink)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 1. push indexes to trigger expand
  int64_t page_num = 2 * INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2 * INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.size_);
  ASSERT_EQ(2 * INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  begin_page_id = mock_cache.mock_index_cache_.at(0);

  // 2. free 7/8 indexes to trigger shrink
  page_num = wbp_index_cache_.capacity_ * BUCKET_CAPACITY * 7 / 8;
  ASSERT_GE(page_num, mock_cache.mock_index_cache_.count() - mock_cache.mock_index_cache_.count() / ObTmpFileWBPIndexCache::SHRINK_THRESHOLD);
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  // 3. push indexes to fill buckets with INIT_BUCKET_ARRAY_CAPACITY
  page_num = INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY
             - mock_cache.mock_index_cache_.count();
  ASSERT_LE(page_num, INIT_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY);
  ASSERT_GT(page_num, 0);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.size_);
  ASSERT_EQ(wbp_index_cache_.size_ * BUCKET_CAPACITY,
            mock_cache.mock_index_cache_.count());

  // 4. free 7/8 indexes, but doesn't trigger shrink
  page_num = wbp_index_cache_.capacity_ * BUCKET_CAPACITY * 7 / 8;
  ASSERT_GE(page_num, mock_cache.mock_index_cache_.count() - mock_cache.mock_index_cache_.count() / ObTmpFileWBPIndexCache::SHRINK_THRESHOLD);
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  remove_page_id = mock_cache.mock_index_cache_.at(page_num - 1);
  ret = truncate_and_free_pages(begin_page_id, page_num, mock_cache, begin_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(INIT_BUCKET_ARRAY_CAPACITY, wbp_index_cache_.capacity_);

  LOG_INFO("test_shrink");
}

int mock_circle_bucket(ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket &bucket)
{
  int ret = OB_SUCCESS;
  if (bucket.right_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket", KR(ret), K(bucket));
  } else if (bucket.left_ < bucket.right_) {
    ObArray<uint32_t> array;
    for (int64_t i = bucket.left_; OB_SUCC(ret) && i <= bucket.right_; ++i) {
      if (OB_FAIL(array.push_back(bucket.page_indexes_.at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t mid = bucket.left_ + (bucket.right_ - bucket.left_) / 2;
      for (int64_t i = 0; i < array.size(); ++i) {
        int64_t new_pos = (bucket.left_ + i + mid) > bucket.right_ ?
                          bucket.left_ + i + mid - array.size() :
                          bucket.left_ + i + mid;
        bucket.page_indexes_.at(new_pos) = array.at(i);
      }
      bucket.left_ = mid;
      bucket.right_= mid - 1;
    }
  }
  return ret;
}

int mock_circle_cache(ObTmpFileWBPIndexCache &wbp_index_cache)
{
  int ret = OB_SUCCESS;
  if (wbp_index_cache.right_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket", KR(ret), K(wbp_index_cache));
  } else if (wbp_index_cache.left_ < wbp_index_cache.right_) {
    ObArray<ObTmpFileWBPIndexCache::ObTmpFilePageIndexBucket*> array;
    for (int64_t i = wbp_index_cache.left_; OB_SUCC(ret) && i <= wbp_index_cache.right_; ++i) {
      if (OB_ISNULL(wbp_index_cache.page_buckets_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_ISNULL(wbp_index_cache.page_buckets_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_FAIL(array.push_back(wbp_index_cache.page_buckets_->at(i)))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t mid = wbp_index_cache.left_ + (wbp_index_cache.right_ - wbp_index_cache.left_) / 2;
      for (int64_t i = 0; i < array.size(); ++i) {
        int64_t new_pos = (wbp_index_cache.left_ + i + mid) > wbp_index_cache.right_ ?
                          wbp_index_cache.left_ + i + mid - array.size() :
                          wbp_index_cache.left_ + i + mid;
        wbp_index_cache.page_buckets_->at(new_pos) = array.at(i);
      }
      wbp_index_cache.left_ = mid;
      wbp_index_cache.right_= mid - 1;
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = wbp_index_cache.left_; OB_SUCC(ret) && i <= wbp_index_cache.get_logic_tail_(); ++i) {
      int64_t pos = i % wbp_index_cache.capacity_;
      if (OB_ISNULL(wbp_index_cache.page_buckets_->at(pos))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(wbp_index_cache));
      } else if (OB_FAIL(mock_circle_bucket(*wbp_index_cache.page_buckets_->at(pos)))) {
        LOG_WARN("fail to mock circle bucket", KR(ret), K(wbp_index_cache));
      }
    }
  }
  return ret;
}

TEST_F(TestTmpFileWBPIndexCache, test_search)
{
  int ret = OB_SUCCESS;
  uint32_t begin_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t end_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;

  // 1. push indexes until reach the max capacity
  int64_t page_num = MAX_BUCKET_ARRAY_CAPACITY * BUCKET_CAPACITY;
  MockWBPIndexCache mock_cache(wbp_);
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  begin_page_id = mock_cache.mock_index_cache_.at(0);
  ASSERT_NE(begin_page_id, ObTmpFileGlobal::INVALID_PAGE_ID);
  ret = mock_circle_cache(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(wbp_index_cache_.left_, wbp_index_cache_.right_);
  ASSERT_GT(wbp_index_cache_.page_buckets_->at(wbp_index_cache_.left_)->left_,
            wbp_index_cache_.page_buckets_->at(wbp_index_cache_.left_)->right_);

  // 2. push indexes to trigger sparsify
  page_num = 20;
  ret = write_and_push_pages(end_page_id, page_num, mock_cache, end_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. truncate indexes to make the first bucket not be full
  page_num = 10;
  ASSERT_LE(page_num, mock_cache.mock_index_cache_.count());
  ret = truncate_pages(page_num, mock_cache);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_cache.compare(wbp_index_cache_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. search page_index which exists in wbp_index_cache
  int64_t search_page_virtual_id = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  uint32_t search_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t remove_page_id = mock_cache.mock_index_cache_.at(mock_cache.mock_index_cache_.count() / 5 + 1);
  ret = wbp_.get_page_virtual_id(fd, remove_page_id, search_page_virtual_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_index_cache_.binary_search(search_page_virtual_id, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(search_page_id, remove_page_id);

  // 5. search page_index which doesn't exist in wbp_index_cache and whose virtual id is not in the range of wbp_index_cache.
  //    the range of cache is [the page_virtual_id of the first index, INFINITY)
  ret = wbp_index_cache_.binary_search(1, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTmpFileGlobal::INVALID_PAGE_ID, search_page_id);

  // 6. search page_index which doesn't exist in wbp_index_cache and whose virtual id is in the range of wbp_index_cache.
  //    the range of cache is [the page_virtual_id of the first index, INFINITY)
  int64_t page_virtual_id1 = -1;
  int64_t page_virtual_id2 = -1;
  ret = wbp_.get_page_virtual_id(fd, mock_cache.mock_index_cache_.at(0), page_virtual_id1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_.get_page_virtual_id(fd, mock_cache.mock_index_cache_.at(1), page_virtual_id2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(page_virtual_id2 - page_virtual_id1, 1);
  search_page_virtual_id = page_virtual_id1 + 1;
  ret = wbp_index_cache_.binary_search(page_virtual_id1, search_page_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t res_virtual_id = -1;
  ret = wbp_.get_page_virtual_id(fd, search_page_id, res_virtual_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(page_virtual_id1, res_virtual_id);

  LOG_INFO("test_search");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_write_buffer_pool_index_cache.log*");
  OB_LOGGER.set_file_name("test_tmp_file_write_buffer_pool_index_cache.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}