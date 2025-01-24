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
#include "mittest/mtlenv/storage/tmp_file/mock_ob_tmp_file.h"
#define USING_LOG_PREFIX STORAGE
#include "mittest/mtlenv/mock_tenant_module_env.h"


namespace oceanbase
{
using namespace common;
using namespace tmp_file;
using namespace storage;

static ObSimpleMemLimitGetter getter;
static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;

static const int64_t WBP_BLOCK_SIZE = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; // each wbp block has 253 pages (253 * 8KB == 2024KB)
static const int64_t TENANT_MEMORY = 8L * 1024L * 1024L * 1024L /* 8 GB */;
static const int64_t SMALL_WBP_MEM_LIMIT = 3 * WBP_BLOCK_SIZE; // the wbp mem size is 5.93MB
static const int64_t BIG_WBP_MEM_LIMIT = 40 * WBP_BLOCK_SIZE; // the wbp mem size is 79.06MB

struct WBPTestHelper
{
public:
  WBPTestHelper(const int64_t fd, ObTmpWriteBufferPool &wbp)
    : fd_(fd),
      data_page_num_(0),
      data_page_ids_(),
      wbp_(wbp) {}
  int alloc_data_pages(const int64_t num, bool notify_dirty = false);
  int alloc_meta_pages(const int64_t num, bool notify_dirty = false);
  int free_all_pages();
  // reserve pages in 'reserve_pages' and free other pages
  int free_data_pages_except(std::vector<uint32_t> &reserve_pages);
  // free n pages at the beginning
  int free_data_pages(const uint64_t num);
  int free_meta_pages(const uint64_t num);
  void random_alloc_and_free(int64_t loop_cnt, int64_t max_alloc_num);
  void check_using_page_list();
public:
  struct PageInfo
  {
    uint32_t page_id_;
    int64_t virtual_id_;
    PageInfo(uint32_t page_id, int64_t vid) : page_id_(page_id), virtual_id_(vid) {}
    TO_STRING_KV(K(page_id_), K(virtual_id_));
  };
  struct MetaPageInfo
  {
    uint32_t page_id_;
    int64_t page_level_; // assume all meta pages are at the same level in unit test
    int64_t virtual_id_;
    MetaPageInfo(uint32_t page_id, int64_t vid) : page_id_(page_id), page_level_(0), virtual_id_(vid) {}
    TO_STRING_KV(K(page_id_), K(virtual_id_));
  };
public:
  int64_t fd_;
  int64_t data_page_num_;
  std::vector<PageInfo> data_page_ids_;
  std::vector<MetaPageInfo> meta_page_ids_;
  uint32_t end_page_id_;
  ObTmpWriteBufferPool &wbp_;
};

int WBPTestHelper::alloc_data_pages(const int64_t num, bool notify_dirty)
{
  int ret = OB_SUCCESS;
  uint32_t prev_page_id = data_page_ids_.empty() ?
                              ObTmpFileGlobal::INVALID_PAGE_ID :
                              data_page_ids_.back().page_id_;
  int64_t old_data_page_num = data_page_ids_.size();
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    int64_t virtual_page_id = data_page_ids_.empty() ? 0 : data_page_ids_.back().virtual_id_ + 1;
    if (OB_FAIL(wbp_.alloc_page(fd_, ObTmpFilePageUniqKey(virtual_page_id), new_page_id, buf))) {
      LOG_WARN("fail to alloc page", K(fd_), K(prev_page_id));
    } else if (FALSE_IT(data_page_ids_.push_back(PageInfo(new_page_id, virtual_page_id)))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (ObTmpFileGlobal::INVALID_PAGE_ID != prev_page_id &&
               OB_FAIL(wbp_.link_page(fd_, new_page_id, prev_page_id, ObTmpFilePageUniqKey(virtual_page_id - 1)))) {
      LOG_WARN("fail to link page", K(fd_), K(new_page_id), K(prev_page_id), K(virtual_page_id - 1));
    } else if (notify_dirty &&
               OB_FAIL(wbp_.notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(virtual_page_id)))) {
      LOG_WARN("fail to notify dirty", K(fd_), K(new_page_id), K(virtual_page_id));
    } else {
      prev_page_id = new_page_id;
    }
  }

  if (OB_FAIL(ret)) {
    int64_t allocated_page_num = data_page_ids_.size() - old_data_page_num;
    LOG_INFO("fail to alloc data pages", K(num), K(allocated_page_num));
  }
  return ret;
}

int WBPTestHelper::alloc_meta_pages(const int64_t num, bool notify_dirty)
{
  int ret = OB_SUCCESS;
  uint32_t prev_page_id = meta_page_ids_.empty() ?
                          ObTmpFileGlobal::INVALID_PAGE_ID :
                          meta_page_ids_.back().page_id_;
  int64_t old_meta_page_num = meta_page_ids_.size();
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    int64_t page_level = meta_page_ids_.empty() ? 0 : meta_page_ids_.back().page_level_;
    int64_t virtual_page_id = meta_page_ids_.empty() ? 0 : meta_page_ids_.back().virtual_id_ + 1;
    if (OB_FAIL(wbp_.alloc_page(fd_, ObTmpFilePageUniqKey(page_level, virtual_page_id), new_page_id, buf))) {
      LOG_WARN("fail to alloc page", K(fd_), K(prev_page_id));
    } else if (FALSE_IT(meta_page_ids_.push_back(MetaPageInfo(new_page_id, virtual_page_id)))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (ObTmpFileGlobal::INVALID_PAGE_ID != prev_page_id &&
               OB_FAIL(wbp_.link_page(fd_, new_page_id, prev_page_id,
                                      ObTmpFilePageUniqKey(page_level, virtual_page_id - 1)))) {
      LOG_WARN("fail to link page", K(fd_), K(new_page_id), K(prev_page_id), K(virtual_page_id - 1));
    } else if (notify_dirty &&
               OB_FAIL(wbp_.notify_dirty(fd_, new_page_id, ObTmpFilePageUniqKey(page_level, virtual_page_id)))) {
      LOG_WARN("fail to notify dirty", K(fd_), K(new_page_id), K(page_level), K(virtual_page_id));
    } else {
      prev_page_id = new_page_id;
    }
  }

  if (OB_FAIL(ret)) {
    int64_t allocated_page_num = meta_page_ids_.size() - old_meta_page_num;
    LOG_INFO("fail to alloc meta pages", K(num), K(allocated_page_num));
  }
  return ret;
}

int WBPTestHelper::free_all_pages()
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < data_page_ids_.size(); ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    PageInfo &page_info = data_page_ids_.at(i);
    if (OB_FAIL(wbp_.free_page(fd_, page_info.page_id_,
                               ObTmpFilePageUniqKey(page_info.virtual_id_),
                               next_page_id))) {
      LOG_WARN("fail to free page", K(fd_), K(data_page_ids_.at(i)));
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < meta_page_ids_.size(); ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    MetaPageInfo &page_info = meta_page_ids_.at(i);
    if (OB_FAIL(wbp_.free_page(fd_, page_info.page_id_,
                               ObTmpFilePageUniqKey(page_info.page_level_, page_info.virtual_id_),
                               next_page_id))) {
      LOG_WARN("fail to free page", K(fd_), K(meta_page_ids_.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    data_page_ids_.clear();
    meta_page_ids_.clear();
  }
  return ret;
}

// data pages in 'reserve_page' are not released
int WBPTestHelper::free_data_pages_except(std::vector<uint32_t> &reserve_page)
{
  int ret = OB_SUCCESS;
  std::set<uint32_t> page_set;
  std::vector<PageInfo> tmp_vec;
  if (reserve_page.size() > 0) {
    page_set.insert(reserve_page.begin(), reserve_page.end());
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < data_page_ids_.size(); ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    PageInfo &page_info = data_page_ids_.at(i);
    if (page_set.count(page_info.page_id_) != 0) {
      tmp_vec.push_back(page_info);
      LOG_DEBUG("skip free page", K(page_info.page_id_));
      continue;
    }
    if (OB_FAIL(wbp_.free_page(fd_, page_info.page_id_, ObTmpFilePageUniqKey(page_info.virtual_id_), next_page_id))) {
      LOG_WARN("fail to free page", K(fd_), K(data_page_ids_.at(i)));
    }
  }
  data_page_ids_.swap(tmp_vec);
  return ret;
}

int WBPTestHelper::free_data_pages(const uint64_t num)
{
  int ret = OB_SUCCESS;
  int64_t actual_num = std::min(num, data_page_ids_.size());
  for (int64_t i = 0; OB_SUCC(ret) && i < actual_num; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    PageInfo &page_info = data_page_ids_.at(i);
    if (OB_FAIL(wbp_.free_page(fd_, page_info.page_id_,
                               ObTmpFilePageUniqKey(page_info.virtual_id_),
                               next_page_id))) {
      LOG_WARN("fail to free page", K(fd_), K(data_page_ids_.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    data_page_ids_.erase(data_page_ids_.begin(), data_page_ids_.begin() + actual_num);
  }
  if (actual_num < num) {
    LOG_INFO("data pages are less than expected in WBPTestHelper::free_data_pages", K(num), K(actual_num));
  }
  return ret;
}

int WBPTestHelper::free_meta_pages(const uint64_t num)
{
  int ret = OB_SUCCESS;
  int64_t actual_num = std::min(num, meta_page_ids_.size());
  for (int64_t i = 0; OB_SUCC(ret) && i < actual_num; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    MetaPageInfo &page_info = meta_page_ids_.at(i);
    if (OB_FAIL(wbp_.free_page(fd_, page_info.page_id_,
                               ObTmpFilePageUniqKey(page_info.page_level_, page_info.virtual_id_),
                               next_page_id))) {
      LOG_WARN("fail to free page", K(fd_), K(meta_page_ids_.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    meta_page_ids_.erase(meta_page_ids_.begin(), meta_page_ids_.begin() + actual_num);
  }
  if (actual_num < num) {
    LOG_INFO("meta pages are less than expected in WBPTestHelper::free_meta_pages", K(num), K(actual_num));
  }
  return ret;
}

void WBPTestHelper::random_alloc_and_free(int64_t loop_cnt, int64_t max_alloc_num)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; i < loop_cnt; ++i) {
    int64_t rand_page_num = ObRandom::rand(0, max_alloc_num);
    ret = alloc_data_pages(rand_page_num);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ALLOCATE_TMP_FILE_PAGE_FAILED == ret);

    check_using_page_list();

    ret = free_all_pages();
    ASSERT_EQ(ret, OB_SUCCESS);
  }
}

void WBPTestHelper::check_using_page_list()
{
  int ret = OB_SUCCESS;
  if (data_page_ids_.size() > 0) {
    PageInfo &page_info = data_page_ids_.at(0);
    uint32_t cur_page_id = page_info.page_id_;
    int64_t virtual_page_id = page_info.virtual_id_;

    int64_t page_list_idx = 0;

    while (OB_SUCC(ret) && ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      EXPECT_EQ(cur_page_id, data_page_ids_.at(page_list_idx).page_id_);
      if (cur_page_id != data_page_ids_.at(page_list_idx).page_id_) {
        LOG_ERROR("recorded page id not equal to fetched page id", K(cur_page_id),
            K(page_list_idx), K(data_page_ids_.at(page_list_idx).page_id_));
        abort();
      }

      ret = wbp_.get_next_page_id(fd_, cur_page_id, ObTmpFilePageUniqKey(virtual_page_id), next_page_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      cur_page_id = next_page_id;
      virtual_page_id += 1;

      page_list_idx += 1;
    }

    ASSERT_EQ(page_list_idx, data_page_ids_.size());
  }
}

class TestBufferPool : public blocksstable::TestDataFilePrepare
{
public:
  TestBufferPool()
      : TestDataFilePrepare(&getter, "TestBufferPool", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT) {}
  virtual ~TestBufferPool() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
};

void TestBufferPool::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();

  lib::set_memory_limit(128LL << 32);
  lib::set_tenant_memory_limit(OB_SYS_TENANT_ID, 128LL << 32);

  CHUNK_MGR.set_limit(128LL << 32);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), 128LL << 32);

  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpBlockCache::get_instance().init("tmp_block_cache", 1));
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpPageCache::get_instance().init("tmp_page_cache", 1));
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  ASSERT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);

  ObTimerService *timer_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  tenant_ctx.set(timer_service);

  MockTenantTmpFileManager *tf_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mtl_new_default(tf_mgr));
  ASSERT_EQ(OB_SUCCESS, tf_mgr->init());
  tenant_ctx.set(tf_mgr);

  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ObTenantEnv::set_tenant(&tenant_ctx);

  ASSERT_NE(nullptr, MTL(ObTenantTmpFileManager *));
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;

  MockIO.reset();
}

void TestBufferPool::TearDown()
{
  MockIO.reset();
  tmp_file::ObTenantTmpFileManager *tmp_file_mgr = MTL(tmp_file::ObTenantTmpFileManager *);
  if (OB_NOT_NULL(tmp_file_mgr)) {
    tmp_file_mgr->destroy();
  }
  tmp_file::ObTmpBlockCache::get_instance().destroy();
  tmp_file::ObTmpPageCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
  common::ObClockGenerator::destroy();

  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
}

TEST_F(TestBufferPool, test_buffer_pool_basic)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  const int64_t MAX_LOOP_NUM = 20;
  WBPTestHelper wbp_test_helper(0/*fd*/, wbp);
  wbp_test_helper.random_alloc_and_free(MAX_LOOP_NUM, wbp.get_max_data_page_num());
  MockIO.check_wbp_free_list(wbp);
}

TEST_F(TestBufferPool, test_buffer_pool_concurrent)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  const int64_t MAX_THREAD_NUM = 10;
  const int64_t MAX_LOOP_NUM = 100;
  std::vector<std::thread> threads;
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    auto functor =[&wbp, i]() {
      WBPTestHelper wbp_test_helper(i/*fd*/, wbp);
      wbp_test_helper.random_alloc_and_free(MAX_LOOP_NUM, wbp.get_max_data_page_num());
    };
    threads.push_back(std::thread(functor));
  }
  for (int64_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  MockIO.check_wbp_free_list(wbp);
}

TEST_F(TestBufferPool, test_entry_state_switch_write_back)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t ALLOC_PAGE_NUM = 200;
  WBPTestHelper wbp_test(fd, wbp);
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  // dirty
  uint32_t cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  int64_t cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_dirty(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.dirty_page_num_);

  // write back
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_write_back(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(0, wbp.dirty_page_num_);

  // write back fail, page entry return to dirty
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_write_back_fail(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.dirty_page_num_);

  // write back again
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_write_back(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }

  // write back succ
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_write_back_succ(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(0, wbp.dirty_page_num_);

  // write back succ re-entrant
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_write_back_succ(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(0, wbp.dirty_page_num_);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBufferPool, test_entry_state_switch_loading)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t ALLOC_PAGE_NUM = 200;
  WBPTestHelper wbp_test(fd, wbp);
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  // load
  uint32_t cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  int64_t cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_load(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(wbp.is_loading(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)));
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }

  // load fail
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_load_fail(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(wbp.is_exist(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)));
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }

  // load again
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_load(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(wbp.is_loading(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)));
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }

  // load succ
  cur_page_id = wbp_test.data_page_ids_.at(0).page_id_;
  cur_page_virtual_id = 0;
  for (int64_t i = 0; i < ALLOC_PAGE_NUM; ++i) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.get_next_page_id(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = wbp.notify_load_succ(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(wbp.is_cached(fd, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id)));
    cur_page_id = next_page_id;
    cur_page_virtual_id += 1;
  }

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBufferPool, test_alloc_page_limit)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t ALLOC_PAGE_NUM = wbp.get_max_page_num() / 2;
  WBPTestHelper wbp_test(fd, wbp);

  // 分配 50% 的 data page
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 再分配 50% 的 data page，超过 MAX_DATA_PAGE_USAGE_RATIO(default 0.9) 后
  // 会触发 OB_ALLOCATE_TMP_FILE_PAGE_FAILED，分配页面失败
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(ret, OB_ALLOCATE_TMP_FILE_PAGE_FAILED);

  // 此时仍可分配少量 meta page
  const int ALLOC_META_NUM = 20;
  ret = wbp_test.alloc_meta_pages(ALLOC_META_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 分配 meta page 到buffer pool上限
  ret = wbp_test.alloc_meta_pages(wbp.get_max_page_num());
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);

  // data page释放后，可以继续分配meta page
  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = wbp_test.alloc_meta_pages(wbp.get_max_page_num() * 2);
  ASSERT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);

  ASSERT_EQ(wbp.get_max_page_num(), wbp.used_page_num_);
  ASSERT_EQ(wbp.meta_page_cnt_, wbp.used_page_num_);
}

TEST_F(TestBufferPool, test_get_page_id_by_offset)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t ALLOC_PAGE_NUM = 400;
  WBPTestHelper wbp_test(fd, wbp);
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  uint32_t page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ret = wbp.get_page_id_by_virtual_id(fd, 0, wbp_test.data_page_ids_.at(0).page_id_, page_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(page_id, ObTmpFileGlobal::INVALID_PAGE_ID);
  ASSERT_EQ(page_id, wbp_test.data_page_ids_.at(0).page_id_);

  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ret = wbp.get_page_id_by_virtual_id(fd, 1, wbp_test.data_page_ids_.at(0).page_id_, page_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(page_id, ObTmpFileGlobal::INVALID_PAGE_ID);
  ASSERT_EQ(page_id, wbp_test.data_page_ids_.at(1).page_id_);

  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ret = wbp.get_page_id_by_virtual_id(fd, ALLOC_PAGE_NUM - 1, wbp_test.data_page_ids_.at(0).page_id_, page_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_NE(page_id, ObTmpFileGlobal::INVALID_PAGE_ID);
  ASSERT_EQ(page_id, wbp_test.data_page_ids_.at(ALLOC_PAGE_NUM - 1).page_id_);

  // offset out of bound, return INVALID_PAGE_ID
  page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ret = wbp.get_page_id_by_virtual_id(fd, ALLOC_PAGE_NUM, wbp_test.data_page_ids_.at(0).page_id_, page_id);
  ASSERT_EQ(page_id, ObTmpFileGlobal::INVALID_PAGE_ID);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBufferPool, test_truncate_page)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  const int64_t ALLOC_PAGE_NUM = 200;
  WBPTestHelper wbp_test(fd, wbp);
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = wbp.truncate_page(fd, wbp_test.data_page_ids_.at(0).page_id_, ObTmpFilePageUniqKey(0), -1);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  ret = wbp.truncate_page(fd, wbp_test.data_page_ids_.at(0).page_id_, ObTmpFilePageUniqKey(0), 0);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  ret = wbp.truncate_page(2, wbp_test.data_page_ids_.at(0).page_id_, ObTmpFilePageUniqKey(0), 4096);
  ASSERT_EQ(ret, OB_STATE_NOT_MATCH);

  const int64_t truncate_size = 4096;
  ret = wbp.truncate_page(fd, wbp_test.data_page_ids_.at(0).page_id_, ObTmpFilePageUniqKey(0), truncate_size);
  ASSERT_EQ(ret, OB_SUCCESS);

  char null_buf[truncate_size];
  memset(null_buf, 0, sizeof(null_buf));
  char *page_buf = nullptr;
  uint32_t unused_next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  ret = wbp.read_page(fd, wbp_test.data_page_ids_.at(0).page_id_, ObTmpFilePageUniqKey(0), page_buf, unused_next_page_id);
  ASSERT_EQ(ret, OB_SUCCESS);
  int cmp = memcmp(null_buf, page_buf, truncate_size);
  ASSERT_EQ(cmp, 0);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBufferPool, test_empty_buffer_pool_shrink)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  // alloc pages to expand to mem limit
  int64_t fd = 0;
  WBPTestHelper wbp_test(fd, wbp);
  const int64_t ALLOC_PAGE_NUM = BIG_WBP_MEM_LIMIT * 0.9 / ObTmpFileGlobal::PAGE_SIZE;
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.used_page_num_);
  ASSERT_EQ(BIG_WBP_MEM_LIMIT, wbp.capacity_);

  // shrink to SMALL_WBP_MEM_LIMIT
  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  ASSERT_EQ(SMALL_WBP_MEM_LIMIT, wbp.capacity_);

  // shrink to WBP_BLOCK_SIZE
  wbp.default_wbp_memory_limit_ = WBP_BLOCK_SIZE;
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  ASSERT_EQ(WBP_BLOCK_SIZE, wbp.capacity_);
}

TEST_F(TestBufferPool, test_buffer_pool_shrink)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  int ret = OB_SUCCESS;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  // alloc pages to expand to mem limit
  int64_t fd = 0;
  WBPTestHelper wbp_test(fd, wbp);
  const int64_t ALLOC_PAGE_NUM = BIG_WBP_MEM_LIMIT * 0.9 / ObTmpFileGlobal::PAGE_SIZE;
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.used_page_num_);
  ASSERT_EQ(BIG_WBP_MEM_LIMIT, wbp.capacity_);

  // shrinking could not progress when wbp fill with pages
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }

  // shrinking complete
  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  ASSERT_EQ(SMALL_WBP_MEM_LIMIT, wbp.capacity_);
}

TEST_F(TestBufferPool, test_buffer_pool_shrink_abort)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  int ret = OB_SUCCESS;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  // alloc pages to expand to mem limit
  int64_t fd = 0;
  WBPTestHelper wbp_test(fd, wbp);
  const int64_t ALLOC_PAGE_NUM = BIG_WBP_MEM_LIMIT * 0.9 / ObTmpFileGlobal::PAGE_SIZE;
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.used_page_num_);
  ASSERT_EQ(BIG_WBP_MEM_LIMIT, wbp.capacity_);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1. abort in SHRINKING_SWAP
  LOG_INFO("test abort in SHRINKING_SWAP", K(wbp.shrink_ctx_));
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  mock_swap_tg.shrink_wbp_if_needed_();
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  mock_swap_tg.shrink_wbp_if_needed_();
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  // 2. abort in SHRINKING_RELEASE_BLOCKS
  LOG_INFO("test abort in SHRINKING_RELEASE_BLOCKS", K(wbp.shrink_ctx_));
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  for (int32_t i = 0; i < 2; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  EXPECT_EQ(WBPShrinkContext::SHRINKING_RELEASE_BLOCKS, wbp.shrink_ctx_.wbp_shrink_state_);
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  mock_swap_tg.shrink_wbp_if_needed_();
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  // 3. abort in flush error code OB_SERVER_OUTOF_DISK_SPACE
  LOG_INFO("test abort in error OB_SERVER_OUTOF_DISK_SPACE", K(wbp.shrink_ctx_));
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  for (int32_t i = 0; i < 5; i++) {
    if (i >= 1) {
      mock_swap_tg.flush_tg_ref_.flush_io_finished_ret_ = OB_SERVER_OUTOF_DISK_SPACE;
    }
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  // 4. works in old size, allocates all pages normally
  MockIO.check_wbp_free_list(wbp);
}

TEST_F(TestBufferPool, test_buffer_pool_shrink_range_boundary)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  int ret = OB_SUCCESS;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;

  int64_t fd = 0;
  WBPTestHelper wbp_test(fd, wbp);
  const int64_t ALLOC_PAGE_NUM = BIG_WBP_MEM_LIMIT * 0.9 / ObTmpFileGlobal::PAGE_SIZE;
  ret = wbp_test.alloc_data_pages(ALLOC_PAGE_NUM);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ALLOC_PAGE_NUM, wbp.used_page_num_);
  ASSERT_EQ(BIG_WBP_MEM_LIMIT, wbp.capacity_);

  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  for (int32_t i = 0; i < 2; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  uint32_t shrink_lower_bound = wbp.shrink_ctx_.lower_page_id_;

  // keep 2 pages in wbp
  std::vector<uint32_t> shrink_range = {shrink_lower_bound - 1, shrink_lower_bound};
  wbp_test.free_data_pages_except(shrink_range);

  ASSERT_EQ(2, wbp.used_page_num_);
  for (int32_t i = 0; i < 5; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  // pages in shrinking range are not freed, shrinking could no progress
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_TRUE(shrink_lower_bound > wbp.shrink_ctx_.max_allow_alloc_page_id_);

  // all pages in shrinking range are freed, shrinking finish
  shrink_range.pop_back();
  wbp_test.free_data_pages_except(shrink_range);
  for (int32_t i = 0; i < 5; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }

  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  MockIO.check_wbp_free_list(wbp);
}

// 检查缩容期间，free page水位线变化时，预留free meta page数量是否符合预期
TEST_F(TestBufferPool, test_buffer_pool_free_page_when_shrinking)
{
  int ret = OB_SUCCESS;
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT * 2;
  int64_t fd = 0;
  const int64_t WBP_MAX_PAGE_NUM = wbp.get_max_page_num();
  const int64_t ALLOC_DATA_PAGE_NUM = wbp.get_max_data_page_num();
  const int64_t ALLOC_META_PAGE_NUM = WBP_MAX_PAGE_NUM - ALLOC_DATA_PAGE_NUM;
  bool notify_dirty = true;
  WBPTestHelper wbp_test(fd, wbp);
  ret = wbp_test.alloc_data_pages(ALLOC_DATA_PAGE_NUM, notify_dirty); // data: 90%
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_test.alloc_meta_pages(ALLOC_META_PAGE_NUM, notify_dirty); // meta: 10%
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(wbp.used_page_num_, WBP_MAX_PAGE_NUM);

  LOG_INFO("init wbp state 1");
  wbp.print_statistics();
  // wbp shrinking from 80 * WBP_BLOCK to 6 * WBP_BLOCK
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT * 2;

  // free_page_num = 0%
  mock_swap_tg.shrink_wbp_if_needed_();
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_EQ(0, WBP_MAX_PAGE_NUM - wbp.used_page_num_);
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, wbp_test.alloc_meta_pages(100));
  LOG_INFO("wbp state 2", K(wbp.shrink_ctx_));
  wbp.print_statistics();

  // free_page_num = 20%
  // alloc 10% meta before return OB_ALLOCATE_TMP_FILE_PAGE_FAILED
  ret = wbp_test.free_data_pages(WBP_MAX_PAGE_NUM * 0.20); // data: 90% -> 70%
  ASSERT_EQ(OB_SUCCESS, ret);
  mock_swap_tg.shrink_wbp_if_needed_(); // 5% pages can not be allocated as new page now
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.20, WBP_MAX_PAGE_NUM - wbp.used_page_num_);
  LOG_INFO("wbp state 3", K(wbp.shrink_ctx_));
  wbp.print_statistics();
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.05, wbp.shrink_ctx_.upper_page_id_ - wbp.shrink_ctx_.max_allow_alloc_page_id_);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  // 验证先分配data到上限后，仍能分配10%的meta page
  ret = wbp_test.alloc_data_pages(WBP_MAX_PAGE_NUM, notify_dirty);    // data: -> 85%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  ret = wbp_test.alloc_meta_pages(ALLOC_META_PAGE_NUM, notify_dirty); // meta: -> 10%
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.10, wbp.meta_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.85, wbp.data_page_cnt_);
  LOG_INFO("wbp state 4", K(wbp.shrink_ctx_));
  wbp.print_statistics();

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  // re-fill wbp(95%) and free 45% data pages
  ret = wbp_test.alloc_meta_pages(ALLOC_META_PAGE_NUM, notify_dirty); // meta: -> 10%
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wbp_test.alloc_data_pages(WBP_MAX_PAGE_NUM, notify_dirty);    // data: -> 85%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  LOG_INFO("wbp state 5-1", K(wbp.shrink_ctx_)); wbp.print_statistics();
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.10, wbp.meta_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.85, wbp.data_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.95, wbp.used_page_num_);  // we can use up to 95% page now due to shrinking
  ret = wbp_test.free_data_pages(WBP_MAX_PAGE_NUM * 0.45); // data: 85% -> 40%
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.50, wbp.used_page_num_);
  mock_swap_tg.shrink_wbp_if_needed_();
  LOG_INFO("wbp state 5-2", K(wbp.shrink_ctx_));
  wbp.print_statistics();
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  // not_allow_alloc_range_size = 30% + 5%(in previous step)
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.35, wbp.shrink_ctx_.upper_page_id_ - wbp.shrink_ctx_.max_allow_alloc_page_id_);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  // re-fill wbp(60%) and free 50% data pages
  ret = wbp_test.alloc_data_pages(WBP_MAX_PAGE_NUM, notify_dirty); // data: -> 55%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  ret = wbp_test.alloc_meta_pages(WBP_MAX_PAGE_NUM, notify_dirty); // meta: -> 10%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.10, wbp.meta_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.55, wbp.data_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.65, wbp.used_page_num_);
  LOG_INFO("wbp state 6-1", K(wbp.shrink_ctx_));
  wbp.print_statistics();
  ret = wbp_test.free_data_pages(WBP_MAX_PAGE_NUM * 0.50);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.05, wbp.data_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.15, wbp.used_page_num_);
  mock_swap_tg.shrink_wbp_if_needed_();
  LOG_INFO("wbp state 6-2", K(wbp.shrink_ctx_));
  wbp.print_statistics();
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.70, wbp.shrink_ctx_.upper_page_id_ - wbp.shrink_ctx_.max_allow_alloc_page_id_);

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  // 脏页减少到5%时，not_alloc_range能覆盖整个shrink_range
  ret = wbp_test.alloc_data_pages(WBP_MAX_PAGE_NUM, notify_dirty); // data: -> 20%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  ret = wbp_test.alloc_meta_pages(WBP_MAX_PAGE_NUM, notify_dirty); // meta: -> 10%
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.20, wbp.data_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.10, wbp.meta_page_cnt_);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.30, wbp.used_page_num_);
  ret = wbp_test.free_data_pages(WBP_MAX_PAGE_NUM * 0.15);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = wbp_test.free_meta_pages(WBP_MAX_PAGE_NUM * 0.10);
  ASSERT_EQ(OB_SUCCESS, ret);
  mock_swap_tg.shrink_wbp_if_needed_();
  LOG_INFO("wbp state 7", K(wbp.shrink_ctx_));
  wbp.print_statistics();
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_EQ(wbp.shrink_ctx_.lower_page_id_ - 1, wbp.shrink_ctx_.max_allow_alloc_page_id_); // all pages in shrink_range are not allowed to alloc

  ret = wbp_test.free_all_pages();
  ASSERT_EQ(OB_SUCCESS, ret);
  // shrink_range内页面无法再被分配，并且可以推动缩容完成
  ret = wbp_test.alloc_meta_pages(WBP_MAX_PAGE_NUM, notify_dirty); // meta: -> 7.5% (wbp shrinking target size)
  EXPECT_EQ(OB_ALLOCATE_TMP_FILE_PAGE_FAILED, ret);
  EXPECT_EQ(WBP_MAX_PAGE_NUM * 0.075, wbp.meta_page_cnt_);
  mock_swap_tg.shrink_wbp_if_needed_();
  mock_swap_tg.shrink_wbp_if_needed_();
  EXPECT_EQ(false, wbp.shrink_ctx_.is_valid());
  ASSERT_EQ(wbp.fat_.size(), wbp.get_max_page_num());
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_buffer_pool.log*");
  OB_LOGGER.set_file_name("test_tmp_file_buffer_pool.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
