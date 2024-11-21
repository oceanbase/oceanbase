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
#include <gtest/gtest.h>
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_pool.h"
#include "lib/random/ob_random.h"


namespace oceanbase
{
using namespace common;
using namespace tmp_file;
using namespace storage;

static const int64_t WBP_BLOCK_SIZE = ObTmpWriteBufferPool::WBP_BLOCK_SIZE; // each wbp block has 253 pages (253 * 8KB == 2024KB)
static const int64_t TENANT_MEMORY = 8L * 1024L * 1024L * 1024L /* 8 GB */;
static const int64_t SMALL_WBP_MEM_LIMIT = 3 * WBP_BLOCK_SIZE; // the wbp mem size is 5.93MB
static const int64_t BIG_WBP_MEM_LIMIT = 40 * WBP_BLOCK_SIZE; // the wbp mem size is 79.06MB

struct WBPTestHelper
{
public:
  WBPTestHelper(const int64_t fd, ObTmpWriteBufferPool &wbp)
    : fd_(fd),
      data_size_(0),
      data_page_num_(0),
      // meta_page_num_(0),
      data_page_ids_(),
      wbp_(wbp) {}
  int alloc_data_pages(const int64_t num);
  // int alloc_meta_pages(const int64_t num);
  int free_all_pages(std::vector<uint32_t> *reserve_page=nullptr);
public:
  struct PageInfo
  {
    uint32_t page_id_;
    int64_t virtual_id_;
    PageInfo(uint32_t page_id, int64_t vid) : page_id_(page_id), virtual_id_(vid) {}
    TO_STRING_KV(K(page_id_), K(virtual_id_));
  };
public:
  int64_t fd_;
  int64_t data_size_;
  int64_t data_page_num_;
  // int64_t meta_page_num_;
  std::vector<PageInfo> data_page_ids_;
  uint32_t end_page_id_;
  ObTmpWriteBufferPool &wbp_;
};

int WBPTestHelper::alloc_data_pages(const int64_t num)
{
  int ret = OB_SUCCESS;
  uint32_t previous_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    int64_t virtual_page_id = data_size_ / ObTmpFileGlobal::PAGE_SIZE;
    if (OB_FAIL(wbp_.alloc_page(fd_, ObTmpFilePageUniqKey(virtual_page_id), new_page_id, buf))) {
      LOG_WARN("fail to alloc page", K(fd_), K(previous_page_id));
    } else if (FALSE_IT(data_page_ids_.push_back(PageInfo(new_page_id, virtual_page_id)))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (ObTmpFileGlobal::INVALID_PAGE_ID != previous_page_id &&
               OB_FAIL(wbp_.link_page(fd_, new_page_id, previous_page_id, ObTmpFilePageUniqKey(virtual_page_id - 1)))) {
      LOG_WARN("fail to link page", K(fd_), K(new_page_id), K(previous_page_id), K(virtual_page_id - 1));
    } else {
      data_size_ += ObTmpFileGlobal::PAGE_SIZE;
      previous_page_id = new_page_id;
    }
  }
  return ret;
}

// page in reserve_page is not released
int WBPTestHelper::free_all_pages(std::vector<uint32_t> *reserve_page)
{
  int ret = OB_SUCCESS;
  std::set<uint32_t> page_set;
  std::vector<PageInfo> tmp_vec;
  if (nullptr != reserve_page) {
    page_set.insert(reserve_page->begin(), reserve_page->end());
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

// TODO: 整理一下这个单测
struct WBPTestFunctor
{
public:
  WBPTestFunctor(const int64_t fd, const int64_t capacity, const int64_t loop,
                 ObTmpWriteBufferPool *wbp)
      : fd_(fd), wbp_capacity_(capacity), loop_(loop), wbp_(wbp), data_(),
        begin_data_page_virtual_id_(-1), end_data_page_virtual_id_(-1) {}
  void operator() ();
  bool check_wbp_data_success();
  bool check_no_page_belong_self();
  void print_deque(std::deque<uint32_t> * dq);
public:
  int64_t fd_;
  int64_t wbp_capacity_;
  int64_t loop_;
  ObTmpWriteBufferPool * wbp_;
  std::deque<uint32_t> data_;
  int64_t begin_data_page_virtual_id_;
  int64_t end_data_page_virtual_id_;
};

/*
 * Randomly allocate several pages (0 ~ `wbp_capacity_`), then randomly free
 * several pages (0 ~ alloced_page_nums), repeat for `loop_` times, and at the
 * end, all pages are automatically returned.
 */
void WBPTestFunctor::operator() ()
{
  // alloc and free
  for (int i = 0; i < loop_; ++i) {
    int ret = OB_SUCCESS;
    // random alloc pages
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *new_page_buf = nullptr;
    uint32_t prev_page_id = data_.size() == 0
                                ? ObTmpFileGlobal::INVALID_PAGE_ID
                                : data_.back();
    int64_t alloc_page_nums = ObRandom::rand(0, wbp_capacity_);
    for (int64_t j = 0; OB_SUCC(ret) && j < alloc_page_nums; ++j) {
      int64_t new_page_begin_virtual_id = end_data_page_virtual_id_ < 0 ? 0 : end_data_page_virtual_id_ + 1;
      ret = wbp_->alloc_page(fd_, ObTmpFilePageUniqKey(new_page_begin_virtual_id), new_page_id, new_page_buf);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (prev_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
        ret = wbp_->link_page(fd_, new_page_id, prev_page_id, ObTmpFilePageUniqKey(end_data_page_virtual_id_));
        ASSERT_EQ(OB_SUCCESS, ret);
      } else {
        begin_data_page_virtual_id_ = new_page_begin_virtual_id;
      }
      data_.push_back(new_page_id);
      LOG_INFO("alloc page succeed", K(fd_), K(new_page_id), K(prev_page_id));
      end_data_page_virtual_id_ = new_page_begin_virtual_id;
      prev_page_id = new_page_id;
    }

    if (!check_wbp_data_success()) {
      std::cout << "check data fail after alloc, loop: " << i << std::endl;
      break;
    }

    int64_t free_page_nums = ObRandom::rand(0, data_.size());
    for (int64_t j = 0; OB_SUCC(ret) && j < free_page_nums && data_.size() > 0; ++j) {
      uint32_t page_to_free = data_.front();
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ret = wbp_->free_page(fd_, page_to_free, ObTmpFilePageUniqKey(begin_data_page_virtual_id_), next_page_id);
      data_.pop_front();
      ASSERT_EQ(ret, OB_SUCCESS);
      begin_data_page_virtual_id_ += 1;
    }

    if (!check_wbp_data_success()) {
      std::cout << "check data fail after free, loop: " << i << std::endl;
      break;
    }
  }
  // free all
  {
    int ret = OB_SUCCESS;
    int64_t total_page_nums = data_.size();
    for (int64_t j = 0; OB_SUCC(ret) && j < total_page_nums; ++j) {
      uint32_t page_to_free = data_.front();
      uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
      ret = wbp_->free_page(fd_, page_to_free, ObTmpFilePageUniqKey(begin_data_page_virtual_id_), next_page_id);
      ASSERT_EQ(ret, OB_SUCCESS);
      data_.pop_front();
      begin_data_page_virtual_id_ += 1;
    }
    if (data_.size() != 0) {
      std::cout << "free all pages error, data size: " << data_.size() << std::endl;
    }
    if (!check_no_page_belong_self()) {
      std::cout << fd_ << " check no page belong self fail" << std::endl;
    }
  }
}

bool WBPTestFunctor::check_wbp_data_success()
{
  bool check_res = true;
  std::deque<uint32_t> wbp_data;
  // collect wbp data
  int ret = OB_SUCCESS;
  uint32_t curr_page_id = data_.size() > 0 ? data_.front() : ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t curr_page_virtual_id = begin_data_page_virtual_id_;
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  char * page_buff = nullptr;
  while (OB_SUCC(ret) && curr_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
    ret = wbp_->read_page(fd_, curr_page_id, ObTmpFilePageUniqKey(curr_page_virtual_id), page_buff, next_page_id);
    if (OB_SUCC(ret)) {
      wbp_data.push_back(curr_page_id);
      curr_page_virtual_id += 1;
    } else {
      std::cout << "fetch page error, ret: " << ret << std::endl;
    }
    curr_page_id = next_page_id;
  }
  // compare data
  if (data_.size() != wbp_data.size()) {
    std::cout << "check wbp data fail, data size: " << data_.size()
              << ", wbp data size: " << wbp_data.size() << std::endl;
    check_res = false;
    print_deque(&data_);
    print_deque(&wbp_data);
  } else {
    for (int64_t i = 0; i < data_.size(); ++i) {
      if (data_.at(i) != wbp_data.at(i)) {
        std::cout << "check wbp data fail, not equal happen at: " << i
                  << ", data: " << data_.at(i)
                  << ", wbp data: " << wbp_data.at(i) << std::endl;
        check_res = false;
        print_deque(&data_);
        print_deque(&wbp_data);
        break;
      }
    }
  }
  return check_res;
}

bool WBPTestFunctor::check_no_page_belong_self()
{
  bool no_page_belong_self = true;
  int64_t page_belong_self_nums = 0;
  for (int64_t i = 0; i < wbp_capacity_; ++i) {
    if (wbp_->fat_[i].fd_ == fd_) {
      std::cout << fd_ << " find self page, idx: " << i
                << ", fd: " << wbp_->fat_[i].fd_
                << ", next_page_id: " << wbp_->fat_[i].next_page_id_
                << std::endl;
      page_belong_self_nums++;
      no_page_belong_self = false;
    }
  }
  if (!no_page_belong_self) {
    std::cout << fd_ << " occupy " << page_belong_self_nums << " pages" << std::endl;
  }
  return no_page_belong_self;
}

void WBPTestFunctor::print_deque(std::deque<uint32_t> * dq)
{
  ObArray<uint32_t> data;
  for (int64_t i = 0; i < dq->size(); ++i) {
    data.push_back(dq->at(i));
  }
  LOG_INFO("print_deque", K(fd_), K(data));
}

static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
static ObSimpleMemLimitGetter getter;

class TestBufferPool : public blocksstable::TestDataFilePrepare
{
public:
  TestBufferPool()
      : TestDataFilePrepare(&getter, "TestBufferPool", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT) {}
  virtual ~TestBufferPool() = default;
  virtual void SetUp();
  virtual void TearDown();
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
}

TEST_F(TestBufferPool, test_buffer_pool_basic)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  WBPTestFunctor wbp_test_functor(0, ObTmpWriteBufferPool::BLOCK_PAGE_NUMS, 100, &wbp);
  wbp_test_functor();
}

TEST_F(TestBufferPool, test_buffer_pool_concurrent)
{
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.get_write_buffer_pool();
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  const int64_t MAX_THREAD_NUM = 5;
  const int64_t MAX_LOOP_NUM = 100;
  std::vector<std::thread> t_vec;
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    WBPTestFunctor functor = WBPTestFunctor(10 + i, ObTmpWriteBufferPool::BLOCK_PAGE_NUMS, MAX_LOOP_NUM, &wbp);
    t_vec.push_back(std::thread(functor));
  }
  for (int64_t i = 0; i < t_vec.size(); ++i) {
    t_vec[i].join();
  }
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
  int64_t max_page_num = wbp.get_max_page_num();
  std::cout << "write buffer pool max page num " << max_page_num << std::endl;
  LOG_INFO("write buffer pool max page num", K(max_page_num));
  int64_t fd = 0;
  int64_t offset = 0;
  uint32_t data_head_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  uint32_t cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  // 分配 50% 的 data page
  const int64_t BATCH_ALLOC_DATA_PAGE_NUM = max_page_num / 2;
  int64_t cur_page_virtual_id = 0;
  for (int64_t i = 0; i < BATCH_ALLOC_DATA_PAGE_NUM; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    ret = wbp.alloc_page(fd, ObTmpFilePageUniqKey(cur_page_virtual_id), new_page_id, buf); // TODO: 替换成wbp_test
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_NE(buf, nullptr);
    data_head_page_id = ObTmpFileGlobal::INVALID_PAGE_ID == data_head_page_id ? new_page_id : data_head_page_id;
    if (ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
      ret = wbp.link_page(fd, new_page_id, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id - 1));
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    cur_page_virtual_id += 1;
    cur_page_id = new_page_id;
  }

  // 再分配 50% 的 data page，超过 MAX_DATA_PAGE_USAGE_RATIO(default 0.9) 后
  // 会触发 OB_ALLOCATE_TMP_FILE_PAGE_FAILED，分配页面失败
  for (int64_t i = 0; OB_SUCC(ret) &&  i < BATCH_ALLOC_DATA_PAGE_NUM; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    ret = wbp.alloc_page(fd, ObTmpFilePageUniqKey(cur_page_virtual_id), new_page_id, buf);
    if (ret == OB_ALLOCATE_TMP_FILE_PAGE_FAILED) {
      break;
    }
    ASSERT_EQ(ret, OB_SUCCESS);
    if (cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      ret = wbp.link_page(fd, new_page_id, cur_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id - 1));
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    cur_page_id = new_page_id;
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(ret, OB_ALLOCATE_TMP_FILE_PAGE_FAILED);

  // 此时仍可分配少量 meta page(buffer pool最小为2MB，为meta page预留空间最少为25页)
  cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  int64_t meta_page_num = 0;
  for (int64_t i = 0; i < std::max(max_page_num * 0.01, 20.0); ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    ObTmpFilePageUniqKey meta_page_offset(1, meta_page_num);
    ret = wbp.alloc_page(fd, meta_page_offset, new_page_id, buf);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_NE(buf, nullptr);
    if (ObTmpFileGlobal::INVALID_PAGE_ID != cur_page_id) {
      ret = wbp.link_page(fd, new_page_id, cur_page_id, ObTmpFilePageUniqKey(1, meta_page_num-1));
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    cur_page_id = new_page_id;
    meta_page_num += 1;
  }

  // 分配 meta page 到buffer pool上限
  cur_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < max_page_num; ++i) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    ObTmpFilePageUniqKey meta_page_offset(1, meta_page_num);
    ret = wbp.alloc_page(fd, meta_page_offset, new_page_id, buf);
    if (ret == OB_ALLOCATE_TMP_FILE_PAGE_FAILED) {
      break;
    }
    ASSERT_EQ(ret, OB_SUCCESS);
    if (cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      ret = wbp.link_page(fd, new_page_id, cur_page_id, ObTmpFilePageUniqKey(1, meta_page_num -1));
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    cur_page_id = new_page_id;
    meta_page_num += 1;
  }
  ASSERT_EQ(ret, OB_ALLOCATE_TMP_FILE_PAGE_FAILED);
  ASSERT_EQ(wbp.data_page_cnt_ + wbp.meta_page_cnt_, wbp.used_page_num_);
  ASSERT_EQ(wbp.used_page_num_, wbp.get_memory_limit() / ObTmpFileGlobal::PAGE_SIZE);

  // data page释放后，可以继续分配meta page
  int64_t cur_meta_page_num = wbp.meta_page_cnt_;
  int64_t free_page_id = data_head_page_id;
  const int64_t FREE_DATA_PAGE_NUM = max_page_num / 2;
  int64_t free_cnt = 0;
  cur_page_virtual_id = 0;
  for (; free_cnt < FREE_DATA_PAGE_NUM && ObTmpFileGlobal::INVALID_PAGE_ID != free_page_id; ++free_cnt) {
    uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    ret = wbp.free_page(fd, free_page_id, ObTmpFilePageUniqKey(cur_page_virtual_id), next_page_id);
    free_page_id = next_page_id;
    ASSERT_EQ(ret, OB_SUCCESS);
    cur_page_virtual_id += 1;
  }
  ASSERT_EQ(free_cnt, FREE_DATA_PAGE_NUM);
  int64_t alloc_cnt = 0;
  for (; OB_SUCC(ret) && alloc_cnt < FREE_DATA_PAGE_NUM; ++alloc_cnt) {
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    char *buf = nullptr;
    ObTmpFilePageUniqKey meta_page_offset(1, meta_page_num);
    ret = wbp.alloc_page(fd, meta_page_offset, new_page_id, buf);
    ASSERT_EQ(ret, OB_SUCCESS);
    if (cur_page_id != ObTmpFileGlobal::INVALID_PAGE_ID) {
      ret = wbp.link_page(fd, new_page_id, cur_page_id, ObTmpFilePageUniqKey(1, meta_page_num - 1));
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    cur_page_id = new_page_id;
    meta_page_num += 1;
  }
  printf("total page num: %ld, data page: %ld, meta page: %ld, capacity: %ld\n", wbp.used_page_num_, wbp.meta_page_cnt_, wbp.data_page_cnt_, wbp.capacity_);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(wbp.meta_page_cnt_, FREE_DATA_PAGE_NUM + cur_meta_page_num);
  ASSERT_EQ(wbp.meta_page_cnt_ + wbp.data_page_cnt_, wbp.used_page_num_);
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
  wbp_test.free_all_pages(&shrink_range);

  ASSERT_EQ(2, wbp.used_page_num_);
  for (int32_t i = 0; i < 5; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }
  // pages in shrinking range are not freed, shrinking could no progress
  EXPECT_EQ(WBPShrinkContext::SHRINKING_SWAP, wbp.shrink_ctx_.wbp_shrink_state_);
  EXPECT_TRUE(shrink_lower_bound > wbp.shrink_ctx_.max_allow_alloc_page_id_);

  // all pages in shrinking range are freed, shrinking finish
  shrink_range.pop_back();
  wbp_test.free_all_pages(&shrink_range);
  for (int32_t i = 0; i < 5; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
  }

  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());
  MockIO.check_wbp_free_list(wbp);
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
