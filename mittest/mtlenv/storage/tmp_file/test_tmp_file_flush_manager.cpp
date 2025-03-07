/**
 * Copyright (c) 2021 OceanBase––
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
#define protected public
#define private public
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_meta_tree.h"

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
static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
static ObSimpleMemLimitGetter getter;

class TestTmpFileFlushMgr : public blocksstable::TestDataFilePrepare
{
public:
  TestTmpFileFlushMgr()
      : TestDataFilePrepare(&getter, "TestTmpFileFlushMgr", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT) {}
  virtual ~TestTmpFileFlushMgr() = default;
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

  void create_tmp_files(const int64_t file_cnt, ObArray<ObSNTmpFileHandle> &tmp_file_handles);
  void generate_write_data(char *&write_buf, const int64_t write_size);
};

void TestTmpFileFlushMgr::SetUp()
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
  ObSharedNothingTmpFileMetaTree::set_max_array_item_cnt(MAX_DATA_ITEM_ARRAY_COUNT);
  ObSharedNothingTmpFileMetaTree::set_max_page_item_cnt(MAX_PAGE_ITEM_COUNT);

  MockIO.reset();
}

void TestTmpFileFlushMgr::TearDown()
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

void TestTmpFileFlushMgr::create_tmp_files(const int64_t file_cnt, ObArray<ObSNTmpFileHandle> &tmp_file_handles)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  int64_t fd = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < file_cnt; ++i) {
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    if (file_cnt <= 32) {
      std::cout << "open temporary file: " << fd << std::endl;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSNTmpFileHandle file_handle;
    ret = static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.get_tmp_file(fd, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    file_handle.get()->page_idx_cache_.max_bucket_array_capacity_ = SMALL_WBP_IDX_CACHE_MAX_CAPACITY;
    ASSERT_EQ(OB_SUCCESS, tmp_file_handles.push_back(file_handle));
  }
  ASSERT_EQ(file_cnt, tmp_file_handles.count());
}

void TestTmpFileFlushMgr::generate_write_data(char *&write_buf, const int64_t write_size)
{
  write_buf = new char [write_size];
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buf[i + j] = random_int;
    }
    i += random_length;
  }
}

void remove_all_files_and_check_state(ObArray<ObSNTmpFileHandle> &tmp_file_handles,
                                      ObTmpFileFlushTG &flush_tg,
                                      ObTmpFilePageCacheController &pc_ctrl)
{
  int ret = OB_SUCCESS;
  // remove all tmp files
  LOG_INFO("begin removing tmp files", K(tmp_file_handles.size()));
  for (int64_t i = 0; i < tmp_file_handles.count(); ++i) {
    ObITmpFileHandle &file_handle = tmp_file_handles.at(i);
    if (OB_NOT_NULL(file_handle.get())) {
      int64_t fd = file_handle.get()->get_fd();
      LOG_INFO("removing tmp file", K(i), K(fd), KPC(file_handle.get()));
      file_handle.reset();
      ret = MTL(ObTenantTmpFileManager *)->remove(fd);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }

  ASSERT_EQ(0, flush_tg.wait_list_size_);
  ASSERT_EQ(0, flush_tg.retry_list_size_);
  ASSERT_EQ(0, flush_tg.finished_list_size_);
  ASSERT_EQ(true, flush_tg.wait_list_.is_empty());
  ASSERT_EQ(true, flush_tg.retry_list_.is_empty());
  ASSERT_EQ(true, flush_tg.finished_list_.is_empty());
  ASSERT_EQ(0, flush_tg.flush_mgr_.flush_ctx_.file_ctx_hash_.size());
  ASSERT_EQ(0, flush_tg.flush_mgr_.flush_ctx_.flush_failed_array_.size());

  ASSERT_EQ(0, pc_ctrl.flush_priority_mgr_.get_file_size());
  ASSERT_EQ(0, pc_ctrl.evict_mgr_.get_file_size());
  ASSERT_EQ(0, static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.files_.count());
}

TEST_F(TestTmpFileFlushMgr, test_basic)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 8 * 1024 * 1024 + 12 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(1, tmp_file_handles);

  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  ASSERT_NE(nullptr, file_handle.get());
  int64_t fd = file_handle.get()->fd_;
  file_handle.reset();  // ATTENTION! REMEMBER to release file_handle before you remove tmp file

  // write data
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpFileFlushTG &flush_tg = pc_ctrl.mock_swap_tg_.flush_tg_ref_;

  flush_tg.do_work_();
  usleep(20 * 1000);
  flush_tg.do_work_();
  usleep(20 * 1000);
  flush_tg.do_work_();
  usleep(20 * 1000);
  pc_ctrl.write_buffer_pool_.print_statistics();

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);

  delete [] write_buf;
  LOG_INFO("test_basic");
}

TEST_F(TestTmpFileFlushMgr, test_maintain_flush_ctx_correctness_when_IO_failed)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 650 * 8 * 1024; // write 650 data pages
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(2, tmp_file_handles);


  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT; // use large write buffer pool size to avoid automatic flushing
  pc_ctrl.write_buffer_pool_.set_max_data_page_usage_ratio_(0.99);
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true); // set flag to flush all pages

  // file 0 write data
  ObTmpFileIOInfo io_info;
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, file_handle.get()->get_file_size());
  file_handle.reset();

  // file 1 write data
  file_handle = tmp_file_handles.at(1);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 512 * 8 * 1024;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, file_handle.get()->get_file_size());
  file_handle.reset();

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;

  // generate 3 flush tasks, inject IO error to ensure send io [task 1(succ), task 2(succ), task 3(fail)]
  // fd 0: [task 1, task 3], fd 1: [task 2]
  LOG_INFO("begin flushing data pages");
  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_SUCC_FOR_FIRST_X_TASK);
  MockIO.set_io_cnt(0);
  MockIO.set_X(2);
  flush_tg.do_work_();
  LOG_INFO("flush_tg phase 1, 2 wait, 1 retry", K(flush_tg));
  usleep(20 * 1000);
  // task 1 && 2 update file meta succ, fd 1 generate new flush task 4,
  // but flush seq will not inc because task 3 && 4 fail to send IO;
  flush_tg.do_work_();
  LOG_INFO("flush_tg phase 2, 0 wait, 2 retry", K(flush_tg));

  // allow 1 more task send IO succ(task 3), now only task-4 is in retry list;
  // fd 0 still have 138 dirty data pages, and it will try to generate task 5 but
  // end with empty task since fd 0 have unfinished flushing task 3;
  // this action will lead to flush mgr reset flush_data_ctx of fd=0.
  MockIO.set_io_cnt(0);
  MockIO.set_X(1);
  flush_tg.do_work_();
  usleep(10 * 1000);

  LOG_INFO("flush_tg phase 3", K(flush_tg));
  MockIO.set_X(INT64_MAX); // allow all IO succ
  flush_tg.do_work_();
  usleep(10 * 1000);
  LOG_INFO("flush_tg phase 4", K(flush_tg));
  flush_tg.do_work_();
  LOG_INFO("flush_tg phase 5", K(flush_tg));

  ASSERT_EQ(0, flush_tg.wait_list_size_);
  ASSERT_EQ(0, flush_tg.retry_list_size_);
  ASSERT_EQ(0, flush_tg.finished_list_size_);
  ASSERT_EQ(0, flush_tg.flush_mgr_.flush_ctx_.file_ctx_hash_.size());
  ASSERT_EQ(0, flush_tg.flush_mgr_.flush_ctx_.flush_failed_array_.size());

  // remove all tmp files
  for (int64_t i = 0; i < tmp_file_handles.count(); ++i) {
    ObITmpFileHandle &file_handle = tmp_file_handles.at(i);
    ASSERT_NE(nullptr, file_handle.get());
    int64_t fd = file_handle.get()->get_fd();
    file_handle.reset();
    ret = MTL(ObTenantTmpFileManager *)->remove(fd);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ASSERT_EQ(0, pc_ctrl.flush_priority_mgr_.get_file_size());
  ASSERT_EQ(0, pc_ctrl.evict_mgr_.get_file_size());
  ASSERT_EQ(0, static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.files_.count());

  ATOMIC_SET(&pc_ctrl.flush_all_data_, false);
  delete [] write_buf;
  LOG_INFO("test_maintain_flush_ctx_correctness_when_IO_failed");
}

void do_async_flush(ObTenantBase *tenant_ctx, ObTmpFileFlushTG &flush_tg, bool &has_stop)
{
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, tenant_ctx);
  ObTenantEnv::set_tenant(tenant_ctx);
  while (!ATOMIC_LOAD(&has_stop)) {
    flush_tg.do_work_();
    LOG_INFO("flush_tg info in background thread", K(flush_tg));
    sleep(1);
  }
}

// delete file during tmp file retry flushing
TEST_F(TestTmpFileFlushMgr, test_delete_file_when_flushing)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 10 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(4, tmp_file_handles);

  // set flag to flush all pages
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT; // use large write buffer pool size to avoid automatic flushing
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true);

  // file 0 write data
  ObTmpFileIOInfo io_info;
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 2 * 1024 * 1024;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.reset();

  // file 1 write data
  file_handle = tmp_file_handles.at(1);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 4 * 1024;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.reset();

  // file 2 write data
  file_handle = tmp_file_handles.at(2);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 4 * 1024;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.reset();

  // file 3 write data
  file_handle = tmp_file_handles.at(3);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 4 * 1024;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  file_handle.reset();

  // all tmp files write 2MB + 4KB + 4KB + 4KB data,
  // make all flush tasks write block return OB_SERVER_OUTOF_DISK_SPACE error code
  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_SERVER_OUTOF_DISK_SPACE);
  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;

  // generate 2 flush tasks to flush all data pages
  // task 0 contains 2MB data from fd=0
  // task 1 contains 12KB data from fd=1,2,3
  const int64_t expect_retry_task_num = 2;
  for (int64_t i = 0; i < expect_retry_task_num; ++i) {
    flush_tg.do_work_();
  }
  LOG_INFO("flush_tg info", K(flush_tg));

  // start a thread to remove deleting tmp file in retry list
  bool has_stop = false;
  std::thread t(do_async_flush, MTL_CTX(), std::ref(flush_tg), std::ref(has_stop)); // TODO: 可以用async mode来替代

  // remove file 2, expect remove complete immediately
  int64_t fd_2 = tmp_file_handles.at(2).get()->fd_;
  tmp_file_handles.at(2).reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // remove file 3, expect remove complete immediately
  int64_t fd_3 = tmp_file_handles.at(3).get()->fd_;
  tmp_file_handles.at(3).reset();
  ret = MTL(ObTenantTmpFileManager *)->remove(fd_3);
  ASSERT_EQ(OB_SUCCESS, ret);

  ATOMIC_STORE(&has_stop, true);
  t.join();

  // allow remaining flush tasks to complete
  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::NORMAL);
  flush_tg.do_work_();
  usleep(20 * 1000);
  flush_tg.do_work_();
  usleep(20 * 1000);
  flush_tg.do_work_();
  LOG_INFO("flush_tg info after remove file 2 && 3", K(flush_tg));

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_delete_file_when_flushing");
}

// write tmp file return error immediately when OB_SERVER_OUTOF_DISK_SPACE occurs
TEST_F(TestTmpFileFlushMgr, test_write_tmp_file_OUTOF_DISK_SPACE)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 100 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(3, tmp_file_handles);

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;

  // make all task io return OB_SERVER_OUTOF_DISK_SPACE
  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_SERVER_OUTOF_DISK_SPACE);
  MockIO.set_async_mode(MTL_CTX(), &pc_ctrl.mock_swap_tg_); // start async flush && swap thread

  // file 0 write data, fail for OB_SERVER_OUTOF_DISK_SPACE
  ObTmpFileIOInfo io_info;
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, ret);
  LOG_INFO("file size", KPC(file_handle.get()));
  file_handle.reset();

  // file 1 write data, fail for OB_SERVER_OUTOF_DISK_SPACE
  file_handle = tmp_file_handles.at(1);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, ret);
  LOG_INFO("file size", KPC(file_handle.get()));
  file_handle.reset();

  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT; // use big wbp to flush more task in each round

  // file 2 write data succ with the first 5 flush tasks return OB_SERVER_OUTOF_DISK_SPACE (succ after retry)
  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_FAIL_FOR_FIRST_X_TASK);
  MockIO.set_X(5);

  LOG_INFO("begin writing file 2");
  file_handle = tmp_file_handles.at(2);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 2 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("file size", KPC(file_handle.get()));
  file_handle.reset();

  ObTmpFileFlushTG &flush_tg = pc_ctrl.mock_swap_tg_.flush_tg_ref_;
  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_write_tmp_file_OUTOF_DISK_SPACE");
}

// flush task will keep retrying when IO_timeout occurs
TEST_F(TestTmpFileFlushMgr, test_write_tmp_file_IO_timeout)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 100 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(2, tmp_file_handles);

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT; // use large write buffer pool size to avoid automatic flushing
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true); // set flag to flush all pages

  // file 0 write data
  ObTmpFileIOInfo io_info;
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 30 * 1024 * 1024;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("file size", KPC(file_handle.get()));
  file_handle.reset();

  // file 1 write data
  file_handle = tmp_file_handles.at(1);
  io_info.fd_ = file_handle.get()->fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = 30 * 1024 * 1024;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("file size", KPC(file_handle.get()));
  file_handle.reset();

  MockIO.set_wait_mode(MockTmpFileUtil::MOCK_WAIT_IO_MODE::MOCK_ALL_IO_TIMEOUT);
  MockIO.set_async_mode(MTL_CTX(), &pc_ctrl.mock_swap_tg_); // start async flush && swap thread

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;

  flush_tg.notify_doing_flush();
  sleep(1);
  LOG_INFO("flush_tg info", K(flush_tg));
  EXPECT_EQ(31, flush_tg.flushing_block_num_); // 30 tasks for data, 1 task for meta

  MockIO.set_wait_mode(MockTmpFileUtil::MOCK_WAIT_IO_MODE::NORMAL); // io recover
  sleep(2);

  LOG_INFO("io recover, flush_tg info", K(flush_tg));
  EXPECT_EQ(0, flush_tg.retry_list_size_);
  EXPECT_EQ(0, flush_tg.wait_list_size_);

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_write_tmp_file_IO_timeout");
}

void batch_write_file(ObTenantBase *tenant_ctx, const int32_t WRITE_DATA_BATCH, const int32_t idx,
                      const ObArray<ObSNTmpFileHandle> tmp_file_handles, char *write_buf, const int64_t write_size)
{
  int ret = OB_SUCCESS;
  ASSERT_NE(nullptr, tenant_ctx);
  ObTenantEnv::set_tenant(tenant_ctx);
  printf("writing file at %d\n", idx);
  for (int64_t i = 0; i < WRITE_DATA_BATCH; ++i) {
    ObITmpFileHandle file_handle = tmp_file_handles.at(idx);
    ASSERT_NE(nullptr, file_handle.get());
    ObTmpFileIOInfo io_info;
    io_info.fd_ = file_handle.get()->fd_;
    io_info.io_desc_.set_wait_event(2);
    io_info.buf_ = write_buf;
    io_info.size_ = write_size;
    io_info.io_timeout_ms_ = 5 * 1000;
    ASSERT_NE(nullptr, MTL(ObTenantTmpFileManager *));
    ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    file_handle.reset();
  }
}

TEST_F(TestTmpFileFlushMgr, test_tmp_file_DISK_HUNG)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 4 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  const int64_t FILE_NUM = 4;
  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(FILE_NUM, tmp_file_handles);
  ASSERT_EQ(FILE_NUM, tmp_file_handles.count());

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  pc_ctrl.write_buffer_pool_.default_wbp_memory_limit_ = 200 * 1024 * 1024; // 200MB
  ATOMIC_SET(&pc_ctrl.flush_all_data_, true); // set flag to flush all pages

  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_DISK_HUNG); // all io return OB_DISK_HUNG error
  MockIO.set_async_mode(MTL_CTX(), &pc_ctrl.mock_swap_tg_);
  const int32_t THREAD_NUM = FILE_NUM;
  const int32_t WRITE_DATA_BATCH = 10;
  ObTenantBase *tenant_ctx = MTL_CTX();

  // total write size: 4 files * 4MB * 10 WRITE = 160MB
  // ensure the data size is less than or equal to the wbp.default_wbp_memory_limit_ to avoid write operations being stalled when the wbp is full.
  std::thread threads[THREAD_NUM];
  for (int32_t i = 0; i < THREAD_NUM; ++i) {
    threads[i] = std::thread(batch_write_file, tenant_ctx, WRITE_DATA_BATCH, i, tmp_file_handles, write_buf, write_size);
  }

  for (int32_t i = 0; i < THREAD_NUM; ++i) {
    threads[i].join();
  }

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;
  LOG_INFO("flush_tg info", K(flush_tg));
  pc_ctrl.write_buffer_pool_.print_statistics();

  MockIO.set_send_mode(MockTmpFileUtil::MOCK_SEND_IO_MODE::NORMAL); // io recover
  sleep(2);
  LOG_INFO("flush_tg info after io recover", K(flush_tg));
  pc_ctrl.write_buffer_pool_.print_statistics();

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_tmp_file_DISK_HUNG");
}

TEST_F(TestTmpFileFlushMgr, test_single_file_wbp_shrink_basic)
{
  int ret = OB_SUCCESS;
  const double WBP_DATA_PAGE_RATIO = 0.99;
  const int64_t write_size = BIG_WBP_MEM_LIMIT * WBP_DATA_PAGE_RATIO; // use all data pages in wbp
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.write_buffer_pool_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  wbp.set_max_data_page_usage_ratio_(WBP_DATA_PAGE_RATIO);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(1, tmp_file_handles);
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  ASSERT_NE(nullptr, file_handle.get());
  int64_t fd = file_handle.get()->fd_;
  file_handle.reset();
  // write data
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;

  pc_ctrl.write_buffer_pool_.print_statistics();

  LOG_INFO("simulate tenant memory shrink, wbp is going to shrink...");
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
    flush_tg.do_work_();
    usleep(20 * 1000);
  }
  LOG_INFO("wbp target shrink size info", K(wbp.default_wbp_memory_limit_), K(wbp.capacity_));
  printf("wbp target shrink size: %ld, actual_size:%ld\n", wbp.default_wbp_memory_limit_, wbp.capacity_);
  printf("shrink complete: %s\n", wbp.default_wbp_memory_limit_ >= wbp.capacity_ ? "True" : "False");

  ASSERT_EQ(wbp.default_wbp_memory_limit_, wbp.capacity_);
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_wbp_shrink_basic");
}

TEST_F(TestTmpFileFlushMgr, test_single_file_wbp_shrink_abort)
{
  int ret = OB_SUCCESS;
  const double WBP_DATA_PAGE_RATIO = 0.9;
  const int64_t write_size = BIG_WBP_MEM_LIMIT * WBP_DATA_PAGE_RATIO; // use all data pages in wbp
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);
  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.write_buffer_pool_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  wbp.set_max_data_page_usage_ratio_(WBP_DATA_PAGE_RATIO);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(1, tmp_file_handles);
  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  ASSERT_NE(nullptr, file_handle.get());
  int64_t fd = file_handle.get()->fd_;
  file_handle.reset();

  // write data
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;

  pc_ctrl.write_buffer_pool_.print_statistics();
  wbp.default_wbp_memory_limit_ = SMALL_WBP_MEM_LIMIT;

  mock_swap_tg.shrink_wbp_if_needed_();
  mock_swap_tg.shrink_wbp_if_needed_();

  LOG_INFO("simulate tenant memory enlarge, abort shrinking");
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT; // abort wbp shrinking
  for (int32_t i = 0; i < 10; i++) {
    mock_swap_tg.shrink_wbp_if_needed_();
    flush_tg.do_work_();
    usleep(10 * 1000);
  }
  wbp.print_statistics();
  MockIO.check_wbp_free_list(wbp);
  ASSERT_EQ(wbp.default_wbp_memory_limit_, wbp.capacity_);
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  LOG_INFO("wbp target shrink size info", K(wbp.default_wbp_memory_limit_), K(wbp.capacity_));
  printf("wbp target shrink size: %ld, actual_size:%ld\n", wbp.default_wbp_memory_limit_, wbp.capacity_);
  printf("shrink complete: %s\n", wbp.default_wbp_memory_limit_ >= wbp.capacity_ ? "True" : "False");
  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_wbp_shrink_abort");
}

TEST_F(TestTmpFileFlushMgr, test_wbp_shrink_during_single_file_writing)
{
  int ret = OB_SUCCESS;
  const double WBP_DATA_PAGE_RATIO = 0.9;
  const int64_t write_size = 5 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(1, tmp_file_handles);

  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  ASSERT_NE(nullptr, file_handle.get());
  int64_t fd = file_handle.get()->fd_;
  file_handle.reset();

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.write_buffer_pool_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT;
  wbp.set_max_data_page_usage_ratio_(WBP_DATA_PAGE_RATIO);

  ObTenantBase *tenant_base = MTL_CTX();
  std::thread t([&]{
    sleep(1);
    // wait for tmp file fill wbp for a while, then shrink wbp
    ATOMIC_SET(&wbp.default_wbp_memory_limit_, SMALL_WBP_MEM_LIMIT);
    wbp.print_statistics();
    LOG_INFO("simulate tenant memory shrink, wbp is going to shrink...", K(wbp.default_wbp_memory_limit_), K(wbp.capacity_));
    MockIO.set_async_mode(tenant_base, &pc_ctrl.mock_swap_tg_); // start mock background thread
  });

  // write data
  const int64_t MAX_READ_BUF_SIZE = 500 * 1024 * 1024;
  int64_t write_count = 0;
  char *read_buf = new char[MAX_READ_BUF_SIZE];
  int64_t read_buf_offset = 0;
  LOG_INFO("write data for 3 seconds");
  wbp.print_statistics();
  int64_t start_ts = ObTimeUtil::current_time();
  while (ObTimeUtil::current_time() - start_ts <= 3 * 1000 * 1000) { // generate at most 300MB data
    ObTmpFileIOInfo io_info;
    io_info.fd_ = fd;
    io_info.io_desc_.set_wait_event(2);
    io_info.buf_ = write_buf;
    io_info.size_ = ObRandom::rand(write_size / 2, write_size);
    io_info.io_timeout_ms_ = 5 * 1000;
    ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    memcpy(read_buf + read_buf_offset, write_buf, io_info.size_);
    read_buf_offset += io_info.size_;
    ASSERT_EQ(true, read_buf_offset <= MAX_READ_BUF_SIZE);
    write_count += 1;
    usleep(50 * 1000); // 50ms
  }
  LOG_INFO("write data count", K(write_count), K(read_buf_offset));

  sleep(2);
  LOG_INFO("wbp target shrink size info", K(wbp.default_wbp_memory_limit_), K(wbp.capacity_));
  printf("wbp target shrink size: %ld, actual_size:%ld\n", wbp.default_wbp_memory_limit_, wbp.capacity_);
  printf("shrink complete: %s\n", wbp.default_wbp_memory_limit_ >= wbp.capacity_ ? "True" : "False");

  t.join();

  file_handle = tmp_file_handles.at(0);
  int64_t file_size = file_handle.get()->get_file_size();
  ObTmpFileIOHandle handle;
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.buf_ = read_buf;
  io_info.size_ = file_size;
  io_info.io_desc_.set_wait_event(2);
  ret = MTL(ObTenantTmpFileManager *)->read(MTL_ID(), io_info, handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(io_info.size_, handle.get_done_size());
  int cmp = memcmp(handle.get_buffer(), read_buf, io_info.size_);
  ASSERT_EQ(0, cmp);
  handle.reset();
  file_handle.reset();
  handle.reset();

  MockTmpFileSwapTg &mock_swap_tg = pc_ctrl.mock_swap_tg_;
  ObTmpFileFlushTG &flush_tg = mock_swap_tg.flush_tg_ref_;
  ASSERT_EQ(wbp.default_wbp_memory_limit_, wbp.capacity_);
  ASSERT_EQ(false, wbp.shrink_ctx_.is_valid());

  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  delete [] read_buf;
  LOG_INFO("test_wbp_shrink_during_single_file_writing");
}

TEST_F(TestTmpFileFlushMgr, test_reinsert_item_into_meta_tree)
{
  int ret = OB_SUCCESS;
  const int64_t write_size = 72 * 1024 * 1024;
  char *write_buf = nullptr;
  generate_write_data(write_buf, write_size);
  ASSERT_NE(nullptr, write_buf);

  ObArray<ObSNTmpFileHandle> tmp_file_handles;
  create_tmp_files(1, tmp_file_handles);

  ObITmpFileHandle file_handle = tmp_file_handles.at(0);
  ASSERT_NE(nullptr, file_handle.get());
  int64_t fd = file_handle.get()->fd_;

  MockTmpFilePageCacheController &pc_ctrl =
      static_cast<MockTenantTmpFileManager *>(MTL(ObTenantTmpFileManager *))->mock_sn_tmp_file_mgr_.mock_page_cache_controller_;
  ObTmpWriteBufferPool &wbp = pc_ctrl.write_buffer_pool_;
  ObTmpFileFlushTG &flush_tg = pc_ctrl.mock_swap_tg_.flush_tg_ref_;
  wbp.default_wbp_memory_limit_ = BIG_WBP_MEM_LIMIT * 2;

  // 1. 产生data和meta的刷盘任务，所有IO都卡住
  MockIO.set_wait_mode(MockTmpFileUtil::MOCK_WAIT_IO_MODE::MOCK_ALL_IO_TIMEOUT);
  pc_ctrl.set_flush_all_data(true);

  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd;
  io_info.io_desc_.set_wait_event(2);
  io_info.buf_ = write_buf;
  io_info.size_ = write_size;
  io_info.io_timeout_ms_ = 5 * 1000;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  flush_tg.do_work_();
  LOG_INFO("flush info 1", K(flush_tg));
  wbp.print_statistics();
  LOG_INFO("file status 1", KPC(file_handle.get()));
  EXPECT_EQ(37, flush_tg.flushing_block_num_); // 36 data task + 1 meta task(13 meta pages)
  EXPECT_TRUE(wbp.meta_page_cnt_ > 0);
  EXPECT_TRUE(wbp.dirty_meta_page_cnt_ == 0);

  // 2. data IO 全部完成，重新插入data刷盘链表；再次触发data刷盘
  io_info.size_ = 8 * 1024 * 1024;
  ret = MTL(ObTenantTmpFileManager *)->write(MTL_ID(), io_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  MockIO.set_wait_mode(MockTmpFileUtil::MOCK_WAIT_IO_MODE::MOCK_META_IO_TIMEOUT);

  flush_tg.do_work_();
  LOG_INFO("flush info 2", K(flush_tg));
  wbp.print_statistics();
  LOG_INFO("file status 2", KPC(file_handle.get()));
  EXPECT_EQ(nullptr, file_handle.get()->data_flush_node_.get_next());

  // 3. meta IO未完成的情况下不能再次刷到meta
  flush_tg.do_work_();
  LOG_INFO("flush info 3", K(flush_tg));
  wbp.print_statistics();
  LOG_INFO("file status 3", KPC(file_handle.get()));
  // add one meta pages at level 0, level 1 rightmost meta page becomes dirty
  EXPECT_EQ(wbp.write_back_meta_cnt_, 12);
  ASSERT_EQ(OB_SUCCESS, MockIO.get_generate_error_code());

  // 4. IO恢复，结束流程
  MockIO.set_wait_mode(MockTmpFileUtil::MOCK_WAIT_IO_MODE::NORMAL);
  for (int32_t i = 0; i < 10; ++i) {
    flush_tg.do_work_();
    usleep(10 * 1000);
  }

  LOG_INFO("flush info 4, complete", K(flush_tg), KPC(file_handle.get()));
  wbp.print_statistics();
  file_handle.reset();
  remove_all_files_and_check_state(tmp_file_handles, flush_tg, pc_ctrl);
  delete [] write_buf;
  LOG_INFO("test_reinsert_item_into_meta_tree");
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_flush_manager.log*");
  system("rm -rf ./run*");
  OB_LOGGER.set_file_name("test_tmp_file_flush_manager.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
