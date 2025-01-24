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

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace std;

typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;

struct TestDirtyPageRecord
{
  TestDirtyPageRecord() : dir(0), dirty_data_size_(0), non_rightmost_meta_page_num_(0), rightmost_meta_page_num_(0) {}
  TestDirtyPageRecord(const int64_t dir,
                      const int64_t dirty_data_size,
                      const int64_t non_rightmost_meta_page_num,
                      const int64_t rightmost_meta_page_num)
    : dir(dir),
      dirty_data_size_(dirty_data_size),
      non_rightmost_meta_page_num_(non_rightmost_meta_page_num),
      rightmost_meta_page_num_(rightmost_meta_page_num) {}
  int64_t dir;
  int64_t dirty_data_size_;
  int64_t non_rightmost_meta_page_num_;
  int64_t rightmost_meta_page_num_;
};

class TestFlushListIterator : public ::testing::Test
{
public:
  TestFlushListIterator() {}
  void insert_file_sequence();
  void create_files(const FlushCtxState state,
                    const int64_t file_num,
                    ObTmpFileFlushPriorityManager &flush_prio_mgr,
                    vector<tmp_file::ObSNTmpFileHandle> &file_handles);
  void create_files_with_dir(const FlushCtxState state,
                             const int64_t dir,
                             const int64_t file_num,
                             ObTmpFileFlushPriorityManager &flush_prio_mgr,
                             vector<tmp_file::ObSNTmpFileHandle> &file_handles);
  void clear_all_files();
protected:
  virtual void TearDown()
  {
    clear_all_files();
  }

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
private:
  char *random_buf_;
  const int64_t write_size_ = 4 * 1024 * 1024; // 4MB
  unordered_map<int64_t, TestDirtyPageRecord> mock_dirty_record_; // 记录模拟生成的脏页数据代替实际写入文件
};

void get_file_range_by_state(const FlushCtxState state, int64_t &low, int64_t &high)
{
  switch(state) {
    case FlushCtxState::FSM_F1: // [2MB, 4MB]
      low = 2 * 1024 * 1024;
      high = 4 * 1024 * 1024;
      break;
    case FlushCtxState::FSM_F2: // [1MB, 2MB)
      low = 1 * 1024 * 1024;
      high = 2 * 1024 * 1024 - 1;
      break;
    case FlushCtxState::FSM_F3: // [128KB, 1MB)
      low = 128 * 1024;
      high = 1 * 1024 * 1024 - 1;
      break;
    case FlushCtxState::FSM_F4: // [8KB, 128KB)
      low = 8 * 1024;
      high = 128 * 1024 - 1;
      break;
    case FlushCtxState::FSM_F5: // (0KB, 8KB)
      low = 1;
      high = 8 * 1024 - 1;
      break;
    default:
      low = 0;
      high = 0;
      break;
  }
}

void TestFlushListIterator::clear_all_files()
{
  int ret = OB_SUCCESS;
//  ObTmpFileFlushPriorityManager &flush_prio_mgr =
//      MTL(ObTenantTmpFileManager *)->get_page_cache_controller().get_flush_priority_mgr();
//  for (auto p : mock_dirty_record_) {
//    int64_t fd = p.first;
//    tmp_file::ObSNTmpFileHandle file_handle;
//    ret = MTL(ObTenantTmpFileManager *)->get_tmp_file(fd, file_handle);
//    ASSERT_EQ(OB_SUCCESS, ret);
//    ASSERT_NE(nullptr, file_handle.get());
//    ret = flush_prio_mgr.remove_file(*file_handle.get());
//    ASSERT_EQ(OB_SUCCESS, ret);
//  }
  mock_dirty_record_.clear();
}

void TestFlushListIterator::insert_file_sequence()
{
}

void TestFlushListIterator::create_files(const FlushCtxState state, const int64_t file_num,
    ObTmpFileFlushPriorityManager &flush_prio_mgr,
    vector<tmp_file::ObSNTmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0 ; i < file_num; ++i) {
    int64_t dir = -1;
    ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t fd = i;

    void *buf = nullptr;
    ObSharedNothingTmpFile *tmp_file = nullptr;
    buf = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_allocator_.alloc(sizeof(ObSharedNothingTmpFile));
    ASSERT_NE(nullptr, buf);

    tmp_file = new (buf) ObSharedNothingTmpFile();
    ret = tmp_file->init(MTL_ID(), fd, dir,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_block_manager_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().callback_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().wbp_index_cache_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().wbp_index_cache_bucket_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_,
                   nullptr);
    ASSERT_EQ(OB_SUCCESS, ret);
    tmp_file->flush_prio_mgr_ = &flush_prio_mgr;

    // ret = MTL(ObTenantTmpFileManager *)->open(fd, dir);
    // ASSERT_EQ(OB_SUCCESS, ret);

    int64_t low = 0, high = 0;
    get_file_range_by_state(state, low, high);

    TestDirtyPageRecord dirty_record;
    int64_t mock_dirty_data_size = ObRandom::rand(low, high);

    dirty_record.dirty_data_size_ = mock_dirty_data_size;
    dirty_record.non_rightmost_meta_page_num_ = 0;
    dirty_record.rightmost_meta_page_num_ = 0;
    mock_dirty_record_[fd] = dirty_record;

    tmp_file::ObSNTmpFileHandle file_handle;
    file_handle.init(tmp_file);
    ObSharedNothingTmpFile &file = *file_handle.get();
    file.file_size_ = mock_dirty_data_size;
    file.cached_page_nums_ =
      upper_align(mock_dirty_data_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
    file_handles.push_back(file_handle);
  }
  ASSERT_EQ(file_num, file_handles.size());
}

void TestFlushListIterator::create_files_with_dir(
    const FlushCtxState state,
    const int64_t dir,
    const int64_t file_num,
    ObTmpFileFlushPriorityManager &flush_prio_mgr,
    vector<tmp_file::ObSNTmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0 ; i < file_num; ++i) {
    int64_t fd = i;

    void *buf = nullptr;
    ObSharedNothingTmpFile *tmp_file = nullptr;
    buf = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_allocator_.alloc(sizeof(ObSharedNothingTmpFile));
    ASSERT_NE(nullptr, buf);

    tmp_file = new (buf) ObSharedNothingTmpFile();
    ret = tmp_file->init(MTL_ID(), fd, dir,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().tmp_file_block_manager_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().callback_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().wbp_index_cache_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().wbp_index_cache_bucket_allocator_,
                   &MTL(ObTenantTmpFileManager *)->get_sn_file_manager().page_cache_controller_,
                   nullptr);
    ASSERT_EQ(OB_SUCCESS, ret);
    tmp_file->flush_prio_mgr_ = &flush_prio_mgr;

    int64_t low = 0, high = 0;
    get_file_range_by_state(state, low, high);

    TestDirtyPageRecord dirty_record;
    int64_t mock_dirty_data_size = ObRandom::rand(low, high);

    dirty_record.dirty_data_size_ = mock_dirty_data_size;
    dirty_record.non_rightmost_meta_page_num_ = 0;
    dirty_record.rightmost_meta_page_num_ = 0;
    mock_dirty_record_[fd] = dirty_record;

    tmp_file::ObSNTmpFileHandle file_handle;
    file_handle.init(tmp_file);
    ObSharedNothingTmpFile &file = *file_handle.get();
    file.file_size_ = mock_dirty_data_size;
    file.cached_page_nums_ =
      upper_align(mock_dirty_data_size, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
    file_handles.push_back(file_handle);
  }
  ASSERT_EQ(file_num, file_handles.size());
}

TEST_F(TestFlushListIterator, test_iter_order)
{
  int ret = OB_SUCCESS;
  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<TestDirtyPageRecord> array;

  // dir 1 has 10KB * 4
  array.push_back(TestDirtyPageRecord(1/*dir*/, 10240/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(1/*dir*/, 10240/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(1/*dir*/, 10240/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(1/*dir*/, 10240/*dirty_data_size*/, 0, 0));

  // dir 2 has (24KB * 4 + 2MB * 4)
  array.push_back(TestDirtyPageRecord(2/*dir*/, 24576/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 24576/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 24576/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 24576/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 2097152/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 2097152/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 2097152/*dirty_data_size*/, 0, 0));
  array.push_back(TestDirtyPageRecord(2/*dir*/, 2097152/*dirty_data_size*/, 0, 0));

  // iterate order: 2MB * 4, 10KB * 4, 4KB * 4
  for (auto &mock_record: array) {
    int64_t fd = -1;
    int64_t dir = mock_record.dir;
    ret = MTL(ObTenantTmpFileManager *)->open(fd, dir, "");
    ASSERT_EQ(OB_SUCCESS, ret);

    mock_dirty_record_[fd] = mock_record;

    tmp_file::ObSNTmpFileHandle file_handle;
    ret = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file(fd, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &file = *file_handle.get();
    file.file_size_ = mock_record.dirty_data_size_;
    file.cached_page_nums_ = upper_align(file.file_size_, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE;
    ret = flush_prio_mgr.insert_data_flush_list(file, mock_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  printf("iterating file...\n");
  for (int64_t i = 0; i < 4; i++) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F1, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &file = *file_handle.get();
    TestDirtyPageRecord &mock_record = mock_dirty_record_[file.fd_];
    ASSERT_EQ(mock_record.dirty_data_size_, file.file_size_);
    printf("fd: %ld, file_size: %ld\n", file.fd_, file.file_size_);
  }

  for (int64_t i = 0; i < 4; i++) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F2, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &file = *file_handle.get();
    TestDirtyPageRecord &mock_record = mock_dirty_record_[file.fd_];
    ASSERT_EQ(mock_record.dirty_data_size_, file.file_size_);
    ASSERT_EQ(24576, file.file_size_);
    printf("fd: %ld, file_size: %ld\n", file.fd_, file.file_size_);
  }

  for (int64_t i = 0; i < 4; i++) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F2, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &file = *file_handle.get();
    TestDirtyPageRecord &mock_record = mock_dirty_record_[file.fd_];
    ASSERT_EQ(mock_record.dirty_data_size_, file.file_size_);
    ASSERT_EQ(10240, file.file_size_);
    printf("fd: %ld, file_size: %ld\n", file.fd_, file.file_size_);
  }

  tmp_file::ObSNTmpFileHandle file_handle;
  ret = iter.next(FlushCtxState::FSM_F3, file_handle);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestFlushListIterator, test_iter_data_basic)
{
  int ret = OB_SUCCESS;

  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  int total_file_num = 0;
  const int64_t FILE_NUM = 10;

  // 创建文件，根据层级生成模拟的脏页数量
  int64_t total_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    file_handles.clear();
    create_files(FlushCtxState(t), FILE_NUM, flush_prio_mgr, file_handles);
    for (int64_t i = 0; i < file_handles.size(); ++i) {
      tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
      ASSERT_NE(file_handle.get(), nullptr);
      ObSharedNothingTmpFile &tmp_file = *file_handle.get();
      TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
      ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
      ASSERT_EQ(OB_SUCCESS, ret);
      total_file_cnt += 1;
    }
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 遍历所有刷盘层级是否能取出该层级所有文件
  int64_t iter_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    for (int64_t i = 0; OB_SUCC(ret) && i < FILE_NUM * 5; ++i) {
      tmp_file::ObSNTmpFileHandle file_handle;
      ret = iter.next(FlushCtxState(t), file_handle);
      if (OB_SUCC(ret)) {
        ASSERT_NE(file_handle.get(), nullptr);
        iter_file_cnt += 1;
      }
    }
    ASSERT_EQ(ret, OB_ITER_END);
    ret = OB_SUCCESS;
  }

  ASSERT_EQ(iter_file_cnt, total_file_cnt);
}

TEST_F(TestFlushListIterator, test_iter_prev_stage)
{
  int ret = OB_SUCCESS;
  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  int total_file_num = 0;
  const int64_t FILE_NUM = 10;

  // 创建文件，根据层级生成模拟的脏页数量
  int64_t total_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    file_handles.clear();
    create_files(FlushCtxState(t), FILE_NUM, flush_prio_mgr, file_handles);
    for (int64_t i = 0; i < file_handles.size(); ++i) {
      tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
      ASSERT_NE(file_handle.get(), nullptr);
      ObSharedNothingTmpFile &tmp_file = *file_handle.get();
      TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
      ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
      ASSERT_EQ(OB_SUCCESS, ret);

      total_file_cnt += 1;
    }
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 直接开始遍历F3直至OB_ITER_END
  for (int64_t i = 0; OB_SUCC(ret) && i < FILE_NUM * 10; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F3, file_handle);
    if (OB_SUCC(ret)) {
      ASSERT_NE(file_handle.get(), nullptr);
      ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);

  // 切换到下一层级后，无法再遍历之前的层级
  tmp_file::ObSNTmpFileHandle file_handle;
  ret = iter.next(FlushCtxState::FSM_F1, file_handle);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

// 某个文件从iterator返回、使用之后重新插回，如果迭代层级没有变更应该能被iterator重新取出
TEST_F(TestFlushListIterator, test_iter_reinsert_file)
{
  int ret = OB_SUCCESS;
  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  int total_file_num = 0;
  const int64_t FILE_NUM = 10;

  // 创建文件，根据层级生成模拟的脏页数量
  int64_t total_file_cnt = 0;
  create_files(FlushCtxState::FSM_F1, FILE_NUM, flush_prio_mgr, file_handles);
  for (int64_t i = 0; i < file_handles.size(); ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
    total_file_cnt += 1;
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 通过iterator取出所有文件
  int64_t iter_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    for (int64_t i = 0; OB_SUCC(ret) && i < FILE_NUM * 5; ++i) {
      tmp_file::ObSNTmpFileHandle file_handle;
      ret = iter.next(FlushCtxState(t), file_handle);
      if (OB_SUCC(ret)) {
        ASSERT_NE(file_handle.get(), nullptr);
        iter_file_cnt += 1;
      }
    }
    ASSERT_EQ(ret, OB_ITER_END);
  }
  ASSERT_EQ(iter_file_cnt, total_file_cnt);
  ret = OB_SUCCESS;

  tmp_file::ObSNTmpFileHandle file_handle = file_handles[0];
  ASSERT_NE(file_handle.get(), nullptr);
  ret = flush_prio_mgr.insert_data_flush_list(*file_handle.get(), 2 * 1024 * 1024);
  ASSERT_EQ(ret, OB_SUCCESS);

  file_handle.reset();
  ret = iter.next(FlushCtxState::FSM_F1, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = iter.next(FlushCtxState::FSM_F1, file_handle);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestFlushListIterator, test_flush_list_remove)
{
  int ret = OB_SUCCESS;
  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  const int64_t FILE_NUM = 10;

  // 创建文件，根据层级生成模拟的脏页数量
  int64_t total_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    vector<tmp_file::ObSNTmpFileHandle> tmp_file_handles;
    create_files(FlushCtxState(t), FILE_NUM, flush_prio_mgr, tmp_file_handles);
    for (int64_t i = 0; i < tmp_file_handles.size(); ++i) {
      tmp_file::ObSNTmpFileHandle file_handle = tmp_file_handles.at(i);
      ASSERT_NE(file_handle.get(), nullptr);
      ObSharedNothingTmpFile &tmp_file = *file_handle.get();
      TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
      ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
      ASSERT_EQ(OB_SUCCESS, ret);
      total_file_cnt += 1;
    }
    file_handles.insert(file_handles.end(), tmp_file_handles.begin(), tmp_file_handles.end());
  }

  // 从文件链表中删除文件
  const int64_t rand_remove_cnt = ObRandom::rand(1, file_handles.size());
  for (int64_t i = 0; i < rand_remove_cnt; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ASSERT_NE(nullptr, file_handle.get());
    ASSERT_EQ(OB_SUCCESS, ret);
    ObSharedNothingTmpFile &file = *file_handle.get();
    ret = flush_prio_mgr.remove_file(false/*is_meta*/, file);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t remain_file_cnt = 0;
  for (int64_t t = FlushCtxState::FSM_F1; t < FlushCtxState::FSM_FINISHED; ++t) {
    for (int64_t i = 0; OB_SUCC(ret) && i < FILE_NUM * 10; ++i) {
      tmp_file::ObSNTmpFileHandle file_handle;
      ret = iter.next(FlushCtxState(t), file_handle);
      if (OB_SUCC(ret)) {
        ASSERT_NE(file_handle.get(), nullptr);
        ObSharedNothingTmpFile &tmp_file = *file_handle.get();
        int64_t fd = tmp_file.get_fd();
        remain_file_cnt += 1;
      }
    }
    ASSERT_EQ(ret, OB_ITER_END);
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(remain_file_cnt + rand_remove_cnt, total_file_cnt);
}

TEST_F(TestFlushListIterator, test_flush_list_update)
{
  int ret = OB_SUCCESS;

  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  int total_file_num = 0;
  const int64_t FILE_NUM = 10;

  // 创建文件，根据层级生成模拟的脏页数量
  int64_t total_file_cnt = 0;
  create_files(FlushCtxState::FSM_F1, FILE_NUM, flush_prio_mgr, file_handles);

  // 0.插入文件0～4
  for (int64_t i = 0; i < file_handles.size() / 2; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
    total_file_cnt += 1;
  }

  // 1. 更新不在链表中的文件
  for (int64_t i = file_handles.size() / 2; i < file_handles.size(); ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.update_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  }

  // 2. 对层级没有变动的文件进行更新
  for (int64_t i = 0; i < file_handles.size() / 2; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.update_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // 3. 更新到新的层级
  for (int64_t i = 0; i < file_handles.size() / 2; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    dirty_record.dirty_data_size_ = 4096;
    ret = flush_prio_mgr.update_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // 4. 插入文件5～10
  for (int64_t i = file_handles.size() / 2; i < file_handles.size(); ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
    total_file_cnt += 1;
  }

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. F1中可以取出5个文件
  for (int64_t i = 0; i < file_handles.size() / 2; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F1, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
  }

  // 6. F5中可以取出5个更新后的文件
  for (int64_t i = 0; i < file_handles.size() / 2; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(FlushCtxState::FSM_F3, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
  }
}

TEST_F(TestFlushListIterator, test_flush_list_reinsert_after_use)
{
  int ret = OB_SUCCESS;
  int64_t dir = -1;
  ret = MTL(ObTenantTmpFileManager *)->alloc_dir(dir);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTmpFileFlushPriorityManager flush_prio_mgr;
  ret = flush_prio_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTmpFilePageCacheController &pc_ctrl = MTL(ObTenantTmpFileManager *)->get_sn_file_manager().get_page_cache_controller();
  vector<tmp_file::ObSNTmpFileHandle> file_handles;
  const int64_t FILE_NUM = 10;
  FlushCtxState state = FlushCtxState::FSM_F2;
  create_files_with_dir(state, dir, FILE_NUM, flush_prio_mgr, file_handles);
  ASSERT_EQ(FILE_NUM, file_handles.size());

  ObTmpFileFlushListIterator iter;
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < file_handles.size(); ++i) {
    tmp_file::ObSNTmpFileHandle file_handle = file_handles.at(i);
    ASSERT_NE(file_handle.get(), nullptr);
    ObSharedNothingTmpFile &tmp_file = *file_handle.get();
    TestDirtyPageRecord &dirty_record = mock_dirty_record_.at(tmp_file.get_fd());
    ret = flush_prio_mgr.insert_data_flush_list(tmp_file, dirty_record.dirty_data_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // 取出迭代器dir中一半的文件
  const int64_t USED_FILE_CNT = file_handles.size() / 2;
  for (int64_t i = 0; i < USED_FILE_CNT; ++i) {
    tmp_file::ObSNTmpFileHandle file_handle;
    ret = iter.next(state, file_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(file_handle.get(), nullptr);
    printf("use file %ld\n", file_handle.get()->get_fd());
  }

  // 迭代器中剩余文件重新插回flush_prio_mgr
  iter.destroy();
  ret = iter.init(&flush_prio_mgr);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 重新初始化迭代器，通过迭代器取出剩余文件
  int64_t remain_file_cnt = 0;
  while (OB_SUCC(ret)) {
    tmp_file::ObSNTmpFileHandle file_handle;
    if (OB_SUCC(iter.next(state, file_handle))) {
      remain_file_cnt += 1;
    }
  }
  printf("remain_file_cnt:%ld, USED_FILE_CNT:%ld, FILE_NUM:%ld\n", remain_file_cnt, USED_FILE_CNT, FILE_NUM);
  ASSERT_EQ(remain_file_cnt + USED_FILE_CNT, FILE_NUM);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_tmp_file_flush_list.log*");
  OB_LOGGER.set_file_name("test_tmp_file_flush_list.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
