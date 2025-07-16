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

#ifndef OB_TMP_FILE_WRITE_CACHE_TEST_HELPER_
#define OB_TMP_FILE_WRITE_CACHE_TEST_HELPER_
#include "mittest/mtlenv/storage/tmp_file/ob_tmp_file_test_helper.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"

namespace oceanbase
{
using namespace common;
using namespace tmp_file;
using namespace share;
/* ------------------------------ Test Helper ------------------------------ */
char *generate_data(int64_t size)
{
  char *write_buffer = new char[size];
  for (int64_t i = 0; i < size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < size; ++j) {
      write_buffer[i + j] = random_int;
    }
    i += random_length;
  }
  return write_buffer;
}

class TestWriteCacheSingleThread : public share::ObThreadPool
{
public:
  void init(
      ObTenantBase *tenant_ctx,
      const int64_t fd,
      char *buffer,
      int64_t buffer_size);
  void run1() override;
  void write_data(const int64_t size);
  void read_data(const int64_t size);
  void free_data();
  void alloc_tmp_file_block_();
  void add_page_to_block_();
public:
  static const int64_t PAGE_SIZE = ObTmpFileGlobal::PAGE_SIZE;
  int ret_code_;
  int32_t allocated_page_cnt_;
  int32_t page_idx_in_blk_;
  int64_t fd_;
  char *buffer_;
  int64_t buffer_size_;
  int64_t write_offset_;
  int64_t read_offset_;
  int64_t read_page_idx_in_blk_;
  int64_t read_block_idx_;
  ObArray<int64_t> block_idxs_;
  ObArray<ObTmpFilePageHandle> current_blk_pages_;
  ObTmpFileWriteCache *write_cache_;
  ObTmpFileBlockManager *tf_block_mgr_;
  ObTenantBase *tenant_ctx_;
};

void TestWriteCacheSingleThread::init(
    ObTenantBase *tenant_ctx,
    const int64_t fd,
    char *buffer,
    int64_t buffer_size)
{
  fd_ = fd;
  buffer_ = buffer;
  buffer_size_ = buffer_size;
  write_cache_ = &MTL(tmp_file::ObTenantTmpFileManager *)->get_sn_file_manager().get_write_cache();
  tf_block_mgr_ = &MTL(tmp_file::ObTenantTmpFileManager *)->get_sn_file_manager().get_tmp_file_block_manager();
  tenant_ctx_ = tenant_ctx;

  // construct
  write_offset_ = 0;
  read_offset_ = 0;
  allocated_page_cnt_ = 0;
  page_idx_in_blk_ = 0;
  read_page_idx_in_blk_ = 0;
  read_block_idx_ = 0;
  ret_code_ = OB_SUCCESS;
  printf("TestWriteCacheSingleThread init fd: %ld, buffer_size: %ld\n", fd_, buffer_size_);
  _OB_LOG(INFO, "TestWriteCacheSingleThread init fd: %ld, buffer_size: %ld\n", fd_, buffer_size_);
}

void TestWriteCacheSingleThread::add_page_to_block_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("add_page_to_block_ begin", K(fd_), K(current_blk_pages_.size()));
  if (current_blk_pages_.size() > 0) {
    for (int64_t i = 0; i < current_blk_pages_.size(); ++i) {
      LOG_DEBUG("add_page_to_block_ to block", K(current_blk_pages_.size()), K(i), K(current_blk_pages_[i]));
    } // DEBUG
    ASSERT_TRUE(block_idxs_.size() > 0);
    int64_t block_index = block_idxs_[block_idxs_.size() - 1];
    ObTmpFileBlockHandle block_handle;
    ASSERT_EQ(OB_SUCCESS, tf_block_mgr_->get_tmp_file_block_handle(block_index, block_handle));
    ASSERT_NE(nullptr, block_handle.get());
    ret = block_handle.get()->insert_pages_into_flushing_list(current_blk_pages_);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (OB_FAIL(ret)) {
      LOG_INFO("fail to insert pages into block's flushing list", KR(ret),
          K(current_blk_pages_.size()), K(block_handle), K(current_blk_pages_[0]));
    }
    ret_code_ = ret;
    current_blk_pages_.reset();
  }
  LOG_DEBUG("add_page_to_block_ end", K(fd_));
}

void TestWriteCacheSingleThread::alloc_tmp_file_block_()
{
  add_page_to_block_();
  int64_t block_index = -1;
  ASSERT_EQ(OB_SUCCESS, tf_block_mgr_->alloc_block(block_index));
  ASSERT_EQ(OB_SUCCESS, block_idxs_.push_back(block_index));
  allocated_page_cnt_ = 256;
  page_idx_in_blk_ = 0;
  LOG_DEBUG("alloc new block", K(block_index), K(block_idxs_.size()));
}

void TestWriteCacheSingleThread::write_data(const int64_t size)
{
  int64_t real_size = min(write_offset_ + size, buffer_size_ - write_offset_);
  int64_t done_size = 0;
  while (OB_SUCCESS == ret_code_ && done_size < real_size) {
    if (allocated_page_cnt_ == 0) {
      alloc_tmp_file_block_();
    }
    int64_t virtual_page_id = write_offset_ / PAGE_SIZE;
    int64_t block_index = block_idxs_.at(block_idxs_.count() - 1);
    ObTmpFileWriteCacheKey page_key(PageType::DATA, fd_, virtual_page_id);
    ObTmpFilePageId page_id(page_idx_in_blk_, block_index);

    ObTmpFilePageHandle page_handle;
    LOG_INFO("write_data_alloc page", K(page_key), K(page_id), K(block_index), K(virtual_page_id));
    ASSERT_EQ(OB_SUCCESS, write_cache_->alloc_page(page_key, page_id, 5 * 1000, page_handle));
    ObTmpFilePage *page = page_handle.get_page();
    ASSERT_NE(nullptr, page);
    ASSERT_TRUE(page_key == page->get_page_key());
    ASSERT_TRUE(page_id == page->get_page_id());

    char *page_buf = page->get_buffer();
    ASSERT_NE(nullptr, page_buf);
    memcpy(page_buf, buffer_ + write_offset_, PAGE_SIZE);
    page->set_is_full(true);
    ASSERT_EQ(OB_SUCCESS, write_cache_->put_page(page_handle));
    ASSERT_EQ(OB_SUCCESS, current_blk_pages_.push_back(page_handle));
    LOG_DEBUG("current_blk_pages_ push back", K(page_handle));
    LOG_DEBUG("write_data", K(fd_),
        K(page_handle), K(allocated_page_cnt_), K(page_idx_in_blk_),
        K(virtual_page_id), K(current_blk_pages_.size()), K(block_index));
    allocated_page_cnt_ -= 1;
    page_idx_in_blk_ += 1;
    done_size += PAGE_SIZE;
    write_offset_ += PAGE_SIZE;
  }
  // add_page_to_block_(); // TODO：等死锁解了之后再恢复这个
}

void TestWriteCacheSingleThread::read_data(const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t real_size = min(read_offset_ + size, write_offset_ - read_offset_);
  int64_t done_size = 0;
  while (OB_SUCCESS == ret_code_ && done_size < real_size) {
    int64_t block_index = -1;
    int64_t virtual_page_id = read_offset_ / PAGE_SIZE;
    if (read_page_idx_in_blk_ >= ObTmpFileGlobal::BLOCK_PAGE_NUMS) {
      block_index = block_idxs_.at(++read_block_idx_);
      read_page_idx_in_blk_ = 0;
    } else {
      block_index = block_idxs_.at(read_block_idx_);
    }
    ASSERT_NE(-1, block_index);

    ObTmpFileWriteCacheKey page_key(PageType::DATA, fd_, virtual_page_id);
    ObTmpFilePageId page_id(read_page_idx_in_blk_++, block_index);
    ObTmpFilePageHandle page_handle;
    if (OB_FAIL(write_cache_->get_page(page_key, page_handle))) {
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
      LOG_DEBUG("page not exist, load from disk",
          K(page_key), K(write_offset_), K(read_offset_), K(real_size));
      // alloc and load
      int64_t timeout_ms = 5 * 1000;
      blocksstable::MacroBlockId macro_id;
      ASSERT_EQ(OB_SUCCESS, tf_block_mgr_->get_macro_block_id(block_index, macro_id));

      ASSERT_EQ(OB_SUCCESS, write_cache_->alloc_page(page_key, page_id, timeout_ms, page_handle));
      ASSERT_EQ(OB_SUCCESS, write_cache_->load_page(page_handle, macro_id, timeout_ms));
    }

    ASSERT_NE(nullptr, page_handle.get_page());
    char *page_buf = page_handle.get_page()->get_buffer();
    ASSERT_NE(nullptr, page_buf);
    int64_t cmp_offset = (read_block_idx_ * 256 + read_page_idx_in_blk_ - 1) * PAGE_SIZE;
    int cmp = memcmp(buffer_ + cmp_offset, page_buf, PAGE_SIZE);
    if (cmp) {
      printf("data not match, write_offset:%ld, read_offset:%ld\n", write_offset_, read_offset_);
      LOG_INFO("data not match", K(cmp_offset),
          K(block_index), K(read_block_idx_), K(read_page_idx_in_blk_ - 1));
      ret_code_ = OB_ERR_SYS;
    }

    ASSERT_EQ(OB_SUCCESS, write_cache_->free_page(page_key));

    done_size += PAGE_SIZE;
    read_offset_ += PAGE_SIZE;
  }
}

void TestWriteCacheSingleThread::run1()
{
  common::ObCurTraceId::TraceId trace_id;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  if (nullptr != cur_trace_id && cur_trace_id->is_valid()) {
    trace_id = *cur_trace_id;
    LOG_INFO("init TestWriteCacheSingleThread with an old trace_id", KPC(cur_trace_id));
  } else {
    trace_id.init(GCONF.self_addr_);
    LOG_INFO("init TestWriteCacheSingleThread with a new trace_id", K(trace_id));
  }
  ObTraceIDGuard trace_guard(trace_id);
  ObTenantEnv::set_tenant(tenant_ctx_);
  while (OB_SUCCESS == ret_code_ && write_offset_ < buffer_size_) {
    int64_t write_size = ObRandom::rand(16, 128) * PAGE_SIZE;
    write_data(write_size);
    int64_t read_size =  ObRandom::rand(24, 169) * PAGE_SIZE;
    read_data(read_size);
    LOG_DEBUG("TestWriteCacheSingleThread running", K(fd_), K(write_offset_), K(read_offset_), K(buffer_size_));
  }

  if (read_offset_ < buffer_size_) {
    read_data(buffer_size_ - read_offset_);
  }
  current_blk_pages_.reset(); // release page handle
  LOG_INFO("TestWriteCacheSingleThread done", K(fd_), K(write_offset_), K(read_offset_));
}

} // namespace oceanbase

#endif