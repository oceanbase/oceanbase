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

#ifndef OB_SN_TMP_FILE_TEST_HELPER_
#define OB_SN_TMP_FILE_TEST_HELPER_
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "mittest/mtlenv/storage/tmp_file/ob_tmp_file_test_helper.h"
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
using namespace common;
using namespace tmp_file;
using namespace share;
/* -------------------------- Test Helper --------------------------- */
int write_and_get_file_size(const ObTmpFileIOInfo &io_info, int64_t &cur_file_size)
{
  int ret = OB_SUCCESS;
  ObSNTenantTmpFileManager &sn_mgr = MTL(ObTenantTmpFileManager *)->get_sn_file_manager();
  ObSNTmpFileHandle tmp_file_handle;
  ObTmpFileIOWriteCtx ctx;
  if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to aio write, invalid argument", KR(ret), K(io_info));
  } else if (OB_FAIL(sn_mgr.get_tmp_file(io_info.fd_, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file io handle", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx.init(io_info.fd_, io_info.io_desc_,
                              io_info.io_timeout_ms_))) {
    LOG_WARN("failed to init io context", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx.prepare(io_info.buf_, io_info.size_))) {
    LOG_WARN("fail to prepare write context", KR(ret), K(io_info));
  } else if (OB_FAIL(tmp_file_handle.get()->write(ctx, cur_file_size))) {
    LOG_WARN("fail to write", KR(ret), K(io_info), K(tmp_file_handle));
  } else {
    tmp_file_handle.get()->set_write_stats_vars(ctx);
  }

  LOG_DEBUG("write a tmp file over", KR(ret), K(io_info), K(ctx));
  return ret;
}

/* -------------------------- TestTmpFileStress --------------------------- */
enum TmpFileOp {
  WRITE,
  READ,
  TRUNCATE,
  OP_MAX
};

class TestTmpFileStress : public share::ObThreadPool
{
public:
  TestTmpFileStress(ObTenantBase *tenant_ctx);
  virtual ~TestTmpFileStress();
  int init(const int fd, const TmpFileOp op, const int64_t thread_cnt, const int64_t timeout_ms,
           char *buf, const int64_t offset, const int64_t size);
  void reset();
  virtual void run1();
  TO_STRING_KV(K(thread_cnt_), K(fd_), K(timeout_ms_), K(op_), KP(buf_), K(offset_), K(size_));
private:
  void write_data_(const int64_t write_size);
  void inner_write_data_(const ObTmpFileIOInfo &io_info);
  void truncate_data_();
  void read_data_(const int64_t read_offset, const int64_t read_size);
private:
  int64_t thread_cnt_;
  int fd_;
  int64_t timeout_ms_;
  TmpFileOp op_;
  char *buf_;
  int64_t offset_;
  int64_t size_;
  ObTenantBase *tenant_ctx_;
};

TestTmpFileStress::TestTmpFileStress(ObTenantBase *tenant_ctx)
  : thread_cnt_(0), fd_(0),
    timeout_ms_(0), op_(OP_MAX),
    buf_(nullptr), offset_(0),
    size_(0),
    tenant_ctx_(tenant_ctx)
{
}

TestTmpFileStress::~TestTmpFileStress()
{
}

int TestTmpFileStress::init(const int fd, const TmpFileOp op,
                            const int64_t thread_cnt,
                            const int64_t timeout_ms,
                            char *buf, int64_t offset,
                            const int64_t size)
{
  int ret = OB_SUCCESS;
  if (thread_cnt < 0 || OB_ISNULL(buf) || offset < 0 || size <= 0 || timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt), KP(buf), K(offset), K(size), K(timeout_ms));
  } else if (TmpFileOp::OP_MAX == op) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(op));
  } else if (op == TmpFileOp::TRUNCATE && 1 != thread_cnt) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(op), K(thread_cnt));
  } else {
    buf_ = buf;
    thread_cnt_ = thread_cnt;
    timeout_ms_ = timeout_ms;
    fd_ = fd;
    op_ = op;
    offset_ = offset;
    size_ = size;
    set_thread_count(static_cast<int32_t>(thread_cnt));
  }
  return ret;
}

void TestTmpFileStress::reset()
{
  thread_cnt_ = 0;
  fd_ = 0;
  op_ = OP_MAX;
  buf_ = nullptr;
  offset_ = 0;
  size_ = 0;
}

void TestTmpFileStress::write_data_(const int64_t write_size)
{
  STORAGE_LOG(INFO, "TestTmpFileStress write thread", K(fd_), K(thread_idx_), KP(buf_), K(write_size));
  int ret = OB_SUCCESS;
  ObArray<int64_t> size_array;
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = timeout_ms_;
  int64_t already_write = 0;
  char *write_buffer = new char[write_size];
  MEMSET(write_buffer, 0, write_size);

  std::vector<int64_t> turn_write_size = generate_random_sequence(ObTmpFileGlobal::PAGE_SIZE/3, write_size / 3, write_size, 3);
  for (int64_t i = 0; i < write_size;) {
    int64_t random_length = generate_random_int(1024, 8 * 1024);
    int64_t random_int = generate_random_int(0, 256);
    for (int64_t j = 0; j < random_length && i + j < write_size; ++j) {
      write_buffer[i + j] = random_int;
    }
    i += random_length;
  }

  for (int i = 0; i < turn_write_size.size(); ++i) {
    int64_t this_turn_write_size = turn_write_size[i];
    STORAGE_LOG(INFO, "random write size", K(fd_), K(thread_idx_), KP(buf_),
                K(write_size), K(already_write), K(this_turn_write_size));
    // write data
    io_info.buf_ = write_buffer + already_write;
    if (this_turn_write_size % ObTmpFileGlobal::PAGE_SIZE == 0 && i == 0) {
      io_info.size_ = this_turn_write_size - 2 * 1024;
      inner_write_data_(io_info);

      io_info.buf_ += io_info.size_;
      io_info.size_ = 2 * 1024;
      inner_write_data_(io_info);
    } else {
      io_info.size_ = this_turn_write_size;
      inner_write_data_(io_info);
    }
    already_write += this_turn_write_size;
  }

  ASSERT_EQ(OB_SUCCESS, ret);
  delete[] write_buffer;
  STORAGE_LOG(INFO, "TestTmpFileStress write thread finished", K(fd_), K(thread_idx_), KP(buf_), K(size_));
}

void TestTmpFileStress::inner_write_data_(const ObTmpFileIOInfo &io_info)
{
  int64_t start_write_offset = 0;
  int64_t end_write_offset = 0;
  int ret = write_and_get_file_size(io_info, end_write_offset);
  start_write_offset = end_write_offset - io_info.size_;
  STORAGE_LOG(INFO, "write size", K(fd_), K(thread_idx_), KP(buf_),
              K(start_write_offset), K(end_write_offset));
  if (OB_FAIL(ret) || end_write_offset <= 0 || end_write_offset < io_info.size_) {
    STORAGE_LOG(ERROR, "TestTmpFileStress write thread failed", KR(ret), K(fd_), K(thread_idx_),
                K(start_write_offset), K(end_write_offset), K(io_info));
    ob_abort();
  } else {
    char *zero_buf = new char[io_info.size_];
    MEMSET(zero_buf, 0, io_info.size_);
    int cmp = 0;
    cmp = MEMCMP(zero_buf, buf_ + start_write_offset, io_info.size_);
    if (cmp != 0) {
      STORAGE_LOG(ERROR, "TestTmpFileStress write thread failed", KR(ret), K(fd_), K(thread_idx_),
                  K(start_write_offset), K(end_write_offset), K(io_info));
      ob_abort();
    }
    MEMCPY(buf_ + start_write_offset, io_info.buf_, io_info.size_);
    delete[] zero_buf;
  }
}

void TestTmpFileStress::read_data_(const int64_t read_offset, const int64_t read_size)
{
  STORAGE_LOG(INFO, "TestTmpFileStress read thread start", K(fd_), K(thread_idx_), KP(buf_), K(read_offset), K(read_size));
  int ret = OB_SUCCESS;
  char *read_buf = new char[read_size];
  ObTmpFileIOInfo io_info;
  ObTmpFileIOHandle handle;
  io_info.fd_ = fd_;
  io_info.size_ = read_size;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = timeout_ms_;
  io_info.buf_ = read_buf;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, read_offset, handle);
  int cmp = MEMCMP(handle.get_buffer(), buf_ + read_offset, io_info.size_);
  if (cmp != 0 || OB_FAIL(ret)) {
    printf("TestTmpFileStress read thread failed, fd_:%d, thread_idx_:%ld\n", fd_, thread_idx_);
    const int64_t start_virtual_page_id = read_offset / ObTmpFileGlobal::PAGE_SIZE;
    STORAGE_LOG(ERROR, "TestTmpFileStress read thread failed", KR(ret), K(fd_), K(cmp), K(thread_idx_), KP(buf_),
                K(start_virtual_page_id), K(read_offset), K(read_size));
    ob_abort();
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  STORAGE_LOG(INFO, "TestTmpFileStress read thread finished", K(fd_), K(thread_idx_), KP(buf_), K(read_offset), K(read_size));
}

void TestTmpFileStress::truncate_data_()
{
  int64_t truncate_offset = offset_ + MIN(size_, MAX(size_ / 10, 8 * 1024));
  STORAGE_LOG(INFO, "TestTmpFileStress truncate thread start", K(fd_), K(thread_idx_), KP(buf_),
              K(truncate_offset), K(offset_), K(size_));
  int ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd_, truncate_offset);
  if (OB_FAIL(ret)) {
    STORAGE_LOG(ERROR, "TestTmpFileStress truncate thread failed", KR(ret), K(fd_), K(thread_idx_),
                K(truncate_offset));
    ob_abort();
  }
  ObTmpFileIOInfo io_info;
  io_info.fd_ = fd_;
  io_info.io_desc_.set_wait_event(2);
  io_info.io_timeout_ms_ = timeout_ms_;
  const int64_t invalid_size = truncate_offset - offset_;
  const int64_t valid_size = size_ - invalid_size;

  char *zero_buf = new char[invalid_size];
  MEMSET(zero_buf, 0, invalid_size);
  char *read_buf = new char[size_];
  io_info.size_ = size_;
  io_info.buf_ = read_buf;
  ObTmpFileIOHandle handle;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, offset_, handle);
  int cmp = MEMCMP(handle.get_buffer()+invalid_size, buf_ + truncate_offset, valid_size);
  if (cmp != 0 || OB_FAIL(ret)) {
    STORAGE_LOG(ERROR, "TestTmpFileStress truncate thread failed. "
                "fail to compare valid part.", KR(ret), K(cmp), K(fd_), K(thread_idx_), KP(buf_),
                K(truncate_offset), K(valid_size), K(invalid_size), K(offset_), K(size_));
    ob_abort();
  }
  cmp = MEMCMP(handle.get_buffer(), zero_buf, invalid_size);
  if (cmp != 0) {
    STORAGE_LOG(ERROR, "TestTmpFileStress truncate thread failed. "
                "fail to compare zero part.", KR(ret), K(cmp), K(fd_), K(thread_idx_), KP(buf_),
                K(truncate_offset), K(valid_size), K(invalid_size), K(offset_), K(size_));
    ob_abort();
  }
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  delete[] zero_buf;

  truncate_offset = offset_ + size_;
  ret = MTL(ObTenantTmpFileManager *)->truncate(MTL_ID(), fd_, truncate_offset);
  if (OB_FAIL(ret)) {
    STORAGE_LOG(ERROR, "TestTmpFileStress truncate thread failed", KR(ret), K(fd_), K(thread_idx_),
                K(truncate_offset));
    ob_abort();
  }

  zero_buf = new char[size_];
  MEMSET(zero_buf, 0, size_);
  read_buf = new char[size_];
  io_info.buf_ = read_buf;
  ret = MTL(ObTenantTmpFileManager *)->pread(MTL_ID(), io_info, offset_, handle);
  cmp = MEMCMP(handle.get_buffer(), zero_buf, size_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, cmp);
  handle.reset();
  delete[] read_buf;
  delete[] zero_buf;
  STORAGE_LOG(INFO, "TestTmpFileStress truncate thread finished", K(fd_), K(thread_idx_), KP(buf_), K(offset_), K(size_));
}

void TestTmpFileStress::run1()
{
  ObTenantEnv::set_tenant(tenant_ctx_);
  common::ObCurTraceId::TraceId trace_id;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  if (nullptr != cur_trace_id && cur_trace_id->is_valid()) {
    trace_id = *cur_trace_id;
    LOG_INFO("init TestTmpFileStress with an old trace_id", KPC(cur_trace_id), KPC(this));
  } else {
    trace_id.init(GCONF.self_addr_);
    LOG_INFO("init TestTmpFileStress with a new trace_id", K(trace_id), KPC(this));
  }
  ObTraceIDGuard trace_guard(trace_id);

  if (op_ == TmpFileOp::WRITE) {
    int64_t write_size = 0;
    if (thread_idx_ == thread_cnt_ - 1) {
      write_size = size_ / thread_cnt_ + size_ % thread_cnt_;
    } else {
      write_size = size_ / thread_cnt_;
    }
    write_data_(write_size);
  } else if (op_ == TmpFileOp::READ) {
    int64_t read_offset = offset_ + (size_ / thread_cnt_) * thread_idx_;
    int64_t read_size = 0;
    if (thread_idx_ == thread_cnt_ - 1) {
      read_size = size_ / thread_cnt_ + size_ % thread_cnt_;
    } else {
      read_size = size_ / thread_cnt_;
    }
    read_data_(read_offset, read_size);
  } else {
    truncate_data_();
  }
}

/* -------------------------- TestMultiTmpFileStress --------------------------- */
class TestMultiTmpFileStress : public share::ObThreadPool
{
public:
  TestMultiTmpFileStress(ObTenantBase *tenant_ctx);
  virtual ~TestMultiTmpFileStress();
  int init(const int64_t file_cnt, const int64_t dir_id,
           const int64_t write_thread_cnt,const int64_t read_thread_cnt,
           const int64_t timeout_ms,
           const int64_t batch_size, const int64_t batch_num);
  virtual void run1();
private:
  int64_t file_cnt_;
  int64_t dir_id_;
  int64_t write_thread_cnt_perf_file_;
  int64_t read_thread_cnt_perf_file_;
  int64_t timeout_ms_;
  int64_t batch_size_;
  int64_t batch_num_;
  ObTenantBase *tenant_ctx_;
};

TestMultiTmpFileStress::TestMultiTmpFileStress(ObTenantBase *tenant_ctx)
  : file_cnt_(0),
    dir_id_(-1),
    write_thread_cnt_perf_file_(0),
    read_thread_cnt_perf_file_(0),
    timeout_ms_(0),
    batch_size_(0),
    batch_num_(0),
    tenant_ctx_(tenant_ctx)
{
}

TestMultiTmpFileStress::~TestMultiTmpFileStress()
{
}

int TestMultiTmpFileStress::init(const int64_t file_cnt,
                                 const int64_t dir_id,
                                 const int64_t write_thread_cnt,
                                 const int64_t read_thread_cnt,
                                 const int64_t timeout_ms,
                                 const int64_t batch_size,
                                 const int64_t batch_num)
{
  int ret = OB_SUCCESS;
  if (file_cnt < 0 || write_thread_cnt < 0 || read_thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(write_thread_cnt), K(read_thread_cnt));
  } else {
    file_cnt_ = file_cnt;
    dir_id_ = dir_id;
    write_thread_cnt_perf_file_ = write_thread_cnt;
    read_thread_cnt_perf_file_ = read_thread_cnt;
    timeout_ms_ = timeout_ms;
    batch_size_ = batch_size;
    batch_num_ = batch_num;
    set_thread_count(static_cast<int32_t>(file_cnt));
  }
  return ret;
}

void TestMultiTmpFileStress::run1()
{
  STORAGE_LOG(INFO, "TestMultiTmpFileStress thread run start");
  int ret = OB_SUCCESS;
  int64_t fd = 0;
  ObTenantEnv::set_tenant(tenant_ctx_);

  ret = MTL(ObTenantTmpFileManager *)->open(fd, dir_id_, "");
  std::cout << "normal case, fd: " << fd << std::endl;
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "open file success", K(fd));

  int64_t file_size = batch_size_ * batch_num_;
  char * data_buffer = new char[file_size];
  MEMSET(data_buffer, 0, file_size);

  TestTmpFileStress test_truncate(tenant_ctx_);
  for (int64_t i = 0; i < batch_num_; ++i) {
    if (i > 0) {
      // truncate read data in previous round
      test_truncate.init(fd, TmpFileOp::TRUNCATE, 1, timeout_ms_, data_buffer, (i-1) * batch_size_, batch_size_);
      ASSERT_EQ(OB_SUCCESS, ret);
      STORAGE_LOG(INFO, "test_truncate run start", K(i), K(batch_size_));
      test_truncate.start();
    }
    TestTmpFileStress test_write(tenant_ctx_);
    ret = test_write.init(fd, TmpFileOp::WRITE, write_thread_cnt_perf_file_, timeout_ms_, data_buffer, 0, batch_size_);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "test_write run start", K(i), K(batch_size_));
    test_write.start();
    test_write.wait();
    STORAGE_LOG(INFO, "test_write run end", K(i));

    TestTmpFileStress test_read(tenant_ctx_);
    ret = test_read.init(fd, TmpFileOp::READ, read_thread_cnt_perf_file_, timeout_ms_, data_buffer, i * batch_size_, batch_size_);
    ASSERT_EQ(OB_SUCCESS, ret);

    STORAGE_LOG(INFO, "test_read run start", K(i), K(batch_size_));
    test_read.start();
    test_read.wait();
    STORAGE_LOG(INFO, "test_read run end", K(i));

    if (i > 0) {
      // wait to truncate read data in last round
      test_truncate.wait();
      test_truncate.reset();
      STORAGE_LOG(INFO, "test_truncate run end", K(i));
    }

    STORAGE_LOG(INFO, "TestMultiTmpFileStress thread run a batch end", K(i));
  }

  test_truncate.init(fd, TmpFileOp::TRUNCATE, 1, timeout_ms_, data_buffer, file_size - batch_size_, batch_size_);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "test_truncate run start");
  test_truncate.start();
  test_truncate.wait();
  STORAGE_LOG(INFO, "test_truncate run end");

  ret = MTL(ObTenantTmpFileManager *)->remove(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete[] data_buffer;
  STORAGE_LOG(INFO, "TestMultiTmpFileStress thread run end");
}

} // namespace oceanbase

#endif
