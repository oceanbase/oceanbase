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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "lib/io/ob_io_manager.h"
#undef private
#include "lib/io/ob_io_benchmark.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/alloc/alloc_func.h"
#include "lib/container/ob_array.h"
#include "lib/stat/ob_di_cache.h"
#include "ob_io_stress.h"

using namespace oceanbase::obsys;
using namespace oceanbase::lib;
using namespace ::testing;

namespace oceanbase {
namespace common {
class MockIOCallback : public ObIOCallback {
public:
  MockIOCallback()
  {}
  virtual ~MockIOCallback()
  {}
  MOCK_CONST_METHOD0(size, int64_t());
  MOCK_METHOD3(alloc_io_buf, int(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset));
  MOCK_METHOD1(inner_process, int(const bool is_success));
  MOCK_CONST_METHOD3(inner_deep_copy, int(char* buf, const int64_t buf_len, ObIOCallback*& callback));
  MOCK_METHOD0(get_data, const char*());
  MOCK_CONST_METHOD2(to_string, int64_t(char* buf, const int64_t buf_len));
};

class TestIOManager : public ::testing::Test {
public:
  TestIOManager();
  virtual ~TestIOManager();
  virtual void SetUp();
  virtual void TearDown();
  virtual void do_multi_disk_test(const bool remove_disk_during_io_ongoing, const bool disk_hang = false);

protected:
  ObDiskFd fd_;
  char filename_[128];
  int64_t file_size_;
};

TestIOManager::TestIOManager() : fd_()
{
  file_size_ = 1024L * 1024L * 1024L;
  snprintf(filename_, 128, "./io_mgr_test_file");
  fd_.disk_id_.disk_idx_ = 0;
  fd_.disk_id_.install_seq_ = 0;
}

TestIOManager::~TestIOManager()
{}

void TestIOManager::SetUp()
{
  int ret = OB_SUCCESS;
  set_memory_limit(4 * 1024 * 1024 * 1024L);
  FileDirectoryUtils::delete_file(filename_);
  ObIOManager::get_instance().destroy();
  const int64_t sys_io_percent = 2;
  ret = ObIOManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  fd_.fd_ = ::open(filename_, O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  ASSERT_TRUE(fd_.fd_ > 0);
#ifdef HAVE_FALLOCATE
  ret = fallocate(fd_, 0 /*MODE*/, 0 /*offset*/, file_size_);
#else
  ret = ftruncate(fd_.fd_, file_size_);
#endif
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().add_disk(fd_, sys_io_percent));
}

void TestIOManager::TearDown()
{
  ObIOManager::get_instance().destroy();
  ::close(fd_.fd_);
  FileDirectoryUtils::delete_file(filename_);
}

void TestIOManager::do_multi_disk_test(const bool remove_disk_during_io_ongoing, const bool disk_hang)
{
  const int64_t file_size = 64L * 1024L * 1024L;
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  static const int64_t DISK_CNT = 4;
  char file_names[DISK_CNT][128];
  int fds[DISK_CNT];
  TestIOStress stress[DISK_CNT];

  // init io manager
  ObIOManager::get_instance().destroy();
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    snprintf(file_names[i], 128, "./test_io_manager%ld", (i + 1));
  }

  ObDiskFd fd;
  fd.disk_id_.disk_idx_ = 0;
  fd.disk_id_.install_seq_ = 0;
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    fds[i] = ::open(file_names[i], O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    ASSERT_TRUE(fds[i] > 0);
    ftruncate(fds[i], file_size);
    fd.fd_ = fds[i];
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().add_disk(fd, ObDisk::DEFAULT_SYS_IO_PERCENT));
    ObMallocAllocator::get_instance()->print_tenant_memory_usage(500);
  }

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    fd.fd_ = fds[i];
    ASSERT_EQ(OB_SUCCESS, stress[i].init(fd, file_size, 3, 1, 0));
    //    stress[i].set_user_iops(1000);
    //    stress[i].set_sys_iops(100);
    COMMON_LOG(WARN, "stress info", K(i), K(fds[i]), K(stress[i]));
  }

  if (disk_hang) {  // hang the first disk, sending fd via error code
#ifdef ERRSIM
    TP_SET_EVENT(EventTable::EN_IO_HANG_ERROR, fds[0], 0, 1);
#endif
  }

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    stress[i].start();
  }

  sleep(3);

  if (disk_hang) {
#ifdef ERRSIM
    TP_SET_EVENT(EventTable::EN_IO_HANG_ERROR, fds[0], 0, 0);
#endif
  }

  if (remove_disk_during_io_ongoing) {
    ObDiskFd fd;
    fd.disk_id_.disk_idx_ = 0;
    fd.disk_id_.install_seq_ = 0;
    for (int64_t i = 0; i < DISK_CNT; ++i) {
      fd.fd_ = fds[i];
      EXPECT_EQ(OB_SUCCESS, ObIOManager::get_instance().delete_disk(fd));
    }
  }

  sleep(3);
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    stress[i].stop();
    stress[i].wait();
    COMMON_LOG(WARN, "stress info", K(i), K(fds[i]), K(stress[i]));
  }

  if (disk_hang) {
#ifdef ERRSIM
    ASSERT_GT(stress[0].get_succeed_count(), 0);
    for (int64_t i = 1; i < DISK_CNT; ++i) {
      ASSERT_GT(stress[i].get_succeed_count(), 0);
      ASSERT_LT(stress[0].get_succeed_count(), stress[i].get_succeed_count());
    }
#endif
  } else {
    for (int64_t i = 0; i < DISK_CNT; ++i) {
      const int64_t fail_count = stress[i].get_fail_count();
      if (!remove_disk_during_io_ongoing) {
        ASSERT_TRUE(fail_count == 0) << "fail_count: " << fail_count;
      } else {
        ASSERT_TRUE(fail_count > 0) << "fail_count: " << fail_count;
      }
      const int64_t succeed_count = stress[i].get_succeed_count();
      COMMON_LOG(INFO, "stat", K(succeed_count));
      ASSERT_TRUE(succeed_count > 0);
    }
  }

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    ObDiskFd fd;
    fd.disk_id_.disk_idx_ = 0;
    fd.disk_id_.install_seq_ = 0;
    fd.fd_ = fds[i];
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().delete_disk(fd));
    ::close(fds[i]);
    FileDirectoryUtils::delete_file(file_names[i]);
  }
}

TEST(ObIOQueue, simple)
{
  int ret = OB_SUCCESS;
  ObIOQueue queue;
  ObIORequest req1;
  ObIORequest req2;
  ObIORequest* preq = NULL;

  // invalid argument
  ret = queue.init(0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = queue.init(-1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = queue.push(req1);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = queue.pop(preq);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal init
  ret = queue.init(10000);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = queue.init(1000);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid argument push and pop

  // push and pop
  ret = queue.push(req1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = queue.pop(preq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(preq, &req1);

  // request sort
  req1.deadline_time_ = 456;
  req2.deadline_time_ = 123;
  ret = queue.push(req1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = queue.push(req2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = queue.pop(preq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(preq, &req2);
  ret = queue.pop(preq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(preq, &req1);

  // prewarm request
  queue.destroy();
  ret = queue.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  req1.desc_.category_ = USER_IO;
  req2.desc_.category_ = PREWARM_IO;
  ret = queue.push(req2);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = queue.push(req1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = queue.pop(preq);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST(ObIOAllocator, simple)
{
  int ret = OB_SUCCESS;
  ObIOAllocator io_allocator;
  void* buf = NULL;
  const int64_t cache_block_size = DIO_ALLOCATOR_CACHE_BLOCK_SIZE;
  const int64_t cache_block_cnt = 16;
  const int64_t mem_limit = 4L * 1024L * 1024L * 1024L;
  const int64_t page_size = 2L * 1024L * 1024L;

  // use when not init
  buf = io_allocator.alloc(1024);
  ASSERT_TRUE(NULL == buf);
  io_allocator.free(buf);

  // invalid init
  ret = io_allocator.init(0, 0);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal init
  ret = io_allocator.init(mem_limit, page_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = io_allocator.init(mem_limit, page_size);
  ASSERT_NE(OB_SUCCESS, ret);

  // alloc cache block size
  buf = io_allocator.alloc(cache_block_size);
  ASSERT_TRUE(NULL != buf);

  io_allocator.free(buf);

  buf = io_allocator.alloc(1024);
  ASSERT_TRUE(NULL != buf);
  io_allocator.free(buf);

  // alloc and free
  void* bufs[cache_block_cnt];
  for (int64_t i = 0; i < cache_block_cnt; ++i) {
    bufs[i] = io_allocator.alloc(cache_block_size);
    ASSERT_TRUE(NULL != bufs[i]);
  }

  for (int64_t i = 0; i < cache_block_cnt; ++i) {
    io_allocator.free(bufs[i]);
  }

  // double destroy
  io_allocator.destroy();
  io_allocator.destroy();
}

class MyThreadPool : public lib::ThreadPool {
public:
  static MyThreadPool& get_ins()
  {
    static MyThreadPool ins;
    return ins;
  }
  void init()
  {
    set_thread_count(64);
    start();
  }

  void destroy()
  {
    stop();
    wait();
  }
  void run1()
  {
    int64_t thread_id = get_thread_idx();
    return (void)thread_id;
  }
};

TEST(ObIOManager, destroy)
{
  set_memory_limit(4 * 1024 * 1024 * 1024L);
  for (int i = 0; i < 10; ++i) {
    COMMON_LOG(INFO, "round", K(i));
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
    ObIOManager::get_instance().destroy();
  }
}

TEST_F(TestIOManager, simple)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObIOHandle io_handle;
  MockIOCallback io_callback;

  ObIOManager::get_instance().destroy();
  // function invoke without init
  ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_read(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_write(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_write(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().read(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().write(io_info);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid add_disk
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  ASSERT_NE(OB_SUCCESS, ObIOManager::get_instance().init());

  ret = ObIOManager::get_instance().add_disk(fd_, 0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().add_disk(fd_, 10);
  ASSERT_EQ(OB_SUCCESS, ret);

  // invalid argument
  ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_read(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_write(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().aio_write(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().read(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObIOManager::get_instance().write(io_info);
  ASSERT_NE(OB_SUCCESS, ret);

  // set valid io info
  io_info.batch_count_ = 1;
  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.offset_ = 0;
  io_point.size_ = 4096;
  io_info.size_ = io_point.size_;
  io_info.io_desc_.category_ = USER_IO;
  io_info.io_desc_.req_deadline_time_ = ObTimeUtility::current_time() + 1000 * 1000 * 10;

  // fail callback deepcopy
  EXPECT_CALL(io_callback, size()).WillRepeatedly(Return(sizeof(io_callback)));
  EXPECT_CALL(io_callback, inner_deep_copy(_, _, _)).WillRepeatedly(Return(OB_ERR_UNEXPECTED));
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().aio_read(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  // fail callback deepcopy
  EXPECT_CALL(io_callback, size()).WillRepeatedly(Return(sizeof(io_callback)));
  EXPECT_CALL(io_callback, deep_copy(_, _, _)).WillRepeatedly(Return(OB_ERR_UNEXPECTED));
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
  ret = ObIOManager::get_instance().aio_write(io_info, io_callback, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal read
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  io_handle.reset();

  // repeat destroy
  ObIOManager::get_instance().destroy();
  ObIOManager::get_instance().destroy();
}

TEST_F(TestIOManager, normal)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObIOHandle io_handle;
  const int64_t data_size = 4096;
  char data[data_size] = "test aio manager";

  io_info.batch_count_ = 1;
  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.size_ = data_size;
  io_point.offset_ = 0;
  io_point.write_buf_ = data;
  io_info.size_ = io_point.size_;
  io_info.io_desc_.category_ = USER_IO;

  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
  ret = ObIOManager::get_instance().write(io_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().read(io_info, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = io_handle.wait(DEFAULT_IO_WAIT_TIME_MS);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = strncmp(data, io_handle.get_buffer(), strlen(data));
  ASSERT_EQ(0, ret);
  io_handle.reset();

  // offset overflow
  //  io_point.offset_ = file_size_ + DIO_READ_ALIGN_SIZE;
  //  ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
  //  ASSERT_EQ(OB_SUCCESS, ret);
  //  ret = io_handle.wait();
  //  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestIOManager, multi)
{
  static const int64_t MULTI_CNT = 1024;
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObIOHandle io_handle[MULTI_CNT];
  char data[4096] = "test aio manager";

  io_info.size_ = 4096;
  io_info.io_desc_.category_ = USER_IO;
  io_info.batch_count_ = 1;

  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.size_ = io_info.size_;
  io_point.offset_ = 0;
  io_point.write_buf_ = data;

  // multi write
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
  for (int64_t i = 0; i < MULTI_CNT; ++i) {
    io_point.offset_ = 4096 * i;
    ret = ObIOManager::get_instance().aio_write(io_info, io_handle[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < MULTI_CNT; ++i) {
    ret = io_handle[i].wait();
    ASSERT_EQ(OB_SUCCESS, ret);
    io_handle[i].reset();
  }

  // multi read
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  for (int64_t i = 0; i < MULTI_CNT; ++i) {
    io_point.offset_ = 4096 * i;
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int64_t i = 0; i < MULTI_CNT; ++i) {
    ret = io_handle[i].wait();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = strncmp(data, io_handle[i].get_buffer(), strlen(data));
    ASSERT_EQ(0, ret);
    io_handle[i].reset();
  }
}

#ifdef ERRSIM
TEST_F(TestIOManager, abnormal)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObIOHandle io_handle;

  io_info.size_ = DIO_READ_ALIGN_SIZE * 4;
  io_info.io_desc_.category_ = USER_IO;
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  io_info.io_desc_.req_deadline_time_ = 0;
  io_info.batch_count_ = 1;

  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.size_ = io_info.size_;
  io_point.offset_ = 0;
  io_point.write_buf_ = NULL;

  // io setup fail
  ObIOManager& io_mgr = ObIOManager::get_instance();
  io_mgr.destroy();
  TP_SET_EVENT(EventTable::EN_IO_SETUP, OB_IO_ERROR, 0, 1);
  ret = io_mgr.init(1024L * 1024L * 1024L);
  ASSERT_NE(OB_SUCCESS, ret);
  TP_SET_EVENT(EventTable::EN_IO_SETUP, OB_SUCCESS, 0, 0);
  io_mgr.destroy();
  ret = io_mgr.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().add_disk(fd_, 10));

  // io partial complete
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_NEED_RETRY, 0, 1);
  for (int64_t i = 0; i < 10000; ++i) {
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = io_handle.wait(DEFAULT_IO_WAIT_TIME_MS);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // io submit fail
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_ERR_UNEXPECTED, 0, 1);
  for (int64_t i = 0; i < 10000; ++i) {
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = io_handle.wait(DEFAULT_IO_WAIT_TIME_MS);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 0);

  // io getevent fail
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_IO_ERROR, 0, 1);
  sleep(1);
  for (int64_t i = 0; i < 10000; ++i) {
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = io_handle.wait(DEFAULT_IO_WAIT_TIME_MS);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 0);
  sleep(1);

  // io hang
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_TIMEOUT, 0, 1);
  sleep(1);
  for (int64_t i = 0; i < 10000; ++i) {
    io_info.offset_ = (io_info.offset_ + io_info.size_) % file_size_;
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    // normal cancel
    this_routine::usleep((uint32_t)ObRandom::rand(1, 500));
    io_handle.reset();
  }

  // io cancel fail
  TP_SET_EVENT(EventTable::EN_IO_CANCEL, OB_ERR_UNEXPECTED, 0, 1);
  for (int64_t i = 0; i < 10000; ++i) {
    ret = ObIOManager::get_instance().aio_read(io_info, io_handle);
    ASSERT_EQ(OB_SUCCESS, ret);
    this_routine::usleep((uint32_t)ObRandom::rand(1, 100));
    io_handle.reset();
  }

  TP_SET_EVENT(EventTable::EN_IO_CANCEL, OB_SUCCESS, 0, 0);
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 0);
  sleep(1);
  ObIOManager::get_instance().reset_disk_error();
}

TEST_F(TestIOManager, io_fault_detector)
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  ObIOHandle io_handle;
  ObIOConfig io_config;
  int64_t io_size = DIO_READ_ALIGN_SIZE * 4;
  char write_buf[io_size];
  bool is_disk_error;
  MEMSET(write_buf, 'a', io_size);

  io_info.size_ = io_size;
  io_info.io_desc_.category_ = USER_IO;
  io_info.io_desc_.req_deadline_time_ = 0;
  io_info.batch_count_ = 1;
  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.offset_ = 0;
  io_point.write_buf_ = write_buf;
  ObIOManager::get_instance().reset_disk_error();
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().is_disk_error(is_disk_error));
  ASSERT_FALSE(is_disk_error);

  // write error
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_IO_ERROR, 0, 1);
  sleep(1);
  for (int64_t i = 0; i < 50; i++) {
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
    ret = ObIOManager::get_instance().write(io_info);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().is_disk_error(is_disk_error));
  ASSERT_FALSE(is_disk_error);

  for (int64_t i = 0; i < 100; i++) {
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
    ret = ObIOManager::get_instance().write(io_info);
    ASSERT_NE(OB_SUCCESS, ret);
  }
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().is_disk_error(is_disk_error));
  ASSERT_TRUE(is_disk_error);

  // read error
  ObIOManager::get_instance().reset_disk_error();
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().is_disk_error(is_disk_error));
  ASSERT_FALSE(is_disk_error);
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().read(io_info, io_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  sleep(1);
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().is_disk_error(is_disk_error));
  ASSERT_TRUE(is_disk_error);

  // clear error
  ObIOManager::get_instance().reset_disk_error();
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 0);
  sleep(1);
}

#endif

TEST_F(TestIOManager, DISABLED_stress)
{
  int ret = OB_SUCCESS;
  TestIOStress stress;
  int64_t old_user_io_cnt = 0;
  int64_t old_user_io_rt = 0;
  int64_t new_user_io_cnt = 0;
  int64_t new_user_io_rt = 0;
  int64_t old_sys_io_cnt = 0;
  int64_t old_sys_io_rt = 0;
  int64_t new_sys_io_cnt = 0;
  int64_t new_sys_io_rt = 0;
  int64_t user_io_cnt = 0;
  int64_t user_io_rt = 0;
  int64_t sys_io_cnt = 0;
  int64_t sys_io_rt = 0;
  ObIOConfig io_config;
  io_config.sys_io_low_percent_ = 0;
  io_config.sys_io_high_percent_ = 90;
  io_config.user_iort_up_percent_ = 100;
  io_config.cpu_high_water_level_ = 3200;

  ret = ObIOBenchmark::get_instance().init("./", "./");
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObIOManager::get_instance().set_io_config(io_config);
  ASSERT_EQ(OB_SUCCESS, ret);

  stress.init(fd_, file_size_, 16, 16, 0);
  stress.set_user_iops(100000);
  stress.set_sys_iops(1000);
  stress.start();

  for (int64_t i = 0; i < 150; ++i) {
    sleep(1);
    new_user_io_cnt = stress.user_io_cnt_;
    new_user_io_rt = stress.user_io_rt_;
    new_sys_io_cnt = stress.sys_io_cnt_;
    new_sys_io_rt = stress.sys_io_rt_;
    user_io_cnt = new_user_io_cnt - old_user_io_cnt;
    user_io_rt = new_user_io_rt - old_user_io_rt;
    sys_io_cnt = new_sys_io_cnt - old_sys_io_cnt;
    sys_io_rt = new_sys_io_rt - old_sys_io_rt;
    old_user_io_cnt = new_user_io_cnt;
    old_user_io_rt = new_user_io_rt;
    old_sys_io_cnt = new_sys_io_cnt;
    old_sys_io_rt = new_sys_io_rt;
    COMMON_LOG(INFO,
        "Stress Info, ",
        K(i),
        "User IOPS",
        user_io_cnt,
        "User RT",
        0 == user_io_cnt ? 0 : user_io_rt / user_io_cnt,
        "Sys IOPS",
        sys_io_cnt,
        "Sys RT",
        0 == sys_io_cnt ? 0 : sys_io_rt / sys_io_cnt);
    if (i < 30) {
      stress.set_user_iops(1000);
    } else if (i < 60) {
      stress.set_user_iops(10000);
    } else if (i < 90) {
      stress.set_user_iops(100000);
    } else if (i < 120) {
      stress.set_user_iops(100);
    } else {
      stress.set_user_iops(10);
    }
  }
  stress.stop();
  stress.wait();
}

TEST_F(TestIOManager, multi_disk)
{
  do_multi_disk_test(false);
}

TEST_F(TestIOManager, multi_disk_remove)
{
  do_multi_disk_test(true);
}

TEST_F(TestIOManager, add_delete)
{
  CHUNK_MGR.set_limit(10L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.delete_disk(fd_));
  ObDiskFd disk_fd;
  disk_fd.disk_id_.disk_idx_ = 0;
  disk_fd.disk_id_.install_seq_ = 0;
  for (int fd = 1; fd < ObDiskManager::MAX_DISK_NUM; ++fd) {
    disk_fd.fd_ = fd;
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.add_disk(disk_fd, 10));
  }
  for (int fd = 1; fd < ObDiskManager::MAX_DISK_NUM; ++fd) {
    disk_fd.fd_ = fd;
    ASSERT_EQ(OB_ENTRY_EXIST, OB_IO_MANAGER.add_disk(disk_fd, 10));
  }
  for (int fd = 1; fd < ObDiskManager::MAX_DISK_NUM; ++fd) {
    disk_fd.fd_ = fd;
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.delete_disk(disk_fd));
  }
  for (int fd = 1; fd < ObDiskManager::MAX_DISK_NUM; ++fd) {
    disk_fd.fd_ = fd;
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.add_disk(disk_fd, 10));
  }
  for (int fd = 1; fd < ObDiskManager::MAX_DISK_NUM; ++fd) {
    disk_fd.fd_ = fd;
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.delete_disk(disk_fd));
  }
}

class TestMemLeakIOCallback : public ObIOCallback {
public:
  TestMemLeakIOCallback() : buf_(NULL), allocator_(nullptr), buf_size_(0)
  {}
  virtual ~TestMemLeakIOCallback()
  {}
  virtual int64_t size() const
  {
    return sizeof(TestMemLeakIOCallback);
  }
  virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf_ = (char*)allocator_->alloc(buf_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      io_buf = reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(buf_), DIO_READ_ALIGN_SIZE));
      io_buf_size = buf_size_;
      aligned_offset = 0;
    }
    return ret;
  }
  virtual int inner_process(const bool is_success)
  {
    UNUSED(is_success);
    if (NULL != buf_) {
      allocator_->free(buf_);
      buf_ = NULL;
    }
    return OB_SUCCESS;
  }
  virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
    } else {
      TestMemLeakIOCallback* pcallback = new (buf) TestMemLeakIOCallback();
      *pcallback = *this;
      callback = pcallback;
    }
    return ret;
  }
  virtual const char* get_data()
  {
    return buf_;
  }
  TO_STRING_KV(KP_(buf));

public:
  char* buf_;
  ObIAllocator* allocator_;
  int64_t buf_size_;
};

#ifdef ERRSIM
TEST_F(TestIOManager, test_mem_leak)
{
  ObConcurrentFIFOAllocator allocator;
  const int64_t MEM_LIMIT = 200 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, allocator.init(MEM_LIMIT, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE));

  // char *buf = (char *)allocator.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);
  // allocator.free(buf);
  // ASSERT_EQ(0, allocator.allocated()) << "allocator used size: " << allocator.allocated();

  ObIOInfo io_info;
  ObIOHandle io_handle;

  io_info.fd_ = fd_;
  io_info.offset_ = 0;
  io_info.size_ = DIO_READ_ALIGN_SIZE * 4;
  io_info.io_desc_.category_ = USER_IO;
  io_info.io_desc_.req_deadline_time_ = 0;
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  // io hang
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, 1 /* io_ret */, 0, 1);
  // simu io_cancel success. Manually map OB_NOT_SUPPORTED to OB_SUCCESS,
  // because inject error_code=0 is confusing with default value of not inject
  TP_SET_EVENT(EventTable::EN_IO_CANCEL, OB_NOT_SUPPORTED /* io_ret */, 0, 1);
  sleep(1);
  for (int64_t i = 0; i < 10; ++i) {
    TestMemLeakIOCallback callback;
    callback.buf_size_ = OB_MALLOC_NORMAL_BLOCK_SIZE;
    callback.allocator_ = &allocator;
    io_info.offset_ = (io_info.offset_ + io_info.size_) % file_size_;
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().aio_read(io_info, callback, io_handle));
    // normal cancel
    this_routine::usleep(10);
    io_handle.reset();
  }
  ASSERT_EQ(0, allocator.allocated()) << "allocator used size: " << allocator.allocated();
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 0);
  TP_SET_EVENT(EventTable::EN_IO_CANCEL, OB_SUCCESS, 0, 0);
}

TEST_F(TestIOManager, enqueue_failed)
{
  int ret = OB_SUCCESS;
  ObIOManager::ObDiskIOManagerGuard guard;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().get_disk_io_mgr(fd_, guard));
  ObDisk* disk_io_mgr = guard.get_disk_io_mgr();
  ASSERT_NE(nullptr, disk_io_mgr);
  ObIOController* control = NULL;
  ASSERT_EQ(OB_SUCCESS, disk_io_mgr->get_free_control(control));
  COMMON_LOG(INFO, "after alloc ref_cnt", K(control->ref_cnt_), K(control->out_ref_cnt_), K(control->disk_io_mgr_));
  ObIOHandle io_handle;
  ASSERT_EQ(OB_SUCCESS, io_handle.set_ctrl(*control));
  COMMON_LOG(INFO, "after set_ctrl ref_cnt", K(control->ref_cnt_), K(control->out_ref_cnt_), K(control->disk_io_mgr_));
  ObIOChannel* channel = NULL;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().get_disk_channel(disk_io_mgr->get_fd(), channel));
  ASSERT_NE(channel, nullptr);
  TP_SET_EVENT(EventTable::EN_IO_CHANNEL_QUEUE_ERROR,
      OB_QUEUE_OVERFLOW /*error_code*/,
      0 /*nr_occurence*/,
      1 /*triger_frequency*/);
  ASSERT_NE(OB_SUCCESS, ret = disk_io_mgr->enqueue(*control, *channel));
  COMMON_LOG(
      INFO, "after enqueue fail ref_cnt", K(control->ref_cnt_), K(control->out_ref_cnt_), K(control->disk_io_mgr_));
  ASSERT_EQ(OB_QUEUE_OVERFLOW, ret);
  io_handle.reset();
  COMMON_LOG(INFO, "after reset ref_cnt", K(control->ref_cnt_), K(control->out_ref_cnt_), K(control->disk_io_mgr_));
  ASSERT_EQ(0, control->ref_cnt_);
  ASSERT_EQ(0, control->out_ref_cnt_);
  TP_SET_EVENT(EventTable::EN_IO_CHANNEL_QUEUE_ERROR, OB_SUCCESS, 0, 0);
}

#endif

class TestIOCallback : public ObIOCallback {
public:
  TestIOCallback() : buf_(NULL), buf_size_(OB_MALLOC_NORMAL_BLOCK_SIZE + DIO_READ_ALIGN_SIZE), allocator_(NULL)
  {}
  virtual ~TestIOCallback()
  {}
  virtual int64_t size() const
  {
    return sizeof(TestIOCallback);
  }
  virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
  {
    int ret = OB_SUCCESS;
    buf_ = (char*)allocator_->alloc(buf_size_);
    if (NULL == buf_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "Fail to allocate memory", K(ret));
    } else {
      io_buf = reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(buf_), DIO_READ_ALIGN_SIZE));
      io_buf_size = OB_MALLOC_NORMAL_BLOCK_SIZE;
      aligned_offset = 0;
    }
    return ret;
  }
  virtual int inner_process(const bool is_success)
  {
    UNUSED(is_success);
    int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "Callback called");
    if (NULL != buf_) {
      allocator_->free(buf_);
      buf_ = NULL;
    }
    return ret;
  }
  virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
    } else {
      TestIOCallback* pcallback = new (buf) TestIOCallback();
      *pcallback = *this;
      callback = pcallback;
    }
    return ret;
  }
  virtual const char* get_data()
  {
    return buf_;
  }
  TO_STRING_KV(KP_(buf));

public:
  char* buf_;
  int64_t buf_size_;
  ObArenaAllocator* allocator_;
};

TEST_F(TestIOManager, callback)
{
  ObArenaAllocator allocator;
  ObIOInfo io_info;
  ObIOHandle io_handle;

  io_info.batch_count_ = 1;
  ObIOPoint& io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.size_ = OB_MALLOC_NORMAL_BLOCK_SIZE;
  io_point.offset_ = 0;
  io_info.size_ = io_point.size_;
  io_info.io_desc_.category_ = USER_IO;
  io_info.io_desc_.req_deadline_time_ = 0;
  io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;

  TestIOCallback callback;
  callback.allocator_ = &allocator;
  io_info.io_desc_ = ObIOMode::IO_MODE_READ;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().aio_read(io_info, callback, io_handle));
  ASSERT_EQ(OB_SUCCESS, io_handle.wait(10 * 1000));

  char buf[OB_MALLOC_NORMAL_BLOCK_SIZE] = {'c'};
  io_point.write_buf_ = buf;

  io_info.io_desc_ = ObIOMode::IO_MODE_WRITE;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().aio_write(io_info, callback, io_handle));
  ASSERT_EQ(OB_SUCCESS, io_handle.wait(10 * 1000));
  // usleep(1000);
  // ObIOManager::get_instance().destroy();
  COMMON_LOG(INFO, "Now tear down");
}

#ifdef ERRSIM
TEST_F(TestIOManager, disk_hang)
{
  do_multi_disk_test(false, true);
}
#endif

TEST_F(TestIOManager, batch_io)
{
  int ret = OB_SUCCESS;
  ObIOInfo info;
  ObIOHandle io_handle;
  const int64_t batch_count = 16;
  const int32_t io_size = batch_count * DIO_READ_ALIGN_SIZE;
  char data[io_size] = "test batch aio";
  for (int64_t i = 0; i < io_size - 1; ++i) {
    data[i] = static_cast<char>(ObRandom::rand(static_cast<const int64_t>('a'), static_cast<const int64_t>('z')));
  }

  info.io_desc_.category_ = USER_IO;
  info.size_ = io_size;
  info.batch_count_ = batch_count;

  // aligned write and read, single disk
  for (int64_t i = 0; i < batch_count; ++i) {
    ObIOPoint& point = info.io_points_[i];
    point.fd_ = fd_;
    point.offset_ = i * DIO_READ_ALIGN_SIZE;
    point.size_ = DIO_READ_ALIGN_SIZE;
    point.write_buf_ = data + point.offset_;
  }
  info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
  ret = ObIOManager::get_instance().write(info);
  ASSERT_EQ(OB_SUCCESS, ret);

  info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
  ret = ObIOManager::get_instance().read(info, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = strncmp(data, io_handle.get_buffer(), strlen(data));
  ASSERT_EQ(0, ret);
  io_handle.reset();

  // random read, single disk
  ObIOPoint& first_point = info.io_points_[0];
  first_point.offset_ = ObRandom::rand(0, DIO_READ_ALIGN_SIZE);
  first_point.size_ = static_cast<int32_t>(DIO_READ_ALIGN_SIZE - first_point.offset_);
  ObIOPoint& last_point = info.io_points_[batch_count - 1];
  last_point.size_ = static_cast<int32_t>(ObRandom::rand(0, DIO_READ_ALIGN_SIZE));
  info.size_ = io_size - (2 * DIO_READ_ALIGN_SIZE) + first_point.size_ + last_point.size_;
  info.offset_ = first_point.offset_;

  COMMON_LOG(INFO, "read param", K(first_point), K(last_point), K(info));

  ret = ObIOManager::get_instance().read(info, io_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char* src = data + first_point.offset_;
  const char* buf = io_handle.get_buffer();
  ret = strncmp(src, buf, first_point.size_);
  if (0 != ret) {
    COMMON_LOG(WARN, "wrong data of 0", K(ret));
  }

  for (int64_t i = 1; i < batch_count; ++i) {
    int64_t pre_size = info.io_points_[i - 1].size_;
    src += pre_size;
    buf += pre_size;
    ret = strncmp(src, buf, info.io_points_[i].size_);
    if (0 != ret) {
      COMMON_LOG(WARN, "wrong data", K(i), K(ret));
    }
  }
  ret = strncmp(data + first_point.offset_, io_handle.get_buffer(), info.size_);
  ASSERT_EQ(0, ret);
  io_handle.reset();
}

TEST_F(TestIOManager, size)
{
  int64_t master_size = sizeof(ObIOMaster);
  int64_t request_size = sizeof(ObIORequest);
  int64_t info_size = sizeof(ObIOInfo);
  int64_t point_size = sizeof(ObIOPoint);
  int64_t ptr_size = sizeof(ObIORequest*);
  int64_t condition_size = sizeof(ObThreadCond);
  int64_t mutex_size = sizeof(ObMutex);
  COMMON_LOG(INFO,
      "struct size",
      K(master_size),
      K(request_size),
      K(info_size),
      K(point_size),
      K(ptr_size),
      K(condition_size),
      K(mutex_size));
}

}  // end namespace common
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_file_name("./test_io_manager.log");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
