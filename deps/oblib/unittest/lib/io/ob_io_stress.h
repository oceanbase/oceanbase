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

#ifndef OCEANBASE_IO_OB_IO_STRESS_H_
#define OCEANBASE_IO_OB_IO_STRESS_H_

#include "lib/io/ob_io_manager.h"
#include "lib/thread/thread_pool.h"

using namespace oceanbase::obsys;
using namespace oceanbase::lib;

namespace oceanbase {
namespace common {
class TestIOStress : public lib::ThreadPool {
public:
  TestIOStress();
  virtual ~TestIOStress();
  int init(const ObDiskFd& fd, const int64_t file_size, const int32_t user_thread_cnt, const int32_t sys_thread_cnt,
      const int32_t busy_thread_cnt);
  void run1() final;
  inline void set_fd(int fd)
  {
    fd_.fd_ = fd;
  }
  inline void set_user_iops(const int32_t user_iops)
  {
    user_iops_ = user_iops;
  }
  inline void set_sys_iops(const int32_t sys_iops)
  {
    sys_iops_ = sys_iops;
  }
  inline void set_user_io_size(const int32_t user_io_size)
  {
    user_io_size_ = user_io_size;
  }
  inline int64_t get_fail_count() const
  {
    return fail_count_;
  }
  inline int64_t get_succeed_count() const
  {
    return succeed_count_;
  }
  void set_no_wait()
  {
    no_wait_ = true;
  }
  TO_STRING_KV(K(fd_), K(file_size_), K(user_thread_cnt_), K(sys_thread_cnt_), K(busy_thread_cnt_), K(user_iops_),
      K(sys_iops_), K(user_io_size_), K(sys_io_size_), K(no_wait_), K(fail_count_), K(succeed_count_), K(user_io_cnt_),
      K(user_io_rt_), K(sys_io_cnt_), K(sys_io_rt_));

private:
  ObDiskFd fd_;
  int64_t file_size_;
  int32_t user_thread_cnt_;
  int32_t sys_thread_cnt_;
  int32_t busy_thread_cnt_;
  int32_t user_iops_;
  int32_t sys_iops_;
  int32_t user_io_size_;
  int32_t sys_io_size_;
  int64_t fail_count_;
  int64_t succeed_count_;
  bool no_wait_;

public:
  int64_t user_io_cnt_;
  int64_t user_io_rt_;
  int64_t sys_io_cnt_;
  int64_t sys_io_rt_;
};

TestIOStress::TestIOStress()
    : fd_(),
      file_size_(0),
      user_thread_cnt_(0),
      sys_thread_cnt_(0),
      busy_thread_cnt_(0),
      user_iops_(0),
      sys_iops_(0),
      user_io_size_(16 * 1024),
      sys_io_size_(2 * 1024 * 1024),
      fail_count_(0),
      succeed_count_(0),
      no_wait_(false)
{}

TestIOStress::~TestIOStress()
{}

int TestIOStress::init(const ObDiskFd& fd, const int64_t file_size, const int32_t user_thread_cnt,
    const int32_t sys_thread_cnt, const int32_t busy_thread_cnt)
{
  int ret = OB_SUCCESS;
  int32_t wr_block_size = 2 * 1024 * 1024;
  ObIOInfo io_info;
  char* buffer = NULL;

  if (!fd.is_valid() || file_size <= 0 || user_thread_cnt < 0 || sys_thread_cnt < 0 || busy_thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(
        WARN, "Invalid argument, ", K(fd), K(file_size), K(user_thread_cnt), K(sys_thread_cnt), K(busy_thread_cnt));
  } else if (NULL == (buffer = (char*)malloc(wr_block_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    memset(buffer, 'a', wr_block_size);
    for (int64_t offset = 0; OB_SUCC(ret) && offset < file_size; offset += wr_block_size) {
      io_info.io_desc_.category_ = USER_IO;
      io_info.size_ = wr_block_size;
      io_info.batch_count_ = 1;
      ObIOPoint& io_point = io_info.io_points_[0];
      io_point.fd_ = fd;
      io_point.offset_ = offset;
      io_point.size_ = io_info.size_;
      io_point.write_buf_ = buffer;
      if (OB_FAIL(ObIOManager::get_instance().write(io_info))) {
        COMMON_LOG(ERROR, "Fail to write data file, ", K(ret));
      }
    }

    free(buffer);
  }

  if (OB_SUCC(ret)) {
    fd_ = fd;
    file_size_ = file_size;
    user_thread_cnt_ = user_thread_cnt;
    sys_thread_cnt_ = sys_thread_cnt;
    busy_thread_cnt_ = busy_thread_cnt;
    set_thread_count(user_thread_cnt_ + sys_thread_cnt_ + busy_thread_cnt_);

    user_iops_ = 0;
    sys_iops_ = 0;
    user_io_cnt_ = 0;
    user_io_rt_ = 0;
    sys_io_cnt_ = 0;
    sys_io_rt_ = 0;
    fail_count_ = 0;
    succeed_count_ = 0;
  }
  return ret;
}

void TestIOStress::run1()
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  io_info.batch_count_ = 1;
  ObIOPoint& io_point = io_info.io_points_[0];
  ObIOHandle io_handle;
  uint64_t thread_id = get_thread_idx();
  int64_t pos = 0;
  int32_t io_thread_cnt = user_thread_cnt_ + sys_thread_cnt_;
  bool is_user = (thread_id < user_thread_cnt_);
  bool is_sys = (thread_id >= user_thread_cnt_ && thread_id < io_thread_cnt);
  char* buffer = NULL;
  buffer = (char*)malloc(sys_io_size_);
  memset(buffer, 'a', sys_io_size_);
  int64_t start_time = 0;
  int64_t end_time = 0;
  int64_t rt = 0;

  while (!has_set_stop()) {
    io_handle.reset();
    io_point.fd_ = fd_;
    start_time = ObTimeUtility::current_time();
    pos = ObRandom::rand(0, file_size_);
    if (is_user) {
      int32_t user_iops = user_iops_;
      io_point.offset_ = (pos * 2 * sys_io_size_) % (file_size_ / io_thread_cnt) +
                         (pos * user_io_size_) % (sys_io_size_) + (file_size_ / io_thread_cnt) * thread_id;
      io_point.size_ = static_cast<int32_t>(ObRandom::rand(user_io_size_ / 2, user_io_size_ * 2));
      io_info.size_ = io_point.size_;
      io_info.io_desc_.category_ = USER_IO;
      io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
      io_info.io_desc_.req_deadline_time_ = 0;
      io_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
      if (user_iops <= 100 && thread_id > 0) {
        continue;
      } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
        COMMON_LOG(WARN, "Fail to submit read aio request, ", K(ret));
      } else if (!no_wait_) {
        if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
          COMMON_LOG(WARN, "Fail to wait io handle, ", K(ret));
        } else {
          end_time = ObTimeUtility::current_time();
          rt = end_time - start_time;
          ATOMIC_INC(&user_io_cnt_);
          ATOMIC_FAA(&user_io_rt_, rt);
          if (user_iops > 0) {
            int32_t sleep_us = user_thread_cnt_ * 1000000 / user_iops;
            if (user_iops <= 100) {
              sleep_us = 1000000 / user_iops;
            }
            if (sleep_us > (int32_t)rt) {
              usleep(sleep_us - (int32_t)rt);
            }
          }
        }
      }
    } else if (is_sys) {
      if (sys_iops_ > 0) {
        io_point.offset_ =
            (pos * sys_io_size_) % (file_size_ / io_thread_cnt) + (file_size_ / io_thread_cnt) * thread_id;
        io_point.size_ = sys_io_size_;
        io_point.write_buf_ = buffer;
        io_info.size_ = io_point.size_;
        io_info.io_desc_.category_ = SYS_IO;
        io_info.io_desc_.req_deadline_time_ = 0;
        io_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
        if ((io_point.offset_ / sys_io_size_) % 2 == 0) {
          io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
          if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
            COMMON_LOG(WARN, "Fail to submit read aio request, ", K(ret));
          }
        } else {
          io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
          if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
            COMMON_LOG(WARN, "Fail to submit write aio request, ", K(ret));
          }
        }

        while (ObTimeUtility::current_time() - start_time < 50000) {
          for (int64_t i = 0; i < 1000000; i++) {
            // cost some cpu
          }
        }

        if (!no_wait_) {
          io_handle.wait(160000);
          end_time = ObTimeUtility::current_time();
          rt = end_time - start_time;
          ATOMIC_INC(&sys_io_cnt_);
          ATOMIC_FAA(&sys_io_rt_, rt);
        }
      } else {
        usleep(1000);
      }
    } else {
      // busy
      ++pos;
    }
    if (OB_FAIL(ret)) {
      ++fail_count_;
    } else {
      ++succeed_count_;
    }
  }
  if (buffer != NULL) {
    free(buffer);
    buffer = NULL;
  }
}

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_IO_OB_IO_STRESS_H_
