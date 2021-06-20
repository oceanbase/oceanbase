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
#include "ob_io_stress.h"

using namespace oceanbase::obsys;
using namespace oceanbase::lib;
using namespace ::testing;

namespace oceanbase {
namespace common {
class TestDiskError : public ::testing::Test {
public:
  TestDiskError()
  {}
  virtual ~TestDiskError()
  {}
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_conf(const int trigger_fd, int submit_fail, int io_hang, int io_timeout);
  virtual void preapre_stress(const int64_t submit_thread_cnt = 1);
  virtual void start_stress();
  virtual void stop_stress();
  virtual void destroy_stress();
  virtual void set_no_wait();
  virtual void set_io_size(const int64_t disk_index, const int64_t io_size);

protected:
  static const int64_t DISK_CNT = 2;
  int64_t file_size_;
  char file_names_[DISK_CNT][128];
  int fds_[DISK_CNT];
  TestIOStress stress_[DISK_CNT];
};

void TestDiskError::SetUp()
{
  file_size_ = 1024L * 1024L * 1024L;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  prepare_conf(-1, 0, 0, 0);
  sleep(10);
}

void TestDiskError::TearDown()
{
  ObIOManager::get_instance().destroy();
}

void TestDiskError::prepare_conf(const int trigger_fd, int submit_fail, int io_hang, int io_timeout)
{
  int fd = ::open("aio_conf", O_CREAT | O_TRUNC | O_RDWR);
  ASSERT_TRUE(fd > 0);
  char buf[64];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, sizeof(buf), "%d,%d,%d,%d", trigger_fd, submit_fail, io_hang, io_timeout);
  ::write(fd, buf, strlen(buf));
  ::close(fd);
}

void TestDiskError::preapre_stress(const int64_t submit_thread_cnt)
{
  UNUSED(submit_thread_cnt);
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    snprintf(file_names_[i], 128, "./test_io_manager%ld", (i + 1));
  }

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    fds_[i] = ::open(file_names_[i], O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    ASSERT_TRUE(fds_[i] > 0);
    ftruncate(fds_[i], file_size_);
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().add_disk(fds_[i], ObDisk::DEFAULT_SYS_IO_PERCENT));
  }

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    stress_[i].init(fds_[i], file_size_, 3, 1, 0);
  }
}

void TestDiskError::start_stress()
{
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    stress_[i].start();
  }
}

void TestDiskError::stop_stress()
{
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    stress_[i].stop();
    stress_[i].wait();
  }
}

void TestDiskError::destroy_stress()
{
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().delete_disk(fds_[i]));
    ::close(fds_[i]);
    FileDirectoryUtils::delete_file(file_names_[i]);
  }
}

void TestDiskError::set_no_wait()
{
  stress_[0].set_no_wait();
}

void TestDiskError::set_io_size(const int64_t disk_index, const int64_t io_size)
{
  stress_[disk_index].set_user_io_size((int32_t)io_size);
}

TEST_F(TestDiskError, io_submit_failed)
{
  preapre_stress();
  prepare_conf(fds_[0], 1, 0, 0);
  sleep(10);
  start_stress();
  sleep(15);
  stop_stress();

  // check result
  ASSERT_TRUE(stress_[0].get_fail_count() > 0);
  // ASSERT_TRUE(stress_[0].get_succeed_count() == 0);
  ASSERT_TRUE(stress_[1].get_fail_count() == 0);
  ASSERT_TRUE(stress_[0].get_succeed_count() > 0);

  destroy_stress();
}

TEST_F(TestDiskError, io_hang_not_mark_error)
{
  ObIOConfig config = ObIOManager::get_instance().get_io_config();
  config.retry_warn_limit_ = 9;
  config.retry_error_limit_ = 18;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().set_io_config(config));

  preapre_stress();
  prepare_conf(fds_[0], 0, 1, 0);
  sleep(10);
  start_stress();
  sleep(30);
  stop_stress();

  // check result
  ASSERT_TRUE(stress_[0].get_fail_count() > 0);
  // ASSERT_TRUE(stress_[0].get_succeed_count() == 0);
  ASSERT_TRUE(stress_[1].get_fail_count() == 0);
  ASSERT_TRUE(stress_[0].get_succeed_count() > 0);

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    // not mark error disk delete disk will timeout if io hang
    ASSERT_EQ(0 == i ? OB_TIMEOUT : OB_SUCCESS, ObIOManager::get_instance().delete_disk(fds_[i]));
    ::close(fds_[i]);
    FileDirectoryUtils::delete_file(file_names_[i]);
  }
}

TEST_F(TestDiskError, io_hang_mark_error)
{
  ObIOConfig config = ObIOManager::get_instance().get_io_config();
  config.retry_warn_limit_ = 1;
  config.retry_error_limit_ = 2;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().set_io_config(config));

  preapre_stress();
  prepare_conf(fds_[0], 0, 1, 0);
  sleep(10);
  start_stress();
  sleep(30);
  stop_stress();

  // check result
  ASSERT_TRUE(stress_[0].get_fail_count() > 0);
  // ASSERT_TRUE(stress_[0].get_succeed_count() == 0);
  ASSERT_TRUE(stress_[1].get_fail_count() == 0);
  ASSERT_TRUE(stress_[0].get_succeed_count() > 0);

  for (int64_t i = 0; i < DISK_CNT; ++i) {
    // mark error disk delete disk will clean onging io request
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().delete_disk(fds_[i]));
    ::close(fds_[i]);
    FileDirectoryUtils::delete_file(file_names_[i]);
  }
}

TEST_F(TestDiskError, io_timeout)
{
  ObIOConfig config = ObIOManager::get_instance().get_io_config();
  config.retry_warn_limit_ = 1;
  config.retry_error_limit_ = 2;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().set_io_config(config));

  preapre_stress();
  prepare_conf(fds_[0], 0, 0, 1);
  sleep(10);
  start_stress();
  sleep(30);
  stop_stress();

  // check result
  ASSERT_TRUE(stress_[0].get_fail_count() > 0);
  // ASSERT_TRUE(stress_[0].get_succeed_count() == 0);
  ASSERT_TRUE(stress_[1].get_fail_count() == 0);
  ASSERT_TRUE(stress_[0].get_succeed_count() > 0);

  bool disk_error = false;
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.get_disk_manager().is_disk_error(fds_[0], disk_error));
  ASSERT_TRUE(disk_error);
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.is_disk_error(disk_error));
  ASSERT_TRUE(disk_error);
  ObArray<ObDiskFd> error_disks;
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.get_disk_manager().get_error_disks(error_disks));
  ASSERT_EQ(1, error_disks.count());
  ASSERT_EQ(fds_[0], error_disks.at(0));

  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.reset_disk_error());
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.is_disk_error(disk_error));
  ASSERT_FALSE(disk_error);

  COMMON_LOG(INFO, "start sleep");
  sleep(30);  // wait io finish
  COMMON_LOG(INFO, "end sleep");
  destroy_stress();
}

TEST_F(TestDiskError, io_hang2)
{
  ObIOConfig config = ObIOManager::get_instance().get_io_config();
  config.retry_warn_limit_ = 1;
  config.retry_error_limit_ = 2;
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().set_io_config(config));

  preapre_stress(2);
  prepare_conf(fds_[0], 0, 1, 0);
  sleep(10);
  set_no_wait();
  set_io_size(0, 2 * 1024 * 1024);
  start_stress();
  sleep(30);
  stop_stress();

  // check result
  ASSERT_TRUE(stress_[0].get_fail_count() > 0);
  // ASSERT_TRUE(stress_[0].get_succeed_count() == 0);
  ASSERT_TRUE(stress_[1].get_fail_count() == 0);
  ASSERT_TRUE(stress_[0].get_succeed_count() > 0);

  // set disk error
  {
    ObDiskGuard guard;
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.get_disk_manager().get_disk_with_guard(fds_[0], guard));
    guard.get_disk()->diagnose_.record_read_fail(10);
  }
  for (int64_t i = 0; i < DISK_CNT; ++i) {
    // delete disk will clean on going io request
    ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.delete_disk(fds_[i]));
    ::close(fds_[i]);
    FileDirectoryUtils::delete_file(file_names_[i]);
  }
  sleep(30);
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  // g++ -shared -o aio_preload.so aio_preload.c -fPIC
  // export LD_PRELOAD="./io/aio_preload.so"
  // return RUN_ALL_TESTS();
  return 0;
}
