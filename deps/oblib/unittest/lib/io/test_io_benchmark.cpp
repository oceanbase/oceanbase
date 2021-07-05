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

#include <malloc.h>
#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <gtest/gtest.h>
#include <limits>

#include "lib/alloc/alloc_func.h"
#define private public
#include "lib/io/ob_io_benchmark.h"
#include "ob_io_stress.h"
#include "lib/file/file_directory_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

#ifdef USE_POSIX_FALLOCATE
#define FALLOCATE(fd, mode, off, len)       \
  ({                                        \
    int rc = posix_fallocate(fd, off, len); \
    if (0 != rc) {                          \
      errno = rc;                           \
    }                                       \
    rc;                                     \
  })
#else
#define FALLOCATE(fd, mode, off, len) fallocate(fd, mode, off, len)
#endif

namespace oceanbase {
namespace unittest {
class TestIOBenchmark : public ::testing::Test {
public:
  virtual void SetUp();
  virtual void TearDown();
};

void TestIOBenchmark::SetUp()
{
  system("rm -rf ./etc");
  system("mkdir -p ./etc");
}

void TestIOBenchmark::TearDown()
{
  system("rm -rf ./etc");
}

TEST_F(TestIOBenchmark, TestObIORunner)
{
  ObIORunner runner;
  ASSERT_EQ(OB_INVALID_ARGUMENT, runner.init(-1, 1 * 1024 * 1024 * 1024L, 64));
  ASSERT_EQ(OB_INVALID_ARGUMENT, runner.init(1, 1024 * 1024L, 64));
  ASSERT_EQ(OB_INVALID_ARGUMENT, runner.init(1, 0, 64));
  ASSERT_EQ(OB_INVALID_ARGUMENT, runner.init(1, 1 * 1024 * 1024 * 1024L, 0));

  // lib::set_memory_limit(1024*1024*1024L);
  ASSERT_EQ(OB_SUCCESS, runner.init(1, 1 * 1024 * 1024 * 1024L, 12));
  ASSERT_EQ(OB_INIT_TWICE, runner.init(1, 1 * 1024 * 1024 * 1024L, 64));
}

TEST_F(TestIOBenchmark, result_set)
{
  int ret = OB_SUCCESS;
  ObIOBenchResultSet result_set;
  ObIOBenchResult res;
  double iops = 0;
  double iort = 0;

  // invalid res
  ret = result_set.add_result(res);
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid load
  ret = result_set.load_from_file(NULL);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = result_set.load_from_file("./etc/invalid_file");
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid print
  ret = result_set.print_to_file(NULL);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = result_set.print_to_file("./etc/bench_cfg");
  ASSERT_NE(OB_SUCCESS, ret);

  // not init
  ret = result_set.get_iops(ObIOMode::IO_MODE_READ, 1024, iops);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = result_set.get_rt(ObIOMode::IO_MODE_WRITE, 1024, iort);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal res
  res.workload_.mode_ = ObIOMode::IO_MODE_READ;
  res.workload_.io_size_ = 16 * 1024;
  res.iops_ = ((double)(1024 * 1024 * 128)) / (double)res.workload_.io_size_;
  res.rt_ = ((double)res.workload_.io_size_) / (double)100;
  ret = result_set.add_result(res);
  ASSERT_EQ(OB_SUCCESS, ret);

  // invalid argument;
  ret = result_set.get_iops(ObIOMode::IO_MODE_MAX, 1024, iops);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = result_set.get_rt(ObIOMode::IO_MODE_MAX, 1024, iort);
  ASSERT_NE(OB_SUCCESS, ret);

  // smaller size
  res.workload_.mode_ = ObIOMode::IO_MODE_READ;
  res.workload_.io_size_ = 8 * 1024;
  res.iops_ = ((double)(1024 * 1024 * 128)) / (double)res.workload_.io_size_;
  res.rt_ = ((double)res.workload_.io_size_) / (double)100;
  ret = result_set.add_result(res);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal res
  res.workload_.mode_ = ObIOMode::IO_MODE_READ;
  res.workload_.io_size_ = 32 * 1024;
  res.iops_ = ((double)(1024 * 1024 * 128)) / (double)res.workload_.io_size_;
  res.rt_ = ((double)res.workload_.io_size_) / (double)100;
  ret = result_set.add_result(res);
  ASSERT_EQ(OB_SUCCESS, ret);
  result_set.set_complete();

  ret = result_set.print_to_file("./etc/bench_cfg");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = result_set.get_iops(ObIOMode::IO_MODE_READ, 16 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(((double)(1024 * 1024 * 128)) / (double)(16 * 1024), iops);
  ret = result_set.get_rt(ObIOMode::IO_MODE_READ, 32 * 1024, iort);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((double)(32 * 1024) / (double)100, iort);
  ret = result_set.get_iops(ObIOMode::IO_MODE_READ, 64 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((double)(1024 * 1024 * 128) / (double)(64 * 1024), iops);

  // load cfg from file and check again
  result_set.reuse();
  ret = result_set.load_from_file("./etc/bench_cfg");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = result_set.get_iops(ObIOMode::IO_MODE_READ, 16 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(((double)(1024 * 1024 * 128)) / (double)(16 * 1024), iops);
  ret = result_set.get_rt(ObIOMode::IO_MODE_READ, 32 * 1024, iort);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((double)(32 * 1024) / (double)100, iort);
  ret = result_set.get_iops(ObIOMode::IO_MODE_READ, 64 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((double)(1024 * 1024 * 128) / (double)(64 * 1024), iops);
}

TEST_F(TestIOBenchmark, benchmark)
{
  int ret = OB_SUCCESS;
  double iort = 0;
  double iops = 0;

  ObIOBenchmark& io_bench = ObIOBenchmark::get_instance();

  // invalid invoke when not init
  ret = io_bench.get_max_iops(ObIOMode::IO_MODE_READ, 1024, iops);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = io_bench.get_min_rt(ObIOMode::IO_MODE_READ, 1024, iort);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = io_bench.reload_io_bench_res();
  ASSERT_NE(OB_SUCCESS, ret);

  // invalid init
  ret = io_bench.init(NULL);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = io_bench.init("./etc");
  ASSERT_NE(OB_SUCCESS, ret);

  // normal init, first run
  ret = io_bench.init("./etc", "./", 128 * 1024 * 1024L, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = io_bench.init("./etc", "./", 128 * 1024 * 1024L, 4);
  ASSERT_NE(OB_SUCCESS, ret);

  // second run, load result from file
  io_bench.destroy();
  int64_t start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, io_bench.init("./etc"));
  ASSERT_TRUE(ObTimeUtility::current_time() - start_time < 1000000);

  // get value
  ret = io_bench.get_max_iops(ObIOMode::IO_MODE_READ, 16 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(iops > 0);
  ret = io_bench.get_min_rt(ObIOMode::IO_MODE_WRITE, 2 * 1024 * 1024, iort);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(iort > 0);

  // reload and reget
  ret = io_bench.reload_io_bench_res();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = io_bench.get_max_iops(ObIOMode::IO_MODE_READ, 16 * 1024, iops);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(iops > 0);
  ret = io_bench.get_min_rt(ObIOMode::IO_MODE_WRITE, 2 * 1024 * 1024, iort);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(iort > 0);

  char buf[common::OB_MAX_IO_BENCH_RESULT_LENGTH];
  ASSERT_EQ(ret, io_bench.get_io_bench_result_str(buf, sizeof(buf)));
  COMMON_LOG(INFO, "io bench result = ", K(buf));

  io_bench.destroy();
  ASSERT_EQ(OB_SUCCESS, io_bench.init("./etc"));
  ASSERT_TRUE(ObTimeUtility::current_time() - start_time < 1000000);
}

TEST_F(TestIOBenchmark, fill_file)
{
  int ret = OB_SUCCESS;
  int fd = 0;
  const int64_t MAX_BENCHMARK_FILE_PATH_LEN = 128;
  const char* data_dir = "./";
  const int64_t file_size = 128 << 20;
  int32_t blk_size = 2 << 20;
  const int64_t blk_cnt = file_size / blk_size;
  char data_file_path[MAX_BENCHMARK_FILE_PATH_LEN + 1];
  MEMSET(data_file_path, 0, sizeof(data_file_path));
  ObIORunner io_runner;
  ObIOBenchmark& io_bench = ObIOBenchmark::get_instance();
  int32_t thread_cnt = 4;

  int n = snprintf(data_file_path, MAX_BENCHMARK_FILE_PATH_LEN, "%s/bench_file", data_dir);
  if (n <= 0 || n >= MAX_BENCHMARK_FILE_PATH_LEN) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The data dir is too long, ", K(data_dir), K(ret));
  } else if ((fd = ::open(
                  data_file_path, O_CREAT | O_TRUNC | O_DIRECT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "open file error", K(data_file_path), K(errno), KERRMSG, K(ret));
  } else if (OB_FAIL(FALLOCATE(fd, 0 /*MODE*/, 0 /*offset*/, file_size))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "allocate file error", K(data_file_path), K(file_size), K(errno), KERRMSG, K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_disk(fd))) {
    COMMON_LOG(WARN, "add disk failed", K(ret), K(fd));
  } else if (OB_FAIL(io_runner.init(fd, file_size, thread_cnt))) {
    COMMON_LOG(WARN, "failed to init io runner");
  } else {
    ret = io_bench.fill_file(io_runner, thread_cnt);
    ASSERT_EQ(OB_SUCCESS, ret);

    void* base = memalign(512, blk_size);
    memset(base, 'a', blk_size);
    void* buf = memalign(512, blk_size);
    fsync(fd);
    for (int i = 0; i < blk_cnt; ++i) {
      COMMON_LOG(INFO, "iteration ", K(i));
      if (0 > pread(fd, buf, blk_size, blk_size * i)) {
        COMMON_LOG(ERROR, "preaderror", KERRMSG);
      }
      ASSERT_EQ(0, memcmp(base, buf, blk_size));
    }
  }
  io_bench.destroy();
}

TEST(TestIOStress, mystress)
{
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);

  // prepare file
  const char* file_name = "./io_stress_file";
  const int64_t file_size = 1 * 1024 * 1024 * 1024L;
  int fd = ::open(file_name, O_CREAT | O_TRUNC | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  ASSERT_TRUE(fd > 0);
  ASSERT_EQ(0, ftruncate(fd, file_size));

  // init io manager
  ObIOManager::get_instance().destroy();
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.add_disk(fd));

  // init stress
  TestIOStress stress;
  ASSERT_EQ(OB_SUCCESS, stress.init(fd, file_size, 16, 0, 0));
  stress.set_user_iops(50 * 10000);

  COMMON_LOG(INFO, "start stress", K(stress));
  stress.start();
  for (int i = 0; i < 5; ++i) {
    int64_t round_begin = ObTimeUtility::current_time();
    int64_t succ_cnt_old = stress.get_succeed_count();
    sleep(5);
    int64_t succ_cnt_new = stress.get_succeed_count();
    int64_t round_end = ObTimeUtility::current_time();
    int64_t time = round_end - round_begin;
    int64_t succ_cnt = succ_cnt_new - succ_cnt_old;
    int64_t iops = succ_cnt * 1000L * 1000L / time;
    COMMON_LOG(INFO, "stress audit", K(i), K(time), K(succ_cnt), K(iops));
  }
  stress.stop();
  stress.wait();

  // destroy io manager and close file
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().delete_disk(fd));
  ::close(fd);
  FileDirectoryUtils::delete_file(file_name);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_max_file_size(256 * 1024 * 1024);
  system("rm -f test_io_benchmark.log*");
  OB_LOGGER.set_file_name("test_io_benchmark.log");
  OB_LOGGER.set_log_level("INFO");
  set_memory_limit(40L * 1024L * 1024L * 1024L);  // 40GB
  ObIOManager::get_instance().init();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
