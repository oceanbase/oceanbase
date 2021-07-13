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

#ifndef SRC_LIBRARY_SRC_LIB_IO_OB_IO_BENCHMARK_H_
#define SRC_LIBRARY_SRC_LIB_IO_OB_IO_BENCHMARK_H_

#include "ob_io_manager.h"
#include "lib/compress/ob_compressor.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_drw_lock.h"

namespace oceanbase {
namespace common {

class ObIOBenchmark;

enum ObDiskType {
  DISK_HDD,
  DISK_SSD,
  DISK_UNKNOWN,
};

struct ObIOWorkload {
  int64_t io_size_;
  ObIOMode mode_;   // read or write
  bool is_seq_io_;  // target addr is sequence or random
  inline bool is_valid() const
  {
    return io_size_ > 0 && mode_ >= ObIOMode::IO_MODE_READ && mode_ < ObIOMode::IO_MODE_MAX;
  }
  TO_STRING_KV(K_(mode), K_(io_size), K_(is_seq_io));
};

struct ObIOBenchResult {
  ObIOBenchResult() : iops_(0), rt_(0)
  {
    workload_.io_size_ = 0;
    workload_.mode_ = ObIOMode::IO_MODE_MAX;
    workload_.is_seq_io_ = false;
  }
  inline void reset()
  {
    iops_ = 0;
    rt_ = 0;
  }
  inline bool is_valid() const
  {
    return workload_.is_valid() && iops_ > 0 && rt_ > 0;
  }
  int to_string(char* buf, const int64_t length, int64_t& pos) const;
  ObIOWorkload workload_;
  double iops_;  // average io times per second
  double rt_;    // response time, unit us
  TO_STRING_KV(K_(workload), K_(iops), K_(rt));
};

class ObIOBenchResultSet {
public:
  ObIOBenchResultSet();
  virtual ~ObIOBenchResultSet();

  /*
   * get benchmark result of given size,
   * if not found the exact one, calculate it linearly
   */
  int get_rt(ObIOMode mode, const int64_t io_size, double& rt) const;
  int get_iops(ObIOMode mode, const int64_t io_size, double& iops) const;
  int add_result(const ObIOBenchResult& result);

  /*
   * export benchmark result to target file
   */
  int print_to_file(const char* conf_file);

  /*
   * import benchmark result from target file
   */
  int load_from_file(const char* conf_file);
  void reuse();

  /*
   * set how many threads to produce io request
   */
  void set_submit_thread_cnt(const int64_t submit_thread_cnt)
  {
    submit_thread_cnt_ = submit_thread_cnt;
  }

  /*
   * set how many threads to consume io request
   */
  void set_getevent_thread_cnt(const int64_t getevent_thread_cnt)
  {
    getevent_thread_cnt_ = getevent_thread_cnt;
  }
  int get_submit_thread_cnt(int64_t& submit_thread_cnt) const;
  int get_getevent_thread_cnt(int64_t& getevent_thread_cnt) const;
  void set_complete()
  {
    complete_ = true;
  }
  int64_t to_string(char* buf, const int64_t length) const;

private:
  /*
   * find index of benchmark result of given size
   */
  int find_result(ObIOMode mode, const int64_t io_size, int64_t& idx) const;
  static const int64_t DEFAULT_READ_SIZE_CNT = 8;   // number of different read size
  static const int64_t DEFAULT_WRITE_SIZE_CNT = 1;  // number of different write size
  static const int64_t IO_CONF_VERSION = 1;  // version of benchmark result file, determine how to read or write it.
  ObSEArray<ObIOBenchResult, DEFAULT_READ_SIZE_CNT> read_results_;
  ObSEArray<ObIOBenchResult, DEFAULT_WRITE_SIZE_CNT> write_results_;
  ObIArray<ObIOBenchResult>* results_[ObIOMode::IO_MODE_MAX];
  int64_t submit_thread_cnt_;
  int64_t getevent_thread_cnt_;
  bool complete_;  // indicate finished benchmark or not
};

class ObIORunner : lib::ThreadPool {
public:
  ObIORunner();
  virtual ~ObIORunner();
  int init(const ObDiskFd& fd, int64_t file_size, const int max_thread_cnt);
  virtual int run_test(const int thread_cnt, const ObIOWorkload& workload, ObIOBenchResult& result);
  void run1();

private:
  void destroy();
  void reuse();
  /*
   * execute io request in target thread
   */
  void issue_io_req(int64_t tid);
  void cal_result(ObIOBenchResult& result);
  DISALLOW_COPY_AND_ASSIGN(ObIORunner);

private:
  static const int64_t BENCH_TIME_US = 4 * 1000 * 1000;  // us
  static const int64_t CHUNK_ALIGN_SIZE = 2 * 1024 * 1024;
  static const int64_t WAIT_MS = 100;

private:
  bool is_inited_;
  bool start_work_;
  bool is_finish_;
  ObDiskFd fd_;
  int ret_;
  int32_t thread_cnt_;
  int32_t running_cnt_;
  int64_t file_size_;
  int64_t io_cnt_;
  int64_t total_time_;
  ObIOWorkload workload_;
  ObThreadCond work_cond_;
  ObThreadCond finish_cond_;
  pthread_barrier_t barrier_;
};

class ObDecompressTester {
public:
  ObDecompressTester();
  virtual ~ObDecompressTester();

  int init(const int64_t total_size, const int64_t block_size, const char* compressor_name);
  // throughtput calculated based on compressed data size
  int get_decompress_throughtput(int64_t& throughtput);

private:
  int prepare_data(const int64_t total_size, const int64_t block_size);
  int fill_buffer(char* buf, const int64_t buf_size, int64_t& data_size);
  bool inited_;
  ObCompressor* compressor_;
  ObArenaAllocator allocator_;
  ObArray<ObString> datas_;
  int64_t block_size_;
};

class ObIOBenchmark {
  struct ObBlockIOInfo {
    uint64_t rd_ios_;     /* Read I/O operations */
    uint64_t rd_merges_;  /* Reads merged */
    uint64_t rd_sectors_; /* Sectors read */
    uint64_t rd_ticks_;   /* Time in queue + service for read */
    uint64_t wr_ios_;     /* Write I/O operations */
    uint64_t wr_merges_;  /* Writes merged */
    uint64_t wr_sectors_; /* Sectors written */
    uint64_t wr_ticks_;   /* Time in queue + service for write */
    uint64_t ticks_;      /* Time of requests in queue */
    uint64_t aveq_;       /* Average queue length */
  };

public:
  static ObIOBenchmark& get_instance();
  /**
   * If data_dir is NULL, will only load conf from configure file.
   * If data_dir is NOT NULL, will run benchmark to generate configure file.
   */
  int init(const char* conf_dir, const char* data_dir = NULL, const int64_t file_size = DEFAULT_BENCHMARK_FILE_SIZE,
      const int32_t max_thread_cnt = DEFAULT_MAX_THREAD_CNT);
  void destroy();
  int get_min_rt(ObIOMode mode, const int64_t io_size, double& rt);
  int get_max_iops(ObIOMode mode, const int64_t io_size, double& iops);
  int get_submit_thread_cnt(int64_t& submit_thread_cnt);
  int get_getevent_thread_cnt(int64_t& get_getevent_thread_cnt);
  int reload_io_bench_res();
  int get_current_disk_bandwidth(uint64_t& bandwidth);
  int get_io_bench_result_str(char* buf, const int64_t length);
  int get_disk_type_str(char* buf, const int64_t length) const;
  ObDiskType get_disk_type() const
  {
    return disk_type_;
  }

private:
  ObIOBenchmark();
  virtual ~ObIOBenchmark();
  int benchmark(const char* data_dir, const int64_t file_size, const int32_t max_thread_cnt);
  int fill_file(ObIORunner& runner, const int32_t max_thread_cnt);
  int find_max_iops(
      ObIORunner& runner, const int start_thread_cnt, const ObIOWorkload& workload, int& res_thread_cnt, double& iops);
  int find_min_rt(ObIORunner& runner, const ObIOWorkload& workload, double& rt);
  int find_submit_thread_cnt(
      ObIORunner& runner, const ObDiskFd& fd, const int thread_cnt, const int64_t iops, int64_t& submit_thread_cnt);
  int find_getevent_thread_cnt(
      const int64_t throughtput, const int64_t submit_thread_cnt, int64_t& getevent_thread_cnt);
  int init_dev_info(const char* data_dir);
  int get_io_stat(const int32_t dev_major, const int32_t dev_minor, ObBlockIOInfo& io_info);
  int get_partition_name(const int32_t dev_major, char* partition_name);
  int get_sector_size(const int32_t dev_major);
  int cal_disk_type();
  DISALLOW_COPY_AND_ASSIGN(ObIOBenchmark);

private:
  static const int64_t DEFAULT_BENCHMARK_FILE_SIZE = 1024 * 1024 * 1024;
  static const int32_t START_THREAD_CNT = 2;
  static const int32_t FILL_FILE_THREAD_CNT = 4;
  static const int32_t DEFAULT_MAX_THREAD_CNT = 64;
  static const int32_t RESULT_SET_CNT = 2;
  static const int32_t MAX_BENCHMARK_FILE_PATH_LEN = 256;
  static const uint64_t DEFAULT_SECTOR_SIZE = 512;
  static constexpr ObIOWorkload WORKLOADS[] = {{4 * 1024, IO_MODE_READ, false},
      {8 * 1024, IO_MODE_READ, false},
      {16 * 1024, IO_MODE_READ, false},
      {32 * 1024, IO_MODE_READ, false},
      {64 * 1024, IO_MODE_READ, false},
      {128 * 1024, IO_MODE_READ, false},
      {256 * 1024, IO_MODE_READ, false},
      {512 * 1024, IO_MODE_READ, false},
      {2 * 1024 * 1024, IO_MODE_WRITE, false}};
  char conf_file_[MAX_BENCHMARK_FILE_PATH_LEN];
  char disk_type_file_[MAX_BENCHMARK_FILE_PATH_LEN];
  ObIOBenchResultSet result_set_[RESULT_SET_CNT];
  DRWLock result_set_lock_;
  int32_t result_set_idx_;
  int32_t dev_major_;
  int32_t dev_minor_;
  uint64_t sector_size_;
  ObDiskType disk_type_;
  bool is_inited_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* SRC_LIBRARY_SRC_LIB_IO_OB_IO_BENCHMARK_H_ */
