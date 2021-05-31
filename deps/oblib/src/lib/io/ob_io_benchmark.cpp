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

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include "ob_io_benchmark.h"
#include "lib/random/ob_random.h"
#include "lib/file/file_directory_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/utility.h"

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
namespace common {

int ObIOBenchResult::to_string(char* buf, const int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const char* workload_name = "READ";
  if (workload_.mode_ == ObIOMode::IO_MODE_WRITE) {
    workload_name = "WRITE";
  }
  ret = databuff_printf(
      buf, length, pos, "\n%s %ldK : %.0lf %.1lf", workload_name, workload_.io_size_ / 1024, iops_, rt_);
  return ret;
}

ObIOBenchResultSet::ObIOBenchResultSet()
{
  results_[ObIOMode::IO_MODE_READ] = &read_results_;
  results_[ObIOMode::IO_MODE_WRITE] = &write_results_;
  submit_thread_cnt_ = 0;
  getevent_thread_cnt_ = 0;
  complete_ = false;
}

ObIOBenchResultSet::~ObIOBenchResultSet()
{}

int ObIOBenchResultSet::get_rt(ObIOMode mode, const int64_t io_size, double& rt) const
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (!complete_) {
    ret = OB_INNER_STAT_ERROR;
    COMMON_LOG(WARN, "resultset not complete, can not access", K(ret));
  } else if (OB_FAIL(find_result(mode, io_size, idx))) {
    COMMON_LOG(WARN, "Fail to find result, ", K(ret));
  } else if (idx < 0 || idx >= results_[mode]->count()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unexpected idx, ", K(ret), K(idx));
  } else {
    ObIArray<ObIOBenchResult>* results = results_[mode];
    if (0 == idx) {
      rt = results->at(0).rt_;
    } else {
      if (io_size == results->at(idx).workload_.io_size_) {
        rt = results->at(idx).rt_;
      } else {
        double value1 = results->at(idx - 1).rt_;
        double value2 = results->at(idx).rt_;
        int64_t size1 = results->at(idx - 1).workload_.io_size_;
        int64_t size2 = results->at(idx).workload_.io_size_;
        if (size2 - size1 <= 0) {
          ret = OB_INVALID_DATA;
          COMMON_LOG(WARN, "Invalid data, ", K(ret), K(size1), K(size2));
        } else {
          double slope = (value2 - value1) / (double)(size2 - size1);
          rt = ((double)(io_size - size1)) * slope + value1;
        }
      }
    }
  }
  return ret;
}

int ObIOBenchResultSet::get_iops(ObIOMode mode, const int64_t io_size, double& iops) const
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (!complete_) {
    ret = OB_INNER_STAT_ERROR;
    COMMON_LOG(WARN, "resultset not complete, can not access", K(ret));
  } else if (OB_FAIL(find_result(mode, io_size, idx))) {
    COMMON_LOG(WARN, "Fail to find result, ", K(ret));
  } else if (idx < 0 || idx >= results_[mode]->count()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unexpected idx, ", K(ret), K(idx));
  } else {
    ObIArray<ObIOBenchResult>* results = results_[mode];
    if (0 == idx) {
      iops = results->at(0).iops_;
    } else {
      if (io_size == results->at(idx).workload_.io_size_) {
        iops = results->at(idx).iops_;
      } else {
        double value1 = results->at(idx - 1).iops_;
        double value2 = results->at(idx).iops_;
        int64_t size1 = results->at(idx - 1).workload_.io_size_;
        int64_t size2 = results->at(idx).workload_.io_size_;
        iops = (value1 * (double)size1 + value2 * (double)size2) / 2.0 / (double)io_size;
      }
    }
  }
  return ret;
}

int ObIOBenchResultSet::find_result(ObIOMode mode, const int64_t io_size, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IO_MODE_READ != mode && IO_MODE_WRITE != mode) || OB_UNLIKELY(io_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invaid argument, ", K(ret), K(mode), K(io_size));
  } else if (results_[mode]->count() <= 0) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The results is empty, ", K(ret));
  } else {
    ObIArray<ObIOBenchResult>* results = results_[mode];
    for (idx = 0; idx < results->count(); ++idx) {
      if (io_size <= results->at(idx).workload_.io_size_) {
        break;
      }
    }
    if (idx == results->count()) {
      idx = results->count() - 1;
    }
  }

  return ret;
}

int ObIOBenchResultSet::add_result(const ObIOBenchResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!result.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid io mode", K(ret), K(result));
  } else {
    ObIArray<ObIOBenchResult>* results = results_[result.workload_.mode_];

    if (results->count() > 0 && results->at(results->count() - 1).workload_.io_size_ >= result.workload_.io_size_) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "the io bench result size must be monotone increasing, ", K(ret), K(result));
    } else if (OB_FAIL(results_[result.workload_.mode_]->push_back(result))) {
      COMMON_LOG(WARN, "failed to push back result", K(ret), K(result));
    }
  }
  return ret;
}

int ObIOBenchResultSet::print_to_file(const char* conf_file)
{
  int ret = OB_SUCCESS;
  FILE* file = NULL;

  if (!complete_) {
    ret = OB_INNER_STAT_ERROR;
    COMMON_LOG(WARN, "resultset not complete, can not access it", K(ret));
  } else if (OB_UNLIKELY(NULL == conf_file)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(ret), KP(conf_file));
  } else if (OB_ISNULL(file = fopen(conf_file, "w+"))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "failed to open result file", K(ret), K(errno), KERRMSG);
  } else {
    const int64_t res_cnt = read_results_.count() + write_results_.count();
    if (0 == res_cnt) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WARN, "There is no result in result set, ", K(ret));
    } else {
      fprintf(file, "version %ld\n", IO_CONF_VERSION);
      fprintf(file, "%-10s %-15s %-15s %-10s\n", "io_type", "io_size_byte", "io_ps", "io_rt_us");
      for (int64_t i = 0; OB_SUCC(ret) && i < read_results_.count(); ++i) {
        const ObIOBenchResult& res = read_results_.at(i);
        fprintf(
            file, "%-10d %-15ld %-15.2lf %-10.2lf\n", res.workload_.mode_, res.workload_.io_size_, res.iops_, res.rt_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < write_results_.count(); ++i) {
        const ObIOBenchResult& res = write_results_.at(i);
        fprintf(
            file, "%-10d %-15ld %-15.2lf %-10.2lf\n", res.workload_.mode_, res.workload_.io_size_, res.iops_, res.rt_);
      }
      fprintf(file, "submit_thread_cnt %ld\n", submit_thread_cnt_);
      fprintf(file, "getevent_thread_cnt %ld\n", getevent_thread_cnt_);
    }

    if (0 != fclose(file)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "failed to close file", K(ret), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObIOBenchResultSet::load_from_file(const char* conf_file)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  FILE* file = NULL;
  ObIOBenchResult result;
  int fscanf_ret = -1;
  const int scan_item_cnt = 4;
  const int32_t head_line_size = 256;
  char head_line[head_line_size];
  int64_t io_conf_version = -1;
  reuse();
  if (OB_UNLIKELY(NULL == conf_file)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(ret), KP(conf_file));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(conf_file, exists))) {
    COMMON_LOG(WARN, "failed to check file exists", K(ret));
  } else if (!exists) {
    ret = OB_FILE_NOT_EXIST;
    COMMON_LOG(WARN, "the io bench result file not exist!");
  } else {
    if (NULL == (file = ::fopen(conf_file, "r"))) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "failed to open iops data file, ", K(ret), K(errno), KERRMSG);
    } else if (1 != (fscanf_ret = fscanf(file, "version %ld\n", &io_conf_version))) {
      // normal case, oldest version don't contain this parameter
      io_conf_version = 0;
    }

    if (OB_FAIL(ret)) {
    } else if (NULL == fgets(head_line, head_line_size, file)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to parse head line, ", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        int mode = -1;
        if (scan_item_cnt !=
            (fscanf_ret =
                    fscanf(file, "%d%ld%lf%lf\n", &mode, &result.workload_.io_size_, &result.iops_, &result.rt_))) {
          if (EOF == fscanf_ret) {
            break;
          } else if (io_conf_version <= 0) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "parse result failed", K(ret), K(io_conf_version), K(fscanf_ret));
          } else if (io_conf_version > 0) {
            // normal case in new version
            break;
          }
        } else {
          result.workload_.mode_ = mode == ObIOMode::IO_MODE_READ ? ObIOMode::IO_MODE_READ : ObIOMode::IO_MODE_WRITE;
          if (OB_FAIL(add_result(result))) {
            COMMON_LOG(WARN, "failed to add result", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (io_conf_version <= 0) {
          submit_thread_cnt_ = ObIOResourceManager::MAX_CHANNEL_CNT;
          getevent_thread_cnt_ = submit_thread_cnt_;
        } else if (IO_CONF_VERSION == io_conf_version) {
          if (1 != (fscanf_ret = fscanf(file, "submit_thread_cnt %ld\n", &submit_thread_cnt_))) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "failed to parse submit_thread_cnt", K(ret), K(fscanf_ret));
          } else if (1 != (fscanf_ret = fscanf(file, "getevent_thread_cnt %ld\n", &getevent_thread_cnt_))) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "failed to parse getevent_thread_cnt", K(ret), K(fscanf_ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "not supported version", K(ret), K(io_conf_version));
        }
      }
    }
    if (NULL != file) {
      fclose(file);
    }
  }
  if (OB_SUCC(ret)) {
    set_complete();
  }

  return ret;
}

int ObIOBenchResultSet::get_submit_thread_cnt(int64_t& submit_thread_cnt) const
{
  int ret = OB_SUCCESS;
  if (!complete_) {
    ret = OB_INNER_STAT_ERROR;
    COMMON_LOG(WARN, "resultset not complete, can not access it", K(ret));
  } else {
    submit_thread_cnt = submit_thread_cnt_;
  }
  return ret;
}

int ObIOBenchResultSet::get_getevent_thread_cnt(int64_t& getevent_thread_cnt) const
{
  int ret = OB_SUCCESS;
  if (!complete_) {
    ret = OB_INNER_STAT_ERROR;
    COMMON_LOG(WARN, "resultset not complete, can not access it", K(ret));
  } else {
    getevent_thread_cnt = getevent_thread_cnt_;
  }
  return ret;
}

void ObIOBenchResultSet::reuse()
{
  read_results_.reuse();
  write_results_.reuse();
  submit_thread_cnt_ = 0;
  getevent_thread_cnt_ = 0;
  complete_ = false;
}

int64_t ObIOBenchResultSet::to_string(char* buf, const int64_t length) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < read_results_.count(); ++i) {
    if (OB_FAIL(read_results_.at(i).to_string(buf, length, pos))) {
      COMMON_LOG(INFO, "failed to covert io bench result to string", K(length), K(pos), K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < write_results_.count(); ++i) {
    if (OB_FAIL(write_results_.at(i).to_string(buf, length, pos))) {
      COMMON_LOG(INFO, "failed to covert io bench result to string", K(length), K(pos), K(ret));
    }
  }
  return pos;
}

//----------------------------------------ObIORunner------------------------------------

ObIORunner::ObIORunner()
    : is_inited_(false),
      start_work_(false),
      is_finish_(false),
      fd_(),
      ret_(OB_SUCCESS),
      thread_cnt_(0),
      running_cnt_(0),
      file_size_(0),
      io_cnt_(0),
      total_time_(0)
{
  workload_.mode_ = IO_MODE_MAX;
  workload_.io_size_ = 0;
  workload_.is_seq_io_ = false;
}

ObIORunner::~ObIORunner()
{
  destroy();
}

int ObIORunner::init(const ObDiskFd& fd, int64_t file_size, const int max_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "io runner init twice", K(ret));
  } else if (!fd.is_valid() || file_size <= 0 || file_size % CHUNK_ALIGN_SIZE != 0 ||
             file_size < max_thread_cnt * CHUNK_ALIGN_SIZE || max_thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(fd), K(file_size), K(max_thread_cnt));
  } else if (OB_FAIL(pthread_barrier_init(&barrier_, NULL, max_thread_cnt))) {
    COMMON_LOG(WARN, "failed to init barrier", K(ret));
  } else if (OB_FAIL(work_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init work cond", K(ret));
  } else if (OB_FAIL(finish_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    COMMON_LOG(WARN, "failed to init finish cond", K(ret));
  } else {
    set_thread_count(max_thread_cnt);
    if (OB_FAIL(start())) {
      COMMON_LOG(WARN, "failed to start thread", K(ret), K(max_thread_cnt));
    } else {
      fd_ = fd;
      file_size_ = file_size;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObIORunner::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    finish_cond_.destroy();
    work_cond_.destroy();
    pthread_barrier_destroy(&barrier_);
    start_work_ = false;
    is_finish_ = false;
    fd_.reset();
    thread_cnt_ = 0;
    running_cnt_ = 0;
    ret_ = OB_SUCCESS;
    file_size_ = 0;
    io_cnt_ = 0;
    total_time_ = 0;
    is_inited_ = false;
  }
}

void ObIORunner::reuse()
{
  ret_ = OB_SUCCESS;
  running_cnt_ = 0;
  io_cnt_ = 0;
  total_time_ = 0;
}

void ObIORunner::cal_result(ObIOBenchResult& result)
{
  result.workload_ = workload_;
  result.iops_ = (double)io_cnt_ / (double)(BENCH_TIME_US / 1000000);
  result.rt_ = (double)total_time_ / (double)io_cnt_;
}

int ObIORunner::run_test(const int thread_cnt, const ObIOWorkload& workload, ObIOBenchResult& result)
{
  int ret = OB_SUCCESS;
  if (thread_cnt <= 0 || thread_cnt > get_thread_count()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "thread_cnt is too large", K(thread_cnt), K(get_thread_count()));
  } else {
    reuse();
    workload_ = workload;
    thread_cnt_ = thread_cnt;
    {
      ObThreadCondGuard guard(work_cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "failed to lock work_cond_", K(ret));
      } else {
        start_work_ = true;
        work_cond_.broadcast();
      }
    }
    {
      ObThreadCondGuard guard(finish_cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "failed to lock finish_cond_", K(ret));
      } else {
        while (!is_finish_) {
          finish_cond_.wait();
        }
      }
      cal_result(result);
      is_finish_ = false;
      COMMON_LOG(INFO, "Run io benchmark: ", K(result), K(thread_cnt));
    }
    ret = ret_;
  }
  return ret;
}

void ObIORunner::issue_io_req(int64_t tid)
{
  int ret = OB_SUCCESS;

  if (tid < thread_cnt_) {
    char* w_buffer = NULL;
    ObIOInfo io_info;
    ObIOPoint& io_point = io_info.io_points_[0];
    ObIOHandle io_handle;
    if (ObIOMode::IO_MODE_WRITE == workload_.mode_) {
      ObMemAttr mem_attr(OB_SYS_TENANT_ID, ObModIds::TEST);
      if (OB_ISNULL(w_buffer = (char*)ob_malloc_align(DIO_ALIGN_SIZE, workload_.io_size_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        MEMSET(w_buffer, 'a', workload_.io_size_);
      }
    }

    int64_t size_per_thread = file_size_ / thread_cnt_;
    size_per_thread = lower_align(size_per_thread, CHUNK_ALIGN_SIZE);
    const int64_t offset = tid * size_per_thread;
    int64_t test_start_time = ObTimeUtility::current_time();
    int64_t rq_start_time = 0;
    int64_t block_size = workload_.io_size_;
    int64_t block_per_thread = size_per_thread / block_size;
    int64_t loop_times = 0;
    io_point.fd_ = fd_;
    io_point.size_ = static_cast<int32_t>(block_size);
    io_info.size_ = io_point.size_;
    io_info.io_desc_.category_ = USER_IO;
    io_info.batch_count_ = 1;

    while (true) {
      io_handle.reset();
      if (workload_.is_seq_io_) {
        io_point.offset_ = loop_times * block_size + offset;
      } else {
        io_point.offset_ = ObRandom::rand(0, block_per_thread - 1) * block_size + offset;
      }
      io_point.write_buf_ = w_buffer;
      rq_start_time = ObTimeUtility::current_time();
      io_info.io_desc_.req_deadline_time_ = rq_start_time;
      io_info.io_desc_.mode_ = workload_.mode_;
      if (ObIOMode::IO_MODE_READ == workload_.mode_) {
        if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
          if (OB_SIZE_OVERFLOW != ret && OB_ENTRY_NOT_EXIST != ret) {
            ret_ = ret;
          }
          COMMON_LOG(WARN, "failed to submit read aio request", K(ret));
        }
      } else {
        if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
          if (OB_SIZE_OVERFLOW != ret && OB_ENTRY_NOT_EXIST != ret) {
            ret_ = ret;
          }
          COMMON_LOG(WARN, "failed to submit write aio request", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
          COMMON_LOG(WARN, "failed to wait io", K(ret));
        } else {
          const int64_t rt = ObTimeUtility::current_time() - rq_start_time;
          ATOMIC_INC(&io_cnt_);
          ATOMIC_FAA(&total_time_, rt);
          ++loop_times;
        }
      }
      if (workload_.is_seq_io_) {
        if (loop_times == block_per_thread) {
          COMMON_LOG(INFO, "finish seq io, ", K(tid), K(loop_times), K(block_per_thread));
          break;
        }
      } else if (ObTimeUtility::current_time() - test_start_time >= BENCH_TIME_US) {
        break;
      }
    }

    if (NULL != w_buffer) {
      ob_free_align(w_buffer);
      w_buffer = NULL;
    }
  }
}

void ObIORunner::run1()
{
  int ret = OB_SUCCESS;
  const int64_t tid = get_thread_idx();
  while (true) {
    {
      ObThreadCondGuard guard(work_cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "failed to lock work_cond_", K(ret));
      } else {
        while (!has_set_stop() && !start_work_) {
          work_cond_.wait(WAIT_MS);
        }
        if (has_set_stop()) {
          break;
        }
      }
    }
    pthread_barrier_wait(&barrier_);
    if (0 == tid) {
      start_work_ = false;
    }
    if (!has_set_stop() && tid < thread_cnt_) {
      issue_io_req(tid);
    }
    pthread_barrier_wait(&barrier_);
    if (0 == tid) {
      ObThreadCondGuard guard(finish_cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(WARN, "failed to lock finish cond", K(ret));
      } else {
        is_finish_ = true;
        finish_cond_.signal();
      }
    }
  }
}

ObDecompressTester::ObDecompressTester() : inited_(false), compressor_(NULL), allocator_(), datas_(), block_size_(0)
{}

ObDecompressTester::~ObDecompressTester()
{}

int ObDecompressTester::init(const int64_t total_size, const int64_t block_size, const char* compressor_name)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_name, compressor_))) {
    COMMON_LOG(WARN, "get_compressor failed", K(ret), K(compressor_name));
  } else if (OB_FAIL(prepare_data(total_size, block_size))) {
    COMMON_LOG(WARN, "prepare_data failed", K(ret), K(total_size), K(block_size));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObDecompressTester::get_decompress_throughtput(int64_t& throughtput)
{
  int ret = OB_SUCCESS;
  char* uncomp_buf = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (NULL == (uncomp_buf = static_cast<char*>(allocator_.alloc(block_size_ * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "alloc memory failed", K(ret), K_(block_size));
  } else {
    const int64_t start = ObTimeUtility::current_time();
    int64_t process_size = 0;
    int64_t uncomp_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < datas_.count(); ++i) {
      ObString& data = datas_.at(i);
      if (OB_FAIL(compressor_->decompress(data.ptr(), data.length(), uncomp_buf, block_size_ * 2, uncomp_size))) {
        COMMON_LOG(WARN, "decompress failed", K(ret), K(data));
      } else {
        process_size += data.length();
      }
    }
    const int64_t cost = ObTimeUtility::current_time() - start;
    const double cost_sec = static_cast<double>(cost) / 1000 / 1000;
    throughtput = static_cast<int64_t>(static_cast<double>(process_size) / cost_sec);
  }
  return ret;
}

int ObDecompressTester::prepare_data(const int64_t total_size, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  char* comp_buf = NULL;
  int64_t max_overflow_size = 0;
  if (total_size <= 0 || block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(total_size), K(block_size));
  } else if (NULL == (buf = static_cast<char*>(allocator_.alloc(block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "alloc memory failed", K(ret), K(block_size));
  } else if (OB_FAIL(compressor_->get_max_overflow_size(block_size, max_overflow_size))) {
    COMMON_LOG(WARN, "get_max_overflow_size failed", K(ret), K(block_size));
  } else {
    const int64_t comp_buf_size = block_size + max_overflow_size;
    const int64_t block_count = total_size / block_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      int64_t data_size = 0;
      int64_t comp_size = 0;
      if (OB_FAIL(fill_buffer(buf, block_size, data_size))) {
        COMMON_LOG(WARN, "fill_buffer failed", K(ret));
      } else if (NULL == (comp_buf = static_cast<char*>(allocator_.alloc(comp_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "alloc memory failed", K(ret), K(comp_buf_size));
      } else if (OB_FAIL(compressor_->compress(buf, data_size, comp_buf, comp_buf_size, comp_size))) {
        COMMON_LOG(WARN, "compress failed", K(ret), K(block_size), K(comp_buf_size));
      } else {
        ObString str(comp_size, comp_buf);
        if (OB_FAIL(datas_.push_back(str))) {
          COMMON_LOG(WARN, "push_back failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      block_size_ = block_size;
    }
  }
  return ret;
}

int ObDecompressTester::fill_buffer(char* buf, const int64_t buf_size, int64_t& data_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char* citys[4] = {"shenzhen", "hangzhou", "shanghai", "guangzhou"};
  while (OB_SUCC(ret)) {
    int64_t user_id = ObRandom::rand(0, 1000000);
    const char* city = citys[user_id % 4];
    int64_t age = ObRandom::rand(0, 100);
    int n = snprintf(buf + pos, buf_size - pos, "%ldtb_%ld%ld%s", user_id, user_id, age, city);
    if (n < 0) {
      ret = OB_ERROR;
      COMMON_LOG(WARN, "snprintf failed", K(ret));
    } else if (n >= buf_size - pos) {
      break;
    } else {
      pos += n;
    }
  }
  if (OB_SUCC(ret)) {
    data_size = pos;
  }
  return ret;
}

/*****************ObIOBenchmark******************************/
constexpr ObIOWorkload ObIOBenchmark::WORKLOADS[];

ObIOBenchmark::ObIOBenchmark()
    : result_set_idx_(0),
      dev_major_(-1),
      dev_minor_(-1),
      sector_size_(DEFAULT_SECTOR_SIZE),
      disk_type_(ObDiskType::DISK_UNKNOWN),
      is_inited_(false)
{
  MEMSET(conf_file_, 0, sizeof(conf_file_));
  MEMSET(disk_type_file_, 0, sizeof(disk_type_file_));
}

ObIOBenchmark::~ObIOBenchmark()
{
  destroy();
}

void ObIOBenchmark::destroy()
{
  if (is_inited_) {
    result_set_idx_ = 0;
    dev_major_ = -1;
    dev_minor_ = -1;
    disk_type_ = ObDiskType::DISK_UNKNOWN;
    is_inited_ = false;
  }
}

ObIOBenchmark& ObIOBenchmark::get_instance()
{
  static ObIOBenchmark instance;
  return instance;
}

int ObIOBenchmark::init(
    const char* conf_dir, const char* data_dir, const int64_t file_size, const int32_t max_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "io benchmark has been inited", K(ret));
  } else if (OB_UNLIKELY(NULL == conf_dir) || OB_UNLIKELY(file_size <= 0) || OB_UNLIKELY(max_thread_cnt <= 0) ||
             OB_UNLIKELY(max_thread_cnt > DEFAULT_MAX_THREAD_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(
        WARN, "invalid argument for io benchmark", K(ret), K(file_size), K(max_thread_cnt), KP(conf_dir), KP(data_dir));
  } else {
    int n1 = snprintf(conf_file_, MAX_BENCHMARK_FILE_PATH_LEN, "%s/io_resource.conf", conf_dir);
    int n2 = snprintf(disk_type_file_, MAX_BENCHMARK_FILE_PATH_LEN, "%s/disk_type", conf_dir);
    if (n1 <= 0 || n1 >= MAX_BENCHMARK_FILE_PATH_LEN || n2 <= 0 || n2 >= MAX_BENCHMARK_FILE_PATH_LEN) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "The conf file name is too long, ", K(conf_dir), K(ret));
    } else {
      if (OB_FAIL(result_set_[result_set_idx_].load_from_file(conf_file_))) {
        if (OB_FILE_NOT_EXIST != ret || NULL == data_dir) {
          COMMON_LOG(WARN, "Fail to load io benchmark result, ", K(ret));
        } else {
          if (OB_FAIL(benchmark(data_dir, file_size, max_thread_cnt))) {
            COMMON_LOG(WARN, "fail to do io benchmark", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(cal_disk_type())) {
      COMMON_LOG(WARN, "failed to cal disk type", K(ret));
    } else {
      is_inited_ = true;
      COMMON_LOG(INFO, "Success to load io benchmark result!");
    }
  }
  return ret;
}

int ObIOBenchmark::fill_file(ObIORunner& runner, const int32_t thread_cnt)
{
  int ret = OB_SUCCESS;
  ObIOWorkload workload = {2L << 20, ObIOMode::IO_MODE_WRITE, true};
  ObIOBenchResult result;
  if (OB_FAIL(runner.run_test(thread_cnt, workload, result))) {
    COMMON_LOG(WARN, "failed to fill file", K(ret));
  }
  return ret;
}

int ObIOBenchmark::find_max_iops(
    ObIORunner& runner, const int start_thread_cnt, const ObIOWorkload& workload, int& res_thread_cnt, double& iops)
{
  int ret = OB_SUCCESS;
  int cur_thread_cnt = start_thread_cnt;
  bool is_found = false;
  double last_iops = 0.0;
  ObIOBenchResult cur_result;
  while (!is_found && OB_SUCC(ret)) {
    if (OB_FAIL(runner.run_test(cur_thread_cnt, workload, cur_result))) {
      COMMON_LOG(WARN, "failed to run test", K(ret), K(cur_thread_cnt));
    } else {
      if (last_iops > cur_result.iops_) {
        is_found = true;
        iops = last_iops;
        res_thread_cnt = cur_thread_cnt * 2;
      } else if (1 == cur_thread_cnt) {
        is_found = true;
        iops = cur_result.iops_;
        res_thread_cnt = cur_thread_cnt;
      } else {
        cur_thread_cnt /= 2;
        last_iops = cur_result.iops_;
      }
    }
  }
  return ret;
}

int ObIOBenchmark::find_min_rt(ObIORunner& runner, const ObIOWorkload& workload, double& rt)
{
  int ret = OB_SUCCESS;
  ObIOBenchResult cur_result;
  if (OB_FAIL(runner.run_test(1, workload, cur_result))) {
    COMMON_LOG(WARN, "failed to run test", K(ret), K(workload));
  } else {
    rt = cur_result.rt_;
  }
  return ret;
}

int ObIOBenchmark::find_submit_thread_cnt(
    ObIORunner& runner, const ObDiskFd& fd, const int thread_cnt, const int64_t iops, int64_t& submit_thread_cnt)
{
  int ret = OB_SUCCESS;
  submit_thread_cnt = 1;
  const int64_t max_submit_thread_cnt = ObIOResourceManager::MAX_CHANNEL_CNT;
  const ObIOWorkload& workload = WORKLOADS[0];
  bool found = false;
  ObIOBenchResult cur_result;
  while (OB_SUCC(ret) && !found && submit_thread_cnt < max_submit_thread_cnt) {
    if (OB_FAIL(ObIOManager::get_instance().delete_disk(fd))) {
      COMMON_LOG(WARN, "delete_disk failed", K(ret), K(fd));
    } else if (OB_FAIL(ObIOManager::get_instance().add_disk(fd, ObDisk::DEFAULT_SYS_IO_PERCENT))) {
      COMMON_LOG(WARN, "add_disk failed", K(ret), K(fd), K(submit_thread_cnt));
    } else if (OB_FAIL(runner.run_test(thread_cnt, workload, cur_result))) {
      COMMON_LOG(WARN, "failed to run test", K(ret), K(thread_cnt));
    } else {
      if (cur_result.iops_ >= iops) {
        found = true;
        COMMON_LOG(INFO, "submit_thread_cnt found", K(submit_thread_cnt), K(cur_result), K(iops));
      } else {
        COMMON_LOG(INFO, "submit_thread_cnt tried", K(submit_thread_cnt), K(cur_result), K(iops));
        submit_thread_cnt = submit_thread_cnt * 2;
      }
    }
  }
  if (submit_thread_cnt > max_submit_thread_cnt) {
    submit_thread_cnt = max_submit_thread_cnt;
    COMMON_LOG(INFO, "no suitable submit_thread_cnt found, use max_submit_thread_cnt", K(max_submit_thread_cnt));
  }

  // set submit thread count back
  int temp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (temp_ret = ObIOManager::get_instance().delete_disk(fd))) {
    COMMON_LOG(WARN, "delete_disk failed", K(temp_ret), K(fd));
  } else if (OB_SUCCESS != (temp_ret = ObIOManager::get_instance().add_disk(fd))) {
    COMMON_LOG(WARN, "add_disk failed", K(temp_ret), K(fd), K(submit_thread_cnt));
  }
  return ret;
}

int ObIOBenchmark::find_getevent_thread_cnt(
    const int64_t throughtput, const int64_t submit_thread_cnt, int64_t& getevent_thread_cnt)
{
  int ret = OB_SUCCESS;
  getevent_thread_cnt = 0;
  ObDecompressTester decompress_tester;
  const int64_t total_size = 1L * 1024L * 1024L * 1024L;
  const int64_t block_size = 16 * 1024L * 1024L;
  const char* compressor_name = "lz4_1.0";
  int64_t decompress_throughtput = 0;
  if (throughtput <= 0 || submit_thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(throughtput), K(submit_thread_cnt));
  } else if (OB_FAIL(decompress_tester.init(total_size, block_size, compressor_name))) {
    COMMON_LOG(WARN, "decompress_tester init failed", K(ret), K(total_size), K(block_size), K(compressor_name));
  } else if (OB_FAIL(decompress_tester.get_decompress_throughtput(decompress_throughtput))) {
    COMMON_LOG(WARN, "get_decompress_throughtput failed", K(ret));
  } else if (decompress_throughtput <= 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "decompress_throughtput should not less than or equal to 0", K(ret), K(decompress_throughtput));
  } else {
    getevent_thread_cnt = upper_align(throughtput / decompress_throughtput + 1, submit_thread_cnt);
    COMMON_LOG(INFO,
        "getevent_thread_cnt found",
        K(getevent_thread_cnt),
        K(submit_thread_cnt),
        K(throughtput),
        K(decompress_throughtput));
  }
  return ret;
}

int ObIOBenchmark::benchmark(const char* data_dir, const int64_t file_size, const int32_t max_thread_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDiskFd fd;
  fd.disk_id_.disk_idx_ = 0;
  fd.disk_id_.install_seq_ = 0;
  char data_file_path[MAX_BENCHMARK_FILE_PATH_LEN + 1];
  MEMSET(data_file_path, 0, sizeof(data_file_path));

  int n = snprintf(data_file_path, MAX_BENCHMARK_FILE_PATH_LEN, "%s/bench_file", data_dir);
  if (n <= 0 || n >= MAX_BENCHMARK_FILE_PATH_LEN) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The data dir is too long, ", K(data_dir), K(ret));
  } else if ((fd.fd_ = ::open(
                  data_file_path, O_CREAT | O_TRUNC | O_DIRECT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "open file error", K(data_file_path), K(errno), KERRMSG, K(ret));
  } else if (OB_FAIL(FALLOCATE(fd.fd_, 0 /*MODE*/, 0 /*offset*/, file_size))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "allocate file error", K(data_file_path), K(file_size), K(errno), KERRMSG, K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_disk(fd))) {
    COMMON_LOG(WARN, "add_disk failed", K(ret), K(fd));
  } else {
    ObIORunner runner;
    ObIOBenchResult result;
    int32_t last_thread_cnt = max_thread_cnt;
    int64_t min_size_iops = 0;
    int min_size_thread_count = 0;
    int64_t max_throughtput = 0;
    if (OB_FAIL(runner.init(fd, file_size, max_thread_cnt))) {
      COMMON_LOG(WARN, "failed to init runner", K(ret));
    } else if (OB_FAIL(fill_file(runner, FILL_FILE_THREAD_CNT))) {
      COMMON_LOG(WARN, "failed to fill file", K(ret));
    } else {
      const int64_t wl_cnt = sizeof(WORKLOADS) / sizeof(ObIOWorkload);
      uint64_t bandwidth = 0;
      if (OB_SUCCESS != (tmp_ret = init_dev_info(data_dir))) {
        COMMON_LOG(INFO, "failed to init device info", K(tmp_ret), K(data_dir));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < wl_cnt; ++i) {
        result.reset();
        result.workload_ = WORKLOADS[i];
        get_current_disk_bandwidth(bandwidth);
        if (OB_FAIL(find_min_rt(runner, WORKLOADS[i], result.rt_))) {
          COMMON_LOG(WARN, "failed to get min rt", K(WORKLOADS[i]), K(ret));
        } else if (OB_FAIL(find_max_iops(runner, last_thread_cnt, WORKLOADS[i], last_thread_cnt, result.iops_))) {
          COMMON_LOG(WARN, "failed to find max iops", K(WORKLOADS[i]), K(ret));
        } else if (OB_FAIL(result_set_[result_set_idx_].add_result(result))) {
          COMMON_LOG(WARN, "failed to push result", K(WORKLOADS[i]), K(ret));
        } else {
          COMMON_LOG(INFO, "finish bechmarking one workload", K(WORKLOADS[i]), K(result), K(bandwidth));
        }
        if (0 == i) {
          min_size_iops = static_cast<int64_t>(result.iops_);
          min_size_thread_count = last_thread_cnt;
        }
        if (WORKLOADS[i].io_size_ * static_cast<int64_t>(result.iops_) > max_throughtput) {
          max_throughtput = WORKLOADS[i].io_size_ * static_cast<int64_t>(result.iops_);
        }
      }

      if (OB_SUCC(ret)) {
        int64_t submit_thread_cnt = 0;
        int64_t getevent_thread_cnt = 0;
        if (OB_FAIL(find_submit_thread_cnt(runner, fd, min_size_thread_count, min_size_iops, submit_thread_cnt))) {
          COMMON_LOG(WARN, "find_submit_thread_cnt failed", K(ret), K(fd), K(min_size_thread_count), K(min_size_iops));
        } else if (OB_FAIL(find_getevent_thread_cnt(max_throughtput, submit_thread_cnt, getevent_thread_cnt))) {
          COMMON_LOG(WARN, "find_getevent_thread_cnt failed", K(ret), K(max_throughtput), K(submit_thread_cnt));
        } else {
          result_set_[result_set_idx_].set_submit_thread_cnt(submit_thread_cnt);
          result_set_[result_set_idx_].set_getevent_thread_cnt(getevent_thread_cnt);
          result_set_[result_set_idx_].set_complete();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(result_set_[result_set_idx_].print_to_file(conf_file_))) {
          COMMON_LOG(WARN, "failed to print result to file", K(ret));
        }
      }
    }
  }

  if (fd.is_valid()) {
    // clean benchmark test file
    if (0 != ::close(fd.fd_)) {
      COMMON_LOG(WARN, "data file close error", K(errno), KERRMSG);
    }
    if (OB_SUCCESS != (tmp_ret = FileDirectoryUtils::delete_file(data_file_path))) {
      COMMON_LOG(WARN, "failed to delete iops_data", K(tmp_ret));
    }
  }
  return ret;
}

int ObIOBenchmark::get_min_rt(ObIOMode mode, const int64_t io_size, double& rt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(1000 * 1000 * 60)) {
      COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
    }
  } else {
    DRWLock::RDLockGuard guard(result_set_lock_);
    if (OB_FAIL(result_set_[result_set_idx_].get_rt(mode, io_size, rt))) {
      COMMON_LOG(WARN, "failed to get rt", K(ret), K_(result_set_idx), K(mode), K(io_size));
    }
  }
  return ret;
}

int ObIOBenchmark::get_max_iops(ObIOMode mode, const int64_t io_size, double& iops)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(1000 * 1000 * 60)) {
      COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
    }
  } else {
    DRWLock::RDLockGuard guard(result_set_lock_);
    if (OB_FAIL(result_set_[result_set_idx_].get_iops(mode, io_size, iops))) {
      COMMON_LOG(WARN, "failed to get iops", K(ret), K_(result_set_idx), K(mode), K(io_size));
    }
  }
  return ret;
}

int ObIOBenchmark::get_submit_thread_cnt(int64_t& submit_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(1000 * 1000 * 60)) {
      COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
    }
  } else {
    DRWLock::RDLockGuard guard(result_set_lock_);
    if (OB_FAIL(result_set_[result_set_idx_].get_submit_thread_cnt(submit_thread_cnt))) {
      COMMON_LOG(WARN, "failed to get_submit_thread_cnt", K(ret), K_(result_set_idx));
    }
  }
  return ret;
}

int ObIOBenchmark::get_getevent_thread_cnt(int64_t& getevent_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    if (REACH_TIME_INTERVAL(1000 * 1000 * 60)) {
      COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
    }
  } else {
    DRWLock::RDLockGuard guard(result_set_lock_);
    if (OB_FAIL(result_set_[result_set_idx_].get_getevent_thread_cnt(getevent_thread_cnt))) {
      COMMON_LOG(WARN, "failed to get_getevent_thread_cnt", K(ret), K_(result_set_idx));
    }
  }
  return ret;
}

int ObIOBenchmark::reload_io_bench_res()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
  } else {
    DRWLock::WRLockGuard guard(result_set_lock_);
    int32_t new_idx = (result_set_idx_ + 1) % RESULT_SET_CNT;
    if (OB_FAIL(result_set_[new_idx].load_from_file(conf_file_))) {
      COMMON_LOG(WARN, "failed to load from file", K(ret));
    } else {
      result_set_idx_ = new_idx;
    }
  }
  return ret;
}

int ObIOBenchmark::init_dev_info(const char* data_dir)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int fd = -1;
  struct stat data_dir_stat;
  if ((fd = ::open(data_dir, O_RDONLY | O_NONBLOCK)) == -1) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "failed to open data_dir", K(data_dir), K(ret), KERRMSG);
  } else if (0 != fstat(fd, &data_dir_stat)) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "failed to stat data_dir", K(data_dir), K(ret), KERRMSG);
  } else {
    dev_major_ = major(data_dir_stat.st_dev);
    dev_minor_ = minor(data_dir_stat.st_dev);
    if (OB_SUCCESS != (tmp_ret = get_sector_size(dev_major_))) {
      COMMON_LOG(WARN, "failed to get sector size", K(tmp_ret), K(data_dir), K_(dev_major));
    }
    COMMON_LOG(INFO, "device info :", K_(dev_major), K_(dev_minor), K_(sector_size));
  }
  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}

int ObIOBenchmark::get_partition_name(const int32_t dev_major, char* partition_name)
{
  int ret = OB_SUCCESS;
  const char* PARTITION_FILE = "/proc/partitions";
  const char* SCANF_FMT = "%ld %ld %ld %s";
  const int32_t SCAN_EXP_CNT = 4;
  const int64_t HEADER_LINE_CNT = 2;
  int32_t scan_cnt = 0;
  bool found = false;
  FILE* file = NULL;
  char line[512];
  int64_t get_line_cnt = 0;
  int64_t major = 0;
  int64_t minor = 0;
  int64_t block_cnt = 0;

  if (OB_ISNULL(file = fopen(PARTITION_FILE, "r"))) {
    ret = OB_ERR_SYS;
    COMMON_LOG(WARN, "failed to open /proc/partitions", K(ret), KERRMSG);
  } else {
    while (OB_SUCC(ret) && (NULL != fgets(line, sizeof(line), file)) && !found) {
      if (++get_line_cnt > HEADER_LINE_CNT) {
        if (SCAN_EXP_CNT != (scan_cnt = sscanf(line, SCANF_FMT, &major, &minor, &block_cnt, partition_name))) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "failed to parse /proc/partitions", K(ret), K(scan_cnt), KERRMSG);
        } else {
          if (major == dev_major && minor == 0) {
            found = true;
          }
        }
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    if (0 != fclose(file)) {
      COMMON_LOG(WARN, "failed to close file", KERRMSG);
    }
  }
  return ret;
}

int ObIOBenchmark::get_sector_size(const int32_t dev_major)
{
  int ret = OB_SUCCESS;
  const char* SECTOR_SIZE_FILE_FORMAT = "/sys/block/%s/queue/hw_sector_size";
  const char* SCANF_FMT = "%lu";
  const int32_t SCAN_EXP_CNT = 1;
  int32_t scan_cnt = 0;
  int32_t print_cnt = 0;
  FILE* file = NULL;
  char partition_name[128];
  char sector_size_path[256];
  uint64_t sector_size = 0;

  if (OB_FAIL(get_partition_name(dev_major, partition_name))) {
    COMMON_LOG(WARN, "failed to get partition name", K(ret), K(dev_major));
  } else {
    print_cnt = snprintf(sector_size_path, sizeof(sector_size_path), SECTOR_SIZE_FILE_FORMAT, partition_name);
    if (0 > print_cnt || sizeof(sector_size_path) <= print_cnt) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to get sector size file path", K(ret), K(print_cnt));
    } else if (OB_ISNULL(file = fopen(sector_size_path, "r"))) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "failed to open sector size file", K(ret), K(sector_size_path), KERRMSG);
    } else {
      if (SCAN_EXP_CNT != (scan_cnt = fscanf(file, SCANF_FMT, &sector_size))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "failed to parse sector size file", K(ret), K(sector_size_path), K(scan_cnt), KERRMSG);
      } else {
        COMMON_LOG(INFO, "sector size: ", K(sector_size), K(sector_size_path));
        sector_size_ = sector_size;
      }
      if (0 != fclose(file)) {
        COMMON_LOG(WARN, "failed to close file", K(ret), KERRMSG);
      }
    }
  }
  return ret;
}

int ObIOBenchmark::get_io_stat(const int32_t dev_major, const int32_t dev_minor, ObBlockIOInfo& io_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const char* SCAN_FMT = "%4d %4d %*s %u %u %llu %u %u %u %llu %u %*u %u %u";
  const char* DISKSTAT_FILE = "/proc/diskstats";
  const int32_t SCAN_EXP_CNT_OLD = 6;
  const int32_t SCAN_EXP_CNT = 12;
  int32_t scan_cnt = 0;
  bool found = false;
  FILE* file = NULL;
  int32_t major = 0;
  int32_t minor = 0;

  if (OB_ISNULL(file = fopen(DISKSTAT_FILE, "r"))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "failed to open diststat file", KERRMSG);
  } else {
    while (OB_SUCC(ret) && !found) {
      scan_cnt = fscanf(file,
          SCAN_FMT,
          &major,
          &minor,
          &io_info.rd_ios_,
          &io_info.rd_merges_,
          &io_info.rd_sectors_,
          &io_info.rd_ticks_,
          &io_info.wr_ios_,
          &io_info.wr_merges_,
          &io_info.wr_sectors_,
          &io_info.wr_ticks_,
          &io_info.ticks_,
          &io_info.aveq_);
      if (SCAN_EXP_CNT != scan_cnt) {
        if (EOF == scan_cnt) {
          break;
        } else if (SCAN_EXP_CNT_OLD == scan_cnt) {
          io_info.rd_sectors_ = io_info.rd_merges_;
          io_info.wr_sectors_ = io_info.rd_ticks_;
          io_info.rd_ios_ = 0;
          io_info.rd_merges_ = 0;
          io_info.rd_ticks_ = 0;
          io_info.wr_ios_ = 0;
          io_info.wr_merges_ = 0;
          io_info.wr_ticks_ = 0;
          io_info.ticks_ = 0;
          io_info.aveq_ = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "failed to parse diskstat file", K(scan_cnt));
        }
      }

      if (dev_major == major && dev_minor == minor) {
        found = true;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "specified device not found", K_(dev_major), K_(dev_minor), K(ret));
    }
    if (0 != (tmp_ret = fclose(file))) {
      COMMON_LOG(WARN, "failed to close diskstat file", KERRMSG);
    }
  }
  return ret;
}

int ObIOBenchmark::get_current_disk_bandwidth(uint64_t& bandwidth)
{
  int ret = OB_SUCCESS;
  ObBlockIOInfo last_info;
  ObBlockIOInfo cur_info;
  if (dev_major_ >= 0 && dev_minor_ >= 0) {
    if (OB_FAIL(get_io_stat(dev_major_, dev_minor_, last_info))) {
      COMMON_LOG(WARN, "failed to get io stat", K(ret));
    } else {
      sleep(1);
      if (OB_FAIL(get_io_stat(dev_major_, dev_minor_, cur_info))) {
        COMMON_LOG(WARN, "failed to get io stat", K(ret));
      } else {
        uint64_t rd_bandwidth = (cur_info.rd_sectors_ - last_info.rd_sectors_) * sector_size_;
        uint64_t wr_bandwidth = (cur_info.wr_sectors_ - last_info.wr_sectors_) * sector_size_;
        bandwidth = rd_bandwidth + wr_bandwidth;
        COMMON_LOG(INFO, "current disk bandwidth = ", K(bandwidth));
      }
    }
  }
  return ret;
}

int ObIOBenchmark::get_io_bench_result_str(char* buf, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
  } else {
    DRWLock::RDLockGuard guard(result_set_lock_);
    result_set_[result_set_idx_].to_string(buf, length);
  }
  return ret;
}

int ObIOBenchmark::get_disk_type_str(char* buf, const int64_t length) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObIOBenchmark has not been inited, ", K(ret));
  } else {
    int64_t pos = 0;
    const char* str = "UNKOWN";
    switch (disk_type_) {
      case DISK_HDD:
        str = "HDD";
        break;
      case DISK_SSD:
        str = "SSD";
        break;
      default:
        break;
    }
    databuff_printf(buf, length, pos, "%s", str);
  }
  return ret;
}

int ObIOBenchmark::cal_disk_type()
{
  int ret = OB_SUCCESS;
  double iops = 0;
  const int64_t TEST_IO_SIZE = 4L << 10;
  const double SSD_IOPS_THRESHOLD = 10000;
  if (OB_FAIL(result_set_[result_set_idx_].get_iops(ObIOMode::IO_MODE_READ, TEST_IO_SIZE, iops))) {
    COMMON_LOG(WARN, "failed to get max iops", K(ret));
  } else {
    if (iops > SSD_IOPS_THRESHOLD) {
      disk_type_ = ObDiskType::DISK_SSD;
    } else {
      disk_type_ = ObDiskType::DISK_HDD;
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
