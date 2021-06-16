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

#ifndef TEST_IO_PERFORMANCE_H
#define TEST_IO_PERFORMANCE_H
#include "lib/io/ob_io_manager.h"
#include "lib/file/file_directory_utils.h"
#include "common/ob_cost_consts_def.h"

namespace oceanbase {
namespace common {

struct IOWorkload {
public:
  IOWorkload() : category_(USER_IO), size_(0), iops_(0), depth_(0), is_sequence_(0)
  {}
  bool is_valid() const
  {
    return category_ < MAX_IO_CATEGORY && size_ > 0 && iops_ > 0 && depth_ > 0;
  }
  TO_STRING_KV(K_(category), K_(size), K_(iops), K_(depth), K_(is_sequence))

public:
  ObIOCategory category_;
  int32_t size_;
  int64_t iops_;
  int32_t depth_;
  bool is_sequence_;
};

class IOInfoGenerator {
  friend struct IORunContext;

public:
  IOInfoGenerator() : inited_(false), mode_(IO_MODE_READ), io_category_(USER_IO), io_size_(0), is_sequence_(false)
  {}
  virtual ~IOInfoGenerator()
  {}
  bool is_inited() const
  {
    return inited_;
  }
  virtual int get_io_info(const char* buf, ObIOInfo& info) = 0;
  TO_STRING_KV(K_(inited), K_(mode), K_(io_category), K_(io_size), K_(is_sequence));

protected:
  bool inited_;
  ObIOMode mode_;
  ObIOCategory io_category_;
  int32_t io_size_;
  bool is_sequence_;
};

struct IORunContext {
public:
  IORunContext() : thread_cnt_(0), generator_(NULL), succ_cnt_(0), fail_cnt_(0), sum_rt_(0)
  {}
  TO_STRING_KV(K_(thread_cnt), K_(workload), K_(succ_cnt), K_(fail_cnt), KP_(generator), K_(sum_rt));
  bool is_valid() const
  {
    bool bret = thread_cnt_ >= 0 && succ_cnt_ >= 0 && fail_cnt_ >= 0 && sum_rt_ >= 0 && OB_NOT_NULL(generator_) &&
                workload_.is_valid() && generator_->is_inited() && generator_->io_category_ == workload_.category_ &&
                generator_->io_size_ == workload_.size_ && generator_->is_sequence_ == workload_.is_sequence_;
    return bret;
  }

public:
  int64_t thread_cnt_;
  IOWorkload workload_;
  IOInfoGenerator* generator_;
  int64_t succ_cnt_;
  int64_t fail_cnt_;
  int64_t sum_rt_;
};

class IORunner : public lib::ThreadPool {
public:
  IORunner() : inited_(0), duration_ms_(0), begin_time_(0), end_time_(0){};
  ~IORunner()
  {}
  int init(const IORunContext& read_ctx, const IORunContext& write_ctx, const int64_t duration_ms);
  int run_workload();
  bool is_inited() const
  {
    return inited_;
  }
  TO_STRING_KV(K_(inited), K_(duration_ms), K_(read_ctx), K_(write_ctx), K_(begin_time), K_(end_time));

private:
  void run1() override;
  int run_read();
  int run_write(const char* data);
  void print_result();
  inline int64_t get_wait_interval(const int64_t iops, const int64_t thread_cnt, const int32_t io_depth);

private:
  bool inited_;
  int64_t duration_ms_;
  IORunContext read_ctx_;
  IORunContext write_ctx_;
  int64_t begin_time_;
  int64_t end_time_;
};

class SingleIOInfoGenerator : public IOInfoGenerator {
public:
  SingleIOInfoGenerator() : fd_(), file_size_(0), last_offset_(0)
  {}
  virtual ~SingleIOInfoGenerator()
  {}
  int init(const ObIOMode mode, const IOWorkload& workload, const ObDiskFd& fd, const int64_t file_size);
  virtual int get_io_info(const char* buf, ObIOInfo& info) override;

private:
  ObDiskFd fd_;
  int64_t file_size_;
  int64_t last_offset_;
};

class MultiIOInfoGenerator : public IOInfoGenerator {
public:
  MultiIOInfoGenerator() : file_size_(0), last_offset_(0)
  {}
  virtual ~MultiIOInfoGenerator()
  {}
  int init(const ObIOMode mode, const IOWorkload& workload, const ObArray<ObDiskFd>& fds, const int64_t file_size);
  virtual int get_io_info(const char* buf, ObIOInfo& info) override;

private:
  ObArray<ObDiskFd> fds_;
  int64_t file_size_;
  int64_t last_offset_;
};

class IOStress {
public:
  IOStress() : inited_(false), file_ready_(false), file_size_(0)
  {}
  ~IOStress();

  int init(const char* config_file_path);
  int run();
  int prepare_files(const int64_t file_num, const int64_t file_size);

private:
  int prepare_one_file(const char* file_path, const int64_t file_size, int& fd);
  void destroy_files();
  int load_io_runner(FILE* config_file, IORunner& runner);
  int get_info_generator(
      const ObIOMode mode, const bool multi_disk, const IOWorkload& workload, IOInfoGenerator*& generator);

private:
  bool inited_;
  bool file_ready_;
  int64_t file_size_;
  ObArray<ObDiskFd> fds_;
  ObArray<IORunner*> runners_;
};

/**
 * ---------------------------------- IORunner -------------------------------
 */
int IORunner::init(const IORunContext& read_ctx, const IORunContext& write_ctx, const int64_t duration_ms)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if ((!read_ctx.is_valid() && !write_ctx.is_valid()) ||
             duration_ms <= 0) {  // require at least one ctx is valid
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(read_ctx), K(write_ctx), K(duration_ms));
  } else {
    read_ctx_ = read_ctx;
    write_ctx_ = write_ctx;
    duration_ms_ = duration_ms;
    set_thread_count(read_ctx_.thread_cnt_ + write_ctx_.thread_cnt_);
    inited_ = true;
  }
  return ret;
}

void IORunner::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret), K(thread_id));
  } else if (thread_id < read_ctx_.thread_cnt_) {
    while (!has_set_stop() && read_ctx_.is_valid()) {
      const int64_t begin_time = ObTimeUtility::current_time();
      if (OB_FAIL(run_read())) {
        COMMON_LOG(WARN, "fail to run read", K(ret));
      }
      const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
      const int64_t interval =
          get_wait_interval(read_ctx_.workload_.iops_, read_ctx_.thread_cnt_, read_ctx_.workload_.depth_);
      if (cost_time < interval) {
        usleep(static_cast<int>(interval - cost_time));
      }
    }
  } else if (thread_id < read_ctx_.thread_cnt_ + write_ctx_.thread_cnt_) {
    while (!has_set_stop() && write_ctx_.is_valid()) {
      char* data = new char[write_ctx_.workload_.size_];
      if (OB_ISNULL(data)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "fail to allocate write buf", K(ret), "data_size", write_ctx_.workload_.size_);
      } else {
        const int64_t begin_time = ObTimeUtility::current_time();
        if (OB_FAIL(run_write(data))) {
          COMMON_LOG(WARN, "fail to run write", K(ret));
        }
        const int64_t interval =
            get_wait_interval(write_ctx_.workload_.iops_, write_ctx_.thread_cnt_, write_ctx_.workload_.depth_);
        const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
        if (cost_time < interval) {
          usleep(static_cast<int>(interval - cost_time));
        }
      }
      if (OB_NOT_NULL(data)) {
        delete[] data;
        data = NULL;
      }
    }
  }
}

void IORunner::print_result()
{
  if (end_time_ <= begin_time_) {
    COMMON_LOG(WARN, "wrong time", K(begin_time_), K(end_time_));
  } else {
    COMMON_LOG(INFO,
        "Read Result",
        "workload",
        read_ctx_.workload_,
        "succ_cnt",
        read_ctx_.succ_cnt_,
        "fail_cnt",
        read_ctx_.fail_cnt_,
        "time_ms",
        (end_time_ - begin_time_) / 1000,
        "iops",
        read_ctx_.succ_cnt_ * 1000000L / (end_time_ - begin_time_),
        "rt",
        read_ctx_.sum_rt_ / (read_ctx_.succ_cnt_ > 0 ? read_ctx_.succ_cnt_ : 1));
    COMMON_LOG(INFO,
        "Write Result",
        "workload",
        read_ctx_.workload_,
        "succ_cnt",
        write_ctx_.succ_cnt_,
        "fail_cnt",
        write_ctx_.fail_cnt_,
        "time_ms",
        (end_time_ - begin_time_) / 1000,
        "iops",
        write_ctx_.succ_cnt_ * 1000000L / (end_time_ - begin_time_),
        "rt",
        write_ctx_.sum_rt_ / (write_ctx_.succ_cnt_ > 0 ? write_ctx_.succ_cnt_ : 1));
  }
}

int IORunner::run_workload()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    begin_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(ThreadPool::start())) {
      COMMON_LOG(WARN, "fail to start threads", K(ret));
    } else {
      usleep(static_cast<int>(duration_ms_ * 1000));
      end_time_ = ObTimeUtility::current_time();
      ThreadPool::stop();
    }
  }
  if (OB_SUCC(ret)) {
    print_result();
  }
  return ret;
}

int IORunner::run_read()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObIOInfo info;
    ObIOHandle handle[read_ctx_.workload_.depth_];
    const int64_t begin_time = ObTimeUtility::current_time();
    for (int32_t i = 0; OB_SUCC(ret) && i < read_ctx_.workload_.depth_; ++i) {
      if (OB_FAIL(read_ctx_.generator_->get_io_info(nullptr, info))) {
        COMMON_LOG(WARN, "fail to get read io info", K(ret), K(i));
      } else if (OB_FAIL(ObIOManager::get_instance().aio_read(info, handle[i]))) {
        COMMON_LOG(WARN, "fail to aio read", K(ret), K(i));
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < read_ctx_.workload_.depth_; ++i) {
      if (OB_FAIL(handle[i].wait())) {
        COMMON_LOG(WARN, "fail to wait read io", K(ret), K(i));
        ATOMIC_INC(&read_ctx_.fail_cnt_);
      } else {
        ATOMIC_INC(&read_ctx_.succ_cnt_);
      }
      int64_t delay = handle[i].get_rt();
      if (delay <= 0) {
        delay = ObTimeUtility::current_time() - begin_time;
      }
      read_ctx_.sum_rt_ += delay;
    }
  }

  return ret;
}

int IORunner::run_write(const char* data)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "data is null", K(ret), K(data));
  } else {
    ObIOInfo info;
    ObIOHandle handle[write_ctx_.workload_.depth_];
    const int64_t begin_time = ObTimeUtility::current_time();
    for (int32_t i = 0; OB_SUCC(ret) && i < write_ctx_.workload_.depth_; ++i) {
      if (OB_FAIL(write_ctx_.generator_->get_io_info(data, info))) {
        COMMON_LOG(WARN, "fail to get read io info", K(ret), K(i));
      } else if (OB_FAIL(ObIOManager::get_instance().aio_write(info, handle[i]))) {
        COMMON_LOG(WARN, "fail to aio read", K(ret), K(i));
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < write_ctx_.workload_.depth_; ++i) {
      if (OB_FAIL(handle[i].wait())) {
        COMMON_LOG(WARN, "fail to wait read io", K(ret), K(i));
        ATOMIC_INC(&write_ctx_.fail_cnt_);
      } else {
        ATOMIC_INC(&write_ctx_.succ_cnt_);
      }
      int64_t delay = handle[i].get_rt();
      if (delay <= 0) {
        delay = ObTimeUtility::current_time() - begin_time;
      }
      write_ctx_.sum_rt_ += delay;
    }
  }

  return ret;
}

inline int64_t IORunner::get_wait_interval(const int64_t iops, const int64_t thread_cnt, const int32_t io_depth)
{
  const int64_t MILLION = 1000L * 1000L;
  const int64_t interval = thread_cnt * io_depth * MILLION / iops;
  return interval;
}

/**
 * ---------------------------------------- SingleIOInfoGenerator --------------------------------
 */
int SingleIOInfoGenerator::init(
    const ObIOMode mode, const IOWorkload& workload, const ObDiskFd& fd, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (mode >= IO_MODE_MAX || !workload.is_valid() || !fd.is_valid() || file_size <= 0 ||
             (IO_MODE_WRITE == mode && workload.size_ % DIO_READ_ALIGN_SIZE != 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mode), K(workload), K(fd), K(file_size));
  } else {
    mode_ = mode;
    io_category_ = workload.category_;
    io_size_ = workload.size_;
    is_sequence_ = workload.is_sequence_;
    fd_ = fd;
    file_size_ = file_size;
    inited_ = true;
  }
  return ret;
}

int SingleIOInfoGenerator::get_io_info(const char* buf, ObIOInfo& info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    ObIOPoint& io_point = info.io_points_[0];
    info.io_desc_.category_ = io_category_;
    info.io_desc_.mode_ = mode_;
    io_point.fd_ = fd_;
    io_point.size_ = io_size_;
    io_point.write_buf_ = buf;
    info.size_ = io_point.size_;
    info.batch_count_ = 1;
    if (IO_MODE_READ == mode_) {
      info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    } else {
      info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
    }

    if (is_sequence_) {
      last_offset_ += io_size_;
      io_point.offset_ = last_offset_;
    } else {
      int64_t rand_offset = ObRandom::rand(0, file_size_ - io_size_);
      if (IO_MODE_READ == mode_) {
        io_point.offset_ = rand_offset;
      } else {
        io_point.offset_ = lower_align(rand_offset, DIO_READ_ALIGN_SIZE);
      }
    }
    info.offset_ = info.io_points_[0].offset_ % common::DEFAULT_MACRO_BLOCK_SIZE;
  }
  return ret;
}

/**
 * -------------------------------- MultiIOInfoGenerator ---------------------------
 */

int MultiIOInfoGenerator::init(
    const ObIOMode mode, const IOWorkload& workload, const ObArray<ObDiskFd>& fds, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (mode >= IO_MODE_MAX || !workload.is_valid() || fds.empty() || file_size <= 0 ||
             (IO_MODE_WRITE == mode && workload.size_ % DIO_READ_ALIGN_SIZE != 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mode), K(workload), K(fds), K(file_size));
  } else {
    mode_ = mode;
    io_category_ = workload.category_;
    io_size_ = workload.size_;
    is_sequence_ = workload.is_sequence_;
    fds_ = fds;
    file_size_ = file_size;
    inited_ = true;
  }
  return ret;
}

int MultiIOInfoGenerator::get_io_info(const char* buf, ObIOInfo& info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    const int32_t batch_count = 16;
    info.io_desc_.category_ = io_category_;
    info.io_desc_.mode_ = mode_;
    info.size_ = io_size_;
    info.batch_count_ = batch_count;
    if (IO_MODE_READ == mode_) {
      info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    } else {
      info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
    }

    for (int i = 0; i < batch_count; ++i) {
      ObIOPoint& point = info.io_points_[i];
      point.fd_ = fds_[i % fds_.size()];
      point.size_ = io_size_ / batch_count;
      point.write_buf_ = buf + i * point.size_;
      if (is_sequence_) {
        last_offset_ += point.size_;
        point.offset_ = last_offset_;
      } else {
        int64_t rand_offset = ObRandom::rand(0, file_size_ - io_size_);
        if (IO_MODE_READ == mode_) {
          point.offset_ = rand_offset;
        } else {
          point.offset_ = lower_align(rand_offset, DIO_READ_ALIGN_SIZE);
        }
      }
    }
    info.offset_ = info.io_points_[0].offset_ % common::DEFAULT_MACRO_BLOCK_SIZE;
  }
  return ret;
}

/**
 * -------------------------------------- IOStress --------------------------------
 */
IOStress::~IOStress()
{
  for (int64_t i = 0; i < runners_.size(); ++i) {
    if (OB_NOT_NULL(runners_[i])) {
      delete runners_[i];
      runners_[i] = NULL;
    }
  }
}

int IOStress::init(const char* config_file_path)
{
  int ret = OB_SUCCESS;
  int64_t workload_cnt = 0;
  int64_t file_size_gb = 0;
  int64_t file_num = 0;
  FILE* config_file = NULL;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(config_file_path)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_ISNULL(config_file = fopen(config_file_path, "rt"))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "fail to open config file", K(ret));
  } else if (3 != fscanf(config_file,
                      "workload_cnt: %ld, file_size_gb: %ld, file_num: %ld\n",
                      &workload_cnt,
                      &file_size_gb,
                      &file_num)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "fail to read config file", K(ret));
  } else if (workload_cnt <= 0 || file_size_gb <= 0 || file_num <= 0) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "invalid config", K(ret), K(workload_cnt), K(file_size_gb), K(file_num));
  } else if (OB_FAIL(prepare_files(file_num, file_size_gb * 1024L * 1024L * 1024L))) {
    COMMON_LOG(WARN, "fail to prepare file", K(ret), K(file_num), K(file_size_gb));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < workload_cnt; ++i) {
      IORunner* runner = new IORunner;
      if (OB_FAIL(load_io_runner(config_file, *runner))) {
        COMMON_LOG(WARN, "fail to load runner", K(ret), K(i));
      } else if (!runner->is_inited()) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "runner not init", K(ret));
      } else if (OB_FAIL(runners_.push_back(runner))) {
        COMMON_LOG(WARN, "fail to push runner to array", K(ret));
      }
    }  // end for loop
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }

  return ret;
}

int IOStress::run()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < runners_.size(); ++i) {
      runners_[i]->run_workload();
    }
  }
  return ret;
}

int IOStress::prepare_files(const int64_t file_num, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_FILE_PATH_LENGTH = 256;
  if (file_ready_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "file is already ready", K(ret));
  } else if (file_num <= 0 || file_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(file_num), K(file_size));
  } else {
    file_size_ = file_size;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_num; ++i) {
      ObDiskFd fd;
      char file_path[MAX_FILE_PATH_LENGTH] = {0};
      snprintf(file_path, sizeof(file_path) - 1, "./io_stress_file_%ld", i + 1);
      fd.disk_id_.disk_idx_ = i;
      fd.disk_id_.install_seq_ = 0;
      if (OB_FAIL(prepare_one_file(file_path, file_size_, fd.fd_))) {
        COMMON_LOG(WARN, "fail to prepare one file", K(ret), K(file_path), K(file_size_));
      } else if (fd.fd_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fd is invalid", K(fd));
      } else if (OB_FAIL(ObIOManager::get_instance().add_disk(fd))) {
        COMMON_LOG(WARN, "fail to add disk", K(ret));
      } else if (OB_FAIL(fds_.push_back(fd))) {
        COMMON_LOG(WARN, "fail to push fd to array", K(ret), K(fd));
      }
    }
    if (OB_SUCC(ret) && fds_.size() == file_num) {
      file_ready_ = true;
    }
  }
  if (!file_ready_) {
    destroy_files();
  }
  return ret;
}

void IOStress::destroy_files()
{
  for (int64_t i = 0; i < fds_.size(); ++i) {
    if (::close(fds_[i].fd_) < 0) {
      COMMON_LOG(ERROR, "fail to close file", "fd", fds_[i]);
    }
  }
  fds_.reset();
  file_size_ = 0;
  file_ready_ = false;
}

int IOStress::prepare_one_file(const char* file_path, const int64_t file_size, int& fd)
{
  int ret = OB_SUCCESS;
  fd = -1;
  // open existing file
  bool is_exist = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(file_path, is_exist))) {
    COMMON_LOG(WARN, "fail to decide if file exist", K(ret), K(file_path));
  } else if (is_exist) {  // file exist
    int64_t exist_file_size = 0;
    if (OB_FAIL(FileDirectoryUtils::get_file_size(file_path, exist_file_size))) {
      COMMON_LOG(WARN, "fail to get existing file size", K(ret));
    } else if (file_size == exist_file_size) {
      fd = ::open(file_path, O_DIRECT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      if (fd < 0) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to open exist file", K(ret), K(file_path));
      }
    }
  }

  // create file if not exist
  if (OB_SUCC(ret) && fd < 0) {
    if ((fd = ::open(file_path, O_CREAT | O_TRUNC | O_DIRECT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "fail to create file", K(ret), K(file_path));
    } else if (fallocate(fd, 0, 0, file_size) < 0) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "fail to allocate file", K(ret), K(fd), K(file_path), K(file_size));
    }
  }
  return ret;
}

int IOStress::load_io_runner(FILE* config_file, IORunner& runner)
{
  int ret = OB_SUCCESS;
  IORunContext read_ctx;
  IORunContext write_ctx;
  int64_t duration_ms = 0;
  int multi_disk = -1;
  int read_category = -1;
  int read_is_sequence = -1;
  int write_category = -1;
  int write_is_sequence = -1;
  if (!file_ready_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "file not ready", K(ret));
  } else if (OB_ISNULL(config_file)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(config_file));
  } else if (2 != fscanf(config_file, "duration_ms: %ld, multi_disk: %d\n", &duration_ms, &multi_disk)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "fail to read config file", K(ret));
  } else if (6 != fscanf(config_file,
                      "Read: thread_cnt: %ld, category: %d, size_kb: %d, iops: %ld, depth: %d, is_sequence: %d\n",
                      &read_ctx.thread_cnt_,
                      &read_category,
                      &read_ctx.workload_.size_,
                      &read_ctx.workload_.iops_,
                      &read_ctx.workload_.depth_,
                      &read_is_sequence)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "fail to read config file", K(ret));
  } else if (6 != fscanf(config_file,
                      "Write: thread_cnt: %ld, category: %d, size_kb: %d, iops: %ld, depth: %d, is_sequence: %d\n",
                      &write_ctx.thread_cnt_,
                      &write_category,
                      &write_ctx.workload_.size_,
                      &write_ctx.workload_.iops_,
                      &write_ctx.workload_.depth_,
                      &write_is_sequence)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "fail to read config file", K(ret));
  } else if (read_category < 0 || read_is_sequence < 0 || write_category < 0 || write_is_sequence < 0) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(
        WARN, "invalid data", K(ret), K(read_category), K(read_is_sequence), K(write_category), K(write_is_sequence));
  } else {
    read_ctx.workload_.category_ = static_cast<ObIOCategory>(read_category);
    read_ctx.workload_.is_sequence_ = static_cast<bool>(read_is_sequence);
    read_ctx.workload_.size_ *= 1024;
    write_ctx.workload_.category_ = static_cast<ObIOCategory>(write_category);
    write_ctx.workload_.is_sequence_ = static_cast<bool>(write_is_sequence);
    write_ctx.workload_.size_ *= 1024;
    if (duration_ms <= 0 || multi_disk < 0 || read_ctx.thread_cnt_ < 0 || write_ctx.thread_cnt_ < 0 ||
        (read_ctx.thread_cnt_ + write_ctx.thread_cnt_ <= 0) || !read_ctx.workload_.is_valid() ||
        !write_ctx.workload_.is_valid() || write_ctx.workload_.size_ % DIO_READ_ALIGN_SIZE != 0) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WARN, "invalid config", K(ret), K(duration_ms), K(read_ctx), K(write_ctx));
    } else if (OB_FAIL(get_info_generator(IO_MODE_READ, multi_disk, read_ctx.workload_, read_ctx.generator_))) {
      COMMON_LOG(WARN, "fail to get read generator", K(ret), K(multi_disk), K(read_ctx.workload_));
    } else if (OB_FAIL(get_info_generator(IO_MODE_WRITE, multi_disk, write_ctx.workload_, write_ctx.generator_))) {
      COMMON_LOG(WARN, "fail to get write generator", K(ret), K(multi_disk), K(write_ctx.workload_));
    } else if (OB_FAIL(runner.init(read_ctx, write_ctx, duration_ms))) {
      COMMON_LOG(WARN, "fail to init io runner", K(ret), K(read_ctx), K(write_ctx));
    }
  }
  return ret;
}

int IOStress::get_info_generator(
    const ObIOMode mode, const bool multi_disk, const IOWorkload& workload, IOInfoGenerator*& generator)
{
  int ret = OB_SUCCESS;
  if (!file_ready_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "file not ready", K(ret));
  } else if (mode >= IO_MODE_MAX || !workload.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mode), K(workload));
  } else if (multi_disk) {
    MultiIOInfoGenerator* multi_gen = new MultiIOInfoGenerator();
    if (OB_ISNULL(multi_gen)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_FAIL(multi_gen->init(mode, workload, fds_, file_size_))) {
      COMMON_LOG(WARN, "fail to init MultiIOInfoGenerator", K(ret));
    } else {
      generator = multi_gen;
    }
  } else {
    SingleIOInfoGenerator* single_gen = new SingleIOInfoGenerator();
    if (OB_ISNULL(single_gen)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to allocate memory", K(ret));
    } else if (OB_FAIL(single_gen->init(mode, workload, fds_[0], file_size_))) {
      COMMON_LOG(WARN, "fail to init SingleIOInfoGenerator", K(ret));
    } else {
      generator = single_gen;
    }
  }
  return ret;
}

}  // end namespace common
}  // end namespace oceanbase

#endif
