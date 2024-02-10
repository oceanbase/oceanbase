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

#ifndef OB_ADMIN_BENCHMARK_TASK_EXECUTOR_H_
#define OB_ADMIN_BENCHMARK_TASK_EXECUTOR_H_

#include "share/backup/ob_backup_struct.h"
#include "deps/oblib/src/lib/ob_define.h"
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

namespace oceanbase
{
namespace tools
{

enum BenchmarkTaskType
{
  BENCHMARK_TASK_NORMAL_WRITE = 0,
  BENCHMARK_TASK_APPEND_WRITE = 1,
  BENCHMARK_TASK_MULTIPART_WRITE = 2,
  BENCHMARK_TASK_READ = 3,
  BENCHMARK_TASK_DEL = 4,
  BENCHMARK_TASK_IS_EXIST = 5,
  BENCHMARK_TASK_MAX_TYPE
};

struct TaskConfig
{
  TaskConfig();
  int assign(const TaskConfig &other);
  TO_STRING_KV(K_(thread_num), K_(max_task_runs), K_(time_limit_s),
      K_(obj_size), K_(obj_num), K_(fragment_size), K_(is_adaptive), K_(type));

  int64_t thread_num_;
  int64_t max_task_runs_;   // 每个线程执行次数
  int64_t time_limit_s_;
  int64_t obj_size_;
  int64_t obj_num_;
  int64_t fragment_size_;
  bool is_adaptive_;
  BenchmarkTaskType type_;
};

struct TimeMap
{
  TimeMap();
  int log_entry(const int64_t start_time_us);
  int add(const TimeMap &other);
  int assign(const TimeMap &other);
  void summary(const char *map_name_str);

  int64_t total_entry_;
  std::map<int64_t, int64_t> time_ms_map_;
};

struct Metrics
{
  Metrics();
  int assign(const Metrics &other);
  int add(const Metrics &other);
  void summary(
      const struct timeval &start_real_time,
      const struct rusage &start_usage,
      const int64_t thread_num);

  int64_t status_;
  int64_t throughput_bytes_;
  int64_t operation_num_;
  TimeMap total_op_time_ms_map_;
  TimeMap open_time_ms_map_;
  TimeMap close_time_ms_map_;
};

class ITaskExecutor
{
public:
  ITaskExecutor();
  virtual ~ITaskExecutor() {}
  void reset();
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config);
  virtual int execute() { return OB_SUCCESS; };
  const Metrics &get_metrics() { return metrics_; }

protected:
  int prepare_(const int64_t object_id);
  void finish_(const int64_t ob_errcode);

protected:
  bool is_inited_;
  int64_t base_uri_len_;
  char base_uri_[common::OB_MAX_URI_LENGTH];
  share::ObBackupStorageInfo *storage_info_;
  Metrics metrics_;
};

void init_random_content();
int init_task_executor(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config, ITaskExecutor *&executor);

class WriteTaskExecutor : public ITaskExecutor
{
public:
  WriteTaskExecutor();
  virtual ~WriteTaskExecutor() {}
  void reset();
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  int64_t obj_size_;
  char *write_buf_;
  ObArenaAllocator allocator_;
};

class AppendWriteTaskExecutor : public ITaskExecutor
{
public:
  AppendWriteTaskExecutor();
  virtual ~AppendWriteTaskExecutor() {}
  void reset();
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  int64_t obj_size_;
  int64_t fragment_size_;
  char *write_buf_;
  ObArenaAllocator allocator_;
};

class MultipartWriteTaskExecutor : public ITaskExecutor
{
public:
  MultipartWriteTaskExecutor();
  virtual ~MultipartWriteTaskExecutor() {}
  void reset();
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  int64_t obj_size_;
  int64_t part_size_;
  char *write_buf_;
  ObArenaAllocator allocator_;
};

class ReadTaskExecutor : public ITaskExecutor
{
public:
  ReadTaskExecutor();
  virtual ~ReadTaskExecutor() {}
  void reset();
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  int64_t obj_size_;
  int64_t obj_num_;
  bool is_adaptive_;
  int64_t expected_read_size_;
  char *read_buf_;
  ObArenaAllocator allocator_;

private:
  static const int64_t ALIGNMENT = 16 * 1024;
};

class DelTaskExecutor : public ITaskExecutor
{
public:
  DelTaskExecutor();
  virtual ~DelTaskExecutor() {}
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  bool is_adaptive_;
};

class IsExistTaskExecutor : public ITaskExecutor
{
public:
  IsExistTaskExecutor();
  virtual ~IsExistTaskExecutor() {}
  virtual int init(const char *base_uri,
      share::ObBackupStorageInfo *storage_info, const TaskConfig &config) override;
  virtual int execute() override;

private:
  int64_t obj_num_;
};

} //namespace tools
} //namespace oceanbase

#endif  // OB_ADMIN_BENCHMARK_TASK_EXECUTOR_H_