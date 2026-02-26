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

#ifndef OB_ADMIN_IO_ADAPTER_BENCH_H_
#define OB_ADMIN_IO_ADAPTER_BENCH_H_
#include <gtest/gtest.h>
#include "../ob_admin_executor.h"
#include "deps/oblib/src/lib/restore/ob_storage_info.h"
#include "deps/oblib/src/lib/ob_define.h"                   // OB_MAX_URI_LENGTH
#include "share/backup/ob_backup_struct.h"                  // OB_MAX_BACKUP_STORAGE_INFO_LENGTH
#include "share/ob_thread_mgr.h"
#include "task_executor.h"

namespace oceanbase
{
namespace tools
{

static const int64_t DEFAULT_THREAD_NUM             = 4;
static const int64_t DEFAULT_MAX_TASK_RUN_TIMES     = 10;
static const int64_t DEFAULT_TIME_LIMIT_S           = -1;
static const int64_t DEFAULT_OBJ_SIZE               = 2 * 1024 * 1024;
static const int64_t DEFAULT_OBJ_NUM                = 10;
static const int64_t DEFAULT_FRAGMENT_SIZE          = -1;
static const int64_t DEFAULT_WRITE_SIZE             = 2 * 1024 * 1024;
static const int64_t DEFAULT_APPEND_SIZE            = 64 * 1024 * 1024;
static const int64_t DEFAULT_APPEND_FRAGMENT_SIZE   = 0;
static const int64_t DEFAULT_MULTI_SIZE             = 64 * 1024 * 1024;
static const int64_t DEFAULT_MULTI_FRAGMENT_SIZE    = 2 * 1024 * 1024;
static const int64_t READ_16K_SIZE                  = 16 * 1024;
static const int64_t READ_2M_SIZE                   = 2 * 1024 * 1024;
static const int64_t DEFAULT_MEMORY_LIMIT_SIZE_GB   = 16;

class ObAdminIOAdapterBenchmarkExecutor : public ObAdminExecutor
{
public:
  ObAdminIOAdapterBenchmarkExecutor();
  virtual ~ObAdminIOAdapterBenchmarkExecutor() {}
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd_(int argc, char *argv[]);
  int run_all_tests_();
  int print_usage_();
  int clean_base_path_(share::ObBackupStorageInfo &info);

private:
  char base_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  BenchmarkTaskType task_type_;
  int64_t thread_num_;
  int64_t max_task_run_times_;
  int64_t time_limit_s_;
  int64_t obj_size_;
  int64_t obj_num_;
  int64_t fragment_size_;
  int64_t write_size_;
  int64_t append_size_;
  int64_t append_fragment_size_;
  int64_t multi_size_;
  int64_t multi_fragment_size_;
  int64_t memory_limit_size_gb_;
  bool is_adaptive_;
  bool clean_after_execution_;

  std::vector<TaskConfig> configs_;

private:
  static constexpr char *HELP_FMT = const_cast<char*>("\t%-30s%-12s\n");
  DISALLOW_COPY_AND_ASSIGN(ObAdminIOAdapterBenchmarkExecutor);
};

class ObBackupIoAdapterBenchmarkRunner : public lib::TGRunnable
{
public:
  ObBackupIoAdapterBenchmarkRunner();
  ~ObBackupIoAdapterBenchmarkRunner();
  int init(const char *base_uri, share::ObBackupStorageInfo *storage_info, const TaskConfig &config);
  void destroy();
  int do_benchmark();
  virtual void run1() override;

private:
  common::SpinRWLock lock_;
  bool is_inited_;
  int tg_id_;
  int ret_code_;
  TaskConfig config_;
  Metrics metrics_;
  char base_uri_[common::OB_MAX_URI_LENGTH];
  share::ObBackupStorageInfo *storage_info_;
  int64_t completed_threads_;  // the count of completed threads
  int64_t total_threads_;    // the total count of threads
};

} //namespace tools
} //namespace oceanbase

#endif  // OB_ADMIN_IO_ADAPTER_BENCH_H_