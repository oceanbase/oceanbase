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

#include <time.h>
#include "ob_admin_io_adapter_bench.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"
#include "src/logservice/archiveservice/ob_archive_file_utils.h"
#include "src/share/backup/ob_backup_path.h"
#include "src/share/backup/ob_backup_clean_util.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{

ObAdminIOAdapterBenchmarkExecutor::ObAdminIOAdapterBenchmarkExecutor()
    : clean_before_execution_(false), clean_after_execution_(false), config_()
{
  MEMSET(base_path_, 0, sizeof(base_path_));
  MEMSET(storage_info_str_, 0, sizeof(storage_info_str_));
}

int ObAdminIOAdapterBenchmarkExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  lib::set_memory_limit(16 * 1024 * 1024 * 1024LL);
  lib::set_tenant_memory_limit(500, 16 * 1024 * 1024 * 1024LL);
  OB_LOGGER.set_log_level("INFO");
  if (OB_FAIL(parse_cmd_(argc, argv))) {
    OB_LOG(WARN, "failed to parse cmd", K(ret), K(argc), K(argv));
  } else if (OB_FAIL(run_all_tests_())) {
    OB_LOG(WARN, "failed to pass all tests", K(ret), K_(base_path));
  }
  return ret;
}

int ObAdminIOAdapterBenchmarkExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "h:d:s:t:r:l:o:n:f:p:b:c:j:";
  struct option longopts[] = {{"help", 0, NULL, 'h'},
      {"file-path-prefix", 1, NULL, 'd'},
      {"storage-info", 1, NULL, 's'},
      {"thread-num", 1, NULL, 't'},
      {"max-task-run-times", 1, NULL, 'r'},
      {"time-limit", 1, NULL, 'l'},
      {"obj-size", 1, NULL, 'o'},
      {"obj-num", 1, NULL, 'n'},
      {"fragment-size", 1, NULL, 'f'},
      {"type", 1, NULL, 'p'},
      {"is-adaptive", 1, NULL, 'j'},
      {"clean-before-execution", 0, NULL, 'b'},
      {"clean-after-execution", 0, NULL, 'c'},
      {NULL, 0, NULL, 0}};
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage_();
        exit(1);
      }
      case 'd': {
        time_t timestamp = time(NULL);
        struct tm *timeinfo = localtime(&timestamp);
        char buffer[OB_MAX_TIME_STR_LENGTH];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d-%H:%M:%S", timeinfo);
        if (OB_FAIL(databuff_printf(base_path_, sizeof(base_path_), "%s", optarg))) {
          OB_LOG(WARN, "failed to construct base path", K(ret), K((char *)optarg), K(buffer));
        }
        break;
      }
      case 's': {
         if (OB_FAIL(databuff_printf(storage_info_str_, sizeof(storage_info_str_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy storage info str", K(ret));
        }
        break;
      }
      case 't': {
        if (OB_FAIL(c_str_to_int(optarg, config_.thread_num_))) {
            OB_LOG(WARN, "fail to parse thread num", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'r': {
        if (OB_FAIL(c_str_to_int(optarg, config_.max_task_runs_))) {
            OB_LOG(WARN, "fail to parse max task runs", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'l': {
        if (OB_FAIL(c_str_to_int(optarg, config_.time_limit_s_))) {
            OB_LOG(WARN, "fail to parse time limit", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'o': {
        if (OB_FAIL(c_str_to_int(optarg, config_.obj_size_))) {
            OB_LOG(WARN, "fail to parse object size", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'n': {
        if (OB_FAIL(c_str_to_int(optarg, config_.obj_num_))) {
            OB_LOG(WARN, "fail to parse object num", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'f': {
        if (OB_FAIL(c_str_to_int(optarg, config_.fragment_size_))) {
            OB_LOG(WARN, "fail to parse fragment size", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'p': {
        if (OB_ISNULL(optarg)) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "type is NULL", K(ret), K((char *)optarg));
        } else if (0 == STRCMP("write", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_NORMAL_WRITE;
        } else if (0 == STRCMP("append", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_APPEND_WRITE;
        } else if (0 == STRCMP("multi", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_MULTIPART_WRITE;
        } else if (0 == STRCMP("read", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_READ;
        } else if (0 == STRCMP("del", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_DEL;
        } else if (0 == STRCMP("is_exist", optarg)) {
          config_.type_ = BenchmarkTaskType::BENCHMARK_TASK_IS_EXIST;
        } else {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "unknown test type", K((char *)optarg), K(ret));
        }
        break;
      }
      case 'j': {
        config_.is_adaptive_ = true;
        break;
      }
      case 'b': {
        clean_before_execution_ = true;
        break;
      }
      case 'c': {
        clean_after_execution_ = true;
        break;
      }
      default: {
        print_usage_();
        exit(1);
      }
    }
  }

  char print_config_buf[OB_MAX_URI_LENGTH];
  config_.to_string(print_config_buf, sizeof(print_config_buf));
  OB_LOG(INFO, "Task Config", K_(config));
  PrintHelper::print_dump_line("Task Config", print_config_buf);
  return ret;
}

class CleanOp : public ObBaseDirEntryOperator
{
public:
  CleanOp(char *base_path, share::ObBackupStorageInfo *storage_info)
      : base_path_(base_path), storage_info_(storage_info), cleaned_objects_(0)
  {
    uri_[0] = '\0';
  }
  ~CleanOp() {}
  int func(const dirent *entry) override;

  char uri_[OB_MAX_URI_LENGTH];
  char *base_path_;
  share::ObBackupStorageInfo *storage_info_;
  int64_t cleaned_objects_;
};

int CleanOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;

  if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%s/%s", base_path_, entry->d_name))) {
    OB_LOG(WARN, "fail to set uri", K(ret), K_(uri), KPC_(storage_info));
  } else if (OB_FAIL(adapter.del_file(uri_, storage_info_))) {
    OB_LOG(WARN, "fail to delete file", K(ret), K_(uri), KPC_(storage_info));
  } else {
    cleaned_objects_++;
  }
  return ret;
}

int ObAdminIOAdapterBenchmarkExecutor::clean_base_path_(share::ObBackupStorageInfo &info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Cleaning");
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid storage info", K(ret), K(info));
  } else if (info.get_type() == ObStorageType::OB_STORAGE_FILE) {
    char cmd[OB_MAX_URI_LENGTH] = { 0 };
    if (OB_FAIL(databuff_printf(cmd, OB_MAX_URI_LENGTH,
                                "rm -rf %s/*", base_path_ + strlen(OB_FILE_PREFIX)))) {
      OB_LOG(WARN, "fail to fill clean cmd", K(ret), K_(base_path));
    } else if (0 != std::system(cmd)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "fail to delete dir", K(ret), K_(base_path), K(cmd));
    }
  } else {
    ObBackupIoAdapter adapter;
    CleanOp op(base_path_, &info);
    if (OB_FAIL(adapter.list_files(base_path_, &info, op))) {
      OB_LOG(WARN, "fail to clean", K(ret), K_(base_path), K(info));
    }
    PrintHelper::print_dump_line("Cleaned Objects", op.cleaned_objects_);
  }

  PrintHelper::print_dump_line("Clean Status", OB_SUCC(ret) ? "SUCCESS" : "FAIL");
  return ret;
}

int ObAdminIOAdapterBenchmarkExecutor::run_all_tests_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStorageInfo storage_info;
  ObBackupIoAdapterBenchmarkRunner runner;

  if (FALSE_IT(init_random_content())) {
  } else if (OB_FAIL(storage_info.set(base_path_, storage_info_str_))) {
    OB_LOG(WARN, "failed to set storage info", K(ret), K_(base_path));
  } else if (OB_FAIL(runner.init(base_path_, &storage_info, config_))) {
    OB_LOG(WARN, "fail to init ObBackupIoAdapterBenchmarkRunner",
        K(ret), K_(base_path), K(storage_info), K_(config));
  } else {
    if (clean_before_execution_) {
      if (OB_FAIL(clean_base_path_(storage_info))) {
        OB_LOG(WARN, "fail to clean base path", K(ret), K(storage_info), K_(base_path));
      }
    }

    if (OB_SUCC(ret)) {
      PrintHelper::print_dump_title("Testing");
      if (OB_FAIL(runner.do_benchmark())) {
        OB_LOG(WARN, "fail to do benchmark",
            K(ret), K_(base_path), K(storage_info), K_(config));
      }
    }

    if (OB_SUCC(ret) && clean_after_execution_) {
      if (OB_FAIL(clean_base_path_(storage_info))) {
        OB_LOG(WARN, "fail to clean base path", K(ret), K(storage_info), K_(base_path));
      }
    }
  }
  return ret;
}

int ObAdminIOAdapterBenchmarkExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  printf("Usage: bench_io_adapter command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h, --help", "display this message.");
  printf("options:\n");
  printf(HELP_FMT, "-d, --file-path-prefix", "absolute file path with file prefix");
  printf(HELP_FMT, "-s, --storage-info", "oss/cos should provide storage info");
  printf(HELP_FMT, "-t, --thread-num", "thread num");
  printf(HELP_FMT, "-r, --max-task-run-times", "max task run times for each thread");
  printf(HELP_FMT, "-l, --time-limit", "time limit in second");
  printf(HELP_FMT, "-o, --object-size", "object size");
  printf(HELP_FMT, "-n, --object-num", "object num");
  printf(HELP_FMT, "-f, --fragment-size",
      "for read operations, 'fragment-size' denotes the expected size of data to be read, "
      "while for append/multipart write tasks, it specifies the size of each individual pwrite operation.");
  printf(HELP_FMT, "-p, --type", "task type");
  printf(HELP_FMT, "-j, --is-adaptive", "use adative interface");
  printf(HELP_FMT, "-b, --clean-before-execution", "clean before execution");
  printf(HELP_FMT, "-c, --clean-after-execution", "clean after execution");
  printf("samples:\n");
  printf("  test nfs device: \n");
  printf("\tob_admin bench_io_adapter -dfile:///home/admin/backup_info \n");
  printf("  test object device: \n");
  printf("\tob_admin bench_io_adapter -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222'\n");
  printf("\tob_admin bench_io_adapter -d'cos://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&appid=333'\n");
  printf("\tob_admin bench_io_adapter -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&s3_region=333'\n");
  return ret;
}

/*--------------------------------ObBackupIoAdapterBenchmarkRunner--------------------------------*/
ObBackupIoAdapterBenchmarkRunner::ObBackupIoAdapterBenchmarkRunner()
    : lock_(), is_inited_(false), tg_id_(-1), ret_code_(OB_SUCCESS),
      config_(), metrics_(), storage_info_(nullptr)
{
}

ObBackupIoAdapterBenchmarkRunner::~ObBackupIoAdapterBenchmarkRunner()
{
  destroy();
}

int ObBackupIoAdapterBenchmarkRunner::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObBackupIoAdapterBenchmarkRunner init twice", K(ret));
  } else if (OB_ISNULL(base_uri) || OB_ISNULL(storage_info)
      || OB_UNLIKELY(!storage_info->is_valid() || config.thread_num_ <= 0)
      || OB_UNLIKELY(config.time_limit_s_ <= 0 && config.max_task_runs_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_FAIL(databuff_printf(base_uri_, sizeof(base_uri_), "%s", base_uri))) {
    OB_LOG(WARN, "fail to deep copy base uri", K(ret), K(base_uri));
  } else if (OB_FAIL(config_.assign(config))) {
    OB_LOG(WARN, "fail to assign task config", K(ret), K(config));
  } else {
    storage_info_ = storage_info;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupIoAdapterBenchmarkRunner::destroy()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    tg_id_ = -1;
  }
  is_inited_ = false;
}

int ObBackupIoAdapterBenchmarkRunner::do_benchmark()
{
  int ret = OB_SUCCESS;
  const int64_t thread_num = config_.thread_num_;
  const int64_t time_limit_s = config_.time_limit_s_;
  struct rusage start_usage;
  struct timeval start_real_time;
  getrusage(RUSAGE_SELF, &start_usage);
  gettimeofday(&start_real_time, nullptr);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObBackupIoAdapterBenchmarkRunner not init", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::COMMON_THREAD_POOL, tg_id_))) {
    OB_LOG(WARN, "create thread group failed", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    OB_LOG(WARN, "set tg_runnable failed", K(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_num))) {
    OB_LOG(WARN, "set thread count failed", K(ret), K_(tg_id), K(thread_num));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    OB_LOG(WARN, "start thread failed", K(ret), K_(tg_id), K(thread_num));
  } else {
    if (time_limit_s > 0) {
      sleep(time_limit_s);
      TG_STOP(tg_id_);
    }
    TG_WAIT(tg_id_);

    if (OB_SUCC(ret_code_)) {
      metrics_.summary(start_real_time, start_usage, config_.thread_num_);
    } else {
      OB_LOG(WARN, "some threads failed, check log", K_(ret_code), K(thread_num));
    }
  }
  return ret;
}

void ObBackupIoAdapterBenchmarkRunner::run1()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ITaskExecutor *executor = nullptr;
  const uint64_t thread_idx = get_thread_idx();
  char uri[common::OB_MAX_URI_LENGTH];

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObBackupIoAdapterBenchmarkRunner not init", K(ret));
  } else if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s/%ld", base_uri_, thread_idx))) {
    OB_LOG(WARN, "fail to construct base task dir for current thread",
        K(ret), K_(base_uri), K(thread_idx));
  } else if (OB_FAIL(util.mkdir(uri, storage_info_))) {
    OB_LOG(WARN, "fail to make base task dir for current thread",
        K(ret), K_(base_uri), K(thread_idx));
  } else if (OB_FAIL(init_task_executor(uri, storage_info_, config_, executor))) {
    OB_LOG(WARN, "fail to create and init task executor", K(ret), K(uri), K(thread_idx));
  } else {
    const bool is_limit_task_runs = (config_.max_task_runs_ > 0);
    for (int64_t i = 0; OB_SUCC(ret) && !has_set_stop(); i++) {
      if (is_limit_task_runs && i >= config_.max_task_runs_) {
        break;
      } else if (OB_FAIL(executor->execute())) {
        OB_LOG(WARN, "fail to execute task", K(ret), K(i), K(uri), K(thread_idx));
      }
    }

    if (OB_SUCC(ret)) {
      SpinWLockGuard guard(lock_);
      metrics_.add(executor->get_metrics());
    } else {
      ret_code_ = ret;
    }
  }

  if (OB_NOT_NULL(executor)) {
    executor->~ITaskExecutor();
    ob_free(executor);
    executor = nullptr;
  }
}

}   //tools
}   //oceanbase