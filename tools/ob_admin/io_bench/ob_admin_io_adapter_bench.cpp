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

#include "ob_admin_io_adapter_bench.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"
#include "src/share/io/ob_io_manager.h"
#include "src/share/ob_device_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{

ObAdminIOAdapterBenchmarkExecutor::ObAdminIOAdapterBenchmarkExecutor()
  : task_type_(BENCHMARK_TASK_AUTO_RUN),
    thread_num_(DEFAULT_THREAD_NUM),
    max_task_run_times_(DEFAULT_MAX_TASK_RUN_TIMES),
    time_limit_s_(DEFAULT_TIME_LIMIT_S),
    obj_size_(DEFAULT_OBJ_SIZE),
    obj_num_(DEFAULT_OBJ_NUM),
    fragment_size_(DEFAULT_FRAGMENT_SIZE),
    write_size_(DEFAULT_WRITE_SIZE),
    append_size_(DEFAULT_APPEND_SIZE),
    append_fragment_size_(DEFAULT_APPEND_FRAGMENT_SIZE),
    multi_size_(DEFAULT_MULTI_SIZE),
    multi_fragment_size_(DEFAULT_MULTI_FRAGMENT_SIZE),
    is_adaptive_(false),
    clean_after_execution_(false),
    configs_()
{
  MEMSET(base_path_, 0, sizeof(base_path_));
  MEMSET(storage_info_str_, 0, sizeof(storage_info_str_));
}

int ObAdminIOAdapterBenchmarkExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_cmd_(argc, argv))) {
    OB_LOG(WARN, "failed to parse cmd", K(ret), K(argc), K(argv));
  } else {
    const int64_t MEMORY_LIMIT = 16 * 1024 * 1024 * 1024LL;
    lib::set_memory_limit(MEMORY_LIMIT);
    lib::set_tenant_memory_limit(500, MEMORY_LIMIT);
    OB_LOGGER.set_log_level("INFO");

    ObTenantBase *tenant_base = new ObTenantBase(OB_SERVER_TENANT_ID);
    ObMallocAllocator *malloc = ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, 0))) {
      if (FAILEDx(malloc->create_and_add_tenant_allocator(OB_SERVER_TENANT_ID))) {
        STORAGE_LOG(WARN, "failed to create_and_add_tenant_allocator", K(ret));
      }
    }

    if (FAILEDx(tenant_base->init())) {
      STORAGE_LOG(WARN, "failed to init tenant base", K(ret));
    } else if (FALSE_IT(ObTenantEnv::set_tenant(tenant_base))) {
    } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
      STORAGE_LOG(WARN, "init device manager failed", KR(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().init(MEMORY_LIMIT))) {
      STORAGE_LOG(WARN, "failed to init io manager", K(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().start())) {
      STORAGE_LOG(WARN, "failed to start io manager", K(ret));
    } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_state_mgr(&ObClusterStateBaseMgr::get_instance()))) {
      STORAGE_LOG(WARN, "fail to register cluster version mgr", KR(ret));
    }

    ObRefHolder<ObTenantIOManager> tenant_holder;
    if (FAILEDx(OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, tenant_holder))) {
      STORAGE_LOG(WARN, "failed to get tenant io manager", K(ret));
    } else if (OB_FAIL(tenant_holder.get_ptr()->update_memory_pool(MEMORY_LIMIT))) {
      STORAGE_LOG(WARN, "failed to update memory pool", K(ret), K(MEMORY_LIMIT));
    }

    if (FAILEDx(run_all_tests_())) {
      OB_LOG(WARN, "failed to pass all tests", K(ret), K_(base_path));
    }
  }
  return ret;
}

int ObAdminIOAdapterBenchmarkExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "hd:s:t:r:l:o:n:f:p:cje:i:a";
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
      {"is-adaptive", 0, NULL, 'j'},
      {"clean-after-execution", 0, NULL, 'c'},
      {"s3_url_encode_type", 1, NULL, 'e'},
      {"sts_credential", 1, NULL, 'i'},
      {"enable_obdal", 0, NULL, 'a'},
      {"write-size", 1, NULL, '0'},
      {"append-size", 1, NULL, '0'},
      {"append-fragment-size", 1, NULL, '0'},
      {"multi-size", 1, NULL, '0'},
      {"multi-fragment-size", 1, NULL, '0'},
      {NULL, 0, NULL, 0}};
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        ret = OB_INVALID_ARGUMENT;
        print_usage_();
        break;
      }
      case 'd': {
        if (OB_FAIL(databuff_printf(base_path_, sizeof(base_path_), "%s", optarg))) {
          OB_LOG(WARN, "failed to databuff printf", K(ret));
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
        if (OB_FAIL(c_str_to_int(optarg, thread_num_))) {
            OB_LOG(WARN, "fail to parse thread num", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'r': {
        if (OB_FAIL(c_str_to_int(optarg, max_task_run_times_))) {
            OB_LOG(WARN, "fail to parse max task runs", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'l': {
        if (OB_FAIL(c_str_to_int(optarg, time_limit_s_))) {
            OB_LOG(WARN, "fail to parse time limit", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'o': {
        if (OB_FAIL(c_str_to_int(optarg, obj_size_))) {
            OB_LOG(WARN, "fail to parse object size", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'n': {
        if (OB_FAIL(c_str_to_int(optarg, obj_num_))) {
            OB_LOG(WARN, "fail to parse object num", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'f': {
        if (OB_FAIL(c_str_to_int(optarg, fragment_size_))) {
            OB_LOG(WARN, "fail to parse fragment size", K(ret), K((char *)optarg));
        }
        break;
      }
      case 'p': {
        if (OB_ISNULL(optarg)) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "type is NULL", K(ret), K((char *)optarg));
        } else if (0 == STRCMP("auto-run", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_AUTO_RUN;
        } else if (0 == STRCMP("write", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_NORMAL_WRITE;
        } else if (0 == STRCMP("append", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_APPEND_WRITE;
        } else if (0 == STRCMP("multi", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_MULTIPART_WRITE;
        } else if (0 == STRCMP("read", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_READ;
        } else if (0 == STRCMP("del", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_DEL;
        } else if (0 == STRCMP("is_exist", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_IS_EXIST;
        } else if (0 == STRCMP("read_user_provided", optarg)) {
          task_type_ = BenchmarkTaskType::BENCHMARK_TASK_READ_USER_PROVIDED;
        } else {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "unknown test type", K((char *)optarg), K(ret));
          printf("unknown test type: %s\n", optarg);
          print_usage_();
        }
        break;
      }
      case 'j': {
        is_adaptive_ = true;
        break;
      }
      case 'c': {
        clean_after_execution_ = true;
        break;
      }
      case 'e': {
        if (OB_FAIL(set_s3_url_encode_type(optarg))) {
          STORAGE_LOG(WARN, "failed to set s3 url encode type", KR(ret));
        }
        break;
      }
      case 'i': {
        if (OB_FAIL(set_sts_credential_key(optarg))) {
          STORAGE_LOG(WARN, "failed to set sts credential", KR(ret));
        }
        break;
      }
      case 'a': {
        // 必须在 ObDeviceManager::get_instance().init_devices_env() 之后执行
        cluster_enable_obdal_config = &ObClusterEnableObdalConfigBase::get_instance();
        break;
      }
      case '0': {
        if (index >= 0) {
          const char *opt_name = longopts[index].name;
          if (strcmp(opt_name, "write-size") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, write_size_))) {
              OB_LOG(WARN, "failed to parse write size", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "append-size") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, append_size_))) {
              OB_LOG(WARN, "failed to parse append size", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "append-fragment-size") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, append_fragment_size_))) {
              OB_LOG(WARN, "failed to parse append fragment size", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "multi-size") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, multi_size_))) {
              OB_LOG(WARN, "failed to parse multi size", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "multi-fragment-size") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, multi_fragment_size_))) {
              OB_LOG(WARN, "failed to parse multi fragment size", KR(ret), K((char *) optarg));
            }
          }
          break;
        } else {
          ret = OB_INVALID_ARGUMENT;
          print_usage_();
          break;
        }
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        print_usage_();
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (task_type_ == BENCHMARK_TASK_AUTO_RUN) {
      // append timestamp to base_path_
      time_t timestamp = time(NULL);
      struct tm *timeinfo = localtime(&timestamp);
      char time_buffer[OB_MAX_TIME_STR_LENGTH];
      strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d-%H:%M:%S", timeinfo);
      int64_t base_path_len = strlen(base_path_);
      if (is_end_with_slash(base_path_)) {
        if (OB_FAIL(databuff_printf(base_path_, sizeof(base_path_), base_path_len, "test_io_device_%s", time_buffer))) {
          OB_LOG(WARN, "failed to databuff printf", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(base_path_, sizeof(base_path_), base_path_len, "/test_io_device_%s", time_buffer))) {
          OB_LOG(WARN, "failed to databuff printf", K(ret));
        }
      }

      TaskConfig write_config(thread_num_, max_task_run_times_, time_limit_s_,
          write_size_, obj_num_, -1, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_NORMAL_WRITE, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_NORMAL_WRITE]);
      if (FAILEDx(databuff_printf(write_config.base_path_, sizeof(write_config.base_path_), "%s/write", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(write_config);

      TaskConfig write_read_16K_config(thread_num_, max_task_run_times_, time_limit_s_,
          write_size_, obj_num_, READ_16K_SIZE, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_READ, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_READ]);
      if (FAILEDx(databuff_printf(write_read_16K_config.base_path_, sizeof(write_read_16K_config.base_path_), "%s/write", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(write_read_16K_config);

      TaskConfig write_read_2M_config(thread_num_, max_task_run_times_, time_limit_s_,
          write_size_, obj_num_, READ_2M_SIZE, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_READ, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_READ]);
      if (FAILEDx(databuff_printf(write_read_2M_config.base_path_, sizeof(write_read_2M_config.base_path_), "%s/write", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(write_read_2M_config);

      TaskConfig append_config(thread_num_, max_task_run_times_, time_limit_s_,
          append_size_, obj_num_, append_fragment_size_, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_APPEND_WRITE, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_APPEND_WRITE]);
      if (FAILEDx(databuff_printf(append_config.base_path_, sizeof(append_config.base_path_), "%s/append", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(append_config);

      TaskConfig append_read_2M_config(thread_num_, max_task_run_times_, time_limit_s_,
          append_size_, obj_num_, READ_2M_SIZE, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_READ, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_READ]);
      if (FAILEDx(databuff_printf(append_read_2M_config.base_path_, sizeof(append_read_2M_config.base_path_), "%s/append", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(append_read_2M_config);

      TaskConfig multi_config(thread_num_, max_task_run_times_, time_limit_s_,
          multi_size_, obj_num_, multi_fragment_size_, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_MULTIPART_WRITE, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_MULTIPART_WRITE]);
      if (FAILEDx(databuff_printf(multi_config.base_path_, sizeof(multi_config.base_path_), "%s/multi", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(multi_config);

      TaskConfig multi_read_2M_config(thread_num_, max_task_run_times_, time_limit_s_,
          multi_size_, obj_num_, READ_2M_SIZE, is_adaptive_,
          BenchmarkTaskType::BENCHMARK_TASK_READ, BenchmarkTaskTypeStr[BenchmarkTaskType::BENCHMARK_TASK_READ]);
      if (FAILEDx(databuff_printf(multi_read_2M_config.base_path_, sizeof(multi_read_2M_config.base_path_), "%s/multi", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(multi_read_2M_config);
    } else if (OB_UNLIKELY(task_type_ < BENCHMARK_TASK_AUTO_RUN || task_type_ >= BENCHMARK_TASK_MAX_TYPE)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid task type", K(ret), K(task_type_));
      print_usage_();
      exit(1);
    } else {
      TaskConfig config(thread_num_, max_task_run_times_, time_limit_s_,
          obj_size_, obj_num_, fragment_size_, is_adaptive_,
          task_type_, BenchmarkTaskTypeStr[task_type_]);
      if (FAILEDx(databuff_printf(config.base_path_, sizeof(config.base_path_), "%s", base_path_))) {
        OB_LOG(WARN, "failed to databuff printf", K(ret));
      }
      configs_.push_back(config);
    }
  }

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

  if (OB_ISNULL(storage_info_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "storage_info_ is null", K(ret), KP(storage_info_));
  } else if (OB_UNLIKELY(storage_info_->is_enable_worm()
                && ObStorageDeleteMode::STORAGE_DELETE_MODE == storage_info_->get_delete_mode())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "worm bucket can not do deleting opeartion", K(ret), KPC(storage_info_));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%s/%s", base_path_, entry->d_name))) {
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
  ObBackupIoAdapter adapter;
  bool is_empty = false;

  if (FALSE_IT(init_random_content())) {
  } else if (OB_FAIL(storage_info.set(base_path_, storage_info_str_))) {
    OB_LOG(WARN, "failed to set storage info", K(ret), K_(base_path));
    std::cerr << LIGHT_RED << "[ERROR] " << "storage info error, please check your -d and -s parameters" << NONE_COLOR << std::endl;
  } else if (task_type_ == BENCHMARK_TASK_NORMAL_WRITE ||
             task_type_ == BENCHMARK_TASK_APPEND_WRITE ||
             task_type_ == BENCHMARK_TASK_MULTIPART_WRITE) {
    if (OB_FAIL(adapter.is_empty_directory(base_path_, &storage_info, is_empty))) {
      OB_LOG(WARN, "failed to check empty path", K(ret));
      std::cout << LIGHT_RED << "[ERROR] " << "failed to check empty path, please make sure the destination is connected" << NONE_COLOR << std::endl;
    } else if (!is_empty) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "when the task type is write, append, or multi, the path should be an empty directory.", K(ret), K(base_path_), K(task_type_));
      std::cout << LIGHT_RED << "[ERROR] " << "When the task type is write, append, or multi, the path should be an empty directory." << NONE_COLOR << std::endl;
    }
  } else {
    // used for check connected
    if (OB_FAIL(adapter.is_empty_directory(base_path_, &storage_info, is_empty))) {
      OB_LOG(WARN, "failed to check empty path", K(ret));
      std::cout << LIGHT_RED << "[ERROR] " << "failed to check empty path, please make sure the destination is connected" << NONE_COLOR << std::endl;
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < configs_.size(); i++) {
    if (OB_FAIL(runner.init(configs_[i].base_path_, &storage_info, configs_[i]))) {
      OB_LOG(WARN, "failed to init ObBackupIoAdapterBenchmarkRunner", K(ret), K(base_path_), K(storage_info), K(configs_[i]));
    } else {
      char print_config_buf[OB_MAX_URI_LENGTH];
      configs_[i].to_string(print_config_buf, sizeof(print_config_buf));
      OB_LOG(INFO, "Task Config", K(configs_[i]));
      OB_LOG(INFO, "Task work path", K(configs_[i].base_path_));
      PrintHelper::print_dump_title("Testing");
      PrintHelper::print_dump_line("Task Config", print_config_buf);
      PrintHelper::print_dump_line("Task work path", configs_[i].base_path_);


      if (FAILEDx(runner.do_benchmark())) {
        OB_LOG(WARN, "failed to do benchmark",
            K(ret), K_(base_path), K(storage_info), K(configs_[i]));
      }
      runner.destroy();
    }
  }

  if (OB_SUCC(ret) && clean_after_execution_) {
    if (OB_FAIL(clean_base_path_(storage_info))) {
      OB_LOG(WARN, "fail to clean base path", K(ret), K(storage_info), K_(base_path));
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
  printf(HELP_FMT, "-d, --file-path-prefix", "required, absolute file path with file prefix");
  printf(HELP_FMT, "-s, --storage-info", "required, oss/cos should provide storage info");
  printf(HELP_FMT, "-p, --type", "specifies the test type, the values can be auto-run, write, read, multi, read, del, is_exist, read_user_provided");
  printf(HELP_FMT, "-t, --thread-num", "thread num");
  printf(HELP_FMT, "-r, --max-task-run-times", "max task run times for each thread, default is 10");
  printf(HELP_FMT, "-l, --time-limit", "time limit in second");
  printf(HELP_FMT, "-o, --object-size", "object size, default is 2097152, 2M");
  printf(HELP_FMT, "-n, --object-num", "object num, default is 10");
  printf(HELP_FMT, "-f, --fragment-size",
      "for read operations, 'fragment-size' denotes the expected size of data to be read, "
      "while for append/multipart write tasks, it specifies the size of each individual pwrite operation.");
  printf(HELP_FMT, "--write-size", "specifies the object size in put task when type is auto-run, default is 2097152, 2M");
  printf(HELP_FMT, "--append-size", "specifies the object size in append-write task when type is auto-run, default is 67108864, 64M");
  printf(HELP_FMT, "--append-fragment-size", "specifies the fragment size in append-write task when type is auto-run, default is 0, denote each time random");
  printf(HELP_FMT, "--multi-size", "specifies the object size in multi-write task when type is auto-run, default is 67108864, 64M");
  printf(HELP_FMT, "--multi-fragment-size", "specifies the fragment size in multi-write task when type is auto-run, default is 2097152, 2M");
  printf(HELP_FMT, "-j, --is-adaptive", "use adative interface");
  printf(HELP_FMT, "-c, --clean-after-execution", "clean after execution");
  printf(HELP_FMT, "-e, --s3_url_encode_type", "set S3 protocol url encode type");
  printf(HELP_FMT, "-i, --sts_credential", "set sts credential");
  printf(HELP_FMT, "-a", "enable obdal");
  printf("samples:\n");
  printf("  test nfs device: \n");
  printf("\tob_admin io_adapter_benchmark -dfile:///home/admin/backup_info \n");
  printf("  test object device: \n");
  printf("\tob_admin io_adapter_benchmark -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222'\n");
  printf("\tob_admin io_adapter_benchmark -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&region=333'\t"
         "-e'compliantRfc3986Encoding'\n");
  printf("\tob_admin io_adapter_benchmark -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&role_arn=xxx'"
         "-i'sts_url=xxx&sts_ak=aaa&sts_sk=bbb'\n");
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
    std::cerr << LIGHT_RED << "[ERROR] please check your thread_num, time_limit_s and mas_task_runs" << NONE_COLOR << std::endl;
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
      metrics_.reset();
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
  } else if (BenchmarkTaskType::BENCHMARK_TASK_READ_USER_PROVIDED == config_.type_) {
    if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s", base_uri_))) {
      OB_LOG(WARN, "fail to copy file uri", K(ret), K_(base_uri), K(thread_idx));
    }
  } else if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s/%ld", base_uri_, thread_idx))) {
    OB_LOG(WARN, "fail to construct base task dir for current thread",
        K(ret), K_(base_uri), K(thread_idx));
  } else if (OB_FAIL(util.mkdir(uri, storage_info_))) {
    OB_LOG(WARN, "fail to make base task dir for current thread",
        K(ret), K_(base_uri), K(thread_idx));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(BenchmarkTaskType::BENCHMARK_TASK_DEL == config_.type_ && storage_info_->is_enable_worm()
                && ObStorageDeleteMode::STORAGE_DELETE_MODE == storage_info_->get_delete_mode())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "worm bucket can not do deleting opeartion", K(ret), KPC(storage_info_));
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
