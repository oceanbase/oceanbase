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

#include <stdlib.h>
#include <getopt.h>
#include "share/ob_version.h"
#include "lib/file/file_directory_utils.h"
#include "ob_storage_perf_thread.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_tenant_mgr.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storageperf;
using namespace oceanbase::sql;
using namespace oceanbase::obsys;



#define MPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#define MPRINTx(format, ...) MPRINT(format, ##__VA_ARGS__); exit(1)

ObArray<int64_t> read_cols;
const char *schema_file = "./storage_perf_cost.schema";
const char *u_schema = "./storage_perf_update.schema";
ObStoragePerfConfig config;

struct ObOptions
{
  const char *optstr_;
  const char *config_file_;
  int8_t log_level_;
};

static void print_help()
{
  MPRINT("observer [OPTIONS]");
  MPRINT("  -h,--help                print this help");
  MPRINT("  -o,--optstr OPTSTR      extra options string");
  MPRINT("  -f,--config_file        config_file");
  MPRINT("  -l,--log_level LOG_LEVEL server log level");
}

static void print_version()
{
  MPRINT("ob_storage_test (%s %s)", PACKAGE_STRING, RELEASEID);
  MPRINT("REVISION: %s", build_version());
  MPRINT("BUILD_TIME: %s %s", build_date(), build_time());
  MPRINT("BUILD_FLAGS: %s\n", build_flags());
  MPRINT("Copyright (c) 2007-2015 Alipay Inc.");
}


static void print_args(int argc, char *argv[])
{
  for (int i = 0; i < argc - 1; ++i) {
    fprintf(stderr, "%s ", argv[i]);
  }
  fprintf(stderr, "%s\n", argv[argc - 1]);
}

static void get_opts_setting(struct option long_opts[],
                             char short_opts[],
                             const size_t size)
{
  static struct {
    const char *long_name_;
    char short_name_;
    bool has_arg_;
  } ob_opts[] = {
    {"help", 'h', 0},
    {"log_level", 'l', 1},
    {"optstr", 'o', 1},
    {"version", 'V', 0},
    {"config_file", 'f', 1},
    {"read_columns", 'c', 1},
    {"read_columns_count", 'C', 1},
    {"schema_file", 's', 1},
    {"update_schema_file", 'u', 1},
    {"row_count", 'r', 1},
    {"output_row", 'O', 0},
    {"perf_stat", 'S', 0},
    {"regenerate_sstable", 'R', 0},
    {"repeat_times", 'T', 1},
    {"print_stat_info", 'I', 0},
    {"flush_block_index_cache", 'Q', 0},
    {"flush_block_cache", 'W', 0},
    {"flush_row_cache", 'E', 0},
    {"WARNING", 'A', 1},
    {"bind", 'B', 0},
    {"test type", 'Y', 1},
    {"multiget range", 'U', 1},
    {"no unittest mode", 'G', 0},
  };

  size_t opts_cnt = sizeof (ob_opts) / sizeof (ob_opts[0]);

  if (opts_cnt >= size) {
    MPRINTx("parse option fail: opts array is too small");
  }

  int short_idx = 0;

  for (size_t i = 0; i < opts_cnt; ++i) {
    long_opts[i].name = ob_opts[i].long_name_;
    long_opts[i].has_arg = ob_opts[i].has_arg_;
    long_opts[i].flag = NULL;
    long_opts[i].val = ob_opts[i].short_name_;

    short_opts[short_idx++] = ob_opts[i].short_name_;
    if (ob_opts[i].has_arg_) {
      short_opts[short_idx++] = ':';
    }
  }
}

void set_rt_and_bind_cpu()
{
  cpu_set_t cpu_set;
  struct sched_param param;
  CPU_ZERO(&cpu_set);
  CPU_SET(5, &cpu_set);
  if(sched_setaffinity(static_cast<pid_t>(gettid()), sizeof(cpu_set), &cpu_set) == -1) {
    perror("sched_setaffinity() error!\n");
    exit(1);
  }
  param.sched_priority = 99;
  if(sched_setscheduler(static_cast<pid_t>(gettid()), SCHED_FIFO, &param) == -1){
    perror("sched_setscheduler() error!\n");
    exit(1);
  }
}

#define STRTOL_ERR(val) ((errno == ERANGE && ((val) == LONG_MAX || (val) == LONG_MIN)))
static void parse_short_opt(const int c, const char *value, ObOptions &opts, ObStoragePerfConfig& config)
{
  switch (c) {
     case 'o':
      MPRINT("optstr: %s", value);
      opts.optstr_ = value;
      break;
    case 'l':
      MPRINT("log level: %s", value);
      if (OB_SUCCESS != OB_LOGGER.level_str2int(value, opts.log_level_)) {
        MPRINT("malformed log level, candicates are: "
               "    ERROR,USER_ERR,WARN,INFO,TRACE,DEBUG");
        MPRINT("!! Back to INFO log level.");
        opts.log_level_ = 3;
      }
      break;
    case 'V':
      print_version();
      exit(0);
      break;
    case 'f':
      opts.config_file_ = value;
      break;
    case 'B':
      set_rt_and_bind_cpu();
      break;
    case 'c':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        int64_t col_id = 0;
        char *opt_arg_end = NULL;
        col_id = strtol(value, &opt_arg_end, 10);
        if (STRTOL_ERR(col_id)) {
          MPRINT("invalid col id");
        } else {
          read_cols.push_back(col_id);
        }
      }
      break;
    case 'A':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        config.log_level_ = value;
      }
      break;
    case 's':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        schema_file = value;
      }
      break;
    case 'u':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        u_schema = value;
      }
      break;
    case 'O':
      config.print_row_ = true;
      break;
    case 'S':
      config.test_single_row_speed_ = true;
      break;
    case 'C':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        int64_t col_count = 0;
        char *opt_arg_end = NULL;
        col_count = strtol(value, &opt_arg_end, 10);
        if (STRTOL_ERR(col_count)) {
          MPRINT("invalid col id");
        } else {
          config.read_columns_count_ = col_count;
          for (int64_t i = 0; i < col_count; ++i) {
            read_cols.push_back(i);
          }
        }
      }
      break;
    case 'Y':
      if (NULL == value) {
        MPRINT("invalid test type");
      } else {
        if ('S' == value[0]) {
          config.scan_thread_count_ = 1;
          config.multi_get_thread_count_ = 0;
          config.single_get_thread_count_ = 0;
        } else if ('M' == value[0]) {
          config.scan_thread_count_ = 0;
          config.multi_get_thread_count_ = 1;
          config.single_get_thread_count_ = 0;
        } else if ('G' == value[0]) {
          config.scan_thread_count_ = 0;
          config.multi_get_thread_count_ = 0;
          config.single_get_thread_count_ = 1;
        }
      }
      break;
    case 'r':
      if (NULL == value) {
        MPRINT("invalid col id");
      } else {
        int64_t row_count = 0;
        char *opt_arg_end = NULL;
        row_count = strtol(value, &opt_arg_end, 10);
        if (STRTOL_ERR(row_count)) {
          MPRINT("invalid col id");
        } else {
          config.total_single_row_count_ = row_count;
          config.total_multi_row_count_ = row_count;
          config.total_scan_row_count_ = row_count;
        }
      }
      break;
    case 'T':
      if (NULL == value) {
        MPRINT("invalid repeat time");
      } else {
        int64_t repeat_time = 0;
        char *opt_arg_end = NULL;
        repeat_time = strtol(value, &opt_arg_end, 10);
        if (STRTOL_ERR(repeat_time)) {
          MPRINT("invalid col id");
        } else {
          config.scan_run_ = static_cast<int>(repeat_time);
          config.single_get_times_ = repeat_time;
          config.multi_get_run_ = static_cast<int>(repeat_time);

          config.scan_times_ = 1;
          config.multi_get_times_ = 1;
        }
      }
      break;
    case 'U':
      if (NULL == value) {
        MPRINT("invalid repeat time");
      } else {
        int64_t mg_range = 0;
        char *opt_arg_end = NULL;
        mg_range = strtol(value, &opt_arg_end, 10);
        if (STRTOL_ERR(mg_range)) {
          MPRINT("invalid col id");
        } else {
          config.get_range_ = mg_range;
        }
      }
      break;
    case 'R':
      config.is_test_write_ = 1;
      config.total_partition_num_ = 1;
      break;
    case 'I':
      config.print_perf_stat_ = true;
      break;
    case 'Q':
      config.flush_block_index_cache_ = true;
      break;
    case 'W':
      config.flush_block_cache_ = true;
      break;
    case 'E':
      config.flush_row_cache_ = true;
      break;
    case 'G':
      config.unittest_mode_ = false;
      break;
    case 'h':
    /*
     * {"flush_block_index_cache", 'Q', 0},
    {"flush_block_cache", 'W', 0},
    {"flush_row_cache", 'E', 0},*/

    default:
      print_help();
      exit(1);
  }
}

static void parse_opts(int argc, char *argv[], ObOptions &opts, ObStoragePerfConfig& config)
{
  static const int MAX_OPTS_CNT = 128;
  static char short_opts[MAX_OPTS_CNT*2+1];
  static struct option long_opts[MAX_OPTS_CNT];

  get_opts_setting(long_opts, short_opts, MAX_OPTS_CNT);

  int long_opts_idx = 0;

  while (1) {
    int c = getopt_long(argc, argv, short_opts, long_opts, &long_opts_idx);

    if (c == -1) {  // end
      break;
    }

    parse_short_opt(c, optarg, opts, config);
  }
}

int run_test() {
  int ret = OB_SUCCESS;

  char log_name[OB_MAX_FILE_NAME_LENGTH];

  ObRestoreSchema restore_schema;
  ObSchemaGetterGuard *schema_guard = NULL;
  MockSchemaService *schema_service = NULL;
  if (OB_FAIL(restore_schema.init())) {
    STORAGE_LOG(WARN, "fail to init schema service", K(ret));
  } else if (OB_FAIL(restore_schema.parse_from_file(schema_file, schema_guard))) {
    STORAGE_LOG(WARN, "fail to parse schema from file", K(ret));
  } else if(OB_FAIL(FileDirectoryUtils::create_full_path(const_cast<char *>(config.get_perf_root_dir())))){
    STORAGE_LOG(WARN, "fail to mkdir", K(config.get_perf_root_dir()));
    ret = OB_ERR_UNEXPECTED;
  } else if(OB_FAIL(FileDirectoryUtils::create_full_path(const_cast<char *>(config.get_slog_dir())))){
    STORAGE_LOG(WARN, "fail to mkdir", K(config.get_slog_dir()));
    ret = OB_ERR_UNEXPECTED;
  } else if(OB_FAIL(FileDirectoryUtils::create_full_path(const_cast<char *>(config.get_sstable_meta_dir())))){
    STORAGE_LOG(WARN, "fail to mkdir", K(config.get_sstable_meta_dir()));
    ret = OB_ERR_UNEXPECTED;
  } else if(OB_FAIL(FileDirectoryUtils::create_full_path(const_cast<char *>(config.get_sstable_data_dir())))){
    STORAGE_LOG(WARN, "fail to mkdir", K(config.get_sstable_data_dir()));
    ret = OB_ERR_UNEXPECTED;
  } else if(OB_FAIL(FileDirectoryUtils::create_full_path(const_cast<char *>(config.get_perf_log_dir())))){
    STORAGE_LOG(WARN, "fail to mkdir", K(config.get_perf_log_dir()));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int n = snprintf(log_name, OB_MAX_FILE_NAME_LENGTH, "%s/storage_perf.log", config.get_perf_log_dir());
    if(n < 0 || n > OB_MAX_FILE_NAME_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "fail to create log name", K(config.get_perf_root_dir()), K(OB_MAX_FILE_NAME_LENGTH));
    } else {
      if (NULL == config.log_level_) {
        config.log_level_ = "ERROR";
      }
      OB_LOGGER.set_log_level(config.log_level_);
      OB_LOGGER.set_max_file_size(256L * 1024L * 1024L);
      OB_LOGGER.set_file_name(log_name, true);
    }
    schema_service = restore_schema.schema_service_;
  }

  if(OB_SUCC(ret)) {
    ObAddr self;
    oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
    obrpc::ObSrvRpcProxy rpc_proxy;
    if(!self.set_ip_addr("127.0.0.1", 8086)){
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to set ipv4");
    } else if (OB_SUCCESS != (ret = ObTenantManager::get_instance().init(self, rpc_proxy,
                                      &req_transport, &ObServerConfig::get_instance()))){
      STORAGE_LOG(WARN, "fail to init tenant manager", K(ret));
    } else if (OB_SUCCESS != (ret = ObTenantManager::get_instance().add_tenant(tenant_id))){
      STORAGE_LOG(WARN, "fail to add tenant_id", K(ret), K(tenant_id));
    } else if (OB_SUCCESS != (ret =  ObTenantManager::get_instance().set_tenant_mem_limit(tenant_id,
        config.get_tenant_cache_size() * 1024L * 1024L/2,
        config.get_tenant_cache_size() * 1024L * 1024L))) {
      STORAGE_LOG(WARN, "fail to set tenant memory", K(ret));
    } else  if(OB_FAIL(ObIOManager::get_instance().init(1L * 1024L * 1024L * 1024L))) {
      STORAGE_LOG(WARN, "fail to init ObIOMananger", K(ret));
    } else if(OB_SUCCESS != (ret = ObKVGlobalCache::get_instance().init())){
      STORAGE_LOG(WARN, "fail to init global kv cache", K(ret));
    } else {
      lib::set_memory_limit(10L * 1024L * 1024L * 1024L);//40GB
      lib::ob_set_reserved_memory(1024L * 1024L * 1024L);//1GB
    }
  }

  if (OB_SUCC(ret)) {
    //multi write thread speed test
    if(!config.is_test_write()) {
      STORAGE_LOG(WARN, "no thread, cannot start write bench");
    } else {
      const int thread_no = config.get_total_partition_num();//one partition one write thread
      MultiThreadWrite write(thread_no);
      if(OB_FAIL(write.init(schema_service, &restore_schema, config))){
        STORAGE_LOG(WARN, "fail to init query", K(ret));
      } else {
        const int64_t begin = ObTimeUtility::current_time();
        int64_t pos = 0;
        char time_string_buf[max_time_string_length+1];
        if (OB_FAIL(ObTimeUtility::usec_format_to_str(begin, bianque_format, &time_string_buf[0], max_time_string_length, pos))) {
          STORAGE_LOG(WARN, "failed to convert bianque format string", K(ret));
        } else {
          time_string_buf[pos] = '\0';
          _OB_LOG(ERROR, "write_begin_time: %s", time_string_buf);
          write.start();
          write.wait();
          const int64_t end = ObTimeUtility::current_time();
          if (OB_FAIL(write.get_first_error())) {
            STORAGE_LOG(WARN, "failed to run write test", K(ret));
          } else {
            pos = 0;
            if (OB_FAIL(ObTimeUtility::usec_format_to_str(end, bianque_format, &time_string_buf[0], max_time_string_length, pos))) {
              STORAGE_LOG(WARN, "failed to convert bianque format string", K(ret));
            } else {
              time_string_buf[pos] = '\0';
              _OB_LOG(ERROR, "write_end_time: %s", time_string_buf);
              const int64_t duration = end - begin;
              const int64_t total_write_size_mb = write.get_total_macro_num() * 2L;
              _OB_LOG(ERROR, "total write thread %d , total time %ld us, total size  %ld MB, speed is %.4LF MB/s",
                  thread_no, duration, total_write_size_mb, 1000L * 1000L * (total_write_size_mb/static_cast<long double>(duration)));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    //multi thread single get speed test
    const int partition_num = config.get_total_partition_num();
    const int thread_no = config.get_single_get_thread_count() * partition_num;
    if(thread_no <= 0) {
      STORAGE_LOG(WARN, "no thread, cannot start single get bench");
    } else {
      MultiThreadSingleGet single_get(thread_no);
      if (NULL != u_schema) {
        single_get.set_update_schema(u_schema);
      }
      if(OB_FAIL(single_get.init(schema_service, &restore_schema, config))){
        STORAGE_LOG(WARN, "fail to init query", K(ret));
      } else {
        single_get.assign_read_cols(read_cols);
        const int64_t begin = ObTimeUtility::current_time();
        single_get.start();
        single_get.wait();
        const int64_t end = ObTimeUtility::current_time();
        if (OB_FAIL(single_get.get_first_error())) {
          STORAGE_LOG(WARN, "failed to run single get test", K(ret));
        } else {
          const int64_t duration = end - begin;
          const int64_t total_get_row = thread_no * config.get_total_single_row_count();
          _OB_LOG(ERROR, "total single get thread %d , total time %ld us, total single row count %ld speed is %.4LF us/row",
              thread_no, duration, total_get_row, duration/static_cast<long double>(total_get_row));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    //multi thread multi get speed test
    const int partition_num = config.get_total_partition_num();
    int thread_no = config.get_multi_get_thread_count() * partition_num;
    if(thread_no <= 0) {
      STORAGE_LOG(WARN, "no thread, cannot start multi get bench");
    } else {
      MultiThreadMultiGet multi_get(thread_no);
      if (NULL != u_schema) {
        multi_get.set_update_schema(u_schema);
      }
      if(OB_FAIL(multi_get.init(schema_service, &restore_schema, config))){
        STORAGE_LOG(WARN, "fail to init query", K(ret));
      } else {
        multi_get.assign_read_cols(read_cols);
        const int64_t begin = ObTimeUtility::current_time();
        multi_get.start();
        multi_get.wait();
        const int64_t end = ObTimeUtility::current_time();
        if (OB_FAIL(multi_get.get_first_error())) {
          STORAGE_LOG(WARN, "failed to run multi get test", K(ret));
        } else {
          const int64_t duration = end - begin;
          const int64_t total_get_row = thread_no * config.get_total_multi_row_count();
          _OB_LOG(ERROR, "total multi get thread %d , total time %ld us, total multi row count %ld speed is %.4LF us/row",
              thread_no, duration, total_get_row, duration/static_cast<long double>(total_get_row));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    //multi thread scan speed test
    const int partition_num = config.get_total_partition_num();
    int thread_no = config.get_scan_thread_count() * partition_num;
    if(thread_no <= 0) {
      STORAGE_LOG(WARN, "no thread, cannot start scan speed bench");
    } else {
      MultiThreadScan scan(thread_no);
      if (NULL != u_schema) {
        scan.set_update_schema(u_schema);
      }
      if(OB_FAIL(scan.init(schema_service, &restore_schema, config))){
        STORAGE_LOG(WARN, "fail to init query", K(ret));
      } else {
        scan.assign_read_cols(read_cols);
        //every thread has different num of row count, no need to cal total speed
        scan.start();
        scan.wait();
        if (OB_FAIL(scan.get_first_error())) {
          STORAGE_LOG(WARN, "failed to run scan test", K(ret));
        }
      }
    }
  }

  //No matter what, we need to release the space occupied
  if (config.unittest_mode_) {
    ObStoragePerfWrite write;
    if(OB_FAIL(write.init(&config, 0, &restore_schema, schema_service))){
      STORAGE_LOG(WARN, "fail to init", K(ret));
    } else {
      write.cleanup_sstable();
    }
  }

  //global destory
  ObIOManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTenantManager::get_instance().destroy();
  return ret;
}

TEST(StorageCostModelUtil, func_check) {
  int ret = OB_SUCCESS;
  //set up unittest mode params
  config.is_test_write_ = 1;
  config.partition_size_ = 1;
  config.total_multi_row_count_ = 10;
  config.total_single_row_count_ = 1;
  config.total_multi_row_count_ = 100;
  config.total_scan_row_count_ = 1000;
  config.get_range_ = 1000;
  config.scan_run_ = 1;
  config.single_get_times_ = 1;
  config.multi_get_run_ = 1;
  config.scan_times_ = 1;
  config.multi_get_times_ = 1;
  config.single_get_thread_count_ = 1;
  config.multi_get_thread_count_ = 1;
  config.scan_thread_count_ = 1;
  config.read_columns_count_ = 10;
  for (int64_t i = 0; i < config.read_columns_count_; ++i) {
    read_cols.push_back(i);
  }
  ret = run_test();
  EXPECT_EQ(ret, OB_SUCCESS);
}

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  print_args(argc, argv);
  ObOptions opts;
  if(OB_FAIL(config.init("./storage_perf.conf"))){
    STORAGE_LOG(WARN, "fail to get config file", K(ret));
    exit(1);
  }
  parse_opts(argc, argv, opts, config);
  if (config.unittest_mode_) {
    int argc_f = 1;
    char arg_f[] = "fake";
    char *argv[] = {arg_f};
    testing::InitGoogleTest(&argc_f, argv);
    return RUN_ALL_TESTS();
  } else {
    ret = run_test();
  }
  return 0;
}
