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

#define USING_LOG_PREFIX SERVER

#include "lib/alloc/malloc_hook.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/file/file_directory_utils.h"
#include "lib/oblog/ob_easy_log.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_libeasy_mem_pool.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/utility/ob_defer.h"
#include "objit/ob_llvm_symbolizer.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_utils.h"
#include "share/config/ob_server_config.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_version.h"
#include <curl/curl.h>
#include <getopt.h>
#include <locale.h>
#include <malloc.h>
#include <sys/time.h>
#include <sys/resource.h>
// easy complains in compiling if put the right position.
#include <link.h>
#include "sql/monitor/ob_security_audit_utils.h"

#ifdef __NEED_PERF__
#include "lib/profile/gperf.h"
#endif

using namespace oceanbase::obsys;
using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::diagnose;
using namespace oceanbase::observer;
using namespace oceanbase::share;

#define MPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#define MPRINTx(format, ...)                                                   \
  MPRINT(format, ##__VA_ARGS__);                                               \
  exit(1)

static void print_help()
{
  MPRINT("observer [OPTIONS]");
  MPRINT("  -h,--help                print this help");
  MPRINT("  -z,--zone ZONE           zone");
  MPRINT("  -p,--mysql_port PORT     mysql port");
  MPRINT("  -P,--rpc_port PORT       rpc port");
  MPRINT("  -N,--nodaemon            don't run in daemon");
  MPRINT("  -n,--appname APPNAME     application name");
  MPRINT("  -c,--cluster_id ID       cluster id");
  MPRINT("  -d,--data_dir DIR        OceanBase data directory");
  MPRINT("  -i,--devname DEV         net dev interface");
  MPRINT("  -I,--local_ip            ip of the current machine");
  MPRINT("  -o,--optstr OPTSTR       extra options string");
  MPRINT("  -r,--rs_list RS_LIST     root service list");
  MPRINT("  -l,--log_level LOG_LEVEL server log level");
  MPRINT("  -6,--ipv6 USE_IPV6       server use ipv6 address");
  MPRINT("  -m,--mode MODE server mode");
  MPRINT("  -f,--scn flashback_scn");
}

static void print_version()
{
#ifndef ENABLE_SANITY
  const char *extra_flags = "";
#else
  const char *extra_flags = "|Sanity";
#endif
  MPRINT("observer (%s)\n", PACKAGE_STRING);
  MPRINT("REVISION: %s", build_version());
  MPRINT("BUILD_BRANCH: %s", build_branch());
  MPRINT("BUILD_TIME: %s %s", build_date(), build_time());
  MPRINT("BUILD_FLAGS: %s%s", build_flags(), extra_flags);
  MPRINT("BUILD_INFO: %s\n", build_info());
  MPRINT("Copyright (c) 2011-present OceanBase Inc.");
  MPRINT();
}

static void print_args(int argc, char *argv[])
{
  for (int i = 0; i < argc - 1; ++i) {
    fprintf(stderr, "%s ", argv[i]);
  }
  fprintf(stderr, "%s\n", argv[argc - 1]);
}

static bool to_int64(const char *sval, int64_t &ival)
{
  char *endp = NULL;
  ival       = static_cast<int64_t>(strtol(sval, &endp, 10));
  return NULL != endp && *endp == '\0';
}

static int to_port_x(const char *sval)
{
  int64_t port = 0;
  if (!to_int64(sval, port)) {
    MPRINTx("need port number: %s", sval);
  } else if (port <= 1024 || port >= 65536) {
    MPRINTx(
        "port number should greater than 1024 and less than 65536: %ld", port);
  }
  return static_cast<int>(port);
}

static int64_t to_cluster_id_x(const char *sval)
{
  int64_t cluster_id = 0;
  if (!to_int64(sval, cluster_id)) {
    MPRINTx("need cluster id: %s", sval);
  } else if (
      cluster_id <= 0 ||
      cluster_id >=
          4294967296) { // cluster id given by command line must be in range [1,4294967295]
    MPRINTx(
        "cluster id in cmd option should be in range [1,4294967295], "
        "but it is %ld",
        cluster_id);
  }
  return cluster_id;
}

static void get_opts_setting(
    struct option long_opts[], char short_opts[], const size_t size)
{
  static struct
  {
    const char *long_name_;
    char        short_name_;
    bool        has_arg_;
  } ob_opts[] = {
      {"help", 'h', 0},
      {"home", 'H', 1},
      {"mysql_port", 'p', 1},
      {"rpc_port", 'P', 1},
      {"nodaemon", 'N', 0},
      {"appname", 'n', 1},
      {"cluster_id", 'c', 1},
      {"data_dir", 'd', 1},
      {"log_level", 'l', 1},
      {"zone", 'z', 1},
      {"optstr", 'o', 1},
      {"devname", 'i', 1},
      {"rs_list", 'r', 1},
      {"mode", 'm', 1},
      {"scn", 'f', 1},
      {"version", 'V', 0},
      {"ipv6", '6', 0},
      {"local_ip", 'I', 1},
  };

  size_t opts_cnt = sizeof(ob_opts) / sizeof(ob_opts[0]);

  if (opts_cnt >= size) {
    MPRINTx("parse option fail: opts array is too small");
  }

  int short_idx = 0;

  for (size_t i = 0; i < opts_cnt; ++i) {
    long_opts[i].name    = ob_opts[i].long_name_;
    long_opts[i].has_arg = ob_opts[i].has_arg_;
    long_opts[i].flag    = NULL;
    long_opts[i].val     = ob_opts[i].short_name_;

    short_opts[short_idx++] = ob_opts[i].short_name_;
    if (ob_opts[i].has_arg_) {
      short_opts[short_idx++] = ':';
    }
  }
}

static void
parse_short_opt(const int c, const char *value, ObServerOptions &opts)
{
  switch (c) {
  case 'H':
    MPRINT("home: %s", value);
    opts.home_ = value;
    // set home
    break;

  case 'p':
    MPRINT("mysql port: %s", value);
    opts.mysql_port_ = to_port_x(value);
    // set port
    break;

  case 'P':
    MPRINT("rpc port: %s", value);
    opts.rpc_port_ = to_port_x(value);
    // set port
    break;

  case 'z':
    MPRINT("zone: %s", value);
    opts.zone_ = value;
    break;

  case 'o':
    MPRINT("optstr: %s", value);
    opts.optstr_ = value;
    break;

  case 'i':
    MPRINT("devname: %s", value);
    opts.devname_ = value;
    break;

  case 'r':
    MPRINT("rs list: %s", value);
    opts.rs_list_ = value;
    break;

  case 'N':
    MPRINT("nodaemon");
    opts.nodaemon_ = true;
    // set nondaemon
    break;

  case 'n':
    MPRINT("appname: %s", value);
    opts.appname_ = value;
    break;

  case 'c':
    MPRINT("cluster id: %s", value);
    opts.cluster_id_ = to_cluster_id_x(value);
    break;

  case 'd':
    MPRINT("data_dir: %s", value);
    opts.data_dir_ = value;
    break;

  case 'l':
    MPRINT("log level: %s", value);
    if (OB_SUCCESS != OB_LOGGER.level_str2int(value, opts.log_level_)) {
      MPRINT("malformed log level, candicates are: "
             "    ERROR,USER_ERR,WARN,INFO,TRACE,DEBUG");
      MPRINT("!! Back to INFO log level.");
      opts.log_level_ = OB_LOG_LEVEL_WARN;
    }
    break;

  case 'm':
    // set mode
    MPRINT("server startup mode: %s", value);
    opts.startup_mode_ = value;
    break;

  case 'f':
    MPRINT("flashback scn: %s", value);
    to_int64(value, opts.flashback_scn_);
    break;

  case 'V':
    print_version();
    exit(0);
    break;

  case '6':
    opts.use_ipv6_ = true;
    break;

  case 'I':
    MPRINT("local_ip: %s", value);
    opts.local_ip_ = value;
    break;

  case 'h':
  default:
    print_help();
    exit(1);
  }
}

// process long only option
static void
parse_long_opt(const char *name, const char *value, ObServerOptions &opts)
{
  MPRINT("long: %s %s", name, value);
  UNUSED(name);
  UNUSED(value);
  UNUSED(opts);
}

static int callback(struct dl_phdr_info *info, size_t size, void *data)
{
  UNUSED(size);
  UNUSED(data);
  if (OB_ISNULL(info)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument", K(info));
  } else {
    MPRINT("name=%s (%d segments)", info->dlpi_name, info->dlpi_phnum);
    for (int j = 0; j < info->dlpi_phnum; j++) {
      if (NULL != info->dlpi_phdr) {
        MPRINT(
            "\t\t header %2d: address=%10p",
            j,
            (void *)(info->dlpi_addr + info->dlpi_phdr[j].p_vaddr));
      }
    }
  }
  return 0;
}

static void parse_opts(int argc, char *argv[], ObServerOptions &opts)
{
  static const int     MAX_OPTS_CNT = 128;
  static char          short_opts[MAX_OPTS_CNT * 2 + 1];
  static struct option long_opts[MAX_OPTS_CNT];

  get_opts_setting(long_opts, short_opts, MAX_OPTS_CNT);

  int long_opts_idx = 0;

  bool end = false;
  while (!end) {
    int c = getopt_long(argc, argv, short_opts, long_opts, &long_opts_idx);

    if (-1 == c || long_opts_idx >= MAX_OPTS_CNT) { // end
      end = true;
    } else if (0 == c) {
      parse_long_opt(long_opts[long_opts_idx].name, optarg, opts);
    } else {
      parse_short_opt(c, optarg, opts);
    }
  }
}

static void print_limit(const char *name, const int resource)
{
  struct rlimit limit;
  if (0 == getrlimit(resource, &limit)) {
    if (RLIM_INFINITY == limit.rlim_cur) {
      _OB_LOG(INFO, "[%s] %-24s = %s", __func__, name, "unlimited");
    } else {
      _OB_LOG(INFO, "[%s] %-24s = %ld", __func__, name, limit.rlim_cur);
    }
  }
  if (RLIMIT_CORE == resource) {
    g_rlimit_core = limit.rlim_cur;
  }
}

static void print_all_limits()
{
  OB_LOG(INFO, "============= *begin server limit report * =============");
  print_limit("RLIMIT_CORE",RLIMIT_CORE);
  print_limit("RLIMIT_CPU",RLIMIT_CPU);
  print_limit("RLIMIT_DATA",RLIMIT_DATA);
  print_limit("RLIMIT_FSIZE",RLIMIT_FSIZE);
  print_limit("RLIMIT_LOCKS",RLIMIT_LOCKS);
  print_limit("RLIMIT_MEMLOCK",RLIMIT_MEMLOCK);
  print_limit("RLIMIT_NOFILE",RLIMIT_NOFILE);
  print_limit("RLIMIT_NPROC",RLIMIT_NPROC);
  print_limit("RLIMIT_STACK",RLIMIT_STACK);
  OB_LOG(INFO, "============= *stop server limit report* ===============");
}

static int check_uid_before_start(const char *dir_path)
{
  int ret = OB_SUCCESS;
  uid_t current_uid = UINT_MAX;
  struct stat64 dir_info;

  current_uid = getuid();
  if (0 != ::stat64(dir_path, &dir_info)) {
    /* do nothing */
  } else {
    if (current_uid != dir_info.st_uid) {
      ret = OB_UTL_FILE_ACCESS_DENIED;
      MPRINT("ERROR: current user(uid=%u) that starts observer is not the same with the original one(uid=%u), observer starts failed!",
              current_uid, dir_info.st_uid);
    }
  }

  return ret;
}

static void print_all_thread(const char* desc)
{
  MPRINT("============= [%s] begin to show unstopped thread =============", desc);
  DIR *dir = opendir("/proc/self/task");
  if (dir == NULL) {
    MPRINT("fail to print all thread");
  } else {
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
      char *tid = entry->d_name;
      if (tid[0] != '.') { // pass . and ..
        char path[256];
        sprintf(path, "/proc/self/task/%s/comm", tid);
        FILE *file = fopen(path, "r");
        if (file == NULL) {
          MPRINT("fail to print thread tid: %s", tid);
        } else {
          char name[256];
          fgets(name, 256, file);
          size_t len = strlen(name);
          if (len > 0 && name[len - 1] == '\n') {
            name[len - 1] = '\0';
          }
          MPRINT("[%s] detect unstopped thread, tid: %s, name: %s", desc, tid, name);
          fclose(file);
        }
      }
    }
  }
  closedir(dir);
  MPRINT("============= [%s] finish to show unstopped thread =============", desc);
}

int main(int argc, char *argv[])
{
#ifdef ENABLE_SANITY
  backtrace_symbolize_func = oceanbase::common::backtrace_symbolize;
#endif
  if (0 != pthread_getname_np(pthread_self(), ob_get_tname(), OB_THREAD_NAME_BUF_LEN)) {
    snprintf(ob_get_tname(), OB_THREAD_NAME_BUF_LEN, "observer");
  }
  ObStackHeaderGuard stack_header_guard;
  // just take effect in observer
#ifndef OB_USE_ASAN
  init_malloc_hook();
#endif
  int64_t memory_used = get_virtual_memory_used();
#ifndef OB_USE_ASAN
  /**
    signal handler stack
   */
  void *ptr = malloc(SIG_STACK_SIZE);
  abort_unless(ptr != nullptr);
  stack_t nss;
  stack_t oss;
  bzero(&nss, sizeof(nss));
  bzero(&oss, sizeof(oss));
  nss.ss_sp = ptr;
  nss.ss_size = SIG_STACK_SIZE;
  abort_unless(0 == sigaltstack(&nss, &oss));
  DEFER(sigaltstack(&oss, nullptr));
  ::oceanbase::common::g_redirect_handler = true;
#endif

#ifdef __NEED_PERF__
  register_gperf_handlers();
#endif
  // Fake routines for current thread.
  easy_pool_set_allocator(ob_easy_realloc);
  ev_set_allocator(ob_easy_realloc);

#ifndef OB_USE_ASAN
  get_mem_leak_checker().init();
#endif

  ObCurTraceId::SeqGenerator::seq_generator_  = ObTimeUtility::current_time();
  static const int  LOG_FILE_SIZE             = 256 * 1024 * 1024;
  char              LOG_DIR[]                 = "log";
  char              PID_DIR[]                 = "run";
  char              CONF_DIR[]                = "etc";
  char              AUDIT_DIR[]               = "audit";
  const char *const LOG_FILE_NAME             = "log/observer.log";
  const char *const RS_LOG_FILE_NAME          = "log/rootservice.log";
  const char *const ELECT_ASYNC_LOG_FILE_NAME = "log/election.log";
  const char *const TRACE_LOG_FILE_NAME       = "log/trace.log";
  const char *const PID_FILE_NAME             = "run/observer.pid";
  int               ret                       = OB_SUCCESS;

  easy_log_format = ob_easy_log_format;
  // change signal mask first.
  if (OB_FAIL(ObSignalHandle::change_signal_mask())) {
    MPRINT("change signal mask failed, ret=%d", ret);
  }
  ObServerOptions opts;

  char audit_file[ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE] = {};
  int64_t pos = 0;

  print_args(argc, argv);

  setlocale(LC_ALL, "");
  // Set character classification type to C to avoid printf large string too
  // slow.
  setlocale(LC_CTYPE, "C");
  setlocale(LC_TIME, "en_US.UTF-8");
  setlocale(LC_NUMERIC, "en_US.UTF-8");
  // memset(&opts, 0, sizeof (opts));
  opts.log_level_ = OB_LOG_LEVEL_WARN;
  parse_opts(argc, argv, opts);

  if (OB_FAIL(check_uid_before_start(CONF_DIR))) {
    MPRINT("Fail check_uid_before_start, please use the initial user to start observer!");
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(PID_DIR))) {
    MPRINT("create pid dir fail: ./run/");
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(LOG_DIR))) {
    MPRINT("create log dir fail: ./log/");
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(CONF_DIR))) {
    MPRINT("create log dir fail: ./etc/");
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(AUDIT_DIR))) {
    MPRINT("create log dir fail: ./audit/");
  } else if (OB_FAIL(ObSecurityAuditUtils::get_audit_file_name(audit_file,
      ObPLogFileStruct::MAX_LOG_FILE_NAME_SIZE,
      pos))) {
  } else if (OB_FAIL(ObEncryptionUtil::init_ssl_malloc())) {
    MPRINT("failed to init crypto malloc");
  } else if (!opts.nodaemon_ && OB_FAIL(start_daemon(PID_FILE_NAME))) {
  } else {
    CURLcode curl_code = curl_global_init(CURL_GLOBAL_ALL);
    OB_ASSERT(CURLE_OK == curl_code);

    const char *syslog_file_info = ObServerUtils::build_syslog_file_info(ObAddr());
    easy_log_level = EASY_LOG_INFO;
    OB_LOGGER.set_log_level(opts.log_level_);
    OB_LOGGER.set_max_file_size(LOG_FILE_SIZE);
    OB_LOGGER.set_new_file_info(syslog_file_info);
    OB_LOGGER.set_file_name(LOG_FILE_NAME, false, true, RS_LOG_FILE_NAME, ELECT_ASYNC_LOG_FILE_NAME, TRACE_LOG_FILE_NAME, audit_file);
    ObPLogWriterCfg log_cfg;
    // if (OB_SUCCESS != (ret = ASYNC_LOG_INIT(ELECT_ASYNC_LOG_FILE_NAME, opts.log_level_, true))) {
    //   LOG_ERROR("election async log init error.", K(ret));
    //   ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    // }
    LOG_INFO("succ to init logger",
             "default file", LOG_FILE_NAME,
             "rs file", RS_LOG_FILE_NAME,
             "election file", ELECT_ASYNC_LOG_FILE_NAME,
             "trace file", TRACE_LOG_FILE_NAME,
             K(audit_file),
             "max_log_file_size", LOG_FILE_SIZE,
             "enable_async_log", OB_LOGGER.enable_async_log());
    if (0 == memory_used) {
      _LOG_INFO("Get virtual memory info failed");
    } else {
      _LOG_INFO("Virtual memory : %'15ld byte", memory_used);
    }
    // print in log file.
    LOG_INFO("Build basic information for each syslog file", "info", syslog_file_info);
    print_args(argc, argv);
    print_version();
    print_all_limits();
    dl_iterate_phdr(callback, NULL);
    ObProcMaps::get_instance().load_maps();

    static const int DEFAULT_MMAP_MAX_VAL = 1024 * 1024 * 1024;
    mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
    mallopt(M_ARENA_MAX, 1); // disable malloc multiple arena pool

    // turn warn log on so that there's a observer.log.wf file which
    // records all WARN and ERROR logs in log directory.
    ObWarningBuffer::set_warn_log_on(true);
    if (OB_SUCC(ret)) {
      // Create worker to make this thread having a binding
      // worker. When ObThWorker is creating, it'd be aware of this
      // thread has already had a worker, which can prevent binding
      // new worker with it.
      lib::Worker worker;
      lib::Worker::set_worker_to_thread_local(&worker);
      ObServer &observer = ObServer::get_instance();
      LOG_INFO("observer starts", "observer_version", PACKAGE_STRING);
      // to speed up bootstrap phase, need set election INIT TS
      // to count election keep silence time as soon as possible after observer process started
      ATOMIC_STORE(&palf::election::INIT_TS, palf::election::get_monotonic_ts());
      if (OB_FAIL(observer.init(opts, log_cfg))) {
        LOG_ERROR("observer init fail", K(ret));
      } else if (OB_FAIL(observer.start())) {
        LOG_ERROR("observer start fail", K(ret));
      } else if (OB_FAIL(observer.wait())) {
        LOG_ERROR("observer wait fail", K(ret));
      }
      print_all_thread("BEFORE_DESTROY");
      observer.destroy();
    }
    curl_global_cleanup();
    unlink(PID_FILE_NAME);
  }

  LOG_INFO("observer exits", "observer_version", PACKAGE_STRING);
  print_all_thread("AFTER_DESTROY");
  return ret;
}
