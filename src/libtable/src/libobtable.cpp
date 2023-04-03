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

#define USING_LOG_PREFIX CLIENT
#include "libobtable.h"
#include "share/ob_version.h"
#include "lib/oblog/ob_easy_log.h"
#include "io/easy_log.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "share/system_variable/ob_system_variable_init.h"  // ObPreProcessSysVars::init_sys_var()
#include "sql/ob_sql_init.h"
#include "lib/resource/ob_resource_mgr.h"
#include "rpc/frame/ob_net_easy.h"
#include "observer/ob_signal_handle.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using oceanbase::observer::ObSignalHandle;

int ObTableServiceLibrary::init()
{
  int ret = OB_SUCCESS;
  // coroutine settings
  ::oceanbase::common::USE_CO_LATCH = false;
  // Create worker for current thread.
  ::oceanbase::lib::Worker worker;
  ::oceanbase::lib::Worker::set_worker_to_thread_local(&worker);
  // setup easy
  easy_log_set_format(easy_log_format_adaptor);
  easy_log_level = EASY_LOG_INFO;
  // setup oblogger
  OB_LOGGER.set_log_level("INFO");
  const bool redirect_stderr = false;
  const bool open_wf = false;
  OB_LOGGER.set_file_name("libobtable.log", !redirect_stderr, open_wf);
  // set total memory limit
  static const int64_t MAX_MEMORY_USAGE_PERCENT = 80;
  lib::set_memory_limit(get_phy_mem_size() * MAX_MEMORY_USAGE_PERCENT / 100);
  // set locale
  setlocale(LC_ALL, "");
  setlocale(LC_TIME, "en_US.UTF-8");

  // mem_limit_getter is used by KVGlobalCache
  static ObSimpleMemLimitGetter mem_limit_getter;
  static const int64_t max_cache_size = 20LL * 1024 * 1024 * 1024;  // 20GB

  if (OB_FAIL(ObSignalHandle::change_signal_mask())) { //change signal mask
    LOG_WARN("change signal mask failed", K(ret));
  } else if (OB_FAIL(mem_limit_getter.add_tenant(OB_SYS_TENANT_ID,
                                                 0,
                                                 max_cache_size))) {
    LOG_ERROR("add OB SYS tenant fail", K(ret));
  } else if (OB_FAIL(mem_limit_getter.add_tenant(OB_SERVER_TENANT_ID,
                                                 0,
                                                 INT64_MAX))) {
    LOG_ERROR("add OB SERVER tenant fail", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(&mem_limit_getter))) { // set kvcache
    LOG_WARN("failed to init ObKVGlobalCache", K(ret));
  } else if (OB_FAIL(lib::ObResourceMgr::get_instance().set_cache_washer(
                         ObKVGlobalCache::get_instance()))) {
    LOG_WARN("Fail to set_cache_washer", K(ret));
  } else if (OB_FAIL(share::ObPreProcessSysVars::init_sys_var())) {  // for session::load_default_variables()
    LOG_WARN("failed to init system variable default value", K(ret));
  } else if (OB_FAIL(::oceanbase::sql::init_sql_factories())) {  // for ObTableServiceClient::get_partition_location
    LOG_WARN("failed to init sql factory", K(ret));
  } else if (OB_FAIL(::oceanbase::sql::init_sql_expr_static_var())) { // for ObTableServiceClient::get_partition_location
    LOG_WARN("failed to init sql expr", K(ret));
  } else {
    // init trace id
    ObCurTraceId::SeqGenerator::seq_generator_ = ObTimeUtility::current_time();
    LOG_INFO("libobtable inited");
  }
  return ret;
}

void ObTableServiceLibrary::destroy()
{
  ObKVGlobalCache::get_instance().destroy();
  LOG_INFO("libobtable destroyed");
  OB_LOGGER.set_stop_append_log();
}

#define MPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
extern "C"
{
  const char my_interp[] __attribute__((section(".interp"))) __maybe_unused
      = "/lib64/ld-linux-x86-64.so.2";
  void __attribute__((constructor)) libobtable_init()
  {
  }

  static void print_version()
  {
    MPRINT("libobtable (%s %s)\n", PACKAGE_STRING, RELEASEID);
    MPRINT("REVISION: %s", build_version());
    MPRINT("BUILD_BRANCH: %s", build_branch());
    MPRINT("BUILD_TIME: %s %s", build_date(), build_time());
    MPRINT("BUILD_FLAGS: %s\n", build_flags());
    MPRINT("Copyright (c) 2007-2018 Alipay Inc.");
    MPRINT();
  }

  int so_main()
  {
    print_version();
    return 0;
  }
}
