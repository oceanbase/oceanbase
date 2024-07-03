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
 *
 * OBCDC Instance
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_instance.h"

#include "lib/oblog/ob_log_module.h"        // LOG_ERROR
#include "lib/file/file_directory_utils.h"  // FileDirectoryUtils
#include "share/ob_version.h"               // build_version
#include "share/system_variable/ob_system_variable.h" // ObPreProcessSysVars
#include "share/ob_time_utility2.h"         // ObTimeUtility2
#include "share/ob_get_compat_mode.h"
#include "sql/ob_sql_init.h"                // init_sql_factories
#include "observer/omt/ob_tenant_timezone_mgr.h"  // OTTZ_MGR
#include "common/ob_clock_generator.h"
#include "lib/alloc/ob_malloc_sample_struct.h"

#include "ob_log_common.h"
#include "ob_log_config.h"                // ObLogConfig
#include "ob_log_utils.h"                 // ob_cdc_malloc
#include "ob_log_meta_manager.h"          // ObLogMetaManager
#include "ob_log_sql_server_provider.h"   // ObLogSQLServerProvider (for cluster sync mode)
#include "ob_cdc_tenant_sql_server_provider.h"  // ObCDCTenantSQLServerProvider(for cluster sync mode)
#include "ob_cdc_tenant_endpoint_provider.h"    // ObCDCEndpointProvider (for tenant sync mode)
#include "ob_log_schema_getter.h"         // ObLogSchemaGetter
#include "ob_log_timezone_info_getter.h"  // ObCDCTimeZoneInfoGetter
#include "ob_log_committer.h"             // ObLogCommitter
#include "ob_log_formatter.h"             // ObLogFormatter
#include "ob_cdc_lob_data_merger.h"       // ObCDCLobDataMerger
#include "ob_log_batch_buffer.h"          // ObLogBatchBuffer
#include "ob_log_storager.h"              // ObLogStorager
#include "ob_log_reader.h"                // ObLogReader
#include "ob_log_sequencer1.h"            // ObLogSequencer
#include "ob_log_part_trans_parser.h"     // ObLogPartTransParser
#include "ob_log_dml_parser.h"            // ObLogDmlParser
#include "ob_log_ddl_parser.h"            // ObLogDdlParser
#include "ob_log_fetcher.h"               // ObLogFetcher
#include "ob_log_part_trans_task.h"       // PartTransTask
#include "ob_log_table_matcher.h"         // ObLogTableMatcher
#include "ob_log_trans_ctx_mgr.h"         // ObLogTransCtxMgr
#include "ob_log_trans_ctx.h"             // TransCtx
#include "ob_log_resource_collector.h"    // ObLogResourceCollector
#include "ob_log_binlog_record_pool.h"    // ObLogBRPool
#include "ob_log_sys_ls_task_handler.h"   // ObLogSysLsTaskHandler
#include "ob_log_ddl_processor.h"         // ObLogDDLProcessor
#include "ob_log_start_schema_matcher.h"  // ObLogStartSchemaMatcher
#include "ob_log_tenant_mgr.h"            // IObLogTenantMgr
#include "ob_log_rocksdb_store_service.h" // RocksDbStoreService
#include "ob_cdc_auto_config_mgr.h"       // CDC_CFG_MGR
#include "ob_cdc_malloc_sample_info.h"    // ObCDCMallocSampleInfo

#include "ob_log_trace_id.h"
#include "share/ob_simple_mem_limit_getter.h"

#define INIT(v, type, args...) \
    do {\
      if (OB_SUCC(ret)) { \
        type *tmp_var = NULL; \
        if (OB_ISNULL(tmp_var = new(std::nothrow) type())) { \
          _LOG_ERROR("construct %s fail", #type); \
          ret = OB_ALLOCATE_MEMORY_FAILED; \
        } else if (OB_FAIL(tmp_var->init(args))) { \
          _LOG_ERROR("init %s fail, ret=%d", #type, ret); \
          delete tmp_var; \
          tmp_var = NULL; \
        } else { \
          v = tmp_var; \
          _LOG_INFO("init component \'%s\' succ", #type); \
        } \
      } \
    } while (0)

#define DESTROY(v, type) \
    do {\
      if (NULL != v) { \
        type *var = static_cast<type *>(v); \
        (void)var->destroy(); \
        delete v; \
        v = NULL; \
      } \
    } while (0)

using namespace oceanbase::common;

namespace oceanbase
{
using namespace share;
namespace libobcdc
{

ObLogInstance *ObLogInstance::instance_ = NULL;
static ObSimpleMemLimitGetter mem_limit_getter;

const int64_t ObLogInstance::DAEMON_THREAD_COUNT = 1;

ObLogInstance *ObLogInstance::get_instance()
{
  if (NULL == instance_) {
    instance_ = new(std::nothrow) ObLogInstance();
  }

  return instance_;
}

// Here is the chicken and egg problem, assuming that ObLogInstance has already been created when get_ref_instance() is called
ObLogInstance &ObLogInstance::get_ref_instance()
{
  if (NULL == instance_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObLogInstance is NULL", K(instance_));
  }
  return *instance_;
}

void ObLogInstance::destroy_instance()
{
  if (NULL != instance_) {
    _LOG_INFO("ObLogInstance %p destroy", instance_);
    delete instance_;
    instance_ = NULL;
  }
}

ObLogInstance::ObLogInstance() :
    inited_(false),
    is_running_(false),
    oblog_major_(0),
    oblog_minor_(0),
    oblog_major_patch_(0),
    oblog_minor_patch_(0),
    timer_tid_(0),
    sql_tid_(0),
    flow_control_tid_(0),
    err_cb_(NULL),
    global_errno_(0),
    handle_error_flag_(0),
    disable_redirect_log_(false),
    log_clean_cycle_time_us_(0),
    output_dml_br_count_(0),
    output_ddl_br_count_(0),
    stop_flag_(true),
    last_heartbeat_timestamp_micro_sec_(0),
    is_assign_log_dir_valid_(false),
    br_index_in_trans_(0),
    part_trans_task_count_(0),
    trans_task_pool_alloc_(),
    start_tstamp_ns_(0),
    sys_start_schema_version_(OB_INVALID_VERSION),
    is_schema_split_mode_(true),
    enable_filter_sys_tenant_(false),
    drc_message_factory_binlog_record_type_(),
    working_mode_(WorkingMode::UNKNOWN_MODE),
    refresh_mode_(RefreshMode::UNKNOWN_REFRSH_MODE),
    fetching_mode_(ClientFetchingMode::FETCHING_MODE_UNKNOWN),
    is_tenant_sync_mode_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    global_info_(),
    mysql_proxy_(),
    tenant_sql_proxy_(),
    timezone_info_getter_(NULL),
    hbase_util_(),
    obj2str_helper_(),
    br_queue_(),
    trans_task_pool_(),
    log_entry_task_pool_(NULL),
    store_service_(NULL),
    br_pool_(NULL),
    trans_ctx_mgr_(NULL),
    meta_manager_(NULL),
    resource_collector_(NULL),
    rs_server_provider_(NULL),
    tenant_server_provider_(NULL),
    schema_getter_(NULL),
    tb_matcher_(NULL),
    ss_matcher_(NULL),
    systable_helper_(NULL),
    committer_(NULL),
    storager_(NULL),
    batch_buffer_(NULL),
    reader_(NULL),
    formatter_(NULL),
    lob_data_merger_(NULL),
    lob_aux_meta_storager_(),
    ddl_processor_(NULL),
    sequencer_(NULL),
    part_trans_parser_(NULL),
    dml_parser_(NULL),
    ddl_parser_(NULL),
    sys_ls_handler_(NULL),
    dispatcher_(),
    fetcher_(NULL),
    trans_stat_mgr_(NULL),
    tenant_mgr_(NULL),
    trans_redo_dispatcher_(NULL),
    trans_msg_sorter_(NULL)
{
  MEMSET(assign_log_dir_, 0, sizeof(assign_log_dir_));
  MEMSET(ob_trace_id_str_, 0, sizeof(ob_trace_id_str_));
}

ObLogInstance::~ObLogInstance()
{
  destroy();

  LOG_INFO("====================libobcdc end====================");
}

int ObLogInstance::init(const char *config_file,
      const uint64_t start_tstamp_sec,
      ERROR_CALLBACK err_cb /* = NULL */)
{
  const int64_t start_tstamp_usec = start_tstamp_sec * _SEC_;
  return init_with_start_tstamp_usec(config_file, start_tstamp_usec, err_cb);
}

int ObLogInstance::init_with_start_tstamp_usec(const char *config_file,
      const uint64_t start_tstamp_usec,
      ERROR_CALLBACK err_cb /* = NULL */)
{
  int ret = OB_SUCCESS;
  uint64_t start_tstamp_ns = start_tstamp_usec * NS_CONVERSION;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("instance has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(config_file)) {
    LOG_ERROR("invalid arguments", K(config_file));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_logger_())) { // First initialize the logging module
    LOG_ERROR("init_logger_ fail", KR(ret));
  } else if (OB_FAIL(TCONF.init())) {
    LOG_ERROR("config init fail", KR(ret));
  } else if (OB_FAIL(TCONF.load_from_file(config_file))) {
    LOG_ERROR("load config from file fail", KR(ret), K(config_file));
  } else if (OB_FAIL(init_common_(start_tstamp_ns, err_cb))) {
    LOG_ERROR("init_common_ fail", KR(ret), K(start_tstamp_ns), K(err_cb));
  } else {
    inited_ = true;
  }

  return ret;
}

int ObLogInstance::init(const std::map<std::string, std::string> &configs,
    const uint64_t start_tstamp_sec,
    ERROR_CALLBACK err_cb /* = NULL */ )
{
  int ret = OB_SUCCESS;
  const int64_t start_tstamp_usec = start_tstamp_sec * _SEC_;

  if (OB_FAIL(init_with_start_tstamp_usec(configs, start_tstamp_usec, err_cb))) {
    LOG_ERROR("init fail", KR(ret), K(start_tstamp_usec));
  }

  return ret;
}

int ObLogInstance::init_with_start_tstamp_usec(const std::map<std::string, std::string> &configs,
    const uint64_t start_tstamp_usec,
    ERROR_CALLBACK err_cb /* = NULL */ )
{
  int ret = OB_SUCCESS;
  LOG_INFO("prepare to init libobcdc", K(start_tstamp_usec));
  int64_t start_tstamp_ns = start_tstamp_usec * NS_CONVERSION;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("instance has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_logger_())) { // First initialize the logging module
    LOG_ERROR("init_logger_ fail", KR(ret));
  } else if (OB_FAIL(TCONF.init())) {
    LOG_ERROR("config init fail", KR(ret));
  } else if (OB_FAIL(TCONF.load_from_map(configs))) {
    LOG_ERROR("load config from map fail", KR(ret));
  } else if (OB_FAIL(init_common_(start_tstamp_ns, err_cb))) {
    // handle error
  } else {
    inited_ = true;
  }

  return ret;
}

int ObLogInstance::set_assign_log_dir(const char *log_dir, const int64_t log_dir_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(log_dir) || OB_UNLIKELY(log_dir_len > OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    (void)snprintf(assign_log_dir_, sizeof(assign_log_dir_), "%.*s", static_cast<int>(log_dir_len), log_dir);
    is_assign_log_dir_valid_ = true;
  }

  return ret;
}

int ObLogInstance::set_data_start_ddl_schema_version(const uint64_t tenant_id,
    const int64_t data_start_ddl_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogTenant *tenant = NULL;
  ObLogTenantGuard guard;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (! stop_flag_) {
    LOG_ERROR("ObLogInstance have already started, can not set data start ddl schema version",
        K(stop_flag_), K(tenant_id), K(data_start_ddl_schema_version));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
        || OB_UNLIKELY(OB_INVALID_TIMESTAMP == data_start_ddl_schema_version)) {
    LOG_ERROR("invalid argument", K(tenant_id), K(data_start_ddl_schema_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("tenant is null", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else {
    tenant->update_global_data_schema_version(data_start_ddl_schema_version);
  }

  return ret;
}

int ObLogInstance::get_start_schema_version(const uint64_t tenant_id,
    const bool is_create_tenant_when_backup,
    int64_t &start_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogTenant *tenant = NULL;
  ObLogTenantGuard guard;
  start_schema_version = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (! is_create_tenant_when_backup && ! stop_flag_) {
    LOG_ERROR("ObLogInstance have already started, can not get start schema version",
        K(stop_flag_), K(tenant_id), K(is_create_tenant_when_backup), K(start_schema_version));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    LOG_ERROR("invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
    }
  } else if (OB_ISNULL(tenant = guard.get_tenant())) {
    LOG_ERROR("tenant is null", K(tenant_id), K(tenant));
    ret = OB_ERR_UNEXPECTED;
  } else {
    start_schema_version = tenant->get_start_schema_version();
  }

  return ret;
}

int ObLogInstance::set_start_global_trans_version(const int64_t start_global_trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_global_trans_version)) {
    LOG_ERROR("invalid argument", K(start_global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(fetcher_)) {
    LOG_ERROR("fetcher_ is null", K(fetcher_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(fetcher_->set_start_global_trans_version(start_global_trans_version))) {
    LOG_ERROR("fetcher_ set_start_global_trans_version fail", KR(ret), K(start_global_trans_version));
  } else {
    LOG_INFO("set_start_global_trans_version succ", K(start_global_trans_version));
  }

  return ret;
}

int ObLogInstance::init_logger_()
{
  int ret = OB_SUCCESS;
  char log_dir[OB_MAX_FILE_NAME_LENGTH];
  char log_file[OB_MAX_FILE_NAME_LENGTH];
  char stderr_log_file[OB_MAX_FILE_NAME_LENGTH];

  if (is_assign_log_dir_valid_) {
    (void)snprintf(log_dir, sizeof(log_dir), "%s", assign_log_dir_);
    (void)snprintf(log_file, sizeof(log_file), "%s%s", assign_log_dir_, DEFAULT_LOG_FILE_NAME);
    (void)snprintf(stderr_log_file, sizeof(stderr_log_file), "%s%s", assign_log_dir_, DEFAULT_STDERR_LOG_FILE_NAME);
  } else {
    (void)snprintf(log_dir, sizeof(log_dir), "%s", DEFAULT_LOG_DIR);
    (void)snprintf(log_file, sizeof(log_file), "%s", DEFAULT_LOG_FILE);
    (void)snprintf(stderr_log_file, sizeof(stderr_log_file), "%s", DEFAULT_STDERR_LOG_FILE);
  }

  if (OB_FAIL(common::FileDirectoryUtils::create_full_path(log_dir))) {
    LOG_ERROR("FileDirectoryUtils create_full_path fail", KR(ret), K(log_dir));
  } else {
    const int64_t max_log_file_count = TCONF.max_log_file_count;
    const bool enable_log_limit = (1 == TCONF.enable_log_limit);
    easy_log_level = EASY_LOG_INFO;
    OB_LOGGER.set_max_file_size(MAX_LOG_FILE_SIZE);
    OB_LOGGER.set_max_file_index(max_log_file_count);
    if (!OB_LOGGER.is_svr_file_opened()) {
      OB_LOGGER.set_file_name(log_file, disable_redirect_log_, false);
    }
    OB_LOGGER.set_log_level("INFO");
    OB_LOGGER.disable_thread_log_level();
    OB_LOGGER.set_enable_log_limit(enable_log_limit);

    if (! disable_redirect_log_) {
      // Open the stderr log file
      // and redirects stderr to that log file
      int32_t stderr_log_fd = open(stderr_log_file, O_RDWR | O_CREAT | O_APPEND | O_LARGEFILE, 0644);
      if (0 > stderr_log_fd) {
        ret = OB_IO_ERROR;
        LOG_ERROR("open stderr log file failed", KR(ret), K(stderr_log_file), K(errno), KERRMSG);
      } else if (0 > dup2(stderr_log_fd, 2)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("dup stderr log fd to stderr failed", KR(ret), K(stderr_log_file), K(stderr_log_fd), K(errno), KERRMSG);
      } else if (0 != close(stderr_log_fd)) {
        ret = OB_IO_ERROR;
        LOG_ERROR("close stderr log fd failed", KR(ret), K(stderr_log_file), K(stderr_log_fd), K(errno), KERRMSG);
      } else {
        stderr_log_fd = -1;
      }
    }

    #ifndef ENABLE_SANITY
      const char *extra_flags = "";
    #else
      const char *extra_flags = "|Sanity";
    #endif
    _LOG_INFO("====================libobcdc start====================");
    _LOG_INFO("libobcdc %s %s", PACKAGE_VERSION, RELEASEID);
    _LOG_INFO("BUILD_VERSION: %s", build_version());
    _LOG_INFO("BUILD_TIME: %s %s", build_date(), build_time());
    _LOG_INFO("BUILD_FLAGS: %s%s", build_flags(), extra_flags);
    _LOG_INFO("BUILD_INFO: %s", build_info());
    _LOG_INFO("Copyright (c) 2024 Ant Group Co., Ltd.");
    _LOG_INFO("======================================================");
    _LOG_INFO("\n");
  }

  return ret;
}

#define MPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)

void ObLogInstance::print_version()
{
  #ifndef ENABLE_SANITY
    const char *extra_flags = "";
  #else
    const char *extra_flags = "|Sanity";
  #endif
  MPRINT("libobcdc %s %s", PACKAGE_VERSION, RELEASEID);
  MPRINT("REVISION: %s", build_version());
  MPRINT("BUILD_TIME: %s %s", build_date(), build_time());
  MPRINT("BUILD_FLAGS: %s%s\n", build_flags(), extra_flags);
  MPRINT("BUILD_INFO: %s\n", build_info());
  MPRINT("Copyright (c) 2024 Ant Group Co., Ltd.");
  MPRINT();
}

int ObLogInstance::init_global_tenant_manager_()
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_TENANT_COUNT = 2;
  static const int64_t SCHEMA_CACHE_MEM_LIMIT_LOWER_BOUND = 0;
  static const int64_t SCHEMA_CACHE_MEM_LIMIT_UPPER_BOUND = 1L << 31L; // 2G
  const int64_t tenant_manager_memory_upper_limit = TCONF.tenant_manager_memory_upper_limit.get();

  if (OB_FAIL(mem_limit_getter.add_tenant(OB_SYS_TENANT_ID,
                                          SCHEMA_CACHE_MEM_LIMIT_LOWER_BOUND,
                                          SCHEMA_CACHE_MEM_LIMIT_UPPER_BOUND))) {
    LOG_ERROR("add OB SYS tenant fail", KR(ret));
  } else if (OB_FAIL(mem_limit_getter.add_tenant(OB_SERVER_TENANT_ID,
                                                 0,
                                                 tenant_manager_memory_upper_limit))) {
    LOG_ERROR("add OB SERVER tenant fail", KR(ret));
  } else {
    LOG_INFO("mem_limit_getter add_tenant succ",
        "tenant_manager_memory_upper_limit", SIZE_TO_STR(tenant_manager_memory_upper_limit));
  }

  return ret;
}

// TODO: verify need invoke or not, and if still used by online schema, may neednoot invoke in
// data_dict mode
int ObLogInstance::init_global_kvcache_()
{
  int ret = OB_SUCCESS;
  static const int64_t KV_CACHE_WASH_TIMER_INTERVAL_US = 60 * _SEC_;
  static const int64_t DEFAULT_BUCKET_NUM = 10000000L;
  static const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;  //1T

  // init schema cache
  if (OB_FAIL(ObKVGlobalCache::get_instance().init(&mem_limit_getter,
                                                   DEFAULT_BUCKET_NUM,
                                                   DEFAULT_MAX_CACHE_SIZE,
                                                   lib::ACHUNK_SIZE,
                                                   KV_CACHE_WASH_TIMER_INTERVAL_US))) {
    LOG_ERROR("Fail to init ObKVGlobalCache", KR(ret));
  } else if (OB_FAIL(lib::ObResourceMgr::get_instance().set_cache_washer(ObKVGlobalCache::get_instance()))) {
    LOG_ERROR("Fail to set_cache_washer", KR(ret));
  } else {
    LOG_INFO("ObKVGlobalCache init succ", "max_cached_size", SIZE_TO_STR(DEFAULT_MAX_CACHE_SIZE));
  }

  return ret;
}

// TODO verify and remove
// FIXME: when refreshing the schema, construct "generated column" schema depends on the default system variables, require initialization
// The specific function is: ObSchemaUtils::cascaded_generated_column()
//
// and @ryan.ly confirm that this situation is only temporary, the subsequent "generated column" logic will decouple the schema and the default system variables.
// Once decoupled, there is no need to initialize system variables here
// After decoupling, there is no need to initialize system variables here.
int ObLogInstance::init_sys_var_for_generate_column_schema_()
{
  int ret = OB_SUCCESS;
  ::oceanbase::sql::init_sql_factories();

  if (OB_FAIL(ObPreProcessSysVars::init_sys_var())){
    LOG_ERROR("PreProcessing init system variable failed", KR(ret));
  }  else if (OB_FAIL(ObBasicSessionInfo::init_sys_vars_cache_base_values())) {
    SQL_LOG(ERROR, "fail to init session base values", K(ret));
  }
  return ret;
}

#include "lib/alloc/alloc_struct.h"
int ObLogInstance::init_common_(uint64_t start_tstamp_ns, ERROR_CALLBACK err_cb)
{
  int ret = OB_SUCCESS;
  int64_t current_timestamp_usec = get_timestamp() * NS_CONVERSION;

  if (start_tstamp_ns <= 0) {
    start_tstamp_ns =  current_timestamp_usec;
    _LOG_INFO("start libobcdc from current timestamp: %ld", start_tstamp_ns);
  }

  if (OB_SUCC(ret)) {
    err_cb_ = err_cb;
    global_errno_ = 0;
    handle_error_flag_ = 0;
    start_tstamp_ns_ = start_tstamp_ns;

    // set pid file
    write_pid_file_();

    // 1. set the initialization log level to ensure that the schema prints an INFO log at startup
    // 2. Change the schema to WARN after the startup is complete
    OB_LOGGER.set_mod_log_levels(TCONF.init_log_level.str());

    if (OB_FAIL(common::ObClockGenerator::init())) {
      LOG_ERROR("failed to init ob clock generator", KR(ret));
    }
    // 校验配置项是否满足期望
    else if (OB_FAIL(TCONF.check_all())) {
      LOG_ERROR("check config fail", KR(ret));
    } else if (OB_FAIL(dump_config_())) {
      LOG_ERROR("dump_config_ fail", KR(ret));
    } else if (OB_FAIL(lib::ThreadPool::set_thread_count(DAEMON_THREAD_COUNT))) {
      LOG_ERROR("set ObLogInstance daemon thread count failed", KR(ret), K(DAEMON_THREAD_COUNT));
    } else if (OB_FAIL(trans_task_pool_alloc_.init(
        TASK_POOL_ALLOCATOR_TOTAL_LIMIT,
        TASK_POOL_ALLOCATOR_HOLD_LIMIT,
        TASK_POOL_ALLOCATOR_PAGE_SIZE))) {
      LOG_ERROR("init fifo allocator fail", KR(ret));
    } else if (OB_FAIL(trans_task_pool_.init(
        &trans_task_pool_alloc_,
        CDC_CFG_MGR.get_part_trans_task_prealloc_count(),
        1 == TCONF.part_trans_task_dynamic_alloc,
        TCONF.part_trans_task_prealloc_page_count))) {
      LOG_ERROR("init task pool fail", KR(ret));
    } else if (OB_FAIL(hbase_util_.init())) {
      LOG_ERROR("init hbase_util_ fail", KR(ret));
    } else if (OB_FAIL(br_queue_.init(CDC_CFG_MGR.get_br_queue_length()))) {
      LOG_ERROR("init binlog record queue fail", KR(ret));
    } else if (OB_FAIL(init_global_tenant_manager_())) {
      LOG_ERROR("init_global_tenant_manager_ fail", KR(ret));
    } else if (OB_FAIL(init_global_kvcache_())) {
      LOG_ERROR("init_global_kvcache_ fail", KR(ret));
    } else if (OB_FAIL(init_sys_var_for_generate_column_schema_())) {
      LOG_ERROR("init_sys_var_for_generate_column_schema_ fail", KR(ret));
    } else if (OB_FAIL(init_components_(start_tstamp_ns))) {
      LOG_ERROR("init_components_ fail", KR(ret), K(start_tstamp_ns));
    } else {
      stop_flag_ = true;
      timer_tid_ = 0;
      sql_tid_ = 0;
      flow_control_tid_ = 0;
      output_dml_br_count_ = 0;
      output_ddl_br_count_ = 0;
      last_heartbeat_timestamp_micro_sec_ = start_tstamp_ns / NS_CONVERSION - 1;
      log_clean_cycle_time_us_ = TCONF.log_clean_cycle_time_in_hours * _HOUR_;
      part_trans_task_count_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("init obcdc succ",
        K_(is_schema_split_mode),
        K_(is_tenant_sync_mode),
        K_(start_tstamp_ns),
        "start_tstamp", NTS_TO_STR(start_tstamp_ns_),
        "working_mode", print_working_mode(working_mode_),
        "refresh_mode", print_refresh_mode(refresh_mode_),
        "fetching_mode", print_fetching_mode(fetching_mode_),
        K_(enable_filter_sys_tenant),
        K(err_cb));
  }

  if (OB_SUCC(ret)) {
    // After startup, set the log level to prevent schema from printing INFO logs
    OB_LOGGER.set_mod_log_levels(TCONF.log_level.str());
  }

  if (OB_SUCC(ret)) {
    const int64_t max_chunk_cache_size = CDC_CFG_MGR.get_max_chunk_cache_size();
    CHUNK_MGR.set_max_chunk_cache_size(max_chunk_cache_size, true /*use_large_chunk_cache*/);
    _LOG_INFO("[CHUNK_MGR] set max_chunk_cache_size: %s", SIZE_TO_STR(max_chunk_cache_size));
  }

  return ret;
}

int ObLogInstance::dump_config_()
{
  int ret = OB_SUCCESS;
  const char *config_fpath = TCONF.config_fpath.str();

  // dump config to log
  TCONF.print();

  // Create the corresponding directory
  char *p = strrchr(const_cast<char*>(config_fpath), '/');
  if (OB_NOT_NULL(p)) {
    char dir_buffer[OB_MAX_FILE_NAME_LENGTH];
    snprintf(dir_buffer, OB_MAX_FILE_NAME_LENGTH, "%.*s", (int)(p - config_fpath), config_fpath);
    if (OB_FAIL(common::FileDirectoryUtils::create_full_path(dir_buffer))) {
      LOG_ERROR("create config file path failed", KR(ret), KCSTRING(dir_buffer), KCSTRING(p));
    }
  }

  if (OB_SUCC(ret)) {
    // dump config to file
    if (OB_FAIL(TCONF.dump2file(config_fpath))) {
      LOG_ERROR("config dump2file fail", KR(ret), K(config_fpath));
    } else {
      LOG_INFO("dump config to file succ", K(config_fpath));
    }
  }

  CDC_CFG_MGR.init(TCONF);

  return ret;
}

int32_t ObLogInstance::get_pid_()
{
  return static_cast<int32_t>(getpid());
}

int ObLogInstance::init_self_addr_()
{
  int ret = OB_SUCCESS;
  static const int64_t BUF_SIZE = 128;
  char BUFFER[BUF_SIZE];
  int32_t self_pid = get_pid_();
  ObString local_ip(sizeof(BUFFER), 0, BUFFER);

  if (OB_FAIL(get_local_ip(local_ip))) {
    LOG_ERROR("get_local_ip fail", KR(ret), K(local_ip));
  } else if (!get_self_addr().set_ip_addr(local_ip, self_pid)) {
    LOG_ERROR("self addr set ip addr error", K(local_ip), K(self_pid));
  } else {
    LOG_INFO("init self_addr success", "addr", get_self_addr());
    // succ
  }

  return ret;
}

// init schema module
int ObLogInstance::init_schema_(const int64_t start_tstamp_us, int64_t &sys_start_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t sys_tenant_id = OB_SYS_TENANT_ID;
  ObLogSchemaGuard sys_schema_guard;

  INIT(schema_getter_, ObLogSchemaGetter, tenant_sql_proxy_.get_ob_mysql_proxy(),
      &(TCONF.get_common_config()), TCONF.cached_schema_version_count,
      TCONF.history_schema_version_count);

  if (OB_SUCC(ret)) {
    // Get the SYS tenant startup schema version
    // Note: SYS tenants do not need to handle tenant deletion scenarios
    if (OB_FAIL(schema_getter_->get_schema_version_by_timestamp(sys_tenant_id, start_tstamp_us,
        sys_start_schema_version, GET_SCHEMA_TIMEOUT_ON_START_UP))) {
      LOG_ERROR("get_schema_version_by_timestamp fail", KR(ret), K(sys_tenant_id), K(start_tstamp_us));
    }
  }

  return ret;
}

int ObLogInstance::init_components_(const uint64_t start_tstamp_ns)
{
  int ret = OB_SUCCESS;
  IObLogErrHandler *err_handler = this;
  int64_t start_seq = DEFAULT_START_SEQUENCE_NUM;
  bool skip_dirty_data = (TCONF.skip_dirty_data != 0);
  bool skip_reversed_schema_verison = (TCONF.skip_reversed_schema_verison != 0);
  bool enable_hbase_mode = (TCONF.enable_hbase_mode != 0);
  bool enable_backup_mode = (TCONF.enable_backup_mode != 0);
  bool skip_hbase_mode_put_column_count_not_consistency = (TCONF.skip_hbase_mode_put_column_count_not_consistency != 0);
  bool enable_convert_timestamp_to_unix_timestamp = (TCONF.enable_convert_timestamp_to_unix_timestamp != 0);
  bool enable_output_hidden_primary_key = (TCONF.enable_output_hidden_primary_key != 0);
  bool enable_oracle_mode_match_case_sensitive = (TCONF.enable_oracle_mode_match_case_sensitive != 0);
  const char *rs_list = TCONF.rootserver_list.str();
  const char *tg_white_list = TCONF.tablegroup_white_list.str();
  const char *tg_black_list = TCONF.tablegroup_black_list.str();
  int64_t max_cached_trans_ctx_count = MAX_CACHED_TRANS_CTX_COUNT;
  const char *ob_trace_id_ptr = TCONF.ob_trace_id.str();
  const char *drc_message_factory_binlog_record_type_str = TCONF.drc_message_factory_binlog_record_type.str();
  // The starting schema version of the SYS tenant
  const char *data_start_schema_version = TCONF.data_start_schema_version.str();
  const char *store_service_path = TCONF.store_service_path.str();
  const char *working_mode_str = TCONF.working_mode.str();
  WorkingMode working_mode = get_working_mode(working_mode_str);
  const char *refresh_mode_str = TCONF.meta_data_refresh_mode.str();
  RefreshMode refresh_mode = get_refresh_mode(refresh_mode_str);
  const char *fetching_mode_str = TCONF.fetching_log_mode.str();
  ClientFetchingMode fetching_mode = get_fetching_mode(fetching_mode_str);
  const char *archive_dest_str = TCONF.archive_dest.str();
  ObBackupPathString archive_dest(archive_dest_str);
  const bool enable_ssl_client_authentication = (1 == TCONF.ssl_client_authentication);
  const bool enable_sort_by_seq_no = (1 == TCONF.enable_output_trans_order_by_sql_operation);
  const int64_t redo_dispatcher_mem_limit = CDC_CFG_MGR.get_redo_dispatcher_memory_limit();
  enable_filter_sys_tenant_ = (0 != TCONF.enable_filter_sys_tenant);

  drc_message_factory_binlog_record_type_.assign(drc_message_factory_binlog_record_type_str,
      strlen(drc_message_factory_binlog_record_type_str));

  const bool enable_white_black_list = (1 == TCONF.enable_white_black_list);

  const char *tb_white_list = TCONF.get_tb_white_list_buf() != NULL ?  TCONF.get_tb_white_list_buf()
      : TCONF.tb_white_list.str();
  const char *tb_black_list = TCONF.get_tb_black_list_buf() != NULL ?  TCONF.get_tb_black_list_buf()
      : TCONF.tb_black_list.str();

  const bool enable_direct_load_inc = (1 == TCONF.enable_direct_load_inc);

  if (OB_UNLIKELY(! is_working_mode_valid(working_mode))) {
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("working_mode is not valid", KR(ret), K(working_mode_str), "working_mode", print_working_mode(working_mode));
  } else {
    working_mode_ = working_mode;

    LOG_INFO("set working mode", K(working_mode_str), K(working_mode_), "working_mode", print_working_mode(working_mode_));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(! is_refresh_mode_valid(refresh_mode))) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("refresh_mode is not valid", KR(ret), K(refresh_mode_str), "refresh_mode", print_refresh_mode(refresh_mode));
    } else {
      refresh_mode_ = refresh_mode;

      if (is_data_dict_refresh_mode(refresh_mode_)) {
        enable_filter_sys_tenant_ = true;
      }

      LOG_INFO("set refresh mode", K(refresh_mode_str), K(refresh_mode_), "refresh_mode", print_refresh_mode(refresh_mode_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(! is_fetching_mode_valid(fetching_mode))) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("fetching mode is not valid", KR(ret), K(fetching_mode_str), "fetching_mode",
      print_fetching_mode(fetching_mode));
    } else {
      fetching_mode_ = fetching_mode;
      if (is_direct_fetching_mode(fetching_mode_)) {
        // don't fetch sys tenant in direct fetching mode
        enable_filter_sys_tenant_ = true;
      }
      LOG_INFO("set fetching mode", K(fetching_mode_str), K(fetching_mode_), "fetching_mode",
      print_fetching_mode(fetching_mode_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(global_info_.init())) {
    LOG_ERROR("global_info_ init fail", KR(ret));
  }

  // init ObTraceId
  if (OB_FAIL(ret)) {
  } else if (TCONF.need_verify_ob_trace_id) {
    if (OB_FAIL(init_ob_trace_id_(ob_trace_id_ptr))) {
      LOG_ERROR("init_ob_trace_id_ fail", KR(ret), K(ob_trace_id_ptr));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMemoryDump::get_instance().init())) {
      LOG_ERROR("init memory dump fail", K(ret));
    // init self addr
    } else if (OB_FAIL(init_self_addr_())) {
      LOG_ERROR("init self addr error", KR(ret));
    }
  }


  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_sql_provider_())) {
      LOG_ERROR("init_sql_provider_ failed", KR(ret));
    }
  }

  // init oblog version，e.g. 2.2.1
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_obcdc_version_components_())) {
      LOG_ERROR("init oblog version components fail", KR(ret));
    }
  }

  // Initialize ObClusterVersion before initializing the schema module
  if (OB_SUCC(ret)) {
    if (is_integrated_fetching_mode(fetching_mode_)) {
      if (OB_FAIL(init_ob_cluster_version_())) {
        LOG_ERROR("init_ob_cluster_version_ fail", KR(ret));
      }
    }
  }

  // init ObCompatModeGetter
  if (OB_SUCC(ret)) {
    if (is_data_dict_refresh_mode(refresh_mode_) && is_direct_fetching_mode(fetching_mode_)) {
      if (OB_FAIL(share::ObCompatModeGetter::instance().init_for_obcdc())) {
        LOG_ERROR("compat_mode_getter init fail", KR(ret));
      }
    } else {
      if (OB_FAIL(share::ObCompatModeGetter::instance().init(&(mysql_proxy_.get_ob_mysql_proxy())))) {
        LOG_ERROR("compat_mode_getter init fail", KR(ret));
      }
    }
  }

  // check CDCConector version is greater than or equal to ob version
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_observer_version_valid_())) {
      LOG_ERROR("check_observer_version_valid_ fail", KR(ret));
    }
  }

  INIT(log_entry_task_pool_, ObLogEntryTaskPool, TCONF.log_entry_task_prealloc_count);

  INIT(store_service_, RocksDbStoreService, store_service_path);

  INIT(br_pool_, ObLogBRPool, TCONF.binlog_record_prealloc_count);

  INIT(trans_ctx_mgr_, ObLogTransCtxMgr, max_cached_trans_ctx_count, TCONF.sort_trans_participants);

  INIT(meta_manager_, ObLogMetaManager, &obj2str_helper_, enable_output_hidden_primary_key);

  INIT(resource_collector_, ObLogResourceCollector,
      TCONF.resource_collector_thread_num, TCONF.resource_collector_thread_num_for_br,
      CDC_CFG_MGR.get_resource_collector_queue_length(),
      br_pool_, trans_ctx_mgr_, meta_manager_, store_service_, err_handler);

  INIT(tenant_mgr_, ObLogTenantMgr, enable_oracle_mode_match_case_sensitive,
      enable_white_black_list, refresh_mode_);

  if (OB_SUCC(ret)) {
    if (is_tenant_sync_mode()) {
      if (OB_FAIL(ObCDCTimeZoneInfoGetter::get_instance().init(TCONF.timezone.str(),
          mysql_proxy_.get_ob_mysql_proxy(), *systable_helper_, *err_handler))) {
            LOG_ERROR("init timezone_info_getter failed", KR(ret));
      }
    } else {
      if (OB_FAIL(ObCDCTimeZoneInfoGetter::get_instance().init(TCONF.timezone.str(),
          tenant_sql_proxy_.get_ob_mysql_proxy(), *systable_helper_, *err_handler))) {
            LOG_ERROR("init timezone_info_getter failed", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      timezone_info_getter_ = &ObCDCTimeZoneInfoGetter::get_instance();
      // init interface for getting tenant timezone map
      // get_tenant_tz_map_function is defined in ob_log_timezone_info_getter file
      OTTZ_MGR.init(get_tenant_tz_map_function);
    }
  }

  const int64_t start_tstamp_usec = start_tstamp_ns / NS_CONVERSION;
  // The initialization of schema depends on the initialization of timezone_info_getter_,
  // and the initialization of timezone_info_getter_ depends on the initialization of tenant_mgr_
  if (OB_SUCC(ret)) {
    // Initialize schema-related modules, split patterns, and SYS tenant starting schema versions based on start-up timestamps
    if (is_online_refresh_mode(refresh_mode_)) {
      if (OB_FAIL(init_schema_(start_tstamp_usec, sys_start_schema_version_))) {
        LOG_ERROR("init schema fail", KR(ret), K(start_tstamp_usec));
      }
    } else if (is_data_dict_refresh_mode(refresh_mode_)) {
      sys_start_schema_version_ = start_tstamp_usec;
      // set g_liboblog_mode_ is true
      ObSchemaService::g_liboblog_mode_ = true;
    }
  }

  INIT(tb_matcher_, ObLogTableMatcher, tb_white_list, tb_black_list, tg_white_list, tg_black_list);

  INIT(ss_matcher_, ObLogStartSchemaMatcher, data_start_schema_version);

  INIT(trans_stat_mgr_, ObLogTransStatMgr);

  // After initializing the timezone info getter successfully, initialize the obj2str_helper_
  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj2str_helper_.init(*timezone_info_getter_, hbase_util_, enable_hbase_mode,
            enable_convert_timestamp_to_unix_timestamp, enable_backup_mode, *tenant_mgr_))) {
      LOG_ERROR("init obj2str_helper fail", KR(ret), K(enable_hbase_mode),
          K(enable_convert_timestamp_to_unix_timestamp), K(enable_backup_mode));
    }
  }

  ObLogSysTableHelper::ClusterInfo cluster_info;
  if (OB_SUCC(ret)) {
    if (is_integrated_fetching_mode(fetching_mode_)) {
      if (OB_FAIL(query_cluster_info_(cluster_info))) {
        LOG_ERROR("query_cluster_info_ fail", KR(ret), K(cluster_info));
      }
    } else {
      // Default is 1, unused now
      cluster_info.cluster_id_ = 1;
    }
  }

  INIT(trans_msg_sorter_, ObLogTransMsgSorter, enable_sort_by_seq_no, TCONF.msg_sorter_thread_num,
      CDC_CFG_MGR.get_msg_sorter_task_count_upper_limit(), *trans_stat_mgr_, err_handler);

  INIT(committer_, ObLogCommitter, start_seq, &br_queue_, resource_collector_,
      br_pool_, trans_ctx_mgr_, trans_stat_mgr_, err_handler);

  INIT(storager_, ObLogStorager, TCONF.storager_thread_num, CDC_CFG_MGR.get_storager_queue_length(), *store_service_, *err_handler);

  INIT(batch_buffer_, ObLogBatchBuffer, TCONF.batch_buf_size, TCONF.batch_buf_count, storager_);

  INIT(reader_, ObLogReader, TCONF.reader_thread_num, CDC_CFG_MGR.get_reader_queue_length(),
      working_mode_, *store_service_, *err_handler);

  INIT(formatter_, ObLogFormatter, TCONF.formatter_thread_num, CDC_CFG_MGR.get_formatter_queue_length(), working_mode_,
      &obj2str_helper_, br_pool_, meta_manager_, schema_getter_, storager_, err_handler,
      skip_dirty_data, enable_hbase_mode, hbase_util_, skip_hbase_mode_put_column_count_not_consistency,
      enable_output_hidden_primary_key);

  INIT(lob_data_merger_, ObCDCLobDataMerger, TCONF.lob_data_merger_thread_num,
      TCONF.lob_data_merger_queue_length, *err_handler);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(lob_aux_meta_storager_.init(store_service_))) {
      LOG_ERROR("lob_aux_meta_storager_ init failed", KR(ret));
    }
  }

  INIT(trans_redo_dispatcher_, ObLogTransRedoDispatcher, redo_dispatcher_mem_limit,
      enable_sort_by_seq_no, *trans_stat_mgr_);

  INIT(ddl_processor_, ObLogDDLProcessor, schema_getter_, TCONF.skip_reversed_schema_verison,
      TCONF.enable_white_black_list);

  INIT(sequencer_, ObLogSequencer, TCONF.sequencer_thread_num, CDC_CFG_MGR.get_sequencer_queue_length(),
      *trans_ctx_mgr_, *trans_stat_mgr_, *committer_, *trans_redo_dispatcher_, *trans_msg_sorter_, *err_handler);

  INIT(part_trans_parser_, ObLogPartTransParser, br_pool_, meta_manager_, cluster_info.cluster_id_);

  INIT(dml_parser_, ObLogDmlParser, TCONF.dml_parser_thread_num, CDC_CFG_MGR.get_dml_parser_queue_length(), *formatter_,
      *err_handler, *part_trans_parser_);

  INIT(ddl_parser_, ObLogDdlParser, TCONF.ddl_parser_thread_num, CDC_CFG_MGR.get_auto_queue_length(), *err_handler,
      *part_trans_parser_);

  INIT(sys_ls_handler_, ObLogSysLsTaskHandler, ddl_parser_, ddl_processor_, sequencer_, err_handler,
      schema_getter_, skip_reversed_schema_verison);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dispatcher_.init(sys_ls_handler_, committer_, start_seq))) {
      LOG_ERROR("fetcher dispatcher init failed", KR(ret), K(sys_ls_handler_),
          K(committer_), K(start_seq));
    }
  }

  INIT(fetcher_, ObLogFetcher, false/*is_load_data_dict_baseline_data*/, enable_direct_load_inc, fetching_mode,
      archive_dest, &dispatcher_, sys_ls_handler_, &trans_task_pool_, log_entry_task_pool_,
      &mysql_proxy_.get_ob_mysql_proxy(), err_handler, cluster_info.cluster_id_, TCONF, start_seq);

  if (OB_SUCC(ret)) {
    if (is_data_dict_refresh_mode(refresh_mode_)) {
      if (OB_FAIL(ObLogMetaDataService::get_instance().init(start_tstamp_ns, fetching_mode, archive_dest,
              sys_ls_handler_, &mysql_proxy_.get_ob_mysql_proxy(), err_handler, *part_trans_parser_,
              cluster_info.cluster_id_, TCONF, start_seq, enable_direct_load_inc))) {
        LOG_ERROR("ObLogMetaDataService init failed", KR(ret), K(start_tstamp_ns));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(start_tenant_service_())) {
    LOG_ERROR("start_tenant_service_ failed", KR(ret));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("init all components done", KR(ret), K(start_tstamp_ns), K_(sys_start_schema_version),
        K(max_cached_trans_ctx_count), K_(is_schema_split_mode), K_(enable_filter_sys_tenant));
  } else {
    do_destroy_(true/*force_destroy*/);
  }

  return ret;
}

// 1. check if is tenant sync mode
// 2. check tb_white_list match tenant
int ObLogInstance::check_sync_mode_()
{
  int ret = OB_SUCCESS;
  const char *tenant_endpoint = TCONF.tenant_endpoint.str();
  const char *tenant_user = TCONF.tenant_user.str();
  const char *tenant_password = TCONF.tenant_password.str();

  if (OB_NOT_NULL(tenant_endpoint) && strlen(tenant_endpoint) > 1
      && OB_NOT_NULL(tenant_user) && strlen(tenant_user) > 1
      && OB_NOT_NULL(tenant_password) && strlen(tenant_password) >= 1) {
    is_tenant_sync_mode_ = true;

    if (! is_data_dict_refresh_mode(refresh_mode_)) {
      LOG_WARN("[NOTICE] detect tenant_sync mode but not using data_dict, will force refresh_mode to data_dict");
      refresh_mode_ = RefreshMode::DATA_DICT;
      enable_filter_sys_tenant_ = true;
    }
  } else if (OB_UNLIKELY((OB_ISNULL(TCONF.cluster_url.str())) || OB_ISNULL(TCONF.rootserver_list.str())
      || OB_ISNULL(TCONF.cluster_user.str())
      || OB_ISNULL(TCONF.cluster_password.str()))) {
    if (is_integrated_fetching_mode(fetching_mode_)) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("invalid config for cluster_sync_mode, integrated_mode but cluster_user info is not valid", KR(ret));
    }
  }
  LOG_INFO("[WORK_MODE]", K_(is_tenant_sync_mode),
      "working_mode", print_working_mode(working_mode_),
      "fetching_mode", print_fetching_mode(fetching_mode_));

  return ret;
}

int ObLogInstance::init_sql_provider_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_sync_mode_())) {
    LOG_ERROR("check_sync_mode_ failed", KR(ret));
  } else if (! is_online_sql_not_available()) {
    const char *ob_user = nullptr;
    const char *ob_password = nullptr;

    if (is_tenant_sync_mode()) {
      ob_user = TCONF.tenant_user.str();
      ob_password = TCONF.tenant_password.str();
      const char *tenant_endpoint = TCONF.tenant_endpoint.str();
      INIT(rs_server_provider_, ObCDCEndpointProvider, tenant_endpoint);
    } else {
      // cluster sync mode
      ob_user = TCONF.cluster_user.str();
      ob_password = TCONF.cluster_password.str();
      // format cluster_url
      if (OB_FAIL(TCONF.format_cluster_url())) {
        LOG_ERROR("format config url fail", KR(ret));
      } else {
        const char *config_url = TCONF.cluster_url.str();
        const char *rs_list = TCONF.rootserver_list.str();
        // init ObLogSQLServerProvider base on config_url or rs_list
        INIT(rs_server_provider_, ObLogSQLServerProvider, config_url, rs_list);
      }
    }

    // init systable_helper_
    if (OB_SUCC(ret)) {
      INIT(systable_helper_, ObLogSysTableHelper, *rs_server_provider_,
          TCONF.access_systable_helper_thread_num, ob_user,
          ob_password, TCONF.cluster_db_name);
    }
    // init RSMysqlProxy
    const bool enable_ssl_client_authentication = (1 == TCONF.ssl_client_authentication);
    if (OB_SUCC(ret)) {
      const int64_t rs_sql_conn_timeout_us = TCONF.rs_sql_connect_timeout_sec * _SEC_;
      const int64_t rs_sql_query_timeout_us = TCONF.rs_sql_query_timeout_sec * _SEC_;
      if (OB_FAIL(mysql_proxy_.init(
          ob_user,
          ob_password,
          rs_sql_conn_timeout_us,
          rs_sql_query_timeout_us,
          enable_ssl_client_authentication,
          rs_server_provider_))) {
        LOG_ERROR("mysql_proxy_ init fail", KR(ret),
            K_(rs_server_provider),
            K(ob_user),
            K(ob_password),
            K(enable_ssl_client_authentication));
      }
    }
    // init tenant sql provider
    if (! is_tenant_sync_mode()) {
      INIT(tenant_server_provider_, ObCDCTenantSQLServerProvider, *systable_helper_);

      // init ObLogTenantSQLProxy
      if (OB_SUCC(ret)) {
        int64_t tenant_sql_conn_timeout_us = TCONF.tenant_sql_connect_timeout_sec * _SEC_;
        int64_t tenant_sql_query_timeout_us = TCONF.tenant_sql_query_timeout_sec * _SEC_;
        if (OB_FAIL(tenant_sql_proxy_.init(
            ob_user,
            ob_password,
            tenant_sql_conn_timeout_us,
            tenant_sql_query_timeout_us,
            enable_ssl_client_authentication,
            tenant_server_provider_,
            true/*is_tenant_server_provider*/))) {
          LOG_ERROR("tenant_sql_proxy_ init fail", KR(ret),
              K(tenant_server_provider_),
              K(tenant_sql_conn_timeout_us),
              K(tenant_sql_query_timeout_us),
              K(enable_ssl_client_authentication));
        }
      }
    } else {
      // query tenant_id
      ObArray<uint64_t> tenant_id_list;
      if (OB_ISNULL(systable_helper_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid systable_helper_", KR(ret));
      } else if (OB_FAIL(systable_helper_->query_tenant_id_list(tenant_id_list))) {
        LOG_ERROR("query_tenant_id_list failed", KR(ret), K(tenant_id_list));
      } else if (OB_UNLIKELY(tenant_id_list.count() <= 0)) {
        LOG_ERROR("empty tenant_id_list in tenant sync mode", KR(ret), K(tenant_id_list));
      } else if (OB_UNLIKELY(tenant_id_list.count() > 1)) {
        LOG_ERROR("too much tenant_id found in tenant sync mode", KR(ret), K(tenant_id_list));
      } else {
        tenant_id_ = tenant_id_list.at(0);
        if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("expect user tenant in tenant_sync_mode", KR(ret), K_(tenant_id));
        }
      }
    }
  }

  return ret;
}

int ObLogInstance::set_all_tenant_compat_mode_()
{
  int ret = OB_SUCCESS;
  std::vector<uint64_t> tenant_ids;

  if (OB_FAIL(tenant_mgr_->get_all_tenant_ids(tenant_ids))) {
    LOG_ERROR("tenant_mgr_ get_all_tenant_ids failed", KR(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_ids.size(); ++idx) {
      const uint64_t tenant_id = tenant_ids[idx];
      lib::Worker::CompatMode compat_mode;

      if (OB_FAIL(ObLogInstance::get_tenant_compat_mode(tenant_id, compat_mode))) {
        LOG_ERROR("get_tenant_compat_mode failed", KR(ret), K(tenant_id), K(compat_mode));
      } else if (OB_FAIL(share::ObCompatModeGetter::instance().set_tenant_compat_mode(tenant_id, compat_mode))) {
        LOG_ERROR("ObCompatModeGetter set_tenant_compat_mode failed", KR(ret), K(tenant_id), K(compat_mode));
      } else {}
    } // for
  }

  return ret;
}

int ObLogInstance::update_data_start_schema_version_on_split_mode_()
{
  int ret = OB_SUCCESS;

  if (! is_schema_split_mode_ || NULL == tenant_mgr_) {
    //do nothing
  } else if (OB_FAIL(tenant_mgr_->set_data_start_schema_version_on_split_mode())) {
    LOG_ERROR("set_data_start_schema_version_on_split_mode fail", KR(ret),
        K(is_schema_split_mode_), K(tenant_mgr_));
  } else {
    // succ
  }

  return ret;
}

int ObLogInstance::config_data_start_schema_version_(const int64_t global_data_start_schema_version)
{
  int ret = OB_SUCCESS;
  // Currently only supports non-split mode configuration for all tenants, split mode to be supported TODO
  if (! is_schema_split_mode_ && NULL != tenant_mgr_) {
    LOG_INFO("config data start schema version", K(global_data_start_schema_version),
        K_(is_schema_split_mode));

    // Set a uniform starting schema version for all tenants in non-split mode
    if (global_data_start_schema_version <= 0) {
      LOG_INFO("global_data_start_schema_version is not configured under non schema split mode, "
          "need not set data start schema version",
          K(global_data_start_schema_version), K_(is_schema_split_mode));
    } else if (OB_FAIL(tenant_mgr_->set_data_start_schema_version_for_all_tenant(
        global_data_start_schema_version))) {
      LOG_ERROR("set_data_start_schema_version_for_all_tenant fail", KR(ret),
          K(global_data_start_schema_version), K(is_schema_split_mode_));
    }
  }

  return ret;
}


int ObLogInstance::start_tenant_service_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start_tenant_service_ begin", K_(start_tstamp_ns), K_(sys_start_schema_version));
  // config tenant mgr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(config_tenant_mgr_(start_tstamp_ns_, sys_start_schema_version_))) {
      LOG_ERROR("config_tenant_mgr_ fail", KR(ret), K_(start_tstamp_ns), K_(sys_start_schema_version));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_data_dict_refresh_mode(refresh_mode_)) {
      if (OB_FAIL(set_all_tenant_compat_mode_())) {
        LOG_ERROR("set_all_tenant_compat_mode_ failed", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(config_data_start_schema_version_(TCONF.global_data_start_schema_version))) {
      LOG_ERROR("config_data_start_schema_version_ fail", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_data_start_schema_version_on_split_mode_())) {
      LOG_ERROR("update_data_start_schema_on_split_mode_ fail", KR(ret));
    }
  }
  LOG_INFO("start_tenant_service_ done", KR(ret), K_(start_tstamp_ns), K_(sys_start_schema_version));
  return ret;
}

int ObLogInstance::config_tenant_mgr_(const int64_t start_tstamp_ns,
    const int64_t sys_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_mgr_) || OB_ISNULL(fetcher_)) {
    LOG_ERROR("invaild argument", K(tenant_mgr_), K(fetcher_));
    ret = OB_INVALID_ARGUMENT;
  }

  // Register the "add LS" callback
  // Called sequentially
  // Committer, Sequencer, Fetcher all need to add partitions dynamically
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_mgr_->register_ls_add_callback(fetcher_))) {
      LOG_ERROR("fetcher register_ls_add_callback fail", KR(ret), K(fetcher_));
    } else {
      LOG_INFO("register add-ls-callback succ", K_(committer), K_(sequencer), K_(fetcher));
    }
  }

  // Register the "Recycle LS" callback
  // Called sequentially
  // Fetcher cannot delete immediately, it needs to wait for the partition to be reclaimed, i.e. safely deleted
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_mgr_->register_ls_recycle_callback(fetcher_))) {
      LOG_ERROR("fetcher register_ls_recycle_callback fail", KR(ret), K(fetcher_));
    } else {
      LOG_INFO("register recycle-ls-callback succ", K_(fetcher));
    }
  }

  // Add all tables for all tenants
  // Beforehand, make sure all callbacks are registered
  if (OB_SUCC(ret)) {
    if (is_online_sql_not_available()) {
      bool add_tenant_succ = false;
      const char *tenant_name = nullptr;

      if (OB_FAIL(tenant_mgr_->add_tenant(
          start_tstamp_ns,
          sys_schema_version,
          tenant_name,
          GET_SCHEMA_TIMEOUT_ON_START_UP,
          add_tenant_succ))) {
        LOG_ERROR("add_tenant fail", KR(ret), K(start_tstamp_ns), K(sys_schema_version));
      } else if (! add_tenant_succ) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("[FATAL] [LAUNCH] ADD_TENANT WITH NO ALIVE SERVER MODE FAILED", KR(ret),
            K(start_tstamp_ns), K_(refresh_mode), K_(fetching_mode));
      }
    } else {
      if (OB_FAIL(tenant_mgr_->add_all_tenants(
          start_tstamp_ns,
          sys_schema_version,
          GET_SCHEMA_TIMEOUT_ON_START_UP))) {
        LOG_ERROR("add_all_tenants fail", KR(ret), K(start_tstamp_ns), K(sys_schema_version));
      }
    }
  }

  return ret;
}

void ObLogInstance::destroy_components_()
{
  LOG_INFO("destroy all components begin");

  // Destruction by reverse order
  DESTROY(fetcher_, ObLogFetcher);
  dispatcher_.destroy();
  DESTROY(sys_ls_handler_, ObLogSysLsTaskHandler);
  DESTROY(ddl_parser_, ObLogDdlParser);
  DESTROY(trans_msg_sorter_, ObLogTransMsgSorter);
  DESTROY(dml_parser_, ObLogDmlParser);
  DESTROY(part_trans_parser_, ObLogPartTransParser);
  DESTROY(trans_redo_dispatcher_, ObLogTransRedoDispatcher);
  DESTROY(ddl_processor_, ObLogDDLProcessor);
  DESTROY(sequencer_, ObLogSequencer);
  DESTROY(formatter_, ObLogFormatter);
  DESTROY(lob_data_merger_, ObCDCLobDataMerger);
  lob_aux_meta_storager_.destroy();
  DESTROY(committer_, ObLogCommitter);
  DESTROY(systable_helper_, ObLogSysTableHelper);
  DESTROY(ss_matcher_, ObLogStartSchemaMatcher);
  DESTROY(tb_matcher_, ObLogTableMatcher);
  if (is_online_refresh_mode(refresh_mode_)) {
    DESTROY(schema_getter_, ObLogSchemaGetter);
  }
  DESTROY(meta_manager_, ObLogMetaManager);
  if (! is_online_sql_not_available()) {
    if (is_tenant_sync_mode()) {
      mysql_proxy_.destroy();
      DESTROY(rs_server_provider_, ObCDCEndpointProvider);
    } else {
      tenant_sql_proxy_.destroy();
      DESTROY(tenant_server_provider_, ObCDCTenantSQLServerProvider);
      mysql_proxy_.destroy();
      DESTROY(rs_server_provider_, ObLogSQLServerProvider);
    }
  }
  DESTROY(resource_collector_, ObLogResourceCollector);
  DESTROY(trans_ctx_mgr_, ObLogTransCtxMgr);
  DESTROY(trans_stat_mgr_, ObLogTransStatMgr);
  DESTROY(tenant_mgr_, ObLogTenantMgr);
  DESTROY(log_entry_task_pool_, ObLogEntryTaskPool);
  DESTROY(br_pool_, ObLogBRPool);
  DESTROY(storager_, ObLogStorager);
  DESTROY(reader_, ObLogReader);
  // NOTICE: should not stop and destroy store_service in case progress core at rocksdb deconstruct
  // DESTROY(store_service_, RocksDbStoreService);
  if (is_data_dict_refresh_mode(refresh_mode_)) {
    ObLogMetaDataService::get_instance().destroy();
  }

  LOG_INFO("destroy all components end");
}

void ObLogInstance::destroy()
{
  const bool force_destroy = false;
  do_destroy_(force_destroy);
}

void ObLogInstance::do_destroy_(const bool force_destroy)
{
  do_stop_("DESTROY_OBCDC");

  if (inited_ || force_destroy) {
    LOG_INFO("destroy obcdc begin", K(force_destroy));
    inited_ = false;

    oblog_major_ = 0;
    oblog_minor_ = 0;
    oblog_major_patch_ = 0;
    oblog_minor_patch_ = 0;

    destroy_components_();
    err_cb_ = NULL;

    TCONF.destroy();
    stop_flag_ = true;
    last_heartbeat_timestamp_micro_sec_ = 0;
    trans_stat_mgr_ = NULL;
    tenant_mgr_ = NULL;
    global_errno_ = 0;
    handle_error_flag_ = 0;
    disable_redirect_log_ = false;
    log_clean_cycle_time_us_ = 0;
    global_info_.reset();
    hbase_util_.destroy();
    obj2str_helper_.destroy();
    br_queue_.destroy();
    timer_tid_ = 0;
    sql_tid_ = 0;
    flow_control_tid_ = 0;
    lib::ThreadPool::destroy();

    (void)trans_task_pool_.destroy();
    (void)trans_task_pool_alloc_.destroy();

    output_dml_br_count_ = 0;
    output_ddl_br_count_ = 0;

    ObCDCTimeZoneInfoGetter::get_instance().destroy();
    timezone_info_getter_ = nullptr;
    ObKVGlobalCache::get_instance().destroy();
    ObMemoryDump::get_instance().destroy();
    ObClockGenerator::destroy();

    is_assign_log_dir_valid_ = false;
    MEMSET(assign_log_dir_, 0, sizeof(assign_log_dir_));
    MEMSET(ob_trace_id_str_, 0, sizeof(ob_trace_id_str_));
    br_index_in_trans_ = 0;
    part_trans_task_count_ = 0;
    start_tstamp_ns_ = 0;
    sys_start_schema_version_ = OB_INVALID_VERSION;
    is_schema_split_mode_ = true;
    enable_filter_sys_tenant_ = false;
    working_mode_ = WorkingMode::UNKNOWN_MODE;
    refresh_mode_ = RefreshMode::UNKNOWN_REFRSH_MODE;
    fetching_mode_ = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
    is_tenant_sync_mode_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    LOG_INFO("destroy obcdc end");
  }
}

int ObLogInstance::launch()
{
  int ret = OB_SUCCESS;

  LOG_INFO("launch all components begin");

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    // Reset global error codes at startup
    global_errno_ = OB_SUCCESS;
    stop_flag_ = false;

    if (OB_FAIL(resource_collector_->start())) {
      LOG_ERROR("start resource collector fail", KR(ret));
    } else if (OB_FAIL(storager_->start())) {
      LOG_ERROR("start storager_ fail", KR(ret));
    } else if (OB_FAIL(reader_->start())) {
      LOG_ERROR("start reader_ fail", KR(ret));
    } else if (OB_FAIL(committer_->start())) {
      LOG_ERROR("start committer fail", KR(ret));
    } else if (OB_FAIL(lob_data_merger_->start())) {
      LOG_ERROR("start formatter fail", KR(ret));
    } else if (OB_FAIL(formatter_->start())) {
      LOG_ERROR("start formatter fail", KR(ret));
    } else if (OB_FAIL(trans_msg_sorter_->start())) {
      LOG_ERROR("start sorter fail", KR(ret));
    } else if (OB_FAIL(sequencer_->start())) {
      LOG_ERROR("start sequencer fail", KR(ret));
    } else if (OB_FAIL(dml_parser_->start())) {
      LOG_ERROR("start DML parser fail", KR(ret));
    } else if (OB_FAIL(ddl_parser_->start())) {
      LOG_ERROR("start DDL parser fail", KR(ret));
    } else if (OB_FAIL(sys_ls_handler_->start())) {
      LOG_ERROR("start fetcher fail", KR(ret));
    } else if (OB_FAIL(fetcher_->start())) {
      LOG_ERROR("start fetcher fail", KR(ret));
    } else if (OB_FAIL(start_threads_())) {
      LOG_ERROR("start_threads_ fail", KR(ret));
    } else if (OB_FAIL(timezone_info_getter_->start())) {
      LOG_ERROR("start_timezone_info_thread_ fail", KR(ret));
    } else {
      is_running_ = true;
      LOG_INFO("launch all components end success");
    }
  }

  return ret;
}

void ObLogInstance::stop()
{
  do_stop_("INVOKE_STOP_INTERFACE");
}

void ObLogInstance::do_stop_(const char *stop_reason)
{
  if (inited_) {

    mark_stop_flag(stop_reason);

    LOG_INFO("stop all components begin");

    stop_flag_ = true;

    // stop thread
    wait_threads_stop_();
    // stop timezon info getter
    timezone_info_getter_->stop();

    fetcher_->stop();
    sys_ls_handler_->stop();
    ddl_parser_->stop();
    dml_parser_->stop();
    sequencer_->stop();
    formatter_->stop();
    lob_data_merger_->stop();
    storager_->stop();
    reader_->stop();
    trans_msg_sorter_->stop();
    committer_->stop();
    resource_collector_->stop();
    mysql_proxy_.stop();
    tenant_sql_proxy_.stop();

    // set global error code
    global_errno_ = (global_errno_ == OB_SUCCESS ? OB_IN_STOP_STATE : global_errno_);
    is_running_ = false;

    LOG_INFO("stop all components end");
    LOG_INFO("obcdc stopped", KR(global_errno_), KCSTRING(stop_reason));
  }
}

int ObLogInstance::get_tenant_ids(std::vector<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tenant_mgr_)) {
    LOG_ERROR("tenant_mgr_ is null", K(tenant_mgr_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr_->get_all_tenant_ids(tenant_ids))) {
    LOG_ERROR("get_all_tenant_ids fail", KR(ret));
  } else {
    // succ
  }

  return ret;
}

void ObLogInstance::mark_stop_flag(const char *stop_reason)
{
  stop_flag_ = true;
  if (inited_) {
    if (OB_ISNULL(stop_reason)) {
      stop_reason = "UNKNOWN";
    }
    LOG_INFO("mark_stop_flag begin", K(global_errno_), KCSTRING(stop_reason));

    fetcher_->mark_stop_flag();
    sys_ls_handler_->mark_stop_flag();
    ddl_parser_->mark_stop_flag();
    dml_parser_->mark_stop_flag();
    sequencer_->mark_stop_flag();
    formatter_->mark_stop_flag();
    lob_data_merger_->mark_stop_flag();
    storager_->mark_stop_flag();
    reader_->mark_stop_flag();
    store_service_->mark_stop_flag();
    trans_msg_sorter_->mark_stop_flag();
    committer_->mark_stop_flag();
    resource_collector_->mark_stop_flag();
    timezone_info_getter_->mark_stop_flag();
    lib::ThreadPool::stop();

    LOG_INFO("mark_stop_flag end", K(global_errno_), KCSTRING(stop_reason));
  }
}

int ObLogInstance::next_record(IBinlogRecord **record, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int32_t major_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;

  if (OB_FAIL(next_record(record, major_version, tenant_id, timeout_us))) {
    if (OB_TIMEOUT != ret && OB_IN_STOP_STATE != ret) {
      LOG_ERROR("next record fail", KR(ret), K(record));
    }
  }

  return ret;
}

int ObLogInstance::next_record(IBinlogRecord **record,
    int32_t &major_version,
    uint64_t &tenant_id,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *pop_record = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("instance has not been initialized", KR(ret));
  } else if (OB_ISNULL(record)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(record));
  } else if (OB_UNLIKELY(OB_SUCCESS != global_errno_)) {
    // In case of global error, the corresponding error code is returned, except for OB_TIMEOUT
    ret = (OB_TIMEOUT == global_errno_) ? OB_IN_STOP_STATE : global_errno_;
  } else if (OB_FAIL(br_queue_.pop(pop_record, major_version, tenant_id, timeout_us))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("pop binlog record from br_queue fail", KR(ret));
    }
  } else if (OB_ISNULL(pop_record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("pop binlog record from br_queue fail", KR(ret), K(pop_record));
  } else {
    *record = pop_record;
  }

  if (OB_SUCC(ret)) {
    ObLogBR *oblog_br = NULL;

    if (OB_ISNULL(record) || OB_ISNULL(*record)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("record is invalid", KR(ret), K(record));
    } else if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>((*record)->getUserData()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get user data fail", KR(ret), "br", *record, K(oblog_br));
    } else {
      int record_type = (*record)->recordType();

      if (HEARTBEAT == record_type) {
        int64_t timestamp_usec = (*record)->getTimestamp() * 1000000 + (*record)->getRecordUsec();
        last_heartbeat_timestamp_micro_sec_ =
            std::max(timestamp_usec, last_heartbeat_timestamp_micro_sec_);
      }

      // NOTE: Set the timestamp of the last heartbeat to Checkpoint1 of the data
      (*record)->setCheckpoint(last_heartbeat_timestamp_micro_sec_ / 1000000,
          last_heartbeat_timestamp_micro_sec_ % 1000000);

      if (EDDL == record_type) {
        ATOMIC_INC(&output_ddl_br_count_);
      } else if (EBEGIN == record_type) {
        do_drc_consume_tps_stat_();
      } else if (HEARTBEAT != record_type && ECOMMIT != record_type) {
        ATOMIC_INC(&output_dml_br_count_);
        do_drc_consume_rps_stat_();
      } else {
        // do nothing
      }

      const int64_t part_trans_task_count = oblog_br->get_part_trans_task_count();
      bool need_accumulate_stat = true;
      do_stat_for_part_trans_task_count_(record_type, part_trans_task_count, need_accumulate_stat);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(verify_ob_trace_id_(*record))) {
      LOG_ERROR("verify_ob_trace_id_ fail", KR(ret), K(record), K(*record));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == TCONF.enable_verify_mode) {
      // do nothing
    } else {
      if (OB_FAIL(verify_dml_unique_id_(*record))) {
        LOG_ERROR("verify_dml_unique_id_ fail", KR(ret), K(record), K(*record));
      } else if (OB_FAIL(verify_ddl_schema_version_(*record))) {
        LOG_ERROR("verify_ddl_schema_version_ fail", KR(ret), K(record), K(*record));
      } else {
      }
    }
  }

  return ret;
}

int ObLogInstance::verify_ob_trace_id_(IBinlogRecord *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("invalid arguments", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == TCONF.need_verify_ob_trace_id) {
    // do nothing
  } else {
    int record_type = br->recordType();

    if (EINSERT == record_type || EUPDATE == record_type || EDELETE == record_type || EPUT == record_type) {
      // only verify insert\update\delete type
      const ObString ob_trace_id_config(ob_trace_id_str_);
      ObLogBR *oblog_br = NULL;
      ObLogEntryTask *log_entry_task = NULL;
      PartTransTask *task = NULL;

      if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>(br->getUserData()))) {
        LOG_ERROR("get user data fail", K(br), K(oblog_br));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(log_entry_task = static_cast<ObLogEntryTask *>(oblog_br->get_host()))) {
        LOG_ERROR("log_entry_task is NULL", KPC(log_entry_task));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
        LOG_ERROR("part trans task is null", KPC(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObString trace_id;
        const int64_t trace_id_idx = 2;

        if (OB_FAIL(get_br_filter_value_(*br, trace_id_idx, trace_id))) {
          LOG_ERROR("get_br_filter_value_ fail", KR(ret), K(trace_id_idx), K(trace_id));
        } else {
          if (0 == ob_trace_id_config.compare(trace_id)) {
            // succ
          } else {
            LOG_ERROR("verify_ob_trace_id fail", K(trace_id), K(ob_trace_id_config), KPC(task));
            ret = OB_ITEM_NOT_MATCH;
          }
        }
      }
    } // record_type
  }

  return ret;
}

int ObLogInstance::verify_ddl_schema_version_(IBinlogRecord *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("invalid arguments", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int record_type = br->recordType();

    if (EDDL == record_type) {
      ObLogBR *oblog_br = NULL;
      PartTransTask *task = NULL;
      int64_t new_cols_count = 0;
      binlogBuf *new_cols = br->newCols((unsigned int &)new_cols_count);
      int64_t ddl_schema_version_index = 1;

      // Currently ddl br only synchronizes two columns, ddl_stmt_str and ddl_schema_version
      if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>(br->getUserData()))) {
        LOG_ERROR("get user data fail", K(br), K(oblog_br));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(task = static_cast<PartTransTask *>(oblog_br->get_host()))) {
        LOG_ERROR("part trans task is null", KPC(task));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_UNLIKELY(2 != new_cols_count)) {
        LOG_ERROR("ddl br new cols count is not equal 2", K(new_cols_count));
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObString br_ddl_schema_version(new_cols[ddl_schema_version_index].buf_used_size,
            new_cols[ddl_schema_version_index].buf);

        int64_t ddl_schema_version = oblog_br->get_schema_version();
        const int64_t ddl_schema_version_str_len = DdlStmtTask::MAX_DDL_SCHEMA_VERSION_STR_LENGTH;
        char ddl_schema_version_str[ddl_schema_version_str_len];
        int64_t pos = 0;

        if (OB_FAIL(databuff_printf(ddl_schema_version_str, ddl_schema_version_str_len,
              pos, "%ld", ddl_schema_version))) {
          LOG_ERROR("databuff_printf fail", KR(ret), K(ddl_schema_version),
            K(ddl_schema_version_str), K(pos));
        } else if (0 == br_ddl_schema_version.compare(ddl_schema_version_str)) {
          // succ
        } else {
          LOG_ERROR("verify_ddl_schema_version_ fail", K(br_ddl_schema_version),
              K(ddl_schema_version_str), KPC(task));
          ret = OB_ITEM_NOT_MATCH;
        }
      }
    }
  }

  return ret;
}

int ObLogInstance::verify_dml_unique_id_(IBinlogRecord *br)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("invalid arguments", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Adding a self-checking adjacent record is a different scenario
    static logservice::TenantLSID last_tls_id;
    static palf::LSN last_commit_log_lsn;
    static palf::LSN last_redo_log_lsn;
    static uint64_t last_row_index = OB_INVALID_ID;

    int record_type = br->recordType();

    if (EINSERT == record_type || EUPDATE == record_type || EDELETE == record_type || EPUT == record_type) {
      // only verify insert\update\delete type
      ObLogBR *oblog_br = NULL;
      ObLogEntryTask *log_entry_task = NULL;
      PartTransTask *task = NULL;
      palf::LSN redo_log_lsn;

      if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>(br->getUserData()))) {
        LOG_ERROR("get user data fail", K(br), K(oblog_br));
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_ISNULL(log_entry_task = static_cast<ObLogEntryTask *>(oblog_br->get_host()))) {
        LOG_ERROR("log_entry_task is NULL", KPC(log_entry_task));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
        LOG_ERROR("part trans task is null", KPC(task));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(log_entry_task->get_log_lsn(redo_log_lsn))) {
        LOG_ERROR("log_entry_task get_log_offset fail", KR(ret), K(log_entry_task), K(redo_log_lsn));
      } else {
        // binlog record set unique id
        ObString br_unique_id;
        const int64_t br_unique_id_idx = 1;
        common::ObString dml_unique_id;
        uint64_t row_index = oblog_br->get_row_index();
        DmlStmtUniqueID dml_stmt_unique_id(task->get_part_trans_info(), redo_log_lsn, row_index);

        if (OB_UNLIKELY(! dml_stmt_unique_id.is_valid())) {
          LOG_ERROR("dml_stmt_unique_id is not valid", K(dml_stmt_unique_id));
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(get_br_filter_value_(*br, br_unique_id_idx, br_unique_id))) {
          LOG_ERROR("get_br_filter_value_ fail", KR(ret), K(br_unique_id_idx), K(br_unique_id));
        } else {
          const int64_t buf_len = dml_stmt_unique_id.get_dml_unique_id_length() + 1;
          // verify current br
          char buf[buf_len];
          int64_t pos = 0;

          if (OB_FAIL(dml_stmt_unique_id.customized_to_string(buf, buf_len, pos))) {
            LOG_ERROR("init_dml_unique_id_ fail", KR(ret), K(buf_len), K(pos));
          } else {
            dml_unique_id.assign_ptr(buf, static_cast<int32_t>(pos));

            if (0 == br_unique_id.compare(dml_unique_id)) {
              // succ
            } else {
              LOG_ERROR("verify_dml_unique_id_ fail", K(br_unique_id), K(dml_unique_id), KPC(task));
              ret = OB_ITEM_NOT_MATCH;
            }
          }

          if (OB_SUCC(ret)) {
            // Verify Adjacent br
            if (! last_commit_log_lsn.is_valid() || ! last_redo_log_lsn.is_valid()) {
              // won't verify for the first time
            } else {
              if (last_tls_id == task->get_tls_id()
                  && last_commit_log_lsn == task->get_commit_log_lsn()
                  && last_redo_log_lsn == redo_log_lsn
                  && last_row_index == row_index) {
                LOG_ERROR("current br_unique_id should not be equal to last_br_unique_id",
                    K(br_unique_id), KPC(task), K(row_index), K(redo_log_lsn), K(last_redo_log_lsn),
                    K(last_tls_id), K(last_commit_log_lsn), K(last_row_index));
                ret = OB_ERR_UNEXPECTED;
              } else {
                // succ
              }
            }
          } // OB_SUCC(ret)

          if (OB_SUCC(ret)) {
            last_tls_id = task->get_tls_id();
            last_commit_log_lsn = task->get_commit_log_lsn();
            last_redo_log_lsn = redo_log_lsn;
            last_row_index = row_index;
          }
        }
      }
    }
  }

  return ret;
}

int ObLogInstance::get_br_filter_value_(IBinlogRecord &br,
    const int64_t idx,
    common::ObString &str)
{
  int ret = OB_SUCCESS;
  BinlogRecordImpl *br_impl = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br_impl = dynamic_cast<BinlogRecordImpl *>(&br))) {
    LOG_ERROR("invalid arguments", K(br_impl));
    ret = OB_INVALID_ARGUMENT;
  } else {
    unsigned int filter_rv_count = 0;
    const binlogBuf *filter_value = br_impl->filterValues((unsigned int &) filter_rv_count);
    const binlogBuf *str_buf = filter_value + idx;

    if (OB_ISNULL(str_buf)) {
      LOG_ERROR("str_buf is NULL");
      ret = OB_ERR_UNEXPECTED;
    } else {
      str.assign_ptr(str_buf->buf, str_buf->buf_used_size);
    }
  }

  return ret;
}

void ObLogInstance::release_record(IBinlogRecord *record)
{
  int ret = OB_SUCCESS;
  if (inited_ && NULL != record) {
    int record_type = record->recordType();
    ObLogBR *br = reinterpret_cast<ObLogBR *>(record->getUserData());

    if (OB_ISNULL(br)) {
      LOG_ERROR("binlog record user data is NULL", K(record));
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (EDDL == record_type) {
        ATOMIC_DEC(&output_ddl_br_count_);
      } else if (EBEGIN == record_type) {
        do_drc_release_tps_stat_();
      } else if (HEARTBEAT != record_type && ECOMMIT != record_type) {
        ATOMIC_DEC(&output_dml_br_count_);
        do_drc_release_rps_stat_();
      } else {
        // do nothing
      }

      const int64_t part_trans_task_count = br->get_part_trans_task_count();
      bool need_accumulate_stat = false;
      do_stat_for_part_trans_task_count_(record_type, part_trans_task_count, need_accumulate_stat);

      if (OB_FAIL(resource_collector_->revert(record_type, br))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert binlog record fail", KR(ret), K(br),
              "record_type", print_record_type(record_type));
        }
      } else {
        br = NULL;
        record = NULL;
      }
    }
  }
}

void ObLogInstance::handle_error(const int err_no, const char *fmt, ...)
{
  static const int64_t MAX_ERR_MSG_LEN = 1024;
  static char err_msg[MAX_ERR_MSG_LEN];

  if (inited_) {
    // Call the error callback only once
    if (0 == ATOMIC_CAS(&handle_error_flag_, 0, 1)) {
      va_list ap;
      va_start(ap, fmt);
      vsnprintf(err_msg, sizeof(err_msg), fmt, ap);
      va_end(ap);

      global_errno_ = (err_no == OB_SUCCESS ? OB_IN_STOP_STATE : err_no);
      _LOG_INFO("HANDLE_ERROR: err_cb=%p, errno=%d, errmsg=\"%s\"", err_cb_, err_no, err_msg);

      if (NULL != err_cb_) {
        ObCDCError err;
        err.level_ = ObCDCError::ERR_ABORT; // FIXME: Support for other types of error levels
        err.errno_ = err_no;
        err.errmsg_ = err_msg;

        LOG_INFO("ERROR_CALLBACK begin", KP(err_cb_));
        err_cb_(err);
        LOG_INFO("ERROR_CALLBACK end", KP(err_cb_));
      } else {
        LOG_ERROR_RET(OB_ERROR, "No ERROR CALLBACK function available, abort now");
      }

      // notify other module to stop
      mark_stop_flag("ERROR_CALLBACK");
    }
  }
}

int32_t ObLogInstance::get_log_level() const
{
  return OB_LOGGER.get_log_level();
}

const char *ObLogInstance::get_log_file() const
{
  return DEFAULT_LOG_FILE;
}

void ObLogInstance::write_pid_file_()
{
  int pid_file_fd = -1;
  const char *pid_file = DEFAULT_PID_FILE;
  char pid_file_dir[32] = {};

  (void)snprintf(pid_file_dir, sizeof(pid_file_dir), "%s", DEFAULT_PID_FILE_DIR);
  common::FileDirectoryUtils::create_full_path(pid_file_dir);

  pid_file_fd = open(pid_file, O_RDWR | O_CREAT, 0600);
  if (OB_UNLIKELY(pid_file_fd < 0)) {
    LOG_ERROR_RET(OB_ERR_SYS, "open pid file fail", K(pid_file), K(pid_file_fd), K(errno), KERRMSG);
  } else {
    char buf[32] = {};
    (void)snprintf(buf, sizeof(buf), "%d\n", getpid());
    (void)ftruncate(pid_file_fd, 0);

    ssize_t len = strlen(buf);
    ssize_t nwrite = write(pid_file_fd, buf, len);
    if (OB_UNLIKELY(len != nwrite)) {
      LOG_ERROR_RET(OB_ERR_SYS, "write pid file fail", K(pid_file), K(pid_file_fd),
          K(buf), K(len), K(errno), KERRMSG);
    }

    close(pid_file_fd);
    pid_file_fd = -1;
  }
}

void *ObLogInstance::timer_thread_func_(void *args)
{
  if (NULL != args) {
    ObLogInstance *instance = static_cast<ObLogInstance *>(args);
    instance->timer_routine();
  }

  return NULL;
}

void *ObLogInstance::sql_thread_func_(void *args)
{
  if (NULL != args) {
    ObLogInstance *instance = static_cast<ObLogInstance *>(args);
    instance->sql_thread_routine();
  }

  return NULL;
}

void *ObLogInstance::flow_control_thread_func_(void *args)
{
  if (NULL != args) {
    ObLogInstance *instance = static_cast<ObLogInstance *>(args);
    instance->flow_control_thread_routine();
  }

  return NULL;
}

void ObLogInstance::sql_thread_routine()
{
  int ret = OB_SUCCESS;
  const static int64_t THREAD_INTERVAL = 1 * _SEC_;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      int64_t cluster_version_refresh_interval_sec = TCONF.cluster_version_refresh_interval_sec * _SEC_;

      // refresh SQL SERVER list
      if (REACH_TIME_INTERVAL(REFRESH_SERVER_LIST_INTERVAL)) {
        if (is_integrated_fetching_mode(fetching_mode_)) {
          if (is_tenant_sync_mode()) {
            if (OB_ISNULL(systable_helper_)) {
              ret = OB_INVALID_DATA;
              LOG_ERROR("invalid systable_helper_ while refresh tenant endpoint", KR(ret));
            } else if (OB_FAIL(systable_helper_->refresh_tenant_endpoint())) {
              LOG_ERROR("refresh_tenant_endpoint failed", KR(ret));
            }
          } else {
            ObLogSQLServerProvider *rs_server_provider = static_cast<ObLogSQLServerProvider *>(rs_server_provider_);
            if (OB_ISNULL(rs_server_provider)) {
              LOG_ERROR("rs_server_provider is NULL", K(rs_server_provider));
              ret = OB_ERR_UNEXPECTED;
            } else {
              rs_server_provider->call_refresh_server_list();
            }
          }
        }
      }

      // refresh cluster version
      if (REACH_TIME_INTERVAL(cluster_version_refresh_interval_sec)) {
        if (is_integrated_fetching_mode(fetching_mode_)) {
          (void)update_cluster_version_();
        }

        // check observer versin <= libobcdc version
        if (OB_FAIL(check_observer_version_valid_())) {
          LOG_ERROR("check_observer_version_valid_ fail", KR(ret));
        }
      }

      ob_usleep(THREAD_INTERVAL);
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
      handle_error(ret, "sql thread exits, err=%d", ret);
      stop_flag_ = true;
    }
  }

  LOG_INFO("instance sql thread exits", KR(ret), K_(stop_flag));
}

void ObLogInstance::flow_control_thread_routine()
{
  int ret = OB_SUCCESS;
  const static int64_t THREAD_INTERVAL = 100 * 1000;  // Flow control takes 100ms at a time

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      // flow control
      global_flow_control_();

      ob_usleep(THREAD_INTERVAL);
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
      handle_error(ret, "flow control thread exits, err=%d", ret);
      stop_flag_ = true;
    }
  }

  LOG_INFO("instance flow control thread exits", KR(ret), K_(stop_flag));
}

void ObLogInstance::timer_routine()
{
  int ret = OB_SUCCESS;
  const static int64_t TIMER_INTERVAL = 1 * _SEC_;
  const static int64_t PRINT_INTERVAL = 10 * _SEC_;
  int64_t clean_log_interval = CLEAN_LOG_INTERVAL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("instance has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      // Periodic reload configuration
      if (REACH_TIME_INTERVAL(RELOAD_CONFIG_INTERVAL)) {
        reload_config_();
      }

      // Periodic printing of statistical information
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        _LOG_INFO("OBCDC RUNNING STATUS: [START_TS %s(%ld)][WORK_MODE: %s][META_REFRESH_MODE:%s][LOG_FETCH_MODE:%s]",
            NTS_TO_STR(start_tstamp_ns_), start_tstamp_ns_,
            print_working_mode(working_mode_),
            print_refresh_mode(refresh_mode_),
            print_fetching_mode(fetching_mode_));
        print_tenant_memory_usage_();
        dump_malloc_sample_();
        if (is_online_refresh_mode(refresh_mode_)) {
          schema_getter_->print_stat_info();
        }
        tenant_mgr_->print_stat_info();
        trans_task_pool_.print_stat_info();
        log_entry_task_pool_->print_stat_info();
        br_pool_->print_stat_info();
        trans_ctx_mgr_->print_stat_info();
        print_trans_stat_();
        resource_collector_->print_stat_info();
        reader_->print_stat_info();
        lob_aux_meta_storager_.print_stat_info();
        part_trans_parser_->print_stat_info();
      }

      // Periodic memory recycling
      if (REACH_TIME_INTERVAL(ObLogSchemaGetter::RECYCLE_MEMORY_INTERVAL)) {
        if (is_online_refresh_mode(refresh_mode_)) {
          schema_getter_->try_recycle_memory();
        }
      }

      // Cycle Cleanup Log
      int64_t clean_cycle = ATOMIC_LOAD(&log_clean_cycle_time_us_);
      clean_log_interval = std::min(clean_log_interval, clean_cycle);
      if (clean_cycle > 0 && REACH_TIME_INTERVAL(clean_log_interval)) {
        clean_log_();
      }

      ob_usleep(TIMER_INTERVAL);
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
      handle_error(ret, "timer thread exits, err=%d", ret);
      stop_flag_ = true;
    }
  }

  LOG_INFO("instance timer thread exits", KR(ret), K_(stop_flag));
}

int ObLogInstance::start_threads_()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (OB_UNLIKELY(0 != timer_tid_)) {
    LOG_ERROR("timer thread has been started", K(timer_tid_));
    ret = OB_NOT_SUPPORTED;
  } else if (0 != (pthread_ret = pthread_create(&timer_tid_, NULL, timer_thread_func_, this))) {
    LOG_ERROR("start timer thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(0 != sql_tid_)) {
    LOG_ERROR("sql thread has been started", K(sql_tid_));
    ret = OB_NOT_SUPPORTED;
  } else if (0 != (pthread_ret = pthread_create(&sql_tid_, NULL, sql_thread_func_, this))) {
    LOG_ERROR("start sql thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(0 != flow_control_tid_)) {
    LOG_ERROR("flow control thread has been started", K(flow_control_tid_));
    ret = OB_NOT_SUPPORTED;
  } else if (0 != (pthread_ret = pthread_create(&flow_control_tid_, NULL, flow_control_thread_func_, this))) {
    LOG_ERROR("start flow control thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_ERROR("start daemon threads failed", KR(ret), K(DAEMON_THREAD_COUNT));
  } else {
    LOG_INFO("start instance threads succ", K(timer_tid_), K(sql_tid_), K(flow_control_tid_));
  }

  return ret;
}

void ObLogInstance::wait_threads_stop_()
{
  if (0 != timer_tid_) {
    int pthread_ret = pthread_join(timer_tid_, NULL);
    if (0 != pthread_ret) {
      LOG_ERROR_RET(OB_ERR_SYS, "join timer thread fail", K(timer_tid_), K(pthread_ret),
          KERRNOMSG(pthread_ret));
    } else {
      LOG_INFO("stop timer thread succ", K(timer_tid_));
    }

    timer_tid_ = 0;
  }

  if (0 != sql_tid_) {
    int pthread_ret = pthread_join(sql_tid_, NULL);
    if (0 != pthread_ret) {
      LOG_ERROR_RET(OB_ERR_SYS, "join sql thread fail", K(sql_tid_), K(pthread_ret),
          KERRNOMSG(pthread_ret));
    } else {
      LOG_INFO("stop sql thread succ", K(sql_tid_));
    }

    sql_tid_ = 0;
  }

  if (0 != flow_control_tid_) {
    int pthread_ret = pthread_join(flow_control_tid_, NULL);
    if (0 != pthread_ret) {
      LOG_ERROR_RET(OB_ERR_SYS, "join flow control thread fail", K(flow_control_tid_), K(pthread_ret),
          KERRNOMSG(pthread_ret));
    } else {
      LOG_INFO("stop flow control thread succ", K(flow_control_tid_));
    }

    flow_control_tid_ = 0;
  }

  LOG_INFO("wait daemon threads stop");
  lib::ThreadPool::wait();
  LOG_INFO("wait daemon threads stop done");
}

void ObLogInstance::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = lib::ThreadPool::get_thread_idx();
  const int64_t thread_count = lib::ThreadPool::get_thread_count();

  if (OB_UNLIKELY(thread_count != DAEMON_THREAD_COUNT || thread_idx >= thread_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid thread count or thread idx", KR(ret), K(thread_idx), K(thread_count), K(DAEMON_THREAD_COUNT));
  } else if (0 == thread_idx) {
    // handle storage_operation
    lib::set_thread_name("CDC-BGD-STORAGE_OP");
    if (OB_FAIL(daemon_handle_storage_op_thd_())) {
      LOG_ERROR("handle_storage_op in background failed", KR(ret), K(thread_idx), K(thread_count));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpect daemon thread", KR(ret), K(thread_count), K(thread_idx));
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    handle_error(ret, "obcdc daemon thread[idx=%ld] exits, err=%d", lib::ThreadPool::get_thread_idx(), ret);
    mark_stop_flag("DAEMON THEAD EXIST");
  }
}

int ObLogInstance::daemon_handle_storage_op_thd_()
{
  int ret = OB_SUCCESS;
  const int64_t TIMER_INTERVAL = 5 * _SEC_;

  while (OB_SUCC(ret) && ! lib::ThreadPool::has_set_stop()) {
    ob_usleep(TIMER_INTERVAL);
    ObLogTraceIdGuard trace_guard;

    if (! is_memory_working_mode(working_mode_)) {
      if (OB_ISNULL(tenant_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid tenant_mgr", KR(ret));
      } else {
        const int64_t redo_flush_interval = TCONF.rocksdb_flush_interval.get();
        const int64_t redo_compact_interval = TCONF.rocksdb_compact_interval.get();
        if (redo_flush_interval > 0 && REACH_TIME_INTERVAL(redo_flush_interval)) {
          tenant_mgr_->flush_storaged_redo();
        }
        if (redo_compact_interval > 0 && REACH_TIME_INTERVAL(redo_compact_interval)) {
          tenant_mgr_->compact_storaged_redo();
        }
      }
    }
  }

  return ret;
}

void ObLogInstance::reload_config_()
{
  int ret = OB_SUCCESS;
  ObLogConfig &config = TCONF;

  _LOG_INFO("====================reload config begin====================");

  if (OB_FAIL(config.load_from_file(config.config_fpath))) {
    LOG_ERROR("load_from_file fail", KR(ret), K(config.config_fpath));
  } else {
    CDC_CFG_MGR.configure(config);
    const int64_t max_log_file_count = config.max_log_file_count;
    const bool enable_log_limit = (1 == config.enable_log_limit);
    LOG_INFO("reset log config", "log_level", config.log_level.str(), K(max_log_file_count));
    OB_LOGGER.set_mod_log_levels(config.log_level.str());
    OB_LOGGER.set_max_file_index(max_log_file_count);
    OB_LOGGER.set_enable_log_limit(enable_log_limit);

    ATOMIC_STORE(&log_clean_cycle_time_us_, config.log_clean_cycle_time_in_hours * _HOUR_);

    if (0 != config.enable_dump_pending_trans_info) {
      dump_pending_trans_info_();
    }

    // config fetcher
    if (OB_NOT_NULL(fetcher_)) {
      fetcher_->configure(config);
    }

    if (OB_NOT_NULL(sys_ls_handler_)) {
      sys_ls_handler_->configure(config);
    }

    // config redo_dispatcher
    if (OB_NOT_NULL(trans_redo_dispatcher_)) {
      trans_redo_dispatcher_->configure(config);
    }
    // config sequencer
    if (OB_NOT_NULL(sequencer_)) {
      sequencer_->configure(config);
    }

    // config committer_
    if (OB_NOT_NULL(committer_)) {
      committer_->configure(config);
    }

    // cofig lob storager
    if (OB_SUCC(ret)) {
      lob_aux_meta_storager_.configure(config);
    }

    // config rs_server_provider_
    if (OB_NOT_NULL(rs_server_provider_)) {
      if (is_tenant_sync_mode()) {
        ObCDCEndpointProvider *endpoint_provider = static_cast<ObCDCEndpointProvider*>(rs_server_provider_);
        if (OB_ISNULL(endpoint_provider)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("endpoint_provider invalid", KR(ret));
        } else {
          endpoint_provider->configure(config);
        }
      } else {
        ObLogSQLServerProvider *oblog_rs_server_provider = static_cast<ObLogSQLServerProvider *>(rs_server_provider_);
        ObCDCTenantSQLServerProvider *obcdc_tenant_sql_server_provider =
            static_cast<ObCDCTenantSQLServerProvider*>(tenant_server_provider_);

        if (OB_ISNULL(oblog_rs_server_provider) || OB_ISNULL(obcdc_tenant_sql_server_provider)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("oblog_rs_server_provider or obcdc_tenant_sql_server_provider is NULL", KR(ret),
              K(oblog_rs_server_provider), K(obcdc_tenant_sql_server_provider));
        } else {
          oblog_rs_server_provider->configure(config);
          obcdc_tenant_sql_server_provider->configure(config);
        }
      }
    }
  }

  _LOG_INFO("====================reload config end====================");
}

void ObLogInstance::print_tenant_memory_usage_()
{
  int ret = OB_SUCCESS;
  lib::ObMallocAllocator *mallocator = lib::ObMallocAllocator::get_instance();

  if (OB_ISNULL(mallocator)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "mallocator is NULL, can not print_tenant_memory_usage");
  } else if (OB_ISNULL(tenant_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant_mgr is NULL, can not print_tenant_memory_usage for each tenant");
  } else {
    std::vector<uint64_t> tenant_ids;
    if (enable_filter_sys_tenant_) {
      //.print sys tenant memory usage here
      mallocator->print_tenant_memory_usage(OB_SYS_TENANT_ID);
      mallocator->print_tenant_ctx_memory_usage(OB_SYS_TENANT_ID);
    }
    mallocator->print_tenant_memory_usage(OB_SERVER_TENANT_ID);
    mallocator->print_tenant_ctx_memory_usage(OB_SERVER_TENANT_ID);

    if (OB_FAIL(tenant_mgr_->get_all_tenant_ids(tenant_ids))) {
      LOG_ERROR("get_all_tenant_ids failed", KR(ret));
    } else {
      for (auto tenant_id: tenant_ids) {
        mallocator->print_tenant_memory_usage(tenant_id);
        mallocator->print_tenant_ctx_memory_usage(tenant_id);
      }
    }
  }
}

void ObLogInstance::dump_malloc_sample_()
{
  int ret = OB_SUCCESS;
  static int64_t last_print_ts = 0;
  lib::ObMallocSampleMap malloc_sample_map;
  ObCDCMallocSampleInfo sample_info;
  const int64_t cur_time = get_timestamp();
  const int64_t print_interval = TCONF.print_mod_memory_usage_interval.get();

  if (OB_LIKELY(last_print_ts + print_interval > cur_time)) {
  } else if (OB_FAIL(malloc_sample_map.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
    LOG_WARN("init malloc_sample_map failed", KR(ret));
  } else if (OB_FAIL(ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map))) {
    LOG_WARN("load_malloc_sample_map failed", KR(ret));
  } else if (OB_FAIL(sample_info.init(malloc_sample_map))) {
    LOG_ERROR("init ob_cdc_malloc_sample_info failed", KR(ret));
  } else {
    const static int64_t top_mem_usage_mod_print_num = 5;
    const int64_t print_mod_memory_usage_threshold = TCONF.print_mod_memory_usage_threshold.get();
    const char *print_mod_memory_usage_label = TCONF.print_mod_memory_usage_label;
    sample_info.print_topk(top_mem_usage_mod_print_num);
    sample_info.print_with_filter(print_mod_memory_usage_label, print_mod_memory_usage_threshold);
    last_print_ts = cur_time;
  }
}

/// Global traffic control
/// Principle: 1. Keep the total number of active Partition Transaction Tasks (PartTransTask) under control by referring to the number of
///        Match the production rate with the consumption rate to avoid OOM
///            2. Consider libobcdc memory usage, when the memory usage reaches a certain limit, flow control should be performed to avoid OOM
///
/// Implementation: 1. Check the number of active partition transaction tasks periodically, and when it exceeds the upper bound, check whether there are enough reusable
///        Partitioned transaction tasks, if they exist, stop Fetcher; otherwise, turn on Fetcher.
//                  2. periodically check the total memory occupied by libobcdc, and when it exceeds the upper bound, check if there are enough reusable
//         partitioned transaction tasks, if they exist, stop Fetcher; otherwise turn on Fetcher.
void ObLogInstance::global_flow_control_()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    if (OB_ISNULL(fetcher_) || OB_ISNULL(dml_parser_)
        || OB_ISNULL(formatter_)
        || OB_ISNULL(sys_ls_handler_) || OB_ISNULL(resource_collector_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid arguments", KR(ret), K(fetcher_), K(dml_parser_),
          K(formatter_), K(sys_ls_handler_), K(resource_collector_));
    } else {
      int64_t part_trans_task_active_count_upper_bound =
        CDC_CFG_MGR.get_part_trans_task_active_count_upper_bound();
      int64_t part_trans_task_reusable_count_upper_bound =
        CDC_CFG_MGR.get_part_trans_task_reusable_count_upper_bound();
      int64_t ready_to_seq_task_upper_bound =
        CDC_CFG_MGR.get_ready_to_seq_task_upper_bound();
      int64_t storager_task_count_upper_bound =
        CDC_CFG_MGR.get_storager_task_count_upper_bound();
      int64_t storager_mem_percentage =
        CDC_CFG_MGR.get_storager_mem_percentage();
      double system_memory_avail_percentage_lower_bound =
        static_cast<double>(TCONF.system_memory_avail_percentage_lower_bound) / 100;
      double memory_usage_warn_percent = TCONF.memory_usage_warn_threshold / 100.0;
      int64_t memory_limit = CDC_CFG_MGR.get_memory_limit();
      int64_t memory_warn_usage = memory_limit * memory_usage_warn_percent;
      int64_t redo_mem_limit = CDC_CFG_MGR.get_redo_dispatcher_memory_limit();
      int64_t redo_mem_usage = trans_redo_dispatcher_->get_dispatched_memory_size();

      int64_t total_part_trans_task_count = trans_task_pool_.get_total_count();
      int64_t active_part_trans_task_count = trans_task_pool_.get_alloc_count();
      int64_t active_log_entry_task_count = log_entry_task_pool_->get_alloc_count();
      int64_t reusable_part_trans_task_count = 0;
      int64_t ddl_part_trans_count = 0;
      int64_t ready_to_seq_task_count = 0;
      int64_t seq_queue_trans_count = 0;

      int64_t fetcher_part_trans_task_count = fetcher_->get_part_trans_task_count();
      int64_t dml_parser_part_trans_task_count = 0;
      int64_t br_queue_part_trans_task_count = br_queue_.get_part_trans_task_count();
      int64_t out_part_trans_task_count = get_out_part_trans_task_count_();
      int64_t resource_collector_part_trans_task_count = 0;
      int64_t resource_collector_br_count = 0;
      resource_collector_->get_task_count(resource_collector_part_trans_task_count, resource_collector_br_count);
      int64_t committer_ddl_part_trans_task_count = 0;
      int64_t committer_dml_part_trans_task_count = 0;
      committer_->get_part_trans_task_count(committer_ddl_part_trans_task_count,
          committer_dml_part_trans_task_count);

      int64_t memory_hold = get_memory_hold_();
      int64_t system_memory_avail = get_memory_avail_();
      int64_t system_memory_limit = get_memory_limit_();
      int64_t system_memory_avail_lower_bound =
        static_cast<int64_t>(static_cast<double>(system_memory_limit) * system_memory_avail_percentage_lower_bound);
      bool need_slow_down_fetcher = false;
      const bool need_pause_dispatch = need_pause_redo_dispatch();
      const bool touch_memory_warn_limit = (memory_hold > memory_warn_usage);
      const bool is_storage_work_mode = is_storage_working_mode(working_mode_);
      const int64_t queue_backlog_lowest_tolerance = TCONF.queue_backlog_lowest_tolerance;
      const char *reason = "";

      if (OB_FAIL(get_task_count_(ready_to_seq_task_count, seq_queue_trans_count, reusable_part_trans_task_count, ddl_part_trans_count))) {
        LOG_ERROR("get_task_count fail", KR(ret), K(ready_to_seq_task_count), K(seq_queue_trans_count),
            K(reusable_part_trans_task_count), K(ddl_part_trans_count));
      } else if (OB_FAIL(dml_parser_->get_log_entry_task_count(dml_parser_part_trans_task_count))) {
        LOG_ERROR("DML parser get_log_entry_task_count fail", KR(ret), K(dml_parser_part_trans_task_count));
      } else {
        const bool exist_trans_sequenced_not_handled = (seq_queue_trans_count > queue_backlog_lowest_tolerance);
        const bool exist_ddl_processing_or_in_queue = (ddl_part_trans_count > queue_backlog_lowest_tolerance);
        int64_t storager_task_count = 0;
        int64_t storager_log_count = 0;
        storager_->get_task_count(storager_task_count, storager_log_count);

        // Use the following policy for global traffic control:
        // need_slow_down =
        // (1) (active partitioned transaction tasks exceed the upper limit || liboblog takes up more memory than the upper limit || system free memory is less than a certain percentage)
        //      && (reusable transaction tasks exceeds limit || Parser and Sequencer module cache tasks exceeds limit || if in storage working mode)
        // OR
        // (2) storager task overload with certain threshold
        // OR
        // (3) memory is limited and exist trans sequenced but not output
        // OR
        // (4) memory is limited and exist ddl_trans in to handle or handling
        // OR
        // (5) memory_limit touch warn threshold and need_pause_dispatch
        bool condition1 = (active_part_trans_task_count >= part_trans_task_active_count_upper_bound)
          || touch_memory_warn_limit
          || (system_memory_avail < system_memory_avail_lower_bound);
        bool condition2 = (reusable_part_trans_task_count >= part_trans_task_reusable_count_upper_bound)
          || (ready_to_seq_task_count > ready_to_seq_task_upper_bound);
        bool condition3 = (storager_task_count > storager_task_count_upper_bound) && (memory_hold >= storager_mem_percentage * memory_limit);

        need_slow_down_fetcher = (condition1 && (condition2 || need_pause_dispatch || exist_trans_sequenced_not_handled || exist_ddl_processing_or_in_queue)) || condition3;

        if (need_slow_down_fetcher) {
          if (condition2) {
            reason = "MEMORY_LIMIT_AND_REUSABLE_PART_TOO_MUCH";
          } else if (need_pause_dispatch) {
            reason = "MEMORY_LIMIT_AND_DISPATCH_PAUSED";
          } else if (exist_trans_sequenced_not_handled) {
            reason = "MEMORY_LIMIT_AND_EXIST_TRANS_TO_OUTPUT";
          } else if (exist_ddl_processing_or_in_queue) {
            reason = "MEMORY_LIMIT_AND_EXIST_DDL_TRANS_TO_HANDLE";
          } else if (condition3) {
            reason = "STORAGER_TASK_OVER_THRESHOLD";
          } else {
            reason = "MEMORY_LIMIT";
          }
        }

        // Get the number of active distributed transactions after sequencing, including sequenced, formatted, and committed
        int64_t seq_trans_count =
          trans_ctx_mgr_->get_trans_count(TransCtx::TRANS_CTX_STATE_SEQUENCED);
        int64_t committed_trans_count =
          trans_ctx_mgr_->get_trans_count(TransCtx::TRANS_CTX_STATE_COMMITTED);

        bool current_fetcher_is_paused = fetcher_->is_paused();

        // Print logs: 1. on status changes; 2. cyclical printing
        bool need_print_state = (current_fetcher_is_paused != need_slow_down_fetcher);

        if (need_print_state || REACH_TIME_INTERVAL(PRINT_GLOBAL_FLOW_CONTROL_INTERVAL)) {
          _LOG_INFO("[STAT] [FLOW_CONTROL] NEED_SLOW_DOWN=%d "
              "PAUSED=%d MEM=%s/%s "
              "AVAIL_MEM=%s/%s "
              "READY_TO_SEQ=%ld/%ld "
              "PART_TRANS(TOTAL=%ld, ACTIVE=%ld/%ld, REUSABLE=%ld/%ld) "
              "LOG_TASK(ACTIVE=%ld) "
              "STORE(%ld/%ld) "
              "[FETCHER=%ld DML_PARSER=%ld DDL=%ld "
              "COMMITER=%ld USER_QUEUE=%ld OUT=%ld RC=%ld] "
              "DIST_TRANS(SEQ_QUEUE=%ld, SEQ=%ld, COMMITTED=%ld) "
              "NEED_PAUSE_DISPATCH=%d REASON=%s",
              need_slow_down_fetcher, current_fetcher_is_paused,
              SIZE_TO_STR(memory_hold), SIZE_TO_STR(memory_limit),
              SIZE_TO_STR(system_memory_avail), SIZE_TO_STR(system_memory_avail_lower_bound),
              ready_to_seq_task_count, ready_to_seq_task_upper_bound,
              total_part_trans_task_count,
              active_part_trans_task_count, part_trans_task_active_count_upper_bound,
              reusable_part_trans_task_count, part_trans_task_reusable_count_upper_bound,
              active_log_entry_task_count,
              storager_task_count, storager_task_count_upper_bound,
              fetcher_part_trans_task_count, dml_parser_part_trans_task_count, ddl_part_trans_count,
              committer_ddl_part_trans_task_count + committer_dml_part_trans_task_count,
              br_queue_part_trans_task_count, out_part_trans_task_count,
              resource_collector_part_trans_task_count,
              seq_queue_trans_count, seq_trans_count, committed_trans_count,
              need_pause_dispatch, reason);
        }
      }

      if (OB_SUCC(ret)) {
        // 1. Traffic control requires fetcher to be suspended
        // 2. The configuration item forces the fetcher to be suspended
        int64_t config_pause_fetcher = TCONF.pause_fetcher;
        if (need_slow_down_fetcher || 0 != config_pause_fetcher) {
          LOG_INFO("[STAT] [FLOW_CONTROL] [CONFIG] [PAUSE_FETCHER]",
              K(need_slow_down_fetcher), K(config_pause_fetcher));
          fetcher_->pause();
        }
        // 3. Recovery fetcher in other cases
        else {
          if (fetcher_->is_paused()) {
            LOG_INFO("[STAT] [FLOW_CONTROL] [RESUME_FETCHER]");
            fetcher_->resume();
          }
        }
      }
    } // else
  } // inited
}

void ObLogInstance::dump_pending_trans_info_()
{
  int ret = OB_SUCCESS;

  if (NULL != trans_ctx_mgr_) {
    const char *file = DEFAULT_PENDING_TRANS_INFO_FILE;
    int fd = open(file, O_WRONLY | O_APPEND | O_CREAT, 0600);
    static const int64_t BUFFER_SIZE = 2 << 26;
    char *buffer = static_cast<char *>(ob_cdc_malloc(BUFFER_SIZE));

    if (OB_UNLIKELY(fd < 0)) {
      LOG_ERROR("open pending trans info file fail", K(file), K(fd),
          K(errno), KERRMSG);
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(buffer)) {
      LOG_ERROR("allocate memory for pending trans info fail", K(BUFFER_SIZE));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t pos = 0;

      (void)databuff_printf(buffer, BUFFER_SIZE, pos, "============================== BEGIN ");
      (void)ObTimeUtility2::usec_to_str(get_timestamp(), buffer, BUFFER_SIZE, pos);
      (void)databuff_printf(buffer, BUFFER_SIZE, pos, " ==============================\n");

      if (OB_FAIL(trans_ctx_mgr_->dump_pending_trans_info(buffer, BUFFER_SIZE, pos))) {
        LOG_ERROR("dump pending trans info fail", KR(ret), K(buffer), K(BUFFER_SIZE), K(pos));
      } else {
        (void)databuff_printf(buffer, BUFFER_SIZE, pos,
            "============================== END ==============================;\n");
      }

      if (OB_SUCC(ret)) {
        int64_t total_size = pos;
        char *ptr = buffer;

        while (OB_SUCC(ret) && total_size > 0 && ! stop_flag_) {
          ssize_t nwrite = 0;
          nwrite = write(fd, ptr, static_cast<int>(total_size));

          if (nwrite >= 0) {
            total_size -= nwrite;
            ptr += nwrite;
          } else {
            LOG_ERROR("write pending trans info file fail",
                K(nwrite), K(total_size), K(file), K(fd), K(errno), KERRMSG, KP(ptr));
            ret = OB_IO_ERROR;
            break;
          }
        }
      }
    }

    if (fd >= 0) {
      if (0 != fsync(fd)) {
        LOG_ERROR("flush pending trans info failed", K(fd), K(errno), KERRMSG);
      }
      if (0 != close(fd)) {
        LOG_ERROR("close pending trans info file failed", K(fd), K(errno), KERRMSG);
      }
      fd = -1;
    }

    if (NULL != buffer) {
      ob_cdc_free(buffer);
      buffer = NULL;
    }
  }
}

void ObLogInstance::clean_log_()
{
  int64_t cycle_time = log_clean_cycle_time_us_;
  const static int64_t PRINT_TIME_BUF_SIZE = 64;
  const static int64_t CMD_BUF_SIZE = 1024;
  static char print_time_buf[PRINT_TIME_BUF_SIZE];
  static char cmd_buf[CMD_BUF_SIZE];
  static const char *print_time_format = "%Y-%m-%d %H:%i:%s";
  static const char *cmd_time_format = "%Y%m%d%H%i%s";
  static const char *log_file = "removed_log_files";

  if (cycle_time > 0) {
    int64_t print_time_pos = 0;
    int64_t cmd_pos = 0;
    int64_t base_time = get_timestamp() - cycle_time;
    int64_t begin_time = get_timestamp();

    (void)ObTimeUtility2::usec_format_to_str(base_time, print_time_format,
        print_time_buf, PRINT_TIME_BUF_SIZE, print_time_pos);

    (void)databuff_printf(cmd_buf, CMD_BUF_SIZE, cmd_pos,
        "echo `date` > log/%s; base_time=", log_file);

    (void)ObTimeUtility2::usec_format_to_str(base_time, cmd_time_format,
        cmd_buf, CMD_BUF_SIZE, cmd_pos);

    (void)databuff_printf(cmd_buf, CMD_BUF_SIZE, cmd_pos, "; "
        "for file in `find log/ | grep \"libobcdc.log.\" | grep -v err`; "
        "do "
        "num=`echo $file | cut -d '.' -f 3`; "
        "if [ $num -lt $base_time ]; "
        "then "
        "echo $file >> log/%s; "
        "rm $file -f; "
        "fi "
        "done", log_file);

    (void)system(cmd_buf);

    _LOG_INFO("[STAT] [CLEAN_LOG] BASE_TIME='%.*s' EXE_TIME=%ld CYCLE_TIME=%ld CMD=%s",
        (int)print_time_pos, print_time_buf, get_timestamp() - begin_time,
        cycle_time, cmd_buf);
  }
}

int64_t ObLogInstance::get_memory_hold_() const
{
  return lib::get_memory_hold();
}

int64_t ObLogInstance::get_memory_avail_() const
{
  return lib::get_memory_avail();
}

int64_t ObLogInstance::get_memory_limit_() const
{
  return lib::get_memory_limit();
}

int ObLogInstance::get_task_count_(
    int64_t &ready_to_seq_task_count,
    int64_t &seq_trans_count,
    int64_t &part_trans_task_resuable_count,
    int64_t &ddl_part_trans_count)
{
  int ret = OB_SUCCESS;
  ready_to_seq_task_count = 0;
  seq_trans_count = 0;
  part_trans_task_resuable_count = 0;

  if (OB_ISNULL(fetcher_) || OB_ISNULL(dml_parser_) || OB_ISNULL(formatter_)
      || OB_ISNULL(storager_) || OB_ISNULL(lob_data_merger_)
      || OB_ISNULL(sequencer_) || OB_ISNULL(reader_) || OB_ISNULL(committer_)
      || OB_ISNULL(sys_ls_handler_) || OB_ISNULL(resource_collector_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid arguments", K(ret), K(fetcher_), K(dml_parser_), K(formatter_), K(storager_),
        K(sequencer_), K(reader_), K(committer_), K(sys_ls_handler_), K(resource_collector_));
  } else {
    // I. Get the number of tasks to be processed by each module
    int64_t dml_parser_log_count = 0;
    int64_t formatter_br_count = 0;
    int64_t formatter_log_count = 0;
    int64_t stmt_in_lob_merger_count = 0;
    int64_t storager_task_count = 0;
    int64_t storager_log_count = 0;
    struct IObLogSequencer::SeqStatInfo seq_stat_info;
    int64_t reader_task_count = 0;
    int64_t sorter_task_count = 0;
    int64_t committer_pending_dml_trans_count = committer_->get_dml_trans_count();

    if (OB_FAIL(dml_parser_->get_log_entry_task_count(dml_parser_log_count))) {
      LOG_ERROR("parser get_log_entry_task_count fail", KR(ret), K(dml_parser_log_count));
    } else if (OB_FAIL(formatter_->get_task_count(formatter_br_count, formatter_log_count, stmt_in_lob_merger_count))) {
      LOG_ERROR("formatter get_task_count fail", KR(ret), K(formatter_br_count), K(formatter_log_count), K(stmt_in_lob_merger_count));
    } else if (OB_FAIL(trans_msg_sorter_->get_task_count(sorter_task_count))) {
      LOG_ERROR("sorter get_task_count fail", KR(ret), K(sorter_task_count));
    } else {
      storager_->get_task_count(storager_task_count, storager_log_count);
      sequencer_->get_task_count(seq_stat_info);
      reader_->get_task_count(reader_task_count);

      // Count the number of partitioned tasks to be ordered
      ready_to_seq_task_count = dml_parser_log_count + formatter_log_count + storager_log_count;
      seq_trans_count = seq_stat_info.sequenced_trans_count_;
    }

    // II. Get the number of reusable tasks for each module
    // 1. count by module
    // 2. The number of reusable tasks in the overall partition includes the following components:
    //   (1) tasks held by committer
    //   (2) Tasks held by br_queue
    //   (3) Tasks held by users that have not been returned
    //   (4) tasks held by resource_collector
    if (OB_SUCC(ret)) {
      int64_t lob_data_list_task_count = 0;
      lob_data_merger_->get_task_count(lob_data_list_task_count);
      int64_t committer_ddl_part_trans_task_count = 0;
      int64_t committer_dml_part_trans_task_count = 0;

      int64_t fetcher_part_trans_task_count = fetcher_->get_part_trans_task_count();
      committer_->get_part_trans_task_count(committer_ddl_part_trans_task_count,
          committer_dml_part_trans_task_count);
      int64_t sys_ls_handle_part_trans_task_count = 0;
      sys_ls_handler_->get_task_count(sys_ls_handle_part_trans_task_count, ddl_part_trans_count);
      int64_t br_queue_part_trans_task_count = br_queue_.get_part_trans_task_count();
      int64_t out_part_trans_task_count = get_out_part_trans_task_count_();
      int64_t resource_collector_part_trans_task_count = 0;
      int64_t resource_collector_br_count = 0;
      resource_collector_->get_task_count(resource_collector_part_trans_task_count, resource_collector_br_count);
      int64_t dml_br_count_in_user_queue = br_queue_.get_dml_br_count();
      int64_t dml_br_count_output = output_dml_br_count_;

      // Get the number of DDL Binlog Records in the user queue
      int64_t ddl_br_count_in_user_queue = br_queue_.get_ddl_br_count();
      int64_t ddl_br_count_output = output_ddl_br_count_;

      part_trans_task_resuable_count = committer_ddl_part_trans_task_count
        + committer_dml_part_trans_task_count
        + br_queue_part_trans_task_count
        + out_part_trans_task_count
        + resource_collector_part_trans_task_count;

      // Print monitoring items
      if (REACH_TIME_INTERVAL(PRINT_GLOBAL_FLOW_CONTROL_INTERVAL)) {
        _LOG_INFO("------------------------------------------------------------");
        _LOG_INFO("[TASK_COUNT_STAT] [FETCHER] [PART_TRANS_TASK=%ld]", fetcher_part_trans_task_count);
        _LOG_INFO("[TASK_COUNT_STAT] [SYS_LS_HANDLE] [PART_TRANS_TASK=%ld][DDL_QUEUED=%ld]",
            sys_ls_handle_part_trans_task_count, ddl_part_trans_count);
        _LOG_INFO("[TASK_COUNT_STAT] [STORAGER] [LOG_TASK=%ld/%ld]", storager_task_count, storager_log_count);
        _LOG_INFO("[TASK_COUNT_STAT] [SEQUENCER] [PART_TRANS_TASK(QUEUE=%ld TOTAL=[%ld][DDL=%ld DML=%ld HB=%ld])] [TRANS(READY=%ld SEQ=%ld)]",
            seq_stat_info.queue_part_trans_task_count_, seq_stat_info.total_part_trans_task_count_,
            seq_stat_info.ddl_part_trans_task_count_, seq_stat_info.dml_part_trans_task_count_, seq_stat_info.hb_part_trans_task_count_,
            seq_stat_info.ready_trans_count_, seq_stat_info.sequenced_trans_count_);
        _LOG_INFO("[TASK_COUNT_STAT] [READER] [ROW_TASK=%ld]", reader_task_count);
        _LOG_INFO("[TASK_COUNT_STAT] [DML_PARSER] [LOG_TASK=%ld]", dml_parser_log_count);
        _LOG_INFO("[TASK_COUNT_STAT] [FORMATTER] [BR=%ld LOG_TASK=%ld LOB_STMT=%ld]",
            formatter_br_count, formatter_log_count, stmt_in_lob_merger_count);
        _LOG_INFO("[TASK_COUNT_STAT] [LOB_MERGER] [LOB_LIST_TASK=%ld]", lob_data_list_task_count);
        _LOG_INFO("[TASK_COUNT_STAT] [SORTER] [TRANS=%ld]", sorter_task_count);
        _LOG_INFO("[TASK_COUNT_STAT] [COMMITER] [DML_TRANS=%ld DDL_PART_TRANS_TASK=%ld DML_PART_TRANS_TASK=%ld]",
            committer_pending_dml_trans_count,
            committer_ddl_part_trans_task_count,
            committer_dml_part_trans_task_count);
        _LOG_INFO("[TASK_COUNT_STAT] [USER_QUEUE] [PART_TRANS_TASK=%ld] [DDL_BR=%ld] [DML_BR=%ld]",
            br_queue_part_trans_task_count,
            ddl_br_count_in_user_queue,
            dml_br_count_in_user_queue);
        _LOG_INFO("[TASK_COUNT_STAT] [OUT] [PART_TRANS_TASK=%ld] [DDL_BR=%ld] [DML_BR=%ld]", out_part_trans_task_count,
            ddl_br_count_output, dml_br_count_output);
        _LOG_INFO("[TASK_COUNT_STAT] [RESOURCE_COLLECTOR] [PART_TRANS_TASK=%ld] [BR=%ld]",
            resource_collector_part_trans_task_count, resource_collector_br_count);
      }
    }
  }

  return ret;
}

void ObLogInstance::do_drc_consume_tps_stat_()
{
  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans_stat is null", K(trans_stat_mgr_));
  } else {
    trans_stat_mgr_->do_drc_consume_tps_stat();
  }
}

void ObLogInstance::do_drc_consume_rps_stat_()
{
  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans_stat is null", K(trans_stat_mgr_));
  } else {
    trans_stat_mgr_->do_drc_consume_rps_stat();
  }
}

void ObLogInstance::do_drc_release_tps_stat_()
{
  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans_stat is null", K(trans_stat_mgr_));
  } else {
    trans_stat_mgr_->do_drc_release_tps_stat();
  }
}

void ObLogInstance::do_drc_release_rps_stat_()
{
  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans_stat is null", K(trans_stat_mgr_));
  } else {
    trans_stat_mgr_->do_drc_release_rps_stat();
  }
}

void ObLogInstance::do_stat_for_part_trans_task_count_(int record_type,
    int64_t part_trans_task_count,
    bool need_accumulate_stat)
{
  if ((EDDL == record_type) || (EBEGIN == record_type)) {
    if (need_accumulate_stat) {
      // hold by user
      (void)ATOMIC_AAF(&part_trans_task_count_, part_trans_task_count);
    } else {
      // return by user
      (void)ATOMIC_AAF(&part_trans_task_count_, -part_trans_task_count);
    }
  } else {
    // do nothing
  }
}

void ObLogInstance::print_trans_stat_()
{
  if (OB_ISNULL(trans_stat_mgr_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans_stat is null", K(trans_stat_mgr_));
  } else {
    trans_stat_mgr_->print_stat_info();
  }
}

int ObLogInstance::init_ob_trace_id_(const char *ob_trace_id_ptr)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(ob_trace_id_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ob_trace_id_ptr is null", K(ob_trace_id_ptr));
  } else if (OB_FAIL(databuff_printf(ob_trace_id_str_, sizeof(ob_trace_id_str_), pos, "%s",
          ob_trace_id_ptr))) {
    LOG_ERROR("databuff_printf ob_trace_id_str_ fail", K(ob_trace_id_str_), K(pos), K(ob_trace_id_ptr));
  } else {
    LOG_INFO("init_ob_trace_id_ succ", K(ob_trace_id_str_), K(ob_trace_id_ptr));
  }

  return ret;
}

int ObLogInstance::query_cluster_info_(ObLogSysTableHelper::ClusterInfo &cluster_info)
{
  int ret = OB_SUCCESS;
  cluster_info.reset();
  bool done = false;

  if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("systable_helper_ is null", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (! done && OB_SUCCESS == ret) {
      if (OB_FAIL(systable_helper_->query_cluster_info(cluster_info))) {
        LOG_WARN("systable_helper_ query_cluster_info fail", KR(ret), K(cluster_info));
      } else {
        done = true;
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

// init cluster version
int ObLogInstance::init_ob_cluster_version_()
{
  int ret = OB_SUCCESS;
  uint64_t min_observer_version = OB_INVALID_ID;

  if (OB_FAIL(query_cluster_min_observer_version_(min_observer_version))) {
    LOG_ERROR("query_cluster_min_observer_version_ fail", KR(ret), K(min_observer_version));
  } else if (OB_FAIL(ObClusterVersion::get_instance().init(min_observer_version))) {
    LOG_ERROR("ObClusterVersion init fail", KR(ret), K(min_observer_version));
  } else {
    LOG_INFO("OceanBase cluster version init succ", "cluster_version", ObClusterVersion::get_instance());
    global_info_.update_min_cluster_version(min_observer_version);

    if (min_observer_version < CLUSTER_VERSION_4_1_0_0) {
      // OB 4.0 only support online schema
      refresh_mode_ = RefreshMode::ONLINE;
    } else if (min_observer_version >= CLUSTER_VERSION_4_2_0_0) {
      // For OB Version greater than 4.1:
      // 1. tenant_sync_mode must use data_dict for OB 4.2
      // 2. suggest use online_schema for OB 4.1
      // 3. use refresh_mode as user configured if skip_ob_version_compat_check
      if ((0 == TCONF.skip_ob_version_compat_check) || is_tenant_sync_mode_) {
        refresh_mode_ = RefreshMode::DATA_DICT;
      }
    } else {
      // CLUSTER_VERSION_4_1_0_0 use user specified refresh_mode
    }
  }

  if (OB_SUCC(ret) && is_data_dict_refresh_mode(refresh_mode_)) {
    enable_filter_sys_tenant_ = true;
  }
  LOG_INFO("[WORK_MODE]", "refresh_mode", print_refresh_mode(refresh_mode_));

  return ret;
}

// Query the smallest version of the cluster until it succeeds
int ObLogInstance::query_cluster_min_observer_version_(uint64_t &min_observer_version)
{
  int ret = OB_SUCCESS;
  bool done = false;

  if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("systable_helper_ is null", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (! done && OB_SUCCESS == ret) {
      if (OB_FAIL(systable_helper_->query_cluster_min_observer_version(min_observer_version))) {
        LOG_WARN("systable_helper_ query_cluster_min_observer_version fail", KR(ret), K(min_observer_version));
      } else {
        done = true;
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
        ob_usleep(100L * 1000L);
      }
    }
  }

  return ret;
}

// update cluster version
void ObLogInstance::update_cluster_version_()
{
  int ret = OB_SUCCESS;
  uint64_t min_observer_version = OB_INVALID_ID;

  if (OB_NOT_NULL(systable_helper_)) {
    if (OB_FAIL(systable_helper_->query_cluster_min_observer_version(min_observer_version))) {
      if (OB_NEED_RETRY == ret) {
        LOG_WARN("systable_helper_ query_cluster_min_observer_version fail", KR(ret), K(min_observer_version));
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("systable_helper_ query_cluster_min_observer_version fail", KR(ret), K(min_observer_version));
      }
    } else {
      ObClusterVersion::get_instance().update_cluster_version(min_observer_version);
      LOG_INFO("OceanBase cluster version update succ", "cluster_version", ObClusterVersion::get_instance());
    }
  }
}

int ObLogInstance::check_ob_version_legal_(const uint64_t ob_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == ob_version)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ob_version is invalid", KR(ret), K(ob_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogInstance::check_observer_version_valid_()
{
  int ret = OB_SUCCESS;

  const uint64_t ob_version = GET_MIN_CLUSTER_VERSION();
  const bool skip_ob_version_compat_check = (0 != TCONF.skip_ob_version_compat_check);
  uint32_t ob_major = 0;
  uint16_t ob_minor = 0;
  uint8_t  ob_major_patch = 0;
  uint8_t  ob_minor_patch = 0;
  cal_version_components_(ob_version, ob_major, ob_minor, ob_major_patch, ob_minor_patch);

  if (skip_ob_version_compat_check) {
    _LOG_INFO("skip_ob_version_compat_check is true, skip check, observer_version(%u.%hu.%hu.%hu)",
        ob_major, ob_minor, ob_major_patch, ob_minor_patch);
  } else if (OB_FAIL(check_ob_version_legal_(ob_version))) {
    _LOG_WARN("check ob version illegal, observer_version(%u.%hu.%hu.%hu), skip it",
        ob_major, ob_minor, ob_major_patch, ob_minor_patch);
    ret = OB_SUCCESS;
  } else if (is_integrated_fetching_mode(fetching_mode_)) {
    if (ob_version > CLUSTER_VERSION_4_2_0_0 && is_online_refresh_mode(refresh_mode_)) {
      LOG_WARN("RUNNING ONLINE REFRESH MODE IS NOT SUGESSTED FOR OB VERSION 4.2 OR UPPER!");
    }
    if ((oblog_major_ < 4 || ob_major < 4)) {
      ret = OB_VERSION_NOT_MATCH;
      _LOG_ERROR("obcdc_version(%u.%hu.%hu.%hu) don't support observer_version(%u.%hu.%hu.%hu)",
          oblog_major_, oblog_minor_, oblog_major_patch_, oblog_minor_patch_,
          ob_major, ob_minor, ob_major_patch, ob_minor_patch);
    } else if (oblog_major_ > ob_major
        || (oblog_major_ == ob_major && oblog_minor_ >= ob_minor)) {
      _LOG_INFO("obcdc_version(%u.%hu.%hu.%hu) compatible with observer_version(%u.%hu.%hu.%hu)",
          oblog_major_, oblog_minor_, oblog_major_patch_, oblog_minor_patch_,
          ob_major, ob_minor, ob_major_patch, ob_minor_patch);
    } else {
      ret = OB_VERSION_NOT_MATCH;
      _LOG_ERROR("obcdc_version(%u.%hu.%hu.%hu) not compatible with observer_version(%u.%hu.%hu.%hu), "
          "obcdc_version is too old, need upgrade",
          oblog_major_, oblog_minor_, oblog_major_patch_, oblog_minor_patch_,
          ob_major, ob_minor, ob_major_patch, ob_minor_patch);
    }
  } else if (is_direct_fetching_mode(fetching_mode_)) {
    _LOG_INFO("obcdc_version(%u.%hu.%hu.%hu) in Direct Fetching Log Mode",
        oblog_major_, oblog_minor_, oblog_major_patch_, oblog_minor_patch_);
  } else {}

  return ret;
}

int ObLogInstance::init_obcdc_version_components_()
{
  int ret = OB_SUCCESS;
  uint64_t obcdc_version = 0;
  if (OB_FAIL(ObClusterVersion::get_version(PACKAGE_VERSION, obcdc_version))) {
    LOG_ERROR("get_version fail", KR(ret), K(PACKAGE_VERSION), K(obcdc_version));
  } else {
    cal_version_components_(obcdc_version, oblog_major_, oblog_minor_, oblog_major_patch_, oblog_minor_patch_);
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("init obcdc version succ", K(PACKAGE_VERSION), K(obcdc_version),
        K(oblog_major_), K(oblog_minor_), K(oblog_major_patch_), K(oblog_minor_patch_));
  }
  return ret;
}

void ObLogInstance::cal_version_components_(const uint64_t version,
    uint32_t &major,
    uint16_t &minor,
    uint8_t  &major_patch,
    uint8_t  &minor_patch)
{
  major = OB_VSN_MAJOR(version);
  minor = OB_VSN_MINOR(version);
  major_patch = OB_VSN_MAJOR_PATCH(version);
  minor_patch = OB_VSN_MINOR_PATCH(version);
}

int ObLogInstance::get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_mgr_)) {
    LOG_ERROR("tenant_mgr is NULL", K(tenant_mgr_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr_->get_tenant_guard(tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id), K(guard));
    }
  } else {
    // success
  }
  return ret;
}

int ObLogInstance::get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfoGuard dict_tenant_info_guard;
  ObDictTenantInfo *tenant_info = nullptr;

  if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
      tenant_id,
      dict_tenant_info_guard))) {
    LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
  } else {
    const common::ObCompatibilityMode &compatible_mode = tenant_info->get_compatibility_mode();

    if (common::ObCompatibilityMode::MYSQL_MODE == compatible_mode) {
      compat_mode = lib::Worker::CompatMode::MYSQL;
    } else if (common::ObCompatibilityMode::ORACLE_MODE == compatible_mode) {
      compat_mode = lib::Worker::CompatMode::ORACLE;
    } else {
      compat_mode = lib::Worker::CompatMode::INVALID;
    }
  }

  return ret;
}

// pause disaptch if:
// 0. user force pause.
// 1. user queue backlog.
// 2. resource collector backlog.
bool ObLogInstance::need_pause_redo_dispatch() const
{
  bool current_need_pause = true;
  static bool last_need_paused = false;
  if (inited_) {
    double memory_usage_warn_percent = TCONF.memory_usage_warn_threshold / 100.0;
    int64_t memory_limit = CDC_CFG_MGR.get_memory_limit();
    int64_t memory_warn_usage = memory_limit * memory_usage_warn_percent;
    int64_t memory_hold = get_memory_hold_();
    int64_t redo_dispatch_exceed_ratio = CDC_CFG_MGR.get_redo_dispatched_memory_limit_exceed_ratio();
    const int64_t redo_memory_limit = CDC_CFG_MGR.get_redo_dispatcher_memory_limit();
    const int64_t rc_br_thread_count = TCONF.resource_collector_thread_num_for_br;
    const int64_t rc_thread_queue_len = CDC_CFG_MGR.get_resource_collector_queue_length();
    int64_t resource_collector_part_trans_task_count = 0;
    int64_t resource_collector_br_count = 0;
    resource_collector_->get_task_count(resource_collector_part_trans_task_count, resource_collector_br_count);
    const int64_t user_queue_br_count = br_queue_.get_dml_br_count() + br_queue_.get_ddl_br_count();
    const bool force_pause_dispatch = (0 != TCONF.pause_dispatch_redo);
    const int64_t pause_dispatch_threshold = TCONF.pause_redo_dispatch_task_count_threshold;
    const bool touch_memory_warn_limit = (memory_hold > memory_warn_usage);
    const bool touch_memory_limit = (memory_hold > memory_limit);
    double pause_dispatch_percent = pause_dispatch_threshold / 100.0;
    if (touch_memory_limit) {
      const int64_t queue_backlog_lowest_tolerance = TCONF.queue_backlog_lowest_tolerance;
      if (user_queue_br_count > queue_backlog_lowest_tolerance || resource_collector_br_count > queue_backlog_lowest_tolerance) {
        pause_dispatch_percent = 0;
      }
      // pause redo dispatch
    } else if (touch_memory_warn_limit) {
      pause_dispatch_percent = pause_dispatch_percent * 0.1;
      // if already touch memory_warn limit, increase probability of redo dispatch flow control
    }
    int64_t dispatched_redo_memory = 0;
    if (OB_NOT_NULL(trans_redo_dispatcher_)) {
      dispatched_redo_memory = trans_redo_dispatcher_->get_dispatched_memory_size();
    }
    const bool is_redo_dispatch_over_exceed = (dispatched_redo_memory >= redo_dispatch_exceed_ratio * redo_memory_limit);
    const char *reason = "";
    if (force_pause_dispatch) {
      current_need_pause = true;
      reason = "USER_FORCE_PAUSE";
      // NOTICE: rely on stat of resource_collector_ is right
    } else if (resource_collector_br_count > (rc_br_thread_count * rc_thread_queue_len * pause_dispatch_percent)) {
      current_need_pause = (is_redo_dispatch_over_exceed || touch_memory_warn_limit);
      reason = "SLOW_RESOURCE_RECYCLING";
      // NOTICE: rely on stat of binlog_record_queue is right
    } else if (user_queue_br_count > (CDC_CFG_MGR.get_br_queue_length() * pause_dispatch_percent)) {
      current_need_pause = (is_redo_dispatch_over_exceed || touch_memory_warn_limit);
      reason = "SLOW_CONSUMPTION_DOWNSTREAM";
    } else {
      current_need_pause = false;
    }
    bool is_state_change = (last_need_paused != current_need_pause);
    bool need_print_state = (is_state_change || current_need_pause) && REACH_TIME_INTERVAL(PRINT_GLOBAL_FLOW_CONTROL_INTERVAL);
    if (need_print_state) {
      _LOG_INFO("[NEED_PAUSE_REDO_DISPATCH=%d]"
          "[REASON:%s]"
          "[REDO_DISPATCH:%s/%s]"
          "[THRESHOLD:%.2f]"
          "[QUEUE_DML_BR:%ld]"
          "[RESOURCE_COLLECTOR:%ld]"
          "[STATE_CHANGED:%d]",
          current_need_pause,
          reason,
          SIZE_TO_STR(dispatched_redo_memory), SIZE_TO_STR(redo_memory_limit),
          pause_dispatch_percent,
          user_queue_br_count,
          resource_collector_br_count,
          is_state_change);
    }
    if (is_state_change) {
      last_need_paused = current_need_pause;
    }
  }
  return current_need_pause;
}

}
}
