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
 * Formatter
 */

#define USING_LOG_PREFIX OBLOG_FORMATTER

#include "ob_log_formatter.h"

#include "share/schema/ob_table_schema.h"           // TableSchemaType
#include "lib/string/ob_string.h"                   // ObString
#include "storage/tx/ob_trans_define.h"             // ObTransID

#include "ob_log_meta_manager.h"        // IObLogMetaManager
#include "ob_log_utils.h"               // obj2str
#include "ob_log_schema_getter.h"       // IObLogSchemaGetter, DBSchemaInfo
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX
#include "ob_obj2str_helper.h"          // ObObj2strHelper
#include "ob_log_trans_ctx_mgr.h"       // IObLogTransCtxMgr
#include "ob_log_binlog_record_pool.h"  // IObLogBRPool
#include "ob_log_storager.h"            // IObLogStorager
#include "ob_log_tenant.h"              // ObLogTenantGuard, ObLogTenant
#include "ob_log_config.h"              // TCONF
#include "ob_log_resource_collector.h"  // IObLogResourceCollector
#include "ob_cdc_lob_ctx.h"             // ObLobDataGetCtx
#include "ob_cdc_lob_data_merger.h"     // IObCDCLobDataMerger
#include "ob_cdc_lob_aux_meta_storager.h"    // ObCDCLobAuxMetaStorager
#include "ob_cdc_lob_aux_table_parse.h"      // ObCDCLobAuxMetaStorager
#include "ob_cdc_udt.h"                 // ObCDCUdtValueBuilder
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_timezone_info_getter.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace libobcdc
{

void ObLogFormatter::RowValue::reset()
{
  column_num_ = 0;
  contain_old_column_ = false;
  new_column_array_ = NULL;
  old_column_array_ = NULL;

  (void)memset(new_columns_, 0, sizeof(new_columns_));
  (void)memset(old_columns_, 0, sizeof(old_columns_));
  (void)memset(orig_default_value_, 0, sizeof(orig_default_value_));
  (void)memset(is_rowkey_, 0, sizeof(is_rowkey_));
  (void)memset(is_changed_, 0, sizeof(is_changed_));
  (void)memset(is_null_lob_columns_, 0, sizeof(is_null_lob_columns_));
  (void)memset(is_diff_, 0, sizeof(is_diff_));
  (void)memset(is_old_col_nop_, 0, sizeof(is_old_col_nop_));
}

int ObLogFormatter::RowValue::init(const int64_t column_num, const bool contain_old_column)
{
  column_num_ = column_num;
  contain_old_column_ = contain_old_column;
  new_column_array_ = NULL;
  old_column_array_ = NULL;

  if (column_num > 0) {
    (void)memset(new_columns_, 0, column_num * sizeof(new_columns_[0]));
    (void)memset(old_columns_, 0, column_num * sizeof(old_columns_[0]));
    (void)memset(orig_default_value_, 0, column_num * sizeof(orig_default_value_[0]));
    (void)memset(is_rowkey_, 0, column_num * sizeof(is_rowkey_[0]));
    (void)memset(is_changed_, 0, column_num * sizeof(is_changed_[0]));
    (void)memset(is_null_lob_columns_, 0, column_num * sizeof(is_null_lob_columns_[0]));
    (void)memset(is_diff_, 0, column_num * sizeof(is_diff_[0]));
    (void)memset(is_old_col_nop_, 0, column_num * sizeof(is_old_col_nop_[0]));
  }

  return OB_SUCCESS;
}

ObLogFormatter::ObLogFormatter() : inited_(false),
                                   working_mode_(WorkingMode::UNKNOWN_MODE),
                                   obj2str_helper_(NULL),
                                   br_pool_(NULL),
                                   err_handler_(NULL),
                                   meta_manager_(NULL),
                                   schema_getter_(NULL),
                                   storager_(NULL),
                                   row_value_array_(NULL),
                                   allocator_(ObModIds::OB_LOG_FORMATTER, PAGE_SIZE),
                                   round_value_(0),
                                   skip_dirty_data_(false),
                                   enable_hbase_mode_(false),
                                   hbase_util_(NULL),
                                   skip_hbase_mode_put_column_count_not_consistency_(false),
                                   enable_output_hidden_primary_key_(false),
                                   log_entry_task_count_(0),
                                   stmt_in_lob_merger_count_(0)

{
}

ObLogFormatter::~ObLogFormatter()
{
  destroy();
}

int ObLogFormatter::init(const int64_t thread_num,
      const int64_t queue_size,
      const WorkingMode working_mode,
      ObObj2strHelper *obj2str_helper,
      IObLogBRPool *br_pool,
      IObLogMetaManager *meta_manager,
      IObLogSchemaGetter *schema_getter,
      IObLogStorager *storager,
      IObLogErrHandler *err_handler,
      const bool skip_dirty_data,
      const bool enable_hbase_mode,
      ObLogHbaseUtil &hbase_util,
      const bool skip_hbase_mode_put_column_count_not_consistency,
      const bool enable_output_hidden_primary_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogFormatter has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_UNLIKELY(! is_working_mode_valid(working_mode))
      || OB_ISNULL(obj2str_helper)
      || OB_ISNULL(br_pool)
      || OB_ISNULL(meta_manager)
      || (OB_ISNULL(schema_getter) && is_online_refresh_mode(TCTX.refresh_mode_))
      || OB_ISNULL(storager)
      || OB_ISNULL(err_handler)) {
    LOG_ERROR("invalid arguments", K(thread_num), K(queue_size), K(working_mode), K(obj2str_helper),
        K(meta_manager), K(schema_getter), K(storager), K(err_handler));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(FormatterThread::init(thread_num, queue_size))) {
    LOG_ERROR("init formatter queue thread fail", KR(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(init_row_value_array_(thread_num))) {
    LOG_ERROR("init_row_value_array_ fail", KR(ret), K(thread_num));
  } else {
    working_mode_ = working_mode;
    obj2str_helper_ = obj2str_helper;
    br_pool_ = br_pool;
    err_handler_ = err_handler;
    meta_manager_ = meta_manager;
    schema_getter_ = schema_getter;
    storager_ = storager;
    round_value_ = 0;
    skip_dirty_data_ = skip_dirty_data;
    enable_hbase_mode_ = enable_hbase_mode;
    hbase_util_ = &hbase_util;
    skip_hbase_mode_put_column_count_not_consistency_ = skip_hbase_mode_put_column_count_not_consistency;
    enable_output_hidden_primary_key_ = enable_output_hidden_primary_key;
    log_entry_task_count_ = 0;
    stmt_in_lob_merger_count_ = 0;
    inited_ = true;
    LOG_INFO("Formatter init succ", K(working_mode_), "working_mode", print_working_mode(working_mode_),
        K(thread_num), K(queue_size));
  }

  return ret;
}

void ObLogFormatter::destroy()
{
  FormatterThread::destroy();

  inited_ = false;

  destroy_row_value_array_();

  working_mode_ = WorkingMode::UNKNOWN_MODE;
  obj2str_helper_ = NULL;
  br_pool_ = NULL;
  row_value_array_ = NULL;
  err_handler_ = NULL;
  meta_manager_ = NULL;
  schema_getter_ = NULL;
  storager_ = NULL;
  round_value_ = 0;
  skip_dirty_data_ = false;
  enable_hbase_mode_ = false;
  hbase_util_ = NULL;
  skip_hbase_mode_put_column_count_not_consistency_ = false;
  enable_output_hidden_primary_key_ = false;
  log_entry_task_count_ = 0;
  stmt_in_lob_merger_count_ = 0;
}

int ObLogFormatter::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogFormatter has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(FormatterThread::start())) {
    LOG_ERROR("start formatter thread fail", KR(ret), "thread_num", get_thread_num());
  } else {
    LOG_INFO("start formatter threads succ", "thread_num", get_thread_num());
  }

  return ret;
}

void ObLogFormatter::stop()
{
  if (inited_) {
    FormatterThread::stop();
    LOG_INFO("stop formatter threads succ", "thread_num", get_thread_num());
  }
}

int ObLogFormatter::push(IStmtTask *stmt_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(stmt_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(stmt_task), KR(ret));
  } else {
    // Ensure that all stmt of ObLogEntryTask are pushed to the same queue
    const uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    int64_t stmt_count = 0;

    while (OB_SUCC(ret) && NULL != stmt_task) {
      IStmtTask *next = stmt_task->get_next();
      void *push_task = static_cast<void *>(stmt_task);

      RETRY_FUNC(stop_flag, *(static_cast<ObMQThread *>(this)), push, push_task, hash_value, DATA_OP_TIMEOUT);

      if (OB_SUCC(ret)) {
        stmt_task = next;
        ++stmt_count;
      } else {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("push task into formatter fail", KR(ret), K(push_task), K(hash_value), K(stmt_count));
        }
      }
    } // while

    if (OB_SUCC(ret)) {
      ATOMIC_INC(&log_entry_task_count_);
    }
  }

  return ret;
}

int ObLogFormatter::push_single_task(IStmtTask *stmt_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(stmt_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(stmt_task));
  } else {
    const uint64_t hash_value = ATOMIC_FAA(&round_value_, 1);
    void *push_task = static_cast<void *>(stmt_task);

    RETRY_FUNC(stop_flag, *(static_cast<ObMQThread *>(this)), push, push_task, hash_value, DATA_OP_TIMEOUT);

    if (OB_SUCC(ret)) {
      ATOMIC_DEC(&stmt_in_lob_merger_count_);
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push task into formatter fail", KR(ret), K(push_task), K(hash_value));
    }
  }

  return ret;
}

int ObLogFormatter::get_task_count(
    int64_t &br_count,
    int64_t &log_entry_task_count,
    int64_t &stmt_in_lob_merger_count)
{
  int ret = OB_SUCCESS;
  br_count = 0;
  log_entry_task_count = 0;
  stmt_in_lob_merger_count = 0;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("parser has not been initialized", KR(ret));
  } else if (OB_FAIL(get_total_task_num(br_count))) {
    LOG_ERROR("get_total_task_num fail", KR(ret), K(br_count));
  } else {
    log_entry_task_count = ATOMIC_LOAD(&log_entry_task_count_);
    stmt_in_lob_merger_count = ATOMIC_LOAD(&stmt_in_lob_merger_count_);

  }

  return ret;
}

int ObLogFormatter::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;
  set_cdc_thread_name("Formatter", thread_index);
  bool cur_stmt_need_callback = false;
  IStmtTask *stmt_task = static_cast<IStmtTask *>(data);
  DmlStmtTask *dml_stmt_task = dynamic_cast<DmlStmtTask *>(stmt_task);
  RowValue *rv = row_value_array_ + thread_index;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(stmt_task) || OB_ISNULL(rv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KPC(stmt_task), KPC(rv));
  } else if (FALSE_IT(rv->reset_column_num())) {
  } else if (OB_UNLIKELY(! stmt_task->is_dml_stmt()) || OB_ISNULL(dml_stmt_task)) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("stmt_task is not DML statement", KR(ret), "stmt_task", *stmt_task);
  } else if (OB_FAIL(handle_dml_stmt_(
      *dml_stmt_task,
      rv,
      cur_stmt_need_callback,
      stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_dml_stmt_ failed", KR(ret), KPC(dml_stmt_task), K(cur_stmt_need_callback));
    }
  }
  // cur_stmt_need_callback is true, do nothing, wait callback process.
  // Note: You cannot continue to manipulate any data structures afterwards.

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCC(ret) && ! cur_stmt_need_callback) {
    LOG_DEBUG("formatter handle task", K(thread_index), "stmt_task", *dml_stmt_task);

    // Doing the finishing job
    // Note: After this function call, neither the partition transaction nor the statement task can be referenced anymore and may be recycled at any time
    if (OB_FAIL(finish_format_(stmt_task->get_host(), dml_stmt_task->get_redo_log_entry_task(), stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("finish_format_ fail", KR(ret));
      }
    }
  }

  // Failure to withdraw
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "formatter thread exits, thread_index=%ld, err=%d",
        thread_index, ret);
    stop_flag = true;
  }

  return ret;
}

int ObLogFormatter::handle_dml_stmt_(
    DmlStmtTask &dml_stmt_task,
    RowValue *row_value,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const bool is_online_schema_mode = is_online_refresh_mode(TCTX.refresh_mode_);
  ObLogBR *br = NULL;
  bool is_ignore = false;

  if (OB_ISNULL(row_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", KR(ret), KP(row_value));
  } else if (OB_FAIL(init_binlog_record_for_dml_stmt_task_(&dml_stmt_task, br, is_ignore))) {
    LOG_ERROR("init_binlog_record_for_dml_stmt_task_ fail", KR(ret), K(dml_stmt_task), K(is_ignore));
  } else if (is_ignore) {
    br->set_is_valid(false);
  } else if (is_online_schema_mode) {
    if (OB_FAIL(handle_dml_stmt_with_online_schema_(
        dml_stmt_task,
        *row_value,
        *br,
        cur_stmt_need_callback,
        stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_dml_stmt_with_online_schema_ failed", KR(ret), K(dml_stmt_task), KPC(br));
      }
    }
  } else {
    // data_dict mode
    if (OB_FAIL(handle_dml_stmt_with_dict_schema_(
        dml_stmt_task,
        *row_value,
        *br,
        cur_stmt_need_callback,
        stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_dml_stmt_with_dict_schema_ failed", KR(ret), K(dml_stmt_task), KPC(br));
      }
    }
  }

  return ret;
}

int ObLogFormatter::init_binlog_record_for_dml_stmt_task_(
    DmlStmtTask *stmt_task,
    ObLogBR *&br,
    bool &is_ignore)
{
  int ret = OB_SUCCESS;
  is_ignore = false;
  ObLogEntryTask *log_entry_task = NULL;
  PartTransTask *part_trans_task = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(stmt_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K(stmt_task));
  } else if (OB_ISNULL(log_entry_task = &(stmt_task->get_redo_log_entry_task()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log_entry_task is NULL", KR(ret), KPC(stmt_task));
  } else if (OB_ISNULL(part_trans_task = static_cast<PartTransTask*>(log_entry_task->get_host()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("part_trans_task is NULL", KR(ret), K(log_entry_task));
  } else if (stmt_task->is_callback()) {
    // Binlog record must have been generated before when is callback
    br = stmt_task->get_binlog_record();
    LOG_DEBUG("br callback process", K(stmt_task), K(*stmt_task));
  } else {
    if (OB_FAIL(br_pool_->alloc(br, log_entry_task, stmt_task))) {
      LOG_ERROR("alloc binlog record from pool fail", KR(ret), K(stmt_task));
    } else if (OB_ISNULL(br)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc binlog record fail", KR(ret), K(br));
    } else {
      // select ... for update to record DF_LOCK log to prevent loss of row lock information on the
      // standby machine in the event of a master/standby switchover, no synchronization required
      if (stmt_task->get_dml_flag().is_lock()) {
        is_ignore = true;
      } else {
        RecordType record_type = get_record_type(stmt_task->get_dml_flag());
        const uint64_t tenant_id = stmt_task->get_host().get_tenant_id();
        const uint64_t cluster_id = part_trans_task->get_cluster_id();
        const uint64_t row_index = stmt_task->get_row_index();
        const ObString &trace_id = part_trans_task->get_trace_id();
        const ObString &trace_info = part_trans_task->get_trace_info();
        ObString dml_unique_id;
        const int64_t schema_version = part_trans_task->get_global_schema_version();
        const int64_t commit_version = part_trans_task->get_trans_commit_version();

        if (OB_FAIL(init_dml_unique_id_(*stmt_task, *log_entry_task, *part_trans_task, dml_unique_id))) {
          LOG_ERROR("init_dml_unique_id_ fail", KR(ret), KPC(stmt_task), KPC(log_entry_task), KPC(part_trans_task),
              K(dml_unique_id));
        } else if (OB_FAIL(br->init_data(static_cast<RecordType>(record_type), cluster_id, tenant_id,
                row_index, trace_id, trace_info, dml_unique_id, schema_version, commit_version))) {
          LOG_ERROR("ObLogBR init_data fail", KR(ret), K(record_type), K(cluster_id), K(tenant_id),
              K(trace_id), K(trace_info), K(dml_unique_id), K(schema_version), K(commit_version));
        } else {
          LOG_DEBUG("br init_data succ", K(record_type), K(tenant_id), K(stmt_task), K(*stmt_task));
        }
      }

      if (OB_SUCC(ret)) {
        stmt_task->set_binlog_record(br);
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != br) {
      br_pool_->free(br);
      br = NULL;
    }
  }
  return ret;
}

int ObLogFormatter::handle_dml_stmt_with_online_schema_(
    DmlStmtTask &dml_stmt_task,
    RowValue &row_value,
    ObLogBR &br,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  DBSchemaInfo db_schema_info;
  const TableSchemaType *table_schema = NULL;
  const uint64_t tenant_id = dml_stmt_task.get_tenant_id();
  // Get the tenant schema: MYSQL or ORACLE
  // To ensure the correctness of ObObj2strHelper::obj2str, you need to set the mysql or Oracle schema locally in the thread, there are two scenarios that depend on it:
  // 1. set_meta_info_: first build local schema cache, depends on ObObj2strHelper
  // 2. format_row_: formatting row data, relies on ObObj2strHelper
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  bool need_ignore = false;

  if (OB_FAIL(get_schema_with_online_schema_(
      dml_stmt_task.get_global_schema_version(),
      dml_stmt_task.get_host().get_tenant_id(),
      dml_stmt_task.get_table_id(),
      stop_flag,
      schema_guard,
      table_schema,
      db_schema_info))) {
    // Ignore the statement if the tenant was deleted, or the table was deleted or the get schema failed
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      LOG_INFO("[IGNORE_DATA] get schema error, tenant may be dropped",
          K(tenant_id),
          "table_id", dml_stmt_task.get_table_id(),
          K(dml_stmt_task));
      br.set_is_valid(false);
      // reset ret
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_schema_ fail", KR(ret), K(dml_stmt_task), K(table_schema));
    }
  } else if (OB_FAIL(check_table_need_ignore_(table_schema, dml_stmt_task, need_ignore))) {
    LOG_ERROR("check_table_need_ignore_ failed", KR(ret), KPC(table_schema), K(dml_stmt_task), K(need_ignore));
  } else if (need_ignore) {
    br.set_is_valid(false);
  } else if (OB_FAIL(get_tenant_compat_mode(tenant_id, compat_mode, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_tenant_compat_mode fail", KR(ret), K(tenant_id),
          "compat_mode", print_compat_mode(compat_mode), KPC(table_schema));
    }
  } else {
    lib::CompatModeGuard g(compat_mode);

    if (OB_FAIL(set_meta_info_with_online_schema_(
        tenant_id,
        dml_stmt_task.get_global_schema_version(),
        table_schema,
        db_schema_info,
        schema_guard,
        &br,
        stop_flag))) {
      // Failed to get schema, ignore the data
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_INFO("[IGNORE_DATA] schema error when set_meta_info, tenant may be dropped", KR(ret), K(tenant_id),
            "table_id", dml_stmt_task.get_table_id(), K(dml_stmt_task), K(db_schema_info));
        br.set_is_valid(false);
        ret = OB_SUCCESS;
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("set_meta_info_ fail", KR(ret), K(table_schema), K(db_schema_info), K(br),
            "compat_mode", print_compat_mode(compat_mode));
      }
    } else if (OB_FAIL(format_row_(
        *table_schema,
        row_value,
        dml_stmt_task,
        br,
        cur_stmt_need_callback,
        stop_flag))) {
      LOG_ERROR("format_row failed", KR(ret), K(dml_stmt_task), K(cur_stmt_need_callback),
          "compat_mode", print_compat_mode(compat_mode), KPC(table_schema));
    } else {
      // Note: You cannot continue to manipulate any data structures afterwards.
    }
  }

  return ret;
}

int ObLogFormatter::handle_dml_stmt_with_dict_schema_(
    DmlStmtTask &dml_stmt_task,
    RowValue &row_value,
    ObLogBR &br,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfoGuard dict_tenant_info_guard;
  DBSchemaInfo db_schema_info;
  datadict::ObDictTableMeta *table_schema = NULL;
  const uint64_t tenant_id = dml_stmt_task.get_tenant_id();
  // Get the tenant schema: MYSQL or ORACLE
  // To ensure the correctness of ObObj2strHelper::obj2str, you need to set the mysql or Oracle schema locally in the thread, there are two scenarios that depend on it:
  // 1. set_meta_info_: first build local schema cache, depends on ObObj2strHelper
  // 2. format_row_: formatting row data, relies on ObObj2strHelper
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  const bool enable_formatter_print_log = (TCONF.enable_formatter_print_log != 0);
  bool need_ignore = false;

  if (enable_formatter_print_log) {
    LOG_DEBUG("handle_dml_stmt_with_dict_schema_", K(tenant_id), K(dml_stmt_task));
  }

  if (OB_FAIL(get_schema_with_data_dict_(
      dml_stmt_task.get_global_schema_version(),
      dml_stmt_task.get_host().get_tenant_id(),
      dml_stmt_task.get_table_id(),
      stop_flag,
      dict_tenant_info_guard,
      table_schema,
      db_schema_info))) {
    // Ignore the statement if the tenant was deleted, or the table was deleted or the get schema failed
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      LOG_INFO("[IGNORE_DATA] get schema error, tenant may be dropped",
          K(tenant_id),
          "table_id", dml_stmt_task.get_table_id(),
          K(dml_stmt_task));
      br.set_is_valid(false);
      // reset ret
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_schema_ fail", KR(ret), K(dml_stmt_task), K(table_schema));
    }
  } else if (OB_FAIL(check_table_need_ignore_(table_schema, dml_stmt_task, need_ignore))) {
    LOG_ERROR("check_table_need_ignore_ failed", KR(ret), KPC(table_schema), K(dml_stmt_task), K(need_ignore));
  } else if (need_ignore) {
    br.set_is_valid(false);
  } else if (OB_FAIL(get_tenant_compat_mode_with_data_dict_(tenant_id, dict_tenant_info_guard,
          compat_mode, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_tenant_compat_mode_with_data_dict_ fail", KR(ret), K(tenant_id),
          "compat_mode", print_compat_mode(compat_mode), KPC(table_schema));
    }
  } else {
    lib::CompatModeGuard g(compat_mode);

    if (OB_FAIL(set_meta_info_with_data_dict_(
        tenant_id,
        dml_stmt_task.get_global_schema_version(),
        table_schema,
        db_schema_info,
        &br,
        stop_flag))) {
      // Failed to get schema, ignore the data
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_INFO("[IGNORE_DATA] schema error when set_meta_info, tenant may be dropped", KR(ret), K(tenant_id),
            "table_id", dml_stmt_task.get_table_id(), K(dml_stmt_task), K(db_schema_info));
        br.set_is_valid(false);
        ret = OB_SUCCESS;
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("set_meta_info_ fail", KR(ret), K(tenant_id), K(table_schema), K(db_schema_info), K(br),
            "compat_mode", print_compat_mode(compat_mode));
      }
    } else if (OB_FAIL(format_row_(
        *table_schema,
        row_value,
        dml_stmt_task,
        br,
        cur_stmt_need_callback,
        stop_flag))) {
      LOG_ERROR("format_row failed", KR(ret), K(dml_stmt_task), K(cur_stmt_need_callback),
          "compat_mode", print_compat_mode(compat_mode), KPC(table_schema));
    } else {
      // Note: You cannot continue to manipulate any data structures afterwards.
    }
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::check_table_need_ignore_(
    const TABLE_SCHEMA *table_schema,
    DmlStmtTask &dml_stmt_task,
    bool &need_ignore)
{
  int ret = OB_SUCCESS;
  need_ignore = false;
  const bool enable_formatter_print_log = (TCONF.enable_formatter_print_log != 0);

  // For Online Schema:
  // Failed to get table schema, table was deleted
  // TODO: After the table is deleted, do some aftercare
  if (OB_ISNULL(table_schema)) {
    need_ignore = true;
    if (enable_formatter_print_log) {
      LOG_INFO("[IGNORE_DATA] get schema error, table may be dropped",
          "table_id", dml_stmt_task.get_table_id(),
          K(dml_stmt_task));
    } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("[IGNORE_DATA] get schema error, table may be dropped",
          "table_id", dml_stmt_task.get_table_id(),
          K(dml_stmt_task));
    }
  } else if (table_schema->is_aux_lob_meta_table()) {
    // set false for aux lob meta table
    need_ignore = true;
    LOG_DEBUG("is_aux_lob_meta_table",
        "table_name", table_schema->get_table_name(),
        "table_id", table_schema->get_table_id(),
        "table_type", ob_table_type_str(table_schema->get_table_type()));

    // Incremental direct load doesn't support lob with outrow in phase1 and Observer will
    // disallow the behavior which loads lob data with outrow.
    bool need_parse_aux_lob_meta_table = true;
    // need_parse_aux_lob_meta_table = !dml_stmt_task.get_redo_log_entry_task().get_redo_log_node()->is_direct_load_inc_log();
    if (need_parse_aux_lob_meta_table && OB_FAIL(parse_aux_lob_meta_table_(dml_stmt_task))) {
      LOG_ERROR("parse_aux_lob_meta_table_ failed", KR(ret), K(need_parse_aux_lob_meta_table),
          K(dml_stmt_task));
    }
  // Filter sys tables that are not user tables and are not in backup mode
  } else if (! table_schema->is_user_table()
      && ! BackupTableHelper::is_sys_table_exist_on_backup_mode(
          table_schema->is_sys_table(),
          table_schema->get_table_id())) {
    need_ignore = true;
    LOG_DEBUG("[IGNORE_DATA] ignore non-user table or sys table not exist on backup mode",
        "table_name", table_schema->get_table_name(),
        "table_id", table_schema->get_table_id(),
        "table_type", ob_table_type_str(table_schema->get_table_type()));
  }
  // Ignore data from tables in the recycle bin
  else if (table_schema->is_in_recyclebin() && ! is_backup_mode()) {
    need_ignore = true;
    if (enable_formatter_print_log) {
      LOG_INFO("[IGNORE_DATA] table is in recyclebin",
          "table_id", dml_stmt_task.get_table_id(),
          "is_backup_mode", is_backup_mode(),
          K(dml_stmt_task));
    } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("[IGNORE_DATA] table is in recyclebin",
          "table_id", dml_stmt_task.get_table_id(),
          "is_backup_mode", is_backup_mode(),
          K(dml_stmt_task));
    }
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::format_row_(
    const TABLE_SCHEMA &table_schema,
    RowValue &row_value,
    DmlStmtTask &dml_stmt_task,
    ObLogBR &br,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t new_column_cnt = 0;
  const uint64_t tenant_id = dml_stmt_task.get_tenant_id();

  if (OB_FAIL(build_row_value_(
      tenant_id,
      &row_value,
      &dml_stmt_task,
      &table_schema,
      new_column_cnt,
      cur_stmt_need_callback,
      stop_flag))) {
    LOG_ERROR("build_row_value_ fail", KR(ret), K(tenant_id), K(dml_stmt_task), K(row_value),
        K(new_column_cnt), K(cur_stmt_need_callback));
  } else if (! cur_stmt_need_callback) {
    if (OB_FAIL(build_binlog_record_(
        &br,
        &row_value,
        new_column_cnt,
        dml_stmt_task.get_dml_flag(),
        &table_schema))) {
      LOG_ERROR("build_binlog_record_ fail", KR(ret), K(br), K(row_value), K(new_column_cnt), K(dml_stmt_task));
    } else if (OB_UNLIKELY(!br.is_valid())) {
      // 1. not found valid column(heap table with all column virtual generated)
      // 2. dml_falg is DF_LOCK
      // 3. tenant has been dropped
      // 4. not user_table or table in recyclebin
    } else {
      if (OB_NOT_NULL(br.get_data())
          && OB_UNLIKELY(SRC_FULL_RECORDED != br.get_data()->getSrcCategory())) {
        // Handling non-full column logging modes: currently not support
        handle_non_full_columns_(dml_stmt_task, table_schema);
        if (skip_dirty_data_) {
          ret = OB_SUCCESS;
        } else {
          // Do not ignore, requires full log, if not full log, exit with an error
          ret = OB_NOT_SUPPORTED;
        }
      }
    }
  } else {
    // cur_stmt_need_callback is true, do nothing, wait callback process.
    // Note: You cannot continue to manipulate any data structures afterwards.
  }

  return ret;
}

template<class TABLE_SCHEMA>
void ObLogFormatter::handle_non_full_columns_(
    DmlStmtTask &dml_stmt_task,
    const TABLE_SCHEMA &table_schema)
{
  PartTransTask &task = dml_stmt_task.get_host();

  if (! skip_dirty_data_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "row data is not full recorded",
        "tls_id", task.get_tls_id(),
        "commit_log_lsn", task.get_commit_log_lsn(),
        "commit_version", task.get_trans_commit_version(),
        "dml_type", dml_stmt_task.get_dml_flag(),
        "dml_type_str", dml_stmt_task.get_dml_flag().getFlagStr(),
        "table_name", table_schema.get_table_name(),
        "table_id", table_schema.get_table_id(),
        K(dml_stmt_task));
  }
}

int ObLogFormatter::finish_format_(PartTransTask &part_trans_task,
    ObLogEntryTask &redo_log_entry_task,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else {
    const int64_t stmt_num = redo_log_entry_task.get_stmt_num();
    int64_t formatted_stmt_num = redo_log_entry_task.inc_formatted_stmt_num();
    const bool is_all_stmt_formatted = formatted_stmt_num >= stmt_num;
    const uint64_t tenant_id = part_trans_task.get_tenant_id();
    int64_t row_ref_cnt = 0;

    if (is_all_stmt_formatted) {
      if (OB_FAIL(redo_log_entry_task.link_row_list(row_ref_cnt))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("redo_log_entry_task link_row_list fail", KR(ret), K(redo_log_entry_task));
        }
      } else {
        LOG_DEBUG("[FORMATT]", K(tenant_id), K(stmt_num), KP(&redo_log_entry_task), K(redo_log_entry_task), K(part_trans_task));
        IObLogResourceCollector *resource_collector = TCTX.resource_collector_;

        if (0 == row_ref_cnt) {
          if (OB_ISNULL(resource_collector)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("resource collector is NULL", KR(ret));
          } else if (OB_FAIL(resource_collector->revert_log_entry_task(&redo_log_entry_task))) {
            if (OB_IN_STOP_STATE != ret) {
              LOG_ERROR("revert LogEntryTask fail", KR(ret), K(redo_log_entry_task), K(stop_flag));
            }
          } else {}
        }
      }

      if (OB_SUCC(ret)) {
        ATOMIC_DEC(&log_entry_task_count_);
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

// @retval OB_SUCCESS                  success
// @retval OB_TENANT_HAS_BEEN_DROPPED  tenant dropped
// #retval other error code            fail
int ObLogFormatter::set_meta_info_with_online_schema_(
    const uint64_t tenant_id,
    const int64_t global_schema_version,
    const TableSchemaType *simple_table_schema,
    const DBSchemaInfo &db_schema_info,
    ObLogSchemaGuard &schema_guard,
    ObLogBR *br,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(simple_table_schema) || OB_UNLIKELY(! db_schema_info.is_valid()) || OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(simple_table_schema), K(br), K(db_schema_info));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K(meta_manager_));
  } else {
    IDBMeta *db_meta = NULL;
    ITableMeta *table_meta = NULL;

    if (OB_FAIL(meta_manager_->get_table_meta(
        tenant_id,
        global_schema_version,
        simple_table_schema,
        table_meta,
        stop_flag)) || OB_ISNULL(table_meta)) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("schema error when get_table_meta, tenant may by dropped", KR(ret), K(tenant_id),
            K(global_schema_version),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name());
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get_table_meta fail", KR(ret), K(global_schema_version), "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(), KPC(simple_table_schema), K(table_meta));
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
    } else if (OB_FAIL(meta_manager_->get_db_meta(tenant_id, db_schema_info, schema_guard, db_meta, stop_flag))
        || OB_ISNULL(db_meta)) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("schema error when get_db_meta, tenant may by dropped", KR(ret), K(tenant_id),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(),
            K(db_schema_info));
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get_db_meta fail", KR(ret),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(),
            K(db_schema_info));
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
    } else if (OB_FAIL(br->set_table_meta(table_meta))) {
      LOG_ERROR("set_table_meta fail", KR(ret), K(br), K(table_meta));
    } else if (OB_FAIL(br->set_db_meta(db_meta))) {
      LOG_ERROR("set_db_meta fail", KR(ret), K(br), K(db_meta));
    } else {
      // success
    }

    if (OB_SUCCESS != ret) {
      if (NULL != table_meta) {
        meta_manager_->revert_table_meta(table_meta);
        table_meta = NULL;
      }

      if (NULL != db_meta) {
        meta_manager_->revert_db_meta(db_meta);
        db_meta = NULL;
      }
    }
  }

  return ret;
}

int ObLogFormatter::set_meta_info_with_data_dict_(
    const uint64_t tenant_id,
    const int64_t global_schema_version,
    datadict::ObDictTableMeta *simple_table_schema,
    const DBSchemaInfo &db_schema_info,
    ObLogBR *br,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(simple_table_schema) || OB_UNLIKELY(! db_schema_info.is_valid()) || OB_ISNULL(br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(simple_table_schema), K(br), K(db_schema_info));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K(meta_manager_));
  } else {
    IDBMeta *db_meta = NULL;
    ITableMeta *table_meta = NULL;

    if (OB_FAIL(meta_manager_->get_table_meta(
        tenant_id,
        global_schema_version,
        simple_table_schema,
        table_meta,
        stop_flag)) || OB_ISNULL(table_meta)) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("schema error when get_table_meta, tenant may by dropped", KR(ret), K(tenant_id),
            K(global_schema_version),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name());
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get_table_meta fail", KR(ret), K(global_schema_version), "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(), KPC(simple_table_schema), K(table_meta));
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
    } else if (OB_FAIL(meta_manager_->get_db_meta(tenant_id, db_schema_info, db_meta, stop_flag))
        || OB_ISNULL(db_meta)) {
      if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
        LOG_WARN("schema error when get_db_meta, tenant may by dropped", KR(ret), K(tenant_id),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(),
            K(db_schema_info));
      } else if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get_db_meta fail", KR(ret),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(),
            K(db_schema_info));
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      }
    } else if (OB_FAIL(br->set_table_meta(table_meta))) {
      LOG_ERROR("set_table_meta fail", KR(ret), K(br), K(table_meta));
    } else if (OB_FAIL(br->set_db_meta(db_meta))) {
      LOG_ERROR("set_db_meta fail", KR(ret), K(br), K(db_meta));
    } else {
      // success
      if (TCONF.enable_formatter_print_log != 0) {
        LOG_DEBUG("set_meta_info_with_data_dict_ succ", K(tenant_id), K(global_schema_version));
      }
    }

    if (OB_SUCCESS != ret) {
      if (NULL != table_meta) {
        meta_manager_->revert_table_meta(table_meta);
        table_meta = NULL;
      }

      if (NULL != db_meta) {
        meta_manager_->revert_db_meta(db_meta);
        db_meta = NULL;
      }
    }
  }

  return ret;
}

int ObLogFormatter::init_row_value_array_(const int64_t row_value_num)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(row_value_num <= 0)) {
    LOG_ERROR("invalid argument", K(row_value_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t size = sizeof(RowValue) * row_value_num;
    void *ptr = allocator_.alloc(size);

    if (NULL == (row_value_array_ = static_cast<RowValue *>(ptr))) {
      LOG_ERROR("allocate memory for RowValue fail", K(size), K(row_value_num));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t index = 0; index < row_value_num; index++) {
        row_value_array_[index].reset();
      }
    }
  }

  return ret;
}

void ObLogFormatter::destroy_row_value_array_()
{
  if (NULL != row_value_array_) {
    allocator_.free(static_cast<void *>(row_value_array_));
    row_value_array_ = NULL;
  }
}

template<class TABLE_SCHEMA>
int ObLogFormatter::build_row_value_(
    const uint64_t tenant_id,
    RowValue *rv,
    DmlStmtTask *stmt_task,
    const TABLE_SCHEMA *simple_table_schema,
    int64_t &new_column_cnt,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ColValueList *rowkey_cols = nullptr;
  ColValueList *new_cols = nullptr;
  ColValueList *old_cols = nullptr;
  ObLobDataOutRowCtxList *new_lob_ctx_cols = nullptr;
  TableSchemaInfo *tb_schema_info = NULL;
  IObCDCTimeZoneInfoGetter *tz_info_getter = TCTX.timezone_info_getter_;
  ObCDCTenantTimeZoneInfo *obcdc_tenant_tz_info = nullptr;
  ObTimeZoneInfoWrap *tz_info_wrap = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(rv) || OB_ISNULL(stmt_task) || OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(rv), K(stmt_task), K(simple_table_schema));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K(meta_manager_));
  } else if (OB_FAIL(meta_manager_->get_table_schema_meta(
        simple_table_schema->get_schema_version(),
        simple_table_schema->get_tenant_id(),
        simple_table_schema->get_table_id(),
        tb_schema_info))) {
    LOG_ERROR("meta_manager_ get_table_schema_meta fail", KR(ret),
        "version", simple_table_schema->get_schema_version(),
        "tenant_id", simple_table_schema->get_tenant_id(),
        "table_id", simple_table_schema->get_table_id(),
        "table_name", simple_table_schema->get_table_name(), KPC(tb_schema_info));
  } else if (OB_ISNULL(tb_schema_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tb_schema_info is null", KR(ret), K(tb_schema_info));
  } else if (OB_ISNULL(tz_info_getter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tz_info_getter is nullptr", KR(ret), K(tz_info_getter));
  } else if (OB_FAIL(tz_info_getter->get_tenant_tz_info(tenant_id, obcdc_tenant_tz_info))) {
    LOG_ERROR("get_tenant_tz_wrap failed", KR(ret), K(tenant_id));
  } else {
    const ObTimeZoneInfoWrap *tz_info_wrap = &(obcdc_tenant_tz_info->get_tz_wrap());
    const int64_t column_num = tb_schema_info->get_usr_column_count();
    const uint64_t aux_lob_meta_tid = tb_schema_info->get_aux_lob_meta_tid();
    const bool is_cur_stmt_task_cb_progress = stmt_task->is_callback();

    if (column_num <= 0) {
      LOG_INFO("no valid column is found", "table_name", simple_table_schema->get_table_name(),
          "table_id", simple_table_schema->get_table_id());
    } else if (! is_cur_stmt_task_cb_progress && OB_FAIL(stmt_task->parse_cols(
        obj2str_helper_,
        tb_schema_info,
        tz_info_wrap,
        enable_output_hidden_primary_key_))) {
      LOG_ERROR("stmt_task.parse_cols fail", KR(ret), K(*stmt_task), K(obj2str_helper_),
          KPC(simple_table_schema), KPC(tb_schema_info),
          K(enable_output_hidden_primary_key_));
    } else if (OB_FAIL(stmt_task->get_cols(&rowkey_cols, &new_cols, &old_cols, &new_lob_ctx_cols))) {
      LOG_ERROR("get_cols fail", KR(ret), K(*stmt_task));
    } else if (OB_ISNULL(rowkey_cols) || OB_ISNULL(new_cols) || OB_ISNULL(old_cols)
        || OB_ISNULL(new_lob_ctx_cols)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get_cols fail", KR(ret), K(rowkey_cols), K(new_cols), K(old_cols),
          K(new_lob_ctx_cols));
    } else if (OB_FAIL(handle_lob_ctx_cols_(tenant_id, aux_lob_meta_tid, *stmt_task, new_lob_ctx_cols,
            cur_stmt_need_callback, stop_flag))) {
      LOG_ERROR("handle_lob_ctx_cols_ fail", KR(ret), K(tenant_id), K(aux_lob_meta_tid),
          K(new_lob_ctx_cols), K(cur_stmt_need_callback));
    } else if (! cur_stmt_need_callback) {
      // NOTE: Logic for determining whether an old value is included: the data in the old value is not empty
      if (OB_FAIL(rv->init(column_num, old_cols->num_ > 0))) {
        LOG_ERROR("init RowValue fail", KR(ret), K(column_num));
      }
      // fill new column value
      else if (OB_FAIL(fill_normal_cols_(*stmt_task, rv, *new_cols, *new_lob_ctx_cols, simple_table_schema,
          *tb_schema_info, tz_info_wrap, true))) {
        LOG_ERROR("fill normal new columns fail", KR(ret), K(rv), KPC(new_cols));
      }
      // fill old column value
      else if (OB_FAIL(fill_normal_cols_(*stmt_task, rv, *old_cols, *new_lob_ctx_cols, simple_table_schema,
          *tb_schema_info, tz_info_wrap, false))) {
        LOG_ERROR("fill normal old columns fail", KR(ret), K(rv), KPC(old_cols));
      } else if (OB_FAIL(fill_rowkey_cols_(rv, *rowkey_cols, simple_table_schema,
              *tb_schema_info))) {
        LOG_ERROR("fill_rowkey_cols_ fail", KR(ret), K(rv), KPC(rowkey_cols),
            "stmt_task", *stmt_task, K(simple_table_schema));
      } else if (OB_FAIL(fill_orig_default_value_(rv, simple_table_schema, *tb_schema_info,
              stmt_task->get_redo_log_entry_task().get_allocator()))) {
        LOG_ERROR("fill_orig_default_value_ fail", KR(ret), K(rv), K(simple_table_schema));
      } else {
        new_column_cnt = new_cols->num_;
        int64_t column_array_size = sizeof(binlogBuf) * column_num;
        binlogBuf *new_column_array =
          static_cast<binlogBuf *>(stmt_task->get_redo_log_entry_task().alloc(column_array_size));
        binlogBuf *old_column_array =
          static_cast<binlogBuf *>(stmt_task->get_redo_log_entry_task().alloc(column_array_size));

        if (OB_ISNULL(new_column_array) || OB_ISNULL(old_column_array)) {
          LOG_ERROR("allocate memory for column array fail", K(column_array_size), K(column_num));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          rv->new_column_array_ = new_column_array;
          rv->old_column_array_ = old_column_array;
        }
      }
    } else {
      // cur_stmt_need_callback is true, do nothing, wait callback process.
      // Note: You cannot continue to manipulate any data structures afterwards.
    }
  }

  return ret;
}

int ObLogFormatter::handle_lob_ctx_cols_(
    const uint64_t tenant_id,
    const uint64_t aux_lob_meta_tid,
    DmlStmtTask &dml_stmt_task,
    ObLobDataOutRowCtxList *new_lob_ctx_cols,
    bool &cur_stmt_need_callback,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask *log_entry_task = NULL;
  IObCDCLobDataMerger *lob_data_merger = TCTX.lob_data_merger_;

  if (OB_ISNULL(lob_data_merger)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lob_data_merger is nullptr", KR(ret));
  } else if (OB_ISNULL(log_entry_task = &(dml_stmt_task.get_redo_log_entry_task()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log_entry_task is NULL", KR(ret), K(dml_stmt_task));
  } else if (dml_stmt_task.is_callback()) {
    // do nothing
  } else {
    const transaction::ObTransID &trans_id = log_entry_task->get_trans_id();

    if (nullptr != new_lob_ctx_cols && new_lob_ctx_cols->has_out_row_lob()) {
      dml_stmt_task.mark_callback();
      cur_stmt_need_callback = true;
      new_lob_ctx_cols->reset(&dml_stmt_task, tenant_id, trans_id, aux_lob_meta_tid, false/*is_ddl*/);

      if (OB_FAIL(lob_data_merger->push(*new_lob_ctx_cols, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("ObCDCLobDataMerger push failed", KR(ret));
        }
      } else {
        ATOMIC_INC(&stmt_in_lob_merger_count_);
      }
    }
  }

  return ret;
}

int ObLogFormatter::group_udt_column_values_(
    const ColumnSchemaInfo &column_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value,
    DmlStmtTask &stmt_task,
    ObLobDataOutRowCtxList &ob_ctx_cols,
    ColValue &cv)
{
  int ret = OB_SUCCESS;
  if (! column_schema_info.is_udt_column()) {
    // do nothing if no group
  } else if (OB_FAIL(ObCDCUdtValueBuilder::build(
      column_schema_info,
      tz_info_wrap,
      is_new_value,
      stmt_task,
      *obj2str_helper_,
      ob_ctx_cols,
      cv))) {
    LOG_ERROR("build udt value failed", KR(ret), K(column_schema_info.get_column_id()), K(column_schema_info));
  }
  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::fill_normal_cols_(
    DmlStmtTask &stmt_task,
    RowValue *rv,
    ColValueList &cv_list,
    ObLobDataOutRowCtxList &lob_ctx_cols,
    const TABLE_SCHEMA *simple_table_schema,
    const TableSchemaInfo &tb_schema_info,
    const ObTimeZoneInfoWrap *tz_info_wrap,
    const bool is_new_value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(rv) || OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(rv), K(simple_table_schema));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K(meta_manager_));
  } else {
    const int64_t column_count = rv->column_num_;
    ColValue *cv = cv_list.head_;

    while (OB_SUCC(ret) && OB_NOT_NULL(cv)) {
      const uint64_t column_id = cv->column_id_;
      int64_t usr_column_idx = OB_INVALID_INDEX;
      ColumnSchemaInfo *column_schema_info = NULL;

      if (OB_FAIL(tb_schema_info.get_column_schema_info_of_column_id(column_id, column_schema_info))) {
        LOG_ERROR("get_column_schema_info_of_column_id failed", KR(ret),
              "table_id", simple_table_schema->get_table_id(),
              "table_schema_version", simple_table_schema->get_schema_version(),
              K(tb_schema_info),
              K(column_id),
              KPC(column_schema_info));
      } else {
        usr_column_idx = column_schema_info->get_usr_column_idx();
        if (OB_UNLIKELY(usr_column_idx >= column_count || usr_column_idx < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid usr_column_idx", KR(ret),
              "table_id", simple_table_schema->get_table_id(),
              "table_schema_version", simple_table_schema->get_schema_version(),
              K(tb_schema_info),
              K(column_id),
              KPC(column_schema_info));
        } else if (OB_FAIL(group_udt_column_values_(
            *column_schema_info,
            tz_info_wrap,
            is_new_value,
            stmt_task,
            lob_ctx_cols,
            *cv))) {
          LOG_ERROR("group_udt_column_values_ fail", KR(ret), K(usr_column_idx), K(column_id), K(is_new_value));
        } else if (is_new_value) {
          if (! cv->is_out_row_) {
            rv->new_columns_[usr_column_idx] = &cv->string_value_;
          } else {
            ObLobDataGetCtx *lob_data_get_ctx = nullptr;
            ObString *new_col_str = nullptr;
            if (OB_FAIL(lob_ctx_cols.get_lob_data_get_ctx(column_id, lob_data_get_ctx))) {
              if (OB_ENTRY_NOT_EXIST != ret) {
                LOG_ERROR("get_lob_column_value failed", KR(ret), K(column_id));
              }
            } else {
              new_col_str = &(lob_data_get_ctx->get_new_lob_column_value());
            }

            if (OB_SUCC(ret)) {
              if (lob_data_get_ctx->is_ext_info_log()) {
                if (cv->is_json()) {
                  rv->new_columns_[usr_column_idx] = new_col_str;
                  rv->is_diff_[usr_column_idx] = true;
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR("not support ext info log type", KR(ret), K(is_new_value), KPC(lob_data_get_ctx), KPC(cv));
                }
              } else if (cv->is_json() || cv->is_geometry() || cv->is_roaringbitmap()) {
                const common::ObObjType obj_type = cv->get_obj_type();
                cv->value_.set_string(obj_type, *new_col_str);

                if (OB_FAIL(stmt_task.parse_col(stmt_task.get_tenant_id(), column_id, *column_schema_info,
                    tz_info_wrap, *obj2str_helper_, *cv))) {
                  LOG_ERROR("stmt_task parse_col failed", KR(ret), K(stmt_task), K(column_id), KPC(cv));
                } else {
                  rv->new_columns_[usr_column_idx] = &cv->string_value_;
                }
              } else {
                rv->new_columns_[usr_column_idx] = new_col_str;
              }
              LOG_DEBUG("fill_normal_cols_", K(is_new_value), K(column_id), KPC(cv), K(lob_ctx_cols),
                  "md5", calc_md5_cstr(new_col_str->ptr(), new_col_str->length()),
                  "buf_len", new_col_str->length(), KPC(lob_data_get_ctx), "is_diff", rv->is_diff_[usr_column_idx]);
            } else if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              rv->new_columns_[usr_column_idx] = nullptr;
              rv->is_null_lob_columns_[usr_column_idx] = true;
              LOG_INFO("fill_normal_cols_ nullptr", K(is_new_value), KPC(cv), K(lob_ctx_cols));
            }
          }
          rv->is_changed_[usr_column_idx] = (1 != cv->is_col_nop_); // column is not changed if col_value is nop(may be in minimal mode)
        } else {
          if (! cv->is_out_row_) {
            if (cv->is_col_nop_) {
              rv->is_old_col_nop_[usr_column_idx] = true;
            } else {
              rv->old_columns_[usr_column_idx] = &cv->string_value_;
            }
          } else {
            ObLobDataGetCtx *lob_data_get_ctx = nullptr;
            ObString *old_col_str = nullptr;
            if (OB_FAIL(lob_ctx_cols.get_lob_data_get_ctx(column_id, lob_data_get_ctx))) {
              if (OB_ENTRY_NOT_EXIST != ret) {
                LOG_ERROR("get_lob_column_value failed", KR(ret), K(column_id));
              }
            } else {
              old_col_str = &(lob_data_get_ctx->get_old_lob_column_value());
            }

            if (OB_SUCC(ret)) {
              if (lob_data_get_ctx->is_ext_info_log()) {
                if (cv->is_json()) {
                  // old data isn't passed when data is partial json
                  // so need set is_null_lob_columns_
                  rv->old_columns_[usr_column_idx] = nullptr;
                  rv->is_null_lob_columns_[usr_column_idx] = true;
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_ERROR("not support ext info log type", KR(ret), K(is_new_value), KPC(lob_data_get_ctx), KPC(cv));
                }
              } else if (cv->is_json() || cv->is_geometry() || cv->is_roaringbitmap()) {
                const common::ObObjType obj_type = cv->get_obj_type();
                cv->value_.set_string(obj_type, *old_col_str);

                if (OB_FAIL(stmt_task.parse_col(stmt_task.get_tenant_id(), column_id, *column_schema_info,
                    tz_info_wrap, *obj2str_helper_, *cv))) {
                  LOG_ERROR("stmt_task parse_col failed", KR(ret), K(stmt_task), K(column_id), KPC(cv));
                } else {
                  rv->old_columns_[usr_column_idx] = &cv->string_value_;
                }
              } else {
                rv->old_columns_[usr_column_idx] = old_col_str;
              }
              // TODO remove
              LOG_DEBUG("fill_normal_cols_", K(is_new_value), K(column_id), KPC(cv), K(lob_ctx_cols),
                  "md5", calc_md5_cstr(old_col_str->ptr(), old_col_str->length()),
                  "buf_len", old_col_str->length(), KPC(lob_data_get_ctx));
            } else if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              rv->old_columns_[usr_column_idx] = nullptr;
              rv->is_null_lob_columns_[usr_column_idx] = true;
              LOG_INFO("fill_normal_cols_ nullptr", K(usr_column_idx), K(is_new_value), KPC(cv), K(lob_ctx_cols));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        cv = cv->next_;
      }
    } // while
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::fill_rowkey_cols_(
    RowValue *rv,
    ColValueList &rowkey_cols,
    const TABLE_SCHEMA *simple_table_schema,
    const TableSchemaInfo &tb_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(rv) || OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(rv), K(simple_table_schema), KR(ret));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K_(meta_manager));
  } else {
    ColValue *cv_node = rowkey_cols.head_;
    int64_t rowkey_count = rowkey_cols.num_;
    int64_t table_schema_version = simple_table_schema->get_schema_version();
    uint64_t table_id = simple_table_schema->get_table_id();
    ColumnSchemaInfo *column_schema_info = NULL;

    for (int64_t rowkey_index = 0;
        OB_SUCC(ret) && rowkey_index < rowkey_count;
        rowkey_index++, cv_node = cv_node->next_) {
      if (OB_ISNULL(cv_node)) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("column value node is NULL", KR(ret), K(rowkey_index), K(rowkey_count), K(cv_node));
      } else if (OB_FAIL(tb_schema_info.get_column_schema_info_for_rowkey(rowkey_index, column_schema_info))) {
        LOG_ERROR("get_column_schema_info_for_rowkey failed", KR(ret),
            "table_id", simple_table_schema->get_table_id(),
            "table_schema_version", simple_table_schema->get_schema_version(),
            K(tb_schema_info),
            K(rowkey_index),
            KPC(cv_node));
      } else if (OB_UNLIKELY(! column_schema_info->is_usr_column())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("rowkey_column is expected output to user", KR(ret),
            K(tb_schema_info), K(rowkey_index), K(column_schema_info), KPC(simple_table_schema));
      } else {
        const int16_t rowkey_usr_index = column_schema_info->get_usr_column_idx();
        // If the primary key column has been modified, the value after the modification is used, otherwise the value before the modification is used
        if (NULL == rv->new_columns_[rowkey_usr_index]) {
          rv->new_columns_[rowkey_usr_index] = &(cv_node->string_value_);
        }

        rv->is_rowkey_[rowkey_usr_index] = true;
        rv->is_changed_[rowkey_usr_index] = (1 != cv_node->is_col_nop_);

        if (rv->contain_old_column_ && NULL == rv->old_columns_[rowkey_usr_index]) {
          rv->old_columns_[rowkey_usr_index] = &(cv_node->string_value_);
        }
      }
    } // for
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::fill_orig_default_value_(
    RowValue *rv,
    const TABLE_SCHEMA *simple_table_schema,
    const TableSchemaInfo &tb_schema_info,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(rv) || OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(rv), K(simple_table_schema));
  } else if (OB_ISNULL(meta_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("meta_manager_ is null", KR(ret), K(meta_manager_));
  } else {
    int64_t column_count = rv->column_num_;
    int64_t table_schema_version = simple_table_schema->get_schema_version();
    uint64_t table_id = simple_table_schema->get_table_id();
    const bool enable_output_invisible_column = (0 != TCONF.enable_output_invisible_column);
    const bool is_column_stored_idx = false;

    for (int64_t usr_column_index = 0; OB_SUCC(ret) && usr_column_index < column_count; usr_column_index++) {
      ColumnSchemaInfo *column_schema_info = NULL;

      if (OB_FAIL(tb_schema_info.get_column_schema_info(usr_column_index, is_column_stored_idx, column_schema_info))) {
        LOG_ERROR("get_column_schema_info fail", KR(ret), K(usr_column_index), K(column_count), K(table_schema_version),
            K(table_id), "table_name", simple_table_schema->get_table_name(),
            K(is_column_stored_idx), K_(enable_output_hidden_primary_key),
            K(column_schema_info));
      } else if (OB_UNLIKELY(! column_schema_info->is_usr_column())) {
        // formatter should not be processed to non-user columns
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("handle non user column, unexpected", KR(ret), KPC(column_schema_info),
            "table_id", simple_table_schema->get_table_id(),
            "table_name", simple_table_schema->get_table_name(),
            "table_schema_version", simple_table_schema->get_schema_version(),
            K(usr_column_index), K(column_count));
      } else {
        // Determine if it is a newly added column, if it is a newly added column, then fill in the original default value
        // If neither the new value nor the old value has a value, then it must be a newly added column
        if (NULL != rv->new_columns_[usr_column_index]
            || NULL != rv->old_columns_[usr_column_index]) {
          rv->orig_default_value_[usr_column_index] = NULL;
        } else {
          // default vlaue
          const common::ObString *orig_default_value_str = column_schema_info->get_orig_default_value_str();
          ObString *str = static_cast<ObString *>(allocator.alloc(sizeof(ObString)));

          if (OB_ISNULL(str)) {
            LOG_ERROR("allocate memory for ObString fail", K(sizeof(ObString)));
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_ISNULL(orig_default_value_str)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("orig_default_value_str is null", KR(ret), K(usr_column_index),
                K(table_id), "table_name", simple_table_schema->get_table_name(),
                KPC(column_schema_info));
          // For varchar:
          // 1. the default value is NULL, which should be set to NULL
          // 2. The default value is an empty string, which should be set to ''
          } else if (NULL == orig_default_value_str->ptr()
              && 0 == orig_default_value_str->length()) {
            new (str) ObString();
          } else if (NULL != orig_default_value_str->ptr()
              && 0 == orig_default_value_str->length()) {
            // Empty strings do not require memcpy
            new (str) ObString();
            str->assign_ptr(ObObj2strHelper::EMPTY_STRING, static_cast<ObString::obstr_size_t>(0));
          } else {
            const int64_t length = orig_default_value_str->length();
            const char *ptr = orig_default_value_str->ptr();
            char *ptr_copy = NULL;

            if (OB_ISNULL(ptr_copy = static_cast<char *>(allocator.alloc(length)))) {
              LOG_ERROR("allocate memory fail", K(length));
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(ptr_copy, ptr, length);
              new (str) ObString(length, ptr_copy);
            }
          }

          if (OB_SUCC(ret)) {
            rv->orig_default_value_[usr_column_index] = str;

            LOG_DEBUG("cast column orig default value",
                "casted", *str,
                "table_id", simple_table_schema->get_table_id(),
                "table_name", simple_table_schema->get_table_name(),
                K(usr_column_index), K(index));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogFormatter::set_src_category_(IBinlogRecord *br_data,
    RowValue *rv,
    const ObDmlRowFlag &dml_flag,
    const bool is_hbase_mode_put)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br_data) || OB_ISNULL(rv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br_data), K(rv));
  } else {
    int src_category = SRC_NO;

    // 1. INSERT statements are always set to full column log format
    // 2. DELETE and UPDATE must be populated with old values in full column logging mode, so if they are populated with old values, they are in full column logging format
    // 3. OB-HBase mode put special handling
    if (dml_flag.is_insert() || rv->contain_old_column_ || is_hbase_mode_put) {
      src_category = SRC_FULL_RECORDED;
    } else {
      src_category = SRC_FULL_FAKED;
    }

    br_data->setSrcCategory(src_category);
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObLogFormatter::build_binlog_record_(
    ObLogBR *br,
    RowValue *rv,
    const int64_t new_column_cnt,
    const ObDmlRowFlag &dml_flag,
    const TABLE_SCHEMA *simple_table_schema)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *br_data = NULL;
  bool is_hbase_mode_put = false;
  const uint64_t table_id = simple_table_schema->get_table_id();

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(br) || OB_ISNULL(rv) || OB_ISNULL(simple_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br), K(rv), K(simple_table_schema));
  } else if (OB_ISNULL(br_data = br->get_data())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("binlog record data is invalid", KR(ret), K(br));
  } else if (OB_FAIL(is_hbase_mode_put_(table_id, dml_flag, rv->column_num_, new_column_cnt,
          rv->contain_old_column_, is_hbase_mode_put))) {
    LOG_ERROR("is_hbase_mode_put_ fail", KR(ret), K(table_id), K(dml_flag),
        "column_num", rv->column_num_,
        K(new_column_cnt),
        "contain_old_column", rv->contain_old_column_,
        K(is_hbase_mode_put));
  } else if (OB_FAIL(set_src_category_(br_data, rv, dml_flag, is_hbase_mode_put))) {
    LOG_ERROR("set_src_category_ fail", KR(ret), K(br_data), K(rv), K(dml_flag), K(is_hbase_mode_put));
  } else {
    // default to be valid
    br->set_is_valid(true);

    if (rv->column_num_ <= 0) {
      LOG_INFO("[IGNORE_DATA] ignore non-user-column table", "table_name", simple_table_schema->get_table_name(),
          "table_id", simple_table_schema->get_table_id());
      // ignore table with no columns
      br->set_is_valid(false);
    } else if (OB_ISNULL(rv->new_column_array_) || OB_ISNULL(rv->old_column_array_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid row value, new_column_array or old_column_array is invalid",
          K(rv->new_column_array_), K(rv->old_column_array_));
    } else {
      br_data->setNewColumn(rv->new_column_array_, static_cast<int>(rv->column_num_));
      br_data->setOldColumn(rv->old_column_array_, static_cast<int>(rv->column_num_));

      ObDmlRowFlag current_dml_flag = dml_flag;
      if (is_hbase_mode_put) {
        current_dml_flag.set_flag(DF_INSERT);
        // modify record type
        RecordType type = get_record_type(current_dml_flag);
        if (OB_FAIL(br->setInsertRecordTypeForHBasePut(type))) {
          LOG_ERROR("br setInsertRecordTypeForHBasePut fail", KR(ret), K(br),
              "type", print_record_type(type),
              K(dml_flag),
              K(current_dml_flag),
              "table_name", simple_table_schema->get_table_name(),
              "table_id", simple_table_schema->get_table_id());
        } else {
          // succ
        }
      }

      if (current_dml_flag.is_delete()) {
        ret = format_dml_delete_(br_data, rv);
      } else if (current_dml_flag.is_delete_insert()) {
        ret = format_dml_put_(br_data, rv);
      } else if (current_dml_flag.is_insert()) {
        ret = format_dml_insert_(br_data, rv);
      } else if (current_dml_flag.is_update()) {
        ret = format_dml_update_(br_data, rv);
      }  else {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("unknown DML type, not supported", K(current_dml_flag));
      }

      if (OB_FAIL(ret)) {
        LOG_ERROR("format dml failed", KR(ret), K(table_id), K(current_dml_flag), KPC(simple_table_schema));
      }
    }
  }

  return ret;
}

int ObLogFormatter::is_hbase_mode_put_(const uint64_t table_id,
    const ObDmlRowFlag &dml_flag,
    const int64_t column_number,
    const int64_t new_column_cnt,
    const bool contain_old_column,
    bool &is_hbase_mode_put)
{
  int ret = OB_SUCCESS;
  is_hbase_mode_put = false;
  bool is_hbase_table = false;

  if (enable_hbase_mode_) {
    if (OB_ISNULL(hbase_util_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("hbase_util_ is null", KR(ret), K(hbase_util_));
    } else if (OB_FAIL(hbase_util_->is_hbase_table(table_id, is_hbase_table))) {
      LOG_ERROR("ObLogHbaseUtil is_hbase_table fail", KR(ret), K(table_id), K(is_hbase_table));
    } else if (is_hbase_table && dml_flag.is_update() && false == contain_old_column) {
      if (column_number == new_column_cnt) {
        is_hbase_mode_put = true;
      } else if (skip_hbase_mode_put_column_count_not_consistency_) {
        is_hbase_mode_put = true;

        LOG_INFO("skip hbase mode put column count not consistency", K(table_id), K(dml_flag),
            "hbase_mode_put_column_cnt", new_column_cnt, K(column_number));
      } else {
        LOG_ERROR("hbase mode put column cnt is not consistency", K(table_id), K(dml_flag),
            "hbase_mode_put_column_cnt", new_column_cnt, K(column_number));
        ret = OB_ERR_UNEXPECTED;
      }

      LOG_DEBUG("[HBASE] [PUT]", K(is_hbase_mode_put), K(table_id), K(dml_flag),
          K(column_number), K(new_column_cnt), K(contain_old_column));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogFormatter::format_dml_delete_(IBinlogRecord *br_data, const RowValue *row_value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br_data) || OB_ISNULL(row_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br_data), K(row_value));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++) {
      // Handling primary key values
      if (row_value->is_rowkey_[i]) {
        // The primary key value is taken from the primary key value recorded in the new columns, regardless of whether it is a full column log or not
        // The primary key is set by the fill_rowkey_cols_() function, which must be placed in the new column
        // DELETE operations use the original primary key value and do not need to use the primary key value from the old value
        ObString *str = row_value->new_columns_[i];

        if (OB_ISNULL(str)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("rowkey column is NULL, unexcepted error", KR(ret), K(i), K(row_value->column_num_));
        } else {
          br_data->putOld(str->ptr(), str->length());
        }
      }
      // Handling non-primary key values
      else {
        ObString *str = nullptr;
        bool need_populate_old_value_to_null_or_empty = false;
        if (row_value->contain_old_column_) {
          // When full column logging, the non-rowkey column of oldCold is set to the corresponding value
          // If the column value is not provided, then it is a new column and the corresponding original default value is set
          str = row_value->old_columns_[i];
          if (OB_ISNULL(str)) {
            if (row_value->is_old_col_nop_[i]) {
              need_populate_old_value_to_null_or_empty = true;
              ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "delete_op with nop_old_col_value", i);
            } else if (row_value->is_null_lob_columns_[i]) {
              // check if is outrow lob old col
              need_populate_old_value_to_null_or_empty = true;
              ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "delete_op with null_lob_col_value", i);
            } else {
              if (OB_ISNULL(str = row_value->orig_default_value_[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("old column value and original default value are all invalid", KR(ret),
                    K(i), "column_num", row_value->column_num_);
              } else {
                br_data->putOld(str->ptr(), str->length());
              }
            }
          } else {
            br_data->putOld(str->ptr(), str->length());
          }
        } else {
          // Non-rowkey columns of oldCold are set to no-change status for non-full column logging
          ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "delete_op not contains old_column", i);
        }

        if (OB_SUCC(ret)) {
          LOG_DEBUG("put_old_column_value for delete operation",
              K(i), K(need_populate_old_value_to_null_or_empty),
              "value", str == nullptr ? "NULL": to_cstring(*str),
              "default_val", row_value->orig_default_value_[i]);
        }
      }
    }
  }

  return ret;
}

int ObLogFormatter::format_dml_insert_(IBinlogRecord *br_data, const RowValue *row_value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br_data) || OB_ISNULL(row_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br_data), K(row_value));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++) {
      if (!row_value->is_changed_[i]) {
        // use defualt value in case of format old data with new schema(e.g. add column)
        ObString *str_val = row_value->orig_default_value_[i];

        if (OB_ISNULL(str_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("column original default value is NULL", KR(ret), K(i),
              "column_num", row_value->column_num_);
        } else {
          br_data->putNew(str_val->ptr(), str_val->length());
        }
      } else {
        ObString *str_val = row_value->new_columns_[i];

        if (OB_ISNULL(str_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("changed column new value is NULL", KR(ret), K(i),
              "column_num", row_value->column_num_);
        } else {
          br_data->putNew(str_val->ptr(), str_val->length());
        }
      }

      // FIXME: No old values are populated, regardless of whether it is a full column log
    }
  }

  return ret;
}

int ObLogFormatter::format_dml_update_(IBinlogRecord *br_data, const RowValue *row_value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br_data) || OB_ISNULL(row_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br_data), K(row_value));
  } else {
    for (int i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++) {
      // fill column value after update
      if (! row_value->is_changed_[i]) {
        if (row_value->contain_old_column_) {
          // In the case of a full column log, for update, if a column is not updated, the new value is filled with the value in old_column
          // If there is no corresponding value in the old column either, the original default value is filled
          ObString *str_val = row_value->old_columns_[i];

          if (OB_ISNULL(str_val)) {
            if (row_value->is_old_col_nop_[i]) {
              ObLogBR::mark_value_populated_by_cdc(*br_data, true /*is_new_col*/, "update_op unchanged_col with nop_old_col", i);
            } else if (row_value->is_null_lob_columns_[i]) {
              ObLogBR::mark_value_populated_by_cdc(*br_data, true /*is_new_col*/, "update_op unchanged_col with null_lob", i);
            } else if (OB_ISNULL(str_val = row_value->orig_default_value_[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("new column value, old column value and original default value "
                  "are all invalid", KR(ret),
                  K(i), "column_num", row_value->column_num_);
            } else {
              br_data->putNew(str_val->ptr(), str_val->length());
            }
          } else {
            br_data->putNew(str_val->ptr(), str_val->length());
          }
        } else {
          // Mark as unmodified when not a full column log
          // e.g. updated columns not include outrow lob column
          ObLogBR::mark_value_populated_by_cdc(*br_data, true /*is_new_col*/, "update_op unchanged_col without old_col_val", i);
        }
      } else {
        ObString *str_val = row_value->new_columns_[i];

        if (OB_ISNULL(str_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("changed column new value is NULL", KR(ret), K(i),
              "column_num", row_value->column_num_);
        } else if (row_value->is_diff_[i]) {
          br_data->putNewDiff(str_val->ptr(), str_val->length());
          LOG_DEBUG("putNewDiff", K(i), KPC(str_val));
        } else {
          br_data->putNew(str_val->ptr(), str_val->length());
        }
      }

      if (OB_SUCC(ret)) {
        // fill column value before update
        bool is_changed = row_value->is_changed_[i];
        if (row_value->contain_old_column_) {
          // For full column logging, the old value is always filled with the value in old_column for updates
          // If there is no valid value in the old column, the original default value is filled
          ObString *str_val = row_value->old_columns_[i];

          if (OB_ISNULL(str_val)) {
            if (row_value->is_old_col_nop_[i]) {
              ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "update_op with nop_old_col", i);
            } else if (row_value->is_null_lob_columns_[i]) {
              ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "update_op with null_lob_col", i);
            } else if (OB_ISNULL(str_val = row_value->orig_default_value_[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("old column value and original default value are all invalid", KR(ret),
                  K(i), "column_num", row_value->column_num_,
                  "is_changed", row_value->is_changed_[i]);
            } else {
              br_data->putOld(str_val->ptr(), str_val->length());
            }
          } else {
            br_data->putOld(str_val->ptr(), str_val->length());
          }
        } else {
          // When not full column logging, for update, the old value is filled with whether the corresponding column has been modified
          ObLogBR::mark_value_populated_by_cdc(*br_data, false /*is_new_col*/, "update_op with unchanged old_col", i);
        }
      }
    } // end of for
  }

  return ret;
}

int ObLogFormatter::format_dml_put_(IBinlogRecord *br_data, const RowValue *row_value)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br_data) || OB_ISNULL(row_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(br_data), K(row_value));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_value->column_num_; i++) {
      if (!row_value->is_changed_[i]) {
        ObString *str_val = row_value->orig_default_value_[i];

        if (OB_ISNULL(str_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("column original default value is NULL", KR(ret), K(i),
              "column_num", row_value->column_num_);
        } else {
          br_data->putNew(str_val->ptr(), str_val->length());
        }
      } else {
        ObString *str_val = row_value->new_columns_[i];

        if (OB_ISNULL(str_val)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("changed column new value is NULL", KR(ret), K(i),
              "column_num", row_value->column_num_);
        } else {
          br_data->putNew(str_val->ptr(), str_val->length());
        }
      }

      // FIXME: No old values are populated, regardless of whether it is a full column log
    }
  }

  return ret;
}

int ObLogFormatter::get_schema_with_online_schema_(
    const int64_t version,
    const uint64_t tenant_id,
    const uint64_t table_id,
    volatile bool &stop_flag,
    ObLogSchemaGuard &schema_guard,
    const TableSchemaType *&table_schema,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_ISNULL(schema_getter_) || OB_UNLIKELY(version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP_(schema_getter), K(version));
  } else {
    int64_t refreshed_version = version;
    const uint64_t pure_tb_id = table_id;

    if (share::OB_ALL_SEQUENCE_VALUE_TID == pure_tb_id) {
      ObLogTenantGuard guard;
      ObLogTenant *tenant = NULL;

      if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
        LOG_ERROR("get tenant fail", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant = guard.get_tenant())) {
        LOG_ERROR("invalid tenant", K(guard), K(tenant));
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int64_t tenant_start_schema_version = tenant->get_start_schema_version();
        refreshed_version = max(version, tenant_start_schema_version);
      }
    }

    // get schema guard
    RETRY_FUNC(stop_flag, (*schema_getter_), get_schema_guard_and_table_schema,
        tenant_id,
        table_id,
        refreshed_version,
        GET_SCHEMA_TIMEOUT,
        schema_guard,
        table_schema);

    if (OB_SUCCESS == ret && NULL != table_schema) {
      uint64_t db_id = table_schema->get_database_id();

      // Get database schema information, including name and version
      RETRY_FUNC(stop_flag, schema_guard, get_database_schema_info, tenant_id, db_id, db_schema_info,
          GET_SCHEMA_TIMEOUT);
    }
  }

  return ret;
}

int ObLogFormatter::get_schema_with_data_dict_(
    const int64_t version,
    const uint64_t tenant_id,
    const uint64_t table_id,
    volatile bool &stop_flag,
    ObDictTenantInfoGuard &dict_tenant_info_guard,
    datadict::ObDictTableMeta *&table_schema,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(version));
  } else {
    ObDictTenantInfo *tenant_info = nullptr;

    if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
        tenant_id,
        dict_tenant_info_guard))) {
      LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_info->get_table_meta(table_id, table_schema))) {
      LOG_ERROR("get_table_meta failed", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(tenant_info->get_database_schema_info(table_schema->get_database_id(), db_schema_info))) {
      LOG_ERROR("get_database_schema_info failed", KR(ret), K(tenant_id), KPC(table_schema));
    } else {
      if (TCONF.enable_formatter_print_log != 0) {
        LOG_DEBUG("get_schema_with_data_dict_ succ", K(tenant_id), K(table_id), KPC(table_schema), K(db_schema_info));
      }
    }
  }

  return ret;
}

int ObLogFormatter::get_tenant_compat_mode_with_data_dict_(
    const uint64_t tenant_id,
    ObDictTenantInfoGuard &dict_tenant_info_guard,
    lib::Worker::CompatMode &compat_mode,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObDictTenantInfo *tenant_info = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFormatter has not been initialized", KR(ret));
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

int ObLogFormatter::init_dml_unique_id_(DmlStmtTask &stmt_task,
    ObLogEntryTask &log_entry_task,
    PartTransTask &part_trans_task,
    common::ObString &dml_unique_id)
{
  int ret = OB_SUCCESS;
  const ObString &part_trans_info_str = part_trans_task.get_part_trans_info();
  palf::LSN redo_log_lsn;
  const uint64_t row_no = stmt_task.get_row_index();

  if (OB_FAIL(log_entry_task.get_log_lsn(redo_log_lsn))) {
    LOG_ERROR("get_redo_log_lsn from log_entry_task failed", KR(ret), K(log_entry_task), K(redo_log_lsn));
  } else {
    DmlStmtUniqueID dml_stmt_unique_id(part_trans_info_str, redo_log_lsn, row_no);

    if (OB_UNLIKELY(! dml_stmt_unique_id.is_valid())) {
      LOG_ERROR("dml_stmt_unique_id is not valid", K(dml_stmt_unique_id));
      ret = OB_INVALID_ARGUMENT;
    } else {
      common::ObIAllocator &allocator= log_entry_task.get_allocator();
      const int64_t buf_len = dml_stmt_unique_id.get_dml_unique_id_length();
      char *buf = static_cast<char*>(allocator.alloc(buf_len));
      int64_t pos = 0;

      if (OB_ISNULL(buf)) {
        LOG_ERROR("allocate memory for trans id buffer fail", K(buf));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(dml_stmt_unique_id.customized_to_string(buf, buf_len, pos))) {
        LOG_ERROR("dml_stmt_unique_id customized_to_string fail", KR(ret), K(buf), K(buf_len), K(pos));
      } else {
        dml_unique_id.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }
  }

  return ret;
}

// For debug
void print(const char *event, ColValueList &cols)
{
  ColValue *col_value = cols.head_;

  while (nullptr != col_value) {
    LOG_INFO("col", K(event), KPC(col_value));

    if (col_value->column_id_ == 16) {
      ObLobId lob_id = *reinterpret_cast<ObLobId*>(col_value->value_.get_string().ptr());
      LOG_INFO("lobid", K(lob_id));
    }

    col_value = col_value->next_;
  }
}

int ObLogFormatter::parse_aux_lob_meta_table_(
    DmlStmtTask &stmt_task)
{
  int ret = OB_SUCCESS;
  ObLogEntryTask *log_entry_task = NULL;
  ObCDCGlobalInfo &globl_info = TCTX.global_info_;
  const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info = globl_info.get_lob_aux_table_schema_info();
  ColValueList *rowkey_cols = NULL;
  ColValueList *new_cols = NULL;
  ColValueList *old_cols = NULL;

  if (OB_ISNULL(log_entry_task = &(stmt_task.get_redo_log_entry_task()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log_entry_task is NULL", KR(ret), K(stmt_task));
  } else if (OB_FAIL(stmt_task.parse_aux_meta_table_cols(lob_aux_table_schema_info))) {
    LOG_ERROR("stmt_task parse_aux_meta_table_cols failed", KR(ret));
  } else if (OB_FAIL(stmt_task.get_cols(&rowkey_cols, &new_cols, &old_cols, nullptr/*new_lob_ctx_cols*/))) {
    LOG_ERROR("get_cols fail", KR(ret), K(stmt_task));
  } else {
    const uint64_t tenant_id = log_entry_task->get_tenant_id();
    const DmlRedoLogNode *redo_log_node = log_entry_task->get_redo_log_node();

    if (stmt_task.is_insert()) {
      if (OB_FAIL(parse_aux_lob_meta_table_insert_(*log_entry_task, stmt_task, *new_cols))) {
        LOG_ERROR("parse_aux_lob_meta_table_insert_ failed", KR(ret));
      }
    } else if (stmt_task.is_update()) {
      // lob meta update data isn't used, just skip
    } else if (OB_ISNULL(redo_log_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("redo_log_node is NULL", KR(ret));
    } else if (redo_log_node->is_direct_load_inc_log() && stmt_task.is_delete()) {
      // lob meta delete isn't used in direct load inc case, just skip
    } else if (stmt_task.is_delete()) {
      if (OB_FAIL(parse_aux_lob_meta_table_delete_(*log_entry_task, stmt_task, *old_cols))) {
        LOG_ERROR("parse_aux_lob_meta_table_delete_ failed", KR(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }

  return ret;
}

int ObLogFormatter::parse_aux_lob_meta_table_insert_(
    ObLogEntryTask &log_entry_task,
    DmlStmtTask &stmt_task,
    ColValueList &new_cols)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! stmt_task.is_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt_task is not INSERT operation", KR(ret));
  } else {
    const PartTransTask &part_trans_task = stmt_task.get_host();
    const int64_t commit_version = part_trans_task.get_trans_commit_version();
    const uint64_t tenant_id = log_entry_task.get_tenant_id();
    const transaction::ObTransID &trans_id = log_entry_task.get_trans_id();
    const uint64_t aux_lob_meta_table_id = stmt_task.get_table_id();
    ObLobId lob_id;
    const ObTxSEQ &row_seq_no = stmt_task.get_row_seq_no();
    const char *lob_data = nullptr;
    int64_t lob_data_len = 0;
    ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;

    if (OB_FAIL(ObCDCLobAuxMetaParse::parse_aux_lob_meta_table_row(new_cols, lob_id, lob_data, lob_data_len))) {
      LOG_ERROR("parse_aux_lob_meta_table_row_ failed", KR(ret), K(tenant_id));
    } else {
      LobAuxMetaKey lob_aux_meta_key(commit_version, tenant_id, trans_id, aux_lob_meta_table_id, lob_id, row_seq_no);

      if (OB_FAIL(lob_aux_meta_storager.put(lob_aux_meta_key, "insert", lob_data, lob_data_len))) {
        LOG_ERROR("lob_aux_meta_storager put failed", KR(ret), K(lob_aux_meta_key));
      }
    }
  }

  return ret;
}

int ObLogFormatter::parse_aux_lob_meta_table_delete_(
    ObLogEntryTask &log_entry_task,
    DmlStmtTask &stmt_task,
    ColValueList &old_cols)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! stmt_task.is_delete())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt_task is not DELETE operation", KR(ret));
  } else {
    const PartTransTask &part_trans_task = stmt_task.get_host();
    const int64_t commit_version = part_trans_task.get_trans_commit_version();
    const uint64_t tenant_id = log_entry_task.get_tenant_id();
    const transaction::ObTransID &trans_id = log_entry_task.get_trans_id();
    const uint64_t aux_lob_meta_table_id = stmt_task.get_table_id();
    ObLobId lob_id;
    const transaction::ObTxSEQ &row_seq_no = stmt_task.get_row_seq_no();
    const char *lob_data = nullptr;
    int64_t lob_data_len = 0;
    ObCDCLobAuxMetaStorager &lob_aux_meta_storager = TCTX.lob_aux_meta_storager_;

    if (OB_FAIL(ObCDCLobAuxMetaParse::parse_aux_lob_meta_table_row(old_cols, lob_id, lob_data, lob_data_len))) {
      LOG_ERROR("parse_aux_lob_meta_table_row_ failed", KR(ret), K(tenant_id));
    } else {
      LobAuxMetaKey lob_aux_meta_key(commit_version, tenant_id, trans_id, aux_lob_meta_table_id, lob_id, row_seq_no);

      if (OB_FAIL(lob_aux_meta_storager.put(lob_aux_meta_key, "delete", lob_data, lob_data_len))) {
        LOG_ERROR("lob_aux_meta_storager put failed", KR(ret), K(lob_aux_meta_key));
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
