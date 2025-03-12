/**
 * Copyright (c) 2023 OceanBase
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

#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/ob_server.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace sql;
using namespace storage;
using namespace share::schema;
using namespace table;

/**
 * ObTableLoadClientTaskParam
 */

ObTableLoadClientTaskParam::ObTableLoadClientTaskParam()
  : allocator_("TLD_CTask"),
    task_id_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    user_id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    table_name_(),
    parallel_(0),
    max_error_row_count_(0),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    timeout_us_(0),
    heartbeat_timeout_us_(0),
    load_method_(),
    column_names_(),
    part_names_()
{
  allocator_.set_tenant_id(MTL_ID());
  column_names_.set_block_allocator(ModulePageAllocator(allocator_));
  part_names_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObTableLoadClientTaskParam::~ObTableLoadClientTaskParam() {}

void ObTableLoadClientTaskParam::reset()
{
  client_addr_.reset();
  task_id_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  user_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  table_name_.reset();
  parallel_ = 0;
  max_error_row_count_ = 0;
  dup_action_ = ObLoadDupActionType::LOAD_INVALID_MODE;
  timeout_us_ = 0;
  heartbeat_timeout_us_ = 0;
  load_method_.reset();
  column_names_.reset();
  part_names_.reset();
  allocator_.reset();
}

int ObTableLoadClientTaskParam::assign(const ObTableLoadClientTaskParam &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    client_addr_ = other.client_addr_;
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    database_id_ = other.database_id_;
    parallel_ = other.parallel_;
    max_error_row_count_ = other.max_error_row_count_;
    dup_action_ = other.dup_action_;
    timeout_us_ = other.timeout_us_;
    heartbeat_timeout_us_ = other.heartbeat_timeout_us_;
    if (OB_FAIL(set_table_name(other.table_name_))) {
      LOG_WARN("fail to set table name", KR(ret));
    } else if (OB_FAIL(set_load_method(other.load_method_))) {
      LOG_WARN("fail to set load method", KR(ret));
    } else if (OB_FAIL(set_column_names(other.column_names_))) {
      LOG_WARN("fail to deep copy column names", KR(ret));
    } else if (OB_FAIL(set_part_names(other.part_names_))) {
      LOG_WARN("fail to deep copy part names", KR(ret));
    }
  }
  return ret;
}

bool ObTableLoadClientTaskParam::is_valid() const
{
  return client_addr_.is_valid() && task_id_ > 0 && OB_INVALID_TENANT_ID != tenant_id_ &&
         OB_INVALID_ID != user_id_ && OB_INVALID_ID != database_id_ && !table_name_.empty() &&
         parallel_ > 0 && max_error_row_count_ >= 0 &&
         ObLoadDupActionType::LOAD_INVALID_MODE != dup_action_ && timeout_us_ > 0 &&
         heartbeat_timeout_us_ > 0;
}

int ObTableLoadClientTaskParam::set_string(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator_, src, dest))) {
    LOG_WARN("fail to write string", KR(ret));
  }
  return ret;
}

int ObTableLoadClientTaskParam::set_string_array(const ObIArray<ObString> &src,
                                                 ObIArray<ObString> &dest)
{
  int ret = OB_SUCCESS;
  dest.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    const ObString &src_str = src.at(i);
    ObString dest_str;
    if (OB_FAIL(ob_write_string(allocator_, src_str, dest_str))) {
      LOG_WARN("fail to write string", KR(ret));
    } else if (OB_FAIL(dest.push_back(dest_str))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

/**
 * ClientTaskExecuteProcessor
 */

class ObTableLoadClientTask::ClientTaskExectueProcessor : public ObITableLoadTaskProcessor
{
public:
  ClientTaskExectueProcessor(ObTableLoadTask &task, ObTableLoadClientTask *client_task)
    : ObITableLoadTaskProcessor(task), client_task_(client_task)
  {
    client_task_->inc_ref_count();
  }
  virtual ~ClientTaskExectueProcessor() { ObTableLoadClientService::revert_task(client_task_); }
  int process() override
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObSQLSessionInfo *origin_session_info = THIS_WORKER.get_session();
    ObSQLSessionInfo *session_info = client_task_->session_info_;
    ObSchemaGetterGuard &schema_guard = client_task_->schema_guard_;
    ObTableLoadParam load_param;
    ObArray<uint64_t> column_ids;
    ObArray<ObTabletID> tablet_ids;
    THIS_WORKER.set_session(session_info);
    if (OB_ISNULL(session_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null session info", KR(ret));
    } else {
      session_info->set_thread_id(get_tid_cache());
      session_info->update_last_active_time();
      if (OB_TMP_FAIL(session_info->set_session_state(QUERY_ACTIVE))) {
        LOG_WARN("fail to set session state", KR(tmp_ret));
      }
    }
    // resolve
    if (OB_FAIL(
          resolve(client_task_->client_exec_ctx_, client_task_->param_, load_param, column_ids, tablet_ids))) {
      LOG_WARN("fail to resolve", KR(ret), K(client_task_->param_));
    }
    // check support
    else if (OB_FAIL(ObTableLoadService::check_support_direct_load(schema_guard,
                                                                   load_param.table_id_,
                                                                   load_param.method_,
                                                                   load_param.insert_mode_,
                                                                   load_param.load_mode_,
                                                                   load_param.load_level_,
                                                                   column_ids))) {
      LOG_WARN("fail to check support direct load", KR(ret));
    }
    // begin
    if (OB_SUCC(ret)) {
      if (OB_FAIL(client_task_->init_instance(load_param, column_ids, tablet_ids))) {
        LOG_WARN("fail to init instance", KR(ret));
      } else if (OB_FAIL(client_task_->set_status_waitting())) {
        LOG_WARN("fail to set status waitting", KR(ret));
      } else if (OB_FAIL(client_task_->set_status_running())) {
        LOG_WARN("fail to set status running", KR(ret));
      }
    }
    // wait client commit
    while (OB_SUCC(ret)) {
      ObTableLoadClientStatus status = client_task_->get_status();
      if (ObTableLoadClientStatus::RUNNING == status) {
        ob_usleep(100_ms);
      } else if (ObTableLoadClientStatus::COMMITTING == status) {
        break;
      } else if (ObTableLoadClientStatus::ABORT == status) {
        ret = client_task_->get_error_code();
        LOG_WARN("client task is abort", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected client status", KR(ret), K(status));
      }
    }
    // commit
    if (OB_SUCC(ret)) {
      if (OB_FAIL(client_task_->commit_instance())) {
        LOG_WARN("fail to commit instance", KR(ret));
      } else if (OB_FAIL(client_task_->set_status_commit())) {
        LOG_WARN("fail to set status running", KR(ret));
      }
    }
    client_task_->destroy_instance();
    THIS_WORKER.set_session(origin_session_info);
    if (session_info != nullptr && OB_TMP_FAIL(session_info->set_session_state(SESSION_SLEEP))) {
      LOG_WARN("fail to set session state", KR(tmp_ret));
    }
    return ret;
  }

  static int resolve(ObTableLoadClientExecCtx &client_exec_ctx,
                     const ObTableLoadClientTaskParam &task_param, ObTableLoadParam &load_param,
                     ObIArray<uint64_t> &column_ids, ObIArray<ObTabletID> &tablet_ids)
  {
    int ret = OB_SUCCESS;
    const uint64_t tenant_id = task_param.get_tenant_id();
    const uint64_t database_id = task_param.get_database_id();
    ObSchemaGetterGuard *schema_guard = nullptr;
    const ObTableSchema *table_schema = nullptr;
    ObDirectLoadMethod::Type method = ObDirectLoadMethod::INVALID_METHOD;
    ObDirectLoadInsertMode::Type insert_mode = ObDirectLoadInsertMode::INVALID_INSERT_MODE;
    ObCompressorType compressor_type = INVALID_COMPRESSOR;
    bool online_opt_stat_gather = false;
    double online_sample_percent = 100.;
    if (OB_UNLIKELY(!GCONF._ob_enable_direct_load)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "when disable direct load, table load client task is");
      LOG_WARN("when disable direct load, table load client task is not support", KR(ret));
    }
    else if (OB_ISNULL(schema_guard = client_exec_ctx.get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected schema guard is null", KR(ret));
    }
    // resolve table_name_
    else if (OB_FAIL(ObTableLoadSchema::get_table_schema(
               *schema_guard, tenant_id, database_id, task_param.get_table_name(), table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id),
               K(task_param.get_table_name()));
    }
    // resolve column_names_
    else if (OB_FAIL(resolve_columns(table_schema, task_param.get_column_names(), column_ids))) {
      LOG_WARN("fail to resolve columns", KR(ret), K(task_param.get_column_names()));
    }
    // resolve load_method_
    else if (OB_FAIL(resolve_load_method(task_param.get_load_method(), method, insert_mode))) {
      LOG_WARN("fail to resolve load method", KR(ret), K(task_param.get_load_method()));
    }
    // compress type
    else if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(
            table_schema, task_param.get_parallel(), compressor_type))) {
      LOG_WARN("fail to get tmp store compressor type", KR(ret));
    }
    // opt stat gather
    else if (OB_FAIL(ObTableLoadSchema::get_tenant_optimizer_gather_stats_on_load(
               tenant_id, online_opt_stat_gather))) {
      LOG_WARN("fail to get tenant optimizer gather stats on load", KR(ret), K(tenant_id));
    } else if (online_opt_stat_gather && OB_FAIL(ObDbmsStatsUtils::get_sys_online_estimate_percent(
                                           *(client_exec_ctx.exec_ctx_), tenant_id,
                                           table_schema->get_table_id(), online_sample_percent))) {
      LOG_WARN("failed to get sys online sample percent", K(ret));
    } else if (OB_FAIL(resolve_part_names(table_schema, task_param.get_part_names(), tablet_ids))) {
      LOG_WARN("fail to resolve part name", KR(ret));
    }
    if (OB_SUCC(ret) && ObDirectLoadMethod::INCREMENTAL == method) {
      if (ObDirectLoadInsertMode::NORMAL == insert_mode
          && ObLoadDupActionType::LOAD_REPLACE == task_param.get_dup_action()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace for inc load method in direct load is");
        LOG_WARN("replace for inc load method in direct load is not supported", KR(ret));
      } else if (ObDirectLoadInsertMode::INC_REPLACE == insert_mode
                 && ObLoadDupActionType::LOAD_STOP_ON_DUP != task_param.get_dup_action()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace or ignore for inc_replace load method in direct load is");
        LOG_WARN("replace or ignore for inc_replace load method in direct load is not supported", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      load_param.tenant_id_ = tenant_id;
      load_param.table_id_ = table_schema->get_table_id();
      load_param.parallel_ = task_param.get_parallel();
      load_param.session_count_ = task_param.get_parallel();
      load_param.batch_size_ = ObTableLoadParam::DEFAULT_BATCH_SIZE;
      load_param.max_error_row_count_ = task_param.get_max_error_row_count();
      load_param.column_count_ = column_ids.count();
      load_param.need_sort_ = true;
      load_param.px_mode_ = false;
      load_param.online_opt_stat_gather_ = online_opt_stat_gather;
      load_param.dup_action_ =
        // rewrite dup action to replace in inc_replace
        (method == ObDirectLoadMethod::INCREMENTAL &&
         insert_mode == ObDirectLoadInsertMode::INC_REPLACE)
          ? ObLoadDupActionType::LOAD_REPLACE
          : task_param.get_dup_action();
      load_param.method_ = method;
      load_param.insert_mode_ = insert_mode;
      load_param.load_mode_ = ObDirectLoadMode::TABLE_LOAD;
      load_param.compressor_type_ = compressor_type;
      load_param.online_sample_percent_ = online_sample_percent;
      load_param.load_level_ = tablet_ids.empty() ? ObDirectLoadLevel::TABLE
                                                  : ObDirectLoadLevel::PARTITION;
    }
    return ret;
  }

  static int resolve_columns(const ObTableSchema *table_schema,
                             const ObIArray<ObString> &column_names, ObIArray<uint64_t> &column_ids)
  {
    int ret = OB_SUCCESS;
    column_ids.reset();
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table schema is null", KR(ret), KP(table_schema));
    } else if (column_names.empty()) {
      if (OB_FAIL(ObTableLoadSchema::get_user_column_ids(table_schema, column_ids))) {
        LOG_WARN("fail to get user column ids", KR(ret));
      }
    } else {
      const static uint64_t INVALID_COLUMN_ID = UINT64_MAX;
      ObArray<uint64_t> user_column_ids;
      ObArray<ObString> user_column_names;
      if (OB_FAIL(ObTableLoadSchema::get_user_column_id_and_names(table_schema,
                                                                  user_column_ids,
                                                                  user_column_names))) {
        LOG_WARN("fail to get user column id and names", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
        const ObString &column_name = column_names.at(i);
        if (OB_UNLIKELY(column_name.empty())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("empty column name is invalid", KR(ret), K(i), K(column_names));
        } else {
          int64_t found_column_idx = -1;
          for (int64_t j = 0; found_column_idx == -1 && j < user_column_names.count(); ++j) {
            const ObString &user_column_name = user_column_names.at(j);
            if (column_name.length() != user_column_name.length()) {
            } else if (column_name.case_compare(user_column_name) == 0) {
              found_column_idx = j;
            }
          }
          if (OB_UNLIKELY(found_column_idx == -1)) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            LOG_WARN("unknow column", KR(ret), K(column_name), K(user_column_names));
          } else {
            const uint64_t user_column_id = user_column_ids.at(found_column_idx);
            if (OB_UNLIKELY(user_column_id == INVALID_COLUMN_ID)) {
              ret = OB_ERR_FIELD_SPECIFIED_TWICE;
              LOG_WARN("column specified twice", KR(ret), K(i), K(column_name), K(column_names));
            } else if (OB_FAIL(column_ids.push_back(user_column_id))) {
              LOG_WARN("fail to push back column id", KR(ret));
            } else {
              user_column_ids.at(found_column_idx) = INVALID_COLUMN_ID;
            }
          }
        }
      }
    }
    return ret;
  }

  static int resolve_part_names(const ObTableSchema *table_schema,
                                const ObIArray<ObString> &part_names,
                                ObIArray<ObTabletID> &tablet_ids)
  {
    int ret = OB_SUCCESS;
    ObArray<ObPartID> part_ids;
    uint64_t table_id = OB_INVALID_ID;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schems is nullptr", KR(ret));
    } else {
      table_id = table_schema->get_table_id();
      for (int i = 0; OB_SUCC(ret) && i < part_names.count(); i++) {
        ObArray<ObObjectID> partition_ids;
        ObString &partition_name = const_cast<ObString &>(part_names.at(i));
        //here just conver partition_name to its lowercase
        ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, partition_name);
        ObPartGetter part_getter(*table_schema);
        if (OB_FAIL(part_getter.get_part_ids(partition_name, partition_ids))) {
          LOG_WARN("fail to get part ids", K(ret), K(partition_name));
          if (OB_UNKNOWN_PARTITION == ret && lib::is_mysql_mode()) {
            LOG_USER_ERROR(OB_UNKNOWN_PARTITION, partition_name.length(), partition_name.ptr(),
                          table_schema->get_table_name_str().length(),
                          table_schema->get_table_name_str().ptr());
          }
        } else if (OB_FAIL(append_array_no_dup(part_ids, partition_ids))) {
          LOG_WARN("Push partition id error", K(ret));
        }
      } // end of for
      if (OB_SUCC(ret) && OB_FAIL(ObTableLoadSchema::get_tablet_ids_by_part_ids(table_schema, part_ids, tablet_ids))) {
        LOG_WARN("fail to get tablet ids", KR(ret));
      }
    }
    return ret;
  }

  static int resolve_load_method(const ObString &load_method_str, ObDirectLoadMethod::Type &method,
                                 ObDirectLoadInsertMode::Type &insert_mode)
  {
    int ret = OB_SUCCESS;
    if (load_method_str.empty()) {
      method = ObDirectLoadMethod::FULL;
      insert_mode = ObDirectLoadInsertMode::NORMAL;
    } else {
      const ObDirectLoadHint::LoadMethod load_method =
        ObDirectLoadHint::get_load_method_value(load_method_str);
      switch (load_method) {
        case ObDirectLoadHint::FULL:
          method = ObDirectLoadMethod::FULL;
          insert_mode = ObDirectLoadInsertMode::NORMAL;
          break;
        case ObDirectLoadHint::INC:
          method = ObDirectLoadMethod::INCREMENTAL;
          insert_mode = ObDirectLoadInsertMode::NORMAL;
          break;
        case ObDirectLoadHint::INC_REPLACE:
          method = ObDirectLoadMethod::INCREMENTAL;
          insert_mode = ObDirectLoadInsertMode::INC_REPLACE;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid load method", KR(ret), K(load_method_str));
          break;
      }
    }
    return ret;
  }

private:
  ObTableLoadClientTask *client_task_;
};

/**
 * ClientTaskExectueCallback
 */

class ObTableLoadClientTask::ClientTaskExectueCallback : public ObITableLoadTaskCallback
{
public:
  ClientTaskExectueCallback(ObTableLoadClientTask *client_task) : client_task_(client_task)
  {
    client_task_->inc_ref_count();
  }
  virtual ~ClientTaskExectueCallback() { ObTableLoadClientService::revert_task(client_task_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    if (OB_UNLIKELY(OB_SUCCESS != ret_code)) {
      client_task_->set_status_error(ret_code);
    }
    OB_DELETE(ObTableLoadTask, "TLD_CTExecTask", task);
  }

private:
  ObTableLoadClientTask *client_task_;
};

/**
 * ObTableLoadClientTask
 */

ObTableLoadClientTask::ObTableLoadClientTask()
  : allocator_("TLD_ClientTask"),
    session_info_(nullptr),
    plan_ctx_(allocator_),
    exec_ctx_(allocator_),
    task_scheduler_(nullptr),
    session_count_(0),
    next_batch_id_(0),
    client_status_(ObTableLoadClientStatus::MAX_STATUS),
    error_code_(OB_SUCCESS),
    ref_count_(0),
    is_in_map_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  free_session_ctx_.sessid_ = sql::ObSQLSessionInfo::INVALID_SESSID;
}

ObTableLoadClientTask::~ObTableLoadClientTask()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  if (nullptr != session_info_) {
    ObTableLoadUtils::free_session_info(session_info_, free_session_ctx_);
    session_info_ = nullptr;
  }
  trans_ctx_.reset();
}

int ObTableLoadClientTask::init(const ObTableLoadClientTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadClientTask init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("fail to assign param", KR(ret), K(param));
    } else if (OB_FAIL(init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", KR(ret));
    } else if (OB_FAIL(init_task_scheduler())) {
      LOG_WARN("fail to init task scheduler", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadClientTask::create_session_info()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param_.get_tenant_id();
  const uint64_t user_id = param_.get_user_id();
  const uint64_t database_id = param_.get_database_id();
  const ObTenantSchema *tenant_info = nullptr;
  const ObUserInfo *user_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_NOT_NULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info not null", KR(ret));
  } else if (OB_FAIL(schema_guard_.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("get tenant info failed", K(ret));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("get user info failed", K(ret));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid user_info", K(ret), K(tenant_id), K(user_id));
  } else if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, database_id, database_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid database schema", K(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(ObTableLoadUtils::create_session_info(session_info_, free_session_ctx_))) {
    LOG_WARN("create session id failed", KR(ret));
  } else {
    ObSqlString query_str;
    ObObj timeout_val;
    timeout_val.set_int(param_.get_timeout_us());
    OX(query_str.assign_fmt("DIRECT LOAD: %.*s, task_id:%ld",
                            static_cast<int>(param_.get_table_name().length()),
                            param_.get_table_name().ptr(), param_.get_task_id()));
    OZ(session_info_->load_default_sys_variable(false /*print_info_log*/, false /*is_sys_tenant*/)); //加载默认的session参数
    OZ(session_info_->load_default_configs_in_pc());
    OX(session_info_->init_tenant(tenant_info->get_tenant_name(), tenant_id));
    OX(session_info_->set_priv_user_id(user_id));
    OX(session_info_->store_query_string(query_str.string()));
    OX(session_info_->set_user(user_info->get_user_name(), user_info->get_host_name_str(),
                               user_info->get_user_id()));
    OX(session_info_->set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT));
    OX(session_info_->set_default_database(database_schema->get_database_name(),
                                           CS_TYPE_UTF8MB4_GENERAL_CI));
    OX(session_info_->set_mysql_cmd(COM_QUERY));
    OX(session_info_->set_current_trace_id(ObCurTraceId::get_trace_id()));
    OX(session_info_->set_client_addr(param_.get_client_addr()));
    OX(session_info_->set_peer_addr(ObServer::get_instance().get_self()));
    OX(session_info_->set_query_start_time(ObTimeUtil::current_time()));
    OZ(session_info_->update_sys_variable(SYS_VAR_OB_QUERY_TIMEOUT, timeout_val));
  }
  return ret;
}

int ObTableLoadClientTask::init_exec_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadSchema::get_schema_guard(param_.get_tenant_id(), schema_guard_))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(create_session_info())) {
    LOG_WARN("fail to create session info", KR(ret));
  } else {
    sql_ctx_.schema_guard_ = &schema_guard_;
    plan_ctx_.set_timeout_timestamp(ObTimeUtil::current_time() + param_.get_timeout_us());
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    exec_ctx_.set_physical_plan_ctx(&plan_ctx_);
    exec_ctx_.set_my_session(session_info_);
    if (OB_FAIL(session_info_->set_cur_phy_plan(&plan_))) {
      LOG_WARN("fail to set cur phy plan", KR(ret));
    } else if (FALSE_IT(exec_ctx_.reference_my_plan(&plan_))) {
    } else if (OB_FAIL(exec_ctx_.init_phy_op(1))) {
      LOG_WARN("fail to init phy op", KR(ret));
    } else {
      client_exec_ctx_.exec_ctx_ = &exec_ctx_;
      client_exec_ctx_.init_heart_beat(param_.get_heartbeat_timeout_us());
    }
  }
  return ret;
}

int ObTableLoadClientTask::init_task_scheduler()
{
  int ret = OB_SUCCESS;
  const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtil::current_time() + param_.get_timeout_us());
  if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_),
                                          1 /*thread_count*/,
                                          param_.get_task_id(),
                                          "Executor",
                                          session_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->init())) {
    LOG_WARN("fail to init task scheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->start())) {
    LOG_WARN("fail to start task scheduler", KR(ret));
  }
  THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  return ret;
}

int ObTableLoadClientTask::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else if (OB_FAIL(set_status_initializing())) {
    LOG_WARN("fail to set status initializing", KR(ret));
  } else {
    ObMemAttr attr(param_.get_tenant_id(), "TLD_CTExecTask");
    ObTableLoadTask *task = nullptr;
    if (OB_ISNULL(task = OB_NEW(ObTableLoadTask, attr, param_.get_tenant_id()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTask", KR(ret));
    } else if (OB_FAIL(task->set_processor<ClientTaskExectueProcessor>(this))) {
      LOG_WARN("fail to set client task processor", KR(ret));
    } else if (OB_FAIL(task->set_callback<ClientTaskExectueCallback>(this))) {
      LOG_WARN("fail to set common task callback", KR(ret));
    } else if (OB_FAIL(task_scheduler_->add_task(0, task))) {
      LOG_WARN("fail to add task", KR(ret));
    }
    if (OB_FAIL(ret)) {
      set_status_error(ret);
      if (nullptr != task) {
        OB_DELETE(ObTableLoadTask, attr, task);
        task = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadClientTask::write(ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else if (OB_UNLIKELY(obj_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(obj_rows));
  } else if (OB_UNLIKELY(session_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session count", KR(ret), K(session_count_));
  } else {
    const int64_t batch_id = ATOMIC_FAA(&next_batch_id_, 1);
    const int32_t session_id = batch_id % session_count_ + 1;
    ObTableLoadSequenceNo start_seq_no(batch_id << ObTableLoadSequenceNo::BATCH_ID_SHIFT);
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_rows.count(); ++i) {
      ObTableLoadObjRow &row = obj_rows.at(i);
      row.seq_no_ = start_seq_no++;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(instance_.write_trans(trans_ctx_, session_id, obj_rows))) {
        LOG_WARN("fail to write trans", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadClientTask::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else {
    obsys::ObWLockGuard guard(rw_lock_);
    if (ObTableLoadClientStatus::COMMITTING == client_status_ ||
        ObTableLoadClientStatus::COMMIT == client_status_) {
      LOG_INFO("client task already commit", K(client_status_));
    } else {
      ret = advance_status_nolock(ObTableLoadClientStatus::RUNNING,
                                  ObTableLoadClientStatus::COMMITTING);
    }
  }
  return ret;
}

void ObTableLoadClientTask::abort(int error_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientTask not init", KR(ret));
  } else {
    set_status_abort(error_code);
    if (nullptr != session_info_ && OB_FAIL(session_info_->kill_query())) {
      LOG_WARN("fail to kill query", KR(ret));
    }
  }
}
void ObTableLoadClientTask::heart_beat() { client_exec_ctx_.heart_beat(); }

int ObTableLoadClientTask::check_status() { return client_exec_ctx_.check_status(); }

int ObTableLoadClientTask::advance_status_nolock(const ObTableLoadClientStatus expected,
                                                 const ObTableLoadClientStatus updated)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(client_status_ == expected)) {
    client_status_ = updated;
    LOG_INFO("LOAD DATA client status advance", K(client_status_));
  } else if (ObTableLoadClientStatus::ERROR == client_status_ ||
             ObTableLoadClientStatus::ABORT == client_status_) {
    ret = error_code_;
    LOG_WARN("client status error", KR(ret), K(client_status_), K(error_code_));
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected status", KR(ret), K(client_status_));
  }
  return ret;
}

int ObTableLoadClientTask::advance_status(const ObTableLoadClientStatus expected,
                                          const ObTableLoadClientStatus updated)
{
  obsys::ObWLockGuard guard(rw_lock_);

  return advance_status_nolock(expected, updated);
}

int ObTableLoadClientTask::set_status_initializing()
{
  return advance_status(ObTableLoadClientStatus::MAX_STATUS, ObTableLoadClientStatus::INITIALIZING);
}

int ObTableLoadClientTask::set_status_waitting()
{
  return advance_status(ObTableLoadClientStatus::INITIALIZING, ObTableLoadClientStatus::WAITTING);
}

int ObTableLoadClientTask::set_status_running()
{
  return advance_status(ObTableLoadClientStatus::WAITTING, ObTableLoadClientStatus::RUNNING);
}

int ObTableLoadClientTask::set_status_commit()
{
  return advance_status(ObTableLoadClientStatus::COMMITTING, ObTableLoadClientStatus::COMMIT);
}

int ObTableLoadClientTask::set_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(error_code));
  } else {
    obsys::ObWLockGuard guard(rw_lock_);
    if (ObTableLoadClientStatus::ERROR == client_status_ ||
        ObTableLoadClientStatus::ABORT == client_status_) {
      // ignore
    } else {
      client_status_ = ObTableLoadClientStatus::ERROR;
      error_code_ = error_code;
    }
  }
  return ret;
}

void ObTableLoadClientTask::set_status_abort(int error_code)
{
  obsys::ObWLockGuard guard(rw_lock_);
  if (ObTableLoadClientStatus::ABORT == client_status_) {
    // ignore
  } else {
    client_status_ = ObTableLoadClientStatus::ABORT;
    if (OB_SUCCESS == error_code_) {
      error_code_ = error_code;
    }
  }
}

int ObTableLoadClientTask::check_status(ObTableLoadClientStatus client_status)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(rw_lock_);
  if (OB_UNLIKELY(client_status != client_status_)) {
    if (ObTableLoadClientStatus::ERROR == client_status_ ||
        ObTableLoadClientStatus::ABORT == client_status_) {
      ret = error_code_;
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

ObTableLoadClientStatus ObTableLoadClientTask::get_status() const
{
  obsys::ObRLockGuard guard(rw_lock_);
  return client_status_;
}

int ObTableLoadClientTask::get_error_code() const
{
  obsys::ObRLockGuard guard(rw_lock_);
  return error_code_;
}

void ObTableLoadClientTask::get_status(ObTableLoadClientStatus &client_status,
                                       int &error_code) const
{
  obsys::ObRLockGuard guard(rw_lock_);
  client_status = client_status_;
  error_code = error_code_;
}

int ObTableLoadClientTask::init_instance(ObTableLoadParam &load_param,
                                         const ObIArray<uint64_t> &column_ids,
                                         const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableLoadTableCtx *tmp_ctx = nullptr;
  if (OB_FAIL(instance_.init(load_param, column_ids, tablet_ids, &client_exec_ctx_))) {
    LOG_WARN("fail to init instance", KR(ret));
  } else if (OB_FAIL(instance_.start_trans(trans_ctx_, ObTableLoadInstance::DEFAULT_SEGMENT_ID,
                                           allocator_))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_ISNULL(tmp_ctx = instance_.get_table_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table ctx", KR(ret));
  } else {
    session_count_ = tmp_ctx->param_.write_session_count_;
    tmp_ctx = nullptr;
  }
  return ret;
}

int ObTableLoadClientTask::commit_instance()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(instance_.commit_trans(trans_ctx_))) {
    LOG_WARN("fail to commit trans", KR(ret));
  } else if (OB_FAIL(instance_.commit())) {
    LOG_WARN("fail to commit instance", KR(ret));
  } else {
    result_info_ = instance_.get_result_info();
  }
  return ret;
}

void ObTableLoadClientTask::destroy_instance() { instance_.destroy(); }

} // namespace observer
} // namespace oceanbase
