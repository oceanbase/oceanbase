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
 * DDL type task parser, work thread pool
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_ddl_processor.h"

#include "ob_log_instance.h"            // TCTX
#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_schema_getter.h"       // IObLogSchemaGetter
#include "ob_log_tenant_mgr.h"          // IObLogTenantMgr
#include "ob_log_table_matcher.h"       // IObLogTableMatcher
#include "ob_log_config.h"              // TCONF

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [DDL_PROCESSOR] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [DDL_PROCESSOR] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)

#define IGNORE_SCHEMA_ERROR(ret, args...) \
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) { \
      LOG_WARN("ignore DDL on schema error, tenant may be dropped in future", ##args); \
      ret = OB_SUCCESS; \
    }

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace libobcdc
{


///////////////////////////////// ObLogDDLProcessor /////////////////////////////////

ObLogDDLProcessor::ObLogDDLProcessor() :
    is_inited_(false),
    schema_getter_(NULL),
    skip_reversed_schema_version_(false)
{}

ObLogDDLProcessor::~ObLogDDLProcessor()
{
  destroy();
}

int ObLogDDLProcessor::init(
    IObLogSchemaGetter *schema_getter,
    const bool skip_reversed_schema_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(schema_getter_ = schema_getter) && is_online_refresh_mode(TCTX.refresh_mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(schema_getter));
  } else {
    skip_reversed_schema_version_ = skip_reversed_schema_version;
    is_inited_ = true;
  }

  return ret;
}

void ObLogDDLProcessor::destroy()
{
  is_inited_ = false;
  schema_getter_ = NULL;
  skip_reversed_schema_version_ = false;
}

int ObLogDDLProcessor::handle_ddl_trans(
    PartTransTask &task,
    ObLogTenant &tenant,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! task.is_ddl_trans())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid ddl task which is not DDL trans", KR(ret), K(task));
  }
  // Iterate through all DDL statements
  else if (OB_FAIL(handle_tenant_ddl_task_(task, tenant, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_tenant_ddl_task_ fail", KR(ret), K(task), K(tenant));
    }
  } else {
  }

  return ret;
}

void ObLogDDLProcessor::mark_stmt_binlog_record_invalid_(DdlStmtTask &stmt_task)
{
  if (NULL != stmt_task.get_binlog_record()) {
    stmt_task.get_binlog_record()->set_is_valid(false);
  }
}

void ObLogDDLProcessor::mark_all_binlog_records_invalid_(PartTransTask &task)
{
  DdlStmtTask *stmt_task = static_cast<DdlStmtTask *>(task.get_stmt_list().head_);
  while (NULL != stmt_task) {
    mark_stmt_binlog_record_invalid_(*stmt_task);
    stmt_task = static_cast<DdlStmtTask *>(stmt_task->get_next());
  }
}

int ObLogDDLProcessor::get_old_schema_version_(
    const uint64_t tenant_id,
    PartTransTask &task,
    const int64_t tenant_ddl_cur_schema_version,
    int64_t &old_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t ddl_schema_version = task.get_local_schema_version();

  // 1. use tenant_ddl_cur_schema_version as old_schema_version by default
  // 2. Special case: when the schema version is reversed and skip_reversed_schema_version_ = true,
  // then to ensure that the corresponding schema is obtained, take the suitable schema version[official version] as old_schema_version
  //
  // e.g.: cur_schema_version=104, reversed ddl_schema_version=90(drop database operation),
  // then according to schema_version =88 can guarantee to get the corresponding database schema
  old_schema_version = tenant_ddl_cur_schema_version;

  if (OB_UNLIKELY(ddl_schema_version <= tenant_ddl_cur_schema_version)) {
    // In ONLINE_SCHEMA refresh mode, should ALWALYS report ERROR if schema_version is reversed
    // except OBCDC is configed with skip_reversed_schema_version = true;
    //
    // In DATA_DICT refresh mode, schema_version reversed is ignored, because tenant_schema_version is
    // updated in sys_ls_handler before ddl really handled, should not report error

    if (is_data_dict_refresh_mode(TCTX.refresh_mode_)) {
      // log WARN and use tenant latest ddl_schema_version for data_dict mode.
      LOG_WARN("DDL schema version is reversed", K(ddl_schema_version), K(tenant_ddl_cur_schema_version),
          K(task));
    } else {
      // here should be online schema mode.
      LOG_ERROR("DDL schema version is reversed", K(ddl_schema_version), K(tenant_ddl_cur_schema_version),
          K(task));

      if (skip_reversed_schema_version_) {
        if (OB_FAIL(get_schema_version_by_timestamp_util_succ_(tenant_id, ddl_schema_version, old_schema_version, stop_flag))) {
          if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
            // do nothing
          } else if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("get_schema_version_by_timestamp_util_succ_ fail", KR(ret), K(tenant_id), K(ddl_schema_version),
                K(old_schema_version));
          }
        } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == old_schema_version)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("old_schema_version is not valid", KR(ret), K(old_schema_version));
        } else {
          LOG_WARN("ignore DDL schema version is reversed, "
              "set old schema version as suitable ddl_schema_version",
              K(skip_reversed_schema_version_), K(old_schema_version),
            K(ddl_schema_version), K(tenant_ddl_cur_schema_version));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLProcessor::get_schema_version_by_timestamp_util_succ_(
    const uint64_t tenant_id,
    const int64_t ddl_schema_version,
    int64_t &old_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_getter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_getter_ is NULL", KR(ret), K(schema_getter_));
  } else {
    RETRY_FUNC(stop_flag, (*schema_getter_), get_schema_version_by_timestamp, tenant_id, ddl_schema_version -1,
        old_schema_version, DATA_OP_TIMEOUT);
  }

  return ret;
}

int ObLogDDLProcessor::filter_ddl_stmt_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    IObLogTenantMgr &tenant_mgr,
    bool &chosen,
    const bool only_filter_by_tenant /* = false */)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type =
      static_cast<ObSchemaOperationType>(ddl_stmt.get_operation_type());

  chosen = false;

  // Note that.
  // 0.1 Push up schema version number based on op_type and tenant, only_filter_by_tenant=false
  // 0.2 Filter ddl output to committer based on tenant, only_filter_by_tenant=true
  //
  // 1. Do not filter add tenant statement because new tenants may be added (located in whitelist)
  // OB_DDL_ADD_TENANT corresponds to the version before schema splitting
  // OB_DDL_ADD_TENANT_START records ddl_stmt, which corresponds to the version after schema split, and outputs only ddl_stmt_str
  // OB_DDL_ADD_TENANT_END
  // OB_DDL_FINISH_SCHEMA_SPLIT does not filter by default
  //
  // 2. do not filter tenant del tenant statements that are in the whitelist
  // OB_DDL_DEL_TENANT
  // OB_DDL_DEL_TENANT_START
  // OB_DDL_DEL_TENANT_END
  // 3. If a tenant is created after the start bit, the start moment will add all tenants at that time (located in the whitelist)
  // 4. filter outline
  if (only_filter_by_tenant) {
    if (OB_FAIL(tenant_mgr.filter_ddl_stmt(tenant.get_tenant_id(), chosen))) {
      LOG_ERROR("filter ddl stmt fail", KR(ret), K(tenant.get_tenant_id()), K(chosen));
    }
  } else {
    if (is_table_operation(op_type)
        && is_inner_object_id(ddl_stmt.get_op_table_id())) {
      // filter ddl_op for inner table.
      chosen = false;
    } else if (OB_DDL_ADD_TENANT == op_type
        || OB_DDL_ADD_TENANT_START == op_type
        || OB_DDL_ADD_TENANT_END == op_type
        || OB_DDL_FINISH_SCHEMA_SPLIT == op_type) {
      chosen = true;
    } else if (OB_DDL_DEL_TENANT == op_type
        || OB_DDL_DEL_TENANT_START == op_type
        || OB_DDL_DEL_TENANT_END == op_type) {
      chosen = true;
    } else if (OB_DDL_CREATE_OUTLINE == op_type
        || OB_DDL_REPLACE_OUTLINE == op_type
        || OB_DDL_DROP_OUTLINE == op_type
        || OB_DDL_ALTER_OUTLINE== op_type) {
      chosen = false;
    }
    // filter based on tenant that ddl belongs to
    else if (OB_FAIL(tenant_mgr.filter_ddl_stmt(tenant.get_tenant_id(), chosen))) {
      LOG_ERROR("filter ddl stmt fail", KR(ret), K(tenant.get_tenant_id()), K(chosen));
    } else {
      // succ
    }
  }

  if (OB_SUCCESS == ret && ! chosen) {
    _ISTAT("[DDL] [FILTER_DDL_STMT] TENANT_ID=%lu OP_TYPE=%s(%d) SCHEMA_VERSION=%ld "
        "SCHEMA_DELAY=%.3lf(sec) CUR_SCHEMA_VERSION=%ld OP_TABLE_ID=%ld OP_TENANT_ID=%ld "
        "EXEC_TENANT_ID=%lu OP_DB_ID=%ld OP_TG_ID=%ld DDL_STMT=[%s] ONLY_FILTER_BY_TENANT=%d",
        tenant.get_tenant_id(),
        ObSchemaOperation::type_str(op_type), op_type,
        ddl_stmt.get_op_schema_version(),
        get_delay_sec(ddl_stmt.get_op_schema_version()),
        tenant.get_schema_version(),
        ddl_stmt.get_op_table_id(),
        ddl_stmt.get_op_tenant_id(),
        ddl_stmt.get_exec_tenant_id(),
        ddl_stmt.get_op_database_id(),
        ddl_stmt.get_op_tablegroup_id(),
        to_cstring(ddl_stmt.get_ddl_stmt_str()),
        only_filter_by_tenant);
  }

  return ret;
}


int ObLogDDLProcessor::handle_tenant_ddl_task_(
    PartTransTask &task,
    ObLogTenant &tenant,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  int64_t ddl_commit_version = task.get_trans_commit_version();
  int64_t ddl_schema_version = task.get_local_schema_version();
  const int64_t checkpoint_seq = task.get_checkpoint_seq();
  int64_t old_schema_version = OB_INVALID_TIMESTAMP;
  int64_t new_schema_version = ddl_schema_version;    // Adopt ddl schema version as new schema version
  const uint64_t ddl_tenant_id = tenant.get_tenant_id();  // The tenant ID to which the DDL belongs
  const int64_t start_schema_version = tenant.get_start_schema_version();
  const int64_t tenant_ddl_cur_schema_version = tenant.get_schema_version();

  _ISTAT("[DDL] [HANDLE_TRANS] TENANT_ID=%ld STMT_COUNT=%ld CHECKPOINT_SEQ=%ld "
      "SCHEMA_VERSION=%ld CUR_SCHEMA_VERSION=%ld LOG_DELAY=%.3lf(sec) SCHEMA_DELAY=%.3lf(sec)",
      ddl_tenant_id, task.get_stmt_num(), checkpoint_seq, ddl_schema_version,
      tenant_ddl_cur_schema_version, get_delay_sec(ddl_commit_version),
      get_delay_sec(ddl_schema_version));

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("tenant_mgr is NULL", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  }
  // Ignore DDL operations that are smaller than the start Schema version
  else if (OB_UNLIKELY(ddl_schema_version <= start_schema_version)) {
    LOG_WARN("ignore DDL task whose schema version is not greater than start schema version",
        K(ddl_schema_version), K(start_schema_version), K(task));
    // Mark all binlog records as invalid
    mark_all_binlog_records_invalid_(task);
  }
  // Calculate the old_schema_version
  else if (OB_FAIL(get_old_schema_version_(ddl_tenant_id, task, tenant_ddl_cur_schema_version, old_schema_version, stop_flag))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Tenant does not exist, or schema fetching failure, ignore this DDL statement
      LOG_WARN("get old schema version fail, tenant may be dropped, ignore",
          KR(ret), K(ddl_tenant_id), K(task), K(tenant_ddl_cur_schema_version), K(old_schema_version));
      // Set all records to be invalid
      mark_all_binlog_records_invalid_(task);
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_old_schema_version_ fail", KR(ret), K(ddl_tenant_id), K(task), K(tenant_ddl_cur_schema_version),
          K(old_schema_version));
    }
  } else {
    // Iterate through each statement of the DDL
    IStmtTask *stmt_task = task.get_stmt_list().head_;
    bool only_filter_by_tenant = true;
    while (NULL != stmt_task && OB_SUCCESS == ret) {
      bool stmt_is_chosen = false;
      DdlStmtTask *ddl_stmt = dynamic_cast<DdlStmtTask *>(stmt_task);

      if (OB_UNLIKELY(! stmt_task->is_ddl_stmt()) || OB_ISNULL(ddl_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid DDL statement", KR(ret), KPC(stmt_task), K(ddl_stmt));
      }
      // filter ddl stmt
      else if (OB_FAIL(filter_ddl_stmt_(tenant, *ddl_stmt, *tenant_mgr, stmt_is_chosen))) {
        LOG_ERROR("filter_ddl_stmt_ fail", KR(ret), KPC(ddl_stmt), K(tenant), K(stmt_is_chosen));
      } else if (! stmt_is_chosen) {
        // If the DDL statement is filtered, mark the binlog record as invalid
        mark_stmt_binlog_record_invalid_(*ddl_stmt);
      } else {
        // statements are not filtered, processing DDL statements
        if (OB_FAIL(handle_ddl_stmt_(tenant, task, *ddl_stmt, old_schema_version, new_schema_version, stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle_ddl_stmt_ fail", KR(ret), K(tenant), K(task), K(ddl_stmt),
                K(old_schema_version), K(new_schema_version));
          }
        }
        // The first filter_ddl_stmt_() will let go of some of the DDLs of the non-service tenants, and here it should be filtered again based on the tenant ID
        // Ensure that only the DDLs of whitelisted tenants are output
        else if (OB_FAIL(filter_ddl_stmt_(tenant, *ddl_stmt, *tenant_mgr, stmt_is_chosen, only_filter_by_tenant))) {
          LOG_ERROR("filter_ddl_stmt fail", KR(ret), KPC(ddl_stmt), K(tenant), K(stmt_is_chosen));
        } else if (! stmt_is_chosen) {
          // If the DDL statement is filtered, mark the binlog record as invalid
          mark_stmt_binlog_record_invalid_(*ddl_stmt);
        }
      }

      if (OB_SUCCESS == ret) {
        stmt_task = stmt_task->get_next();
      }
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_(
    ObLogTenant &tenant,
    PartTransTask &task,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type = (ObSchemaOperationType)ddl_stmt.get_operation_type();
  const int64_t checkpoint_seq = ddl_stmt.get_host().get_checkpoint_seq();

  _ISTAT("[DDL] [HANDLE_STMT] TENANT_ID=%lu OP_TYPE=%s(%d) OP_TABLE_ID=%ld SCHEMA_VERSION=%ld "
      "SCHEMA_DELAY=%.3lf(sec) CUR_SCHEMA_VERSION=%ld EXEC_TENANT_ID=%ld OP_TENANT_ID=%ld "
      "OP_TABLE_ID=%ld OP_DB_ID=%ld OP_TG_ID=%ld DDL_STMT=[%s] CHECKPOINT_SEQ=%ld TRANS_ID=%ld",
      tenant.get_tenant_id(), ObSchemaOperation::type_str(op_type), op_type,
      ddl_stmt.get_op_table_id(),
      ddl_stmt.get_op_schema_version(),
      get_delay_sec(ddl_stmt.get_op_schema_version()),
      tenant.get_schema_version(),
      ddl_stmt.get_exec_tenant_id(),
      ddl_stmt.get_op_tenant_id(),
      ddl_stmt.get_op_table_id(),
      ddl_stmt.get_op_database_id(),
      ddl_stmt.get_op_tablegroup_id(),
      to_cstring(ddl_stmt.get_ddl_stmt_str()),
      checkpoint_seq,
      task.get_trans_id().get_id());

  switch (op_type) {
    case OB_DDL_DROP_TABLE : {
      ret = handle_ddl_stmt_drop_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_ALTER_TABLE : {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "alter_table", stop_flag);
      break;
    }
    case OB_DDL_CREATE_TABLE : {
      ret = handle_ddl_stmt_create_table_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_TABLE_RENAME : {
      ret = handle_ddl_stmt_rename_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_TRUNCATE_TABLE_DROP : {
      ret = handle_ddl_stmt_truncate_table_drop_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_truncate_drop_table_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version);
      break;
    }
    case OB_DDL_TRUNCATE_TABLE_CREATE : {
      ret = handle_ddl_stmt_truncate_table_create_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_TABLE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_table_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_ADD_TENANT : {
      ret = handle_ddl_stmt_add_tenant_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DEL_TENANT : {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version,
          false/*is_del_tenant_start_op*/, stop_flag);
      break;
    }
    case OB_DDL_ALTER_TENANT : {
      ret = handle_ddl_stmt_alter_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_ADD_TENANT_START : {
      ret = handle_ddl_stmt_add_tenant_start_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_ADD_TENANT_END: {
      ret = handle_ddl_stmt_add_tenant_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DEL_TENANT_START: {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version,
          true/*is_del_tenant_start_op*/, stop_flag);
      break;
    }
    case OB_DDL_DEL_TENANT_END: {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version,
          false/*is_del_tenant_start_op*/, stop_flag);
      break;
    }
    case OB_DDL_RENAME_TENANT: {
      ret = handle_ddl_stmt_rename_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_TENANT_TO_RECYCLEBIN: {
      ret = handle_ddl_stmt_drop_tenant_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_ALTER_DATABASE : {
      ret = handle_ddl_stmt_alter_database_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DEL_DATABASE : {
      ret = handle_ddl_stmt_drop_database_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_RENAME_DATABASE : {
      ret = handle_ddl_stmt_rename_database_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_DATABASE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_database_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version, stop_flag);
      break;
    }
    /* TODO Won't Handle INDEX OP currently.
    case OB_DDL_CREATE_GLOBAL_INDEX: {
      // add global index
      ret = handle_ddl_stmt_create_index_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_GLOBAL_INDEX: {
      // delete global index
      ret = handle_ddl_stmt_drop_index_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_CREATE_INDEX: {
      // add unique index to TableIDCache
      ret = handle_ddl_stmt_create_index_(tenant, ddl_stmt, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_INDEX : {
      // delete unique index from TableIDCache
      ret = handle_ddl_stmt_drop_index_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    case OB_DDL_DROP_INDEX_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_index_to_recyclebin_(tenant, ddl_stmt, old_schema_version, new_schema_version, stop_flag);
      break;
    }
    */

    default: {
      // Other DDL types, by default, are output directly and not processed
      // new version of schema parsing is used by default
      ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, new_schema_version, stop_flag, true);
      break;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle ddl statement fail", KR(ret), K(op_type), K(ddl_stmt));
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_direct_output_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    volatile bool &stop_flag,
    const bool format_with_new_schema /* = false */)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type =
      static_cast<ObSchemaOperationType>(ddl_stmt.get_operation_type());
  _ISTAT("[DDL] [HANDLE_STMT] [DIRECT_OUTPUT] TENANT_ID=%ld OP_TYPE=%s(%d) DDL_STMT=[%s]",
      tenant.get_tenant_id(), ObSchemaOperation::type_str(op_type), op_type,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, schema_version, stop_flag, NULL, NULL, format_with_new_schema))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(schema_version));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogDDLProcessor::commit_ddl_stmt_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    volatile bool &stop_flag,
    const char *tenant_name /* = NULL */,
    const char *db_name /* = NULL */,
    const bool format_with_new_schema /* = false */,
    const bool filter_ddl_stmt /* = false */)
{
  int ret = OB_SUCCESS;
  ObLogBR *br = ddl_stmt.get_binlog_record();
  IBinlogRecord *br_data = NULL;
  ObSchemaOperationType op_type = (ObSchemaOperationType)ddl_stmt.get_operation_type();
  const char *op_type_str = ObSchemaOperation::type_str(op_type);
  /// Need to get schema when the tenant name is empty
  /// Allow DB name to be empty
  bool need_get_schema = (NULL == tenant_name);

  // The tenant to which this DDL statement belongs is the one that follows
  uint64_t ddl_tenant_id = tenant.get_tenant_id();
  const int64_t checkpoint_seq = ddl_stmt.get_host().get_checkpoint_seq();

  if (ddl_stmt.get_ddl_stmt_str().empty()) {
    // Ignore empty DDL statements
    ISTAT("[DDL] [FILTER_DDL_STMT] ignore empty DDL",
        "schema_version", ddl_stmt.get_op_schema_version(), K(op_type_str),
        K(ddl_tenant_id),
        "op_tenant_id", ddl_stmt.get_op_tenant_id(),
        "exec_tenant_id", ddl_stmt.get_exec_tenant_id(),
        "op_database_id", ddl_stmt.get_op_database_id(),
        "op_table_id", ddl_stmt.get_op_table_id());

    // Set binlog record invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  } else if (filter_ddl_stmt) {
    // 过滤指定的DDL语句
    ISTAT("[DDL] [FILTER_DDL_STMT] ignore DDL",
        "schema_version", ddl_stmt.get_op_schema_version(), K(op_type_str),
        K(ddl_tenant_id),
        "op_tenant_id", ddl_stmt.get_op_tenant_id(),
        "exec_tenant_id", ddl_stmt.get_exec_tenant_id(),
        "op_database_id", ddl_stmt.get_op_database_id(),
        "op_table_id", ddl_stmt.get_op_table_id(),
        "ddl_stmt_str", ddl_stmt.get_ddl_stmt_str());

    // Set binlog record invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  } else if (OB_ISNULL(br) || OB_ISNULL(br_data = br->get_data())) {
    LOG_ERROR("invalid binlog record", K(br), K(br_data), K(ddl_stmt));
    ret = OB_ERR_UNEXPECTED;
  }
  // get tenant schmea and db schema
  else if (need_get_schema
      && OB_FAIL(get_schemas_for_ddl_stmt_(
          ddl_tenant_id,
          ddl_stmt,
          schema_version,
          format_with_new_schema,
          tenant_name,
          db_name,
          stop_flag))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Tenant does not exist, or schema fetching failure, ignore this DDL statement
      LOG_WARN("get schemas for ddl stmt fail, tenant may be dropped, ignore DDL statement",
          KR(ret), K(ddl_tenant_id), K(schema_version), K(format_with_new_schema), K(ddl_stmt));
      // Set all binlog record invalid
      mark_stmt_binlog_record_invalid_(ddl_stmt);
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      if (OB_ENTRY_NOT_EXIST == ret && is_data_dict_refresh_mode(TCTX.refresh_mode_)) {
        LOG_INFO("ignore binlog_record which dict_meta may not exist", KR(ret), K(ddl_stmt));
        mark_stmt_binlog_record_invalid_(ddl_stmt);
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("get_schemas_for_ddl_stmt_ fail", KR(ret), K(ddl_stmt), K(schema_version),
            K(format_with_new_schema), K(ddl_tenant_id));
      }
    }
  }
  // set db name for binlog record
  else if (OB_FAIL(set_binlog_record_db_name_(*br_data, op_type, tenant_name, db_name))) {
    LOG_ERROR("set_binlog_record_db_name_ fail", KR(ret), K(op_type), K(tenant_name), K(db_name));
  } else {
    // handle done
    _ISTAT("[DDL] [HANDLE_DONE] TENANT_ID=%lu DB_NAME=%s OP_TYPE=%s(%d) SCHEMA_VERSION=%ld "
        "OP_TENANT_ID=%lu EXEC_TENANT_ID=%lu OP_DB_ID=%lu OP_TABLE_ID=%lu DDL_STMT=[%s] CHECKPOINT_SEQ=%ld",
        ddl_tenant_id, br_data->dbname(), op_type_str, op_type, ddl_stmt.get_op_schema_version(),
        ddl_stmt.get_op_tenant_id(), ddl_stmt.get_exec_tenant_id(), ddl_stmt.get_op_database_id(),
        ddl_stmt.get_op_table_id(),
        to_cstring(ddl_stmt.get_ddl_stmt_str()),
        checkpoint_seq);
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLProcessor::get_lazy_schema_guard_(
    const uint64_t tenant_id,
    const int64_t version,
    ObLogSchemaGuard &schema_guard,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_getter_)) {
    LOG_ERROR("schema getter is invalid", K(schema_getter_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(version < 0)) {
    LOG_ERROR("invalid version", K(version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    RETRY_FUNC(stop_flag, (*schema_getter_), get_lazy_schema_guard, tenant_id, version,
        DATA_OP_TIMEOUT, schema_guard);
  }

  return ret;
}

// The database_id determines the DDL BinlogRcord output database_name, database_id determines the policy:
// 1. when using new schema, directly use the database id that comes with DDL stmt
// 2. When using old schema, if the table id is invalid, use the database_id in DDL directly; otherwise,
// refresh the table_schema based on the table_id, and then determine the databse id according to the table schema.
// In some cases, the database ids in the old and new schema are not the same, for example,
// if you drop a table to the recycle bin, the DDL database id is "__recyclebin" database id, not the original database id
//
//
// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLProcessor::decide_ddl_stmt_database_id_(
    const uint64_t tenant_id,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    ObLogSchemaGuard &schema_guard,
    uint64_t &db_id,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const bool is_use_new_schema_version_mode = is_use_new_schema_version_(ddl_stmt, schema_version);

  if (is_use_new_schema_version_mode) {
    db_id = ddl_stmt.get_op_database_id();
  } else {
    uint64_t table_id = ddl_stmt.get_op_table_id();

    // If the table id is invalid, the db_id is used in the DDL.
    if (OB_INVALID_ID == table_id || 0 == table_id) {
      db_id = ddl_stmt.get_op_database_id();
    } else {
      const ObSimpleTableSchemaV2 *tb_schema = NULL;

      // Retry to get the table schema until it succeeds or exit
      RETRY_FUNC(stop_flag, schema_guard, get_table_schema, tenant_id, table_id, tb_schema, DATA_OP_TIMEOUT);

      if (OB_FAIL(ret)) {
        if (OB_IN_STOP_STATE != ret) {
          // OB_TENANT_HAS_BEEN_DROPPED means tenant has been droped, dealed by caller
          LOG_ERROR("get_table_schema fail", KR(ret), K(table_id), K(schema_version), K(ddl_stmt));
        }
      }
      // If the schema of the table is empty, the database id is invalid
      else if (NULL == tb_schema) {
        LOG_WARN("table schema is NULL. set database name NULL",
            K(table_id), K(schema_version), "ddl_stmt", ddl_stmt.get_ddl_stmt_str());

        db_id = OB_INVALID_ID;
      } else {
        db_id = tb_schema->get_database_id();
      }
    }
  }

  return ret;
}

int ObLogDDLProcessor::decide_ddl_stmt_database_id_with_data_dict_(
    const uint64_t tenant_id,
    const bool use_new_schema,
    DdlStmtTask &ddl_stmt,
    uint64_t &db_id)
{
  int ret = OB_SUCCESS;
  db_id = OB_INVALID_ID;

  if (use_new_schema) {
    db_id = ddl_stmt.get_op_database_id();
  } else {
    uint64_t table_id = ddl_stmt.get_op_table_id();

    // If the table id is invalid, the db_id is used in the DDL.
    if (OB_INVALID_ID == table_id || 0 == table_id) {
      db_id = ddl_stmt.get_op_database_id();
    } else {
      ObDictTenantInfoGuard dict_tenant_info_guard;
      ObDictTenantInfo *tenant_info = nullptr;
      datadict::ObDictTableMeta *table_meta = nullptr;

      if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
          tenant_id,
          dict_tenant_info_guard))) {
        LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tenant_info is nullptr", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_info->get_table_meta(table_id, table_meta))) {
        LOG_ERROR("get_table_meta failed", KR(ret), K(tenant_id), K(table_id), KPC(tenant_info));
      } else {
        db_id = table_meta->get_database_id();
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLProcessor::get_schemas_for_ddl_stmt_(
    const uint64_t ddl_tenant_id,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    const bool format_with_new_schema,
    const char *&tenant_name,
    const char *&db_name,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  TenantSchemaInfo tenant_schema_info;
  DBSchemaInfo db_schema_info;

  if (is_online_refresh_mode(TCTX.refresh_mode_)) {
    if (OB_FAIL(get_schema_info_with_online_schema_(
        ddl_tenant_id,
        schema_version,
        ddl_stmt,
        tenant_schema_info,
        db_schema_info,
        stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get_schema_info_with_online_schema failed", KR(ret), K(ddl_tenant_id), K(schema_version), K(ddl_stmt));
      }
    }
  } else if (OB_FAIL(get_schema_info_with_data_dict_(
      ddl_tenant_id,
      format_with_new_schema,
      ddl_stmt,
      tenant_schema_info,
      db_schema_info))) {
    LOG_ERROR("get_schema_info_with_data_dict failed", KR(ret), K(ddl_tenant_id), K(schema_version), K(format_with_new_schema), K(ddl_stmt));
  }

  if (OB_SUCC(ret)) {
    // set tenant name and db name, NOTICE: tenant_name and db_name may be NULL.
    tenant_name = tenant_schema_info.name_;
    db_name = db_schema_info.name_;

    if (ddl_tenant_id != ddl_stmt.get_op_tenant_id()) {
      LOG_INFO("[DDL] [NOTICE] DDL stmt belong to different tenant with operated tenant",
          K(ddl_tenant_id), K(ddl_stmt));
    }
  }

  return ret;
}

int ObLogDDLProcessor::get_schema_info_with_online_schema_(
    const uint64_t ddl_tenant_id,
    const int64_t schema_version,
    DdlStmtTask &ddl_stmt,
    TenantSchemaInfo &tenant_schema_info,
    DBSchemaInfo &db_schema_info,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  tenant_schema_info.reset();
  db_schema_info.reset();
  uint64_t db_id = OB_INVALID_ID;
  ObLogSchemaGuard schema_guard;

  // Get schema guard based on tenant_id and version number
  if (OB_FAIL(get_lazy_schema_guard_(ddl_tenant_id, schema_version, schema_guard, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
      LOG_WARN("get_lazy_schema_guard_ fail", KR(ret), K(ddl_tenant_id), K(schema_version));
    }
  }
  // decide database id
  else if (OB_FAIL(decide_ddl_stmt_database_id_(ddl_tenant_id, ddl_stmt, schema_version, schema_guard, db_id, stop_flag))) {
    // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
    if (OB_IN_STOP_STATE != ret) {
      LOG_WARN("decide_ddl_stmt_database_id_ fail", KR(ret), K(ddl_tenant_id), K(ddl_stmt), K(schema_version));
    }
  } else {
    // Retry to get tenant schema until success or exit
    RETRY_FUNC(stop_flag, schema_guard, get_tenant_schema_info, ddl_tenant_id, tenant_schema_info,
        DATA_OP_TIMEOUT);

    if (OB_FAIL(ret)) {
      if (OB_IN_STOP_STATE != ret) {
        // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
        LOG_WARN("get_tenant_schema_info fail", KR(ret), K(ddl_tenant_id), K(tenant_schema_info),
            K(schema_version), K(ddl_stmt));
      }
    } else {
      // FIXME: Currently there are two invalid values: 0 and OB_INVALID_ID, it is recommended that the observer is unified
      if (OB_INVALID_ID != db_id && 0 != db_id) {
        // Retry to get the database schema until it succeeds or exit
        RETRY_FUNC(stop_flag, schema_guard, get_database_schema_info, ddl_tenant_id, db_id, db_schema_info,
            DATA_OP_TIMEOUT);

        if (OB_FAIL(ret)) {
          if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
            // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
            // DB does not exist and is considered normal
            LOG_WARN("get database schema fail, set database name NULL", KR(ret),
                K(tenant_schema_info), K(db_id), K(schema_version),
                "ddl_stmt", ddl_stmt.get_ddl_stmt_str());
            ret = OB_SUCCESS;
          } else if (OB_IN_STOP_STATE != ret) {
            LOG_WARN("get_database_schema_info fail", KR(ret), K(db_id), K(schema_version));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogDDLProcessor::get_schema_info_with_data_dict_(
    const uint64_t ddl_tenant_id,
    const bool format_with_new_schema,
    DdlStmtTask &ddl_stmt,
    TenantSchemaInfo &tenant_schema_info,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;
  tenant_schema_info.reset();
  db_schema_info.reset();
  uint64_t db_id = OB_INVALID_ID;

  if (OB_FAIL(decide_ddl_stmt_database_id_with_data_dict_(ddl_tenant_id, format_with_new_schema, ddl_stmt, db_id))) {
    LOG_ERROR("decide_ddl_stmt_database_id_with_data_dict_ failed", KR(ret), K(ddl_tenant_id), K(db_id), K(ddl_stmt));
  } else if (format_with_new_schema) {
    if (OB_FAIL(ddl_stmt.get_host().get_tenant_schema_info_with_inc_dict(ddl_tenant_id, tenant_schema_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("tenant_schema_info not found, may skip by invoker", KR(ret), K(ddl_tenant_id), K(format_with_new_schema));
      } else {
        LOG_ERROR("get_tenant_schema_info_with_inc_dict failed", KR(ret), K(ddl_tenant_id), K(format_with_new_schema));
      }
    } else if (OB_INVALID_ID != db_id && 0 != db_id) {
      if (OB_FAIL(ddl_stmt.get_host().get_database_schema_info_with_inc_dict(ddl_tenant_id, db_id, db_schema_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_DEBUG("get_database_schema_info_with_inc_dict failed, may skip by invoker", KR(ret), K(ddl_tenant_id), K(db_id));
        } else {
          LOG_ERROR("get_database_schema_info_with_inc_dict failed", KR(ret), K(ddl_tenant_id), K(db_id));
        }
      }
    }
  } else {
    ObDictTenantInfoGuard dict_tenant_info_guard;
    ObDictTenantInfo *tenant_info = nullptr;

    if (OB_FAIL(GLOGMETADATASERVICE.get_tenant_info_guard(
        ddl_tenant_id,
        dict_tenant_info_guard))) {
      LOG_ERROR("get_tenant_info_guard failed", KR(ret), K(ddl_tenant_id));
    } else if (OB_ISNULL(tenant_info = dict_tenant_info_guard.get_tenant_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_info is nullptr", KR(ret), K(ddl_tenant_id));
    } else if (OB_FAIL(tenant_info->get_tenant_schema_info(tenant_schema_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("tenant_schema_info not found, may skip by invoker", KR(ret), K(ddl_tenant_id), K(format_with_new_schema));
      } else {
        LOG_ERROR("get_tenant_schema_info failed", KR(ret), K(ddl_tenant_id), K(format_with_new_schema));
      }
    } else if (OB_INVALID_ID != db_id && 0 != db_id) {
      if (OB_FAIL(tenant_info->get_database_schema_info(db_id, db_schema_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_DEBUG("get_database_schema_info failed, may skip by invoker", KR(ret), K(ddl_tenant_id), K(db_id), K(format_with_new_schema));
        } else {
          LOG_ERROR("get_database_schema_info failed", KR(ret), K(ddl_tenant_id), K(db_id), K(format_with_new_schema));
        }
      }
    }
  }

  LOG_DEBUG("get_schema_info_with_data_dict_ for ddl_stmt done", KR(ret),
      K(ddl_tenant_id), K(tenant_schema_info), K(db_id), K(db_schema_info), K(ddl_stmt));

  return ret;
}

bool ObLogDDLProcessor::is_use_new_schema_version_(DdlStmtTask &ddl_stmt,
    const int64_t schema_version)
{
  const int64_t part_local_ddl_schema_version = ddl_stmt.get_host().get_local_schema_version();

  return schema_version == part_local_ddl_schema_version;
}

int ObLogDDLProcessor::set_binlog_record_db_name_(IBinlogRecord &br_data,
    const int64_t ddl_operation_type,
    const char * const tenant_name,
    const char * const db_name)
{
  int ret = OB_SUCCESS;

  // allow db_name empty
  if (OB_ISNULL(tenant_name)) {
    LOG_ERROR("invalid argument", K(tenant_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSchemaOperationType op_type =
        static_cast<ObSchemaOperationType>(ddl_operation_type);

    // If a DDL only operates on a tenant, the database is invalid
    std::string db_name_str = tenant_name;
    // For create database DDL statement, IBinlogRecord db information only records tenant information, no database information is recorded.
    if (NULL != db_name && OB_DDL_ADD_DATABASE != op_type && OB_DDL_FLASHBACK_DATABASE != op_type) {
      db_name_str.append(".");
      db_name_str.append(db_name);
    }

    br_data.setDbname(db_name_str.c_str());
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_table_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;
  bool is_table_should_ignore_in_committer = false;

  /*
   * TODO Support
  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), drop_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      is_table_should_ignore_in_committer,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);
   */

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", old_schema_version, K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // Delete table using old_schema_version parsing
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, stop_flag, tenant_name, db_name, false,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(ddl_stmt), K(tenant), K(old_schema_version),
            K(tenant_name), K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_table_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // TODO: After the Table is put into the Recycle Bin, modify the places related to the Table name in PartMgr; in addition, support displaying the table into different states
  UNUSED(new_schema_version);

  // Parsing with older schema versions
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_alter_table_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    const char *event,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  ObLogSchemaGuard new_schema_guard;
  const char *old_tenant_name = NULL;
  const char *old_db_name = NULL;

  // TODO：Support table renaming, refiltering  based on filtering rules
  int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      trans_commit_version);

  // Atopt new_schema_version
  // RETRY_FUNC(stop_flag, tenant.get_part_mgr(), alter_table,
  //     ddl_stmt.get_op_table_id(),
  //     old_schema_version,
  //     new_schema_version,
  //     start_serve_timestamp,
  //     old_schema_guard,
  //     new_schema_guard,
  //     old_tenant_name,
  //     old_db_name,
  //     event,
  //     DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt));

  if (OB_SUCC(ret)) {
    // Set tenant, database and table name with old schema
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, stop_flag, old_tenant_name, old_db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(old_schema_version),
            K(old_tenant_name), K(old_db_name));
      }
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_create_table_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool is_create_table = true;
  bool is_table_should_ignore_in_committer = false;
  int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      trans_commit_version);
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  // TODO support
  /*
  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), add_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      is_create_table,
      is_table_should_ignore_in_committer,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);
      */

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, stop_flag, tenant_name, db_name, true,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_rename_table_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // TODO：support table rename
  UNUSED(new_schema_version);

  // Parsing with older schema versions
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_create_index_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      trans_commit_version);
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), add_index_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, stop_flag, tenant_name, db_name, true))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_index_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), drop_index_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(old_schema_version), K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    // drop index uses old_schema_version parsing
    // drop index DDL, __all_ddl_operation table_id is the table_id of the index table, in order to ensure
    // that the table_schema is available, use the old_schema version to ensure that the database information is available in BinlogRecord
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, stop_flag, tenant_name, db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name));
      }
    } else {
      // succ
    }
  }

  return OB_SUCCESS;
}

int ObLogDDLProcessor::handle_ddl_stmt_truncate_table_drop_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;
  bool is_table_should_ignore_in_committer = false;

  _ISTAT("[DDL] [TRUNCATE_DROP] TENANT_ID=%lu TABLE_ID=%ld SCHEMA_VERSION=(OLD=%ld,NEW=%ld) DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  // TODO Support
  /*
  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), drop_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      is_table_should_ignore_in_committer,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);
  */

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(old_schema_version), K(new_schema_version), K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // TRUNCATE DROP operation don't need output
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_truncate_drop_table_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  _ISTAT("[DDL] [TRUNCATE_DROP_TABLE_TO_RECYCLEBIN] TENANT_ID=%lu TABLE_ID=%ld "
      "SCHEMA_VERSION=(OLD=%ld,NEW=%ld) DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  if (OB_SUCC(ret)) {
    // OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN operation does not need to output DDL
    // Set binlog record to be invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_truncate_table_create_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool is_create_table = true;
  bool is_table_should_ignore_in_committer = false;
  int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      trans_commit_version);

  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  _ISTAT("[DDL] [TRUNCATE_CREATE] TENANT_ID=%lu TABLE_ID=%ld SCHEMA_VERSION=%ld START_TSTAMP=%ld DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  // TODO Support
  /*
  RETRY_FUNC(stop_flag, tenant.get_part_mgr(), add_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      is_create_table,
      is_table_should_ignore_in_committer,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);
  */

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(start_serve_tstamp), K(is_create_table), K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // Adopt new version of schema parsing
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, stop_flag, tenant_name, db_name, true,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_index_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  // Use old version schema parsing
  // Ensure that the binlog record DB information is correct
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_add_tenant_start_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  const uint64_t op_tenant_id = ddl_stmt.get_op_tenant_id();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_ISNULL(tenant_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_mgr", KR(ret));
  } else if (is_user_tenant(op_tenant_id)
      && OB_FAIL(tenant_mgr->regist_add_tenant_start_ddl_info(op_tenant_id, new_schema_version, trans_commit_version))) {
    LOG_ERROR("regist_add_tenant_start_ddl_info failed", KR(ret), K(op_tenant_id), K(new_schema_version), K(trans_commit_version));
  }

  if (OB_SUCC(ret)) {
    ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, new_schema_version, stop_flag, true);
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_add_tenant_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool tenant_is_chosen = false;
  bool is_new_created_tenant = true;
  bool is_new_tenant_by_restore = false;
  uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      trans_commit_version);
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  int64_t valid_schema_version = new_schema_version;

  if (OB_ISNULL(tenant_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant mgr", KR(ret), K(tenant_mgr));
  } else if (OB_FAIL(parse_tenant_ddl_stmt_for_restore_(ddl_stmt, valid_schema_version, start_serve_tstamp, is_new_tenant_by_restore))) {
    LOG_ERROR("parse_tenant_ddl_stmt_for_restore_ failed", KR(ret), K(ddl_stmt), K(valid_schema_version), K(start_serve_tstamp), K(is_new_tenant_by_restore));
  } else {
    RETRY_FUNC(stop_flag, (*tenant_mgr), add_tenant,
        target_tenant_id,
        is_new_created_tenant,
        is_new_tenant_by_restore,
        start_serve_tstamp,
        valid_schema_version,
        schema_guard,
        tenant_name,
        DATA_OP_TIMEOUT,
        tenant_is_chosen);

    // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
    // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(valid_schema_version), K(start_serve_tstamp),
        K(target_tenant_id), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      bool filter_ddl_stmt = false;
      // Filter tenants that are not on the whitelist
      if (! tenant_is_chosen) {
        filter_ddl_stmt = true;
      }

      // DB name is empty
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, stop_flag, tenant_name, NULL, true, filter_ddl_stmt))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(filter_ddl_stmt),
              K(tenant_name));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_tenant_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    const bool is_del_tenant_start_op,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  const int64_t trans_commit_version = ddl_stmt.get_host().get_trans_commit_version();

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ISTAT("[DDL] begin to handle drop tenant DDL stmt",
        K(is_del_tenant_start_op), K(ddl_stmt), K(old_schema_version), K(new_schema_version));

    // In split mode, it is triggered by DDL OFFLINE Task, see handle_ddl_offline_task_() for details
    if (is_del_tenant_start_op) {
      // DROP TENANT START for split mode, marking the start of tenant deletion
      ret = tenant_mgr->drop_tenant_start(target_tenant_id, trans_commit_version);
    } else {
      // DROP TENANT END for schema split mode, marking the end of tenant deletion
      ret = tenant_mgr->drop_tenant_end(target_tenant_id, trans_commit_version);
    }

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(target_tenant_id), K(old_schema_version), K(new_schema_version),
        K(is_del_tenant_start_op), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_alter_tenant_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  // TODO: support for changing tenant names
  // Adopt new version of schema parsing
  // Currently OB does not support changing the tenant name, the code needs to be tested later

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

// Background: For data consumption chain, a large number of tenant split deployment method is used, i.e. for tenant tt1,
// tenant whitelist is used to start single or multiple libobcdc for synchronization
// When the tenant name changes, it will cause the tenant whitelist  expire
// Support: libobcdc processes a DDL to rename tenant, with the error OB_NOT_SUPPORTED; libobcdc consumers need to start a new instance of the new tenant
int ObLogDDLProcessor::handle_ddl_stmt_rename_tenant_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const char *tenant_name = NULL;
  bool tenant_is_chosen = false;
  const uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    RETRY_FUNC(stop_flag, (*tenant_mgr), alter_tenant_name,
        target_tenant_id,
        old_schema_version,
        new_schema_version,
        DATA_OP_TIMEOUT,
        tenant_name,
        tenant_is_chosen);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(target_tenant_id), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      bool filter_ddl_stmt = false;
      // Filter tenants that are not on the whitelist
      if (! tenant_is_chosen) {
        filter_ddl_stmt = true;
      }

      // DB name is empty
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, stop_flag, tenant_name, NULL, true, filter_ddl_stmt))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(filter_ddl_stmt),
              K(tenant_name));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_tenant_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_alter_database_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  // TODO: support for changing database names
  // Use old version schema
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_database_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // don't need to handle drop database
  UNUSED(new_schema_version);

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_drop_database_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::handle_ddl_stmt_rename_database_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version, stop_flag);

  return ret;
}

int ObLogDDLProcessor::parse_tenant_ddl_stmt_for_restore_(
    DdlStmtTask &ddl_stmt,
    int64_t &schema_version,
    int64_t &tenant_gts_value,
    bool &is_create_tenant_by_restore_ddl)
{
  int ret = OB_SUCCESS;
  is_create_tenant_by_restore_ddl = false;
  ObString ddl_stmt_str = ddl_stmt.get_ddl_stmt_str();
  int64_t len = ddl_stmt_str.length();
  char ddl_stmt_buf[len + 1];
  MEMSET(ddl_stmt_buf, '\0', len + 1);
  MEMCPY(ddl_stmt_buf, ddl_stmt_str.ptr(), len);
  const char *pair_delimiter = "; ";
  const char *kv_delimiter = "=";
  const char *key_gts = "tenant_gts";
  const char *key_version = "schema_version";
  const char *value_gts = NULL;
  const char *value_version = NULL;

//ddl for physical backup restore contains str like: schema_version=%ld; tenant_gts=%ld
  const bool is_restore_tenant_ddl = !ddl_stmt_str.empty()
      && (NULL != strstr(ddl_stmt_buf, pair_delimiter)) && (NULL != strstr(ddl_stmt_buf, kv_delimiter))
      && (NULL != strstr(ddl_stmt_buf, key_gts)) && (NULL != strstr(ddl_stmt_buf, key_version));

  if (is_restore_tenant_ddl) {
    const bool skip_dirty_data = (TCONF.skip_dirty_data != 0);
    int64_t tenant_schema_version_from_ddl = 0;
    int64_t tenant_gts_from_ddl = 0;
    ObLogKVCollection kv_c;

    if (OB_FAIL(ret) || OB_FAIL(kv_c.init(kv_delimiter, pair_delimiter))) {
      LOG_ERROR("init key-value str fail", KR(ret), K(ddl_stmt_str));
    } else if (OB_FAIL(kv_c.deserialize(ddl_stmt_buf))) {
      LOG_ERROR("deserialize kv string fail", KR(ret), K(ddl_stmt_str));
    } else if (OB_UNLIKELY(!kv_c.is_valid())) {
      LOG_ERROR("key-value collection built by ddl_stmt is not valid", K(ddl_stmt_str), K(kv_c));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(kv_c.get_value_of_key(key_gts, value_gts))) {
      LOG_ERROR("failed to get tenant gts value", KR(ret), K(ddl_stmt_str), K(kv_c), K(key_gts));
    } else if (OB_FAIL(kv_c.get_value_of_key(key_version, value_version))) {
      LOG_ERROR("failed to get tenant gts value", KR(ret), K(ddl_stmt_str), K(kv_c), K(key_version));
    } else if (OB_FAIL(c_str_to_int(value_version, tenant_schema_version_from_ddl))) {
      LOG_ERROR("failed to get value of tenant schema version", KR(ret), K(value_version), K(tenant_schema_version_from_ddl), K(ddl_stmt));
    } else if (OB_FAIL(c_str_to_int(value_gts, tenant_gts_from_ddl))) {
      LOG_ERROR("failed to get value of tenant schema version", KR(ret), K(value_gts), K(tenant_gts_value), K(ddl_stmt));
    } else {
      is_create_tenant_by_restore_ddl = true;
      schema_version = tenant_schema_version_from_ddl > schema_version ? tenant_schema_version_from_ddl : schema_version;
      tenant_gts_value = std::max(tenant_gts_from_ddl, tenant_gts_value);
    }

    if (OB_SUCC(ret)) {
      mark_stmt_binlog_record_invalid_(ddl_stmt);
      LOG_INFO("mark create_tenant_end_ddl invalid for restore tenant", K(is_create_tenant_by_restore_ddl),
        K(schema_version), K(tenant_gts_value), K(tenant_gts_from_ddl), K(ddl_stmt));
    } else if (skip_dirty_data) {
      LOG_WARN("parse_tenant_ddl_stmt_for_restore_ fail!", KR(ret), K(is_create_tenant_by_restore_ddl), K(schema_version), K(tenant_gts_value),
          K(value_gts), K(value_version), K(ddl_stmt), K(kv_c));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("parse_tenant_ddl_stmt_for_restore_ fail!", KR(ret), K(is_create_tenant_by_restore_ddl), K(schema_version), K(tenant_gts_value),
          K(value_gts), K(value_version), K(ddl_stmt), K(kv_c));
    }
  } else {
    LOG_DEBUG("needn't parse restore info", K(ddl_stmt_str));
  }

  return ret;
}

int64_t ObLogDDLProcessor::get_start_serve_timestamp_(
    const int64_t new_schema_version,
    const int64_t commit_log_timestamp)
{
  // The table start timestamp selects the maximum value of the Schema version and prepare log timestamp of table  __all_ddl_operation
  // The purpose is to avoid heartbeat timestamp fallback
  return std::max(new_schema_version, commit_log_timestamp);
}

}
}

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
