/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ai_service/ob_ai_system_table_manager.h"
#include "share/ai_service/ob_ai_exec_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
namespace share
{

// Column name constants for __all_ai_batch_task table
const char *const ObAiTaskTableColumns::TASK_ID = "task_id";
const char *const ObAiTaskTableColumns::TENANT_ID = "tenant_id";
const char *const ObAiTaskTableColumns::MODEL_NAME = "model_name";
const char *const ObAiTaskTableColumns::COMMAND_TYPE = "command_type";
const char *const ObAiTaskTableColumns::STATUS = "status";
const char *const ObAiTaskTableColumns::REQUESTS_HANDLED = "requests_handled";
const char *const ObAiTaskTableColumns::TOTAL_REQUESTS = "total_requests";
const char *const ObAiTaskTableColumns::TASK_CREATE_TIME = "task_create_time";
const char *const ObAiTaskTableColumns::TASK_UPDATE_TIME = "task_update_time";
const char *const ObAiTaskTableColumns::BATCH_ID = "batch_id";
const char *const ObAiTaskTableColumns::REMOTE_FILE_IDS = "remote_file_ids";
const char *const ObAiTaskTableColumns::LOCAL_FILE_METADATA = "local_file_metadata";
const char *const ObAiTaskTableColumns::ERROR_DETAIL = "error_detail";
const char *const ObAiTaskTableColumns::DDL_TASK_ID = "ddl_task_id";

ObAiSystemTableManager::ObAiSystemTableManager()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      allocator_(nullptr)
{
}

ObAiSystemTableManager::~ObAiSystemTableManager()
{
  destroy();
}

int ObAiSystemTableManager::init(uint64_t tenant_id, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAiSystemTableManager already initialized", K(ret));
  } else if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

void ObAiSystemTableManager::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    allocator_ = nullptr;
  }
}

int ObAiSystemTableManager::register_task(const ObAiTaskInfo &task_info, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  const int64_t current_time = get_current_time_us_();
  LOG_INFO("[BATCH-FILE] register_task", K(task_info));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(!task_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_info", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_info.task_id_)))) {
    LOG_WARN("failed to add task_id column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TENANT_ID, task_info.tenant_id_))) {
    LOG_WARN("failed to add tenant_id column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::MODEL_NAME, ObHexEscapeSqlStr(task_info.model_name_)))) {
    LOG_WARN("failed to add model_name column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::COMMAND_TYPE, ObAiExecUtils::get_command_type_str(task_info.command_type_)))) {
    LOG_WARN("failed to add command_type column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::STATUS, static_cast<int64_t>(task_info.status_)))) {
    LOG_WARN("failed to add status column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::REQUESTS_HANDLED, task_info.requests_handled_))) {
    LOG_WARN("failed to add requests_handled column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::TOTAL_REQUESTS, task_info.total_requests_))) {
    LOG_WARN("failed to add total_requests column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_CREATE_TIME, current_time))) {
    LOG_WARN("failed to add task_create_time column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_UPDATE_TIME, current_time))) {
    LOG_WARN("failed to add task_update_time column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::BATCH_ID, ObHexEscapeSqlStr(task_info.batch_id_)))) {
    LOG_WARN("failed to add batch_id column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::REMOTE_FILE_IDS, ObHexEscapeSqlStr(task_info.remote_file_ids_)))) {
    LOG_WARN("failed to add remote_file_ids column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::LOCAL_FILE_METADATA, ObHexEscapeSqlStr(task_info.local_file_metadata_)))) {
    LOG_WARN("failed to add local_file_metadata column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::ERROR_DETAIL, ObHexEscapeSqlStr(task_info.error_detail_)))) {
    LOG_WARN("failed to add error_detail column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::DDL_TASK_ID, task_info.ddl_task_id_))) {
    LOG_WARN("failed to add ddl_task_id column", K(ret), K(task_info));
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_BATCH_TASK_TNAME, buffer))) {
    LOG_WARN("failed to splice insert sql", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id_, buffer.ptr(), affected_rows))) {
    LOG_WARN("failed to execute write", K(ret), K(buffer));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(buffer));
  } else {
    LOG_INFO("[BATCH-FILE] registered AI task", K(task_info.task_id_), K(task_info.tenant_id_));
  }
  return ret;
}

int ObAiSystemTableManager::update_task_status(const ObString &task_id,
                                               ObAiTaskStatus status,
                                               int64_t requests_handled,
                                               int error_code,
                                               const ObString &error_message,
                                               int64_t http_error_code,
                                               const ObString &remote_file_ids,
                                               common::ObISQLClient &sql_client,
                                               const ObString &batch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", K(ret));
  } else {
    const int64_t current_time = get_current_time_us_();
    ObDMLSqlSplicer sql;
    ObSqlString update_sql;
    int64_t affected_rows = 0;

    // Build error_detail JSON: {"ob_error_code":N,"model_http_code":N,"message":"..."}
    ObArenaAllocator tmp_alloc("AiSysTable", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    char *error_buf = static_cast<char*>(tmp_alloc.alloc(OB_AI_MAX_ERROR_DETAIL_LENGTH));
    if (OB_ISNULL(error_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc error_buf", K(ret));
    } else if (OB_FAIL(ObAiExecUtils::build_error_detail_json(error_code, http_error_code, error_message,
                                                              error_buf, OB_AI_MAX_ERROR_DETAIL_LENGTH))) {
      LOG_WARN("failed to build error_detail json", K(ret), K(error_code), K(http_error_code));
    } else if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_id)))) {
      LOG_WARN("failed to add task_id column", K(ret));
    } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::STATUS, static_cast<int64_t>(status)))) {
      LOG_WARN("failed to add status column", K(ret));
    } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::REQUESTS_HANDLED, requests_handled))) {
      LOG_WARN("failed to add requests_handled column", K(ret));
    } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_UPDATE_TIME, current_time))) {
      LOG_WARN("failed to add task_update_time column", K(ret));
    } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::ERROR_DETAIL, ObHexEscapeSqlStr(error_buf)))) {
      LOG_WARN("failed to add error_detail column", K(ret));
    } else if (!remote_file_ids.empty() && OB_FAIL(sql.add_column(ObAiTaskTableColumns::REMOTE_FILE_IDS, ObHexEscapeSqlStr(remote_file_ids)))) {
      LOG_WARN("failed to add remote_file_ids column", K(ret));
    } else if (!batch_id.empty() && OB_FAIL(sql.add_column(ObAiTaskTableColumns::BATCH_ID, ObHexEscapeSqlStr(batch_id)))) {
      LOG_WARN("failed to add batch_id column", K(ret));
    } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_BATCH_TASK_TNAME, update_sql))) {
      LOG_WARN("failed to splice update sql", K(ret));
    } else if (OB_FAIL(sql_client.write(tenant_id_, update_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute update", K(ret), K(update_sql));
    } else if (affected_rows == 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("task not found", K(ret), K(task_id));
    } else {
      LOG_DEBUG("[BATCH-FILE] updated task status", K(task_id), K(status), K(requests_handled),
              K(remote_file_ids));
    }
  }
  return ret;
}

int ObAiSystemTableManager::query_task_status(const ObString &task_id,
                                              ObAiTaskInfo &task_info,
                                              common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", K(ret));
  } else {
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      ObSqlString select_sql;
      task_info.reset();

      ObDMLSqlSplicer where_splicer;
      ObSqlString where_clause;
      if (OB_FAIL(where_splicer.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_id)))) {
        LOG_WARN("failed to add task_id pk column", K(ret));
      } else if (OB_FAIL(where_splicer.add_pk_column(ObAiTaskTableColumns::TENANT_ID, tenant_id_))) {
        LOG_WARN("failed to add tenant_id pk column", K(ret));
      } else if (OB_FAIL(where_splicer.splice_predicates(where_clause))) {
        LOG_WARN("failed to splice where predicates", K(ret));
      } else if (OB_FAIL(select_sql.assign_fmt(
          "SELECT model_name, command_type, status, requests_handled, total_requests, "
          "task_create_time, task_update_time, "
          "batch_id, remote_file_ids, local_file_metadata, error_detail, "
          "ddl_task_id "
          "FROM %s WHERE %s",
          OB_ALL_AI_BATCH_TASK_TNAME,
          where_clause.ptr()))) {
        LOG_WARN("failed to build select sql", K(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id_, select_sql.ptr()))) {
        LOG_WARN("failed to execute select", K(ret), K(select_sql));
      } else if (nullptr == (result = res.get_result())) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("task not found", K(ret), K(task_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not found", K(ret), K(task_id));
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        task_info.task_id_ = task_id;
        task_info.tenant_id_ = tenant_id_;

        if (OB_SUCC(ret)) {
          common::ObString model_name;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "model_name", model_name, true, true, "");
          if (OB_SUCC(ret) && OB_FAIL(copy_string_field_(model_name, task_info.model_name_))) {
            LOG_WARN("failed to copy model_name", K(ret), K(model_name));
          }
        }

        if (OB_SUCC(ret)) {
          common::ObString command_type_str;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "command_type", command_type_str, true, true, "");
          if (OB_SUCC(ret)) {
            task_info.command_type_ = ObAiExecUtils::str_to_command_type(command_type_str);
          } else {
            LOG_WARN("failed to get command_type", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          int64_t temp_int = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "status", temp_int, int64_t);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get status", K(ret));
          } else {
            task_info.status_ = static_cast<ObAiTaskStatus>(temp_int);
          }
        }

        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "requests_handled", task_info.requests_handled_, int64_t, true, true, 0);
        if (OB_FAIL(ret)) { LOG_WARN("failed to get requests_handled", K(ret)); }

        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "total_requests", task_info.total_requests_, int64_t, true, true, 0);
        if (OB_FAIL(ret)) { LOG_WARN("failed to get total_requests", K(ret)); }

        EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*result, "task_create_time", task_info.task_create_time_);
        if (OB_FAIL(ret)) { LOG_WARN("failed to get task_create_time", K(ret)); }

        EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(*result, "task_update_time", task_info.task_update_time_);
        if (OB_FAIL(ret)) { LOG_WARN("failed to get task_update_time", K(ret)); }

        if (OB_SUCC(ret)) {
          common::ObString batch_id;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "batch_id", batch_id, true, true, "");
          if (OB_SUCC(ret) && OB_FAIL(copy_string_field_(batch_id, task_info.batch_id_))) {
            LOG_WARN("failed to copy batch_id", K(ret), K(batch_id));
          }
        }

        if (OB_SUCC(ret)) {
          common::ObString remote_file_ids;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "remote_file_ids", remote_file_ids, true, true, "");
          if (OB_SUCC(ret) && OB_FAIL(copy_string_field_(remote_file_ids, task_info.remote_file_ids_))) {
            LOG_WARN("failed to copy remote_file_ids", K(ret), K(remote_file_ids));
          }
        }

        if (OB_SUCC(ret)) {
          common::ObString local_file_metadata;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "local_file_metadata", local_file_metadata, true, true, "");
          if (OB_SUCC(ret) && !local_file_metadata.empty()) {
            if (OB_FAIL(copy_string_field_(local_file_metadata, task_info.local_file_metadata_))) {
              LOG_WARN("failed to copy local_file_metadata", K(ret), K(local_file_metadata));
            }
            if (OB_SUCC(ret)) {
              task_info.deserialize_file_metadata(local_file_metadata);
            }
          }
        }

        if (OB_SUCC(ret)) {
          common::ObString error_detail;
          EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "error_detail", error_detail, true, true, "");
          if (OB_SUCC(ret) && OB_FAIL(copy_string_field_(error_detail, task_info.error_detail_))) {
            LOG_WARN("failed to copy error_detail", K(ret), K(error_detail));
          }
        }

        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "ddl_task_id", task_info.ddl_task_id_, int64_t, true, true, 0);
        if (OB_FAIL(ret)) { LOG_WARN("failed to get ddl_task_id", K(ret)); }
      }
    }
  }
  return ret;
}

int ObAiSystemTableManager::copy_string_field_(const common::ObString &src,
                                               common::ObString &dest)
{
  int ret = OB_SUCCESS;
  dest.reset();
  if (src.empty()) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", K(ret));
  } else {
    char *buf = static_cast<char *>(allocator_->alloc(src.length() + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for string field", K(ret), K(src.length()));
    } else {
      MEMCPY(buf, src.ptr(), src.length());
      buf[src.length()] = '\0';
      dest.assign_ptr(buf, src.length());
    }
  }
  return ret;
}

int ObAiSystemTableManager::update_task_file_metadata(const ObString &task_id,
                                                       const ObString &local_file_metadata,
                                                       common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", K(ret));
  } else {
    ObDMLSqlSplicer sql;
    ObSqlString update_sql;
    int64_t affected_rows = 0;
    int64_t current_time = get_current_time_us_();

    if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_id)))) {
      LOG_WARN("failed to add task_id column", K(ret));
    } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::LOCAL_FILE_METADATA, ObHexEscapeSqlStr(local_file_metadata)))) {
      LOG_WARN("failed to add local_file_metadata column", K(ret));
    } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_UPDATE_TIME, current_time))) {
      LOG_WARN("failed to add task_update_time column", K(ret));
    } else if (OB_FAIL(sql.splice_update_sql(OB_ALL_AI_BATCH_TASK_TNAME, update_sql))) {
      LOG_WARN("failed to splice update sql", K(ret));
    } else if (OB_FAIL(sql_client.write(tenant_id_, update_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to update file metadata", K(ret), K(update_sql));
    } else if (affected_rows == 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("task not found", K(ret), K(task_id));
    } else {
      LOG_INFO("[BATCH-FILE] updated task file metadata", K(task_id), K(local_file_metadata));
    }
  }
  return ret;
}

int ObAiSystemTableManager::delete_task(const ObString &task_id, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", K(ret));
  } else {
    ObDMLSqlSplicer sql;
    ObSqlString delete_sql;
    int64_t affected_rows = 0;

    if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_id)))) {
      LOG_WARN("failed to add task_id column", K(ret));
    } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_AI_BATCH_TASK_TNAME, delete_sql))) {
      LOG_WARN("failed to splice delete sql", K(ret));
    } else if (OB_FAIL(sql_client.write(tenant_id_, delete_sql.ptr(), affected_rows))) {
      LOG_WARN("failed to delete task", K(ret), K(delete_sql));
    } else if (affected_rows == 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("task not found", K(ret), K(task_id));
    } else {
      LOG_INFO("successfully deleted task", K(task_id));
    }
  }
  return ret;
}

int ObAiSystemTableManager::archive_task_to_history(const ObString &task_id,
                                                     const ObString &token_usage,
                                                     const ObString &provider_timeline,
                                                     common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAiSystemTableManager not initialized", K(ret));
  } else if (OB_UNLIKELY(task_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", K(ret));
  } else {
    common::ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(&sql_proxy, tenant_id_))) {
      LOG_WARN("failed to start transaction for archive", K(ret), K(task_id));
    } else {
      // Step 1: Read the task from main table (within transaction for consistency)
      ObAiTaskInfo task_info;
      if (OB_FAIL(query_task_status(task_id, task_info, trans))) {
        LOG_WARN("failed to query task for archiving", K(ret), K(task_id));
      } else {
        // Step 2: Insert into history table
        ObDMLSqlSplicer sql;
        ObSqlString buffer;
        int64_t affected_rows = 0;

        if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TASK_ID, ObHexEscapeSqlStr(task_info.task_id_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_pk_column(ObAiTaskTableColumns::TENANT_ID, task_info.tenant_id_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::MODEL_NAME, ObHexEscapeSqlStr(task_info.model_name_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::COMMAND_TYPE, ObAiExecUtils::get_command_type_str(task_info.command_type_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::STATUS, static_cast<int64_t>(task_info.status_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::REQUESTS_HANDLED, task_info.requests_handled_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::TOTAL_REQUESTS, task_info.total_requests_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_CREATE_TIME, task_info.task_create_time_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_time_column(ObAiTaskTableColumns::TASK_UPDATE_TIME, task_info.task_update_time_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::BATCH_ID, ObHexEscapeSqlStr(task_info.batch_id_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::REMOTE_FILE_IDS, ObHexEscapeSqlStr(task_info.remote_file_ids_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::LOCAL_FILE_METADATA, ObHexEscapeSqlStr(task_info.local_file_metadata_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::ERROR_DETAIL, ObHexEscapeSqlStr(task_info.error_detail_)))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (OB_FAIL(sql.add_column(ObAiTaskTableColumns::DDL_TASK_ID, task_info.ddl_task_id_))) {
          LOG_WARN("failed to add column", K(ret));
        } else if (!token_usage.empty() && OB_FAIL(sql.add_column("token_usage", ObHexEscapeSqlStr(token_usage)))) {
          LOG_WARN("failed to add token_usage column", K(ret));
        } else if (!provider_timeline.empty() && OB_FAIL(sql.add_column("provider_timeline", ObHexEscapeSqlStr(provider_timeline)))) {
          LOG_WARN("failed to add provider_timeline column", K(ret));
        } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_BATCH_TASK_HISTORY_TNAME, buffer))) {
          LOG_WARN("failed to splice insert sql", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id_, buffer.ptr(), affected_rows))) {
          LOG_WARN("failed to insert into history table", K(ret), K(buffer));
        } else if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows for history insert", K(ret), K(affected_rows));
        } else {
          // Step 3: Delete from main table within same transaction
          if (OB_FAIL(delete_task(task_id, trans))) {
            LOG_WARN("failed to delete task from main table after archiving", K(ret), K(task_id));
          }
        }
      }

      int tmp_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS == ret && OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to commit archive transaction", K(ret), K(task_id));
      } else if (OB_SUCCESS != ret && OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to rollback archive transaction", K(ret), K(tmp_ret), K(task_id));
      }
    }
  }
  return ret;
}

int64_t ObAiSystemTableManager::get_current_time_us_() const
{
  return common::ObTimeUtility::current_time();
}

} // namespace share
} // namespace oceanbase