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

#define USING_LOG_PREFIX SHARE
#include "ob_ddl_error_message_table_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_schema_utils.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage::~ObBuildDDLErrorMessage()
{
  if (nullptr != user_message_) {
    allocator_.free(user_message_);
    user_message_ = nullptr;
  }
}

int ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage::prepare_user_message_buf(const int64_t len)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to prepare user msg buf", K(ret), K(len));
  } else {
    memset(buf, 0, len);
    user_message_ = buf;
  }
  return ret;
}

bool ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage::operator==(const ObBuildDDLErrorMessage &other) const
{
  bool equal = ret_code_ == other.ret_code_ && ddl_type_ == other.ddl_type_
            && 0 == STRNCMP(dba_message_, other.dba_message_, OB_MAX_ERROR_MSG_LEN);
  if (equal) {
    if (nullptr == user_message_ && nullptr == other.user_message_) {
    } else if (nullptr == user_message_ || nullptr == other.user_message_) {
      equal = false;
    } else {
      equal = equal && 0 == STRNCMP(user_message_, other.user_message_, OB_MAX_ERROR_MSG_LEN);
    }
  }
  return equal;
}

bool ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage::operator!=(const ObBuildDDLErrorMessage &other) const
{
  return !(operator==(other));
}

ObDDLErrorMessageTableOperator::ObDDLErrorMessageTableOperator()
{
}

ObDDLErrorMessageTableOperator::~ObDDLErrorMessageTableOperator()
{
}

// to get task id for rebuild unique index task, which is a child task for offline DDL, like drop column.
int ObDDLErrorMessageTableOperator::get_index_task_id(
    ObMySQLProxy &sql_proxy, const share::schema::ObTableSchema &index_schema, int64_t &task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t target_object_id = index_schema.get_table_id();
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_string.assign_fmt("SELECT task_id FROM %s WHERE tenant_id = %lu AND target_object_id = %lu",
        OB_ALL_DDL_TASK_STATUS_TNAME, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), target_object_id))) {
      LOG_WARN("assign sql string failed", K(ret), K(exec_tenant_id), K(target_object_id));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql_string.ptr()))) {
      LOG_WARN("update status of ddl task record failed", K(ret), K(sql_string));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret), KP(result));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, int64_t);
    }
  }
  return ret;
}

int ObDDLErrorMessageTableOperator::extract_index_key(const ObTableSchema &index_schema,
    const ObStoreRowkey &index_key, char *buffer, const int64_t buffer_len)
{
  int ret = OB_SUCCESS;
  if (!index_schema.is_valid() || !index_key.is_valid() || OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_schema), K(index_key), KP(buffer), K(buffer_len));
  } else {
    const int64_t index_size = index_schema.get_index_column_num();
    int64_t pos = 0;
    int64_t valid_index_size = 0;
    uint64_t column_id = OB_INVALID_ID;
    MEMSET(buffer, 0, buffer_len);

    for (int64_t i = 0; OB_SUCC(ret) && i < index_size; i++) {
      if (OB_FAIL(index_schema.get_index_info().get_column_id(i, column_id))) {
        LOG_WARN("Failed to get index column description", K(i), K(ret));
      } else if (column_id <= OB_MIN_SHADOW_COLUMN_ID) {
        valid_index_size ++;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_size; ++i) {
      const ObObj &obj  = index_key.get_obj_ptr()[i];
      if (OB_FAIL(obj.print_plain_str_literal(buffer, buffer_len, pos))) {
        LOG_WARN("fail to print_plain_str_literal", K(ret), KP(buffer));
      } else if (i < valid_index_size - 1) {
        if (OB_FAIL(databuff_printf(buffer,  buffer_len, pos, "-"))) {
          LOG_WARN("databuff print failed", K(ret));
        }
      }
    }
    if (buffer != nullptr) {
      buffer[pos++] = '\0';
      if (OB_SIZE_OVERFLOW == ret) {
        LOG_WARN("the index key length is larger than OB_TMP_BUF_SIZE_256", K(index_key), KP(buffer));
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObDDLErrorMessageTableOperator::load_ddl_user_error(const uint64_t tenant_id,
                                                        const int64_t task_id,
                                                        const uint64_t table_id,
                                                        ObMySQLProxy &sql_proxy,
                                                        ObBuildDDLErrorMessage &error_message)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  LOG_INFO("begin to load ddl user error", K(tenant_id), K(task_id), K(table_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == table_id
        || nullptr != error_message.user_message_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT ret_code, ddl_type, affected_rows, user_message, dba_message from %s WHERE tenant_id = %ld AND "
        "task_id = %ld AND object_id = %ld", OB_ALL_DDL_ERROR_MESSAGE_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
        task_id, ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id)))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ITER_END;
      LOG_INFO("single replica has not reported before", K(ret), K(table_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          }
        } else {
          ObString str_user_message;
          ObString str_dba_message;
          int ddl_type = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, int);
          EXTRACT_INT_FIELD_MYSQL(*result, "affected_rows", error_message.affected_rows_, int);
          error_message.ddl_type_ = static_cast<ObDDLType>(ddl_type);
          EXTRACT_INT_FIELD_MYSQL(*result, "ret_code", error_message.ret_code_, int);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "user_message", str_user_message);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "dba_message", str_dba_message);
          const int64_t buf_len = str_user_message.length() + 1;
          if (OB_FAIL(error_message.prepare_user_message_buf(buf_len))) {
            LOG_WARN("failed to prepare buf", K(ret));
          } else if (OB_FAIL(databuff_printf(error_message.user_message_, buf_len, "%.*s", str_user_message.length(), str_user_message.ptr()))) {
            LOG_WARN("print to buffer failed", K(ret), K(str_user_message));
          } else if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "%.*s", str_dba_message.length(), str_dba_message.ptr()))) {
            LOG_WARN("print to buffer failed", K(ret), K(str_dba_message));
          } else if (OB_SUCCESS != error_message.ret_code_ && !str_user_message.empty() && NULL == str_user_message.find('%')) {
            LOG_INFO("load ddl user error success", K(error_message));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLErrorMessageTableOperator::get_ddl_error_message(
    const uint64_t tenant_id, const int64_t task_id, const int64_t target_object_id,
    const ObAddr &addr, const bool is_ddl_retry_task, ObMySQLProxy &sql_proxy, 
    ObBuildDDLErrorMessage &error_message, int64_t &forward_user_msg_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  forward_user_msg_len = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    sqlclient::ObMySQLResult *result = NULL;
    char ip[common::OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || target_object_id < -1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(target_object_id), K(addr));
    } else if (!is_ddl_retry_task && OB_FAIL(sql.assign_fmt(
        "SELECT ret_code, ddl_type, affected_rows, dba_message, user_message from %s "
        "WHERE tenant_id = %ld AND task_id = %ld AND target_object_id = %ld "
        , OB_ALL_DDL_ERROR_MESSAGE_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), task_id, target_object_id))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (is_ddl_retry_task && OB_FAIL(sql.assign_fmt(
        "SELECT ret_code, ddl_type, affected_rows, dba_message, UNHEX(user_message) as user_message from %s "
        "WHERE tenant_id = %ld AND task_id = %ld AND target_object_id = %ld "
        , OB_ALL_DDL_ERROR_MESSAGE_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), task_id, target_object_id))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (addr.is_valid()) {
      if (!addr.ip_to_string(ip, sizeof(ip))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to convert ip to string", K(ret), K(addr));
      } else if (OB_FAIL(sql.append_fmt(" AND svr_ip = '%s' AND svr_port = %d", ip, addr.get_port()))) {
        LOG_WARN("append sql string failed", K(ret), K(addr));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      char *buf = nullptr;
      int ddl_type = 0;
      ObString str_dba_message;
      ObString str_user_message;
      EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, int);
      EXTRACT_INT_FIELD_MYSQL(*result, "affected_rows", error_message.affected_rows_, int);
      error_message.ddl_type_ = static_cast<ObDDLType>(ddl_type);
      EXTRACT_INT_FIELD_MYSQL(*result, "ret_code", error_message.ret_code_, int);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "dba_message", str_dba_message);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "user_message", str_user_message);
      forward_user_msg_len = str_user_message.length();
      const int64_t buf_size = str_user_message.length() + 1;
      if (OB_ISNULL(error_message.user_message_ = static_cast<char *>(error_message.allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "%.*s", str_dba_message.length(), str_dba_message.ptr()))) {
        LOG_WARN("print to buffer failed", K(ret), K(str_dba_message));
      } else {
        error_message.user_message_[buf_size - 1] = '\0';
        MEMCPY(error_message.user_message_, str_user_message.ptr(), str_user_message.length());
      }
    }
  }
  return ret;
}

//report the status of building index into __all_ddl_error_message
int ObDDLErrorMessageTableOperator::report_ddl_error_message(const ObBuildDDLErrorMessage &error_message,
    const uint64_t tenant_id, const int64_t task_id, const uint64_t table_id, const int64_t schema_version, 
    const int64_t object_id, const ObAddr &addr, ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  int64_t unused_user_msg_len = 0;
  ObBuildDDLErrorMessage report_error_message;
  bool need_report = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == table_id 
    || OB_INVALID_VERSION == schema_version || object_id < -1 || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id), K(schema_version), K(object_id), K(addr), K(error_message));
  } else if (OB_FAIL(get_ddl_error_message(tenant_id, task_id, object_id /*target_object_id*/, addr, false /* is_ddl_retry_task */, sql_proxy, 
    report_error_message, unused_user_msg_len))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      need_report = true;
    } else {
      LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(table_id), K(schema_version), K(object_id), K(addr));
    }
  } else {
    need_report = (report_error_message != error_message);
  }

  if (!need_report) {
    LOG_INFO("process ddl error message has already been reported before", K(ret),
        K(tenant_id), K(report_error_message), K(error_message), K(table_id), K(schema_version), K(object_id));
  }

  if (OB_SUCC(ret) && need_report) {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    int64_t affected_rows = 0;
    ObSqlString update_sql;
    char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (!addr.ip_to_string(ip, sizeof(ip))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert ip to string failed", K(ret), K(addr));
    } else if (OB_FAIL(update_sql.assign_fmt("INSERT INTO %s (tenant_id, task_id, object_id, schema_version, "
        "target_object_id, svr_ip, svr_port, ret_code, ddl_type, affected_rows, user_message, dba_message) VALUES("
        "%ld, %ld, %ld, %ld, %ld, \"%s\", %d, %d, %d, %ld, \"%s\", \"%s\") ON DUPLICATE KEY UPDATE ret_code = %d, "
        "ddl_type = %d, affected_rows = %ld, user_message = \"%s\", dba_message = \"%s\"", OB_ALL_DDL_ERROR_MESSAGE_TNAME,
        ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id), task_id,
        ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
        schema_version, object_id, ip, addr.get_port(), error_message.ret_code_,
        error_message.ddl_type_, error_message.affected_rows_, error_message.user_message_, error_message.dba_message_,
        error_message.ret_code_, error_message.ddl_type_, error_message.affected_rows_, error_message.user_message_, error_message.dba_message_))) {
      LOG_WARN("fail to assign fmt", K(ret), K(table_id), K(schema_version), K(object_id), K(addr), K(error_message));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, update_sql.ptr(), affected_rows))) {     //execute update sql
      LOG_WARN("fail to write sql", KR(ret), K(update_sql));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
    } else {
      LOG_INFO("process ddl error message report success", K(ret), K(task_id), K(schema_version), K(table_id), K(addr), K(error_message), K(update_sql.ptr()));
    }
  }
  return ret;
}

// generate error message based on input param
int ObDDLErrorMessageTableOperator::build_ddl_error_message(
    const int ret_code,
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObBuildDDLErrorMessage &error_message,
    const ObString index_name,
    const uint64_t index_id,
    const ObDDLType ddl_type,
    const char *message,
    int &report_ret_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret_code = ret_code;
  bool is_oracle_mode = false;
  const char *str_user_error = NULL;
  const char *str_error = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id || index_name.empty() || OB_INVALID_ID == index_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(table_id), K(index_id), K(tmp_ret_code));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
      tenant_id, table_id, is_oracle_mode))) {
    LOG_WARN("fail to check whether is oracle mode", K(ret), K(tenant_id), K(table_id));
  } else {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == tmp_ret_code) {
      tmp_ret_code = OB_ERR_DUPLICATED_UNIQUE_KEY;    //error message of OB_ERR_PRIMARY_KEY_DUPLICATE is not compatiable with oracle, so use a new error code
      report_ret_code = tmp_ret_code;
    }
    error_message.ret_code_ = tmp_ret_code;
    error_message.ddl_type_ = ddl_type;
    str_user_error = ob_errpkt_str_user_error(tmp_ret_code, is_oracle_mode);
    str_error = ob_errpkt_strerror(tmp_ret_code, is_oracle_mode);
    if (OB_SUCCESS == tmp_ret_code) {
      if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print to buffer failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", "Successful ddl"))) {
        LOG_WARN("print to buffer failed", K(ret));
      }
    } else if (OB_ERR_DUPLICATED_UNIQUE_KEY == tmp_ret_code) {    //building a unique index violates the uniqueness constraint
      if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN,
          "Supported ddl error message type: create unique index, index_id = %ld", index_id))) {
        LOG_WARN("print to buffer failed", K(ret), K(table_id));
      } else {
        if (is_oracle_mode) {
          if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", str_user_error))) {
            LOG_WARN("print to buffer failed", K(ret), K(str_user_error), K(index_name));
          }
        } else {
          if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN,
              str_user_error, message, index_name.length(), index_name.ptr()))) {
            LOG_WARN("print to buffer failed", K(ret), K(str_user_error), K(index_name));
          }
        }
      }
    } else {
      if (OB_FAIL(databuff_printf(error_message.dba_message_, OB_MAX_ERROR_MSG_LEN, "Unsupported ddl error message type"))) {
        LOG_WARN("print to buffer failed", K(ret));
      } else if (OB_FAIL(databuff_printf(error_message.user_message_, OB_MAX_ERROR_MSG_LEN, "%s", str_error))) {
        LOG_WARN("print to buffer failed", K(ret), K(str_error));
      }
    }
  }
  return ret;
}

int ObDDLErrorMessageTableOperator::generate_index_ddl_error_message(const int ret_code,
    const ObTableSchema &index_schema, const int64_t task_id, const int64_t object_id, const ObAddr &addr,
    ObMySQLProxy &sql_proxy, const char *index_key, int &report_ret_code)
{
  int ret = OB_SUCCESS;
  ObBuildDDLErrorMessage error_message;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t data_table_id = index_schema.get_data_table_id();
  const uint64_t index_table_id = index_schema.get_table_id();
  const int64_t schema_version = index_schema.get_schema_version();
  ObString index_name;
  LOG_INFO("begin to generate index ddl error message", K(ret_code), K(data_table_id), K(index_table_id),
      K(schema_version), K(object_id), K(addr), K(index_key));
  if (OB_ISNULL(error_message.user_message_ = static_cast<char *>(error_message.allocator_.alloc(OB_MAX_ERROR_MSG_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else if (OB_FALSE_IT(memset(error_message.user_message_, 0, OB_MAX_ERROR_MSG_LEN))) {
  } else if (OB_FAIL(index_schema.get_index_name(index_name))) {        //get index name
    LOG_WARN("fail to get index name", K(ret), K(index_name), K(index_table_id));
  } else if (OB_FAIL(build_ddl_error_message(ret_code, index_schema.get_tenant_id(), data_table_id, error_message, index_name,
      index_table_id, DDL_CREATE_INDEX, index_key, report_ret_code))) {
    LOG_WARN("build ddl error message failed", K(ret), K(data_table_id), K(index_name));
  } else if (OB_FAIL(report_ddl_error_message(error_message,    //report into __all_ddl_error_message
      tenant_id, task_id, data_table_id, schema_version, object_id, addr, sql_proxy))) {
    LOG_WARN("fail to report ddl error message", K(ret), K(tenant_id), K(data_table_id),
        K(schema_version), K(object_id), K(addr), K(index_table_id));
  }
  return ret;
}
