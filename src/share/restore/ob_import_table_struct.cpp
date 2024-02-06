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
#include "ob_import_table_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/restore/ob_import_util.h"
//#include "share/schema/ob_column_schema.h"

using namespace oceanbase;
using namespace share;

void ObImportResult::set_result(const bool is_succeed, const Comment &comment)
{
  is_succeed_ = is_succeed;
  comment_ = comment;
}

int ObImportResult::set_result(const bool is_succeed, const char *buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(comment_.ptr(), comment_.capacity(), "%.*s", static_cast<int>(comment_.capacity()), buf))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else {
    is_succeed_ = is_succeed;
  }
  return ret;
}

int ObImportResult::set_result(const int err_code, const share::ObTaskId &trace_id, const ObAddr &addr, const ObString &extra_info)
{
  int ret = OB_SUCCESS;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char addr_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_SUCCESS == err_code) {
    is_succeed_ = true;
  } else if (OB_FALSE_IT(trace_id.to_string(trace_id_buf, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(addr.ip_port_to_string(addr_buf, OB_MAX_SERVER_ADDR_SIZE))) {
    LOG_WARN("failed to convert addr to string", K(ret), K(addr));
  } else if (OB_FAIL(databuff_printf(comment_.ptr(), comment_.capacity(), "result:%d(%s), addr:%s, trace_id:%s",
                                     err_code, extra_info.empty() ? "" : extra_info.ptr(),
                                     addr_buf, trace_id_buf))) {
    LOG_WARN("failed to databuff printf", K(ret), K(err_code), K(trace_id), K(addr));
  } else {
    is_succeed_ = false;
  }
  return ret;
}
void ObImportResult::reset()
{
  is_succeed_ = true;
  comment_.reset();
}

ObImportResult &ObImportResult::operator=(const ObImportResult &result)
{
  is_succeed_ = result.is_succeed_;
  comment_ = result.comment_;
  return *this;
}
const char* ObImportTableTaskStatus::get_str() const
{
  const char *str = "UNKNOWN";
  const char *status_strs[] = {
    "INIT",
    "DOING",
    "FINISH",
  };

  STATIC_ASSERT(MAX == ARRAYSIZEOF(status_strs), "status count mismatch");
  if (status_ < INIT || status_ >= MAX) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid status", K(status_));
  } else {
    str = status_strs[status_];
  }
  return str;
}

int ObImportTableTaskStatus::set_status(const char *str)
{
  int ret = OB_SUCCESS;
  ObString s(str);
  const char *status_strs[] = {
    "INIT",
    "DOING",
    "FINISH",
  };
  const int64_t count = ARRAYSIZEOF(status_strs);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(status_strs[i])) {
        status_ = static_cast<Status>(i);
      }
    }
  }
  return ret;
}

ObImportTableTaskStatus ObImportTableTaskStatus::get_next_status(const int err_code)
{
  ObImportTableTaskStatus status;
  if (!is_valid()) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected import table task status", K(status_));
  } else if (OB_SUCCESS != err_code || Status::DOING == status_) {
    status = Status::FINISH;
  } else {
    status = Status::DOING;
  }
  return status;
}



#define PARSE_INT_VALUE(COLUMN_NAME)                                    \
  if (OB_SUCC(ret)) {                                                   \
    int64_t value = 0;                                                  \
    EXTRACT_INT_FIELD_MYSQL(result, #COLUMN_NAME, value, int64_t);      \
    if (OB_SUCC(ret)) {                                                 \
      set_##COLUMN_NAME(value);                                         \
    }                                                                   \
  }

#define PARSE_UINT_VALUE(COLUMN_NAME)                                   \
  if (OB_SUCC(ret)) {                                                   \
    uint64_t value = 0;                                                 \
    EXTRACT_UINT_FIELD_MYSQL(result, #COLUMN_NAME, value, uint64_t);    \
    if (OB_SUCC(ret)) {                                                 \
      set_##COLUMN_NAME(value);                                         \
    }                                                                   \
  }

#define PARSE_STR_VALUE(COLUMN_NAME)    \
  if (OB_SUCC(ret)) {                        \
    ObString value;                          \
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, #COLUMN_NAME, value, true, false, value); \
    if (OB_FAIL(set_##COLUMN_NAME(value))) {                  \
      LOG_WARN("failed to set column value", KR(ret), K(value));    \
    }                                                               \
  }

#define FILL_INT_COLUMN(COLUMN_NAME)  \
  if (OB_SUCC(ret)) {                      \
    if (OB_FAIL(dml.add_column(#COLUMN_NAME, (COLUMN_NAME##_)))) { \
      LOG_WARN("failed to add column", K(ret));                    \
    }                                                              \
  }

#define FILL_UINT_COLUMN(COLUMN_NAME)  \
  if (OB_SUCC(ret)) {                      \
    if (OB_FAIL(dml.add_uint64_column(#COLUMN_NAME, (COLUMN_NAME##_)))) { \
      LOG_WARN("failed to add column", K(ret));                           \
    }                                                                     \
  }

#define FILL_STR_COLUMN(COLUMN_NAME) \
  if (OB_SUCC(ret)) {                     \
    if (OB_FAIL((dml.add_column(#COLUMN_NAME, (COLUMN_NAME##_))))) { \
      LOG_WARN("failed to add column", K(ret));                            \
    }                                                                      \
  }

#define FILL_HEX_STR_COLUMN(COLUMN_NAME) \
if (OB_SUCC(ret)) {                     \
  if (OB_FAIL((dml.add_column(#COLUMN_NAME, ObHexEscapeSqlStr(COLUMN_NAME##_))))) { \
    LOG_WARN("failed to add column", K(ret));                            \
  }                                                                      \
}


void ObImportTableTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_id_ = 0;
  job_id_ = 0;
  src_tenant_id_ = OB_INVALID_TENANT_ID;
  src_tablespace_.reset();
  src_tablegroup_.reset();
  src_database_.reset();
  src_table_.reset();
  src_partition_.reset();
  target_tablespace_.reset();
  target_tablegroup_.reset();
  target_database_.reset();
  target_table_.reset();
  table_column_ = 0;
  status_ = ObImportTableTaskStatus::MAX;
  start_ts_ = OB_INVALID_TIMESTAMP;
  completion_ts_ = OB_INVALID_TIMESTAMP;
  cumulative_ts_ = OB_INVALID_TIMESTAMP;
  total_bytes_ = 0;
  total_rows_ = 0;
  imported_bytes_ = 0;
  imported_rows_ = 0;
  total_index_count_ = 0;
  imported_index_count_ = 0;
  failed_index_count_ = 0;
  total_constraint_count_ = 0;
  imported_constraint_count_ = 0;
  failed_constraint_count_ = 0;
  total_ref_constraint_count_ = 0;
  imported_ref_constraint_count_ = 0;
  failed_ref_constraint_count_ = 0;
  total_trigger_count_ = 0;
  imported_trigger_count_ = 0;
  failed_trigger_count_ = 0;
  result_.reset();
}

int ObImportTableTask::assign(const ObImportTableTask &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(that));
  } else if (OB_FAIL(set_src_tablespace(that.get_src_tablespace()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_src_tablegroup(that.get_src_tablegroup()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_src_database(that.get_src_database()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_src_table(that.get_src_table()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_src_partition(that.get_src_partition()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_target_tablespace(that.get_target_tablespace()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_target_tablegroup(that.get_target_tablegroup()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_target_database(that.get_target_database()))) {
    LOG_WARN("failed to set", K(ret));
  } else if (OB_FAIL(set_target_table(that.get_target_table()))) {
    LOG_WARN("failed to set", K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    task_id_ = that.task_id_;
    job_id_ = that.job_id_;
    src_tenant_id_ = that.src_tenant_id_;
    table_column_ = that.table_column_;
    status_ = that.status_;
    start_ts_ = that.start_ts_;
    completion_ts_ = that.completion_ts_;
    cumulative_ts_ = that.cumulative_ts_;
    total_bytes_ = that.total_bytes_;
    total_rows_ = that.total_rows_;
    imported_bytes_ = that.imported_bytes_;
    imported_rows_ = that.imported_rows_;
    total_index_count_ = that.total_index_count_;
    imported_index_count_ = that.imported_index_count_;
    failed_index_count_ = that.failed_index_count_;
    total_constraint_count_ = that.total_constraint_count_;
    imported_constraint_count_ = that.imported_constraint_count_;
    failed_constraint_count_ = that.failed_constraint_count_;
    total_ref_constraint_count_ = that.total_ref_constraint_count_;
    imported_ref_constraint_count_ = that.imported_ref_constraint_count_;
    failed_ref_constraint_count_ = that.failed_ref_constraint_count_;
    total_trigger_count_ = that.total_trigger_count_;
    imported_trigger_count_ = that.imported_trigger_count_;
    failed_trigger_count_ = that.failed_trigger_count_;
    result_ = that.result_;
  }
  return ret;
}

int ObImportTableTask::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObImportTableTask::get_pkey(Key &key) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pkey", K(ret), KPC(this));
  } else {
    key.tenant_id_ = get_tenant_id();
    key.task_id_ = get_task_id();
  }
  return ret;
}

int ObImportTableTask::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TASK_ID, task_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObImportTableTask::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t OB_MAX_RESULT_BUF_LEN = 12;
  char result_buf[OB_MAX_RESULT_BUF_LEN] = "";
  ObImportResult::Comment comment;
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "src_tenant_id", src_tenant_id_, uint64_t);
  PARSE_INT_VALUE(task_id)
  PARSE_INT_VALUE(job_id)
  PARSE_STR_VALUE(src_tablespace)
  PARSE_STR_VALUE(src_tablegroup)
  PARSE_STR_VALUE(src_database)
  PARSE_STR_VALUE(src_table)
  PARSE_STR_VALUE(src_partition)
  PARSE_STR_VALUE(target_tablespace)
  PARSE_STR_VALUE(target_tablegroup)
  PARSE_STR_VALUE(target_database)
  PARSE_STR_VALUE(target_table)
  PARSE_INT_VALUE(table_column)
  if (OB_SUCC(ret)) {
    int64_t real_length = 0;
    char status_str[OB_DEFAULT_STATUS_LENTH] = "";
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(status_.set_status(status_str))) {
        LOG_WARN("failed to set status", K(ret), K(status_str));
      }
    }
  }
  PARSE_INT_VALUE(start_ts)
  PARSE_INT_VALUE(completion_ts)
  PARSE_INT_VALUE(cumulative_ts)
  PARSE_INT_VALUE(total_bytes)
  PARSE_INT_VALUE(total_rows)
  PARSE_INT_VALUE(imported_bytes)
  PARSE_INT_VALUE(imported_rows)
  PARSE_INT_VALUE(total_index_count)
  PARSE_INT_VALUE(imported_index_count)
  PARSE_INT_VALUE(failed_index_count)
  PARSE_INT_VALUE(total_constraint_count)
  PARSE_INT_VALUE(imported_constraint_count)
  PARSE_INT_VALUE(failed_constraint_count)
  PARSE_INT_VALUE(total_ref_constraint_count)
  PARSE_INT_VALUE(imported_ref_constraint_count)
  PARSE_INT_VALUE(failed_ref_constraint_count)
  PARSE_INT_VALUE(total_trigger_count)
  PARSE_INT_VALUE(imported_trigger_count)
  PARSE_INT_VALUE(failed_trigger_count)

  int64_t real_length = 0;
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_RESULT, result_buf, OB_MAX_RESULT_BUF_LEN, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, OB_STR_COMMENT, comment.ptr(), comment.capacity(), real_length);
  if (OB_SUCC(ret)) {
    bool is_succeed = true;
    if (0 == STRCMP("FAILED", result_buf)) {
      is_succeed = false;
    }
    result_.set_result(is_succeed, comment);
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to parse ObImportTableTask", KPC(this));
  }
  return ret;
}

int ObImportTableTask::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill pkey dml", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  }
  FILL_INT_COLUMN(job_id)
  FILL_INT_COLUMN(src_tenant_id)
  FILL_HEX_STR_COLUMN(src_tablespace)
  FILL_HEX_STR_COLUMN(src_tablegroup)
  FILL_HEX_STR_COLUMN(src_database)
  FILL_HEX_STR_COLUMN(src_table)
  FILL_HEX_STR_COLUMN(src_partition)
  FILL_HEX_STR_COLUMN(target_tablespace)
  FILL_HEX_STR_COLUMN(target_tablegroup)
  FILL_HEX_STR_COLUMN(target_database)
  FILL_HEX_STR_COLUMN(target_table)
  FILL_INT_COLUMN(table_column)
  FILL_INT_COLUMN(start_ts)
  FILL_INT_COLUMN(completion_ts)
  FILL_INT_COLUMN(cumulative_ts)
  FILL_INT_COLUMN(total_bytes)
  FILL_INT_COLUMN(total_rows)
  FILL_INT_COLUMN(imported_bytes)
  FILL_INT_COLUMN(imported_rows)
  FILL_INT_COLUMN(total_index_count)
  FILL_INT_COLUMN(imported_index_count)
  FILL_INT_COLUMN(failed_index_count)
  FILL_INT_COLUMN(total_constraint_count)
  FILL_INT_COLUMN(imported_constraint_count)
  FILL_INT_COLUMN(failed_constraint_count)
  FILL_INT_COLUMN(total_ref_constraint_count)
  FILL_INT_COLUMN(imported_ref_constraint_count)
  FILL_INT_COLUMN(failed_ref_constraint_count)
  FILL_INT_COLUMN(total_trigger_count)
  FILL_INT_COLUMN(imported_trigger_count)
  FILL_INT_COLUMN(failed_trigger_count)
  if (OB_SUCC(ret) && status_.is_finish()) {
    if (FAILEDx(dml.add_column(OB_STR_RESULT, result_.get_result_str()))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, result_.get_comment()))) {
      LOG_WARN("failed to add column", K(ret));
    }
  }

  return ret;
}


const char* ObImportTableJobStatus::get_str() const
{
  const char *str = "UNKNOWN";
  const char *status_strs[] = {
    "INIT",
    "IMPORT_TABLE",
    "RECONSTRUCT_REF_CONSTRAINT",
    "CANCELING",
    "IMPORT_FINISH",
  };

  STATIC_ASSERT(MAX_STATUS == ARRAYSIZEOF(status_strs), "status count mismatch");
  if (status_ < INIT || status_ >= MAX_STATUS) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid status", K(status_));
  } else {
    str = status_strs[status_];
  }
  return str;
}

int ObImportTableJobStatus::set_status(const char *str)
{
  int ret = OB_SUCCESS;
  ObString s(str);
  const char *status_strs[] = {
    "INIT",
    "IMPORT_TABLE",
    "RECONSTRUCT_REF_CONSTRAINT",
    "CANCELING",
    "IMPORT_FINISH",
  };
  const int64_t count = ARRAYSIZEOF(status_strs);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(status_strs[i])) {
        status_ = static_cast<Status>(i);
      }
    }
  }
  return ret;
}

ObImportTableJobStatus ObImportTableJobStatus::get_next_status(const ObImportTableJobStatus &cur_status)
{
  ObImportTableJobStatus ret;
  switch(cur_status) {
    case ObImportTableJobStatus::Status::INIT: {
      ret = ObImportTableJobStatus::Status::IMPORT_TABLE;
      break;
    }
    case ObImportTableJobStatus::Status::IMPORT_TABLE: {
      ret = ObImportTableJobStatus::Status::RECONSTRUCT_REF_CONSTRAINT;
      break;
    }
    case ObImportTableJobStatus::Status::RECONSTRUCT_REF_CONSTRAINT: {
      ret = ObImportTableJobStatus::Status::IMPORT_FINISH;
      break;
    }
    case ObImportTableJobStatus::Status::IMPORT_FINISH: {
      ret = ObImportTableJobStatus::Status::IMPORT_FINISH;
      break;
    }
    default: {
      break;
    }
  }
  return ret;
}



void ObImportTableJob::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_ = 0;
  initiator_tenant_id_ = OB_INVALID_TENANT_ID;
  initiator_job_id_ = 0;
  start_ts_ = 0;
  end_ts_ = 0;
  src_tenant_name_.reset();
  src_tenant_id_ = OB_INVALID_TENANT_ID;
  status_ = ObImportTableJobStatus::Status::MAX_STATUS;
  total_table_count_ = 0;
  finished_table_count_ = 0;
  failed_table_count_ = 0;
  total_bytes_ = 0;
  finished_bytes_ = 0;
  failed_bytes_ = 0;
  result_.reset();
  import_arg_.reset();
  description_.reset();
}

int ObImportTableJob::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObImportTableJob::get_pkey(Key &key) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pkey", K(ret), KPC(this));
  } else {
    key.tenant_id_ = get_tenant_id();
    key.job_id_ = get_job_id();
  }
  return ret;
}

int ObImportTableJob::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObImportTableJob::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode name_case_mode;
  bool import_all = false;
  const int64_t OB_MAX_RESULT_BUF_LEN = 12;
  char result_buf[OB_MAX_RESULT_BUF_LEN] = "";
  ObImportResult::Comment comment;
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id_, uint64_t);
  if (FAILEDx(ObImportTableUtil::get_tenant_name_case_mode(tenant_id_, name_case_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(tenant_id_));
  }
  EXTRACT_INT_FIELD_MYSQL(result, "initiator_tenant_id", initiator_tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "src_tenant_id", src_tenant_id_, uint64_t);
  EXTRACT_BOOL_FIELD_MYSQL(result, "import_all", import_all);
  if (OB_SUCC(ret) && import_all) {
    import_arg_.get_import_table_arg().set_import_all();
  }

  PARSE_INT_VALUE(job_id)
  PARSE_INT_VALUE(initiator_job_id)
  PARSE_INT_VALUE(start_ts)
  PARSE_INT_VALUE(end_ts)
  PARSE_STR_VALUE(src_tenant_name)
  PARSE_STR_VALUE(description)
  if (OB_SUCC(ret)) {
    int64_t real_length = 0;
    char status_str[OB_DEFAULT_STATUS_LENTH] = "";
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(status_.set_status(status_str))) {
        LOG_WARN("failed to set status", K(ret), K(status_str));
      }
    }
  }
  PARSE_INT_VALUE(total_table_count)
  PARSE_INT_VALUE(finished_table_count)
  PARSE_INT_VALUE(failed_table_count)
  PARSE_INT_VALUE(total_bytes)
  PARSE_INT_VALUE(finished_bytes)
  PARSE_INT_VALUE(failed_bytes)

  int64_t real_length = 0;
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_RESULT, result_buf, OB_MAX_RESULT_BUF_LEN, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, OB_STR_COMMENT, comment.ptr(), comment.capacity(), real_length);
  if (OB_SUCC(ret)) {
    bool is_succeed = true;
    if (0 == STRCMP("FAILED", result_buf)) {
      is_succeed = false;
    }
    result_.set_result(is_succeed, comment);
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportDatabaseArray &import_database_array = import_arg_.get_import_table_arg().get_import_database_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_db_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_database_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize database array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportTableArray &import_table_array = import_arg_.get_import_table_arg().get_import_table_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_table_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_table_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize table array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportPartitionArray &import_part_array = import_arg_.get_import_table_arg().get_import_partition_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_partition_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_part_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize partition array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapDatabaseArray &remap_database_array = import_arg_.get_remap_table_arg().get_remap_database_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_db_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_database_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap database array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTableArray &remap_table_array = import_arg_.get_remap_table_arg().get_remap_table_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_table_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_table_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap table array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapPartitionArray &remap_partition_array = import_arg_.get_remap_table_arg().get_remap_partition_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_partition_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_partition_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap partition array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTablegroupArray &remap_tablegroup_array = import_arg_.get_remap_table_arg().get_remap_tablegroup_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_tablegroup_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_tablegroup_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap tablegroup array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTablespaceArray &remap_tablespace_array = import_arg_.get_remap_table_arg().get_remap_tablespace_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_tablespace_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_tablespace_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap tablespace array", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed parse import table job", KPC(this));
  }

  return ret;
}

int ObImportTableJob::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  const bool import_all = import_arg_.get_import_table_arg().is_import_all();
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column("import_all", import_all))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, result_.get_comment()))) {
    LOG_WARN("failed to add column", K(ret));
  }
  FILL_INT_COLUMN(initiator_tenant_id)
  FILL_INT_COLUMN(initiator_job_id)
  FILL_INT_COLUMN(start_ts)
  FILL_INT_COLUMN(end_ts)
  FILL_STR_COLUMN(src_tenant_name)
  FILL_INT_COLUMN(src_tenant_id)
  FILL_INT_COLUMN(total_table_count)
  FILL_INT_COLUMN(finished_table_count)
  FILL_INT_COLUMN(failed_table_count)
  FILL_INT_COLUMN(total_bytes)
  FILL_INT_COLUMN(finished_bytes)
  FILL_INT_COLUMN(failed_bytes)
  FILL_STR_COLUMN(description)

  if (OB_SUCC(ret) && status_.is_finish()) {
    if (OB_FAIL(dml.add_column(OB_STR_RESULT, result_.get_result_str()))) {
      LOG_WARN("failed to add column", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator;

    ObString db_list_;
    ObString table_list_;
    ObString partition_list_;
    ObString remap_db_list_;
    ObString remap_table_list_;
    ObString remap_partition_list_;
    ObString remap_tablegroup_list_;
    ObString remap_tablespace_list_;

    ObString hex_db_list_;
    ObString hex_table_list_;
    ObString hex_partition_list_;
    ObString hex_remap_db_list_;
    ObString hex_remap_table_list_;
    ObString hex_remap_partition_list_;
    ObString hex_remap_tablegroup_list_;
    ObString hex_remap_tablespace_list_;

    const share::ObImportTableArg &import_table_arg = import_arg_.get_import_table_arg();
    const share::ObImportRemapArg &remap_table_arg = import_arg_.get_remap_table_arg();

    if (OB_FAIL(import_table_arg.get_db_list_format_str(allocator, db_list_))) {
      LOG_WARN("fail to get db list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_table_list_format_str(allocator, table_list_))) {
      LOG_WARN("fail to get table list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_partition_list_format_str(allocator, partition_list_))) {
      LOG_WARN("fail to get partition list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_db_list_format_str(allocator, remap_db_list_))) {
      LOG_WARN("fail to get remap db list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_table_list_format_str(allocator, remap_table_list_))) {
      LOG_WARN("fail to get remap table list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_partition_list_format_str(allocator, remap_partition_list_))) {
      LOG_WARN("fail to get remap partition list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablegroup_list_format_str(allocator, remap_tablegroup_list_))) {
      LOG_WARN("fail to get remap tablegroup list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablespace_list_format_str(allocator, remap_tablespace_list_))) {
      LOG_WARN("fail to get remap tablespace list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(import_table_arg.get_db_list_hex_format_str(allocator, hex_db_list_))) {
      LOG_WARN("fail to get hex db list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_table_list_hex_format_str(allocator, hex_table_list_))) {
      LOG_WARN("fail to get hex table list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_partition_list_hex_format_str(allocator, hex_partition_list_))) {
      LOG_WARN("fail to get hex partition list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_db_list_hex_format_str(allocator, hex_remap_db_list_))) {
      LOG_WARN("fail to get remap hex db list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_table_list_hex_format_str(allocator, hex_remap_table_list_))) {
      LOG_WARN("fail to get remap hex table list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_partition_list_hex_format_str(allocator, hex_remap_partition_list_))) {
      LOG_WARN("fail to get remap hex partition list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablegroup_list_hex_format_str(allocator, hex_remap_tablegroup_list_))) {
      LOG_WARN("fail to get remap hex tablegroup list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablespace_list_hex_format_str(allocator, hex_remap_tablespace_list_))) {
      LOG_WARN("fail to get remap hex tablespace list format str", KR(ret), K(remap_table_arg));
    } else {
      FILL_STR_COLUMN(db_list)
      FILL_STR_COLUMN(table_list)
      FILL_STR_COLUMN(partition_list)
      FILL_STR_COLUMN(remap_db_list)
      FILL_STR_COLUMN(remap_table_list)
      FILL_STR_COLUMN(remap_partition_list)
      FILL_STR_COLUMN(remap_tablegroup_list)
      FILL_STR_COLUMN(remap_tablespace_list)

      FILL_STR_COLUMN(hex_db_list)
      FILL_STR_COLUMN(hex_table_list)
      FILL_STR_COLUMN(hex_partition_list)
      FILL_STR_COLUMN(hex_remap_db_list)
      FILL_STR_COLUMN(hex_remap_table_list)
      FILL_STR_COLUMN(hex_remap_partition_list)
      FILL_STR_COLUMN(hex_remap_tablegroup_list)
      FILL_STR_COLUMN(hex_remap_tablespace_list)
    }
  }
  return ret;
}

int ObImportTableJob::assign(const ObImportTableJob &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(import_arg_.assign(that.import_arg_))) {
    LOG_WARN("failed to assign import arg", K(ret));
  } else if (OB_FAIL(set_src_tenant_name(that.get_src_tenant_name()))) {
    LOG_WARN("failed to set src tenant name", K(ret));
  } else if (OB_FAIL(set_description(that.get_description()))) {
    LOG_WARN("failed to set description", K(ret));
  } else {
    set_tenant_id(that.get_tenant_id());
    set_job_id(that.get_job_id());
    set_initiator_job_id(that.get_initiator_job_id());
    set_initiator_tenant_id(that.get_initiator_tenant_id());
    set_start_ts(that.get_start_ts());
    set_end_ts(that.get_end_ts());
    set_src_tenant_id(that.get_src_tenant_id());
    set_status(that.get_status());
    set_total_table_count(that.get_total_table_count());
    set_finished_table_count(that.get_finished_table_count());
    set_failed_table_count(that.get_failed_table_count());
    set_total_bytes(that.get_total_bytes());
    set_finished_bytes(that.get_finished_bytes());
    set_failed_bytes(that.get_failed_bytes());
    set_result(that.get_result());
  }
  return ret;
}

const char* ObRecoverTableStatus::get_str() const
{
  const char *str = "UNKNOWN";
  const char *status_strs[] = {
    "PREPARE",
    "RECOVERING",
    "RESTORE_AUX_TENANT",
    "ACTIVE_AUX_TENANT",
    "PRECHECK_IMPORT",
    "GEN_IMPORT_JOB",
    "IMPORTING",
    "CANCELING",
    "COMPLETED",
    "FAILED",
  };

  STATIC_ASSERT(MAX_STATUS == ARRAYSIZEOF(status_strs), "status count mismatch");
  if (status_ < PREPARE || status_ >= MAX_STATUS) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid status", K(status_));
  } else {
    str = status_strs[status_];
  }
  return str;
}

int ObRecoverTableStatus::set_status(const char *str)
{
  int ret = OB_SUCCESS;
  ObString s(str);
  const char *status_strs[] = {
    "PREPARE",
    "RECOVERING",
    "RESTORE_AUX_TENANT",
    "ACTIVE_AUX_TENANT",
    "PRECHECK_IMPORT",
    "GEN_IMPORT_JOB",
    "IMPORTING",
    "CANCELING",
    "COMPLETED",
    "FAILED",
  };
  const int64_t count = ARRAYSIZEOF(status_strs);
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status can't empty", K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == s.case_compare(status_strs[i])) {
        status_ = static_cast<Status>(i);
      }
    }
  }
  return ret;
}

ObRecoverTableStatus ObRecoverTableStatus::get_sys_next_status(const ObRecoverTableStatus &cur_status)
{
  ObRecoverTableStatus ret;
  switch(cur_status) {
    case ObRecoverTableStatus::Status::PREPARE: {
      ret = ObRecoverTableStatus::Status::RECOVERING;
      break;
    }
    case ObRecoverTableStatus::Status::RECOVERING: {
      ret = ObRecoverTableStatus::Status::COMPLETED;
      break;
    }
    case ObRecoverTableStatus::Status::COMPLETED: {
      ret = ObRecoverTableStatus::Status::COMPLETED;
      break;
    }
    default: {
      break;
    }
  }
  return ret;
}

ObRecoverTableStatus ObRecoverTableStatus::get_user_next_status(const ObRecoverTableStatus &cur_status)
{
  ObRecoverTableStatus ret;
  switch(cur_status) {
    case ObRecoverTableStatus::Status::PREPARE: {
      ret = ObRecoverTableStatus::Status::RESTORE_AUX_TENANT;
      break;
    }
    case ObRecoverTableStatus::Status::RESTORE_AUX_TENANT: {
      ret = ObRecoverTableStatus::Status::ACTIVE_AUX_TENANT;
      break;
    }
    case ObRecoverTableStatus::Status::ACTIVE_AUX_TENANT: {
      ret = ObRecoverTableStatus::Status::GEN_IMPORT_JOB;
      break;
    }
    case ObRecoverTableStatus::Status::GEN_IMPORT_JOB: {
      ret = ObRecoverTableStatus::Status::IMPORTING;
      break;
    }
    case ObRecoverTableStatus::Status::IMPORTING: {
      ret = ObRecoverTableStatus::Status::COMPLETED;
      break;
    }
    case ObRecoverTableStatus::Status::COMPLETED: {
      ret = ObRecoverTableStatus::Status::COMPLETED;
      break;
    }
    default: {
      break;
    }
  }
  return ret;
}


int ObRecoverTableJob::assign(const ObRecoverTableJob &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(that));
  } else if (OB_FAIL(set_aux_tenant_name(that.get_aux_tenant_name()))) {
    LOG_WARN("failed to set aux tenant name", K(ret));
  } else if (OB_FAIL(set_target_tenant_name(that.get_target_tenant_name()))) {
    LOG_WARN("failed to set target tenant name", K(ret));
  } else if (OB_FAIL(set_restore_option(that.get_restore_option()))) {
    LOG_WARN("failed to set restore option", K(ret));
  } else if (OB_FAIL(set_backup_dest(that.get_backup_dest()))) {
    LOG_WARN("failed to set backup dest", K(ret));
  } else if (OB_FAIL(set_backup_passwd(that.get_backup_passwd()))) {
    LOG_WARN("failed to set backup passwd", K(ret));
  } else if (OB_FAIL(set_external_kms_info(that.get_external_kms_info()))) {
    LOG_WARN("failed to set external kms", K(ret));
  } else if (OB_FAIL(multi_restore_path_list_.assign(that.get_multi_restore_path_list()))) {
    LOG_WARN("failed to assign multi restore path", K(ret));
  } else if (OB_FAIL(import_arg_.assign(that.get_import_arg()))) {
    LOG_WARN("failed to assign import arg", K(ret));
  } else if (OB_FAIL(set_description(that.get_description()))) {
    LOG_WARN("failed to set description", K(ret));
  } else {
    set_tenant_id(that.get_tenant_id());
    set_job_id(that.get_job_id());
    set_initiator_job_id(that.get_initiator_job_id());
    set_initiator_tenant_id(that.get_initiator_tenant_id());
    set_start_ts(that.get_start_ts());
    set_end_ts(that.get_end_ts());
    set_status(that.get_status());
    set_target_tenant_id(that.get_target_tenant_id());
    set_restore_scn(that.get_restore_scn());
    set_result(that.get_result());
  }
  return ret;
}

int ObRecoverTableJob::Key::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

void ObRecoverTableJob::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_ = 0;
  initiator_tenant_id_ = OB_INVALID_TENANT_ID;
  initiator_job_id_ = 0;
  start_ts_ = 0;
  end_ts_ = 0;
  status_ = ObRecoverTableStatus::Status::MAX_STATUS;
  aux_tenant_name_.reset();
  target_tenant_name_.reset();
  target_tenant_id_ = OB_INVALID_TENANT_ID;
  restore_scn_.reset();
  restore_option_.reset();
  backup_dest_.reset();
  backup_passwd_.reset();
  external_kms_info_.reset();
  result_.reset();
  import_arg_.reset();
  multi_restore_path_list_.reset();
  description_.reset();
}

int ObRecoverTableJob::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }
  return ret;
}

int ObRecoverTableJob::get_pkey(Key &key) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pkey", K(ret), KPC(this));
  } else {
    key.tenant_id_ = get_tenant_id();
    key.job_id_ = get_job_id();
  }
  return ret;
}


int ObRecoverTableJob::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode name_case_mode;
  bool import_all = false;
  const int64_t OB_MAX_RESULT_BUF_LEN = 12;
  char result_buf[OB_MAX_RESULT_BUF_LEN] = "";
  ObImportResult::Comment comment;
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id_, uint64_t);
  if (FAILEDx(ObImportTableUtil::get_tenant_name_case_mode(tenant_id_, name_case_mode))) {
    LOG_WARN("failed to get tenant name case mode", K(ret), K(tenant_id_));
  }
  EXTRACT_INT_FIELD_MYSQL(result, "initiator_tenant_id", initiator_tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "target_tenant_id", target_tenant_id_, uint64_t);
  PARSE_INT_VALUE(job_id)
  PARSE_INT_VALUE(initiator_job_id)
  PARSE_INT_VALUE(start_ts)
  PARSE_INT_VALUE(end_ts)
  EXTRACT_BOOL_FIELD_MYSQL(result, "import_all", import_all);
  if (OB_SUCC(ret) && import_all) {
    import_arg_.get_import_table_arg().set_import_all();
  }
  if (OB_SUCC(ret)) {
    int64_t real_length = 0;
    char status_str[OB_DEFAULT_STATUS_LENTH] = "";
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(status_.set_status(status_str))) {
        LOG_WARN("failed to set status", K(ret), K(status_str));
      }
    }
  }

  PARSE_STR_VALUE(aux_tenant_name)
  PARSE_STR_VALUE(target_tenant_name)

  if (OB_SUCC(ret)) {
    uint64_t restore_scn = 0;
    EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_RESTORE_SCN, restore_scn, uint64_t);
    if (FAILEDx(restore_scn_.convert_for_inner_table_field(restore_scn))) {
      LOG_WARN("failed to conver for inner table", K(ret), K(restore_scn));
    }
  }

  PARSE_STR_VALUE(restore_option)
  PARSE_STR_VALUE(backup_dest)
  PARSE_STR_VALUE(backup_passwd)
  PARSE_STR_VALUE(external_kms_info)
  PARSE_STR_VALUE(description)

  int64_t real_length = 0;
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_RESULT, result_buf, OB_MAX_RESULT_BUF_LEN, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, OB_STR_COMMENT, comment.ptr(), comment.capacity(), real_length);
  if (OB_SUCC(ret)) {
    bool is_succeed = true;
    if (0 == STRCMP("FAILED", result_buf)) {
      is_succeed = false;
    }
    result_.set_result(is_succeed, comment);
  }

  if (OB_SUCC(ret)) {
    ObString str;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, OB_STR_BACKUP_SET_LIST, str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(multi_restore_path_list_.backup_set_list_assign_with_format_str(str))) {
      LOG_WARN("fail to assign backup set list", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, OB_STR_BACKUP_PIECE_LIST, str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(multi_restore_path_list_.backup_piece_list_assign_with_format_str(str))) {
      LOG_WARN("fail to assign backup set list", KR(ret), K(str));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportDatabaseArray &import_database_array = import_arg_.get_import_table_arg().get_import_database_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_db_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_database_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize database array", KR(ret), K(str));
    } else {
      LOG_INFO("import database array hex format deserialize", K(str), K(import_database_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportTableArray &import_table_array = import_arg_.get_import_table_arg().get_import_table_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_table_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_table_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize table array", KR(ret), K(str));
    } else {
      LOG_INFO("import table array hex format deserialize", K(str), K(import_table_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObImportPartitionArray &import_part_array = import_arg_.get_import_table_arg().get_import_partition_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_partition_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(import_part_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize partition array", KR(ret), K(str));
    } else {
      LOG_INFO("import partition array hex format deserialize", K(str), K(import_part_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapDatabaseArray &remap_database_array = import_arg_.get_remap_table_arg().get_remap_database_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_db_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_database_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap database array", KR(ret), K(str));
    } else {
      LOG_INFO("remap database array hex format deserialize", K(str), K(remap_database_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTableArray &remap_table_array = import_arg_.get_remap_table_arg().get_remap_table_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_table_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_table_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap table array", KR(ret), K(str));
    } else {
      LOG_INFO("remap table array hex format deserialize", K(str), K(remap_table_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapPartitionArray &remap_part_array = import_arg_.get_remap_table_arg().get_remap_partition_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_partition_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_part_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap partition array", KR(ret), K(str));
    } else {
      LOG_INFO("remap partition array hex format deserialize", K(str), K(remap_part_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTablegroupArray &remap_tablegroup_array = import_arg_.get_remap_table_arg().get_remap_tablegroup_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_tablegroup_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_tablegroup_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap tablegroup array", KR(ret), K(str));
    } else {
      LOG_INFO("remap tablegroup array hex format deserialize", K(str), K(remap_tablegroup_array));
    }
  }

  if (OB_SUCC(ret)) {
    ObString str;
    int64_t pos = 0;
    share::ObRemapTablespaceArray &remap_tablespace_array = import_arg_.get_remap_table_arg().get_remap_tablespace_array();
    EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "hex_remap_tablespace_list", str);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(remap_tablespace_array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize remap tablespace array", KR(ret), K(str));
    } else {
      LOG_INFO("remap tablespace array hex format deserialize", K(str), K(remap_tablespace_array));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("parse recover table job succeed", KPC(this));
  }

  return ret;
}

int ObRecoverTableJob::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  const bool import_all = import_arg_.get_import_table_arg().is_import_all();
  if (OB_FAIL(fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status_.get_str()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_RESTORE_SCN, restore_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add restore scn", K(ret));
  } else if (OB_FAIL(dml.add_column("import_all", import_all))) {
    LOG_WARN("failed to add column", K(ret));
  }

  FILL_INT_COLUMN(initiator_tenant_id)
  FILL_INT_COLUMN(initiator_job_id)
  FILL_INT_COLUMN(start_ts)
  FILL_INT_COLUMN(end_ts)
  FILL_STR_COLUMN(aux_tenant_name)
  FILL_STR_COLUMN(target_tenant_name)
  FILL_INT_COLUMN(target_tenant_id)
  FILL_STR_COLUMN(restore_option)
  FILL_STR_COLUMN(backup_dest)
  FILL_STR_COLUMN(backup_passwd)
  FILL_STR_COLUMN(external_kms_info)
  FILL_STR_COLUMN(description)

  if (OB_SUCC(ret) && status_.is_finish()) {
    if (OB_FAIL(dml.add_column(OB_STR_RESULT, result_.get_result_str()))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, result_.get_comment()))) {
      LOG_WARN("failed to add column", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator;
    ObString backup_set_list_;
    ObString backup_piece_list_;

    ObString db_list_;
    ObString table_list_;
    ObString partition_list_;
    ObString remap_db_list_;
    ObString remap_table_list_;
    ObString remap_partition_list_;
    ObString remap_tablegroup_list_;
    ObString remap_tablespace_list_;

    ObString hex_db_list_;
    ObString hex_table_list_;
    ObString hex_partition_list_;
    ObString hex_remap_db_list_;
    ObString hex_remap_table_list_;
    ObString hex_remap_partition_list_;
    ObString hex_remap_tablegroup_list_;
    ObString hex_remap_tablespace_list_;

    const share::ObImportTableArg &import_table_arg = import_arg_.get_import_table_arg();
    const share::ObImportRemapArg &remap_table_arg = import_arg_.get_remap_table_arg();

    if (OB_FAIL(multi_restore_path_list_.get_backup_set_list_format_str(allocator, backup_set_list_))) {
      LOG_WARN("fail to get format str", KR(ret), K(multi_restore_path_list_));
    } else if (OB_FAIL(multi_restore_path_list_.get_backup_piece_list_format_str(allocator, backup_piece_list_))) {
      LOG_WARN("fail to get format str", KR(ret), K(multi_restore_path_list_));
    } else if (OB_FAIL(import_table_arg.get_db_list_format_str(allocator, db_list_))) {
      LOG_WARN("fail to get db list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_table_list_format_str(allocator, table_list_))) {
      LOG_WARN("fail to get table list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_partition_list_format_str(allocator, partition_list_))) {
      LOG_WARN("fail to get partition list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_db_list_format_str(allocator, remap_db_list_))) {
      LOG_WARN("fail to get remap db list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_table_list_format_str(allocator, remap_table_list_))) {
      LOG_WARN("fail to get remap table list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_partition_list_format_str(allocator, remap_partition_list_))) {
      LOG_WARN("fail to get remap partition list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablegroup_list_format_str(allocator, remap_tablegroup_list_))) {
      LOG_WARN("fail to get remap tablegroup list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablespace_list_format_str(allocator, remap_tablespace_list_))) {
      LOG_WARN("fail to get remap tablespace list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(import_table_arg.get_db_list_hex_format_str(allocator, hex_db_list_))) {
      LOG_WARN("fail to get hex db list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_table_list_hex_format_str(allocator, hex_table_list_))) {
      LOG_WARN("fail to get hex table list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(import_table_arg.get_partition_list_hex_format_str(allocator, hex_partition_list_))) {
      LOG_WARN("fail to get hex partition list format str", KR(ret), K(import_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_db_list_hex_format_str(allocator, hex_remap_db_list_))) {
      LOG_WARN("fail to get remap hex db list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_table_list_hex_format_str(allocator, hex_remap_table_list_))) {
      LOG_WARN("fail to get remap hex table list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_partition_list_hex_format_str(allocator, hex_remap_partition_list_))) {
      LOG_WARN("fail to get remap hex partition list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablegroup_list_hex_format_str(allocator, hex_remap_tablegroup_list_))) {
      LOG_WARN("fail to get remap hex tablegroup list format str", KR(ret), K(remap_table_arg));
    } else if (OB_FAIL(remap_table_arg.get_remap_tablespace_list_hex_format_str(allocator, hex_remap_tablespace_list_))) {
      LOG_WARN("fail to get remap hex tablespace list format str", KR(ret), K(remap_table_arg));
    } else {
      FILL_STR_COLUMN(backup_set_list)
      FILL_STR_COLUMN(backup_piece_list)
      FILL_STR_COLUMN(db_list)
      FILL_STR_COLUMN(table_list)
      FILL_STR_COLUMN(partition_list)
      FILL_STR_COLUMN(remap_db_list)
      FILL_STR_COLUMN(remap_table_list)
      FILL_STR_COLUMN(remap_partition_list)
      FILL_STR_COLUMN(remap_tablegroup_list)
      FILL_STR_COLUMN(remap_tablespace_list)

      FILL_STR_COLUMN(hex_db_list)
      FILL_STR_COLUMN(hex_table_list)
      FILL_STR_COLUMN(hex_partition_list)
      FILL_STR_COLUMN(hex_remap_db_list)
      FILL_STR_COLUMN(hex_remap_table_list)
      FILL_STR_COLUMN(hex_remap_partition_list)
      FILL_STR_COLUMN(hex_remap_tablegroup_list)
      FILL_STR_COLUMN(hex_remap_tablespace_list)
    }
  }

  return ret;
}

#undef PARSE_INT_VALUE
#undef PARSE_UINT_VALUE
#undef PARSE_STR_VALUE
#undef FILL_INT_COLUMN
#undef FILL_UINT_COLUMN
#undef FILL_STR_COLUMN
#undef FILL_HEX_STR_COLUMN
