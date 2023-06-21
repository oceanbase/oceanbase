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
#include "share/restore/ob_restore_persist_helper.h"
#include "share/restore/ob_physical_restore_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_smart_var.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace sqlclient;

/**
 * ------------------------------ObRestoreJobPersistKey---------------------
 */
bool ObRestoreJobPersistKey::is_pkey_valid() const
{
  return (is_sys_tenant(tenant_id_) || is_user_tenant(tenant_id_))
         && job_id_ > 0;
}

int ObRestoreJobPersistKey::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
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

bool ObInitiatorRestoreJobPersistKey::is_pkey_valid() const
{
  return (is_sys_tenant(initiator_tenant_id_) || is_user_tenant(initiator_tenant_id_))
         && initiator_job_id_ > 0;
}

int ObInitiatorRestoreJobPersistKey::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_INITIATOR_TENANT_ID, initiator_tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_INITIATOR_JOB_ID, initiator_job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

/**
 * ------------------------------ObRestoreProgressPersistInfo---------------------
 */
// Return if both primary key and value are valid.
bool ObRestoreProgressPersistInfo::is_valid() const
{
  return key_.is_pkey_valid();
}

// Parse row from the sql result, the result has full columns.
int ObRestoreProgressPersistInfo::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  uint64_t restore_scn = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, key_.job_id_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_RESTORE_SCN, restore_scn, uint64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_COUNT, ls_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_LS_COUNT, finish_ls_count_, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TABLET_COUNT, tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_BYTES, total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_BYTES, finish_bytes_, int64_t);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(restore_scn_.convert_for_inner_table_field(restore_scn))) {
    LOG_WARN("fail to set restore scn", K(ret), K(restore_scn));
  }
  return ret;
}

// Fill primary key and value to dml.
int ObRestoreProgressPersistInfo::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(key_.fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_RESTORE_SCN, restore_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_LS_COUNT, ls_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_LS_COUNT, finish_ls_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TABLET_COUNT, tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TOTAL_BYTES, total_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_BYTES, finish_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } 

  return ret;
}


/**
 * ------------------------------ObLSRestoreJobPersistKey---------------------
 */
ObRestoreJobPersistKey ObLSRestoreJobPersistKey::generate_restore_job_key() const
{
  ObRestoreJobPersistKey job_key;
  job_key.tenant_id_ = tenant_id_;
  job_key.job_id_ = job_id_;
  return job_key;
}

bool ObLSRestoreJobPersistKey::is_pkey_valid() const
{
  return (is_sys_tenant(tenant_id_) || is_user_tenant(tenant_id_))
         && job_id_ > 0;
}

int ObLSRestoreJobPersistKey::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";

  if (!is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(*this));
  } else if (!addr_.ip_to_string(ip, OB_MAX_SERVER_ADDR_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to dump ip to string", K(ret), K(*this));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, job_id_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_LS_ID, ls_id_.id()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_SEVER_IP, ip))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_SERVER_PORT, addr_.get_port()))) {
    LOG_WARN("failed to add column", K(ret));
  } 

  return ret;
}


/**
 * ------------------------------ObLSHisRestorePersistInfo---------------------
 */
// Return if both primary key and value are valid.
bool ObLSHisRestorePersistInfo::is_valid() const
{
  return key_.is_pkey_valid();
}

// Parse row from the sql result, the result has full columns.
int ObLSHisRestorePersistInfo::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("read from history table is not allowed.", K(ret));
  return ret;
}

// Fill primary key and value to dml.
int ObLSHisRestorePersistInfo::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char trace_id[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";

  if (OB_FAIL(key_.fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FALSE_IT(trace_id_.to_string(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_RESTORE_SCN, restore_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_REPLAY_SCN, start_replay_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_LAST_REPLAY_SCN, last_replay_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TABLET_COUNT, tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TOTAL_BYTES, total_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_BYTES, finish_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TRACE_ID, trace_id))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, result_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}


/**
 * ------------------------------ObLSRestoreProgressPersistInfo---------------------
 */
int ObLSRestoreProgressPersistInfo::generate_his_progress(ObLSHisRestorePersistInfo &his) const
{
  int ret = OB_SUCCESS;
  his.key_ = key_;
  his.restore_scn_ = restore_scn_;
  his.start_replay_scn_ = start_replay_scn_;
  his.last_replay_scn_ = last_replay_scn_;
  his.tablet_count_ = tablet_count_;
  his.finish_tablet_count_ = finish_tablet_count_;
  his.total_bytes_ = total_bytes_;
  his.finish_bytes_ = finish_bytes_;
  his.result_ = result_;
  his.trace_id_.set(trace_id_);
  if (OB_FAIL(his.comment_.assign(comment_.ptr()))) {
    LOG_WARN("fail to assign comment", K(ret), K(comment_));
  }
  return ret;
}

// Return if both primary key and value are valid.
bool ObLSRestoreProgressPersistInfo::is_valid() const
{
  return key_.is_pkey_valid();
}

int ObLSRestoreProgressPersistInfo::assign(const ObLSRestoreProgressPersistInfo &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(that));
  } else {
    key_.ls_id_ = that.key_.ls_id_;
    key_.job_id_ = that.key_.job_id_;
    key_.addr_ = that.key_.addr_;
    key_.tenant_id_ = that.key_.tenant_id_;
    status_ = that.status_;
    restore_scn_ = that.restore_scn_;
    start_replay_scn_ = that.start_replay_scn_;
    last_replay_scn_ = that.last_replay_scn_;
    tablet_count_ = that.tablet_count_;
    finish_tablet_count_ = that.finish_tablet_count_;
    total_bytes_ = that.total_bytes_;
    finish_bytes_ = that.finish_bytes_;
    trace_id_ = that.trace_id_;
    result_ = that.result_;
    if (OB_FAIL(comment_.assign(that.comment_))) {
      LOG_WARN("fail to assign comment", K(ret));
    }
  }
  return ret;
}

// Parse row from the sql result, the result has full columns.
int ObLSRestoreProgressPersistInfo::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int32_t port = 0;
  int32_t status = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  char comment[MAX_TABLE_COMMENT_LENGTH] = "";
  int64_t ls_id = 0;
  int64_t real_length = 0;
  uint64_t restore_scn = 0;
  uint64_t start_replay_scn = 0;
  uint64_t last_replay_scn = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, key_.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_ID, ls_id, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_SEVER_IP, ip, OB_MAX_SERVER_ADDR_SIZE, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_SERVER_PORT, port, int32_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_STATUS, status, int32_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_RESTORE_SCN, restore_scn, uint64_t);

  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_REPLAY_SCN, start_replay_scn, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_LAST_REPLAY_SCN, last_replay_scn, uint64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TABLET_COUNT, tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_BYTES, total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_BYTES, finish_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESULT, result_, int32_t);

  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TRACE_ID, trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_COMMENT, comment, MAX_TABLE_COMMENT_LENGTH, real_length);
  if (OB_FAIL(ret)) {
  } else if (!key_.addr_.set_ip_addr(ip, port)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set addr", K(ret), K(ip), K(port));
  } else if (OB_FALSE_IT(key_.ls_id_ = ObLSID(ls_id))) {
  } else if (OB_FAIL(trace_id_.set(trace_id))) {
    LOG_WARN("failed to set trace id", K(ret), K(trace_id));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("fail to assign comment", K(ret));
  } else if (OB_FAIL(status_.set_status(status))) {
    LOG_WARN("failed to set status", K(ret), K(status));
  } else if (OB_FAIL(restore_scn_.convert_for_inner_table_field(restore_scn))) {
    LOG_WARN("failed to set restore scn", K(ret), K(restore_scn));
  } else if (OB_FAIL(start_replay_scn_.convert_for_inner_table_field(start_replay_scn))) {
    LOG_WARN("failed to set start_replay scn", K(ret), K(restore_scn));
  } else if (OB_FAIL(last_replay_scn_.convert_for_inner_table_field(last_replay_scn))) {
    LOG_WARN("failed to set last_replay scn", K(ret), K(restore_scn));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse restore progress failed", K(ret), K(*this));
  }
  return ret;
}

// Fill primary key and value to dml.
int ObLSRestoreProgressPersistInfo::fill_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  char trace_id[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";

  if (OB_FAIL(key_.fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } else if (OB_FALSE_IT(trace_id_.to_string(trace_id, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(dml.add_column(OB_STR_STATUS, status_.get_status()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_RESTORE_SCN, restore_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_START_REPLAY_SCN, start_replay_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_uint64_column(OB_STR_LAST_REPLAY_SCN, last_replay_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TABLET_COUNT, tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TOTAL_BYTES, total_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_FINISH_BYTES, finish_bytes_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_TRACE_ID, trace_id))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_RESULT, result_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_column(OB_STR_COMMENT, comment_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}


/**
 * ------------------------------ObHisRestoreJobPersistInfo---------------------
 */
const char *STATUS_STR[] = {"SUCCESS", "FAIL"};
const char *ObHisRestoreJobPersistInfo::get_status_str() const
{
  const char *ptr = STATUS_STR[0];
  if (0 != status_) {
    ptr = STATUS_STR[1];
  }

  return ptr;
}
int ObHisRestoreJobPersistInfo::get_status(const ObString &str_str) const
{
  int status = -1;
  for (int32_t i = 0; i < ARRAYSIZEOF(STATUS_STR); ++i) {
    if (0 == str_str.case_compare(STATUS_STR[i])) {
      status = i;
      break;
    }
  }
  return status;
}

// Return if both primary key and value are valid.
bool ObHisRestoreJobPersistInfo::is_valid() const
{
  return key_.is_pkey_valid();
}

// Parse row from the sql result, the result has full columns.
int ObHisRestoreJobPersistInfo::parse_from(common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  ObString status_str;
  uint64_t restore_scn = 0;
 #define RETRIEVE_STR_VALUE(COLUMN_NAME)                       \
  if (OB_SUCC(ret)) {                                              \
    ObString value;                                              \
    EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, #COLUMN_NAME, value, true, false, value);\
    if (FAILEDx(COLUMN_NAME##_.assign(value))) {               \
      LOG_WARN("failed to set column value", KR(ret), K(value)); \
    }                                                            \
  }

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, key_.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_TENANT_ID, initiator_tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INITIATOR_JOB_ID, initiator_job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RESTORE_TENANT_ID, restore_tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_BACKUP_TENANT_ID, backup_tenant_id_, uint64_t);

  EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_RESTORE_SCN, restore_scn, uint64_t);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_scn_.convert_for_inner_table_field(restore_scn))) {
      LOG_WARN("failed to set restore scn", K(ret), K(restore_scn));
    }
  }

  EXTRACT_INT_FIELD_MYSQL(result, "backup_cluster_version", backup_cluster_version_, int64_t);
  //TODO start time and finish time

  RETRIEVE_STR_VALUE(restore_tenant_name);
  RETRIEVE_STR_VALUE(backup_dest);
  RETRIEVE_STR_VALUE(restore_option);
  RETRIEVE_STR_VALUE(backup_piece_list);
  RETRIEVE_STR_VALUE(backup_set_list);
  RETRIEVE_STR_VALUE(description);
  RETRIEVE_STR_VALUE(comment);

  //TODO restore_type
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, OB_STR_STATUS, status_str, true, false, status_str);
  if (OB_SUCC(ret)) {
    status_ = get_status(status_str);
  }

  //TODO
  //table_list_, remap_table_list_, database_list_, remap_database_list_
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_LS_COUNT, ls_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_LS_COUNT, finish_ls_count_, int64_t);

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TABLET_COUNT, tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_TABLET_COUNT, finish_tablet_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TOTAL_BYTES, total_bytes_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_FINISH_BYTES, finish_bytes_, int64_t);


  return ret;
}

// Fill primary key and value to dml.
int ObHisRestoreJobPersistInfo::fill_dml(ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;

  const char *status_str = get_status_str();

#define ADD_COMMON_COLUMN_DML(column_name) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_FAIL(dml.add_column(#column_name, column_name##_))) { \
      LOG_WARN("failed to add column", K(ret)); \
    } \
  }

#define ADD_UINT_COLUMN_DML(column_name) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_FAIL(dml.add_uint64_column(#column_name, column_name##_))) { \
      LOG_WARN("failed to add column", K(ret)); \
    } \
  }

#define ADD_FIXED_STR_COLUMN_DML(column_name) \
  if (OB_SUCC(ret)) \
  { \
    ObString column_value = column_name##_.str(); \
    if (OB_FAIL(dml.add_column(#column_name, column_value))) { \
      LOG_WARN("failed to add column", K(ret)); \
    } \
  }

#define ADD_LONG_STR_COLUMN_DML(column_name) \
  if (OB_SUCC(ret)) \
  { \
    ObString column_value = column_name##_.string(); \
    if (OB_FAIL(dml.add_column(#column_name, column_value))) { \
      LOG_WARN("failed to add column", K(ret)); \
    } \
  }

#define ADD_COMMON_COLUMN_DML_WITH_VALUE(column_name, column_value) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_FAIL(dml.add_column(#column_name, column_value))) { \
      LOG_WARN("failed to add column", K(ret)); \
    } \
  }

  if (OB_FAIL(key_.fill_pkey_dml(dml))) {
    LOG_WARN("failed to fill key", K(ret));
  } 

  if (FAILEDx(dml.add_time_column("start_time", start_time_))) {
    LOG_WARN("failed to add column", KR(ret), K(start_time_));
  } else if (OB_FAIL(dml.add_time_column("finish_time", finish_time_))) {
    LOG_WARN("failed to add column", KR(ret), K(finish_time_));
  }
  ADD_COMMON_COLUMN_DML(initiator_job_id);
  ADD_COMMON_COLUMN_DML(initiator_tenant_id);
  ADD_COMMON_COLUMN_DML(restore_type);
  ADD_FIXED_STR_COLUMN_DML(restore_tenant_name);
  ADD_FIXED_STR_COLUMN_DML(backup_tenant_name);
  ADD_FIXED_STR_COLUMN_DML(backup_cluster_name);
  ADD_COMMON_COLUMN_DML(restore_tenant_id);
  ADD_COMMON_COLUMN_DML(backup_tenant_id);
  ADD_LONG_STR_COLUMN_DML(backup_dest);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml.add_uint64_column("restore_scn", restore_scn_.get_val_for_inner_table_field()))) {
      LOG_WARN("failed to add column", K(ret));
    }
  }
  ADD_LONG_STR_COLUMN_DML(restore_option);

  ADD_LONG_STR_COLUMN_DML(table_list);
  ADD_LONG_STR_COLUMN_DML(remap_table_list);
  ADD_LONG_STR_COLUMN_DML(database_list);
  ADD_LONG_STR_COLUMN_DML(remap_database_list);
  ADD_LONG_STR_COLUMN_DML(backup_piece_list);
  ADD_LONG_STR_COLUMN_DML(backup_set_list);
  ADD_COMMON_COLUMN_DML(backup_cluster_version);

  ADD_COMMON_COLUMN_DML(ls_count);
  ADD_COMMON_COLUMN_DML(finish_ls_count);
  ADD_COMMON_COLUMN_DML(tablet_count);
  ADD_COMMON_COLUMN_DML(finish_tablet_count);
  ADD_COMMON_COLUMN_DML(total_bytes);
  ADD_COMMON_COLUMN_DML(finish_bytes);

  ADD_COMMON_COLUMN_DML_WITH_VALUE(status, status_str);

  ADD_LONG_STR_COLUMN_DML(description);
  ADD_FIXED_STR_COLUMN_DML(comment);
  
  return ret;
}

int ObHisRestoreJobPersistInfo::init_with_job_process(
    const share::ObPhysicalRestoreJob &job,
    const ObRestoreProgressPersistInfo &progress)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.is_valid() || !progress.is_valid()
  || job.get_restore_key() != progress.key_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job), K(progress));
  } else if (OB_FAIL(init_with_job(job))) {
    LOG_WARN("failed to init job", KR(ret), K(job));
  } else {
    ls_count_ = progress.ls_count_;
    finish_ls_count_ = progress.finish_ls_count_;
    tablet_count_ = progress.tablet_count_;
    finish_tablet_count_ = progress.finish_tablet_count_;
    finish_bytes_ = progress.finish_bytes_;
    total_bytes_ = progress.total_bytes_;
  }
  return ret;
}

int ObHisRestoreJobPersistInfo::init_with_job(const share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(restore_tenant_name_.assign(job.get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", KR(ret), K(job));
  } else if (OB_FAIL(backup_cluster_name_.assign(job.get_backup_cluster_name()))) {
    LOG_WARN("failed to assign backup cluster name", KR(ret), K(job));
  } else if (OB_FAIL(backup_tenant_name_.assign(job.get_backup_tenant_name()))) {
    LOG_WARN("failed to assign backup tenant name", KR(ret), K(job));
  } else if (OB_FAIL(backup_dest_.assign(job.get_backup_dest()))) {
    LOG_WARN("failed to assign back dest", KR(ret), K(job));
  } else if (OB_FAIL(restore_option_.assign(job.get_restore_option()))) {
    LOG_WARN("failed to assign restore option", KR(ret), K(job));
  } else if (OB_FAIL(description_.assign(job.get_description()))) {
    LOG_WARN("failed to assign description", KR(ret), K(job));
  } else if (OB_FAIL(comment_.assign(job.get_comment()))) {
    LOG_WARN("failed to assign commit", KR(ret), K(job));
  } else {
    ObArenaAllocator allocator;
    ObString backup_set_list;
    ObString backup_piece_list;
    const ObPhysicalRestoreBackupDestList &dest_list =
        job.get_multi_restore_path_list();
    if (OB_FAIL(dest_list.get_backup_set_list_format_str(allocator,
                                                         backup_set_list))) {
      LOG_WARN("fail to get format str", KR(ret), K(dest_list));
    } else if (OB_FAIL(dest_list.get_backup_piece_list_format_str(
                   allocator, backup_piece_list))) {
      LOG_WARN("fail to get format str", KR(ret), K(dest_list));
    } else if (OB_FAIL(backup_piece_list_.assign(backup_piece_list))) {
      LOG_WARN("failed to assign backup piece list", KR(ret),
               K(backup_piece_list_));
    } else if (OB_FAIL(backup_set_list_.assign(backup_set_list))) {
      LOG_WARN("failed to assign backup set list", KR(ret), K(backup_set_list));
    }
  }
  
  if (OB_SUCC(ret)) {
    key_ = job.get_restore_key();
    initiator_job_id_ = job.get_initiator_job_id();
    initiator_tenant_id_ = job.get_initiator_tenant_id();
    start_time_ = job.get_restore_start_ts();
    finish_time_ = ObTimeUtility::current_time();
    restore_type_ = job.get_restore_type();
    restore_tenant_id_ = job.get_tenant_id();
    backup_tenant_id_ = job.get_backup_tenant_id();
    restore_scn_ = job.get_restore_scn();
    backup_cluster_version_ = job.get_source_cluster_version(); 
    
    if (PHYSICAL_RESTORE_SUCCESS == job.get_status()) {
      set_success();
    } else {
      set_fail();
    }
  }
  return ret; 
}
int ObHisRestoreJobPersistInfo::init_initiator_job_history(
    const share::ObPhysicalRestoreJob &job,
    const ObHisRestoreJobPersistInfo &job_history)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.is_valid() || !job_history.is_valid()
  || job.get_job_id() != job_history.initiator_job_id_
  || job.get_restore_key().tenant_id_ != job_history.initiator_tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job), K(job_history));
  } else if (OB_FAIL(init_with_job(job))) {
    LOG_WARN("failed to init with job", KR(ret), K(job));
  } else if (OB_FAIL(comment_.assign(job_history.comment_.ptr()))) {
    LOG_WARN("failed to assign comment", K(ret));
  } else { 
    ls_count_ = job_history.ls_count_;
    finish_ls_count_ = job_history.finish_ls_count_;
    tablet_count_ = job_history.tablet_count_;
    finish_tablet_count_ = job_history.finish_tablet_count_;
    total_bytes_ = job_history.total_bytes_;
    finish_bytes_ = job_history.finish_bytes_;
  }
  return ret;
}

/**
 * ------------------------------ObRestorePersistHelper---------------------
 */
ObRestorePersistHelper::ObRestorePersistHelper()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{

}

uint64_t ObRestorePersistHelper::get_exec_tenant_id() const
{
  return gen_meta_tenant_id(tenant_id_);
}

int ObRestorePersistHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObRestorePersistHelper::insert_initial_restore_progress(
    common::ObISQLClient &proxy, const ObRestoreProgressPersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator restore_progress_table_operator;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.insert_or_update_row(proxy, persist_info, affected_rows))) {
    LOG_WARN("failed to insert initial restore progress", K(ret));
  }

  return ret;
}

int ObRestorePersistHelper::get_restore_process(
      common::ObISQLClient &proxy,
      const ObRestoreJobPersistKey &key,
      ObRestoreProgressPersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  ObInnerTableOperator table_op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(table_op.get_row(proxy, false, key, persist_info))) {
    LOG_WARN("failed to get persist info", KR(ret), K(key));
  }
  return ret;
}

  //__all_restore_job_history
int ObRestorePersistHelper::insert_restore_job_history(
       common::ObISQLClient &proxy, const ObHisRestoreJobPersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  ObInnerTableOperator table_op;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op.init(OB_ALL_RESTORE_JOB_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(table_op.insert_row(proxy, persist_info, affected_rows))) {
    LOG_WARN("failed to get persist info", KR(ret), K(persist_info));
  }
  return ret;
}
int ObRestorePersistHelper::get_restore_job_history(
    common::ObISQLClient &proxy, const int64_t initiator_job,
    const uint64_t initiator_tenant_id,
    ObHisRestoreJobPersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  ObInnerTableOperator table_op;
  ObInitiatorRestoreJobPersistKey restore_key;
  restore_key.initiator_job_id_ = initiator_job;
  restore_key.initiator_tenant_id_ = initiator_tenant_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(table_op.init(OB_ALL_RESTORE_JOB_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(table_op.get_row(proxy, false, restore_key, persist_info))) {
    LOG_WARN("failed to get persist info", KR(ret), K(restore_key));
  }
  return ret; 
}

int ObRestorePersistHelper::insert_initial_ls_restore_progress(
    ObISQLClient &proxy, const ObLSRestoreProgressPersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator ls_restore_progress_table_operator;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.insert_or_update_row(proxy, persist_info, affected_rows))) {
    LOG_WARN("failed to insert initial ls restore progress", K(ret));
  }

  return ret;
}

int ObRestorePersistHelper::record_ls_his_restore_progress(
    common::ObISQLClient &proxy, const ObLSHisRestorePersistInfo &persist_info) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator ls_his_restore_table_operator;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(ls_his_restore_table_operator.init(OB_ALL_LS_RESTORE_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init ls his restore progress table", K(ret));
  } else if (OB_FAIL(ls_his_restore_table_operator.insert_row(proxy, persist_info, affected_rows))) {
    LOG_WARN("failed to insert ls his restore progress", K(ret));
  }

  return ret;
}

int ObRestorePersistHelper::inc_need_restore_ls_count_by_one(
    common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key, int result) const
{
  int ret = OB_SUCCESS;
  ObRestoreJobPersistKey job_key = ls_key.generate_restore_job_key();
  ObInnerTableOperator restore_progress_table_operator;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.increase_column_by_one(trans, job_key, OB_STR_LS_COUNT, affected_rows))) {
    LOG_WARN("failed to increase finished ls count in restore progress table", K(ret), K(job_key));
  }
  return ret;
}

// One log stream restore finish.
int ObRestorePersistHelper::inc_finished_ls_count_by_one(
    ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key) const
{
  /*
    1. remove ls from ls restore progress table;
    2. inc finished ls count in restore progress table;
    3. insert finished to history table. 
  */
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator restore_progress_table_operator;
  ObInnerTableOperator ls_restore_progress_table_operator;
  ObInnerTableOperator ls_his_restore_table_operator;

  ObLSRestoreProgressPersistInfo progress_info;
  ObLSHisRestorePersistInfo his_info;

  ObRestoreJobPersistKey job_key = ls_key.generate_restore_job_key();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(ls_his_restore_table_operator.init(OB_ALL_LS_RESTORE_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init ls restore history table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.get_row(trans, true/* need lock */, ls_key, progress_info))) {
    LOG_WARN("failed to get ls restore progress", K(ret), K(ls_key));
  } else if (OB_FAIL(ls_restore_progress_table_operator.delete_row(trans, ls_key, affected_rows))) {
    LOG_WARN("failed to delete ls restore progress", K(ret), K(ls_key), K(progress_info));
  } else if (OB_FAIL(progress_info.generate_his_progress(his_info))) {
    LOG_WARN("fail to generate his progress", K(ret));
  } else if (OB_FAIL(record_ls_his_restore_progress(trans, his_info))) {
    LOG_WARN("failed to insert ls his restore progress", K(ret), K(his_info));
  } else if (OB_FAIL(restore_progress_table_operator.increase_column_by_one(trans, job_key, OB_STR_FINISH_LS_COUNT, affected_rows))) {
    LOG_WARN("failed to increase finished ls count in restore progress table", K(ret), K(job_key));
  }

  return ret;
}

// One tablet restore finish.
int ObRestorePersistHelper::inc_finished_tablet_count_by_one(
    common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator restore_progress_table_operator;
  ObInnerTableOperator ls_restore_progress_table_operator;

  ObRestoreJobPersistKey job_key = ls_key.generate_restore_job_key();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.increase_column_by_one(trans, job_key, OB_STR_FINISH_TABLET_COUNT, affected_rows))) {
    LOG_WARN("failed to increase finished tablet count in restore progress table", K(ret), K(job_key));
  } else if (OB_FAIL(ls_restore_progress_table_operator.increase_column_by_one(trans, ls_key, OB_STR_FINISH_TABLET_COUNT, affected_rows))) {
    LOG_WARN("failed to increase finished tablet count in ls restore progress table", K(ret), K(ls_key));
  }

  return ret;
}

// 
int ObRestorePersistHelper::inc_finished_restored_block_bytes(
    common::ObMySQLTransaction &trans, const ObLSRestoreJobPersistKey &ls_key, 
    const int64_t inc_finished_bytes) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator restore_progress_table_operator;
  ObInnerTableOperator ls_restore_progress_table_operator;

  ObRestoreJobPersistKey job_key = ls_key.generate_restore_job_key();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.init(OB_ALL_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(restore_progress_table_operator.increase_column_by(trans, job_key, OB_STR_FINISH_BYTES, inc_finished_bytes, affected_rows))) {
    LOG_WARN("failed to update finished bytes in restore progress table", K(ret), K(job_key), K(inc_finished_bytes));
  } else if (OB_FAIL(ls_restore_progress_table_operator.increase_column_by(trans, ls_key, OB_STR_FINISH_BYTES, inc_finished_bytes, affected_rows))) {
    LOG_WARN("failed to update finished bytes in ls restore progress table", K(ret), K(ls_key), K(inc_finished_bytes));
  }

  return ret;
}

int ObRestorePersistHelper::update_log_restore_progress(
  common::ObISQLClient &proxy, const ObLSRestoreJobPersistKey &ls_key,
  const SCN &last_replay_scn) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator ls_restore_progress_table_operator;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.update_uint_column(proxy, ls_key, OB_STR_LAST_REPLAY_SCN, last_replay_scn.get_val_for_inner_table_field(), affected_rows))) {
    LOG_WARN("failed to update last replay scn in ls restore progress table", K(ret), K(ls_key), K(last_replay_scn));
  } 

  return ret;
}

int ObRestorePersistHelper::update_ls_restore_status(
    common::ObISQLClient &proxy, const ObLSRestoreJobPersistKey &ls_key,
    const share::ObTaskId &trace_id, const share::ObLSRestoreStatus &status, 
    const int result, const char *comment) const

{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator ls_restore_progress_table_operator;
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else if (OB_ISNULL(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("comment must not be null", K(ret), KP(comment));
  } else if (OB_FALSE_IT(trace_id.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(ls_restore_progress_table_operator.init(OB_ALL_LS_RESTORE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls restore progress table", K(ret));
  } else if (OB_FAIL(ls_restore_progress_table_operator.update_int_column(proxy, ls_key, OB_STR_STATUS, 
      status, affected_rows))) {
    LOG_WARN("failed to update last replay lsn in restore progress table", K(ret), K(ls_key), K(result));
  } else if (OB_FAIL(ls_restore_progress_table_operator.update_int_column(proxy, ls_key, OB_STR_RESULT, 
      result, affected_rows))) {
    LOG_WARN("failed to update last replay scn in restore progress table", K(ret), K(ls_key), K(result));
  } else if (OB_FAIL(ls_restore_progress_table_operator.update_string_column(proxy, ls_key, OB_STR_TRACE_ID, 
      trace_id_str, affected_rows))) {
    LOG_WARN("failed to update last replay lsn in ls restore progress table", K(ret), K(ls_key), K(trace_id));
  } else if (OB_FAIL(ls_restore_progress_table_operator.update_string_column(proxy, ls_key, OB_STR_COMMENT, 
      comment, affected_rows))) {
    LOG_WARN("fail to update comment", K(ret), K(ls_key), KP(comment));
  } else if (affected_rows == 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("update row not exist", K(ret), K(ls_key));
  }
  return ret;
}

int ObRestorePersistHelper::get_all_ls_restore_progress(common::ObISQLClient &proxy, 
    ObIArray<ObLSRestoreProgressPersistInfo> &ls_restore_progress_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRestorePersistHelper not init", K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_LS_RESTORE_PROGRESS_TNAME))) {
      LOG_WARN("fail to assign sql", K(ret), K(OB_ALL_LS_RESTORE_PROGRESS_TNAME));
    } else {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
          LOG_WARN("failed to exec sql", K(ret), K(sql), "exec_tenant_id", get_exec_tenant_id());
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret), K(sql), "exec_tenant_id", get_exec_tenant_id());
        } else if (OB_FAIL(do_parse_ls_restore_progress_result_(*result, ls_restore_progress_info))) {
          LOG_WARN("il to parse result", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRestorePersistHelper::do_parse_ls_restore_progress_result_(sqlclient::ObMySQLResult &result, 
    ObIArray<ObLSRestoreProgressPersistInfo> &ls_restore_progress_info)
{
  int ret = OB_SUCCESS;
  while(OB_SUCC(ret)) {
    ObLSRestoreProgressPersistInfo ls_restore_info;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(ls_restore_info.parse_from(result))) {
      LOG_WARN("fail to parse from result", K(ret));
    } else if (OB_FAIL(ls_restore_progress_info.push_back(ls_restore_info))) {
      LOG_WARN("fail to push backup ls restore info", K(ls_restore_progress_info));
    }
  }
  return ret;
}
