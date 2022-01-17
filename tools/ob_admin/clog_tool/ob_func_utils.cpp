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

#include "ob_func_utils.h"

const char* get_submit_log_type(const int64_t submit_log_type)
{
  const char* char_ret = NULL;
  switch (submit_log_type) {
    case OB_LOG_SP_TRANS_REDO:
      char_ret = "OB_LOG_SP_TRANS_REDO";
      break;
    case OB_LOG_TRANS_REDO:
      char_ret = "OB_LOG_TRANS_REDO";
      break;
    case OB_LOG_TRANS_PREPARE:
      char_ret = "OB_LOG_TRANS_PREPARE";
      break;
    case OB_LOG_TRANS_REDO_WITH_PREPARE:
      char_ret = "OB_LOG_TRANS_REDO_WITH_PREPARE";
      break;
    case OB_LOG_TRANS_PREPARE_WITH_COMMIT:
      char_ret = "OB_LOG_TRANS_PREPARE_WITH_COMMIT";
      break;
    case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT:
      char_ret = "OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT";
      break;
    case OB_LOG_SP_TRANS_COMMIT:
      char_ret = "OB_LOG_SP_TRANS_COMMIT";
      break;
    case OB_LOG_SP_ELR_TRANS_COMMIT:
      char_ret = "OB_LOG_SP_ELR_TRANS_COMMIT";
      break;
    case OB_LOG_TRANS_COMMIT:
      char_ret = "OB_LOG_TRANS_COMMIT";
      break;
    case OB_LOG_SP_TRANS_ABORT:
      char_ret = "OB_LOG_SP_TRANS_ABORT";
      break;
    case OB_LOG_TRANS_ABORT:
      char_ret = "OB_LOG_TRANS_ABORT";
      break;
    case OB_LOG_TRANS_CLEAR:
      char_ret = "OB_LOG_TRANS_CLEAR";
      break;
    case OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR:
      char_ret = "OB_LOG_TRANS_REDO_WITH_PREPARE_WITH_COMMIT_WITH_CLEAR";
      break;
    case OB_LOG_MUTATOR:
      char_ret = "OB_LOG_MUTATOR";
      break;
    case OB_LOG_TRANS_STATE:
      char_ret = "OB_LOG_TRANS_STATE";
      break;
    case OB_LOG_MUTATOR_WITH_STATE:
      char_ret = "OB_LOG_MUTATOR_WITH_STATE";
      break;
    case OB_LOG_MUTATOR_ABORT:
      char_ret = "OB_LOG_MUTATOR_ABORT";
      break;
    case OB_LOG_SPLIT_SOURCE_PARTITION:
      char_ret = "OB_LOG_SPLIT_SOURCE_PARTITION";
      break;
    case OB_LOG_SPLIT_DEST_PARTITION:
      char_ret = "OB_LOG_SPLIT_DEST_PARTITION";
      break;
    case OB_LOG_TRANS_CHECKPOINT:
      char_ret = "OB_LOG_TRANS_CHECKPOINT";
      break;
    case OB_LOG_MAJOR_FREEZE:
      char_ret = "OB_LOG_MAJOR_FREEZE";
      break;
    case OB_LOG_ADD_PARTITION_TO_PG:
      char_ret = "OB_LOG_ADD_PARTITION_TO_PG";
      break;
    case OB_LOG_REMOVE_PARTITION_FROM_PG:
      char_ret = "OB_LOG_REMOVE_PARTITION_FROM_PG";
      break;
    case OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG:
      char_ret = "OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG";
      break;
    default:
      char_ret = "OB_LOG_UNKNOWN";
      break;
  }
  return char_ret;
}

int file_name_parser(const char* path, uint64_t& file_id)
{
  int ret = oceanbase::OB_SUCCESS;

  struct stat _stat;
  if (OB_ISNULL(path)) {
    ret = oceanbase::common::OB_INVALID_ARGUMENT;
  } else if (!is_ofs_file(path) && 0 != stat(path, &_stat)) {
    ret = OB_IO_ERROR;
    _LOGTOOL_LOG(ERROR, "fstate:%s", strerror(errno));
  } else {
    file_id = 0;
    int i = 0;
    int path_len = static_cast<int>(strlen(path));
    for (--path_len; path_len >= 0 && path[path_len] >= '0' && path[path_len] <= '9'; --path_len) {
      file_id += (path[path_len] - '0') * static_cast<int>(pow(10, i++));
    }
  }
  return ret;
}

const char* get_log_type(const enum ObLogType log_type)
{
  const char* char_ret = NULL;
  switch (log_type) {
    case OB_LOG_SUBMIT:
      char_ret = "OB_LOG_SUBMIT";
      break;
    case OB_LOG_MEMBERSHIP:
      char_ret = "OB_LOG_MEMBERSHIP";
      break;
    case OB_LOG_PREPARED:
      char_ret = "OB_LOG_PREPARED";
      break;
    case oceanbase::clog::OB_LOG_NOP:
      char_ret = "OB_LOG_NOP";
      break;
    case OB_LOG_START_MEMBERSHIP:
      char_ret = "OB_LOG_START_MEMBERSHIP";
      break;
    case OB_LOG_NOT_EXIST:
      char_ret = "OB_LOG_NOT_EXIST";
      break;
    case OB_LOG_AGGRE:
      char_ret = "OB_LOG_AGGRE";
      break;
    case OB_LOG_ARCHIVE_CHECKPOINT:
      char_ret = "OB_LOG_ARCHIVE_CHECKPOINT";
      break;
    case OB_LOG_ARCHIVE_KICKOFF:
      char_ret = "OB_LOG_ARCHIVE_KICKOFF";
      break;
    default:
      char_ret = "OB_LOG_UNKNOWN";
      break;
  }
  return char_ret;
}

const char* get_freeze_type(ObFreezeType freeze_type)
{
  const char* char_ret = NULL;
  switch (freeze_type) {
    case INVALID_FREEZE:
      char_ret = "INVALID_FREEZE";
      break;
    case MAJOR_FREEZE:
      char_ret = "MAJOR_FREEZE";
      break;
    case MINOR_FREEZE:
      char_ret = "MINOR_FREEZE";
      break;
  }
  return char_ret;
}

const char* get_row_dml_type_str(const ObRowDml& dml_type)
{
  const char* dml_str = "UNKNOWN";
  switch (dml_type) {
    case T_DML_INSERT:
      dml_str = "INSERT";
      break;
    case T_DML_UPDATE:
      dml_str = "UPDATE";
      break;
    case T_DML_DELETE:
      dml_str = "DELETE";
      break;
    case T_DML_REPLACE:
      dml_str = "REPLACE";
      break;
    case T_DML_LOCK:
      dml_str = "LOCK";
      break;
    default:
      dml_str = "UNKNOWN";
      CLOG_LOG(ERROR, "unknown dml_type", K(dml_type));
      break;
  }
  return dml_str;
}

bool is_ofs_file(const char* path)
{
  UNUSED(path);
  return false;
}
