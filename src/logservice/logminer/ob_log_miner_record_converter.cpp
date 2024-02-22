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

#define USING_LOG_PREFIX LOGMNR

#include "lib/json_type/ob_json_base.h"               // ObJsonBaseUtil
#include "lib/timezone/ob_time_convert.h"             // ObTimeConverter
#include "ob_log_miner_record_converter.h"
#include "logservice/common_util/ob_log_time_utils.h"
#include "ob_log_miner_logger.h"
#include "ob_log_miner_utils.h"

#define APPEND_STR(buf, args...) \
  do {\
    if (OB_SUCC(ret) && OB_FAIL(buf.append(args))) { \
      LOG_ERROR("append str failed", "buf_len", buf.length(), "buf_cap", buf.capacity()); \
    }\
  } while(0)

namespace oceanbase
{
namespace oblogminer
{
#define MINER_SCHEMA_DEF(field, id, args...) \
ILogMinerRecordConverter::ColType::field,
const ILogMinerRecordConverter::ColType ILogMinerRecordConverter::COL_ORDER[] =
{
  #include "ob_log_miner_analyze_schema.h"
};
#undef MINER_SCHEMA_DEF

const char *ILogMinerRecordConverter::DELIMITER = ",";

ILogMinerRecordConverter *ILogMinerRecordConverter::get_converter_instance(const RecordFileFormat format)
{
  static ObLogMinerRecordCsvConverter csv_converter;
  static ObLogMinerRecordRedoSqlConverter redo_sql_converter;
  static ObLogMinerRecordUndoSqlConverter undo_sql_converter;
  static ObLogMinerRecordJsonConverter json_converter;

  ILogMinerRecordConverter *converter = nullptr;
  switch(format) {
    case RecordFileFormat::CSV: {
      converter = &csv_converter;
      break;
    }

    case RecordFileFormat::REDO_ONLY: {
      converter = &redo_sql_converter;
      break;
    }

    case RecordFileFormat::UNDO_ONLY: {
      converter = &undo_sql_converter;
      break;
    }

    case RecordFileFormat::JSON: {
      converter = &json_converter;
      break;
    }

    default: {
      converter = nullptr;
      break;
    }
  }

  return converter;
}

////////////////////////////// ObLogMinerRecordCsvConverter //////////////////////////////

int ObLogMinerRecordCsvConverter::write_record(const ObLogMinerRecord &record,
    common::ObStringBuffer &buffer, bool &is_written)
{
  int ret = OB_SUCCESS;

  const int64_t col_num = sizeof(COL_ORDER) / sizeof(ColType);

  for (int64_t i = 0; i < col_num && OB_SUCC(ret); i++) {
    const char *end_char = i == col_num-1 ? "\n": DELIMITER;
    switch(COL_ORDER[i]) {
      case ColType::TENANT_ID: {
        if (OB_FAIL(write_unsigned_number(record.get_tenant_id(), buffer))) {
          LOG_ERROR("write tenant_id failed", K(record));
        } // tenant_id
        break;
      }

      case ColType::TRANS_ID: {
        if (OB_FAIL(write_signed_number(record.get_ob_trans_id().get_id(), buffer))) {
          LOG_ERROR("write trans_id failed", K(record));
        } // trans_id
        break;
      }

      case ColType::PRIMARY_KEY: {
        if (OB_FAIL(write_keys(record.get_primary_keys(), buffer))) {
          LOG_ERROR("write primary_key failed", K(record));
        } // primary_key
        break;
      }

      case ColType::TENANT_NAME: {
        if (OB_FAIL(write_string_no_escape(record.get_tenant_name().str(), buffer))) {
          LOG_ERROR("write tenant_name failed", K(record));
        } // tenant_name
        break;
      }

      case ColType::DATABASE_NAME: {
        if (OB_FAIL(write_string_no_escape(record.get_database_name().str(), buffer))) {
          LOG_ERROR("write database_name failed", K(record));
        } // database_name
        break;
      }

      case ColType::TABLE_NAME: {
        if (OB_FAIL(write_string_no_escape(record.get_table_name().str(), buffer))) {
          LOG_ERROR("write table_name failed", K(record));
        } // table_name
        break;
      }

      case ColType::OPERATION: {
        if (OB_FAIL(write_string_no_escape(record_type_to_str(record.get_record_type()), buffer))) {
          LOG_ERROR("write operation failed", K(record));
        } // operation
        break;
      }

      case ColType::OPERATION_CODE: {
        if (OB_FAIL(write_signed_number(record_type_to_num(record.get_record_type()), buffer))) {
          LOG_ERROR("write operation_code failed", K(record));
        } // operation_code
        break;
      }

      case ColType::COMMIT_SCN: {
        if (OB_FAIL(write_signed_number(record.get_commit_scn().get_val_for_inner_table_field(), buffer))) {
          LOG_ERROR("write commit_scn failed", K(record));
        } // commit scn
        break;
      }

      case ColType::COMMIT_TIMESTAMP: {
        const int16_t scale = 6;
        char time_buf[128] = {0};
        int64_t pos = 0;
        ObString nls_format;
        if (OB_FAIL(ObTimeConverter::datetime_to_str(record.get_commit_scn().convert_to_ts(),
            &LOGMINER_TZ.get_tz_info(), nls_format, scale, time_buf, sizeof(time_buf), pos))) {
          LOG_ERROR("failed to get time string from commit_scn", K(record));
        } else if (OB_FAIL(write_string_no_escape(time_buf, buffer))) {
          LOG_ERROR("write commit_timestamp failed", K(record));
        }
        break;
      }

      case ColType::SQL_REDO: {
        if (OB_FAIL(write_csv_string_escape_(record.get_redo_stmt().string(), buffer))) {
          LOG_ERROR("write redo_stmt failed", K(record));
        } // redo_stmt
        break;
      }

      case ColType::SQL_UNDO: {
        if (OB_FAIL(write_csv_string_escape_(record.get_undo_stmt().string(), buffer))) {
          LOG_ERROR("write undo_stmt failed", K(record));
        } // undo_stmt
        break;
      }

      case ColType::ORG_CLUSTER_ID: {
        if (OB_FAIL(write_signed_number(record.get_cluster_id(), buffer))) {
          LOG_ERROR("write org_cluster_id failed", K(record));
        } // org_cluster_id
        break;
      }

      default: {
        ret = OB_ERR_DEFENSIVE_CHECK;
        LOG_ERROR("unsupported column type", K(COL_ORDER[i]), K(record));
        break;
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(buffer.append(end_char))) {
      LOG_ERROR("append delimiter failed", K(record), K(end_char));
    }
  }

  if (OB_SUCC(ret)) {
    is_written = true;
    LOG_TRACE("ObLogMinerRecordCsvConverter write record succ", K(record));
  }

  return ret;
}

int ObLogMinerRecordCsvConverter::write_csv_string_escape_(const ObString &str, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  const char *data = str.ptr(), *prev_ptr = data;
  APPEND_STR(buffer, "\"");
  while (OB_SUCC(ret) && nullptr != prev_ptr && nullptr != (data = strchr(prev_ptr, '"'))) {
    APPEND_STR(buffer, prev_ptr, data - prev_ptr + 1);
    APPEND_STR(buffer, "\"");
    prev_ptr = data + 1;
  }

  APPEND_STR(buffer, prev_ptr);
  APPEND_STR(buffer, "\"");
  return ret;
}

////////////////////////////// ObLogMinerRecordRedoSqlConverter //////////////////////////////

int ObLogMinerRecordRedoSqlConverter::write_record(const ObLogMinerRecord &record,
    common::ObStringBuffer &buffer, bool &is_written)
{
  int ret = OB_SUCCESS;
  if (!record.get_redo_stmt().empty()) {
    APPEND_STR(buffer, record.get_redo_stmt().string());
    APPEND_STR(buffer, "\n");
    if (OB_SUCC(ret)) {
      is_written = true;
      LOG_TRACE("ObLogMinerRecordRedoSqlConverter write record succ", K(record));
    }
  }
  return ret;
}

////////////////////////////// ObLogMinerRecordUndoSqlConverter //////////////////////////////

int ObLogMinerRecordUndoSqlConverter::write_record(const ObLogMinerRecord &record,
    common::ObStringBuffer &buffer, bool &is_written)
{
  int ret = OB_SUCCESS;
  if (!record.get_undo_stmt().empty()) {
    APPEND_STR(buffer, record.get_undo_stmt().string());
    APPEND_STR(buffer, "\n");
    if (OB_SUCC(ret)) {
      is_written = true;
      LOG_TRACE("ObLogMinerRecordUndoSqlConverter write record succ", K(record));
    }
  }
  return ret;
}

////////////////////////////// ObLogMinerRecordJsonConverter //////////////////////////////

int ObLogMinerRecordJsonConverter::write_record(const ObLogMinerRecord &record,
    common::ObStringBuffer &buffer, bool &is_written)
{
  int ret = OB_SUCCESS;

  const int64_t col_num = sizeof(COL_ORDER) / sizeof(ColType);
  APPEND_STR(buffer, "{");

  for (int64_t i = 0; i < col_num && OB_SUCC(ret); i++) {
    const char *end_char = i == col_num-1 ? "}\n": DELIMITER;
    switch(COL_ORDER[i]) {
      case ColType::TENANT_ID: {
        if (OB_FAIL(write_json_key_("TENANT_ID", buffer))) {
          LOG_ERROR("write json_key TENANT_ID failed", K(record));
        } else if (OB_FAIL(write_unsigned_number(record.get_tenant_id(), buffer))) {
          LOG_ERROR("write tenant_id failed", K(record));
        } // tenant_id
        break;
      }

      case ColType::TRANS_ID: {
        if (OB_FAIL(write_json_key_("TRANS_ID", buffer))) {
          LOG_ERROR("write json_key TRANS_ID failed", K(record));
        } else if (OB_FAIL(write_signed_number(record.get_ob_trans_id().get_id(), buffer))) {
          LOG_ERROR("write trans_id failed", K(record));
        } // trans_id
        break;
      }

      case ColType::PRIMARY_KEY: {
        if (OB_FAIL(write_json_key_("PRIMARY_KEY", buffer))) {
          LOG_ERROR("write json_key PRIMARY_KEY failed", K(record));
        } else if (OB_FAIL(write_keys(record.get_primary_keys(), buffer))) {
          LOG_ERROR("write primary_key failed", K(record));
        } // primary_key
        break;
      }

      case ColType::TENANT_NAME: {
        if (OB_FAIL(write_json_key_("TENANT_NAME", buffer))) {
          LOG_ERROR("write json_key TENANT_NAME failed", K(record));
        } else if (OB_FAIL(write_string_no_escape(record.get_tenant_name().str(), buffer))) {
          LOG_ERROR("write tenant_name failed", K(record));
        } // tenant_name
        break;
      }

      case ColType::DATABASE_NAME: {
        if (OB_FAIL(write_json_key_("DATABASE_NAME", buffer))) {
          LOG_ERROR("write json_key DATABASE_NAME failed", K(record));
        } else if (OB_FAIL(write_string_no_escape(record.get_database_name().str(), buffer))) {
          LOG_ERROR("write database_name failed", K(record));
        } // database_name
        break;
      }

      case ColType::TABLE_NAME: {
        if (OB_FAIL(write_json_key_("TABLE_NAME", buffer))) {
          LOG_ERROR("write json_key TABLE_NAME failed", K(record));
        } else if (OB_FAIL(write_string_no_escape(record.get_table_name().str(), buffer))) {
          LOG_ERROR("write table_name failed", K(record));
        } // table_name
        break;
      }

      case ColType::OPERATION: {
        if (OB_FAIL(write_json_key_("OPERATION", buffer))) {
          LOG_ERROR("write json_key OPERATION failed", K(record));
        } else if (OB_FAIL(write_string_no_escape(record_type_to_str(record.get_record_type()), buffer))) {
          LOG_ERROR("write operation failed", K(record));
        } // operation
        break;
      }

      case ColType::OPERATION_CODE: {
        if (OB_FAIL(write_json_key_("OPERATION_CODE", buffer))) {
          LOG_ERROR("write json_key OPERATION_CODE failed", K(record));
        } else if (OB_FAIL(write_signed_number(record_type_to_num(record.get_record_type()), buffer))) {
          LOG_ERROR("write operation_code failed", K(record));
        } // operation_code
        break;
      }

      case ColType::COMMIT_SCN: {
        if (OB_FAIL(write_json_key_("COMMIT_SCN", buffer))) {
          LOG_ERROR("write json_key COMMIT_SCN failed", K(record));
        } else if (OB_FAIL(write_signed_number(record.get_commit_scn().get_val_for_inner_table_field(), buffer))) {
          LOG_ERROR("write commit_scn failed", K(record));
        } // commit scn
        break;
      }

      case ColType::COMMIT_TIMESTAMP: {
        const int16_t scale = 6;
        char time_buf[128] = {0};
        int64_t pos = 0;
        ObString nls_format;
        if (OB_FAIL(write_json_key_("COMMIT_TIMESTAMP", buffer))) {
          LOG_ERROR("write json_key COMMIT_TIMESTAMP failed", K(record));
        } else if (OB_FAIL(ObTimeConverter::datetime_to_str(record.get_commit_scn().convert_to_ts(),
            &LOGMINER_TZ.get_tz_info(), nls_format, scale, time_buf, sizeof(time_buf), pos))) {
          LOG_ERROR("failed to get time string from commit_scn", K(record));
        } else if (OB_FAIL(write_string_no_escape(time_buf, buffer))) {
          LOG_ERROR("write commit_timestamp failed", K(record));
        }
        break;
      }

      case ColType::SQL_REDO: {
        if (OB_FAIL(write_json_key_("SQL_REDO", buffer))) {
          LOG_ERROR("write json_key SQL_REDO failed", K(record));
        } else if (OB_FAIL(write_json_string_escape_(record.get_redo_stmt().string(), buffer))) {
          LOG_ERROR("write redo_stmt failed", K(record));
        } // redo_stmt
        break;
      }

      case ColType::SQL_UNDO: {
        if (OB_FAIL(write_json_key_("SQL_UNDO", buffer))) {
          LOG_ERROR("write json_key SQL_UNDO failed", K(record));
        } else if (OB_FAIL(write_json_string_escape_(record.get_undo_stmt().string(), buffer))) {
          LOG_ERROR("write undo_stmt failed", K(record));
        } // undo_stmt
        break;
      }

      case ColType::ORG_CLUSTER_ID: {
        if (OB_FAIL(write_json_key_("ORG_CLUSTER_ID", buffer))) {
          LOG_ERROR("write json_key ORG_CLUSTER_ID failed", K(record));
        } else if (OB_FAIL(write_signed_number(record.get_cluster_id(), buffer))) {
          LOG_ERROR("write org_cluster_id failed", K(record));
        } // org_cluster_id
        break;
      }

      default: {
        ret = OB_ERR_DEFENSIVE_CHECK;
        LOG_ERROR("unsupported column type", K(COL_ORDER[i]), K(record));
        break;
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(buffer.append(end_char))) {
      LOG_ERROR("append delimiter failed", K(record), K(end_char));
    }

    if (OB_SUCC(ret)) {
      is_written = true;
      LOG_TRACE("ObLogMinerRecordJsonConverter write record succ", K(record));
    }
  }

  return ret;
}

int ObLogMinerRecordJsonConverter::write_json_key_(const char *str, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  APPEND_STR(buffer, "\"");
  APPEND_STR(buffer, str);
  APPEND_STR(buffer, "\":");
  return ret;
}

int ObLogMinerRecordJsonConverter::write_json_string_escape_(const ObString &str, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  const char *data = str.ptr();
  if (nullptr != data) {
    if (OB_FAIL(ObJsonBaseUtil::add_double_quote(buffer, data, str.length()))) {
      LOG_ERROR("add_double_quote failed", K(str));
    }
  } else {
    APPEND_STR(buffer, "\"\"");
  }

  return ret;
}

}
}

#undef APPEND_STR