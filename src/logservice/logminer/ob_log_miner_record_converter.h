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

#ifndef OCEANBASE_LOG_MINER_RECORD_CONVERTER_H_
#define OCEANBASE_LOG_MINER_RECORD_CONVERTER_H_

#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_record.h"

namespace oceanbase
{
namespace oblogminer
{

class ILogMinerRecordConverter
{
public:
  // TODO: there may be concurrency issue in future?
  static ILogMinerRecordConverter *get_converter_instance(const RecordFileFormat format);
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written) = 0;

public:
  // TENANT_ID,TRANS_ID,PRIMARY_KEY,ROW_UNIQUE_ID,SEQ_NO,TENANT_NAME,USER_NAME,TABLE_NAME,OPERATION,
  // OPERATION_CODE,COMMIT_SCN,COMMIT_TIMESTAMP,SQL_REDO,SQL_UNDO,ORG_CLUSTER_ID
  #define MINER_SCHEMA_DEF(field, id, args...) \
    field = id,
  enum class ColType {
    #include "ob_log_miner_analyze_schema.h"
  };
  #undef MINER_SCHEMA_DEF

  const static ColType COL_ORDER[];
  static const char *DELIMITER;
};

class ObLogMinerRecordCsvConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordCsvConverter() {};
  ~ObLogMinerRecordCsvConverter() {}
private:
  int write_csv_string_escape_(const ObString &str, common::ObStringBuffer &buffer);
};

class ObLogMinerRecordRedoSqlConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordRedoSqlConverter() {}
  ~ObLogMinerRecordRedoSqlConverter() {}

};

class ObLogMinerRecordUndoSqlConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordUndoSqlConverter() {}
  ~ObLogMinerRecordUndoSqlConverter() {}
};

class ObLogMinerRecordJsonConverter: public ILogMinerRecordConverter
{
public:
  virtual int write_record(const ObLogMinerRecord &record, common::ObStringBuffer &buffer, bool &is_written);
public:
  ObLogMinerRecordJsonConverter() {};
  ~ObLogMinerRecordJsonConverter() {}
private:
  int write_json_key_(const char *str, common::ObStringBuffer &buffer);
  int write_json_string_escape_(const ObString &str, common::ObStringBuffer &buffer);
};

}
}

#endif