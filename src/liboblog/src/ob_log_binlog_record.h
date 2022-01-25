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

#ifndef OCEANBASE_LIBOBLOG_BINLOG_RECORD_
#define OCEANBASE_LIBOBLOG_BINLOG_RECORD_

#include <LogRecord.h>                               // ILogRecord
#include <LogMsgFactory.h>                       // createLogRecord

#include "share/ob_define.h"
#include "lib/oblog/ob_log_module.h"                 // OBLOG_LOG
#include "lib/string/ob_string.h"                    // ObString
#include "common/ob_range.h"                         // ObVersion

#include "ob_log_resource_recycle_task.h"            // ObLogResourceRecycleTask

using namespace oceanbase::logmessage;

namespace oceanbase
{
namespace liboblog
{

class ObLogBR : public ObLogResourceRecycleTask
{
public:
  static const char *COLUMN_CHANGED_LABEL_PTR;
  static const char *COLUMN_UNCHANGED_LABEL_PTR;

public:
  ObLogBR();
  virtual ~ObLogBR();

public:
  static int put_old(ILogRecord *br, const bool is_changed);

public:
  void reset();

  // init INSERT/UPDATE/DELETE Binlog Record
  // set record_type/srcCategory/checkpoint/scrType/firstInLogevent
  int init_dml_data_first(const RecordType type,
      const uint64_t tenant_id);

  // read persist data, fill data after deserialize
  //
  // INSERT/UPDATE/DELETE
  // threadId/timestamp(checkpoint)/filterRuleVal
  int init_dml_data_second(const RecordType type,
    const uint64_t cluster_id,
     const int64_t tenant_id,
     const common::ObString &trace_id,
      const common::ObString &trace_info,
      const common::ObString &unique_id,
      const common::ObVersion &freeze_version,
      const int64_t commit_version);

  // init Binlog Record of DDL/BEGIN/COMMIT
  int init_data(const RecordType type,
      const uint64_t cluster_id,
      const uint64_t tenant_id,
      const int64_t ddl_schema_version,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const common::ObString &unique_id,
      const common::ObVersion &freeze_version,
      const int64_t commit_version,
      const int64_t part_trans_task_count = 0,
      const common::ObString *major_version_str = NULL);

  ILogRecord *get_data() { return data_; }
  int get_record_type(int &record_type);

  void set_next(ObLogBR *next) {next_ = next;};
  ObLogBR *get_next() {return next_;};

  void set_is_valid(const bool is_valid) { valid_ = is_valid; }
  bool is_valid() const { return valid_; }

  int set_table_meta(ITableMeta *table_meta);
  int set_db_meta(IDBMeta *db_meta);

  inline void set_precise_timestamp(int64_t precise_timestamp) { precise_timestamp_ = precise_timestamp; }
  inline int64_t get_precise_timestamp() const { return precise_timestamp_; }

  inline void *get_host() { return host_; }
  void set_host(void *host) { host_ = host; }

  inline void *get_log_entry_task() { return log_entry_task_; }
  void set_log_entry_task(void *log_entry_task) { log_entry_task_ = log_entry_task; }

  inline bool is_serilized() const { return is_serilized_; }
  void set_serilized(const bool is_serilized) { is_serilized_ = is_serilized; }

  int32_t get_major_version() const { return freeze_version_.major_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_ddl_schema_version() const { return ddl_schema_version_; }
  int64_t get_part_trans_task_count() const { return part_trans_task_count_; }

  // for put operation of HBASE: store data type as update, new value use full-column mode, old value is empty
  // special treatment for liboblog:
  // TODO:observer add new dml operation type to represend PUT operation
  int setInsertRecordTypeForHBasePut(const RecordType type);

 public:
  TO_STRING_KV("is_ser", is_serilized_,
      K_(valid));

private:
  int verify_part_trans_task_count_(const RecordType type,
    const int64_t part_trans_task_count);

  // unique id of BinlogRecord: Pkey + LogId + row_index
  // 1. DML statement
  //    set unique id by puFilterRuleVal
  // 2. DDL statement
  //    set tenant id and schema version
  int init_binlog_record_(const RecordType type,
      const uint64_t cluster_id,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const common::ObString &unique_id,
      const int64_t commit_version,
      const common::ObString *major_version_str = NULL);
  int init_binlog_record_first_(const RecordType type);
  int init_binlog_record_second_(const RecordType type,
      const uint64_t cluster_id,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const common::ObString &unique_id,
      const int64_t commit_version,
      const common::ObString *major_version_str = NULL);

private:
  ILogRecord *data_;               ///< real BinlogRecord
  // is binglog record serilized, reference construct_data_ function implementation
  // 1. If binglog record has not been serilized, is_serilized_ = false
  // 2. If binglog record parse from string, is_serilized_ = true
  // is_serilized_ can only be modified by set_serilized function
  bool          is_serilized_;
  void          *host_;               ///< record corresponsding RowIndex
  void          *log_entry_task_;
  ObLogBR       *next_;
  bool          valid_;               ///< statement is valid or not
  int64_t       precise_timestamp_;   ///< precise timestamp in micro seconds

  common::ObVersion freeze_version_;
  uint64_t          tenant_id_;
  // corresponding schema version for DDL
  // use schema version allocated by Sequencer for DML
  // 0 for HEARTBEAT
  int64_t           ddl_schema_version_;

  // Number of tasks in the transaction partition, i.e. number of participants in the transaction
  // 1. DDL
  // 2. DML begin/commit binglog record will carry this info
  int64_t           part_trans_task_count_;

protected:
  /*
   * LogMsgFactory
   *  static ILogRecord* createLogRecord(
   *      const std::string& type = DFT_BR, bool creating = true);
   *
   *  @param creating is to differentiate two kinds of usage, if creating is
   *         true, it means the created binlog record has not been
   *         serilized, all in-memory functions can be called. Otherwise
   *         if creating is false, only after-serialized function could
   *         be called
   */
  void construct_data_(const bool creating_binlog_record);

private:
  void destruct_data_();
};

class ObLogUnserilizedBR : public ObLogBR
{
public:
  ObLogUnserilizedBR();
  virtual ~ObLogUnserilizedBR();

private:
  // Build unserialized ILogRecord, in-memory operations
  void construct_unserilized_data_();
};

class ObLogSerilizedBR : public ObLogBR
{
public:
  ObLogSerilizedBR();
  virtual ~ObLogSerilizedBR();

private:
  // Build serialized ILogRecord, parse based on persistent data
  void construct_serilized_data_();
};

} // end namespace liboblog
} // end namespace oceanbase

#endif // end OCEANBASE_LIBOBLOG_BINLOG_RECORD_
