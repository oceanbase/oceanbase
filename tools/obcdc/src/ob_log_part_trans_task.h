/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBLOG_TRANS_TASK_H__
#define OCEANBASE_LIBOBLOG_TRANS_TASK_H__

#include "lib/queue/ob_link.h"                      // ObLink
#include "lib/atomic/ob_atomic.h"                   // ATOMIC_LOAD
#include "lib/lock/ob_small_spin_lock.h"            // ObByteLock
#include "common/object/ob_object.h"                // ObObj
#include "common/ob_partition_key.h"                // ObPartitionKey
#include "common/ob_queue_thread.h"                 // ObCond
#include "storage/transaction/ob_trans_define.h"    // ObTransID, PartitionLogInfoArray
#include "storage/memtable/ob_memtable_mutator.h"   // ObMemtableMutatorRow, ObMemtableMutatorMeta
#include "storage/ob_i_store.h"                     // ObRowDml

#include "ob_log_trans_log.h"                       // SortedRedoLogList
#include "ob_log_rollback_section.h"                // RollbackList
#include "ob_log_lighty_list.h"                     // LightyList
#include "ob_small_arena.h"                         // ObSmallArena
#include "ob_log_task_pool.h"                       // TransTaskBase
#include "ob_log_utils.h"                           // is_ddl_table
#include "ob_log_resource_recycle_task.h"           // ObLogResourceRecycleTask
#include "ob_log_callback.h"                        // ObILogCallback

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
}
}

namespace liboblog
{
class PartTransTask;
class ObLogBR;
class ObLogEntryTask;

class IStmtTask : public ObLink   // Inheritance of ObLink is only used for Sequencer
{
public:
  enum StmtType
  {
    STMT_TYPE_UNKNOWN = 0,
    STMT_TYPE_DML = 1,              // DML statement
    STMT_TYPE_DDL = 2,              // DDL statement
  };

  IStmtTask(const StmtType type, PartTransTask &host) :
    type_(type),
    host_(host),
    hash_value_(common::OB_INVALID_ID),
    row_index_(OB_INVALID_ID),
    br_(NULL),
    next_(NULL)
  {}

  virtual ~IStmtTask() { reset(); }

  bool is_unknown_stmt() const { return STMT_TYPE_UNKNOWN == type_; }
  bool is_dml_stmt() const { return STMT_TYPE_DML == type_; }
  bool is_ddl_stmt() const { return STMT_TYPE_DDL == type_; }

  PartTransTask &get_host() { return host_; }
  const PartTransTask &get_host() const { return host_; }
  uint64_t get_tenant_id() const;

  StmtType get_type() const { return type_; }
  void set_type(const int type) { type_ = static_cast<StmtType>(type); }

  void reset();

  IStmtTask *get_next() { return next_; }
  void set_next(IStmtTask *next) { next_ = next; }

  uint64_t hash() const { return hash_value_; }
  void set_hash_value(const int64_t hash_value) { hash_value_ = hash_value; }

  uint64_t get_row_index() const { return row_index_; }
  void set_row_index(const uint64_t row_index) { row_index_ = row_index; }

  ObLogBR *get_binlog_record() { return br_; }
  const ObLogBR *get_binlog_record() const { return br_; }
  void set_binlog_record(ObLogBR *br) { br_ = br; }

  static const char* print_stmt_type(const int type)
  {
    const char *type_str = "UNKNOWN";
    switch (type) {
    case STMT_TYPE_UNKNOWN:
      type_str = "UNKNOWN";
      break;
    case STMT_TYPE_DML:
      type_str = "DML";
      break;
    case STMT_TYPE_DDL:
      type_str = "DDL";
      break;
    default:
      type_str = "INVALID";
      break;
    }
    return type_str;
  }

  TO_STRING_KV(K_(type),
      "type_str", print_stmt_type(type_),
      K_(hash_value),
      K_(row_index),
      KP_(br),
      K_(host),
      KP_(next));

protected:
  StmtType          type_;
  PartTransTask     &host_;        // part trans task the stmt belongs to
  uint64_t          hash_value_;   // HASH value
  uint64_t          row_index_;    // row index for the stmt in the trans it belongs
  ObLogBR           *br_;
  IStmtTask         *next_;

private:
  DISALLOW_COPY_AND_ASSIGN(IStmtTask);
};

////////////////////////////////////////////////////////////////////////////////////

// node of column value
struct ColValue
{
  common::ObObj value_;
  uint64_t      column_id_;
  ObString      string_value_;    // The value after converting Obj to a string
  ColValue      *next_;

  void reset()
  {
    value_.reset();
    column_id_ = common::OB_INVALID_ID;
    string_value_.reset();
    next_ = NULL;
  }

  bool is_valid()
  {
    return value_.is_valid_type() && common::OB_INVALID_ID != column_id_;
  }

  ColValue *get_next() { return next_; }
  void set_next(ColValue *next) { next_ = next; }

  TO_STRING_KV(K_(value), K_(column_id), K_(string_value));
};

///////////////////////////////////////////////////////////////////////////////////

typedef LightyList<ColValue> ColValueList;
class ObObj2strHelper;

// row value
class MutatorRow : public memtable::ObMemtableMutatorRow
{
public:
  explicit MutatorRow(common::ObIAllocator &allocator);
  virtual ~MutatorRow();

public:
  // Deserialize a row
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);

  // Support for filtering table data within PG
  // Deserialize some fields: first step to get row_size, table_id
  int deserialize_first(const char* buf,
      const int64_t data_len,
      int64_t &pos,
      int32_t &row_size,
      uint64_t &table_id);

  // Deserialize some fields: Step 2 continues the parsing to get the table_version
  int deserialize_second(const char* buf,
      const int64_t data_len,
      int64_t &pos,
      int64_t &table_version);

  void reset();

  // Parse the column data
  // If obj2str_helper is empty, do not convert obj to string
  int parse_cols(ObObj2strHelper *obj2str_helper = NULL,
      const share::schema::ObSimpleTableSchemaV2 *simple_table_schema = NULL,
      const TableSchemaInfo *tb_schema_info = NULL,
      const bool enable_output_hidden_primary_key = false);

  int get_cols(ColValueList **rowkey_cols, ColValueList **new_cols, ColValueList **old_cols);

  bool is_rollback_stmt() const { return 1 == flag_; }

public:
  TO_STRING_KV("Row", static_cast<const memtable::ObMemtableMutatorRow &>(*this),
      K_(deserialized),
      K_(cols_parsed),
      K_(new_cols),
      K_(old_cols),
      K_(rowkey_cols));

private:
  int parse_columns_(ColValueList &cols,
      const char *col_data,
      const int64_t col_data_size,
      ObObj2strHelper *obj2str_helper,
      const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
      const TableSchemaInfo *tb_schema_info,
      const bool enable_output_hidden_primary_key);
  int parse_rowkey_(ColValueList &rowkey_cols,
      const common::ObStoreRowkey &rowkey,
      ObObj2strHelper *obj2str_helper,
      const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
      const TableSchemaInfo *tb_schema_info,
      const bool enable_output_hidden_primary_key);
  int add_column_(ColValueList &cols,
      const uint64_t column_id,
      const ObObj *value,
      ObObj2strHelper *obj2str_helper,
      const share::schema::ObSimpleTableSchemaV2 *simple_table_schema,
      const ColumnSchemaInfo *column_schema);

private:
  common::ObIAllocator  &allocator_;

  bool                  deserialized_;
  bool                  cols_parsed_;
  ColValueList          new_cols_;     // A list of new values for the columns, currently no primary key values are stored, only normal columns
  ColValueList          old_cols_;     // A list of old values for the columns, currently no primary key values are stored, only normal columns
  ColValueList          rowkey_cols_;  // rowkey column

private:
  DISALLOW_COPY_AND_ASSIGN(MutatorRow);
};

///////////////////////////////////////////////////////////////////////////////////
#define DELIMITER_STR ","

// The DML unique ID is Pkey+LogId+RowIndex
// The format is: PkeyStr,LogID,RowIndex, where the separator is ,
class DmlStmtUniqueID
{
public:
  DmlStmtUniqueID(const ObString &pkey_and_log_id_str, const int32_t log_offset, const uint64_t row_index) :
    pkey_and_log_id_str_(pkey_and_log_id_str), log_offset_(log_offset), row_index_(row_index) {}
  ~DmlStmtUniqueID() { reset(); }
public:
  void reset()
  {
    log_offset_ = 0;
    row_index_ = OB_INVALID_ID;
  }

  bool is_valid() const
  { return ! pkey_and_log_id_str_.empty() && log_offset_ >= 0 && OB_INVALID_ID != row_index_; }
  const ObString &get_pkey_and_log_id_str() const { return pkey_and_log_id_str_; }
  uint64_t get_row_index() const { return row_index_; }
  int64_t get_dml_unique_id_length() const;

public:
  // row_index(uint64_t): to_cstring长度20
  static const int64_t MAX_ROW_INDEX_LENGTH = 20;
  static int64_t compute_str_length_base_num(uint64_t num);
  // Optimising customisation to_string
  int customized_to_string(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(pkey_and_log_id_str), K_(row_index));

private:
  const ObString &pkey_and_log_id_str_;
  int32_t log_offset_;
  uint64_t row_index_;

private:
  DISALLOW_COPY_AND_ASSIGN(DmlStmtUniqueID);
};

// DML statement task
class DmlStmtTask : public IStmtTask
{
public:
  DmlStmtTask(PartTransTask &host,
      ObLogEntryTask &redo_log_entry_task,
      MutatorRow &row);
  virtual ~DmlStmtTask();

  void reset();

  int64_t get_global_schema_version() const;
  int64_t get_table_version() const { return row_.table_version_; }
  uint64_t get_table_id() const { return row_.table_id_; }
  int64_t get_part_id() const;
  const common::ObStoreRowkey &get_rowkey() const { return row_.rowkey_; }
  storage::ObRowDml get_dml_type() const { return row_.dml_type_; }

  // Parse the column data
  // If obj2str_helper is empty, then no conversion of obj to string
  // NOTE: you can get_cols() only if you succeed in parse_cols()
  int parse_cols(ObObj2strHelper *obj2str_helper = NULL,
      const share::schema::ObSimpleTableSchemaV2 *simple_table_schema = NULL,
      const TableSchemaInfo *tb_schema_info = NULL,
      const bool enable_output_hidden_primary_key = false)
  {
    return row_.parse_cols(obj2str_helper, simple_table_schema, tb_schema_info, enable_output_hidden_primary_key);
  }

  int get_cols(ColValueList **rowkey_cols, ColValueList **new_cols, ColValueList **old_cols)
  {
    return row_.get_cols(rowkey_cols, new_cols, old_cols);
  }

  ObLogEntryTask &get_redo_log_entry_task() { return redo_log_entry_task_; }

  int32_t get_row_sql_no() const { return row_.sql_no_; }
  int64_t get_row_seq_for_rollback() const;

public:
  TO_STRING_KV("IStmtTask", static_cast<const IStmtTask &>(*this),
      K_(row),
      K_(redo_log_entry_task));

private:
  ObLogEntryTask          &redo_log_entry_task_;
  MutatorRow              &row_;
private:
  DISALLOW_COPY_AND_ASSIGN(DmlStmtTask);
};

///////////////////////////////////////////////////////////////////////////////

// DDL unique ID using cluster_id + schema_version
class DdlStmtUniqueID
{
public:
  DdlStmtUniqueID(const int64_t cluster_id, const uint64_t schema_version) :
    cluster_id_(cluster_id), schema_version_(schema_version) {}
  ~DdlStmtUniqueID() { reset(); }
public:
  void reset()
  {
    cluster_id_ = OB_INVALID_CLUSTER_ID;
    schema_version_ = OB_INVALID_TIMESTAMP;
  }

  bool is_valid() const
  { return OB_INVALID_CLUSTER_ID != cluster_id_
    && OB_INVALID_TIMESTAMP != schema_version_; }

  int64_t get_cluster_id() const { return cluster_id_; }
  uint64_t get_schema_version() const { return schema_version_; }

public:
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int64_t     cluster_id_;       // cluster ID
  int64_t     schema_version_;   // schema version

private:
  DISALLOW_COPY_AND_ASSIGN(DdlStmtUniqueID);
};

class DdlStmtTask : public IStmtTask
{
public:
  DdlStmtTask(PartTransTask &host, MutatorRow &row, const int64_t cluster_id);
  virtual ~DdlStmtTask();

public:
  void reset();

  // parse DDL data
  // init DDL Binlog Record
  int parse_ddl_info(ObLogBR *br,
      const uint64_t row_index,
      bool &is_valid_ddl,
      int64_t &update_schema_version,
      uint64_t &exec_tennat_id);

  // get ddl str
  const ObString &get_ddl_stmt_str() const { return ddl_stmt_str_; }
  int64_t get_operation_type() const { return ddl_operation_type_; }
  uint64_t get_op_table_id() const { return ddl_op_table_id_; }
  uint64_t get_op_tenant_id() const { return ddl_op_tenant_id_; }
  uint64_t get_op_database_id() const { return ddl_op_database_id_; }
  uint64_t get_op_tablegroup_id() const { return ddl_op_tablegroup_id_; }
  int64_t get_op_schema_version() const { return ddl_op_schema_version_; }
  uint64_t get_exec_tenant_id() const { return ddl_exec_tenant_id_; }
  int64_t get_cluster_id() const { return cluster_id_; }
  int32_t get_row_sql_no() const { return row_.sql_no_; }
  int64_t get_row_seq_for_rollback() const;

public:
  // tennat_id(UINT64_MAX: 20) + schema_version(INT64_MAX:19)
  static const int64_t MAX_DDL_UNIQUE_ID_LENGTH = 50;
  // schema_version(INT64_MAX:19)
  static const int64_t MAX_DDL_SCHEMA_VERSION_STR_LENGTH = 20;
  // log id (INT64_MAX:19)
  static const int64_t MAX_PREPRAR_LOG_ID_LENGTH = 20;

public:
  TO_STRING_KV("IStmtTask", static_cast<const IStmtTask &>(*this),
      K_(row),
      K_(ddl_exec_tenant_id),
      K_(ddl_stmt_str),
      K_(ddl_op_schema_version),
      K_(ddl_operation_type),
      K_(ddl_op_table_id),
      K_(ddl_op_tenant_id),
      K_(ddl_op_database_id),
      K_(ddl_op_tablegroup_id),
      K_(cluster_id));

private:
  int parse_ddl_info_(bool &contain_ddl_stmt, int64_t &update_schema_version);
  int parse_schema_version_(ObObj &col_value, int64_t &schema_version);
  int parse_ddl_info_from_normal_columns_(ColValueList &col_value_list);
  // 1. schema non-split mode returns the pure_id itself
  // 2. schema split mode returns the calculated result if the pure_id is valid; otherwise returns the pure_id itself
  // When in schema split mode, the common tenant table_id, database_id, user_id and tablegroup_id are
  // removed from the tenant information and need to be recalculated to ensure the schema is refreshed correctly
  uint64_t combine_id_(const bool is_schema_split_mode,
      const uint64_t tenant_id,
      const uint64_t pure_id);
  int build_ddl_binlog_record_(ObLogBR *br,
      const ObString &ddl_stmt,
      const uint64_t row_index);
  bool is_recyclebin_database_id(const uint64_t tenant_id, const uint64_t database_id);
  bool is_drop_table_ddl_(const int64_t ddl_operation_type);
  bool is_drop_tablegroup_ddl_(const int64_t ddl_operation_type);
  bool is_drop_tenant_ddl_(const int64_t ddl_operation_type);
  bool is_global_index_ddl_(const int64_t ddl_operation_type);
  // OB_DDL_CREATE_INDEX
  // OB_DDL_DROP_INDEX
  bool is_normal_index_ddl_(const int64_t ddl_operation_type);
  bool is_create_tenant_end_ddl_(const int64_t ddl_operation_type);
  bool is_finish_schema_split_ddl_(const int64_t ddl_operation_type);
  // OB_DDL_ADD_SUB_PARTITION
  // OB_DDL_DROP_SUB_PARTITION
  bool is_sub_partition_alter_ddl_(const int64_t ddl_operation_type);
  int init_ddl_unique_id_(common::ObString &ddl_unique_id);

private:
  MutatorRow  &row_;
  ObString    ddl_stmt_str_;
  int64_t     ddl_operation_type_;
  int64_t     ddl_op_schema_version_;
  char        ddl_op_schema_version_str_[MAX_DDL_SCHEMA_VERSION_STR_LENGTH];

  uint64_t    ddl_op_table_id_;
  uint64_t    ddl_op_tenant_id_;
  uint64_t    ddl_op_database_id_;
  uint64_t    ddl_op_tablegroup_id_;

  // Record Executor Tenant ID
  uint64_t    ddl_exec_tenant_id_;

  // record cluster ID
  int64_t     cluster_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(DdlStmtTask);
};

/////////////////////////////////////////////////////////////////////////////////

typedef LightyList<IStmtTask> StmtList;

class ObLogEntryTask
{
public:
  ObLogEntryTask();
  virtual ~ObLogEntryTask();
  void reset();
  bool is_valid() const;

public:
  int init(const common::ObPartitionKey &pkey,
      const char *participant,
      const transaction::ObTransID &trans_id,
      DmlRedoLogNode *redo_node);

  uint64_t hash() const
  {
    uint64_t hash_value = get_partition().hash();

    return hash_value;
  }

public:
  int get_status(bool &is_stored);
  int get_storage_key(std::string &key);

  inline void *get_host() { return host_; }
  inline void set_host(void *host) { host_ = host; }

  const common::ObPartitionKey &get_partition() const { return partition_; }
  uint64_t get_tenant_id() const { return partition_.get_tenant_id(); }
  bool is_ddl_part() const { return is_ddl_table(partition_.get_table_id()); }

  const transaction::ObTransID &get_trans_id() const { return trans_id_; }

  int get_valid_row_num(int64_t &valid_row_num);

  common::ObIAllocator &get_allocator() { return arena_allocator_; }
  void *alloc(const int64_t size);
  void free(void *ptr);

  const DmlRedoLogNode *get_redo_log_node() const { return redo_node_; }
  int get_log_id(uint64_t &log_id);
  int get_log_offset(int32_t &log_offset);
  int get_data_len(int64_t &data_len);

  int set_data_and_readed_status(bool is_big_row, char *data, int64_t data_len);
  int rc_callback();

  const StmtList &get_stmt_list() const { return stmt_list_; }
  StmtList &get_stmt_list() { return stmt_list_; }
  int64_t get_stmt_num() const { return stmt_list_.num_; }
  int add_stmt(const uint64_t row_index, IStmtTask *stmt_task);

  int revert_by_rollback_savepoint(const uint64_t current_log_id,
      const uint64_t rollback_log_id,
      const uint64_t row_index,
      const int64_t rollback_no);

  int revert_by_rollback_savepoints(const uint64_t current_log_id,
      const RollbackList &rollback_list);

  // Increases the number of statements that complete the formatting and returns the result after the increase
  int64_t inc_formatted_stmt_num();

  // 1. iterate through the formatted DmlStmt, concatenating all DmlStmtTask
  // 2. Recycle directly for invalid binlog records
  // return row ref cnt
  int link_row_list(int64_t &row_ref_cnt);

  int set_redo_log_parsed();
  int set_redo_log_formatted();

  int64_t dec_row_ref_cnt();
  void set_row_ref_cnt(const int64_t ref_cnt);
  int64_t get_row_ref_cnt() const { return ATOMIC_LOAD(&row_ref_cnt_); }

  TO_STRING_KV(K_(partition),
      K_(trans_id),
      KPC_(redo_node),
      K_(stmt_list),
      K_(formatted_stmt_num),
      K_(row_ref_cnt));

private:
  int revert_binlog_record_(ObLogBR *br);
  int revert_dml_stmt_(const uint64_t current_log_id,
      const uint64_t rollback_log_id,
      const int64_t rollback_no);

private:
  void                   *host_;            // PartTransTask host

  const char             *participant_;

  common::ObPartitionKey partition_;        // Partition key
  transaction::ObTransID trans_id_;         // Transaction ID

  DmlRedoLogNode     *redo_node_;           // dml redo log node
  bool               is_big_row_;

  StmtList           stmt_list_;            // statement list
  int64_t            formatted_stmt_num_;   // Number of statements that formatted
  int64_t            row_ref_cnt_;          // reference count

  // Non-thread safe allocator
  // used for Parser/Formatter
  common::ObArenaAllocator arena_allocator_;          // allocator

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogEntryTask);
};

/////////////////////////////////////////////////////////////////////////////////

class PartTransDispatcher;
struct TransCommitInfo;

// Partitioned transaction tasks
// Distinguish between DDL transactions, DML transactions and heartbeats to facilitate differentiation of transaction types when parsing
class PartTransTask : public TransTaskBase<PartTransTask>, public ObLogResourceRecycleTask, public ObILogCallback
{
public:
  enum TaskType
  {
    TASK_TYPE_UNKNOWN = 0,
    TASK_TYPE_DML_TRANS,          // DML trans
    TASK_TYPE_DDL_TRANS,          // DDL trans
    TASK_TYPE_PART_HEARTBEAT,     // heartbeat of partition level
    TASK_TYPE_GLOBAL_HEARTBEAT,   // heartbeat of global level, used to pass checkpoint info for downstream
    TASK_TYPE_OFFLINE_PARTITION,  // partition offline task
    TASK_TYPE_NOT_SERVED_TRANS,   // not served trans, convert from other trans type
    TASK_TYPE_MAX
  };
  enum ServedState
  {
    UNSERVED = 0,  // Partition transaction is abort or partition transaction is not served
    SERVED = 1
  };

public:
  PartTransTask();
  virtual ~PartTransTask();

public:
  static const char *print_task_type(const TaskType type);

public:
  void reset();
  void set_pkey_info(const common::ObPartitionKey &partition,
      const char *pkey_str);

  /// The initialisation process of a transaction task is divided into four stages.
  /// where: the DML transaction task processing process, where the maintenance of the completion status is completed, and the disassembly, maintenance and distribution of the task.
  ///
  /// 1. Upon receipt of the Redo log: push_redo_log();
  /// For DML: When the detection of single redo/multiple redo logs [LOB] is complete, split the sub-task and send it to the redo ObLogEntryTask, followed by the ObLogEntryTask callback processing
  /// 2. Upon receipt of the prepare log: prepare()
  /// If the transaction commits in bulk, call commit() to enter the commit or pre-commit state
  /// 3. If the Redo log is missing, continue to push_redo_log(); after the missing log is filled, prepare() again
  /// 4. If the Commit log is received: commit()
  ///
  /// @retval OB_ENTRY_EXIST redo log already exists
  /// @retval OB_LOG_MISSING redo log missing, current log push failed: LOB intermediate log scenario, missing LOB start log
  int push_redo_log(const common::ObPartitionKey &pkey,
      const transaction::ObTransID &trans_id,
      const int64_t log_no,
      const uint64_t log_id,
      const int32_t log_offset,
      const int64_t tstamp,
      const char *buf,
      const int64_t buf_len);

  /// Prepare normal transaction tasks, the transaction type may be DDL or DML
  ///
  /// @param [in] partition             partitionKey
  /// @param [in] timestamp             Partition transaction timestamp, set to Prepare log timestamp
  /// @param [in] trans_id              Transaction ID
  /// @param [in] prepare_log_id        Prepare log ID, for single partitioned transactions without Prepare, the last one shall prevail
  /// @param [in] cluster_id            cluster ID
  /// @param [in] freeze                version freeze version
  /// @param [in] trace_id              app trace id
  /// @param [in] trace_info            app trace info
  /// @param [in] elt_trans_info_array  Precursor transaction information, for early unlocking scenarios:
  ///                                   Single partition trans on single machine : precursor transaction information is based on the OB_LOG_SP_ELR_TRANS_COMMITCsp_commit) log
  ///                                   Multi-partition trans on single machine: Preceding transaction information is based on the PREPARE log
  ///
  /// @retval OB_SUCCESS          Success
  /// @retval Other error codes   Fail
  int prepare(const common::ObPartitionKey &partition,
      const int64_t timestamp,
      const transaction::ObTransID &trans_id,
      const uint64_t prepare_log_id,
      const uint64_t cluster_id,
      const ObString &trace_id,
      const ObString &trace_info,
      const transaction::ObElrTransInfoArray &elt_trans_info_array);

  /// Submit a normal transaction task
  /// Requires that the prepares have been successful and that the redo log is complete
  ///
  /// @param [in] global_trans_version    Global transaction version
  /// @param [in] participant             <PartitionKey, PrepareLogID> array
  /// @param [in] is_ready_to_commit      1. whether it is pre-commit, i.e. the commit status has not yet been determined and the status is subsequently advanced by checkpoint
  ///                                     2. This contains the batch commit transaction and the early unlock scenario "pre-commit"
  /// @param [in] first_log_ts            Record the first log timestamp of the partition
  /// @param [in] trans_id                Transaction ID
  /// @param [in] trans_commit_info       Transaction commit info
  /// @param [in] part_trans_dispatcher   Partition transaction dispatcher
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int commit(const int64_t global_trans_version,
      const transaction::PartitionLogInfoArray &participants,
      const bool is_ready_to_commit,
      const int64_t first_log_ts,
      const transaction::ObTransID &trans_id,
      const TransCommitInfo &trans_commit_info,
      PartTransDispatcher &part_trans_dispatcher);

  /// Early Row Unlock Scenario: registering information about the successor transaction for callbacks by the successor transaction
  ///
  /// @param [in] trans_id              trans id
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int register_elt_follow_trans(const transaction::ObTransID &trans_id);

  /// Early Row Unlock Scenario: registering information about the successor transaction for callbacks by the successor transaction
  ///
  /// @param [in] trans_id              trans id
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int handle_elt_trans_callback(const transaction::ObTransID &trans_id,
      PartTransDispatcher &part_trans_dispatcher);

  /// try to set PartTransTask in DataReady
  /// PartTransDispatcher dispatch commit part trans
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int try_to_set_data_ready_status();

  /// Storager handle log callback
  ///
  ///
  /// @retval OB_SUCCESS  succ
  /// @retval other       fail
  int handle_log_callback();

  /// PartTransDispatcher::remove_task() call
  ///
  ///
  /// @retval OB_SUCCESS  Success
  /// @retval other       Fail
  int handle_unserved_trans();

  // Initialize partition heartbeat task information
  // Set the type to: TASK_TYPE_PART_HEARTBEAT
  //
  /// @param [in] pkey         partition key
  /// @param [in] timestamp    Heartbeat timestamp
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_part_heartbeat_info(const common::ObPartitionKey &pkey, const int64_t timestamp);

  // Initialize global heartbeat task information, global heartbeat task is independent of partition
  // Set the type to: TASK_TYPE_GLOBAL_HEARTBEAT
  //
  /// @param [in] timestamp          heartbeat timestamp
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_global_heartbeat_info(const int64_t timestamp);

  // Initially offline partition task
  // Set the type to: TASK_TYPE_OFFLINE_PARTITION
  //
  // @param [in] partition          PartitionKey
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_offline_partition_task(const common::ObPartitionKey &partition);

  // is task info valid or not
  bool is_task_info_valid() const;

  // Convert to a non-serviceable partitioned transaction type
  // Note: Only conversions from DML/DDL type transactions are supported, direct initialisation is not supported
  int convert_to_not_served_trans();

  SortedRedoLogList &get_sorted_redo_list() { return sorted_redo_list_; }
  bool is_contain_empty_redo_log() const { return 0 == sorted_redo_list_.get_node_number(); }

  void set_checkpoint_seq(const int64_t seq) { checkpoint_seq_ = seq; }
  int64_t get_checkpoint_seq() const { return checkpoint_seq_; }

  void set_type(const TaskType type) { type_ = type; }
  TaskType get_type() const { return type_; }

  void set_exec_tenant_id(const uint64_t exec_tenant_id) { exec_tenant_id_ = exec_tenant_id; }
  uint64_t get_exec_tenant_id() const { return exec_tenant_id_; }

  bool is_global_heartbeat() const { return TASK_TYPE_GLOBAL_HEARTBEAT == type_; }
  bool is_part_heartbeat() const { return TASK_TYPE_PART_HEARTBEAT == type_; }
  bool is_ddl_part_heartbeat() const
  {
    return is_part_heartbeat() && is_ddl_table(partition_.get_table_id());
  }
  bool is_ddl_part() const { return is_ddl_table(partition_.get_table_id()); }
  bool is_dml_trans() const { return TASK_TYPE_DML_TRANS == type_; }
  bool is_ddl_trans() const { return TASK_TYPE_DDL_TRANS == type_; }
  bool is_offline_partition_task() const { return TASK_TYPE_OFFLINE_PARTITION == type_; }
  // Is it a DDL OFFLINE task
  bool is_ddl_offline_task() const
  {
    return is_offline_partition_task() && is_ddl_table(partition_.get_table_id());
  }
  bool is_not_served_trans() const { return TASK_TYPE_NOT_SERVED_TRANS == type_; }

  void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }

  uint64_t get_prepare_log_id() const { return prepare_log_id_; }
  uint64_t get_cluster_id() const { return cluster_id_; }

  void set_partition(const common::ObPartitionKey &partition) { partition_ = partition; }
  const common::ObPartitionKey &get_partition() const { return partition_; }
  bool is_pg() const { return partition_.is_pg(); }
  uint64_t get_tenant_id() const { return partition_.get_tenant_id(); }

  const char *get_participant_key_str() const { return pkey_str_; }
  const ObString &get_trans_id_str() const { return trans_id_str_; }

  void set_timestamp(const int64_t timestamp) { timestamp_ = timestamp; }
  int64_t get_timestamp() const { return timestamp_; }

  // 单机多分区优化事务，根据checkpoint来决定是否提交, 并更新状态
  //
  /// @param [in] checkpoint  维护分区级别checkpoint
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int update_trans_committed_status(const int64_t checkpoint,
      PartTransDispatcher &part_trans_dispatcher);
  bool is_trans_committed() const { return ATOMIC_LOAD(&is_trans_committed_); }
  bool is_trans_ready_to_commit() const  { return ATOMIC_LOAD(&is_trans_ready_to_commit_); }

  void update_local_schema_version(const int64_t sm_version)
  {
    if (sm_version > 0) {
      local_schema_version_ = std::max(sm_version, local_schema_version_);
    }
  }
  int64_t get_local_schema_version() const { return local_schema_version_; }

  void *alloc(const int64_t size);
  void free(void *ptr);

  int add_stmt(const uint64_t row_index, IStmtTask *stmt_task);
  int add_ddl_stmt(const uint64_t row_index, DdlStmtTask *ddl_stmt);
  const StmtList &get_stmt_list() const { return stmt_list_; }
  StmtList &get_stmt_list() { return stmt_list_; }
  int64_t get_stmt_num() const { return stmt_list_.num_; }

  // Free stmt_list memory and clear the statement
  void free_stmt_list()
  {
    // FIXME: Every IStmtTask in the list of statements should be destructured here
    // but currently all the memory in the IStmtTask is allocated by the PartTransTask allocator,
    // so it is sufficient to reuse the allocator memory directly, which is also the highest performance
    stmt_list_.reset();
  }

  void set_formatted();
  int wait_formatted(const int64_t timeout, common::ObCond &cond);

  // data_ready means:
  // (1) all redo of current PartTransTask are collected(already in storage if needed)
  // (2) then iterator of sorted_redo_list is inited(will be used in redo_dispatcher and sorter)
  void set_data_ready();
  int wait_data_ready(const int64_t timeout);

  int64_t dec_ref_cnt();
  void set_ref_cnt(const int64_t ref_cnt);
  int64_t get_ref_cnt() const { return ref_cnt_; }

  void set_global_trans_seq(const int64_t seq) { global_trans_seq_ = seq; }
  int64_t get_global_trans_seq() const { return global_trans_seq_; }

  void set_global_schema_version(const int64_t global_schema_version) { global_schema_version_ = global_schema_version; }
  int64_t get_global_schema_version() const { return global_schema_version_; }

  void set_next_task(PartTransTask *next) { next_task_ = next; }
  PartTransTask *next_task() { return next_task_; }

  int64_t get_global_trans_version() const { return global_trans_version_; }

  common::ObIAllocator &get_allocator() { return allocator_; }

  const transaction::ObPartitionLogInfo *get_participants() const
  {
    return participants_;
  }

  int64_t get_participant_count() const
  {
    return participant_count_;
  }

  // Retrieve the last digit of reserve_field_
  bool has_valid_binlog_record() const
  {
    return reserve_field_ & 0x01;
  }

  // Set the last digit of reserve_field_ to 1
  void set_has_valid_binlog_record()
  {
    reserve_field_ |= 0x01;
  }

  // for unittest start
  int set_prepare_log_id(const uint64_t prepare_log_id);
  int set_participants(const transaction::PartitionLogInfoArray &participants);
  // for unittest end

  void set_allocator(const int64_t page_size,
      common::ObIAllocator &large_allocator);

  void set_prealloc_page(void *page);
  void revert_prealloc_page(void *&page);

  const ObString &get_trace_id() const { return trace_id_; }
  const ObString &get_trace_info() const { return trace_info_; }
  const ObString &get_pkey_and_log_id_str() const { return pkey_and_log_id_str_; }
  uint64_t &get_row_no() { return row_no_; }
  int to_string_trans_id();

  int revert_by_rollback_savepoint(const uint64_t row_index, const int64_t rollback_no);
  bool is_served() const { return SERVED == serve_state_; }
  void is_part_trans_sort_finish() const { sorted_redo_list_.is_dml_stmt_iter_end(); }
  bool is_part_dispatch_finish() const { return sorted_redo_list_.is_dispatch_finish(); }
  void inc_sorted_br() { ATOMIC_INC(&output_br_count_by_turn_); }
  // get and reset sorted br count
  int64_t get_and_reset_sorted_br() { return ATOMIC_TAS(&output_br_count_by_turn_, 0); }
  /// get next dml_redo_node to dispatch(to reader or parser)
  /// note: if is_last_redo is true, dispatcher can't access PartTransTask because trans may be recycled any time
  /// @param dml_redo_node    [out] dml_redo_node to dispatch
  /// @param is_last_redo     [out] is last redo node in current PartTransTask
  /// @retval OB_SUCCESS      succ get redo_node
  /// @retval OB_EMPTY_RESULT no more redo to dispatch:
  ///                           (1) no redo in this part_trans_task or
  ///                           (2) all redo dispatched then call this functin again(should not happen)
  /// @retval other_code      unexpected error
  int next_redo_to_dispatch(DmlRedoLogNode *&dml_redo_node, bool &is_last_redo);
  /// get next dml stmt for sorter, the br in dml_stmt_task will append to br_commit_queue in trans_ctx.
  /// Theoretically this function faces the same problem with next_redo_to_dispatch, however it can safely access by sorter after the last stmt is outputed,
  /// THe safety is guaranteed by:
  ///   (1) sorter will set Trans state to TRANS_SORTED  after get OB_ITER_END from all participants
  ///   (2) resource_collector will just decrement ref-count of TransCtx if all br of PartTransTask are consumed
  ///   (3) committer will recycle all participants(PartTransTask) after Trans state is TRANS_SORTED
  /// note: should think about whether change this function or not while changing the recycle logic of PartTrasnTask
  ///
  /// @param dml_stmt_task    [out] dml_stmt_task get from next formatted redo(if contains valid br)
  /// @retval OB_SUCCESS      succ get dml_stmt_task
  /// @retval OB_ITER_END     all stmt are output:
  ///                           (1) part_trans_task has no valid br at all or
  ///                           (2) all br are outputed to sorter(the caller)
  /// @retval OB_NEED_RETRY   the redo node to find stmt is not formatted
  int next_dml_stmt(DmlStmtTask *&dml_stmt_task);
  RollbackList& get_rollback_list() { return rollback_list_; }
  // is dispatched redo all sorted:
  OB_INLINE bool is_dispatched_redo_be_sorted() const
  {
    return ! sorted_redo_list_.has_dispatched_but_unsorted_redo();
  }

  TO_STRING_KV("state", serve_state_,
      "type", print_task_type(type_),
      K_(type),
      K_(exec_tenant_id),
      K_(partition),
      K_(timestamp),
      K_(trans_id),
      K_(prepare_log_id),
      K_(cluster_id),
      K_(row_no),
      K_(sorted_redo_list),
      K_(global_trans_version),
      K_(is_trans_committed),
      K_(checkpoint_seq),
      K_(global_trans_seq),
      K_(global_schema_version),
      KP_(participants),
      K_(participant_count),
      K_(local_schema_version),
      K_(stmt_list),
      KP_(next_task),
      K_(ref_cnt),
      K_(is_data_ready),
      KP_(wait_formatted_cond),
      K_(trace_id),
      K_(trace_info));

private:
  int init_trace_id_(const ObString &trace_id);
  int init_trace_info_(const ObString &trace_info);
  int to_string_pkey_and_log_id_(const uint64_t prepare_log_id);
  int init_participant_array_(const transaction::PartitionLogInfoArray &participants);
  void destroy_participant_array_();
  bool is_base_trans_info_valid_() const;
  int set_commit_info_(const int64_t global_trans_version,
      const transaction::PartitionLogInfoArray &participants);
  // 1. memory mode: all data is in memory,
  // 2. storage mode: all data need be stored
  // 3. auto mode:
  bool need_store_data_() const;
  int free_big_row_();
  // Handling of row start
  int push_redo_on_row_start_(const bool need_store_data,
      const transaction::ObTransID &trans_id,
      const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const int32_t log_offset,
      const char *redo_data,
      const int64_t redo_data_size);
  int push_ddl_redo_on_row_start_(const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const char *redo_data,
      const int64_t redo_data_size,
      const int64_t mutator_row_size);
  int push_dml_redo_on_row_start_(const bool need_store_data,
      const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const int32_t log_offset,
      const char *redo_data,
      const int64_t redo_data_size,
      const int64_t mutator_row_size);
  // handle non-row-start for lob
  int push_redo_on_not_row_start_(const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const char *redo_data,
      const int64_t redo_data_size);
  int push_ddl_redo_on_not_row_start_(const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const char *redo_data,
      const int64_t redo_data_size);
  int push_dml_redo_on_not_row_start_(const memtable::ObMemtableMutatorMeta &meta,
      const int64_t log_no,
      const uint64_t log_id,
      const char *redo_data,
      const int64_t redo_data_size);
  int parse_rollback_savepoint_(const uint64_t log_id,
      const int32_t log_offset,
      const char *data_buf,
      const int64_t data_len);
  int get_and_submit_store_task_(const uint64_t tenant_id,
      const uint8_t row_flags,
      const uint64_t log_id,
      const int32_t log_offset,
      const char *data_buf,
      const int64_t data_len);
  int revert_ddl_stmt_(const int64_t seq_no);
  int handle_elr_prev_trans_(PartTransDispatcher &part_trans_dispatcher,
      const int64_t first_log_ts);
  int handle_elr_follow_trans_(PartTransDispatcher &part_trans_dispatcher);
  int commit_elr_trans_(const int64_t prev_trans_arr_cnt,
      const int64_t prev_trans_commit_cnt,
      const char *commit_reason,
      PartTransDispatcher &part_trans_dispatcher);
  // Record the list of succeeding transactions for the current transaction
  typedef common::ObSEArray<transaction::ObTransID, 1> ElrFollowTransIdArray;
  int init_trans_id_info_(const common::ObPartitionKey &pkey,
    const transaction::ObTransID &trans_id);

  int check_dml_redo_node_ready_and_handle_();
  int handle_unserved_trans_();
  void set_unserved_() { serve_state_ = UNSERVED; }
  bool is_data_ready() const { return ATOMIC_LOAD(&is_data_ready_); }

private:
  ServedState             serve_state_;
  TaskType                type_;                  // task type
  uint64_t                exec_tenant_id_;        // record tenant_id for DDL task

  common::ObPartitionKey  partition_;             // partition Key
  const char              *pkey_str_;
  int64_t                 timestamp_;             // Transaction timestamp, usually set to the Prepare log timestamp
  bool                    is_trans_id_inited_;    // Indicates whether trans_id is initialized
  transaction::ObTransID  trans_id_;              // trans ID
  ObString                trans_id_str_;          // string value of trans ID
  uint64_t                prepare_log_id_;        // Prepare log ID, if there is no Prepare transaction for a single partition, the last one shall prevail
  uint64_t                cluster_id_;            // cluster ID
  uint64_t                cluster_version_;       // cluster version when trans start, should be same for all TransLog in the same Trans

  ObString                pkey_and_log_id_str_;   // store pkey + logId to_cstring
  uint64_t                row_no_;                // for alloc global row_no

  SortedRedoLogList       sorted_redo_list_;      // ordered redo list
  DmlRedoLogNode          big_row_dml_redo_node_; // For big row

  RollbackList            rollback_list_;         // Rollback list

  int64_t                 global_trans_version_;  // Global transaction version, transaction commit version

  // whether the transaction has been committed, i.e. whether the commit log has arrived and the whole transaction is complete
  // This variable is only relevant for DML transactions and DDL transactions
  bool                    is_trans_committed_;

  // Whether or not it has been pre-committed, for standalone multipartition transactions
  // This variable is only relevant for DML transactions and DDL transactions
  bool                    is_trans_ready_to_commit_;

  // checkpoint seq number
  //
  // The Fetcher assigns a seq number to all tasks that are sent down and periodically calculates the seq information to be sent down via heartbeat tasks
  // Committer sorts tasks arriving out of order based on the seq number and maintains the overall data seq by processing the tasks sequentially
  int64_t                 checkpoint_seq_;

  // Transaction serial number assigned by sequencer globally
  // Distributed transaction level, partitioned transactions within a distributed transaction have the same number
  int64_t                 global_trans_seq_;
  int64_t                 global_schema_version_;

  // participants info
  transaction::ObPartitionLogInfo *participants_;
  int64_t                         participant_count_;

  // Data parsed from Redo
  int64_t                 local_schema_version_;  // Schema versions for partitioned transactions
  StmtList                stmt_list_;             // statement list

  // PartTransTask linked list structure
  // list of participants in Sequencer
  // Fetcher for linking all partition transactions
  PartTransTask           *next_task_;

  // state variables
  int64_t                 ref_cnt_;                // ref count

  common::ObByteLock      data_ready_lock_;

  // For DDL partition: whether the formatting is complete
  // For DML partition: whether the all logs which need store have stored
  // Note: DML partition: empty redo scene, is_data_ready_ = true
  bool                    is_data_ready_;

  // To optimise memory usage, the condition variable is passed in externally
  common::ObCond          *wait_formatted_cond_;
  common::ObCond          wait_data_ready_cond_;

  ObSmallArena            allocator_;              // allocator

  // App Trace ID
  ObString                trace_id_;
  // App Trace Info
  ObString                trace_info_;

  // Support for early unlocking of lines
  transaction::ObElrTransInfoArray prev_trans_arr_;         // Array of precursor transactions
  ElrFollowTransIdArray            follow_trans_arr_;       // Array of successor transactions
  // An 8 bit reserved field:
  // The lowest bit currently represents whether the partition contains a valid DML binlog_record (for DML only)
  int8_t                  reserve_field_;           // reserved field
  int64_t                 output_br_count_by_turn_; // sorted br count in each statistic round

private:
  DISALLOW_COPY_AND_ASSIGN(PartTransTask);
};

}
}
#endif
