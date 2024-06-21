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
 * Transaction Structures
 */

#ifndef OCEANBASE_LIBOBCDC_TRANS_TASK_H__
#define OCEANBASE_LIBOBCDC_TRANS_TASK_H__

#include "lib/queue/ob_link.h"                      // ObLink
#include "lib/atomic/ob_atomic.h"                   // ATOMIC_LOAD
#include "lib/lock/ob_small_spin_lock.h"            // ObByteLock
#include "common/object/ob_object.h"                // ObObj
#include "common/ob_queue_thread.h"                 // ObCond
#include "ob_cdc_tablet_to_table_info.h"            // ObCDCTabletChangeInfo
#include "storage/tx/ob_trans_define.h"             // ObTransID, ObLSLogInfoArray
#include "storage/tx/ob_tx_big_segment_buf.h"       // ObTxBigSegmentBuf
#include "storage/memtable/ob_memtable_mutator.h"   // ObMemtableMutatorRow, ObMemtableMutatorMeta
#include "storage/blocksstable/ob_datum_row.h"      // ObRowDml
#include "logservice/data_dictionary/ob_data_dict_storager.h"  // ObDataDictStorage

#include "ob_log_trans_log.h"                       // SortedRedoLogList
#include "ob_cdc_multi_data_source_info.h"          // MultiDataSourceNode, MultiDataSourceInfo
#include "ob_log_rollback_section.h"                // RollbackList
#include "ob_log_lighty_list.h"                     // LightyList
#include "ob_log_all_ddl_operation_schema_info.h"   // ObLogAllDdlOperationSchemaInfo
#include "ob_small_arena.h"                         // ObSmallArena
#include "ob_log_task_pool.h"                       // TransTaskBase
#include "ob_log_utils.h"                           // is_ddl_table
#include "ob_log_resource_recycle_task.h"           // ObLogResourceRecycleTask
#include "ob_log_callback.h"                        // ObILogCallback
#include "ob_cdc_lob_ctx.h"                         // ObLobDataOutRowCtxList
#include "ob_cdc_lob_aux_table_schema_info.h"       // ObCDCLobAuxTableSchemaInfo
#include "lib/allocator/ob_lf_fifo_allocator.h"     // ObConcurrentFIFOAllocator
#include "ob_log_safe_arena.h"
#include "ob_log_tic_update_info.h"                 // TICUpdateInfo

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

namespace libobcdc
{
class PartTransTask;
class ObLogBR;
class ObLogEntryTask;
class TableSchemaInfo;
class ObCDCUdtValueMap;

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

  static const char *print_stmt_type(const int type)
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

  TO_STRING_KV(
      K_(type),
      "type_str", print_stmt_type(type_),
      K_(hash_value),
      K_(row_index),
      KP_(br),
      K_(host),
      KP_(next));

protected:
  StmtType           type_;
  PartTransTask      &host_;        // part trans task the stmt belongs to
  uint64_t           hash_value_;   // HASH value
  uint64_t           row_index_;    // row index for the stmt in the trans it belongs
  ObLogBR            *br_;
  IStmtTask          *next_;

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
  union {
    uint8_t column_flags_;
    struct {
      uint8_t   is_out_row_       : 1;  // Column data is stored out row
      uint8_t   is_col_nop_       : 1;  // Column data is nop
      uint8_t   reserve_fields_   : 6;  // reserve fileds
    };
  };

  // if this ColValue is group value
  // then children_ store group hidden ColValue
  LightyList<ColValue> children_;

  void reset()
  {
    value_.reset();
    column_id_ = common::OB_INVALID_ID;
    string_value_.reset();
    next_ = NULL;
    column_flags_ = 0;
    children_.reset();
  }

  bool is_valid()
  {
    return value_.is_valid_type() && common::OB_INVALID_ID != column_id_;
  }

  ColValue *get_next() { return next_; }
  void set_next(ColValue *next) { next_ = next; }

  bool is_json() const { return value_.is_json(); }
  bool is_geometry() const { return value_.is_geometry(); }
  bool is_roaringbitmap() const { return value_.is_roaringbitmap(); }
  common::ObObjType get_obj_type() const { return value_.get_type(); }

  int add_child(ColValue *child) {return children_.add(child);}

  TO_STRING_KV(
      "type", common::ob_obj_type_str(get_obj_type()),
      K_(value),
      K_(column_id),
      K_(string_value),
      K_(is_out_row),
      K_(is_col_nop));
};

typedef LightyList<ColValue> ColValueList;
class ObObj2strHelper;

//////////////////////////////////////// MutatorRow ///////////////////////////////////////////////
class MutatorRow
{
public:
  explicit MutatorRow(common::ObIAllocator &allocator);
  virtual ~MutatorRow();
public:
  common::ObIAllocator &get_allocator() { return allocator_; }
  void reset();
  // Parse the column data
  // If obj2str_helper is empty, do not convert obj to string
  virtual int parse_cols(
      ObObj2strHelper *obj2str_helper = NULL,
      const uint64_t tenant_id = OB_INVALID_TENANT_ID,
      const uint64_t table_id = OB_INVALID_ID,
      const TableSchemaInfo *tb_schema_info = NULL,
      const ObTimeZoneInfoWrap *tz_info_wrap = nullptr,
      const bool enable_output_hidden_primary_key = false,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info = NULL) = 0;
  // Parse the column data based on ObTableSchema
  virtual int parse_cols( const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info) = 0;
  virtual int parse_ext_info_log(ObString &ext_info_log) = 0;
  int get_cols(
      ColValueList **rowkey_cols,
      ColValueList **new_cols,
      ColValueList **old_cols,
      ObLobDataOutRowCtxList **new_lob_ctx_cols);
  ObLobDataOutRowCtxList &get_new_lob_ctx_cols() { return new_lob_ctx_cols_; }
  virtual uint64_t hash(const uint64_t hash) const = 0;
  virtual uint64_t get_table_id() const = 0;
  virtual blocksstable::ObDmlRowFlag get_dml_flag() const  = 0;
  virtual const transaction::ObTxSEQ &get_seq_no() const = 0;

public:
  TO_STRING_KV(
     K_(deserialized),
     K_(cols_parsed),
     K_(new_cols),
     K_(old_cols),
     K_(rowkey_cols));

protected:
  int parse_columns_(
      const bool is_parse_new_col,
      const blocksstable::ObDatumRow &datum_row,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const int64_t rowkey_cnt,
      ObObj2strHelper *obj2str_helper,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const TableSchemaInfo *tb_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool enable_output_hidden_primary_key,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
      const bool is_macroblock_row,
      ColValueList &cols);
  int parse_rowkey_(
      ColValueList &rowkey_cols,
      const common::ObStoreRowkey &rowkey,
      ObObj2strHelper *obj2str_helper,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const TableSchemaInfo *tb_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool enable_output_hidden_primary_key);
  int deep_copy_encoded_column_value_(blocksstable::ObStorageDatum &datum);
  // 1. get column_id and column_schema_info for user table;
  // 2. get column_id for all_ddl_operation_table
  int get_column_info_(
      const TableSchemaInfo *tb_schema_info,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
      const int64_t column_idx,
      const bool is_rowkey_column_idx,
      uint64_t &column_id,
      ColumnSchemaInfo *&column_schema_info) const;
  int add_column_(
      const uint64_t column_id,
      const ObObj *value,
      const bool is_out_row,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const ColumnSchemaInfo *column_schema,
      const ObObj2strHelper *obj2str_helper,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      ColValueList &cols,
      ObCDCUdtValueMap *udt_value_map);
  int set_obj_propertie_(
      const uint64_t column_id,
      const int64_t column_idx_for_datum_row,
      const ColumnSchemaInfo *column_schema_info,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info,
      ObObjMeta &obj_meta,
      ObObj &obj);

  // InnerTable parse
  template<class CDC_INNER_TABLE_SCHEMA>
  int parse_columns_(
      const bool is_parse_new_col,
      const blocksstable::ObDatumRow &datum_row,
      const int64_t rowkey_cnt,
      const CDC_INNER_TABLE_SCHEMA &inner_table_schema,
      const bool is_macroblock_row,
      ColValueList &cols);
  template<class TABLE_SCHEMA>
  int parse_rowkey_(
      const TABLE_SCHEMA &table_schema,
      const common::ObStoreRowkey &rowkey,
      ColValueList &rowkey_cols);
  int add_column_(
      ColValueList &cols,
      const uint64_t column_id,
      const ObObj *value);
  template<class TABLE_SCHEMA>
  int set_obj_propertie_(
      const uint64_t column_id,
      const int64_t column_idx_for_datum_row,
      const TABLE_SCHEMA &table_schema,
      ObObjMeta &obj_meta,
      ObObj &obj);

protected:
  common::ObIAllocator     &allocator_;
  bool                     deserialized_;
  bool                     cols_parsed_;
  ColValueList             new_cols_;     // A list of new values for the columns, currently no primary key values are stored, only normal columns
  ColValueList             old_cols_;     // A list of old values for the columns, currently no primary key values are stored, only normal columns
  ColValueList             rowkey_cols_;  // rowkey column
  ObLobDataOutRowCtxList   new_lob_ctx_cols_;
private:
  DISALLOW_COPY_AND_ASSIGN(MutatorRow);
};
//////////////////////////////////////// MutatorRow ///////////////////////////////////////////////

//////////////////////////////////////// MacroBlockMutatorRow ///////////////////////////////////////////////
class MacroBlockMutatorRow : public MutatorRow
{
public:
  explicit MacroBlockMutatorRow(
      common::ObIAllocator &allocator,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const transaction::ObTxSEQ &seq_no);

  virtual ~MacroBlockMutatorRow();
public:
  int init(ObIAllocator &allocator, const blocksstable::ObDatumRow &src_datum_row, const common::ObStoreRowkey &src_row_key);
  void reset();
  // Parse the column data
  // If obj2str_helper is empty, do not convert obj to string
  int parse_cols(
      ObObj2strHelper *obj2str_helper = NULL,
      const uint64_t tenant_id = OB_INVALID_TENANT_ID,
      const uint64_t table_id = OB_INVALID_ID,
      const TableSchemaInfo *tb_schema_info = NULL,
      const ObTimeZoneInfoWrap *tz_info_wrap = nullptr,
      const bool enable_output_hidden_primary_key = false,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info = NULL);
  // Parse the column data based on ObTableSchema
  int parse_cols(const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info);
  int parse_ext_info_log(ObString &ext_info_log);
  uint64_t hash(const uint64_t hash) const { return row_key_.murmurhash(hash); }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }
  blocksstable::ObDmlRowFlag get_dml_flag() const { return dml_flag_; }
  const transaction::ObTxSEQ &get_seq_no() const { return seq_no_; }

public:
  TO_STRING_KV(
      K_(is_inited),
      K_(table_id),
      K_(dml_flag),
      K_(seq_no),
      K_(row),
      K_(row_key));
private:
  bool                              is_inited_;
  uint64_t                          table_id_;
  blocksstable::ObDmlRowFlag        dml_flag_;
  transaction::ObTxSEQ              seq_no_;
  blocksstable::ObDatumRow          row_;
  common::ObStoreRowkey             row_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(MacroBlockMutatorRow);
};

//////////////////////////////////////// MacroBlockMutatorRow ///////////////////////////////////////////////

//////////////////////////////////////// MemtableMutatorRow ///////////////////////////////////////////////
class MemtableMutatorRow : public memtable::ObMemtableMutatorRow, public MutatorRow
{
public:
  explicit MemtableMutatorRow(common::ObIAllocator &allocator);
  virtual ~MemtableMutatorRow();

public:
  // Deserialize a row
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);

  // Support for filtering table data within PG
  // Deserialize some fields: first step to get row_size, table_id
  int deserialize_first(
      const char *buf,
      const int64_t data_len,
      int64_t &pos,
      int32_t &row_size);

  // Deserialize some fields: Step 2 continues the parsing to get the table_version
  int deserialize_second(
      const char *buf,
      const int64_t data_len,
      int64_t &pos,
      int64_t &table_version);

  void reset();

  // Parse the column data
  // If obj2str_helper is empty, do not convert obj to string
  int parse_cols(
      ObObj2strHelper *obj2str_helper = NULL,
      const uint64_t tenant_id = OB_INVALID_TENANT_ID,
      const uint64_t table_id = OB_INVALID_ID,
      const TableSchemaInfo *tb_schema_info = NULL,
      const ObTimeZoneInfoWrap *tz_info_wrap = nullptr,
      const bool enable_output_hidden_primary_key = false,
      const ObLogAllDdlOperationSchemaInfo *all_ddl_operation_table_schema_info = NULL);

  // Parse the column data based on ObTableSchema
  int parse_cols(const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info);
  int parse_ext_info_log(ObString &ext_info_log);
  uint64_t hash(const uint64_t hash) const { return rowkey_.murmurhash(hash); }
  uint64_t get_table_id() const { return table_id_; }
  blocksstable::ObDmlRowFlag get_dml_flag() const { return dml_flag_; }
  const transaction::ObTxSEQ &get_seq_no() const { return seq_no_; }

public:
  TO_STRING_KV(
    "MutatorRow", static_cast<const MutatorRow &>(*this),
    "MemtableMutatorRow", static_cast<const memtable::ObMemtableMutatorRow &>(*this));
private:
  DISALLOW_COPY_AND_ASSIGN(MemtableMutatorRow);
};

//////////////////////////////////////// MemtableMutatorRow ///////////////////////////////////////////////
#define DELIMITER_STR "_"

// The DML unique ID is part_trans_id(tenant_id + ls_id + trans_id) + redo_log_lsn  + row_index, the separator is `_`
// tenant_id+ls_id+trans_id+redo_log_lsn could locate an unique log_entry for a trans,
// an redo_log_lsn locate a unique redo_log in a trans,
// row_index locate a specifed row recorded in a redo_log
class DmlStmtUniqueID
{
public:
  DmlStmtUniqueID(
      const ObString &part_trans_info_str,
      const palf::LSN &redo_log_lsn,
      const uint64_t row_index) :
    part_trans_info_str_(part_trans_info_str),
    redo_log_lsn_(redo_log_lsn),
    row_index_(row_index) {}

  ~DmlStmtUniqueID() { reset(); }
public:
  void reset()
  {
    // part_trans_info_str_.reset(); part_trans_info_str_ is a reference, should not reset it.
    redo_log_lsn_.reset();
    row_index_ = OB_INVALID_ID;
  }

  bool is_valid() const
  { return !part_trans_info_str_.empty() && redo_log_lsn_.is_valid() && OB_INVALID_ID != row_index_; }
  const palf::LSN &get_redo_lsn() const { return redo_log_lsn_; }
  uint64_t get_row_index() const { return row_index_; }
  // get length of serialized DmlStmtUniqueID(with '\0')
  int64_t get_dml_unique_id_length() const;

public:
  // row_index(uint64_t): to_cstring长度20
  static const int64_t MAX_ROW_INDEX_LENGTH = 20;
  template<typename NUM> static int64_t compute_str_length_base_num(const NUM num_to_compute);
  // Optimising customisation to_string
  int customized_to_string(char *buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(part_trans_info_str), K_(redo_log_lsn), K_(row_index));

private:
  const ObString &part_trans_info_str_; // str of tenant_id + ls_id + trans_id
  palf::LSN redo_log_lsn_; // redo_log_lsn
  uint64_t row_index_;

private:
  DISALLOW_COPY_AND_ASSIGN(DmlStmtUniqueID);
};

// DML statement task
class DmlStmtTask : public IStmtTask
{
public:
  DmlStmtTask(PartTransTask &host,
      ObLogEntryTask &log_entry_task,
      MutatorRow &row);
  virtual ~DmlStmtTask();

  void reset();

  int64_t get_global_schema_version() const;
  int64_t get_table_version() const { return 0; }
  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  const logservice::TenantLSID &get_tls_id() const;
  // TODO(fankun.fan)
  // const common::ObStoreRowkey &get_rowkey() const {}
  blocksstable::ObDmlRowFlag get_dml_flag() const { return row_.get_dml_flag(); }
  bool is_insert() const { return row_.get_dml_flag().is_insert(); }
  bool is_update() const { return row_.get_dml_flag().is_update(); }
  bool is_delete() const { return row_.get_dml_flag().is_delete(); }
  bool is_put() const { return row_.get_dml_flag().is_delete_insert(); }

  // Parse the column data
  // If obj2str_helper is empty, then no conversion of obj to string
  // NOTE: you can get_cols() only if you succeed in parse_cols()
  int parse_cols(
      ObObj2strHelper *obj2str_helper = NULL,
      const TableSchemaInfo *tb_schema_info = NULL,
      const ObTimeZoneInfoWrap *tz_info_wrap = nullptr,
      const bool enable_output_hidden_primary_key = false);

  int parse_aux_meta_table_cols(const ObCDCLobAuxTableSchemaInfo &lob_aux_table_schema_info);

  // For the JSON or GIS(outrow storage)
  // The JSON/GIS data column size is over 4K and is outrow storage, reusing the basic capabilities of LOB.
  // So we need to call the obj2str API to get the final message format when the complete data is retrieved.
  int parse_col(
      const uint64_t tenant_id,
      const uint64_t column_id,
      const ColumnSchemaInfo &column_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      ObObj2strHelper &obj2str_helper,
      ColValue &cv_node);

  int get_cols(
      ColValueList **rowkey_cols,
      ColValueList **new_cols,
      ColValueList **old_cols,
      ObLobDataOutRowCtxList **new_lob_ctx_cols)
  {
    return row_.get_cols(rowkey_cols, new_cols, old_cols, new_lob_ctx_cols);
  }

  ObLobDataOutRowCtxList &get_new_lob_ctx_cols() { return row_.get_new_lob_ctx_cols(); }

  ObLogEntryTask &get_redo_log_entry_task() { return log_entry_task_; }

  const transaction::ObTxSEQ &get_row_seq_no() const { return row_.get_seq_no(); }

  bool is_callback() const { return 1 == is_callback_; }
  void mark_callback() { is_callback_ = 1; }

public:
  TO_STRING_KV("IStmtTask", static_cast<const IStmtTask &>(*this),
      "is_cb", is_callback_,
      "seq_no", row_.get_seq_no(),
      "dml_flag", row_.get_dml_flag(),
      K_(table_id),
      K_(row),
      K_(log_entry_task));

private:
  uint8_t is_callback_ : 1;
  ObLogEntryTask          &log_entry_task_;
  uint64_t                table_id_;
  MutatorRow              &row_;
private:
  DISALLOW_COPY_AND_ASSIGN(DmlStmtTask);
};

///////////////////////////////////////////////////////////////////////////////

// DDL unique ID using tenant_id + schema_version
class DdlStmtUniqueID
{
public:
  DdlStmtUniqueID(const uint64_t tenant_id, const uint64_t schema_version) :
    tenant_id_(tenant_id), schema_version_(schema_version) {}
  ~DdlStmtUniqueID() { reset(); }
public:
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    schema_version_ = OB_INVALID_TIMESTAMP;
  }

  bool is_valid() const
  { return OB_INVALID_TENANT_ID != tenant_id_
    && OB_INVALID_TIMESTAMP != schema_version_; }

  uint64_t get_schema_version() const { return schema_version_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

public:
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  uint64_t    tenant_id_;        // TenantId
  uint64_t    schema_version_;   // schema version

private:
  DISALLOW_COPY_AND_ASSIGN(DdlStmtUniqueID);
};

class DdlStmtTask : public IStmtTask
{
public:
  DdlStmtTask(PartTransTask &host, MutatorRow &row);
  virtual ~DdlStmtTask();

public:
  void reset();

  // parse DDL data
  // init DDL Binlog Record
  int parse_ddl_info(
      ObLogBR *br,
      const uint64_t row_index,
      const ObLogAllDdlOperationSchemaInfo &all_ddl_operation_table_schema_info,
      const bool is_build_baseline,
      bool &is_valid_ddl,
      int64_t &update_schema_version,
      uint64_t &exec_tennat_id,
      volatile bool &stop_flag);

  // get ddl str
  const ObString &get_ddl_stmt_str() const { return ddl_stmt_str_; }
  int64_t get_operation_type() const { return ddl_operation_type_; }
  uint64_t get_op_table_id() const { return ddl_op_table_id_; }
  uint64_t get_op_tenant_id() const { return ddl_op_tenant_id_; }
  uint64_t get_op_database_id() const { return ddl_op_database_id_; }
  uint64_t get_op_tablegroup_id() const { return ddl_op_tablegroup_id_; }
  int64_t get_op_schema_version() const { return ddl_op_schema_version_; }
  uint64_t get_exec_tenant_id() const { return ddl_exec_tenant_id_; }
  const transaction::ObTxSEQ &get_row_seq_no() const { return row_.get_seq_no(); }

public:
  // tennat_id(UINT64_MAX: 20) + schema_version(INT64_MAX:19)
  static const int64_t MAX_DDL_UNIQUE_ID_LENGTH = 50;
  // schema_version(INT64_MAX:19)
  static const int64_t MAX_DDL_SCHEMA_VERSION_STR_LENGTH = 20;
  // log id (INT64_MAX:19)
  static const int64_t MAX_PREPRAR_LOG_ID_LENGTH = 20;

public:
  TO_STRING_KV(
      "IStmtTask", static_cast<const IStmtTask &>(*this),
      K_(row),
      K_(ddl_exec_tenant_id),
      K_(ddl_stmt_str),
      K_(ddl_op_schema_version),
      K_(ddl_operation_type),
      K_(ddl_op_table_id),
      K_(ddl_op_tenant_id),
      K_(ddl_op_database_id),
      K_(ddl_op_tablegroup_id));

private:
  int parse_ddl_info_(
      const bool is_build_baseline,
      bool &contain_ddl_stmt,
      int64_t &update_schema_version,
      volatile bool &stop_flag);
  int parse_schema_version_(ObObj &col_value, int64_t &schema_version);
  int parse_ddl_info_from_normal_columns_(
      const bool is_build_baseline,
      ColValueList &col_value_list,
      ObLobDataOutRowCtxList &new_lob_ctx_cols);
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
  // Offline DDL: create hidden table
  bool is_create_table_ddl_(const int64_t ddl_operation_type);
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
  bool is_sub_tls_id_alter_ddl_(const int64_t ddl_operation_type);
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

private:
  DISALLOW_COPY_AND_ASSIGN(DdlStmtTask);
};

/////////////////////////////////////////////////////////////////////////////////

typedef LightyList<IStmtTask> StmtList;

class ObLogEntryTask
{
public:
  ObLogEntryTask(PartTransTask &host, const bool is_direct_load_inc_log = false);
  virtual ~ObLogEntryTask();
  void reset();
  bool is_valid() const;

public:
  int init(
      const logservice::TenantLSID &tls_id,
      const char *participant,
      const transaction::ObTransID &trans_id,
      DmlRedoLogNode *redo_node);

  uint64_t hash() const
  {
    uint64_t hash_value = get_tls_id().hash();

    return hash_value;
  }

public:
  int get_status(bool &is_stored);
  int get_storage_key(std::string &key);

  inline void *get_host() { return host_; }
  inline void set_host(void *host) { host_ = host; }

  const logservice::TenantLSID &get_tls_id() const { return tls_id_; }
  uint64_t get_tenant_id() const { return tls_id_.get_tenant_id(); }
  const share::ObLSID &get_ls_id() const { return tls_id_.get_ls_id(); }
  bool is_sys_ls_task() const { return tls_id_.is_sys_log_stream(); }

  const transaction::ObTransID &get_trans_id() const { return trans_id_; }

  int get_valid_row_num(int64_t &valid_row_num);

  common::ObIAllocator &get_allocator() { return arena_allocator_; }
  void *alloc(const int64_t size);
  void free(void *ptr);

  const DmlRedoLogNode *get_redo_log_node() const { return redo_node_; }
  int get_log_lsn(palf::LSN &log_lsn); // get redo log lsn: may not unique cause multi redo in one log_entry
  int get_data_len(int64_t &data_len);

  int set_data_and_readed_status(bool is_big_row, char *data, int64_t data_len);
  int rc_callback();

  const StmtList &get_stmt_list() const { return stmt_list_; }
  StmtList &get_stmt_list() { return stmt_list_; }
  int64_t get_stmt_num() const { return stmt_list_.num_; }
  int add_stmt(const uint64_t row_index, IStmtTask *stmt_task);

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

  TO_STRING_KV(
      K_(tls_id),
      K_(trans_id),
      KPC_(redo_node),
      K_(stmt_list),
      K_(formatted_stmt_num),
      K_(row_ref_cnt));

private:
  int revert_binlog_record_(ObLogBR *br);

private:
  void                   *host_;            // PartTransTask host

  const char             *participant_;

  logservice::TenantLSID tls_id_;           // logservice::TenantLSID
  transaction::ObTransID trans_id_;         // Transaction ID

  DmlRedoLogNode     *redo_node_;           // dml redo log node
  bool               is_big_row_;

  StmtList           stmt_list_;            // statement list
  int64_t            formatted_stmt_num_;   // Number of statements that formatted
  int64_t            row_ref_cnt_;          // reference count

  // thread safe allocator
  // used for Parser/Formatter/LobDataMerger
  ObCdcSafeArena arena_allocator_;          // allocator

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
    TASK_TYPE_LS_OP_TRANS,        // Log stream operations trans
    TASK_TYPE_LS_HEARTBEAT,       // heartbeat of LS level, used to push ls dispatch progress, to push ls dispatch progress
    TASK_TYPE_GLOBAL_HEARTBEAT,   // heartbeat of global level, used to pass checkpoint info for downstream
    TASK_TYPE_OFFLINE_LS,         // LS offline task
    TASK_TYPE_NOT_SERVED_TRANS,   // not served trans, convert from other trans type
    TASK_TYPE_MAX
  };
  enum ServedState
  {
    UNSERVED = 0,  // LS transaction is abort or LS transaction is not served
    SERVED = 1
  };

public:
  PartTransTask();
  virtual ~PartTransTask();

public:
  static const char *print_task_type(const TaskType type);

public:
  void reset();

  int init_log_entry_task_allocator();

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
  int push_redo_log(
      const transaction::ObTransID &trans_id,
      const palf::LSN &log_lsn,
      const int64_t tstamp,
      const char *buf,
      const int64_t buf_len);

  int push_direct_load_inc_log(
      const transaction::ObTransID &trans_id,
      const palf::LSN &log_lsn,
      const int64_t tstamp,
      const char *buf,
      const int64_t buf_len);

  /**
   * @brief: push rollback_to info:
   * 1. lsn of rollback_to should push into all_recorded_lsns(ObTxRollbackToLog should has independent LogEntry)
   * 2. construct RollbackNode and push into rollback_list_
   * @param {LSN} &lsn lsn of ObTxRollbackToLog
   * @param {ObTxSEQ} rollback_from
   * @param {ObTxSEQ} rollback_to
   * @retval OB_ENTRY_EXIST: the tollback_to node already recorded
   * @retval OB_SUCCESS: op succ
   * @retval other error code: op fail by other reason
   */
  int push_rollback_to_info(const palf::LSN &lsn, const transaction::ObTxSEQ &rollback_from, const transaction::ObTxSEQ &rollback_to);

  /// set PartTrans Commit Info
  /// @param [in] trace_id              app trace id
  /// @param [in] trace_info            app trace info
  /// @retval OB_SUCCESS          Success
  /// @retval Other error codes   Fail
  int set_commit_info(
      const ObString &trace_id,
      const ObString &trace_info,
      const bool is_dup_ts,
      const transaction::ObXATransID &xid);
  /// prepare task.
  /// invoker:
  ///     PartTransResolver::handle_prepare_ for DIST_TRANS
  ///     PartTransResolver::handle_commit_ for SINGLE_LS_TRANS
  int prepare(
      const palf::LSN &prepare_log_lsn,
      const int64_t prepare_ts,
      PartTransDispatcher &part_trans_dispatcher);
  /// Submit a normal transaction task
  /// Requires that the prepares have been successful and that the redo log is complete
  ///
  /// @param [in] cluster_id              the cluster that trans belongs to
  /// @param [in] tx_id                   Transaction ID
  /// @param [in] trans_commit_version    Transaction commit version
  /// @param [in] ls_info_array           <LSID, PrepareLogLSN> array
  /// @param [in] commit_log_lsn          lsn of commit_log.
  /// @param [in] commit_log_submit_ts    submit_ts of commit_log_entry
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int commit(
      const uint64_t cluster_id,
      const transaction::ObTransID &tx_id,
      const int64_t trans_commit_version,
      const transaction::TransType &trans_type,
      const transaction::ObLSLogInfoArray &ls_info_array,
      const palf::LSN &commit_log_lsn,
      const int64_t commit_log_submit_ts,
      const bool is_data_dict_mode);

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

  // Initialize tls_id heartbeat task information
  // Set the type to: TASK_TYPE_LS_HEARTBEAT
  //
  /// @param [in] tls_id        tls_id
  /// @param [in] timestamp     Heartbeat timestamp
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_ls_heartbeat_info(const logservice::TenantLSID &tls_id, const int64_t timestamp);

  // Initialize global heartbeat task information, global heartbeat task is independent of tls_id
  // Set the type to: TASK_TYPE_GLOBAL_HEARTBEAT
  //
  /// @param [in] timestamp          heartbeat timestamp
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_global_heartbeat_info(const int64_t timestamp);

  // Initially offline tls_id task
  // Set the type to: TASK_TYPE_OFFLINE_LS
  //
  // @param [in] tls_id          PartitionKey
  //
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  int init_offline_ls_task(const logservice::TenantLSID &tls_id);

  // is task info valid or not
  bool is_task_info_valid() const;

  // Convert to a non-serviceable tls_ided transaction type
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

  bool is_trans_type_unknown() const { return TASK_TYPE_UNKNOWN == type_; }
  bool is_global_heartbeat() const { return TASK_TYPE_GLOBAL_HEARTBEAT == type_; }
  bool is_ls_heartbeat() const { return TASK_TYPE_LS_HEARTBEAT == type_; }
  bool is_sys_ls_heartbeat() const
  {
    return is_ls_heartbeat() && is_sys_ls_part_trans();
  }
  bool is_sys_ls_part_trans() const { return tls_id_.is_sys_log_stream(); }
  bool is_dml_trans() const { return TASK_TYPE_DML_TRANS == type_; }
  bool is_ddl_trans() const { return TASK_TYPE_DDL_TRANS == type_; }
  bool is_ls_op_trans() const { return TASK_TYPE_LS_OP_TRANS == type_; }
  bool is_offline_ls_task() const { return TASK_TYPE_OFFLINE_LS == type_; }
  // Is it a DDL OFFLINE task
  bool is_sys_ls_offline_task() const
  {
    return is_offline_ls_task() && is_sys_ls_part_trans();
  }
  bool is_not_served_trans() const { return TASK_TYPE_NOT_SERVED_TRANS == type_; }
  bool is_sys_ls_dml_trans() const { return is_dml_trans() && is_sys_ls_part_trans(); }

  void set_task_info(const logservice::TenantLSID &tls_id, const char *info);

  void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }

  uint64_t get_cluster_id() const { return cluster_id_; }

  void set_tls_id(const logservice::TenantLSID &tls_id) { tls_id_ = tls_id; }
  const logservice::TenantLSID &get_tls_id() const { return tls_id_; }
  uint64_t get_tenant_id() const { return tls_id_.get_tenant_id(); }
  const share::ObLSID &get_ls_id() const { return tls_id_.get_ls_id(); }

  const char *get_tenant_ls_str() const { return tls_str_; }

  int64_t get_prepare_ts() const { return prepare_ts_; }
  const palf::LSN &get_prepare_log_lsn() const { return prepare_log_lsn_; }
  void set_commit_ts(const int64_t commit_ts) { commit_ts_ = commit_ts; }
  int64_t get_commit_ts() const { return commit_ts_; }
  const palf::LSN &get_commit_log_lsn() const { return commit_log_lsn_; }

  bool is_trans_committed() const { return ATOMIC_LOAD(&is_trans_committed_); }

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
  int add_ddl_lob_aux_stmt(const uint64_t row_index, DmlStmtTask *stmt_task);
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

  void free_ddl_lob_aux_stmt_list()
  {
    ddl_lob_aux_stmt_list_.reset();
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

  int64_t get_trans_commit_version() const { return trans_commit_version_; }

  common::ObIAllocator &get_allocator() { return allocator_; }

  const transaction::ObLSLogInfoArray &get_participants() const
  {
    return participants_;
  }

  int64_t get_participant_count() const
  {
    return participants_.count();
  }

  // for unittest start
  int set_commit_log_lsn(const palf::LSN &commit_log_lsn);
  int set_participants(
      const transaction::ObLSLogInfoArray &participants,
      const palf::LSN &commit_log_lsn);
  // for unittest end

  void set_allocator(const int64_t page_size,
      common::ObIAllocator &large_allocator);

  void set_prealloc_page(void *page);
  void revert_prealloc_page(void *&page);

  const ObString &get_trace_id() const { return trace_id_; }
  const ObString &get_trace_info() const { return trace_info_; }
  const ObString &get_part_trans_info() const { return part_trans_info_str_; }

  bool is_served() const { return SERVED == serve_state_; }
  bool is_single_ls_trans() const { return transaction::TransType::SP_TRANS == trans_type_; }
  bool is_dist_trans() const { return transaction::TransType::DIST_TRANS == trans_type_; }
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
  const RollbackList &get_rollback_list() const { return rollback_list_; }
  void mark_read_first_record() { part_tx_fetch_state_ |= 0x01; }
  bool has_find_first_record() const { return part_tx_fetch_state_ & 0x01; }
  void mark_read_commit_info() { part_tx_fetch_state_ |= (0x01 << 1); };
  bool has_read_commit_info() const { return part_tx_fetch_state_ & (0x01 << 1); };
  void mark_read_prepare() { part_tx_fetch_state_ |= (0x01 << 2); };
  bool has_read_prepare() const { return part_tx_fetch_state_ & (0x01 << 2); };

  int push_fetched_log_entry(const palf::LSN &lsn);
  int push_back_recored_redo_lsn_arr(
      const transaction::ObRedoLSNArray &prev_redo_lsns,
      const palf::LSN &commit_info_lsn,
      const bool has_redo_in_cur_entry);
  SortedLogEntryInfo &get_sorted_log_entry_info() { return sorted_log_entry_info_; }
  int is_all_redo_log_entry_fetched(bool &is_all_redo_fetched)
  { return sorted_log_entry_info_.is_all_log_entry_fetched(is_all_redo_fetched); };
  // is dispatched redo all sorted:
  OB_INLINE bool is_dispatched_redo_be_sorted() const
  {
    return ! sorted_redo_list_.has_dispatched_but_unsorted_redo();
  }
  transaction::ObTxBigSegmentBuf *get_segment_buf() { return &segment_buf_; }
  int push_multi_data_source_data(
      const palf::LSN &lsn,
      const transaction::ObTxBufferNodeArray &mds_data_arr,
      const bool is_commit_log);
  // parse multi_data_source in multi_data_source_node_arr to multi_data_source_info
  //
  // 1. Support LS_MEMBER_TABLE MDS node.
  // 2. Support Tablet
  int parse_multi_data_source_data();

  // TODO Support single registration over 1.5M
  // 1. parse multi_data_source to get the collection of changed schemas which DDL transactions affect
  int parse_multi_data_source_data_for_ddl(
      const char *caller);

  // sys_ls trans which not ddl_trans/ls_table_trans won't serve
  // TODO: Could this trans is a dist_trans? Should CDC filter this trans if is a dict_trans.
  bool is_sys_ls_not_serve_trans() const
  {
    return tls_id_.is_sys_log_stream() && ! multi_data_source_info_.is_valid();
  }
  const MultiDataSourceInfo &get_multi_data_source_info() const { return multi_data_source_info_; }
  const share::ObLSAttrArray &get_ls_attr_arr() const { return multi_data_source_info_.get_ls_attr_arr(); }
  DictTenantArray &get_dict_tenant_array() { return multi_data_source_info_.get_dict_tenant_array(); }
  DictDatabaseArray &get_dict_database_array() { return multi_data_source_info_.get_dict_database_array(); }
  DictTableArray &get_dict_table_array() { return multi_data_source_info_.get_dict_table_array(); }
  // get tenant_schema_info with MultiDataSourceInfo in DDL. get from baseline data_dict if ddl
  // doesn't contains tenant_meta for specifed tenant, otherwise use tenant_meta in inc_data_dict.
  // NOTICE: ONLY AVALIABLE FOR DDL_TRANS.
  int get_tenant_schema_info_with_inc_dict(const uint64_t tenant_id, TenantSchemaInfo &tenant_schema_info);
  // get database_schema_info with MultiDataSourceInfo in DDL. get from baseline data_dict if ddl
  // doesn't contains database_meta for specifed database, otherwise use database_meta in inc_data_dict
  // NOTICE: ONLY AVALIABLE FOR DDL_TRANS.
  int get_database_schema_info_with_inc_dict(const uint64_t tenant_id, const uint64_t db_id, DBSchemaInfo &db_schema_info);
  // get dict_table_meta with MultiDataSourceInfo in DDL. get from baseline data_dict if ddl
  // doesn't contains table_meta for specifed table, otherwise use table_meta in inc_data_dict.
  // NOTICE: ONLY AVALIABLE FOR DDL_TRANS.
  int get_table_meta_with_inc_dict(const uint64_t tenant_id, const uint64_t table_id, const datadict::ObDictTableMeta *&tb_meta);

  // Check if the DDL transaction needs to be treated as a barrier.
  //
  // @param [out] is_not_barrier is not a barrier
  // @param [out] op_type Schema operation type
  int check_for_ddl_trans(
      bool &is_not_barrier,
      ObSchemaOperationType &op_type) const;
  ObIAllocator &get_log_entry_task_base_allocator() { return log_entry_task_base_allocator_; };
  int push_tic_update_info(const TICUpdateInfo &tic_update_info);
  void get_tic_update_info(ObArray<TICUpdateInfo> &tic_update_infos) const
  {
    tic_update_infos = tic_update_infos_;
  }

  bool need_update_table_id_cache() const
  {
    return !tic_update_infos_.empty();
  }
  void set_unserved() { set_unserved_(); }

  TO_STRING_KV(
      "state", serve_state_,
      "type", print_task_type(type_),
      K_(type),
      K_(cluster_id),
      K_(exec_tenant_id),
      K_(tls_id),
      K_(trans_id),
      "trans_type", transaction::trans_type_to_cstr(trans_type_),
      K_(is_xa_or_dup),
      K_(is_trans_committed),
      K_(trans_commit_version),
      K_(prepare_ts),
      K_(prepare_log_lsn),
      K_(commit_ts),
      K_(commit_log_lsn),
      "participant_count", participants_.count(),
      K_(participants),
      K_(trace_id),
      K_(trace_info),
      K_(sorted_log_entry_info),
      K_(sorted_redo_list),
      K_(rollback_list),
      K_(part_tx_fetch_state),
      K_(ref_cnt),
      K_(multi_data_source_node_arr),
      K_(multi_data_source_info),
      K_(checkpoint_seq),
      K_(global_trans_seq),
      K_(global_schema_version),
      K_(local_schema_version),
      K_(stmt_list),
      K_(is_data_ready),
      KP_(wait_formatted_cond),
      K_(output_br_count_by_turn),
      KP_(next_task));

private:
  int init_trace_id_(const ObString &trace_id);
  int init_trace_info_(const ObString &trace_info);
  int to_string_part_trans_info_();
  int init_participant_array_(
      const transaction::ObLSLogInfoArray &participants,
      const palf::LSN &commit_log_lsn);
  void destroy_participant_array_();
  bool is_base_trans_info_valid_() const;
  // alloc LogEntryNode with specifed LSN, free while PartTransTask destroy.
  int alloc_log_entry_node_(const palf::LSN &lsn, LogEntryNode *&log_entry_node);
  // 1. memory mode: all data is in memory,
  // 2. storage mode: all data need be stored
  // 3. auto mode:
  bool need_store_data_() const;
  // Handling of row start
  int push_redo_on_row_start_(
      const bool need_store_data,
      const transaction::ObTransID &trans_id,
      const memtable::ObMemtableMutatorMeta &meta,
      const palf::LSN &log_lsn,
      const char *redo_data,
      const int64_t redo_data_size);
  int push_ddl_redo_on_row_start_(
      const memtable::ObMemtableMutatorMeta &meta,
      const palf::LSN &log_lsn,
      const char *redo_data,
      const int64_t redo_data_size,
      const int64_t mutator_row_size);
  int push_dml_redo_on_row_start_(
      const bool need_store_data,
      const bool is_direct_load_inc_log,
      const palf::LSN &log_lsn,
      const char *redo_data,
      const int64_t redo_data_size,
      const int64_t mutator_row_size);
  int get_and_submit_store_task_(
      const uint64_t tenant_id,
      const uint8_t row_flags,
      const palf::LSN &log_lsn,
      const char *data_buf,
      const int64_t data_len);

  int check_dml_redo_node_ready_and_handle_();
  int handle_unserved_trans_();
  void set_unserved_() { serve_state_ = UNSERVED; }
  bool is_data_ready() const { return ATOMIC_LOAD(&is_data_ready_); }

  // MultiDataSource Transaction
  int alloc_and_save_multi_data_source_node_(
      const palf::LSN &lsn,
      const transaction::ObTxBufferNode &mds_buffer_node);

  int parse_tablet_change_mds_(
      const MultiDataSourceNode &multi_data_source_node,
      ObCDCTabletChangeInfo &tablet_change_info);

private:
  ServedState             serve_state_;
  // trans basic info
  uint64_t                cluster_id_;            // cluster ID
  TaskType                type_;                  // task type
  uint64_t                exec_tenant_id_;        // record tenant_id for DDL task

  logservice::TenantLSID  tls_id_;                // logservice::TenantLSID
  const char              *tls_str_;
  transaction::ObTransID  trans_id_;              // trans ID
  ObString                part_trans_info_str_;   // tls_str + tx_id
  // whether the transaction has been committed, i.e. whether the commit log has arrived and the whole transaction is complete
  // This variable is only relevant for DML transactions and DDL transactions
  bool                    is_trans_committed_;
  int64_t                 trans_commit_version_;  // Global transaction version, transaction commit version
  int64_t                 prepare_ts_;            // Transaction PrepareLog timestamp, same with commit_ts_ if SINGLE_LS_TRANS
  palf::LSN               prepare_log_lsn_;       // PrepareLog LSN(same with commit_log_lsn_ if SINGLE_LS_TRANS)
  int64_t                 commit_ts_;             // Transaction timestamp, usually set to the Commit log timestamp
  palf::LSN               commit_log_lsn_;        // CommitLog LSN
  transaction::TransType  trans_type_;
  bool                    is_xa_or_dup_;          // true if xa dist trans or duplicate table trans.

  // participants info, used for determine the sequence of trans at sequencer moudle.
  transaction::ObLSLogInfoArray participants_;
  // App Trace ID (get from commit_info log)
  ObString                trace_id_;
  // App Trace Info (get from commit_info log)
  ObString                trace_info_;

  // Trans fetche log info
  SortedLogEntryInfo      sorted_log_entry_info_; // sorted log_entry list
  SortedRedoLogList       sorted_redo_list_;      // ordered redo list
  // PartTrans fetch_log state
  // part_tx_fetch_state_ & 0x01:      read first record log
  // part_tx_fetch_state_ & 0x01 << 1: read commit info log
  // part_tx_fetch_state_ & 0x01 << 2: read prepare log
  // part_tx_fetch_state_ & 0x01 << 3: read commit log
  int8_t                  part_tx_fetch_state_;
  RollbackList            rollback_list_;         // Rollback list
  int64_t                 ref_cnt_;               // ref count(redo_node count + 1)

  // For MultiDataSource
  MultiDataSourceNodeArray  multi_data_source_node_arr_;    // array record MultiDataSourceNode
  MultiDataSourceInfo       multi_data_source_info_;        // MultiDataSourceInfo
  transaction::ObTxBigSegmentBuf segment_buf_;              // ObTxBigSegmentBuf for Big Tx Log

  // checkpoint seq number
  //
  // The Fetcher assigns a seq number to all tasks that are sent down and periodically calculates the seq information to be sent down via heartbeat tasks
  // Committer sorts tasks arriving out of order based on the seq number and maintains the overall data seq by processing the tasks sequentially
  int64_t                 checkpoint_seq_;

  // Trans info generate at sequencer
  // Transaction serial number assigned by sequencer globally
  // Distributed transaction level, tls_ided transactions within a distributed transaction have the same number
  int64_t                 global_trans_seq_;
  int64_t                 global_schema_version_;

  // PartTransTask linked list structure
  // list of participants in Sequencer
  // Fetcher for linking all tls_id transactions
  PartTransTask           *next_task_;

  // Data parsed from Redo
  int64_t                 local_schema_version_;  // Schema versions for tls_ided transactions
  StmtList                stmt_list_;             // statement list
  StmtList                ddl_lob_aux_stmt_list_; // DDL Trans Lob Aux meta stmt list

  common::ObByteLock      data_ready_lock_;
  // For sys LS: whether the formatting is complete
  // For user LS: whether the all logs which need store have stored
  // Note: DML tls_id: empty redo scene, is_data_ready_ = true
  bool                    is_data_ready_;
  common::ObCond          wait_data_ready_cond_;

  // To optimise memory usage, the condition variable is passed in externally
  common::ObCond          *wait_formatted_cond_;

  int64_t                 output_br_count_by_turn_; // sorted br count in each statistic round

  ObArray<TICUpdateInfo>  tic_update_infos_; // table id cache update info

  // allocator used to alloc:
  // LogEntryNode/RollbackNode
  // DdlRedoLogNode/DmlRedoLogNode/mutator_row_data
  // trace_id/trace_info/part_trans_info_str_/participant_
  // MutatorRow(DDL)/DdlStmtTask
  ObSmallArena            allocator_;
  ObLfFIFOAllocator       log_entry_task_base_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(PartTransTask);
};

}
}
#endif
