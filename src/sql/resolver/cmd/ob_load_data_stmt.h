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

#ifndef OB_LOAD_DATA_STMT_H_
#define OB_LOAD_DATA_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "share/backup/ob_backup_struct.h"
namespace oceanbase
{
namespace sql
{

enum class ObLoadDupActionType {
  LOAD_STOP_ON_DUP = 0, //stop when going to insert duplicated key
  LOAD_REPLACE, //replace into table when the rowkey is already existed
  LOAD_IGNORE, //skip this line, when the rowkey is already existed
  LOAD_INVALID_MODE
};

enum class ObLoadFileLocation {
  SERVER_DISK = 0,
  CLIENT_DISK,
  OSS,
};

class ObLoadFileIterator
{
public:
  ObLoadFileIterator() : pos_(0) {}
  void reset();
  bool is_valid() const { return !files_.empty(); }
  int64_t count() const { return files_.count(); }
  int add_files(common::ObString *start, const int64_t count = 1);
  int get_next_file(common::ObString &file);
  int copy(const ObLoadFileIterator &other);
  TO_STRING_KV(K_(files), K_(pos));
private:
  common::ObSEArray<common::ObString, 16> files_;
  int64_t pos_;
};

struct ObLoadArgument
{
  ObLoadArgument(): load_file_storage_(ObLoadFileLocation::SERVER_DISK),
                    is_default_charset_(true),
                    ignore_rows_(0),
                    dupl_action_(ObLoadDupActionType::LOAD_STOP_ON_DUP),
                    file_cs_type_(common::CS_TYPE_UTF8MB4_BIN),
                    tenant_id_(OB_INVALID_INDEX_INT64),
                    database_id_(OB_INVALID_INDEX_INT64),
                    table_id_(OB_INVALID_INDEX_INT64),
                    is_csv_format_(false),
                    part_level_(share::schema::PARTITION_LEVEL_MAX)

  {}

  TO_STRING_KV(K_(load_file_storage),
               K_(is_default_charset),
               K_(ignore_rows),
               K_(dupl_action),
               K_(file_cs_type),
               K_(file_name),
               K_(access_info),
               K_(database_name),
               K_(table_name),
               K_(combined_name),
               K_(tenant_id),
               K_(database_id),
               K_(table_id),
               K_(is_csv_format),
               K_(file_iter));

  void assign(const ObLoadArgument &other) {
    load_file_storage_ = other.load_file_storage_;
    is_default_charset_ = other.is_default_charset_;
    ignore_rows_ = other.ignore_rows_;
    dupl_action_ = other.dupl_action_;
    file_cs_type_ = other.file_cs_type_;
    file_name_ = other.file_name_;
    access_info_ = other.access_info_;
    database_name_ = other.database_name_;
    table_name_ = other.table_name_;
    combined_name_ = other.combined_name_;
    tenant_id_ = other.tenant_id_;
    database_id_ = other.database_id_;
    table_id_ = other.table_id_;
    is_csv_format_ = other.is_csv_format_;
    part_level_ = other.part_level_;
    file_iter_.copy(other.file_iter_);
  }

  ObLoadFileLocation load_file_storage_;
  bool is_default_charset_;
  int64_t ignore_rows_;
  ObLoadDupActionType dupl_action_;
  common::ObCollationType file_cs_type_;
  common::ObString file_name_;
  share::ObBackupStorageInfo access_info_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString combined_name_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_; // physical table id
  bool is_csv_format_;
  share::schema::ObPartitionLevel part_level_;
  ObLoadFileIterator file_iter_;
};

struct ObDataInFileStruct
{
  static const char* DEFAULT_LINE_TERM_STR;
  static const char* DEFAULT_LINE_BEGIN_STR;
  static const char* DEFAULT_FIELD_TERM_STR;
  static const char* DEFAULT_FIELD_ESCAPED_STR;
  static const char* DEFAULT_FIELD_ENCLOSED_STR;
  static const int64_t DEFAULT_FIELD_ESCAPED_CHAR;
  static const int64_t DEFAULT_FIELD_ENCLOSED_CHAR;
  static const bool DEFAULT_OPTIONAL_ENCLOSED;

  ObDataInFileStruct()
      : line_term_str_(DEFAULT_LINE_TERM_STR),
        line_start_str_(DEFAULT_LINE_BEGIN_STR),
        field_term_str_(DEFAULT_FIELD_TERM_STR),
        field_escaped_str_(DEFAULT_FIELD_ESCAPED_STR),
        field_enclosed_str_(DEFAULT_FIELD_ENCLOSED_STR),
        field_escaped_char_(DEFAULT_FIELD_ESCAPED_CHAR),
        field_enclosed_char_(DEFAULT_FIELD_ENCLOSED_CHAR),
        is_opt_field_enclosed_(DEFAULT_OPTIONAL_ENCLOSED)
  {
  }
  void assign(const ObDataInFileStruct &other) {
    line_term_str_ = other.line_term_str_;
    line_start_str_ = other.line_start_str_;
    field_term_str_ = other.field_term_str_;
    field_escaped_str_ = other.field_escaped_str_;
    field_enclosed_str_ = other.field_enclosed_str_;
    field_escaped_char_ = other.field_escaped_char_;
    field_enclosed_char_ = other.field_enclosed_char_;
    is_opt_field_enclosed_ = other.is_opt_field_enclosed_;
  }
  TO_STRING_KV(K_(line_term_str),
               K_(line_start_str),
               K_(field_term_str),
               K_(field_escaped_str),
               K_(field_enclosed_str),
               K_(field_escaped_char),
               K_(field_enclosed_char),
               K_(is_opt_field_enclosed));

  common::ObString line_term_str_;   // line teminated str
  common::ObString line_start_str_;  // line string by
  common::ObString field_term_str_;  // field terminated str
  common::ObString field_escaped_str_;    // field escaped str, such as \, ^, just copy from parsernode data
  common::ObString field_enclosed_str_;   // field enclosed str, such as ", ', just copy from parsernode data
  int64_t field_escaped_char_;    // valid escaped char after stmt validation
  int64_t field_enclosed_char_;   // valid enclosed char after stmt validation
  bool is_opt_field_enclosed_;    // true means no need use enclosed char for int
};

class ObLoadDataHint
{
public:
  ObLoadDataHint()
  {
    reset();
  }
  enum IntHintItem {
    PARALLEL_THREADS = 0,  //parallel threads on the host server, for parsing and calc partition
    BATCH_SIZE,
    QUERY_TIMEOUT,
    APPEND,
    ENABLE_DIRECT,
    NEED_SORT,
    ERROR_ROWS,
    GATHER_OPTIMIZER_STATISTICS,
    NO_GATHER_OPTIMIZER_STATISTICS,
    TOTAL_INT_ITEM
  };
  enum StringHintItem {
    LOG_LEVEL,
    TOTAL_STRING_ITEM
  };
  void reset()
  {
    memset(integer_values_, 0, sizeof(integer_values_));
    for (int64_t i = 0; i < TOTAL_STRING_ITEM; ++i) {
      string_values_[i].reset();
    }
  }
  int set_value(IntHintItem item, int64_t value);
  int get_value(IntHintItem item, int64_t &value) const;
  int set_value(StringHintItem item, const ObString &value);
  int get_value(StringHintItem item, ObString &value) const;
  TO_STRING_KV("Int Hint Item",
               common::ObArrayWrap<int64_t>(integer_values_, TOTAL_INT_ITEM),
               "String Hint Item",
               common::ObArrayWrap<ObString>(string_values_, TOTAL_STRING_ITEM));
private:
  int64_t integer_values_[TOTAL_INT_ITEM];
  ObString string_values_[TOTAL_STRING_ITEM];

};

class ObLoadDataStmt : public ObCMDStmt
{
public:
  static const int MAX_DELIMIT_STR_LEN = 50;
  struct FieldOrVarStruct
  {
    FieldOrVarStruct() : field_or_var_name_(),
                         column_id_(OB_INVALID_ID),
                         column_type_(common::ObMaxType),
                         is_table_column_(true) { }
    TO_STRING_KV(K_(field_or_var_name), K_(column_id), K_(column_type), K_(is_table_column));
    ObString field_or_var_name_;
    uint64_t column_id_;
    common::ColumnType column_type_;
    bool is_table_column_;  //false: is a user variable
  };

  ObLoadDataStmt() :
    ObCMDStmt(stmt::T_LOAD_DATA), is_default_table_columns_(false)
  {
  }
  virtual ~ObLoadDataStmt()
  {
  }
  //let previous transaction commit first
  virtual bool cause_implicit_commit() const { return true; }

  int add_assignment(ObAssignment& assign) { return assignments_.push_back(assign); }
  const ObAssignments& get_table_assignment() const { return assignments_; }
  ObLoadArgument &get_load_arguments() { return load_args_; }
  const ObLoadArgument &get_load_arguments() const { return load_args_; }
  ObDataInFileStruct &get_data_struct_in_file() { return data_struct_in_file_; }
  const ObDataInFileStruct &get_data_struct_in_file() const { return data_struct_in_file_; }
  common::ObIArray<FieldOrVarStruct> &get_field_or_var_list() { return field_or_var_list_; }
  const common::ObIArray<FieldOrVarStruct> &get_field_or_var_list() const { return field_or_var_list_; }
  int add_column_item(ColumnItem& item);
  ColumnItem *get_column_item_by_idx(uint64_t column_id);
  ObLoadDataHint &get_hints() { return hints_; }
  void set_default_table_columns() { is_default_table_columns_ = true; }
  bool get_default_table_columns() { return is_default_table_columns_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K_(load_args),
               K_(data_struct_in_file),
               K_(field_or_var_list),
               K_(assignments),
               K_(hints),
               K_(is_default_table_columns));

private:
  ObLoadArgument load_args_;
  ObDataInFileStruct data_struct_in_file_;
  common::ObSEArray<FieldOrVarStruct, 4> field_or_var_list_;
  common::ObSEArray<ColumnItem, 16> column_items_;
  ObAssignments assignments_;
  ObLoadDataHint hints_;
  bool is_default_table_columns_;

  DISALLOW_COPY_AND_ASSIGN(ObLoadDataStmt);
};


} // sql
} // oceanbase
#endif
