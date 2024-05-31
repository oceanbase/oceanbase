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

#ifndef OCEANBASE_SRC_PL_OB_PL_STMT_H_
#define OCEANBASE_SRC_PL_OB_PL_STMT_H_

#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObObjAccessIdent;
}
namespace pl {

static const int64_t FUNC_MAX_LABELS = 1024;
static const int64_t FUNC_MAX_CONDITIONS = 128;
static const int64_t FUNC_MAX_CURSORS = 128;
static const int64_t LABEL_MAX_SIZE = 128;
// static const int64_t PACKAGE_MAX_ROUTINES = 64;
static const int64_t PL_CONSTRUCT_COLLECTION = INT_MAX64;
static const int64_t OB_MAX_PL_IDENT_LENGTH = 128; // latest oracle ident max length is 128, before is 30
static const int64_t OB_MAX_MYSQL_PL_IDENT_LENGTH = 64;

static const ObString PL_IMPLICIT_SAVEPOINT = "PL/SQL@IMPLICIT_SAVEPOINT";

OB_INLINE uint64_t get_tenant_id_by_object_id(uint64_t object_id)
{
  object_id = object_id & ~(OB_MOCK_TRIGGER_PACKAGE_ID_MASK);
  object_id = object_id & ~(OB_MOCK_OBJECT_PACAKGE_ID_MASK);
  object_id = object_id & ~(OB_MOCK_PACKAGE_BODY_ID_MASK);
  object_id = object_id & ~(OB_MOCK_DBLINK_UDT_ID_MASK);
  return is_inner_pl_object_id(object_id) ? OB_SYS_TENANT_ID : MTL_ID();
}

enum ObBlockNSScope {
  PL_NS_PACKAGE_SPEC,
  PL_NS_PACKAGE_BODY,
  PL_NS_ROUTINE
};

enum AccessorItemKind
{
  PL_ACCESSOR_ALL = -1,
  PL_ACCESSOR_FUNCTION = 0,
  PL_ACCESSOR_PROCEDURE,
  PL_ACCESSOR_PACKAGE,
  PL_ACCESSOR_TRIGGER,
  PL_ACCESSOR_TYPE,
  PL_ACCESSOR_INVALID,
};

struct AccessorItem
{
  AccessorItem()
    : kind_(AccessorItemKind::PL_ACCESSOR_INVALID), schema_(), name_() {}
  AccessorItem(AccessorItemKind kind, ObString &schema, ObString &name)
    : kind_(kind), schema_(schema), name_(name) {}

  inline bool operator==(const AccessorItem &other) const
  {
    return kind_ == other.kind_
           && schema_ == other.schema_
           && name_ == other.name_;
  }

  AccessorItemKind kind_;
  ObString schema_;
  ObString name_;

  TO_STRING_KV(K_(kind), K_(schema), K_(name));
};

// struct SourceLocation
// {
// public:
//   SourceLocation() : line_(0), col_(0) {}
//   virtual ~SourceLocation() {}
//   int64_t line_;
//   int64_t col_;
//   TO_STRING_KV(K_(line), K_(col));
// };

union SourceLocation
{
  int64_t loc_;
  struct {
    int32_t col_;
    int32_t line_;
  };
};

typedef common::ObSEArray<share::schema::ObSchemaObjVersion, 4> ObPLDependencyTable;

template<typename T>
class ObPLSEArray : public common::ObSEArrayImpl<T, 16, common::ObIAllocator&>
{
public:
  ObPLSEArray(common::ObIAllocator &allocator)
   : common::ObSEArrayImpl<T, 16, common::ObIAllocator&>(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator) {}
private:
};

class ObPLVar : public share::schema:: ObIRoutineParam
{
public:
  ObPLVar()
    : name_(),
      type_(),
      default_(-1),
      is_readonly_(false),
      is_not_null_(false),
      is_default_construct_(false),
      is_formal_param_(false),
      is_referenced_(false) {}
  virtual ~ObPLVar() {}

  inline const common::ObString &get_name() const { return name_; }
  inline void set_name(const common::ObString &name) { name_ = name; }
  inline const ObPLDataType &get_type() const { return type_; }
  inline ObPLDataType &get_type() { return type_; }
  inline ObPLDataType get_pl_data_type() const { return type_; }
  inline void set_type(const ObPLDataType &type) { type_ = type; }
  inline int64_t get_default() const { return default_; }
  //for ObIRoutineParam, should never be called
  const ObString &get_default_value() const { return name_; }
  inline void set_default(int64_t default_idx) { default_ = default_idx; }
  bool is_default() const { return default_ != -1; }
  inline bool is_readonly() const { return is_readonly_; }
  inline void set_readonly(bool readonly) { is_readonly_ = readonly; }
  inline bool is_not_null() const { return is_not_null_; }
  inline void set_not_null(bool not_null) { is_not_null_ = not_null; }
  inline bool is_default_construct() const { return is_default_construct_; }
  inline void set_default_construct(bool is_default_construct) { is_default_construct_ = is_default_construct; }
  inline void set_is_formal_param(bool flag) { is_formal_param_ = flag; }
  inline bool is_formal_param() const { return is_formal_param_; }
  int deep_copy(const ObPLVar &var, common::ObIAllocator &allocator);
  inline void set_dup_declare(bool dup_declare) { is_dup_declare_ = dup_declare; }
  inline bool is_dup_declare() const { return is_dup_declare_; }
  inline void set_is_referenced(bool is_referenced) { is_referenced_ = is_referenced; }
  inline bool is_referenced() const { return is_referenced_; }

  TO_STRING_KV(K_(name),
               K_(type),
               K_(default),
               K_(is_readonly),
               K_(is_not_null),
               K_(is_default_construct),
               K_(is_formal_param));
private:
  common::ObString name_;
  ObPLDataType type_; //主要用来表示类型，同时要存储变量的初始值或default值，运行状态的值不存储在这里
  int64_t default_; //-1:代表没有default值, 其他:default值在表达式表中的下标
  // const sql::ObRawExpr *default_; //是否有default值
  bool is_readonly_; //该变量是否只读
  bool is_not_null_; //该变量不允许为NULL
  bool is_default_construct_; //默认值是否是该变量的构造函数
  bool is_formal_param_; // this is formal param of a routine
  bool is_dup_declare_;
  bool is_referenced_;
};

class ObPLSymbolTable
{
public:
  ObPLSymbolTable(ObIAllocator &allocator) : variables_(allocator),
                                             self_param_idx_(OB_INVALID_INDEX) {}
  virtual ~ObPLSymbolTable() {}

  inline int64_t get_count() const  { return variables_.count(); }
  inline const ObPLVar *get_symbol(int64_t idx) const { return idx < 0 || idx >= variables_.count() ? NULL : &variables_[idx]; }
  int add_symbol(const common::ObString &name,
                 const ObPLDataType &type,
                 const int64_t default_idx = -1,
                 const bool read_only = false,
                 const bool not_null = false,
                 const bool default_construct_ = false,
                 const bool is_formal_param = false,
                 const bool is_dup_declare = false);
  int delete_symbol(int64_t symbol_idx);

  inline void set_self_param_idx() { self_param_idx_ = variables_.count() - 1; }
  inline int64_t get_self_param_idx() const { return self_param_idx_; }
  inline const ObPLVar *get_self_param() const
  {
    const ObPLVar *val = NULL;
    if (OB_INVALID_INDEX == self_param_idx_
      || self_param_idx_ < 0
      || self_param_idx_ >= variables_.count()) {
      val = NULL;
    } else {
      val = &variables_[self_param_idx_];
    }
    return val;
  }

  TO_STRING_KV(K_(variables), K_(self_param_idx));

private:
  //所有输入输出参数，和PL体内使用的所有变量（包括游标，但是不包括condition，也不包括函数返回值和隐藏的ctx参数）,和ObPLFunction里的符号表一一对应
  ObPLSEArray<ObPLVar> variables_;
  int64_t self_param_idx_; // index of self_param
};

class ObPLStmt;
class ObPLSymbolDebugInfoTable
{
public:
  ObPLSymbolDebugInfoTable(ObIAllocator &allocator) : variable_debuginfos_(allocator) {}

  inline int reserve(int capcity) { return variable_debuginfos_.reserve(capcity); }
  inline int64_t get_count() const { return variable_debuginfos_.count(); }
  inline const ObPLVarDebugInfo* get_symbol(int64_t idx) const
  {
    return idx < 0 || idx >= variable_debuginfos_.count() ? NULL : &variable_debuginfos_.at(idx);
  }

  int add(ObPLVarDebugInfo info)
  {
    return variable_debuginfos_.push_back(info);
  }

  int add(int idx, const ObString &name, ObPLType type, int start, int end)
  {
    int ret = OB_SUCCESS;
    if (idx < 0 && idx > variable_debuginfos_.count()) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      variable_debuginfos_.at(idx) =
        ObPLVarDebugInfo(name, type, ObPLVarDebugInfo::ObPLVarScope(start, end));
    }
    return ret;
  }

  ObIArray<ObPLVarDebugInfo>& get_var_debuginfos() { return variable_debuginfos_; }

  TO_STRING_KV(K_(variable_debuginfos));

private:
  // 所有输入输出参数, 和PL体内使用的所有变量 (包括游标), 和ObPLFunction中的符号表一一对应
  // 主要用于记录每个符号的作用范围, 以及该符号如果是复杂类型的话是Collection还是Record
  ObPLSEArray<ObPLVarDebugInfo> variable_debuginfos_;
};

class ObPLLabelTable
{
public:
  enum ObPLLabelType {LABEL_INVALID, LABEL_BLOCK, LABEL_CONTROL};
public:
  ObPLLabelTable() : count_(0) {}
  virtual ~ObPLLabelTable() {}

  inline int64_t get_count() const  { return count_; }
  inline const common::ObString *get_label(int64_t idx) const { return idx < 0 || idx >= count_ ? NULL : &(labels_[idx].label_); }
  inline ObPLLabelType get_label_type(int64_t idx) const { return idx < 0 || idx >= count_ ? LABEL_INVALID : labels_[idx].type_; }
  inline ObPLStmt *get_next_stmt(int64_t idx) const
  {
    return idx < 0 || idx >= count_ ? NULL : labels_[idx].next_stmt_;
  }
  inline void set_next_stmt(int64_t idx, ObPLStmt *stmt) {
    if (idx >= 0 && idx < count_) {
      labels_[idx].next_stmt_ = stmt;
    }
  }
  inline void set_end_flag(int64_t idx, bool end_flag) {
    if (idx >= 0 && idx < count_) {
      labels_[idx].is_end_ = end_flag;
    }
  }
  inline bool is_ended(int64_t idx) const {
    return (idx >= 0 && idx < count_) ? labels_[idx].is_end_ : false;
  }
  inline bool get_is_goto_dst(int64_t idx) const {
    return (idx >= 0 && idx < count_) ? labels_[idx].is_goto_dst_ : false;
  }
  inline void set_is_goto_dst(int64_t idx, bool flag) {
    if (idx >= 0 && idx < count_) {
      labels_[idx].is_goto_dst_ = flag;
    }
  }
  int add_label(const common::ObString &name, const ObPLLabelType &type, ObPLStmt *stmt);

  TO_STRING_KV(K_(count), K_(labels));

private:
  int64_t count_;
  //所有声明的标签（block的标签和普通语句一样，放在父block里）,和ObPLFunction里的标签表一一对应
  typedef struct ObPLLabel
  {
    ObPLLabel() : label_(), type_(LABEL_INVALID), next_stmt_(NULL), is_goto_dst_(false), is_end_(false) {}
    TO_STRING_KV(K_(label), K_(type), K_(next_stmt), K_(is_goto_dst), K_(is_end));
    common::ObString label_;
    ObPLLabelType type_;
    // 和label关联的stmt
    ObPLStmt *next_stmt_;
    bool is_goto_dst_;
    bool is_end_;
  } ObPLLabel;
  ObPLLabel labels_[FUNC_MAX_LABELS];
};

class ObPLUserTypeTable
{
public:
#ifdef OB_BUILD_ORACLE_PL
  ObPLUserTypeTable() : type_start_gen_id_(0), sys_refcursor_type_(), user_types_(), external_user_types_()
  {
    sys_refcursor_type_.set_name("SYS_REFCURSOR");
    sys_refcursor_type_.set_user_type_id(generate_user_type_id(OB_INVALID_ID));
    sys_refcursor_type_.set_type_from(PL_TYPE_SYS_REFCURSOR);
  }
#else
  ObPLUserTypeTable() : type_start_gen_id_(0), user_types_(), external_user_types_() {}
#endif
  virtual ~ObPLUserTypeTable() {}

  inline void set_type_start_gen_id(uint64_t type_start_gen_id) { type_start_gen_id_ = type_start_gen_id; }
  inline uint64_t get_type_start_gen_id() const { return type_start_gen_id_; }
  inline int64_t get_count() const { return user_types_.count(); }
#ifdef OB_BUILD_ORACLE_PL
  inline const ObRefCursorType &get_sys_refcursor_type() const { return sys_refcursor_type_; }
#endif
  const common::ObIArray<const ObUserDefinedType *> &get_types() const { return user_types_; }
  int add_type(ObUserDefinedType *user_defined_type);
  const ObUserDefinedType *get_type(const common::ObString &type_name) const;
  const ObUserDefinedType *get_type(uint64_t type_id) const;
  const ObUserDefinedType *get_type(int64_t idx) const;
  const ObUserDefinedType *get_external_type(const common::ObString &type_name) const;
  const ObUserDefinedType *get_external_type(uint64_t type_id) const;
  const common::ObIArray<const ObUserDefinedType *> &get_external_types() const { return external_user_types_; }
  int add_external_types(common::ObIArray<const ObUserDefinedType *> &user_types);
  int add_external_type(const ObUserDefinedType *user_type);
  inline uint64_t generate_user_type_id(uint64_t package_id) { return common::combine_pl_type_id(package_id, type_start_gen_id_++); }
private:
  uint64_t type_start_gen_id_;
#ifdef OB_BUILD_ORACLE_PL
  ObRefCursorType sys_refcursor_type_;
#endif
  common::ObSEArray<const ObUserDefinedType *, 4> user_types_;
  common::ObSEArray<const ObUserDefinedType *, 4> external_user_types_;
};


enum ObPLConditionType //按照优先级顺序从高到低
{
  INVALID_TYPE = -1,
  ERROR_CODE = 0,
  SQL_STATE,
  SQL_EXCEPTION,
  SQL_WARNING,
  NOT_FOUND,
  OTHERS,
  MAX_TYPE,
};

#define IDX_CONDITION_TYPE 0
#define IDX_CONDITION_CODE 1
#define IDX_CONDITION_STATE 2
#define IDX_CONDITION_LEN 3
#define IDX_CONDITION_STMT 4
#define IDX_CONDITION_SIGNAL 5

struct ObPLConditionValue
{
public:
  ObPLConditionValue()
    : type_(INVALID_TYPE),
      error_code_(0),
      sql_state_(NULL),
      str_len_(0),
      stmt_id_(OB_INVALID_INDEX),
      signal_(false),
      duplicate_(false) {}
  ObPLConditionValue(ObPLConditionType type, int64_t error_code)
    : type_(type),
      error_code_(error_code),
      sql_state_(ob_sqlstate(static_cast<int>(error_code))),
      str_len_(STRLEN(sql_state_)),
      stmt_id_(OB_INVALID_INDEX),
      signal_(false),
      duplicate_(false) {}

  //不实现析构函数，省得LLVM映射麻烦

  static uint32_t type_offset_bits() { return offsetof(ObPLConditionValue, type_) * 8; }
  static uint32_t error_code_offset_bits() { return offsetof(ObPLConditionValue, error_code_) * 8; }
  static uint32_t sql_state_offset_bits() { return offsetof(ObPLConditionValue, sql_state_) * 8; }
  static uint32_t str_len_offset_bits() { return offsetof(ObPLConditionValue, str_len_) * 8; }
  static uint32_t stmt_id_offset_bits() { return offsetof(ObPLConditionValue, stmt_id_) * 8; }
  static uint32_t signal_offset_bits() { return offsetof(ObPLConditionValue, signal_) * 8; }

  ObPLConditionType type_;
  int64_t error_code_;
  const char *sql_state_;
  int64_t str_len_;
  int64_t stmt_id_; //FOR DEBUG，主流程不需要：作为landingpad的clause时，代表level；当作为抛出的异常时，代表stmt id
  bool signal_; //FOR DEBUG，主流程不需要：作为landingpad的clause时，无意义；当作为抛出的异常时，代表是否由signal语句抛出
  bool duplicate_; // 代表是否重复声明

  TO_STRING_KV(
    K_(type), K_(error_code), K_(sql_state), K_(str_len), K_(stmt_id), K_(signal), K_(duplicate));
};

class ObPLCondition //用于Declare Condition定义
{
public:
  ObPLCondition() : name_(), value_() {}
  virtual ~ObPLCondition() {}

  inline const common::ObString &get_name() const { return name_; }
  inline void set_name(const common::ObString &name) { name_ = name; }
  inline const ObPLConditionValue &get_value() const { return value_; }
  inline void set_value(const ObPLConditionValue &value) { value_ = value; }
  inline ObPLConditionType get_type() const { return value_.type_; }
  inline void set_type(ObPLConditionType type) { value_.type_ = type; }
  inline int64_t get_error_code() const { return value_.error_code_; }
  inline void set_error_code(int64_t error_code) { value_.error_code_ = error_code; }
  inline const char *get_sql_state() const { return value_.sql_state_; }
  inline void set_sql_state(const char *sql_state) { value_.sql_state_ = sql_state; }
  inline bool get_duplicate() const { return value_.duplicate_; }
  inline void set_duplicate() { value_.duplicate_ = true; }

  int deep_copy(const ObPLCondition &condition, ObIAllocator &allocator);

  common::ObString name_;
  ObPLConditionValue value_;

  TO_STRING_KV(K_(name), K_(value));
};

class ObPLConditionTable
{
public:
  ObPLConditionTable() : count_(0) {}
  virtual ~ObPLConditionTable() {}

  inline int64_t get_count() const  { return count_; }
  inline const ObPLCondition *get_conditions() const  { return conditions_; }
  inline const ObPLCondition *get_condition(int64_t idx) const { return idx < 0 || idx >= count_ ? NULL : &conditions_[idx]; }

  int init(ObPLConditionTable &parent_condition_table);
  int add_condition(const common::ObString &name, const ObPLConditionValue &value);

  TO_STRING_KV(K_(count), K_(conditions));

private:
  int64_t count_;
  ObPLCondition conditions_[FUNC_MAX_CONDITIONS];
};

class ObPLSql
{
public:
  ObPLSql(common::ObIAllocator &allocator) :
    forall_sql_(false),
    for_update_(false),
    has_hidden_rowid_(false),
    sql_(),
    params_(allocator),
    array_binding_params_(allocator),
    stmt_type_(sql::stmt::T_NONE),
    ref_objects_(allocator),
    row_desc_(NULL),
    rowid_table_id_(OB_INVALID_ID),
    ps_sql_(),
    is_link_table_(false),
    is_skip_locked_(false) {}
  virtual ~ObPLSql() {}

  inline const common::ObString &get_sql() const { return sql_; }
  inline void set_sql(const common::ObString &str) { sql_ = str; }
  inline const common::ObIArray<int64_t> &get_params() const { return params_; }
  inline int64_t get_param(int64_t i) const { return params_.at(i); }
  inline int set_params(const common::ObIArray<int64_t> &params) { return append(params_, params); }
  inline int add_param(int64_t expr) { return params_.push_back(expr); }
  inline void set_forall_sql(bool forall_sql) { forall_sql_ = forall_sql; }
  inline bool is_forall_sql() const { return forall_sql_; }
  inline void set_for_update(bool for_update) { for_update_ = for_update; }
  inline bool is_for_update() const { return for_update_; }
  inline void set_hidden_rowid(bool has_hidden_rowid) { has_hidden_rowid_ = has_hidden_rowid; }
  inline bool has_hidden_rowid() const { return has_hidden_rowid_; }
  inline const common::ObIArray<int64_t> &get_array_binding_params() const { return array_binding_params_; }
  inline common::ObIArray<int64_t> &get_array_binding_params() { return array_binding_params_; }
  inline const common::ObString &get_ps_sql() const { return ps_sql_; }
  inline sql::stmt::StmtType get_stmt_type() const { return stmt_type_; }
  inline void set_ps_sql(const common::ObString &sql, const sql::stmt::StmtType type) { ps_sql_ = sql; stmt_type_ = type; }
  inline const common::ObIArray<share::schema::ObSchemaObjVersion> &get_ref_objects() const { return ref_objects_; }
  inline int set_ref_objects(const common::ObIArray<share::schema::ObSchemaObjVersion> &objects) { return append(ref_objects_, objects); }
  inline int add_ref_object(const share::schema::ObSchemaObjVersion &object) { return ref_objects_.push_back(object); }
  inline void set_row_desc(const ObRecordType *row_desc) { row_desc_ = row_desc; }
  inline const ObRecordType *get_row_desc() const { return row_desc_; }
  inline uint64_t get_rowid_table_id() const { return rowid_table_id_; }
  inline void set_rowid_table_id(uint64 table_id) { rowid_table_id_ = table_id; }

  inline void set_link_table(bool is_link_table) { is_link_table_ = is_link_table; }
  inline bool has_link_table() const { return is_link_table_; }

  inline void set_skip_locked(bool is_skip_locked) { is_skip_locked_ = is_skip_locked; }
  inline bool is_skip_locked() const { return is_skip_locked_; }

  TO_STRING_KV(K_(sql), K_(params), K_(ps_sql), K_(stmt_type), K_(ref_objects), K_(rowid_table_id), K_(is_skip_locked));

protected:
  bool forall_sql_;
  bool for_update_;
  bool has_hidden_rowid_;
  common::ObString sql_;
  ObPLSEArray<int64_t> params_;
  ObPLSEArray<int64_t> array_binding_params_;
  sql::stmt::StmtType stmt_type_;
  ObPLSEArray<share::schema::ObSchemaObjVersion> ref_objects_;
  const ObRecordType *row_desc_;
  uint64_t rowid_table_id_;
  common::ObString ps_sql_;
  bool is_link_table_;
  bool is_skip_locked_;
};

class ObPLCursor
{
public:

  enum CursorState {
    INVALID = -1,
    DEFINED = 0,
    DECLARED,
    DUP_DECL,
    PASSED_IN,
  };

  ObPLCursor(common::ObIAllocator &allocator)
    : pkg_id_(OB_INVALID_ID),
      routine_id_(OB_INVALID_ID),
      idx_(OB_INVALID_INDEX),
      value_(allocator),
      cursor_type_(),
      formal_params_(allocator),
      state_(INVALID),
      has_dup_column_name_(false) {}
  virtual ~ObPLCursor() {}

  inline bool is_package_cursor() const
  {
    return pkg_id_ != OB_INVALID_ID && OB_INVALID_ID == routine_id_;
  }
  inline void set_package_id(uint64_t package_id) { pkg_id_ = package_id; }
  inline uint64_t get_package_id() const { return pkg_id_; }
  inline bool is_routine_cursor() const { return routine_id_ != OB_INVALID_ID; }
  inline void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline int64_t get_index() const { return idx_; }
  inline void set_index(int64_t idx) { idx_ = idx; }
  inline const ObPLSql &get_value() const { return value_; }
  inline void set_value(const ObPLSql &value) { value_ = value; }
  inline const common::ObString &get_sql() const { return value_.get_sql(); }
  inline void set_sql(const common::ObString &str) { value_.set_sql(str); }
  inline const common::ObIArray<int64_t> &get_sql_params() const { return value_.get_params(); }
  inline int64_t get_sql_param(int64_t i) const { return value_.get_param(i); }
  inline int set_sql_params(const common::ObIArray<int64_t> &params) { return value_.set_params(params); }
  inline int add_sql_param(int64_t expr) { return value_.add_param(expr); }
  inline const common::ObString &get_ps_sql() const { return value_.get_ps_sql(); }
  inline sql::stmt::StmtType get_stmt_type() const { return value_.get_stmt_type(); }
  inline void set_ps_sql(const common::ObString &sql, const sql::stmt::StmtType type) { value_.set_ps_sql(sql, type); }
  inline void set_for_update(bool for_update) { value_.set_for_update(for_update); }
  inline bool is_for_update() const { return value_.is_for_update(); }
  inline void set_hidden_rowid(bool has_hidden_rowid) { value_.set_hidden_rowid(has_hidden_rowid); }
  inline bool has_hidden_rowid() const { return value_.has_hidden_rowid(); }
  inline void set_skip_locked(bool is_skip_locked) { value_.set_skip_locked(is_skip_locked); }
  inline bool is_skip_locked() { return value_.is_skip_locked(); }
  inline const common::ObIArray<share::schema::ObSchemaObjVersion> &get_ref_objects() const { return value_.get_ref_objects(); }
  inline int set_ref_objects(const common::ObIArray<share::schema::ObSchemaObjVersion> &ref_objects) { return value_.set_ref_objects(ref_objects); }
  inline void set_row_desc(const ObRecordType* row_desc) { value_.set_row_desc(row_desc); }
  inline const ObRecordType* get_row_desc() const { return value_.get_row_desc(); }
  inline void set_cursor_type(const ObPLDataType& cursor_type) { cursor_type_ = cursor_type; }
  inline const ObPLDataType& get_cursor_type() const { return cursor_type_; }
  inline const common::ObIArray<int64_t> &get_formal_params() const { return formal_params_; }
  inline int64_t get_formal_param(int64_t i) const { return formal_params_.at(i); }
  inline int set_formal_params(const common::ObIArray<int64_t> &params) { return formal_params_.assign(params); }
  inline int add_formal_param(int64_t expr) { return formal_params_.push_back(expr); }
  inline CursorState get_state() const { return state_; }
  inline void set_state(CursorState state) { state_ = state; }
  inline uint64_t get_rowid_table_id() const { return value_.get_rowid_table_id(); }
  inline void set_rowid_table_id(uint64 table_id) { value_.set_rowid_table_id(table_id); }
  inline void set_dup_column() { has_dup_column_name_ = true; }
  inline bool is_dup_column() const { return has_dup_column_name_; }

  int set(const ObString &sql,
                 const ObIArray<int64_t> &expr_idxs,
                 const common::ObString &ps_sql,
                 sql::stmt::StmtType type,
                 bool for_update,
                 ObRecordType *record_type,
                 const ObPLDataType &cursor_type,
                 CursorState state,
                 const ObIArray<share::schema::ObSchemaObjVersion> &ref_objects,
                 const common::ObIArray<int64_t> &params,
                 bool has_dup_column_name
                 );

  TO_STRING_KV(
    K_(pkg_id), K_(routine_id), K_(idx), K_(value), K_(cursor_type), K_(formal_params), K_(state), K_(has_dup_column_name));

protected:
  uint64_t pkg_id_;
  uint64_t routine_id_;
  int64_t idx_; //指向ObPLSymbolTable的下标
  ObPLSql value_;
  ObPLDataType cursor_type_;
  ObPLSEArray<int64_t> formal_params_;
  CursorState state_;
  bool has_dup_column_name_;
};

class ObPLCursorTable
{
public:
  ObPLCursorTable(common::ObIAllocator &allocator)
    : cursors_(allocator),
      allocator_(allocator) {}
  virtual ~ObPLCursorTable() {}

  inline int64_t get_count() const  { return cursors_.count(); }
  inline const ObIArray<ObPLCursor*> &get_cursors() const { return cursors_; }
  inline const ObPLCursor *get_cursor(int64_t idx) const { return idx < 0 || idx >= cursors_.count() ? NULL : cursors_.at(idx); }
  inline ObPLCursor *get_cursor(int64_t idx) { return idx < 0 || idx >= cursors_.count() ? NULL : cursors_.at(idx); }
  const ObPLCursor *get_cursor(uint64_t pkg_id, uint64_t routine_id, int64_t idx) const;
  int add_cursor(uint64_t pkg_id,
                 uint64_t routine_id,
                 int64_t idx,
                 const common::ObString &sql,
                 const common::ObIArray<int64_t> &sql_params,
                 const common::ObString &ps_sql,
                 sql::stmt::StmtType stmt_type,
                 bool for_update,
                 bool has_hidden_rowid,
                 uint64_t rowid_table_id,
                 const common::ObIArray<share::schema::ObSchemaObjVersion> &ref_objects,
                 const ObRecordType* row_desc,
                 const ObPLDataType &cursor_type,
                 const common::ObIArray<int64_t> &formal_params,
                 ObPLCursor::CursorState state = ObPLCursor::DEFINED,
                 bool has_dup_column_name = false,
                 bool skip_locked = false);

  TO_STRING_KV(K_(cursors));

private:
  ObPLSEArray<ObPLCursor *> cursors_;
  common::ObIAllocator &allocator_;
};

enum ObPLRoutineParamMode
{
  PL_PARAM_INVALID = 0,
  PL_PARAM_IN = 1,
  PL_PARAM_OUT = 2,
  PL_PARAM_INOUT = 3
};

class ObPLRoutineParam : public share::schema::ObIRoutineParam
{
public:
  ObPLRoutineParam()
    : name_(),
      type_(),
      mode_(PL_PARAM_INVALID),
      is_nocopy_(false),
      default_value_(),
      default_cast_(false),
      extern_type_(SP_EXTERN_INVALID),
      type_owner_(OB_INVALID_ID),
      type_name_(),
      type_subname_(),
      obj_version_(),
      is_self_param_(false) {}
  virtual ~ObPLRoutineParam() {}

  void reset();
  inline const ObString &get_name() const { return name_; }
  inline const ObPLDataType &get_type() const { return type_; }
  inline ObPLDataType get_pl_data_type() const { return type_; }
  inline void set_type(const ObPLDataType &type) { type_ = type; }
  bool operator ==(const ObPLRoutineParam &other) const;
  inline bool is_in_param() const { return mode_ == PL_PARAM_IN; }
  inline bool is_out_param() const { return mode_ == PL_PARAM_OUT; }
  inline bool is_inout_param() const { return mode_ == PL_PARAM_INOUT; }
  inline void set_param_mode(ObPLRoutineParamMode mode) { mode_ = mode; }
  inline int64_t get_mode() const { return static_cast<int64_t>(mode_); }
  inline void set_is_nocopy(bool is_nocopy) { is_nocopy_ = is_nocopy; }
  inline bool get_is_nocopy() { return is_nocopy_; }
  inline void set_default_value(ObString &default_value) { default_value_ = default_value; }
  inline ObString &get_default_value() { return default_value_; }
  inline const ObString &get_default_value() const { return default_value_; }
  inline void set_default_cast(bool default_cast) { default_cast_ = default_cast; }
  inline bool is_complex_type() const { return type_.is_user_type(); }
  inline bool is_schema_routine_param() const { return false; }

  inline const ObString &get_type_name() const { return type_name_; }
  inline const ObString &get_type_subname() const { return type_subname_; }

  inline void set_extern_type(int64_t extern_type) { extern_type_ = extern_type; }
  inline int64_t get_extern_type() const { return extern_type_; }

  inline void set_type_owner(uint64_t type_owner) { type_owner_ = type_owner; }
  inline uint64_t get_type_owner() const { return type_owner_; }

  inline const share::schema::ObSchemaObjVersion& get_obj_version() const { return obj_version_; }
  inline void set_obj_version(const share::schema::ObSchemaObjVersion &obj_version)
  {
    obj_version_ = obj_version;
  }
  virtual bool is_nocopy_param() const { return is_nocopy_; }
  virtual bool is_default_cast() const { return default_cast_; }

  inline void set_is_self_param(bool sp) { is_self_param_ = sp; }
  inline bool get_is_self_param() const { return is_self_param_; }

  bool is_self_param() const { return get_is_self_param(); }

  TO_STRING_KV(K_(name), K_(type), K_(mode), K_(is_nocopy), K_(default_value),
               K_(extern_type), K_(type_owner), K_(type_name), K_(type_subname),
               K_(obj_version), K_(is_self_param));
private:
  common::ObString name_;
  ObPLDataType type_;
  ObPLRoutineParamMode mode_;
  bool is_nocopy_;
  common::ObString default_value_;
  bool default_cast_;
  int64_t extern_type_;
  uint64_t type_owner_;
  common::ObString type_name_;
  common::ObString type_subname_;
  share::schema::ObSchemaObjVersion obj_version_;
  bool is_self_param_;
};

struct ObPLCompileFlag
{
public:
  ObPLCompileFlag() : flags_() {}

  enum { TRUST, RNDS, WNDS, RNPS, WNPS, UDF, INVOKER_RIGHT,
         INTF, UDT_STATIC, UDT_FINAL, UDT_MAP, UDT_ORDER, UDT_CONS };

  inline int add_trust() { return flags_.add_member(TRUST); }
  inline int add_rnds()  { return flags_.add_member(RNDS); }
  inline int add_wnds()  { return flags_.add_member(WNDS); }
  inline int add_rnps()  { return flags_.add_member(RNPS); }
  inline int add_wnps()  { return flags_.add_member(WNPS); }
  inline int add_udf()   { return flags_.add_member(UDF); }
  inline int add_invoker_right() { return flags_.add_member(INVOKER_RIGHT); }
  inline int add_intf()   { return flags_.add_member(INTF); }

  inline int add_compile_flag(int flag) { return flags_.add_member(flag); }
  inline int del_compile_flag(int flag) { return flags_.del_member(flag); }

  inline int add_compile_flag(const ObPLCompileFlag &flag)
  {
    return flags_.add_members(flag.flags_);
  }

  inline bool has_flag() const { return flags_.num_members() != 0; }

  inline int add_static() { return flags_.add_member(UDT_STATIC); }
  inline int add_map() { return flags_.add_member(UDT_MAP); }
  inline int add_order() { return flags_.add_member(UDT_ORDER); }

  inline bool compile_with_trust() const
  {
    return flags_.has_member(TRUST);
  }
  inline bool compile_with_rnds() const
  {
    return flags_.has_member(RNDS) && !flags_.has_member(TRUST);
  }
  inline bool compile_with_wnds() const
  {
    return flags_.has_member(WNDS) && !flags_.has_member(TRUST);
  }
  inline bool compile_with_rnps() const
  {
    return flags_.has_member(RNPS) && !flags_.has_member(TRUST);
  }
  inline bool compile_with_wnps() const
  {
    return flags_.has_member(WNPS) && !flags_.has_member(TRUST);
  }
  inline bool compile_with_udf() const
  {
    return flags_.has_member(UDF);
  }
  inline bool compile_with_invoker_right() const
  {
    return flags_.has_member(INVOKER_RIGHT);
  }
  inline bool compile_with_intf() const
  {
    return flags_.has_member(INTF);
  }

  inline bool compile_with_static() const
  {
    return flags_.has_member(UDT_STATIC);
  }
  inline bool compile_with_order() const {
    return flags_.has_member(UDT_ORDER);
  }
  inline bool compile_with_map() const {
    return flags_.has_member(UDT_MAP);
  }

  inline int add_cons() { return flags_.add_member(UDT_CONS); }

  inline bool compile_with_cons() const
  {
    return flags_.has_member(UDT_CONS);
  }

  inline ObPLCompileFlag &operator=(const ObPLCompileFlag &other)
  {
    flags_ = other.flags_;
    return *this;
  }

  TO_STRING_KV(K_(flags));

private:
  common::ObFixedBitSet<16> flags_;
};

class ObPLRoutineInfo : public share::schema::ObIRoutineInfo
{
public:
  ObPLRoutineInfo(common::ObIAllocator &allocator)
      : allocator_(allocator),
        tenant_id_(OB_INVALID_ID),
        db_id_(OB_INVALID_ID),
        pkg_id_(OB_INVALID_ID),
        type_(INVALID_PROC_TYPE),
        parent_id_(OB_INVALID_ID),
        id_(OB_INVALID_ID),
        subprogram_path_(allocator),
        name_(),
        decl_str_(),
        route_sql_(),
        routine_body_(),
        md5_(0),
        ret_info_(NULL),
        params_(allocator),
        compile_flag_(),
        is_pipelined_(false),
        is_deterministic_(false),
        is_parallel_enable_(false),
        is_result_cache_(false),
        has_accessible_by_clause_(false),
        is_udt_routine_(false),
        is_private_routine_(false),
        accessors_(allocator),
        priv_user_(),
        loc_(0),
        analyze_flag_(0) {}
  virtual ~ObPLRoutineInfo();

  int make_routine_param(common::ObIAllocator &allocator,
                         const common::ObDataTypeCastParams &dtc_params,
                         const ObString &param_name,
                         const ObPLDataType &param_type,
                         ObPLRoutineParamMode param_mode,
                         bool is_nocopy,
                         const ObString &default_value,
                         bool default_cast,
                         const ObPLExternTypeInfo &extern_type_info,
                         ObPLRoutineParam *&param);
  bool is_equal(const ObPLRoutineInfo &other) const { return md5_ == other.md5_; }
  inline bool is_procedure() const { return type_ == STANDALONE_PROCEDURE
                                         || type_ == PACKAGE_PROCEDURE
                                         || UDT_PROCEDURE == type_; }
  inline bool is_function() const { return type_ == STANDALONE_FUNCTION
                                        || type_ == PACKAGE_FUNCTION
                                        || UDT_FUNCTION == type_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_db_id() const { return db_id_; }

  virtual uint64_t get_database_id() const { return db_id_; }
  virtual uint64_t get_package_id() const { return pkg_id_; }
  inline uint64_t get_pkg_id() const { return pkg_id_; }
  inline ObProcType get_type() const { return type_; }
  int get_idx(int64_t &idx) const;
  inline uint64_t get_parent_id() const { return parent_id_; }
  inline uint64_t get_id() const { return id_; }
  inline uint64_t get_routine_id() const { return id_; }
  inline const ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_; }
  inline int set_subprogram_path(const ObIArray<int64_t> &path) { return append(subprogram_path_, path); }
  inline int add_subprogram_path(int64_t path) { return subprogram_path_.push_back(path); }
  inline void set_priv_user(const ObString &priv_user) { priv_user_ = priv_user; }
  inline const common::ObString &get_priv_user() const { return priv_user_; }
  inline const ObString &get_name() const { return name_; }
  inline const ObString &get_decl_str() const { return decl_str_; }
  inline const ObString &get_route_sql() const { return route_sql_; }
  inline const ObString &get_routine_body() const { return routine_body_; }
  inline void set_name(const ObString &name) { name_ = name; }
  inline void set_decl_str(const ObString &decl_str) { decl_str_ = decl_str; }
  inline void set_route_sql(const ObString &route_sql) { route_sql_ = route_sql; }
  inline void set_routine_body(const ObString &routine_body) { routine_body_ = routine_body; }
  inline uint64_t get_md5() const { return md5_; }
  int set_idx(int64_t idx);
  inline void set_parent_id(uint64_t id) { parent_id_ = id; }
  inline void set_id(uint64_t id) { id_ = id; }
  inline void set_md5(uint64_t md5) { md5_ = md5; }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_db_id(uint64_t db_id) { db_id_ = db_id; }
  inline void set_pkg_id(uint64_t pkg_id) { pkg_id_ = pkg_id; }
  inline void set_type(ObProcType type) { type_ = type; }
  inline void set_ret_info(ObPLRoutineParam *ret_info) { ret_info_ = ret_info; }
  inline const share::schema::ObIRoutineParam *get_ret_info() const { return ret_info_; }
  int add_param(ObPLRoutineParam *param_info);
  inline const common::ObIArray<ObPLRoutineParam *> &get_params() const { return params_; }
  inline common::ObIArray<AccessorItem> &get_accessors() { return accessors_; }
  inline const common::ObIArray<AccessorItem> &get_accessors() const { return accessors_; }
  inline int64_t get_param_count() const { return params_.count(); }
  int is_equal(const ObPLRoutineInfo* other, bool &equal) const;
  int get_routine_param(int64_t idx, ObPLRoutineParam*& param) const;
  int get_routine_param(int64_t idx, share::schema::ObIRoutineParam*& param) const;
  int find_param_by_name(const ObString &name, int64_t &position) const;

  inline void set_compile_flag(const ObPLCompileFlag &compile_flag)
  {
    compile_flag_ = compile_flag;
  }
  inline int add_compile_flag(const ObPLCompileFlag &compile_flag)
  {
    return compile_flag_.add_compile_flag(compile_flag);
  }
  inline const ObPLCompileFlag &get_compile_flag() const { return compile_flag_; }
  inline ObPLCompileFlag &get_compile_flag() { return compile_flag_; }

  inline int32_t get_line_number() const
  {
    return static_cast<int32_t>((loc_ & 0xFFFFFFFF00000000)>>32);
  }
  inline int32_t get_col_number() const
  {
    return static_cast<int32_t>(loc_ & 0x00000000FFFFFFFF);
  }
  inline void set_loc(uint64_t loc) { loc_ = loc; }

  inline void set_pipelined() { is_pipelined_ = true; }
  inline bool is_pipelined() const { return is_pipelined_; }

  virtual void set_deterministic() { is_deterministic_ = true; }
  virtual bool is_deterministic() const { return is_deterministic_; }

  virtual void set_no_sql() { is_no_sql_ = true; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  virtual bool is_no_sql() const { return is_no_sql_; }
  virtual void set_reads_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = true; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  virtual bool is_reads_sql_data() const { return is_reads_sql_data_; }
  virtual void set_modifies_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = true; is_contains_sql_ = false; }
  virtual bool is_modifies_sql_data() const { return is_modifies_sql_data_; }
  virtual void set_contains_sql() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = true; }
  virtual bool is_contains_sql() const { return is_contains_sql_; }

  virtual void set_parallel_enable() { is_parallel_enable_ = true; }
  virtual bool is_parallel_enable() const { return is_parallel_enable_; }

  virtual bool is_invoker_right() const { return compile_flag_.compile_with_invoker_right(); }

  inline void set_analyze_flag(uint64_t flag) { analyze_flag_ = flag; }
  inline uint64_t get_analyze_flag() const { return analyze_flag_; }

  virtual void set_wps() { is_wps_ = true; }
  virtual bool is_wps() const { return is_wps_; }
  virtual void set_rps() { is_rps_ = true; }
  virtual bool is_rps() const { return is_rps_; }
  virtual void set_has_sequence() { is_has_sequence_ = true; }
  virtual bool is_has_sequence() const { return is_has_sequence_; }
  virtual void set_has_out_param() { is_has_out_param_ = true; }
  virtual bool is_has_out_param() const { return is_has_out_param_; }
  virtual void set_external_state() { is_external_state_ = true; }
  virtual bool is_external_state() const { return is_external_state_; }

  virtual void set_result_cache() { is_result_cache_ = true; }
  virtual bool is_result_cache() const { return is_result_cache_; }

  virtual void set_accessible_by_clause() { has_accessible_by_clause_ = true; }
  virtual bool has_accessible_by_clause() const { return has_accessible_by_clause_; }

  virtual void set_is_udt_routine() { is_udt_routine_ = true; }
  virtual bool is_udt_routine() const { return is_udt_routine_; }
  virtual bool is_udt_static_routine() const { return compile_flag_.compile_with_static(); }

  virtual void set_is_private_routine() { is_private_routine_ = true; }
  virtual bool is_private_routine() const { return is_private_routine_; }

  virtual void set_is_udt_cons() { compile_flag_.add_cons(); }
  virtual bool is_udt_cons() const { return compile_flag_.compile_with_cons(); }
  virtual bool is_udt_map() const { return compile_flag_.compile_with_map(); }
  virtual bool is_udt_order() const { return compile_flag_.compile_with_order(); }

  virtual const ObString& get_routine_name() const { return get_name(); }

  bool has_self_param() const;
  int64_t get_self_param_pos() const;

  bool has_generic_type() const;

  TO_STRING_KV(K_(tenant_id),
               K_(db_id),
               K_(pkg_id),
               K_(type),
               K_(parent_id),
               K_(id),
               K_(subprogram_path),
               K_(name),
               K_(decl_str),
               K_(md5),
               KPC_(ret_info),
               K_(params),
               K_(compile_flag),
               K_(is_pipelined),
               K_(is_deterministic),
               K_(is_parallel_enable),
               K_(is_udt_routine),
               K_(accessors),
               K_(loc),
               K_(analyze_flag));
private:
  common::ObIAllocator &allocator_;
  uint64_t tenant_id_;
  uint64_t db_id_;
  uint64_t pkg_id_;
  ObProcType type_;
  uint64_t parent_id_;
  uint64_t id_;
  ObPLSEArray<int64_t> subprogram_path_;
  common::ObString name_;
  common::ObString decl_str_;
  common::ObString route_sql_;
  common::ObString routine_body_;
  uint64_t md5_;
  ObPLRoutineParam *ret_info_;
  ObPLSEArray<ObPLRoutineParam *> params_;
  ObPLCompileFlag compile_flag_;
  bool is_pipelined_;
  bool is_deterministic_;
  bool is_parallel_enable_;
  bool is_result_cache_;
  bool has_accessible_by_clause_;
  bool is_udt_routine_;
  bool is_private_routine_;
  ObPLSEArray<AccessorItem> accessors_;
  common::ObString priv_user_;
  uint64_t loc_;
  union {
    uint64_t analyze_flag_;
    struct {
      uint64_t is_no_sql_ : 1;
      uint64_t is_reads_sql_data_ : 1;
      uint64_t is_modifies_sql_data_ : 1;
      uint64_t is_contains_sql_ : 1;
      uint64_t is_wps_ : 1;
      uint64_t is_rps_ : 1;
      uint64_t is_has_sequence_ : 1;
      uint64_t is_has_out_param_ : 1;
      uint64_t is_external_state_ : 1;
      uint64_t reserved_:54;
    };
  };
};

class ObPLFunctionAST;
class ObPLCompileUnitAST;
class ObPLRoutineTable
{
public:
  static const int64_t INIT_ROUTINE_COUNT = 1;
  static const int64_t INIT_ROUTINE_IDX = 0;
  static const int64_t NORMAL_ROUTINE_START_IDX = 1;

  ObPLRoutineTable(common::ObIAllocator &allocator)
    : allocator_(allocator),
      routine_infos_(allocator),
      routine_asts_(allocator) {}
  virtual ~ObPLRoutineTable();

  int init(ObPLRoutineTable *parent_routine_table);
  int make_routine_info(common::ObIAllocator &allocator,
                        const ObString &name,
                        ObProcType type,
                        const ObString &decl_str,
                        uint64_t database_id,
                        uint64_t pkg_id,
                        uint64_t id,
                        const ObIArray<int64_t> &subprogram_path,
                        ObPLRoutineInfo *&routine_info) const;
  int make_routine_ast(common::ObIAllocator &allocator,
                       const ObString &db_name,
                       const ObString &package_name,
                       uint64_t package_version,
                       const ObPLRoutineInfo &routine_info,
                       ObPLFunctionAST *&routine_ast);
  inline common::ObIArray<ObPLRoutineInfo *> &get_routine_infos() { return routine_infos_; }
  inline uint64_t get_count() const { return routine_infos_.count(); }
  int add_routine_info(ObPLRoutineInfo *routine_info);
  int set_routine_info(int64_t routine_idx, ObPLRoutineInfo *routine_info);
  int set_routine_ast(int64_t routine_idx, ObPLFunctionAST *routine_ast);
  int get_routine_info(const ObPLRoutineInfo *routine_info, const ObPLRoutineInfo *&info) const;
  int get_routine_info(int64_t routine_idx, const ObPLRoutineInfo *&routine_info) const;
  int get_routine_info(int64_t routine_idx, ObPLRoutineInfo *&routine_info);
  int get_routine_info(const ObString &routine_decl_str, ObPLRoutineInfo *&routine_info) const;
  int get_routine_ast(int64_t routine_idx, ObPLFunctionAST *&routine_ast) const;
  inline const ObPLRoutineInfo *get_init_routine_info() { return routine_infos_[INIT_ROUTINE_IDX]; }
  inline void set_init_routine_info(ObPLRoutineInfo *init_routine_info) { routine_infos_.at(INIT_ROUTINE_IDX) = init_routine_info; }
  inline const ObPLFunctionAST *get_init_routine_ast() { return routine_asts_[INIT_ROUTINE_IDX]; }
  inline void set_init_routine_ast(ObPLFunctionAST *init_routine_ast) { routine_asts_.at(INIT_ROUTINE_IDX) = init_routine_ast; }

private:
  common::ObIAllocator &allocator_;
  ObPLSEArray<ObPLRoutineInfo *> routine_infos_;
  ObPLSEArray<ObPLFunctionAST *> routine_asts_;
};

class ObPLStmtBlock;
class ObPLExternalNS
{
public:
  enum ExternalType
  {
    INVALID_VAR = -1,
    LOCAL_VAR = 0,      // 本地变量
    DB_NS,              // 数据库名字 如test.t中的test
    PKG_NS,             // package名字 如dbms_output.put_line中的dbms_output
    PKG_VAR,            // package中的变量 如dbms_output.var中var
    USER_VAR,           // 用户变量(mysql mode)
    SESSION_VAR,        // session变量(mysql mode)
    GLOBAL_VAR,         // 全局变量(mysql mode)
    TABLE_NS,           // 表名字
    TABLE_COL,          // 表中的列
    LABEL_NS,           // Label
    SUBPROGRAM_VAR,     // 子过程中的变量
    EXPR_VAR,           //for table type access index
    CONST_VAR,          //常量 special case for is_expr
    PROPERTY_TYPE,      //固有属性，如count
    INTERNAL_PROC,      //Package中的Procedure
    EXTERNAL_PROC,      //Standalone的Procedure
    NESTED_PROC,
    TYPE_METHOD_TYPE,   //自定义类型的方法
    SYSTEM_PROC,        //系统中已经预定义的Procedure(如: RAISE_APPLICATION_ERROR)
    UDT_NS,
    UDF_NS,
    LOCAL_TYPE,         // 本地的自定义类型
    PKG_TYPE,           // 包中的自定义类型
    SELF_ATTRIBUTE,
    DBLINK_PKG_NS,      // dblink package
    UDT_MEMBER_ROUTINE, //
  };

  ObPLExternalNS(const ObPLResolveCtx &resolve_ctx, const ObPLBlockNS *parent_ns)
    : resolve_ctx_(resolve_ctx), parent_ns_(parent_ns), dependency_table_(NULL) {}
  virtual ~ObPLExternalNS() {}
  inline bool is_procedure(ObProcType routine_type) const { return STANDALONE_PROCEDURE == routine_type || PACKAGE_PROCEDURE == routine_type; }
  inline bool is_function(ObProcType routine_type) const { return STANDALONE_FUNCTION == routine_type || PACKAGE_FUNCTION == routine_type; }
  int resolve_synonym(uint64_t object_db_id,
                      const ObString &object_name,
                      ExternalType &type,
                      uint64_t &parent_id,
                      int64_t &var_idx,
                      const ObString &synonym_name,
                      const uint64_t cur_db_id) const;
  int resolve_external_symbol(const common::ObString &name, ExternalType &type, ObPLDataType &data_type,
                              uint64_t &parent_id, int64_t &var_idx) const;
  int resolve_external_type_by_name(const ObString &db_name,
                                    const ObString &package_name,
                                    const ObString &type_name,
                                    const ObUserDefinedType *&user_type,
                                    bool try_synonym);
  int resolve_external_type_by_id(uint64_t type_id, const ObUserDefinedType *&user_type);
  int resolve_external_routine(const ObString &db_name,
                               const ObString &package_name,
                               const ObString &routine_name,
                               const common::ObIArray<sql::ObRawExpr *> &expr_params,
                               ObProcType &routine_type,
                               ObIArray<const share::schema::ObIRoutineInfo *> &routine_info) const;
  int check_routine_exists(const ObString &db_name,
                           const ObString &package_name,
                           const ObString &routine_name,
                           const share::schema::ObRoutineType routine_type,
                           bool &exists,
                           pl::ObProcType &proc_type,
                           uint64_t udt_id) const;
  int search_in_standard_package(const ObString &name,
                                 ExternalType &type,
                                 ObPLDataType &data_type,
                                 uint64_t &parent_id,
                                 int64_t &var_idx) const;
  inline const ObPLBlockNS *get_parent_ns() const { return parent_ns_; }
  inline const ObPLResolveCtx &get_resolve_ctx() { return resolve_ctx_; }
  inline const ObPLDependencyTable *get_dependency_table() const { return dependency_table_; }

  inline ObPLDependencyTable *get_dependency_table() { return dependency_table_; }
  inline void set_dependency_table(ObPLDependencyTable *dependency_table) { dependency_table_ = dependency_table; }
  int add_dependency_object(const share::schema::ObSchemaObjVersion &obj_version) const;

private:
  const ObPLResolveCtx &resolve_ctx_;
  const ObPLBlockNS *parent_ns_;
  ObPLDependencyTable *dependency_table_;
};

class ObPLUDTNS : public ObPLINS
{
public:
  ObPLUDTNS(share::schema::ObSchemaGetterGuard &schema_guard)
    : schema_guard_(schema_guard) {}

  virtual int get_user_type(uint64_t type_id,
                            const ObUserDefinedType *&user_type,
                            ObIAllocator *allocator = NULL) const;

private:
  share::schema::ObSchemaGetterGuard &schema_guard_;
};

class ObPLBlockNS : public ObPLINS
{
public:
  enum BlockType {
    BLOCK_ROUTINE,
    BLOCK_PACKAGE_SPEC,
    BLOCK_PACKAGE_BODY,
    BLOCK_OBJECT_SPEC,
    BLOCK_OBJECT_BODY,
  };
  ObPLBlockNS(common::ObIAllocator &allocator,
              ObPLBlockNS *pre_ns,
              ObPLSymbolTable *symbol_table,
              ObPLLabelTable *label_table,
              ObPLConditionTable *condition_table,
              ObPLCursorTable *cursor_table,
              ObPLRoutineTable *routine_table,
              common::ObIArray<sql::ObRawExpr*> *exprs,
              common::ObIArray<sql::ObRawExpr*> *obj_access_exprs,
              ObPLExternalNS *external_ns)
    : pre_ns_(pre_ns),
      type_(BLOCK_ROUTINE),
      db_name_(),
      database_id_(OB_INVALID_ID),
      package_name_(),
      package_id_(OB_INVALID_ID),
      package_version_(OB_INVALID_VERSION),
      routine_id_(OB_INVALID_ID),
      routine_name_(),
      stop_search_label_(false),
      explicit_block_(false),
      function_block_(false),
      udt_routine_(false),
      types_(allocator),
      symbols_(allocator),
      labels_(allocator),
      conditions_(allocator),
      cursors_(allocator),
      routines_(allocator),
      type_table_(NULL),
      symbol_table_(symbol_table),
      label_table_(label_table),
      condition_table_(condition_table),
      cursor_table_(cursor_table),
      routine_table_(routine_table),
      exprs_(exprs),
      obj_access_exprs_(obj_access_exprs),
      external_ns_(external_ns),
      compile_flag_() {}
  virtual ~ObPLBlockNS() {}

  virtual int get_user_type(uint64_t type_id,
                            const ObUserDefinedType *&user_type,
                            ObIAllocator *allocator = NULL) const;

public:
  inline const ObPLBlockNS *get_pre_ns() const { return pre_ns_; }
  inline void set_pre_ns(const ObPLBlockNS *ns) { pre_ns_ = ns; }
  inline uint64_t get_block_type() const { return type_; }
  inline void set_block_type(BlockType type) { type_ = type; }
  inline const common::ObString &get_db_name() const { return db_name_; }
  inline void set_db_name(const ObString &db_name) { db_name_ = db_name; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline const common::ObString &get_package_name() const { return package_name_; }
  inline void set_package_name(const ObString &package_name) { package_name_ = package_name; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  inline const common::ObString &get_routine_name() const { return routine_name_; }
  inline void set_routine_name(const ObString &routine_name) { routine_name_ = routine_name; }
  inline bool stop_search_label() const { return stop_search_label_; }
  inline void set_stop_search_label() { stop_search_label_ = true; }
  inline bool explicit_block() const { return explicit_block_; }
  inline void set_explicit_block() { explicit_block_ = true; }
  inline bool function_block() const { return function_block_; }
  inline void set_function_block() { function_block_ = true; }
  inline uint64_t get_package_version() const { return package_version_; }
  inline void set_package_version(uint64_t package_version) { package_version_ = package_version; }
  int add_type(ObUserDefinedType *type);
  inline const common::ObIArray<int64_t> &get_types() const { return types_; }
  inline common::ObIArray<int64_t> &get_types() { return types_; }
  inline ObPLUserTypeTable *get_type_table() { return type_table_; }
  inline ObPLUserTypeTable *get_type_table() const { return type_table_; }
  inline void set_type_table(ObPLUserTypeTable *user_type_table) { type_table_ = user_type_table; }
  int add_type(const common::ObString &name, const ObPLDataType &type);
  inline const common::ObIArray<int64_t> &get_symbols() const { return symbols_; }
  inline common::ObIArray<int64_t> &get_symbols() { return symbols_; }
  inline ObPLSymbolTable *get_symbol_table() { return symbol_table_; }
  inline const ObPLSymbolTable *get_symbol_table() const { return symbol_table_; }
  inline void set_symbol_table(ObPLSymbolTable *symbol_table) { symbol_table_ = symbol_table; }
  int add_symbol(const ObString &name, const ObPLDataType &type, const sql::ObRawExpr *expr = NULL,
                 const bool read_only = false, const bool not_null = false,
                 const bool default_construct = false,
                 const bool is_formal_param = false);
  int delete_symbols();
  inline const common::ObIArray<int64_t> &get_labels() const { return labels_; }
  inline ObPLLabelTable *get_label_table() { return label_table_; }
  inline const ObPLLabelTable *get_label_table() const { return label_table_; }
  inline void set_label_table(ObPLLabelTable *label_table) { label_table_ = label_table; }
  int add_label(const common::ObString &name,
                const ObPLLabelTable::ObPLLabelType &type,
                ObPLStmt *stmt);
  inline const common::ObIArray<int64_t> &get_conditions() const { return conditions_; }
  inline ObPLConditionTable *get_condition_table() { return condition_table_; }
  inline const ObPLConditionTable *get_condition_table() const { return condition_table_; }
  inline void set_condition_table(ObPLConditionTable *condition_table) { condition_table_ = condition_table; }
  int add_condition(const common::ObString &name,
                    const ObPLConditionValue &value,
                    bool exception_init = false);
  inline const common::ObIArray<int64_t> &get_cursors() const { return cursors_; }
  inline common::ObIArray<int64_t> &get_cursors() { return cursors_; }
  inline ObPLCursorTable *get_cursor_table() { return cursor_table_; }
  inline const ObPLCursorTable *get_cursor_table() const { return cursor_table_; }
  inline void set_cursor_table(ObPLCursorTable *cursor_table) { cursor_table_ = cursor_table; }
  int add_cursor(const common::ObString &name,
                 const ObPLDataType &type,
                 const common::ObString &sql,
                 const common::ObIArray<int64_t> &sql_params,
                 const common::ObString &ps_sql,
                 sql::stmt::StmtType stmt_type,
                 bool for_update,
                 bool has_hidden_rowid,
                 uint64_t rowid_table_id,
                 const common::ObIArray<share::schema::ObSchemaObjVersion> &ref_objects,
                 const ObRecordType* row_desc,
                 const ObPLDataType& cursor_type,
                 const common::ObIArray<int64_t> &formal_params,
                 ObPLCursor::CursorState state,
                 bool has_dup_column_name,
                 int64_t &index,
                 bool skip_locked = false);
  int add_questionmark_cursor(const int64_t symbol_idx);
  inline const common::ObIArray<sql::ObRawExpr*> *get_exprs() const { return exprs_; }
  inline void set_exprs(common::ObIArray<sql::ObRawExpr*> *exprs) { exprs_ = exprs; }
  inline void set_obj_access_exprs(common::ObIArray<sql::ObRawExpr*> *obj_access_exprs)
  {
    obj_access_exprs_ = obj_access_exprs;
  }
  inline ObPLExternalNS *get_external_ns() { return external_ns_; }
  inline const ObPLExternalNS *get_external_ns() const { return external_ns_; }
  inline void set_external_ns(ObPLExternalNS *external_ns) { external_ns_ = external_ns; }
  int check_dup_symbol(const ObString &name, const ObPLDataType &type, bool &is_dup) const;
  int check_dup_label(const ObString &name, bool &is_dup) const;
  int check_dup_goto_label(const ObString &name, bool &is_dup) const;
  int check_dup_condition(const ObString &name, bool &is_dup, const void *&dup_item) const;
  int check_dup_cursor(const ObString &name, bool &is_dup) const;
  int check_dup_type(const ObString &name, bool &is_dup, const void *&dup_item) const;
  int get_pl_data_type_by_name(const ObPLResolveCtx &resolve_ctx,
                               const ObString &db_name, const ObString &package_name,
                               const ObString &type_name, const ObUserDefinedType *&user_type) const;
  int get_pl_data_type_by_id(uint64_t type_id, const ObUserDefinedType *&user_type) const;
#ifdef OB_BUILD_ORACLE_PL
  int get_subtype(uint64_t type_id, const ObUserDefinedSubType *&subtype);
#endif
  int get_subtype_actually_basetype(ObPLDataType &pl_type);
  int get_subtype_actually_basetype(const ObPLDataType *pl_type,
                                    const ObPLDataType *&actually_type);
  int get_subtype_not_null_constraint(const ObPLDataType *pl_type, bool &not_null);
  int get_subtype_range_constraint(const ObPLDataType *pl_type,
                                  bool &has_range, int64_t &lower, int64_t &upper);
  int get_cursor(uint64_t pkg_id, uint64_t routine_id, int64_t idx,
                 const ObPLCursor *&cursor) const;
  int get_cursor_var(uint64_t pkg_id, uint64_t routine_id, int64_t idx,
                   const ObPLVar *&var) const;
  int get_cursor_by_name(const ObExprResolveContext &resolve_ctx,
                         const ObString &database_name, 
                         const ObString &package_name, 
                         const ObString &cursor_name, 
                         const ObPLCursor *&cursor) const;

  int resolve_local_symbol(const common::ObString &name,
                           ObPLExternalNS::ExternalType &type,
                           ObPLDataType &data_type,
                           int64_t &var_idx) const;
  int resolve_local_label(const common::ObString &name,
                          ObPLExternalNS::ExternalType &type,
                          int64_t &var_idx) const;

  int try_resolve_udt_name(const ObString &udt_var_name,
                           ObString &udt_type_name,
                           uint64_t &udt_id,
                           pl::ObPLExternalNS::ExternalType external_type
                            = pl::ObPLExternalNS::ExternalType::INVALID_VAR,
                           uint64_t parent_id = OB_INVALID_ID) const;

  int get_package_var(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                      const ObString &var_name, const ObPLVar *&var, int64_t &var_idx) const;
  int get_package_var(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                      int64_t var_idx, const ObPLVar *&var) const;
  
  int get_subprogram_var(
    uint64_t package_id, uint64_t routine_id, int64_t var_idx, const ObPLVar *&var) const;

  static
  int search_parent_next_ns(const ObPLBlockNS *parent_ns,
                            const ObPLBlockNS *current_ns,
                            const ObPLBlockNS *&next_ns);
  int search_parent_next_ns(const ObPLBlockNS *parent_ns, const ObPLBlockNS *&next_ns) const;
  int resolve_label_symbol(const common::ObString &var_name, ObPLExternalNS::ExternalType &type,
                           ObPLDataType &data_type, const ObPLBlockNS *&parent_ns,
                           int64_t &var_idx) const;
  int resolve_symbol(const common::ObString &var_name, ObPLExternalNS::ExternalType &type,
                     ObPLDataType &data_type, uint64_t &parent_id, int64_t &var_idx,
                     bool resolve_external = true) const;
  int resolve_udt_symbol(uint64_t udt_id, const common::ObString &var_name,
                    ObPLExternalNS::ExternalType &type,
                    ObPLDataType &data_type,
                    uint64_t &parent_id,
                    int64_t &var_idx,
                    ObString &parent_udt_name) const;
  inline const ObPLRoutineTable *get_routine_table() const { return routine_table_; }
  inline void set_routine_table(ObPLRoutineTable *routine_table) { routine_table_ = routine_table; }
  int get_routine_info(int64_t routine_idx, const ObPLRoutineInfo *&routine) const;
  int get_routine_info(const ObString &routine_decl_str, ObPLRoutineInfo *&routine) const;
  int get_routine_info(const ObPLRoutineInfo *routine_info, const ObPLRoutineInfo *&info) const;
  int add_routine_info(ObPLRoutineInfo *routine_info);
  int set_routine_info(int64_t routine_idx, ObPLRoutineInfo *routine_info);
  inline const common::ObIArray<int64_t>& get_routines() const { return routines_; }
  inline common::ObIArray<int64_t>& get_routines() { return routines_; }
  bool search_routine_local(const ObString &db_name, const ObString &package_name) const;
  int resolve_routine(const ObPLResolveCtx &resolve_ctx,
                      const ObString &db_name,
                      const ObString &package_name,
                      const ObString &routine_name,
                      const common::ObIArray<sql::ObRawExpr *> &expr_params,
                      ObProcType &routine_type,
                      const share::schema::ObIRoutineInfo *&routine_info) const;
  int resolve_routine(const ObPLResolveCtx &resolve_ctx,
                      const ObString &db_name,
                      const ObString &package_name,
                      const ObString &routine_name,
                      const common::ObIArray<sql::ObRawExpr *> &expr_params,
                      ObProcType &routine_type,
                      ObIArray<const share::schema::ObIRoutineInfo *> &routine_infos) const;
  int check_routine_exists(const ObString &db_name,
                                            const ObString &package_name,
                                            const ObString &routine_name,
                                            const share::schema::ObRoutineType routine_type,
                                            bool &exists,
                                            pl::ObProcType &proc_type,
                                            uint64_t udt_id) const;
#ifdef OB_BUILD_ORACLE_PL
  int add_column_conv_for_coll_func(ObSQLSessionInfo &session_info,
                                    ObRawExprFactory &expr_factory,
                                    const ObUserDefinedType *user_type,
                                    const ObString &attr_name,
                                    ObRawExpr *&expr) const;
#endif
  int find_sub_attr_by_name(const ObUserDefinedType &user_type,
                            const sql::ObObjAccessIdent &access_ident,
                            ObSQLSessionInfo &session_info,
                            ObRawExprFactory &expr_factory,
                            ObPLCompileUnitAST &func,
                            ObObjAccessIdx &access_idx,
                            ObPLDataType &data_type,
                            uint64_t &package_id,
                            int64_t &var_idx) const;
  int find_sub_attr_by_index(const ObUserDefinedType &user_type, int64_t attr_index, const sql::ObRawExpr *func_expr, ObObjAccessIdx &access_idx) const;
  int expand_data_type(const ObUserDefinedType *user_type,
                       ObIArray<ObDataType> &types,
                       ObIArray<bool> *not_null_flags = NULL,
                       ObIArray<int64_t> *pls_ranges = NULL) const;
  int expand_data_type_once(const ObUserDefinedType *user_type,
                            ObIArray<ObDataType> &types,
                            ObIArray<bool> *not_null_flags = NULL,
                            ObIArray<int64_t> *pls_ranges = NULL) const;
  inline bool is_procedure(ObProcType routine_type) const
  {
    return STANDALONE_PROCEDURE == routine_type
        || PACKAGE_PROCEDURE == routine_type
        || NESTED_PROCEDURE == routine_type
        || UDT_PROCEDURE == routine_type;
  }
  inline bool is_function(ObProcType routine_type) const
  {
    return STANDALONE_FUNCTION == routine_type
        || PACKAGE_FUNCTION == routine_type
        || NESTED_FUNCTION == routine_type
        || UDT_FUNCTION == routine_type;
  }
  inline void set_compile_flag(const ObPLCompileFlag &compile_flag)
  {
    compile_flag_ = compile_flag;
  }
  inline const ObPLCompileFlag &get_compile_flag() const { return compile_flag_; }

  inline void set_is_udt_routine() { udt_routine_ = true; }
  inline bool is_udt_routine() const { return udt_routine_; }

  const ObPLBlockNS* get_udt_routine_ns() const;

  int extract_external_record_default_expr(ObRawExpr &expr) const;

  DECLARE_TO_STRING;

private:
  const ObPLBlockNS *pre_ns_;
  BlockType type_;
  common::ObString db_name_;
  uint64_t database_id_;
  common::ObString package_name_;
  uint64_t package_id_;
  uint64_t package_version_;
  uint64_t routine_id_;
  common::ObString routine_name_;
  bool stop_search_label_; //handler的body首层block置为true，因为handler的body看不到外层的label
  bool explicit_block_; //用户通过BEGIN END显示声明的BLOCK, 对应Parser的T_SP_BLOCK_CONTENT
  bool function_block_; //是否是Function的Block
  bool udt_routine_; // function 是否udt里面的function，这种函数需要特殊处理，因为它能访问udt的属性
  ObPLSEArray<int64_t> types_; //类型表里的下标
  ObPLSEArray<int64_t> symbols_; //符号表里的下标
  ObPLSEArray<int64_t> labels_; //标签表里的下标
  ObPLSEArray<int64_t> conditions_; //异常表里的下标
  ObPLSEArray<int64_t> cursors_; //游标表里的下标
  ObPLSEArray<int64_t> routines_; //子过程在过程表里的下标
  ObPLUserTypeTable *type_table_; //全局用户自定义数据类型表，所有的block都指向ObPLFunctionAST里的
  ObPLSymbolTable *symbol_table_; //全局符号表，所有的block都指向ObPLFunctionAST里的
  ObPLLabelTable *label_table_; //全局标签表，所有的block都指向ObPLFunctionAST里的
  ObPLConditionTable *condition_table_; //全局异常表，所有的block都指向ObPLFunctionAST里的
  ObPLCursorTable *cursor_table_; //全局游标表，所有的block都指向ObPLFunctionAST里的
  ObPLRoutineTable *routine_table_;
  common::ObIArray<sql::ObRawExpr*> *exprs_; //使用的表达式，所有的block都指向ObPLFunctionAST里的
  common::ObIArray<sql::ObRawExpr*> *obj_access_exprs_; //所有Block都指向ObPLFunctionAST里的
  ObPLExternalNS *external_ns_; //外部名称空间，用于解释PL所解释不了外部变量：包变量、用户变量、系统变量
  ObPLCompileFlag compile_flag_;
};

class ObPLStmtVisitor;
class ObPLStmtBlock;
class ObPLSqlStmt;

class ObPLCompileUnitAST
{
public:
  enum UnitType {
    INVALID_TYPE = -1,
    ROUTINE_TYPE,
    PACKAGE_TYPE,
    OBJECT_TYPE,
  };

  ObPLCompileUnitAST(common::ObIAllocator &allocator, UnitType type)
     : type_(type),
       body_(NULL),
       obj_access_exprs_(allocator),
       exprs_(allocator),
       simple_calc_bitset_(),
       sql_stmts_(allocator),
       expr_factory_(allocator),
       symbol_table_(allocator),
       symbol_debuginfo_table_(allocator),
       user_type_table_(),
       label_table_(),
       condition_table_(),
       cursor_table_(allocator),
       routine_table_(allocator),
       dependency_table_(),
       compile_flag_(),
       can_cached_(true),
       priv_user_(),
       analyze_flag_(0)
  {}

  virtual ~ObPLCompileUnitAST();

  inline UnitType get_type() const { return type_; }
  inline void set_type(UnitType type) { type_ = type; }
  inline bool is_routine() const { return ROUTINE_TYPE == type_; }
  inline bool is_package() const { return PACKAGE_TYPE == type_; }
  inline bool is_object() const { return OBJECT_TYPE == type_; }
  inline const ObPLStmtBlock *get_body() const { return body_; }
  inline ObPLStmtBlock *get_body() { return body_; }
  inline void set_body(ObPLStmtBlock *body) { body_ = body; }
  inline const common::ObIArray<sql::ObRawExpr*> &get_obj_access_exprs() const { return obj_access_exprs_; }
  inline common::ObIArray<sql::ObRawExpr*> &get_obj_access_exprs() { return obj_access_exprs_; }
  inline int64_t get_obj_access_expr_count() { return obj_access_exprs_.count(); }
  inline const sql::ObRawExpr* get_obj_access_expr(int64_t i) const { return obj_access_exprs_.at(i); }
  inline sql::ObRawExpr* get_obj_access_expr(int64_t i) { return obj_access_exprs_.at(i); }
  inline int set_obj_access_exprs(common::ObIArray<sql::ObRawExpr*> &exprs) { return append(obj_access_exprs_, exprs); }
  inline int add_obj_access_expr(sql::ObRawExpr* expr) { return obj_access_exprs_.push_back(expr); }
  inline int add_obj_access_exprs(common::ObIArray<sql::ObRawExpr*> &exprs) { return append(obj_access_exprs_, exprs); }
  inline const common::ObIArray<sql::ObRawExpr*> &get_exprs() const { return exprs_; }
  inline common::ObIArray<sql::ObRawExpr*> &get_exprs() { return exprs_; }
  inline const sql::ObRawExpr* get_expr(int64_t i) const { return exprs_.at(i); }
  inline sql::ObRawExpr* get_expr(int64_t i) { return exprs_.at(i); }
  int set_exprs(common::ObIArray<sql::ObRawExpr*> &exprs);
  int add_expr(sql::ObRawExpr* expr, bool is_simple_integer = false);
  int add_exprs(common::ObIArray<sql::ObRawExpr*> &exprs);
  inline void set_expr(sql::ObRawExpr* expr, int64_t i) { exprs_.at(i) = expr; }
  inline int64_t get_expr_count() const { return exprs_.count(); }
  inline int add_simple_calc(int64_t i) { return simple_calc_bitset_.add_member(i); }
  inline int add_simple_calcs(const ObBitSet<> &simple_calc) { return simple_calc_bitset_.add_members(simple_calc); }
  inline const ObBitSet<> & get_simple_calcs() const { return simple_calc_bitset_; }
  inline bool is_simple_calc(int64_t i) const { return simple_calc_bitset_.has_member(i); }
  inline sql::ObRawExprFactory &get_expr_factory() { return expr_factory_; }
  inline const ObPLSymbolTable &get_symbol_table() const { return symbol_table_; }
  inline ObPLSymbolTable &get_symbol_table() { return symbol_table_; }
  inline const ObPLLabelTable &get_label_table() const { return label_table_; }
  inline ObPLLabelTable &get_label_table() { return label_table_; }
  inline const ObPLUserTypeTable &get_user_type_table() const { return user_type_table_; }
  inline ObPLUserTypeTable &get_user_type_table() { return user_type_table_; }
  inline const ObPLConditionTable &get_condition_table() const { return condition_table_; }
  inline ObPLConditionTable &get_condition_table() { return condition_table_; }
  inline const ObPLCursorTable &get_cursor_table() const { return cursor_table_; }
  inline ObPLCursorTable &get_cursor_table() { return cursor_table_; }
  inline ObPLRoutineTable &get_routine_table() { return routine_table_; }
  inline const ObPLRoutineTable &get_routine_table() const { return routine_table_; }
  inline const ObPLDependencyTable &get_dependency_table() const { return dependency_table_; }
  inline ObPLDependencyTable &get_dependency_table() { return dependency_table_; }
  int add_dependency_objects(
                  const common::ObIArray<share::schema::ObSchemaObjVersion> &dependency_objects);
  int add_dependency_object(const share::schema::ObSchemaObjVersion &obj_version);
  static int add_dependency_object_impl(const ObPLDependencyTable &dep_tbl,
                                        const share::schema::ObSchemaObjVersion &obj_version);
  static int add_dependency_object_impl(ObPLDependencyTable &dep_tbl,
                             const share::schema::ObSchemaObjVersion &obj_version);
  inline bool get_can_cached() const { return can_cached_; }
  inline void set_can_cached(bool can_cached) { can_cached_ = can_cached; }
  int add_sql_exprs(common::ObIArray<sql::ObRawExpr*> &exprs);

  inline const ObPLCompileFlag &get_compile_flag() const { return compile_flag_; }
  inline ObPLCompileFlag &get_compile_flag() { return compile_flag_; }
  inline void set_compile_flag(const ObPLCompileFlag &compile_flag)
  {
    compile_flag_ = compile_flag;
  }

  inline ObString &get_priv_user() { return priv_user_; }
  inline void set_priv_user(const ObString &priv_user)
  {
    priv_user_ = priv_user;
  }

  inline ObIArray<ObPLSqlStmt *>& get_sql_stmts() { return sql_stmts_; }

  void process_default_compile_flag();

  virtual const common::ObString &get_db_name() const = 0;
  virtual const common::ObString &get_name() const = 0;
  virtual uint64_t get_id() const = 0;
  virtual int64_t get_version() const = 0;
  virtual uint64_t get_database_id() const = 0;

  virtual uint64_t get_analyze_flag() const { return analyze_flag_; }

  virtual void set_no_sql() { is_no_sql_ = true; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  virtual bool is_no_sql() const { return is_no_sql_; }
  virtual void set_reads_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = true; is_modifies_sql_data_ = false; is_contains_sql_ = false; }
  virtual bool is_reads_sql_data() const { return is_reads_sql_data_; }
  virtual void set_modifies_sql_data() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = true; is_contains_sql_ = false; }
  virtual bool is_modifies_sql_data() const { return is_modifies_sql_data_; }
  virtual void set_contains_sql() { is_no_sql_ = false; is_reads_sql_data_ = false; is_modifies_sql_data_ = false; is_contains_sql_ = true; }
  virtual bool is_contains_sql() const { return is_contains_sql_; }

  virtual void set_wps() { is_wps_ = true; }
  virtual bool is_wps() const { return is_wps_; }
  virtual void set_rps() { is_rps_ = true; }
  virtual bool is_rps() const { return is_rps_; }
  virtual void set_has_sequence() { is_has_sequence_ = true; }
  virtual bool is_has_sequence() const { return is_has_sequence_; }
  virtual void set_has_out_param() { is_has_out_param_ = true; }
  virtual bool is_has_out_param() const { return is_has_out_param_; }
  virtual void set_external_state() { is_external_state_ = true; }
  virtual bool is_external_state() const { return is_external_state_; }

  ObPLSymbolDebugInfoTable &get_symbol_debuginfo_table()
  {
    return symbol_debuginfo_table_;
  }
  int generate_symbol_debuginfo();

  static int extract_assoc_index(sql::ObRawExpr &expr, common::ObIArray<sql::ObRawExpr *> &exprs);

  TO_STRING_KV(K_(body), K_(symbol_table), K_(symbol_debuginfo_table), K_(condition_table));

private:
  int check_simple_calc_expr(sql::ObRawExpr *&expr, bool &is_simple);

protected:
  UnitType type_;
  ObPLStmtBlock *body_;
  ObPLSEArray<sql::ObRawExpr*> obj_access_exprs_; //使用的ObjAccessRawExpr
  ObPLSEArray<sql::ObRawExpr*> exprs_; //使用的表达式，在AST里是ObRawExpr，在ObPLFunction里是ObISqlExpression
  ObBitSet<> simple_calc_bitset_; //可以使用LLVM进行计算的表达式下标
  ObPLSEArray<ObPLSqlStmt*> sql_stmts_;
  sql::ObRawExprFactory expr_factory_;
  ObPLSymbolTable symbol_table_;
  ObPLSymbolDebugInfoTable symbol_debuginfo_table_;
  ObPLUserTypeTable user_type_table_;
  ObPLLabelTable label_table_;
  ObPLConditionTable condition_table_;
  ObPLCursorTable cursor_table_;
  ObPLRoutineTable routine_table_;
  ObPLDependencyTable dependency_table_;
  ObPLCompileFlag compile_flag_;
  bool can_cached_;
  ObString priv_user_;
  union {
    uint64_t analyze_flag_;
    struct {
      uint64_t is_no_sql_ : 1;
      uint64_t is_reads_sql_data_ : 1;
      uint64_t is_modifies_sql_data_ : 1;
      uint64_t is_contains_sql_ : 1;
      uint64_t is_wps_ : 1;
      uint64_t is_rps_ : 1;
      uint64_t is_has_sequence_ : 1;
      uint64_t is_has_out_param_ : 1;
      uint64_t is_external_state_ : 1;
      uint64_t reserved_:54;
    };
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLCompileUnitAST);
};

class ObPLFunctionAST : public ObPLFunctionBase, public ObPLCompileUnitAST
{
public:
  ObPLFunctionAST(common::ObIAllocator &allocator)
    : ObPLFunctionBase(), ObPLCompileUnitAST(allocator, ROUTINE_TYPE),
      id_(OB_INVALID_ID),
      subprogram_id_(OB_INVALID_ID),
      subprogram_path_(allocator),
      is_all_sql_stmt_(true),
      is_pipelined_(false),
      has_return_(false) {}
  virtual ~ObPLFunctionAST() {}

  inline void set_db_name(const common::ObString &db_name) { db_name_ = db_name; }
  inline const common::ObString &get_package_name() const { return package_name_; }
  inline common::ObString &get_package_name() { return package_name_; }
  inline void set_package_name(const common::ObString &package_name) { package_name_ = package_name; }
  inline void set_name(const common::ObString &name) { name_ = name; }
  inline void set_id(uint64_t id) { id_ = id; }
  inline void set_subprogram_id(uint64_t id) { subprogram_id_ = id; }
  inline const ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_; }
  inline int set_subprogram_path(const ObIArray<int64_t> &path) { return append(subprogram_path_, path); }
  inline int add_subprogram_path(int64_t path) { return subprogram_path_.push_back(path); }
  inline bool get_is_all_sql_stmt() const { return is_all_sql_stmt_; }
  inline void set_is_all_sql_stmt(bool is_all_sql_stmt) { is_all_sql_stmt_ = is_all_sql_stmt; }
  inline bool has_parallel_affect_factor() const
  {
    return is_reads_sql_data() || is_modifies_sql_data() || is_wps() ||
           is_rps() || is_has_sequence() || is_external_state();
  }
  int add_argument(const common::ObString &name, const ObPLDataType &type,
                   const sql::ObRawExpr *expr = NULL,
                   const common::ObIArray<common::ObString> *type_info = NULL,
                   const bool read_only = false,
                   const bool is_udt_self_param = false);
  int get_argument(int64_t idx, common::ObString &name, ObPLDataType &type) const;

  virtual const common::ObString &get_db_name() const { return db_name_; }
  virtual const common::ObString &get_name() const { return name_; }
  virtual uint64_t get_id() const { return id_; }
  virtual uint64_t get_subprogram_id() const { return subprogram_id_; }
  virtual int64_t get_version() const { return OB_INVALID_VERSION; }
  virtual uint64_t get_database_id() const { return ObPLFunctionBase::get_database_id(); }

  void set_pipelined() { is_pipelined_ = true; }
  bool get_pipelined() const { return is_pipelined_; }

  inline void set_return() { has_return_ = true; }
  inline bool has_return() { return has_return_; }

  INHERIT_TO_STRING_KV("compile", ObPLCompileUnitAST, K(NULL));
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLFunctionAST);

  common::ObString db_name_;
  common::ObString package_name_;
  common::ObString name_;
  uint64_t id_;
  uint64_t subprogram_id_;
  ObPLSEArray<int64_t> subprogram_path_; // subprogram的寻址路径
  bool is_all_sql_stmt_;
  bool is_pipelined_;
  bool has_return_;
};

enum ObPLStmtType
{
  INVALID_PL_STMT = -1,
  PL_BLOCK,
  PL_VAR,
  PL_USER_TYPE,
  PL_USER_SUBTYPE,
  PL_ASSIGN,
  PL_IF,
  PL_LEAVE,
  PL_ITERATE,
  PL_WHILE,
  PL_FOR_LOOP,
  PL_CURSOR_FOR_LOOP,
  PL_FORALL,
  PL_REPEAT,
  PL_LOOP,
  PL_RETURN,
  PL_SQL,
  PL_EXECUTE,
  PL_EXTEND,
  PL_DELETE,
  PL_COND,
  PL_HANDLER,
  PL_SIGNAL,
  PL_CALL,
  PL_INNER_CALL,
  PL_CURSOR,
  PL_OPEN,
  PL_OPEN_FOR,
  PL_FETCH,
  PL_CLOSE,
  PL_NULL,
  PL_PIPE_ROW,
  PL_ROUTINE_DEF,
  PL_ROUTINE_DECL,
  PL_RAISE_APPLICATION_ERROR,
  PL_GOTO,
  PL_TRIM,
  PL_INTERFACE,
  PL_DO,
  PL_CASE,
  MAX_PL_STMT
};

class ObPLStmt
{
public:
  ObPLStmt() : type_(INVALID_PL_STMT), loc_(),
      level_(OB_INVALID_INDEX), label_cnt_(0), parent_(NULL) {}
  ObPLStmt(ObPLStmtType type) : type_(type), loc_(),
      level_(OB_INVALID_INDEX), label_cnt_(0), parent_(NULL) {}
  ObPLStmt(ObPLStmtBlock *parent) : type_(INVALID_PL_STMT), loc_(),
    level_(OB_INVALID_INDEX), label_cnt_(0), parent_(parent) {}
  virtual ~ObPLStmt() {}

  virtual int accept(ObPLStmtVisitor &visitor) const = 0;
  virtual int64_t get_child_size() const { return 0; }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const { UNUSED(i); return NULL; }

  inline ObPLStmtType get_type() const { return type_; }
  inline void set_type(ObPLStmtType type) { type_ = type; }
  inline void set_location(const SourceLocation &loc) { loc_ = loc; }
  inline void set_location(int32_t line, int32_t col) { loc_.line_ = line; loc_.col_ = col; }
  inline uint64_t get_location() const { return loc_.loc_; }
  inline uint32_t get_line() const { return static_cast<uint32_t>(loc_.line_); }
  inline uint32_t get_col() const { return static_cast<uint32_t>(loc_.col_); }
  inline int64_t get_stmt_id() const { return loc_.loc_; }
  inline int64_t get_level() const { return level_; }
  inline void set_level(int64_t level) { level_ = level; }
  inline int64_t get_label_cnt() const { return label_cnt_; }
  inline int64_t get_label_idx(int64_t idx) const { return (idx >= 0 && idx < label_cnt_) ? labels_[idx] : OB_INVALID_INDEX; }
  const common::ObString* get_label(int64_t idx) const;
  int set_label_idx(int64_t idx);
  inline bool has_label() const { return label_cnt_ > 0; }
  inline void set_block(const ObPLStmtBlock *block) { parent_ = block;}
  inline const ObPLStmtBlock *get_block() const { return parent_; }
  const ObPLBlockNS *get_namespace() const;
  const ObPLSymbolTable *get_symbol_table() const;
  const ObPLLabelTable *get_label_table() const;
  const ObPLVar *get_variable(int64_t i) const { return NULL == get_symbol_table() ? NULL : get_symbol_table()->get_symbol(i); }
  const ObPLConditionTable *get_condition_table() const;
  const ObPLCondition *get_conditions() const;
  inline const ObPLCondition *get_condition(int64_t i) const { return NULL == get_conditions() ? NULL : &(get_conditions()[i]); }
  const ObPLCursorTable *get_cursor_table() const;
  const common::ObIArray<ObPLCursor *> *get_cursors() const;
  inline const ObPLCursor *get_cursor(int64_t i) const
  {
    return OB_ISNULL(get_cursors()) || i < 0 || i >= get_cursors()->count()
      ? NULL : get_cursors()->at(i);
  }
  inline ObPLCursor *get_cursor(int64_t i)
  {
    return OB_ISNULL(get_cursors()) || i < 0 || i >= get_cursors()->count()
        ? NULL : const_cast<ObPLCursor *>(get_cursors()->at(i));
  }
  const common::ObIArray<sql::ObRawExpr*> *get_exprs() const;
  inline const sql::ObRawExpr *get_expr(int64_t i) const { return NULL == get_exprs() ? NULL : get_exprs()->at(i); }
  inline bool get_is_goto_dst() const
  {
    bool ret = false;
    if (OB_NOT_NULL(get_label_table()) && label_cnt_ > 0) {
      for (int64_t i = 0; !ret && i < label_cnt_; ++i) {
        ret = get_label_table()->get_is_goto_dst(labels_[i]);
      }
    }
    return ret;
  }
  inline const common::ObString* get_goto_label() const
  {
    const common::ObString *ret = NULL;
    if (OB_NOT_NULL(get_label_table()) && label_cnt_ > 0) {
      int64_t i = 0;
      for (; i < label_cnt_; ++i) {
        if (get_label_table()->get_is_goto_dst(labels_[i])) {
          ret = get_label_table()->get_label(labels_[i]);
          break;
        }
      }
    }
    return ret;
  }

  VIRTUAL_TO_STRING_KV(K_(type), K_(loc_.loc), K_(level), K_(label), KP_(parent));
protected:
  ObPLStmtType type_;
  SourceLocation loc_;
  int64_t level_;
  int64_t label_;
  int64_t label_cnt_;
  int64_t labels_[FUNC_MAX_LABELS]; // stmt may has multi lables, like <<a>><<b>>begin null; end;
  const ObPLStmtBlock *parent_;
};

class ObPLDeclareHandlerStmt;
class ObPLSymbolDebugInfoTable;
class ObPLStmtBlock : public ObPLStmt
{
public:
  ObPLStmtBlock(common::ObIAllocator &allocator)
    : ObPLStmt(PL_BLOCK),
      stmts_(allocator),
      forloop_cursor_stmts_(allocator),
      ns_(allocator, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
      eh_(NULL),
      in_notfound_scope_(false),
      in_warning_scope_(false),
      in_handler_scope_(false),
      is_contain_goto_stmt_(false),
      is_autonomous_block_(false) {}
  ObPLStmtBlock(common::ObIAllocator &allocator,
                ObPLBlockNS *pre_ns,
                ObPLSymbolTable *symbol_table,
                ObPLLabelTable *label_table,
                ObPLConditionTable *condition_table,
                ObPLCursorTable *cursor_table,
                ObPLRoutineTable *routine_table,
                common::ObIArray<sql::ObRawExpr*> *exprs,
                common::ObIArray<sql::ObRawExpr*> *obj_access_exprs,
                ObPLExternalNS *external_ns)
    : ObPLStmt(PL_BLOCK),
      stmts_(allocator),
      forloop_cursor_stmts_(allocator),
      ns_(allocator, pre_ns, symbol_table, label_table, condition_table, cursor_table, routine_table, exprs, obj_access_exprs, external_ns),
      eh_(NULL),
      in_notfound_scope_(false),
      in_warning_scope_(false),
      in_handler_scope_(false),
      is_contain_goto_stmt_(false),
      is_autonomous_block_(false) {}
  virtual ~ObPLStmtBlock() {
    reset();
  }

  void reset()
  {
    for (int64_t i = 0; i < stmts_.count(); ++i) {
      if (NULL != stmts_.at(i)) {
        stmts_.at(i)->~ObPLStmt();
      }
    }
    stmts_.reset();
  }

  int accept(ObPLStmtVisitor &visitor) const;
  virtual int64_t get_child_size() const { return stmts_.count(); }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const { return i < 0 || i >= stmts_.count() ? NULL : stmts_.at(i); }

  inline const common::ObIArray<ObPLStmt*> &get_stmts() const { return stmts_; }
  inline const ObPLBlockNS &get_namespace() const { return ns_; }
  inline ObPLBlockNS &get_namespace() { return ns_; }
  int add_stmt(ObPLStmt *stmt);
  inline ObPLStmt *get_eh() { return eh_; }
  inline void set_eh(ObPLStmt *eh) { eh_ = eh; }
  inline bool has_eh() const { return NULL != eh_; }
  inline void set_notfound() { in_notfound_scope_ = true; }
  inline bool in_notfound() const { return in_notfound_scope_; }
  inline void set_warning() { in_warning_scope_ = true; }
  inline bool in_warning() const { return in_warning_scope_; }
  inline void set_handler() { in_handler_scope_ = true; }
  inline bool in_handler() const { return in_handler_scope_; }
  inline void set_is_autonomous() { is_autonomous_block_ = true; }
  inline bool get_is_autonomous() const { return is_autonomous_block_; }
  inline void clear_aotonomous() { is_autonomous_block_ = false; }
  inline bool get_is_contain_goto_stmt() const { return is_contain_goto_stmt_; }
  inline void set_is_contain_goto_stmt(bool flag) { is_contain_goto_stmt_ = flag; }
  inline const ObPLSymbolTable *get_symbol_table() const { return ns_.get_symbol_table(); }
  inline const ObPLLabelTable *get_label_table() const { return ns_.get_label_table(); }
  inline const ObPLConditionTable *get_condition_table() const { return ns_.get_condition_table(); }
  inline const ObPLCursorTable *get_cursor_table() const { return ns_.get_cursor_table(); }
  inline const ObPLRoutineTable *get_routine_table() const { return ns_.get_routine_table(); }
  inline const common::ObIArray<sql::ObRawExpr*> *get_exprs() const { return ns_.get_exprs(); }
  inline const common::ObIArray<ObPLStmt*> &get_cursor_stmts() const
  { return forloop_cursor_stmts_; }
  bool is_contain_stmt(const ObPLStmt *stmt) const;

  int generate_symbol_debuginfo(ObPLSymbolDebugInfoTable &symbol_debuginfo_table) const;

  TO_STRING_KV(K_(type), K_(label), K_(stmts),
               K_(forloop_cursor_stmts), K_(is_contain_goto_stmt));

private:
  ObPLSEArray<ObPLStmt*> stmts_;
  ObPLSEArray<ObPLStmt*> forloop_cursor_stmts_;
  ObPLBlockNS ns_;
  ObPLStmt *eh_;
  bool in_notfound_scope_;
  bool in_warning_scope_;
  bool in_handler_scope_;
  bool is_contain_goto_stmt_;
  bool is_autonomous_block_; // 是否是标记为autonomous的block
};

class ObPLDeclareVarStmt : public ObPLStmt
{
public:
  ObPLDeclareVarStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_VAR),
      idx_(allocator),
      default_(OB_INVALID_INDEX) {}
  virtual ~ObPLDeclareVarStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline const common::ObIArray<int64_t> &get_index() const { return idx_; }
  inline int64_t get_index(int64_t i) const { return idx_.at(i); }
  inline int add_index(int64_t var) { return idx_.push_back(var); }
  inline int64_t get_default() const { return default_; }
  inline const sql::ObRawExpr *get_default_expr() const { return get_expr(default_); }
  inline void set_default(int64_t idx) { default_ = idx; }
  inline const ObPLVar *get_var(int64_t i) const { return get_variable(idx_.at(i)); }

  TO_STRING_KV(K_(type), K_(label), K_(idx), K(default_));

private:
  ObPLSEArray<int64_t> idx_;
  int64_t default_;
};

class ObPLDeclareUserTypeStmt : public ObPLStmt
{
public:
  ObPLDeclareUserTypeStmt()
    : ObPLStmt(PL_USER_TYPE),
      user_type_(NULL) {}

  int accept(ObPLStmtVisitor &visitor) const;
  void set_user_type(ObUserDefinedType *user_type) { user_type_ = user_type; }
  ObUserDefinedType *get_user_type() { return user_type_; }
  const ObUserDefinedType *get_user_type() const { return user_type_; }
private:
  ObUserDefinedType *user_type_;
};

class ObPLAssignStmt : public ObPLStmt
{
public:
  ObPLAssignStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_ASSIGN), into_(allocator), value_(allocator) {}
  virtual ~ObPLAssignStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline const common::ObIArray<int64_t> &get_into() const { return into_;}
  inline int64_t get_into_index(int64_t i) const { return into_.at(i);}
  inline const sql::ObRawExpr *get_into_expr(int64_t i) const { return get_expr(into_.at(i));}
  inline int set_into(ObPLSEArray<int64_t> &idxs) { return append(into_, idxs); }
  inline int add_into(int64_t idx) { return into_.push_back(idx); }
  inline int64_t get_into_count() { return into_.count(); }
  inline const common::ObIArray<int64_t> &get_value() const { return value_;}
  inline int64_t get_value_index(int64_t i) const { return value_.at(i);}
  inline const sql::ObRawExpr *get_value_expr(int64_t i) const { return get_expr(value_.at(i)); }
  inline int set_value(ObPLSEArray<int64_t> &idxs) { return append(value_, idxs); }
  inline int add_value(int64_t idx) { return value_.push_back(idx); }
  inline int64_t get_value_count() { return value_.count(); }
private:
  ObPLSEArray<int64_t> into_;
  ObPLSEArray<int64_t> value_;
};

class ObPLIfStmt : public ObPLStmt
{
public:
  ObPLIfStmt() : ObPLStmt(PL_IF), cond_(OB_INVALID_INDEX), then_(NULL), else_(NULL) {}
  virtual ~ObPLIfStmt() {
    if (NULL != then_) {
      then_->~ObPLStmtBlock();
      then_ = NULL;
    }
    if (NULL != else_) {
      else_->~ObPLStmtBlock();
      else_ = NULL;
    }
  }

  int accept(ObPLStmtVisitor &visitor) const;
  virtual int64_t get_child_size() const { return NULL == else_ ? 1 : 2; }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const { return i < 0 || i >= 2 ? NULL : (0 == i ? then_ : else_); }

  inline int64_t get_cond() const { return cond_; }
  inline const sql::ObRawExpr *get_cond_expr() const { return get_expr(cond_); }
  inline void set_cond(int64_t idx) { cond_ = idx; }
  inline const ObPLStmtBlock *get_then() const { return then_; }
  inline void set_then(ObPLStmtBlock *stmt) { then_ = stmt; }
  inline const ObPLStmtBlock *get_else() const { return else_; }
  inline void set_else(ObPLStmtBlock *stmt) { else_ = stmt; }

  TO_STRING_KV(K_(type), K_(label), K_(cond), K_(then), K(else_));

private:
  int64_t cond_;
  ObPLStmtBlock *then_;
  ObPLStmtBlock *else_;
};

class ObPLCaseStmt : public ObPLStmt {
public:
  ObPLCaseStmt(common::ObIAllocator &allocator)
      : ObPLStmt(PL_CASE),
        case_expr_idx_(OB_INVALID_INDEX), case_var_idx_(OB_INVALID_INDEX),
        when_(allocator), else_(nullptr) {}
  virtual ~ObPLCaseStmt() {
    if (NULL != else_) {
      else_->~ObPLStmtBlock();
      else_ = NULL;
    }
    int64_t when_count = when_.count();
    for (int64_t i = 0; i < when_count; ++i) {
      ObPLStmt *ret = when_[i].body_;
      if (OB_NOT_NULL(ret)) {
        ret->~ObPLStmt();
      }
    }
  }

  virtual int64_t get_child_size() const override { return when_.count() + 1; }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const override {
    ObPLStmt *ret = nullptr;
    int64_t when_count = when_.count();
    if (OB_LIKELY(0 <= i && i <= when_count)) {
      if (OB_LIKELY(i < when_count)) {
        ret = when_[i].body_;
      } else {
        ret = else_;
      }
    }
    return ret;
  }

  virtual int accept(ObPLStmtVisitor &visitor) const override;

  struct WhenClause {
    int64_t expr_;
    ObPLStmtBlock *body_;

    TO_STRING_KV(K_(expr), K_(body));
  };
  using WhenClauses = ObPLSEArray<WhenClause>;

  void set_case_expr(int64_t case_expr_idx) { case_expr_idx_ = case_expr_idx; }
  int64_t get_case_expr() const { return case_expr_idx_; }
  void set_case_var(int64_t case_var_idx) { case_var_idx_ = case_var_idx; }
  int64_t get_case_var() const { return case_var_idx_; }
  int add_when_clause(int64_t expr, ObPLStmtBlock *body) {
    return when_.push_back(WhenClause{expr, body});
  }
  const WhenClauses &get_when_clauses() const { return when_; }
  void set_else_clause(ObPLStmtBlock *stmts) { else_ = stmts; }
  const ObPLStmtBlock *get_else_clause() const { return else_; }

private:
  int64_t case_expr_idx_;
  int64_t case_var_idx_;
  WhenClauses when_;
  ObPLStmtBlock *else_;
};

typedef common::ObSEArray<ObObjAccessIdx, 4> ObObjAccessIndexs;
typedef common::ObSEArray<ObObjAccessIndexs, 4> ObObjAccessArray;

class ObPLInto
{
public:
  ObPLInto(common::ObIAllocator &allocator)
    : into_(allocator),
      not_null_flags_(allocator),
      pl_integer_ranges_(allocator),
      data_type_(allocator),
      into_data_type_(allocator),
      bulk_(false),
      is_type_record_(false) {}
  virtual ~ObPLInto() {}

  inline const common::ObIArray<int64_t> &get_into() const { return into_; }
  inline common::ObIArray<int64_t> &get_into() { return into_; }
  inline int64_t get_into(int64_t i) const { return into_.at(i); }
  inline int set_into(const common::ObIArray<int64_t> &idxs) { return append(into_, idxs); }
  int set_into(const common::ObIArray<int64_t> &idxs, ObPLBlockNS &ns, const common::ObIArray<sql::ObRawExpr*> &exprs);
  inline int add_into(int64_t idx) { return into_.push_back(idx); }
  int add_into(int64_t idx, ObPLBlockNS &ns, const sql::ObRawExpr &expr);
  int generate_into_variable_info(ObPLBlockNS &ns, const ObRawExpr &expr);
  inline const common::ObIArray<ObDataType> &get_data_type() const { return data_type_; }
  inline common::ObIArray<ObDataType> &get_data_type() { return data_type_; }
  inline const ObDataType &get_data_type(int64_t i) const { return data_type_.at(i); }
  inline int set_data_type(const common::ObIArray<ObDataType> &types) { return append(data_type_, types); }
  inline int add_data_type(ObDataType &type) { return data_type_.push_back(type); }
  inline const common::ObIArray<ObPLDataType> &get_into_data_type() const { return into_data_type_; }
  inline common::ObIArray<ObPLDataType> &get_into_data_type() { return into_data_type_; }
  inline const ObPLDataType &get_into_data_type(int64_t i) const { return into_data_type_.at(i); }
  inline bool is_type_record() const { return is_type_record_; }
  inline bool is_bulk() const { return bulk_; }
  inline void set_bulk() { bulk_ = true; }
  int check_into(ObPLFunctionAST &func, ObPLBlockNS &ns, bool is_bulk);
  inline const common::ObIArray<bool> &get_not_null_flags() const { return not_null_flags_; }
  inline const bool &get_not_null_flag(int64_t i) const { return not_null_flags_.at(i); }
  inline const common::ObIArray<int64_t> &get_pl_integer_ranges() const
  {
    return pl_integer_ranges_;
  }
  inline int64_t get_pl_integer_range(int64_t i) const { return pl_integer_ranges_.at(i); }
  int calc_type_constraint(const sql::ObRawExpr &expr,
                           const ObPLBlockNS &ns,
                           bool &flag,
                           ObPLIntegerRange &pl_integer_range) const;

  TO_STRING_KV(K_(into), K_(not_null_flags), K_(pl_integer_ranges), K_(data_type), K_(bulk));

protected:
  ObPLSEArray<int64_t> into_;
  ObPLSEArray<bool> not_null_flags_;
  ObPLSEArray<int64_t> pl_integer_ranges_;
  ObPLSEArray<ObDataType> data_type_;
  ObPLSEArray<ObPLDataType> into_data_type_;
  bool bulk_;
  bool is_type_record_; // 表示into后面是否只有一个type定义的record类型(非object定义)
};

class ObPLLoopControl : public ObPLStmt
{
public:
  ObPLLoopControl(ObPLStmtType type) : ObPLStmt(type), next_label_(), cond_(OB_INVALID_INDEX) {}
  virtual ~ObPLLoopControl() {}

  inline const common::ObString &get_next_label() const { return next_label_; }
  inline void set_next_label(const common::ObString &label) { next_label_ = label; }
  inline int64_t get_cond() const { return cond_; }
  inline const sql::ObRawExpr *get_cond_expr() const { return get_expr(cond_); }
  inline void set_cond(int64_t idx) { cond_ = idx; }

private:
  common::ObString next_label_;
  int64_t cond_;
};

class ObPLLeaveStmt : public ObPLLoopControl
{
public:
  ObPLLeaveStmt() : ObPLLoopControl(PL_LEAVE) {}
  virtual ~ObPLLeaveStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

private:

};

class ObPLIterateStmt : public ObPLLoopControl
{
public:
  ObPLIterateStmt() : ObPLLoopControl(PL_ITERATE) {}
  virtual ~ObPLIterateStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

private:

};

class ObPLLoop : public ObPLStmt
{
public:
  ObPLLoop(ObPLStmtType type) : ObPLStmt(type), body_(NULL) {}
  virtual ~ObPLLoop() {
    if (NULL != body_) {
      body_->~ObPLStmtBlock();
      body_ = NULL;
    }
  }

  virtual int64_t get_child_size() const { return 1; }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const { return 0 == i ? body_ : NULL; }

  inline const ObPLStmtBlock *get_body() const { return body_; }
  inline void set_body(ObPLStmtBlock *body) { body_ = body; }
  inline bool is_contain_goto_stmt() const { return body_->get_is_contain_goto_stmt(); }

private:
  ObPLStmtBlock *body_;
};

class ObPLCondLoop : public ObPLLoop
{
public:
  ObPLCondLoop(ObPLStmtType type) : ObPLLoop(type), cond_(OB_INVALID_INDEX) {}
  virtual ~ObPLCondLoop() {}

  inline int64_t get_cond() const { return cond_; }
  inline const sql::ObRawExpr *get_cond_expr() const { return get_expr(cond_); }
  inline void set_cond(int64_t idx) { cond_ = idx; }

private:
  int64_t cond_;
};

class ObPLWhileStmt : public ObPLCondLoop
{
public:
  ObPLWhileStmt() : ObPLCondLoop(PL_WHILE) {}
  virtual ~ObPLWhileStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

private:

};

class ObPLForLoopStmt : public ObPLLoop
{
public:
  enum BoundType
  {
    INVALID,
    NORMAL,
    VALUES,
    INDICES,
    INDICES_WITH_BETWEEN,
  };

  struct Bound
  {
  public:
    Bound()
      : type_(NORMAL),
        first_idx_(OB_INVALID_INDEX),
        last_idx_(OB_INVALID_INDEX),
        next_idx_(OB_INVALID_INDEX),
        exists_idx_(OB_INVALID_INDEX),
        value_idx_(OB_INVALID_INDEX) {}

    BoundType type_; // 标识当前Loop的范围计算方式
    int64_t first_idx_;
    int64_t last_idx_;
    int64_t next_idx_;
    int64_t exists_idx_;
    int64_t value_idx_;
  };

public:
  ObPLForLoopStmt()
    : ObPLLoop(PL_FOR_LOOP),
      reverse_(false),
      is_forall_(false),
      ident_idx_(OB_INVALID_INDEX),
      bound_() {}
  ObPLForLoopStmt(ObPLStmtType type)
    : ObPLLoop(type),
      reverse_(false),
      is_forall_(false),
      ident_idx_(OB_INVALID_INDEX),
      bound_() {}

  virtual ~ObPLForLoopStmt() {}

  inline void set_reverse(bool reverse) { reverse_ = reverse; }
  inline void set_is_forall(bool is_forall) { is_forall_ = is_forall; }
  inline void set_ident(int64_t idx) { ident_idx_ = idx; }

  inline void set_lower(int64_t idx) { bound_.first_idx_ = idx; }
  inline void set_upper(int64_t idx) { bound_.last_idx_ = idx; }
  inline void set_first(int64_t idx) { bound_.first_idx_ = idx; }
  inline void set_last(int64_t idx) { bound_.last_idx_ = idx; }
  inline void set_next(int64_t idx) { bound_.next_idx_ = idx; }
  inline void set_exists(int64_t idx) { bound_.exists_idx_ = idx; }
  inline void set_value(int64_t idx) { bound_.value_idx_ = idx; }

  inline const sql::ObRawExpr *get_lower_expr() const { return get_expr(bound_.first_idx_); }
  inline const sql::ObRawExpr *get_upper_expr() const { return get_expr(bound_.last_idx_); }
  inline const sql::ObRawExpr *get_first_expr() const { return get_expr(bound_.first_idx_); }
  inline const sql::ObRawExpr *get_last_expr() const { return get_expr(bound_.last_idx_); }
  inline const sql::ObRawExpr *get_next_expr() const { return get_expr(bound_.next_idx_); }
  inline const sql::ObRawExpr *get_exists_expr() const { return get_expr(bound_.exists_idx_); }
  inline const sql::ObRawExpr *get_value_expr() const { return get_expr(bound_.value_idx_); }

  inline void set_bound_type(BoundType type) { bound_.type_ = type; }

  inline bool is_normal_bound() const { return BoundType::NORMAL == bound_.type_; }
  inline bool is_indices_bound() const { return BoundType::INDICES == bound_.type_; }
  inline bool is_values_bound() const { return BoundType::VALUES == bound_.type_; }
  inline bool is_indices_with_between_bound() const
  {
    return BoundType::INDICES_WITH_BETWEEN == bound_.type_;
  }

  inline bool get_reverse() const { return reverse_; }
  inline bool get_is_forall() const { return is_forall_; }
  inline int64_t get_lower() const { return bound_.first_idx_; }
  inline int64_t get_upper() const { return bound_.last_idx_; }
  inline int64_t get_ident() const { return ident_idx_; }
  inline int64_t get_first() const { return bound_.first_idx_; }
  inline int64_t get_last() const { return bound_.last_idx_; }
  inline int64_t get_next() const { return bound_.next_idx_; }
  inline int64_t get_exists() const { return bound_.exists_idx_; }
  inline int64_t get_value() const { return bound_.value_idx_; }

  inline const ObPLVar *get_index_var() const { return get_variable(ident_idx_); }

  int accept(ObPLStmtVisitor &visitor) const;

private:
  bool reverse_;
  bool is_forall_;
  int64_t ident_idx_; // INDEX 在符号表里面的下标
  Bound bound_;
};

class ObPLCursorForLoopStmt : public ObPLLoop, public ObPLInto
{
public:
  ObPLCursorForLoopStmt(common::ObIAllocator &allocator)
    : ObPLLoop(PL_CURSOR_FOR_LOOP),
      ObPLInto(allocator),
      index_idx_(OB_INVALID_INDEX),
      cursor_idx_(OB_INVALID_INDEX),
      params_(allocator),
      need_declare_(false),
      user_type_(NULL) {}

  virtual ~ObPLCursorForLoopStmt() {}

  inline void set_index_index(int64_t idx)
  {
    index_idx_ = idx;
  }
  inline int64_t get_index_index() const
  {
    return index_idx_;
  }
  inline const ObPLVar *get_index_var() const
  {
    return OB_INVALID_INDEX == index_idx_
            ? NULL : get_variable(index_idx_);
  }
  inline int64_t get_cursor_index() const
  {
    return cursor_idx_;
  }
  inline void set_cursor_index(int64_t idx)
  {
    cursor_idx_ = idx;
  }

  inline const ObPLCursor *get_cursor() const
  {
    return ObPLStmt::get_cursor(cursor_idx_);
  }
  inline uint64_t get_package_id() const
  {
    return NULL == get_cursor()
            ? OB_INVALID_ID : ObPLStmt::get_cursor(cursor_idx_)->get_package_id();
  }
  inline uint64_t get_routine_id() const
  {
    return NULL == get_cursor()
            ? common::OB_INVALID_ID : ObPLStmt::get_cursor(cursor_idx_)->get_routine_id();
  }
  inline int get_var(const ObPLVar *&var) const
  {
    //只有local cursor才能在本地符号表找到对应变量
    return get_namespace()->get_cursor_var(get_package_id(),
                                           get_routine_id(),
                                           get_index(), var);
  }
  inline int64_t get_index() const
  {
    return NULL == get_cursor()
            ? OB_INVALID_INDEX : ObPLStmt::get_cursor(cursor_idx_)->get_index();
  }
  inline const ObPLVar *get_cursor_var() const
  {
    return OB_INVALID_INDEX == get_index()
            ? NULL : get_variable(get_index());
  }
  inline const ObIArray<int64_t> &get_params() const { return params_; }
  inline int set_params(const ObIArray<int64_t> &params) { return params_.assign(params); }
  inline int add_param(int64_t param) { return params_.push_back(param); }
  inline const ObUserDefinedType* get_user_type() const
  {
    return user_type_;
  }
  inline void set_user_type(const ObUserDefinedType *user_type)
  {
    user_type_ = user_type;
  }

  inline void set_need_declare(bool need) { need_declare_ = need; }
  inline bool get_need_declare() const { return need_declare_; }

  int accept(ObPLStmtVisitor &visitor) const;

  TO_STRING_KV(K_(index_idx), K_(cursor_idx));

private:
  int64_t index_idx_;  // INDEX 在符号表中的下标
  int64_t cursor_idx_; // CURSOR 在Cursor表中的下标
  ObPLSEArray<int64_t> params_; //cursor的实参
  bool need_declare_;  // 是否是SQL语句, 需要在codegen时声明
  const ObUserDefinedType *user_type_; // CURSOR返回值类型
};

class ObPLSqlStmt;
class ObPLForAllStmt : public ObPLForLoopStmt
{
public:
  ObPLForAllStmt()
    : ObPLForLoopStmt(PL_FORALL),
      save_exception_(false),
      binding_array_(false),
      sql_stmt_(NULL),
      tab_to_subtab_()
  {
    set_is_forall(true);
  }

  virtual ~ObPLForAllStmt() { tab_to_subtab_.destroy(); }

  inline void set_save_exception(bool save_exception) { save_exception_ = save_exception; }
  inline bool get_save_exception() const { return save_exception_; }

  inline void set_binding_array(bool binding_array) { binding_array_ = binding_array; }
  inline bool get_binding_array() { return binding_array_; }

  inline void set_sql_stmt(const ObPLSqlStmt *sql_stmt) { sql_stmt_ = sql_stmt; }
  inline const ObPLSqlStmt* get_sql_stmt() const { return sql_stmt_; }

  inline const hash::ObHashMap<int64_t, int64_t>& get_tab_to_subtab_map() const { return tab_to_subtab_; }
  inline hash::ObHashMap<int64_t, int64_t>& get_tab_to_subtab_map() { return tab_to_subtab_; }

  inline int create_tab_to_subtab()
  {
    return tab_to_subtab_.create(16, ObModIds::OB_PL_TEMP, ObModIds::OB_HASH_NODE, MTL_ID());
  }

  int accept(ObPLStmtVisitor &visitor) const;

private:
  bool save_exception_;
  bool binding_array_;
  const ObPLSqlStmt *sql_stmt_;
  hash::ObHashMap<int64_t, int64_t> tab_to_subtab_;
};

class ObPLRepeatStmt : public ObPLCondLoop
{
public:
  ObPLRepeatStmt() : ObPLCondLoop(PL_REPEAT) {}
  virtual ~ObPLRepeatStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

private:

};

class ObPLLoopStmt : public ObPLLoop
{
public:
  ObPLLoopStmt() : ObPLLoop(PL_LOOP) {}
  virtual ~ObPLLoopStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

private:

};

class ObPLReturnStmt : public ObPLStmt
{
public:
  ObPLReturnStmt() : ObPLStmt(PL_RETURN), ret_(OB_INVALID_INDEX), ref_cursor_type_(false) {}
  virtual ~ObPLReturnStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_ret() const { return ret_;}
  inline const sql::ObRawExpr *get_ret_expr() const { return get_expr(ret_);}
  inline void set_ret(int64_t idx) { ret_ = idx; }
  inline void set_is_ref_cursor_type(bool is_ref_cursor_type) { ref_cursor_type_ = is_ref_cursor_type; }
  inline bool is_return_ref_cursor_type() const { return ref_cursor_type_; }

  TO_STRING_KV(K_(type), K_(label), K_(ret), K_(ref_cursor_type));

private:
  int64_t ret_;
  bool ref_cursor_type_;
};

class ObPLSqlStmt : public ObPLStmt, public ObPLSql, public ObPLInto
{
public:
  ObPLSqlStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_SQL), ObPLSql(allocator), ObPLInto(allocator) {}
  virtual ~ObPLSqlStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  TO_STRING_KV(K_(type), K_(label), K_(sql), K_(params), K_(ps_sql), K_(stmt_type), K_(into), K_(data_type), K_(bulk));
private:

};

struct InOutParam
{
  InOutParam(int64_t param = OB_INVALID_INDEX,
             ObPLRoutineParamMode mode = PL_PARAM_INVALID,
             int64_t out_idx = OB_INVALID_INDEX) : param_(param), mode_(mode), out_idx_(out_idx) {}
  virtual ~InOutParam() {}

  bool is_out() const { return PL_PARAM_OUT == mode_ || PL_PARAM_INOUT == mode_; }
  bool is_pure_out() const { return PL_PARAM_OUT == mode_; }

  int64_t param_;
  ObPLRoutineParamMode mode_;
  int64_t out_idx_; //OB_INVALID_INDEX：非输出参数，其他：输出（包括OUT和INOUT）参数在全局符号表里的下标

  TO_STRING_KV(K_(param), K_(mode), K_(out_idx));
};

class ObPLUsing
{
public:
  ObPLUsing(common::ObIAllocator &allocator) : using_(allocator) {}
  virtual ~ObPLUsing() {}

  inline common::ObIArray<InOutParam> &get_using() { return using_; }
  inline const common::ObIArray<InOutParam> &get_using() const { return using_; }
  int64_t get_using_index(int64_t i) const { return using_.at(i).param_; }
  inline int add_using(int64_t expr,
                       ObPLRoutineParamMode mode = PL_PARAM_INVALID,
                       int64_t idx = OB_INVALID_INDEX)
  {
    return using_.push_back(InOutParam(expr, mode, idx));
  }
  inline bool is_out(int64_t i) const
  {
    return PL_PARAM_OUT == using_.at(i).mode_ || PL_PARAM_INOUT == using_.at(i).mode_;
  }
  inline bool is_pure_out(int64_t i) const { return PL_PARAM_OUT == using_.at(i).mode_; }
  inline int64_t get_out_index(int64_t i) const { return using_.at(i).out_idx_; }
  inline bool has_out() const
  {
    for (int64_t i = 0; i < using_.count(); ++i) {
      if (using_.at(i).is_out()) {
        return true;
      }
    }
    return false;
  }

  TO_STRING_KV(K_(using));

protected:
  ObPLSEArray<InOutParam> using_;
};

class ObPLExecuteStmt : public ObPLStmt, public ObPLInto, public ObPLUsing
{
public:
  ObPLExecuteStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_EXECUTE),
      ObPLInto(allocator),
      ObPLUsing(allocator),
      sql_(OB_INVALID_INDEX), is_returning_(false) {}
  virtual ~ObPLExecuteStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_sql() const { return sql_; }
  inline const sql::ObRawExpr *get_sql_expr() const { return get_expr(sql_); }
  inline void set_sql(int64_t idx) { sql_ = idx; }
  inline void set_is_returning(bool is_returning) { is_returning_ = is_returning; }
  inline bool get_is_returning() const { return is_returning_; }
  const sql::ObRawExpr *get_using_expr(int64_t i) const { return get_expr(using_.at(i).param_); }
  TO_STRING_KV(K_(type), K_(label), K_(sql), K_(into), K_(bulk), K_(using), K_(is_returning));
private:
  int64_t sql_;
  bool is_returning_;
};

class ObPLExtendStmt : public ObPLStmt
{
public:
  ObPLExtendStmt()
    : ObPLStmt(PL_EXECUTE), extend_(OB_INVALID_INDEX), n_(OB_INVALID_INDEX), i_(OB_INVALID_INDEX) {}
  virtual ~ObPLExtendStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_extend() const { return extend_; }
  inline const sql::ObRawExpr *get_extend_expr() const { return get_expr(extend_); }
  inline void set_extend(int64_t expr) { extend_ = expr; }
  inline int64_t get_n() const { return n_; }
  inline int64_t get_i() const { return i_; }
  inline const sql::ObRawExpr *get_n_expr() const { return get_expr(n_); }
  inline const sql::ObRawExpr *get_i_expr() const { return get_expr(i_); }
  inline void set_ni(int64_t n, int64_t i) { n_ = n; i_ = i; }
  TO_STRING_KV(K_(type), K_(label), K_(extend), K_(n), K_(i));
private:
  int64_t extend_;
  int64_t n_;
  int64_t i_;
};

class ObPLDeleteStmt : public ObPLStmt
{
public:
  ObPLDeleteStmt() :
    ObPLStmt(PL_DELETE),
    delete_(OB_INVALID_INDEX),
    m_(OB_INVALID_INDEX),
    n_(OB_INVALID_INDEX) {}
  virtual ~ObPLDeleteStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_delete() const { return delete_; }
  inline const sql::ObRawExpr *get_delete_expr() const { return get_expr(delete_); }
  inline void set_delete(int64_t expr) { delete_ = expr; }
  inline int64_t get_n() const { return n_; }
  inline int64_t get_m() const { return m_; }
  inline const sql::ObRawExpr *get_n_expr() const { return get_expr(n_); }
  inline const sql::ObRawExpr *get_m_expr() const { return get_expr(m_); }
  inline void set_mn(int64_t m, int64_t n) { m_ = m; n_ = n; }
  TO_STRING_KV(K_(type), K_(label), K_(delete), K_(m), K_(n));
private:
  int64_t delete_;
  int64_t m_;
  int64_t n_;
};

class ObPLTrimStmt : public ObPLStmt
{
public:
  ObPLTrimStmt() :
  ObPLStmt(PL_TRIM),
  trim_(OB_INVALID_INDEX),
  n_(OB_INVALID_INDEX) {}

  virtual ~ObPLTrimStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;
  inline int64_t get_trim() const { return trim_; }
  inline void set_trim(int64_t trim) { trim_ = trim; }
  inline const sql::ObRawExpr *get_trim_expr() const { return get_expr(trim_); }
  inline int64_t get_n() const { return n_; }
  inline void set_n(int64_t n) { n_ = n; }
  inline const sql::ObRawExpr *get_n_expr() const { return get_expr(n_); }
  TO_STRING_KV(K_(type), K_(label), K_(trim), K_(n));

private:
  int64_t trim_;
  int64_t n_;
};

class ObPLDeclareCondStmt : public ObPLStmt
{
public:
  ObPLDeclareCondStmt() : ObPLStmt(PL_COND) {}
  virtual ~ObPLDeclareCondStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  TO_STRING_KV(K_(type), K_(label));

private:

};

class ObPLDeclareHandlerStmt : public ObPLStmt
{
public:
  class DeclareHandler
  {
  public:
    enum Action
    {
      INVALID = -1,
      EXIT = 0,
      CONTINUE = 1,
      UNDO,
    };

    class HandlerDesc
    {
    public:
      HandlerDesc(common::ObIAllocator &allocator)
        : action_(INVALID), conditions_(allocator), body_(NULL) {}
      virtual ~HandlerDesc() {}

      inline Action get_action() const { return action_; }
      inline void set_action(Action action) { action_ = action; }
      inline const common::ObIArray<ObPLConditionValue> &get_conditions() const { return conditions_; }
      inline int set_conditions(common::ObIArray<ObPLConditionValue> &values) { return append(conditions_, values); }
      inline const ObPLConditionValue &get_condition(int64_t i) const { return conditions_.at(i); }
      inline int add_condition(ObPLConditionValue &value) { return conditions_.push_back(value); }
      inline const ObPLStmtBlock *get_body() const { return body_; }
      inline void set_body(ObPLStmtBlock *body) { body_ = body; }
      inline bool is_exit() const { return EXIT == action_; }
      inline bool is_continue() const { return CONTINUE == action_; }
      TO_STRING_KV(K_(action), K_(conditions), K_(body));

    private:
      Action action_;
      ObPLSEArray<ObPLConditionValue> conditions_;
      ObPLStmtBlock *body_;
    };

    DeclareHandler() : level_(OB_INVALID_INDEX), desc_(NULL) {}
    DeclareHandler(int64_t level, HandlerDesc *desc) : level_(level), desc_(desc) {}
    virtual ~DeclareHandler() {}

    inline int64_t get_level() const { return level_; }
    inline void set_level(int64_t level) { level_ = level; }
    inline HandlerDesc *get_desc() const { return desc_; }
    inline void set_desc(HandlerDesc *desc) { desc_ = desc; }
    inline bool is_original() const { return OB_INVALID_INDEX == level_; }

    static int compare_condition(ObPLConditionType type1, int64_t level1, ObPLConditionType type2, int64_t level2);

    TO_STRING_KV(K_(level), K_(desc));

  private:
    int64_t level_; //OB_INVALID_INDEX代表是本级原生Handler，否则代表是从上层降下来的Handler
    HandlerDesc *desc_;
  };

public:
  ObPLDeclareHandlerStmt(common::ObIAllocator &allocator) : ObPLStmt(PL_HANDLER), handlers_(allocator) {}
  virtual ~ObPLDeclareHandlerStmt()
  {
    for (int64_t i = 0; i < get_child_size(); ++i) {
      if (NULL != get_child_stmt(i)) {
        (const_cast<ObPLStmt *>(get_child_stmt(i)))->~ObPLStmt();
      }
    }
    handlers_.reset();
  }

  int accept(ObPLStmtVisitor &visitor) const;
  virtual int64_t get_child_size() const { return handlers_.count(); }
  virtual const ObPLStmt *get_child_stmt(int64_t i) const { return i < 0 || i >= handlers_.count() ? NULL : (NULL == handlers_.at(i).get_desc() ? NULL : handlers_.at(i).get_desc()->get_body()); }

  inline const common::ObIArray<DeclareHandler> &get_handlers() const { return handlers_; }
  inline const DeclareHandler &get_handler(int64_t i) const { return handlers_.at(i); }
  int add_handler(DeclareHandler &handler);

  TO_STRING_KV(K_(type), K_(label), K_(handlers));

private:
  ObPLSEArray<DeclareHandler> handlers_;
};

class ObPLSignalStmt : public ObPLStmt
{
  struct InfoItem
  {
    enum ItemName
    {
      INVALID_ITEM = -1,
      CLASS_ORIGIN = 0,
      SUBCLASS_ORIGIN,
      MESSAGE_TEXT,
      MYSQL_ERRNO,
      CONSTRAINT_CATALOG,
      CONSTRAINT_SCHEMA,
      CONSTRAINT_NAME,
      CATALOG_NAME,
      SCHEMA_NAME,
      TABLE_NAME,
      COLUMN_NAME,
      CURSOR_NAME,
    };

    ItemName type_;
    int64_t code_;
    common::ObString name_;

    TO_STRING_KV(K_(type), K_(code), K_(name));
  };

public:
  ObPLSignalStmt()
    : ObPLStmt(PL_SIGNAL),
      value_(),
      item_to_expr_idx_(),
      ob_error_code_(0),
      is_signal_null_(false),
      is_resignal_stmt_(false) {}
  virtual ~ObPLSignalStmt() { item_to_expr_idx_.destroy(); }

  int accept(ObPLStmtVisitor &visitor) const;

  inline const ObPLConditionValue &get_value() const { return value_; }
  inline void set_value(const ObPLConditionValue &value) { value_ = value; }
  inline ObPLConditionType get_cond_type() const { return value_.type_; }
  inline void set_cond_type(ObPLConditionType type) { value_.type_ = type; }
  inline int get_ob_error_code() const { return ob_error_code_; }
  inline void set_ob_error_code(int ob_error_code) { ob_error_code_ = ob_error_code; }
  inline int64_t get_error_code() const { return value_.error_code_; }
  inline void set_error_code(int64_t code) { value_.error_code_ = code; }
  inline const char *get_sql_state() const { return value_.sql_state_; }
  inline void set_sql_state(const char *state) { value_.sql_state_ = state; }
  inline int64_t get_str_len() const { return value_.str_len_; }
  inline void set_str_len(int64_t len) { value_.str_len_ = len; }
  inline void set_is_signal_null() { is_signal_null_ = true; }
  inline bool is_signal_null() const { return is_signal_null_; }
  inline int create_item_to_expr_idx(int64_t capacity) { 
    return item_to_expr_idx_.create(capacity, ObModIds::OB_PL_TEMP);
  }
  inline const int64_t *get_expr_idx(const int64_t item) const { return item_to_expr_idx_.get(item); }
  inline const hash::ObHashMap<int64_t, int64_t>& get_item_to_expr_idx() const 
  { return item_to_expr_idx_; }
  inline hash::ObHashMap<int64_t, int64_t>& get_item_to_expr_idx() { return item_to_expr_idx_; }

  inline void set_is_resignal_stmt() { is_resignal_stmt_ = true; }
  inline bool is_resignal_stmt() const { return is_resignal_stmt_; }

  TO_STRING_KV(K_(type), K_(label), K_(value));

private:
  ObPLConditionValue value_;
  hash::ObHashMap<int64_t, int64_t> item_to_expr_idx_;
  int ob_error_code_;
  bool is_signal_null_; // Oracle模式下RAISE;语句, 不指定异常名, 此时需要抛出当前异常
  bool is_resignal_stmt_;
};

class ObPLRaiseAppErrorStmt : public ObPLStmt
{
public:
  ObPLRaiseAppErrorStmt(common::ObIAllocator &allocator)
      : ObPLStmt(PL_RAISE_APPLICATION_ERROR), params_(allocator) {}
  virtual ~ObPLRaiseAppErrorStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline const common::ObIArray<int64_t> &get_params() const { return params_; }
  inline int add_param(int64_t idx) { return params_.push_back(idx); }

  TO_STRING_KV(K_(type), K_(label), K_(params));

private:
  ObPLSEArray<int64_t> params_;
};

class ObPLCallStmt : public ObPLStmt
{
public:
  ObPLCallStmt(common::ObIAllocator &allocator)
      : ObPLStmt(PL_CALL),
        invoker_id_(common::OB_INVALID_ID),
        package_id_(common::OB_INVALID_ID),
        proc_id_(common::OB_INVALID_ID),
        is_object_udf_(0),
        subprogram_path_(allocator),
        params_(allocator),
        nocopy_params_(allocator),
        route_sql_(),
        dblink_id_(common::OB_INVALID_ID) {}
  virtual ~ObPLCallStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline uint64_t get_invoker_id() const { return invoker_id_; }
  inline void set_invoker_id(const uint64_t invoker_id) { invoker_id_ = invoker_id; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(const uint64_t package_id) { package_id_ = package_id; }
  inline int64_t get_proc_id() const { return proc_id_; }
  inline void set_proc_id(int64_t proc_id) { proc_id_ = proc_id; }
  inline const common::ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_; }
  inline int set_subprogram_path(const common::ObIArray<int64_t> &path) { return subprogram_path_.assign(path); }
  inline const common::ObIArray<InOutParam> &get_params() const { return params_; }
  inline int64_t get_param(int64_t i) const { return params_.at(i).param_; }
  inline const sql::ObRawExpr *get_param_expr(int64_t i) const { return get_expr(params_.at(i).param_); }
  inline int64_t get_out_index(int64_t i) const { return params_.at(i).out_idx_; }
  inline bool is_out(int64_t i) const { return PL_PARAM_OUT == params_.at(i).mode_ || PL_PARAM_INOUT == params_.at(i).mode_; }
  inline bool is_pure_out(int64_t i) const { return PL_PARAM_OUT == params_.at(i).mode_; }
  inline int add_param(int64_t param, ObPLRoutineParamMode mode, int64_t idx) { return params_.push_back(InOutParam(param, mode, idx)); }
  inline common::ObIArray<int64_t> &get_nocopy_params() { return nocopy_params_; }
  inline const common::ObIArray<int64_t> &get_nocopy_params() const { return nocopy_params_; }
  inline void set_is_object_udf() { is_object_udf_ = 1; }
  inline uint64_t get_is_object_udf() const { return is_object_udf_; }
  inline const common::ObString &get_route_sql() const { return route_sql_; }
  inline void set_route_sql(const common::ObString &route_sql) { route_sql_ = route_sql; }
  inline void set_dblink_id(uint64_t dblink_id) { dblink_id_ = dblink_id; }
  inline uint64_t get_dblink_id() const { return dblink_id_; }
  inline bool is_dblink_call() const { return common::OB_INVALID_ID == dblink_id_; }

  TO_STRING_KV(K_(type),
               K_(label),
               K_(package_id),
               K_(proc_id),
               K_(is_object_udf),
               K_(params),
               K_(nocopy_params),
               K_(route_sql),
               K_(dblink_id));

private:
  uint64_t invoker_id_;
  uint64_t package_id_;
  uint64_t proc_id_;
  uint64_t is_object_udf_; // 1: true, why use uint64_t but not bool, for the convenience with llvm cg
  ObPLSEArray<int64_t> subprogram_path_;
  ObPLSEArray<InOutParam> params_;
  ObPLSEArray<int64_t> nocopy_params_;
  common::ObString route_sql_;
  uint64_t dblink_id_;
};

class ObPLInnerCallStmt : public ObPLStmt
{
public:
  ObPLInnerCallStmt(common::ObIAllocator &allocator)
      : ObPLStmt(PL_INNER_CALL),
        package_id_(common::OB_INVALID_ID),
        proc_id_(common::OB_INVALID_ID),
        params_(allocator) {}
  virtual ~ObPLInnerCallStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(const uint64_t package_id) { package_id_ = package_id; }
  inline int64_t get_proc_id() const { return proc_id_; }
  inline void set_proc_id(int64_t proc_id) { proc_id_ = proc_id; }
  inline const common::ObIArray<InOutParam> &get_params() const { return params_; }
  inline int64_t get_param(int64_t i) const { return params_.at(i).param_; }
  inline const sql::ObRawExpr *get_param_expr(int64_t i) const { return get_expr(params_.at(i).param_); }
  inline int64_t get_out_index(int64_t i) const { return params_.at(i).out_idx_; }
  inline bool is_out(int64_t i) const { return PL_PARAM_OUT == params_.at(i).mode_ || PL_PARAM_INOUT == params_.at(i).mode_; }
  inline bool is_pure_out(int64_t i) const { return PL_PARAM_OUT == params_.at(i).mode_; }
  inline int add_param(int64_t param, ObPLRoutineParamMode mode, int64_t idx) { return params_.push_back(InOutParam(param, mode, idx)); }

  TO_STRING_KV(K_(type), K_(label), K_(package_id), K_(proc_id), K_(params));

private:
  uint64_t package_id_;
  uint64_t proc_id_;
  ObPLSEArray<InOutParam> params_;
};

class ObPLDeclareCursorStmt : public ObPLStmt
{
public:
  ObPLDeclareCursorStmt() : ObPLStmt(PL_CURSOR), cur_idx_(common::OB_INVALID_INDEX) {}
  virtual ~ObPLDeclareCursorStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_cursor_index() const { return cur_idx_; }
  inline void set_cursor_index(int64_t idx) { cur_idx_ = idx; }
  inline const ObPLCursor *get_cursor() const { return ObPLStmt::get_cursor(cur_idx_); }
  inline int64_t get_index() const { return NULL == get_cursor() ? common::OB_INVALID_INDEX : ObPLStmt::get_cursor(cur_idx_)->get_index(); }
  inline const ObPLVar *get_var() const { return common::OB_INVALID_INDEX == get_index() ? NULL : get_variable(get_index()); }

  TO_STRING_KV(K_(type), K_(label), K_(cur_idx));

private:
  int64_t cur_idx_; // cursor表里的下标
};

class ObPLOpenStmt : public ObPLStmt
{
public:
  ObPLOpenStmt(common::ObIAllocator &allocator, ObPLStmtType type = PL_OPEN)
      : ObPLStmt(type),
        cur_idx_(common::OB_INVALID_INDEX),
        params_(allocator) {}
  virtual ~ObPLOpenStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_cursor_index() const { return cur_idx_; }
  inline void set_cursor_index(int64_t idx) { cur_idx_ = idx; }
  inline const ObIArray<int64_t> &get_params() const { return params_; }
  inline int set_params(const ObIArray<int64_t> &params) { return params_.assign(params); }
  inline int add_param(int64_t param) { return params_.push_back(param); }

  inline uint64_t get_package_id() const
  {
    return NULL == get_cursor()
            ? common::OB_INVALID_ID : ObPLStmt::get_cursor(cur_idx_)->get_package_id();
  }

  inline uint64_t get_routine_id() const
  {
    return NULL == get_cursor()
            ? common::OB_INVALID_ID : ObPLStmt::get_cursor(cur_idx_)->get_routine_id();
  }

  inline int64_t get_index() const
  {
    return NULL == get_cursor()
            ? common::OB_INVALID_INDEX : ObPLStmt::get_cursor(cur_idx_)->get_index();
  }
  inline int get_var(const ObPLVar *&var) const
  {
    //只有local cursor才能在本地符号表找到对应变量
    return get_namespace()->get_cursor_var(get_package_id(),
                                           get_routine_id(),
                                           get_index(), var);
  }
  inline const ObPLCursor *get_cursor() const
  {
    return ObPLStmt::get_cursor(cur_idx_);
  }

  TO_STRING_KV(K_(type), K_(label), K_(cur_idx), K_(params));

protected:
  int64_t cur_idx_; //cursor表里的下标
  ObPLSEArray<int64_t> params_;
};

class ObPLOpenForStmt : public ObPLOpenStmt, public ObPLUsing
{
public:
  ObPLOpenForStmt(common::ObIAllocator &allocator)
    : ObPLOpenStmt(allocator, PL_OPEN_FOR),
      ObPLUsing(allocator),
      dynamic_sql_(OB_INVALID_INDEX), static_sql_(allocator) {}
  virtual ~ObPLOpenForStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_dynamic_sql() const { return dynamic_sql_; }
  inline void set_dynamic_sql(int64_t idx) { dynamic_sql_ = idx; }
  inline const ObPLSql &get_static_sql() const { return static_sql_; }
  inline ObPLSql &get_static_sql() { return static_sql_; }
  inline void set_static_sql(ObPLSql &sql) { static_sql_ = sql; }
  inline bool is_dynamic() const { return OB_INVALID_INDEX != dynamic_sql_; }

  TO_STRING_KV(K_(type), K_(label), K_(cur_idx), K_(dynamic_sql), K_(static_sql));

private:
  int64_t dynamic_sql_; //dynamic_sql表达式的下标
  ObPLSql static_sql_; //static_sql的描述
};

class ObPLFetchStmt : public ObPLStmt, public ObPLInto
{
public:
  ObPLFetchStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_FETCH),
      ObPLInto(allocator),
      pkg_id_(OB_INVALID_ID),
      routine_id_(OB_INVALID_ID),
      idx_(common::OB_INVALID_INDEX),
      limit_(INT64_MAX),
      user_type_(NULL) {}
  virtual ~ObPLFetchStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline uint64_t get_package_id() const { return pkg_id_; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline int64_t get_index() const { return idx_; }
  inline void set_index(int64_t idx) { idx_ = idx; }
  inline void set_index(uint64_t pkg_id, uint64_t routine_id, int64_t idx)
  {
    pkg_id_ = pkg_id;
    routine_id_ = routine_id;
    idx_ = idx;
  }
  inline const ObUserDefinedType* get_user_type() const
  {
    return user_type_;
  }
  inline void set_user_type(const ObUserDefinedType *user_type)
  {
    user_type_ = user_type;
  }
  inline int64_t get_limit() const { return limit_; }
  inline void set_limit(int64_t limit) { limit_ = limit; }

  TO_STRING_KV(K_(type), K_(label), K_(pkg_id), K_(routine_id), K_(idx), K_(into), K_(bulk));

private:
  uint64_t pkg_id_;
  uint64_t routine_id_;
  int64_t idx_; //symbol表里的下标
  int64_t limit_; //INT64_MAX:是bulk fetch但是没有limit子句
  const ObUserDefinedType *user_type_; // CURSOR返回值类型
};

class ObPLCloseStmt : public ObPLStmt
{
public:
  ObPLCloseStmt()
    : ObPLStmt(PL_CLOSE),
    pkg_id_(OB_INVALID_ID),
    routine_id_(OB_INVALID_ID),
    idx_(common::OB_INVALID_INDEX) {}
  virtual ~ObPLCloseStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline uint64_t get_package_id() const { return pkg_id_; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline int64_t get_index() const { return idx_; }
  inline void set_index(int64_t idx) { idx_ = idx; }
  inline void set_index(uint64_t pkd_id, uint64_t routine_id, int64_t idx)
  {
    pkg_id_ = pkd_id;
    routine_id_ = routine_id;
    idx_ = idx;
  }

  TO_STRING_KV(K_(type), K_(label), K_(pkg_id), K_(routine_id), K_(idx));

private:
  uint64_t pkg_id_;
  uint64_t routine_id_;
  int64_t idx_; //symbol表里的下标
};

class ObPLNullStmt : public ObPLStmt
{
public:
  ObPLNullStmt() : ObPLStmt(PL_NULL) {}
  virtual ~ObPLNullStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  TO_STRING_KV(K_(type), K_(label));

private:

};

class ObPLPipeRowStmt : public ObPLStmt
{
public:
  ObPLPipeRowStmt() : ObPLStmt(PL_PIPE_ROW), row_(OB_INVALID_INDEX), pipe_type_() {}
  virtual ~ObPLPipeRowStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline void set_row(int64_t row) { row_ = row; }
  inline int64_t get_row() const { return row_; }

  inline void set_type(const ObPLDataType &type) { pipe_type_ = type; }
  inline const ObPLDataType& get_type() const { return pipe_type_; }

  TO_STRING_KV(K_(type), K_(label), K_(row), K_(pipe_type));

private:
  int64_t row_;
  ObPLDataType pipe_type_;
};

class ObPLRoutineDefStmt : public ObPLStmt
{
public:
  ObPLRoutineDefStmt()
      : ObPLStmt(PL_ROUTINE_DEF), type_(INVALID_PROC_TYPE),
        idx_(common::OB_INVALID_INDEX) {}
  virtual ~ObPLRoutineDefStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_index() const { return idx_; }
  inline void set_index(int64_t idx) { idx_ = idx; }

  TO_STRING_KV(K_(type), K_(idx));

private:
  ObProcType type_;
  int64_t idx_; //routine表里的下标
};

class ObPLRoutineDeclStmt : public ObPLStmt
{
public:
  ObPLRoutineDeclStmt()
      : ObPLStmt(PL_ROUTINE_DECL), type_(INVALID_PROC_TYPE),
        idx_(common::OB_INVALID_INDEX) {}
  virtual ~ObPLRoutineDeclStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline int64_t get_index() const { return idx_; }
  inline void set_index(int64_t idx) { idx_ = idx; }

  TO_STRING_KV(K_(type), K_(idx));

private:
  ObProcType type_;
  int64_t idx_; //routine表里的下标
};

class ObPLGotoStmt : public ObPLStmt
{
public:
  ObPLGotoStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_GOTO),
    dst_stmt_(NULL),
    cursor_stmts_(allocator),
    dst_label_(),
    in_exception_handler_(false) {}

  virtual ~ObPLGotoStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  void set_dst_label(const common::ObString label) { dst_label_ = label; }
  common::ObString get_dst_label() const { return dst_label_; }
  void set_dst_stmt(const ObPLStmt *stmt) { dst_stmt_ = stmt; }
  const ObPLStmt *get_dst_stmt() const { return dst_stmt_; }
  void set_in_exception_handler(bool flag) { in_exception_handler_ = flag; }
  bool get_in_exception_handler() { return in_exception_handler_; }
  const ObPLStmt *get_cursor_stmt(int64_t idx) const
  {
    const ObPLStmt *stmt = NULL;
    if (idx >= 0 && idx < cursor_stmts_.count()) {
      stmt = cursor_stmts_.at(idx);
    }
    return stmt;
  }
  inline int push_cursor_stmt(const ObPLStmt *stmt) { return cursor_stmts_.push_back(stmt); }
  inline void pop_cursor_stmt() { cursor_stmts_.pop_back(); }
  inline int64_t get_cursor_stmt_count() const { return cursor_stmts_.count(); }

  TO_STRING_KV(K_(dst_stmt), K_(dst_label), K_(in_exception_handler), K_(cursor_stmts));

private:
  const ObPLStmt *dst_stmt_;
  // 这里保存goto跳出cursor的时候，所有需要关闭的cursor语句, 因为forloop cursor可能会多层嵌套
  ObPLSEArray<const ObPLStmt *> cursor_stmts_;
  common::ObString dst_label_;
  bool in_exception_handler_;
};

class ObPLInterfaceStmt : public ObPLStmt
{
public:
  ObPLInterfaceStmt() : ObPLStmt(PL_INTERFACE), entry_(0) {}
  virtual ~ObPLInterfaceStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline ObString get_entry() const { return entry_; }
  inline void set_entry(const common::ObString &entry) { entry_ = entry; }

  TO_STRING_KV(K_(entry));

private:
  common::ObString entry_;
};

class ObPLDoStmt : public ObPLStmt
{
public:
  ObPLDoStmt(common::ObIAllocator &allocator)
    : ObPLStmt(PL_DO), value_(allocator) {}
  virtual ~ObPLDoStmt() {}

  int accept(ObPLStmtVisitor &visitor) const;

  inline const common::ObIArray<int64_t> &get_value() const { return value_;}
  inline int64_t get_value_index(int64_t i) const { return value_.at(i);}
  inline const sql::ObRawExpr *get_value_expr(int64_t i) const { return get_expr(value_.at(i)); }
  inline int add_value(int64_t idx) { return value_.push_back(idx); }
private:
  ObPLSEArray<int64_t> value_;
};

class ObPLStmtFactory
{
public:
  explicit ObPLStmtFactory(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObPLStmtFactory() {}
  int allocate(ObPLStmtType type, const ObPLStmtBlock *block, ObPLStmt *&stmt);

private:
  common::ObIAllocator &allocator_;
};

class ObPLStmtVisitor
{
public:
  virtual ~ObPLStmtVisitor() {}
  virtual int visit(const ObPLStmtBlock &s) = 0;
  virtual int visit(const ObPLDeclareVarStmt &s) = 0;
  virtual int visit(const ObPLAssignStmt &s) = 0;
  virtual int visit(const ObPLIfStmt &s) = 0;
  virtual int visit(const ObPLLeaveStmt &s) = 0;
  virtual int visit(const ObPLIterateStmt &s) = 0;
  virtual int visit(const ObPLWhileStmt &s) = 0;
  virtual int visit(const ObPLForLoopStmt &s) = 0;
  virtual int visit(const ObPLCursorForLoopStmt &s) = 0;
  virtual int visit(const ObPLForAllStmt &s) = 0;
  virtual int visit(const ObPLRepeatStmt &s) = 0;
  virtual int visit(const ObPLLoopStmt &s) = 0;
  virtual int visit(const ObPLReturnStmt &s) = 0;
  virtual int visit(const ObPLSqlStmt &s) = 0;
  virtual int visit(const ObPLExecuteStmt &s) = 0;
  virtual int visit(const ObPLExtendStmt &s) = 0;
  virtual int visit(const ObPLDeleteStmt &s) = 0;
  virtual int visit(const ObPLDeclareCondStmt &s) = 0;
  virtual int visit(const ObPLDeclareHandlerStmt &s) = 0;
  virtual int visit(const ObPLSignalStmt &s) = 0;
  virtual int visit(const ObPLCallStmt &s) = 0;
  virtual int visit(const ObPLDeclareCursorStmt &s) = 0;
  virtual int visit(const ObPLOpenStmt &s) = 0;
  virtual int visit(const ObPLOpenForStmt &s) = 0;
  virtual int visit(const ObPLFetchStmt &s) = 0;
  virtual int visit(const ObPLCloseStmt &s) = 0;
  virtual int visit(const ObPLNullStmt &s) = 0;
  virtual int visit(const ObPLPipeRowStmt &s) = 0;
  virtual int visit(const ObPLDeclareUserTypeStmt &s) = 0;
  virtual int visit(const ObPLRaiseAppErrorStmt &s) = 0;
  virtual int visit(const ObPLGotoStmt &s) = 0;
  virtual int visit(const ObPLTrimStmt &s) = 0;
  virtual int visit(const ObPLInterfaceStmt &s) = 0;
  virtual int visit(const ObPLDoStmt &s) = 0;
  virtual int visit(const ObPLCaseStmt &s) = 0;
};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_STMT_H_ */
