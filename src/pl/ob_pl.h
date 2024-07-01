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

#ifndef OCEANBASE_SRC_PL_OB_PL_H_

#define OCEANBASE_SRC_PL_OB_PL_H_
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_2d_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/rc/context.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "objit/ob_llvm_helper.h"
#include "objit/ob_llvm_di_helper.h"
#include "share/ob_errno.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/parser/parse_node.h"
#include "pl/parser/parse_stmt_node.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_package_manager.h"
#include "pl/ob_pl_interface_pragma.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "pl/pl_cache/ob_pl_cache.h"
#include "pl/pl_cache/ob_pl_cache_object.h"

namespace test
{
class MockCacheObjectFactory;
}
namespace oceanbase
{

namespace sql
{
class ObPsCache;
class ObSQLSessionInfo;
class ObAlterRoutineResolver;
}
namespace jit
{
class ObDWARFHelper;
}
using common::ObPsStmtId;
namespace pl
{
typedef common::ObFixedBitSet<128> ObPLFlag;
typedef void* ObPointer;
typedef uint64_t ObFuncPtr;
typedef common::ParamStore ParamStore;

class ObPLCacheCtx;

class ObPLProfilerTimeStack;

enum ObPLObjectType
{
  INVALID_OBJECT_TYPE = -1,
  TABLE,
  PL,
  PACKAGE_SPEC
};

class ObPLINS
{
public:
  ObPLINS() {}
  virtual ~ObPLINS() {}

  virtual int get_user_type(uint64_t type_id,
                            const ObUserDefinedType *&user_type,
                            ObIAllocator *allocator = NULL) const = 0;

  virtual int get_size(ObPLTypeSize type,
                       const ObPLDataType &pl_type,
                       int64_t &size,
                       ObIAllocator *allocator = NULL) const;
  virtual int get_element_data_type(const ObPLDataType &pl_type,
                        ObDataType &elem_type,
                        ObIAllocator *allocator = NULL) const;
  virtual int get_not_null(const ObPLDataType &pl_type,
                                     bool &not_null,
                                     ObIAllocator *allocator = NULL) const;
  virtual int init_complex_obj(ObIAllocator &allocator,
                               const ObPLDataType &pl_type,
                               common::ObObjParam &obj,
                               bool set_allocator = false,
                               bool set_null = true);

  virtual int calc_expr(uint64_t package_id, int64_t expr_idx, ObObjParam &result);
};

class ObPLFunctionBase
{
  static const int64_t DEBUG_MODE = 1; // DEBUG INFO
  static const int64_t CONTAINS_DYNAMIC_SQL = 2; // PS
  static const int64_t MULTI_RESULTS = 3; // SELECT
  static const int64_t HAS_COMMIT_OR_ROLLBACK = 4; // DDL
  static const int64_t HAS_SET_AUTOCOMMIT_STMT = 5; // SET AUTOCOMMIT
  static const int64_t IS_AUTONOMOUS_TRANSACTION = 6;
  static const int64_t IS_UDT_ROUTINE = 7; // function inside udt object
  static const int64_t HAS_OPEN_EXTERNAL_REF_CURSOR = 8; // A SUBPROGRAM MAY OPEN PARENT REF CURSOR
  static const int64_t IS_UDT_CONS = 9; // udt constructor
  static const int64_t HAS_DEBUG_PRIV = 10;

public:
  ObPLFunctionBase()
  : proc_type_(INVALID_PROC_TYPE),
    routine_id_(common::OB_INVALID_ID),
    package_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    tenant_id_(common::OB_INVALID_ID),
    package_version_(common::OB_INVALID_VERSION),
    owner_(common::OB_INVALID_ID),
    priv_user_(common::OB_INVALID_ID),
    gmt_create_(0),
    ret_type_(common::ObNullType),
    arg_count_(0),
    flag_() {}
  virtual ~ObPLFunctionBase() {}

public:
  inline ObProcType get_proc_type() const { return proc_type_; }
  inline void set_proc_type(ObProcType type) { proc_type_ = type; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  // inline uint64_t get_tenant_id() const { return tenant_id_; }
  // inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_package_version() const { return package_version_; }
  inline void set_package_version(uint64_t package_version) { package_version_ = package_version; }
  inline uint64_t get_owner() const { return owner_; }
  inline void set_owner(uint64_t owner) { owner_ = owner; }
  inline uint64_t get_gmt_create() const { return gmt_create_; }
  inline void set_gmt_create(uint64_t gmt_create) { gmt_create_ = gmt_create; }
  inline int64_t get_arg_count() const { return arg_count_; }
  inline void set_arg_count(int64_t arg_count) { arg_count_ = arg_count; }
  inline const ObPLFlag &get_flag() const { return flag_; }
  inline int add_members(const ObPLFlag &flag) { return flag_.add_members(flag); }

  inline void add_flag(int64_t index) { flag_.add_member(index); }
  inline bool has_flag(int64_t index) const { return flag_.has_member(index); }

  inline void set_contain_dynamic_sql() { flag_.add_member(CONTAINS_DYNAMIC_SQL); }
  inline bool get_contain_dynamic_sql() const { return flag_.has_member(CONTAINS_DYNAMIC_SQL); }
  inline void set_multi_results() { flag_.add_member(MULTI_RESULTS); }
  inline bool get_multi_results() const { return flag_.has_member(MULTI_RESULTS); }
  inline void set_has_commit_or_rollback() { flag_.add_member(HAS_COMMIT_OR_ROLLBACK); }
  inline bool get_has_commit_or_rollback() const { return flag_.has_member(HAS_COMMIT_OR_ROLLBACK); }
  inline void set_has_set_autocommit_stmt() { flag_.add_member(HAS_SET_AUTOCOMMIT_STMT); }
  inline bool get_has_set_autocommit_stmt() const { return flag_.has_member(HAS_SET_AUTOCOMMIT_STMT); }
  inline void set_autonomous() { flag_.add_member(IS_AUTONOMOUS_TRANSACTION); }
  inline bool is_autonomous() const { return flag_.has_member(IS_AUTONOMOUS_TRANSACTION); }

  inline const ObPLDataType &get_ret_type() const { return ret_type_; }
  inline void set_ret_type(const ObPLDataType &ret_type) { ret_type_ = ret_type; }
  inline int set_ret_type_info(const common::ObIArray<common::ObString>& type_info)
  {
    return ret_type_.set_type_info(type_info);
  }
  inline bool is_function()
  {
    return STANDALONE_FUNCTION == proc_type_
          || PACKAGE_FUNCTION == proc_type_
          || NESTED_FUNCTION == proc_type_
          || UDT_FUNCTION == proc_type_;
  }

  inline bool is_udt_routine() const {
    return has_flag(IS_UDT_ROUTINE);
  }
  inline void set_is_udt_routine() {
    flag_.add_member(IS_UDT_ROUTINE);
  }
  inline bool has_open_external_ref_cursor() const {
    return has_flag(HAS_OPEN_EXTERNAL_REF_CURSOR);
  }
  inline void set_open_external_ref_cursor() {
    flag_.add_member(HAS_OPEN_EXTERNAL_REF_CURSOR);
  }
  inline void set_is_udt_cons() {
    flag_.add_member(IS_UDT_CONS);
  }
  inline bool is_udt_cons() const {
    return has_flag(IS_UDT_CONS);
  }
  inline bool has_debug_priv() const {
    return has_flag(HAS_DEBUG_PRIV);
  }
  inline void set_debug_priv() {
    flag_.add_member(HAS_DEBUG_PRIV);
  }
  inline void clean_debug_priv() {
    flag_.del_member(HAS_DEBUG_PRIV);
  }

private:
  //基础信息
  ObProcType proc_type_;
  uint64_t routine_id_;
  uint64_t package_id_;
  uint64_t database_id_;
  uint64_t tenant_id_;
  uint64_t package_version_;
  uint64_t owner_;
  uint64_t priv_user_;
  int64_t gmt_create_;
  ObPLDataType ret_type_;
  int64_t arg_count_; //参数一定在符号表的最前面
  ObPLFlag flag_;

  DISALLOW_COPY_AND_ASSIGN(ObPLFunctionBase);
};

class ObPLCompileUnit : public ObPLCacheObject
{
  friend class ::test::MockCacheObjectFactory;
public:
  ObPLCompileUnit(sql::ObLibCacheNameSpace ns, lib::MemoryContext &mem_context);
  virtual ~ObPLCompileUnit();

  inline bool get_can_cached() { return can_cached_; }
  inline void set_can_cached(bool can_cached) { can_cached_ = can_cached; }
  inline const ObIArray<ObPLFunction*> &get_routine_table() const { return routine_table_; }
  inline ObIArray<ObPLFunction*> &get_routine_table() { return routine_table_; }
  inline int set_routine_table(ObIArray<ObPLFunction*> &table)
  {
    return routine_table_.assign(table);
  }
  int add_routine(ObPLFunction *routine);
  int get_routine(int64_t routine_idx, ObPLFunction *&routine) const;
  void init_routine_table(int64_t count) { routine_table_.set_capacity(static_cast<uint32_t>(count)); }
  inline const ObIArray<ObUserDefinedType *> &get_type_table() const { return type_table_; }

  inline jit::ObLLVMHelper &get_helper() { return helper_; }
  inline jit::ObLLVMDIHelper &get_di_helper() { return di_helper_; }

  inline const sql::ObExecEnv &get_exec_env() const { return exec_env_; }
  inline sql::ObExecEnv &get_exec_env() { return exec_env_; }
  inline void set_exec_env(const sql::ObExecEnv &env) { exec_env_ = env; }

  jit::ObDIRawData get_debug_info() const { return helper_.get_debug_info(); }

  virtual void reset();
  virtual void dump_deleted_log_info(const bool is_debug_log = true) const;
  virtual int check_need_add_cache_obj_stat(ObILibCacheCtx &ctx, bool &need_real_add);

  OB_INLINE std::pair<uint64_t, ObProcType> get_profiler_unit_info() const { return profiler_unit_info_; }
  OB_INLINE void set_profiler_unit_info(uint64_t unit_id, ObProcType type) { profiler_unit_info_ = std::make_pair(unit_id, type); }
  OB_INLINE void set_profiler_unit_info(const std::pair<uint64_t, ObProcType> &unit_info) { profiler_unit_info_ = unit_info; }

  TO_STRING_KV(K_(routine_table), K_(can_cached),
               K_(tenant_schema_version), K_(sys_schema_version));

protected:

  common::ObFixedArray<ObPLFunction*, common::ObIAllocator> routine_table_;
  common::ObArray<ObUserDefinedType *> type_table_;

  jit::ObLLVMHelper helper_;
  jit::ObLLVMDIHelper di_helper_;

  bool can_cached_;
  sql::ObExecEnv exec_env_;

  std::pair<uint64_t, ObProcType> profiler_unit_info_;

  DISALLOW_COPY_AND_ASSIGN(ObPLCompileUnit);
};

class ObPLSymbolTable;
class ObPLSymbolDebugInfoTable;
class ObPLFunctionAST;

class ObPLSqlStmt;
class ObPLSqlInfo
{
public:
  ObPLSqlInfo()
    : loc_(0), forall_sql_(false), for_update_(false), has_hidden_rowid_(false),
      sql_(), params_(), array_binding_params_(), ps_sql_(),
      stmt_type_(sql::stmt::StmtType::T_NONE), rowid_table_id_(OB_INVALID_ID),
      into_(), not_null_flags_(), pl_integer_ranges_(),
      data_type_(), bulk_(false), allocator_(nullptr) {}

  ObPLSqlInfo(common::ObIAllocator &allocator)
    : loc_(0), forall_sql_(false), for_update_(false), has_hidden_rowid_(false),
      sql_(), params_(allocator), array_binding_params_(allocator), ps_sql_(),
      stmt_type_(sql::stmt::StmtType::T_NONE), rowid_table_id_(OB_INVALID_ID),
      into_(allocator), not_null_flags_(allocator), pl_integer_ranges_(allocator),
      data_type_(allocator), bulk_(false), allocator_(&allocator) {}

  virtual ~ObPLSqlInfo() {}

  int generate(const ObPLSqlStmt &sql, ObIArray<sql::ObSqlExpression *> &exprs);

  TO_STRING_KV(K(loc_), K(forall_sql_), K(for_update_), K(has_hidden_rowid_), K(sql_),
               K(params_), K(array_binding_params_), K(ps_sql_), K(stmt_type_),
               K(rowid_table_id_), K(into_), K(not_null_flags_), K(pl_integer_ranges_),
               K(data_type_), K(bulk_));

public:
  uint64_t loc_;

  bool forall_sql_;
  bool for_update_;
  bool has_hidden_rowid_;
  common::ObString sql_;
  ObFixedArray<const sql::ObSqlExpression *, common::ObIAllocator> params_;
  ObFixedArray<const sql::ObSqlExpression *, common::ObIAllocator> array_binding_params_;
  common::ObString ps_sql_;
  sql::stmt::StmtType stmt_type_;
  uint64_t rowid_table_id_;

  ObFixedArray<const sql::ObSqlExpression *, common::ObIAllocator> into_;
  ObFixedArray<bool, common::ObIAllocator> not_null_flags_;
  ObFixedArray<int64_t, common::ObIAllocator> pl_integer_ranges_;
  ObFixedArray<ObDataType, common::ObIAllocator> data_type_;
  bool bulk_;

  ObIAllocator *allocator_;
};

class ObPLVarDebugInfo
{
public:
  struct ObPLVarScope {
    ObPLVarScope() : start_(-1), end_(-1) {}
    ObPLVarScope(int start, int end)
      : start_(start), end_(end) {}

    inline bool contain(int line) { return start_ <= line && end_ >= line; }

    int start_;
    int end_;

    TO_STRING_KV(K_(start), K_(end));
  };

  ObPLVarDebugInfo()
    : name_(), type_(ObPLType::PL_INVALID_TYPE), scope_() {}
  ObPLVarDebugInfo(const common::ObString &name, ObPLType type, ObPLVarScope scope)
    : name_(name), type_(type), scope_(scope) {}

  inline bool contain(int line) { return scope_.contain(line); }

  inline bool is_obj()
  {
    return ObPLType::PL_OBJ_TYPE == type_;
  }
  inline bool is_collection()
  {
    return ObPLType::PL_NESTED_TABLE_TYPE == type_
      || ObPLType::PL_ASSOCIATIVE_ARRAY_TYPE == type_
      || ObPLType::PL_VARRAY_TYPE == type_;
  }
  inline bool is_record()
  {
    return ObPLType::PL_RECORD_TYPE == type_;
  }
  inline const common::ObString& get_name() const { return name_; }

  int deep_copy(ObIAllocator &allocator, const ObPLVarDebugInfo& other);

  TO_STRING_KV(K_(name), K_(type), K_(scope));

private:
  common::ObString name_;
  ObPLType type_;
  ObPLVarScope scope_;
};

class ObPLNameDebugInfo
{
public:
  ObPLNameDebugInfo() : owner_name_(), package_name_(), routine_name_() {}
public:
  ObString owner_name_;
  ObString package_name_;
  ObString routine_name_;
};

class ObPLFunction : public ObPLFunctionBase, public ObPLCompileUnit
{
public:
  ObPLFunction(lib::MemoryContext &mem_context)
  : ObPLFunctionBase(), ObPLCompileUnit(sql::ObLibCacheNameSpace::NS_PRCR, mem_context),
    variables_(allocator_),
    variables_debuginfo_(allocator_),
    default_idxs_(allocator_),
    sql_infos_(allocator_),
    in_args_(),
    out_args_(),
    action_(0),
    di_buf_(NULL),
    di_len_(0),
    is_all_sql_stmt_(true),
    is_invoker_right_(false),
    is_pipelined_(false),
    name_debuginfo_(),
    function_name_(),
    has_parallel_affect_factor_(false) { }
  virtual ~ObPLFunction();

  inline const common::ObIArray<ObPLDataType> &get_variables() const { return variables_; }
  inline const common::ObIArray<ObPLVarDebugInfo *> &get_variables_debuginfo() const
  {
    return variables_debuginfo_;
  }
  inline const common::ObIArray<int64_t> &get_default_idxs() const { return default_idxs_; }
  inline sql::ObSqlExpression* get_default_expr(int64_t param)
  {
    int64_t idx = param >= 0 && param < default_idxs_.count() ? default_idxs_.at(param) : -1;
    return idx >= 0 && idx < expressions_.count() ? expressions_.at(idx) : NULL;
  }
  inline const ObPLNameDebugInfo& get_name_debuginfo() const { return name_debuginfo_; }
  int set_name_debuginfo(const ObPLFunctionAST &ast);
  int set_variables(const ObPLSymbolTable &symbol_table);
  int set_variables_debuginfo(const ObPLSymbolDebugInfoTable &symbol_debuginfo_table);
  int set_types(const ObPLUserTypeTable &type_table);
  inline const common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> &get_in_args() const { return in_args_; }
  inline void set_in_args(const common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> &out_idx) { in_args_ = out_idx; }
  inline int add_in_arg(int64_t i) { return in_args_.add_member(i); }
  inline const common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> &get_out_args() const { return out_args_; }
  inline void set_out_args(const common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> &out_idx) { out_args_ = out_idx; }
  inline int add_out_arg(int64_t i) { return out_args_.add_member(i); }
  inline ObFuncPtr get_action() const { return action_; }
  inline void set_action(ObFuncPtr action) { action_ = action; }

  inline bool is_debug_mode() const { return !get_debug_info().empty(); }

  inline bool get_is_all_sql_stmt() const { return is_all_sql_stmt_; }
  inline void set_is_all_sql_stmt(bool is_all_sql_stmt) { is_all_sql_stmt_ = is_all_sql_stmt; }

  inline bool is_invoker_right() const { return is_invoker_right_; }
  inline void set_invoker_right() { is_invoker_right_ = true; }

  inline bool is_pipelined() const { return is_pipelined_; }
  inline void set_pipelined(bool is_pipelined) { is_pipelined_ = is_pipelined; }

  inline bool get_has_parallel_affect_factor() const { return has_parallel_affect_factor_; }
  inline void set_has_parallel_affect_factor(bool value) { has_parallel_affect_factor_ = value; }

  int get_subprogram(const ObIArray<int64_t> &path, ObPLFunction *&routine) const;

  inline const common::ObString &get_function_name() const { return function_name_; }
  inline const common::ObString &get_package_name() const { return package_name_; }
  inline const common::ObString &get_database_name() const { return database_name_; }
  inline const common::ObString &get_priv_user() const { return priv_user_; }
  int set_function_name(const ObString &function_name)
  {
    return ob_write_string(get_allocator(), function_name, function_name_);
  }
  int set_package_name(const ObString &package_name)
  {
    return ob_write_string(get_allocator(), package_name, package_name_);
  }
  int set_database_name(const ObString &database_name)
  {
    return ob_write_string(get_allocator(), database_name, database_name_);
  }
  int set_priv_user(const ObString &priv_user)
  {
    return ob_write_string(get_allocator(), priv_user, priv_user_);
  }

  inline bool need_register_debug_info()
  {
    return is_debug_mode()
        && get_tenant_id() != OB_SYS_TENANT_ID
        && has_debug_priv()
        && !ObTriggerInfo::is_trigger_package_id(get_package_id())
        && !ObUDTObjectType::is_object_id(get_package_id());
  }
  bool should_init_as_session_cursor();
  /*
  * some package subprogram has special invoker right, though the package may have definer privs
  * for example: dbms_utility package is definer privs, but some function such as
  * name_resolve must be run as current_user, oracle do it in interface functions
  * see:
  * we hacked it using name compared, for the interface funtion can't get the origin db name and id
  * test -> oceanbase, we see oceanbase in interface but can't see test.
  */
  int is_special_pkg_invoke_right(ObSchemaGetterGuard &guard, bool &flag);

  int gen_action_from_precompiled(const ObString &name, size_t length, const char *ptr);

  common::ObFixedArray<ObPLSqlInfo, common::ObIAllocator>& get_sql_infos()
  {
    return sql_infos_;
  }

  TO_STRING_KV(K_(ns),
               K_(ref_count),
               K_(tenant_schema_version),
               K_(sys_schema_version),
               K_(object_id),
               K_(dependency_tables),
               K_(params_info),
               K_(variables),
               K_(default_idxs),
               K_(function_name),
               K_(priv_user));

private:
  //符号表信息
  common::ObFixedArray<ObPLDataType, common::ObIAllocator> variables_; //根据ObPLSymbolTable的全局符号表生成，所有输入输出参数和PL体内使用的所有变量
  common::ObFixedArray<ObPLVarDebugInfo*, common::ObIAllocator> variables_debuginfo_;
  common::ObFixedArray<int64_t, common::ObIAllocator> default_idxs_;
  common::ObFixedArray<ObPLSqlInfo, common::ObIAllocator> sql_infos_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> in_args_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> out_args_;
  ObFuncPtr action_;
  char *di_buf_;
  int64_t di_len_;
  bool is_all_sql_stmt_;
  bool is_invoker_right_;
  bool is_pipelined_;
  ObPLNameDebugInfo name_debuginfo_;

  common::ObString function_name_;
  common::ObString package_name_;
  common::ObString database_name_;
  common::ObString priv_user_;
  bool has_parallel_affect_factor_;

  DISALLOW_COPY_AND_ASSIGN(ObPLFunction);
};

struct ObPLExecRecursionCtx
{
public:
  ObPLExecRecursionCtx() : init_(false), max_recursion_depth_(0), recursion_depth_map_() {}
  ~ObPLExecRecursionCtx() {}
  int init(sql::ObSQLSessionInfo &session_info);
  int inc_and_check_depth(uint64_t package_id, uint64_t proc_id, bool is_function);
  int dec_and_check_depth(uint64_t package_id, uint64_t proc_id);
private:
  static const int64_t RECURSION_ARRAY_SIZE = 4;
  static const int64_t RECURSION_MAP_SIZE = 10;
private:
  bool init_;
  int64_t max_recursion_depth_;
  // To avoid hash map initialize cost, we store recursion depth in array
  // if routines less than RECURSION_ARRAY_SIZE.
  common::ObSEArray<std::pair<std::pair<uint64_t, uint64_t>, int64_t>, RECURSION_MAP_SIZE> recursion_depth_array_;
  common::hash::ObHashMap<std::pair<uint64_t, uint64_t>, int64_t> recursion_depth_map_;
};

struct ObPLSqlCodeInfo
{
public:
  ObPLSqlCodeInfo() : sqlcode_(OB_SUCCESS), sqlmsg_() {}
  inline void set_sqlcode(int sqlcode, const ObString &sqlmsg = ObString(""))
  {
    sqlcode_ = sqlcode;
    sqlmsg_ = sqlmsg;
  }
  inline void reset()
  {
    sqlcode_ = OB_SUCCESS;
    sqlmsg_ = ObString("");
    stakced_warning_buff_.reset();
  }
  inline void set_sqlmsg(const ObString &sqlmsg) { sqlmsg_ = sqlmsg; }
  inline int get_sqlcode() const { return sqlcode_; }
  inline const ObString& get_sqlmsg() const { return sqlmsg_; }
  inline common::ObIArray<ObWarningBuffer>& get_stack_warning_buf()
  {
    return stakced_warning_buff_;
  }
private:
  int sqlcode_;
  ObString sqlmsg_;
  common::ObSEArray<ObWarningBuffer, 4> stakced_warning_buff_;
};

struct ObPLException;
struct ObPLCtx
{
public:
  ObPLCtx() {}
  ~ObPLCtx();
  int add(ObObj &obj) {
    return objects_.push_back(obj);
  }
  void clear() {
    objects_.reset();
  }
  void reset_obj();
  void reset_obj_range_to_end(int64_t index);
  common::ObIArray<ObObj>& get_objects() { return objects_; }
private:
  // 用于收集在PL执行过程中使用到的Allocator,
  // 有些数据的生命周期是整个执行期的, 所以不能在PL执行完成后马上释放, 如OUT参数
  // 这里的Allocator需要保证由ObExecContext->allocator分配,
  // 这样才能保证释放这里的allocator时指针是有效的
  ObSEArray<ObObj, 32> objects_;
};

class ObPLPackageGuard;

#define IDX_PLEXECCTX_VTABLE 0
#define IDX_PLEXECCTX_ALLOCATOR 1
#define IDX_PLEXECCTX_CTX 2
#define IDX_PLEXECCTX_PARAMS 3
#define IDX_PLEXECCTX_RESULT 4
#define IDX_PLEXECCTX_STATUS 5
#define IDX_PLEXECCTX_FUNC 6
#define IDX_PLEXECCTX_IN 7
#define IDX_PLEXECCTX_PL_CTX 8

struct ObPLExecCtx : public ObPLINS
{
  ObPLExecCtx(common::ObIAllocator *allocator,
              sql::ObExecContext *exec_ctx,
              ParamStore *params,
              common::ObObj *result,
              int *status,
              ObPLFunction *func,
              bool in_function = false,
              const common::ObIArray<int64_t> *nocopy_params = NULL,
              ObPLPackageGuard *guard = NULL) :
    allocator_(allocator), exec_ctx_(exec_ctx), params_(params),
    result_(result), status_(status), func_(func),
    in_function_(in_function), pl_ctx_(NULL), nocopy_params_(nocopy_params), guard_(guard) {
      if (NULL != exec_ctx && NULL != exec_ctx_->get_my_session()) {
        pl_ctx_ = exec_ctx_->get_my_session()->get_pl_context();
      }
    }

  static uint32_t allocator_offset_bits() { return offsetof(ObPLExecCtx, allocator_) * 8; }
  static uint32_t exec_ctx_offset_bits() { return offsetof(ObPLExecCtx, exec_ctx_) * 8; }
  static uint32_t params_offset_bits() { return offsetof(ObPLExecCtx, params_) * 8; }
  static uint32_t result_offset_bits() { return offsetof(ObPLExecCtx, result_) * 8; }
  static uint32_t pl_ctx_offset_bits() { return offsetof(ObPLExecCtx, pl_ctx_) * 8; }

  bool valid();

  virtual int get_user_type(uint64_t type_id,
                                const ObUserDefinedType *&user_type,
                                ObIAllocator *allocator = NULL) const;
  virtual int calc_expr(uint64_t package_id, int64_t expr_idx, ObObjParam &result);

  common::ObIAllocator *allocator_;
  sql::ObExecContext *exec_ctx_;
  ParamStore *params_; // param stroe, 对应PL Function的符号表
  common::ObObj *result_;
  int *status_; //PL里面的sql发生错误会直接抛出异常，走不到函数正常结束的返回值返回错误码，所以要在这里记录错误码
  ObPLFunction *func_; // 对应该执行上下文的func_
  bool in_function_; //记录当前是否在function中
  ObPLContext *pl_ctx_; // for error stack
  const common::ObIArray<int64_t> *nocopy_params_; //用于描述nocopy参数
  ObPLPackageGuard *guard_; //对应该次执行的package_guard
};

// backup and restore ObExecContext attributes
struct ExecCtxBak
{
#define PL_EXEC_CTX_BAK_ATTRS phy_plan_ctx_,expr_op_ctx_store_,expr_op_size_,has_non_trivial_expr_op_ctx_,frames_,frame_cnt_

#define DEF_BACKUP_ATTR(x) typeof(sql::ObExecContext::x) x = 0
  LST_DO_CODE(DEF_BACKUP_ATTR, EXPAND(PL_EXEC_CTX_BAK_ATTRS));
#undef DEF_BACKUP_ATTR

  void backup(sql::ObExecContext &ctx);
  void restore(sql::ObExecContext &ctx);
};

class ObPLContext;
class ObPLExecState
{
public:
  ObPLExecState(common::ObIAllocator &allocator,
                sql::ObExecContext &ctx,
                ObPLPackageGuard &guard,
                ObPLFunction &func,
                common::ObObj &result,
                int &status,
                bool top_call = false,
                bool inner_call = false,
                bool in_function = false,
                const common::ObIArray<int64_t> *nocopy_params = NULL,
                uint64_t loc = 0,
                bool is_called_from_sql = false) :
    func_(func),
    phy_plan_ctx_(allocator),
    eval_ctx_(ctx),
    result_(result),
    ctx_(&allocator,
         &ctx,
         &phy_plan_ctx_.get_param_store_for_update(),
         &result,
         &status,
         &func_,
         in_function,
         nocopy_params,
         &guard),
    inner_call_(inner_call),
    top_call_(top_call),
    need_reset_physical_plan_(false),
    top_context_(NULL),
    current_line_(OB_INVALID_INDEX),
    loc_(loc),
    is_called_from_sql_(is_called_from_sql),
    dwarf_helper_(NULL),
    pure_sql_exec_time_(0),
    pure_plsql_exec_time_(0),
    pure_sub_plsql_exec_time_(0),
    profiler_time_stack_(nullptr)
  { }
  virtual ~ObPLExecState();

  int init(const ParamStore *params = NULL, bool is_anonymous = false);
  int check_routine_param_legal(ParamStore *params = NULL);
  int init_params(const ParamStore *params = NULL, bool is_anonymous = false);
  int execute();
  int final(int ret);
  int deep_copy_result_if_need();
  int init_complex_obj(common::ObIAllocator &allocator, const ObPLDataType &pl_type, common::ObObjParam &obj, bool set_null = true);
  inline const common::ObObj &get_result() const { return result_; }
  inline common::ObIAllocator *get_allocator() { return ctx_.allocator_; }
  inline const sql::ObPhysicalPlanCtx &get_physical_plan_ctx() const { return phy_plan_ctx_; }
  inline sql::ObPhysicalPlanCtx &get_physical_plan_ctx() { return phy_plan_ctx_; }
  inline const ParamStore &get_params() const { return phy_plan_ctx_.get_param_store(); }
  inline ParamStore &get_params() { return phy_plan_ctx_.get_param_store_for_update(); }
  ObPLFunction &get_function() { return func_; }
  int get_var(int64_t var_idx, ObObjParam& result);
  int set_var(int64_t var_idx, const ObObjParam& value);
  ObPLExecCtx& get_exec_ctx() { return ctx_; }
  int check_pl_execute_priv(ObSchemaGetterGuard &guard,
                                          const uint64_t tenant_id,
                                          const uint64_t user_id,
                                          const ObSchemaObjVersion &schema_obj,
                                          const ObIArray<uint64_t> &role_id_array);
  int check_pl_priv(share::schema::ObSchemaGetterGuard &guard,
                        const uint64_t tenant_id,
                        const uint64_t user_id,
                        const sql::DependenyTableStore &dep_obj);

  inline bool is_top_call() const { return top_call_; }
  inline uint64_t get_loc() const { return loc_; }
  inline void set_loc(uint64_t loc) { loc_ = loc; }
  inline uint64_t get_line_number() { return static_cast<uint64_t>(get_loc() >> 32 & 0xffffffff); }

  inline uint64_t get_current_line() { return get_line_number() + 1; }

  inline void set_current_line(int64_t current_line)
  {
    current_line = (current_line > 0) ? (current_line - 1) : current_line;
    set_loc((current_line) << 32 | (get_loc() & 0x00000000ffffffff));
  }

  inline bool is_called_from_sql() const { return is_called_from_sql_; }
  inline void set_is_called_from_sql(bool flag) { is_called_from_sql_ = flag; }

  inline void set_dwarf_helper(jit::ObDWARFHelper *dwarf_helper)
  {
    dwarf_helper_ = dwarf_helper;
  }
  inline jit::ObDWARFHelper* get_dwarf_helper() { return dwarf_helper_; } 

  inline void add_pure_sql_exec_time(int64_t sql_exec_time)
  {
    pure_sql_exec_time_ += sql_exec_time;
  }
  inline void reset_pure_sql_exec_time() { pure_sql_exec_time_ = 0; }

  int64_t get_pure_sql_exec_time() { return pure_sql_exec_time_; }

  int add_pl_exec_time(int64_t pl_exec_time, bool is_called_from_sql);

  void reset_plsql_exec_time() { pure_plsql_exec_time_ = 0; }
  void add_plsql_exec_time(int64_t plsql_exec_time) { pure_plsql_exec_time_ = plsql_exec_time; }
  int64_t get_plsql_exec_time() { return pure_plsql_exec_time_; }
  void add_sub_plsql_exec_time(int64_t sub_plsql_exec_time) { pure_sub_plsql_exec_time_ += sub_plsql_exec_time; }
  int64_t get_sub_plsql_exec_time() { return pure_sub_plsql_exec_time_; }
  void reset_sub_plsql_exec_time() { pure_sub_plsql_exec_time_ = 0; }

  inline void set_profiler_time_stack(ObPLProfilerTimeStack *time_stack) { profiler_time_stack_ = time_stack;}

  inline ObPLProfilerTimeStack *get_profiler_time_stack() { return profiler_time_stack_; }

  TO_STRING_KV(K_(inner_call),
               K_(top_call),
               K_(need_reset_physical_plan),
               K_(loc),
               K_(is_called_from_sql),
               K_(pure_sql_exec_time),
               K_(pure_plsql_exec_time),
               K_(pure_sub_plsql_exec_time));
private:
private:
  ObPLFunction &func_;
  sql::ObPhysicalPlanCtx phy_plan_ctx_; //运行态的param值放在这里面，跟ObPLFunction里的variables_一一对应，初始化的时候需要设置default值
  sql::ObEvalCtx eval_ctx_;
  common::ObObj &result_;
  ObPLExecCtx ctx_;
  bool inner_call_;
  bool top_call_;

  ExecCtxBak exec_ctx_bak_;
  bool need_reset_physical_plan_;

  ObPLContext *top_context_;

  int64_t current_line_;
  uint64_t loc_; // combine of line and column number
  bool is_called_from_sql_;
  jit::ObDWARFHelper *dwarf_helper_; // for decode dwarf debuginfo
  int64_t pure_sql_exec_time_;
  int64_t pure_plsql_exec_time_;
  int64_t pure_sub_plsql_exec_time_;
  ObPLProfilerTimeStack *profiler_time_stack_;
};

class ObPLContext
{
  friend class LinkPLStackGuard;
public:
  ObPLContext() { reset(); }
  virtual ~ObPLContext() { reset(); }
  void reset()
  {
    inc_recursion_depth_ = false;
    reset_autocommit_ = false;
    has_stash_savepoint_ = false;
    has_implicit_savepoint_ = false;
    has_inner_dml_write_ = false;
    is_top_stack_ = false;
    exception_handler_illegal_ = false;
    need_reset_exec_env_ = false;
    is_autonomous_ = false;
    saved_session_.reset();
    saved_has_implicit_savepoint_ = false;
    database_id_ = OB_INVALID_ID;
    need_reset_default_database_ = false;
    session_info_ = NULL;
    exec_stack_.reset();
    need_reset_role_id_array_ = false;
    old_role_id_array_.reset();
    old_priv_user_id_ = OB_INVALID_ID;
    old_in_definer_ = false;
    has_output_arguments_ = false;
#ifdef OB_BUILD_ORACLE_PL
    call_trace_.reset();
#endif
    old_worker_timeout_ts_ = 0;
    old_phy_plan_timeout_ts_ = 0;
    parent_stack_ctx_ = nullptr;
    top_stack_ctx_ = nullptr;
    my_exec_ctx_ = nullptr;
    cur_query_.reset();
    is_function_or_trigger_ = false;
    last_insert_id_ = 0;
    trace_id_.reset();
    old_user_priv_set_ = OB_PRIV_SET_EMPTY;
    old_db_priv_set_ = OB_PRIV_SET_EMPTY;
  }

  int is_inited() { return session_info_ != NULL; }

  int init(sql::ObSQLSessionInfo &session_info,
           sql::ObExecContext &ctx,
           ObPLFunction *routine,
           bool is_function_or_trigger,
           ObIAllocator *allocator = NULL,
           const bool is_dblink = false);
  void destory(sql::ObSQLSessionInfo &session_info, sql::ObExecContext &ctx, int &ret);

  inline ObPLCursorInfo& get_cursor_info() { return cursor_info_; }
  inline ObPLSqlCodeInfo& get_sqlcode_info() { return sqlcode_info_; }
  inline bool has_implicit_savepoint() { return has_implicit_savepoint_; }
  inline void clear_implicit_savepoint() { has_implicit_savepoint_ = false; }
  inline void set_has_implicit_savepoint(bool v) { has_implicit_savepoint_ = v; }

  bool is_top_stack() { return is_top_stack_; }

  inline bool is_exception_handler_illegal() const { return exception_handler_illegal_; }
  inline void set_exception_handler_illegal() { exception_handler_illegal_ = true; }
  inline void set_reset_autocommit() { reset_autocommit_ = true; }
  inline bool get_reset_autocommit() const { return reset_autocommit_; }

  static int valid_execute_context(sql::ObExecContext &ctx);
  static int check_stack_overflow();
  static int check_routine_legal(ObPLFunction &routine, bool in_function, bool in_tg);

  static int debug_start(sql::ObSQLSessionInfo *sql_session);
  static int debug_stop(sql::ObSQLSessionInfo *sql_session);
  static int notify(sql::ObSQLSessionInfo *sql_session);

  static int get_exec_state_from_local(sql::ObSQLSessionInfo &session_info,
                                    int64_t package_id,
                                    int64_t routine_id,
                                    ObPLExecState *&plstate);
  static int get_param_store_from_local(ObSQLSessionInfo &session_info,
                                        int64_t package_id,
                                        int64_t routine_id,
                                        ParamStore *&params);
  static int get_routine_from_local(sql::ObSQLSessionInfo &session_info,
                                    int64_t package_id,
                                    int64_t routine_id,
                                    ObPLFunction *&routine);
  static int get_subprogram_var_from_local(sql::ObSQLSessionInfo &session_info,
                                    int64_t package_id,
                                    int64_t routine_id,
                                    int64_t var_idx,
                                    ObObjParam &result);
  static int set_subprogram_var_from_local(sql::ObSQLSessionInfo &session_info,
                                    int64_t package_id,
                                    int64_t routine_id,
                                    int64_t var_idx,
                                    const ObObjParam &value);
  static int check_debug_priv(ObSchemaGetterGuard *guard,
                              sql::ObSQLSessionInfo *sess_info,
                              ObPLFunction *func);

  int inc_and_check_depth(int64_t package_id, int64_t routine_id, bool is_function);
  void dec_and_check_depth(int64_t package_id, int64_t routine_id, int &ret, bool inner_call);

  int set_exec_env(ObPLFunction &routine);
  int set_default_database(ObPLFunction &routine, share::schema::ObSchemaGetterGuard &guard);
  void reset_exec_env(int &ret);
  void reset_default_database(int &ret);

  int set_role_id_array(ObPLFunction &routine, share::schema::ObSchemaGetterGuard &guard);
  void reset_role_id_array(int &ret);

  ObIArray<ObPLExecState *> &get_exec_stack() { return exec_stack_; }
#ifdef OB_BUILD_ORACLE_PL
  ObIArray<DbmsUtilityHelper::BtInfo*> &get_error_trace() { return call_trace_.error_trace; }
  ObIArray<DbmsUtilityHelper::BtInfo*> &get_call_stack() { return call_trace_.call_stack; }
  void set_call_trace_error_code(int errcode) { call_trace_.err_code = errcode; }
  int get_call_trace_error_code() const { return call_trace_.err_code; }
#endif
  ObPLExecState *get_current_state()
  {
    return exec_stack_.empty() ? NULL : exec_stack_.at(exec_stack_.count() - 1);
  }
  ObPLFunction *get_current_routine()
  {
    return NULL == get_current_state() ? NULL : &get_current_state()->get_function();
  }
  ObPLExecCtx *get_current_ctx()
  {
    return NULL == get_current_state() ? NULL : &get_current_state()->get_exec_ctx();
  }
#ifdef OB_BUILD_ORACLE_PL
  static int get_exact_error_msg(ObIArray<DbmsUtilityHelper::BtInfo*> &error_trace,
                                   ObIArray<DbmsUtilityHelper::BtInfo*> &call_stack,
                                   common::ObSqlString &err_msg);
#endif
  bool has_output_arguments() { return has_output_arguments_; }
  void set_has_output_arguments(bool has_output_arguments)
  {
    has_output_arguments_ = has_output_arguments;
  }
  static int implicit_end_trans(
    sql::ObSQLSessionInfo &session, sql::ObExecContext &ctx, bool is_rollback, bool can_async = false);

  inline ObString get_database_name() const { return database_name_.string(); }
  inline uint64_t get_database_id() const { return database_id_; }
  inline bool is_function_or_trigger() const { return is_function_or_trigger_; }
  bool is_autonomous() const { return is_autonomous_; }
  void clear_autonomous() { is_autonomous_ = false; }
  bool in_autonomous() const;
  int end_autonomous(ObExecContext &ctx, sql::ObSQLSessionInfo &session_info);
  bool in_nested_sql_ctrl() const
  { return ObStmt::is_dml_stmt(my_exec_ctx_->get_sql_ctx()->stmt_type_) && !in_autonomous(); }
  pl::ObPLContext *get_parent_stack_ctx() { return parent_stack_ctx_; }
  pl::ObPLContext *get_top_stack_ctx() { return top_stack_ctx_; }
  sql::ObExecContext *get_my_exec_ctx() { return my_exec_ctx_; }
  ObCurTraceId::TraceId get_trace_id() const { return trace_id_; }

private:
  ObPLContext* get_stack_pl_ctx();
  void set_parent_stack_ctx(pl::ObPLContext *parent_stack_ctx)
  {
    parent_stack_ctx_ = parent_stack_ctx;
    top_stack_ctx_ = (parent_stack_ctx == nullptr) ? this : parent_stack_ctx->get_top_stack_ctx();
  }
  void set_my_exec_ctx(sql::ObExecContext *my_exec_ctx) { my_exec_ctx_ = my_exec_ctx; }
  static void record_tx_id_before_begin_autonomous_session_for_deadlock_(ObSQLSessionInfo &session_info,
                                                                         transaction::ObTransID &last_trans_id);
  static void register_after_begin_autonomous_session_for_deadlock_(ObSQLSessionInfo &session_info,
                                                                    const transaction::ObTransID last_trans_id);
private:
  ObPLCursorInfo cursor_info_;
  ObPLSqlCodeInfo sqlcode_info_;
  ObPLExecRecursionCtx recursion_ctx_;
  bool inc_recursion_depth_;
  bool reset_autocommit_;
  bool has_stash_savepoint_;
  bool has_implicit_savepoint_;
  bool has_inner_dml_write_;
  bool is_top_stack_;
  bool is_autonomous_;
  sql::ObBasicSessionInfo::TransSavedValue saved_session_;
  bool saved_has_implicit_savepoint_;
  bool exception_handler_illegal_;

  sql::ObExecEnv exec_env_;
  bool need_reset_exec_env_;
  ObSqlString database_name_;
  uint64_t database_id_;
  common::ObSEArray<uint64_t, 8> old_role_id_array_;
  uint64_t old_priv_user_id_;
  ObPrivSet old_user_priv_set_;
  ObPrivSet old_db_priv_set_;
  bool old_in_definer_;
  bool need_reset_default_database_;
  bool need_reset_role_id_array_;

  bool has_output_arguments_;

  int64_t old_worker_timeout_ts_;
  int64_t old_phy_plan_timeout_ts_;

  sql::ObSQLSessionInfo *session_info_;

  common::ObString cur_query_;

  common::ObSEArray<ObPLExecState*, 4> exec_stack_;
#ifdef OB_BUILD_ORACLE_PL
  DbmsUtilityHelper::BackTrace call_trace_;
#endif
  ObPLContext *parent_stack_ctx_;
  ObPLContext *top_stack_ctx_;
  sql::ObExecContext *my_exec_ctx_; //my exec context
  bool is_function_or_trigger_;
  uint64_t last_insert_id_;
  ObCurTraceId::TraceId trace_id_;
};

struct PlTransformTreeCtx
{
  ObIAllocator *allocator_;
  ParamStore *params_;
  char *buf_; // 反拼后的参数化字符串
  int64_t buf_len_;
  int64_t buf_size_;
  ObString raw_sql_; // 原始匿名块字符串
  int64_t raw_anonymous_off_; // 原始匿名块相对于用户输入首字符的偏移, 解决单个分隔符内存在多个sql场景
  ObString raw_sql_or_expr_; // 匿名块内部单个expr或者sql原始字符串
  ObString no_param_sql_; // 匿名块内部单个expr或者sql对应的fast parser后字符串
  int64_t copied_idx_;
  ParamList *p_list_; // 存储匿名块内部所有expr和sql语句fast parser后得到的raw param node
  int64_t raw_param_num_; // 匿名块内部单个expr或者sql fast parser后raw param node的个数, 每个expr和sql fast parser后, 会将param num存储在node节点中
  PlTransformTreeCtx() :
    allocator_(NULL),
    params_(NULL),
    buf_(NULL),
    buf_len_(0),
    buf_size_(0),
    raw_sql_(),
    raw_anonymous_off_(0),
    raw_sql_or_expr_(),
    no_param_sql_(),
    copied_idx_(0),
    p_list_(NULL),
    raw_param_num_(0)
  {}
};

class ObPL
{
  friend class sql::ObAlterRoutineResolver;
public:
  ObPL() :
    sql_proxy_(NULL),
    package_manager_(),
    interface_service_(),
    codegen_lock_() {}
  virtual ~ObPL() {}

  int init(common::ObMySQLProxy &sql_proxy);
  void destory();

public:
  // for anonymous + ps
  int execute(sql::ObExecContext &ctx,
              ParamStore &params,
              uint64_t stmt_id,
              const common::ObString &sql,
              ObBitSet<OB_DEFAULT_BITSET_SIZE> &out_args);
  int parameter_anonymous_block(ObExecContext &ctx,
                              const ObStmtNodeTree *block,
                              ParamStore &params,
                              ObIAllocator &allocator,
                              ObCacheObjGuard &cacheobj_guard);
  int transform_tree(PlTransformTreeCtx &trans_ctx, ParseNode *block, ParseNode *no_param_root, ObExecContext &ctx, ParseResult &parse_result);
  int trans_sql(PlTransformTreeCtx &trans_ctx, ParseNode *root, ObExecContext &ctx);
  // for anonymous
  int execute(sql::ObExecContext &ctx,
              ParamStore &params,
              const ObStmtNodeTree *block);

  // for normal routine or package routine
  int execute(sql::ObExecContext &ctx,
              ObIAllocator &allocator,
              uint64_t package_id,
              uint64_t routine_id,
              const ObIArray<int64_t> &subprogram_path,
              ParamStore &params,
              const ObIArray<int64_t> &nocopy_params,
              common::ObObj &result,
              int *status = NULL,
              bool inner_call = false,
              bool in_function = false,
              uint64_t loc = 0,
              bool is_called_from_sql = false,
              uint64_t dblink_id = OB_INVALID_ID);
  int check_exec_priv(sql::ObExecContext &ctx,
                      const ObString &database_name,
                      ObPLFunction *routine);

private:
  // for normal routine
  int get_pl_function(sql::ObExecContext &ctx,
                      ObPLPackageGuard &package_guard,
                      int64_t package_id,
                      int64_t routine_id,
                      const ObIArray<int64_t> &subprogram_path,
                      ObCacheObjGuard& cacheobj_guard,
                      ObPLFunction *&local_routine);

  // for anonymous + ps
  int get_pl_function(sql::ObExecContext &ctx,
                      ParamStore &params,
                      uint64_t stmt_id,
                      const ObString &sql,
                      ObCacheObjGuard& cacheobj_guard);

  // for anonymous + ps
  int generate_pl_function(sql::ObExecContext &ctx,
                           const ObString &anonymouse_sql,
                           ParamStore &params,
                           ParseNode &node,
                           ObCacheObjGuard& cacheobj_guard,
                           const uint64_t stmt_id,
                           bool is_anonymous_text = false);

  // for inner common execute
  int execute(sql::ObExecContext &ctx,
              ObIAllocator &allocator,
              ObPLPackageGuard &package_guard,
              ObPLFunction &routine,
              ParamStore *params,
              const ObIArray<int64_t> *nocopy_params,
              ObObj *result,
              int *status = NULL,
              bool is_top_stack = false,
              bool is_inner_call = false,
              bool is_in_function = false,
              bool is_anonymous = false,
              uint64_t loc = 0,
              bool is_called_from_sql = false);

public:
  // for normal routine
  static int generate_pl_function(sql::ObExecContext &ctx,
                           uint64_t proc_id,
                           ObCacheObjGuard& cacheobj_guard);
  // add pl to cache
  static int add_pl_lib_cache(ObPLFunction *pl_func, ObPLCacheCtx &pc_ctx);
  static int execute_proc(ObPLExecCtx &ctx,
                          uint64_t package_id,
                          uint64_t proc_id,
                          int64_t *subprogram_path,
                          int64_t path_length,
                          uint64_t line_num, /* call position line number, for call_stack info*/
                          int64_t argc,
                          common::ObObjParam **argv,
                          int64_t *nocopy_argv,
                          uint64_t dblink_id);

  static int set_user_type_var(ObPLExecCtx *ctx,
                               int64_t var_index,
                               int64_t var_addr,
                               int64_t init_size);

  static int set_implicit_cursor_in_forall(ObPLExecCtx *ctx, bool save_exception);
  static int unset_implicit_cursor_in_forall(ObPLExecCtx *ctx);

  inline ObPLPackageManager &get_package_manager() { return package_manager_; }
  inline common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  inline const ObPLInterfaceService &get_interface_service() const { return interface_service_; }
  static int insert_error_msg(int errcode);

  static int simple_execute(ObPLExecCtx *ctx, int64_t argc, int64_t *argv);

  static int check_trigger_arg(const ParamStore &params, const ObPLFunction &func);

  std::pair<common::ObBucketLock, common::ObBucketLock>& get_jit_lock() { return jit_lock_; }

  static int check_session_alive(const ObBasicSessionInfo &session);

private:
  common::ObMySQLProxy *sql_proxy_;
  ObPLPackageManager package_manager_;
  ObPLInterfaceService interface_service_;
  common::ObBucketLock codegen_lock_;

  // first bucket is for deduplication, second bucket is for concurrency control
  std::pair<common::ObBucketLock, common::ObBucketLock> jit_lock_;
};

class LinkPLStackGuard
{
public:
  LinkPLStackGuard(sql::ObExecContext &exec_ctx, ObPLContext &pl_stack)
    : exec_ctx_(exec_ctx),
      parent_stack_(nullptr)
  {
    //last_pl_stack means the last pl stack in all execution stack
    //such as: pl_stack1->sql_stack->pl_stack2, pl_stack2.last_pl_stack is pl_stack1
    ObPLContext *last_pl_stack = nullptr;
    //parent_stack means the direct parent pl stack in current execution stack
    //such as: pl_stack1->sql_stack->pl_stack2, pl_stack2.parent_stack is nullptr
    ObExecContext *cur_exec_ctx = &exec_ctx;
    while (cur_exec_ctx != nullptr && last_pl_stack == nullptr) {
      if (cur_exec_ctx->get_pl_stack_ctx() != nullptr) {
        last_pl_stack = cur_exec_ctx->get_pl_stack_ctx();
        if (cur_exec_ctx == &exec_ctx) {
          parent_stack_ = cur_exec_ctx->get_pl_stack_ctx();
        }
      } else {
        //find the parent execution stack
        cur_exec_ctx = cur_exec_ctx->get_parent_ctx();
      }
    }
    pl_stack.set_parent_stack_ctx(last_pl_stack);
    pl_stack.set_my_exec_ctx(&exec_ctx_);
    exec_ctx_.set_pl_stack_ctx(&pl_stack);
  }
  ~LinkPLStackGuard()
  {
    //pop the pl stack from current execution stack
    exec_ctx_.set_pl_stack_ctx(parent_stack_);
  }
private:
  sql::ObExecContext &exec_ctx_;
  ObPLContext *parent_stack_;
};
}
}
#endif /* OCEANBASE_SRC_PL_OB_PL_H_ */
