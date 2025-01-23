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

#ifndef OCEANBASE_SRC_PL_OB_PL_CODE_GENERATOR_H_
#define OCEANBASE_SRC_PL_OB_PL_CODE_GENERATOR_H_

#include "objit/ob_llvm_helper.h"
#include "objit/ob_llvm_di_helper.h"
#include "ob_pl_stmt.h"
#include "ob_pl_adt_service.h"
#include "ob_pl_exception_handling.h"
#include "ob_pl_di_adt_service.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase {
using sql::ObSqlExpression;
namespace pl {

class ObPLCGBufferGuard;

class ObPLCodeGenerator
{
friend class ObPLCGBufferGuard;

public:
  static const int64_t RET_IDX = 0;
  static const int64_t CTX_IDX = 1;
  static const int64_t ARGC_IDX = 2;
  static const int64_t ARGV_IDX = 3;
  static const int64_t USER_ARG_OFFSET = 4;
  const char *ArgName[USER_ARG_OFFSET]
  {
    "__ret__",
    "__hidden_exec_ctx__",
    "__argc__",
    "__argv__",
  };

  static const int64_t EH_STACK_DEPTH = 64;
  static const int64_t LABEL_STACK_DEPTH = 64;
  static const int64_t LOOP_STACK_DEPTH = 64;

  static const int64_t EARLY_EXIT_CHECK_CNT = 10000;

  enum goto_label_flag {
    GOTO_LABEL_INVALID = -1,
    GOTO_LABEL_EXIST,
    GOTO_LABEL_NONEXIST,
    GOTO_LABEL_CG,
  };
  typedef common::hash::ObHashMap<int64_t,
                  common::hash::HashMapPair<goto_label_flag,  std::pair<jit::ObLLVMBasicBlock, jit::ObLLVMBasicBlock>>,
                  common::hash::NoPthreadDefendMode> goto_label_map;

public:
  struct ObPLSPIService
  {
    jit::ObLLVMFunction spi_calc_expr_at_idx_;
    jit::ObLLVMFunction spi_calc_package_expr_;
    jit::ObLLVMFunction spi_set_variable_to_expr_;
    jit::ObLLVMFunction spi_query_into_expr_idx_;
    jit::ObLLVMFunction spi_execute_with_expr_idx_;
    jit::ObLLVMFunction spi_execute_immediate_;
    jit::ObLLVMFunction spi_extend_collection_;
    jit::ObLLVMFunction spi_delete_collection_;
    jit::ObLLVMFunction spi_cursor_init_;
    jit::ObLLVMFunction spi_cursor_open_with_param_idx_;
    jit::ObLLVMFunction spi_dynamic_open_;
    jit::ObLLVMFunction spi_cursor_fetch_;
    jit::ObLLVMFunction spi_cursor_close_;
    jit::ObLLVMFunction spi_destruct_collection_;
    jit::ObLLVMFunction spi_reset_composite_;
    jit::ObLLVMFunction spi_copy_datum_;
    jit::ObLLVMFunction spi_destruct_obj_;
    jit::ObLLVMFunction spi_sub_nestedtable_;
    jit::ObLLVMFunction spi_alloc_complex_var_;
    jit::ObLLVMFunction spi_construct_collection_;
    jit::ObLLVMFunction spi_clear_diagnostic_area_;
    jit::ObLLVMFunction spi_end_trans_;
    jit::ObLLVMFunction spi_update_location_;
    jit::ObLLVMFunction spi_set_pl_exception_code_;
    jit::ObLLVMFunction spi_get_pl_exception_code_;
    jit::ObLLVMFunction spi_check_early_exit_;
    jit::ObLLVMFunction spi_convert_objparam_;
    jit::ObLLVMFunction spi_raise_application_error_;
    jit::ObLLVMFunction spi_pipe_row_to_result_;
    jit::ObLLVMFunction spi_check_exception_handler_legal_;
    jit::ObLLVMFunction spi_trim_collection_;
    jit::ObLLVMFunction spi_interface_impl_;
    jit::ObLLVMFunction spi_process_nocopy_params_;
    jit::ObLLVMFunction spi_add_ref_cursor_refcount_;
    jit::ObLLVMFunction spi_handle_ref_cursor_refcount_;
    jit::ObLLVMFunction spi_update_package_change_info_;
    jit::ObLLVMFunction spi_check_composite_not_null_;
    jit::ObLLVMFunction spi_process_resignal_error_;
    jit::ObLLVMFunction spi_check_autonomous_trans_;
    jit::ObLLVMFunction spi_opaque_assign_null_;
    jit::ObLLVMFunction spi_pl_profiler_before_record_;
    jit::ObLLVMFunction spi_pl_profiler_after_record_;
    jit::ObLLVMFunction spi_init_composite_;
    jit::ObLLVMFunction spi_get_parent_allocator_;
    jit::ObLLVMFunction spi_get_current_expr_allocator_;
  };

  struct EHStack
  {
  public:
    struct EHInfo
    {
      jit::ObLLVMBasicBlock exception_;
      jit::ObLLVMBasicBlock exit_;
      int64_t level_;
      jit::ObLLVMBasicBlock raising_block_;
    };

    EHStack() : exceptions_(), cur_(0) {}
    virtual ~EHStack() {}
    EHInfo exceptions_[EH_STACK_DEPTH];
    int64_t cur_;
  };

  struct LabelStack
  {
  public:
    struct LabelInfo
    {
      common::ObString name_;
      int64_t level_;
      jit::ObLLVMBasicBlock start_;
      jit::ObLLVMBasicBlock exit_;
    };

    LabelStack() : labels_(), cur_(0) {}
    virtual ~LabelStack() {}
    LabelInfo labels_[LABEL_STACK_DEPTH];
    int64_t cur_;
  };

  struct LoopStack
  {
  public:
    struct LoopInfo
    {
      int64_t level_;
      jit::ObLLVMBasicBlock start_;
      jit::ObLLVMBasicBlock exit_;
      const ObPLCursorForLoopStmt *cursor_;
      jit::ObLLVMValue count_;
      jit::ObLLVMValue index_;
    };

    LoopStack() : loops_(), cur_(0) {}
    virtual ~LoopStack() {}
    LoopInfo loops_[LOOP_STACK_DEPTH];
    int64_t cur_;
  };

public:
  ObPLCodeGenerator(common::ObIAllocator &allocator,
                    sql::ObSQLSessionInfo &session_info,
                    share::schema::ObSchemaGetterGuard &schema_guard,
                    ObPLCompileUnitAST &func_ast,
                    common::ObIArray<ObSqlExpression*> &exprs,
                    jit::ObLLVMHelper &helper,
                    jit::ObLLVMDIHelper &di_helper,
                    bool oracle_mode)
  :
    allocator_(allocator),
    session_info_(session_info),
    schema_guard_(schema_guard),
    ast_(func_ast),
    exprs_(exprs),
    helper_(helper),
    func_(),
    entry_(),
    exit_(),
    current_(),
    exception_stack_(),
    label_stack_(),
    loop_stack_(),
    adt_service_(helper),
    spi_service_(),
    eh_service_(),
    pl_execute_(),
    set_user_type_var_(),
    set_implicit_cursor_in_forall_(),
    unset_implicit_cursor_in_forall_(),
    user_type_map_(),
    saved_ob_error_(),
    saved_exception_(),
    vars_(allocator),
    di_helper_(di_helper),
    di_adt_service_(di_helper),
    di_user_type_map_(),
    debug_mode_(session_info_.is_pl_debug_on() && func_ast.is_routine()),
    oracle_mode_(oracle_mode),
    out_params_(allocator),
    profile_mode_(session_info_.get_pl_profiler() != nullptr),
    global_strings_(),
    int_buffer_(allocator),
    objparam_buffer_(allocator)
    { }

  virtual ~ObPLCodeGenerator() {}

  int init();
  int generate(ObPLFunction &pl_func);
  int generate(ObPLPackage &pl_package);

  int generate_normal(ObPLFunction &pl_func);
  int generate_simple(ObPLFunction &pl_func);

  int generate_global_string(const ObString &string, jit::ObLLVMValue &str, jit::ObLLVMValue &len);
  int generate_string(const ObString &string, jit::ObLLVMValue &str, jit::ObLLVMValue &len);
  int generate_empty_string(jit::ObLLVMValue &str, jit::ObLLVMValue &len);
  int generate_null(ObObjType type, jit::ObLLVMValue &value);
  int generate_null_pointer(ObObjType type, jit::ObLLVMValue &value);
  int generate_int64_array(const ObIArray<int64_t> &array, jit::ObLLVMValue &result);
  int generate_uint64_array(const ObIArray<uint64_t> &array, jit::ObLLVMValue &result);
  int generate_int8_array(const ObIArray<int8_t> &array, jit::ObLLVMValue &result);
  int generate_expr(int64_t expr_idx, const ObPLStmt &s, int64_t result_idx, jit::ObLLVMValue &p_result_obj);
  int generate_early_exit(jit::ObLLVMValue &count, int64_t stmt_id, bool in_notfound, bool in_warning);
  int generate_pointer(const void *ptr, jit::ObLLVMValue &value);
  int generate_expression_array(const ObIArray<int64_t> &exprs, jit::ObLLVMValue &value, jit::ObLLVMValue &count);
  int generate_loop_control(const ObPLLoopControl &control);
  int generate_sql(const ObPLSql &sql,
                   jit::ObLLVMValue &str,
                   jit::ObLLVMValue &length,
                   jit::ObLLVMValue &ps_sql,
                   jit::ObLLVMValue &type,
                   jit::ObLLVMValue &for_update,
                   jit::ObLLVMValue &hidden_rowid,
                   jit::ObLLVMValue &params,
                   jit::ObLLVMValue &count,
                   jit::ObLLVMValue &skip_locked);
  int generate_into(const ObPLInto &into,
                    ObPLCGBufferGuard &buffer_guard,
                    jit::ObLLVMValue &into_array_value,
                    jit::ObLLVMValue &into_count_value,
                    jit::ObLLVMValue &type_array_value,
                    jit::ObLLVMValue &type_count_value,
                    jit::ObLLVMValue &exprs_not_null_array_value,
                    jit::ObLLVMValue &pl_integer_range_array_value,
                    jit::ObLLVMValue &is_bulk);
  int generate_into_restore(const ObIArray<int64_t> &into, const common::ObIArray<sql::ObRawExpr*> *exprs, const ObPLSymbolTable *symbol_table);
  int generate_exception(jit::ObLLVMValue &type,
                         jit::ObLLVMValue &ob_error_code,
                         jit::ObLLVMValue &error_code,
                         jit::ObLLVMValue &sql_state,
                         jit::ObLLVMValue &str_len,
                         jit::ObLLVMValue &level,
                         jit::ObLLVMBasicBlock &normal,
                         jit::ObLLVMValue &line_number,
                         bool in_notfound,
                         bool in_warning,
                         bool signal);
  int generate_close_loop_cursor(bool is_from_exception, int64_t dest_level);
  int generate_destruct_out_params();
  int raise_exception(jit::ObLLVMValue &exception,
                      jit::ObLLVMValue &error_code,
                      jit::ObLLVMValue &sql_staten,
                      jit::ObLLVMBasicBlock &normal,
                      bool in_notfound,
                      bool in_warning,
                      bool signal);
  int generate_declare_cursor(const ObPLStmt &s, const int64_t  &cursor_index);
  int generate_open(const ObPLStmt &s,
                    const ObPLSql &cursor_sql,
                    const uint64_t package_id,
                    const uint64_t routine_id,
                    const int64_t cursor_index);
  int generate_open_for(const ObPLOpenForStmt &s);
  int generate_fetch(const ObPLStmt &s,
                     const ObPLInto &into,
                     const uint64_t &package_id,
                     const uint64_t &routine_id,
                     const int64_t &cursor_index,
                     const int64_t &limit,
                     const ObUserDefinedType *user_defined_type,
                     jit::ObLLVMValue &ret_err);
  int generate_close(const ObPLStmt &s,
                     const uint64_t &package_id,
                     const uint64_t &routine_id,
                     const int64_t &cursor_index,
                     bool ignore = false, //是否忽略未打开的游标，不忽略的情况下遇到未打开的游标会报错，默认不忽略
                     bool exception = true); //在关闭过程中遇到错误是否抛出exception，默认抛出
  int generate_check_not_null(const ObPLStmt &s,
                              bool is_not_null,
                              jit::ObLLVMValue &p_result_obj);
  int generate_collection_check_not_null(const ObPLStmt &s,
                              const ObPLDataType &ob_coll_type,
                              jit::ObLLVMValue &p_result_obj);

  int generate_bound_and_check(const ObPLForLoopStmt &s,
                               bool is_forall,
                               jit::ObLLVMValue &lower_value,
                               jit::ObLLVMValue &upper_value,
                               jit::ObLLVMValue &lower_obj,
                               jit::ObLLVMValue &upper_obj,
                               jit::ObLLVMBasicBlock &illegal_range_block);
  int generate_indices_with_between_bound(const ObPLForLoopStmt &s,
                              jit::ObLLVMValue &p_lower_obj);
  int generate_next_and_check(const ObPLForLoopStmt &s,
                              jit::ObLLVMValue &p_index_obj,
                              jit::ObLLVMValue &p_index_value,
                              jit::ObLLVMValue &index_obj,
                              jit::ObLLVMValue &index_value,
                              jit::ObLLVMValue &dest_datum,
                              jit::ObLLVMValue &lower_value,
                              jit::ObLLVMValue &upper_value,
                              jit::ObLLVMValue &is_true);
  int generate_expr_next_and_check(const ObPLForLoopStmt &s,
                              jit::ObLLVMValue &index_obj,
                              jit::ObLLVMValue &index_value,
                              jit::ObLLVMValue &dest_datum,
                              jit::ObLLVMValue &upper_value,
                              jit::ObLLVMValue &is_true);
  int generate_normal_next_and_check(const ObPLForLoopStmt &s,
                              jit::ObLLVMValue &p_index_obj,
                              jit::ObLLVMValue &p_index_value,
                              jit::ObLLVMValue &index_obj,
                              jit::ObLLVMValue &index_value,
                              jit::ObLLVMValue &dest_datum,
                              jit::ObLLVMValue &lower_value,
                              jit::ObLLVMValue &upper_value,
                              jit::ObLLVMValue &is_true);

  int generate_sql(const ObPLSqlStmt &s, jit::ObLLVMValue &ret_err);
  int generate_after_sql(const ObPLSqlStmt &s, jit::ObLLVMValue &ret_err);
  int generate_reset_objparam(jit::ObLLVMValue &result, int64_t udt_id = OB_INVALID_ID, int8_t actual_type = 0, int8_t extend_type = -1);
  int check_success(jit::ObLLVMValue &ret_err,
                    int64_t stmt_id = OB_INVALID_ID,
                    bool in_notfound = false,
                    bool in_warning = false,
                    bool signal = false);
  int finish_current(const jit::ObLLVMBasicBlock &next);
  jit::ObLLVMValue stack_save();
  void stack_restore(jit::ObLLVMValue &stack);
  int generate_user_type(const ObUserDefinedType &type);
  int generate_obj_access_expr();
  int generate_set_variable(int64_t expr, jit::ObLLVMValue &value, bool is_default, int64_t stmt_id, bool in_notfound, bool in_warning);
  common::ObIAllocator &get_allocator() { return allocator_; }
  const ObSqlExpression *get_expr(int64_t i) const { return i < 0 || i >= exprs_.count() ? NULL : exprs_.at(i); }
  ObSqlExpression *get_expr(int64_t i) { return i < 0 || i >= exprs_.count() ? NULL : exprs_.at(i); }
  int generate_goto_label(const ObPLStmt &stmt);
  int generate_destruct_obj(const ObPLStmt &s, jit::ObLLVMValue &src_datum);
  int generate_out_param(
    const ObPLStmt &s,
    const ObIArray<InOutParam> &param_desc,
    jit::ObLLVMValue &params,
    int64_t i);
  int generate_out_params(
    const ObPLStmt &s,
    const ObIArray<InOutParam> &param_desc,
    jit::ObLLVMValue &params);
  int generate_update_package_changed_info(
    const ObPLStmt &s, uint64_t package_id, uint64_t var_idx);
  int restart_cg_when_goto_dest(const ObPLStmt &stmt);
  int generate_update_location(const ObPLStmt &s);
public:
  inline jit::ObLLVMHelper &get_helper() { return helper_; }
  inline jit::ObLLVMBasicBlock &get_entry() { return entry_; }
  inline jit::ObLLVMBasicBlock &get_exit() { return exit_; }
  inline jit::ObLLVMBasicBlock &get_current() { return current_; }
  inline int set_current(const jit::ObLLVMBasicBlock &block)
  {
    int ret = common::OB_SUCCESS;
    current_ = block;
    if (NULL != block.get_v()) {
      ret = helper_.set_insert_point(block);
    }
    return ret;
  }

  inline const EHStack::EHInfo *get_parent_exception() const { return exception_stack_.cur_ < 2 ? NULL : &exception_stack_.exceptions_[exception_stack_.cur_ - 2]; }
  inline const EHStack::EHInfo *get_current_exception() const { return exception_stack_.cur_ < 1 ? NULL : &exception_stack_.exceptions_[exception_stack_.cur_ - 1]; }
  inline int64_t get_exception_depth() const { return exception_stack_.cur_; }
  inline const EHStack::EHInfo *get_exception(int64_t idx) const { return exception_stack_.cur_ - 1 < idx ? NULL : &exception_stack_.exceptions_[idx]; }
  inline EHStack::EHInfo *get_exception(int64_t idx) { return exception_stack_.cur_ - 1 < idx ? NULL : &exception_stack_.exceptions_[idx]; }
  inline int set_exception(jit::ObLLVMBasicBlock &block,
                           jit::ObLLVMBasicBlock &exit,
                           int64_t level)
  {
    int ret = common::OB_SUCCESS;
    if (exception_stack_.cur_ < EH_STACK_DEPTH - 1) {
      exception_stack_.exceptions_[exception_stack_.cur_].exception_ = block;
      exception_stack_.exceptions_[exception_stack_.cur_].exit_ = exit;
      exception_stack_.exceptions_[exception_stack_.cur_].level_ = level;
      exception_stack_.exceptions_[exception_stack_.cur_].raising_block_.reset();
      ++exception_stack_.cur_;
    } else {
      ret = OB_NOT_SUPPORTED;
      PL_LOG(WARN, "max exception block nested level exceeded", K(exception_stack_.cur_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "exception blocks nested level exceed 64");
    }
    return ret;
  }

  inline int reset_exception()
  {
    int ret = common::OB_SUCCESS;
    if (exception_stack_.cur_ > 0) {
      --exception_stack_.cur_;
    } else {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  inline const LabelStack::LabelInfo *get_label(const common::ObString &name) const
  {
    const LabelStack::LabelInfo *label = NULL;
    for (int64_t i = label_stack_.cur_; NULL == label && i > 0; --i) {
      if (0 == label_stack_.labels_[i - 1].name_.case_compare(name)) {
        label = &label_stack_.labels_[i - 1];
      }
    }
    return label;
  }

  inline int set_label(const ObPLStmt &s, jit::ObLLVMBasicBlock &start, jit::ObLLVMBasicBlock &exit)
  {
    int ret = common::OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < s.get_label_cnt(); ++i) {
      ret = set_label(s.get_label(s.get_label_idx(i)), s.get_level(), start, exit);
    }
    return ret;
  }

  inline int set_label(const common::ObString *name, int64_t level, jit::ObLLVMBasicBlock &start, jit::ObLLVMBasicBlock &exit)
  {
    int ret = common::OB_SUCCESS;
    if (label_stack_.cur_ < LABEL_STACK_DEPTH - 1) {
      label_stack_.labels_[label_stack_.cur_].name_ = (NULL == name) ? ObString() : *name;
      label_stack_.labels_[label_stack_.cur_].level_ = level;
      label_stack_.labels_[label_stack_.cur_].start_ = start;
      label_stack_.labels_[label_stack_.cur_].exit_ = exit;
      ++label_stack_.cur_;
    } else {
      ret = OB_NOT_SUPPORTED;
      PL_LOG(WARN, "max label nested level exceeded", K(label_stack_.cur_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "labels nested level exceeds 64");
    }
    return ret;
  }
  inline int reset_label()
  {
    int ret = common::OB_SUCCESS;
    int64_t new_cur = label_stack_.cur_ - 1;
    while (new_cur > 0) {
      if (label_stack_.labels_[label_stack_.cur_].level_ == label_stack_.labels_[new_cur].level_
          && label_stack_.labels_[label_stack_.cur_].start_.get_v() == label_stack_.labels_[new_cur].start_.get_v()
          && label_stack_.labels_[label_stack_.cur_].exit_.get_v() == label_stack_.labels_[new_cur].exit_.get_v()) {
        new_cur --;
      } else {
        break;
      }
    }
    if (new_cur >= 0) {
      label_stack_.cur_ = new_cur;
    } else {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  inline LoopStack::LoopInfo *get_loops()
  {
    return loop_stack_.loops_;
  }
  inline int64_t get_loop_count() const
  {
    return loop_stack_.cur_;
  }
  inline LoopStack::LoopInfo *get_current_loop()
  {
    return loop_stack_.cur_ > 0 ? &loop_stack_.loops_[loop_stack_.cur_ - 1] : NULL;
  }

  // set loop must be called before br to loop body
  int set_loop(int64_t level,
               jit::ObLLVMBasicBlock &start,
               jit::ObLLVMBasicBlock &exit,
               const ObPLCursorForLoopStmt* cursor = NULL);

  // reset loop must be called after generate_early_exit
  inline int reset_loop()
  {
    int ret = common::OB_SUCCESS;
    if (loop_stack_.cur_ > 0) {
      --loop_stack_.cur_;
    } else {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  inline jit::ObLLVMBasicBlock &get_current_exception_block() {
    EHStack::EHInfo *curr = const_cast<EHStack::EHInfo*>(get_current_exception());
    return nullptr != curr ? curr->raising_block_
                           : default_raise_block_;
  }

  inline ObPLADTService &get_adt_service() { return adt_service_; }
  inline ObPLEHService &get_eh_service() { return eh_service_; }
  inline ObPLSPIService &get_spi_service() { return spi_service_; }
  inline jit::ObLLVMFunction &get_pl_execute() { return pl_execute_; }
  inline jit::ObLLVMFunction &get_user_type_var_func() { return set_user_type_var_; }
  inline jit::ObLLVMFunction &get_set_implicit_cursor_in_forall_func()
  {
    return set_implicit_cursor_in_forall_;
  }
  inline jit::ObLLVMFunction &get_unset_implicit_cursor_in_forall_func()
  {
    return unset_implicit_cursor_in_forall_;
  }
  inline jit::ObLLVMFunction &get_func() { return func_; }
  inline ObPLSEArray<jit::ObLLVMValue> &get_vars() { return vars_; }
  typedef common::hash::ObHashMap<
            uint64_t, jit::ObLLVMType,
            common::hash::NoPthreadDefendMode,
            common::hash::hash_func<uint64_t>,
            common::hash::equal_to<uint64_t>,
            common::hash::SimpleAllocer<common::hash::ObHashTableNode<
              common::hash::HashMapPair<uint64_t, jit::ObLLVMType>>>,
            common::hash::NormalPointer,
            common::ObMalloc,
            2>  // EXTEND_RATIO
      ObLLVMTypeMap;
  typedef common::hash::ObHashMap<
            uint64_t, jit::ObLLVMDIType,
            common::hash::NoPthreadDefendMode,
            common::hash::hash_func<uint64_t>,
            common::hash::equal_to<uint64_t>,
            common::hash::SimpleAllocer<common::hash::ObHashTableNode<
              common::hash::HashMapPair<uint64_t, jit::ObLLVMDIType>>>,
            common::hash::NormalPointer,
            common::ObMalloc,
            2>  // EXTEND_RATIO
      ObLLVMDITypeMap;
  inline ObLLVMTypeMap &get_user_type_map() { return user_type_map_; }
  int set_var_addr_to_param_store(int64_t var_index, jit::ObLLVMValue &var, jit::ObLLVMValue &init_value);
  int get_llvm_type(const ObPLDataType &pl_type, jit::ObLLVMType &ir_type);
  int get_datum_type(const ObPLDataType &pl_type, jit::ObLLVMType &ir_type);
  int64_t get_param_size() const { return ast_.is_routine() ? get_ast().get_arg_count() : 0; }
  inline const ObPLFunctionAST &get_ast() const { return static_cast<const ObPLFunctionAST &>(ast_); }
  inline ObPLFunctionAST &get_ast() { return static_cast<ObPLFunctionAST &>(ast_); }

  jit::ObLLVMValue& get_saved_ob_error() { return saved_ob_error_; }
  jit::ObLLVMValue& get_saved_exception() { return saved_exception_; }

public:
  int extract_meta_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_meta_ptr_from_obj(jit::ObLLVMValue &p_obj, jit::ObLLVMValue &result);
  int extract_accuracy_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_param_flag_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_raw_text_pos_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_raw_text_len_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_type_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_type_ptr_from_obj(jit::ObLLVMValue &p_obj, jit::ObLLVMValue &result);
  int extract_cslevel_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_cstype_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_scale_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_flag_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_obobj_ptr_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_obobj_from_objparam(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_value_ptr_from_obj(jit::ObLLVMValue &p_obj, ObObjType type, jit::ObLLVMValue &result);
  int extract_value_from_obj(jit::ObLLVMValue &p_obj, ObObjType type, jit::ObLLVMValue &result);
  int extract_datum_ptr_from_objparam(jit::ObLLVMValue &p_objparam, ObObjType type, jit::ObLLVMValue &result);
  int extract_value_ptr_from_objparam(jit::ObLLVMValue &p_objparam, ObObjType type, jit::ObLLVMValue &result);
  int extract_datum_from_objparam(jit::ObLLVMValue &p_objparam, common::ObObjType type, jit::ObLLVMValue &result);
  int extract_value_from_objparam(jit::ObLLVMValue &p_objparam, common::ObObjType type, jit::ObLLVMValue &result);
  int extract_extend_from_objparam(jit::ObLLVMValue &p_objparam, const ObPLDataType &type, jit::ObLLVMValue &result);
  int extract_extend_from_obj(jit::ObLLVMValue &p_obj, const ObPLDataType &type, jit::ObLLVMValue &result);
  int extract_obj_ptr_from_result(jit::ObLLVMValue &p_objparam, jit::ObLLVMValue &result);
  int extract_objparam_from_store(jit::ObLLVMValue &p_param_store, const int64_t idx, jit::ObLLVMValue &result);
  int extract_param_store_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_objparam_from_context(jit::ObLLVMValue &p_pl_exex_ctx, int64_t idx, jit::ObLLVMValue &result);
  int extract_value_from_context(jit::ObLLVMValue &p_pl_exex_ctx, int64_t idx, common::ObObjType type, jit::ObLLVMValue &result);
  int extract_datum_from_context(jit::ObLLVMValue &p_pl_exex_ctx, int64_t idx, ObObjType type, jit::ObLLVMValue &result);
  int extract_allocator_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_result_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_status_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_pl_ctx_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_pl_function_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result);
  int extract_arg_from_argv(jit::ObLLVMValue &p_argv, int64_t idx, jit::ObLLVMValue &result);
  int extract_objparam_from_argv(jit::ObLLVMValue &p_argv, const int64_t idx, jit::ObLLVMValue &result);
  int extract_datum_from_argv(jit::ObLLVMValue &p_argv, const int64_t idx, common::ObObjType type, jit::ObLLVMValue &result);
  int extract_value_from_argv(jit::ObLLVMValue &p_argv, const int64_t idx, common::ObObjType type, jit::ObLLVMValue &result);
  int extract_type_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_code_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_name_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_len_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_stmt_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_signal_ptr_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_type_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_code_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_name_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_len_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_stmt_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_signal_from_condition_value(jit::ObLLVMValue &p_condition_value, jit::ObLLVMValue &result);
  int extract_type_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_id_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_isnull_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_allocator_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_element_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_count_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_first_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_last_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_notnull_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_field_count_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_data_ptr_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_type_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_id_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_isnull_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_allocator_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_element_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_count_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_first_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_last_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_notnull_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_field_count_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_data_from_collection(jit::ObLLVMValue &p_collection, jit::ObLLVMValue &result);
  int extract_capacity_ptr_from_varray(jit::ObLLVMValue &p_varray, jit::ObLLVMValue &result);
  int extract_capacity_from_varray(jit::ObLLVMValue &p_varray, jit::ObLLVMValue &result);
  int extract_type_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_type_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_data_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_id_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_id_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_isnull_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_isnull_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_allocator_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_allocator_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_count_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_count_ptr_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_data_from_record(jit::ObLLVMValue &p_record, jit::ObLLVMValue &result);
  int extract_notnull_from_record(jit::ObLLVMValue &p_record, int64_t idx,
                                                     jit::ObLLVMValue &result);
  int extract_notnull_ptr_from_record(jit::ObLLVMValue &p_record, int64_t idx,
                                                     jit::ObLLVMValue &result);
  int extract_meta_from_record(jit::ObLLVMValue &p_record, int64_t count, int64_t idx,
                                                     jit::ObLLVMValue &result);
  int extract_meta_ptr_from_record(jit::ObLLVMValue &p_record, int64_t count, int64_t idx,
                                                     jit::ObLLVMValue &result);
  int extract_element_ptr_from_record(jit::ObLLVMValue &p_record, int64_t count, int64_t idx,
                                      jit::ObLLVMValue &result);
  int extract_type_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);
  int extract_type_ptr_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);
  int extract_notnull_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);
  int extract_notnull_ptr_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);
  int extract_field_count_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);
  int extract_field_count_ptr_from_elemdesc(jit::ObLLVMValue &p_elemdesc, jit::ObLLVMValue &result);

  int extract_allocator_from_composite_write(jit::ObLLVMValue &composite_write, jit::ObLLVMValue &result);
  int extract_allocator_ptr_from_composite_write(jit::ObLLVMValue &composite_write, jit::ObLLVMValue &result);
  int extract_value_from_composite_write(jit::ObLLVMValue &composite_write, jit::ObLLVMValue &result);
  int extract_value_ptr_from_composite_write(jit::ObLLVMValue &composite_write, jit::ObLLVMValue &result);

public:
  int store_obj(const ObObj &object, jit::ObLLVMValue &p_obj);
  int store_data_type(const ObDataType &object, jit::ObLLVMValue &result);
  int store_elem_desc(const ObElemDesc &object, jit::ObLLVMValue &result);
  int generate_debug(const ObString &name, int64_t value);
  int generate_debug(const ObString &name, jit::ObLLVMValue &value);
  int cast_to_int64(jit::ObLLVMValue &p_value);
  int generate_handle_ref_cursor(const ObPLCursor *cursor, const ObPLStmt &s,
                                 bool is_notfound, bool in_warning);
  int generate_set_extend(jit::ObLLVMValue &p_obj,
                                             ObPLType type,
                                             int32_t size,
                                             int64_t ptr);
  int generate_set_extend(jit::ObLLVMValue &p_obj,
                          jit::ObLLVMValue &type,
                          jit::ObLLVMValue &size,
                          jit::ObLLVMValue &ptr);
  int generate_check_autonomos(const ObPLStmt &s);
  int generate_spi_package_calc(uint64_t package_id,
                                int64_t expr_idx,
                                const ObPLStmt &s,
                                jit::ObLLVMValue &p_result_obj);
  int prepare_expression(ObPLCompileUnit &pl_func);
  int final_expression(ObPLCompileUnit &pl_func);
  ObSQLSessionInfo& get_session_info() const { return session_info_; }

private:
  int init_spi_service();
  int init_adt_service();
  int init_eh_service();
  int store_objparam(const ObObjParam &object, jit::ObLLVMValue &p_objparam);
  int generate_prototype();
  int init_argument();
  int prepare_local_user_type();
  int prepare_external();
  int prepare_subprogram(ObPLFunction &pl_func);
  int generate_get_attr(jit::ObLLVMValue &param_array,
                        const common::ObIArray<ObObjAccessIdx> &obj_access,
                        bool for_write,
                        jit::ObLLVMValue &ir_value,
                        jit::ObLLVMValue &allocator_ptr,
                        jit::ObLLVMValue &ret_value,
                        jit::ObLLVMBasicBlock &exit,
                        const sql::ObExprResType &res_type);
  int generate_get_record_attr(const ObObjAccessIdx &current_access,
                                           uint64_t udt_id,
                                           bool for_write,
                                           jit::ObLLVMValue &current_value,
                                           jit::ObLLVMValue &current_allocator,
                                           jit::ObLLVMValue &ret_value_ptr,
                                           jit::ObLLVMBasicBlock& exit);
  int generate_get_collection_attr(jit::ObLLVMValue &param_array,
                                           const ObObjAccessIdx &current_access,
                                           int64_t access_i,
                                           bool for_write,
                                           bool is_assoc_array,
                                           jit::ObLLVMValue &current_value,
                                           jit::ObLLVMValue &current_allocator,
                                           jit::ObLLVMValue &ret_value_ptr,
                                           jit::ObLLVMBasicBlock& exit,
                                           const sql::ObExprResType &res_type);
  int generate_get_attr_func(const common::ObIArray<ObObjAccessIdx> &idents,
                             int64_t param_count, const
                             common::ObString &func_name,
                             bool for_write,
                             const sql::ObExprResType &res_type);
#ifdef OB_BUILD_ORACLE_PL
  int build_nested_table_type(const ObNestedTableType &table_type, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_assoc_array_type(const ObAssocArrayType &table_type, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_varray_type(const ObVArrayType &array_type, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_collection_type(const ObCollectionType &collection_type, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_subtype(const ObUserDefinedSubType &subtype, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_opaque_type(const ObUserDefinedType &opaque_type, ObIArray<jit::ObLLVMType> &elem_type_array);
#endif
  int build_record_type(const ObRecordType &record_type, ObIArray<jit::ObLLVMType> &elem_type_array);
  int build_composite(ObIArray<jit::ObLLVMType> &elem_type_array);
  int generate_spi_calc(int64_t expr_idx,
                        int64_t stmt_id,
                        bool in_notfound,
                        bool in_warning,
                        int64_t result_idx,
                        jit::ObLLVMValue &p_result_obj);
  int generate_llvm_calc(int64_t expr_idx, int64_t stmt_id, bool in_notfound,
                         bool in_warning, int64_t result_idx, jit::ObLLVMValue &p_result_obj);
  int generate_const_calc(int32_t value, jit::ObLLVMValue &p_result_obj);
  int generate_compare_calc(jit::ObLLVMValue &left,
                                               jit::ObLLVMValue &right,
                                               ObItemType type,
                                               jit::ObLLVMValue &p_result_obj);
  int generate_arith_calc(jit::ObLLVMValue &left,
                          jit::ObLLVMValue &right,
                          ObItemType type,
                          const sql::ObExprResType &result_type,
                          int64_t stmt_id,
                          bool in_notfound,
                          bool in_warning,
                          jit::ObLLVMValue &p_result_obj);
private:
  common::ObIAllocator &allocator_;
  sql::ObSQLSessionInfo &session_info_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  ObPLCompileUnitAST &ast_;
  common::ObIArray<ObSqlExpression*> &exprs_;
  jit::ObLLVMHelper &helper_;
  jit::ObLLVMFunction func_;
  jit::ObLLVMBasicBlock entry_;
  jit::ObLLVMBasicBlock exit_;
  jit::ObLLVMBasicBlock current_;
  EHStack exception_stack_;
  LabelStack label_stack_;
  LoopStack loop_stack_;
  ObPLADTService adt_service_;
  ObPLSPIService spi_service_;
  ObPLEHService eh_service_;
  jit::ObLLVMFunction pl_execute_;
  jit::ObLLVMFunction set_user_type_var_;
  jit::ObLLVMFunction set_implicit_cursor_in_forall_;
  jit::ObLLVMFunction unset_implicit_cursor_in_forall_;
  ObLLVMTypeMap user_type_map_;
  jit::ObLLVMValue saved_ob_error_;
  jit::ObLLVMValue saved_exception_;
  ObPLSEArray<jit::ObLLVMValue> vars_; //第0个是隐藏ctx参数，从第1个开始与ObPLSymbolTable对应
  // key: stmt id, value: pair(key: index, -1,)
  goto_label_map goto_label_map_;

  // an unreachable block, used to tell LLVM there is no successor block after this
  jit::ObLLVMBasicBlock unreachable_;

  // current stmt_id, updated when throw an exception
  // also anchor of allocas in entry block
  jit::ObLLVMValue stmt_id_;

  // if there is no current_exception, use this block to throw an exception to PL engine
  jit::ObLLVMBasicBlock default_raise_block_;

public:
  int get_unreachable_block(jit::ObLLVMBasicBlock &unreachable);
  inline goto_label_map &get_goto_label_map() { return goto_label_map_; }
  // debug.
public:
//  inline jit::ObLLVMDIHelper &get_di_helper() { return di_helper_; }
//  inline ObPLDIADTService &get_di_adt_service() { return di_adt_service_; }
  int set_debug_location(const ObPLStmt &s);
  int unset_debug_location();
  int get_di_llvm_type(const ObPLDataType &pl_type, jit::ObLLVMDIType &di_type);
  int get_di_datum_type(const ObPLDataType &pl_type, jit::ObLLVMDIType &di_type);
  int generate_di_user_type(const ObUserDefinedType &type, uint32_t line);
#ifdef OB_BUILD_ORACLE_PL
  int generate_di_table_type(const ObNestedTableType &table_type, uint32_t line);
#endif
  int generate_di_record_type(const ObRecordType &record_type, uint32_t line);
  int generate_di_argument();
  // generate debug info for variables which have a llvm::Value,
  // for parameters of function, arg_no should be start from 1.
  // for common variables, arg_no should be 0.
  int generate_di_local_variable(const ObPLVar &var,
                                 uint32_t arg_no, uint32_t line, jit::ObLLVMValue &value);
  int generate_di_local_variable(const ObString &name, jit::ObLLVMDIType &di_type,
                                 uint32_t arg_no, uint32_t line, jit::ObLLVMValue &value);
  bool get_debug_mode() { return debug_mode_; }

  inline ObPLSEArray<jit::ObLLVMValue> &get_out_params() { return out_params_; }
  inline void reset_out_params() { out_params_.reset(); }
  inline int add_out_params(jit::ObLLVMValue &value) { return out_params_.push_back(value); }
  int generate_entry_alloca(const common::ObString &name, const common::ObObjType &type, jit::ObLLVMValue &result);
  int generate_entry_alloca(const common::ObString &name, const jit::ObLLVMType &ir_type, jit::ObLLVMValue &result);
  bool get_profile_mode() { return profile_mode_; }

  int generate_spi_pl_profiler_before_record(const ObPLStmt &s);
  int generate_spi_pl_profiler_after_record(const ObPLStmt &s);

  int generate_get_parent_allocator(jit::ObLLVMValue &allocator,
                                    jit::ObLLVMValue &parent_allocator,
                                    jit::ObLLVMValue &ret_value_ptr,
                                    jit::ObLLVMBasicBlock &exit);
  int extract_allocator_and_restore_obobjparam(jit::ObLLVMValue &into_address, jit::ObLLVMValue &allocator);
  int generate_get_current_expr_allocator(const ObPLStmt &s, jit::ObLLVMValue &expr_allocator);
  static int set_profiler_unit_info_recursive(const ObPLCompileUnit &unit);

private:
  int init_di_adt_service();
  int generate_di_prototype();

private:
  jit::ObLLVMDIHelper &di_helper_;
  ObPLDIADTService di_adt_service_;
  ObLLVMDITypeMap di_user_type_map_;
  bool debug_mode_;
  bool oracle_mode_;
  ObPLSEArray<jit::ObLLVMValue> out_params_;
  bool profile_mode_;

  using GlobalStringMap = common::hash::ObHashMap<
                            common::ObString,
                            std::pair<jit::ObLLVMValue, jit::ObLLVMValue>,
                            common::hash::NoPthreadDefendMode,
                            common::hash::hash_func<common::ObString>,
                            common::hash::equal_to<common::ObString>,
                            common::hash::SimpleAllocer<common::hash::ObHashTableNode<
                            common::hash::HashMapPair<common::ObString, std::pair<jit::ObLLVMValue, jit::ObLLVMValue>>>>,
                            common::hash::NormalPointer,
                            common::ObMalloc,
                            2  // EXTEND_RATIO
                          >;

  GlobalStringMap global_strings_;

  ObPLCGBufferGuard *top_buffer_guard_ = nullptr;

  ObPLSEArray<jit::ObLLVMValue> int_buffer_;
  int64_t int_buffer_idx_ = 0;

  ObPLSEArray<jit::ObLLVMValue> objparam_buffer_;
  int64_t objparam_buffer_idx_ = 0;

  jit::ObLLVMValue char_buffer_;
  jit::ObLLVMValue condition_buffer_;
  jit::ObLLVMValue data_type_buffer_;

  jit::ObLLVMValue into_type_array_ptr_;
  int64_t into_type_array_size_ = 0;

  jit::ObLLVMValue return_type_array_ptr_;
  int64_t return_type_array_size_ = 0;

  jit::ObLLVMValue argv_array_ptr_;
  int64_t argv_array_size_ = 0;

  int get_int_buffer(jit::ObLLVMValue &result);
  int get_char_buffer(jit::ObLLVMValue &result);
  int get_condition_buffer(jit::ObLLVMValue &result);
  int get_data_type_buffer(jit::ObLLVMValue &result);
  int get_objparam_buffer(jit::ObLLVMValue &result);

  int64_t get_objparam_buffer_idx() { return objparam_buffer_idx_; };
  void set_objparam_buffer_idx(int64_t idx) { objparam_buffer_idx_ = idx; }

  int64_t get_int_buffer_idx() { return int_buffer_idx_; };
  void set_int_buffer_idx(int64_t idx) { int_buffer_idx_ = idx; }

  int get_into_type_array_buffer(int64_t size, jit::ObLLVMValue &result);
  int get_return_type_array_buffer(int64_t size, jit::ObLLVMValue &result);
  int get_argv_array_buffer(int64_t size, jit::ObLLVMValue &result);
};

class ObPLCodeGenerateVisitor : public ObPLStmtVisitor
{
public:
  ObPLCodeGenerateVisitor(ObPLCodeGenerator &generator) : generator_(generator) {}
  virtual ~ObPLCodeGenerateVisitor() {}

  int generate(const ObPLStmt &s);

  virtual int visit(const ObPLStmtBlock &s);
  virtual int visit(const ObPLDeclareVarStmt &s);
  virtual int visit(const ObPLAssignStmt &s);
  virtual int visit(const ObPLIfStmt &s);
  virtual int visit(const ObPLLeaveStmt &s);
  virtual int visit(const ObPLIterateStmt &s);
  virtual int visit(const ObPLWhileStmt &s);
  virtual int visit(const ObPLForLoopStmt &s);
  virtual int visit(const ObPLCursorForLoopStmt &s);
  virtual int visit(const ObPLForAllStmt &s);
  virtual int visit(const ObPLRepeatStmt &s);
  virtual int visit(const ObPLLoopStmt &s);
  virtual int visit(const ObPLReturnStmt &s);
  virtual int visit(const ObPLSqlStmt &s);
  virtual int visit(const ObPLExecuteStmt &s);
  virtual int visit(const ObPLExtendStmt &s);
  virtual int visit(const ObPLDeleteStmt &s);
  virtual int visit(const ObPLDeclareCondStmt &s);
  virtual int visit(const ObPLDeclareHandlerStmt &s);
  virtual int visit(const ObPLSignalStmt &s);
  virtual int visit(const ObPLCallStmt &s);
  virtual int visit(const ObPLDeclareCursorStmt &s);
  virtual int visit(const ObPLOpenStmt &s);
  virtual int visit(const ObPLOpenForStmt &s);
  virtual int visit(const ObPLFetchStmt &s);
  virtual int visit(const ObPLCloseStmt &s);
  virtual int visit(const ObPLNullStmt &s);
  virtual int visit(const ObPLPipeRowStmt &s);
  virtual int visit(const ObPLDeclareUserTypeStmt &s);
  virtual int visit(const ObPLRaiseAppErrorStmt &s);
  virtual int visit(const ObPLGotoStmt &s);
  virtual int visit(const ObPLTrimStmt &s);
  virtual int visit(const ObPLInterfaceStmt &s);
  virtual int visit(const ObPLDoStmt &s);
  virtual int visit(const ObPLCaseStmt &s);

private:
  int find_next_procedence_condition(common::ObIArray<std::pair<ObPLConditionType, int64_t>> &conditions,
                                     common::ObIArray<int64_t> &position_map, int64_t &idx);
private:

  int get_element_ir_type(const ObPLDataType &pl_type, jit::ObLLVMType &ir_type);
  int get_element_di_type(const ObPLDataType &pl_type, jit::ObLLVMDIType &di_type);

private:
  ObPLCodeGenerator &generator_;
};

struct ObPLCGBufferGuard
{
public:
  ObPLCGBufferGuard(ObPLCodeGenerator &generator)
    : generator_(generator),
      objparam_buffer_idx_(generator.get_objparam_buffer_idx()),
      old_guard_(generator.top_buffer_guard_)
  {
    generator.top_buffer_guard_ = this;
  }

  virtual ~ObPLCGBufferGuard();

#define GENERATE_BUFFER_GETTER(buffer_name)                                    \
  OB_INLINE int get_##buffer_name(jit::ObLLVMValue &result)                    \
  {                                                                            \
    int ret = OB_SUCCESS;                                                      \
    if (OB_FAIL(check_guard_valid())) {                                        \
      PL_LOG(WARN, "failed to check_guard_valid", K(ret));                     \
    } else if (OB_FAIL(generator_.get_##buffer_name(result))) {                \
      PL_LOG(WARN, "failed to get buffer", K(ret));                            \
    }                                                                          \
    return ret;                                                                \
  }

  GENERATE_BUFFER_GETTER(int_buffer)
  GENERATE_BUFFER_GETTER(condition_buffer)
  GENERATE_BUFFER_GETTER(data_type_buffer)
  GENERATE_BUFFER_GETTER(char_buffer)

  int get_objparam_buffer(jit::ObLLVMValue &result);

#undef GENERATE_BUFFER_GETTER

#define GENERATE_ARRAY_BUFFER_GETTER(buffer_name)                              \
  OB_INLINE int get_##buffer_name(int64_t size, jit::ObLLVMValue &result)      \
  {                                                                            \
    int ret = OB_SUCCESS;                                                      \
    if (OB_FAIL(check_guard_valid())) {                                        \
      PL_LOG(WARN, "failed to check_guard_valid", K(ret));                     \
    } else if (OB_FAIL(generator_.get_##buffer_name(size, result))) {          \
      PL_LOG(WARN, "failed to get buffer", K(ret));                            \
    }                                                                          \
    return ret;                                                                \
  }

  GENERATE_ARRAY_BUFFER_GETTER(into_type_array_buffer)
  GENERATE_ARRAY_BUFFER_GETTER(return_type_array_buffer)
  GENERATE_ARRAY_BUFFER_GETTER(argv_array_buffer)

#undef GENERATE_ARRAY_BUFFER_GETTER

private:
  OB_INLINE int check_guard_valid()
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(this != generator_.top_buffer_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      PL_LOG(WARN,
             "unexpected buffer guard, only top buffer guard is allowed to get buffer",
             K(ret), K(lbt()));
    }

    return ret;
  }

private:
  ObPLCodeGenerator &generator_;
  int64_t objparam_buffer_idx_;
  int64_t objparam_count_ = 0;
  ObPLCGBufferGuard *old_guard_ = nullptr;
};

}
}
#endif /* OCEANBASE_SRC_PL_OB_PL_CODE_GENERATOR_H_ */
