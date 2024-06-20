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

#define USING_LOG_PREFIX PL

#include "ob_pl_code_generator.h"
#include "ob_pl_type.h"
#include "ob_pl_compile.h"
#include "ob_pl_package.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "lib/hash/ob_hashmap.h"
#include "parser/parse_stmt_item_type.h"

namespace oceanbase
{
using namespace common;
using namespace jit;
using namespace sql;
namespace pl
{

int ObPLCodeGenerateVisitor::generate(const ObPLStmt &s)
{
  int ret = OB_SUCCESS;
  OZ (generator_.restart_cg_when_goto_dest(s));
  OZ (s.accept(*this));
  return ret;
}

int ObPLCodeGenerator::generate_check_autonomos(const ObPLStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
    // 控制流已断, 后面的语句不再处理
  } else if (lib::is_oracle_mode()) {
    OZ (get_helper().set_insert_point(get_current()));
    if(OB_SUCC(ret)) {
      ObSEArray<ObLLVMValue, 1> args;
      if (OB_FAIL(args.push_back(get_vars().at(CTX_IDX)))) { //PL的执行环境
        LOG_WARN("push_back error", K(ret));
      } else {
        ObLLVMValue ret_err;
        if (OB_FAIL(get_helper().create_call(ObString("spi_check_autonomous_trans"), get_spi_service().spi_check_autonomous_trans_, args, ret_err))) {
          LOG_WARN("failed to create call", K(ret));
        } else if (OB_FAIL(check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
          LOG_WARN("failed to check success", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLStmtBlock &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMBasicBlock exit;
    if (s.has_label()) {
      if (OB_FAIL(generator_.get_helper().create_block(ObString("exit_block"), generator_.get_func(), exit))) {
        LOG_WARN("failed to create block", K(s.get_stmts()), K(ret));
      } else {
        ObLLVMBasicBlock null_start;
        if (OB_FAIL(generator_.set_label(s, null_start, exit))) {
          LOG_WARN("failed to set label", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < s.get_stmts().count(); ++i) {
      ObPLStmt *stmt = s.get_stmts().at(i);
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt in block is NULL", K(i), K(s.get_stmts()), K(ret));
      } else if (OB_FAIL(SMART_CALL(generate(*stmt)))) {
        LOG_WARN("failed to generate", K(i), K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() && s.get_stmts().count() > 0 &&
        !generator_.get_ast().is_function()) { // function check logic is in returnstmt cg stage
      ObPLStmt *stmt = s.get_stmts().at(s.get_stmts().count() - 1);
      if ((0 == s.get_level() && NULL != s.get_block() && NULL == s.get_block()->get_block()) ||
          (1 == s.get_level() && NULL != s.get_block() && NULL != s.get_block()->get_block() &&
          NULL == s.get_block()->get_block()->get_block())) {
        if (PL_RETURN != stmt->get_type() &&
                  s.get_is_autonomous()) {
          OZ (generator_.generate_check_autonomos(*stmt));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (s.has_eh()) { //如果有eh，跳到eh的exit分支
        if (NULL != generator_.get_current_exception()->exit_.get_v()) {
          if (OB_FAIL(generator_.finish_current(generator_.get_current_exception()->exit_))) {
            LOG_WARN("failed to finish current", K(s.get_stmts()), K(ret));
          } else if (NULL != exit.get_v()) {
            if (generator_.get_current_exception()->exit_.get_v() == generator_.get_exit().get_v()) {
              //exit是终点，不再跳转
            } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current_exception()->exit_))) {
              LOG_WARN("failed to set insert point", K(ret));
            } else if (OB_FAIL(generator_.get_helper().create_br(exit))) {
              LOG_WARN("failed to create br", K(ret));
            } else { /*do nothing*/ }
          } else { /*do nothing*/ }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(generator_.set_current(NULL == exit.get_v() ? generator_.get_current_exception()->exit_ : exit))) {
              LOG_WARN("failed to set current", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(generator_.reset_exception())) {
            LOG_WARN("failed to reset exception", K(ret));
          }
        }
      } else if (NULL != exit.get_v()) { //如果没有eh，调到BLOCK自己的exit分支
        if (NULL == generator_.get_current().get_v()) {
          // do nothing...
        } else if (OB_FAIL(generator_.get_helper().create_br(exit))) {
          LOG_WARN("failed to create br", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generator_.set_current(exit))) {
          LOG_WARN("failed to set current", K(ret));
        } else { /*do nothing*/ }
      } else { /*do nothing*/ }

      if (OB_SUCC(ret) && NULL != exit.get_v()) {
        if (OB_FAIL(generator_.reset_label())) {
          LOG_WARN("failed to reset label", K(ret));
        }
      }
    }

    //释放内存
    if (OB_SUCC(ret) && NULL != generator_.get_current().get_v()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_namespace().get_symbols().count(); ++i) {
        const ObPLVar *var = s.get_variable(s.get_namespace().get_symbols().at(i));
        if (OB_ISNULL(var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var is NULL", K(i), K(s.get_namespace().get_symbols().at(i)), K(var), K(ret));
        } else if (var->get_type().is_collection_type() && var->get_name().length() != 0) {
          ObSEArray<ObLLVMValue, 2> args;
          ObLLVMValue int_value;
          if (s.get_namespace().get_symbols().at(i) < generator_.get_param_size()) {
            // 不要释放入参的内存
          } else if (OB_FAIL(args.push_back(generator_.get_vars().at(generator_.CTX_IDX)))) { //PL的执行环境
            LOG_WARN("push_back error", K(ret));
          } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_namespace().get_symbols().at(i), int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (OB_FAIL(args.push_back(int_value))) {
            LOG_WARN("push_back error", K(ret));
          } else {
            ObLLVMValue ret_err;
            if (OB_FAIL(generator_.get_helper().create_call(ObString("spi_destruct_collection"), generator_.get_spi_service().spi_destruct_collection_, args, ret_err))) {
              LOG_WARN("failed to create call", K(ret));
            } else if (OB_FAIL(generator_.check_success(ret_err, s.get_stmt_id(), s.in_notfound(), s.in_warning()))) {
              LOG_WARN("failed to check success", K(ret));
            }
          }
        }
      }
      // close cursor
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_namespace().get_cursors().count(); ++i) {
        const ObPLCursor *cursor = s.get_cursor(s.get_namespace().get_cursors().at(i));
        OZ (generator_.generate_handle_ref_cursor(cursor, s, s.in_notfound(), s.in_warning()));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDeclareUserTypeStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else {
    const ObUserDefinedType *user_defined_type = s.get_user_type();
    if (OB_ISNULL(user_defined_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user defined type is null");
    } else if (OB_FAIL(generator_.generate_user_type(*user_defined_type))) {
      LOG_WARN("failed to generate user type", K(user_defined_type->get_type()));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDeclareVarStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else {
    OZ (generator_.get_helper().set_insert_point(generator_.get_current()));
    OZ (generator_.set_debug_location(s));
    OZ (generator_.generate_spi_pl_profiler_before_record(s));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(generator_.get_current().get_v())) {
    ObLLVMType ir_type;
    ObLLVMValue value;
    bool is_complex_type_var = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < s.get_index().count(); ++i) {
      const ObPLVar *var = s.get_var(i);
      CK (OB_NOT_NULL(var));
      if (OB_SUCC(ret)) {
        if (var->get_type().is_obj_type()) {
          OZ (var->get_type().generate_construct(generator_, *s.get_namespace(), value, &s));
          OX (generator_.get_vars().at(s.get_index(i) + generator_.USER_ARG_OFFSET) = value);
        } else { // Record和Collection的内存不在栈上申请, 统一在Allocator中申请, 执行结束后统一释放
          ObSEArray<ObLLVMValue, 3> args;
          ObLLVMValue var_idx, init_value, var_value, extend_value;
          ObLLVMValue ret_err;
          ObLLVMValue var_type, type_id;
          ObLLVMValue null_int;
          int64_t init_size = 0;
          is_complex_type_var = true;
          // Step 1: 初始化内存
          CK (OB_NOT_NULL(s.get_namespace()));
          OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
          OZ (generator_.get_helper().get_int8(var->get_type().get_type(), var_type));
          OZ (args.push_back(var_type));
          OZ (generator_.get_helper().get_int64(var->get_type().get_user_type_id(), type_id));
          OZ (args.push_back(type_id));
          OZ (generator_.get_helper().get_int64(s.get_index(i), var_idx));
          OZ (args.push_back(var_idx));
          OZ (s.get_namespace()->get_size(PL_TYPE_INIT_SIZE, var->get_type(), init_size));
          OZ (generator_.get_helper().get_int32(init_size, init_value));
          OZ (args.push_back(init_value));
          OZ (generator_.generate_null_pointer(ObIntType, null_int));
          OZ (args.push_back(null_int));
          OZ (generator_.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                               generator_.get_spi_service().spi_alloc_complex_var_,
                                               args,
                                               ret_err));
          OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                       s.get_block()->in_notfound(),
                                       s.get_block()->in_warning()));
          // Step 2: 初始化类型内容, 如Collection的rowsize,element type等
          if (OB_SUCC(ret) && !var->get_type().is_opaque_type()) {
            OZ (generator_.extract_objparam_from_context(
                generator_.get_vars().at(generator_.CTX_IDX),
                s.get_index(i),
                var_value));
            OZ (generator_.extract_extend_from_objparam(var_value, var->get_type(), extend_value));
            OZ (var->get_type().generate_construct(generator_, *s.get_namespace(),
                                                   extend_value, &s));
#ifndef NDEBUG
            {
              ObLLVMType int_type;
              ObLLVMValue tmp;
              OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
              OZ (generator_.get_helper().create_ptr_to_int(ObString("cast_pointer_to_int"), extend_value, int_type, tmp));
              OZ (generator_.generate_debug(ObString("print new construct"), tmp));
            }
#endif
          }
          // Step 3: 默认值是构造函数,调用构造函数的初始化
          if (OB_SUCC(ret)
              && var->get_type().is_collection_type()
              && PL_CONSTRUCT_COLLECTION == s.get_default()) {
            ObLLVMValue package_id;
            args.reset();
            OZ (generator_.get_helper().get_int64(OB_INVALID_ID, package_id));
            OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
            OZ (args.push_back(package_id));
            OZ (args.push_back(var_value));
            OZ (generator_.get_helper().create_call(ObString("spi_construct_collection"), generator_.get_spi_service().spi_construct_collection_, args, ret_err));
            OZ (generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
          }
        }
      }
    }
    // 处理变量默认值
    if (OB_SUCC(ret)
        && OB_INVALID_INDEX != s.get_default()
        && PL_CONSTRUCT_COLLECTION != s.get_default()) {
      ObLLVMValue p_result_obj;
      OZ (generator_.generate_expr(s.get_default(),
                                   s,
                                   is_complex_type_var ? OB_INVALID_ID : s.get_index(0),
                                   p_result_obj));
      CK (OB_NOT_NULL(s.get_default_expr()));

      // 检查notnull约束
      CK (OB_NOT_NULL(s.get_var(0)));
      OZ (generator_.generate_check_not_null(s, s.get_var(0)->is_not_null(), p_result_obj));

      if (OB_SUCC(ret)) {
        ObLLVMValue result;
        if (OB_FAIL(generator_.extract_datum_from_objparam(p_result_obj, s.get_default_expr()->get_data_type(), result))) {
          LOG_WARN("failed to extract_datum_from_objparam", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < s.get_index().count(); ++i) {
            const ObPLVar *var = s.get_var(i);
            CK (OB_NOT_NULL(var));
            // 将默认值存储到paramstore, 第0个变量已经在generate_expr处理过了
            if (OB_SUCC(ret) && (is_complex_type_var || 0 != i)) {
              ObLLVMValue into_meta_p, ori_meta_p, ori_meta;
              ObLLVMValue into_accuracy_p, ori_accuracy_p, ori_accuracy;
              ObLLVMValue into_obj, dest_datum;
              OZ (generator_.extract_objparam_from_context(generator_.get_vars().at(generator_.CTX_IDX), s.get_index(i), into_obj));
              if (var->get_type().is_obj_type()) {
                OZ (generator_.extract_datum_ptr_from_objparam(into_obj, s.get_var(i)->get_type().get_obj_type(), dest_datum));
                OZ (generator_.get_helper().create_store(result, generator_.get_vars().at(s.get_index(i) + generator_.USER_ARG_OFFSET)));
                OZ (generator_.get_helper().create_store(result, dest_datum));
              } else {
                ObLLVMValue allocator, src_datum;
                const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(s.get_default_expr());
                OZ (generator_.generate_null(ObIntType, allocator));
                CK (NULL != s.get_symbol_table());
                if (OB_FAIL(ret)) {
                } else if (T_NULL == s.get_default_expr()->get_expr_type() ||
                    (T_QUESTIONMARK == const_expr->get_expr_type() &&
                    0 == s.get_symbol_table()->get_symbol(const_expr->get_value().get_unknown())->get_name().case_compare(ObPLResolver::ANONYMOUS_ARG) &&
                    NULL != s.get_symbol_table()->get_symbol(const_expr->get_value().get_unknown())->get_pl_data_type().get_data_type() &&
                    ObNullType == s.get_symbol_table()->get_symbol(const_expr->get_value().get_unknown())->get_pl_data_type().get_data_type()->get_obj_type())) {
                  /*
                   * allocator可以是null，因为这儿只是修改原复杂类型的属性信息
                   * 对于record来说把内部的元素初始化成一个null，
                   * 对于collection来说把count改为-1标记为未初始化状态
                   */
                  OZ (generator_.extract_extend_from_objparam(into_obj,
                                                              var->get_type(),
                                                              dest_datum));
                  OZ (var->get_type().generate_assign_with_null(generator_,
                                                                *s.get_namespace(),
                                                                allocator,
                                                                dest_datum));
                } else {
                  OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, src_datum));
                  OZ (generator_.extract_obobj_ptr_from_objparam(into_obj, dest_datum));
                  OZ (var->get_type().generate_copy(generator_, *s.get_namespace(),
                                                    allocator, src_datum, dest_datum,
                                                    s.get_block()->in_notfound(),
                                                    s.get_block()->in_warning(),
                                                    OB_INVALID_ID));
                  OZ (generator_.extract_meta_ptr_from_objparam(p_result_obj, ori_meta_p));
                  OZ (generator_.extract_meta_ptr_from_objparam(into_obj, into_meta_p));
                  OZ (generator_.get_helper().create_load(ObString("load_meta"),
                                                          ori_meta_p, ori_meta));
                  OZ (generator_.get_helper().create_store(ori_meta, into_meta_p));
                  OZ (generator_.extract_accuracy_ptr_from_objparam(p_result_obj, ori_accuracy_p));
                  OZ (generator_.extract_accuracy_ptr_from_objparam(into_obj, into_accuracy_p));
                  OZ (generator_.get_helper().create_load(ObString("load_accuracy"),
                                                          ori_accuracy_p, ori_accuracy));
                  OZ (generator_.get_helper().create_store(ori_accuracy, into_accuracy_p));
                }
              }
            }
          }
        }
      }
    }
    if (lib::is_mysql_mode()) {
      ObLLVMValue ret_err;
      ObSEArray<ObLLVMValue, 1> args;
      OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
      OZ (generator_.get_helper().create_call(ObString("spi_clear_diagnostic_area"),
                                                generator_.get_spi_service().spi_clear_diagnostic_area_,
                                                args,
                                                ret_err));
      OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                        s.get_block()->in_notfound(),
                                        s.get_block()->in_warning()));
    }

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

/***************************************************************************************/
/* 注意：以下是内存排列有关的代码，修改这里一定要十分理解各种数据类型在LLVM端和SQL端的内存排列和生命周期。
 * 如有问题请联系如颠ryan.ly
 ***************************************************************************************/
int ObPLCodeGenerateVisitor::visit(const ObPLAssignStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (s.get_into().count() != s.get_value().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("into exprs not equal to value exprs", K(ret), K(s.get_into().count()), K(s.get_value().count()));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    ObLLVMValue stack;
    OZ (generator_.get_helper().stack_save(stack));
    for (int64_t i = 0; OB_SUCC(ret) && i < s.get_into().count(); ++i) {
      const ObRawExpr *into_expr = s.get_into_expr(i);
      const ObRawExpr *value_expr = NULL;
      if (OB_ISNULL(into_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a assign stmt must has expr", K(s), K(into_expr), K(ret));
      } else {
        int64_t result_idx = OB_INVALID_INDEX;
        if (into_expr->is_const_raw_expr()) {
          const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(into_expr);
          if (const_expr->get_value().is_unknown()) {
            const ObPLVar *var = NULL;
            int64_t idx = const_expr->get_value().get_unknown();
            CK (OB_NOT_NULL(var = s.get_variable(idx)));
            if (OB_SUCC(ret)) {
              const ObPLDataType &into_type = var->get_type();
              if (!into_type.is_collection_type() && !into_type.is_record_type()) {
                result_idx = idx;
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected const expr", K(const_expr->get_value()), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObLLVMValue p_result_obj;
          ObObjAccessIdx::AccessType alloc_scop = ObObjAccessIdx::IS_INVALID;
          uint64_t package_id = OB_INVALID_ID;
          uint64_t var_idx = OB_INVALID_ID;
          ObLLVMValue allocator;
          jit::ObLLVMValue src_datum;
          jit::ObLLVMValue dest_datum;
          const ObPLBlockNS *ns = s.get_namespace();
          if (OB_ISNULL(ns)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Assign stmt must have a valid namespace", K(ret));
          } else if (PL_CONSTRUCT_COLLECTION == s.get_value_index(i)) {
            ObPLDataType final_type;
            CK (into_expr->is_obj_access_expr());
            OZ (static_cast<const ObObjAccessRawExpr*>(into_expr)->get_final_type(final_type));
            CK (final_type.is_collection_type());
          } else {
            value_expr = s.get_value_expr(i);
            if (OB_FAIL(generator_.generate_expr(s.get_value_index(i), s, result_idx,
                                                 p_result_obj))) {
              LOG_WARN("failed to generate calc_expr func", K(ret));
            }
            if (lib::is_mysql_mode()) {
              ObLLVMValue ret_err;
              ObSEArray<ObLLVMValue, 1> args;
              OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
              OZ (generator_.get_helper().create_call(ObString("spi_clear_diagnostic_area"),
                                                        generator_.get_spi_service().spi_clear_diagnostic_area_,
                                                        args,
                                                        ret_err));
              OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                                s.get_block()->in_notfound(),
                                                s.get_block()->in_warning()));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObObjAccessIdx::datum_need_copy(into_expr, value_expr, alloc_scop))) {
            LOG_WARN("failed to check if datum_need_copy", K(*into_expr), K(*value_expr), K(ret));
          } else if (ObObjAccessIdx::IS_LOCAL == alloc_scop
                     || (ObObjAccessIdx::IS_PKG != alloc_scop)) {
            alloc_scop = ObObjAccessIdx::IS_LOCAL;
          }
          if (OB_SUCC(ret) && ObObjAccessIdx::IS_PKG == alloc_scop) { // 如果是Package变量, 记录下PackageId
            OZ (ObObjAccessIdx::get_package_id(into_expr, package_id, &var_idx));
          }
          if (OB_SUCC(ret)) {
            //如果是LOCAL，使用运行时语句级allocator
            if (ObObjAccessIdx::IS_LOCAL == alloc_scop) {
            /*
             * 如果不是Collection，原则上应该进一步向上找到母体Collection的allocator，这样太复杂了，我们放弃这么做，直接用生命周期更长的运行时语句级allocator
             * 这样的后果是：Collection里的string和number类型数据有的是在NestedTable自己的allocator里，有的不是。
             * 这其实没有问题，基础数据类型不会占用过多内存，而且语句级内存最终会释放，不会泄漏。
             */
              if (OB_FAIL(generator_.extract_allocator_from_context(generator_.get_vars().at(generator_.CTX_IDX), allocator))) {
                LOG_WARN("Failed to extract_allocator_from_nestedtable", K(*into_expr), K(ret));
              }
            } else {
              //如果不是LOCAL，那么一定是PKG或者是INVALID，传入NULL allocator;
              //对于PKG, 由SPI根据package_id从Session上获取对应PackageState上的Allocator;
              if (OB_FAIL(generator_.generate_null(ObIntType, allocator))) { //初始化一个空的allocator
                LOG_WARN("Failed to extract_allocator_from_nestedtable", K(*into_expr), K(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            /**
             * 分三种情况讨论：
             * 1、Local变量
             *   a、原生基本数据类型（不在母结构中的基本数据类型，通过Question Mark访问）：直接找到该变量调用create store，同时需要修改param_store里的值（对于string和number也是如此，指针指向同一份内存）
             *   b、结构中的基本数据类型（在Record或Collection里中的基本数据类型，通过ObjAccess访问）：直接找到该变量的地址调用create store或者调用COPY，不必修改param_store里的值（对于string和number也是如此，指针指向同一份内存）
             *      注意： 为什么不全部直接调用COPY函数？————其实全部直接调COPY函数代码更简洁，考虑到大多数情况下不需要内存深拷贝，直接create store性能更好。
             *           这样会造成一种可能性：NestedTable里的string和number类型数据有的是在Collection自己的allocator里，有的不是。————这其实不一定有问题，只要目的端数据的内存生命周期比源端长就可以。
             *   c、集合数据类型（通过ObjAccess访问）：调用该类型的COPY函数，其COPY函数递归进行子域的COPY，不必修改param_store里的值
             *      注意： NestedTable的数据域的结构内存是自身的allocator_分配的，同时String/Number指针指向的内存也是。
             *           copy之后的Collection同样也是这样。如果有母子关系，Collection的allocator_也相互独立，没有任何所属关系。
             * 2、PKG变量
             *   PKG变量只能通过通过ObjAccess访问访问，同1.b和1.c
             * 3、USER/SYS变量
             *   通过SPI直接赋值。
             * */
            if (into_expr->is_const_raw_expr()) { //Local Basic Variables
              const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(into_expr);
              const ObPLVar *var = NULL;
              CK (OB_NOT_NULL(const_expr));
              CK (OB_NOT_NULL(var = s.get_variable(const_expr->get_value().get_unknown())));
              OZ (generator_.generate_check_not_null(s, var->is_not_null(), p_result_obj));
              if (OB_SUCC(ret)) {
                int64_t idx = const_expr->get_value().get_unknown();
                const ObPLDataType &into_type = var->get_type();
                ObLLVMValue datum;
                ObLLVMValue &var_addr = generator_.get_vars().at(idx + generator_.USER_ARG_OFFSET);
                if (!into_type.is_collection_type() && !into_type.is_record_type()
                    && !into_type.is_ref_cursor_type()) {
                  OZ (generator_.extract_datum_from_objparam(
                    p_result_obj, into_expr->get_result_type().get_type(), datum));
                  OZ (generator_.get_helper().create_store(datum, var_addr));
                }
                if (OB_SUCC(ret)
                    && (ObObjAccessIdx::IS_INVALID != alloc_scop
                        || into_type.is_collection_type()
                        || into_type.is_record_type()
                        || into_type.is_ref_cursor_type())) {
                  ObLLVMValue into_obj;
                  OZ (generator_.extract_objparam_from_context(generator_.get_vars().at(generator_.CTX_IDX), idx, into_obj));
                  if (into_type.is_obj_type()) {
                    OZ (generator_.extract_datum_ptr_from_objparam(
                      into_obj, into_expr->get_result_type().get_type(), dest_datum));
                    OZ (generator_.extract_datum_ptr_from_objparam(
                      p_result_obj, into_expr->get_result_type().get_type(), src_datum));
                    OZ (into_type.generate_copy(generator_,
                                                *ns,
                                                allocator,
                                                src_datum,
                                                dest_datum,
                                                s.get_block()->in_notfound(),
                                                s.get_block()->in_warning(),
                                                package_id));
                  } else if (into_type.is_ref_cursor_type()) {
                    OZ (generator_.extract_datum_ptr_from_objparam(
                      p_result_obj, into_expr->get_result_type().get_type(), src_datum));
                    #ifndef NDEBUG
                      OZ (generator_.generate_debug(ObString("ref cursor data"), src_datum));
                    #endif
                    // ref cursor 主要调用ref cursortype的generate copy函数，处理cursor的引用计数
                    OZ (into_type.generate_copy(generator_,
                                                *ns,
                                                allocator,
                                                src_datum,
                                                var_addr,
                                                s.get_block()->in_notfound(),
                                                s.get_block()->in_warning(),
                                                package_id));
                  } else {
                    OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, src_datum));
                    OZ (generator_.extract_obobj_ptr_from_objparam(into_obj, dest_datum));
                    OZ (into_type.generate_copy(generator_,
                                                *ns,
                                                allocator,
                                                src_datum,
                                                dest_datum,
                                                s.get_block()->in_notfound(),
                                                s.get_block()->in_warning(),
                                                package_id));
                  }
                }
                // 处理Nocopy参数
                if (OB_SUCC(ret) && generator_.get_param_size() > 0) {
                  ObSEArray<ObLLVMValue, 2> args;
                  ObLLVMValue ret_err;
                  ObLLVMValue idx_value;
                  OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                  OZ (generator_.get_helper().get_int64(idx, idx_value));
                  OZ (args.push_back(idx_value));
                  OZ (generator_.get_helper().create_call(
                    ObString("spi_process_nocopy_params"),
                    generator_.get_spi_service().spi_process_nocopy_params_,
                    args, ret_err));
                  OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                               s.get_block()->in_notfound(),
                                               s.get_block()->in_warning()));
                }
              }
            //Sys Var/User Var or PKG Basic Variables or Subprogram Basic Variables
            } else if (into_expr->is_sys_func_expr()) {
              CK (OB_NOT_NULL(value_expr));
              OZ (generator_.generate_set_variable(s.get_into_index(i),
                                                   p_result_obj,
                                                   T_DEFAULT == value_expr->get_expr_type(),
                                                   s.get_stmt_id(),
                                                   s.get_block()->in_notfound(),
                                                   s.get_block()->in_warning()));
            } else if (into_expr->is_obj_access_expr()) { //ADT
              ObLLVMValue into_address;
              ObPLDataType final_type;
              OZ (generator_.generate_expr(s.get_into_index(i), s, OB_INVALID_INDEX, into_address));
              if (s.get_value_index(i) != PL_CONSTRUCT_COLLECTION
                  && ObObjAccessIdx::has_same_collection_access(s.get_value_expr(i), static_cast<const ObObjAccessRawExpr *>(into_expr))) {
                OZ (generator_.generate_expr(s.get_value_index(i), s, result_idx, p_result_obj));
              }
              OZ (static_cast<const ObObjAccessRawExpr*>(into_expr)->get_final_type(final_type));
              if (s.get_value_index(i) != PL_CONSTRUCT_COLLECTION) {
                OZ (generator_.generate_check_not_null(s, final_type.get_not_null(), p_result_obj));
              }

              if (OB_FAIL(ret)) {
              } else if (final_type.is_obj_type()) {
                OZ (generator_.extract_extend_from_objparam(into_address, final_type, dest_datum));
                if (ObObjAccessIdx::IS_INVALID != alloc_scop) {

                  OZ (generator_.extract_datum_ptr_from_objparam(
                                              p_result_obj,
                                              into_expr->get_result_type().get_type(),
                                              src_datum));
                  OZ (final_type.generate_copy(generator_,
                                               *ns,
                                               allocator,
                                               src_datum,
                                               dest_datum,
                                               s.get_block()->in_notfound(),
                                               s.get_block()->in_warning(),
                                               package_id));
                } else {
                  ObLLVMValue ori_meta_p, into_meta_p, ori_meta;
                  ObLLVMValue ori_accuracy_p, into_accuracy_p, ori_accuracy;

                  OZ (generator_.extract_datum_from_objparam(
                    p_result_obj, into_expr->get_result_type().get_type(), src_datum));
                  OZ (generator_.get_helper().create_store(src_datum, dest_datum));
                  OZ (generator_.extract_meta_ptr_from_objparam(p_result_obj, ori_meta_p));
                  OZ (generator_.extract_meta_ptr_from_objparam(into_address, into_meta_p));
                  OZ (generator_.get_helper().create_load(ObString("load_meta"), ori_meta_p, ori_meta));
                  OZ (generator_.get_helper().create_store(ori_meta, into_meta_p));
                  OZ (generator_.extract_accuracy_ptr_from_objparam(p_result_obj, ori_accuracy_p));
                  OZ (generator_.extract_accuracy_ptr_from_objparam(into_address, into_accuracy_p));
                  OZ (generator_.get_helper().create_load(ObString("load_accuracy"), ori_accuracy_p, ori_accuracy));
                  OZ (generator_.get_helper().create_store(ori_accuracy, into_accuracy_p));
                }
              } else {
                // 设置复杂类型的ID
                ObLLVMValue dest;
                ObLLVMValue src;
                OZ (generator_.extract_accuracy_ptr_from_objparam(into_address, dest));
                OZ (generator_.get_helper().get_int64(final_type.get_user_type_id(), src));
                OZ (generator_.get_helper().create_store(src, dest));

                if (OB_FAIL(ret)) {
                } else if (PL_CONSTRUCT_COLLECTION == s.get_value_index(i)){//右值是个数组的构造函数走SPI初始化函数
                  ObSEArray<ObLLVMValue, 2> args;
                  ObLLVMValue ret_err;
                  ObLLVMValue v_package_id;
                  CK (final_type.is_collection_type());
                  OZ (generator_.get_helper().get_int64(package_id, v_package_id));
                  OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                  OZ (args.push_back(v_package_id));
                  OZ (args.push_back(into_address));
                  OZ (generator_.get_helper().create_call(
                    ObString("spi_construct_collection"),
                    generator_.get_spi_service().spi_construct_collection_,
                    args, ret_err));
                  OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                               s.get_block()->in_notfound(),
                                               s.get_block()->in_warning()));
                } else {
                  OZ (generator_.extract_extend_from_objparam(into_address,
                                                              final_type,
                                                              dest_datum));
                  if (OB_SUCC(ret) && final_type.is_collection_type()) { //如果是表，用表自带的allocator
                    OZ (generator_.extract_allocator_from_collection(dest_datum,allocator),
                        *into_expr);
                  }
                  CK (OB_NOT_NULL(value_expr));
                  // 这儿的dest_datum不一定是一个复杂类型，它可能是一个null
                  // 如 rec := null这样的赋值，这个时候如果extract_extend，结果的类型其实是错误的。
                  // 所以这儿需要区分一下。
                  if (OB_FAIL(ret)) {
                  } else if (T_NULL == value_expr->get_expr_type()) {
                    OZ (final_type.generate_assign_with_null(generator_,
                                                             *ns,
                                                             allocator,
                                                             dest_datum));
                  } else {
                    OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, src_datum));
                    OZ (generator_.extract_obobj_ptr_from_objparam(into_address, dest_datum));
                    OZ (final_type.generate_copy(generator_,
                                                 *ns,
                                                 allocator,
                                                 src_datum,
                                                 dest_datum,
                                                 s.get_block()->in_notfound(),
                                                 s.get_block()->in_warning(),
                                                 package_id));
                    OZ (generator_.generate_debug("print dest obj2", dest_datum));
                    OZ (generator_.generate_debug("print dest objparam2", into_address));
                  }
                }
              }
              if (OB_SUCC(ret) && package_id != OB_INVALID_ID && var_idx != OB_INVALID_ID) {
                OZ (generator_.generate_update_package_changed_info(s, package_id, var_idx));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Invalid into expr type", K(*into_expr), K(ret));
            }
          }
        }
      }
    }
    OZ (generator_.get_helper().stack_restore(stack));

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLIfStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("faile to create goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    ObLLVMBasicBlock continue_branch;
    ObLLVMBasicBlock then_branch;
    ObLLVMBasicBlock else_branch;
    ObLLVMBasicBlock current = generator_.get_current();

    if (OB_FAIL(generator_.get_helper().create_block(ObString("continue"), generator_.get_func(), continue_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("then"), generator_.get_func(), then_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(generator_.set_current(then_branch))) {
      LOG_WARN("failed to set current", K(ret));
    } else {
      if (OB_ISNULL(s.get_then())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a if must have then body", K(s.get_then()), K(ret));
      } else if (OB_FAIL(visit(*s.get_then()))) {
        LOG_WARN("failed to visit then clause", K(ret));
      } else {
        if (OB_FAIL(generator_.finish_current(continue_branch))) {
          LOG_WARN("failed to finish current", K(ret));
        } else {
          if (NULL == s.get_else()) {
            else_branch = continue_branch;
          } else {
            if (OB_FAIL(generator_.get_helper().create_block(ObString("else"),
                                                             generator_.get_func(),
                                                             else_branch))) {
              LOG_WARN("failed to create block", K(ret));
            } else if (OB_FAIL(generator_.set_current(else_branch))) {
              LOG_WARN("failed to set current", K(ret));
            } else if (OB_FAIL(visit(*s.get_else()))) {
              LOG_WARN("failed to visit else clause", K(ret));
            } else if (OB_FAIL(generator_.finish_current(continue_branch))) {
              LOG_WARN("failed to finish current", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(generator_.set_current(current))) {
          LOG_WARN("failed to set current", K(ret));
        } else if (OB_FAIL(generator_.set_debug_location(s))) {
          LOG_WARN("failed to set debug location", K(ret));
        } else {
          const sql::ObRawExpr *expr = s.get_cond_expr();
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("a assign stmt must has expr", K(expr), K(ret));
          } else {
            ObLLVMValue p_result_obj;
            ObLLVMValue result;
            ObLLVMValue is_false;
            if (OB_FAIL(generator_.generate_expr(s.get_cond(), s, OB_INVALID_INDEX,
                                                 p_result_obj))) {
              LOG_WARN("failed to generate calc_expr func", K(ret));
            } else if (OB_FAIL(generator_.extract_value_from_objparam(p_result_obj,
                                                                      expr->get_data_type(),
                                                                      result))) {
              LOG_WARN("failed to extract_value_from_objparam", K(ret));
            } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(result, FALSE, is_false))) {
              LOG_WARN("failed to create_icmp_eq", K(ret));
            } else if (OB_FAIL(generator_.get_helper().create_cond_br(is_false, else_branch, then_branch))) {
              LOG_WARN("failed to create_cond_br", K(ret));
            } else if (OB_FAIL(generator_.set_current(continue_branch))) {
              LOG_WARN("failed to set current", K(ret));
            } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
              LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
            } else { /*do nothing*/ }
          }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLLeaveStmt &s)
{
  return generator_.generate_loop_control(s);
}

int ObPLCodeGenerateVisitor::visit(const ObPLIterateStmt &s)
{
  return generator_.generate_loop_control(s);
}

int ObPLCodeGenerateVisitor::visit(const ObPLWhileStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("faile to generate goto lab", K(ret));
  } else {
    ObLLVMBasicBlock while_begin;
    ObLLVMBasicBlock continue_begin;
    ObLLVMBasicBlock do_body;
    ObLLVMBasicBlock alter_while;
    ObLLVMValue count_value;

    if (OB_FAIL(generator_.get_helper().create_block(ObString("while_begin"), generator_.get_func(), while_begin))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("continue_begin"),
                                                            generator_.get_func(), continue_begin))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("do_body"), generator_.get_func(), do_body))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("after_while"), generator_.get_func(), alter_while))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_ialloca(ObString("count_value"), ObIntType, 0, count_value))) {
      LOG_WARN("failed to create_ialloca", K(ret));
    } else if (s.has_label() && OB_FAIL(generator_.set_label(s, while_begin, alter_while))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(while_begin))) {
      LOG_WARN("failed to create_br", K(ret));
    } else if (OB_FAIL(generator_.set_current(while_begin))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else {
      ObLLVMValue p_result_obj;
      ObLLVMValue result;
      ObLLVMValue is_false;
      ObLLVMValue stack;
      if (OB_ISNULL(s.get_body()) || OB_ISNULL(s.get_cond_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a if must have then body", K(s), K(s.get_body()), K(s.get_cond()), K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_br(continue_begin))) {
        LOG_WARN("failed to create_br", K(ret));
      } else if (OB_FAIL(generator_.set_current(continue_begin))) {
        LOG_WARN("failed to set current", K(s), K(ret));
      } else if (OB_FAIL(generator_.get_helper().stack_save(stack))) {
        LOG_WARN("failed to stack_save", K(ret));
      } else if (OB_FAIL(generator_.generate_expr(s.get_cond(), s, OB_INVALID_INDEX,
                                                  p_result_obj))) {
        LOG_WARN("failed to generate calc_expr func", K(ret));
      } else if (OB_FAIL(generator_.extract_value_from_objparam(p_result_obj, s.get_cond_expr()->get_data_type(), result))) {
        LOG_WARN("failed to extract_value_from_objparam", K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(result, FALSE, is_false))) {
        LOG_WARN("failed to create_icmp_eq", K(ret));
      } else if (OB_FAIL(generator_.get_helper().stack_restore(stack))) {
        LOG_WARN("failed to stack_restore", K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_cond_br(is_false, alter_while, do_body))) {
        LOG_WARN("failed to create_cond_br", K(ret));
      } else if (OB_FAIL(generator_.set_current(do_body))) {
        LOG_WARN("failed to set current", K(s), K(ret));
      } else {
        if (OB_FAIL(generator_.get_helper().stack_save(stack))) {
          LOG_WARN("failed to stack_save", K(ret));
        } else if (OB_FAIL(generator_.set_loop(stack, s.get_level(), continue_begin, alter_while))) {
          LOG_WARN("failed to set loop stack", K(ret));
        } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
          LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
        } else if (OB_FAIL(SMART_CALL(generate(*s.get_body())))) {
          LOG_WARN("failed to generate exception body", K(ret));
        } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
          LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
        } else if (OB_FAIL(generator_.reset_loop())) {
          LOG_WARN("failed to reset loop stack", K(ret));
        } else if (NULL != generator_.get_current().get_v()) {
          if (OB_FAIL(generator_.generate_early_exit(count_value, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
            LOG_WARN("failed to generate calc_expr func", K(ret));
          } else if (OB_FAIL(generator_.generate_expr(s.get_cond(), s, OB_INVALID_INDEX,
                                                      p_result_obj))) {
            LOG_WARN("failed to generate calc_expr func", K(ret));
          } else if (OB_FAIL(generator_.extract_value_from_objparam(p_result_obj, s.get_cond_expr()->get_data_type(), result))) {
            LOG_WARN("failed to extract_value_from_objparam", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(result, FALSE, is_false))) {
            LOG_WARN("failed to create_icmp_eq", K(ret));
          } else if (OB_FAIL(generator_.get_helper().stack_restore(stack))) {
            LOG_WARN("failed to stack_restore", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_cond_br(is_false, alter_while, do_body))) {
            LOG_WARN("failed to create_cond_br", K(ret));
          } else { /*do nothing*/ }
        } else { /*do nothing*/ }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(generator_.set_current(alter_while))) {
            LOG_WARN("failed to set current", K(s), K(ret));
          } else if (s.has_label() && OB_FAIL(generator_.reset_label())) {
            LOG_WARN("failed to reset_label", K(s), K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLForLoopStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto labe", K(ret));
  } else {
    ObLLVMBasicBlock forloop_begin;
    ObLLVMBasicBlock forloop_body;
    ObLLVMBasicBlock forloop_continue;
    ObLLVMBasicBlock forloop_end;
    OZ (generator_.get_helper().create_block(ObString("forloop_begin"), generator_.get_func(), forloop_begin));
    OZ (generator_.get_helper().create_block(ObString("forloop_body"), generator_.get_func(), forloop_body));
    OZ (generator_.get_helper().create_block(ObString("forloop_continue"), generator_.get_func(), forloop_continue));
    OZ (generator_.get_helper().create_block(ObString("forloop_end"), generator_.get_func(), forloop_end));
    if (s.has_label()) {
      OZ (generator_.set_label(s, forloop_continue, forloop_end));
    }
    OZ (generator_.get_helper().create_br(forloop_begin));
    OZ (generator_.set_current(forloop_begin));
    if (OB_SUCC(ret)) {
      ObLLVMValue count_value;
      ObLLVMValue p_lower_obj, p_upper_obj, p_index_obj, p_index_value;
      ObLLVMValue lower_obj, upper_obj, index_obj;
      ObLLVMValue lower_value, upper_value, index_value;
      ObLLVMValue is_true, stack;
      ObLLVMValue allocator, into_obj, src_datum, dest_datum;
      ObLLVMType ir_type;
      const ObPLVar *var = s.get_index_var();
      // 生成INDEX指针
      CK (OB_NOT_NULL(var));
      OZ (generator_.get_helper().create_ialloca(ObString("count_value"), ObIntType, 0, count_value));
      OZ (generator_.get_llvm_type(var->get_type(), ir_type));
      OZ (generator_.get_helper().create_alloca(var->get_name(), ir_type, p_index_obj));
      OZ (generator_.extract_allocator_from_context(generator_.get_vars().at(generator_.CTX_IDX), allocator));
      OZ (generator_.extract_objparam_from_context(generator_.get_vars().at(generator_.CTX_IDX), s.get_ident(), into_obj));
      OZ (generator_.extract_datum_ptr_from_objparam(into_obj, var->get_type().get_obj_type(), dest_datum));
      OX (generator_.get_vars().at(s.get_ident() + generator_.USER_ARG_OFFSET) = p_index_obj);
      // 计算上界与下界
      if (OB_SUCC(ret)) {
        OZ (generator_.generate_bound_and_check(s, s.get_is_forall(), lower_value, upper_value, lower_obj, upper_obj, forloop_end));
      }
      // 给INDEX附初始值，并存储进param_stroe
      if (OB_SUCC(ret)) {
        if (!s.get_reverse()) {
          // 下标从lower bound开始
          OZ (generator_.get_helper().create_store(lower_obj, p_index_obj));
          OZ (generator_.get_helper().create_store(lower_obj, dest_datum));
        } else {
          // 下标从upper bound开始
          OZ (generator_.get_helper().create_store(upper_obj, p_index_obj));
          OZ (generator_.get_helper().create_store(upper_obj, dest_datum));
        }
      }
      if (s.is_values_bound()) {
        ObLLVMValue p_value;
        OZ (generator_.generate_expr(s.get_value(), s, s.get_ident(), p_value));
      }
      // 跳转到loop body
      if (OB_SUCC(ret)) {
        OZ (generator_.get_helper().create_br(forloop_body));
        OZ (generator_.set_current(forloop_body));
      }
      // 生成循环体
      if (OB_SUCC(ret)) {
        OZ (generator_.get_helper().stack_save(stack));
        OZ (generator_.set_loop(stack, s.get_level(), forloop_continue, forloop_end));
        OZ (generator_.generate_early_exit(count_value, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
        OZ (generator_.generate_spi_pl_profiler_before_record(s));
        OZ (SMART_CALL(generate(*s.get_body())));
        OZ (generator_.generate_spi_pl_profiler_after_record(s));
        OZ (generator_.reset_loop());
        if (OB_SUCC(ret)
            && NULL != generator_.get_current().get_v()
            && generator_.get_current().get_v() != generator_.get_exit().get_v()) {
          OZ (generator_.set_current(generator_.get_current()));
          OZ (generator_.get_helper().create_br(forloop_continue));
        }
      }

      OZ (generator_.set_current(forloop_continue));
      OZ (generator_.generate_next_and_check(s,
                                             p_index_obj,
                                             p_index_value,
                                             index_obj,
                                             index_value,
                                             dest_datum,
                                             lower_value,
                                             upper_value,
                                             is_true));
      // 根据条件跳转
      if (OB_SUCC(ret)) {
        OZ (generator_.get_helper().stack_restore(stack));
        OZ (generator_.get_helper().create_cond_br(is_true, forloop_body, forloop_end));
      }

      // forloop_end
      if (OB_SUCC(ret)) {
        OZ (generator_.set_current(forloop_end));
        if (s.has_label()) {
          OZ (generator_.reset_label());
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLCursorForLoopStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto labe", K(ret));
  } else {
    ObLLVMBasicBlock cursor_forloop_begin;
    ObLLVMBasicBlock cursor_forloop_body;
    ObLLVMBasicBlock cursor_forloop_end;
    ObLLVMBasicBlock cursor_forloop_fetch;
    ObLLVMBasicBlock cursor_forloop_check_success;
    ObLLVMValue ret_err;
    ObLLVMValue count_value;
    if (OB_FAIL(generator_.get_helper().create_block(ObString("cursor_forloop_begin"), generator_.get_func(), cursor_forloop_begin))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("cursor_forloop_body"), generator_.get_func(), cursor_forloop_body))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("cursor_forloop_end"), generator_.get_func(), cursor_forloop_end))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("cursor_forloop_fetch"), generator_.get_func(), cursor_forloop_fetch))) {
      LOG_WARN("failed to craete block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("cursor_forloop_check_success"), generator_.get_func(), cursor_forloop_check_success))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (s.has_label() && OB_FAIL(generator_.set_label(s, cursor_forloop_fetch, cursor_forloop_end))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(cursor_forloop_begin))) {
      LOG_WARN("failed to create_br", K(ret));
    }
    if (OB_SUCC(ret)) {
      const ObUserDefinedType *user_defined_type = s.get_user_type();
      const ObPLVar *var = s.get_index_var();
      if (OB_FAIL(generator_.set_current(cursor_forloop_begin))) {
        LOG_WARN("failed to set current", K(s), K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_ialloca(ObString("count_value"), ObIntType, 0, count_value))) {
        LOG_WARN("failed to create_ialloca", K(ret));
      } else if (OB_ISNULL(user_defined_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user defined type is null");
      } else if (OB_FAIL(generator_.generate_user_type(*user_defined_type))) {
        LOG_WARN("failed to generate user type", K(user_defined_type->get_type()));
      } else if (OB_ISNULL(var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user var is null", K(s), K(ret));
      } else {
        if (var->get_type().is_composite_type()) {
          ObSEArray<ObLLVMValue, 3> args;
          ObLLVMValue var_idx, init_value, var_value, extend_value;
          ObLLVMValue ret_err;
          ObLLVMValue var_type, type_id;
          ObLLVMValue null_int;
          int64_t init_size = 0;
          CK (OB_NOT_NULL(s.get_namespace()));
          OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
          OZ (generator_.get_helper().get_int8(var->get_type().get_type(), var_type));
          OZ (args.push_back(var_type));
          OZ (generator_.get_helper().get_int64(var->get_type().get_user_type_id(), type_id));
          OZ (args.push_back(type_id));
          OZ (generator_.get_helper().get_int64(s.get_index_index(), var_idx));
          OZ (args.push_back(var_idx));
          OZ (s.get_namespace()->get_size(PL_TYPE_INIT_SIZE, var->get_type(), init_size));
          OZ (generator_.get_helper().get_int32(init_size, init_value));
          OZ (args.push_back(init_value));
          OZ (generator_.generate_null_pointer(ObIntType, null_int));
          OZ (args.push_back(null_int));
          OZ (generator_.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                               generator_.get_spi_service().spi_alloc_complex_var_,
                                               args,
                                               ret_err));
          OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                       s.get_block()->in_notfound(),
                                       s.get_block()->in_warning()));
          OZ (generator_.extract_objparam_from_context(
              generator_.get_vars().at(generator_.CTX_IDX),
              s.get_index_index(),
              var_value));
          OZ (generator_.extract_extend_from_objparam(var_value, var->get_type(), extend_value));
          OZ (var->get_type().generate_construct(generator_, *s.get_namespace(),
                                                 extend_value, &s));
          OZ (generator_.set_var_addr_to_param_store(s.get_index_index(), extend_value, init_value));
          generator_.get_vars().at(s.get_index_index() + generator_.USER_ARG_OFFSET) = extend_value;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cursor index is not a record", K(*var), K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(s.get_cursor())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cursor is null", K(s.get_cursor_index()), K(s.get_cursor()), K(ret));
          } else if (s.get_need_declare()
              && generator_.generate_declare_cursor(static_cast<const ObPLStmt&>(s),
                                                    s.get_cursor_index())) {
            LOG_WARN("failed to generate declare cursor", K(ret));
          } else if (OB_FAIL(generator_.generate_open(static_cast<const ObPLStmt&>(s),
                                                      s.get_cursor()->get_value(),
                                                      s.get_cursor()->get_package_id(),
                                                      s.get_cursor()->get_routine_id(),
                                                      s.get_index()))) {
            LOG_WARN("failed to generate_open", K(s), K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_br(cursor_forloop_fetch))) {
            LOG_WARN("failed to create_br cursor_forloop_fetch", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObLLVMValue is_not_found;
      ObLLVMValue stack;
      CK (OB_NOT_NULL(s.get_cursor()));
      OZ (generator_.set_current(cursor_forloop_fetch));
      OZ (generator_.get_helper().stack_save(stack));
      OZ (generator_.set_loop(stack, s.get_level(), cursor_forloop_fetch, cursor_forloop_end, &s));
      OZ (generator_.generate_spi_pl_profiler_before_record(s));
      OZ (generator_.generate_fetch(static_cast<const ObPLStmt&>(s),
                                    static_cast<const ObPLInto&>(s),
                                    s.get_cursor()->get_package_id(),
                                    s.get_cursor()->get_routine_id(),
                                    s.get_index(),
                                    static_cast<int64_t>(INT64_MAX),
                                    s.get_user_type(),
                                    ret_err));
      OZ (generator_.get_helper().create_icmp_eq(ret_err, OB_READ_NOTHING, is_not_found));
      OZ (generator_.get_helper().stack_restore(stack));
      OZ (generator_.get_helper().create_cond_br(is_not_found, cursor_forloop_end, cursor_forloop_check_success));
    }
    if (OB_SUCC(ret)) {
      ObLLVMValue stack;
      OZ (generator_.set_current(cursor_forloop_check_success));
      OZ (generator_.get_helper().stack_save(stack));
      OZ (generator_.check_success(ret_err,
                                   s.get_stmt_id(),
                                   s.get_block()->in_notfound(),
                                   s.get_block()->in_warning()));
      OZ (generator_.get_helper().stack_restore(stack));
      OZ (generator_.get_helper().create_br(cursor_forloop_body));
    }
    // 生成循环体
    if (OB_SUCC(ret)) {
      ObLLVMValue stack;
      if (OB_FAIL(generator_.set_current(cursor_forloop_body))) {
        LOG_WARN("failed to set current", K(s), K(ret));
      } else if (OB_FAIL(generator_.get_helper().stack_save(stack))) {
        LOG_WARN("failed to stack_save", K(ret));
      } else if (OB_FAIL(SMART_CALL(generate(*s.get_body())))) {
        LOG_WARN("failed to generate exception body", K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
      } else if (OB_FAIL(generator_.reset_loop())) {
        LOG_WARN("failed to reset loop stack", K(ret));
      } else { /*do nothing*/ }
      // 重置堆栈，并且跳转到循环头
      if (OB_SUCC(ret)
          && NULL != generator_.get_current().get_v()
          && generator_.get_current().get_v() != generator_.get_exit().get_v()) {
        if (OB_FAIL(generator_.set_current(generator_.get_current()))) {
          LOG_WARN("failed to set current", K(ret));
        } else if (OB_FAIL(generator_.generate_early_exit(count_value, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
          LOG_WARN("failed to generate calc_expr func", K(ret));
        } else if (OB_FAIL(generator_.get_helper().stack_restore(stack))) {
          LOG_WARN("failed to stack_restore", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_br(cursor_forloop_fetch))) {
          LOG_WARN("failed to create_br", K(ret));
        }
      }
    }
    // 循环结束, 关闭游标
    if (OB_SUCC(ret)) {
      OZ (generator_.set_current(cursor_forloop_end));
      CK (OB_NOT_NULL(s.get_cursor()));
      OZ (generator_.generate_close(static_cast<const ObPLStmt&>(s),
                                    s.get_cursor()->get_package_id(),
                                    s.get_cursor()->get_routine_id(),
                                    s.get_index()));
      if (s.has_label()) {
        OZ (generator_.reset_label());
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLForAllStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    // 控制流已断, 后面的语句不再处理
  } else {
    OZ (generator_.get_helper().set_insert_point(generator_.get_current()));
    OZ (generator_.set_debug_location(s));
    OZ (generator_.generate_goto_label(s));
  }
  // 设置隐式游标在forall语句中
  if (OB_SUCC(ret) && OB_NOT_NULL(generator_.get_current().get_v())) {
    ObSEArray<ObLLVMValue, 2> args;
    ObLLVMValue ret_err;
    ObLLVMValue is_save_exception;
    OZ (generator_.get_helper().get_int8(static_cast<int64_t>(s.get_save_exception()), is_save_exception));
    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
    OZ (args.push_back(is_save_exception));
    OZ (generator_.get_helper().create_call(ObString("set_implicit_cursor_in_forall"),
                                            generator_.get_set_implicit_cursor_in_forall_func(),
                                            args,
                                            ret_err));
    OZ (generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
  }
  // 执行forall，forall的循环体中只会有一句DML，不会出现跳转，因此这里不判断控制流是否断开
  if (OB_SUCC(ret) && OB_NOT_NULL(generator_.get_current().get_v())) {
    const ObPLForLoopStmt& for_loop = static_cast<const ObPLForLoopStmt&>(s);
    if (OB_NOT_NULL(s.get_sql_stmt()) && !s.get_save_exception()) {
      ObLLVMValue lower, upper, lower_obj, upper_obj, p_index_obj;
      ObLLVMValue ret_err, is_need_rollback;
      ObLLVMType ir_type;
      ObLLVMBasicBlock illegal_block, after_block, rollback_block, not_rollback_block;
      ObPLSqlStmt *sql_stmt = const_cast<ObPLSqlStmt*>(s.get_sql_stmt());
      const ObPLVar *var = s.get_index_var();
      sql_stmt->set_forall_sql(true);
      CK (OB_NOT_NULL(var));
      OZ (generator_.get_helper().create_block(ObString("illegal_block"), generator_.get_func(), illegal_block));
      OZ (generator_.get_helper().create_block(ObString("after_block"), generator_.get_func(), after_block));
      OZ (generator_.get_helper().create_block(ObString("rollback_block"), generator_.get_func(), rollback_block));
      OZ (generator_.get_helper().create_block(ObString("not_rollback_block"), generator_.get_func(), not_rollback_block));
      OZ (generator_.get_llvm_type(var->get_type(), ir_type));
      OZ (generator_.get_helper().create_alloca(var->get_name(), ir_type, p_index_obj));
      OX (generator_.get_vars().at(s.get_ident() + generator_.USER_ARG_OFFSET) = p_index_obj);
      OZ (generator_.generate_bound_and_check(s, true, lower, upper, lower_obj, upper_obj, illegal_block));
      OZ (generator_.generate_sql(*(s.get_sql_stmt()), ret_err));
      OZ (generator_.get_helper().create_icmp_eq(ret_err, OB_BATCHED_MULTI_STMT_ROLLBACK, is_need_rollback));
      OZ (generator_.get_helper().create_cond_br(is_need_rollback, rollback_block, not_rollback_block));
      OZ (generator_.set_current(not_rollback_block));
      OZ (generator_.generate_after_sql(*(s.get_sql_stmt()), ret_err));
      OZ (generator_.get_helper().create_br(after_block));
      OZ (generator_.set_current(rollback_block));
      sql_stmt->set_forall_sql(false);
      OZ (visit(for_loop));
      OZ (generator_.get_helper().create_br(after_block));
      OZ (generator_.get_helper().set_insert_point(illegal_block));
      OZ (generator_.get_helper().create_br(after_block));
      OZ (generator_.set_current(after_block));
    } else {
      OZ (visit(for_loop));
    }
  }
  // 设置隐式游标不在forall环境中
  if (OB_SUCC(ret) && OB_NOT_NULL(generator_.get_current().get_v())) {
    ObSEArray<ObLLVMValue, 2> args;
    ObLLVMValue ret_err;
    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
    OZ (generator_.get_helper().create_call(ObString("unset_implicit_cursor_in_forall"),
                                                    generator_.get_unset_implicit_cursor_in_forall_func(),
                                                    args,
                                                    ret_err));
    OZ (generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLRepeatStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMBasicBlock repeat;
    ObLLVMBasicBlock alter_repeat;
    ObLLVMValue count_value;

    if (OB_FAIL(generator_.get_helper().create_block(ObString("repeat"), generator_.get_func(), repeat))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("after_repeat"), generator_.get_func(), alter_repeat))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (s.has_label() && OB_FAIL(generator_.set_label(s, repeat, alter_repeat))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_ialloca(ObString("count_value"), ObIntType, 0, count_value))) {
      LOG_WARN("failed to create_ialloca", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(repeat))) {
      LOG_WARN("failed to create_br", K(ret));
    } else if (OB_FAIL(generator_.set_current(repeat))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else {
      ObLLVMValue stack;
      ObLLVMValue p_result_obj;
      ObLLVMValue result;
      ObLLVMValue is_false;
      if (OB_FAIL(generator_.get_helper().stack_save(stack))) {
        LOG_WARN("failed to stack_save", K(ret));
      } else if (OB_ISNULL(s.get_body()) || OB_ISNULL(s.get_cond_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a if must have then body", K(s), K(s.get_body()), K(s.get_cond()), K(ret));
      } else if (OB_FAIL(generator_.set_loop(stack, s.get_level(), repeat, alter_repeat))) {
        LOG_WARN("failed to set loop stack", K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
        LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
      } else if (OB_FAIL(SMART_CALL(generate(*s.get_body())))) {
        LOG_WARN("failed to generate exception body", K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
      } else if (OB_FAIL(generator_.reset_loop())) {
        LOG_WARN("failed to reset loop stack", K(ret));
      } else if (NULL != generator_.get_current().get_v()) {
        if (OB_FAIL(generator_.generate_early_exit(count_value, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
          LOG_WARN("failed to generate calc_expr func", K(ret));
        } else if (OB_FAIL(generator_.generate_expr(s.get_cond(), s, OB_INVALID_INDEX,
                                                    p_result_obj))) {
          LOG_WARN("failed to generate calc_expr func", K(ret));
        } else if (OB_FAIL(generator_.extract_value_from_objparam(p_result_obj, s.get_cond_expr()->get_data_type(), result))) {
          LOG_WARN("failed to extract_value_from_objparam", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(result, FALSE, is_false))) {
          LOG_WARN("failed to create_icmp_eq", K(ret));
        } else if (OB_FAIL(generator_.get_helper().stack_restore(stack))) {
          LOG_WARN("failed to stack_restore", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_cond_br(is_false, repeat, alter_repeat))) {
          LOG_WARN("failed to create_cond_br", K(ret));
        }
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(generator_.set_current(alter_repeat))) {
          LOG_WARN("failed to set current", K(s), K(ret));
        } else if (s.has_label() && OB_FAIL(generator_.reset_label())) {
          LOG_WARN("failed to reset_label", K(s), K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLLoopStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMBasicBlock loop;
    ObLLVMBasicBlock alter_loop;
    ObLLVMValue count_value;

    if (OB_FAIL(generator_.get_helper().create_block(ObString("loop"), generator_.get_func(), loop))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("after_loop"), generator_.get_func(), alter_loop))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (s.has_label() && OB_FAIL(generator_.set_label(s, loop, alter_loop))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_ialloca(ObString("count_value"), ObIntType, 0, count_value))) {
      LOG_WARN("failed to create_ialloca", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(loop))) {
      LOG_WARN("failed to create_br", K(ret));
    } else if (OB_FAIL(generator_.set_current(loop))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else {
      ObLLVMValue stack;
      if (OB_FAIL(generator_.get_helper().stack_save(stack))) {
        LOG_WARN("failed to stack_save", K(ret));
      } else if (OB_FAIL(generator_.set_loop(stack, s.get_level(), loop, alter_loop))) {
        LOG_WARN("failed to set loop stack", K(ret));
      } else if (OB_ISNULL(s.get_body())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a if must have valid body", K(s), K(s.get_body()), K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
        LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
      } else if (OB_FAIL(SMART_CALL(generate(*s.get_body())))) {
        LOG_WARN("failed to generate exception body", K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
      } else if (OB_FAIL(generator_.reset_loop())) {
        LOG_WARN("failed to reset loop stack", K(ret));
      } else if (NULL != generator_.get_current().get_v()) {
        if (OB_FAIL(generator_.generate_early_exit(count_value, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
          LOG_WARN("failed to generate calc_expr func", K(ret));
        } else if (OB_FAIL(generator_.get_helper().stack_restore(stack))) {
          LOG_WARN("failed to stack_restore", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_br(loop))) {
          LOG_WARN("failed to create_cond_br", K(ret));
        } else { /*do nothing*/ }
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(generator_.set_current(alter_loop))) {
          LOG_WARN("failed to set current", K(s), K(ret));
        } else if (s.has_label() && OB_FAIL(generator_.reset_label())) {
          LOG_WARN("failed to reset_label", K(s), K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLReturnStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMBasicBlock normal_return;
    if (OB_FAIL(generator_.get_helper().create_block(ObString("return"), generator_.get_func(), normal_return))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(normal_return))) {
      LOG_WARN("failed to create_br", K(ret));
    } else if (OB_FAIL(generator_.set_current(normal_return))) {
      LOG_WARN("failed to set current", K(s), K(ret));
    } else if (OB_FAIL(generator_.set_debug_location(s))) {
      LOG_WARN("failed to set debug location", K(ret));
    } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
      LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
    } else if (s.get_ret() != OB_INVALID_INDEX) {
      ObLLVMValue p_result_obj;
      ObLLVMValue result;
      ObLLVMValue p_result;
      OZ (generator_.generate_expr(s.get_ret(), s, OB_INVALID_INDEX, p_result_obj));
      if (OB_SUCC(ret) && lib::is_oracle_mode()) { // check logic need before store function ret value
        if (generator_.get_ast().is_autonomous() &&
            OB_FAIL(generator_.generate_check_autonomos(s))) {
          LOG_WARN("failed to generate_check_autonomos", K(ret));
        }
      }
      if (OB_SUCC(ret) && generator_.get_ast().get_ret_type().is_composite_type()) {
        //return NULL as UDT means returning a uninitialized UDT object
        ObLLVMBasicBlock null_branch;
        ObLLVMBasicBlock final_branch;
        ObLLVMValue p_type_value;
        ObLLVMValue type_value;
        ObLLVMValue is_null;
        ObLLVMValue p_obj_value;
        ObLLVMValue obj_value;
        const ObUserDefinedType *user_type = NULL;
        OZ (generator_.get_helper().create_block(ObString("null_branch"),
                                                 generator_.get_func(),
                                                 null_branch));
        OZ (generator_.get_helper().create_block(ObString("final_branch"),
                                                 generator_.get_func(),
                                                 final_branch));
        OZ (generator_.extract_type_ptr_from_objparam(p_result_obj, p_type_value));
        OZ (generator_.get_helper().create_load(ObString("load_type"), p_type_value, type_value));
        OZ (generator_.get_helper().create_icmp_eq(type_value, ObNullType, is_null));
        OZ (generator_.get_helper().create_cond_br(is_null, null_branch, final_branch));

        //null branch
        OZ (generator_.set_current(null_branch));
        OZ (s.get_namespace()->get_user_type(generator_.get_ast().get_ret_type().get_user_type_id(),
                                             user_type));

        ObSEArray<ObLLVMValue, 3> args;
        ObLLVMType int_type;
        jit::ObLLVMType ir_type, ir_ptr_type;
        ObLLVMValue var_idx, init_value, extend_ptr, extend_value, composite_ptr, p_obj;
        ObLLVMValue ret_err;
        ObLLVMValue var_type, type_id;
        int64_t init_size = 0;
        // Step 1: 初始化内存
        CK (OB_NOT_NULL(s.get_namespace()));
        OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
        OZ (generator_.get_helper().get_int8(user_type->get_type(), var_type));
        OZ (args.push_back(var_type));
        OZ (generator_.get_helper().get_int64(
            generator_.get_ast().get_ret_type().get_user_type_id(),
            type_id));
        OZ (args.push_back(type_id));
        OZ (generator_.get_helper().get_int64(OB_INVALID_INDEX, var_idx));
        OZ (args.push_back(var_idx));
        OZ (user_type->get_size(PL_TYPE_INIT_SIZE, init_size));
        OZ (generator_.get_helper().get_int32(init_size, init_value));
        OZ (args.push_back(init_value));
        OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
        OZ (generator_.get_helper().create_alloca(ObString("exception_type"), int_type, extend_ptr));
        OZ (args.push_back(extend_ptr));
        OZ (generator_.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                                generator_.get_spi_service().spi_alloc_complex_var_,
                                                args,
                                                ret_err));
        OZ (generator_.check_success(ret_err, s.get_stmt_id(),
                                     s.get_block()->in_notfound(),
                                     s.get_block()->in_warning()));
        // Step 2: 初始化类型内容, 如Collection的rowsize,element type等
        OZ (generator_.get_helper().create_load("load_extend", extend_ptr, extend_value));
        OZ (generator_.get_llvm_type(*user_type, ir_type));
        OZ (ir_type.get_pointer_to(ir_ptr_type));
        OZ (generator_.get_helper().create_int_to_ptr(ObString("cast_extend_to_ptr"), extend_value, ir_ptr_type, composite_ptr));
        OZ (user_type->ObPLDataType::generate_construct( //must call ObPLDataType's
            generator_, *s.get_namespace(), composite_ptr, &s));
        OZ (generator_.get_helper().create_gep(ObString("extract_obj_pointer"), p_result_obj, 0, p_obj));
        OZ (generator_.generate_set_extend(p_obj, var_type, init_value, extend_value));
        OZ (generator_.finish_current(final_branch));
        OZ (generator_.set_current(final_branch));
      }
      OZ (generator_.extract_obobj_from_objparam(p_result_obj, result));
      OZ (generator_.extract_result_from_context(
        generator_.get_vars().at(generator_.CTX_IDX), p_result));
      CK (OB_NOT_NULL(s.get_ret_expr()));
      if (OB_FAIL(ret)) {
      } else if (s.get_ret_expr()->is_obj_access_expr()) {
        const ObObjAccessRawExpr *obj_access
          = static_cast<const ObObjAccessRawExpr*>(s.get_ret_expr());
        const common::ObIArray<pl::ObObjAccessIdx> *access_idxs = NULL;
        ObPLDataType pl_type;
        int64_t local_idx = OB_INVALID_INDEX;
        CK (OB_NOT_NULL(obj_access));
        CK (OB_NOT_NULL(access_idxs = &(obj_access->get_access_idxs())));
        OZ (obj_access->get_final_type(pl_type));
        OX (local_idx = ObObjAccessIdx::get_local_variable_idx(*access_idxs));
        if (OB_FAIL(ret)) {
        } else if (ObObjAccessIdx::is_local_variable(*access_idxs)
                   && local_idx < access_idxs->count()
                   && access_idxs->at(local_idx).elem_type_.is_collection_type()
                   && pl_type.is_obj_type()) {
          ObLLVMValue allocator;
          ObLLVMValue src_datum;
          ObLLVMValue dest_datum;
          OZ (generator_.extract_allocator_from_context(
            generator_.get_vars().at(generator_.CTX_IDX), allocator));
          OZ (generator_.extract_datum_ptr_from_objparam(
            p_result_obj, pl_type.get_obj_type(), src_datum));
          OZ (generator_.extract_datum_ptr_from_objparam(
            p_result, pl_type.get_obj_type(), dest_datum));
          OZ (pl_type.generate_copy(generator_,
                                    *(s.get_namespace()),
                                    allocator,
                                    src_datum,
                                    dest_datum,
                                    s.get_block()->in_notfound(),
                                    s.get_block()->in_warning(),
                                    OB_INVALID_ID));
        } else {
          OZ (generator_.get_helper().create_store(result, p_result));
        }
      } else {
        OZ (generator_.get_helper().create_store(result, p_result));
      }
      if (OB_SUCC(ret) && s.is_return_ref_cursor_type()) {
        ObSEArray<ObLLVMValue, 16> args;
        ObLLVMValue addend;
        // check ref cursor
        OZ (args.push_back(generator_.get_vars()[generator_.CTX_IDX]));
        OZ (args.push_back(p_result));
        OZ (generator_.get_helper().get_int64(1, addend));
        OZ (args.push_back(addend));
        jit::ObLLVMValue ret_err;
        // 这儿为啥需要对ref count +1, 因为一个被return的ref cursor，在函数block结束的时候，会被dec ref
        // 那么这个时候可能会减到0， 从而导致这个被return的ref cursor被close了。
        // 这个+1，在ob_expr_udf那儿会对这个ref cursor -1进行平衡操作。
        OZ (generator_.get_helper().create_call(ObString("spi_add_ref_cursor_refcount"),
                                                generator_.get_spi_service().spi_add_ref_cursor_refcount_,
                                                args,
                                                ret_err));
        OZ (generator_.check_success(ret_err,
                                     s.get_stmt_id(),
                                     s.get_block()->in_notfound(),
                                     s.get_block()->in_warning()));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("generate return stmt, close cursor",
                                    K(generator_.get_ast().get_cursor_table().get_count()),
                                    K(s.get_stmt_id()),
                                    K(generator_.get_ast().get_name()));
      OZ (generator_.generate_close_loop_cursor(false, 0));

      // close cursor
      for (int64_t i = 0; OB_SUCC(ret) && i < generator_.get_ast().get_cursor_table().get_count(); ++i) {
        const ObPLCursor *cursor = generator_.get_ast().get_cursor_table().get_cursor(i);
        OZ (generator_.generate_handle_ref_cursor(cursor, s, false, false));
      }
      ObLLVMValue ret_value;
      ObLLVMBasicBlock null_block;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
      } else if (OB_FAIL(generator_.get_helper().create_load(ObString("load_ret"), generator_.get_vars().at(generator_.RET_IDX), ret_value))) {
        LOG_WARN("failed to create_load", K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_ret(ret_value))) {
        LOG_WARN("failed to create_ret", K(ret));
      } else if (OB_FAIL(generator_.set_current(null_block))) { // Return语句会结束函数, 切断控制流, 后面的语句不再Codegen
        LOG_WARN("failed to set current", K(s), K(ret));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLSqlStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    // 控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMValue ret_err;
    ObLLVMValue stack;
    OZ (generator_.generate_spi_pl_profiler_before_record(s));
    OZ (generator_.get_helper().stack_save(stack));
    OZ (generator_.generate_sql(s, ret_err));
    OZ (generator_.generate_after_sql(s, ret_err));
    OZ (generator_.get_helper().stack_restore(stack));
    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLExecuteStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else {
    ObLLVMType int_type;
    ObLLVMValue null_pointer;
    ObLLVMValue int_value;
    ObSEArray<ObLLVMValue, 16> args;
    ObLLVMValue sql_idx;
    ObLLVMValue params;
    ObLLVMType params_type;

    ObLLVMValue into_array_value;
    ObLLVMValue into_count_value;
    ObLLVMValue type_array_value;
    ObLLVMValue type_count_value;
    ObLLVMValue exprs_not_null_array_value;
    ObLLVMValue pl_integer_range_array_value;
    ObLLVMValue is_bulk;
    ObLLVMValue is_returning, is_type_record;
    ObLLVMValue ret_err;

    OZ (generator_.get_helper().set_insert_point(generator_.get_current()));
    OZ (generator_.set_debug_location(s));
    OZ (generator_.generate_goto_label(s));
    OZ (generator_.generate_spi_pl_profiler_before_record(s));

    OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
    OZ (generator_.generate_null_pointer(ObIntType, null_pointer));

    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX))); //PL的执行环境
    /*
     * 为什么sql和using同样都是表达式，sql直接传rawexpr，而using调用generate_expr传入obobjparam？？？
     * 这是因为直接传rawexpr，由spi_execute_immediate进行计算会省掉一次spi交互。
     * 而using不仅是传入参数，还可以做传出参数，所以必须用obobjparam向外传递结果。
     */
    OZ (generator_.get_helper().get_int64(s.get_sql(), sql_idx));
    OZ (args.push_back(sql_idx));

    // param exprs & param count
    ObLLVMType param_mode_arr_type; //param mode array elem type
    ObLLVMValue param_mode_arr_value; // param mode array
    ObLLVMType param_mode_arr_pointer;
    if (OB_FAIL(ret)) {
    } else if (s.get_using().empty()) {
      OZ (generator_.get_helper().create_ptr_to_int(
        ObString("cast_pointer_to_int"), null_pointer, int_type, int_value));
      OZ (args.push_back(int_value));
      // param mode
      OZ (args.push_back(null_pointer));
    } else {
      OZ (generator_.get_adt_service().get_argv(s.get_using().count(), params_type));
      OZ (generator_.get_helper().create_alloca(
        ObString("execute_immediate_params"), params_type, params));

      OZ (int_type.get_pointer_to(param_mode_arr_pointer));
      OZ (ObLLVMHelper::get_array_type(int_type,
                                      s.get_using().count(),
                                      param_mode_arr_type));
      OZ (generator_.get_helper().create_alloca(ObString("param_mode_array"),
                                param_mode_arr_type,
                                param_mode_arr_value));
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_using().count(); ++i) {
        ObLLVMValue p_result_obj;
        ObLLVMValue p_arg;
        ObLLVMValue pp_arg;
        ObLLVMValue param_mode_value;
        ObLLVMValue param_mode_arr_elem;
        OZ (generator_.get_helper().get_int64(s.get_using().at(i).mode_, param_mode_value));
        OZ (generator_.get_helper().create_gep(ObString("extract_param_mode"),
                               param_mode_arr_value, i, param_mode_arr_elem));
        OZ (generator_.get_helper().create_store(param_mode_value, param_mode_arr_elem));

#define GET_USING_EXPR(idx) (generator_.get_ast().get_expr(s.get_using_index(idx)))

        int64_t udt_id = GET_USING_EXPR(i)->get_result_type().get_udt_id();
        if (s.is_pure_out(i)) {
          OZ (generator_.generate_new_objparam(p_result_obj, udt_id));
          OZ (generator_.add_out_params(p_result_obj));
        } else if (!GET_USING_EXPR(i)->is_obj_access_expr()
                   || !(static_cast<const ObObjAccessRawExpr *>(GET_USING_EXPR(i))->for_write())) {
          OZ (generator_.generate_expr(s.get_using_index(i), s, OB_INVALID_INDEX, p_result_obj));
        } else {
          ObLLVMValue address;
          ObPLDataType final_type;
          const ObObjAccessRawExpr *obj_access
            = static_cast<const ObObjAccessRawExpr *>(GET_USING_EXPR(i));
          CK (OB_NOT_NULL(obj_access));
          OZ (obj_access->get_final_type(final_type));
          OZ (generator_.generate_expr(s.get_using_index(i),
                                       s,
                                       OB_INVALID_INDEX,
                                       address));
          if (OB_FAIL(ret)) {
          } else if (final_type.is_obj_type()) {
            ObLLVMType obj_type;
            ObLLVMType obj_type_ptr;
            ObLLVMValue p_obj;
            ObLLVMValue src_obj;
            ObLLVMValue p_dest_obj;
            OZ (generator_.generate_new_objparam(p_result_obj));
            OZ (generator_.add_out_params(p_result_obj));
            OZ (generator_.extract_extend_from_objparam(address, final_type, p_obj));
            OZ (generator_.get_adt_service().get_obj(obj_type));
            OZ (obj_type.get_pointer_to(obj_type_ptr));
            OZ (generator_.get_helper().create_bit_cast(
                ObString("cast_addr_to_obj_ptr"), p_obj, obj_type_ptr, p_obj));
            OZ (generator_.get_helper().create_load(ObString("load obj value"), p_obj, src_obj));
            OZ (generator_.extract_datum_ptr_from_objparam(p_result_obj, ObNullType, p_dest_obj));
            OZ (generator_.get_helper().create_store(src_obj, p_dest_obj));
          } else {
            ObLLVMValue allocator;
            ObLLVMValue src_datum;
            ObLLVMValue dest_datum;
            int64_t udt_id = GET_USING_EXPR(i)->get_result_type().get_udt_id();
            OZ (generator_.extract_allocator_from_context(
              generator_.get_vars().at(generator_.CTX_IDX), allocator));
            OZ (generator_.generate_new_objparam(p_result_obj, udt_id));
            OZ (generator_.add_out_params(p_result_obj));
            OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, dest_datum));
            OZ (generator_.extract_obobj_ptr_from_objparam(address, src_datum));
            OZ (final_type.generate_copy(generator_,
                                         *s.get_namespace(),
                                         allocator,
                                         src_datum,
                                         dest_datum,
                                         s.get_block()->in_notfound(),
                                         s.get_block()->in_warning(),
                                         OB_INVALID_ID));
          }
        }

#undef GET_USING_EXPR

        OZ (generator_.get_helper().create_ptr_to_int(
          ObString("cast_arg_to_pointer"), p_result_obj, int_type, p_arg));
        OZ (generator_.extract_arg_from_argv(params, i, pp_arg));
        OZ (generator_.get_helper().create_store(p_arg, pp_arg));
      }
      OZ (generator_.get_helper().create_ptr_to_int(
        ObString("cast_pointer_to_int"), params, int_type, int_value));
      OZ (args.push_back(int_value));

      OZ (generator_.get_helper().create_bit_cast(ObString("cast_param_mode_arr_pointer"),
            param_mode_arr_value, param_mode_arr_pointer, param_mode_arr_value));
      OZ (args.push_back(param_mode_arr_value));
    }

    OZ (generator_.get_helper().get_int64(s.get_using().count(), int_value));
    OZ (args.push_back(int_value));


    //result_exprs & result_count
    OZ (generator_.generate_into(s, into_array_value, into_count_value,
                                 type_array_value, type_count_value,
                                 exprs_not_null_array_value,
                                 pl_integer_range_array_value,
                                 is_bulk));
    OZ (args.push_back(into_array_value));
    OZ (args.push_back(into_count_value));
    OZ (args.push_back(type_array_value));
    OZ (args.push_back(type_count_value));
    OZ (args.push_back(exprs_not_null_array_value));
    OZ (args.push_back(pl_integer_range_array_value));
    OZ (args.push_back(is_bulk));
    OZ (generator_.get_helper().get_int8(s.get_is_returning(), is_returning));
    OZ (args.push_back(is_returning));
    OZ (generator_.get_helper().get_int8(s.is_type_record(), is_type_record));
    OZ (args.push_back(is_type_record));

    // execution
    OZ (generator_.get_helper().create_call(
      ObString("spi_execute_immediate"),
      generator_.get_spi_service().spi_execute_immediate_, args, ret_err));
    OZ (generator_.check_success(
      ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));

    // into result
    OZ (generator_.generate_into_restore(s.get_into(), s.get_exprs(), s.get_symbol_table()));

    // out result
    OZ (generator_.generate_out_params(s, s.get_using(), params));

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

#define COLLECTION_STMT_COMM(expr) \
    const ObObjAccessRawExpr *access_expr = NULL; \
    ObPLDataType type; \
    const ObUserDefinedType *user_type = NULL; \
    const ObPLBlockNS *ns = NULL; \
    uint64_t type_id = OB_INVALID_ID; \
    int64_t row_size = 0; \
    int64_t filed_cnt = 0; \
    uint64_t package_id = OB_INVALID_ID; \
    uint64_t var_idx = OB_INVALID_ID; \
    OZ (generator_.get_helper().set_insert_point(generator_.get_current())); \
    CK (OB_NOT_NULL(expr)); \
    CK (expr->is_obj_access_expr()); \
    CK (OB_NOT_NULL(access_expr = static_cast<const ObObjAccessRawExpr*>(expr))); \
    OZ (access_expr->get_final_type(type)); \
    CK (OB_INVALID_ID != (type_id = type.get_user_type_id())); \
    CK (OB_NOT_NULL(ns = s.get_namespace())); \
    CK (OB_NOT_NULL(s.get_namespace()->get_type_table())); \
    if (OB_SUCC(ret) \
        && OB_ISNULL(user_type = s.get_namespace()->get_type_table()->get_type(type_id))) { \
      CK (OB_NOT_NULL(user_type \
              = s.get_namespace()->get_type_table()->get_external_type(type_id))); \
    } \
    OZ (user_type->get_size(PL_TYPE_ROW_SIZE, row_size)); \
    OZ (user_type->get_field_count(*s.get_namespace(), filed_cnt)); \
    if (OB_SUCC(ret) \
        && ObObjAccessIdx::is_package_variable(access_expr->get_access_idxs())) { \
      OZ (ObObjAccessIdx::get_package_id(access_expr, package_id, &var_idx)); \
    }

int ObPLCodeGenerateVisitor::visit(const ObPLExtendStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    COLLECTION_STMT_COMM(s.get_extend_expr());

    if (OB_SUCC(ret)) {
      ObLLVMValue int_value;
      ObSEArray<ObLLVMValue, 16> args;
      if (OB_FAIL(args.push_back(generator_.get_vars().at(generator_.CTX_IDX)))) { //PL的执行环境
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_extend(), int_value))) {
        LOG_WARN("failed to generate a pointer", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator_.get_helper().get_int64(filed_cnt, int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_n(), int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_i(), int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generator_.get_helper().get_int64(package_id, int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) {
        LOG_WARN("push_back error", K(ret));
      } else { /*do nothing*/ }

      if (OB_SUCC(ret)) {
        ObLLVMValue ret_err;
        if (OB_FAIL(generator_.get_helper().create_call(ObString("spi_extend_collection"), generator_.get_spi_service().spi_extend_collection_, args, ret_err))) {
          LOG_WARN("failed to create call", K(ret));
        } else if (OB_FAIL(generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
          LOG_WARN("failed to check success", K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret) && package_id != OB_INVALID_ID && var_idx != OB_INVALID_ID) {
        OZ (generator_.generate_update_package_changed_info(s, package_id, var_idx));
      }

      OZ (generator_.generate_spi_pl_profiler_after_record(s));
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLTrimStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    COLLECTION_STMT_COMM(s.get_trim_expr());

    if (OB_SUCC(ret)) {
      ObLLVMValue int_value;
      ObSEArray<ObLLVMValue, 4> args;
      ObLLVMValue ret_err;
      OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
      OZ (generator_.get_helper().get_int64(s.get_trim(), int_value));
      OZ (args.push_back(int_value));
      OZ (generator_.get_helper().get_int64(row_size, int_value));
      OZ (args.push_back(int_value));
      OZ (generator_.get_helper().get_int64(s.get_n(), int_value));
      OZ (args.push_back(int_value));

      OZ (generator_.get_helper().create_call(ObString("spi_trim_collection"),
       generator_.get_spi_service().spi_trim_collection_, args, ret_err));
      OZ (generator_.check_success(ret_err, s.get_stmt_id(),
       s.get_block()->in_notfound(), s.get_block()->in_warning()));
    }
    if (OB_SUCC(ret) && package_id != OB_INVALID_ID && var_idx != OB_INVALID_ID) {
      OZ (generator_.generate_update_package_changed_info(s, package_id, var_idx));
    }

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDeleteStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    COLLECTION_STMT_COMM(s.get_delete_expr());

    if (OB_SUCC(ret)) {
      ObLLVMValue int_value;
      ObSEArray<ObLLVMValue, 5> args;
      ObLLVMValue ret_err;
      OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
      OZ (generator_.get_helper().get_int64(s.get_delete(), int_value));
      OZ (args.push_back(int_value));
      OZ (generator_.get_helper().get_int64(row_size, int_value));
      OZ (args.push_back(int_value));
      OZ (generator_.get_helper().get_int64(s.get_m(), int_value));
      OZ (args.push_back(int_value));
      OZ (generator_.get_helper().get_int64(s.get_n(), int_value));
      OZ (args.push_back(int_value));

      OZ (generator_.get_helper().create_call(ObString("spi_delete_collection"),
       generator_.get_spi_service().spi_delete_collection_, args, ret_err));
      OZ (generator_.check_success(ret_err, s.get_stmt_id(),
       s.get_block()->in_notfound(), s.get_block()->in_warning()));
    }
    if (OB_SUCC(ret) && package_id != OB_INVALID_ID && var_idx != OB_INVALID_ID) {
      OZ (generator_.generate_update_package_changed_info(s, package_id, var_idx));
    }

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

#undef COLLECTION_STMT_COMM

int ObPLCodeGenerateVisitor::visit(const ObPLDeclareCondStmt &s)
{
  int ret = OB_SUCCESS;
  UNUSED(s);
  //do nothing
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDeclareHandlerStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    ObLLVMBasicBlock exception;
    ObLLVMBasicBlock body;
    ObLLVMLandingPad catch_result;
    ObLLVMType landingpad_type;
    ObLLVMValue int_value;

    if (OB_FAIL(generator_.get_helper().create_block(ObString("exception"), generator_.get_func(), exception))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("body"), generator_.get_func(), body))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_br(body))) {
      LOG_WARN("failed to create_br", K(ret));
    } else if (OB_FAIL(generator_.get_helper().set_insert_point(exception))) {
      LOG_WARN("failed to set_insert_point", K(ret));
    } else if (OB_FAIL(generator_.get_adt_service().get_landingpad_result(landingpad_type))) {
      LOG_WARN("failed to get_landingpad_result", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_landingpad(ObString("landingPad"), landingpad_type, catch_result))) {
      LOG_WARN("failed to create_landingpad", K(ret));
    } else if (OB_FAIL(catch_result.set_cleanup())) {
      LOG_WARN("failed to set_cleanup", K(ret));
#ifndef NDEBUG
    } else if (OB_FAIL(generator_.get_helper().get_int64(3333, int_value))) {
      LOG_WARN("failed to get_int64", K(ret));
    } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), int_value))) {
      LOG_WARN("failed to create_call", K(ret));
#endif
    } else {
      common::ObArray<ObLLVMGlobalVariable> condition_clauses;
      common::ObArray<std::pair<ObPLConditionType, int64_t>> precedences;

      ObSEArray<ObLLVMValue, 8> condition_elements;
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_handlers().count(); ++i) {
        if (OB_ISNULL(s.get_handler(i).get_desc())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("handler is NULL", K(i), K(s.get_handler(i)), K(ret));
        } else {
          ObSqlString const_name;
          for (int64_t j = 0; OB_SUCC(ret) && j < s.get_handler(i).get_desc()->get_conditions().count(); ++j) {
            const_name.reset();
            condition_elements.reset();
            if (OB_FAIL(generator_.get_helper().get_int64(s.get_handler(i).get_desc()->get_condition(j).type_, int_value))) {
              LOG_WARN("failed to get_int64", K(ret));
            } else if (OB_FAIL(condition_elements.push_back(int_value))) {
              LOG_WARN("push_back error", K(ret));
            } else if (s.get_handler(i).get_desc()->get_condition(j).type_ >= MAX_TYPE) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type value error", K(ret));
            } else if (OB_FAIL(const_name.append(ConditionType[s.get_handler(i).get_desc()->get_condition(j).type_]))) {
              LOG_WARN("append error", K(ret));
            } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_handler(i).get_desc()->get_condition(j).error_code_, int_value))) {
              LOG_WARN("failed to get_int64", K(ret));
            } else if (OB_FAIL(condition_elements.push_back(int_value))) {
              LOG_WARN("push_back error", K(ret));
            } else if (OB_FAIL(const_name.append_fmt("%ld%ld", s.get_handler(i).get_level(), s.get_handler(i).get_desc()->get_condition(j).error_code_))) {
              LOG_WARN("append_fmt error", K(ret));
            } else {
              ObLLVMValue elem_value;
              if (NULL == s.get_handler(i).get_desc()->get_condition(j).sql_state_ || 0 == s.get_handler(i).get_desc()->get_condition(j).str_len_) {
                ObLLVMValue str;
                ObLLVMValue len;
                if (OB_FAIL(generator_.generate_empty_string(str, len))) {
                  LOG_WARN("failed to generate_empty_string", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(str))) {
                  LOG_WARN("push_back error", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(len))) {
                  LOG_WARN("push_back error", K(ret));
                } else { /*do nothing*/ }
              } else {
                if (OB_FAIL(generator_.get_helper().create_global_string(ObString(s.get_handler(i).get_desc()->get_condition(j).str_len_, s.get_handler(i).get_desc()->get_condition(j).sql_state_), elem_value))) {
                  LOG_WARN("failed to create_global_string", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(elem_value))) {
                  LOG_WARN("push_back error", K(ret));
                } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_handler(i).get_desc()->get_condition(j).str_len_, int_value))) {
                  LOG_WARN("failed to get int64", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(int_value))) {
                  LOG_WARN("push_back error", K(ret));
                } else if (OB_FAIL(const_name.append(s.get_handler(i).get_desc()->get_condition(j).sql_state_, s.get_handler(i).get_desc()->get_condition(j).str_len_))) {
                  LOG_WARN("append error", K(ret));
                } else { /*do nothing*/ }
              }

              if (OB_SUCC(ret)) {
                ObLLVMType llvm_type;
                ObLLVMConstant const_value;
                ObLLVMGlobalVariable const_condition;
                ObLLVMType condition_value_type;
                if (OB_FAIL(generator_.get_helper().get_int64(s.get_handler(i).get_level(), elem_value))) { //level(stmt_id)
                  LOG_WARN("failed to get int64", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(elem_value))) {
                  LOG_WARN("push_back error", K(ret));
                } else if (OB_FAIL(generator_.generate_null(ObTinyIntType, elem_value))) { //signal
                  LOG_WARN("failed to get_llvm_type", K(ret));
                } else if (OB_FAIL(condition_elements.push_back(elem_value))) {
                  LOG_WARN("push_back error", K(ret));
                } else if (OB_FAIL(generator_.get_adt_service().get_pl_condition_value(condition_value_type))) {
                  LOG_WARN("failed to get_pl_condition_value", K(ret));
                } else if (OB_FAIL(generator_.get_helper().get_or_insert_global(ObString(const_name.length(), const_name.ptr()), condition_value_type, const_condition))) {
                  LOG_WARN("failed to get_or_insert_global", K(ret));
                } else if (OB_FAIL(ObLLVMHelper::get_const_struct(condition_value_type, condition_elements, const_value))) {
                  LOG_WARN("failed to get_const_struct", K(ret));
                } else if (OB_FAIL(const_condition.set_initializer(const_value))) {
                  LOG_WARN("failed to set_initializer", K(ret));
                } else if (OB_FAIL(const_condition.set_constant())) {
                  LOG_WARN("failed to set_constant", K(ret));
                } else if (OB_FAIL(condition_clauses.push_back(const_condition))) {
                  LOG_WARN("failed to add_clause", K(ret));
                } else if (OB_FAIL(precedences.push_back(std::make_pair(s.get_handler(i).get_desc()->get_condition(j).type_, s.get_handler(i).get_level())))) {
                  LOG_WARN("push back error", K(ret));
                } else { /*do nothing*/ }
              }
            }
          }
        }
      }

      common::ObArray<int64_t> position_map;
      int64_t pos = OB_INVALID_INDEX;
      for (int64_t i = 0; OB_SUCC(ret) && i < precedences.count(); ++i) {
        if (OB_FAIL(find_next_procedence_condition(precedences, position_map, pos))) {
          LOG_WARN("failed to find next condition", K(ret));
        } else if (OB_INVALID_INDEX == pos) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pos is invalid", K(ret), K(pos));
        } else if (OB_FAIL(catch_result.add_clause(condition_clauses.at(pos)))) {
          LOG_WARN("failed to add clause", K(ret));
        } else if (OB_FAIL(position_map.push_back(pos))) {
          LOG_WARN("push back error", K(ret));
        }
      }


      if (OB_SUCC(ret)) {
        common::ObArray<int64_t> tmp_array;
        for (int64_t i = 0; OB_SUCC(ret) && i < position_map.count(); ++i) {
          bool find = false;
          for (int64_t j = 0; OB_SUCC(ret) && !find && j < position_map.count(); ++j) {
            if (i == position_map.at(j)) {
              if (OB_FAIL(tmp_array.push_back(j))) {
                LOG_WARN("push back error", K(ret));
              } else {
                find = true;
              }
            }
          }
        }
        if (FAILEDx(position_map.assign(tmp_array))) {
          LOG_WARN("assign error", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObLLVMValue unwindException;
        ObLLVMValue retTypeInfoIndex;
        ObLLVMValue unwindException_header;
        ObLLVMType condition_type;
        ObLLVMType condition_pointer_type;
        ObLLVMValue condition;

        if (OB_FAIL(generator_.get_helper().create_extract_value(ObString("extract_unwind_exception"), catch_result, 0, unwindException))) {
          LOG_WARN("failed to create_extract_value", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_extract_value(ObString("extract_ret_type_info_index"), catch_result, 1, retTypeInfoIndex))) {
          LOG_WARN("failed to create_extract_value", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_const_gep1_64(ObString("extract_unwind_exception_header"), unwindException, generator_.get_eh_service().pl_exception_base_offset_, unwindException_header))) {
          LOG_WARN("failed to create_const_gep1_64", K(ret));
        } else if (OB_FAIL(generator_.get_adt_service().get_pl_condition_value(condition_type))) {
          LOG_WARN("failed to get_pl_condition_value", K(ret));
        } else if (OB_FAIL(condition_type.get_pointer_to(condition_pointer_type))) {
          LOG_WARN("failed to get_pointer_to", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_pointer_cast(ObString("cast_header"), unwindException_header, condition_pointer_type, condition))) {
          LOG_WARN("failed to create_pointer_cast", K(ret));
        } else {
          ObLLVMValue type;
          ObLLVMValue code;
          ObLLVMValue name;
          ObLLVMValue stmt_id;
          ObLLVMValue signal;
          if (OB_FAIL(generator_.extract_type_from_condition_value(condition, type))) {
            LOG_WARN("failed to extract_type_from_condition_value", K(ret));
          } else if (OB_FAIL(generator_.extract_code_from_condition_value(condition, code))) {
            LOG_WARN("failed to extract_code_from_condition_value", K(ret));
          } else if (OB_FAIL(generator_.extract_name_from_condition_value(condition, name))) {
            LOG_WARN("failed to extract_name_from_condition_value", K(ret));
          } else if (OB_FAIL(generator_.extract_stmt_from_condition_value(condition, stmt_id))) {
            LOG_WARN("failed to extract_stmt_from_condition_value", K(ret));
          } else if (OB_FAIL(generator_.extract_signal_from_condition_value(condition, signal))) {
            LOG_WARN("failed to extract_signal_from_condition_value", K(ret));
#ifndef NDEBUG
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), unwindException))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), retTypeInfoIndex))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), type))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), code))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), name))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), stmt_id))) {
            LOG_WARN("failed to create_call", K(ret));
#endif
          } else { /*do nothing*/ }

          if (OB_SUCC(ret)) {
            //定义resume出口和exit出口
            ObLLVMBasicBlock resume_handler;
            ObLLVMBasicBlock exit_handler;

            if (OB_FAIL(generator_.get_helper().create_block(ObString("resume_handler"), generator_.get_func(), resume_handler))) {
              LOG_WARN("failed to create block", K(ret));
            } else if (OB_FAIL(generator_.get_helper().create_block(ObString("exit_handler"), generator_.get_func(), exit_handler))) {
              LOG_WARN("failed to create block", K(ret));
            } else { /*do nothing*/ }

            if (OB_SUCC(ret)) {
              ObLLVMSwitch switch_inst;
              if (OB_FAIL(generator_.get_helper().create_switch(retTypeInfoIndex, resume_handler, switch_inst))) {
                LOG_WARN("failed to create switch", K(ret));
              } else {
                int64_t index = 0;
                for (int64_t i = 0; OB_SUCC(ret) && i < s.get_handlers().count(); ++i) {
                  ObLLVMBasicBlock case_branch;
                  if (OB_FAIL(generator_.get_helper().create_block(ObString("case"), generator_.get_func(), case_branch))) {
                    LOG_WARN("failed to create block", K(ret));
                  } else {
                    for (int64_t j = 0; OB_SUCC(ret) && j < s.get_handler(i).get_desc()->get_conditions().count(); ++j) {
                      if (OB_FAIL(generator_.get_helper().get_int32(position_map.at(index++) + 1, int_value))) {
                        LOG_WARN("failed to get int32", K(ret));
                      } else if (OB_FAIL(switch_inst.add_case(int_value, case_branch))) {
                        LOG_WARN("failed to add_case", K(ret));
                      } else { /*do nothing*/ }
                    }

                    /*
                     * 如果捕捉到了该exception，处理完body后，视handler的ACTION和level决定下一个目的地：
                     * EXIT：跳出当前block
                     * CONTINUE：跳到抛出exception的语句的下一句
                     * 我们对Handler做了改写，所以这里的逻辑是：
                     * 原生EXIT：跳出当前block
                     * 下降的CONTINUE：跳出当前block
                     * 下降的EXIT：继续向上抛
                     */
                    if (OB_SUCC(ret)) {
                      if (NULL == s.get_handler(i).get_desc()->get_body() || (s.get_handler(i).get_desc()->is_exit() && !s.get_handler(i).is_original())) { //下降的EXIT，不执行body
                        if (OB_FAIL(generator_.get_helper().set_insert_point(case_branch))) {
                          LOG_WARN("failed to set_insert_point", K(ret));
                        } else if (OB_FAIL(generator_.get_helper().create_br(s.get_handler(i).get_desc()->is_exit() && !s.get_handler(i).is_original() ? resume_handler : exit_handler))) {
                          LOG_WARN("failed to create_br", K(ret));
                        } else { /*do nothing*/ }
                      } else {
                        ObLLVMValue p_status, status;
                        ObLLVMBasicBlock current = generator_.get_current();
                        ObLLVMValue old_exception, old_ob_error;
                        ObSEArray<ObLLVMValue, 2> args;
                        ObLLVMValue result;
                        ObLLVMValue p_old_sqlcode, is_need_pop_warning_buf;
                        ObLLVMType int_type;
                        OZ (generator_.set_current(case_branch));
#ifndef NDEBUG
                        OZ (generator_.get_helper().get_int64(1111+i, int_value));
                        OZ (generator_.generate_debug(ObString("debug"), int_value));
#endif
                        OZ (generator_.get_helper().get_int32(OB_SUCCESS, int_value));
                        OZ (generator_.get_helper().create_store(int_value, generator_.get_vars().at(generator_.RET_IDX)));
                        OZ (generator_.extract_status_from_context(generator_.get_vars().at(generator_.CTX_IDX), p_status));
                        OZ (generator_.get_helper().create_load(ObString("load status"), p_status, status));
                        OZ (generator_.get_helper().create_store(int_value, p_status));

                        // 记录下当前捕获到的ObError, 用于设置再次抛出该异常时设置status
                        OX (old_ob_error = generator_.get_saved_ob_error());
                        OX (generator_.get_saved_ob_error() = status);

                        OX (args.reset());
                        OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
                        OZ (generator_.get_helper().create_alloca(ObString("old_sqlcode"), int_type, p_old_sqlcode));
                        OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                        OZ (args.push_back(p_old_sqlcode));
                        OZ (generator_.get_helper().create_call(ObString("spi_get_pl_exception_code"), generator_.get_spi_service().spi_get_pl_exception_code_, args, result));
                        OZ (generator_.check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));

                        // 记录下当前捕获到的Exception, 用于SIGNAL语句抛出当前异常
                        OX (old_exception = generator_.get_saved_exception());
                        OX (generator_.get_saved_exception() = unwindException);

                        // 设置当前ExceptionCode到SQLCODE
                        OX (args.reset());
                        OZ (generator_.get_helper().get_int8(false, is_need_pop_warning_buf));
                        OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                        OZ (args.push_back(code));
                        OZ (args.push_back(is_need_pop_warning_buf));
                        OZ (generator_.get_helper().create_call(ObString("spi_set_pl_exception_code"), generator_.get_spi_service().spi_set_pl_exception_code_, args, result));
                        OZ (generator_.check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));

/*
                        // Check当前是否是嵌套事务的ExceptionHandler, 失效掉嵌套事务内部的Exception Handler
                        OX (args.reset());
                        OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                        OZ (args.push_back(code));
                        OZ (generator_.get_helper().create_call(
                              ObString("spi_check_exception_handler_legal"),
                              generator_.get_spi_service().spi_check_exception_handler_legal_,
                              args,
                              result));
                        OZ (generator_.check_success(
                              result,
                              s.get_stmt_id(),
                              s.get_block()->in_notfound(),
                              s.get_block()->in_warning()));
*/

                        // Codegen当前Handler的Body
                        OZ (SMART_CALL(generate(*s.get_handler(i).get_desc()->get_body())));

                        // 恢复原来的Exception
                        OX (generator_.get_saved_exception() = old_exception);
                        OX (generator_.get_saved_ob_error() = old_ob_error);

                        // 恢复原来的SQLCODE
                        if (OB_SUCC(ret)
                            && OB_NOT_NULL(generator_.get_current().get_v())) {
                          ObLLVMValue old_code;
                          if (OB_ISNULL(old_exception.get_v())) {
                            OZ (generator_.get_helper().create_load(ObString("load_old_sqlcode"), p_old_sqlcode, old_code));
                            OZ (generator_.generate_debug(ObString("debug"), int_value));
                            //OZ (generator_.get_helper().get_int64(OB_SUCCESS, old_code));
                          } else {
                            ObLLVMValue old_unwindException_header;
                            ObLLVMType old_condition_type;
                            ObLLVMType old_condition_pointer_type;
                            ObLLVMValue old_condition;
                            OZ (generator_.get_helper().create_const_gep1_64(ObString("extract_unwind_exception_header"), unwindException, generator_.get_eh_service().pl_exception_base_offset_, old_unwindException_header));
                            OZ (generator_.get_adt_service().get_pl_condition_value(old_condition_type));
                            OZ (condition_type.get_pointer_to(old_condition_pointer_type));
                            OZ (generator_.get_helper().create_pointer_cast(ObString("cast_header"), old_unwindException_header, old_condition_pointer_type, old_condition));
                            OZ (generator_.extract_code_from_condition_value(old_condition, old_code));
                          }
                          OX (args.reset());
                          OZ (generator_.get_helper().get_int8(true, is_need_pop_warning_buf));
                          OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
                          OZ (args.push_back(old_code));
                          OZ (args.push_back(is_need_pop_warning_buf));
                          OZ (generator_.get_helper().create_call(ObString("spi_set_pl_exception_code"), generator_.get_spi_service().spi_set_pl_exception_code_, args, result));
                          OZ (generator_.check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
                        }
                        OZ (generator_.finish_current(s.get_handler(i).get_desc()->is_exit() && !s.get_handler(i).is_original() ? resume_handler : exit_handler)); //下降的EXIT才抛出
                        OZ (generator_.set_current(current));
                      }
                    }
                  }
                }

                if (OB_SUCC(ret)) {
                  ObLLVMType unwind_exception_type;
                  ObLLVMType unwind_exception_pointer_type;
                  if (OB_FAIL(generator_.set_exception(exception,
                                                       exit_handler,
                                                       s.get_level()))) { //这里才可以压栈exception
                    LOG_WARN("failed to set_exception", K(ret));
                  } else if (OB_FAIL(generator_.get_helper().set_insert_point(resume_handler))) {
                    LOG_WARN("failed to set_insert_point", K(ret));
                  } else if (NULL != generator_.get_parent_exception()) {
#ifndef NDEBUG
                    if (OB_FAIL(generator_.get_helper().get_int64(2222, int_value))) {
                      LOG_WARN("failed to get int64", K(ret));
                    } else if (OB_FAIL(generator_.generate_debug(ObString("debug"), int_value))) {
                      LOG_WARN("failed to create_call", K(ret));
                    } else
#endif
                    if (OB_FAIL(generator_.get_adt_service().get_unwind_exception(unwind_exception_type))) {
                      LOG_WARN("failed to get_unwind_exception", K(ret));
                    } else if (OB_FAIL(unwind_exception_type.get_pointer_to(unwind_exception_pointer_type))) {
                      LOG_WARN("failed to get_pointer_to", K(ret));
                    } else if (OB_FAIL(generator_.get_helper().create_pointer_cast(ObString("cast_unwind_exception"), unwindException, unwind_exception_pointer_type, unwindException))) {
                      LOG_WARN("failed to create_pointer_cast", K(ret));
                    } else  if (OB_FAIL(generator_.get_helper().create_invoke(ObString("call_resume"), generator_.get_eh_service().eh_resume_, unwindException, body, generator_.get_parent_exception()->exception_))) {
                      LOG_WARN("failed to create block", K(ret));
                    } else { /*do nothing*/ }
                  } else {
                    if (OB_FAIL(generator_.get_helper().create_resume(catch_result))) {
                      LOG_WARN("failed to create_resume", K(ret));
                    }
                  }
                }

                if (OB_SUCC(ret)) {
                  if (OB_FAIL(generator_.set_current(body))) {
                    LOG_WARN("failed to set_current", K(ret));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLSignalStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else {
    OZ (generator_.get_helper().set_insert_point(generator_.get_current()));
    OZ (generator_.set_debug_location(s));
    OZ (generator_.generate_goto_label(s));

    ObLLVMBasicBlock normal;
    ObLLVMType unwind_exception_type, unwind_exception_pointer_type;
    ObLLVMType condition_type;
    ObLLVMType condition_pointer_type;
    ObLLVMValue unwindException = generator_.get_saved_exception();
    ObLLVMValue ob_error_code = generator_.get_saved_ob_error();
    ObLLVMValue unwind_exception_header;
    ObLLVMValue condition;
    ObLLVMValue sql_state;
    ObLLVMValue error_code;
    OZ (generator_.get_adt_service().get_unwind_exception(unwind_exception_type));
    OZ (unwind_exception_type.get_pointer_to(unwind_exception_pointer_type));
    OZ (generator_.get_adt_service().get_pl_condition_value(condition_type));
    OZ (condition_type.get_pointer_to(condition_pointer_type));
    if (OB_FAIL(ret)) {
    } else if (s.is_signal_null()) {
      ObLLVMValue status;
      CK (OB_NOT_NULL(generator_.get_saved_exception().get_v()));
      CK (OB_NOT_NULL(generator_.get_saved_ob_error().get_v()));
      OZ (generator_.extract_status_from_context(generator_.get_vars().at(generator_.CTX_IDX), status));
      OZ (generator_.get_helper().create_store(ob_error_code, status));
      OZ (generator_.get_helper().create_const_gep1_64(ObString("extract_unwind_exception_header"),
                                                       unwindException,
                                                       generator_.get_eh_service().pl_exception_base_offset_,
                                                       unwind_exception_header));
      OZ (generator_.get_helper().create_pointer_cast(ObString("cast_header"),
                                                      unwind_exception_header,
                                                      condition_pointer_type,
                                                      condition));
      OZ (generator_.extract_name_from_condition_value(condition, sql_state));
      OZ (generator_.extract_code_from_condition_value(condition, error_code));
      OZ (generator_.get_helper().create_pointer_cast(
          ObString("cast_unwind_exception"), unwindException,
          unwind_exception_pointer_type, unwindException));
      OZ (generator_.get_helper().create_block(ObString("normal"), generator_.get_func(), normal));
      OZ (generator_.raise_exception(unwindException, error_code, sql_state, normal,
                                     s.get_block()->in_notfound(), s.get_block()->in_warning(), true));
      OZ (generator_.set_current(normal));
    } else {
      ObLLVMValue type, ob_err_code, err_code, sql_state, str_len, is_signal, stmt_id, loc, err_code_ptr;
      if (lib::is_mysql_mode() && (s.is_resignal_stmt() || s.get_cond_type() != ERROR_CODE)) {
        ObLLVMValue int_value;
        ObLLVMType int_type, int32_type, int32_type_ptr;
        ObSEArray<ObLLVMValue, 5> args;

        int64_t *err_idx = const_cast<int64_t *>(s.get_expr_idx(
                              static_cast<int64_t>(SignalCondInfoItem::DIAG_MYSQL_ERRNO)));
        int64_t *msg_idx = const_cast<int64_t *>(s.get_expr_idx(
                              static_cast<int64_t>(SignalCondInfoItem::DIAG_MESSAGE_TEXT)));
        OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
        OZ (generator_.get_helper().get_llvm_type(ObInt32Type, int32_type));
        OZ (int32_type.get_pointer_to(int32_type_ptr));
        OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
        OZ (generator_.get_helper().get_int64(nullptr != err_idx ? *err_idx : OB_INVALID_ID, int_value));
        OZ (args.push_back(int_value));
        OZ (generator_.get_helper().get_int64(nullptr != msg_idx ? *msg_idx : OB_INVALID_ID, int_value));
        OZ (args.push_back(int_value));
        OZ (generator_.generate_string(ObString(s.get_str_len(), s.get_sql_state()), sql_state, str_len));
        OZ (args.push_back(sql_state));
        OZ (generator_.get_helper().create_alloca(ObString("error_code"), int_type, err_code_ptr));
        if (s.is_resignal_stmt()) {
          // ObLLVMValue code_ptr;
          CK (OB_NOT_NULL(generator_.get_saved_exception().get_v()));
          CK (OB_NOT_NULL(generator_.get_saved_ob_error().get_v()));
          OZ (generator_.get_helper().create_const_gep1_64(ObString("extract_unwind_exception_header"),
                                                           unwindException,
                                                           generator_.get_eh_service().pl_exception_base_offset_,
                                                           unwind_exception_header));
          OZ (generator_.get_helper().create_pointer_cast(ObString("cast_header"),
                                                          unwind_exception_header,
                                                          condition_pointer_type,
                                                          condition));
          OZ (generator_.extract_name_from_condition_value(condition, sql_state));
          OZ (generator_.extract_code_from_condition_value(condition, error_code));
          OZ (generator_.get_helper().create_store(error_code, err_code_ptr));
        } else {
          OZ (generator_.get_helper().get_int64(OB_SUCCESS, err_code));
          OZ (generator_.get_helper().create_store(err_code, err_code_ptr));
        }
        OZ (args.push_back(err_code_ptr));
        OZ (args.push_back(sql_state));
        OZ (generator_.get_helper().get_int8(!s.is_resignal_stmt(), is_signal));
        OZ (args.push_back(is_signal));
        OZ (generator_.get_helper().create_call(ObString("spi_process_resignal"),
                                                generator_.get_spi_service().spi_process_resignal_error_,
                                                args,
                                                ob_err_code));
        OZ (generator_.check_success(ob_err_code, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
        OZ (generator_.get_helper().create_load(ObString("load_error_code"), err_code_ptr, err_code));
        OZ (generator_.get_helper().create_bit_cast(ObString("cast_int64_to_int32"), err_code_ptr, int32_type_ptr, err_code_ptr));
        OZ (generator_.get_helper().create_load(ObString("load_error_code"), err_code_ptr, ob_err_code));
      } else {
        OZ (generator_.get_helper().get_int32(s.get_ob_error_code(), ob_err_code));
        OZ (generator_.get_helper().get_int64(s.get_error_code(), err_code));
      }
      OZ (generator_.get_helper().get_int64(s.get_cond_type(), type));
      OZ (generator_.get_helper().create_block(ObString("normal"), generator_.get_func(), normal));
      OZ (generator_.generate_string(ObString(s.get_str_len(), s.get_sql_state()), sql_state, str_len));
      OZ (generator_.get_helper().get_int64(s.get_stmt_id(), stmt_id));
      // 暂时先用stmtid， 这个id就是col和line的组合
      OZ (generator_.get_helper().get_int64(s.get_stmt_id(), loc));
      OZ (generator_.generate_exception(type, ob_err_code, err_code, sql_state, str_len, stmt_id,
                                        normal, loc, s.get_block()->in_notfound(),
                                        s.get_block()->in_warning(), true/*is signal*/));
      OZ (generator_.set_current(normal));
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLRaiseAppErrorStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    // 控制流已断, 后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    ObLLVMValue int_value;
    ObSEArray<ObLLVMValue, 3> args;
    ObLLVMValue ret_err;
    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
    OZ (generator_.get_helper().get_int64(s.get_params().at(0), int_value));
    OZ (args.push_back(int_value));
    OZ (generator_.get_helper().get_int64(s.get_params().at(1), int_value));
    OZ (args.push_back(int_value));
    OZ (generator_.get_helper().create_call(ObString("spi_raise_application_error"), generator_.get_spi_service().spi_raise_application_error_, args, ret_err));
    OZ (generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLCallStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    ObLLVMValue params;
    ObLLVMType argv_type;
    ObLLVMType int_type;
    if (OB_FAIL(generator_.get_helper().get_llvm_type(ObIntType, int_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(generator_.get_adt_service().get_argv(s.get_params().count(), argv_type))) {
      LOG_WARN("failed to get argv type", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_alloca(ObString("inner_pl_argv"), argv_type, params))) {
      LOG_WARN("failed to create_alloca", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_params().count(); ++i) {
        // 传递入参
        ObLLVMValue p_result_obj;
        if (s.is_pure_out(i)) {
          const ObPLVar *var =
            OB_INVALID_INDEX == s.get_out_index(i) ? NULL : s.get_variable(s.get_out_index(i));
          const ObPLDataType *pl_type = OB_ISNULL(var) ? NULL : &(var->get_type());
          ObPLDataType final_type;
          if (lib::is_oracle_mode() && NULL != var && var->get_type().is_cursor_type()) {
            OZ (generator_.extract_objparam_from_context(generator_.get_vars().at(
                                                             generator_.CTX_IDX),
                                                         s.get_out_index(i),
                                                         p_result_obj));
          } else {
            OZ (generator_.generate_new_objparam(p_result_obj), K(i), KPC(var));
            OZ (generator_.add_out_params(p_result_obj));
          }
          if (OB_SUCC(ret)
              && OB_ISNULL(pl_type)
              && OB_NOT_NULL(s.get_param_expr(i))
              && s.get_param_expr(i)->is_obj_access_expr()) {
            const ObObjAccessRawExpr *obj_access
              = static_cast<const ObObjAccessRawExpr *>(s.get_param_expr(i));
            CK (OB_NOT_NULL(obj_access));
            OZ (obj_access->get_final_type(final_type));
            OX (pl_type = &final_type);
          }
          if (OB_SUCC(ret)
              && OB_NOT_NULL(pl_type)
              && pl_type->is_composite_type()
              && !pl_type->is_opaque_type()) { // 普通类型构造空的ObjParam即可,复杂类型需要构造对应的指针
            ObLLVMType ir_type;
            ObLLVMValue value;
            ObLLVMValue const_value;
            if (OB_FAIL(generator_.get_llvm_type(*pl_type, ir_type))) {
              LOG_WARN("failed to get llvm type", K(ret));
            } else if (OB_FAIL(
                generator_.get_helper().create_alloca(ObString("PureOut"), ir_type, value))) {
              LOG_WARN("failed to create alloca", K(ret));
            } else if (OB_FAIL(pl_type->generate_construct(generator_,
                                                           *s.get_namespace(),
                                                           value,
                                                           &s))) {
              LOG_WARN("failed to generate_construct", K(ret));
            } else if (pl_type->is_collection_type()) {
              const ObUserDefinedType *user_type = NULL;
              if (OB_ISNULL(s.get_namespace())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("ns is NULL", K(ret));
              } else if (OB_FAIL(s.get_namespace()
                  ->get_pl_data_type_by_id(pl_type->get_user_type_id(), user_type))) {
                LOG_WARN("failed to get pl data type by id", K(ret));
              } else if (OB_ISNULL(user_type)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("user type is NULL", K(pl_type->get_user_type_id()), K(ret));
              } else { /*do nothing*/ }
            }
            if (OB_SUCC(ret)) {
              int64_t init_size = OB_INVALID_SIZE;
              ObLLVMValue p_dest_value;
              ObLLVMType int_type;
              ObLLVMValue var_addr;
              ObLLVMValue var_type;
              ObLLVMValue init_value;
              ObLLVMValue extend_value;
              OZ (generator_.get_helper().get_int8(pl_type->get_type(), var_type));
              OZ (pl_type->get_size(PL_TYPE_INIT_SIZE, init_size));
              OZ (generator_.get_helper().get_int32(init_size, init_value));
              OZ (generator_.get_helper().get_llvm_type(ObIntType, int_type));
              OZ (generator_.get_helper().create_ptr_to_int(ObString("cast_ptr_to_int64"),
                                                            value,
                                                            int_type,
                                                            var_addr));
              OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, p_dest_value));
              OZ (generator_.generate_set_extend(p_dest_value, var_type, init_value, var_addr));
            }
          }
        } else {
          const ObPLVar *var = OB_INVALID_INDEX == s.get_out_index(i)
                                                  ? NULL : s.get_variable(s.get_out_index(i));
          if (lib::is_oracle_mode() && NULL != var && var->get_type().is_cursor_type()) {
            OZ (generator_.extract_objparam_from_context(generator_.get_vars().at(
                                                             generator_.CTX_IDX),
                                                         s.get_out_index(i),
                                                         p_result_obj));
          } else if (!s.get_param_expr(i)->is_obj_access_expr()
              || !(static_cast<const ObObjAccessRawExpr *>(s.get_param_expr(i)))->for_write()) {
            OZ (generator_.generate_expr(s.get_param(i), s, OB_INVALID_INDEX, p_result_obj));
          } else {
            ObLLVMValue address;
            ObPLDataType final_type;
            bool is_no_copy_param = s.get_nocopy_params().count() > 0 && OB_INVALID_INDEX != s.get_nocopy_params().at(i);
            const ObObjAccessRawExpr *obj_access
              = static_cast<const ObObjAccessRawExpr *>(s.get_param_expr(i));
            CK (OB_NOT_NULL(obj_access));
            OZ (obj_access->get_final_type(final_type));
            OZ (generator_.generate_expr(s.get_param(i),
                                         s,
                                         OB_INVALID_INDEX,
                                         (final_type.is_obj_type() || !is_no_copy_param) ? address : p_result_obj));
            if (OB_FAIL(ret)) {
            } else if (final_type.is_obj_type()) {
              ObLLVMType obj_type;
              ObLLVMType obj_type_ptr;
              ObLLVMValue p_obj;
              ObLLVMValue src_obj;
              ObLLVMValue p_dest_obj;
              OZ (generator_.generate_new_objparam(p_result_obj));
              OZ (generator_.add_out_params(p_result_obj));
              OZ (generator_.extract_extend_from_objparam(address, final_type, p_obj));
              OZ (generator_.get_adt_service().get_obj(obj_type));
              OZ (obj_type.get_pointer_to(obj_type_ptr));
              OZ (generator_.get_helper().create_bit_cast(
                ObString("cast_addr_to_obj_ptr"), p_obj, obj_type_ptr, p_obj));
              OZ (generator_.get_helper().create_load(ObString("load obj value"), p_obj, src_obj));
              OZ (generator_.extract_datum_ptr_from_objparam(p_result_obj, ObNullType, p_dest_obj));
              OZ (generator_.get_helper().create_store(src_obj, p_dest_obj));
            } else if (!is_no_copy_param) {
              ObLLVMValue allocator;
              ObLLVMValue src_datum;
              ObLLVMValue dest_datum;
              int64_t udt_id = s.get_param_expr(i)->get_result_type().get_udt_id();
              OZ (generator_.extract_allocator_from_context(
                generator_.get_vars().at(generator_.CTX_IDX), allocator));
              OZ (generator_.generate_new_objparam(p_result_obj, udt_id));
              OZ (generator_.add_out_params(p_result_obj));
              OZ (generator_.extract_obobj_ptr_from_objparam(p_result_obj, dest_datum));
              OZ (generator_.extract_obobj_ptr_from_objparam(address, src_datum));
              OZ (final_type.generate_copy(generator_,
                                            *s.get_namespace(),
                                            allocator,
                                            src_datum,
                                            dest_datum,
                                            s.get_block()->in_notfound(),
                                            s.get_block()->in_warning(),
                                            OB_INVALID_ID));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObLLVMValue p_arg;
          ObLLVMValue pp_arg;
          if (OB_FAIL(generator_.get_helper().create_ptr_to_int(ObString("cast_arg_to_pointer"), p_result_obj, int_type, p_arg))) {
            LOG_WARN("failed to create_PtrToInt", K(ret));
          } else if (OB_FAIL(generator_.extract_arg_from_argv(params, i, pp_arg))) {
            LOG_WARN("failed to extract_arg_from_argv", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_store(p_arg, pp_arg))) {
            LOG_WARN("failed to create_store", K(ret));
          } else { /*do nothing*/ }
        }
      }

      if (OB_SUCC(ret)) {
        ObSEArray<ObLLVMValue, 5> args;
        ObLLVMValue argv;
        ObLLVMValue int_value;
        ObLLVMValue array_value;
        ObLLVMValue nocopy_array_value;
        uint64_t package_id = s.get_package_id();
        package_id = (1 == s.get_is_object_udf())
                          ? share::schema::ObUDTObjectType::mask_object_id(package_id) : package_id;
        if (OB_FAIL(args.push_back(generator_.get_vars().at(generator_.CTX_IDX)))) { //PL的执行环境
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(generator_.get_helper().get_int64(package_id, int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { //PL的package id
          LOG_WARN("push_back error", K(ret));
        }else if (OB_FAIL(generator_.get_helper().get_int64(s.get_proc_id(), int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { //PL的proc id
          LOG_WARN("push_back error", K(ret));
        }else if (OB_FAIL(generator_.generate_int64_array(s.get_subprogram_path(), array_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(array_value))) { //PL的subprogram path
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_subprogram_path().count(),
                                                             int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { //subprogram path长度
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(generator_.get_helper().get_int64(static_cast<uint64_t>(s.get_stmt_id()), int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { // line number;
          LOG_WARN("push back line number error", K(ret));
        } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_params().count(), int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { //argc
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(generator_.get_helper().create_ptr_to_int(ObString("cast_argv_to_pointer"), params, int_type, argv))) {
          LOG_WARN("failed to create_PtrToInt", K(ret));
        } else if (OB_FAIL(args.push_back(argv))) {//argv
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(generator_.generate_int64_array(s.get_nocopy_params(),
                                                           nocopy_array_value))) {
          LOG_WARN("failed to get int64_t array", K(ret));
        } else if (OB_FAIL(args.push_back(nocopy_array_value))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(generator_.get_helper().get_int64(s.get_dblink_id(), int_value))) {
          LOG_WARN("failed to get int64", K(ret));
        } else if (OB_FAIL(args.push_back(int_value))) { //PL的dblink id
          LOG_WARN("push_back error", K(ret));
        } else {
          ObLLVMValue result;
          if (NULL == generator_.get_current_exception()) {
            if (OB_FAIL(generator_.get_helper().create_call(ObString("inner_pl_execute"), generator_.get_pl_execute(), args, result))) {
              LOG_WARN("failed to create_call", K(ret));
            }
            OZ (generator_.generate_debug(ObString("debug inner pl execute"), result));
          } else {
            ObLLVMBasicBlock alter_inner_call;

            if (OB_FAIL(generator_.get_helper().create_block(ObString("alter_inner_call"), generator_.get_func(), alter_inner_call))) {
              LOG_WARN("failed to create block", K(s), K(ret));
            } else if (OB_FAIL(generator_.get_helper().create_invoke(ObString("inner_pl_execute"), generator_.get_pl_execute(), args, alter_inner_call, generator_.get_current_exception()->exception_, result))) {
              LOG_WARN("failed to create_call", K(ret));
            } else if (OB_FAIL(generator_.set_current(alter_inner_call))) {
              LOG_WARN("failed to set_current", K(ret));
            } else { /*do nothing*/ }
          }
          OZ (generator_.generate_debug(ObString("debug inner pl execute result"), result));
          OZ (generator_.check_success(
            result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
          OZ (generator_.generate_out_params(s, s.get_params(), params));

          OZ (generator_.generate_spi_pl_profiler_after_record(s));
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDeclareCursorStmt &s)
{
  int ret = OB_SUCCESS;

  OZ (generator_.generate_spi_pl_profiler_before_record(s));
  OZ (generator_.generate_declare_cursor(s, s.get_cursor_index()));
  OZ (generator_.generate_spi_pl_profiler_after_record(s));

  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLOpenStmt &s)
{
  int ret = OB_SUCCESS;
  OZ (generator_.generate_goto_label(s));
  OZ (generator_.generate_spi_pl_profiler_before_record(s));
  CK (OB_NOT_NULL(s.get_cursor()));
  OZ (generator_.generate_open(static_cast<const ObPLStmt&>(s),
                               s.get_cursor()->get_value(),
                               s.get_cursor()->get_package_id(),
                               s.get_cursor()->get_routine_id(),
                               s.get_index()));
  OZ (generator_.generate_spi_pl_profiler_after_record(s));
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLOpenForStmt &s)
{
  int ret = OB_SUCCESS;
  OZ (generator_.generate_goto_label(s));
  OZ (generator_.generate_spi_pl_profiler_before_record(s));
  OZ (generator_.generate_open_for(s));
  OZ (generator_.generate_spi_pl_profiler_after_record(s));
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLFetchStmt &s)
{
  int ret = OB_SUCCESS;
  ObLLVMValue ret_err;
  OZ (generator_.generate_goto_label(s));
  OZ (generator_.generate_spi_pl_profiler_before_record(s));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(generator_.generate_fetch(static_cast<const ObPLStmt&>(s),
                                               static_cast<const ObPLInto&>(s),
                                               s.get_package_id(),
                                               s.get_routine_id(),
                                               s.get_index(),
                                               s.get_limit(),
                                               s.get_user_type(),
                                               ret_err))) {
    LOG_WARN("failed to generate fetch", K(ret));
  } else if (lib::is_mysql_mode()) { //Mysql模式直接检查抛出异常
    OZ (generator_.check_success(
      ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning(), true));
  } else { //Oracle模式如果是OB_READ_NOTHING错误，吞掉异常不抛出
    ObLLVMValue is_not_found;
    ObLLVMBasicBlock fetch_end;
    ObLLVMBasicBlock fetch_check_success;
    if (OB_FAIL(generator_.get_helper().create_block(ObString("fetch_end"), generator_.get_func(), fetch_end))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_block(ObString("fetch_check_success"), generator_.get_func(), fetch_check_success))) {
      LOG_WARN("failed to create block", K(s), K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(ret_err, OB_READ_NOTHING, is_not_found))) {
      LOG_WARN("failed to create_icmp_eq", K(ret));
    } else if (OB_FAIL(generator_.get_helper().create_cond_br(is_not_found, fetch_end, fetch_check_success))) {
      LOG_WARN("failed to create_cond_br", K(ret));
    } else { /*do nothing*/ }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(generator_.set_current(fetch_check_success))) {
        LOG_WARN("failed to set current", K(ret));
      } else if (OB_FAIL(generator_.check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
        LOG_WARN("failed to check success", K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_br(fetch_end))) {
        LOG_WARN("failed to create_br", K(ret));
      } else if (OB_FAIL(generator_.set_current(fetch_end))) {
        LOG_WARN("failed to set current", K(ret));
      } else { /*do nothing*/ }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generator_.generate_into_restore(s.get_into(), s.get_exprs(), s.get_symbol_table()))) {
      LOG_WARN("Failed to generate_into", K(ret));
    } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
      LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLCloseStmt &s)
{
  int ret = OB_SUCCESS;
  OZ (generator_.generate_goto_label(s));
  OZ (generator_.generate_spi_pl_profiler_before_record(s));

  OZ (generator_.generate_close(static_cast<const ObPLStmt&>(s),
                                s.get_package_id(),
                                s.get_routine_id(),
                                s.get_index()));

  OZ (generator_.generate_spi_pl_profiler_after_record(s));
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLNullStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
    LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
  } else {}

  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLPipeRowStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else {
    OZ (generator_.get_helper().set_insert_point(generator_.get_current()));
    OZ (generator_.set_debug_location(s));
    OZ (generator_.generate_goto_label(s));
    OZ (generator_.generate_spi_pl_profiler_before_record(s));

    ObLLVMBasicBlock pipe_row_block;
    ObLLVMBasicBlock init_result_block;
    ObLLVMBasicBlock pipe_result_block;
    OZ (generator_.get_helper().create_block(
      ObString("pipe_row_block"), generator_.get_func(), pipe_row_block));
    OZ (generator_.get_helper().create_block(
      ObString("init_result_block"), generator_.get_func(), init_result_block));
    OZ (generator_.get_helper().create_block(
      ObString("pipe_result_block"), generator_.get_func(), pipe_result_block));
    OZ (generator_.get_helper().create_br(pipe_row_block));
    OZ (generator_.set_current(pipe_row_block));

    ObLLVMValue p_result;
    ObLLVMValue p_type_value;
    ObLLVMValue type_value;
    ObLLVMValue is_invalid;
    ObLLVMType obj_param_type;
    ObLLVMType obj_param_type_pointer;
    // 检查result数组是否已经初始化
    OZ (generator_.extract_result_from_context(
      generator_.get_vars().at(generator_.CTX_IDX), p_result));
    OZ (generator_.get_adt_service().get_objparam(obj_param_type));
    OZ (obj_param_type.get_pointer_to(obj_param_type_pointer));
    OZ (generator_.get_helper().create_bit_cast(ObString("obobj_to_obobjparam"),
                                                p_result, obj_param_type_pointer, p_result));
    OZ (generator_.extract_type_ptr_from_objparam(p_result, p_type_value));
    OZ (generator_.get_helper().create_load(
      ObString("load_result_type"), p_type_value, type_value));
    OZ (generator_.get_helper().create_icmp_eq(type_value, ObMaxType, is_invalid));
    OZ (generator_.get_helper().create_cond_br(is_invalid, init_result_block, pipe_result_block));

    // 初始化result数组
    ObSEArray<ObLLVMValue, 3> args;
    ObLLVMValue var_idx, init_value, var_value, extend_value;
    ObLLVMValue ret_err;
    ObLLVMValue var_type, type_id;
    int64_t init_size = 0;
    OZ (generator_.set_current(init_result_block));
    CK (OB_NOT_NULL(s.get_namespace()));
    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
    OZ (generator_.get_helper().get_int8(s.get_type().get_type(), var_type));
    OZ (args.push_back(var_type));
    OZ (generator_.get_helper().get_int64(s.get_type().get_user_type_id(), type_id));
    OZ (args.push_back(type_id));
    OZ (generator_.get_helper().get_int64(OB_INVALID_INDEX, var_idx));
    OZ (args.push_back(var_idx));
    OZ (s.get_namespace()->get_size(PL_TYPE_INIT_SIZE, s.get_type(), init_size));
    OZ (generator_.get_helper().get_int32(init_size, init_value));
    OZ (args.push_back(init_value));
    OZ (generator_.generate_null_pointer(ObIntType, init_value));
    OZ (args.push_back(init_value));
    OZ (generator_.get_helper().create_call(ObString("spi_alloc_complex_var"),
                                            generator_.get_spi_service().spi_alloc_complex_var_,
                                            args,
                                            ret_err));
    OZ (generator_.check_success(
      ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));

    OZ (generator_.extract_result_from_context(
      generator_.get_vars().at(generator_.CTX_IDX), var_value));
    OZ (generator_.get_helper().create_bit_cast(
      ObString("obobj_to_obobjparam"), var_value, obj_param_type_pointer, var_value));
    OZ (generator_.extract_extend_from_objparam(var_value, s.get_type(), extend_value));
    OZ (s.get_type().generate_construct(
      generator_, *s.get_namespace(), extend_value, &s));
    OZ (generator_.get_helper().create_br(pipe_result_block));

    // 将当前行插入Result数组中
    ObLLVMValue p_result_obj;
    OZ (generator_.set_current(pipe_result_block));
    OZ (generator_.generate_expr(s.get_row(), s, OB_INVALID_INDEX, p_result_obj));
    OX (args.reset());
    OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX))); //PL的执行环境
    OZ (args.push_back(p_result_obj));
    OZ (generator_.get_helper().create_call(ObString("spi_pipe_row_to_result"),
                                            generator_.get_spi_service().spi_pipe_row_to_result_,
                                            args, ret_err));
    OZ (generator_.check_success(ret_err,
                                 s.get_stmt_id(),
                                 s.get_block()->in_notfound(),
                                 s.get_block()->in_warning()));

    OZ (generator_.generate_spi_pl_profiler_after_record(s));
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLGotoStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    ObLLVMBasicBlock entry_blk;
    if (!s.get_dst_label().empty()) {
      if (OB_FAIL(generator_.get_helper().create_block(ObString("goto_blk"),
                  generator_.get_func(), entry_blk))) {
        LOG_WARN("faile to create basic block.", K(ret));
      } else if (OB_FAIL(generator_.get_helper().create_br(entry_blk))) {
        LOG_WARN("failed to create br instr", K(ret));
      } else if (OB_FAIL(generator_.set_current(entry_blk))) {
        LOG_WARN("failed to set current block", K(ret));
      } else if (OB_FAIL(generator_.get_helper().set_insert_point(entry_blk))) {
        LOG_WARN("failed to set insert point", K(ret));
      } else {
        #define GEN_BR_WITH_COLSE_CURSOR(goto_dst) \
        do { \
          for (int64_t i = 0; OB_SUCC(ret) && i < s.get_cursor_stmt_count(); ++i) { \
            const ObPLCursorForLoopStmt *cs =  \
                  static_cast<const ObPLCursorForLoopStmt *>(s.get_cursor_stmt(i)); \
            if (OB_NOT_NULL(cs)) { \
              CK (OB_NOT_NULL(cs->get_cursor())); \
              OZ (generator_.generate_close(static_cast<const ObPLStmt&>(*cs), \
                                            cs->get_cursor()->get_package_id(), \
                                            cs->get_cursor()->get_routine_id(), \
                                            cs->get_index()));\
            } else {\
              ret = OB_ERR_UNEXPECTED;\
              LOG_WARN("null goto cursor for loop stmt", K(ret));\
            }\
          }\
          if (OB_SUCC(ret)) { \
            ObSEArray<ObLLVMValue, 1> args;    \
            ObLLVMValue result;                \
            if (OB_FAIL(args.push_back(generator_.get_vars().at(generator_.CTX_IDX)))) {    \
              LOG_WARN("fail to push back.", K(ret));               \
            } else if (OB_FAIL(generator_.get_helper().create_call(ObString("check_early_exit"), generator_.get_spi_service().spi_check_early_exit_, args, result))) { \
              LOG_WARN("fail to create call check_early_exit", K(ret));      \
            } else if (OB_FAIL(generator_.check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {   \
              LOG_WARN("fail to check success", K(ret));  \
            } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) { \
              LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s)); \
            } else if (OB_FAIL(generator_.get_helper().create_br(goto_dst))) { \
              LOG_WARN("failed to create br instr", K(ret)); \
            }\
          }\
        } while(0)

        //跳转目的地和跳转语句之间的所有循环的栈都需要重置
        #define RESTORE_LOOP_STACK(src_level, dst_level) \
        do { \
          for (int64_t i = generator_.get_loop_count() - 1; OB_SUCC(ret) && i >= 0; --i) { \
            ObPLCodeGenerator::LoopStack::LoopInfo &loop_info = generator_.get_loops()[i]; \
            LOG_DEBUG("loop info", K(i), K(loop_info.level_), K(src_level), K(dst_level)); \
            if (loop_info.level_ <= src_level && loop_info.level_ >= dst_level) { \
              if (OB_FAIL(generator_.get_helper().stack_restore(loop_info.loop_))) { \
                LOG_WARN("failed to stack_restore", K(ret), K(src_level), K(dst_level)); \
              } else { \
                LOG_DEBUG("success stack restore", \
                          K(i), K(loop_info.level_), K(src_level), K(dst_level)); \
              } \
            } \
          } \
        } while(0)

        hash::HashMapPair<ObPLCodeGenerator::goto_label_flag, std::pair<ObLLVMBasicBlock, ObLLVMBasicBlock>> pair;
        int tmp_ret = generator_.get_goto_label_map().get_refactored(
                                            s.get_dst_stmt()->get_stmt_id(), pair);
        if (OB_SUCCESS == tmp_ret) {
          RESTORE_LOOP_STACK(s.get_level(), s.get_dst_stmt()->get_level());
          if (ObPLCodeGenerator::goto_label_flag::GOTO_LABEL_CG == pair.first) {
            GEN_BR_WITH_COLSE_CURSOR(pair.second.second);
          } else {
            GEN_BR_WITH_COLSE_CURSOR(pair.second.first);
          }
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          ObLLVMBasicBlock dst_blk;
          ObLLVMBasicBlock stack_save_blk;
          if (OB_FAIL(generator_.get_helper().create_block(s.get_dst_label(),
                      generator_.get_func(), dst_blk))) {
            LOG_WARN("faile to create dst block", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_block(s.get_dst_label(),
                      generator_.get_func(), stack_save_blk))) {
            LOG_WARN("faile to create stack save block", K(ret));
          } else if (OB_FAIL(pair.init(ObPLCodeGenerator::goto_label_flag::GOTO_LABEL_EXIST,
                                      std::pair<ObLLVMBasicBlock, ObLLVMBasicBlock>(stack_save_blk, dst_blk)))) {
            LOG_WARN("failed to init pair", K(ret));
          } else if (OB_FAIL(generator_.get_goto_label_map().set_refactored(
                                          s.get_dst_stmt()->get_stmt_id(), pair))) {
            LOG_WARN("fill hash map failed", K(ret));
          } else {
            GEN_BR_WITH_COLSE_CURSOR(stack_save_blk);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create goto label", K(ret));
         }
      #undef RESTORE_LOOP_STACK
      }
      if (OB_SUCC(ret)) {
        ObLLVMBasicBlock new_blk;
        if (OB_FAIL(generator_.get_helper().create_block(ObString("after_goto"),
                         generator_.get_func(), new_blk))) {
          LOG_WARN("failed to create basic block", K(ret));
        } else if (OB_FAIL(generator_.set_current(new_blk))) {
          LOG_WARN("failed to set current block", K(ret));
        }
      }
    } else {
      ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
      LOG_WARN("goto label is empty", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLInterfaceStmt &s)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 2> args;
  ObLLVMValue entry;
  ObLLVMValue interface_name_length;
  ObLLVMValue ret_err;
  const ObString interface_name = s.get_entry();
  CK (!interface_name.empty());
  OZ (args.push_back(generator_.get_vars().at(generator_.CTX_IDX)));
  OZ (generator_.generate_string(interface_name, entry, interface_name_length));
  OZ (args.push_back(entry));
  OZ (generator_.get_helper().create_call(ObString("spi_interface_impl"),
      generator_.get_spi_service().spi_interface_impl_,
      args,
      ret_err));
  OZ (generator_.check_success(ret_err,
      s.get_stmt_id(),
      s.get_block()->in_notfound(),
      s.get_block()->in_warning()));
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLDoStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < s.get_value().count(); ++i) {
        const ObRawExpr *value_expr = s.get_value_expr(i);
        int64_t result_idx = OB_INVALID_INDEX;
        ObLLVMValue p_result_obj;
        CK(OB_NOT_NULL(value_expr));
        OZ(generator_.generate_expr(s.get_value_index(i), s, result_idx,p_result_obj));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::visit(const ObPLCaseStmt &s)
{
  int ret = OB_SUCCESS;
  if (NULL == generator_.get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(generator_.get_helper().set_insert_point(generator_.get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(generator_.set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generator_.generate_goto_label(s))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generator_.generate_spi_pl_profiler_before_record(s))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(s));
  } else {
    // generate case expr, if any
    if (s.get_case_expr() != OB_INVALID_ID) {
      int64_t case_expr_idx = s.get_case_expr();
      int64_t case_var_idx = s.get_case_var();
      ObLLVMValue p_result_obj;
      if (OB_FAIL(generator_.generate_expr(case_expr_idx, s, case_var_idx, p_result_obj))) {
        LOG_WARN("failed to generate calc_expr func", K(ret));
      }
    }

    // generate when clause
    ObLLVMBasicBlock continue_branch;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generator_.get_helper().create_block(
              ObString("continue"), generator_.get_func(), continue_branch))) {
        LOG_WARN("faild to create continue branch for case stmt", K(ret));
      } else {
        const ObPLCaseStmt::WhenClauses &when = s.get_when_clauses();
        for (int64_t i = 0; OB_SUCC(ret) && i < when.count(); ++i) {
          const ObPLCaseStmt::WhenClause &current_when = when.at(i);
          const sql::ObRawExpr *expr = s.get_expr(current_when.expr_);
          ObLLVMBasicBlock current_then;
          ObLLVMBasicBlock current_else;
          ObLLVMValue p_cond;
          ObLLVMValue cond;
          ObLLVMValue is_false;
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr to when expr", K(i), K(current_when));
          } else if (OB_FAIL(generator_.get_helper().create_block(
                         ObString("then"), generator_.get_func(), current_then))) {
            LOG_WARN("failed to create then branch for case stmt", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_block(
                         ObString("else"), generator_.get_func(), current_else))) {
            LOG_WARN("failed to create else branch for case stmt", K(ret));
          } else if (OB_FAIL(generator_.generate_expr(
                         current_when.expr_, s, OB_INVALID_INDEX, p_cond))) {
            LOG_WARN("failed to generate calc_expr func", K(ret));
          } else if (OB_FAIL(generator_.extract_value_from_objparam(
                         p_cond, expr->get_data_type(), cond))) {
            LOG_WARN("failed to extract_value_from_objparam", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_icmp_eq(
                         cond, FALSE, is_false))) {
            LOG_WARN("failed to create_icmp_eq", K(ret));
          } else if (OB_FAIL(generator_.get_helper().create_cond_br(
                         is_false, current_else, current_then))) {
            LOG_WARN("failed to create_cond_br", K(ret));
          } else if (OB_FAIL(generator_.set_current(current_then))) {
            LOG_WARN("failed to set current to current_then branch", K(ret));
          } else if (OB_FAIL(visit(*current_when.body_))) {
            LOG_WARN("failed to visit then clause for case stmt", K(ret));
          } else if (OB_FAIL(generator_.finish_current(continue_branch))) {
            LOG_WARN("failed to finish current", K(ret));
          } else if (OB_FAIL(generator_.set_current(current_else))) {
            LOG_WARN("failed to set current to current_else branch", K(ret));
          } else {
            // do nothing
          }
        }
      }
    }

    // generate else
    if (OB_SUCC(ret)) {
      const ObPLStmtBlock *else_clause = s.get_else_clause();
      if (OB_ISNULL(else_clause)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("else in CASE stmt is NULL in CG", K(s), K(ret));
      } else if (OB_FAIL(visit(*else_clause))) {
        LOG_WARN("failed to visit else clause for case stmt", K(ret));
      } else if (OB_FAIL(generator_.finish_current(continue_branch))) {
        LOG_WARN("failed to finish current", K(ret));
      } else if (OB_FAIL(generator_.set_current(continue_branch))) {
        LOG_WARN("failed to set current", K(ret));
      } else if (OB_FAIL(generator_.generate_spi_pl_profiler_after_record(s))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(s));
      } else {
        // do nohting
      }
    }
  }
  return ret;
}

int ObPLCodeGenerateVisitor::find_next_procedence_condition(common::ObIArray<std::pair<ObPLConditionType, int64_t>> &conditions,
                                                            common::ObIArray<int64_t> &position_map, int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  ObPLConditionType type = INVALID_TYPE;
  int64_t level = OB_INVALID_INDEX;
  for (int64_t i = 0; i < conditions.count(); ++i) {
    bool need_ignore = false;
    for (int64_t j = 0; j < position_map.count(); ++j) {
      if (i == position_map.at(j)) {
        need_ignore = true;
        break;
      }
    }
    if (need_ignore) {
      continue;
    } else if (INVALID_TYPE == type) {
      type = conditions.at(i).first;
      level = conditions.at(i).second;
      idx = i;
    } else {
      int compare = ObPLDeclareHandlerStmt::DeclareHandler::compare_condition(conditions.at(i).first, conditions.at(i).second, type, level);
      if (compare <= 0) {
        //do nothing, just continue
      } else {
        type = conditions.at(i).first;
        level = conditions.at(i).second;
        idx = i;
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_user_type(const ObUserDefinedType &type)
{
  int ret = OB_SUCCESS;
  ObLLVMStructType udt_ir_type;
  ObSEArray<ObLLVMType, 16> elem_type_array;
  bool is_cursor_type = false;
  switch (type.get_type()) {
  case PL_RECORD_TYPE: {
    const ObRecordType &record_type = static_cast<const ObRecordType&>(type);
    OZ (build_record_type(record_type, elem_type_array));
  }
    break;
#ifdef OB_BUILD_ORACLE_PL
  case PL_NESTED_TABLE_TYPE: {
    const ObNestedTableType &table_type = static_cast<const ObNestedTableType&>(type);
    OZ (build_nested_table_type(table_type, elem_type_array));
  }
    break;
  case PL_ASSOCIATIVE_ARRAY_TYPE: {
    const ObAssocArrayType &assoc_array_type = static_cast<const ObAssocArrayType&>(type);
    OZ (build_assoc_array_type(assoc_array_type, elem_type_array));
  }
    break;
  case PL_VARRAY_TYPE: {
    const ObVArrayType &varray_type = static_cast<const ObVArrayType&>(type);
    OZ (build_varray_type(varray_type, elem_type_array));
  }
    break;
  case PL_SUBTYPE: {
    const ObUserDefinedSubType &subtype = static_cast<const ObUserDefinedSubType&>(type);
    ObLLVMType base_type;
    CK (OB_NOT_NULL(subtype.get_base_type()));
    if (OB_FAIL(ret)) {
    } else if (!subtype.get_base_type()->is_cursor_type()) {
      OZ (build_subtype(subtype, elem_type_array));
    } else {
      is_cursor_type = true;
    }
  }
    break;
  case PL_OPAQUE_TYPE: {
    const ObOpaqueType &opaque_type = static_cast<const ObOpaqueType&>(type);
    OZ (build_opaque_type(opaque_type, elem_type_array));
  }
    break;
#endif
  case PL_CURSOR_TYPE:
  case PL_REF_CURSOR_TYPE: {
    is_cursor_type =  true;
  }
    break;
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user defined type is invalid", K(type.get_type()), K(ret));
  }
    break;
  }

  if (OB_SUCC(ret) && !is_cursor_type) {
    if (!type.is_subtype()) {
      OZ (helper_.create_struct_type(type.get_name(), elem_type_array, udt_ir_type), type);
      if (OB_SUCC(ret) && OB_ISNULL(user_type_map_.get(type.get_user_type_id()))) {
        OZ (user_type_map_.set_refactored(type.get_user_type_id(), udt_ir_type), type);
      }
    } else {
      CK (1 == elem_type_array.count());
      if (OB_SUCC(ret) && OB_ISNULL(user_type_map_.get(type.get_user_type_id()))) {
        OZ (user_type_map_.set_refactored(type.get_user_type_id(), elem_type_array.at(0)), type);
      }
    }
  }
  LOG_DEBUG("generator user type",
           K(ret), K(type.get_user_type_id()), K(ast_.get_id()), K(ast_.get_name()), K(lbt()));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLCodeGenerator::build_nested_table_type(const ObNestedTableType &table_type,
                                                  ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_collection_type(table_type, elem_type_array))) {
    LOG_WARN("declare collection super type failed", K(ret));
  }
  return ret;
}

int ObPLCodeGenerator::build_assoc_array_type(const ObAssocArrayType &array_type,
                                                 ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  //ObObj*
  ObLLVMType obj_type;
  ObLLVMType key_type;
  //int64*
  ObLLVMType int_type;
  ObLLVMType sort_type;
  if (OB_FAIL(build_collection_type(array_type, elem_type_array))) {
    LOG_WARN("declare collection super type failed", K(ret));
  } else if (OB_FAIL(adt_service_.get_obj(obj_type))) {
    LOG_WARN("failed to get obj type", K(ret));
  } else if (OB_FAIL(obj_type.get_pointer_to(key_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int_type))) {
    LOG_WARN("failed to get int type", K(ret));
  } else if (OB_FAIL(int_type.get_pointer_to(sort_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(elem_type_array.push_back(key_type))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(elem_type_array.push_back(sort_type))) {
    LOG_WARN("push_back error", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::build_varray_type(const ObVArrayType &array_type,
                                                 ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  ObLLVMType int_type;
  if (OB_FAIL(build_collection_type(array_type, elem_type_array))) {
    LOG_WARN("declare collection super type failed", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int_type))) {
    LOG_WARN("failed to get int type", K(ret));
  } else if (OB_FAIL(elem_type_array.push_back(int_type))) {
    LOG_WARN("push_back error", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::build_collection_type(const ObCollectionType &collection_type,
                                                ObIArray<ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;

  //common::ObIAllocator *allocator_;
  ObLLVMType allocator_type;
  //common::ObDataType element_type_;
  ObLLVMType element_type;
  //int64_t count_;
  ObLLVMType count_type;
  //int64_t first_;
  ObLLVMType first_type;
  //int64_t last_;
  ObLLVMType last_type;

  ObLLVMType element_ir_type;
  ObLLVMType data_array_type;
  ObLLVMType data_array_pointer_type;

  OZ (build_composite(elem_type_array));
  OZ (helper_.get_llvm_type(ObIntType, allocator_type));
  OZ (adt_service_.get_elem_desc(element_type));
  OZ (helper_.get_llvm_type(ObIntType, count_type));
  OZ (helper_.get_llvm_type(ObIntType, first_type));
  OZ (helper_.get_llvm_type(ObIntType, last_type));
  OZ (elem_type_array.push_back(allocator_type));
  OZ (elem_type_array.push_back(element_type));
  OZ (elem_type_array.push_back(count_type));
  OZ (elem_type_array.push_back(first_type));
  OZ (elem_type_array.push_back(last_type));
  OZ (get_datum_type(collection_type.get_element_type(), element_ir_type));
  OZ (ObLLVMHelper::get_array_type(element_ir_type, 0, data_array_type));
  OZ (data_array_type.get_pointer_to(data_array_pointer_type));
  OZ (elem_type_array.push_back(data_array_pointer_type));
  return ret;
}
#endif

int ObPLCodeGenerator::build_record_type(const ObRecordType &record_type,
                                         ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;

  //int64_t count_;
  ObLLVMType count_type;
  ObLLVMType is_null_type;
  ObLLVMType null_array_type;
  ObLLVMType meta_type;
  ObLLVMType meta_array_type;
  ObLLVMType member_type;
  ObLLVMType member_array_type;

  OZ (build_composite(elem_type_array));
  OZ (helper_.get_llvm_type(ObInt32Type, count_type));
  OZ (elem_type_array.push_back(count_type));

  OZ (helper_.get_llvm_type(ObTinyIntType, is_null_type));
  for (int64_t i = 0; OB_SUCC(ret) && i < record_type.get_record_member_count(); ++i) {
    OZ (elem_type_array.push_back(is_null_type));
  }

  OZ (adt_service_.get_data_type(meta_type));
  for (int64_t i = 0; OB_SUCC(ret) && i < record_type.get_record_member_count(); ++i) {
    OZ (elem_type_array.push_back(meta_type));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < record_type.get_record_member_count(); ++i) {
    CK (OB_NOT_NULL(record_type.get_record_member_type(i)));
    OZ (get_datum_type(*record_type.get_record_member_type(i), member_type));
    OZ (elem_type_array.push_back(member_type));
  }

  return ret;
}

int ObPLCodeGenerator::build_composite(ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  //ObPLType type_;
  //uint64_t id_;
  //bool is_null_;
  ObLLVMType type;
  ObLLVMType id;
  ObLLVMType is_null;
  OZ (helper_.get_llvm_type(ObInt32Type, type));
  OZ (elem_type_array.push_back(type));
  OZ (helper_.get_llvm_type(ObIntType, id));
  OZ (elem_type_array.push_back(id));
  OZ (helper_.get_llvm_type(ObTinyIntType, is_null));
  OZ (elem_type_array.push_back(is_null));
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLCodeGenerator::build_subtype(const ObUserDefinedSubType &subtype,
                                     ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  ObLLVMType base_type;
  CK (OB_NOT_NULL(subtype.get_base_type()));
  OZ (get_datum_type(*subtype.get_base_type(), base_type));
  OZ (elem_type_array.push_back(base_type));
  return ret;
}

int ObPLCodeGenerator::build_opaque_type(const ObUserDefinedType &opaque_type,
                                         ObIArray<jit::ObLLVMType> &elem_type_array)
{
  int ret = OB_SUCCESS;
  UNUSED(opaque_type);
  ObLLVMType type;
  OZ (helper_.get_llvm_type(ObInt32Type, type));
  OZ (elem_type_array.push_back(type));
  return ret;
}
#endif

int ObPLCodeGenerator::init()
{
  int ret = OB_SUCCESS;

  // CG local types + external types at least, so pre-allocate doubled buckets
  // bucket number will grow up automatically if udt_count_guess is not enough
  int64_t udt_count_guess =
      (ast_.get_user_type_table().get_count() +
        ast_.get_user_type_table().get_external_types().count()) * 2;

  // make udt_count_guess at least 64, to prevent size grow up frequently in bad case
  if (udt_count_guess < 64) {
    udt_count_guess = 64;
  }

  int64_t goto_label_count_guess = 64;
  if (OB_NOT_NULL(ast_.get_body()) &&
       ast_.get_body()->get_stmts().count() > goto_label_count_guess) {
    goto_label_count_guess = ast_.get_body()->get_stmts().count();
  }

  if (OB_FAIL(user_type_map_.create(
               udt_count_guess,
               ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_CODE_GEN))))){
    LOG_WARN("failed to create user_type_map_", K(ret), K(udt_count_guess));
  } else if (OB_FAIL(di_user_type_map_.create(
                      udt_count_guess,
                      ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_CODE_GEN))))){
    LOG_WARN("failed to create di_user_type_map_", K(ret), K(udt_count_guess));
  } else if (OB_FAIL(goto_label_map_.create(
                      goto_label_count_guess,
                      ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_CODE_GEN))))) {
    LOG_WARN("failed to create goto_label_map_", K(ret), K(goto_label_count_guess));
  } else if (debug_mode_ && OB_FAIL(di_helper_.init(helper_.get_jc()))) {
    LOG_WARN("failed to init di helper", K(ret));
  } else if (OB_FAIL(init_spi_service())) {
    LOG_WARN("failed to init spi service", K(ret));
  } else if (OB_FAIL(init_adt_service())) {
    LOG_WARN("failed to init adt service", K(ret));
  } else if (OB_FAIL(init_eh_service())) {
    LOG_WARN("failed to init eh service", K(ret));
  } else if (OB_FAIL(init_di_adt_service())) {
    LOG_WARN("failed to init di service", K(ret));
  } else {
    ObSEArray<ObLLVMType, 8> arg_types;
    ObLLVMFunctionType ft;
    ObLLVMType pl_exec_context_type;
    ObLLVMType pl_exec_context_pointer_type;
    ObLLVMType int64_type;
    ObLLVMType int64_pointer_type;
    ObLLVMType int32_type;
    ObLLVMType bool_type;
    if (OB_FAIL(adt_service_.get_pl_exec_context(pl_exec_context_type))) {
      LOG_WARN("failed to get argv type", K(ret));
    } else if (OB_FAIL(pl_exec_context_type.get_pointer_to(pl_exec_context_pointer_type))) {
      LOG_WARN("failed to get_pointer_to", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(int64_type.get_pointer_to(int64_pointer_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, bool_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else { /*do nothing*/ }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //uint64_t package id
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //uint64_t proc id
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_pointer_type))) { //int64_t* subprogram path
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t path length
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t line number
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t ArgC
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t[] ArgV
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_pointer_type))) { //int64_t* nocopy params
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t dblink id
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(ObString("pl_execute"), ft, pl_execute_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }

    //declare user type var addr
    if (OB_SUCC(ret)) {
      arg_types.reset();
      if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t var_index
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int64_type))) { //int64_t var_addr
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(int32_type))) { //int32_t init_value
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(ObString("set_user_type_var"), ft, set_user_type_var_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }
    // declare set_implicit_in_forall
    if (OB_SUCC(ret)) {
      arg_types.reset();
      if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(arg_types.push_back(bool_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(ObString("set_implicit_cursor_in_forall"), ft, set_implicit_cursor_in_forall_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }
    // declare unset_implicit_in_forall
    if (OB_SUCC(ret)) {
      arg_types.reset();
      if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(ObString("unset_implicit_cursor_in_forall"), ft, unset_implicit_cursor_in_forall_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLCodeGenerator::init_spi_service()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMType, 9> arg_types;
  ObLLVMFunctionType ft;
  ObLLVMType pl_exec_context_type;
  ObLLVMType pl_exec_context_pointer_type;
  ObLLVMType obj_param_type;
  ObLLVMType obj_param_pointer_type;
  ObLLVMType obj_type;
  ObLLVMType obj_pointer_type;
  ObLLVMType data_type;
  ObLLVMType data_type_pointer_type;
  ObLLVMType int64_type;
  ObLLVMType int32_type;
  ObLLVMType bool_type;
  ObLLVMType char_type;
  ObLLVMType int_pointer_type;
  ObLLVMType bool_type_pointer_type;

  if (OB_FAIL(adt_service_.get_pl_exec_context(pl_exec_context_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(pl_exec_context_type.get_pointer_to(pl_exec_context_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else  if (OB_FAIL(adt_service_.get_objparam(obj_param_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(obj_param_type.get_pointer_to(obj_param_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(adt_service_.get_obj(obj_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(obj_type.get_pointer_to(obj_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(adt_service_.get_data_type(data_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(data_type.get_pointer_to(data_type_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, bool_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObCharType, char_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(int64_type.get_pointer_to(int_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(bool_type.get_pointer_to(bool_type_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(obj_param_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_calc_expr_at_idx"), ft, spi_service_.spi_calc_expr_at_idx_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(obj_param_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_calc_package_expr"), ft, spi_service_.spi_calc_package_expr_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(obj_param_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(obj_param_pointer_type));
    OZ (arg_types.push_back(bool_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_convert_objparam"), ft, spi_service_.spi_convert_objparam_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(obj_param_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_set_variable_to_expr"), ft, spi_service_.spi_set_variable_to_expr_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(char_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(data_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_query_into_expr_idx"), ft, spi_service_.spi_query_into_expr_idx_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(bool_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_end_trans"), ft, spi_service_.spi_end_trans_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(char_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(data_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_execute_with_expr_idx"), ft, spi_service_.spi_execute_with_expr_idx_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) { // param_mode
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(data_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_execute_immediate"), ft, spi_service_.spi_execute_immediate_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(bool_type)); //int8 type
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int32_type));
    OZ (arg_types.push_back(int_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_alloc_complex_var"), ft, spi_service_.spi_alloc_complex_var_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(obj_param_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_construct_collection"), ft, spi_service_.spi_construct_collection_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_clear_diagnostic_area"), ft, spi_service_.spi_clear_diagnostic_area_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { // package id
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_extend_collection"), ft, spi_service_.spi_extend_collection_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_delete_collection"), ft, spi_service_.spi_delete_collection_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_trim_collection"),
                                               ft, spi_service_.spi_trim_collection_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_cursor_init"), ft, spi_service_.spi_cursor_init_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(char_type)); //sql
    OZ (arg_types.push_back(char_type));//id
    OZ (arg_types.push_back(int64_type));//type
    OZ (arg_types.push_back(bool_type));//for_update
    OZ (arg_types.push_back(bool_type));//hidden_rowid
    OZ (arg_types.push_back(int_pointer_type));//sql_param_exprs
    OZ (arg_types.push_back(int64_type));//sql_param_count
    OZ (arg_types.push_back(int64_type));//package_id
    OZ (arg_types.push_back(int64_type));//routine_id
    OZ (arg_types.push_back(int64_type));//cursor_index
    OZ (arg_types.push_back(int_pointer_type));//formal_param_idxs
    OZ (arg_types.push_back(int_pointer_type));//actual_param_exprs
    OZ (arg_types.push_back(int64_type));//cursor_param_count
    OZ (arg_types.push_back(bool_type));//skip_locked
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_cursor_open_with_param_idx"), ft, spi_service_.spi_cursor_open_with_param_idx_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type)); //sql_expr
    OZ (arg_types.push_back(int_pointer_type));//sql_param_exprs
    OZ (arg_types.push_back(int64_type));//sql_param_count
    OZ (arg_types.push_back(int64_type));//package_id
    OZ (arg_types.push_back(int64_type));//routine_id
    OZ (arg_types.push_back(int64_type));//cursor_index
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_dynamic_open"), ft, spi_service_.spi_dynamic_open_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type));//package_id
    OZ (arg_types.push_back(int64_type));//routine_id
    OZ (arg_types.push_back(int64_type));//cursor_index
    OZ (arg_types.push_back(int_pointer_type));//into_exprs
    OZ (arg_types.push_back(int64_type));//into_count
    OZ (arg_types.push_back(data_type_pointer_type));//column_types
    OZ (arg_types.push_back(int64_type));//type_count
    OZ (arg_types.push_back(bool_type_pointer_type));//exprs_not_null_flag
    OZ (arg_types.push_back(int_pointer_type));//pl_integer_ranges
    OZ (arg_types.push_back(bool_type));//is_bulk
    OZ (arg_types.push_back(int64_type));//limit
    OZ (arg_types.push_back(data_type_pointer_type));//return_type
    OZ (arg_types.push_back(int64_type));//return_type_count
    OZ (arg_types.push_back(bool_type));//is_type_record
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_cursor_fetch"), ft, spi_service_.spi_cursor_fetch_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type));//package_id
    OZ (arg_types.push_back(int64_type));//routine_id
    OZ (arg_types.push_back(int64_type));//cursor_index
    OZ (arg_types.push_back(bool_type));//ignore
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_cursor_close"), ft, spi_service_.spi_cursor_close_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_destruct_collection"), ft, spi_service_.spi_destruct_collection_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
      arg_types.reset();
      if (OB_FAIL(arg_types.push_back(int64_type))) { //src
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(ObString("spi_reset_collection"), ft, spi_service_.spi_reset_collection_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { // 函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { //src index
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { //dest index
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int32_type))) { //subarray lower pos
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int32_type))) { //subarray upper pos
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_sub_nestedtable"), ft, spi_service_.spi_sub_nestedtable_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type)); //allocator
    OZ (arg_types.push_back(obj_pointer_type)); //src
    OZ (arg_types.push_back(obj_pointer_type)); //dest
    OZ (arg_types.push_back(data_type_pointer_type)); //dest type
    OZ (arg_types.push_back(int64_type)); // package id
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_copy_datum"), ft, spi_service_.spi_copy_datum_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(obj_pointer_type)); //src
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_destruct_obj"), ft, spi_service_.spi_destruct_obj_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { // error code
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(bool_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_set_pl_exception_code"), ft, spi_service_.spi_set_pl_exception_code_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("spi_get_pl_exception_code"), ft, spi_service_.spi_get_pl_exception_code_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_raise_application_error"), ft, spi_service_.spi_raise_application_error_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_check_early_exit"), ft, spi_service_.spi_check_early_exit_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(obj_param_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(
      ObString("spi_pipe_row_to_result"), ft, spi_service_.spi_pipe_row_to_result_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_check_exception_handler_legal"),
                                ft, spi_service_.spi_check_exception_handler_legal_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(char_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_interface_impl"),
                                ft, spi_service_.spi_interface_impl_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_process_nocopy_params"),
                                ft, spi_service_.spi_process_nocopy_params_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(obj_pointer_type)); // ref cursor
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_add_ref_cursor_refcount"), ft, spi_service_.spi_add_ref_cursor_refcount_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type)); //函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type)); //package id
    OZ (arg_types.push_back(int64_type)); //routine id
    OZ (arg_types.push_back(int64_type)); //cursor index
    OZ (arg_types.push_back(int64_type)); //addend
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_handle_ref_cursor_refcount"), ft, spi_service_.spi_handle_ref_cursor_refcount_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_update_package_change_info"),
                                ft, spi_service_.spi_update_package_change_info_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(obj_param_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_check_composite_not_null"),
                                ft, spi_service_.spi_check_composite_not_null_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int_pointer_type));
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(bool_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_process_resignal"), ft, spi_service_.spi_process_resignal_error_));
  }
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_check_autonomous_trans"), ft, spi_service_.spi_check_autonomous_trans_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_opaque_assign_null"), ft, spi_service_.spi_opaque_assign_null_));;
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));  // 函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type));                    // line
    OZ (arg_types.push_back(int64_type));                    // level
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_pl_profiler_before_record"), ft, spi_service_.spi_pl_profiler_before_record_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(pl_exec_context_pointer_type));  // 函数第一个参数必须是基础环境信息隐藏参数
    OZ (arg_types.push_back(int64_type));                    // line
    OZ (arg_types.push_back(int64_type));                    // level
    OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
    OZ (helper_.create_function(ObString("spi_pl_profiler_after_record"), ft, spi_service_.spi_pl_profiler_after_record_));
  }

  return ret;
}

int ObPLCodeGenerator::init_adt_service()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adt_service_.init(get_param_size(), ast_.get_symbol_table().get_count()))) {
    LOG_WARN("failed to init adt service", K(ret));
  }
  return ret;
}

int ObPLCodeGenerator::init_eh_service()
{
  int ret = OB_SUCCESS;

  eh_service_.pl_exception_class_ = ObPLEHService::get_exception_class();
  eh_service_.pl_exception_base_offset_ = ObPLEHService::get_exception_base_offset();

  ObSEArray<ObLLVMType, 8> arg_types;
  ObLLVMFunctionType ft;
  ObLLVMType unwind_exception_type;
  ObLLVMType unwind_exception_pointer_type;
  ObLLVMType condition_type;
  ObLLVMType condition_pointer_type;
  ObLLVMType obj_type;
  ObLLVMType obj_pointer_type;
  ObLLVMType objparam_type;
  ObLLVMType objparam_pointer_type;
  ObLLVMType int_pointer_type;
  ObLLVMType int32_pointer_type;
  ObLLVMType tinyint_pointer_type;
  ObLLVMType char_pointer_type;
  ObLLVMType int64_type;
  ObLLVMType int32_type;
  ObLLVMType int8_type;
  ObLLVMType char_type;
  ObLLVMType void_type;

  if (OB_FAIL(adt_service_.get_unwind_exception(unwind_exception_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(unwind_exception_type.get_pointer_to(unwind_exception_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(adt_service_.get_pl_condition_value(condition_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(condition_type.get_pointer_to(condition_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(adt_service_.get_obj(obj_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(obj_type.get_pointer_to(obj_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(adt_service_.get_objparam(objparam_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(objparam_type.get_pointer_to(objparam_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, int32_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, int8_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObCharType, char_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.get_void_type(void_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(int64_type.get_pointer_to(int_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(int32_type.get_pointer_to(int32_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(int8_type.get_pointer_to(tinyint_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(char_type.get_pointer_to(char_pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(arg_types.push_back(int64_type))) { // obplexeccontext
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { // obplfunction
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { // line number
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(condition_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(unwind_exception_pointer_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("eh_create_exception"), ft, eh_service_.eh_create_exception_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(unwind_exception_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("_Unwind_RaiseException"), ft, eh_service_.eh_raise_exception_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(unwind_exception_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(void_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("_Unwind_Resume"), ft, eh_service_.eh_resume_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int8_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int8_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("eh_personality"), ft, eh_service_.eh_personality_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(int8_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int32_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(char_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int_pointer_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int32_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("eh_convert_exception"), ft, eh_service_.eh_convert_exception_))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    if (OB_FAIL(arg_types.push_back(char_type))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(ObLLVMFunctionType::get(int64_type, arg_types, ft))) {
      LOG_WARN("failed to get function type", K(ret));
    } else if (OB_FAIL(helper_.create_function(ObString("eh_classify_exception"), ft, eh_service_.eh_classify_exception))) {
      LOG_WARN("failed to create function", K(ret));
    } else { /*do nothing*/ }
  }

  //for debug
  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int64_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int64"), ft, eh_service_.eh_debug_int64_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int_pointer_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int64ptr"), ft, eh_service_.eh_debug_int64ptr_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int32_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int32"), ft, eh_service_.eh_debug_int32_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int32_pointer_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int32ptr"), ft, eh_service_.eh_debug_int32ptr_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(int8_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int8"), ft, eh_service_.eh_debug_int8_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(tinyint_pointer_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_int8ptr"), ft, eh_service_.eh_debug_int8ptr_));
  }

  if (OB_SUCC(ret)) {
    arg_types.reset();
    OZ (arg_types.push_back(char_type));
    OZ (arg_types.push_back(int64_type));
    OZ (arg_types.push_back(obj_pointer_type));
    OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
    OZ (helper_.create_function(ObString("eh_debug_obj"), ft, eh_service_.eh_debug_obj_));
  }

  if (OB_SUCC(ret)) {
      arg_types.reset();
      OZ (arg_types.push_back(char_type));
      OZ (arg_types.push_back(int64_type));
      OZ (arg_types.push_back(objparam_pointer_type));
      OZ (ObLLVMFunctionType::get(void_type, arg_types, ft));
      OZ (helper_.create_function(ObString("eh_debug_objparam"), ft, eh_service_.eh_debug_objparam_));
    }

  return ret;
}

int ObPLCodeGenerator::set_var_addr_to_param_store(
  int64_t var_index, jit::ObLLVMValue &var, jit::ObLLVMValue &init_value)
{
  int ret = OB_SUCCESS;
  ObLLVMValue ir_index;
  ObLLVMType int_type;
  ObLLVMValue var_addr;

  if (OB_FAIL(helper_.get_int64(var_index, ir_index))) {
    LOG_WARN("failed to get int64", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_ptr_to_int(ObString("cast_ptr_to_int64"), var, int_type, var_addr))) {
    LOG_WARN("failed to create ptr to int", K(ret));
#ifndef NDEBUG
  } else if (OB_FAIL(generate_debug(ObString("debug"), var_addr))) {
    LOG_WARN("failed to create call", K(ret));
#endif
  } else {
    ObSEArray<ObLLVMValue, 4> args;
    if (OB_FAIL(args.push_back(vars_.at(CTX_IDX)))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(args.push_back(ir_index))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(args.push_back(var_addr))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(args.push_back(init_value))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_call(ObString("set_var_addr"), get_user_type_var_func(), args))) {
      LOG_WARN("failed to create call", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_get_collection_attr(ObLLVMValue &param_array,
                                         const ObObjAccessIdx &current_access,
                                         int64_t access_i,
                                         bool for_write,
                                         bool is_assoc_array,
                                         ObLLVMValue &current_value,
                                         ObLLVMValue &ret_value_ptr,
                                         ObLLVMBasicBlock& exit)
{
  int ret = OB_SUCCESS;
  ObLLVMValue ret_value;
  // 对于数组类型的访问, 需要确保其已经通过构造函数进行了初始化
  ObLLVMValue is_inited, not_init_value, not_init;
  ObLLVMBasicBlock after_init_block, not_init_block;
  OZ (helper_.create_block(ObString("after_init_block"), func_, after_init_block));
  OZ (helper_.create_block(ObString("not_init_block"), func_, not_init_block));
  OZ (generate_debug("generate_get_collection_attr", current_value));
  OZ (extract_count_from_collection(current_value, is_inited));
  OZ (helper_.get_int64(-1, not_init_value));
  OZ (helper_.create_icmp(is_inited, not_init_value, ObLLVMHelper::ICMP_EQ, not_init));
  OZ (helper_.create_cond_br(not_init, not_init_block, after_init_block));
  OZ (helper_.set_insert_point(not_init_block));
  OZ (helper_.get_int32(OB_ERR_COLLECION_NULL, ret_value));
  OZ (helper_.create_store(ret_value, ret_value_ptr));
  OZ (helper_.create_br(exit));
  OZ (helper_.set_insert_point(after_init_block));

  ObLLVMValue data_value;
  if (!current_access.is_property()) {
    // 取出集合中元素的值ObCollection->data_
    // 当时获取Collection的固有属性时, 不需要这么做, 固有属性都存储在Collection本身上
    OZ (extract_data_from_collection(current_value, data_value));
  }
  // 获取下标或者固有属性在Collection中的位置信息
  if (OB_SUCC(ret)) {
    ObLLVMValue element_idx;
    // 常量, 直接Codegen掉
    if (current_access.is_property()) {
      //do nothing
    } else if (current_access.is_const()) {
      OZ (helper_.get_int64(current_access.var_index_ - 1, element_idx));
    } else {
      // 非常量，那么首先用obj access中的var_index获取到变量的值，然后再获取element
      ObLLVMValue element_idx_ptr;
      OZ (helper_.create_gep(ObString("param_value"),
                             param_array,
                             current_access.var_index_,
                             element_idx_ptr));
      OZ (helper_.create_load(ObString("element_idx"), element_idx_ptr, element_idx));
      OZ (helper_.create_dec(element_idx, element_idx));
    }
    if (OB_SUCC(ret)) {
      // 如果不是固有属性, 需要check下标的合法性, 避免访问越界
      if (!current_access.is_property()) {
        ObLLVMValue low, high_ptr, high, is_true;
        ObLLVMBasicBlock check_block, after_block, error_block;
        ObLLVMBasicBlock delete_block, after_delete_block;
        char check_block_name[OB_MAX_OBJECT_NAME_LENGTH];
        char check_after_name[OB_MAX_OBJECT_NAME_LENGTH];
        char check_error_name[OB_MAX_OBJECT_NAME_LENGTH];
        char delete_block_name[OB_MAX_OBJECT_NAME_LENGTH];
        char after_delete_name[OB_MAX_OBJECT_NAME_LENGTH];
        int64_t check_pos = 0, after_pos = 0, error_pos = 0;
        int64_t delete_pos = 0, after_delete_pos = 0;
        OZ (databuff_printf(check_block_name,
                            OB_MAX_OBJECT_NAME_LENGTH,
                            check_pos,
                            "check_block_%ld",
                            access_i));
        OZ (databuff_printf(check_after_name,
                            OB_MAX_OBJECT_NAME_LENGTH,
                            after_pos,
                            "after_block_%ld",
                            access_i));
        OZ (databuff_printf(check_error_name,
                            OB_MAX_OBJECT_NAME_LENGTH,
                            error_pos,
                            "check_error_%ld",
                            access_i));
        OZ (databuff_printf(delete_block_name,
                            OB_MAX_OBJECT_NAME_LENGTH,
                            delete_pos,
                            "delete_pos_%ld",
                            access_i));
        OZ (databuff_printf(after_delete_name,
                            OB_MAX_OBJECT_NAME_LENGTH,
                            after_delete_pos,
                            "after_delete_pos_%ld",
                            access_i));
        OZ (helper_.create_block(ObString(check_pos, check_block_name), func_, check_block));
        OZ (helper_.create_block(ObString(after_pos, check_after_name), func_, after_block));
        OZ (helper_.create_block(ObString(error_pos, check_error_name), func_, error_block));

        OZ (helper_.get_int64(0, low));
        OZ (helper_.create_icmp(element_idx, low, ObLLVMHelper::ICMP_SGE, is_true));
        OZ (helper_.create_cond_br(is_true, check_block, error_block));
        OZ (helper_.set_insert_point(check_block));
        OZ (helper_.create_gep(ObString("rowcount"),
                               current_value,
                               IDX_COLLECTION_COUNT,
                               high_ptr));
        OZ (helper_.create_load(ObString("load_rowcount"), high_ptr, high));
        OZ (helper_.create_icmp(high, element_idx, ObLLVMHelper::ICMP_SGT, is_true));
        OZ (helper_.create_cond_br(is_true, after_block, error_block));
        OZ (helper_.set_insert_point(error_block));
        OZ (helper_.get_int32(is_assoc_array ? OB_READ_NOTHING : OB_ERR_SUBSCRIPT_OUTSIDE_LIMIT, ret_value));
        OZ (helper_.create_store(ret_value, ret_value_ptr));
        OZ (helper_.create_br(exit));
        OZ (helper_.set_insert_point(after_block));
        OZ (helper_.get_int32(OB_SUCCESS, ret_value));
        OZ (helper_.create_store(ret_value, ret_value_ptr));
        OZ (helper_.create_gep(ObString("table_element_array"),
                               data_value,
                               element_idx,
                               current_value));

        if (OB_SUCC(ret)) {
          // check deleted value
          ObLLVMValue p_type_value;
          ObLLVMValue type_value;
          ObLLVMValue is_deleted;
          OZ (helper_.create_block(ObString(delete_pos, delete_block_name), func_, delete_block));
          OZ (helper_.create_block(ObString(after_delete_pos, after_delete_name), func_, after_delete_block));
          OZ (extract_type_ptr_from_obj(current_value, p_type_value));
          OZ (helper_.create_load(ObString("load_type"), p_type_value, type_value));
          OZ (helper_.create_icmp_eq(type_value, ObMaxType, is_deleted));
          OZ (helper_.create_cond_br(is_deleted, delete_block, after_delete_block));
          OZ (helper_.set_insert_point(delete_block));
          if (!for_write) {
            OZ (helper_.get_int32(OB_READ_NOTHING, ret_value));
            OZ (helper_.create_store(ret_value, ret_value_ptr));
            OZ (helper_.create_br(exit));
          } else {
            if (current_access.var_type_.is_composite_type()) {
              OZ (helper_.get_int8(ObExtendType, type_value));
              OZ (helper_.create_store(type_value, p_type_value));
            }
            OZ (helper_.create_br(after_delete_block));
          }
          OZ (helper_.set_insert_point(after_delete_block));
        }
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_get_record_attr(const ObObjAccessIdx &current_access,
                                                uint64_t udt_id,
                                                bool for_write,
                                                ObLLVMValue &current_value,
                                                ObLLVMValue &ret_value_ptr,
                                                ObLLVMBasicBlock& exit)
{
  int ret = OB_SUCCESS;
  ObLLVMValue ret_value;
  ObLLVMValue data_value;
  const ObUserDefinedType *user_type = NULL;
  const ObRecordType *record_type = NULL;
  int64_t field_cnt = OB_INVALID_COUNT;
  int64_t element_idx = RECORD_META_OFFSET; //type_ is 0 and count_ is 1
  if (NULL == (user_type = ast_.get_user_type_table().get_type(udt_id))
      && NULL == (user_type = ast_.get_user_type_table().get_external_type(udt_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get user type failed", K(ret));
  }
  CK (OB_NOT_NULL(user_type));
  CK (user_type->is_record_type());
  OX (record_type = static_cast<const ObRecordType*>(user_type));
  OX (element_idx += record_type->get_record_member_count()); //null map
  OX (element_idx += record_type->get_record_member_count()); //record meta list
  OV (current_access.is_const(), OB_ERR_UNEXPECTED, K(current_access));
  OX (element_idx += current_access.var_index_); //访问的域对应的下标
  if (OB_SUCC(ret) && user_type->is_object_type() && for_write) {
    ObLLVMBasicBlock null_block, not_null_block;
    ObLLVMValue is_null, err_value, is_null_object;

    OZ (extract_isnull_from_record(current_value, is_null_object));
#ifndef NDEBUG
    OZ (generate_debug(ObString("object instance is null"), is_null_object));
#endif
    OZ (get_helper().create_block(ObString("null_block"), get_func(), null_block));
    OZ (get_helper().create_block(ObString("not_null_block"), get_func(), not_null_block));
    OZ (get_helper().create_icmp_eq(is_null_object, TRUE, is_null));
    OZ (get_helper().create_cond_br(is_null, null_block, not_null_block));
    OZ (set_current(null_block));
    OZ (get_helper().get_int32(OB_ERR_ACCESS_INTO_NULL, err_value));
    OZ (helper_.create_store(err_value, ret_value_ptr));
    OZ (helper_.create_br(exit));
    OZ (set_current(not_null_block));
  }
  OZ (helper_.create_gep(ObString("extract record data"),
                         current_value,
                         element_idx,
                         current_value));
  return ret;
}

int ObPLCodeGenerator::generate_get_attr(ObLLVMValue &param_array,
                                         const ObIArray<ObObjAccessIdx> &obj_access,
                                         bool for_write,
                                         ObLLVMValue &value_ptr,     // 读取的值
                                         ObLLVMValue &ret_value_ptr, // 标识是否越界
                                         ObLLVMBasicBlock& exit)     // 越界后需要跳转的BLOCK
{
  int ret = OB_SUCCESS;
  ObLLVMType user_type;
  ObLLVMValue composite_addr;
  ObLLVMValue ret_value;
  ObLLVMValue value;
  ObLLVMType int64_type;

  CK (!obj_access.empty());
  CK (obj_access.at(0).var_type_.is_composite_type()
      || obj_access.at(0).var_type_.is_cursor_type());

  // 获取数据类型
  OZ (helper_.get_llvm_type(ObIntType, int64_type));
  // 获取变量首地址
  OZ (helper_.get_int32(OB_SUCCESS, ret_value));
  OZ (helper_.create_store(ret_value, ret_value_ptr));
  OZ (helper_.create_gep(ObString("param_value"),
                         param_array,
                         obj_access.at(0).var_index_,
                         composite_addr));
  OZ (helper_.create_load(ObString("element_idx"), composite_addr, composite_addr));
  // 逐级解析复杂数据类型
  for (int64_t i = 1; OB_SUCC(ret) && i < obj_access.count(); ++i) {
    const ObPLDataType &parent_type = obj_access.at(i - 1).var_type_;
    OZ (user_type_map_.get_refactored(parent_type.get_user_type_id(), user_type),
        parent_type.get_user_type_id());
    OZ (user_type.get_pointer_to(user_type));
    OZ (helper_.create_int_to_ptr(ObString("var_value"), composite_addr, user_type, value));
    CK (!obj_access.at(i).is_invalid());
    if (OB_SUCC(ret)) {
      if (parent_type.is_collection_type()) { //数组类型
        OZ (generate_get_collection_attr(param_array,
                                         obj_access.at(i),
                                         i,
                                         for_write,
                                         parent_type.is_associative_array_type(),
                                         value,
                                         ret_value_ptr,
                                         exit));
      } else if (parent_type.is_record_type()) { //record类型
        OZ (generate_get_record_attr(obj_access.at(i),
                                     parent_type.get_user_type_id(),
                                     for_write,
                                     value,
                                     ret_value_ptr,
                                     exit), K(obj_access), K(i));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected user type" , K(obj_access.at(i - 1).var_type_), K(ret));
      }
    }

    if (obj_access.at(i).var_type_.is_composite_type()) {
      OZ (extract_value_from_obj(value, ObExtendType, composite_addr));
    }
  }

  if (ObObjAccessIdx::get_final_type(obj_access).is_obj_type()) {
    OZ (helper_.create_ptr_to_int(ObString("element_value_addr"), value, int64_type, value));
  } else {
    value = composite_addr;
  }
  OZ (helper_.create_store(value, value_ptr));
  return ret;
}

int ObPLCodeGenerator::generate_declare_cursor(const ObPLStmt &s, const int64_t &cursor_index)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
    // 控制流已断，后面的语句不再处理
  } else {
    const ObPLCursor *cursor = s.get_cursor(cursor_index);
    CK (OB_NOT_NULL(cursor));
    CK (ObPLCursor::INVALID != cursor->get_state());
    if (OB_SUCC(ret) && ObPLCursor::DUP_DECL != cursor->get_state()) { //如果是无效的cursor跳过即可
      ObLLVMValue cursor_index_value;
      ObLLVMValue cursor_value;
      ObLLVMValue ret_err;
      ObSEArray<ObLLVMValue, 2> args;
      OZ (get_helper().set_insert_point(get_current()));
      OZ (set_debug_location(s));
      OZ (get_helper().get_int64(cursor->get_index(), cursor_index_value));
      OZ (args.push_back(get_vars().at(CTX_IDX)));
      OZ (args.push_back(cursor_index_value));
      OZ (get_helper().create_call(ObString("spi_cursor_init"), get_spi_service().spi_cursor_init_, args, ret_err));
      OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
      OZ (extract_objparam_from_context(get_vars().at(CTX_IDX), cursor->get_index(), cursor_value));
      OZ (helper_.create_gep(ObString("obj"), cursor_value, 0, cursor_value));
      OX (get_vars().at(cursor->get_index() + USER_ARG_OFFSET) = cursor_value);
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_open(
  const ObPLStmt &s, const ObPLSql &cursor_sql,
  const uint64_t package_id, const uint64_t routine_id, const int64_t cursor_index)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(get_helper().set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else {
    ObSEArray<ObLLVMValue, 8> args;
    //sql & ps_id & stmt_type & params & param count
    ObLLVMValue str;
    ObLLVMValue len;
    ObLLVMValue ps_sql;
    ObLLVMValue type;
    ObLLVMValue for_update;
    ObLLVMValue hidden_rowid;
    ObLLVMValue sql_params;
    ObLLVMValue sql_param_count;
    ObLLVMValue package_id_value;
    ObLLVMValue routine_id_value;
    ObLLVMValue cursor_index_value;
    ObLLVMValue formal_params;
    ObLLVMValue actual_params;
    ObLLVMValue cursor_param_count;
    ObLLVMValue skip_locked;
    ObLLVMValue ret_err;
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (generate_sql(cursor_sql, str, len, ps_sql, type, for_update, hidden_rowid, sql_params,
                     sql_param_count, skip_locked));
    OZ (args.push_back(str));
    OZ (args.push_back(ps_sql));
    OZ (args.push_back(type));
    OZ (args.push_back(for_update));
    OZ (args.push_back(hidden_rowid));
    OZ (args.push_back(sql_params));
    OZ (args.push_back(sql_param_count));
    OZ (get_helper().get_int64(package_id, package_id_value));
    OZ (args.push_back(package_id_value));
    OZ (get_helper().get_int64(routine_id, routine_id_value));
    OZ (args.push_back(routine_id_value));
    OZ (get_helper().get_int64(cursor_index, cursor_index_value));
    OZ (args.push_back(cursor_index_value));
    if (PL_OPEN == s.get_type() || PL_OPEN_FOR == s.get_type()) {
      const ObPLOpenStmt &open_stmt = static_cast<const ObPLOpenStmt &>(s);
      const ObPLCursor *cursor = open_stmt.get_cursor();
      OZ (generate_int64_array(cursor->get_formal_params(), formal_params));
      OZ (generate_expression_array(static_cast<const ObPLOpenStmt&>(s).get_params(), actual_params, cursor_param_count));
    } else if (PL_CURSOR_FOR_LOOP == s.get_type()) {
      const ObPLCursorForLoopStmt& for_stmt = static_cast<const ObPLCursorForLoopStmt&>(s);
      const ObPLCursor *cursor = for_stmt.get_cursor();
      OZ (generate_int64_array(cursor->get_formal_params(), formal_params));
      OZ (generate_expression_array(static_cast<const ObPLCursorForLoopStmt&>(s).get_params(), actual_params, cursor_param_count));
    } else { //must be OPEN FOR
      OZ (generate_null_pointer(ObIntType, formal_params));
      OZ (generate_null_pointer(ObIntType, actual_params));
      OZ (helper_.get_int64(0, cursor_param_count));
    }
    OZ (args.push_back(formal_params));
    OZ (args.push_back(actual_params));
    OZ (args.push_back(cursor_param_count));
    OZ (args.push_back(skip_locked));
    OZ (get_helper().create_call(ObString("spi_cursor_open_with_param_idx"), get_spi_service().spi_cursor_open_with_param_idx_, args, ret_err));
    OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
  }
  return ret;
}

int ObPLCodeGenerator::generate_open_for(const ObPLOpenForStmt &s)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(s.get_cursor()));
  // 这个不能，因为如果是ref cursor，这个ref count可能会有多个，这个调用会清零
  // 同时在ob_spi.cpp的 open_cursor_check函数中，reopen之前做了close操作
  OZ (generate_close(s,
                         s.get_package_id(),
                         s.get_cursor()->get_routine_id(),
                         s.get_index(),
                         true/*ignoe if not opened*/));
  if (OB_SUCC(ret)) {
    if (s.is_dynamic()) {
      if (NULL == get_current().get_v()) {
        //控制流已断，后面的语句不再处理
      } else if (OB_FAIL(get_helper().set_insert_point(get_current()))) {
        LOG_WARN("failed to set insert point", K(ret));
      } else if (OB_FAIL(set_debug_location(s))) {
        LOG_WARN("failed to set debug location", K(ret));
      } else {
        ObArray<int64_t> using_exprs;
        ObSEArray<ObLLVMValue, 8> args;
        ObLLVMValue sql_idx;
        ObLLVMValue sql_params;
        ObLLVMValue sql_param_count;
        ObLLVMValue package_id_value;
        ObLLVMValue routine_id_value;
        ObLLVMValue cursor_index_value;
        ObLLVMValue ret_err;
        OZ (args.push_back(get_vars().at(CTX_IDX)));
        OZ (helper_.get_int64(s.get_dynamic_sql(), sql_idx));
        OZ (args.push_back(sql_idx));
        for (int64_t i = 0; OB_SUCC(ret) && i < s.get_using().count(); ++i) {
          OZ (using_exprs.push_back(s.get_using_index(i)));
        }
        OZ (generate_expression_array(using_exprs, sql_params, sql_param_count));
        OZ (args.push_back(sql_params));
        OZ (args.push_back(sql_param_count));
        OZ (get_helper().get_int64(s.get_cursor()->get_package_id(), package_id_value));
        OZ (args.push_back(package_id_value));
        OZ (get_helper().get_int64(s.get_cursor()->get_routine_id(), routine_id_value));
        OZ (args.push_back(routine_id_value));
        OZ (get_helper().get_int64(s.get_index(), cursor_index_value));
        OZ (args.push_back(cursor_index_value));
        OZ (get_helper().create_call(ObString("spi_dynamic_open"),
                                     get_spi_service().spi_dynamic_open_, args, ret_err));
        OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(),
                          s.get_block()->in_warning()));
      }
    } else {
      OZ(generate_open(s,
                       s.get_static_sql(),
                       s.get_cursor()->get_package_id(),
                       s.get_cursor()->get_routine_id(),
                       s.get_index()));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_fetch(const ObPLStmt &s,
                                      const ObPLInto &into,
                                      const uint64_t &package_id,
                                      const uint64_t &routine_id,
                                      const int64_t &cursor_index,
                                      const int64_t &limit,
                                      const ObUserDefinedType *user_defined_type,
                                      jit::ObLLVMValue &ret_err)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
    //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(get_helper().set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else {
    ObSEArray<ObLLVMValue, 8> args;

    ObLLVMValue package_id_value;
    ObLLVMValue routine_id_value;
    ObLLVMValue cursor_index_value;
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (get_helper().get_int64(package_id, package_id_value));
    OZ (args.push_back(package_id_value));
    OZ (get_helper().get_int64(routine_id, routine_id_value));
    OZ (args.push_back(routine_id_value));
    OZ (get_helper().get_int64(cursor_index, cursor_index_value));
    OZ (args.push_back(cursor_index_value));

    ObLLVMValue into_array_value;
    ObLLVMValue into_count_value;
    ObLLVMValue type_array_value;
    ObLLVMValue type_count_value;
    ObLLVMValue exprs_not_null_array_value;
    ObLLVMValue pl_integer_array_value;
    ObLLVMValue is_bulk, is_type_record;
    OZ (generate_into(into, into_array_value, into_count_value,
                            type_array_value, type_count_value,
                            exprs_not_null_array_value,
                            pl_integer_array_value,
                            is_bulk));
    OZ (args.push_back(into_array_value));
    OZ (args.push_back(into_count_value));
    OZ (args.push_back(type_array_value));
    OZ (args.push_back(type_count_value));
    OZ (args.push_back(exprs_not_null_array_value));
    OZ (args.push_back(pl_integer_array_value));
    OZ (args.push_back(is_bulk));

    //limit
    if (OB_SUCC(ret)) {
      ObLLVMValue limit_value;
      if (limit != INT64_MAX) {
        ObLLVMBasicBlock null_block, not_null_block;
        ObLLVMValue p_limit_value, p_type_value, type_value, is_null;
        ObLLVMValue result;

        OZ (get_helper().create_block(ObString("null block"), get_func(), null_block));
        OZ (get_helper().create_block(ObString("not null block"), get_func(), not_null_block));

        OZ (generate_expr(limit, s, OB_INVALID_INDEX, p_limit_value));
        OZ (extract_type_ptr_from_objparam(p_limit_value, p_type_value));
        OZ (get_helper().create_load(ObString("load_type"), p_type_value, type_value));
        OZ (get_helper().create_icmp_eq(type_value, ObNullType, is_null));
        OZ (get_helper().create_cond_br(is_null, null_block, not_null_block));

        OZ (set_current(null_block));
        OZ (get_helper().get_int32(OB_ERR_NULL_VALUE, result));
        OZ (check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
        OZ (get_helper().create_br(not_null_block));

        OZ (set_current(not_null_block));
        CK (OB_NOT_NULL(s.get_expr(limit)));
        OZ (extract_value_from_objparam(p_limit_value, s.get_expr(limit)->get_data_type(), limit_value));
      } else {
        OZ (get_helper().get_int64(limit, limit_value));
      }
      OZ (args.push_back(limit_value));
    }

    ObLLVMValue return_type_array_value;
    ObLLVMValue return_type_count_value;
    ObLLVMValue type_value;
    ObLLVMType data_type;
    ObLLVMType data_type_pointer;
    ObLLVMType array_type;

    if (OB_ISNULL(user_defined_type)) {
      OZ (adt_service_.get_data_type(data_type));
      OZ (data_type.get_pointer_to(data_type_pointer));
      OZ (ObLLVMHelper::get_null_const(data_type_pointer, return_type_array_value));
      OZ (helper_.get_int64(0, return_type_count_value));
    } else {
      const ObRecordType *return_type = static_cast<const ObRecordType*>(user_defined_type);
      const ObPLDataType *pl_data_type = nullptr;
      CK (OB_NOT_NULL(return_type));
      OZ (adt_service_.get_data_type(data_type));
      OZ (data_type.get_pointer_to(data_type_pointer));
      OZ (ObLLVMHelper::get_array_type(data_type,
                                      return_type->get_record_member_count(),
                                      array_type));
      OZ (helper_.create_alloca(ObString("datatype_array"),
                              array_type,
                              return_type_array_value));

      for (int64_t i = 0; OB_SUCC(ret) && i < return_type->get_record_member_count(); ++i) {
        type_value.reset();
        OZ (helper_.create_gep(ObString("extract_datatype"),
                                return_type_array_value,
                                i, type_value));
        pl_data_type = return_type->get_record_member_type(i);
        CK (OB_NOT_NULL(pl_data_type));
        if (OB_SUCC(ret)) {
          if (pl_data_type->is_obj_type()) {
            OZ (store_data_type(*(pl_data_type->get_data_type()), type_value));
          } else { // 构造函数场景
            ObDataType ext_type;
            ext_type.set_obj_type(ObExtendType);
            OZ (store_data_type(ext_type, type_value));
          }
        }
      }

      OZ (helper_.create_bit_cast(ObString("datatype_array_to_pointer"),
              return_type_array_value, data_type_pointer, return_type_array_value));
      OZ (helper_.get_int64(static_cast<int64_t>(return_type->get_record_member_count()),
                            return_type_count_value));
    }

    OZ (args.push_back(return_type_array_value));
    OZ (args.push_back(return_type_count_value));
    OZ (helper_.get_int8(static_cast<int64_t>(into.is_type_record()), is_type_record));
    OZ (args.push_back(is_type_record));

    OZ (get_helper().create_call(ObString("spi_cursor_fetch"),
                                 get_spi_service().spi_cursor_fetch_,
                                 args, ret_err));
  }
  return ret;
}

int ObPLCodeGenerator::generate_close(const ObPLStmt &s,
                                      const uint64_t &package_id,
                                      const uint64_t &routine_id,
                                      const int64_t &cursor_index,
                                      bool ignore,
                                      bool exception)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(get_helper().set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(set_debug_location(s))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else {
    ObLLVMValue ret_err;
    ObSEArray<ObLLVMValue, 2> args;
    ObLLVMValue cursor_index_value, ignore_value;
    ObLLVMValue package_id_value;
    ObLLVMValue routine_id_value;
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (get_helper().get_int64(package_id, package_id_value));
    OZ (args.push_back(package_id_value));
    OZ (get_helper().get_int64(routine_id, routine_id_value));
    OZ (args.push_back(routine_id_value));
    OZ (get_helper().get_int64(cursor_index, cursor_index_value));
    OZ (args.push_back(cursor_index_value));
    OZ (get_helper().get_int8(ignore, ignore_value));
    OZ (args.push_back(ignore_value));
    OZ (get_helper().create_call(ObString("spi_cursor_close"), get_spi_service().spi_cursor_close_, args, ret_err));
    if (OB_SUCC(ret) && exception) {
      OZ (check_success(ret_err,
                        s.get_stmt_id(),
                        s.get_block()->in_notfound(),
                        s.get_block()->in_warning()));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_check_not_null(const ObPLStmt &s,
                                               bool is_not_null,
                                               ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  if (is_not_null) {
    ObLLVMBasicBlock illegal_block, legal_block;
    ObLLVMBasicBlock check_composite, do_check_composite;
    ObLLVMValue p_type_value, type_value, is_null, is_extend;
    ObLLVMValue result, ret_err;
    ObSEArray<ObLLVMValue, 1> args;

    OZ (get_helper().create_block(ObString("illegal_block"), get_func(), illegal_block));
    OZ (get_helper().create_block(ObString("legal_block"), get_func(), legal_block));

    OZ (get_helper().create_block(ObString("check_composite"), get_func(), check_composite));
    OZ (get_helper().create_block(ObString("do_check_composite"), get_func(), do_check_composite));

    OZ (extract_type_ptr_from_objparam(p_result_obj, p_type_value));
    OZ (get_helper().create_load(ObString("load_type"), p_type_value, type_value));
    OZ (get_helper().create_icmp_eq(type_value, ObNullType, is_null));
    OZ (get_helper().create_cond_br(is_null, illegal_block, check_composite));

    OZ (set_current(illegal_block));
    OZ (get_helper().get_int32(OB_ERR_NUMERIC_OR_VALUE_ERROR, result));
    OZ (check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
    OZ (get_helper().create_br(legal_block));

    OZ (set_current(check_composite));
    OZ (get_helper().create_icmp_eq(type_value, ObExtendType, is_extend));
    OZ (get_helper().create_cond_br(is_extend, do_check_composite, legal_block));

    OZ (set_current(do_check_composite));
    OZ (args.push_back(p_result_obj));
    OZ (get_helper().create_call(ObString("spi_check_composite_not_null"),
                                 get_spi_service().spi_check_composite_not_null_,
                                 args,
                                 ret_err));
    OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
    OZ (get_helper().create_br(legal_block));

    OZ (set_current(legal_block));
  }
  return ret;
}

int ObPLCodeGenerator::generate_normal_next_and_check(const ObPLForLoopStmt &s,
                                                      ObLLVMValue &p_index_obj,
                                                      ObLLVMValue &p_index_value,
                                                      ObLLVMValue &index_obj,
                                                      ObLLVMValue &index_value,
                                                      ObLLVMValue &dest_datum,
                                                      ObLLVMValue &lower_value,
                                                      ObLLVMValue &upper_value,
                                                      ObLLVMValue &is_true)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(get_current().get_v()));
  // 对INDEX +1/-1 并将结果存储进param_store
  if (!s.get_reverse()) {
    OZ (extract_value_ptr_from_obj(
      p_index_obj, s.get_lower_expr()->get_data_type(), p_index_value));
    OZ (get_helper().create_load(ObString("load_index_value"), p_index_value, index_value));
    OZ (get_helper().create_inc(index_value, index_value));
    OZ (get_helper().create_store(index_value, p_index_value));
    OZ (cast_to_int64(p_index_value));
    OZ (get_helper().create_load(ObString("load_index_obj"), p_index_obj, index_obj));
    OZ (get_helper().create_store(index_obj, dest_datum));
    OZ (get_helper().create_icmp(index_value, upper_value, ObLLVMHelper::ICMP_SLE, is_true));
  } else {
    OZ (extract_value_ptr_from_obj(
      p_index_obj, s.get_upper_expr()->get_data_type(), p_index_value));
    OZ (get_helper().create_load(ObString("load_index_value"), p_index_value, index_value));
    OZ (get_helper().create_dec(index_value, index_value));
    OZ (get_helper().create_store(index_value, p_index_value));
    OZ (cast_to_int64(p_index_value));
    OZ (get_helper().create_load(ObString("load_index_obj"), p_index_obj, index_obj));
    OZ (get_helper().create_store(index_obj, dest_datum));
    OZ (get_helper().create_icmp(index_value, lower_value, ObLLVMHelper::ICMP_SGE, is_true));
  }
  return ret;
}

int ObPLCodeGenerator::generate_expr_next_and_check(const ObPLForLoopStmt &s,
                                                    ObLLVMValue &p_index_obj,
                                                    ObLLVMValue &p_index_value,
                                                    ObLLVMValue &index_obj,
                                                    ObLLVMValue &index_value,
                                                    ObLLVMValue &dest_datum,
                                                    ObLLVMValue &upper_value,
                                                    ObLLVMValue &is_true)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_type_value, type_value, bool_value, p_is_continue_value;
  ObLLVMBasicBlock normal_block, not_null_block, continue_block, not_continue_block;

  CK (OB_NOT_NULL(s.get_next_expr()));
  OZ (get_helper().create_block(ObString("normal_block"), get_func(), normal_block));
  OZ (get_helper().create_block(ObString("not_null_block"), get_func(), not_null_block));
  OZ (get_helper().create_block(ObString("continue_block"), get_func(), continue_block));
  OZ (get_helper().create_block(ObString("not_continue_block"), get_func(), not_continue_block));
  // 初始化一个变量用于存储比较的结果
  OZ (get_helper().create_ialloca(
    ObString("p_is_continue_value"), ObTinyIntType, TRUE, p_is_continue_value));
  if (s.is_values_bound()) {
    OX (p_index_obj = get_vars().at(s.get_ident() + USER_ARG_OFFSET));
    OZ (get_helper().create_load(ObString("load_index_obj"), p_index_obj, index_obj));
    OZ (get_helper().create_store(index_obj, dest_datum));
  }
  // 计算NextValue
  OZ (generate_expr(s.get_next(), s, s.get_ident(), p_index_obj));
  if (s.is_values_bound()) {
    ObLLVMValue p_tmp_obj;
    OX (p_tmp_obj = get_vars().at(s.get_ident() + USER_ARG_OFFSET));
    OZ (extract_obobj_from_objparam(p_index_obj, index_obj));
    OZ (get_helper().create_store(index_obj, p_tmp_obj));
  }
  // 判断NextValue是否为Null, Null说明已经迭代到结尾
  OZ (extract_type_ptr_from_objparam(p_index_obj, p_type_value));
  OZ (get_helper().create_load(ObString("load_type"), p_type_value, type_value));
  OZ (get_helper().create_icmp_eq(type_value, ObNullType, bool_value));
  OZ (get_helper().create_cond_br(bool_value, not_continue_block, not_null_block));
  // 不为Null还需要跟UpperValue做比较, 如果已经大于UpperValue, 也说明迭代到结尾了
  OZ (set_current(not_null_block));
  OZ (extract_value_ptr_from_objparam(
    p_index_obj, s.get_next_expr()->get_data_type(), p_index_value));
  OZ (get_helper().create_load(ObString("load_index_value"), p_index_value, index_value));
  OZ (get_helper().create_icmp(index_value, upper_value, ObLLVMHelper::ICMP_SLE, bool_value));
  OZ (get_helper().create_cond_br(bool_value, continue_block, not_continue_block));
  // 设置is_true的值, 因为is_true不是指针, 因此在这个函数中仅能且必须被设置一次
  OZ (set_current(continue_block));
  OZ (get_helper().create_istore(TRUE, p_is_continue_value));
  OZ (get_helper().create_br(normal_block));
  OZ (set_current(not_continue_block));
  OZ (get_helper().create_istore(FALSE, p_is_continue_value));
  OZ (get_helper().create_br(normal_block));
  OZ (set_current(normal_block));
  OZ (get_helper().create_load(ObString("load_null_value"), p_is_continue_value, bool_value));
  OZ (get_helper().create_icmp_eq(bool_value, TRUE, is_true));
  // 如果是values of子句, 需要将paramstore中index的位置赋值为数组的实际值而不是下标
  if (s.is_values_bound()) {
    ObLLVMBasicBlock do_nothing_block, value_block;
    ObLLVMValue p_value;
    OZ (get_helper().create_block(
      ObString("do_nothing_block"), get_func(), do_nothing_block));
    OZ (get_helper().create_block(
      ObString("value_block"), get_func(), value_block));
    OZ (get_helper().create_cond_br(is_true, value_block, do_nothing_block));

    OZ (set_current(value_block));
    OZ (generate_expr(s.get_value(), s, s.get_ident(), p_value));
    OZ (get_helper().create_br(do_nothing_block));
    OZ (set_current(do_nothing_block));
  }
  return ret;
}

int ObPLCodeGenerator::generate_next_and_check(const ObPLForLoopStmt &s,
                                               ObLLVMValue &p_index_obj,
                                               ObLLVMValue &p_index_value,
                                               ObLLVMValue &index_obj,
                                               ObLLVMValue &index_value,
                                               ObLLVMValue &dest_datum,
                                               ObLLVMValue &lower_value,
                                               ObLLVMValue &upper_value,
                                               ObLLVMValue &is_true)
{
  int ret = OB_SUCCESS;
  if (s.is_normal_bound()) {
    OZ (generate_normal_next_and_check(s,
                                       p_index_obj,
                                       p_index_value,
                                       index_obj,
                                       index_value,
                                       dest_datum,
                                       lower_value,
                                       upper_value,
                                       is_true));
  } else {
    OZ (generate_expr_next_and_check(s,
                                     p_index_obj,
                                     p_index_value,
                                     index_obj,
                                     index_value,
                                     dest_datum,
                                     upper_value,
                                     is_true));
  }
  return ret;
}

int ObPLCodeGenerator::generate_indices_with_between_bound(const ObPLForLoopStmt &s,
                                                           ObLLVMValue &p_lower_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_exist_obj, exist_obj, exist_value, is_exist;
  ObLLVMBasicBlock exist_block, not_exist_block;
  CK (get_current().get_v());
  OZ (get_helper().create_block(ObString("exist_block"),
                                get_func(),
                                exist_block));
  OZ (get_helper().create_block(ObString("not_exist_block"),
                                get_func(),
                                not_exist_block));
  OZ (generate_expr(s.get_exists(), s, OB_INVALID_INDEX, p_exist_obj));
  OZ (extract_value_from_objparam(
    p_exist_obj, s.get_exists_expr()->get_data_type(), exist_value));
  OZ (get_helper().create_icmp_eq(exist_value, TRUE, is_exist));
  OZ (get_helper().create_cond_br(is_exist, exist_block, not_exist_block));
  OZ (set_current(not_exist_block));
  OZ (generate_expr(s.get_next(), s, s.get_ident(), p_exist_obj));
  OZ (get_helper().create_load(ObString("load_exist_lower_obj"), p_exist_obj, exist_obj));
  OZ (get_helper().create_store(exist_obj, p_lower_obj));
  OZ (get_helper().create_br(exist_block));
  OZ (set_current(exist_block));
  return ret;
}

int ObPLCodeGenerator::generate_bound_and_check(const ObPLForLoopStmt &s,
                                                bool is_forall,
                                                ObLLVMValue &lower_value, // lower value int64_t
                                                ObLLVMValue &upper_value, // upper value int64_t
                                                ObLLVMValue &lower_obj,   // lower obj ObObject
                                                ObLLVMValue &upper_obj,   // upper obj ObObject
                                                ObLLVMBasicBlock &illegal_range_block) // ret_err int32_t
{
  int ret = OB_SUCCESS;
  if (get_current().get_v()) {
    // 控制流已断, 后面的语句不再处理
  } else {
    OZ (get_helper().set_insert_point(get_current()));
    OZ (set_debug_location(s));
  }
  if (OB_SUCC(ret)) {
    ObLLVMBasicBlock check_null_block, illegal_null_block, body_block, check_up_low_block;
    ObLLVMValue p_lower_obj, p_upper_obj;
    ObSEArray<ObLLVMValue, 5> args;
    ObLLVMValue result;

    CK (OB_NOT_NULL(s.get_body()));
    CK (OB_NOT_NULL(s.get_lower_expr()));
    CK (OB_NOT_NULL(s.get_upper_expr()));

    OZ (get_helper().create_block(ObString("check_null_block"),
                                  get_func(),
                                  check_null_block));
    OZ (get_helper().create_block(ObString("body_block"),
                                  get_func(),
                                  body_block));
    OZ (get_helper().create_block(ObString("check_up_low_block"),
                                  get_func(),
                                  check_up_low_block));
    OZ (get_helper().create_block(ObString("illegal_null_block"),
                                  get_func(),
                                  illegal_null_block));

    // 计算lower, upper
    OZ (generate_expr(s.get_lower(), s, s.get_ident(), p_lower_obj));
    OZ (generate_expr(s.get_upper(), s, OB_INVALID_INDEX, p_upper_obj));

    if (s.is_indices_with_between_bound()) {
      OZ (generate_indices_with_between_bound(s, p_lower_obj));
    }

    if (OB_SUCC(ret)) {
      // 在FORALL中上界或者下界为NULL, 不报错
      // 在FORLOOP中上界或者下界为NULL, 报错
      // 如果上界下界均不为NULL, 不报错, 范围不符合条件什么都不做
      ObLLVMValue p_type_value, type_value, is_null, is_legal;
      OZ (get_helper().get_int32(OB_SUCCESS, result));

      OZ (extract_type_ptr_from_objparam(p_lower_obj, p_type_value));
      OZ (get_helper().create_load(ObString("load_type"), p_type_value, type_value));
      OZ (get_helper().create_icmp_eq(type_value, ObNullType, is_null));
      OZ (get_helper().create_cond_br(is_null, illegal_null_block, check_null_block));

      OZ (get_helper().set_insert_point(check_null_block));
      OZ (extract_type_ptr_from_objparam(p_upper_obj, p_type_value));
      OZ (get_helper().create_load(ObString("load_type"), p_type_value, type_value));
      OZ (get_helper().create_icmp_eq(type_value, ObNullType, is_null));
      OZ (get_helper().create_cond_br(is_null, illegal_null_block, check_up_low_block));

      OZ (set_current(illegal_null_block));
      OZ (get_helper().get_int32(OB_ERR_NULL_VALUE, result));
      if (!is_forall) {
        OZ (check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
      }
      OZ (get_helper().create_br(illegal_range_block));

      OZ (get_helper().set_insert_point(check_up_low_block));
      OZ (extract_value_from_objparam(p_lower_obj, s.get_lower_expr()->get_data_type(), lower_value));
      OZ (extract_datum_from_objparam(p_lower_obj, s.get_lower_expr()->get_data_type(), lower_obj));
      OZ (extract_value_from_objparam(p_upper_obj, s.get_upper_expr()->get_data_type(), upper_value));
      OZ (extract_datum_from_objparam(p_upper_obj, s.get_upper_expr()->get_data_type(), upper_obj));
      /*!
       * Without REVERSE, the value of index starts at lower_bound and increases by one with
       * each iteration of the loop until it reaches upper_bound. If lower_bound is greater than
       * upper_bound, then the statements never run.
       * With REVERSE, the value of index starts at upper_bound and decreases by one with each
       * iteration of the loop until it reaches lower_bound. If upper_bound is less than lower_
       * bound, then the statements never run.
       */
      OZ (get_helper().create_icmp(lower_value, upper_value, ObLLVMHelper::ICMP_SLE, is_legal));
      OZ (get_helper().create_cond_br(is_legal, body_block, illegal_range_block));

      OZ (set_current(body_block));
    }

    if (OB_SUCC(ret) && is_forall) {
      const ObPLForAllStmt *forall_stmt = static_cast<const ObPLForAllStmt*>(&s);
      CK (OB_NOT_NULL(forall_stmt));
      if (OB_SUCC(ret)) {
        ObLLVMValue src_idx_value, dst_idx_value, ret_err;
        ObSEArray<ObLLVMValue, 5> args;
        const hash::ObHashMap<int64_t, int64_t> &sub_map = forall_stmt->get_tab_to_subtab_map();
        for (hash::ObHashMap<int64_t, int64_t>::const_iterator it = sub_map.begin();
            OB_SUCC(ret) && it != sub_map.end(); ++it) {
          args.reset();
          int64_t src_idx = (*it).first;
          int64_t dst_idx = (*it).second;
          OZ (get_helper().get_int64(src_idx, src_idx_value));
          OZ (get_helper().get_int64(dst_idx, dst_idx_value));
          OZ (args.push_back(get_vars().at(CTX_IDX)));
          OZ (args.push_back(src_idx_value));
          OZ (args.push_back(dst_idx_value));
          OZ (args.push_back(lower_value));
          OZ (args.push_back(upper_value));
          OZ (get_helper().create_call(ObString("spi_sub_nestedtable"),
                                       get_spi_service().spi_sub_nestedtable_,
                                       args,
                                       ret_err));
          OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_sql(const ObPLSqlStmt &s, ObLLVMValue &ret_err)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_current().get_v())) {
    // 控制流已断，后面的语句不再处理
  } else {
    OZ (get_helper().set_insert_point(get_current()));
    OZ (set_debug_location(s));
  }
  if (OB_FAIL(ret)) {
  } else if (stmt::T_END_TRANS == s.get_stmt_type()) {
    bool is_rollback = false;
    ObSEArray<ObLLVMValue, 2> args;
    ObLLVMValue is_rollback_value, sql, length;
    // only allow stmt here: COMMIT; COMMIT COMMENT 'X'; ROLLBACK;
    // COMMIT WORK; ROLLBACK WORK;
    CK (((s.get_sql().length() >= 5 && 0 == STRNCASECMP(s.get_sql().ptr(), "commit", 5))
         || (s.get_sql().length() >= 8 && 0 == STRNCASECMP(s.get_sql().ptr(), "rollback", 8))));
    OX (is_rollback
      = (s.get_sql().length() >= 8 && 0 == STRNCASECMP(s.get_sql().ptr(), "rollback", 8)));
    OZ (generate_string(s.get_sql(), sql, length));
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (args.push_back(sql));
    OZ (get_helper().get_int8(is_rollback, is_rollback_value));
    OZ (args.push_back(is_rollback_value));
    OZ (get_helper().create_call(ObString("spi_end_trans"), get_spi_service().spi_end_trans_, args, ret_err));
    LOG_DEBUG("explicit end trans in pl", K(ret), K(s.get_sql()));
  } else {
    ObSEArray<ObLLVMValue, 16> args;
    ObLLVMValue str;
    ObLLVMValue len;
    ObLLVMValue ps_sql;
    ObLLVMValue type;
    ObLLVMValue for_update;
    ObLLVMValue hidden_rowid;
    ObLLVMValue params;
    ObLLVMValue count, is_type_record;
    ObLLVMValue skip_locked;
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (generate_sql(s, str, len, ps_sql, type, for_update, hidden_rowid, params, count, skip_locked));
    if (OB_SUCC(ret)) {
      if (s.get_params().empty()) {
        OZ (args.push_back(str));
        OZ (args.push_back(type));
      } else {
        OZ (args.push_back(ps_sql));
        OZ (args.push_back(type));
        OZ (args.push_back(params));
        OZ (args.push_back(count));
      }
    }
    if (OB_SUCC(ret)) {
      ObLLVMValue into_array_value;
      ObLLVMValue into_count_value;
      ObLLVMValue type_array_value;
      ObLLVMValue type_count_value;
      ObLLVMValue exprs_not_null_array_value;
      ObLLVMValue pl_integer_range_array_value;
      ObLLVMValue is_bulk;
      OZ (generate_into(s, into_array_value, into_count_value,
                           type_array_value, type_count_value,
                           exprs_not_null_array_value,
                           pl_integer_range_array_value,
                           is_bulk));
      OZ (args.push_back(into_array_value));
      OZ (args.push_back(into_count_value));
      OZ (args.push_back(type_array_value));
      OZ (args.push_back(type_count_value));
      OZ (args.push_back(exprs_not_null_array_value));
      OZ (args.push_back(pl_integer_range_array_value));
      OZ (args.push_back(is_bulk));
    }
    if (OB_SUCC(ret)) {
      if (s.get_params().empty()) {
        OZ (get_helper().get_int8(static_cast<int64_t>(s.is_type_record()), is_type_record));
        OZ (args.push_back(is_type_record));
        OZ (args.push_back(for_update));
        OZ (get_helper().create_call(ObString("spi_query_into_expr_idx"), get_spi_service().spi_query_into_expr_idx_, args, ret_err));
      } else { //有外部变量，走prepare/execute接口
        ObLLVMValue is_forall;
        OZ (get_helper().get_int8(static_cast<int64_t>(s.is_forall_sql()), is_forall));
        OZ (args.push_back(is_forall));
        OZ (get_helper().get_int8(static_cast<int64_t>(s.is_type_record()), is_type_record));
        OZ (args.push_back(is_type_record));
        OZ (args.push_back(for_update));
        OZ (get_helper().create_call(ObString("spi_execute_with_expr_idx"), get_spi_service().spi_execute_with_expr_idx_, args, ret_err));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_after_sql(const ObPLSqlStmt &s, ObLLVMValue &ret_err)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_current().get_v())) {
    // 控制流已断, 后面的语句不再处理
  } else {
    OZ (check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
    OZ (generate_into_restore(s.get_into(), s.get_exprs(), s.get_symbol_table()));
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      ObLLVMValue is_not_found;
      ObLLVMBasicBlock normal_end;
      ObLLVMBasicBlock reset_ret;
      ObLLVMValue ret_success;
      OZ (get_helper().create_block(ObString("sql_end"), get_func(), normal_end));
      OZ (get_helper().create_block(ObString("sql_check_success"), get_func(), reset_ret));
      OZ (get_helper().create_icmp_eq(ret_err, OB_READ_NOTHING, is_not_found));
      OZ (get_helper().create_cond_br(is_not_found, reset_ret, normal_end));
      OZ (get_helper().set_insert_point(reset_ret));
      OZ (get_helper().get_int32(OB_SUCCESS, ret_success));
      OZ (get_helper().create_store(ret_success, get_vars().at(RET_IDX)));
      OZ (get_helper().create_br(normal_end));
      OZ (set_current(normal_end));
    }
  }
  return ret;
}

int ObPLCodeGenerator::get_llvm_type(const ObPLDataType &pl_type, ObLLVMType &ir_type)
{
  int ret = OB_SUCCESS;
  ir_type = NULL;
  if (pl_type.is_obj_type()) {
    if (OB_FAIL(ObPLDataType::get_llvm_type(pl_type.get_obj_type(), get_helper(), get_adt_service(), ir_type))) {
      LOG_WARN("failed to get datum type", K(ret));
    }
  } else if (pl_type.is_user_type()) {
    uint64_t user_type_id = pl_type.get_user_type_id();
    if (OB_FAIL(get_user_type_map().get_refactored(user_type_id, ir_type))) {
      LOG_WARN("get user type map failed", K(ret), K(user_type_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl type is invalid", K(pl_type.get_type()));
  }
  return ret;
}

int ObPLCodeGenerator::get_datum_type(const ObPLDataType &pl_type, ObLLVMType &ir_type)
{
  int ret = OB_SUCCESS;
  ir_type = NULL;
//  if (pl_type.is_obj_type()) {
    if (OB_FAIL(ObPLDataType::get_datum_type(pl_type.get_obj_type(), helper_, adt_service_, ir_type))) { //集合类型里的基础数据类型统一用int64存储
      LOG_WARN("failed to get datum type", K(ret));
    }
//  } else if (pl_type.is_user_type()) {
//    uint64_t user_type_id = pl_type.get_user_type_id();
//    if (OB_FAIL(user_type_map_.get_refactored(user_type_id, ir_type))) {
//      LOG_WARN("get user type map failed", K(ret), K(user_type_id));
//    }
//  } else {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("pl type is invalid", K(pl_type.get_type()));
//  }
  return ret;
}

int ObPLCodeGenerator::generate_string(const ObString &string, ObLLVMValue &str, ObLLVMValue &len)
{
  int ret = OB_SUCCESS;
  if (string.empty()) {
    if (OB_FAIL(generate_empty_string(str, len))) {
      LOG_WARN("failed to generate_empty_string", K(ret));
    }
  } else {
    ObLLVMValue llvm_string;
    ObLLVMType llvm_string_type;
    ObLLVMValue p_llvm_string;
    ObLLVMType llvm_type;
    OZ (helper_.get_llvm_type(ObCharType, llvm_type));
    OZ (helper_.get_string(string, llvm_string));
    OZ (llvm_string.get_type(llvm_string_type));
    OZ (helper_.create_alloca(ObString("string"), llvm_string_type, p_llvm_string));
    OZ (helper_.create_store(llvm_string, p_llvm_string));
    OZ (helper_.create_pointer_cast(ObString("string_to_char"), p_llvm_string, llvm_type, str));

    OZ (helper_.get_int64(string.length(), len));
  }
  return ret;
}

int ObPLCodeGenerator::generate_global_string(const ObString &string, ObLLVMValue &str, ObLLVMValue &len)
{
  int ret = OB_SUCCESS;
  if (string.empty()) {
    OZ (generate_empty_string(str, len));
  } else {
    ObLLVMType llvm_type;
    ObLLVMValue llvm_string;
    OZ (helper_.get_llvm_type(ObCharType, llvm_type));
    OZ (helper_.get_string(string, llvm_string));
    OZ (helper_.get_global_string(llvm_string, llvm_string));
    OZ (helper_.create_pointer_cast(ObString("string_to_char"), llvm_string, llvm_type, str));
    OZ (helper_.get_int64(string.length(), len));
  }
  return ret;
}

int ObPLCodeGenerator::generate_empty_string(ObLLVMValue &str, ObLLVMValue &len)
{
  int ret = OB_SUCCESS;
  ObLLVMType llvm_type;
  if (OB_FAIL(helper_.get_llvm_type(ObCharType, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(ObLLVMHelper::get_null_const(llvm_type, str))) {
    LOG_WARN("failed to get_null_const", K(ret));
  } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(ObLLVMHelper::get_null_const(llvm_type, len))) {
    LOG_WARN("failed to get_null_const", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_null(ObObjType type, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  ObLLVMType llvm_type;
  if (OB_FAIL(helper_.get_llvm_type(type, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(ObLLVMHelper::get_null_const(llvm_type, value))) {
    LOG_WARN("failed to get_null_const", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_null_pointer(ObObjType type, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  ObLLVMType llvm_type;
  ObLLVMType llvm_pointer_type;
  if (OB_FAIL(helper_.get_llvm_type(type, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(llvm_type.get_pointer_to(llvm_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(ObLLVMHelper::get_null_const(llvm_pointer_type, value))) {
    LOG_WARN("failed to get_null_const", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_int64_array(const ObIArray<int64_t> &array, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> uint64_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
    uint64_t v = array.at(i);
    OZ (uint64_array.push_back(v));
  }
  OZ (generate_uint64_array(uint64_array, result));
  return ret;
}

int ObPLCodeGenerator::generate_uint64_array(const ObIArray<uint64_t> &array, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  if (array.empty()) {
    OZ (generate_null_pointer(ObIntType, result));
  } else {
    ObLLVMValue llvm_array;
    if (OB_FAIL(helper_.get_uint64_array(array, llvm_array))) {
      LOG_WARN("failed to get_uint64_array", K(ret));
    } else if (OB_FAIL(generate_array(llvm_array, result))) {
      LOG_WARN("failed to generate_array", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_array(const ObLLVMValue array, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  ObLLVMType array_type;
  ObLLVMValue p_array;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(0))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(array.get_type(array_type))) {
    LOG_WARN("failed to get_type", K(ret));
  } else if (OB_FAIL(helper_.create_alloca(ObString("array"), array_type, p_array))) {
    LOG_WARN("failed to create_alloca", K(ret));
  } else if (OB_FAIL(helper_.create_store(array, p_array))) {
    LOG_WARN("failed to create_store", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_first_addr_from_array"), p_array, indices, result))) {
    LOG_WARN("failed to create_gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

//just for debug
int ObPLCodeGenerator::generate_debug(const ObString &name, int64_t value)
{
  int ret = OB_SUCCESS;
  ObLLVMValue int_value;
  OZ (helper_.get_int64(value, int_value));
  OZ (generate_debug(name, int_value));
  return ret;
}
int ObPLCodeGenerator::generate_debug(const ObString &name, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 4> args;
  ObLLVMValue str;
  ObLLVMValue len;
  OZ (generate_string(name, str, len));
  OZ (args.push_back(str));
  OZ (args.push_back(len));
  OZ (args.push_back(value));
  if (12 == value.get_type_id()) { //12 is IntegerTyID
    switch (value.get_type().get_width()) {
      case 8: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int8_, args));
      }
      break;
      case 32: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int32_, args));
      }
      break;
      case 64: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int64_, args));
      }
      break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Unexpected integer to debug", K(value.get_type().get_width()), K(ret));
      }
      break;
    }
  } else if (14 == value.get_type_id() && 12 == value.get_type().get_child(0).get_id()) { //14 is PointerTyID
    switch (value.get_type().get_child(0).get_width()) {
      case 8: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int8ptr_, args));
      }
      break;
      case 32: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int32ptr_, args));
      }
      break;
      case 64: {
        OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int64ptr_, args));
      }
      break;
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Unexpected integer pointer to debug", K(value.get_type().get_width()), K(ret));
      }
      break;
    }
  } else if (14 == value.get_type_id() && 15 == value.get_type().get_child(0).get_id()) { //14 is PointerTyID, 15 is StructTyID
    ObLLVMType obj_type;
    ObLLVMType objparam_type;
    OZ (adt_service_.get_obj(obj_type));
    OZ (adt_service_.get_objparam(objparam_type));
    if (OB_SUCC(ret)) {
      bool is_same = false;
      OZ (value.get_type().get_child(0).same_as(obj_type, is_same));
      if (OB_SUCC(ret)) {
        if (is_same) {
          OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_obj_, args));
        } else {
          OZ (value.get_type().get_child(0).same_as(objparam_type, is_same));
          if (OB_SUCC(ret)) {
            if (is_same) {
              OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_objparam_, args));
            } else {
              ObLLVMType int_type;
              ObLLVMValue int_value;
              OZ (helper_.get_llvm_type(ObIntType, int_type));
              OZ (helper_.create_ptr_to_int(ObString("object_to_int64"),
                                            value,
                                            int_type,
                                            int_value));
              OX (args.at(2) = int_value);
              OZ (helper_.create_call(ObString("debug"), get_eh_service().eh_debug_int64_, args));
            }
          }
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected value to debug", K(value.get_type_id())/*, K(value.get_type().get_child(0).get_id()), K(ret)*/);
  }
  return ret;
}

#define STORE_META(first, second, name, get_func) \
  do { \
    if (OB_SUCC(ret)) { \
      indices.reset(); \
      if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(first)) || OB_FAIL(indices.push_back(second))) { \
        LOG_WARN("push_back error", K(ret)); \
      } else if (OB_FAIL(helper_.create_gep(ObString(name), p_obj, indices, dest))) { \
        LOG_WARN("failed to create gep", K(ret)); \
      } else if (OB_FAIL(helper_.get_int8(meta.get_func(), src))) { \
        LOG_WARN("failed to get_int64", K(meta.get_type()), K(meta), K(ret)); \
      } else if (OB_FAIL(helper_.create_store(src, dest))) { \
        LOG_WARN("failed to create store", K(ret)); \
      } else { /*do nothing*/ } \
    } \
  } while (0)

#define STORE_ELEMENT(idx, name, get_func, length) \
  do { \
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(helper_.create_gep(ObString(name), p_obj, idx, dest))) { \
        LOG_WARN("failed to create gep", K(ret)); \
      } else if (OB_FAIL(helper_.get_int##length(object.get_func(), src))) { \
        LOG_WARN("failed to get_int64", K(object), K(ret)); \
      } else if (OB_FAIL(helper_.create_store(src, dest))) { \
        LOG_WARN("failed to create store", K(ret)); \
      } else { /*do nothing*/ } \
    } \
  } while (0)

int ObPLCodeGenerator::generate_obj(const ObObj &object, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMType obj_type;
  if (OB_FAIL(adt_service_.get_obj(obj_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_alloca(ObString("ObObj"), obj_type, result))) {
    LOG_WARN("failed to create_alloca", K(ret));
  } else if (OB_FAIL(store_obj(object, result))) {
    LOG_WARN("failed to store obj", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::store_obj(const ObObj &object, ObLLVMValue &p_obj)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &meta = object.get_meta();
  ObSEArray<int64_t, 3> indices;
  ObLLVMValue src;
  ObLLVMValue dest;
  //obj type
  STORE_META(0, 0, "extract_obj_type", get_type);
  //collation level
  STORE_META(0, 1, "extract_obj_collation_level", get_collation_level);
  //collation type
  STORE_META(0, 2, "extract_obj_collation_type", get_collation_type);
  //scale
  STORE_META(0, 3, "extract_obj_scale", get_scale);
  //length
  STORE_ELEMENT(1, "extract_obj_length", get_val_len, 32);
  //value
  STORE_ELEMENT(2, "extract_obj_value", get_int, 64);

  return ret;
}

int ObPLCodeGenerator::store_objparam(const ObObjParam &object, ObLLVMValue &p_objparam)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_obj;
  if (OB_FAIL(helper_.create_gep(ObString("extract_obj_pointer"), p_objparam, 0, p_obj))) {
    LOG_WARN("failed to create gep", K(ret));
  } else if (OB_FAIL(store_obj(object, p_obj))) {
    LOG_WARN("failed to store obj", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::store_data_type(const ObDataType &object, jit::ObLLVMValue &p_obj)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &meta = object.get_meta_type();
  ObSEArray<int64_t, 2> indices;
  ObLLVMValue src;
  ObLLVMValue dest;
  //obj type
  STORE_META(0, 0, "extract_obj_type", get_type);
  //collation level
  STORE_META(0, 1, "extract_obj_collation_level", get_collation_level);
  //collation type
  STORE_META(0, 2, "extract_obj_collation_type", get_collation_type);
  //scale
  STORE_META(0, 3, "extract_obj_scale", get_scale);
  //accuracy
  STORE_ELEMENT(1, "extract_accuracy", get_accuracy_value, 64);
  //charset
  STORE_ELEMENT(2, "extract_charset", get_charset_type, 32);
  //is_binary_collation
  STORE_ELEMENT(3, "extract_binary_collation", is_binary_collation, 8);
  //is_zero_fill
  STORE_ELEMENT(4, "extract_zero_fill", is_zero_fill, 8);
  return ret;
}

int ObPLCodeGenerator::store_elem_desc(const ObElemDesc &object, jit::ObLLVMValue &p_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMValue src;
  ObLLVMValue dest;
  OZ (store_data_type(object, p_obj));
  //pl type
  STORE_ELEMENT(5, "extract_pl_type", get_pl_type, 32);
  //not_null
  STORE_ELEMENT(6, "extract_not_null", is_not_null, 8);
  //field_count
  STORE_ELEMENT(7, "extract_field_count", get_field_count, 32);
  return ret;
}

#define GENERATE_OBJECT(name, class) \
int ObPLCodeGenerator::generate_##name(const class &object, jit::ObLLVMValue &result) \
{ \
  int ret = OB_SUCCESS; \
  ObLLVMType object_type; \
  if (OB_FAIL(adt_service_.get_##name(object_type))) { \
    LOG_WARN("failed to get_llvm_type", K(ret)); \
  } else if (OB_FAIL(helper_.create_alloca(ObString("NewObject"), object_type, result))) { \
    LOG_WARN("failed to create_alloca", K(ret)); \
  } else if (OB_FAIL(store_##name(object, result))) { \
    LOG_WARN("failed to store", K(ret)); \
  } else { /*do nothing*/ } \
  return ret; \
}

GENERATE_OBJECT(data_type, ObDataType)
GENERATE_OBJECT(elem_desc, ObElemDesc)

#define INIT_OBJPARAM_ELEMENT(name, length, value) \
  do { \
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(extract_##name##_ptr_from_objparam(result, dest))) { \
        LOG_WARN("failed to extract ptr from objparam", K(ret)); \
      } else if (OB_FAIL(helper_.get_int##length(value, src))) { \
        LOG_WARN("failed to get_int8", K(ret)); \
      } else if (OB_FAIL(helper_.create_store(src, dest))) { \
        LOG_WARN("failed to create_store", K(ret)); \
      } else { /*do nothing*/ } \
    } \
  } while (0)

int ObPLCodeGenerator::generate_new_objparam(ObLLVMValue &result, int64_t udt_id)
{
  int ret = OB_SUCCESS;
  ObLLVMType objparam_type;
  ObLLVMValue const_value;
  if (OB_FAIL(adt_service_.get_objparam(objparam_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_alloca(ObString("ObObjParam"), objparam_type, result))) {
    LOG_WARN("failed to create_alloca", K(ret));
  } else if (OB_FAIL(ObLLVMHelper::get_null_const(objparam_type, const_value))) {
    LOG_WARN("failed to get_null_const", K(ret));
  } else if (OB_FAIL(helper_.create_store(const_value, result))) {
    LOG_WARN("failed to create_store", K(ret));
  } else {
    ObLLVMValue src;
    ObLLVMValue dest;
    //init cs level
    INIT_OBJPARAM_ELEMENT(cslevel, 8, ObCollationLevel::CS_LEVEL_INVALID);
    //init scale
    INIT_OBJPARAM_ELEMENT(scale, 8, -1);
    //init accuracy
    INIT_OBJPARAM_ELEMENT(accuracy, 64, udt_id);
    //init flag
    INIT_OBJPARAM_ELEMENT(flag, 8, 1);
    //init raw_text_pos
    INIT_OBJPARAM_ELEMENT(raw_text_pos, 32, -1);
    //init raw_text_len
    INIT_OBJPARAM_ELEMENT(raw_text_len, 32, -1);
    //init param_meta
    INIT_OBJPARAM_ELEMENT(type, 8, 0);
    INIT_OBJPARAM_ELEMENT(cslevel, 8, 7);
    INIT_OBJPARAM_ELEMENT(cstype, 8, 0);
    INIT_OBJPARAM_ELEMENT(scale, 8, -1);
  }
  return ret;
}

int ObPLCodeGenerator::generate_set_extend(ObLLVMValue &p_obj,
                                           ObPLType type,
                                           int32_t size,
                                           int64_t ptr)
{
  int ret = OB_SUCCESS;
  ObLLVMValue var_type;
  ObLLVMValue init_value;
  ObLLVMValue extend_value;
  OZ (get_helper().get_int8(type, var_type));
  OZ (get_helper().get_int32(size, init_value));
  OZ (get_helper().get_int32(ptr, extend_value));
  OZ (generate_set_extend(p_obj, var_type, init_value, extend_value));

  return ret;
}

int ObPLCodeGenerator::generate_set_extend(ObLLVMValue &p_obj,
                                           ObLLVMValue &type,
                                           ObLLVMValue &size,
                                           ObLLVMValue &ptr)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 3> indices;
  ObLLVMValue dest;

  //init extend
  ObObj extend_obj(ObExtendType);
  extend_obj.set_int_value(0);
  OZ (store_obj(extend_obj, p_obj));

  //set extend type
  OZ (indices.push_back(0));
  OZ (indices.push_back(0));
  OZ (indices.push_back(3));
  OZ (helper_.create_gep("extend_type", p_obj, indices, dest));
  OZ (helper_.create_store(type, dest));

  //set extend size
  OZ (helper_.create_gep("extend_type", p_obj, 1, dest));
  OZ (helper_.create_store(size, dest));

  //set ptr
  OZ (helper_.create_gep("extend_type", p_obj, 2, dest));
  OZ (helper_.create_store(ptr, dest));

  return ret;
}

int ObPLCodeGenerator::generate_spi_calc(int64_t expr_idx,
                                         int64_t stmt_id,
                                         bool in_notfound,
                                         bool in_warning,
                                         int64_t result_idx,
                                         ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  const ObSqlExpression *expr = get_expr(expr_idx);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else {
    ObLLVMValue expr_idx_val;
    if (OB_FAIL(helper_.get_int64(expr_idx, expr_idx_val))) {
      LOG_WARN("failed to generate a pointer", K(ret));
    } else {
      ObSEArray<ObLLVMValue, 4> args;
      ObLLVMValue result;
      ObLLVMValue int_value;
      ObLLVMValue package_id;
      int64_t udt_id = ast_.get_expr(expr_idx)->get_result_type().get_udt_id();

      if (OB_FAIL(args.push_back(vars_.at(CTX_IDX)))) { //PL的执行环境
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(args.push_back(expr_idx_val))) { //表达式的下标
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.get_int64(result_idx, int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(args.push_back(int_value))) { //结果在ObArray里的位置
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(generate_new_objparam(p_result_obj, udt_id))) {
        LOG_WARN("failed to generate_new_objparam", K(ret));
      } else if (OB_FAIL(args.push_back(p_result_obj))) {
        LOG_WARN("push_back error", K(ret));
      } else if (OB_FAIL(helper_.create_call(ObString("calc_expr"), get_spi_service().spi_calc_expr_at_idx_, args, result))) {
        LOG_WARN("failed to create call", K(ret));
      } else if (OB_FAIL(check_success(result, stmt_id, in_notfound, in_warning))) {
        LOG_WARN("failed to check success", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_spi_package_calc(uint64_t package_id,
                                                 int64_t expr_idx,
                                                 const ObPLStmt &s,
                                                 ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 4> args;
  ObLLVMValue result;
  ObLLVMValue v_package_id;
  ObLLVMValue v_expr_idx;
  CK (OB_NOT_NULL(s.get_block()));
  OZ (args.push_back(vars_.at(CTX_IDX)));
  OZ (helper_.get_int64(package_id, v_package_id));
  OZ (helper_.get_int64(expr_idx, v_expr_idx));
  OZ (args.push_back(v_package_id));
  OZ (args.push_back(v_expr_idx));
  OZ (generate_new_objparam(p_result_obj));
  OZ (args.push_back(p_result_obj));
  OZ (helper_.create_call(ObString("calc_package_expr"), get_spi_service().spi_calc_package_expr_, args, result));
  OZ (check_success(result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
  return ret;
}

int ObPLCodeGenerator::generate_llvm_calc(int64_t expr_idx,
                                          int64_t stmt_id,
                                          bool in_notfound,
                                          bool in_warning,
                                          int64_t result_idx,
                                          ObLLVMValue &p_result_obj)
{

  /*
   * 为防止计算溢出，用int64进行计算
   */
#define GET_LLVM_VALUE(expr, value) \
  do { \
    if (OB_SUCC(ret)) { \
      int64_t int_value = static_cast<const ObConstRawExpr*>(expr)->get_value().get_unknown(); \
      if (T_QUESTIONMARK == expr->get_expr_type()) { \
        if (OB_FAIL(extract_value_from_context(vars_.at(CTX_IDX),  int_value, ObIntType, value))) { \
          LOG_WARN("failed to extract_value_from_context", K(ret)); \
        } \
      } else if (expr->is_const_raw_expr()) { \
        if (OB_FAIL(helper_.get_int64(int_value, value))) { \
          LOG_WARN("failed to get int64", K(ret)); \
        } \
      } else { \
        ret = OB_INVALID_ARGUMENT; \
        LOG_WARN("invalid expr type", K(*expr), K(ret)); \
      } \
    } \
  } while (0)

  int ret = OB_SUCCESS;
  const ObRawExpr *expr = ast_.get_expr(expr_idx);
  int64_t udt_id = expr->get_result_type().get_udt_id();
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  } else if (OB_FAIL(generate_new_objparam(p_result_obj, udt_id))) {
    LOG_WARN("failed to generate_new_objparam", K(ret));
  } else if (expr->is_const_raw_expr()) {
    const ObObj &const_value = static_cast<const ObConstRawExpr*>(expr)->get_value();
    if (!const_value.is_int32()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid simple integer", K(const_value), K(ret));
    } else {
      if (OB_FAIL(generate_const_calc(const_value.get_int32(), p_result_obj))) {
        LOG_WARN("failed to generate const calc", K(const_value), K(ret));
      }
#ifndef NDEBUG
      OZ (generate_debug(ObString("generate_llvm_calc result"), p_result_obj));
#endif
    }
  } else {
    if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child expr is NULL", K(expr->get_param_expr(0)), K(expr->get_param_expr(1)), K(ret));
    } else if (!expr->get_param_expr(0)->get_result_type().is_int32() || !expr->get_param_expr(1)->get_result_type().is_int32()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected integer type", K(*expr->get_param_expr(0)), K(*expr->get_param_expr(1)), K(ret));
    } else {
      const ObRawExpr *left_expr = expr->get_param_expr(0);
      const ObRawExpr *right_expr = expr->get_param_expr(1);
      ObLLVMValue left;
      ObLLVMValue right;

      GET_LLVM_VALUE(left_expr, left);

      GET_LLVM_VALUE(right_expr, right);

      if (OB_SUCC(ret)) {
        if (IS_COMMON_COMPARISON_OP(expr->get_expr_type())) {
          if (OB_FAIL(generate_compare_calc(left, right, expr->get_expr_type(), p_result_obj))) {
            LOG_WARN("failed to generate_new_objparam", K(ret));
          }
        } else {
          if (OB_FAIL(generate_arith_calc(left, right, expr->get_expr_type(), expr->get_result_type(), stmt_id, in_notfound, in_warning, p_result_obj))) {
            LOG_WARN("failed to generate_new_objparam", K(ret));
          }
        }
      }
    }
  }

  //存储进全局符号表和paramstore
  if (OB_SUCC(ret) && OB_INVALID_INDEX != result_idx) {
    ObLLVMValue p_objparam;
    ObLLVMValue obj;
    ObLLVMValue objparam;
    if (OB_FAIL(extract_datum_from_objparam(p_result_obj, ObInt32Type, obj))) {
      LOG_WARN("failed to extract_datum_from_objparam", K(ret));
    } else if (OB_FAIL(helper_.create_store(obj, vars_.at(result_idx + USER_ARG_OFFSET)))) {
      LOG_WARN("failed to create_store", K(ret));
    } else if (OB_FAIL(helper_.create_load("load_out_arg", p_result_obj, objparam))) {
      LOG_WARN("failed to create_load", K(ret));
    } else if (OB_FAIL(extract_objparam_from_context(vars_.at(CTX_IDX), result_idx, p_objparam))) {
      LOG_WARN("failed to extract_datum_from_objparam", K(ret));
    } else if (OB_FAIL(helper_.create_store(objparam, p_objparam))) {
      LOG_WARN("failed to create_store", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_const_calc(int32_t value, ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_value;
  ObLLVMValue calc_result;
  ObObjParam objparam;
  objparam.set_type(ObInt32Type);
  objparam.set_param_meta();
  if (OB_FAIL(store_objparam(objparam, p_result_obj))) {
    LOG_WARN("Not supported yet", K(ret));
  } else if (OB_FAIL(extract_value_ptr_from_objparam(p_result_obj, ObInt32Type, p_value))) {
    LOG_WARN("failed to extract_value_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.get_int32(value, calc_result))) {
    LOG_WARN("failed to extract_value_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.create_store(calc_result, p_value))) {
    LOG_WARN("failed to create store", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_compare_calc(ObLLVMValue &left,
                                             ObLLVMValue &right,
                                             ObItemType type,
                                             ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMHelper::CMPTYPE compare_type = ObLLVMHelper::ICMP_EQ;
  switch (type) {
  case T_OP_EQ: {
    compare_type = ObLLVMHelper::ICMP_EQ;
  }
    break;
  case T_OP_NSEQ: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("T_OP_NSEQ is not supported", K(type), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "null safe equals operator");
  }
    break;
  case T_OP_LE: {
    compare_type = ObLLVMHelper::ICMP_SLE;
  }
    break;
  case T_OP_LT: {
    compare_type = ObLLVMHelper::ICMP_SLT;
  }
    break;
  case T_OP_GE: {
    compare_type = ObLLVMHelper::ICMP_SGE;
  }
    break;
  case T_OP_GT: {
    compare_type = ObLLVMHelper::ICMP_SGT;
  }
    break;
  case T_OP_NE: {
    compare_type = ObLLVMHelper::ICMP_NE;
  }
    break;
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected expr type", K(type), K(ret));
  }
    break;
  }

  if (OB_SUCC(ret)) {
    ObLLVMValue is_true;
    ObLLVMBasicBlock true_branch;
    ObLLVMBasicBlock false_branch;
    ObLLVMBasicBlock end_branch;

#ifndef NDEBUG
    OZ (generate_debug(ObString("debug"), left));
    OZ (generate_debug(ObString("debug"), right));
#endif
    if (OB_FAIL(helper_.create_block(ObString("true_branch"), get_func(), true_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(helper_.create_block(ObString("false_branch"), get_func(), false_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(helper_.create_block(ObString("end_branch"), get_func(), end_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(helper_.create_icmp(left, right, compare_type, is_true))) { //这里的is_true是int1(1 bit)，需要转成int8(8 bit)
      LOG_WARN("failed to create_icmp_eq", K(ret));
    } else if (OB_FAIL(helper_.create_cond_br(is_true, true_branch, false_branch))) {
      LOG_WARN("failed to create_cond_br", K(ret));
    } else { /*do nothing*/ }

    if (OB_SUCC(ret)) {
      ObObjParam objparam;
      objparam.set_tinyint(1);
      objparam.set_param_meta();
      if (OB_FAIL(helper_.set_insert_point(true_branch))) { \
        LOG_WARN("failed to set insert point", K(ret)); \
      } else if (OB_FAIL(store_objparam(objparam, p_result_obj))) {
        LOG_WARN("failed to store objparam", K(ret));
      } else if (OB_FAIL(helper_.create_br(end_branch))) {
        LOG_WARN("failed to create_br", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret)) {
      ObObjParam objparam;
      objparam.set_tinyint(0);
      objparam.set_param_meta();
      if (OB_FAIL(helper_.set_insert_point(false_branch))) { \
        LOG_WARN("failed to set insert point", K(ret)); \
      } else if (OB_FAIL(store_objparam(objparam, p_result_obj))) {
        LOG_WARN("failed to store objparam", K(ret));
      } else if (OB_FAIL(helper_.create_br(end_branch))) {
        LOG_WARN("failed to create_br", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret) && OB_FAIL(set_current(end_branch))) {
      LOG_WARN("failed to set_current", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_arith_calc(ObLLVMValue &left,
                                           ObLLVMValue &right,
                                           ObItemType type,
                                           const ObExprResType &result_type,
                                           int64_t stmt_id,
                                           bool in_notfound,
                                           bool in_warning,
                                           ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_value;
  ObLLVMValue calc_result;
  if (T_OP_ADD == type) {
    if (OB_FAIL(helper_.create_add(left, right, calc_result))) {
      LOG_WARN("failed to create_add", K(ret));
    }
  } else if (T_OP_MINUS == type) {
    if (OB_FAIL(helper_.create_sub(left, right, calc_result))) {
      LOG_WARN("failed to create_add", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not supported yet", K(type), K(ret));
  }

  if (OB_SUCC(ret)) { //检查值域
    ObLLVMBasicBlock succ_block;
    ObLLVMBasicBlock error_block;
    ObLLVMBasicBlock check_block;
    ObLLVMBasicBlock end_block;

    ObLLVMValue low;
    ObLLVMValue high;
    ObLLVMValue is_true;
    ObLLVMValue ret_value;
    ObLLVMValue ret_value_ptr;

    OZ (helper_.create_ialloca(ObString("ret_value"), ObInt32Type, OB_SUCCESS, ret_value_ptr));
    OZ (helper_.create_block(ObString("succ_block"), func_, succ_block));
    OZ (helper_.create_block(ObString("error_block"), func_, error_block));
    OZ (helper_.create_block(ObString("check_block"), func_, check_block));
    OZ (helper_.create_block(ObString("end_block"), func_, end_block));
    OZ (helper_.get_int64(INT32_MIN, low));
    OZ (helper_.create_icmp(calc_result, low, ObLLVMHelper::ICMP_SGE, is_true));
    OZ (helper_.create_cond_br(is_true, check_block, error_block));
    OZ (helper_.set_insert_point(check_block));
    OZ (helper_.get_int64(INT32_MAX, high));
    OZ (helper_.create_icmp(high, calc_result, ObLLVMHelper::ICMP_SGE, is_true));
    OZ (helper_.create_cond_br(is_true, succ_block, error_block));
    OZ (helper_.set_insert_point(error_block));
    OZ (helper_.get_int32(OB_NUMERIC_OVERFLOW, ret_value));
    OZ (helper_.create_store(ret_value, ret_value_ptr));
    OZ (helper_.create_br(end_block));
    OZ (helper_.set_insert_point(succ_block));
    OZ (helper_.get_int32(OB_SUCCESS, ret_value));
    OZ (helper_.create_store(ret_value, ret_value_ptr));
    OZ (helper_.create_br(end_block));
    OZ (set_current(end_block));
    OZ (helper_.create_load(ObString("load_ret"), ret_value_ptr, ret_value));
    OZ (check_success(ret_value, stmt_id, in_notfound, in_warning));
  }

  if (OB_SUCC(ret)) {
    ObObjParam objparam;
    objparam.set_type(result_type.get_type()); //计算之后的结果放在int32里
    objparam.set_param_meta();
    if (OB_FAIL(store_objparam(objparam, p_result_obj))) {
      LOG_WARN("Not supported yet", K(type), K(ret));
    } else if (OB_FAIL(extract_value_ptr_from_objparam(p_result_obj, ObIntType, p_value))) {
      LOG_WARN("failed to extract_value_ptr_from_objparam", K(ret));
    } else if (OB_FAIL(helper_.create_store(calc_result, p_value))) {
      LOG_WARN("failed to create store", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_expr(int64_t expr_idx,
                                     const ObPLStmt &s,
                                     int64_t result_idx,
                                     ObLLVMValue &p_result_obj)
{
  int ret = OB_SUCCESS;
  ObLLVMValue expr_addr;
  if (OB_INVALID_INDEX == expr_idx || OB_ISNULL(s.get_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected argument", K(expr_idx), K(s.get_block()), K(ret));
  } else if (lib::is_oracle_mode() && ast_.is_simple_calc(expr_idx)) {
    if (OB_FAIL(generate_llvm_calc(expr_idx,
                                   s.get_stmt_id(),
                                   s.get_block()->in_notfound(),
                                   s.get_block()->in_warning(),
                                   result_idx, p_result_obj))) {
      LOG_WARN("failed to create_store", K(ret));
    }
  } else {
    if (OB_FAIL(generate_spi_calc(expr_idx,
                                  s.get_stmt_id(),
                                  s.get_block()->in_notfound(),
                                  s.get_block()->in_warning(),
                                  result_idx,
                                  p_result_obj))) {
      LOG_WARN("failed to create_store", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_early_exit(ObLLVMValue &count,
                                           int64_t stmt_id,
                                           bool in_notfound,
                                           bool in_warning)
{
  int ret = OB_SUCCESS;
  ObLLVMBasicBlock early_exit;
  ObLLVMBasicBlock after_check;
  ObLLVMValue is_true, need_check, first_check;
  ObSEArray<ObLLVMValue, 4> args;
  ObLLVMValue result;
  ObLLVMValue count_value;

  OZ (helper_.create_block(ObString("early_exit"), get_func(), early_exit));
  OZ (helper_.create_block(ObString("after_check"), get_func(), after_check));

  OZ (helper_.create_load(ObString("load_count"), count, count_value));
  OZ (helper_.create_inc(count_value, count_value));
  OZ (helper_.create_store(count_value, count));
#ifndef NDEBUG
  OZ (generate_debug(ObString("print count value"), count_value));
#endif
  /*!
   * need_check和first_check不等说明需要check, 相等则不需要check
   * 因为need_check和firstcheck不可能同时为true; 当同时为false时说明两个条件都没满足; 只有一个true时则说明满足一个条件;
   * 引入first_check是为了避免类似下面的匿名块形成的死循环无法退出;
   * BEGIN
   *   <<outer_loop>>
   *   FOR idx IN 1..10 LOOP
   *     CONTINUE outer_loop;
   *   END LOOP;
   * END;
   */
  OZ (helper_.create_icmp(count_value, EARLY_EXIT_CHECK_CNT, ObLLVMHelper::ICMP_SGE, need_check)); // 到达check次数
  OZ (helper_.create_icmp(count_value, 1, ObLLVMHelper::ICMP_EQ, first_check)); // 该值为1说明是循环的第一次check
  OZ (helper_.create_icmp(need_check, first_check, ObLLVMHelper::ICMP_NE, is_true));
  OZ (helper_.create_cond_br(is_true, early_exit, after_check));

  //处理check early exit
  OZ (set_current(early_exit));
  OZ (helper_.create_istore(1, count));
  OZ (args.push_back(get_vars().at(CTX_IDX)));
  OZ (helper_.create_call(ObString("check_early_exit"), get_spi_service().spi_check_early_exit_, args, result));
  OZ (check_success(result, stmt_id, in_notfound, in_warning));
  OZ (helper_.create_br(after_check));

  //处理正常流程
  OZ (set_current(after_check));

  return ret;
}

int ObPLCodeGenerator::generate_pointer(const void *ptr, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  int64_t addr = reinterpret_cast<int64_t>(ptr);
  if (OB_FAIL(helper_.get_int64(addr, value))) {
    LOG_WARN("failed to get int64", K(ret));
  }
  return ret;
}

int ObPLCodeGenerator::generate_expression_array(const ObIArray<int64_t> &exprs,
                                                 jit::ObLLVMValue &value,
                                                 jit::ObLLVMValue &count)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> expr_idx;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(expr_idx.push_back(exprs.at(i)))) {
      LOG_WARN("store expr addr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObLLVMValue first_addr;
    if (expr_idx.empty()) {
      if (OB_FAIL(generate_null_pointer(ObIntType, value))) {
        LOG_WARN("failed to generate_null_pointer", K(ret));
      }
    } else {
      if (OB_FAIL(generate_uint64_array(expr_idx, value))) {
        LOG_WARN("failed to get_uint64_array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObLLVMValue int_value;
      if (OB_FAIL(helper_.get_int64(exprs.count(), count))) {
        LOG_WARN("failed to get int64", K(ret));
      }
    }
  }
  return ret;
}

#define CHECK_COND_CONTROL \
  do { \
    if (OB_SUCC(ret)) { \
      ObLLVMBasicBlock do_control; \
      if (OB_INVALID_INDEX != control.get_cond()) { \
        ObLLVMValue p_result_obj; \
        ObLLVMValue result; \
        ObLLVMValue is_false; \
        if (OB_FAIL(helper_.create_block(ObString("do_control"), get_func(), do_control))) { \
          LOG_WARN("failed to create block", K(ret)); \
        } else if (OB_FAIL(helper_.create_block(ObString("after_control"), get_func(), after_control))) { \
          LOG_WARN("failed to create block", K(ret)); \
        } else if (OB_FAIL(generate_expr(control.get_cond(), control, OB_INVALID_INDEX, p_result_obj))) { \
          LOG_WARN("failed to generate calc_expr func", K(ret)); \
        } else if (OB_FAIL(extract_value_from_objparam(p_result_obj, control.get_cond_expr()->get_data_type(), result))) { \
          LOG_WARN("failed to extract_value_from_objparam", K(ret)); \
        } else if (OB_FAIL(helper_.create_icmp_eq(result, FALSE, is_false))) { \
          LOG_WARN("failed to create_icmp_eq", K(ret)); \
        } else if (OB_FAIL(helper_.create_cond_br(is_false, after_control, do_control))) { \
          LOG_WARN("failed to create_cond_br", K(ret)); \
        } else if (OB_FAIL(set_current(do_control))) { \
          LOG_WARN("failed to set insert point", K(ret)); \
        } else { } \
      } else { \
        if (OB_FAIL(helper_.create_block(ObString("do_control"), get_func(), do_control))) { \
          LOG_WARN("failed to create block", K(ret)); \
        } else if (OB_FAIL(helper_.create_block(ObString("after_control"), get_func(), after_control))) { \
          LOG_WARN("failed to create block", K(ret)); \
        } else if (OB_FAIL(helper_.create_br(do_control))) { \
          LOG_WARN("failed to create_cond_br", K(ret)); \
        } else if (OB_FAIL(set_current(do_control))) { \
          LOG_WARN("failed to set insert point", K(ret)); \
        } else { } \
      } \
    } \
  } while(0)

//跳转目的地和跳转语句之间的所有循环的栈都需要重置
#define RESTORE_LOOP_STACK \
  do { \
    if (OB_SUCC(ret)) { \
      const ObPLCodeGenerator::LabelStack::LabelInfo *label_info = get_label(control.get_next_label()); \
      if (OB_ISNULL(label_info)) { \
        ret = OB_ERR_LABEL_ILLEGAL; \
        LOG_WARN("label info is NULL", K(control.get_next_label()), K(ret)); \
        LOG_USER_ERROR(OB_ERR_LABEL_ILLEGAL, control.get_next_label().length(), \
                       control.get_next_label().ptr()); \
      } else { \
        LOG_DEBUG("label info is not null", K(control.get_next_label()), K(label_info->name_), K(label_info->level_)); \
        for (int64_t i = get_loop_count() - 1; OB_SUCC(ret) && i >= 0; --i) { \
          ObPLCodeGenerator::LoopStack::LoopInfo &loop_info = get_loops()[i]; \
          LOG_DEBUG("loop info", K(i), K(loop_info.level_), K(control.get_level())); \
          if (loop_info.level_ <= control.get_level() && loop_info.level_ >= label_info->level_) { \
            if (OB_NOT_NULL(loop_info.cursor_) \
                && loop_info.level_ != label_info->level_ \
                && OB_FAIL(generate_close(*loop_info.cursor_, \
                                          loop_info.cursor_->get_cursor()->get_package_id(), \
                                          loop_info.cursor_->get_cursor()->get_routine_id(), \
                                          loop_info.cursor_->get_index(), \
                                          false, \
                                          false))) { \
              LOG_WARN("failed to generate close for loop cursor", K(ret)); \
            } else if (OB_FAIL(helper_.stack_restore(loop_info.loop_))) { \
              LOG_WARN("failed to stack_restore", K(ret)); \
            } else { \
              LOG_DEBUG("success stack restore", K(i), K(loop_info.level_)); \
            } \
          } \
        } \
      } \
    } \
  } while(0)

int ObPLCodeGenerator::generate_loop_control(const ObPLLoopControl &control)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
      //控制流已断，后面的语句不再处理
  } else if (OB_FAIL(helper_.set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(set_debug_location(control))) {
    LOG_WARN("failed to set debug location", K(ret));
  } else if (OB_FAIL(generate_goto_label(control))) {
    LOG_WARN("failed to generate goto label", K(ret));
  } else if (OB_FAIL(generate_spi_pl_profiler_before_record(control))) {
    LOG_WARN("failed to generate spi profiler before record call", K(ret), K(control));
  } else if (!control.get_next_label().empty()) {
    ObLLVMBasicBlock after_control;

    CHECK_COND_CONTROL;

    RESTORE_LOOP_STACK;

    if (OB_SUCC(ret)) {
      ObLLVMBasicBlock next = PL_LEAVE == control.get_type() ? get_label(control.get_next_label())->exit_ : get_label(control.get_next_label())->start_;
      if (OB_ISNULL(next.get_v())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("a loop must have valid body", K(next), K(ret));
      } else if (OB_FAIL(generate_spi_pl_profiler_after_record(control))) {
        LOG_WARN("failed to generate spi profiler after record call", K(ret), K(control));
      } else if (OB_FAIL(helper_.create_br(next))) {
        LOG_WARN("failed to create br", K(ret));
      } else {
        if (OB_FAIL(set_current(after_control))) { //设置CURRENT, 调整INSERT POINT点
          LOG_WARN("failed to set current", K(ret));
        }
      }
    }
  } else {
    ObLLVMBasicBlock after_control;

    CHECK_COND_CONTROL;

    if (OB_SUCC(ret)) {
      ObPLCodeGenerator::LoopStack::LoopInfo *loop_info = get_current_loop();
      if (OB_ISNULL(loop_info)) {
        ret = OB_ERR_EXIT_CONTINUE_ILLEGAL;
        LOG_WARN("illegal EXIT/CONTINUE statement; it must appear inside a loop", K(ret));
      } else if (OB_FAIL(helper_.stack_restore(loop_info->loop_))) {
         LOG_WARN("failed to stack_restore", K(ret));
      } else {
        ObLLVMBasicBlock next = PL_LEAVE == control.get_type() ? loop_info->exit_ : loop_info->start_;
        if (OB_ISNULL(next.get_v())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("a loop must have valid body", K(ret));
        } else if (OB_FAIL(helper_.create_br(next))) {
          LOG_WARN("failed to create br", K(ret));
        } else {
          if (OB_FAIL(set_current(after_control))) { //设置CURRENT, 调整INSERT POINT点
            LOG_WARN("failed to set current", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_sql(const ObPLSql &sql,
                                    jit::ObLLVMValue &str,
                                    jit::ObLLVMValue &length,
                                    jit::ObLLVMValue &ps_sql,
                                    jit::ObLLVMValue &type,
                                    jit::ObLLVMValue &for_update,
                                    jit::ObLLVMValue &hidden_rowid,
                                    jit::ObLLVMValue &params,
                                    jit::ObLLVMValue &count,
                                    jit::ObLLVMValue &skip_locked)
{
  int ret = OB_SUCCESS;
  ObLLVMValue int_value;
  if (sql.get_params().empty()) {
    if (OB_FAIL(generate_global_string(sql.get_sql(), str, length))) {
      LOG_WARN("failed to get_string", K(ret));
    }
  } else {
    if (OB_FAIL(generate_null(ObCharType, str))) {
      LOG_WARN("failed to get_null_const", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_global_string(sql.get_ps_sql(), ps_sql, length))) {
      LOG_WARN("failed to get_string", K(ret));
    } else if (OB_FAIL(helper_.get_int64(sql.get_stmt_type(), type))) {
      LOG_WARN("failed to get int64", K(ret));
    } else if (OB_FAIL(helper_.get_int8(sql.is_for_update(), for_update))) {
      LOG_WARN("failed to get int8", K(ret));
    } else if (OB_FAIL(helper_.get_int8(sql.has_hidden_rowid(), hidden_rowid))) {
      LOG_WARN("failed to get int8", K(ret));
    } else if (OB_FAIL(generate_expression_array(
        sql.is_forall_sql() ? sql.get_array_binding_params() : sql.get_params(), params, count))) {
      LOG_WARN("get precalc expr array ir value failed", K(ret));
    } else if (OB_FAIL(helper_.get_int8(sql.is_skip_locked(), skip_locked))) {
      LOG_WARN("failed to get int8", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_into(const ObPLInto &into,
                                     ObLLVMValue &into_array_value,
                                     ObLLVMValue &into_count_value,
                                     ObLLVMValue &type_array_value,
                                     ObLLVMValue &type_count_value,
                                     ObLLVMValue &exprs_not_null_array_value,
                                     ObLLVMValue &pl_integer_range_array_value,
                                     ObLLVMValue &is_bulk)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_expression_array(into.get_into(), into_array_value, into_count_value))) {
    LOG_WARN("Failed to generate_into", K(ret));
  } else if (OB_FAIL(helper_.get_int8(static_cast<int64_t>(into.is_bulk()), is_bulk))) {
    LOG_WARN("failed to get int8", K(ret));
  } else if (into.get_data_type().count() != into.get_not_null_flags().count()
            || into.get_data_type().count() != into.get_pl_integer_ranges().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not null flags count is inconsistent with data type count.",
             K(ret),
             K(into.get_data_type()),
             K(into.get_not_null_flags()),
             K(into.get_pl_integer_ranges()));
  } else if (into.is_type_record() && 1 != into.get_into_data_type().count()) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("coercion into multiple record targets not supported", K(ret));
  } else {
    ObLLVMType data_type;
    ObLLVMType data_type_pointer;
    ObLLVMType array_type;
    ObLLVMType bool_type;
    ObLLVMType not_null_pointer;
    ObLLVMType nn_array_type; //not null array
    ObLLVMType int64_type;
    ObLLVMType range_range_pointer;
    ObLLVMType range_range_type;
    OZ (adt_service_.get_data_type(data_type));
    OZ (data_type.get_pointer_to(data_type_pointer));
    OZ (ObLLVMHelper::get_array_type(data_type,
                                     into.get_data_type().count(),
                                     array_type));
    OZ (helper_.create_alloca(ObString("datatype_array"),
                              array_type,
                              type_array_value));

    OZ (helper_.get_llvm_type(ObTinyIntType, bool_type));
    OZ (bool_type.get_pointer_to(not_null_pointer));
    OZ (ObLLVMHelper::get_array_type(bool_type,
                                     into.get_data_type().count(),
                                     nn_array_type));
    OZ (helper_.create_alloca(ObString("not_null_array"),
                              nn_array_type,
                              exprs_not_null_array_value));

    OZ (helper_.get_llvm_type(ObIntType, int64_type));
    OZ (int64_type.get_pointer_to(range_range_pointer));
    OZ (ObLLVMHelper::get_array_type(int64_type,
                                     into.get_data_type().count(),
                                     range_range_type));
    OZ (helper_.create_alloca(ObString("range_array"),
                              range_range_type,
                              pl_integer_range_array_value));
    if (OB_SUCC(ret)) {
      ObLLVMValue type_value;
      ObLLVMValue nnv_dest;  // nnv is short for not null value
      ObLLVMValue nnv_src;
      ObLLVMValue src_not_null_value;
      ObLLVMValue range_dest;
      ObLLVMValue range_src;

      for (int64_t i = 0; OB_SUCC(ret) && i < into.get_data_type().count(); ++i) {
        type_value.reset();
        nnv_src.reset();
        OZ (helper_.create_gep(ObString("extract_datatype"), type_array_value, i, type_value));
        OZ (store_data_type(into.get_data_type(i), type_value));

        OZ (helper_.create_gep(ObString("extract_not_null"),
                               exprs_not_null_array_value, i, nnv_dest));
        OZ (helper_.get_int8(static_cast<int64_t>(into.get_not_null_flag(i)), nnv_src));
        OZ (helper_.create_store(nnv_src, nnv_dest));

        OZ (helper_.create_gep(ObString("extract_range"),
                               pl_integer_range_array_value, i, range_dest));
        OZ (helper_.get_int64(into.get_pl_integer_range(i), range_src));
        OZ (helper_.create_store(range_src, range_dest));
      }

      OZ (helper_.create_bit_cast(ObString("datatype_array_to_pointer"),
            type_array_value, data_type_pointer, type_array_value));
      OZ (helper_.create_bit_cast(ObString("not_null_array_to_pointer"),
            exprs_not_null_array_value, not_null_pointer, exprs_not_null_array_value));
      OZ (helper_.create_bit_cast(ObString("range_array_to_pointer"),
            pl_integer_range_array_value, range_range_pointer, pl_integer_range_array_value));
      OZ (helper_.get_int64(static_cast<int64_t>(into.get_data_type().count()), type_count_value));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_into_restore(const ObIArray<int64_t> &into,
                                             const common::ObIArray<ObRawExpr*> *exprs,
                                             const ObPLSymbolTable *symbol_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exprs) || OB_ISNULL(symbol_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("symbol_table is NULL", K(exprs), K(symbol_table), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < into.count(); ++i) {
      const ObRawExpr *into_expr = NULL;
      if (into.at(i) < 0 || into.at(i) >= exprs->count() || OB_ISNULL(into_expr = exprs->at(into.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("into expr is NULL", K(i), K(into.at(i)), K(exprs->count()), K(into_expr), K(ret));
      } else if (into_expr->is_const_raw_expr() && !into_expr->get_result_type().is_ext()) {
        //只有值传递的变量（Local 基本变量）需要回存进全局符号表
        if (!static_cast<const ObConstRawExpr*>(into_expr)->get_value().is_unknown()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid into expr", K(static_cast<const ObConstRawExpr*>(into_expr)->get_value()), K(ret));
        } else {
          int64_t idx = static_cast<const ObConstRawExpr*>(into_expr)->get_value().get_unknown();
          const ObPLVar* var = symbol_table->get_symbol(idx);
          if (OB_ISNULL(var)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("var is NULL", K(i), K(idx), K(var), K(ret));
          } else {
            ObLLVMValue result;
            if (OB_FAIL(extract_datum_from_context(vars_.at(CTX_IDX), idx, var->get_type().get_obj_type(), result))) {
              LOG_WARN("failed to extract_value_from_context", K(ret));
            } else if (var->get_type().is_composite_type()) {
              ObLLVMType ir_type;
              ObLLVMType ir_popinter_type;
              if (OB_FAIL(get_llvm_type(var->get_type(), ir_type))) {
                LOG_WARN("get pl ir type failed", K(ret));
              } else if (OB_FAIL(ir_type.get_pointer_to(ir_popinter_type))) {
                LOG_WARN("failed to get pointer", K(ret));
              } else if (OB_FAIL(helper_.create_int_to_ptr(ObString("cast_int_to_ptr"), result, ir_popinter_type, result))) {
                LOG_WARN("failed to create_int_to_ptr", K(ret));
              } else if (OB_FAIL(helper_.create_load(ObString("load_var"), result, result))) {
                LOG_WARN("failed to create_load", K(ret));
              } else { /*do nothing*/ }
            } else { /*do nothing*/ }

            if (OB_SUCC(ret) && OB_FAIL(helper_.create_store(result, vars_.at(idx + USER_ARG_OFFSET)))) {
              LOG_WARN("failed to create_store", K(ret));
            }
          }
        }
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_set_variable(int64_t expr,
                                             ObLLVMValue &value,
                                             bool is_default,
                                             int64_t stmt_id,
                                             bool in_notfound,
                                             bool in_warning)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 5> args;
  ObLLVMValue expr_idx;
  ObLLVMValue is_default_value, need_copy;
  ObLLVMValue result;
  if (OB_FAIL(args.push_back(get_vars().at(CTX_IDX)))) { //PL的执行环境
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.get_int64(expr, expr_idx))) {
    LOG_WARN("failed to generate a pointer", K(ret));
  } else if (OB_FAIL(args.push_back(expr_idx))) { //expr
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(args.push_back(value))) { //value
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.get_int8(is_default, is_default_value))) {
    LOG_WARN("failed tio get int8", K(is_default), K(ret));
  } else if (OB_FAIL(args.push_back(is_default_value))) { //is_default
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.get_int8(false, need_copy))) {
    LOG_WARN("failed tio get int8", K(ret));
  } else if (OB_FAIL(args.push_back(need_copy))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(get_helper().create_call(ObString("spi_set_variable_to_expr"), get_spi_service().spi_set_variable_to_expr_, args, result))) {
    LOG_WARN("failed to create call", K(ret));
  } else if (OB_FAIL(check_success(result, stmt_id, in_notfound, in_warning))) {
    LOG_WARN("failed to check success", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_update_package_changed_info(const ObPLStmt &s,
                                                            uint64_t package_id,
                                                            uint64_t var_idx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMValue, 3> args;
  ObLLVMValue package_id_value;
  ObLLVMValue var_idx_value;
  ObLLVMValue result;
  OZ (args.push_back(get_vars().at(CTX_IDX)));
  OZ (helper_.get_int64(package_id, package_id_value));
  OZ (args.push_back(package_id_value));
  OZ (helper_.get_int64(var_idx, var_idx_value));
  OZ (args.push_back(var_idx_value));
  OZ (get_helper().create_call(ObString("spi_update_package_change_info"),
                               get_spi_service().spi_update_package_change_info_,
                               args, result));
  OZ (check_success(
    result, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
  return ret;
}

int ObPLCodeGenerator::generate_exception(ObLLVMValue &type,
                                          ObLLVMValue &ob_error_code,
                                          ObLLVMValue &error_code,
                                          ObLLVMValue &sql_state,
                                          ObLLVMValue &str_len,
                                          ObLLVMValue &stmt_id,
                                          ObLLVMBasicBlock &normal,
                                          ObLLVMValue &line_number,
                                          bool in_notfound,
                                          bool in_warning,
                                          bool signal)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(helper_.set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else {
    ObSEArray<ObLLVMValue, 2> args;
    ObLLVMValue arg;
    ObLLVMValue status;
    if (OB_FAIL(extract_status_from_context(get_vars().at(CTX_IDX), status))) {
      LOG_WARN("failed to extract_status_from_context", K(ret));
    } else if (OB_FAIL(helper_.create_store(ob_error_code, status))) {
      LOG_WARN("failed to create_store", K(ret));
    } else if (OB_FAIL(extract_pl_ctx_from_context(get_vars().at(CTX_IDX), arg))) { //obplcontext
      LOG_WARN("failed to set pl context", K(ret));
    } else if (OB_FAIL(args.push_back(arg))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(extract_pl_function_from_context(get_vars().at(CTX_IDX), arg))) { //obplfunction
      LOG_WARN("failed to set pl function", K(ret));
    } else if (OB_FAIL(args.push_back(arg))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(args.push_back(line_number))) { // line number
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(extract_allocator_from_context(get_vars().at(CTX_IDX), arg))) { //allocator
      LOG_WARN("failed to set extract_allocator_from_context", K(ret));
    } else if (OB_FAIL(args.push_back(arg))) {
      LOG_WARN("push_back error", K(ret));
    } else {
      ObLLVMType condition_type;
      ObLLVMValue condition;
      ObLLVMValue type_pointer;
      ObLLVMValue error_code_pointer;
      ObLLVMValue sql_state_pointer;
      ObLLVMValue str_len_pointer;
      ObLLVMValue stmt_id_pointer;
      ObLLVMValue signal_pointer;
      ObLLVMValue int_value;
      if (OB_FAIL(adt_service_.get_pl_condition_value(condition_type))) {
        LOG_WARN("failed to get_pl_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_alloca(ObString("handler_condition"), condition_type, condition))) {
        LOG_WARN("failed to create_alloca", K(ret));
      } else if (OB_FAIL(extract_type_ptr_from_condition_value(condition, type_pointer))) {
        LOG_WARN("failed to extract_type_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_store(type, type_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(extract_code_ptr_from_condition_value(condition, error_code_pointer))) {
        LOG_WARN("failed to extract_code_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_store(error_code, error_code_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(extract_name_ptr_from_condition_value(condition, sql_state_pointer))) {
        LOG_WARN("failed to extract_name_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_store(sql_state, sql_state_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(extract_len_ptr_from_condition_value(condition, str_len_pointer))) {
        LOG_WARN("failed to extract_len_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_store(str_len, str_len_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(extract_stmt_ptr_from_condition_value(condition, stmt_id_pointer))) {
        LOG_WARN("failed to extract_stmt_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.create_store(stmt_id, stmt_id_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(extract_signal_ptr_from_condition_value(condition, signal_pointer))) {
        LOG_WARN("failed to extract_signal_ptr_from_condition_value", K(ret));
      } else if (OB_FAIL(helper_.get_int8(signal, int_value))) {
        LOG_WARN("failed to get int8", K(ret));
      } else if (OB_FAIL(helper_.create_store(int_value, signal_pointer))) {
        LOG_WARN("failed to create_store", K(ret));
      } else if (OB_FAIL(args.push_back(condition))) {
        LOG_WARN("push_back error", K(ret));
#ifndef NDEBUG
      } else if (OB_FAIL(helper_.get_int64(4444, int_value))) {
        LOG_WARN("failed to get int64", K(ret));
      } else if (OB_FAIL(generate_debug(ObString("debug"), int_value))) {
        LOG_WARN("failed to create_call", K(ret));
#endif
      } else {
        ObLLVMValue exception;
        if (OB_FAIL(helper_.create_call(ObString("create_exception"), get_eh_service().eh_create_exception_, args, exception))) {
          LOG_WARN("failed to create_call", K(ret));
        } else {
          OZ (raise_exception(exception, error_code, sql_state, normal, in_notfound, in_warning, signal));
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_close_loop_cursor(bool is_from_exception, int64_t dest_level)
{
  int ret = OB_SUCCESS;
  for (int64_t i = get_loop_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    LoopStack::LoopInfo &loop_info = get_loops()[i];
    if (loop_info.level_ >= dest_level && NULL != loop_info.cursor_) {
      LOG_INFO("close ForLoop Cursor", KPC(loop_info.cursor_->get_cursor()), K(dest_level), K(is_from_exception), K(ret));
      OZ (generate_close(*loop_info.cursor_,
                          loop_info.cursor_->get_cursor()->get_package_id(),
                          loop_info.cursor_->get_cursor()->get_routine_id(),
                          loop_info.cursor_->get_index(),
                          false,/*cannot ignoe as must have been opened*/
                          !is_from_exception/*此时已经在exception里，如果出错不能再抛exception了*/));
    }
  }
  return ret;
}

int ObPLCodeGenerator::raise_exception(ObLLVMValue &exception,
                                       ObLLVMValue &error_code,
                                       ObLLVMValue &sql_state,
                                       ObLLVMBasicBlock &normal,
                                       bool in_notfound,
                                       bool in_warning,
                                       bool signal)
{
  int ret = OB_SUCCESS;
  /*
   * MySQL模式下：
   * 如果到了顶层handler，都没有捕捉到了该exception，视exception的类型决定下一个目的地：
   * SQLEXCEPTION：继续抛出异常
   * SQLWARNING：跳到抛出exception的语句的下一句
   * NOT FOUND：如果显式（raised by SIGNAL or RESIGNAL）抛出，继续抛出异常；如果是隐式抛出（raised normally），跳到抛出exception的语句的下一句
   *
   * Oracle模式下：
   * 遇到错误都抛出
   */
  ObLLVMBasicBlock current = get_current();
  ObLLVMBasicBlock raise_exception;
  OZ (helper_.create_block(ObString("raise_exception"), get_func(), raise_exception));
  OZ (set_current(raise_exception));
  if (OB_SUCC(ret)) {
    ObLLVMValue ret_value;
    ObLLVMValue exception_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_out_params().count(); ++i) {
      ObLLVMValue src_datum;
      jit::ObLLVMValue ret_err;
      ObSEArray<jit::ObLLVMValue, 2> args;
      OZ (extract_obobj_ptr_from_objparam(get_out_params().at(i), src_datum));
      OZ (args.push_back(get_vars()[CTX_IDX]));
      OZ (args.push_back(src_datum));
      OZ (helper_.create_call(ObString("spi_destruct_obj"), get_spi_service().spi_destruct_obj_, args, ret_err));
    }
    /*
     * 关闭从当前位置开始到目的exception位置所有For Loop Cursor
     * 因为此时已经在exception过程中，只尝试关闭，不再check_success
     */
    OZ (generate_close_loop_cursor(true, get_current_exception() != NULL ? get_current_exception()->level_ : 0));
    if (OB_ISNULL(get_current_exception())) {
      OZ (helper_.create_call(ObString("raise_exception"),
                              get_eh_service().eh_raise_exception_,
                              exception,
                              exception_result));
#if defined(__aarch64__)
      // On ARM, _Unwind_RaiseException may failed.
      OZ (generate_debug(ObString("CALL: failed to raise exception!"), exception_result));
      OZ (helper_.create_load(ObString("load_ret"), get_vars().at(RET_IDX), ret_value));
      OZ (helper_.create_ret(ret_value));
#else
      OZ (helper_.create_unreachable());
#endif
    } else {
      OZ (helper_.create_invoke(ObString("raise_exception"),
                                get_eh_service().eh_raise_exception_,
                                exception,
                                get_exit(),
                                get_current_exception()->exception_,
                                exception_result));
    }
  }
  if (OB_SUCC(ret)) {
    OZ (set_current(current));
    if (OB_FAIL(ret)) {
    } else if (lib::is_oracle_mode()) {
      OZ (helper_.create_br(raise_exception));
    } else if (lib::is_mysql_mode()) {
      ObLLVMBasicBlock normal_raise_block;
      ObLLVMValue exception_class;
      ObLLVMSwitch switch_inst1;
      ObLLVMSwitch switch_inst2;
      ObLLVMValue int_value;

      OZ (helper_.create_block(ObString("normal_raise_block"), get_func(), normal_raise_block));
      OZ (helper_.create_switch(error_code, normal_raise_block, switch_inst1));
      OZ (helper_.get_int64(ER_WARN_TOO_MANY_RECORDS, int_value));
      OZ (switch_inst1.add_case(int_value, raise_exception));
      OZ (helper_.get_int64(WARN_DATA_TRUNCATED, int_value));
      OZ (switch_inst1.add_case(int_value, raise_exception));
      OZ (helper_.get_int64(ER_SIGNAL_WARN, int_value));
      OZ (switch_inst1.add_case(int_value, raise_exception));

      OZ (set_current(normal_raise_block));
      OZ (helper_.create_call(ObString("get_exception_class"), get_eh_service().eh_classify_exception, sql_state, exception_class));
      OZ (helper_.create_switch(exception_class, raise_exception, switch_inst2));
      OZ (helper_.get_int64(SQL_WARNING, int_value));
      OZ (switch_inst2.add_case(int_value, in_warning ? raise_exception : normal));
      OZ (helper_.get_int64(NOT_FOUND, int_value));
      OZ (switch_inst2.add_case(int_value, signal || in_notfound ? raise_exception : normal));
    }
  }
  return ret;
}
int ObPLCodeGenerator::check_success(jit::ObLLVMValue &ret_err, int64_t stmt_id,
                                     bool in_notfound, bool in_warning, bool signal)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(helper_.set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(helper_.create_store(ret_err, vars_.at(RET_IDX)))) {
    LOG_WARN("failed to create_store", K(ret));
  } else {
    ObLLVMBasicBlock success_branch;
    ObLLVMBasicBlock fail_branch;
    ObLLVMValue is_true;

    if (OB_FAIL(helper_.create_block(ObString("ob_success"), get_func(), success_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(helper_.create_block(ObString("ob_fail"), get_func(), fail_branch))) {
      LOG_WARN("failed to create block", K(ret));
    } else if (OB_FAIL(helper_.create_icmp_eq(ret_err, OB_SUCCESS, is_true))) {
      LOG_WARN("failed to create_icmp_eq", K(ret));
    } else if (OB_FAIL(helper_.create_cond_br(is_true, success_branch, fail_branch))) {
      LOG_WARN("failed to create_cond_br", K(ret));
    } else if (OB_FAIL(set_current(fail_branch))) {
      LOG_WARN("failed to set_current", K(ret));
    } else {
      ObLLVMType int_type;
      ObLLVMType char_type;
      ObLLVMValue type_ptr;
      ObLLVMValue error_code_ptr;
      ObLLVMValue sql_state_ptr;
      ObLLVMValue str_len_ptr;
      if (OB_FAIL(helper_.get_llvm_type(ObIntType, int_type))) {
        LOG_WARN("failed to get_llvm_type", K(ret));
      } else if (OB_FAIL(helper_.get_llvm_type(ObCharType, char_type))) {
        LOG_WARN("failed to get_llvm_type", K(ret));
      } else if (OB_FAIL(helper_.create_alloca(ObString("exception_type"), int_type, type_ptr))) {
        LOG_WARN("failed to create_alloca", K(ret));
      } else if (OB_FAIL(helper_.create_alloca(ObString("error_code"), int_type, error_code_ptr))) {
        LOG_WARN("failed to create_alloca", K(ret));
      } else if (OB_FAIL(helper_.create_alloca(ObString("sql_state"), char_type, sql_state_ptr))) {
        LOG_WARN("failed to create_alloca", K(ret));
      } else if (OB_FAIL(helper_.create_alloca(ObString("str_len"), int_type, str_len_ptr))) {
        LOG_WARN("failed to create_alloca", K(ret));
      } else {
        ObSEArray<ObLLVMValue, 2> args;
        ObLLVMValue oracle_mode;
        if (OB_FAIL(helper_.get_int8(oracle_mode_, oracle_mode))) {
          LOG_WARN("helper get int8 failed", K(ret));
        } else if (OB_FAIL(args.push_back(oracle_mode))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(args.push_back(ret_err))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(args.push_back(type_ptr))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(args.push_back(error_code_ptr))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(args.push_back(sql_state_ptr))) {
          LOG_WARN("push_back error", K(ret));
        } else if (OB_FAIL(args.push_back(str_len_ptr))) {
          LOG_WARN("push_back error", K(ret));
        } else {
          ObLLVMValue result;
          ObLLVMValue type;
          ObLLVMValue error_code;
          ObLLVMValue sql_state;
          ObLLVMValue str_len;
          ObLLVMValue stmt_id_value;
          ObLLVMValue line_number_value;
          // stmt id目前是col和line的组合，暂时先用这个
          int64_t line_number = stmt_id;
          if (OB_FAIL(helper_.get_int64(line_number, line_number_value))) {
            LOG_WARN("failed to get_line_number", K(ret));
          } else if (OB_FAIL(helper_.get_int64(stmt_id, stmt_id_value))) {
            LOG_WARN("failed to get_int64", K(ret));
          } else if (OB_FAIL(helper_.create_call(ObString("convert_exception"), get_eh_service().eh_convert_exception_, args, result))) {
            LOG_WARN("failed to create_call", K(ret));
          } else if (OB_FAIL(helper_.create_load(ObString("load_type"), type_ptr, type))) {
            LOG_WARN("failed to create_load", K(ret));
          } else if (OB_FAIL(helper_.create_load(ObString("load_error_code"), error_code_ptr, error_code))) {
            LOG_WARN("failed to create_load", K(ret));
          } else if (OB_FAIL(helper_.create_load(ObString("load_sql_state"), sql_state_ptr, sql_state))) {
            LOG_WARN("failed to create_load", K(ret));
          } else if (OB_FAIL(helper_.create_load(ObString("load_str_len"), str_len_ptr, str_len))) {
            LOG_WARN("failed to create_load", K(ret));
          } else if (OB_FAIL(generate_exception(type, ret_err, error_code, sql_state, str_len, stmt_id_value, success_branch, line_number_value, in_notfound, in_warning, signal))) {
            LOG_WARN("failed to generate exception", K(ret));
          } else if (OB_FAIL(set_current(success_branch))) {
            LOG_WARN("failed to set_current", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::finish_current(const ObLLVMBasicBlock &next)
{
  int ret = OB_SUCCESS;
  if (NULL == get_current().get_v()) {
    //如果current为NULL，说明控制流已经被显式切走了（例如Iterate、Leave语句）
  } else if (get_current().get_v() == get_exit().get_v()) {
    //如果current是exit，说明控制流已经显式结束（如return）
  } else if (OB_FAIL(helper_.set_insert_point(get_current()))) {
    LOG_WARN("failed to set insert point", K(ret));
  } else if (OB_FAIL(helper_.create_br(next))) {
    LOG_WARN("failed to create br", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::generate_prototype()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMType, 8> arg_types;
  ObLLVMFunctionType ft;
  ObLLVMType pl_exec_context_type;
  ObLLVMType pl_exec_context_pointer_type;
  ObLLVMType argv_type;
  ObLLVMType argv_pointer_type;
  if (OB_FAIL(adt_service_.get_pl_exec_context(pl_exec_context_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(pl_exec_context_type.get_pointer_to(pl_exec_context_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else if (OB_FAIL(adt_service_.get_argv(argv_type))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(argv_type.get_pointer_to(argv_pointer_type))) {
    LOG_WARN("failed to get_pointer_to", K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    ObLLVMType int64_type;
    if (OB_FAIL(arg_types.push_back(pl_exec_context_pointer_type))) { //函数第一个参数必须是基础环境信息隐藏参数ObPLExecCtx*
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.get_llvm_type(ObIntType, int64_type))) {
      LOG_WARN("failed to get_llvm_type", K(ret));
    } else if (OB_FAIL(arg_types.push_back(int64_type))) { //第二个参数是int64_t ArgC
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(arg_types.push_back(argv_pointer_type))) { //第三个参数是int64_t[] ArgV
      LOG_WARN("push_back error", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (get_ast().get_ret_type().is_obj_type()
        && (OB_ISNULL(get_ast().get_ret_type().get_data_type())
            || get_ast().get_ret_type().get_data_type()->get_meta_type().is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return type is invalid", K(func_), K(ret));
    } else {
      ObLLVMType ret_type;
      ObLLVMFunctionType ft;
      if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, ret_type))) {
        LOG_WARN("failed to get_llvm_type", K(ret));
      } else if (OB_FAIL(ObLLVMFunctionType::get(ret_type, arg_types, ft))) {
        LOG_WARN("failed to get function type", K(ret));
      } else if (OB_FAIL(helper_.create_function(get_ast().get_name(), ft, func_))) {
        LOG_WARN("failed to create function", K(ret));
      } else { /*do nothing*/ }
    }
  }

  //生成参数
  if (OB_SUCC(ret)) {
    //获取数据类型属性
    int64_t size = 0;
    if (OB_FAIL(func_.get_argument_size(size))) {
      LOG_WARN("failed to get argument size", K(ret));
    } else {
      ObLLVMValue arg;
      int64_t i = CTX_IDX;
      for (int64_t j = 0; OB_SUCC(ret) && j < size; ++j) {
        if (OB_FAIL(func_.get_argument(j, arg))) {
          LOG_WARN("failed to get argument", K(j), K(ret));
        } else if (OB_FAIL(arg.set_name(ArgName[i]))) {
          LOG_WARN("failed to set name", K(j), K(ret));
        } else {
          vars_.at(i++) = arg;
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::unset_debug_location()
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    jit::ObLLVMDIScope scope;
    if (OB_FAIL(di_helper_.get_current_scope(scope))) {
      LOG_WARN("failed to set debug location", K(ret));
    } else if (OB_FAIL(helper_.unset_debug_location(&scope))) {
      LOG_WARN("failed to unset debug location", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::set_debug_location(const ObPLStmt &s)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    uint32_t line = s.get_line() + 1;
    uint32_t col = s.get_col();
    jit::ObLLVMDIScope scope;
    if (OB_FAIL(di_helper_.get_current_scope(scope))) {
      LOG_WARN("failed to get current scope", K(line), K(col), K(ret));
    } else if (OB_FAIL(helper_.set_debug_location(line, col, &scope))) {
      LOG_WARN("failed to set debug location", K(line), K(col), K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::get_di_llvm_type(const ObPLDataType &pl_type, ObLLVMDIType &di_type)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    if (pl_type.is_obj_type()) {
      if (OB_FAIL(di_adt_service_.get_di_type(pl_type.get_obj_type(), di_type))) {
        LOG_WARN("failed to get di type", K(ret));
      }
    } else if (pl_type.is_user_type()) {
      uint64_t user_type_id = pl_type.get_user_type_id();
      if (OB_FAIL(di_user_type_map_.get_refactored(user_type_id, di_type))) {
        LOG_WARN("failed to get di user type map", K(user_type_id), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl type is invalid", K(pl_type.get_type()));
    }
  }
  return ret;
}

int ObPLCodeGenerator::get_di_datum_type(const ObPLDataType &pl_type, ObLLVMDIType &di_type)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    if (pl_type.is_obj_type()) {
      // why we call get_datum_type(), not get_di_type().
      // see ObPLCodeGenerate::get_datum_type().
      if (OB_FAIL(di_adt_service_.get_datum_type(pl_type.get_obj_type(), di_type))) {
        LOG_WARN("failed to get datum type", K(ret));
      }
    } else if (pl_type.is_user_type()) {
      uint64_t user_type_id = pl_type.get_user_type_id();
      if (OB_FAIL(di_user_type_map_.get_refactored(user_type_id, di_type))) {
        LOG_WARN("failed to get di user type map", K(user_type_id), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl type is invalid", K(pl_type.get_type()));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_di_user_type(const ObUserDefinedType &type, uint32_t line)
{
  int ret = OB_SUCCESS;
  if (debug_mode_ && false) {
    if (type.is_record_type()) {
      const ObRecordType &record_type = static_cast<const ObRecordType&>(type);
      if (OB_FAIL(generate_di_record_type(record_type, line))) {
        LOG_WARN("failed to generate di record type", K(ret));
      }
#ifdef OB_BUILD_ORACLE_PL
    } else if (type.is_nested_table_type()) {
      const ObNestedTableType &table_type = static_cast<const ObNestedTableType&>(type);
      if (OB_FAIL(generate_di_table_type(table_type, line))) {
        LOG_WARN("failed to generate di table type", K(ret));
      }
#endif
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user defined type is invalid", K(type.get_type()));
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLCodeGenerator::generate_di_table_type(const ObNestedTableType &table_type, uint32_t line)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    const ObString &type_name = table_type.get_name();
    const ObPLDataType &data_type = table_type.get_element_type();
    ObLLVMDIType element_type;
    ObLLVMDIType di_table_type;
    uint64_t type_id = table_type.get_user_type_id();
    if (OB_FAIL(get_di_datum_type(data_type, element_type))) {
      LOG_WARN("failed to get di datum type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_table_type(type_name, line, element_type, di_table_type))) {
      LOG_WARN("failed to get array type", K(ret));
    } else if (OB_FAIL(di_user_type_map_.set_refactored(type_id, di_table_type))) {
      LOG_WARN("failed to set di table type to user type map", K(ret), K(table_type));
    }
  }
  return ret;
}
#endif

int ObPLCodeGenerator::generate_di_record_type(const ObRecordType &record_type, uint32_t line)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObSEArray<ObString, 8> cell_names;
    ObSEArray<ObLLVMDIType, 8> cell_types;
    ObLLVMDIType di_record_type;
    for (int64_t i = 0; OB_SUCC(ret) && i < record_type.get_record_member_count(); ++i) {
      const ObString *cell_name = record_type.get_record_member_name(i);
      const ObPLDataType *data_type = record_type.get_record_member_type(i);
      ObLLVMDIType cell_type;
      if (OB_ISNULL(cell_name) || OB_ISNULL(data_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cell name or data type is null");
      } else if (OB_FAIL(get_di_datum_type(*data_type, cell_type))) {
        LOG_WARN("failed to get di datum type", K(i), K(*data_type), K(ret));
      } else if (OB_FAIL(cell_names.push_back(*cell_name))) {
        LOG_WARN("failed to push back cell name", K(ret));
      } else if (OB_FAIL(cell_types.push_back(cell_type))) {
        LOG_WARN("failed to push back cell type", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObString &type_name = record_type.get_name();
      uint64_t type_id = record_type.get_user_type_id();
      if (OB_FAIL(di_adt_service_.get_record_type(type_name, line, cell_names, cell_types, di_record_type))) {
        LOG_WARN("failed to get record type", K(record_type), K(ret));
      } else if (OB_FAIL(di_user_type_map_.set_refactored(type_id, di_record_type))) {
        LOG_WARN("failed to set di record type to user type map", K(record_type), K(ret));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_di_argument()
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObLLVMDIType di_ret_type;
    ObLLVMDIType di_ctx_type;
    ObLLVMDIType di_argc_type;
    ObLLVMDIType di_argv_type;
    ObLLVMDIType di_argv_ptr_type;
    if (OB_FAIL(di_adt_service_.get_obj_type(ObInt32Type, di_ret_type))) {
      LOG_WARN("failed to get element di type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_exec_ctx(di_ctx_type))) {
      LOG_WARN("failed to get element di type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_obj_type(ObIntType, di_argc_type))) {
      LOG_WARN("failed to get element di type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_argv(get_ast().get_arg_count(), di_argv_type))) {
      LOG_WARN("failed to get element di type", K(ret));
    } else if (OB_FAIL(di_helper_.create_pointer_type(di_argv_type, di_argv_ptr_type))) {
      LOG_WARN("failed to create di poiner type", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_di_local_variable(const ObPLVar &var,
                                                  uint32_t arg_no, uint32_t line, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObLLVMDIType type;
    if (OB_FAIL(get_di_llvm_type(var.get_type(), type))) {
      LOG_WARN("failed to get llvm di type", K(ret));
    } else if (OB_FAIL(generate_di_local_variable(var.get_name(), type, arg_no, line, value))) {
      LOG_WARN("failed to generate di local variable", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_di_local_variable(const ObString &name, ObLLVMDIType &type,
                                                  uint32_t arg_no, uint32_t line, ObLLVMValue &value)
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObLLVMBasicBlock block;
    ObLLVMDILocalVariable variable;
    if (OB_FAIL(helper_.get_insert_block(block))) {
      LOG_WARN("failed to get insert block", K(name), K(ret));
    } else if (OB_FAIL(di_helper_.create_local_variable(name, arg_no, line, type, variable))) {
      LOG_WARN("failed to create di local variable", K(name), K(ret));
    } else if (OB_FAIL(di_helper_.insert_declare(value, variable, block))) {
      LOG_WARN("failed to insert declare", K(name), K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::init_di_adt_service()
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObSqlString sql;
    if (get_ast().get_package_name().empty()) {
      CK (!get_ast().get_db_name().empty());
      OZ (sql.append_fmt("%.*s.%.*s",
                         get_ast().get_db_name().length(), get_ast().get_db_name().ptr(),
                         get_ast().get_name().length(), get_ast().get_name().ptr()));
    } else {
      CK (!get_ast().get_db_name().empty());
      OZ (sql.append_fmt("%.*s.%.*s.%.*s",
                         get_ast().get_db_name().length(), get_ast().get_db_name().ptr(),
                         get_ast().get_package_name().length(), get_ast().get_package_name().ptr(),
                         get_ast().get_name().length(), get_ast().get_name().ptr()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(di_adt_service_.init(sql.ptr()))) {
      LOG_WARN("failed to init di service", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_di_prototype()
{
  int ret = OB_SUCCESS;
  if (debug_mode_) {
    ObSqlString sql;
    int64_t argc = 0;
    ObLLVMDIType ret_type;
    ObLLVMDIType pl_exec_ctx_type;
    ObLLVMDIType pl_exec_ctx_ptr_type;
    ObLLVMDIType argc_type;
    ObLLVMDIType argv_type;
    ObLLVMDIType argv_ptr_type;
    ObSEArray<ObLLVMDIType, 8> member_types;
    ObLLVMDISubroutineType sr_type;
    ObLLVMDISubprogram sp;
    if (get_ast().get_package_name().empty()) {
      CK (!get_ast().get_db_name().empty());
      OZ (sql.append_fmt("%.*s.%.*s",
                         get_ast().get_db_name().length(), get_ast().get_db_name().ptr(),
                         get_ast().get_name().length(), get_ast().get_name().ptr()));
    } else {
      CK (!get_ast().get_db_name().empty());
      OZ (sql.append_fmt("%.*s.%.*s.%.*s",
                         get_ast().get_db_name().length(), get_ast().get_db_name().ptr(),
                         get_ast().get_package_name().length(), get_ast().get_package_name().ptr(),
                         get_ast().get_name().length(), get_ast().get_name().ptr()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(func_.get_argument_size(argc))) {
      LOG_WARN("failed to get argument size", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_obj_type(ObInt32Type, ret_type))) {
      LOG_WARN("failed to get di ret type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_exec_ctx(pl_exec_ctx_type))) {
      LOG_WARN("failed to get di pl exec ctx type", K(ret));
    } else if (OB_FAIL(di_helper_.create_pointer_type(pl_exec_ctx_type, pl_exec_ctx_ptr_type))) {
      LOG_WARN("failed to create di poiner type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_obj_type(ObIntType, argc_type))) {
      LOG_WARN("failed to get di argc type", K(ret));
    } else if (OB_FAIL(di_adt_service_.get_argv(argc, argv_type))) {
      LOG_WARN("failed to get di argv type", K(ret));
    } else if (OB_FAIL(di_helper_.create_pointer_type(argv_type, argv_ptr_type))) {
      LOG_WARN("failed to create di poiner type", K(ret));
    } else if (OB_FAIL(member_types.push_back(ret_type))) {
      LOG_WARN("failed to push back di ret type", K(ret));
    } else if (OB_FAIL(member_types.push_back(pl_exec_ctx_ptr_type))) {
      LOG_WARN("failed to push back di pl exec ctx type", K(ret));
    } else if (OB_FAIL(member_types.push_back(argc_type))) {
      LOG_WARN("failed to push back di argc type", K(ret));
    } else if (OB_FAIL(member_types.push_back(argv_ptr_type))) {
      LOG_WARN("failed to push back di argv type", K(ret));
    } else if (OB_FAIL(di_helper_.create_subroutine_type(member_types, sr_type))) {
      LOG_WARN("failed to create di subroutine type", K(ret));
    } else if (OB_ISNULL(sr_type.get_v())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create di subroutine type", K(ret));
    } else if (OB_FAIL(di_helper_.create_function(sql.ptr(), sr_type, sp))) {
      LOG_WARN("failed to create di subprogram", K(ret));
    } else {
      func_.set_subprogram(&sp);
    }
  }
  return ret;
}

int ObPLCodeGenerator::init_argument()
{
  int ret = OB_SUCCESS;
  ObLLVMValue int32_value;
  if (OB_FAIL(helper_.create_ialloca(ObString(ArgName[RET_IDX]), ObInt32Type, OB_SUCCESS, vars_.at(RET_IDX)))) {
    LOG_WARN("failed to create_alloca", K(ret));
  } else if (OB_FAIL(helper_.get_int32(OB_SUCCESS, int32_value))) {
    LOG_WARN("failed to get_int32", K(ret));
  } else if (OB_FAIL(helper_.create_store(int32_value, vars_.at(RET_IDX)))) {
    LOG_WARN("failed to create_store", K(ret));
  } else if (OB_FAIL(unset_debug_location())) {
    LOG_WARN("failed to unset debug location", K(ret));
  } else {
    for (int64_t i = 0; i < get_ast().get_arg_count(); ++i) {
      const ObPLVar *var = get_ast().get_symbol_table().get_symbol(i);
      if (OB_ISNULL(var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("var is NULL", K(i), K(var), K(ret));
      } else {
        ObLLVMValue arg;
        ObLLVMValue addr;
        ObLLVMValue init_value;
        if (var->get_type().is_composite_type() || var->get_type().is_cursor_type()) {
          if (OB_FAIL(extract_objparam_from_argv(vars_.at(ARGV_IDX), i, arg))) {
            LOG_WARN("failed to extract_objparam_from_argv", K(i), K(ret));
          } else if (OB_FAIL(extract_value_from_objparam(arg, ObExtendType, addr))) {
            LOG_WARN("failed to extract_value_from_objparam", K(ret));
          } else if (OB_FAIL(helper_.get_int32(0, init_value))) {
            LOG_WARN("failed to get_int32", K(ret));
          } else if (OB_FAIL(set_var_addr_to_param_store(i, addr, init_value))) {
            LOG_WARN("set var addr to param store failed", K(ret));
          } else if (OB_FAIL(helper_.create_gep(ObString("obj"), arg, 0, arg))) {
            LOG_WARN("failed to create_gep", K(ret));
          } else {
            get_vars().at(i + USER_ARG_OFFSET) = arg;
          }
        } else if (OB_ISNULL(var->get_type().get_data_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("basic pl type got null data type is unexpected", K(ret), K(var->get_type()));
        } else {
          const ObDataType &data_type = *(var->get_type().get_data_type());
          ObLLVMType ir_type;
          if (OB_FAIL(extract_datum_from_argv(vars_.at(ARGV_IDX), i, data_type.get_obj_type(), arg))) {
            LOG_WARN("failed to extract_datum_from_argv", K(ret));
          } else if (OB_FAIL(ObPLDataType::get_llvm_type(data_type.get_obj_type(), helper_, adt_service_, ir_type))) {
            LOG_WARN("failed to get_ir_type", K(data_type.get_obj_type()), K(ret));
          } else if (OB_FAIL(helper_.create_alloca(var->get_name(), ir_type, vars_.at(i + USER_ARG_OFFSET)))) {
            LOG_WARN("failed to create_alloca", K(ret));
          } else if (OB_FAIL(helper_.create_store(arg, vars_.at(i + USER_ARG_OFFSET)))) {
            LOG_WARN("failed to create_store", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::prepare_local_user_type()
{
  int ret = OB_SUCCESS;
  int64_t count = ast_.get_user_type_table().get_count();
  for (int64_t  i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObUserDefinedType *user_type = ast_.get_user_type_table().get_type(i);
    CK (OB_NOT_NULL(user_type));
    OZ (generate_user_type(*user_type), K(i), KPC(user_type));
  }
  return ret;
}

int ObPLCodeGenerator::prepare_external()
{
  int ret = OB_SUCCESS;
  int64_t count = ast_.get_user_type_table().get_external_types().count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObUserDefinedType *user_type = ast_.get_user_type_table().get_external_types().at(i);
    CK (OB_NOT_NULL(user_type));
    OZ (generate_user_type(*user_type), K(i), K(count), KPC(user_type));
  }
  LOG_DEBUG("pl/sql code generator prepare_external types",
            K(ret),
            K(ast_.get_user_type_table().get_external_types().count()),
            K(ast_.get_user_type_table().get_external_types()),
            K(&(ast_.get_user_type_table().get_external_types())),
            K(&(ast_)),
            K(ast_.get_db_name()),
            K(ast_.get_name()),
            K(ast_.get_id()));
  return ret;
}

int ObPLCodeGenerator::prepare_expression(ObPLCompileUnit &pl_func)
{
  int ret = OB_SUCCESS;
  ObArray<ObSqlExpression*> array;
  for (int64_t i = 0; OB_SUCC(ret) && i < ast_.get_exprs().count(); ++i) {
    ObSqlExpression *expr = NULL;
    if (OB_FAIL(pl_func.get_sql_expression_factory().alloc(expr))) {
      LOG_WARN("failed to alloc expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create expr", K(ret));
    } else if (OB_FAIL(array.push_back(expr))) {
      LOG_WARN("push back error", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pl_func.set_expressions(array))) {
      LOG_WARN("failed to set expressions", K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::prepare_subprogram(ObPLFunction &pl_func)
{
  int ret = OB_SUCCESS;
  OZ (ObPLCompiler::compile_subprogram_table(allocator_,
                                             session_info_,
                                             pl_func.get_exec_env(),
                                             ast_.get_routine_table(),
                                             pl_func,
                                             schema_guard_));
  return ret;
}

int ObPLCodeGenerator::set_profiler_unit_info_recursive(const ObPLCompileUnit &unit)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < unit.get_routine_table().count(); ++i) {
    if (OB_NOT_NULL(unit.get_routine_table().at(i))) {
      unit.get_routine_table().at(i)->set_profiler_unit_info(unit.get_profiler_unit_info());
      OZ (SMART_CALL(set_profiler_unit_info_recursive(*unit.get_routine_table().at(i))));
    }
  }

  return ret;
}

int ObPLCodeGenerator::generate_obj_access_expr()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ast_.get_obj_access_exprs().count(); ++i) {
    const ObRawExpr *expr = ast_.get_obj_access_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(i), K(ast_.get_obj_access_exprs()), K(ret));
    } else if (!expr->is_obj_access_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is not obj access", K(i), K(*expr), K(ret));
    } else {
      const ObObjAccessRawExpr *obj_access_expr = static_cast<const ObObjAccessRawExpr*>(expr);
      if (OB_FAIL(generate_get_attr_func(
          obj_access_expr->get_access_idxs(),
          obj_access_expr->get_var_indexs().count() + obj_access_expr->get_param_count(),
          obj_access_expr->get_func_name(),
          obj_access_expr->for_write()))) {
        LOG_WARN("generate get attr function failed",
                 K(ret),
                 K(obj_access_expr->get_access_idxs()),
                 K(obj_access_expr->get_func_name()));
      }
    }
  }
  return ret;
}

/*
 * 把ObObjAccessRawExpr翻译成一个函数，其中的每个ObObjAccessIdx可能是：
 * 1、const，即常量，只能出现在表的下标位置，如a(1)里的1;
 * 2、property,即固有属性，只能出现在表的属性位置，如a.count里的count；
 * 3、local,即PL内部变量，可能出现在
 *    1）、初始内存位置，如a(1)里的a；
 *    2）、表的下标位置，如a(i)里的i；
 *    3）、record的属性位置，如a.b里的b；
 * 4、external（包括IS_PKG、IS_USER、IS_SESSION、IS_GLOBAL），可能出现在
 *    1）、初始内存位置，如a(1)里的a；
 *    2）、表的下标位置，如a(i)里的i；
 *    3）、record的属性位置，如a.b里的b；
 * 5、ns不可能出现，在resolver阶段已经被剥掉了。
 * 对于以上几种情况：
 * 1、直接用作解析复杂变量；
 * 2、直接用作解析复杂变量；
 * 3、因为把变量在param store里的下标放在var_indexs_里，所以根据var_indexs_从param store里取出值使用；
 * 4、因为把计算的结果作为ObObjAccessRawExpr的child传给了表达式计算，所以直接取obj_stack的值使用。
 * 函数签名:
 *  int32_t get_func(int64_t param_cnt, int64_t* params, int64_t* element_val);
 * */
int ObPLCodeGenerator::generate_get_attr_func(const ObIArray<ObObjAccessIdx> &idents,
                                              int64_t param_count, const ObString &func_name,
                                              bool for_write)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLLVMType, 4> arg_types;
  ObLLVMFunctionType ft;
  ObLLVMType bool_type;
  ObLLVMType array_type;
  ObLLVMType array_pointer_type;
  ObLLVMType int64_type;
  ObLLVMType int32_type;
  ObLLVMType int64_pointer_type;

  OZ (helper_.get_llvm_type(ObIntType, int64_type));
  OZ (helper_.get_llvm_type(ObInt32Type, int32_type));
  OZ (helper_.get_array_type(int64_type, param_count, array_type));
  OZ (array_type.get_pointer_to(array_pointer_type));
  OZ (int64_type.get_pointer_to(int64_pointer_type));

  OZ (arg_types.push_back(int64_type));
  OZ (arg_types.push_back(array_pointer_type));
  OZ (arg_types.push_back(int64_pointer_type));
  OZ (ObLLVMFunctionType::get(int32_type, arg_types, ft));
  OZ (helper_.create_function(func_name, ft, func_));

  OZ (helper_.create_block(ObString("entry"), func_, entry_));
  OZ (helper_.create_block(ObString("exit"), func_, exit_));
  OZ (set_current(entry_));

  if (OB_SUCC(ret)) {
    //获取数据类型属性
    ObLLVMValue param_cnt;
    ObLLVMValue params_ptr;
    ObLLVMValue element_value;
    ObLLVMValue result_value_ptr;
    ObLLVMValue ret_value_ptr;
    ObLLVMValue ret_value;
    OZ (func_.get_argument(0,param_cnt));
    OZ (param_cnt.set_name(ObString("param_cnt")));
    OZ (func_.get_argument(1, params_ptr));
    OZ (params_ptr.set_name(ObString("param_array")));
    OZ (func_.get_argument(2, result_value_ptr));
    OZ (result_value_ptr.set_name(ObString("result_value_ptr")));
    OZ (helper_.create_alloca(ObString("ret_value"), int32_type, ret_value_ptr));

    OZ (generate_get_attr(params_ptr, idents, for_write,
                          result_value_ptr, ret_value_ptr, exit_), idents);

    OZ (helper_.create_br(exit_));
    OZ (helper_.set_insert_point(exit_));
    OZ (helper_.create_load(ObString("load_ret_value"), ret_value_ptr, ret_value));
    OZ (helper_.create_ret(ret_value));
  }
  return ret;
}

int ObPLCodeGenerator::final_expression(ObPLCompileUnit &pl_func)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ast_.get_obj_access_exprs().count(); ++i) {
    ObRawExpr *expr = ast_.get_obj_access_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj_access_expr is null");
    } else if (!expr->is_obj_access_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not a obj access", K(*expr), K(ret));
    } else {
      ObObjAccessRawExpr* obj_access_expr = static_cast<ObObjAccessRawExpr*>(expr);
      obj_access_expr->set_get_attr_func_addr(helper_.get_function_address(obj_access_expr->get_func_name()));
    }
  }

  if (OB_SUCC(ret)) {
    {
      // generate static engine expressions
      sql::ObRawExprUniqueSet raw_exprs(false);
      for (int64_t i = 0; OB_SUCC(ret) && i < ast_.get_exprs().count(); i++) {
        OZ(raw_exprs.append(ast_.get_expr(i)));
      }
      sql::ObStaticEngineExprCG se_cg(pl_func.get_allocator(),
                                      &session_info_,
                                      &schema_guard_,
                                      0 /* original param cnt */,
                                      0/* param count*/,
                                      GET_MIN_CLUSTER_VERSION());
      se_cg.set_rt_question_mark_eval(true);
      OZ(se_cg.generate(raw_exprs, pl_func.get_frame_info()));

      uint32_t expr_op_size = 0;
      RowDesc row_desc;
      ObExprGeneratorImpl expr_generator(pl_func.get_expr_operator_factory(), 0, 0,
                                         &expr_op_size, row_desc);
      for (int64_t i = 0; OB_SUCC(ret) && i < ast_.get_exprs().count(); ++i) {
        ObRawExpr *raw_expr = ast_.get_expr(i);
        ObSqlExpression *expression = static_cast<ObSqlExpression*>(get_expr(i));
        if (OB_ISNULL(raw_expr) || OB_ISNULL(expression)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid arguments", K(i), K(raw_expr), K(expression), K(ret));
        } else {
          // TODO bin.lb: No need to generate expression if static engine enabled
          //
          if (OB_FAIL(expr_generator.generate(*raw_expr, *expression))) {
            SQL_LOG(WARN, "Generate post_expr error", K(ret), KPC(raw_expr));
          } else {
            expression->set_expr(raw_expr->rt_expr_);
          }
        }
      }
      if (OB_SUCC(ret)) {
        pl_func.set_expr_op_size(std::max(pl_func.get_frame_info().need_ctx_cnt_,
                                          static_cast<int64_t>(expr_op_size)));
      }
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_goto_label(const ObPLStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.get_is_goto_dst()) {

    if (NULL == get_current().get_v()) {
        //控制流已断，后面的语句不再处理
    } else {
      // 去看一下对应的goto是否已经cg了，没有的话就记录一下这个label地址。
      ObLLVMValue stack;
      hash::HashMapPair<ObPLCodeGenerator::goto_label_flag, std::pair<ObLLVMBasicBlock, ObLLVMBasicBlock>> pair;
      int tmp_ret = get_goto_label_map().get_refactored(stmt.get_stmt_id(), pair);
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        ObLLVMBasicBlock label_block;
        ObLLVMBasicBlock stack_save_block;
        const ObString *lab = stmt.get_goto_label();
        if (OB_FAIL(get_helper().create_block(NULL == lab ? ObString("") : *lab, get_func(),
                                                label_block))) {
          LOG_WARN("create goto label failed", K(ret));
        } else if (OB_FAIL(get_helper().create_block(NULL == lab ? ObString("") : *lab, get_func(),
                                                stack_save_block))) {
          LOG_WARN("create goto label failed", K(ret));
        } else if (OB_FAIL(get_helper().create_br(stack_save_block))) {
          LOG_WARN("failed to create_br", K(ret));
        } else if (OB_FAIL(get_helper().set_insert_point(stack_save_block))) {
          LOG_WARN("failed to set insert point", K(ret));
        } else if (OB_FAIL(set_current(stack_save_block))) {
          LOG_WARN("failed to set current block", K(ret));
        } else if (OB_FAIL(get_helper().stack_save(stack))) {
          LOG_WARN("failed to save current stack", K(ret));
        } else if (OB_FAIL(get_helper().create_br(label_block))) {
          LOG_WARN("failed to create_br", K(ret));
        } else if (OB_FAIL(get_helper().set_insert_point(label_block))) {
          LOG_WARN("failed to set insert point", K(ret));
        } else if (OB_FAIL(set_current(label_block))) {
          LOG_WARN("failed to set current block", K(ret));
        } else if (OB_FAIL(get_helper().stack_restore(stack))) {
          LOG_WARN("failed to restore stack", K(ret));
        } else if (OB_FAIL(pair.init(ObPLCodeGenerator::goto_label_flag::GOTO_LABEL_CG,
                                     std::pair<ObLLVMBasicBlock, ObLLVMBasicBlock>(stack_save_block, label_block)))) {
          LOG_WARN("init label block pair failed.", K(ret));
        } else if (OB_FAIL(get_goto_label_map().set_refactored(stmt.get_stmt_id(), pair))) {
          LOG_WARN("set label block failed", K(ret));
        } else {}
      } else if (OB_SUCCESS == tmp_ret) {
        ObLLVMBasicBlock &stack_save_block = pair.second.first;
        ObLLVMBasicBlock &goto_block = pair.second.second;
        if (OB_FAIL(get_helper().create_br(stack_save_block))) {
          LOG_WARN("failed to create_br", K(ret));
        } else if (OB_FAIL(get_helper().set_insert_point(stack_save_block))) {
          LOG_WARN("failed to set insert point", K(ret));
        } else if (OB_FAIL(set_current(stack_save_block))) {
          LOG_WARN("failed to set current block", K(ret));
        } else if (OB_FAIL(get_helper().stack_save(stack))) {
          LOG_WARN("failed to save stack", K(ret));
        } else if (OB_FAIL(get_helper().create_br(goto_block))) {
          LOG_WARN("failed to create_br", K(ret));
        } else if (OB_FAIL(get_helper().set_insert_point(goto_block))) {
          LOG_WARN("failed to set insert point", K(ret));
        } else if (OB_FAIL(set_current(goto_block))) {
          LOG_WARN("failed to set current block", K(ret));
        } else if (OB_FAIL(get_helper().stack_restore(stack))) {
          LOG_WARN("failed to restore stack", K(ret));
        } else {
          pair.first = ObPLCodeGenerator::goto_label_flag::GOTO_LABEL_CG;
          if (OB_FAIL(get_goto_label_map().set_refactored(stmt.get_stmt_id(), pair, true))) {
            LOG_WARN("set label block failed", K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to label block", K(ret));
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPLCodeGenerator::generate_destruct_obj(const ObPLStmt &s, ObLLVMValue &src_datum)
{
  int ret = OB_SUCCESS;
  ObSEArray<jit::ObLLVMValue, 2> args;

  OZ (args.push_back(get_vars()[CTX_IDX]));
  OZ (args.push_back(src_datum));
  if (OB_SUCC(ret)) {
    jit::ObLLVMValue ret_err;
    if (OB_FAIL(get_helper().create_call(ObString("spi_destruct_obj"), get_spi_service().spi_destruct_obj_, args, ret_err))) {
      LOG_WARN("failed to create call", K(ret));
    } else if (OB_FAIL(check_success(ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()))) {
      LOG_WARN("failed to check success", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLCodeGenerator::generate_out_param(
  const ObPLStmt &s, const ObIArray<InOutParam> &param_desc, ObLLVMValue &params, int64_t i)
{
  int ret = OB_SUCCESS;
  ObLLVMType obj_param_type;
  ObLLVMType obj_param_type_pointer;
  ObLLVMValue pp_arg;
  ObLLVMValue p_arg;
  OZ (get_adt_service().get_objparam(obj_param_type));
  OZ (obj_param_type.get_pointer_to(obj_param_type_pointer));
  // 获取输出结果
  OZ (extract_arg_from_argv(params, i, pp_arg));
  OZ (get_helper().create_load("load_out_arg_pointer", pp_arg, p_arg));
  OZ (get_helper().create_int_to_ptr(
    ObString("cast_pointer_to_arg"), p_arg, obj_param_type_pointer, p_arg));

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX != param_desc.at(i).out_idx_) { // 处理本地变量
    //存储进全局符号表和param store
    ObLLVMValue result;
    ObLLVMValue p_param;
    ObPLDataType pl_type = s.get_variable(param_desc.at(i).out_idx_)->get_type();
    if (pl_type.is_composite_type() || pl_type.is_cursor_type()) {
      if (param_desc.at(i).is_out()) {
        // 对于INOUT参数, execute immediate复杂类型传递的是指针, 什么都不需要做; inner call场景, inout参数会入参会深拷，这里需要重新拷回
        // 对于OUT参数, 复杂类型构造了新的ObjParam, 这里进行COPY;
        if (PL_CALL == s.get_type() &&
            static_cast<const ObPLCallStmt *>(&s)->get_nocopy_params().count() > i &&
            OB_INVALID_INDEX != static_cast<const ObPLCallStmt *>(&s)->get_nocopy_params().at(i) &&
            !param_desc.at(i).is_pure_out()) {
            // inner call nocopy的inout参数传递是指针, 无需重新拷贝
        } else {
          ObLLVMValue into_address;
          ObLLVMValue allocator;
          ObLLVMValue src_datum;
          ObLLVMValue dest_datum;
          OZ (extract_objparam_from_context(
            get_vars().at(CTX_IDX), param_desc.at(i).out_idx_, into_address));
          if (pl_type.is_collection_type()) {
            ObLLVMValue dest_collection;
            OZ (extract_extend_from_objparam(into_address, pl_type, dest_collection));
            OZ (extract_allocator_from_collection(dest_collection, allocator));
          } else {
            OZ (generate_null(ObIntType, allocator));
          }
          OZ (extract_obobj_ptr_from_objparam(into_address, dest_datum));
          OZ (extract_obobj_ptr_from_objparam(p_arg, src_datum));
          OZ (pl_type.generate_copy(*this,
                                    *(s.get_namespace()),
                                    allocator,
                                    src_datum,
                                    dest_datum,
                                    s.get_block()->in_notfound(),
                                    s.get_block()->in_warning(),
                                    OB_INVALID_ID));
          OZ (generate_destruct_obj(s, src_datum));
        }
      }
    } else { //处理基础类型的出参
      ObSEArray<ObLLVMValue, 4> args;
      ObLLVMValue result_idx;
      ObLLVMValue ret_err;
      ObLLVMValue p_result_obj;
      ObLLVMValue need_set;
      OZ (get_helper().get_int64(param_desc.at(i).out_idx_, result_idx));
      OZ (generate_new_objparam(p_result_obj));
      OZ (get_helper().get_int8(true, need_set));
      OZ (args.push_back(get_vars().at(CTX_IDX)));
      OZ (args.push_back(p_arg));
      OZ (args.push_back(result_idx));
      OZ (args.push_back(p_result_obj));
      OZ (args.push_back(need_set));
      OZ (get_helper().create_call(ObString("spi_convert_objparam"),
                                   get_spi_service().spi_convert_objparam_,
                                   args,
                                   ret_err));
      OZ (check_success(
        ret_err, s.get_stmt_id(), s.get_block()->in_notfound(), s.get_block()->in_warning()));
      OZ (extract_datum_from_objparam(p_result_obj, pl_type.get_obj_type(), result));
      OZ (get_helper().create_store(
        result, get_vars().at(param_desc.at(i).out_idx_ + USER_ARG_OFFSET)),
        param_desc.at(i).out_idx_);
    }
  } else { // 处理外部变量(Sys Var/User Var or PKG Basic Variables or Subprogram Basic Variables)
    const ObRawExpr *expr = NULL;
    CK (OB_NOT_NULL(expr = s.get_expr(param_desc.at(i).param_)));
    if (OB_FAIL(ret)) {
    } else if (expr->is_sys_func_expr()) {
      OZ (generate_set_variable(param_desc.at(i).param_,
                                p_arg,
                                T_DEFAULT == expr->get_expr_type(),
                                s.get_stmt_id(),
                                s.get_block()->in_notfound(),
                                s.get_block()->in_warning()));
    } else if (expr->is_obj_access_expr()) {
      ObLLVMValue address;
      ObLLVMValue src_datum;
      ObLLVMValue dest_datum;
      ObLLVMValue allocator;
      ObPLDataType final_type;
      const ObObjAccessRawExpr *obj_access = NULL;
      uint64_t package_id = OB_INVALID_ID;
      uint64_t var_idx = OB_INVALID_ID;
      CK (OB_NOT_NULL(obj_access = static_cast<const ObObjAccessRawExpr *>(expr)));
      if (OB_SUCC(ret)
          && ObObjAccessIdx::is_package_variable(obj_access->get_access_idxs())) {
        OZ (ObObjAccessIdx::get_package_id(obj_access, package_id, &var_idx));
      }
      OZ (generate_null(ObIntType, allocator));
      CK (OB_NOT_NULL(obj_access));
      OZ (generate_expr(param_desc.at(i).param_, s, OB_INVALID_INDEX, address));
      OZ (obj_access->get_final_type(final_type));
      OZ (generate_check_not_null(s, final_type.get_not_null(), p_arg));
      if (final_type.is_obj_type()) {
        OZ (extract_datum_ptr_from_objparam(
          p_arg, obj_access->get_result_type().get_type(), src_datum));
        OZ (extract_extend_from_objparam(address, final_type, dest_datum));
        OZ (final_type.generate_copy(*this,
                                     *(s.get_namespace()),
                                     allocator,
                                     src_datum,
                                     dest_datum,
                                     s.get_block()->in_notfound(),
                                     s.get_block()->in_warning(),
                                     package_id));
      } else {
        OZ (extract_obobj_ptr_from_objparam(p_arg, src_datum));
        OZ (extract_obobj_ptr_from_objparam(address, dest_datum));
        OZ (final_type.generate_copy(*this,
                                    *(s.get_namespace()),
                                    allocator,
                                    src_datum,
                                    dest_datum,
                                    s.get_block()->in_notfound(),
                                    s.get_block()->in_warning(),
                                    package_id));
        if (OB_FAIL(ret)) {
        } else if (PL_CALL == s.get_type()) {
          const ObPLCallStmt *call_stmt = static_cast<const ObPLCallStmt *>(&s);
          if (call_stmt->get_nocopy_params().count() > i &&
              OB_INVALID_INDEX != call_stmt->get_nocopy_params().at(i) &&
              !param_desc.at(i).is_pure_out()) {
            // inner call nocopy的inout参数传递是指针, 无需释放
          } else {
            OZ (generate_destruct_obj(s, src_datum));
          }
        } else if (PL_EXECUTE == s.get_type()) {
          OZ (generate_destruct_obj(s, src_datum));
        }
      }
      if (OB_SUCC(ret) && package_id != OB_INVALID_ID && var_idx != OB_INVALID_ID) {
        OZ (generate_update_package_changed_info(s, package_id, var_idx));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid expr", K(i), K(*expr), K(ret));
    }
  }
  return ret;
}

int ObPLCodeGenerator::generate_out_params(
  const ObPLStmt &s, const ObIArray<InOutParam> &param_desc, ObLLVMValue &params)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> nocopy_params;
  if (PL_CALL == s.get_type()) {
    const ObPLCallStmt *call_stmt = static_cast<const ObPLCallStmt*>(&s);
    OZ (nocopy_params.assign(call_stmt->get_nocopy_params()));
    CK (nocopy_params.count() == 0 || nocopy_params.count() == param_desc.count());
  }
  // 先处理NoCopy的参数
  for (int64_t i = 0; OB_SUCC(ret) && i < nocopy_params.count(); ++i) {
    if (nocopy_params.at(i) != OB_INVALID_INDEX && param_desc.at(i).is_out()) {
      OZ (generate_out_param(s, param_desc, params, i));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param_desc.count(); ++i) {
    // 处理输出参数的情况, NoCopy参数已经处理过了, 处理非NoCopy的参数
    if (nocopy_params.count() > 0 && nocopy_params.at(i) != OB_INVALID_INDEX) {
      // do nothing...
    } else if (param_desc.at(i).is_out()) {
      OZ (generate_out_param(s, param_desc, params, i));
    }
  }
  reset_out_params();
  return ret;
}

int ObPLCodeGenerator::generate(ObPLPackage &pl_package)
{
  int ret = OB_SUCCESS;

  OZ (prepare_external());
  OZ (prepare_local_user_type());
  OZ (prepare_expression(pl_package));
  OZ (generate_obj_access_expr());

  if (OB_SUCC(ret)) {
#ifndef NDEBUG
    LOG_INFO("================Original LLVM Module================", K(debug_mode_));
    helper_.dump_module();
#endif

    // set optimize_level to 1 if in debug mode, otherwise use PLSQL_OPTIMIZE_LEVEL in exec_env
    int64_t optimize_level = debug_mode_ ? 1 : pl_package.get_exec_env().get_plsql_optimize_level();

    OZ (helper_.verify_module(), pl_package);
    OZ (helper_.compile_module(static_cast<jit::ObPLOptLevel>(optimize_level)));
  }

  OZ (final_expression(pl_package));
  return ret;
}

int ObPLCodeGenerator::generate(ObPLFunction &pl_func)
{
  int ret = OB_SUCCESS;
  ObPLFunctionAST &ast = static_cast<ObPLFunctionAST&>(ast_);
  if (debug_mode_
      || profile_mode_
      || !ast.get_is_all_sql_stmt()
      || !ast_.get_obj_access_exprs().empty()) {
    OZ (generate_normal(pl_func));
  } else {
    OZ (generate_simple(pl_func));
  }
  LOG_TRACE("generate pl function",
    K(debug_mode_), K(ast.get_is_all_sql_stmt()), K(ast.get_obj_access_exprs().empty()));
  return ret;
}

int ObPLCodeGenerator::generate_simple(ObPLFunction &pl_func)
{
  int ret = OB_SUCCESS;
  ObPLFunctionAST &ast = static_cast<ObPLFunctionAST&>(ast_);
  common::ObFixedArray<ObPLSqlInfo, common::ObIAllocator> &sql_infos = pl_func.get_sql_infos();
  CK (!debug_mode_);
  CK (ast.get_is_all_sql_stmt());
  OZ (prepare_expression(pl_func));
  OZ (final_expression(pl_func));
  OZ (pl_func.set_variables(get_ast().get_symbol_table()));
  OZ (pl_func.get_dependency_table().assign(get_ast().get_dependency_table()));
  OZ (pl_func.add_members(get_ast().get_flag()));
  OX (pl_func.set_pipelined(get_ast().get_pipelined()));
  OX (pl_func.set_action((uint64_t)(&ObPL::simple_execute)));
  OX (pl_func.set_can_cached(get_ast().get_can_cached()));
  OX (pl_func.set_is_all_sql_stmt(get_ast().get_is_all_sql_stmt()));
  OX (pl_func.set_has_parallel_affect_factor(get_ast().has_parallel_affect_factor()));

  OX (sql_infos.set_capacity(static_cast<uint32_t>(ast.get_sql_stmts().count())));
  for (int64_t i = 0; OB_SUCC(ret) && i < ast.get_sql_stmts().count(); ++i) {
    const ObPLSqlStmt *sql_stmt = ast.get_sql_stmts().at(i);
    ObPLSqlInfo sql_info(pl_func.get_allocator());
    CK (OB_NOT_NULL(sql_stmt));
    OZ (sql_info.generate(*sql_stmt, pl_func.get_expressions()));
    OZ (sql_infos.push_back(sql_info));
  }
  if (OB_SUCC(ret) && ObTriggerInfo::is_trigger_body_package_id(pl_func.get_package_id())) {
    OZ (pl_func.set_types(get_ast().get_user_type_table()));
  }

  return ret;
}

int ObPLCodeGenerator::generate_normal(ObPLFunction &pl_func)
{
  int ret = OB_SUCCESS;
  // 初始化符号表
  for (int64_t i = 0;
      OB_SUCC(ret) && i < get_ast().get_symbol_table().get_count() + USER_ARG_OFFSET + 1;
      ++i) {
    ObLLVMValue dummy_value;
    if (OB_FAIL(vars_.push_back(dummy_value))) {
      LOG_WARN("failed to push back dummy value", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_prototype())) {
    LOG_WARN("failed to generate a pointer", K(ret));
  } else if (OB_FAIL(func_.set_personality(get_eh_service().eh_personality_))) {
    LOG_WARN("failed to set_personality", K(ret));
  } else if (OB_FAIL(helper_.create_block(ObString("entry"), func_, entry_))) {
    LOG_WARN("failed to create block", K(ret));
  } else if (OB_FAIL(helper_.create_block(ObString("exit"), func_, exit_))) {
    LOG_WARN("failed to create block", K(ret));
  } else if (OB_FAIL(set_current(entry_))) {
    LOG_WARN("failed to set current", K(ret));
  } else if (OB_FAIL(generate_di_prototype())) {
    LOG_WARN("failed to generate di prototype", K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret)) {
    ObPLCodeGenerateVisitor visitor(*this);
    if (OB_ISNULL(get_ast().get_body())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl body is NULL", K(ret));
    } else if (OB_FAIL(helper_.set_insert_point(entry_))) {
      LOG_WARN("failed set_insert_point", K(ret));
    } else if (OB_FAIL(set_debug_location(*get_ast().get_body()))) {
      LOG_WARN("failed to set debug location", K(ret));
    } else if (OB_FAIL(init_argument())) {
      LOG_WARN("failed to init augument", K(ret));
    } else if (OB_FAIL(prepare_external())) {
      LOG_WARN("failed to prepare external", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(prepare_local_user_type())) {
      LOG_WARN("failed to prepare local user type", K(ret));
    } else if (OB_FAIL(prepare_expression(pl_func))) {
      LOG_WARN("failed to prepare expression", K(ret));
    } else if (OB_FAIL(prepare_subprogram(pl_func))) {
      LOG_WARN("failed to prepare subprogram", K(ret));
    } else if (OB_FAIL(SMART_CALL(set_profiler_unit_info_recursive(pl_func)))) {
      LOG_WARN("failed to set profiler unit id recursively", K(ret), K(pl_func.get_routine_table()));
    } else if (OB_FAIL(generate_spi_pl_profiler_before_record(*get_ast().get_body()))) {
      LOG_WARN("failed to generate spi profiler before record call", K(ret), K(*get_ast().get_body()));
    } else if (OB_FAIL(SMART_CALL(visitor.generate(*get_ast().get_body())))) {
      LOG_WARN("failed to generate a pl body", K(ret));
    } else if (OB_FAIL(generate_spi_pl_profiler_after_record(*get_ast().get_body()))) {
      LOG_WARN("failed to generate spi profiler after record call", K(ret), K(*get_ast().get_body()));
    }
  }

  if (OB_SUCC(ret)) {
    if (get_ast().get_ret_type().is_obj_type()
        && (OB_ISNULL(get_ast().get_ret_type().get_data_type())
            || get_ast().get_ret_type().get_data_type()->get_meta_type().is_invalid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("return type is invalid", K(func_), K(ret));
    } else if (!current_.is_terminated()) { //如果当前block没有终止符，强制跳转
      if (OB_FAIL(finish_current(exit_))) {
        LOG_WARN("failed to finish_current", K(ret));
      }
    } else { /*do nothing*/ }

    if (OB_SUCC(ret)) {
      ObLLVMValue ret_value;
      if (OB_FAIL(helper_.set_insert_point(exit_))) {
        LOG_WARN("failed to set_insert_point", K(ret));
      } else if (OB_FAIL(helper_.create_load(ObString("load_ret"), vars_.at(RET_IDX), ret_value))) {
        LOG_WARN("failed to create_load", K(ret));
      } else if (OB_FAIL(helper_.create_ret(ret_value))) {
        LOG_WARN("failed to create_ret", K(ret));
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_obj_access_expr())) {
      LOG_WARN("generate obj access expr failed", K(ret));
    } else if (debug_mode_ && OB_FAIL(di_helper_.finalize())) {
      LOG_WARN("failed to finalize", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
#ifndef NDEBUG
    LOG_INFO("================Original================", K(pl_func), K(debug_mode_));
    helper_.dump_module();
#endif

    // set optimize_level to 1 if in debug mode, otherwise use PLSQL_OPTIMIZE_LEVEL in exec_env
    int64_t optimize_level = debug_mode_ ? 1 : pl_func.get_exec_env().get_plsql_optimize_level();

    OZ (helper_.verify_module(), pl_func);
    OZ (helper_.compile_module(static_cast<jit::ObPLOptLevel>(optimize_level)));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(final_expression(pl_func))) {
      LOG_WARN("generate obj access expr failed", K(ret));
    } else if (OB_FAIL(pl_func.set_variables(get_ast().get_symbol_table()))) {
      LOG_WARN("failed to set variables", K(get_ast().get_symbol_table()), K(ret));
    } else if (OB_FAIL(pl_func.get_dependency_table().assign(get_ast().get_dependency_table()))) {
      LOG_WARN("failed to set ref objects", K(get_ast().get_dependency_table()), K(ret));
    } else if (OB_FAIL(pl_func.set_types(get_ast().get_user_type_table()))) {
      LOG_WARN("failed to set types", K(ret));
    } else {
      pl_func.add_members(get_ast().get_flag());
      pl_func.set_pipelined(get_ast().get_pipelined());
      pl_func.set_action(helper_.get_function_address(get_ast().get_name()));
            pl_func.set_can_cached(get_ast().get_can_cached());
      pl_func.set_is_all_sql_stmt(get_ast().get_is_all_sql_stmt());
      pl_func.set_has_parallel_affect_factor(get_ast().has_parallel_affect_factor());
    }
  }
  if (debug_mode_) {
    OX (helper_.dump_debuginfo());
    OZ (pl_func.set_variables_debuginfo(get_ast().get_symbol_debuginfo_table()));
    OZ (pl_func.set_name_debuginfo(get_ast()));
  }
  OX (helper_.final());
  debug_mode_ = false;
  return ret;
}

int ObPLCodeGenerator::extract_meta_ptr_from_obj(ObLLVMValue &p_obj, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(0))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"), p_obj,
                                        indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_type_ptr_from_obj(ObLLVMValue &p_obj, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  OZ (extract_meta_ptr_from_obj(p_obj, p_meta));
  OZ (helper_.create_gep(ObString("extract_type_pointer"), p_meta, 0, result));
  return ret;
}

int ObPLCodeGenerator::extract_meta_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 3> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(0))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"), p_objparam, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_accuracy_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(1))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"), p_objparam, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_param_flag_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(3))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"),
                                        p_objparam, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_raw_text_pos_ptr_from_objparam(ObLLVMValue &p_objparam,
                                                              ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(4))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"),
                                        p_objparam, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_raw_text_len_ptr_from_objparam(ObLLVMValue &p_objparam,
                                                              ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 2> indices;
  if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(5))) {
    LOG_WARN("push_back error", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_meta_pointer"), p_objparam, indices, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_type_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  OZ (extract_meta_ptr_from_objparam(p_objparam, p_meta));
  OZ (helper_.create_gep(ObString("extract_type_pointer"), p_meta, 0, result));
  return ret;
}

int ObPLCodeGenerator::extract_cslevel_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  if (OB_FAIL(extract_meta_ptr_from_objparam(p_objparam, p_meta))) {
    LOG_WARN("faled to extract_meta_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_scale_pointer"), p_meta, 1, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_cstype_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  if (OB_FAIL(extract_meta_ptr_from_objparam(p_objparam, p_meta))) {
    LOG_WARN("faled to extract_meta_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_scale_pointer"), p_meta, 2, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_scale_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  if (OB_FAIL(extract_meta_ptr_from_objparam(p_objparam, p_meta))) {
    LOG_WARN("faled to extract_meta_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_scale_pointer"), p_meta, 3, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_flag_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_meta;
  if (OB_FAIL(extract_param_flag_ptr_from_objparam(p_objparam, p_meta))) {
    LOG_WARN("faled to extract_meta_ptr_from_objparam", K(ret));
  } else if (OB_FAIL(helper_.create_gep(ObString("extract_scale_pointer"), p_meta, 1, result))) {
    LOG_WARN("failed to create gep", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_obobj_ptr_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  OZ (helper_.create_gep(ObString("extract_obj_pointer"), p_objparam, 0, result));
  return ret;
}

int ObPLCodeGenerator::extract_obobj_from_objparam(ObLLVMValue &p_objparam, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_obj;
  if (OB_FAIL(extract_obobj_ptr_from_objparam(p_objparam, p_obj))) {
    LOG_WARN("failed to create gep", K(ret));
  } else if (OB_FAIL(helper_.create_load(ObString("load_value"), p_obj, result))) {
    LOG_WARN("failed to create load", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_datum_ptr_from_objparam(ObLLVMValue &p_objparam, ObObjType type, ObLLVMValue &result)
{
  UNUSED(type);
  int ret = OB_SUCCESS;
  ObLLVMValue datum_addr;
  ObLLVMType datum_type;
  ObLLVMType datum_pointer_type;
/*  if (ob_is_string_tc(type)
      || ob_is_number_tc(type)
      || ob_is_text_tc(type)
      || ob_is_otimestampe_tc(type)
      || ob_is_raw_tc(type)) {*/
    ObSEArray<int64_t, 3> indices;
    if (OB_FAIL(indices.push_back(0)) || OB_FAIL(indices.push_back(0))) {
      LOG_WARN("push_back error", K(ret));
    } else if (OB_FAIL(helper_.create_gep(ObString("extract_int64_pointer"), p_objparam, indices, datum_addr))) {
      LOG_WARN("failed to create gep", K(ret));
    } else if (OB_FAIL(adt_service_.get_obj(datum_type))) {
      LOG_WARN("failed to get argv type", K(ret));
    } else if (OB_FAIL(datum_type.get_pointer_to(datum_pointer_type))) {
      LOG_WARN("failed to get pointer to", K(ret));
    } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_addr_to_datum"), datum_addr, datum_pointer_type, result))) {
      LOG_WARN("failed to create bit cast", K(ret));
    } else { /*do nothing*/ }
/*  } else {
    if (OB_FAIL(extract_value_ptr_from_objparam(p_objparam, type, result))) {
      LOG_WARN("failed to extract value from objparam", K(ret));
    }
  }*/
  return ret;
}

int ObPLCodeGenerator::cast_to_int64(ObLLVMValue &p_value)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_value_int64, value_int64, value;
  ObLLVMType llvm_type, pointer_type;
  OZ (helper_.get_llvm_type(ObIntType, llvm_type));
  OZ (llvm_type.get_pointer_to(pointer_type));
  OZ (helper_.create_load(ObString("load value"), p_value, value));
  OZ (helper_.create_sext(ObString("sext to int64"), value, llvm_type, value_int64));
// #ifndef NDEBUG
  // OZ (generate_debug(ObString("after sext value to int64"), value_int64));
// #endif
  OZ (helper_.create_bit_cast(ObString("bitcast"), p_value, pointer_type, p_value_int64));
  OZ (helper_.create_store(value_int64, p_value_int64));
  return ret;
}

int ObPLCodeGenerator::extract_value_ptr_from_obj(ObLLVMValue &p_obj, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_obj_int64;
  ObLLVMType llvm_type;
  ObLLVMType pointer_type;
  if (OB_FAIL(helper_.create_gep(ObString("extract_int64_pointer"), p_obj, 2, p_obj_int64))) {
    LOG_WARN("failed to create gep", K(ret));
  } else {
    switch (type) {
    case ObNullType: {
      ObLLVMValue false_value;
      OZ (helper_.get_llvm_type(ObTinyIntType, llvm_type));
      OZ (llvm_type.get_pointer_to(pointer_type));
      OZ (helper_.create_bit_cast(ObString("cast_int64_to_int8"), p_obj_int64, pointer_type, result));
      OZ (helper_.get_int8(false, false_value));
      OZ (helper_.create_store(false_value, result));
    }
      break;
    case ObTinyIntType:
    case ObUTinyIntType: {
      if (OB_FAIL(helper_.get_llvm_type(ObTinyIntType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_int8"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create bit cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObSmallIntType:
    case ObUSmallIntType: {
      if (OB_FAIL(helper_.get_llvm_type(ObSmallIntType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_int16"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create bit cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObMediumIntType:
    case ObInt32Type:
    case ObUMediumIntType:
    case ObUInt32Type: {
      if (OB_FAIL(helper_.get_llvm_type(ObInt32Type, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_int32"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create bit cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObIntType:
    case ObUInt64Type:
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObYearType:
    case ObBitType:
    case ObEnumType:
    case ObSetType: {
      result = p_obj_int64;
    }
      break;
    case ObFloatType:
    case ObUFloatType: {
      if (OB_FAIL(helper_.get_llvm_type(ObFloatType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_float"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create bit cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObDoubleType:
    case ObUDoubleType: {
      if (OB_FAIL(helper_.get_llvm_type(ObDoubleType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_double"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create bit cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObNumberType:
    case ObUNumberType:
    case ObNumberFloatType: {
      if (OB_FAIL(helper_.get_llvm_type(ObNumberType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_number"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create_bit_cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObNVarchar2Type:
    case ObNCharType:
    case ObVarcharType:
    case ObCharType:
    case ObHexStringType:
    case ObURowIDType: {
      if (OB_FAIL(helper_.get_llvm_type(ObCharType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_char"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create_bit_cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObExtendType: {
      if (OB_FAIL(helper_.get_llvm_type(ObExtendType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_extend"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create_bit_cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObUnknownType: {
      if (OB_FAIL(helper_.get_llvm_type(ObUnknownType, llvm_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(llvm_type.get_pointer_to(pointer_type))) {
        LOG_WARN("failed to get pointer to", K(ret));
      } else if (OB_FAIL(helper_.create_bit_cast(ObString("cast_int64_to_unknown"), p_obj_int64, pointer_type, result))) {
        LOG_WARN("failed to create_bit_cast", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType://TODO: not support now, @yanhua
    case ObRawType: //TODO: not support now, @xiaofeng
    case ObIntervalYMType: //TODO: not support now, @jim.wjh
    case ObIntervalDSType: //TODO: not support now, @jim.wjh
    default: {
      if (OB_FAIL(generate_null_pointer(ObIntType, result))) {
        LOG_WARN("failed to get pointer to", K(ret));
      }
    }
      break;
    }
  }
  return ret;
}

int ObPLCodeGenerator::extract_value_from_obj(jit::ObLLVMValue &p_obj,
                                              ObObjType type,
                                              jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result_ptr;
  OZ (extract_value_ptr_from_obj(p_obj, type, result_ptr));
  OZ (helper_.create_load(ObString("load_value"), result_ptr, result));
  return ret;
}

int ObPLCodeGenerator::extract_value_ptr_from_objparam(ObLLVMValue &p_objparam, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_obj;
  if (OB_FAIL(helper_.create_gep(ObString("extract_obj_pointer"), p_objparam, 0, p_obj))) {
    LOG_WARN("failed to create gep", K(ret));
  } else if (OB_FAIL(extract_value_ptr_from_obj(p_obj, type, result))) {
    LOG_WARN("failed to extract_value_ptr_from_obj", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_datum_from_objparam(ObLLVMValue &p_objparam, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result_ptr;
  if (OB_FAIL(extract_datum_ptr_from_objparam(p_objparam, type, result_ptr))) {
    LOG_WARN("failed to create gep", K(ret));
  } else if (OB_FAIL(helper_.create_load(ObString("load_datum"), result_ptr, result))) {
    LOG_WARN("failed to create load", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_value_from_objparam(ObLLVMValue &p_objparam, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue result_ptr;
  if (OB_FAIL(extract_value_ptr_from_objparam(p_objparam, type, result_ptr))) {
    LOG_WARN("failed to create gep", K(ret));
  } else if (OB_FAIL(helper_.create_load(ObString("load_value"), result_ptr, result))) {
    LOG_WARN("failed to create load", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_extend_from_objparam(ObLLVMValue &p_objparam, const ObPLDataType &type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue extend;
  ObLLVMType llvm_type;
  ObLLVMType addr_type;
  if (OB_FAIL(extract_value_from_objparam(p_objparam, ObExtendType, extend))) {
    LOG_WARN("failed to extract_value_from_objparam", K(ret));
  } else if (OB_FAIL(get_llvm_type(type, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(llvm_type.get_pointer_to(addr_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_int_to_ptr(ObString("cast_extend_to_ptr"), extend, addr_type, result))) {
    LOG_WARN("failed to create_bit_cast", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_extend_from_obj(ObLLVMValue &p_obj,
                                               const ObPLDataType &type,
                                               ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue extend;
  ObLLVMType llvm_type;
  ObLLVMType addr_type;
  if (OB_FAIL(extract_value_from_obj(p_obj, ObExtendType, extend))) {
    LOG_WARN("failed to extract_value_from_objparam", K(ret));
  } else if (OB_FAIL(get_llvm_type(type, llvm_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(llvm_type.get_pointer_to(addr_type))) {
    LOG_WARN("failed to get_llvm_type", K(ret));
  } else if (OB_FAIL(helper_.create_int_to_ptr(ObString("cast_extend_to_ptr"),
                                               extend,
                                               addr_type,
                                               result))) {
    LOG_WARN("failed to create_bit_cast", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_obj_ptr_from_result(jit::ObLLVMValue &p_objparam,
                                                   jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue extend;
  ObLLVMType obj_type;
  ObLLVMType p_obj_type;
  OZ (extract_value_from_obj(p_objparam, ObExtendType, extend));
  OZ (adt_service_.get_obj(obj_type));
  OZ (obj_type.get_pointer_to(p_obj_type));
  OZ (helper_.create_int_to_ptr(ObString("cast_extend_to_obj_ptr"), extend, p_obj_type, result));
  return ret;
}

int ObPLCodeGenerator::extract_objparam_from_store(ObLLVMValue &p_param_store, const int64_t idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  const static int64_t BlocksIDX = 3;
  const static int64_t SEArrayDataIDX = 1;
  ObLLVMValue param_store;
  ObLLVMValue blocks;
  ObLLVMValue p_blocks_carray;
  ObLLVMValue p_obj_block;
  ObLLVMValue obj_block;
  ObLLVMValue p_obj_param;
  ObLLVMValue result_tmp_p;
  ObLLVMType obj_param_type;
  ObLLVMType p_obj_param_type;
  ObArray<int64_t> extract_obj_param_idxes;
  ObArray<int64_t> extract_block_addr_idxes;

  if (OB_FAIL(helper_.create_load(ObString("load_param_store"),
                                  p_param_store,
                                  param_store))) {
    LOG_WARN("failed to create load", K(ret));
  } else if (OB_FAIL(helper_.create_extract_value(ObString("extract_blocks"),
                                                  param_store, BlocksIDX,
                                                  blocks))) {
    LOG_WARN("failed to create extract value", K(ret));
  } else if (OB_FAIL(helper_.create_extract_value(ObString("extract_blocks_pointer"),
                                                  blocks, SEArrayDataIDX,
                                                  p_blocks_carray))) {
    LOG_WARN("failed to create extract value", K(ret));
  } else {
    const int64_t block_obj_num = ParamStore::BLOCK_CAPACITY;
    const int64_t block_idx = idx / block_obj_num;
    const int64_t obj_in_block_idx = idx % block_obj_num;
    if (OB_FAIL(extract_block_addr_idxes.push_back(block_idx))
        || OB_FAIL(extract_block_addr_idxes.push_back(0))) {
      LOG_WARN("failed to push back element", K(ret));
    } else if (OB_FAIL(helper_.create_gep(ObString("extract_block_pointer"),
                                          p_blocks_carray,
                                          extract_block_addr_idxes, p_obj_block))) {
      LOG_WARN("failed to create gep", K(ret));
    } else if (OB_FAIL(helper_.create_load(ObString("load_obj_block"),
                                           p_obj_block, obj_block))) {
      LOG_WARN("failed to create_load", K(ret));
    } else if (OB_FAIL(adt_service_.get_objparam(obj_param_type))) {
      LOG_WARN("failed to get_objparam", K(ret));
    } else if (OB_FAIL(obj_param_type.get_pointer_to(p_obj_param_type))) {
      LOG_WARN("failed to get_pointer_to", K(ret));
    } else if (OB_FAIL(helper_.create_int_to_ptr(ObString("cast_block_to_objparam_p"),
                                                 obj_block,
                                                 p_obj_param_type, p_obj_param))) {
      LOG_WARN("failed to create_int_to_ptr cast", K(ret));
    } else if (OB_FAIL(extract_obj_param_idxes.push_back(obj_in_block_idx))
               || OB_FAIL(extract_obj_param_idxes.push_back(0))) {
      LOG_WARN("failed to push back element", K(ret));
    } else if (OB_FAIL(helper_.create_gep(ObString("extract_objparam"),
                                          p_obj_param,
                                          extract_obj_param_idxes,
                                          result_tmp_p))) {
      LOG_WARN("failed to create gep", K(ret));
    } else if (OB_FAIL(helper_.create_bit_cast(ObString("bitcast"),
                                               result_tmp_p, p_obj_param_type, result))) {
      LOG_WARN("failed to cast obj pointer to objparam pointer", K(ret));
    }
  }
  return ret;
}

#define DEFINE_EXTRACT_CONTEXT_ELEM(item, idx) \
int ObPLCodeGenerator::extract_##item##_from_context(jit::ObLLVMValue &p_pl_exex_ctx, jit::ObLLVMValue &result) \
{ \
  int ret = OB_SUCCESS; \
  ObLLVMValue pl_exex_ctx; \
  OZ (helper_.create_load(ObString("load_pl_exex_ctx"), p_pl_exex_ctx, pl_exex_ctx)); \
  OZ (helper_.create_extract_value(ObString("extract_"#item), pl_exex_ctx, idx, result)); \
  return ret; \
}

DEFINE_EXTRACT_CONTEXT_ELEM(allocator, IDX_PLEXECCTX_ALLOCATOR)
DEFINE_EXTRACT_CONTEXT_ELEM(param_store, IDX_PLEXECCTX_PARAMS)
DEFINE_EXTRACT_CONTEXT_ELEM(result, IDX_PLEXECCTX_RESULT)
DEFINE_EXTRACT_CONTEXT_ELEM(status, IDX_PLEXECCTX_STATUS)
DEFINE_EXTRACT_CONTEXT_ELEM(pl_ctx, IDX_PLEXECCTX_PL_CTX)
DEFINE_EXTRACT_CONTEXT_ELEM(pl_function, IDX_PLEXECCTX_FUNC)

int ObPLCodeGenerator::extract_objparam_from_context(ObLLVMValue &p_pl_exex_ctx, int64_t idx, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_param_store;
  if (OB_FAIL(extract_param_store_from_context(p_pl_exex_ctx, p_param_store))) {
    LOG_WARN("failed to extract_param_store_from_context", K(ret));
  } else if (OB_FAIL(extract_objparam_from_store(p_param_store, idx, result))) {
    LOG_WARN("failed to extract_objparam_from_store", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_value_from_context(ObLLVMValue &p_pl_exex_ctx, int64_t idx, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_objparam;
  if (OB_FAIL(extract_objparam_from_context(p_pl_exex_ctx, idx, p_objparam))) {
    LOG_WARN("failed to extract_param_store_from_context", K(ret));
  } else if (OB_FAIL(extract_value_from_objparam(p_objparam, type, result))) {
    LOG_WARN("failed to extract_objparam_from_store", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_datum_from_context(ObLLVMValue &p_pl_exex_ctx, int64_t idx, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_objparam;
  if (OB_FAIL(extract_objparam_from_context(p_pl_exex_ctx, idx, p_objparam))) {
    LOG_WARN("failed to extract_param_store_from_context", K(ret));
  } else if (OB_FAIL(extract_datum_from_objparam(p_objparam, type, result))) {
    LOG_WARN("failed to extract_objparam_from_store", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_arg_from_argv(ObLLVMValue &p_argv, int64_t idx, ObLLVMValue &result)
{
  return helper_.create_gep(ObString("extract_arg"), p_argv, idx, result);
}

int ObPLCodeGenerator::extract_objparam_from_argv(jit::ObLLVMValue &p_argv,
                                                  const int64_t idx,
                                                  jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue pp_arg;
  ObLLVMValue p_arg;
  ObLLVMType objparam;
  ObLLVMType pointer_type;
  if (OB_FAIL(extract_arg_from_argv(p_argv, idx, pp_arg))) {
    LOG_WARN("failed to create load", K(ret));
  } else if (OB_FAIL(helper_.create_load(ObString("load_arg"), pp_arg, p_arg))) {
    LOG_WARN("failed to create load", K(ret));
  } else if (OB_FAIL(adt_service_.get_objparam(objparam))) {
    LOG_WARN("failed to get argv type", K(ret));
  } else if (OB_FAIL(objparam.get_pointer_to(pointer_type))) {
    LOG_WARN("failed to get pointer to", K(ret));
  } else if (OB_FAIL(helper_.create_int_to_ptr(ObString("cast_arg_to_pointer"), p_arg,
                                               pointer_type, result))) {
    LOG_WARN("failed to create bit cast", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_datum_from_argv(ObLLVMValue &p_argv,
                                               int64_t idx,
                                               ObObjType type,
                                               ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_objparam;
  if (OB_FAIL(extract_objparam_from_argv(p_argv, idx, p_objparam))) {
    LOG_WARN("failed to create load", K(ret));
  } else if (OB_FAIL(extract_datum_from_objparam(p_objparam, type, result))) {
    LOG_WARN("push_back error", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_value_from_argv(ObLLVMValue &p_argv, const int64_t idx, ObObjType type, ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue pp_arg;
  ObLLVMValue p_arg;
  ObLLVMValue p_objparam;
  ObLLVMType objparam;
  ObLLVMType pointer_type;
  if (OB_FAIL(extract_objparam_from_argv(p_argv, idx, p_objparam))) {
    LOG_WARN("failed to create load", K(ret));
  } else if (OB_FAIL(extract_value_from_objparam(p_objparam, type, result))) {
    LOG_WARN("push_back error", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObPLCodeGenerator::extract_notnull_from_record(jit::ObLLVMValue &p_record, int64_t idx,
                                                   jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_result;
  OZ (extract_notnull_ptr_from_record(p_record, idx, p_result));
  OZ (helper_.create_load(ObString("load_record_elem"), p_result, result));
  return ret;
}

int ObPLCodeGenerator::extract_notnull_ptr_from_record(jit::ObLLVMValue &p_record,
                                                       int64_t idx,
                                                       jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  return helper_.create_gep(ObString("extract_record_elem"),
                            p_record,
                            RECORD_META_OFFSET + idx,
                            result);
}

int ObPLCodeGenerator::extract_meta_from_record(jit::ObLLVMValue &p_record,
                                                   int64_t member_cnt,
                                                   int64_t idx,
                                                   jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_result;
  OZ (extract_meta_ptr_from_record(p_record, member_cnt, idx, p_result));
  OZ (helper_.create_load(ObString("load_record_elem"), p_result, result));
  return ret;
}

int ObPLCodeGenerator::extract_meta_ptr_from_record(jit::ObLLVMValue &p_record,
                                                       int64_t member_cnt,
                                                       int64_t idx,
                                                       jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  return helper_.create_gep(ObString("extract_record_elem"),
                            p_record,
                            RECORD_META_OFFSET + member_cnt + idx,
                            result);
}

int ObPLCodeGenerator::extract_element_from_record(jit::ObLLVMValue &p_record,
                                                   int64_t member_cnt,
                                                   int64_t idx,
                                                   jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_result;
  OZ (extract_element_ptr_from_record(p_record, member_cnt, idx, p_result));
  OZ (helper_.create_load(ObString("load_record_elem"), p_result, result));
  return ret;
}

int ObPLCodeGenerator::extract_element_ptr_from_record(jit::ObLLVMValue &p_record,
                                                       int64_t member_cnt,
                                                       int64_t idx,
                                                       jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  return helper_.create_gep(ObString("extract_record_elem"),
                            p_record,
                            RECORD_META_OFFSET + member_cnt + member_cnt + idx,
                            result);
}

#define DEFINE_EXTRACT_PTR_FROM_STRUCT(item, s, idx) \
int ObPLCodeGenerator::extract_##item##_ptr_from_##s(jit::ObLLVMValue &p_struct, jit::ObLLVMValue &result) \
{ \
  return helper_.create_gep(ObString("extract_"#item), p_struct, idx, result); \
}

#define DEFINE_EXTRACT_VALUE_FROM_STRUCT(item, s) \
int ObPLCodeGenerator::extract_##item##_from_##s(jit::ObLLVMValue &p_struct, jit::ObLLVMValue &result) \
{ \
  int ret = OB_SUCCESS; \
  ObLLVMValue p_result; \
  OZ (extract_##item##_ptr_from_##s(p_struct, p_result)); \
  OZ (helper_.create_load(ObString("load_"#item), p_result, result)); \
  return ret; \
}

DEFINE_EXTRACT_PTR_FROM_STRUCT(type, condition_value, IDX_CONDITION_TYPE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(code, condition_value, IDX_CONDITION_CODE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(name, condition_value, IDX_CONDITION_STATE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(len, condition_value, IDX_CONDITION_LEN)
DEFINE_EXTRACT_PTR_FROM_STRUCT(stmt, condition_value, IDX_CONDITION_STMT)
DEFINE_EXTRACT_PTR_FROM_STRUCT(signal, condition_value, IDX_CONDITION_SIGNAL)

DEFINE_EXTRACT_VALUE_FROM_STRUCT(type, condition_value)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(code, condition_value)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(name, condition_value)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(len, condition_value)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(stmt, condition_value)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(signal, condition_value)


DEFINE_EXTRACT_PTR_FROM_STRUCT(type, collection, IDX_COLLECTION_TYPE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(id, collection, IDX_COLLECTION_ID)
DEFINE_EXTRACT_PTR_FROM_STRUCT(isnull, collection, IDX_COLLECTION_ISNULL)
DEFINE_EXTRACT_PTR_FROM_STRUCT(allocator, collection, IDX_COLLECTION_ALLOCATOR)
DEFINE_EXTRACT_PTR_FROM_STRUCT(element, collection, IDX_COLLECTION_ELEMENT)
DEFINE_EXTRACT_PTR_FROM_STRUCT(count, collection, IDX_COLLECTION_COUNT)
DEFINE_EXTRACT_PTR_FROM_STRUCT(first, collection, IDX_COLLECTION_FIRST)
DEFINE_EXTRACT_PTR_FROM_STRUCT(last, collection, IDX_COLLECTION_LAST)
DEFINE_EXTRACT_PTR_FROM_STRUCT(data, collection, IDX_COLLECTION_DATA)

DEFINE_EXTRACT_VALUE_FROM_STRUCT(type, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(id, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(isnull, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(allocator, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(element, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(count, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(first, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(last, collection)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(data, collection)

DEFINE_EXTRACT_PTR_FROM_STRUCT(capacity, varray, IDX_VARRAY_CAPACITY)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(capacity, varray)

DEFINE_EXTRACT_PTR_FROM_STRUCT(type, record, IDX_RECORD_TYPE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(id, record, IDX_RECORD_ID)
DEFINE_EXTRACT_PTR_FROM_STRUCT(isnull, record, IDX_RECORD_ISNULL)
DEFINE_EXTRACT_PTR_FROM_STRUCT(count, record, IDX_RECORD_COUNT)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(type, record)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(id, record)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(isnull, record)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(count, record)

DEFINE_EXTRACT_PTR_FROM_STRUCT(type, elemdesc, IDX_ELEMDESC_TYPE)
DEFINE_EXTRACT_PTR_FROM_STRUCT(notnull, elemdesc, IDX_ELEMDESC_NOTNULL)
DEFINE_EXTRACT_PTR_FROM_STRUCT(field_count, elemdesc, IDX_ELEMDESC_FIELD_COUNT)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(type, elemdesc)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(notnull, elemdesc)
DEFINE_EXTRACT_VALUE_FROM_STRUCT(field_count, elemdesc)


DEFINE_EXTRACT_VALUE_FROM_STRUCT(notnull, collection)

int ObPLCodeGenerator::extract_notnull_ptr_from_collection(jit::ObLLVMValue &p_collection,
                                                           jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_element;
  OZ (extract_element_ptr_from_collection(p_collection, p_element));
  OZ (extract_notnull_ptr_from_elemdesc(p_element, result));
  return ret;
}

DEFINE_EXTRACT_VALUE_FROM_STRUCT(field_count, collection)

int ObPLCodeGenerator::extract_field_count_ptr_from_collection(jit::ObLLVMValue &p_collection,
                                                               jit::ObLLVMValue &result)
{
  int ret = OB_SUCCESS;
  ObLLVMValue p_element;
  OZ (extract_element_ptr_from_collection(p_collection, p_element));
  OZ (extract_field_count_ptr_from_elemdesc(p_element, result));
  return ret;
}

int ObPLCodeGenerator::generate_handle_ref_cursor(const ObPLCursor *cursor, const ObPLStmt &s,
                                                  bool is_notfound, bool in_warning)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(cursor));
  // 以下一些情况不要关闭cursor:
  // function (cur out sys_refcursor), out类型参数
  // dup cursor 没有被init，所以不需要关闭，见generate_declare_cursor
  // 直接使用subprog 外部的cursor(不是ref cursor)，也不要关闭，例如
  /*
  * DECLARE
    CURSOR c (job VARCHAR2, max_sal NUMBER) IS
      SELECT employee_name, (salary - max_sal) overpayment FROM emp4 WHERE
      job_id = job AND salary > max_sal ORDER BY salary;
    PROCEDURE print_overpaid IS
      employee_name_ emp4.employee_name%TYPE;
      overpayment_ emp4.salary%TYPE;
    BEGIN
      LOOP
        FETCH c INTO employee_name_, overpayment_; //这儿直接使用了外部cursor
        EXIT WHEN c%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE(employee_name_ || ' (by ' || overpayment_ || ')');
        INSERT INTO test2 VALUES(employee_name_, TO_CHAR(overpayment_));
      END LOOP;
    END print_overpaid;
  */
  bool is_pkg_cursor = false;
  if (OB_SUCC(ret)) {
    // 定义在package spec中的才是package id，这种cursor的routine id是无效的
    // 有些定义在package函数中的cursor，它的package id也是有效的，routine id也是有效的。
    is_pkg_cursor = OB_INVALID_ID != cursor->get_package_id()
                 && OB_INVALID_ID == cursor->get_routine_id();
  }
  OX (LOG_DEBUG("generate handle ref cursor", K(cursor->get_state()), K(is_pkg_cursor),
                           K(cursor->get_package_id()), K(cursor->get_routine_id()),K(*cursor)));
  if (OB_SUCC(ret) && (pl::ObPLCursor::CursorState::PASSED_IN != cursor->get_state()
                    && pl::ObPLCursor::CursorState::DUP_DECL != cursor->get_state()
                    && !is_pkg_cursor)
                    && cursor->get_routine_id() == s.get_namespace()->get_routine_id()) {

#ifndef NDEBUG
          {
            ObLLVMValue line_num;
            OZ (get_helper().get_int64(s.get_stmt_id(), line_num));
            OZ (generate_debug(ObString("close cursor line number"), line_num));
          }
#endif
    ObSEArray<ObLLVMValue, 6> args;
    ObLLVMValue ret_err;
    ObLLVMValue arg_value;
    OZ (args.push_back(get_vars().at(CTX_IDX)));
    OZ (get_helper().get_int64(cursor->get_package_id(), arg_value));
    OZ (args.push_back(arg_value));
    OZ (get_helper().get_int64(cursor->get_routine_id(), arg_value));
    OZ (args.push_back(arg_value));
    OZ (get_helper().get_int64(cursor->get_index(), arg_value));
    OZ (args.push_back(arg_value));
    OZ (get_helper().get_int64(-1, arg_value));
    OZ (args.push_back(arg_value));
    OZ (get_helper().create_call(ObString("spi_handle_ref_cursor_ref_count"),
                              get_spi_service().spi_handle_ref_cursor_refcount_,
                              args, ret_err));
    OZ (check_success(ret_err, s.get_stmt_id(), is_notfound, in_warning));
  }
  return ret;
}

int ObPLCodeGenerator::restart_cg_when_goto_dest(const ObPLStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_current().get_v())) {
    // do nothing
  } else if (stmt.get_is_goto_dst()) {
    ObLLVMBasicBlock goto_dst_blk;
    OZ (get_helper().create_block(ObString("restart_goto_block"), get_func(), goto_dst_blk));
    OZ (set_current(goto_dst_blk));
  }
  return ret;
}

int ObPLCodeGenerator::generate_spi_pl_profiler_before_record(const ObPLStmt &s)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(get_current().get_v()) && profile_mode_) {
    ObSEArray<ObLLVMValue, 4> args;
    ObLLVMValue ret_err;
    ObLLVMValue value;

    int64_t line = s.get_line() + 1;
    int64_t level = s.get_level();

    if (OB_FAIL(args.push_back(get_vars().at(CTX_IDX)))) {
      LOG_WARN("failed to push back CTX_IDX", K(ret));
    } else if (OB_FAIL(get_helper().get_int64(line, value))) {
      LOG_WARN("failed to get line# value", K(ret), K(line));
    } else if (OB_FAIL(args.push_back(value))) {
      LOG_WARN("failed to push back line#", K(ret), K(line));
    } else if (OB_FAIL(get_helper().get_int64(level, value))) {
      LOG_WARN("failed to get stmt level value", K(ret), K(level));
    } else if (OB_FAIL(args.push_back(value))) {
      LOG_WARN("failed to push back stmt level", K(ret), K(level));
    } else if (OB_FAIL(get_helper().create_call(ObString("spi_pl_profiler_before_record"),
                                                get_spi_service().spi_pl_profiler_before_record_,
                                                args,
                                                ret_err))) {
      LOG_WARN("failed to create spi_pl_profiler_before_record call", K(ret), K(line), K(level));
    } else if (OB_FAIL(check_success(ret_err,s.get_stmt_id(),
                                       OB_NOT_NULL(s.get_block()) ? s.get_block()->in_notfound() : false,
                                       OB_NOT_NULL(s.get_block()) ? s.get_block()->in_warning() : false))) {
      LOG_WARN("failed to check spi_pl_profiler_before_record success", K(ret), K(line), K(level));
    }
  }

  return ret;
}

int ObPLCodeGenerator::generate_spi_pl_profiler_after_record(const ObPLStmt &s)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(get_current().get_v()) && profile_mode_) {
    ObSEArray<ObLLVMValue, 4> args;
    ObLLVMValue ret_err;
    ObLLVMValue value;

    int64_t line = s.get_line() + 1;
    int64_t level = s.get_level();

    if (OB_FAIL(args.push_back(get_vars().at(CTX_IDX)))) {
      LOG_WARN("failed to push back CTX_IDX", K(ret));
    } else if (OB_FAIL(get_helper().get_int64(line, value))) {
      LOG_WARN("failed to get line# value", K(ret), K(line));
    } else if (OB_FAIL(args.push_back(value))) {
      LOG_WARN("failed to push back line#", K(ret), K(line));
    } else if (OB_FAIL(get_helper().get_int64(level, value))) {
      LOG_WARN("failed to get stmt level value", K(ret), K(level));
    } else if (OB_FAIL(args.push_back(value))) {
      LOG_WARN("failed to push back stmt level", K(ret), K(level));
    } else if (OB_FAIL(get_helper().create_call(ObString("spi_pl_profiler_after_record"),
                                                get_spi_service().spi_pl_profiler_after_record_,
                                                args,
                                                ret_err))) {
      LOG_WARN("failed to create spi_pl_profiler_after_record call", K(ret), K(line), K(level));
    } else if (OB_FAIL(check_success(ret_err,s.get_stmt_id(),
                                       OB_NOT_NULL(s.get_block()) ? s.get_block()->in_notfound() : false,
                                       OB_NOT_NULL(s.get_block()) ? s.get_block()->in_warning() : false))) {
      LOG_WARN("failed to check spi_pl_profiler_after_record success", K(ret), K(line), K(level));
    }
  }

  return ret;
}

} // namespace pl
} // namespace oceanbase
