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

#ifndef _OB_RAW_EXPR_DEDUCE_TYPE_H
#define _OB_RAW_EXPR_DEDUCE_TYPE_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_type_demotion.h"
#include "lib/container/ob_iarray.h"
#include "lib/udt/ob_collection_type.h"
#include "common/ob_accuracy.h"
#include "share/ob_i_sql_expression.h"
#include "ob_raw_expr_util.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObRawExprDeduceType: public ObRawExprVisitor
{
public:
  ObRawExprDeduceType(const ObSQLSessionInfo *my_session,
                      ObRawExprFactory *expr_factory,
                      bool solidify_session_vars,
                      const ObLocalSessionVar *local_vars,
                      int64_t local_vars_id,
                      ObRawExprTypeDemotion &type_demotion)
    : ObRawExprVisitor(),
      my_session_(my_session),
      alloc_(),
      expr_factory_(expr_factory),
      my_local_vars_(local_vars),
      local_vars_id_(local_vars_id),
      solidify_session_vars_(solidify_session_vars),
      type_demotion_(type_demotion)
  {}
  virtual ~ObRawExprDeduceType()
  {
    alloc_.reset();
  }
  int deduce(ObRawExpr &expr);
  /// interface of ObRawExprVisitor
  virtual int visit(ObConstRawExpr &expr);
  virtual int visit(ObVarRawExpr &expr);
  virtual int visit(ObOpPseudoColumnRawExpr &expr);
  virtual int visit(ObExecParamRawExpr &expr);
  virtual int visit(ObQueryRefRawExpr &expr);
  virtual int visit(ObColumnRefRawExpr &expr);
  virtual int visit(ObOpRawExpr &expr);
  virtual int visit(ObCaseOpRawExpr &expr);
  virtual int visit(ObAggFunRawExpr &expr);
  virtual int visit(ObSysFunRawExpr &expr);
  virtual int visit(ObSetOpRawExpr &expr);
  virtual int visit(ObAliasRefRawExpr &expr);
  virtual int visit(ObWinFunRawExpr &expr);
  virtual int visit(ObPseudoColumnRawExpr &expr);
  virtual int visit(ObUDFRawExpr &expr);
  virtual int visit(ObPlQueryRefRawExpr &expr);
  virtual int visit(ObMatchFunRawExpr &expr);

  int add_implicit_cast(ObOpRawExpr &parent, const ObCastMode &cast_mode);
  int add_implicit_cast(ObCaseOpRawExpr &parent, const ObCastMode &cast_mode);
  int add_implicit_cast(ObAggFunRawExpr &parent, const ObCastMode &cast_mode);

  int check_type_for_case_expr(ObCaseOpRawExpr &case_expr, common::ObIAllocator &alloc);
  static bool skip_cast_expr(const ObRawExpr &parent, const int64_t child_idx);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprDeduceType);
  // function members
  int check_lob_param_allowed(const common::ObObjType from,
                              const common::ObCollationType from_cs_type,
                              const common::ObObjType to,
                              const common::ObCollationType to_cs_type,
                              ObExprOperatorType expr_type);
  int push_back_types(const ObRawExpr *param_expr, ObExprResTypes &types);
  int calc_result_type(ObNonTerminalRawExpr &expr, ObIExprResTypes &types,
                       common::ObCastMode &cast_mode, int32_t row_dimension);
  template<typename RawExprType>
  int construct_collecton_attr_expr(RawExprType &expr);
  template<typename RawExprType>
  int add_attr_exprs(const ObCollectionTypeBase *coll_meta, RawExprType &expr);
  int calc_result_type_with_const_arg(
    ObNonTerminalRawExpr &expr,
    ObIExprResTypes &types,
    common::ObExprTypeCtx &type_ctx,
    ObExprOperator *op,
    ObExprResType &result_type,
    int32_t row_dimension);
  int check_expr_param(ObOpRawExpr &expr);
  int check_row_param(ObOpRawExpr &expr);
  int check_param_expr_op_row(ObRawExpr *param_expr, int64_t column_count);
  int visit_left_param(ObRawExpr &expr);
  //观察右操作符的参数个数，由于要知道左操作符参数个数，所以传入根操作符
  int visit_right_param(ObOpRawExpr &expr);
  int64_t get_expr_output_column(const ObRawExpr &expr);
  int get_row_expr_param_type(const ObRawExpr &expr, ObIExprResTypes &types);
  int deduce_type_visit_for_special_func(int64_t param_index, const ObRawExpr &expr, ObIExprResTypes &types);
  // init udf expr
  int init_normal_udf_expr(ObNonTerminalRawExpr &expr, ObExprOperator *op);
  // get agg udf result type
  int set_agg_udf_result_type(ObAggFunRawExpr &expr);

  int set_agg_group_concat_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);
  int set_json_agg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type, bool &need_add_cast);
  int set_asmvt_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);
  int set_rb_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);
  int set_agg_json_array_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);

  int set_agg_min_max_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type,
                                  bool &need_add_cast);
  int set_agg_regr_result_type(ObAggFunRawExpr &expr, ObExprResType &result_type);
  int set_xmlagg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);

  int set_agg_xmlagg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);
  int set_array_agg_result_type(ObAggFunRawExpr &expr, ObExprResType& result_type);

  // helper functions for add_implicit_cast
  int add_implicit_cast_for_op_row(ObRawExpr *&child_ptr,
                                   const common::ObIArray<ObExprResType> &input_types,
                                   const ObCastMode &cast_mode);
  // try add cast expr on subquery stmt's output && update column types.
  int add_implicit_cast_for_subquery(ObQueryRefRawExpr &expr,
                                     const common::ObIArray<ObExprResType> &input_types,
                                     const ObCastMode &cast_mode);
  // try add cast expr above %child_idx child,
  // %input_type.get_calc_meta() is the destination type!
  template<typename RawExprType>
  int try_add_cast_expr(RawExprType &parent, int64_t child_idx,
                        const ObExprResType &input_type, const ObCastMode &cast_mode);

  // try add cast expr above %expr , set %new_expr to &expr if no cast added.
  // %input_type.get_calc_meta() is the destination type!
  int try_add_cast_expr_above_for_deduce_type(ObRawExpr &expr, ObRawExpr *&new_expr,
                                              const ObExprResType &input_type,
                                              const ObCastMode &cm);
  int check_group_aggr_param(ObAggFunRawExpr &expr);
  int check_group_rank_aggr_param(ObAggFunRawExpr &expr);
  int check_median_percentile_param(ObAggFunRawExpr &expr);
  int add_median_percentile_implicit_cast(ObAggFunRawExpr &expr,
                                          const ObCastMode& cast_mode,
                                          const bool keep_type);
  int add_group_aggr_implicit_cast(ObAggFunRawExpr &expr, const ObCastMode& cast_mode);
  int adjust_cast_as_signed_unsigned(ObSysFunRawExpr &expr);

  bool ignore_scale_adjust_for_decimal_int(const ObItemType expr_type);
  int try_replace_casts_with_questionmarks_ora(ObRawExpr *row_expr);

  int try_replace_cast_with_questionmark_ora(ObRawExpr &parent, ObRawExpr *cast_expr, int param_idx);
  int build_subschema_for_enum_set_type(ObRawExpr &expr);
private:
  const sql::ObSQLSessionInfo *my_session_;
  common::ObArenaAllocator alloc_;
  ObRawExprFactory *expr_factory_;
  //deduce with current session vars if solidify_session_vars_ is true,
  //otherwise deduce with my_local_vars_ if my_local_vars_ is not null
  const ObLocalSessionVar *my_local_vars_;
  int64_t local_vars_id_;
  bool solidify_session_vars_;
  ObRawExprTypeDemotion &type_demotion_;
  // data members
};

template<typename RawExprType>
int ObRawExprDeduceType::try_add_cast_expr(RawExprType &parent,
                                           int64_t child_idx,
                                           const ObExprResType &input_type,
                                           const ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child_ptr = NULL;
  if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "my_session_ is NULL", K(ret));
  } else if (OB_UNLIKELY(parent.get_param_count() <= child_idx)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "child_idx is invalid", K(ret), K(parent.get_param_count()), K(child_idx));
  } else if (OB_ISNULL(child_ptr = parent.get_param_expr(child_idx))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "child_ptr raw expr is NULL", K(ret));
  } else {
    ObRawExpr *new_expr = NULL;
  // 本函数原本被定义在CPP中，【因为UNITY合并编译单元的作用，而通过了编译，但模版代码的实现需要在头文件中定义】，因此关闭UNITY后导致observer无法通过编译
  // 为解决关闭UNITY后的编译问题，将其挪至头文件中
  // 但本函数使用了OZ、CK宏，这两个宏内部的log打印使用了LOG_WARN，要求必须定义USING_LOG_PREFIX
  // 由于这里是头文件，这将导致非常棘手的问题：
  // 1. 如果在本头文件之前没有定义USING_LOG_PREFIX，则必须重新定义USING_LOG_PREFIX（但宏被定义在头文件中将造成污染）
  // 2. 如果是在本文件中新定义的USING_LOG_PREFIX，则需要被清理掉，防止污染被传播到其他.h以及cpp中
  // 因此这里判断USING_LOG_PREFIX是否已定义，若已定义则放弃重新定义（这意味着日志并不总是被以“SQL_RESV”标识打印），同时也定义特殊标识
  // 若发现定义特殊标识，则在预处理过程中执行宏清理动作
  // 整个逻辑相当trick，是为了尽量少的修改代码逻辑，代码owner后续需要整改这里的逻辑
#ifndef USING_LOG_PREFIX
#define MARK_MACRO_DEFINED_BY_OB_RAW_EXPR_DEDUCE_TYPE_H
#define USING_LOG_PREFIX SQL_RESV
#endif
    OZ(try_add_cast_expr_above_for_deduce_type(*child_ptr, new_expr, input_type,
                                               cast_mode));
    CK(NULL != new_expr);
    if (OB_SUCC(ret) && child_ptr != new_expr) { // cast expr added
      ObObjTypeClass ori_tc = ob_obj_type_class(child_ptr->get_data_type());
      ObObjTypeClass expect_tc = ob_obj_type_class(input_type.get_calc_type());
      if (T_FUN_UDF == parent.get_expr_type()
          && ObNumberTC == ori_tc
          && ((ObTextTC == expect_tc && lib::is_oracle_mode()) || ObLobTC == expect_tc)) {
        // oracle mode can not cast number to text, but mysql mode can
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        SQL_RESV_LOG(WARN, "cast to lob type not allowed", K(ret));
      }

      // for consistent with mysql, if const cast as json, should regard as scalar, don't need parse
      if (ObStringTC == ori_tc && ObJsonTC == expect_tc && IS_JSON_COMPATIBLE_OP(parent.get_expr_type())) {
        uint64_t extra = new_expr->get_extra();
        new_expr->set_extra(CM_SET_SQL_AS_JSON_SCALAR(extra));
      }
      OZ(parent.replace_param_expr(child_idx, new_expr));
#ifdef MARK_MACRO_DEFINED_BY_OB_RAW_EXPR_DEDUCE_TYPE_H
#undef USING_LOG_PREFIX
#endif
      if (OB_FAIL(ret) && my_session_->is_varparams_sql_prepare()) {
        ret = OB_SUCCESS;
        SQL_RESV_LOG(DEBUG, "ps prepare phase ignores type deduce error");
      }
      //add local vars to cast expr
      if (OB_SUCC(ret)) {
        if (solidify_session_vars_) {
          if (OB_FAIL(new_expr->set_local_session_vars(NULL, my_session_, local_vars_id_))) {
            SQL_RESV_LOG(WARN, "fail to set session vars", K(ret), KPC(new_expr));
          }
        } else if (NULL != my_local_vars_) {
          if (OB_FAIL(new_expr->set_local_session_vars(my_local_vars_, NULL, local_vars_id_))) {
            SQL_RESV_LOG(WARN, "fail to set local vars", K(ret), KPC(new_expr));
          }
        }
      }
    }
  }
  return ret;
};


template<typename RawExprType>
int ObRawExprDeduceType::add_attr_exprs(const ObCollectionTypeBase *coll_meta, RawExprType &expr)
{
  int ret = OB_SUCCESS;
  ObItemType expr_type = T_REF_COLUMN;
  if (coll_meta->type_id_ == ObNestedType::OB_ARRAY_TYPE || coll_meta->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    ObColumnRefRawExpr *attr_expr = NULL;
    const ObCollectionArrayType *arr_meta = static_cast<const ObCollectionArrayType*>(coll_meta);
    if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_NULL_BITMAP,
                                                 attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_OFFSETS,
                                                 attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to create offset attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
    } else if (OB_FAIL(add_attr_exprs(arr_meta->element_type_, expr))) {
      SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
    }
  } else if (coll_meta->type_id_ == ObNestedType::OB_BASIC_TYPE) {
    ObColumnRefRawExpr *attr_expr = NULL;
    const ObCollectionBasicType *elem_type = static_cast<const ObCollectionBasicType*>(coll_meta);
    if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_NULL_BITMAP,
                                                 attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
    } else if (!is_fixed_length(elem_type->basic_meta_.get_obj_type())) {
      if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                 expr_type, ArrayAttr::ATTR_OFFSETS,
                                                 attr_expr))) {
        SQL_RESV_LOG(WARN, "failed to create nullbitmap attr expr", K(ret));
      } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
        SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                        expr_type, ArrayAttr::ATTR_DATA,
                                                        attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to create nullbitmap attr expr", K(ret));
    } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
      SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
    }
  }
  return ret;
}

template<typename RawExprType>
int ObRawExprDeduceType::construct_collecton_attr_expr(RawExprType &expr)
{
  int ret = OB_SUCCESS;
  uint16_t subschema_id = expr.get_result_type().get_subschema_id();
  ObExecContext *exec_ctx = const_cast<ObExecContext *>(my_session_->get_cur_exec_ctx());
  ObSubSchemaValue value;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObItemType expr_type = expr.get_expr_type();
  bool need_set_values = expr.get_enum_set_values().empty();
  bool need_construct_attrs = true;
  if (expr.get_attr_count() > 0) {
    // attrs constructed already, do nothing
    need_construct_attrs = false;
  } else if (expr.is_const_expr()) {
    // is uniform format, do nothing
    need_construct_attrs = false;
  }
  if (!need_set_values && !need_construct_attrs) {
    // do nothing
  } else if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "exec ctx is null", K(ret));
  } else if (OB_ISNULL(expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpected null raw expr", K(ret));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(subschema_id, value))) {
    SQL_RESV_LOG(WARN, "failed to get subschema ctx", K(ret));
  } else if (OB_ISNULL(value.value_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "subschema is null", K(ret));
  } else {
    coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_);
    ObCollectionTypeBase *coll_meta = coll_info->collection_meta_;
    ObColumnRefRawExpr *attr_expr = NULL;
    if (coll_meta->type_id_ != ObNestedType::OB_ARRAY_TYPE && coll_meta->type_id_ != ObNestedType::OB_VECTOR_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "unexpected meta type", K(ret), K(coll_meta->type_id_));
    } else if (OB_ISNULL(coll_meta)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "subschema is null", K(ret));
    } else if (need_construct_attrs) {
      if (OB_FAIL(ObRawExprUtils::create_attr_expr(expr_factory_, my_session_,
                                                          T_REF_COLUMN, ArrayAttr::ATTR_LENGTH,
                                                          attr_expr))) {
        SQL_RESV_LOG(WARN, "failed to create nullbitmap attr expr", K(ret));
      } else if (OB_FAIL(expr.add_attr_expr(attr_expr))) {
        SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
      } else if (OB_FAIL(add_attr_exprs(reinterpret_cast<ObCollectionArrayType *>(coll_meta)->element_type_, expr))) {
        SQL_RESV_LOG(WARN, "failed to add attr expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_set_values) {
      ObString def = coll_info->get_def_string();
      ObSEArray<ObString, 1> enum_set_values;
      if (OB_FAIL(enum_set_values.push_back(def))) {
        SQL_RESV_LOG(WARN, "failed to push back array", K(ret));
      } else if (OB_FAIL(expr.set_enum_set_values(enum_set_values))) {
        SQL_RESV_LOG(WARN, "failed to set values", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_DEDUCE_TYPE_H */
