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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/expr/ob_raw_expr_deduce_type.h"
#include "sql/resolver/expr/ob_expr_relation_analyzer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "common/object/ob_object.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "sql/engine/expr/ob_expr_udf.h"
#include "sql/engine/expr/ob_expr_pl_get_cursor_attr.h"
#include "sql/engine/expr/ob_expr_pl_sqlcode_sqlerrm.h"
#include "sql/engine/expr/ob_expr_plsql_variable.h"
#include "sql/engine/expr/ob_expr_collection_construct.h"
#include "sql/engine/expr/ob_expr_object_construct.h"
#include "sql/engine/expr/ob_pl_expr_subquery.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/engine/expr/ob_expr_sql_udt_construct.h"
#include "sql/engine/expr/ob_expr_priv_attribute_access.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::jit::expr;

namespace oceanbase
{
namespace sql
{
ObRawExpr *USELESS_POINTER = NULL;

#define GENERATE_CASE(type, class)                                      \
  case type: {                                                          \
    class *dst = NULL;                                                  \
    if (OB_FAIL(create_raw_expr_inner(type, dst))) {                    \
      SQL_RESV_LOG(WARN, "failed to create raw expr", K(ret), K(type)); \
    } else {                                                            \
      raw_expr = dst;                                                   \
    }                                                                   \
  } break;
#define GENERATE_DEFAULT()                                              \
  default: {                                                            \
    if (OB_FAIL(create_raw_expr_inner(expr_type, raw_expr))) {          \
      SQL_RESV_LOG(WARN, "failed to create raw expr", K(ret), K(expr_type)); \
    }                                                                   \
  }

template <>
int ObRawExprFactory::create_raw_expr<ObSysFunRawExpr>(ObItemType expr_type, ObSysFunRawExpr *&raw_expr)
{
  int ret = OB_SUCCESS;
  switch (expr_type) {
    GENERATE_CASE(T_FUN_SYS_SEQ_NEXTVAL, ObSequenceRawExpr);
    GENERATE_CASE(T_FUN_NORMAL_UDF, ObNormalDllUdfRawExpr);
    GENERATE_CASE(T_FUN_PL_COLLECTION_CONSTRUCT, ObCollectionConstructRawExpr);
    GENERATE_CASE(T_FUN_UDF, ObUDFRawExpr);
    GENERATE_CASE(T_FUN_PL_OBJECT_CONSTRUCT, ObObjectConstructRawExpr);
    GENERATE_CASE(T_FUN_PL_GET_CURSOR_ATTR, ObPLGetCursorAttrRawExpr);
    GENERATE_CASE(T_FUN_PL_SQLCODE_SQLERRM, ObPLSQLCodeSQLErrmRawExpr);
    GENERATE_CASE(T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT, ObUDTConstructorRawExpr);
    GENERATE_CASE(T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS, ObUDTAttributeAccessRawExpr);
    GENERATE_DEFAULT();
  }
  return ret;
}
template<>
int ObRawExprFactory::create_raw_expr<ObOpRawExpr>(ObItemType expr_type, ObOpRawExpr *&raw_expr)
{
  int ret = OB_SUCCESS;
  switch (expr_type) {
    GENERATE_CASE(T_FUN_PL_INTEGER_CHECKER, ObPLIntegerCheckerRawExpr);
    GENERATE_CASE(T_SP_CPARAM, ObCallParamRawExpr);
    GENERATE_CASE(T_FUN_PL_ASSOCIATIVE_INDEX, ObPLAssocIndexRawExpr);
    GENERATE_CASE(T_OBJ_ACCESS_REF, ObObjAccessRawExpr);
    GENERATE_CASE(T_OP_MULTISET, ObMultiSetRawExpr);
    GENERATE_CASE(T_OP_COLL_PRED, ObCollPredRawExpr);
    GENERATE_DEFAULT();
  }
  return ret;
}

#undef GENERATE_CASE
#undef GENERATE_DEFAULT

void ObQualifiedName::format_qualified_name()
{
  if (access_idents_.count() == 1) {
    col_name_ = access_idents_.at(0).access_name_;
  }
  if (access_idents_.count() == 2) {
    tbl_name_ = access_idents_.at(0).access_name_;
    col_name_ = access_idents_.at(1).access_name_;
  }
  if (access_idents_.count() == 3) {
    database_name_ = access_idents_.at(0).access_name_;
    tbl_name_ = access_idents_.at(1).access_name_;
    col_name_ = access_idents_.at(2).access_name_;
  }
}

void ObQualifiedName::format_qualified_name(ObNameCaseMode mode)
{
  UNUSED(mode); //TODO: @ryan.ly @yuming.wyc
  bool maybe_column = !is_sys_func() && !is_pl_udf() && !is_dll_udf() && !is_pl_var() && !is_udf_return_access();
  for (int64_t i = 0; maybe_column && i < access_idents_.count(); ++i) {
    if (access_idents_.at(i).access_name_.empty() || access_idents_.at(i).access_index_ != OB_INVALID_INDEX) {
      maybe_column = false;
    }
  }
  if (maybe_column && access_idents_.count() == 1) {
    col_name_ = access_idents_.at(0).access_name_;
  }
  if (maybe_column && access_idents_.count() == 2) {
    tbl_name_ = access_idents_.at(0).access_name_;
    col_name_ = access_idents_.at(1).access_name_;
  }
  if (maybe_column && access_idents_.count() == 3) {
    database_name_ = access_idents_.at(0).access_name_;
    tbl_name_ = access_idents_.at(1).access_name_;
    col_name_ = access_idents_.at(2).access_name_;
  }
}

int ObQualifiedName::replace_access_ident_params(ObRawExpr *from, ObRawExpr *to)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idents_.count(); ++i) {
    OZ (access_idents_.at(i).replace_params(from, to));
  }
  return ret;
}

int ObObjAccessIdent::check_param_num() const
{
  return (ObRawExpr::EXPR_SYS_FUNC == sys_func_expr_->get_expr_class())
      ? static_cast<ObSysFunRawExpr*>(sys_func_expr_)->check_param_num()
        : OB_SUCCESS;
}

int ObObjAccessIdent::extract_params(int64_t level, common::ObIArray<ObRawExpr*> &params) const
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params_.count(); ++i) {
    if (params_.at(i).second == level && OB_FAIL(params.push_back(params_.at(i).first))) {
      LOG_WARN("push back error", K(ret));
    }
  }
  return ret;
}

int ObObjAccessIdent::replace_params(ObRawExpr *from, ObRawExpr *to)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params_.count(); ++i) {
    OZ (ObRawExprUtils::replace_ref_column(params_.at(i).first, from, to));
  }
  return ret;
}

int OrderItem::deep_copy(ObIRawExprCopier &copier,
                         const OrderItem &other)
{
  int ret = common::OB_SUCCESS;
  order_type_ = other.order_type_;
  expr_ = other.expr_;
  if (OB_FAIL(copier.copy(expr_))) {
    LOG_WARN("failed to copy expr when copying OrderItem", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObVarInfo, type_, name_);

int ObVarInfo::deep_copy(common::ObIAllocator &allocator, ObVarInfo &var_info) const
{
  int ret = OB_SUCCESS;
  ObString clone_name;
  if (OB_FAIL(ob_write_string(allocator, name_, clone_name))) {
    LOG_WARN("fail to write string", K(name_), K(*this), K(ret));
  } else {
    var_info.type_ = type_;
    var_info.name_ = clone_name;
  }
  return ret;
}

ObRawExpr::~ObRawExpr()
{
  if (0x86427531 == magic_num_) {
    LOG_ERROR_RET(OB_ERROR, "ObRawExpr maybe double free!");
  }
  magic_num_ = 0x86427531;
}
//这个函数用来判断这个表达式本身或者参数是否含有非常量表达式
//常量表达式的定义是在plan中需要特定的operator产生的表达式
//我们约定ObBinaryRefExpr必须由TableScan产生
//ObAggFunRawExpr必须由Groupby operator产生
//ObSetOpRawExpr必须由Set operator产生
//ObUnaryRefExpr(即subquery ref)必须由SubPlanFilter产生
//PSEUDO_COLUMN一般是由CTE或者层次查询产生
//SYS_CONNECT_BY_PATH,CONNECT_BY_ROOT由cby nestloop join产生
bool ObRawExpr::has_generalized_column() const
{
  return has_flag(CNT_COLUMN) || has_flag(CNT_AGG) || has_flag(CNT_SET_OP) || has_flag(CNT_SUB_QUERY)
         || has_flag(CNT_WINDOW_FUNC) || has_flag(CNT_ROWNUM) || has_flag(CNT_PSEUDO_COLUMN)
         || has_flag(CNT_SEQ_EXPR) || has_flag(CNT_SYS_CONNECT_BY_PATH) || has_flag(CNT_CONNECT_BY_ROOT)
         || has_flag(CNT_OP_PSEUDO_COLUMN);

}

//这个函数是用来判断表达式中是否可能存在enum或者set的列
bool ObRawExpr::has_enum_set_column() const
{
  return has_flag(CNT_ENUM_OR_SET);
}

bool ObRawExpr::has_specified_pseudocolumn() const
{
  return has_flag(CNT_ROWNUM) || has_flag(CNT_PSEUDO_COLUMN);
}

bool ObRawExpr::is_vectorize_result() const
{
  // subset of ObRawExpr::cnt_not_calculable_expr
  // TODO bin.lb: more sophisticate
  bool not_pre_calc = has_generalized_column()
      || has_flag(CNT_STATE_FUNC)
      || has_flag(CNT_DYNAMIC_USER_VARIABLE)
      || has_flag(CNT_ALIAS)
      || has_flag(CNT_VALUES)
      || has_flag(CNT_SEQ_EXPR)
      || has_flag(CNT_SYS_CONNECT_BY_PATH)
      || has_flag(CNT_RAND_FUNC)
      || has_flag(CNT_SO_UDF)
      || has_flag(CNT_PRIOR)
      || has_flag(CNT_OP_PSEUDO_COLUMN)
      || has_flag(CNT_VOLATILE_CONST);

  // Cant be const expr, const expr's evaluate flag is not cleared in execution.
  // E.g.:
  //  last_insert_id() is not pre-calculable but is const set to vectorize result will
  //  coredump because evaluate flag not cleared which reset datum's pointers depends.
  bool is_const = is_const_expr();

  return not_pre_calc && !is_const;
}

int ObRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(&other != this)) {
    if (OB_UNLIKELY(get_expr_type() != other.get_expr_type()) ||
        OB_UNLIKELY(get_expr_class() != other.get_expr_class())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr type or class does not match", K(ret),
               K(get_expr_type()), K(other.get_expr_type()),
               K(get_expr_class()), K(other.get_expr_class()));
    } else {
      result_type_ = other.result_type_;
      info_ = other.info_;
      rel_ids_ = other.rel_ids_;
      alias_column_name_ = other.alias_column_name_;
      expr_name_= other.expr_name_;
      ref_count_ = other.ref_count_;
      reference_type_ = other.reference_type_;
      is_for_generated_column_ = other.is_for_generated_column_;
      extra_ = other.extra_;
      is_called_in_sql_ = other.is_called_in_sql_;
      is_calculated_ = other.is_calculated_;
      is_deterministic_ = other.is_deterministic_;
      partition_id_calc_type_ = other.partition_id_calc_type_;
      local_session_var_id_ = other.local_session_var_id_;
      if (OB_FAIL(enum_set_values_.assign(other.enum_set_values_))) {
        LOG_WARN("failed to assign enum set values", K(ret));
      } else if (OB_FAIL(local_session_var_.assign(other.local_session_var_))) {
        LOG_WARN("fail to assign local session vars", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExpr::deep_copy(ObIRawExprCopier &copier,
                         const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(&other != this)) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("failed to assign other expr", K(ret));
    } else if (OB_FAIL(inner_deep_copy(copier))) {
      LOG_WARN("failed to deep copy new allocator", K(ret));
    }
  }
  return ret;
}

int ObRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (copier.deep_copy_attributes()) {
    ObObj param;
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_,
                                       alias_column_name_,
                                       alias_column_name_))) {
      LOG_WARN("fail to write string", K(alias_column_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, expr_name_, expr_name_))) {
      LOG_WARN("fail to write string", K(expr_name_), K(ret));
    } else if (OB_FAIL(deep_copy_obj(*inner_alloc_, result_type_.get_param(), param))) {
      LOG_WARN("failed to deep copy object", K(ret), K(param));
    } else if (OB_FAIL(local_session_var_.deep_copy_self())) {
        LOG_WARN("fail to deep opy local session vars", K(ret));
    } else {
      result_type_.set_param(param);
      for (int64_t i = 0; OB_SUCC(ret) && i < enum_set_values_.count(); i++) {
        if (OB_FAIL(ob_write_string(*inner_alloc_, enum_set_values_.at(i), enum_set_values_.at(i)))) {
          LOG_WARN("fail to write string", K(ret), K(enum_set_values_), K(i));
        }
      }
    }
  }
  return ret;
}

int ObRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                            const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  UNUSED(other_exprs);
  UNUSED(new_exprs);
  return ret;
}

void ObRawExpr::reset()
{
  type_ = T_INVALID;
  info_.reset();
  rel_ids_.reset();
  set_data_type(ObMaxType);
  ref_count_ = 0;
  reference_type_ = ExplicitedRefType::NONE_REF;
  is_for_generated_column_ = false;
  is_called_in_sql_ = true;
  is_calculated_ = false;
  is_deterministic_ = true;
}

int ObRawExpr::get_name(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("fail to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    LOG_DEBUG("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    if (OB_FAIL(get_name_internal(buf, buf_len, pos, type))) {
      LOG_WARN("fail to get_name", K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

int ObRawExpr::get_type_and_length(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_EXTENDED == type || EXPLAIN_EXTENDED_NOADDR == type) {
    const char* type_str = common::inner_obj_type_str(get_data_type());
    if (nullptr == type_str || strlen(type_str) > INT32_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some error happend");
    } else {
      int str_length = (int)strlen(type_str);
      int32_t out_data = get_accuracy().get_precision();
      bool output_length = ob_is_accuracy_length_valid_tc(get_data_type());
      if (output_length) {
        out_data = get_accuracy().get_length();
      }
      if (OB_FAIL(BUF_PRINTF(", %.*s, %d", str_length, type_str, out_data))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }else { }
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObRawExpr::extract_info()
{
  //LOG_DEBUG("extract_info", "usec", ObSQLUtils::get_usec());
  //scope may be NULL, do not check
  int ret = OB_SUCCESS;
  ObRawExprInfoExtractor extractor;
  if (OB_FAIL(extractor.analyze(*this))) {
    LOG_WARN("fail to analyze", K(ret));
  }
  //LOG_DEBUG("extractor_info", "usec", ObSQLUtils::get_usec());
  return ret;
}

int ObRawExpr::deduce_type(const ObSQLSessionInfo *session_info,
                           bool solidify_session_vars,
                           const ObLocalSessionVar *local_vars,
                           int64_t local_var_id)
{
  //LOG_DEBUG("deduce_type", "usec", ObSQLUtils::get_usec());
  int ret = OB_SUCCESS;
  ObRawExprDeduceType expr_deducer(session_info, solidify_session_vars, local_vars, local_var_id);
  expr_deducer.set_expr_factory(expr_factory_);
  if (OB_FAIL(expr_deducer.deduce(*this))) {
    if (session_info->is_varparams_sql_prepare() &&
        OB_ERR_INVALID_COLUMN_NUM != ret &&
        OB_ERR_TOO_MANY_VALUES != ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("ps prepare phase ignores type deduce error");
    } else {
      LOG_WARN("fail to deduce", K(ret));
    }
  }
  //LOG_DEBUG("deduce_type", "usec", ObSQLUtils::get_usec());
  return ret;
}

int ObRawExpr::formalize_with_local_vars(const ObSQLSessionInfo *session_info,
                                         const ObLocalSessionVar *local_vars,
                                         int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(formalize(session_info, false, local_vars, local_var_id))) {
    LOG_WARN("formalize with local vars failed", K(ret));
  }
  return ret;
}

int ObRawExpr::formalize(const ObSQLSessionInfo *session_info,
                         bool solidify_session_vars)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(formalize(session_info, solidify_session_vars, NULL, OB_INVALID_INDEX_INT64))) {
    LOG_WARN("formalize with local vars failed", K(ret));
  }
  return ret;
}

int ObRawExpr::formalize(const ObSQLSessionInfo *session_info,
                         bool solidify_session_vars,
                         const ObLocalSessionVar *local_vars,
                         int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("fail to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(extract_info())) {
    LOG_WARN("failed to extract info", K(*this));
  } else if (OB_FAIL(deduce_type(session_info, solidify_session_vars, local_vars, local_var_id))) {
    LOG_WARN("failed to deduce type", K(*this));
  } else {}
  return ret;
}

int ObRawExpr::pull_relation_id()
{
  int ret = OB_SUCCESS;
  ObExprRelationAnalyzer expr_relation_analyzer;
  if (OB_FAIL(expr_relation_analyzer.pull_expr_relation_id(this))) {
    LOG_WARN("pull expr failed", K(ret));
  }
  return ret;
}

int ObRawExpr::add_child_flags(const ObExprInfo &flags)
{
  int ret = OB_SUCCESS;
  if (INHERIT_MASK_BEGIN < flags.bit_count()) {
    ObExprInfo tmp(flags);
    int64_t mask_end = INHERIT_MASK_END < flags.bit_count() ?
                       static_cast<int64_t>(INHERIT_MASK_END) : flags.bit_count() - 1;
    if (OB_FAIL(tmp.do_mask(INHERIT_MASK_BEGIN, mask_end))) {
      LOG_WARN("failed to do mask", K(ret));
    } else if (OB_FAIL(info_.add_members(tmp))) {
      LOG_WARN("failed to add expr info", K(ret));
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i <= IS_INFO_MASK_END; ++i) {
    if (flags.has_member(i)) {
      if (OB_FAIL(info_.add_member(i + CNT_INFO_MASK_BEGIN))) {
        LOG_WARN("failed to add member", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObRawExpr::set_expr_name(const common::ObString &expr_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inner_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, expr_name, expr_name_))) {
    LOG_WARN("write string failed", K(ret));
  }
  return ret;
}

int ObRawExpr::preorder_accept(ObRawExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_visit(visitor))) {
    LOG_WARN("visit failed", K(ret),
        "type", get_expr_type(), "name", get_type_name(get_expr_type()));
  } else {
    if (!skip_visit_child() && !visitor.skip_child(*this)) {
      const int64_t cnt = get_param_count();
      for (int64_t i = 0; i < cnt && OB_SUCC(ret); i++) {
        ObRawExpr *e = get_param_expr(i);
        if (NULL == e) {
          LOG_WARN("null param expr returned", K(ret), K(i), K(cnt));
        } else if (OB_FAIL(SMART_CALL(e->preorder_accept(visitor)))) {
          LOG_WARN("child visit failed", K(ret), K(i),
              "type", get_expr_type(), "name", get_type_name(get_expr_type()));
        }
      }
    }
  }
  return ret;
}

int ObRawExpr::postorder_accept(ObRawExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (!skip_visit_child() && !visitor.skip_child(*this)) {
    const int64_t cnt = get_param_count();
    for (int64_t i = 0; i < cnt && OB_SUCC(ret); i++) {
      ObRawExpr *e = get_param_expr(i);
      if (NULL == e) {
        LOG_WARN("null param expr returned", K(ret), K(i), K(cnt));
      } else if (OB_FAIL(SMART_CALL(e->postorder_accept(visitor)))) {
        LOG_WARN("child visit failed", K(ret), K(i),
            "type", get_expr_type(), "name", get_type_name(get_expr_type()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_visit(visitor))) {
      LOG_WARN("visit failed", K(ret),
          "type", get_expr_type(), "name", get_type_name(get_expr_type()));
    }
  }
  return ret;
}

// Get identify expr for same as check:
// 1. Original expr of exec param expr
// 2. Skip the inner added implicit cast expr for none root node, e.g.:
//    abs(cast(c1)) is same as abs(c1)
//    but: cast(abs(c1)) is not same as abs(c1)
const ObRawExpr *ObRawExpr::get_same_identify(const ObRawExpr *e,
                                              const ObExprEqualCheckContext *check_ctx) const
{
  while (NULL != e) {
    if (e->is_exec_param_expr()) {
      if (NULL != static_cast<const ObExecParamRawExpr *>(e)->get_ref_expr()) {
        e = static_cast<const ObExecParamRawExpr *>(e)->get_ref_expr();
      }
    } else if (NULL != check_ctx && check_ctx->ignore_implicit_cast_
               && check_ctx->recursion_level_ > 1
               && T_FUN_SYS_CAST == e->get_expr_type()
               && e->has_flag(IS_OP_OPERAND_IMPLICIT_CAST)) {
      e = e->get_param_expr(0);
    } else {
      break;
    }
  }
  return e;
}

bool ObRawExpr::is_spatial_expr() const
{
  const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(this);
  return IS_SPATIAL_OP(expr->get_expr_type());
}

bool ObRawExpr::is_oracle_spatial_expr() const
{
  bool ret_bool = false;
  if (lib::is_oracle_mode() && get_expr_type() == T_OP_EQ) {
    const ObRawExpr *l_expr = get_param_expr(0);
    const ObRawExpr *r_expr = get_param_expr(1);
    if (OB_NOT_NULL(l_expr) && OB_NOT_NULL(r_expr)) {
      const ObRawExpr *l_inner = ObRawExprUtils::skip_inner_added_expr(l_expr);
      const ObRawExpr *r_inner = ObRawExprUtils::skip_inner_added_expr(r_expr);
      const ObRawExpr *const_expr = nullptr;
      const ObRawExpr *geo_expr = nullptr;
      if (l_inner->get_expr_type() == T_FUN_SYS_SDO_RELATE && r_inner->is_const_expr()) {
        geo_expr = l_inner;
        const_expr = r_inner;
      } else if (r_inner->get_expr_type() == T_FUN_SYS_SDO_RELATE && l_inner->is_const_expr()) {
        geo_expr = r_inner;
        const_expr = l_inner;
      }
      if (OB_NOT_NULL(const_expr) && OB_NOT_NULL(geo_expr)) {
        ret_bool = true;
      }
    } // do nothing
  }
  return ret_bool;
}

bool ObRawExpr::is_json_domain_expr() const
{
  const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(this);
  return IS_JSON_DOMAIN_OP(expr->get_expr_type());
}

bool ObRawExpr::is_multivalue_expr() const
{
  const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(this);
  return IS_MULTIVALUE_EXPR(expr->get_expr_type());
}

ObRawExpr* ObRawExpr::get_json_domain_param_expr()
{
  ObRawExpr* param_expr = nullptr;

  if (get_expr_type() == T_FUN_SYS_JSON_MEMBER_OF) {
    param_expr = get_param_expr(1);
  } else if (get_expr_type() == T_FUN_SYS_JSON_CONTAINS) {
    param_expr = get_param_expr(0);
  } else if (get_expr_type() == T_FUN_SYS_JSON_OVERLAPS) {
    param_expr = get_param_expr(0);
    if (OB_NOT_NULL(param_expr) && param_expr->is_const_expr()) {
      param_expr = get_param_expr(1);
    }
  }

  return param_expr;
}

bool ObRawExpr::is_domain_expr() const
{
  const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(this);
  return IS_DOMAIN_OP(expr->get_expr_type());
}

bool ObRawExpr::is_domain_json_expr() const
{
  const ObRawExpr *expr = ObRawExprUtils::skip_inner_added_expr(this);
  return IS_DOMAIN_JSON_OP(expr->get_expr_type());
}

bool ObRawExpr::is_multivalue_define_json_expr() const
{
  bool b_ret = false;
  const ObRawExpr *sub_expr = nullptr;
  if (type_ == T_FUN_SYS_JSON_QUERY &&
      get_param_count() >= 13 &&
      OB_NOT_NULL(sub_expr = get_param_expr(12)) &&
      sub_expr->is_const_expr()) {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(sub_expr);
    b_ret = const_expr->get_value().get_int() == 0;
  }

  return b_ret;
}

bool ObRawExpr::extract_multivalue_json_expr(const ObRawExpr*& json_expr) const
{
  bool found = false;

  for (int i = 0; i < get_param_count() && !found; ++i) {
    const ObRawExpr *child = get_param_expr(i);
    if (OB_ISNULL(child)) {
    } else if (child->type_ == T_FUN_SYS_JSON_QUERY) {
      json_expr = child;
      found = json_expr->is_multivalue_define_json_expr();
      break;
    } else if (child->extract_multivalue_json_expr(json_expr)) {
      found = (json_expr && json_expr->type_ == T_FUN_SYS_JSON_QUERY);
    }
  }
  return found;
}

bool ObRawExpr::is_geo_expr() const
{
  return IS_GEO_OP(get_expr_type());
}

bool ObRawExpr::is_xml_expr() const
{
  return IS_XML_OP(get_expr_type());
}

bool ObRawExpr::is_mysql_geo_expr() const
{
  return IS_MYSQL_GEO_OP(get_expr_type());
}

bool ObRawExpr::is_priv_geo_expr() const
{
  return IS_PRIV_GEO_OP(get_expr_type());
}

// has already been confirmed that the result type is geometry.
ObGeoType ObRawExpr::get_geo_expr_result_type() const
{
  ObGeoType geo_type = ObGeoType::GEOTYPEMAX;
  switch (this->get_expr_type()) {
    case T_FUN_SYS_CAST: {
      int ret = OB_SUCCESS;
      if (OB_FAIL(get_geo_cast_result_type(geo_type))) {
        geo_type = ObGeoType::GEOTYPEMAX;
      }
      break;
    }
    case T_FUN_SYS_POINT:
    case T_FUN_SYS_ST_CENTROID: {
      geo_type = ObGeoType::POINT;
      break;
    }
    case T_FUN_SYS_LINESTRING: {
      geo_type = ObGeoType::LINESTRING;
      break;
    }
    case T_FUN_SYS_MULTIPOINT: {
      geo_type = ObGeoType::MULTIPOINT;
      break;
    }
    case T_FUN_SYS_MULTILINESTRING: {
      geo_type = ObGeoType::MULTILINESTRING;
      break;
    }
    case T_FUN_SYS_POLYGON: {
      geo_type = ObGeoType::POLYGON;
      break;
    }
    case T_FUN_SYS_MULTIPOLYGON: {
      geo_type = ObGeoType::MULTIPOLYGON;
      break;
    }
    case T_FUN_SYS_GEOMCOLLECTION: {
      geo_type = ObGeoType::GEOMETRYCOLLECTION;
      break;
    }
    default: {
      geo_type = ObGeoType::GEOMETRY;
      break;
    }
  }
  return geo_type;
}

int ObRawExpr::get_geo_cast_result_type(ObGeoType& geo_type) const
{
  int ret = OB_SUCCESS;
  if (T_FUN_SYS_CAST != get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be cast expr", K(ret));
  } else if (OB_ISNULL(get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast second param expr is NULL", K(ret));
  } else if (!get_param_expr(1)->is_const_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast second param expr is not const expr", K(ret));
  } else {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(get_param_expr(1));
    ObObj value = const_expr->get_value();
    if (!value.is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value of second param expr is not int", K(value), K(ret));
    } else {
      ParseNode parse_node;
      parse_node.value_ = value.get_int();
      geo_type = static_cast<ObGeoType>(parse_node.int16_values_[OB_NODE_CAST_GEO_TYPE_IDX]);
    }
  }
  return ret;
}

bool ObRawExpr::same_as(const ObRawExpr &expr,
                        ObExprEqualCheckContext *check_context) const
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (this == &expr) {
    bret = true;
  } else {
    if (NULL != check_context) {
      check_context->recursion_level_ += 1;
    }
    const ObRawExpr *l = get_same_identify(this, check_context);
    const ObRawExpr *r = get_same_identify(&expr, check_context);
    ret = SMART_CALL(bret = l->inner_same_as(*r, check_context));
    if (NULL != check_context) {
      if (OB_SIZE_OVERFLOW == ret) {
        bret = false;
        check_context->error_code_ = ret;
        LOG_WARN("check smart call fail", K(ret));
      } else {
        check_context->recursion_level_ -= 1;
      }
    }

    if (bret) {
      //check if local vars are the same
      bret = (l->get_local_session_var() == r->get_local_session_var());
    }
  }
  return bret;
}


////////////////////////////////////////////////////////////////
ObRawExpr *&ObTerminalRawExpr::get_param_expr(int64_t index)
{
  UNUSED(index);
  return USELESS_POINTER;
}

bool ObRawExpr::is_bool_expr() const
{
  return IS_BOOL_OP(type_) || (is_const_raw_expr() && static_cast<const ObConstRawExpr*>(this)->is_literal_bool());
}

bool ObRawExpr::is_json_expr() const
{
  return (T_FUN_SYS_JSON_OBJECT <= get_expr_type() && get_expr_type() <= T_FUN_JSON_OBJECTAGG) ? true : false;
}

bool ObRawExpr::is_multiset_expr() const
{
  return is_query_ref_expr() && static_cast<const ObQueryRefRawExpr *>(this)->is_multiset();
}

int ObRawExpr::set_enum_set_values(const common::ObIArray<common::ObString> &values)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(enum_set_values_.assign(values))) {
    LOG_WARN("failed to assign array", K(ret));
  }
  return ret;
}

bool ObRawExpr::is_not_calculable_expr() const
{
  return is_generalized_column()
         || has_flag(IS_STATE_FUNC)
         || has_flag(IS_DYNAMIC_USER_VARIABLE)
         || has_flag(IS_ALIAS)
         || has_flag(IS_VALUES)
         || has_flag(IS_SEQ_EXPR)
         || has_flag(IS_SYS_CONNECT_BY_PATH)
         || has_flag(IS_RAND_FUNC)
         || has_flag(IS_SO_UDF_EXPR)
         || has_flag(IS_PRIOR)
         || has_flag(IS_VOLATILE_CONST)
         || has_flag(IS_ASSIGN_EXPR);
}

bool ObRawExpr::cnt_not_calculable_expr() const
{
  // NOTE: When adding rules here, please check whether ObRawExpr::is_vectorize_result()
  // need add the same rules.
  return has_generalized_column()
         || has_flag(CNT_STATE_FUNC)
         || has_flag(CNT_DYNAMIC_USER_VARIABLE)
         || has_flag(CNT_ALIAS)
         || has_flag(CNT_VALUES)
         || has_flag(CNT_SEQ_EXPR)
         || has_flag(CNT_SYS_CONNECT_BY_PATH)
         || has_flag(CNT_RAND_FUNC)
         || has_flag(CNT_SO_UDF)
         || has_flag(CNT_PRIOR)
         || has_flag(CNT_VOLATILE_CONST)
         || has_flag(CNT_ASSIGN_EXPR);
}

/**
 * @brief check whether an expr is const when all of its param exprs are const expr.
 * param_need_replace is true when called in ObOptimizerUtil::is_const_expr_recursively
 * e.g.:
 *  1 + exists (select 1 > t1) will be const expr after replacing the T_OP_EXISTS with onetime expr,
 *  we need to treat the expr as const expr without replacement when generate logical plan.
 *  The param_need_replace flag only disables the shortcut of cnt_not_calculable_expr(),
 *  the param expr remains non-const if it can not be replaced into exec param
 */
int ObRawExpr::is_const_inherit_expr(bool &is_const_inherit,
                                     const bool param_need_replace /*=false*/) const
{
  int ret = OB_SUCCESS;
  is_const_inherit = true;
  if (T_FUN_SYS_RAND == type_
      || T_FUN_SYS_RANDOM == type_
      || T_FUN_SYS_GENERATOR == type_
      || T_FUN_SYS_UUID == type_
      || T_FUN_SYS_UUID_SHORT == type_
      || T_FUN_SYS_SEQ_NEXTVAL == type_
      || T_FUN_SYS_AUTOINC_NEXTVAL == type_
      || T_FUN_SYS_DOC_ID == type_
      || T_FUN_SYS_TABLET_AUTOINC_NEXTVAL == type_
      || T_FUN_SYS_ROWNUM == type_
      || T_FUN_SYS_ROWKEY_TO_ROWID == type_
      || T_OP_CONNECT_BY_ROOT == type_
      || T_FUN_SYS_CONNECT_BY_PATH == type_
      || T_FUN_SYS_GUID == type_
      || T_FUN_SYS_STMT_ID == type_
      || T_FUN_SYS_SLEEP == type_
      || T_OP_PRIOR == type_
      || T_OP_ASSIGN == type_
      || T_FUN_NORMAL_UDF == type_
      || T_FUN_SYS_REMOVE_CONST == type_
      || T_FUN_SYS_WRAPPER_INNER == type_
      || T_FUN_SYS_VALUES == type_
      || T_OP_GET_PACKAGE_VAR == type_
      || T_OP_GET_SUBPROGRAM_VAR == type_
      || T_FUN_SYS_JSON_VALUE == type_
      || T_FUN_SYS_JSON_QUERY == type_
      || (T_FUN_SYS_JSON_EXISTS == type_ && lib::is_oracle_mode())
      || T_FUN_SYS_JSON_EQUAL == type_
      || T_FUN_SYS_IS_JSON == type_
      || (T_FUN_SYS_JSON_MERGE_PATCH == type_ && lib::is_oracle_mode())
      || T_FUN_SYS_JSON_OBJECT == type_
      || IS_LABEL_SE_POLICY_FUNC(type_)
      || (T_FUN_SYS_LAST_INSERT_ID == type_ && get_param_count() > 0)
      || T_FUN_SYS_TO_BLOB == type_
      || (T_FUN_SYS_SYSDATE == type_ && lib::is_mysql_mode())
      || (param_need_replace ? is_not_calculable_expr() : cnt_not_calculable_expr())
      || T_FUN_LABEL_SE_SESSION_LABEL == type_
      || T_FUN_LABEL_SE_SESSION_ROW_LABEL == type_
      || T_FUN_SYS_LAST_REFRESH_SCN == type_
      || (T_FUN_UDF == type_
          && !static_cast<const ObUDFRawExpr*>(this)->is_deterministic())
      || T_FUN_SYS_GET_LOCK == type_
      || T_FUN_SYS_IS_FREE_LOCK == type_
      || T_FUN_SYS_IS_USED_LOCK == type_
      || T_FUN_SYS_RELEASE_LOCK == type_
      || T_FUN_SYS_RELEASE_ALL_LOCKS == type_) {
     is_const_inherit = false;
  }
  if (is_const_inherit && T_OP_GET_USER_VAR == type_) {
    if (get_param_count() != 1 || OB_ISNULL(get_param_expr(0)) ||
        get_param_expr(0)->type_ != T_USER_VARIABLE_IDENTIFIER) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret));
    } else {
      const ObUserVarIdentRawExpr *var_expr =
          static_cast<const ObUserVarIdentRawExpr*>(get_param_expr(0));
      if (var_expr->get_is_contain_assign() || var_expr->get_query_has_udf()) {
        is_const_inherit = false;
      }
    }
  }
  return ret;
}

/**
 * @brief 判断Oracle的系统函数是不是pure的，用于创建生成列或函数索引时的检查
 * @return
 */
int ObRawExpr::is_non_pure_sys_func_expr(bool &is_non_pure) const
{
  int ret = OB_SUCCESS;
  is_non_pure = false;
  if (lib::is_oracle_mode()) {
    if (T_FUN_SYS_LOCALTIMESTAMP == type_
          || T_FUN_SYS_SESSIONTIMEZONE == type_
          || T_FUN_SYS_DBTIMEZONE == type_
          || T_FUN_SYS_SYSDATE == type_
          || T_FUN_SYS_SYSTIMESTAMP == type_
          || T_FUN_SYS_UID == type_
          || T_FUN_SYS_USER == type_
          || T_FUN_SYS_CUR_TIMESTAMP == type_
          || T_FUN_SYS_RANDOM == type_
          || T_FUN_SYS_GUID == type_
          || T_FUN_SYS_CUR_DATE == type_
          || T_FUN_SYS_USERENV == type_
          || T_FUN_SYS_REGEXP_REPLACE == type_
          || T_FUN_GET_TEMP_TABLE_SESSID == type_
          || T_FUN_LABEL_SE_SESSION_LABEL == type_
          || T_FUN_LABEL_SE_SESSION_ROW_LABEL == type_
          || T_FUN_SYS_USER_CAN_ACCESS_OBJ == type_) {
      is_non_pure = true;
    } else if (T_FUN_SYS_TO_DATE == type_ || T_FUN_SYS_TO_TIMESTAMP == type_ ||
               T_FUN_SYS_TO_TIMESTAMP_TZ == type_) {
      const ObRawExpr *format_expr = NULL;
      if (get_param_count() < 2) {
        is_non_pure = true;
      } else if (OB_ISNULL(format_expr = get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (!format_expr->is_const_raw_expr()) {
        is_non_pure = true;
      } else {
        const ObConstRawExpr* const_expr = static_cast<const ObConstRawExpr*>(format_expr);
        const ObObj &value = const_expr->get_value();
        bool need_tz = T_FUN_SYS_TO_TIMESTAMP_TZ == type_;
        bool complete = true;
        if (OB_UNLIKELY(!value.is_string_type())) {
          // just pass, will report error when calc result type
          if (value.is_null()) {
            is_non_pure = true;
          }
        } else if (OB_FAIL(ObTimeConverter::check_dfm_deterministic(value.get_string(),
                                                      value.get_collation_type(),
                                                      need_tz, complete))) {
          LOG_WARN("check datetime format model complete failed", K(ret), K(value));
        } else {
          is_non_pure = !complete;
        }
      }
    }
  } else {
    if (T_FUN_SYS_CONNECTION_ID == type_
          || T_FUN_SYS_VERSION == type_
          || T_FUN_SYS_CURRENT_USER == type_
          || T_FUN_SYS_USER == type_
          || T_FUN_SYS_DATABASE == type_
          || T_FUN_SYS_SYSDATE == type_
          || T_FUN_SYS_CUR_DATE == type_
          || T_FUN_SYS_CUR_TIME == type_
          || T_FUN_SYS_CUR_TIMESTAMP == type_
          || T_FUN_SYS_UNIX_TIMESTAMP == type_
          || T_FUN_SYS_UTC_TIME == type_
          || T_FUN_SYS_UTC_TIMESTAMP == type_
          || T_FUN_SYS_UTC_DATE == type_
          || T_FUN_SYS_RAND == type_
          || T_FUN_SYS_RANDOM == type_
          || T_FUN_SYS_UUID == type_
          || T_FUN_SYS_UUID_SHORT == type_
          || T_FUN_SYS_SLEEP == type_
          || T_FUN_SYS_LAST_INSERT_ID == type_
          || T_FUN_SYS_ROW_COUNT == type_
          || T_FUN_SYS_FOUND_ROWS == type_
          || T_FUN_SYS_CURRENT_USER_PRIV == type_
          || T_FUN_SYS_TRANSACTION_ID == type_
          || T_FUN_SYS_GET_LOCK == type_
          || T_FUN_SYS_IS_FREE_LOCK == type_
          || T_FUN_SYS_IS_USED_LOCK == type_
          || T_FUN_SYS_RELEASE_LOCK == type_
          || T_FUN_SYS_RELEASE_ALL_LOCKS == type_
          || T_FUN_SYS_CURRENT_ROLE == type_) {
      is_non_pure = true;
    }
  }
  return ret;
}

/**
 * @brief 判断Oracle的伪列能否出现在生成列中
 * @return
 */
bool ObRawExpr::is_specified_pseudocolumn_expr() const
{
  if (T_FUN_SYS_ROWNUM == type_
      || T_LEVEL == type_
      || T_CONNECT_BY_ISCYCLE == type_
      || T_CONNECT_BY_ISLEAF == type_) {
    return true;
  }
  return false;
}

int ObRawExpr::extract_local_session_vars_recursively(ObIArray<const share::schema::ObSessionSysVar *> &var_array)
{
  int ret = OB_SUCCESS;
  if (get_local_session_var().get_var_count() > 0) {
    ObSEArray<const share::schema::ObSessionSysVar *, 4> local_vars;
    if (OB_FAIL(get_local_session_var().get_local_vars(local_vars))) {
      LOG_WARN("fail to append session var array", K(ret));
    } else if (OB_FAIL(append(var_array, local_vars))) {
      LOG_WARN("append local vars failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count(); ++i) {
      if (OB_ISNULL(get_param_expr(i))){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_param_expr(i)->extract_local_session_vars_recursively(var_array)))) {
        LOG_WARN("fail to extract sysvar from params", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExpr::has_exec_param(bool &bool_ret) const
{
  int ret = OB_SUCCESS;
  bool_ret = false;
  if (is_exec_param_expr()) {
    bool_ret = true;
  } else {
    for (int64_t i = 0; i < get_param_count() && OB_SUCC(ret) && !bool_ret; ++i) {
      const ObRawExpr *child_expr = get_param_expr(i);
      if (OB_FAIL(SMART_CALL(child_expr->has_exec_param(bool_ret)))) {
        LOG_WARN("failed to has_exec_param");
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObConstRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObTerminalRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObTerminalRawExpr failed", K(ret));
    } else {
      const ObConstRawExpr &const_expr = static_cast<const ObConstRawExpr &>(other);
      value_ = const_expr.get_value();
      literal_prefix_ = const_expr.get_literal_prefix();
      obj_meta_ = const_expr.get_expr_obj_meta();
      is_date_unit_ = const_expr.is_date_unit_;
      is_literal_bool_ = const_expr.is_literal_bool();
      array_param_group_id_ = const_expr.get_array_param_group_id();
      is_dynamic_eval_questionmark_ = const_expr.is_dynamic_eval_questionmark_;
      orig_questionmark_type_.assign(const_expr.get_orig_qm_type());
    }
  }
  return ret;
}

int ObConstRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("copy in Base class ObTerminalRawExpr failed", K(ret));
  } else if (copier.deep_copy_attributes()) {
    ObObj tmp;
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator is NULL", K(ret));
    } else if (OB_FAIL(deep_copy_obj(*inner_alloc_, value_, tmp))) {
      LOG_WARN("deep copy error", K(value_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, literal_prefix_, literal_prefix_))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      value_ = tmp;
    }
  }
  return ret;
}

int ObConstRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                 const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObConstRawExpr::set_value(const ObObj &val)
{
  set_meta_type(val.get_meta()); //让const raw expr的类型和值类型相同
  set_expr_obj_meta(val.get_meta());
  value_ = val;
}

void ObConstRawExpr::set_literal_prefix(const ObString &name)
{
  literal_prefix_ = name;
}

void ObConstRawExpr::reset()
{
  ObRawExpr::reset();
  value_.reset();
  literal_prefix_.reset();
  is_literal_bool_ = false;
  array_param_group_id_ = -1;
  is_dynamic_eval_questionmark_ = false;
  orig_questionmark_type_.reset();

}

void ObConstRawExpr::set_is_date_unit()
{
  is_date_unit_ = true;
}

void ObConstRawExpr::reset_is_date_unit()
{
  is_date_unit_ = false;
}

uint64_t ObConstRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_val = seed;
  value_.hash(hash_val, seed);
  if (T_QUESTIONMARK == get_expr_type() && is_dynamic_eval_questionmark()) {
    if (!(result_type_.is_decimal_int() || result_type_.is_number())
        || !(orig_questionmark_type_.is_decimal_int() || orig_questionmark_type_.is_number())) {
      int ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported dynamic eval quesiton mark", K(ret));
      OB_ASSERT(false);
      // do nothing
    } else {
      // if questionmark is evaluated during runtime, it's value is determined by:
      // - value_
      // - result_type_
      // - result_type_'s cast_mode
      ObCastMode cm = result_type_.get_cast_mode();
      hash_val = ObMurmurHash::hash(&cm, sizeof(uint64_t), hash_val);
      if (result_type_.is_decimal_int()) {
        // value determined by precision & scale
        ObPrecision prec = result_type_.get_precision();
        ObScale scale = result_type_.get_scale();
        hash_val = ObMurmurHash::hash(&prec, sizeof(ObPrecision), hash_val);
        hash_val = ObMurmurHash::hash(&scale, sizeof(ObScale), hash_val);
      } else if (result_type_.is_number()) {
        // decint->number, precision & scale are both ignored
        // do nothing
      }
    }
  }
  return hash_val;
}

bool ObConstRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  bool left_dyn_const = (T_QUESTIONMARK == get_expr_type() && is_dynamic_eval_questionmark());
  bool right_dyn_const = (T_QUESTIONMARK == expr.get_expr_type() && expr.is_static_const_expr()
                          && static_cast<const ObConstRawExpr &>(expr).is_dynamic_eval_questionmark());
  if (left_dyn_const && right_dyn_const) {
    // if following conditions are matched, two dynamic evaluated question_marks are same:
    // 1. param_idxes are same
    // 2. result_types are same
    // 3. cast modes are same
    const ObConstRawExpr &r_expr = static_cast<const ObConstRawExpr &>(expr);
    if (check_context != NULL) {
      int64_t l_param_idx = -1, r_param_idx = -1;
      int &ret = check_context->err_code_;
      if (OB_FAIL(get_value().get_unknown(l_param_idx))) {
        LOG_WARN("get param idx failed", K(ret));
      } else if (OB_FAIL(r_expr.get_value().get_unknown(r_param_idx))) {
        LOG_WARN("get param idx failed", K(ret));
      } else if (l_param_idx == r_param_idx) {
        bool_ret =
          (get_result_type() == r_expr.get_result_type()
           && get_result_type().get_cast_mode() == r_expr.get_result_type().get_cast_mode());
      }
    }
  } else if (left_dyn_const || right_dyn_const) {
    // for simplicity's sake, if question is evaluated during runtime, just return false
    // do nothing
  } else if (check_context != NULL && check_context->override_const_compare_) {
    if (expr.is_const_raw_expr()) {
      bool_ret = check_context->compare_const(*this, static_cast<const ObConstRawExpr&>(expr));
    }
  } else if (get_expr_type() != expr.get_expr_type()) {
    //what are you doing ?
    if (NULL != check_context) {
      if (expr.is_const_raw_expr()) {
        if (check_context->ora_numeric_compare_ && T_FUN_SYS_CAST == expr.get_expr_type() && lib::is_oracle_mode()) {
          bool_ret = check_context->compare_ora_numeric_consts(*this, static_cast<const ObSysFunRawExpr&>(expr));
        } else if (T_QUESTIONMARK == expr.get_expr_type()) {
          bool_ret = true;
          const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr *>(&expr);
          int64_t param_idx = -1;
          int &ret = check_context->err_code_;
          if (OB_FAIL(c_expr->get_value().get_unknown(param_idx))) {
            LOG_WARN("Failed to get param", K(ret));
          } else if (OB_FAIL(check_context->param_expr_.push_back(
              ObExprEqualCheckContext::ParamExprPair(param_idx, this)))) {
            LOG_WARN("Failed to add param expr pair", K(ret));
          } else { }
        }
      }
    }
  } else {
    const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr *>(&expr);
    if (get_value().get_meta() != c_expr->get_value().get_meta()) {
      bool_ret = false;
    } else if (get_value().is_equal(c_expr->get_value(), CS_TYPE_BINARY)) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObConstRawExpr::set_local_session_vars(const share::schema::ObLocalSessionVar *local_sys_vars,
                                            const ObBasicSessionInfo *session,
                                            int64_t ctx_array_idx)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_type(get_result_type().get_type())) {
    //solidify vars for parser
    local_session_var_id_ = ctx_array_idx;
    local_session_var_.reset();
    local_session_var_.set_local_var_capacity(2);
    if (OB_FAIL(ObExprOperator::add_local_var_to_expr(SYS_VAR_SQL_MODE, local_sys_vars,
                                                      session, local_session_var_))) {
      LOG_WARN("fail to add sql mode", K(ret));
    } else if (OB_FAIL(ObExprOperator::add_local_var_to_expr(SYS_VAR_COLLATION_CONNECTION, local_sys_vars,
                                                             session, local_session_var_))) {
      LOG_WARN("fail to add collation connection", K(ret));
    }
  }
  return ret;
}

int ObConstRawExpr::set_dynamic_eval_questionmark(const ObExprResType &dst_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(dst_type.is_decimal_int() || dst_type.is_number())
                  || !(result_type_.is_decimal_int() || result_type_.is_number()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported types for questionmark dynamic eval", K(ret));
  } else if (OB_UNLIKELY(is_dynamic_eval_questionmark_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectedly set dynamic evaluation twice", K(ret));
  } else {
    is_dynamic_eval_questionmark_ = true;
    orig_questionmark_type_.assign(result_type_);
    result_type_.assign(dst_type);
  }
  return ret;
}

bool ObExprEqualCheckContext::compare_const(const ObConstRawExpr &left,
                                            const ObConstRawExpr &right)
{
  int &ret = err_code_;
  bool result = false;
  if (left.get_result_type() == right.get_result_type()) {
    if (ignore_param_ && (left.get_value().is_unknown() || right.get_value().is_unknown())) {
      result = true;
    } else {
      const ObObj &this_value = left.get_value().is_unknown() ?
          left.get_result_type().get_param() : left.get_value();
      const ObObj &other_value = right.get_value().is_unknown() ?
          right.get_result_type().get_param() : right.get_value();
      result = this_value.is_equal(other_value, CS_TYPE_BINARY);
    }
  }
  if (OB_SUCC(ret) && result && left.get_value().is_unknown()) {
    if (OB_FAIL(add_param_pair(left.get_value().get_unknown(), NULL))) {
      LOG_WARN("add param pair failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && result && right.get_value().is_unknown()) {
    if (OB_FAIL(add_param_pair(right.get_value().get_unknown(), NULL))) {
      LOG_WARN("add param pair failed", K(ret));
    }
  }
  return result;
}

bool ObExprEqualCheckContext::compare_ora_numeric_consts(const ObConstRawExpr &left,
                                                         const ObSysFunRawExpr &right)
{
  int &ret = err_code_;
  bool result = false;
  if (OB_LIKELY(lib::is_oracle_mode() && right.is_const_expr() && right.get_expr_type() == T_FUN_SYS_CAST)) {
    ObCastMode cm = right.get_extra();
    const ObRawExpr *real_right = nullptr;
    bool is_lossless = false;
    if (CM_IS_IMPLICIT_CAST(cm) && !CM_IS_CONST_TO_DECIMAL_INT(cm)) {
      if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(&right, is_lossless))) {
        LOG_WARN("check lossless cast failed", K(ret));
      } else if (is_lossless) {
        real_right = right.get_param_expr(0);
        if (OB_ISNULL(real_right)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid null param expr", K(ret));
        } else {
          result = left.same_as(*real_right, this);
        }
      }
    }
  }
  return result;
}

bool ObExprEqualCheckContext::compare_ora_numeric_consts(const ObSysFunRawExpr &left, const ObConstRawExpr &right)
{
  int &ret = err_code_;
  LOG_INFO("debug test");
  bool result = false;
  if (OB_LIKELY(lib::is_oracle_mode() && left.get_expr_type()== T_FUN_SYS_CAST && left.is_const_expr())) {
    ObCastMode cm = left.get_extra();
    const ObRawExpr *real_left = nullptr;
    bool is_lossless = false;
    if (CM_IS_IMPLICIT_CAST(cm) && !CM_IS_CONST_TO_DECIMAL_INT(cm)) {
      if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(&left, is_lossless))) {
        LOG_WARN("check lossless cast failed", K(ret));
      } else if (is_lossless) {
        real_left = left.get_param_expr(0);
        if (OB_ISNULL(real_left)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid null param expr", K(ret));
        } else {
          result = real_left->same_as(right, this);
        }
      }
    }
  }
  return result;
}

int ObConstRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObConstRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                      ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_HINT_FORMAT == type) {
    if (OB_FAIL(BUF_PRINTF("?"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (get_value().is_unknown()) { //为explain特殊处理QuestionMark为？，其他地方打印成$IntNum
    if (EXPLAIN_DBLINK_STMT != type) {
      // if (OB_FAIL(get_value().print_plain_str_literal(buf, buf_len, pos))) {
      //   LOG_WARN("fail to print_sql_literal", K(get_value()), K(ret));
      // }
      if (OB_FAIL(BUF_PRINTF(":%ld", get_value().get_unknown()))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else {
      if (OB_FAIL(ObLinkStmtParam::write(buf, buf_len, pos, get_value().get_unknown()))) {
        LOG_WARN("fail to write param to buf", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_value().print_sql_literal(buf, buf_len, pos))) {
      LOG_WARN("fail to print_sql_literal", K(get_value()), K(ret));
      if (OB_ERR_NULL_VALUE == ret) {
        //ignore mull time zone info
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int64_t ObConstRawExpr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ITEM_TYPE, type_,
      N_RESULT_TYPE, result_type_,
      N_EXPR_INFO, info_,
      N_REL_ID, rel_ids_,
      N_VALUE, value_);
  if (!literal_prefix_.empty()) {
    J_COMMA();
    J_KV(K_(literal_prefix));
  }
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////
int ObVarRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObTerminalRawExpr::assign(other))) {
      LOG_WARN("failed to assign terminal raw expr", K(ret));
    } else {
      const ObVarRawExpr &var_expr = static_cast<const ObVarRawExpr &>(other);
      result_type_assigned_ = var_expr.result_type_assigned_;
    }
  }
  return ret;
}

int ObVarRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                               const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

bool ObVarRawExpr::inner_same_as(const ObRawExpr &expr,
                                 ObExprEqualCheckContext *check_context) const
{
  UNUSED(check_context);
  return expr.is_var_expr()
         && get_expr_type() == expr.get_expr_type()
         && get_result_type() == expr.get_result_type();
}

int ObVarRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                    ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_HINT_FORMAT == type) {
    if (OB_FAIL(BUF_PRINTF("?"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s",
                                     get_type_name(type_)))) {
    LOG_WARN("databuff print failed", K(ret));
  }
  return ret;
}

int ObVarRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

////////////////////////////////////////////////////////////////
int ObUserVarIdentRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObConstRawExpr::assign(other))) {
      LOG_WARN("failed to assign const raw expr", K(ret));
    } else {
      const ObUserVarIdentRawExpr &tmp = static_cast<const ObUserVarIdentRawExpr &>(other);
      is_contain_assign_ = tmp.is_contain_assign_;
      query_has_udf_ = tmp.query_has_udf_;
    }
  }
  return ret;
}

int ObUserVarIdentRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                        const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObUserVarIdentRawExpr::reset()
{
  ObConstRawExpr::reset();
  is_contain_assign_ = false;
}

uint64_t ObUserVarIdentRawExpr::hash_internal(uint64_t seed) const
{
  value_.hash(seed, seed);
  seed = common::do_hash(is_contain_assign_, seed);
  return seed;
}

bool ObUserVarIdentRawExpr::inner_same_as(const ObRawExpr &expr,
                                          ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  UNUSED(check_context);
  if (get_expr_type() == expr.get_expr_type()) {
    const ObUserVarIdentRawExpr *user_var_expr = static_cast<const ObUserVarIdentRawExpr *>(&expr);
    if (get_value().get_meta() != user_var_expr->get_value().get_meta()) {
      bool_ret = false;
    } else if (get_value().is_equal(user_var_expr->get_value(), CS_TYPE_BINARY)) {
      bool_ret = (get_is_contain_assign() == user_var_expr->get_is_contain_assign());
    }
  }
  return bool_ret;
}

bool ObUserVarIdentRawExpr::is_same_variable(const ObObj &obj) const
{
  bool bool_ret = false;
  if (get_value().get_meta() == obj.get_meta() &&
      get_value().is_equal(obj, CS_TYPE_BINARY)) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObUserVarIdentRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int64_t ObUserVarIdentRawExpr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ITEM_TYPE, type_,
       N_RESULT_TYPE, result_type_,
       N_EXPR_INFO, info_,
       N_REL_ID, rel_ids_,
       N_VALUE, value_,
       "is_contain_assign", is_contain_assign_,
       "query_has_udf", query_has_udf_);
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////
int ObQueryRefRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObTerminalRawExpr failed", K(ret));
    } else {
      const ObQueryRefRawExpr &tmp = static_cast<const ObQueryRefRawExpr &>(other);
      if (OB_FAIL(exec_params_.assign(tmp.exec_params_))) {
        LOG_WARN("failed to assign exec params", K(ret));
      } else if (OB_FAIL(column_types_.assign(tmp.column_types_))) {
        LOG_WARN("failed to copy column types", K(ret));
      } else {
        ref_id_ = tmp.ref_id_;
        ref_stmt_ = tmp.ref_stmt_;
        output_column_ = tmp.output_column_;
        is_set_ = tmp.is_set_;
        is_cursor_ = tmp.is_cursor_;
        has_nl_param_ = tmp.has_nl_param_;
        is_multiset_ = tmp.is_multiset_;
        column_types_ = tmp.column_types_;
      }
    }
  }
  return ret;
}

int ObQueryRefRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); ++i) {
    ObRawExpr *exec_param = exec_params_.at(i);
    ObRawExpr *new_expr = NULL;
    if (OB_FAIL(copier.find_in_copy_context(exec_param, new_expr))) {
      LOG_WARN("failed to find in copy context", K(ret));
    } else if (new_expr != NULL) {
      exec_params_.at(i) = static_cast<ObExecParamRawExpr *>(new_expr);
    } else if (OB_FAIL(copier.do_copy_expr(exec_param, new_expr))) {
      LOG_WARN("failed to copy exec param", K(ret));
    } else if (OB_ISNULL(new_expr) ||
               OB_UNLIKELY(!new_expr->is_exec_param_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is invalid", K(ret), K(new_expr));
    } else {
      exec_params_.at(i) = static_cast<ObExecParamRawExpr *>(new_expr);
    }
  }
  if (OB_SUCC(ret) && copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner alloc is null", K(ret), K(inner_alloc_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_types_.count(); ++i) {
      ObObj param;
      if (OB_FAIL(ob_write_obj(*inner_alloc_, column_types_.at(i).get_param(), param))) {
        LOG_WARN("failed to write obj", K(ret));
      } else {
        column_types_.at(i).set_param(param);
      }
    }
  }
  return ret;
}

void ObQueryRefRawExpr::clear_child()
{
  exec_params_.reset();
}

int ObQueryRefRawExpr::add_exec_param_expr(ObExecParamRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exec_params_.push_back(expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  }
  return ret;
}

int ObQueryRefRawExpr::add_exec_param_exprs(const ObIArray<ObExecParamRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_array_no_dup(exec_params_, exprs))) {
    LOG_WARN("failed to append array no dup", K(ret));
  }
  return ret;
}

int64_t ObQueryRefRawExpr::get_param_count() const
{
  return exec_params_.count();
}

const ObRawExpr *ObQueryRefRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr *expr = NULL;
  if (index >= 0 && index < exec_params_.count() && NULL != exec_params_.at(index)) {
    expr = exec_params_.at(index)->get_ref_expr();
  }
  return expr;
}

ObRawExpr *&ObQueryRefRawExpr::get_param_expr(int64_t index)
{
  if (index >= 0 && index < exec_params_.count() && NULL != exec_params_.at(index)) {
    return exec_params_.at(index)->get_ref_expr();
  } else {
    return USELESS_POINTER;
  }
}

ObExecParamRawExpr *ObQueryRefRawExpr::get_exec_param(int64_t index)
{
  if (index >= 0 && index < exec_params_.count()) {
    return exec_params_.at(index);
  } else {
    return NULL;
  }
}

int ObQueryRefRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                    const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, exec_params_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  return ret;
}

void ObQueryRefRawExpr::reset()
{
  ObRawExpr::reset();
  ref_id_ = OB_INVALID_ID;
}

bool ObQueryRefRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() == expr.get_expr_type()) {
    const ObQueryRefRawExpr &u_expr = static_cast<const ObQueryRefRawExpr &>(expr);
    if (is_set_ != u_expr.is_set_ || is_multiset_ != u_expr.is_multiset_) {
      /* bool bool_ret = false; */
    } else if (check_context != NULL && check_context->override_query_compare_) {
      bool_ret = check_context->compare_query(*this, u_expr);
    } else {
      // very tricky, check the definition of ref_stmt_ and get_ref_stmt()
      bool_ret = (get_ref_id() == u_expr.get_ref_id() &&
                  ref_stmt_ == u_expr.ref_stmt_ &&
                  is_set_ == u_expr.is_set_ &&
                  is_multiset_ == u_expr.is_multiset_);
    }
  }
  return bool_ret;
}

bool ObExprEqualCheckContext::compare_query(const ObQueryRefRawExpr &left,
                                            const ObQueryRefRawExpr &right)
{
  return left.get_ref_id() == right.get_ref_id() &&
      left.get_ref_stmt() == right.get_ref_stmt() &&
      left.is_set() == right.is_set() &&
      left.is_multiset() == right.is_multiset();
}

int ObQueryRefRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObQueryRefRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                         ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_HINT_FORMAT == type) {
    ObString qb_name;
    if (OB_ISNULL(ref_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(ref_stmt_));
    } else if (OB_FAIL(ref_stmt_->get_qb_name(qb_name))) {
      LOG_WARN("failed to get qb name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("SQ(%.*s)", qb_name.length(), qb_name.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (is_multiset() && OB_FAIL(BUF_PRINTF("multiset("))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("subquery(%lu)", ref_id_))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (is_multiset() && OB_FAIL(BUF_PRINTF(")"))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

void ObExecParamRawExpr::set_param_index(int64_t index)
{
  ObObjParam val;
  val.set_unknown(index);
  val.set_param_meta();
  set_value(val);
  set_result_type(outer_expr_->get_result_type());
}

int64_t ObExecParamRawExpr::get_param_index() const
{
  int64_t param_index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(get_value().is_unknown())) {
    param_index = get_value().get_unknown();
  }
  return param_index;
}

int ObExecParamRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObConstRawExpr::assign(other))) {
      LOG_WARN("failed to copy exec param expr", K(ret));
    } else {
      const ObExecParamRawExpr &tmp = static_cast<const ObExecParamRawExpr &>(other);
      outer_expr_ = tmp.outer_expr_;
      is_onetime_ = tmp.is_onetime_;
      ref_same_dblink_ = tmp.ref_same_dblink_;
    }
  }
  return ret;
}

int ObExecParamRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObConstRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to inner deep copy", K(ret));
  } else if (!is_onetime()) {
    // do nothing
  } else if (OB_FAIL(copier.copy(outer_expr_))) {
    LOG_WARN("failed to copy outer expr", K(ret));
  }
  return ret;
}

int ObExecParamRawExpr::replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                                     const common::ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                             new_exprs,
                                             outer_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  }
  return ret;
}

bool ObExecParamRawExpr::inner_same_as(const ObRawExpr &expr,
                                       ObExprEqualCheckContext *check_context) const
{
  bool bret = false;
  if (this == &expr) {
    bret = true;
  } else if (expr.is_exec_param_expr()) {
    const ObRawExpr *ref_expr = get_ref_expr();
    const ObRawExpr *other_ref_expr =  static_cast<const ObExecParamRawExpr &>(expr).get_ref_expr();
    if (NULL != ref_expr && NULL != other_ref_expr) {
      bret = ref_expr->same_as(*other_ref_expr, check_context);
    }
  }
  return bret;
}

int ObExecParamRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

uint64_t ObExecParamRawExpr::hash_internal(uint64_t seed) const
{
  if (NULL == outer_expr_) {
    // do nothing
  } else {
    seed = outer_expr_->hash(seed);
  }
  return seed;
}

int ObExecParamRawExpr::get_name_internal(char *buf,
                                          const int64_t buf_len,
                                          int64_t &pos,
                                          ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_DBLINK_STMT != type) {
    if (OB_FAIL(ObConstRawExpr::get_name_internal(buf, buf_len, pos, type))) {
      LOG_WARN("failed to print const raw expr", K(ret));
    }
  } else {
    if (OB_FAIL(ObLinkStmtParam::write(buf, buf_len, pos, get_value().get_unknown()))) {
      LOG_WARN("failed to write param to buf", K(ret));
    }
  }
  return ret;
}

int64_t ObExecParamRawExpr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ITEM_TYPE, type_,
       N_RESULT_TYPE, result_type_,
       N_EXPR_INFO, info_,
       N_VALUE, value_,
       K_(outer_expr),
       K_(is_onetime));
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////
void ObColumnRefRawExpr::set_column_attr(const ObString &table_name,
                                         const ObString &column_name)
{
  /* NOTE: we just point to the buffer without owning it. */
  table_name_.assign_ptr(table_name.ptr(), table_name.length());
  column_name_.assign_ptr(column_name.ptr(), column_name.length());
}

uint64_t ObColumnRefRawExpr::hash_internal(uint64_t seed) const
{
  seed = common::do_hash(table_id_, seed);
  seed = common::do_hash(column_id_, seed);
  return seed;
}

int ObColumnRefRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObTerminalRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObTerminalRawExpr failed", K(ret));
    } else {
      const ObColumnRefRawExpr &tmp =
          static_cast<const ObColumnRefRawExpr &>(other);
      table_id_ = tmp.table_id_;
      column_id_ = tmp.column_id_;
      database_name_ = tmp.database_name_;
      table_name_ = tmp.table_name_;
      synonym_name_ = tmp.synonym_name_;
      synonym_db_name_ = tmp.synonym_db_name_;
      column_name_ = tmp.column_name_;
      column_flags_ = tmp.column_flags_;
      dependant_expr_ = tmp.dependant_expr_;
      is_unpivot_mocked_column_ = tmp.is_unpivot_mocked_column_;
      is_hidden_ = tmp.is_hidden_;
      is_lob_column_ = tmp.is_lob_column_;
      is_joined_dup_column_ = tmp.is_joined_dup_column_;
      from_alias_table_ = tmp.from_alias_table_;
      is_rowkey_column_ = tmp.is_rowkey_column_;
      is_unique_key_column_ = tmp.is_unique_key_column_;
      is_mul_key_column_ = tmp.is_mul_key_column_;
      is_strict_json_column_ = tmp.is_strict_json_column_;
      srs_id_ = tmp.srs_id_;
      udt_set_id_ = tmp.udt_set_id_;
    }
  }
  return ret;
}

int ObColumnRefRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("copy in Base class ObTerminalRawExpr failed", K(ret));
  } else if (OB_FAIL(copier.copy(dependant_expr_))) {
    LOG_WARN("failed to copy dependant expr", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, database_name_, database_name_))) {
      LOG_WARN("fail to write string", K(database_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, table_name_, table_name_))) {
      LOG_WARN("fail to write string", K(table_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, synonym_name_, synonym_name_))) {
      LOG_WARN("fail to write string", K(synonym_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, synonym_db_name_,
                                       synonym_db_name_))) {
      LOG_WARN("fail to write string", K(synonym_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, column_name_, column_name_))) {
      LOG_WARN("fail to write string", K(column_name_), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObColumnRefRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                     const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (NULL != dependant_expr_ &&
             OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    dependant_expr_))) {
    LOG_WARN("failed to replace dependant exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObColumnRefRawExpr::reset()
{
  ObTerminalRawExpr::reset();
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  database_name_.reset();
  table_name_.reset();
  column_name_.reset();
}

bool ObColumnRefRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  UNUSED(check_context);
  bool bool_ret = false;

  if (OB_UNLIKELY(check_context != NULL) && OB_UNLIKELY(check_context->override_column_compare_)) {
    // use new column compare method
    bool_ret = check_context->compare_column(*this,
                                             static_cast<const ObColumnRefRawExpr &>(expr));
  } else if (get_expr_type() != expr.get_expr_type()) {
  } else {
    const ObColumnRefRawExpr *b_expr = static_cast<const ObColumnRefRawExpr *>(&expr);
    if (this->get_table_id() == b_expr->get_table_id()
        && this->get_column_id() == b_expr->get_column_id()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObExprEqualCheckContext::compare_column(const ObColumnRefRawExpr &left, const ObColumnRefRawExpr &right)
{
  return left.get_result_type() == right.get_result_type();
}

int ObColumnRefRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObColumnRefRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                          ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_DBLINK_STMT == type || EXPLAIN_HINT_FORMAT == type) {
    if (!table_name_.empty() &&
        OB_FAIL(BUF_PRINTF("%.*s.", table_name_.length(), table_name_.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%.*s", column_name_.length(), column_name_.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else {
    if (OB_FAIL(BUF_PRINTF("%.*s.%.*s", table_name_.length(), table_name_.ptr(),
                           column_name_.length(), column_name_.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      if (EXPLAIN_EXTENDED == type) {
        if (OB_FAIL(BUF_PRINTF("("))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      }
    }
  }
  return ret;
}

const ObRawExpr *ObAliasRefRawExpr::get_ref_expr() const
{
  const ObRawExpr *ret = NULL;
  if (project_index_ == OB_INVALID_INDEX) {
    ret = ref_expr_;
  } else if (ref_expr_->is_query_ref_expr()) {
    ObQueryRefRawExpr *subquery = static_cast<ObQueryRefRawExpr *>(ref_expr_);
    ObSelectStmt *stmt = NULL;
    if (OB_ISNULL(stmt = subquery->get_ref_stmt()) ||
        OB_UNLIKELY(project_index_ < 0 ||
                    project_index_ >= stmt->get_select_item_size())) {
      // do nothing
    } else {
      ret = stmt->get_select_item(project_index_).expr_;
    }
  }
  return ret;
}

ObRawExpr *ObAliasRefRawExpr::get_ref_expr()
{
  ObRawExpr *ret = NULL;
  if (project_index_ == OB_INVALID_INDEX) {
    ret = ref_expr_;
  } else if (ref_expr_->is_query_ref_expr()) {
    ObQueryRefRawExpr *subquery = static_cast<ObQueryRefRawExpr *>(ref_expr_);
    ObSelectStmt *stmt = NULL;
    if (OB_ISNULL(stmt = subquery->get_ref_stmt()) ||
        OB_UNLIKELY(project_index_ < 0 ||
                    project_index_ >= stmt->get_select_item_size())) {
      // do nothing
    } else {
      ret = stmt->get_select_item(project_index_).expr_;
    }
  }
  return ret;
}

int ObAliasRefRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObRawExpr failed", K(ret));
    } else {
      const ObAliasRefRawExpr &alias_expr =
          static_cast<const ObAliasRefRawExpr &>(other);
      ref_expr_ = alias_expr.ref_expr_;
      project_index_ = alias_expr.project_index_;
    }
  }
  return ret;
}

int ObAliasRefRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to deep copy attributes", K(ret));
  } else if (OB_FAIL(copier.copy(ref_expr_))) {
    LOG_WARN("failed to copy ref expr", K(ret));
  }
  return ret;
}

int ObAliasRefRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                    const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_ERR_UNEXPECTED;
  if (OB_FAIL(ObRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    ref_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

const ObRawExpr *ObAliasRefRawExpr::get_param_expr(int64_t index) const
{
  ObRawExpr *expr = NULL;
  if (index < 0 || index > 1) {
    LOG_WARN_RET(OB_ERROR, "index out of range", K(index));
  } else {
    expr = ref_expr_;
  }
  return expr;
}

ObRawExpr *&ObAliasRefRawExpr::get_param_expr(int64_t index)
{
  ObRawExpr **expr = &USELESS_POINTER;
  if (index < 0 || index >= 1) {
    LOG_WARN_RET(OB_ERROR, "index out of range", K(index));
  } else {
    expr = &ref_expr_;
  }
  return *expr;
}

int ObAliasRefRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

uint64_t ObAliasRefRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_code = 0;
  if (ref_expr_ != NULL) {
   hash_code = ref_expr_->hash(seed);
  }
  return hash_code;
}

int ObAliasRefRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                         ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Ref expr is NULL", K(ret));
  } else if (OB_FAIL(ref_expr_->get_name(buf, buf_len, pos, type))) {
  } else { }//do nothing
  return ret;
}

bool ObAliasRefRawExpr::inner_same_as(const ObRawExpr &expr,
                                      ObExprEqualCheckContext *check_context) const
{
  UNUSED(check_context);
  bool bret = false;
  if (OB_ISNULL(ref_expr_)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "ref_expr_ is null");
  } else if (expr.get_expr_type() == get_expr_type()) {
    const ObAliasRefRawExpr &alias_ref = static_cast<const ObAliasRefRawExpr&>(expr);
    bret = (alias_ref.get_ref_expr() == get_ref_expr());
  }
  return bret;
}

////////////////////////////////////////////////////////////////
ObExprOperator *ObNonTerminalRawExpr::get_op()
{
  if (NULL == op_) {
    if (OB_ISNULL(inner_alloc_)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid inner alloc", K(inner_alloc_));
    } else if (type_ != T_OP_ROW) {
      ObExprOperatorFactory factory(*inner_alloc_);
      if (OB_SUCCESS != factory.alloc(type_, op_)) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "Can not malloc expression operator", "expr_type", get_type_name(type_), K(lbt()));
      } else if (OB_ISNULL(op_)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "alloc a null expr_op", K(op_));
      }
    }
  }
  return op_;
}

void ObNonTerminalRawExpr::free_op()
{
  if (NULL != op_) {
    //等待inner_alloc统一释放
    op_ = NULL;
  }
}

int ObNonTerminalRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObRawExpr failed", K(ret));
    } else {
      const ObNonTerminalRawExpr &tmp = static_cast<const ObNonTerminalRawExpr &>(other);
      if (OB_FAIL(this->set_input_types(tmp.input_types_))) {
        LOG_WARN("copy input types failed", K(ret));
      } else if (tmp.op_ != NULL) {
        ObExprOperator *this_op = get_op();
        if (OB_ISNULL(this_op)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocated memory for expr operator failed", K_(type));
        } else if (OB_FAIL(this_op->assign(*tmp.op_))) {
          LOG_WARN("assign this operator failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObNonTerminalRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret), K(inner_alloc_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < input_types_.count(); ++i) {
      ObObj param;
      if (OB_FAIL(ob_write_obj(*inner_alloc_, input_types_.at(i).get_param(), param))) {
        LOG_WARN("failed to write object", K(ret));
      } else {
        input_types_.at(i).set_param(param);
      }
    }
  }
  return ret;
}

int ObNonTerminalRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                       const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr for base class ObRawExpr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}


////////////////////////////////////////////////////////////////
ObOpRawExpr::ObOpRawExpr(ObRawExpr *first_expr,
                         ObRawExpr *second_expr,
                         ObItemType type)
  : ObIRawExpr(),
    ObNonTerminalRawExpr(),
    jit::expr::ObOpExpr(),
    subquery_key_(T_WITH_NONE),
    deduce_type_adding_implicit_cast_(true)
{
  set_param_exprs(first_expr, second_expr);
  set_expr_type(type);
  set_expr_class(EXPR_OPERATOR);
}

int ObOpRawExpr::set_param_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(0 == exprs_.count())) {
    if (OB_FAIL(exprs_.push_back(expr))) {
      LOG_WARN("failed to set param", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be set twice", K(ret));
  }
  return ret;
}

int ObOpRawExpr::set_param_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr, ObRawExpr *third_expr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(0 == exprs_.count())) {
    if (OB_FAIL(exprs_.push_back(first_expr))) {
      LOG_WARN("failed to set param", K(ret));
    } else if (OB_FAIL(exprs_.push_back(second_expr))) {
      LOG_WARN("failed to set param", K(ret));
    } else if (OB_FAIL(exprs_.push_back(third_expr))) {
      LOG_WARN("failed to set param", K(ret));
    } else {}
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be set twice", K(ret));
  }
  return ret;
}

int ObOpRawExpr::set_param_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr)
{
  int ret = OB_SUCCESS;
  ObItemType exchange_type = T_MIN_OP;
  switch (get_expr_type()) {
    case T_OP_LE:
      exchange_type = T_OP_GE;
      break;
    case T_OP_LT:
      exchange_type = T_OP_GT;
      break;
    case T_OP_GE:
      exchange_type = T_OP_LE;
      break;
    case T_OP_GT:
      exchange_type = T_OP_LT;
      break;
    case T_OP_EQ:
    case T_OP_NSEQ:
    case T_OP_NE:
      exchange_type = get_expr_type();
      break;
    default:
      exchange_type = T_MIN_OP;
      break;
  }
  if (T_MIN_OP != exchange_type && first_expr && first_expr->has_flag(IS_CONST)
      && second_expr && second_expr->has_flag(IS_COLUMN)) {
    set_expr_type(exchange_type);
    ObRawExpr *tmp_expr = second_expr;
    second_expr = first_expr;
    first_expr = tmp_expr;
  }

  if (0 == exprs_.count()) {
    if (OB_ISNULL(first_expr) || OB_ISNULL(second_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(first_expr), K(second_expr), K(ret));
    } else if (OB_FAIL(exprs_.push_back(first_expr))) {
      LOG_WARN("failed to push back first_expr", K(ret));
    } else if (OB_FAIL(exprs_.push_back(second_expr))) {
      LOG_WARN("failed to push back gc_expr", K(ret));
    } else {}
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be set twice", K(ret));
  }
  return ret;
}

void ObOpRawExpr::reset()
{
  ObNonTerminalRawExpr::reset();
  clear_child();
  deduce_type_adding_implicit_cast_ = true;
}

//used for jit expr
//在获取jit_exprs_时才初始化，而不是在初始化exprs_时一起初始化原因:
//在resolver时解析ObColumnRefRawExpr只是将一个未初始化的ObColumnRefRawExpr
//对象push到exprs_中, 在后面会再进行该对象的替换, 所以此处没有一起初始化exprs_。
//

int ObOpRawExpr::get_children(ExprArray &jit_exprs) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCCESS == ret && i < exprs_.count(); i++) {
    if (OB_FAIL(jit_exprs.push_back(static_cast<ObIRawExpr *>(exprs_.at(i))))) {
      LOG_WARN("fail to gen ObIRawExpr Array", K(ret));
    }
  }

  return ret;
}

int ObOpRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObNonTerminalRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObNonTerminalRawExpr failed", K(ret));
    } else {
      const ObOpRawExpr &tmp = static_cast<const ObOpRawExpr &>(other);
      if (OB_FAIL(exprs_.assign(tmp.exprs_))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else {
        subquery_key_ = tmp.subquery_key_;
        deduce_type_adding_implicit_cast_ = tmp.deduce_type_adding_implicit_cast_;
      }
    }
  }
  return ret;
}

int ObOpRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                              const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObNonTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObOpRawExpr::clear_child()
{
  exprs_.reset();
}

int ObOpRawExpr::get_subquery_comparison_flag() const
{
  enum {
    INVALID = 0, // not subquery comparison
    NONE = 1,
    ALL = 2,
    ANY = 3
  } comparison_flag;
  comparison_flag = INVALID;
  if (IS_SUBQUERY_COMPARISON_OP(get_expr_type())) {
    if (has_flag(IS_WITH_ALL)) {
      comparison_flag = ALL;
    } else if (has_flag(IS_WITH_ANY)) {
      comparison_flag = ANY;
    } else {
      comparison_flag = NONE;
    }
  }
  return comparison_flag;
}

bool ObOpRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  bool need_cmp = true;

  enum {
    REGULAR_CMP = 1,
    REVERSE_CMP = 2,
    BOTH_CMP = 3
  }cmp_type;

  cmp_type = REGULAR_CMP;

  if (get_expr_type() == T_OP_ASSIGN) {
    need_cmp = false;
  } else if (T_OP_EQ == get_expr_type()
             || T_OP_NSEQ == get_expr_type()
             || T_OP_NE == get_expr_type()) {
    if (expr.get_expr_type() == get_expr_type()) {
      cmp_type = BOTH_CMP;
    } else {
      need_cmp = false;
    }
  } else if (IS_COMMON_COMPARISON_OP(get_expr_type())) {
    // GE, GT, LE, LT
    if (expr.get_expr_type() == get_expr_type()) {
      cmp_type = REGULAR_CMP;
    } else if ((T_OP_GE == expr.get_expr_type() && T_OP_LE == get_expr_type())
               || (T_OP_GT == expr.get_expr_type() && T_OP_LT == get_expr_type())
               || (T_OP_GE == get_expr_type() && T_OP_LE == expr.get_expr_type())
               || (T_OP_GT == get_expr_type() && T_OP_LT == expr.get_expr_type())) {
      cmp_type = REVERSE_CMP;
    } else {
      need_cmp = false;
    }
  } else if (IS_SUBQUERY_COMPARISON_OP(get_expr_type())) {
    const ObOpRawExpr &tmp = static_cast<const ObOpRawExpr &>(expr);
    if (tmp.get_expr_type() != get_expr_type() ||
        tmp.get_subquery_comparison_flag() != get_subquery_comparison_flag()) {
      need_cmp = false;
    }
  } else if (expr.get_expr_type() != get_expr_type()) {
    need_cmp = false;
  }
  if (need_cmp) {
    if (BOTH_CMP == cmp_type || REGULAR_CMP == cmp_type) {
      if (this->get_param_count() == expr.get_param_count()) {
        bool_ret = true;
        for (int64_t i = 0; bool_ret && i < expr.get_param_count(); ++i) {
          if (NULL == this->get_param_expr(i) || NULL == expr.get_param_expr(i)
              || !(this->get_param_expr(i)->same_as(*expr.get_param_expr(i), check_context))) {
            bool_ret = false;
          }
        }
      }
    }
    if (!bool_ret && (BOTH_CMP == cmp_type || REVERSE_CMP == cmp_type)) {
      if (NULL == this->get_param_expr(0) || NULL == expr.get_param_expr(0)
          || NULL == this->get_param_expr(1) || NULL == expr.get_param_expr(1)) {
        /* bool_ret = false; */
      } else {
        bool_ret = this->get_param_expr(0)->same_as(*expr.get_param_expr(1), check_context)
            && this->get_param_expr(1)->same_as(*expr.get_param_expr(0), check_context);
      }
    }
  }
  return bool_ret;
}

int ObOpRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObOpRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                   ExplainType type) const {
  int ret = OB_SUCCESS;
  ObString symbol;
  switch (get_expr_type()) {
    case T_OP_ADD:
    case T_OP_AGG_ADD:
      symbol = "+";
      break;
    case T_OP_MINUS:
    case T_OP_AGG_MINUS:
      symbol = "-";
      break;
    case T_OP_MUL:
    case T_OP_AGG_MUL:
      symbol = "*";
      break;
    case T_OP_DIV:
    case T_OP_AGG_DIV:
      symbol = "/";
      break;
    case T_OP_MOD:
      symbol = (EXPLAIN_DBLINK_STMT == type) ? "MOD" : "%";
      break;
    case T_OP_INT_DIV:
      symbol = "DIV";
      break;
    case T_OP_LE:
    case T_OP_SQ_LE:
      symbol = "<=";
      break;
    case T_OP_LT:
    case T_OP_SQ_LT:
      symbol = "<";
      break;
    case T_OP_EQ:
    case T_OP_SQ_EQ:
      symbol = "=";
      break;
    case T_OP_NSEQ:
    case T_OP_SQ_NSEQ:
      symbol = "<=>";
      break;
    case T_OP_GE:
    case T_OP_SQ_GE:
      symbol = ">=";
      break;
    case T_OP_GT:
    case T_OP_SQ_GT:
      symbol = ">";
      break;
    case T_OP_NE:
    case T_OP_SQ_NE:
      symbol = "!=";
      break;
    case T_OP_AND:
      symbol = "AND";
      break;
    case T_OP_OR:
      symbol = "OR";
      break;
    case T_OP_IN:
      symbol = "IN";
      break;
    case T_OP_POW:
      symbol = "POW";
      break;
    case T_OP_BIT_XOR:
      symbol = "^";
      break;
    case T_OP_XOR:
      symbol = "XOR";
      break;
    case T_OP_MULTISET:
      symbol = "MULTISET";
      break;
    case T_OP_COLL_PRED:
      symbol = "COLL_PRED";
      break;
    case T_OP_IS:
      symbol = "IS";
      break;
    case T_OP_IS_NOT:
      symbol = "IS NOT";
      break;
    default:
      break;
  }
  if (IS_SUBQUERY_COMPARISON_OP(get_expr_type())) {
    if (OB_FAIL(get_subquery_comparison_name(symbol, buf, buf_len, pos, type))) {
      LOG_WARN("get subquery comparison name failed", K(ret));
    }
  } else if ((!symbol.empty() && 2 == get_param_count())) {
    if (OB_ISNULL(get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first param expr is NULL", K(ret));
    } else if (OB_ISNULL(get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second param expr is NULL", K(ret));
    } else if (EXPLAIN_DBLINK_STMT == type && T_OP_MOD == get_expr_type()) {
      if (OB_FAIL(BUF_PRINTF("%.*s", symbol.length(), symbol.ptr()))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(get_param_expr(0)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(get_param_expr(1)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else {
      if (EXPLAIN_DBLINK_STMT == type && OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(get_param_expr(0)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%.*s", symbol.length(), symbol.ptr()))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(get_param_expr(1)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (EXPLAIN_DBLINK_STMT == type && OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
  } else if (T_OP_ROW == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          }
        } else {}
      }
      if (OB_SUCCESS == ret && OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
  } else if (T_OP_ORACLE_OUTER_JOIN_SYMBOL == get_expr_type()) {
    if (OB_UNLIKELY(1 != get_param_count())
        || OB_ISNULL(get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first param expr is NULL", K(ret), K(get_param_count()));
    } else if (OB_FAIL(get_param_expr(0)->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("fail to get_name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("(+)"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (has_flag(IS_INNER_ADDED_EXPR)
             && (EXPLAIN_EXTENDED != type && EXPLAIN_EXTENDED_NOADDR != type)
             && T_OP_BOOL == get_expr_type()) {
    CK(1 == get_param_count());
    OZ(get_param_expr(0)->get_name(buf, buf_len, pos, type));
    LOG_DEBUG("debug print wrapper inner", K(ret), K(lbt()));
  } else if (T_OP_BOOL == get_expr_type()) {
    if (OB_UNLIKELY(1 != get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_count of bool expr is invalid", K(ret), K(get_param_count()));
    } else {
      if (OB_FAIL(BUF_PRINTF("BOOL("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_ISNULL(get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is NULL", K(ret));
      } else if (OB_FAIL(get_param_expr(0)->get_name_internal(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name_internal", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
  } else if (T_OP_RUNTIME_FILTER == get_expr_type()) {
    if (RuntimeFilterType::BLOOM_FILTER == runtime_filter_type_) {
      if (OB_FAIL(BUF_PRINTF("RF_BLOOM_FILTER("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else if (RuntimeFilterType::RANGE == runtime_filter_type_) {
      if (OB_FAIL(BUF_PRINTF("RF_RANGE_FILTER("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else if (RuntimeFilterType::IN == runtime_filter_type_) {
      if (OB_FAIL(BUF_PRINTF("RF_IN_FILTER("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
          }
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
        }
      }
    }
  } else if (T_OP_TO_OUTFILE_ROW == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("SYS_OP_TO_OUTFILE_ROW("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
          }
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
        }
      }
    }
  } else if (T_OP_OUTPUT_PACK == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("INTERNAL_FUNCTION("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
          }
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      }
    }
  } else if (T_FUN_SYS_HASH == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("HASH("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
          }
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      }
    }
  } else if (T_FUN_SYS_BM25 == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("BM25(k1=1.2, b=0.75, epsilon=0.25)"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (T_OP_PUSHDOWN_TOPN_FILTER == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("TOPN_FILTER("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (i < get_param_count() - 1) {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
          }
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(BUF_PRINTF("(%s", get_type_name(get_expr_type())))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_param_count() ; ++i) {
        if (OB_ISNULL(get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
        } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else {}
      }
      if (OB_SUCCESS == ret && OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else {}
    }
  }
  return ret;
}

int ObOpRawExpr::get_subquery_comparison_name(const ObString &symbol,
                                              char *buf,
                                              int64_t buf_len,
                                              int64_t &pos,
                                              ExplainType type) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *param1 = NULL;
  const ObRawExpr *param2 = NULL;
  ObString subquery_keyname = "";
  if (has_flag(IS_WITH_ANY)) {
    subquery_keyname = "ANY";
  } else if (has_flag(IS_WITH_ALL)) {
    subquery_keyname = "ALL";
  }
  if (OB_UNLIKELY(symbol.empty()) || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(symbol), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(get_param_count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr param count is unexpected", K(get_param_count()));
  } else if (OB_ISNULL(param1 = get_param_expr(0)) || OB_ISNULL(param2 = get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(param1), K(param2));
  } else if (OB_FAIL(param1->get_name(buf, buf_len, pos, type))) {
    LOG_WARN("get param name failed", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" %.*s %.*s", symbol.length(), symbol.ptr(),
                                subquery_keyname.length(), subquery_keyname.ptr()))) {
    LOG_WARN("print symbol name failed", K(ret));
  } else if (!subquery_keyname.empty() && OB_FAIL(BUF_PRINTF("("))) {
    LOG_WARN("print paren failed", K(ret));
  } else if (OB_FAIL(param2->get_name(buf, buf_len, pos, type))) {
    LOG_WARN("get param2 name failed", K(ret));
  } else if (!subquery_keyname.empty() && OB_FAIL(BUF_PRINTF(")"))) {
    LOG_WARN("print paren failed", K(ret));
  }
  return ret;
}

void ObOpRawExpr::set_expr_type(ObItemType type)
{
  type_ = type;
  if ((T_OP_IN == type_  || T_OP_NOT_IN == type_)) {
    set_deduce_type_adding_implicit_cast(false);
  }
}

bool ObOpRawExpr::is_white_runtime_filter_expr() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  if (OB_FAIL(OB_E(EventTable::EN_PX_DISABLE_WHITE_RUNTIME_FILTER) OB_SUCCESS)) {
    LOG_WARN("disable push down white filter", K(ret));
    return false;
  }
  // FIXME: @zhouhaiyu.zhy
  // The in runtime filter is forbidden to push down as white filter because
  // the white pushdown filter with type WHITE_OP_IN is not available now
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) {
    bool_ret = false;
  } else if (with_null_equal_cond()) {
    // <=> join is not allowed to pushdown as white filter
    bool_ret = false;
  } else if (RANGE == runtime_filter_type_ /*|| IN == runtime_filter_type_*/) {
    for (int i = 0; i < exprs_.count(); ++i) {
      if (T_REF_COLUMN != exprs_.at(i)->get_expr_type()) {
        bool_ret = false;
        break;
      }
    }
  // sort is compare in vectorize format, so only one column can pushdown as
  // white filter
  } else if (T_OP_PUSHDOWN_TOPN_FILTER == type_ && 1 == exprs_.count()
             && T_REF_COLUMN == exprs_.at(0)->get_expr_type()) {
    // FIXME: @zhouhaiyu.zhy
    // for now, storage pushdown filter can not process both a < 10 and a is null in one filter
    // so disable white topn runtime filter
    // LOG_TRACE("[TopN Filter] push topn filter as white filter");
    bool_ret = false;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

////////////////////////////////////////////////////////////////

int ObPLAssocIndexRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObOpRawExpr failed", K(ret));
    } else {
      const ObPLAssocIndexRawExpr &tmp =
          static_cast<const ObPLAssocIndexRawExpr &>(other);
      for_write_ = tmp.for_write_;
      out_of_range_set_err_ = tmp.out_of_range_set_err_;
      parent_type_ = tmp.parent_type_;
      is_index_by_varchar_ = tmp.is_index_by_varchar_;
    }
  }
  return ret;
}

int ObObjAccessRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObOpRawExpr failed", K(ret));
    } else {
      const ObObjAccessRawExpr &tmp =
          static_cast<const ObObjAccessRawExpr &>(other);
      if (OB_FAIL(append(access_indexs_, tmp.access_indexs_))) {
        LOG_WARN("append error", K(ret));
      } else if (OB_FAIL(append(var_indexs_, tmp.var_indexs_))) {
        LOG_WARN("append error", K(ret));
      } else if (OB_FAIL(append(orig_access_indexs_, tmp.orig_access_indexs_))) {
        LOG_WARN("append error", K(ret));
      } else {
        get_attr_func_ = tmp.get_attr_func_;
        func_name_ = tmp.func_name_;
        for_write_ = tmp.for_write_;
        property_type_ = tmp.property_type_;
      }
    }
  }
  return ret;
}

int ObObjAccessRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to inner deep copy raw expr", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator is NULL", K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, func_name_, func_name_))) {
      LOG_WARN("Failed to write string", K(func_name_), K(ret));
    } else {
      pl::ObObjAccessIdx access;
      common::ObSEArray<pl::ObObjAccessIdx, 4> access_array;
      pl::ObObjAccessIdx orig_access;
      common::ObSEArray<pl::ObObjAccessIdx, 4> orig_access_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_indexs_.count(); ++i) {
        access.reset();
        if (OB_FAIL(access.deep_copy(*inner_alloc_, copier.get_expr_factory(), access_indexs_.at(i)))) {
          LOG_WARN("Failed to deep copy ObObjAccessIdx", K(i), K(access_indexs_.at(i)), K(ret));
        } else if (OB_FAIL(access_array.push_back(access))) {
          LOG_WARN("push back error", K(i), K(access_indexs_.at(i)), K(access), K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret) && OB_FAIL(access_indexs_.assign(access_array))) {
        LOG_WARN("assign array error", K(access_array), K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < orig_access_indexs_.count(); ++i) {
        orig_access.reset();
        if (OB_FAIL(orig_access.deep_copy(*inner_alloc_, copier.get_expr_factory(), orig_access_indexs_.at(i)))) {
          LOG_WARN("failed to deep copy ObObjAccessIdx", K(i), K(orig_access_indexs_.at(i)), K(ret));
        } else if (OB_FAIL(orig_access_array.push_back(orig_access))) {
          LOG_WARN("push back error", K(i), K(orig_access_indexs_.at(i)), K(orig_access), K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret) && OB_FAIL(orig_access_indexs_.assign(orig_access_array))) {
        LOG_WARN("assign array error", K(orig_access_array), K(ret));
      }
    }
  }
  return ret;
}

bool ObObjAccessRawExpr::inner_same_as(const ObRawExpr &expr,
                                       ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() != expr.get_expr_type()) {
  } else if (ObOpRawExpr::inner_same_as(expr, check_context)) {
    const ObObjAccessRawExpr &obj_access_expr = static_cast<const ObObjAccessRawExpr &>(expr);
    bool_ret = get_attr_func_ == obj_access_expr.get_attr_func_
        && 0 == func_name_.case_compare(obj_access_expr.func_name_)
        && is_array_equal(access_indexs_, obj_access_expr.access_indexs_)
        && is_array_equal(var_indexs_, obj_access_expr.var_indexs_)
        && for_write_ == obj_access_expr.for_write_
        && property_type_ == obj_access_expr.property_type_
        && is_array_equal(orig_access_indexs_, obj_access_expr.orig_access_indexs_);
  } else { /*do nothing*/ }
  return bool_ret;
}

int ObObjAccessRawExpr::add_access_indexs(const ObIArray<pl::ObObjAccessIdx> &access_idxs)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<pl::ObObjAccessIdx, 4> expr_access;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idxs.count(); ++i) {
    const pl::ObObjAccessIdx &access_idx = access_idxs.at(i);
    switch (access_idx.access_type_) {
    case pl::ObObjAccessIdx::IS_LOCAL: {
      //obj access是变量，access index是变量的下标，
      //但是obj access expr中变量只能通过参数来传递，所以把变量记录下来
      pl::ObObjAccessIdx tmp_idx = access_idxs.at(i);
      tmp_idx.var_index_ = var_indexs_.count();
      if (OB_FAIL(expr_access.push_back(tmp_idx))) {
        LOG_WARN("store access index failed", K(ret));
      } else if (OB_FAIL(var_indexs_.push_back(access_idx.var_index_))) {
        LOG_WARN("store var index failed", K(ret));
      } else { /*do nothing*/ }
    }
    break;
    case pl::ObObjAccessIdx::IS_DB_NS: //fallthrough
    case pl::ObObjAccessIdx::IS_PKG_NS: {
      //skip
    }
    break;
    case pl::ObObjAccessIdx::IS_PKG: //fallthrough
    case pl::ObObjAccessIdx::IS_USER: //fallthrough
    case pl::ObObjAccessIdx::IS_SESSION: //fallthrough
    case pl::ObObjAccessIdx::IS_GLOBAL:
    case pl::ObObjAccessIdx::IS_SUBPROGRAM_VAR: {
      //do nothing
    }
    break;
    case pl::ObObjAccessIdx::IS_EXPR: {
      //do nothing
    }
    break;
    case pl::ObObjAccessIdx::IS_UDF_NS:
    case pl::ObObjAccessIdx::IS_UDT_NS: {
      //do nothing
    }
    break;
    case pl::ObObjAccessIdx::IS_CONST:
    case pl::ObObjAccessIdx::IS_PROPERTY: {
      if (OB_FAIL(expr_access.push_back(access_idx))) {
        LOG_WARN("store access index failed", K(ret));
#ifdef OB_BUILD_ORACLE_PL
      } else if (access_idx.is_property()) {
        if (0 == access_idx.var_name_.case_compare("count")) {
          set_property(pl::ObAssocArrayType::COUNT_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("first")) {
          set_property(pl::ObAssocArrayType::FIRST_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("last")) {
          set_property(pl::ObAssocArrayType::LAST_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("limit")) {
          set_property(pl::ObAssocArrayType::LIMIT_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("prior")) {
          set_property(pl::ObAssocArrayType::PRIOR_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("next")) {
          set_property(pl::ObAssocArrayType::NEXT_PROPERTY);
        } else if (0 == access_idx.var_name_.case_compare("exists")) {
          set_property(pl::ObAssocArrayType::EXISTS_PROPERTY);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid property type", K(i), K(access_idx), K(ret));
        }
#endif
      } else { /*do nothing*/ }
    }
    break;
    case pl::ObObjAccessIdx::IS_INTERNAL_PROC:
    case pl::ObObjAccessIdx::IS_EXTERNAL_PROC:
    case pl::ObObjAccessIdx::IS_TYPE_METHOD: {
      //do nothing
    }
    break;
    case pl::ObObjAccessIdx::IS_LABEL_NS: {
      //do nothing
    }
    break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid access type", K(i), K(access_idx), K(ret));
    }
    break;
    }
  }

  /*
   * 把外部表达式作为ObObjAccessRawExpr的child，在后缀表达式计算框架里会被首先计算出来，然后压入param_array供get_attr_func使用。
   * 所有外部表达式压在var_indexs_的后面，所以在上一轮for循环不处理，而放在这一轮循环处理。
   * 所以会有ObExprObjAccess::init_param_array的时候压完param_store里的变量之后要压入obj_stack.
   * */
  for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < access_idxs.count(); ++i) {
    const pl::ObObjAccessIdx &access_idx = access_idxs.at(i);
    if (access_idx.is_external() || access_idx.is_udf_type()) {
      pl::ObObjAccessIdx tmp_idx = access_idx;
      tmp_idx.var_index_ = get_param_count() + var_indexs_.count(); //外部变量的下标排在
      if (OB_FAIL(access_indexs_.push_back(tmp_idx))) {
        LOG_WARN("store access index failed", K(ret));
      } else if (OB_FAIL(add_param_expr(access_idx.get_sysfunc_))) {
        LOG_WARN("Failed to add param expr", K(ret));
      } else { /*do nothing*/ }
    } else if ( pl::ObObjAccessIdx::AccessType::IS_EXPR == access_idx.access_type_) {
      pl::ObObjAccessIdx tmp_idx = access_idx;
      tmp_idx.var_index_ = get_param_count() + var_indexs_.count(); //外部变量的下标排在
      if (OB_FAIL(access_indexs_.push_back(tmp_idx))) {
        LOG_WARN("store access index failed", K(ret));
      } else if (OB_FAIL(add_param_expr(access_idx.get_sysfunc_))) {
        LOG_WARN("Failed to add param expr", K(ret));
      } else { /*do nothing*/ }
    } else if (pl::ObObjAccessIdx::AccessType::IS_PROPERTY == access_idx.access_type_
              && ((0 == access_idx.var_name_.case_compare("prior"))
              || (0 == access_idx.var_name_.case_compare("next"))
              || (0 == access_idx.var_name_.case_compare("exists")))) {
      pl::ObObjAccessIdx tmp_idx = access_idx;
      tmp_idx.var_index_ = get_param_count() + var_indexs_.count(); //外部变量的下标排在
      if (OB_FAIL(access_indexs_.push_back(tmp_idx))) {
        LOG_WARN("store access index failed", K(ret));
      } else if (OB_NOT_NULL(access_idx.get_sysfunc_) && OB_FAIL(add_param_expr(access_idx.get_sysfunc_))) {
        LOG_WARN("Failed to add param expr", K(ret));
      } else { /*do nothing*/ }
    } else if (access_idx.is_ns()
              || access_idx.is_procedure()
              || access_idx.is_type_method()
              || access_idx.is_label()) {
      //skip
    } else {
      if (OB_FAIL(access_indexs_.push_back(expr_access.at(j++)))) {
        LOG_WARN("store access index failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(orig_access_indexs_.push_back(access_idx))) {
      LOG_WARN("failed to assign access indexs", K(ret), K(access_idx));
    }
  }

  return ret;
}

int ObObjAccessRawExpr::get_final_type(pl::ObPLDataType &type) const
{
  int ret = OB_SUCCESS;
  if (access_indexs_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid obj access expr, indexs is empty", K(ret));
  } else {
    type = access_indexs_.at(access_indexs_.count() - 1).elem_type_;
  }
  return ret;
}

int ObNonTerminalRawExpr::set_input_types(const ObIExprResTypes &input_types)
{
  int ret = OB_SUCCESS;
  // 如果原来有数据，立即清除。防御性代码。
  input_types_.reset();
  if (OB_ISNULL(inner_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner allocator is NULL", K(ret));
  } else if (OB_FAIL(input_types_.prepare_allocate(input_types.count()))) {
    LOG_WARN("fail to prepare allocator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_types.count(); ++i) {
    input_types_.at(i).set_allocator(inner_alloc_);
    if (OB_FAIL(input_types_.at(i).assign(input_types.at(i)))) {
      LOG_WARN("fail to assign input types", K(input_types), K(input_types_), K(ret));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObCaseOpRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObNonTerminalRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObNonTerminalRawExpr failed", K(ret));
    } else {
      const ObCaseOpRawExpr &tmp =
          static_cast<const ObCaseOpRawExpr &>(other);
      arg_expr_ = tmp.arg_expr_;
      default_expr_ = tmp.default_expr_;
      is_decode_func_ = tmp.is_decode_func_;
      if (OB_FAIL(when_exprs_.assign(tmp.when_exprs_))) {
        LOG_WARN("failed to assign when exprs", K(ret));
      } else if (OB_FAIL(then_exprs_.assign(tmp.then_exprs_))) {
        LOG_WARN("failed to assign then exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObCaseOpRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                  const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObNonTerminalRawExpr::replace_expr(other_exprs,
                                                 new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    arg_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    default_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     when_exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     then_exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

//jit exprs存储顺序：
//[arg_expr], when_expr, then_expr, [when_expr, then_expr,]...[default_expr]
int ObCaseOpRawExpr::get_children(ExprArray &jit_exprs) const
{
  int ret = OB_SUCCESS;
  if (arg_expr_ != NULL) {
    if (OB_FAIL(jit_exprs.push_back(static_cast<ObIRawExpr *>(arg_expr_)))) {
      LOG_WARN("fail to push arg_expr_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (when_exprs_.count() != then_exprs_.count()) {
      LOG_WARN("when_exprs count is not equal to then_exprs count",
          K(when_exprs_.count()), K(then_exprs_.count()));
    }
    for (int64_t i = 0;
         OB_SUCCESS == ret && i < when_exprs_.count();
         i++) {
      if (OB_FAIL(jit_exprs.push_back(
                  static_cast<ObIRawExpr *>(when_exprs_.at(i))))) {
        LOG_WARN("fail to push when_expr_", K(ret));
      } else if (OB_FAIL(jit_exprs.push_back(
                  static_cast<ObIRawExpr *>(then_exprs_.at(i))))) {
        LOG_WARN("fail to push then_expr_", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && NULL != default_expr_) {
    if (OB_FAIL(jit_exprs.push_back(static_cast<ObIRawExpr *>(default_expr_)))) {
      LOG_WARN("fail to push arg_expr_", K(ret));
    }
  }
  return ret;
}

void ObCaseOpRawExpr::clear_child()
{
  arg_expr_ = NULL;
  when_exprs_.reset();
  then_exprs_.reset();
  default_expr_ = NULL;
}

void ObCaseOpRawExpr::reset()
{
  ObNonTerminalRawExpr::reset();
  clear_child();
}

const ObRawExpr *ObCaseOpRawExpr::get_param_expr(int64_t index) const
{
  ObRawExpr *expr = const_cast<ObCaseOpRawExpr *>(this)->get_param_expr(index);
  return &expr == &USELESS_POINTER ? NULL : expr;
}

int ObCaseOpRawExpr::replace_param_expr(int64_t index, ObRawExpr *new_expr)
{
  int ret = common::OB_SUCCESS;
  ObRawExpr *&old_expr = this->get_param_expr(index);
  if (OB_ISNULL(old_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace_param_expr for case expr failed", K(ret), K(index),
        K(get_when_expr_size()), K(get_then_expr_size()));
  } else {
    old_expr = new_expr;
  }
  return ret;
}

// return child expr in visitor order:
//    arg expr, when expr, then expr, when expr, then expr ..., default expr
ObRawExpr *&ObCaseOpRawExpr::get_param_expr(int64_t index)
{
  int ret = OB_SUCCESS;
  ObRawExpr **expr = &USELESS_POINTER;
  if (index < 0 || index >= get_param_count()) {
    LOG_WARN("invalid index", K(index), K(get_param_count()));
  } else if (when_exprs_.count() != then_exprs_.count()) {
    LOG_WARN("when and then expr count mismatch",
        K(ret), K(when_exprs_.count()), K(then_exprs_.count()));
  } else {
    if (NULL != arg_expr_ && 0 == index) {
      expr = &arg_expr_;
    } else {
      index -= (NULL != arg_expr_ ? 1 : 0);
      if (index >= when_exprs_.count() * 2) {
        if (NULL != default_expr_) {
          expr = &default_expr_;
        }
      } else {
        expr = &(index % 2 == 0 ? when_exprs_ : then_exprs_).at(index / 2);
      }
    }
  }
  return *expr;
}

bool ObCaseOpRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() != expr.get_expr_type()) {
  } else {
    const ObCaseOpRawExpr *c_expr = static_cast<const ObCaseOpRawExpr *>(&expr);
    if (((NULL == arg_expr_ && NULL == c_expr->arg_expr_)
         || (NULL != arg_expr_ && c_expr->arg_expr_ && arg_expr_->same_as(*c_expr->arg_expr_, check_context)))
        && ((NULL == default_expr_ && NULL == c_expr->default_expr_)
            || (NULL != default_expr_ && c_expr->default_expr_ && default_expr_->same_as(*c_expr->default_expr_, check_context)))
        && this->get_when_expr_size() == c_expr->get_when_expr_size()) {
      bool_ret = true;
      for (int64_t i = 0; bool_ret && i < c_expr->get_when_expr_size(); ++i) {
        if (OB_ISNULL(this->get_when_param_expr(i)) || OB_ISNULL(c_expr->get_when_param_expr(i))
            || OB_ISNULL(this->get_then_param_expr(i)) || OB_ISNULL(c_expr->get_then_param_expr(i))
            || !this->get_when_param_expr(i)->same_as(*c_expr->get_when_param_expr(i), check_context)
            || !this->get_then_param_expr(i)->same_as(*c_expr->get_then_param_expr(i), check_context)) {
          bool_ret = false;
        }
      }
    }
  }
  return bool_ret;
}

int ObCaseOpRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

/***
 * replace by common implement which use get_param_count() && get_param_expr()
 *
int ObCaseOpRawExpr::preorder_accept(ObRawExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  int64_t when_count = when_exprs_.count();
  int64_t then_count = then_exprs_.count();
  if (OB_UNLIKELY(when_count != then_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of when_exprs_ is not equal to count of then_exprs", K(when_count),
             K(then_count), K(ret));
  } else if (OB_FAIL(visitor.visit(*this))) {
    LOG_WARN("failed to visit this", K(ret));
  } else if (!visitor.skip_child(*this)) {
    if (arg_expr_) {
      if (OB_FAIL(arg_expr_->preorder_accept(visitor))) {
        LOG_WARN("failed to accept arg_expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < when_count; ++i) {
      if (OB_ISNULL(when_exprs_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("when_expr is NULL", K(i), K(ret));
      } else if (OB_ISNULL(then_exprs_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then_expr is NULL", K(i), K(ret));
      } else if (OB_FAIL(when_exprs_[i]->preorder_accept(visitor))) {
        LOG_WARN("failed to accept when", K(i), K(ret));
      } else if (OB_FAIL(then_exprs_[i]->preorder_accept(visitor))) {
        LOG_WARN("failed to accept then", K(i), K(ret));
      } else {}
    } // end for

    if (OB_SUCCESS == ret && NULL != default_expr_) {
      if (OB_FAIL(default_expr_->preorder_accept(visitor))) {
        LOG_WARN("failed to accept default", K(ret));
      }
    }
  }
  return ret;
}

int ObCaseOpRawExpr::postorder_accept(ObRawExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  int64_t when_count = when_exprs_.count();
  int64_t then_count = then_exprs_.count();
  if (OB_UNLIKELY(when_count != then_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of when_exprs_ is not equal to count of then_exprs", K(when_count),
             K(then_count), K(ret));
  } else {
    if (!visitor.skip_child(*this)) {
      if (arg_expr_) {
        if (OB_FAIL(arg_expr_->postorder_accept(visitor))) {
          LOG_WARN("failed to accept arg_expr", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < when_count; ++i) {
        if (OB_ISNULL(when_exprs_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("when_expr is NULL", K(i), K(ret));
        } else if (OB_ISNULL(then_exprs_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("then_expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(when_exprs_[i]->postorder_accept(visitor))) {
          LOG_WARN("failed to accept when", K(i), K(ret));
        } else if (OB_FAIL(then_exprs_[i]->postorder_accept(visitor))) {
          LOG_WARN("failed to accept then", K(i), K(ret));
        } else {}
      } // end for
      if (OB_SUCCESS == ret && default_expr_) {
        if (OB_FAIL(default_expr_->postorder_accept(visitor))) {
          LOG_WARN("failed to accept default", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(visitor.visit(*this))) {
        LOG_WARN("failed to visit this", K(ret));
      }
    }
  }
  return ret;
}

int ObCaseOpRawExpr::postorder_replace(ObRawExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  int64_t when_count = when_exprs_.count();
  int64_t then_count = then_exprs_.count();
  if (OB_UNLIKELY(when_count != then_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of when_exprs_ is not equal to count of then_exprs", K(when_count),
             K(then_count), K(ret));
  } else {
    ObRawExprPullUpAggrExpr &pull_up_visitor = reinterpret_cast<ObRawExprPullUpAggrExpr &>(visitor);
    if (arg_expr_) {
      if (OB_FAIL(arg_expr_->postorder_replace(visitor))) {
        LOG_WARN("failed to accept arg_expr", K(ret));
      } else {
        // replace the child expr
        arg_expr_ = pull_up_visitor.get_new_expr();
      }
    }

    for (int32_t i = 0; OB_SUCC(ret) && i < when_count; ++i) {
      if (OB_ISNULL(when_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("when_expr is NULL", K(i), K(ret));
      } else if (OB_ISNULL(then_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then_expr is NULL", K(i), K(ret));
      } else if (OB_FAIL(when_exprs_.at(i)->postorder_replace(visitor))) {
        LOG_WARN("failed to accept when expr", K(i), K(ret));
      } else {
        // replace the child expr
        when_exprs_.at(i) = pull_up_visitor.get_new_expr();
        if (OB_FAIL(then_exprs_.at(i)->postorder_replace(visitor))) {
          LOG_WARN("failed to accept then expr", K(i), K(ret));
        } else {
          // replace the child expr
          then_exprs_.at(i) = pull_up_visitor.get_new_expr();
        }
      }
    } // end for

    if (OB_SUCCESS == ret && default_expr_) {
      if (OB_FAIL(default_expr_->postorder_replace(visitor))) {
        LOG_WARN("failed to accept default", K(ret));
      } else {
        // replace the child expr
        default_expr_ = pull_up_visitor.get_new_expr();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(visitor.visit(*this))) {
        LOG_WARN("failed to visit this", K(ret));
      }
    }
  }
  return ret;
}

*/

int ObCaseOpRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                       ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("CASE"))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (NULL != arg_expr_) {
    if (OB_FAIL(BUF_PRINTF(" "))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(arg_expr_->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("fail to get_name", K(ret));
    } else {}
  } else {}

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_when_expr_size(); ++i) {
      if (OB_ISNULL(get_when_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("when_param_expr is NULL", K(i), K(ret));
      } else if (OB_ISNULL(get_then_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("then_param_expr is NULL", K(i), K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
      } else if (OB_FAIL(BUF_PRINTF("WHEN"))) {
        LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(i), K(ret));
      } else if (OB_FAIL(get_when_param_expr(i)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name", K(i), K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("THEN"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(get_then_param_expr(i)->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_name", K(i), K(ret));
      } else {}
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(" "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("ELSE"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else {
        if (NULL != default_expr_) {
          if (OB_FAIL(BUF_PRINTF(" "))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          } else if (OB_FAIL(default_expr_->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("fail to get_name", K(ret));
          } else {}
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(" "))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("END"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  if (OB_SUCCESS == ret && EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObAggFunRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObRawExpr failed", K(ret));
    } else {
      const ObAggFunRawExpr &tmp =
          static_cast<const ObAggFunRawExpr &>(other);
      if (OB_FAIL(real_param_exprs_.assign(tmp.real_param_exprs_))) {
        LOG_WARN("faile to assign real param expr", K(ret));
      } else if (OB_FAIL(order_items_.assign(tmp.order_items_))) {
        LOG_WARN("failed to assign order items", K(ret));
      } else {
        distinct_ = tmp.distinct_;
        separator_param_expr_ = tmp.separator_param_expr_;
        expr_in_inner_stmt_ = tmp.expr_in_inner_stmt_;
        pl_agg_udf_expr_ = tmp.pl_agg_udf_expr_;
        udf_meta_.assign(tmp.udf_meta_);
        is_need_deserialize_row_ = tmp.is_need_deserialize_row_;
      }
    }
  }
  return ret;
}

int ObAggFunRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attribute", K(ret));
  } else if (OB_FAIL(copier.copy(pl_agg_udf_expr_))) {
    LOG_WARN("failed to copy pl agg udf expr", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner alloc is null", K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf_meta_.name_, udf_meta_.name_))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf_meta_.dl_, udf_meta_.dl_))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObAggFunRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                  const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     real_param_exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr_for_order_item(other_exprs,
                                                                   new_exprs,
                                                                   order_items_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    pl_agg_udf_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

void ObAggFunRawExpr::clear_child()
{
  real_param_exprs_.reset();
}

void ObAggFunRawExpr::reset()
{
  ObRawExpr::reset();
  distinct_ = false;
  clear_child();
  order_items_.reset();
  separator_param_expr_ = NULL;
  expr_in_inner_stmt_ = false;
  is_need_deserialize_row_ = false;
  pl_agg_udf_expr_ = NULL;
}

bool ObAggFunRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() != expr.get_expr_type()) {
  } else  {
    const ObAggFunRawExpr *a_expr = static_cast<const ObAggFunRawExpr *>(&expr);
    if (expr_in_inner_stmt_ != a_expr->expr_in_inner_stmt_) {
      //do nothing.
    } else if (distinct_ == a_expr->is_param_distinct()) {
      if ((NULL == separator_param_expr_ && NULL == a_expr->separator_param_expr_)
          || (NULL != separator_param_expr_ && NULL != a_expr->separator_param_expr_
              && separator_param_expr_->same_as(*(a_expr->separator_param_expr_), check_context))) {
        if (real_param_exprs_.count() == a_expr->real_param_exprs_.count()) {
          bool_ret = true;
          for (int64_t i = 0; bool_ret && i < real_param_exprs_.count(); ++i) {
            if (OB_ISNULL(real_param_exprs_.at(i)) || OB_ISNULL(a_expr->real_param_exprs_.at(i)) ||
                !real_param_exprs_.at(i)->same_as(*(a_expr->real_param_exprs_.at(i)), check_context)) {
              bool_ret = false;
            }
          }

          if (bool_ret) {
            if (order_items_.count() == a_expr->order_items_.count()) {
              for (int64_t i = 0; bool_ret && i < order_items_.count(); ++i) {
                if(OB_ISNULL(order_items_.at(i).expr_) || OB_ISNULL(a_expr->order_items_.at(i).expr_)
                   || order_items_.at(i).order_type_ != a_expr->order_items_.at(i).order_type_
                   || !order_items_.at(i).expr_->same_as(*(a_expr->order_items_.at(i).expr_), check_context)) {
                  bool_ret = false;
                }
              }
            } else {
              bool_ret = false;
            }
          }
          if (bool_ret) {
            bool_ret = is_need_deserialize_row_ == a_expr->is_need_deserialize_row();
          }
          if (bool_ret) {
            if ((NULL == pl_agg_udf_expr_ && NULL == a_expr->pl_agg_udf_expr_) ||
                (NULL != pl_agg_udf_expr_ && NULL != a_expr->pl_agg_udf_expr_ &&
                 pl_agg_udf_expr_->same_as(*(a_expr->pl_agg_udf_expr_), check_context))) {
              bool_ret = true;
            } else {
              bool_ret = false;
            }
          }
        }
      }
    }
  }
  return bool_ret;
}


int64_t ObAggFunRawExpr::get_param_count() const
{
  int64_t param_count = real_param_exprs_.count() + order_items_.count();
  if (NULL != separator_param_expr_) {
    param_count++;
  }
  return param_count;
}

const ObRawExpr *ObAggFunRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr *ptr_ret = NULL;
  if (0 <= index && index < real_param_exprs_.count()) {
    ptr_ret = real_param_exprs_.at(index);
  } else if ((0 <= index - real_param_exprs_.count())
             && (index - real_param_exprs_.count() < order_items_.count())) {
    ptr_ret = order_items_.at(index - real_param_exprs_.count()).expr_;
  } else if (index == real_param_exprs_.count() + order_items_.count()) {
    if (NULL != separator_param_expr_) {
      ptr_ret = separator_param_expr_;
    } else { /*do nothing*/ }
  }
  return ptr_ret;
}

ObRawExpr *&ObAggFunRawExpr::get_param_expr(int64_t index)
{
  if (0 <= index && index < real_param_exprs_.count()) {
    return real_param_exprs_.at(index);
  } else if ((0 <= index - real_param_exprs_.count())
             && (index - real_param_exprs_.count() < order_items_.count())) {
    return order_items_.at(index - real_param_exprs_.count()).expr_;
  } else if (index == real_param_exprs_.count() + order_items_.count()) {
    if (NULL != separator_param_expr_) {
      return separator_param_expr_;
    } else {
      return USELESS_POINTER;
    }
  } else {
    return USELESS_POINTER;
  }
}

int ObAggFunRawExpr::get_param_exprs(common::ObIArray<ObRawExpr*> &param_exprs)
{
  int ret = OB_SUCCESS;
  int64_t param_count = get_param_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    if (OB_FAIL(param_exprs.push_back(get_param_expr(i)))) {
      LOG_WARN("fail to push back param expr", K(ret));
    }
  }
  return ret;
}


int ObAggFunRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObAggFunRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                       ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (T_FUN_AGG_UDF == get_expr_type()) {
    if (OB_FAIL(BUF_PRINTF("%.*s(", udf_meta_.name_.length(), udf_meta_.name_.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (T_FUN_PL_AGG_UDF == get_expr_type()) {
    if (OB_ISNULL(pl_agg_udf_expr_) || OB_UNLIKELY(!pl_agg_udf_expr_->is_udf_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(pl_agg_udf_expr_));
    } else if (OB_FAIL(BUF_PRINTF("%.*s(",
                            static_cast<ObUDFRawExpr*>(pl_agg_udf_expr_)->get_func_name().length(),
                            static_cast<ObUDFRawExpr*>(pl_agg_udf_expr_)->get_func_name().ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {/*do nothing*/}
  } else if (EXPLAIN_DBLINK_STMT == type) {
    if (OB_FAIL(BUF_PRINTF("%s(", get_name_dblink(get_expr_type())))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else {
    if (OB_FAIL(BUF_PRINTF("%s(", get_type_name(get_expr_type())))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (distinct_) {
      if (OB_FAIL(BUF_PRINTF("distinct "))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
    int64_t i = 0;
    if (T_FUN_MEDIAN == get_expr_type() ||
        T_FUN_GROUP_PERCENTILE_CONT == get_expr_type()) {
      if (1 > get_real_param_count()) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("invalid number of arguments", K(ret), K(get_expr_type()));
      } else if (OB_FAIL(get_real_param_exprs().at(0)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
      } else {}
    } else {
      for (; OB_SUCC(ret) && i < get_real_param_count() - 1; ++i) {
        if (OB_ISNULL(get_real_param_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_real_param_exprs().at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      }
      if (OB_SUCC(ret)) {
        if (0 == get_real_param_count()) {
          if (OB_FAIL(BUF_PRINTF("*"))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          }
        } else if (OB_ISNULL(get_real_param_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is NULL", K(i), K(ret));
        } else if (OB_FAIL(get_real_param_exprs().at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      }
    }

    if (OB_SUCCESS == ret && OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
    if (OB_SUCCESS == ret &&
        (T_FUN_GROUP_CONCAT == get_expr_type() ||
         T_FUN_GROUP_RANK == get_expr_type() ||
         T_FUN_GROUP_DENSE_RANK == get_expr_type() ||
         T_FUN_GROUP_PERCENT_RANK == get_expr_type() ||
         T_FUN_GROUP_CUME_DIST == get_expr_type() ||
         T_FUN_GROUP_PERCENTILE_CONT == get_expr_type() ||
         T_FUN_GROUP_PERCENTILE_DISC == get_expr_type() ||
         T_FUN_KEEP_MAX == get_expr_type() ||
         T_FUN_KEEP_MIN == get_expr_type() ||
         T_FUN_KEEP_SUM == get_expr_type() ||
         T_FUN_KEEP_COUNT == get_expr_type() ||
         T_FUN_KEEP_WM_CONCAT == get_expr_type())) {
      if (order_items_.count() > 0) {
        if (OB_FAIL(BUF_PRINTF(" order_items("))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
          if (i > 0) {
            if (OB_FAIL(BUF_PRINTF(", "))) {
              LOG_WARN("fail to BUF_PRINTF", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
            //do nothing
          } else if (OB_ISNULL(order_items_.at(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL", K(i), K(ret));
          } else if (OB_FAIL(order_items_.at(i).expr_->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("fail to get_name", K(i), K(ret));
          } else {}
        }
        if (OB_SUCCESS == ret && OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      }
      if (OB_SUCCESS == ret && NULL != separator_param_expr_) {
        if (OB_FAIL(BUF_PRINTF(" separator_param_expr("))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(separator_param_expr_->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      }
    }
    if (OB_SUCCESS == ret && EXPLAIN_EXTENDED == type) {
      if (OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else {}
    }
  }
  return ret;
}

const char *ObAggFunRawExpr::get_name_dblink(ObItemType expr_type) const
{
  const char *name = NULL;
  switch(expr_type){
  case T_FUN_MAX :
    name = "max";
    break;
  case T_FUN_MIN :
    name = "min";
    break;
  case T_FUN_SUM :
    name = "sum";
    break;
  case T_FUN_COUNT :
    name = "count";
    break;
  case T_FUN_AVG :
    name = "avg";
    break;
  default:
    name = "UNKNOWN";
    break;
  }
  return name;
}

int ObAggFunRawExpr::set_udf_meta(const share::schema::ObUDF &udf)
{
  int ret = OB_SUCCESS;
  udf_meta_.tenant_id_ = udf.get_tenant_id();
  udf_meta_.ret_ = udf.get_ret();
  udf_meta_.type_ = udf.get_type();

  if (OB_ISNULL(inner_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf.get_name_str(), udf_meta_.name_))) {
    LOG_WARN("fail to write string", K(udf.get_name_str()), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf.get_dl_str(), udf_meta_.dl_))) {
    LOG_WARN("fail to write string", K(udf.get_name_str()), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObSysFunRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObOpRawExpr failed", K(ret));
    } else {
      const ObSysFunRawExpr &tmp =
          static_cast<const ObSysFunRawExpr &>(other);
      func_name_ = tmp.func_name_;
      dblink_name_ = tmp.dblink_name_;
      operator_id_ = tmp.operator_id_;
      dblink_id_ = tmp.dblink_id_;
    }
  }
  return ret;
}

int ObSysFunRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, func_name_, func_name_))) {
      LOG_WARN("fail to write string", K(func_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, dblink_name_, dblink_name_))) {
      LOG_WARN("fail to write string", K(dblink_name_), K(ret));
    }
  }
  return ret;
}

void ObSysFunRawExpr::clear_child()
{
  exprs_.reset();
}

void ObSysFunRawExpr::reset()
{
  ObNonTerminalRawExpr::reset();
  func_name_.reset();
  clear_child();
  dblink_id_ = OB_INVALID_ID;
}

bool ObSysFunRawExpr::inner_json_expr_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  const ObRawExpr *l_expr = this;
  const ObRawExpr *r_expr = &expr;

  if (r_expr->is_domain_json_expr()) {
    l_expr->extract_multivalue_json_expr(l_expr);
  }

  if (l_expr->get_expr_type() == T_FUN_SYS_JSON_QUERY
      && r_expr->is_domain_json_expr()) {
    const ObRawExpr *r_param_expr = nullptr;
    const ObRawExpr *l_param_expr = l_expr->get_param_expr(1);

    const ObRawExpr *r_column_expr = nullptr;
    const ObRawExpr *l_column_expr = l_expr->get_param_expr(0);

    if (r_expr->get_expr_type() == T_FUN_SYS_JSON_MEMBER_OF) {
      r_param_expr = r_expr->get_param_expr(1);
    } else {
      r_param_expr = r_expr->get_param_expr(0);
      if (r_expr->get_expr_type() == T_FUN_SYS_JSON_OVERLAPS && OB_NOT_NULL(r_param_expr) && r_param_expr->is_const_expr()) {
        r_param_expr = r_expr->get_param_expr(1);
      }
    }
    if (OB_ISNULL(r_param_expr)) {
    } else if (r_param_expr->get_expr_type() == T_REF_COLUMN) {
      r_column_expr = r_param_expr;
      r_param_expr = nullptr;
      if (l_param_expr->is_const_expr()) {
        ObString path_str = (static_cast<const ObConstRawExpr*>(l_param_expr))->get_value().get_string();
        bool_ret = path_str.empty();
      }
    } else if (r_param_expr->is_wrappered_json_extract()) {
      r_column_expr = r_param_expr->get_param_expr(0)->get_param_expr(0);
      r_param_expr = r_param_expr->get_param_expr(0)->get_param_expr(1);
    } else if (r_param_expr->get_expr_type() == T_FUN_SYS_JSON_VALUE) {
      r_column_expr = r_param_expr->get_param_expr(0);
      r_param_expr = r_param_expr->get_param_expr(1);
    } else if (r_param_expr->get_expr_type() == T_FUN_SYS_JSON_EXTRACT) {
      r_column_expr = r_param_expr->get_param_expr(0);
      r_param_expr = r_param_expr->get_param_expr(1);
    }

    if (OB_NOT_NULL(r_param_expr)) {
      bool_ret = l_param_expr->same_as(*r_param_expr, check_context);
    }

    if (bool_ret) {
      bool_ret = r_column_expr == l_column_expr;
    }
  } else if (l_expr->get_expr_type() == r_expr->get_expr_type()) {
    bool_ret = l_expr->same_as(*r_expr, check_context);
  }

  return bool_ret;
}

bool ObSysFunRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() != expr.get_expr_type()) {
    if (expr.get_expr_type() ==  T_OP_BOOL && expr.is_domain_json_expr() &&
        IS_QUERY_JSON_EXPR(get_expr_type())) {
      const ObRawExpr* right_expr = ObRawExprUtils::skip_inner_added_expr(&expr);
      bool_ret = inner_json_expr_same_as(*right_expr, check_context);
    } else if (IS_QUERY_JSON_EXPR(expr.get_expr_type()) || IS_QUERY_JSON_EXPR(get_expr_type())) {
      bool_ret = inner_json_expr_same_as(expr, check_context);
    } else if (check_context != NULL && check_context->ora_numeric_compare_ && expr.is_const_raw_expr()
        && T_FUN_SYS_CAST == get_expr_type() && lib::is_oracle_mode()) {
      bool_ret = check_context->compare_ora_numeric_consts(*this, static_cast<const ObConstRawExpr &>(expr));
    }
  } else if (T_FUN_SYS_RAND == get_expr_type() ||
             T_FUN_SYS_RANDOM == get_expr_type() ||
             T_FUN_SYS_GUID == get_expr_type() ||
             T_OP_GET_USER_VAR == get_expr_type() ||
             T_OP_GET_SYS_VAR == get_expr_type() ||
             (has_flag(IS_STATE_FUNC) && (NULL == check_context ||
                          (NULL != check_context && check_context->need_check_deterministic_)))) {
  } else if (T_FUN_SYS_LAST_REFRESH_SCN == get_expr_type()) {
    bool_ret = get_mview_id() == static_cast<const ObSysFunRawExpr&>(expr).get_mview_id();
  } else if (get_expr_class() == expr.get_expr_class()) {
    //for EXPR_UDF and EXPR_SYS_FUNC
    const ObSysFunRawExpr *s_expr = static_cast<const ObSysFunRawExpr *>(&expr);
    if (ObCharset::case_insensitive_equal(func_name_, s_expr->get_func_name())
        && this->get_param_count() == s_expr->get_param_count() &&
        get_dblink_id() == s_expr->get_dblink_id()) {
      bool_ret = true;
      for (int64_t i = 0; bool_ret && i < s_expr->get_param_count(); i++) {
        if (OB_ISNULL(get_param_expr(i)) || OB_ISNULL(s_expr->get_param_expr(i))) {
          bool_ret = false;
        } else {
          // get package has 3 params
          // 0 : package_id
          // 1 : var_idx
          // 2 : the point of result_type. result_type need compare with type ObExprResType
          if (T_OP_GET_PACKAGE_VAR == get_expr_type() && 2 == i) {
            if (T_INT == get_param_expr(i)->get_expr_type()
                && T_INT == s_expr->get_param_expr(i)->get_expr_type()) {
              const ObConstRawExpr* val1 = static_cast<const ObConstRawExpr*>(get_param_expr(i));
              const ObConstRawExpr* val2 =
                static_cast<const ObConstRawExpr*>(s_expr->get_param_expr(i));
              ObExprResType* res_type1 =
                reinterpret_cast<ObExprResType*>(val1->get_value().get_int());
              ObExprResType* res_type2 =
                reinterpret_cast<ObExprResType*>(val2->get_value().get_int());
              if (NULL == res_type1 || NULL == res_type2 || *res_type1 != *res_type2) {
                bool_ret = false;
              }
            } else {
              bool_ret = false;
            }
          } else if (!this->get_param_expr(i)->same_as(*s_expr->get_param_expr(i),
                      check_context)) {
            bool_ret = false;
          } else {}
        }
      }
      if (0 == get_param_count()
          && (T_FUN_SYS_CUR_TIMESTAMP == get_expr_type()
              || T_FUN_SYS_SYSDATE == get_expr_type()
              || T_FUN_SYS_CUR_TIME == get_expr_type()
              || T_FUN_SYS_UTC_TIMESTAMP == get_expr_type()
              || T_FUN_SYS_UTC_TIME == get_expr_type())) {
        bool_ret = result_type_.get_scale() == s_expr->get_result_type().get_scale();
      }
      if ((T_FUN_SYS_CAST == get_expr_type() ||
           T_FUN_SYS == get_expr_type() ||
           T_FUN_SYS_CALC_TABLET_ID == get_expr_type() ||
           T_FUN_SYS_CALC_PARTITION_ID == get_expr_type() ||
           T_FUN_SYS_CALC_PARTITION_TABLET_ID == get_expr_type()) &&
          get_extra() != expr.get_extra()) { // for calc_partition_id
        bool_ret = false;
      }

      // for json partial update
      // update json_test set j1 = json_replace(j2, '$[0]', 'xyz'), j2 = json_replace(j2, '$[0]', 'xyz') where pk=1;
      // first and second value expr is same, but behavior is different
      // so do not share expr
      if ((T_FUN_SYS_JSON_REPLACE == get_expr_type() ||
           T_FUN_SYS_JSON_SET == get_expr_type() ||
           T_FUN_SYS_JSON_REMOVE == get_expr_type())) {
        bool_ret = false;
      }
    }
  } else if (expr.is_op_expr() && T_OP_CNN == expr.get_expr_type()) {
    //for cases which compares concat('xxx','xxx') with 'xxx'||'xxx'
    const ObOpRawExpr *m_expr = static_cast<const ObOpRawExpr *>(&expr);
    if (this->get_param_count() == m_expr->get_param_count()) {
      bool_ret = true;
      for (int64_t i = 0; bool_ret && i < m_expr->get_param_count(); ++i) {
        if (NULL == this->get_param_expr(i) || NULL == m_expr->get_param_expr(i)
            || !(this->get_param_expr(i)->same_as(*m_expr->get_param_expr(i), check_context))) {
          bool_ret = false;
        }
      }
    }
  }
  return bool_ret;
}

ObExprOperator *ObSysFunRawExpr::get_op()
{
  ObExprOperator *op = NULL;
  if (get_expr_type() == T_FUN_SYS) {
    free_op();
    ObExprOperatorType type;
    if (T_INVALID != (type = ObExprOperatorFactory::get_type_by_name(func_name_))) {
      set_expr_type(type);
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid func name", K_(func_name));
    }
  }
  if (OB_UNLIKELY(NULL == (op = ObOpRawExpr::get_op()))) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "make function failed", K_(func_name));
  }
  return op;
}

int ObSysFunRawExpr::check_param_num_internal(int32_t param_num, int32_t param_count, ObExprOperatorType type)
{
  int ret = OB_SUCCESS;
  switch (param_num) {
    case ObExprOperator::MORE_THAN_ZERO: {
      if (param_count <= 0) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function can not be 0", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::MORE_THAN_ONE: {
      if (param_count <= 1) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be more than 1", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::MORE_THAN_TWO: {
      if (param_count <= 2) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be more than 2", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::ZERO_OR_ONE: {
      if (0 != param_count && 1 != param_count) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be 0 or 1", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::ONE_OR_TWO: {
      if (param_count != 1 && param_count != 2) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be 1 or 2", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::TWO_OR_THREE: {
      if (param_count != 2 && param_count != 3) {
        if (lib::is_oracle_mode() && (T_FUN_SYS_RPAD == type || T_FUN_SYS_LPAD == type)) {
          if (param_count > 3) {
            ret = OB_ERR_TOO_MANY_ARGS_FOR_FUN;
            LOG_WARN("param count larger than 3", K(ret), K(func_name_), K(param_count));
          } else {
            ret = OB_ERR_NOT_ENOUGH_ARGS_FOR_FUN;
            LOG_WARN("param count less than 2", K(ret), K(func_name_), K(param_count));
          }
        } else {
          ret = OB_ERR_PARAM_SIZE;
          LOG_WARN("Param num of function should be 2 or 3", K(func_name_), K(ret), K(param_count));
        }
      } else if (!lib::is_oracle_mode() && T_FUN_SYS_REPLACE == type && param_count != 3) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be 3", K(func_name_), K(ret), K(lib::is_oracle_mode()));
      }
      break;
    }
    case ObExprOperator::OCCUR_AS_PAIR: {
      if (param_count % 2 != 0) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("Param num of function should be even", K(func_name_), K(ret));
      }
      break;
    }
    case ObExprOperator::PARAM_NUM_UNKNOWN: {
      // nothing
      break;
    }
    default: {
      if (param_count != param_num) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("invalid Param num of function", K(func_name_), K(param_num),
                  K(param_count), K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObSysFunRawExpr::check_param_num()
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = NULL;
  ObExprOperatorType type;
  if (OB_UNLIKELY(T_INVALID == (type = ObExprOperatorFactory::get_type_by_name(func_name_)))) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    // 不向USER报错，外部会根据这个错误码继续尝试解析为UDF
    LOG_WARN("system function not exists, maybe a user define function", K(func_name_), K(ret));
    // LOG_USER_ERROR(ret, "FUNCTION", to_cstring(func_name_)); //throw to user
  } else if (OB_UNLIKELY(NULL == (op = get_op()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to make function", K(func_name_), K(ret));
  } else {
    int32_t param_num = op->get_param_num();
    int32_t param_count = get_param_count();
    ret = check_param_num_internal(param_num, param_count, type);
    if (OB_UNLIKELY(OB_ERR_PARAM_SIZE == ret)) {
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
    }
  }
  return ret;
}

int ObSysFunRawExpr::check_param_num(int32_t param_count)
{
  int ret = OB_SUCCESS;
  ObExprOperator *op = NULL;
  ObExprOperatorType type;
  if (OB_UNLIKELY(T_INVALID == (type = ObExprOperatorFactory::get_type_by_name(func_name_)))) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    // 不向USER报错，外部会根据这个错误码继续尝试解析为UDF
    LOG_WARN("system function not exists, maybe a user define function", K(func_name_), K(ret));
    // LOG_USER_ERROR(ret, "FUNCTION", to_cstring(func_name_)); //throw to user
  } else if (OB_UNLIKELY(NULL == (op = get_op()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to make function", K(func_name_), K(ret));
  } else {
    int32_t param_num = op->get_param_num();
    ret = check_param_num_internal(param_num, param_count, type);
    if (OB_UNLIKELY(OB_ERR_PARAM_SIZE == ret)) {
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
    }
  }
  return ret;
}

int ObSysFunRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObSysFunRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                       ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (has_flag(IS_INNER_ADDED_EXPR)
      && (EXPLAIN_EXTENDED != type && EXPLAIN_EXTENDED_NOADDR != type)
      && (T_FUN_SYS_REMOVE_CONST == get_expr_type() || T_FUN_SYS_WRAPPER_INNER == get_expr_type())) {
    CK(1 == get_param_count());
    OZ(get_param_expr(0)->get_name(buf, buf_len, pos, type));
  } else {
    if (T_FUN_SYS_AUTOINC_NEXTVAL == get_expr_type() &&
        OB_FAIL(get_autoinc_nextval_name(buf, buf_len, pos))) {
      LOG_WARN("fail to get_autoinc_nextval_name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%.*s", get_func_name().length(), get_func_name().ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      if (is_valid_id(get_dblink_id())) {
        if (get_dblink_name().empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dblink name is empty", K(ret), KPC(this));
        } else if (OB_FAIL(BUF_PRINTF("@%.*s", get_dblink_name().length(), get_dblink_name().ptr()))) {
          LOG_WARN("failed to print dblink name", K(ret), K(get_dblink_name()));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF("("))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (T_FUN_COLUMN_CONV == get_expr_type()) {
      if (OB_FAIL(get_column_conv_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to get_column_conv_name", K(ret));
      }
    } else if (T_FUN_SYS_PART_ID == get_expr_type()) {
      //ignore the print of T_FUN_SYS_PART_ID expr
    } else if (T_FUN_SYS_INNER_ROW_CMP_VALUE == get_expr_type()) {
      CK(3 == get_param_count());
      OZ(get_param_expr(2)->get_name(buf, buf_len, pos, type));
    } else if (T_FUN_SYS_LAST_REFRESH_SCN == get_expr_type()) {
      if (OB_FAIL(BUF_PRINTF("%ld", get_mview_id()))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else {
      int64_t i = 0;
      if (get_param_count() > 1) {
        for (; OB_SUCC(ret) && i < get_param_count() - 1; ++i) {
          if (OB_ISNULL(get_param_expr(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param_expr is NULL", K(i), K(ret));
          } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("fail to get_name", K(i), K(ret));
          } else if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          } else {}
        }
      }
      if (OB_SUCC(ret)) {
        if (T_FUN_SYS_CAST != get_expr_type()) {
          if (get_param_count() >= 1) {
            if (OB_ISNULL(get_param_expr(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param expr is NULL", K(i), K(ret));
            } else if (OB_FAIL(get_param_expr(i)->get_name(buf, buf_len, pos, type))) {
              LOG_WARN("fail to get_name", K(ret));
            } else {}
          }
        } else {
          if (OB_FAIL(get_cast_type_name(buf, buf_len, pos))) {
            LOG_WARN("fail to get_cast_type_name", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(BUF_PRINTF(")"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      } else if (EXPLAIN_EXTENDED == type) {
        if (OB_FAIL(BUF_PRINTF("("))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      } else {}
    }
  }
  return ret;
}

int ObSysFunRawExpr::get_cast_type_name(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("second param expr is NULL", K(ret));
  } else if (!get_param_expr(1)->is_const_raw_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("second param expr is not const expr", K(ret));
  } else {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(get_param_expr(1));
    ObObj value = const_expr->get_value();
    if (!value.is_int()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value of second param expr is not int", K(value), K(ret));
    } else {
      ParseNode node;
      node.value_ = value.get_int();
      ObObjType dest_type = static_cast<ObObjType>(node.int16_values_[0]);
      ObCollationType coll_type = static_cast<ObCollationType>(node.int16_values_[1]);
      int32_t length = 0;
      int16_t precision = 0;
      int16_t scale = 0;
      const char *type_str = inner_obj_type_str(dest_type);
      if (ob_is_string_tc(dest_type)) {
        length = node.int32_values_[1] < 0 ?
            static_cast<int32_t>(OB_MAX_VARCHAR_LENGTH) : node.int32_values_[1];
        if (lib::is_oracle_mode()) {
          ObLengthSemantics length_semantics = const_expr->get_accuracy().get_length_semantics();
          const char* length_semantics_str = ob_is_nstring(dest_type) ?
                            "" : get_length_semantics_str(length_semantics);
          if (OB_FAIL(BUF_PRINTF("%s(%d %s)", type_str, length, length_semantics_str))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          }
        } else {
          if (OB_FAIL(BUF_PRINTF("%s(%d)", type_str, length))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          }
        }
      } else if(ob_is_text_tc(dest_type) || ob_is_json_tc(dest_type) || ob_is_geometry_tc(dest_type)) {
        // TODO@hanhui texttc should use default length
        length = ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length();
        if (OB_FAIL(BUF_PRINTF("%s(%d)", type_str, length))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      } else if (ob_is_lob_tc(dest_type)) {
        const char *dest_type_str = CS_TYPE_BINARY == coll_type ? "blob" : "clob";
        if (OB_FAIL(BUF_PRINTF(dest_type_str, type_str, length))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      } else {
        precision = node.int16_values_[2];
        scale = node.int16_values_[3];
        if (OB_FAIL(BUF_PRINTF("%s(%d, %d)", type_str, precision, scale))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSysFunRawExpr::get_column_conv_name(char *buf, int64_t buf_len, int64_t &pos,
                                          ExplainType explain_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || T_FUN_COLUMN_CONV != get_expr_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(get_expr_type()), K(ret));
  } else if (OB_ISNULL(get_param_expr(0)) || OB_ISNULL(get_param_expr(1))
             || OB_ISNULL(get_param_expr(2)) || OB_ISNULL(get_param_expr(3))
             || OB_ISNULL(get_param_expr(4))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parm expr is NULL", K(get_param_expr(0)), K(get_param_expr(1)),
             K(get_param_expr(2)), K(get_param_expr(3)), K(get_param_expr(4)), K(ret));
  } else {
    const ObConstRawExpr *type_expr = static_cast<const ObConstRawExpr*>(get_param_expr(0));
    ObObjType type = static_cast<ObObjType>(type_expr->get_value().get_int());
    const ObConstRawExpr *collation_expr = static_cast<const ObConstRawExpr*>(get_param_expr(1));
    common::ObCollationType cs_type = static_cast<common::ObCollationType>(collation_expr->get_value().get_int());
    const ObConstRawExpr *accuracy_expr = static_cast<const ObConstRawExpr*>(get_param_expr(2));
    int64_t accuray_value = accuracy_expr->get_value().get_int();
    ObAccuracy accuracy;
    accuracy.set_accuracy(accuray_value);
    const ObConstRawExpr *bool_expr = static_cast<const ObConstRawExpr*>(get_param_expr(3));
    bool is_nullable = bool_expr->get_value().get_bool();
    const char *type_str = inner_obj_type_str(type);
    if (ob_is_string_type(type)) {
      if (OB_FAIL(BUF_PRINTF("%s,%s,length:%d,%s,", type_str, ObCharset::collation_name(cs_type),
                             accuracy.get_length(), is_nullable ? "NULL" : "NOT NULL"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    } else {
      if (OB_FAIL(BUF_PRINTF("%s,PS:(%d,%d),%s,", type_str, accuracy.get_precision(),
                             accuracy.get_scale(), is_nullable ? "NULL" : "NOT NULL"))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(get_param_expr(4)->get_name(buf, buf_len, pos, explain_type))) {
      LOG_WARN("fail to get_name", K(ret));
    }
  }
  return ret;
}

int ObSysFunRawExpr::get_autoinc_nextval_name(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObAutoincNextvalExtra *autoinc_table_extra = NULL;
  if (OB_ISNULL(buf) || T_FUN_SYS_AUTOINC_NEXTVAL != get_expr_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(get_expr_type()), K(ret));
  } else if (OB_ISNULL((autoinc_table_extra =
          reinterpret_cast<ObAutoincNextvalExtra *>(extra_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_ is null", K(ret));
  } else {
    ObString autoinc_table_name = autoinc_table_extra->autoinc_table_name_;
    ObString autoinc_column_name = autoinc_table_extra->autoinc_column_name_;
    ObString autoinc_qualified_name =
            concat_qualified_name("", autoinc_table_name, autoinc_column_name);
    if (OB_FAIL(BUF_PRINTF("%s.", autoinc_qualified_name.ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  }
  return ret;
}

int ObSysFunRawExpr::set_local_session_vars(const share::schema::ObLocalSessionVar *local_var_info,
                                            const ObBasicSessionInfo *session,
                                            int64_t ctx_array_idx) {
  int ret = OB_SUCCESS;
  ObExprOperator * op = get_op();
  local_session_var_id_ = ctx_array_idx;
  local_session_var_.reset();
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(op));
  } else if (OB_FAIL(op->set_local_session_vars(this, local_var_info, session, local_session_var_))) {
    LOG_WARN("fail to set local session info for expr operators", K(ret));
  }
  return ret;
}

int ObSequenceRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign sys raw expr");
    } else {
      const ObSequenceRawExpr &tmp =
          static_cast<const ObSequenceRawExpr &>(other);
      database_name_ = tmp.database_name_;
      name_ = tmp.name_;
      action_ = tmp.action_;
      sequence_id_ = tmp.sequence_id_;
    }
  }
  return ret;
}

int ObSequenceRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, database_name_, database_name_))) {
      LOG_WARN("fail to write string", K(database_name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, name_, name_))) {
      LOG_WARN("fail to write string", K(name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, action_, action_))) {
      LOG_WARN("fail to write string", K(action_), K(ret));
    }
  }
  return ret;
}

int ObSequenceRawExpr::set_sequence_meta(const common::ObString &database_name,
                                         const common::ObString &name,
                                         const common::ObString &action,
                                         uint64_t sequence_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(*inner_alloc_, database_name, database_name_))) {
    LOG_WARN("fail to write string", K(name), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, name, name_))) {
    LOG_WARN("fail to write string", K(name), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, action, action_))) {
    LOG_WARN("fail to write string", K(action), K(ret));
  } else {
    sequence_id_ = sequence_id;
  }
  return ret;
}

bool ObSequenceRawExpr::inner_same_as(const ObRawExpr &expr,
                                      ObExprEqualCheckContext *check_context) const
{
  UNUSED(expr);
  UNUSED(check_context);
  return false;
}

int ObSequenceRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                         ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("%.*s.%.*s",
                          name_.length(), name_.ptr(),
                          action_.length(), action_.ptr()))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (is_dblink_sys_func()) {
    if (OB_FAIL(BUF_PRINTF("@%.*s",
                          get_dblink_name().length(),
                          get_dblink_name().ptr()))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  }
  if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

int ObNormalDllUdfRawExpr::set_udf_meta(const share::schema::ObUDF &udf)
{
  int ret = OB_SUCCESS;
  udf_meta_.tenant_id_ = udf.get_tenant_id();
  udf_meta_.ret_ = udf.get_ret();
  udf_meta_.type_ = udf.get_type();

  /* data from schema, deep copy maybe a better choices */
  if (OB_ISNULL(inner_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf.get_name_str(), udf_meta_.name_))) {
    LOG_WARN("fail to write string", K(udf.get_name_str()), K(ret));
  } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf.get_dl_str(), udf_meta_.dl_))) {
    LOG_WARN("fail to write string", K(udf.get_name_str()), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObNormalDllUdfRawExpr::add_udf_attribute_name(const common::ObString &name)
{
  return udf_attributes_.push_back(name);
}

int ObNormalDllUdfRawExpr::add_udf_attribute(const ObRawExpr* expr, const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the expr is null", K(ret), K(expr), K(node));
  } else if (expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
    const ObString &column_name = col_expr->get_column_name();
    if (OB_FAIL(add_udf_attribute_name(column_name))) {
      LOG_WARN("failed to udf's attribute name", K(ret));
    }
  } else {
    ObString name(node->str_len_, node->str_value_);
    if (OB_FAIL(add_udf_attribute_name(name))) {
      LOG_WARN("failed to udf's attribute name", K(ret));
    }
  }
  return ret;
}

bool ObNormalDllUdfRawExpr::inner_same_as(const ObRawExpr &expr,
                                          ObExprEqualCheckContext *check_context) const
{
  //FIXME muhang 不太清楚这个same as的作用，先全部置为false
  UNUSED(expr);
  UNUSED(check_context);
  return false;
}

int ObNormalDllUdfRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign sys raw expr");
    } else {
      const ObNormalDllUdfRawExpr &tmp =
          static_cast<const ObNormalDllUdfRawExpr &>(other);
      IGNORE_RETURN udf_meta_.assign(tmp.get_udf_meta());
    }
  }
  return ret;
}

int ObNormalDllUdfRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("copy in Base class ObSysRawExpr failed", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf_meta_.name_, udf_meta_.name_))) {
      LOG_WARN("fail to write string", K(udf_meta_.name_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, udf_meta_.dl_, udf_meta_.dl_))) {
      LOG_WARN("fail to write string", K(udf_meta_.name_), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < udf_attributes_.count(); ++i) {
      if (OB_FAIL(ob_write_string(*inner_alloc_,
                                  udf_attributes_.at(i),
                                  udf_attributes_.at(i)))) {
        LOG_WARN("failed to write string", K(ret));
      }
    }
  }
  return ret;
}

int ObCollectionConstructRawExpr::set_access_names(
  const common::ObIArray<ObObjAccessIdent> &access_idents)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idents.count(); ++i) {
    OZ (access_names_.push_back(access_idents.at(i).access_name_));
  }
  return ret;
}

int ObCollectionConstructRawExpr::get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  CK (coll_schema_version_ != OB_INVALID_VERSION);
  CK (udt_id_ != common::OB_INVALID_ID);
  OX (obj_version.object_id_ = udt_id_);
  OX (obj_version.object_type_ = share::schema::DEPENDENCY_TYPE);
  OX (obj_version.version_ = coll_schema_version_);

  return ret;
}

int ObCollectionConstructRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObCollectionConstructRawExpr &tmp =
          static_cast<const ObCollectionConstructRawExpr &>(other);
      type_ = tmp.type_;
      capacity_ = tmp.capacity_;
      udt_id_ = tmp.udt_id_;
      elem_type_ = tmp.elem_type_;
      coll_schema_version_ = tmp.coll_schema_version_;
      if (OB_FAIL(access_names_.assign(tmp.access_names_))) {
        LOG_WARN("failed to assign access names", K(ret));
      }
    }
  }
  return ret;
}

int ObCollectionConstructRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    pl::ObPLDataType new_elem_type_;
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(new_elem_type_.deep_copy(*inner_alloc_, elem_type_))) {
      LOG_WARN("faile to copy elem type", K(ret));
    } else {
      elem_type_ = new_elem_type_;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_names_.count(); ++i) {
        if (OB_FAIL(ob_write_string(*inner_alloc_,
                                    access_names_.at(i),
                                    access_names_.at(i)))) {
          LOG_WARN("failed to copy string", K(ret));
        }
      }
    }
  }
  return ret;
}

ObExprOperator *ObCollectionConstructRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprCollectionConstruct *coll_op = NULL;
  if (T_FUN_PL_COLLECTION_CONSTRUCT == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make collection construct operator failed", K(ret));
    }
    CK (OB_NOT_NULL(coll_op = static_cast<ObExprCollectionConstruct *>(expr_op)));
    OX (coll_op->set_type(type_));
    OX (coll_op->set_capacity(capacity_));
    OX (coll_op->set_udt_id(udt_id_));
    if (OB_SUCC(ret)) {
      if (elem_type_.is_obj_type()) {
        CK (OB_NOT_NULL(elem_type_.get_data_type()));
        OX (coll_op->set_elem_type(*(elem_type_.get_data_type())));
      } else {
        ObDataType data_type;
        data_type.set_obj_type(ObExtendType);
        OX (coll_op->set_elem_type(data_type));
      }
    }
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObObjectConstructRawExpr::get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  CK (object_schema_version_ != OB_INVALID_VERSION);
  CK (udt_id_ != common::OB_INVALID_ID);
  OX (obj_version.object_id_ = udt_id_);
  OX (obj_version.object_type_ = share::schema::DEPENDENCY_TYPE);
  OX (obj_version.version_ = object_schema_version_);

  return ret;
}


int ObObjectConstructRawExpr::set_access_names(
  const common::ObIArray<ObObjAccessIdent> &access_idents)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idents.count(); ++i) {
    OZ (access_names_.push_back(access_idents.at(i).access_name_));
  }
  return ret;
}

int ObObjectConstructRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObObjectConstructRawExpr &tmp =
          static_cast<const ObObjectConstructRawExpr &>(other);
      rowsize_ = tmp.rowsize_;
      udt_id_ = tmp.udt_id_;
      object_schema_version_ = tmp.object_schema_version_;
      if (OB_FAIL(elem_types_.assign(tmp.elem_types_))) {
        LOG_WARN("failed to assign elem types", K(ret));
      } else if (OB_FAIL(access_names_.assign(tmp.access_names_))) {
        LOG_WARN("failed to assign access names", K(ret));
      }
    }
  }
  return ret;
}

int ObObjectConstructRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    CK (OB_NOT_NULL(inner_alloc_));
    for (int64_t i = 0; OB_SUCC(ret) && i < elem_types_.count(); ++i) {
      ObObj param;
      OZ (deep_copy_obj(*inner_alloc_, elem_types_.at(i).get_param(), param));
      OX (elem_types_.at(i).set_param(param));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < access_names_.count(); ++i) {
      OZ (ob_write_string(*inner_alloc_, access_names_.at(i), access_names_.at(i)));
    }
  }
  return ret;
}

ObExprOperator *ObObjectConstructRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprObjectConstruct *object_op = NULL;
  if (T_FUN_PL_OBJECT_CONSTRUCT == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make object construct operator failed", K(ret));
    }
    CK (OB_NOT_NULL(object_op = static_cast<ObExprObjectConstruct *>(expr_op)));
    OZ (object_op->set_elem_types(elem_types_));
    OX (object_op->set_rowsize(rowsize_));
    OX (object_op->set_udt_id(udt_id_));
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObPlQueryRefRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("failed to assign raw expr", K(ret));
    } else {
      const ObPlQueryRefRawExpr &tmp = static_cast<const ObPlQueryRefRawExpr &>(other);
      OZ (exprs_.assign(tmp.exprs_));
      OX (ps_sql_ = tmp.ps_sql_);
      OX (type_ = tmp.type_);
      OX (route_sql_ = tmp.route_sql_);
      OX (subquery_result_type_ = tmp.subquery_result_type_);
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObUDFParamDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  OB_UNIS_ENCODE(id1_);
  OB_UNIS_ENCODE(id2_);
  OB_UNIS_ENCODE(id3_);
  return ret;
}

DEFINE_DESERIALIZE(ObUDFParamDesc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  OB_UNIS_DECODE(id1_);
  OB_UNIS_DECODE(id2_);
  OB_UNIS_DECODE(id3_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObUDFParamDesc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  OB_UNIS_ADD_LEN(id1_);
  OB_UNIS_ADD_LEN(id2_);
  OB_UNIS_ADD_LEN(id3_);
  return len;
}

int ObPlQueryRefRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to deep copy raw expr fileds", K(ret));
  } else if (copier.deep_copy_attributes()) {
    CK (OB_NOT_NULL(inner_alloc_));
    OZ (ob_write_string(*inner_alloc_, route_sql_, route_sql_));
  }
  return ret;
}

int ObUDFRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObOpRawExpr failed", K(ret));
    } else {
      const ObUDFRawExpr &tmp =
          static_cast<const ObUDFRawExpr &>(other);
      udf_id_ = tmp.udf_id_;
      pkg_id_ = tmp.pkg_id_;
      type_id_ = tmp.type_id_;
      subprogram_path_ = tmp.subprogram_path_;
      udf_schema_version_ = tmp.udf_schema_version_;
      pkg_schema_version_ = tmp.pkg_schema_version_;
      pls_type_ = tmp.pls_type_;
      params_type_ = tmp.params_type_;
      database_name_ = tmp.database_name_;
      package_name_ = tmp.package_name_;
      is_parallel_enable_ = tmp.is_parallel_enable_;
      is_udt_udf_ = tmp.is_udt_udf_;
      is_pkg_body_udf_ = tmp.is_pkg_body_udf_;
      is_return_sys_cursor_ = tmp.is_return_sys_cursor_;
      is_aggregate_udf_ = tmp.is_aggregate_udf_;
      is_aggr_udf_distinct_ = tmp.is_aggr_udf_distinct_;
      nocopy_params_ = tmp.nocopy_params_;
      loc_ = tmp.loc_;
      is_udt_cons_ = tmp.is_udt_cons_;
      params_name_ = tmp.params_name_;
      params_desc_v2_ = tmp.params_desc_v2_;
      result_type_ = tmp.result_type_;
    }
  }
  return ret;
}

int ObUDFRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("copy in Base class ObOpRawExpr failed", K(ret));
  } else if (copier.deep_copy_attributes()) {
    CK (OB_NOT_NULL(inner_alloc_));
    OZ (ob_write_string(*inner_alloc_, database_name_, database_name_));
    OZ (ob_write_string(*inner_alloc_, package_name_, package_name_));
    for (int64_t i = 0; OB_SUCC(ret) && i < params_type_.count(); ++i) {
      ObExprResType &param_type = params_type_.at(i);
      ObObj param;
      OZ (deep_copy_obj(*inner_alloc_, param_type.get_param(), param));
      OX (param_type.set_param(param));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params_name_.count(); ++i) {
      CK (OB_NOT_NULL(inner_alloc_));
      OZ (ob_write_string(*inner_alloc_, params_name_.at(i), params_name_.at(i)));
    }
  }
  return ret;
}

ObExprOperator *ObUDFRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprUDF *udf_op = NULL;
  if (T_FUN_UDF == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
	    ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make user defined function operator failed", K(ret));
    } else if (OB_ISNULL(udf_op = static_cast<ObExprUDF*>(expr_op))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast expr to udf", K(ret));
    } else {
      udf_op->set_udf_id(udf_id_);
      udf_op->set_udf_package_id(pkg_id_);
      udf_op->set_result_type(result_type_);
      OZ (udf_op->set_subprogram_path(subprogram_path_));
      OZ (udf_op->set_params_type(params_type_));
      OZ (udf_op->set_params_desc(params_desc_v2_));
      OZ (udf_op->set_nocopy_params(nocopy_params_));
    }
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObUDFRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                    ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (!get_database_name().empty()) {
    OZ (BUF_PRINTF("%.*s.", get_database_name().length(), get_database_name().ptr()));
  }
  if (!get_package_name().empty()) {
    OZ (BUF_PRINTF("%.*s.", get_package_name().length(), get_package_name().ptr()));
  }
  OZ (ObSysFunRawExpr::get_name_internal(buf, buf_len, pos, type));
  return ret;
}

bool ObUDFRawExpr::inner_same_as(const ObRawExpr &expr,
                                 ObExprEqualCheckContext *check_context) const

{
  bool bool_ret = true;
  if (this == &expr) {
    // do nothing
  } else if (!is_deterministic() && (NULL == check_context ||
                                     (NULL != check_context && check_context->need_check_deterministic_))) {
    bool_ret = false;
  } else if (!ObSysFunRawExpr::inner_same_as(expr, check_context)) {
    bool_ret = false;
  } else {
    const ObUDFRawExpr *other = static_cast<const ObUDFRawExpr *>(&expr);
    bool_ret = udf_id_ == other->get_udf_id() &&
                pkg_id_ == other->get_pkg_id() &&
                type_id_ == other->get_type_id() &&
                pls_type_ == other->get_pls_type() &&
                database_name_.compare(other->get_database_name()) == 0 &&
                package_name_.compare(other->get_package_name()) == 0 &&
                is_deterministic() == other->is_deterministic() &&
                is_parallel_enable_ == other->is_parallel_enable() &&
                is_udt_udf_ == other->get_is_udt_udf() &&
                is_pkg_body_udf_ == other->is_pkg_body_udf() &&
                is_return_sys_cursor_ == other->get_is_return_sys_cursor() &&
                is_aggregate_udf_ == other->get_is_aggregate_udf() &&
                is_aggr_udf_distinct_ == other->get_is_aggr_udf_distinct() &&
                loc_ == other->get_loc() &&
                is_udt_cons_ == other->get_is_udt_cons() &&
                subprogram_path_.count() == other->get_subprogram_path().count() &&
                params_type_.count() == other->get_params_type().count() &&
                nocopy_params_.count() == other->get_nocopy_params().count() &&
                params_name_.count() == other->get_params_name().count() &&
                params_desc_v2_.count() == other->get_params_desc().count();
    for (int64_t i = 0; bool_ret && i < subprogram_path_.count(); ++i) {
      bool_ret = subprogram_path_.at(i) == other->get_subprogram_path().at(i);
    }
    for (int64_t i = 0; bool_ret && i < params_type_.count(); ++i) {
      bool_ret = params_type_.at(i) == other->get_params_type().at(i);
    }
    for (int64_t i = 0; bool_ret && i < nocopy_params_.count(); ++i) {
      bool_ret = nocopy_params_.at(i) == other->get_nocopy_params().at(i);
    }
    for (int64_t i = 0; bool_ret && i < params_name_.count(); ++i) {
      bool_ret = params_name_.at(i).compare(other->get_params_name().at(i)) == 0;
    }
    for (int64_t i = 0; bool_ret && i < params_desc_v2_.count(); ++i) {
      bool_ret = params_desc_v2_.at(i) == other->get_params_desc().at(i);
    }
  }
  return bool_ret;
}

int ObUDFRawExpr::get_schema_object_version(share::schema::ObSchemaGetterGuard &schema_guard,
                                            ObIArray<share::schema::ObSchemaObjVersion> &obj_versions)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaObjVersion obj_version;
  /*!
   * schema_version will be set when call ObRawExprUtils::resolve_udf_common_info
   *
   * udf_schema_version != OB_INVALID_VERSION && pkg_schema_version != OB_INVALID_VERSION :
   *  NOTICE : UNEXPECTED!!!
   * udf_schema_version != OB_INVALID_VERSION : standalone function call , add udf dependency
   * pkg_schema_version != OB_INVALID_VERSION : package function call, add package dependency
   * udf_schema_version == OB_INVALID_VERSION && pkg_schema_version == OB_INVALID_VERSION :
   *  subprogram function call, do not need add dependency
   */
  CK (!(udf_schema_version_ != OB_INVALID_VERSION && pkg_schema_version_ != OB_INVALID_VERSION));
  if (OB_FAIL(ret)) {
  } else if (udf_schema_version_ != common::OB_INVALID_VERSION) {
    CK (udf_id_ != common::OB_INVALID_ID);
    OX (obj_version.object_id_ = udf_id_);
    OX (obj_version.object_type_ = share::schema::DEPENDENCY_FUNCTION);
    OX (obj_version.version_ = udf_schema_version_);
  } else if (pkg_schema_version_ != common::OB_INVALID_VERSION) {
    CK (pkg_id_ != common::OB_INVALID_ID);
    OX (obj_version.object_id_ = pkg_id_);
    if (!is_udt_udf_) {
      OX (obj_version.object_type_ =
      is_pkg_body_udf_ ? share::schema::DEPENDENCY_PACKAGE_BODY
                         : share::schema::DEPENDENCY_PACKAGE);
    } else {
      OX (obj_version.object_type_ =
      is_pkg_body_udf_ ? share::schema::DEPENDENCY_TYPE_BODY
                         : share::schema::DEPENDENCY_TYPE);
    }
    OX (obj_version.version_ = pkg_schema_version_);
  } else {
    CK (udf_schema_version_ == OB_INVALID_VERSION && pkg_schema_version_ == OB_INVALID_VERSION);
    // do nothing ...
  }
  if (OB_FAIL(ret)) {
  } else if (common::OB_INVALID_ID != pkg_id_ && !is_udt_udf_) {
    const ObPackageInfo *spec_info = NULL;
    const ObPackageInfo *body_info = NULL;
    ObSchemaObjVersion ver;
    OZ (pl::ObPLPackageManager::get_package_schema_info(schema_guard, pkg_id_, spec_info, body_info));
    if (OB_NOT_NULL(spec_info)) {
      OX (ver.object_id_ = spec_info->get_package_id());
      OX (ver.version_ = spec_info->get_schema_version());
      OX (ver.object_type_ = DEPENDENCY_PACKAGE);
      OZ (obj_versions.push_back(ver));
    }
    if (OB_NOT_NULL(body_info)) {
      OX (ver.object_id_ = body_info->get_package_id());
      OX (ver.version_ = body_info->get_schema_version());
      OX (ver.object_type_ = DEPENDENCY_PACKAGE_BODY);
      OZ (obj_versions.push_back(ver));
    }
  } else {
    OZ (obj_versions.push_back(obj_version));
  }
  return ret;
}

int ObPLIntegerCheckerRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObPLIntegerCheckerRawExpr &tmp =
          static_cast<const ObPLIntegerCheckerRawExpr &>(other);
      pl_integer_type_ = tmp.pl_integer_type_;
      pl_integer_range_ = tmp.pl_integer_range_;
    }
  }
  return ret;
}

int ObPLGetCursorAttrRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObPLGetCursorAttrRawExpr &tmp =
          static_cast<const ObPLGetCursorAttrRawExpr &>(other);
      cursor_info_ = tmp.cursor_info_;
    }
  }
  return ret;
}

ObExprOperator *ObPLGetCursorAttrRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprPLGetCursorAttr *op = NULL;
  if (T_FUN_PL_GET_CURSOR_ATTR == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObSysFunRawExpr::get_op())) {
	    ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make user defined function operator failed", K(ret));
    } else if (OB_ISNULL(op = static_cast<ObExprPLGetCursorAttr*>(expr_op))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast expr to udf", K(ret));
    } else {
      OX (op->set_pl_get_cursor_attr_info(get_pl_get_cursor_attr_info()));
    }
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObPLSQLCodeSQLErrmRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObPLSQLCodeSQLErrmRawExpr &tmp =
          static_cast<const ObPLSQLCodeSQLErrmRawExpr &>(other);
      is_sqlcode_ = tmp.is_sqlcode_;
    }
  }
  return ret;
}

ObExprOperator *ObPLSQLCodeSQLErrmRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprPLSQLCodeSQLErrm *op = NULL;
  if (T_FUN_PL_SQLCODE_SQLERRM == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
	    ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make user defined function operator failed", K(ret));
    } else if (OB_ISNULL(op = static_cast<ObExprPLSQLCodeSQLErrm*>(expr_op))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to cast expr to udf", K(ret));
    } else {
      op->set_is_sqlcode(get_is_sqlcode());
    }
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObPLSQLVariableRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObPLSQLVariableRawExpr &tmp =
          static_cast<const ObPLSQLVariableRawExpr &>(other);
      plsql_line_ = tmp.plsql_line_;
      plsql_variable_ = tmp.plsql_variable_;
    }
  }
  return ret;
}

int ObPLSQLVariableRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner allocator or expr factory is NULL", K(inner_alloc_), K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, plsql_variable_, plsql_variable_))) {
      LOG_WARN("failed to copy plsql variable", K(ret));
    }
  }
  return ret;
}

ObExprOperator *ObPLSQLVariableRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprPLSQLVariable *op = NULL;
  if (T_FUN_PLSQL_VARIABLE == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make plsql variable operator failed", K(ret));
    } else if (OB_ISNULL(op = static_cast<ObExprPLSQLVariable *>(expr_op))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast expr to ObExprPLSQLVariable", K(ret));
    } else {
      op->set_plsql_line(get_plsql_line());
      op->deep_copy_plsql_variable(get_plsql_variable());
    }
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

int ObCallParamRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObCallParamRawExpr &tmp =
          static_cast<const ObCallParamRawExpr &>(other);
      name_ = tmp.name_;
      expr_ = tmp.expr_;
    }
  }
  return ret;
}

int ObCallParamRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOpRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (OB_FAIL(copier.copy(expr_))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (copier.deep_copy_attributes()) {
    CK (OB_NOT_NULL(inner_alloc_));
    OZ (ob_write_string(*inner_alloc_, name_, name_));
  }
  return ret;
}

int ObSetOpRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObSetOpRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                 const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

bool ObSetOpRawExpr::inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  UNUSED(check_context);
  if (get_expr_type() != expr.get_expr_type()) {
  } else if (&expr == this) {
    bool_ret = true;
  } else if (OB_UNLIKELY(check_context != NULL) &&
             OB_UNLIKELY(check_context->override_set_op_compare_)) {
    bool_ret = check_context->compare_set_op_expr(*this,
                                                  static_cast<const ObSetOpRawExpr &>(expr));
  }
  return bool_ret;
}

bool ObExprEqualCheckContext::compare_set_op_expr(const ObSetOpRawExpr& left,
                                                  const ObSetOpRawExpr& right)
{
  return &left == &right;
}

int ObSetOpRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObRawExpr failed", K(ret));
    } else {
      const ObSetOpRawExpr &tmp =
          static_cast<const ObSetOpRawExpr &>(other);
      idx_ = tmp.idx_;
    }
  }
  return ret;
}

int ObSetOpRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                      ExplainType type) const
{
  int ret = OB_SUCCESS;
  const char *op_name = "UNKNOWN";
  switch (get_expr_type()) {
    case T_OP_UNION:
      op_name = "UNION";
      break;
    case T_OP_INTERSECT:
      op_name = "INTERSECT";
      break;
    case T_OP_EXCEPT:
      if (lib::is_oracle_mode()) {
        op_name = "MINUS";
      } else {
        op_name = "EXCEPT";
      }
      break;
    default:
      break;
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(BUF_PRINTF("%s([%ld])", op_name, idx_ + 1))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else {}
  if (OB_SUCCESS == ret && EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

int Bound::assign(const Bound &other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  is_preceding_ = other.is_preceding_;
  is_nmb_literal_ = other.is_nmb_literal_;
  interval_expr_ = other.interval_expr_;
  date_unit_expr_ = other.date_unit_expr_;
  for (int64_t i = 0; i < BOUND_EXPR_MAX; ++i) {
    exprs_[i] = other.exprs_[i];
  }
  return ret;
}

int Bound::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copier.copy(date_unit_expr_))) {
    LOG_WARN("failed to copy date unit expr", K(ret));
  }
  return ret;
}

int Bound::replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                        const common::ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::replace_expr(
                other_exprs, new_exprs, interval_expr_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(
                other_exprs, new_exprs, date_unit_expr_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < BOUND_EXPR_MAX; ++i) {
    if (OB_ISNULL(exprs_[i])) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::replace_expr(
                         other_exprs, new_exprs, exprs_[i]))) {
      LOG_WARN("failed to replace bound expr", K(ret));
    }
  }
  return ret;
}

bool Bound::same_as(const Bound &other, ObExprEqualCheckContext *check_context) const
{
  bool bret = true;
  if (type_ != other.type_ ||
      is_preceding_ != other.is_preceding_ ||
      is_nmb_literal_ != other.is_nmb_literal_) {
    bret = false;
  }
  if (bret) {
    if ((interval_expr_ == NULL && other.interval_expr_ == NULL) ||
        (interval_expr_ != NULL && other.interval_expr_ != NULL &&
         interval_expr_->same_as(*(other.interval_expr_), check_context))) {
      bret = true;
    } else {
      bret = false;
    }
  }
  if (bret) {
    if ((date_unit_expr_ == NULL && other.date_unit_expr_ == NULL) ||
        (date_unit_expr_ != NULL && other.date_unit_expr_ != NULL &&
         date_unit_expr_->same_as(*(other.date_unit_expr_), check_context))) {
      bret = true;
    } else {
      bret = false;
    }
  }
  for (int64_t i = 0; bret && i < BOUND_EXPR_MAX; ++i) {
    if ((exprs_[i] == NULL && other.exprs_[i] == NULL) ||
        (exprs_[i] != NULL && other.exprs_[i] != NULL &&
         exprs_[i]->same_as(*(other.exprs_[i]), check_context))) {
      bret = true;
    } else {
      bret = false;
    }
  }
  return bret;
}

int ObFrame::assign(const ObFrame &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    win_type_ = other.win_type_;
    is_between_ = other.is_between_;
    upper_ = other.upper_;
    lower_ = other.lower_;
  }
  return ret;
}

int ObWindow::assign(const ObWindow &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_FAIL(partition_exprs_.assign(other.partition_exprs_))) {
      LOG_WARN("failed to assign partition exprs", K(ret));
    } else if (OB_FAIL(order_items_.assign(other.order_items_))) {
      LOG_WARN("failed to assign order items", K(ret));
    } else if (OB_FAIL(ObFrame::assign(other))) {
      LOG_WARN("failed to assign frame", K(ret));
    } else {
      win_name_ = other.win_name_;
      has_frame_orig_ = other.has_frame_orig_;
    }
  }
  return ret;
}

void ObWinFunRawExpr::clear_child()
{
  func_type_ = T_MAX;
  func_params_.reset();
  partition_exprs_.reset();
  order_items_.reset();
}

int ObWinFunRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("failed to assign raw expr", K(ret));
    } else {
      const ObWinFunRawExpr &tmp =
          static_cast<const ObWinFunRawExpr &>(other);
      if (OB_FAIL(ObWindow::assign(tmp))) {
        LOG_WARN("failed to assign window", K(ret));
      } else if (OB_FAIL(func_params_.assign(tmp.func_params_))) {
        LOG_WARN("failed to assign func params", K(ret));
      } else if (OB_FAIL(upper_.assign(tmp.upper_))) {
        LOG_WARN("failed to assign upper bound", K(ret));
      } else if (OB_FAIL(lower_.assign(tmp.lower_))) {
        LOG_WARN("failed to assign lower bound", K(ret));
      } else {
        func_type_ = tmp.func_type_;
        is_ignore_null_ = tmp.is_ignore_null_;
        is_from_first_ = tmp.is_from_first_;
        agg_expr_ = tmp.agg_expr_;
        sort_str_ = tmp.sort_str_;
        pl_agg_udf_expr_ = tmp.pl_agg_udf_expr_;
      }
    }
  }
  return ret;
}

/**
 * @brief ObWinFunRawExpr::inner_deep_copy
 * A winfun expr contains a aggr expr as its first param.
 * The aggr expr is owned by the winfunc expr.
 * Hence, copying the winfunc expr always results in copying the aggr expr.
 * @return
 */
int ObWinFunRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_agg_expr = NULL;
  if (OB_FAIL(ObRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (OB_FAIL(copier.copy(pl_agg_udf_expr_))) {
    LOG_WARN("failed to copy partition exprs", K(ret));
  } else if (OB_FAIL(upper_.inner_deep_copy(copier))) {
    LOG_WARN("failed to copy upper bound", K(ret));
  } else if (OB_FAIL(lower_.inner_deep_copy(copier))) {
    LOG_WARN("failed to copy lower bound", K(ret));
  } else if (NULL != agg_expr_) {
    if (OB_FAIL(copier.do_copy_expr(agg_expr_, new_agg_expr))) {
      LOG_WARN("failed to create and copy expr", K(ret), K(agg_expr_));
    } else if (OB_ISNULL(new_agg_expr) || OB_UNLIKELY(!new_agg_expr->is_aggr_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new aggregation expr is invalid", K(ret), K(new_agg_expr));
    } else {
      agg_expr_ = static_cast<ObAggFunRawExpr *>(new_agg_expr);
    }
  }
  if (OB_SUCC(ret) && copier.deep_copy_attributes()) {
    if (OB_ISNULL(inner_alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner alloc is null", K(ret));
    } else if (OB_FAIL(ob_write_string(*inner_alloc_, sort_str_, sort_str_))) {
      LOG_WARN("failed to copy string", K(ret));
    }
  }
  return ret;
}

int64_t ObWinFunRawExpr::get_partition_param_index(int64_t index)
{
  int64_t count = agg_expr_ != NULL ? agg_expr_->get_param_count() : 0;
  count = pl_agg_udf_expr_ != NULL ? count + 1 : count;
  count += func_params_.count();
  count += index;
  return count;
}

int ObWinFunRawExpr::replace_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  int64_t count = get_partition_param_index(0);
  if (index < count || index >= count + partition_exprs_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("only replace partition expr supported", K(ret), K(index), K(*this));
  } else {
    ObRawExpr *&target_expr = partition_exprs_.at(index - count);
    target_expr = expr;
  }
  return ret;
}

int ObWinFunRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                  const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(
                       other_exprs, new_exprs,
                       reinterpret_cast<ObRawExpr *&>(agg_expr_)))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     func_params_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     partition_exprs_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    pl_agg_udf_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr_for_order_item(other_exprs,
                                                                   new_exprs,
                                                                   order_items_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(upper_.replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr in upper bound", K(ret));
  } else if (OB_FAIL(lower_.replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr in lower bound", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

bool ObWinFunRawExpr::inner_same_as(const ObRawExpr &expr,
                                    ObExprEqualCheckContext *check_context) const
{
  bool bret = false;
  if (expr.is_win_func_expr()) {
    bret = true;
    const ObWinFunRawExpr &other_ma = static_cast<const ObWinFunRawExpr&>(expr);
    //对窗口函数的类型做检查，如果类型不同即不同
    if (other_ma.get_func_type() != get_func_type() ||
        other_ma.get_window_type() != get_window_type() ||
        other_ma.is_between() != is_between()) {
      bret = false;
    //由于name window会构造一个count(1) over的窗口函数，这里我们需要比较的是name Windows的名称
    //如果是mysql模式下，名称是敏感的，在生成count(1) over的时候不需要去重。oracle模式为true.
    } else if (0 != other_ma.win_name_.case_compare(win_name_)) {
      bret = false;
    //如果是nth_value，有可能是first_value或者last_value转换成的，故需要检查is_from_first_字段
    //和is_ignore_null_两个字段。
    } else if (T_WIN_FUN_NTH_VALUE == other_ma.get_func_type()
        && (other_ma.is_from_first_ != is_from_first_
        || other_ma.is_ignore_null_ != is_ignore_null_)) {
      bret = false;
    //对于lead和lag两个函数也需要检查is_ignore_null_这个字段
    } else if ((T_WIN_FUN_LAG == other_ma.get_func_type()
        || T_WIN_FUN_LEAD == other_ma.get_func_type())
        && other_ma.is_ignore_null_ != is_ignore_null_) {
      bret = false;
    //如果是聚集函数的窗口函数，需要比较聚集函数。
    } else if (agg_expr_ != NULL && other_ma.agg_expr_ != NULL) {
      bret = agg_expr_->same_as(*(other_ma.agg_expr_), check_context) ? true : false;
    } else if (pl_agg_udf_expr_ != NULL && other_ma.pl_agg_udf_expr_ != NULL) {
      bret = pl_agg_udf_expr_->same_as(*(other_ma.pl_agg_udf_expr_), check_context) ? true : false;
    } else { /* do nothing. */ }

    //如果之上都是相同的，需要比较窗口属性
    if (!bret) {
    } else if (other_ma.get_param_count() != get_param_count()
               || other_ma.func_params_.count() != func_params_.count()
               || other_ma.partition_exprs_.count() != partition_exprs_.count()
               || other_ma.order_items_.count() != order_items_.count()
               || !upper_.same_as(other_ma.upper_, check_context)
               || !lower_.same_as(other_ma.lower_, check_context)) {
      bret = false;
    } else {
      for (int64_t i = 0; bret && i < other_ma.func_params_.count(); ++i) {
        if (other_ma.func_params_.at(i) == NULL || func_params_.at(i) == NULL ||
            !func_params_.at(i)->same_as(*other_ma.func_params_.at(i), check_context)) {
          bret = false;
        }
      }
      for (int64_t i = 0; bret && i < other_ma.partition_exprs_.count(); ++i) {
        if (other_ma.partition_exprs_.at(i) == NULL || partition_exprs_.at(i) == NULL ||
            !partition_exprs_.at(i)->same_as(*other_ma.partition_exprs_.at(i), check_context)) {
          bret = false;
        }
      }
      for (int64_t i = 0; bret && i < other_ma.order_items_.count(); ++i) {
        if (other_ma.order_items_.at(i).order_type_ != order_items_.at(i).order_type_) {
          bret = false;
        } else if (other_ma.order_items_.at(i).expr_ == NULL || order_items_.at(i).expr_ == NULL ||
                   !order_items_.at(i).expr_->same_as(*other_ma.order_items_.at(i).expr_,
                                                      check_context)) {
          bret = false;
        }
      }
    }
  }
  return bret;
}

const ObRawExpr *ObWinFunRawExpr::get_param_expr(int64_t index) const
{
  int i = 0;
  ObRawExpr *expr = NULL;
  if (pl_agg_udf_expr_ != NULL) {
    if (i++ == index) {
      expr = pl_agg_udf_expr_;
    }
  }
  for (int64_t j = 0; NULL == expr && agg_expr_ != NULL && j < agg_expr_->get_param_count(); ++j) {
    if (i++ == index) {
      expr = agg_expr_->get_param_expr(j);
    }
  }
  for (int64_t j = 0; NULL == expr && j < func_params_.count(); ++j) {
    if (i++ == index) {
      expr = func_params_.at(j);
    }
  }
  for (int64_t j = 0; NULL == expr && j < partition_exprs_.count(); ++j) {
    if (i++ == index) {
      expr = partition_exprs_.at(j);
    }
  }
  for (int64_t j = 0; NULL == expr && j < order_items_.count(); ++j) {
    if (i++ == index) {
      expr = order_items_.at(j).expr_;
    }
  }
  if (NULL == expr) {
    if (upper_.interval_expr_ != NULL) {
      if (i++ == index) {
        expr = upper_.interval_expr_;
      }
    }
  }
  if (NULL == expr) {
    if (lower_.interval_expr_ != NULL) {
      if (i++ == index) {
        expr = lower_.interval_expr_;
      }
    }
  }
  for (int64_t j = 0; j < 2 && NULL == expr; ++j) {
    const Bound *bound = 0 == j ? &upper_ : &lower_;
    for (int64_t k = 0; k < BOUND_EXPR_MAX && NULL == expr; ++k) {
      if (NULL != bound->exprs_[k]) {
        if (i++ == index) {
          expr = bound->exprs_[k];
        }
      }
    }
  }
  return expr;
}

ObRawExpr *&ObWinFunRawExpr::get_param_expr(int64_t index)
{
  int i = 0;
  if (pl_agg_udf_expr_ != NULL) {
    if (i++ == index) {
      return pl_agg_udf_expr_;
    }
  }
  for (int64_t j = 0; agg_expr_ != NULL && j < agg_expr_->get_param_count(); ++j) {
    if (i++ == index) {
      return agg_expr_->get_param_expr(j);
    }
  }
  for (int64_t j = 0; j < func_params_.count(); ++j) {
    if (i++ == index) {
      return func_params_.at(j);
    }
  }
  for (int64_t j = 0; j < partition_exprs_.count(); ++j) {
    if (i++ == index) {
      return partition_exprs_.at(j);
    }
  }
  for (int64_t j = 0; j < order_items_.count(); ++j) {
    if (i++ == index) {
      return order_items_.at(j).expr_;
    }
  }
  if (upper_.interval_expr_ != NULL) {
    if (i++ == index) {
      return upper_.interval_expr_;
    }
  }
  if (lower_.interval_expr_ != NULL) {
    if (i++ == index) {
      return lower_.interval_expr_;
    }
  }
  for (int64_t j  = 0; j < 2; ++j) {
    Bound *bound = 0 == j ? &upper_ : &lower_;
    for (int64_t k = 0; k < BOUND_EXPR_MAX; ++k) {
      if (NULL != bound->exprs_[k]) {
        if (i++ == index) {
          return bound->exprs_[k];
        }
      }
    }
  }
  return USELESS_POINTER;
}

int ObWinFunRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

uint64_t ObWinFunRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_value = seed;
  for (int64_t i = 0; i < get_param_count(); ++i) {
    if (NULL != get_param_expr(i)) {
      hash_value = do_hash(*get_param_expr(i), hash_value);
    }
  }
  return hash_value;
}

int ObWinFunRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                       ExplainType type) const
{
  int ret = OB_SUCCESS;

  if (agg_expr_ != NULL) {
    ret = get_agg_expr()->get_name(buf, buf_len, pos, type);
  } else {
    if (OB_FAIL(BUF_PRINTF("%s(", get_type_name(func_type_)))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
    // partition_by && order_by will be printed in ObLogWindowFunction operator
    for (int64_t i = 0; OB_SUCC(ret) && i < func_params_.count(); ++i) {
      ObRawExpr *func_param;
      if (0 != i) {
        if (OB_FAIL(BUF_PRINTF(","))) {
          LOG_WARN("Failed to add comma", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(func_param = func_params_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("func param is NULL", K(ret));
      } else if (OB_SUCC(ret) && OB_FAIL(func_param->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("fail to BUF_PRINTF", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  }

  if (OB_SUCCESS == ret && EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

OB_SERIALIZE_MEMBER(Bound, type_, is_preceding_, is_nmb_literal_);

int ObPseudoColumnRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObTerminalRawExpr::assign(other))) {
      LOG_WARN("fail to assign", K(ret));
    } else {
      const ObPseudoColumnRawExpr &tmp =
          static_cast<const ObPseudoColumnRawExpr &>(other);
      cte_cycle_value_ = tmp.cte_cycle_value_;
      cte_cycle_default_value_ = tmp.cte_cycle_default_value_;
      table_id_ = tmp.table_id_;
      table_name_ = tmp.table_name_;
      data_access_path_ = tmp.data_access_path_;
    }
  }
  return ret;
}

int ObPseudoColumnRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                        const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

bool ObPseudoColumnRawExpr::inner_same_as(const ObRawExpr &expr,
                                          ObExprEqualCheckContext *check_context/* = NULL*/) const
{
  UNUSED(check_context);
  return type_ == expr.get_expr_type() &&
         table_id_ == static_cast<const ObPseudoColumnRawExpr&>(expr).get_table_id() &&
         0 == data_access_path_.compare(static_cast<const ObPseudoColumnRawExpr&>(expr).get_data_access_path());
}

int ObPseudoColumnRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

uint64_t ObPseudoColumnRawExpr::hash_internal(uint64_t seed) const
{
  return do_hash(get_expr_type(), seed);
}

int ObPseudoColumnRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                             ExplainType type) const
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  switch (get_expr_type()) {
    case T_LEVEL:
      if (OB_FAIL(BUF_PRINTF("LEVEL"))) {
        LOG_WARN("fail to print", K(ret));
      }
      break;
    case T_CONNECT_BY_ISCYCLE:
      if (OB_FAIL(BUF_PRINTF("CONNECT_BY_ISCYCLE"))) {
        LOG_WARN("fail to print", K(ret));
      }
      break;
    case T_CONNECT_BY_ISLEAF:
      if (OB_FAIL(BUF_PRINTF("CONNECT_BY_ISLEAF"))) {
        LOG_WARN("fail to print", K(ret));
      }
      break;
    case T_CTE_SEARCH_COLUMN:
      if (OB_FAIL(BUF_PRINTF("T_CTE_SEARCH_COLUMN"))) {
        LOG_WARN("fail to print", K(ret));
      }
      break;
    case T_CTE_CYCLE_COLUMN:
      if (OB_FAIL(BUF_PRINTF("T_CTE_CYCLE_COLUMN"))) {
        LOG_WARN("fail to print", K(ret));
      }
      break;
    case T_ORA_ROWSCN:
      if (OB_FAIL(BUF_PRINTF("ORA_ROWSCN"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    case T_PSEUDO_GROUP_ID:
    case T_PSEUDO_STMT_ID:
    case T_PSEUDO_GROUP_PARAM:
    case T_PSEUDO_IDENTIFY_SEQ:
      if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, expr_name_))) {
        LOG_WARN("failed to print expr name", K(ret));
      }
      break;
    case T_PSEUDO_EXTERNAL_FILE_URL:
    case T_PSEUDO_PARTITION_LIST_COL:
    case T_PSEUDO_EXTERNAL_FILE_COL:
    case T_PSEUDO_EXTERNAL_FILE_ROW:
      if (!table_name_.empty() && OB_FAIL(BUF_PRINTF("%.*s.", table_name_.length(), table_name_.ptr()))) {
        LOG_WARN("failed to print table name", K(ret));
      } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, expr_name_))) {
        LOG_WARN("failed to print expr name", K(ret));
      }
      break;
    case T_TABLET_AUTOINC_NEXTVAL:
      if (OB_FAIL(BUF_PRINTF("T_HIDDEN_PK"))) {
        LOG_WARN("failed to print", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr type", K(get_expr_type()));
  }
  if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

ObOpPseudoColumnRawExpr::ObOpPseudoColumnRawExpr()
  : name_("")
{
  set_expr_class(EXPR_OP_PSEUDO_COLUMN);
}

ObOpPseudoColumnRawExpr::ObOpPseudoColumnRawExpr(ObItemType expr_type /* = T_INVALID */)
  : ObIRawExpr(expr_type),
    ObTerminalRawExpr(expr_type),
    name_("")
{
  set_expr_class(EXPR_OP_PSEUDO_COLUMN);
}

ObOpPseudoColumnRawExpr::ObOpPseudoColumnRawExpr(ObIAllocator &alloc)
  : ObIRawExpr(alloc),
    ObTerminalRawExpr(alloc),
    name_("")
{
  set_expr_class(EXPR_OP_PSEUDO_COLUMN);
}

ObOpPseudoColumnRawExpr::~ObOpPseudoColumnRawExpr()
{
}


int ObOpPseudoColumnRawExpr::assign(const ObOpPseudoColumnRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    name_ = other.name_;
    set_result_type(other.get_result_type());
    OZ(ObTerminalRawExpr::assign(other));
  }
  return ret;
}

int ObOpPseudoColumnRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTerminalRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to inner deep copy terminal expr", K(ret));
  }
  return ret;
}

int ObOpPseudoColumnRawExpr::replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                                          const common::ObIArray<ObRawExpr *> &new_exprs)
{
  return ObTerminalRawExpr::replace_expr(other_exprs, new_exprs);
}

bool ObOpPseudoColumnRawExpr::inner_same_as(
    const ObRawExpr &expr, ObExprEqualCheckContext *) const
{
  return get_expr_type() == expr.get_expr_type()
      && strcmp(name_, static_cast<const ObOpPseudoColumnRawExpr &>(expr).name_) == 0;
}

int ObOpPseudoColumnRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObOpPseudoColumnRawExpr::get_name_internal(char *buf,
                                               const int64_t buf_len,
                                               int64_t &pos,
                                               ExplainType type) const
{
  int ret = OB_SUCCESS;
  OZ(BUF_PRINTF("%s", name_));
  if (OB_FAIL(ret)) {
  } else if (EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

ObRawExprPointer::ObRawExprPointer() : expr_group_() {
}

ObRawExprPointer::~ObRawExprPointer() {
}

int ObRawExprPointer::get(ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;
  if (expr_group_.count() <= 0
      || OB_ISNULL(expr_group_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr pointer is not set", K(ret));
  } else {
    expr = *(expr_group_.at(0));
  }
  return ret;
}

int ObRawExprPointer::set(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_group_.count(); ++i) {
    if (OB_ISNULL(expr_group_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr pointer is null", K(ret));
    } else if (*expr_group_.at(i) == expr) {
      // not changed
      break;
    } else {
      *expr_group_.at(i) = expr;
    }
  }
  return ret;
}

int ObRawExprPointer::add_ref(ObRawExpr **expr)
{
  return expr_group_.push_back(expr);
}

int ObRawExprPointer::assign(const ObRawExprPointer &other)
{
  return expr_group_.assign(other.expr_group_);
}

int ObMultiSetRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObOpRawExpr::assign(other))) {
      LOG_WARN("failed to assign multiset expr", K(ret));
    } else {
      const ObMultiSetRawExpr &tmp =
          static_cast<const ObMultiSetRawExpr &>(other);
      ms_type_ = tmp.ms_type_;
      ms_modifier_ = tmp.ms_modifier_;
    }
  }
  return ret;
}

bool ObMultiSetRawExpr::inner_same_as(const ObRawExpr &expr,
                                      ObExprEqualCheckContext *check_context) const
{
  return ObOpRawExpr::inner_same_as(expr, check_context);
}

bool ObCollPredRawExpr::inner_same_as(const ObRawExpr &expr,
                                      ObExprEqualCheckContext *check_context) const
{
  return ObMultiSetRawExpr::inner_same_as(expr, check_context);
}

void ObExprParamCheckContext::init(const ObIArray<ObHiddenColumnItem> *calculable_items,
                                   const ObIArray<ObPCParamEqualInfo> *equal_param_constraints,
                                   EqualSets *equal_sets)

{
  calculable_items_ = calculable_items;
  equal_param_constraints_ = equal_param_constraints;
  equal_sets_ = equal_sets;
}

bool ObExprParamCheckContext::compare_column(const ObColumnRefRawExpr &left,
                                             const ObColumnRefRawExpr &right)
{
  bool b_ret = false;
  if (&left == &right) {
    b_ret = true;
  } else if (left.get_expr_type() != right.get_expr_type()) {
  } else if (left.get_table_id() == right.get_table_id() &&
             left.get_column_id() == right.get_column_id()) {
    b_ret = true;
  } else if (NULL != equal_sets_) {
    b_ret = ObOptimizerUtil::is_expr_equivalent(&left, &right, *equal_sets_);
  }
  return b_ret;
}

bool ObExprParamCheckContext::compare_const(const ObConstRawExpr &left, const ObConstRawExpr &right)
{
  int &ret = err_code_;
  bool bret = false;
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (&left == &right) {
    bret = true;
  } else if (left.get_result_type() != right.get_result_type()) {
    /*do thing*/
  } else if (left.is_param_expr() && right.is_param_expr()) {
    bool is_left_calc_item = false;
    bool is_right_calc_item = false;
    if (OB_FAIL(is_pre_calc_item(left, is_left_calc_item))) {
      LOG_WARN("failed to is pre calc item", K(ret));
    } else if (OB_FAIL(is_pre_calc_item(right, is_right_calc_item))) {
      LOG_WARN("failed to is pre calc item", K(ret));
    } else if (is_left_calc_item && is_right_calc_item) {
      const ObRawExpr *left_param = NULL;
      const ObRawExpr *right_param = NULL;
      if (OB_FAIL(get_calc_expr(left.get_value().get_unknown(), left_param))) {
        LOG_WARN("failed to get calculable expr", K(ret));
      } else if (OB_FAIL(get_calc_expr(right.get_value().get_unknown(), right_param))) {
        LOG_WARN("failed to get calculable expr", K(ret));
      } else if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param exprs are null", K(ret), K(left_param), K(right_param));
      } else {
        bret = left_param->same_as(*right_param, this);
      }
    } else if (is_left_calc_item || is_right_calc_item) {
        bret = false;
    } else {
      ObPCParamEqualInfo info;
      const int64_t l_param_idx = left.get_value().get_unknown();
      const int64_t r_param_idx = right.get_value().get_unknown();
      if (l_param_idx == r_param_idx) {
        bret = true;
      } else if (NULL != equal_param_constraints_) {
        for (int64_t i = 0; OB_SUCC(ret) && !bret && i < equal_param_constraints_->count(); ++i) {
          if (equal_param_constraints_->at(i).first_param_idx_ == l_param_idx &&
              equal_param_constraints_->at(i).second_param_idx_ == r_param_idx) {
            bret = true;
          } else if (equal_param_constraints_->at(i).first_param_idx_ == r_param_idx &&
                     equal_param_constraints_->at(i).second_param_idx_ == l_param_idx) {
            bret = true;
          }
        }
      } else {/*do nothing*/}
    }
  } else if (left.is_param_expr() || right.is_param_expr()) {
    /*do nothing*/
  } else {
    bret = left.get_value().is_equal(right.get_value(), CS_TYPE_BINARY);
  }
  return bret;
}

int ObExprParamCheckContext::is_pre_calc_item(const ObConstRawExpr &const_expr, bool &is_calc)
{
  int ret = OB_SUCCESS;
  int64_t calc_count = 0;
  is_calc = false;
  if (OB_ISNULL(calculable_items_) || OB_UNLIKELY((calc_count = calculable_items_->count()) < 0
      || const_expr.get_expr_type() != T_QUESTIONMARK)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(calculable_items_), K(const_expr.get_expr_type()),
                                     K(calc_count));
  } else if (const_expr.has_flag(IS_DYNAMIC_PARAM)) {
    is_calc = true;
  } else if (calc_count > 0) {
    int64_t q_idx = const_expr.get_value().get_unknown();
    int64_t min_calc_index = calculable_items_->at(0).hidden_idx_;
    if (OB_UNLIKELY(q_idx < 0 || min_calc_index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid argument", K(q_idx), K(min_calc_index));
    } else if (q_idx - min_calc_index >= 0 && q_idx - min_calc_index < calc_count) {
      is_calc = true;
    } else {/*do nothing*/}
  }
  return ret;
}

int ObExprParamCheckContext::get_calc_expr(const int64_t param_idx, const ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(calculable_items_) || calculable_items_->count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calculable_items_ is null", K(ret));
  } else {
    int64_t offset = param_idx - calculable_items_->at(0).hidden_idx_;
    if (offset < 0 || offset >= calculable_items_->count() ||
        param_idx != calculable_items_->at(offset).hidden_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param index", K(ret), K(param_idx), K(offset));
    } else {
      expr = calculable_items_->at(offset).expr_;
    }
  }
  return ret;
}

int ObPlQueryRefRawExpr::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                              const ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  return ret;
}

void ObPlQueryRefRawExpr::clear_child()
{
  exprs_.reset();
}

bool ObPlQueryRefRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  bool bool_ret = false;
  if (get_expr_type() == expr.get_expr_type()) {
    const ObPlQueryRefRawExpr &u_expr = static_cast<const ObPlQueryRefRawExpr &>(expr);
    if (check_context != NULL && check_context->override_query_compare_) {
      bool_ret = check_context->compare_query(*this, u_expr);
    } else {
      // very tricky, check the definition of ref_stmt_ and get_ref_stmt()
      bool_ret = (get_ps_sql() == u_expr.get_ps_sql() &&
                  get_stmt_type() == u_expr.get_stmt_type() &&
                  get_route_sql() == u_expr.get_route_sql() &&
                  get_subquery_result_type() == u_expr.get_subquery_result_type());
    }
  }
  return bool_ret;
}

bool ObExprEqualCheckContext::compare_query(const ObPlQueryRefRawExpr &left,
                                            const ObPlQueryRefRawExpr &right)
{
  return left.get_ps_sql() == right.get_ps_sql() &&
         left.get_stmt_type() == right.get_stmt_type() &&
         left.get_route_sql() == right.get_route_sql() &&
         left.get_subquery_result_type() == right.get_subquery_result_type();
}

int ObPlQueryRefRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

int ObPlQueryRefRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos,
                                         ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (EXPLAIN_HINT_FORMAT == type) {
    if (OB_FAIL(BUF_PRINTF("PL_SQ"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    }
  } else if (OB_FAIL(BUF_PRINTF("subquery(%.*s)", ps_sql_.length(), ps_sql_.ptr()))) {
    LOG_WARN("fail to BUF_PRINTF", K(ret));
  } else if (EXPLAIN_EXTENDED == type) {
    if (OB_FAIL(BUF_PRINTF("("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {}
  }
  return ret;
}

int ObRawExprFactory::create_raw_expr(ObRawExpr::ExprClass expr_class,
                                      ObItemType expr_type,
                                      ObRawExpr *&dest)
{
  int ret = OB_SUCCESS;
  switch (expr_class) {
  case ObRawExpr::EXPR_QUERY_REF: {
    ObQueryRefRawExpr *dest_query_ref = NULL;
    if (OB_FAIL(create_raw_expr(T_REF_QUERY, dest_query_ref)) ||
        OB_ISNULL(dest_query_ref)) {
      LOG_WARN("failed to allocate raw expr", K(dest_query_ref), K(ret));
    } else {
      dest = dest_query_ref;
    }
    break;
  }
  case ObRawExpr::EXPR_COLUMN_REF: {
    ObColumnRefRawExpr *dest_column_ref = NULL;
    if (OB_FAIL(create_raw_expr(T_REF_COLUMN, dest_column_ref)) ||
        OB_ISNULL(dest_column_ref)) {
      LOG_WARN("failed to allocate raw expr", K(dest_column_ref), K(ret));
    } else {
      dest = dest_column_ref;
    }
    break;
  }
  case ObRawExpr::EXPR_AGGR: {
    ObAggFunRawExpr *dest_agg = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_agg)) ||
        OB_ISNULL(dest_agg)) {
      LOG_WARN("failed to allocate raw expr", K(dest_agg), K(ret));
    } else {
      dest = dest_agg;
    }
    break;
  }
  case ObRawExpr::EXPR_CONST: {
    if (T_USER_VARIABLE_IDENTIFIER == expr_type) {
      ObUserVarIdentRawExpr *dest_var = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_var)) ||
          OB_ISNULL(dest_var)) {
        LOG_WARN("failed to allocate user var expr", K(ret));
      } else {
        dest = dest_var;
      }
    } else {
      ObConstRawExpr *dest_const = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_const)) ||
          OB_ISNULL(dest_const)) {
        LOG_WARN("failed to allocate raw expr", K(dest_const), K(ret));
      } else {
        dest = dest_const;
      }
    }
    break;
  }
  case ObRawExpr::EXPR_EXEC_PARAM: {
    ObExecParamRawExpr *dest_exec_expr = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_exec_expr)) ||
        OB_ISNULL(dest_exec_expr)) {
      LOG_WARN("failed to allocate new expr", K(dest_exec_expr), K(ret));
    } else {
      dest = dest_exec_expr;
    }
    break;
  }
  case ObRawExpr::EXPR_OPERATOR: {
    if (T_OBJ_ACCESS_REF == expr_type) {
      ObObjAccessRawExpr *dest_oa = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_oa))) {
        LOG_WARN("failed to allocate raw expr", K(ret));
      } else if (OB_ISNULL(dest_oa)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_oa), K(ret));
      } else {
        dest = dest_oa;
      }
    } else if (T_FUN_PL_ASSOCIATIVE_INDEX == expr_type) {
      ObPLAssocIndexRawExpr *dest_ai = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_ai))) {
        LOG_WARN("failed to allocate raw expr", K(ret));
      } else if (OB_ISNULL(dest_ai)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_ai), K(ret));
      } else {
        dest = dest_ai;
      }
    } else if (T_SP_CPARAM == expr_type) {
      ObCallParamRawExpr *dest_cp = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_cp))) {
        LOG_WARN("failed to allocate raw expr", K(ret));
      } else if (OB_ISNULL(dest_cp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_cp), K(ret));
      } else {
        dest = dest_cp;
      }
    } else if (T_FUN_PL_INTEGER_CHECKER == expr_type) {
      ObPLIntegerCheckerRawExpr *dest_oa = NULL;
      OZ (create_raw_expr(expr_type, dest_oa));
      CK (OB_NOT_NULL(dest_oa));
      OX (dest = dest_oa);
    } else {
      ObOpRawExpr *dest_op = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_op)) ||
          OB_ISNULL(dest_op)) {
        LOG_WARN("failed to allocate raw expr", K(dest_op), K(ret));
      } else {
        dest = dest_op;
      }
    }
    break;
  }
  case ObRawExpr::EXPR_CASE_OPERATOR: {
    ObCaseOpRawExpr *dest_case = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_case)) ||
        OB_ISNULL(dest_case)) {
      LOG_WARN("failed to allocate raw expr", K(dest_case), K(ret));
    } else {
      dest = dest_case;
    }
    break;
  }
  case ObRawExpr::EXPR_SYS_FUNC: {
    if (T_FUN_SYS_SEQ_NEXTVAL == expr_type) {
      ObSequenceRawExpr *dest_seq = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_seq))) {
        LOG_WARN("failed to allocate raw expr", K(dest_seq), K(ret));
      } else if (OB_ISNULL(dest_seq)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_seq), K(ret));
      } else {
        dest = dest_seq;
      }
    } else if (T_FUN_NORMAL_UDF == expr_type) {
      ObNormalDllUdfRawExpr *dest_nudf = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_nudf))) {
        LOG_WARN("failed to allocate raw expr", K(dest_nudf), K(ret));
      } else if (OB_ISNULL(dest_nudf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_nudf), K(ret));
      } else {
        dest = dest_nudf;
      }
    } else if (T_FUN_PL_COLLECTION_CONSTRUCT == expr_type) {
      ObCollectionConstructRawExpr *dest_cc = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_cc))) {
        LOG_WARN("failed to allocate raw expr", K(dest_cc), K(ret));
      } else if (OB_ISNULL(dest_cc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_cc), K(ret));
      } else {
        dest = dest_cc;
      }
    } else if (T_FUN_PL_OBJECT_CONSTRUCT == expr_type) {
      ObObjectConstructRawExpr *dest_oc = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_oc))) {
        LOG_WARN("failed to allocate raw expr", K(dest_oc), K(ret));
      } else if (OB_ISNULL(dest_oc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_oc), K(ret));
      } else {
        dest = dest_oc;
      }
    } else if (T_FUN_PL_GET_CURSOR_ATTR == expr_type) {
      ObPLGetCursorAttrRawExpr *dest_oa = NULL;
      OZ (create_raw_expr(expr_type, dest_oa));
      CK (OB_NOT_NULL(dest_oa));
      OX (dest = dest_oa);
    } else if (T_FUN_PL_SQLCODE_SQLERRM == expr_type) {
      ObPLSQLCodeSQLErrmRawExpr *dest_scse = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_scse))) {
        LOG_WARN("failed to allocate raw expr", K(dest_scse), K(ret));
      } else if (OB_ISNULL(dest_scse)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_scse), K(ret));
      } else {
        dest = dest_scse;
      }
    } else if (T_FUN_PLSQL_VARIABLE == expr_type) {
      ObPLSQLVariableRawExpr *dest_scse = nullptr;
      if (OB_FAIL(create_raw_expr(expr_type, dest_scse))) {
        LOG_WARN("failed to allocate raw expr", K(dest_scse), K(ret));
      } else if (OB_ISNULL(dest_scse)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_scse), K(ret));
      } else {
        dest = dest_scse;
      }
    } else if (T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT == expr_type) {
      ObUDTConstructorRawExpr *dest_udt_expr = nullptr;
      if (OB_FAIL(create_raw_expr(expr_type, dest_udt_expr))) {
        LOG_WARN("failed to allocate raw expr", K(dest_udt_expr), K(ret));
      } else if (OB_ISNULL(dest_udt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_udt_expr), K(ret));
      } else {
        dest = dest_udt_expr;
      }
    } else if (T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS == expr_type) {
      ObUDTAttributeAccessRawExpr *dest_udt_attr_expr = nullptr;
      if (OB_FAIL(create_raw_expr(expr_type, dest_udt_attr_expr))) {
        LOG_WARN("failed to allocate raw expr", K(dest_udt_attr_expr), K(ret));
      } else if (OB_ISNULL(dest_udt_attr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_udt_attr_expr), K(ret));
      } else {
        dest = dest_udt_attr_expr;
      }
    } else {
      ObSysFunRawExpr *dest_sys = NULL;
      if (OB_FAIL(create_raw_expr(expr_type, dest_sys))) {
        LOG_WARN("failed to allocate raw expr", K(dest_sys), K(ret));
      } else if (OB_ISNULL(dest_sys)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(dest_sys), K(ret));
      } else {
        dest = dest_sys;
      }
    }
    break;
  }
  case ObRawExpr::EXPR_UDF: {
    ObUDFRawExpr *dest_udf = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_udf))) {
      LOG_WARN("failed to allocate raw expr", K(dest_udf), K(ret));
    } else {
      dest = dest_udf;
    }
    break;
  }
  case ObRawExpr::EXPR_ALIAS_REF: {
    ObAliasRefRawExpr *dest_alias = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_alias))
        || OB_ISNULL(dest_alias)) {
      LOG_WARN("failed to allocate raw expr", K(dest_alias), K(ret));
    } else {
      dest = dest_alias;
    }
    break;
  }
  case ObRawExpr::EXPR_PSEUDO_COLUMN: {
    ObPseudoColumnRawExpr *dest_pseudo_column = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_pseudo_column))
        || OB_ISNULL(dest_pseudo_column)) {
      LOG_WARN("failed to alocate raw expr", K(ret));
    } else {
      dest = dest_pseudo_column;
    }
    break;
  }
  case ObRawExpr::EXPR_WINDOW: {
    ObWinFunRawExpr *dest_win_fun = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_win_fun))
        || OB_ISNULL(dest_win_fun)) {
      LOG_WARN("failed to allocate raw expr", K(dest_win_fun), K(ret));
    } else {
      dest = dest_win_fun;
    }
    break;
  }
  case ObRawExpr::EXPR_VAR: {
    ObVarRawExpr *dest_var_expr = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_var_expr))
        || OB_ISNULL(dest_var_expr)) {
      LOG_WARN("failed to allocate raw expr", K(dest_var_expr), K(ret));
    } else {
      dest = dest_var_expr;
    }
    break;
  }
  case ObRawExpr::EXPR_SET_OP: {
    ObSetOpRawExpr *dest_set_op_expr = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_set_op_expr))
        || OB_ISNULL(dest_set_op_expr)) {
      LOG_WARN("failed to allocate raw expr", K(dest_set_op_expr), K(ret));
    } else {
      dest = dest_set_op_expr;
    }
    break;
  }
  case ObRawExpr::EXPR_OP_PSEUDO_COLUMN: {
    ObOpPseudoColumnRawExpr *dest_op_pseduo_expr = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_op_pseduo_expr))
        || OB_ISNULL(dest_op_pseduo_expr)) {
      LOG_WARN("failed to allocate raw expr", K(dest_op_pseduo_expr), K(ret));
    } else {
      dest = dest_op_pseduo_expr;
    }
    break;
  }
  case ObRawExpr::EXPR_PL_QUERY_REF: {
    ObPlQueryRefRawExpr *dest_pl_query_ref = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_pl_query_ref)) ||
        OB_ISNULL(dest_pl_query_ref)) {
      LOG_WARN("failed to allocate raw expr", K(dest_pl_query_ref), K(ret));
    } else {
      dest = dest_pl_query_ref;
    }
    break;
  }
  case ObRawExpr::EXPR_MATCH_AGAINST: {
    ObMatchFunRawExpr *dest_match_against_expr = NULL;
    if (OB_FAIL(create_raw_expr(expr_type, dest_match_against_expr))
        || OB_ISNULL(dest_match_against_expr)) {
      LOG_WARN("failed to allocate raw expr", KPC(dest_match_against_expr), K(ret));
    } else {
      dest = dest_match_against_expr;
    }
    break;
  }
  case ObRawExpr::EXPR_INVALID_CLASS: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("does not implement expr type copy", K(ret), K(expr_type), K(expr_class));
    break;
  }
  }
  return ret;
}

int ObMatchFunRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObRawExpr::assign(other))) {
      LOG_WARN("copy in Base class ObRawExpr failed", K(ret));
    } else {
      const ObMatchFunRawExpr &tmp = static_cast<const ObMatchFunRawExpr &>(other);
      if (OB_FAIL(match_columns_.assign(tmp.match_columns_))) {
        LOG_WARN("faile to assign match columns", K(ret));
      } else {
        mode_flag_ = tmp.mode_flag_;
        search_key_ = tmp.search_key_;
      }
    }
  }
  return ret;
}

int ObMatchFunRawExpr::replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                                    const common::ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRawExpr::replace_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs,
                                                     new_exprs,
                                                     match_columns_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                    new_exprs,
                                                    search_key_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObMatchFunRawExpr::do_visit(ObRawExprVisitor &visitor)
{
  return visitor.visit(*this);
}

uint64_t ObMatchFunRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_value = seed;
  for (int64_t i = 0; i < get_param_count(); ++i) {
    if (NULL != get_param_expr(i)) {
      hash_value = do_hash(*get_param_expr(i), hash_value);
    }
  }
  hash_value = common::do_hash(mode_flag_, hash_value);
  return hash_value;
}

int ObMatchFunRawExpr::get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (OB_FAIL(BUF_PRINTF("MATCH("))) {
      LOG_WARN("fail to BUF_PRINTF", K(ret));
    } else {
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < get_match_columns().count() - 1; ++i) {
        if (OB_ISNULL(get_match_columns().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(get_match_columns().at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to get_name", K(i), K(ret));
        } else if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else {}
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(get_match_columns().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(get_match_columns().at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(") AGAINST("))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(get_search_key()->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (NATURAL_LANGUAGE_MODE == get_mode_flag() &&
                   OB_FAIL(BUF_PRINTF(""))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (BOOLEAN_MODE == get_mode_flag() &&
                   OB_FAIL(BUF_PRINTF(" IN BOOLEAN MODE"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION == get_mode_flag() &&
                   OB_FAIL(BUF_PRINTF(" IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (WITH_QUERY_EXPANSION == get_mode_flag() &&
                   OB_FAIL(BUF_PRINTF(" WITH QUERY EXPANSION"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(")"))) {
          LOG_WARN("fail to BUF_PRINTF", K(ret));
        } else if (EXPLAIN_EXTENDED == type) {
          if (OB_FAIL(BUF_PRINTF("("))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("%p", this))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          } else if (OB_FAIL(BUF_PRINTF(")"))) {
            LOG_WARN("fail to BUF_PRINTF", K(ret));
          } else {}
        }
      }
    }
  } else {
    // jinmao TODO: serialize oracle contains()
  }
  return ret;
}

bool ObMatchFunRawExpr::inner_same_as(const ObRawExpr &expr, ObExprEqualCheckContext *check_context) const
{
  bool bret = true;
  if (get_expr_type() != expr.get_expr_type()) {
    bret = false;
  } else {
    const ObMatchFunRawExpr *match_expr = static_cast<const ObMatchFunRawExpr*>(&expr);
    if (mode_flag_ != match_expr->mode_flag_ ||
        match_columns_.count() != match_expr->match_columns_.count()) {
      bret = false;
    } else if (OB_ISNULL(search_key_) || OB_ISNULL(match_expr->search_key_) ||
               !search_key_->same_as(*match_expr->search_key_, check_context)) {
      bret = false;
    }
    for (int64_t i = 0; bret && i < match_columns_.count(); i++) {
      if (OB_ISNULL(match_columns_.at(i)) || OB_ISNULL(match_expr->match_columns_.at(i)) ||
          !match_columns_.at(i)->same_as(*match_expr->match_columns_.at(i), check_context)) {
        bret = false;
      }
    }
  }
  return bret;
}

void ObMatchFunRawExpr::clear_child()
{
  match_columns_.reset();
  search_key_ = NULL;
  mode_flag_ = NATURAL_LANGUAGE_MODE;
}

void ObMatchFunRawExpr::reset()
{
  ObRawExpr::reset();
  clear_child();
}

int64_t ObMatchFunRawExpr::get_param_count() const
{
  return match_columns_.count() + 1 /*search key*/;
}

const ObRawExpr *ObMatchFunRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr *ptr_ret = NULL;
  if (0 <= index && index < match_columns_.count()) {
    ptr_ret = match_columns_.at(index);
  } else if (index == match_columns_.count()) {
    ptr_ret = search_key_;
  } else { /*do nothing*/ }
  return ptr_ret;
}

ObRawExpr *&ObMatchFunRawExpr::get_param_expr(int64_t index)
{
  if (0 <= index && index < match_columns_.count()) {
    return match_columns_.at(index);
  } else if (index == match_columns_.count()) {
    return search_key_;
  } else {
    return USELESS_POINTER;
  }
  return USELESS_POINTER;
}

int ObMatchFunRawExpr::get_table_id(uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  if (get_match_columns().count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_match_columns().count(); i++) {
      ObColumnRefRawExpr *match_col = NULL;
      if (OB_ISNULL(get_match_columns().at(i)) || !get_match_columns().at(i)->is_column_ref_expr()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FALSE_IT(match_col = static_cast<ObColumnRefRawExpr*>(get_match_columns().at(i)))) {
      } else if (table_id != OB_INVALID_ID && table_id != match_col->get_table_id()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else {
        table_id = match_col->get_table_id();
      }
    }
  }
  return ret;
}

int ObMatchFunRawExpr::get_match_column_type(ObExprResType &result_type)
{
  int ret = OB_SUCCESS;
  if (get_match_columns().count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected empty match column set", K(ret));
  } else if (OB_ISNULL(get_match_columns().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    result_type.assign(get_match_columns().at(0)->get_result_type());
  }
  return ret;
}

int ObMatchFunRawExpr::replace_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= get_param_count())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K(get_param_count()));
  } else if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null expr", K(ret));
  } else {
    ObRawExpr *&target_expr = get_param_expr(index);
    target_expr = expr;
  }
  return ret;
}

int ObUDTConstructorRawExpr::get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  CK (object_schema_version_ != OB_INVALID_VERSION);
  CK (udt_id_ != common::OB_INVALID_ID);
  OX (obj_version.object_id_ = udt_id_);
  OX (obj_version.object_type_ = share::schema::DEPENDENCY_TYPE);
  OX (obj_version.version_ = object_schema_version_);

  return ret;
}

int ObUDTConstructorRawExpr::set_access_names(
  const common::ObIArray<ObObjAccessIdent> &access_idents)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_idents.count(); ++i) {
    OZ (access_names_.push_back(access_idents.at(i).access_name_));
  }
  return ret;
}

int ObUDTConstructorRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObUDTConstructorRawExpr &tmp =
          static_cast<const ObUDTConstructorRawExpr &>(other);
      udt_id_ = tmp.udt_id_;
      root_udt_id_ = tmp.root_udt_id_;
      attr_pos_ = tmp.attr_pos_;
      object_schema_version_ = tmp.object_schema_version_;
      if (OB_FAIL(access_names_.assign(tmp.access_names_))) {
        LOG_WARN("failed to assgin access names", K(ret));
      }
    }
  }
  return ret;
}

int ObUDTConstructorRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  } else if (copier.deep_copy_attributes()) {
    CK (OB_NOT_NULL(inner_alloc_));
    for (int64_t i = 0; OB_SUCC(ret) && i < access_names_.count(); ++i) {
      OZ (ob_write_string(*inner_alloc_, access_names_.at(i), access_names_.at(i)));
    }
  }
  return ret;
}

ObExprOperator *ObUDTConstructorRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprUdtConstruct *object_op = NULL;
  if (T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make object construct operator failed", K(ret));
    }
    CK (OB_NOT_NULL(object_op = static_cast<ObExprUdtConstruct *>(expr_op)));
    OX (object_op->set_udt_id(udt_id_));
    OX (object_op->set_root_udt_id(root_udt_id_));
    OX (object_op->set_attribute_pos(attr_pos_));
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}


int ObUDTAttributeAccessRawExpr::get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  CK (object_schema_version_ != OB_INVALID_VERSION);
  CK (udt_id_ != common::OB_INVALID_ID);
  OX (obj_version.object_id_ = udt_id_);
  OX (obj_version.object_type_ = share::schema::DEPENDENCY_TYPE);
  OX (obj_version.version_ = object_schema_version_);

  return ret;
}

int ObUDTAttributeAccessRawExpr::assign(const ObRawExpr &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(get_expr_class() != other.get_expr_class() ||
                    get_expr_type() != other.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input expr", K(ret), K(other.get_expr_type()));
    } else if (OB_FAIL(ObSysFunRawExpr::assign(other))) {
      LOG_WARN("failed to assign expr", K(ret));
    } else {
      const ObUDTAttributeAccessRawExpr &tmp =
          static_cast<const ObUDTAttributeAccessRawExpr &>(other);
      udt_id_ = tmp.udt_id_;
      object_schema_version_ = tmp.object_schema_version_;
      attr_type_ = tmp.attr_type_;
    }
  }
  return ret;
}

int ObUDTAttributeAccessRawExpr::inner_deep_copy(ObIRawExprCopier &copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSysFunRawExpr::inner_deep_copy(copier))) {
    LOG_WARN("failed to copy expr attributes", K(ret));
  }
  return ret;
}

ObExprOperator *ObUDTAttributeAccessRawExpr::get_op()
{
  int ret = OB_SUCCESS;
  ObExprOperator *expr_op = NULL;
  ObExprUDTAttributeAccess *object_op = NULL;
  if (T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS == get_expr_type()) {
    free_op();
    if (OB_ISNULL(expr_op = ObOpRawExpr::get_op())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("make object construct operator failed", K(ret));
    }
    CK (OB_NOT_NULL(object_op = static_cast<ObExprUDTAttributeAccess *>(expr_op)));
    object_op->set_attribute_type(attr_type_);
    object_op->set_udt_id(udt_id_);
  }
  return OB_SUCCESS == ret ? expr_op : NULL;
}

}//namespace sql
}//namespace oceanbase
