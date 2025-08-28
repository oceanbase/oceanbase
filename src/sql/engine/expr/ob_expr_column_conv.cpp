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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

static bool is_string_text_cast(const ObExpr &expr)
{
  return lib::is_mysql_mode() && ob_is_text_tc(expr.datum_meta_.type_)
      && T_FUN_SYS_CAST == expr.args_[4]->type_ && ob_is_string_tc(expr.args_[4]->args_[0]->datum_meta_.type_);
}

ObFastColumnConvExpr::ObFastColumnConvExpr(ObIAllocator &alloc)
    : ObBaseExprColumnConv(alloc),
      ObFastExprOperator(T_FUN_COLUMN_CONV),
      column_type_(),
      value_item_(),
      column_info_()
{
}

int ObFastColumnConvExpr::assign(const ObFastExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other.get_op_type() != T_FUN_COLUMN_CONV)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(other.get_op_type()));
  } else {
    const ObFastColumnConvExpr &other_conv = static_cast<const ObFastColumnConvExpr&>(other);
    column_type_ = other_conv.column_type_;
    value_item_ = other_conv.value_item_;
    if (OB_FAIL(ob_write_string(alloc_, other_conv.column_info_, column_info_))) {
      LOG_WARN("fail to set column info", K(ret));
    }
  }
  return ret;
}

int ObFastColumnConvExpr::calc(ObExprCtx &expr_ctx, const ObNewRow &row, ObObj &result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(expr_ctx.my_session_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr context is invalid", K(ret), K(expr_ctx.my_session_), K(expr_ctx.phy_plan_ctx_));
  } else {
    bool is_strict = is_strict_mode(expr_ctx.my_session_->get_sql_mode());
    const ObObj *value = NULL;
    const ObString *column_info = NULL;
    if (has_column_info()) {
      column_info = &column_info_;
    }
    if (OB_FAIL(value_item_.get_item_value_directly(*expr_ctx.phy_plan_ctx_, row, value))) {
      LOG_WARN("get item value directly from value item failed", K(ret), K_(value_item));
    } else if (OB_FAIL(ObSqlExpressionUtil::expand_array_params(expr_ctx, *value, value))) {
      LOG_WARN("expand array params failed", K(ret));
    } else if (OB_FAIL(ObExprColumnConv::convert_skip_null_check(result, *value, column_type_,
                                         is_strict, expr_ctx.column_conv_ctx_,
                                         &str_values_, column_info))) {
      LOG_WARN("convert column value failed", K(ret), K(*this));
    }
  }
  return ret;
}

int ObFastColumnConvExpr::set_const_value(const ObObj &value)
{
  int ret = OB_SUCCESS;
  ObObj tmp;
  if (OB_FAIL(ob_write_obj(alloc_, value, tmp))) {
    LOG_WARN("write const value failed", K(ret));
  } else if (OB_FAIL(value_item_.assign(tmp))) {
    LOG_WARN("write value item failed", K(ret));
  }
  return ret;
}
//由于在14x的版本中ObExprColumnConv使用的是父类ObExprOperator的序列化函数
//为了保证兼容性，这里先序列化父类ObExprOperator的成员，再序列化自身的成员
OB_SERIALIZE_MEMBER(ObExprColumnConv, row_dimension_, real_param_num_, result_type_, input_types_, id_, str_values_);

ObExprColumnConv::ObExprColumnConv(ObIAllocator &alloc)
    : ObBaseExprColumnConv(alloc),
      ObFuncExprOperator(alloc, T_FUN_COLUMN_CONV, N_COLUMN_CONV, -1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
  disable_operand_auto_cast();
  //obexprcolumnconv有自己特殊处理，不走字符集自动转换框架
  need_charset_convert_ = false;
}

ObExprColumnConv::~ObExprColumnConv()
{
}

int ObExprColumnConv::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprColumnConv *tmp_other = dynamic_cast<const ObExprColumnConv *>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObFuncExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObFuncExprOperator failed", K(ret));
    } else if (tmp_other->str_values_.count() > 0 && 
               OB_FAIL(deep_copy_str_values(tmp_other->str_values_))) {
      LOG_WARN("copy str_values failed", K(ret));
    }
  }
  return ret;
}

int ObExprColumnConv::convert_with_null_check(ObObj &result,
                                              const ObObj &obj,
                                              const ObExprResType &res_type,
                                              bool is_strict,
                                              ObCastCtx &cast_ctx,
                                              const ObIArray<ObString> *type_infos)
{
  int ret = OB_SUCCESS;
  bool is_not_null = res_type.is_not_null_for_write();
  if (OB_FAIL(ObExprColumnConv::convert_skip_null_check(result, obj, res_type, is_strict,
                                                        cast_ctx, type_infos))) {
    LOG_WARN("fail to convert skip null check", K(ret));
  } else if (is_not_null && (result.is_null() || (lib::is_oracle_mode() && result.is_null_oracle()))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("Column should not be null", K(ret));
  }
  return ret;
}

int ObExprColumnConv::convert_skip_null_check(ObObj &result,
                                              const ObObj &obj,
                                              const ObExprResType &res_type,
                                              bool is_strict,
                                              ObCastCtx &cast_ctx,
                                              const ObIArray<ObString> *type_infos,
                                              const ObString *column_info)
{
  int ret = OB_SUCCESS;
  ObObjType type = res_type.get_type();
  const ObAccuracy &accuracy = res_type.get_accuracy();
  ObCollationType collation_type = res_type.get_collation_type();
  const ObObj *res_obj = NULL;
  cast_ctx.expect_obj_collation_ = collation_type;
  cast_ctx.dest_collation_ = collation_type;

  ObAccuracy tmp_accuracy = accuracy;
  cast_ctx.res_accuracy_ = &tmp_accuracy;
  if (OB_UNLIKELY(ob_is_enum_or_set_type(type))) {
    ObExpectType expect_type;
    expect_type.set_type(type);
    expect_type.set_collation_type(collation_type);
    expect_type.set_type_infos(type_infos);
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, obj, result))) {
      LOG_WARN("fail to cast to enum or set", K(obj), K(expect_type), K(ret));
    } else {
      res_obj = &result;
    }
  } else if (OB_FAIL(ObObjCaster::to_type(type, cast_ctx, obj, result, res_obj)) || OB_ISNULL(res_obj)) {
    ret = COVER_SUCC(OB_ERR_UNEXPECTED);
    LOG_WARN("failed to cast object", K(ret), K(obj), "type", type);
  } else {
    LOG_DEBUG("succ to to_type", K(type), K(obj), K(result), K(collation_type));
  }
  if (OB_SUCC(ret)) {
    const int64_t max_accuracy_len = static_cast<int64_t>(res_type.
                                                          get_accuracy().get_length());
    const int64_t str_len_byte = static_cast<int64_t>(result.get_string_len());
    if (OB_FAIL(obj_collation_check(is_strict, collation_type, *const_cast<ObObj*>(res_obj)))) {
      LOG_WARN("failed to check collation", K(ret), K(collation_type), KPC(res_obj));
    } else if (OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, collation_type, *res_obj, result, res_obj))) {
      LOG_WARN("failed to check accuracy", K(ret), K(accuracy), K(collation_type), KPC(res_obj));
      if (OB_ERR_DATA_TOO_LONG == ret && lib::is_oracle_mode()
          && OB_NOT_NULL(column_info) && !column_info->empty()) {
        int64_t col_length = str_len_byte;
        if (ObStringTC == ob_obj_type_class(type)
            && !is_oracle_byte_length(lib::is_oracle_mode(), accuracy.get_length_semantics())) {
          col_length = ObCharset::strlen_char(collation_type, result.get_string().ptr(), str_len_byte);
        }
        LOG_ORACLE_USER_ERROR(OB_ERR_DATA_TOO_LONG_MSG_FMT_V2, column_info->length(),
                              column_info->ptr(), col_length, max_accuracy_len);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (res_obj != &result) {
      //res_obj没有指向result的地址，需要将res_obj的值拷贝到result中
      result = *res_obj;
    }
//    if (is_not_null && (result.is_null() || (is_oracle_mode() && result.is_null_oracle()))) {
//      ret = OB_BAD_NULL_ERROR;
//      LOG_WARN("Column should not be null", K(ret));
//    }
  }
  return ret;
}

int ObExprColumnConv::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = common::OB_SUCCESS;
  //objs[0]  type
  //objs[1] collation_type
  //objs[2] accuray_expr
  //objs[3] nullable_expr
  //objs[4] value
  //objs[5] column_info
  ObCollationType coll_type = CS_TYPE_INVALID;
  if (OB_ISNULL(type_ctx.get_session()) || OB_ISNULL(type_ctx.get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or raw expr is NULL", K(ret));
  } else if (OB_UNLIKELY(((PARAMS_COUNT_WITHOUT_COLUMN_INFO != param_num)
                        && (PARAMS_COUNT_WITH_COLUMN_INFO != param_num))
                        || OB_ISNULL(types))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument, param_num should be 5 or 6",
               K(param_num), K(types), K(ret));
  } else if (OB_FAIL(type_ctx.get_session()->get_collation_connection(coll_type))) {
    SQL_ENG_LOG(WARN, "fail to get_collation_connection", K(coll_type), K(ret));
  } else {
    type.set_type(types[0].get_type());
    type.set_collation_type(types[1].get_collation_type());
    // set collation level
    ObCollationLevel coll_level = ObRawExprUtils::get_column_collation_level(types[0].get_type());
    type.set_collation_level(coll_level);
    type.set_accuracy(types[2].get_accuracy());
    if (type.get_type() == ObUserDefinedSQLType) {
      uint64_t udt_id = types[2].get_accuracy().get_accuracy();
      uint16_t subschema_id = ObMaxSystemUDTSqlType;
      // need const cast to modify subschema ctx, in physcial plan ctx belong to cur exec_ctx;
      ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
      ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
      if (udt_id == T_OBJ_XML) {
        subschema_id = ObXMLSqlType;
      } else if (OB_ISNULL(exec_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("need context to search subschema mapping", K(ret), K(udt_id));
      } else if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id, subschema_id))) {
        LOG_WARN("failed to get sub schema id", K(ret), K(udt_id));
      }
      if (OB_SUCC(ret)) {
        type.set_subschema_id(subschema_id);
      }
    }
    if (ob_is_enumset_tc(type.get_type()) || ob_is_collection_sql_type(type.get_type())) {
      type.set_subschema_id(types[0].get_subschema_id());
      if (ob_is_enumset_tc(type.get_type())) {
        type.mark_enum_set_with_subschema(types[0].get_enum_set_subschema_state());
      }
    }
    if (types[3].is_not_null_for_read()) {
      type.set_result_flag(NOT_NULL_FLAG | NOT_NULL_WRITE_FLAG);
    }

    bool wrap_to_str = false;
    if (OB_SUCC(ret) && OB_FAIL(calc_enum_set_result_type(type, types, coll_type, type_ctx,
                                                          wrap_to_str))) {
      LOG_WARN("fail to calc enum set result type", K(ret));
    }

    // for table modify in oracle mode, we ignore charset convert failed
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_CHARSET_CONVERT_IGNORE_ERR);
    }

    if (OB_SUCC(ret) &&
        OB_FAIL(ObCharset::check_valid_implicit_convert(types[4].get_collation_type(),
                                                        types[1].get_collation_type()))) {
      LOG_WARN("failed to check valid implicit convert", K(ret));
    }

    if (OB_SUCC(ret) && !wrap_to_str) {
      //cast type when type not same.
      const ObObjTypeClass value_tc = ob_obj_type_class(types[4].get_type());
      const ObObjTypeClass type_tc = ob_obj_type_class(types[0].get_type());
      ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
      bool is_prepare_stage = session->is_pl_prepare_stage();
      if (lib::is_oracle_mode()
          && OB_UNLIKELY(!cast_supported(types[4].get_type(), types[4].get_collation_type(),
                                         types[0].get_type(), types[1].get_collation_type()))
          && !(is_prepare_stage && type_tc == ObGeometryTC)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes", "expected", type_tc, "got", value_tc);
      } else {
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() |
                               type_ctx.get_raw_expr()->get_cast_mode() |
                               CM_COLUMN_CONVERT);
        types[4].set_calc_meta(type);
        if (ob_is_number_or_decimal_int_tc(type.get_type())) {
          // decimal/number int need to setup calc accuracy
          types[4].set_calc_accuracy(type.get_accuracy());
          type.set_collation_level(common::CS_LEVEL_NUMERIC);
        } else if (ob_is_user_defined_type(type.get_type())
            || ob_is_collection_sql_type(type.get_type())) { // if calc meta is udt, set calc udt id
          types[4].set_calc_accuracy(type.get_accuracy());
        } else if (lib::is_mysql_mode() &&
            ob_is_double_tc(types[4].get_type()) && ob_is_string_tc(type.get_type())) {
          types[4].set_calc_accuracy(type.get_accuracy());
        }
      }
    }
    LOG_DEBUG("finish calc_result_typeN", K(type), K(types[4]), K(types[0]), K(wrap_to_str));
  }
  return ret;
}

int ObExprColumnConv::calc_enum_set_result_type(ObExprResType &type,
                                                ObExprResType *types,
                                                ObCollationType coll_type,
                                                ObExprTypeCtx &type_ctx,
                                                bool &wrap_to_str) const
{
  int ret = OB_SUCCESS;
  // here will wrap type_to_str if necessary
  // for enum set type with subschema, it can directly execute any type of cast,
  // so there is no need to wrap type_to_str.
  if (ob_is_enumset_tc(types[4].get_type())) {
    ObObjType calc_type = get_enumset_calc_type(types[0].get_type(), 4);
    // When the types are inconsistent or it doesn't support enum/set type with subschema,
    // new cast expression is required.
    const bool support_enum_set_type_subschema = is_enum_set_with_subschema_arg(4);
    bool need_add_cast = type.get_type() != types[4].get_type() || !support_enum_set_type_subschema;
    // keep old behavior use session collation
    coll_type = support_enum_set_type_subschema ? types[1].get_collation_type() : coll_type;
    if (OB_UNLIKELY(ObMaxType == calc_type)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid type of parameter ", K(types[4]), K(types), K(ret));
    } else if (ob_is_string_type(calc_type) && need_add_cast) {
      wrap_to_str = true;
    } else if (!need_add_cast && ob_is_enum_or_set_type(type.get_type())) {
      wrap_to_str = true; // set wrap to str to true first
      // the src and dst types are the same, and both are enum/set. we need to check the
      // subschema id of the expr result type.
      const ObRawExpr *conv_expr = get_raw_expr();
      const ObRawExpr *enumset_expr = NULL;
      if (OB_ISNULL(conv_expr) || OB_ISNULL(enumset_expr = conv_expr->get_param_expr(4))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr or child expr is null", K(ret), KP(conv_expr));
      } else {
        // set wrap to str to false, it will be checked in `ObRawExprWrapEnumSet`
        wrap_to_str = type.get_subschema_id() != enumset_expr->get_subschema_id();
      }
    }
    if (OB_SUCC(ret)) {
      if (wrap_to_str) {
        types[4].set_calc_type(calc_type);
        types[4].set_calc_collation_type(coll_type);
        types[4].set_calc_collation_level(CS_LEVEL_IMPLICIT);
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | type_ctx.get_raw_expr()->get_cast_mode());
      }
    }
  }
  return ret;
}

int ObExprColumnConv::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  //compatible with old code
  CK((PARAMS_COUNT_WITHOUT_COLUMN_INFO == rt_expr.arg_cnt_)
    || (PARAMS_COUNT_WITH_COLUMN_INFO == rt_expr.arg_cnt_));
  if (OB_ISNULL(op_cg_ctx.session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (OB_FAIL(ObEnumSetInfo::init_enum_set_info(op_cg_ctx.allocator_, rt_expr, type_,
      raw_expr.get_cast_mode(), str_values_))) {
    LOG_WARN("fail to init_enum_set_info", K(ret), K(type_), K(str_values_));
  } else {
    ObEnumSetInfo *enumset_info = static_cast<ObEnumSetInfo *>(rt_expr.extra_info_);
    if (op_cg_ctx.session_->is_ignore_stmt()) {
      enumset_info->cast_mode_ = enumset_info->cast_mode_ | CM_WARN_ON_FAIL
                                | CM_CHARSET_CONVERT_IGNORE_ERR;
    }
    enumset_info->cast_mode_ |= CM_COLUMN_CONVERT;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0
        && (enumset_info->cast_mode_ & CM_FAST_COLUMN_CONV)
        && !ob_is_enum_or_set_type(rt_expr.datum_meta_.type_)) {
      rt_expr.eval_func_ = column_convert_fast;
      if (rt_expr.args_[4]->is_batch_result()) {
        rt_expr.eval_batch_func_ = column_convert_batch_fast;
        rt_expr.eval_vector_func_ = column_convert_vector_fast;
      }
    } else {
      rt_expr.eval_func_ = column_convert;
      if (rt_expr.args_[4]->is_batch_result()
          && !ob_is_enum_or_set_type(rt_expr.datum_meta_.type_)
          && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0) {
        if (!is_lob_storage(rt_expr.datum_meta_.type_)
            || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_0) {
          rt_expr.eval_batch_func_ = column_convert_batch;
          if (!is_lob_storage(rt_expr.datum_meta_.type_) || is_string_text_cast(rt_expr)) {
            rt_expr.eval_vector_func_ = column_convert_vector;
          }
        }
      }
    }
  }
  return ret;
}

static OB_INLINE int column_convert_datum_accuracy_check(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      bool has_lob_header,
                                                      ObDatum &datum,
                                                      const uint64_t &cast_mode,
                                                      ObDatum &datum_for_check)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  const int64_t max_accuracy_len = static_cast<int64_t>(expr.max_length_);
  const int64_t str_len_byte = static_cast<int64_t>(datum_for_check.len_);
  if (OB_FAIL(datum_accuracy_check(expr,
                                   cast_mode,
                                   ctx,
                                   has_lob_header,
                                   datum_for_check,
                                   datum,
                                   warning))) {
    LOG_WARN("fail to check accuracy", K(ret), K(expr), K(warning));
    //compatible with old code
    if (OB_ERR_DATA_TOO_LONG == ret && lib::is_oracle_mode()
        && ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr.arg_cnt_) {
      ObString column_info_str;
      ObDatum *column_info = NULL;
      if (OB_FAIL(expr.args_[5]->eval(ctx, column_info))) {
        LOG_WARN("evaluate parameter failed", K(ret));
      } else {
        column_info_str = column_info->get_string();
        int64_t col_length = str_len_byte;
        if (ObStringTC == ob_obj_type_class(expr.datum_meta_.get_type())
            && !is_oracle_byte_length(lib::is_oracle_mode(), expr.datum_meta_.length_semantics_)) {
          col_length = ObCharset::strlen_char(expr.datum_meta_.cs_type_, datum_for_check.ptr_, str_len_byte);
        }
        LOG_ORACLE_USER_ERROR(OB_ERR_DATA_TOO_LONG_MSG_FMT_V2, column_info_str.length(),
                              column_info_str.ptr(), col_length, max_accuracy_len);
      }
      ret = OB_ERR_DATA_TOO_LONG;
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode() && OB_ERR_DATA_TOO_LONG == warning) {
    ObDatum *column_info = NULL;
    int64_t rownum = ctx.exec_ctx_.get_cur_rownum();
    if (rownum > 0
        && ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr.arg_cnt_
        && OB_SUCCESS == expr.args_[5]->eval(ctx, column_info)) {
      LOG_USER_WARN(OB_ERR_DATA_TRUNCATED, column_info->get_string().length(),
                                           column_info->get_string().ptr(), rownum);
    }
  }
  return ret;
}

static OB_INLINE int column_convert_vector_accuracy_check(const ObExpr &expr,
                                                          ObEvalCtx &ctx,
                                                          const ObBitVector &skip,
                                                          bool has_lob_header,
                                                          const uint64_t &cast_mode,
                                                          const int64_t idx,
                                                          const ObIVector &in_vec,
                                                          ObIVector &out_vec)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  const int64_t max_accuracy_len = static_cast<int64_t>(expr.max_length_);
  const int64_t str_len_byte = static_cast<int64_t>(in_vec.get_length(idx));
  if (OB_FAIL(vector_accuracy_check(expr,
                                   cast_mode,
                                   ctx,
                                   has_lob_header,
                                   idx,
                                   in_vec,
                                   out_vec,
                                   warning))) {
    LOG_WARN("fail to check accuracy", K(ret), K(expr), K(warning));
    //compatible with old code
    if (OB_ERR_DATA_TOO_LONG == ret && lib::is_oracle_mode()
        && ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr.arg_cnt_) {
      ObString column_info_str;
      ObDatum *column_info = NULL;
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_size(ctx.max_batch_size_);
      batch_info_guard.set_batch_idx(idx);
      EvalBound bound(ctx.max_batch_size_, idx, idx + 1, false);
      if (OB_FAIL(expr.args_[5]->eval_vector(ctx, skip, bound))) {
        LOG_WARN("evaluate parameter failed", K(ret));
      } else {
        column_info_str = expr.args_[5]->get_vector(ctx)->get_string(idx);
        int64_t col_length = str_len_byte;
        if (ObStringTC == ob_obj_type_class(expr.datum_meta_.get_type())
            && !is_oracle_byte_length(lib::is_oracle_mode(), expr.datum_meta_.length_semantics_)) {
          col_length = ObCharset::strlen_char(expr.datum_meta_.cs_type_, in_vec.get_payload(idx), str_len_byte);
        }
        LOG_ORACLE_USER_ERROR(OB_ERR_DATA_TOO_LONG_MSG_FMT_V2, column_info_str.length(),
                              column_info_str.ptr(), col_length, max_accuracy_len);
      }
      ret = OB_ERR_DATA_TOO_LONG;
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode() && OB_ERR_DATA_TOO_LONG == warning) {
    ObDatum *column_info = NULL;
    int64_t rownum = ctx.exec_ctx_.get_cur_rownum();
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(ctx.max_batch_size_);
    batch_info_guard.set_batch_idx(idx);
    EvalBound bound(ctx.max_batch_size_, idx, idx + 1, false);
    if (rownum > 0
        && ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr.arg_cnt_
        && OB_SUCCESS == expr.args_[5]->eval_vector(ctx, skip, bound)) {
      LOG_USER_WARN(OB_ERR_DATA_TRUNCATED, expr.args_[5]->get_vector(ctx)->get_string(idx).length(),
                                           expr.args_[5]->get_vector(ctx)->get_string(idx).ptr(), rownum);
    }
  }
  return ret;
}

int enum_set_valid_check(const uint64_t val, const int64_t str_values_count, const bool is_enum)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(str_values_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str values count", K(ret), K(str_values_count), K(is_enum));
  } else if (is_enum && (val > str_values_count)) {
    // ENUM type, its value should not exceed str_values_count
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected enum value", K(ret), K(val), K(str_values_count));
  } else if (!is_enum && (str_values_count < OB_MAX_SET_ELEMENT_NUM) &&
      (val >= (1UL << str_values_count))) {
    // SET type, its value should not be greater than or equal to 2^(str_values_count)
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set value", K(ret), K(val), K(str_values_count));
  }
  return ret;
}

int ObExprColumnConv::column_convert(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra_info_ is unexpected", K(ret), KP(expr.extra_info_));
  } else {
    const ObEnumSetInfo *enumset_info = static_cast<ObEnumSetInfo *>(expr.extra_info_);
    const uint64_t cast_mode = enumset_info->cast_mode_;
    bool is_strict = CM_IS_STRICT_MODE(cast_mode) && !CM_IS_IGNORE_CHARSET_CONVERT_ERR(cast_mode);
    ObObjType out_type = expr.datum_meta_.type_;
    ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
    ObDatum *val = NULL;
    if (ob_is_enum_or_set_type(out_type) && !expr.args_[4]->obj_meta_.is_enum_or_set()) {
      if (OB_FAIL(eval_enumset(expr, ctx, val))) {
        LOG_WARN("fail to eval enumset result", K(ret));
      }
    } else {
      if (OB_FAIL(expr.args_[4]->eval(ctx, val))) {
        LOG_WARN("evaluate parameter failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (val->is_null()) {
      datum.set_null();
    } else {
      // cast is done implictly if needed, before call this function
      // 1. cs type validation for string types
      // 2. accuracy check and modification
      // 3. handle delta lob
      if (!is_lob_storage(out_type)) {
        if (ob_is_string_type(out_type)) { // in type must not be lob type
          ObString str = val->get_string();
          if (OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
            LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
          } else {
            val->set_string(str);
          }
        } else if (ob_is_enum_or_set_type(out_type)) {
          if (OB_FAIL(enum_set_valid_check(val->get_uint64(), enumset_info->str_values_.count(),
                                           (expr.datum_meta_.type_ == ObEnumType)))) {
            LOG_WARN("enum set val is invalid", K(ret), K(val->get_uint64()),
                                                K(enumset_info->str_values_.count()), K(expr));
          }
        }
        if (OB_SUCC(ret)
            && OB_FAIL(column_convert_datum_accuracy_check(expr, ctx, false, datum, cast_mode, *val))) {
          LOG_WARN("fail do datum_accuracy_check for lob res", K(ret), K(expr), K(*val));
        }
        if (OB_SUCC(ret)) {
          LOG_DEBUG("after column convert", K(expr), K(datum), K(cast_mode));
        }
      } else if (is_lob_storage(out_type) && expr.args_[4]->obj_meta_.is_user_defined_sql_type()) {
        // udt types can only insert to lob columns by rewrite.
        // but before rewrite, column convert type deducing may happen
        // so prevent the convertion during execution
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, ob_obj_type_str(out_type), "ANYDATA");
        LOG_WARN("convert xmltype to character type is not supported in PL",
                 K(ret), K(expr.args_[4]->obj_meta_), K(out_type));
      } else {
        ObObjType in_type = expr.args_[4]->obj_meta_.get_type();
        ObCollationType in_cs_type = expr.args_[4]->obj_meta_.get_collation_type();
        bool has_lob_header = expr.args_[4]->obj_meta_.has_lob_header();
        bool has_lob_header_for_check = has_lob_header;
        ObString raw_str = val->get_string();
        ObLobLocatorV2 input_lob(raw_str.ptr(), raw_str.length(), has_lob_header);
        bool is_delta = input_lob.is_valid() && input_lob.is_delta_temp_lob();
        if (is_delta) { // delta lob
          if (!(ob_is_text_tc(in_type) || ob_is_json(in_type))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("delta lob can not convert to non-text type", K(ret), K(out_type));
          } else {
            datum.set_string(raw_str);
          }
        } else {
          ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx); // temp alloc only used for lob types
          common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
          ObDatum datum_for_check = *val;
          ObString str;
          ObTextStringIter striter(in_type, in_cs_type, val->get_string(), has_lob_header);
          if (OB_FAIL(striter.init(0, ctx.exec_ctx_.get_my_session(), &temp_allocator))) {
            LOG_WARN("fail to init string iter", K(ret), K(is_strict), K(expr));
          } else if (OB_FAIL(striter.get_full_data(str))) {
            LOG_WARN("fail to get full data from string iter", K(ret), K(is_strict), K(expr));
          } else if (ob_is_geometry(out_type)) {
            ObGeoType geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
            if (OB_FAIL(ObGeoTypeUtil::check_geo_type(geo_type, str))) {
              LOG_WARN("fail to check geo type", K(ret), K(str), K(geo_type), K(expr));
              ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
              LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
            }
          } else if (OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
            LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
          }
          if (OB_SUCC(ret)) {
            has_lob_header_for_check = false; // datum_for_check must have no lob header
            datum_for_check.set_string(str);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(column_convert_datum_accuracy_check(expr, ctx, has_lob_header_for_check, datum,
                                                                 cast_mode, datum_for_check))) {
            LOG_WARN("fail do datum_accuracy_check for lob res", K(ret), K(expr), K(datum_for_check));
          } else {
            // in type is the same with out type, if length changed, build a new lob
            ObLobLocatorV2 loc(raw_str, has_lob_header);
            int64_t old_data_byte_len = 0;
            if (raw_str.length() > 0) {
              if (OB_FAIL(loc.get_lob_data_byte_len(old_data_byte_len))) {
                LOG_WARN("Lob: failed to get data byte len", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              int64_t new_data_byte_len = datum.get_string().length();
              if (new_data_byte_len == old_data_byte_len) {
                datum.set_string(raw_str);
              } else {
                ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &datum);
                if (OB_FAIL(str_result.init(new_data_byte_len))) {
                  LOG_WARN("Lob: init lob result failed", K(ret));
                } else if (OB_FAIL(str_result.append(datum.get_string().ptr(), new_data_byte_len))) {
                  LOG_WARN("Lob: append lob result failed", K(ret));
                } else {
                  str_result.set_result();
                }
              }
            }
          }
          LOG_DEBUG("after column convert", K(expr), K(datum), K(cast_mode));
        }
      }
    }
  }

  return ret;
}

int ObExprColumnConv::column_convert_fast(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDatum *val = nullptr;
  if (OB_FAIL(expr.args_[4]->eval(ctx, val))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else {
    datum.set_datum(*val);
  }
  return ret;
}

int ObExprColumnConv::column_convert_batch(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           const ObBitVector &skip,
                                           const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  const ObEnumSetInfo *enumset_info = static_cast<ObEnumSetInfo *>(expr.extra_info_);
  const uint64_t cast_mode = enumset_info->cast_mode_;
  bool is_strict = CM_IS_STRICT_MODE(cast_mode) && !CM_IS_IGNORE_CHARSET_CONVERT_ERR(cast_mode);
  ObEvalInfo &eval_info = expr.args_[4]->get_eval_info(ctx);
  bool param_not_eval = !expr.args_[4]->get_eval_info(ctx).evaluated_
                        && !expr.args_[4]->get_eval_info(ctx).projected_;
  if (OB_FAIL(expr.args_[4]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch vals", K(ret));
  } else {
    ObDatum *vals = expr.args_[4]->locate_batch_datums(ctx);
    ObDatum *results = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    bool is_string_type = ob_is_string_type(out_type);
    bool is_string2text = is_string_text_cast(expr);
    bool is_int_tc = ob_is_int_uint_tc(out_type);
    bool is_decimal_int_tc = ob_is_decimal_int_tc(out_type);
    ObAccuracy accuracy;
    accuracy.set_length(expr.max_length_);
    accuracy.set_scale(expr.datum_meta_.scale_);
    const ObObjTypeClass &dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
    const ObLength max_accuracy_len = accuracy.get_length();
    if (is_string_type) {
      accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
    }
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    if (!is_lob_storage(out_type) || is_string2text) {
      if (is_string2text
          && OB_SUCCESS != (ret = inner_loop_for_convert_batch<PARAM_TC::TEXT_TC>(expr, ctx, skip, batch_size,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags, vals, results,
                                                                             batch_info_guard))) {
      } else if (is_string_type
          && OB_SUCCESS != (ret = inner_loop_for_convert_batch<PARAM_TC::STRING_TC>(expr, ctx, skip, batch_size,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags, vals, results,
                                                                             batch_info_guard))) {
        LOG_WARN("failed to convert batch", K(ret));
      } else if (is_int_tc
                 && OB_SUCCESS != (ret = inner_loop_for_convert_batch<PARAM_TC::INT_TC>(expr, ctx, skip, batch_size,
                                                                                 is_strict, max_accuracy_len, cast_mode,
                                                                                 eval_flags, vals, results,
                                                                                 batch_info_guard))) {
        LOG_WARN("failed to convert batch", K(ret));
      } else if (is_decimal_int_tc
                 && OB_SUCCESS != (ret = inner_loop_for_convert_batch<PARAM_TC::DECIMAL_INT_TC>(expr, ctx, skip, batch_size,
                                                                                         is_strict, max_accuracy_len, cast_mode,
                                                                                         eval_flags, vals, results,
                                                                                         batch_info_guard))) {
        LOG_WARN("failed to convert batch", K(ret));
      } else if (OB_SUCCESS != (ret = inner_loop_for_convert_batch<PARAM_TC::OTHER_TC>(expr, ctx, skip, batch_size,
                                                                   is_strict, max_accuracy_len, cast_mode,
                                                                   eval_flags, vals, results,
                                                                   batch_info_guard))) {
        LOG_WARN("failed to convert batch", K(ret));
      }
    } else if (expr.args_[4]->obj_meta_.is_user_defined_sql_type()) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, ob_obj_type_str(out_type), "ANYDATA");
      LOG_WARN("convert xmltype to character type is not supported in PL",
                K(ret), K(expr.args_[4]->obj_meta_), K(out_type));
    } else {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx); // temp alloc only used for lob types
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObObjType in_type = expr.args_[4]->obj_meta_.get_type();
      ObCollationType in_cs_type = expr.args_[4]->obj_meta_.get_collation_type();
      bool has_lob_header = expr.args_[4]->obj_meta_.has_lob_header();
      bool has_lob_header_for_check = has_lob_header;
      bool can_use_raw_str = T_FUN_SYS_CAST == expr.args_[4]->type_
                             && param_not_eval
                             && ob_is_string_tc(expr.args_[4]->args_[0]->datum_meta_.type_)
                             && !ob_is_geometry(out_type);
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        }
        if (vals[i].is_null()) {
          results[i].set_null();
        } else {
          batch_info_guard.set_batch_idx(i);
          ObString raw_str = vals[i].get_string();
          ObLobLocatorV2 input_lob(raw_str.ptr(), raw_str.length(), has_lob_header);
          bool is_delta = input_lob.is_valid() && input_lob.is_delta_temp_lob();
          if (is_delta) { // delta lob
            if (!(ob_is_text_tc(in_type) || ob_is_json(in_type))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("delta lob can not convert to non-text type", K(ret), K(out_type));
            } else {
              results[i].set_string(raw_str);
            }
          } else {
            ObDatum datum_for_check = vals[i];
            ObString str;
            ObTextStringIter striter(in_type, in_cs_type, vals[i].get_string(), has_lob_header);
            if (can_use_raw_str) {
              str = expr.args_[4]->args_[0]->locate_expr_datum(ctx).get_string();
              if (str.length() >= max_accuracy_len) {
                can_use_raw_str = false;
              }
            }
            if (!can_use_raw_str) {
              if (OB_FAIL(striter.init(0, ctx.exec_ctx_.get_my_session(), &temp_allocator))) {
                LOG_WARN("fail to init string iter", K(ret), K(is_strict), K(expr));
              } else if (OB_FAIL(striter.get_full_data(str))) {
                LOG_WARN("fail to get full data from string iter", K(ret), K(is_strict), K(expr));
              } else if (ob_is_geometry(out_type)) {
                ObGeoType geo_type = ObGeoCastUtils::get_geo_type_from_cast_mode(cast_mode);
                if (OB_FAIL(ObGeoTypeUtil::check_geo_type(geo_type, str))) {
                  LOG_WARN("fail to check geo type", K(ret), K(str), K(geo_type), K(expr));
                  ret = OB_ERR_CANT_CREATE_GEOMETRY_OBJECT;
                  LOG_USER_ERROR(OB_ERR_CANT_CREATE_GEOMETRY_OBJECT);
                }
              }
            }
           int64_t ori_len = str.length();
           if (OB_FAIL(ret)) {
           }  else if (!storage::is_ascii_str(str.ptr(), str.length())
                        && OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
              LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
            }
            if (OB_SUCC(ret)) {
              has_lob_header_for_check = false; // datum_for_check must have no lob header
              datum_for_check.set_string(str);
            }
            int warning = OB_SUCCESS;
            const int64_t str_len_byte = static_cast<int64_t>(datum_for_check.len_);
            bool need_check_length = true;
            if (OB_FAIL(ret)) {
            } else if (ori_len == str.length()
                       && max_accuracy_len > 0
                       && str.length() < max_accuracy_len) {
              need_check_length = false;
              results[i].set_datum(datum_for_check);
            } else if (OB_FAIL(column_convert_datum_accuracy_check(expr, ctx, has_lob_header_for_check, results[i],
                                                                  cast_mode, datum_for_check))) {
              LOG_WARN("fail do datum_accuracy_check for lob res", K(ret), K(expr), K(datum_for_check));
            }
            if (OB_SUCC(ret)) {
              // in type is the same with out type, if length changed, build a new lob
              ObLobLocatorV2 loc(raw_str, has_lob_header);
              int64_t old_data_byte_len = 0;
              if (need_check_length && raw_str.length() > 0) {
                if (OB_FAIL(loc.get_lob_data_byte_len(old_data_byte_len))) {
                  LOG_WARN("Lob: failed to get data byte len", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                int64_t new_data_byte_len = results[i].get_string().length();
                if (!need_check_length || new_data_byte_len == old_data_byte_len) {
                  results[i].set_string(raw_str);
                } else {
                  ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &results[i]);
                  if (OB_FAIL(str_result.init(new_data_byte_len))) {
                    LOG_WARN("Lob: init lob result failed", K(ret));
                  } else if (OB_FAIL(str_result.append(results[i].get_string().ptr(), new_data_byte_len))) {
                    LOG_WARN("Lob: append lob result failed", K(ret));
                  } else {
                    str_result.set_result();
                  }
                }
              }
            }
          }
          
          if (OB_FAIL(ret) && ctx.exec_ctx_.get_my_session()->is_diagnosis_enabled()) {
            // overwrite ret on diagnosis node
            if (OB_FAIL(ctx.exec_ctx_.get_diagnosis_manager().add_warning_info(ret, i))) {
              LOG_WARN("failed to add warning info", K(ret), K(i));
            } else {
              // set null to avoid accessing invalid data before setting skip
              // in ObTableScanOp::do_diagnosis
              results[i].set_null();
            }
          }
        }
        eval_flags.set(i);
      }
    }

    if (OB_SUCC(ret) && ctx.exec_ctx_.get_my_session()->is_diagnosis_enabled()) {
      if (OB_FAIL(calc_column_name_for_diagnosis(expr, ctx,
                                                ctx.exec_ctx_.get_diagnosis_manager()))) {
        LOG_WARN("fail to calculate column name for diagnosis", K(ret), K(expr));
      }
    }
  }
  return ret;
}

int ObExprColumnConv::calc_column_name_for_diagnosis(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDiagnosisManager& diagnosis_manager) {
  int ret = OB_SUCCESS;

  if (diagnosis_manager.col_names_.count() < diagnosis_manager.rets_.count()) {
    ObDatum *column_info = NULL;
    if (ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr.arg_cnt_
        && OB_SUCCESS == expr.args_[5]->eval(ctx, column_info)) {
      ObString column_name(column_info->get_string().length(), column_info->get_string().ptr());

      ObString tmp_str;
      if (OB_FAIL(ob_write_string(diagnosis_manager.allocator_, column_name, tmp_str, true))) {
        LOG_WARN("failed to write string", K(ret));
      } else {
        int64_t gap_cnt = diagnosis_manager.rets_.count() - diagnosis_manager.col_names_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < gap_cnt; i++) {
          if (OB_FAIL(diagnosis_manager.col_names_.push_back(tmp_str))) {
            LOG_WARN("failed to push back column name into array", K(ret), K(tmp_str));
          }
        }
      }
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObExprColumnConv::column_convert_vector(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            const ObBitVector &skip,
                                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  const ObEnumSetInfo *enumset_info = static_cast<ObEnumSetInfo *>(expr.extra_info_);
  const uint64_t cast_mode = enumset_info->cast_mode_;
  bool is_strict = CM_IS_STRICT_MODE(cast_mode) && !CM_IS_IGNORE_CHARSET_CONVERT_ERR(cast_mode);
  ObEvalInfo &eval_info = expr.args_[4]->get_eval_info(ctx);
  bool param_not_eval = !expr.args_[4]->get_eval_info(ctx).evaluated_
                        && !expr.args_[4]->get_eval_info(ctx).projected_;
  if (OB_FAIL(expr.args_[4]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval args", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    bool is_string_type = ob_is_string_type(out_type);
    bool is_string2text = is_string_text_cast(expr); 
    bool is_int_tc = ob_is_int_uint_tc(out_type);
    bool is_decimal_int_tc = ob_is_decimal_int_tc(out_type);
    ObAccuracy accuracy;
    accuracy.set_length(expr.max_length_);
    accuracy.set_scale(expr.datum_meta_.scale_);
    const ObObjTypeClass &dst_tc = ob_obj_type_class(expr.datum_meta_.type_);
    const ObLength max_accuracy_len = accuracy.get_length();
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat arg_format = expr.args_[4]->get_format(ctx);
    if (is_string_type) {
      accuracy.set_length_semantics(expr.datum_meta_.length_semantics_);
    }
    if (!is_lob_storage(out_type) || is_string2text) {
      if (is_string2text) {
        if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
          bool has_null = expr.args_[4]->get_vector(ctx)->has_null()
                          && (static_cast<ObDiscreteBase *>
                              (expr.args_[4]->get_vector(ctx))->get_nulls()->accumulate_bit_cnt(bound) != 0);
          if (bound.get_all_rows_active()) {
            if (has_null) {
              ret = inner_loop_for_convert_vector<PARAM_TC::TEXT_TC, ObDiscreteFormat, ObDiscreteFormat, true, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
            } else {
              ret = inner_loop_for_convert_vector<PARAM_TC::TEXT_TC, ObDiscreteFormat, ObDiscreteFormat, true, false>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
            }
          } else {
            ret = inner_loop_for_convert_vector<PARAM_TC::TEXT_TC, ObDiscreteFormat, ObDiscreteFormat, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
          }
        } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
          ret = inner_loop_for_convert_vector<PARAM_TC::TEXT_TC, ObUniformFormat<false>, ObUniformFormat<false>, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        } else {
          ret = inner_loop_for_convert_vector<PARAM_TC::TEXT_TC, ObVectorBase, ObVectorBase, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert batch", K(ret));
        }
      } else if (is_string_type) {
        if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
          bool has_null = expr.args_[4]->get_vector(ctx)->has_null()
                          && (static_cast<ObDiscreteBase *>
                              (expr.args_[4]->get_vector(ctx))->get_nulls()->accumulate_bit_cnt(bound) != 0);
          if (bound.get_all_rows_active()) {
            if (has_null) {
              ret = inner_loop_for_convert_vector<PARAM_TC::STRING_TC, ObDiscreteFormat, ObDiscreteFormat, true, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
            } else {
              ret = inner_loop_for_convert_vector<PARAM_TC::STRING_TC, ObDiscreteFormat, ObDiscreteFormat, true, false>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
            }
          } else {
            ret = inner_loop_for_convert_vector<PARAM_TC::STRING_TC, ObDiscreteFormat, ObDiscreteFormat, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
          }
        } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
          ret = inner_loop_for_convert_vector<PARAM_TC::STRING_TC, ObUniformFormat<false>, ObUniformFormat<false>, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        } else {
          ret = inner_loop_for_convert_vector<PARAM_TC::STRING_TC, ObVectorBase, ObVectorBase, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert batch", K(ret));
        }
      } else if (is_int_tc) {
        if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
          bool has_null = expr.args_[4]->get_vector(ctx)->has_null()
                          && (static_cast<ObFixedLengthBase *>
                              (expr.args_[4]->get_vector(ctx))->get_nulls()->accumulate_bit_cnt(bound) != 0);
          if (bound.get_all_rows_active()) {
            if (has_null) {
              ret = inner_loop_for_convert_vector<PARAM_TC::INT_TC,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              true, true>(expr, ctx, skip, bound,
                                                     is_strict, max_accuracy_len, cast_mode,
                                                     eval_flags);
            } else {
              ret = inner_loop_for_convert_vector<PARAM_TC::INT_TC,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              true, false>(expr, ctx, skip, bound,
                                                     is_strict, max_accuracy_len, cast_mode,
                                                     eval_flags);
            }
          } else {
            ret = inner_loop_for_convert_vector<PARAM_TC::INT_TC,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                              false, true>(expr, ctx, skip, bound,
                                                     is_strict, max_accuracy_len, cast_mode,
                                                     eval_flags);
          }
        } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
          ret = inner_loop_for_convert_vector<PARAM_TC::INT_TC, ObUniformFormat<false>, ObUniformFormat<false>, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        } else {
          ret = inner_loop_for_convert_vector<PARAM_TC::INT_TC, ObVectorBase, ObVectorBase, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert batch", K(ret));
        }
      } else if (is_decimal_int_tc
                 && OB_SUCCESS != (ret = inner_loop_for_convert_vector<PARAM_TC::DECIMAL_INT_TC, ObVectorBase, ObVectorBase, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags))) {
        LOG_WARN("failed to convert batch", K(ret));
      } else if (OB_SUCCESS != (ret = inner_loop_for_convert_vector<PARAM_TC::OTHER_TC, ObVectorBase, ObVectorBase, false, true>(expr, ctx, skip, bound,
                                                                             is_strict, max_accuracy_len, cast_mode,
                                                                             eval_flags))) {
        LOG_WARN("failed to convert batch", K(ret));
      }
    }

    if (OB_SUCC(ret) && ctx.exec_ctx_.get_my_session()->is_diagnosis_enabled()) {
      if (OB_FAIL(calc_column_name_for_diagnosis(expr, ctx,
                                                ctx.exec_ctx_.get_diagnosis_manager()))) {
        LOG_WARN("fail to calculate column name for diagnosis", K(ret), K(expr));
      }
    }
  }
  return ret;
}

int ObExprColumnConv::column_convert_batch_fast(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[4]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch vals", K(ret));
  } else {
    ObDatum *vals = expr.args_[4]->locate_batch_datums(ctx);
    ObDatum *results = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      results[i].set_datum(vals[i]);
      eval_flags.set(i);
    } 
  }
  return ret;
}

int ObExprColumnConv::column_convert_vector_fast(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  VectorFormat arg_format = expr.args_[4]->get_format(ctx);
  ObObjType out_type = expr.datum_meta_.type_;
  if (OB_FAIL(expr.args_[4]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval args", K(ret));
  } else {
    if (VEC_FIXED == res_format && VEC_FIXED == arg_format) {
      if (ob_is_integer_type(out_type)) {
        ret = inner_calc_column_convert_vector_fast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
                                                    ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>> (expr, ctx, skip, bound);
      } else if (ObDoubleType == out_type) {
        ret = inner_calc_column_convert_vector_fast<ObFixedLengthFormat<RTCType<VEC_TC_DOUBLE>>,
                                                    ObFixedLengthFormat<RTCType<VEC_TC_DOUBLE>>> (expr, ctx, skip, bound);
      } else if (ObFloatType == out_type) {
        ret = inner_calc_column_convert_vector_fast<ObFixedLengthFormat<RTCType<VEC_TC_FLOAT>>,
                                                    ObFixedLengthFormat<RTCType<VEC_TC_FLOAT>>> (expr, ctx, skip, bound);
      } else {
        ret = inner_calc_column_convert_vector_fast<ObVectorBase, ObVectorBase> (expr, ctx, skip, bound);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to convert args", K(ret));
      }
    } else if (VEC_DISCRETE == res_format && VEC_DISCRETE == arg_format) {
      if (OB_SUCCESS != inner_calc_column_convert_vector_fast<ObDiscreteFormat, ObDiscreteFormat>(expr, ctx, skip, bound)) {
        LOG_WARN("failed to convert args", K(ret));
      }
    } else if (OB_SUCCESS != inner_calc_column_convert_vector_fast<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound)) {
      LOG_WARN("failed to convert args", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprColumnConv::inner_calc_column_convert_vector_fast(const ObExpr &expr,
                                                            ObEvalCtx &ctx,
                                                            const ObBitVector &skip,
                                                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[4]->get_vector(ctx));
  if (bound.get_all_rows_active()) {
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      if (arg_vec->is_null(i)) {
        res_vec->set_null(i);
      } else {
        res_vec->set_payload_shallow(i, arg_vec->get_payload(i), arg_vec->get_length(i));
      }
    }
    eval_flags.set_all(bound.start(), bound.end());
  } else {
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      if (arg_vec->is_null(i)) {
        res_vec->set_null(i);
      } else {
        res_vec->set_payload_shallow(i, arg_vec->get_payload(i), arg_vec->get_length(i));
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

int ObExprColumnConv::eval_enumset(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum *&datum)
{
  int ret = OB_SUCCESS;
  const ObEnumSetInfo *enumset_info = static_cast<ObEnumSetInfo *>(expr.extra_info_);
  const uint64_t cast_mode = enumset_info->cast_mode_;
  const uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (OB_UNLIKELY(expr_ctx_id == ObExpr::INVALID_EXP_CTX_ID)) {
    ObExpr *old_expr = expr.args_[0];
    expr.args_[0] = expr.args_[4];
    if (OB_FAIL(expr.eval_enumset(ctx, enumset_info->str_values_, cast_mode, datum))) {
      LOG_WARN("fail to eval_enumset", KPC(enumset_info), K(ret));
    }
    expr.args_[0] = old_expr;
  } else {
    ObExprColumnConvCtx *column_conv_ctx = NULL;
    if (OB_ISNULL(column_conv_ctx = static_cast<ObExprColumnConvCtx *>
        (ctx.exec_ctx_.get_expr_op_ctx(expr_ctx_id)))) {
      if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr_ctx_id, column_conv_ctx))) {
        LOG_WARN("fail to create expr op ctx", K(ret), K(expr_ctx_id));
      } else if (OB_FAIL(column_conv_ctx->setup_eval_expr(ctx.exec_ctx_.get_allocator(), expr))) {
        LOG_WARN("fail to init column conv ctx", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_conv_ctx->expr_.eval_enumset(ctx, enumset_info->str_values_, cast_mode,
                                                      datum))) {
        LOG_WARN("fail to eval_enumset", KPC(enumset_info), K(ret));
      }
    }
  }
  return ret;
}

int ObExprColumnConv::ObExprColumnConvCtx::setup_eval_expr(ObIAllocator &allocator,
                                                           const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(ObExpr*) * expr.arg_cnt_;
  if (OB_ISNULL(args_ = static_cast<ObExpr**>(allocator.alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc args", K(ret), K(mem_size));
  } else {
    expr_ = expr;
    expr_.args_ = args_;
    MEMCPY(args_, expr.args_, mem_size);
    args_[0] = args_[4];
  }
  return ret;
}

bool ObExprColumnConv::check_is_ascii(ObString &str)
{
  bool is_ascii = true;
  for (int64_t i = 0; is_ascii && i < str.length(); ++i) {
    is_ascii &= (static_cast<unsigned char> (str[i]) <= 127);
  }
  return is_ascii;
}

template <ObExprColumnConv::PARAM_TC TC>
int ObExprColumnConv::inner_loop_for_convert_batch(const ObExpr &expr,
                                                  ObEvalCtx &ctx,
                                                  const ObBitVector &skip,
                                                  const int64_t batch_size,
                                                  const bool is_strict,
                                                  const ObLength max_accuracy_len,
                                                  const uint64_t cast_mode,
                                                  ObBitVector &eval_flags,
                                                  ObDatum *vals,
                                                  ObDatum *results,
                                                  ObEvalCtx::BatchInfoScopeGuard &batch_info_guard)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  int32_t int_bytes = 0;
  const ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
  common::ObPrecision precision = expr.datum_meta_.precision_;
  const bool has_lob_header = expr.args_[4]->obj_meta_.has_lob_header();
  decint_cmp_fp cmp_fp;
  if (TC == PARAM_TC::DECIMAL_INT_TC) {
    min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
    max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
    int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    cmp_fp = wide::ObDecimalIntCmpSet::get_decint_decint_cmp_func(int_bytes, int_bytes);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    }
    if (vals[i].is_null()) {
      results[i].set_null();
    } else {
      batch_info_guard.set_batch_idx(i);
      if (TC == PARAM_TC::STRING_TC) {
        ObString str = vals[i].get_string();
        if (!storage::is_ascii_str(str.ptr(), str.length())
            && OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
          LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
        } else {
          vals[i].set_string(str);
        }
      } else if (TC == PARAM_TC::TEXT_TC) {
        ObString str = vals[i].get_string();
        const char* origin_ptr = str.ptr();
        int64_t origin_len = str.length();
        if (! has_lob_header) { // tinytext
        } else if (has_lob_header && vals[i].len_ != 0 && ! vals[i].get_lob_data().is_mem_loc_ && vals[i].get_lob_data().in_row_) {
          const ObLobCommon& lob = vals[i].get_lob_data();
          origin_ptr = lob.get_inrow_data_ptr();
          origin_len = lob.get_byte_size(str.length());
          str.assign_ptr(origin_ptr, origin_len);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not reach here", K(ret), K(str));
        }
        if (OB_FAIL(ret)) {
        }  else if (!storage::is_ascii_str(str.ptr(), str.length())
                      && OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
            LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
        } else if (origin_len != str.length()) {
          if (has_lob_header) {
            if (origin_ptr != str.ptr()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("data memroy is changed", K(ret), K(i), KP(origin_ptr), K(origin_len), KP(str.ptr()), K(str.length()));
            } else {
              vals[i].set_string(vals[i].ptr_, sizeof(ObLobCommon) + str.length());
            }
          } else {
            vals[i].set_string(str);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (TC == PARAM_TC::INT_TC
                 || ((TC == PARAM_TC::STRING_TC || TC == PARAM_TC::TEXT_TC)
                     && max_accuracy_len > 0
                     && vals[i].len_ < max_accuracy_len)) {
        results[i].set_datum(vals[i]);
      } else if (TC == PARAM_TC::DECIMAL_INT_TC
                 && cmp_fp(vals[i].get_decimal_int(), min_decint) >= 0
                 && cmp_fp(vals[i].get_decimal_int(), max_decint) <= 0) {
        results[i].set_datum(vals[i]);
      } else if (OB_FAIL(column_convert_datum_accuracy_check(expr, ctx,
                                                             has_lob_header, results[i],
                                                             cast_mode, vals[i]))) {
        LOG_WARN("fail do datum_accuracy_check for lob res", K(ret), K(expr));
      }

      if (OB_FAIL(ret) && ctx.exec_ctx_.get_my_session()->is_diagnosis_enabled()) {
        // overwrite ret on diagnosis node
        if (OB_FAIL(ctx.exec_ctx_.get_diagnosis_manager().add_warning_info(ret, i))) {
          LOG_WARN("failed to add warning info", K(ret), K(i));
        } else {
          results[i].set_null();
        }
      }
    }
    eval_flags.set(i);
  }
  return ret;
}


template <ObExprColumnConv::PARAM_TC TC, typename ArgVec, typename ResVec, bool ALL_ROWS_ACTIVE, bool HAS_NULL>
int ObExprColumnConv::inner_loop_for_convert_vector(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    const ObBitVector &skip,
                                                    const EvalBound &bound,
                                                    const bool is_strict,
                                                    const ObLength max_accuracy_len,
                                                    const uint64_t cast_mode,
                                                    ObBitVector &eval_flags)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  int32_t int_bytes = 0;
  const ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
  common::ObPrecision precision = expr.datum_meta_.precision_;
  const bool has_lob_header = expr.args_[4]->obj_meta_.has_lob_header();
  decint_cmp_fp cmp_fp;
  ResVec *res_vec = static_cast<ResVec *> (expr.get_vector(ctx));
  ArgVec *arg_vec = static_cast<ArgVec *> (expr.args_[4]->get_vector(ctx));
  if (TC == PARAM_TC::DECIMAL_INT_TC) {
    min_decint = wide::ObDecimalIntConstValue::get_min_value(precision);
    max_decint = wide::ObDecimalIntConstValue::get_max_value(precision);
    int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
    cmp_fp = wide::ObDecimalIntCmpSet::get_decint_decint_cmp_func(int_bytes, int_bytes);
  }
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(bound.batch_size());
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (!ALL_ROWS_ACTIVE && (skip.at(i) || eval_flags.at(i))) {
      continue;
    }
    if (HAS_NULL && arg_vec->is_null(i)) {
      res_vec->set_null(i);
    } else {
      batch_info_guard.set_batch_idx(i);
      if (TC == PARAM_TC::STRING_TC) {
        ObString str = arg_vec->get_string(i);
        int64_t orign_len = str.length();
        if (!storage::is_ascii_str(str.ptr(), str.length())
            && OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
          LOG_WARN("fail to check collation", K(i), K(bound), K(ret), K(str), K(is_strict), K(expr));
        } else if (orign_len != str.length()) {
          arg_vec->set_string(i, str);
        }
      } else if (TC == PARAM_TC::TEXT_TC) {
        ObString str = arg_vec->get_string(i);
        const char* origin_ptr = str.ptr();
        int64_t origin_len = str.length();
        if (! has_lob_header) { // tinytext
        } else if (has_lob_header && str.length() != 0 && ! arg_vec->get_lob_data(i).is_mem_loc_ && arg_vec->get_lob_data(i).in_row_) {
          const ObLobCommon& lob = arg_vec->get_lob_data(i);
          origin_ptr = lob.get_inrow_data_ptr();
          origin_len = lob.get_byte_size(str.length());
          str.assign_ptr(origin_ptr, origin_len);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not reach here", K(ret), K(str));
        }
        if (OB_FAIL(ret)) {
        }  else if (!storage::is_ascii_str(str.ptr(), str.length())
                      && OB_FAIL(string_collation_check(is_strict, out_cs_type, out_type, str))) {
            LOG_WARN("fail to check collation", K(ret), K(str), K(is_strict), K(expr));
        } else if (origin_len != str.length()) {
          if (has_lob_header) {
            if (origin_ptr != str.ptr()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("data memroy is changed", K(ret), K(i), KP(origin_ptr), K(origin_len), KP(str.ptr()), K(str.length()));
            } else {
              arg_vec->set_string(i, arg_vec->get_string(i).ptr(), sizeof(ObLobCommon) + str.length());
            }
          } else {
            arg_vec->set_string(i, str);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (TC == PARAM_TC::INT_TC
                 || ((TC == PARAM_TC::STRING_TC || TC == PARAM_TC::TEXT_TC)
                     && max_accuracy_len > 0
                     && arg_vec->get_length(i) < max_accuracy_len)) {
        res_vec->set_payload_shallow(i, arg_vec->get_payload(i), arg_vec->get_length(i));
      } else if (TC == PARAM_TC::DECIMAL_INT_TC
                 && cmp_fp(arg_vec->get_decimal_int(i), min_decint) >= 0
                 && cmp_fp(arg_vec->get_decimal_int(i), max_decint) <= 0) {
        res_vec->set_payload_shallow(i, arg_vec->get_payload(i), arg_vec->get_length(i));
      } else if (OB_FAIL(column_convert_vector_accuracy_check(expr, ctx, skip,
                                                             has_lob_header, cast_mode,
                                                             i, *arg_vec, *res_vec))) {
        LOG_WARN("fail do datum_accuracy_check for lob res", K(ret), K(expr));
      }
    }
    if (!ALL_ROWS_ACTIVE) {
      eval_flags.set(i);
    }
    if (OB_FAIL(ret) && ctx.exec_ctx_.get_my_session()->is_diagnosis_enabled()) {
      // overwrite ret on diagnosis node
      if (OB_FAIL(ctx.exec_ctx_.get_diagnosis_manager().add_warning_info(ret, i))) {
        LOG_WARN("failed to add warning info", K(ret), K(i));
      } else {
        // set null to avoid accessing invalid data before setting skip
        // in ObTableScanOp::do_diagnosis
        res_vec->set_null(i);
      }
    }
  }
  if (OB_SUCC(ret) && ALL_ROWS_ACTIVE) {
    eval_flags.set_all(bound.start(), bound.end());
  }
  return ret;
}

//TODO(yaoying.yyy):(duplicate_with_type_to_str, remove later
int ObBaseExprColumnConv::shallow_copy_str_values(const common::ObIArray<common::ObString> &str_values)
{
  int ret = OB_SUCCESS;
  str_values_.reset();
  if (OB_UNLIKELY(str_values.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str_values", K(str_values), K(ret));
  } else if (OB_FAIL(str_values_.assign(str_values))) {
    LOG_WARN("fail to assign str values", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObBaseExprColumnConv::deep_copy_str_values(const ObIArray<ObString> &str_values)
{
  int ret = OB_SUCCESS;
  str_values_.reset();
  if (OB_UNLIKELY(str_values.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str_values", K(str_values), K(ret));
  } else if (OB_FAIL(str_values_.reserve(str_values.count()))) {
    LOG_WARN("fail to init str_values_", K(ret));
  } else {/*do nothing*/}

  for (int64_t i = 0; OB_SUCC(ret) && i < str_values.count(); ++i) {
    const ObString &str = str_values.at(i);
    char *buf = NULL;
    ObString str_tmp;
    if (str.empty()) {
      //just keep str_tmp empty
    } else if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(alloc_.alloc(str.length()))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(i), K(str), K(ret));
    } else {
      MEMCPY(buf, str.ptr(), str.length());
      str_tmp.assign_ptr(buf, str.length());
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(str_values_.push_back(str_tmp))) {
        LOG_WARN("failed to push back str", K(i), K(str_tmp), K(str), K(ret));
      }
    }
  }
  return ret;
}
}//sql
}//oceanbase
