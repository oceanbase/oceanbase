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
 * This file contains implementation for string_to_array expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_string_to_array.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "lib/charset/ob_ctype.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprStringToArray::ObExprStringToArray(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_STRING_TO_ARRAY, N_STRING_TO_ARRAY, TWO_OR_THREE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprStringToArray::~ObExprStringToArray()
{
}

int ObExprStringToArray::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types,
                                           int64_t param_num,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  uint16_t subschema_id;
  ObDataType res_data_type;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if (ob_is_null(types[i].get_type())) {
      // do nothing
    } else if (ob_is_varchar_char_type(types[i].get_type(), types[i].get_collation_type())) {
      types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "VARCHAR", ob_obj_type_str(types[i].get_type()));
    }
  }

  if (OB_SUCC(ret)) {
    res_data_type.set_meta_type(types[0].get_obj_meta());
    res_data_type.set_accuracy(types[0].get_accuracy());
    res_data_type.set_length(types[0].get_length());
    if (OB_FAIL(exec_ctx->get_subschema_id_by_collection_elem_type(ObNestedType::OB_ARRAY_TYPE, res_data_type, subschema_id))) {
      LOG_WARN("failed to get collection subschema id", K(ret));
    } else {
      type.set_collection(subschema_id);
    }
  }
  return ret;
}

int ObExprStringToArray::eval_string_to_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;;
  ObDatum *arr_str_datum = NULL;
  ObDatum *delimiter_datum = NULL;
  ObDatum *null_str_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_str_datum))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, delimiter_datum))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval(ctx, null_str_datum))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    std::string arr_str;
    std::string delimiter;
    std::string null_str;
    bool has_arr_str = false;
    bool has_delimiter = false;
    bool has_null_str = false;
    ObIArrayType *arr_obj = NULL;
    ObArrayBinary *binary_array = NULL;
    if (!arr_str_datum->is_null()) {
      has_arr_str = true;
      arr_str.assign(arr_str_datum->get_string().ptr(), arr_str_datum->get_string().length());
    }
    if (!delimiter_datum->is_null()) {
      has_delimiter = true;
      delimiter.assign(delimiter_datum->get_string().ptr(), delimiter_datum->get_string().length());
    }
    if (expr.arg_cnt_ > 2 && !null_str_datum->is_null()) {
      has_null_str = true;
      null_str.assign(null_str_datum->get_string().ptr(), null_str_datum->get_string().length());
    }
    if (OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, arr_obj, false))) {
      LOG_WARN("construct array obj failed", K(ret), K(subschema_id));
    } else if (OB_ISNULL(binary_array = static_cast<ObArrayBinary *>(arr_obj))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("binary array is null", K(ret), K(subschema_id));
    } else if (OB_FAIL(string_to_array(binary_array, arr_str, delimiter, null_str, cs_type, has_arr_str, has_delimiter, has_null_str))) {
      LOG_WARN("failed to convert string to array", K(ret));
    } else if (!has_arr_str) {
      res.set_null();
    } else {
      ObString res_str;
      if (OB_FAIL(ObArrayExprUtils::set_array_res(arr_obj, arr_obj->get_raw_binary_len(), expr, ctx, res_str))) {
        LOG_WARN("get array binary string failed", K(ret));
      } else {
        res.set_string(res_str);
      }
    }
  }
  return ret;
}

int ObExprStringToArray::eval_string_to_array_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                    const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;;
  ObIArrayType *arr_obj = NULL;

  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    ObDatumVector arr_str_array = expr.args_[0]->locate_expr_datumvector(ctx);
    ObDatumVector delimiter_array = expr.args_[1]->locate_expr_datumvector(ctx);
    ObDatumVector null_str_array = expr.arg_cnt_ > 2 ? expr.args_[2]->locate_expr_datumvector(ctx) : ObDatumVector();
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
      std::string arr_str;
      std::string delimiter;
      std::string null_str;
      bool has_arr_str = false;
      bool has_delimiter = false;
      bool has_null_str = false;
      ObArrayBinary *binary_array = NULL;

      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      if (!arr_str_array.at(j)->is_null()) {
        has_arr_str = true;
        arr_str.assign(arr_str_array.at(j)->get_string().ptr(), arr_str_array.at(j)->get_string().length());
      }
      if (!delimiter_array.at(j)->is_null()) {
        has_delimiter = true;
        delimiter.assign(delimiter_array.at(j)->get_string().ptr(), delimiter_array.at(j)->get_string().length());
      }
      if (expr.arg_cnt_ > 2 && !null_str_array.at(j)->is_null()) {
        has_null_str = true;
        null_str.assign(null_str_array.at(j)->get_string().ptr(), null_str_array.at(j)->get_string().length());
      }
      if (OB_ISNULL(arr_obj) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, arr_obj, false))) {
        LOG_WARN("construct array obj failed", K(ret), K(subschema_id));
      } else if (OB_FALSE_IT(arr_obj->clear())) {
      } else if (OB_ISNULL(binary_array = static_cast<ObArrayBinary *>(arr_obj))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("binary array is null", K(ret), K(subschema_id));
      } else if (OB_FAIL(string_to_array(binary_array, arr_str, delimiter, null_str, cs_type, has_arr_str, has_delimiter, has_null_str))) {
        LOG_WARN("failed to convert string to array", K(ret));
      } else if (!has_arr_str) {
        res_datum.at(j)->set_null();
      } else {
        int32_t res_size = binary_array->get_raw_binary_len();
        char *res_buf = nullptr;
        int64_t res_buf_len = 0;
        ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, res_datum.at(j));
        if (OB_FAIL(output_result.init_with_batch_idx(res_size, j))) {
          LOG_WARN("fail to init result", K(ret), K(res_size));
        } else if (OB_FAIL(output_result.get_reserved_buffer(res_buf, res_buf_len))) {
          LOG_WARN("fail to get reserver buffer", K(ret));
        } else if (res_buf_len < res_size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid res buf len", K(ret), K(res_buf_len), K(res_size));
        } else if (OB_FAIL(binary_array->get_raw_binary(res_buf, res_buf_len))) {
          LOG_WARN("get array raw binary failed", K(ret), K(res_buf_len), K(res_size));
        } else if (OB_FAIL(output_result.lseek(res_size, 0))) {
          LOG_WARN("failed to lseek res.", K(ret), K(output_result), K(res_size));
        } else {
          output_result.set_result();
        }
      }
    } // end for
  }
  return ret;
}

int ObExprStringToArray::eval_string_to_array_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                     const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  const uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;;
  ObIArrayType *arr_obj = NULL;

  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval source array failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval delimiter string failed", K(ret));
  } else if (OB_FAIL(expr.arg_cnt_ > 2 && expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval null string failed", K(ret));
  } else {
    ObIVector *arr_str_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *delimiter_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *null_str_vec = expr.arg_cnt_ > 2 ? expr.args_[2]->get_vector(ctx) : NULL;
    ObIVector *res_vec = expr.get_vector(ctx);
    ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool has_arr_str = false;
      bool has_delimiter = false;
      bool has_null_str = false;
      std::string arr_str;
      std::string delimiter;
      std::string null_str;
      ObArrayBinary *binary_array = NULL;

      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      eval_flags.set(idx);
      if (!arr_str_vec->is_null(idx)) {
        has_arr_str = true;
        arr_str.assign(arr_str_vec->get_string(idx).ptr(), arr_str_vec->get_string(idx).length());
      }
      if (!delimiter_vec->is_null(idx)) {
        has_delimiter = true;
        delimiter.assign(delimiter_vec->get_string(idx).ptr(), delimiter_vec->get_string(idx).length());
      }
      if (expr.arg_cnt_ > 2 && !null_str_vec->is_null(idx)) {
        has_null_str = true;
        null_str.assign(null_str_vec->get_string(idx).ptr(), null_str_vec->get_string(idx).length());
      }
      if (OB_ISNULL(arr_obj) && OB_FAIL(ObArrayExprUtils::construct_array_obj(tmp_allocator, ctx, subschema_id, arr_obj, false))) {
        LOG_WARN("construct array obj failed", K(ret), K(subschema_id));
      } else if (OB_FALSE_IT(arr_obj->clear())) {
      } else if (OB_ISNULL(binary_array = static_cast<ObArrayBinary *>(arr_obj))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("binary array is null", K(ret), K(subschema_id));
      } else if (OB_FAIL(string_to_array(binary_array, arr_str, delimiter, null_str, cs_type, has_arr_str, has_delimiter, has_null_str))) {
        LOG_WARN("failed to convert string to array", K(ret));
      } else if (!has_arr_str) {
        res_vec->set_null(idx);
      } else {
        if (res_format == VEC_DISCRETE) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObDiscreteFormat>(arr_obj, expr, ctx, static_cast<ObDiscreteFormat *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (res_format == VEC_UNIFORM) {
          if (OB_FAIL(ObArrayExprUtils::set_array_res<ObUniformFormat<false>>(arr_obj, expr, ctx, static_cast<ObUniformFormat<false> *>(res_vec), idx))) {
            LOG_WARN("set array res failed", K(ret));
          }
        } else if (OB_FAIL(ObArrayExprUtils::set_array_res<ObVectorBase>(arr_obj, expr, ctx, static_cast<ObVectorBase *>(res_vec), idx))) {
          LOG_WARN("set array res failed", K(ret));
        }
      }
    } // end for
  }
  return ret;
}

int ObExprStringToArray::string_to_array(ObArrayBinary *binary_array,
                                         std::string arr_str, std::string delimiter, std::string null_str,
                                         ObCollationType cs_type, bool has_arr_str, bool has_delimiter, bool has_null_str)
{
  int ret = OB_SUCCESS;
  int32_t str_len_char = 0;
  if (!has_arr_str || arr_str.empty()) {
    // do nothing
  } else if (!has_delimiter) {
    // add value to array character by character
    size_t offset = 0;
    const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
    while (offset < arr_str.length() && OB_SUCC(ret)) {
      int mb_len = use_mb(cs) ? ob_ismbchar(cs, arr_str.data() + offset, arr_str.data() + arr_str.length()) : 0;
      size_t char_len = mb_len ? mb_len : 1;
      std::string value_str;
      if (offset + char_len > arr_str.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected string end", K(arr_str.length()), K(offset), K(char_len));
      } else if (OB_FALSE_IT(value_str = arr_str.substr(offset, char_len))) {
      } else if (OB_FAIL(add_value_str_to_array(binary_array, value_str, has_null_str, null_str))) {
        LOG_WARN("failed to add character to array", K(ret), K(ObString(value_str.length(), value_str.data())));
      } else {
        offset += char_len;
      }
    } // end while
  } else {
    size_t value_start = 0;
    size_t value_end = 0;
    bool parse_finished = false;
    while (OB_SUCC(ret) && !parse_finished) {
      value_end = delimiter.empty() ? arr_str.length() : arr_str.find(delimiter, value_start);
      if (value_end >= arr_str.length()) {
        parse_finished = true;
        value_end = arr_str.length();
      }
      std::string value_str = arr_str.substr(value_start, value_end - value_start);
      if (OB_FAIL(add_value_str_to_array(binary_array, value_str, has_null_str, null_str))) {
        LOG_WARN("failed to add value string to array", K(ret), K(ObString(value_str.length(), value_str.data())));
      } else {
        value_start = value_end + delimiter.length();
      }
    } // end while
  }
  return ret;
}

int ObExprStringToArray::add_value_str_to_array(ObArrayBinary *binary_array, std::string value_str, bool has_null_str, std::string null_str)
{
  int ret = OB_SUCCESS;
  if (has_null_str && value_str.compare(null_str) == 0) {
    // value is null
    if (OB_FAIL(binary_array->push_back(ObString(), true))) {
      LOG_WARN("failed to push back null value", K(ret));
    }
  } else {
    if (OB_FAIL(binary_array->push_back(ObString(value_str.length(), value_str.data())))) {
      LOG_WARN("failed to push back value string", K(ret));
    }
  }
  return ret;
}

int ObExprStringToArray::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_string_to_array;
  rt_expr.eval_batch_func_ = eval_string_to_array_batch;
  rt_expr.eval_vector_func_ = eval_string_to_array_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
