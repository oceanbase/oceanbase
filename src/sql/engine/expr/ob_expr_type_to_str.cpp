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
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array_serialization.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

//////////////////////////// ObExprTypeToStr ////////////////////////////

OB_SERIALIZE_MEMBER_INHERIT(ObExprTypeToStr, ObExprOperator, str_values_);

int ObExprTypeToStr::assign(const ObExprOperator &other) {
  int ret = OB_SUCCESS;
  const ObExprTypeToStr *tmp_other = dynamic_cast<const ObExprTypeToStr*>(&other);

  LOG_DEBUG("start to assign ObExprTypeToStr", K(other), K(*this));
  if ((OB_ISNULL(tmp_other))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cast failed, type of argument is wrong", K(ret), K(other));
  } else if (OB_UNLIKELY(tmp_other == this)) {
    LOG_DEBUG("other is same with this, no need to assign");
  } else {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("ObExprOperator::assign failed", K(ret));
    } else if (OB_FAIL(str_values_.assign(tmp_other->str_values_))) {
      LOG_WARN("copy str_values failed");
    }
  }
  return ret;
}

// types[0]  collation_type and length
// types[1]     value
int ObExprTypeToStr::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type2);
  int ret = OB_SUCCESS;
  if (get_raw_expr()->get_extra() == 0) {
    type.set_type(ObVarcharType);
  } else {
    ObObjType dst_type = static_cast<ObObjType>(get_raw_expr()->get_extra());
    if (!ob_is_large_text(dst_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid dst type", K(ret), K(dst_type));
    } else {
      type.set_type(dst_type);
    }
  }
  type.set_collation_type(type1.get_collation_type());
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_length(type1.get_length());
  return ret;
}

int ObExprTypeToStr::shallow_copy_str_values(const common::ObIArray<common::ObString> &str_values)
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

int ObExprTypeToStr::deep_copy_str_values(const ObIArray<ObString> &str_values)
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

int ObExprTypeToStr::deep_copy_str(const ObString &src_str, char *dest_buf, int64_t buf_len, int64_t &pos) const
{

  int ret = OB_SUCCESS;
  int64_t length = 0;
  if(OB_ISNULL(dest_buf)
     || OB_UNLIKELY(buf_len < 0)
     || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(src_str.empty())) {
    //do nothing
  } else {
    length = src_str.length();
    if (OB_UNLIKELY(buf_len < pos + length)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf length is not enough", K(pos), K(length), K(buf_len), K(ret));
    } else {
      MEMCPY(dest_buf + pos, src_str.ptr(), src_str.length());
      pos += length;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObEnumSetInfo, cast_mode_, str_values_);

int ObEnumSetInfo::init_enum_set_info(common::ObIAllocator *allocator, ObExpr &rt_expr,
    const ObExprOperatorType type, const uint64_t cast_mode,
    const common::ObIArray<common::ObString> &str_values)
{
  int ret = OB_SUCCESS;
  ObEnumSetInfo *enumset_info = NULL;
  void *buf = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(buf = allocator->alloc(sizeof(ObEnumSetInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    enumset_info = new(buf) ObEnumSetInfo(*allocator, type);
    enumset_info->cast_mode_ = cast_mode;
    if (OB_FAIL(enumset_info->str_values_.reserve(str_values.count()))) {
      LOG_WARN("fail to init str_values_", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < str_values.count(); ++i) {
      const ObString &str = str_values.at(i);
      char *buf = NULL;
      ObString str_tmp;
      if (str.empty()) {
        //just keep str_tmp empty
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(str.length())))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(i), K(str), K(ret));
      } else {
        MEMCPY(buf, str.ptr(), str.length());
        str_tmp.assign_ptr(buf, str.length());
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(enumset_info->str_values_.push_back(str_tmp))) {
          LOG_WARN("failed to push back str", K(i), K(str_tmp), K(str), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      rt_expr.extra_info_ = enumset_info;
      LOG_DEBUG("succ init_enum_set_info", KPC(enumset_info));
    }
  }
  return ret;
}

int ObEnumSetInfo::deep_copy(common::ObIAllocator &allocator,
                             const ObExprOperatorType type,
                             ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  ObEnumSetInfo *copied_enum_set_info = NULL;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc expr extra info", K(ret));
  } else if (OB_ISNULL(copied_enum_set_info = dynamic_cast<ObEnumSetInfo *>(copied_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FAIL(copied_enum_set_info->str_values_.prepare_allocate(str_values_.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    copied_enum_set_info->cast_mode_ = cast_mode_;
    for (int i = 0; OB_SUCC(ret) && i < str_values_.count(); i++) {
      if (OB_FAIL(ob_write_string(allocator, str_values_.at(i),
                                  copied_enum_set_info->str_values_.at(i)))) {
        LOG_WARN("failed to write string", K(ret));
      }
    }
  }
  return ret;
}

//////////////////////////// ObExprSetToStr ////////////////////////////

ObExprSetToStr::ObExprSetToStr(ObIAllocator &alloc)
    : ObExprTypeToStr(alloc, T_FUN_SET_TO_STR, N_SET_TO_STR, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprSetToStr::~ObExprSetToStr()
{
}

int ObExprSetToStr::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (OB_FAIL(ObEnumSetInfo::init_enum_set_info(op_cg_ctx.allocator_, rt_expr, type_,
      0, str_values_))) {
    LOG_WARN("fail to init_enum_set_info", K(ret), K(type_), K(str_values_));
  } else {
    rt_expr.eval_func_ = calc_to_str_expr;
  }
  return ret;
}

int ObExprSetToStr::calc_to_str_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *set_datum = NULL;
  const ObString &sep = ObCharsetUtils::get_const_str(expr.datum_meta_.cs_type_, ',');
  if (OB_UNLIKELY(expr.arg_cnt_ != 2)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[1])
      || OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr.arg_cnt_ is unexpected", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, set_datum))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (set_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObIArray<ObString> &str_values = static_cast<ObEnumSetInfo *>(expr.extra_info_)->str_values_;
    uint64_t set_val = set_datum->get_set();

    //在value存在重复的情况时，element_num会大于64，忽略64以后的值
    int64_t element_num = str_values.count();
    if (OB_UNLIKELY(element_num < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid element num", K(element_num), K(ret));
    } else if (OB_UNLIKELY(element_num < EFFECTIVE_COUNT && set_val >= (1ULL << element_num))) {
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("set value out of range", K(set_val), K(element_num));
    }

    int64_t need_size = 0;
    uint64_t index = 1ULL;
    for (int64_t i = 0;
         OB_SUCC(ret) && i < element_num && i < EFFECTIVE_COUNT && set_val >= index;
         ++i, index = index << 1) {
      if (set_val & (index)) {
        need_size += str_values.at(i).length();
        need_size += ((set_val >= (index << 1)) ? sep.length() : 0);
      }
    }

    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      char *buf = NULL;
      ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
      if (OB_FAIL(text_result.init(need_size))) {
        LOG_WARN("init lob result failed", K(ret), K(need_size));
      } else {
        uint64_t index = 1ULL;
        for (int64_t i = 0;
             OB_SUCC(ret) && i < element_num && i < EFFECTIVE_COUNT && set_val >= index;
             ++i, index = index << 1) {
          if (set_val & (index)) {
            const ObString &element_val = str_values.at(i);
            if (OB_FAIL(text_result.append(element_val))) {
              LOG_WARN("fail to append str to lob result", K(ret), K(element_val));
            } else if ((i + 1) < element_num && (i + 1) < EFFECTIVE_COUNT &&
                ((index << 1) <= set_val)) {
              // skip setting last seperator
              if (OB_FAIL(text_result.append(sep))) {
                LOG_WARN("fail to append str to lob result", K(ret), K(sep));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          text_result.set_result();
        }
      }
    }
  }
  return ret;
}

//////////////////////////// ObExprEnumTostr ////////////////////////////
ObExprEnumToStr::ObExprEnumToStr(ObIAllocator &alloc)
    : ObExprTypeToStr(alloc, T_FUN_ENUM_TO_STR, N_ENUM_TO_STR, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprEnumToStr::~ObExprEnumToStr()
{
}

int ObExprEnumToStr::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (OB_FAIL(ObEnumSetInfo::init_enum_set_info(op_cg_ctx.allocator_, rt_expr, type_,
      0, str_values_))) {
    LOG_WARN("fail to init_enum_set_info", K(ret), K(type_), K(str_values_));
  } else {
    rt_expr.eval_func_ = calc_to_str_expr;
  }
  return ret;
}

int ObExprEnumToStr::calc_to_str_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *enum_datum = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ != 2)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[1])
      || OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr.arg_cnt_ is unexpected", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, enum_datum))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (enum_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObIArray<ObString> &str_values = static_cast<ObEnumSetInfo *>(expr.extra_info_)->str_values_;
    char *buf = NULL;
    uint64_t enum_val = enum_datum->get_enum();
    int64_t element_num = str_values.count();
    uint64_t element_idx = enum_val - 1;
    ObString element_str;
    if (OB_UNLIKELY(element_num < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid element num", K(element_num), K(element_num));
    } else if (0 == enum_val) {
      // ObString empty_string;
      // res_datum.set_enumset_inner(empty_string.make_empty_string());
    } else if (OB_UNLIKELY(element_idx > element_num - 1)) {
      ret = OB_ERR_DATA_TRUNCATED;
      LOG_WARN("enum value out of range", K(element_idx), K(element_num), K(ret));
    } else {
      element_str = str_values.at(element_idx);
    }
    if (OB_SUCC(ret)) {
      ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
      if (OB_FAIL(text_result.init(element_str.length()))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.append(element_str.ptr(), element_str.length()))) {
        LOG_WARN("failed to append realdata", K(ret), K(text_result));
      } else {
        text_result.set_result();
      }
    }
  }
  return ret;
}

//////////////////////////// ObExprSetToInnerType ////////////////////////////
ObExprSetToInnerType::ObExprSetToInnerType(ObIAllocator &alloc)
    : ObExprTypeToStr(alloc, T_FUN_SET_TO_INNER_TYPE, N_SET_TO_INNER_TYPE,
                      2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprSetToInnerType::~ObExprSetToInnerType()
{
}

// types[0]  collation_type and length
// types[1]     value
int ObExprSetToInnerType::calc_result_type2(ObExprResType &type,
                                            ObExprResType &type1,
                                            ObExprResType &type2,
                                            ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type2);
  int ret = OB_SUCCESS;
  type.set_type(ObSetInnerType);
  type.set_collation_type(type1.get_collation_type());
  type.set_length(type1.get_length());
  return ret;
}

int ObExprSetToInnerType::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (OB_FAIL(ObEnumSetInfo::init_enum_set_info(op_cg_ctx.allocator_, rt_expr, type_,
      0, str_values_))) {
    LOG_WARN("fail to init_enum_set_info", K(ret), K(type_), K(str_values_));
  } else {
    rt_expr.eval_func_ = calc_to_inner_expr;
  }
  return ret;
}

int ObExprSetToInnerType::calc_to_inner_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObString &sep = ObCharsetUtils::get_const_str(expr.datum_meta_.cs_type_, ',');
  ObDatum *set_datum = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ != 2)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[1])
      || OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr.arg_cnt_ is unexpected", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, set_datum))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (set_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObIArray<ObString> &str_values = static_cast<ObEnumSetInfo *>(expr.extra_info_)->str_values_;
    const int64_t element_num = str_values.count();
    const uint64_t element_val = set_datum->get_set();
    if (OB_UNLIKELY(element_num < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid element_num", K(str_values), K(element_num), K(ret));
    } else if (OB_UNLIKELY(element_num < EFFECTIVE_COUNT)
                && OB_UNLIKELY(element_val >= (1ULL << element_num))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set value out of range", K(element_val), K(element_num));
    }

    ObSqlString sql_string;
    uint64_t index = 1ULL;
    for (int64_t i = 0;
         OB_SUCC(ret) && i < element_num && i < EFFECTIVE_COUNT && element_val >= index;
         ++i, index = index << 1) {
      if (element_val & (index)) {
        const ObString &tmp_val = str_values.at(i);
        if (OB_FAIL(sql_string.append(tmp_val))) {
          LOG_WARN("fail to deep copy str", K(element_val), K(i), K(ret));
        } else if ((element_val >= (index << 1)) && (OB_FAIL(sql_string.append(sep)))) {
          LOG_WARN("fail to deep copy comma", K(element_val), K(tmp_val), K(i), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObString string_value(sql_string.length(), sql_string.ptr());
      ObEnumSetInnerValue inner_value(element_val, string_value);
      char *buf = NULL;
      const int64_t BUF_LEN = inner_value.get_serialize_size();
      int64_t pos = 0;
      if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, BUF_LEN))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(buf), K(BUF_LEN));
      } else if (OB_FAIL(inner_value.serialize(buf, BUF_LEN, pos))) {
        LOG_WARN("failed to serialize inner_value", K(BUF_LEN), K(ret));
      } else {
        res_datum.set_enumset_inner(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

//////////////////////////// ObExprEnumToInnerType ////////////////////////////
ObExprEnumToInnerType::ObExprEnumToInnerType(ObIAllocator &alloc)
    : ObExprTypeToStr(alloc, T_FUN_ENUM_TO_INNER_TYPE, N_ENUM_TO_INNER_TYPE,
                      2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE)
{
}

ObExprEnumToInnerType::~ObExprEnumToInnerType()
{
}

// types[0]  collation_type and length
// types[1]     value
int ObExprEnumToInnerType::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprTypeCtx &type_ctx) const
{
  UNUSED(type2);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_type(ObEnumInnerType);
  type.set_collation_type(type1.get_collation_type());
  type.set_length(type1.get_length());
  return ret;
}

int ObExprEnumToInnerType::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  if (OB_FAIL(ObEnumSetInfo::init_enum_set_info(op_cg_ctx.allocator_, rt_expr, type_,
      0, str_values_))) {
    LOG_WARN("fail to init_enum_set_info", K(ret), K(type_), K(str_values_));
  } else {
    rt_expr.eval_func_ = calc_to_inner_expr;
  }
  return ret;
}

int ObExprEnumToInnerType::calc_to_inner_expr(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *enum_datum = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ != 2)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[1])
      || OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr.arg_cnt_ is unexpected", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, enum_datum))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (enum_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObIArray<ObString> &str_values = static_cast<ObEnumSetInfo *>(expr.extra_info_)->str_values_;
    const int64_t element_num = str_values.count();
    const uint64_t element_val = enum_datum->get_enum();

    int64_t element_idx = static_cast<int64_t>(element_val - 1);//enum value start from 1
    ObString element_str;
    if (OB_UNLIKELY(element_num < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("str_values_ should not be empty", K(str_values), K(ret));
    } else if (OB_UNLIKELY(0 == element_val)) {
      //do nothing just keep element_string empty
    } else if (OB_UNLIKELY(element_idx > element_num - 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid enum value", K(element_idx), K(element_num), K(element_val), K(ret));
    } else {
      element_str = str_values.at(element_idx);
    }

    if (OB_SUCC(ret)) {
      ObEnumSetInnerValue inner_value(element_val, element_str);
      const int64_t BUF_LEN = inner_value.get_serialize_size();
      int64_t pos = 0;
      char *buf = NULL;
      if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, BUF_LEN))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(buf), K(BUF_LEN));
      } else if (OB_FAIL(inner_value.serialize(buf, BUF_LEN, pos))) {
        LOG_WARN("failed to serialize inner_value", K(BUF_LEN), K(ret));
      } else {
        res_datum.set_enumset_inner(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  return ret;
}

}// sql
}// oceanbase
