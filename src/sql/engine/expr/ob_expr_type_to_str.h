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

#ifndef _OB_SQL_EXPR_TYPE_TO_STR_H_
#define _OB_SQL_EXPR_TYPE_TO_STR_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace sql
{
class ObExprResType;
class ObExprTypeToStr : public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
    ObExprTypeToStr(common::ObIAllocator &alloc, ObExprOperatorType type,
                    const char *name, int32_t param_num, ObValidForGeneratedColFlag valid_for_generated_col, int32_t dimension,
                    bool is_internal_for_mysql = false,
                    bool is_internal_for_oracle = false)
      : ObFuncExprOperator(alloc, type, name, param_num, valid_for_generated_col, dimension, is_internal_for_mysql, is_internal_for_oracle),
      alloc_(alloc), str_values_(alloc)
  {
    disable_operand_auto_cast();
  };
  virtual ~ObExprTypeToStr() {}
  virtual inline void reset();
  virtual int assign(const ObExprOperator &other);

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual const common::ObIArray<common::ObString> &get_str_values() const {return str_values_;}
  virtual int shallow_copy_str_values(const common::ObIArray<common::ObString> &str_values);
  //this func is used in code generator
  virtual int deep_copy_str_values(const common::ObIArray<common::ObString> &str_values);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const = 0;
protected:
  int deep_copy_str(const common::ObString &src_str, char *dest_buf, int64_t buf_len, int64_t &pos) const;
public:
  static const int64_t EFFECTIVE_COUNT = 64;
protected:
  common::ObIAllocator &alloc_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> str_values_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTypeToStr) const;
};

inline void ObExprTypeToStr::reset()
{
  ObExprOperator::reset();
  str_values_.reset();
}

struct ObEnumSetInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObEnumSetInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      cast_mode_(0), str_values_(alloc)
  {}

 virtual ~ObEnumSetInfo() { str_values_.destroy(); }

  inline void set_allocator(common::ObIAllocator *alloc)
  {
    str_values_.set_allocator(alloc);
  }
  static int init_enum_set_info(common::ObIAllocator *allocator,
                                ObExpr &expr,
                                const ObExprOperatorType type,
                                const uint64_t cast_mode,
                                const common::ObIArray<common::ObString> &str_values);

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
  TO_STRING_KV(K_(cast_mode), K_(str_values));

  uint64_t cast_mode_;
  ObStrValues str_values_;
};

class ObExprSetToStr : public ObExprTypeToStr
{
public:
  explicit ObExprSetToStr(common::ObIAllocator &alloc);
  virtual ~ObExprSetToStr();
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_str_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSetToStr) const;
};

class ObExprEnumToStr : public ObExprTypeToStr
{
public:
  explicit ObExprEnumToStr(common::ObIAllocator &alloc);
  virtual ~ObExprEnumToStr();
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_str_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprEnumToStr) const;
};

class ObExprSetToInnerType : public ObExprTypeToStr
{
public:
  explicit ObExprSetToInnerType(common::ObIAllocator &alloc);
  virtual ~ObExprSetToInnerType();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_inner_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSetToInnerType) const;
};

class ObExprEnumToInnerType : public ObExprTypeToStr
{
public:
  explicit ObExprEnumToInnerType(common::ObIAllocator &alloc);
  virtual ~ObExprEnumToInnerType();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_inner_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprEnumToInnerType) const;
};

} // sql
} // oceanbase
#endif
