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

#ifndef _OB_SQL_EXPR_ORACLE_DECODE_H_
#define _OB_SQL_EXPR_ORACLE_DECODE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprOracleDecode : public ObExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObExprOracleDecode(common::ObIAllocator &alloc);
  virtual ~ObExprOracleDecode();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  int no_cast_calc(common::ObObj &result,
                   const common::ObObj *objs_stack,
                   int64_t param_num) const;
  int calc_result_type_for_literal(ObExprResType &type,
                                   ObExprResType *types_stack,
                                   int64_t param_num,
                                   common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const override;
  static int eval_decode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
public:
  inline void set_cond_all_same_meta(bool all_same_meta);
  inline void set_val_all_same_meta(bool all_same_meta);
  static bool can_compare_directly(common::ObObjType type1, common::ObObjType type2);
private:
  inline static int get_cmp_type(common::ObObjType &type, const common::ObObjType &type1,
                                 const common::ObObjType &type2, const common::ObObjType &type3);
  inline bool is_cond_all_same_meta() const;
  inline bool is_val_all_same_meta() const;
  inline bool need_no_cast() const;
private:
  typedef uint32_t ObExprDecodeParamFlag;
  static const uint32_t COND_ALL_SAME_META = 1U << 0;
  static const uint32_t VAL_ALL_SAME_META = 1U << 1;
  ObExprDecodeParamFlag param_flags_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleDecode);
};

inline void ObExprOracleDecode::set_cond_all_same_meta(bool all_same_meta)
{
  if (all_same_meta) {
    param_flags_ |= COND_ALL_SAME_META;
  } else {
    param_flags_ &= ~COND_ALL_SAME_META;
  }
}

inline void ObExprOracleDecode::set_val_all_same_meta(bool all_same_meta)
{
  if (all_same_meta) {
    param_flags_ |= VAL_ALL_SAME_META;
  } else {
    param_flags_ &= ~VAL_ALL_SAME_META;
  }
}

inline bool ObExprOracleDecode::is_cond_all_same_meta() const
{
  return param_flags_ & COND_ALL_SAME_META;
}

inline bool ObExprOracleDecode::is_val_all_same_meta() const
{
  return param_flags_ & VAL_ALL_SAME_META;
}

inline bool ObExprOracleDecode::need_no_cast() const
{
  return is_cond_all_same_meta() && is_val_all_same_meta();
}

}
}
#endif /* _OB_SQL_EXPR_ORACLE_DECODE_H_ */
