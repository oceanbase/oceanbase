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

#ifndef _OB_EXPR_CAST_H_
#define _OB_EXPR_CAST_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

// TODO::only support 26??
static const int32_t CAST_STRING_DEFUALT_LENGTH[26] = {
    0,   // null
    4,   // tinyint
    6,   // smallint
    9,   // medium
    11,  // int32
    20,  // int
    3,   // utinyint
    5,   // usmallint
    8,   // umedium
    10,  // uint32
    20,  // uint
    12,  // float
    23,  // double  here we set it larger than mysql, mysql is 22, sometimes it is 23,for safely.we set 23.
    12,  // ufloat
    23,  // udouble
    11,  // decimal
    11,  // decimal
    19,  // datetime
    19,  // timestamp
    10,  // date
    10,  // time
    4,   // year
    1,   // varchar
    1,   // char
    1,   // binary
    0    // extend
};

class ObExprCast : public ObFuncExprOperator {
  OB_UNIS_VERSION_V(1);
  const static int32_t OB_LITERAL_MAX_INT_LEN = 21;

public:
  ObExprCast();
  explicit ObExprCast(common::ObIAllocator& alloc);
  virtual ~ObExprCast(){};

  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  // extra_serialize_ == 1 : is implicit cast
  void set_implicit_cast(bool v)
  {
    extra_serialize_ = v ? 1 : 0;
  }

private:
  int check_target_type_precision_valid(const ObExprResType& type, const common::ObCastMode cast_mode) const;
  int get_cast_type(const ObExprResType param_type2, ObExprResType& dst_type) const;
  int get_explicit_cast_cm(const ObExprResType& src_type, const ObExprResType& dst_type,
      const ObSQLSessionInfo& session, const ObRawExpr& cast_raw_expr, common::ObCastMode& cast_mode) const;

private:
  int get_cast_string_len(ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx, int32_t& res_len,
      int16_t& length_semantics, common::ObCollationType conn) const;
  int get_cast_inttc_len(ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx, int32_t& res_len,
      int16_t& length_semantics, common::ObCollationType conn) const;
  int do_implicit_cast(common::ObExprCtx& expr_ctx, const common::ObCastMode& cast_mode,
      const common::ObObjType& dst_type, const common::ObObj& src_obj, common::ObObj& dst_obj) const;
  // disallow copy
  ObExprCast(const ObExprCast& other);
  ObExprCast& operator=(const ObExprCast& other);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_EXPR_CAST_H_ */
