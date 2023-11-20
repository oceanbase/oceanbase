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

#ifndef OCEANBASE_SQL_EXPR_FUNC_REVERSE_
#define OCEANBASE_SQL_EXPR_FUNC_REVERSE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprReverse : public ObStringExprOperator
{
public:
  explicit  ObExprReverse(common::ObIAllocator &alloc);
  virtual ~ObExprReverse();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const;
  static int do_reverse(const common::ObString &input_str,
                 const common::ObCollationType &cs_type,
                 common::ObIAllocator *allocator,
                 common::ObString &res_str);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprReverse);
};

inline int ObExprReverse::calc_result_type1(ObExprResType &type,
                                            ObExprResType &type1,
                                            common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(common::ObVarcharType);
  type1.set_calc_collation_type(type1.get_collation_type());
  if (lib::is_mysql_mode()) {
    if (ObTextType == type1.get_type()
        || ObMediumTextType == type1.get_type()
        || ObLongTextType == type1.get_type()) {
      type1.set_calc_type(type1.get_type());
      type.set_type(ObLongTextType);
      const int32_t mbmaxlen = 4;
      const int32_t default_text_length =
            ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType].get_length() / mbmaxlen;
      type.set_length(default_text_length);
    } else {
      type.set_varchar();
      if (ObTinyTextType == type1.get_type()) {
        type.set_length(OB_MAX_TINYTEXT_LENGTH - 1);
      } else {
        type.set_length(type1.get_length());
      }
    }
    ret = aggregate_charsets_for_string_result(type, &type1, 1, type_ctx.get_coll_type());
  } else {
    if (ob_is_character_type(type1.get_type(), type1.get_collation_type())
        || ob_is_varbinary_or_binary(type1.get_type(), type1.get_collation_type())
        || ObNullType == type1.get_type()) {
      type.set_type(type1.get_type());
      type.set_collation_type(type1.get_collation_type());
      type.set_collation_level(type1.get_collation_level());
      type.set_length(type1.get_length());
      type.set_length_semantics(type1.get_length_semantics());
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP,
                    ob_obj_type_str(ObCharType),
                    ob_obj_type_str(type1.get_type()));
    }
  }
  return ret;
}

}
}

#endif /* OCEANBASE_SQL_EXPR_FUNC_REVERSE_ */
