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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_INTEGER_CHECKER_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_INTEGER_CHECKER_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "pl/ob_pl_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprPLIntegerChecker : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  struct ExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        pl_integer_type_(pl::ObPLIntegerType::PL_INTEGER_INVALID),
        pl_integer_range_() {}
    void reset();
    virtual int deep_copy(common::ObIAllocator &allocator,
                          const ObExprOperatorType type,
                          ObIExprExtraInfo *&copied_info) const override;
    int assign(const ExtraInfo &other);
    TO_STRING_KV(K_(pl_integer_type),
                 K_(pl_integer_range));
    pl::ObPLIntegerType pl_integer_type_;
    pl::ObPLIntegerRange pl_integer_range_;
  };
public:
  explicit ObExprPLIntegerChecker(common::ObIAllocator &alloc);
  virtual ~ObExprPLIntegerChecker();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_pl_integer_checker(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int assign(const ObExprOperator &other);
  void set_pl_integer_type(pl::ObPLIntegerType type) { info_.pl_integer_type_ = type; }
  void set_range(int32_t lower, int32_t upper)
  {
    info_.pl_integer_range_.set_range(lower, upper);
  }
  template<typename T>
  static int check_range(const T &obj, const ObObjType type, int64_t range);
  static int calc(ObObj &result,
                  const ObObj &obj,
                  const pl::ObPLIntegerType &pls_type,
                  const pl::ObPLIntegerRange &pls_range,
                  ObIAllocator &calc_buf);
  static int calc(ObDatum &expr_datum,
                  const ObObjType type,
                  const ObDatum &param,
                  const pl::ObPLIntegerType &pls_type,
                  const pl::ObPLIntegerRange &pls_range,
                  ObIAllocator &calc_buf);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPLIntegerChecker);
private:
  ExtraInfo info_;
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_PL_INTEGER_CHECKER_H_ */
