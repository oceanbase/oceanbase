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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_

#include "lib/container/ob_array.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprSubstringIndex : public ObStringExprOperator {
public:
  explicit ObExprSubstringIndex(common::ObIAllocator& alloc);
  virtual ~ObExprSubstringIndex();
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& str, ObExprResType& delim, ObExprResType& count,
      common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result3(common::ObObj& result, const common::ObObj& str, const common::ObObj& delim,
      const common::ObObj& count, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  static int eval_substring_index(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  /**
   * find m_count apperance of m_delim in m_str(from left or from right)
   * @param[in] result      calculated result
   * parma[in] m_str        string
   * @param[in] m_delim     substring
   * @param[in] m_count     nth apperance
   * @return                position of nth apperance of delim
   */
  static int string_search(
      common::ObString& result, const common::ObString& m_str, const common::ObString& m_delim, const int64_t m_count);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubstringIndex);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SUBSTRING_INDEX_
