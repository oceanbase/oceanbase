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

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_SEARCH_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_SEARCH_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

typedef common::ObVector<ObJsonBuffer *> ObJsonBufferVector;
typedef common::ObSortedVector<ObJsonBuffer *> ObJsonBufferSortedVector;

class ObExprJsonSearch : public ObFuncExprOperator
{
public:
  explicit ObExprJsonSearch(common::ObIAllocator &alloc);
  virtual ~ObExprJsonSearch();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_json_search(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int add_path_unique(const ObJsonBuffer* path, ObJsonBufferSortedVector &duplicates, ObJsonBufferVector &hits);
  static int path_add_key(ObJsonBuffer &path, ObString key);
  static int find_matches(common::ObIAllocator *allocator,
                          const ObIJsonBase *j_base,
                          ObJsonBuffer &path,
                          ObJsonBufferVector &hits,
                          ObJsonBufferSortedVector &duplicates,
                          const ObString &target,
                          bool one_match,
                          const int32_t &escape_wc);
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonSearch);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_SEARCH_H_