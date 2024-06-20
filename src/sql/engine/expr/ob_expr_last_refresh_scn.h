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

#ifndef OCEANBASE_SQL_OB_EXPR_LAST_REFRESH_SCN_H_
#define OCEANBASE_SQL_OB_EXPR_LAST_REFRESH_SCN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLastRefreshScn : public ObFuncExprOperator
{
public:
  explicit  ObExprLastRefreshScn(common::ObIAllocator &alloc);
  virtual ~ObExprLastRefreshScn();

  struct LastRefreshScnExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    LastRefreshScnExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
        : ObIExprExtraInfo(alloc, type),
          mview_id_(share::OB_INVALID_SCN_VAL)
    {
    }
    virtual ~LastRefreshScnExtraInfo() { }
    virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
    uint64_t mview_id_;
  };

  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_last_refresh_scn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int set_last_refresh_scns(const ObIArray<uint64_t> &src_mview_ids,
                                   ObMySQLProxy *sql_proxy,
                                   ObSQLSessionInfo *session,
                                   const share::SCN &scn,
                                   ObIArray<uint64_t> &mview_ids,
                                   ObIArray<uint64_t> &last_refresh_scns);
  static int get_last_refresh_scn_sql(const share::SCN &scn,
                                      const ObIArray<uint64_t> &mview_ids,
                                      ObSqlString &sql);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprLastRefreshScn);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_CURRENT_SCN_H_
