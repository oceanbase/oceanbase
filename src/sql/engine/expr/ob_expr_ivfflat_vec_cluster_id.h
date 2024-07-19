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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IVFFLAT_VEC_CLUSTER_ID_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IVFFLAT_VEC_CLUSTER_ID_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase {
namespace sql {

struct ScanTableId : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ScanTableId(common::ObIAllocator &alloc, const ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      ref_table_id_(common::OB_INVALID_ID)
  {}

  virtual ~ScanTableId() { }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const;

  int64_t ref_table_id_;
  TO_STRING_KV(K_(ref_table_id));
};

class ObExprIvfflatVecClusterId : public ObFuncExprOperator
{
public:
  explicit  ObExprIvfflatVecClusterId(common::ObIAllocator &alloc);
  virtual ~ObExprIvfflatVecClusterId();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &vec,
                                ObExprResType &ivfflat_index_table_id_type,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_ivfflat_vec_cluster_id(const ObExpr &expr, ObEvalCtx &ctx, 
                              ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIvfflatVecClusterId);
};

}
}

#endif