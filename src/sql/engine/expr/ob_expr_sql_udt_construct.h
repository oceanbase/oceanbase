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

#ifndef OCEANBASE_SQL_OB_EXPR_UDT_CONSTRUCT_H_
#define OCEANBASE_SQL_OB_EXPR_UDT_CONSTRUCT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "deps/oblib/src/lib/udt/ob_udt_type.h"


namespace oceanbase
{
namespace sql
{

struct ObExprUdtConstructInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprUdtConstructInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
      udt_id_(OB_INVALID_ID), root_udt_id_(OB_INVALID_ID), attr_pos_(0)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);

  uint64_t udt_id_;
  uint64_t root_udt_id_;
  uint64_t attr_pos_;

};

class ObExprUdtConstruct : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprUdtConstruct(common::ObIAllocator &alloc);
  virtual ~ObExprUdtConstruct();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_udt_construct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_sdo_geom_udt_access(ObIAllocator &allocator, const ObExpr &expr, ObEvalCtx &ctx,
                                      const ObString &swkb, uint64_t udt_id, ObDatum &res);
  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }
  inline void set_root_udt_id(uint64_t udt_id) { root_udt_id_ = udt_id; }
  inline void set_attribute_pos(uint64_t attr_pos) { attr_pos_ = attr_pos; }
private:
  static uint64_t caculate_udt_data_length(const ObExpr &expr, ObEvalCtx &ctx);
  static int fill_udt_res_values(const ObExpr &expr, ObEvalCtx &ctx, ObSqlUDT &udt_obj,
                                 bool is_sub_udt, ObObj &nested_udt_bitmap_obj);
  static int fill_sub_udt_nested_bitmap(ObSqlUDT &udt_obj, ObObj &nested_udt_bitmap_obj);
  uint64_t udt_id_;
  uint64_t root_udt_id_;
  uint64_t attr_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUdtConstruct);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_UDT_CONSTRUCT_H_
