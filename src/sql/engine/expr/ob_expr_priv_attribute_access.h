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

#ifndef OCEANBASE_SQL_OB_EXPR_UDT_ATTR_ACCESS_H_
#define OCEANBASE_SQL_OB_EXPR_UDT_ATTR_ACCESS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "deps/oblib/src/lib/udt/ob_udt_type.h"


namespace oceanbase
{
namespace sql
{

struct ObExprUdtAttrAccessInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprUdtAttrAccessInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
      udt_id_(OB_INVALID_ID), attr_type_(OB_INVALID_ID)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);

  uint64_t udt_id_;
  uint64_t attr_type_; // attribute type
};

class ObExprUDTAttributeAccess : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprUDTAttributeAccess(common::ObIAllocator &alloc);
  virtual ~ObExprUDTAttributeAccess();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_attr_access(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int get_udt_meta_by_udt_id(uint64_t udt_id, ObSqlUDTMeta &udt_meta);
  static int eval_sdo_geom_attr_access(ObIAllocator &allocator, const ObString &swkb, const int32_t attr_idx, ObDatum &res);
  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }
  uint64_t get_udt_id() { return udt_id_; }
  inline void set_attribute_type(uint64_t attr_type) { attr_type_ = attr_type; }
  uint64_t get_attribute_type() { return attr_type_; }

private:
  uint64_t udt_id_;
  uint64_t attr_type_; // attribute type

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUDTAttributeAccess);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_UDT_ATTR_ACCESS_H_
