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

#ifndef OCEANBASE_EXPR_TIMESTAMP_H
#define OCEANBASE_EXPR_TIMESTAMP_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_to_temporal_base.h"

namespace oceanbase
{
namespace sql
{
//note: this is oracle only function
class ObExprSysTimestamp: public ObFuncExprOperator
{
public:
  explicit ObExprSysTimestamp(common::ObIAllocator &alloc);
  virtual ~ObExprSysTimestamp();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_systimestamp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSysTimestamp);
};

//note: this is oracle only function
class ObExprLocalTimestamp: public ObFuncExprOperator
{
public:
  explicit ObExprLocalTimestamp(common::ObIAllocator &alloc);
  virtual ~ObExprLocalTimestamp();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_localtimestamp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLocalTimestamp);
};

//note: this is oracle only function
class ObExprToTimestamp: public ObExprToTemporalBase
{
public:
  explicit ObExprToTimestamp(common::ObIAllocator &alloc)
    : ObExprToTemporalBase(alloc, T_FUN_SYS_TO_TIMESTAMP, N_TO_TIMESTAMP)
  {}
  virtual ~ObExprToTimestamp() {}

  int set_my_result_from_ob_time(common::ObExprCtx &expr_ctx,
                                 common::ObTime &ob_time,
                                 common::ObObj &result) const;
  common::ObObjType get_my_target_obj_type() const
  {
    return common::ObTimestampNanoType;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToTimestamp);
};

//note: this is oracle only function
class ObExprToTimestampTZ: public ObExprToTemporalBase
{
public:
  explicit ObExprToTimestampTZ(common::ObIAllocator &alloc)
    : ObExprToTemporalBase(alloc, T_FUN_SYS_TO_TIMESTAMP_TZ, N_TO_TIMESTAMP_TZ)
  {}
  virtual ~ObExprToTimestampTZ() {}

  int set_my_result_from_ob_time(common::ObExprCtx &expr_ctx,
                                 common::ObTime &ob_time,
                                 common::ObObj &result) const;
  common::ObObjType get_my_target_obj_type() const
  {
    return common::ObTimestampTZType;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToTimestampTZ);
};

class ObExprTimestamp : public ObFuncExprOperator
{
public:
  explicit ObExprTimestamp(common::ObIAllocator &alloc);
  virtual ~ObExprTimestamp();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual common::ObCastMode get_cast_mode() const { return CM_NULL_ON_WARN;}
  static int calc_timestamp1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
  static int calc_timestamp2(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimestamp);
};

}
}

#endif // OCEANBASE_EXPR_TIMESTAMP_H
