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

#ifndef _OB_EXPR_NEXTVAL_H
#define _OB_EXPR_NEXTVAL_H
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "share/ob_autoincrement_service.h"

namespace oceanbase
{
namespace share
{
}
namespace sql
{
class ObPhysicalPlanCtx;
class ObExprAutoincNextval : public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
  explicit  ObExprAutoincNextval(common::ObIAllocator &alloc);
  ObExprAutoincNextval(
      common::ObIAllocator &alloc,
      ObExprOperatorType type,
      const char *name,
      int32_t param_num,
      ObValidForGeneratedColFlag valid_for_generated_col,
      int32_t dimension,
      bool is_internal_for_mysql = false,
      bool is_internal_for_oracle = false);
  virtual ~ObExprAutoincNextval();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nextval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int get_uint_value(const ObExpr &input_expr, ObDatum *input_value,
                            bool &is_zero, uint64_t &casted_value);
private:
  //check to generate auto-inc value or not and cast.
  static int check_and_cast(common::ObObj &result,
                            common::ObObjType result_type,
                            const common::ObObj *objs_array,
                            int64_t param_num,
                            common::ObExprCtx &expr_ctx,
                            share::AutoincParam *autoinc_param,
                            bool &is_to_generate,
                            uint64_t &casted_value);
  static int generate_autoinc_value(const ObSQLSessionInfo &my_session,
                                    uint64_t &new_val,
                                    share::ObAutoincrementService &auto_service,
                                    share::AutoincParam *autoinc_param,
                                    ObPhysicalPlanCtx *plan_ctx);

  // get the input value && check need generate
  static int get_input_value(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum *input_value,
                             share::AutoincParam &autoinc_param,
                             bool &is_to_generate,
                             uint64_t &casted_value);
  static int get_casted_value_by_result_type(common::ObCastCtx &cast_ctx,
                                             common::ObObjType result_type,
                                             const common::ObObj &param,
                                             uint64_t &casted_value,
                                             bool &try_sync);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAutoincNextval);

};

// used in expr.extra_ to pass in table and column info
struct ObAutoincNextvalExtra
{
  OB_UNIS_VERSION_V(1);
public:
  ObAutoincNextvalExtra()
    : autoinc_table_id_(common::OB_INVALID_ID),
      autoinc_col_id_(common::OB_INVALID_ID),
      autoinc_table_name_(),
      autoinc_column_name_()
  {}
  virtual ~ObAutoincNextvalExtra() {}

  static int init_autoinc_nextval_extra(common::ObIAllocator *allocator,
                                        ObRawExpr *&expr,
                                        const uint64_t autoinc_table_id,
                                        const uint64_t autoinc_col_id,
                                        const ObString autoinc_table_name,
                                        const ObString autoinc_column_name);

  TO_STRING_KV(K_(autoinc_table_id),
               K_(autoinc_col_id),
               K_(autoinc_table_name),
               K_(autoinc_column_name));

  uint64_t autoinc_table_id_;
  uint64_t autoinc_col_id_;
  ObString autoinc_table_name_;
  ObString autoinc_column_name_;
};

// used in ObExprAutoincNextval to pass in table and column info
struct ObAutoincNextvalInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObAutoincNextvalInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
    : ObIExprExtraInfo(alloc, type),
      autoinc_table_id_(common::OB_INVALID_ID),
      autoinc_col_id_(common::OB_INVALID_ID)
  {}

  virtual ~ObAutoincNextvalInfo() { }

  static int init_autoinc_nextval_info(common::ObIAllocator *allocator,
                                       const ObRawExpr &raw_expr,
                                       ObExpr &expr,
                                       const ObExprOperatorType type);

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  TO_STRING_KV(K_(autoinc_table_id), K_(autoinc_col_id));

  uint64_t autoinc_table_id_;
  uint64_t autoinc_col_id_;
};

}//end namespace sql
}//end namespace oceanbase
#endif
