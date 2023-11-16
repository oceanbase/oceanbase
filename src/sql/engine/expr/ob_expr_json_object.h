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
 * This file contains declare of the json_object.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_OBJECT_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_OBJECT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/resolver/dml/ob_select_resolver.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonObject : public ObFuncExprOperator
{
public:
  explicit ObExprJsonObject(common::ObIAllocator &alloc);
  virtual ~ObExprJsonObject();
  virtual int calc_result_typeN(ObExprResType& type, ObExprResType* types, int64_t param_num, 
    common::ObExprTypeCtx& type_ctx) const override;
  static int eval_json_object(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_ora_json_object(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int set_result(ObObjType dst_type, ObString str_res, common::ObIAllocator *allocator,
                        ObEvalCtx &ctx, const ObExpr &expr, ObDatum &res, uint8_t strict_type,
                        uint8_t unique_type);
  static int check_key_valid(common::hash::ObHashSet<ObString> &view_key_names, const ObString &key_name);
private:

  /* process null */
  const static int64_t OB_JSON_ON_NULL_NUM = 3;
  const static uint8_t OB_JSON_ON_NULL_NULL         = 1;
  const static uint8_t OB_JSON_ON_NULL_ABSENT       = 2;
  const static uint8_t OB_JSON_ON_NULL_IMPLICIT     = 0;

  /* process format strict */
  const static int64_t OB_JSON_ON_STRICT_NUM = 2;
  const static uint8_t OB_JSON_ON_STRICT_USE          = 1;
  const static uint8_t OB_JSON_ON_STRICT_IMPLICIT     = 0;

  /* process unique */
  const static int64_t OB_JSON_ON_UNIQUE_NUM = 2;
  const static uint8_t OB_JSON_ON_UNIQUE_USE          = 1;
  const static uint8_t OB_JSON_ON_UNIQUE_IMPLICIT     = 0;

  static int eval_option_clause_value(ObExpr *expr,
                                      ObEvalCtx &ctx,
                                      uint8_t &type,
                                      int64_t size_para);

  static int get_ora_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                          uint16_t index, ObDatum*& j_datum,
                          bool &is_null);

private:  
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonObject);
};

// mock inner expr as json object with star node
class ObExprJsonObjectStar : public ObFuncExprOperator
{
public:
  explicit ObExprJsonObjectStar(common::ObIAllocator &alloc);
  virtual ~ObExprJsonObjectStar();
  virtual int calc_result_typeN(ObExprResType& type, ObExprResType* types, int64_t param_num,
    common::ObExprTypeCtx& type_ctx) const override;
  static int eval_ora_json_object_star(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonObjectStar);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_JSON_OBJECT_H_