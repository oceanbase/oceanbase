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

#ifndef _OB_SQL_EXPR_INNER_INFO_COLS_PRINTER_H_
#define _OB_SQL_EXPR_INNER_INFO_COLS_PRINTER_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_priv_type.h"

namespace oceanbase
{
namespace sql
{

class ObExprInnerInfoColsColumnDefPrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsColumnDefPrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsColumnDefPrinter();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_def(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsColumnDefPrinter);
};

class ObExprInnerInfoColsCharLenPrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsCharLenPrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsCharLenPrinter();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_char_len(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsCharLenPrinter);
};

class ObExprInnerInfoColsCharNamePrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsCharNamePrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsCharNamePrinter();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_char_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsCharNamePrinter);
};


class ObExprInnerInfoColsCollNamePrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsCollNamePrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsCollNamePrinter();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_collation_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsCollNamePrinter);
};

class ObExprInnerInfoColsPrivPrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsPrivPrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsPrivPrinter();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_priv(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int fill_col_privs(share::schema::ObSchemaGetterGuard &schema_guard,
                            const share::schema::ObSessionPrivInfo &session_priv,
                            const common::ObIArray<uint64_t> &enable_role_id_array,
                            share::schema::ObNeedPriv &need_priv, 
                            ObPrivSet priv_set, 
                            const char *priv_str,
                            char* buf,
                            const int64_t buf_len,
                            int64_t &pos);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsPrivPrinter);
};

class ObExprInnerInfoColsExtraPrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsExtraPrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsExtraPrinter();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_extra(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsExtraPrinter);
};

class ObExprInnerInfoColsDataTypePrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsDataTypePrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsDataTypePrinter();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_data_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsDataTypePrinter);
};

class ObExprInnerInfoColsColumnTypePrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsColumnTypePrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsColumnTypePrinter();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_column_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsColumnTypePrinter);
};

class ObExprInnerInfoColsColumnKeyPrinter : public ObExprOperator
{
public:
  explicit ObExprInnerInfoColsColumnKeyPrinter(common::ObIAllocator &alloc);
  virtual ~ObExprInnerInfoColsColumnKeyPrinter();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_column_column_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerInfoColsColumnKeyPrinter);
};

}
}
#endif /* _OB_SQL_EXPR_INNER_INFO_COLS_PRINTER_H_ */
