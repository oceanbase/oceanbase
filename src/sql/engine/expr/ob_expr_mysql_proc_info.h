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

#ifndef _OB_EXPR_MYSQL_PROC_INFORMATION_H
#define _OB_EXPR_MYSQL_PROC_INFORMATION_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/container/ob_iarray.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

class ObExprMysqlProcInfo : public ObStringExprOperator {
private:
  enum MysqlProcInfoField {
    PARAM_LIST = 0,
    RETURNS,
    BODY,
    SQL_MODE,
    CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION,
    DB_COLLATION,
  };
public:
  ObExprMysqlProcInfo();
  explicit ObExprMysqlProcInfo(common::ObIAllocator& alloc);
  virtual ~ObExprMysqlProcInfo();

  static int eval_mysql_proc_info(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int get_param_list_info(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum,
                                 uint64_t routine_id);
  static int get_param_list_info(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum,
                                 ObString &routine_body,
                                 ObString &exec_env_str,
                                 uint64_t routine_id);
  static int get_returns_info(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObDatum &expr_datum,
                              uint64_t routine_id);

  static int get_returns_info(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObDatum &expr_datum,
                              int64_t param_type,
                              int64_t param_length,
                              int64_t param_precision,
                              int64_t param_scale,
                              int64_t param_coll_type);
  static int get_body_info(const ObExpr &expr,
                           ObEvalCtx &ctx,
                           ObDatum &expr_datum,
                           uint64_t routine_id);
  static int get_body_info(const ObExpr &expr,
                           ObEvalCtx &ctx,
                           ObDatum &expr_datum,
                           ObString &routine_body,
                           ObString &exec_env_str);

  static int get_info_by_field_id(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum,
                                  uint64_t routine_id,
                                  uint64_t field_id);

  static int get_info_by_field_id(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum,
                                  ObString &routine_body,
                                  uint64_t field_id);
  static int get_routine_info(ObSQLSessionInfo *session,
                              uint64_t routine_id,
                              const ObRoutineInfo *&routine_info);
  static int set_return_result(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &expr_datum,
                               ObString &value_str);
  static int extract_create_node_from_routine_info(ObIAllocator &alloc,
                                                   const ObRoutineInfo &routine_info,
                                                   const sql::ObExecEnv &exec_env,
                                                   ParseNode *&create_node);

  static int extract_create_node_from_routine_info(ObIAllocator &alloc,
                                                   const ObString &routine_body,
                                                   const sql::ObExecEnv &exec_env,
                                                   ParseNode *&create_node);
  static int calc_mysql_proc_info_arg_cnt_2(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &expr_datum);
  static int calc_mysql_proc_info_arg_cnt_9(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMysqlProcInfo);
};

} // end namespace sql
} // end namespace oceanbase
#endif
