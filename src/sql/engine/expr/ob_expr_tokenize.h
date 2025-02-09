/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_
#define _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "storage/fts/ob_fts_parser_property.h"

namespace oceanbase
{
namespace sql
{
class MultimodeAlloctor;
class ObExprTokenize : public ObStringExprOperator
{
public:
  explicit ObExprTokenize(common::ObIAllocator &alloc);
  ~ObExprTokenize() override;
  /**
   * @brief evaluate function
   * @param expr expression
   * @param ctx expression evaluation context
   * @param expr_datum expression result
   * @note see cg_expr REG_OP and g_expr_eval_functions
   */
  static int eval_tokenize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  int calc_result_typeN(ObExprResType &type,
                        ObExprResType *types,
                        int64_t param_num,
                        common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  struct TokenizeParam
  {
  public:
    static constexpr const char *CASE_INDICATOR_STR = "case";
    static constexpr const char *OUTPUT_MODE_STR = "output";
    static constexpr const char *STOPWORDS_LIST_STR = "stopwords";
    static constexpr const char *ADDITIONAL_ARGS_STR = "additional_args";

  public:
    TokenizeParam();

    int parse_json_param(const ObIJsonBase *obj);

    // check and reform parser properties to standard format
    int reform_parser_properties(const ObString &properties);

  public:
    // for property and tmp json string
    mutable ObArenaAllocator allocator_;
    ObString parser_name_;
    ObString properties_;
    ObCollationType cs_type_;
    ObString fulltext_;
    enum OUTPUT_MODE
    {
      DEFAULT,
      ALL,
    } output_mode_;
  };

private:
  static int parse_param(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         common::ObArenaAllocator &allocator,
                         TokenizeParam &param);

  static int parse_fulltext(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param);
  static int parse_parser_name(const ObExpr &expr, ObEvalCtx &ctx, TokenizeParam &param);
  static int parse_parser_properties(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     MultimodeAlloctor &mm_alloc,
                                     TokenizeParam &param);

  static int tokenize_fulltext(const TokenizeParam &param,
                               TokenizeParam::OUTPUT_MODE mode,
                               common::ObIAllocator &allocator,
                               ObIJsonBase *&result);

  static int construct_ft_parser_inner_name(const ObString &input_str, TokenizeParam &param);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTokenize);
};

} // namespace sql
} // namespace oceanbase

#endif // _OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TOKENIZE_H_
