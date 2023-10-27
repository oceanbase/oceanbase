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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_statement_digest.h"

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/ob_encryption_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/ob_resolver_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace sql
{

int calc_digest_text(ObIAllocator &allocator,
                     const ObString sql_str,
                     const ObCollationType cs_type,
                     ObSQLSessionInfo *session,
                     share::schema::ObSchemaGetterGuard *schema_guard,
                     ObString &digest_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator(ObModIds::OB_SQL_EXPR_CALC);
  HEAP_VAR(ObExecContext, exec_ctx, temp_allocator) {
    uint64_t tenant_id = session->get_effective_tenant_id();
    ObPhysicalPlanCtx phy_plan_ctx(allocator);
    exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
    exec_ctx.set_my_session(session);
    exec_ctx.set_mem_attr(ObMemAttr(tenant_id, ObModIds::OB_SQL_EXEC_CONTEXT, ObCtxIds::EXECUTE_CTX_ID));
    ObSqlCtx sql_ctx;
    sql_ctx.session_info_ = session;
    sql_ctx.schema_guard_ = schema_guard;
    ObPlanCacheCtx pc_ctx(sql_str, PC_TEXT_MODE, allocator, sql_ctx, exec_ctx,
                          session->get_effective_tenant_id());
    ObCharsets4Parser charsets4parser = session->get_charsets4parser();
    charsets4parser.string_collation_ = cs_type;
    ObParser parser(allocator, session->get_sql_mode(), charsets4parser);
    ObSEArray<ObString, 1> queries;
    ObMPParseStat parse_stat;
    if (OB_FAIL(parser.split_multiple_stmt(sql_str, queries, parse_stat))) {
      LOG_WARN("failed to split multiple stmt", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < queries.count(); ++i) {
      ParseResult parse_result;
      ParamStore tmp_params((ObWrapperAllocator(allocator)));
      stmt::StmtType stmt_type = stmt::T_NONE;
      ObItemType item_type = T_NULL;
      if (OB_FAIL(parser.parse(queries.at(i), parse_result))) {
        LOG_WARN("fail to parse sql str", K(sql_str), K(ret));
      } else if (OB_ISNULL(parse_result.result_tree_)
                || OB_ISNULL(parse_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected parse result", K(ret));
      } else if (FALSE_IT(item_type = parse_result.result_tree_->children_[0]->type_)) {
      } else if (i > 0) {
        if (OB_UNLIKELY(T_EMPTY_QUERY != item_type)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid stmt type", K(item_type), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "digest function");
        }
      } else if (OB_UNLIKELY(T_EMPTY_QUERY == item_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid empty query", K(item_type), K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "digest function");
      } else if (OB_FAIL(ObResolverUtils::resolve_stmt_type(parse_result, stmt_type))) {
        LOG_WARN("failed to resolve stmt type", K(ret));
      } else if (ObStmt::is_dml_stmt(stmt_type) && !ObStmt::is_show_stmt(stmt_type)) {
        if (OB_UNLIKELY(parse_result.result_tree_->children_[0]->value_ > 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("query contains questionmark", K(queries.at(i)), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "digest function");
        } else if (OB_FAIL(ObSqlParameterization::parameterize_syntax_tree(allocator,
                                                                    true,
                                                                    pc_ctx,
                                                                    parse_result.result_tree_,
                                                                    tmp_params,
                                                                    charsets4parser))) {
          LOG_WARN("fail to parameterize syntax tree", K(sql_str), K(ret));
        } else {
          digest_str = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_;
        }
      } else {
        digest_str = queries.at(i);
      }
    }
    exec_ctx.set_physical_plan_ctx(NULL);
  }
  return ret;
}

ObExprStatementDigest::ObExprStatementDigest(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_STATEMENT_DIGEST, N_STATEMENT_DIGEST, 1, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprStatementDigest::~ObExprStatementDigest()
{
}

int ObExprStatementDigest::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType tmp_type;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(CS_LEVEL_COERCIBLE);
  type.set_length(OB_MAX_SQL_ID_LENGTH);
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(tmp_type, &type1, 1, type_ctx.get_coll_type()));
  OX (type1.set_calc_collation_type(tmp_type.get_collation_type()));
  OX (type1.set_calc_collation_level(tmp_type.get_collation_level()));
  return ret;
}

int ObExprStatementDigest::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprStatementDigest::eval_statement_digest;
  return ret;
}

int ObExprStatementDigest::eval_statement_digest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session()) ||
      OB_ISNULL(ctx.exec_ctx_.get_sql_ctx()) ||
      OB_ISNULL(schema_guard = ctx.exec_ctx_.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else{
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObString sql_str = arg->get_string();
    ObString digest_str;
    ObString hash_str;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    char *buf = NULL;
    if (OB_FAIL(calc_digest_text(calc_alloc, sql_str, expr.args_[0]->datum_meta_.cs_type_,
                                 session, schema_guard, digest_str))) {
      LOG_WARN("fail to calc statement digest text", K(sql_str), K(ret));
    } else if (OB_FAIL(ObHashUtil::hash(OB_HASH_MD5, digest_str, calc_alloc, hash_str))) {
      LOG_WARN("fail to calc md5", K(digest_str), K(ret));
    } else if (OB_FAIL(ObDatumHexUtils::hex(expr, hash_str, ctx, calc_alloc, expr_datum))) {
      LOG_WARN("fail to convert hash_str to hex", K(ret));
    }
  }
  return ret;
}

ObExprStatementDigestText::ObExprStatementDigestText(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_STATEMENT_DIGEST_TEXT, N_STATEMENT_DIGEST_TEXT, 1, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprStatementDigestText::~ObExprStatementDigestText()
{
}

int ObExprStatementDigestText::calc_result_type1(ObExprResType &type,
                                 ObExprResType &type1,
                                 common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_clob();
  type1.set_calc_type(ObVarcharType);
  OZ (aggregate_charsets_for_string_result(type, &type1, 1, type_ctx.get_coll_type()));
  OX (type1.set_calc_collation_type(type.get_collation_type()));
  OX (type1.set_calc_collation_level(type.get_collation_level()));
  type.set_length(OB_MAX_MYSQL_VARCHAR_LENGTH);
  return ret;
}

int ObExprStatementDigestText::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprStatementDigestText::eval_statement_digest_text;
  return ret;
}

int ObExprStatementDigestText::eval_statement_digest_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  ObSQLSessionInfo *session = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session()) ||
      OB_ISNULL(ctx.exec_ctx_.get_sql_ctx()) ||
      OB_ISNULL(schema_guard = ctx.exec_ctx_.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_null();
  } else{
    // result must be clob
    ObString sql_str = arg->get_string();
    ObString digest_str;
    ObString res_str;
    const ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    const ObCollationType res_cs_type = expr.datum_meta_.cs_type_;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    if (OB_FAIL(calc_digest_text(calc_alloc, sql_str, in_cs_type, session,
                                 schema_guard, digest_str))) {
      LOG_WARN("fail to calc statement digest text", K(sql_str), K(ret));
    } else if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(calc_alloc, digest_str, res_str,
        in_cs_type, res_cs_type))) {
      LOG_WARN("fail to check need_convert_string_collation", K(ret));
    } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(expr, ctx, expr_datum, res_str))) {
      LOG_WARN("fail to convert result to temporary lob", K(ret));
    }
  }
  return ret;
}

}
}
