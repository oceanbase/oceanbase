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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql.h"
#include <sys/time.h>
#include <openssl/md5.h>
#include "lib/string/ob_sql_string.h"
#include "lib/timezone/ob_time_convert.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_i_tablet_scan.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/printer/ob_select_stmt_printer.h"
#include "sql/printer/ob_insert_all_stmt_printer.h"
#include "sql/printer/ob_insert_stmt_printer.h"
#include "sql/printer/ob_update_stmt_printer.h"
#include "sql/printer/ob_delete_stmt_printer.h"
#include "sql/printer/ob_merge_stmt_printer.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/optimizer/ob_route_policy.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_printer.h"
#include "share/ob_order_perserving_encoder.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "storage/ob_locality_manager.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/charset/ob_charset.h"
#include "pl/ob_pl_user_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_controller.h"
#endif
#include "observer/omt/ob_tenant_srs.h"
#include "sql/executor/ob_maintain_dependency_info_task.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
extern "C" {
#include "sql/parser/ob_non_reserved_keywords.h"
}
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common::sqlclient;

ObSqlArrayExpandGuard::ObSqlArrayExpandGuard(ParamStore &params, ObIAllocator &allocator)
  : array_obj_list_(allocator),
    ret_(OB_SUCCESS)
{
  int &ret = ret_;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (params.at(i).is_ext_sql_array()) {
      int64_t param_addr = 0;
      param_addr = params.at(i).get_ext();
      const ObSqlArrayObj *array_param = reinterpret_cast<const ObSqlArrayObj*>(param_addr);
      if (array_param->count_ > 0) {
        ArrayObjPair array_pair(&params.at(i), params.at(i));
        if (OB_FAIL(array_obj_list_.push_back(array_pair))) {
          LOG_WARN("store array obj list failed", K(ret));
        } else {
          params.at(i) = array_param->data_[0];
        }
      }
    }
  }
}

ObSqlArrayExpandGuard::~ObSqlArrayExpandGuard()
{
  common::ObList<ArrayObjPair, common::ObIAllocator>::iterator iter = array_obj_list_.begin();
  for (; iter != array_obj_list_.end(); ++iter) {
    *(iter->first) = iter->second;
  }
}

int ObSQLUtils::check_enable_decimalint(const ObSQLSessionInfo *session, bool &enable_decimalint)
{
  int ret = OB_SUCCESS;
  enable_decimalint = false;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else {
    enable_decimalint = (const_cast<ObSQLSessionInfo *>(session)->is_enable_decimal_int_type()
                         && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_0_0);
  }
  return ret;
}

bool ObSQLUtils::is_trans_commit_need_disconnect_err(int err)
{
  bool bool_ret = true;
  if (OB_SUCCESS == err
      || OB_TRANS_KILLED == err
      || OB_TRANS_CTX_NOT_EXIST == err
      || OB_TRANS_TIMEOUT == err
      || OB_TRANS_STMT_TIMEOUT == err
      || OB_TRANS_NEED_ROLLBACK == err
      || OB_TRANS_ROLLBACKED == err
      || OB_NOT_MASTER == err
      || OB_TRANS_IS_EXITING == err) {
    bool_ret = false;
  }
  return bool_ret;
}

void ObSQLUtils::check_if_need_disconnect_after_end_trans(const int end_trans_err,
                                                          const bool is_rollback,
                                                          const bool is_explicit,
                                                          bool &is_need_disconnect)
{
  // 1.对于commit操作（不管是隐式还是显式），失败的时候遇到事务模块目前没有明确指明的错误码，都采取断连接操作。
  // 2.对于显式rollback操作，如果失败，由于客户端就算收到错误码也不知道怎么处理，因此统一都断连接。
  // 3.对于隐式rollback操作，如果失败，这种情况是autocommit=1的情况，由于autocommit=1的分布式查询经常遇到rollback失败，
  // 所以这种情况不断连接，如果这种情况下有特殊情况需要断连接，需要在外层调用implicit_end_trans之后自行加上断连接的逻辑。
  is_need_disconnect = false;
  if (is_rollback) {
    // rollback
    if (OB_UNLIKELY(OB_SUCCESS != end_trans_err && is_explicit)) {
      // 显式rollback失败，要断连接
      is_need_disconnect = true;
      LOG_WARN_RET(end_trans_err, "fail to rollback explicitly, disconnect", K(end_trans_err));
    } else {
      // 隐式rollback（不管成功还是失败），或者显式rollback成功，不用断连接
      is_need_disconnect = false;
    }
  } else {
    // commit
    if (OB_UNLIKELY(ObSQLUtils::is_trans_commit_need_disconnect_err(end_trans_err))) {
      is_need_disconnect = true;
      LOG_WARN_RET(end_trans_err, "fail to commit, and error number is unexpected, disconnect", K(end_trans_err), K(lbt()));
    } else {
      is_need_disconnect = false;
    }
  }
}

int ObSQLUtils::md5(const ObString &stmt, char *sql_id, int32_t len)
{
  const int32_t MD5_LENGTH = 16;
  int ret = OB_SUCCESS;
  if (sql_id == NULL || len < 32) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid args", KP(sql_id), K(len));
  }
  char md5_sum_buf[MD5_LENGTH];
  ObString::obstr_size_t md5_sum_len = MD5_LENGTH;
  if (OB_SUCC(ret)) {
    unsigned char *res = MD5(reinterpret_cast<const unsigned char *>(stmt.ptr()),
                             stmt.length(),
                             reinterpret_cast<unsigned char *>(md5_sum_buf));
    if (OB_ISNULL(res)) {
      // MD5() in openssl always return an pointer not NULL, so we need not check return value.
      // see:
      // http://www.openssl.org/docs/crypto/md5.html#DESCRIPTION
      // http://www.openssl.org/docs/crypto/md5.html#RETURN_VALUES
      // Even so, we HAVE TO check it here. You know it.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("md5 res null pointer", K(ret), K(res));
    } else if (OB_FAIL(to_hex_cstr(md5_sum_buf, md5_sum_len, sql_id, len))) {
      LOG_WARN("transform to hex str error", K(ret));
    } else { }//do nothing
  }
  return ret;
}

int ObSQLUtils::calc_partition_ids(const ObIArray<ObNewRange*> &ranges,
                                   const ObSqlExpression &partition_func,
                                   const uint64_t part_num,
                                   ObIArray<uint64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> par_ids;  // for keeping the perhaps duplicate partition ids
  ObNewRow calc_row;
  ObExprCtx expr_ctx;
  ObObj result;
  ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC);
  expr_ctx.calc_buf_ = &allocator;
  int64_t N = ranges.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    // calculate
    ObNewRange *range = ranges.at(i);
    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_EXE_LOG(WARN, "invalid argument", K(ret));
    } else {
      calc_row.reset();
      result.reset();
      int64_t result_par_id = 0;
      // assuming all ranges are single-value
      calc_row.cells_ = const_cast<ObObj*>(range->start_key_.get_obj_ptr());//FIXME 此处强转是否有问题？
      calc_row.count_ = (range->start_key_.get_obj_cnt());
      if (OB_FAIL(partition_func.calc(expr_ctx, calc_row, result))) {
        SQL_EXE_LOG(WARN, "fail to calc hash expr", K(ret), K(calc_row));
      } else if (OB_FAIL(result.get_int(result_par_id))) {
        SQL_EXE_LOG(WARN, "fail to get int64 from result", K(ret), K(result));
      } else if (OB_FAIL(par_ids.push_back(result_par_id % part_num))) {
        SQL_EXE_LOG(WARN, "fail to push back partition id into array", K(ret));
      }
    }
  }

  // 最后将这些结果进行去重，即可得出最终的不重复的partition_id数组。
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < par_ids.count(); ++i) {
      bool duplicated = false;
      for (int64_t j = 0; !duplicated && j < partition_ids.count(); ++j) {
        if (par_ids.at(i) == partition_ids.at(j)) {
          duplicated = true;
        }
      }
      if (!duplicated) {
        if (OB_FAIL(partition_ids.push_back(par_ids.at(i)))) {
          SQL_EXE_LOG(WARN, "fail to push back partition id into array", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSQLUtils::get_phy_plan_type(ObIArray<share::ObPartitionLocation> &part_location_set,
                                  const ObAddr &my_address,
                                  ObPhyPlanType &plan_type)
{
  int ret = OB_SUCCESS;
  bool is_same = true;
  int64_t N = part_location_set.count();
  if (0 == N) {
    plan_type = OB_PHY_PLAN_LOCAL;
    LOG_TRACE("no tables used, thus local plan");
  } else {
    ObReplicaLocation replica_first;
    if (OB_FAIL(part_location_set.at(0).get_strong_leader(replica_first))) {
      SQL_EXE_LOG(WARN, "failed to get leader replica location", K(ret));
    } else {
      SQL_EXE_LOG(DEBUG, "part_location_set first replica", K(ret), K(replica_first));
      for (int64_t i = 1; OB_SUCC(ret) && true == is_same && i < N; ++i) {
        ObReplicaLocation replica_location;
        if (OB_FAIL(part_location_set.at(i).get_strong_leader(replica_location))) {
          SQL_EXE_LOG(WARN, "failed to get leader replica location", K(ret));
        } else {
          is_same = is_same && (replica_location.server_ == replica_first.server_);
          SQL_EXE_LOG(DEBUG, "part_location_set replica", K(ret), K(i), K(replica_location));
        }
      }
      if (OB_SUCC(ret)) {
        if (is_same) {
          if (my_address == replica_first.server_) {
            plan_type = OB_PHY_PLAN_LOCAL;
          } else {
            plan_type = OB_PHY_PLAN_REMOTE;
          }
        } else {
          plan_type = OB_PHY_PLAN_DISTRIBUTED;
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::has_outer_join_symbol(const ParseNode *node, bool &has)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null pointer", K(node), K(ret));
  } else if (node->type_ == T_OP_ORACLE_OUTER_JOIN_SYMBOL) {
    has = true;
  }
  for (int64_t i = 0 ; OB_SUCC(ret) && !has && i < node->num_child_; i++) {
    if (NULL == node->children_[i]) {
      //do nothing
    } else if (OB_FAIL(SMART_CALL(has_outer_join_symbol(node->children_[i], has)))) {
      LOG_WARN("check has_outer_join_symbol fail", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::replace_questionmarks(ParseNode *tree,
                                      const ParamStore &params)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (NULL != tree) {
    // replace ? with given params
    if (T_QUESTIONMARK == tree->type_) {
      const ObObj &param = params.at(tree->value_);
      switch (param.get_type()) {
      case ObIntType:
        tree->value_ = param.get_int();
        tree->type_ = T_INT;
        break;
      case ObDateTimeType:
        tree->value_ = param.get_datetime();
        tree->type_ = T_DATETIME;
        break;
      case ObTimestampType:
        tree->value_ = param.get_timestamp();
        tree->type_ = T_TIMESTAMP;
        break;
      case ObDateType:
        tree->value_ = param.get_date();
        tree->type_ = T_DATE;
        break;
      case ObTimeType:
        tree->value_ = param.get_time();
        tree->type_ = T_TIME;
        break;
      case ObYearType:
        tree->value_ = param.get_year();
        tree->type_ = T_YEAR;
        break;
      case ObVarcharType:
        tree->str_value_ = param.get_varchar().ptr();
        tree->str_len_ = param.get_varchar().length();
        tree->type_ = T_VARCHAR;
        break;
      case ObTinyIntType:
        tree->value_ = param.get_bool();
        tree->type_ = T_BOOL;
        break;
      case ObNumberType:
        tree->str_value_ = param.get_number().format();
        tree->type_ = T_NUMBER;
        break;
      default:
        LOG_WARN("never reach here", "type", param.get_type());
        break;
      }
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < tree->num_child_; ++i) {
      if (OB_ISNULL(tree->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument");
      } else {
        ret = SMART_CALL(replace_questionmarks(tree->children_[i], params));
      }
    }
  }
  return ret;
}

/**
 * @brief calculate the result of the const or calculable expr
 * @param[in] exec_ctx: exec context
 * @param[in] raw_expr: input expr, must be const or calculable expr
 * @param[out] result: result obj
 * @param[out] is_valid: whether the result is valid when ignore_failure == true
 * @param[out] allocator: used to deep copy result
 * @param[in] ignore_failure: whether ignore failure when calc failed
 * @param[out] constraints: add calc failure constraint when calc failed, used in query range
 */
int ObSQLUtils::calc_const_or_calculable_expr(
    ObExecContext *exec_ctx,
    const ObRawExpr *raw_expr,
    ObObj &result,
    bool &is_valid,
    ObIAllocator &allocator,
    bool ignore_failure /* = true*/,
    ObIArray<ObExprConstraint> *constraints /* = NULL */)
{
  int ret = OB_SUCCESS;
  const ParamStore *params = NULL;
  is_valid = false;
  if (OB_ISNULL(raw_expr) || OB_ISNULL(exec_ctx) || OB_ISNULL(exec_ctx->get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(exec_ctx), K(ret));
  } else if (FALSE_IT(params = &exec_ctx->get_physical_plan_ctx()->get_param_store())) {
  } else if (raw_expr->is_const_raw_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", K(ret));
    } else {
      is_valid = true;
    }
  } else if (raw_expr->is_static_scalar_const_expr()) {
    bool hit_cache = false;
    if (OB_FAIL(get_result_from_ctx(*exec_ctx, raw_expr, result, is_valid, hit_cache))) {
      LOG_WARN("failed to get result from ctx", K(ret));
    } else if (hit_cache && (is_valid || ignore_failure)) {
      // do nothing
    } else if (OB_FAIL(calc_const_expr(*exec_ctx, raw_expr,
                                       result, allocator, *params))) {
      if (T_FUN_SYS_INNER_ROW_CMP_VALUE == raw_expr->get_expr_type()) {
        if (ret == OB_ERR_MIN_VALUE) {
          result.set_min_value();
          is_valid = true;
          ret = OB_SUCCESS;
        } else if (ret == OB_ERR_MAX_VALUE) {
          result.set_max_value();
          is_valid = true;
          ret = OB_SUCCESS;
        }
      }
      if (ignore_failure && !IS_SPATIAL_EXPR(raw_expr->get_expr_type())) {
        LOG_TRACE("failed to calc const expr, ignore the failure", K(ret));
        ret = OB_SUCCESS;
      }
    } else {
      is_valid = true;
      if (result.is_pl_extend()) {
        if (OB_ISNULL(exec_ctx->get_pl_ctx())) {
          if (OB_FAIL(exec_ctx->init_pl_ctx())) {
            LOG_WARN("failed to init pl ctx", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(exec_ctx->get_pl_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("pl ctx is null", K(ret));
        } else if (OB_FAIL(exec_ctx->get_pl_ctx()->add(result))) {
          LOG_WARN("failed to add pl obj to pl ctx", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !hit_cache) {
      if (OB_FAIL(store_result_to_ctx(*exec_ctx, raw_expr, result, is_valid))) {
        LOG_WARN("failed to store result to ctx", K(ret));
      }
    }
    bool add_calc_failure_cons = false;
    if (OB_SUCC(ret) && T_FUN_SYS_INNER_ROW_CMP_VALUE == raw_expr->get_expr_type()) {
      if (result.is_min_value() || result.is_max_value()) {
        add_calc_failure_cons = true;
      }
    }
    if (OB_SUCC(ret) && (!is_valid || add_calc_failure_cons)) {
      if (NULL == constraints) {
        // do nothing
      } else if (OB_FAIL(add_calc_failure_constraint(raw_expr, *constraints))) {
        LOG_WARN("failed to add calc failure constraint", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Expr should be const_expr or calculable_expr", K(*raw_expr), K(ret));
  }
  return ret;
}

int ObSQLUtils::calc_simple_expr_without_row(
    ObSQLSessionInfo *session,
    const ObRawExpr *raw_expr,
    ObObj &result,
    const ParamStore *params,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  if (OB_ISNULL(raw_expr) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(params), K(ret));
  } else if (raw_expr->has_flag(CNT_SEQ_EXPR)) {
    // skip accuracy check when default value includes sequence expr.
    result.set_null();
  } else if (raw_expr->is_const_raw_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", KPC(raw_expr), K(ret));
    } else { /*do nothing*/ }
  } else if (OB_FAIL(calc_const_expr(session, *raw_expr, result, allocator, *params))) {
    SQL_LOG(WARN, "Get const_expr value error", KPC(raw_expr), K(ret));
  }

  return ret;
}
int ObSQLUtils::calc_raw_expr_without_row(
  ObExecContext &exec_ctx,
  const ObRawExpr *raw_expr,
  ObObj &result,
  const ParamStore *params,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(ret));
  } else if (raw_expr->is_const_raw_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", K(ret));
    } else { /*do nothing*/ }
  } else {
    ParamStore empty_params;
    if (OB_FAIL(calc_const_expr(exec_ctx, raw_expr, result, allocator, NULL == params ? empty_params : *params))) {
      SQL_LOG(WARN, "Get calculable expr value without addr to parition id error", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

// Clear expression's evaluation flag (children's evaluation flags are cascaded cleared too)
// to make expression be evaluated again.
//
// NOTE: this evaluation flag clear method can only be used in PL expression which
// evaluate value without row. This is why we implement this function here instead of making
// it a member function of ObExpr, otherwise it will be abused in SQL Engine.
void ObSQLUtils::clear_expr_eval_flags(const ObExpr &expr, ObEvalCtx &ctx)
{
  if (expr.eval_func_ != NULL || T_OP_ROW == expr.type_) {
    // The eval_func_ of the T_OP_ROW expression is null, causing the issue where the evaluation
    // flag of the child expressions is not cleared. For more detail, see issue
    //
    expr.get_eval_info(ctx).clear_evaluated_flag();
    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      clear_expr_eval_flags(*expr.args_[i], ctx);
    }
  }
}

int ObSQLUtils::calc_sql_expression_without_row(
  ObExecContext &exec_ctx,
  const ObISqlExpression &expr,
  ObObj &result,
  ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is NULL", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const sql::ObExpr *new_expr = expr.get_expr();
    exec_ctx.get_physical_plan_ctx()->set_cur_time(ObTimeUtility::current_time(), *exec_ctx.get_my_session());
    if (NULL == new_expr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("static engine should have implement this function. unexpected null", K(ret));
    } else {
      ObDatum *datum = NULL;
      ObEvalCtx eval_ctx(exec_ctx, allocator);
      clear_expr_eval_flags(*new_expr, eval_ctx);
      OZ(new_expr->eval(eval_ctx, datum)); // sql exprs called here
      OZ(datum->to_obj(result, new_expr->obj_meta_, new_expr->obj_datum_map_));
    }
  }
  return ret;
}

int ObSQLUtils::calc_const_expr(const ObRawExpr *expr,
                                const ParamStore *params,
                                common::ObObj &result,
                                bool &need_check)
{
  int ret = OB_SUCCESS;
  const ObConstRawExpr *const_expr = NULL;
  need_check = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (OB_UNLIKELY(!expr->is_const_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not const expr", K(expr->get_expr_type()), K(ret));
  } else if (FALSE_IT(const_expr = static_cast<const ObConstRawExpr *>(expr))) {
    // do nothing
  } else if (T_QUESTIONMARK == const_expr->get_expr_type()) {
    if (OB_ISNULL(params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(params), K(ret));
    } else if (OB_FAIL(get_param_value(const_expr->get_value(), *params, result, need_check))) {
      LOG_WARN("get param value error", K(ret));
    } else {  /*do nothing*/ }
  } else {
    need_check = true;
    result = const_expr->get_value();
  }
  return ret;
}

int ObSQLUtils::is_charset_data_version_valid(ObCharsetType charset_type, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    SQL_LOG(WARN, "failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (CHARSET_LATIN1 == charset_type && data_version < DATA_VERSION_4_1_0_0 ) {
    ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "latin1 not supported when data_version < 4_1_0_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, charset latin1 is");
  } else if (CHARSET_GB18030_2022 == charset_type && data_version < DATA_VERSION_4_2_0_0 ) {
    ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "GB18030_2022 not supported when data_version < 4_2_0_0", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, charset GB18030_2022 is");
  }
  return ret;
}

int ObSQLUtils::is_collation_data_version_valid(ObCollationType collation_type, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_CLOSE_MODULES
   uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    SQL_LOG(WARN, "failed to GET_MIN_DATA_VERSION", K(ret));
  } else if (data_version < DATA_VERSION_4_2_2_0 &&
             (CS_TYPE_UTF16_UNICODE_CI == collation_type ||
              CS_TYPE_UTF8MB4_UNICODE_CI == collation_type)) {
    ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "Unicode collation not supported when data_version < 4_2_2_0", K(collation_type), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2.2, unicode collation is");
  }
#endif
  return ret;
}

// 参数raw_expr中如果出现函数addr_to_partition_id，
// 那么得到的partition_id结果在后面无法映射到相应的addr
int ObSQLUtils::calc_calculable_expr(ObSQLSessionInfo *session,
                                     const ObRawExpr *expr,
                                     ObObj &result,
                                     ObIAllocator *allocator,
                                     const ParamStore &params_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Invalid arguments", K(expr), K(allocator));
  } else if (!expr->is_static_scalar_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "expr should be calculable expr", K(*expr), K(ret));
  } else if (OB_FAIL(calc_const_expr(session,
                                     *expr,
                                     result,
                                     *allocator,
                                     params_array))) {
    SQL_LOG(WARN, "failed to calc const expr", K(*expr), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSQLUtils::calc_const_expr(ObExecContext &exec_ctx,
                                const ObRawExpr *expr,
                                ObObj &result,
                                ObIAllocator &allocator,
                                const ParamStore &params_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Invalid arguments", K(expr));
  } else if (OB_FAIL(calc_const_expr(exec_ctx.get_my_session(),
                                     *expr,
                                     result,
                                     allocator,
                                     params_array,
                                     &exec_ctx))) {
    SQL_LOG(WARN, "failed to calc const expr", K(*expr), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSQLUtils::calc_const_expr(ObSQLSessionInfo *session,
                                const ObRawExpr &expr,
                                ObObj &result,
                                ObIAllocator &allocator,
                                const ParamStore &params_array,
                                ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null session", K(ret));
  } else {
    if (OB_FAIL(se_calc_const_expr(session, &expr, params_array, allocator, exec_ctx, result))) {
      LOG_WARN("failed to calc const expr", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::se_calc_const_expr(ObSQLSessionInfo *session,
                                   const ObRawExpr *expr,
                                   const ParamStore &params,
                                   ObIAllocator &allocator,
                                   ObExecContext *out_ctx,
                                   common::ObObj &result)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != session);
  lib::ContextParam param;
  param.set_mem_attr(session->get_effective_tenant_id(), "CalcConstExpr",
                     ObCtxIds::DEFAULT_CTX_ID)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL)
    .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ObIAllocator &tmp_allocator = CURRENT_CONTEXT->get_arena_allocator();
    ObPhysicalPlanCtx phy_plan_ctx(tmp_allocator);
    phy_plan_ctx.set_rich_format(session->use_rich_format());
    // pass the outside timeout timestamp if available
    if (NULL != out_ctx && NULL != out_ctx->get_physical_plan_ctx()) {
      phy_plan_ctx.set_timeout_timestamp(
          out_ctx->get_physical_plan_ctx()->get_timeout_timestamp());
    }
    for (int i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      if (OB_FAIL(phy_plan_ctx.get_param_store_for_update().push_back(params.at(i)))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    } // end for
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(phy_plan_ctx.init_datum_param_store())) {
      LOG_WARN("failed to init datum param store", K(ret));
    } else {
      ObSchemaGetterGuard *schema_guard = NULL;
      if (NULL != out_ctx) {
        if (OB_ISNULL(out_ctx->get_sql_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get nul sql ctx", K(ret));
        } else {
          schema_guard = out_ctx->get_sql_ctx()->schema_guard_;
        }
      } else {
        schema_guard = &session->get_cached_schema_guard_info().get_schema_guard();
      }
      uint64_t effective_tenant_id = session->get_effective_tenant_id();
      if (session->get_ddl_info().is_ddl_check_default_value()) {
        effective_tenant_id = OB_SERVER_TENANT_ID;
      }
      SMART_VARS_2((ObExecContext, exec_ctx, tmp_allocator),
                   (ObStaticEngineExprCG, expr_cg, tmp_allocator,
                    session, schema_guard,
                    phy_plan_ctx.get_original_param_cnt(),
                    phy_plan_ctx.get_datum_param_store().count(),
                    (NULL != out_ctx ? out_ctx->get_min_cluster_version() : GET_MIN_CLUSTER_VERSION()))) {
        LinkExecCtxGuard link_guard(*session, exec_ctx);
        exec_ctx.set_my_session(session);
        exec_ctx.set_mem_attr(ObMemAttr(effective_tenant_id,
                                        ObModIds::OB_SQL_EXEC_CONTEXT,
                                        ObCtxIds::EXECUTE_CTX_ID));
        exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
        if (NULL != out_ctx) {
          exec_ctx.set_sql_ctx(out_ctx->get_sql_ctx());
          if (NULL != out_ctx->get_original_package_guard()) {
            exec_ctx.set_package_guard(out_ctx->get_original_package_guard());
          }
        }
        void *frame_buf = NULL;
        ObPreCalcExprFrameInfo *pre_calc_frame = NULL;
        ObRawExpr *copied_expr = NULL;
        ObRawExprFactory expr_factory(tmp_allocator);
        int org_obj_cnt = phy_plan_ctx.get_param_store().count();
        if (OB_FAIL(ObRawExprCopier::copy_expr(expr_factory, expr, copied_expr))) {
          LOG_WARN("failed to copy raw expr", K(ret));
        } else if (OB_ISNULL(copied_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null expr", K(ret), K(expr), K(copied_expr));
        } else if (OB_ISNULL(frame_buf = tmp_allocator.alloc(sizeof(ObPreCalcExprFrameInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          pre_calc_frame = new(frame_buf)ObPreCalcExprFrameInfo(tmp_allocator);
          if (OB_FAIL(expr_cg.generate_calculable_expr(copied_expr, *pre_calc_frame))) {
            LOG_WARN("failed to generate calculable expr", K(ret));
            // set current time before do pre calculation
          } else if (FALSE_IT(phy_plan_ctx.set_cur_time(ObTimeUtility::current_time(), *session))) {
            // do nothing
          } else if (FALSE_IT(phy_plan_ctx.set_last_trace_id(session->get_last_trace_id()))) {
            // do nothing
          } else if (OB_FAIL(ObPlanCacheObject::pre_calculation(false,
                                                                *pre_calc_frame,
                                                                exec_ctx))) {
            LOG_WARN("failed to pre calculate", K(ret));
          } else if (OB_UNLIKELY(org_obj_cnt + 1 != phy_plan_ctx.get_param_store().count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unpected param store", K(phy_plan_ctx.get_param_store()), K(org_obj_cnt));
          } else {
            const ObObj &tmp_result = phy_plan_ctx.get_param_store().at(org_obj_cnt);
            if (!tmp_result.is_ext()) {
              if (OB_FAIL(deep_copy_obj(allocator, tmp_result, result))) {
                LOG_WARN("failed to deep copy obj", K(ret));
              }
            } else {
              // res is_ext, data in obj may be destructed when the temp exec ctx destruct at the end.
              // e.g. eval_collection_construct is called
              if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(allocator, tmp_result, result))) {
                LOG_WARN("failed to deep copy pl extend obj", K(ret), K(tmp_result));
              }
            }
          }
        }
      }
      if (NULL != out_ctx &&  NULL != out_ctx->get_original_package_guard()) {
        // avoid out_ctx.package_guard_ be freed
        exec_ctx.set_package_guard(NULL);
      }
    }
  }
  return ret;
}

int ObSQLUtils::make_generated_expression_from_str(const common::ObString &expr_str,
                                                   const share::schema::ObTableSchema &schema,
                                                   const share::schema::ObColumnSchemaV2 &gen_col,
                                                   const common::ObIArray<share::schema::ObColDesc> &col_ids,
                                                   common::ObIAllocator &allocator,
                                                   ObTempExpr *&temp_expr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  const ObTenantSchema *tenant_schema = nullptr;
  ObSchemaGetterGuard guard;
  SMART_VAR(sql::ObSQLSessionInfo, default_session) {
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(default_session.init(0, 0, &allocator))) {
      LOG_WARN("init empty session failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant_schema", K(ret));
    } else if (OB_FAIL(default_session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
      LOG_WARN("fail to init", K(ret));
    } else if (OB_FAIL(default_session.load_all_sys_vars(guard))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(make_generated_expression_from_str(
                expr_str, default_session, schema, gen_col, col_ids, allocator, temp_expr))) {
      LOG_WARN("make generated expression failed", K(ret), K(expr_str));
    }
  }
  return ret;
}

int ObSQLUtils::make_generated_expression_from_str(const common::ObString &expr_str,
                                                   ObSQLSessionInfo &session,
                                                   const share::schema::ObTableSchema &schema,
                                                   const share::schema::ObColumnSchemaV2 &gen_col,
                                                   const common::ObIArray<share::schema::ObColDesc> &col_ids,
                                                   common::ObIAllocator &allocator,
                                                   ObTempExpr *&temp_expr)
{
  int ret = OB_SUCCESS;
  temp_expr = NULL;
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr *expr = NULL;
  RowDesc row_desc;
  ObArray<ObQualifiedName> columns;
  ObSEArray<ObRawExpr *, 6> real_exprs;
  ObSchemaGetterGuard guard;
  ObSchemaChecker schema_checker;
  const bool allow_sequence = false;
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_checker.init(guard))) {
    LOG_WARN("failed to init schema checker", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_str, expr_factory,
                                                          session, expr, columns,
                                                          &schema, allow_sequence, NULL,
                                                          &schema_checker))) {
    LOG_WARN("get generated column expr failed", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else {
    //create row_desc
    for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
      ObColumnRefRawExpr *col_ref = NULL;
      const ObColumnSchemaV2 *col_schema = NULL;
      if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == col_ids.at(i).col_id_ ||
          OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == col_ids.at(i).col_id_) {
        continue; // hidden multi version column, not exist in schema, no need to add.
      } else if (OB_ISNULL(col_schema = schema.get_column_schema(col_ids.at(i).col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K_(col_ids.at(i).col_id));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, col_ref))) {
        LOG_WARN("build column expr failed", K(ret));
      } else if (OB_FAIL(row_desc.add_column(col_ref))) {
        LOG_WARN("add column to row desc failed", K(ret));
      } else { /*do nothing*/ }
      //替换expr中所有和当前column ref相同的column
      for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
        ObQualifiedName &q_name = columns.at(j);
        if ((!q_name.database_name_.empty() || !q_name.tbl_name_.empty()) && !q_name.is_pl_udf()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is not based on single table", K(q_name));
        } else if (ObCharset::case_insensitive_equal(q_name.col_name_, col_schema->get_column_name_str())) {
          if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, col_ref))) {
            LOG_WARN("replace reference column failed", K(ret));
          }
        } else { /*do nothing*/ }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      ObQualifiedName &q_name = columns.at(i);
      if (q_name.is_pl_udf()) {
          OZ (session.set_default_database(q_name.tbl_name_));
          OZ (ObRawExprUtils::resolve_gen_column_udf_expr(expr, const_cast<ObQualifiedName &>(q_name),
          expr_factory, session, &schema_checker, columns, real_exprs, NULL), q_name, i, q_name.access_idents_.at(q_name.access_idents_.count() -1).udf_info_);
      } else {
        OZ (real_exprs.push_back(q_name.ref_expr_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::build_pad_expr_recursively(expr_factory, session, schema, gen_col, expr))) {
        LOG_WARN("add pad expr failed", K(ret));
      } else if (OB_FAIL(expr->formalize(&session))) {
        LOG_WARN("formalize expression failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObExprResType dest_type;
      dest_type.set_meta(gen_col.get_meta_type());
      dest_type.set_accuracy(gen_col.get_accuracy());
      if (ObRawExprUtils::need_column_conv(dest_type, *expr)) {
        if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(expr_factory, &gen_col, expr, &session))) {
          LOG_WARN("create column convert expr failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      OZ (ObStaticEngineExprCG::gen_expr_with_row_desc(expr, row_desc, allocator,
                                                       &session, &guard, temp_expr));
      CK (OB_NOT_NULL(temp_expr));
    }
  }
  return ret;
}

int ObSQLUtils::make_default_expr_context(uint64_t tenant_id, ObIAllocator &allocator, ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTenantSchema *tenant_schema = nullptr;
  ObSQLSessionInfo *default_session = static_cast<ObSQLSessionInfo*>(allocator.alloc(sizeof(ObSQLSessionInfo)));
  if (OB_ISNULL(default_session)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    default_session = new(default_session)ObSQLSessionInfo();
    if (OB_FAIL(default_session->init(0, 0, &allocator))) {
      LOG_WARN("init default session failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant_schema", K(ret));
    } else if (OB_FAIL(default_session->init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
      LOG_WARN("fail to init", K(ret));
    } else if (OB_FAIL(default_session->load_all_sys_vars(guard))) {
      LOG_WARN("load default system variable to session failed", K(ret));
    } else {
      expr_ctx.my_session_ = default_session;
    }
  }

  if (OB_SUCC(ret)) {
    ObPhysicalPlanCtx *phy_plan_ctx = static_cast<ObPhysicalPlanCtx*>(allocator.alloc(sizeof(ObPhysicalPlanCtx)));
    if (OB_ISNULL(phy_plan_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      phy_plan_ctx = new(phy_plan_ctx)ObPhysicalPlanCtx(allocator);
      expr_ctx.phy_plan_ctx_ = phy_plan_ctx;
    }
  }

  if (OB_SUCC(ret)) {
    ObPhysicalPlan *phy_plan = static_cast<ObPhysicalPlan*>(allocator.alloc(sizeof(ObPhysicalPlan)));
    if (OB_ISNULL(phy_plan)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      phy_plan = new(phy_plan)ObPhysicalPlan();
      expr_ctx.phy_plan_ctx_->set_phy_plan(phy_plan);
    }
  }

  if (OB_SUCC(ret)) {
    ObExecContext *exec_ctx = static_cast<ObExecContext*>(allocator.alloc(sizeof(ObExecContext)));
    if (OB_ISNULL(exec_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      exec_ctx = new(exec_ctx)ObExecContext(allocator);
      exec_ctx->set_my_session(default_session);
      exec_ctx->set_mem_attr(ObMemAttr(tenant_id,
                                       ObModIds::OB_SQL_EXEC_CONTEXT,
                                       ObCtxIds::EXECUTE_CTX_ID));
      expr_ctx.exec_ctx_ = exec_ctx;
      expr_ctx.exec_ctx_->set_my_session(default_session);
    }
  }

  if (OB_SUCC(ret)) {
    expr_ctx.calc_buf_ = &allocator;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(wrap_column_convert_ctx(expr_ctx, expr_ctx.column_conv_ctx_))) {
      LOG_WARN("wrap column convert ctx failed", K(ret));
    }
  }
  return ret;
}

void ObSQLUtils::destruct_default_expr_context(ObExprCtx &expr_ctx)
{
  if (NULL != expr_ctx.my_session_) {
    expr_ctx.my_session_->~ObSQLSessionInfo();
    expr_ctx.my_session_ = NULL;
  }
  if (NULL != expr_ctx.phy_plan_ctx_ && NULL != expr_ctx.phy_plan_ctx_->get_phy_plan()) {
    expr_ctx.phy_plan_ctx_->get_phy_plan()->~ObPhysicalPlan();
    expr_ctx.phy_plan_ctx_->set_phy_plan(NULL);
  }
  if (NULL != expr_ctx.phy_plan_ctx_) {
    expr_ctx.phy_plan_ctx_->~ObPhysicalPlanCtx();
    expr_ctx.phy_plan_ctx_ = NULL;
  }
  if (NULL != expr_ctx.exec_ctx_) {
    expr_ctx.exec_ctx_->~ObExecContext();
    expr_ctx.exec_ctx_ = NULL;
  }
}

int ObSQLUtils::calc_sql_expression(const ObTempExpr *expr,
                                    const ObIArray<share::schema::ObColDesc> &col_ids,
                                    const ObNewRow &row,
                                    ObExecContext &exec_ctx,
                                    ObObj &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Expr to calc is NULL", K(expr), K(ret));
  } else if (col_ids.count() != row.count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cells count in row is not equal to col_ids", K(col_ids), K(row.count_), K(ret));
  } else {
    if (OB_FAIL(expr->eval(exec_ctx, row, result))) {
      LOG_WARN("Fail to calc value", K(ret), K(*expr), K(row));
    }
  }
  return ret;
}

int64_t ObSQLUtils::get_usec()
{
  struct timeval time_val;
  if (0 != gettimeofday(&time_val, NULL)) { //success: return 0, failed: return -1
    LOG_WARN_RET(OB_ERR_SYS, "fail to get time of day");
  }
  return time_val.tv_sec*1000000 + time_val.tv_usec;
}

int ObSQLUtils::check_and_convert_db_name(const ObCollationType cs_type, const bool preserve_lettercase,
                                          ObString &name)
{
  /*如果database name的字节数大于384则报错OB_WRONG_DB_NAME;
   *如果database name的字节数大于128且小于等于384则报错OB_ERR_TOO_LONG_IDENT;
   *如果database name的最后一个字符是空格，则报错OB_WRONG_DB_NAME;
   *如果database name的前缀为#mysql50#，并且以以下四个字符结尾，'.','~','/','\\', 则报错OB_WRONG_DB_NAME;
   */
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  ObString origin_name = name;
  int64_t name_len = name.length();
  const char *name_str = name.ptr();
  int32_t max_database_name_length = OB_MAX_DATABASE_NAME_LENGTH;
  if (0 == name_len || name_len > (max_database_name_length * OB_MAX_CHAR_LEN)) {
    ret = OB_WRONG_DB_NAME;
    LOG_USER_ERROR(OB_WRONG_DB_NAME, static_cast<int32_t>(name_len), name_str);
    LOG_WARN("incorrect database name", K(name), K(ret));
  } else {
    bool check_for_path_chars = check_mysql50_prefix(name);
    if (check_for_path_chars) {
      name_str += OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH;
      name_len -= OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH;
    }

    ObString last_name(name_len, name_str);
    if (!preserve_lettercase
        || (lib::is_mysql_mode() && 0 == name.case_compare(OB_INFORMATION_SCHEMA_NAME))) {
      ObCharset::casedn(CS_TYPE_UTF8MB4_BIN, last_name);
    }
    if (OB_ERR_WRONG_IDENT_NAME == (ret = check_ident_name(cs_type, last_name, check_for_path_chars,
                                                           max_database_name_length))) {
      ret = OB_WRONG_DB_NAME;
      LOG_USER_ERROR(OB_WRONG_DB_NAME, static_cast<int32_t>(origin_name.length()), origin_name.ptr());
      LOG_WARN("Incorrect database name", K(origin_name), K(ret));
    } else if (OB_ERR_TOO_LONG_IDENT == ret) {
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(origin_name.length()), origin_name.ptr());
      LOG_WARN("database name is too long", K(origin_name), K(ret));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to check ident name", K(origin_name), K(ret));
    } else {
      name = last_name;
    }
  }
  return ret;
}

/* 将用户输入的dbname换成数据库内部存放的大小写 */
int ObSQLUtils::cvt_db_name_to_org(share::schema::ObSchemaGetterGuard &schema_guard,
                                   const ObSQLSessionInfo *session,
                                   common::ObString &name,
                                   ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode() && session != NULL && !session->is_inner()) {
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(session->get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (case_mode == OB_ORIGIN_AND_INSENSITIVE || case_mode == OB_LOWERCASE_AND_INSENSITIVE) {
      const ObDatabaseSchema *db_schema = NULL;
      if (OB_FAIL(schema_guard.get_database_schema(session->get_effective_tenant_id(),
                                                   name,
                                                   db_schema))) {
        LOG_WARN("fail to get database schema", K(name), K(ret));
      } else if (db_schema != NULL) {
        name = db_schema->get_database_name();
        if (allocator != NULL) {
          OZ(ob_write_string(*allocator, name, name));
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::check_and_convert_table_name(const ObCollationType cs_type,
                                             const bool preserve_lettercase,
                                             ObString &name,
                                             const stmt::StmtType stmt_type,
                                             const bool is_index_table)
{
  return check_and_convert_table_name(cs_type, preserve_lettercase, name, lib::is_oracle_mode(), stmt_type, is_index_table);
}

int ObSQLUtils::check_and_convert_table_name(const ObCollationType cs_type,
                                             const bool preserve_lettercase,
                                             ObString &name,
                                             const bool is_oracle_mode,
                                             const stmt::StmtType stmt_type,
                                             const bool is_index_table)
{
  /**
   * MYSQL模式
   *  如果table name的字节数大于192则报错OB_WRONG_TABLE_NAME;
   *  如果table name的字节数大于64且小于等于192则报错OB_ERR_TOO_LONG_IDENT;
   *  如果table name的最后一个字符是空格，则报错OB_WRONG_TABLE_NAME;
   *  如果是直接查询索引表,对表名长度特殊处理
   * ORACLE模式
   *  如果table name的字节数大于384则报错OB_WRONG_TABLE_NAME;
   *  如果table name的字节数大于128且小于等于384则报错OB_ERR_TOO_LONG_IDENT;
   *  如果table name的最后一个字符是空格，则报错OB_WRONG_TABLE_NAME;
   *  如果是直接查询索引表,对表名长度特殊处理
   */
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  int64_t name_len = name.length();
  const char *name_str = name.ptr();
  const int64_t max_user_table_name_length = is_oracle_mode
              ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
  const int64_t max_index_name_prefix_len = 30;
  if (0 == name_len
      || (!is_index_table && (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN)))
      || (is_index_table && (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN + max_index_name_prefix_len)))
      || OB_ISNULL(name_str)) {
    ret = OB_WRONG_TABLE_NAME;
    LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int32_t>(name_len), name_str);
    LOG_WARN("incorrect table name", K(name), K(ret));
  } else {
    char origin_name[OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE * OB_MAX_CHAR_LEN + 1] = {'\0'};
    MEMCPY(origin_name, name_str, name_len);
    if (!preserve_lettercase) {
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
    }
    bool check_for_path_chars = false;
    int64_t max_ident_len = max_user_table_name_length;
    if ((stmt::T_SELECT == stmt_type || stmt::T_INSERT == stmt_type) && is_index_table) {
      //索引表会有额外前缀,因此查询时长度限制用OB_MAX_TABLE_NAME_LENGTH
      max_ident_len = OB_MAX_TABLE_NAME_LENGTH;
    }
    if (OB_ERR_WRONG_IDENT_NAME == (ret = check_ident_name(CS_TYPE_UTF8MB4_GENERAL_CI,
                                                           name,
                                                           check_for_path_chars,
                                                           max_ident_len))) {
      if (lib::is_oracle_mode()) {
        // It allows the last char of table name and index name is space in oracle mode
        ret = OB_SUCCESS;
      } else {
        ret = OB_WRONG_TABLE_NAME;
        LOG_USER_ERROR(OB_WRONG_TABLE_NAME, (int)strlen(origin_name), origin_name);
        LOG_WARN("Incorrect table name", K(origin_name), K(ret));
      }
    } else if (OB_ERR_TOO_LONG_IDENT == ret) {
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)strlen(origin_name), origin_name);
      LOG_WARN("table name is too long", K(origin_name), K(max_ident_len), K(ret), K(stmt_type), K(is_index_table));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to check ident name", K(origin_name), K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::check_and_convert_context_namespace(const common::ObCollationType cs_type,
                                                    common::ObString &name)
{
  bool preserve_lettercase = true;
  stmt::StmtType type = stmt::T_CREATE_CONTEXT;
  return check_and_convert_table_name(cs_type, preserve_lettercase, name, type);
}

int ObSQLUtils::check_index_name(const ObCollationType cs_type, ObString &name)
{
  /* MYSQL模式
   *  如果table name的字节数大于64则报错OB_ERR_TOO_LONG_IDENT;
   *  如果table name的最后一个字符是空格，则报错OB_WRONG_NAME_FOR_INDEX
   * ORACLE模式
   *  如果table name的字节数大于128则报错OB_ERR_TOO_LONG_IDENT;
   *  如果table name的最后一个字符是空格，则报错OB_WRONG_TABLE_NAME;
   *  */
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  int64_t name_len = name.length();
  const char *name_str = name.ptr();
  const int64_t max_user_table_name_length = lib::is_oracle_mode()
              ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
  if (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN)) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_len), name_str);
    LOG_WARN("index name is too long", K(name), K(ret));
  } else if (0 == name_len) {
    if (lib::is_oracle_mode()) {
      ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
      LOG_WARN("index name is empty", K(ret));
    } else {
      ret = OB_WRONG_NAME_FOR_INDEX;
      LOG_WARN("index name is empty", K(ret));
      LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, static_cast<int32_t>(name_len), name_str);
    }
  } else {
    bool check_for_path_chars = false;
    if (OB_ERR_WRONG_IDENT_NAME == (ret = check_ident_name(CS_TYPE_UTF8MB4_GENERAL_CI, name, check_for_path_chars,
                                                           max_user_table_name_length))) {
      if (lib::is_mysql_mode()) {
        ret = OB_WRONG_NAME_FOR_INDEX;
        LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, name.length(), name.ptr());
        LOG_WARN("Incorrect index name", K(name), K(ret));
      } else { // It allows the last char of index name is space in oracle mode.
        ret = OB_SUCCESS;
      }
    } else if (OB_ERR_TOO_LONG_IDENT == ret) {
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, name.length(), name.ptr());
      LOG_WARN("index name is too long", K(name), K(ret));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to check ident name", K(name), K(ret));
    }
  }
  return ret;
}
int ObSQLUtils::check_column_name(const ObCollationType cs_type, ObString &name, bool is_from_view)
{
  /*如果table name的字节数大于128则报错OB_ERR_TOO_LONG_IDENT;
   *如果table name的最后一个字符是空格，则报错OB_WRONG_COLUMN_NAME */
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  bool last_char_is_space = false;
  const char *end = name.ptr() + name.length();
  const char *name_str = name.ptr();
  size_t name_len = 0; // char semantics for MySQL mode, and byte semantics for Oracle mode
  size_t byte_length = 0;
  int is_mb_char = 0;
  while (OB_SUCCESS == ret && name_str != end) {
    last_char_is_space = ObCharset::is_space(CS_TYPE_UTF8MB4_GENERAL_CI, *name_str);
    if (ObCharset::usemb(CS_TYPE_UTF8MB4_GENERAL_CI)) {
      is_mb_char = ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end);
      if (is_mb_char) {
        byte_length = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end - name_str, 1);
        name_str += byte_length;
        if (lib::is_mysql_mode()) {
          name_len++;
        } else {
          name_len += byte_length;
        }
        continue;
      }
    }
    last_char_is_space = (*name_str == ' ');
    if ('\377' == *name_str) {
      ret = OB_WRONG_COLUMN_NAME;
    } else {
      name_str++;
      name_len++;
    }
  }

  if (OB_SUCC(ret)) {
    if (last_char_is_space && !is_from_view) {
      ret = OB_WRONG_COLUMN_NAME;
      LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, name.length(), name.ptr());
      LOG_WARN("incorrect column name", K(name), K(ret));
    } else if (name_len > static_cast<size_t>(OB_MAX_COLUMN_NAME_LENGTH)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, name.length(), name.ptr());
      LOG_WARN("column name is too long", K(ret), K(name), K(name_len));
    }
  }
  return ret;
}

int ObSQLUtils::check_and_copy_column_alias_name(const ObCollationType cs_type, const bool is_auto_gen,
                                                 ObIAllocator *allocator, ObString &name)
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  ObString origin_name = name;
  if (NULL == allocator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator is NULL", K(ret));
  } else if (OB_FAIL(make_field_name(name.ptr(),
                                     static_cast<const int64_t>(name.length()),
                                     CS_TYPE_UTF8MB4_GENERAL_CI,
                                     allocator,
                                     name))) {
    LOG_WARN("fail to copy column alias name", K(origin_name), K(ret));
  } else {
    if (name.length() < origin_name.length() && !is_auto_gen) {
      if (0 == name.length()) {
        LOG_USER_WARN(OB_ERR_NAME_BECOMES_EMPTY, origin_name.length(), origin_name.ptr());
      } else {
        LOG_USER_WARN(OB_ERR_REMOVED_SPACES, origin_name.length(), origin_name.ptr());
      }
    }
  }
  return ret;
}

int ObSQLUtils::check_ident_name(const ObCollationType cs_type, ObString &name,
                                 const bool check_for_path_char, const int64_t max_ident_len)
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  bool last_char_is_space = false;
  const char *end = name.ptr() + name.length();
  const char *name_str = name.ptr();
  size_t name_len = 0; // char semantics for MySQL mode, and byte semantics for Oracle mode
  size_t byte_length = 0;
  int is_mb_char = 0;
  while (OB_SUCCESS == ret && NULL != name_str && name_str != end) {
    last_char_is_space = ObCharset::is_space(CS_TYPE_UTF8MB4_GENERAL_CI, *name_str);
    if (ObCharset::usemb(CS_TYPE_UTF8MB4_GENERAL_CI)) {
      is_mb_char = ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end);
      if (is_mb_char) {
        byte_length = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end - name_str, 1);
        name_str += byte_length;
        if (lib::is_mysql_mode()) {
          name_len++;
        } else {
          name_len += byte_length;
        }
        continue;
      }
    }
    if (check_for_path_char
        && ('/' == *name_str || '\\' == *name_str || '~' == *name_str  || '.' == *name_str)) {
      ret = OB_ERR_WRONG_IDENT_NAME;
      LOG_WARN("Incorrect database name", K(name), K(ret));
    } else {
      name_str++;
      name_len++;
    }
  }
  if (OB_SUCC(ret)) {
    if (name_len > static_cast<size_t>(max_ident_len)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_WARN("ident name is too long", K(ret), K(name), K(name.length()), K(name_len));
    } else if (last_char_is_space) {
      ret = OB_ERR_WRONG_IDENT_NAME;
      LOG_WARN("incorrect ident name", K(name), K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::get_proxy_can_activate_role(const ObIArray<uint64_t> &role_id_array,
                                            const ObIArray<uint64_t> &role_id_option_array,
                                            const ObProxyInfo &proxied_info,
                                            ObIArray<uint64_t> &new_role_id_array,
                                            ObIArray<uint64_t> &new_role_id_option_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(role_id_array.count() != role_id_option_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (proxied_info.proxy_flags_ == PROXY_USER_NO_ROLES_BE_ACTIVATED) {
    new_role_id_array.reuse();
    new_role_id_option_array.reuse();
  } else if (proxied_info.proxy_flags_ == PROXY_USER_ACTIVATE_ALL_ROLES)  {
    OZ (new_role_id_array.assign(role_id_array));
    OZ (new_role_id_option_array.assign(role_id_option_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < role_id_array.count(); i++) {
      bool found = false;
      for (int j = 0; OB_SUCC(ret) && !found && j < proxied_info.role_id_cnt_; j++) {
        uint64_t role_id = proxied_info.get_role_id_by_idx(j);
        if (role_id == OB_INVALID_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else if (role_id_array.at(i) == role_id) {
          found = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (proxied_info.proxy_flags_ == PROXY_USER_MAY_ACTIVATE_ROLE) {
        if (found) {
          OZ (new_role_id_array.push_back(role_id_array.at(i)));
          OZ (new_role_id_option_array.push_back(role_id_option_array.at(i)));
        }
      } else if (proxied_info.proxy_flags_ == PROXY_USER_ROLE_CAN_NOT_BE_ACTIVATED) {
        if (!found) {
          OZ (new_role_id_array.push_back(role_id_array.at(i)));
          OZ (new_role_id_option_array.push_back(role_id_option_array.at(i)));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      }
    }
  }
  return ret;
}

bool ObSQLUtils::check_mysql50_prefix(ObString &db_name)
{
  bool ret = false;
  const char *str = db_name.ptr();
  int64_t len = db_name.length();
  if (NULL == str || len < OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH) {
    //do nothing
  } else {
    ret = !STRNCMP(str, OB_MYSQL50_TABLE_NAME_PREFIX,
                   OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH);
  }
  return ret;
}

/***************************/
/*   本处为不完全列举，是根据ObBasicStmt::virtual bool cause_implicit_commit()中提取出来 */
/* 需要手动同步改列表     */
/***************************/
bool ObSQLUtils::cause_implicit_commit(ParseResult &result)
{
  bool ret = false;
  if (NULL != result.result_tree_
      && NULL != result.result_tree_->children_
      && NULL != result.result_tree_->children_[0]) {
    ParseNode *root = result.result_tree_->children_[0];
    ObItemType type = root->type_;
    if (T_SET_PASSWORD == type
        || T_DROP_USER == type
        || T_CREATE_USER == type
        || T_LOCK_USER == type
        || T_REVOKE == type
        || T_REVOKE_ALL == type
        || T_RENAME_USER == type
        || T_GRANT == type
        || T_CREATE_TABLE == type
        || T_ALTER_DATABASE == type
        || T_DROP_DATABASE == type
        || T_CREATE_TENANT == type
        || T_CREATE_STANDBY_TENANT == type
        || T_CREATE_VIEW == type
        || T_DROP_TABLE == type
        || T_DROP_INDEX == type
        || T_CREATE_DATABASE == type
        || T_MODIFY_TENANT == type
        || T_CREATE_INDEX == type
        || T_CREATE_MLOG == type
        || T_DROP_MLOG == type
        || T_DROP_TENANT == type
        /* pl item type*/
        || T_SP_CREATE_TYPE == type
        || T_SP_DROP_TYPE == type
        || T_SP_CREATE_TYPE_BODY == type
        || T_PACKAGE_CREATE == type
        || T_PACKAGE_CREATE_BODY == type
        || T_PACKAGE_ALTER == type
        || T_PACKAGE_DROP == type
        || T_SF_CREATE == type
        || T_SF_DROP == type
        || T_SF_ALTER == type
        || T_SP_CREATE == type
        || T_SP_DROP == type
        || T_SP_ALTER == type
        || T_TG_CREATE == type
        || T_TG_DROP == type
        || T_TG_ALTER == type
        /* pl item type*/) {
      ret = true;
    }
  }
  return ret;
}

bool ObSQLUtils::is_end_trans_stmt(const ParseResult &result)
{
  bool bret = false;
  if (NULL != result.result_tree_ &&
      NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    bret = (T_COMMIT == result.result_tree_->children_[0]->type_
            || T_ROLLBACK == result.result_tree_->children_[0]->type_) ? true : false;
  }
  return bret;
}

bool ObSQLUtils::is_commit_stmt(const ParseResult &result)
{
  bool bret = false;
  if (NULL != result.result_tree_ &&
      NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    bret = T_COMMIT == result.result_tree_->children_[0]->type_ ? true : false;
  }
  return bret;
}

bool ObSQLUtils::is_modify_tenant_stmt(ParseResult &result)
{
  ObItemType type = T_INVALID;
  if (NULL != result.result_tree_
      && NULL != result.result_tree_->children_
      && NULL != result.result_tree_->children_[0]) {
    type = result.result_tree_->children_[0]->type_;
  }
  return type == T_MODIFY_TENANT;
}

// 用于判断在mysql模式下prepare语句不支持的语句类型
bool ObSQLUtils::is_mysql_ps_not_support_stmt(const ParseResult &result)
{
  bool ret = false;
  if (NULL != result.result_tree_
      && NULL != result.result_tree_->children_
      && NULL != result.result_tree_->children_[0]) {
    ParseNode *root = result.result_tree_->children_[0];
    ObItemType type = root->type_;
    switch (type) {
      case T_SP_CREATE:
      case T_SP_DROP: {
        ret = true;
        break;
      }
      default: {
        ret = false;
      }
    }
  }
  return ret;
}

bool ObSQLUtils::is_readonly_stmt(ParseResult &result)
{
  bool ret = false;
  if (NULL != result.result_tree_
      && NULL != result.result_tree_->children_
      && NULL != result.result_tree_->children_[0]) {
    ParseNode *root = result.result_tree_->children_[0];
    ObItemType type = root->type_;
    if (T_SELECT == type && NULL == root->children_[PARSE_SELECT_FOR_UPD]) {
      ret = true;
    } else if (T_EXPLAIN == type
               && NULL != root->children_[1]
               && root->children_[1]->type_ == T_SELECT
               && NULL == root->children_[1]->children_[PARSE_SELECT_FOR_UPD]) {
      ret = true;
    } else if (T_SHOW_COLUMNS == type
               || T_SHOW_TABLES == type
               || T_SHOW_DATABASES == type
               || T_SHOW_TABLE_STATUS == type
               || T_SHOW_SERVER_STATUS == type
               || T_SHOW_VARIABLES == type
               || T_SHOW_SCHEMA == type
               || T_SHOW_CREATE_DATABASE == type
               || T_SHOW_CREATE_TABLE == type
               || T_SHOW_CREATE_VIEW == type
               || T_SHOW_WARNINGS == type
               || T_SHOW_ERRORS == type
               || T_SHOW_GRANTS == type
               || T_SHOW_CHARSET == type
               || T_SHOW_COLLATION == type
               || T_SHOW_PARAMETERS == type
               || T_SHOW_INDEXES == type
               || T_SHOW_PROCESSLIST == type
               || T_SHOW_TABLEGROUPS == type
               || T_SHOW_TRIGGERS == type
               || T_HELP == type
               || T_USE_DATABASE == type
               || T_TRANSACTION == type
               || T_BEGIN == type
               || T_COMMIT == type
               || T_ROLLBACK == type
               || T_VARIABLE_SET == type
               || T_SET_NAMES == type //read only not restrict it
               || T_SET_CHARSET == type  //read only not restrict it
               || T_SHOW_RECYCLEBIN == type
               || T_SHOW_PROFILE == type
               || T_SHOW_TENANT == type
               || T_SHOW_RESTORE_PREVIEW == type
               || T_SHOW_SEQUENCES == type
               || T_SHOW_ENGINE == type
               || T_SHOW_OPEN_TABLES == type
               || (T_SET_ROLE == type && lib::is_mysql_mode())) {
      ret = true;
    }
  }
  return ret;
}

int ObSQLUtils::make_field_name(const char *src,
                                const int64_t len,
                                const ObCollationType cs_type,
                                ObIAllocator *allocator,
                                ObString &name)
{
  /*
   * select ' abc'; 显示的列名种左边的联系的非字母非数字非标点的字符将会被过滤掉。
   * select 'acc' as ' adf';alias name 需要做同样的处理。此外，显示的列明最多保留256个字符。
   */
  int ret = OB_SUCCESS;
  if (len < 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(len), K(allocator), K(ret));
  } else if (NULL == src || 0 == len) {
    name.assign_ptr(src, static_cast<int32_t>(len));
  } else {
    int64_t new_length = len;
    const char *new_start = src;
    while (new_length && !ObCharset::is_graph(cs_type, *new_start)) {
      new_start++;
      new_length--;
    }

    int64_t final_len = min(new_length, OB_MAX_ALIAS_NAME_LENGTH);
    if (new_length == len) {
      name.assign_ptr(src, static_cast<int32_t>(final_len));
    } else {
      char *buf = reinterpret_cast<char *>(allocator->alloc(final_len + 1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory", K(ret));
      } else {
        MEMCPY(buf, new_start, final_len);
        buf[final_len] = '\0';
        name.assign_ptr(buf, static_cast<int32_t>(final_len));
      }
    }
  }
  return ret;
}

int ObSQLUtils::set_compatible_cast_mode(const ObSQLSessionInfo *session, ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument in set_compatible_cast_mode ", K(session), K(ret), K(cast_mode), K(lbt()));
  } else if (is_oracle_mode()) {
    cast_mode &= ~CM_WARN_ON_FAIL;
    cast_mode |= CM_ORACLE_MODE;
  } else {
    cast_mode &= ~CM_ORACLE_MODE;
  }
  return ret;
}

int ObSQLUtils::get_default_cast_mode(const stmt::StmtType &stmt_type,
                                      const ObSQLSessionInfo *session,
                                      ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  } else {
    get_default_cast_mode(stmt_type, session->is_ignore_stmt(), session->get_sql_mode(), cast_mode);
  }
  return ret;
}

void ObSQLUtils::get_default_cast_mode(const stmt::StmtType &stmt_type,
                                      bool is_ignore_stmt,
                                      ObSQLMode sql_mode,
                                      ObCastMode &cast_mode)
{
  cast_mode = CM_NONE;
  if (is_oracle_mode() && stmt::T_EXPLAIN != stmt_type) {
    cast_mode = CM_ORACLE_MODE;
  } else if (stmt::T_SELECT == stmt_type
             || stmt::T_EXPLAIN == stmt_type
             || (!is_strict_mode(sql_mode))
             || is_ignore_stmt) {
    cast_mode = CM_WARN_ON_FAIL;
  }
  if (is_mysql_mode()) {
    if (is_allow_invalid_dates(sql_mode)) {
      cast_mode |= CM_ALLOW_INVALID_DATES;
    }
    if (is_no_zero_date(sql_mode)) {
      cast_mode |= CM_NO_ZERO_DATE;
    }
    if (is_time_truncate_fractional(sql_mode)) {
      cast_mode |= CM_TIME_TRUNCATE_FRACTIONAL;
    }
  }
}

void ObSQLUtils::set_insert_update_scope(ObCastMode &cast_mode)
{
  cast_mode = cast_mode | CM_INSERT_UPDATE_SCOPE;
}

bool ObSQLUtils::is_insert_update_scope(ObCastMode &cast_mode)
{
  return (cast_mode & CM_INSERT_UPDATE_SCOPE);
}

int ObSQLUtils::get_default_cast_mode(const ObPhysicalPlan *plan,
                                      const ObSQLSessionInfo *session,
                                      ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan), K(ret));
  } else if (OB_FAIL(get_default_cast_mode(plan->get_stmt_type(), session, cast_mode))) {
    LOG_WARN("fail to get default cast mode", K(ret));
  }
  return ret;
}

int ObSQLUtils::get_default_cast_mode(const ObSQLSessionInfo *session, ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan or session is NULL", K(ret));
  } else {
    get_default_cast_mode(session->get_sql_mode(), cast_mode);
  }
  return ret;
}

void ObSQLUtils::get_default_cast_mode(const ObSQLMode sql_mode, ObCastMode &cast_mode)
{
  cast_mode = CM_NONE;
  if (is_oracle_mode()) {
    cast_mode = CM_ORACLE_MODE;
  } else if (!is_strict_mode(sql_mode)) {
    cast_mode = CM_WARN_ON_FAIL;
  }
  if (is_mysql_mode()) {
    if (is_allow_invalid_dates(sql_mode)) {
      cast_mode |= CM_ALLOW_INVALID_DATES;
    }
    if (is_no_zero_date(sql_mode)) {
      cast_mode |= CM_NO_ZERO_DATE;
    }
    if (is_time_truncate_fractional(sql_mode)) {
      cast_mode |= CM_TIME_TRUNCATE_FRACTIONAL;
    }
  }
}

int ObSQLUtils::get_default_cast_mode(const bool is_explicit_cast,
                                      const uint32_t result_flag,
                                      const ObSQLSessionInfo *session,
                                      ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret), KP(session));
  } else {
    ObSQLUtils::get_default_cast_mode(is_explicit_cast, result_flag,
                                      session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      session->get_sql_mode(),
                                      cast_mode);
  }
  return ret;
}

void ObSQLUtils::get_default_cast_mode(const bool is_explicit_cast,
                                      const uint32_t result_flag,
                                      const stmt::StmtType &stmt_type,
                                      bool is_ignore_stmt,
                                      ObSQLMode sql_mode,
                                      ObCastMode &cast_mode)
{
  cast_mode = CM_NONE;
  ObSQLUtils::get_default_cast_mode(stmt_type, is_ignore_stmt, sql_mode, cast_mode);
  if (is_explicit_cast) {
    cast_mode |= CM_EXPLICIT_CAST;
  } else {
    cast_mode &= ~CM_EXPLICIT_CAST;
  }
  if (result_flag & ZEROFILL_FLAG) {
    cast_mode |= CM_ZERO_FILL;
  }
  if (!is_oracle_mode() && is_strict_mode(sql_mode)) {
    cast_mode |= CM_NONE;
    cast_mode |= CM_STRICT_MODE;
  }
  if (is_mysql_mode()) {
    if (is_allow_invalid_dates(sql_mode)) {
      cast_mode |= CM_ALLOW_INVALID_DATES;
    }
    if (is_no_zero_date(sql_mode)) {
      cast_mode |= CM_NO_ZERO_DATE;
    }
    if (is_time_truncate_fractional(sql_mode)) {
      cast_mode |= CM_TIME_TRUNCATE_FRACTIONAL;
    }
  }
  cast_mode |= CM_FORMAT_NUMBER_WITH_LIMIT;
  LOG_DEBUG("in get_default_cast_mode", K(is_explicit_cast),
      K(result_flag), K(stmt_type), K(cast_mode), K(sql_mode));
}

int ObSQLUtils::get_cast_mode_for_replace(const ObRawExpr *expr,
                                          const ObExprResType &dst_type,
                                          const ObSQLSessionInfo *session,
                                          ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KP(expr), KP(session), K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false,/* explicit_cast */
                                                       0,    /* result_flag */
                                                       session, cast_mode))) {
    LOG_WARN("failed to get default cast mode", K(ret));
  } else if (OB_FAIL(ObSQLUtils::set_cs_level_cast_mode(expr->get_collation_level(), cast_mode))) {
    LOG_WARN("failed to set cs level cast mode", K(ret));
  } else if (lib::is_mysql_mode() && dst_type.is_string_type() &&
             expr->get_result_type().has_result_flag(ZEROFILL_FLAG)) {
    cast_mode |= CM_ADD_ZEROFILL;
  }
  cast_mode = CM_SET_BY_TRANSFORMERN(cast_mode);
  return ret;
}

ObCollationLevel ObSQLUtils::transform_cs_level(const ObCollationLevel cs_level)
{
  // CS_LEVEL_INVALID is not defined as 0, transform the input cs level to make the default 0
  // representing CS_LEVEL_INVALID
  ObCollationLevel tmp_cs_level = cs_level;
  if (CS_LEVEL_EXPLICIT == cs_level) {
    tmp_cs_level = CS_LEVEL_INVALID;
  } else if (CS_LEVEL_INVALID == cs_level) {
    tmp_cs_level = CS_LEVEL_EXPLICIT;
  }
  return tmp_cs_level;
}

int ObSQLUtils::set_cs_level_cast_mode(const ObCollationLevel cs_level, ObCastMode &cast_mode)
{
  int ret = OB_SUCCESS;
  ObCollationLevel tmp_cs_level = transform_cs_level(cs_level);
  CM_SET_CS_LEVEL(cast_mode, tmp_cs_level);
  return ret;
}

int ObSQLUtils::get_cs_level_from_cast_mode(const ObCastMode cast_mode,
                                            const ObCollationLevel default_level,
                                            ObCollationLevel &cs_level)
{
  int ret = OB_SUCCESS;
  if (CM_IS_EXPLICIT_CAST(cast_mode)) {
    cs_level = CS_LEVEL_IMPLICIT;
  } else {
    ObCollationLevel tmp_cs_level = static_cast<ObCollationLevel>(CM_GET_CS_LEVEL(cast_mode));
    tmp_cs_level = transform_cs_level(tmp_cs_level);
    if (CS_LEVEL_INVALID == tmp_cs_level) {
      cs_level = default_level;
    } else {
      cs_level = tmp_cs_level;
    }
  }
  return ret;
}

int ObSQLUtils::check_well_formed_str(const ObString &src_str,
                                      const ObCollationType cs_type,
                                      ObString &dst_str,
                                      bool &is_null,
                                      const bool is_strict_mode,
                                      const bool ret_error)
{
  int ret = OB_SUCCESS;
  // make sure src_str is from varchar obj
  is_null = false;
  int64_t str_len = src_str.length();
  int64_t well_formed_length = 0;
  int32_t well_formed_error = 0;
  if (CS_TYPE_INVALID == cs_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collation type", K(ret), K(src_str), K(cs_type));
  } else if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                                src_str.ptr(),
                                                str_len,
                                                well_formed_length,
                                                well_formed_error))) {
    LOG_WARN("fail to check well_formed_len", K(ret), K(src_str), K(cs_type));
  } else if (well_formed_length < str_len) {
    // mysql是这样判断的，其实用well_formed_error来判断也可以
    int32_t diff = static_cast<int32_t>(str_len - well_formed_length);
    diff = diff > 3 ? 3 : diff;
    char hex_buf[7] = {0};
    int64_t hex_len = 0;
    const char *charset_name = ObCharset::charset_name(cs_type);
    int64_t charset_name_len = strlen(charset_name);
    if (OB_FAIL(common::hex_print(src_str.ptr() + well_formed_length,
                                  diff, hex_buf, sizeof(hex_buf), hex_len))) {
      LOG_WARN("Failed to transform to hex cstr", K(ret), K(src_str),
                K(well_formed_length));
    } else if (ret_error) {
      ret = OB_ERR_INVALID_CHARACTER_STRING;
      LOG_USER_ERROR(OB_ERR_INVALID_CHARACTER_STRING,
                     (int)charset_name_len, charset_name, (int)hex_len, hex_buf);
    } else if (is_strict_mode) {
      dst_str.reset();
      is_null = true;
    } else {
      dst_str.assign_ptr(src_str.ptr(), static_cast<int32_t>(well_formed_length));
    }
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      LOG_USER_WARN(OB_ERR_INVALID_CHARACTER_STRING,
          static_cast<int>(charset_name_len), charset_name,
          static_cast<int>(hex_len), hex_buf);
    }
  } else {
    dst_str = src_str;
  }
  return ret;
}

int ObSQLUtils::check_well_formed_str(const ObObj &src,
                                      ObObj &dest,
                                      const bool is_strict_mode,
                                      const bool ret_error)
{
  int ret = OB_SUCCESS;
  if (!src.is_varchar_or_char() && !src.is_varbinary() && !src.is_binary()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else {
    ObString src_str;
    ObString dst_str;
    bool is_null = false;
    if (OB_FAIL(src.get_varchar(src_str))) { // 必须为varchar类型
      LOG_WARN("fail to get varchar", K(ret), K(src));
    } else if (OB_FAIL(check_well_formed_str(src_str, src.get_collation_type(),
                                             dst_str, is_null,
                                             is_strict_mode, ret_error))) {
      LOG_WARN("check_well_formed_str failed", K(ret), K(src_str), K(is_strict_mode),
                                               K(ret_error));
    } else if (is_null) {
      dest.set_null();
    } else {
      dest.set_varchar(dst_str);
      dest.set_collation_level(src.get_collation_level());
      dest.set_collation_type(src.get_collation_type());
    }
  }
  return ret;
}

//这个函数需要换一下，直接通过sql_id获取outline key。
int ObSQLUtils::get_outline_key(ObIAllocator &allocator,
                                const ObSQLSessionInfo *session,
                                const ObString &query_sql,
                                ObString &outline_key,
                                ObMaxConcurrentParam::FixParamStore &fix_param_store,
                                ParseMode parse_mode,
                                bool &has_questionmark_in_sql)
{
  int ret = OB_SUCCESS;
  has_questionmark_in_sql = false;
  bool need_parser = !session->get_enable_exact_mode();
  bool check_param = true;

  if (!need_parser && FP_PARAMERIZE_AND_FILTER_HINT_MODE == parse_mode) {
    need_parser = true;
    parse_mode = FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE;
    check_param = false;
  }

  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, session is NULL", K(ret));
  } else if (!need_parser) {
    char *buf = NULL;
    int64_t pos_s = 0;
    int64_t size = query_sql.get_serialize_size();
    if (0 == size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query sql serialize size is 0", K(ret), K(query_sql));
    } else if (OB_ISNULL(buf = (char *)allocator.alloc(size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc mem", K(ret));
    } else if (OB_FAIL(query_sql.serialize(buf, size, pos_s))) {
        LOG_WARN("fail to serialize key", K(ret));
    } else if (FALSE_IT(outline_key.assign_ptr(buf,
                        static_cast<ObString::obstr_size_t>(pos_s)))) {
      // do nothing
    }
  } else {
    //快速参数化后带？的sql
    ObString no_param_sql;
    ParseResult parse_result;
    ObParser parser(allocator, session->get_sql_mode(), session->get_charsets4parser());
    ObSEArray<ObPCParam *, OB_PC_SPECIAL_PARAM_COUNT> special_params;
    ObString param_sql;
    ParamStore params( (ObWrapperAllocator(allocator)) );
    ObSqlTraits sql_traits;
    ObSEArray<ObPCParam *, OB_PC_RAW_PARAM_COUNT> raw_params;
    SqlInfo sql_info;
    char *buf = NULL;
    int32_t pos = 0;
    const bool is_transform_outline = true;
    const bool is_parameterized_execute = false;
    ParseNode *type_node = NULL;
    sql_info.need_check_fp_ = false;
    if (OB_FAIL(parser.parse(query_sql, parse_result))) {
      LOG_WARN("Generate syntax tree failed", "sql", query_sql, K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("parse result tree not inited", K(parse_result.result_tree_), K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_->children_)
               || OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), KP(parse_result.result_tree_->children_),
               "number of children", parse_result.result_tree_->num_child_);
    } else if (NULL == (type_node = parse_result.result_tree_->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(type_node));
    } else if (!IS_DML_STMT(type_node->type_)) {
      ret = OB_SQL_DML_ONLY;
      LOG_TRACE("statement not dml sql", K(type_node));
    } else if (check_param && OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator,
                                                                    *session,
                                                                    NULL,
                                                                    parse_result.result_tree_,
                                                                    sql_info,
                                                                    params,
                                                                    NULL,
                                                                    fix_param_store,
                                                                    is_transform_outline))) {
      if (OB_NOT_SUPPORTED != ret) {
        SQL_PC_LOG(WARN, "fail to transform syntax_tree", K(ret));
      }
    } else if (OB_FAIL(ObSqlParameterization::raw_fast_parameterize_sql(allocator,
                                                                        *session,
                                                                        query_sql,
                                                                        no_param_sql,
                                                                        raw_params,
                                                                        parse_mode))) {
      LOG_WARN("fail to fast_parameterize_sql", K(ret));
    } else if (check_param && OB_FAIL(ObSqlParameterization::check_and_generate_param_info(raw_params,
                                                                            sql_info,
                                                                            special_params))) {
      if (OB_NOT_SUPPORTED == ret) {
        LOG_TRACE("fail to check and generate not params",
                 K(ret), K(query_sql), K(no_param_sql));
      } else {
        LOG_WARN("fail to check and generate not params",
                 K(ret), K(query_sql), K(no_param_sql));
      }
    } else if (OB_UNLIKELY(NULL == (buf = (char *)allocator.alloc(query_sql.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc buf", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::construct_sql(no_param_sql, special_params, buf, query_sql.length(), pos))) {
      LOG_WARN("fail to construct_sql", K(ret), K(no_param_sql), K(special_params.count()));
    } else {
      ObString constructed_sql(pos, buf);
      int64_t size = constructed_sql.get_serialize_size();
      if (0 == size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("constructed_sql serialize size is 0", K(ret), K(constructed_sql));
      } else {
        char *buf = NULL;
        int64_t pos_s = 0;
        if (OB_UNLIKELY(NULL == (buf = (char *)allocator.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("fail to alloc mem", K(ret));
        } else if (OB_FAIL(constructed_sql.serialize(buf, size, pos_s))) {
          LOG_WARN("fail to serialize key", K(ret));
        } else {
          outline_key.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos_s));
          if (params.count() != fix_param_store.count()) {
            has_questionmark_in_sql = true;
          }
        }
      }
    }
    parser.free_result(parse_result);
  }
  return ret;
}

int ObSQLUtils::filter_hint_in_query_sql(ObIAllocator &allocator,
                                         const ObSQLSessionInfo &session,
                                         const ObString &sql,
                                         ObString &param_sql)
{
  int ret = OB_SUCCESS;
  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  ParseResult parse_result;
  if (OB_FAIL(parser.parse(sql, parse_result, FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE))) {
    SQL_PC_LOG(WARN, "fail to parse query while filter hint", K(ret));
  } else {
    param_sql.assign(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
    parser.free_result(parse_result);
  }
  return ret;
}

int ObSQLUtils::construct_outline_sql(ObIAllocator &allocator,
                                      const ObSQLSessionInfo &session,
                                      const ObString &outline_content,
                                      const ObString &orig_sql,
                                      bool is_need_filter_hint,
                                      ObString &outline_sql)
{
  int ret = OB_SUCCESS;
  UNUSED(is_need_filter_hint);
  ObString filter_sql;
  ObSqlString sql_helper;
  // 该接口会将注释和hint均去掉
  if (OB_FAIL(filter_hint_in_query_sql(allocator, session, orig_sql, filter_sql))) {
    LOG_WARN("fail to filter hint", K(ret));
  } else if (OB_FAIL(filter_head_space(filter_sql))) {
    LOG_WARN("fail to filter head space", K(ret));
  }
  if (OB_SUCC(ret)) {
    char empty_split = find_first_empty_char(filter_sql);
    ObString first_token = filter_sql.split_on(empty_split);
    if (OB_FAIL(sql_helper.assign_fmt("%.*s %.*s%.*s", first_token.length(), first_token.ptr(),
                                      outline_content.length(), outline_content.ptr(),
                                      filter_sql.length(), filter_sql.ptr()))) {
       LOG_WARN("failed to construct new sql", K(first_token), K(orig_sql),
                                 K(filter_sql), K(outline_content), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, sql_helper.string(), outline_sql))) {
      LOG_WARN("failed to write string", K(first_token), K(orig_sql),
                                         K(filter_sql), K(outline_content), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

// 将sql中开头为' ', '\r', '\n', '\t', '\f'的字符去除
int ObSQLUtils::filter_head_space(ObString &sql)
{
  int ret = OB_SUCCESS;
  int64_t head_space_len = 0;
  bool is_space = true;
  for (int64_t i = 0; is_space && i < sql.length(); i++) {
    char ch = sql[i];
    if (' ' == ch || '\r' == ch || '\n' == ch || '\t' == ch || '\f' == ch) {
      ++head_space_len;
    } else {
      is_space = false;
    }
  }
  sql.assign(sql.ptr() + head_space_len, sql.length() - head_space_len);
  return ret;
}

char ObSQLUtils::find_first_empty_char(const ObString &sql)
{
  char empty_char = ' '; // default split
  for (int64_t i = 0; i < sql.length(); ++i) {
    char ch = sql[i];
    if (' ' == ch || '\r' == ch || '\n' == ch || '\t' == ch || '\f' == ch) {
      empty_char = ch;
      break;
    }
  }
  return empty_char;
}

int ObSQLUtils::reconstruct_sql(ObIAllocator &allocator, const ObStmt *stmt, ObString &sql,
                                ObSchemaGetterGuard *schema_guard,
                                ObObjPrintParams print_params,
                                const ParamStore *param_store,
                                const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObSqlPrinter sql_printer(stmt, schema_guard, print_params, param_store, session);
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(stmt), K(ret));
  } else if (OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else if (!stmt->is_dml_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected stmt type", K(stmt->get_stmt_type()), K(stmt->get_query_ctx()->get_sql_stmt()), K(ret));
  } else if (OB_FAIL(sql_printer.do_print(allocator, sql))) {
    LOG_WARN("failed to print sql", K(ret));
  }
  return ret;
}

int ObISqlPrinter::do_print(ObIAllocator &allocator, ObString &result)
{
  int ret = OB_SUCCESS;
  //First try 64K buf on the stack, if it fails, then try 128K.
  //If it still fails, allocate 256K from the heap. If it continues to fail, expand twice each time.
  int64_t res_len = 0;
  SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
    MEMSET(buf, 0, sizeof(buf));
    if (OB_FAIL(inner_print(buf, sizeof(buf), res_len))) {
        LOG_WARN("failed to print", K(sizeof(buf)), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(res_len, buf), result))) {
      LOG_WARN("fail to deep copy string", K(ret));
    }
  }
  if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_SUCCESS;
    SMART_VAR(char[OB_MAX_SQL_LENGTH * 2], buf) {
      MEMSET(buf, 0, sizeof(buf));
      if (OB_FAIL(inner_print(buf, sizeof(buf), res_len))) {
        LOG_WARN("failed to print", K(sizeof(buf)), K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString(res_len, buf), result))) {
        LOG_WARN("fail to deep copy string", K(ret));
      }
    }
  }
  if (OB_SIZE_OVERFLOW == ret) {
    bool is_succ = false;
    ret = OB_SUCCESS;
    for (int64_t i = 4; OB_SUCC(ret) && !is_succ && i <= 1024; i = i * 2) {
      ObArenaAllocator alloc;
      const int64_t length = OB_MAX_SQL_LENGTH * i;
      char *buf = NULL;
      if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for sql", K(ret), K(length));
      } else if (FALSE_IT(MEMSET(buf, 0, length))) {
      } else if (OB_FAIL(inner_print(buf, length, res_len))) {
        LOG_WARN("failed to print", K(sizeof(buf)), K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, ObString(res_len, buf), result))) {
        LOG_WARN("fail to deep copy string", K(ret));
      }
      if (OB_SUCC(ret)) {
        is_succ = true;
      } else if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObSqlPrinter::inner_print(char *buf, int64_t buf_len, int64_t &res_len)
{
  return ObSQLUtils::print_sql(buf,
                               buf_len,
                               res_len,
                               stmt_,
                               schema_guard_,
                               print_params_,
                               param_store_,
                               session_);
}

int ObSQLUtils::print_sql(char *buf,
                          int64_t buf_len,
                          int64_t &res_len,
                          const ObStmt *stmt,
                          ObSchemaGetterGuard *schema_guard,
                          ObObjPrintParams print_params,
                          const ParamStore *param_store,
                          const ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDMLStmt *reconstruct_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_explain_stmt()) {
    if (OB_ISNULL(reconstruct_stmt = static_cast<const ObExplainStmt*>(stmt)->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Explain query stmt is NULL", K(ret));
    } else {
      BUF_PRINTF("EXPLAIN ");
    }
  } else if (stmt->is_dml_stmt()) {
    reconstruct_stmt = static_cast<const ObDMLStmt*>(stmt);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Invalid stmt type", K(stmt->get_stmt_type()), K(stmt->get_query_ctx()->get_sql_stmt()), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type");
  }
  if (OB_SUCC(ret)) {
    switch (reconstruct_stmt->get_stmt_type()) {
    case stmt::T_SELECT: {
      ObSelectStmtPrinter printer(buf,
                                  buf_len,
                                  &pos,
                                  static_cast<const ObSelectStmt*>(reconstruct_stmt),
                                  schema_guard,
                                  print_params,
                                  param_store,
                                  false,
                                  session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      printer.enable_print_temp_table_as_cte();
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("fail to print select stmt", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case stmt::T_INSERT_ALL: {
      ObInsertAllStmtPrinter printer(buf,
                                    buf_len,
                                    &pos,
                                    static_cast<const ObInsertAllStmt*>(reconstruct_stmt),
                                    schema_guard,
                                    print_params,
                                    param_store,
                                    session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("fail to print insert stmt", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case stmt::T_REPLACE:
    case stmt::T_INSERT: {
      ObInsertStmtPrinter printer(buf,
                                  buf_len,
                                  &pos,
                                  static_cast<const ObInsertStmt*>(reconstruct_stmt),
                                  schema_guard,
                                  print_params,
                                  param_store,
                                  session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("fail to print insert stmt", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case stmt::T_DELETE: {
      ObDeleteStmtPrinter printer(buf,
                                  buf_len,
                                  &pos,
                                  static_cast<const ObDeleteStmt*>(reconstruct_stmt),
                                  schema_guard,
                                  print_params,
                                  param_store,
                                  session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("fail to print delete stmt", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case stmt::T_UPDATE: {
      ObUpdateStmtPrinter printer(buf,
                                  buf_len,
                                  &pos,
                                  static_cast<const ObUpdateStmt*>(reconstruct_stmt),
                                  schema_guard,
                                  print_params,
                                  param_store,
                                  session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("fail to print update stmt", K(ret));
      } else { /*do nothing*/ }
    }
      break;
    case stmt::T_MERGE: {
      ObMergeStmtPrinter printer(buf,
                                  buf_len,
                                  &pos,
                                  static_cast<const ObMergeStmt*>(reconstruct_stmt),
                                  schema_guard,
                                  print_params,
                                  param_store,
                                  session);
      printer.set_is_root(true);
      printer.set_is_first_stmt_for_hint(true);
      if (OB_FAIL(printer.do_print())) {
        LOG_WARN("failed to print merge stmt", K(ret));
      }
    }
      break;
    default: {

    }
      break;
    }
  }
  res_len = pos;
  return ret;
}

int ObSQLUtils::wrap_expr_ctx(
    const stmt::StmtType &stmt_type,
    ObExecContext &exec_ctx,
    common::ObIAllocator &allocator,
    common::ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = NULL;
  const ObTimeZoneInfo *tz_info = NULL;
  int64_t tz_offset = 0;
  if (OB_ISNULL(my_session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(tz_info = get_timezone_info(my_session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tz info pointer failed", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("get tz offset failed", K(ret));
  } else if (OB_FAIL(get_default_cast_mode(stmt_type, my_session, expr_ctx.cast_mode_))) {
    LOG_WARN("Failed to get default cast mode", K(ret));
  } else {
    expr_ctx.exec_ctx_ = &exec_ctx;
    expr_ctx.calc_buf_ = &allocator;
    expr_ctx.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();
    expr_ctx.my_session_ = my_session;
    expr_ctx.tz_offset_ = tz_offset;
    if (OB_FAIL(wrap_column_convert_ctx(expr_ctx, expr_ctx.column_conv_ctx_))) {
      LOG_WARN("wrap column convert ctx failed", K(ret));
    }
  }
  return ret;
}

bool ObSQLUtils::is_same_type(const ObExprResType &type1, const ObExprResType &type2)
{
  return is_same_type(type1.get_obj_meta(), type2.get_obj_meta(), type1.get_accuracy(), type2.get_accuracy());
}

bool ObSQLUtils::is_same_type(const ObObjMeta &meta1,
                              const ObObjMeta &meta2,
                              const ObAccuracy &accuracy1,
                              const ObAccuracy &accuracy2)
{
  return meta1.get_type() == meta2.get_type()
      && meta1.get_collation_type() == meta2.get_collation_type()
      && meta1.get_scale() == meta2.get_scale()
      && accuracy1 == accuracy2;
}

int RelExprCheckerBase::init(int64_t bucket_num)
{
  return duplicated_checker_.create(bucket_num);
}

int RelExprCheckerBase::add_exprs(common::ObIArray<ObRawExpr*> &exprs)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(add_expr(exprs.at(i)))) {
      LOG_WARN("add expression to relation expr checker failed", K(ret));
    }
  }
  return ret;
}

int RelExprChecker::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_HASH_NOT_EXIST == (ret = duplicated_checker_.exist_refactored(reinterpret_cast<uint64_t>(expr)))) {
    if (OB_FAIL(duplicated_checker_.set_refactored(reinterpret_cast<uint64_t>(expr)))) {
      LOG_WARN("set expr to duplicated checker failed", K(ret), K(duplicated_checker_.size()));
    } else if (OB_FAIL(rel_array_.push_back(expr))) {
      LOG_WARN("push expr to relation array failed", K(ret));
    }
  } else if (OB_HASH_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("check expr in duplicated checker", K(ret));
  }
  return ret;
}

FastRelExprChecker::FastRelExprChecker(common::ObIArray<ObRawExpr *> &rel_array)
  : RelExprCheckerBase(), rel_array_(rel_array), init_size_(rel_array.count())
{

}

FastRelExprChecker::~FastRelExprChecker()
{
  for (int64_t i = init_size_; i < rel_array_.count(); ++i) {
    if (OB_ISNULL(rel_array_.at(i))) {
      // do nothing
    } else {
      rel_array_.at(i)->clear_flag(BE_USED);
    }
  }
}

int FastRelExprChecker::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is invalid", K(ret));
  } else if (expr->has_flag(BE_USED)) {
    // do nothing
  } else if (OB_FAIL(rel_array_.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(expr->add_flag(BE_USED))) {
    LOG_WARN("failed to add flag", K(ret));
  }
  return ret;
}

int RelExprPointerChecker::init(int64_t bucket_num)
{
  return expr_id_map_.create(bucket_num, common::ObModIds::OB_SQL_EXPR);
}

int RelExprPointerChecker::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  int flag = 0;
  uint64_t expr_id = UINT64_MAX;
  if (OB_HASH_NOT_EXIST == (flag = expr_id_map_.get_refactored(reinterpret_cast<uint64_t>(expr), expr_id))) {
    ObRawExprPointer pointer;
    expr_id = rel_array_.count();
    if (OB_FAIL(expr_id_map_.set_refactored(reinterpret_cast<uint64_t>(expr), expr_id))) {
      LOG_WARN("set expr to duplicated checker failed", K(ret), K(duplicated_checker_.size()));
    } else if (OB_FAIL(rel_array_.push_back(pointer))) {
      LOG_WARN("failed to push back new array", K(ret));
    }
  } else if (OB_SUCCESS == flag) {
    // do nothing
  } else {
    ret = flag;
    LOG_WARN("failed to get from hash map", K(ret));
  }
  if (OB_SUCC(ret) && expr_id < rel_array_.count()) {
    if (OB_FAIL(rel_array_.at(expr_id).add_ref(&expr))) {
      LOG_WARN("failed to push expr into array", K(ret));
    }
  }
  return ret;
}

int AllExprPointerCollector::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rel_array_.push_back(&expr))) {
    LOG_WARN("push expr to relation array failed", K(ret));
  }

  return ret;
}


FastUdtExprChecker::FastUdtExprChecker(common::ObIArray<ObRawExpr *> &rel_array)
  : RelExprCheckerBase(), rel_array_(rel_array), init_size_(rel_array.count())
{

}

int FastUdtExprChecker::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::extract_udt_exprs(expr, rel_array_))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

JsonObjectStarChecker::JsonObjectStarChecker(common::ObIArray<ObRawExpr *> &rel_array)
  : RelExprCheckerBase(), rel_array_(rel_array), init_size_(rel_array.count())
{

}

int JsonObjectStarChecker::add_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransformUtils::extract_json_object_exprs(expr, rel_array_))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

//used for C module
bool check_stack_overflow_c()
{
  bool is_overflow = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(oceanbase::common::check_stack_overflow(is_overflow))) {
    LOG_ERROR("fail to check stack overflow", K(ret));
  }
  return is_overflow;
}

int ObSQLUtils::extract_pre_query_range(const ObQueryRange &pre_query_range,
                                        ObIAllocator &allocator,
                                        ObExecContext &exec_ctx,
                                        ObQueryRangeArray &key_ranges,
                                        const ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;
  bool dummy_all_single_value_ranges = false;
  if (OB_FAIL(pre_query_range.get_tablet_ranges(allocator, exec_ctx, key_ranges,
                                                dummy_all_single_value_ranges,
                                                dtc_params))) {
    LOG_WARN("failed to get tablet ranges", K(ret));
  }
  return ret;
}

int ObSQLUtils::extract_geo_query_range(const ObQueryRange &pre_query_range,
                                          ObIAllocator &allocator,
                                          ObExecContext &exec_ctx,
                                          ObQueryRangeArray &key_ranges,
                                          ObMbrFilterArray &mbr_filters,
                                          const ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;
  bool dummy_all_single_value_ranges = false;
  if (OB_LIKELY(!pre_query_range.need_deep_copy())) {
    //对于大多数查询来说，query条件是非常规范和工整的，这种条件我们不需要拷贝进行graph的变化，可以直接提取
    if (OB_FAIL(pre_query_range.direct_get_tablet_ranges(allocator,
                                                        exec_ctx,
                                                        key_ranges,
                                                        dummy_all_single_value_ranges,
                                                        dtc_params))) {
      LOG_WARN("fail to get tablet ranges", K(ret));
    } else {
      const MbrFilterArray &pre_filters = pre_query_range.get_mbr_filter();
      FOREACH_X(it, pre_filters, OB_SUCC(ret) && it != pre_filters.end()) {
        if (OB_FAIL(mbr_filters.push_back(*it))) {
          LOG_WARN("store mbr_filters_ failed", K(ret));
        }
      }
    }
  } else {
    ObQueryRange final_query_range(allocator);
    if (OB_FAIL(final_query_range.deep_copy(pre_query_range))) {
      // MUST deep copy to make it thread safe
      LOG_WARN("fail to create final query range", K(ret), K(pre_query_range));
    } else if (OB_FAIL(final_query_range.final_extract_query_range(exec_ctx, dtc_params))) {
      LOG_WARN("fail to final extract query range", K(ret), K(final_query_range));
    } else if (OB_FAIL(final_query_range.get_tablet_ranges(key_ranges,
                                                           dummy_all_single_value_ranges,
                                                           dtc_params))) {
      LOG_WARN("fail to get tablet ranges from query range", K(ret), K(final_query_range));
    } else {
      const MbrFilterArray &pre_filters = final_query_range.get_mbr_filter();
      FOREACH_X(it, pre_filters, OB_SUCC(ret) && it != pre_filters.end()) {
        if (OB_FAIL(mbr_filters.push_back(*it))) {
          LOG_WARN("store mbr_filters_ failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::extract_equal_pre_query_range(const ObQueryRange &pre_query_range,
                                              void *range_buffer,
                                              const ParamStore &param_store,
                                              ObQueryRangeArray &key_ranges)
{
  int ret = OB_SUCCESS;
  bool include_null = false;  // the value from ParamStore set to ObNewRange whether null
  int64_t include_border = 0;  // the numer of ObNewRange border is included
  // set ptr about ObNewRange
  int64_t column_count = pre_query_range.get_column_count();
  ObNewRange *key_range = static_cast<ObNewRange*>(range_buffer);
  ObObj *start = reinterpret_cast<ObObj*>(static_cast<char*>(range_buffer) + sizeof(ObNewRange));
  ObObj *end = start + column_count;
  // init start and end ObObj
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    (start + i)->set_min_value();
    (end + i)->set_max_value();
  }
  // refresh start and end ObObj
  const ObIArray<ObQueryRange::ObEqualOff>& equal_offs = pre_query_range.get_raw_equal_offs();
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_offs.count(); ++i) {
    int64_t param_idx = equal_offs.at(i).param_idx_;
    if (OB_UNLIKELY(equal_offs.at(i).pos_off_ < 0 || equal_offs.at(i).pos_off_ >= column_count)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("out of key range", K(ret));
    } else if (equal_offs.at(i).only_pos_) {
      int64_t range_pos = equal_offs.at(i).pos_off_;
      *(start + range_pos) = equal_offs.at(i).pos_value_;
      *(end + range_pos) = equal_offs.at(i).pos_value_;
    } else if (OB_UNLIKELY(param_idx < 0 || param_idx >= param_store.count())) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("out of param store", K(ret));
    } else if (param_store.at(param_idx).is_null()) {
      include_null = true;
    } else {
      int64_t range_pos = equal_offs.at(i).pos_off_;
      *(start + range_pos) = param_store.at(param_idx);
      *(end + range_pos) = param_store.at(param_idx);
    }
  }
  // check start and end ObObj
  if (OB_SUCC(ret)) {
    if (include_null) {
      for (int64_t i = 0; i < column_count; ++i) {
        (start + i)->set_max_value();
        (end + i)->set_min_value();
      }
    } else {
      for (int64_t i = 0; i < column_count; ++i) {
        if ((start + i)->is_min_value()) {
          for (int64_t j = i + 1; j < column_count; ++j) {
            (start + j)->set_min_value();
            (end + j)->set_max_value();
          }
          break;
        } else {
          include_border++;
        }
      }
    }
  }
  // fill other data in ObNewRange
  if (OB_SUCC(ret)) {
    if (include_null || column_count != include_border) {
      key_range->border_flag_.unset_inclusive_start();
      key_range->border_flag_.unset_inclusive_end();
    } else {
      key_range->border_flag_.set_inclusive_start();
      key_range->border_flag_.set_inclusive_end();
    }
    key_range->start_key_.assign(start, column_count);
    key_range->end_key_.assign(end, column_count);
    key_ranges.push_back(key_range);
  }
  return ret;
}

int ObSQLUtils::get_partition_range(ObObj *start_row_key,
																		ObObj *end_row_key,
																		ObObj *function_obj,
																		const share::schema::ObPartitionFuncType part_type,
																		const ObSqlExpression &part_expr,
																		int64_t range_key_count,
																		uint64_t table_id,
																		ObExprCtx &expr_ctx,
																		common::ObNewRange &part_range)
{
	int ret = OB_SUCCESS;
  if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS ||
      part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_LIST_COLUMNS ||
      part_expr.is_empty()) {
    part_range.table_id_ = table_id;
    part_range.start_key_.assign(start_row_key, range_key_count);
    part_range.end_key_.assign(end_row_key, range_key_count);
    part_range.border_flag_.set_inclusive_start();
    part_range.border_flag_.set_inclusive_end();
    if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH) {
      // hash partition and part_expr.is_empty(), should not reach here
      LOG_TRACE("get empty part_expr");
      ObNewRow start_row;
      start_row.cells_ = start_row_key;
      start_row.count_ = range_key_count;
      ObNewRow end_row;
      end_row.cells_ = end_row_key;
      end_row.count_ = range_key_count;
      if (OB_FAIL(ObSQLUtils::revise_hash_part_object(start_row_key[0], start_row, true, part_type))) {
        LOG_WARN("failed to revise hash part object", K(ret));
      } else if (OB_FAIL(ObSQLUtils::revise_hash_part_object(end_row_key[0], end_row, true, part_type))) {
        LOG_WARN("failed to revise hash part object", K(ret));
      } else { /*do nothing*/ }
    }
  } else {
    LOG_TRACE("get non empty part_expr");
    ObNewRow input_row;
    ObNewRow dummy_row;
    input_row.cells_ = start_row_key;
    input_row.count_ = range_key_count;
    if (OB_FAIL(part_expr.calc(expr_ctx, input_row, *function_obj))) {
      LOG_WARN("failed to calc expr", K(ret));
    } else if (FALSE_IT(dummy_row.cells_ = function_obj)) {
    } else if (FALSE_IT(dummy_row.count_ = 1)) {
    } else if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH &&
               OB_FAIL(ObSQLUtils::revise_hash_part_object(*function_obj, dummy_row, false, part_type))) {
      LOG_WARN("failed to revise hash partition object", K(ret));
    } else {
      part_range.table_id_ = table_id;
      part_range.start_key_.assign(function_obj, 1);
      part_range.end_key_.assign(function_obj, 1);
      part_range.border_flag_.set_inclusive_start();
      part_range.border_flag_.set_inclusive_end();
    }
  }
	return ret;
}

bool ObSQLUtils::part_expr_has_virtual_column(const ObExpr *part_expr)
{
  bool has_virtual_column = false;
  if (part_expr == NULL) {
    // do nothing.
  } else if (part_expr->type_ == T_REF_COLUMN || part_expr->type_ == T_FUN_SYS_PART_HASH) {
    // do nothing. column or oracle mode hash.
  } else if (part_expr->type_ == T_OP_ROW) {
    for (int64_t i = 0; i < part_expr->arg_cnt_ && !has_virtual_column; i++) {
      if (part_expr->args_[i]->type_ != T_REF_COLUMN) {
        has_virtual_column = true;
      }
    }
  } else {
    has_virtual_column = true;
  }

  return has_virtual_column;
}

int ObSQLUtils::get_partition_range(ObObj *start_row_key,
                                    ObObj *end_row_key,
                                    ObObj *function_obj,
                                    const share::schema::ObPartitionFuncType part_type,
                                    const ObExpr *part_expr,
                                    int64_t range_key_count,
                                    uint64_t table_id,
                                    ObEvalCtx &eval_ctx,
                                    common::ObNewRange &part_range,
                                    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  bool has_virtual_column = part_expr_has_virtual_column(part_expr);
  // If there is a virtual generated column in part_expr, this logic cannot be used
  if (!has_virtual_column) {
    // opt logic. no need evalute.
    if ((part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS ||
        part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_LIST_COLUMNS)) {
      if (OB_FAIL(get_range_for_vector(
                                    start_row_key,
                                    end_row_key,
                                    range_key_count,
                                    table_id,
                                    part_range))) {
        LOG_WARN("get partition range in opt failed", K(ret));
      }
      LOG_DEBUG("opt logic", K(part_range), KPC(part_expr));
    } else {
      // part expr only have one column.
      if (OB_FAIL(get_range_for_scalar(
                                    start_row_key,
                                    end_row_key,
                                    function_obj,
                                    part_type,
                                    part_expr,
                                    range_key_count,
                                    table_id,
                                    eval_ctx,
                                    part_range,
                                    allocator))) {
        LOG_WARN("get partition range in part expr for scalar failed", K(ret));
      }
    }
  // conclude virtual generated column & part expr not NULL
  } else {
    if (OB_FAIL(get_range_for_scalar(
                                    start_row_key,
                                    end_row_key,
                                    function_obj,
                                    part_type,
                                    part_expr,
                                    range_key_count,
                                    table_id,
                                    eval_ctx,
                                    part_range,
                                    allocator))) {
      LOG_WARN("get partition range in part expr for scalar failed", K(ret));
    }
  }

  return ret;
}

int ObSQLUtils::get_range_for_scalar(ObObj *start_row_key,
                                    ObObj *end_row_key,
                                    ObObj *function_obj,
                                    const share::schema::ObPartitionFuncType part_type,
                                    const ObExpr *part_expr,
                                    int64_t range_key_count,
                                    uint64_t table_id,
                                    ObEvalCtx &eval_ctx,
                                    common::ObNewRange &part_range,
                                    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObj *tmp_start_row_key = NULL;
  ObObj *tmp_end_row_key = NULL;
  int64_t count = 0;
  // only range column & list column
  // hash column mode type is T_FUN_SYS_PART_HASH
  if (part_expr->type_ == T_OP_ROW) {
    count = range_key_count;
    if (OB_ISNULL(tmp_start_row_key = static_cast<ObObj*>(allocator.alloc(
      sizeof(ObObj) * range_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else if (OB_ISNULL(tmp_end_row_key = static_cast<ObObj*>(allocator.alloc(
      sizeof(ObObj) * range_key_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else {
      for (int64_t i = 0; i < part_expr->arg_cnt_ && OB_SUCC(ret); i++) {
        if (part_expr->args_[i]->type_ == T_REF_COLUMN) {
          tmp_start_row_key[i] = start_row_key[i];
          tmp_end_row_key[i] = end_row_key[i];
        } else {
          ObExpr *part_expr_arg = NULL;
          part_expr_arg = part_expr->args_[i];
        // virtual generated column scene
          if (OB_FAIL(get_partition_range_common(
                                    function_obj,
                                    part_type,
                                    part_expr_arg,
                                    eval_ctx))) {
            LOG_WARN("get partition range common failed", K(ret));
          } else {
            tmp_start_row_key[i] = *function_obj;
            tmp_end_row_key[i] = *function_obj;
          }
        }
      }
    }
  // one column or hash columns oracle mode.
  } else {
    count = 1;
    if (OB_ISNULL(tmp_start_row_key = static_cast<ObObj*>(allocator.alloc(
      sizeof(ObObj) * 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else if (OB_ISNULL(tmp_end_row_key = static_cast<ObObj*>(allocator.alloc(
      sizeof(ObObj) * 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else if (OB_FAIL(get_partition_range_common(
                                    function_obj,
                                    part_type,
                                    part_expr,
                                    eval_ctx))) {
      LOG_WARN("get partition range common failed", K(ret));
    } else {
      tmp_start_row_key[0] = *function_obj;
      tmp_end_row_key[0] = *function_obj;
    }
  }
  if (OB_SUCC(ret)) {
    part_range.table_id_ = table_id;
    part_range.start_key_.assign(tmp_start_row_key, count);
    part_range.end_key_.assign(tmp_end_row_key, count);
    part_range.border_flag_.set_inclusive_start();
    part_range.border_flag_.set_inclusive_end();
  }
  LOG_DEBUG("get partition range", K(ret), K(part_range), KPC(part_expr), K(count));
  return ret;
}

int ObSQLUtils::get_range_for_vector(ObObj *start_row_key,
                                    ObObj *end_row_key,
                                    int64_t range_key_count,
                                    uint64_t table_id,
                                    common::ObNewRange &part_range)
{
  int ret = OB_SUCCESS;
  part_range.table_id_ = table_id;
  part_range.start_key_.assign(start_row_key, range_key_count);
  part_range.end_key_.assign(end_row_key, range_key_count);
  part_range.border_flag_.set_inclusive_start();
  part_range.border_flag_.set_inclusive_end();
  return ret;
}



int ObSQLUtils::get_partition_range_common(
                                    ObObj *function_obj,
                                    const share::schema::ObPartitionFuncType part_type,
                                    const ObExpr *part_expr,
                                    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObNewRow dummy_row;
  ObDatum *datum = NULL;
  if (OB_FAIL(part_expr->eval(eval_ctx, datum))) {
    LOG_WARN("failed to calc expr", K(ret));
  } else if (OB_FAIL(datum->to_obj(*function_obj,
                                  part_expr->obj_meta_,
                                  part_expr->obj_datum_map_))) {
    LOG_WARN("convert datum to obj failed", K(ret));
  } else if (FALSE_IT(dummy_row.cells_ = function_obj)) {
  } else if (FALSE_IT(dummy_row.count_ = 1)) {
  } else if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH &&
      OB_FAIL(ObSQLUtils::revise_hash_part_object(*function_obj, dummy_row, false, part_type))) {
    LOG_WARN("failed to revise hash partition object", K(ret));
  }
  LOG_DEBUG("get_partition_range_common", K(ret), KPC(part_expr));
  return ret;
}

int ObSQLUtils::revise_hash_part_object(common::ObObj &obj,
                                        const ObNewRow &row,
                                        const bool calc_oracle_hash,
                                        const share::schema::ObPartitionFuncType part_type)
{
  int ret = OB_SUCCESS;
  ObObj result;
  if (lib::is_oracle_mode()) {
    if (calc_oracle_hash) {
      ret = ObExprFuncPartHash::calc_value_for_oracle(row.cells_, row.count_, result);
    } else {
      result = obj;
    }
  } else {
    ret = ObExprFuncPartHash::calc_value_for_mysql(obj, result, obj.get_type());
  }
  if (OB_SUCC(ret)) {
    obj.set_int(result.get_int());
  }
  return ret;
}

/**
 * choose best replica for storage row estimation with following priority:
 *  1. local replica
 *  2. random replica in local idc
 *  3. random replica in local region
 *  4. other
 */
int ObSQLUtils::choose_best_replica_for_estimation(
                          const ObCandiTabletLoc &phy_part_loc_info,
                          const ObAddr &local_addr,
                          const common::ObIArray<ObAddr> &addrs_list,
                          const bool no_use_remote,
                          EstimatedPartition &best_partition)
{
  int ret = OB_SUCCESS;
  best_partition.reset();
  const ObIArray<ObRoutePolicy::CandidateReplica> &replica_loc_array =
              phy_part_loc_info.get_partition_location().get_replica_locations();
  bool found = false;
  // 2. check whether best partition can find in local
  for (int64_t i = -1; !found && i < addrs_list.count(); ++i) {
    const ObAddr &addr = (i == -1? local_addr : addrs_list.at(i));
    for (int64_t j = 0; !found && j < replica_loc_array.count(); ++j) {
      if (addr == replica_loc_array.at(j).get_server() &&
          0 != replica_loc_array.at(j).get_property().get_memstore_percent()) {
        found = true;
        best_partition.set(addr,
                            phy_part_loc_info.get_partition_location().get_tablet_id(),
                            phy_part_loc_info.get_partition_location().get_ls_id());
      }
    }
  }
  if (!found && !no_use_remote) {
    // best partition not find in local
    ObAddr remote_addr;
    if (OB_FAIL(choose_best_partition_replica_addr(local_addr,
                                                   phy_part_loc_info,
                                                   false,
                                                   remote_addr))) {
      LOG_WARN("failed to get best partition replica addr", K(ret));
      // choose partition replica failed doesn't affect execution, we will decide whether use
      // storage estimation interface by (!use_local && remote_addr.is_valid()).
      ret = OB_SUCCESS;
    }
    best_partition.set(remote_addr,
                       phy_part_loc_info.get_partition_location().get_tablet_id(),
                       phy_part_loc_info.get_partition_location().get_ls_id());
  }
  return ret;
}

/*
 * 选择副本优先级： 本机-->本idc(随机)-->本region(随机) --> 其他region(随机)
 * */
int ObSQLUtils::choose_best_partition_replica_addr(const ObAddr &local_addr,
                                                   const ObCandiTabletLoc &phy_part_loc_info,
                                                   const bool is_est_block_count,
                                                   ObAddr &selected_addr)
{
  int ret = OB_SUCCESS;
  selected_addr.reset();

  ObServerLocality local_locality;
  ObSEArray<ObServerLocality, 8> all_server_arr;
  bool has_read_only_zone = false; // UNUSED;
  if (OB_ISNULL(GCTX.locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("locality manager should not be null", K(ret), KP(GCTX.locality_manager_));
  } else if (OB_FAIL(GCTX.locality_manager_->get_server_locality_array(all_server_arr,
                                                                       has_read_only_zone))) {
    LOG_WARN("fail to get server locality", K(ret));
  } else if (OB_FAIL(ObRoutePolicy::get_server_locality(local_addr,
                                                        all_server_arr,
                                                        local_locality))) {
    LOG_WARN("fail to get local locality", K(ret));
  } else {
    bool  need_continue = true;
    const ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas =
        phy_part_loc_info.get_partition_location().get_replica_locations();
    ObSEArray<ObAddr, 5> same_idc_addr;
    ObSEArray<ObAddr, 5> same_region_addr;
    ObSEArray<ObAddr, 5> other_region_addr;
    for (int64_t i = 0 ;
         need_continue && OB_SUCC(ret) && i < candi_replicas.count();
         ++i) {
      // 有本地选本地
      // 否则从远程中选
      // 优先级: same_idc, same_region
      ObServerLocality candi_locality;
      const ObAddr &candi_addr = candi_replicas.at(i).get_server();
      if (!is_est_block_count &&
          candi_replicas.at(i).get_property().get_memstore_percent() == 0) {
        // skip replica whose memstore percent is zero
      } else if (OB_FAIL(ObRoutePolicy::get_server_locality(candi_addr,
                                                            all_server_arr,
                                                            candi_locality))) {
        LOG_WARN("fail to get server locality", K(all_server_arr), K(candi_addr), K(ret));
      } else if (OB_UNLIKELY(!candi_locality.is_init())) {
        //maybe the locality cache hasn't been flushed yet, we just trust it.
        if (local_addr == candi_addr) {
          selected_addr = candi_addr;
          need_continue = false;
        } else if (OB_FAIL(other_region_addr.push_back(candi_addr))) {
          LOG_WARN("failed to push back other region candidate address", K(ret));
        } else {/*do nothing*/}
      } else if (!candi_locality.is_active()
                 || ObServerStatus::OB_SERVER_ACTIVE != candi_locality.get_server_status()
                 || 0 == candi_locality.get_start_service_time()
                 || 0 != candi_locality.get_server_stop_time()) {
        // server may not serving
      } else if (local_addr == candi_addr) {
        selected_addr = candi_addr;
        need_continue = false;
      } else if (local_locality.is_init()
                 && ObRoutePolicy::is_same_idc(local_locality, candi_locality)) {
        if (OB_FAIL(same_idc_addr.push_back(candi_addr))) {
          LOG_WARN("failed to push back same idc candidate address", K(ret));
        }
      } else if (local_locality.is_init()
                 && ObRoutePolicy::is_same_region(local_locality, candi_locality)) {
        if (OB_FAIL(same_region_addr.push_back(candi_addr))) {
          LOG_WARN("failed to push back same region candidate address", K(ret));
        }
      } else if (OB_FAIL(other_region_addr.push_back(candi_addr))) {
        LOG_WARN("failed to push back other region candidate address", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (selected_addr.is_valid()) {
        // do noting, find local addr
        LOG_TRACE("has local replica", K(selected_addr));
      } else if (!same_idc_addr.empty()) {
        int64_t selected_idx = rand() % same_idc_addr.count();
        selected_addr = same_idc_addr.at(selected_idx);
        LOG_TRACE("has same idc replica", K(selected_addr));
      } else if (!same_region_addr.empty()) {
        int64_t selected_idx = rand() % same_region_addr.count();
        selected_addr = same_region_addr.at(selected_idx);
        LOG_TRACE("has same region replica", K(selected_addr));
      } else if (!other_region_addr.empty()) {
        int64_t selected_idx = rand() % other_region_addr.count();
        selected_addr = other_region_addr.at(selected_idx);
        LOG_TRACE("has other region replica", K(selected_addr));
      } else {
        ret = OB_WORKING_PARTITION_NOT_EXIST;
        LOG_WARN("No useful replica found", K(ret));
      }
    }
  }

  return ret;
}

int ObSQLUtils::wrap_column_convert_ctx(const ObExprCtx &expr_ctx, ObCastCtx &column_conv_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. null session", K(expr_ctx.my_session_), K(ret));
  } else {
    //is_strict == true is_ignore_stmt == true  then   cast_mode = CM_WARN_ON_FAIL
    //is_strict == true  is_ignore_stmt == false  then   cast_mode = CM_NONE
    //is_strict == false   is_ignore_stmt == true  then   cast_mode = CM_NO_RANGE_CHECK & CM_WARN_ON_FAIL
    //is_strict == false   is_ignore_stmt == false  then  cast_mode = CM_NO_RANGE_CHECK & CM_WARN_ON_FAIL
    ObCastMode cast_mode = is_strict_mode(
        expr_ctx.my_session_->get_sql_mode()) ? CM_NONE : CM_WARN_ON_FAIL;
    cast_mode = cast_mode | CM_COLUMN_CONVERT;
    if (expr_ctx.phy_plan_ctx_ != NULL && expr_ctx.phy_plan_ctx_->is_ignore_stmt()) {
      cast_mode = CM_WARN_ON_FAIL;
    }
    //do not worry too much about the efficiency here
    //gcc will construct and set column_conv_ctx_ directly
    //only one constructor call is necessary. no temporary object is created
    EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
    column_conv_ctx = cast_ctx;
  }
  return ret;
}

int ObSQLUtils::merge_solidified_var_into_collation(const share::schema::ObLocalSessionVar &session_vars_snapshot,
                                                     ObCollationType &cs_type) {
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    ObSessionSysVar *local_var = NULL;
    if (OB_FAIL(session_vars_snapshot.get_local_var(SYS_VAR_COLLATION_CONNECTION, local_var))) {
      LOG_WARN("get local session var failed", K(ret));
    } else if (NULL != local_var) {
      cs_type = static_cast<ObCollationType>(local_var->val_.get_int());
    }
  }
  return ret;
}

int ObSQLUtils::merge_solidified_vars_into_type_ctx(ObExprTypeCtx &type_ctx,
                                                    const ObLocalSessionVar &session_vars_snapshot)
{
  int ret = OB_SUCCESS;

  //coll_type_ for mysql mode
  ObCollationType cs_type = type_ctx.get_coll_type();
  if (OB_FAIL(merge_solidified_var_into_collation(session_vars_snapshot, cs_type))) {
    LOG_WARN("get collation failed", K(ret));
  } else {
    type_ctx.set_coll_type(cs_type);
  }
  //merge dtc param
  if (OB_SUCC(ret)) {
    bool dummy = false;
    const ObTimeZoneInfo *local_timezone = NULL;
    ObSessionSysVar *local_var = NULL;
    const ObTZInfoMap * tz_info_map = type_ctx.get_tz_info_map();
    if (OB_ISNULL(tz_info_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(tz_info_map));
    } else if (OB_FAIL(session_vars_snapshot.get_local_var(SYS_VAR_TIME_ZONE, local_var))) {
      LOG_WARN("get local var failed", K(ret));
    } else if (NULL != local_var) {
      if (OB_FAIL(type_ctx.get_local_tz_wrap().init_time_zone(local_var->val_.get_string(),
                                                              OB_INVALID_VERSION,
                                                              *(const_cast<ObTZInfoMap *>(tz_info_map))))) {
        LOG_WARN("get init_time_zone failed", K(ret), K(local_var->val_.get_string()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(merge_solidified_var_into_dtc_params(&session_vars_snapshot,
                                                        type_ctx.get_local_tz_wrap().get_time_zone_info(),
                                                        type_ctx.get_dtc_params()))) {
        LOG_WARN("get dtc params failed", K(ret));
      }
    }
  }
  //merge sql mode
  if (OB_SUCC(ret)) {
    ObSQLMode sql_mode = type_ctx.get_sql_mode();
    if (OB_FAIL(merge_solidified_var_into_sql_mode(&session_vars_snapshot, sql_mode))) {
      LOG_WARN("get sql mode failed", K(ret));
    } else {
      type_ctx.set_sql_mode(sql_mode);
    }
  }

  //merge max allowed packet
  if (OB_SUCC(ret)) {
    int64_t max_allowed_packet = type_ctx.get_max_allowed_packet();
    if (OB_FAIL(merge_solidified_var_into_max_allowed_packet(&session_vars_snapshot, max_allowed_packet))) {
      LOG_WARN("get sql mode failed", K(ret));
    } else {
      type_ctx.set_max_allowed_packet(max_allowed_packet);
    }
  }
  return ret;
}

int ObSQLUtils::merge_solidified_var_into_dtc_params(const share::schema::ObLocalSessionVar *local_vars,
                                                const ObTimeZoneInfo *local_timezone,
                                                ObDataTypeCastParams &dtc_param)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  //dtc_param = ObBasicSessionInfo::create_dtc_params(session);
  //time zone
  if (NULL != local_timezone) {
    dtc_param.tz_info_ = local_timezone;
  }
  //nls format
  if (NULL != local_vars) {
    if (OB_FAIL(local_vars->get_local_var(SYS_VAR_NLS_DATE_FORMAT, local_var))) {
      LOG_WARN("get local session var failed", K(ret));
    } else if (NULL != local_var) {
      dtc_param.session_nls_formats_[0] = local_var->val_.get_string();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(local_vars->get_local_var(SYS_VAR_NLS_TIMESTAMP_FORMAT, local_var))) {
        LOG_WARN("get local session var failed", K(ret));
      } else if (NULL != local_var) {
        dtc_param.session_nls_formats_[1] = local_var->val_.get_string();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(local_vars->get_local_var(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT, local_var))) {
        LOG_WARN("get local session var failed", K(ret));
      } else if (NULL != local_var) {
        dtc_param.session_nls_formats_[2] = local_var->val_.get_string();
      }
    }
  }
  return ret;
}

int ObSQLUtils::merge_solidified_var_into_sql_mode(const share::schema::ObLocalSessionVar *local_vars,
                                                    ObSQLMode &sql_mode) {
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  if (NULL == local_vars) {
    //do nothing
  } else if (OB_FAIL(local_vars->get_local_var(SYS_VAR_SQL_MODE, local_var))) {
    LOG_WARN("get local session var failed", K(ret));
  } else if (NULL != local_var) {
    if (ObUInt64Type == local_var->val_.get_type()) {
      sql_mode = local_var->val_.get_uint64();
    } else if (ObIntType == local_var->val_.get_type()) {
      sql_mode = static_cast<uint64_t>(local_var->val_.get_int());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sql mode val type", K(ret), K(local_var->val_));
    }
  }
  return ret;
}

int ObSQLUtils::merge_solidified_var_into_max_allowed_packet(const share::schema::ObLocalSessionVar *local_vars,
                                                             int64_t &max_allowed_packet)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  if (NULL == local_vars) {
    //do nothing
  } else if (OB_FAIL(local_vars->get_local_var(SYS_VAR_MAX_ALLOWED_PACKET, local_var))) {
    LOG_WARN("get local session var failed", K(ret));
  } else if (NULL != local_var) {
    if (ObIntType == local_var->val_.get_type()) {
      max_allowed_packet = local_var->val_.get_int();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid max allowed packet val type", K(ret), K(local_var->val_));
    }
  }
  return ret;
}

int ObSQLUtils::merge_solidified_var_into_compat_version(const share::schema::ObLocalSessionVar *local_vars,
                                                         uint64_t &compat_version)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  if (NULL == local_vars) {
    //do nothing
  } else if (OB_FAIL(local_vars->get_local_var(SYS_VAR_OB_COMPATIBILITY_VERSION, local_var))) {
    LOG_WARN("get local session var failed", K(ret));
  } else if (NULL != local_var) {
    if (ObUInt64Type == local_var->val_.get_type()) {
      compat_version = local_var->val_.get_uint64();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid compat version val type", K(ret), K(local_var->val_));
    }
  }
  return ret;
}

void ObSQLUtils::init_type_ctx(const ObSQLSessionInfo *session, ObExprTypeCtx &type_ctx)
{
  if (NULL != session) {
    ObCollationType coll_type = CS_TYPE_INVALID;
    int64_t div_precision_increment = OB_INVALID_COUNT;
    int64_t ob_max_allowed_packet;
    // 对于type_ctx的collation_type，我理解这里需要初始化一个默认值，
    // 对于MySQL模式，我们代码中使用的是collation_connection，这也是常量的默认collation，
    // 但是在Oracle模式下，常量都转换成了nls_collation，所以在Oracle模式下设置成nls_collation更合理
    if (lib::is_mysql_mode()) {
      if (OB_SUCCESS == (session->get_collation_connection(coll_type))) {
        type_ctx.set_coll_type(coll_type);
      }
    } else if (lib::is_oracle_mode()) {
      type_ctx.set_coll_type(session->get_nls_collation());
    }

    if (OB_SUCCESS == (session->get_div_precision_increment(div_precision_increment))) {
      type_ctx.set_div_precision_increment(div_precision_increment);
    }
    if (OB_SUCCESS == (session->get_max_allowed_packet(ob_max_allowed_packet))) {
      type_ctx.set_max_allowed_packet(ob_max_allowed_packet);
    }
    type_ctx.set_sql_mode(session->get_sql_mode());
    ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    type_ctx.set_dtc_params(dtc_params);
    ObTZMapWrap tz_map_wrap;
    if (OB_SUCCESS == (OTTZ_MGR.get_tenant_tz(session->get_effective_tenant_id(), tz_map_wrap))) {
      type_ctx.set_tz_info_map(tz_map_wrap.get_tz_map());;
    }
    CHECK_COMPATIBILITY_MODE(session);
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Molly couldn't get compatibility mode from session, use default", K(lbt()));
  }
  type_ctx.set_session(session);
}

bool ObSQLUtils::is_oracle_sys_view(const ObString &table_name)
{
  // 当前支持ALL_,USER_,DBA_,GV$,V$为前缀的SYS中的视图
  // 如果有不是以这些开头的，可以再定义其他pattern，如固定字符串数组whitelist之类的
  return table_name.prefix_match("ALL_")
        || table_name.prefix_match("USER_")
        || table_name.prefix_match("DBA_")
        || table_name.prefix_match("GV$")
        || table_name.prefix_match("V$")
        || 0 == table_name.compare("AUDIT_ACTIONS")
        || 0 == table_name.compare("STMT_AUDIT_OPTION_MAP")
        || 0 == table_name.compare("NLS_SESSION_PARAMETERS")
        || 0 == table_name.compare("NLS_INSTANCE_PARAMETERS")
        || 0 == table_name.compare("NLS_DATABASE_PARAMETERS")
        || 0 == table_name.compare("DICTIONARY")
        || 0 == table_name.compare("DICT")
        || 0 == table_name.compare("ROLE_TAB_PRIVS")
        || 0 == table_name.compare("ROLE_SYS_PRIVS")
        || 0 == table_name.compare("ROLE_ROLE_PRIVS")
        || 0 == table_name.compare("STMT_AUDIT_OPTION_MAP");
}

int ObSQLUtils::make_whole_range(ObIAllocator &allocator,
                                 const uint64_t ref_table_id,
                                 const int64_t rowkey_count,
                                 ObNewRange *&whole_range)
{
  int ret = OB_SUCCESS;
  whole_range = NULL;
  ObNewRange *temp_range = NULL;
  if (OB_ISNULL(temp_range = static_cast<ObNewRange*>(allocator.alloc(sizeof(ObNewRange))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for query range failed");
  } else if (OB_ISNULL(whole_range = new(temp_range) ObNewRange())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to new whole range");
  } else if (OB_FAIL(make_whole_range(allocator, ref_table_id,
                                      rowkey_count, *whole_range))) {
    LOG_WARN("Failed to make whole range inner", K(ret));
  }
  return ret;
}

int ObSQLUtils::make_whole_range(ObIAllocator &allocator,
                                 const uint64_t ref_table_id,
                                 const int64_t rowkey_count,
                                 ObNewRange &whole_range)
{
  int ret = OB_SUCCESS;
  ObObj *start_row_key = NULL;
  ObObj *end_row_key = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == ref_table_id) ||
      OB_UNLIKELY(rowkey_count < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ref_table_id), K(rowkey_count), K(ret));
  } else if (OB_ISNULL(start_row_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * rowkey_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end_row_key = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * rowkey_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for end_obj failed", K(ret));
  } else {
    for (int i = 0; i < rowkey_count; ++i) {
      start_row_key[i] = ObRowkey::MIN_OBJECT;
      end_row_key[i] = ObRowkey::MAX_OBJECT;
    }
    whole_range.table_id_ = ref_table_id;
    whole_range.start_key_.assign(start_row_key, rowkey_count);
    whole_range.end_key_.assign(end_row_key, rowkey_count);
    whole_range.border_flag_.unset_inclusive_start();
    whole_range.border_flag_.unset_inclusive_end();
  }
  return ret;
}

//获取pl array中每个obj对应的数据类型信息
int ObSQLUtils::get_ext_obj_data_type(const ObObjParam &obj, ObDataType &data_type)
{
  int ret = OB_SUCCESS;
  if (obj.is_ext_sql_array()) {
    const ObSqlArrayObj *array_params = reinterpret_cast<const ObSqlArrayObj*>(obj.get_ext());
    if (OB_ISNULL(array_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array params is null", K(ret));
    } else {
      data_type = array_params->element_;
    }
  } else if (obj.is_pl_extend()) {
#ifndef OB_BUILD_ORACLE_PL
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret));
#else
    if (obj.get_meta().get_extend_type() == pl::PL_NESTED_TABLE_TYPE
      || obj.get_meta().get_extend_type() == pl::PL_ASSOCIATIVE_ARRAY_TYPE
      || obj.get_meta().get_extend_type() == pl::PL_VARRAY_TYPE) {
      const pl::ObPLNestedTable *nested_table =
          reinterpret_cast<const pl::ObPLNestedTable*>(obj.get_ext());
      if (OB_ISNULL(nested_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nested table is null", K(ret));
      } else {
        data_type = nested_table->get_element_type();
      }
    }
#endif
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj is not extend type", K(ret), K(obj));
  }

  return ret;
}

int ObSQLUtils::ConvertFiledNameAttribute(ObIAllocator& allocator,
    const ObString &src,
    ObString &dst,
    const uint16_t collation_type)
{
  int ret = OB_SUCCESS;
  const ObCollationType cs_type = static_cast<ObCollationType>(collation_type);
  if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(cs_type)
      || CS_TYPE_BINARY == cs_type
      || CS_TYPE_INVALID == cs_type) {
    OZ(ob_write_string(allocator, src, dst));
  } else {
    char *buf = nullptr;
    int32_t buf_len = src.length() * ObCharset::CharConvertFactorNum;
    uint32_t result_len = 0;
    const ObCollationType from_collation =
              ObCharset::get_default_collation(ObCharset::get_default_charset());
    const ObCollationType to_collation = static_cast<ObCollationType>(collation_type);

    if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src.length())) {
      dst.assign(NULL, 0);
    } else if (nullptr == (buf = static_cast<char*>(allocator.alloc(buf_len)))) {
      dst.assign(NULL, 0);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory failed", K(ret), "len", buf_len);
    } else {
      ret = ObCharset::charset_convert(from_collation,
          src.ptr(), src.length(),
          to_collation,
          buf, buf_len, result_len);

      if (OB_SUCCESS != ret) {
        int32_t str_offset = 0;
        int64_t buf_offset = 0;
        ObString question_mark = ObCharsetUtils::get_const_str(to_collation, '?');
        while (str_offset < src.length() && buf_offset + question_mark.length() <= buf_len) {
          int64_t offset = ObCharset::charpos(from_collation, src.ptr() + str_offset,
              src.length() - str_offset, 1);
          ret = ObCharset::charset_convert(from_collation, src.ptr() + str_offset, offset,
              to_collation, buf + buf_offset, buf_len - buf_offset, result_len);
          str_offset += offset;
          if (OB_SUCCESS == ret) {
            buf_offset += result_len;
          } else {
            MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
            buf_offset += question_mark.length();
          }
        }
        if (str_offset < src.length()) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("size overflow", K(ret), K(src), KPHEX(src.ptr(), src.length()));
        } else {
          result_len = buf_offset;
          ret = OB_SUCCESS;
          LOG_WARN("charset convert failed", K(ret), K(from_collation), K(to_collation));
        }
      }

      if (OB_SUCC(ret)) {
        dst.assign(buf, static_cast<int32_t>(result_len));
      }
    }
  }

  return ret;
}

int ObSQLUtils::clear_evaluated_flag(const ObExprPtrIArray &calc_exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < calc_exprs.count(); i++) {
    if (OB_ISNULL(calc_exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(calc_exprs), K(i));
    } else {
      calc_exprs.at(i)->get_eval_info(eval_ctx).clear_evaluated_flag();
    }
  }
  return ret;
}

int ObSQLUtils::copy_and_convert_string_charset(ObIAllocator& allocator,
                                                const ObString &src,
                                                ObString &dst,
                                                ObCollationType src_coll,
                                                ObCollationType dst_coll)
{
  return ObCharset::charset_convert(allocator, src, src_coll, dst_coll, dst,
                                    ObCharset::COPY_STRING_ON_SAME_CHARSET);
}

int ObSQLUtils::update_session_last_schema_version(ObMultiVersionSchemaService &schema_service,
                                                   ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  int64_t received_schema_version = OB_INVALID_VERSION;
  uint64_t tenant_id = session_info.get_effective_tenant_id();
  if (OB_FAIL(schema_service.get_tenant_received_broadcast_version(tenant_id,
                                                                   received_schema_version))) {
    LOG_WARN("fail to get tenant received broadcast version", K(ret), K(tenant_id));
  } else if (OB_FAIL(session_info.update_sys_variable(SYS_VAR_OB_LAST_SCHEMA_VERSION,
                                                      received_schema_version))) {
    LOG_WARN("fail to set session variable for last_schema_version", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObImplicitCursorInfo,
                    stmt_id_,
                    affected_rows_,
                    found_rows_,
                    matched_rows_,
                    duplicated_rows_,
                    deleted_rows_);

int ObImplicitCursorInfo::merge_cursor(const ObImplicitCursorInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == stmt_id_) {
    //not init, init it with first cursor info
    *this = other;
  } else if (stmt_id_ != other.stmt_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_id is different", K(stmt_id_), K(other.stmt_id_));
  } else {
    affected_rows_ += other.affected_rows_;
    found_rows_ += other.found_rows_;
    matched_rows_ += other.matched_rows_;
    duplicated_rows_ += other.duplicated_rows_;
    deleted_rows_ += other.deleted_rows_;
  }
  return ret;
}

bool ObSQLUtils::is_same_type_for_compare(const ObObjMeta &meta1, const ObObjMeta &meta2)
{
  bool is_same = false;
  if (meta1.get_type() == meta2.get_type()) {
    is_same = true;
    if (meta1.is_string_type()) {
      is_same = meta1.get_collation_type() == meta2.get_collation_type();
    }
    if (meta1.is_decimal_int()) {
      is_same = meta1.get_scale() == meta2.get_scale();
    }
  }
  LOG_DEBUG("is same type for compare", K(meta1), K(meta2), K(is_same), K(lbt()));
  return is_same;
}

int ObSQLUtils::generate_new_name_with_escape_character(
    common::ObIAllocator &allocator,
    const ObString &src,
    ObString &dst,
    bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  ObString::obstr_size_t dst_len = src_len;
  const char escape_character = is_oracle_mode ? '"' : '`';
  char *ptr = NULL;

  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == src.find(escape_character)) {
    dst = src;
  } else {
    for (int64_t i = 0; i < src_len; ++i) {
      if (src[i] == escape_character) {
        ++dst_len;
      }
    }
    if (NULL == (ptr = static_cast<char *>(allocator.alloc(dst_len + 1)))) {
      dst = NULL;
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory failed", K(ret), "size", dst_len);
    } else {
      int64_t tmp_dst_pos = 0;
      for (int64_t i = 0; i < src_len; i++) {
        if (src[i] == escape_character) {
          ptr[tmp_dst_pos++] = escape_character;
          ptr[tmp_dst_pos++] = escape_character;
        } else {
          ptr[tmp_dst_pos++] = src[i];
        }
      }
      if (tmp_dst_pos != dst_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dst_len is not equal to tmp_dst_pos", K(ret), K(tmp_dst_pos), K(dst_len));
      } else {
        ptr[dst_len] = '\0';
        dst.assign_ptr(ptr, dst_len);
      }
    }
  }

  return ret;
}

int ObVirtualTableResultConverter::reset_and_init(
  ObIAllocator *key_alloc,
  sql::ObSQLSessionInfo *session,
  const common::ObIArray<ObObjMeta> *output_row_types,
  const common::ObIArray<ObObjMeta> *key_types,
  ObIAllocator *init_alloc,
  const share::schema::ObTableSchema *table_schema,
  const common::ObIArray<uint64_t> *output_column_ids,
  const bool has_tenant_id_col,
  const int64_t tenant_id_col_idx,
  const int64_t max_col_cnt /* INT64_MAX */)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(key_alloc)) {
    // like reset
    key_cast_ctx_ = *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx());
    output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_)) ObCastCtx());
    key_alloc_ = nullptr;
    output_row_types_ = nullptr;
    key_types_ = nullptr;
    table_schema_ = nullptr;
    output_column_ids_ = nullptr;
    base_table_id_ = UINT64_MAX;
    cur_tenant_id_ = UINT64_MAX;
    output_row_alloc_.reuse();
  }
  if (OB_ISNULL(key_alloc) || OB_ISNULL(init_alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), KP(key_alloc), KP(init_alloc));
  } else if (output_column_ids->count() != output_row_types->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of column type and extra tenant id is not match", K(ret),
      K(output_column_ids->count()), K(output_row_types->count()));
  } else {
    key_alloc_ = key_alloc;
    if (OB_ISNULL(init_alloc_)) {
      init_alloc_ = init_alloc;
    }
    output_row_types_ = output_row_types;
    key_types_ = key_types;
    max_col_cnt_ = max_col_cnt;
    has_tenant_id_col_ = has_tenant_id_col;
    tenant_id_col_idx_ = tenant_id_col_idx;
    table_schema_ = table_schema;
    output_column_ids_ = output_column_ids;
    base_table_id_ = table_schema->get_table_id();
    cur_tenant_id_ = table_schema->get_tenant_id();
    // calc collation
    ObCollationType cs_type = ObCharset::get_system_collation();
    for (int64_t i = 0; i < output_row_types->count() && OB_SUCC(ret); ++i) {
      const ObObjMeta &obj_meta = output_row_types->at(i);
      if (ob_is_string_tc(obj_meta.get_type())) {
        cs_type = obj_meta.get_collation_type();
        break;
      }
    }
    ObCollationType key_cs_type = ObCharset::get_system_collation();
    for (int64_t i = 0; i < key_types->count() && OB_SUCC(ret); ++i) {
      const ObObjMeta &obj_meta = key_types->at(i);
      if (ob_is_string_tc(obj_meta.get_type())) {
        key_cs_type = obj_meta.get_collation_type();
        break;
      }
    }
    {
      lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      key_cast_ctx_ = *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx(key_alloc, &dtc_params, CM_NONE, key_cs_type));
    }

    // for output row to convert
    const ObDataTypeCastParams dtc_params2 = ObBasicSessionInfo::create_dtc_params(session);
    output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_)) ObCastCtx(&output_row_alloc_, &dtc_params2, CM_NONE, cs_type));
    if (0 == cols_schema_.count() && OB_FAIL(get_all_columns_schema())) {
      LOG_WARN("fail to get column schema", K(ret));
    }
    LOG_DEBUG("debug init converter", K(cs_type));
  }
  return ret;
}

int ObVirtualTableResultConverter::init_without_allocator(
  const common::ObIArray<const share::schema::ObColumnSchemaV2*> &col_schemas,
  uint64_t cur_tenant_id,
  uint64_t tenant_id_col_id)
{
  int ret = OB_SUCCESS;
  cur_tenant_id_ = cur_tenant_id;
  tenant_id_col_id_ = tenant_id_col_id;
  ObTZMapWrap tz_map_wrap;
  if (0 < cols_schema_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cols schema is not empty", K(ret));
  } else if (OB_FAIL(cols_schema_.assign(col_schemas))) {
    LOG_WARN("failed to assign column schema", K(ret));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(cur_tenant_id_, tz_map_wrap))) {
    LOG_WARN("get tenant timezone map failed", K(ret));
  } else {
    int ret_more = OB_SUCCESS;
    int32_t offset_sec = 0;
    tz_info_wrap_.set_tz_info_map(tz_map_wrap.get_tz_map());
    if (OB_FAIL(ObTimeConverter::str_to_offset("+8:00", offset_sec, ret_more, true))) {
      if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
        LOG_WARN("fail to convert str_to_offset", K(ret));
      } else if (ret_more != OB_SUCCESS) {
        ret = ret_more;
        LOG_WARN("invalid time zone hour or minute", K(ret));
      }
    } else {
      tz_info_wrap_.set_tz_info_offset(offset_sec);
    }
  }
  // set cat params
  ObString session_nls_formats[ObNLSFormatEnum::NLS_MAX];
  session_nls_formats[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
  session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  session_nls_formats[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT;
  ObDataTypeCastParams dtc_params2 = ObDataTypeCastParams(tz_info_wrap_.get_time_zone_info(),
                                                          session_nls_formats,
                                                          col_schemas.at(0)->get_collation_type(),
                                                          col_schemas.at(0)->get_collation_type(),
                                                          CS_TYPE_UTF8MB4_BIN);
  output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_)) ObCastCtx(&output_row_alloc_, &dtc_params2, CM_NONE, ObCharset::get_system_collation()));
  LOG_DEBUG("debug timezone", K(tz_info_wrap_.get_time_zone_info()), K(ret));
  return ret;
}

void ObVirtualTableResultConverter::destroy()
{
  key_cast_ctx_ = *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx());
  output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_)) ObCastCtx());
  output_row_types_ = nullptr;
  key_types_ = nullptr;
  inited_row_ = false;
  if (OB_NOT_NULL(convert_row_.cells_)) {
    if (OB_NOT_NULL(init_alloc_)) {
      init_alloc_->free(convert_row_.cells_);
    }
    convert_row_.cells_ = nullptr;
  }
  convert_row_.count_ = 0;
  base_table_id_ = UINT64_MAX;
  cur_tenant_id_ = UINT64_MAX;
  table_schema_ = nullptr;
  output_column_ids_ = nullptr;
  cols_schema_.reset();
  output_row_alloc_.reset();
  key_alloc_ = nullptr;
  init_alloc_ = nullptr;
}

int ObVirtualTableResultConverter::get_need_convert_key_ranges_pos(ObNewRange &key_range, int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = INT64_MAX;
  const ObRowkey &start_key = key_range.start_key_;
  const ObRowkey &end_key = key_range.end_key_;
  if (start_key.get_obj_cnt() != end_key.get_obj_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start key is not match with end key", K(ret),
      "start key cnt", start_key.get_obj_cnt(),
      "end key cnt", end_key.get_obj_cnt());
  } else if (start_key.get_obj_cnt() > 0) {
    const ObObj *start_key_objs = start_key.get_obj_ptr();
    const ObObj *end_key_objs = end_key.get_obj_ptr();
    for (uint64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < start_key.get_obj_cnt(); ++nth_obj) {
      if ((start_key_objs[nth_obj].is_ext() || start_key_objs[nth_obj].is_null()) &&
          !end_key_objs[nth_obj].is_ext()) {
        pos = nth_obj;
        break;
      } else if ((end_key_objs[nth_obj].is_ext() || end_key_objs[nth_obj].is_null()) &&
          !start_key_objs[nth_obj].is_ext()) {
        pos = nth_obj;
        break;
      }
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::convert_key_ranges(ObIArray<ObNewRange> &key_ranges)
{
  int ret = OB_SUCCESS;
  if (!key_ranges.empty()) {
    common::ObArray<common::ObNewRange> tmp_range;
    OZ(tmp_range.reserve(key_ranges.count()));
    CK(OB_NOT_NULL(key_types_));
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      ObNewRange new_range;
      new_range.table_id_ = key_ranges.at(i).table_id_;
      new_range.border_flag_ = key_ranges.at(i).border_flag_;
      int64_t pos = INT64_MAX;
      if (OB_FAIL(get_need_convert_key_ranges_pos(key_ranges.at(i), pos))) {
        LOG_WARN("failed to get convert key range pos", K(ret));
      } else if (OB_FAIL(convert_key(key_ranges.at(i).start_key_, new_range.start_key_, true,
                                     pos))) {
        LOG_WARN("fail to convert start key", K(ret));
      } else if (OB_FAIL(convert_key(key_ranges.at(i).end_key_, new_range.end_key_, false,
                                     pos))) {
        LOG_WARN("fail to convert end key", K(ret));
      } else if (!has_exist_in_array(tmp_range, new_range) && OB_FAIL(tmp_range.push_back(new_range))) {
        LOG_WARN("fail to push back new range", K(ret));
      }
    }//end for
    if (OB_SUCC(ret)) {
      key_ranges.reset();
      if (OB_FAIL(key_ranges.assign(tmp_range))) {
        LOG_WARN("fail to assign new range", K(ret));
      }
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::get_all_columns_schema()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_) || OB_ISNULL(output_column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null or output columns ids is null", K(ret));
  } else if (0 < output_column_ids_->count()
      && OB_FAIL(cols_schema_.reserve(output_column_ids_->count()))) {
    LOG_WARN("failed to reserve output columns ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
    const uint64_t column_id = output_column_ids_->at(i);
    if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id) {
      continue;
    }
    const ObColumnSchemaV2 *col_schema = table_schema_->get_column_schema(column_id);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("col_schema is NULL", K(ret), K(column_id));
    } else if (OB_FAIL(cols_schema_.push_back(col_schema))) {
      LOG_WARN("failed to push back column schema", K(ret));
    } else if (0 == col_schema->get_column_name_str().case_compare("TENANT_ID")) {
      if (UINT64_MAX != tenant_id_col_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("init twice tenant id col id", K(ret), K(tenant_id_col_id_), K(column_id));
      }
      tenant_id_col_id_ = column_id;
    } else {
      LOG_TRACE("trace column type", K(col_schema->get_data_type()), K(col_schema->get_collation_type()));
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::init_output_row(int64_t cell_cnt)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = nullptr;
  ObObj *cells = NULL;
  int64_t max_cell_cnt = INT64_MAX != max_col_cnt_ ? max_col_cnt_ : cell_cnt;
  if (max_cell_cnt <= 0) {
    max_cell_cnt = 1;
  }
  if (max_col_cnt_ < cell_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max col cnt is less than cell cnt", K(ret), K(max_cell_cnt), K(cell_cnt));
  } else if (OB_ISNULL(tmp_ptr = init_alloc_->alloc(max_cell_cnt * sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cells", K(ret), K(max_cell_cnt), K(cell_cnt));
  } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[max_cell_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new cell array", K(ret), K(max_cell_cnt));
  } else {
    convert_row_.cells_ = cells;
    convert_row_.count_ = cell_cnt;
    inited_row_ = true;
    LOG_DEBUG("debug output row", K(cell_cnt), K(max_col_cnt_));
  }
  return ret;
}

int ObVirtualTableResultConverter::convert_output_row(ObNewRow *&src_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is NULL", K(ret));
  } else if (!inited_row_ && OB_FAIL(init_output_row(src_row->count_))) {
    LOG_WARN("failed to init row");
  } else if (output_column_ids_->count() != src_row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: src row and dst row is not match", K(ret),
      K(src_row->count_), K(output_column_ids_->count()));
  } else {
    output_row_alloc_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
      bool need_cast = true;
      const uint64_t column_id = output_column_ids_->at(i);
      const ObObjMeta &obj_meta = output_row_types_->at(i);
      ObObj src_obj = src_row->get_cell(i);
      if (tenant_id_col_id_ == column_id) {
        src_obj.set_uint64(cur_tenant_id_);
      } else if ((src_row->get_cell(i).is_string_type() && 0 == src_row->get_cell(i).get_data_length()) ||
                 (src_row->get_cell(i).is_timestamp() && src_row->get_cell(i).get_timestamp() <= 0)) {
        need_cast = false;
        convert_row_.cells_[i].set_null();
      }
      LOG_DEBUG("debug type", K(obj_meta.get_type()), K(obj_meta.get_collation_type()),
          K(src_obj), K(convert_row_.cells_[i]));
      if (OB_FAIL(ret)) {
      } else if (need_cast) {
        if (OB_FAIL(ObObjCaster::to_type(obj_meta.get_type(),
                                        obj_meta.get_collation_type(),
                                        output_row_cast_ctx_,
                                        src_obj,
                                        convert_row_.cells_[i]))) {
          LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
        } else {
          LOG_DEBUG("debug type", K(obj_meta.get_type()), K(obj_meta.get_collation_type()),
            K(src_obj), K(convert_row_.cells_[i]), K(output_row_cast_ctx_.dest_collation_));
        }
      }
    }
    src_row = &convert_row_;
  }
  return ret;
}

// only for stat collect
int ObVirtualTableResultConverter::convert_column(ObObj &src_obj, uint64_t column_id, uint64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx >= cols_schema_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column idx is invalid", K(idx), K(cols_schema_.count()));
  } else {
    ObObj dst_obj = src_obj;
    bool need_cast = true;
    const ObColumnSchemaV2 *col_schema = cols_schema_.at(idx);
    if (col_schema->get_column_id() != column_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is not match", K(ret), K(column_id), K(col_schema->get_column_id()));
    } else if (tenant_id_col_id_ == column_id) {
      dst_obj.set_uint64(cur_tenant_id_);
    } else if (src_obj.is_null() ||
               (src_obj.is_string_type() && 0 == src_obj.get_data_length()) ||
               (src_obj.is_timestamp() && src_obj.get_timestamp() <= 0)) {
      need_cast = false;
      src_obj.set_null();
    } else if (src_obj.is_ext()) {
      need_cast = false;
    }
    if (OB_FAIL(ret)) {
    } else if (need_cast && OB_FAIL(ObObjCaster::to_type(col_schema->get_data_type(),
                                            col_schema->get_collation_type(),
                                            output_row_cast_ctx_,
                                            dst_obj,
                                            src_obj))) {
      LOG_WARN("failed to cast obj in oracle mode", K(ret), K(idx), K(column_id));
    } else {
      LOG_DEBUG("debug convert column", K(src_obj), K(column_id), K(idx));
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::convert_output_row(
  ObEvalCtx &eval_ctx,
  const common::ObIArray<ObExpr*> &src_exprs,
  const common::ObIArray<ObExpr*> &dst_exprs)
{
  int ret = OB_SUCCESS;
  if (!inited_row_ && OB_FAIL(init_output_row(dst_exprs.count()))) {
    LOG_WARN("failed to init row");
  } else if (dst_exprs.count() != dst_exprs.count() ||
      output_column_ids_->count() != dst_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: src row and dst row is not match", K(ret), K(dst_exprs.count()));
  } else {
    output_row_alloc_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
      const uint64_t column_id = output_column_ids_->at(i);
      const ObObjMeta &obj_meta = output_row_types_->at(i);
      ObDatum *datum = nullptr;
      ObExpr *expr = src_exprs.at(i);
      ObExpr *dst_expr = dst_exprs.at(i);
      ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx);
      ObObj dst_obj;
      if (tenant_id_col_id_ == column_id) {
        convert_row_.cells_[i].set_uint64(cur_tenant_id_);
      } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else if (OB_FAIL(datum->to_obj(convert_row_.cells_[i], expr->obj_meta_))) {
        LOG_WARN("failed to cast obj", K(ret));
      } else if (convert_row_.cells_[i].is_null() ||
                 (convert_row_.cells_[i].is_string_type() && 0 == convert_row_.cells_[i].get_data_length()) ||
                 (convert_row_.cells_[i].is_timestamp() && convert_row_.cells_[i].get_timestamp() <= 0)) {
        convert_row_.cells_[i].set_null();
      } else if (convert_row_.cells_[i].is_lob_storage()) {
        ObLobLocatorV2 lob;
        if (OB_FAIL(convert_row_.cells_[i].get_lob_locatorv2(lob))) {
          LOG_WARN("failed to get lob locator", K(ret));
        } else if (lob.is_empty_lob()) {
          convert_row_.cells_[i].set_null();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjCaster::to_type(obj_meta.get_type(),
                                              obj_meta.get_collation_type(),
                                              output_row_cast_ctx_,
                                              convert_row_.cells_[i],
                                              dst_obj))) {
        LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
      } else if (OB_FAIL(dst_datum.from_obj(dst_obj))) {
        LOG_WARN("failed to cast obj", K(ret));
      } else if (is_lob_storage(dst_obj.get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(dst_obj, dst_expr->obj_meta_,
                                             dst_expr->obj_datum_map_, *output_row_cast_ctx_.allocator_v2_, dst_datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(i), K(dst_obj.get_meta()), K(dst_expr->obj_meta_));
      } else {
        dst_expr->set_evaluated_projected(eval_ctx);
      }
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::convert_key(const ObRowkey &src, ObRowkey &dst,
                                               bool is_start_key, int64_t pos)
{
  int ret = OB_SUCCESS;
  UNUSED(is_start_key);
  if (src.get_obj_cnt() > 0) {
    const ObObj *src_key_objs = src.get_obj_ptr();
    void *tmp_ptr = NULL;
    ObObj *new_key_obj = NULL;
    tmp_ptr = key_alloc_->alloc(src.get_obj_cnt() * sizeof(ObObj));
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (OB_ISNULL(new_key_obj = new (tmp_ptr) ObObj[src.get_obj_cnt()])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (src.get_obj_cnt() != key_types_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keys are not match with columns", K(ret));
    }
    lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
    for (int64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < src.get_obj_cnt(); ++nth_obj) {
      const ObObj &src_obj = src_key_objs[nth_obj];
      if (pos == nth_obj && (src_obj.is_ext() || src_obj.is_null())) {
        /**
         * explain extended select * from t1 where c1<1;
          mysql: range(NULL,MAX,MAX ; 1,MIN,MIN)
          oracle:range(MIN,MIN,MIN ; 1,MIN,MIN)

          explain extended select * from t1 where c1<=1;
          mysql:range(NULL,MAX,MAX ; 1,MAX,MAX)
          oracle:range(MIN,MIN,MIN ; 1,MAX,MAX)

          explain extended select * from t1 where c1>1;
          mysql:range(1,MAX,MAX ; MAX,MAX,MAX)
          oracle:range(1,MAX,MAX ; NULL,MIN,MIN)

          explain extended select * from t1 where c1>=1;
          mysql:range(1,MIN,MIN ; MAX,MAX,MAX)
          oracle:range(1,MIN,MIN ; NULL,MIN,MIN)

          explain extended select * from t1 where c1=1 and c2<1;
          mysql:range(1,NULL,MAX ; 1,1,MIN)
          oracle:range(1,MIN,MIN ; 1,1,MIN)
         **/
        if (is_start_key) {
          if (src_obj.is_min_value()) {
            for (int64_t null_pos = nth_obj; null_pos < src.get_obj_cnt() && OB_SUCC(ret); ++null_pos) {
              if (src_key_objs[null_pos].is_min_value()) {
                if (null_pos == nth_obj) {
                  new_key_obj[nth_obj].set_null();
                } else {
                  new_key_obj[nth_obj].set_max_value();
                }
              }
            }
          }
        } else {
          if (src_obj.is_null()) {
            for (int64_t null_pos = nth_obj; null_pos < src.get_obj_cnt() && OB_SUCC(ret); ++null_pos) {
              if (src_key_objs[null_pos].is_null()) {
                if (pos != nth_obj) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected status: end key range is invalid", K(ret), K(src));
                } else {
                  new_key_obj[nth_obj].set_max_value();
                }
              } else if (src_key_objs[null_pos].is_min_value()) {
                new_key_obj[nth_obj].set_max_value();
              }
            }
          }
        }
        break;
      } else if (src_obj.is_min_value()) {
        new_key_obj[nth_obj].set_min_value();
      } else if (src_obj.is_max_value()) {
        new_key_obj[nth_obj].set_max_value();
      } else if (src_obj.is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key range", K(src), K(ret));
      } else {
        const ObObjMeta &obj_meta = key_types_->at(nth_obj);
        if (OB_FAIL(ObObjCaster::to_type(obj_meta.get_type(),
                                        obj_meta.get_collation_type(),
                                        key_cast_ctx_,
                                        src_key_objs[nth_obj],
                                        new_key_obj[nth_obj]))) {
          LOG_WARN("fail to cast obj", K(ret), K(key_types_->at(nth_obj)),
            K(src_key_objs[nth_obj]));
        } else {
          if (has_tenant_id_col_ && tenant_id_col_idx_ == nth_obj) {
            if (new_key_obj[nth_obj].get_type() == ObIntType) {
              new_key_obj[nth_obj].set_int(new_key_obj[nth_obj].get_int() - cur_tenant_id_);
            }
          }
        }
      }
    }//end for
    if (OB_SUCC(ret)) {
      LOG_TRACE("trace range key", K(ret), K(new_key_obj[0]), K(tenant_id_col_id_));
      dst.assign(new_key_obj, src.get_obj_cnt());
    }
  }
  return ret;
}

//attention!!!, following function is inited to convert range only, can't do other operator.
int ObVirtualTableResultConverter::init_convert_key_ranges_info(
  ObIAllocator *key_alloc,
  sql::ObSQLSessionInfo *session,
  const share::schema::ObTableSchema *table_schema,
  const common::ObIArray<ObObjMeta> *key_types,
  const bool has_tenant_id_col,
  const int64_t tenant_id_col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_alloc) || OB_ISNULL(session) || OB_ISNULL(table_schema) || OB_ISNULL(key_types)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(key_alloc), K(session), K(table_schema), K(key_types));
  } else {
    key_alloc_ = key_alloc;
    key_types_ = key_types;
    has_tenant_id_col_ = has_tenant_id_col;
    tenant_id_col_idx_ = tenant_id_col_idx;
    cur_tenant_id_ = table_schema->get_tenant_id();
    ObCollationType key_cs_type = ObCharset::get_system_collation();
    for (int64_t i = 0; i < key_types->count() && OB_SUCC(ret); ++i) {
      const ObObjMeta &obj_meta = key_types->at(i);
      if (ob_is_string_tc(obj_meta.get_type())) {
        key_cs_type = obj_meta.get_collation_type();
        break;
      }
    }
    lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
    key_cast_ctx_ = *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx(key_alloc, &dtc_params, CM_NONE, key_cs_type));
  }
  return ret;
}

int ObSQLUtils::check_table_version(bool &equal,
    const DependenyTableStore &dependency_tables,
    share::schema::ObSchemaGetterGuard &schema_guard) {
  int ret = OB_SUCCESS;
  equal = true;
  int64_t latest_table_version = -1;
  for (int64_t i = 0; i < dependency_tables.count(); i++) {
    const share::schema::ObSchemaObjVersion &table_version = dependency_tables.at(i);
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(schema_guard.get_schema_version(
        TABLE_SCHEMA, tenant_id, table_version.get_object_id(), latest_table_version))) {
      LOG_WARN("failed to get table schema version", K(ret),
              K(tenant_id), K(table_version.get_object_id()));
    }
    if (table_version.get_version() != latest_table_version) {
      equal = false;
    }
  }
  return ret;
}

int ObSQLUtils::generate_view_definition_for_resolve(ObIAllocator &allocator,
                                                     ObCollationType connection_collation,
                                                     const ObViewSchema &view_schema,
                                                     ObString &view_definition)
{
  int ret = OB_SUCCESS;
  const ObString raw_view_def = view_schema.get_view_definition_str();
  view_definition.reset();

  if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(allocator,
                                                          raw_view_def,
                                                          view_definition,
                                                          CS_TYPE_UTF8MB4_GENERAL_CI,
                                                          connection_collation))) {
    LOG_WARN("fail to copy and convert string charset", K(ret));
  }
  return ret;
}

int ObSQLUtils::convert_sql_text_from_schema_for_resolve(ObIAllocator &allocator,
                                                         const ObDataTypeCastParams &dtc_params,
                                                         ObString &sql_text,
                                                         int64_t convert_flag,
                                                         int64_t *action_flag)
{
  int ret = OB_SUCCESS;
  OZ (ObCharset::charset_convert(allocator,
                                 sql_text,
                                 CS_TYPE_UTF8MB4_BIN,
                                 dtc_params.connection_collation_,
                                 sql_text,
                                 convert_flag,
                                 action_flag));
  return ret;
}

int ObSQLUtils::convert_escape_char(ObIAllocator &allocator, const ObString &in, ObString &out)
{
  int ret = OB_SUCCESS;

  if (!in.empty()) {
    char *buf = nullptr;
    int32_t src_len = in.length();
    int32_t res_buf_len = 2 * src_len;
    int32_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(res_buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      out.reset();
      LOG_WARN("allocate memory failed", K(ret), K(in), K(res_buf_len));
    } else {
      for (int32_t i = 0; i < src_len; i++) {
        if (in[i] == '\\') {
          buf[pos++] = '\\';
        }
        buf[pos++] = in[i];
      }
      out.assign_ptr(buf, pos);
    }
  }
  return ret;
}


int ObSQLUtils::convert_sql_text_to_schema_for_storing(ObIAllocator &allocator,
                                                       const ObDataTypeCastParams &dtc_params,
                                                       ObString &sql_text,
                                                       int64_t convert_flag,
                                                       int64_t *action_flag)
{
  int ret = OB_SUCCESS;
  OZ (ObCharset::charset_convert(allocator,
                                 sql_text,
                                 dtc_params.connection_collation_,
                                 CS_TYPE_UTF8MB4_BIN,
                                 sql_text,
                                 convert_flag,
                                 action_flag));

  //validation for oracle mode:
  //  since the meta table is always utf8 in OB
  //  we need test if sql_text can convert to the database charset in oracle mode
  //  if not, replace the invalid character to '?'
  if (OB_SUCC(ret) && lib::is_oracle_mode()
      && ObCharset::is_valid_collation(dtc_params.nls_collation_)
      && ObCharset::charset_type_by_coll(dtc_params.nls_collation_) != CHARSET_UTF8MB4) {
    int64_t actions = 0;
    ObString temp_result;
    OZ (ObCharset::charset_convert(allocator,
                                   sql_text,
                                   CS_TYPE_UTF8MB4_BIN,
                                   dtc_params.nls_collation_,
                                   temp_result,
                                   ObCharset::REPLACE_UNKNOWN_CHARACTER,
                                   &actions));
    if (!!(actions & ObCharset::REPLACE_UNKNOWN_CHARACTER)) {
      OZ (ObCharset::charset_convert(allocator,
                                     temp_result,
                                     dtc_params.nls_collation_,
                                     CS_TYPE_UTF8MB4_BIN,
                                     sql_text));
    }
  }
  return ret;
}

int ObSQLUtils::print_identifier(char *buf, const int64_t buf_len, int64_t &pos,
                                 ObCollationType connection_collation,
                                 const common::ObString &identifier_name,
                                 bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintIdentifier");
  ObString print_name;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(generate_new_name_with_escape_character(allocator,
                                                      identifier_name,
                                                      print_name,
                                                      is_oracle_mode))) {
    LOG_WARN("failed to generate new name with escape character", K(ret));
  } else if (ObCharset::charset_type_by_coll(connection_collation)
      == CHARSET_UTF8MB4) {
    if (OB_UNLIKELY(pos + print_name.length() > buf_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(print_name));
    } else {
      MEMCPY(buf + pos, print_name.ptr(), print_name.length());
      pos += print_name.length();
    }
  } else if (OB_UNLIKELY(buf_len <= pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret));
  } else {
    uint32_t result_len = 0;
    if (OB_FAIL(ObCharset::charset_convert(CS_TYPE_UTF8MB4_BIN,
                                           print_name.ptr(),
                                           print_name.length(),
                                           connection_collation,
                                           buf + pos,
                                           buf_len - pos,
                                           result_len))) {
      LOG_WARN("fail to convert charset", K(ret), K(buf_len), K(pos));
    } else {
      pos += result_len;
    }
  }

  return ret;
}

int64_t ObSqlFatalErrExtraInfoGuard::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  J_OBJ_START();
  J_NEWLINE();
  J_KV(K_(cur_sql));
  const ObIArray<ObSchemaObjVersion> *dep_tables = nullptr;
  ObString sys_var_values;

  if (OB_NOT_NULL(plan_)) { //plan非空，处于执行期
    dep_tables = &(plan_->get_dependency_table());
    sys_var_values = plan_->stat_.sys_vars_str_;
  } else if (OB_NOT_NULL(exec_ctx_)) {
    ObStmtFactory *stmt_factory = nullptr;
    ObQueryCtx *query_ctx = nullptr;
    if (OB_ISNULL(stmt_factory = exec_ctx_->get_stmt_factory())
        || OB_ISNULL(query_ctx = stmt_factory->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get query ctx", K(ret));
    } else {
      dep_tables = &(query_ctx->global_dependency_tables_);
    }
    if (OB_NOT_NULL(exec_ctx_->get_my_session())) {
      sys_var_values = exec_ctx_->get_my_session()->get_sys_var_in_pc_str();
    }
  }

  //打印计划依赖的schema信息
  if (OB_NOT_NULL(dep_tables)) {
    OZ (databuff_printf(buf, buf_len, pos, ", \ndependency_table_def:"));
    for (int i = 0; i < dep_tables->count(); ++i) {
      const ObSchemaObjVersion &schema_obj = dep_tables->at(i);
      if (schema_obj.get_schema_type() == TABLE_SCHEMA) {
        ObSchemaGetterGuard schema_guard;
        ObSchemaPrinter schema_printer(schema_guard);
        ObCharsetType charset_type = CHARSET_INVALID;
        OZ (exec_ctx_->get_my_session()->get_character_set_results(charset_type));
        OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard, schema_obj.version_));
        OZ (databuff_printf(buf, buf_len, pos, (i != 0) ? ",\n\"" : "\n\""));
        OZ (schema_printer.print_table_definition(tenant_id_, schema_obj.get_object_id(), buf, buf_len, pos, NULL, LS_DEFAULT, false, charset_type));
        OZ (databuff_printf(buf, buf_len, pos, "\""));
      }
    }
  }

  //打印计划执行系统变量环境信息
  if (!sys_var_values.empty()) {
    OZ (databuff_printf(buf, buf_len, pos, ",\nsys_vars:{"));
    for (int i = 0; i < ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
      if (!!(ObSysVariables::get_flags(i) & ObSysVarFlag::INFLUENCE_PLAN)) {
        ObString cur_var_value = sys_var_values.split_on(',');
        const char *sep_str = ",";
        if (cur_var_value.empty()) {
          cur_var_value = sys_var_values;
          sep_str = "";
        }
        OZ (databuff_printf(buf, buf_len, pos, "\"%.*s\":\"%.*s\"%s",
                            ObSysVariables::get_name(i).length(),
                            ObSysVariables::get_name(i).ptr(),
                            cur_var_value.length(),
                            cur_var_value.ptr(),
                            sep_str));
      }
    }
    OZ (databuff_printf(buf, buf_len, pos, "}"));
  }
  //打印计划树
  //OX (plan_->print_tree(buf, buf_len, pos, plan_->get_main_query()));


  J_NEWLINE();
  J_OBJ_END();
  return pos;
}

void ObSQLUtils::record_execute_time(const ObPhyPlanType type,
                                     const int64_t time_cost)
{
  #define ADD_EXECUTE_TIME(type)                  \
    case OB_PHY_PLAN_##type:                      \
      EVENT_ADD(SQL_##type##_TIME, time_cost);    \
      break
  switch(type)
  {
    ADD_EXECUTE_TIME(LOCAL);
    ADD_EXECUTE_TIME(REMOTE);
    ADD_EXECUTE_TIME(DISTRIBUTED);
    default: {}
  }
  #undef ADD_EXECUTE_TIME
}

int ObSQLUtils::handle_audit_record(bool need_retry,
                                    const ObExecuteMode exec_mode,
                                    ObSQLSessionInfo &session,
                                    bool is_sensitive)
{
  int ret = OB_SUCCESS;
  if (need_retry) {
    /*do nothing*/
  } else if (GCONF.enable_sql_audit && session.get_local_ob_enable_sql_audit()) {
    ObMySQLRequestManager *req_manager = session.get_request_manager();
    if (OB_ISNULL(req_manager)) {
      // failed to get request manager, maybe tenant has been dropped, NOT NEED TO record;
    } else {
      const ObAuditRecordData &audit_record = session.get_final_audit_record(exec_mode);
      if (OB_FAIL(req_manager->record_request(audit_record,
                                              session.enable_query_response_time_stats(),
                                              is_sensitive))) {
        if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
          LOG_DEBUG("cannot allocate mem for record", K(ret));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  if (lib::is_diagnose_info_enabled()) {
    session.update_stat_from_exec_record();
  }
  session.update_stat_from_exec_timestamp();
  session.reset_audit_record(need_retry);
  return ret;
}


bool ObSQLUtils::is_oracle_empty_string(const ObObjParam &param)
{
  return (param.is_null() && (ObCharType == param.get_param_meta().get_type()
                              || ObNCharType == param.get_param_meta().get_type()));
}

bool ObSQLUtils::is_one_part_table_can_skip_part_calc(const ObTableSchema &schema)
{
  bool can_skip = false;
  if (!schema.is_partitioned_table()) {
    can_skip = true;
  } else if (schema.get_all_part_num() == 1 && schema.is_hash_part()) {
    if (PARTITION_LEVEL_ONE == schema.get_part_level()) {
      can_skip = true;
    } else if (PARTITION_LEVEL_TWO == schema.get_part_level()
              && schema.is_hash_subpart()) {
      can_skip = true;
    } else {
      can_skip = false;
    }
  } else {
    can_skip = false;
  }
  return can_skip;
}

int ObSQLUtils::create_encode_sortkey_expr(
  ObRawExprFactory &expr_factory,
  ObExecContext* exec_ctx,
  const common::ObIArray<OrderItem> &order_keys,
  int64_t start_key,
  OrderItem &encode_sortkey)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* encode_expr = NULL;
  if (OB_ISNULL(exec_ctx)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_ENCODE_SORTKEY, encode_expr))) {
    LOG_WARN("failed to create encode_expr", K(ret));
  } else {
    // Assemble encode sortkey.
    for (int64_t i = start_key; OB_SUCC(ret) && i < order_keys.count(); i++) {
      ObConstRawExpr *nulls_pos_expr = nullptr;
      ObConstRawExpr *order_expr = nullptr;
      ObObj null_pos_obj;
      ObObj order_obj;
      null_pos_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      order_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      if (OB_ISNULL(order_keys.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(order_keys.at(i).expr_), K(ret));
      } else if (OB_FAIL(expr_factory.create_raw_expr(T_VARCHAR, nulls_pos_expr))) {
        LOG_WARN("failed to create null_pos expr", K(ret));
      } else if (OB_FAIL(expr_factory.create_raw_expr(T_VARCHAR, order_expr))) {
        LOG_WARN("failed to create order expr", K(ret));
      } else {
        switch (order_keys.at(i).order_type_) {
          case NULLS_LAST_ASC: {
            null_pos_obj.set_int(1);
            order_obj.set_int(0);
            break;
          }
          case NULLS_FIRST_DESC: {
            null_pos_obj.set_int(0);
            order_obj.set_int(1);
            break;
          }
          case NULLS_LAST_DESC: {
            null_pos_obj.set_int(1);
            order_obj.set_int(1);
            break;
          }
          case NULLS_FIRST_ASC:
          default: {
            null_pos_obj.set_int(0);
            order_obj.set_int(0);
            break;
          }
        }
        nulls_pos_expr->set_value(null_pos_obj);
        order_expr->set_value(order_obj);
        encode_expr->add_param_expr(order_keys.at(i).expr_);
        encode_expr->add_param_expr(order_expr);
        encode_expr->add_param_expr(nulls_pos_expr);
      }
    } // end for

    // push bach encode sortkey expr
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(encode_expr->formalize(exec_ctx->get_my_session()))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }  else {
      encode_sortkey.expr_ = encode_expr;
      encode_sortkey.order_type_ = NULLS_FIRST_ASC;
      LOG_DEBUG("debug encode sortkey", K(*encode_expr));
    }
  }
  return ret;
}

bool ObSQLUtils::is_enable_explain_batched_multi_statement()
{
  return 0 != EVENT_CALL(EventTable::EN_EXPLAIN_BATCHED_MULTI_STATEMENT);
}

bool ObExprConstraint::operator==(const ObExprConstraint &rhs) const
{
  bool bret = false;
  if (OB_ISNULL(pre_calc_expr_) || OB_ISNULL(rhs.pre_calc_expr_)) {
    bret = false;
  } else {
    bret = expect_result_ == rhs.expect_result_ &&
           pre_calc_expr_->same_as(*rhs.pre_calc_expr_);
  }
  return bret;
}

int ObSqlGeoUtils::check_srid_by_srs(uint64_t tenant_id, uint64_t srid)
{
  int ret = OB_SUCCESS;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;

  if (0 == srid || UINT32_MAX == srid) {
    // do nothing
  } else if (UINT32_MAX < srid) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "srid", "UINT32_MAX");
  } else if (srid != 0 &&
      OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("failed to get srs guard", K(tenant_id), K(srid), K(ret));
  } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
    LOG_WARN("get srs failed", K(srid), K(ret));
  }

  return ret;
}

int ObSqlGeoUtils::check_srid(uint32_t column_srid, uint32_t input_srid)
{
  int ret = OB_SUCCESS;
  // todo : get effective tenant_id
  uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(check_srid_by_srs(tenant_id, input_srid))) {
    LOG_WARN("invalid srid", K(ret), K(input_srid));
  } else if (UINT32_MAX == column_srid) {
    // do nothing, accept all.
  } else if (input_srid != column_srid) {
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    LOG_WARN("different srid", K(ret), K(column_srid), K(input_srid));
  }

  return ret;
}

int ObPreCalcExprConstraint::assign(const ObPreCalcExprConstraint &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pre_calc_expr_info_.assign(other.pre_calc_expr_info_, allocator))) {
    LOG_WARN("failed to copy pre calculable expression info");
  } else {
    expect_result_ = other.expect_result_;
  }
  return ret;
}

int ObPreCalcExprConstraint::check_is_match(const ObObjParam &obj_param, bool &is_match) const
{
  int ret = OB_SUCCESS;
  switch (expect_result_) {
    case PRE_CALC_RESULT_NULL:
      is_match = obj_param.is_null();
      break;
    case PRE_CALC_RESULT_NOT_NULL:
      is_match = !obj_param.is_null();
      break;
    case PRE_CALC_RESULT_TRUE:
      is_match = obj_param.get_bool();
      break;
    case PRE_CALC_RESULT_FALSE:
      is_match = !obj_param.get_bool();
      break;
    case PRE_CALC_PRECISE:
    case PRE_CALC_NOT_PRECISE: {
      //default escape
      //@todu JueHui: make escape value can be parameterized
      char escape = '\\';
      bool is_precise = false;
      bool expect_precise = PRE_CALC_PRECISE == expect_result_;
      if (OB_FAIL(ObQueryRange::is_precise_like_range(obj_param, escape, is_precise))) {
        LOG_WARN("failed to check precise constraint.", K(ret));
      } else {
        is_match = is_precise == expect_precise;
      }
      break;
    }
    case PRE_CALC_RESULT_NO_WILDCARD: {
      ObString pattern_val = obj_param.get_string();
      if (obj_param.is_lob()) {
        is_match = false;
      } else if (!pattern_val.empty()) {
        is_match = OB_ISNULL(pattern_val.find('%')) &&
                  OB_ISNULL(pattern_val.find('_')) &&
                  OB_ISNULL(pattern_val.find('\\'));
      } else {
        is_match = true;
      }
    }
    break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expect res type", K_(expect_result), K(ret));
      break;
  } // switch end
  return ret;
}

int ObRowidConstraint::assign(const ObPreCalcExprConstraint &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObRowidConstraint *tmp_other = dynamic_cast<const ObRowidConstraint *>(&other);
  if (OB_ISNULL(tmp_other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected constraint", K(other), K(ret));
  } else if (OB_FAIL(ObPreCalcExprConstraint::assign(other, allocator))) {
    LOG_WARN("failed to copy pre calc expr constraint", K(ret));
  } else if (OB_FAIL(rowid_type_array_.assign(tmp_other->rowid_type_array_))) {
    LOG_WARN("failed to copy rowid type array", K(ret));
  } else {
    rowid_version_ = tmp_other->rowid_version_;
  }
  return ret;
}

int ObRowidConstraint::check_is_match(const ObObjParam &obj_param, bool &is_match) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!obj_param.is_urowid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowid param", K(obj_param), K(ret));
  } else {
    int64_t pk_cnt;
    ObArray<ObObj> pk_vals;
    const ObURowIDData &urowid_data = obj_param.get_urowid();
    if (OB_FAIL(urowid_data.get_pk_vals(pk_vals))) {
      LOG_WARN("failed to get pk values", K(ret));
    } else {
      pk_cnt = urowid_data.get_real_pk_count(pk_vals);
      is_match = rowid_version_ == urowid_data.get_version() &&
                 rowid_type_array_.count() == pk_cnt;
      for (int i = 0; is_match && i < pk_cnt; ++i) {
        if (rowid_type_array_.at(i) != pk_vals.at(i).get_type()) {
          is_match = false;
        }
      }
    }
  }
  return ret;
}

ObItemType ObSQLUtils::get_sql_item_type(const ParseResult &result)
{
  ObItemType type = ObItemType::T_INVALID;
  if (NULL != result.result_tree_
      && NULL != result.result_tree_->children_
      && NULL != result.result_tree_->children_[0]) {
    type = result.result_tree_->children_[0]->type_;
  }
  return type;
}

bool ObSQLUtils::is_support_batch_exec(ObItemType type)
{
  bool is_support = false;
  if (T_UPDATE == type || T_INSERT == type || T_DELETE == type) {
    is_support = true;
  }
  return is_support;
}
//this sql is triggered by pl(trigger, procedure, pl udf etc.)
//and the transaction is not autonomous, otherwise this SQL is controlled by a independent transaction
bool ObSQLUtils::is_pl_nested_sql(ObExecContext *cur_ctx)
{
  bool bret = false;
  if (cur_ctx != nullptr && cur_ctx->get_parent_ctx() != nullptr) {
    ObExecContext *parent_ctx = cur_ctx->get_parent_ctx();
    //parent_sql = is_dml_stmt means this sql is triggered by a sql, not pl procedure
    if (OB_NOT_NULL(parent_ctx->get_sql_ctx())
        && parent_ctx->get_pl_stack_ctx() != nullptr
        && !parent_ctx->get_pl_stack_ctx()->in_autonomous()) {
      if (ObStmt::is_dml_stmt(parent_ctx->get_sql_ctx()->stmt_type_)) {
        bret = true;
      } else if (stmt::T_ANONYMOUS_BLOCK == parent_ctx->get_sql_ctx()->stmt_type_
                 || stmt::T_CALL_PROCEDURE == parent_ctx->get_sql_ctx()->stmt_type_) {
        /* anonymous block in a store procedure will be send to sql engine as sql, which will make new obexeccontext.
           consider follow scene:
            dml1(exec_ctx1)->udf/trigger->procedure->anonymous block(exec_ctx2)->dml2
          outer dml1 and inner dml2 form a nested scene. bug current code logic cannot identify this scene.
          so it need to skip anonymous block obexeccontext */
        do {
          parent_ctx = parent_ctx->get_parent_ctx();
        } while (OB_NOT_NULL(parent_ctx) && OB_NOT_NULL(parent_ctx->get_sql_ctx()) &&
                 (stmt::T_ANONYMOUS_BLOCK == parent_ctx->get_sql_ctx()->stmt_type_
                  || stmt::T_CALL_PROCEDURE == parent_ctx->get_sql_ctx()->stmt_type_));

        if (OB_NOT_NULL(parent_ctx) &&
            OB_NOT_NULL(parent_ctx->get_sql_ctx()) &&
            OB_NOT_NULL(parent_ctx->get_pl_stack_ctx()) &&
            ObStmt::is_dml_stmt(parent_ctx->get_sql_ctx()->stmt_type_) &&
            !parent_ctx->get_pl_stack_ctx()->in_autonomous()) {
          bret = true;
        }
      }
    }
  }
  return bret;
}

//this sql is triggered by foreign key
bool ObSQLUtils::is_fk_nested_sql(ObExecContext *cur_ctx)
{
  bool bret = false;
  if (cur_ctx != nullptr &&
      cur_ctx->get_parent_ctx() != nullptr &&
      cur_ctx->get_parent_ctx()->get_das_ctx().is_fk_cascading_) {
    bret = true;
  }
  return bret;
}

//this sql is triggered by online stat gathering
bool ObSQLUtils::is_online_stat_gathering_nested_sql(ObExecContext *cur_ctx)
{
  bool bret = false;
  if (cur_ctx != nullptr &&
      cur_ctx->get_parent_ctx() != nullptr &&
      cur_ctx->get_parent_ctx()->is_online_stats_gathering()) {
    bret = true;
  }
  return bret;
}

bool ObSQLUtils::is_iter_uncommitted_row(ObExecContext *cur_ctx)
{
  bool bret = false;
  if (cur_ctx != nullptr &&
      cur_ctx->get_parent_ctx() != nullptr &&
      cur_ctx->get_parent_ctx()->get_das_ctx().iter_uncommitted_row_) {
    bret = true;
  }
  return bret;
}

//notice: if a SQL is triggered by a PL defined as an autonomous transaction,
//then it is not nested sql, nor is it restricted by the constraints of nested sql
bool ObSQLUtils::is_nested_sql(ObExecContext *cur_ctx)
{
  return is_pl_nested_sql(cur_ctx) || is_fk_nested_sql(cur_ctx) || is_online_stat_gathering_nested_sql(cur_ctx);
}

bool ObSQLUtils::is_in_autonomous_block(ObExecContext *cur_ctx)
{
  bool bret = false;
  pl::ObPLContext *pl_context = nullptr;
  if (cur_ctx != nullptr) {
    pl_context = cur_ctx->get_pl_stack_ctx();
    for (; !bret && pl_context != nullptr; pl_context = pl_context->get_parent_stack_ctx()) {
      if (pl_context->in_autonomous()) {
        bret = true;
      }
    }
  }
  return bret;
}

bool ObSQLUtils::is_select_from_dual(ObExecContext &ctx)
{
  bool bret = false;
  if (stmt::T_SELECT == ctx.get_sql_ctx()->stmt_type_ && ctx.get_sql_ctx() != nullptr) {
    const ObIArray<ObSchemaObjVersion> *depend_tables = nullptr;
    if (ctx.get_sql_ctx()->cur_plan_ != nullptr) {
      depend_tables = &(ctx.get_sql_ctx()->cur_plan_->get_dependency_table());
    } else {
      depend_tables = ctx.get_sql_ctx()->cur_stmt_->get_global_dependency_table();
    }
    if (depend_tables == nullptr) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "depend tables is nullptr");
    } else if (depend_tables->empty()) {
      bret = true;
    } else {
      bool has_table = false;
      for (int64_t i = 0; !has_table && i < depend_tables->count(); ++i) {
        if (depend_tables->at(i).is_base_table()) {
          has_table = true;
        }
      }
      bret = !has_table;
    }
  }
  return bret;
}

//bind current execute context to my session,
//in order to access exec_ctx of the current statement through the session
void LinkExecCtxGuard::link_current_context()
{
  LOG_DEBUG("link current context", K(session_.get_cur_exec_ctx()), K(&exec_ctx_), K(&session_));
  if (session_.get_cur_exec_ctx() != &exec_ctx_) {
    //current execute context in session is not itself
    //it means that the execution of this SQL is in nested sql
    //reference the parent exec ctx in my exec context
    //to facilitate subsequent access to the execution status of parent sql
    ObExecContext *parent_ctx = session_.get_cur_exec_ctx();
    exec_ctx_.set_parent_ctx(parent_ctx);
    if (parent_ctx != nullptr) {
      //mark the recursive sql levels
      exec_ctx_.set_nested_level(parent_ctx->get_nested_level() + 1);
    }
    //switch the exec ctx reference in sql session
    session_.set_cur_exec_ctx(&exec_ctx_);
    is_linked_ = true;
    if (ObSQLUtils::is_nested_sql(&exec_ctx_)) {
      //to be compatible with MySQL and Oracle's nested sql behavior
      //force to set session.autocommit=false
      //make the nested sql commit as a whole
      session_.get_autocommit(is_autocommit_);
      session_.set_autocommit(false);
    }
  }
}

//unlink current execute context with my session to
//forbid accessing this sql exec context through session
//usually at the end of the sql execution
void LinkExecCtxGuard::unlink_current_context()
{
  //current sql execution finish,
  //now restore the cur_exec_ctx pointer on the session to the state
  //before the statement was executed
  LOG_DEBUG("unlink current context", K(is_linked_), K(session_.get_cur_exec_ctx()),
           K(&exec_ctx_), K(&session_), K(exec_ctx_.get_parent_ctx()));
  if (is_linked_) {
    if (ObSQLUtils::is_nested_sql(&exec_ctx_)) {
      //restore the autocommit flag at first
      session_.set_autocommit(is_autocommit_);
    }
    session_.set_cur_exec_ctx(exec_ctx_.get_parent_ctx());
  }
}

int ObSQLUtils::get_obj_from_ext_obj(const ObObjParam &ext_obj, int64_t pos, ObObj *&data)
{
  int ret = OB_SUCCESS;
  pl::ObPLCollection *coll = NULL;
  if (!ext_obj.is_ext()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arraybinding params must be ext type", K(ret), K(ext_obj));
  } else if (OB_ISNULL(coll = reinterpret_cast<pl::ObPLCollection*>(ext_obj.get_ext()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll is null", K(ret), K(ext_obj));
  } else if (coll->get_count() <= pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll count is unexpected", K(ret), K(coll->get_count()));
  } else if (1 != coll->get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll column_count is unexpected", K(ret), K(coll->get_column_count()));
  } else if (OB_ISNULL(data = reinterpret_cast<ObObj*>(coll->get_data()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is unexpected", K(ret));
  } else {
    data = data + pos;
    LOG_DEBUG("get one obj from ext params", KPC(data));
  }
  return ret;
}

int ObSQLUtils::get_result_from_ctx(ObExecContext &exec_ctx,
                                    const ObRawExpr *expr,
                                    ObObj &result,
                                    bool &is_valid,
                                    bool &hit_cache)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = nullptr;
  uint64_t key = 0;
  hit_cache = false;
  if (OB_ISNULL(expr) || OB_ISNULL(exec_ctx.get_stmt_factory()) ||
      OB_ISNULL(query_ctx = exec_ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KP(expr), K(exec_ctx), K(ret));
  } else if (!query_ctx->calculable_expr_results_.created()) {
    // do nothing
  } else if (FALSE_IT(key = reinterpret_cast<uint64_t>(expr))) {
  } else if (OB_FAIL(query_ctx->calculable_expr_results_.get_refactored(key, result))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get result", K(ret));
    }
  } else {
    hit_cache = true;
    is_valid = !result.is_nop_value();
  }
  return ret;
}

int ObSQLUtils::store_result_to_ctx(ObExecContext &exec_ctx,
                                    const ObRawExpr *expr,
                                    const ObObj &result,
                                    bool is_valid)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = nullptr;
  uint64_t key = 0;
  ObObj val;
  if (OB_ISNULL(expr) || OB_ISNULL(exec_ctx.get_stmt_factory()) ||
      OB_ISNULL(query_ctx = exec_ctx.get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KP(expr), K(exec_ctx), K(ret));
  } else {
    key = reinterpret_cast<uint64_t>(expr);
    if (is_valid) {
      if (result.is_pl_extend()) {
        // pl extend type is already deep copied in se_calc_const_expr
        val = result;
      } else if (OB_FAIL(ob_write_obj(exec_ctx.get_allocator(), result, val))) {
        LOG_WARN("failed to write obj", K(result), K(ret));
      }
    } else {
      val.set_nop_value();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!query_ctx->calculable_expr_results_.created() &&
             OB_FAIL(query_ctx->calculable_expr_results_.create(20, ObModIds::OB_SQL_COMPILE))) {
    LOG_WARN("failed to create calculable expr results map", K(ret));
  } else if (OB_FAIL(query_ctx->calculable_expr_results_.set_refactored(key, val))) {
    LOG_WARN("failed to set result", K(ret));
  }
  return ret;
}

int ObSQLUtils::add_calc_failure_constraint(const ObRawExpr *raw_expr,
                                            ObIArray<ObExprConstraint> &constraints)
{
  int ret = OB_SUCCESS;
  bool existed = false;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null", K(ret));
  } else if (OB_UNLIKELY(!raw_expr->is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calculable expr is expected here", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i <constraints.count(); ++i) {
    if (constraints.at(i).expect_result_ == PRE_CALC_ERROR &&
        constraints.at(i).pre_calc_expr_->same_as(*raw_expr)) {
      existed = true;
      break;
    }
  }
  if (OB_SUCC(ret) && !existed) {
    ObExprConstraint cons(const_cast<ObRawExpr*>(raw_expr), PRE_CALC_ERROR);
    if (OB_FAIL(constraints.push_back(cons))) {
      LOG_WARN("failed to push back pre calc constraints", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::create_multi_stmt_param_store(common::ObIAllocator &allocator,
                                              int64_t query_num,
                                              int64_t param_num,
                                              ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_store.reserve(param_num))) {
    LOG_WARN("failed to reserve param num", K(param_num), K(ret));
  } else {
    void *ptr = NULL;
    void *data_ptr = NULL;
    ObSqlArrayObj *array_params = NULL;
    ObObjParam param;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      param.reset();
      if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObSqlArrayObj)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_ISNULL(data_ptr = allocator.alloc(sizeof(ObObjParam) * query_num))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        array_params = new(ptr) ObSqlArrayObj();
        array_params->count_ = query_num;
        array_params->data_ = new(data_ptr) ObObjParam[query_num];
        param.set_extend(reinterpret_cast<int64_t>(array_params), T_EXT_SQL_ARRAY);
        param.set_param_meta();
        if (OB_FAIL(param_store.push_back(param))) {
          LOG_WARN("failed to push back param", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::get_one_group_params(int64_t &actual_pos, ParamStore &src, ParamStore &obj_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    ObObjParam &obj = src.at(i);
    pl::ObPLCollection *coll = NULL;
    ObObj *data = NULL;
    if (OB_UNLIKELY(!obj.is_ext())) {
      OZ (obj_params.push_back(obj));
    } else {
      CK (OB_NOT_NULL(coll = reinterpret_cast<pl::ObPLCollection*>(obj.get_ext())));
      CK (coll->get_count() > actual_pos);
      CK (1 == coll->get_column_count());
      CK (OB_NOT_NULL(data = reinterpret_cast<ObObj*>(coll->get_data())));
      if (OB_SUCC(ret)) {
        bool is_del = true;
        for (; OB_SUCC(ret) && is_del;) {
          OZ (coll->is_elem_deleted(actual_pos, is_del));
          if (is_del) {
            ++actual_pos;
          }
        }
        OX (obj_params.push_back(*(data + actual_pos)));
      }
    }
  }
  return ret;
}

int ObSQLUtils::copy_params_to_array_params(int64_t query_pos, ParamStore &src, ParamStore &dst,
                                            ObIAllocator &alloc, bool is_forall)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < dst.count(); j++) {
    ObSqlArrayObj *array_params = nullptr;
    if (OB_UNLIKELY(!dst.at(j).is_ext_sql_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param object is invalid", K(ret), K(dst.at(j)));
    } else if (OB_ISNULL(array_params =
        reinterpret_cast<ObSqlArrayObj*>(dst.at(j).get_ext()))
        || OB_ISNULL(array_params->data_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KPC(array_params));
    } else {
      ObObjParam new_param = src.at(j);
      if (is_forall) {
        OZ (deep_copy_obj(alloc, src.at(j), new_param));
      }
      array_params->data_[query_pos] = new_param;
    }
  }
  return ret;
}

int ObSQLUtils::init_elements_info(ParamStore &src, ParamStore &dst)
{
  int ret = OB_SUCCESS;
  CK (dst.count() == src.count())
  for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); ++i) {
    ObSqlArrayObj *array_params = nullptr;
    ObObjParam &obj = src.at(i);
    pl::ObPLCollection *coll = NULL;
    ObObj *data = NULL;
    CK (dst.at(i).is_ext_sql_array());
    CK (OB_NOT_NULL(array_params = reinterpret_cast<ObSqlArrayObj*>(dst.at(i).get_ext())));
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!obj.is_ext())) {
      array_params->element_.set_meta_type(obj.get_meta());
      array_params->element_.set_accuracy(obj.get_accuracy());
    } else {
      CK (OB_NOT_NULL(coll = reinterpret_cast<pl::ObPLCollection*>(obj.get_ext())));
      if (OB_SUCC(ret)) {
        array_params->element_ = coll->get_element_type();
      }
    }
  }
  return ret;
}

int ObSQLUtils::transform_pl_ext_type(
    ParamStore &src, int64_t array_binding_size, ObIAllocator &alloc, ParamStore *&dst, bool is_forall)
{
  int ret = OB_SUCCESS;
  ParamStore *ps_ab_params = NULL;
  // 在这里折叠batch参数到SQL能识别的类型
  if (OB_ISNULL(ps_ab_params = static_cast<ParamStore *>(alloc.alloc(sizeof(ParamStore))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(dst = new(ps_ab_params)ParamStore(ObWrapperAllocator(alloc)))) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::create_multi_stmt_param_store(alloc,
                                                               array_binding_size,
                                                               src.count(),
                                                               *dst))) {
    LOG_WARN("fail to do create param store", K(ret));
  } else {
    ObArenaAllocator tmp_alloc;
    ParamStore temp_obj_params((ObWrapperAllocator(tmp_alloc)));
    int64_t N = src.count();
    int64_t actual_pos = 0;
    for (int64_t query_pos = 0; OB_SUCC(ret) && query_pos < array_binding_size; ++query_pos, ++actual_pos) {
      temp_obj_params.reuse();
      if (OB_FAIL(temp_obj_params.reserve(N))) {
        LOG_WARN("fail to reverse params_store", K(ret));
      } else if (OB_FAIL(get_one_group_params(actual_pos, src, temp_obj_params))) {
        LOG_WARN("get one group params failed", K(ret), K(actual_pos));
      } else if (OB_FAIL(copy_params_to_array_params(query_pos, temp_obj_params, *dst, alloc, is_forall))) {
        LOG_WARN("copy params to array params failed", K(ret), K(query_pos));
      } else if (query_pos == 0) {
        if (OB_FAIL(init_elements_info(src, *dst))) {
          LOG_WARN("copy params to array params failed", K(ret), K(query_pos));
        }
      }
    }
  }
  return ret;
}


void ObSQLUtils::adjust_time_by_ntp_offset(int64_t &dst_timeout_ts)
{
  dst_timeout_ts += THIS_WORKER.get_ntp_offset();
}

bool ObSQLUtils::is_external_files_on_local_disk(const ObString &url)
{
  return url.prefix_match_ci(OB_FILE_PREFIX);
}

int ObSQLUtils::split_remote_object_storage_url(ObString &url, ObBackupStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  ObString https_header = "https://";
  ObString http_header = "http://";
  ObString access_id = url.split_on(':').trim_space_only();
  ObString access_key = url.split_on('@').trim_space_only();
  ObString host_name;
  int64_t header_len = 0;

  url = url.trim_space_only();
  if (url.prefix_match_ci(https_header)) {
    header_len = https_header.length();
  } else if (url.prefix_match_ci(http_header)) {
    header_len = http_header.length();
  } else {
    header_len = 0;
  }
  if (header_len > 0) {
    host_name = url;
    url += header_len;
    ObString temp = url.split_on('/');
    host_name.assign_ptr(host_name.ptr(), header_len + temp.length());
    host_name = host_name.trim_space_only();
  } else {
    host_name = url.split_on('/').trim_space_only();
  }
  url = url.trim_space_only();
  if (access_id.empty() || access_key.empty() || host_name.empty() || url.empty()) {
    ret = OB_URI_ERROR;
    LOG_WARN("incorrect uri", K(ret));
  }
  LOG_DEBUG("check access info", K(access_id), K(access_key), K(host_name), K(url));

  //fill storage_info
  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    OZ (databuff_printf(storage_info.access_id_, OB_MAX_BACKUP_ACCESSID_LENGTH, pos,
                        "%s%.*s", ACCESS_ID, access_id.length(), access_id.ptr()));
    pos = 0;
    OZ (databuff_printf(storage_info.access_key_, OB_MAX_BACKUP_ACCESSKEY_LENGTH, pos,
                        "%s%.*s", ACCESS_KEY, access_key.length(), access_key.ptr()));
    pos = 0;
    OZ (databuff_printf(storage_info.endpoint_, OB_MAX_BACKUP_ENDPOINT_LENGTH, pos,
                        "%s%.*s", "host=", host_name.length(), host_name.ptr()));
    if (OB_FAIL(ret)) {
      ret = OB_URI_ERROR;
      LOG_WARN("incorrect uri", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::check_location_access_priv(const ObString &location, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session", K(ret));
  } else if (is_external_files_on_local_disk(location) && !session->is_inner()) {
    ObArenaAllocator allocator;
    ObString real_location = location;
    real_location += strlen(OB_FILE_PREFIX);
    if (!real_location.empty()) {
      ObArrayWrap<char> buffer;
      OZ (buffer.allocate_array(allocator, PATH_MAX));
      if (OB_SUCC(ret)) {
        real_location = ObString(realpath(to_cstring(real_location), buffer.get_data()));
      }
    }

    if (OB_SUCC(ret) && !real_location.empty()) {
      ObString secure_file_priv;
      OZ (session->get_secure_file_priv(secure_file_priv));
      OZ (ObResolverUtils::check_secure_path(secure_file_priv, real_location));
      if (OB_ERR_NO_PRIVILEGE == ret) {
        ret = OB_ERR_NO_PRIV_DIRECT_PATH_ACCESS;
        LOG_WARN("fail to check secure path", K(ret), K(secure_file_priv), K(real_location));
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SPM
int ObSQLUtils::handle_plan_baseline(const ObAuditRecordData &audit_record,
                                     ObPhysicalPlan *plan,
                                     const int ret_code,
                                     ObSqlCtx &sql_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(plan) && OB_UNLIKELY(OB_SUCCESS != ret_code && plan->get_evolution())) {
    plan->update_plan_error_cnt();
  }
  if (OB_LIKELY(!sql_ctx.spm_ctx_.check_execute_status_)) {
    /*do nothing*/
  } else if (OB_SUCCESS == ret_code) {
    if (OB_FAIL(ObSpmController::accept_new_plan_as_baseline(sql_ctx.spm_ctx_, audit_record))) {
      LOG_WARN("failed to accept new plan as baseline", K(ret));
    }
  } else if (OB_FAIL(ObSpmController::deny_new_plan_as_baseline(sql_ctx.spm_ctx_))) {
    LOG_WARN("failed to deny new plan as baseline", K(ret));
  }
  return ret;
}
#endif

int ObSQLUtils::async_recompile_view(const share::schema::ObTableSchema &old_view_schema,
                                     ObSelectStmt *select_stmt,
                                     bool reset_column_infos,
                                     ObIAllocator &alloc,
                                     ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObTableSchema new_view_schema(&alloc);
  uint64_t data_version = 0;
  bool changed = false;
  if (reset_column_infos) {
    // failed to resolve view definition, do nothing
  } else if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get select stmt", K(ret));
  } else if (is_oracle_mode() && !old_view_schema.is_sys_view()) {
    // column name in column schema should be the same as select item alias name in view definition
    // when view definition is not rebuilt and column list grammar is used, overwrite alias name
    // sys view can not use column list grammar and column count of sys view may be changed
    const ObColumnSchemaV2 *column_schema;
    uint64_t column_id;
    bool is_column_schema_null = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_column_schema_null
                        && i < select_stmt->get_select_item_size(); ++i) {
      column_id = i + OB_APP_MIN_COLUMN_ID;
      column_schema = old_view_schema.get_column_schema(column_id);
      if (OB_ISNULL(column_schema)) {
        is_column_schema_null = true; // column schema of sys view can be null
      } else if (OB_ISNULL(column_schema->get_column_name())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is null", K(ret));
      } else {
        select_stmt->get_select_item(i).alias_name_ = column_schema->get_column_name();
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_view_schema.assign(old_view_schema))) {
    LOG_WARN("failed to assign table schema", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(old_view_schema.get_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_1_0_0) {
    // do nothing
  } else if (OB_ISNULL(GCTX.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get sql engine", K(ret));
  } else if ((0 == old_view_schema.get_object_status()
             || 0 == old_view_schema.get_column_count()
             || (old_view_schema.is_sys_view()
                 && old_view_schema.get_schema_version() <= GCTX.start_time_
                 && OB_HASH_NOT_EXIST == GCTX.sql_engine_->get_dep_info_queue()
                    .read_consistent_sys_view_from_set(old_view_schema.get_tenant_id(),
                                                       old_view_schema.get_table_id())))) {
    if (old_view_schema.is_sys_view() && GCONF.in_upgrade_mode()) {
      //do not recompile sys view until upgrade finish
    } else if (!reset_column_infos) {
      ObArray<ObString> dummy_column_list;
      ObArray<ObString> column_comments;
      bool resolve_succ = true;
      if (OB_ISNULL(select_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get select stmt", K(ret));
      } else if (OB_FAIL(new_view_schema.get_view_column_comment(column_comments))) {
        LOG_WARN("failed to get view column comment", K(ret));
      } else if (OB_FAIL(new_view_schema.delete_all_view_columns())) {
        LOG_WARN("failed to delete all columns", K(ret));
      } else if (OB_ISNULL(select_stmt->get_ref_obj_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref obj is null", K(ret));
      } else if (OB_FAIL(ObCreateViewResolver::add_column_infos(old_view_schema.get_tenant_id(),
                                                                *select_stmt,
                                                                new_view_schema,
                                                                alloc,
                                                                session_info,
                                                                dummy_column_list,
                                                                column_comments))) {
        LOG_WARN("failed to update view column info", K(ret));
      } else if (!new_view_schema.is_view_table() || new_view_schema.get_column_count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong schema", K(ret), K(new_view_schema));
      } else if (old_view_schema.is_sys_view() && OB_FAIL(check_sys_view_changed(old_view_schema, new_view_schema, changed))) {
        LOG_WARN("failed to check sys view changed", K(ret));
      } else if (!select_stmt->get_ref_obj_table()->is_inited() || (old_view_schema.is_sys_view() && !changed)) {
        // do nothing
      } else if (OB_FAIL(GCTX.sql_engine_->get_dep_info_queue().add_view_id_to_set(new_view_schema.get_table_id()))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("table id exists", K(new_view_schema.get_table_id()));
        } else {
          LOG_WARN("failed to set table id", K(ret));
        }
      } else if (OB_FAIL(select_stmt->get_ref_obj_table()->process_reference_obj_table(
        new_view_schema.get_tenant_id(), new_view_schema.get_table_id(), &new_view_schema, GCTX.sql_engine_->get_dep_info_queue()))) {
        LOG_WARN("failed to process reference obj table", K(ret), K(new_view_schema), K(old_view_schema));
      }
    } else if (lib::is_oracle_mode()) {
      bool already_invalid = false;
      if (OB_FAIL(new_view_schema.alter_all_view_columns_type_undefined(already_invalid))) {
        LOG_WARN("failed to reset all columns", K(ret));
      } else if (already_invalid) {
        // have reset all column to be undefined before, do nothing
      } else if (!new_view_schema.is_view_table() || new_view_schema.get_column_count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong schema", K(ret), K(new_view_schema));
      } else if (OB_FAIL(GCTX.sql_engine_->get_dep_info_queue().add_view_id_to_set(new_view_schema.get_table_id()))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("table id exists", K(new_view_schema.get_table_id()));
        } else {
          LOG_WARN("failed to set table id", K(ret));
        }
      } else {
        SMART_VAR(sql::ObMaintainObjDepInfoTask, task, new_view_schema.get_tenant_id()) {
          if (OB_FAIL(task.assign_view_schema(new_view_schema))) {
            LOG_WARN("failed to assign view schema", K(ret));
          } else if (FALSE_IT(task.set_reset_view_column_infos(true))) {
          } else if (OB_FAIL(GCTX.sql_engine_->get_dep_info_queue().push(task))) {
            LOG_WARN("push task failed", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = GCTX.sql_engine_->get_dep_info_queue().erase_view_id_from_set(new_view_schema.get_table_id()))) {
            LOG_WARN("failed to erase obj id", K(tmp_ret), K(ret));
          }
           if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
            LOG_TRACE("async queue is full");
          }
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::check_sys_view_changed(const share::schema::ObTableSchema &old_view_schema,
                                       const share::schema::ObTableSchema &new_view_schema,
                                       bool &changed)
{
  int ret = OB_SUCCESS;
  changed = false;
  if (old_view_schema.get_column_count() != new_view_schema.get_column_count()) {
    changed = true;
    LOG_TRACE("sys view changed, need recompile task", K(old_view_schema.get_tenant_id()),
                  K(old_view_schema.get_table_id()), K(old_view_schema.get_column_count()),
                  K(new_view_schema.get_column_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !changed && i < old_view_schema.get_column_count(); ++i) {
      const ObColumnSchemaV2 *old_col = old_view_schema.get_column_schema_by_idx(i);
      const ObColumnSchemaV2 *new_col = new_view_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(old_col) || OB_ISNULL(new_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column", K(ret), K(old_view_schema.get_table_id()), K(i), KP(old_col), KP(new_col));
      } else if (0 != old_col->get_column_name_str().case_compare(new_col->get_column_name_str())) {
        changed = true;
        LOG_TRACE("sys view changed, need recompile task", K(old_view_schema.get_tenant_id()),
                  K(old_view_schema.get_table_id()), K(i),
                  K(old_col->get_column_name_str()), K(new_col->get_column_name_str()));
      } else if (old_col->get_data_type() != new_col->get_data_type()
                 || old_col->get_data_length() != new_col->get_data_length()
                 || (ob_is_accurate_numeric_type(old_col->get_data_type())
                     && old_col->get_data_precision() != new_col->get_data_precision())
                 || (ob_is_accurate_numeric_type(old_col->get_data_type())
                     && old_col->get_data_scale() != new_col->get_data_scale())) {
        changed = true;
        LOG_TRACE("sys view changed, need recompile task", K(old_view_schema.get_tenant_id()),
                  K(old_view_schema.get_table_id()), K(i),
                  K(old_col->get_data_type()), K(new_col->get_data_type()),
                  K(old_col->get_data_length()), K(new_col->get_data_length()),
                  K(old_col->get_data_precision()), K(new_col->get_data_precision()),
                  K(old_col->get_data_scale()), K(new_col->get_data_scale()));
      }
    }
  }
  if (OB_SUCC(ret) && !changed) {
    if (OB_FAIL(GCTX.sql_engine_->get_dep_info_queue()
                .add_consistent_sys_view_id_to_set(old_view_schema.get_tenant_id(),
                                                   old_view_schema.get_table_id()))) {
      LOG_WARN("failed to add sys view", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::find_synonym_ref_obj(const ObString &database_name,
                                     const ObString &object_name,
                                     const uint64_t tenant_id,
                                     bool &exist,
                                     uint64_t &object_id,
                                     ObObjectType &obj_type,
                                     uint64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  exist = false;
  uint64_t database_id = OB_INVALID_ID;
  const ObTableSchema *table_schema = nullptr;
  const ObSimpleSynonymSchema *synonym_schema = nullptr;
  const share::schema::ObPackageInfo *package_info = nullptr;
  const ObUDTTypeInfo *udt_info = nullptr;
  const ObRoutineInfo *routine_info = nullptr;
  const ObSequenceSchema *seq_schema = nullptr;
  // table/view/synomyon/package/function/udt/sequence/procedure
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(guard.get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(database_name));
  } else if (OB_FAIL(find_synonym_ref_obj(database_id, object_name, tenant_id, exist, object_id, obj_type, schema_version))) {
    LOG_WARN("failed to find synonym ref obj", K(ret));
  }
  return ret;
}

bool ObSQLUtils::check_need_disconnect_parser_err(const int ret_code)
{
  bool bret = true;
  if (OB_LIKELY(OB_ERR_PARSE_SQL == ret_code
                || OB_ERR_PARSER_SYNTAX == ret_code
                || OB_ERR_EMPTY_QUERY == ret_code
                || OB_SIZE_OVERFLOW == ret_code
                || OB_ERR_ILLEGAL_NAME == ret_code
                || OB_ERR_STR_LITERAL_TOO_LONG == ret_code
                || OB_ERR_NOT_VALID_ROUTINE_NAME == ret_code
                || OB_ERR_CONSTRUCT_MUST_RETURN_SELF == ret_code
                || OB_ERR_ONLY_FUNC_CAN_PIPELINED == ret_code
                || OB_ERR_NO_ATTR_FOUND == ret_code
                || OB_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK == ret_code
                || OB_ERR_NON_INT_LITERAL == ret_code
                || OB_ERR_PARSER_INIT == ret_code
                || OB_NOT_SUPPORTED == ret_code)) {
    bret = false;
  }
  return bret;
}

int ObSQLUtils::find_synonym_ref_obj(const uint64_t database_id,
                                     const ObString &object_name,
                                     const uint64_t tenant_id,
                                     bool &exist,
                                     uint64_t &object_id,
                                     ObObjectType &obj_type,
                                     uint64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  exist = false;
  const ObTableSchema *table_schema = nullptr;
  const ObSimpleSynonymSchema *synonym_schema = nullptr;
  const share::schema::ObPackageInfo *package_info = nullptr;
  const ObUDTTypeInfo *udt_info = nullptr;
  const ObRoutineInfo *routine_info = nullptr;
  const ObSequenceSchema *seq_schema = nullptr;
  // table/view/synomyon/package/function/udt/sequence/procedure
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_INVALID_ID == database_id) {
    // do nothing
  } else if (OB_FAIL(guard.get_table_schema(tenant_id, database_id, object_name, false, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (nullptr != table_schema) {
    exist = true;
    object_id = table_schema->get_table_id();
    obj_type = table_schema->is_view_table() ? ObObjectType::VIEW : ObObjectType::TABLE;
    schema_version = table_schema->get_schema_version();
  } else if (OB_FAIL(guard.get_synonym_info(tenant_id, database_id, object_name, synonym_schema))) {
    LOG_WARN("failed to get synonym schema", K(ret));
  } else if (nullptr != synonym_schema) {
    exist = true;
    object_id = synonym_schema->get_synonym_id();
    obj_type = ObObjectType::SYNONYM;
    schema_version = synonym_schema->get_schema_version();
  } else if (OB_FAIL(guard.get_standalone_function_info(tenant_id, database_id, object_name, routine_info))) {
    LOG_WARN("failed to get udf info", K(ret));
  } else if (nullptr != routine_info) {
    exist = true;
    object_id = routine_info->get_object_id();
    obj_type = ObObjectType::FUNCTION;
    schema_version = routine_info->get_schema_version();
  } else if (OB_FAIL(guard.get_standalone_procedure_info(tenant_id, database_id, object_name, routine_info))) {
    LOG_WARN("failed to get procedure info", K(ret));
  } else if (nullptr != routine_info) {
    exist = true;
    object_id = routine_info->get_object_id();
    obj_type = ObObjectType::PROCEDURE;
    schema_version = routine_info->get_schema_version();
  } else if (OB_FAIL(guard.get_udt_info(tenant_id, database_id, OB_INVALID_ID,
                                        object_name, udt_info))) {
    LOG_WARN("failed to get udt info", K(ret));
  } else if (nullptr != udt_info) {
    exist = true;
    object_id = udt_info->get_type_id();
    obj_type = ObObjectType::TYPE;
    schema_version = udt_info->get_schema_version();
  } else if (OB_FAIL(guard.get_sequence_schema_with_name(tenant_id, database_id, object_name, seq_schema))) {
    LOG_WARN("failed to get sequence schema", K(ret));
  } else if (nullptr != seq_schema) {
    exist = true;
    object_id = seq_schema->get_sequence_id();
    obj_type = ObObjectType::SEQUENCE;
    schema_version = seq_schema->get_schema_version();
  } else if (OB_FAIL(guard.get_package_info(tenant_id, database_id, object_name,
                                            share::schema::ObPackageType::PACKAGE_TYPE,
                                            COMPATIBLE_ORACLE_MODE,
                                            package_info))) {
    LOG_WARN("failed to get package info", K(ret));
  } else if (nullptr != package_info) {
    exist = true;
    object_id = package_info->get_package_id();
    obj_type = ObObjectType::PACKAGE;
    schema_version = package_info->get_schema_version();
  } else if (OB_FAIL(guard.get_package_info(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, object_name,
                                            share::schema::ObPackageType::PACKAGE_TYPE,
                                            COMPATIBLE_ORACLE_MODE,
                                            package_info))) {
    LOG_WARN("failed to get package info", K(ret));
  } else if (nullptr != package_info) {
    exist = true;
    object_id = package_info->get_package_id();
    obj_type = ObObjectType::PACKAGE;
    schema_version = package_info->get_schema_version();
  }
  return ret;
}

int ObSQLUtils::print_identifier_require_quotes(ObCollationType collation_type,
                                                const ObString &ident,
                                                bool &require)
{
  int ret = OB_SUCCESS;
  bool pure_digit = true;
  const ObCharsetInfo *info = ObCharset::get_charset(collation_type);
  require = false;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret));
  } else if (ident.length() > 0 && ident[0] == '$') {
    require = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !require && i < ident.length(); i++) {
    char ch = ident[i];
    uint length = ob_mbcharlen(info, ch);
    if (length == 0) {
      require = true;
    } else if (length == 1 && !oceanbase::sql::MYSQL_IDENTIFIER_FALGS[static_cast<uint8_t>(ch)]) {
      require = true;
    } else if (length == 1 && (ch < '0' || ch > '9')) {
      pure_digit = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (pure_digit) {
    require = true;
  } else if (require) {
    //do nothing
  } else if (-1 != mysql_sql_reserved_keyword_lookup(ident.ptr())) {
    require = true;
  }
  return ret;
}

bool ObSQLUtils::check_json_expr(ObItemType type)
{
  bool res = false;
  switch(type) {
    case T_FUN_SYS_JSON_ARRAY:
    case T_FUN_SYS_JSON_OBJECT:
    case T_FUN_ORA_JSON_ARRAYAGG:
    case T_FUN_ORA_JSON_OBJECTAGG:
    case T_FUN_SYS_JSON_QUERY:
    case T_FUN_SYS_JSON_MERGE_PATCH: {
      res = true;
      break;
    }
    default : {
      break;
    }
  }
  return res;
}

int ObSQLUtils::compatibility_check_for_mysql_role_and_column_priv(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
  OV ((MOCK_DATA_VERSION_4_2_3_0 <= data_version && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0 , OB_NOT_SUPPORTED, data_version);
  return ret;
}

bool ObSQLUtils::is_data_version_ge_422_or_431(uint64_t data_version)
{
  return ((DATA_VERSION_4_2_2_0 <= data_version && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0);
}

bool ObSQLUtils::is_data_version_ge_423_or_431(uint64_t data_version)
{
  return ((MOCK_DATA_VERSION_4_2_3_0 <= data_version && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0);
}

bool ObSQLUtils::is_data_version_ge_423_or_432(uint64_t data_version)
{
  return ((MOCK_DATA_VERSION_4_2_3_0 <= data_version && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_2_0);
}