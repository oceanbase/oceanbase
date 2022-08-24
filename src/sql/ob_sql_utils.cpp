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
#include "share/ob_i_data_access_service.h"
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_table_stat.h"
#include "storage/ob_partition_service.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/resolver/expr/ob_raw_expr_precalc_analyzer.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/ob_insert_stmt_printer.h"
#include "sql/ob_update_stmt_printer.h"
#include "sql/ob_delete_stmt_printer.h"
#include "sql/ob_merge_stmt_printer.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/optimizer/ob_route_policy.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "common/ob_smart_call.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/resolver/expr/ob_raw_expr.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

bool ObSQLUtils::is_trans_commit_need_disconnect_err(int err)
{
  bool bool_ret = true;
  if (OB_SUCCESS == err || OB_TRANS_KILLED == err || OB_TRANS_CTX_NOT_EXIST == err || OB_TRANS_TIMEOUT == err ||
      OB_TRANS_STMT_TIMEOUT == err || OB_TRANS_NEED_ROLLBACK == err || OB_TRANS_ROLLBACKED == err ||
      OB_NOT_MASTER == err || OB_TRANS_IS_EXITING == err) {
    bool_ret = false;
  }
  return bool_ret;
}

void ObSQLUtils::check_if_need_disconnect_after_end_trans(
    const int end_trans_err, const bool is_rollback, const bool is_explicit, bool& is_need_disconnect)
{
  is_need_disconnect = false;
  if (is_rollback) {
    // rollback
    if (OB_UNLIKELY(OB_SUCCESS != end_trans_err && is_explicit)) {
      is_need_disconnect = true;
      LOG_WARN("fail to rollback explicitly, disconnect", K(end_trans_err));
    } else {
      is_need_disconnect = false;
    }
  } else {
    // commit
    if (OB_UNLIKELY(ObSQLUtils::is_trans_commit_need_disconnect_err(end_trans_err))) {
      is_need_disconnect = true;
      LOG_WARN("fail to commit, and error number is unexpected, disconnect", K(end_trans_err), K(lbt()));
    } else {
      is_need_disconnect = false;
    }
  }
}

int ObSQLUtils::md5(const ObString& stmt, char* sql_id, int32_t len)
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
    unsigned char* res = MD5(reinterpret_cast<const unsigned char*>(stmt.ptr()),
        stmt.length(),
        reinterpret_cast<unsigned char*>(md5_sum_buf));
    if (OB_ISNULL(res)) {
      // MD5() in openssl always return an pointer not NULL, so we need not check return value.
      // Even so, we HAVE TO check it here. You know it.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("md5 res null pointer", K(ret), K(res));
    } else if (OB_FAIL(to_hex_cstr(md5_sum_buf, md5_sum_len, sql_id, len))) {
      LOG_WARN("transform to hex str error", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObSQLUtils::calc_partition_ids(const ObIArray<ObNewRange*>& ranges, const ObSqlExpression& partition_func,
    const uint64_t part_num, ObIArray<uint64_t>& partition_ids)
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
    ObNewRange* range = ranges.at(i);
    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_EXE_LOG(WARN, "invalid argument", K(ret));
    } else {
      calc_row.reset();
      result.reset();
      int64_t result_par_id = 0;
      // assuming all ranges are single-value
      calc_row.cells_ = const_cast<ObObj*>(range->start_key_.get_obj_ptr());
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

int ObSQLUtils::get_phy_plan_type(
    ObIArray<share::ObPartitionLocation>& part_location_set, const ObAddr& my_address, ObPhyPlanType& plan_type)
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

int ObSQLUtils::has_local_leader_replica(
    const ObPartitionLocationIArray& part_array, const ObAddr& addr, ObPartitionKey& key, bool& has_local)
{
  int ret = OB_SUCCESS;
  // get first local partition
  has_local = false;
  // init with firt partition key
  if (part_array.count() > 0) {
    if (OB_FAIL(part_array.at(0).get_partition_key(key))) {
      LOG_WARN("Get partition key error", "partition", part_array.at(0), K(ret));
    }
  }
  for (int64_t i = 0; !has_local && OB_SUCCESS == ret && i < part_array.count(); i++) {
    ObReplicaLocation replica_leader;
    if (OB_FAIL(part_array.at(i).get_strong_leader(replica_leader))) {
      LOG_WARN("Get leader error", K(ret));
    } else if (addr == replica_leader.server_) {  // local partition
      if (OB_FAIL(part_array.at(i).get_partition_key(key))) {
        LOG_WARN("Get partition key error", "partition", part_array.at(i), K(ret));
      } else {
        has_local = true;
      }
    }
  }
  return ret;
}

int ObSQLUtils::has_local_replica(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
    const common::ObAddr& addr, common::ObPartitionKey& key, bool& has_local)
{
  int ret = OB_SUCCESS;
  has_local = false;
  // init with firt partition key
  if (part_loc_info_array.count() > 0) {
    if (OB_FAIL(part_loc_info_array.at(0).get_partition_location().get_partition_key(key))) {
      LOG_WARN("Get partition key error", "partition info", part_loc_info_array.at(0), K(ret));
    }
  }
  for (int64_t i = 0; !has_local && OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
    const ObOptPartLoc& part_loc = part_loc_info_array.at(i).get_partition_location();
    const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_array = part_loc.get_replica_locations();
    for (int64_t j = 0; !has_local && OB_SUCC(ret) && j < replica_loc_array.count(); ++j) {
      const ObReplicaLocation& replica_location = replica_loc_array.at(j);
      if (addr == replica_location.server_) {
        if (OB_FAIL(part_loc.get_partition_key(key))) {
          LOG_WARN("Get partition key error", K(part_loc), K(ret));
        } else {
          has_local = true;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_TRACE("[FIND LOCAL REPLICA] ret is failed", K(ret), K(key), K(addr), K(has_local));
  } else if (has_local) {
    LOG_TRACE("[FIND LOCAL REPLICA] has local replica", K(key), K(addr));
  } else {
    LOG_TRACE("[FIND LOCAL REPLICA] no local replica", K(addr));
  }
  return ret;
}

int ObSQLUtils::find_all_local_replica(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
    const common::ObAddr& addr, common::ObIArray<common::ObPartitionKey>& keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
    const ObOptPartLoc& part_loc = part_loc_info_array.at(i).get_partition_location();
    const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_array = part_loc.get_replica_locations();
    ObPartitionKey temp_key;
    bool has_local = false;
    for (int64_t j = 0; OB_SUCC(ret) && !has_local && j < replica_loc_array.count(); j++) {
      const ObReplicaLocation& replica_location = replica_loc_array.at(j);
      if (addr == replica_location.server_) {
        if (OB_FAIL(part_loc.get_partition_key(temp_key))) {
          LOG_WARN("Get partition key error", K(part_loc), K(ret));
        } else if (OB_FAIL(keys.push_back(temp_key))) {
          LOG_WARN("failed to push back temp key", K(ret));
        } else {
          has_local = true;
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::replace_questionmarks(ParseNode* tree, const ParamStore& params)
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
      const ObObj& param = params.at(tree->value_);
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

int ObSQLUtils::calc_const_or_calculable_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session,
    const ObRawExpr* raw_expr, ObObj& result, const ParamStore* params, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  if (OB_ISNULL(raw_expr) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(params), K(ret));
  } else if (raw_expr->is_const_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", K(ret));
    } else { /*do nothing*/
    }
  } else if (raw_expr->has_flag(IS_CALCULABLE_EXPR)) {
    if (OB_FAIL(calc_calculable_expr(stmt_type, session, raw_expr, result, &allocator, *params))) {
      SQL_LOG(WARN, "Get calculable expr value without addr to parition id error", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Expr should be const_expr or calculable_expr", K(*raw_expr), K(ret));
  }
  return ret;
}

int ObSQLUtils::calc_simple_expr_without_row(const stmt::StmtType stmt_type, ObSQLSessionInfo* session,
    const ObRawExpr* raw_expr, ObObj& result, const ParamStore* params, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  if (OB_ISNULL(raw_expr) || OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(params), K(ret));
  } else if (raw_expr->is_const_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", KPC(raw_expr), K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(calc_const_expr(stmt_type, session, *raw_expr, result, allocator, *params))) {
    SQL_LOG(WARN, "Get const_expr value error", KPC(raw_expr), K(ret));
  }
  return ret;
}
int ObSQLUtils::calc_raw_expr_without_row(ObExecContext& exec_ctx, const ObRawExpr* raw_expr, ObObj& result,
    const ParamStore* params, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Input arguments error", K(raw_expr), K(ret));
  } else if (raw_expr->is_const_expr()) {
    bool need_check = false;
    if (OB_FAIL(calc_const_expr(raw_expr, params, result, need_check))) {
      SQL_LOG(WARN, "failed to calc const expr", K(ret));
    } else { /*do nothing*/
    }
  } else {
    ParamStore empty_params;
    if (OB_FAIL(calc_const_expr(
            stmt::T_NONE, exec_ctx, raw_expr, result, &allocator, NULL == params ? empty_params : *params))) {
      SQL_LOG(WARN, "Get calculable expr value without addr to parition id error", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSQLUtils::calc_sql_expression_without_row(ObExecContext& exec_ctx, const ObISqlExpression& expr, ObObj& result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is NULL", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObExprCtx expr_ctx;
    ObNewRow empty_row;
    exec_ctx.get_physical_plan_ctx()->set_cur_time(ObTimeUtility::current_time());  // set the current time
    if (OB_FAIL(wrap_expr_ctx(stmt::T_NONE, exec_ctx, exec_ctx.get_allocator(), expr_ctx))) {
      LOG_WARN("Failed to wrap expr ctx", K(ret));
    } else if (OB_FAIL(expr.calc(expr_ctx, empty_row, result))) {
      LOG_WARN("failed to calc expression", K(ret), K(expr));
    }
  }
  return ret;
}

int ObSQLUtils::calc_const_expr(
    const ObRawExpr* expr, const ParamStore* params, common::ObObj& result, bool& need_check)
{
  int ret = OB_SUCCESS;
  const ObConstRawExpr* const_expr = NULL;
  need_check = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ret));
  } else if (OB_UNLIKELY(!expr->is_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not const expr", K(expr->get_expr_type()), K(ret));
  } else if (FALSE_IT(const_expr = static_cast<const ObConstRawExpr*>(expr))) {
    // do nothing
  } else if (T_QUESTIONMARK == const_expr->get_expr_type()) {
    if (OB_ISNULL(params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(params), K(ret));
    } else if (OB_FAIL(get_param_value(const_expr->get_value(), *params, result, need_check))) {
      LOG_WARN("get param value error", K(ret));
    } else { /*do nothing*/
    }
  } else {
    need_check = true;
    result = const_expr->get_value();
  }
  return ret;
}

int ObSQLUtils::get_param_value(
    const common::ObObj& param, const ParamStore& params_array, ObObjParam& result, bool& need_check)
{
  return get_param_value<ObObjParam>(param, params_array, result, need_check);
}

int ObSQLUtils::get_param_value(
    const common::ObObj& param, const ParamStore& params_array, common::ObObj& result, bool& need_check)
{
  return get_param_value<ObObj>(param, params_array, result, need_check);
}

template <class T>
int ObSQLUtils::get_param_value(const ObObj& param, const ParamStore& params_array, T& result, bool& need_check)
{
  int ret = OB_SUCCESS;
  int64_t param_idx = -1;
  need_check = false;
  if (param.is_unknown()) {
    if (OB_FAIL(param.get_unknown(param_idx))) {
      SQL_LOG(WARN, "get question mark value failed", K(param), K(ret));
    } else if (param_idx < 0 || param_idx >= params_array.count()) {
      ret = OB_ERR_ILLEGAL_INDEX;
      SQL_LOG(WARN, "Wrong index of question mark position", K(ret), K(param_idx));
    } else {
      need_check = params_array.at(param_idx).need_to_check_bool_value();
      result = params_array.at(param_idx);
      if (result.is_nop_value()) {
        ret = OB_ERR_NOP_VALUE;
      }
    }
  }
  return ret;
}

int ObSQLUtils::calc_calculable_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session, const ObRawExpr* expr,
    ObObj& result, ObIAllocator* allocator, const ParamStore& params_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Invalid arguments", K(expr), K(allocator));
  } else if (!expr->has_flag(IS_CALCULABLE_EXPR)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "expr should be calculabe expr", K(*expr), K(ret));
  } else if (OB_FAIL(calc_const_expr(stmt_type, session, *expr, result, *allocator, params_array))) {
    SQL_LOG(WARN, "failed to calc const expr", K(*expr), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSQLUtils::calc_const_expr(const stmt::StmtType stmt_type, ObExecContext& exec_ctx, const ObRawExpr* expr,
    ObObj& result, ObIAllocator* allocator, const ParamStore& params_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "Invalid arguments", K(expr), K(allocator));
  } else if (OB_FAIL(calc_const_expr(
                 stmt_type, exec_ctx.get_my_session(), *expr, result, *allocator, params_array, &exec_ctx))) {
    SQL_LOG(WARN, "failed to calc const expr", K(*expr), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSQLUtils::calc_const_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session, const ObRawExpr& expr,
    ObObj& result, ObIAllocator& allocator, const ParamStore& params_array, ObExecContext* exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null session", K(ret));
  } else if (session->use_static_typing_engine()) {
    if (OB_FAIL(calc_const_expr(session, &expr, params_array, allocator, result))) {
      LOG_WARN("failed to calc const expr", K(ret));
    }
  } else {
    ObPhysicalPlan phy_plan;
    ObPhysicalPlanCtx phy_plan_ctx(allocator);
    ObSqlExpression sql_expr(allocator, 0);
    SMART_VAR(ObExecContext, exec_ctx_dummy)
    {
      ObExecContext* exec_ctx_p = exec_ctx;
      ObExprCtx expr_ctx;
      RowDesc row_desc;
      ObNewRow empty_row;
      ObExprGeneratorImpl expr_generator(0, 0, NULL, row_desc);
      if (OB_ISNULL(exec_ctx_p)) {
        exec_ctx_dummy.set_my_session(session);
        exec_ctx_p = &exec_ctx_dummy;
      }
      phy_plan_ctx.set_cur_time(ObTimeUtility::current_time(), *session);

      if (OB_FAIL(wrap_expr_ctx(stmt_type, *exec_ctx_p, allocator, expr_ctx))) {
        LOG_WARN("fail to wrap expr ctx", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < params_array.count(); i++) {
        if (OB_FAIL(phy_plan_ctx.get_param_store_for_update().push_back(params_array.at(i)))) {
          SQL_LOG(WARN, "Add param to param_store failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(expr_generator.generate(const_cast<ObRawExpr&>(expr), sql_expr))) {
          SQL_LOG(WARN, "Generate post_expr error", K(ret));
        } else {
          phy_plan.set_regexp_op_count(expr_generator.get_cur_regexp_op_count());
          phy_plan.set_like_op_count(expr_generator.get_cur_like_op_count());
          phy_plan_ctx.set_phy_plan(&phy_plan);
          expr_ctx.phy_plan_ctx_ = &phy_plan_ctx;
          if (OB_FAIL(sql_expr.calc(expr_ctx, empty_row, result))) {
            SQL_LOG(WARN, "Fail to calc expression", K(sql_expr), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::calc_const_expr(ObSQLSessionInfo* session, const ObRawExpr* expr, const ParamStore& params,
    ObIAllocator& allocator, common::ObObj& result)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(NULL != session);
  lib::ContextParam param;
  param.set_mem_attr(session->get_effective_tenant_id(), ObModIds::OB_SQL_EXECUTOR, ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObIAllocator& tmp_allocator = CURRENT_CONTEXT->get_arena_allocator();
    ObPhysicalPlanCtx phy_plan_ctx(tmp_allocator);
    for (int i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      if (OB_FAIL(phy_plan_ctx.get_param_store_for_update().push_back(params.at(i)))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }  // end for
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(phy_plan_ctx.init_datum_param_store())) {
      LOG_WARN("failed to init datum param store", K(ret));
    } else {
      ObExecContext exec_ctx(tmp_allocator);
      exec_ctx.set_my_session(session);
      exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
      void* frame_buf = NULL;
      ObPreCalcExprFrameInfo* pre_calc_frame = NULL;
      ObStaticEngineExprCG expr_cg(tmp_allocator, &phy_plan_ctx.get_datum_param_store());
      ObRawExpr* copied_expr = NULL;
      ObRawExprFactory expr_factory(tmp_allocator);
      int org_obj_cnt = phy_plan_ctx.get_param_store().count();
      if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, expr, copied_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy raw expr", K(ret));
      } else if (OB_ISNULL(copied_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), K(copied_expr));
      } else if (OB_ISNULL(frame_buf = tmp_allocator.alloc(sizeof(ObPreCalcExprFrameInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        pre_calc_frame = new (frame_buf) ObPreCalcExprFrameInfo(tmp_allocator);
        expr_cg.init_operator_cg_ctx(&exec_ctx);
        if (OB_FAIL(expr_cg.generate_calculable_expr(copied_expr, *pre_calc_frame))) {
          LOG_WARN("failed to generate calculable expr", K(ret));
          // set current time before do pre calculation
        } else if (FALSE_IT(phy_plan_ctx.set_cur_time(ObTimeUtility::current_time(), *session))) {
          // do nothing
        } else if (OB_FAIL(ObCacheObject::pre_calculation(false, *pre_calc_frame, exec_ctx))) {
          LOG_WARN("failed to pre calculate", K(ret));
        } else if (OB_UNLIKELY(org_obj_cnt + 1 != phy_plan_ctx.get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unpected param store", K(phy_plan_ctx.get_param_store()), K(org_obj_cnt));
        } else if (OB_FAIL(deep_copy_obj(allocator, phy_plan_ctx.get_param_store().at(org_obj_cnt), result))) {
          LOG_WARN("failed to deep copy obj", K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::convert_calculable_expr_to_question_mark(ObDMLStmt& stmt, ObRawExpr*& expr, ObTransformerCtx& ctx)
{
  int ret = OB_SUCCESS;
  int64_t old_idx = stmt.get_calculable_exprs().count();
  int64_t old_pre_param_size = stmt.get_pre_param_size();
  ObSEArray<ObHiddenColumnItem, 4> calculable_exprs;
  if (OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.phy_plan_) || OB_ISNULL(ctx.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(ctx.exec_ctx_), K(ctx.phy_plan_));
  } else {
    ObRawExprPrecalcAnalyzer pre_calc_analyzer(*ctx.expr_factory_, ctx.exec_ctx_->get_my_session());
    if (OB_FAIL(pre_calc_analyzer.analyze_expr_tree(expr, stmt))) {
      LOG_WARN("analyze stmt all expr failed", K(ret));
    }
  }
  // pre calculable
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(extract_calc_exprs(stmt, expr, old_idx, old_pre_param_size, calculable_exprs))) {
    LOG_WARN("failed to extract calc exprs", K(ret));
  } else if (OB_FAIL(ObSql::calc_pre_calculable_exprs(calculable_exprs, *ctx.exec_ctx_, stmt, *ctx.phy_plan_))) {
    LOG_WARN("Failed to calc_pre_calculable_exprs", K(ret));
  }
  return ret;
}

int ObSQLUtils::extract_calc_exprs(ObDMLStmt& stmt, ObRawExpr* expr, int64_t old_idx, int64_t old_pre_param_size,
    ObIArray<ObHiddenColumnItem>& calc_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> record_exprs;
  bool is_need_adjust = false;
  ObIArray<ObHiddenColumnItem>& stmt_calc_exprs = stmt.get_calculable_exprs();
  if (OB_UNLIKELY(old_idx > stmt_calc_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(old_idx), K(stmt_calc_exprs.count()));
  } else if (1 == stmt_calc_exprs.count() - old_idx) {
    if (OB_FAIL(calc_exprs.push_back(stmt_calc_exprs.at(old_idx)))) {
      LOG_WARN("Failed to push back calculable_exprs", K(ret));
    } else { /*do nothing*/
    }
  } else if (stmt_calc_exprs.count() - old_idx > 1) {
    if (OB_FAIL(recursively_extract_calc_exprs(
            stmt_calc_exprs, expr, old_idx, old_pre_param_size, record_exprs, is_need_adjust, calc_exprs))) {
      LOG_WARN("failed to recursively extract calc exprs", K(ret));
    } else if (is_need_adjust) {
      if (OB_UNLIKELY(calc_exprs.count() + old_idx != stmt_calc_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(stmt_calc_exprs.count()), K(calc_exprs.count() + old_idx));
      } else {
        for (int64_t i = 0; i < calc_exprs.count(); ++i) {
          stmt.get_calculable_exprs().at(old_idx + i) = calc_exprs.at(i);
        }
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObSQLUtils::recursively_extract_calc_exprs(ObIArray<ObHiddenColumnItem>& stmt_calc_exprs, ObRawExpr* expr,
    int64_t old_idx, int64_t old_pre_param_size, ObIArray<ObRawExpr*>& record_exprs, bool& is_need_adjust,
    ObIArray<ObHiddenColumnItem>& calc_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr* param_expr = expr->get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get unexpected null", K(ret), K(param_expr));
      } else if (ObRawExprUtils::find_expr(record_exprs, param_expr)) {
        /*do nothing*/
      } else if (T_QUESTIONMARK == param_expr->get_expr_type()) {
        int64_t idx = -1;
        ObObj& question_value = static_cast<ObConstRawExpr*>(param_expr)->get_value();
        if (OB_FAIL(question_value.get_unknown(idx))) {
          LOG_WARN("failed to get unknown", K(ret));
        } else if (-1 == idx || OB_UNLIKELY(idx - old_pre_param_size + old_idx >= stmt_calc_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid argument", K(ret), K(idx - old_pre_param_size + old_idx), K(stmt_calc_exprs.count()));
        } else if (idx < old_pre_param_size) {
          /*do nothing*/
        } else {
          ObHiddenColumnItem& hidden_col = stmt_calc_exprs.at(idx - old_pre_param_size + old_idx);
          int64_t old_calculable_exprs_size = calc_exprs.count();
          if (OB_ISNULL(hidden_col.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get unexpected null", K(ret), K(hidden_col.expr_));
          } else if (OB_FAIL(SMART_CALL(recursively_extract_calc_exprs(stmt_calc_exprs,
                         hidden_col.expr_,
                         old_idx,
                         old_pre_param_size,
                         record_exprs,
                         is_need_adjust,
                         calc_exprs)))) {
            LOG_WARN("failed to recursively extract calc exprs", K(ret));
          } else if (old_calculable_exprs_size != calc_exprs.count() ||
                     idx != calc_exprs.count() + old_pre_param_size) {
            hidden_col.hidden_idx_ = calc_exprs.count() + old_pre_param_size;
            ObObjParam val;
            val.set_unknown(hidden_col.hidden_idx_);
            val.set_param_meta();
            const ObExprResType result_type = param_expr->get_result_type();
            static_cast<ObConstRawExpr*>(param_expr)->set_value(val);
            param_expr->set_result_type(result_type);
            if (OB_FAIL(calc_exprs.push_back(hidden_col))) {
              LOG_WARN("failed to push back item", K(ret));
            } else if (OB_FAIL(record_exprs.push_back(param_expr))) {
              LOG_WARN("failed to push back item", K(ret));
            } else {
              is_need_adjust = true;
            }
          } else if (OB_FAIL(calc_exprs.push_back(hidden_col))) {
            LOG_WARN("failed to push back item", K(ret));
          } else if (OB_FAIL(record_exprs.push_back(param_expr))) {
            LOG_WARN("failed to push back item", K(ret));
          } else { /*do nothing*/
          }
        }
      } else if (param_expr->has_flag(CNT_PARAM)) {
        if (OB_FAIL(SMART_CALL(recursively_extract_calc_exprs(
                stmt_calc_exprs, param_expr, old_idx, old_pre_param_size, record_exprs, is_need_adjust, calc_exprs)))) {
          LOG_WARN("failed to recursively extract calc exprs", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSQLUtils::make_generated_expression_from_str(const common::ObString& expr_str,
    const share::schema::ObTableSchema& schema, const share::schema::ObColumnSchemaV2& gen_col,
    const common::ObIArray<share::schema::ObColDesc>& col_ids, common::ObIAllocator& allocator,
    common::ObISqlExpression*& expression)
{
  int ret = OB_SUCCESS;
  const bool make_column_expression = false;  // return ObSqlExpression
  ObSQLSessionInfo default_session;
  uint64_t tenant_id = extract_tenant_id(schema.get_table_id());
  const ObTenantSchema *tenant_schema = nullptr;
  ObSchemaGetterGuard guard;
  
  if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(default_session.init(0, 0, 0, &allocator))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant_schema", K(ret));
  } else if (OB_FAIL(default_session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
    LOG_WARN("fail to init", K(ret));
  } else if (OB_FAIL(default_session.load_all_sys_vars(guard))) {
    LOG_WARN("session load default system variable failed", K(ret));
  } else if (OB_FAIL(make_generated_expression_from_str(
                 expr_str, default_session, schema, gen_col, col_ids, allocator, expression, make_column_expression))) {
    LOG_WARN("make generated expression failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObSQLUtils::make_generated_expression_from_str(const common::ObString& expr_str, ObSQLSessionInfo& session,
    const share::schema::ObTableSchema& schema, const share::schema::ObColumnSchemaV2& gen_col,
    const common::ObIArray<share::schema::ObColDesc>& col_ids, common::ObIAllocator& allocator,
    common::ObISqlExpression*& expression, const bool make_column_expression)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr* expr = NULL;
  RowDesc row_desc;
  ObArray<ObQualifiedName> columns;
  if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_str, expr_factory, session, expr, columns))) {
    LOG_WARN("get generated column expr failed", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  } else {
    // create row_desc
    for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
      ObColumnRefRawExpr* col_ref = NULL;
      const ObColumnSchemaV2* col_schema = NULL;
      if (OB_ISNULL(col_schema = schema.get_column_schema(col_ids.at(i).col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K_(col_ids.at(i).col_id));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, col_ref))) {
        LOG_WARN("build column expr failed", K(ret));
      } else if (OB_FAIL(row_desc.add_column(col_ref))) {
        LOG_WARN("add column to row desc failed", K(ret));
      } else { /*do nothing*/
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
        ObQualifiedName& q_name = columns.at(j);
        if (!q_name.database_name_.empty() || !q_name.tbl_name_.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is not based on single table", K(q_name));
        } else if (ObCharset::case_insensitive_equal(q_name.col_name_, col_schema->get_column_name_str())) {
          if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, col_ref))) {
            LOG_WARN("replace reference column failed", K(ret));
          }
        } else { /*do nothing*/
        }
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
      ObSqlExpressionFactory sql_expression_factory(allocator);
      ObExprOperatorFactory expr_op_factory(allocator);
      ObExprGeneratorImpl expr_gen(expr_op_factory, 0, 0, NULL, row_desc);
      ObSqlExpression* sql_expr = NULL;
      ObColumnExpression* col_expr = NULL;
      if (!make_column_expression) {
        if (OB_FAIL(sql_expression_factory.alloc(sql_expr))) {
          LOG_WARN("alloc sql expression failed", K(ret));
        }
      } else {
        if (OB_FAIL(sql_expression_factory.alloc(col_expr))) {
          LOG_WARN("alloc column expression failed", K(ret));
        } else {
          sql_expr = col_expr;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(NULL == sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to alloc sql expression", K(ret));
      } else if (OB_FAIL(expr_gen.generate(*expr, *sql_expr))) {
        LOG_WARN("fail to fill sql expression", K(ret));
      } else {
        expression = sql_expr;
      }
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
    default_session = new (default_session) ObSQLSessionInfo();
    if (OB_FAIL(default_session->init(0, 0, 0, &allocator))) {
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
    ObPhysicalPlanCtx* phy_plan_ctx = static_cast<ObPhysicalPlanCtx*>(allocator.alloc(sizeof(ObPhysicalPlanCtx)));
    if (OB_ISNULL(phy_plan_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      phy_plan_ctx = new (phy_plan_ctx) ObPhysicalPlanCtx(allocator);
      expr_ctx.phy_plan_ctx_ = phy_plan_ctx;
    }
  }

  if (OB_SUCC(ret)) {
    ObPhysicalPlan* phy_plan = static_cast<ObPhysicalPlan*>(allocator.alloc(sizeof(ObPhysicalPlan)));
    if (OB_ISNULL(phy_plan)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      phy_plan = new (phy_plan) ObPhysicalPlan();
      expr_ctx.phy_plan_ctx_->set_phy_plan(phy_plan);
    }
  }

  if (OB_SUCC(ret)) {
    ObExecContext* exec_ctx = static_cast<ObExecContext*>(allocator.alloc(sizeof(ObExecContext)));
    if (OB_ISNULL(exec_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      exec_ctx = new (exec_ctx) ObExecContext();
      exec_ctx->set_my_session(default_session);
      expr_ctx.exec_ctx_ = exec_ctx;
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

void ObSQLUtils::destruct_default_expr_context(ObExprCtx& expr_ctx)
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

int ObSQLUtils::calc_sql_expression(const ObISqlExpression* expr, const share::schema::ObTableSchema& schema,
    const ObIArray<share::schema::ObColDesc>& col_ids, const ObNewRow& row, ObIAllocator& allocator,
    ObExprCtx& expr_ctx, ObObj& result)
{
  UNUSED(schema);
  UNUSED(allocator);
  UNUSED(col_ids);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Expr to calc is NULL", K(expr), K(ret));
  } else if (col_ids.count() != row.count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cells count in row is not equal to col_ids", K(col_ids), K(row.count_), K(ret));
  } else {
    ObNewRow pad_row;
    bool need_pad = false;
    /*
    if (is_pad_char_to_full_length(expr_ctx.my_session_->get_sql_mode())) {
      ObSEArray<int32_t, 16> whitespace_length;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); ++i) {
        if (col_ids.at(i).col_type_.is_fixed_len_char_type()) {
          const ObColumnSchemaV2* col_schema = NULL;
          if (OB_ISNULL(col_schema = schema.get_column_schema(col_ids.at(i).col_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column schema failed", K_(col_ids.at(i).col_id));
          } else {
            const ObAccuracy accuracy = col_schema->get_accuracy();
            int32_t cell_strlen = 0;  // byte or char length
            if (OB_FAIL(row.cells_[i].get_char_length(accuracy, cell_strlen, share::is_oracle_mode()))) {
              STORAGE_LOG(WARN, "Fail to get char length, ", K(ret));
            } else {
              need_pad = need_pad ? true : cell_strlen < col_schema->get_data_length();
              if (OB_FAIL(whitespace_length.push_back(col_schema->get_data_length() - cell_strlen))) {
                LOG_WARN("push back error", K(whitespace_length), K(ret));
              }
            }
          }
        } else {
          if (OB_FAIL(whitespace_length.push_back(0))) {
            LOG_WARN("push back error", K(whitespace_length), K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && need_pad) {
        if (row.count_ != whitespace_length.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cells count in row is not equal to pad_length", K(row), K(whitespace_length), K(ret));
        } else {
          pad_row.cells_ = static_cast<ObObj*>(allocator.alloc(sizeof(common::ObObj) * row.count_));
          if (OB_ISNULL(pad_row.cells_)) {
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no memory", K(sizeof(common::ObObj)), K(row.count_), K(ret));
          } else {
            pad_row.cells_ = new (pad_row.cells_) common::ObObj[row.count_]();
            for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
              if (whitespace_length.at(i) > 0) {
                ObString res_string;
                if (OB_FAIL(ObCharset::whitespace_padding(allocator,
                        row.cells_[i].get_collation_type(),
                        row.cells_[i].get_string(),
                        whitespace_length.at(i),
                        res_string))) {
                  LOG_WARN("whitespace_padding failed", K(ret), K(whitespace_length.at(i)));
                } else {
                  pad_row.cells_[i].set_string(row.cells_[i].get_type(), res_string);
                }
              } else {
                pad_row.cells_[i] = row.cells_[i];
              }
            }
          }
        }
      }
    }
    */

    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr->calc(expr_ctx, need_pad ? pad_row : row, result))) {
        LOG_WARN("Fail to calc value", K(ret), K(*expr), K(row), K(need_pad), K(pad_row));
      }
    }
  }
  return ret;
}

int64_t ObSQLUtils::get_usec()
{
  struct timeval time_val;
  if (0 != gettimeofday(&time_val, NULL)) {  // success: return 0, failed: return -1
    LOG_WARN("fail to get time of day");
  }
  return time_val.tv_sec * 1000000 + time_val.tv_usec;
}

int ObSQLUtils::check_and_convert_db_name(const ObCollationType cs_type, const bool perserve_lettercase, ObString& name)
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  ObString origin_name = name;
  int64_t name_len = name.length();
  const char* name_str = name.ptr();
  int32_t max_database_name_length = GET_MIN_CLUSTER_VERSION() < CLUSTER_CURRENT_VERSION
                                         ? OB_MAX_DATABASE_NAME_LENGTH - 1
                                         : OB_MAX_DATABASE_NAME_LENGTH;
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
    if (!perserve_lettercase) {
      ObCharset::casedn(CS_TYPE_UTF8MB4_BIN, last_name);
    }
    if (OB_ERR_WRONG_IDENT_NAME ==
        (ret = check_ident_name(cs_type, last_name, check_for_path_chars, max_database_name_length))) {
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

int ObSQLUtils::cvt_db_name_to_org(
    share::schema::ObSchemaGetterGuard& schema_guard, const ObSQLSessionInfo* session, common::ObString& name)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode() && session != NULL && !session->is_inner()) {
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(session->get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (case_mode == OB_ORIGIN_AND_INSENSITIVE) {
      const ObDatabaseSchema* db_schema = NULL;
      if (OB_FAIL(schema_guard.get_database_schema(session->get_effective_tenant_id(), name, db_schema))) {
        LOG_WARN("fail to get database schema", K(name), K(ret));
      } else if (db_schema != NULL) {
        name = db_schema->get_database_name();
      }
    }
  }
  return ret;
}

int ObSQLUtils::check_and_convert_table_name(const ObCollationType cs_type, const bool perserve_lettercase,
    ObString& name, const stmt::StmtType stmt_type, const bool is_index_table)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  int64_t name_len = name.length();
  const char* name_str = name.ptr();
  const int64_t max_user_table_name_length =
      share::is_oracle_mode() ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
  const int64_t max_index_name_prefix_len = 30;
  if (0 == name_len || (!is_index_table && (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN))) ||
      (is_index_table && (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN + max_index_name_prefix_len))) ||
      OB_ISNULL(name_str)) {
    ret = OB_WRONG_TABLE_NAME;
    LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int32_t>(name_len), name_str);
    LOG_WARN("incorrect table name", K(name), K(ret));
  } else {
    char origin_name[OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE * OB_MAX_CHAR_LEN + 1] = {'\0'};
    MEMCPY(origin_name, name_str, name_len);
    if (!perserve_lettercase) {
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
    }
    bool check_for_path_chars = false;
    int64_t max_ident_len = max_user_table_name_length;
    if (stmt::T_SELECT == stmt_type && is_index_table) {
      max_ident_len = OB_MAX_TABLE_NAME_LENGTH;
    }
    if (OB_ERR_WRONG_IDENT_NAME ==
        (ret = check_ident_name(CS_TYPE_UTF8MB4_GENERAL_CI, name, check_for_path_chars, max_ident_len))) {
      ret = OB_WRONG_TABLE_NAME;
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME, (int)strlen(origin_name), origin_name);
      LOG_WARN("Incorrect table name", K(origin_name), K(ret));
    } else if (OB_ERR_TOO_LONG_IDENT == ret) {
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, (int)strlen(origin_name), origin_name);
      LOG_WARN("table name is too long", K(origin_name), K(max_ident_len), K(ret));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to check ident name", K(origin_name), K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::check_index_name(const ObCollationType cs_type, ObString& name)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  int64_t name_len = name.length();
  const char* name_str = name.ptr();
  const int64_t max_user_table_name_length =
      share::is_oracle_mode() ? OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE : OB_MAX_USER_TABLE_NAME_LENGTH_MYSQL;
  if (name_len > (max_user_table_name_length * OB_MAX_CHAR_LEN)) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_len), name_str);
    LOG_WARN("index name is too long", K(name), K(ret));
  } else if (0 == name_len) {
    if (share::is_oracle_mode()) {
      ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
      LOG_WARN("index name is empty", K(ret));
    } else {
      ret = OB_WRONG_NAME_FOR_INDEX;
      LOG_WARN("index name is empty", K(ret));
      LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, static_cast<int32_t>(name_len), name_str);
    }
  } else {
    bool check_for_path_chars = false;
    if (OB_ERR_WRONG_IDENT_NAME ==
        (ret = check_ident_name(CS_TYPE_UTF8MB4_GENERAL_CI, name, check_for_path_chars, max_user_table_name_length))) {
      ret = OB_WRONG_NAME_FOR_INDEX;
      LOG_USER_ERROR(OB_WRONG_NAME_FOR_INDEX, name.length(), name.ptr());
      LOG_WARN("Incorrect index name", K(name), K(ret));
    } else if (OB_ERR_TOO_LONG_IDENT == ret) {
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, name.length(), name.ptr());
      LOG_WARN("index name is too long", K(name), K(ret));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to check ident name", K(name), K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::check_column_name(const ObCollationType cs_type, ObString& name)
{
  UNUSED(cs_type);
  int ret = OB_SUCCESS;
  bool last_char_is_space = false;
  const char* end = name.ptr() + name.length();
  const char* name_str = name.ptr();
  size_t name_len = 0;  // char semantics for MySQL mode, and byte semantics for Oracle mode
  size_t byte_length = 0;
  int is_mb_char = 0;
  while (OB_SUCCESS == ret && name_str != end) {
    last_char_is_space = ObCharset::is_space(CS_TYPE_UTF8MB4_GENERAL_CI, *name_str);
    if (ObCharset::usemb(CS_TYPE_UTF8MB4_GENERAL_CI)) {
      is_mb_char = ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end);
      if (is_mb_char) {
        byte_length = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end - name_str, 1);
        name_str += byte_length;
        if (share::is_mysql_mode()) {
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
    if (last_char_is_space) {
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

int ObSQLUtils::check_and_copy_column_alias_name(
    const ObCollationType cs_type, const bool is_auto_gen, ObIAllocator* allocator, ObString& name)
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  ObString origin_name = name;
  if (NULL == allocator) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid allocator is NULL", K(ret));
  } else if (OB_FAIL(make_field_name(
                 name.ptr(), static_cast<const int64_t>(name.length()), CS_TYPE_UTF8MB4_GENERAL_CI, allocator, name))) {
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

int ObSQLUtils::check_ident_name(
    const ObCollationType cs_type, ObString& name, const bool check_for_path_char, const int64_t max_ident_len)
{
  int ret = OB_SUCCESS;
  UNUSED(cs_type);
  bool last_char_is_space = false;
  const char* end = name.ptr() + name.length();
  const char* name_str = name.ptr();
  size_t name_len = 0;  // char semantics for MySQL mode, and byte semantics for Oracle mode
  size_t byte_length = 0;
  int is_mb_char = 0;
  while (OB_SUCCESS == ret && NULL != name_str && name_str != end) {
    last_char_is_space = ObCharset::is_space(CS_TYPE_UTF8MB4_GENERAL_CI, *name_str);
    if (ObCharset::usemb(CS_TYPE_UTF8MB4_GENERAL_CI)) {
      is_mb_char = ObCharset::is_mbchar(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end);
      if (is_mb_char) {
        byte_length = ObCharset::charpos(CS_TYPE_UTF8MB4_GENERAL_CI, name_str, end - name_str, 1);
        name_str += byte_length;
        if (share::is_mysql_mode()) {
          name_len++;
        } else {
          name_len += byte_length;
        }
        continue;
      }
    }
    if (check_for_path_char && ('/' == *name_str || '\\' == *name_str || '~' == *name_str || '.' == *name_str)) {
      ret = OB_ERR_WRONG_IDENT_NAME;
      LOG_WARN("Incorrect database name", K(name), K(ret));
    } else {
      name_str++;
      name_len++;
    }
  }
  if (OB_SUCC(ret)) {
    if (last_char_is_space) {
      ret = OB_ERR_WRONG_IDENT_NAME;
      LOG_WARN("incorrect ident name", K(name), K(ret));
    } else if (name_len > static_cast<size_t>(max_ident_len)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_WARN("ident name is too long", K(ret), K(name), K(name.length()), K(name_len));
    }
  }
  return ret;
}

bool ObSQLUtils::check_mysql50_prefix(ObString& db_name)
{
  bool ret = false;
  const char* str = db_name.ptr();
  int64_t len = db_name.length();
  if (NULL == str || len < OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH) {
    // do nothing
  } else {
    ret = !STRNCMP(str, OB_MYSQL50_TABLE_NAME_PREFIX, OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH);
  }
  return ret;
}

bool ObSQLUtils::cause_implicit_commit(ParseResult& result)
{
  bool ret = false;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    ParseNode* root = result.result_tree_->children_[0];
    ObItemType type = root->type_;
    if (T_SET_PASSWORD == type || T_DROP_USER == type || T_CREATE_USER == type || T_LOCK_USER == type ||
        T_REVOKE == type || T_REVOKE_ALL == type || T_RENAME_USER == type || T_GRANT == type ||
        T_CREATE_TABLE == type || T_ALTER_DATABASE == type || T_DROP_DATABASE == type || T_CREATE_TENANT == type ||
        T_CREATE_VIEW == type || T_DROP_TABLE == type || T_DROP_INDEX == type || T_CREATE_DATABASE == type ||
        T_MODIFY_TENANT == type || T_CREATE_INDEX == type || T_DROP_TENANT == type) {
      ret = true;
    }
  }
  return ret;
}

bool ObSQLUtils::is_end_trans_stmt(const ParseResult& result)
{
  bool bret = false;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    bret =
        (T_COMMIT == result.result_tree_->children_[0]->type_ || T_ROLLBACK == result.result_tree_->children_[0]->type_)
            ? true
            : false;
  }
  return bret;
}

bool ObSQLUtils::is_commit_stmt(const ParseResult& result)
{
  bool bret = false;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    bret = T_COMMIT == result.result_tree_->children_[0]->type_ ? true : false;
  }
  return bret;
}

bool ObSQLUtils::is_modify_tenant_stmt(ParseResult& result)
{
  ObItemType type = T_INVALID;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    type = result.result_tree_->children_[0]->type_;
  }
  return type == T_MODIFY_TENANT;
}

bool ObSQLUtils::is_mysql_ps_not_support_stmt(const ParseResult& result)
{
  bool ret = false;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    ParseNode* root = result.result_tree_->children_[0];
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

bool ObSQLUtils::is_readonly_stmt(ParseResult& result)
{
  bool ret = false;
  if (NULL != result.result_tree_ && NULL != result.result_tree_->children_ &&
      NULL != result.result_tree_->children_[0]) {
    ParseNode* root = result.result_tree_->children_[0];
    ObItemType type = root->type_;
    if (T_SELECT == type && NULL == root->children_[PARSE_SELECT_FOR_UPD]) {
      ret = true;
    } else if (T_EXPLAIN == type && NULL != root->children_[1] && root->children_[1]->type_ == T_SELECT &&
               NULL == root->children_[1]->children_[PARSE_SELECT_FOR_UPD]) {
      ret = true;
    } else if (T_SHOW_COLUMNS == type || T_SHOW_TABLES == type || T_SHOW_DATABASES == type ||
               T_SHOW_TABLE_STATUS == type || T_SHOW_SERVER_STATUS == type || T_SHOW_VARIABLES == type ||
               T_SHOW_SCHEMA == type || T_SHOW_CREATE_DATABASE == type || T_SHOW_CREATE_TABLE == type ||
               T_SHOW_CREATE_VIEW == type || T_SHOW_WARNINGS == type || T_SHOW_ERRORS == type ||
               T_SHOW_GRANTS == type || T_SHOW_CHARSET == type || T_SHOW_COLLATION == type ||
               T_SHOW_PARAMETERS == type || T_SHOW_INDEXES == type || T_SHOW_PROCESSLIST == type ||
               T_SHOW_TABLEGROUPS == type || T_USE_DATABASE == type || T_TRANSACTION == type || T_BEGIN == type ||
               T_COMMIT == type || T_ROLLBACK == type || T_VARIABLE_SET == type ||
               T_SHOW_RECYCLEBIN == type || T_SHOW_TENANT == type || T_SHOW_RESTORE_PREVIEW == type) {
      ret = true;
    }
  }
  return ret;
}

int ObSQLUtils::make_field_name(
    const char* src, const int64_t len, const ObCollationType cs_type, ObIAllocator* allocator, ObString& name)
{
  int ret = OB_SUCCESS;
  if (len < 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(len), K(allocator), K(ret));
  } else if (NULL == src || 0 == len) {
    name.assign_ptr(src, static_cast<int32_t>(len));
  } else {
    int64_t new_length = len;
    const char* new_start = src;
    while (new_length && !ObCharset::is_graph(cs_type, *new_start)) {
      new_start++;
      new_length--;
    }

    int64_t final_len = min(new_length, OB_MAX_ALIAS_NAME_LENGTH);
    if (new_length == len) {
      name.assign_ptr(src, static_cast<int32_t>(final_len));
    } else {
      char* buf = reinterpret_cast<char*>(allocator->alloc(final_len + 1));
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

int ObSQLUtils::set_compatible_cast_mode(const ObSQLSessionInfo* session, ObCastMode& cast_mode)
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

int ObSQLUtils::get_default_cast_mode(
    const stmt::StmtType& stmt_type, const ObSQLSessionInfo* session, ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  cast_mode = CM_NONE;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  } else if (is_oracle_mode() && stmt::T_EXPLAIN != stmt_type) {
    cast_mode = CM_ORACLE_MODE;
  } else if (stmt::T_SELECT == stmt_type || stmt::T_EXPLAIN == stmt_type ||
             (!is_strict_mode(session->get_sql_mode())) || session->is_ignore_stmt()) {
    cast_mode = CM_WARN_ON_FAIL;
  }

  return ret;
}

void ObSQLUtils::set_insert_update_scope(ObCastMode& cast_mode)
{
  cast_mode = cast_mode | CM_INSERT_UPDATE_SCOPE;
}

bool ObSQLUtils::is_insert_update_scope(ObCastMode& cast_mode)
{
  return (cast_mode & CM_INSERT_UPDATE_SCOPE);
}

int ObSQLUtils::get_default_cast_mode(
    const ObPhysicalPlan* plan, const ObSQLSessionInfo* session, ObCastMode& cast_mode)
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

int ObSQLUtils::get_default_cast_mode(const ObSQLSessionInfo* session, ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  cast_mode = CM_NONE;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan or session is NULL", K(ret));
  } else if (is_oracle_mode()) {
    cast_mode = CM_ORACLE_MODE;
  } else if (!is_strict_mode(session->get_sql_mode())) {
    cast_mode = CM_WARN_ON_FAIL;
  }
  return ret;
}

int ObSQLUtils::get_default_cast_mode(
    const bool is_explicit_cast, const uint32_t result_flag, const ObSQLSessionInfo* session, ObCastMode& cast_mode)
{
  int ret = OB_SUCCESS;
  cast_mode = CM_NONE;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret), KP(session));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode))) {
    LOG_WARN("ObSqlUtils::get_default_cast_mode failed", K(ret));
  } else {
    if (is_explicit_cast) {
      cast_mode |= CM_EXPLICIT_CAST;
    } else {
      cast_mode &= ~CM_EXPLICIT_CAST;
    }
    if (result_flag & OB_MYSQL_ZEROFILL_FLAG) {
      cast_mode |= CM_ZERO_FILL;
    }
    if (!is_oracle_mode() && is_strict_mode(session->get_sql_mode())) {
      cast_mode |= CM_NONE;
      cast_mode |= CM_STRICT_MODE;
    }
    cast_mode |= CM_FORMAT_NUMBER_WITH_LIMIT;
    LOG_DEBUG("in get_defalut_cast_mode",
        K(ret),
        K(is_explicit_cast),
        K(result_flag),
        K(session->get_stmt_type()),
        K(cast_mode));
  }
  return ret;
}

int ObSQLUtils::check_well_formed_str(const ObString& src_str, const ObCollationType cs_type, ObString& dst_str,
    bool& is_null, const bool is_strict_mode, const bool ret_error)
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
  } else if (OB_FAIL(
                 ObCharset::well_formed_len(cs_type, src_str.ptr(), str_len, well_formed_length, well_formed_error))) {
    LOG_WARN("fail to check well_formed_len", K(ret), K(src_str), K(cs_type));
  } else if (well_formed_length < str_len) {
    int32_t diff = static_cast<int32_t>(str_len - well_formed_length);
    diff = diff > 3 ? 3 : diff;
    char hex_buf[7] = {0};
    int64_t hex_len = 0;
    const char* charset_name = ObCharset::charset_name(cs_type);
    int64_t charset_name_len = strlen(charset_name);
    if (OB_FAIL(common::hex_print(src_str.ptr() + well_formed_length, diff, hex_buf, sizeof(hex_buf), hex_len))) {
      LOG_WARN("Failed to transform to hex cstr", K(ret), K(src_str), K(well_formed_length));
    } else if (ret_error) {
      ret = OB_ERR_INVALID_CHARACTER_STRING;
      LOG_USER_ERROR(OB_ERR_INVALID_CHARACTER_STRING, (int)charset_name_len, charset_name, (int)hex_len, hex_buf);
    } else if (is_strict_mode) {
      dst_str.reset();
      is_null = true;
    } else {
      dst_str.assign_ptr(src_str.ptr(), static_cast<int32_t>(well_formed_length));
    }
    if (OB_SUCC(ret)) {
      LOG_USER_WARN(OB_ERR_INVALID_CHARACTER_STRING,
          static_cast<int>(charset_name_len),
          charset_name,
          static_cast<int>(hex_len),
          hex_buf);
    }
  } else {
    dst_str = src_str;
  }
  return ret;
}

int ObSQLUtils::check_well_formed_str(const ObObj& src, ObObj& dest, const bool is_strict_mode, const bool ret_error)
{
  int ret = OB_SUCCESS;
  if (!src.is_varchar_or_char() && !src.is_varbinary() && !src.is_binary()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else {
    ObString src_str;
    ObString dst_str;
    bool is_null = false;
    if (OB_FAIL(src.get_varchar(src_str))) {
      LOG_WARN("fail to get varchar", K(ret), K(src));
    } else if (OB_FAIL(check_well_formed_str(
                   src_str, src.get_collation_type(), dst_str, is_null, is_strict_mode, ret_error))) {
      LOG_WARN("check_well_formed_str failed", K(ret), K(src_str), K(is_strict_mode), K(ret_error));
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

int ObSQLUtils::get_outline_key(ObIAllocator& allocator, const ObSQLSessionInfo* session, const ObString& query_sql,
    ObString& outline_key, ObMaxConcurrentParam::FixParamStore& fix_param_store, ParseMode parse_mode,
    bool& has_questionmark_in_sql)
{
  int ret = OB_SUCCESS;
  has_questionmark_in_sql = false;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, session is NULL", K(ret));
  } else {
    ObString no_param_sql;
    ParseResult parse_result;
    ObParser parser(allocator, session->get_sql_mode(), session->get_local_collation_connection());
    ObSEArray<ObPCParam*, OB_PC_SPECIAL_PARAM_COUNT> special_params;
    ObString param_sql;
    const int64_t PARAM_COUNT = 128;
    ParamStore params((ObWrapperAllocator(allocator)));
    ObSqlTraits sql_traits;
    ObSEArray<ObPCParam*, OB_PC_RAW_PARAM_COUNT> raw_params;
    SqlInfo sql_info;
    char* buf = NULL;
    int32_t pos = 0;
    const bool is_transform_outline = true;
    ParseNode* type_node = NULL;
    if (OB_FAIL(parser.parse(query_sql, parse_result))) {
      LOG_WARN("Generate syntax tree failed", "sql", query_sql, K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("parse result tree not inited", K(parse_result.result_tree_), K(ret));
    } else if (OB_ISNULL(parse_result.result_tree_->children_) ||
               OB_UNLIKELY(parse_result.result_tree_->num_child_ < 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args",
          K(ret),
          KP(parse_result.result_tree_->children_),
          "number of children",
          parse_result.result_tree_->num_child_);
    } else if (NULL == (type_node = parse_result.result_tree_->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(type_node));
    } else if (!IS_DML_STMT(type_node->type_)) {
      ret = OB_SQL_DML_ONLY;
      LOG_TRACE("statement not dml sql", K(type_node));
    } else if (OB_FAIL(ObSqlParameterization::transform_syntax_tree(allocator,
                   *session,
                   NULL,
                   0,
                   parse_result.result_tree_,
                   sql_info,
                   params,
                   NULL,
                   fix_param_store,
                   is_transform_outline))) {
      if (OB_NOT_SUPPORTED != ret) {
        SQL_PC_LOG(WARN, "fail to tranform syntax_tree", K(ret));
      }
    } else if (OB_FAIL(ObSqlParameterization::raw_fast_parameterize_sql(
                   allocator, *session, query_sql, no_param_sql, raw_params, parse_mode))) {
      LOG_WARN("fail to fast_parameterize_sql", K(ret));
    } else if (OB_FAIL(ObSqlParameterization::check_and_generate_param_info(raw_params, sql_info, special_params))) {
      if (OB_NOT_SUPPORTED == ret) {
        LOG_TRACE("fail to check and generate not params", K(ret), K(query_sql), K(no_param_sql));
      } else {
        LOG_WARN("fail to check and generate not params", K(ret), K(query_sql), K(no_param_sql));
      }
    } else if (OB_UNLIKELY(NULL == (buf = (char*)allocator.alloc(query_sql.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc buf", K(ret));
    } else if (OB_FAIL(
                   ObSqlParameterization::construct_sql(no_param_sql, special_params, buf, query_sql.length(), pos))) {
      LOG_WARN("fail to construct_sql", K(ret), K(no_param_sql), K(special_params.count()));
    } else {
      ObString constructed_sql(pos, buf);
      int64_t size = constructed_sql.get_serialize_size();
      if (0 == size) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        char* buf = NULL;
        int64_t pos_s = 0;
        if (OB_UNLIKELY(NULL == (buf = (char*)allocator.alloc(size)))) {
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

int ObSQLUtils::filter_hint_in_query_sql(
    ObIAllocator& allocator, const ObSQLSessionInfo& session, const ObString& sql, ObString& param_sql)
{
  int ret = OB_SUCCESS;
  ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
  ParseResult parse_result;
  if (OB_FAIL(parser.parse(sql, parse_result, FP_NO_PARAMERIZE_AND_FILTER_HINT_MODE))) {
    SQL_PC_LOG(WARN, "fail to parse query while filter hint", K(ret));
  } else {
    param_sql.assign(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
    parser.free_result(parse_result);
  }
  return ret;
}

int ObSQLUtils::construct_outline_sql(ObIAllocator& allocator, const ObSQLSessionInfo& session,
    const ObString& outline_content, const ObString& orig_sql, bool is_need_filter_hint, ObString& outline_sql)
{
  int ret = OB_SUCCESS;
  UNUSED(is_need_filter_hint);
  ObString filter_sql;
  ObSqlString sql_helper;
  int first_token_len = 0;
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
      LOG_WARN("failed to write string", K(first_token), K(orig_sql), K(filter_sql), K(outline_content), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSQLUtils::filter_head_space(ObString& sql)
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
                                ObObjPrintParams print_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(stmt), K(ret));
  } else if (!stmt->is_dml_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected stmt type", K(stmt->get_stmt_type()), K(stmt->get_sql_stmt()), K(ret));
  } else {
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf)
    {
      int64_t buf_len = OB_MAX_SQL_LENGTH;
      int64_t pos = 0;
      switch (stmt->get_stmt_type()) {
        case stmt::T_SELECT: {
          bool is_set_subquery = false;
          ObSelectStmtPrinter printer(
              buf, buf_len, &pos, static_cast<const ObSelectStmt*>(stmt), print_params, NULL, is_set_subquery);
          if (OB_FAIL(printer.do_print())) {
            LOG_WARN("fail to print select stmt", K(ret));
          } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), sql))) {
            LOG_WARN("fail to deep copy select stmt string", K(ret));
          } else { /*do nothing*/
          }
        } break;
        case stmt::T_INSERT: {
          ObInsertStmtPrinter printer(buf, buf_len, &pos, static_cast<const ObInsertStmt*>(stmt), print_params);
          if (OB_FAIL(printer.do_print())) {
            LOG_WARN("fail to print insert stmt", K(ret));
          } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), sql))) {
            LOG_WARN("fail to deep copy insert stmt string", K(ret));
          } else { /*do nothing*/
          }
        } break;
        case stmt::T_REPLACE: {

        } break;
        case stmt::T_DELETE: {
          ObDeleteStmtPrinter printer(buf, buf_len, &pos, static_cast<const ObDeleteStmt*>(stmt), print_params);
          if (OB_FAIL(printer.do_print())) {
            LOG_WARN("fail to print delete stmt", K(ret));
          } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), sql))) {
            LOG_WARN("fail to deep copy delete stmt string", K(ret));
          } else { /*do nothing*/
          }
        } break;
        case stmt::T_UPDATE: {
          ObUpdateStmtPrinter printer(buf, buf_len, &pos, static_cast<const ObUpdateStmt*>(stmt), print_params);
          if (OB_FAIL(printer.do_print())) {
            LOG_WARN("fail to print update stmt", K(ret));
          } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), sql))) {
            LOG_WARN("fail to deep copy update stmt string", K(ret));
          } else { /*do nothing*/
          }
        } break;
        case stmt::T_MERGE: {
          ObMergeStmtPrinter printer(buf, buf_len, &pos, static_cast<const ObMergeStmt*>(stmt), print_params);
          if (OB_FAIL(printer.do_print())) {
            LOG_WARN("failed to print merge stmt", K(ret));
          } else if (OB_FAIL(ob_write_string(allocator, ObString(pos, buf), sql))) {
            LOG_WARN("failed to deep copy merge stmt string", K(ret));
          }
        } break;
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Invalid stmt type", K(stmt->get_stmt_type()), K(stmt->get_sql_stmt()), K(ret));
        } break;
      }
    }
  }
  return ret;
}

int ObSQLUtils::wrap_expr_ctx(const stmt::StmtType& stmt_type, ObExecContext& exec_ctx, common::ObIAllocator& allocator,
    common::ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = NULL;
  const ObTimeZoneInfo* tz_info = NULL;
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

bool ObSQLUtils::is_same_type(const ObExprResType& type1, const ObExprResType& type2)
{
  return is_same_type(type1.get_obj_meta(), type2.get_obj_meta(), type1.get_accuracy(), type2.get_accuracy());
}

bool ObSQLUtils::is_same_type(
    const ObObjMeta& meta1, const ObObjMeta& meta2, const ObAccuracy& accuracy1, const ObAccuracy& accuracy2)
{
  return meta1.get_type() == meta2.get_type() && meta1.get_collation_type() == meta2.get_collation_type() &&
         meta1.get_scale() == meta2.get_scale() && accuracy1 == accuracy2;
}

const int32_t RelExprCheckerBase::FIELD_LIST_SCOPE = 1 << 1;
const int32_t RelExprCheckerBase::WHERE_SCOPE = 1 << 2;
// const int32_t RelExprCheckerBase::ON_SCOPE = 1<<3;
const int32_t RelExprCheckerBase::GROUP_SCOPE = 1 << 4;
const int32_t RelExprCheckerBase::HAVING_SCOPE = 1 << 5;
// const int32_t RelExprCheckerBase::INSERT_SCOPE = 1<<6;
// const int32_t RelExprCheckerBase::UPDATE_SCOPE = 1<<7;
// const int32_t RelExprCheckerBase::AGG_SCOPE = 1<<8;
// const int32_t RelExprCheckerBase::VARIABLE_SCOPE = 1<<9;
// const int32_t RelExprCheckerBase::WHEN_SCOPE = 1<<10;
const int32_t RelExprCheckerBase::ORDER_SCOPE = 1 << 11;
// const int32_t RelExprCheckerBase::EXPIRE_SCOPE = 1<<12;
// const int32_t RelExprCheckerBase::PARTITION_SCOPE = 1<<13;
const int32_t RelExprCheckerBase::FROM_SCOPE = 1 << 14;
const int32_t RelExprCheckerBase::LIMIT_SCOPE = 1 << 15;
// const int32_t RelExprCheckerBase::PARTITION_RANGE_SCOPE = 1<<16;
// const int32_t RelExprCheckerBase::INTO_SCOPE = 1<<17;
const int32_t RelExprCheckerBase::START_WITH_SCOPE = 1 << 18;
const int32_t RelExprCheckerBase::CONNECT_BY_SCOPE = 1 << 19;
const int32_t RelExprCheckerBase::JOIN_CONDITION_SCOPE = 1 << 20;
const int32_t RelExprCheckerBase::EXTRA_OUTPUT_SCOPE = 1 << 21;

int RelExprCheckerBase::init(int64_t bucket_num)
{
  return duplicated_checker_.create(bucket_num);
}

int RelExprCheckerBase::add_exprs(common::ObIArray<ObRawExpr*>& exprs)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(add_expr(exprs.at(i)))) {
      LOG_WARN("add expression to relation expr checker failed", K(ret));
    }
  }
  return ret;
}

int RelExprChecker::add_expr(ObRawExpr*& expr)
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

FastRelExprChecker::FastRelExprChecker(common::ObIArray<ObRawExpr*>& rel_array)
    : RelExprCheckerBase(), rel_array_(rel_array), init_size_(rel_array.count())
{}

FastRelExprChecker::FastRelExprChecker(common::ObIArray<ObRawExpr*>& rel_array, int32_t ignore_scope)
    : RelExprCheckerBase(ignore_scope), rel_array_(rel_array), init_size_(rel_array.count())
{}

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

int FastRelExprChecker::add_expr(ObRawExpr*& expr)
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

int RelExprPointerChecker::add_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  int flag = 0;
  uint64_t expr_id = UINT64_MAX;
  if (OB_HASH_NOT_EXIST == (flag = expr_id_map_.get_refactored(reinterpret_cast<uint64_t>(expr), expr_id))) {
    ObRawExprPointer pointer;
    if (OB_FAIL(rel_array_.push_back(pointer))) {
      LOG_WARN("failed to push back new array", K(ret));
    } else {
      expr_id = rel_array_.count() - 1;
    }
  } else if (OB_HASH_EXIST == flag) {
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

int AllExprPointerCollector::add_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rel_array_.push_back(&expr))) {
    LOG_WARN("push expr to relation array failed", K(ret));
  }

  return ret;
}

ObSqlTraits::ObSqlTraits()
    : is_readonly_stmt_(false),
      is_modify_tenant_stmt_(false),
      is_cause_implicit_commit_(false),
      is_commit_stmt_(false),
      stmt_type_(T_INVALID)
{
  sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
}

// used for C module
bool check_stack_overflow_c()
{
  bool is_overflow = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(oceanbase::common::check_stack_overflow(is_overflow))) {
    LOG_ERROR("fail to check stack overflow", K(ret));
  }
  return is_overflow;
}

int ObSQLUtils::get_partition_service(ObTaskExecutorCtx& executor_ctx, int64_t table_id, ObIDataAccessService*& das)
{
  int ret = OB_SUCCESS;
  if (is_virtual_table(table_id)) {
    das = executor_ctx.get_vt_partition_service();
  } else {
    das = static_cast<ObIDataAccessService*>(executor_ctx.get_partition_service());
  }
  if (OB_ISNULL(das)) {

    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get data access service", K(ret), K(table_id));
  }
  LOG_TRACE("get data access service", K(table_id));
  return ret;
}

/*int ObSQLUtils::calculate_phy_table_location(ObExecContext &exec_ctx,
                                            ObPartMgr *part_mgr,
                                            const ObIArray<ObObjParam> &params,
                                            ObIPartitionLocationCache &location_cache,
                                            const ObTimeZoneInfo *tz_info,
                                            ObTableLocation &table_location,
                                            ObPhyTableLocation &phy_location)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_location.calculate_partition_locations(exec_ctx,
                                                           part_mgr,
                                                           params,
                                                           location_cache,
                                                           phy_location.get_partition_location_list(),
                                                           tz_info))) {
    LOG_WARN("Failed to calculate table location", K(ret));
  } else if (OB_FAIL(phy_location.fill_part_loc_list())) {
    LOG_WARN("Failed to fill part loc list", K(ret), K(phy_location));
  } else {
    phy_location.set_table_location_key(table_location.get_table_id(), table_location.get_ref_table_id());
  }

  return ret;
}*/

/*
 * duplicated with
 * int ObQueryRange::get_tablet_ranges(common::ObIAllocator &allocator,
 *                                     const ParamsIArray &params,
 *                                     ObQueryRangeArray &ranges,
 *                                     bool &all_single_value_ranges,
 *                                     const common::ObTimeZoneInfo *tz_info) const
 */
int ObSQLUtils::extract_pre_query_range(const ObQueryRange& pre_query_range, ObIAllocator& allocator,
    const ParamStore& param_store, ObQueryRangeArray& key_ranges, ObGetMethodArray get_method,
    const ObDataTypeCastParams& dtc_params, common::ObIArray<int64_t>* range_pos)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!pre_query_range.need_deep_copy())) {
    if (OB_FAIL(
            pre_query_range.get_tablet_ranges(allocator, param_store, key_ranges, get_method, dtc_params, range_pos))) {
      LOG_WARN("fail to get tablet ranges", K(ret));
    }
  } else {
    ObQueryRange final_query_range(allocator);
    if (OB_FAIL(final_query_range.deep_copy(pre_query_range))) {
      // MUST deep copy to make it thread safe
      LOG_WARN("fail to create final query range", K(ret), K(pre_query_range));
    } else if (OB_FAIL(final_query_range.final_extract_query_range(param_store, dtc_params))) {
      LOG_WARN("fail to final extract query range", K(ret), K(final_query_range));
    } else if (OB_FAIL(final_query_range.get_tablet_ranges(key_ranges, get_method, dtc_params))) {
      LOG_WARN("fail to get tablet ranges from query range", K(ret), K(final_query_range));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObSQLUtils::get_partition_range(ObObj* start_row_key, ObObj* end_row_key, ObObj* function_obj,
    const share::schema::ObPartitionFuncType part_type, const ObSqlExpression& part_expr, int64_t range_key_count,
    uint64_t table_id, ObExprCtx& expr_ctx, common::ObNewRange& part_range)
{
  int ret = OB_SUCCESS;
  if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS ||
      part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_LIST_COLUMNS || part_expr.is_empty()) {
    part_range.table_id_ = table_id;
    part_range.start_key_.assign(start_row_key, range_key_count);
    part_range.end_key_.assign(end_row_key, range_key_count);
    part_range.border_flag_.set_inclusive_start();
    part_range.border_flag_.set_inclusive_end();
    if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH ||
        part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH_V2) {
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
      } else { /*do nothing*/
      }
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
    } else if ((part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH ||
                   part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH_V2) &&
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

int ObSQLUtils::get_partition_range(ObObj* start_row_key, ObObj* end_row_key, ObObj* function_obj,
    const share::schema::ObPartitionFuncType part_type, const ObExpr* part_expr, int64_t range_key_count,
    uint64_t table_id, ObEvalCtx& eval_ctx, common::ObNewRange& part_range)
{
  int ret = OB_SUCCESS;
  if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_RANGE_COLUMNS ||
      part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_LIST_COLUMNS || OB_ISNULL(part_expr)) {
    part_range.table_id_ = table_id;
    part_range.start_key_.assign(start_row_key, range_key_count);
    part_range.end_key_.assign(end_row_key, range_key_count);
    part_range.border_flag_.set_inclusive_start();
    part_range.border_flag_.set_inclusive_end();
    if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH ||
        part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH_V2) {
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
      } else { /*do nothing*/
      }
    }
  } else {
    LOG_TRACE("get non empty part_expr");
    ObNewRow dummy_row;
    ObDatum* datum = NULL;
    if (OB_FAIL(part_expr->eval(eval_ctx, datum))) {
      LOG_WARN("failed to calc expr", K(ret));
    } else if (OB_FAIL(datum->to_obj(*function_obj, part_expr->obj_meta_, part_expr->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (FALSE_IT(dummy_row.cells_ = function_obj)) {
    } else if (FALSE_IT(dummy_row.count_ = 1)) {
    } else if ((part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH ||
                   part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH_V2) &&
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

int ObSQLUtils::revise_hash_part_object(common::ObObj& obj, const ObNewRow& row, const bool calc_oracle_hash,
    const share::schema::ObPartitionFuncType part_type)
{
  int ret = OB_SUCCESS;
  ObObj result;
  if (part_type == share::schema::ObPartitionFuncType::PARTITION_FUNC_TYPE_HASH) {
    if (share::is_oracle_mode()) {
      if (calc_oracle_hash) {
        ret = ObExprFuncPartOldHash::calc_value_for_oracle(row.cells_, row.count_, result);
      } else {
        result = obj;
      }
    } else {
      ret = ObExprFuncPartOldHash::calc_value_for_mysql(obj, result);
    }
  } else {
    if (share::is_oracle_mode()) {
      if (calc_oracle_hash) {
        ret = ObExprFuncPartHash::calc_value_for_oracle(row.cells_, row.count_, result);
      } else {
        result = obj;
      }
    } else {
      ret = ObExprFuncPartHash::calc_value_for_mysql(obj, result);
    }
  }
  if (OB_SUCC(ret)) {
    obj.set_int(result.get_int());
  }
  return ret;
}

int ObSQLUtils::choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
    const common::ObAddr& addr, common::ObPartitionKey& key, bool& has_local)
{
  common::ObStatManager& stat_manager = common::ObStatManager::get_instance();
  return choose_best_partition_for_estimation(part_loc_info_array, addr, key, stat_manager, has_local);
}

int ObSQLUtils::choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
    const common::ObAddr& addr, common::ObPartitionKey& key, common::ObStatManager& stat_manager, bool& has_local)
{
  int ret = OB_SUCCESS;
  has_local = false;
  ObSEArray<common::ObPartitionKey, 4> local_partition_keys;
  ObSEArray<common::ObPartitionKey, 4> all_partition_keys;
  if (OB_FAIL(ObSQLUtils::find_all_local_replica(part_loc_info_array, addr, local_partition_keys))) {
    LOG_WARN("failed to find all local replica", K(ret));
  } else {
    // try to find best local partition for estimation
    int64_t local_max_count = -1;
    if (OB_SUCC(ret) && local_partition_keys.count() > 0) {
      has_local = true;
      common::ObTableStat tstat;
      for (int64_t i = 0; OB_SUCC(ret) && i < local_partition_keys.count(); i++) {
        if (OB_FAIL(stat_manager.get_table_stat(local_partition_keys.at(i), tstat))) {
          LOG_WARN("failed to get table stat", K(ret));
        } else if (tstat.get_row_count() > local_max_count) {
          local_max_count = tstat.get_row_count();
          key = local_partition_keys.at(i);
        } else { /*do nothing*/
        }
      }
    }
    // if we do not have local partition or the largest local partition is empty, use the largest non_local partition
    // for estimation
    if (OB_SUCC(ret) && (!has_local || local_max_count == 0)) {
      int64_t all_max_count = -1;
      common::ObTableStat tstat;
      common::ObPartitionKey temp_key;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); i++) {
        if (OB_FAIL(part_loc_info_array.at(i).get_partition_location().get_partition_key(temp_key))) {
          LOG_WARN("Get partition key error", "partition info", part_loc_info_array.at(0), K(ret));
        } else if (OB_FAIL(all_partition_keys.push_back(temp_key))) {
          LOG_WARN("failed to push back partition key", K(ret));
        } else { /*do nothing*/
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < all_partition_keys.count(); i++) {
        if (OB_FAIL(stat_manager.get_table_stat(all_partition_keys.at(i), tstat))) {
          LOG_WARN("failed to get table stat", K(ret));
        } else if (tstat.get_row_count() > all_max_count) {
          all_max_count = tstat.get_row_count();
          temp_key = all_partition_keys.at(i);
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        if (has_local && all_max_count == 0) {
          /*do nothing*/
        } else {
          has_local = false;
          key = temp_key;
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
    storage::ObPartitionService* partition_service, ObStatManager& stat_manager, const ObAddr& local_addr,
    const common::ObIArray<ObAddr>& addrs_list, const bool no_use_new, EstimatedPartition& best_partition)
{
  int ret = OB_SUCCESS;
  int64_t max_row_count = -1;
  int64_t part_idx = -1;
  ObPartitionKey part_pkey;
  best_partition.reset();
  if (OB_UNLIKELY(part_loc_info_array.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition array is empty", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_info_array.count(); ++i) {
    const ObOptPartLoc& part_loc = part_loc_info_array.at(i).get_partition_location();
    ObPartitionKey key;
    common::ObTableStat tstat;
    if (OB_FAIL(part_loc.get_partition_key(key))) {
      LOG_WARN("fail to get partition key", K(ret));
    } else if (OB_FAIL(stat_manager.get_table_stat(key, tstat))) {
      // reminder: stat manager get statisics from both local storage and
      //           __all_meta_table (inner table)
      LOG_WARN("failed to get table stat", K(ret));
    } else {
      LOG_TRACE("partition info", K(i), K(key), K(tstat.get_row_count()));
    }
    if (OB_FAIL(ret) || tstat.get_row_count() <= max_row_count) {
      // do nothing
    } else {
      // 1. get partition has largest row count
      max_row_count = tstat.get_row_count();
      part_idx = i;
      part_pkey = key;
    }
  }  // for part_array end

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(-1 == part_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part idx should larger than -1", K(ret));
  } else {
    const ObOptPartLoc& part_loc = part_loc_info_array.at(part_idx).get_partition_location();
    const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_array = part_loc.get_replica_locations();
    bool found = false;
    // 2. check whether best partition can find in local
    for (int64_t k = -1; !found && k < addrs_list.count(); ++k) {
      for (int64_t j = 0; !found && j < replica_loc_array.count(); ++j) {
        const ObAddr& addr = (k == -1 ? local_addr : addrs_list.at(k));
        if (addr == replica_loc_array.at(j).server_ && 0 != replica_loc_array.at(j).property_.get_memstore_percent()) {
          found = true;
          best_partition.set(part_pkey, addr, max_row_count);
        }
      }
    }
    if (!found) {
      // best partition not find in local
      ObAddr remote_addr;
      if (no_use_new) {
        // do nothing
      } else if (OB_FAIL(choose_best_partition_replica_addr(
                     local_addr, part_loc_info_array.at(part_idx), partition_service, remote_addr))) {
        LOG_WARN("failed to get best partition replica addr", K(ret));
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        best_partition.set(part_pkey, remote_addr, max_row_count);
      }
    }
  }
  return ret;
}

int ObSQLUtils::choose_best_partition_replica_addr(const ObAddr& local_addr,
    const ObPhyPartitionLocationInfo& phy_part_loc_info, storage::ObPartitionService* partition_service,
    ObAddr& selected_addr)
{
  int ret = OB_SUCCESS;
  selected_addr.reset();

  ObServerLocality local_locality;
  ObSEArray<ObServerLocality, 32> all_server_arr;
  bool has_read_only_zone = false;  // UNUSED;
  if (OB_ISNULL(partition_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(partition_service));
  } else if (OB_FAIL(partition_service->get_server_locality_array(all_server_arr, has_read_only_zone))) {
    LOG_WARN("fail to get server locality", K(ret));
  } else if (OB_FAIL(ObRoutePolicy::get_server_locality(local_addr, all_server_arr, local_locality))) {
    LOG_WARN("fail to get local locality", K(ret));
  } else {
    bool need_continue = true;
    const ObIArray<ObRoutePolicy::CandidateReplica>& candi_replicas =
        phy_part_loc_info.get_partition_location().get_replica_locations();
    ObSEArray<ObAddr, 5> same_idc_addr;
    ObSEArray<ObAddr, 5> same_region_addr;
    ObSEArray<ObAddr, 5> other_region_addr;
    for (int64_t i = 0; need_continue && OB_SUCC(ret) && i < candi_replicas.count(); ++i) {
      ObServerLocality candi_locality;
      const ObAddr& candi_addr = candi_replicas.at(i).server_;
      if (candi_replicas.at(i).property_.get_memstore_percent() == 0) {
        // skip replica whose memstore percent is zero
      } else if (OB_FAIL(ObRoutePolicy::get_server_locality(candi_addr, all_server_arr, candi_locality))) {
        LOG_WARN("fail to get server locality", K(all_server_arr), K(candi_addr), K(ret));
      } else if (OB_UNLIKELY(!candi_locality.is_init())) {
        // not find
        // do nothing
      } else if (!candi_locality.is_active() ||
                 ObServerStatus::OB_SERVER_ACTIVE != candi_locality.get_server_status() ||
                 0 == candi_locality.get_start_service_time() || 0 != candi_locality.get_server_stop_time()) {
        // server may not serving
      } else if (local_addr == candi_addr) {
        selected_addr = candi_addr;
        need_continue = false;
      } else if (local_locality.is_init() && ObRoutePolicy::is_same_idc(local_locality, candi_locality)) {
        if (OB_FAIL(same_idc_addr.push_back(candi_addr))) {
          LOG_WARN("failed to push back same idc candidate address", K(ret));
        }
      } else if (local_locality.is_init() && ObRoutePolicy::is_same_region(local_locality, candi_locality)) {
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
    // is_strict == true is_ignore_stmt == true  then   cast_mode = CM_WARN_ON_FAIL
    // is_strict == true  is_ignore_stmt == false  then   cast_mode = CM_NONE
    // is_strict == false   is_ignore_stmt == true  then   cast_mode = CM_NO_RANGE_CHECK & CM_WARN_ON_FAIL
    // is_strict == false   is_ignore_stmt == false  then  cast_mode = CM_NO_RANGE_CHECK & CM_WARN_ON_FAIL
    ObCastMode cast_mode = is_strict_mode(expr_ctx.my_session_->get_sql_mode()) ? CM_NONE : CM_WARN_ON_FAIL;
    cast_mode = cast_mode | CM_COLUMN_CONVERT;
    if (expr_ctx.phy_plan_ctx_ != NULL && expr_ctx.phy_plan_ctx_->is_ignore_stmt()) {
      cast_mode = CM_WARN_ON_FAIL;
    }
    // do not worry too much about the efficiency here
    // gcc will construct and set column_conv_ctx_ directly
    // only one constructor call is necessary. no temporary object is created
    EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
    column_conv_ctx = cast_ctx;
  }
  return ret;
}

void ObSQLUtils::init_type_ctx(const ObSQLSessionInfo* session, ObExprTypeCtx& type_ctx)
{
  if (NULL != session) {
    ObCollationType coll_type = CS_TYPE_INVALID;
    int64_t div_precision_increment = OB_INVALID_COUNT;
    int64_t ob_max_allowed_packet;
    if (share::is_mysql_mode()) {
      if (OB_SUCCESS == (session->get_collation_connection(coll_type))) {
        type_ctx.set_coll_type(coll_type);
      }
    } else if (share::is_oracle_mode()) {
      type_ctx.set_coll_type(session->get_nls_collation());
    }

    if (OB_SUCCESS == (session->get_div_precision_increment(div_precision_increment))) {
      type_ctx.set_div_precision_increment(div_precision_increment);
    }
    if (OB_SUCCESS == (session->get_max_allowed_packet(ob_max_allowed_packet))) {
      type_ctx.set_max_allowed_packet(ob_max_allowed_packet);
    }
    CHECK_COMPATIBILITY_MODE(session);
  } else {
    LOG_WARN("Molly couldn't get compatibility mode from session, use default", K(lbt()));
  }
  type_ctx.set_session(session);
}

bool ObSQLUtils::use_px(const ObStmtHint& hint, bool dist_table /*true*/)
{
  bool use_px = false;
  if (ObUsePxHint::ENABLE == hint.use_px_ && ObStmtHint::DEFAULT_PARALLEL < hint.parallel_) {
    use_px = true;
  } else if (ObUsePxHint::ENABLE == hint.use_px_ &&
             (ObStmtHint::DEFAULT_PARALLEL == hint.parallel_ || ObStmtHint::UNSET_PARALLEL == hint.parallel_) &&
             dist_table) {
    use_px = true;
  } else {
    use_px = false;
  }
  return use_px;
}

bool ObSQLUtils::is_oracle_sys_view(const ObString& table_name)
{
  return table_name.prefix_match("ALL_") || table_name.prefix_match("USER_") || table_name.prefix_match("DBA_") ||
         table_name.prefix_match("GV$") || table_name.prefix_match("V$") ||
         0 == table_name.compare("NLS_SESSION_PARAMETERS") || 0 == table_name.compare("NLS_INSTANCE_PARAMETERS") ||
         0 == table_name.compare("NLS_DATABASE_PARAMETERS") || 0 == table_name.compare("DICTIONARY") ||
         0 == table_name.compare("DICT") || 0 == table_name.compare("ROLE_TAB_PRIVS") ||
         0 == table_name.compare("ROLE_SYS_PRIVS") || 0 == table_name.compare("ROLE_ROLE_PRIVS");
}

int ObSQLUtils::extend_checker_stmt(ObExecContext& ctx, uint64_t table_id, uint64_t ref_table_id,
    const common::ObIArray<int64_t>& part_ids, const bool is_weak)
{
  int ret = OB_SUCCESS;
  if (!part_ids.empty()) {
    if (OB_FAIL(
            ObTableLocation::append_phy_table_location(ctx, table_id, ref_table_id, is_weak, part_ids, UNORDERED))) {
      LOG_WARN("append phy table location failed", K(ret));
    }
  }
  return ret;
}

int ObSQLUtils::make_whole_range(
    ObIAllocator& allocator, const uint64_t ref_table_id, const int64_t rowkey_count, ObNewRange*& whole_range)
{
  int ret = OB_SUCCESS;
  whole_range = NULL;
  ObNewRange* temp_range = NULL;
  if (OB_ISNULL(temp_range = static_cast<ObNewRange*>(allocator.alloc(sizeof(ObNewRange))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for query range failed");
  } else if (OB_ISNULL(whole_range = new (temp_range) ObNewRange())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to new whole range");
  } else if (OB_FAIL(make_whole_range(allocator, ref_table_id, rowkey_count, *whole_range))) {
    LOG_WARN("Failed to make whole range inner", K(ret));
  }
  return ret;
}

int ObSQLUtils::make_whole_range(
    ObIAllocator& allocator, const uint64_t ref_table_id, const int64_t rowkey_count, ObNewRange& whole_range)
{
  int ret = OB_SUCCESS;
  ObObj* start_row_key = NULL;
  ObObj* end_row_key = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == ref_table_id) || OB_UNLIKELY(rowkey_count < 1)) {
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

ObAcsIndexInfo::~ObAcsIndexInfo()
{
  if (NULL != allocator_) {
    if (NULL != query_range_) {
      query_range_->~ObQueryRange();
      allocator_->free(query_range_);
      query_range_ = NULL;
    }
    if (NULL != index_name_.ptr()) {
      allocator_->free(index_name_.ptr());
      index_name_.reset();
    }
  }
};

int ObAcsIndexInfo::deep_copy(const ObAcsIndexInfo& other)
{
  int ret = OB_SUCCESS;
  index_id_ = other.index_id_;
  is_whole_range_ = other.is_whole_range_;
  is_index_back_ = other.is_index_back_;
  prefix_filter_sel_ = other.prefix_filter_sel_;
  query_range_ = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(allocator_), K(ret));
  } else if (OB_FAIL(column_id_.init(other.column_id_.count()))) {
    LOG_WARN("failed to init fixed array", K(ret));
  } else if (OB_FAIL(column_id_.assign(other.column_id_))) {
    LOG_WARN("failed to assign column id", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, other.index_name_, index_name_))) {
    LOG_WARN("failed to copy index name", K(ret));
  } else {
    // deep copy query range
    if (NULL != other.query_range_) {
      ObQueryRange* temp_query_range = static_cast<ObQueryRange*>(allocator_->alloc(sizeof(ObQueryRange)));
      if (OB_ISNULL(temp_query_range)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for query range");
      } else {
        temp_query_range = new (temp_query_range) ObQueryRange();
        if (OB_FAIL(temp_query_range->deep_copy(*other.query_range_))) {
          temp_query_range->~ObQueryRange();
          query_range_ = NULL;
        } else {
          query_range_ = temp_query_range;
        }
      }
    }
  }
  return ret;
}

int ObSQLUtils::ConvertFiledNameAttribute(
    ObIAllocator& allocator, const ObString& src, ObString& dst, const uint16_t collation_type)
{
  int ret = OB_SUCCESS;
  const ObCollationType cs_type = static_cast<ObCollationType>(collation_type);
  if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(cs_type) || CS_TYPE_BINARY == cs_type ||
      CS_TYPE_INVALID == cs_type) {
    OZ(ob_write_string(allocator, src, dst));
  } else {
    char* buf = nullptr;
    const int32_t CharConvertFactorNum = 2;
    int32_t buf_len = src.length() * CharConvertFactorNum;
    uint32_t result_len = 0;
    const ObCollationType from_collation = ObCharset::get_default_collation(ObCharset::get_default_charset());
    const ObCollationType to_collation = static_cast<ObCollationType>(collation_type);

    if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src.length())) {
      dst.assign(NULL, 0);
    } else if (nullptr == (buf = static_cast<char*>(allocator.alloc(buf_len)))) {
      dst.assign(NULL, 0);
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory failed", K(ret), "len", buf_len);
    } else {
      ret = ObCharset::charset_convert(from_collation, src.ptr(), src.length(), to_collation, buf, buf_len, result_len);

      if (OB_SUCCESS != ret) {
        int32_t str_offset = 0;
        int64_t buf_offset = 0;
        ObString question_mark = ObCharsetUtils::get_const_str(to_collation, '?');
        while (str_offset < src.length() && buf_offset + question_mark.length() <= buf_len) {
          int64_t offset = ObCharset::charpos(from_collation, src.ptr() + str_offset, src.length() - str_offset, 1);
          ret = ObCharset::charset_convert(from_collation,
              src.ptr() + str_offset,
              offset,
              to_collation,
              buf + buf_offset,
              buf_len - buf_offset,
              result_len);
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
        dst.assign(buf, result_len);
      }
    }
  }

  return ret;
}

int ObSQLUtils::clear_evaluated_flag(const ObExprPtrIArray& calc_exprs, ObEvalCtx& eval_ctx)
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

int ObSQLUtils::copy_and_convert_string_charset(
    ObIAllocator& allocator, const ObString& src, ObString& dst, ObCollationType src_coll, ObCollationType dst_coll)
{
  return ObCharset::charset_convert(allocator, src, src_coll, dst_coll, dst,
                                    ObCharset::COPY_STRING_ON_SAME_CHARSET);
}

int ObSQLUtils::update_session_last_schema_version(
    ObMultiVersionSchemaService& schema_service, ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  int64_t received_schema_version = OB_INVALID_VERSION;
  uint64_t tenant_id = session_info.get_effective_tenant_id();
  if (OB_FAIL(schema_service.get_tenant_received_broadcast_version(tenant_id, received_schema_version))) {
    LOG_WARN("fail to get tenant received brocast version", K(ret), K(tenant_id));
  } else if (OB_FAIL(session_info.update_sys_variable(SYS_VAR_OB_LAST_SCHEMA_VERSION, received_schema_version))) {
    LOG_WARN("fail to set session variable for last_schema_version", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObImplicitCursorInfo, stmt_id_, affected_rows_, found_rows_, matched_rows_, duplicated_rows_, deleted_rows_);

int ObImplicitCursorInfo::merge_cursor(const ObImplicitCursorInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_INDEX == stmt_id_) {
    // not init, init it with first cursor info
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

OB_SERIALIZE_MEMBER(ObParamPosIdx, pos_, idx_);

int ObLinkStmtParam::write(char* buf, int64_t buf_len, int64_t& pos, int64_t param_idx)
{
  /*
   * we need 4 bytes for every const param:
   * 1 byte:  '\0' for meta info flag. '\0' can not appear in any sql stmt fmt.
   * 1 byte:  meta info type. now we used 0 to indicate const param.
   * 2 bytes: uint16 for param index.
   */
  int ret = OB_SUCCESS;
  if (buf_len - pos < PARAM_LEN) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer is not enough", K(ret), K(buf_len), K(pos));
  } else if (param_idx < 0 || param_idx > UINT16_MAX) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("param count should be between 0 and UINT16_MAX", K(ret), K(param_idx));
  } else {
    buf[pos++] = 0;  // meta flag.
    buf[pos++] = 0;  // meta type.
    *(uint16_t*)(buf + pos) = param_idx;
    pos += sizeof(uint16_t);
  }
  return ret;
}

int ObLinkStmtParam::read_next(const char* buf, int64_t buf_len, int64_t& pos, int64_t& param_idx)
{
  int ret = OB_SUCCESS;
  const char* ch = buf + pos;
  const char* buf_end = buf + buf_len - PARAM_LEN + 1;
  param_idx = -1;
  while (OB_SUCC(ret) && param_idx < 0 && ch < buf_end) {
    if (0 != ch[0]) {
      ch++;
    } else if (0 != ch[1]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown meta type", "meta_type", static_cast<int>(ch[1]));
    } else {
      param_idx = static_cast<int64_t>(*(uint16_t*)(ch + 2));
    }
  }
  pos = ch - buf;
  return ret;
}

int64_t ObLinkStmtParam::get_param_len()
{
  return PARAM_LEN;
}

const int64_t ObLinkStmtParam::PARAM_LEN = sizeof(char) * 2 + sizeof(uint16_t);

bool ObSQLUtils::is_same_type_for_compare(const ObObjMeta& meta1, const ObObjMeta& meta2)
{
  bool is_same = false;
  if (meta1.get_type() == meta2.get_type()) {
    is_same = true;
    if (meta1.is_string_type()) {
      is_same = meta1.get_collation_type() == meta2.get_collation_type();
    }
  }
  LOG_DEBUG("is same type for compare", K(meta1), K(meta2), K(is_same), K(lbt()));
  return is_same;
}

int ObSQLUtils::generate_new_name_with_escape_character(
    common::ObIAllocator& allocator, const ObString& src, ObString& dst, bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  ObString::obstr_size_t dst_len = src_len;
  const char escape_character = is_oracle_mode ? '"' : '`';
  char* ptr = NULL;

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
    if (NULL == (ptr = static_cast<char*>(allocator.alloc(dst_len + 1)))) {
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

int ObVirtualTableResultConverter::reset_and_init(ObIAllocator* key_alloc, sql::ObSQLSessionInfo* session,
    const common::ObIArray<ObObjMeta>* output_row_types, const common::ObIArray<ObObjMeta>* key_types,
    const common::ObIArray<bool>* key_with_tenant_ids, const common::ObIArray<bool>* extra_tenant_id,
    ObIAllocator* init_alloc, const share::schema::ObTableSchema* table_schema,
    const common::ObIArray<uint64_t>* output_column_ids, const bool use_real_tenant_id, const bool has_tenant_id_col,
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
    key_with_tenant_ids_ = nullptr;
    has_extra_tenant_ids_ = nullptr;
    table_schema_ = nullptr;
    output_column_ids_ = nullptr;
    base_table_id_ = UINT64_MAX;
    cur_tenant_id_ = UINT64_MAX;
    output_row_alloc_.reuse();
  }
  if (OB_ISNULL(key_alloc) || OB_ISNULL(init_alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), KP(key_alloc), KP(init_alloc));
  } else if (OB_NOT_NULL(key_with_tenant_ids) && key_with_tenant_ids->count() != key_types->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of column type and extra tenant id is not match",
        K(ret),
        K(key_with_tenant_ids->count()),
        K(key_types->count()));
  } else if (output_column_ids->count() != output_row_types->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of column type and extra tenant id is not match",
        K(ret),
        K(output_column_ids->count()),
        K(output_row_types->count()));
  } else {
    key_alloc_ = key_alloc;
    if (OB_ISNULL(init_alloc_)) {
      init_alloc_ = init_alloc;
    }
    output_row_types_ = output_row_types;
    key_types_ = key_types;
    max_col_cnt_ = max_col_cnt;
    key_with_tenant_ids_ = key_with_tenant_ids;
    has_extra_tenant_ids_ = extra_tenant_id;
    use_real_tenant_id_ = use_real_tenant_id;
    has_tenant_id_col_ = has_tenant_id_col;
    table_schema_ = table_schema;
    output_column_ids_ = output_column_ids;
    base_table_id_ = table_schema->get_table_id();
    cur_tenant_id_ = table_schema->get_tenant_id();
    // calc collation
    ObCollationType cs_type = ObCharset::get_system_collation();
    for (int64_t i = 0; i < output_row_types->count() && OB_SUCC(ret); ++i) {
      const ObObjMeta& obj_meta = output_row_types->at(i);
      if (ob_is_string_tc(obj_meta.get_type())) {
        cs_type = obj_meta.get_collation_type();
        break;
      }
    }
    ObCollationType key_cs_type = ObCharset::get_system_collation();
    for (int64_t i = 0; i < key_types->count() && OB_SUCC(ret); ++i) {
      const ObObjMeta& obj_meta = key_types->at(i);
      if (ob_is_string_tc(obj_meta.get_type())) {
        key_cs_type = obj_meta.get_collation_type();
        break;
      }
    }
    {
      CompatModeGuard g(ObWorker::CompatMode::MYSQL);
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
      key_cast_ctx_ =
          *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx(key_alloc, &dtc_params, CM_NONE, key_cs_type));
    }

    // for output row to convert
    const ObDataTypeCastParams dtc_params2 = ObBasicSessionInfo::create_dtc_params(session);
    output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_))
            ObCastCtx(&output_row_alloc_, &dtc_params2, CM_NONE, cs_type));
    if (0 == cols_schema_.count() && OB_FAIL(get_all_columns_schema())) {
      LOG_WARN("fail to get column schema", K(ret));
    }
    LOG_DEBUG("debug init converter", K(cs_type));
  }
  return ret;
}

int ObVirtualTableResultConverter::init_without_allocator(
    const common::ObIArray<const share::schema::ObColumnSchemaV2*>& col_schemas,
    const common::ObIArray<bool>* extra_tenant_ids, uint64_t cur_tenant_id, uint64_t tenant_id_col_id,
    const bool use_real_tenant_id)
{
  int ret = OB_SUCCESS;
  has_extra_tenant_ids_ = extra_tenant_ids;
  cur_tenant_id_ = cur_tenant_id;
  tenant_id_col_id_ = tenant_id_col_id;
  use_real_tenant_id_ = use_real_tenant_id;
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
  output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_))
          ObCastCtx(&output_row_alloc_, &dtc_params2, CM_NONE, ObCharset::get_system_collation()));
  LOG_DEBUG("debug timezone", K(tz_info_wrap_.get_time_zone_info()), K(ret));
  return ret;
}

void ObVirtualTableResultConverter::destroy()
{
  key_cast_ctx_ = *(new (reinterpret_cast<char*>(&key_cast_ctx_)) ObCastCtx());
  output_row_cast_ctx_ = *(new (reinterpret_cast<char*>(&output_row_cast_ctx_)) ObCastCtx());
  output_row_types_ = nullptr;
  key_types_ = nullptr;
  has_extra_tenant_ids_ = nullptr;
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

int ObVirtualTableResultConverter::get_need_convert_key_ranges_pos(ObNewRange& key_range, int64_t& pos)
{
  int ret = OB_SUCCESS;
  pos = INT64_MAX;
  const ObRowkey& start_key = key_range.start_key_;
  const ObRowkey& end_key = key_range.end_key_;
  if (start_key.get_obj_cnt() != end_key.get_obj_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start key is not match with end key",
        K(ret),
        "start key cnt",
        start_key.get_obj_cnt(),
        "end key cnt",
        end_key.get_obj_cnt());
  } else if (start_key.get_obj_cnt() > 0) {
    const ObObj* start_key_objs = start_key.get_obj_ptr();
    const ObObj* end_key_objs = end_key.get_obj_ptr();
    for (uint64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < start_key.get_obj_cnt(); ++nth_obj) {
      if ((start_key_objs[nth_obj].is_ext() || start_key_objs[nth_obj].is_null()) && !end_key_objs[nth_obj].is_ext()) {
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

int ObVirtualTableResultConverter::convert_key_ranges(ObIArray<ObNewRange>& key_ranges)
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
      } else if (OB_FAIL(convert_key(key_ranges.at(i).start_key_, new_range.start_key_, true, pos))) {
        LOG_WARN("fail to convert start key", K(ret));
      } else if (OB_FAIL(convert_key(key_ranges.at(i).end_key_, new_range.end_key_, false, pos))) {
        LOG_WARN("fail to convert end key", K(ret));
      } else if (OB_FAIL(tmp_range.push_back(new_range))) {
        LOG_WARN("fail to push back new range", K(ret));
      }
    }  // end for
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
  } else if (0 < output_column_ids_->count() && OB_FAIL(cols_schema_.reserve(output_column_ids_->count()))) {
    LOG_WARN("failed to reserve output columns ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
    const uint64_t column_id = output_column_ids_->at(i);
    const ObColumnSchemaV2* col_schema = table_schema_->get_column_schema(column_id);
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
  void* tmp_ptr = nullptr;
  ObObj* cells = NULL;
  const int64_t max_cell_cnt = INT64_MAX != max_col_cnt_ ? max_col_cnt_ : cell_cnt;
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

int ObVirtualTableResultConverter::convert_output_row(ObNewRow*& src_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is NULL", K(ret));
  } else if (!inited_row_ && OB_FAIL(init_output_row(src_row->count_))) {
    LOG_WARN("failed to init row");
  } else if (output_column_ids_->count() != src_row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: src row and dst row is not match",
        K(ret),
        K(src_row->count_),
        K(output_column_ids_->count()));
  } else {
    output_row_alloc_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
      bool need_cast = true;
      const uint64_t column_id = output_column_ids_->at(i);
      const ObObjMeta& obj_meta = output_row_types_->at(i);
      ObObj src_obj = src_row->get_cell(i);
      if (tenant_id_col_id_ == column_id) {
        src_obj.set_uint64(cur_tenant_id_);
      } else if (src_row->get_cell(i).is_string_type() && 0 == src_row->get_cell(i).get_data_length()) {
        need_cast = false;
        convert_row_.cells_[i].set_null();
      } else if (nullptr != has_extra_tenant_ids_ && need_process_tenant_id(i, src_obj) &&
                 OB_FAIL(process_tenant_id(has_extra_tenant_ids_, i, output_row_alloc_, false, src_obj))) {
        // extra tenant id
        LOG_WARN("failed to process tenant id", K(ret));
      }
      LOG_DEBUG("debug type",
          K(obj_meta.get_type()),
          K(obj_meta.get_collation_type()),
          K(src_obj),
          K(convert_row_.cells_[i]));
      if (OB_FAIL(ret)) {
      } else if (need_cast) {
        if (OB_FAIL(ObObjCaster::to_type(obj_meta.get_type(),
                obj_meta.get_collation_type(),
                output_row_cast_ctx_,
                src_obj,
                convert_row_.cells_[i]))) {
          LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
        } else {
          LOG_DEBUG("debug type",
              K(obj_meta.get_type()),
              K(obj_meta.get_collation_type()),
              K(src_obj),
              K(convert_row_.cells_[i]),
              K(output_row_cast_ctx_.dest_collation_));
        }
      }
    }
    src_row = &convert_row_;
  }
  return ret;
}

// only for stat collect
int ObVirtualTableResultConverter::convert_column(ObObj& src_obj, uint64_t column_id, uint64_t idx)
{
  int ret = OB_SUCCESS;
  if (idx >= cols_schema_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column idx is invalid", K(idx), K(cols_schema_.count()));
  } else {
    ObObj dst_obj = src_obj;
    bool need_cast = true;
    const ObColumnSchemaV2* col_schema = cols_schema_.at(idx);
    if (col_schema->get_column_id() != column_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column is not match", K(ret), K(column_id), K(col_schema->get_column_id()));
    } else if (tenant_id_col_id_ == column_id) {
      dst_obj.set_uint64(cur_tenant_id_);
    } else if (src_obj.is_null() || (src_obj.is_string_type() && 0 == src_obj.get_data_length())) {
      need_cast = false;
      src_obj.set_null();
    } else if (src_obj.is_ext()) {
      need_cast = false;
    } else if (nullptr != key_with_tenant_ids_ && need_process_tenant_id(idx, dst_obj) &&
               OB_FAIL(process_tenant_id(key_with_tenant_ids_, idx, output_row_alloc_, false, dst_obj))) {
      // extra tenant id
      LOG_WARN("failed to process tenant id", K(ret));
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

bool ObVirtualTableResultConverter::need_process_tenant_id(int64_t nth_col, ObObj& obj)
{
  bool needed = false;
  if (!has_extra_tenant_ids_->at(nth_col) || obj.is_null()) {
    // do nothing
  } else if (OB_SYS_TENANT_ID == cur_tenant_id_ && OB_ALL_SEQUENCE_VALUE_TID != extract_pure_id(base_table_id_)) {
    // __all_sequence_value's sequence_id always filter tenant_id
  } else if ((OB_ALL_ROUTINE_PARAM_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_ROUTINE_PARAM_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == cols_schema_.at(nth_col)->get_column_name_str().case_compare("type_owner") && obj.is_int() &&
             obj.get_int() > 0 && OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_routine_param's type_owner may be OB_SYS_TENANT_ID, do not filter it.
  } else if (OB_ALL_TENANT_DEPENDENCY_TID == extract_pure_id(base_table_id_) &&
             (0 == cols_schema_.at(nth_col)->get_column_name_str().case_compare("ref_obj_id") && obj.is_int() &&
                 obj.get_int() > 0) &&
             OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // ref_obj_id may contain sys tenant id, do not filter it.
  } else if ((OB_ALL_TYPE_ATTR_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_TYPE_ATTR_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == cols_schema_.at(nth_col)->get_column_name_str().case_compare("type_attr_id") && obj.is_int() &&
             obj.get_int() > 0 && OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_type_attr's type_attr_id may be OB_SYS_TENANT_ID, do not filter it.
  } else if ((OB_ALL_COLL_TYPE_TID == extract_pure_id(base_table_id_) ||
                 OB_ALL_COLL_TYPE_HISTORY_TID == extract_pure_id(base_table_id_)) &&
             0 == cols_schema_.at(nth_col)->get_column_name_str().case_compare("elem_type_id") && obj.is_int() &&
             obj.get_int() > 0 && OB_SYS_TENANT_ID == extract_tenant_id(obj.get_int())) {
    // tenant_id of __all_coll_type's elem_type_id may be OB_SYS_TENANT_ID, do not filter it.
  } else {
    needed = true;
  }
  return needed;
}

int ObVirtualTableResultConverter::convert_output_row(
    ObEvalCtx& eval_ctx, const common::ObIArray<ObExpr*>& src_exprs, const common::ObIArray<ObExpr*>& dst_exprs)
{
  int ret = OB_SUCCESS;
  if (!inited_row_ && OB_FAIL(init_output_row(dst_exprs.count()))) {
    LOG_WARN("failed to init row");
  } else if (dst_exprs.count() != dst_exprs.count() || output_column_ids_->count() != dst_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: src row and dst row is not match", K(ret), K(dst_exprs.count()));
  } else {
    output_row_alloc_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_->count(); ++i) {
      const uint64_t column_id = output_column_ids_->at(i);
      const ObObjMeta& obj_meta = output_row_types_->at(i);
      ObDatum* datum = nullptr;
      ObExpr* expr = src_exprs.at(i);
      ObExpr* dst_expr = dst_exprs.at(i);
      ObDatum& dst_datum = dst_expr->locate_datum_for_write(eval_ctx);
      ObObj dst_obj;
      if (tenant_id_col_id_ == column_id) {
        convert_row_.cells_[i].set_uint64(cur_tenant_id_);
      } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else if (OB_FAIL(datum->to_obj(convert_row_.cells_[i], expr->obj_meta_))) {
        LOG_WARN("failed to cast obj", K(ret));
      } else if (convert_row_.cells_[i].is_null() ||
                 (convert_row_.cells_[i].is_string_type() && 0 == convert_row_.cells_[i].get_data_length())) {
        convert_row_.cells_[i].set_null();
      } else if (nullptr != has_extra_tenant_ids_ && need_process_tenant_id(i, convert_row_.cells_[i]) &&
                 OB_FAIL(
                     process_tenant_id(has_extra_tenant_ids_, i, output_row_alloc_, false, convert_row_.cells_[i]))) {
        // extra tenant id
        LOG_WARN("failed to process tenant id", K(ret));
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
      } else {
        dst_expr->get_eval_info(eval_ctx).evaluated_ = true;
      }
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::convert_key(const ObRowkey& src, ObRowkey& dst, bool is_start_key, int64_t pos)
{
  int ret = OB_SUCCESS;
  UNUSED(is_start_key);
  if (src.get_obj_cnt() > 0) {
    const ObObj* src_key_objs = src.get_obj_ptr();
    void* tmp_ptr = NULL;
    ObObj* new_key_obj = NULL;
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
    CompatModeGuard g(ObWorker::CompatMode::MYSQL);
    for (int64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < src.get_obj_cnt(); ++nth_obj) {
      const ObObj& src_obj = src_key_objs[nth_obj];
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
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected status: start key range is invalid", K(ret), K(src));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: start key range is invalid", K(ret), K(src));
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
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected status: end key range is invalid", K(ret), K(src));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: end key range is invalid", K(ret), K(src));
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
        const ObObjMeta& obj_meta = key_types_->at(nth_obj);
        if (OB_FAIL(ObObjCaster::to_type(obj_meta.get_type(),
                obj_meta.get_collation_type(),
                key_cast_ctx_,
                src_key_objs[nth_obj],
                new_key_obj[nth_obj]))) {
          LOG_WARN("fail to cast obj", K(ret), K(key_types_->at(nth_obj)), K(src_key_objs[nth_obj]));
        } else if (OB_FAIL(process_tenant_id(key_with_tenant_ids_, nth_obj, *key_alloc_, true, new_key_obj[nth_obj]))) {
          // extra tenant id
          LOG_WARN("failed to process tenant id", K(ret));
        } else if (has_tenant_id_col_ && 0 == nth_obj && !use_real_tenant_id_) {
          if (new_key_obj[nth_obj].get_type() == ObIntType) {
            new_key_obj[nth_obj].set_int(new_key_obj[nth_obj].get_int() - cur_tenant_id_);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tenant id type", K(new_key_obj[nth_obj].get_type()), K(ret));
          }
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      LOG_TRACE("trace range key", K(ret), K(new_key_obj[0]), K(tenant_id_col_id_), K(use_real_tenant_id_));
      dst.assign(new_key_obj, src.get_obj_cnt());
    }
  }
  return ret;
}

int ObVirtualTableResultConverter::process_tenant_id(const ObIArray<bool>* extract_tenant_ids, const int64_t nth_col,
    ObIAllocator& allocator, bool is_extra_tenant_id, ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extract_tenant_ids) || !extract_tenant_ids->at(nth_col) || obj.is_null()) {
  } else if (!obj.is_int() && !obj.is_uint64() && !obj.is_varchar_or_char()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported obj type", K(ret), K(obj));
  } else if (obj.is_int() && obj.get_int() > 0) {
    int64_t value = is_extra_tenant_id ? extract_pure_id(obj.get_int())
                                       : static_cast<int64_t>(combine_id(cur_tenant_id_, obj.get_int()));
    obj.set_int_value(value);
  } else if (obj.is_uint64() && 0 != obj.get_uint64()) {
    uint64_t value = is_extra_tenant_id ? static_cast<uint64_t>(extract_pure_id(obj.get_int()))
                                        : combine_id(cur_tenant_id_, obj.get_int());
    obj.set_uint64_value(value);
  } else if (obj.is_varchar_or_char()) {
    // __all_sys_stat column `value`
    int64_t value = OB_INVALID_ID;
    if (OB_FAIL(ObSchemaUtils::str_to_int(obj.get_string(), value))) {
      LOG_WARN("fail to covert str to int", K(ret), K(obj));
    } else if (value > 0) {
      value = is_extra_tenant_id ? extract_pure_id(value) : static_cast<int64_t>(combine_id(cur_tenant_id_, value));
      int64_t len = OB_MAX_BIT_LENGTH;
      char* buf = static_cast<char*>(allocator.alloc(len));
      int64_t pos = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, len, pos, "%lu", value))) {
        LOG_WARN("fail to convert uint to str", K(ret), K(value));
      } else if (0 == pos || pos >= len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pos", K(ret), K(pos));
      } else {
        obj.set_char_value(buf, static_cast<ObString::obstr_size_t>(pos));
      }
    }
  }
  LOG_DEBUG("debug extra tenant id", K(obj));
  return ret;
}

int ObSQLUtils::check_table_version(
    bool& equal, const DependenyTableStore& dependency_tables, share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  equal = true;
  int64_t latest_table_version = -1;
  for (int64_t i = 0; i < dependency_tables.count(); i++) {
    const share::schema::ObSchemaObjVersion& table_version = dependency_tables.at(i);
    if (OB_FAIL(schema_guard.get_table_schema_version(table_version.get_object_id(), latest_table_version))) {
      LOG_WARN("failed to get table schema version", K(ret), K(table_version.get_object_id()));
    }
    if (table_version.get_version() != latest_table_version) {
      equal = false;
    }
  }
  return ret;
}

int ObSQLUtils::generate_view_definition_for_resolve(ObIAllocator& allocator, ObCollationType connection_collation,
    const ObViewSchema& view_schema, ObString& view_definition)
{
  int ret = OB_SUCCESS;
  const ObString raw_view_def = view_schema.get_view_definition_str();
  view_definition.reset();

  if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(
          allocator, raw_view_def, view_definition, CS_TYPE_UTF8MB4_GENERAL_CI, connection_collation))) {
    LOG_WARN("fail to copy and convert string charset", K(ret));
  }
  return ret;
}

int ObSQLUtils::convert_sql_text_from_schema_for_resolve(
    ObIAllocator& allocator, const ObDataTypeCastParams& dtc_params, ObString& sql_text)
{
  int ret = OB_SUCCESS;
  OZ(ObCharset::charset_convert(allocator, sql_text, CS_TYPE_UTF8MB4_BIN, dtc_params.connection_collation_, sql_text));
  return ret;
}

int ObSQLUtils::convert_sql_text_to_schema_for_storing(
    ObIAllocator& allocator, const ObDataTypeCastParams& dtc_params, ObString& sql_text)
{
  int ret = OB_SUCCESS;
  OZ(ObCharset::charset_convert(allocator, sql_text, dtc_params.connection_collation_, CS_TYPE_UTF8MB4_BIN, sql_text));
  return ret;
}

int ObSQLUtils::print_identifier(char* buf, const int64_t buf_len, int64_t& pos, ObCollationType connection_collation,
    const common::ObString& identifier_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (ObCharset::charset_type_by_coll(connection_collation) == CHARSET_UTF8MB4) {
    if (OB_UNLIKELY(pos + identifier_name.length() > buf_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(identifier_name));
    } else {
      MEMCPY(buf + pos, identifier_name.ptr(), identifier_name.length());
      pos += identifier_name.length();
    }
  } else if (OB_UNLIKELY(buf_len <= pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret));
  } else {
    uint32_t result_len = 0;
    if (OB_FAIL(ObCharset::charset_convert(CS_TYPE_UTF8MB4_BIN,
            identifier_name.ptr(),
            identifier_name.length(),
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

int64_t ObSqlFatalErrExtraInfoGuard::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  J_OBJ_START();
  J_NEWLINE();
  J_KV(K_(cur_sql));
  const ObIArray<ObSchemaObjVersion>* dep_tables = nullptr;
  ObString sys_var_values;

  if (OB_NOT_NULL(plan_)) {
    dep_tables = &(plan_->get_dependency_table());
    sys_var_values = plan_->stat_.sys_vars_str_;
  } else if (OB_NOT_NULL(exec_ctx_)) {
    ObStmtFactory* stmt_factory = nullptr;
    ObQueryCtx* query_ctx = nullptr;
    if (OB_ISNULL(stmt_factory = exec_ctx_->get_stmt_factory()) ||
        OB_ISNULL(query_ctx = stmt_factory->get_query_ctx())) {
      LOG_WARN("fail to get query ctx", K(ret));
    } else {
      dep_tables = &(query_ctx->global_dependency_tables_);
    }
    if (OB_NOT_NULL(exec_ctx_->get_my_session())) {
      sys_var_values = exec_ctx_->get_my_session()->get_sys_var_in_pc_str();
    }
  }

  if (OB_NOT_NULL(dep_tables)) {
    OZ(databuff_printf(buf, buf_len, pos, ", \ndependency_table_def:"));
    for (int i = 0; i < dep_tables->count(); ++i) {
      const ObSchemaObjVersion& schema_obj = dep_tables->at(i);
      if (schema_obj.get_schema_type() == TABLE_SCHEMA) {
        ObSchemaGetterGuard schema_guard;
        ObSchemaPrinter schema_printer(schema_guard);
        OZ(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard, schema_obj.version_));
        OZ(databuff_printf(buf, buf_len, pos, (i != 0) ? ",\n\"" : "\n\""));
        OZ(schema_printer.print_table_definition(
            schema_obj.get_object_id(), buf, buf_len, pos, NULL, LS_DEFAULT, false));
        OZ(databuff_printf(buf, buf_len, pos, "\""));
      }
    }
  }

  if (!sys_var_values.empty()) {
    OZ(databuff_printf(buf, buf_len, pos, ",\nsys_vars:{"));
    for (int i = 0; i < ObSysVarFactory::ALL_SYS_VARS_COUNT; ++i) {
      if (!!(ObSysVariables::get_flags(i) & ObSysVarFlag::INFLUENCE_PLAN)) {
        ObString cur_var_value = sys_var_values.split_on(',');
        const char* sep_str = ",";
        if (cur_var_value.empty()) {
          cur_var_value = sys_var_values;
          sep_str = "";
        }
        OZ(databuff_printf(buf,
            buf_len,
            pos,
            "\"%.*s\":\"%.*s\"%s",
            ObSysVariables::get_name(i).length(),
            ObSysVariables::get_name(i).ptr(),
            cur_var_value.length(),
            cur_var_value.ptr(),
            sep_str));
      }
    }
    OZ(databuff_printf(buf, buf_len, pos, "}"));
  }
  // OX (plan_->print_tree(buf, buf_len, pos, plan_->get_main_query()));

  J_NEWLINE();
  J_OBJ_END();
  return pos;
}

bool ObSQLUtils::is_oracle_empty_string(const ObObjParam& param)
{
  return (param.is_null() && ObCharType == param.get_param_meta().get_type());
}

int ObSQLUtils::handle_audit_record(
    bool need_retry, const ObExecuteMode exec_mode, ObSQLSessionInfo& session, ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  if (need_retry) {
    /*do nothing*/
  } else if (GCONF.enable_sql_audit) {
    if (session.get_local_ob_enable_sql_audit()) {
      FETCH_ENTITY(TENANT_SPACE, session.get_priv_tenant_id())
      {
        ObMySQLRequestManager* req_manager = MTL_GET(ObMySQLRequestManager*);
        if (nullptr == req_manager) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get request manager for current tenant", K(ret));
        } else {
          ObAuditRecordData audit_record = session.get_final_audit_record(exec_mode);
          audit_record.sched_info_ = exec_ctx.get_sched_info();
          bool is_sensitive = (NULL != exec_ctx.get_sql_ctx()) ?
                              exec_ctx.get_sql_ctx()->is_sensitive_ : true;
          if (OB_FAIL(req_manager->record_request(audit_record, is_sensitive))) {
            if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
              LOG_DEBUG("cannot allocate mem for record", K(ret));
              ret = OB_SUCCESS;
            } else {
              if (REACH_TIME_INTERVAL(100 * 1000)) {  // in case logging is too frequent
                LOG_WARN("failed to record request info in request manager", K(ret));
              }
            }
          }
        }
      }
    }
  }
  session.update_stat_from_audit_record();
  session.reset_audit_record();
  return ret;
}

bool ObSQLUtils::is_one_part_table_can_skip_part_calc(const ObTableSchema &schema)
{
  bool can_skip = false;
  if (!schema.is_partitioned_table()) {
    can_skip = true;
  } else if (schema.get_all_part_num() == 1 && schema.is_hash_part()) {
    can_skip = true;
  } else {
    can_skip = false;
  }
  return can_skip;
}
