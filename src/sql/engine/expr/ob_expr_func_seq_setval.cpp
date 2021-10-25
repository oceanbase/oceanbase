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

#include "sql/engine/expr/ob_expr_func_seq_setval.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/sequence/ob_sequence_cache.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace sql {
ObExprFuncSeqSetval::ObExprFuncSeqSetval(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MYSQL_SEQ_SETVAL, N_MYSQL_SEQ_SETVAL, 4, NOT_ROW_DIMENSION)
{}

ObExprFuncSeqSetval::~ObExprFuncSeqSetval()
{}


int ObExprFuncSeqSetval::calc_result_typeN(ObExprResType& type, ObExprResType* types, 
    int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  type.set_number();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].precision_);
  // type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  return ret;
}

int ObExprFuncSeqSetval::calc_resultN(ObObj& res, const ObObj* objs, int64_t param_num, ObExprCtx& ctx) const
{
  int ret = OB_SUCCESS;
  const ObSequenceSchema* seq_schema = nullptr;
  ObSQLSessionInfo* session = ctx.exec_ctx_->get_my_session();
  uint64_t tenant_id = session->get_effective_tenant_id();

  if (OB_UNLIKELY(param_num < 3 || param_num > 5)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg num", K(ret), K(param_num));
  } else {
    const ParamStore& param_store = ctx.exec_ctx_->get_physical_plan_ctx()->get_param_store();  
    const ObString& db_name = objs[0].get_string();
    const ObString& seq_name = objs[1].get_string();
    common::number::ObNumber new_next_val_num;
    common::number::ObNumber round_num;
    bool used = true;
    ObNumStackAllocator<4> allocator;

    if (OB_FAIL(ObExprFuncSeqSetval::acquire_sequence_schema(tenant_id, *ctx.exec_ctx_, db_name, seq_name, seq_schema))) {
      LOG_WARN("get schema failed", K(ret));
    } else if (OB_FAIL(ObExprFuncSeqSetval::number_from_obj(param_store.at(0), new_next_val_num, allocator))) {
      LOG_WARN("get next value param failed", K(ret));
    } else if (param_store.count() > 2 && OB_FAIL(ObExprFuncSeqSetval::number_from_obj(param_store.at(2), round_num, allocator))) {
      LOG_WARN("get round param failed", K(ret));
    } else if (param_store.count() > 1) {
      int num = param_store.at(1).get_int();
      ObObjType type = param_store.at(1).get_type();
      if (type != ObIntType && type != ObTinyIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid type of is_used parameter", K(type));
      } else if (num < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of is_used parameter", K(num));
      } else {
        used = param_store.at(1).get_bool();
      }
    }

    ObSequenceValue round;
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(round_num < static_cast<int64_t>(0))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of round parameter", K(round_num));
      } else if(!seq_schema->get_cycle_flag() && round_num != static_cast<int64_t>(0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid round value", K(seq_schema->get_cycle_flag()), K(round_num));
      } else {
        round.set(round_num);
      }
    }

    ObSequenceValue new_next_val;
    common::number::ObNumber old_val;
    if (OB_SUCC(ret)) {
      old_val.shadow_copy(new_next_val_num);
      if (used) {
        const common::number::ObNumber increment = seq_schema->get_increment_by();
        old_val.add(increment, new_next_val_num, allocator);
      }
      
      if (new_next_val_num > seq_schema->get_max_value()) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_WARN("new_next_value is larger than MAX_VALUE, please check next value and used params", K(seq_schema->get_max_value()));
      } else if (new_next_val_num < seq_schema->get_min_value()) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_WARN("new_next_value is lower than MIN_VALUE, please check next value and used params", K(seq_schema->get_min_value()));
      } else {
        new_next_val.set(new_next_val_num);
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("setval parameters in calc_sequence_setval.", K(db_name), K(seq_name), K(new_next_val_num.format()), K(used), K(round_num.format()), K(ret));
      share::ObSequenceCache* sequence_cache = &share::ObSequenceCache::get_instance();
      common::number::ObNumber calc_result;
      ObSequenceValue value;
      bool valid = true;
      if (OB_FAIL(sequence_cache->setval(*seq_schema, new_next_val, round, value, valid))) {
        LOG_WARN("failed to get sequence value from cache", K(tenant_id), K(ret));
      } else if (valid) {
        if (OB_FAIL(calc_result.from(value.val(), allocator))) {
          LOG_WARN("fail deep copy value", K(ret));
        } else if(used) {
          res.set_number(old_val);
        } else {
          res.set_number(calc_result);
        }
      } else {
        res.set_null();
      }
      LOG_DEBUG("trace sequence setval", K(calc_result), K(ret));
      
    }
  }
  
  return ret;
}

int ObExprFuncSeqSetval::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 3 || rt_expr.arg_cnt_ > 5)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg num", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = calc_sequence_setval;
  }
  return ret;
}

int ObExprFuncSeqSetval::number_from_obj(const ObObj& obj, common::number::ObNumber& number, common::ObIAllocator& allocator) 
{
  int ret = OB_SUCCESS;
  ObObjType type = obj.get_meta().get_type();
  switch (type) {
    case ObIntType: {
      if (OB_FAIL(number.from(obj.get_int(), allocator))) {
        LOG_WARN("generate number from object failed", K(ret), K(type));
      }
      break;
    }
    case ObNumberType: {
      if (OB_FAIL(number.from(obj.get_number(), allocator))) {
        LOG_WARN("generate number from object failed", K(ret), K(type));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the data type of ObObj is not int or number", K(ret), K(type));
      break;
    }
  }

  return ret;
}

int ObExprFuncSeqSetval::acquire_sequence_schema(const uint64_t tenant_id, ObExecContext& exec_ctx, const ObString& db_name, 
    const ObString& seq_name, const share::schema::ObSequenceSchema*& seq_schema)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_ctx = NULL;
  share::schema::ObMultiVersionSchemaService* schema_service = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  uint64_t seq_id = OB_INVALID_ID;
  uint64_t db_id = OB_INVALID_ID;
  bool exist = false;

  if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null", K(ret));
  } else if (OB_ISNULL(schema_service = task_ctx->schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, db_id))) {
    LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(db_name));
  } else if (OB_FAIL(
    schema_guard.check_sequence_exist_with_name(tenant_id, db_id, seq_name, exist, seq_id))) {
    LOG_WARN("failed to check sequence with name", K(ret), K(seq_name), K(db_id));
  } else if (!exist) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sequence is not exist", K(db_name), K(db_id), K(seq_name));
  } else if (OB_FAIL(schema_guard.get_sequence_schema(tenant_id, seq_id, seq_schema))) {
    LOG_WARN("fail get sequence schema", K(seq_id), K(ret));
  } else if (OB_ISNULL(seq_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  }

  return ret;
}

int ObExprFuncSeqSetval::calc_sequence_setval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  const ObSequenceSchema* seq_schema = nullptr;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  uint64_t tenant_id = session->get_effective_tenant_id();

  const ParamStore& param_store = ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();  
  const ObString& db_name = expr.locate_param_datum(ctx, 0).get_string();
  const ObString& seq_name = expr.locate_param_datum(ctx, 1).get_string();
  common::number::ObNumber new_next_val_num;
  common::number::ObNumber round_num;
  bool used = true;
  ObNumStackAllocator<4> allocator;

  if (OB_FAIL(acquire_sequence_schema(tenant_id, ctx.exec_ctx_, db_name, seq_name, seq_schema))) {
    LOG_WARN("get schema failed", K(ret));
  } else if (param_store.count() != 0) {
    if (OB_FAIL(number_from_obj(param_store.at(0), new_next_val_num, allocator))) {
      LOG_WARN("get next value param failed", K(ret));
    } else if (param_store.count() > 2 && OB_FAIL(number_from_obj(param_store.at(2), round_num, allocator))) {
      LOG_WARN("get round param failed", K(ret));
    } else if (param_store.count() > 1) {
      int num = param_store.at(1).get_int();
      ObObjType type = param_store.at(1).get_type();
      if (type != ObIntType && type != ObTinyIntType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid type of is_used parameter", K(type));
      } else if (num < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of is_used parameter", K(num));
      } else {
        used = param_store.at(1).get_bool();
      }
    }
  } else {
    new_next_val_num = expr.locate_param_datum(ctx, 2).get_number();
    if (ObString(new_next_val_num.format()) == ObString("")) {
      new_next_val_num.from(expr.locate_param_datum(ctx, 2).get_int(), allocator);
    }

    if (expr.arg_cnt_ > 4) {
      round_num = expr.locate_param_datum(ctx, 4).get_number();
      if (ObString(round_num.format()) == ObString("")) {
        round_num.from(expr.locate_param_datum(ctx, 4).get_int(), allocator);
      }
    } 
    if (expr.arg_cnt_ > 3) {
      int num = expr.locate_param_datum(ctx, 3).get_tinyint();
      if (num < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value of is_used parameter", K(num));
      } else {
        used = (num==0);
      }
    }
  }

  ObSequenceValue round;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(round_num < static_cast<int64_t>(0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid value of round parameter", K(round_num));
    } else if(!seq_schema->get_cycle_flag() && round_num != static_cast<int64_t>(0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid round value", K(seq_schema->get_cycle_flag()), K(round_num));
    } else {
      round.set(round_num);
    }
  }

  ObSequenceValue new_next_val;
  common::number::ObNumber old_val;
  if (OB_SUCC(ret)) {
    old_val.shadow_copy(new_next_val_num);
    if (used) {
      const common::number::ObNumber increment = seq_schema->get_increment_by();
      old_val.add(increment, new_next_val_num, allocator);
    }
    
    if (new_next_val_num > seq_schema->get_max_value()) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("new_next_value is larger than MAX_VALUE, please check next value and used params", K(seq_schema->get_max_value()));
    } else if (new_next_val_num < seq_schema->get_min_value()) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("new_next_value is lower than MIN_VALUE, please check next value and used params", K(seq_schema->get_min_value()));
    } else {
      new_next_val.set(new_next_val_num);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("setval parameters in calc_sequence_setval.", K(db_name), K(seq_name), K(new_next_val_num.format()), K(used), K(round_num.format()), K(ret));
    share::ObSequenceCache* sequence_cache = &share::ObSequenceCache::get_instance();
    common::number::ObNumber calc_result;
    ObSequenceValue value;
    bool valid = true;
    if (OB_FAIL(sequence_cache->setval(*seq_schema, new_next_val, round, value, valid))) {
      LOG_WARN("failed to get sequence value from cache", K(tenant_id), K(ret));
    } else if (valid) {
      if (OB_FAIL(calc_result.from(value.val(), allocator))) {
        LOG_WARN("fail deep copy value", K(ret));
      } else if(used) {
        res.set_number(old_val);
      } else {
        res.set_number(calc_result);
      }
    } else {
      res.set_null();
    }
    LOG_DEBUG("trace sequence setval", K(calc_result), K(ret));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
