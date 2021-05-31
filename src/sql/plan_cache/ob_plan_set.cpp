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

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_plan_set.h"

#include "lib/trace/ob_trace_event.h"
#include "lib/number/ob_number_v2.h"
#include "common/ob_role.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/plan_cache/ob_dist_plans.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "share/part/ob_part_mgr.h"
#include "storage/ob_partition_service.h"
#include "ob_plan_set.h"

using namespace oceanbase;
using namespace common;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;
using namespace share;
using namespace share::schema;
namespace oceanbase {
namespace sql {
ObPlanSet::~ObPlanSet()
{
  // Make sure destory planset before destory pre calculable expression.
  if (OB_ISNULL(pre_cal_expr_handler_)) {
    // have no pre calculable expression, do nothing
  } else {
    int64_t ref_cnt = pre_cal_expr_handler_->dec_ref_cnt();
    if (ref_cnt == 0) {
      common::ObIAllocator* alloc = pre_cal_expr_handler_->pc_alloc_;
      pre_cal_expr_handler_->~PreCalcExprHandler();
      alloc->free(pre_cal_expr_handler_);
      pre_cal_expr_handler_ = NULL;
    }
  }
}

// used for get plan
int ObPlanSet::match_params_info(
    const ParamStore* params, ObPlanCacheCtx& pc_ctx, int64_t outline_param_idx, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObExecContext& exec_ctx = pc_ctx.exec_ctx_;
  ObSessionVariable sess_var;
  if (false == is_match_outline_param(outline_param_idx)) {
    is_same = false;
  } else if (OB_ISNULL(params)) {
    is_same = true;
  } else if (params->count() > params_info_.count()) {
    is_same = false;
  } else {
    bool is_batched_multi_stmt = pc_ctx.sql_ctx_.multi_stmt_item_.is_batched_multi_stmt();
    int64_t N = params->count();
    LOG_TRACE("params info", K(params_info_), K(*params), K(change_char_index_));
    for (int64_t i = 0; is_same && i < N; ++i) {
      if (OB_FAIL(match_param_info(
              params_info_.at(i), params->at(i), change_char_index_.has_member(i), is_batched_multi_stmt, is_same))) {
        LOG_WARN("fail to match param info", K(ret), K(params_info_), K(*params));
      }
    }
    // if all true, change varchar obj to char obj if neccessary
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < N; i++) {
      if (change_char_index_.has_member(i) && ObCharType == params_info_.at(i).type_) {
        if (!params->at(i).is_ext()) {
          if (ObVarcharType == params->at(i).get_type()) {
            const_cast<ObObjParam&>(params->at(i)).meta_.set_type(ObCharType);
            const_cast<ObObjParam&>(params->at(i)).set_param_meta();
          }
        } else {
          ret = OB_NOT_SUPPORTED;
        }
      }
    }
    if (OB_SUCC(ret) && is_same && related_user_var_names_.count() > 0) {
      if (related_user_var_names_.count() != related_user_sess_var_metas_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related_user_var_names and related_user_sess_vars should have the same size",
            K(ret),
            K(related_user_var_names_.count()),
            K(related_user_sess_var_metas_.count()));
      } else if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(pc_ctx.sql_ctx_.session_info_));
      } else {
        ObSQLSessionInfo* session_info = pc_ctx.sql_ctx_.session_info_;
        for (int64_t i = 0; OB_SUCC(ret) && is_same && i < related_user_var_names_.count(); i++) {
          if (OB_FAIL(session_info->get_user_variable(related_user_var_names_.at(i), sess_var))) {
            LOG_WARN("failed to get user variable", K(ret), K(related_user_var_names_.at(i)), K(i));
          } else {
            is_same = (related_user_sess_var_metas_.at(i) == sess_var.meta_);
          }
        }
      }
    }

    // pre calculate
    if (OB_SUCC(ret) && is_same) {
      ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
      ObSQLSessionInfo* session = exec_ctx.get_my_session();

      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null session", K(ret));
      } else if (OB_ISNULL(plan_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan context", K(ret));
      } else if (fetch_cur_time_ && FALSE_IT(plan_ctx->set_cur_time(ObClockGenerator::getClock(), *session))) {
        // never reach
      } else if (OB_ISNULL(pre_cal_expr_handler_) && (params->count() == params_info_.count())) {
        // pre calculable expression handler is null, do nothing.
      } else if (OB_FAIL(pre_calc_exprs(exec_ctx))) {
        LOG_WARN("pre calculate exprs failed", K(ret));
      } else if (params->count() != params_info_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param info count is different", K(params_info_), K(*params), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < params->count(); ++i) {
      if (OB_FAIL(match_param_bool_value(params_info_.at(i), params->at(i), is_batched_multi_stmt, is_same))) {
        LOG_WARN("fail to match param bool value", K(params_info_), K(*params));
      }
    }  // for end

    // check const constraint
    if (OB_SUCC(ret) && is_same) {
      OC((match_constraint)(*params, is_same));
    }

    if (OB_SUCC(ret) && is_same) {
      if (OB_FAIL(match_multi_stmt_info(*params, multi_stmt_rowkey_pos_, is_same))) {
        LOG_WARN("failed to match multi stmt info", K(ret));
      } else if (!is_same) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_TRACE("batched multi stmt needs rollback", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      is_same = false;
    }
  }
  LOG_TRACE("match param result", K(ret), K(is_same), K(params_info_));
  return ret;
}

int ObPlanSet::match_param_info(const ObParamInfo& param_info, const ObObjParam& param, const bool is_varchar_eq_char,
    const bool& is_batched_multi_stmt, bool& is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  // extend type must be checked
  // insert into t values (1)
  // insert into t values (:0)
  // two sql have the same key `insert into t values (?)`
  // but they have complete different plans
  if (param_info.flag_.need_to_check_type_ || (is_batched_multi_stmt && param.is_ext())) {
    if (param.get_param_meta().get_type() != param.get_type()) {
      LOG_TRACE("differ in match param info", K(param.get_param_meta().get_type()), K(param.get_type()));
    }
    if (param.get_param_meta().get_type() != param_info.type_) {
      if (ObVarcharType == param.get_param_meta().get_type() && ObCharType == param_info.type_ && is_varchar_eq_char) {
        if (param_info.is_oracle_empty_string_ && !param.is_null()) {
          is_same = false;
        } else if (ObSQLUtils::is_oracle_empty_string(param) && !param_info.is_oracle_empty_string_) {
          is_same = false;
        } else {
          is_same = (param.get_scale() == param_info.scale_);
        }
      } else {
        is_same = false;
      }
    } else if (param.is_ext()) {
      ret = OB_NOT_SUPPORTED;
    } else if (param_info.is_oracle_empty_string_ && !param.is_null()) {
      is_same = false;
    } else if (ObSQLUtils::is_oracle_empty_string(param) && !param_info.is_oracle_empty_string_) {
      is_same = false;
    } else {
      is_same = (param.get_scale() == param_info.scale_);
    }
  }
  return ret;
}

int ObPlanSet::match_param_bool_value(
    const ObParamInfo& param_info, const ObObjParam& param, const bool& is_batched_multi_stmt, bool& is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  bool vec_param_same = true;
  bool first_val = true;
  if (param_info.flag_.need_to_check_bool_value_) {
    bool is_value_true = false;
    if (!param.is_ext()) {
      if (OB_FAIL(ObObjEvaluator::is_true(param, is_value_true))) {
        SQL_PC_LOG(WARN, "fail to get param info", K(ret));
      } else if (is_value_true != param_info.flag_.expected_bool_value_) {
        is_same = false;
      }
    } else {  // is_ext
      if (!is_batched_multi_stmt) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ext param in none batched multi stmt not support check bool param", K(ret));
      } else if (OB_FAIL(check_vector_param_same_bool(param, first_val, vec_param_same))) {
        LOG_WARN("fail to check vector param same bool", K(ret));
      } else if (false == vec_param_same) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      } else if (first_val != param_info.flag_.expected_bool_value_) {
        is_same = false;
      }
    }  // is_ext end
  }

  return ret;
}

int ObPlanSet::match_multi_stmt_info(
    const ParamStore& params, const ObIArray<int64_t>& multi_stmt_rowkey_pos, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (multi_stmt_rowkey_pos.empty()) {
    is_match = true;
  } else {
    // check all rowkey are different
    int64_t stmt_count = 0;
    int64_t rowkey_count = multi_stmt_rowkey_pos.count();
    ObSEArray<const ObObj*, 16> binding_data;
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_stmt_rowkey_pos.count(); i++) {
      int64_t pos = multi_stmt_rowkey_pos.at(i);
      if (OB_UNLIKELY(pos < 0 || pos >= params.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected array pos", K(pos), K(params.count()), K(ret));
      } else if (OB_UNLIKELY(ObExtendType != params.at(pos).get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected type", K(params.at(pos).get_type()), K(ret));
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    }
    if (OB_SUCC(ret)) {
      is_match = true;
      HashKey hash_key;
      UniqueHashSet unique_ctx;
      if (OB_FAIL(unique_ctx.create(stmt_count))) {
        LOG_WARN("failed to hash set", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && is_match && i < stmt_count; i++) {
          hash_key.reuse();
          for (int64_t j = 0; OB_SUCC(ret) && j < binding_data.count(); j++) {
            ret = hash_key.rowkey_.push_back(binding_data.at(j)[i]);
          }
          if (OB_SUCC(ret)) {
            ret = unique_ctx.exist_refactored(hash_key);
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
              is_match = false;
              if (REACH_TIME_INTERVAL(10000000)) {
                LOG_INFO("batched multi-stmt does not have the same rowkey", K(i), K(hash_key));
              }
            } else if (OB_HASH_NOT_EXIST == ret) {
              if (OB_FAIL(unique_ctx.set_refactored(hash_key))) {
                LOG_WARN("store rowkey failed", K(ret));
              }
            } else {
              LOG_WARN("check rowkey distinct failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPlanSet::check_vector_param_same_bool(const ObObjParam& param_obj, bool& first_val, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  const ObObj* array_data = NULL;
  if (param_obj.is_ext()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", K(first_val));
  }

  return ret;
}

// used for add plan
int ObPlanSet::match_params_info(
    const Ob2DArray<ObParamInfo, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator, false>& infos, int64_t outline_param_idx,
    const ObPlanCacheCtx& pc_ctx, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObSQLSessionInfo* session_info = pc_ctx.sql_ctx_.session_info_;
  ObSessionVariable sess_var;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null session_info", K(ret));
  } else if (false == is_match_outline_param(outline_param_idx)) {
    is_same = false;
  } else if (infos.count() != params_info_.count()) {
    is_same = false;
  } else {
    int64_t N = infos.count();
    for (int64_t i = 0; is_same && i < N; ++i) {
      if (true == is_same && params_info_.at(i).flag_.need_to_check_type_) {
        if (infos.at(i).type_ != params_info_.at(i).type_ || infos.at(i).scale_ != params_info_.at(i).scale_ ||
            (params_info_.at(i).flag_.need_to_check_extend_type_ &&
                infos.at(i).ext_real_type_ != params_info_.at(i).ext_real_type_)) {
          is_same = false;
        }
      }
      if (true == is_same && params_info_.at(i).flag_.need_to_check_bool_value_) {
        if (infos.at(i).flag_.expected_bool_value_ != params_info_.at(i).flag_.expected_bool_value_) {
          is_same = false;
        }
      }
    }

    if (OB_SUCC(ret) && is_same) {
      if (OB_ISNULL(pc_ctx.sql_ctx_.trans_happened_route_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("transform happend route is null", K(ret));
      } else {
        if (pc_ctx.sql_ctx_.trans_happened_route_->count() != trans_happened_route_.count()) {
          is_same = false;
          LOG_TRACE("trans happened route is diff",
              K(is_same),
              K(trans_happened_route_.count()),
              K(pc_ctx.sql_ctx_.trans_happened_route_->count()));
        } else {
          for (int64_t i = 0; is_same && i < trans_happened_route_.count(); i++) {
            if (pc_ctx.sql_ctx_.trans_happened_route_->at(i) != trans_happened_route_.at(i)) {
              is_same = false;
            }
          }  // for end
        }
      }
    }

    if (is_same && related_user_var_names_.count() > 0) {
      if (related_user_var_names_.count() != pc_ctx.sql_ctx_.related_user_var_names_.count()) {
        is_same = false;
      } else {
        int64_t CNT = related_user_var_names_.count();
        for (int64_t i = 0; OB_SUCC(ret) && is_same && i < CNT; i++) {
          if (related_user_var_names_.at(i) != pc_ctx.sql_ctx_.related_user_var_names_.at(i)) {
            is_same = false;
          } else if (OB_FAIL(session_info->get_user_variable(related_user_var_names_.at(i), sess_var))) {
            LOG_WARN("failed to get user variable", K(ret), K(sess_var));
          } else {
            is_same = (sess_var.meta_ == related_user_sess_var_metas_.at(i));
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_same) {
      CK(OB_NOT_NULL(pc_ctx.exec_ctx_.get_physical_plan_ctx()));
      if (OB_SUCC(ret)) {
        const ParamStore& params = pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
        OC((match_constraint)(params, is_same));
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

void ObPlanSet::reset()
{
  ObDLinkBase<ObPlanSet>::reset();
  plan_cache_value_ = NULL;
  params_info_.reset();
  stmt_type_ = stmt::T_NONE;
  fetch_cur_time_ = false;
  is_ignore_stmt_ = false;
  // is_wise_join_ = false;
  outline_param_idx_ = OB_INVALID_INDEX;

  related_user_var_names_.reset();
  related_user_sess_var_metas_.reset();

  all_possible_const_param_constraints_.reset();
  all_plan_const_param_constraints_.reset();
  all_equal_param_constraints_.reset();
  change_char_index_.reset();
  alloc_.reset();
}

ObPlanCache* ObPlanSet::get_plan_cache() const
{
  ObPlanCache* pc = NULL;
  if (NULL == plan_cache_value_ || NULL == plan_cache_value_->get_pcv_set() ||
      NULL == plan_cache_value_->get_pcv_set()->get_plan_cache()) {
    pc = NULL;
  } else {
    pc = plan_cache_value_->get_pcv_set()->get_plan_cache();
  }
  return pc;
}

int ObPlanSet::init_new_set(
    const ObPlanCacheCtx& pc_ctx, const ObCacheObject& plan, int64_t outline_param_idx, common::ObIAllocator* pc_alloc_)
{
  int ret = OB_SUCCESS;
  ObPlanCache* pc = nullptr;
  const ObSqlCtx& sql_ctx = pc_ctx.sql_ctx_;
  const ObSQLSessionInfo* session_info = sql_ctx.session_info_;
  if (OB_ISNULL(pc = get_plan_cache()) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null plan cache or session info", K(ret), K(pc), K(session_info));
  } else {
    alloc_.set_tenant_id(pc->get_tenant_id());
    alloc_.set_ctx_id(ObCtxIds::PLAN_CACHE_CTX_ID);
  }
  if (OB_SUCC(ret)) {
    // set outline_param_idx
    outline_param_idx_ = outline_param_idx;
    char* buf = NULL;
    ObString var_name;
    ObSessionVariable sess_var;

    fetch_cur_time_ = plan.get_fetch_cur_time();
    stmt_type_ = plan.get_stmt_type();
    is_ignore_stmt_ = plan.is_ignore();
    // add param info
    params_info_.reset();

    if (OB_FAIL(init_pre_calc_exprs(plan, session_info->use_static_typing_engine(), pc_alloc_))) {
      LOG_WARN("failed to init pre calc exprs", K(ret));
    } else if (OB_FAIL(params_info_.reserve(plan.get_params_info().count()))) {
      LOG_WARN("failed to reserve 2d array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < plan.get_params_info().count(); ++i) {
      if (OB_FAIL(params_info_.push_back(plan.get_params_info().at(i)))) {
        SQL_PC_LOG(WARN, "fail to push back param info", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(change_char_index_.add_members2(pc_ctx.change_char_index_))) {
      LOG_WARN("failed to add bitset membsers", K(ret));
    }

    // add user session vars if necessary
    CK(OB_NOT_NULL(sql_ctx.session_info_));
    if (OB_SUCC(ret) && sql_ctx.related_user_var_names_.count() > 0) {
      related_user_var_names_.reset();
      related_user_var_names_.set_allocator(&alloc_);
      related_user_sess_var_metas_.reset();
      related_user_sess_var_metas_.set_allocator(&alloc_);

      int64_t N = sql_ctx.related_user_var_names_.count();
      OZ(related_user_var_names_.init(N), N);
      OZ(related_user_sess_var_metas_.init(N), N);
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_ctx.related_user_var_names_.count(); i++) {
        buf = (char*)alloc_.alloc(sql_ctx.related_user_var_names_.at(i).length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(sql_ctx.related_user_var_names_.at(i).length()));
        } else {
          MEMCPY(buf, sql_ctx.related_user_var_names_.at(i).ptr(), sql_ctx.related_user_var_names_.at(i).length());
          var_name.assign_ptr(buf, sql_ctx.related_user_var_names_.at(i).length());
          OC((related_user_var_names_.push_back)(var_name));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < related_user_var_names_.count(); i++) {
        OZ(sql_ctx.session_info_->get_user_variable(related_user_var_names_.at(i), sess_var),
            ret,
            related_user_var_names_.at(i),
            i);
        OC((related_user_sess_var_metas_.push_back)(sess_var.meta_));
      }

      if (OB_FAIL(ret)) {
        related_user_var_names_.reset();
        related_user_sess_var_metas_.reset();
      }
    }

    // init const param constriants
    ObPlanSetType ps_t = get_plan_set_type_by_cache_obj_type(plan.get_type());
    if (PST_PRCD == ps_t) {
      // pl does not have any const param constraint
      all_possible_const_param_constraints_.reset();
      all_plan_const_param_constraints_.reset();
      all_equal_param_constraints_.reset();
      trans_happened_route_.reset();
    } else if (PST_SQL_CRSR == ps_t) {
      // otherwise it should not be empty
      CK(OB_NOT_NULL(sql_ctx.all_plan_const_param_constraints_),
          OB_NOT_NULL(sql_ctx.all_possible_const_param_constraints_),
          OB_NOT_NULL(sql_ctx.all_equal_param_constraints_),
          OB_NOT_NULL(sql_ctx.trans_happened_route_));
      OZ((set_const_param_constraint)(*sql_ctx.all_plan_const_param_constraints_, false));
      OZ((set_const_param_constraint)(*sql_ctx.all_possible_const_param_constraints_, true));
      OZ((set_equal_param_constraint)(*sql_ctx.all_equal_param_constraints_));
      OZ(trans_happened_route_.assign(*sql_ctx.trans_happened_route_));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected plan set type", K(ps_t), K(plan.get_type()));
    }

    // initialize multi_stmt rowkey pos
    if (OB_SUCC(ret) && sql_ctx.multi_stmt_rowkey_pos_.count() > 0) {
      if (OB_FAIL(multi_stmt_rowkey_pos_.init(sql_ctx.multi_stmt_rowkey_pos_.count()))) {
        LOG_WARN("failed to init array count", K(ret));
      } else if (OB_FAIL(append(multi_stmt_rowkey_pos_, sql_ctx.multi_stmt_rowkey_pos_))) {
        LOG_WARN("failed to append multi stmt rowkey pos", K(ret));
      } else { /*do nothing*/
      }
    }
  }

  return ret;
}

int ObPlanSet::set_const_param_constraint(
    ObIArray<ObPCConstParamInfo>& const_param_constraint, const bool is_all_constraint)
{
  int ret = OB_SUCCESS;
  ConstParamConstraint& cons_array =
      (is_all_constraint ? all_possible_const_param_constraints_ : all_plan_const_param_constraints_);
  cons_array.reset();
  cons_array.set_allocator(&alloc_);

  if (const_param_constraint.count() > 0) {
    if (OB_FAIL(cons_array.init(const_param_constraint.count()))) {
      LOG_WARN("failed to init const param constraint array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < const_param_constraint.count(); i++) {
        ObPCConstParamInfo& tmp_info = const_param_constraint.at(i);
        if (tmp_info.const_idx_.count() <= 0 || tmp_info.const_params_.count() <= 0 ||
            tmp_info.const_idx_.count() != tmp_info.const_params_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected const param info", K(tmp_info.const_idx_), K(tmp_info.const_params_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < tmp_info.const_params_.count(); i++) {
            if (tmp_info.const_params_.at(i).need_deep_copy()) {
              const ObObj& src_obj = tmp_info.const_params_.at(i);
              int64_t deep_cp_size = tmp_info.const_params_.at(i).get_deep_copy_size();
              int64_t pos = 0;
              char* tmp_buf = NULL;

              if (OB_ISNULL(tmp_buf = (char*)alloc_.alloc(deep_cp_size))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to allocate mem", K(ret));
              } else if (OB_FAIL(tmp_info.const_params_.at(i).deep_copy(src_obj, tmp_buf, deep_cp_size, pos))) {
                LOG_WARN("failed to deep copy obj", K(ret));
              } else if (pos != deep_cp_size) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("deep copy went wrong", K(ret));
              } else {
                // do nothing
              }
            }
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(cons_array.push_back(tmp_info))) {
            LOG_WARN("failed to push back element", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    cons_array.reset();
  }
  return ret;
}

int ObPlanSet::set_equal_param_constraint(common::ObIArray<ObPCParamEqualInfo>& equal_param_constraint)
{
  int ret = OB_SUCCESS;
  all_equal_param_constraints_.reset();
  all_equal_param_constraints_.set_allocator(&alloc_);
  if (equal_param_constraint.empty()) {
    // do nothing
  } else if (OB_FAIL(all_equal_param_constraints_.init(equal_param_constraint.count()))) {
    LOG_WARN("failed to init equal param constraint array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_param_constraint.count(); ++i) {
    ObPCParamEqualInfo& equal_info = equal_param_constraint.at(i);
    if (equal_info.first_param_idx_ < 0 || equal_info.second_param_idx_ < 0 ||
        equal_info.first_param_idx_ == equal_info.second_param_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid equal param constraint", K(ret), K(equal_info));
    } else if (OB_FAIL(all_equal_param_constraints_.push_back(equal_info))) {
      LOG_WARN("failed to push back equal param info", K(ret));
    }
  }
  return ret;
}

int ObPlanSet::match_constraint(const ParamStore& params, bool& is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = true;

  if (all_plan_const_param_constraints_.count() > 0) {  // check all_plan_const_param_constraints_ first
    for (int64_t i = 0; is_matched && OB_SUCC(ret) && i < all_plan_const_param_constraints_.count(); i++) {
      const ObPCConstParamInfo& const_param_info = all_plan_const_param_constraints_.at(i);
      CK(const_param_info.const_idx_.count() > 0,
          const_param_info.const_params_.count() > 0,
          const_param_info.const_idx_.count() == const_param_info.const_params_.count());

      for (int64_t j = 0; is_matched && OB_SUCC(ret) && j < const_param_info.const_idx_.count(); j++) {
        const int64_t param_idx = const_param_info.const_idx_.at(j);
        const ObObj& const_param = const_param_info.const_params_.at(j);
        if (param_idx >= params.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get an unexpected param index", K(ret), K(param_idx), K(params.count()));
        } else if (const_param.is_invalid_type() || params.at(param_idx).is_invalid_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "get unexpected invalid type", K(ret), K(const_param.get_type()), K(params.at(param_idx).get_type()));
        } else if (!const_param.can_compare(params.at(param_idx)) || 0 != const_param.compare(params.at(param_idx))) {
          LOG_DEBUG("not matched const param", K(const_param), K(params.at(param_idx)));
          is_matched = false;
        } else {
          // do nothing
        }
      }
    }
  } else if (all_possible_const_param_constraints_.count() > 0) {
    // check if possible generated column exists
    for (int64_t i = 0; is_matched && OB_SUCC(ret) && i < all_possible_const_param_constraints_.count(); i++) {
      bool match_const = true;
      const ObPCConstParamInfo& const_param_info = all_possible_const_param_constraints_.at(i);
      CK(const_param_info.const_idx_.count() > 0,
          const_param_info.const_params_.count() > 0,
          const_param_info.const_idx_.count() == const_param_info.const_params_.count());
      for (int64_t j = 0; match_const && OB_SUCC(ret) && j < const_param_info.const_idx_.count(); j++) {
        const int64_t param_idx = const_param_info.const_idx_.at(j);
        const ObObj& const_param = const_param_info.const_params_.at(j);
        if (param_idx >= params.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get an unexpected param index", K(ret), K(param_idx), K(params.count()));
        } else if (const_param.is_invalid_type() || params.at(param_idx).is_invalid_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "get unexpected invalid type", K(ret), K(const_param.get_type()), K(params.at(param_idx).get_type()));
        } else if (!const_param.can_compare(params.at(param_idx)) || 0 != const_param.compare(params.at(param_idx))) {
          match_const = false;
        } else {
          // do nothing
        }
      }
      if (match_const) {
        LOG_DEBUG("matched const param constraint", K(params), K(all_possible_const_param_constraints_.at(i)));
        is_matched = false;  // matching one of the constraint, need to generated new plan
      }
    }
  } else {
    // do nothing
  }
  for (int64_t i = 0; is_matched && OB_SUCC(ret) && i < all_equal_param_constraints_.count(); ++i) {
    int64_t first_idx = all_equal_param_constraints_.at(i).first_param_idx_;
    int64_t second_idx = all_equal_param_constraints_.at(i).second_param_idx_;
    if (OB_UNLIKELY(first_idx < 0 || first_idx >= params.count() || second_idx < 0 || second_idx >= params.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param index is invalid", K(ret), K(params.count()), K(first_idx), K(second_idx));
    } else if (params.at(first_idx).can_compare(params.at(second_idx)) &&
               params.at(first_idx).get_collation_type() == params.at(second_idx).get_collation_type()) {
      is_matched = (0 == params.at(first_idx).compare(params.at(second_idx)));
    }
    if (OB_SUCC(ret) && !is_matched) {
      is_matched = false;
      LOG_DEBUG("not match equal param constraint", K(params), K(first_idx), K(second_idx));
    }
  }
  if (OB_FAIL(ret)) {
    is_matched = false;
  }

  return ret;
}

int ObPlanSet::init_pre_calc_exprs(
    const ObCacheObject& phy_plan, const bool is_use_static_engine, common::ObIAllocator* pc_alloc_)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;

  if (phy_plan.get_pre_calc_exprs().get_size() == 0 && phy_plan.get_pre_calc_frames().get_size() == 0) {
    // have no pre calculable expression, not initalize pre calculable expression handle.
    pre_cal_expr_handler_ = NULL;
  } else if (OB_ISNULL(pc_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan cache allocator has not been initialized.");
  } else if (OB_ISNULL(buf = pc_alloc_->alloc(sizeof(PreCalcExprHandler)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory.", K(ret));
  } else if (FALSE_IT(pre_cal_expr_handler_ = new (buf) PreCalcExprHandler())) {
    // do nothing
  } else if (FALSE_IT(pre_cal_expr_handler_->init(phy_plan.get_tenant_id(), pc_alloc_))) {
    // do nothing
  } else if (is_use_static_engine) {
    buf = NULL;
    common::ObIAllocator& pre_expr_alloc = (pre_cal_expr_handler_->alloc_);

    if (OB_ISNULL(buf = pre_expr_alloc.alloc(sizeof(common::ObDList<ObPreCalcExprFrameInfo>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else {
      pre_cal_expr_handler_->pre_calc_frames_ = new (buf) common::ObDList<ObPreCalcExprFrameInfo>;

      common::ObDList<ObPreCalcExprFrameInfo>* pre_calc_frames = pre_cal_expr_handler_->pre_calc_frames_;
      ObPreCalcExprFrameInfo* pre_calc_frame = NULL;
      void* frame_buf = NULL;
      DLIST_FOREACH(frame, phy_plan.get_pre_calc_frames())
      {
        if (OB_ISNULL(frame)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null frame", K(ret));
        } else if (OB_ISNULL(frame_buf = pre_expr_alloc.alloc(sizeof(ObPreCalcExprFrameInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (FALSE_IT(pre_calc_frame = new (frame_buf) ObPreCalcExprFrameInfo(pre_expr_alloc))) {
          // do nothing
        } else if (OB_FAIL(pre_calc_frame->assign(*frame, pre_expr_alloc))) {
          LOG_WARN("failed to deep copy pre calc frame", K(ret));
        } else if (OB_UNLIKELY(!pre_calc_frames->add_last(pre_calc_frame))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to add element to dlist", K(ret));
        } else {
          frame_buf = NULL;
          pre_calc_frame = NULL;
        }
      }
      // set expr list of old engine nullptr
      pre_cal_expr_handler_->pre_calc_exprs_ = NULL;
    }
  } else {
    buf = NULL;
    common::ObIAllocator& pre_expr_alloc = pre_cal_expr_handler_->alloc_;

    if (OB_ISNULL(buf = pre_expr_alloc.alloc(sizeof(common::ObDList<ObSqlExpression>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else {
      pre_cal_expr_handler_->pre_calc_exprs_ = new (buf) common::ObDList<ObSqlExpression>;
      common::ObDList<ObSqlExpression>* pre_calc_exprs_ = pre_cal_expr_handler_->pre_calc_exprs_;
      ObSqlExpressionFactory factory(pre_expr_alloc);
      const ObDList<ObSqlExpression>& exprs = phy_plan.get_pre_calc_exprs();
      ObSqlExpression* new_expr = NULL;
      DLIST_FOREACH(node, exprs)
      {
        if (OB_FAIL(factory.alloc(new_expr))) {
          LOG_WARN("failed to alloc expr", K(ret));
        } else if (OB_ISNULL(new_expr) || OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null sql expr", K(ret), K(new_expr), K(node));
        } else {
          LOG_DEBUG("cp pre calc expr", K(*node));
          if (OB_FAIL(new_expr->assign(*node))) {
            LOG_WARN("failed to copy expr", K(ret));
          } else if (OB_UNLIKELY(!pre_calc_exprs_->add_last(new_expr))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add pre calculable expr", K(ret));
          } else {
            // do nothing
          }
          new_expr = NULL;
        }
      }
      // set expr list of new engine nullptr
      pre_cal_expr_handler_->pre_calc_frames_ = NULL;
    }
  }
  return ret;
}

int ObPlanSet::pre_calc_exprs(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;

  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  if (OB_ISNULL(pre_cal_expr_handler_)) {
    // have no pre-calculable expression, do nothing
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
    // for new engine
  } else if (session->use_static_typing_engine() && OB_NOT_NULL(pre_cal_expr_handler_->pre_calc_frames_)) {
    DLIST_FOREACH(pre_calc_frame, *(pre_cal_expr_handler_->pre_calc_frames_))
    {
      if (OB_ISNULL(pre_calc_frame)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pre calc frame", K(ret));
      } else if (OB_FAIL(ObCacheObject::pre_calculation(is_ignore_stmt_, *pre_calc_frame, exec_ctx))) {
        LOG_WARN("failed to pre calculate", K(ret));
      } else {
        // do nothing
      }
    }
    // for old engine
  } else if (!(session->use_static_typing_engine()) && OB_NOT_NULL(pre_cal_expr_handler_->pre_calc_exprs_) &&
             OB_FAIL(ObPhysicalPlan::pre_calculation(
                 stmt_type_, is_ignore_stmt_, *(pre_cal_expr_handler_->pre_calc_exprs_), exec_ctx))) {
    LOG_WARN("failed to pre calculate", K(ret));
  }
  return ret;
}

int ObSqlPlanSet::add_cache_obj(ObCacheObject& cache_object, ObPlanCacheCtx& pc_ctx, int64_t ol_param_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!cache_object.is_sql_crsr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cache_object type is invalid", K(cache_object.get_type()));
  } else {
    ret = add_plan(static_cast<ObPhysicalPlan&>(cache_object), pc_ctx, ol_param_idx);
  }
  // ref_cnt++;
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add plan.", K(ret));
  } else if (OB_ISNULL(pre_cal_expr_handler_)) {
    // have no pre-calculable expression, do nothing
  } else {
    cache_object.set_pre_calc_expr_handler(pre_cal_expr_handler_);
    cache_object.inc_pre_expr_ref_count();
    // planset, handle, plan, ref_cnt(val)
    LOG_DEBUG("add pre calculable expression.",
        KP(this),
        KP(cache_object.get_pre_calc_expr_handler()),
        KP(&cache_object),
        K(cache_object.get_pre_expr_ref_count()));
  }
  return ret;
}

int ObSqlPlanSet::add_plan(ObPhysicalPlan& plan, ObPlanCacheCtx& pc_ctx, int64_t outline_param_idx)
{
  int ret = OB_SUCCESS;
  ObSqlCtx& sql_ctx = pc_ctx.sql_ctx_;
  ObSEArray<ObPhyTableLocation, 4> phy_locations;
  ObSEArray<ObPhyTableLocationInfo, 4> phy_location_infos;
  ObPhyPlanType plan_type = OB_PHY_PLAN_UNINITIALIZED;
  loc_sensitive_hint_ = plan.loc_sensitive_hint_;
  if (OB_ISNULL(plan_cache_value_) || OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "invalid argument", KP(plan_cache_value_), K(ret));
  } else if (OB_FAIL(get_phy_locations(sql_ctx.partition_infos_, phy_locations, phy_location_infos))) {
    LOG_WARN("fail to get physical locations", K(ret));
  } else if (OB_FAIL(set_concurrent_degree(outline_param_idx, plan))) {
    LOG_WARN("fail to check oncurrent degree", K(ret));
  } else {
    if (pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_or_expand_transformed()) {
      need_try_plan_ |= TRY_PLAN_OR_EXPAND;
    }
    if (plan.get_is_late_materialized()) {
      need_try_plan_ |= TRY_PLAN_LATE_MAT;
    }
    if (plan.has_uncertain_local_operator()) {
      need_try_plan_ |= TRY_PLAN_UNCERTAIN;
    }
    if (plan.get_is_contain_global_index()) {
      need_try_plan_ |= TRY_PLAN_GLOBAL_INDEX;
    }
    plan_type = plan.get_plan_type();
    if (OB_SUCC(ret)) {
      switch (plan_type) {
        case OB_PHY_PLAN_LOCAL: {
          SQL_PC_LOG(DEBUG, "plan set add plan, local plan", K(ret));
          if (/*has_array_binding_ ||*/ is_multi_stmt_plan()) {
            if (NULL != array_binding_plan_) {
              ret = OB_SQL_PC_PLAN_DUPLICATE;
            } else {
              array_binding_plan_ = &plan;
            }
          } else {
            local_plan_ = &plan;
            local_phy_locations_.reset();
            if (OB_FAIL(init_phy_location(phy_locations.count()))) {
              SQL_PC_LOG(WARN, "init phy location failed");
            } else if (OB_FAIL(local_phy_locations_.assign(phy_locations))) {
              SQL_PC_LOG(WARN, "fail to assign phy locations");
            }
            LOG_DEBUG("local phy locations", K(local_phy_locations_));
          }
        } break;
        case OB_PHY_PLAN_REMOTE: {
          SQL_PC_LOG(DEBUG, "plan set add plan, remote plan", K(ret), K(remote_plan_));
          if (NULL != remote_plan_) {
            ret = OB_SQL_PC_PLAN_DUPLICATE;
          } else {
            remote_plan_ = &plan;
          }
        } break;
        case OB_PHY_PLAN_DISTRIBUTED: {
          SQL_PC_LOG(DEBUG, "plan set add plan, distr plan", K(ret));
          bool is_single_table = (1 == phy_location_infos.count());
          int64_t need_try_plan = is_multi_stmt_plan() ? 0 : need_try_plan_;
          if (OB_FAIL(dist_plans_.add_plan(is_single_table, need_try_plan, plan, pc_ctx))) {
            LOG_WARN("failed to add dist plan", K(ret), K(is_single_table), K(plan));
          } else {
            SQL_PC_LOG(DEBUG, "plan added to dist plan list", K(ret));
          }
        } break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_PC_LOG(WARN, "unknown plan type", K(plan_type), K(ret));
          break;
      }
    }
  }
  if (stmt::T_SELECT == stmt_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_location_infos.count(); ++i) {
      const ObPhyTableLocationInfo& PhyTblLocInfo = phy_location_infos.at(i);
      if (PhyTblLocInfo.is_duplicate_table_not_in_dml()) {
        has_duplicate_table_ = true;
        break;
      }
    }
  }
  SQL_PC_LOG(
      DEBUG, "plan set add plan", K(ret), K(&plan), "plan type ", plan_type, K(has_duplicate_table_), K(stmt_type_));
  // increase plan ref_count,
  // if plan doesn't add in plan cache,don't increase ref_count;
  bool real_add = OB_PHY_PLAN_LOCAL != plan_type || pc_ctx.need_real_add_;
  if (OB_SUCCESS == ret && real_add) {
    CacheRefHandleID plan_handle;
    if (array_binding_plan_ == &plan) {
      plan_handle = PC_REF_PLAN_ARR_HANDLE;
    } else {
      plan_handle = OB_PHY_PLAN_LOCAL == plan_type    ? PC_REF_PLAN_LOCAL_HANDLE
                    : OB_PHY_PLAN_REMOTE == plan_type ? PC_REF_PLAN_REMOTE_HANDLE
                                                      : PC_REF_PLAN_DIST_HANDLE;
    }
    plan.inc_ref_count(plan_handle);
    if (phy_location_infos.count() > 0) {
      if (phy_location_infos.at(0).get_phy_part_loc_info_list().count() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part loc info list is empty", K(phy_location_infos.at(0)), K(ret));
      } else if (OB_FAIL(phy_location_infos.at(0)
                             .get_phy_part_loc_info_list()
                             .at(0)
                             .get_partition_location()
                             .get_partition_key(partition_key_))) {
        LOG_WARN("fail to get partition key", K(ret));
      }
    }
  }

  return ret;
}

int ObSqlPlanSet::init_new_set(
    const ObPlanCacheCtx& pc_ctx, const ObCacheObject& plan, int64_t outline_param_idx, common::ObIAllocator* pc_alloc_)
{
  int ret = OB_SUCCESS;
  const ObSqlCtx& sql_ctx = pc_ctx.sql_ctx_;
  // set outline_param_idx
  outline_param_idx_ = outline_param_idx;
  need_try_plan_ = 0;
  has_duplicate_table_ = false;
  if (OB_ISNULL(pc_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pc_allocator has not been initialized.", K(ret));
  } else if (OB_FAIL(ObPlanSet::init_new_set(pc_ctx, plan, outline_param_idx, pc_alloc_))) {
    LOG_WARN("init new set failed", K(ret));
  } else if (OB_FAIL(dist_plans_.init(this))) {
    SQL_PC_LOG(WARN, "failed to init dist plans", K(ret));
  } else {
    // if (pc_ctx.sql_ctx_.multi_stmt_rowkey_pos_.empty()) {
    // for (int64_t i = 0; i < plan.get_params_info().count(); ++i) {
    // if (ObExtendType == plan.get_params_info().at(i).type_) {
    // has_array_binding_ = true;
    //}
    //}
    //}
    for (int64_t i = 0; !is_contain_virtual_table_ && i < plan.get_dependency_table().count(); i++) {
      const ObSchemaObjVersion& schema_obj = plan.get_dependency_table().at(i);
      if (TABLE_SCHEMA == schema_obj.get_schema_type() && is_virtual_table(extract_pure_id(schema_obj.object_id_))) {
        is_contain_virtual_table_ = true;
        LOG_DEBUG("contain virtual table", K(is_contain_virtual_table_), K(schema_obj));
      }
    }  // for end
  }

  bool is_contain_global_index = false;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (T_CO_SQL_CRSR != plan.get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cache object type", K(ret), K(plan.get_type()));
  } else {
    const ObPhysicalPlan& sql_plan = dynamic_cast<const ObPhysicalPlan&>(plan);
    enable_inner_part_parallel_exec_ = sql_plan.get_px_dop() > 1;
    is_contain_global_index = sql_plan.get_is_contain_global_index();
    LOG_DEBUG("using px", K(enable_inner_part_parallel_exec_));
  }
  if (OB_SUCC(ret) && (!is_contain_global_index || is_multi_stmt_plan())) {
    const ObTablePartitionInfoArray& partition_infos = sql_ctx.partition_infos_;
    int64_t N = partition_infos.count();
    // copy table location
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (NULL == partition_infos.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(DEBUG, "invalid partition info");
      } else if (OB_FAIL(table_locations_.push_back(partition_infos.at(i)->get_table_location()))) {
        SQL_PC_LOG(WARN, "fail to push table location", K(ret));
      } else if (is_all_non_partition_ && partition_infos.at(i)->get_table_location().is_partitioned()) {
        is_all_non_partition_ = false;
      }
      if (OB_SUCC(ret) && OB_FAIL(init_fast_calc_part_ids_info())) {
        LOG_WARN("fail to init fast calc part ids info", K(ret));
      }
    }  // for end
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(plan_cache_value_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(plan_cache_value_));
    } else if (OB_ISNULL(plan_cache_value_->get_pc_alloc())) {
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(ObPlanBaselineHeler::init_baseline_params_info_str(
                   plan.get_params_info(), *plan_cache_value_->get_pc_alloc(), params_info_str_))) {
      LOG_WARN("failed to init baseline params info str", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObSqlPlanSet::select_plan(
    ObIPartitionLocationCache* location_cache, ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* plan = NULL;

  if (OB_ISNULL(location_cache) || OB_ISNULL(plan_cache_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location cache not init", K(location_cache), K(plan_cache_value_), K(ret));
  } else if (0 == need_try_plan_ || is_multi_stmt_plan()) {
    if (OB_FAIL(get_plan_normal(*location_cache, pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret) {
        LOG_DEBUG("fail to get plan normal", K(ret));
      } else {
        LOG_WARN("fail to get plan normal", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_plan_special(*location_cache, pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret) {
        LOG_DEBUG("fail to get plan special", K(ret));
      } else {
        LOG_WARN("fail to get plan special", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(plan) && plan->is_remote_plan()) {
    pc_ctx.not_param_index_.reset();
    pc_ctx.neg_param_index_.reset();
    if (OB_FAIL(pc_ctx.not_param_index_.add_members2(plan_cache_value_->get_not_param_index()))) {
      LOG_WARN("assign not param info failed", K(ret), K(plan_cache_value_->get_not_param_index()));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(plan_cache_value_->get_neg_param_index()))) {
      LOG_WARN("assign neg param index failed", K(ret), K(plan_cache_value_->get_neg_param_index()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(plan->is_limited_concurrent_num())) {
    if (OB_FAIL(plan->inc_concurrent_num())) {
      if (OB_REACH_MAX_CONCURRENT_NUM == ret) {
        LOG_USER_ERROR(OB_REACH_MAX_CONCURRENT_NUM, plan->get_max_concurrent_num());
      } else {
        LOG_WARN("fail to inc concurrent num", K(ret));
      }
    }
  } else { /*do nothing*/
  }

  if (OB_SQL_PC_NOT_EXIST == ret) {
    pc_ctx.sql_ctx_.loc_sensitive_hint_ = loc_sensitive_hint_;
  }

  if (OB_FAIL(ret)) {
    if (NULL != plan) {
      ObCacheObjectFactory::free(plan, pc_ctx.handle_id_);
      plan = NULL;
    }
  }

  if (WAY_PLAN_BASELINE == pc_ctx.gen_plan_way_ && NULL == plan) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = ob_write_string(pc_ctx.allocator_, params_info_str_, pc_ctx.bl_key_.params_info_str_))) {
      pc_ctx.gen_plan_way_ = WAY_DEPENDENCE_ENVIRONMENT;
      LOG_WARN("fail to copy params info str", K(ret), K(tmp_ret));
    }
  }
  if (OB_SUCC(ret)) {
    cache_obj = plan;
  }
  return ret;
}

int ObSqlPlanSet::get_local_plan_direct(ObPlanCacheCtx& pc_ctx, bool& is_direct_local_plan, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  is_direct_local_plan = false;
  int64_t N = table_locations_.count();
  bool is_all_non_partition = true;
  bool is_leader = false;
  ObSEArray<TablePart, 4> table_parts;
  ObExecContext& exec_ctx = pc_ctx.exec_ctx_;
  ObSchemaGetterGuard* schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(pc_ctx.sql_ctx_.session_info_);
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  if (OB_ISNULL(schema_guard) || OB_ISNULL(dtc_params.tz_info_) || OB_ISNULL(plan_ctx) ||
      OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        KP(schema_guard),
        KP(dtc_params.tz_info_),
        KP(plan_ctx),
        KP(pc_ctx.sql_ctx_.session_info_),
        K(ret));
  }

  ObSEArray<int64_t, 1> partition_ids;
  for (int64_t i = 0; OB_SUCC(ret) && is_all_non_partition && i < N; ++i) {
    ObPGKey pg_key;
    uint64_t tg_id = OB_INVALID_ID;
    int64_t pg_id = OB_INVALID_ID;
    const ObTableLocation& table_location = table_locations_.at(i);
    if (OB_FAIL(table_location.calculate_partition_ids(
            exec_ctx, schema_guard, plan_ctx->get_param_store(), partition_ids, dtc_params))) {
      LOG_WARN("fail to get partition ids", K(ret));
    } else if (partition_ids.count() != 1) {
      is_all_non_partition = false;
    } else if (OB_FAIL(ObSqlPartitionLocationCache::get_phy_key(
                   *schema_guard, table_location.get_ref_table_id(), partition_ids.at(0), tg_id, pg_id, pg_key))) {
      LOG_WARN("fail to get pg key", K(ret));
    } else if (OB_FAIL(table_parts.push_back(TablePart(
                   table_location.get_table_id(), table_location.get_ref_table_id(), partition_ids.at(0), pg_key)))) {
      LOG_WARN("fail to push table part", K(ret));
    }
    partition_ids.reuse();
  }

  if (OB_SUCC(ret) && true == is_all_non_partition) {  // get local plan directly
    ObPartitionKey partition_key = partition_key_;
    if (OB_FAIL(gen_partition_key(table_parts, partition_key))) {
      LOG_WARN("fail to gen partition key", K(ret));
    } else if (OB_FAIL(check_partition_status(partition_key, is_leader))) {
      LOG_WARN("check partition status failed", K(ret), K(partition_key));
    } else if (!is_leader) {
      is_direct_local_plan = false;
    } else if (OB_ISNULL(local_plan_)) {
      SQL_PC_LOG(DEBUG, "get local plan failed", K(ret));
    } else {
      local_plan_->inc_ref_count(pc_ctx.handle_id_);
      plan = local_plan_;
    }
    if (OB_SUCC(ret) && plan != NULL) {
      int last_retry_err = pc_ctx.sql_ctx_.session_info_->get_retry_info().get_last_query_retry_err();
      if (plan->is_last_open_succ()) {
        is_direct_local_plan = true;
      } else if (pc_ctx.sql_ctx_.session_info_->get_is_in_retry() && is_local_plan_opt_allowed(last_retry_err)) {
        is_direct_local_plan = true;
      } else {
        is_direct_local_plan = false;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_direct_local_plan) {
    ObPhyTableLocationIArray& phy_locations = exec_ctx.get_task_exec_ctx().get_table_locations();
    if (OB_FAIL(phy_locations.assign(local_phy_locations_))) {  // copy phy table locations
      SQL_PC_LOG(WARN, "fail to assign phy locations", K(ret));
    } else if (OB_FAIL(replace_partition_id(phy_locations, table_parts))) {
      SQL_PC_LOG(WARN, "fail to replace partition id", K(ret));
    }
  }

  LOG_TRACE("get local plan direct", K(ret), KP(plan), K(is_direct_local_plan));

  if (OB_FAIL(ret)) {
    is_direct_local_plan = false;
  }
  exec_ctx.set_direct_local_plan(is_direct_local_plan);

  return ret;
}

int ObSqlPlanSet::gen_partition_key(ObIArray<TablePart>& table_parts, ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  if (table_parts.count() != 1) {
    // do nothing
  } else if (table_parts.at(0).ref_table_id_ != partition_key_.get_table_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table id", K(table_parts.at(0)), K(partition_key_.get_table_id()));
  } else if (OB_FAIL(partition_key.init(
                 partition_key_.get_table_id(), table_parts.at(0).part_id_, partition_key_.get_partition_cnt()))) {
    LOG_WARN("fail to gen partition key", K(ret));
  }

  return ret;
}

int ObSqlPlanSet::replace_partition_id(ObPhyTableLocationIArray& phy_locations, ObIArray<TablePart>& table_parts)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_locations.count(); i++) {
    ObPhyTableLocation& phy_tl = phy_locations.at(i);
    int64_t part_cnt = phy_tl.get_partition_location_list().count();
    int64_t part_id = OB_INVALID_ID;
    ObPGKey pg_key;
    if (1 != part_cnt) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid parition cnt", K(part_cnt), K(ret));
    } else if (OB_FAIL(get_partition_id(table_parts, phy_tl.get_table_location_key(), part_id, pg_key))) {
      LOG_WARN("fail to get partition id", K(ret));
    } else {
      phy_tl.get_partition_location_list().at(0).set_partition_id(part_id);
      if (OB_FAIL(phy_tl.get_partition_location_list().at(0).set_pg_key(pg_key))) {
        LOG_WARN("fail to set pg key", K(ret));
      }
    }
  }

  return ret;
}

int ObSqlPlanSet::get_partition_id(
    ObIArray<TablePart>& table_parts, int64_t table_id, int64_t& part_id, ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  part_id = OB_INVALID_ID;
  for (int64_t i = 0; !exist && i < table_parts.count(); i++) {
    if (table_parts.at(i).table_id_ == table_id) {
      part_id = table_parts.at(i).part_id_;
      pg_key = table_parts.at(i).pg_key_;
      exist = true;
    }
  }
  if (!exist) {
    ret = OB_NOT_EXIST_TABLE_TID;
    LOG_WARN("invalid table id", K(table_parts), K(table_id));
  }

  return ret;
}

int ObSqlPlanSet::get_plan_normal(
    ObIPartitionLocationCache& location_cache, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  bool is_direct_local_plan = false;
  ObSQLSessionInfo* session = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (session->has_remote_plan_in_tx()
             //|| has_array_binding_
             || enable_inner_part_parallel_exec_ || is_contain_virtual_table_ ||
             (session->get_is_in_retry() &&
                 !is_local_plan_opt_allowed(session->get_retry_info().get_last_query_retry_err())) ||
             is_multi_stmt_plan()) {
    // in retry, not get local plan direct
    is_direct_local_plan = false;
    LOG_DEBUG("do not use direct local plan",
        K(session->has_remote_plan_in_tx()),
        K(session->get_is_in_retry()),
        // K(has_array_binding_),
        K(is_contain_virtual_table_),
        K(enable_inner_part_parallel_exec_),
        K(is_multi_stmt_plan()));
  } else if (OB_FAIL(get_local_plan_direct(pc_ctx, is_direct_local_plan, plan))) {
    if (OB_SQL_PC_NOT_EXIST == ret) {
      LOG_DEBUG("fail to get local plan direct", K(ret));
    } else {
      LOG_WARN("fail to get local plan direct", K(ret));
    }
  }

  if (OB_SQL_PC_NOT_EXIST == ret || false == is_direct_local_plan || NULL == plan) {
    pc_ctx.exec_ctx_.set_direct_local_plan(false);
    ret = OB_SUCCESS;
    if (NULL != plan) {
      ObCacheObjectFactory::free(plan, pc_ctx.handle_id_);
      plan = NULL;
    }
    ObPhyPlanType plan_type = OB_PHY_PLAN_UNINITIALIZED;
    ObPartitionKey partition_key;
    typedef ObSEArray<ObSEArray<int64_t, 1024>, 4> PISS;
    typedef ObSEArray<ObPhyTableLocationInfo, 4> PLS;
    SMART_VAR(PISS, partition_idss)
    {
      SMART_VAR(PLS, phy_location_infos)
      {
        if (/*has_array_binding_ ||*/ is_multi_stmt_plan()) {
          if (OB_FAIL(get_array_bind_plan_type(pc_ctx, location_cache, plan_type, partition_idss))) {
            LOG_DEBUG("fail to get array bind plan type", K(ret));
          } else if (is_multi_stmt_plan()) {
            if (OB_UNLIKELY(1 != partition_idss.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected count", K(partition_idss.count()), K(ret));
            } else if (OB_ISNULL(pc_ctx.sql_ctx_.multi_stmt_item_.get_queries()) ||
                       OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (OB_UNLIKELY(
                           pc_ctx.sql_ctx_.multi_stmt_item_.get_queries()->count() != partition_idss.at(0).count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected array count",
                  K(partition_idss.at(0).count()),
                  K(pc_ctx.sql_ctx_.multi_stmt_item_.get_queries()->count()),
                  K(ret));
            } else if (OB_FAIL(pc_ctx.exec_ctx_.get_physical_plan_ctx()->set_batched_stmt_partition_ids(
                           partition_idss.at(0)))) {
              LOG_WARN("failed to set partition ids", K(ret));
            } else {
              LOG_DEBUG("batched multi stmt partition ids", K(partition_idss.at(0)));
            }
            if (OB_SUCC(ret) && OB_PHY_PLAN_REMOTE == plan_type && 0 != need_try_plan_) {
              plan_type = OB_PHY_PLAN_DISTRIBUTED;
            }
          }
          LOG_DEBUG("get from array binding plan", K(ret), K(plan_type), K(pc_ctx));
        } else if (enable_inner_part_parallel_exec_) {
          if (OB_FAIL(dist_plans_.get_plan(pc_ctx, need_try_plan_, location_cache, plan))) {
            LOG_DEBUG("failed to get px plan", K(ret));
          }
        } else if (OB_FAIL(
                       get_plan_type(table_locations_, false, pc_ctx, location_cache, phy_location_infos, plan_type))) {
          // ret = OB_SQL_PC_NOT_EXIST;
          SQL_PC_LOG(DEBUG, "failed to get plan type", K(ret));
        }

        if (OB_SUCC(ret) && OB_PHY_PLAN_LOCAL != plan_type && session->get_in_transaction() &&
            session->get_is_in_retry()) {
          session->set_has_remote_plan_in_tx(true);
          LOG_DEBUG("do not get local direct plan from now on", K(session->has_remote_plan_in_tx()));
        }
        if (OB_SUCC(ret) && !enable_inner_part_parallel_exec_) {
          NG_TRACE(get_plan_type_end);
          SQL_PC_LOG(DEBUG, "get plan type before select plan", K(ret), K(plan_type));
          switch (plan_type) {
            case OB_PHY_PLAN_LOCAL: {
              if (/*has_array_binding_||*/ is_multi_stmt_plan()) {
                if (NULL != array_binding_plan_) {
                  array_binding_plan_->inc_ref_count(pc_ctx.handle_id_);
                  plan = array_binding_plan_;
                }
              } else {
                if (OB_FAIL(get_partition_key(
                        phy_location_infos, pc_ctx.exec_ctx_.get_addr(), OB_PHY_PLAN_LOCAL, partition_key))) {
                  LOG_WARN("fail to get partition key", K(ret));
                } else if (OB_ISNULL(local_plan_)) {
                  SQL_PC_LOG(DEBUG, "get local plan failed", K(ret), K(plan_type));
                } else {
                  local_plan_->inc_ref_count(pc_ctx.handle_id_);
                  plan = local_plan_;
                }
              }
            } break;
            case OB_PHY_PLAN_REMOTE: {
              if (NULL != remote_plan_) {
                remote_plan_->inc_ref_count(pc_ctx.handle_id_);
                plan = remote_plan_;
              }
            } break;
            case OB_PHY_PLAN_DISTRIBUTED: {
              if (OB_FAIL(dist_plans_.get_plan(pc_ctx, need_try_plan_, location_cache, plan))) {
                if (OB_SQL_PC_NOT_EXIST == ret) {
                  LOG_DEBUG("fail to get dist plan", K(ret));
                } else {
                  LOG_WARN("fail to get dist plan", K(ret));
                }
              }
            } break;
            default:
              break;
          }
          if (NULL == plan) {
            ret = OB_SQL_PC_NOT_EXIST;
          }
        }
      }
    }
  }

  return ret;
}

int ObSqlPlanSet::get_partition_key(const ObIArray<ObPhyTableLocationInfo>& phy_location_infos, const ObAddr& addr,
    const ObPhyPlanType& plan_type, ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  bool has_local_part = false;
  if (1 != phy_location_infos.count()) {
    // do nothing
  } else if (OB_FAIL(ObSQLUtils::choose_best_partition_for_estimation(
                 phy_location_infos.at(0).get_phy_part_loc_info_list(), addr, partition_key, has_local_part))) {
    LOG_WARN("fail to choose best partition for estimation", K(ret));
  } else if (OB_PHY_PLAN_LOCAL == plan_type && false == has_local_part) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local plan doesn't have local partition", K(ret));
  }

  return ret;
}

int ObSqlPlanSet::get_plan_special(
    ObIPartitionLocationCache& location_cache, ObPlanCacheCtx& pc_ctx, ObPhysicalPlan*& plan)
{
  LOG_DEBUG("get plan special", K(need_try_plan_));
  int ret = OB_SUCCESS;
  plan = NULL;
  bool get_next = false;
  ObPartitionKey partition_key;
  ObPhyPlanType real_type = OB_PHY_PLAN_UNINITIALIZED;
  ObSEArray<ObPhyTableLocationInfo, 4> phy_location_infos;

  // try local plan
  phy_location_infos.reset();
  real_type = OB_PHY_PLAN_UNINITIALIZED;
  if (OB_ISNULL(local_plan_)) {
    LOG_DEBUG("local plan is null");
    get_next = true;
  } else if (OB_FAIL(get_plan_type(local_plan_->get_table_locations(),
                 local_plan_->has_uncertain_local_operator(),
                 pc_ctx,
                 location_cache,
                 phy_location_infos,
                 real_type))) {
    LOG_WARN("fail to get plan type", K(ret));
  } else if (OB_PHY_PLAN_LOCAL != real_type) {
    LOG_DEBUG("local type is not match", K(real_type));
    plan = NULL;
    get_next = true;
  } else {
    local_plan_->inc_ref_count(pc_ctx.handle_id_);
    plan = local_plan_;
  }

  // try remote plan
  if (OB_SUCC(ret) && get_next) {
    get_next = false;
    phy_location_infos.reset();
    real_type = OB_PHY_PLAN_UNINITIALIZED;
    if (OB_ISNULL(remote_plan_)) {
      LOG_DEBUG("remote plan is null");
      get_next = true;
    } else if (OB_FAIL(get_plan_type(remote_plan_->get_table_locations(),
                   remote_plan_->has_uncertain_local_operator(),
                   pc_ctx,
                   location_cache,
                   phy_location_infos,
                   real_type))) {
      LOG_WARN("fail to get plan type", K(ret));
    } else if (OB_PHY_PLAN_REMOTE != real_type) {
      LOG_DEBUG("remote type is not match", K(real_type));
      plan = NULL;
      get_next = true;
    } else {
      remote_plan_->inc_ref_count(pc_ctx.handle_id_);  // inc ref count by 1 if matched
      plan = remote_plan_;
    }
  }

  // try dist plan
  if (OB_SUCC(ret) && get_next) {
    if (OB_FAIL(dist_plans_.get_plan(pc_ctx, need_try_plan_, location_cache, plan))) {
      LOG_DEBUG("failed to get dist plan", K(ret));
    } else if (plan != NULL) {
      LOG_DEBUG("succeed to get dist plan", K(*plan));
    }
  }

  if (OB_SUCC(ret) && nullptr == plan) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  return ret;
}

int ObSqlPlanSet::remove_plan_stat()
{
  int ret = OB_SUCCESS;
  ObPlanCache* pc = NULL;
  if (NULL == plan_cache_value_ || NULL == plan_cache_value_->get_pcv_set() ||
      NULL == plan_cache_value_->get_pcv_set()->get_plan_cache()) {
    ret = OB_NOT_INIT;
  } else {
    pc = plan_cache_value_->get_pcv_set()->get_plan_cache();
    if (NULL != local_plan_) {
      pc->remove_cache_obj_stat_entry(local_plan_->get_plan_id());
    }
    if (NULL != remote_plan_) {
      pc->remove_cache_obj_stat_entry(remote_plan_->get_plan_id());
    }
    if (NULL != array_binding_plan_) {
      pc->remove_cache_obj_stat_entry(array_binding_plan_->get_plan_id());
    }
    IGNORE_RETURN dist_plans_.remove_plan_stat();
  }
  return ret;
}

int64_t ObSqlPlanSet::get_mem_size()
{
  int64_t plan_set_mem = 0;
  if (NULL != local_plan_) {
    plan_set_mem += local_plan_->get_mem_size();
  }
  if (NULL != remote_plan_) {
    plan_set_mem += remote_plan_->get_mem_size();
  }
  plan_set_mem += dist_plans_.get_mem_size();
  return plan_set_mem;
}

void ObSqlPlanSet::reset()
{
  is_all_non_partition_ = true;
  need_try_plan_ = 0;
  has_duplicate_table_ = false;
  // has_array_binding_ = false;
  table_location_fast_calc_ = true;
  is_contain_virtual_table_ = false;
  enable_inner_part_parallel_exec_ = false;
  table_locations_.reset();
  if (OB_ISNULL(plan_cache_value_) || OB_ISNULL(plan_cache_value_->get_pc_alloc())) {
    // do nothing
    LOG_WARN("plan_cache_value or pc allocator is NULL");
  } else {
    if (OB_NOT_NULL(params_info_str_.ptr())) {
      plan_cache_value_->get_pc_alloc()->free(params_info_str_.ptr());
    }
  }
  params_info_str_.reset();
  local_plan_ = NULL;
  array_binding_plan_ = NULL;
  remote_plan_ = NULL;
  dist_plans_.reset();
  local_phy_locations_.reset();
  loc_sensitive_hint_.reset();
  partition_key_.reset();
  ObPlanSet::reset();
}

// get plan used
int ObSqlPlanSet::get_phy_locations(const ObIArray<ObTableLocation>& table_locations, ObPlanCacheCtx& pc_ctx,
    ObIPartitionLocationCache& location_cache, ObIArray<ObPhyTableLocationInfo>& phy_location_infos,
    bool& need_check_on_same_server)
{
  int ret = OB_SUCCESS;
  need_check_on_same_server = true;
  if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(
          table_locations, pc_ctx, location_cache, phy_location_infos, need_check_on_same_server))) {
    LOG_WARN("failed to get phy locations", K(ret), K(table_locations));
  }
  return ret;
}

/*
 * calculate phy_plan type:
 *1. if not all tables are signal partition --->type is distributed
 *2  if all tables are signal partition:
 *         if all partitions in local server --->type is local
 *         if all partitions in the same remote server --->type is remote
 *         if all partitions don't in the same server ---->type is distributed
 * parameter need_check_on_same_server,
 * TRUE: default value, need check;
 * FALSE: we know partitions on different servers via ObPhyLocationGetter::get_phy_locations
 *        (when there are duplicate tables not in DML), no need to check again
 */
int ObSqlPlanSet::calc_phy_plan_type_v2(const ObIArray<ObPhyTableLocationInfo>& phy_location_infos,
    ObPhyPlanType& plan_type, bool need_check_on_same_server)
{
  int ret = OB_SUCCESS;
  int64_t N = phy_location_infos.count();
  if (0 == N) {
    plan_type = OB_PHY_PLAN_LOCAL;
    SQL_PC_LOG(DEBUG, "no table used, thus local plan");
  } else {
    bool is_all_empty = true;
    bool is_all_single_partition = true;
    for (int i = 0; is_all_single_partition && i < N; ++i) {
      if (phy_location_infos.at(i).get_partition_cnt() != 0) {
        is_all_empty = false;
      }
      if (phy_location_infos.at(i).get_partition_cnt() > 1) {
        is_all_single_partition = false;
      }
    }
    if (is_all_empty) {
      plan_type = OB_PHY_PLAN_LOCAL;
    } else if (is_all_single_partition) {
      bool is_same = true;
      ObAddr my_address = GCTX.self_addr_;
      ObAddr first_addr;
      if (!need_check_on_same_server) {
        is_same = false;
      }
      if (is_same && OB_FAIL(is_partition_in_same_server(phy_location_infos, is_same, first_addr))) {
        SQL_PC_LOG(WARN, "fail to calculate whether all partitions in same server", K(ret), K(phy_location_infos));
      } else {
        if (is_same) {
          if (my_address == first_addr) {
            plan_type = OB_PHY_PLAN_LOCAL;
          } else {
            plan_type = OB_PHY_PLAN_REMOTE;
          }
        } else {
          plan_type = OB_PHY_PLAN_DISTRIBUTED;
        }
      }
    } else {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
  }

  return ret;
}

int ObSqlPlanSet::calc_phy_plan_type(
    const ObIArray<ObPhyTableLocationInfo>& phy_location_infos, ObPhyPlanType& plan_type)
{
  int ret = OB_SUCCESS;
  int64_t phy_location_cnt = phy_location_infos.count();

  if (0 == phy_location_cnt) {
    plan_type = OB_PHY_PLAN_LOCAL;
    SQL_PC_LOG(DEBUG, "no table used, thus local plan");
  } else {
    bool is_all_empty = true;
    bool is_all_single_partition = true;
    for (int i = 0; is_all_single_partition && i < phy_location_cnt; ++i) {
      if (phy_location_infos.at(i).get_partition_cnt() != 0) {
        is_all_empty = false;
      }
      if (phy_location_infos.at(i).get_partition_cnt() > 1) {
        is_all_single_partition = false;
      }
    }
    if (is_all_empty) {
      plan_type = OB_PHY_PLAN_LOCAL;
    } else if (is_all_single_partition) {
      bool is_same = true;
      ObAddr first_addr;
      if (OB_FAIL(is_partition_in_same_server(phy_location_infos, is_same, first_addr))) {
        SQL_PC_LOG(WARN, "fail to calculate whether all partitions in same server", K(ret), K(phy_location_infos));
      } else {
        if (is_same) {
          ObAddr my_address = GCTX.self_addr_;
          if (my_address == first_addr) {
            plan_type = OB_PHY_PLAN_LOCAL;
          } else {
            plan_type = OB_PHY_PLAN_REMOTE;
          }
        } else {
          plan_type = OB_PHY_PLAN_DISTRIBUTED;
        }
      }
    } else {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
  }
  return ret;
}

// calculate whether all partitions in same server
int ObSqlPlanSet::is_partition_in_same_server(
    const ObIArray<ObPhyTableLocationInfo>& phy_location_infos, bool& is_same, ObAddr& first_addr)
{
  int ret = OB_SUCCESS;
  int64_t phy_location_count = phy_location_infos.count();
  if (phy_location_count > 0) {
    bool is_first = true;
    ObReplicaLocation replica_location;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < phy_location_count; ++i) {
      int64_t partition_location_count = phy_location_infos.at(i).get_partition_cnt();
      if (partition_location_count > 0) {
        for (int64_t j = 0; OB_SUCC(ret) && is_same && j < partition_location_count; ++j) {
          replica_location.reset();
          const ObPhyPartitionLocationInfo& phy_part_loc_info =
              phy_location_infos.at(i).get_phy_part_loc_info_list().at(j);
          if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_location))) {
            SQL_PC_LOG(WARN, "fail to get selected replica", K(ret), K(phy_part_loc_info));
          } else if (!replica_location.is_valid()) {
            SQL_PC_LOG(WARN, "replica_location is invalid", K(ret), K(replica_location));
          } else {
            if (is_first) {
              // get first replica
              first_addr = replica_location.server_;
              is_same = true;
              is_first = false;
              SQL_PC_LOG(DEBUG, "part_location first replica", K(ret), K(replica_location));
            } else {
              is_same = (replica_location.server_ == first_addr);
              SQL_PC_LOG(DEBUG, "part_location replica", K(ret), K(i), K(replica_location));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(WARN, "there is no partition_locattion in this phy_location", K(phy_location_infos.at(i)));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "phy_locations is empty");
  }

  return ret;
}

void ObSqlPlanSet::remove_all_plan()
{
  ObPhysicalPlan* plan = NULL;
  if (NULL != local_plan_) {
    plan = local_plan_;
    local_plan_ = NULL;
    ObCacheObjectFactory::free(plan, PC_REF_PLAN_LOCAL_HANDLE);
    plan = NULL;
  }
  if (NULL != remote_plan_) {
    plan = remote_plan_;
    remote_plan_ = NULL;
    ObCacheObjectFactory::free(plan, PC_REF_PLAN_REMOTE_HANDLE);
    plan = NULL;
  }
  if (NULL != array_binding_plan_) {
    ObCacheObjectFactory::free(array_binding_plan_, PC_REF_PLAN_ARR_HANDLE);
    array_binding_plan_ = NULL;
  }
  IGNORE_RETURN dist_plans_.remove_all_plan();
}

int ObPLPlanSet::add_cache_obj(ObCacheObject& cache_object, ObPlanCacheCtx& pc_ctx, int64_t ol_param_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(pc_ctx);
  UNUSED(ol_param_idx);
  if (OB_UNLIKELY(
          !cache_object.is_prcr() && !cache_object.is_sfc() && !cache_object.is_pkg() && !cache_object.is_anon())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cache object is invalid", K(cache_object));
  } else if (OB_UNLIKELY(pl_obj_ != NULL)) {
    ret = OB_SQL_PC_PLAN_DUPLICATE;
  } else {
    pl_obj_ = &cache_object;
    pl_obj_->inc_ref_count(PC_REF_PL_HANDLE);
    if (OB_ISNULL(pre_cal_expr_handler_)) {
      // have no pre-calculable expression, do nothing
    } else {
      cache_object.set_pre_calc_expr_handler(pre_cal_expr_handler_);
      cache_object.inc_pre_expr_ref_count();
      // planset, handle, plan, ref_cnt(val)
      LOG_DEBUG("add pre calculable expression.",
          KP(this),
          KP(cache_object.get_pre_calc_expr_handler()),
          KP(&cache_object),
          K(cache_object.get_pre_expr_ref_count()));
    }
  }
  return ret;
}

int ObPLPlanSet::select_plan(
    share::ObIPartitionLocationCache* location_cache, ObPlanCacheCtx& pc_ctx, ObCacheObject*& cache_obj)
{
  int ret = OB_SUCCESS;
  UNUSED(location_cache);
  UNUSED(pc_ctx);
  if (pl_obj_ != NULL) {
    cache_obj = pl_obj_;
    cache_obj->inc_ref_count(pc_ctx.handle_id_);
  } else {
    cache_obj = NULL;
    ret = OB_SQL_PC_NOT_EXIST;
  }
  return ret;
}

void ObPLPlanSet::remove_all_plan()
{
  if (NULL != pl_obj_) {
    ObCacheObjectFactory::free(pl_obj_, PC_REF_PL_HANDLE);
    pl_obj_ = NULL;
  }
}

int ObPLPlanSet::remove_plan_stat()
{
  int ret = OB_SUCCESS;
  ObPlanCache* pc = NULL;
  if (NULL == plan_cache_value_ || NULL == plan_cache_value_->get_pcv_set() ||
      NULL == (pc = plan_cache_value_->get_pcv_set()->get_plan_cache()) || NULL == pl_obj_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ret = pc->remove_cache_obj_stat_entry(pl_obj_->get_object_id());
    LOG_DEBUG("remove pl obj stat", K(pl_obj_->get_ref_count()), K(pl_obj_->get_object_id()), K(ret));
  }
  return ret;
}

int64_t ObPLPlanSet::get_mem_size()
{
  int64_t plan_set_mem = 0;
  if (pl_obj_ != NULL) {
    plan_set_mem += pl_obj_->get_mem_size();
  }
  return plan_set_mem;
}

void ObPLPlanSet::reset()
{
  pl_obj_ = NULL;
  ObPlanSet::reset();
}

// add plan used
int ObSqlPlanSet::get_phy_locations(const ObTablePartitionInfoArray& partition_infos,
    ObIArray<ObPhyTableLocation>& phy_locations, ObIArray<ObPhyTableLocationInfo>& phy_location_infos)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("add plan partition infos", K(partition_infos));
  if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(partition_infos, phy_locations, phy_location_infos))) {
    LOG_WARN("failed to get phy location while adding plan",
        K(ret),
        K(partition_infos),
        K(phy_locations),
        K(phy_location_infos));
  } else { /* do nothing */
  }
  LOG_DEBUG("add plan phy_tbl_location", K(phy_locations));
  return ret;
}

int ObSqlPlanSet::set_concurrent_degree(int64_t outline_param_idx, ObPhysicalPlan& plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_cache_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (outline_param_idx != OB_INVALID_INDEX) {
    const ObMaxConcurrentParam* param = plan_cache_value_->get_outline_param(outline_param_idx);
    if (OB_ISNULL(param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is null", K(ret));
    } else {
      plan.set_max_concurrent_num(param->get_concurrent_num());
      if (OB_FAIL(plan.inc_concurrent_num())) {
        if (OB_REACH_MAX_CONCURRENT_NUM == ret) {
          LOG_USER_ERROR(OB_REACH_MAX_CONCURRENT_NUM, plan.get_max_concurrent_num());
        } else {
          LOG_WARN("fail to inc concurrent num", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSqlPlanSet::extend_param_store(
    const ParamStore& params, ObIAllocator& allocator, ObIArray<ParamStore*>& expanded_params)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (params.at(i).is_ext()) {
      ret = OB_NOT_SUPPORTED;
    }
  }
  ParamStore* tmp_param_store = NULL;
  void* param_store_buf = NULL;
  ParamStore tmp_params((ObWrapperAllocator(allocator)));
  for (int64_t vec_cnt = 0; OB_SUCC(ret) && vec_cnt < count; vec_cnt++) {
    if (OB_ISNULL(param_store_buf = allocator.alloc(sizeof(ParamStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      tmp_param_store = new (param_store_buf) ParamStore(ObWrapperAllocator(allocator));
      if (OB_FAIL(expanded_params.push_back(tmp_param_store))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (params.at(i).is_ext()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      for (int64_t vec_cnt = 0; OB_SUCC(ret) && vec_cnt < count; vec_cnt++) {
        if (OB_FAIL(expanded_params.at(vec_cnt)->push_back(params.at(i)))) {
          LOG_WARN("fail to push param", K(ret), K(vec_cnt));
        }
      }
    }
  }  // for

  LOG_DEBUG("extend param store", K(ret), K(count), K(expanded_params), K(params));
  return ret;
}

int ObSqlPlanSet::get_table_array_binding_parition_ids(const ObTableLocation& table_location,
    const ObSQLSessionInfo* session, const ParamStore& param_store, ObSchemaGetterGuard* schema_guard,
    ObExecContext& exec_ctx, ObIArray<int64_t>& partition_ids)
{
  int ret = OB_SUCCESS;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  ObSEArray<ParamStore*, 16> param_stores;
  ObSEArray<int64_t, 16> temp_partition_ids;
  ObArenaAllocator tmp_alloc;
  if (OB_FAIL(extend_param_store(param_store, tmp_alloc, param_stores))) {
    LOG_WARN("fail to extend param store", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_stores.count(); i++) {
    LOG_DEBUG("calculate binding param partition ids", K(param_stores.at(i)));
    temp_partition_ids.reuse();
    if (OB_ISNULL(param_stores.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(table_location.calculate_partition_ids(
                   exec_ctx, schema_guard, *param_stores.at(i), temp_partition_ids, dtc_params))) {
      LOG_WARN("fail to get partition ids", K(ret));
    } else if (OB_FAIL(append(partition_ids, temp_partition_ids))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else {
      LOG_DEBUG("succeed to calculate binding param partition ids", K(partition_ids));
    }
  }
  return ret;
}

int ObSqlPlanSet::get_array_bind_plan_type(ObPlanCacheCtx& pc_ctx, ObIPartitionLocationCache& location_cache,
    ObPhyPlanType& plan_type, ObIArray<ObSEArray<int64_t, 1024>>& dup_partition_idss)
{
  int ret = OB_SUCCESS;
  bool is_same_single_partition = true;
  bool need_check_on_same_server = true;
  ObSEArray<ObSEArray<int64_t, 4>, 4> partition_idss;
  ObSEArray<ObPhyTableLocationInfo, 4> phy_location_infos;
  if (OB_FAIL(get_array_bind_partition_ids(
          pc_ctx.exec_ctx_, pc_ctx.sql_ctx_.schema_guard_, pc_ctx.sql_ctx_.session_info_, dup_partition_idss))) {
    LOG_WARN("fail to get array bind partition ids", K(ret));
  } else {
    ObSEArray<int64_t, 4> temp_partition_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_partition_idss.count(); i++) {
      temp_partition_ids.reuse();
      if (OB_FAIL(append_array_no_dup(temp_partition_ids, dup_partition_idss.at(i)))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else if (OB_FAIL(partition_idss.push_back(temp_partition_ids))) {
        LOG_WARN("failed to push back partition ids", K(ret));
      } else if (1 != temp_partition_ids.count()) {
        is_same_single_partition = false;
      } else { /*do nothing*/
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_array_bind_phy_location(
            pc_ctx, partition_idss, pc_ctx.exec_ctx_, location_cache, phy_location_infos, need_check_on_same_server))) {
      LOG_WARN("fail to get array bind phy location", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    bool is_same = true;
    ObAddr my_address = GCTX.self_addr_;
    ObAddr first_addr;
    if (!need_check_on_same_server) {
      is_same = false;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_same_single_partition) {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    } else if (is_same && OB_FAIL(is_partition_in_same_server(phy_location_infos, is_same, first_addr))) {
      SQL_PC_LOG(WARN, "fail to calculate whether all partitions in same server", K(ret), K(phy_location_infos));
    } else if (is_same && first_addr == my_address) {
      plan_type = OB_PHY_PLAN_LOCAL;
      //} else if (has_array_binding_) {
      // ret = OB_ARRAY_BINDING_ROLLBACK;
    } else if (is_same && first_addr != my_address) {
      plan_type = OB_PHY_PLAN_REMOTE;
    } else {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
  }
  return ret;
}

int ObSqlPlanSet::get_array_bind_partition_ids(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard* schema_guard, const ObSQLSessionInfo* session,
    ObIArray<ObSEArray<int64_t, 1024>>& dup_partition_idss)
{
  int ret = OB_SUCCESS;
  bool fast_calc = true;
  if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumente", K(ret));
  } else if (OB_FAIL(can_fast_calc_part_ids(exec_ctx.get_physical_plan_ctx()->get_param_store(), fast_calc))) {
    LOG_WARN("fail to check fast calc part ids", K(ret));
  } else if (fast_calc) {
    if (OB_FAIL(fast_get_array_bind_partition_ids(exec_ctx, schema_guard, session, dup_partition_idss))) {
      LOG_WARN("fail to fast get array bind partition ids", K(ret));
    }
  } else {
    if (OB_FAIL(normal_get_array_bind_partition_ids(exec_ctx, schema_guard, session, dup_partition_idss))) {
      LOG_WARN("fail to normal get array bind partition ids", K(ret));
    }
  }

  return ret;
}

int ObSqlPlanSet::init_fast_calc_part_ids_info()
{
  int ret = OB_SUCCESS;
  int64_t N = table_locations_.count();
  for (int64_t i = 0; OB_SUCC(ret) && table_location_fast_calc_ && i < N; i++) {
    const ObTableLocation& table_location = table_locations_.at(i);
    if (table_location_fast_calc_ && table_location.can_fast_calc_part_ids()) {
      if (OB_FAIL(append_array_no_dup(part_param_idxs_, table_location.get_part_expr_param_idxs()))) {
        LOG_WARN("fail to append array", K(ret));
      }
    } else {
      table_location_fast_calc_ = false;
    }
  }

  return ret;
}

int ObSqlPlanSet::can_fast_calc_part_ids(const ParamStore& params, bool& fast_calc) const
{
  int ret = OB_SUCCESS;
  fast_calc = true;
  int64_t count = 0;
  if (table_location_fast_calc_) {
    for (int64_t i = 0; fast_calc && i < part_param_idxs_.count(); i++) {
      const ObObjParam& obj_param = params.at(part_param_idxs_.at(i));
      if (obj_param.is_ext()) {
        ret = OB_NOT_SUPPORTED;
      }
    }  // for end
  } else {
    fast_calc = false;
  }
  return ret;
}

int ObSqlPlanSet::fast_get_array_bind_partition_ids(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard* schema_guard, const ObSQLSessionInfo* session,
    ObIArray<ObSEArray<int64_t, 1024>>& dup_partition_idss)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1024> partition_ids;
  ObSEArray<ObObjParam, 16> tmp_param_store;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumente", K(ret));
  } else {
    int64_t query_count = 0;
    const ParamStore& params = exec_ctx.get_physical_plan_ctx()->get_param_store();
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      if (params.at(i).is_ext()) {
        ret = OB_NOT_SUPPORTED;
      } else {
        if (OB_FAIL(tmp_param_store.push_back(params.at(i)))) {
          LOG_WARN("fail to push param", K(ret));
        }
      }
    }  // for end
    int64_t N = table_locations_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      partition_ids.reuse();
      const ObTableLocation& table_location = table_locations_.at(i);
      if (OB_FAIL(table_location.calculate_partition_ids(exec_ctx, schema_guard, params, partition_ids, dtc_params))) {
        LOG_WARN("fail to get partition ids", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < query_count; j++) {
          if (j == 0 && OB_FAIL(dup_partition_idss.push_back(partition_ids))) {
            LOG_WARN("failed to push back partition ids", K(ret));
          } else if (j != 0 && OB_FAIL(append(dup_partition_idss.at(i), partition_ids))) {
            LOG_WARN("failed to append partition ids", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }  // for table location end
  }
  return ret;
}

int ObSqlPlanSet::normal_get_array_bind_partition_ids(ObExecContext& exec_ctx,
    share::schema::ObSchemaGetterGuard* schema_guard, const ObSQLSessionInfo* session,
    ObIArray<ObSEArray<int64_t, 1024>>& dup_partition_idss)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1024> partition_ids;
  ObSEArray<ParamStore*, 16> param_stores;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(exec_ctx.get_physical_plan_ctx())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argumente", K(ret));
    } else if (OB_FAIL(extend_param_store(
                   exec_ctx.get_physical_plan_ctx()->get_param_store(), exec_ctx.get_allocator(), param_stores))) {
      LOG_WARN("fail to extend param store", K(ret));
    }
  }
  int64_t N = table_locations_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_stores.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < N; j++) {
      partition_ids.reuse();
      const ObTableLocation& table_location = table_locations_.at(j);
      if (OB_ISNULL(param_stores.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FAIL(table_location.calculate_partition_ids(
                     exec_ctx, schema_guard, *param_stores.at(i), partition_ids, dtc_params))) {
        LOG_WARN("fail to get partition ids", K(ret));
      } else if (i == 0 && OB_FAIL(dup_partition_idss.push_back(partition_ids))) {
        LOG_WARN("failed to push back partition ids", K(ret));
      } else if (i != 0 && OB_FAIL(append(dup_partition_idss.at(j), partition_ids))) {
        LOG_WARN("failed to append partition ids", K(ret));
      } else { /*do nothing*/
      }
    }  // for table location end
  }    // for param stores end
  return ret;
}

int ObSqlPlanSet::get_array_bind_phy_location(const ObPlanCacheCtx& pc_ctx,
    const ObIArray<ObSEArray<int64_t, 4>>& partition_idss, ObExecContext& exec_ctx,
    ObIPartitionLocationCache& location_cache, ObIArray<ObPhyTableLocationInfo>& phy_location_infos,
    bool& need_check_on_same_server)
{
  int ret = OB_SUCCESS;
  bool is_retrying = false;
  bool on_same_server = true;
  bool has_duplicate_tbl_not_in_dml = false;
  need_check_on_same_server = true;
  if (OB_ISNULL(exec_ctx.get_task_executor_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    int64_t N = table_locations_.count();
    ObSEArray<const ObTableLocation*, 2> table_location_ptrs;
    ObSEArray<ObPhyTableLocationInfo*, 2> phy_location_info_ptrs;
    if (OB_FAIL(phy_location_infos.prepare_allocate(N))) {
      LOG_WARN("phy_locations_info prepare allocate error", K(ret), K(N));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      LOG_DEBUG("partition ids", K(partition_idss.at(0)));
      const ObTableLocation& table_location = table_locations_.at(i);
      ObPhyTableLocationInfo& phy_location_info = phy_location_infos.at(i);
      if (OB_FAIL(table_location.set_partition_locations(exec_ctx,
              location_cache,
              table_location.get_ref_table_id(),
              partition_idss.at(i),
              phy_location_info.get_phy_part_loc_info_list_for_update(),
              true /*nonblock*/))) {
        LOG_WARN("failed to calculate partition location", K(ret));
      } else {
        if (table_location.is_duplicate_table_not_in_dml()) {
          has_duplicate_tbl_not_in_dml = true;
        }
        phy_location_info.set_duplicate_type(table_location.get_duplicate_type());
        phy_location_info.set_table_location_key(table_location.get_table_id(), table_location.get_ref_table_id());
        LOG_DEBUG("after get phy_location_info", K(phy_location_info));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_location_ptrs.push_back(&table_location))) {
          LOG_WARN("failed to push back table location ptrs", K(ret), K(i), K(N), K(table_locations_.at(i)));
        } else if (OB_FAIL(phy_location_info_ptrs.push_back(&phy_location_info))) {
          LOG_WARN("failed to push back phy location info ptrs", K(ret), K(i), K(N), K(phy_location_infos.at(i)));
        }
      }
    }  // for end
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pc_ctx.is_retry_for_dup_tbl(is_retrying))) {
        LOG_WARN("failed to test if retrying", K(ret));
      } else if (OB_FAIL(ObLogPlan::select_replicas(
                     exec_ctx, table_location_ptrs, exec_ctx.get_addr(), phy_location_info_ptrs))) {
        LOG_WARN("failed to select replicas",
            K(ret),
            K(table_locations_),
            K(exec_ctx.get_addr()),
            K(phy_location_info_ptrs));
      } else if (!has_duplicate_tbl_not_in_dml || is_retrying) {
        // do nothing
      } else if (OB_FAIL(
                     ObPhyLocationGetter::reselect_duplicate_table_best_replica(phy_location_infos, on_same_server))) {
        LOG_WARN("failed to reselect replicas", K(ret));
      }
      if (!on_same_server) {
        need_check_on_same_server = false;
      }
      LOG_DEBUG("after select_replicas", K(on_same_server), K(has_duplicate_tbl_not_in_dml), K(phy_location_infos));
    }
    ObPhyTableLocationIArray& phy_locations = exec_ctx.get_task_executor_ctx()->get_table_locations();
    if (OB_SUCC(ret)) {
      phy_locations.reset();
      if (OB_FAIL(phy_locations.prepare_allocate(N))) {
        LOG_WARN("phy_locations prepare allocate error", K(ret), K(N));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      const ObTableLocation& table_location = table_locations_.at(i);
      ObPhyTableLocation& phy_location = phy_locations.at(i);
      ObPhyTableLocationInfo& phy_location_info = phy_location_infos.at(i);
      LOG_DEBUG("before assign", K(phy_location_info));
      if (OB_FAIL(phy_location_info.set_direction(table_location.get_direction()))) {
        LOG_WARN("failed to set phy location info direction", K(ret), K(table_location));
      } else if (OB_FAIL(phy_location.assign_from_phy_table_loc_info(phy_location_info))) {
        LOG_WARN("failed to assign from phy table loc info", K(ret), K(phy_location_info));
      }
      LOG_DEBUG("after assign", K(phy_location));
    }  // for end
  }

  return ret;
}

int ObSqlPlanSet::get_plan_type(const ObIArray<ObTableLocation>& table_locations, const bool is_contain_uncertain_op,
    ObPlanCacheCtx& pc_ctx, ObIPartitionLocationCache& location_cache,
    ObIArray<ObPhyTableLocationInfo>& phy_location_infos, ObPhyPlanType& plan_type)
{
  int ret = OB_SUCCESS;
  bool need_check_on_same_server = true;
  phy_location_infos.reuse();

  if (OB_FAIL(
          get_phy_locations(table_locations, pc_ctx, location_cache, phy_location_infos, need_check_on_same_server))) {
    LOG_WARN("failed to get physical locations", K(ret));
  } else if (OB_FAIL(calc_phy_plan_type_v2(phy_location_infos, plan_type, need_check_on_same_server))) {
    LOG_WARN("failed to calcute physical plan type", K(ret));
  } else {
    if (is_contain_uncertain_op && plan_type != OB_PHY_PLAN_LOCAL && stmt::T_SELECT != stmt_type_) {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
    LOG_DEBUG("get plan type",
        K(table_locations),
        K(plan_type),
        K(phy_location_infos),
        K(is_contain_uncertain_op),
        K(stmt_type_),
        K(ret));
  }

  return ret;
}

bool ObSqlPlanSet::is_local_plan_opt_allowed(const int last_retry_err)
{
  bool ret_bool = false;

  if (OB_SUCCESS == last_retry_err) {
    ret_bool = true;
  } else if (last_retry_err == OB_TRANSACTION_SET_VIOLATION || last_retry_err == OB_TRY_LOCK_ROW_CONFLICT) {
    ret_bool = true;
  } else {
    ret_bool = false;
  }
  LOG_DEBUG("use direct local plan", K(ret_bool), K(last_retry_err));
  return ret_bool;
}

int ObSqlPlanSet::check_partition_status(const ObPartitionKey& pkey, bool& is_leader)
{
  int ret = OB_SUCCESS;
  is_leader = false;
  if (OB_ISNULL(GCTX.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition service or election mgr is NULL", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || NULL == GCTX.par_ser_->get_trans_service()) {
  } else if (OB_FAIL(GCTX.par_ser_->get_trans_service()->check_trans_partition_leader_unsafe(pkey, is_leader))) {
    // ignore ret
    ret = OB_SUCCESS;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
