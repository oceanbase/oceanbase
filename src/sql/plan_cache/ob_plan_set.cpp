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
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "pl/ob_pl.h"
#include "ob_plan_set.h"
#include "share/resource_manager/ob_resource_manager.h"

using namespace oceanbase;
using namespace common;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;
using namespace share;
using namespace share::schema;
using namespace pl;
namespace oceanbase
{
namespace sql
{
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

//used for get plan
int ObPlanSet::match_params_info(const ParamStore *params,
                                 ObPlanCacheCtx &pc_ctx,
                                 int64_t outline_param_idx,
                                 bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObExecContext &exec_ctx = pc_ctx.exec_ctx_;
  ObSessionVariable sess_var;
  bool is_sql = is_sql_planset();
  if (false == is_match_outline_param(outline_param_idx)) {
    is_same = false;
  } else if (OB_ISNULL(params)) {
    is_same = true;
  } else if (params->count() > params_info_.count()) {
    is_same = false;
  } else {
    //匹配原始的参数
    int64_t N = params->count();
    LOG_TRACE("params info", K(params_info_), K(*params), K(this));
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < N; ++i) {
      if (OB_FAIL(match_param_info(params_info_.at(i),
                                   params->at(i),
                                   is_same,
                                   is_sql))) {
        LOG_WARN("fail to match param info", K(ret), K(params_info_), K(*params));
      }
    }

    // 匹配相关的用户session变量

    // 这里应该先进行related_user_var_names_跟session_info里面变量的比较，再进行预计算
    // 否则会导致session var类型改变后, 无法匹配上计划. 举例如下
    // eg:   SQL                   ParamStore
    //     1. set @a := 1;
    //     2. select @a;            int obj
    //     3. set @a := '1';
    //     4. select @a;            varchar obj
    //     5. select @a;            int obj(因为填的是匹配sql2 plan时预计算的结果)
    //     结果: sql5无法匹配上sql4的计划
    //     原因: sql5匹配sql2的计划时先预计算，得到int obj，再比较related_user_var_names_
    //           发现session_info_里面的sess_var为varchar，匹配失败.
    //           sql5再次匹配sql4的计划，由于已经预计算，不再进行计算，所以ParamStore
    //           里面还是int obj，而params_info_中Obj为varchar，匹配失败.
    //
    //     所以应该改为先比较related_user_var_names是否和sess_var相同，再进行预计算
    //     就不会导致匹配时，ParamStore中填的是上一个计划的预计算结果
    if (OB_SUCC(ret) && is_same && related_user_var_names_.count() > 0) {
      if (related_user_var_names_.count() != related_user_sess_var_metas_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related_user_var_names and related_user_sess_vars should have the same size",
                 K(ret), K(related_user_var_names_.count()), K(related_user_sess_var_metas_.count()));
      } else if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null",
                 K(ret), K(pc_ctx.sql_ctx_.session_info_));
      } else {
        ObSQLSessionInfo *session_info = pc_ctx.sql_ctx_.session_info_;
        for (int64_t i = 0 ; OB_SUCC(ret) && is_same && i < related_user_var_names_.count(); i++) {
          if (OB_FAIL(session_info->get_user_variable(related_user_var_names_.at(i), sess_var))) {
            LOG_WARN("failed to get user variable", K(ret), K(related_user_var_names_.at(i)), K(i));
          } else {
            ObPCUserVarMeta tmp_meta(sess_var);
            is_same = (related_user_sess_var_metas_.at(i) == tmp_meta);
          }
        }
      }
    }

    // privilege
    if (OB_SUCC(ret) && is_same && all_priv_constraints_.count() > 0) {
      if (OB_FAIL(match_priv_cons(pc_ctx, is_same))) {
        LOG_WARN("failed to check privilege constraint", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(pc_ctx.sql_ctx_.session_info_) && is_same) {
      is_same = (is_cli_return_rowid_ == pc_ctx.sql_ctx_.session_info_->is_client_return_rowid());
    }

    //pre calculate
    if (OB_SUCC(ret) && is_same) {
      ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
      ObSQLSessionInfo *session = exec_ctx.get_my_session();

      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null session", K(ret));
      } else if (OB_ISNULL(plan_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null plan context", K(ret));
      } else if (fetch_cur_time_ && FALSE_IT(plan_ctx->set_cur_time(
                                ObClockGenerator::getClock(), *session))) {
        // never reach
      } else if (FALSE_IT(plan_ctx->set_last_trace_id(session->get_last_trace_id()))) {
      } else if (params->count() != params_info_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param info count is different", K(params_info_), K(*params), K(ret));
      } else {
        /* check calculable expr constraints*/
        DLIST_FOREACH(pre_calc_con, all_pre_calc_constraints_) {
          if (OB_FAIL(ObPlanCacheObject::check_pre_calc_cons(is_ignore_stmt_,
                                                             is_same,
                                                             *pre_calc_con,
                                                             exec_ctx))) {
            LOG_WARN("failed to pre calculate expression", K(ret));
          } else if (!is_same) {
            break;
          }
        }
      }
    }

    //匹配true/false,此时的params中flag_是初始化的值, 不能直接使用。
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < params->count(); ++i) {
      if (OB_FAIL(match_param_bool_value(params_info_.at(i),
                                         params->at(i),
                                         is_same))) {
        LOG_WARN("failed to match param bool value", K(ret), K(params_info_), K(*params));
      }
    } //for end

    // check const constraint
    if (OB_SUCC(ret) && is_same) {
      OC( (match_constraint)(*params, is_same) );
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
      LOG_TRACE("after match param result", K(ret), K(is_same), K(params_info_));
    }
  }
  LOG_DEBUG("after match param result", K(ret), K(is_same), K(params_info_));
  return ret;
}

int ObPlanSet::copy_param_flag_from_param_info(ParamStore *params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is null", K(ret));
  } else if (params->count() != params_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is null", K(ret), KPC(params), K(params_info_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
    params->at(i).set_param_flag(params_info_.at(i).flag_);
  }
  return ret;
}

//匹配参数类型信息
int ObPlanSet::match_param_info(const ObParamInfo &param_info,
                                const ObObjParam &param,
                                bool &is_same,
                                bool is_sql_planset) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  // extend type must be checked
  // insert into t values (1)
  // insert into t values (:0)
  // two sql have the same key `insert into t values (?)`
  // but they have complete different plans
  if ((param_info.flag_.need_to_check_type_ || need_match_all_params_)
      || (is_sql_planset && lib::is_oracle_mode() &&
          (param_info.type_ == ObTinyIntType || param.get_type() == ObTinyIntType))) {
    if (lib::is_oracle_mode() &&
        param.get_param_meta().get_type() == ObCharType &&
        param.get_type() == ObNullType) {
    // in oracle mode, empty string represents null
    // and therefore meta_'s type (ObCharType) is inconsistent
    // with param's type.
    } else if (param.get_param_meta().get_type() != param.get_type()) {
      LOG_TRACE("differ in match param info",
                K(param.get_param_meta().get_type()),
                K(param.get_type()));
    }

    if (param.get_collation_type() != param_info.col_type_
        && !(param.is_user_defined_sql_type() || param.is_collection_sql_type())) {
      is_same = false;
    } else if (param.get_param_meta().get_type() != param_info.type_) {
      is_same = false;
    } else if (param.is_user_defined_sql_type() || param.is_collection_sql_type()) {
      uint64_t udt_id_param = param.get_accuracy().get_accuracy();
      uint64_t udt_id_info = static_cast<uint64_t>(param_info.ext_real_type_) << 32
                             | static_cast<uint32_t>(param_info.col_type_);
      is_same = (udt_id_info == udt_id_param) ? true : false;
    } else if (param.is_ext()) {
      ObDataType data_type;
      if (!param_info.flag_.need_to_check_extend_type_) {
        // do nothing
      } else if (OB_FAIL(ObSQLUtils::get_ext_obj_data_type(param, data_type))) {
        LOG_WARN("fail to get obj data_type", K(ret), K(param));
      } else if (data_type.get_scale() == param_info.scale_ &&
                 data_type.get_obj_type() == param_info.ext_real_type_) {
        is_same = true;
      } else {
        is_same = false;
        LOG_TRACE("ext match param info", K(data_type), K(param_info), K(is_same), K(ret));
      }
      LOG_DEBUG("ext match param info", K(data_type), K(param_info), K(is_same), K(ret));
    } else if (param_info.is_oracle_empty_string_ && !param.is_null()) { //普通字符串不匹配空串的计划
      is_same = false;
    } else if (ObSQLUtils::is_oracle_empty_string(param)
               &&!param_info.is_oracle_empty_string_) { //空串不匹配普通字符串的计划
      is_same = false;
    } else if (param_info.flag_.is_boolean_ != param.is_boolean()) { //bool type not match int type
      is_same = false;
    } else {
      // number params in point and st_point can ignore scale check to share plancache
      // please refrer to ObSqlParameterization::is_ignore_scale_check
      is_same = param_info.flag_.ignore_scale_check_
                ? true
                : (param.get_scale() == param_info.scale_);
      is_same = is_same && match_decint_precision(param_info, param.get_precision());
    }
  }
  return ret;
}

// 匹配真/假参数
int ObPlanSet::match_param_bool_value(const ObParamInfo &param_info,
                                      const ObObjParam &param,
                                      bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  bool vec_param_same = true;
  bool first_val = true;
  if (param_info.flag_.need_to_check_bool_value_) {
    bool is_value_true = false;
    if (OB_FAIL(ObObjEvaluator::is_true(param, is_value_true))) {
      SQL_PC_LOG(WARN, "fail to get param info", K(ret));
    } else if (is_value_true != param_info.flag_.expected_bool_value_) {
      is_same = false;
    }
  }

  return ret;
}

int ObPlanSet::match_multi_stmt_info(const ParamStore &params,
                                     const ObIArray<int64_t> &multi_stmt_rowkey_pos,
                                     bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (multi_stmt_rowkey_pos.empty()) {
    is_match = true;
  } else {
    // check all rowkey are different
    int64_t stmt_count = 0;
    ObSEArray<const ObObj*, 16> binding_data;
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_stmt_rowkey_pos.count(); i++) {
      int64_t pos = multi_stmt_rowkey_pos.at(i);
      if (OB_UNLIKELY(pos < 0 || pos >= params.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected array pos",K(pos), K(params.count()), K(ret));
      } else if (OB_UNLIKELY(!params.at(pos).is_ext_sql_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected type", K(params.at(pos)), K(ret));
      } else {
        const ObSqlArrayObj *array_params = reinterpret_cast<const ObSqlArrayObj*>(
                                                  params.at(pos).get_ext());
        if (OB_ISNULL(array_params)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", KPC(array_params), K(ret));
        } else if (OB_FAIL(binding_data.push_back(array_params->data_))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (i == 0) {
          stmt_count = array_params->count_;
        } else if (OB_UNLIKELY(stmt_count != array_params->count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected stmt count", K(ret));
        } else { /*do nothing*/ }
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
                LOG_INFO("batched multi-stmt does not have the same rowkey", K(i),
                    K(hash_key));
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
int ObPlanSet::match_priv_cons(ObPlanCacheCtx &pc_ctx, bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = true;
  bool has_priv = false;
  ObSQLSessionInfo *session_info = pc_ctx.sql_ctx_.session_info_;
  ObSchemaGetterGuard *schema_guard = pc_ctx.sql_ctx_.schema_guard_;
  if (OB_ISNULL(session_info) || OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_matched && i < all_priv_constraints_.count(); ++i) {
    const ObPCPrivInfo &priv_info = all_priv_constraints_.at(i);
    bool has_priv = false;
    if (OB_FAIL(ObOraSysChecker::check_ora_user_sys_priv(*schema_guard,
                                                         session_info->get_effective_tenant_id(),
                                                         session_info->get_priv_user_id(),
                                                         session_info->get_database_name(),
                                                         priv_info.sys_priv_,
                                                         session_info->get_enable_role_array()))) {
      if (OB_ERR_NO_PRIVILEGE == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("lack sys privilege", "priv_type", all_priv_constraints_.at(i).sys_priv_);
      } else {
        LOG_WARN("failed to check ora user sys priv", K(ret));
      }
    } else {
      has_priv = true;
    }
    if (OB_SUCC(ret)) {
      is_matched = priv_info.has_privilege_ == has_priv;
    }
  }
  return ret;
}


//判断多组参数中同一列参数的是否均为true/false, 并返回第一个参数是true/false
int ObPlanSet::check_vector_param_same_bool(const ObObjParam &param_obj,
                                            bool &first_val,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (param_obj.is_ext_sql_array()) {
    const ObSqlArrayObj *array_params = reinterpret_cast<const ObSqlArrayObj*>(param_obj.get_ext());
    if (OB_ISNULL(array_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array params is null", K(ret));
    } else if (OB_ISNULL(array_params->data_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array data is null", K(ret), KPC(array_params));
    } else {
      int64_t count = array_params->count_;
      if (count > 0) {
        bool is_value_true = true;
        for (int64_t param_idx = 0; OB_SUCC(ret) && is_same && param_idx < count; param_idx++) {
          if (OB_FAIL(ObObjEvaluator::is_true(array_params->data_[param_idx], is_value_true))) {
            SQL_PC_LOG(WARN, "fail to get param info", K(ret));
          } else if (param_idx == 0) {
            first_val = is_value_true;
          } else if (first_val != is_value_true) {
            is_same = false;
          }
        } // for end
      }
    }
  }

  return ret;
}

/*//判断param store中array参数的每个obj是否恒真/假*/
//int ObPlanSet::check_array_bind_same_bool_param(const Ob2DArray<ObParamInfo,
                                                //OB_MALLOC_BIG_BLOCK_SIZE,
                                                //ObWrapperAllocator, false> &param_infos,
                                                //const ParamStore &param_store,
                                                //bool &same_bool_param)
//{
  //int ret = OB_SUCCESS;
  //bool first_val = false;
  //same_bool_param = true;
  //for (int64_t i = 0; OB_SUCC(ret) && same_bool_param && i < param_store.count(); ++i) {
    //if (param_infos.at(i).flag_.need_to_check_bool_value_
        //&& param_store.at(i).is_ext()) {
      ////检查每一组参数的结果是否为true/false
      //if (OB_FAIL(check_vector_param_same_bool(param_store.at(i),
                                               //first_val,
                                               //same_bool_param))) {
        //LOG_WARN("fail to check vector param same bool", K(ret));
      //}
    //}
  //} //for end

  //return ret;
/*}*/

//used for add plan
int ObPlanSet::match_params_info(const Ob2DArray<ObParamInfo,
                                         OB_MALLOC_BIG_BLOCK_SIZE,
                                         ObWrapperAllocator, false> &infos,
                                 int64_t outline_param_idx,
                                 const ObPlanCacheCtx &pc_ctx,
                                 bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObSQLSessionInfo *session_info = pc_ctx.sql_ctx_.session_info_;
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
      if (true == is_same
          && (params_info_.at(i).flag_.need_to_check_type_ || need_match_all_params_)) {
        if (infos.at(i).type_ != params_info_.at(i).type_
            || infos.at(i).scale_ != params_info_.at(i).scale_
            || infos.at(i).col_type_ != params_info_.at(i).col_type_
            || (params_info_.at(i).flag_.need_to_check_extend_type_
                && infos.at(i).ext_real_type_ != params_info_.at(i).ext_real_type_)
            || (params_info_.at(i).flag_.is_boolean_ != infos.at(i).flag_.is_boolean_)
            || !match_decint_precision(params_info_.at(i), infos.at(i).precision_)) {
          is_same = false;
        }
      }
      if (true == is_same && params_info_.at(i).flag_.need_to_check_bool_value_) {
        if (infos.at(i).flag_.expected_bool_value_
            != params_info_.at(i).flag_.expected_bool_value_) {
          is_same = false;
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
          } else if (OB_FAIL(session_info->get_user_variable(related_user_var_names_.at(i),
                                                             sess_var))) {
            LOG_WARN("failed to get user variable", K(ret), K(sess_var));
          } else {
            ObPCUserVarMeta tmp_meta(sess_var);
            is_same = (tmp_meta == related_user_sess_var_metas_.at(i));
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_same) {
      if (OB_FAIL(ObPlanCacheObject::match_pre_calc_cons(all_pre_calc_constraints_, pc_ctx,
                                                         is_ignore_stmt_, is_same))) {
        LOG_WARN("failed to match pre calc cons", K(ret));
      } else if (!is_same) {
        LOG_TRACE("pre calc constraints for plan set and cur plan not match");
      }
    }

    if (is_sql_planset() && OB_SUCC(ret) && is_same) {
      CK( OB_NOT_NULL(pc_ctx.exec_ctx_.get_physical_plan_ctx()) );
      if (OB_SUCC(ret)) {
        const ParamStore &params = pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
        OC( (match_constraint)(params, is_same));
        OC( (match_cons)(pc_ctx, is_same));
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_same = false;
  }
  return ret;
}

bool ObPlanSet::can_skip_params_match()
{
  bool can_skip = true;
  for (int64_t i = 0; can_skip && i < params_info_.count(); i++) {
    if (params_info_.at(i).flag_.need_to_check_type_) {
      can_skip = false;
    }
  }
  if (can_skip) {
    if (!all_plan_const_param_constraints_.empty() ||
        !all_possible_const_param_constraints_.empty() ||
        !all_equal_param_constraints_.empty() ||
        all_pre_calc_constraints_.get_size() != 0) {
      can_skip = false;
      LOG_DEBUG("print can't skip", K(can_skip), K(all_plan_const_param_constraints_.empty()),
      K(all_possible_const_param_constraints_.empty()),
      K(all_equal_param_constraints_.empty()),
      K(all_pre_calc_constraints_.get_size()));
    }
  }
  return can_skip;
}

bool ObPlanSet::can_delay_init_datum_store()
{
  bool can_delay = true;
  if (all_pre_calc_constraints_.get_size() != 0) {
    can_delay = false;
  }
  return can_delay;
}

void ObPlanSet::reset()
{
  ObDLinkBase<ObPlanSet>::reset();
  plan_cache_value_ = NULL;
  params_info_.reset();
  stmt_type_ = stmt::T_NONE;
  fetch_cur_time_ = false;
  is_ignore_stmt_ = false;
  //is_wise_join_ = false;
  outline_param_idx_ = OB_INVALID_INDEX;

  related_user_var_names_.reset();
  related_user_sess_var_metas_.reset();

  is_cli_return_rowid_ = false;
  all_possible_const_param_constraints_.reset();
  all_plan_const_param_constraints_.reset();
  all_equal_param_constraints_.reset();
  all_pre_calc_constraints_.reset();
  all_priv_constraints_.reset();
  can_skip_params_match_ = false;
  can_delay_init_datum_store_ = false;
  alloc_.reset();
}

ObPlanCache *ObPlanSet::get_plan_cache() const
{
  ObPlanCache *pc = NULL;
  if (NULL == plan_cache_value_
      || NULL == plan_cache_value_->get_pcv_set()
      || NULL == plan_cache_value_->get_pcv_set()->get_plan_cache()) {
    pc = NULL;
  } else {
    pc = plan_cache_value_->get_pcv_set()->get_plan_cache();
  }
  return pc;
}

int ObPlanSet::remove_cache_obj_entry(const ObCacheObjID obj_id)
{
  int ret = OB_SUCCESS;
  ObPlanCache *pc = NULL;
  ObPCVSet *pcv_set = NULL;
  if (OB_ISNULL(get_plan_cache_value())
     || OB_ISNULL(pcv_set = get_plan_cache_value()->get_pcv_set())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(pcv_set));
  } else if (NULL == (pc = get_plan_cache())) {
    LOG_WARN("invalid argument", K(pc));
  } else if (OB_FAIL(pcv_set->remove_cache_obj_entry(obj_id))) {
    LOG_WARN("failed to remove cache obj entry", K(ret), K(obj_id));
  } else if (OB_FAIL(pc->remove_cache_obj_stat_entry(obj_id))) {
    LOG_WARN("failed to remove plan stat", K(obj_id), K(ret));
  }
  return ret;
}

int ObPlanSet::init_new_set(const ObPlanCacheCtx &pc_ctx,
                            const ObPlanCacheObject &plan,
                            int64_t outline_param_idx,
                            common::ObIAllocator* pc_alloc_)
{
  int ret = OB_SUCCESS;
  ObPlanCache *pc = nullptr;
  const ObSqlCtx &sql_ctx = pc_ctx.sql_ctx_;
  const ObSQLSessionInfo *session_info = sql_ctx.session_info_;
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
    char *buf = NULL;
    ObString var_name;
    ObSessionVariable sess_var;

    fetch_cur_time_ = plan.get_fetch_cur_time();
    stmt_type_ = plan.get_stmt_type();
    is_ignore_stmt_ = plan.is_ignore();
    is_cli_return_rowid_ = session_info->is_client_return_rowid();
    //add param info
    params_info_.reset();
    // set variables for resource map rule
    // if rule changed, plan cache will be flush.
    res_map_rule_id_ = pc_ctx.sql_ctx_.res_map_rule_id_;
    res_map_rule_param_idx_ = pc_ctx.sql_ctx_.res_map_rule_param_idx_;

    if (OB_FAIL(init_pre_calc_exprs(plan, pc_alloc_))) {
      LOG_WARN("failed to init pre calc exprs", K(ret));
    } else if (OB_FAIL(params_info_.reserve(plan.get_params_info().count()))) {
      LOG_WARN("failed to reserve 2d array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < plan.get_params_info().count(); ++i) {
      if (OB_FAIL(params_info_.push_back(plan.get_params_info().at(i)))) {
        SQL_PC_LOG(WARN, "fail to push back param info", K(ret));
      }
    }
    need_match_all_params_ = sql_ctx.need_match_all_params_;

    // add user session vars if necessary
    CK( OB_NOT_NULL(sql_ctx.session_info_) );
    if (OB_SUCC(ret) && sql_ctx.related_user_var_names_.count() > 0) {
      related_user_var_names_.reset();
      related_user_var_names_.set_allocator(&alloc_);
      related_user_sess_var_metas_.reset();
      related_user_sess_var_metas_.set_allocator(&alloc_);

      int64_t N = sql_ctx.related_user_var_names_.count();
      OZ( related_user_var_names_.init(N), N );
      OZ( related_user_sess_var_metas_.init(N), N );
      for (int64_t i = 0; OB_SUCC(ret) && i < sql_ctx.related_user_var_names_.count(); i++) {
        buf = (char *)alloc_.alloc(sql_ctx.related_user_var_names_.at(i).length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory",
                   K(ret), K(sql_ctx.related_user_var_names_.at(i).length()));
        } else {
          MEMCPY(buf, sql_ctx.related_user_var_names_.at(i).ptr(), sql_ctx.related_user_var_names_.at(i).length());
          var_name.assign_ptr(buf, sql_ctx.related_user_var_names_.at(i).length());
          OC( (related_user_var_names_.push_back)(var_name) );
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < related_user_var_names_.count(); i++) {
        OZ( sql_ctx.session_info_->get_user_variable(related_user_var_names_.at(i),
                                                     sess_var),
            ret,
            related_user_var_names_.at(i),
            i );
        OC( (related_user_sess_var_metas_.push_back)(ObPCUserVarMeta(sess_var)) );
      }

      if (OB_FAIL(ret)) {
        related_user_var_names_.reset();
        related_user_sess_var_metas_.reset();
      }
    }

    // init const param constraints
    ObPlanSetType ps_t = get_plan_set_type_by_cache_obj_type(plan.get_ns());
    if (PST_PRCD == ps_t) {
        // pl does not have any const param constraint
        all_possible_const_param_constraints_.reset();
        all_plan_const_param_constraints_.reset();
        all_equal_param_constraints_.reset();
        all_pre_calc_constraints_.reset();
        all_priv_constraints_.reset();
    } else if (PST_SQL_CRSR == ps_t) {
      // otherwise it should not be empty
      CK( OB_NOT_NULL(sql_ctx.all_plan_const_param_constraints_),
          OB_NOT_NULL(sql_ctx.all_possible_const_param_constraints_),
          OB_NOT_NULL(sql_ctx.all_equal_param_constraints_),
          OB_NOT_NULL(sql_ctx.all_pre_calc_constraints_),
          OB_NOT_NULL(sql_ctx.all_priv_constraints_));
      OZ( (set_const_param_constraint)(*sql_ctx.all_plan_const_param_constraints_, false) );
      OZ( (set_const_param_constraint)(*sql_ctx.all_possible_const_param_constraints_, true) );
      OZ( (set_equal_param_constraint)(*sql_ctx.all_equal_param_constraints_) );
      OZ( (set_pre_calc_constraint(*sql_ctx.all_pre_calc_constraints_)));
      OZ( (set_priv_constraint(*sql_ctx.all_priv_constraints_)));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected plan set type", K(ps_t), K(plan.get_ns()));
    }

    // initialize multi_stmt rowkey pos
    if (OB_SUCC(ret) && sql_ctx.multi_stmt_rowkey_pos_.count() > 0) {
      if (OB_FAIL(multi_stmt_rowkey_pos_.init(sql_ctx.multi_stmt_rowkey_pos_.count()))) {
        LOG_WARN("failed to init array count", K(ret));
      } else if (OB_FAIL(append(multi_stmt_rowkey_pos_, sql_ctx.multi_stmt_rowkey_pos_))) {
        LOG_WARN("failed to append multi stmt rowkey pos", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret) && sql_ctx.is_do_insert_batch_opt()) {
      can_skip_params_match_ = can_skip_params_match();
      can_delay_init_datum_store_ = can_delay_init_datum_store();
    }
  }

 return ret;
}

int ObPlanSet::set_const_param_constraint(ObIArray<ObPCConstParamInfo> &const_param_constraint,
                                          const bool is_all_constraint)
{
  int ret = OB_SUCCESS;
  ConstParamConstraint &cons_array = (is_all_constraint ?
                                      all_possible_const_param_constraints_ : all_plan_const_param_constraints_);
  cons_array.reset();
  cons_array.set_allocator(&alloc_);

  if (const_param_constraint.count() > 0) {
    if (OB_FAIL(cons_array.prepare_allocate(const_param_constraint.count()))) {
      LOG_WARN("failed to init const param constraint array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < const_param_constraint.count(); i++) {
        ObPCConstParamInfo &tmp_info = cons_array.at(i);
        tmp_info = const_param_constraint.at(i);
        if (tmp_info.const_idx_.count() <= 0 ||
            tmp_info.const_params_.count() <= 0 ||
            tmp_info.const_idx_.count() != tmp_info.const_params_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected const param info", K(tmp_info.const_idx_), K(tmp_info.const_params_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < tmp_info.const_params_.count(); i++) {
            if (tmp_info.const_params_.at(i).need_deep_copy()) {
              const ObObj &src_obj = tmp_info.const_params_.at(i);
              int64_t deep_cp_size = tmp_info.const_params_.at(i).get_deep_copy_size();
              int64_t pos = 0;
              char *tmp_buf = NULL;

              if (OB_ISNULL(tmp_buf = (char *)alloc_.alloc(deep_cp_size))) {
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
          } // for end
        }
      } // for end
    }
  }

  if (OB_FAIL(ret)) {
    cons_array.reset();
  }
  return ret;
}

int ObPlanSet::set_equal_param_constraint(common::ObIArray<ObPCParamEqualInfo> &equal_param_constraint)
{
  int ret = OB_SUCCESS;
  all_equal_param_constraints_.reset();
  all_equal_param_constraints_.set_allocator(&alloc_);
  if (equal_param_constraint.empty()) {
    //do nothing
  } else if (OB_FAIL(all_equal_param_constraints_.init(equal_param_constraint.count()))) {
    LOG_WARN("failed to init equal param constraint array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < equal_param_constraint.count(); ++i) {
    ObPCParamEqualInfo &equal_info = equal_param_constraint.at(i);
    if (equal_info.first_param_idx_ < 0 || equal_info.second_param_idx_ < 0 ||
        equal_info.first_param_idx_ > params_info_.count() ||
        equal_info.second_param_idx_ > params_info_.count() ||
        equal_info.first_param_idx_ == equal_info.second_param_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid equal param constraint", K(ret), K(equal_info));
    } else if (OB_FAIL(all_equal_param_constraints_.push_back(equal_info))) {
      LOG_WARN("failed to push back equal param info", K(ret));
    }
  }
  return ret;
}

// adds pre calc constraint
int ObPlanSet::set_pre_calc_constraint(common::ObDList<ObPreCalcExprConstraint> &pre_calc_cons)
{
  int ret = OB_SUCCESS;
  ObPreCalcExprConstraint *pre_calc_constraint = NULL;
  void *cons_buf = NULL;
  DLIST_FOREACH(cur_cons, pre_calc_cons) {
    if (PRE_CALC_ROWID == cur_cons->expect_result_) {
      if (OB_ISNULL(cons_buf = alloc_.alloc(sizeof(ObRowidConstraint)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        pre_calc_constraint = new(cons_buf)ObRowidConstraint(alloc_);
      }
    } else {
      if (OB_ISNULL(cons_buf = alloc_.alloc(sizeof(ObPreCalcExprConstraint)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        pre_calc_constraint = new(cons_buf)ObPreCalcExprConstraint(alloc_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pre_calc_constraint->assign(*cur_cons, alloc_))) {
      LOG_WARN("failed to deep copy pre calculable expression constriants", K(*cur_cons), K(ret));
    } else if (OB_UNLIKELY(!all_pre_calc_constraints_.add_last(pre_calc_constraint))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add element to dlist", K(ret));
    }
  }
  return ret;
}

// add priv constraint
int ObPlanSet::set_priv_constraint(common::ObIArray<ObPCPrivInfo> &priv_constraint)
{
  int ret = OB_SUCCESS;
  all_priv_constraints_.reset();
  all_priv_constraints_.set_allocator(&alloc_);
  if (priv_constraint.empty()) {
    //do nothing
  } else if (OB_FAIL(all_priv_constraints_.init(priv_constraint.count()))) {
    LOG_WARN("failed to init privilege constraint array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < priv_constraint.count(); ++i) {
    const ObPCPrivInfo &priv_info = priv_constraint.at(i);
    if (OB_UNLIKELY(!(priv_info.sys_priv_ > PRIV_ID_NONE && priv_info.sys_priv_ < PRIV_ID_MAX))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid priv type", K(priv_info), K(ret));
    } else if (OB_FAIL(all_priv_constraints_.push_back(priv_info))) {
      LOG_WARN("failed to push back priv info");
    }
  }
  return ret;
}

// match actually constraint
int ObPlanSet::match_cons(const ObPlanCacheCtx &pc_ctx, bool &is_matched)
{
  int ret = OB_SUCCESS;
  ObIArray<ObPCConstParamInfo> *param_cons = pc_ctx.sql_ctx_.all_plan_const_param_constraints_;
  ObIArray<ObPCConstParamInfo> *possible_param_cons =
                                        pc_ctx.sql_ctx_.all_possible_const_param_constraints_;
  ObIArray<ObPCParamEqualInfo> *equal_cons = pc_ctx.sql_ctx_.all_equal_param_constraints_;
  ObIArray<ObPCPrivInfo> *priv_cons = pc_ctx.sql_ctx_.all_priv_constraints_;
  is_matched = true;

  if (OB_ISNULL(param_cons) ||
      OB_ISNULL(possible_param_cons) ||
      OB_ISNULL(equal_cons)) {
    is_matched = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_cons), K(possible_param_cons), K(equal_cons));
  } else if (param_cons->count() != all_plan_const_param_constraints_.count() ||
             possible_param_cons->count() != all_possible_const_param_constraints_.count() ||
             equal_cons->count() != all_equal_param_constraints_.count() ||
             priv_cons->count() != all_priv_constraints_.count()) {
    is_matched = false;
  } else {
    for (int64_t i=0; is_matched && i < all_plan_const_param_constraints_.count(); i++) {
      is_matched = (all_plan_const_param_constraints_.at(i)==param_cons->at(i));
    }
    for (int64_t i=0; is_matched && i < all_possible_const_param_constraints_.count(); i++) {
      is_matched = (all_possible_const_param_constraints_.at(i)==possible_param_cons->at(i));
    }
    for (int64_t i=0; is_matched && i < all_equal_param_constraints_.count(); i++) {
      is_matched = (all_equal_param_constraints_.at(i)==equal_cons->at(i));
    }
    for (int64_t i=0; is_matched && i < all_priv_constraints_.count(); i++) {
      is_matched = (all_priv_constraints_.at(i)==priv_cons->at(i));
    }
  }

  return ret;
}

// 常量约束的检查逻辑：
// 1. all_plan_const_param_constraints_不为空，检查all_plan_const_param_constraints_的约束是否满足，
//    满足则命中plan_set，否则不命中;
// 2. 否则，检查所有可能的常量约束，如果某一个约束被满足，那么需要生成新的计划，也即不命中，否则命中
// 3. 检查要求相等的参数约束是否被满足
int ObPlanSet::match_constraint(const ParamStore &params, bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = true;

  if (all_plan_const_param_constraints_.count() > 0) { // check all_plan_const_param_constraints_ first
    for (int64_t i = 0; is_matched && OB_SUCC(ret) && i < all_plan_const_param_constraints_.count(); i++) {
      const ObPCConstParamInfo &const_param_info = all_plan_const_param_constraints_.at(i);
      CK( const_param_info.const_idx_.count() > 0,
          const_param_info.const_params_.count() > 0,
          const_param_info.const_idx_.count() == const_param_info.const_params_.count() );

      for (int64_t j = 0; is_matched && OB_SUCC(ret) && j < const_param_info.const_idx_.count(); j++) {
        const int64_t param_idx = const_param_info.const_idx_.at(j);
        const ObObj &const_param = const_param_info.const_params_.at(j);
        if (param_idx >= params.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get an unexpected param index", K(ret), K(param_idx), K(params.count()));
        } else if (const_param.is_invalid_type() ||
                   params.at(param_idx).is_invalid_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected invalid type",
                   K(ret), K(const_param.get_type()), K(params.at(param_idx).get_type()));
        } else if (!const_param.can_compare(params.at(param_idx)) ||
                   0 != const_param.compare(params.at(param_idx))) {
          LOG_TRACE("not matched const param", K(const_param), K(params.at(param_idx)));
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
      const ObPCConstParamInfo &const_param_info = all_possible_const_param_constraints_.at(i);
      CK( const_param_info.const_idx_.count() > 0,
          const_param_info.const_params_.count() > 0,
          const_param_info.const_idx_.count() == const_param_info.const_params_.count() );
      for (int64_t j = 0; match_const && OB_SUCC(ret) && j < const_param_info.const_idx_.count(); j++) {
        const int64_t param_idx = const_param_info.const_idx_.at(j);
        const ObObj &const_param = const_param_info.const_params_.at(j);
        if (param_idx >= params.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get an unexpected param index", K(ret), K(param_idx), K(params.count()));
        } else if (const_param.is_invalid_type() ||
                   params.at(param_idx).is_invalid_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected invalid type",
                   K(ret), K(const_param.get_type()), K(params.at(param_idx).get_type()));
        } else if (!const_param.can_compare(params.at(param_idx)) ||
                   0 != const_param.compare(params.at(param_idx))) {
          match_const = false;
        } else {
          // do nothing
        }
      }
      if (match_const) {
        LOG_TRACE("matched const param constraint", K(params), K(all_possible_const_param_constraints_.at(i)));
        is_matched = false; // matching one of the constraint, need to generated new plan
      }
    }
  } else {
    // do nothing
  }

  for (int64_t i = 0; is_matched && OB_SUCC(ret) && i < all_equal_param_constraints_.count(); ++i) {
    int64_t first_idx = all_equal_param_constraints_.at(i).first_param_idx_;
    int64_t second_idx = all_equal_param_constraints_.at(i).second_param_idx_;
    common::ObObjParam param1 = params.at(first_idx);
    common::ObObjParam param2 = params.at(second_idx);
    param1.set_collation_type(CS_TYPE_BINARY);
    param2.set_collation_type(CS_TYPE_BINARY);

    if (OB_UNLIKELY(first_idx < 0 || first_idx >= params.count() ||
                    second_idx < 0 || second_idx >= params.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param index is invalid", K(ret), K(params.count()), K(first_idx), K(second_idx));
    } else if (!all_equal_param_constraints_.at(i).use_abs_cmp_ &&
               param1.can_compare(param2) &&
               param1.get_collation_type() == param2.get_collation_type()) {
      is_matched = (0 == param1.compare(param2));
    } else if (all_equal_param_constraints_.at(i).use_abs_cmp_) {
      /*
       * for plan like: select ? from t1 group by grouping sets (c1, ?);
       * if  plan has absequal constraint,
       * then both "select -1 from t1 group by grouping sets (c1, 1);"
       *       and "select 1 from t1 group by grouping sets (c1, 1);"
       * will hit the plan.
       * but "select -2 from t1 group by grouping sets (c1, 1);" won't hit the plan.
       */
      if (param1.is_number() && param2.is_number()) {
        is_matched = (0 == param1.get_number().abs_compare(param2.get_number()));
      } else if (param1.is_double() && param2.is_double()) {
        is_matched = (0 == param1.get_double() + param2.get_double()) ||
                     (param1.get_double() == param2.get_double());
      } else if (param1.is_float() && param2.is_float()) {
        is_matched = (0 == param1.get_float() + param2.get_float()) ||
                     (param1.get_float() == param2.get_float());
      } else if (param1.is_decimal_int() && param2.is_decimal_int()) {
        is_matched = wide::abs_equal(param1, param2);
      } else if (param1.can_compare(param2) &&
                 param1.get_collation_type() == param2.get_collation_type()) {
        is_matched = (0 == param1.compare(param2));
      }
    }
    if (OB_SUCC(ret) && !is_matched) {
      is_matched = false;
      LOG_TRACE("not match equal param constraint", K(params), K(first_idx), K(second_idx));
    }
  }

  if (OB_FAIL(ret)) {
    is_matched = false;
  }

  return ret;
}

int ObPlanSet::init_pre_calc_exprs(const ObPlanCacheObject &phy_plan,
                                   common::ObIAllocator* pc_alloc_)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  if (phy_plan.get_pre_calc_frames().get_size() == 0) {
    // have no pre calculable expression, not initialize pre calculable expression handle.
    pre_cal_expr_handler_ = NULL;
  } else if (OB_ISNULL(pc_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan cache allocator has not been initialized.");
  } else if(OB_ISNULL( buf = pc_alloc_->alloc(sizeof(PreCalcExprHandler)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory.", K(ret));
  } else {
    pre_cal_expr_handler_ = new(buf)PreCalcExprHandler();
    pre_cal_expr_handler_->init(phy_plan.get_tenant_id(), pc_alloc_);
    buf = NULL;
    common::ObIAllocator& pre_expr_alloc = (pre_cal_expr_handler_->alloc_);

    if (OB_ISNULL(buf = pre_expr_alloc.alloc(sizeof(common::ObDList<ObPreCalcExprFrameInfo>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else {
      pre_cal_expr_handler_->pre_calc_frames_ =
                              new (buf)common::ObDList<ObPreCalcExprFrameInfo>;

      common::ObDList<ObPreCalcExprFrameInfo>* pre_calc_frames =
                                                pre_cal_expr_handler_->pre_calc_frames_;
      ObPreCalcExprFrameInfo *pre_calc_frame = NULL;
      void *frame_buf = NULL;
      DLIST_FOREACH(frame, phy_plan.get_pre_calc_frames()) {
        if (OB_ISNULL(frame_buf = pre_expr_alloc.alloc(sizeof(ObPreCalcExprFrameInfo)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (FALSE_IT(pre_calc_frame = new(frame_buf)ObPreCalcExprFrameInfo(
                                                                     pre_expr_alloc))) {
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
  }
  return ret;
}

int ObPlanSet::pre_calc_exprs(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;

  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  if (OB_ISNULL(pre_cal_expr_handler_)) {
    // have no pre-calculable expression, do nothing
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
    // for new engine
  } else if (OB_NOT_NULL(pre_cal_expr_handler_->pre_calc_frames_)) {
    DLIST_FOREACH(pre_calc_frame, *(pre_cal_expr_handler_->pre_calc_frames_)) {
      if (OB_FAIL(ObPlanCacheObject::pre_calculation(is_ignore_stmt_,
                                                     *pre_calc_frame,
                                                     exec_ctx))) {
        LOG_WARN("failed to pre calculate", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlPlanSet::add_cache_obj(ObPlanCacheObject &cache_object,
                                ObPlanCacheCtx &pc_ctx,
                                int64_t ol_param_idx,
                                int &add_ret)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!cache_object.is_sql_crsr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cache_object type is invalid", K(cache_object.get_ns()));
  } else {
    ret = add_plan(static_cast<ObPhysicalPlan&>(cache_object), pc_ctx, ol_param_idx);
  }
  cache_object.get_pre_expr_ref_count();
  // ref_cnt++;
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to add plan.", K(ret));
  } else if (OB_ISNULL(pre_cal_expr_handler_)) {
    // have no pre-calculable expression, do nothing
  } else {
    cache_object.set_pre_calc_expr_handler(pre_cal_expr_handler_);
    cache_object.inc_pre_expr_ref_count();
    // planset, handle, plan, ref_cnt(val)
    LOG_INFO("add pre calculable expression.", KP(this),
                                               KP(cache_object.get_pre_calc_expr_handler()),
                                               KP(&cache_object),
                                               K(cache_object.get_pre_expr_ref_count()));
  }
  add_ret = ret;
  return ret;
}

int ObSqlPlanSet::add_plan(ObPhysicalPlan &plan,
                           ObPlanCacheCtx &pc_ctx,
                           int64_t outline_param_idx)
{
  int ret = OB_SUCCESS;
  ObSqlCtx &sql_ctx = pc_ctx.sql_ctx_;
  //DASTableLocList table_locs(pc_ctx.exec_ctx_.get_allocator());
  ObArray<ObCandiTableLoc> candi_table_locs;
  ObPhyPlanType plan_type = OB_PHY_PLAN_UNINITIALIZED;
  if (OB_ISNULL(plan_cache_value_) ||
      OB_ISNULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "invalid argument", KP(plan_cache_value_), K(ret));
  } else if (OB_FAIL(get_phy_locations(sql_ctx.partition_infos_,
                                       //table_locs,
                                       candi_table_locs))) {
    LOG_WARN("fail to get physical locations", K(ret));
  } else if (OB_FAIL(set_concurrent_degree(outline_param_idx, plan))) {
    if (OB_REACH_MAX_CONCURRENT_NUM == ret && 0 == plan.get_max_concurrent_num()) {
      pc_ctx.is_max_curr_limit_ = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check concurrent degree", K(ret));
    }
  } else {
    // do nothing
  }

  if (OB_FAIL(ret)) {
    // do nothing
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
    if (plan.contain_index_location()) {
      need_try_plan_ |= TRY_PLAN_INDEX;
    }
    plan_type = plan.get_plan_type();
    if (OB_SUCC(ret)) {
      switch(plan_type) {
      case OB_PHY_PLAN_LOCAL:{
        SQL_PC_LOG(TRACE, "plan set add plan, local plan", K(ret));
        if (is_multi_stmt_plan()) {
          if (NULL != array_binding_plan_) {
            ret = OB_SQL_PC_PLAN_DUPLICATE;
          } else {
            array_binding_plan_ = &plan;
          }
        } else {
          if (OB_FAIL(add_physical_plan(OB_PHY_PLAN_LOCAL, pc_ctx, plan))) {
            SQL_PC_LOG(TRACE, "fail to add local plan", K(ret));
          } else if (OB_SUCC(ret)
#ifdef OB_BUILD_SPM
                    && is_spm_closed_
#endif
                    && FALSE_IT(direct_local_plan_ = &plan)) {
            // do nothing
          } else {
           // local_phy_locations_.reset();
           // if (OB_FAIL(init_phy_location(table_locs.count()))) {
           //   SQL_PC_LOG(WARN, "init phy location failed");
           // } else if (OB_FAIL(local_phy_locations_.assign(phy_locations))) {
           //   SQL_PC_LOG(WARN, "fail to assign phy locations");
           // }
           // LOG_TRACE("local phy locations", K(local_phy_locations_));
          }
        }
      } break;
      case OB_PHY_PLAN_REMOTE:{
        SQL_PC_LOG(DEBUG, "plan set add plan, remote plan", K(ret), K(remote_plan_));
        if (NULL != remote_plan_) {
          ret = OB_SQL_PC_PLAN_DUPLICATE;
        } else {
          remote_plan_ = &plan;
        }
      } break;
      case OB_PHY_PLAN_DISTRIBUTED: {
        SQL_PC_LOG(TRACE, "plan set add plan, distr plan",  K(ret));
        if (OB_FAIL(add_physical_plan(OB_PHY_PLAN_DISTRIBUTED, pc_ctx, plan))) {
          LOG_WARN("failed to add dist plan", K(ret), K(plan));
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
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_table_locs.count(); ++i) {
      const ObCandiTableLoc &candi_table_loc = candi_table_locs.at(i);
      if (candi_table_loc.is_duplicate_table_not_in_dml()) {
        has_duplicate_table_ = true;
        break;
      }
    }
  }
  SQL_PC_LOG(TRACE, "plan set add plan", K(ret), K(&plan), "plan type ", plan_type,
                    K(has_duplicate_table_), K(stmt_type_));
  // increase plan ref_count,
  // if plan doesn't add in plan cache,don't increase ref_count;
  bool real_add = OB_PHY_PLAN_LOCAL != plan_type || pc_ctx.need_add_obj_stat_;
  if (OB_SUCCESS == ret && real_add) {
    CacheRefHandleID plan_handle;
    if (array_binding_plan_ == &plan) {
      plan_handle = PC_REF_PLAN_ARR_HANDLE;
    } else {
      plan_handle = OB_PHY_PLAN_LOCAL == plan_type ? PC_REF_PLAN_LOCAL_HANDLE
                      : OB_PHY_PLAN_REMOTE == plan_type ? PC_REF_PLAN_REMOTE_HANDLE
                                                          : PC_REF_PLAN_DIST_HANDLE;
    }
   plan.set_dynamic_ref_handle(plan_handle);
   // if (candi_table_locs.count() > 0) {
   //   if (candi_table_locs.at(0).get_phy_part_loc_info_list().count() <= 0) {
   //     ret = OB_INVALID_ARGUMENT;
   //     LOG_WARN("part loc info list is empty", K(candi_table_locs.at(0)), K(ret));
   //   } else if (OB_FAIL(candi_table_locs.at(0).get_phy_part_loc_info_list().
   //     at(0).get_partition_location().get_partition_key(partition_key_))) {
   //     LOG_WARN("fail to get partition key", K(ret));
   //   }
   // }
  }
  return ret;
}

int ObSqlPlanSet::init_new_set(const ObPlanCacheCtx &pc_ctx,
                               const ObPlanCacheObject &plan,
                               int64_t outline_param_idx,
                               common::ObIAllocator* pc_malloc_)
{
  int ret = OB_SUCCESS;
  const ObSqlCtx &sql_ctx = pc_ctx.sql_ctx_;
  // set outline_param_idx
  outline_param_idx_ = outline_param_idx;
  need_try_plan_ = 0;
  has_duplicate_table_ = false;
#ifdef OB_BUILD_SPM
  int64_t spm_mode = 0;
#endif
  const ObSQLSessionInfo *session_info = sql_ctx.session_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null plan cache or session info", K(ret), K(session_info));
#ifdef OB_BUILD_SPM
  } else if (OB_FAIL(session_info->get_spm_mode(spm_mode))) {
    LOG_WARN("failed to get spm mode", K(ret));
  } else if (FALSE_IT(is_spm_closed_ = (0 == spm_mode))) {
    // do nothing
#endif
  } else if (OB_ISNULL(pc_malloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pc_allocator has not been initialized.", K(ret));
  } else if (OB_FAIL(ObPlanSet::init_new_set(pc_ctx, plan, outline_param_idx, pc_malloc_))) {
    LOG_WARN("init new set failed", K(ret));
  } else if (OB_FAIL(table_locations_.prepare_allocate_and_keep_count(sql_ctx.partition_infos_.count(),
                                                        *plan_cache_value_->get_pcv_set()->get_allocator()))) {
    LOG_WARN("fail to init table location count", K(ret));
#ifdef OB_BUILD_SPM
  } else if (OB_FAIL(local_evolution_plan_.init(this))) {
    SQL_PC_LOG(WARN, "failed to init local evolution plan", K(ret));
  } else if (OB_FAIL(dist_evolution_plan_.init(this))) {
    SQL_PC_LOG(WARN, "failed to init dist evolution plan", K(ret));
#endif
  } else if (OB_FAIL(dist_plans_.init(this))) {
    SQL_PC_LOG(WARN, "failed to init dist plans", K(ret));
  } else {
    //if (pc_ctx.sql_ctx_.multi_stmt_rowkey_pos_.empty()) {
      //for (int64_t i = 0; i < plan.get_params_info().count(); ++i) {
        //if (ObExtendType == plan.get_params_info().at(i).type_) {
          //has_array_binding_ = true;
        //}
      //}
    //}
    for (int64_t i = 0; !is_contain_virtual_table_ && i < plan.get_dependency_table().count(); i++) {
      const ObSchemaObjVersion &schema_obj = plan.get_dependency_table().at(i);
      if (TABLE_SCHEMA == schema_obj.get_schema_type()
          && is_virtual_table(schema_obj.object_id_)) {
        is_contain_virtual_table_ = true;
        LOG_DEBUG("contain virtual table", K(is_contain_virtual_table_), K(schema_obj));
      }
    } // for end
  }

  bool contain_index_location = false;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (NS_CRSR != plan.get_ns()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cache object type", K(ret), K(plan.get_ns()));
  } else {
    const ObPhysicalPlan &sql_plan = dynamic_cast<const ObPhysicalPlan &>(plan);
    enable_inner_part_parallel_exec_ = sql_plan.get_px_dop() > 1;
    contain_index_location = sql_plan.contain_index_location();
    LOG_DEBUG("using px", K(enable_inner_part_parallel_exec_));
    plan.get_pre_expr_ref_count();
  }
  if (OB_SUCC(ret) && (!contain_index_location || is_multi_stmt_plan())) {
    const ObTablePartitionInfoArray &partition_infos = sql_ctx.partition_infos_;
    int64_t N = partition_infos.count();
    //copy table location
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (NULL == partition_infos.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(TRACE, "invalid partition info");
      } else if (OB_FAIL(table_locations_.push_back(partition_infos.at(i)->get_table_location()))) {
        SQL_PC_LOG(WARN, "fail to push table location", K(ret));
      } else if (is_all_non_partition_
                 && partition_infos.at(i)->get_table_location().is_partitioned()) {
        is_all_non_partition_ = false;
      }
    } // for end
  }

 return ret;
}

int ObSqlPlanSet::select_plan(ObPlanCacheCtx &pc_ctx, ObPlanCacheObject *&cache_obj)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  if (OB_ISNULL(plan_cache_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("location cache not init", K(plan_cache_value_), K(ret));
  } else {
    if (OB_FAIL(get_plan_special(pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret) {
        LOG_TRACE("fail to get plan special", K(ret));
      } else {
        LOG_WARN("fail to get plan special", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(plan) && plan->is_remote_plan()) {
    //记录下not param info和neg_param_index，用于在转发remote sql的时候定位能参数化常量的个数
    pc_ctx.not_param_index_.reset();
    pc_ctx.neg_param_index_.reset();
    if (OB_FAIL(pc_ctx.not_param_index_.add_members2(plan_cache_value_->get_not_param_index()))) {
      LOG_WARN("assign not param info failed", K(ret), K(plan_cache_value_->get_not_param_index()));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(
        plan_cache_value_->get_neg_param_index()))) {
      LOG_WARN("assign neg param index failed", K(ret),
               K(plan_cache_value_->get_neg_param_index()));
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
  } else {/*do nothing*/}

  if (OB_SUCC(ret)) {
    cache_obj = plan;
  }
  return ret;
}

/*
 * 不查询location cache直接获取local plan 条件：
 *  1.该次请求不是在重试中
 *  2.该sql涉及的表裁剪后均为单分区
 *  3.local plan 存在
 *  4.上次执行local plan时open成功
 *
 *  如果直接获取了local plan, 而实际需要的不是local plan，
 *  会在执行阶段open时报错，更新plan中last_execute_result状态并重试。
 * */
//int ObSqlPlanSet::get_local_plan_direct(ObPlanCacheCtx &pc_ctx,
//                                        bool &is_direct_local_plan,
//                                        ObPhysicalPlan *&plan)
//{
//  int ret = OB_SUCCESS;
//  plan = NULL;
//  is_direct_local_plan = false;
//  int64_t N = table_locations_.count();
//  bool is_all_non_partition = true;
//  bool is_leader = false;
//  ObSEArray<TablePart, 4> table_parts;
//  ObExecContext &exec_ctx = pc_ctx.exec_ctx_;
//  ObSchemaGetterGuard *schema_guard = pc_ctx.sql_ctx_.schema_guard_;
//  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(pc_ctx.sql_ctx_.session_info_);
//  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
//  if (OB_ISNULL(schema_guard)
//      || OB_ISNULL(dtc_params.tz_info_)
//      || OB_ISNULL(plan_ctx)
//      || OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("invalid argument", KP(schema_guard), KP(dtc_params.tz_info_),
//             KP(plan_ctx), KP(pc_ctx.sql_ctx_.session_info_), K(ret));
//  }
//
//  //if(!has_partition_group_) LOG_INFO("has pg_key", K(has_partition_group_));
//  ObSEArray<int64_t, 1> partition_ids;
//  bool all_not_contains_pg = plan_cache_value_->is_all_not_contains_pg_key();
//  for (int64_t i = 0; OB_SUCC(ret) && is_all_non_partition && i < N; ++i) {
//    ObPGKey pg_key;
//    uint64_t tg_id = OB_INVALID_ID;
//    int64_t pg_id = OB_INVALID_ID;
//    int64_t p_cnt = OB_INVALID_ID;
//    const ObTableLocation &table_location = table_locations_.at(i);
//    if (OB_ISNULL(plan_cache_value_)) {
//      ret = OB_ERR_UNEXPECTED;
//      LOG_WARN("empty plan cache value");
//    } else if (OB_FAIL(table_location.calculate_partition_ids(exec_ctx,
//                                                       schema_guard,
//                                                       plan_ctx->get_param_store(),
//                                                       partition_ids,
//                                                       dtc_params))) {
//      LOG_WARN("fail to get partition ids", K(ret));
//    } else if (partition_ids.count() != 1) {
//      is_all_non_partition = false;
//    // all table not in partition groups, just init a pg_key
//    } else if (all_not_contains_pg && FALSE_IT(pg_key.init(table_location.get_ref_table_id(),
//                                                partition_ids.at(0),
//                                                table_location.get_partition_cnt()))) {
//    // do nothing
//    } else if (!all_not_contains_pg &&
//                  OB_FAIL(ObSqlPartitionLocationCache::get_phy_key(*schema_guard,
//                                                                   table_location.get_ref_table_id(),
//                                                                   partition_ids.at(0),
//                                                                   tg_id,
//                                                                   pg_id,
//                                                                   pg_key))) {
//      LOG_WARN("fail to get pg key", K(ret));
//    } else if (OB_FAIL(table_parts.push_back(TablePart(table_location.get_table_id(),
//                                                       table_location.get_ref_table_id(),
//                                                       partition_ids.at(0),
//                                                       pg_key)))) {
//      LOG_WARN("fail to push table part", K(ret));
//    }
//    partition_ids.reuse();
//  }
//
//  if (OB_SUCC(ret) && true == is_all_non_partition) { //get local plan directly
//    ObPartitionKey partition_key = partition_key_;
//    // directly get plan when spm is off
//   if (is_spm_acs_closed_ && OB_NOT_NULL(direct_local_plan_)) {
//      plan = direct_local_plan_;
//      plan->inc_ref_count(pc_ctx.handle_id_);
//    } else if (OB_FAIL(gen_partition_key(table_parts, partition_key))) {
//      LOG_WARN("fail to gen partition key", K(ret));
//    } else if (OB_FAIL(local_plans_.get_plan(pc_ctx, partition_key, plan))) {
//      SQL_PC_LOG(DEBUG, "get local plan failed", K(ret));
//    }
//    if (OB_SUCC(ret) && plan != NULL) {
//      int last_retry_err = pc_ctx.sql_ctx_.session_info_
//                             ->get_retry_info().get_last_query_retry_err();
//      if (plan->is_last_exec_succ()) {
//        is_direct_local_plan = true;
//      } else if (pc_ctx.sql_ctx_.session_info_->get_is_in_retry()
//                 && is_local_plan_opt_allowed(last_retry_err)) {
//        is_direct_local_plan = true;
//      } else {
//        is_direct_local_plan = false;
//      }
//    }
//  }
//
//  if (OB_FAIL(ret)) {
//    //do nothing
//  } else if (is_direct_local_plan) {
//    ObPhyTableLocationIArray &phy_locations = exec_ctx.get_task_exec_ctx().get_table_locations();
//    if (OB_FAIL(phy_locations.assign(local_phy_locations_))) { //copy phy table locations
//      SQL_PC_LOG(WARN, "fail to assign phy locations", K(ret));
//    } else if (OB_FAIL(replace_partition_id(phy_locations, table_parts))) {
//      SQL_PC_LOG(WARN, "fail to replace partition id", K(ret));
//    }
//  }
//
//  LOG_DEBUG("get local plan direct", K(ret), KP(plan), K(is_direct_local_plan));
//
//  if (OB_FAIL(ret)) {
//    is_direct_local_plan = false;
//  }
//  exec_ctx.set_direct_local_plan(is_direct_local_plan);
//
//  return ret;
//}

int ObSqlPlanSet::add_physical_plan(const ObPhyPlanType plan_type,
                                    ObPlanCacheCtx &pc_ctx,
                                    ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  if (plan_type != OB_PHY_PLAN_LOCAL && plan_type != OB_PHY_PLAN_DISTRIBUTED) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan type", K(ret), K(plan_type));
#ifndef OB_BUILD_SPM
  } else if (OB_PHY_PLAN_LOCAL == plan_type) {
    if (NULL != local_plan_) {
      ret = OB_SQL_PC_PLAN_DUPLICATE;
    } else {
      local_plan_ = &plan;
    }
  } else if (OB_FAIL(dist_plans_.add_plan(plan, pc_ctx))) {
    LOG_WARN("failed to add dist plan", K(ret), K(plan));
  }
#else
  } else if (!pc_ctx.need_evolution_ || is_spm_closed_) {
    // Addition of other non-evolving plans is prohibited in plan evolution
    if (OB_PHY_PLAN_LOCAL == plan_type) {
      if (local_evolution_plan_.get_is_evolving_flag()) {
        ret = OB_SQL_PC_PLAN_DUPLICATE;
        LOG_WARN("addition of other non-evolving plans is prohibited in local plan evolution");
      } else if (NULL != local_plan_) {
        ret = OB_SQL_PC_PLAN_DUPLICATE;
      } else {
        local_plan_ = &plan;
      }
    } else {
      if (OB_FAIL(dist_plans_.add_plan(plan, pc_ctx))) {
        LOG_WARN("failed to add dist plan", K(ret), K(plan));
      }
    }
  } else {
    // Clear local_plan_ and dist_plans_ before adding evolution plans
    ObSpmCacheCtx& spm_ctx = pc_ctx.sql_ctx_.spm_ctx_;
    if (ObSpmCacheCtx::STAT_ADD_BASELINE_PLAN == spm_ctx.spm_stat_) {
      if (OB_PHY_PLAN_LOCAL == spm_ctx.evolution_plan_type_) {
        OZ (local_evolution_plan_.add_plan(pc_ctx, &plan));
      } else {
        OZ (dist_evolution_plan_.add_plan(pc_ctx, &plan));
      }
    } else {
      if (OB_PHY_PLAN_LOCAL == plan_type) {
        if (NULL != local_plan_) {
          remove_cache_obj_entry(local_plan_->get_plan_id());
          local_plan_ = NULL;
        }
        OZ (local_evolution_plan_.add_plan(pc_ctx, &plan));
        OX (spm_ctx.evolution_plan_type_ = OB_PHY_PLAN_LOCAL);
      } else {
        OZ (dist_plans_.remove_plan_stat());
        OZ (dist_evolution_plan_.add_plan(pc_ctx, &plan));
        OX (spm_ctx.evolution_plan_type_ = OB_PHY_PLAN_DISTRIBUTED);
      }
    }
  }
#endif
  return ret;
}

int ObSqlPlanSet::get_physical_plan(const ObPhyPlanType plan_type,
                                    ObPlanCacheCtx &pc_ctx,
                                    ObPhysicalPlan *&plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  bool get_next = false;
#ifndef OB_BUILD_SPM
  if (plan_type != OB_PHY_PLAN_LOCAL && plan_type != OB_PHY_PLAN_DISTRIBUTED) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan type", K(ret), K(plan_type));
  } else if (OB_PHY_PLAN_LOCAL == plan_type) {
    plan = local_plan_;
  } else if (OB_FAIL(dist_plans_.get_plan(pc_ctx, plan))) {
    LOG_DEBUG("failed to get dist plan", K(ret));
  }
#else
  if (plan_type != OB_PHY_PLAN_LOCAL && plan_type != OB_PHY_PLAN_DISTRIBUTED) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan type", K(ret), K(plan_type));
  } else if (OB_PHY_PLAN_LOCAL == plan_type) {
    if (OB_FAIL(try_get_local_evolution_plan(pc_ctx, plan, get_next))) {
      LOG_WARN("failed to try get local evolution plan", K(ret));
    } else if (get_next) {
      plan = local_plan_;
    }
    if (OB_SUCC(ret) && NULL == plan) {
      ret = OB_SQL_PC_NOT_EXIST;
    }
  } else if (OB_PHY_PLAN_DISTRIBUTED == plan_type) {
    if (OB_FAIL(try_get_dist_evolution_plan(pc_ctx, plan, get_next))) {
      LOG_WARN("failed to try get local evolution plan", K(ret));
    } else if (get_next && OB_FAIL(dist_plans_.get_plan(pc_ctx, plan))) {
      LOG_TRACE("failed to get dist plan", K(ret));
    }
    if (OB_SUCC(ret) && NULL == plan) {
      ret = OB_SQL_PC_NOT_EXIST;
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SPM
int ObSqlPlanSet::add_evolution_plan_for_spm(ObPhysicalPlan *plan, ObPlanCacheCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObPlanCache *pc = NULL;
  if (NULL == plan_cache_value_
      || NULL == plan_cache_value_->get_pcv_set()
      || NULL == plan_cache_value_->get_pcv_set()->get_plan_cache()) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_PHY_PLAN_LOCAL != plan->get_plan_type()
    && OB_PHY_PLAN_DISTRIBUTED != plan->get_plan_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported type", K(ret), K(plan->get_plan_type()));
  } else if (FALSE_IT(pc = plan_cache_value_->get_pcv_set()->get_plan_cache())) {
  } else if (OB_PHY_PLAN_LOCAL == plan->get_plan_type()) {
    if (NULL != local_plan_) {
      ret = OB_SQL_PC_PLAN_DUPLICATE;
      LOG_WARN("local plan duplicate", K(ret));
    } else {
      local_plan_ = plan;
    }
  } else if (OB_FAIL(dist_plans_.add_evolution_plan(*plan, ctx))) {
    LOG_WARN("failed to add dist plan", K(ret), K(plan));
  }
  return ret;
}
#endif

//int ObSqlPlanSet::get_plan_normal(ObPlanCacheCtx &pc_ctx,
//                                  ObPhysicalPlan *&plan)
//{
//  int ret = OB_SUCCESS;
//  plan = NULL;
//  ObSQLSessionInfo *session = pc_ctx.sql_ctx_.session_info_;
//  if (OB_ISNULL(session)) {
//    ret = OB_INVALID_ARGUMENT;
//    LOG_WARN("invalid argument", K(ret));
//  }
//
//  if (OB_SQL_PC_NOT_EXIST == ret
//      || NULL == plan) {
//    pc_ctx.exec_ctx_.set_direct_local_plan(false);
//    // 进入该分支, 说明不走直接获取local plan的优化，
//    // 如果plan不为空, 说明已经拿到执行计划，
//    // 此时已经对该plan的引用计数+1, 在这里需要先减掉引用计数
//    if (NULL != plan) {
//      /*
//       * 以下并发场景会进入该分支
//       *                         切主
//       *     A线程                                  B线程
//       *
//       * 直接获取到local plan                 直接获取local plan
//       *
//       *
//       *  分区不在本地重试
//       *
//       *                                        发现其他线程已经
//       *                                        执行该local计划失败，
//       *                                        重新计算plan type获取
//       *                                        正确remote计划
//       *
//       *   进入plan cache，
//       *   重新计算plan type
//       *   获取remote 计划
//       * */
//      plan = NULL;
//    }
//    ObPhyPlanType plan_type = OB_PHY_PLAN_UNINITIALIZED;
//    typedef ObSEArray<ObCandiTableLoc, 4> PLS;
//    SMART_VAR(PLS, candi_table_locs) {
//      if (enable_inner_part_parallel_exec_) {
//        if (OB_FAIL(get_physical_plan(OB_PHY_PLAN_DISTRIBUTED, pc_ctx, plan))) {
//          LOG_TRACE("failed to get px plan", K(ret));
//        }
//      } else if (OB_FAIL(get_plan_type(table_locations_,
//                                       false,
//                                       pc_ctx,
//                                       candi_table_locs,
//                                       plan_type))) {
//        // ret = OB_SQL_PC_NOT_EXIST;
//        SQL_PC_LOG(TRACE, "failed to get plan type", K(ret));
//      }
//
//      if (OB_SUCC(ret) && !enable_inner_part_parallel_exec_) {
//        NG_TRACE(get_plan_type_end);
//        SQL_PC_LOG(DEBUG, "get plan type before select plan", K(ret), K(plan_type));
//        switch (plan_type) {
//          case OB_PHY_PLAN_LOCAL: {
//            if (/*has_array_binding_||*/ is_multi_stmt_plan()) {
//              if (NULL != array_binding_plan_) {
//                array_binding_plan_->set_dynamic_ref_handle(pc_ctx.handle_id_);
//                plan = array_binding_plan_;
//              }
//            } else if (OB_FAIL(get_physical_plan(OB_PHY_PLAN_LOCAL, pc_ctx, plan))) {
//              LOG_TRACE("failed to get local plan", K(ret));
//            }
//          } break;
//          case OB_PHY_PLAN_REMOTE: {
//            if (NULL != remote_plan_) {
//              remote_plan_->set_dynamic_ref_handle(pc_ctx.handle_id_);
//              plan = remote_plan_;
//            }
//          } break;
//          case OB_PHY_PLAN_DISTRIBUTED: {
//            if (OB_FAIL(get_physical_plan(OB_PHY_PLAN_DISTRIBUTED, pc_ctx, plan))) {
//              if (OB_SQL_PC_NOT_EXIST == ret) {
//                LOG_TRACE("fail to get dist plan", K(ret));
//              } else {
//                LOG_WARN("fail to get dist plan", K(ret));
//              }
//            }
//          } break;
//          default:
//            break;
//        }
//        if (NULL == plan) {
//          ret = OB_SQL_PC_NOT_EXIST;
//        }
//      }
//    }
//  }
//
//  return ret;
//}

#ifdef OB_BUILD_SPM

int ObSqlPlanSet::try_get_local_evolution_plan(ObPlanCacheCtx &pc_ctx,
                                               ObPhysicalPlan *&plan,
                                               bool &get_next)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  get_next = false;
  if ((!is_spm_closed_ && local_evolution_plan_.get_is_evolving_flag())
    || pc_ctx.sql_ctx_.spm_ctx_.force_get_evolution_plan()) {
    if (OB_FAIL(local_evolution_plan_.get_plan(pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      SQL_PC_LOG(TRACE, "get evolution plan failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL == plan) {
    get_next = true;
  }
  return ret;
}

int ObSqlPlanSet::try_get_dist_evolution_plan(ObPlanCacheCtx &pc_ctx,
                                              ObPhysicalPlan *&plan,
                                              bool &get_next)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  get_next = false;
  if ((!is_spm_closed_ && dist_evolution_plan_.get_is_evolving_flag())
    || pc_ctx.sql_ctx_.spm_ctx_.force_get_evolution_plan()) {
    if (OB_FAIL(dist_evolution_plan_.get_plan(pc_ctx, plan))) {
      if (OB_SQL_PC_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      SQL_PC_LOG(TRACE, "get evolution plan failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL == plan) {
    get_next = true;
  }
  return ret;
}

int ObSqlPlanSet::try_get_evolution_plan(ObPlanCacheCtx &pc_ctx,
                                         ObPhysicalPlan *&plan,
                                         bool &get_next)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  get_next = false;
  if (OB_FAIL(try_get_local_evolution_plan(pc_ctx, plan, get_next))) {
    LOG_WARN("failed to try get local evolution plan", K(ret));
  } else if (get_next && OB_FAIL(try_get_dist_evolution_plan(pc_ctx, plan, get_next))) {
    LOG_WARN("failed to try get dist evolution plan", K(ret));
  }
  return ret;
}
#endif

int ObSqlPlanSet::try_get_local_plan(ObPlanCacheCtx &pc_ctx,
                                     ObPhysicalPlan *&plan,
                                     bool &get_next)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  get_next = false;
  ObPhyPlanType real_type = OB_PHY_PLAN_UNINITIALIZED;
  ObSEArray<ObCandiTableLoc, 2> candi_table_locs;
  if (OB_ISNULL(local_plan_)) {
    LOG_DEBUG("local plan is null");
    get_next = true;
  } else if (FALSE_IT(plan = local_plan_)) {
  } else if (OB_FAIL(get_plan_type(plan->get_table_locations(),
                                    plan->has_uncertain_local_operator(),
                                    pc_ctx,
                                    candi_table_locs,
                                    real_type))) {
    LOG_WARN("fail to get plan type", K(ret));
  } else if (OB_PHY_PLAN_LOCAL != real_type) {
    LOG_DEBUG("not local plan", K(real_type));
    plan = NULL;
    get_next = true;
  }
  if (OB_SUCC(ret) && NULL == plan) {
    get_next = true;
  }
  return ret;
}

int ObSqlPlanSet::try_get_remote_plan(ObPlanCacheCtx &pc_ctx,
                                      ObPhysicalPlan *&plan,
                                      bool &get_next)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  get_next = false;
  ObPhyPlanType real_type = OB_PHY_PLAN_UNINITIALIZED;
  ObSEArray<ObCandiTableLoc, 2> candi_table_locs;
  if (OB_ISNULL(remote_plan_)) {
    LOG_DEBUG("remote plan is null");
    get_next = true;
  } else if (OB_FAIL(get_plan_type(remote_plan_->get_table_locations(),
                                  remote_plan_->has_uncertain_local_operator(),
                                  pc_ctx,
                                  candi_table_locs,
                                  real_type))) {
    LOG_WARN("fail to get plan type", K(ret));
  } else if (OB_PHY_PLAN_REMOTE != real_type) {
    LOG_DEBUG("remote type is not match", K(real_type));
    plan = NULL;
    get_next = true;
  } else {
    remote_plan_->set_dynamic_ref_handle(pc_ctx.handle_id_);
    plan = remote_plan_;
  }
  if (OB_SUCC(ret) && NULL == plan) {
    get_next = true;
  }
  return ret;
}

int ObSqlPlanSet::try_get_dist_plan(ObPlanCacheCtx &pc_ctx,
                                    ObPhysicalPlan *&plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  if (OB_FAIL(dist_plans_.get_plan(pc_ctx, plan))) {
    LOG_TRACE("failed to get dist plan", K(ret));
  } else if (plan != NULL) {
    LOG_TRACE("succeed to get dist plan", K(*plan));
  }
  if (OB_SQL_PC_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    plan = NULL;
  }
  return ret;
}

int ObSqlPlanSet::get_plan_special(ObPlanCacheCtx &pc_ctx,
                                   ObPhysicalPlan *&plan)
{
  LOG_DEBUG("get plan special", K(need_try_plan_));
  int ret = OB_SUCCESS;
  plan = NULL;
  bool get_next = true;
  ObPhyPlanType real_type = OB_PHY_PLAN_UNINITIALIZED;
  ObSEArray<ObCandiTableLoc, 2> candi_table_locs;
#ifdef OB_BUILD_SPM
  if (OB_FAIL(try_get_evolution_plan(pc_ctx, plan, get_next))) {
    SQL_PC_LOG(TRACE, "get evolution plan failed", K(ret));
  }
#endif
  // try local plan
  if (OB_SUCC(ret) && get_next) {
    if (OB_FAIL(try_get_local_plan(pc_ctx, plan, get_next))) {
      LOG_WARN("failed to try get local plan", K(ret));
    }
  }
  // try remote plan
  if (OB_SUCC(ret) && get_next) {
    if (OB_FAIL(try_get_remote_plan(pc_ctx, plan, get_next))) {
      LOG_WARN("failed to try get remote plan", K(ret));
    }
  }
  //try dist plan
  if (OB_SUCC(ret) && get_next) {
    if (OB_FAIL(try_get_dist_plan(pc_ctx, plan))) {
      LOG_TRACE("failed to try get dist plan", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr == plan) {
    ret = OB_SQL_PC_NOT_EXIST;
  }
  return ret;
}

int64_t ObSqlPlanSet::get_mem_size()
{
  int64_t plan_set_mem = 0;
#ifdef OB_BUILD_SPM
  plan_set_mem += local_evolution_plan_.get_mem_size();
  plan_set_mem += dist_evolution_plan_.get_mem_size();
#endif
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
  //has_array_binding_ = false;
  is_contain_virtual_table_ = false;
  enable_inner_part_parallel_exec_ = false;
  table_locations_.reset();
  if (OB_ISNULL(plan_cache_value_)
      || OB_ISNULL(plan_cache_value_->get_pc_alloc())) {
    //do nothing
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "plan_cache_value or pc allocator is NULL");
  }
#ifdef OB_BUILD_SPM
  local_evolution_plan_.reset();
  dist_evolution_plan_.reset();
#endif
  local_plan_ = NULL;
  array_binding_plan_ = NULL;
  remote_plan_ = NULL;
  dist_plans_.reset();
  //local_phy_locations_.reset();
  //partition_key_.reset();
  ObPlanSet::reset();
}

//get plan used
//need_check_on_same_server: out, 是否需要检查分区在同一server, 如果里面检查过且不在同一server则置为false
int ObSqlPlanSet::get_phy_locations(const ObIArray<ObTableLocation> &table_locations,
                                    ObPlanCacheCtx &pc_ctx,
                                    ObIArray<ObCandiTableLoc> &candi_table_locs)
{
  int ret = OB_SUCCESS;
  DAS_CTX(pc_ctx.exec_ctx_).clear_all_location_info();
  if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(table_locations,
                                                     pc_ctx,
                                                     candi_table_locs))) {
    LOG_WARN("failed to get phy locations", K(ret), K(table_locations));
  } else if (candi_table_locs.empty()) {
    // do nothing.
  } else if (OB_FAIL(ObPhyLocationGetter::build_candi_table_locs(pc_ctx.exec_ctx_.get_das_ctx(),
                                                                 table_locations,
                                                                 candi_table_locs))) {
    LOG_WARN("fail to init table locs", K(ret));
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
int ObSqlPlanSet::calc_phy_plan_type_v2(const common::ObIArray<ObCandiTableLoc> &candi_table_locs,
                                        const ObPlanCacheCtx &pc_ctx,
                                        ObPhyPlanType &plan_type)
{
  int ret = OB_SUCCESS;
  ObDASCtx &das_ctx = pc_ctx.exec_ctx_.get_das_ctx();
  const DASTableLocList &table_locs = das_ctx.get_table_loc_list();
  int64_t N = table_locs.size();
  if (0 == N) {
    plan_type = OB_PHY_PLAN_LOCAL;
    SQL_PC_LOG(DEBUG, "no table used, thus local plan");
  } else {
    bool is_all_empty = true;
    bool is_all_single_partition = true;
    FOREACH_X(table_loc, table_locs, is_all_single_partition)
    {
      const DASTabletLocList &tablet_locs = (*table_loc)->get_tablet_locs();
      if (tablet_locs.size() != 0) {
        is_all_empty = false;
      }
      if (tablet_locs.size() > 1) {
        is_all_single_partition = false;
      }
    }

    if (is_all_empty) {
      plan_type = OB_PHY_PLAN_LOCAL;
    } else if (is_all_single_partition) {
      if (das_ctx.same_server_) {
        if (GCTX.self_addr() == das_ctx.same_tablet_addr()) {
          plan_type = OB_PHY_PLAN_LOCAL;
        } else {
          plan_type = OB_PHY_PLAN_REMOTE;
        }
      } else {
        plan_type = OB_PHY_PLAN_DISTRIBUTED;
      }
    } else {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
  }
  return ret;
}

int ObSqlPlanSet::calc_phy_plan_type(const ObIArray<ObCandiTableLoc> &candi_table_locs,
                                     ObPhyPlanType &plan_type)
{
  int ret = OB_SUCCESS;
  int64_t phy_location_cnt = candi_table_locs.count();

  if (0 == phy_location_cnt) {
    plan_type = OB_PHY_PLAN_LOCAL;
    SQL_PC_LOG(DEBUG, "no table used, thus local plan");
  } else {
    bool is_all_empty = true;
    bool is_all_single_partition = true;
    for (int i = 0; is_all_single_partition && i < phy_location_cnt; ++i) {
      if (candi_table_locs.at(i).get_partition_cnt() != 0) {
        is_all_empty = false;
      }
      if (candi_table_locs.at(i).get_partition_cnt() > 1) {
        is_all_single_partition = false;
      }
    }
    if (is_all_empty) {
      plan_type = OB_PHY_PLAN_LOCAL;
    } else if (is_all_single_partition) {
      bool is_same = true;
      ObAddr first_addr;
      if (OB_FAIL(is_partition_in_same_server(candi_table_locs, is_same, first_addr))) {
        SQL_PC_LOG(WARN, "fail to calculate whether all partitions in same server",
                   K(ret),
                   K(candi_table_locs));
      } else {
        if (is_same) {
          ObAddr my_address = GCTX.self_addr();
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

//calculate whether all partitions in same server
int ObSqlPlanSet::is_partition_in_same_server(const ObIArray<ObCandiTableLoc> &candi_table_locs,
                                              bool &is_same,
                                              ObAddr &first_addr)
{
  int ret = OB_SUCCESS;
  int64_t phy_location_count = candi_table_locs.count();
  if (phy_location_count > 0) {
    bool is_first = true;
    ObLSReplicaLocation replica_location;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < phy_location_count; ++i) {
      int64_t partition_location_count = candi_table_locs.at(i).get_partition_cnt();
      if (partition_location_count > 0) {
        for (int64_t j = 0; OB_SUCC(ret) && is_same && j < partition_location_count; ++j) {
          replica_location.reset();
          const ObCandiTabletLoc &candi_table_loc = candi_table_locs.at(i).get_phy_part_loc_info_list().at(j);
          if (OB_FAIL(candi_table_loc.get_selected_replica(replica_location))) {
            SQL_PC_LOG(WARN, "fail to get selected replica", K(ret), K(candi_table_loc));
          } else if (!replica_location.is_valid()) {
            SQL_PC_LOG(WARN, "replica_location is invalid", K(ret), K(replica_location));
          } else {
            if (is_first) {
              //get first replica
              first_addr = replica_location.get_server();
              is_same = true;
              is_first = false;
              SQL_PC_LOG(DEBUG, "part_location first replica", K(ret), K(replica_location));
            } else {
              is_same = (replica_location.get_server() == first_addr);
              SQL_PC_LOG(DEBUG, "part_location replica", K(ret), K(i), K(replica_location));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(WARN, "there is no partition_location in this phy_location",
                   K(candi_table_locs.at(i)));
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
#ifdef OB_BUILD_SPM
  IGNORE_RETURN local_evolution_plan_.remove_all_plan();
  IGNORE_RETURN dist_evolution_plan_.remove_all_plan();
#endif
  IGNORE_RETURN dist_plans_.remove_all_plan();
}


//add plan used
int ObSqlPlanSet::get_phy_locations(const ObTablePartitionInfoArray &partition_infos,
                                    //ObIArray<ObDASTableLoc> &table_locs,
                                    ObIArray<ObCandiTableLoc> &candi_table_locs)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("add plan partition infos", K(partition_infos));
  if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(partition_infos,
                                                     //table_locs,
                                                     candi_table_locs))) {
    LOG_WARN("failed to get phy location while adding plan",
             K(ret), K(partition_infos), K(candi_table_locs));
  } else {/* do nothing */}
  return ret;
}

int ObSqlPlanSet::set_concurrent_degree(int64_t outline_param_idx,
                                        ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_cache_value_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (outline_param_idx != OB_INVALID_INDEX) {
    const ObMaxConcurrentParam *param = plan_cache_value_->get_outline_param(outline_param_idx);
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

int ObSqlPlanSet::extend_param_store(const ParamStore &params,
                                     ObIAllocator &allocator,
                                     ObIArray<ParamStore *> &expanded_params) {
  int ret = OB_SUCCESS;
  int64_t count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (params.at(i).is_ext_sql_array()) {
      const ObSqlArrayObj *array_params = reinterpret_cast<const ObSqlArrayObj*>(params.at(i).get_ext());
      if (OB_ISNULL(array_params)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array params is null", K(ret));
      } else {
        count = array_params->count_;
        break;
      }
    }
  }
  ParamStore *tmp_param_store = NULL;
  void *param_store_buf = NULL;
  ParamStore tmp_params( (ObWrapperAllocator(allocator)) );
  for (int64_t vec_cnt = 0; OB_SUCC(ret) && vec_cnt < count; vec_cnt++) {
    if (OB_ISNULL(param_store_buf = allocator.alloc(sizeof(ParamStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      tmp_param_store = new(param_store_buf)ParamStore( ObWrapperAllocator(allocator) );
      if (OB_FAIL(expanded_params.push_back(tmp_param_store))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (params.at(i).is_ext_sql_array()) {
      const ObSqlArrayObj *array_params = reinterpret_cast<const ObSqlArrayObj*>(params.at(i).get_ext());
      if (OB_ISNULL(array_params) || OB_ISNULL(array_params->data_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("array params is null", K(ret), KPC(array_params));
      }
      for (int64_t vec_cnt = 0; OB_SUCC(ret) && vec_cnt < count; vec_cnt++) {
        ObObjParam obj_param(array_params->data_[vec_cnt]);
        obj_param.set_accuracy(params.at(i).get_accuracy());
        obj_param.set_result_flag(params.at(i).get_result_flag());
        if (OB_FAIL(expanded_params.at(vec_cnt)->push_back(obj_param))) {
          LOG_WARN("fail to push param", K(ret), K(vec_cnt));
        }
      }
    } else {
      for (int64_t vec_cnt = 0; OB_SUCC(ret) && vec_cnt < count; vec_cnt++) {
        if (OB_FAIL(expanded_params.at(vec_cnt)->push_back(params.at(i)))) {
          LOG_WARN("fail to push param", K(ret), K(vec_cnt));
        }
      }
    }
  } // for

  LOG_DEBUG("extend param store", K(ret), K(count), K(expanded_params), K(params));
  return ret;
}

int ObSqlPlanSet::get_plan_type(const ObIArray<ObTableLocation> &table_locations,
                                const bool is_contain_uncertain_op,
                                ObPlanCacheCtx &pc_ctx,
                                ObIArray<ObCandiTableLoc> &candi_table_locs,
                                ObPhyPlanType &plan_type)
{
  int ret = OB_SUCCESS;
  candi_table_locs.reuse();

  if (OB_FAIL(get_phy_locations(table_locations,
                                pc_ctx,
                                candi_table_locs))) {
    LOG_WARN("failed to get physical locations", K(ret));
  } else if (OB_FAIL(calc_phy_plan_type_v2(candi_table_locs,
                                           pc_ctx,
                                           plan_type))) {
    LOG_WARN("failed to calcute physical plan type", K(ret));
  } else {
    // Lookup算子支持压到远程去执行:
    //
    // Select的sql如果包含uncertain算子，不能将类型改为分布式计划
    if (is_contain_uncertain_op && plan_type != OB_PHY_PLAN_LOCAL
        && stmt::T_SELECT != stmt_type_) {
      plan_type = OB_PHY_PLAN_DISTRIBUTED;
    }
    LOG_DEBUG("get plan type", K(table_locations),
              K(plan_type), K(candi_table_locs),
              K(is_contain_uncertain_op), K(stmt_type_), K(ret));
  }

  return ret;
}

bool ObSqlPlanSet::is_local_plan_opt_allowed(const int last_retry_err)
{
  bool ret_bool = false;

  if (OB_SUCCESS == last_retry_err) {
    ret_bool = true;
  } else if (last_retry_err == OB_TRANSACTION_SET_VIOLATION
             || last_retry_err == OB_TRY_LOCK_ROW_CONFLICT) {
    // 直接选取local计划，因为leader并没有变化
    ret_bool = true;
  } else {
    ret_bool = false;
  }
  LOG_DEBUG("use direct local plan",
            K(ret_bool),
            K(last_retry_err));
  return ret_bool;
}

bool ObSqlPlanSet::is_sql_planset()
{
  return true;
}

#ifdef OB_BUILD_SPM
int ObSqlPlanSet::get_evolving_evolution_task(EvolutionPlanList &evo_task_list)
{
  int ret = OB_SUCCESS;
  if (local_evolution_plan_.get_is_evolving_flag() &&
      OB_FAIL(evo_task_list.push_back(&local_evolution_plan_))) {
    LOG_WARN("failed to push back evolution plan", K(ret));
  } else if (dist_evolution_plan_.get_is_evolving_flag() &&
             OB_FAIL(evo_task_list.push_back(&dist_evolution_plan_))) {
    LOG_WARN("failed to push back evolution plan", K(ret));
  }
  return ret;
}
#endif

}

bool ObPlanSet::match_decint_precision(const ObParamInfo &param_info, ObPrecision other_prec) const
{
  bool ret = false;
  if (ob_is_decimal_int(param_info.type_)) {
    ret = (param_info.precision_ == other_prec);
  } else {
    // not decimal_int, return true
    ret = true;
  }
  return ret;
}

}
