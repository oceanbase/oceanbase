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
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/ob_sql_context.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_cluster_version.h"
#include "share/config/ob_server_config.h"
#include "lib/container/ob_bit_set.h"
#include "lib/charset/ob_charset.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

int ObPCVSet::init(ObILibCacheCtx &ctx, const ObILibCacheObject *obj)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* sess;
#ifdef OB_BUILD_SPM
  bool is_spm_on = false;
#endif
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  const ObPlanCacheObject *cache_obj = static_cast<const ObPlanCacheObject*>(obj);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(lib_cache_) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (NULL == (sess = pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "session info is null", K(ret));
#ifdef OB_BUILD_SPM
  } else if (OB_FAIL(sess->get_use_plan_baseline(is_spm_on))) {
    LOG_WARN("fail to get spm config");
  } else if (FALSE_IT(is_spm_closed_ = (!is_spm_on))) {
    // do nothing
#endif
  } else if (NULL == (pc_alloc_ = lib_cache_->get_pc_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan cache allocator", K_(pc_alloc), K(ret));
  } else {
    if (OB_FAIL(pc_key_.deep_copy(allocator_, pc_ctx.fp_result_.pc_key_))) {
      LOG_WARN("fail to init plan cache key in pcv set", K(ret));
    } else if (OB_FAIL(deep_copy_sql(pc_ctx.raw_sql_))) {
      LOG_WARN("fail to deep_copy_sql", K(ret), K(pc_ctx.raw_sql_));
    } else {
      is_inited_ = true;
      if (pc_ctx.exec_ctx_.get_min_cluster_version() != GET_MIN_CLUSTER_VERSION()) {
        LOG_TRACE("Lob Debug, using remote min cluster version",
                K(pc_ctx.exec_ctx_.get_min_cluster_version()),
                K(GET_MIN_CLUSTER_VERSION()));
      }
      min_cluster_version_ = pc_ctx.exec_ctx_.get_min_cluster_version();
      normal_parse_const_cnt_ = pc_ctx.normal_parse_const_cnt_;
      LOG_TRACE("inited pcv set", K(pc_key_), K(ObTimeUtility::current_time()));
    }
  }
  return ret;
}

void ObPCVSet::destroy()
{
  if (is_inited_) {
    while (!pcv_list_.is_empty()) {
      ObPlanCacheValue *pcv= pcv_list_.get_first();
      if (OB_ISNULL(pcv)) {
        //do nothing;
      } else {
        pcv_list_.remove(pcv);
        free_pcv(pcv);
        pcv = NULL;
      }
    }
    pc_key_.destory(allocator_);
    pc_key_.reset();
    if (NULL != sql_.ptr()) {
      allocator_.free(sql_.ptr());
    }

    // free sql_id list
    for (int64_t i = 0; i < sql_ids_.count(); i++) {
      if (NULL != sql_ids_[i].ptr()) {
        allocator_.free(sql_ids_[i].ptr());
      }
    }
    sql_ids_.reset();
    sql_.reset();
    normal_parse_const_cnt_ = 0;
    min_cluster_version_ = 0;
    plan_num_ = 0;
    need_check_gen_tbl_col_ = false;
    is_inited_ = false;
  }
}

//1.检查fast parser识别常量个数是否与正常parser识别常量个数一致
//2.通过match 每个pcv中db_id, sys_vars, not params找到对应的pcv
//3.在pcv中get plan
int ObPCVSet::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SPM
  bool is_spm_on = false;
#endif
  ObPlanCacheObject *plan = NULL;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
    if (normal_parse_const_cnt_ != pc_ctx.fp_result_.parameterized_params_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param num is not equal", K_(normal_parse_const_cnt),
               "parameterized_params_count", pc_ctx.fp_result_.parameterized_params_.count());
    }
  } else {
    if (normal_parse_const_cnt_ != pc_ctx.fp_result_.raw_params_.count()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(TRACE, "const number of fast parse and normal parse is different",
                 "fast_parse_const_num", pc_ctx.fp_result_.raw_params_.count(),
                 K_(normal_parse_const_cnt),
                 K(pc_ctx.fp_result_.raw_params_));
    }
  }
  if (pc_ctx.exec_ctx_.get_min_cluster_version() != GET_MIN_CLUSTER_VERSION()) {
    LOG_TRACE("Lob Debug, using remote min cluster version",
             K(pc_ctx.exec_ctx_.get_min_cluster_version()),
             K(GET_MIN_CLUSTER_VERSION()));
  }
  if (OB_FAIL(ret)) {
  } else if (min_cluster_version_ != pc_ctx.exec_ctx_.get_min_cluster_version()) {
    ret = OB_OLD_SCHEMA_VERSION;
  } else if (need_check_gen_tbl_col_) {
    // 检查是否有generated table投影列同名的可能
    bool contain_dup_col = false;
    if (OB_FAIL(check_raw_param_for_dup_col(pc_ctx, contain_dup_col))) {
      LOG_WARN("failed to check raw param for dup col", K(ret));
    } else if (contain_dup_col) {
      ret = OB_SQL_PC_NOT_EXIST;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_) ||
             OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null schema guard",
             K(ret), K(pc_ctx.sql_ctx_.schema_guard_), K(pc_ctx.sql_ctx_.session_info_));
#ifdef OB_BUILD_SPM
  } else if (OB_FAIL(pc_ctx.sql_ctx_.session_info_->get_use_plan_baseline(is_spm_on))) {
    LOG_WARN("failed to get spm status", K(ret));
  } else if (is_spm_closed_ != (!is_spm_on)) {
    // spm param is altered
    ret = OB_OLD_SCHEMA_VERSION;
#endif
  } else {
    ObSEArray<PCVSchemaObj, 4> schema_array;
    //plan cache匹配临时表应该始终使用用户创建的session才能保证语义的正确性
    //远端执行的时候会创建临时的session对象，其session_id也是临时的，
    //所以这里必须使用get_sessid_for_table()规则去判断
    pc_ctx.sql_ctx_.schema_guard_->set_session_id(
        pc_ctx.sql_ctx_.session_info_->get_sessid_for_table());
    ObPlanCacheValue *matched_pcv = NULL;
    int64_t new_tenant_schema_version = OB_INVALID_VERSION;
    bool need_check_schema = true;
    DLIST_FOREACH(pcv, pcv_list_) {
      bool is_same = false;
      LOG_TRACE("get plan, pcv", K(pcv));
      if (OB_FAIL(pcv->get_all_dep_schema(pc_ctx,
                                          pc_ctx.sql_ctx_.session_info_->get_database_id(),
                                          new_tenant_schema_version,
                                          need_check_schema,
                                          schema_array))) {
        if (OB_OLD_SCHEMA_VERSION == ret) {
          LOG_TRACE("failed to get all table schema", K(ret));
        } else {
          LOG_WARN("failed to get all table schema", K(ret));
        }
      } else if (OB_FAIL(pcv->match(pc_ctx, schema_array, is_same))) {
        LOG_WARN("fail to match pcv when get plan", K(ret));
      } else if (false == is_same) {
        LOG_TRACE("failed to match param");
        /*do nothing*/
      } else {
        matched_pcv = pcv;
        if (OB_FAIL(pcv->choose_plan(pc_ctx, schema_array, plan))) {
          LOG_TRACE("failed to get plan in plan cache value", K(ret));
        }
        break;
      }
    } //end foreach
    // 如果tenant schema变化了, 但table schema没有过期, 则更新tenant schema
    // plan必须不为NULL，因为下层可能会有覆盖错误码的行为，即使计划没有匹配上，
    // ret仍然为success，此时tenant schema version又会被推高，可能有正确性问题
    // bug link：
    if (OB_SUCC(ret) && NULL != matched_pcv && NULL != plan) {
      if (OB_FAIL(matched_pcv->lift_tenant_schema_version(new_tenant_schema_version))) {
        LOG_WARN("failed to lift pcv's tenant schema version", K(ret));
      } else if (!need_check_schema && OB_NOT_NULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
        pc_ctx.exec_ctx_.get_physical_plan_ctx()->set_tenant_schema_version(new_tenant_schema_version);
      }
    }
  }
  if (OB_SUCC(ret)) {
    cache_obj = plan;
    if (NULL == plan) {
      ret = OB_SQL_PC_NOT_EXIST;
    }
  }
  return ret;
}

//1.通过match 每个pcv中db_id, sys_vars, not params找到对应的pcv
//2.如果匹配的pcv则在pcv中add plan， 否则重新生成pcv并add plan
int ObPCVSet::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
  bool is_new = true;
  ObPlanCacheObject *plan = static_cast<ObPlanCacheObject*>(cache_obj);
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  ObSEArray<PCVSchemaObj, 4> schema_array;
  if (OB_ISNULL(plan) ||
      OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_) ||
      OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(plan),
             K(pc_ctx.sql_ctx_.schema_guard_),
             K(pc_ctx.sql_ctx_.session_info_));
  } else if (get_plan_num() >= MAX_PCV_SET_PLAN_NUM) {
    static const int64_t PRINT_PLAN_EXCEEDS_LOG_INTERVAL = 20 * 1000 * 1000; // 20s
    ret = OB_ERR_UNEXPECTED;
    if (REACH_TIME_INTERVAL(PRINT_PLAN_EXCEEDS_LOG_INTERVAL)) {
      LOG_INFO("number of plans in a single pcv_set reach limit", K(ret), K(get_plan_num()), K(pc_ctx));
    }
  } else if (OB_FAIL(ObPlanCacheValue::get_all_dep_schema(*pc_ctx.sql_ctx_.schema_guard_,
                                                          plan->get_dependency_table(),
                                                          schema_array))) {
    LOG_WARN("failed to get all dep schema", K(ret));
  } else {
    DLIST_FOREACH(pcv, pcv_list_) {
      bool is_same = false;
      LOG_DEBUG("add plan, pcv", K(pcv));
      if (OB_FAIL(pcv->match(pc_ctx, schema_array, is_same))) {
        LOG_WARN("fail to match pcv in pcv_set", K(ret));
      } else if (is_same) {
        is_new = false;
        LOG_INFO("has identical pcv", K(is_same), K(pcv));
        if (OB_FAIL(pcv->add_plan(*plan, schema_array, pc_ctx))) {
          if (OB_SQL_PC_PLAN_DUPLICATE == ret
              || is_not_supported_err(ret)) {
            LOG_TRACE("fail to add plan to pcv", K(ret));
          } else {
            LOG_WARN("fail to add plan to pcv", K(ret));
          }
        }
        break;
      }
    } //for end
  }
  if (OB_SUCC(ret) && is_new) {
    ObPlanCacheValue *pcv = NULL;
    if (OB_FAIL(create_pcv_and_add_plan(plan,
                                        pc_ctx,
                                        schema_array,
                                        pcv))) {
      if (!is_not_supported_err(ret)) {
        LOG_WARN("fail to create and add pcv", K(ret));
      }
    } else if (NULL == pcv) {
      ret = OB_ERR_UNEXPECTED;
    } else if (!pcv_list_.add_last(pcv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add pcv to pcv_list", K(ret));
      free_pcv(pcv);
      pcv = NULL;
    } else {
      // do nothing
    }
  }

  //在add plan时维护一个最小的merged version, 用于后台在淘汰时进行检查;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_raw_param_info_if_needed(plan))) {
      LOG_WARN("failed to set raw param info", K(ret));
    } else {
      // inc plan num if succeeds
      plan_num_ += 1;
    }
  }
  return ret;
}

//生成pcv并add plan
int ObPCVSet::create_pcv_and_add_plan(ObPlanCacheObject *cache_obj,
                                      ObPlanCacheCtx &pc_ctx,
                                      const ObIArray<PCVSchemaObj> &schema_array,
                                      ObPlanCacheValue *&new_pcv)
{
  int ret = OB_SUCCESS;
  new_pcv = nullptr;
  common::ObString sql_id;
  common::ObString sql_id_org(common::OB_MAX_SQL_ID_LENGTH, (const char*)&pc_ctx.sql_ctx_.sql_id_);
  //create pcv and init
  if (OB_ISNULL(cache_obj) ||
      OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(create_new_pcv(new_pcv))) {
    SQL_PC_LOG(WARN, "failed to create new plan cache value", K(ret));
  } else if (OB_UNLIKELY(nullptr == new_pcv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null for new_pcv", K(new_pcv));
  } else if (OB_FAIL(new_pcv->init(this, cache_obj, pc_ctx))) {
    SQL_PC_LOG(WARN, "failed to init plan cache value");
  } else if (OB_FAIL(ob_write_string(allocator_, sql_id_org, sql_id))) {
    SQL_PC_LOG(WARN, "failed to deep copy sql_id_", K(sql_id_org), K(ret));
  } else if (OB_FAIL(push_sql_id(sql_id))) {
    SQL_PC_LOG(WARN, "failed to push sql_id_", K(ret));
  } else {
    // do nothing
  }

  //add plan
  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_pcv->add_plan(*cache_obj, schema_array, pc_ctx))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "failed to add plan to plan cache value",  K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && nullptr != new_pcv) {
    free_pcv(new_pcv);
    new_pcv = nullptr;
  }
  return ret;
}

int ObPCVSet::deep_copy_sql(const ObString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql.ptr())) {
    SQL_PC_LOG(TRACE, "sql is empty, ignore copy sql", K(ret), K(lbt()));
  } else if (OB_FAIL(ob_write_string(allocator_, sql, sql_))) {
    SQL_PC_LOG(WARN, "deep copy sql into pcv_set failed", K(ret), K(sql));
  }
  return ret;
}

int ObPCVSet::create_new_pcv(ObPlanCacheValue *&new_pcv)
{
  int ret = OB_SUCCESS;
  void *buff = nullptr;
  new_pcv = nullptr;

  if (nullptr == (buff = allocator_.alloc(sizeof(ObPlanCacheValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObPlanCacheValue", K(ret));
  } else if (nullptr == (new_pcv = new(buff)ObPlanCacheValue())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct ObPlanCacheValue", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (nullptr != new_pcv) { // cleanup
    new_pcv->~ObPlanCacheValue();
    allocator_.free(new_pcv);
    new_pcv = nullptr;
  }

  return ret;
}

void ObPCVSet::free_pcv(ObPlanCacheValue *pcv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pcv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pcv), K(ret));
  } else {
    pcv->~ObPlanCacheValue();
    allocator_.free(pcv);
    pcv = nullptr;
  }
}

int ObPCVSet::set_raw_param_info_if_needed(ObPlanCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  col_field_arr_.reset();
  col_field_arr_.set_allocator(&allocator_);
  ObBitSet<> visited_idx;
  PCColStruct col_item;
  if (need_check_gen_tbl_col_) {
    // do nothing
  } else if (OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cache_obj));
  } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan *>(cache_obj))) {
    // do nothing
  } else if (!plan->contain_paramed_column_field()) {
    // do nothing
  } else if (OB_FAIL(col_field_arr_.reserve(plan->get_field_columns().count()))) {
    LOG_WARN("failed to init fixed array",
             K(ret), K(plan->get_field_columns().count()));
  } else {
    for (int64_t i = 0; i < plan->get_field_columns().count(); i++) {
      const ObField &cur_field = plan->get_field_columns().at(i);
      if (!cur_field.is_paramed_select_item_ || OB_ISNULL(cur_field.paramed_ctx_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument", K(ret), K(cur_field.is_paramed_select_item_), K(cur_field.paramed_ctx_));
      } else if (visited_idx.has_member(i) || !cur_field.paramed_ctx_->need_check_dup_name_) {
        // do nothing
      } else {
        if (OB_FAIL(visited_idx.add_member(i))) {
          LOG_WARN("failed to add member", K(ret), K(i));
        } else if (OB_FAIL(col_item.param_idxs_.assign(cur_field.paramed_ctx_->param_idxs_))) {
          LOG_WARN("failed to assign array", K(ret));
        } else if (OB_FAIL(col_field_arr_.push_back(col_item))) {
          LOG_WARN("failed to push back element", K(ret));
        } else {
          // do nothing
        }
        col_item.reset();
        for (int64_t j = i + 1; OB_SUCC(ret) && j < plan->get_field_columns().count(); j++) {
          const ObField &next_field = plan->get_field_columns().at(j);
          if (!next_field.is_paramed_select_item_ || OB_ISNULL(next_field.paramed_ctx_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid argument", K(ret), K(next_field.is_paramed_select_item_), K(next_field.paramed_ctx_));
          } else if (visited_idx.has_member(j) || !next_field.paramed_ctx_->need_check_dup_name_) {
            // do nothing
          } else if (ObCharset::case_compat_mode_equal(cur_field.paramed_ctx_->paramed_cname_,
                                                       next_field.paramed_ctx_->paramed_cname_)) {
            if (OB_FAIL(visited_idx.add_member(j))) {
              LOG_WARN("failed to add member", K(ret));
            } else if (OB_FAIL(col_item.param_idxs_.assign(next_field.paramed_ctx_->param_idxs_))) {
              LOG_WARN("failed to assign array", K(ret));
            } else if (OB_FAIL(col_field_arr_.push_back(col_item))) {
              LOG_WARN("failed to push back element");
            } else {
              col_item.reset();
            }
          } else {
            // do nothing
          }
        } // for end
        if (OB_SUCC(ret)) {
          col_field_arr_.at(col_field_arr_.count() - 1).is_last_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      need_check_gen_tbl_col_ = true;
    }
  }
  return ret;
}

int ObPCVSet::check_raw_param_for_dup_col(ObPlanCacheCtx &pc_ctx, bool &contain_dup_col)
{
  int ret = OB_SUCCESS;
  contain_dup_col = false;
  if (!need_check_gen_tbl_col_) {
    // do nothing
  } else {
    for (int64_t cur_idx = 0; OB_SUCC(ret) && !contain_dup_col && cur_idx < col_field_arr_.count();
         cur_idx++) {
      if (col_field_arr_.at(cur_idx).is_last_) {
        // do nothing
      } else {
        PCColStruct &left_col_item = col_field_arr_.at(cur_idx);
        PCColStruct &right_col_item = col_field_arr_.at(cur_idx + 1);
        bool all_same = true;
        ObIArray<ObPCParam *> &raw_params = pc_ctx.fp_result_.raw_params_;
        for (int64_t i = 0; OB_SUCC(ret) && all_same && i < left_col_item.param_idxs_.count(); i++) {
          int64_t l_idx = left_col_item.param_idxs_.at(i);
          int64_t r_idx = right_col_item.param_idxs_.at(i);
          if (OB_ISNULL(raw_params.at(l_idx)) ||
              OB_ISNULL(raw_params.at(r_idx)) ||
              OB_ISNULL(raw_params.at(l_idx)->node_) ||
              OB_ISNULL(raw_params.at(r_idx)->node_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret));
          } else {
            ObString l_tmp_str(raw_params.at(l_idx)->node_->text_len_,
                               raw_params.at(l_idx)->node_->raw_text_);
            ObString r_tmp_str(raw_params.at(r_idx)->node_->text_len_,
                               raw_params.at(r_idx)->node_->raw_text_);

            if (0 != l_tmp_str.compare(r_tmp_str)) {
              all_same = false;
              LOG_TRACE("raw text not matched", K(l_tmp_str), K(r_tmp_str));
            }
          }
        } // for end
        contain_dup_col = all_same;
      }
    }
  }
  return ret;
}
int ObPCVSet::check_contains_table(uint64_t db_id, common::ObString tab_name, bool &contains)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(pcv, pcv_list_) {
    if (OB_ISNULL(pcv)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(pcv), K(ret));
    } else if (OB_FAIL(pcv->check_contains_table(db_id, tab_name, contains))) {
      LOG_WARN("fail to check table name", K(ret), K(db_id), K(tab_name));
    } else if (!contains) {
      // continue find
    } else {
      break;
    }
  }
  return ret;
}

#ifdef OB_BUILD_SPM
int ObPCVSet::get_evolving_evolution_task(EvolutionPlanList &evo_task_list)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(pcv, pcv_list_) {
    if (OB_ISNULL(pcv)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(pcv), K(ret));
    } else if (OB_FAIL(pcv->get_evolving_evolution_task(evo_task_list))) {
      LOG_WARN("fail to get evolving evolution task", K(ret));
    }
  }
  return ret;
}
#endif

}
}
