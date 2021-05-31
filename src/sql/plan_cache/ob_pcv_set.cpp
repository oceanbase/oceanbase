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

namespace oceanbase {
namespace sql {

int ObPCVSet::init(const ObPlanCacheCtx& pc_ctx, const ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(plan_cache_) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (NULL == (pc_alloc_ = plan_cache_->get_pc_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid plan cache allocator", K_(pc_alloc), K(ret));
  } else {
    if (OB_FAIL(pc_key_.deep_copy(*pc_alloc_, pc_ctx.fp_result_.pc_key_))) {
      LOG_WARN("fail to init plan cache key in pcv set", K(ret));
    } else if (OB_FAIL(deep_copy_sql(pc_ctx.raw_sql_))) {
      LOG_WARN("fail to deep_copy_sql", K(ret), K(pc_ctx.raw_sql_));
    } else {
      is_inited_ = true;
      min_merged_version_ = cache_obj->get_merged_version();
      min_cluster_version_ = GET_MIN_CLUSTER_VERSION();
      normal_parse_const_cnt_ = pc_ctx.normal_parse_const_cnt_;
      LOG_DEBUG("inited pcv set", K(pc_key_), K(ObTimeUtility::current_time()));
    }
  }

  return ret;
}

void ObPCVSet::destroy()
{
  if (is_inited_) {
    while (!pcv_list_.is_empty()) {
      ObPlanCacheValue* pcv = pcv_list_.get_first();
      if (OB_ISNULL(pcv)) {
        // do nothing;
      } else {
        pcv_list_.remove(pcv);
        free_pcv(pcv);
        pcv = NULL;
      }
    }
    // free key.
    if (NULL != pc_alloc_) {
      pc_key_.destory(*pc_alloc_);
      pc_key_.reset();
      if (NULL != sql_.ptr()) {
        pc_alloc_->free(sql_.ptr());
      }
    }

    sql_.reset();
    plan_cache_ = NULL;
    pc_alloc_ = NULL;
    ref_count_ = 0;
    normal_parse_const_cnt_ = 0;
    min_merged_version_ = 0;
    min_cluster_version_ = 0;
    plan_num_ = 0;
    need_check_gen_tbl_col_ = false;
    is_inited_ = false;
  }
}

// 1. Check whether the number of constants recognized by the fast parser is consistent with the
//   number of constants recognized by the normal parser
// 2. Find the corresponding pcv through match db_id, sys_vars, not params in each pcv
// 3. Get plan in pcv
int ObPCVSet::get_plan(ObPlanCacheCtx& pc_ctx, ObCacheObject*& plan)
{
  int ret = OB_SUCCESS;
  if (pc_ctx.is_ps_mode_) {
    if (normal_parse_const_cnt_ != pc_ctx.fp_result_.ps_params_.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("param num is not equal",
          K_(normal_parse_const_cnt),
          "ps_params_count",
          pc_ctx.fp_result_.ps_params_.count());
    }
  } else {
    if (normal_parse_const_cnt_ != pc_ctx.fp_result_.raw_params_.count()) {
      ret = OB_NOT_SUPPORTED;
      SQL_PC_LOG(DEBUG,
          "const number of fast parse and normal parse is different",
          "fast_parse_const_num",
          pc_ctx.fp_result_.raw_params_.count(),
          K_(normal_parse_const_cnt),
          K(pc_ctx.fp_result_.raw_params_));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (min_cluster_version_ != GET_MIN_CLUSTER_VERSION()) {
    ret = OB_OLD_SCHEMA_VERSION;
  }
  // Check if there is a possibility that the generated table projection column has the same name
  if (OB_SUCC(ret) && need_check_gen_tbl_col_) {
    bool contain_dup_col = false;
    if (OB_FAIL(check_raw_param_for_dup_col(pc_ctx, contain_dup_col))) {
      LOG_WARN("failed to check raw param for dup col", K(ret));
    } else if (contain_dup_col) {
      ret = OB_SQL_PC_NOT_EXIST;
    } else {
      // do nothing
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_) || OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "unexpected null schema guard", K(ret), K(pc_ctx.sql_ctx_.schema_guard_), K(pc_ctx.sql_ctx_.session_info_));
  } else {
    plan = NULL;
    ObSEArray<PCVSchemaObj, 4> schema_array;
    // The plan cache matching temporary table should always use the user-created session to ensure semantic correctness
    // a temporary session object will be created during remote execution, and its session_id is also temporary
    // so here must use get_sessid_for_table() rule to judge
    pc_ctx.sql_ctx_.schema_guard_->set_session_id(pc_ctx.sql_ctx_.session_info_->get_sessid_for_table());
    ObPlanCacheValue* matched_pcv = NULL;
    int64_t new_tenant_schema_version = OB_INVALID_VERSION;
    bool need_check_schema = true;
    DLIST_FOREACH(pcv, pcv_list_)
    {
      bool is_same = false;
      LOG_DEBUG("get plan, pcv", K(pcv));
      if (OB_ISNULL(pcv)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(pcv), K(ret));
      } else if (OB_FAIL(pcv->get_all_dep_schema(pc_ctx,
                     pc_ctx.sql_ctx_.session_info_->get_database_id(),
                     new_tenant_schema_version,
                     need_check_schema,
                     schema_array))) {
        if (OB_OLD_SCHEMA_VERSION == ret) {
          LOG_DEBUG("failed to get all table schema", K(ret));
        } else {
          LOG_WARN("failed to get all table schema", K(ret));
        }
      } else if (OB_FAIL(pcv->match(pc_ctx, schema_array, is_same))) {
        LOG_WARN("fail to match pcv when get plan", K(ret));
      } else if (false == is_same) {
        LOG_DEBUG("failed to match param");
        /*do nothing*/
      } else {
        matched_pcv = pcv;
        if (OB_FAIL(pcv->choose_plan(pc_ctx, schema_array, plan))) {
          LOG_DEBUG("failed to get plan in plan cache value", K(ret));
        }
        break;
      }
    }  // end foreach
    if (OB_SUCC(ret) && NULL != matched_pcv && NULL != plan) {
      if (OB_FAIL(matched_pcv->lift_tenant_schema_version(new_tenant_schema_version))) {
        LOG_WARN("failed to lift pcv's tenant schema version", K(ret));
      } else if (!need_check_schema && OB_NOT_NULL(pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
        pc_ctx.exec_ctx_.get_physical_plan_ctx()->set_tenant_schema_version(new_tenant_schema_version);
      }
    }
  }
  if (OB_SUCCESS == ret && NULL == plan) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  return ret;
}

// 1. find the corresponding pcv through match db_id, sys_vars, not params in each pcv
// 2. if the matching pcv, add plan in pcv, otherwise regenerate pcv and add plan
int ObPCVSet::add_cache_obj(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  bool is_new = true;
  ObSEArray<PCVSchemaObj, 4> schema_array;
  if (OB_ISNULL(cache_obj) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_) || OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(ret), K(cache_obj), K(pc_ctx.sql_ctx_.schema_guard_), K(pc_ctx.sql_ctx_.session_info_));
  } else if (get_plan_num() >= MAX_PCV_SET_PLAN_NUM) {
    static const int64_t PRINT_PLAN_EXCEEDS_LOG_INTERVAL = 20 * 1000 * 1000;  // 20s
    ret = OB_NOT_SUPPORTED;
    if (REACH_TIME_INTERVAL(PRINT_PLAN_EXCEEDS_LOG_INTERVAL)) {
      LOG_INFO("number of plans in a single pcv_set reach limit", K(ret), K(get_plan_num()), K(pc_ctx));
    }
  } else if (OB_FAIL(ObPlanCacheValue::get_all_dep_schema(
                 *pc_ctx.sql_ctx_.schema_guard_, cache_obj->get_dependency_table(), schema_array))) {
    LOG_WARN("failed to get all dep schema", K(ret));
  } else {
    DLIST_FOREACH(pcv, pcv_list_)
    {
      bool is_same = false;
      LOG_DEBUG("add plan, pcv", K(pcv));
      if (OB_ISNULL(pcv)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(pcv), K(ret));
      } else if (OB_FAIL(pcv->match(pc_ctx, schema_array, is_same))) {
        LOG_WARN("fail to match pcv in pcv_set", K(ret));
      } else if (is_same) {
        is_new = false;
        if (OB_FAIL(pcv->add_plan(*cache_obj, schema_array, pc_ctx))) {
          if (OB_SQL_PC_PLAN_DUPLICATE == ret || is_not_supported_err(ret)) {
            LOG_DEBUG("fail to add plan to pcv", K(ret));
          } else {
            LOG_WARN("fail to add plan to pcv", K(ret));
          }
        }
        break;
      }
    }  // for end
  }

  if (OB_SUCC(ret) && is_new) {
    ObPlanCacheValue* pcv = NULL;
    if (OB_FAIL(create_pcv_and_add_plan(cache_obj, pc_ctx, schema_array, pcv))) {
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

  if (OB_SUCC(ret)) {
    cache_obj->set_added_pc(true);
    if (min_merged_version_ > cache_obj->get_merged_version()) {
      min_merged_version_ = cache_obj->get_merged_version();
    }

    if (OB_FAIL(set_raw_param_info_if_needed(cache_obj))) {
      LOG_WARN("failed to set raw param info", K(ret));
    } else {
      // inc plan num if succeeds
      plan_num_ += 1;
    }
  }
  return ret;
}

int ObPCVSet::create_pcv_and_add_plan(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx,
    const ObIArray<PCVSchemaObj>& schema_array, ObPlanCacheValue*& new_pcv)
{
  int ret = OB_SUCCESS;
  new_pcv = nullptr;
  // create pcv and init
  if (OB_ISNULL(cache_obj) || OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(create_new_pcv(new_pcv))) {
    SQL_PC_LOG(WARN, "failed to create new plan cache value", K(ret));
  } else if (OB_UNLIKELY(nullptr == new_pcv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null for new_pcv", K(new_pcv));
  } else if (OB_FAIL(new_pcv->init(this, cache_obj, pc_ctx))) {
    SQL_PC_LOG(WARN, "failed to init plan cache value");
  } else {
    // do nothing
  }

  // add plan
  if (OB_SUCC(ret)) {
    if (OB_FAIL(new_pcv->add_plan(*cache_obj, schema_array, pc_ctx))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "failed to add plan to plan cache value", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && nullptr != new_pcv) {
    free_pcv(new_pcv);
    new_pcv = nullptr;
  }
  return ret;
}

int ObPCVSet::deep_copy_sql(const ObString& sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql.ptr())) {
    SQL_PC_LOG(DEBUG, "sql is empty, ignore copy sql", K(ret), K(lbt()));
  } else if (OB_FAIL(ob_write_string(*pc_alloc_, sql, sql_))) {
    SQL_PC_LOG(WARN, "deep copy sql into pcv_set failed", K(ret), K(sql));
  }
  return ret;
}

int64_t ObPCVSet::inc_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    if (OB_ISNULL(plan_cache_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid null plan cache", K(ret));
    } else {
      plan_cache_->get_ref_handle_mgr().record_ref_op(ref_handle);
    }
  }
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObPCVSet::dec_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    if (OB_ISNULL(plan_cache_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid null plan cache", K(ret));
    } else {
      plan_cache_->get_ref_handle_mgr().record_deref_op(ref_handle);
    }
  }
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    /*do nothing*/
  } else if (0 == ref_count) {
    LOG_DEBUG("remove pcvset", K(ref_count), K(this), K(pc_key_));
    if (OB_ISNULL(plan_cache_)) {
      LOG_ERROR("plan_cache not inited");
    } else {
      plan_cache_->dec_mem_used(get_mem_size());
      ObIAllocator& pc_alloc = plan_cache_->get_pc_allocator_ref();
      this->~ObPCVSet();
      pc_alloc.free(this);
    }
  } else {
    LOG_ERROR("invalid pcv_set ref count", K(ref_count));
  }
  return ref_count;
}

int ObPCVSet::update_stmt_stat()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&(stmt_stat_.last_active_timestamp_), ObTimeUtility::current_time());
  ATOMIC_INC(&(stmt_stat_.execute_count_));
  return ret;
}

int64_t ObPCVSet::get_mem_size()
{
  int64_t value_mem_size = 0;
  DLIST_FOREACH_NORET(pcv, pcv_list_)
  {
    if (OB_ISNULL(pcv)) {
      LOG_ERROR("invalid pcv set", K(pcv));
    } else {
      value_mem_size += pcv->get_mem_size();
    }
  }  // end for
  return value_mem_size;
}

int ObPCVSet::create_new_pcv(ObPlanCacheValue*& new_pcv)
{
  int ret = OB_SUCCESS;
  ObIAllocator* pc_alloc = get_pc_allocator();
  void* buff = nullptr;
  new_pcv = nullptr;

  if (OB_ISNULL(pc_alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc));
  } else if (nullptr == (buff = pc_alloc->alloc(sizeof(ObPlanCacheValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObPlanCacheValue", K(ret));
  } else if (nullptr == (new_pcv = new (buff) ObPlanCacheValue())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct ObPlanCacheValue", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (nullptr != new_pcv && nullptr != pc_alloc) {  // cleanup
    new_pcv->~ObPlanCacheValue();
    pc_alloc->free(new_pcv);
    new_pcv = nullptr;
  }

  return ret;
}

void ObPCVSet::free_pcv(ObPlanCacheValue* pcv)
{
  int ret = OB_SUCCESS;
  ObIAllocator* pc_alloc = get_pc_allocator();
  if (OB_ISNULL(pc_alloc) || OB_ISNULL(pcv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc), K(pcv), K(ret));
  } else {
    pcv->~ObPlanCacheValue();
    pc_alloc->free(pcv);
    pcv = nullptr;
  }
}

int ObPCVSet::set_raw_param_info_if_needed(ObCacheObject* cache_obj)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* plan = NULL;
  col_field_arr_.reset();
  col_field_arr_.set_allocator(pc_alloc_);
  ObBitSet<> visited_idx;
  PCColStruct col_item;
  if (need_check_gen_tbl_col_) {
    // do nothing
  } else if (OB_ISNULL(pc_alloc_) || OB_ISNULL(cache_obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc_), K(cache_obj));
  } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan*>(cache_obj))) {
    // do nothing
  } else if (!plan->contain_paramed_column_field()) {
    // do nothing
  } else if (OB_FAIL(col_field_arr_.reserve(plan->get_field_columns().count()))) {
    LOG_WARN("failed to init fixed array", K(ret), K(plan->get_field_columns().count()));
  } else {
    for (int64_t i = 0; i < plan->get_field_columns().count(); i++) {
      const ObField& cur_field = plan->get_field_columns().at(i);
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
          const ObField& next_field = plan->get_field_columns().at(j);
          if (!next_field.is_paramed_select_item_ || OB_ISNULL(next_field.paramed_ctx_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid argument", K(ret), K(next_field.is_paramed_select_item_), K(next_field.paramed_ctx_));
          } else if (visited_idx.has_member(j) || !next_field.paramed_ctx_->need_check_dup_name_) {
            // do nothing
          } else if (ObCharset::case_compat_mode_equal(
                         cur_field.paramed_ctx_->paramed_cname_, next_field.paramed_ctx_->paramed_cname_)) {
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
        }  // for end
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

int ObPCVSet::check_raw_param_for_dup_col(ObPlanCacheCtx& pc_ctx, bool& contain_dup_col)
{
  int ret = OB_SUCCESS;
  contain_dup_col = false;
  if (!need_check_gen_tbl_col_) {
    // do nothing
  } else {
    for (int64_t cur_idx = 0; OB_SUCC(ret) && !contain_dup_col && cur_idx < col_field_arr_.count(); cur_idx++) {
      if (col_field_arr_.at(cur_idx).is_last_) {
        // do nothing
      } else {
        PCColStruct& left_col_item = col_field_arr_.at(cur_idx);
        PCColStruct& right_col_item = col_field_arr_.at(cur_idx + 1);
        bool all_same = true;
        ObIArray<ObPCParam*>& raw_params = pc_ctx.fp_result_.raw_params_;
        for (int64_t i = 0; OB_SUCC(ret) && all_same && i < left_col_item.param_idxs_.count(); i++) {
          int64_t l_idx = left_col_item.param_idxs_.at(i);
          int64_t r_idx = right_col_item.param_idxs_.at(i);
          if (OB_ISNULL(raw_params.at(l_idx)) || OB_ISNULL(raw_params.at(r_idx)) ||
              OB_ISNULL(raw_params.at(l_idx)->node_) || OB_ISNULL(raw_params.at(r_idx)->node_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret));
          } else {
            ObString l_tmp_str(raw_params.at(l_idx)->node_->text_len_, raw_params.at(l_idx)->node_->raw_text_);
            ObString r_tmp_str(raw_params.at(r_idx)->node_->text_len_, raw_params.at(r_idx)->node_->raw_text_);

            if (0 != l_tmp_str.compare(r_tmp_str)) {
              all_same = false;
              LOG_DEBUG("raw text not matched", K(l_tmp_str), K(r_tmp_str));
            }
          }
        }  // for end
        contain_dup_col = all_same;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
