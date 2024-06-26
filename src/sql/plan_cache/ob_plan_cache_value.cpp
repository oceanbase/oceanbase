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
#include "ob_plan_cache_value.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/udr/ob_udr_mgr.h"
#include "sql/udr/ob_udr_utils.h"
#include "share/ob_duplicate_scope_define.h"
#include "pl/ob_pl_stmt.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/plan_cache/ob_values_table_compression.h"

using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::pl;

namespace oceanbase
{
namespace sql
{

int PCVSchemaObj::init(const ObTableSchema *schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema) || OB_ISNULL(inner_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema), K(inner_alloc_));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();
    is_mv_container_table_ = schema->mv_container_table();
    // copy table name
    char *buf = nullptr;
    const ObString &tname = schema->get_table_name_str();
    if (nullptr == (buf = static_cast<char *>(inner_alloc_->alloc(tname.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(tname.length()));
    } else {
      MEMCPY(buf, tname.ptr(), tname.length());
      table_name_.assign_ptr(buf, tname.length());
    }
  }
  return ret;
}

int PCVSchemaObj::init_with_synonym(const ObSimpleSynonymSchema *schema) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema) || OB_ISNULL(inner_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema), K(inner_alloc_));
  } else {
    database_id_ = schema->get_database_id();
    // copy table name
    char *buf = nullptr;
    const ObString &tname = schema->get_synonym_name_str();
    if (nullptr == (buf = static_cast<char *>(inner_alloc_->alloc(tname.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(tname.length()));
    } else {
      MEMCPY(buf, tname.ptr(), tname.length());
      table_name_.assign_ptr(buf, tname.length());
    }
  }
  return ret;
}

bool PCVSchemaObj::operator==(const PCVSchemaObj &other) const
{
  bool ret = true;
  if (schema_type_ != other.schema_type_) {
    ret = false;
  } else if (TABLE_SCHEMA == other.schema_type_) {
    ret = tenant_id_ == other.tenant_id_ &&
          database_id_ == other.database_id_ &&
          schema_id_ == other.schema_id_ &&
          schema_version_ == other.schema_version_ &&
          table_type_ == other.table_type_;
  } else {
    ret = schema_id_ == other.schema_id_ &&
          schema_version_ == other.schema_version_;
  }
  return ret;
}

int PCVSchemaObj::init_without_copy_name(const ObSimpleTableSchemaV2 *schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();

    table_name_ = schema->get_table_name_str();
  }
  return ret;
}

int PCVSchemaObj::init_with_version_obj(const ObSchemaObjVersion &schema_obj_version)
{
  int ret = OB_SUCCESS;
  schema_type_ = schema_obj_version.get_schema_type();
  schema_id_ = schema_obj_version.object_id_;
  schema_version_ = schema_obj_version.version_;
  is_explicit_db_name_ = schema_obj_version.is_db_explicit_;
  return ret;
}

void PCVSchemaObj::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  database_id_ = common::OB_INVALID_ID;
  schema_id_ = common::OB_INVALID_ID;
  schema_type_ = OB_MAX_SCHEMA;
  schema_version_ = 0;
  table_type_ = MAX_TABLE_TYPE;
  is_tmp_table_ = false;
  if (inner_alloc_ != nullptr && table_name_.ptr() != nullptr) {
    inner_alloc_->free(table_name_.ptr());
    table_name_.reset();
    inner_alloc_ = nullptr;
  }
}

PCVSchemaObj::~PCVSchemaObj()
{
  reset();
}

ObPlanCacheValue::ObPlanCacheValue()
  : pcv_set_(NULL),
    pc_alloc_(NULL),
    last_plan_id_(OB_INVALID_ID),
    //use_global_location_cache_(true),
    tenant_schema_version_(OB_INVALID_VERSION),
    sys_schema_version_(OB_INVALID_VERSION),
    outline_state_(),
    outline_params_wrapper_(),
    sessid_(OB_INVALID_ID),
    sess_create_time_(0),
    contain_sys_name_table_(false),
    need_param_(true),
    is_nested_sql_(false),
    is_batch_execute_(false),
    has_dynamic_values_table_(false),
    stored_schema_objs_(pc_alloc_),
    stmt_type_(stmt::T_MAX),
    enable_rich_vector_format_(false)
{
  MEMSET(sql_id_, 0, sizeof(sql_id_));
  not_param_index_.set_attr(ObMemAttr(MTL_ID(), "NotParamIdex"));
  neg_param_index_.set_attr(ObMemAttr(MTL_ID(), "NegParamIdex"));
  must_be_positive_idx_.set_attr(ObMemAttr(MTL_ID(), "MustBePosiIdx"));
  not_param_info_.set_attr(ObMemAttr(MTL_ID(), "NotParamInfo"));
  not_param_var_.set_attr(ObMemAttr(MTL_ID(), "NotParamVar"));
  param_charset_type_.set_attr(ObMemAttr(MTL_ID(), "ParamCharsType"));
}

int ObPlanCacheValue::assign_udr_infos(ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dynamic_param_list_.assign(pc_ctx.dynamic_param_info_list_))) {
    LOG_WARN("fail to assign dynamic param info list", K(ret));
  } else if (OB_FAIL(tpl_sql_const_cons_.assign(pc_ctx.tpl_sql_const_cons_))) {
    LOG_WARN("failed to assign tpl sql const cons", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tpl_sql_const_cons_.count(); ++i) {
      NotParamInfoList &not_param_list = tpl_sql_const_cons_.at(i);
      lib::ob_sort(not_param_list.begin(), not_param_list.end(),
              [](NotParamInfo &l, NotParamInfo &r) { return (l.idx_  < r.idx_); });
      for (int64_t j = 0; OB_SUCC(ret) && j < not_param_list.count(); ++j) {
        if (OB_FAIL(ob_write_string(*pc_alloc_,
                                   not_param_list.at(j).raw_text_,
                                   not_param_list.at(j).raw_text_))) {
          LOG_WARN("deep_copy_obj failed", K(i), K(not_param_list.at(j)));
        }
      }
    }
  }
  return ret;
}

void ObPlanCacheValue::reset_tpl_sql_const_cons()
{
  for (int64_t i = 0; i < tpl_sql_const_cons_.count(); ++i) {
    NotParamInfoList &not_param_list = tpl_sql_const_cons_.at(i);
    for (int64_t j = 0; j < not_param_list.count(); ++j) {
      pc_alloc_->free(not_param_list.at(j).raw_text_.ptr());
      not_param_list.at(j).raw_text_.reset();
    }
  }
  tpl_sql_const_cons_.reset();
}

int ObPlanCacheValue::init(ObPCVSet *pcv_set, const ObILibCacheObject *cache_obj, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  const ObPlanCacheObject *plan = static_cast<const ObPlanCacheObject*>(cache_obj);
  if (OB_ISNULL(pcv_set) || OB_ISNULL(plan) || OB_ISNULL(pcv_set->get_plan_cache())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pcv_set), K(plan));
  } else if (FALSE_IT(mem_attr = pcv_set->get_plan_cache()->get_mem_attr())) {
      // do nothing
  } else if (OB_ISNULL(pc_alloc_ = pcv_set->get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(pcv_set->get_allocator()));
  } else if (OB_ISNULL(pc_malloc_ = pcv_set->get_pc_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(pcv_set->get_pc_allocator()));
  } else if (OB_FAIL(outline_params_wrapper_.set_allocator(pc_alloc_, mem_attr))) {
    LOG_WARN("fail to set outline param wrapper allocator", K(ret));
  } else if (OB_NOT_NULL(pc_ctx.exec_ctx_.get_outline_params_wrapper())) {
    if (OB_FAIL(set_outline_params_wrapper(*pc_ctx.exec_ctx_.get_outline_params_wrapper()))) {
      LOG_WARN("fail to set outline params", K(ret));
    }
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_) ||
             OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
	     K(ret),
	     K(pc_ctx.sql_ctx_.session_info_),
	     K(pc_ctx.sql_ctx_.schema_guard_));
  }

  if (OB_SUCC(ret)) {
    pcv_set_ = pcv_set;
    //use_global_location_cache_ = !cache_obj->is_contain_virtual_table();
    outline_state_ = plan->get_outline_state();
    sys_schema_version_ = plan->get_sys_schema_version();
    tenant_schema_version_ = plan->get_tenant_schema_version();
    sql_traits_ = pc_ctx.sql_traits_;
    enable_rich_vector_format_ = static_cast<const ObPhysicalPlan *>(plan)->get_use_rich_format();
    stmt_type_ = plan->get_stmt_type();
    need_param_ = plan->need_param();
    is_nested_sql_ = ObSQLUtils::is_nested_sql(&pc_ctx.exec_ctx_);
    is_batch_execute_ = pc_ctx.sql_ctx_.is_batch_params_execute();
    has_dynamic_values_table_ = pc_ctx.exec_ctx_.has_dynamic_values_table();
    MEMCPY(sql_id_, pc_ctx.sql_ctx_.sql_id_, sizeof(pc_ctx.sql_ctx_.sql_id_));
    if (OB_FAIL(not_param_index_.add_members2(pc_ctx.not_param_index_))) {
      LOG_WARN("fail to add not param index members", K(ret));
    } else if (OB_FAIL(neg_param_index_.add_members2(pc_ctx.neg_param_index_))) {
      LOG_WARN("fail to add neg param index members", K(ret));
    } else if (OB_FAIL(param_charset_type_.assign(pc_ctx.param_charset_type_))) {
      LOG_WARN("fail to assign param charset type", K(ret));
    } else if (OB_FAIL(must_be_positive_idx_.add_members2(pc_ctx.must_be_positive_index_))) {
      LOG_WARN("failed to add bitset members", K(ret));
    } else if (OB_FAIL(set_stored_schema_objs(plan->get_dependency_table(),
                                              pc_ctx.sql_ctx_.schema_guard_))) {
      LOG_WARN("failed to set stored schema objs",
               K(ret), K(plan->get_dependency_table()), K(pc_ctx.sql_ctx_.schema_guard_));
    } else if (OB_FAIL(assign_udr_infos(pc_ctx))) {
      LOG_WARN("failed to assign user-defined rule infos", K(ret));
    } else {
      //deep copy special param raw text
      if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
        if (OB_FAIL(not_param_var_.assign(pc_ctx.not_param_var_))) {
          LOG_WARN("fail to assign not param var", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < not_param_var_.count(); ++i) {
          if (OB_FAIL(deep_copy_obj(*pc_alloc_,
                                    not_param_var_.at(i).ps_param_,
                                    not_param_var_.at(i).ps_param_))) {
            LOG_WARN("deep_copy_obj failed", K(i), K(not_param_var_.at(i)));
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < pc_ctx.not_param_info_.count(); i++) {
          if (OB_FAIL(not_param_info_.push_back(pc_ctx.not_param_info_.at(i)))) {
            LOG_WARN("fail to push not_param_info", K(ret));
          } else if (OB_FAIL(ob_write_string(*pc_alloc_,
                                             not_param_info_.at(i).raw_text_,
                                             not_param_info_.at(i).raw_text_))) {
            LOG_WARN("fail to deep copy param raw text", K(ret));
          }
        } //for end
        if (OB_SUCC(ret)) {
          lib::ob_sort(not_param_info_.begin(), not_param_info_.end(),
              [](NotParamInfo &l, NotParamInfo &r) { return (l.idx_  < r.idx_); });
        }
      }
      //deep copy constructed sql
      if (OB_SUCC(ret)) {
        ObString outline_signature_str;
        if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
          outline_signature_str = pc_ctx.raw_sql_;
        } else {
          outline_signature_str = pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_;
        }
        int64_t size = outline_signature_str.get_serialize_size();
        if (0 == size) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          char *buf = NULL;
          int64_t pos_s = 0;
          if (OB_UNLIKELY(NULL == (buf = (char *)pc_alloc_->alloc(size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mem", K(ret));
          } else if (OB_FAIL(outline_signature_str.serialize(buf, size, pos_s))) {
            LOG_WARN("fail to serialize constructed_sql_", K(ret));
          } else {
            outline_signature_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos_s));
          }
        }
      }
      // deep copy constructed_sql_, used for baseline;
      if (OB_SUCC(ret)) {
        if ((PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
             && OB_FAIL(ob_write_string(*pc_alloc_, pc_ctx.raw_sql_, constructed_sql_))) {
          LOG_WARN("fail to deep copy param raw text", K(ret));
        } else if (!(PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
                   && OB_FAIL(ob_write_string(*pc_alloc_,
                                              pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_,
                                              constructed_sql_))) {
          LOG_WARN("failed to write string", K(ret));
        }
      }
      // deep copy raw_sql if not need to param
      if (OB_SUCC(ret) && !need_param_ &&
              OB_FAIL(ob_write_string(*pc_alloc_, pc_ctx.raw_sql_, raw_sql_))) {
        LOG_WARN("failed to write raw sql", K(ret));
      }

      // set sessid_ if necessary
      if (OB_SUCC(ret)) {
        if (is_contain_tmp_tbl()) {
          //临时表的行为取决于用户创建的session，而对于远程执行而言，远程的session id是一个临时的session_id
          //因此这里统一应该使用master session id，来保证匹配计划一直使用的是用户session
          sessid_ = pc_ctx.sql_ctx_.session_info_->get_sessid_for_table();
          sess_create_time_ = pc_ctx.sql_ctx_.session_info_->get_sess_create_time();
          // 获取临时表的表名
          pc_ctx.tmp_table_names_.reset();
          if (OB_FAIL(get_tmp_depend_tbl_names(pc_ctx.tmp_table_names_))) {
            LOG_WARN("failed to get tmp depend tbl ids", K(ret));
          } else {
            // do nothing
          }
        } else { /* do nothing */ }
      }
    }

  }

  return ret;
}

int ObPlanCacheValue::match_all_params_info(ObPlanSet *batch_plan_set,
                                            ObPlanCacheCtx &pc_ctx,
                                            int64_t outline_param_idx,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  bool is_batched_multi_stmt = pc_ctx.sql_ctx_.is_batch_params_execute();
  ParamStore *params = pc_ctx.fp_result_.cache_params_;
  if (is_batched_multi_stmt) {
    ObArenaAllocator tmp_alloc;
    ParamStore param_store((ObWrapperAllocator(tmp_alloc)));
    int64_t query_cnt = pc_ctx.sql_ctx_.get_batch_params_count();
    ParamStore *ab_params = pc_ctx.ab_params_;
    ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
    if (OB_ISNULL(ab_params) || OB_ISNULL(phy_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr unexpected", K(ret), K(phy_ctx), K(ab_params));
    } else if (query_cnt <= 1) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("unexpected query count", K(ret), K(query_cnt));
    } else if (batch_plan_set->get_can_skip_params_match()) {
      // can_skip_params_match_ is true, we not need match all params,
      // only call match_params_info once
      param_store.reuse();
      phy_ctx->reset_datum_param_store();
      phy_ctx->set_original_param_cnt(ab_params->count());
      if (OB_FAIL(get_one_group_params(0, *ab_params, param_store))) {
        LOG_WARN("fail to get one params", K(ret));
      } else if (OB_FAIL(pc_ctx.fp_result_.cache_params_->assign(param_store))) {
        LOG_WARN("assign params failed", K(ret), K(param_store));
      } else if (OB_FAIL(batch_plan_set->match_params_info(params, pc_ctx, outline_param_idx, is_same))) {
        LOG_WARN("fail to match_params_info", K(ret), K(outline_param_idx), KPC(params));
      } else {
        // not match this plan, try match next plan
      }
    } else {
      // pc_ctx.fp_result_.cache_params_  在batch场景下，cache_params_不应该为null
      // When the first set of parameters cannot match the plan_set parameter requirements,
      // -5787 cannot be returned and the current batch optimization cannot be rolled back.
      for (int64_t i = 0; OB_SUCC(ret) && is_same && i < query_cnt; ++i) {
        param_store.reuse();
        phy_ctx->reset_datum_param_store();
        phy_ctx->set_original_param_cnt(ab_params->count());
        if (OB_FAIL(get_one_group_params(i, *ab_params, param_store))) {
          LOG_WARN("fail to get one params", K(ret), K(i));
        } else if (OB_FAIL(pc_ctx.fp_result_.cache_params_->assign(param_store))) {
          LOG_WARN("assign params failed", K(ret), K(param_store));
        } else if (OB_FAIL(phy_ctx->init_datum_param_store())) {
          LOG_WARN("init datum_store failed", K(ret), K(param_store));
        } else if (OB_FAIL(batch_plan_set->match_params_info(params, pc_ctx, outline_param_idx, is_same))) {
          LOG_WARN("fail to match_params_info", K(ret), K(outline_param_idx), KPC(params));
        } else if (i != 0 && !is_same) {
          ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
          LOG_TRACE("params is not same type", K(param_store), K(i));
        }
      }
    }

    if (OB_SUCC(ret) && is_same) {
      phy_ctx->reset_datum_param_store();
      phy_ctx->set_original_param_cnt(ab_params->count());
      if (OB_FAIL(batch_plan_set->copy_param_flag_from_param_info(ab_params))) {
        LOG_WARN("failed copy param flag", K(ret), KPC(ab_params));
      } else if (OB_FAIL(phy_ctx->get_param_store_for_update().assign(*ab_params))) {
        LOG_WARN("failed to assign params_store", K(ret), K(*ab_params));
      } else if (OB_FAIL(phy_ctx->init_datum_param_store())) {
        LOG_WARN("failed to init datum store", K(ret));
      }
    }
  } else {
    if (OB_FAIL(batch_plan_set->match_params_info(params, pc_ctx, outline_param_idx, is_same))) {
      LOG_WARN("fail to match_params_info", K(ret), K(outline_param_idx));
    }
  }
  return ret;
}

int ObPlanCacheValue::choose_plan(ObPlanCacheCtx &pc_ctx,
                                  const ObIArray<PCVSchemaObj> &schema_array,
                                  ObPlanCacheObject *&plan_out)
{
  int ret = OB_SUCCESS;
  bool is_old_version = false;
  plan_out = NULL;
  ObSQLSessionInfo *session = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObPlanCacheObject *plan = NULL;
  int64_t outline_param_idx = 0;
  //检查在pcv中缓存的该sql涉及的view 及 table的version，
  //如果不为最新的,在plan cache层会删除该value，并重新add plan
  //TODO shengle 此处拷贝需要想办法处理掉
  int64_t spm_mode = 0;
  bool need_check_schema = (schema_array.count() != 0);
  if (schema_array.count() == 0 && stored_schema_objs_.count() == 0) {
    need_check_schema = true;
  }
  ObBasicSessionInfo::ForceRichFormatStatus orig_rich_format_status = ObBasicSessionInfo::ForceRichFormatStatus::Disable;
  bool orig_phy_ctx_rich_format = false;
  if (stmt::T_NONE == pc_ctx.sql_ctx_.stmt_type_) {
    //sql_ctx_.stmt_type_ != stmt::T_NONE means this calling in nested sql,
    //can't cover the first stmt type in sql context
    pc_ctx.sql_ctx_.stmt_type_ = stmt_type_;
  }
  if (OB_ISNULL(session = pc_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got session is NULL", K(ret));
  } else if (FALSE_IT(orig_rich_format_status = session->get_force_rich_format_status())) {
  } else if (FALSE_IT(session->set_stmt_type(stmt_type_))) {
  } else if (OB_FAIL(session->get_spm_mode(spm_mode))) {
    LOG_WARN("fail to get spm mode", K(ret));
  } else if (spm_mode > 0) {
    if (OB_FAIL(ob_write_string(pc_ctx.allocator_,
                                constructed_sql_,
                                pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_))) {
      LOG_WARN("fail to deep copy constructed_sql_", K(ret));
    } else if (OB_FAIL(ob_write_string(pc_ctx.allocator_,
                                       common::ObString(OB_MAX_SQL_ID_LENGTH, sql_id_),
                                       pc_ctx.sql_ctx_.spm_ctx_.bl_key_.sql_id_))) {
      LOG_WARN("fail to deep copy sql_id_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_INVALID_ID == (tenant_id = session->get_effective_tenant_id())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got effective tenant id is invalid", K(ret));
  } else if (OB_FAIL(check_value_version_for_get(pc_ctx.sql_ctx_.schema_guard_,
                                                 need_check_schema,
                                                 schema_array,
                                                 tenant_id,
                                                 is_old_version))) {
    SQL_PC_LOG(WARN, "fail to check table version", K(ret));
  } else if (true == is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    SQL_PC_LOG(TRACE, "view or table is old version", K(ret));
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else {
    ParamStore *params = pc_ctx.fp_result_.cache_params_;
    //init param store
    if (OB_LIKELY(pc_ctx.sql_ctx_.is_batch_params_execute())) {
      if (OB_FAIL(resolve_multi_stmt_params(pc_ctx))) {
        if (OB_BATCHED_MULTI_STMT_ROLLBACK != ret) {
          LOG_WARN("failed to resolver row params", K(ret));
        }
      }
    } else if (OB_UNLIKELY(pc_ctx.exec_ctx_.has_dynamic_values_table())) {
      if (OB_FAIL(ObValuesTableCompression::resolve_params_for_values_clause(pc_ctx, stmt_type_,
                  not_param_info_, param_charset_type_, neg_param_index_, not_param_index_,
                  must_be_positive_idx_, params))) {
        LOG_WARN("failed to resolve_params_for_values_clause ", K(ret));
      }
    } else if (OB_FAIL(resolver_params(pc_ctx,
                                       stmt_type_,
                                       param_charset_type_,
                                       neg_param_index_,
                                       not_param_index_,
                                       must_be_positive_idx_,
                                       pc_ctx.fp_result_.raw_params_,
                                       params))) {
      LOG_WARN("fail to resolver raw params", K(ret));
    }
    // cons user-defined rule param store
    if (OB_SUCC(ret)) {
      ParamStore param_store( (ObWrapperAllocator(pc_ctx.allocator_)) );
      if (OB_FAIL(ObUDRUtils::cons_udr_param_store(dynamic_param_list_, pc_ctx, param_store))) {
        LOG_WARN("failed to construct user-defined rule param store", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
      if (NULL != phy_ctx) {
        orig_phy_ctx_rich_format = phy_ctx->is_rich_format();
        phy_ctx->set_original_param_cnt(phy_ctx->get_param_store().count());
        phy_ctx->set_rich_format(enable_rich_vector_format_);
        session->set_force_rich_format(enable_rich_vector_format_ ?
                                         ObBasicSessionInfo::ForceRichFormatStatus::FORCE_ON :
                                         ObBasicSessionInfo::ForceRichFormatStatus::FORCE_OFF);
        if (OB_FAIL(phy_ctx->init_datum_param_store())) {
          LOG_WARN("fail to init datum param store", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(get_outline_param_index(pc_ctx.exec_ctx_,
                                               outline_param_idx))) {//need use param store
      LOG_WARN("fail to judge concurrent limit sql", K(ret));
    } else {
      // 这里记录org_param_count, 用于在匹配计划结束后, 如果没有匹配成功,
      // 则将param store中参数恢复到当前状态(plan_set中match会进行可计算表
      // 达式计算并将结果加入到param store) 避免本次失败后, 上层进行重试时,
      // param store参数变多导致再匹配时, 因参数个数不符合预期报错
      int64_t org_param_count = 0;
      if (OB_NOT_NULL(params)) {
        org_param_count = params->count();
      }

      DLIST_FOREACH(plan_set, plan_sets_) {
        plan = NULL;
        bool is_same = false;
        if (OB_FAIL(match_all_params_info(plan_set, pc_ctx, outline_param_idx, is_same))) {
          SQL_PC_LOG(WARN, "fail to match params info", K(ret));
        } else if (!is_same) {        //do nothing
          LOG_TRACE("params info does not match", KPC(params));
        } else {
          if (OB_FAIL(plan_set->select_plan(pc_ctx, plan))) {
            if (OB_SQL_PC_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              SQL_PC_LOG(TRACE, "failed to select plan in plan set", K(ret));
            }
          } else if (NULL != params) {
            // set res map rule
            uint64_t rule_id = plan_set->res_map_rule_id_;
            int64_t param_idx = plan_set->res_map_rule_param_idx_;
            uint64_t tenant_id = OB_INVALID_ID;
            ObString param_text;
            ObCollationType cs_type = CS_TYPE_INVALID;
            if (rule_id != OB_INVALID_ID && param_idx != OB_INVALID_INDEX
                && pc_ctx.sql_ctx_.enable_sql_resource_manage_) {
              if (OB_UNLIKELY(param_idx < 0 || param_idx >= params->count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("unexpected res map rule param idx", K(ret), K(rule_id), K(param_idx), K(params->count()));
              } else if (OB_FAIL(session->get_collation_connection(cs_type))) {
                LOG_WARN("get collation connection failed", K(ret));
              } else if (OB_INVALID_ID == (tenant_id = session->get_effective_tenant_id())) {
                ret = OB_ERR_UNEXPECTED;
                SQL_PC_LOG(ERROR, "got effective tenant id is invalid", K(ret));
              } else if (OB_FAIL(ObObjCaster::get_obj_param_text(
                                      params->at(plan_set->res_map_rule_param_idx_),
                                      pc_ctx.raw_sql_, pc_ctx.allocator_,
                                      cs_type, param_text))) {
                LOG_WARN("get obj param text failed", K(ret));
              } else {
                uint64_t group_id = G_RES_MGR.get_col_mapping_rule_mgr().get_column_mapping_group_id(
                                      tenant_id,
                                      plan_set->res_map_rule_id_,
                                      session->get_user_name(),
                                      param_text);
                if (OB_INVALID_ID == group_id) {
                   // OB_INVALID_ID means current user+param_value is not defined in mapping rule,
                   // get group_id according to current user.
                  if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_user(
                                tenant_id, session->get_user_id(), group_id))) {
                    LOG_WARN("get group id by user failed", K(ret));
                  } else if (OB_INVALID_ID == group_id) {
                    // if not set consumer_group for current user, use OTHER_GROUP by default.
                    group_id = 0;
                  }
                }
                if (OB_SUCC(ret)) {
                  session->set_expect_group_id(group_id);
                  if (group_id == THIS_WORKER.get_group_id()) {
                    // do nothing if equals to current group id.
                  } else if (session->get_is_in_retry()
                            && OB_NEED_SWITCH_CONSUMER_GROUP
                                == session->get_retry_info().get_last_query_retry_err()) {
                    LOG_ERROR("use unexpected group when retry, maybe set packet retry failed before",
                              K(group_id), K(THIS_WORKER.get_group_id()), K(rule_id), K(param_idx));
                  } else {
                    ret = OB_NEED_SWITCH_CONSUMER_GROUP;
                  }
                  LOG_TRACE("get expect rule id", K(ret), K(group_id),
                            K(THIS_WORKER.get_group_id()), K(session->get_expect_group_id()),
                            K(pc_ctx.raw_sql_));
                }
              }
            }
            break; //这个地方建议保留，如果去掉，需要另外加标记在for()中判断，并且不使用上面的for循环的宏；
          }
        }
        if (NULL == plan
            && OB_NOT_NULL(params)
            && (params->count() > org_param_count)) {
          ObPhysicalPlanCtx * phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
          if (NULL == phy_ctx) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("physical ctx is null", K(ret));
          }
          for (int64_t i = params->count(); OB_SUCC(ret) && i > org_param_count; --i) {
            params->pop_back();
            phy_ctx->get_datum_param_store().pop_back();
            // todo: param_frame_ptrs_ and other params in physical_plan_ctx need be reset
          }
        }
      } // end for
    }
  }

  if (NULL == plan && OB_SUCC(ret)) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  if (OB_SUCC(ret)) {
    plan_out = plan;
    pc_ctx.sql_traits_ = sql_traits_; //used for check read only
  }
  // reset force rich format status
  if (NULL == plan) {
    if (session != nullptr) {
      session->set_force_rich_format(orig_rich_format_status);
    }
    if (pc_ctx.exec_ctx_.get_physical_plan_ctx() != nullptr) {
      pc_ctx.exec_ctx_.get_physical_plan_ctx()->set_rich_format(orig_phy_ctx_rich_format);
    }
  }
  return ret;
}

//将fast parser参数化的参数转化为ObObjParam, 用于get plan 时
int ObPlanCacheValue::resolver_params(ObPlanCacheCtx &pc_ctx,
                                      const stmt::StmtType stmt_type,
                                      const ObIArray<ObCharsetType> &param_charset_type,
                                      const ObBitSet<> &neg_param_index,
                                      const ObBitSet<> &not_param_index,
                                      const ObBitSet<> &must_be_positive_idx,
                                      ObIArray<ObPCParam *> &raw_params,
                                      ParamStore *obj_params)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = pc_ctx.exec_ctx_.get_my_session();
  ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  const int64_t raw_param_cnt = raw_params.count();
  ObObjParam value;
  bool enable_decimal_int = false;
  if (OB_ISNULL(session) || OB_ISNULL(phy_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), KP(session), KP(phy_ctx));
  } else if (obj_params == NULL || PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
    /* do nothing */
  } else if (OB_UNLIKELY(raw_param_cnt != param_charset_type.count())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "raw_params and param_charset_type count is different", K(ret),
               K(raw_param_cnt), K(param_charset_type.count()), K(pc_ctx.raw_sql_));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session, enable_decimal_int))) {
    LOG_WARN("fail to check enable decimal int", K(ret));
  } else {
    CHECK_COMPATIBILITY_MODE(session);
    ObCollationType collation_connection = static_cast<ObCollationType>(session->get_local_collation_connection());
    (void)obj_params->reserve(raw_param_cnt);
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_param_cnt; i++) {
      bool is_param = false;
      if (OB_FAIL(ObResolverUtils::resolver_param(pc_ctx, *session, phy_ctx->get_param_store_for_update(), stmt_type,
                  param_charset_type.at(i), neg_param_index, not_param_index, must_be_positive_idx,
                  raw_params.at(i), i, value, is_param, enable_decimal_int))) {
        SQL_PC_LOG(WARN, "failed to resolver param", K(ret), K(i));
      } else if (is_param && OB_FAIL(obj_params->push_back(value))) {
        SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
      } else {/* do nothing */}
    }
  }
  return ret;
}

int ObPlanCacheValue::before_resolve_array_params(ObPlanCacheCtx &pc_ctx, int64_t query_num, int64_t param_num, ParamStore *&ab_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ab_params = static_cast<ParamStore *>(pc_ctx.allocator_.alloc(sizeof(ParamStore))))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(ab_params = new(ab_params)ParamStore(ObWrapperAllocator(pc_ctx.allocator_)))) {
   // do nothing
  } else if (OB_FAIL(ObSQLUtils::create_multi_stmt_param_store(pc_ctx.allocator_,
                                                               query_num,
                                                               param_num,
                                                               *ab_params))) {
    LOG_WARN("failed to create multi_stmt_param_store", K(ret));
  }
  return ret;
}

int ObPlanCacheValue::resolve_multi_stmt_params(ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ParamStore *ab_params = NULL;
  bool is_valid = true;
  if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
    int64_t query_num = pc_ctx.sql_ctx_.get_insert_batch_row_cnt();
    int64_t param_num = pc_ctx.fp_result_.raw_params_.count() - not_param_info_.count();
    if (!not_param_info_.empty() &&
        OB_FAIL(check_insert_multi_values_param(pc_ctx, is_valid))) {
      LOG_WARN("failed to check multi insert param value", K(ret));
    } else if (!is_valid) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
    } else if (OB_FAIL(before_resolve_array_params(pc_ctx, query_num, param_num, ab_params))) {
      LOG_WARN("fail to prepare resolve params", K(ret));
    } else if (OB_ISNULL(ab_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (resolve_insert_multi_values_param(pc_ctx,
                                                 stmt_type_,
                                                 param_charset_type_,
                                                 neg_param_index_,
                                                 not_param_index_,
                                                 must_be_positive_idx_,
                                                 param_num,
                                                 *ab_params)) {

    } else {
      pc_ctx.ab_params_ = ab_params;
    }
  } else if (!pc_ctx.sql_ctx_.multi_stmt_item_.is_ab_batch_opt()) {
    int64_t query_num = pc_ctx.multi_stmt_fp_results_.count();
    int64_t param_num = pc_ctx.fp_result_.raw_params_.count() - not_param_info_.count();
    // check whether all the values are the same
    // 1、创建param_store指针
    if (!not_param_info_.empty() &&
        OB_FAIL(check_multi_stmt_not_param_value(pc_ctx.multi_stmt_fp_results_,
                                                 not_param_info_,
                                                 is_valid))) {
      LOG_WARN("failed to check multi stmt not param value", K(ret));
    } else if (!is_valid) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("batched multi_stmt needs rollback", K(ret));
    } else if (OB_FAIL(before_resolve_array_params(pc_ctx, query_num, param_num, ab_params))) {
      LOG_WARN("fail to prepare resolve params", K(ret));
    } else if (OB_ISNULL(ab_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(check_multi_stmt_param_type(pc_ctx,
                                                   stmt_type_,
                                                   param_charset_type_,
                                                   neg_param_index_,
                                                   not_param_index_,
                                                   must_be_positive_idx_,
                                                   *ab_params))) {
      LOG_WARN("failed to check multi stmt param type", K(ret));
    } else {
      pc_ctx.ab_params_ = ab_params;
    }
  }

  return ret;
}

int ObPlanCacheValue::resolve_insert_multi_values_param(ObPlanCacheCtx &pc_ctx,
                                                        const stmt::StmtType stmt_type,
                                                        const ObIArray<ObCharsetType> &param_charset_type,
                                                        const ObBitSet<> &neg_param_index,
                                                        const ObBitSet<> &not_param_index,
                                                        const ObBitSet<> &must_be_positive_idx,
                                                        int64_t params_num,
                                                        ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  ObRawParams *raw_param_array = nullptr;
  ParamStore temp_obj_params((ObWrapperAllocator(tmp_alloc)));
  ParamStore first_obj_params((ObWrapperAllocator(tmp_alloc)));
  int64_t query_num = pc_ctx.sql_ctx_.get_insert_batch_row_cnt();
  for (int64_t i = 0; OB_SUCC(ret) && i < query_num; i++) {
    raw_param_array = nullptr;
    temp_obj_params.reuse();
    if (OB_ISNULL(raw_param_array = pc_ctx.insert_batch_opt_info_.multi_raw_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(resolver_params(pc_ctx,
                                       stmt_type,
                                       param_charset_type,
                                       neg_param_index,
                                       not_param_index,
                                       must_be_positive_idx,
                                       *raw_param_array,
                                       &temp_obj_params))) {
      LOG_WARN("failed to resolve parames", K(ret));
    } else {
      LOG_DEBUG("print one insert temp_obj_params",
          K(temp_obj_params), K(params_num), K(query_num), K(pc_ctx.not_param_info_));
    }

    if (OB_SUCC(ret) && i == 0) {
      if (OB_FAIL(first_obj_params.assign(temp_obj_params))) {
        LOG_WARN("fail to assign params", K(ret));
      }
      // set type and external type
      for (int64_t j = 0; OB_SUCC(ret) && j < params_num; j++) {
        ObSqlArrayObj *array_params = nullptr;
        if (OB_UNLIKELY(!param_store.at(j).is_ext_sql_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param store object is invalid", K(ret), K(param_store.at(j)));
        } else if (OB_ISNULL(array_params = reinterpret_cast<ObSqlArrayObj*>(param_store.at(j).get_ext()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          array_params->element_.set_meta_type(temp_obj_params.at(j).get_meta());
          array_params->element_.set_accuracy(temp_obj_params.at(j).get_accuracy());
        }
      } // end init accuracy
    }

    // copy data
    for (int64_t j = 0; OB_SUCC(ret) && j < params_num; j++) {
      ObSqlArrayObj *array_params = nullptr;
      if (OB_UNLIKELY(!param_store.at(j).is_ext_sql_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param object is invalid", K(ret), K(param_store.at(j)));
      } else if (OB_ISNULL(array_params =
          reinterpret_cast<ObSqlArrayObj*>(param_store.at(j).get_ext()))
          || OB_ISNULL(array_params->data_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(array_params));
      } else {
        array_params->data_[i] = temp_obj_params.at(j);
      }
    } // end copy
  }
  return ret;
}

int ObPlanCacheValue::check_multi_stmt_param_type(ObPlanCacheCtx &pc_ctx,
                                                  stmt::StmtType stmt_type,
                                                  const ObIArray<ObCharsetType> &param_charset_type,
                                                  const ObBitSet<> &neg_param_index,
                                                  const ObBitSet<> &not_param_index,
                                                  const ObBitSet<> &must_be_positive_idx,
                                                  ParamStore &param_store)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc;
  ParamStore temp_obj_params((ObWrapperAllocator(tmp_alloc)));
  ParamStore first_obj_params((ObWrapperAllocator(tmp_alloc)));
  int64_t query_num = pc_ctx.multi_stmt_fp_results_.count();
  int64_t param_num = pc_ctx.fp_result_.raw_params_.count() - not_param_index.num_members();

  for (int64_t i = 0; OB_SUCC(ret) && i < query_num; i++) {
    temp_obj_params.reuse();
    if (OB_FAIL(resolver_params(pc_ctx,
                                stmt_type,
                                param_charset_type,
                                neg_param_index,
                                not_param_index,
                                must_be_positive_idx,
                                pc_ctx.multi_stmt_fp_results_.at(i).raw_params_,
                                &temp_obj_params))) {
      LOG_WARN("failed to resolve parames", K(ret));
    } else if (i == 0) {
      ret = first_obj_params.assign(temp_obj_params);
      // set type and external type
      for (int64_t j = 0; OB_SUCC(ret) && j < param_num; j++) {
        ObSqlArrayObj *array_params = nullptr;
        if (OB_UNLIKELY(!param_store.at(j).is_ext_sql_array())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param store object is invalid", K(ret), K(param_store.at(j)));
        } else if (OB_ISNULL(array_params = reinterpret_cast<ObSqlArrayObj*>(param_store.at(j).get_ext()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          array_params->element_.set_meta_type(temp_obj_params.at(j).get_meta());
          array_params->element_.set_accuracy(temp_obj_params.at(j).get_accuracy());
        }
      }
    }
    // copy data
    for (int64_t j = 0; OB_SUCC(ret) && j < param_num; j++) {
      ObSqlArrayObj *array_params = nullptr;
      if (OB_UNLIKELY(!param_store.at(j).is_ext_sql_array())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param object is invalid", K(ret), K(param_store.at(j)));
      } else if (OB_ISNULL(array_params =
          reinterpret_cast<ObSqlArrayObj*>(param_store.at(j).get_ext()))
          || OB_ISNULL(array_params->data_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KPC(array_params));
      } else {
        array_params->data_[i] = temp_obj_params.at(j);
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::check_multi_stmt_not_param_value(
                              const ObIArray<ObFastParserResult> &multi_stmt_fp_results,
                              const ObIArray<NotParamInfo> &not_param_info,
                              bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < multi_stmt_fp_results.count(); i++) {
    if (OB_FAIL(check_not_param_value(multi_stmt_fp_results.at(i),
                                      not_param_info,
                                      is_same))) {
      LOG_WARN("failed to check not param value", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPlanCacheValue::check_insert_multi_values_param(ObPlanCacheCtx &pc_ctx, bool &is_same)
{
  int ret = OB_SUCCESS;
  ObRawParams *raw_params = nullptr;
  int64_t count = pc_ctx.insert_batch_opt_info_.multi_raw_params_.count();
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < count; i++) {
    if (OB_ISNULL(raw_params = pc_ctx.insert_batch_opt_info_.multi_raw_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else if (OB_FAIL(check_not_param_value(*raw_params,
                                             pc_ctx.not_param_info_,
                                             is_same))) {
      LOG_WARN("failed to check not param value", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPlanCacheValue::check_not_param_value(const ObIArray<ObPCParam *> &raw_params,
                                            const ObIArray<NotParamInfo> &not_param_info,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  ParseNode *raw_param = NULL;
  ObPCParam *pc_param = NULL;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < not_param_info.count(); ++i) {
    if (OB_FAIL(raw_params.at(not_param_info.at(i).idx_, pc_param))) {
      LOG_WARN("fail to get raw params", K(not_param_info.at(i).idx_), K(ret));
    } else if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(pc_param));
    } else if (NULL == (raw_param = pc_param->node_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(raw_param));
    } else if (0 != not_param_info.at(i).raw_text_.compare(
        ObString(raw_param->text_len_, raw_param->raw_text_))) {
      is_same = false;
      LOG_TRACE("can't match not param info",
                "raw value", ObString(raw_param->text_len_, raw_param->raw_text_),
                "cached special value", not_param_info.at(i).raw_text_);
    } else {
      LOG_TRACE("match not param info success",
                "raw value", ObString(raw_param->text_len_, raw_param->raw_text_),
                "cached special value", not_param_info.at(i).raw_text_);
    }
  }
  return ret;
}

int ObPlanCacheValue::check_not_param_value(const ObFastParserResult &fp_result,
                                            const ObIArray<NotParamInfo> &not_param_info,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  ParseNode *raw_param = NULL;
  ObPCParam *pc_param = NULL;
  is_same = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < not_param_info.count(); ++i) {
    if (OB_FAIL(fp_result.raw_params_.at(not_param_info.at(i).idx_, pc_param))) {
      LOG_WARN("fail to get raw params", K(not_param_info.at(i).idx_), K(ret));
    } else if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(pc_param));
    } else if (NULL == (raw_param = pc_param->node_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(raw_param));
    } else if (0 != not_param_info.at(i).raw_text_.compare(
        ObString(raw_param->text_len_, raw_param->raw_text_))) {
      is_same = false;
      LOG_TRACE("match not param info",
                "raw value", ObString(raw_param->text_len_, raw_param->raw_text_),
                "cached special value", not_param_info.at(i).raw_text_);
    } else {
      LOG_TRACE("match param info",
                "raw value", ObString(raw_param->text_len_, raw_param->raw_text_),
                "cached special value", not_param_info.at(i).raw_text_);
    }
  }
  return ret;
}

int ObPlanCacheValue::cmp_not_param_info(const NotParamInfoList &l_param_info_list,
                                         const NotParamInfoList &r_param_info_list,
                                         bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (l_param_info_list.count() != r_param_info_list.count()) {
    is_equal = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_param_info_list.count(); ++i) {
      const NotParamInfo &l_param_info = l_param_info_list.at(i);
      const NotParamInfo &r_param_info = r_param_info_list.at(i);
      if (l_param_info.idx_ != r_param_info.idx_) {
        is_equal = false;
        LOG_TRACE("compare not param info", K(l_param_info), K(r_param_info));
      } else if (0 != l_param_info.raw_text_.compare(r_param_info.raw_text_)) {
        is_equal = false;
        LOG_TRACE("compare not param info", K(l_param_info), K(r_param_info));
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::check_tpl_sql_const_cons(const ObFastParserResult &fp_result,
                                               const TplSqlConstCons &tpl_cst_cons_list,
                                               bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  bool is_match_tpl_cst_cons = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_tpl_cst_cons && i < tpl_cst_cons_list.count(); ++i) {
    const NotParamInfoList &not_param_list = tpl_cst_cons_list.at(i);
    if (OB_FAIL(check_not_param_value(fp_result, not_param_list, is_match_tpl_cst_cons))) {
      LOG_WARN("failed to check not param value", K(ret));
    } else if (is_match_tpl_cst_cons
      && OB_FAIL(cmp_not_param_info(not_param_list, not_param_info_, is_same))) {
      LOG_WARN("failed to cmp not param info", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_match_tpl_cst_cons && !is_same) {
    if (OB_FAIL(check_not_param_value(fp_result, not_param_info_, is_same))) {
      LOG_WARN("failed to check not param value", K(ret));
    }
  }
  return ret;
}

int ObPlanCacheValue::get_outline_param_index(ObExecContext &exec_ctx, int64_t &param_idx) const
{
  int ret = OB_SUCCESS;
  param_idx = OB_INVALID_INDEX;
  int64_t param_count = outline_params_wrapper_.get_outline_params().count();
  int64_t concurrent_num = INT64_MAX;
  if (exec_ctx.get_physical_plan_ctx() != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      bool is_match = false;
      const ObMaxConcurrentParam *param = outline_params_wrapper_.get_outline_params().at(i);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is NULl", K(ret));
      } else if (OB_FAIL(param->match_fixed_param(exec_ctx.get_physical_plan_ctx()->get_param_store(), is_match))) {
        LOG_WARN("fail to match", K(ret), K(i));
      } else if (is_match && param->concurrent_num_ < concurrent_num) {
        concurrent_num = param->concurrent_num_;
        param_idx = i;
      } else {/*do nothing*/}
    }
  }
  return ret;
}

const ObMaxConcurrentParam *ObPlanCacheValue::get_outline_param(int64_t index) const
{
  return outline_params_wrapper_.get_outline_param(index);
}

int ObPlanCacheValue::get_one_group_params(int64_t pos, const ParamStore &src_params, ParamStore &dst_params)
{
  int ret = OB_SUCCESS;
  int64_t N = src_params.count();
  if (OB_FAIL(dst_params.reserve(N))) {
    LOG_WARN("fail to reserve paramer_store", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObObjParam &objparam = src_params.at(i);
    const ObSqlArrayObj *array_obj = NULL;
    if (OB_UNLIKELY(!objparam.is_ext_sql_array())) {
      if (OB_FAIL(dst_params.push_back(objparam))) {
        LOG_WARN("fail to push param_obj to param_store", K(i), K(pos), K(array_obj->data_[pos]), K(ret));
      }
      LOG_DEBUG("get one obj", K(ret), K(pos), K(i), K(objparam));
    } else if (OB_ISNULL(array_obj = reinterpret_cast<const ObSqlArrayObj*>(objparam.get_ext()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(objparam));
    } else if (array_obj->count_ <= pos) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameters pos", K(ret), K(i), K(pos), K(objparam));
    } else if (OB_FAIL(dst_params.push_back(array_obj->data_[pos]))) {
      LOG_WARN("fail to push param_obj to param_store", K(i), K(pos), K(array_obj->data_[pos]), K(ret));
    } else {
      LOG_TRACE("get one batch obj", K(pos), K(i), K(array_obj->data_[pos]));
    }
  }
  return ret;
}

int ObPlanCacheValue::match_and_generate_ext_params(ObPlanSet *batch_plan_set,
                                                    ObPlanCacheCtx &pc_ctx,
                                                    int64_t outline_param_idx)
{
  int ret = OB_SUCCESS;
  ParamStore *ab_params = pc_ctx.ab_params_;
  int64_t query_cnt = pc_ctx.sql_ctx_.get_batch_params_count();
  if (OB_ISNULL(batch_plan_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_set is null", K(ret));
  } else if (OB_ISNULL(ab_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ab_params is null", K(ret));
  } else {
    ObArenaAllocator tmp_alloc;
    ParamStore param_store((ObWrapperAllocator(tmp_alloc)));
    bool is_same = false;
    ObPhysicalPlanCtx *plan_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
    int64_t origin_params_cnt = plan_ctx->get_original_param_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < query_cnt; ++i) {
      is_same = false;
      param_store.reuse();
      const ParamStore &params = pc_ctx.exec_ctx_.get_physical_plan_ctx()->get_param_store();
      plan_ctx->reset_datum_param_store();
      plan_ctx->set_original_param_cnt(origin_params_cnt);
      if (OB_FAIL(get_one_group_params(i, *ab_params, param_store))) {
        LOG_WARN("fail to get one params", K(ret), K(i));
      } else if (OB_FAIL(plan_ctx->get_param_store_for_update().assign(param_store))) {
        LOG_WARN("assign params failed", K(ret), K(param_store));
      } else if (OB_FAIL(plan_ctx->init_datum_param_store())) {
        LOG_WARN("init datum_store failed", K(ret), K(param_store));
      } else if (OB_FAIL(batch_plan_set->match_params_info(&(plan_ctx->get_param_store()),
                                                           pc_ctx,
                                                           outline_param_idx,
                                                           is_same))) {
        LOG_WARN("fail to match params info", K(ret), K(i), K(param_store), K(outline_param_idx));
      } else if (!is_same) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_TRACE("params is not same type", K(param_store), K(i));
      } else {
        // do nothing
      }
    }
    // 所有的参数都match 成功了，然后折叠参数
    if (OB_SUCC(ret)) {
      plan_ctx->reset_datum_param_store();
      plan_ctx->set_original_param_cnt(origin_params_cnt);
      if (OB_FAIL(plan_ctx->get_param_store_for_update().assign(*ab_params))) {
        LOG_WARN("failed to assign params_store", K(ret), K(*ab_params));
      } else if (OB_FAIL(plan_ctx->init_datum_param_store())) {
        LOG_WARN("failed to init datum store", K(ret));
      } else {
        LOG_DEBUG("succ to init datum", K(ret));
      }
    }
  }
  return ret;
}

int ObPlanCacheValue::add_plan(ObPlanCacheObject &plan,
                               const ObIArray<PCVSchemaObj> &schema_array,
                               ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  bool need_new_planset = true;
  bool is_old_version = false;
  int64_t outline_param_idx = OB_INVALID_INDEX;
  int add_plan_ret = OB_SUCCESS;
  bool is_multi_stmt_batch = pc_ctx.sql_ctx_.is_batch_params_execute();
  //检查在pcv中缓存的该sql涉及的view 及 table的version，
  //如果不为最新的,在plan cache层会删除该value，并重新add plan
  if (OB_FAIL(check_value_version_for_add(plan,
                                          schema_array,
                                          is_old_version))) {
    SQL_PC_LOG(WARN, "fail to check table version", K(ret));
  } else if (is_old_version) {
    ret = OB_OLD_SCHEMA_VERSION;
    SQL_PC_LOG(TRACE, "view or table is old version", K(ret));
  /* Consider this concurrent scene:
     1. No mapping is defined on t.c1 at first.
     2. Thread1 resolve select * from t where c1 = 1; and generate a plan with rule_id = INVALID
     3. User define a mapping rule on t.c1.
     4. Thread2 load the new mapping rule on t.c1 into cache and evict all plans related with t
     5. Thread1 add the plan into plan cache. The plan is marked without a mapping rule
        but there is actually a mapping rule on t.c1 now
     Solution:
     1. When start to resolve a sql, record the current version of mapping rule.
     2. Before adding a plan into plan cache, check whether the recorded version is same as current version,
        and not add into plan cache if not same.
     THERE IS A FLAW of this solution. If step 4 accurs right in the gap between check version and add plan in plan cache,
     a stale plan will be added into plan cache. Since the gap is quite small, we think the flaw is acceptable.
  */
  } else if (pc_ctx.sql_ctx_.res_map_rule_version_ != 0) {
    int64_t latest_rule_version = G_RES_MGR.get_col_mapping_rule_mgr().get_column_mapping_version(MTL_ID());
    if (pc_ctx.sql_ctx_.res_map_rule_version_ != latest_rule_version) {
      ret = OB_OLD_SCHEMA_VERSION;
      pc_ctx.set_need_retry_add_plan(false);
      SQL_PC_LOG(TRACE, "resource map rule version is outdated, not add to plan cache.", K(ret),
                K(pc_ctx.sql_ctx_.res_map_rule_version_), K(latest_rule_version));
    }
  }
  if (OB_FAIL(ret)) {//do nothing
  } else if (OB_FAIL(get_outline_param_index(pc_ctx.exec_ctx_, outline_param_idx))) {
    LOG_WARN("fail to judge concurrent limit sql", K(ret));
  } else if (OB_INVALID_INDEX == outline_param_idx
             && outline_params_wrapper_.get_outline_params().count() > 0) {
    plan.get_outline_state().reset();
  }
  if (OB_SUCC(ret)) {
    DLIST_FOREACH(cur_plan_set, plan_sets_) {
      bool is_same = false;
      if (OB_FAIL(cur_plan_set->match_params_info(plan.get_params_info(),
                                                  outline_param_idx,
                                                  pc_ctx,
                                                  is_same))) {
        SQL_PC_LOG(WARN, "fail to match params info", K(ret));
      } else if (false == is_same) { //do nothing
      } else {//param info已经匹配
        SQL_PC_LOG(DEBUG, "add plan to plan set");
        need_new_planset = false;
        if (is_multi_stmt_batch &&
            OB_FAIL(match_and_generate_ext_params(cur_plan_set, pc_ctx, outline_param_idx))) {
          LOG_TRACE("fail to match and generate ext_params", K(ret));
        } else if (OB_FAIL(cur_plan_set->add_cache_obj(plan, pc_ctx, outline_param_idx, add_plan_ret))) {
          SQL_PC_LOG(TRACE, "failed to add plan", K(ret));
        }
        break;
      }
    } // end for

    /**
     *  Add the plan to a new allocated plan set
     *  and then add the plan set to plan cache value
     */
    if (OB_SUCC(ret) && need_new_planset) {
      SQL_PC_LOG(DEBUG, "add new plan set");
      ObPlanSet *plan_set = nullptr;
      if (OB_FAIL(create_new_plan_set(ObPlanSet::get_plan_set_type_by_cache_obj_type(plan.get_ns()),
                                      plan_set))) {
        SQL_PC_LOG(WARN, "failed to create new plan set", K(ret));
      } else {
        plan_set->set_plan_cache_value(this);
        if (OB_FAIL(plan_set->init_new_set(pc_ctx,
                                           plan,
                                           outline_param_idx,
                                           get_pc_malloc()))) {
          LOG_WARN("init new plan set failed", K(ret));
        } else if (is_multi_stmt_batch &&
            OB_FAIL(match_and_generate_ext_params(plan_set, pc_ctx, outline_param_idx))) {
          LOG_TRACE("fail to match and generate ext_params", K(ret));
        } else if (OB_FAIL(plan_set->add_cache_obj(plan, pc_ctx, outline_param_idx, add_plan_ret))) {
          SQL_PC_LOG(TRACE, "failed to add plan to plan set", K(ret));
        } else if (!plan_sets_.add_last(plan_set)) {
          ret = OB_ERROR;
          SQL_PC_LOG(WARN, "failed to add plan set to plan cache value", K(ret));
        } else {
          ret = OB_SUCCESS;
          SQL_PC_LOG(DEBUG, "plan set added", K(ret));
        }

        // free the memory if not used
        if (OB_FAIL(ret) && NULL != plan_set) {
          free_plan_set(plan_set);
          plan_set = NULL;
        }
      }
    }
  }
  return ret;
}

//删除对应的该plan cache value中某一个plan
/*void ObPlanCacheValue::remove_plan(ObExecContext &exec_context, ObPhysicalPlan &plan)*/
//{
  //int ret = OB_SUCCESS;
  //int64_t outline_param_idx = OB_INVALID_INDEX;
  //if (OB_FAIL(get_outline_param_index(exec_context, outline_param_idx))) {
    //LOG_WARN("fail to judge concurrent limit sql", K(ret));
  //} else {
    //DLIST_FOREACH(cur_plan_set, plan_sets_) {
      //bool is_same = false;
      //if (OB_FAIL(cur_plan_set->match_params_info(plan.get_params_info(),
                                                  //outline_param_idx,
                                                  //is_same))) {
        //BACKTRACE(ERROR, true, "fail to match param info");
      //} else if (true == is_same) {
        //cur_plan_set->remove_plan(plan);
      //}
    //} // end for
  //}
/*}*/

void ObPlanCacheValue::reset()
{
  // TODO TBD: do nothing to rw_lock_
  ObDLinkBase<ObPlanCacheValue>::reset();
  last_plan_id_ = OB_INVALID_ID;
  while (!plan_sets_.is_empty()) {
    ObPlanSet *plan_set = plan_sets_.get_first();
    if (OB_ISNULL(plan_set)) {
      //do nothing;
    } else {
      plan_sets_.remove(plan_set);
      plan_set->remove_all_plan();
      free_plan_set(plan_set);
      plan_set = nullptr;
    }
  }
  plan_sets_.clear();
  // free plan_cache_key
  if (NULL == pc_alloc_) {
    SQL_PC_LOG(TRACE, "pc alloc not init, may be reset before", K(pc_alloc_));
  } else {
    for (int64_t i = 0; i < not_param_info_.count(); i++) {
      if (NULL != not_param_info_.at(i).raw_text_.ptr()) {
        pc_alloc_->free(not_param_info_.at(i).raw_text_.ptr());
        not_param_info_.at(i).raw_text_.reset();
      }
    }

    for (int64_t i = 0; i < not_param_var_.count(); i++) {
      // deep-copy destory
      // free v
      ObObjParam param = not_param_var_.at(i).ps_param_;
      void * ptr = param.get_deep_copy_obj_ptr();
      if (NULL == ptr){
        // do nothing
      } else {
        pc_alloc_->free(ptr);
        not_param_var_.at(i).ps_param_.reset();
      }
    }

    if (NULL != outline_signature_.ptr()) {
      pc_alloc_->free(outline_signature_.ptr());
      outline_signature_.reset();
    }
    if (NULL != constructed_sql_.ptr()) {
      pc_alloc_->free(constructed_sql_.ptr());
      constructed_sql_.reset();
    }
  }
  not_param_info_.reset();
  not_param_index_.reset();
  not_param_var_.reset();
  neg_param_index_.reset();
  param_charset_type_.reset();
  sql_traits_.reset();
  reset_tpl_sql_const_cons();

  if (OB_SUCCESS != outline_params_wrapper_.destroy()) {
    LOG_ERROR_RET(OB_ERROR, "fail to destroy ObOutlineParamWrapper");
  }
  outline_params_wrapper_.reset_allocator();
  //use_global_location_cache_ = true;
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  outline_state_.reset();
  sessid_ = OB_INVALID_ID;
  sess_create_time_ = 0;
  contain_sys_name_table_ = false;
  is_nested_sql_ = false;
  is_batch_execute_ = false;
  has_dynamic_values_table_ = false;
  for (int64_t i = 0; i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i)) || OB_ISNULL(pc_alloc_)) {
      // do nothing
    } else {
      stored_schema_objs_.at(i)->reset();
      pc_alloc_->free(stored_schema_objs_.at(i));
    }
  }
  stored_schema_objs_.reset();
  enable_rich_vector_format_ = false;
  pcv_set_ = NULL; //放最后，前面可能存在需要pcv_set
}

//获取该plan cache value下所有plan占用内存
int64_t ObPlanCacheValue::get_mem_size()
{
  int64_t value_mem_size = 0;
  DLIST_FOREACH_NORET(plan_set, plan_sets_) {
    if (OB_ISNULL(plan_set)) {
      BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "invalid plan_set");
    } else {
      value_mem_size += plan_set->get_mem_size();
    }
  } // end for
  return value_mem_size;
}

int ObPlanCacheValue::check_value_version_for_add(const ObPlanCacheObject &cache_obj,
                                                  const ObIArray<PCVSchemaObj> &schema_array,
                                                  bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (OB_FAIL(check_value_version(true/*need_check_schema*/,
                                  cache_obj.get_outline_state().outline_version_,
                                  schema_array,
                                  result))) {
    LOG_WARN("failed to check pcv version", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObPlanCacheValue::check_value_version_for_get(share::schema::ObSchemaGetterGuard *schema_guard,
                                                  bool need_check_schema,
                                                  const ObIArray<PCVSchemaObj> &schema_array,
                                                  const uint64_t tenant_id,
                                                  bool &result)
{
  int ret = OB_SUCCESS;
  ObSchemaObjVersion local_outline_version;
  result = false;
  if (OB_ISNULL(schema_guard)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_guard));
  } else if (need_check_schema
             &&OB_FAIL(get_outline_version(*schema_guard,
                                           tenant_id,
                                           local_outline_version))) {
    LOG_WARN("failed to get local outline version", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_value_version(need_check_schema,
                                         local_outline_version,
                                         schema_array,
                                         result))) {
    LOG_WARN("failed to check value version", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

//检查plan是否过期逻辑：
//1.如果:schema变化：
//    a.判断outline version是否过期
//    b.判断table version是否过期
//    c.判断view version是否过期
//  否则:直接考虑第2步
//2.如果:plan是由outline生成的plan, 则不需要关心merge_version是否变化
//  否则:判断merge_version是否过期。
//3. 表的直方图统计信息是否已经过期
int ObPlanCacheValue::check_value_version(bool need_check_schema,
                                          const ObSchemaObjVersion &outline_version,
                                          const ObIArray<PCVSchemaObj> &schema_array,
                                          bool &is_old_version)
{
  int ret = OB_SUCCESS;
  is_old_version = false;
  if (need_check_schema) {
    if (outline_version != outline_state_.outline_version_) {
      is_old_version = true;
    } else if (OB_FAIL(check_dep_schema_version(schema_array,
                                                is_old_version))) {
      LOG_WARN("failed to check schema obj versions", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

//检查table schema version是否过期, get plan时使用
int ObPlanCacheValue::check_dep_schema_version(const ObIArray<PCVSchemaObj> &schema_array,
                                               bool &is_old_version)
{
  int ret = OB_SUCCESS;
  is_old_version = false;
  int64_t table_count = schema_array.count();
  ObSEArray<PCVSchemaObj*, 4> check_stored_schema;

  if (OB_FAIL(remove_mv_schema(schema_array, check_stored_schema))) {
    LOG_WARN("failed to remove mv schema", K(ret));
  } else if (schema_array.count() != check_stored_schema.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table count do not match", K(ret), K(schema_array.count()), K(check_stored_schema.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_old_version && i < table_count; ++i) {
      const PCVSchemaObj *schema_obj1 = check_stored_schema.at(i);
      const PCVSchemaObj &schema_obj2 = schema_array.at(i);
      if (nullptr == schema_obj1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null table schema", K(ret), K(schema_obj1));
      } else if (*schema_obj1 == schema_obj2) { // schema do match
        LOG_DEBUG("matched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
        // do nothing
      } else {
        LOG_TRACE("mismatched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
        is_old_version = true;
      }
    }
  }

  return ret;
}


//检查outline version是否过期
int ObPlanCacheValue::get_outline_version(ObSchemaGetterGuard &schema_guard,
                                          const uint64_t tenant_id,
                                          ObSchemaObjVersion &local_outline_version)
{
  int ret = OB_SUCCESS;
  const ObOutlineInfo *outline_info = NULL;
  local_outline_version.reset();
  uint64_t database_id = OB_INVALID_ID;
  if (OB_ISNULL(pcv_set_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pcv_set_ is null");
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(database_id));
  } else if (OB_INVALID_ID == (database_id = pcv_set_->get_plan_cache_key().db_id_)
             || ObLibCacheNameSpace::NS_PRCR == pcv_set_->get_plan_cache_key().namespace_
             || ObLibCacheNameSpace::NS_SFC == pcv_set_->get_plan_cache_key().namespace_
             || ObLibCacheNameSpace::NS_PKG == pcv_set_->get_plan_cache_key().namespace_
             || ObLibCacheNameSpace::NS_ANON == pcv_set_->get_plan_cache_key().namespace_) {
    //do nothing
  } else {
    const ObString &signature = outline_signature_;
    if (OB_FAIL(schema_guard.get_outline_info_with_signature(tenant_id,
            database_id,
            signature,
            outline_info))) {
      LOG_WARN("failed to get_outline_info", K(tenant_id), K(database_id), K(signature));
    } else if (NULL == outline_info) {
      if (OB_FAIL(schema_guard.get_outline_info_with_sql_id(tenant_id,
              database_id,
              ObString::make_string(sql_id_),
              outline_info))) {
        LOG_WARN("failed to get_outline_info", K(tenant_id), K(database_id), K(signature));
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL == outline_info) {
        // do nothing
      } else {
        local_outline_version.object_id_ = outline_info->get_outline_id();
        local_outline_version.version_ = outline_info->get_schema_version();
        local_outline_version.object_type_ = DEPENDENCY_OUTLINE;
      }
    }
  }
  return ret;
}

// 1. 通过匹配不能参数化的常量
// 2. 检查是否包含临时表以及同名表
int ObPlanCacheValue::match(ObPlanCacheCtx &pc_ctx,
                            const ObIArray<PCVSchemaObj> &schema_array,
                            bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  //compare not param
  ObSQLSessionInfo *session_info = pc_ctx.sql_ctx_.session_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session_info));
  }
  if (OB_FAIL(ret)) {
  } else if (is_nested_sql_ != ObSQLUtils::is_nested_sql(&pc_ctx.exec_ctx_)) {
    //the plan of nested sql can't match with the plan of general sql
    //because nested sql's plan be forced to use DAS plan
    //but the general sql's plan has no this constraint
    is_same = false;
  } else if (is_batch_execute_ != pc_ctx.sql_ctx_.is_batch_params_execute()||
             has_dynamic_values_table_ != pc_ctx.exec_ctx_.has_dynamic_values_table()) {
    // the plan of batch execute sql can't match with the plan of general sql
    is_same = false;
  } else if (!need_param_) {
    // not needs to param, compare raw_sql
    is_same = (pc_ctx.raw_sql_==raw_sql_);
  } else if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
    const ObObjParam *ps_param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < not_param_var_.count(); ++i) {
      ps_param = NULL;
      if (OB_FAIL(pc_ctx.fp_result_.parameterized_params_.at(not_param_var_[i].idx_, ps_param))) {
        LOG_WARN("fail to get ps param", K(not_param_info_[i].idx_), K(ret));
      } else if (OB_ISNULL(ps_param)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ps_param));
      } else if (ps_param->is_pl_extend() || not_param_var_[i].ps_param_.is_pl_extend()
                  || !not_param_var_[i].ps_param_.can_compare(*ps_param)) {
        is_same = false;
        LOG_WARN("can not compare", K(not_param_var_[i].ps_param_), K(*ps_param), K(i));
      } else if (not_param_var_[i].ps_param_.is_string_type()
          && not_param_var_[i].ps_param_.get_collation_type() != ps_param->get_collation_type()) {
        is_same = false;
        LOG_WARN("can not compare", K(not_param_var_[i].ps_param_), K(*ps_param), K(i));
      } else if (0 != not_param_var_[i].ps_param_.compare(*ps_param)) {
        is_same = false;
        LOG_WARN("match not param var", K(not_param_var_[i]), K(ps_param), K(i));
      }
      LOG_DEBUG("match", K(not_param_var_[i].idx_), K(not_param_var_[i].ps_param_), KPC(ps_param));
    }
  } else {
     if (OB_FAIL(check_tpl_sql_const_cons(pc_ctx.fp_result_,
                                          tpl_sql_const_cons_,
                                          is_same))) {
      LOG_WARN("failed to check tpl sql const cons", K(ret));
    }
  }

  // check for temp tables and same table
  if (OB_SUCC(ret) && is_same && schema_array.count() != 0) {
    if (OB_FAIL(match_dep_schema(pc_ctx, schema_array, is_same))) {
      LOG_WARN("failed to check dep table schema", K(ret));
    }
  }
  return ret;
}

bool ObPlanCacheValue::is_contain_tmp_tbl() const
{
  bool is_contain = false;

  for (int64_t i = 0; !is_contain && i < stored_schema_objs_.count(); i++) {
    if (nullptr != stored_schema_objs_.at(i)
        && stored_schema_objs_.at(i)->is_tmp_table_) {
      is_contain = true;
    }
  }

  return is_contain;
}

bool ObPlanCacheValue::is_contain_synonym() const
{
  bool is_contain = false;

  for (int64_t i = 0; !is_contain && i < stored_schema_objs_.count(); i++) {
    if (nullptr != stored_schema_objs_.at(i)
        && (SYNONYM_SCHEMA == stored_schema_objs_.at(i)->schema_type_)) {
      is_contain = true;
    }
  }

  return is_contain;
}

/*!
 * 系统包/类型的更新仅会推高系统租户的schema_version, 在普通租户下的对象如果依赖了系统包/类型,
 * 在更新的场景下因为未推高普通租户的schema_version, 可能会漏check系统包/类型是否过期,
 * 导致更新后依赖系统包/类型的routine对象不可用,因此对于包含系统包/类型的对象总是做schema check
 */
bool ObPlanCacheValue::is_contain_sys_pl_object() const
{
  bool is_contain = false;

  for (int64_t i = 0; !is_contain && i < stored_schema_objs_.count(); i++) {
    if (nullptr != stored_schema_objs_.at(i)
        && (PACKAGE_SCHEMA == stored_schema_objs_.at(i)->schema_type_
            || UDT_SCHEMA == stored_schema_objs_.at(i)->schema_type_)
        && OB_SYS_TENANT_ID == get_tenant_id_by_object_id(stored_schema_objs_.at(i)->schema_id_)) {
      is_contain = true;
    }
  }

  return is_contain;
}

int ObPlanCacheValue::get_tmp_depend_tbl_names(TmpTableNameArray &tmp_tbl_names) {
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("got unexpected null", K(ret), K(i));
    } else if (stored_schema_objs_.at(i)->is_tmp_table_ &&
               OB_FAIL(tmp_tbl_names.push_back(stored_schema_objs_.at(i)->table_name_))) {
      LOG_WARN("failed to push back tmp table name ptr", K(ret), K(i));
    }
  }
  return ret;
}

int ObPlanCacheValue::create_new_plan_set(const ObPlanSetType plan_set_type,
                                          ObPlanSet *&new_plan_set)
{
  int ret = OB_SUCCESS;
  ObIAllocator *pc_alloc = get_pc_alloc();
  void *buff = nullptr;
  new_plan_set = nullptr;

  int64_t mem_sz =  sizeof(ObSqlPlanSet);

  if (OB_ISNULL(pc_alloc) ||
      (plan_set_type < PST_SQL_CRSR || plan_set_type >= PST_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc), K(plan_set_type));
  } else if (nullptr == (buff = pc_alloc->alloc(mem_sz))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObPlanSet", K(ret));
  } else {
    new_plan_set = new(buff)ObSqlPlanSet();
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (nullptr != new_plan_set && nullptr != pc_alloc) { // cleanup
    new_plan_set->reset();
    new_plan_set->~ObPlanSet();
    pc_alloc->free(new_plan_set);
    new_plan_set = nullptr;
  }
  return ret;
}

void ObPlanCacheValue::free_plan_set(ObPlanSet *plan_set)
{
  int ret = OB_SUCCESS;
  ObIAllocator *pc_alloc = get_pc_alloc();
  if (OB_ISNULL(pc_alloc) || OB_ISNULL(plan_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc_alloc), K(plan_set), K(ret));
  } else {
    plan_set->reset();
    plan_set->~ObPlanSet();
    pc_alloc->free(plan_set);
    plan_set = nullptr;
  }
}

int ObPlanCacheValue::set_stored_schema_objs(const DependenyTableStore &dep_table_store,
                                             share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  PCVSchemaObj *pcv_schema_obj = nullptr;
  void *obj_buf = nullptr;

  stored_schema_objs_.reset();
  stored_schema_objs_.set_allocator(pc_alloc_);

  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null argument", K(ret), K(schema_guard));
  } else if (OB_FAIL(stored_schema_objs_.init(dep_table_store.count()))) {
    LOG_WARN("failed to init stored_schema_objs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_table_store.count(); i++) {
      const ObSchemaObjVersion &table_version = dep_table_store.at(i);
      table_schema = nullptr;
      int hash_err = OB_SUCCESS;
      if (table_version.get_schema_type() != TABLE_SCHEMA) {
        // 如果不是table schema，直接存schema id和version即可，synonym 多存个 db_id
        const ObSimpleSynonymSchema *synonym_schema = nullptr;
        bool is_synonym = (SYNONYM_SCHEMA == table_version.get_schema_type());
        if (is_synonym && OB_SUCC(ret)) {
          if (OB_FAIL(schema_guard->get_simple_synonym_info(
              MTL_ID(), table_version.get_object_id(), synonym_schema))) {
            LOG_WARN("failed to get table schema", K(ret), K(table_version), K(synonym_schema));
          } else if (nullptr == synonym_schema) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get an unexpected null schema", K(ret), K(synonym_schema));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVSchemaObj)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (FALSE_IT(pcv_schema_obj = new(obj_buf)PCVSchemaObj(pc_alloc_))) {
          // do nothing
        } else if (OB_FAIL(pcv_schema_obj->init_with_version_obj(table_version))) {
          LOG_WARN("failed to init pcv schema obj", K(ret), K(table_version));
        } else if (is_synonym && OB_FAIL(pcv_schema_obj->init_with_synonym(synonym_schema))) {
          LOG_WARN("failed to init table name", K(ret));
        } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
          LOG_WARN("failed to push back array", K(ret));
        } else {
          obj_buf = nullptr;
          pcv_schema_obj = nullptr;
        }
      } else if (OB_FAIL(schema_guard->get_table_schema(
                  MTL_ID(),
                  table_version.get_object_id(),
                  table_schema))) { // now deal with table schema
        LOG_WARN("failed to get table schema", K(ret), K(table_version), K(table_schema));
      } else if (nullptr == table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get an unexpected null schema", K(ret), K(table_schema));
      } else if (table_schema->is_index_table()) {
        // plan cache不需要考虑索引表，直接略过比较
        // do nothing
      } else if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVSchemaObj)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (FALSE_IT(pcv_schema_obj = new(obj_buf)PCVSchemaObj(pc_alloc_))) {
        // do nothing
      } else if (OB_FAIL(pcv_schema_obj->init(table_schema))) {
        LOG_WARN("failed to init pcv schema obj with table schema", K(ret));
      } else if (FALSE_IT(pcv_schema_obj->is_explicit_db_name_ = table_version.is_db_explicit_)) {
        // do nothing
      } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if(!contain_sys_name_table_) {
        // oracle 模式下普通表可以和sys下的表同名，计划匹配时需要区分以匹配不同的计划
        // sys下的表包含系统表和视图，所以调用is_sys_table_name来检查表是否在sys下
        //
        // 此外，sql如果包含内部表，内部表的schema version变化不会体现在普通租户的tenant schema version上
        // 为了能够及时更新计划，需要查对应的内部表的schema 版本号，oracle租户的内部表在sys下，mysql租户
        // 在oceanbase下。
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(MTL_ID(),
                                                                          OB_ORA_SYS_DATABASE_ID,
                                                                          table_schema->get_table_name(),
                                                                          contain_sys_name_table_))) {
            LOG_WARN("failed to check sys table", K(ret));
          }
        } else if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(MTL_ID(),
                                                                               OB_SYS_DATABASE_ID,
                                                                               table_schema->get_table_name(),
                                                                               contain_sys_name_table_))) {
          LOG_WARN("failed to check sys table", K(ret));
        } else {
          // do nothing
        }
        LOG_TRACE("check sys table", K(table_schema->get_table_name()), K(contain_sys_name_table_));
      } else {
        // do nothing
      }
      obj_buf = nullptr;
      pcv_schema_obj = nullptr;
      table_schema = nullptr;
    } // for end
  }
  if (OB_FAIL(ret)) {
    stored_schema_objs_.reset();
  } else {
    // do nothing
  }
  return ret;
}

/* used to get plan

   对于查询的是Table Schema的情况，需要传入database id，不同的模式有不同的策略
   MySQL模式：
     get plan时直接从缓存PCVSchemaObj中取database id

   Oracle模式：
     Oracle模式如果在SQL中没有指定schema，使用session上的db id，比如select xx from test;
     否则，需要直接使用PCVSchemaObj的table id查schema，比如select xx from user.test;

     这样做的原因是为了解决以下场景：user1和user2都有一张名为test的表，
     用user1连接，执行select * from user2.test，
     如果直接使用user1连接的session上的db id，由于用的table_name查schema，
     那么会查到user1下的schema，并用于match逻辑，一定会匹配失败。这种场景下，指定了db吗，db id是固定的，直接用table_id
     查schema即可以查到user2下的test表的schema

   Oracle系统表和普通表解决方式：
     用当前session上的database id查table_name，如果存在则用这个table schema作为匹配条件；否则再去SYS DB下查schema
 */
int ObPlanCacheValue::get_all_dep_schema(ObPlanCacheCtx &pc_ctx,
                                         const uint64_t database_id,
                                         int64_t &new_schema_version,
                                         bool &need_check_schema,
                                         ObIArray<PCVSchemaObj> &schema_array)
{
  int ret = OB_SUCCESS;
  need_check_schema = false;
  if (OB_FAIL(need_check_schema_version(pc_ctx,
                                        new_schema_version,
                                        need_check_schema))) {
    LOG_WARN("failed to get need_check_schema flag", K(ret));
  } else if (!need_check_schema) {
    // do nothing
  } else if (OB_ISNULL(pc_ctx.sql_ctx_.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pc_ctx.sql_ctx_.schema_guard_));
  } else {
    schema_array.reset();
    const ObSimpleTableSchemaV2 *table_schema = nullptr;
    PCVSchemaObj tmp_schema_obj;
    uint64_t tenant_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); i++) {
      tenant_id = MTL_ID();
      ObSchemaGetterGuard &schema_guard = *pc_ctx.sql_ctx_.schema_guard_;
      PCVSchemaObj *pcv_schema = stored_schema_objs_.at(i);
      if (OB_ISNULL(pcv_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("got unexpected null", K(ret));
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) {
        // if no table schema, get schema version is enough
        int64_t new_version = 0;
        if (PACKAGE_SCHEMA == stored_schema_objs_.at(i)->schema_type_
            || UDT_SCHEMA == stored_schema_objs_.at(i)->schema_type_) {
          tenant_id = get_tenant_id_by_object_id(stored_schema_objs_.at(i)->schema_id_);
        } else if (SYNONYM_SCHEMA == pcv_schema->schema_type_) {
          const ObSimpleSynonymSchema *synonym_schema = nullptr;
          if (pcv_schema->is_explicit_db_name_) {
            if (OB_FAIL(schema_guard.get_simple_synonym_info(tenant_id, pcv_schema->schema_id_,
                                                             synonym_schema))) {
              LOG_WARN("failed to get private synonym", K(ret));
            }
          } else {
            if (OB_FAIL(schema_guard.get_synonym_info(tenant_id, database_id,
                                                      pcv_schema->table_name_, synonym_schema))) {
              LOG_WARN("failed to get private synonym", K(ret));
            } else if (OB_ISNULL(synonym_schema)
                       && OB_FAIL(schema_guard.get_synonym_info(tenant_id, OB_PUBLIC_SCHEMA_ID,
                                                                pcv_schema->table_name_,
                                                                synonym_schema))) {
              LOG_WARN("failed to get public synonym", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_NOT_NULL(synonym_schema)) {
            tmp_schema_obj.database_id_ = synonym_schema->get_database_id();
            tmp_schema_obj.schema_version_ = synonym_schema->get_schema_version();
            tmp_schema_obj.schema_id_ = synonym_schema->get_synonym_id();
            tmp_schema_obj.schema_type_ = pcv_schema->schema_type_;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(schema_guard.get_schema_version(pcv_schema->schema_type_,
                                                    tenant_id,
                                                    pcv_schema->schema_id_,
                                                    new_version))) {
          LOG_WARN("failed to get schema version",
                   K(ret), K(tenant_id), K(pcv_schema->schema_type_), K(pcv_schema->schema_id_));
        } else {
          if (SYNONYM_SCHEMA != pcv_schema->schema_type_) {
            tmp_schema_obj.schema_id_ = pcv_schema->schema_id_;
            tmp_schema_obj.schema_version_ = new_version;
            tmp_schema_obj.schema_type_ = pcv_schema->schema_type_;
          }
          if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
            LOG_WARN("failed to push back array", K(ret));
          } else {
            tmp_schema_obj.reset();
          }
        }
      } else if (lib::is_oracle_mode()) {
       // In oracle mode, entity tables and temporary tables do not have
       // the same name. Therefore, there is no need to check the temporary
       // table here, just get it directly.
        if (pcv_schema->is_explicit_db_name_) {
          // oracle模式下，如果指定了database的name，直接用table id查schema
          if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                      pcv_schema->schema_id_, table_schema))) {
            LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
          } else { /* do nothing */ }
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, // 否则用session上db id和table name查schema
                                                                database_id,
                                                                pcv_schema->table_name_,
                                                                false,
                                                                table_schema))) {
          LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
        } else if (nullptr == table_schema && OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                                                           common::OB_ORA_SYS_DATABASE_ID,
                                                                                           pcv_schema->table_name_,
                                                                                           false,
                                                                                           table_schema))) { // 如果查不到再查sys库
          LOG_WARN("failed to get table schema", K(ret), K(tenant_id),
                   K(pcv_schema->table_name_));
        } else {
          // do nothing
        }
      // In mysql mode, there is already logic in the interface
      // to directly obtain temporary tables
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                              pcv_schema->database_id_,
                                                              pcv_schema->table_name_,
                                                              false,
                                                              table_schema))) { // mysql 模式下直接用pcv schema缓存的db id查
        LOG_WARN("failed to get table schema",
                 K(ret), K(pcv_schema->tenant_id_), K(pcv_schema->database_id_),
                 K(pcv_schema->table_name_));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) { // not table schema
        tmp_schema_obj.reset();
      } else if (nullptr == table_schema) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_TRACE("table not exist", K(ret), K(*pcv_schema), K(table_schema));
      } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        table_schema = nullptr;
        tmp_schema_obj.reset();
      }
    } // for end
  }
  return ret;
}

// 针对
// 物化视图改写会使得同一条 SQL 的不同计划依赖的表不同，导致 plan cache 进入不同的 pcv set
// 在检查 pcv set 的时候跳过物化视图相关的表，使得改写与不改写的 SQL 进入同一个 pcv set
int ObPlanCacheValue::remove_mv_schema(const common::ObIArray<PCVSchemaObj> &schema_array,
                                       common::ObIArray<PCVSchemaObj*> &check_stored_schema)
{
  int ret = OB_SUCCESS;
  bool need_remove = true;
  int64_t j = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); ++i) {
    if (OB_ISNULL(stored_schema_objs_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null table schema", K(ret), K(i), K(stored_schema_objs_.at(i)));
    } else if (MATERIALIZED_VIEW == stored_schema_objs_.at(i)->table_type_
        || MATERIALIZED_VIEW_LOG == stored_schema_objs_.at(i)->table_type_
        || stored_schema_objs_.at(i)->is_mv_container_table_) {
      if (j < schema_array.count()
          && stored_schema_objs_.at(i)->schema_id_ == schema_array.at(j).schema_id_) {
        if (OB_FAIL(check_stored_schema.push_back(stored_schema_objs_.at(i)))) {
          LOG_WARN("failed to push back schema", K(ret));
        } else {
          ++j;
        }
      } else {
        // do nothing, 仅存在于 pcv set 中的物化视图是改写出来的，抛弃
      }
    } else if (OB_FAIL(check_stored_schema.push_back(stored_schema_objs_.at(i)))) {
      LOG_WARN("failed to push back schema", K(ret));
    } else {
      ++j;
    }
  }
  return ret;
}

// 对于计划所依赖的schema进行比较，注意这里不比较table schema的version信息
// table schema的version信息用于淘汰pcv set，在check_value_version时进行比较
int ObPlanCacheValue::match_dep_schema(const ObPlanCacheCtx &pc_ctx,
                                       const ObIArray<PCVSchemaObj> &schema_array,
                                       bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObSQLSessionInfo *session_info = pc_ctx.sql_ctx_.session_info_;
  common::ObSEArray<PCVSchemaObj*, 4> check_stored_schema;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session_info));
  } else if (OB_FAIL(remove_mv_schema(schema_array, check_stored_schema))) {
    LOG_WARN("failed to remove mv schema", K(ret));
  } else if (schema_array.count() != check_stored_schema.count()) {
    // schema objs count不匹配，可能是以下情况:
    // select * from all_sequences;  // 系统视图，系统视图的dependency_table有多个
    // select * from all_sequences;  // 普通表
    is_same = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < schema_array.count(); i++) {
      if (OB_ISNULL(check_stored_schema.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null table schema",
                 K(ret), K(i), K(schema_array.at(i)), K(check_stored_schema.at(i)));
      } else if (TMP_TABLE == schema_array.at(i).table_type_
                 && schema_array.at(i).is_tmp_table_) { // check for mysql tmp table
        // 如果包含临时表
        // 临时表也是同名表的一种特殊情况，但是这里用sessid_来区分了该pcv是否包含临时计划，
        // sessid_不为0，则是包含临时表的pocv，否则是普通表的pcv
        // oracle 模式下，临时表其实是一张普通表，server内部通过改写添加sessid字段以区分不同session上的临时表
        // sessid可能存在重用的情况，所以不同的临时表需要匹配sessid以及sess_create_time_字段
        // mysql模式下，临时表只在对应的session创建，sessid不同即可区分，sess_create_time一定相同

        //plan cache匹配临时表应该始终使用用户创建的session才能保证语义的正确性
        //远端执行的时候会创建临时的session对象，其session_id也是临时的，
        //所以这里必须使用get_sessid_for_table()规则去判断
        is_same = ((session_info->get_sessid_for_table() == sessid_) &&
                   (session_info->get_sess_create_time() == sess_create_time_));
      } else if (lib::is_oracle_mode()
                 && TABLE_SCHEMA == check_stored_schema.at(i)->schema_type_
                 && !check_stored_schema.at(i)->match_compare(schema_array.at(i))) {
        // 检查oracle模式下普通表是否与系统表同名
        is_same = false;
      } else if (lib::is_oracle_mode()
                 && SYNONYM_SCHEMA == check_stored_schema.at(i)->schema_type_) {
        is_same = (check_stored_schema.at(i)->database_id_ == schema_array.at(i).database_id_);
      } else {
        // do nothing
      }
    }
  }
  return ret;
}


// used for add plan
int ObPlanCacheValue::get_all_dep_schema(ObSchemaGetterGuard &schema_guard,
                                         const DependenyTableStore &dep_schema_objs,
                                         ObIArray<PCVSchemaObj> &schema_array)
{
  int ret = OB_SUCCESS;
  schema_array.reset();
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  PCVSchemaObj tmp_schema_obj;

  for (int64_t i = 0; OB_SUCC(ret) && i < dep_schema_objs.count(); i++) {
    if (TABLE_SCHEMA != dep_schema_objs.at(i).get_schema_type()) {
      if (OB_FAIL(tmp_schema_obj.init_with_version_obj(dep_schema_objs.at(i)))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back pcv schema obj", K(ret));
      } else {
        tmp_schema_obj.reset();
      }
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                                     MTL_ID(),
                                     dep_schema_objs.at(i).get_object_id(),
                                     table_schema))) {
      LOG_WARN("failed to get table schema",
               K(ret), K(dep_schema_objs.at(i)));
    } else if (nullptr == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null table schema", K(ret));
    } else if (table_schema->is_index_table()) {
      // do nothing
    } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
      LOG_WARN("failed to init pcv schema obj", K(ret));
    } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
      LOG_WARN("failed to push back pcv schema obj", K(ret));
    } else {
      table_schema = nullptr;
      tmp_schema_obj.reset();
    }
  }

  if (OB_FAIL(ret)) {
    schema_array.reset();
  } else {
    LOG_DEBUG("get all dep schema", K(schema_array));
  }
  return ret;
}

int ObPlanCacheValue::need_check_schema_version(ObPlanCacheCtx &pc_ctx,
                                                int64_t &new_schema_version,
                                                bool &need_check)
{
  int ret = OB_SUCCESS;
  need_check = false;
  if (OB_FAIL(pc_ctx.sql_ctx_.schema_guard_->get_schema_version(pc_ctx.sql_ctx_.session_info_->get_effective_tenant_id(),
                                                                new_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret));
  } else {
    int64_t cached_tenant_schema_version = ATOMIC_LOAD(&tenant_schema_version_);
    /*
      session1 :
           create temporary table t1(c1 int, c2 int);
      session2 :
           create table t1(c1 int);
           insert into t1 values(1);
           select * from t1;
      session1 :
           select * from t1;   ->The plan match is wrong, the entity table t1 is queried, and one row is returned

        First create a temporary table and then create a normal table,
        and then add the plan of the normal table to the plan cache. In
        this case, it is impossible to predict whether there is a
        temporary table with the same name in the current session.
        Therefore, if there is a temporary table, you need to recheck the schema .
     */
    need_check = ((new_schema_version != cached_tenant_schema_version)
                  || is_contain_synonym()
                  || is_contain_tmp_tbl()
                  || is_contain_sys_pl_object()
                  || contain_sys_name_table_
                  || pc_ctx.sql_ctx_.session_info_->get_has_temp_table_flag());
    if (need_check && REACH_TIME_INTERVAL(10000000)) { //10s间隔打印
      LOG_INFO("need check schema", K(new_schema_version), K(cached_tenant_schema_version),
               K(is_contain_synonym()), K(contain_sys_name_table_), K(is_contain_tmp_tbl()),
               K(is_contain_sys_pl_object()), K(pc_ctx.sql_ctx_.session_info_->get_has_temp_table_flag()),
               K(need_check), K(constructed_sql_));
    }
  }
  return ret;
}

int ObPlanCacheValue::lift_tenant_schema_version(int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  if (new_schema_version <= tenant_schema_version_) {
    // do nothing
  } else {
    ATOMIC_STORE(&(tenant_schema_version_), new_schema_version);
  }
  return ret;
}

int ObPlanCacheValue::check_contains_table(uint64_t db_id, common::ObString tab_name, bool &contains)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; !contains && i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i))) {
      // do nothing
    } else {
      if ((stored_schema_objs_.at(i)->database_id_ == db_id) &&
              (stored_schema_objs_.at(i)->table_name_ == tab_name)) {
        contains = true;
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SPM
int ObPlanCacheValue::get_evolving_evolution_task(EvolutionPlanList &evo_task_list)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH(plan_set, plan_sets_) {
    if (OB_ISNULL(plan_set)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(plan_set), K(ret));
    } else if (!plan_set->is_sql_planset()) {
      // do nothing
    } else if (OB_FAIL(static_cast<ObSqlPlanSet*>(plan_set)->get_evolving_evolution_task(evo_task_list))) {
      LOG_WARN("fail to get evolving evolution task", K(ret));
    }
  }
  return ret;
}
#endif

}//end of namespace sql
}//end of namespace oceanbase
