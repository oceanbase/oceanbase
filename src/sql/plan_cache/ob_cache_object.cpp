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
#include "sql/plan_cache/ob_cache_object.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_truncated_string.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"

#include <cstring>

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
ObCacheObject::ObCacheObject(ObCacheObjType co_type, lib::MemoryContext& mem_context /* = CURRENT_CONTEXT */)
    : mem_context_(mem_context),
      allocator_(mem_context.get_safe_arena_allocator()),
      type_(co_type),
      ref_count_(0),
      tenant_schema_version_(OB_INVALID_VERSION),
      sys_schema_version_(OB_INVALID_VERSION),
      merged_version_(0),
      object_id_(OB_INVALID_ID),
      dependency_tables_(allocator_),
      table_stat_versions_(allocator_),
      outline_state_(),
      pre_calc_exprs_(),
      pre_calc_frames_(),
      params_info_((ObWrapperAllocator(allocator_))),
      is_contain_virtual_table_(false),
      is_contain_inner_table_(false),
      fetch_cur_time_(false),
      is_ignore_stmt_(false),
      stmt_type_(stmt::T_NONE),
      log_del_time_(INT64_MAX),
      added_to_pc_(false),
      tenant_id_(OB_INVALID_ID)
{}

int ObCacheObject::add_dependency_table_version(
    const ObIArray<uint64_t>& dependency_ids, ObDependencyTableType table_type, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaObjVersion schema_obj_version;
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_ids.count(); ++i) {
    uint64_t dependency_id = dependency_ids.at(i);
    int64_t schema_version = OB_INVALID_ID;
    schema_obj_version.object_type_ = table_type;
    if (OB_FAIL(
            schema_guard.get_schema_version_v2(schema_obj_version.get_schema_type(), dependency_id, schema_version))) {
      LOG_WARN("fail to get synonym info schema", K(dependency_id), K(schema_version), K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("dependency table not exists", K(dependency_id));
    } else {
      schema_obj_version.object_id_ = dependency_id;
      schema_obj_version.version_ = schema_version;
      if (OB_FAIL(dependency_tables_.push_back(schema_obj_version))) {
        LOG_WARN("fail to add dependency table version", K(dependency_id), K(schema_obj_version), K(ret));
      }
    }
  }
  return ret;
}

int ObCacheObject::set_params_info(const ParamStore& params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  ObParamInfo param_info;
  if (N > 0 && OB_FAIL(params_info_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve params info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    param_info.flag_ = params.at(i).get_param_flag();
    param_info.type_ = params.at(i).get_param_meta().get_type();
    if (ObCharType == param_info.type_ && params.at(i).is_null()) {
      param_info.is_oracle_empty_string_ = true;
    }
    if (params.at(i).get_param_meta().get_type() != params.at(i).get_type()) {
      LOG_TRACE("differ in set_params_info",
          K(params.at(i).get_param_meta().get_type()),
          K(params.at(i).get_type()),
          K(common::lbt()));
    }
    if (params.at(i).is_ext()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      param_info.scale_ = params.at(i).get_scale();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(params_info_.push_back(param_info))) {
        LOG_WARN("failed to push back param info", K(ret));
      }
    }
    param_info.reset();
  }
  return ret;
}

int ObCacheObject::get_base_table_version(const uint64_t table_id, int64_t& table_version) const
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(dependency_tables_, i)
  {
    const ObSchemaObjVersion& obj_version = dependency_tables_.at(i);
    if (obj_version.object_id_ == table_id) {
      if (obj_version.is_base_table()) {
        table_version = obj_version.version_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid dependency table type", K(obj_version));
      }
      break;
    }
  }
  return ret;
}

void ObCacheObject::reset()
{
  type_ = T_CO_MAX;
  ref_count_ = 0;
  tenant_schema_version_ = OB_INVALID_VERSION;
  sys_schema_version_ = OB_INVALID_VERSION;
  merged_version_ = 0;
  object_id_ = OB_INVALID_ID;
  dependency_tables_.reset();
  table_stat_versions_.reset();
  outline_state_.reset();
  pre_calc_exprs_.reset();
  pre_calc_frames_.reset();
  params_info_.reset();
  is_contain_virtual_table_ = false;
  is_contain_inner_table_ = false;
  fetch_cur_time_ = false;
  is_ignore_stmt_ = false;
  stmt_type_ = stmt::T_NONE;
  log_del_time_ = INT64_MAX;
  added_to_pc_ = false;
  tenant_id_ = OB_INVALID_ID;
}

int ObCacheObject::pre_calculation(
    const bool is_ignore_stmt, ObPreCalcExprFrameInfo& pre_calc_frame, ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo* session = exec_ctx.get_my_session();

  // TODO
  // select case when a = 1 then 1 else 1 / 0 end from t;
  // in OB, result throw an error, but in Oracle, result is fine
  // pre calc exprs should be evaluated as needed
  if (OB_ISNULL(phy_plan_ctx) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session or phy plan ctx", K(ret), K(phy_plan_ctx), K(session));
  } else {
    phy_plan_ctx->set_ignore_stmt(is_ignore_stmt);
    // create a temporary memory context for pre calculation
    // use a tmp exec ctx to pre calc exprs
    if (pre_calc_frame.pre_calc_rt_exprs_.count() <= 0) {
      // do nothing
    } else if (OB_FAIL(pre_calc_frame.pre_alloc_exec_memory(exec_ctx))) {
      LOG_WARN("failed to pre alloc exec memory", K(ret));
    } else if (OB_FAIL(exec_ctx.init_eval_ctx())) {
      LOG_WARN("failed to init eval ctx", K(ret));
    } else if (OB_FAIL(exec_ctx.init_expr_op(pre_calc_frame.need_ctx_cnt_))) {
      LOG_WARN("failed to init expr op ctx", K(ret));
    } else {
      // datum_store will be used out of the block, use exec_ctx's allocator
      DatumParamStore datum_store(ObWrapperAllocator(exec_ctx.get_allocator()));

      ObDatum* res_datum = NULL;
      ObDatumObjParam datum_param;
      for (int i = 0; OB_SUCC(ret) && i < pre_calc_frame.pre_calc_rt_exprs_.count(); i++) {
        ObExpr* rt_expr = pre_calc_frame.pre_calc_rt_exprs_.at(i);
        if (OB_ISNULL(rt_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null rt expr", K(ret));
        } else if (OB_FAIL(rt_expr->eval(*exec_ctx.get_eval_ctx(), res_datum))) {
          LOG_WARN("failed to to eval", K(ret));
        } else {
          datum_param.set_datum(*res_datum);
          datum_param.set_meta(rt_expr->datum_meta_);
          if (OB_FAIL(datum_store.push_back(datum_param))) {
            LOG_WARN("failed to push back datum param", K(ret));
          }
        }
      }  // for end
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(phy_plan_ctx->extend_datum_param_store(datum_store))) {
        LOG_WARN("failed to extend param frame", K(ret));
      } else {
        // do nothing
      }
      // reset expr op anyway
      exec_ctx.reset_expr_op();
    }
  }
  return ret;
}

int ObCacheObject::pre_calculation(const stmt::StmtType& stmt_type, const bool is_ignore_stmt,
    const common::ObDList<ObSqlExpression>& pre_calc_exprs, ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;

  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx()) || OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx or session is NULL", K(plan_ctx), K(session));
  } else {
    plan_ctx->set_ignore_stmt(is_ignore_stmt);
    if (pre_calc_exprs.get_size() > 0) {
      ObExprCtx expr_ctx;
      ParamStore& params = plan_ctx->get_param_store_for_update();
      ObObjParam result;
      ObObj tmp_res;
      ObNewRow empty_row;
      expr_ctx.calc_buf_ = &ctx.get_allocator();
      expr_ctx.phy_plan_ctx_ = plan_ctx;
      expr_ctx.my_session_ = session;
      expr_ctx.exec_ctx_ = &ctx;
      expr_ctx.is_pre_calculation_ = true;
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(stmt_type, expr_ctx.my_session_, expr_ctx.cast_mode_))) {
        LOG_WARN("set cast mode failed", K(ret));
      } else {
        if (ObStmt::is_dml_stmt(stmt_type)) {
          if (OB_FAIL(ObSQLUtils::wrap_column_convert_ctx(expr_ctx, expr_ctx.column_conv_ctx_))) {
            LOG_WARN("wrap column convert ctx failed", K(ret));
          }
        }
        DLIST_FOREACH(node, pre_calc_exprs)
        {
          if (OB_ISNULL(node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node is null");
          } else if (node->need_construct_binding_array()) {
            // array bind expr, need construct new binding array
            if (OB_FAIL(construct_array_params(*node, ctx.get_allocator(), expr_ctx, empty_row, result))) {
              LOG_WARN("construct array params failed", K(ret));
            }
          } else if (OB_FAIL(node->calc(expr_ctx, empty_row, tmp_res))) {
            LOG_WARN("failed to calculate", K(ret), "node", *node);
          } else if (OB_FAIL(ob_write_obj(ctx.get_allocator(), tmp_res, result))) {  // deep copy
            LOG_WARN("failed to deep copy param obj", K(ret), K(tmp_res));
          } else {
            result.set_param_meta();
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(params.push_back(result))) {
              LOG_WARN("failed to add hidden const value", K(ret), K(result));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCacheObject::type_to_name(const ObCacheObjType type, common::ObIAllocator& allocator, common::ObString& type_name)
{
  int ret = OB_SUCCESS;
  const char* type_strs[] = {"SQL_PLAN", "PROCEDURE", "FUNCTION", "PACKAGE", "ANONYMOUS"};
  char* buf = NULL;
  if (type >= T_CO_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache obj type", K(ret), K(type));
  } else {
    int32_t str_len = (int32_t)std::strlen(type_strs[static_cast<int64_t>(type)]);
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(str_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(str_len));
    } else {
      MEMCPY(buf, type_strs[static_cast<int64_t>(type)], str_len);
      type_name.assign(buf, str_len);
    }
  }
  return ret;
}

int ObCacheObject::construct_array_params(
    const ObSqlExpression& expr, ObIAllocator& allocator, ObExprCtx& expr_ctx, ObNewRow& row, ObObjParam& result)
{
  UNUSED(expr_ctx);
  UNUSED(allocator);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not supported", K(expr), K(row), K(result));
  return ret;
}

bool ObCacheObject::has_sequence() const
{
  int ret = OB_SUCCESS;
  bool has_sequence = false;
  ARRAY_FOREACH(dependency_tables_, i)
  {
    const ObSchemaObjVersion& obj_version = dependency_tables_.at(i);
    if (DEPENDENCY_SEQUENCE == obj_version.object_type_) {
      has_sequence = true;
      break;
    }
  }
  return has_sequence;
}

void ObCacheObject::dump_deleted_log_info(const bool is_debug_log) const
{
  ObString raw_sql;
  if (is_sql_crsr()) {
    const ObPhysicalPlan* plan = dynamic_cast<const ObPhysicalPlan*>(this);
    if (OB_ISNULL(plan)) {
      LOG_ERROR("the plan is null", K(plan), K(this));
    } else {
      raw_sql = ObTruncatedString(plan->stat_.raw_sql_, OB_MAX_SQL_LENGTH).string();
    }
  } else if (is_anon()) {
  } else {
    // do nothing
  }
  if (is_debug_log) {
    SQL_PC_LOG(DEBUG,
        "Dumping Cache Deleted Info",
        K(object_id_),
        K(tenant_id_),
        K(added_to_pc_),
        K(type_),
        K(get_ref_count()),
        K(log_del_time_),
        K(raw_sql),
        K(this));
  } else {
    SQL_PC_LOG(INFO,
        "Dumping Cache Deleted Info",
        K(object_id_),
        K(tenant_id_),
        K(added_to_pc_),
        K(type_),
        K(get_ref_count()),
        K(log_del_time_),
        K(raw_sql),
        K(this));
  }
}

int64_t ObCacheObject::inc_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    ObPlanCache* plan_cache = ObCacheObjectFactory::get_plan_cache(tenant_id_);
    if (OB_ISNULL(plan_cache)) {
      LOG_ERROR("invalid null plan cache", K(ret), K(tenant_id_));
    } else {
      plan_cache->get_ref_handle_mgr().record_ref_op(ref_handle);
    }
  }
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObCacheObject::dec_ref_count(const CacheRefHandleID ref_handle)
{
  int ret = OB_SUCCESS;
  if (GCONF._enable_plan_cache_mem_diagnosis) {
    ObPlanCache* plan_cache = ObCacheObjectFactory::get_plan_cache(tenant_id_);
    if (OB_ISNULL(plan_cache)) {
      // if plan cache is marked by destroyed_, this is not a ERROR.
      LOG_WARN("invalid null plan cache", K(ret), K(tenant_id_));
    } else {
      plan_cache->get_ref_handle_mgr().record_deref_op(ref_handle);
    }
  }
  return ATOMIC_SAF(&ref_count_, 1);
}
}  // namespace sql
}  // namespace oceanbase
