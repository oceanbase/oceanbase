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

#include "ob_expr_udf.h"
#include "observer/ob_server.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package.h"
#include "ob_udf_result_cache.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprUDFCtx::~ObExprUDFCtx()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(deterministic_cache_)) {
    deterministic_cache_->~ObExprUDFDeterministerCache();
    deterministic_cache_ = nullptr;
  }
  if (OB_NOT_NULL(phy_plan_ctx_)) {
    phy_plan_ctx_->~ObPhysicalPlanCtx();
    phy_plan_ctx_ = nullptr;
  }
}

int ObExprUDFCtx::init_param_store(ObExecContext &exec_ctx, int param_num)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = ctx_allocator_;
  void *param_store_buf = nullptr;
  void *phy_plan_ctx_buf = nullptr;
  int64_t query_timeout = 0;
  if (OB_FAIL(exec_ctx.get_my_session()->get_query_timeout(query_timeout))) {
    LOG_WARN("failed to get query timeout", K(ret), K(query_timeout));
  } else if (OB_ISNULL(param_store_buf = allocator.alloc(sizeof(ParamStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(param_num));
  } else if (param_num > 0
             && OB_ISNULL(obj_stack_ = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * param_num)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(param_num));
  } else if (OB_ISNULL(phy_plan_ctx_buf = allocator.alloc(sizeof(sql::ObPhysicalPlanCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate physical plan ctx", K(ret));
  } else if (FALSE_IT(phy_plan_ctx_ = new(phy_plan_ctx_buf)sql::ObPhysicalPlanCtx(allocator))) {
  } else {
    params_ = new(param_store_buf)ParamStore(ObWrapperAllocator(allocator));
    if (OB_ISNULL(params_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), KP(params_));
    } else if (OB_FAIL(params_->prepare_allocate(param_num))) {
      LOG_WARN("failed to prepare allocate", K(ret), K(param_num));
    } else {
      for (int64_t i = 0; i < param_num; ++i) {
        new (&obj_stack_[i]) ObObj();
      }
      params_->reuse();
    }
    int64_t total_time =
      (exec_ctx.get_physical_plan_ctx() != NULL
        && exec_ctx.get_physical_plan_ctx()->get_timeout_timestamp() > 0) ?
          exec_ctx.get_physical_plan_ctx()->get_timeout_timestamp()
          : exec_ctx.get_my_session()->get_query_start_time() + query_timeout;
    phy_plan_ctx_->set_timeout_timestamp(total_time);
    phy_plan_ctx_->set_cur_time(ObTimeUtility::current_time(), *exec_ctx.get_my_session());
  }
  return ret;
}

int ObExprUDFCtx::init_exec_ctx(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx())
             || OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
    sql::ObTaskExecutorCtx &task_ctx = exec_ctx.get_task_exec_ctx();
    const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
    if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(exec_ctx.get_my_session()->get_effective_tenant_id(),
                                                              schema_guard_,
                                                              task_ctx.get_query_tenant_begin_schema_version(),
                                                              task_ctx.get_query_sys_begin_schema_version()))) {
      LOG_WARN("get schema guard failed", K(ret));
    }
  }
  // 通过分布式计划执行的function没有sqlctx信息, 构造一个
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx())) {
    sql_ctx_.session_info_ = exec_ctx.get_my_session();
    sql_ctx_.schema_guard_ = &schema_guard_;
    exec_ctx.set_sql_ctx(&sql_ctx_);
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
    exec_ctx.get_sql_ctx()->schema_guard_ = &schema_guard_;
  }
  return ret;
}

int ObExprUDFCtx::init(const ObExpr &expr, ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  info_ = static_cast<ObExprUDFInfo *>(expr.extra_info_);
  exec_ctx_ = &exec_ctx;
  session_info_ = exec_ctx.get_my_session();

  if (OB_ISNULL(info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr info", K(ret), KP(info_));
  } else {
    package_id_ = info_->is_udt_udf_
      ? share::schema::ObUDTObjectType::mask_object_id(info_->udf_package_id_)
        : info_->udf_package_id_;
    arg_count_ = expr.arg_cnt_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_types(expr))) {
    LOG_WARN("failed to check types", K(ret));
  } else if (OB_FAIL(init_param_store(exec_ctx, expr.arg_cnt_))) {
    LOG_WARN("failed to init param store", K(ret));
  } else if (OB_FAIL(init_exec_ctx(exec_ctx))) {
    LOG_WARN("failed to init exec ctx", K(ret));
  } else if (OB_FAIL(init_pl_alloc())) {
    LOG_WARN("failed to init pl alloc", K(ret));
  } else if (OB_FAIL(calc_cache_enabled())) {
    LOG_WARN("failed to calc cache enabled", K(ret));
  } else if (is_cache_enabled()) {
    if (OB_FAIL(generate_influence_string())) {
      LOG_WARN("failed to generate influence string", K(ret));
    } else if (OB_FAIL(init_row_key(ctx_allocator_, arg_count_))) {
      LOG_WARN("failed to init row key", K(ret), K(arg_count_));
    } else if (get_info()->is_deterministic_) {
      if (OB_FAIL(init_deterministic_cache())) {
        LOG_WARN("failed to init deterministic cache", K(ret));
      }
    }
  }
  return ret;
}

int ObExprUDFCtx::check_types(const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected info", K(ret));
  } else if (expr.arg_cnt_ != info_->params_desc_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udf parameter number is not equel to params desc count",
             K(ret), K(expr.arg_cnt_), K(info_->params_desc_.count()), K(info_->params_desc_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (!expr.args_[i]->obj_meta_.is_null() && (!info_->params_desc_.at(i).is_out())) {
      if (expr.args_[i]->obj_meta_.get_type() != info_->params_type_.at(i).get_type()) {
        if (info_->params_type_.at(i).is_enum_or_set() && (ObVarcharType == expr.args_[i]->obj_meta_.get_type() || expr.args_[i]->obj_meta_.is_numeric_type())) {
          // do nothing ...
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("check param type failed", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && info_->params_desc_.at(i).is_out()) {
      has_out_param_ = true;
    }
  }
  return ret;
}

int ObExprUDFCtx::reuse(const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  if (info_ != expr.extra_info_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret), KP(info_), KP(expr.extra_info_));
  } else {
    if (OB_NOT_NULL(params_)) {
      params_->reuse();
    }
    if (OB_NOT_NULL(phy_plan_ctx_)) {
      phy_plan_ctx_->get_param_store_for_update().reuse();
    }
    allocator_.reuse();
  }
  return ret;
}

int ObExprUDFCtx::init_pl_alloc()
{
  int ret = OB_SUCCESS;
  pl::ObPLExecCtx *pl_exec_ctx = nullptr;
  if (OB_ISNULL(session_info_) || OB_ISNULL(info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info", K(ret));
  } else if (!info_->is_called_in_sql_) {
    if (OB_ISNULL(session_info_->get_pl_context())) {
      if (OB_ISNULL(exec_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exec ctx ptr", K(ret));
      } else {
        alloc_ = &(exec_ctx_->get_allocator());
      }
    } else if (OB_ISNULL(pl_exec_ctx = session_info_->get_pl_context()->get_current_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pl exec context", K(ret));
    } else if (OB_ISNULL(alloc_ = pl_exec_ctx->get_top_expr_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pl allocator", K(ret));
    }
  }
  return ret;
}

int ObExprUDFCtx::init_deterministic_cache()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exec context", K(ret));
  } else if (OB_ISNULL(deterministic_cache_ = reinterpret_cast<ObExprUDFDeterministerCache *>
                          (ctx_allocator_.alloc(sizeof(ObExprUDFDeterministerCache))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for determistic cache", K(ret));
  } else if (FALSE_IT(new (deterministic_cache_)ObExprUDFDeterministerCache(exec_ctx_->get_deterministic_udf_cache_allocator()))) {
  } else if (OB_FAIL(deterministic_cache_->init())) {
    LOG_WARN("failed to init deterministic cache", K(ret));
  }
  return ret;
}

int ObExprUDFCtx::init_row_key(ObIAllocator &allocator, int64_t arg_cnt)
{
  int ret = OB_SUCCESS;
  row_key_.cnt_ = arg_cnt;

  if (arg_cnt > 0) {
    if (OB_ISNULL(row_key_.elems_ = static_cast<ObDatum *>(allocator.alloc(arg_cnt * sizeof(ObDatum))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg_cnt; ++i) {
        if (OB_ISNULL(row_key_.elems_[i].ptr_ = reinterpret_cast<char*>(allocator.alloc(OBJ_DATUM_MAX_RES_SIZE)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else {
          if (info_->params_type_.at(i).is_null()) {
            if (OB_FAIL(row_key_.default_param_bitmap_.add_member(i))) {
              LOG_WARN("fail to add member", K(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprUDFCtx::construct_key()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_arg_count(); ++i) {
    if (OB_FAIL(row_key_.elems_[i].from_obj(get_obj_stack()[i]))) {
      LOG_WARN("failed to construct datum from obj", K(ret), K(i));
    }
  }
  return ret;
}

int ObExprUDFCtx::calc_cache_enabled()
{
  int ret = OB_SUCCESS;
  bool is_cursor_type = ObExtendType == get_info()->result_type_.get_type()
                        && (pl::PL_CURSOR_TYPE == get_info()->result_type_.get_extend_type()
                            || pl::PL_REF_CURSOR_TYPE == get_info()->result_type_.get_extend_type());
  bool is_subprogram_routine = !get_info()->subprogram_path_.empty();
  bool is_dblink_routine = get_info()->dblink_id_ != OB_INVALID_ID;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (is_cursor_type || !tenant_config.is_valid() || is_subprogram_routine || is_dblink_routine) {
    enable_result_cache_ = false;
    enable_deterministic_cache_ = false;
  } else {
    int64_t result_cache_size = tenant_config->result_cache_max_size;
    int64_t deterministic_cache_size = tenant_config->ob_deterministic_udf_cache_max_size;
    enable_result_cache_ = get_info()->is_result_cache_ && result_cache_size > 0;
    enable_deterministic_cache_ = !enable_result_cache_
                                  && get_info()->is_deterministic_
                                  && deterministic_cache_size > 0;
    result_cache_max_result_ = tenant_config->result_cache_max_result;
    result_cache_max_size_ = result_cache_size;

  }
  return ret;
}

int ObExprUDFCtx::calc_result_cache_enabled()
{
  int ret = OB_SUCCESS;
  if (!is_result_cache_enabled()) {
    // do nothing ...
  } else if (OB_FAIL(calc_current_function())) {
    LOG_WARN("failed to cacl current function", K(ret));
  } else if (OB_ISNULL(current_function_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected current function", K(ret));
  } else if (current_function_->get_has_parallel_affect_factor()) {
    enable_result_cache_ = false;
    if (get_info()->is_deterministic_) {
      enable_deterministic_cache_ = true;
    }
  }
  return ret;
}

int ObExprUDFCtx::calc_current_function()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(current_function_)) {
    // do nothing ...
  } else {
    ObCacheObjGuard *cacheobj_guard = NULL;
    pl::ObPLFunction *cache_object = NULL;
    CK (OB_NOT_NULL(exec_ctx_->get_sql_ctx()->schema_guard_));
    if (get_info()->udf_package_id_ != OB_INVALID_ID) {
      pl::ObPLPackageGuard *package_guard = NULL;
      if (OB_ISNULL(package_guard = exec_ctx_->get_package_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected package guard", K(ret));
      } else if (OB_ISNULL(exec_ctx_->get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected schema guard", K(ret));
      } else if (!get_info()->is_udt_udf_) { // package routine
        ObArenaAllocator allocator("UDFTmpAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        pl::ObPLResolveCtx resolve_ctx(allocator,
                                       *session_info_,
                                       *exec_ctx_->get_sql_ctx()->schema_guard_,
                                       *exec_ctx_->get_package_guard(),
                                       *GCTX.sql_proxy_,
                                       false);
        pl::ObPLPackage *package_spec = nullptr;
        pl::ObPLPackage *package_body = nullptr;
        OZ (session_info_->get_pl_engine()->get_package_manager().get_cached_package(
          resolve_ctx, get_info()->udf_package_id_, package_spec, package_body));
        CK (OB_NOT_NULL(package_spec));
        CK (OB_NOT_NULL(package_body));
        OZ (package_body->get_routine(get_info()->udf_id_, current_function_));
        CK (OB_NOT_NULL(current_compile_unit_ = package_body));
        CK (OB_NOT_NULL(current_function_));
      } else { // udt routine
        uint64_t udt_id = ObUDTObjectType::clear_object_id_mask(get_info()->udf_package_id_);
        uint64_t udt_body_id = OB_INVALID_ID;
        const ObUDTTypeInfo *udt_info = NULL;
        uint64_t effective_tenant_id = get_session_info()->get_effective_tenant_id();
        const int64_t tenant_id = pl::get_tenant_id_by_object_id(udt_id);
        pl::ObPLPackage *udt_body = nullptr;
        OZ (exec_ctx_->get_sql_ctx()->schema_guard_->get_udt_info(tenant_id, udt_id, udt_info), K(udt_id));
        OV (OB_NOT_NULL(udt_info), OB_ERR_UNEXPECTED, K(udt_id), K(ObUDTObjectType::clear_object_id_mask(udt_id)));
        OX (udt_body_id = udt_info->get_object_body_id(effective_tenant_id));
        OZ (package_guard->get(ObUDTObjectType::mask_object_id(udt_body_id), cacheobj_guard));
        CK (udt_body = static_cast<pl::ObPLPackage *>(cacheobj_guard->get_cache_obj()));
        OZ (udt_body->get_routine(get_info()->udf_id_, current_function_));
        CK (OB_NOT_NULL(current_compile_unit_ = udt_body));
        CK (OB_NOT_NULL(current_function_));
      }
    } else {
      CK (OB_NOT_NULL(current_function_ = static_cast<pl::ObPLFunction *>(cacheobj_guard_.get_cache_obj())));
      OX (current_compile_unit_ = current_function_);
    }
  }
  return ret;
}

int ObExprUDFCtx::get_result_from_cache(ObObj &result, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (!is_cache_enabled()) {
    // do nothing ...
  } else if (OB_FAIL(construct_key())) {
    LOG_WARN("faield to construct key", K(ret));
  } else if (is_deterministic_cache_enabled()) {
    if (OB_ISNULL(deterministic_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected deterministic cache", K(ret));
    } else if (OB_FAIL(deterministic_cache_->get_result_from_cache(row_key_, result, found))) {
      LOG_WARN("failed to get result from cache", K(ret));
    }
  } else if (is_result_cache_enabled()) {
    DISABLE_SQL_MEMLEAK_GUARD;
    if (OB_FAIL(get_result_from_result_cache(result, found))) {
      LOG_WARN("failed to get result from result cache", K(ret));
    }
  }
  return ret;
}


int ObExprUDFCtx::add_result_to_cache(ObObj &result)
{
  int ret = OB_SUCCESS;
  if (is_cache_enabled()) {
    if (is_result_cache_enabled()) {
      if (OB_FAIL(calc_result_cache_enabled())) {
        LOG_WARN("failed to cacl cache result", K(ret));
      } else if (is_result_cache_enabled()) {
        DISABLE_SQL_MEMLEAK_GUARD;
        if (OB_FAIL(add_result_to_result_cache(result))) {
          LOG_WARN("failed to add result to result cache", K(ret), K(result));
        }
      }
    }
    if (OB_SUCC(ret) && is_deterministic_cache_enabled()) {
      if (OB_ISNULL(deterministic_cache_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected determinister cache", K(ret));
      } else if (OB_FAIL(deterministic_cache_->add_result_to_cache(row_key_, result))) {
        LOG_WARN("failed to add result to determinister cache", K(ret), K(result));
      }
    }
  }
  return ret;
}

// Interface for Determinister Cache

int ObExprUDFDeterministerCache::init()
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_FAIL(cache_.create(64 * 1024,
                ObMemAttr(MTL_ID(), "UdfResult", ObCtxIds::DEFAULT_CTX_ID),
                ObMemAttr(MTL_ID(), "UdfResultNode", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("failed to create hash map", K(ret), K(MTL_ID()));
  } else if (!tenant_config.is_valid()) {
    // do nothing ...
  } else {
    cache_limit_ = tenant_config->ob_deterministic_udf_cache_max_size;
    is_inited_ = true;
  }
  return ret;
}

int ObExprUDFDeterministerCache::add_result_to_cache(const pl::UDFArgRow &key, ObObj &result)
{
  int ret = OB_SUCCESS;
  pl::UDFArgRow cache_key;
  ObObj value;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("determinister cache not init yet", K(ret));
  } else {
    int64_t need_size = sizeof(ObDatum) + sizeof(int64_t);
    cache_key.cnt_ = key.cnt_;
    for (int64_t i = 0; i < cache_key.cnt_; ++i) {
      need_size += (key.elems_[i].len_ + sizeof(ObDatum));
    }
    if (need_size + allocator_.used() >= cache_limit_) {
      LOG_TRACE("udf cache reach memory limit", K(cache_limit_), K(need_size), K(allocator_.used()));
    } else {
      if (cache_key.cnt_ > 0
          && OB_ISNULL(cache_key.elems_ = reinterpret_cast<ObDatum *>(allocator_.alloc(sizeof(ObDatum)*cache_key.cnt_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for row key", K(ret),  K(cache_key.cnt_));
      }
      OZ (cache_key.default_param_bitmap_.assign(key.default_param_bitmap_));
      for (int64_t i = 0; OB_SUCC(ret) && i < cache_key.cnt_; ++i) {
        if (OB_FAIL(cache_key.elems_[i].deep_copy(key.elems_[i], allocator_))) {
          LOG_WARN("failed to copy probe row", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (result.is_pl_extend()) {
        if (OB_FAIL(pl::ObUserDefinedType::deep_copy_obj(allocator_, result, value))) {
          LOG_WARN("fail to deep copy result", K(result));
        }
      } else if (OB_FAIL(deep_copy_obj(allocator_, result, value))) {
        LOG_WARN("fail to deep copy result", K(result));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cache_.set_refactored(cache_key, value))) {
        LOG_WARN("fail to add value to hashmap", K(ret), K(cache_key));
        if (value.is_pl_extend()) {
          int tmp_ret = pl::ObUserDefinedType::destruct_obj(value, NULL);
          if (tmp_ret != OB_SUCCESS) {
            LOG_WARN("failed to destuct obj", K(ret), K(value));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprUDFDeterministerCache::get_result_from_cache(const pl::UDFArgRow &key, ObObj &result, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("determinister cache not init yet!", K(ret));
  } else if (OB_FAIL(cache_.get_refactored(key, result))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to find in hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    found = true;
  }
  return ret;
}

// Interface for Result Cache

int ObExprUDFCtx::generate_influence_string()
{
  int ret = OB_SUCCESS;
  if (!is_result_cache_enabled()) {
    // do nothing ....
  } else if (OB_FAIL(generate_influence_sys_var_string())) {
    LOG_WARN("failed to init influence sys var string", K(ret));
  } else if (OB_FAIL(generate_influence_config_string())) {
    LOG_WARN("failed to init influence config string", K(ret));
  }
  return ret;
}

int ObExprUDFCtx::generate_influence_sys_var_string()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo &session = *get_session_info();
  ObIAllocator &allocator = ctx_allocator_; // only call this once for every request.
  ObObj val;
  ObSysVarInPC sys_vars;
  char *buf = nullptr;
  int64_t pos = 0;
  const int64_t MAX_SYS_VARS_STR_SIZE = 512;
  int64_t sys_var_encode_max_size = MAX_SYS_VARS_STR_SIZE;
  for (int64_t i = 0; OB_SUCC(ret) && i < pl::RESULT_CACHE_SYS_VAR_COUNT; ++i) {
    val.reset();
    if (OB_FAIL(session.get_sys_variable(pl::InfluenceMap[i], val))) {
      LOG_WARN("failed to get sys_variable", K(pl::InfluenceMap[i]), K(ret));
    } else if (OB_FAIL(sys_vars.push_back(val))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(MAX_SYS_VARS_STR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocator memory", K(ret), K(MAX_SYS_VARS_STR_SIZE));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sys_vars.serialize_sys_vars(buf, sys_var_encode_max_size, pos))) {
      if (OB_BUF_NOT_ENOUGH == ret || OB_SIZE_OVERFLOW ==ret) {
        ret = OB_SUCCESS;
        // expand MAX_SYS_VARS_STR_SIZE 3 times.
        for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
          sys_var_encode_max_size = 2 * sys_var_encode_max_size;
          if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(sys_var_encode_max_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocator memory", K(ret), K(sys_var_encode_max_size));
          } else if (OB_FAIL(sys_vars.serialize_sys_vars(buf, sys_var_encode_max_size, pos))) {
            if (i != 2 && (OB_BUF_NOT_ENOUGH == ret || OB_SIZE_OVERFLOW ==ret)) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to serialize system vars", K(ret));
            }
          } else {
            break;
          }
        }
      } else {
        LOG_WARN("fail to serialize system vars", K(ret));
      }
      if (OB_SUCC(ret)) {
        sys_var_str_.assign(buf, (int32_t)(pos));
      }
    } else {
      sys_var_str_.assign(buf, (int32_t)(pos));
    }
  }
  return ret;
}

int ObExprUDFCtx::generate_influence_config_string()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_CONFIG_VARS_STR_SIZE = 32;
  char *config_buf = nullptr;
  int64_t pos = 0;
  ObIAllocator &allocator = ctx_allocator_;
  if (!is_cache_enabled()) {
    // do nothing ...
  } else if (OB_ISNULL(config_buf = reinterpret_cast<char *>(allocator.alloc(MAX_CONFIG_VARS_STR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocator memory", K(ret), K(MAX_CONFIG_VARS_STR_SIZE));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config invalid", K(ret));
    } else if (OB_FAIL(databuff_printf(config_buf, MAX_CONFIG_VARS_STR_SIZE, pos,
                                       "%d", (bool)(tenant_config->_enable_decimal_int_type)))) {
      LOG_WARN("failed to databuff_printf", K(ret));
    } else {
      config_var_str_.assign(config_buf, int32_t(pos));
    }
  }
  return ret;
}

int ObExprUDFCtx::get_result_from_result_cache(ObObj &result, bool &found)
{
  int ret = OB_SUCCESS;
  ObCacheObjGuard cacheobj_guard(PC_REF_UDF_RESULT_HANDLE);
  pl::ObPLUDFResultCacheCtx rc_ctx;
  pl::ObPLUDFResultCacheObject *udf_result = nullptr;
  found = false;
  if (OB_ISNULL(current_function_)) {
    // first execute, do not try to get cache for permission check
  } else if (OB_FAIL(construct_cache_ctx_for_get(rc_ctx))) {
    LOG_WARN("failed to construct cache ctx for get", K(ret));
  } else if (OB_FAIL(pl::ObPLUDFResultCacheMgr::get_udf_result_cache(
                        get_session_info()->get_plan_cache(), cacheobj_guard, rc_ctx))) {
    LOG_TRACE("get result from result cache failed", K(ret), K(rc_ctx.key_));
    ret = OB_ERR_UNEXPECTED != ret ? OB_SUCCESS : ret;
  } else if (FALSE_IT(udf_result = static_cast<pl::ObPLUDFResultCacheObject*>(cacheobj_guard.get_cache_obj()))) {
    // do nothing
  } else if (OB_NOT_NULL(udf_result)) {
    LOG_DEBUG("get result from result cache success", KPC(udf_result));
    if (OB_FAIL(pl::ObPLUDFResultCacheObject::deep_copy_result(exec_ctx_->get_allocator(), udf_result->get_result(), result))) {
      LOG_WARN("failed to deep copy result from cache result", K(ret));
    } else {
      found = true;
      if (result.is_pl_extend() && result.get_meta().get_extend_type()) {
        if (OB_ISNULL(exec_ctx_->get_pl_ctx())) {
          OZ (exec_ctx_->init_pl_ctx());
        }
        OZ (exec_ctx_->get_pl_ctx()->add(result));
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if ((tmp_ret = pl::ObUserDefinedType::destruct_obj(result, exec_ctx_->get_my_session())) != OB_SUCCESS) {
            LOG_WARN("failed to destruct result object", K(ret), K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprUDFCtx::construct_cache_ctx_for_get(pl::ObPLUDFResultCacheCtx &rc_ctx)
{
  int ret = OB_SUCCESS;
  rc_ctx.session_info_ = get_session_info();
  rc_ctx.schema_guard_ = exec_ctx_->get_sql_ctx()->schema_guard_;

  rc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_UDF_RESULT_CACHE;
  if (OB_ISNULL(current_function_) || current_function_->is_invoker_right()) {
    rc_ctx.key_.db_id_ = get_session_info()->get_database_id();
  } else {
    rc_ctx.key_.db_id_ = current_function_->get_database_id();
  }
  rc_ctx.key_.package_id_ = get_info()->udf_package_id_;
  rc_ctx.key_.routine_id_ = get_info()->udf_id_;
  rc_ctx.key_.sys_vars_str_ = sys_var_str_;
  rc_ctx.key_.config_vars_str_ = config_var_str_;
  rc_ctx.argument_params_ = row_key_;
  OZ (row_key_.hash(rc_ctx.cache_obj_hash_value_, 0));
  return ret;
}

int ObExprUDFCtx::construct_cache_ctx_for_add(pl::ObPLUDFResultCacheCtx &rc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_current_function())) {
    LOG_WARN("failed to calc current function", K(ret));
  } else if (OB_ISNULL(current_function_) || OB_ISNULL(current_compile_unit_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected current function", K(ret), KP(current_function_), KP(current_compile_unit_));
  } else {
    rc_ctx.dependency_tables_ = &current_compile_unit_->get_dependency_table();
    rc_ctx.sys_schema_version_ = current_compile_unit_->get_sys_schema_version();
    rc_ctx.tenant_schema_version_ = current_compile_unit_->get_tenant_schema_version();
    rc_ctx.name_ = current_function_->get_function_name();
    if (current_function_->is_invoker_right()) {
      rc_ctx.key_.db_id_ = get_session_info()->get_database_id();
    } else {
      rc_ctx.key_.db_id_ = current_function_->get_database_id();
    }
    rc_ctx.result_cache_max_result_ = result_cache_max_result_;
    rc_ctx.result_cache_max_size_ = result_cache_max_size_;
  }
  return ret;
}

int ObExprUDFCtx::generate_result_cache_object(ObObj &result,
                                       ObCacheObjGuard &cache_result_guard,
                                       pl::ObPLUDFResultCacheObject *&udf_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCacheObjectFactory::alloc(cache_result_guard,
                                          ObLibCacheNameSpace::NS_UDF_RESULT_CACHE,
                                          get_session_info()->get_effective_tenant_id()))) {
    LOG_WARN("fail to alloc cache object", K(ret));
  } else if (FALSE_IT(udf_result = static_cast<pl::ObPLUDFResultCacheObject *>(cache_result_guard.get_cache_obj()))) {
  } else if (OB_ISNULL(udf_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected udf_result ptr", K(ret), KP(udf_result));
  } else if (OB_FAIL(pl::ObPLUDFResultCacheObject::deep_copy_result(udf_result->get_allocator(), result, udf_result->get_result()))) {
    LOG_WARN("fail to deep copy result", K(ret), K(result));
  }
  return ret;
}

int ObExprUDFCtx::add_result_to_result_cache(ObObj &result)
{
  int ret = OB_SUCCESS;
  ObCacheObjGuard cache_result_guard(PC_REF_UDF_RESULT_HANDLE);
  ObPlanCache *plan_cache = NULL;
  pl::ObPLCacheCtx pc_ctx;
  pl::ObPLUDFResultCacheObject *udf_result = NULL;
  pl::ObPLUDFResultCacheCtx rc_ctx;
  if (OB_ISNULL(plan_cache = get_session_info()->get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_FAIL(construct_cache_ctx_for_get(rc_ctx))) {
    LOG_WARN("failed to construct cache ctx for get", K(ret));
  } else if (OB_FAIL(construct_cache_ctx_for_add(rc_ctx))) {
    LOG_WARN("fail to construct result cache key", K(ret));
  } else if (OB_FAIL(generate_result_cache_object(result, cache_result_guard, udf_result))) {
    LOG_WARN("fail to gen cache obj", K(ret));
  } else if (OB_FAIL(pl::ObPLUDFResultCacheMgr::add_udf_result_cache(plan_cache, udf_result, rc_ctx))) {
    if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("this cache obj has been added by others, need not add again", KPC(udf_result));
    } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else if (is_not_supported_err(ret)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("plan cache don't support add this kind of plan now",  KPC(udf_result));
    } else {
      if (OB_REACH_MAX_CONCURRENT_NUM != ret) {
        ret = OB_SUCCESS;
        LOG_WARN("Failed to add plan to ObPlanCache", K(ret));
      }
    }
  }
  return ret;
}

}
}
