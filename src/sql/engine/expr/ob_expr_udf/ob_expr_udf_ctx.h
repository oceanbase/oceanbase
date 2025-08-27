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

#ifndef OCEANBASE_SQL_OB_EXPR_UDF_CTX_H_
#define OCEANBASE_SQL_OB_EXPR_UDF_CTX_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "ob_udf_result_cache_mgr.h"
#include "pl/ob_pl.h"
#include "ob_expr_udf_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace sql
{

class ObExprUDFInfo;

class ObExprUDFDeterministerCache
{
public:
  ObExprUDFDeterministerCache(ObIAllocator &allocator)
    : is_inited_(false), cache_limit_(0), allocator_(allocator) {}
  ~ObExprUDFDeterministerCache()
  {
    if (cache_.created()) {
      for (hash::ObHashMap<pl::UDFArgRow, ObObj, common::hash::NoPthreadDefendMode>::iterator iter = cache_.begin();
          iter != cache_.end();
          iter++) {
        if (iter->second.is_pl_extend()) {
          pl::ObUserDefinedType::destruct_obj(iter->second, nullptr);
        }
      }
      cache_.destroy();
    }
  }

  int init();

  int add_result_to_cache(const pl::UDFArgRow &key, ObObj &result);
  int get_result_from_cache(const pl::UDFArgRow &key, ObObj &result, bool &found);

  ObIAllocator &get_allocator() { return allocator_; }

private:
  bool is_inited_;
  int64_t cache_limit_;
  ObIAllocator &allocator_;
  common::hash::ObHashMap<pl::UDFArgRow, common::ObObj, common::hash::NoPthreadDefendMode> cache_;
};

// Its An UDF status cache, this Ctx will cache almost all reusable status for singal UDF.
class ObExprUDFCtx : public ObExprOperatorCtx
{
public:
  ObExprUDFCtx()
  : ObExprOperatorCtx(),
    params_(nullptr),

    row_key_(),
    sys_var_str_(),
    config_var_str_(),
    enable_result_cache_(false),
    enable_deterministic_cache_(false),
    deterministic_cache_(nullptr),
    current_function_(nullptr),
    current_compile_unit_(nullptr),

    obj_stack_(nullptr),
    exec_ctx_(nullptr),
    info_(nullptr),
    alloc_(nullptr),
    session_info_(nullptr),
    has_out_param_(false),
    package_id_(OB_INVALID_ID),
    cacheobj_guard_(PL_ROUTINE_HANDLE),
    phy_plan_ctx_(nullptr),
    allocator_("UDFRowAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    ctx_allocator_("UDFCtxAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    arg_count_(0),
    result_cache_max_result_(0),
    result_cache_max_size_(0),
    is_first_execute_(true),
    pl_execute_arg_() {}

  ~ObExprUDFCtx();

  int init(const ObExpr &expr, ObExecContext &exec_ctx);
  inline ParamStore* get_param_store() { return params_; }
  inline ObExecContext* get_exec_ctx() { return exec_ctx_; }
  inline int64_t get_param_count() { return OB_ISNULL(params_) ? 0 : params_->count(); }
  inline void set_obj_stack(ObObj *obj_stack) { obj_stack_ = obj_stack; }
  inline ObObj* get_obj_stack() { return obj_stack_; }
  inline bool has_out_param() { return has_out_param_; }
  inline uint64_t get_package_id() { return package_id_; }

  int get_result_from_cache(ObObj &result, bool &found);
  int add_result_to_cache(ObObj &result);

  inline ObSQLSessionInfo *get_session_info() { return session_info_; }
  inline ObExprUDFInfo *get_info() { return info_; }
  inline ObCacheObjGuard &get_cacheobj_guard() { return cacheobj_guard_; }
  inline sql::ObPhysicalPlanCtx *get_phy_plan_ctx() { return phy_plan_ctx_; }
  inline pl::ExecCtxBak &get_exec_ctx_bak() { return exec_ctx_bak_; }

  inline ObIAllocator &get_allocator() { return alloc_ != nullptr ? *alloc_ : allocator_; }
  inline int64_t get_arg_count() { return arg_count_; }

  int reuse(const ObExpr &expr);

  void reset_cacheobj_guard()
  {
    cacheobj_guard_.~ObCacheObjGuard();
    new (&cacheobj_guard_) ObCacheObjGuard(PL_ROUTINE_HANDLE);
  }

  bool is_first_execute() { return is_first_execute_; }

  pl::ObPLExecuteArg &get_pl_execute_arg() { return pl_execute_arg_; }

private:
  int check_types(const ObExpr &expr);
  int init_param_store(ObExecContext &exec_ctx, int param_num);
  int init_exec_ctx(ObExecContext &exec_ctx);
  int init_pl_alloc();
  int init_row_key(ObIAllocator &allocator, int64_t arg_cnt);
  int init_deterministic_cache();

  int calc_result_cache_enabled();
  int calc_current_function();
  int calc_cache_enabled();

  inline bool is_cache_enabled() { return enable_result_cache_ || enable_deterministic_cache_; }
  inline bool is_result_cache_enabled() { return enable_result_cache_; }
  inline bool is_deterministic_cache_enabled() { return enable_deterministic_cache_; }

  int generate_influence_string();
  int generate_influence_sys_var_string();
  int generate_influence_config_string();
  int construct_key();
  int get_result_from_result_cache(ObObj &result, bool &found);
  int construct_cache_ctx_for_get(pl::ObPLUDFResultCacheCtx &rc_ctx);
  int construct_cache_ctx_for_add(pl::ObPLUDFResultCacheCtx &rc_ctx);
  int generate_result_cache_object(ObObj &result,
                                   ObCacheObjGuard &cache_result_guard,
                                   pl::ObPLUDFResultCacheObject *&udf_result);
  int add_result_to_result_cache(ObObj &result);

private:
  ParamStore* params_;

  // for result cache
  pl::UDFArgRow row_key_;
  ObString sys_var_str_;
  ObString config_var_str_;
  bool enable_result_cache_;
  bool enable_deterministic_cache_;
  ObExprUDFDeterministerCache *deterministic_cache_;
  pl::ObPLFunction *current_function_;
  pl::ObPLCompileUnit *current_compile_unit_;

  // for rusable member
  ObObj* obj_stack_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  ObSqlCtx sql_ctx_;
  ObExecContext *exec_ctx_;
  ObExprUDFInfo *info_;
  ObIAllocator *alloc_;
  ObSQLSessionInfo *session_info_;
  bool has_out_param_;
  uint64_t package_id_;
  ObCacheObjGuard cacheobj_guard_;
  sql::ObPhysicalPlanCtx *phy_plan_ctx_;
  pl::ExecCtxBak exec_ctx_bak_;
  ObArenaAllocator allocator_; // row level allocator
  ObArenaAllocator ctx_allocator_; // ctx level allocator
  int64_t arg_count_;
  int64_t result_cache_max_result_;
  int64_t result_cache_max_size_;
  bool is_first_execute_;
  pl::ObPLExecuteArg pl_execute_arg_;
};

}
}

#endif
