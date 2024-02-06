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

#define USING_LOG_PREFIX SQL_CG

#include "sql/code_generator/ob_code_generator.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/optimizer/ob_log_plan.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace sql
{

int ObCodeGenerator::generate(const ObLogPlan &log_plan,
                              ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = 0;
  const uint64_t cur_cluster_version = CLUSTER_CURRENT_VERSION;
  OZ(detect_batch_size(log_plan, batch_size));
  if (OB_SUCC(ret) && batch_size > 0) {
    log_plan.get_optimizer_context().set_batch_size(batch_size);
    phy_plan.set_batch_size(batch_size);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_exprs(log_plan, phy_plan, cur_cluster_version))) {
    LOG_WARN("fail to get all raw exprs", K(ret));
  } else if (OB_FAIL(generate_operators(log_plan, phy_plan, cur_cluster_version))) {
    LOG_WARN("fail to generate plan", K(ret));
  }

  return ret;
}

//1. 生成老的执行计划, 用于初始化所有表达式operator
//   并初始化到ObExpr的op_中, 供新老表达式混跑使用, 后续不需要混跑会去掉
//2. 获取执行期需要使用到的所有表达式
//3. 生成所有物理表达式
int ObCodeGenerator::generate_exprs(const ObLogPlan &log_plan,
                                    ObPhysicalPlan &phy_plan,
                                    const uint64_t cur_cluster_version)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  CK(NULL != exec_ctx && NULL != exec_ctx->get_physical_plan_ctx());
  if (OB_SUCC(ret)) {
    ObStaticEngineExprCG expr_cg(
        phy_plan.get_allocator(),
        log_plan.get_optimizer_context().get_session_info(),
        exec_ctx->get_sql_ctx()->schema_guard_,
        exec_ctx->get_physical_plan_ctx()->get_original_param_cnt(),
        param_store_->count(),
        min_cluster_version_);
    // init ctx for operator cg
    expr_cg.set_batch_size(phy_plan.get_batch_size());
    if (OB_FAIL(expr_cg.generate(log_plan.get_optimizer_context().get_all_exprs(),
                                 phy_plan.get_expr_frame_info()))) {
      LOG_WARN("fail to generate expr", K(ret));
    } else {
      phy_plan.get_next_expr_id() = phy_plan.get_expr_frame_info().need_ctx_cnt_;
    }
  }

  return ret;
}

int ObCodeGenerator::generate_operators(const ObLogPlan &log_plan,
                                        ObPhysicalPlan &phy_plan,
                                        const uint64_t cur_cluster_version)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG static_engin_cg(min_cluster_version_);
  if (OB_FAIL(static_engin_cg.generate(log_plan, phy_plan))) {
    LOG_WARN("fail to code generate", K(ret));
  }
  return ret;
}

int ObCodeGenerator::detect_batch_size(
    const ObLogPlan &log_plan, int64_t &batch_size)
{
  int ret = OB_SUCCESS;
  bool vectorize = false;
  bool stop_checking = false;
  batch_size = 0;
  ObBasicSessionInfo *session =
      log_plan.get_optimizer_context().get_session_info();
  ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  CK(NULL != exec_ctx && NULL != exec_ctx->get_physical_plan_ctx());
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(session)) {
    // empty session disable batch processing
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
    double scan_cardinality = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    // TODO bin.lb: move to optimizer and more sophisticated rules
    bool rowsets_enabled = tenant_config.is_valid() && tenant_config->_rowsets_enabled;
    const ObOptParamHint *opt_params = &log_plan.get_stmt()->get_query_ctx()->get_global_hint().opt_params_;
    if (OB_FAIL(opt_params->get_bool_opt_param(ObOptParamHint::ROWSETS_ENABLED, rowsets_enabled))) {
      LOG_WARN("fail to check rowsets enabled", K(ret));
    } else if (rowsets_enabled) {
      // TODO bin.lb; check all sub plans
      OZ(ObStaticEngineCG::check_vectorize_supported(vectorize,
                                                     stop_checking,
                                                     scan_cardinality,
                                                     log_plan.get_plan_root()));
    }
    if (OB_SUCC(ret) && vectorize) {
      ObArenaAllocator alloc;
      ObRawExprUniqueSet flattened_exprs(true);
      OZ(flattened_exprs.flatten_and_add_raw_exprs(log_plan.get_optimizer_context()
                                                           .get_all_exprs()));
      ObStaticEngineExprCG expr_cg(
          alloc,
          log_plan.get_optimizer_context().get_session_info(),
          exec_ctx->get_sql_ctx()->schema_guard_,
          exec_ctx->get_physical_plan_ctx()->get_original_param_cnt(),
          0,
          exec_ctx->get_min_cluster_version());
      int64_t rowsets_max_rows = tenant_config->_rowsets_max_rows;
      OZ(expr_cg.detect_batch_size(flattened_exprs, batch_size,
                                   rowsets_max_rows,
                                   tenant_config->_rowsets_target_maxsize,
                                   scan_cardinality));
      // overwrite batch size if hint is specified
      OZ(opt_params->get_integer_opt_param(ObOptParamHint::ROWSETS_MAX_ROWS, batch_size));
    }
    // TODO qubin.qb: remove the tracelog when rowsets/batch_size is displayed
    // in plan
    LOG_TRACE("detect_batch_size", K(vectorize), K(scan_cardinality), K(batch_size));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
