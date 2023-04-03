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

#ifndef OCEANBASE_SQL_SPM_OB_SPM_CONTROLLER_H_
#define OCEANBASE_SQL_SPM_OB_SPM_CONTROLLER_H_

#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/spm/ob_spm_define.h"
#include "share/ob_rpc_struct.h"
#include "sql/monitor/ob_exec_stat.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;

class ObSpmController
{
public:
  static int check_baseline_enable(const ObPlanCacheCtx& pc_ctx,
                                   ObPhysicalPlan* plan,
                                   bool& need_capture);
  static int check_baseline_exists(ObPlanCacheCtx& pc_ctx,
                                   ObPhysicalPlan* plan,
                                   bool& is_exists);
  static void get_next_baseline_outline(ObSpmCacheCtx& spm_ctx);
  static int accept_new_plan_as_baseline(ObSpmCacheCtx& spm_ctx, const ObAuditRecordData &audit_record);

  static int update_evolution_task_result(EvolutionTaskResult& result);

  static int accept_plan_baseline_by_user(obrpc::ObModifyPlanBaselineArg& arg);
  static int cancel_evolve_task(obrpc::ObModifyPlanBaselineArg& arg);
  static int load_baseline(const obrpc::ObLoadPlanBaselineArg& arg, ObPhysicalPlan* plan);
  static int deny_new_plan_as_baseline(ObSpmCacheCtx& spm_ctx);
  static int64_t calc_spm_timeout_us(const int64_t normal_timeout, const int64_t baseline_exec_time);
};

} // namespace sql end
} // namespace ocenabase end

#endif
