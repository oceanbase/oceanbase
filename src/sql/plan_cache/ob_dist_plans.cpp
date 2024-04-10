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
#include "sql/plan_cache/ob_dist_plans.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_set.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_match_helper.h"
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {
int ObDistPlans::init(ObSqlPlanSet *ps)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    plan_set_ = ps;
  }
  return ret;
}

//对于复制表会在检查是否匹配的过程中修改副本index
int ObDistPlans::get_plan(ObPlanCacheCtx &pc_ctx,
                          ObPhysicalPlan *&plan)
{
  int ret = OB_SUCCESS;
  plan = NULL;
  bool is_matched = false;

  LOG_DEBUG("Get Plan", K(dist_plans_.count()));
  //need to clear all location info before calculate candi tablet locations
  //because get_phy_locations will build the related_tablet_map in ObDASCtx
  //and add candi table location into DASCtx
  DAS_CTX(pc_ctx.exec_ctx_).clear_all_location_info();
  if (OB_ISNULL(plan_set_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null plan_set_", K(ret));
  } else if ((plan_set_->is_multi_stmt_plan())) {
    // 如果是multi stmt计划，一定是单表分布式计划，可以直接根据table location算出物理分区地址
    // single table should just return plan, do not match
    if (0 == dist_plans_.count()) {
      ret = OB_SQL_PC_NOT_EXIST;
      LOG_DEBUG("dist plan list is empty", K(ret), K(dist_plans_.count()));
    } else if (OB_ISNULL(dist_plans_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null plan", K(ret), K(dist_plans_.at(0)));
    } else {
      dist_plans_.at(0)->set_dynamic_ref_handle(pc_ctx.handle_id_);
      plan = dist_plans_.at(0);
      is_matched = true;

      // fill table location for single plan using px
      // for single dist plan without px, we already fill the phy locations while calculating plan type
      // for multi table px plan, physical location is calculated in match step
      ObArray<ObCandiTableLoc> candi_table_locs;
      if (OB_ISNULL(plan_set_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null plan set", K(ret), K(plan_set_));
      } else if (!plan_set_->enable_inner_part_parallel()) {
        // do nothing
      } else if (OB_FAIL(ObPhyLocationGetter::get_phy_locations(plan->get_table_locations(),
                                                                pc_ctx,
                                                                candi_table_locs))) {
        LOG_WARN("failed to get physical table locations", K(ret));
      } else if (candi_table_locs.empty()) {
        // do nothing.
      } else if (OB_FAIL(ObPhyLocationGetter::build_candi_table_locs(pc_ctx.exec_ctx_.get_das_ctx(),
                                                                     plan->get_table_locations(),
                                                                     candi_table_locs))) {
        LOG_WARN("fail to init table locs", K(ret));
      }
    }
  }

  ObPlanMatchHelper helper(plan_set_);
  ObArray<ObCandiTableLoc> phy_tbl_infos;
  ObArray<ObTableLocation> out_tbl_locations;
  for (int64_t i = 0; OB_SUCC(ret) && !is_matched && i < dist_plans_.count();
       i++) {
    ObPhysicalPlan *tmp_plan = dist_plans_.at(i);
    phy_tbl_infos.reuse();
    out_tbl_locations.reuse();
    if (OB_ISNULL(tmp_plan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(tmp_plan));
    } else if (OB_FAIL(helper.match_plan(pc_ctx, tmp_plan, is_matched, phy_tbl_infos, out_tbl_locations))) {
      LOG_WARN("fail to match dist plan", K(ret));
    } else if (is_matched) {
      tmp_plan->set_dynamic_ref_handle(pc_ctx.handle_id_);
      plan = tmp_plan;
      if (OB_FAIL(ObPhyLocationGetter::build_table_locs(DAS_CTX(pc_ctx.exec_ctx_),
                                                        out_tbl_locations,
                                                        phy_tbl_infos))) {
        LOG_WARN("fail to init table locs", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && plan == NULL) {
    ret = OB_SQL_PC_NOT_EXIST;
  }

  return ret;
}

int ObDistPlans::add_evolution_plan(ObPhysicalPlan &plan, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  ObPlanMatchHelper helper(plan_set_);
  ObArray<ObCandiTableLoc> phy_tbl_infos;
  ObArray<ObTableLocation> out_tbl_locations;
  for (int64_t i = 0; OB_SUCC(ret) && !is_matched && i < dist_plans_.count(); i++) {
    const ObPhysicalPlan *tmp_plan = dist_plans_.at(i);
    phy_tbl_infos.reuse();
    out_tbl_locations.reuse();
    if (OB_ISNULL(tmp_plan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(tmp_plan));
    } else if (OB_FAIL(helper.match_plan(pc_ctx, tmp_plan, is_matched, phy_tbl_infos, out_tbl_locations))) {
      LOG_WARN("fail to match dist plan", K(ret));
    } else if (false == is_matched) {
      // do nothing
    } else {
      ret = OB_SQL_PC_PLAN_DUPLICATE;
    }
  }
  if (OB_SUCC(ret) && !is_matched) {
    if (OB_FAIL(dist_plans_.push_back(&plan))) {
      LOG_WARN("fail to add plan", K(ret));
    }
  }
  return ret;
}

int ObDistPlans::add_plan(ObPhysicalPlan &plan,
                          ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  bool is_matched = false;
  ObPlanMatchHelper helper(plan_set_);
  ObArray<ObCandiTableLoc> phy_tbl_infos;
  ObArray<ObTableLocation> out_tbl_locations;

  for (int64_t i = 0; OB_SUCC(ret) && !is_matched && i < dist_plans_.count(); i++) {
    //检查是否已有其他线程add该plan成功
    phy_tbl_infos.reuse();
    out_tbl_locations.reuse();
    const ObPhysicalPlan *tmp_plan = dist_plans_.at(i);
    if (OB_ISNULL(tmp_plan)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(tmp_plan));
    } else if (OB_FAIL(helper.match_plan(pc_ctx, tmp_plan, is_matched, phy_tbl_infos, out_tbl_locations))) {
      LOG_WARN("fail to match dist plan", K(ret));
    } else {
      is_matched = is_matched && tmp_plan->has_same_location_constraints(plan);
    }

    if (!is_matched) {
      // do nothing
    } else {
      ret = OB_SQL_PC_PLAN_DUPLICATE;
    }
  }

  if (OB_SUCC(ret) && !is_matched) {
    if (OB_FAIL(dist_plans_.push_back(&plan))) {
      LOG_WARN("fail to add plan", K(ret));
    }
  }

  return ret;
}

//使用plan的hash value判断是否为同一个plan
int ObDistPlans::is_same_plan(const ObPhysicalPlan *l_plan,
                              const ObPhysicalPlan *r_plan,
                              bool &is_same) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_plan) || OB_ISNULL(r_plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_plan), K(r_plan));
  } else {
    is_same = (l_plan->get_signature() == r_plan->get_signature());
    LOG_DEBUG("compare plan", K(l_plan->get_signature()), K(r_plan->get_signature()), K(is_same));
  }
  return ret;
}

//删除所有plan及对应plan stat
int ObDistPlans::remove_all_plan()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  for (int64_t i = 0; i < dist_plans_.count(); i++) {
    if (OB_ISNULL(dist_plans_.at(i))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get un expected null in dist_plans",
               K(tmp_ret), K(dist_plans_.at(i)));
    } else {
      dist_plans_.at(i) = NULL;
    }
  }

  dist_plans_.reset();
  ret = tmp_ret;

  return ret;
}

//获取所有plan使用内存
int64_t ObDistPlans::get_mem_size() const
{
  int64_t plan_set_mem = 0;
  for (int64_t i = 0; i < dist_plans_.count(); i++) {
    if (OB_ISNULL(dist_plans_.at(i))) {
      BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "null physical plan");
    } else {
      plan_set_mem += dist_plans_.at(i)->get_mem_size();
    }
  }
  return plan_set_mem;
}

int ObDistPlans::remove_plan_stat()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPlanCache *pc = NULL;
  if (OB_ISNULL(plan_set_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan_set_));
  } else if (OB_ISNULL(pc = plan_set_->get_plan_cache())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pc));
  } else {
    uint64_t plan_id = OB_INVALID_ID;
    for (int64_t i = 0; i < dist_plans_.count(); i++) {
      if (OB_ISNULL(dist_plans_.at(i))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN_RET(tmp_ret, "get an unexpected null", K(tmp_ret), K(dist_plans_.at(i)));
      } else if (FALSE_IT(plan_id = dist_plans_.at(i)->get_plan_id())) {
      } else if (OB_SUCCESS !=
                 (tmp_ret = plan_set_->remove_cache_obj_entry(plan_id))) {
        LOG_WARN_RET(tmp_ret, "failed to remove plan stat",
                 K(tmp_ret), K(plan_id), K(i));
      } else {
        /* do nothing */
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      dist_plans_.reuse();
    }
  }
  ret = tmp_ret;
  return ret;
}

} // namespace sql
} // namespace oceanbase
