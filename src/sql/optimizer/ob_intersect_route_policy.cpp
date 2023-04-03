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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_intersect_route_policy.h"
#include "sql/optimizer/ob_replica_compare.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace sql
{
int ObIntersectRoutePolicy::init_candidate_replicas(const ObList<common::ObAddr, ObArenaAllocator> &candidate_server_list,
                                                    common::ObIArray<CandidateReplica> &candi_replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  auto server_iter = candidate_server_list.begin();
  CandidateReplica candi_replica;
  for (; OB_SUCC(ret) && server_iter != candidate_server_list.end(); ++server_iter) {
    candi_replica.set_replica_type(REPLICA_TYPE_MAX);//fixme(hanting) : ignore replica type for mv
    candi_replica.set_server(*server_iter);
    if (OB_FAIL(init_candidate_replica(server_locality_array_, candi_replica))) {
      LOG_WARN("fail to init candidate replica", K(server_locality_array_), K(candi_replica), K(ret));
    } else if (OB_FAIL(candi_replicas.push_back(candi_replica))) {
      LOG_WARN("fail to push back candi_replica", K(candi_replica), K(candi_replicas), K(ret));
    }
  }
  return ret;
}
int ObIntersectRoutePolicy::select_replica_with_priority(const ObRoutePolicyCtx &route_policy_ctx,
                                                         const common::ObIArray<CandidateReplica> &replica_array,
                                                         common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list)
{
  int ret = OB_SUCCESS;
  bool has_found = false;
  ObAddr selected_addr;
  for (int64_t i = 0; OB_SUCC(ret) && !has_found && i < replica_array.count(); ++i) {
    if (replica_array.at(i).is_usable()) {
      selected_addr = replica_array.at(i).get_server();
      has_found = true;
    }
  }

  if (OB_UNLIKELY(false == has_found)) {
    int64_t select_idx = rand() % replica_array.count();
    selected_addr = replica_array.at(select_idx).get_server();
    LOG_WARN("all replica is not usable currently", K(replica_array), K(route_policy_ctx), K(select_idx));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogPlan::select_one_server(selected_addr, phy_tbl_loc_info_list))) {
    LOG_WARN("fail to select one server", K(ret));
  }
  return ret;
}

}//sql
}//oceanbase
