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
#include "sql/optimizer/ob_route_policy.h"
#include "sql/optimizer/ob_replica_compare.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/optimizer/ob_log_plan.h"
#include "storage/ob_locality_manager.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
namespace oceanbase
{
namespace sql
{

int ObRoutePolicy::CandidateReplica::assign(CandidateReplica &other)
{
  int ret = common::OB_SUCCESS;
  OZ(ObLSReplicaLocation::assign(other));
  if (OB_SUCC(ret)) {
    attr_ = other.attr_;
    is_filter_ = other.is_filter_;
    replica_idx_ = other.replica_idx_;
  }

  return ret;
}

int ObRoutePolicy::weak_sort_replicas(ObIArray<CandidateReplica>& candi_replicas, ObRoutePolicyCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (candi_replicas.count() > 1) {
    auto first = &candi_replicas.at(0);
    ObRoutePolicyType policy_type = get_calc_route_policy_type(ctx);
    ObReplicaCompare replica_cmp(policy_type);
    std::sort(first, first + candi_replicas.count(), replica_cmp);
    if (OB_FAIL(replica_cmp.get_sort_ret())) {
      LOG_WARN("fail sort", K(candi_replicas), K(ret));
    }
  }
  return ret;
}

int ObRoutePolicy::strong_sort_replicas(ObIArray<CandidateReplica>& candi_replicas, ObRoutePolicyCtx &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(candi_replicas);
  UNUSED(ctx);
  return ret;
}

int ObRoutePolicy::filter_replica(const ObAddr &local_server,
                                  const ObLSID &ls_id,
                                  ObIArray<CandidateReplica>& candi_replicas,
                                  ObRoutePolicyCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObRoutePolicyType policy_type = get_calc_route_policy_type(ctx);
  bool need_break = false;
  for (int64_t i = 0; !need_break && OB_SUCC(ret) && i < candi_replicas.count(); ++i) {
    CandidateReplica &cur_replica = candi_replicas.at(i);
    bool can_read = true;
    bool is_local = cur_replica.get_server() == local_server;

    if (is_local && OB_FAIL(ObSqlTransControl::check_ls_readable(ctx.tenant_id_,
                                                     ls_id,
                                                     cur_replica.get_server(),
                                                     ctx.max_read_stale_time_,
                                                     can_read))) {
      LOG_WARN("fail to check ls readable", K(ctx), K(cur_replica), K(ret));
    } else {
      LOG_TRACE("check ls readable", K(ctx), K(ls_id), K(cur_replica.get_server()), K(can_read));
      if ((policy_type == ONLY_READONLY_ZONE && cur_replica.attr_.zone_type_ == ZONE_TYPE_READWRITE)
          || cur_replica.attr_.zone_status_ == ObZoneStatus::INACTIVE
          || cur_replica.attr_.server_status_ != ObServerStatus::OB_SERVER_ACTIVE
          || cur_replica.attr_.start_service_time_ == 0
          || cur_replica.attr_.server_stop_time_ != 0
          || (0 == cur_replica.get_property().get_memstore_percent()
              && is_follower(cur_replica.get_role()))// 作为Follower的D副不能选择
          || !can_read) {
        cur_replica.is_filter_ = true;
      }

      // if is local replica and can read, filter all replicas and only select this replica.
      if (is_local && !cur_replica.is_filter_) {
        for (int64_t j = 0; j < candi_replicas.count(); ++j) {
          candi_replicas.at(i).is_filter_ = true;
        }
        cur_replica.is_filter_ = false;
        need_break = true;
      }
    }
  }
  return ret;
}

int ObRoutePolicy::calculate_replica_priority(const ObAddr &local_server,
                                              const ObLSID &ls_id,
                                              ObIArray<CandidateReplica>& candi_replicas,
                                              ObRoutePolicyCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (candi_replicas.count() <= 1) {//do nothing
  } else if (WEAK == ctx.consistency_level_) {
    if (OB_FAIL(filter_replica(local_server, ls_id, candi_replicas, ctx))) {
      LOG_WARN("fail to filter replicas", K(candi_replicas), K(ctx), K(ret));
    } else if (OB_FAIL(weak_sort_replicas(candi_replicas, ctx))) {
      LOG_WARN("fail to sort replicas", K(candi_replicas), K(ctx), K(ret));
    }
  } else if (STRONG == ctx.consistency_level_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("strong consistency cant't be here", K(ctx), K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected consistency level_", K(ctx));
  }
  return ret;
}

//local locality没有初始化情况无法判断pos type,因此统一设置为other
int ObRoutePolicy::calc_position_type(const ObServerLocality &candi_locality,
                                      CandidateReplica &candi_replica)
{
  int ret = OB_SUCCESS;
  if (local_addr_ == candi_replica.get_server()) {
    candi_replica.attr_.pos_type_ = SAME_SERVER;
  } else if (OB_UNLIKELY(false == local_locality_.is_init())) {
    candi_replica.attr_.pos_type_ = OTHER_REGION;
  } else if (OB_UNLIKELY(false == candi_locality.is_init())) {
    candi_replica.attr_.pos_type_ = OTHER_REGION;
  } else if (is_same_idc(local_locality_, candi_locality)) {
    candi_replica.attr_.pos_type_ = SAME_IDC;
  } else if (is_same_region(local_locality_, candi_locality)) {
    candi_replica.attr_.pos_type_ = SAME_REGION;
  } else {
    candi_replica.attr_.pos_type_ = OTHER_REGION;
  }
  return ret;
}

int ObRoutePolicy::init_candidate_replica(const ObIArray<share::ObServerLocality> &server_locality_array,
                                          CandidateReplica &candi_replica)
{
  int ret = OB_SUCCESS;
  ObServerLocality candi_locality;
  if (OB_FAIL(get_server_locality(candi_replica.get_server(), server_locality_array, candi_locality))) {
    LOG_WARN("fail to get server locality", K(server_locality_array), K(candi_locality), K(ret));
  } else if (OB_UNLIKELY(false == candi_locality.is_init())) {
    //if can't get candi_replica locality, mark it's filter.
    candi_replica.is_filter_ = true;
  } else if (OB_FAIL(get_merge_status(candi_locality, candi_replica))) {
    LOG_WARN("fail to get merge status", K(candi_locality), K(candi_replica), K(ret));
  } else if (OB_FAIL(get_zone_status(candi_locality, candi_replica))) {
    LOG_WARN("fail to get zone status", K(candi_locality), K(candi_replica), K(ret));
  } else if (OB_FAIL(calc_position_type(candi_locality, candi_replica))) {
    LOG_WARN("fail to calc postion type", K(server_locality_array), K(ret));
  } else {
    candi_replica.attr_.zone_type_ = candi_locality.get_zone_type();
    candi_replica.attr_.start_service_time_ = candi_locality.get_start_service_time();
    candi_replica.attr_.server_stop_time_ = candi_locality.get_server_stop_time();
    candi_replica.attr_.server_status_ = candi_locality.get_server_status();
  }
  return ret;
}

int ObRoutePolicy::get_merge_status(const ObServerLocality &candi_locality,
                                    CandidateReplica &candi_replica)
{
  int ret = OB_SUCCESS;
  // from ob4.0, server do not have merge status
  UNUSED(candi_locality);
  candi_replica.attr_.merge_status_ = NOMERGING;
  return ret;
}

int ObRoutePolicy::get_zone_status(const ObServerLocality &candi_locality,
                                   CandidateReplica &candi_replica)
{
  int ret = OB_SUCCESS;
  if (candi_locality.is_active()) {
    candi_replica.attr_.zone_status_ = ObZoneStatus::ACTIVE;
  } else {
    candi_replica.attr_.zone_status_ = ObZoneStatus::INACTIVE;
  }
  return ret;
}

int ObRoutePolicy::get_server_locality(const ObAddr &addr,
                                       const ObIArray<share::ObServerLocality> &server_locality_array,
                                       share::ObServerLocality &svr_locality)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for(int64_t i = 0; OB_SUCC(ret) && !is_found && i < server_locality_array.count(); ++i) {
    const ObServerLocality &cur_locality = server_locality_array.at(i);
    if (addr == cur_locality.get_addr()) {
      if (OB_FAIL(svr_locality.assign(cur_locality))) {
        LOG_WARN("fail to assign locality", K(addr), K(cur_locality), K(server_locality_array), K(ret));
      } else {
        is_found = true;
      }
    }
  }
  if (!is_found) {
    //此处不报错。当locality找不到时，后面无法正确设置pos_type，每个replica均会变为other region
    //这种情况相当于随机选择一个replica, weak读时应该尽可能保证可执行
    LOG_WARN("not found locality", K(addr), K(server_locality_array), K(ret));
  }
  return ret;
}

int ObRoutePolicy::init_candidate_replicas(common::ObIArray<CandidateReplica> &candi_replicas)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  for (int64_t i = 0 ; OB_SUCC(ret) && i < candi_replicas.count(); ++i) {
    if (OB_FAIL(init_candidate_replica(server_locality_array_, candi_replicas.at(i)))) {
      LOG_WARN("fail to candidate replica", K(i), K(server_locality_array_), K(candi_replicas), K(ret));
    }
  }
  return ret;
}

int ObRoutePolicy::select_replica_with_priority(const ObRoutePolicyCtx &route_policy_ctx,
                                                const ObIArray<ObRoutePolicy::CandidateReplica> &replica_array,
                                                ObCandiTabletLoc &phy_part_loc_info)
{
  int ret = OB_SUCCESS;
  bool has_found = false;
  bool same_priority = true;
  ReplicaAttribute priority_attr;
  for (int64_t i = 0; OB_SUCC(ret) && same_priority && i < replica_array.count(); ++i) {
    if (replica_array.at(i).is_usable()/*+满足max_read_stale_time事务延迟*/) {
      if (has_found) {
        if (priority_attr == replica_array.at(i).attr_) {
          if (OB_FAIL(phy_part_loc_info.add_priority_replica_idx(i))) {
            LOG_WARN("fail to select replica", K(i), K(priority_attr), K(ret));
          }
        } else {
          same_priority = false;
        }
      } else {
        if (OB_FAIL(phy_part_loc_info.add_priority_replica_idx(i))) {
          LOG_WARN("fail to select replica", K(i), K(ret));
        } else {
          has_found = true;
          priority_attr = replica_array.at(i).attr_;
        }
      }
    }
  }

  //极端情况下，replica_arry中的内容均变为不可读，则随机选一个
  if (OB_UNLIKELY(false == has_found)) {
    int64_t select_idx = rand() % replica_array.count();
    if (OB_FAIL(phy_part_loc_info.add_priority_replica_idx(select_idx))) {
      LOG_WARN("fail to select replica", K(select_idx), K(ret));
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {//10s打印一次
      LOG_WARN("all replica is not usable currently", K(replica_array), K(route_policy_ctx), K(select_idx));
    }
  }
  return ret;
}

int ObRoutePolicy::calc_intersect_repllica(const common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                           ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> &intersect_server_list)
{
  int ret = OB_SUCCESS;
  ObRoutePolicy::CandidateReplica tmp_replica;
  bool can_select_one_server = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_select_one_server && i < phy_tbl_loc_info_list.count(); ++i) {
    const ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
    if (OB_ISNULL(phy_tbl_loc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
    } else {
      const ObCandiTabletLocIArray &phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list();
      for (int64_t j = 0; OB_SUCC(ret) && can_select_one_server && j < phy_part_loc_info_list.count(); ++j) {
        const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
        const ObIArray<int64_t> &priority_replica_idxs = phy_part_loc_info.get_priority_replica_idxs();
        if (0 == i && 0 == j) { // 第一个partition
          if (phy_part_loc_info.has_selected_replica()) { // 已经选定了副本
            tmp_replica.reset();
            if (OB_FAIL(phy_part_loc_info.get_selected_replica(tmp_replica))) {
              LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
            } else if (OB_FAIL(intersect_server_list.push_back(tmp_replica))) {
              LOG_WARN("fail to push back candidate server", K(ret), K(tmp_replica));
            }
          } else { // 还没选定副本
            for (int64_t k = 0; OB_SUCC(ret) && k < priority_replica_idxs.count(); ++k) {
              tmp_replica.reset();
              int64_t replica_idx = priority_replica_idxs.at(k);
              if (OB_FAIL(phy_part_loc_info.get_priority_replica(replica_idx, tmp_replica))) {
                LOG_WARN("fail to get priority replica", K(k), K(priority_replica_idxs), K(ret));
              } else if (OB_FAIL(intersect_server_list.push_back(tmp_replica))) {
                LOG_WARN("fail to push back server ", K(k), K(priority_replica_idxs), K(ret));
              }
            }
          }
        } else {// 不是第一个partition
          ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator>::iterator intersect_server_list_iter = intersect_server_list.begin();
          for (; OB_SUCC(ret) && intersect_server_list_iter != intersect_server_list.end(); intersect_server_list_iter++) {
            const ObAddr &candidate_server = intersect_server_list_iter->get_server();
            bool has_replica = false;
            if (phy_part_loc_info.has_selected_replica()) { // 已经选定了副本
              tmp_replica.reset();
              if (OB_FAIL(phy_part_loc_info.get_selected_replica(tmp_replica))) {
                LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
              } else if (tmp_replica.get_server() == candidate_server) {
                has_replica = true;
              }
            } else { // 还没选定副本
              for (int64_t k = 0; OB_SUCC(ret) && !has_replica && k < priority_replica_idxs.count(); ++k) {
                tmp_replica.reset();
                int64_t replica_idx = priority_replica_idxs.at(k);
                if (OB_FAIL(phy_part_loc_info.get_priority_replica(replica_idx, tmp_replica))) {
                  LOG_WARN("fail to get priority replica", K(k), K(priority_replica_idxs), K(ret));
                } else if (candidate_server == tmp_replica.get_server()) {
                  has_replica = true;
                }
              }
            }
            if (OB_SUCC(ret) && !has_replica) {
              if (OB_FAIL(intersect_server_list.erase(intersect_server_list_iter))) {
                LOG_WARN("fail to erase from list", K(ret), K(candidate_server));
              }
            }
          }
          if (OB_SUCC(ret) && intersect_server_list.empty()) {
            can_select_one_server = false;
          }
        }
      }
    }
  }
  return ret;
}

int ObRoutePolicy::select_intersect_replica(ObRoutePolicyCtx &route_policy_ctx,
                                            common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                                            ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> &intersect_server_list,
                                            bool &is_proxy_hit)
{
  UNUSED(route_policy_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_intersect_repllica(phy_tbl_loc_info_list, intersect_server_list))) {
    LOG_WARN("fail to calc intersect replica", K(phy_tbl_loc_info_list), K(ret));
  } else if (intersect_server_list.empty()) {//没有交集的情况，每个partition单独选择replica
    is_proxy_hit = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_tbl_loc_info_list.count(); ++i) {
      ObCandiTableLoc *phy_tbl_loc_info = phy_tbl_loc_info_list.at(i);
      if (OB_ISNULL(phy_tbl_loc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("phy_tbl_loc_info is NULL", K(ret), K(i), K(phy_tbl_loc_info_list.count()));
      } else {
        ObCandiTabletLocIArray &phy_part_loc_info_list = phy_tbl_loc_info->get_phy_part_loc_info_list_for_update();
        for (int64_t j = 0; OB_SUCC(ret) && j < phy_part_loc_info_list.count(); ++j) {
          ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(j);
          if (phy_part_loc_info.has_selected_replica()) {
            // do nothing
          } else if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx_with_priority())) {
            LOG_WARN("fail to set selected replica idx", K(ret));
          }
        }
      }
    }
  } else {
    CandidateReplica selected_replica;
    ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator>::iterator replica_iter = intersect_server_list.begin();
    ObSEArray<ObAddr, 16> same_priority_servers;
    selected_replica.attr_.pos_type_ = POSITION_TYPE_MAX;
    bool is_first = true;
    for (; OB_SUCC(ret) && replica_iter != intersect_server_list.end(); replica_iter++) {
      if (is_first) {
        selected_replica = *replica_iter;
        is_first = false;
        if (OB_FAIL(same_priority_servers.push_back(replica_iter->get_server()))) {
          LOG_WARN("fail to replica iterator", K(replica_iter), K(ret));
        }
      } else if (replica_iter->attr_.pos_type_ == selected_replica.attr_.pos_type_) {
        //将same priority server统计起来，用于后面随机选择
        if (OB_FAIL(same_priority_servers.push_back(replica_iter->get_server()))) {
          LOG_WARN("fail to replica iterator", K(replica_iter), K(ret));
        }
      } else if (replica_iter->attr_.pos_type_ < selected_replica.attr_.pos_type_) {
        selected_replica = *replica_iter;
        same_priority_servers.reset();
        if (OB_FAIL(same_priority_servers.push_back(replica_iter->get_server()))) {
          LOG_WARN("fail to replica iterator", K(replica_iter), K(ret));
        }
      } else {
        /* replica_iter->attr_.pos_type_ > selected_replica.attr_.pos_type_ do nothing */
      }
    }

    if (OB_SUCC(ret)) {//select server for all partitions of the query
      int64_t selected_idx = rand() % same_priority_servers.count();
      const ObAddr &selected_server = same_priority_servers.at(selected_idx);
      if (OB_FAIL(ObLogPlan::select_one_server(selected_server, phy_tbl_loc_info_list))) {
        LOG_WARN("fail to select one server", K(selected_idx), K(selected_server), K(ret));
      } else {
        is_proxy_hit = (local_addr_ == selected_server);
      }
    }
  }
  return ret;
}

int ObRoutePolicy::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(GCTX.locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("locality manager is null", K(ret), KP(GCTX.locality_manager_));
  } else if (OB_FAIL(GCTX.locality_manager_->get_server_locality_array(server_locality_array_, has_readonly_zone_))) {
    LOG_WARN("fail to get server locality", K(ret));
  } else if (OB_FAIL(get_server_locality(local_addr_, server_locality_array_, local_locality_))) {
    LOG_WARN("fail to get local locality", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObRoutePolicy::is_same_idc(const share::ObServerLocality &locality1, const share::ObServerLocality &locality2)
{
  bool ret_bool = false;
  if (locality1.get_region().is_empty() || locality2.get_region().is_empty()) {
    //如果没有为集群设置REGION，则无法判断是否在同一REGION
    ret_bool = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "cluster region is not set", K(locality1), K(locality2));
  } else if (locality1.get_idc().is_empty() || locality2.get_idc().is_empty()) {
    //如果没有为zone设置IDC，则无法判断是否在同一IDC
    ret_bool = false;
    LOG_TRACE("zone idc is not set", K(locality1), K(locality2));
  } else if (locality1.get_region() == locality2.get_region()) {
    //先判断region是否相同，避免不同region中有相同name的idc
    if (locality1.get_idc() == locality2.get_idc()) {
      ret_bool = true;
    }
  }
  return ret_bool;
}

bool ObRoutePolicy::is_same_region(const share::ObServerLocality &locality1, const share::ObServerLocality &locality2)
{
  bool ret_bool = false;
  if (locality1.get_region().is_empty() || locality2.get_region().is_empty()) {
    //如果没有为集群设置REGION，则无法判断是否在同一REGION
    ret_bool = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "cluster region is not set", K(locality1), K(locality2));
  } else if (locality1.get_region() == locality2.get_region()) {
    ret_bool = true;
  }
  return ret_bool;
}

}//sql
}//oceanbase
