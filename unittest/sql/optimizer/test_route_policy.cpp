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
#include <gtest/gtest.h>
#include <assert.h>
#define protected public
#include "sql/optimizer/ob_route_policy.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#undef private
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_list.h"
#include "common/ob_zone_type.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
namespace test
{
class ObRoutePolicyTest:public ::testing::Test
{
public:
  ObRoutePolicyTest()
      :addr_(),
       route_policy_(addr_),
       route_policy_ctx_()
  {
    route_policy_.has_readonly_zone_ = true;
    route_policy_.is_inited_ = true;
  }
  int add_candi_replica(ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas,
                        ObRoutePolicy::PositionType pos_type,
                        ObRoutePolicy::MergeStatus merge_status,
                        ObZoneType zone_type,
                        oceanbase::share::ObZoneStatus::Status zone_status);
  int add_candi_replica(ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas,
                        ObAddr addr,
                        ObRoutePolicy::PositionType pos_type,
                        ObRoutePolicy::MergeStatus merge_status,
                        ObZoneType zone_type,
                        oceanbase::share::ObZoneStatus::Status zone_status);
public:
  ObAddr addr_;
  ObRoutePolicy route_policy_;
  ObRoutePolicyCtx route_policy_ctx_;
  static int64_t replica_idx_;
};

int64_t ObRoutePolicyTest::replica_idx_ = 0;
int ObRoutePolicyTest::add_candi_replica(ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas,
                                         ObRoutePolicy::PositionType pos_type,
                                         ObRoutePolicy::MergeStatus merge_status,
                                         ObZoneType zone_type,
                                         ObZoneStatus::Status zone_status)
{
  return add_candi_replica(candi_replicas, ObAddr(ObAddr::IPV4, "0.0.0.0", 0), pos_type, merge_status, zone_type, zone_status);
}

int ObRoutePolicyTest::add_candi_replica(ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas,
                                         ObAddr addr,
                                         ObRoutePolicy::PositionType pos_type,
                                         ObRoutePolicy::MergeStatus merge_status,
                                         ObZoneType zone_type,
                                         oceanbase::share::ObZoneStatus::Status zone_status)
{
  int ret = OB_SUCCESS;
  ObRoutePolicy::CandidateReplica candi_replica;
  candi_replica.server_ = addr;
  candi_replica.attr_.pos_type_ = pos_type;
  candi_replica.attr_.merge_status_ = merge_status;
  candi_replica.attr_.zone_status_ = zone_status;
  candi_replica.attr_.zone_type_ = zone_type;
  candi_replica.replica_idx_ = replica_idx_++;
  candi_replica.attr_.start_service_time_ = 1;
  candi_replica.attr_.server_status_ = ObServerStatus::OB_SERVER_ACTIVE;
  if (OB_FAIL(candi_replicas.push_back(candi_replica))) {
    LOG_WARN("fail to push back candi", K(candi_replica), K(ret));
  }
  return ret;
}

TEST_F(ObRoutePolicyTest, READONLY_ZONE_FIRST)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  ObArray<ObRoutePolicy::CandidateReplica> candi_replicas;
  replica_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  LOG_INFO("READONLY_ZONE_FIRST sort result", K(candi_replicas));
  for (int64_t i = 0; i < candi_replicas.count(); ++i) {
    ASSERT_EQ(candi_replicas.count() - i -1, candi_replicas.at(i).replica_idx_);
  }
}

TEST_F(ObRoutePolicyTest, UNMERGE_ZONE_FIRST)
{
  route_policy_ctx_.policy_type_ = UNMERGE_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  ObArray<ObRoutePolicy::CandidateReplica> candi_replicas;
  replica_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  LOG_INFO("UNMERGE_ZONE_FIRST sort result", K(candi_replicas));
  for (int64_t i = 0; i < candi_replicas.count(); ++i) {
    ASSERT_EQ(candi_replicas.count() - i -1, candi_replicas.at(i).replica_idx_);
  }
}

TEST_F(ObRoutePolicyTest, ONLY_READONLY_ZONE)
{
  route_policy_ctx_.policy_type_ = ONLY_READONLY_ZONE;
  route_policy_ctx_.consistency_level_ = WEAK;
  ObArray<ObRoutePolicy::CandidateReplica> candi_replicas;
  replica_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::OTHER_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  //  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));

  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  LOG_INFO("ONLY_READONLY_ZONE sort result", K(candi_replicas));
  for (int64_t i = 0; i < candi_replicas.count(); ++i) {
    ASSERT_EQ(candi_replicas.count() - i -1, candi_replicas.at(i).replica_idx_);
    if (candi_replicas.at(i).attr_.zone_type_ == ZONE_TYPE_READWRITE) {
      ASSERT_EQ(false, candi_replicas.at(i).is_usable());
    }
  }
}

TEST_F(ObRoutePolicyTest, SELECT_INTERSECT_SAME_IDC)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(2, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.3", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(2, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(1, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));
  ASSERT_EQ(1, candi_replica1.replica_idx_);
  ASSERT_EQ(0, candi_replica2.replica_idx_);
}

TEST_F(ObRoutePolicyTest, SELECT_INTERSECT_SAME_REGION)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(1, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.3", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(1, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(0, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));
  ASSERT_EQ(0, candi_replica1.replica_idx_);
  ASSERT_EQ(1, candi_replica2.replica_idx_);
}

TEST_F(ObRoutePolicyTest, SELECT_NO_INTERSECT)
{
  ObCandiTabletLoc phy_part_loc_info;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas = phy_part_loc_info.get_partition_location().get_replica_locations();

  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  replica_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::MERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas, ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READONLY, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas, phy_part_loc_info));

  LOG_INFO("SELECT NO INTERSET", K(candi_replicas));
  //assert result
  ASSERT_EQ(2, phy_part_loc_info.get_priority_replica_idxs().count());

  int64_t priority_idx = phy_part_loc_info.get_priority_replica_idxs().at(0);
  ASSERT_EQ(true, priority_idx == 0 || priority_idx == 1);
  priority_idx = phy_part_loc_info.get_priority_replica_idxs().at(1);
  ASSERT_EQ(true, priority_idx == 0 || priority_idx == 1);

  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info.set_selected_replica_idx_with_priority());
  ObRoutePolicy::CandidateReplica selected_replica;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info.get_selected_replica(selected_replica));
  ASSERT_EQ(5, selected_replica.replica_idx_);

}

int init_server_locality(ObServerLocality &server_locality,
                         int64_t start_time,
                        int64_t stop_time,
                        const char *svr_status,
                        const char *svr_ip,
                        const int32_t svr_port,
                        ObZone zone,
                        ObZoneType zone_type,
                        ObIDC idc,
                        ObRegion region,
                        bool is_active)
{
  int ret = OB_SUCCESS;
  server_locality.set_start_service_time(start_time);
  server_locality.set_server_stop_time(stop_time);
  server_locality.set_server_status(svr_status);
  if (OB_FAIL(server_locality.init(svr_ip, svr_port, zone, zone_type, idc, region, is_active))) {
    LOG_WARN("fail to init server locality", K(ret));
  }
  return ret;
}

TEST_F(ObRoutePolicyTest, CANDI_PRELICA_EMPTY)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;

  ObCandiTabletLoc phy_part_loc_info;
  ObRoutePolicy::CandidateReplica candi_replica1, candi_replica2;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas = phy_part_loc_info.get_partition_location().get_replica_locations();

  //init candidate_replica;
  candi_replica1.server_ = ObAddr(ObAddr::IPV4, "127.0.0.2", 12345);
  candi_replica2.server_ = ObAddr(ObAddr::IPV4, "127.0.0.3", 12345);
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica2));
  replica_idx_ = 0;

  //init locality
  ASSERT_EQ(OB_SUCCESS, init_server_locality(route_policy_.local_locality_, 1, 0, "active",
                                            "127.0.0.1", 12345,
                                            "zone1", ZONE_TYPE_READWRITE, "IDC1", "HANGZHOU", true));
  ObServerLocality server_locality;
  ASSERT_EQ(OB_SUCCESS, init_server_locality(server_locality, 1, 0, "active",
                                            "127.0.0.3", 12345,
                                            "zone2", ZONE_TYPE_READWRITE, "IDC1", "SHANGHAI", true));

  ASSERT_EQ(OB_SUCCESS, route_policy_.server_locality_array_.push_back(server_locality));

  // select replica
  ASSERT_EQ(OB_SUCCESS, route_policy_.init_candidate_replicas(candi_replicas));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas, phy_part_loc_info));

  //test result
  LOG_INFO("CANDI_PRELICA_EMPTY", K(candi_replicas));
  ASSERT_EQ(1, phy_part_loc_info.get_priority_replica_idxs().count());
  ASSERT_EQ(1, phy_part_loc_info.get_priority_replica_idxs().at(0));

}

TEST_F(ObRoutePolicyTest, LOCAL_CANDI_EMPTY)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  route_policy_.has_readonly_zone_ = true;
  ObCandiTableLoc phy_tbl_loc_info;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info);
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info = phy_tbl_loc_info.get_phy_part_loc_info_list_for_update().at(0);
  ObRoutePolicy::CandidateReplica candi_replica1, candi_replica2;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas = phy_part_loc_info.get_partition_location().get_replica_locations();

  //init candidate_replica;
  candi_replica1.server_ = ObAddr(ObAddr::IPV4, "127.0.0.2", 12345);
  candi_replica2.server_ = ObAddr(ObAddr::IPV4, "127.0.0.3", 12345);
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica2));
  replica_idx_ = 0;

  //init locality
  ObServerLocality server_locality;
  ASSERT_EQ(OB_SUCCESS, init_server_locality(server_locality, 1, 0, "active",
                                            "127.0.0.2", 12345,
                                            "zone1", ZONE_TYPE_READWRITE, "IDC1", "HANGZHOU", true));
  ASSERT_EQ(OB_SUCCESS, route_policy_.server_locality_array_.push_back(server_locality));
  server_locality.reset();
  ASSERT_EQ(OB_SUCCESS, init_server_locality(server_locality, 1, 0, "active",
                                            "127.0.0.3", 12345,
                                            "zone2", ZONE_TYPE_READONLY, "IDC1", "SHANGHAI", true));

  ASSERT_EQ(OB_SUCCESS, route_policy_.server_locality_array_.push_back(server_locality));

  // select replica
  ASSERT_EQ(OB_SUCCESS, route_policy_.init_candidate_replicas(candi_replicas));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas, phy_part_loc_info));


  //test result
  LOG_INFO("LOCAL_CANDI_EMPTY", K(candi_replicas));
  ASSERT_EQ(1, phy_part_loc_info.get_priority_replica_idxs().count());
  ASSERT_EQ(0, phy_part_loc_info.get_priority_replica_idxs().at(0));

  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);
  bool is_hit_partition = false;
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));

}

TEST_F(ObRoutePolicyTest, ALL_EMPTY)
{
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  route_policy_.has_readonly_zone_ = true;
  ObCandiTableLoc phy_tbl_loc_info;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info);
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info = phy_tbl_loc_info.get_phy_part_loc_info_list_for_update().at(0);
  ObRoutePolicy::CandidateReplica candi_replica1, candi_replica2;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas = phy_part_loc_info.get_partition_location().get_replica_locations();

  //init candidate_replica;
  candi_replica1.server_ = ObAddr(ObAddr::IPV4, "127.0.0.2", 12345);
  candi_replica2.server_ = ObAddr(ObAddr::IPV4, "127.0.0.3", 12345);
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, candi_replicas.push_back(candi_replica2));
  replica_idx_ = 0;

  // select replica
  ASSERT_EQ(OB_SUCCESS, route_policy_.init_candidate_replicas(candi_replicas));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas, phy_part_loc_info));


  //test result
  LOG_INFO("LOCAL_CANDI_EMPTY", K(candi_replicas));
  ASSERT_EQ(1, phy_part_loc_info.get_priority_replica_idxs().count());
  //ASSERT_EQ(0, phy_part_loc_info.get_priority_replica_idxs().at(0));

  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);
  bool is_hit_partition = false;
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));

}

TEST_F(ObRoutePolicyTest, INTERSECT_SAME_PRIORITY)
{
  //测试多个partition存在交集，并且交集中的server priority均相同时，应该随机去选择server
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(2, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_REGION, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(2, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(2, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));

  //由于intersect中所有server的priority相同，因此selected replica具有随机性
  ASSERT_EQ(true, candi_replica1.replica_idx_ == 0 || candi_replica1.replica_idx_ == 1);
  ASSERT_EQ(true, candi_replica2.replica_idx_ == 0 || candi_replica2.replica_idx_ == 1);
}

TEST_F(ObRoutePolicyTest, INTERSECT_SAME_PRIORITY_WITH_IDC)
{
  //测试多个partition存在交集，交集中server有SAME_SERVER和SAME_IDC两种，server选择时应该选中SAME_SERVER
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(2, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(2, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(2, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));

  //position type为SAME_SERVER的应该被选中
  ASSERT_EQ(1, candi_replica1.replica_idx_);
  ASSERT_EQ(1, candi_replica2.replica_idx_);
}

TEST_F(ObRoutePolicyTest, NO_INTERSECT_WITH_RAND_SELECTED)
{
  //两个partition没有intersect的情况，但是每个partition都具有多个相同priority的server，此时应该随机选择
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(2, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.3", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.4", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(2, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(0, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));

  //priority 相同，随机选择
  ASSERT_EQ(true, candi_replica1.replica_idx_ == 0 || candi_replica1.replica_idx_ == 1);
  ASSERT_EQ(true, candi_replica2.replica_idx_ == 0 || candi_replica2.replica_idx_ == 1);
}

TEST_F(ObRoutePolicyTest, NO_INTERSECT_WITH_SAME_SERVER_SELECTED)
{
  //两个partition没有intersect的情况，但是每个partition都具有多个priority, 此时position type为SAME SERVER的应该被选中
  route_policy_ctx_.policy_type_ = READONLY_ZONE_FIRST;
  route_policy_ctx_.consistency_level_ = WEAK;
  bool is_hit_partition = false;
  ObArenaAllocator allocator(ObModIds::OB_SQL_OPTIMIZER_SELECT_REPLICA);
  ObCandiTableLoc phy_tbl_loc_info1;
  ObCandiTableLoc phy_tbl_loc_info2;
  ObArray<ObCandiTableLoc*> phy_tbl_loc_info_list;
  ObList<ObRoutePolicy::CandidateReplica, ObArenaAllocator> intersect_server_list(allocator);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().prepare_allocate(1));
  ObCandiTabletLoc &phy_part_loc_info1 = phy_tbl_loc_info1.get_phy_part_loc_info_list_for_update().at(0);
  ObCandiTabletLoc &phy_part_loc_info2 = phy_tbl_loc_info2.get_phy_part_loc_info_list_for_update().at(0);

  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info1));
  ASSERT_EQ(OB_SUCCESS, phy_tbl_loc_info_list.push_back(&phy_tbl_loc_info2));

  //calc partition 1
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas1 = phy_part_loc_info1.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.1", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas1, ObAddr(ObAddr::IPV4, "127.0.0.2", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas1, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas1, phy_part_loc_info1));

  ASSERT_EQ(2, phy_part_loc_info1.get_priority_replica_idxs().count());

  //calc partition 2
  replica_idx_ = 0;
  ObIArray<ObRoutePolicy::CandidateReplica> &candi_replicas2 = phy_part_loc_info2.get_partition_location().get_replica_locations();
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.3", 123),
                                          ObRoutePolicy::SAME_IDC, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, add_candi_replica(candi_replicas2, ObAddr(ObAddr::IPV4, "127.0.0.4", 123),
                                          ObRoutePolicy::SAME_SERVER, ObRoutePolicy::NOMERGING, ZONE_TYPE_READWRITE, ObZoneStatus::ACTIVE));
  ASSERT_EQ(OB_SUCCESS, route_policy_.calculate_replica_priority(candi_replicas2, route_policy_ctx_));
  ASSERT_EQ(OB_SUCCESS, route_policy_.select_replica_with_priority(route_policy_ctx_, candi_replicas2, phy_part_loc_info2));

  //test result
  ASSERT_EQ(2, phy_part_loc_info2.get_priority_replica_idxs().count());

  ASSERT_EQ(OB_SUCCESS, route_policy_.select_intersect_replica(route_policy_ctx_, phy_tbl_loc_info_list, intersect_server_list, is_hit_partition));
  ASSERT_EQ(0, intersect_server_list.size());
  ObRoutePolicy::CandidateReplica candi_replica1;
  ObRoutePolicy::CandidateReplica candi_replica2;
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info1.get_selected_replica(candi_replica1));
  ASSERT_EQ(OB_SUCCESS, phy_part_loc_info2.get_selected_replica(candi_replica2));

  //position type为SAME_SERVER的应该被选中
  ASSERT_EQ(1, candi_replica1.replica_idx_);
  ASSERT_EQ(1, candi_replica2.replica_idx_);
}

}
int main(int argc, char **argv)
{
  system("rm -rf test_route_policy.log");
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  OB_LOGGER.set_file_name("test_route_policy.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
