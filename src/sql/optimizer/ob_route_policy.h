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

#ifndef OCEANBASE_SQL_OB_ROUTE_POLICY_H
#define OCEANBASE_SQL_OB_ROUTE_POLICY_H
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"
#include "lib/list/ob_list.h"
#include "common/ob_zone_status.h"
#include "common/ob_zone_type.h"
#include "share/location_cache/ob_location_struct.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_server_locality_cache.h"
#include "share/ob_server_status.h"
namespace oceanbase
{
namespace sql
{
class ObCandiTabletLoc;
class ObCandiTableLoc;
enum ObRoutePolicyType
{
  INVALID_POLICY = 0,
  READONLY_ZONE_FIRST = 1,
  ONLY_READONLY_ZONE = 2,
  UNMERGE_ZONE_FIRST = 3,
  // 仅在非读写分离场景中有效, server端的路由规则和非读写分离下普通弱读相同,
  // 即使客户端将请求路由到partition主上, 也在本地执行，
  // 区别在于返回给OCJ && ObProxy的反馈不同;
  UNMERGE_FOLLOWER_FIRST = 4,
  POLICY_TYPE_MAX
};

struct ObRoutePolicyCtx
{
  ObRoutePolicyCtx()
    :policy_type_(POLICY_TYPE_MAX),
     consistency_level_(common::INVALID_CONSISTENCY),
     is_proxy_priority_hit_support_(false),
     tenant_id_(OB_INVALID_TENANT_ID),
     max_read_stale_time_(0)
   {}

  TO_STRING_KV(K(policy_type_),
               K(consistency_level_),
               K(is_proxy_priority_hit_support_),
               K(tenant_id_),
               K(max_read_stale_time_));

  ObRoutePolicyType policy_type_;
  common::ObConsistencyLevel consistency_level_;
  bool is_proxy_priority_hit_support_;
  uint64_t tenant_id_;
  int64_t max_read_stale_time_;
};

class ObRoutePolicy
{
public:
  enum PositionType
  {
    SAME_SERVER = 0,
    SAME_IDC = 1,
    SAME_REGION = 2,
    OTHER_REGION = 3,
    POSITION_TYPE_MAX,
  };
  enum MergeStatus
  {
    MERGING,
    NOMERGING,
    MERGE_STATUS_MAX,
  };
  class ReplicaAttribute
  {
  public:
  ReplicaAttribute()
      :pos_type_(POSITION_TYPE_MAX),
        merge_status_(MERGE_STATUS_MAX),
        zone_status_(share::ObZoneStatus::UNKNOWN),
        zone_type_(common::ZONE_TYPE_INVALID),
        start_service_time_(0),
        server_stop_time_(0),
        server_status_(share::ObServerStatus::OB_DISPLAY_MAX)
        {}
    ~ReplicaAttribute(){}
    bool operator==(const ReplicaAttribute &other_attr) const
    {
      bool bool_ret = false;
      if (merge_status_ == other_attr.merge_status_
          && zone_type_ == other_attr.zone_type_) {
        if (pos_type_ == SAME_SERVER) {
          bool_ret = (other_attr.pos_type_ == SAME_SERVER || other_attr.pos_type_ == SAME_IDC);
        } else if (pos_type_ == SAME_IDC) {
          bool_ret = (other_attr.pos_type_ == SAME_SERVER || other_attr.pos_type_ == SAME_IDC);
        } else {
          bool_ret = (pos_type_ == other_attr.pos_type_);
        }
      }
      return bool_ret;
    }
    void reset()
    {
      pos_type_ = POSITION_TYPE_MAX;
      merge_status_ = MERGE_STATUS_MAX;
      zone_status_ = share::ObZoneStatus::UNKNOWN;
      zone_type_ = common::ZONE_TYPE_INVALID;
      start_service_time_ = 0;
      server_stop_time_ = 0;
      server_status_ = share::ObServerStatus::OB_DISPLAY_MAX;
    }
    TO_STRING_KV(K(pos_type_), K(merge_status_), K(zone_type_), K(zone_status_), K(start_service_time_), K(server_stop_time_), K(server_status_));
    PositionType pos_type_;
    MergeStatus merge_status_;
    share::ObZoneStatus::Status zone_status_;
    common::ObZoneType zone_type_;
    int64_t start_service_time_;
    int64_t server_stop_time_;
    share::ObServerStatus::DisplayStatus server_status_;
  };
  class CandidateReplica final : public share::ObLSReplicaLocation
  {
  public:
    CandidateReplica()
      : ObLSReplicaLocation(),
        attr_(),
        is_filter_(false),
        replica_idx_(common::OB_INVALID_INDEX)
    {}
   CandidateReplica(const share::ObLSReplicaLocation &replica_location)
      : ObLSReplicaLocation(replica_location),
        attr_(),
        is_filter_(false),
        replica_idx_(common::OB_INVALID_INDEX)
    {}
    bool is_usable() const { return is_filter_ == false; }
    void reset()
    {
      ObLSReplicaLocation::reset();
      attr_.reset();
      is_filter_ = false;
    }
    int assign(CandidateReplica &other);
    //to_string only outputs the key information that SQL execution is concerned with,
    //and other information can be obtained through the corresponding virtual tables.
    TO_STRING_KV(K_(server),
                 K_(role),
                 K_(sql_port),
                 K_(is_filter));
  public:
    ReplicaAttribute attr_;
    bool is_filter_;
    int64_t replica_idx_;//invalid
  };
public:
  ObRoutePolicy(const common::ObAddr &addr)
      :local_addr_(addr),
      local_locality_(),
      server_locality_array_(),
      has_refresh_locality_(false),
      has_readonly_zone_(false),
      is_inited_(false)
  {}
  ~ObRoutePolicy() {}
  int init();
  int calculate_replica_priority(const ObAddr &local_server,
                                 const share::ObLSID &ls_id,
                                 common::ObIArray<CandidateReplica>& candi_replicas,
                                 ObRoutePolicyCtx &ctx);
  int init_candidate_replicas(common::ObIArray<CandidateReplica> &candi_replicas);
  int select_replica_with_priority(const ObRoutePolicyCtx &route_policy_ctx,
                                   const common::ObIArray<CandidateReplica> &replica_array,
                                   ObCandiTabletLoc &phy_part_loc_info);
  int select_intersect_replica(ObRoutePolicyCtx &route_policy_ctx,
                               common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                               common::ObList<ObRoutePolicy::CandidateReplica, common::ObArenaAllocator> &intersect_server_list,
                               bool &is_proxy_hit);

  TO_STRING_KV(K(local_addr_),
               K(local_locality_),
               K(server_locality_array_),
               K(has_refresh_locality_),
               K(has_readonly_zone_),
               K(is_inited_));

  inline bool is_follower_first_route_policy_type(const ObRoutePolicyCtx &ctx) const
  {
    return !has_readonly_zone_ && (UNMERGE_FOLLOWER_FIRST == ctx.policy_type_);
  }

  static int get_server_locality(const common::ObAddr &addr,
                                 const common::ObIArray<share::ObServerLocality> &server_locality_array,
                                 share::ObServerLocality &svr_locality);
  static bool is_same_idc(const share::ObServerLocality &locality1,
                          const share::ObServerLocality &locality2);
  static bool is_same_region(const share::ObServerLocality &locality1,
                             const share::ObServerLocality &locality2);

protected:
  int init_candidate_replica(const common::ObIArray<share::ObServerLocality> &server_locality_array,
                             CandidateReplica &candi_replica);
  int calc_position_type(const share::ObServerLocality &candi_locality,
                         CandidateReplica &candi_replica);
  int calc_intersect_repllica(const common::ObIArray<ObCandiTableLoc*> &phy_tbl_loc_info_list,
                              common::ObList<ObRoutePolicy::CandidateReplica, common::ObArenaAllocator> &intersect_server_list);
  int get_merge_status(const share::ObServerLocality &candi_locality, CandidateReplica &candi_replica);
  int get_zone_status(const share::ObServerLocality &candi_locality, CandidateReplica &candi_replica);

  int filter_replica(const ObAddr &local_server,
                     const share::ObLSID &ls_id,
                     common::ObIArray<CandidateReplica>& candi_replicas,
                     ObRoutePolicyCtx &ctx);
  int weak_sort_replicas(common::ObIArray<CandidateReplica>& candi_replicas, ObRoutePolicyCtx &ctx);
  int strong_sort_replicas(common::ObIArray<CandidateReplica>& candi_replicas, ObRoutePolicyCtx &ctx);
  inline ObRoutePolicyType get_calc_route_policy_type(const ObRoutePolicyCtx &ctx) const
  {
    // 集群为读写zone时，忽略ob_route_policy系统变量的值，按照READONLY_ZONE_FIRST处理
    // 集群为读写zone时, 且ob_route_policy为UNMERGE_FOLLOWER_FIRST时，同样按照READONLY_ZONE_FIRST处理, 但会增加反馈内容
    // 集群为有只读zone时，且ob_route_policy为UNMERGE_FOLLOWER_FIRST时, 同样按照READONLY_ZONE_FIRST处理，此时不会增加反馈内容
    ObRoutePolicyType type = INVALID_POLICY;
    if (has_readonly_zone_) {
      if (UNMERGE_FOLLOWER_FIRST == ctx.policy_type_) {
        type = READONLY_ZONE_FIRST;
      } else {
        type = ctx.policy_type_;
      }
    } else {
      type = READONLY_ZONE_FIRST;
    }
    return type;
  }
protected:
  common::ObAddr local_addr_;
  share::ObServerLocality local_locality_;
  common::ObSEArray<share::ObServerLocality, 32> server_locality_array_;
  bool has_refresh_locality_;
  bool has_readonly_zone_;
  bool is_inited_;
};

}//sql
}//oceanbase
#endif
