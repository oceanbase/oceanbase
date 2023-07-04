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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_PHY_TABLE_LOCATION_INFO_
#define OCEANBASE_SQL_OPTIMIZER_OB_PHY_TABLE_LOCATION_INFO_

#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_route_policy.h"
namespace oceanbase
{
namespace sql
{
class DASRelatedTabletMap;
class ObOptTabletLoc
{
  OB_UNIS_VERSION(1);
  //friend class ObPartitionReplicaLocation;
public:
  typedef common::ObSEArray<ObRoutePolicy::CandidateReplica,
                            common::OB_MAX_MEMBER_NUMBER, common::ModulePageAllocator,
                            true> ObSmartReplicaLocationArray;

  ObOptTabletLoc();
  virtual ~ObOptTabletLoc();

  void reset();
  int assign(const ObOptTabletLoc &partition_location);
  int assign_with_only_readable_replica(const ObObjectID &partition_id,
                                        const ObObjectID &first_level_part_id,
                                        const common::ObTabletID &tablet_id,
                                        const share::ObLSLocation &partition_location);

  bool is_valid() const;
  bool operator==(const ObOptTabletLoc &other) const;

  // return OB_LS_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int get_strong_leader(share::ObLSReplicaLocation &replica_location, int64_t &replica_idx) const;
  int get_strong_leader(share::ObLSReplicaLocation &replica_location) const;

  void set_tablet_info(common::ObTabletID tablet_id,
                       common::ObPartID part_id,
                       common::ObPartID first_level_part_id)
  {
    tablet_id_ = tablet_id;
    partition_id_ = part_id;
    first_level_part_id_ = first_level_part_id;
  }
  inline int64_t get_partition_id() const { return partition_id_; }

  inline int64_t get_first_level_part_id() const { return first_level_part_id_; }

  inline common::ObTabletID get_tablet_id() const { return tablet_id_; }

  inline const share::ObLSID &get_ls_id() const { return ls_id_; }

  inline const common::ObIArray<ObRoutePolicy::CandidateReplica> &get_replica_locations() const { return replica_locations_; }

  inline common::ObIArray<ObRoutePolicy::CandidateReplica> &get_replica_locations() { return replica_locations_; }

  TO_STRING_KV(K_(partition_id),
               K_(tablet_id),
               K_(ls_id),
               K_(replica_locations));

private:
  int64_t partition_id_;
  // first level part id, only valid for subpartitioned table
  int64_t first_level_part_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  ObSmartReplicaLocationArray replica_locations_;
};

class ObCandiTabletLoc
{
public:
  ObCandiTabletLoc();
  ~ObCandiTabletLoc();

  void reset();
  int assign(const ObCandiTabletLoc &other);

  int set_selected_replica_idx(int64_t selected_replica_idx);
  int set_selected_replica_idx_with_priority();
  int add_priority_replica_idx(int64_t priority_replica_idx);
  int64_t get_selected_replica_idx() const { return selected_replica_idx_; }
  bool has_selected_replica() const { return common::OB_INVALID_INDEX != selected_replica_idx_; }
  const share::ObLSID &get_ls_id() const { return opt_tablet_loc_.get_ls_id(); }
  int get_selected_replica(share::ObLSReplicaLocation &replica_loc) const;
  int get_selected_replica(ObRoutePolicy::CandidateReplica &replica_loc) const;
  int get_priority_replica(int64_t idx, share::ObLSReplicaLocation &replica_loc) const;
  int get_priority_replica(int64_t idx, ObRoutePolicy::CandidateReplica &replica_loc) const;
  template<class T>
  int get_priority_replica_base(int64_t selected_replica_idx, T &replica_loc) const;
  int set_part_loc_with_only_readable_replica(const ObObjectID &partition_id,
                                              const ObObjectID &first_level_part_id,
                                              const common::ObTabletID &tablet_id,
                                              const share::ObLSLocation &partition_location);
  const ObOptTabletLoc &get_partition_location() const { return opt_tablet_loc_; }
  ObOptTabletLoc &get_partition_location() { return opt_tablet_loc_; }
  const common::ObIArray<int64_t> &get_priority_replica_idxs() const { return priority_replica_idxs_; }
  bool is_server_in_replica(const common::ObAddr &server, int64_t &idx) const;
  TO_STRING_KV(K_(opt_tablet_loc), K_(selected_replica_idx), K_(priority_replica_idxs));

private:
  ObOptTabletLoc opt_tablet_loc_;
  //对所有partition求完交集后的结果，是最终选定的replica的index
  int64_t selected_replica_idx_;
  //对当前partition的所有副本进行优先级判断后，将最高优先级的replica index存到这里
  common::ObSEArray<int64_t, 2, common::ModulePageAllocator, true> priority_replica_idxs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCandiTabletLoc);
};

typedef common::ObIArray<ObCandiTabletLoc> ObCandiTabletLocIArray;
typedef common::ObSEArray<ObCandiTabletLoc, 2, common::ModulePageAllocator, true> ObCandiTabletLocSEArray;

class ObCandiTableLoc
{
public:
  ObCandiTableLoc();
  virtual ~ObCandiTableLoc();
public:
  void reset();
  int assign(const ObCandiTableLoc &other);

  void set_table_location_key(uint64_t table_location_key, uint64_t ref_table_id);
  int replace_local_index_loc(DASRelatedTabletMap &map, common::ObTableID ref_table_id);
  inline uint64_t get_table_location_key() const { return table_location_key_; }
  inline uint64_t get_ref_table_id() const { return ref_table_id_; }

  inline const ObCandiTabletLocIArray &get_phy_part_loc_info_list() const
  {
    return candi_tablet_locs_;
  }
  inline ObCandiTabletLocIArray &get_phy_part_loc_info_list_for_update()
  {
    return candi_tablet_locs_;
  }
  int64_t get_partition_cnt() const { return candi_tablet_locs_.count(); }

  int all_select_leader(bool &is_on_same_server,
                        common::ObAddr &same_server);
  int all_select_local_replica_or_leader(bool &is_on_same_server,
                                         common::ObAddr &same_server,
                                         const common::ObAddr &local_server);
  int all_select_fixed_server(const common::ObAddr &fixed_server);
  int get_all_servers(common::ObIArray<common::ObAddr> &servers) const;
  bool is_duplicate_table() const { return ObDuplicateType::NOT_DUPLICATE != duplicate_type_; }
  bool is_duplicate_table_not_in_dml() const { return ObDuplicateType::DUPLICATE == duplicate_type_; }
  void set_duplicate_type(ObDuplicateType v) { duplicate_type_ = v; }
  ObDuplicateType get_duplicate_type() const { return duplicate_type_; }
  bool is_server_in_replica(const common::ObAddr &server, common::ObIArray<int64_t> &new_replic_idxs) const;
  static int is_server_in_replica(const ObCandiTableLoc &l_phy_loc,
                                  const ObCandiTabletLoc &part_loc_info,
                                  bool &is_same,
                                  common::ObAddr &the_server,
                                  int64_t &new_idx);
  TO_STRING_KV(K_(table_location_key), K_(ref_table_id), K_(candi_tablet_locs),
               K_(duplicate_type));

private:
  /* 用于表ID(可能是generated alias id)寻址location */
  uint64_t table_location_key_;
  /* 用于获取实际的物理表ID */
  uint64_t ref_table_id_;
  /* locations */
  ObCandiTabletLocSEArray candi_tablet_locs_;
  //复制表类型, 如果是复制表且未被更改则可以在分配exg算子时挑选更合适的副本
  ObDuplicateType duplicate_type_;
private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObCandiTableLoc);
};
}
}
#endif /* OCEANBASE_SQL_OPTIMIZER_OB_PHY_TABLE_LOCATION_INFO_ */
