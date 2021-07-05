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
namespace oceanbase {
namespace sql {

class ObOptPartLoc {
  OB_UNIS_VERSION(1);
  // friend class ObPartitionReplicaLocation;
public:
  typedef common::ObSEArray<ObRoutePolicy::CandidateReplica, common::OB_MAX_MEMBER_NUMBER, common::ModulePageAllocator,
      true>
      ObSmartReplicaLocationArray;

  ObOptPartLoc();
  virtual ~ObOptPartLoc();

  void reset();
  int assign(const ObOptPartLoc& partition_location);
  int assign_with_only_readable_replica(
      const share::ObPartitionLocation& partition_location, const common::ObIArray<common::ObAddr>& invalid_servers);

  bool is_valid() const;
  bool operator==(const ObOptPartLoc& other) const;

  // return OB_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int get_strong_leader(share::ObReplicaLocation& replica_location, int64_t& replica_idx) const;
  int get_strong_leader(share::ObReplicaLocation& replica_location) const;

  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  // inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }

  inline int64_t get_partition_id() const
  {
    return partition_id_;
  }
  // inline void set_partition_id(const int64_t partition_id) { partition_id_ = partition_id; }

  inline int64_t get_partition_cnt() const
  {
    return partition_cnt_;
  }
  // inline void set_partition_cnt(const int64_t partition_cnt) { partition_cnt_ = partition_cnt; }

  inline int64_t get_renew_time() const
  {
    return renew_time_;
  }
  // inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; }

  inline const common::ObIArray<ObRoutePolicy::CandidateReplica>& get_replica_locations() const
  {
    return replica_locations_;
  }

  inline common::ObIArray<ObRoutePolicy::CandidateReplica>& get_replica_locations()
  {
    return replica_locations_;
  }

  inline int get_partition_key(common::ObPartitionKey& key) const
  {
    return key.init(table_id_, partition_id_, partition_cnt_);
  }
  inline const common::ObPGKey& get_pg_key() const
  {
    return pg_key_;
  }

  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(partition_cnt), K_(pg_key), K_(replica_locations), K_(renew_time),
      K_(is_mark_fail));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;
  ObSmartReplicaLocationArray replica_locations_;
  int64_t renew_time_;
  bool is_mark_fail_;
  common::ObPGKey pg_key_;
};

class ObPhyPartitionLocationInfo {
public:
  ObPhyPartitionLocationInfo();
  virtual ~ObPhyPartitionLocationInfo();

  void reset();
  int assign(const ObPhyPartitionLocationInfo& other);

  int set_selected_replica_idx(int64_t selected_replica_idx);
  int set_selected_replica_idx_with_priority();
  int add_priority_replica_idx(int64_t priority_replica_idx);
  int64_t get_selected_replica_idx() const
  {
    return selected_replica_idx_;
  }
  bool has_selected_replica() const
  {
    return common::OB_INVALID_INDEX != selected_replica_idx_;
  }

  int get_selected_replica(share::ObReplicaLocation& replica_loc) const;
  int get_selected_replica(ObRoutePolicy::CandidateReplica& replica_loc) const;
  int get_priority_replica(int64_t idx, share::ObReplicaLocation& replica_loc) const;
  int get_priority_replica(int64_t idx, ObRoutePolicy::CandidateReplica& replica_loc) const;
  template <class T>
  int get_priority_replica_base(int64_t selected_replica_idx, T& replica_loc) const;
  int set_part_loc_with_only_readable_replica(
      const share::ObPartitionLocation& partition_location, const common::ObIArray<common::ObAddr>& invalid_servers);
  const ObOptPartLoc& get_partition_location() const
  {
    return partition_location_;
  }
  ObOptPartLoc& get_partition_location()
  {
    return partition_location_;
  }
  const common::ObIArray<int64_t>& get_priority_replica_idxs() const
  {
    return priority_replica_idxs_;
  }
  static int reselect_duplicate_table_best_replica(const common::ObAddr& server, ObPhyPartitionLocationInfo& l,
      bool l_can_reselect, ObPhyPartitionLocationInfo& r, bool r_can_reselect);
  int reset_duplicate_table_best_replica(const common::ObIArray<int64_t>& new_replic_idxs);
  bool is_server_in_replica(const common::ObAddr& server, int64_t& idx) const;
  TO_STRING_KV(K_(partition_location), K_(selected_replica_idx), K_(priority_replica_idxs));

private:
  ObOptPartLoc partition_location_;

  int64_t selected_replica_idx_;

  common::ObSEArray<int64_t, 2, common::ModulePageAllocator, true> priority_replica_idxs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhyPartitionLocationInfo);
};

typedef common::ObIArray<ObPhyPartitionLocationInfo> ObPhyPartitionLocationInfoIArray;
typedef common::ObSEArray<ObPhyPartitionLocationInfo, 2, common::ModulePageAllocator, true>
    ObPhyPartitionLocationInfoSEArray;

class ObPhyTableLocationInfo {
public:
  ObPhyTableLocationInfo();
  virtual ~ObPhyTableLocationInfo();

public:
  void reset();
  int assign(const ObPhyTableLocationInfo& other);

  inline void set_table_location_key(uint64_t table_location_key, uint64_t ref_table_id)
  {
    table_location_key_ = table_location_key;
    ref_table_id_ = ref_table_id;
  }
  inline uint64_t get_table_location_key() const
  {
    return table_location_key_;
  }
  inline uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }

  inline const ObPhyPartitionLocationInfoIArray& get_phy_part_loc_info_list() const
  {
    return phy_part_loc_info_list_;
  }
  inline ObPhyPartitionLocationInfoIArray& get_phy_part_loc_info_list_for_update()
  {
    return phy_part_loc_info_list_;
  }
  int64_t get_partition_cnt() const
  {
    return phy_part_loc_info_list_.count();
  }
  const common::ObIArray<ObSplittedRanges>& get_splitted_ranges() const
  {
    return splitted_range_list_;
  }
  common::ObIArray<ObSplittedRanges>& get_splitted_ranges_for_update()
  {
    return splitted_range_list_;
  }

  int all_select_leader(bool& is_on_same_server, common::ObAddr& same_server);
  int all_select_local_replica_or_leader(
      bool& is_on_same_server, common::ObAddr& same_server, const common::ObAddr& local_server);
  int all_select_fixed_server(const common::ObAddr& fixed_server);

  int set_direction(const ObOrderDirection& direction);
  const ObOrderDirection& get_direction() const
  {
    return direction_;
  }
  bool is_duplicate_table() const
  {
    return ObDuplicateType::NOT_DUPLICATE != duplicate_type_;
  }
  bool is_duplicate_table_not_in_dml() const
  {
    return ObDuplicateType::DUPLICATE == duplicate_type_;
  }
  void set_duplicate_type(ObDuplicateType v)
  {
    duplicate_type_ = v;
  }
  ObDuplicateType get_duplicate_type() const
  {
    return duplicate_type_;
  }
  int reset_duplicate_table_best_replica(const common::ObIArray<int64_t>& new_replic_idxs);
  bool is_server_in_replica(const common::ObAddr& server, common::ObIArray<int64_t>& new_replic_idxs) const;
  static int is_server_in_replica(const ObPhyTableLocationInfo& l_phy_loc,
      const ObPhyPartitionLocationInfo& part_loc_info, bool& is_same, common::ObAddr& the_server, int64_t& new_idx);
  TO_STRING_KV(K_(table_location_key), K_(ref_table_id), K_(phy_part_loc_info_list), K_(splitted_range_list),
      K_(duplicate_type));

private:
  uint64_t table_location_key_;

  uint64_t ref_table_id_;
  /* location order */
  ObOrderDirection direction_;
  /* locations */
  ObPhyPartitionLocationInfoSEArray phy_part_loc_info_list_;
  common::ObSEArray<ObSplittedRanges, 2> splitted_range_list_;  // corresponding to partition list

  ObDuplicateType duplicate_type_;

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPhyTableLocationInfo);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OPTIMIZER_OB_PHY_TABLE_LOCATION_INFO_ */
