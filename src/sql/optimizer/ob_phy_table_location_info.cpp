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

#include "ob_phy_table_location_info.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

ObOptPartLoc::ObOptPartLoc()
    : replica_locations_(ObModIds::OB_SQL_OPTIMIZER_LOCATION_CACHE, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

ObOptPartLoc::~ObOptPartLoc()
{}

void ObOptPartLoc::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  replica_locations_.reset();
  renew_time_ = 0;
  is_mark_fail_ = false;
  pg_key_.reset();
}

int ObOptPartLoc::assign(const ObOptPartLoc& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  partition_cnt_ = other.partition_cnt_;
  renew_time_ = other.renew_time_;
  is_mark_fail_ = other.is_mark_fail_;
  pg_key_ = other.pg_key_;
  if (OB_FAIL(replica_locations_.assign(other.replica_locations_))) {
    LOG_WARN("Failed to assign replica locations", K(ret));
  }
  return ret;
}

int ObOptPartLoc::assign_with_only_readable_replica(
    const ObPartitionLocation& other, const ObIArray<ObAddr>& invalid_servers)
{
  int ret = OB_SUCCESS;
  reset();
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  partition_cnt_ = other.partition_cnt_;
  renew_time_ = other.renew_time_;
  is_mark_fail_ = other.is_mark_fail_;
  pg_key_ = other.pg_key_;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.replica_locations_.count(); ++i) {
    const ObReplicaLocation& replica_loc = other.replica_locations_.at(i);
    if (ObReplicaTypeCheck::is_readable_replica(replica_loc.replica_type_)) {
      bool is_in_invalid_servers = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_in_invalid_servers && j < invalid_servers.count(); ++j) {
        if (replica_loc.server_ == invalid_servers.at(j)) {
          is_in_invalid_servers = true;
        }
      }
      if (OB_SUCC(ret) && !is_in_invalid_servers) {
        if (OB_FAIL(replica_locations_.push_back(replica_loc))) {
          LOG_WARN("Failed to push back replica locations", K(ret), K(i), K(replica_loc), K(replica_locations_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == replica_locations_.count())) {
      ret = OB_NO_READABLE_REPLICA;
      LOG_WARN("there has no readable replica", K(ret), K(invalid_servers), K(other.replica_locations_));
    }
  }
  return ret;
}

bool ObOptPartLoc::is_valid() const
{
  // for compat, after 1.4.x and 2.1 is upgraded to 2.2, pg_key is invalid
  // hence does not check pg_key_ here
  return OB_INVALID_ID != table_id_ && OB_INVALID_INDEX != partition_id_ && renew_time_ >= 0;
}

// bool ObOptPartLoc::operator==(const ObOptPartLoc &other) const
// {
//   bool equal = true;
//   if (!is_valid() || !other.is_valid()) {
//     equal = false;
//     LOG_WARN("invalid argument", "self", *this, K(other));
//   } else if (replica_locations_.count() != other.replica_locations_.count()) {
//     equal = false;
//   } else {
//     for (int64_t i = 0; i < replica_locations_.count(); ++i) {
//       if (replica_locations_.at(i) != other.replica_locations_.at(i)) {
//         equal = false;
//         break;
//       }
//     }
//     equal = equal && (table_id_ == other.table_id_) && (partition_id_ == other.partition_id_)
//         && (partition_cnt_ == other.partition_cnt_) && (renew_time_ == other.renew_time_)
//         && (is_mark_fail_ == other.is_mark_fail_);
//   }
//   return equal;
// }

int ObOptPartLoc::get_strong_leader(ObReplicaLocation& replica_location, int64_t& replica_idx) const
{
  int ret = OB_LOCATION_LEADER_NOT_EXIST;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LOCATION_LEADER_NOT_EXIST == ret)
    {
      if (replica_locations_.at(i).is_strong_leader()) {
        replica_location = replica_locations_.at(i);
        replica_idx = i;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObOptPartLoc::get_strong_leader(ObReplicaLocation& replica_location) const
{
  int64_t replica_idx = OB_INVALID_INDEX;
  return get_strong_leader(replica_location, replica_idx);
}

ObPhyPartitionLocationInfo::ObPhyPartitionLocationInfo()
    : partition_location_(), selected_replica_idx_(OB_INVALID_INDEX), priority_replica_idxs_()
{}

ObPhyPartitionLocationInfo::~ObPhyPartitionLocationInfo()
{}

void ObPhyPartitionLocationInfo::reset()
{
  partition_location_.reset();
  selected_replica_idx_ = OB_INVALID_INDEX;
  priority_replica_idxs_.reset();
}

int ObPhyPartitionLocationInfo::assign(const ObPhyPartitionLocationInfo& other)
{
  int ret = OB_SUCCESS;
  /*
  if (OB_UNLIKELY(other.selected_replica_idx_ < 0
                  || other.selected_replica_idx_ >=
                  other.partition_location_.get_replica_locations().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("other select replica idx is invalid", K(ret), K(other.selected_replica_idx_),
              K(other.partition_location_.get_replica_locations().count()),
              K(other.partition_location_));
  } else
  */

  if (OB_FAIL(partition_location_.assign(other.partition_location_))) {
    LOG_WARN("fail to assign other partition_location_", K(ret), K(other.partition_location_));
  } else if (OB_FAIL(priority_replica_idxs_.assign(other.priority_replica_idxs_))) {
    LOG_WARN("fail to assign replica idxs", K(ret), K(other.priority_replica_idxs_));
  } else {
    selected_replica_idx_ = other.selected_replica_idx_;
  }
  return ret;
}

int ObPhyPartitionLocationInfo::set_selected_replica_idx(int64_t selected_replica_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          selected_replica_idx < 0 || selected_replica_idx >= partition_location_.get_replica_locations().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("select replica idx is invalid",
        K(ret),
        K(selected_replica_idx),
        K(partition_location_.get_replica_locations().count()),
        K(partition_location_));
  } else {
    selected_replica_idx_ = selected_replica_idx;
  }
  return ret;
}

int ObPhyPartitionLocationInfo::set_selected_replica_idx_with_priority()
{
  int ret = OB_SUCCESS;
  ObRoutePolicy::CandidateReplica selected_replica;
  ObRoutePolicy::CandidateReplica cur_replica;
  int64_t selected_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(priority_replica_idxs_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid priority replica count", K(priority_replica_idxs_), K(ret));
  } else if (priority_replica_idxs_.count() == 1) {
    selected_idx = priority_replica_idxs_.at(0);
    if (OB_FAIL(set_selected_replica_idx(selected_idx))) {
      LOG_WARN("fail to set selected replica idx", K(priority_replica_idxs_), K(ret));
    }
  } else if (priority_replica_idxs_.count() > 1) {
    ObSEArray<int64_t, 16> same_priority_ids;
    bool is_first = true;
    selected_replica.attr_.pos_type_ = ObRoutePolicy::POSITION_TYPE_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < priority_replica_idxs_.count(); ++i) {
      int64_t cur_priority_idx = priority_replica_idxs_.at(i);
      cur_replica.reset();
      if (OB_FAIL(get_priority_replica(cur_priority_idx, cur_replica))) {
        LOG_WARN("fail to get priority replca", K(cur_priority_idx), K(ret));
      } else if (is_first) {
        is_first = false;
        selected_replica = cur_replica;
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else if (cur_replica.attr_.pos_type_ == selected_replica.attr_.pos_type_) {
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else if (cur_replica.attr_.pos_type_ < selected_replica.attr_.pos_type_) {
        selected_replica = cur_replica;
        same_priority_ids.reset();
        if (OB_FAIL(same_priority_ids.push_back(cur_priority_idx))) {
          LOG_WARN("fail to push back id", K(same_priority_ids), K(cur_priority_idx), K(ret));
        }
      } else {
        /*cur_replica.attr_.pos_type_ < selected_replica.attr_.pos_type_ do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      int64_t rand_idx = rand() % same_priority_ids.count();
      selected_idx = same_priority_ids.at(rand_idx);
      if (OB_FAIL(set_selected_replica_idx(selected_idx))) {
        LOG_WARN("fail to set selected replica idx", K(priority_replica_idxs_), K(ret));
      }
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObPhyPartitionLocationInfo::add_priority_replica_idx(int64_t priority_replica_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(priority_replica_idxs_.push_back(priority_replica_idx))) {
    LOG_WARN("fail to push back priority replica idx", K(priority_replica_idx), K(ret));
  }
  return ret;
}

int ObPhyPartitionLocationInfo::get_priority_replica(
    int64_t selected_replica_idx, share::ObReplicaLocation& replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica_base(selected_replica_idx, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(ret));
  }
  return ret;
}

int ObPhyPartitionLocationInfo::get_priority_replica(
    int64_t selected_replica_idx, ObRoutePolicy::CandidateReplica& replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica_base(selected_replica_idx, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(ret));
  }
  return ret;
}

template <class T>
int ObPhyPartitionLocationInfo::get_priority_replica_base(int64_t selected_replica_idx, T& replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_INDEX == selected_replica_idx)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObPhyPartitionLocationInfo is not inited", K(ret), K(partition_location_));
  } else {
    const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_list = partition_location_.get_replica_locations();
    if (OB_UNLIKELY(selected_replica_idx < 0 || selected_replica_idx >= replica_loc_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("select replica idx is invalid", K(ret), K(selected_replica_idx), K(replica_loc_list.count()));
    } else {
      replica_loc = replica_loc_list.at(selected_replica_idx);
    }
  }
  return ret;
}

bool ObPhyPartitionLocationInfo::is_server_in_replica(const ObAddr& server, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  bool found_flag = false;
  ObRoutePolicy::CandidateReplica tmp_replica;
  idx = 0;
  for (int64_t replica_idx = 0;
       !found_flag && OB_SUCC(ret) && replica_idx < get_partition_location().get_replica_locations().count();
       ++replica_idx) {
    tmp_replica.reset();
    if (OB_FAIL(get_priority_replica(replica_idx, tmp_replica))) {
      LOG_WARN("fail to get priority replica", K(replica_idx), K(ret));
    } else if (tmp_replica.server_ == server) {
      idx = replica_idx;
      found_flag = true;
    }
  }
  return found_flag;
}

int ObPhyPartitionLocationInfo::reselect_duplicate_table_best_replica(const ObAddr& server,
    ObPhyPartitionLocationInfo& l_info, bool l_can_reselect, ObPhyPartitionLocationInfo& r_info, bool r_can_reselect)
{
  int ret = OB_SUCCESS;
  bool has_optted = false;
  share::ObReplicaLocation l_replica_loc;
  share::ObReplicaLocation r_replica_loc;
  int64_t l_idx = 0;
  int64_t r_idx = 0;
  ObRoutePolicy::CandidateReplica tmp_replica;
  if (l_can_reselect || r_can_reselect) {
    if (OB_FAIL(l_info.get_selected_replica(l_replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(l_info));
    } else if (OB_FAIL(r_info.get_selected_replica(r_replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(r_info));
    } else if (l_replica_loc.server_ != r_replica_loc.server_) {
      if (l_can_reselect && r_can_reselect && l_info.is_server_in_replica(server, l_idx) &&
          r_info.is_server_in_replica(server, r_idx)) {
        LOG_TRACE("reselect replica index will happen", K(l_info), K(r_info), K(l_idx), K(r_idx));
        if (OB_FAIL(l_info.set_selected_replica_idx(l_idx))) {
          LOG_WARN("failed to set left selected replica idx", K(ret), K(l_idx));
        } else if (OB_FAIL(r_info.set_selected_replica_idx(r_idx))) {
          LOG_WARN("failed to set right selected replica idx", K(ret), K(r_idx));
        } else {
          has_optted = true;
        }
      } else {
        if (l_can_reselect && r_can_reselect) {
          for (int64_t replica_idx = 0; !has_optted && OB_SUCC(ret) &&
                                        replica_idx < l_info.get_partition_location().get_replica_locations().count();
               ++replica_idx) {
            tmp_replica.reset();
            if (OB_FAIL(l_info.get_priority_replica(replica_idx, tmp_replica))) {
              LOG_WARN("fail to get priority replica", K(replica_idx), K(ret));
            } else if (r_info.is_server_in_replica(tmp_replica.server_, r_idx)) {
              LOG_TRACE("reselect replica index will happen", K(l_info), K(r_info), K(replica_idx), K(r_idx));
              if (OB_FAIL(l_info.set_selected_replica_idx(replica_idx))) {
                LOG_WARN("failed to set left selected replica idx", K(ret), K(replica_idx));
              } else if (OB_FAIL(r_info.set_selected_replica_idx(r_idx))) {
                LOG_WARN("failed to set right selected replica idx", K(ret), K(r_idx));
              } else {
                has_optted = true;
              }
            }
          }
        } else if (l_can_reselect && l_info.is_server_in_replica(r_replica_loc.server_, l_idx)) {
          LOG_TRACE("reselect replica index will happen", K(l_info), K(l_idx));
          if (OB_FAIL(l_info.set_selected_replica_idx(l_idx))) {
            LOG_WARN("failed to set left selected replica idx", K(ret), K(l_idx));
          } else {
            has_optted = true;
          }
        } else if (r_can_reselect && r_info.is_server_in_replica(l_replica_loc.server_, r_idx)) {
          LOG_TRACE("reselect replica index will happen", K(r_info), K(r_idx));
          if (OB_FAIL(r_info.set_selected_replica_idx(r_idx))) {
            LOG_WARN("failed to set right selected replica idx", K(ret), K(r_idx));
          } else {
            has_optted = true;
          }
        }
      }
    }
  }
  LOG_TRACE("reselect replica idx happened", K(has_optted), K(ret));
  return ret;
}
int ObPhyPartitionLocationInfo::get_selected_replica(share::ObReplicaLocation& replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica(selected_replica_idx_, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(selected_replica_idx_), K(ret));
  }
  return ret;
}

int ObPhyPartitionLocationInfo::get_selected_replica(ObRoutePolicy::CandidateReplica& replica_loc) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_priority_replica(selected_replica_idx_, replica_loc))) {
    LOG_WARN("fail to get priority replica", K(replica_loc), K(selected_replica_idx_), K(ret));
  }
  return ret;
}

int ObPhyPartitionLocationInfo::set_part_loc_with_only_readable_replica(
    const ObPartitionLocation& partition_location, const ObIArray<ObAddr>& invalid_servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(has_selected_replica())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition location has not been set yet, but replica idx has been selected",
        K(ret),
        K(*this),
        K(partition_location));
  } else if (OB_FAIL(partition_location_.assign_with_only_readable_replica(partition_location, invalid_servers))) {
    LOG_WARN("fail to assign partition location with only readable replica",
        K(ret),
        K(partition_location),
        K(invalid_servers));
  }
  return ret;
}

ObPhyTableLocationInfo::ObPhyTableLocationInfo()
    : table_location_key_(OB_INVALID_ID),
      ref_table_id_(OB_INVALID_ID),
      direction_(MAX_DIR),
      phy_part_loc_info_list_(),
      splitted_range_list_(),
      duplicate_type_(ObDuplicateType::NOT_DUPLICATE)
{}

ObPhyTableLocationInfo::~ObPhyTableLocationInfo()
{}

void ObPhyTableLocationInfo::reset()
{
  table_location_key_ = OB_INVALID_ID;
  ref_table_id_ = OB_INVALID_ID;
  direction_ = MAX_DIR;
  phy_part_loc_info_list_.reset();
  splitted_range_list_.reset();
  duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
}

int ObPhyTableLocationInfo::assign(const ObPhyTableLocationInfo& other)
{
  int ret = OB_SUCCESS;
  table_location_key_ = other.table_location_key_;
  ref_table_id_ = other.ref_table_id_;
  direction_ = other.direction_;
  duplicate_type_ = other.duplicate_type_;
  if (OB_FAIL(phy_part_loc_info_list_.assign(other.phy_part_loc_info_list_))) {
    LOG_WARN("Failed to assign phy_part_loc_info_list", K(ret));
  } else if (OB_FAIL(splitted_range_list_.assign(other.splitted_range_list_))) {
    LOG_WARN("Failed to assign splitte range list", K(ret));
  }
  return ret;
}

int ObPhyTableLocationInfo::all_select_local_replica_or_leader(
    bool& is_on_same_server, ObAddr& same_server, const ObAddr& local_server)
{
  int ret = OB_SUCCESS;
  is_on_same_server = true;
  ObAddr first_server;
  ObReplicaLocation replica_location;
  int64_t replica_idx = OB_INVALID_INDEX;
  int64_t local_replica_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list_.count(); ++i) {
    replica_location.reset();
    replica_idx = OB_INVALID_INDEX;
    local_replica_idx = OB_INVALID_INDEX;
    ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list_.at(i);
    if (OB_FAIL(phy_part_loc_info.get_partition_location().get_strong_leader(replica_location, replica_idx))) {
      if (OB_LOCATION_LEADER_NOT_EXIST == ret &&
          !ObMultiClusterUtil::is_cluster_allow_strong_consistency_read_write(ref_table_id_)) {
        ret = OB_STANDBY_WEAK_READ_ONLY;
      }
      LOG_WARN("fail to get leader", K(ret), K(phy_part_loc_info.get_partition_location()));
    } else {
      if (phy_part_loc_info.is_server_in_replica(local_server, local_replica_idx)) {
        if (replica_idx != local_replica_idx) {
          LOG_TRACE("about to choose local replica rather than leader replica for duplicate table",
              K(replica_idx),
              K(local_replica_idx));
          replica_idx = local_replica_idx;
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing ...
    } else if (phy_part_loc_info.has_selected_replica() &&
               phy_part_loc_info.get_selected_replica_idx() != replica_idx) {

      ret = OB_NOT_SUPPORTED;
      LOG_WARN("selected replica is not leader", K(ret), K(phy_part_loc_info));
    } else if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(replica_idx))) {
      LOG_WARN("fail to set selected replica idx", K(ret), K(replica_idx), K(phy_part_loc_info));
    } else {
      if (0 == i) {
        first_server = replica_location.server_;
      } else if (first_server != replica_location.server_) {
        is_on_same_server = false;
      }
    }
  }
  if (OB_SUCC(ret) && is_on_same_server) {
    same_server = first_server;
  }
  return ret;
}

int ObPhyTableLocationInfo::all_select_leader(bool& is_on_same_server, ObAddr& same_server)
{
  int ret = OB_SUCCESS;
  is_on_same_server = true;
  ObAddr first_server;
  ObReplicaLocation replica_location;
  int64_t replica_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list_.count(); ++i) {
    replica_location.reset();
    replica_idx = OB_INVALID_INDEX;
    ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list_.at(i);
    if (OB_FAIL(phy_part_loc_info.get_partition_location().get_strong_leader(replica_location, replica_idx))) {
      if (OB_LOCATION_LEADER_NOT_EXIST == ret &&
          !ObMultiClusterUtil::is_cluster_allow_strong_consistency_read_write(ref_table_id_)) {
        ret = OB_STANDBY_WEAK_READ_ONLY;
      }
      LOG_WARN("fail to get leader", K(ret), K(phy_part_loc_info.get_partition_location()));
    } else if (phy_part_loc_info.has_selected_replica() &&
               phy_part_loc_info.get_selected_replica_idx() != replica_idx) {

      ret = OB_NOT_SUPPORTED;
      LOG_WARN("selected replica is not leader", K(ret), K(phy_part_loc_info));
    } else if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(replica_idx))) {
      LOG_WARN("fail to set selected replica idx", K(ret), K(replica_idx), K(phy_part_loc_info));
    } else {
      if (0 == i) {
        first_server = replica_location.server_;
      } else if (first_server != replica_location.server_) {
        is_on_same_server = false;
      }
    }
  }
  if (OB_SUCC(ret) && is_on_same_server) {
    same_server = first_server;
  }
  return ret;
}

int ObPhyTableLocationInfo::all_select_fixed_server(const ObAddr& fixed_server)
{
  int ret = OB_SUCCESS;
  ObReplicaLocation tmp_replica;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list_.count(); ++i) {
    ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list_.at(i);
    if (phy_part_loc_info.has_selected_replica()) {
      tmp_replica.reset();
      if (OB_FAIL(phy_part_loc_info.get_selected_replica(tmp_replica))) {
        LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
      } else if (fixed_server != tmp_replica.server_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("selected replica is not equal fixed server", K(ret), K(phy_part_loc_info), K(fixed_server));
      }
    } else {
      const ObIArray<ObRoutePolicy::CandidateReplica>& replica_loc_list =
          phy_part_loc_info.get_partition_location().get_replica_locations();
      bool has_selected = false;
      for (int64_t j = 0; OB_SUCC(ret) && !has_selected && j < replica_loc_list.count(); ++j) {
        if (ObReplicaTypeCheck::is_readable_replica(replica_loc_list.at(j).replica_type_)) {
          if (fixed_server == replica_loc_list.at(j).server_) {
            if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(j))) {
              LOG_WARN("fail to set selected replica idx", K(ret), K(j), K(phy_part_loc_info));
            } else {
              has_selected = true;
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(!has_selected)) {

        ret = OB_NO_READABLE_REPLICA;
        LOG_WARN("cannot found readable partition", K(ret), K(fixed_server));
      }
    }
  }
  return ret;
}

int ObPhyTableLocationInfo::is_server_in_replica(const ObPhyTableLocationInfo& l_phy_loc,
    const ObPhyPartitionLocationInfo& part_loc_info, bool& is_same, ObAddr& the_server, int64_t& new_idx)
{
  int ret = OB_SUCCESS;
  ObAddr first_server;
  ObReplicaLocation replica_location;
  const ObPhyPartitionLocationInfoIArray& left_locations = l_phy_loc.get_phy_part_loc_info_list();
  is_same = true;
  for (int64_t i = 0; is_same && OB_SUCC(ret) && i < left_locations.count(); ++i) {
    replica_location.reset();
    ObPhyPartitionLocationInfo& l_part_loc_info = const_cast<ObPhyPartitionLocationInfo&>(left_locations.at(i));
    if (OB_FAIL(l_part_loc_info.get_selected_replica(replica_location))) {
      LOG_WARN("fail to get leader", K(ret), K(l_part_loc_info.get_partition_location()));
    } else if (0 == i) {
      first_server = replica_location.server_;
      the_server = first_server;
    } else if (first_server != replica_location.server_) {
      is_same = false;
    }
  }
  if (OB_SUCC(ret) && is_same && !part_loc_info.is_server_in_replica(first_server, new_idx)) {
    is_same = false;
  }
  return ret;
}

bool ObPhyTableLocationInfo::is_server_in_replica(const ObAddr& server, ObIArray<int64_t>& new_replic_idxs) const
{
  int ret = OB_SUCCESS;
  bool found_flag = false;
  int64_t partition_location_count = get_partition_cnt();
  for (int64_t j = 0; OB_SUCC(ret) && j < partition_location_count; j++) {
    const ObPhyPartitionLocationInfo& phy_part_loc_info = get_phy_part_loc_info_list().at(j);
    if (!phy_part_loc_info.is_server_in_replica(server, new_replic_idxs.at(j))) {
      found_flag = false;
      break;
    } else {
      found_flag = true;
    }
  }
  return found_flag;
}

int ObPhyTableLocationInfo::reset_duplicate_table_best_replica(const ObIArray<int64_t>& new_replic_idxs)
{
  int ret = OB_SUCCESS;
  if (is_duplicate_table_not_in_dml()) {
    const ObPhyPartitionLocationInfoIArray& locations = get_phy_part_loc_info_list();
    if (locations.count() != new_replic_idxs.count()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(WARN, "location and replic idx count don't match", K(ret), K(locations), K(new_replic_idxs));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_replic_idxs.count(); ++i) {
      ObPhyPartitionLocationInfo& phy_part_loc_info = const_cast<ObPhyPartitionLocationInfo&>(locations.at(i));
      if (OB_FAIL(phy_part_loc_info.set_selected_replica_idx(new_replic_idxs.at(i)))) {
        SQL_PC_LOG(WARN, "failed to set selected replica idx", K(ret), K(i), K(new_replic_idxs.at(i)));
      }
    }
    SQL_PC_LOG(DEBUG, "reset duplicate table replic idx completed", K(ret), K(locations), K(new_replic_idxs));
  }
  return ret;
}

int ObPhyTableLocationInfo::set_direction(const ObOrderDirection& direction)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MAX_DIR != direction_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("direction_ has been set", K(ret), K(direction_), K(direction));
  } else {
    direction_ = direction;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
