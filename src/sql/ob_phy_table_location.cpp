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

#define USING_LOG_PREFIX SQL

#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

bool ObPhyTableLocation::compare_phy_part_loc_info_asc(
    const ObPhyPartitionLocationInfo*& left, const ObPhyPartitionLocationInfo*& right)
{
  bool is_less_than = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    LOG_ERROR("phy part loc info ptr is NULL", K(left), K(right));
  } else {
    is_less_than =
        left->get_partition_location().get_partition_id() < right->get_partition_location().get_partition_id();
  }
  return is_less_than;
}

bool ObPhyTableLocation::compare_phy_part_loc_info_desc(
    const ObPhyPartitionLocationInfo*& left, const ObPhyPartitionLocationInfo*& right)
{
  bool is_larger_than = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    LOG_ERROR("phy part loc info ptr is NULL", K(left), K(right));
  } else {
    is_larger_than =
        left->get_partition_location().get_partition_id() > right->get_partition_location().get_partition_id();
  }
  return is_larger_than;
}

OB_DEF_SERIALIZE(ObPhyTableLocation)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      table_location_key_,
      ref_table_id_,
      part_loc_list_,
      partition_location_list_,
      splitted_range_list_,
      duplicate_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPhyTableLocation)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      table_location_key_,
      ref_table_id_,
      part_loc_list_,
      partition_location_list_,
      splitted_range_list_,
      duplicate_type_);

  if (OB_SUCC(ret) && partition_location_list_.count() <= 0 && part_loc_list_.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_loc_list_.count(); ++i) {
      ObPartitionReplicaLocation part_rep_loc;
      if (OB_FAIL(part_rep_loc.assign_strong_leader(part_loc_list_.at(i)))) {
        LOG_WARN("fail to assign from patition replica location", K(ret), K(i), K(part_loc_list_.at(i)));
      } else if (OB_FAIL(partition_location_list_.push_back(part_rep_loc))) {
        LOG_WARN(
            "fail to push back partition location", K(ret), K(i), K(part_rep_loc), K(partition_location_list_.count()));
      }
    }
  }
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObSplittedRanges)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, ranges_, offsets_);
  return len;
}

OB_DEF_SERIALIZE(ObSplittedRanges)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, ranges_, offsets_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSplittedRanges)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, ranges_, offsets_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhyTableLocation)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      table_location_key_,
      ref_table_id_,
      part_loc_list_,
      partition_location_list_,
      splitted_range_list_,
      duplicate_type_);
  return len;
}

ObPhyTableLocation::ObPhyTableLocation()
    : table_location_key_(OB_INVALID_ID),
      ref_table_id_(OB_INVALID_ID),
      part_loc_list_(),
      partition_location_list_(),
      duplicate_type_(ObDuplicateType::NOT_DUPLICATE)
{}

void ObPhyTableLocation::reset()
{
  table_location_key_ = OB_INVALID_ID;
  ref_table_id_ = OB_INVALID_ID;
  duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
  part_loc_list_.reset();
  partition_location_list_.reset();
  splitted_range_list_.reset();
}

int ObPhyTableLocation::assign(const ObPhyTableLocation& other)
{
  int ret = OB_SUCCESS;
  table_location_key_ = other.table_location_key_;
  ref_table_id_ = other.ref_table_id_;
  duplicate_type_ = other.duplicate_type_;
  if (OB_FAIL(partition_location_list_.assign(other.partition_location_list_))) {
    LOG_WARN("Failed to assign partition location list", K(ret));
  } else if (OB_FAIL(splitted_range_list_.assign(other.splitted_range_list_))) {
    LOG_WARN("Failed to assign splitte range list", K(ret));
  }
  return ret;
}

// Find the partition copy information in the ObPhyTableLocation array according to ObPartitionKey
// @param is_retry_for_dup_tbl: Whether it is in the retry caused by the copy table,
//                              if = true, the copy type is not optimized
ObPartitionType ObPhyTableLocation::get_partition_type(
    const ObPartitionKey& pkey, const ObIArray<ObPhyTableLocation>& table_locations, bool is_retry_for_dup_tbl)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  bool is_dup = false;
  bool is_dup_not_dml = false;
  ObPartitionKey key;
  ObPartitionType type = ObPartitionType::NORMAL_PARTITION;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < table_locations.count(); ++i) {
    const ObPhyTableLocation& phy_tbl_loc = table_locations.at(i);
    is_dup = phy_tbl_loc.is_duplicate_table();
    is_dup_not_dml = phy_tbl_loc.is_duplicate_table_not_in_dml();
    if (is_dup) {
      const ObPartitionReplicaLocationIArray& part_locations = phy_tbl_loc.get_partition_location_list();
      int64_t M = part_locations.count();
      for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < M; ++j) {
        if (OB_FAIL(part_locations.at(j).get_partition_key(key))) {
          LOG_WARN("failed to get partition key", K(ret));
        } else if (pkey == key) {
          if (part_locations.at(j).get_replica_location().is_follower()) {
            // The copy of the copy table used for reading is set to follower
            // regardless of the copy type to facilitate the optimization of
            // the transaction layer
            type = ObPartitionType::DUPLICATE_FOLLOWER_PARTITION;
          } else if (is_dup_not_dml && false == is_retry_for_dup_tbl) {
            type = ObPartitionType::DUPLICATE_FOLLOWER_PARTITION;
          } else {
            type = ObPartitionType::DUPLICATE_LEADER_PARTITION;
          }
          is_found = true;
          break;
        }
      }
    }
  }
  LOG_DEBUG("get partition type",
      K(type),
      K(is_found),
      K(table_locations),
      K(pkey),
      K(is_dup),
      K(is_dup_not_dml),
      K(is_retry_for_dup_tbl));
  return type;
}

int ObPhyTableLocation::append(const ObPhyTableLocation& other)
{
  int ret = OB_SUCCESS;
  if (table_location_key_ != other.table_location_key_ || ref_table_id_ != other.ref_table_id_ ||
      duplicate_type_ != other.duplicate_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(table_location_key_),
        K(other.table_location_key_),
        K(ref_table_id_),
        K(other.ref_table_id_),
        K(other.duplicate_type_));
  } else if (OB_FAIL(append_array_no_dup(partition_location_list_, other.partition_location_list_))) {
    LOG_WARN("append array no dup failed", K(ret));
  }
  return ret;
}

int ObPhyTableLocation::add_partition_locations(const ObPhyTableLocationInfo& phy_location_info)
{
  int ret = OB_SUCCESS;
  ObPartitionReplicaLocation part_replica_loc;
  ObReplicaLocation replica_loc;
  const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = phy_location_info.get_phy_part_loc_info_list();
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
    part_replica_loc.reset();
    replica_loc.reset();
    const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(i);
    const ObOptPartLoc& part_loc = phy_part_loc_info.get_partition_location();
    if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
    } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
                   part_loc.get_partition_id(),
                   part_loc.get_partition_cnt(),
                   replica_loc,
                   part_loc.get_renew_time(),
                   part_loc.get_pg_key()))) {
      LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
    } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
      LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
    }
  }
  return ret;
}

int ObPhyTableLocation::assign_from_phy_table_loc_info(const ObPhyTableLocationInfo& other)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObPhyPartitionLocationInfo*, 2> phy_part_loc_info_sorted_list;
  ObPartitionReplicaLocation part_replica_loc;
  ObReplicaLocation replica_loc;
  table_location_key_ = other.get_table_location_key();
  ref_table_id_ = other.get_ref_table_id();
  const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list = other.get_phy_part_loc_info_list();
  if (other.get_direction() == UNORDERED) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
      part_replica_loc.reset();
      replica_loc.reset();
      const ObPhyPartitionLocationInfo& phy_part_loc_info = phy_part_loc_info_list.at(i);
      const ObOptPartLoc& part_loc = phy_part_loc_info.get_partition_location();
      if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
      } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
                     part_loc.get_partition_id(),
                     part_loc.get_partition_cnt(),
                     replica_loc,
                     part_loc.get_renew_time(),
                     part_loc.get_pg_key()))) {
        LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
      } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
        LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
      if (OB_FAIL(phy_part_loc_info_sorted_list.push_back(&phy_part_loc_info_list.at(i)))) {
        LOG_WARN("fail to push back phy part loc info", K(ret), K(phy_part_loc_info_list.at(i)), K(i));
      }
    }
    if (OB_SUCC(ret) && phy_part_loc_info_sorted_list.count() > 0) {
      // Output the partition order in the specified direction order.
      // Currently, the partition order is dependent on the range order.
      // After complex situations such as splitting are done
      // in the future, part_mgr(schema) will be required to sort
      if (is_ascending_direction(other.get_direction())) {
        std::sort(&phy_part_loc_info_sorted_list.at(0),
            &phy_part_loc_info_sorted_list.at(0) + phy_part_loc_info_sorted_list.count(),
            ObPhyTableLocation::compare_phy_part_loc_info_asc);
      } else if (is_descending_direction(other.get_direction())) {
        std::sort(&phy_part_loc_info_sorted_list.at(0),
            &phy_part_loc_info_sorted_list.at(0) + phy_part_loc_info_sorted_list.count(),
            ObPhyTableLocation::compare_phy_part_loc_info_desc);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected order", K(ret), K(other.get_direction()), K(other));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_sorted_list.count(); ++i) {
      part_replica_loc.reset();
      replica_loc.reset();
      const ObPhyPartitionLocationInfo* phy_part_loc_info = phy_part_loc_info_sorted_list.at(i);
      if (OB_ISNULL(phy_part_loc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("phy part loc info is NULL", K(ret), K(i));
      } else {
        const ObOptPartLoc& part_loc = phy_part_loc_info->get_partition_location();
        if (OB_FAIL(phy_part_loc_info->get_selected_replica(replica_loc))) {
          LOG_WARN("fail to get selected replica", K(ret), K(*phy_part_loc_info));
        } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
                       part_loc.get_partition_id(),
                       part_loc.get_partition_cnt(),
                       replica_loc,
                       part_loc.get_renew_time(),
                       part_loc.get_pg_key()))) {
          LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
        } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
          LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(splitted_range_list_.assign(other.get_splitted_ranges()))) {
      LOG_WARN("assign splitted range list failed", K(ret));
    } else {
      set_duplicate_type(other.get_duplicate_type());
    }
  }
  return ret;
}

int ObPhyTableLocation::add_partition_location(const ObIArray<ObPartitionReplicaLocation>& partition_locations)
{
  int ret = OB_SUCCESS;
  int64_t N = partition_locations.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(add_partition_location(partition_locations.at(i)))) {
      LOG_WARN("Fail to add partition_location to list");
    }
  }
  return ret;
}

int ObPhyTableLocation::add_partition_location(const ObPartitionReplicaLocation& partition_location)
{
  int ret = OB_SUCCESS;
  bool duplicated = false;
  for (int64_t i = 0; OB_SUCC(ret) && !duplicated && i < partition_location_list_.count(); ++i) {
    if (partition_location.get_table_id() == partition_location_list_.at(i).get_table_id() &&
        partition_location.get_partition_id() == partition_location_list_.at(i).get_partition_id()) {
      duplicated = true;
    }
  }
  if (!duplicated) {
    if (OB_FAIL(partition_location_list_.push_back(partition_location))) {
      LOG_WARN("Fail to add partition location to list", K(ret), K(partition_location));
    }
  }
  return ret;
}

const ObPartitionReplicaLocation* ObPhyTableLocation::get_part_replic_by_part_id(int64_t part_id) const
{
  const ObPartitionReplicaLocation* ret = NULL;
  for (int64_t i = 0; i < partition_location_list_.count(); ++i) {
    if (partition_location_list_.at(i).get_partition_id() == part_id) {
      ret = &partition_location_list_.at(i);
      break;
    }
  }
  return ret;
}

const share::ObPartitionReplicaLocation* ObPhyTableLocation::get_part_replic_by_index(int64_t part_idx) const
{
  const ObPartitionReplicaLocation* ret = NULL;
  if (part_idx >= 0 && part_idx < partition_location_list_.count()) {
    ret = &partition_location_list_.at(part_idx);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
