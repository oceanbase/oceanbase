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
#include "observer/ob_server_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

bool ObPhyTableLocation::compare_phy_part_loc_info_asc(const ObCandiTabletLoc *&left,
                                                       const ObCandiTabletLoc *&right)
{
  bool is_less_than = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "phy part loc info ptr is NULL", K(left), K(right));
  } else {
    is_less_than = left->get_partition_location().get_partition_id()
        < right->get_partition_location().get_partition_id();
  }
  return is_less_than;
}

bool ObPhyTableLocation::compare_phy_part_loc_info_desc(const ObCandiTabletLoc *&left,
                                                        const ObCandiTabletLoc *&right)
{
  bool is_larger_than = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "phy part loc info ptr is NULL", K(left), K(right));
  } else {
    is_larger_than = left->get_partition_location().get_partition_id()
        > right->get_partition_location().get_partition_id();
  }
  return is_larger_than;
}

//OB_SERIALIZE_MEMBER(ObPhyTableLocation, table_location_key_,
//                    ref_table_id_, part_loc_list_, partition_location_list_);

OB_DEF_SERIALIZE(ObPhyTableLocation)
{
  int ret = OB_SUCCESS;
  static ObPartitionLocationSEArray unused_part_loc_list;
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_location_key_,
              ref_table_id_,
              unused_part_loc_list,
              partition_location_list_,
              duplicate_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPhyTableLocation)
{
  int ret = OB_SUCCESS;
  ObPartitionLocationSEArray unused_part_loc_list;
  LST_DO_CODE(OB_UNIS_DECODE,
              table_location_key_,
              ref_table_id_,
              unused_part_loc_list,
              partition_location_list_,
              duplicate_type_);

  // build local index cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_build_location_idx_map())) {
      LOG_WARN("fail build location idx map", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhyTableLocation)
{
  int64_t len = 0;
  static ObPartitionLocationSEArray unused_part_loc_list;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_location_key_,
              ref_table_id_,
              unused_part_loc_list,
              partition_location_list_,
              duplicate_type_);
  return len;
}

ObPhyTableLocation::ObPhyTableLocation()
  : table_location_key_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    partition_location_list_(),
    duplicate_type_(ObDuplicateType::NOT_DUPLICATE)
{
}

void ObPhyTableLocation::reset()
{
  table_location_key_ = OB_INVALID_ID;
  ref_table_id_ = OB_INVALID_ID;
  duplicate_type_ = ObDuplicateType::NOT_DUPLICATE;
  partition_location_list_.reset();
}

int ObPhyTableLocation::assign(const ObPhyTableLocation &other)
{
  int ret = OB_SUCCESS;
  table_location_key_ = other.table_location_key_;
  ref_table_id_ = other.ref_table_id_;
  duplicate_type_ = other.duplicate_type_;
  if (OB_FAIL(partition_location_list_.assign(other.partition_location_list_))) {
    LOG_WARN("Failed to assign partition location list", K(ret));
  }
  return ret;
}

int ObPhyTableLocation::append(const ObPhyTableLocation &other)
{
  int ret = OB_SUCCESS;
  if (table_location_key_ != other.table_location_key_
      || ref_table_id_ != other.ref_table_id_
      || duplicate_type_ != other.duplicate_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_location_key_), K(other.table_location_key_),
             K(ref_table_id_), K(other.ref_table_id_), K(other.duplicate_type_));
  } else if (OB_FAIL(append_array_no_dup(partition_location_list_, other.partition_location_list_))) {
    LOG_WARN("append array no dup failed", K(ret));
  }
  return ret;
}

int ObPhyTableLocation::add_partition_locations(const ObCandiTableLoc &phy_location_info)
{
  int ret = OB_SUCCESS;
  ObPartitionReplicaLocation part_replica_loc;
  ObReplicaLocation replica_loc;
  const ObCandiTabletLocIArray &phy_part_loc_info_list = phy_location_info.get_phy_part_loc_info_list();
 // for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
 //   part_replica_loc.reset();
 //   replica_loc.reset();
 //   const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(i);
 //   const ObOptTabletLoc &part_loc = phy_part_loc_info.get_partition_location();
 //   if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_loc))) {
 //     LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
 //   } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
 //                                              part_loc.get_partition_id(),
 //                                              part_loc.get_partition_cnt(),
 //                                              replica_loc,
 //                                              part_loc.get_renew_time(),
 //                                              part_loc.get_pg_key()))) {
 //     LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
 //   } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
 //     LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
 //   }
 // }
  return ret;
}

int ObPhyTableLocation::assign_from_phy_table_loc_info(const ObCandiTableLoc &other)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObCandiTabletLoc*, 2> phy_part_loc_info_sorted_list;
  ObPartitionReplicaLocation part_replica_loc;
  ObReplicaLocation replica_loc;
  table_location_key_ = other.get_table_location_key();
  ref_table_id_ = other.get_ref_table_id();
  const ObCandiTabletLocIArray &phy_part_loc_info_list = other.get_phy_part_loc_info_list();
 // if (other.get_direction() == UNORDERED) {
 //   if (OB_FAIL(partition_location_list_.reserve(phy_part_loc_info_list.count()))) {
 //     LOG_WARN("fail reserve memory", K(ret), K(phy_part_loc_info_list.count()));
 //   }
 //   for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
 //     part_replica_loc.reset();
 //     replica_loc.reset();
 //     const ObCandiTabletLoc &phy_part_loc_info = phy_part_loc_info_list.at(i);
 //     const ObOptTabletLoc &part_loc = phy_part_loc_info.get_partition_location();
 //     if (OB_FAIL(phy_part_loc_info.get_selected_replica(replica_loc))) {
 //       LOG_WARN("fail to get selected replica", K(ret), K(phy_part_loc_info));
 //     } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
 //                                                part_loc.get_partition_id(),
 //                                                part_loc.get_partition_cnt(),
 //                                                replica_loc,
 //                                                part_loc.get_renew_time(),
 //                                                part_loc.get_pg_key()))) {
 //       LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
 //     } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
 //       LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
 //     }
 //   }
 // } else {
 //   if (OB_FAIL(phy_part_loc_info_sorted_list.reserve(phy_part_loc_info_list.count()))) {
 //     LOG_WARN("fail reserve memory", K(ret), K(phy_part_loc_info_list.count()));
 //   } else if (OB_FAIL(partition_location_list_.reserve(phy_part_loc_info_list.count()))) {
 //     LOG_WARN("fail reserve memory", K(ret), K(phy_part_loc_info_list.count()));
 //   }
 //   for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_list.count(); ++i) {
 //     if (OB_FAIL(phy_part_loc_info_sorted_list.push_back(&phy_part_loc_info_list.at(i)))) {
 //       LOG_WARN("fail to push back phy part loc info", K(ret), K(phy_part_loc_info_list.at(i)), K(i));
 //     }
 //   }
 //   if (OB_SUCC(ret) && phy_part_loc_info_sorted_list.count() > 0) {
 //     //按照指定的direction顺序输出partition顺序.
 //     //目前依赖partition序代表range序.以后做分裂等复杂情况后，就需要依赖part_mgr(schema)来排序
 //     if (is_ascending_direction(other.get_direction())) {
 //       lib::ob_sort(&phy_part_loc_info_sorted_list.at(0),
 //                 &phy_part_loc_info_sorted_list.at(0) + phy_part_loc_info_sorted_list.count(),
 //                 ObPhyTableLocation::compare_phy_part_loc_info_asc);
 //     } else if (is_descending_direction(other.get_direction())) {
 //       lib::ob_sort(&phy_part_loc_info_sorted_list.at(0),
 //                 &phy_part_loc_info_sorted_list.at(0) + phy_part_loc_info_sorted_list.count(),
 //                 ObPhyTableLocation::compare_phy_part_loc_info_desc);
 //     } else {
 //       ret = OB_ERR_UNEXPECTED;
 //       LOG_ERROR("unexpected order", K(ret), K(other.get_direction()), K(other));
 //     }
 //   }
 //   for (int64_t i = 0; OB_SUCC(ret) && i < phy_part_loc_info_sorted_list.count(); ++i) {
 //     part_replica_loc.reset();
 //     replica_loc.reset();
 //     const ObCandiTabletLoc *phy_part_loc_info = phy_part_loc_info_sorted_list.at(i);
 //     if (OB_ISNULL(phy_part_loc_info)) {
 //       ret = OB_ERR_UNEXPECTED;
 //       LOG_ERROR("phy part loc info is NULL", K(ret), K(i));
 //     } else {
 //       const ObOptTabletLoc &part_loc = phy_part_loc_info->get_partition_location();
 //       if (OB_FAIL(phy_part_loc_info->get_selected_replica(replica_loc))) {
 //         LOG_WARN("fail to get selected replica", K(ret), K(*phy_part_loc_info));
 //       } else if (OB_FAIL(part_replica_loc.assign(part_loc.get_table_id(),
 //                                                  part_loc.get_partition_id(),
 //                                                  part_loc.get_partition_cnt(),
 //                                                  replica_loc,
 //                                                  part_loc.get_renew_time(),
 //                                                  part_loc.get_pg_key()))) {
 //         LOG_WARN("fail to assin part replica loc", K(ret), K(part_loc), K(replica_loc));
 //       } else if (OB_FAIL(partition_location_list_.push_back(part_replica_loc))) {
 //         LOG_WARN("fail to push back part replica loc", K(ret), K(part_replica_loc));
 //       }
 //     }
 //   }
 // }

  if (OB_SUCC(ret)) {
    set_duplicate_type(other.get_duplicate_type());
  }
  return ret;
}

int ObPhyTableLocation::add_partition_location(
    const ObIArray<ObPartitionReplicaLocation> &partition_locations)
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

int ObPhyTableLocation::add_partition_location(
    const ObPartitionReplicaLocation &partition_location)
{
  int ret = OB_SUCCESS;
  bool duplicated = false;
  for (int64_t i = 0; OB_SUCC(ret) && !duplicated
       && i < partition_location_list_.count(); ++i) {
    if (partition_location.get_table_id()
        == partition_location_list_.at(i).get_table_id()
        && partition_location.get_partition_id()
        == partition_location_list_.at(i).get_partition_id()) {
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

const ObPartitionReplicaLocation *ObPhyTableLocation::get_part_replic_by_part_id(int64_t part_id) const
{
  const ObPartitionReplicaLocation *ret = NULL;
  int64_t i = 0;
  if (OB_SUCCESS == get_location_idx_by_part_id(part_id, i)) {
    ret = &partition_location_list_.at(i);
  }
  return ret;
}

const share::ObPartitionReplicaLocation *ObPhyTableLocation::get_part_replic_by_index(int64_t part_idx) const
{
  const ObPartitionReplicaLocation *ret = NULL;
  if (part_idx >= 0 && part_idx < partition_location_list_.count()) {
    ret = &partition_location_list_.at(part_idx);
  }
  return ret;
}

int ObPhyTableLocation::try_build_location_idx_map()
{
  int ret = OB_SUCCESS;
  // if more than FAST_LOOKUP_LOC_IDX_SIZE_THRES partitions in a 'Task/SQC/Worker',
  // build a hash map to accelerate lookup speed
  if (OB_UNLIKELY(partition_location_list_.count() > FAST_LOOKUP_LOC_IDX_SIZE_THRES)) {
    if (OB_FAIL(location_idx_map_.create(
                partition_location_list_.count(), "LocIdxPerfMap", "LocIdxPerfMap"))) {
      LOG_WARN("fail init location idx map", K(ret));
    } else {
      ARRAY_FOREACH(partition_location_list_, idx) {
        // ObPartitionReplicaLocation
        auto &item = partition_location_list_.at(idx);
        if (OB_FAIL(location_idx_map_.set_refactored(item.get_partition_id(), idx))) {
          LOG_WARN("fail set value to map", K(ret), K(item));
        }
      }
    }
  }
  return ret;
}

int ObPhyTableLocation::get_location_idx_by_part_id(int64_t part_id, int64_t &loc_idx) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (OB_UNLIKELY(location_idx_map_.size() > 0)) {
    ret = location_idx_map_.get_refactored(part_id, loc_idx);
    LOG_DEBUG("lookup location index from map", K(partition_location_list_.count()),
             K(part_id), K(loc_idx), K(table_location_key_), K(ref_table_id_));
  } else {
    // single part table scan
    for (int64_t idx = 0; idx < partition_location_list_.count(); ++idx) {
      if (partition_location_list_.at(idx).get_partition_id() == part_id) {
        loc_idx = idx;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

// The reason for not using the remove interface is that
// the remove interface will not free the memory of the removed node.
// If the interface is called multiple times for anti-corruption,
// the problem of memory bloat will occur.
int ObPhyTableLocation::erase_partition_location(int64_t partition_id)
{
  int ret = OB_SUCCESS;
  ObPartitionReplicaLocationSEArray tmp_location_list;
  int64_t N = partition_location_list_.count();
  for (int64_t idx = N - 1; OB_SUCC(ret) && idx >= 0; --idx) {
    if (partition_location_list_.at(idx).get_partition_id() != partition_id) {
      if (OB_FAIL(tmp_location_list.push_back(partition_location_list_.at(idx)))) {
        LOG_WARN("add partition location from list failed", K(ret), K(idx));
      }
    } else {
      LOG_DEBUG("remove partition succ", K(idx), K(partition_id));
    }
  }

  if (OB_SUCC(ret)) {
    partition_location_list_.reset();
    if (OB_FAIL(partition_location_list_.assign(tmp_location_list))) {
      LOG_WARN("fail to assign partition location list", K(ret));
    }
  }

  if (OB_SUCC(ret) && location_idx_map_.size() > 0) {
    location_idx_map_.clear();
  }
  return ret;
}
}/* ns sql*/
}/* ns oceanbase */
