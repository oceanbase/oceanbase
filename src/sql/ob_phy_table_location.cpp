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

#include "ob_phy_table_location.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

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

}/* ns sql*/
}/* ns oceanbase */
