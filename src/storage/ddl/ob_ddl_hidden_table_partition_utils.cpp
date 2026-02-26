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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_ddl_hidden_table_partition_utils.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace storage
{
int ObDDLHiddenTablePartitionUtils::check_support_partition_pruning(
    const ObTableSchema &orig_table_schema,
    const ObIArray<ObTabletID> &tablet_ids,
    const uint64_t tenant_id,
    int64_t &target_part_idx)
{
  int ret = OB_SUCCESS;
  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  const ObPartitionLevel part_level = orig_table_schema.get_part_level();
  const ObPartitionFuncType part_type = orig_table_schema.get_part_option().get_part_func_type();
  target_part_idx = OB_INVALID_INDEX;

  if (OB_UNLIKELY(tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty tablet_ids", KR(ret), K(tablet_ids));
  } else if ((cluster_version < MOCK_CLUSTER_VERSION_4_3_5_5)
      || (cluster_version >= CLUSTER_VERSION_4_4_0_0 && cluster_version < CLUSTER_VERSION_4_4_2_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3.5.5 or between 4.4.0.0 and 4.4.2.1 does not support direct load hidden table partition pruning", KR(ret), K(cluster_version));
  } else if (OB_UNLIKELY(!orig_table_schema.is_partitioned_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support partitioned table", KR(ret), K(orig_table_schema));
  } else if (OB_UNLIKELY((PARTITION_FUNC_TYPE_RANGE != part_type)
                      && (PARTITION_FUNC_TYPE_RANGE_COLUMNS != part_type)
                      && (PARTITION_FUNC_TYPE_LIST != part_type)
                      && (PARTITION_FUNC_TYPE_LIST_COLUMNS != part_type))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support range(columns)/list(columns) partitions", KR(ret), K(part_type));
  } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level) {
    const ObPartitionFuncType subpart_type = orig_table_schema.get_sub_part_option().get_part_func_type();
    if (OB_UNLIKELY(PARTITION_FUNC_TYPE_HASH != subpart_type)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support hash subpartitions", KR(ret), K(part_level), K(subpart_type));
    }
  }

  if (OB_SUCC(ret)) {
    // Check if all tablet_ids belong to the same partition
    int64_t first_part_idx = OB_INVALID_INDEX;
    ObSEArray<int64_t, 32> subpart_idx_array;
    for (int64_t i = 0; OB_SUCC(ret) && (i < tablet_ids.count()); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      int64_t part_idx = OB_INVALID_INDEX;
      int64_t subpart_idx = OB_INVALID_INDEX;

      if (OB_FAIL(orig_table_schema.get_part_idx_by_tablet(tablet_id, part_idx, subpart_idx))) {
        LOG_WARN("failed to get part idx by tablet", KR(ret), K(tablet_id), K(i));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == part_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part_idx", KR(ret), K(part_idx), K(tablet_id), K(i));
      } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level) {
        if (OB_UNLIKELY(OB_INVALID_INDEX == subpart_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid subpart_idx", KR(ret), K(part_level), K(subpart_idx), K(tablet_id), K(i));
        } else if (OB_UNLIKELY(has_exist_in_array(subpart_idx_array, subpart_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected duplicate subpart_idx", KR(ret), K(subpart_idx_array), K(subpart_idx), K(i));
        } else if (OB_FAIL(subpart_idx_array.push_back(subpart_idx))) {
          LOG_WARN("failed to push back subpart_idx", KR(ret), K(subpart_idx), K(i));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_INVALID_INDEX == first_part_idx) {
        first_part_idx = part_idx;
      } else if (OB_UNLIKELY(part_idx != first_part_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all tablet_ids must belong to the same partition",
            KR(ret), K(first_part_idx), K(part_idx), K(tablet_id), K(i));
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == first_part_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first_part_idx is invalid", KR(ret), K(first_part_idx));
    } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == part_level) {
      // Check if tablet_ids contain all subpartitions
      const ObPartition *first_part = nullptr;
      if (OB_FAIL(orig_table_schema.get_partition_by_partition_index(first_part_idx, CHECK_PARTITION_MODE_NORMAL, first_part))) {
        LOG_WARN("failed to get src part by idx", KR(ret), K(first_part_idx));
      } else if (OB_ISNULL(first_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null first_part", KR(ret), K(first_part_idx), KP(first_part));
      } else if (OB_UNLIKELY(subpart_idx_array.count() != first_part->get_subpartition_num())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpart_idx_array count is not equal to subpartition num",
            KR(ret), K(subpart_idx_array.count()), K(first_part->get_subpartition_num()));
      }
    }

    if (OB_SUCC(ret)) {
      target_part_idx = first_part_idx;
    }
  }
  return ret;
}

int ObDDLHiddenTablePartitionUtils::rebuild_table_schema_with_partition_pruning(
    const ObTableSchema &orig_table_schema,
    const int64_t target_part_idx,
    ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!orig_table_schema.is_partitioned_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support partitioned table", KR(ret), K(orig_table_schema));
  } else if (OB_UNLIKELY(OB_INVALID_INDEX == target_part_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target_part_idx is invalid", KR(ret), K(target_part_idx));
  } else {
    new_table_schema.reset_partition_array();
    const ObPartition *orig_part = nullptr;
    if (OB_FAIL(orig_table_schema.get_partition_by_partition_index(target_part_idx, CHECK_PARTITION_MODE_NORMAL, orig_part))) {
      LOG_WARN("failed to get partition by partition index", KR(ret), K(target_part_idx));
    } else if (OB_ISNULL(orig_part)) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("partition not exist", KR(ret), K(target_part_idx), KP(orig_part));
    } else {
      // rebuild the partition schema of hidden table by copying the partition of original table
      // Note: assign() will copy all subpartitions, which is what we want
      ObPartition new_part;
      if (OB_FAIL(new_part.assign(*orig_part))) {
        LOG_WARN("failed to assign partition", KR(ret), KPC(orig_part));
      } else if (OB_FALSE_IT(new_part.set_part_idx(0))) {
      } else if (OB_FAIL(new_table_schema.add_partition(new_part))) {
        LOG_WARN("failed to add partition to hidden table schema", KR(ret), K(new_part));
      } else {
        new_table_schema.set_part_num(1);
      }
    }
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase