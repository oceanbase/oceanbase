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
#include "ob_part_mgr_ad.h"
#include "common/ob_range.h"
#include "common/row/ob_row.h"
#include "share/part/ob_part_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
// for hash/key/range_func part or insert statement
int ObPartMgrAD::get_part(ObPartMgr* part_mgr, const int64_t table_id, const ObPartitionLevel part_level,
    const ObPartitionFuncType part_type, const bool insert_or_replace, const int64_t p_id, const ObObj& value,
    ObIArray<int64_t>& partition_ids, int64_t* part_idx)
{
  int ret = common::OB_SUCCESS;
  ObRowkey rowkey(const_cast<ObObj*>(&value), 1);
  ObNewRange range;
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(range.build_range(table_id, rowkey))) {
    LOG_WARN("Failed to build range", K(ret));
  } else if (OB_FAIL(get_part(
                 part_mgr, table_id, part_level, part_type, insert_or_replace, p_id, range, partition_ids, part_idx))) {
    LOG_WARN("Failed to get part", K(ret));
  } else {
  }  // do nothing

  return ret;
}

int ObPartMgrAD::get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
    const share::schema::ObPartitionLevel part_level, const share::schema::ObPartitionFuncType part_type,
    const bool insert_or_replace, const int64_t p_id, const common::ObNewRow& row,
    common::ObIArray<int64_t>& partition_ids, int64_t* part_idx)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> part_ids;
  if (OB_ISNULL(part_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part mgr should not be NULL", K(ret));
  } else if (OB_FAIL(part_mgr->get_part(table_id, part_level, p_id, row, part_ids))) {
    LOG_WARN("Failed to get part from part_mgr", K(ret));
  } else if (0 == part_ids.count()) {
    if (insert_or_replace) {  // For Insert or replace stmt, if no partition, report error
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_USER_WARN(OB_NO_PARTITION_FOR_GIVEN_VALUE);
    } else if (share::schema::is_hash_like_part(part_type)) {  // For other stmt
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("For hash part type, should not have no part_id", K(ret), K(part_type));
    } else {
    }  // do nothing
  } else if (1 != part_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected that one value get multi ids", K(ret), K(part_ids), K(part_level), K(p_id), K(row));
  } else {
    int64_t part_id = part_ids.at(0);
    if (PARTITION_LEVEL_TWO == part_level) {
      if (-1 == p_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("PARTITION_LEVEL_TWO, p_id should not be invalid", K(ret));
      } else {
        part_id = generate_phy_part_id(p_id, part_id, part_level);  // get phy_part_id
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_var_to_array_no_dup(partition_ids, part_id, part_idx))) {
        LOG_WARN("Failed to add var to array no dup", K(ret));
      }
    }
  }
  return ret;
}

/////////The for range range partition must only contain column. If there are multiple columns, range represents the
/// column vector in the schema//////
int ObPartMgrAD::get_part(ObPartMgr* part_mgr, const int64_t table_id, const ObPartitionLevel part_level,
    const int64_t p_id, ObOrderDirection direction, const common::ObIArray<common::ObNewRange*>& ranges,
    common::ObIArray<int64_t>& part_ids)
{
  int ret = common::OB_SUCCESS;
  ObSEArray<int64_t, OB_DEFAULT_SE_ARRAY_COUNT> ids;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < ranges.count(); ++idx) {
    ids.reuse();
    if (OB_FAIL(part_mgr->get_part(
            table_id, part_level, p_id, *(ranges.at(idx)), !is_ascending_direction(direction), ids))) {
      LOG_WARN("Get part id error", K(ret));
    } else if (OB_FAIL(append_array_no_dup(part_ids, ids))) {
      LOG_WARN("Failed to append array", K(ret));
    } else {
    }
  }
  return ret;
}

int ObPartMgrAD::get_all_part(ObPartMgr* part_mgr, const int64_t table_id, const ObPartitionLevel part_level,
    const int64_t p_id, ObOrderDirection direction, ObIArray<int64_t>& part_ids)
{
  int ret = common::OB_SUCCESS;
  ObNewRange range;
  range.table_id_ = table_id;
  range.set_whole_range();
  if (OB_ISNULL(part_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part mgr should not be NULL", K(ret));
  } else if (OB_FAIL(
                 part_mgr->get_part(table_id, part_level, p_id, range, !is_ascending_direction(direction), part_ids))) {
    LOG_WARN("Failed to get part from part mgr", K(ret));
  } else {
  }

  return ret;
}

int ObPartMgrAD::get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
    const share::schema::ObPartitionLevel part_level, const share::schema::ObPartitionFuncType part_type,
    const bool insert_or_replace, const int64_t p_id, const common::ObNewRange& range,
    common::ObIArray<int64_t>& partition_ids, int64_t* part_idx)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> part_ids;
  if (OB_ISNULL(part_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part mgr should not be NULL", K(ret));
  } else if (OB_FAIL(part_mgr->get_part(table_id, part_level, p_id, range, false, part_ids))) {
    LOG_WARN("Failed to get part from part_mgr", K(ret));
  } else if (0 == part_ids.count()) {
    if (insert_or_replace) {  // For Insert or replace stmt, if no partition, report error
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_USER_WARN(OB_NO_PARTITION_FOR_GIVEN_VALUE);
    } else if (PARTITION_FUNC_TYPE_RANGE != part_type && PARTITION_FUNC_TYPE_RANGE_COLUMNS != part_type &&
               PARTITION_FUNC_TYPE_LIST != part_type &&
               PARTITION_FUNC_TYPE_LIST_COLUMNS != part_type) {  // For other stmt, range partition ignore this
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("For hash part type, should not have no part_id", K(ret));
    } else {
    }  // do nothing
  } else if (1 != part_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected that one value get multi ids", K(ret), K(part_ids), K(part_level), K(p_id), K(range));
  } else {
    int64_t part_id = part_ids.at(0);
    if (PARTITION_LEVEL_TWO == part_level) {
      if (-1 == p_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("PARTITION_LEVEL_TWO, p_id should not be invalid", K(ret));
      } else {
        part_id = generate_phy_part_id(p_id, part_id, part_level);  // get phy_part_id
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_var_to_array_no_dup(partition_ids, part_id, part_idx))) {
        LOG_WARN("Failed to add var to array no dup", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
