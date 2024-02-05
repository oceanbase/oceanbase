/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "share/schema/ob_schema_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"

namespace oceanbase
{
namespace blocksstable
{

int ObSkipIndexColMeta::append_skip_index_meta(
    const share::schema::ObSkipIndexColumnAttr &skip_idx_attr,
    const int64_t col_idx,
    common::ObIArray<ObSkipIndexColMeta> &skip_idx_metas)
{
  int ret = OB_SUCCESS;
  bool has_null_count_column = false;
  if (OB_UNLIKELY(!skip_idx_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid skip index attribute", K(ret), K(skip_idx_attr));
  } else if (skip_idx_attr.has_min_max()) {
    if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MIN)))) {
      STORAGE_LOG(WARN, "failed to push min skip idx meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MAX)))) {
      STORAGE_LOG(WARN, "failed to push max skip idx meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_NULL_COUNT)))) {
      STORAGE_LOG(WARN, "failed to push null count skip index meta", K(ret));
    } else {
      has_null_count_column = true;
    }
  }

  if (OB_SUCC(ret) && skip_idx_attr.has_sum()) {
    if (!has_null_count_column
        && OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_NULL_COUNT)))) {
      STORAGE_LOG(WARN, "failed to push null count skip index meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_SUM)))) {
      STORAGE_LOG(WARN, "failed to push sum skip index meta", K(ret));
    }
  }
  return ret;
}

int ObSkipIndexColMeta::calc_skip_index_maximum_size(
    const share::schema::ObSkipIndexColumnAttr &skip_idx_attr,
    const ObObjType obj_type,
    const int16_t precision,
    int64_t &max_size)
{
  int ret = OB_SUCCESS;
  max_size = 0;
  const ObObjDatumMapType datum_type = ObDatum::get_obj_datum_map_type(obj_type);
  if (OB_UNLIKELY(datum_type >= OBJ_DATUM_MAPPING_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column type", K(ret), K(obj_type), K(datum_type));
  } else {
    int64_t normal_agg_column_cnt = 0;
    int64_t sum_column_cnt = 0;
    bool has_null_count_column = false;
    if (skip_idx_attr.has_min_max()) {
      normal_agg_column_cnt += 2;
      has_null_count_column = true;
    }
    if (skip_idx_attr.has_sum()) {
      sum_column_cnt += 1;
      has_null_count_column = true;
    }
    const int64_t null_count_column_cnt = has_null_count_column ? 1 : 0;
    uint32_t data_type_upper_size = 0;
    uint32_t null_count_upper_size = 0;
    uint32_t sum_store_size = 0;
    if (OB_FAIL(get_skip_index_store_upper_size(datum_type, precision, data_type_upper_size))) {
      LOG_WARN("failed to get datum stored upper size", K(ret), K(datum_type));
    } else if (OB_FAIL(get_skip_index_store_upper_size(NULL_CNT_COL_TYPE, 0, null_count_upper_size))) {
      LOG_WARN("failed to get null count col upper size", K(ret), K(datum_type));
    } else if (can_agg_sum(obj_type) && OB_FAIL(get_sum_store_size(obj_type, sum_store_size))) {
      LOG_WARN("failed to get sum store size", K(ret), K(obj_type));
    } else {
      max_size = normal_agg_column_cnt * data_type_upper_size + sum_column_cnt * sum_store_size
          + null_count_column_cnt * null_count_upper_size;
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase