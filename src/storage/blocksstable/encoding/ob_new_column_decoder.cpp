/**
 * Copyright (c) 2024 OceanBase
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

#include "ob_new_column_decoder.h"
#include "storage/access/ob_aggregate_base.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObNewColumnCommonDecoder::decode(
    const ObColumnParam *col_param,
    common::ObIAllocator *allocator,
    common::ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  // it is sure that col_param can not be nullptr here
  const ObObj &def_cell = col_param->get_orig_default_value();
  if (OB_FAIL(datum.from_obj(def_cell))) {
    LOG_WARN("Failed to transfer obj to datum", K(ret), K(def_cell));
  } else if (def_cell.is_lob_storage() && !def_cell.is_null()) {
    // lob def value must have no lob header when not null
    // When do lob decode, should add lob header for default value
    ObString data = datum.get_string();
    ObString out;
    if (OB_FAIL(ObLobManager::fill_lob_header(*allocator, data, out))) {
      LOG_WARN("failed to fill lob header for column", K(ret), K(def_cell), K(data));
    } else {
      datum.set_string(out);
    }
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] decode new column", K(def_cell), K(datum), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::batch_decode(
    const ObColumnParam *col_param,
    common::ObIAllocator *allocator,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    if (OB_FAIL(decode(col_param, allocator, datums[i]))) {
      LOG_WARN("Failed to decode new added datum", K(ret), K(col_param->get_orig_default_value()));
    }
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] batch decode", K(col_param->get_orig_default_value()), K(row_cap), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::decode_vector(ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector_ctx.default_datum_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null datum", K(ret));
  } else {
#define FOREACH_SET_VEC(FORMAT)                                                                     \
  for (int64_t idx = 0; idx < vector_ctx.row_cap_; ++idx) {                                         \
    const int64_t vec_idx = idx + vector_ctx.vec_offset_;                                           \
    static_cast<FORMAT *>(vector_ctx.get_vector())->set_datum(vec_idx, *vector_ctx.default_datum_); \
  }
    if (VEC_DISCRETE == vector_ctx.get_format()) {
      FOREACH_SET_VEC(ObDiscreteFormat)
    } else {
      FOREACH_SET_VEC(ObFixedLengthBase)
    }
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] decode vector", KPC(vector_ctx.default_datum_), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::pushdown_operator(
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  bool filtered = false;
  ObStorageDatum *default_datum = const_cast<ObStorageDatum *>(&filter.get_default_datums().at(0));
  if (OB_FAIL(blocksstable::ObIMicroBlockReader::filter_white_filter(filter, *default_datum, filtered))) {
    LOG_WARN("Failed to filter row with white filter", K(ret), K(filter), K(default_datum));
  } else if (!filtered) {
    result_bitmap.bit_not();
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] pushdown white filter", KPC(default_datum), K(filtered), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::pushdown_operator(
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  bool filtered = false;
  sql::ObPhysicalFilterExecutor *black_filter = static_cast<sql::ObPhysicalFilterExecutor *>(&filter);
  ObStorageDatum *default_datum = const_cast<ObStorageDatum *>(&filter.get_default_datums().at(0));
  if (OB_FAIL(black_filter->filter(default_datum, black_filter->get_col_count(), *pd_filter_info.skip_bit_, filtered))) {
    LOG_WARN("Failed to filter row with black filter", K(ret), K(black_filter));
  } else if (!filtered) {
    result_bitmap.bit_not();
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] pushdown black filter", KPC(default_datum), K(filtered), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::get_null_count(
    const ObColumnParam *col_param,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  if (col_param->get_orig_default_value().is_null()) {
    null_count = row_cap;
  } else {
    null_count = 0;
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] get null count", K(col_param->get_orig_default_value()), K(null_count), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::read_distinct(
    const ObColumnParam *col_param,
    storage::ObGroupByCellBase &group_by_cell) const
{
  int ret = OB_SUCCESS;
  common::ObDatum *datums = group_by_cell.get_group_by_col_datums_to_fill();
  if (OB_FAIL(datums[0].from_obj(col_param->get_orig_default_value(), ObDatum::get_obj_datum_map_type(col_param->get_orig_default_value().get_type())))) {
    LOG_WARN("Failed to from storage datum", K(ret), K(col_param->get_orig_default_value()));
  } else {
    group_by_cell.set_distinct_cnt(1);
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] read distinct", K(group_by_cell.get_group_by_col_offset()), KPC(datums), K(lbt()));
  return ret;
}

int ObNewColumnCommonDecoder::read_reference(
    const int64_t row_cap,
    storage::ObGroupByCellBase &group_by_cell) const
{
  int ret = OB_SUCCESS;
  uint32_t *ref_buf = group_by_cell.get_refs_buf();
  MEMSET(ref_buf, 0, sizeof(uint32_t) * row_cap);
  group_by_cell.set_ref_cnt(row_cap);
  LOG_DEBUG("[NEW_COLUMN_DECODE] read refrence", K(row_cap), K(group_by_cell.get_group_by_col_offset()), K(lbt()));
  return ret;
}

} // end of namespace oceanbase
} // end of namespace oceanbase
