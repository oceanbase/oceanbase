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
    const bool enable_precise_agg,
    const share::schema::ObSkipIndexColumnAttr &skip_idx_attr,
    const int64_t col_idx,
    common::ObIArray<ObSkipIndexColMeta> &skip_idx_metas)
{
  int ret = OB_SUCCESS;
  bool has_null_count_column = false;
  bool has_min_max_column = false;
  if (OB_UNLIKELY(!skip_idx_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid skip index attribute", K(ret), K(skip_idx_attr));
  } else if (skip_idx_attr.has_min_max() && enable_precise_agg) {
    if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MIN)))) {
      STORAGE_LOG(WARN, "failed to push min skip idx meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MAX)))) {
      STORAGE_LOG(WARN, "failed to push max skip idx meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_NULL_COUNT)))) {
      STORAGE_LOG(WARN, "failed to push null count skip index meta", K(ret));
    } else {
      has_null_count_column = true;
      has_min_max_column = true;
    }
  }

  if (OB_SUCC(ret) && skip_idx_attr.has_loose_min_max() && !has_min_max_column) {
    if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MIN)))) {
      STORAGE_LOG(WARN, "failed to push min skip idx meta for loose min", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_MAX)))) {
      STORAGE_LOG(WARN, "failed to push max skip idx meta for loose max", K(ret));
    } else {
      has_min_max_column = true;
    }
  }

  if (OB_SUCC(ret) && skip_idx_attr.has_sum() && enable_precise_agg) {
    if (!has_null_count_column
        && OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_NULL_COUNT)))) {
      STORAGE_LOG(WARN, "failed to push null count skip index meta", K(ret));
    } else if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_SUM)))) {
      STORAGE_LOG(WARN, "failed to push sum skip index meta", K(ret));
    }
  }

  if (OB_SUCC(ret) && skip_idx_attr.has_bm25_token_freq_param()) {
    if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ)))) {
      STORAGE_LOG(WARN, "failed to push bm25 token freq skip index meta", K(ret));
    }
  }

  if (OB_SUCC(ret) && skip_idx_attr.has_bm25_doc_len_param()) {
    if (OB_FAIL(skip_idx_metas.push_back(ObSkipIndexColMeta(col_idx, ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN)))) {
      STORAGE_LOG(WARN, "failed to push bm25 doc len skip index meta", K(ret));
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
    if (skip_idx_attr.has_min_max() || skip_idx_attr.has_loose_min_max()) {
      normal_agg_column_cnt += 2;
      has_null_count_column = skip_idx_attr.has_min_max();
    }
    if (skip_idx_attr.has_bm25_token_freq_param() || skip_idx_attr.has_bm25_doc_len_param()) {
      normal_agg_column_cnt += 1;
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

int get_prefix_for_string_tc_datum(
    const ObDatum &orig_datum,
    const ObObjType obj_type,
    const ObCollationType collation_type,
    const int64_t max_prefix_byte_len,
    ObDatum &prefix_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(orig_datum.is_null() || (!ob_is_string_tc(obj_type) && ObTinyTextType != obj_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(orig_datum), K(max_prefix_byte_len), K(obj_type));
  } else {
    int64_t prefix_len = 0;
    int32_t error = 0;
    const ObString &src_str = orig_datum.get_string();
    if (OB_FAIL(common::ObCharset::well_formed_len(collation_type, src_str.ptr(), max_prefix_byte_len, prefix_len, error))) {
      LOG_WARN("failed to get well formed len", K(ret), K(orig_datum), K(obj_type),
          K(collation_type), K(max_prefix_byte_len));
    } else {
      prefix_datum.pack_ = prefix_len;
      prefix_datum.set_string(src_str.ptr(), prefix_len);
    }
  }
  return ret;
}

int get_prefix_for_text_tc_datum(
    const ObDatum &orig_datum,
    const ObObjType obj_type,
    const ObCollationType collation_type,
    const int64_t max_prefix_byte_len,
    ObDatum &prefix_datum,
    char *prefix_datum_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(orig_datum.is_outrow() || !ob_is_text_tc(obj_type)) || OB_ISNULL(prefix_datum_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected out row datum", K(ret), K(orig_datum), K(obj_type), KP(prefix_datum_buf));
  } else {
    int64_t prefix_len = 0;
    int32_t error = 0;
    const ObLobCommon &lob_data = orig_datum.get_lob_data();
    const char *text_data = lob_data.get_inrow_data_ptr();
    const int64_t text_size = lob_data.get_byte_size(orig_datum.len_);
    const int64_t lob_header_size = text_data - orig_datum.ptr_;
    const int64_t max_prefix_len = max_prefix_byte_len - lob_header_size;
    if (OB_FAIL(common::ObCharset::well_formed_len(collation_type, text_data, max_prefix_len, prefix_len, error))) {
      LOG_WARN("failed to get well formend len for text type", K(ret), K(orig_datum), K(obj_type),
        K(collation_type), K(max_prefix_byte_len), K(lob_header_size), K(max_prefix_len), K(lob_data));
    } else {
      ObLobCommon *new_lob_data = new (prefix_datum_buf) ObLobCommon();
      MEMCPY(new_lob_data->buffer_, text_data, prefix_len);
      prefix_datum.set_lob_data(*new_lob_data, new_lob_data->get_handle_size(prefix_len));
      prefix_datum.set_has_lob_header();
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
