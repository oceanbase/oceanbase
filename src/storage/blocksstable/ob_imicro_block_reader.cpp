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
#include "ob_imicro_block_reader.h"
#include "index_block/ob_index_block_row_struct.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
namespace blocksstable
{

int ObIMicroBlockReader::locate_range(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border,
    int64_t &begin_idx,
    int64_t &end_idx,
    const bool is_index_block)
{
  int ret = OB_SUCCESS;
  begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  bool equal = false;
  int64_t end_key_begin_idx = 0;
  int64_t end_key_end_idx = row_count_;
  if (OB_UNLIKELY(0 > row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row count", K(ret), K_(row_count));
  } else if (0 == row_count_) {
  } else if (OB_ISNULL(datum_utils_)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datum utils is null", K(ret), KP_(datum_utils));
  } else {
    if (!is_left_border || range.get_start_key().is_min_rowkey()) {
      begin_idx = 0;
    } else if (OB_FAIL(find_bound(range, 0, begin_idx, equal, end_key_begin_idx, end_key_end_idx))) {
      LOG_WARN("fail to get lower bound start key", K(ret));
    } else if (begin_idx == row_count_) {
      ret = OB_BEYOND_THE_RANGE;
    } else if (!range.get_border_flag().inclusive_start()) {
      if (equal) {
        ++begin_idx;
        if (begin_idx == row_count_) {
          ret = OB_BEYOND_THE_RANGE;
        }
      }
    }
    LOG_DEBUG("locate range for start key", K(is_left_border), K(is_right_border),
              K(range), K(begin_idx), K(end_idx), K(equal), K(end_key_begin_idx), K(end_key_end_idx));
    if (OB_SUCC(ret)) {
      if (!is_right_border || range.get_end_key().is_max_rowkey()) {
        end_idx = row_count_ - 1;
      } else if (OB_UNLIKELY(end_key_begin_idx > end_key_end_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(ret), K(end_key_begin_idx), K(end_key_end_idx), K(range));
      } else  {
        const bool is_percise_rowkey = datum_utils_->get_rowkey_count() == range.get_end_key().get_datum_cnt();
        // we should use upper_bound if the range include endkey
        if (OB_FAIL(find_bound(range.get_end_key(),
                               !range.get_border_flag().inclusive_end()/*lower_bound*/,
                               end_key_begin_idx > begin_idx ? end_key_begin_idx : begin_idx,
                               end_idx,
                               equal))) {
          LOG_WARN("fail to get lower bound endkey", K(ret));
        } else if (end_idx == row_count_) {
          --end_idx;
        } else if (is_index_block && !(equal && range.get_border_flag().inclusive_end() && is_percise_rowkey)) {
          // Skip
          // When right border is closed and found rowkey is equal to end key of range, do --end_idx
        } else if (end_idx == 0) {
          ret = OB_BEYOND_THE_RANGE;
        } else {
          --end_idx;
        }
      }
    }
  }
  LOG_DEBUG("locate range for end key", K(is_left_border), K(is_right_border), K(range), K(begin_idx), K(end_idx), K(equal));
  return ret;
}

int ObIMicroBlockReader::validate_filter_info(
    const sql::PushdownFilterInfo &pd_filter_info,
    const sql::ObPushdownFilterExecutor &filter,
    const void* col_buf,
    const int64_t col_capacity,
    const ObMicroBlockHeader *header)
{
  int ret = OB_SUCCESS;
  int64_t col_count = filter.get_col_count();
  const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
  const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(header) || OB_ISNULL(read_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid micro block reader", K(ret), KP(read_info_));
  } else if (OB_UNLIKELY(0 > col_count || col_capacity < col_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter col count", K(ret), K(col_count), K(col_capacity));
  } else if (0 == col_count) {
  } else if (OB_ISNULL(col_buf) && 0 < col_capacity) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null col buf", K(ret), K(col_capacity));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      if (OB_UNLIKELY(col_offsets.at(i) >= header->column_count_)) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("Filter col offset greater than store cnt",
                 K(ret), K(header->column_count_), K(col_offsets.at(i)));
      }
    }
  }
  return ret;
}

int ObIMicroBlockReader::filter_white_filter(
    const sql::ObWhiteFilterExecutor &filter,
    const common::ObDatum &datum,
    bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = true;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  if (OB_UNLIKELY(sql::WHITE_OP_MAX <= op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid operator type of Filter node", K(ret), K(filter));
  } else {
    const common::ObIArray<common::ObDatum> &ref_datums = filter.get_datums();
    ObDatumCmpFuncType cmp_func = filter.cmp_func_;
    switch (op_type) {
      case sql::WHITE_OP_NN: {
        if (!datum.is_null()) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_NU: {
        if (datum.is_null()) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_EQ:
      case sql::WHITE_OP_NE:
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE: {
        bool cmp_ret = false;
        if (OB_UNLIKELY(ref_datums.count() != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid argument for comparison operator", K(ret), K(ref_datums));
        } else if (datum.is_null() || ref_datums.at(0).is_null()) {
          // Result of compare with null is null
        } else if (OB_FAIL(compare_datum(
                   datum, ref_datums.at(0),
                   cmp_func,
                   sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()],
                   cmp_ret))) {
          LOG_WARN("Failed to compare datum", K(ret), K(datum), K(ref_datums.at(0)),
              K(sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()]));
        } else if (cmp_ret) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        int cmp_ret_0 = 0;
        int cmp_ret_1 = 0;
        if (OB_UNLIKELY(ref_datums.count() != 2)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid argument for between operators", K(ret), K(ref_datums));
        } else if (datum.is_null()) {
          // Result of compare with null is null
        } else if (OB_FAIL(cmp_func(datum, ref_datums.at(0), cmp_ret_0))) {
          LOG_WARN("Failed to compare datum", K(ret), K(datum), K(ref_datums.at(0)));
        } else if (cmp_ret_0 < 0) {
        } else if (OB_FAIL(cmp_func(datum, ref_datums.at(1), cmp_ret_1))) {
          LOG_WARN("Failed to compare datum", K(ret), K(datum), K(ref_datums.at(0)));
        } else if (cmp_ret_1 <= 0) {
          //cmp_ret_0 >= 0 && cmp_ret_1 <= 0
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        bool is_existed = false;
        if (OB_FAIL(filter.exist_in_datum_set(datum, is_existed))) {
          LOG_WARN("Failed to check object in hashset", K(ret), K(datum));
        } else if (is_existed) {
          filtered = false;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unexpected filter pushdown operation type", K(ret), K(op_type));
      }
    } // end of switch
  }
  return ret;
}
}
}
