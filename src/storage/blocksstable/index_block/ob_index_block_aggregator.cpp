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

#include "ob_index_block_aggregator.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "storage/blocksstable/cs_encoding/ob_column_datum_iter.h"
#include "storage/blocksstable/cs_encoding/ob_dict_encoding_hash_table.h"
#include "src/sql/session/ob_sql_session_info.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "storage/blocksstable/cs_encoding/ob_column_datum_iter.h"
#include "storage/blocksstable/cs_encoding/ob_dict_encoding_hash_table.h"
#include "sql/engine/expr/ob_expr_bm25.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

ObSkipIndexDatumAttr::ObSkipIndexDatumAttr(const bool is_raw_data, const bool is_prefix)
  : is_raw_data_(is_raw_data),
    is_min_max_prefix_(is_prefix)
{}

ObSkipIndexAggResult::ObSkipIndexAggResult()
  : agg_row_(),
    attr_array_()
{}

void ObSkipIndexAggResult::reset()
{
  agg_row_.reset();
  attr_array_.reset();
}

void ObSkipIndexAggResult::reuse()
{
  agg_row_.reuse();
  FOREACH(it, attr_array_) {
    it->reset();
  }
}

bool ObSkipIndexAggResult::is_valid() const
{
  return agg_row_.is_valid() && agg_row_.get_column_count() == attr_array_.count();
}

int ObSkipIndexAggResult::init(const int64_t agg_col_cnt, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(agg_row_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_FAIL(agg_row_.init(allocator, agg_col_cnt))) {
    LOG_WARN("failed to init agg datum row", K(ret));
  } else if (FALSE_IT(attr_array_.set_allocator(&allocator))) {
  } else if (OB_FAIL(attr_array_.init(agg_col_cnt))) {
    LOG_WARN("failed to init agg datum attr array", K(ret));
  } else if (OB_FAIL(attr_array_.prepare_allocate(agg_col_cnt))) {
    LOG_WARN("failed to prepare allocate agg datum array", K(ret));
  }
  return ret;
}

int ObSkipIndexAggResult::deep_copy(const ObSkipIndexAggResult &src, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(agg_row_.deep_copy(src.agg_row_, allocator))) {
    LOG_WARN("failed to copy src aggregated datum row", K(ret));
  } else if (FALSE_IT(attr_array_.set_allocator(&allocator))) {
  } else if (OB_FAIL(attr_array_.assign(src.attr_array_))) {
    LOG_WARN("failed to assign attribute array", K(ret));
  }
  return ret;
}

int ObIColAggregator::init(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result,
    ObSkipIndexDatumAttr &result_attr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else {
    col_desc_ = col_desc;
    major_working_cluster_version_ = major_working_cluster_version;
    result_ = &result;
    result_->set_null();
    result_attr_ = &result_attr;
    result_attr_->reset();
    result_attr_->is_raw_data_ = false;
    is_major_ = is_major;
    if (is_skip_index_black_list_type(col_desc.col_type_.get_type())) {
      set_not_aggregate();
    }
  }
  return ret;
}

void ObIColAggregator::reuse()
{
  if (nullptr != result_) {
    result_->set_null();
  }
  if (is_skip_index_black_list_type(col_desc_.col_type_.get_type())) {
    set_not_aggregate();
  } else {
    can_aggregate_ = true;
  }
}

int ObIColAggregator::copy_agg_datum(const ObDatum &src, ObDatum &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.is_outrow())|| OB_ISNULL(dst.ptr_) || OB_UNLIKELY(src.is_nop()) ||
      OB_UNLIKELY(!src.is_null() && src.len_ > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected agg datum for copy", K(ret), K(src), K(dst));
  } else if (src.is_null()) {
    dst.set_null();
  } else {
    dst.pack_ = src.len_;
    MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
  }
  return ret;
}

int ObIColAggregator::copy_inrow_string_prefix(
    const ObDatum &orig_datum,
    const ObObjType obj_type,
    const ObCollationType collation_type,
    const int64_t max_prefix_byte_len,
    ObStorageDatum &prefix_datum)
{
  int ret = OB_SUCCESS;
  int64_t prefix_len = 0;
  if (OB_UNLIKELY(!prefix_datum.is_local_buf()
      || max_prefix_byte_len > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH
      || orig_datum.is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(orig_datum), K(prefix_datum), K(max_prefix_byte_len));
  } else if (ob_is_string_type(obj_type) && !ob_is_large_text(obj_type)) {
    ObDatum tmp_prefix;
    if (OB_FAIL(get_prefix_for_string_tc_datum(orig_datum, obj_type, collation_type, max_prefix_byte_len, tmp_prefix))) {
      LOG_WARN("failed to get prefix for string tc datum", K(ret));
    } else if (OB_FAIL(copy_agg_datum(tmp_prefix, prefix_datum))) {
      LOG_WARN("failed to copy string agg datum", K(ret));
    }
  } else if (ob_is_large_text(obj_type)) {
    if (OB_FAIL(get_prefix_for_text_tc_datum(
        orig_datum, obj_type, collation_type, max_prefix_byte_len, prefix_datum, prefix_datum.buf_))) {
      LOG_WARN("failed to get prefix for text tc datum", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type for string prefix", K(ret), K(obj_type), K(orig_datum));
  }
  return ret;
}

int ObIColAggregator::copy_agg_datum_for_min_max(const ObDatum &datum, const ObSkipIndexDatumAttr &agg_datum_attr)
{
  int ret = OB_SUCCESS;
  if (!agg_datum_attr.is_raw_data_) {
    result_attr_->is_min_max_prefix_ = agg_datum_attr.is_min_max_prefix_;
    if (OB_FAIL(copy_agg_datum(datum, *result_))) {
      LOG_WARN("Failed to copy aggregated datum", K(ret));
    }
  } else if (ob_is_string_type(col_desc_.col_type_.get_type()) && datum.len_ > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH) {
    result_attr_->is_min_max_prefix_ = true;
    if OB_FAIL(copy_inrow_string_prefix(
        datum,
        col_desc_.col_type_.get_type(),
        col_desc_.col_type_.get_collation_type(),
        ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH,
        *result_)) {
      LOG_WARN("Failed to copy inrow string prefix", K(ret), K(datum), K_(col_desc));
    }
  } else {
    result_attr_->is_min_max_prefix_ = false;
    if (OB_FAIL(copy_agg_datum(datum, *result_))) {
      LOG_WARN("Failed to copy aggregated datum", K(ret));
    }
  }
  return ret;
}

bool ObIColAggregator::need_set_not_aggregate(const ObObjType type, const ObDatum &datum) const
{
  const bool is_outrow_lob = is_lob_storage(type) && !datum.is_null() && !datum.get_lob_data().in_row_;
  const bool datum_exceed_limit = !datum.is_null() && datum.len_ > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH;
  const bool non_string_type_exceed_limit = datum_exceed_limit && !ob_is_string_type(type);
  const bool exceed_limit = enable_skip_index_min_max_prefix(major_working_cluster_version_)
      ? non_string_type_exceed_limit : datum_exceed_limit;
  return is_outrow_lob || exceed_limit;
}

void ObIColAggregator::process_nop_for_loose_agg(const bool is_major)
{
  if (is_major) {
    set_not_aggregate();
  } else {
    // for loose_min_max, we can ignore nop value in incremental data and get a loose min/max bound
  }
}

int ObColNullCountAggregator::init(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result,
    ObSkipIndexDatumAttr &result_attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_major)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("null count aggregator for non-major data not supported yet", K(ret));
  } else if (OB_FAIL(ObIColAggregator::init(is_major, col_desc, major_working_cluster_version, result, result_attr))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else {
    null_count_ = 0;
  }
  return ret;
}

void ObColNullCountAggregator::reuse()
{
  ObIColAggregator::reuse();
  null_count_ = 0;
}

int ObColNullCountAggregator::eval(const ObStorageDatum &datum, const ObSkipIndexDatumAttr &agg_datum_attr)
{
  int ret = OB_SUCCESS;

  if (!can_aggregate_) {
    // Skip
  } else if (datum.is_nop()) {
    // null count on nop data not supported
    set_not_aggregate();
  } else if (OB_UNLIKELY(datum.is_ext()) || OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate status", K(ret), K(datum), KP_(result));
  } else if (agg_datum_attr.is_raw_data_) {
    null_count_ += datum.is_null() ? 1 : 0;
  } else if (datum.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index block data", K(ret), K(datum));
  } else {
    null_count_ += datum.get_int();
  }
  return ret;
}

int ObColNullCountAggregator::get_result(const ObStorageDatum *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    if (can_aggregate_) {
      result_->set_int(null_count_);
    } else {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}

int ObColNullCountAggregator::eval(ObIDatumIter &datum_iter)
{
  return OB_NOT_SUPPORTED;
}

int ObColMaxAggregator::init(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result,
    ObSkipIndexDatumAttr &result_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(is_major, col_desc, major_working_cluster_version, result, result_attr))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type());
    cmp_func_ = basic_funcs->null_first_cmp_;
    data_evaluated_ = false;
    LOG_DEBUG("[SKIP INDEX] init max aggregator", K_(col_desc), K_(can_aggregate), K_(is_major));
  }
  return ret;
}

void ObColMaxAggregator::reuse()
{
  ObIColAggregator::reuse();
  data_evaluated_ = false;
}

int ObColMaxAggregator::eval(const ObStorageDatum &datum, const ObSkipIndexDatumAttr &agg_datum_attr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_) {
    // Skip
  } else if (datum.is_nop()) {
    process_nop_for_loose_agg(is_major_);
  } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), datum)){
    set_not_aggregate();
  } else {
    data_evaluated_ = true;
    int cmp_res = 0;
    if (OB_FAIL(cmp_with_prefix(
        datum, *result_, agg_datum_attr.is_min_max_prefix_, result_attr_->is_min_max_prefix_, cmp_res))){
      LOG_WARN("Failed to compare datum", K(ret), K(datum), K(*result_), K(col_desc_));
    } else if (cmp_res > 0) {
      if (OB_FAIL(copy_agg_datum_for_min_max(datum, agg_datum_attr))) {
        LOG_WARN("failed to copy agg datum", K(ret));
      }
    }
  }
  return ret;
}

int ObColMaxAggregator::eval(ObIDatumIter &datum_iter)
{
  int ret = OB_SUCCESS;
  ObDatum tmp_result;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_) {
    // skip
  } else {
    tmp_result = *result_;
    const ObDatum *iter_datum = nullptr;
    while (OB_SUCC(ret) && can_aggregate_) {
      int cmp_res = 0;
      if (OB_FAIL(datum_iter.get_next(iter_datum))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next iter datum", K(ret));
        }
      } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), *iter_datum)) {
        set_not_aggregate();
      } else if (iter_datum->is_nop()) {
        process_nop_for_loose_agg(is_major_);
      } else if (FALSE_IT(data_evaluated_ = true)) {
      } else if (OB_FAIL(cmp_with_prefix(*iter_datum, tmp_result, false, result_attr_->is_min_max_prefix_, cmp_res))) {
        LOG_WARN("failed to compare datum", K(ret));
      } else if (cmp_res > 0) {
        tmp_result = *iter_datum;
      }
    }

    if (!can_aggregate_) {
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to do max aggregation", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (data_evaluated_) {
        ObSkipIndexDatumAttr attr(true, false);
        if (OB_FAIL(copy_agg_datum_for_min_max(tmp_result, attr))) {
          LOG_WARN("failed to copy aggregated datum", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObColMaxAggregator::get_result(const ObStorageDatum *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    if (!can_aggregate_ || !data_evaluated_) {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}

// Logically, we recognize a prefix as 'prefix + $MAX_CHARACTER' in max aggregation
int ObColMaxAggregator::cmp_with_prefix(
    const ObDatum &left_datum,
    const ObDatum &right_datum,
    const bool &left_is_prefix,
    const bool &right_is_prefix,
    int &cmp_res)
{
  int ret = OB_SUCCESS;
  int tmp_res = 0;
  if (OB_FAIL(cmp_func_(left_datum, right_datum, tmp_res))) {
    LOG_WARN("failed to compare datums", K(ret), K(left_datum), K(right_datum));
  } else {
    if (!left_is_prefix && !right_is_prefix) {
      cmp_res = tmp_res;
    } else if (0 == tmp_res) {
      if (left_is_prefix == right_is_prefix) {
        cmp_res = 0;
      } else {
        cmp_res = left_is_prefix ? 1 : -1;
      }
    } else {
      const bool both_prefix = left_is_prefix && right_is_prefix;
      const bool left_prefix_larger = left_is_prefix && tmp_res > 0;
      const bool right_prefix_larger = right_is_prefix && tmp_res < 0;
      if (!both_prefix && (left_prefix_larger || right_prefix_larger)) {
        cmp_res = tmp_res;
      } else {
        ObString left_str;
        ObString right_str;
        if (ob_is_large_text(col_desc_.col_type_.get_type())) {
          const ObLobCommon &left_lob = left_datum.get_lob_data();
          const ObLobCommon &right_lob = right_datum.get_lob_data();
          left_str = ObString(left_lob.get_byte_size(left_datum.len_), left_lob.get_inrow_data_ptr());
          right_str = ObString(right_lob.get_byte_size(right_datum.len_), right_lob.get_inrow_data_ptr());
        } else {
          left_str = left_datum.get_string();
          right_str = right_datum.get_string();
        }
        const ObObjType &obj_type = col_desc_.col_type_.get_type();
        const ObCollationType &coll = col_desc_.col_type_.get_collation_type();
        const int64_t left_char_num = ObCharset::strlen_char(coll, left_str.ptr(), left_str.length());
        const int64_t right_char_num = ObCharset::strlen_char(coll, right_str.ptr(), right_str.length());
        if (left_char_num == right_char_num
            || (left_char_num < right_char_num && tmp_res > 0)
            || (!enable_revise_max_prefix(major_working_cluster_version_) && left_char_num > right_char_num && tmp_res > 0)
            || (enable_revise_max_prefix(major_working_cluster_version_) && left_char_num > right_char_num && tmp_res < 0)) {
          cmp_res = tmp_res;
        } else {
          const bool left_shorter = left_char_num < right_char_num;
          const int64_t prefix_char_num = MIN(left_char_num, right_char_num);
          const ObString &short_str = left_shorter ? left_str : right_str;
          const ObString &long_str = left_shorter ? right_str : left_str;
          const int64_t prefix_length = ObCharset::charpos(coll, long_str.ptr(), long_str.length(), prefix_char_num);
          ObString prefix_str(prefix_length, long_str.ptr());
          const bool end_with_space = common::is_calc_with_end_space(obj_type, obj_type, lib::is_oracle_mode(), coll, coll);
          const bool prefix_match = (0 == ObCharset::strcmpsp(
              coll, long_str.ptr(), prefix_length, short_str.ptr(), short_str.length(), end_with_space));
          if (!prefix_match) {
            cmp_res = tmp_res;
          } else {
            // short str prefix larger than long str prefix with same prefix
            cmp_res = left_shorter ? 1 : -1;
          }
        }
      }
    }
  }
  return ret;
}

int ObColMinAggregator::init(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result,
    ObSkipIndexDatumAttr &result_attr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(is_major, col_desc, major_working_cluster_version, result, result_attr))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type());
    cmp_func_ = basic_funcs->null_last_cmp_;
    data_evaluated_ = false;
    LOG_DEBUG("[SKIP INDEX] init min aggregator", K_(col_desc), K_(can_aggregate), K_(is_major));
  }
  return ret;
}

void ObColMinAggregator::reuse()
{
  ObIColAggregator::reuse();
  data_evaluated_ = false;
}

int ObColMinAggregator::eval(const ObStorageDatum &datum, const ObSkipIndexDatumAttr &agg_datum_attr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_) {
    // Skip
  } else if (datum.is_nop()) {
    process_nop_for_loose_agg(is_major_);
  } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), datum)){
    set_not_aggregate();
  } else {
    data_evaluated_ = true;
    int cmp_res = 0;
    if (OB_FAIL(cmp_with_prefix(
        datum, *result_, agg_datum_attr.is_min_max_prefix_, result_attr_->is_min_max_prefix_, cmp_res))){
      LOG_WARN("Failed to compare datum", K(ret), K(datum), K(*result_), K(col_desc_));
    } else if (cmp_res < 0) {
      if (OB_FAIL(copy_agg_datum_for_min_max(datum, agg_datum_attr))) {
        LOG_WARN("failed to copy agg datum", K(ret));
      }
    }
  }
  return ret;
}

int ObColMinAggregator::eval(ObIDatumIter &datum_iter)
{
  int ret = OB_SUCCESS;
  ObDatum tmp_result;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_) {
    // skip
  } else {
    tmp_result = *result_;
    const ObDatum *iter_datum = nullptr;
    while (OB_SUCC(ret) && can_aggregate_) {
      int cmp_res = 0;
      if (OB_FAIL(datum_iter.get_next(iter_datum))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next iter datum", K(ret));
        }
      } else if (iter_datum->is_nop()) {
        process_nop_for_loose_agg(is_major_);
      } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), *iter_datum)) {
        set_not_aggregate();
      } else if (FALSE_IT(data_evaluated_ = true)) {
      } else if (OB_FAIL(cmp_with_prefix(*iter_datum, tmp_result, false, result_attr_->is_min_max_prefix_, cmp_res))) {
        LOG_WARN("failed to compare datum", K(ret));
      } else if (cmp_res < 0) {
        tmp_result = *iter_datum;
      }
    }

    if (!can_aggregate_ || !data_evaluated_) {
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to do max aggregation", K(ret));
    } else {
      ret = OB_SUCCESS;
      ObSkipIndexDatumAttr attr(true, false);
      if (OB_FAIL(copy_agg_datum_for_min_max(tmp_result, attr))) {
        LOG_WARN("failed to copy aggregated datum", K(ret));
      }
    }
  }
  return ret;
}

int ObColMinAggregator::get_result(const ObStorageDatum *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    if (!can_aggregate_ || !data_evaluated_) {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}

int ObColMinAggregator::cmp_with_prefix(
    const ObDatum &left_datum,
    const ObDatum &right_datum,
    const bool &left_is_prefix,
    const bool &right_is_prefix,
    int &cmp_res)
{
  int ret = OB_SUCCESS;
  int tmp_res = 0;
  if (OB_FAIL(cmp_func_(left_datum, right_datum, tmp_res))) {
    LOG_WARN("failed to compare datums", K(ret), K(left_datum), K(right_datum));
  } else {
    if (tmp_res != 0 || (left_is_prefix == right_is_prefix)) {
      cmp_res = tmp_res;
    } else {
      // when prefix is same with a complete value, prefix is larger
      cmp_res = left_is_prefix ? 1 : -1;
    }
  }
  return ret;
}

int ObColSumAggregator::init(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result,
    ObSkipIndexDatumAttr &result_attr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_major)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("skip index sum column aggregator on non-major data not supported", K(ret));
  } else if (OB_FAIL(ObIColAggregator::init(is_major, col_desc, major_working_cluster_version, result, result_attr))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else if (!can_agg_sum(col_desc.col_type_.get_type())) {
    set_not_aggregate();
    LOG_DEBUG("[SKIP INDEX] init col sum agg but is not numberic", K(col_desc));
  }
  LOG_DEBUG("[SKIP INDEX] ObColSumAggregator init", K(col_desc_));
  return ret;
}

void ObColSumAggregator::reuse()
{
  ObIColAggregator::reuse();
  if (can_agg_sum(col_desc_.col_type_.get_type())) {
    can_aggregate_ = true;
  } else {
    set_not_aggregate();
  }
}

int ObColSumAggregator::eval(const ObStorageDatum &datum, const ObSkipIndexDatumAttr &agg_datum_attr)
{
  int ret = OB_SUCCESS;
  ObStorageDatum cast_datum;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_ || datum.is_null()) {
    // Skip
  } else if (datum.is_nop()) {
    // sum pre-agg on nop data not supported
    set_not_aggregate();
  } else if (OB_FAIL(choose_eval_func(agg_datum_attr.is_raw_data_))) {
    LOG_WARN("fail to choose eval func", K(ret), K(agg_datum_attr));
  } else if (OB_FAIL((this->*eval_func_)(datum))) {
    if (OB_INTEGER_PRECISION_OVERFLOW == ret || OB_DECIMAL_PRECISION_OVERFLOW == ret || OB_NUMERIC_OVERFLOW == ret) {
      // sum precision overflow set not aggregate.
      set_not_aggregate();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to eval sum", K(ret), K(datum), KPC(result_), K(col_desc_));
    }
  }
  return ret;
}

int ObColSumAggregator::eval(ObIDatumIter &datum_iter)
{
  int ret = OB_SUCCESS;
  ObDatum tmp_result;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_) {
    // skip
  } else if (OB_FAIL(choose_eval_func(true))) {
    LOG_WARN("failed to choose eval func", K(ret));
  } else {
    tmp_result = *result_;
    const ObDatum *iter_datum = nullptr;
    while (OB_SUCC(ret) && can_aggregate_) {
      int cmp_res = 0;
      if (OB_FAIL(datum_iter.get_next(iter_datum))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next iter datum", K(ret));
        }
      } else if (iter_datum->is_nop()) {
        // sum pre-agg on nop data not supported
        set_not_aggregate();
      } else if (OB_FAIL((this->*eval_func_)(*iter_datum))) {
        if (OB_INTEGER_PRECISION_OVERFLOW == ret || OB_DECIMAL_PRECISION_OVERFLOW == ret || OB_NUMERIC_OVERFLOW == ret) {
          // sum precision overflow set not aggregate.
          set_not_aggregate();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to eval sum", K(ret), K(iter_datum), KPC(result_), K(col_desc_));
        }
      }
    }

    if (!can_aggregate_) {
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to do max aggregation", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObColSumAggregator::get_result(const ObStorageDatum *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    if (!can_aggregate_) {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}



int ObColSumAggregator::choose_eval_func(const bool is_data)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass obj_tc = col_desc_.col_type_.get_type_class();
  if (is_data) {
    switch (obj_tc) {
      case ObObjTypeClass::ObIntTC: {
        eval_func_ = &ObColSumAggregator::eval_int_number;
        break;
      }
      case ObObjTypeClass::ObUIntTC: {
        eval_func_ = &ObColSumAggregator::eval_uint_number;
        break;
      }
      case ObObjTypeClass::ObFloatTC: {
        eval_func_ = &ObColSumAggregator::eval_float;
        break;
      }
      case ObObjTypeClass::ObDoubleTC: {
        eval_func_ = &ObColSumAggregator::eval_double;
        break;
      }
      case ObObjTypeClass::ObNumberTC: {
        eval_func_ = &ObColSumAggregator::eval_number;
        break;
      }
      case ObObjTypeClass::ObDecimalIntTC: {
        eval_func_ = &ObColSumAggregator::eval_decimal_int_number;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected skip index sum type", K(ret), K(is_data), K(obj_tc));
        break;
      }
    }
  } else {
    switch (obj_tc) {
      case ObObjTypeClass::ObIntTC:
      case ObObjTypeClass::ObUIntTC:
      case ObObjTypeClass::ObDecimalIntTC:
      case ObObjTypeClass::ObNumberTC: {
        eval_func_ = &ObColSumAggregator::eval_number;
        break;
      }
      case ObObjTypeClass::ObFloatTC: {
        eval_func_ = &ObColSumAggregator::eval_float;
        break;
      }
      case ObObjTypeClass::ObDoubleTC: {
        eval_func_ = &ObColSumAggregator::eval_double;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected skip index sum type", K(ret), K(is_data), K(obj_tc));
        break;
      }
    }
  }
  return ret;
}

int ObColSumAggregator::eval_int_number(const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    sql::ObNumStackAllocator<1> tmp_alloc;
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from(datum.get_int(), tmp_alloc))) {
      LOG_WARN("create number from int failed", K(ret));
    } else if (OB_FAIL(inner_eval_number(nmb))) {
      LOG_WARN("fail to eval number", K(datum), "number: ", nmb.format());
    }
  }
  return ret;
}

int ObColSumAggregator::eval_uint_number(const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    sql::ObNumStackAllocator<1> tmp_alloc;
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from(datum.get_uint(), tmp_alloc))) {
      LOG_WARN("create number from int failed", K(ret));
    } else if (OB_FAIL(inner_eval_number(nmb))) {
      LOG_WARN("fail to eval number", K(datum), "number: ", nmb.format());
    }
  }
  return ret;
}

int ObColSumAggregator::eval_decimal_int_number(const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    sql::ObNumStackAllocator<1> tmp_alloc;
    number::ObNumber nmb;
    LOG_DEBUG("decimal int to number", K(lbt()));
    if (OB_FAIL(wide::to_number(datum.get_decimal_int(), datum.get_int_bytes(),
                                col_desc_.col_type_.get_scale(), tmp_alloc, nmb))) {
      LOG_WARN("to_number failed", K(ret));
    } else if (OB_FAIL(inner_eval_number(nmb))) {
      LOG_WARN("fail to eval number", K(datum), "number: ", nmb.format());
    }
  }
  return ret;
}

int ObColSumAggregator::eval_number(const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    number::ObNumber nmb(datum.get_number());
    if (OB_FAIL(inner_eval_number(nmb))) {
      LOG_WARN("fail to eval number", K(datum), "number: ", nmb.format());
    }
  }
  return ret;
}

int ObColSumAggregator::inner_eval_number(const number::ObNumber &right_nmb)
{
  int ret = OB_SUCCESS;
  if (result_->is_null()) {
    result_->set_number(right_nmb);
  } else {
    sql::ObNumStackAllocator<2> tmp_alloc;
    number::ObNumber left_nmb(result_->get_number());
    number::ObNumber result_nmb;
    if (OB_FAIL(left_nmb.add_v3(right_nmb, result_nmb, tmp_alloc, false))) {
      LOG_WARN("number add failed", K(ret),
          "left nmb: ", left_nmb.format(), "right nmb: ", right_nmb.format(),
          K(left_nmb), K(right_nmb));
    } else {
      result_->set_number(result_nmb);
    }
  }
  return ret;
}

int ObColSumAggregator::eval_float(const common::ObDatum &datum)
{
  // ref to ObSumAggCell::eval_float_inner
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    float right_f = datum.get_float();
    if (result_->is_null()) {
      result_->set_float(right_f);
    } else {
      float left_f = result_->get_float();
      if (OB_UNLIKELY(sql::ObArithExprOperator::is_float_out_of_range(left_f + right_f))
        && !lib::is_oracle_mode()) {
          // out of range
          set_not_aggregate();
      } else {
        result_->set_float(left_f + right_f);
      }
    }
  }
  return ret;
}

int ObColSumAggregator::eval_double(const common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (!datum.is_null()) {
    double right_d = datum.get_double();
    if (result_->is_null()) {
      result_->set_double(right_d);
    } else {
      double left_d = result_->get_double();
      result_->set_double(left_d + right_d);
    }
  }
  return ret;
}

ObIMultiColAggregator::ObIMultiColAggregator()
  : allocator_(nullptr),
    agg_result_row_(nullptr),
    agg_row_proj_idxes_(),
    result_idxes_(),
    col_agg_metas_(),
    is_inited_(false)
{}

void ObIMultiColAggregator::reset()
{
  allocator_ = nullptr;
  agg_result_row_ = nullptr;
  agg_row_proj_idxes_.reset();
  result_idxes_.reset();
  col_agg_metas_.reset();
  is_inited_ = false;
}

void ObIMultiColAggregator::reuse()
{
  if (agg_result_row_ != nullptr) {
    for (int64_t i = 0; i < result_idxes_.count(); ++i) {
      const int64_t result_idx = result_idxes_.at(i);
      if (OB_LIKELY(result_idx >= 0 && result_idx < agg_result_row_->get_agg_col_cnt())) {
        agg_result_row_->get_agg_datum_row().storage_datums_[result_idx].reuse();
        agg_result_row_->get_agg_datum_row().storage_datums_[result_idx].set_null();
      }
    }
  }
}

int ObBM25ParamAggregator::init(
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    ObSkipIndexAggResult &agg_result_row,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t agg_col_cnt = 2;
  agg_row_proj_idxes_.set_allocator(&allocator);
  result_idxes_.set_allocator(&allocator);
  col_agg_metas_.set_allocator(&allocator);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_FAIL(agg_row_proj_idxes_.prepare_allocate(agg_col_cnt))) {
    LOG_WARN("Fail to reserve agg row proj idxes", K(ret));
  } else if (OB_FAIL(result_idxes_.prepare_allocate(agg_col_cnt))) {
    LOG_WARN("Fail to reserve result idxes", K(ret));
  } else if (OB_FAIL(col_agg_metas_.prepare_allocate(agg_col_cnt))) {
    LOG_WARN("Fail to reserve col agg metas", K(ret));
  }

  bool found_token_freq = false;
  bool found_doc_length = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas.count(); ++i) {
    const ObSkipIndexColMeta &idx_col_meta = full_agg_metas.at(i);
    if (idx_col_meta.get_col_type() == SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ) {
      if (OB_UNLIKELY(found_token_freq)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected duplicate token freq column", K(ret), K(full_agg_metas));
      } else if (OB_UNLIKELY(!full_col_descs.at(idx_col_meta.get_col_idx()).col_type_.is_uint64())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected token freq column type", K(ret), K(i), K(full_agg_metas), K(full_col_descs));
      } else {
        found_token_freq = true;
        agg_row_proj_idxes_.at(TOKEN_FREQ_IDX) = i;
        result_idxes_.at(TOKEN_FREQ_IDX) = i;
        col_agg_metas_.at(TOKEN_FREQ_IDX) = idx_col_meta;
        agg_result_row.get_agg_datum_row().storage_datums_[i].reuse();
        agg_result_row.get_agg_datum_row().storage_datums_[i].set_uint(0);
      }
    } else if (idx_col_meta.get_col_type() == SK_IDX_BM25_MAX_SCORE_DOC_LEN) {
      if (OB_UNLIKELY(found_doc_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected duplicate doc length column", K(ret), K(full_agg_metas));
      } else if (OB_UNLIKELY(!full_col_descs.at(idx_col_meta.get_col_idx()).col_type_.is_uint64())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected doc length column type", K(ret), K(i), K(full_agg_metas), K(full_col_descs));
      } else {
        found_doc_length = true;
        agg_row_proj_idxes_.at(DOC_LENGTH_IDX) = i;
        result_idxes_.at(DOC_LENGTH_IDX) = i;
        col_agg_metas_.at(DOC_LENGTH_IDX) = idx_col_meta;
        agg_result_row.get_agg_datum_row().storage_datums_[i].reuse();
        agg_result_row.get_agg_datum_row().storage_datums_[i].set_uint(0);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!found_token_freq || !found_doc_length)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected missing token freq or doc length column", K(ret), K(full_agg_metas));
  } else {
    agg_result_row_ = &agg_result_row;
    allocator_ = &allocator;
    curr_max_score_ = 0.0;
    is_inited_ = true;
  }

  return ret;
}

void ObBM25ParamAggregator::reset()
{
  ObIMultiColAggregator::reset();
  agg_result_row_ = nullptr;
  allocator_ = nullptr;
  curr_max_score_ = 0.0;
  is_inited_ = false;
}

void ObBM25ParamAggregator::reuse()
{
  ObIMultiColAggregator::reuse();
  if (nullptr != agg_result_row_ && result_idxes_.count() >= 2) {
    const int64_t token_freq_result_idx = result_idxes_.at(TOKEN_FREQ_IDX);
    const int64_t doc_length_result_idx = result_idxes_.at(DOC_LENGTH_IDX);
    if (OB_LIKELY(token_freq_result_idx >= 0 && token_freq_result_idx < agg_result_row_->get_agg_col_cnt())) {
      agg_result_row_->get_agg_datum_row().storage_datums_[token_freq_result_idx].set_uint(0);
    }
    if (OB_LIKELY(doc_length_result_idx >= 0 && doc_length_result_idx < agg_result_row_->get_agg_col_cnt())) {
      agg_result_row_->get_agg_datum_row().storage_datums_[doc_length_result_idx].set_uint(0);
    }
  }
  curr_max_score_ = 0.0;
}

int ObBM25ParamAggregator::eval(const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const int64_t token_freq_data_idx = col_agg_metas_.at(TOKEN_FREQ_IDX).col_idx_;
    const int64_t doc_length_data_idx = col_agg_metas_.at(DOC_LENGTH_IDX).col_idx_;
    const ObDatum &token_freq_datum = datum_row.storage_datums_[token_freq_data_idx];
    const ObDatum &doc_length_datum = datum_row.storage_datums_[doc_length_data_idx];
    if (OB_FAIL(do_max_score_agg(token_freq_datum, doc_length_datum))) {
      LOG_WARN("Fail to do max score agg", K(ret), K(datum_row), K(token_freq_datum), K(doc_length_datum));
    }
  }
  return ret;
}

int ObBM25ParamAggregator::eval(const ObIMicroBlockWriter &data_micro_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    ObMicroDataPreAggParam token_freq_param;
    ObMicroDataPreAggParam doc_length_param;
    const int64_t token_freq_data_idx = col_agg_metas_.at(TOKEN_FREQ_IDX).col_idx_;
    const int64_t doc_length_data_idx = col_agg_metas_.at(DOC_LENGTH_IDX).col_idx_;
    if (OB_FAIL(data_micro_writer.get_pre_agg_param(token_freq_data_idx, token_freq_param))) {
      LOG_WARN("Fail to get token freq pre agg param", K(ret), K(token_freq_data_idx));
    } else if (OB_FAIL(data_micro_writer.get_pre_agg_param(doc_length_data_idx, doc_length_param))) {
      LOG_WARN("Fail to get doc length pre agg param", K(ret), K(doc_length_data_idx));
    } else {
      ObColumnDatumIter token_freq_iter(*token_freq_param.col_datums_);
      ObColumnDatumIter doc_length_iter(*doc_length_param.col_datums_);
      const ObDatum *token_freq_datum = nullptr;
      const ObDatum *doc_length_datum = nullptr;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(token_freq_iter.get_next(token_freq_datum))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("Fail to get token freq datum", K(ret));
          } else if (OB_ITER_END != doc_length_iter.get_next(doc_length_datum)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("token freq datum iter inconsistent with doc length datum iter",
                K(ret), KPC(token_freq_param.col_datums_), KPC(doc_length_param.col_datums_));
          }
        } else if (OB_FAIL(doc_length_iter.get_next(doc_length_datum))) {
          LOG_WARN("Fail to get doc length datum", K(ret));
        } else if (OB_FAIL(do_max_score_agg(*token_freq_datum, *doc_length_datum))) {
          LOG_WARN("Fail to do max score agg", K(ret), K(*token_freq_datum), K(*doc_length_datum));
        }
      }

      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to do bm25 max score aggregate on micro block", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObBM25ParamAggregator::eval(const ObSkipIndexAggResult &agg_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const uint32_t token_freq_agg_idx = agg_row_proj_idxes_.at(TOKEN_FREQ_IDX);
    const uint32_t doc_length_agg_idx = agg_row_proj_idxes_.at(DOC_LENGTH_IDX);
    const ObDatum &token_freq_datum = agg_row.get_agg_datum_row().storage_datums_[token_freq_agg_idx];
    const ObDatum &doc_length_datum = agg_row.get_agg_datum_row().storage_datums_[doc_length_agg_idx];
    if (OB_FAIL(do_max_score_agg(token_freq_datum, doc_length_datum))) {
      LOG_WARN("Fail to do max score agg", K(ret), K(token_freq_datum), K(doc_length_datum));
    }
  }
  return ret;
}

int ObBM25ParamAggregator::eval(ObAggRowReader &agg_row_reader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const ObSkipIndexColMeta &token_freq_col_meta = col_agg_metas_.at(TOKEN_FREQ_IDX);
    const ObSkipIndexColMeta &doc_length_col_meta = col_agg_metas_.at(DOC_LENGTH_IDX);
    ObStorageDatum token_freq_datum;
    ObStorageDatum doc_length_datum;
    if (OB_FAIL(agg_row_reader.read(token_freq_col_meta, token_freq_datum))) {
      LOG_WARN("Fail to read token freq datum", K(ret), K(token_freq_col_meta));
    } else if (OB_FAIL(agg_row_reader.read(doc_length_col_meta, doc_length_datum))) {
      LOG_WARN("Fail to read doc length datum", K(ret), K(doc_length_col_meta));
    } else if (OB_FAIL(do_max_score_agg(token_freq_datum, doc_length_datum))) {
      LOG_WARN("Fail to do max score agg", K(ret), K(token_freq_datum), K(doc_length_datum));
    }
  }
  return ret;
}

int ObBM25ParamAggregator::do_max_score_agg(const ObDatum &token_freq_datum, const ObDatum &doc_length_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(token_freq_datum.is_null() || doc_length_datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null datum", K(ret), K(token_freq_datum), K(doc_length_datum));
  } else if (token_freq_datum.is_nop() || doc_length_datum.is_nop()) {
    // skip
  } else {
    const uint64_t token_freq = token_freq_datum.get_uint();
    const uint64_t doc_length = doc_length_datum.get_uint();
    const double tf_score = sql::ObExprBM25::doc_token_weight(token_freq, get_norm_len(doc_length));
    if (tf_score > curr_max_score_) {
      curr_max_score_ = tf_score;
      const int64_t token_freq_result_idx = result_idxes_.at(TOKEN_FREQ_IDX);
      const int64_t doc_length_result_idx = result_idxes_.at(DOC_LENGTH_IDX);
      agg_result_row_->get_agg_datum_row().storage_datums_[token_freq_result_idx].set_uint(token_freq);
      agg_result_row_->get_agg_datum_row().storage_datums_[doc_length_result_idx].set_uint(doc_length);
    }
  }
  return ret;
}

ObISkipIndexAggregator::ObISkipIndexAggregator()
  : allocator_(nullptr),
    single_col_agg_idxes_(),
    col_aggs_(),
    multi_col_aggs_(),
    agg_result_(),
    full_agg_metas_(nullptr),
    full_col_descs_(nullptr),
    max_agg_size_(0),
    major_working_cluster_version_(0),
    need_aggregate_(false),
    evaluated_(false),
    is_inited_(false) {}


void ObISkipIndexAggregator::reset()
{
  agg_result_.reset();
  for (int64_t i = 0; i < col_aggs_.count(); ++i) {
    ObIColAggregator *col_agg = col_aggs_.at(i);
    col_agg->~ObIColAggregator();
    if (allocator_ != nullptr) {
      allocator_->free(col_agg);
    }
  }
  single_col_agg_idxes_.reset();
  col_aggs_.reset();
  for (int64_t i = 0; i < multi_col_aggs_.count(); ++i) {
    ObIMultiColAggregator *multi_col_agg = multi_col_aggs_.at(i);
    multi_col_agg->~ObIMultiColAggregator();
    if (allocator_ != nullptr) {
      allocator_->free(multi_col_agg);
    }
  }
  multi_col_aggs_.reset();
  full_agg_metas_ = nullptr;
  full_col_descs_ = nullptr;
  allocator_ = nullptr;
  max_agg_size_ = 0;
  major_working_cluster_version_ = 0;
  need_aggregate_ = false;
  evaluated_ = false;
  is_inited_ = false;
}

void ObISkipIndexAggregator::reuse()
{
  agg_result_.reuse();
  for (int64_t i = 0; i < col_aggs_.count(); ++i) {
    col_aggs_.at(i)->reuse();
  }
  for (int64_t i = 0; i < multi_col_aggs_.count(); ++i) {
    ObIMultiColAggregator *multi_col_agg = multi_col_aggs_.at(i);
    multi_col_agg->reuse();
  }
  evaluated_ = false;
}

int ObISkipIndexAggregator::init(
    const bool is_major,
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    const int64_t major_working_cluster_version,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (0 == full_agg_metas.count()) {
    need_aggregate_ = false;
    is_inited_ = true;
  } else if (OB_FAIL(agg_result_.init(full_agg_metas.count(), allocator))) {
    LOG_WARN("Fail to init aggregate result datum row", K(ret), K(full_agg_metas));
  } else if (OB_FAIL(init_col_aggregators(
      is_major, full_agg_metas, full_col_descs, major_working_cluster_version, allocator))) {
    LOG_WARN("Fail to init column aggregators", K(ret), K(full_agg_metas), K(full_col_descs));
  } else if (FALSE_IT(major_working_cluster_version_ = major_working_cluster_version)) {
  } else if(OB_FAIL(calc_max_agg_size(full_agg_metas, full_col_descs))){
    LOG_WARN("Fail to calculate max aggregate data size",
        K(ret), K(full_agg_metas), K(full_col_descs));
  } else {
    allocator_ = &allocator;
    full_agg_metas_ = &full_agg_metas;
    full_col_descs_ = &full_col_descs;
    need_aggregate_ = true;
    evaluated_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObISkipIndexAggregator::eval(const char *buf, const int64_t buf_size, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  ObStorageDatum tmp_datum;
  ObStorageDatum tmp_null_datum;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!need_aggregate_) {
    // skip
  } else if (FALSE_IT(agg_row_reader_.reset())) {
  } else if (OB_FAIL(agg_row_reader_.init(buf, buf_size))) {
    LOG_WARN("Fail to init agg row reader", K(ret), KP(buf), K(buf_size));
  } else {
    for (int64_t i = 0;
        OB_SUCC(ret) && i < full_agg_metas_->count() && full_agg_metas_->at(i).is_single_col_agg();
        ++i) {
      tmp_datum.reuse();
      tmp_null_datum.reuse();
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas_->at(i);
      bool is_min_max_prefix = false;
      const int32_t single_col_agg_idx = single_col_agg_idxes_.at(i);
      if (OB_FAIL(agg_row_reader_.read(idx_col_meta, tmp_datum, is_min_max_prefix))) {
        LOG_WARN("Fail to read aggregated data", K(ret), K(idx_col_meta));
      } else if (OB_UNLIKELY(tmp_datum.is_ext())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected ext agg datum", K(ret), K(tmp_datum), K(idx_col_meta));
      } else if (tmp_datum.is_null()) {
        ObSkipIndexColMeta null_col_meta(idx_col_meta.col_idx_, SK_IDX_NULL_COUNT);
        if (OB_FAIL(agg_row_reader_.read(null_col_meta, tmp_null_datum))) {
          LOG_WARN("Fail to read aggregated null", K(ret), K(idx_col_meta));
        } else if (tmp_null_datum.is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null count datum", K(ret), K(tmp_null_datum), K(idx_col_meta));
        } else if (tmp_null_datum.is_null()) {
          col_aggs_.at(single_col_agg_idx)->set_not_aggregate();
        } else if (tmp_null_datum.get_int() > row_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Unexpected null count datum out row count", K(ret),
              K(tmp_null_datum), K(row_count), K(null_col_meta));
        } else if (tmp_null_datum.get_int() < row_count) {
          col_aggs_.at(single_col_agg_idx)->set_not_aggregate();
        }
      }
      ObSkipIndexDatumAttr agg_datum_attr(false, is_min_max_prefix);
      if (FAILEDx(col_aggs_.at(single_col_agg_idx)->eval(tmp_datum, agg_datum_attr))) {
        col_aggs_.at(single_col_agg_idx)->set_not_aggregate();
        LOG_ERROR("Fail to eval aggregate column", K(ret), K(tmp_datum),
            K(idx_col_meta), K(i), K(col_aggs_.at(single_col_agg_idx)->get_col_desc()));
        ret = OB_SUCCESS;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_col_aggs_.count(); ++i) {
      if (OB_FAIL(multi_col_aggs_.at(i)->eval(agg_row_reader_))) {
        LOG_WARN("Fail to eval multi col aggregate", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      evaluated_ = true;
    }
  }
  return ret;
}

int ObISkipIndexAggregator::get_aggregated_row(const ObSkipIndexAggResult *&aggregated_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!need_aggregate_) {
    aggregated_row = nullptr;
  } else if (OB_UNLIKELY(!evaluated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected get aggregated row from unevaluated data",
        K(ret), K_(evaluated), K_(need_aggregate), K_(col_aggs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      const ObStorageDatum *result = nullptr;
      const int32_t single_col_agg_idx = single_col_agg_idxes_.at(i);
      if (!full_agg_metas_->at(i).is_single_col_agg()) {
        // skip
      } else if (OB_FAIL(col_aggs_.at(single_col_agg_idx)->get_result(result))) {
        LOG_WARN("Fail to get result from column aggregator", K(ret));
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to get aggregated column result", K(ret), K(i));
      } else if (OB_UNLIKELY(result->len_ > ObSkipIndexColMeta::MAX_SKIP_INDEX_COL_LENGTH
          || result->is_outrow())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected aggregated result datum", K(ret), K(result), K(i), K_(full_agg_metas));
      }
    }

    if (OB_SUCC(ret)) {
      aggregated_row = &agg_result_;
      LOG_DEBUG("[SKIP INDEX] generate aggregated row", K(ret), K_(agg_result));
    }
  }
  return ret;
}

int ObISkipIndexAggregator::calc_max_agg_size(
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs)
{
  int ret = OB_SUCCESS;
  const bool enable_store_prefix = enable_skip_index_min_max_prefix(major_working_cluster_version_);
  max_agg_size_ = 0;
  if (full_agg_metas.count() > 0) {
    uint8_t agg_col_idx_size = 0;
    uint8_t agg_col_idx_off_size = ObAggRowHeader::AGG_COL_MAX_OFFSET_SIZE;
    uint8_t agg_col_off_size = ObAggRowHeader::AGG_COL_MAX_OFFSET_SIZE;
    int64_t aggs_count = full_agg_metas.count();
    uint32_t max_col_idx = full_agg_metas.at(aggs_count - 1).col_idx_;
    do {
      ++agg_col_idx_size;
      max_col_idx >>= 8;
    } while(max_col_idx != 0);

    int64_t col_idx_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < aggs_count; /*++i*/) {
      int64_t start = i, end = i;
      int64_t cur_max_cell_size = 0;
      const int64_t cur_col_idx = full_agg_metas.at(start).col_idx_;
      const ObObjType &obj_type = full_col_descs.at(cur_col_idx).col_type_.get_type();
      int16_t precision = PRECISION_UNKNOWN_YET;
      if (ob_is_decimal_int(obj_type)) {
        precision = full_col_descs.at(cur_col_idx).col_type_.get_stored_precision();
      }
      ObObjDatumMapType datum_type = ObDatum::get_obj_datum_map_type(obj_type);
      uint32_t agg_cell_size = 0;
      uint32_t sum_store_size = 0;
      if (OB_FAIL(get_skip_index_store_upper_size(datum_type, precision, agg_cell_size))) {
        LOG_WARN("failed to get skip index store upper size", K(ret), K(datum_type), K(precision));
      } else if (can_agg_sum(obj_type) && OB_FAIL(get_sum_store_size(obj_type, sum_store_size))) {
        LOG_WARN("failed to get sum store size", K(ret), K(obj_type));
      } else {
        while (OB_SUCC(ret) && end < aggs_count && cur_col_idx == full_agg_metas.at(end).col_idx_) {
          const ObSkipIndexColType idx_type = static_cast<ObSkipIndexColType>(full_agg_metas.at(end).col_type_);
          switch (idx_type) {
          case ObSkipIndexColType::SK_IDX_MIN:
          case ObSkipIndexColType::SK_IDX_MAX:
          case ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ:
          case ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN: {
            cur_max_cell_size += agg_cell_size;
            break;
          }
          case ObSkipIndexColType::SK_IDX_NULL_COUNT: {
            cur_max_cell_size += sizeof(int64_t);
            break;
          }
          case ObSkipIndexColType::SK_IDX_SUM: {
            cur_max_cell_size += sum_store_size;
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Not support skip index aggregate type", K(ret), K(idx_type));
          }
          }
          ++end;
        }
        cur_max_cell_size += ObAggRowHeader::AGG_COL_TYPE_BITMAP_SIZE;
        if (enable_store_prefix) {
          cur_max_cell_size += ObAggRowHeader::AGG_COL_TYPE_BITMAP_SIZE;
        }
        //reserve one more column to save cell size
        cur_max_cell_size += (end - start + 1) * agg_col_off_size;
        max_agg_size_ += cur_max_cell_size;
        ++col_idx_count;
        i = end; //start next loop
      }
    }
    max_agg_size_ += col_idx_count * agg_col_idx_size;
    max_agg_size_ += col_idx_count * agg_col_off_size;
    max_agg_size_ += sizeof(ObAggRowHeader);
    LOG_DEBUG("calc max aggregate size", K(max_agg_size_), K(col_idx_count), K(full_agg_metas));
  }
  return ret;
}

int ObISkipIndexAggregator::init_col_aggregators(
    const bool is_major,
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    const int64_t major_working_cluster_version,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  col_aggs_.clear();
  col_aggs_.set_allocator(&allocator);
  multi_col_aggs_.clear();
  multi_col_aggs_.set_allocator(&allocator);
  single_col_agg_idxes_.clear();
  single_col_agg_idxes_.set_allocator(&allocator);

  if (OB_FAIL(single_col_agg_idxes_.prepare_allocate(full_agg_metas.count()))) {
    LOG_WARN("Fail to prepare allocate single col agg idxes array", K(ret));
  }

  int64_t single_col_agg_count = 0;
  bool has_bm25_token_freq = false;
  bool has_bm25_doc_len = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas.count(); ++i) {
    if (full_agg_metas.at(i).is_single_col_agg()) {
      single_col_agg_idxes_.at(i) = single_col_agg_count;
      ++single_col_agg_count;
    } else if (ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ == full_agg_metas.at(i).col_type_) {
      single_col_agg_idxes_.at(i) = -1;
      has_bm25_token_freq = true;
    } else if (ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN == full_agg_metas.at(i).col_type_) {
      single_col_agg_idxes_.at(i) = -1;
      has_bm25_doc_len = true;
    }
  }
  const int64_t multi_col_agg_count = (has_bm25_token_freq && has_bm25_doc_len) ? 1 : 0;

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(has_bm25_token_freq != has_bm25_doc_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected bm25 token freq withour doc len parameter in skip index", K(ret), K(full_agg_metas));
  } else if (OB_FAIL(col_aggs_.reserve(single_col_agg_count))) {
    LOG_WARN("Fail to reserve col aggregator array", K(ret));
  } else if (OB_FAIL(init_multi_col_aggregators(multi_col_agg_count, full_agg_metas, full_col_descs, agg_result_, allocator))) {
    LOG_WARN("Fail to init multi col aggregators", K(ret));
  } else {
    for (int64_t i = 0;
        OB_SUCC(ret) && i < full_agg_metas.count() && full_agg_metas.at(i).is_single_col_agg();
        ++i) {
      const ObSkipIndexColType idx_type = static_cast<ObSkipIndexColType>(full_agg_metas.at(i).col_type_);
      const int64_t col_idx = full_agg_metas.at(i).col_idx_;
      ObStorageDatum &agg_res_datum = agg_result_.get_agg_datum_row().storage_datums_[i];
      ObSkipIndexDatumAttr &agg_datum_attr = agg_result_.get_agg_attrs().at(i);
      switch (idx_type) {
      case ObSkipIndexColType::SK_IDX_MIN: {
        if (OB_FAIL(init_col_aggregator<ObColMinAggregator>(
            is_major, full_col_descs.at(col_idx), major_working_cluster_version, agg_res_datum, agg_datum_attr, allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_MAX: {
        if (OB_FAIL(init_col_aggregator<ObColMaxAggregator>(
            is_major, full_col_descs.at(col_idx), major_working_cluster_version, agg_res_datum, agg_datum_attr, allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_NULL_COUNT: {
        if (OB_FAIL(init_col_aggregator<ObColNullCountAggregator>(
            is_major, full_col_descs.at(col_idx), major_working_cluster_version, agg_res_datum, agg_datum_attr, allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_SUM: {
        if (OB_FAIL(init_col_aggregator<ObColSumAggregator>(
            is_major, full_col_descs.at(col_idx), major_working_cluster_version, agg_res_datum, agg_datum_attr, allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Not supported skip index aggregate type", K(ret), K(idx_type));
      }
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < col_aggs_.count(); ++i) {
        allocator.free(col_aggs_.at(i));
        col_aggs_.at(i) = nullptr;
      }
      col_aggs_.clear();
      for (int64_t i = 0; i < multi_col_aggs_.count(); ++i) {
        allocator.free(multi_col_aggs_.at(i));
        multi_col_aggs_.at(i) = nullptr;
      }
      multi_col_aggs_.clear();
      single_col_agg_idxes_.clear();
    }
  }
  return ret;
}

int ObISkipIndexAggregator::init_multi_col_aggregators(
    const int64_t multi_col_agg_count,
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    ObSkipIndexAggResult &agg_result_row,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // Since BM25 max_score is the only multi col agg for now, build a bm25 aggregator directly
  ObIMultiColAggregator *multi_col_agg = nullptr;
  if (0 == multi_col_agg_count) {
    // skip
  } else if (OB_FAIL(multi_col_aggs_.reserve(multi_col_agg_count))) {
    LOG_WARN("Fail to reserve multi col aggregator array", K(ret));
  } else if (OB_ISNULL(multi_col_agg = OB_NEWx(ObBM25ParamAggregator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for bm25 param aggregator", K(ret));
  } else if (OB_FAIL(multi_col_agg->init(full_agg_metas, full_col_descs, agg_result_row, allocator))) {
    LOG_WARN("Fail to init bm25 param aggregator", K(ret));
  } else if (OB_FAIL(multi_col_aggs_.push_back(multi_col_agg))) {
    LOG_WARN("Fail to append multi col aggregator to array", K(ret));
  }

  if (OB_FAIL(ret) && nullptr != multi_col_agg) {
    OB_DELETEx(ObIMultiColAggregator, &allocator, multi_col_agg);
    multi_col_agg = nullptr;
  }
  return ret;
}

template <typename T>
int ObISkipIndexAggregator::init_col_aggregator(
    const bool is_major,
    const ObColDesc &col_desc,
    const int64_t major_working_cluster_version,
    ObStorageDatum &result_datum,
    ObSkipIndexDatumAttr &result_attr,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObIColAggregator *col_aggregator = nullptr;
  void *buf = nullptr;
  const ObObjType col_type = col_desc.col_type_.get_type();
  if (!is_skip_index_while_list_type(col_type) &&
      !is_skip_index_black_list_type(col_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported skip index on column with unknown column type", K(col_desc), K(col_type));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for column aggregator", K(ret));
  } else if (FALSE_IT(col_aggregator = new (buf) T())) {
  } else if (OB_FAIL(col_aggregator->init(is_major, col_desc, major_working_cluster_version, result_datum, result_attr))) {
    LOG_WARN("Fail to init column aggregator", K(ret), K(col_desc));
  } else if (OB_FAIL(col_aggs_.push_back(col_aggregator))) {
    LOG_WARN("Fail to append col aggregator to array", K(ret), K(col_desc));
  }

  if (OB_FAIL(ret) && nullptr != buf) {
    allocator.free(buf);
    col_aggregator = nullptr;
  }
  return ret;
}

ObSkipIndexIndexAggregator::ObSkipIndexIndexAggregator() : ObISkipIndexAggregator()
{}

int ObSkipIndexIndexAggregator::eval(const ObSkipIndexAggResult &agg_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!need_aggregate_) {
    // skip
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      const ObStorageDatum &datum = agg_row.get_agg_datum_row().storage_datums_[i];
      const ObSkipIndexDatumAttr &agg_datum_attr = agg_row.get_agg_attrs().at(i);
      const int32_t single_col_agg_idx = single_col_agg_idxes_.at(i);
      if (!full_agg_metas_->at(i).is_single_col_agg()) {
        // skip
      } else if (OB_UNLIKELY(single_col_agg_idx == -1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected single col agg idx", K(ret), K(single_col_agg_idx), K(full_agg_metas_->at(i)));
      } else if (OB_UNLIKELY(datum.is_ext() && !datum.is_nop())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexcepted extended datum" , K(ret), K(datum), K(agg_row), K(full_agg_metas_->at(i)));
      } else if (OB_FAIL(col_aggs_.at(single_col_agg_idx)->eval(datum, agg_datum_attr))) {
        col_aggs_.at(single_col_agg_idx)->set_not_aggregate();
        LOG_ERROR("Fail to eval aggregate column", K(ret), K(datum),
            K(full_agg_metas_->at(i)), K(i), K(single_col_agg_idx), K(col_aggs_.at(single_col_agg_idx)->get_col_desc()));
        ret = OB_SUCCESS;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < multi_col_aggs_.count(); ++i) {
      if (OB_FAIL(multi_col_aggs_.at(i)->eval(agg_row))) {
        LOG_WARN("Fail to eval multi col aggregate", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      evaluated_ = true;
    }
  }
  return ret;
}

ObSkipIndexDataAggregator::ObSkipIndexDataAggregator() : ObISkipIndexAggregator()
{}

int ObSkipIndexDataAggregator::eval(const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!need_aggregate_) {
    // skip
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas_->at(i);
      const int64_t datum_idx = idx_col_meta.col_idx_;
      const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
      ObSkipIndexDatumAttr agg_datum_attr(true, false);
      const int32_t single_col_agg_idx = single_col_agg_idxes_.at(i);
      if (!full_agg_metas_->at(i).is_single_col_agg()) {
        // skip
      } else if (OB_FAIL(col_aggs_.at(single_col_agg_idx)->eval(datum, agg_datum_attr))) {
        col_aggs_.at(single_col_agg_idx)->set_not_aggregate();
        LOG_ERROR("Fail to eval aggregate column", K(ret), K(datum),
            K(idx_col_meta), K(i), K(single_col_agg_idx), K(col_aggs_.at(single_col_agg_idx)->get_col_desc()));
        ret = OB_SUCCESS;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < multi_col_aggs_.count(); ++i) {
      if (OB_FAIL(multi_col_aggs_.at(i)->eval(datum_row))) {
        LOG_WARN("Fail to eval multi col aggregate", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      evaluated_ = true;
    }
  }
  return ret;
}

int ObSkipIndexDataAggregator::eval(const ObIMicroBlockWriter &data_micro_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_aggregate_) {
  } else {
    ObMicroDataPreAggParam pre_agg_param;
    ObStorageDatum tmp_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas_->at(i);
      bool evaluated = false;
      const int32_t single_col_agg_idx = single_col_agg_idxes_.at(i);
      if (!full_agg_metas_->at(i).is_single_col_agg()) {
        // skip
      } else if (OB_FAIL(data_micro_writer.get_pre_agg_param(idx_col_meta.col_idx_, pre_agg_param))) {
        LOG_WARN("failed to get pre agg param", K(ret), K(idx_col_meta), K(data_micro_writer));
      } else if (OB_ISNULL(pre_agg_param.col_datums_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr to column datums", K(ret));
      } else {
        if (ObSkipIndexColType::SK_IDX_NULL_COUNT == idx_col_meta.col_type_) {
          tmp_result.set_int(pre_agg_param.null_cnt_);
          ObSkipIndexDatumAttr agg_datum_attr;
          if (OB_FAIL(col_aggs_.at(single_col_agg_idx)->eval(tmp_result, agg_datum_attr))) {
            LOG_WARN("failed to evaluate ", K(ret), K(tmp_result));
          }
        } else if (can_use_pre_agg_integer(idx_col_meta) && pre_agg_param.is_integer_aggregated_) {
          if (OB_FAIL(do_col_agg_with_pre_agg_integer(single_col_agg_idx, idx_col_meta, pre_agg_param))) {
            LOG_WARN("failed to do column aggregation with pre agg integer", K(ret), K(i), K(idx_col_meta));
          }
        } else if (can_agg_with_dict(static_cast<ObSkipIndexColType>(idx_col_meta.col_type_))) {
          // try aggregate with dict
          if (pre_agg_param.is_all_null_column()) {
            tmp_result.set_null();
            if (OB_FAIL(col_aggs_.at(single_col_agg_idx)->eval(tmp_result, false))) {
              LOG_WARN("failed to evaluate all null column", K(ret), K(pre_agg_param));
            }
          } else if (pre_agg_param.use_cs_encoding_ht()) {
            if (OB_FAIL(do_col_agg(single_col_agg_idx, *pre_agg_param.cs_encoding_ht_))) {
              LOG_WARN("failed to do column aggregation with cs encoding hashtable", K(ret));
            }
          } else if (pre_agg_param.use_encoding_ht()) {
            if (OB_FAIL(do_col_agg(single_col_agg_idx, *pre_agg_param.encoding_ht_))) {
              LOG_WARN("failed to do column aggregation with encoding hashtable", K(ret));
            }
          } else if (OB_FAIL(do_col_agg(single_col_agg_idx, *pre_agg_param.col_datums_))) {
            LOG_WARN("failed to do column aggregation with column datum array", K(ret));
          }
        } else if (OB_FAIL(do_col_agg(single_col_agg_idx, *pre_agg_param.col_datums_))) {
          LOG_WARN("failed to do column aggregation with column datum array", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < multi_col_aggs_.count(); ++i) {
      if (OB_FAIL(multi_col_aggs_.at(i)->eval(data_micro_writer))) {
        LOG_WARN("failed to eval multi col aggregate", K(ret), K(i));
      }
    }

    if (OB_SUCC(ret)) {
      evaluated_ = true;
    }
  }
  return ret;
}

bool ObSkipIndexDataAggregator::can_use_pre_agg_integer(const ObSkipIndexColMeta &col_meta)
{
  const ObObjTypeClass type_class = full_col_descs_->at(col_meta.col_idx_).col_type_.get_type_class();
  const ObSkipIndexColType idx_type = static_cast<ObSkipIndexColType>(col_meta.col_type_);

  const bool data_type_valid = (ObIntTC == type_class || ObUIntTC == type_class);
  const bool idx_type_valid = (SK_IDX_MIN == idx_type || SK_IDX_MAX == idx_type);

  return data_type_valid && idx_type_valid;
}

int ObSkipIndexDataAggregator::do_col_agg_with_pre_agg_integer(
    const int64_t agg_idx,
    const ObSkipIndexColMeta &col_meta,
    const ObMicroDataPreAggParam &agg_param)
{
  int ret = OB_SUCCESS;
  uint64_t pre_agg_result = 0;
  const ObObjTypeClass type_class = full_col_descs_->at(col_meta.col_idx_).col_type_.get_type_class();
  const ObSkipIndexColType idx_type = static_cast<ObSkipIndexColType>(col_meta.col_type_);
  const ObObjDatumMapType datum_type =
      ObDatum::get_obj_datum_map_type(full_col_descs_->at(col_meta.col_idx_).col_type_.get_type());
  ObStorageDatum tmp_result;
  if (SK_IDX_MIN == idx_type) {
    pre_agg_result = agg_param.min_integer_;
  } else if (SK_IDX_MAX == idx_type) {
    pre_agg_result = agg_param.max_integer_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected skip index type", K(ret), K(idx_type));
  }

  if (OB_FAIL(ret)) {
  } else if (agg_param.is_all_null_column()) {
    tmp_result.set_null();
  } else if (type_class == ObIntTC) {
    switch (datum_type) {
      case OBJ_DATUM_8BYTE_DATA: {
        tmp_result.set_int(static_cast<int64_t>(pre_agg_result));
        break;
      }
      case OBJ_DATUM_4BYTE_DATA: {
        tmp_result.set_int32(static_cast<int32_t>(pre_agg_result));
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data type", K(ret), K(datum_type), K(type_class));
    }
  } else if (type_class == ObUIntTC) {
    switch (datum_type) {
      case OBJ_DATUM_8BYTE_DATA: {
        tmp_result.set_uint(pre_agg_result);
        break;
      }
      case OBJ_DATUM_4BYTE_DATA: {
        tmp_result.set_uint32(static_cast<uint32_t>(pre_agg_result));
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected data type", K(ret), K(datum_type), K(type_class));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data type class", K(ret), K(type_class));
  }
  ObSkipIndexDatumAttr agg_datum_attr;
  if (FAILEDx(col_aggs_.at(agg_idx)->eval(tmp_result, agg_datum_attr))) {
    LOG_WARN("failed to do aggregation", K(ret), K(agg_idx), K(tmp_result));
  }
  return ret;
}

template <typename IterParamType>
struct SkipIndexDatumIterTypeTrait { typedef void IterType; };
template<> struct SkipIndexDatumIterTypeTrait<ObPodFix2dArray<ObDatum, 1 << 20, common::OB_MALLOC_NORMAL_BLOCK_SIZE>> { typedef ObColumnDatumIter IterType; };
template<> struct SkipIndexDatumIterTypeTrait<ObDictEncodingHashTable> { typedef ObDictDatumIter IterType; };
template<> struct SkipIndexDatumIterTypeTrait<ObEncodingHashTable> { typedef ObEncodingHashTableDatumIter IterType; };

template<typename IterParamType>
int ObSkipIndexDataAggregator::do_col_agg(const int64_t agg_idx, const IterParamType &iter_param)
{
  int ret = OB_SUCCESS;
  typedef typename SkipIndexDatumIterTypeTrait<IterParamType>::IterType DatumIterType;
  DatumIterType datum_iter(iter_param);
  if (OB_FAIL(col_aggs_.at(agg_idx)->eval(datum_iter))) {
    LOG_WARN("failed to do column aggregation with datum iter");
  }
  return ret;
}

/* ------------------------------------ObAggregateInfo-------------------------------------*/
ObAggregateInfo::ObAggregateInfo()
  : row_count_(0), row_count_delta_(0), max_merged_trans_version_(0), macro_block_count_(0),
    micro_block_count_(0), can_mark_deletion_(true), contain_uncommitted_row_(false),
    has_string_out_row_(false), has_lob_out_row_(false), is_last_row_last_flag_(false),
    is_first_row_first_flag_(false)
{
}

ObAggregateInfo::~ObAggregateInfo()
{
  reset();
}

void ObAggregateInfo::reset()
{
  row_count_ = 0;
  row_count_delta_ = 0;
  max_merged_trans_version_ = 0;
  macro_block_count_ = 0;
  micro_block_count_ = 0;
  can_mark_deletion_ = true;
  contain_uncommitted_row_ = false;
  has_string_out_row_ = false;
  has_lob_out_row_ = false;
  is_last_row_last_flag_ = false;
  is_first_row_first_flag_ = false;
}

void ObAggregateInfo::eval(const ObIndexBlockRowDesc &row_desc)
{
  if (0 == row_count_) {
    is_first_row_first_flag_ = row_desc.is_first_row_first_flag_;
  }
  row_count_ += row_desc.row_count_;
  row_count_delta_ += row_desc.row_count_delta_;
  can_mark_deletion_ = can_mark_deletion_ && row_desc.is_deleted_;
  max_merged_trans_version_ =
      max_merged_trans_version_ > row_desc.max_merged_trans_version_
      ? max_merged_trans_version_ : row_desc.max_merged_trans_version_;
  macro_block_count_ += row_desc.macro_block_count_;
  micro_block_count_ += row_desc.micro_block_count_;
  contain_uncommitted_row_ =
      contain_uncommitted_row_ || row_desc.contain_uncommitted_row_;
  has_string_out_row_ =
      has_string_out_row_ || row_desc.has_string_out_row_;
  has_lob_out_row_ = has_lob_out_row_ || row_desc.has_lob_out_row_;
  is_last_row_last_flag_ = row_desc.is_last_row_last_flag_;
}

void ObAggregateInfo::get_agg_result(ObIndexBlockRowDesc &row_desc) const
{
  row_desc.row_count_ = row_count_;
  row_desc.row_count_delta_ = row_count_delta_;
  row_desc.is_deleted_ = can_mark_deletion_;
  row_desc.max_merged_trans_version_ = max_merged_trans_version_;
  row_desc.macro_block_count_ = macro_block_count_;
  row_desc.micro_block_count_ = micro_block_count_;
  row_desc.contain_uncommitted_row_ = contain_uncommitted_row_;
  row_desc.has_string_out_row_ = has_string_out_row_;
  row_desc.has_lob_out_row_ = has_lob_out_row_;
  row_desc.is_last_row_last_flag_ = is_last_row_last_flag_;
  row_desc.is_first_row_first_flag_ = is_first_row_first_flag_;
}

/* ------------------------------------ObAggregateInfo-------------------------------------*/

ObIndexRowAggInfo::ObIndexRowAggInfo()
  : aggregated_row_(), aggregate_info_(), need_data_aggregate_(false) {}

ObIndexRowAggInfo::~ObIndexRowAggInfo()
{
  reset();
}

void ObIndexRowAggInfo::reset()
{
  aggregated_row_.reset();
  aggregate_info_.reset();
  need_data_aggregate_ = false;
}


/* ------------------------------------ObIndexBlockAggregator-------------------------------------*/

ObIndexBlockAggregator::ObIndexBlockAggregator()
    : skip_index_aggregator_(),
      aggregate_info_(),
      need_data_aggregate_(false),
      has_reused_null_agg_in_this_micro_block_(false),
      is_inited_(false)
{
}

void ObIndexBlockAggregator::reset()
{
  skip_index_aggregator_.reset();
  aggregate_info_.reset();
  need_data_aggregate_ = false;
  has_reused_null_agg_in_this_micro_block_ = false;
  is_inited_ = false;
}

void ObIndexBlockAggregator::reuse()
{
  skip_index_aggregator_.reuse();
  aggregate_info_.reset();
  has_reused_null_agg_in_this_micro_block_ = false;
}

int ObIndexBlockAggregator::init(const ObDataStoreDesc &store_desc, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Already inited", K(ret));
  } else {
    need_data_aggregate_ = store_desc.get_agg_meta_array().count() != 0;
    if (!need_data_aggregate_) {
    } else if (OB_FAIL(skip_index_aggregator_.init(
        store_desc.is_major_or_meta_merge_type(),
        store_desc.get_agg_meta_array(),
        store_desc.get_full_stored_col_descs(),
        store_desc.get_major_working_cluster_version(),
        allocator))) {
      LOG_WARN("Fail to init skip index aggregator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockAggregator::eval(const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index block row descriptor", K(ret));
  } else if (need_data_aggregate()) {
    if (OB_ISNULL(row_desc.aggregated_row_)) {
      // There is data that does not contain aggregate row, so we disable skip index aggregate, do nothing here.
      has_reused_null_agg_in_this_micro_block_ = true;
    } else if (row_desc.is_serialized_agg_row_) {
      const ObAggRowHeader *agg_row_header = reinterpret_cast<const ObAggRowHeader *>(
          row_desc.serialized_agg_row_buf_);
      if (OB_UNLIKELY(!agg_row_header->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid aggregated row header", K(ret), KPC(agg_row_header));
      } else if (OB_FAIL(skip_index_aggregator_.ObISkipIndexAggregator::eval(
          row_desc.serialized_agg_row_buf_, agg_row_header->length_, row_desc.row_count_))) {
        LOG_WARN("Fail to aggregate serialized index row", K(ret), K(row_desc), KPC(agg_row_header));
      }
    } else if (OB_FAIL(skip_index_aggregator_.eval(*row_desc.aggregated_row_))) {
      LOG_WARN("Fail to aggregate on index row", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    aggregate_info_.eval(row_desc);
  }
  return ret;
}

int ObIndexBlockAggregator::get_index_agg_result(ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (need_data_aggregate()
             && OB_FAIL(skip_index_aggregator_.get_aggregated_row(row_desc.aggregated_row_))) {
    LOG_WARN("Fail to get aggregated row", K(ret));
  } else {
    aggregate_info_.get_agg_result(row_desc);
  }
  return ret;
}

int ObIndexBlockAggregator::get_index_row_agg_info(ObIndexRowAggInfo &index_row_agg_info,
                                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObSkipIndexAggResult *agg_row = nullptr;
  index_row_agg_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (need_data_aggregate()) {
    if (OB_FAIL(skip_index_aggregator_.get_aggregated_row(agg_row))) {
      LOG_WARN("Fail to get aggregated row", K(ret));
    } else if (OB_FAIL(index_row_agg_info.aggregated_row_.init(agg_row->get_agg_col_cnt(), allocator))) {
      LOG_WARN("Fail to init aggregated row", K(ret), KPC(agg_row));
    } else if (OB_FAIL(index_row_agg_info.aggregated_row_.deep_copy(*agg_row, allocator))) {
      LOG_WARN("Failed to deep copy datum row", K(ret), KPC(agg_row));
    }
  }
  if (OB_SUCC(ret)) {
    index_row_agg_info.aggregate_info_ = aggregate_info_;
    index_row_agg_info.need_data_aggregate_ = need_data_aggregate();
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
