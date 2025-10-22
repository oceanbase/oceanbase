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
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"
namespace oceanbase
{
namespace blocksstable
{

int ObSkipIndexFilterExecutor::init(const int64_t batch_size, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSkipIndexFilterExecutor has been inited", K(ret));
  } else if (OB_UNLIKELY(batch_size <= 0 || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid batch_size or allocator", K(ret), K(batch_size), KP(allocator));
  } else if (OB_ISNULL(skip_bit_ = sql::to_bit_vector(allocator->alloc(sql::ObBitVector::memory_size(batch_size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for skip bit", K(ret), K(batch_size));
  } else {
    skip_bit_->init(batch_size);
    allocator_ = allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObSkipIndexFilterExecutor::read_aggregate_data(const uint32_t col_idx,
                  common::ObIAllocator &allocator,
                  const share::schema::ObColumnParam *col_param,
                  const ObObjMeta &obj_meta,
                  const bool is_padding_mode,
                  ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  meta_.col_idx_ = col_idx;
  meta_.col_type_ = SK_IDX_NULL_COUNT;
  if (OB_FAIL(agg_row_reader_.read(meta_, param.null_count_))) {
    LOG_WARN("Failed read agg null count", K(ret), K(meta_));
  } else if (FALSE_IT(meta_.col_type_ = SK_IDX_MIN)) {
  } else if (OB_FAIL(agg_row_reader_.read(meta_, param.min_datum_, param.is_min_prefix_))) {
    LOG_WARN("Failed read agg min datum", K(ret), K(meta_));
  } else if (FALSE_IT(meta_.col_type_ = SK_IDX_MAX)) {
  } else if (OB_FAIL(agg_row_reader_.read(meta_, param.max_datum_, param.is_max_prefix_))) {
    LOG_WARN("Failed read agg max datum", K(ret), K(meta_));
  } else if (OB_UNLIKELY(ob_is_string_type(obj_meta.get_type())
      && ObCharset::usemb(obj_meta.get_collation_type())
      && !param.max_datum_.is_null()
      && !agg_row_reader_.has_correct_max_prefix()
      && param.max_datum_.len_ > ObSkipIndexColMeta::SAFE_MBCHARSET_PREFIX_MAX_LEN)) {
    // Invalid agg result for string type whose prefix max might not accurate on mbcharset
    param.min_datum_.set_null();
    param.max_datum_.set_null();
  } else if (!param.min_datum_.is_null() && !param.is_min_prefix_ &&
             OB_FAIL(pad_column(obj_meta, col_param, is_padding_mode, allocator, param.min_datum_))) {
    LOG_WARN("Failed to pad column on min datum", K(ret));
  } else if (!param.max_datum_.is_null() && !param.is_max_prefix_ &&
             OB_FAIL(pad_column(obj_meta, col_param, is_padding_mode, allocator, param.max_datum_))){
    LOG_WARN("Failed to pad column on max datum", K(ret));
  }
  LOG_DEBUG("[SKIP INDEX] read aggregate row", K(ret), K(param.null_count_), K(param.min_datum_), K(param.max_datum_));
  return ret;
}

int ObSkipIndexFilterExecutor::falsifiable_pushdown_filter(
    const uint32_t col_idx,
    const ObObjMeta &obj_meta,
    const ObSkipIndexType index_type,
    const ObMicroIndexInfo &index_info,
    sql::ObPhysicalFilterExecutor &filter,
    common::ObIAllocator &allocator,
    const bool use_vectorize)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSkipIndexFilterExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!index_info.has_agg_data())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(index_info));
  } else if (FALSE_IT(agg_row_reader_.reset())) {
  } else if (OB_FAIL(agg_row_reader_.init(index_info.agg_row_buf_, index_info.agg_buf_size_))) {
    LOG_WARN("failed to init agg row reader", K(ret));
  } else {
    switch (index_type) {
      case ObSkipIndexType::MIN_MAX: {
        const share::schema::ObColumnParam *col_param = filter.get_col_params().at(0);
        ObMinMaxFilterParam param;
        if (filter.is_filter_dynamic_node()) {
          sql::ObDynamicFilterExecutor &dynamic_filter =
            static_cast<sql::ObDynamicFilterExecutor &>(filter);
          if (!dynamic_filter.is_data_prepared()) {
            filter.get_filter_bool_mask().set_uncertain();
          } else if (dynamic_filter.is_filter_all_data()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (dynamic_filter.is_pass_all_data()) {
            filter.get_filter_bool_mask().set_always_true();
          } else if (dynamic_filter.is_cmp_op_with_null_ref_value()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (OB_FAIL(read_aggregate_data(col_idx, allocator, col_param, obj_meta,
                                        filter.is_padding_mode(), param))) {
            LOG_WARN("Failed to read min and max", K(ret), K(col_idx));
          } else if (OB_FAIL(filter_on_min_max(col_idx, index_info.get_row_count(),
              param, dynamic_filter))) {
            LOG_WARN("Failed to filter on min_max for dynamic filter", K(ret), K(col_idx));
          }
        } else if (filter.is_filter_white_node()) {
          sql::ObWhiteFilterExecutor &white_filter =
            static_cast<sql::ObWhiteFilterExecutor &>(filter);
          if (white_filter.is_cmp_op_with_null_ref_value()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (OB_FAIL(read_aggregate_data(col_idx, allocator, col_param, obj_meta,
                                        filter.is_padding_mode(), param))) {
            LOG_WARN("Failed to read min and max", K(ret), K(col_idx));
          } else if (OB_FAIL(filter_on_min_max(col_idx, index_info.get_row_count(),
              param, white_filter))) {
            LOG_WARN("Failed to filter on min_max for white filter", K(ret), K(col_idx));
          }
        } else if (filter.is_filter_black_node()) {
          sql::ObBlackFilterExecutor &black_filter =
            static_cast<sql::ObBlackFilterExecutor &>(filter);
          if (OB_FAIL(read_aggregate_data(col_idx, allocator, col_param, obj_meta,
                            filter.is_padding_mode(), param))) {
            LOG_WARN("Failed to read min and max", K(ret), K(col_idx));
          } else if (OB_FAIL(black_filter_on_min_max(col_idx, index_info.get_row_count(),
              param, black_filter, use_vectorize, obj_meta.get_collation_type()))) {
            LOG_WARN("Failed to filter on min_max for black filter", K(ret), K(col_idx));
          }
        }
        break;
      }
      default :
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported skip index type", K(ret), K(index_type));
        break;
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::falsifiable_pushdown_filter(
    const uint32_t col_idx,
    const ObCollationType &cs_type,
    const ObSkipIndexType index_type,
    const int64_t row_count,
    ObMinMaxFilterParam &param,
    sql::ObPhysicalFilterExecutor &filter,
    const bool use_vectorize)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSkipIndexFilterExecutor has not been inited", K(ret));
  } else {
    switch (index_type) {
      case ObSkipIndexType::MIN_MAX: {
        if (filter.is_filter_dynamic_node()) {
          sql::ObDynamicFilterExecutor &dynamic_filter =
            static_cast<sql::ObDynamicFilterExecutor &>(filter);
          if (!dynamic_filter.is_data_prepared()) {
            filter.get_filter_bool_mask().set_uncertain();
          } else if (dynamic_filter.is_filter_all_data()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (dynamic_filter.is_pass_all_data()) {
            filter.get_filter_bool_mask().set_always_true();
          } else if (dynamic_filter.is_cmp_op_with_null_ref_value()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (OB_FAIL(filter_on_min_max(col_idx, row_count,
              param, dynamic_filter))) {
            LOG_WARN("Failed to filter on min_max for dynamic filter", K(ret), K(col_idx));
          }
        } else if (filter.is_filter_white_node()) {
          sql::ObWhiteFilterExecutor &white_filter =
            static_cast<sql::ObWhiteFilterExecutor &>(filter);
          if (white_filter.is_cmp_op_with_null_ref_value()) {
            filter.get_filter_bool_mask().set_always_false();
          } else if (OB_FAIL(filter_on_min_max(col_idx, row_count,
              param, white_filter))) {
            LOG_WARN("Failed to filter on min_max for white filter", K(ret), K(col_idx));
          }
        } else if (filter.is_filter_black_node()) {
          sql::ObBlackFilterExecutor &black_filter =
            static_cast<sql::ObBlackFilterExecutor &>(filter);
          if (OB_FAIL(black_filter_on_min_max(col_idx, row_count,
              param, black_filter, use_vectorize, cs_type))) {
            LOG_WARN("Failed to filter on min_max for black filter", K(ret), K(col_idx));
          }
        }
        break;
      }
      default :
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported skip index type", K(ret), K(index_type));
        break;
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::filter_on_min_max(
    const uint32_t col_idx,
    const uint64_t row_count,
    const ObMinMaxFilterParam &param,
    sql::ObWhiteFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  sql::ObBoolMask &fal_desc = filter.get_filter_bool_mask();
  if (param.is_uncertain()) {
    // min max null_count all null, expect uncertain cause by progressive merge
    fal_desc.set_uncertain();
  } else if (OB_UNLIKELY(param.null_count_.is_null() || param.null_count_.get_int() < 0 || param.null_count_.get_int() > row_count ||
             param.min_datum_.is_null() != param.max_datum_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not correct min_max agg info", K(ret), K(col_idx), K(row_count),
             K(param.null_count_), K(param.min_datum_), K(param.max_datum_));
  } else {
    // following three flags are mutually exclusive, only one can be true
    const bool is_all_null = param.null_count_.get_int() == row_count;
    const bool is_all_not_null = param.null_count_.get_int() == 0;
    const bool has_null = param.null_count_.get_int() > 0 && param.null_count_.get_int() < row_count;
    const bool is_min_max_null = param.min_datum_.is_null() && param.max_datum_.is_null(); //for unsupported data, eg: lob out row, json ...
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    switch (op_type) {
      case sql::WHITE_OP_NU: {
        if (is_all_not_null) {
          fal_desc.set_always_false();
        } else if (is_all_null) {
          fal_desc.set_always_true();
        } else {
          fal_desc.set_uncertain();
        }
        break;
      }
      case sql::WHITE_OP_NN: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_all_not_null) {
          fal_desc.set_always_true();
        } else {
          fal_desc.set_uncertain();
        }
        break;
      }
      case sql::WHITE_OP_EQ: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(eq_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run EQ operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_NE: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(ne_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run NE operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_GT: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(gt_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run GT operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_GE: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(ge_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run GE operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_LT: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(lt_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run LT operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_LE: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(le_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run LE operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(in_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run IN operator", K(ret));
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        if (is_all_null) {
          fal_desc.set_always_false();
        } else if (is_min_max_null) {
          fal_desc.is_uncertain();
        } else if (OB_FAIL(bt_operator(filter, param.min_datum_, param.is_min_prefix_, param.max_datum_, param.is_max_prefix_, fal_desc))) {
          LOG_WARN("Failed to run BT operator", K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unexpected filter pushdown operation type", K(ret), K(op_type));
      }
    } // end of switch
    if (OB_SUCC(ret)) {
      if (has_null && fal_desc.is_always_true()) {
        fal_desc.set_uncertain();
      }
    }
    LOG_TRACE("[SKIP INDEX] filter on min max", K(ret), K(fal_desc), K(param), K(filter));
  }
  return ret;
}

inline int ObSkipIndexFilterExecutor::pad_column(const ObObjMeta &obj_meta,
                                          const share::schema::ObColumnParam *col_param,
                                          const bool is_padding_mode,
                                          common::ObIAllocator &padding_alloc,
                                          blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (is_padding_mode && obj_meta.is_fixed_len_char_type()) {
    if (OB_FAIL(storage::pad_column(obj_meta, col_param->get_accuracy(),
                                    padding_alloc, datum))) {
      LOG_WARN("Failed to pad column", K(ret));
    }
  }
  return ret;
}

inline int ObSkipIndexFilterExecutor::compare_with_prefix(const sql::ObWhiteFilterExecutor &filter,
                                                          const common::ObDatum &datum,
                                                          const common::ObDatum &filter_datum,
                                                          ObSkipIndexCmpRes &res)
{
  int ret = OB_SUCCESS;
  res.set_uncertain();
  const ObObjMeta &filter_datum_meta = filter.get_param_obj_meta();
  if (!is_lob_storage(filter_datum_meta.get_type())) {
    const ObObjMeta &datum_meta = filter.get_col_obj_meta();
    ObString filter_datum_str = filter_datum.get_string();
    ObString datum_str;
    if (is_lob_storage(datum_meta.get_type())) {
      const ObLobCommon &datum_lob = datum.get_lob_data();
      datum_str = ObString(datum_lob.get_byte_size(datum.len_), datum_lob.get_inrow_data_ptr());
    } else {
      datum_str = datum.get_string();
    }
    const int64_t datum_char_num = ObCharset::strlen_char(datum_meta.get_collation_type(), datum_str.ptr(), datum_str.length());
    const int64_t filter_datum_char_num = ObCharset::strlen_char(filter_datum_meta.get_collation_type(), filter_datum_str.ptr(), filter_datum_str.length());
    if (datum_char_num >= filter_datum_char_num) {
      res.set_certain();
    } else {
      const int64_t filter_datum_prefix_len = ObCharset::charpos(filter_datum_meta.get_collation_type(), filter_datum_str.ptr(), filter_datum_str.length(), datum_char_num, &ret);
      ObDatum filter_prefix_datum = filter_datum;
      filter_prefix_datum.len_ = filter_datum_prefix_len;
      ObDatumCmpFuncType cmp_func = filter.cmp_func_;
      int cmp_ret = 0;
      if (OB_FAIL(ret)) {
        LOG_WARN("Failed to get charpos", K(ret), K(datum), K(filter_datum), K(datum_meta), K(filter_datum_meta));
      } else if (OB_FAIL(cmp_func(datum, filter_prefix_datum, cmp_ret))) {
        LOG_WARN("Failed to compare datum", K(ret), K(datum), K(filter_prefix_datum));
      } else if (cmp_ret == res.cmp_res_) {
        res.set_certain();
      } else if (cmp_ret == 0) {
        res.set_uncertain();
      } else if (cmp_ret != res.cmp_res_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cmp result", K(ret), K(cmp_ret), K(res), K(datum), K(filter_datum));
      }
    }
  }
  return ret;
}

// PAD charset comparison will pad the shorter string with pad space.
int ObSkipIndexFilterExecutor::compare_for_pad_charset(const sql::ObWhiteFilterExecutor &filter,
                                                       const common::ObDatum &skip_datum,
                                                       const bool is_prefix,
                                                       const common::ObDatum &filter_datum,
                                                       ObSkipIndexCmpRes &res)
{
  int ret = OB_SUCCESS;
  res.reset();
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;
  if (OB_FAIL(cmp_func(skip_datum, filter_datum, res.cmp_res_))) {
    LOG_WARN("Failed to compare datum", K(ret), K(skip_datum), K(filter_datum));
  } else if (!is_prefix) {
    res.set_certain();
  } else if (res.cmp_res_ == 0) {
    res.set_uncertain();
  } else if (OB_FAIL(compare_with_prefix(filter, skip_datum, filter_datum, res))) {
    LOG_WARN("Failed to compare with prefix", K(ret), K(skip_datum), K(filter_datum));
  }
  return ret;
}

// Non-PAD charset comparison will not pad the shorter string with pad space.
int ObSkipIndexFilterExecutor::compare_for_non_pad_charset(const sql::ObWhiteFilterExecutor &filter,
                                                           const common::ObDatum &skip_datum,
                                                           const bool is_prefix,
                                                           const common::ObDatum &filter_datum,
                                                           ObSkipIndexCmpRes &res)
{
  int ret = OB_SUCCESS;
  res.reset();
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;
  if (OB_FAIL(cmp_func(skip_datum, filter_datum, res.cmp_res_))) {
    LOG_WARN("Failed to compare datum", K(ret), K(skip_datum), K(filter_datum));
  } else if (!is_prefix) {
    res.set_certain();
  } else if (res.cmp_res_ >= 0) {
    res.cmp_res_ = 1;
    res.set_certain();
  } else if (OB_FAIL(compare_with_prefix(filter, skip_datum, filter_datum, res))) {
    LOG_WARN("Failed to compare with prefix", K(ret), K(skip_datum), K(filter_datum));
  }
  return ret;
}

int ObSkipIndexFilterExecutor::compare(const sql::ObWhiteFilterExecutor &filter,
                                       const common::ObDatum &skip_datum,
                                       const bool is_prefix,
                                       const common::ObDatum &filter_datum,
                                       ObSkipIndexCmpRes &res)
{
  int ret = OB_SUCCESS;
  bool is_pad_coll = false;
  if (OB_FAIL(ObCharset::is_pad_charset(filter.get_col_obj_meta().get_collation_type(), is_pad_coll))) {
    LOG_WARN("Failed to check pad collation type", K(ret), K(filter.get_col_obj_meta().get_collation_type()));
  } else if (is_pad_coll) {
    if (OB_FAIL(compare_for_pad_charset(filter, skip_datum, is_prefix, filter_datum, res))) {
      LOG_WARN("Failed to compare for pad charset", K(ret), K(skip_datum), K(filter_datum));
    }
  } else if (OB_FAIL(compare_for_non_pad_charset(filter, skip_datum, is_prefix, filter_datum, res))) {
    LOG_WARN("Failed to compare for non pad charset", K(ret), K(skip_datum), K(filter_datum));
  }
  return ret;
}

int ObSkipIndexFilterExecutor::eq_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable EQ operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_gt()) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_lt()) {
      fal_desc.set_always_false();
    } else if (min_cmp_res.is_eq() && max_cmp_res.is_eq()) {
      fal_desc.set_always_true();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::ne_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable NE operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_gt()) {
      fal_desc.set_always_true();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_lt()) {
      fal_desc.set_always_true();
    } else if (min_cmp_res.is_eq() && max_cmp_res.is_eq()) {
      fal_desc.set_always_false();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::gt_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable GT operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_gt()) {
      fal_desc.set_always_true();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_le()) {
      fal_desc.set_always_false();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::ge_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable GE operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_ge()) {
      fal_desc.set_always_true();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_lt()) {
      fal_desc.set_always_false();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::lt_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable LT operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_ge()) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_lt()) {
      fal_desc.set_always_true();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::le_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 1 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable LE operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = datums.at(0);
    ObSkipIndexCmpRes min_cmp_res;
    ObSkipIndexCmpRes max_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_datum, min_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_datum));
    } else if (min_cmp_res.is_gt()) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, max_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
    } else if (max_cmp_res.is_le()) {
      fal_desc.set_always_true();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::in_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  const sql::ObExpr *col_expr = filter.get_filter_node().expr_;
  bool is_pad_coll = false;
  if (OB_UNLIKELY(nullptr == col_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable IN operator", K(ret), K(filter));
  } else if (filter.null_param_contained()) {
    fal_desc.set_always_false();
  } else if (OB_FAIL(ObCharset::is_pad_charset(filter.get_col_obj_meta().get_collation_type(), is_pad_coll))) {
    LOG_WARN("Failed to check pad collation type", K(ret), K(filter.get_col_obj_meta().get_collation_type()));
  } else if (is_pad_coll && (is_min_prefix || is_max_prefix)) {
    fal_desc.set_uncertain();
  } else {
    ObDatumCmpFuncType cmp_func = filter.cmp_func_;
    ObSkipIndexCmpRes cmp_res;
    bool equal = false;
    ObDatumComparator cmp_rev(cmp_func, ret, equal, true);
    int64_t pos = 0;
    if (is_min_prefix) {
      // attention: equal is not correct in upper_bound
      pos = std::upper_bound(datums.get_data(), datums.get_data() + datums.count(), min_datum, cmp_rev) - datums.get_data();
      equal = false;
    } else {
      pos = std::lower_bound(datums.get_data(), datums.get_data() + datums.count(), min_datum, cmp_rev) - datums.get_data();
    }
    if (OB_FAIL(ret)) {
    } else if (pos < 0 || pos > datums.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected binary search result", K(ret), K(pos), K(datums.count()));
    } else if (pos == datums.count()) { // datums[datums.count()-1] < min_datum <= max_datum
      fal_desc.set_always_false();
    } else {
      const ObDatum &ref_datum = datums.at(pos);
      if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_datum, cmp_res))) {
        LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_datum));
      } else if (cmp_res.is_gt()) { // min_datum <= datums[pos] < max_datum
        fal_desc.set_uncertain();
      } else if (cmp_res.is_lt()) { // min_datum <= max_datum < datums[0] or datums[pos-1] < min_datum <= max_datum < datums[pos]
        fal_desc.set_always_false();
      } else if (equal) { // min_datum == max_datum == datums[pos]
        if (!is_max_prefix) {
          fal_desc.set_always_true();
        } else {
          fal_desc.set_uncertain();
        }
      } else {
        fal_desc.set_uncertain(); // min_datum != max_datum and max_datum == datums[pos]
      }
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::bt_operator(const sql::ObWhiteFilterExecutor &filter,
                                           const common::ObDatum &min_datum,
                                           const bool &is_min_prefix,
                                           const common::ObDatum &max_datum,
                                           const bool &is_max_prefix,
                                           sql::ObBoolMask &fal_desc)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(datums.count() != 2 || filter.null_param_contained())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for falsifiable bt operator", K(ret), K(filter));
  } else {
    const ObDatum &ref_left_datum = datums.at(0);
    const ObDatum &ref_right_datum = datums.at(1);
    ObSkipIndexCmpRes min_left_cmp_res;
    ObSkipIndexCmpRes min_right_cmp_res;
    ObSkipIndexCmpRes max_left_cmp_res;
    ObSkipIndexCmpRes max_right_cmp_res;
    if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_right_datum, min_right_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_right_datum));
    } else if (min_right_cmp_res.is_gt()) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_left_datum, max_left_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_left_datum));
    } else if (max_left_cmp_res.is_lt()) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(compare(filter, min_datum, is_min_prefix, ref_left_datum, min_left_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(min_datum), K(ref_left_datum));
    } else if (OB_FAIL(compare(filter, max_datum, is_max_prefix, ref_right_datum, max_right_cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(max_datum), K(ref_right_datum));
    } else if (min_left_cmp_res.is_ge() && max_right_cmp_res.is_le()) {
      fal_desc.set_always_true();
    } else {
      fal_desc.set_uncertain();
    }
  }
  return ret;
}

int ObSkipIndexFilterExecutor::black_filter_on_min_max(
  const uint32_t col_idx,
  const uint64_t row_count,
  ObMinMaxFilterParam &param,
  sql::ObBlackFilterExecutor &filter,
  const bool use_vectorize,
  const ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  sql::ObBoolMask &fal_desc = filter.get_filter_bool_mask();
  bool is_pad_coll = false;
  if (OB_UNLIKELY(!filter.is_monotonic())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid black filter, filter is not monotonic", K(ret), K(filter));
  } else if (param.null_count_.is_null() && param.min_datum_.is_null() && param.max_datum_.is_null()) {
    // min max null_count all null, expect uncertain cause by progressive merge
    fal_desc.set_uncertain();
  } else if (OB_UNLIKELY(param.null_count_.is_null() || param.null_count_.get_int() < 0 || param.null_count_.get_int() > row_count ||
             param.min_datum_.is_null() != param.max_datum_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not correct min_max agg info", K(ret), K(col_idx), K(row_count),
             K(param.null_count_), K(param.min_datum_), K(param.max_datum_));
  } else if (use_vectorize &&
             filter.get_op().enable_rich_format_ &&
             OB_FAIL(init_exprs_uniform_header(filter.get_cg_col_exprs(),
                                               filter.get_op().get_eval_ctx(),
                                               filter.get_op().get_eval_ctx().max_batch_size_))) {
    LOG_WARN("Failed to init exprs vector header", K(ret));
  } else if (OB_FAIL(ObCharset::is_pad_charset(cs_type, is_pad_coll))) {
    LOG_WARN("Failed to check pad collation type", K(ret), K(cs_type));
  } else {
    const bool is_all_null = param.null_count_.get_int() == row_count;
    const bool has_null = param.null_count_.get_int() > 0 && param.null_count_.get_int() < row_count;
    if (is_all_null) {
      fal_desc.set_always_false();
    } else if (OB_FAIL(check_skip_by_monotonicity(filter,
                                                  param.min_datum_,
                                                  param.is_min_prefix_,
                                                  param.max_datum_,
                                                  param.is_max_prefix_,
                                                  *skip_bit_,
                                                  has_null,
                                                  is_pad_coll,
                                                  nullptr, /*result_bitmap*/
                                                  fal_desc))) {
      LOG_WARN("Failed to check can skip by monotonicity", K(ret), K(param.min_datum_), K(param.max_datum_), K(has_null), K(filter));
    }
  }
  LOG_DEBUG("Utilize skip index judge black filter", K(ret), K(fal_desc), K(row_count), K(param), K(filter));
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
