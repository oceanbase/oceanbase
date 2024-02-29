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
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

int ObIColAggregator::init(const ObColDesc &col_desc, ObStorageDatum &result)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else {
    col_desc_ = col_desc;
    result_ = &result;
    result_->set_null();
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
  if (OB_UNLIKELY(src.is_outrow())|| OB_ISNULL(dst.ptr_) ||
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

int ObColNullCountAggregator::init(const ObColDesc &col_desc, ObStorageDatum &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(col_desc, result))) {
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

int ObColNullCountAggregator::eval(const ObStorageDatum &datum, const bool is_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datum.is_ext()) || OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected aggregate status", K(ret), K(datum), KP_(result));
  } else if (!can_aggregate_) {
    // Skip
  } else if (is_data) {
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

int ObColMaxAggregator::init(const ObColDesc &col_desc, ObStorageDatum &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(col_desc, result))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type());
    cmp_func_ = basic_funcs->null_first_cmp_;
    LOG_DEBUG("[SKIP INDEX] init max aggregator", K(col_desc_), K(can_aggregate_));
  }
  return ret;
}

void ObColMaxAggregator::reuse()
{
  ObIColAggregator::reuse();
}

int ObColMaxAggregator::eval(const ObStorageDatum &datum, const bool is_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_ || datum.is_nop()) {
    // Skip
  } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), datum)){
    set_not_aggregate();
  } else {
    int cmp_res = 0;
    if (OB_FAIL(cmp_func_(datum, *result_, cmp_res))){
      LOG_WARN("Failed to compare datum", K(ret), K(datum), K(*result_), K(col_desc_));
    } else if (cmp_res > 0) {
      if (OB_FAIL(copy_agg_datum(datum, *result_))) {
        LOG_WARN("Fail to copy aggregated datum", K(ret), K(datum), KPC(result_), K(col_desc_));
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
    if (!can_aggregate_) {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}

int ObColMinAggregator::init(const ObColDesc &col_desc, ObStorageDatum &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(col_desc, result))) {
    LOG_WARN("fail to init ObIColAggregator", K(ret));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type());
    cmp_func_ = basic_funcs->null_last_cmp_;
    LOG_DEBUG("[SKIP INDEX] init min aggregator", K(col_desc_), K(can_aggregate_));
  }
  return ret;
}

void ObColMinAggregator::reuse()
{
  ObIColAggregator::reuse();
}

int ObColMinAggregator::eval(const ObStorageDatum &datum, const bool is_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_ || datum.is_nop()) {
    // Skip
  } else if (need_set_not_aggregate(col_desc_.col_type_.get_type(), datum)){
    set_not_aggregate();
  } else {
    int cmp_res = 0;
    if (OB_FAIL(cmp_func_(datum, *result_, cmp_res))){
      LOG_WARN("Failed to compare datum", K(ret), K(datum), K(*result_), K(col_desc_));
    } else if (cmp_res < 0) {
      if (OB_FAIL(copy_agg_datum(datum, *result_))) {
        LOG_WARN("Fail to copy aggregated datum", K(ret), K(datum), KPC(result_), K(col_desc_));
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
    if (!can_aggregate_) {
      result_->set_nop();
    }
    result = result_;
  }
  return ret;
}

int ObColSumAggregator::init(const ObColDesc &col_desc, ObStorageDatum &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIColAggregator::init(col_desc, result))) {
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

int ObColSumAggregator::eval(const ObStorageDatum &datum, const bool is_data)
{
  int ret = OB_SUCCESS;
  ObStorageDatum cast_datum;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!can_aggregate_ || datum.is_nop() || datum.is_null()) {
    // Skip
  } else if (OB_FAIL(choose_eval_func(is_data))) {
    LOG_WARN("fail to choose eval func", K(is_data));
  } else if (OB_FAIL((this->*eval_func_)(datum))) {
    if (OB_INTEGER_PRECISION_OVERFLOW == ret || OB_DECIMAL_PRECISION_OVERFLOW == ret || OB_NUMERIC_OVERFLOW == ret) {
      // sum precision overflow set not aggregate.
      set_not_aggregate();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to eval sum", K(datum), KPC(result_), K(col_desc_));
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

ObSkipIndexAggregator::ObSkipIndexAggregator()
  : allocator_(nullptr),
    col_aggs_(),
    agg_result_(nullptr),
    full_agg_metas_(nullptr),
    full_col_descs_(nullptr),
    max_agg_size_(0),
    is_data_(false),
    need_aggregate_(false),
    evaluated_(false),
    is_inited_(false) {}


void ObSkipIndexAggregator::reset()
{
  agg_result_ = nullptr;
  for (int64_t i = 0; i < col_aggs_.count(); ++i) {
    ObIColAggregator *col_agg = col_aggs_.at(i);
    col_agg->~ObIColAggregator();
    if (allocator_ != nullptr) {
      allocator_->free(col_agg);
    }
  }
  col_aggs_.reset();
  full_agg_metas_ = nullptr;
  full_col_descs_ = nullptr;
  allocator_ = nullptr;
  max_agg_size_ = 0;
  is_data_ = false;
  need_aggregate_ = false;
  evaluated_ = false;
  is_inited_ = false;
}

void ObSkipIndexAggregator::reuse()
{
  for (int64_t i = 0; i < col_aggs_.count(); ++i) {
    col_aggs_.at(i)->reuse();
  }
  evaluated_ = false;
}

int ObSkipIndexAggregator::init(
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    const bool is_data,
    ObDatumRow &agg_result,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (0 == full_agg_metas.count()) {
    need_aggregate_ = false;
    is_inited_ = true;
  } else if (OB_UNLIKELY(agg_result.get_column_count() != full_agg_metas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid aggregate row result column count", K(ret), K(agg_result), K(full_agg_metas));
  } else if (FALSE_IT(agg_result_ = &agg_result)) {
  } else if (FALSE_IT(is_data_ = is_data)) {
  } else if (OB_FAIL(init_col_aggregators(full_agg_metas, full_col_descs, allocator))) {
    LOG_WARN("Fail to init column aggregators", K(ret), K(full_agg_metas), K(full_col_descs));
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

int ObSkipIndexAggregator::eval(const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (!need_aggregate_) {
    // skip
  } else {
    evaluated_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas_->at(i);
      // TODO: for reused aggregated data with progressive merge, the datum_idx might change and need to handle this situation
      const int64_t datum_idx = is_data_ ? idx_col_meta.col_idx_ : i;
      const ObStorageDatum &datum = datum_row.storage_datums_[datum_idx];
      if (datum.is_ext()) {
        if (datum.is_nop()) {
          col_aggs_.at(i)->set_not_aggregate();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexcepted extended datum" , K(ret), K(datum), K(datum_row), K(idx_col_meta));
        }
      } else if (OB_FAIL(col_aggs_.at(i)->eval(datum, is_data_))) {
        col_aggs_.at(i)->set_not_aggregate();
        LOG_ERROR("Fail to eval aggregate column", K(ret), K(datum), K_(is_data),
            K(idx_col_meta), K(i), K(col_aggs_.at(i)->get_col_decs()));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSkipIndexAggregator::eval(const char *buf, const int64_t buf_size, const int64_t row_count)
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
    evaluated_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas_->count(); ++i) {
      tmp_datum.reuse();
      tmp_null_datum.reuse();
      const ObSkipIndexColMeta &idx_col_meta = full_agg_metas_->at(i);
      if (OB_FAIL(agg_row_reader_.read(idx_col_meta, tmp_datum))) {
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
          col_aggs_.at(i)->set_not_aggregate();
        } else if (tmp_null_datum.get_int() > row_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Unexpected null count datum out row count", K(ret),
              K(tmp_null_datum), K(row_count), K(null_col_meta));
        } else if (tmp_null_datum.get_int() < row_count) {
          col_aggs_.at(i)->set_not_aggregate();
        }
      }

      if (FAILEDx(col_aggs_.at(i)->eval(tmp_datum, false))) {
        col_aggs_.at(i)->set_not_aggregate();
        LOG_ERROR("Fail to eval aggregate column", K(ret), K(tmp_datum), K_(is_data),
            K(idx_col_meta), K(i), K(col_aggs_.at(i)->get_col_decs()));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSkipIndexAggregator::get_aggregated_row(const ObDatumRow *&aggregated_row)
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
      if (OB_FAIL(col_aggs_.at(i)->get_result(result))) {
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
      aggregated_row = agg_result_;
      LOG_DEBUG("[SKIP INDEX] generate aggregated row", K(ret), KPC_(agg_result), K(is_data_));
    }
  }
  return ret;
}

int ObSkipIndexAggregator::calc_max_agg_size(
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs)
{
  int ret = OB_SUCCESS;
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
          case ObSkipIndexColType::SK_IDX_MAX: {
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

int ObSkipIndexAggregator::init_col_aggregators(
    const ObIArray<ObSkipIndexColMeta> &full_agg_metas,
    const ObIArray<ObColDesc> &full_col_descs,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  col_aggs_.clear();
  col_aggs_.set_allocator(&allocator);
  if (OB_FAIL(col_aggs_.reserve(full_agg_metas.count()))) {
    LOG_WARN("Fail to reserve col aggregator array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_agg_metas.count(); ++i) {
      const ObSkipIndexColType idx_type = static_cast<ObSkipIndexColType>(full_agg_metas.at(i).col_type_);
      const int64_t col_idx = full_agg_metas.at(i).col_idx_;
      switch (idx_type) {
      case ObSkipIndexColType::SK_IDX_MIN: {
        if (OB_FAIL(init_col_aggregator<ObColMinAggregator>(
            full_col_descs.at(col_idx), agg_result_->storage_datums_[i], allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_MAX: {
        if (OB_FAIL(init_col_aggregator<ObColMaxAggregator>(
            full_col_descs.at(col_idx), agg_result_->storage_datums_[i], allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_NULL_COUNT: {
        if (OB_FAIL(init_col_aggregator<ObColNullCountAggregator>(
            full_col_descs.at(col_idx), agg_result_->storage_datums_[i], allocator))) {
          LOG_WARN("Fail to allocate column aggregator", K(ret));
        }
        break;
      }
      case ObSkipIndexColType::SK_IDX_SUM: {
        if (OB_FAIL(init_col_aggregator<ObColSumAggregator>(
            full_col_descs.at(col_idx), agg_result_->storage_datums_[i], allocator))) {
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
    }
  }
  return ret;
}

template <typename T>
int ObSkipIndexAggregator::init_col_aggregator(
    const ObColDesc &col_desc,
    ObStorageDatum &result_datum,
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
  } else if (OB_FAIL(col_aggregator->init(col_desc, result_datum))) {
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


/* ------------------------------------ObIndexBlockAggregator-------------------------------------*/

ObIndexBlockAggregator::ObIndexBlockAggregator()
  : skip_index_aggregator_(), aggregated_row_(), row_count_(0), row_count_delta_(0),
    max_merged_trans_version_(0), macro_block_count_(0), micro_block_count_(0),
    can_mark_deletion_(true), contain_uncommitted_row_(false), has_string_out_row_(false),
    has_lob_out_row_(false), is_last_row_last_flag_(false), is_inited_(false) {}

void ObIndexBlockAggregator::reset()
{
  skip_index_aggregator_.reset();
  aggregated_row_.reset();
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
  is_inited_ = false;
}

void ObIndexBlockAggregator::reuse()
{
  skip_index_aggregator_.reuse();
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
}

int ObIndexBlockAggregator::init(const ObDataStoreDesc &store_desc, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Already inited", K(ret));
  } else {
    need_data_aggregate_ = store_desc.get_agg_meta_array().count() != 0 && store_desc.is_major_or_meta_merge_type();
    if (!need_data_aggregate_) {
    } else if (OB_FAIL(aggregated_row_.init(allocator, store_desc.get_agg_meta_array().count()))) {
      LOG_WARN("Fail to init aggregated row", K(ret));
    } else if (OB_FAIL(skip_index_aggregator_.init(
        store_desc.get_agg_meta_array(),
        store_desc.get_full_stored_col_descs(),
        false, /* is data row */
        aggregated_row_,
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
  } else if (need_data_aggregate_) {
    if (OB_ISNULL(row_desc.aggregated_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null pointer for aggregated row", K(ret), K(row_desc));
    } else if (row_desc.is_serialized_agg_row_) {
      const ObAggRowHeader *agg_row_header = reinterpret_cast<const ObAggRowHeader *>(
          row_desc.serialized_agg_row_buf_);
      if (OB_UNLIKELY(!agg_row_header->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid aggregated row header", K(ret), KPC(agg_row_header));
      } else if (OB_FAIL(skip_index_aggregator_.eval(
          row_desc.serialized_agg_row_buf_, agg_row_header->length_, row_desc.row_count_))) {
        LOG_WARN("Fail to aggregate serialized index row", K(ret), K(row_desc), KPC(agg_row_header));
      }
    } else if (OB_FAIL(skip_index_aggregator_.eval(*row_desc.aggregated_row_))) {
      LOG_WARN("Fail to aggregate on index row", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    row_count_ += row_desc.row_count_;
    row_count_delta_ += row_desc.row_count_delta_;
    can_mark_deletion_ = can_mark_deletion_ && row_desc.is_deleted_;
    max_merged_trans_version_ = max_merged_trans_version_ > row_desc.max_merged_trans_version_
                              ? max_merged_trans_version_
                              : row_desc.max_merged_trans_version_;
    contain_uncommitted_row_ = contain_uncommitted_row_
                            || row_desc.contain_uncommitted_row_;
    has_string_out_row_ = has_string_out_row_ || row_desc.has_string_out_row_;
    has_lob_out_row_ = has_lob_out_row_ || row_desc.has_lob_out_row_;
    is_last_row_last_flag_ = row_desc.is_last_row_last_flag_;
    micro_block_count_ += row_desc.micro_block_count_;
    macro_block_count_ += row_desc.macro_block_count_;
  }
  return ret;
}

int ObIndexBlockAggregator::get_index_agg_result(ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (need_data_aggregate_
      && OB_FAIL(skip_index_aggregator_.get_aggregated_row(row_desc.aggregated_row_))) {
    LOG_WARN("Fail to get aggregated row", K(ret));
  } else {
    row_desc.row_count_ = row_count_;
    row_desc.row_count_delta_ = row_count_delta_;
    row_desc.is_deleted_ = can_mark_deletion_;
    row_desc.max_merged_trans_version_ = max_merged_trans_version_;
    row_desc.contain_uncommitted_row_ = contain_uncommitted_row_;
    row_desc.has_string_out_row_ = has_string_out_row_;
    row_desc.has_lob_out_row_ = has_lob_out_row_;
    row_desc.is_last_row_last_flag_ = is_last_row_last_flag_;
    row_desc.macro_block_count_ = macro_block_count_;
    row_desc.micro_block_count_ = micro_block_count_;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
