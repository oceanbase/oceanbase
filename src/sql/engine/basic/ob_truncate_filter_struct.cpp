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
#include "ob_truncate_filter_struct.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/schema/ob_list_row_values.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "lib/container/ob_fixed_array_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;

namespace sql
{

// ------------------------------------------------- ObITruncateFilterExecutor -------------------------------------------------
ObITruncateFilterExecutor::ObITruncateFilterExecutor(common::ObIAllocator &alloc)
  : truncate_item_type_(MAX_TRUNCATE_PART_EXPR_TYPE),
    need_flip_(false),
    col_idxs_(alloc),
    tmp_datum_buf_(nullptr)

{
}

ObITruncateFilterExecutor::~ObITruncateFilterExecutor()
{
  if (tmp_datum_buf_ != nullptr && datum_allocator_ != nullptr) {
    datum_allocator_->free(tmp_datum_buf_);
  }
}

int ObITruncateFilterExecutor::filter(const ObDatumRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  const ObIArray<int32_t> &col_idxs = get_col_idxs();
  const int64_t col_count = col_idxs.count();
  if (OB_UNLIKELY(col_count <= 0 || col_count >= row.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected col count or datum buf", K(ret), K(col_count), K(row));
  } else if (is_truncate_scn_item()) {
    if (OB_UNLIKELY(1 != col_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid col count for scn filter", K(ret), K(col_count));
    } else {
      int64_t origin_version = 0;
      const int64_t col_idx = col_idxs.at(0);
      if (OB_UNLIKELY(col_idx >= row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part col idx", K(ret), K(col_idxs), K(row));
      } else if (OB_UNLIKELY(row.storage_datums_[col_idx].is_nop_value() || row.storage_datums_[col_idx].is_null())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("filtered col is nop or null", KR(ret), K(col_idx), K(row.storage_datums_[col_idx]));
      } else {
        origin_version = row.storage_datums_[col_idx].get_int();
        // convert trans version to positive value temporary as the truncate version is positive
        if (origin_version < 0) {
          row.storage_datums_[col_idx].reuse();
          row.storage_datums_[col_idx].set_int(-origin_version);
        }
        if (OB_FAIL(inner_filter(row.storage_datums_, col_count, filtered, &col_idxs))) {
          LOG_WARN("failed to filter row", K(ret), K(col_count), K(row));
        }
        if (origin_version < 0) {
          row.storage_datums_[col_idx].set_int(origin_version);
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_idxs.count(); ++i) {
      const int64_t col_idx = col_idxs.at(i);
      if (OB_UNLIKELY(col_idx >= row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part col idx", K(ret), K(i), K(col_idxs), K(row));
      } else if (OB_UNLIKELY(row.storage_datums_[col_idx].is_nop_value())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("filtered col is nop or null", KR(ret), K(col_idx), K(row.storage_datums_[col_idx]));
      }
    }
    if (FAILEDx(inner_filter(row.storage_datums_, col_count, filtered, &col_idxs))) {
      LOG_WARN("failed to filter row", K(ret), K(col_count), K(row));
    }
  }
  return ret;
}

int ObITruncateFilterExecutor::prepare_datum_buf(ObIAllocator &alloc, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid count", K(ret), K(count));
  } else if (nullptr == tmp_datum_buf_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObStorageDatum) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      tmp_datum_buf_ = new (buf) ObStorageDatum[count];
      datum_allocator_ = &alloc;
    }
  }
  return ret;
}

// ------------------------------------------------- ObTruncateWhiteFilterNode -------------------------------------------------

int ObTruncateWhiteFilterNode::set_op_type(const ObRawExpr &raw_expr)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected state, truncate white filter should not reach here", K(ret), K(lbt()));
  return ret;
}

int ObTruncateWhiteFilterNode::get_filter_val_meta(ObObjMeta &obj_meta) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!obj_meta_.is_valid() || obj_meta_.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj meta", K(ret), K_(obj_meta));
  } else {
    obj_meta = obj_meta_;
  }
  return ret;
}

ObObjType ObTruncateWhiteFilterNode::get_filter_arg_obj_type(int64_t arg_idx) const
{
  return obj_meta_.get_type();
}

// ------------------------------------------------- ObTruncateWhiteFilterExecutor -------------------------------------------------

const ObIArray<ObExpr *> *ObTruncateWhiteFilterExecutor::get_cg_col_exprs() const
{
  OB_ASSERT_MSG(false, "ObTruncateWhiteFilterExecutor dose not promise cg col exprs");
  return &filter_.column_exprs_;
}

int ObTruncateWhiteFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  LOG_DEBUG("truncate filter do not need do this now", K(ret), K(lbt()));
  return ret;
}

int ObTruncateWhiteFilterExecutor::filter(ObEvalCtx &eval_ctx, const sql::ObBitVector &skip_bit, bool &filtered)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected state, truncate white filter should not reach here", K(ret), K(lbt()));
  return ret;
}

int ObTruncateWhiteFilterExecutor::prepare_truncate_param(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_idxs_.empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!is_truncate_scn_item() && 1 != truncate_partition.part_key_idxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part key count for whiter filter", K(ret), K(truncate_partition.part_key_idxs_));
  } else if (OB_FAIL(init_array_param(col_idxs_, 1))) {
    LOG_WARN("failed init col idxs array", K(ret));
  } else {
    int64_t col_idx = -1;
    const bool is_range_part = ObTruncatePartition::is_range_part(truncate_partition.part_type_);
    ObTruncateWhiteFilterNode &node = get_filter_node();
    switch (truncate_item_type_) {
      case TRUNCATE_SCN:
      case TRUNCATE_RANGE_LEFT:
      case TRUNCATE_RANGE_RIGHT: {
        if (TRUNCATE_SCN == truncate_item_type_) {
          col_idx = schema_rowkey_cnt;
        } else if (OB_UNLIKELY(!is_range_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
              K_(truncate_item_type), K(truncate_partition));
        } else {
          col_idx = truncate_partition.part_key_idxs_.at(0);
        }
        if (FAILEDx(init_array_param(datum_params_, 1))) {
          LOG_WARN("failed init datum params", K(ret));
        }
        break;
      }
      case TRUNCATE_LIST: {
        if (OB_UNLIKELY(is_range_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
              K_(truncate_item_type), K(truncate_partition));
        } else {
          col_idx = truncate_partition.part_key_idxs_.at(0);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected truncate item type", K(ret), K_(truncate_item_type), KPC(this), K(truncate_partition));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(col_idx < 0 || col_idx >= cols_desc.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part col idx", K(col_idx), K(cols_desc));
    } else if (OB_FAIL(col_idxs_.push_back(col_idx))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      node.obj_meta_.set_meta(cols_desc.at(col_idx).col_type_);
      cmp_func_ = get_datum_cmp_func(node.obj_meta_, node.obj_meta_);
    }
    if (FAILEDx(prepare_datum_buf(allocator_, 1))) {
      LOG_WARN("failed to prepare datum buf", K(ret));
    }
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(schema_rowkey_cnt), K(cols_desc), K(truncate_partition), KPC(this));
  return ret;
}

int ObTruncateWhiteFilterExecutor::prepare_truncate_value(
    const int64_t truncate_commit_viersion,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_SUCCESS;
  storage_datum_param_.reuse();
  datum_params_.clear();
  set_filter_bool_mask(ObBoolMaskType::PROBABILISTIC);
  null_param_contained_ = false;
  const bool is_range_part = ObTruncatePartition::is_range_part(truncate_partition.part_type_);
  switch (truncate_item_type_) {
    case TRUNCATE_SCN: {
      truncate_version_ = truncate_commit_viersion;
      storage_datum_param_.set_int(truncate_version_);
      if (OB_FAIL(datum_params_.push_back(storage_datum_param_))) {
        LOG_WARN("failed to push back datum", K(ret));
      }
      break;
    }
    case TRUNCATE_RANGE_LEFT:
    case TRUNCATE_RANGE_RIGHT: {
      set_need_flip(ObTruncatePartition::EXCEPT == truncate_partition.part_op_);
      const ObRowkey &rowkey = truncate_item_type_ == TRUNCATE_RANGE_LEFT ?
                               truncate_partition.low_bound_val_ :
                               truncate_partition.high_bound_val_;
      if (OB_UNLIKELY(!is_range_part ||
                      (!rowkey.is_min_row() && 1 != rowkey.get_obj_cnt()) ||
                      (!rowkey.is_max_row() && 1 != rowkey.get_obj_cnt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
            K_(truncate_item_type), K(rowkey), K(truncate_partition));
      } else if (rowkey.is_min_row() || rowkey.is_max_row()) {
        set_filter_bool_mask(need_flip() ? ObBoolMaskType::ALWAYS_TRUE : ObBoolMaskType::ALWAYS_FALSE);
      } else if (OB_FAIL(storage_datum_param_.from_obj(rowkey.get_obj_ptr()[0]))) {
        LOG_WARN("failed to from obj", K(ret), K(rowkey));
      } else if (OB_UNLIKELY(is_truncate_value_invalid(storage_datum_param_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid truncate param value", K(ret), K(rowkey));
      } else if (OB_FAIL(datum_params_.push_back(storage_datum_param_))) {
        LOG_WARN("failed to push back datum", K(ret));
      }
      break;
    }
    case TRUNCATE_LIST: {
      set_need_flip(ObTruncatePartition::EXCEPT == truncate_partition.part_op_);
      if (OB_UNLIKELY(is_range_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
            K_(truncate_item_type), K(truncate_partition));
      } else if (ObTruncatePartition::ALL == truncate_partition.part_op_) {
        set_filter_bool_mask(ObBoolMaskType::ALWAYS_FALSE);
      } else if (OB_FAIL(prepare_truncate_list_value(truncate_partition.list_row_values_))) {
        LOG_WARN("failed to prepare truncate list param", K(ret), K(truncate_partition));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected truncate item type", K(ret), K_(truncate_item_type), KPC(this), K(truncate_partition));
    }
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(truncate_commit_viersion), K(truncate_partition), KPC(this));
  return ret;
}

// do not check validness of col_idxs as the caller already do this
int ObTruncateWhiteFilterExecutor::inner_filter(
    const ObStorageDatum *datums,
    int64_t count,
    bool &filtered,
    const ObIArray<int32_t> *col_idxs) const
{
  int ret = OB_SUCCESS;
  const int64_t col_idx = nullptr == col_idxs ? 0 : col_idxs->at(0);
  if (OB_UNLIKELY(1 != count || nullptr == datums)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state", K(ret), K(count), KP(datums));
  } else if (is_filter_always_true()) {
    filtered = false;
  } else if (is_filter_always_false()) {
    filtered = true;
  } else if (OB_FAIL(ObIMicroBlockReader::filter_white_filter(*this, datums[col_idx], filtered))) {
    LOG_WARN("failed to filter white filter", K(ret));
  } else if (OB_UNLIKELY(need_flip())) {
    filtered = !filtered;
  }
  LOG_DEBUG("[TRUNCATE INFO]", K(ret), KPC(this), K(count), K(filtered));
  return ret;
}

void ObTruncateWhiteFilterExecutor::pre_filter_batch()
{
  if (TRUNCATE_SCN == truncate_item_type_) {
    get_filter_node().set_truncate_white_op_type(WHITE_OP_LE);
    datum_params_.at(0).set_int(-truncate_version_);
  }
}

void ObTruncateWhiteFilterExecutor::post_filter_batch()
{
  if (TRUNCATE_SCN == truncate_item_type_) {
    get_filter_node().set_truncate_white_op_type(WHITE_OP_GT);
    datum_params_.at(0).set_int(truncate_version_);
  }
}

int ObTruncateWhiteFilterExecutor::prepare_truncate_list_value(const ObStorageListRowValues &list_row_values)
{
  int ret = OB_SUCCESS;
  const int truncate_row_cnt = list_row_values.count();
  const bool use_white_eq = 1 == truncate_row_cnt;
  ObTruncateWhiteFilterNode &node = get_filter_node();
  const ObNewRow *values = list_row_values.get_values();
  if (OB_UNLIKELY(truncate_row_cnt <= 0 || nullptr == values)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list row values", K(ret), K(truncate_row_cnt), K(list_row_values));
  } else if (OB_FAIL(reserve_storage_datum_space(truncate_row_cnt))) {
    LOG_WARN("failed to reserve storage datum params space", K(ret), K(truncate_row_cnt));
  } else if (OB_FAIL(init_array_param(datum_params_, truncate_row_cnt))) {
    LOG_WARN("failed to init in array datums", K(ret), K(truncate_row_cnt));
  } else if (FALSE_IT(param_set_.destroy())) {
  } else if (use_white_eq) {
    node.set_truncate_white_op_type(WHITE_OP_EQ);
    if (OB_UNLIKELY(1 != values[0].get_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected list row values for white filter", K(ret), K(list_row_values));
    } else if (OB_FAIL(storage_datum_param_.from_obj(values[0].get_cell(0)))) {
      LOG_WARN("failed to from obj", K(ret));
    } else if (OB_UNLIKELY(is_truncate_value_invalid(storage_datum_param_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid truncate param value", K(ret), K(values[0]));
    } else if (OB_FAIL(datum_params_.push_back(storage_datum_param_))) {
      LOG_WARN("failed to push back datum", K(ret));
    }
  } else {
    if (OB_FAIL(param_set_.create(truncate_row_cnt * 2))) {
      LOG_WARN("failed to create in hash set", K(ret), K(truncate_row_cnt));
    } else {
      ObPrecision precision = PRECISION_UNKNOWN_YET;
      if (node.obj_meta_.is_decimal_int()) {
        precision = node.obj_meta_.get_stored_precision();
        OB_ASSERT(precision != PRECISION_UNKNOWN_YET);
      }
      sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(node.obj_meta_.get_type(),
                                                                        node.obj_meta_.get_collation_type(),
                                                                        node.obj_meta_.get_scale(),
                                                                        lib::is_oracle_mode(),
                                                                        false,
                                                                        precision);
      param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, basic_funcs->null_first_cmp_);
    }
    if (OB_SUCC(ret)) {
      node.set_truncate_white_op_type(WHITE_OP_IN);
      for (int64_t i = 0; OB_SUCC(ret) && i < truncate_row_cnt; ++i) {
        if (OB_UNLIKELY(1 != values[i].get_count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid list row obj cnt for white fiter", K(ret), K(i), K(list_row_values));
        } else if (FALSE_IT(in_storage_datum_params_[i].reuse())) {
        } else if (OB_FAIL(in_storage_datum_params_[i].from_obj(values[i].get_cell(0)))) {
          LOG_WARN("failed to from obj", K(ret), K(i), K(list_row_values));
        } else if (OB_UNLIKELY(is_truncate_value_invalid(in_storage_datum_params_[i]))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid truncate param value", K(ret), K(values[i].get_cell(0)));
                } else if (OB_FAIL(add_to_param_set_and_array(in_storage_datum_params_[i], nullptr))) {
          LOG_WARN("failed to add to param set and array", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        bool mock_equal = false;
        ObDatumComparator cmp(cmp_func_, ret, mock_equal);
        lib::ob_sort(datum_params_.begin(), datum_params_.end(), cmp);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to sort in datums", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // use WHITE_OP_EQ and WHITE_OP_IN, not WHITE_OP_NE and NOT IN(not white filter)
    // so need flip the filter result
    set_need_flip(!need_flip());
  }
  return ret;
}

int ObTruncateWhiteFilterExecutor::reserve_storage_datum_space(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(1 == count)) {
    // use storage_datum_param_
  } else if (count <= in_param_cnt_) {
    if (OB_ISNULL(in_storage_datum_params_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpectd null in param", K(ret), K(count), KP_(in_param_cnt), KP_(in_storage_datum_params));
    }
  } else {
    if (nullptr != in_storage_datum_params_) {
      allocator_.free(in_storage_datum_params_);
      in_storage_datum_params_ = nullptr;
    }
    in_param_cnt_ = 0;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageDatum) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(count));
    } else {
      in_param_cnt_ = count;
      in_storage_datum_params_ = new (buf) ObStorageDatum[count];
    }
  }
  return ret;
}

void ObTruncateWhiteFilterExecutor::release_storage_datum_space()
{
  if (nullptr != in_storage_datum_params_) {
    allocator_.free(in_storage_datum_params_);
    in_storage_datum_params_ = nullptr;
  }
  in_param_cnt_ = 0;
}

// ------------------------------------------------- ObTruncateBlackFilterExecutor -------------------------------------------------

int ObTruncateBlackFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  LOG_DEBUG("truncate filter do not need do this now", K(ret));
  return ret;
}

int ObTruncateBlackFilterExecutor::prepare_truncate_param(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!col_idxs_.empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const bool is_range_part = ObTruncatePartition::is_range_part(truncate_partition.part_type_);
    switch (truncate_item_type_) {
      case TRUNCATE_SCN: {
        ObObjMeta meta_type;
        meta_type.set_int();
        truncate_row_cnt_ = row_obj_cnt_ = 1;
        if (OB_FAIL(init_array_param(col_idxs_, 1))) {
          LOG_WARN("failed init col idxs array", K(ret));
        } else if (OB_FAIL(col_idxs_.push_back(schema_rowkey_cnt))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(obj_metas_.push_back(meta_type))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(cmp_funcs_.push_back(get_datum_cmp_func(meta_type, meta_type)))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(reserve_storage_datum_space(1))) {
          LOG_WARN("failed to reserve storage datum param space", K(ret));
        }
        break;
      }
      case TRUNCATE_RANGE_LEFT:
      case TRUNCATE_RANGE_RIGHT: {
        if (OB_UNLIKELY(!is_range_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
              K_(truncate_item_type), K(truncate_partition));
        } else {
          truncate_row_cnt_ = 1;
          row_obj_cnt_ = truncate_partition.part_key_idxs_.count();
          if (OB_FAIL(prepare_metas_and_cmp_funcs(cols_desc, truncate_partition))) {
            LOG_WARN("failed to prepare metas and cmp funcs", K(ret));
          } else if (OB_FAIL(reserve_storage_datum_space(row_obj_cnt_))) {
            LOG_WARN("failed to reserve storage datum param space", K(ret), K_(row_obj_cnt));
          }
        }
        break;
      }
      case TRUNCATE_LIST: {
        row_obj_cnt_ = truncate_partition.part_key_idxs_.count();
        if (OB_UNLIKELY(is_range_part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
              K_(truncate_item_type), K(truncate_partition));
        } else if (OB_FAIL(prepare_metas_and_cmp_funcs(cols_desc, truncate_partition))) {
          LOG_WARN("failed to prepare metas and cmp funcs", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected truncate item type", K(ret), KPC(this));
      }
    }
    if (FAILEDx(prepare_datum_buf(allocator_, row_obj_cnt_))) {
      LOG_WARN("failed to prepare datum buf", K(ret));
    }
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), K(schema_rowkey_cnt), K(cols_desc), K(truncate_partition), KPC(this));
  return ret;
}

int ObTruncateBlackFilterExecutor::prepare_truncate_value(
    const int64_t truncate_commit_viersion,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_SUCCESS;
  set_filter_bool_mask(ObBoolMaskType::PROBABILISTIC);
  const bool is_range_part = ObTruncatePartition::is_range_part(truncate_partition.part_type_);
  switch (truncate_item_type_) {
    case TRUNCATE_SCN: {
      storage_datum_params_[0].set_int(truncate_commit_viersion);
      break;
    }
    case TRUNCATE_RANGE_LEFT:
    case TRUNCATE_RANGE_RIGHT: {
      set_need_flip(ObTruncatePartition::EXCEPT == truncate_partition.part_op_);
      const ObRowkey &rowkey = truncate_item_type_ == TRUNCATE_RANGE_LEFT ?
                               truncate_partition.low_bound_val_ :
                               truncate_partition.high_bound_val_;
      if (OB_UNLIKELY(!is_range_part || row_obj_cnt_ != truncate_partition.part_key_idxs_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
            K_(truncate_item_type), K(rowkey), K(truncate_partition));
      } else if ((truncate_item_type_ == TRUNCATE_RANGE_LEFT && rowkey.is_min_row()) ||
                 (truncate_item_type_ == TRUNCATE_RANGE_RIGHT && rowkey.is_max_row())) {
        set_filter_bool_mask(ObBoolMaskType::ALWAYS_FALSE);
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_obj_cnt_; ++i) {
          storage_datum_params_[i].reuse();
          if (OB_FAIL(storage_datum_params_[i].from_obj(rowkey.get_obj_ptr()[i]))) {
            LOG_WARN("failed to from obj", K(ret), K(i), K(rowkey));
          } else if (OB_UNLIKELY(is_truncate_value_invalid(storage_datum_params_[i]))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected invalid truncate param value", K(ret), K(i), K(rowkey.get_obj_ptr()[i]), K(rowkey));
          }
        }
      }
      break;
    }
    case TRUNCATE_LIST: {
      set_need_flip(ObTruncatePartition::EXCEPT == truncate_partition.part_op_);
      if (OB_UNLIKELY(is_range_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state, filter and truncate info are not matched", K(ret), K(is_range_part),
            K_(truncate_item_type), K(truncate_partition));
      } else if (ObTruncatePartition::ALL == truncate_partition.part_op_) {
        set_filter_bool_mask(ObBoolMaskType::ALWAYS_FALSE);
      } else if (OB_FAIL(prepare_truncate_list_value(truncate_partition.list_row_values_))) {
        LOG_WARN("failed to prepare truncate list param", K(ret), K(truncate_partition));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected truncate item type", K(ret), KPC(this));
    }
  }
  LOG_INFO("[TRUNCATE INFO]", K(ret), KPC(this), K(truncate_partition));
  return ret;
}

// do not check validness of col_idxs as the caller already do this
int ObTruncateBlackFilterExecutor::inner_filter(
    const ObStorageDatum *datums,
    int64_t count,
    bool &filtered,
    const ObIArray<int32_t> *col_idxs) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(datums), K(count));
  } else if (is_filter_always_true()) {
    filtered = false;
  } else if (is_filter_always_false()) {
    filtered = true;
  } else {
    switch (truncate_item_type_) {
      case TRUNCATE_SCN:
      case TRUNCATE_RANGE_LEFT:
      case TRUNCATE_RANGE_RIGHT: {
        int cmp_ret = 0;
        if (OB_UNLIKELY(1 != truncate_row_cnt_ || count != cmp_funcs_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state, should only one truncate row", K(ret), K_(truncate_row_cnt), K(count), KPC(this));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < count && 0 == cmp_ret; ++i) {
          const int64_t col_idx = nullptr == col_idxs ? i : col_idxs->at(i);
          if (datums[col_idx].is_null()) {
            cmp_ret = lib::is_oracle_mode() ? 1 : -1;
          } else if (OB_FAIL(cmp_funcs_.at(i)(datums[col_idx], storage_datum_params_[i], cmp_ret))) {
            LOG_WARN("failed to compare", K(ret), K(i), K(col_idx), K(datums[col_idx]), K(storage_datum_params_[i]));
          }
          LOG_DEBUG("[TRUNCATE INFO]", K(ret), K(i), K(cmp_ret), K(col_idx), K(datums[col_idx]), K(storage_datum_params_[i]),
                    K(cmp_ret), KPC(this));
        }
        if (OB_FAIL(ret)) {
        } else if (truncate_item_type_ == TRUNCATE_RANGE_LEFT) {
          filtered = cmp_ret >= 0;
        } else if (truncate_item_type_ == TRUNCATE_SCN) {
          filtered = cmp_ret <= 0;
        } else {
          filtered = cmp_ret < 0;
        }
        break;
      }
      case TRUNCATE_LIST: {
        int cmp_ret = -1;
        if (OB_UNLIKELY(1 > truncate_row_cnt_ || count != cmp_funcs_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state", K(ret), K_(truncate_row_cnt), K(count), KPC(this));
        }
        int64_t datum_offset = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < truncate_row_cnt_ && 0 != cmp_ret; ++i) {
          cmp_ret = 0;
          for (int64_t j = 0; OB_SUCC(ret) && j < count && 0 == cmp_ret; ++j) {
            const int64_t col_idx = nullptr == col_idxs ? j : col_idxs->at(j);
            const int64_t datum_idx = datum_offset + j;
            if (datums[col_idx].is_null() || storage_datum_params_[datum_idx].is_null()) {
              cmp_ret = datums[col_idx].is_null() && storage_datum_params_[datum_idx].is_null() ? 0 : 1;
            } else if (OB_FAIL(cmp_funcs_.at(j)(datums[col_idx], storage_datum_params_[datum_idx], cmp_ret))) {
              LOG_WARN("failed to compare", K(ret), K(count), K(i), K(j), K(datum_idx), K(datums[col_idx]),
              K(storage_datum_params_[datum_idx]), KPC(this));
            }
            LOG_DEBUG("[TRUNCATE INFO]", K(ret), K(cmp_ret), K(count), K(i), K(j), K(datum_idx), K(col_idx), K(datums[col_idx]),
                     K(storage_datum_params_[datum_idx]), KPC(this));
          }
          datum_offset += count;
        }
        if (OB_SUCC(ret)) {
          filtered = 0 == cmp_ret;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected truncate item type", K(ret), K_(truncate_item_type));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(need_flip())) {
      filtered = !filtered;
    }
  }
  LOG_DEBUG("[TRUNCATE INFO]", K(ret), KPC(this), K(count), K(filtered));
  return ret;
}

int ObTruncateBlackFilterExecutor::prepare_truncate_list_value(const ObStorageListRowValues &list_row_values)
{
  int ret = OB_SUCCESS;
  truncate_row_cnt_ = list_row_values.count();
  const ObNewRow *values = list_row_values.get_values();
  if (OB_UNLIKELY(truncate_row_cnt_ <= 0 || nullptr == values)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid list row values", K(ret), K(list_row_values), KP(values));
  } else if (OB_FAIL(reserve_storage_datum_space(truncate_row_cnt_ * row_obj_cnt_))) {
    LOG_WARN("failed to reserve storage datum params space", K(ret), K_(truncate_row_cnt), K_(row_obj_cnt));
  }
  int64_t datum_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < truncate_row_cnt_; ++i) {
    if (OB_UNLIKELY(values[i].get_count() != row_obj_cnt_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid list row obj cnt for black fiter", K(ret), K(i), K(list_row_values), K_(row_obj_cnt));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < row_obj_cnt_; ++j) {
      storage_datum_params_[datum_idx].reuse();
      if (OB_FAIL(storage_datum_params_[datum_idx++].from_obj(values[i].get_cell(j)))) {
        LOG_WARN("failed to from obj", K(ret), K(i), K(list_row_values));
      } else if (OB_UNLIKELY(storage_datum_params_[datum_idx - 1].is_nop_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid truncate param value", K(ret), K(i), K(j), K(values[i]));
      }
    }
  }
  return ret;
}

int ObTruncateBlackFilterExecutor::prepare_metas_and_cmp_funcs(
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_param(col_idxs_, row_obj_cnt_))) {
    LOG_WARN("failed init col idxs array", K(ret));
  } else if (OB_FAIL(init_array_param(obj_metas_, row_obj_cnt_))) {
    LOG_WARN("failed init obj metas array", K(ret));
  } else if (OB_FAIL(init_array_param(cmp_funcs_, row_obj_cnt_))) {
    LOG_WARN("failed init cmp funcs array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_obj_cnt_; ++i) {
    const int64_t col_idx = truncate_partition.part_key_idxs_.at(i);
    if (OB_UNLIKELY(col_idx < 0 || col_idx >= cols_desc.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part col idx", K(ret), K(col_idx), K(cols_desc));
    } else if (OB_FAIL(col_idxs_.push_back(col_idx))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(obj_metas_.push_back(cols_desc.at(col_idx).col_type_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(cmp_funcs_.push_back(get_datum_cmp_func(cols_desc.at(col_idx).col_type_, cols_desc.at(col_idx).col_type_)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTruncateBlackFilterExecutor::reserve_storage_datum_space(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (count <= storage_datum_cnt_) {
  } else if (nullptr != storage_datum_params_) {
    allocator_.free(storage_datum_params_);
    storage_datum_params_ = nullptr;
    storage_datum_cnt_ = 0;
  }
  if (nullptr == storage_datum_params_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageDatum) * count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(count));
    } else {
      storage_datum_params_ = new (buf) ObStorageDatum[count];
      storage_datum_cnt_ = count;
    }
  }
  return ret;
}

void ObTruncateBlackFilterExecutor::release_storage_datum_space()
{
  if (nullptr != storage_datum_params_) {
    allocator_.free(storage_datum_params_);
    storage_datum_params_ = nullptr;
  }
  storage_datum_cnt_ = 0;
}

// ------------------------------------------------- ObTruncateOrFilterExecutor -------------------------------------------------

int ObTruncateOrFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  LOG_DEBUG("truncate filter do not need do this now", K(ret), K(lbt()));
  return ret;
}

int ObTruncateOrFilterExecutor::prepare_truncate_param(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected state, truncate or filter should not reach here", K(ret), K(lbt()));
  return ret;
}

int ObTruncateOrFilterExecutor::prepare_truncate_value(
    const int64_t truncate_commit_viersion,
    const ObTruncatePartition &truncate_partition)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected state, truncate or filter should not reach here", K(ret), K(lbt()));
  return ret;
}

int ObTruncateOrFilterExecutor::filter(const ObDatumRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = true;
  ObITruncateFilterExecutor *truncate_executor = nullptr;
  for (uint32_t i = 0; OB_SUCC(ret) && i < n_child_ && filtered; ++i) {
    ObPushdownFilterExecutor *child = childs_[i];
    if (OB_UNLIKELY(nullptr == child || !child->is_truncate_node())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected truncate executor type", K(ret), K(i), KPC(child));
    } else if (child->is_filter_white_node()) {
      truncate_executor = static_cast<ObTruncateWhiteFilterExecutor*>(child);
    } else {
      truncate_executor = static_cast<ObTruncateBlackFilterExecutor*>(child);
    }
    if (OB_FAIL(ret)) {
    } else if (child->is_filter_always_true()) {
      filtered = false;
    } else if (child->is_filter_always_false()) {
      filtered = true;
    } else if (OB_FAIL(truncate_executor->filter(row, filtered))) {
      LOG_WARN("failed to filter", K(ret), K(i), KPC(truncate_executor));
    }
  }
  return ret;
}

int ObTruncateOrFilterExecutor::inner_filter(
    const blocksstable::ObStorageDatum *datums,
    int64_t count,
    bool &filtered,
    const ObIArray<int32_t> *col_idxs) const
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected state, truncate or filter should not reach here", K(ret), K(lbt()));
  return ret;
}

// ------------------------------------------------- ObTruncateAndFilterExecutor -------------------------------------------------

ObTruncateAndFilterExecutor::ObTruncateAndFilterExecutor(
    ObIAllocator &alloc,
    ObTruncateAndFilterNode &filter,
    ObPushdownOperator &op)
  : ObPushdownFilterExecutor(alloc, op, PushdownExecutorType::TRUNCATE_AND_FILTER_EXECUTOR),
    is_inited_(false),
    has_nullable_col_(false),
    valid_truncate_filter_cnt_(0),
    truncate_filters_(),
    part_filter_buffer_(),
    subpart_filter_buffer_(),
    filter_(filter)
{
  MEMSET(item_buffer_, 0, sizeof(item_buffer_));
  ObMemAttr mem_attr(MTL_ID(), "TruncateExe");
  truncate_filters_.set_attr(mem_attr);
  part_filter_buffer_.set_attr(mem_attr);
  subpart_filter_buffer_.set_attr(mem_attr);
}

ObTruncateAndFilterExecutor::~ObTruncateAndFilterExecutor()
{
#define RELEASE_TRUNCATE_FILTERS(filters)           \
  for (int64_t i = 0; i < filters.count(); ++i) {   \
    if (OB_NOT_NULL(filters.at(i))) {               \
      filters.at(i)->~ObTruncateOrFilterExecutor(); \
    }                                               \
  }

  RELEASE_TRUNCATE_FILTERS(truncate_filters_);
  RELEASE_TRUNCATE_FILTERS(part_filter_buffer_);
  RELEASE_TRUNCATE_FILTERS(subpart_filter_buffer_);
#undef RELEASE_TRUNCATE_FILTERS
}

int ObTruncateAndFilterExecutor::init_evaluated_datums(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  LOG_DEBUG("truncate filter do not need do this now", K(ret));
  return ret;
}

int ObTruncateAndFilterExecutor::init(
    ObPushdownFilterFactory &filter_factory,
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObIArray<ObColumnParam *> *cols_param,
    const ObTruncateInfoArray &truncate_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(schema_rowkey_cnt <= 0 ||
                         schema_rowkey_cnt >= cols_desc.count() ||
                         truncate_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_rowkey_cnt), K(cols_desc), K(truncate_info_array));
    } else {
    ObTruncateOrFilterExecutor *single_truncate_filter = nullptr;
    has_nullable_col_ = nullptr == cols_param;
    if (!has_nullable_col_) {
      for (int64_t i = 0; !has_nullable_col_ && i < cols_param->count(); ++i) {
        if (nullptr == cols_param->at(i)) {
          has_nullable_col_ = true;
        } else if (IS_SHADOW_COLUMN(cols_param->at(i)->get_column_id())) {
        } else {
          has_nullable_col_ = cols_param->at(i)->is_nullable_for_write();
        }
      }
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < truncate_info_array.count(); ++idx) {
      single_truncate_filter = nullptr;
      const ObTruncateInfo *truncate_info = truncate_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == truncate_info || !truncate_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid truncate info", KR(ret), K(idx), KPC(truncate_info));
      } else if (OB_FAIL(build_single_truncate_filter(schema_rowkey_cnt, cols_desc, *truncate_info,
                                                      filter_factory, single_truncate_filter))) {
        LOG_WARN("failed to build single trunate filter", K(ret));
      } else if (OB_ISNULL(single_truncate_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null single truncate filter", K(ret), K(idx), K(truncate_info));
      } else if (OB_FAIL(truncate_filters_.push_back(single_truncate_filter))) {
        LOG_WARN("failed to push back", K(ret));
        RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObTruncateOrFilterExecutor, single_truncate_filter);
      }
    }
  }
  if (OB_SUCC(ret)) {
    valid_truncate_filter_cnt_ =  truncate_info_array.count();
    is_inited_ = true;
  }
  return ret;
}

int ObTruncateAndFilterExecutor::switch_info(
    ObPushdownFilterFactory &filter_factory,
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncateInfoArray &truncate_info_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(schema_rowkey_cnt <= 0 ||
                         schema_rowkey_cnt >= cols_desc.count() ||
                         truncate_info_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_rowkey_cnt), K(cols_desc), K(truncate_info_array));
  } else if (OB_FAIL(inner_reuse())) {
    LOG_WARN("failed to inner reuse", K(ret));
  } else {
    ObTruncateOrFilterExecutor *single_truncate_filter = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < truncate_info_array.count(); ++idx) {
      single_truncate_filter = nullptr;
      const ObTruncateInfo *truncate_info = truncate_info_array.at(idx);
      if (OB_UNLIKELY(nullptr == truncate_info || !truncate_info->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid truncate info", KR(ret), K(idx), KPC(truncate_info));
      } else if (OB_FAIL(try_use_cached_filter(schema_rowkey_cnt, cols_desc, *truncate_info, single_truncate_filter))) {
        LOG_WARN("failed to try use cached filter", K(ret));
      } else if (nullptr == single_truncate_filter &&
                 OB_FAIL(build_single_truncate_filter(schema_rowkey_cnt, cols_desc, *truncate_info,
                                                      filter_factory, single_truncate_filter))) {
        LOG_WARN("failed to build single trunate filter", K(ret));
      } else if (OB_ISNULL(single_truncate_filter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null single truncate filter", K(ret), K(idx), K(truncate_info));
      } else if (OB_FAIL(truncate_filters_.push_back(single_truncate_filter))) {
        LOG_WARN("failed to push back", K(ret));
        RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObTruncateOrFilterExecutor, single_truncate_filter);
      }
    }
    if (OB_SUCC(ret)) {
      valid_truncate_filter_cnt_ =  truncate_info_array.count();
    }
  }
  return ret;
}

int ObTruncateAndFilterExecutor::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < truncate_filters_.count(); ++i) {
    ObTruncateOrFilterExecutor *filter = truncate_filters_.at(i);
    if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null filter", K(ret), K(i));
    } else {
      ObIArray<ObTruncateOrFilterExecutor*> &filter_buffer = filter->is_subpart_filter() ?
          subpart_filter_buffer_ : part_filter_buffer_;
      if (OB_FAIL(filter_buffer.push_back(filter))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    truncate_filters_.reuse();
    valid_truncate_filter_cnt_ = 0;
  }
  return ret;
}

int ObTruncateAndFilterExecutor::try_use_cached_filter(
    const int64_t schema_rowkey_cnt,
    const ObIArray<share::schema::ObColDesc> &cols_desc,
    const storage::ObTruncateInfo &truncate_info,
    ObTruncateOrFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;
  filter = nullptr;
  ObIArray<ObTruncateOrFilterExecutor*> &filter_buffer = truncate_info.is_sub_part_ ? subpart_filter_buffer_ : part_filter_buffer_;
  if (!filter_buffer.empty()) {
    int64_t part_child_cnt = 0;
    if (OB_FAIL(filter_buffer.pop_back(filter))) {
      LOG_WARN("failed to pop back filter", K(ret));
    } else if (OB_ISNULL(filter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null cached filter", K(ret), K(filter_buffer));
    } else if (truncate_info.is_sub_part_) {
      part_child_cnt = filter->get_part_child_count();
    } else {
      part_child_cnt = filter->get_child_count();
    }
    if (FAILEDx(prepare_part_filter_param(schema_rowkey_cnt, truncate_info.commit_version_, cols_desc,
                                          truncate_info.truncate_part_, 0, part_child_cnt - 1,
                                          filter->get_childs(), false))) {
      LOG_WARN("failed to prepare part filter param", K(ret));
    } else if (part_child_cnt < filter->get_child_count() &&
               OB_FAIL(prepare_part_filter_param(schema_rowkey_cnt, truncate_info.commit_version_, cols_desc,
                                                 truncate_info.truncate_subpart_, part_child_cnt, filter->get_child_count() - 1,
                                                 filter->get_childs(), false))) {
      LOG_WARN("failed to prepare sub part filter param", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObTruncateOrFilterExecutor, filter);
  }
  return ret;
}

int ObTruncateAndFilterExecutor::filter(const ObDatumRow &row, bool &filtered)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    filtered = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_truncate_filter_cnt_ && !filtered; ++i) {
      if (OB_FAIL(truncate_filters_.at(i)->filter(row, filtered))) {
        LOG_WARN("failed to filter truncate filter", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTruncateAndFilterExecutor::build_single_truncate_filter(
    const int64_t schema_rowkey_cnt,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncateInfo &truncate_info,
    ObPushdownFilterFactory &filter_factory,
    ObTruncateOrFilterExecutor *&filter)
{
  int ret = OB_SUCCESS;
  int64_t filter_item_cnt = 0;
  int64_t part_filter_item_cnt = 0;
  if (OB_FAIL(ObTruncateFilterFactory::build_scn_filter(op_, filter_factory, item_buffer_[filter_item_cnt++]))) {
    LOG_WARN("failed to build scn filter", K(ret));
  } else if (OB_FAIL(build_single_part_filter(filter_factory, truncate_info.truncate_part_, filter_item_cnt))) {
    LOG_WARN("failed to build single part filter", K(ret));
  } else if (OB_FAIL(prepare_part_filter_param(schema_rowkey_cnt, truncate_info.commit_version_, cols_desc,
                                               truncate_info.truncate_part_, 0, filter_item_cnt - 1,
                                               item_buffer_))) {
    LOG_WARN("failed to prepare part filter param", K(ret));
  } else if (FALSE_IT(part_filter_item_cnt = filter_item_cnt)) {
  } else if (truncate_info.is_sub_part_) {
    const int64_t sub_item_start = filter_item_cnt;
    if (OB_FAIL(build_single_part_filter(filter_factory, truncate_info.truncate_subpart_, filter_item_cnt))) {
      LOG_WARN("failed to build single sub part filter", K(ret));
    } else if (OB_FAIL(prepare_part_filter_param(schema_rowkey_cnt, truncate_info.commit_version_, cols_desc,
                                                 truncate_info.truncate_subpart_, sub_item_start, filter_item_cnt - 1,
                                                 item_buffer_))) {
      LOG_WARN("failed to prepare sub part filter param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObPushdownFilterNode *or_node = nullptr;
    ObPushdownFilterExecutor *or_executor = nullptr;
    if (FAILEDx(filter_factory.alloc(TRUNCATE_OR_FILTER, filter_item_cnt, or_node))) {
      LOG_WARN("failed to alloc truncate and filter node", K(ret));
    } else if (OB_FAIL(filter_factory.alloc(TRUNCATE_OR_FILTER_EXECUTOR, filter_item_cnt, *or_node, or_executor, op_))) {
      LOG_WARN("failed to alloc truncate and filter executor", K(ret));
    } else {
      for (int64_t i = 0; i < filter_item_cnt; ++i) {
        or_executor->set_child(i, item_buffer_[i]);
        item_buffer_[i] = nullptr;
      }
      filter = static_cast<ObTruncateOrFilterExecutor*>(or_executor);
      filter->set_part_child_count(part_filter_item_cnt);
    }
    if (OB_FAIL(ret)) {
      RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterNode, or_node);
      RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterExecutor, or_executor);
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < filter_item_cnt; ++i) {
      RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterExecutor, item_buffer_[i]);
    }
  }
  return ret;
}

int ObTruncateAndFilterExecutor::build_single_part_filter(
    ObPushdownFilterFactory &filter_factory,
    const ObTruncatePartition &truncate_partition,
    int64_t &filter_item_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!truncate_partition.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate partition", K(ret), K(truncate_partition));
  } else {
    const bool is_range_part = ObTruncatePartition::is_range_part(truncate_partition.part_type_);
    const bool need_black = has_nullable_col_ || 1 != truncate_partition.part_key_idxs_.count();
    if (is_range_part) {
      const ObRowkey &low_bound = truncate_partition.low_bound_val_;
      const ObRowkey &high_bound = truncate_partition.high_bound_val_;
      if (OB_FAIL(ObTruncateFilterFactory::build_range_filter(op_, filter_factory, need_black,
                  item_buffer_[filter_item_pos], item_buffer_[filter_item_pos + 1]))) {
        LOG_WARN("failed to build range filter", K(ret));
      } else {
        filter_item_pos += 2;
      }
    } else {
      const ObStorageListRowValues &list_row_values = truncate_partition.list_row_values_;
      if (OB_UNLIKELY(ObTruncatePartition::ALL != truncate_partition.part_op_ && 0 == list_row_values.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(list_row_values));
      } else if (OB_FAIL(ObTruncateFilterFactory::build_list_filter(op_, filter_factory, need_black,
                item_buffer_[filter_item_pos++]))) {
        LOG_WARN("failed to build list filter", K(ret));
      }
    }
  }
  return ret;
}

int ObTruncateAndFilterExecutor::prepare_part_filter_param(
    const int64_t schema_rowkey_cnt,
    const int64_t truncate_commit_viersion,
    const ObIArray<ObColDesc> &cols_desc,
    const ObTruncatePartition &truncate_partition,
    const int64_t filter_item_begin,
    const int64_t filter_item_end,
    ObPushdownFilterExecutor **childs,
    const bool need_prepare_param)
{
  int ret = OB_SUCCESS;
  ObITruncateFilterExecutor *truncate_executor = nullptr;
  if (OB_UNLIKELY(filter_item_begin > filter_item_end ||
                  filter_item_begin < 0 ||
                  filter_item_end > MAX_FILTER_ITEM_CNT - 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(filter_item_begin), K(filter_item_end));
  }
  for (int64_t i = filter_item_begin; OB_SUCC(ret) && i <= filter_item_end; ++i) {
    ObPushdownFilterExecutor *filter_item = childs[i];
    if (OB_UNLIKELY(nullptr == filter_item || !filter_item->is_truncate_node())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected truncate executor type", K(ret), K(i), KPC(filter_item));
    } else if (filter_item->is_filter_white_node()) {
      truncate_executor = static_cast<ObTruncateWhiteFilterExecutor*>(filter_item);
    } else {
      truncate_executor = static_cast<ObTruncateBlackFilterExecutor*>(filter_item);
    }
    if (OB_FAIL(ret)) {
    } else if (need_prepare_param &&
               OB_FAIL(truncate_executor->prepare_truncate_param(schema_rowkey_cnt, cols_desc, truncate_partition))) {
      LOG_WARN("failed to prepare truncate param", K(ret), K(i));
    } else if (OB_FAIL(truncate_executor->prepare_truncate_value(truncate_commit_viersion, truncate_partition))) {
      LOG_WARN("failed to prepare truncate value", K(ret), K(i));
    }
  }
  return ret;
}

int ObTruncateAndFilterExecutor::execute_logic_filter(
    PushdownFilterInfo &filter_info,
    ObIMicroBlockRowScanner *micro_scanner,
    const bool use_vectorize,
    ObBitmap &result)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < valid_truncate_filter_cnt_; i++) {
    const ObBitmap *child_result = nullptr;
    if (OB_ISNULL(truncate_filters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child filter", K(ret), K(i));
    } else if (OB_FAIL(truncate_filters_.at(i)->execute(this, filter_info, micro_scanner, use_vectorize))) {
      LOG_WARN("failed to filter micro block", K(ret), K(i), KP(truncate_filters_.at(i)));
    } else if (OB_ISNULL(child_result = truncate_filters_.at(i)->get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected get null filter bitmap", K(ret));
    } else if (OB_FAIL(result.bit_and(*child_result))) {
      LOG_WARN("failed to merge result bitmap", K(ret), KP(child_result));
    } else if (result.is_all_false()) {
      break;
    }
  }
  return ret;
}

// ------------------------------------------------- ObTruncateFilterFactory -------------------------------------------------

#define SET_TRUNCATE_ITEM_TYPE(executor, type)                                              \
  if (executor->is_filter_white_node()) {                                                   \
    static_cast<ObTruncateWhiteFilterExecutor*>(executor)->set_truncate_item_type(type);    \
  } else {                                                                                  \
    static_cast<ObTruncateBlackFilterExecutor*>(executor)->set_truncate_item_type(type);    \
  }

#define SET_TRUNCATE_WHITE_FILTER_OP_TYPE(node, type)                                      \
  ObTruncateWhiteFilterNode *white_##node = static_cast<ObTruncateWhiteFilterNode*>(node); \
  white_##node->set_truncate_white_op_type(type);

int ObTruncateFilterFactory::build_scn_filter(
    ObPushdownOperator &op,
    ObPushdownFilterFactory &filter_factory,
    ObPushdownFilterExecutor *&scn_filter)
{
  int ret = OB_SUCCESS;
  scn_filter = nullptr;
  ObPushdownFilterNode *scn_node = nullptr;
  if (OB_FAIL(build_filter(op, filter_factory, false, scn_node, scn_filter))) {
    LOG_WARN("failed to build scn filter", K(ret));
  } else {
    SET_TRUNCATE_ITEM_TYPE(scn_filter, TRUNCATE_SCN);
    SET_TRUNCATE_WHITE_FILTER_OP_TYPE(scn_node, WHITE_OP_GT);
  }
  return ret;
}

int ObTruncateFilterFactory::build_range_filter(
    ObPushdownOperator &op,
    ObPushdownFilterFactory &filter_factory,
    const bool need_black,
    ObPushdownFilterExecutor *&low_filter,
    ObPushdownFilterExecutor *&high_filter)
{
  int ret = OB_SUCCESS;
  low_filter = nullptr;
  high_filter = nullptr;
  ObPushdownFilterNode *low_node = nullptr;
  ObPushdownFilterNode *high_node = nullptr;
  if (OB_FAIL(build_filter(op, filter_factory, need_black, low_node, low_filter))) {
    LOG_WARN("failed to build low filter", K(ret));
  } else if (OB_FAIL(build_filter(op, filter_factory, need_black, high_node, high_filter))) {
    LOG_WARN("failed to build high filter", K(ret));
  } else {
    SET_TRUNCATE_ITEM_TYPE(low_filter, TRUNCATE_RANGE_LEFT);
    SET_TRUNCATE_ITEM_TYPE(high_filter, TRUNCATE_RANGE_RIGHT);
    if (!need_black) {
      SET_TRUNCATE_WHITE_FILTER_OP_TYPE(low_node, WHITE_OP_LT);
      SET_TRUNCATE_WHITE_FILTER_OP_TYPE(high_node, WHITE_OP_GE);
    }
  }
  if (OB_FAIL(ret)) {
    RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterExecutor, low_filter);
    RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterExecutor, high_filter);
  }
  return ret;
}

int ObTruncateFilterFactory::build_list_filter(
    ObPushdownOperator &op,
    ObPushdownFilterFactory &filter_factory,
    const bool need_black,
    ObPushdownFilterExecutor *&list_filter)
{
  int ret = OB_SUCCESS;
  list_filter = nullptr;
  ObPushdownFilterNode *list_node = nullptr;
  if (OB_FAIL(build_filter(op, filter_factory, need_black, list_node, list_filter))) {
    LOG_WARN("failed to build list filter", K(ret));
  } else {
    SET_TRUNCATE_ITEM_TYPE(list_filter, TRUNCATE_LIST);
    if (!need_black) {
      // the real op type is determined in prepare_truncate_list_value
      // (WHITE_OP_EQ or WHITE_OP_IN) and need_flip=true
      SET_TRUNCATE_WHITE_FILTER_OP_TYPE(list_node, WHITE_OP_MAX);
    }
  }
  return ret;
}

int ObTruncateFilterFactory::build_filter(
    ObPushdownOperator &op,
    ObPushdownFilterFactory &filter_factory,
    const bool need_black,
    ObPushdownFilterNode *&node,
    ObPushdownFilterExecutor *&executor)
{
  int ret = OB_SUCCESS;
  const PushdownFilterType node_type = need_black ? TRUNCATE_BLACK_FILTER : TRUNCATE_WHITE_FILTER;
  const PushdownExecutorType executor_type = need_black ? TRUNCATE_BLACK_FILTER_EXECUTOR : TRUNCATE_WHITE_FILTER_EXECUTOR;
  if (OB_FAIL(filter_factory.alloc(node_type, 0, node))) {
    LOG_WARN("failed to alloc low node", K(ret));
  } else if (OB_FAIL(filter_factory.alloc(executor_type, 0, *node, executor, op))) {
    LOG_WARN("failed to alloc low executor", K(ret));
  }
  if (OB_FAIL(ret)) {
    RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterNode, node);
    RELEASE_TRUNCATE_PTR_WHEN_FAILED(ObPushdownFilterExecutor, executor);
  }
  return ret;
}

}
}
