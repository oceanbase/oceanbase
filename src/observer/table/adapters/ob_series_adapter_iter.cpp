/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "ob_series_adapter_iter.h"

namespace oceanbase
{   
namespace table
{

/**
 * ---------------------------------------- ObAdapterCellCompare ----------------------------------------
 */

int ObAdapterCellCompare::compare(const common::ObNewRow *lhs, const common::ObNewRow *rhs, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, ObNewRow cell is null, can not compare", K(ret), KPC(lhs), KPC(rhs));
  } else if (lhs->get_count() <= ObHTableConstants::COL_IDX_Q || rhs->get_count() <= ObHTableConstants::COL_IDX_Q) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, ObNewRow cell count less than 2, can not compare", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = rhs->get_cell(ObHTableConstants::COL_IDX_Q)
                  .get_string()
                  .compare(lhs->get_cell(ObHTableConstants::COL_IDX_Q).get_string());
  }
  return ret;
}

bool ObAdapterCellCompare::operator()(const common::ObNewRow *lhs, const common::ObNewRow *rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret < 0;
}

/**
 * ---------------------------------------- ObHbaseSeriesCellIter ----------------------------------------
 */

ObHbaseSeriesCellIter::ObHbaseSeriesCellIter()
    : ObHbaseCellIter(),
      copy_alloc_("HtSeriIterAloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      unique_qualifier_(),
      compare_(),
      rows_heap_(compare_),
      next_row_(nullptr),
      is_inited_(false),
      origin_range_(),
      now_k_(),
      now_t_(INT_MAX64),
      iter_is_end_(false)
{}

int ObHbaseSeriesCellIter::init()
{
  int ret = OB_SUCCESS;
  // 先填充next_row
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iterator init twice", K(ret));
  } else if (!unique_qualifier_.created()) {
    if (OB_FAIL(unique_qualifier_.create(ObHTableConstants::INITIAL_MAX_QUALIFIER_SET, ObMemAttr(MTL_ID(), "QualfSet")))) {
      LOG_WARN("failed to create unique qualifier set", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tb_row_iter_.get_next_row(next_row_))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      // out of key range
      iter_is_end_ = true;
      LOG_DEBUG("iterator is end", K(ret));
    } else {
      LOG_WARN("fail to get next cell", K(ret));
    }
  } else if (OB_ISNULL(next_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next_row_ is null in init", K(ret));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObHbaseSeriesCellIter::convert_series_to_normal(ObNewRow &series_row, ObIArray<ObNewRow *> &normal_rows)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *child_bin = nullptr;
  ObObj json_obj = series_row.get_cell(ObHTableConstants::COL_IDX_V);
  if (!json_obj.has_lob_header() || !is_lob_storage(json_obj.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value is not a lob type", K(ret), K(json_obj));
  } else if (OB_FAIL(ObTableCtx::read_real_lob(copy_alloc_, json_obj))) {
    LOG_WARN("fail to read lob", K(ret), K(json_obj));
  } else {
    ObJsonBin bin(json_obj.get_string_ptr(), json_obj.get_string_len(), &allocator_);
    if (OB_FAIL(bin.reset_iter())) {
      LOG_WARN("fail to reset iter", K(ret), K(json_obj));
    }
    for (int i = 0; OB_SUCC(ret) && i < bin.element_count(); ++i) {
      ObObj *obj_array = nullptr;
      ObNewRow *new_row = nullptr;
      int N = series_row.get_count();
      ObObj tmp_array[4];
      if (OB_FAIL(bin.get_object_value(i, key, child_bin))) {
        LOG_WARN("fail to get object value", K(ret), K(i), K(bin));
      } else if (OB_ISNULL(child_bin)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_bin is null", K(ret));
      } else if (child_bin->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child_bin type is not string object", K(ret), KPC(child_bin));
      } else {
        tmp_array[ObHTableConstants::COL_IDX_K] = series_row.get_cell(ObHTableConstants::COL_IDX_K);
        tmp_array[ObHTableConstants::COL_IDX_Q].set_varbinary(key);
        tmp_array[ObHTableConstants::COL_IDX_T] = series_row.get_cell(ObHTableConstants::COL_IDX_SER_T);
        ObString value(child_bin->get_data_length(), child_bin->get_data());
        tmp_array[ObHTableConstants::COL_IDX_V].set_varbinary(value);
        if (OB_ISNULL(obj_array = static_cast<ObObj *>(copy_alloc_.alloc(sizeof(ObObj) * N)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for start_obj failed", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < 4; i++) {
            if (OB_FAIL(ob_write_obj(copy_alloc_, tmp_array[i], obj_array[i]))) {
              LOG_WARN("fail to deep copy ob_row", KR(ret));
            }
          }
          for (int i = 4; OB_SUCC(ret) && i < N; i++) {
            // generator column
            if (OB_FAIL(ob_write_obj(copy_alloc_, series_row.get_cell(i), obj_array[i]))) {
              LOG_WARN("fail to deep copy ob_row", KR(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(new_row = OB_NEWx(ObNewRow, (&copy_alloc_), obj_array, N))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory for new_row failed", K(ret));
            } else if (OB_FAIL(normal_rows.push_back(new_row))) {
              LOG_WARN("push back normal rows failed ", K(ret), K(normal_rows));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHbaseSeriesCellIter::handle_json_row(ObNewRow &series_row)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObNewRow *, 8> normal_rows;
  normal_rows.set_attr(ObMemAttr(MTL_ID(), "CelIterNorRows"));
  if (series_row.get_count() < 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row column count less than 4", K(ret), K(series_row.get_count()));
  } else if (OB_FAIL(ob_write_string(copy_alloc_, series_row.get_cell(ObHTableConstants::COL_IDX_K).get_varchar(), now_k_))) {
    LOG_WARN("fail to write string", K(ret), K(series_row.get_cell(ObHTableConstants::COL_IDX_K)));
  } else {
    now_t_ = series_row.get_cell(ObHTableConstants::COL_IDX_SER_T).get_int();
    if (OB_FAIL(convert_series_to_normal(series_row, normal_rows))) {
      LOG_WARN("fail to convert series to normal", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < normal_rows.count(); i++) {
        ObNewRow *tmp_row = normal_rows[i];
        if (OB_ISNULL(tmp_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tmp_row is null", K(ret));
        } else {
          ObString qualifier = tmp_row->get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
          if (OB_FAIL(unique_qualifier_.exist_refactored(qualifier))) {
            if (ret == OB_HASH_EXIST) {
              ret = OB_SUCCESS;
            } else if (ret == OB_HASH_NOT_EXIST){
              if (OB_FAIL(rows_heap_.push(tmp_row))) {
                LOG_WARN("fail to push row to heap", K(ret), K(i), K(rows_heap_));
              } else if (OB_FAIL(unique_qualifier_.set_refactored(qualifier))) {
                LOG_WARN("fail to add qualifier to unique qualifier set", K(ret), K(i), K(qualifier));
              }
            } else {
              LOG_WARN("fail to check qualifier exist", K(qualifier));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("occur error when check exist qualifier", K(ret), K(i), K(qualifier));
          }
        }
      }
    }
  }
  return ret;
}

int ObHbaseSeriesCellIter::check_left_border(ObNewRow &row, bool &left_inclusive) {
  int ret = OB_SUCCESS;
  ObRowkey &start_key = origin_range_.start_key_;
  ObBorderFlag &border_flag = origin_range_.border_flag_;
  // compare key
  const ObString &key = row.get_cell(ObHTableConstants::COL_IDX_K).get_varchar();
  if (start_key.is_min_row()) {
    left_inclusive = true;
  } else if (start_key.get_obj_cnt() < ObHTableConstants::COL_IDX_T) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_key column count less than 3", K(ret), K(start_key.get_obj_cnt()));
  } else if (start_key.get_obj_ptr()[0].get_varchar().compare(key) < 0) {
    left_inclusive = true;
  } else if (start_key.get_obj_ptr()[0].get_varchar().compare(key) == 0) {
    // compare qualifier
    const ObString &qualifier = row.get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
    if (start_key.get_obj_ptr()[1].is_min_value()) {
      left_inclusive = true;
    } else if (start_key.get_obj_ptr()[1].is_max_value()) {
      left_inclusive = false;
    } else if (start_key.get_obj_ptr()[1].get_varchar().compare(qualifier) < 0) {
      left_inclusive = true;
    } else if (start_key.get_obj_ptr()[1].get_varchar().compare(qualifier) == 0) {
      // compare timestamp
      const int64_t &timestamp = row.get_cell(ObHTableConstants::COL_IDX_T).get_int();
      if (start_key.get_obj_ptr()[2].is_min_value()) {
        left_inclusive = true;
      } else if (start_key.get_obj_ptr()[2].is_max_value()) {
        left_inclusive = false;
      } else if (start_key.get_obj_ptr()[2].get_int() > timestamp) {
        left_inclusive = true;
      } else if (start_key.get_obj_ptr()[2].get_int() == timestamp && border_flag.inclusive_start()) {
        left_inclusive = true;
      }
    }
  }
  return ret;
}

int ObHbaseSeriesCellIter::check_right_border(ObNewRow &row, bool &right_inclusive) {
  int ret = OB_SUCCESS;
  ObRowkey &end_key = origin_range_.end_key_;
  ObBorderFlag &border_flag = origin_range_.border_flag_;
  // compare key
  const ObString &key = row.get_cell(ObHTableConstants::COL_IDX_K).get_varchar();
  if (end_key.is_max_row()) {
    right_inclusive = true;
  } else if (end_key.get_obj_cnt() < ObHTableConstants::COL_IDX_T) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_key column count less than 3", K(ret), K(end_key.get_obj_cnt()));
  } else if (end_key.get_obj_ptr()[0].get_varchar().compare(key) > 0) {
    right_inclusive = true;
  } else if (end_key.get_obj_ptr()[0].get_varchar().compare(key) == 0) {
    // compare qualifier
    const ObString &qualifier = row.get_cell(ObHTableConstants::COL_IDX_Q).get_varchar();
    if (end_key.get_obj_ptr()[1].is_min_value()) {
      right_inclusive = false;
    } else if (end_key.get_obj_ptr()[1].is_max_value()) {
      right_inclusive = true;
    } else if (end_key.get_obj_ptr()[1].get_varchar().compare(qualifier) > 0) {
      right_inclusive = true;
    } else if (end_key.get_obj_ptr()[1].get_varchar().compare(qualifier) == 0) {
      // compare timestamp
      const int64_t &timestamp = row.get_cell(ObHTableConstants::COL_IDX_T).get_int();
      if (end_key.get_obj_ptr()[2].is_min_value()) {
        right_inclusive = false;
      } else if (end_key.get_obj_ptr()[2].is_max_value()) {
        right_inclusive = true;
      } else if (end_key.get_obj_ptr()[2].get_int() < timestamp) {
        right_inclusive = true;
      } else if (end_key.get_obj_ptr()[2].get_int() == timestamp && border_flag.inclusive_start()) {
        right_inclusive = true;
      }
    }
  }
  return ret;
}

int ObHbaseSeriesCellIter::is_in_range(ObNewRow &row, bool &in_range)
{
  int ret = OB_SUCCESS;
  in_range = false;
  bool left_inclusive = false;
  bool right_inclusive = false;
  if (!row.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret), K(row));
  } else if (row.get_count() < ObHTableConstants::COL_IDX_T) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row column count less than 3", K(ret), K(row.get_count()));
  } else if (OB_FAIL(check_left_border(row, left_inclusive))) {
    LOG_WARN("fail to check left border", K(ret));
  } else if (OB_FAIL(check_right_border(row, right_inclusive))) {
    LOG_WARN("fail to check right border", K(ret));
  } else {
    in_range = left_inclusive && right_inclusive;
  }
  return ret;
}

bool ObHbaseSeriesCellIter::same_kt(ObNewRow &json_row)
{
  bool is_same = false;
  if (json_row.get_count() < 2) {
    is_same = false;
  } else if (now_k_.case_compare(json_row.get_cell(ObHTableConstants::COL_IDX_K).get_varchar()) == 0 &&
             json_row.get_cell(ObHTableConstants::COL_IDX_SER_T).get_int() == now_t_) {
    is_same = true;
  }
  return is_same;
}

int ObHbaseSeriesCellIter::get_next_cell(ObNewRow *&row) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_cell(row, 0))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next cell", K(ret));
    }
  }
  LOG_DEBUG("ObHbaseSeriesCellIter::get_next_cell", K(ret), KPC(row));
  return ret;
}

int ObHbaseSeriesCellIter::rescan(ObHbaseRescanParam &rescan_param)
{
  int ret = OB_SUCCESS;
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("series iterator is not support rescan", K(ret));
  return ret;
}

int ObHbaseSeriesCellIter::get_next_cell(ObNewRow *&row, uint8_t depth)
{
  int ret = OB_SUCCESS;
  // 1. add cell to rows_heap
  if (!is_opened_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is not opened", K(ret));
  } else if (!is_inited_ && OB_FAIL(init())) {
    LOG_WARN("fail to init", K(ret));
  } else if (rows_heap_.empty() && OB_ISNULL(next_row_)) {
    ret = OB_ITER_END;
  } else if (rows_heap_.empty()) {
    // reuse pre cells
    ObNewRow *json_row = nullptr;
    copy_alloc_.reuse();
    unique_qualifier_.reuse();
    if (OB_FAIL(handle_json_row(*next_row_))) {
      LOG_WARN("fail to add next row to heap", K(ret));
    } else if (FALSE_IT(next_row_ = nullptr)) {
    } else if (!iter_is_end_) {
      bool loop = true;
      while (OB_SUCC(ret) && loop) {
        if (OB_FAIL(tb_row_iter_.get_next_row(json_row))) {
          if (ret == OB_ITER_END) {
            // out of key range
            iter_is_end_ = true;
            LOG_DEBUG("iterator is end", K(ret));
          } else {
            LOG_WARN("fail to get next cell", K(ret));
          }
        } else if (OB_ISNULL(json_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json_row is null", K(ret));
        } else if (same_kt(*json_row)) {
          // split a json-type ObNewRow into multiple normal ObNewRow.
          if (OB_FAIL(handle_json_row(*json_row))) {
            LOG_WARN("fail to handle json row", K(ret));
          }
        } else {
          next_row_ = json_row;
          loop = false;
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  // 2. take cell from rows_heap
  if (OB_SUCC(ret)) {
    bool in_range = false;
    while (OB_SUCC(ret) && !in_range && !rows_heap_.empty()) {
      row = rows_heap_.top();
      if (OB_FAIL(rows_heap_.pop())) {
        LOG_WARN("fail to pop", K(ret));
      } else if (OB_FAIL(compare_.get_error_code())) {
        ret = compare_.get_error_code();
        LOG_WARN("fail to compare row", K(ret)); 
      } else if (OB_FAIL(is_in_range(*row, in_range))) {
        LOG_WARN("fail to eval row is valid", K(ret), KPC(row), K(in_range));
      }
    }
    if (OB_SUCC(ret) && !in_range) {
      if (depth > 0) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("series iter is too deep", K(ret), K(depth));
      } else if (OB_FAIL(get_next_cell(row, depth + 1))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next cell", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end of namespace table
} // end of namespace oceanbase
  