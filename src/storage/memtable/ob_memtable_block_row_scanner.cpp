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

#include "ob_memtable_block_row_scanner.h"
#include "storage/access/ob_block_row_store.h"
#include "storage/blocksstable/ob_row_reader.h"

namespace oceanbase
{
namespace memtable
{


ObMemtableBlockRowScanner::ObMemtableBlockRowScanner()
    : last_batch_(false),
      end_of_iter_(false),
      is_inited_(false),
      cur_idx_(0),
      batch_size_(0),
      allocator_(NULL),
      result_bitmap_(NULL),
      read_info_(NULL)
{
}

ObMemtableBlockRowScanner::~ObMemtableBlockRowScanner()
{
  reset();
}

int ObMemtableBlockRowScanner::init(
    common::ObIAllocator *allocator,
    const int64_t capacity,
    char *trans_info_ptr,
    const storage::ObITableReadInfo *read_info)
{
  if (is_inited_) {
    reset();
  }
  int ret = OB_SUCCESS;
  allocator_ = allocator;
  read_info_ = read_info;
  for (int i = 0; i < ObMemtableBlockRowScanner::MAX_ROW_BATCH_SIZE; ++i) {
    if (OB_FAIL(rows_[i].init(*allocator_, capacity, trans_info_ptr))) {
      STORAGE_LOG(WARN, "Fail to init memtable scan row batch", K(ret), K(i));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObMemtableBlockRowScanner::reset()
{
  for (int i = 0; i < ObMemtableBlockRowScanner::MAX_ROW_BATCH_SIZE; ++i) {
    rows_[i].reset();
  }
  last_batch_ = false;
  end_of_iter_ = false;
  cur_idx_ = 0;
  batch_size_ = 0;
  result_bitmap_ = nullptr;
}

void ObMemtableBlockRowScanner::reuse()
{
  last_batch_ = false;
  end_of_iter_ = false;
  cur_idx_ = 0;
  batch_size_ = 0;
  result_bitmap_ = nullptr;
}

blocksstable::ObDatumRow *ObMemtableBlockRowScanner::get_next_row(bool fast_filter_skipped)
{
  rows_[cur_idx_].fast_filter_skipped_ = fast_filter_skipped;
  return rows_ + (cur_idx_++);
}

bool ObMemtableBlockRowScanner::has_cached_row()
{
  bool ret_valid = false;
  if (OB_ISNULL(result_bitmap_)) {
    // no bitmap, directly return next row in cur_idx_;
    ret_valid = (cur_idx_ < batch_size_);
  } else if (cur_idx_ < batch_size_) {
    int iter = cur_idx_;
    // find next row which is not filtered by the result bimap.
    for (; iter < batch_size_ && !result_bitmap_->test(iter); ++iter) {}
    if (iter < batch_size_) {
      // if find one, update cur_idx_ to avoid next search.
      cur_idx_ = iter;
      ret_valid = true;
    } else {
      // all rows left in current batch are filtered out.
      ret_valid = false;
    }
  } else {
    // the rows in the current batch have been used up.
    ret_valid = false;
  }
  return ret_valid;
}

int ObMemtableBlockRowScanner::filter_pushdown_filter(
    sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter,
    storage::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &bitmap)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageDatum *col_buf = pd_filter_info.datum_buf_;
  // TODO(jianxian): validate_filter_info.
  int64_t col_count = filter->get_col_count();
  const common::ObIArray<int32_t> &col_offsets = filter->get_col_offsets();
  const sql::ColumnParamFixedArray &col_params = filter->get_col_params();
  const common::ObIArray<blocksstable::ObStorageDatum> &default_datums = filter->get_default_datums();
  const blocksstable::ObColDescIArray &cols_desc = read_info_->get_columns_desc();

  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size_; ++row_idx) {
    if (nullptr != parent && parent->can_skip_filter(row_idx)) {
      continue;
    } else if (0 < col_count) {
      blocksstable::ObDatumRow *cur_row = rows_ + row_idx;
      for (int i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        int64_t col_idx = col_offsets.at(i);
        blocksstable::ObStorageDatum &datum = col_buf[i];
        datum = cur_row->storage_datums_[col_idx];
        if (datum.is_nop_value()) {
          // TODO(jianxian): check default_datums size, use i or col_idx.
          if (OB_LIKELY(!default_datums.at(i).is_nop())) {
            datum = default_datums.at(i);
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected nop value", K(i), K(col_idx), K(*cur_row));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (nullptr != col_params.at(i) && !datum.is_null() &&
                   OB_FAIL(storage::pad_column(
                           col_params.at(i)->get_meta_type(),
                           col_params.at(i)->get_accuracy(),
                           *allocator_,
                           datum))) {
          STORAGE_LOG(WARN, "Failed to pad column", K(ret), K(i), K(col_idx), K(row_idx), K(datum));
        }
      }
    }
    bool filtered = false;
    if (OB_SUCC(ret)) {
      if (filter->is_filter_black_node()) {
        sql::ObBlackFilterExecutor &black_filter = static_cast<sql::ObBlackFilterExecutor &>(*filter);
        if (OB_FAIL(black_filter.filter(col_buf, col_count, filtered))) {
          STORAGE_LOG(WARN, "Failed to filter row with black filter", K(ret), K(row_idx));
        }
      } else {
        sql::ObWhiteFilterExecutor &white_filter = static_cast<sql::ObWhiteFilterExecutor &>(*filter);
        common::ObObj obj;
        if (1 != filter->get_col_count()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected col_ids count: not 1", K(ret), K(filter), K(filter->get_col_count()));
        } else if (OB_FAIL(col_buf[0].to_obj_enhance(obj, cols_desc.at(col_offsets.at(0)).col_type_))) {
          STORAGE_LOG(WARN, "Failed to obj", K(ret), K(row_idx), K(col_buf[0]));
        } else if (OB_FAIL(filter_white_filter(white_filter, obj, filtered))) {
          STORAGE_LOG(WARN, "Failed to filter row with white filter", K(ret), K(row_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!filtered) {
        if (OB_FAIL(bitmap.set(row_idx))) {
          STORAGE_LOG(WARN, "Failed to set result bitmap", K(ret), K(row_idx));
        }
      }
    }
  }
  return ret;
}

int ObMemtableBlockRowScanner::filter_white_filter(
    const sql::ObWhiteFilterExecutor &filter,
    const common::ObObj &obj,
    bool &filtered)
{
  int ret = OB_SUCCESS;
  filtered = true;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  if (OB_UNLIKELY(sql::WHITE_OP_MAX <= op_type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid operator type of Filter node", K(ret), K(filter));
  } else {
    const ObIArray<ObObj> &ref_objs = filter.get_objs();
    switch (op_type) {
      case sql::WHITE_OP_NN: {
        if ((lib::is_mysql_mode() && !obj.is_null())
            || (lib::is_oracle_mode() && !obj.is_null_oracle())) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_NU: {
        if ((lib::is_mysql_mode() && obj.is_null())
            || (lib::is_oracle_mode() && obj.is_null_oracle())) {
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
        if (OB_UNLIKELY(ref_objs.count() != 1)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "Invalid argument for comparison operator", K(ret), K(ref_objs));
        } else if ((lib::is_mysql_mode() && (obj.is_null() || ref_objs.at(0).is_null()))
                    || (lib::is_oracle_mode() && (obj.is_null_oracle() || ref_objs.at(0).is_null_oracle()))) {
          // Result of compare with null is null
        } else if (ObObjCmpFuncs::compare_oper_nullsafe(
                   obj,
                   ref_objs.at(0),
                   obj.get_collation_type(),
                   sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type])) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        if (OB_UNLIKELY(ref_objs.count() != 2)) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "Invalid argument for between operators", K(ret), K(ref_objs));
        } else if ((lib::is_mysql_mode() && obj.is_null())
                    || (lib::is_oracle_mode() && obj.is_null_oracle())) {
          // Result of compare with null is null
        } else if (obj >= ref_objs.at(0) && obj <= ref_objs.at(1)) {
          filtered = false;
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        bool is_existed = false;
        if (OB_FAIL(filter.exist_in_obj_set(obj, is_existed))) {
          STORAGE_LOG(WARN, "Failed to check object in hashset", K(ret), K(obj));
        } else if (is_existed) {
          filtered = false;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "Unexpected filter pushdown operation type", K(ret), K(op_type));
      }
    } // end of switch
  }
  return ret;
}

} // end of namespace memtable
} // end of namespace oceanbase