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

#include "ob_memtable_block_reader.h"
#include "ob_memtable_single_row_reader.h"
#include "ob_memtable.h"
#include "storage/access/ob_table_access_context.h"

namespace oceanbase {
using namespace storage;
using namespace blocksstable;

namespace memtable {
int ObMemtableBlockReader::init(const bool is_delete_insert)
{
  int ret = OB_SUCCESS;
  read_info_ = single_row_reader_.get_read_info();
  if (OB_UNLIKELY(nullptr == read_info_ || !read_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read info", K(ret), KPC_(read_info), K_(single_row_reader));
  } else {
    int64_t request_cnt = read_info_->get_request_count();
    for (int i = 0; OB_SUCC(ret) && i < MAX_BATCH_SIZE; ++i) {
      if (OB_FAIL(rows_[i].init(allocator_, request_cnt))) {
        LOG_WARN("init datum row failed", KR(ret), K(i), K(request_cnt));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row_count_ = 0;
    datum_utils_ = &(read_info_->get_datum_utils());
    is_delete_insert_ = is_delete_insert;
    is_inited_ = true;
  }

  return ret;
}

void ObMemtableBlockReader::reuse()
{
  for (int i = 0; i < row_count_; ++i) {
    rows_[i].reuse();
    for (int j = 0; j < rows_[i].get_column_count(); ++j) {
      rows_[i].storage_datums_[j].set_nop();
    }
  }
  row_count_ = 0;
  has_lob_out_row_ = false;
  is_single_version_rows_ = true;
  inner_allocator_.reset();
}

void ObMemtableBlockReader::reset()
{
  has_lob_out_row_ = false;
  is_single_version_rows_ = true;
  is_delete_insert_ = false;
  ObIMicroBlockReader::reset();
  for (int i = 0; i < MAX_BATCH_SIZE; ++i) {
    rows_[i].reset();
  }
  inner_allocator_.reset();
}

int ObMemtableBlockReader::get_row(const int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > index || row_count_ <= index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count));
  } else if (OB_FAIL(row.shallow_copy(rows_[index]))) {
    LOG_WARN("failed to shallow copy datum row", K(ret), K(rows_[index]));
  } else {
    row.fast_filter_skipped_ = false;
  }
  return ret;
}

int ObMemtableBlockReader::get_row_delete_version(const int64_t index, int64_t &delete_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > index || row_count_ <= index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count));
  } else if (OB_UNLIKELY(!rows_[index].row_flag_.is_delete())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row", K(ret), K(rows_[index]));
  } else {
    delete_version = rows_[index].snapshot_version_;
  }
  return ret;
}

int ObMemtableBlockReader::prefetch_rows()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    reuse();
    // read rows from single reader
    int64_t acquired_row_cnt = 0;
    int64_t cap = is_delete_insert_ ? MAX_BATCH_SIZE - 1 : MAX_BATCH_SIZE;
    for (; OB_SUCC(ret) && row_count_ < cap;) {
      if (is_delete_insert_) {
        ret = single_row_reader_.fill_in_next_delete_insert_row(
            rows_[row_count_], rows_[row_count_ + 1], acquired_row_cnt);
      } else {
        ret = single_row_reader_.fill_in_next_row(rows_[row_count_]);
        acquired_row_cnt = 1;
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(acquired_row_cnt < 1 || acquired_row_cnt > 2)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected row count", K(ret), K(acquired_row_cnt), K_(is_delete_insert));
        } else {
          if (is_single_version_rows_ &&
              (acquired_row_cnt == 2 || rows_[row_count_].row_flag_.is_delete())) {
            is_single_version_rows_ = false;
          }
          if (acquired_row_cnt > 1) {
            row_count_++;
          }
          rows_[row_count_].set_last_multi_version_row();
          row_count_++;
        }
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", K(ret), K_(is_delete_insert));
      }
    }

    if (OB_ITER_END == ret && 0 < row_count_) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMemtableBlockReader::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datum_buf = pd_filter_info.datum_buf_;
  const storage::ObTableIterParam *param = pd_filter_info.param_;
  storage::ObTableAccessContext *context = pd_filter_info.context_;
  const bool has_lob_out_row = param->has_lob_column_out();
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_ ||
                  pd_filter_info.is_pd_to_cg_ ||
                  (has_lob_out_row && nullptr == context->lob_locator_helper_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K_(row_count), K(pd_filter_info),
             K(has_lob_out_row), KP(context->lob_locator_helper_));
  } else {
    int64_t col_count = filter.get_col_count();
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets();
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    const common::ObIArray<ObStorageDatum> &default_datums = filter.get_default_datums();
    const ObColDescIArray &cols_desc = read_info_->get_columns_desc();
    int64_t row_idx = 0;
    bool found_lob_out_row = false;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      found_lob_out_row = false;
      row_idx = offset + pd_filter_info.start_;
      if (pd_filter_info.can_skip_filter_delete_insert(offset)) {
        continue;
      } else if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      } else if (0 < col_count) {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
          ObStorageDatum &datum = datum_buf[i];
          const ObObjType obj_type = cols_desc.at(col_offsets.at(i)).col_type_.get_type();
          const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(obj_type);
          ObStorageDatum &src_datum = rows_[row_idx].storage_datums_[col_offsets.at(i)];
          datum.reuse();
          if (src_datum.is_nop_value()) {
            if (OB_UNLIKELY(default_datums.at(i).is_nop())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected nop value", K(ret), K(col_offsets.at(i)), K(row_idx));
            } else if (OB_FAIL(datum.from_storage_datum(default_datums.at(i), map_type))) {
              LOG_WARN("Failed to convert storage datum", K(ret), K(i), K(default_datums.at(i)), K(obj_type), K(map_type));
            }
          } else if (OB_FAIL(datum.from_storage_datum(src_datum, map_type))) {
            LOG_WARN("Failed to convert storage datum", K(ret), K(i), K(src_datum), K(obj_type), K(map_type));
          }
          if (OB_FAIL(ret) || nullptr == col_params.at(i) || datum.is_null()) {
          } else if (need_padding(filter.is_padding_mode(), col_params.at(i)->get_meta_type())) {
            if (OB_FAIL(storage::pad_column(
                        col_params.at(i)->get_meta_type(),
                        col_params.at(i)->get_accuracy(),
                        inner_allocator_,
                        datum))) {
              LOG_WARN("Failed to pad column", K(ret), K(i), K(col_offsets.at(i)), K(row_idx), K(datum));
            }
          } else if (has_lob_out_row && col_params.at(i)->get_meta_type().is_lob_storage() && !datum.get_lob_data().in_row_) {
            if (OB_FAIL(context->lob_locator_helper_->fill_lob_locator_v2(datum, *col_params.at(i), *param, *context))) {
              LOG_WARN("Failed to fill lob loactor", K(ret), K(has_lob_out_row), K(datum), KPC(context), KPC(param));
            } else {
              found_lob_out_row = true;
            }
          }
        }
      }

      bool filtered = false;
      if (OB_SUCC(ret)) {
        if (filter.is_filter_black_node() || found_lob_out_row) {
          sql::ObPhysicalFilterExecutor &physical_filter = static_cast<sql::ObPhysicalFilterExecutor &>(filter);
          if (OB_FAIL(physical_filter.filter(datum_buf, col_count, *pd_filter_info.skip_bit_, filtered))) {
            LOG_WARN("Failed to filter row with black filter", K(ret), K(row_idx));
          }
          if (found_lob_out_row) {
            context->lob_locator_helper_->reuse();
          }
        } else {
          sql::ObWhiteFilterExecutor &white_filter = static_cast<sql::ObWhiteFilterExecutor &>(filter);
          if (1 != filter.get_col_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected col_ids count: not 1", K(ret), K(filter));
          } else if (OB_FAIL(filter_white_filter(white_filter, datum_buf[0], filtered))) {
            LOG_WARN("Failed to filter row with white filter", K(ret), K(row_idx));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!filtered) {
          if (OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(offset));
          }
        }
      }
    }
    LOG_TRACE("[PUSHDOWN] memtable block pushdown filter row", K(ret), K(has_lob_out_row),
              K(pd_filter_info), K(col_offsets), K(result_bitmap.popcnt()), K(result_bitmap));
  }
  return ret;
}

}  // end of namespace memtable
}  // end of namespace oceanbase
