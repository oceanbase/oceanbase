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
#include "ob_micro_block_reader.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx_table/ob_tx_table.h"
#include "share/ob_force_print_log.h"
#include "storage/access/ob_aggregated_store.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{


template<typename ReaderType>
class PreciseCompare
{
public:
  PreciseCompare(
      int &ret,
      bool &equal,
      ReaderType *reader,
      const char *data_begin,
      const int32_t *index_data,
      const ObStorageDatumUtils *datum_utils)
      : ret_(ret),
      equal_(equal),
      reader_(reader),
      data_begin_(data_begin),
      index_data_(index_data),
      datum_utils_(datum_utils) {}
  ~PreciseCompare() {}
  inline bool operator() (const int64_t row_idx, const ObDatumRowkey &rowkey)
  {
    return compare(row_idx, rowkey, true);
  }
  inline bool operator() (const ObDatumRowkey &rowkey, const int64_t row_idx)
  {
    return compare(row_idx, rowkey, false);
  }
private:
  inline bool compare(const int64_t row_idx, const ObDatumRowkey &rowkey, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(reader_->compare_meta_rowkey(
                rowkey,
                *datum_utils_,
                data_begin_ + index_data_[row_idx],
                index_data_[row_idx + 1] - index_data_[row_idx],
                compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret), K(rowkey), KPC_(datum_utils));
    } else {
      bret = lower_bound ? compare_result < 0 : compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal reuslt, must prevent it from being modified again
      if (0 == compare_result && !equal_) {
        equal_ = true;
      }
    }
    return bret;
  }

private:
  int &ret_;
  bool &equal_;
  ReaderType *reader_;
  const char *data_begin_;
  const int32_t *index_data_;
  const ObStorageDatumUtils *datum_utils_;
};

ObIMicroBlockFlatReader::ObIMicroBlockFlatReader()
  : header_(nullptr),
    data_begin_(nullptr),
    data_end_(nullptr),
    index_data_(nullptr),
    allocator_(ObModIds::OB_STORE_ROW_GETTER),
    flat_row_reader_()
{
}

ObIMicroBlockFlatReader::~ObIMicroBlockFlatReader()
{
  reset();
}

void ObIMicroBlockFlatReader::reset()
{
  header_ = nullptr;
  data_begin_ = nullptr;
  data_end_ = nullptr;
  index_data_ = nullptr;
  flat_row_reader_.reset();
}

int ObIMicroBlockFlatReader::find_bound_(
    const ObDatumRowkey &key,
    const bool lower_bound,
    const int64_t begin_idx,
    const int64_t end_idx,
    const ObStorageDatumUtils &datum_utils,
    int64_t &row_idx,
    bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (OB_UNLIKELY(nullptr == data_begin_ || nullptr == index_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ptr", K(ret), K(data_begin_), K(index_data_));
  } else {
    PreciseCompare<ObRowReader> flat_compare(
        ret,
        equal,
        &flat_row_reader_,
        data_begin_,
        index_data_,
        &datum_utils);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(end_idx);
    ObRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, flat_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, flat_compare);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret), K(key), K(lower_bound), K(datum_utils));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

int ObIMicroBlockFlatReader::init(const ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(!block_data.is_valid())){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data));
  } else {
    const char *buf = block_data.get_buf();
    header_ = reinterpret_cast<const ObMicroBlockHeader*>(buf);
    data_begin_ = buf + header_->header_size_;
    data_end_ = buf + header_->row_index_offset_;
    index_data_ = reinterpret_cast<const int32_t *>(buf + header_->row_index_offset_);
  }
  return ret;
}

/*
 * ObMicroBlockGetReader
 * */
int ObMicroBlockGetReader::inner_init(
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info,
    const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIMicroBlockFlatReader::init(block_data))) {
    LOG_WARN("failed to init reader", K(ret), K(block_data), K(read_info));
  } else {
    row_count_ = header_->row_count_;
    read_info_ = &read_info;
    if (OB_FAIL(ObIMicroBlockGetReader::init_hash_index(block_data, hash_index_, header_))) {
      LOG_WARN("failed to init micro block hash index", K(ret), K(rowkey), K(block_data), K(read_info));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMicroBlockGetReader::get_row(
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  int64_t row_idx;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid columns info ", K(ret), K(read_info));
  } else if (OB_FAIL(inner_init(block_data, read_info, rowkey))) {
    LOG_WARN("fail to inner init ", K(ret), K(block_data));
  } else if (OB_FAIL(locate_rowkey(rowkey, row_idx))){
    if (OB_BEYOND_THE_RANGE != ret) {
      LOG_WARN("failed to locate row, ", K(ret), K(rowkey));
    }
  } else if (OB_FAIL(flat_row_reader_.read_row(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              &read_info,
              row))) {
    LOG_WARN("Fail to read row, ", K(ret), K(rowkey));
  } else {
    row.fast_filter_skipped_ = false;
  }
  return ret;
}

int ObMicroBlockGetReader::exist_row(
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    bool &exist,
    bool &found)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(block_data, read_info, rowkey))) {
    LOG_WARN("failed to inner init", K(ret), K(block_data));
  } else {
    int64_t row_idx;
    exist = false;
    found = false;
    if (OB_FAIL(locate_rowkey(rowkey, row_idx))){
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("failed to locate row, ", K(ret), K(rowkey));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      const ObRowHeader *row_header =
          reinterpret_cast<const ObRowHeader*>(data_begin_ + index_data_[row_idx]);
      exist = !row_header->get_row_flag().is_delete();
      found = true;
    }
  }
  return ret;
}

int ObMicroBlockGetReader::locate_rowkey(const ObDatumRowkey &rowkey, int64_t &row_idx)
{
  int ret = OB_SUCCESS;
  bool need_binary_search = false;
  bool found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(locate_rowkey_fast_path(rowkey, row_idx, need_binary_search, found))) {
    LOG_WARN("faile to locate rowkey by hash index", K(ret));
  } else if (need_binary_search) {
    bool is_equal = false;
    if (OB_FAIL(ObIMicroBlockFlatReader::find_bound_(rowkey, true/*lower_bound*/, 0, row_count_,
        read_info_->get_datum_utils(), row_idx, is_equal))) {
      LOG_WARN("fail to lower_bound rowkey", K(ret));
    } else if (row_count_ == row_idx || !is_equal) {
      row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      ret = OB_BEYOND_THE_RANGE;
    } else {
      const ObRowHeader *row_header =
          reinterpret_cast<const ObRowHeader*>(data_begin_ + index_data_[row_idx]);
      if (row_header->get_row_multi_version_flag().is_ghost_row()) {
        row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
        ret = OB_BEYOND_THE_RANGE;
      }
    }
  } else if (!found) {
    ret = OB_BEYOND_THE_RANGE;
  }
  return ret;
}

int ObMicroBlockGetReader::locate_rowkey_fast_path(const ObDatumRowkey &rowkey,
                                                   int64_t &row_idx,
                                                   bool &need_binary_search,
                                                   bool &found)
{
  int ret = OB_SUCCESS;
  need_binary_search = false;
  if (hash_index_.is_inited()) {
    uint64_t hash_value = 0;
    const blocksstable::ObStorageDatumUtils &datum_utils = read_info_->get_datum_utils();
    if (OB_FAIL(rowkey.murmurhash(0, datum_utils, hash_value))) {
      LOG_WARN("Failed to calc rowkey hash", K(ret), K(rowkey), K(datum_utils));
    } else  {
      const uint8_t tmp_row_idx = hash_index_.find(hash_value);
      if (tmp_row_idx == ObMicroBlockHashIndex::NO_ENTRY) {
        row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
        found = false;
      } else if (tmp_row_idx == ObMicroBlockHashIndex::COLLISION) {
        need_binary_search = true;
      } else {
        int32_t compare_result = 0;
        if (OB_UNLIKELY(tmp_row_idx >= row_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected row_idx", K(ret), K(tmp_row_idx), K(row_count_), K(rowkey), KPC_(read_info));
        } else if (OB_FAIL(flat_row_reader_.compare_meta_rowkey(
                      rowkey,
                      read_info_->get_datum_utils(),
                      data_begin_ + index_data_[tmp_row_idx],
                      index_data_[tmp_row_idx + 1] - index_data_[tmp_row_idx],
                      compare_result))) {
          LOG_WARN("fail to compare rowkey", K(ret), K(rowkey), KPC_(read_info));
        } else if (0 != compare_result) {
          row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
          found = false;
        } else {
          row_idx = tmp_row_idx;
          found = true;
        }
      }
    }
  } else {
    need_binary_search = true;
  }
  return ret;
}

/***************             ObMicroBlockReader              ****************/
void ObMicroBlockReader::reset()
{
  ObIMicroBlockFlatReader::reset();
  ObIMicroBlockReader::reset();
  header_ = NULL;
  data_begin_ = NULL;
  data_end_ = NULL;
  index_data_ = NULL;
  flat_row_reader_.reset();
  allocator_.reuse();
}

int ObMicroBlockReader::init(
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    reset();
  }
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("columns info is invalid", K(ret), K(read_info));
  } else if (OB_FAIL(ObIMicroBlockFlatReader::init(block_data))) {
    LOG_WARN("fail to init, ", K(ret));
  } else {
    row_count_ = header_->row_count_;
    read_info_ = &read_info;
    datum_utils_ = &(read_info.get_datum_utils());
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObMicroBlockReader::init(
    const ObMicroBlockData &block_data,
	const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    reset();
  }
  if (OB_FAIL(ObIMicroBlockFlatReader::init(block_data))) {
    LOG_WARN("fail to init, ", K(ret));
  } else {
    row_count_ = header_->row_count_;
    read_info_ = nullptr;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}


int ObMicroBlockReader::find_bound(
    const ObDatumRowkey &key,
    const bool lower_bound,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < 0 || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(begin_idx), K(row_count_),
             KP_(data_begin), KP_(index_data), KP_(datum_utils));
  } else if (OB_FAIL(ObIMicroBlockFlatReader::find_bound_(
          key,
          lower_bound,
          begin_idx,
          row_count_,
          *datum_utils_,
          row_idx,
          equal))) {
    LOG_WARN("failed to find bound", K(ret), K(lower_bound), K(begin_idx), K_(row_count), KPC_(datum_utils));
  }
  return ret;
}

int ObMicroBlockReader::find_bound(
    const ObDatumRange &range,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal,
    int64_t &end_key_begin_idx,
    int64_t &end_key_end_idx)
{
  UNUSEDx(end_key_begin_idx, end_key_end_idx);
  return find_bound(range.get_start_key(), true, begin_idx, row_idx, equal);
}

int ObMicroBlockReader::get_row(const int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if(IS_NOT_INIT){
    ret = OB_NOT_INIT;
    LOG_WARN("should init reader first, ", K(ret));
  } else if(OB_UNLIKELY(nullptr == header_ ||
                        index < 0 || index >= header_->row_count_ ||
                        !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K(row), KPC_(header));
  } else if (OB_FAIL(flat_row_reader_.read_row(
              data_begin_ + index_data_[index],
              index_data_[index + 1] - index_data_[index],
              read_info_,
              row))) {
    LOG_WARN("row reader read row failed", K(ret), K(index), K(index_data_[index + 1]),
             K(index_data_[index]), KPC_(header), KPC_(read_info));
  } else {
    row.fast_filter_skipped_ = false;
    LOG_DEBUG("row reader read row success", K(ret), KPC_(read_info), K(index), K(index_data_[index + 1]),
            K(index_data_[index]), K(row));
  }
  return ret;
}

int ObMicroBlockReader::get_row_header(
    const int64_t row_idx,
    const ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("reader not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == header_ || row_idx >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Id is NULL", K(ret), K(row_idx), KPC_(header));
  } else if (OB_FAIL(flat_row_reader_.read_row_header(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              row_header))) {
    LOG_WARN("failed to setup row", K(ret), K(row_idx));
  }
  return ret;
}

int ObMicroBlockReader::get_row_count(int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    row_count = header_->row_count_;
  }
  return ret;
}

// notice, trans_version of ghost row is min(0)
int ObMicroBlockReader::get_multi_version_info(
    const int64_t row_idx,
    const int64_t schema_rowkey_cnt,
    const ObRowHeader *&row_header,
    int64_t &trans_version,
    int64_t &sql_sequence)
{
  int ret = OB_SUCCESS;
  row_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == header_ ||
                         row_idx < 0 || row_idx > row_count_ ||
                         0 > schema_rowkey_cnt ||
                         header_->column_count_ < schema_rowkey_cnt + 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_idx), K_(row_count), K(schema_rowkey_cnt),
             KPC_(header), K(lbt()));
  } else if (OB_FAIL(flat_row_reader_.read_row_header(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              row_header))) {
    LOG_WARN("fail to setup row", K(ret), K(row_idx), K(index_data_[row_idx + 1]),
             K(index_data_[row_idx]), KP(data_begin_));
  } else {
    ObStorageDatum datum;
    const int64_t read_col_idx =
      row_header->get_row_multi_version_flag().is_uncommitted_row()
      ? schema_rowkey_cnt + 1 : schema_rowkey_cnt;
    if (OB_FAIL(flat_row_reader_.read_column(
                data_begin_ + index_data_[row_idx],
                index_data_[row_idx + 1] - index_data_[row_idx],
                read_col_idx,
                datum))) {
      LOG_WARN("fail to read column", K(ret), K(read_col_idx));
    } else {
      if (!row_header->get_row_multi_version_flag().is_uncommitted_row()) {
        // get trans_version for committed row
        sql_sequence = 0;
        trans_version = row_header->get_row_multi_version_flag().is_ghost_row() ? 0 : -datum.get_int();
      } else {
        // get sql_sequence for uncommitted row
        trans_version = INT64_MAX;
        sql_sequence = -datum.get_int();
      }
    }
  }

  return ret;
}

int ObMicroBlockReader::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter,
    const storage::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  ObStorageDatum *col_buf = pd_filter_info.datum_buf_;
  const int64_t col_capacity = pd_filter_info.col_capacity_;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 || pd_filter_info.end_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.end_));
  } else if (OB_FAIL(validate_filter_info(filter, col_buf, col_capacity, header_))) {
    LOG_WARN("Failed to validate filter info", K(ret));
  } else {
    int64_t col_count = filter.get_col_count();
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets();
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    const common::ObIArray<ObStorageDatum> &default_datums = filter.get_default_datums();
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    const ObColDescIArray &cols_desc = read_info_->get_columns_desc();
    for (int64_t row_idx = pd_filter_info.start_; OB_SUCC(ret) && row_idx < pd_filter_info.end_; ++row_idx) {
      if (nullptr != parent && parent->can_skip_filter(row_idx)) {
        continue;
      } else if (0 < col_count) {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
          ObStorageDatum &datum = col_buf[i];
          int64_t col_idx = cols_index.at(col_offsets.at(i));
          if (OB_FAIL(flat_row_reader_.read_column(
              data_begin_ + index_data_[row_idx],
              index_data_[row_idx + 1] - index_data_[row_idx],
              col_idx,
              datum))) {
            LOG_WARN("fail to read column", K(ret), K(i), K(col_idx), K(row_idx), KPC_(header));
          } else if (datum.is_nop_value()) {
            if (OB_LIKELY(!default_datums.at(i).is_nop())) {
              datum = default_datums.at(i);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected nop value", K(ret), K(datum), K(col_idx), K(row_idx),
                      KPC(reinterpret_cast<const ObRowHeader *>(data_begin_ + index_data_[row_idx])));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (nullptr != col_params.at(i) && !datum.is_null() &&
                     OB_FAIL(storage::pad_column(
                             col_params.at(i)->get_meta_type(),
                             col_params.at(i)->get_accuracy(),
                             allocator_,
                             datum))) {
            LOG_WARN("Failed to pad column", K(ret), K(i), K(col_idx), K(row_idx), K(datum));
          }
        }
      }

      bool filtered = false;
      if (OB_SUCC(ret)) {
        if (filter.is_filter_black_node()) {
          sql::ObBlackFilterExecutor &black_filter = static_cast<sql::ObBlackFilterExecutor &>(filter);
          if (OB_FAIL(black_filter.filter(col_buf, col_count, filtered))) {
            LOG_WARN("Failed to filter row with black filter", K(ret), K(row_idx));
          }
        } else {
          sql::ObWhiteFilterExecutor &white_filter = static_cast<sql::ObWhiteFilterExecutor &>(filter);
          common::ObObj obj;
          if (1 != filter.get_col_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected col_ids count: not 1", K(ret), K(filter));
          } else if (OB_FAIL(col_buf[0].to_obj_enhance(obj, cols_desc.at(col_offsets.at(0)).col_type_))) {
            LOG_WARN("Failed to obj", K(ret), K(row_idx), K(col_buf[0]));
          } else if (OB_FAIL(filter_white_filter(white_filter, obj, filtered))) {
            LOG_WARN("Failed to filter row with white filter", K(ret), K(row_idx));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!filtered) {
          if (OB_FAIL(result_bitmap.set(row_idx))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_idx));
          }
        }
      }
    }
    LOG_TRACE("[PUSHDOWN] micro block pushdown filter row", K(ret), K(col_params),
              K(col_offsets), K(result_bitmap.popcnt()), K(result_bitmap.size()));
  }
  return ret;
}

int ObMicroBlockReader::get_rows(
    const common::ObIArray<int32_t> &cols_projector,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const common::ObIArray<ObObjDatumMapType> &map_types,
    const blocksstable::ObDatumRow &default_row,
    const int64_t *row_ids,
    const int64_t row_cap,
    ObDatumRow &row_buf,
    common::ObIArray<ObDatum *> &datums,
    sql::ExprFixedArray &exprs,
    sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  int64_t row_idx = common::OB_INVALID_INDEX;
  allocator_.reuse();
  if (OB_UNLIKELY(nullptr == header_ ||
                  nullptr == read_info_ ||
                  row_cap > header_->row_count_ ||
                  !row_buf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(header_), KPC(read_info_), K(row_cap), K(row_buf));
  } else if (OB_FAIL(row_buf.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Failed to reserve row buf", K(ret), K(row_buf), KPC(read_info_));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < row_cap; ++idx) {
      row_idx = row_ids[idx];
      if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uexpected row idx", K(ret), K(row_idx), KPC(header_));
      } else if (OB_FAIL(flat_row_reader_.read_row(
          data_begin_ + index_data_[row_idx],
          index_data_[row_idx + 1] - index_data_[row_idx],
          read_info_,
          row_buf))) {
        LOG_WARN("Fail to read row", K(ret), K(idx), K(row_idx), K(row_cap), KPC_(header));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cols_projector.count(); ++i) {
          common::ObDatum &datum = datums.at(i)[idx];
          int32_t col_idx = cols_projector.at(i);
          if (col_idx >= read_info_->get_request_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected col idx", K(ret), K(i), K(col_idx), K(read_info_->get_request_count()));
          } else if (row_buf.storage_datums_[col_idx].is_null()) {
            datum.set_null();
          } else if (row_buf.storage_datums_[col_idx].is_nop()) {
            if (default_row.storage_datums_[i].is_nop()) {
              // virtual columns will be calculated in sql
            } else if (OB_FAIL(datum.from_storage_datum(default_row.storage_datums_[i], map_types.at(i)))) {
              // fill columns added
              LOG_WARN("Fail to transfer datum", K(ret), K(i), K(idx), K(row_idx), K(default_row));
            }
            LOG_TRACE("Transfer nop value", K(ret), K(idx), K(row_idx), K(col_idx), K(default_row));
          } else {
            bool need_copy = false;
            if (row_buf.storage_datums_[col_idx].need_copy_for_encoding_column_with_flat_format(map_types.at(i))) {
              need_copy = true;
              datum.ptr_ = exprs[i]->get_str_res_mem(eval_ctx, row_buf.storage_datums_[col_idx].len_, idx);
            }
            if (OB_FAIL(datum.from_storage_datum(row_buf.storage_datums_[col_idx], map_types.at(i), need_copy))) {
              LOG_WARN("Failed to from storage datum", K(ret), K(idx), K(row_idx), K(col_idx), K(need_copy),
                        K(row_buf.storage_datums_[col_idx]), KPC_(header));
            }
         }
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < cols_projector.count(); ++i) {
        if (nullptr != col_params.at(i)) {
          if (OB_FAIL(storage::pad_on_datums(
              col_params.at(i)->get_accuracy(),
              col_params.at(i)->get_meta_type().get_collation_type(),
              allocator_,
              row_cap,
              datums.at(i)))) {
            LOG_WARN("fail to pad on datums", K(ret), K(i), K(row_cap), KPC_(header));
          }
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockReader::get_row_count(
    int32_t col,
    const int64_t *row_ids,
    const int64_t row_cap,
    const bool contains_null,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ ||
                  nullptr == read_info_ ||
                  row_cap > header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(header_), KPC_(read_info), K(row_cap), K(col));
  } else if (contains_null) {
    count = row_cap;
  } else {
    count = 0;
    int64_t row_idx = common::OB_INVALID_INDEX;
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    int64_t col_idx = cols_index.at(col);
    ObStorageDatum datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
      row_idx = row_ids[i];
      if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uexpected row idx", K(ret), K(row_idx), KPC(header_));
      } else if (OB_FAIL(flat_row_reader_.read_column(
          data_begin_ + index_data_[row_idx],
          index_data_[row_idx + 1] - index_data_[row_idx],
          col_idx,
          datum))) {
        LOG_WARN("fail to read column", K(ret), K(i), K(col_idx), K(row_idx));
      } else if (!datum.is_null()) {
        ++count;
      }
    }
  }
  return ret;
}

int ObMicroBlockReader::get_min_or_max(
    int32_t col,
    const share::schema::ObColumnParam *col_param,
    const int64_t *row_ids,
    const int64_t row_cap,
    ObMicroBlockAggInfo<ObStorageDatum> &agg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ ||
                  nullptr == read_info_ ||
                  row_cap > header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(header_), KPC_(read_info), K(row_cap), K(col));
  } else {
    int64_t row_idx = common::OB_INVALID_INDEX;
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    int64_t col_idx = cols_index.at(col);
    ObStorageDatum datum;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
      row_idx = row_ids[i];
      if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uexpected row idx", K(ret), K(row_idx), KPC(header_));
      } else if (OB_FAIL(flat_row_reader_.read_column(
          data_begin_ + index_data_[row_idx],
          index_data_[row_idx + 1] - index_data_[row_idx],
          col_idx,
          datum))) {
        LOG_WARN("fail to read column", K(ret), K(i), K(col_idx), K(row_idx));
      } else if (datum.is_nop()) {
        if (OB_UNLIKELY(nullptr == col_param || col_param->get_orig_default_value().is_nop_value())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected datum, can not process in batch", K(ret), K(col), KPC(col_param));
        } else if (OB_FAIL(datum.from_obj_enhance(col_param->get_orig_default_value()))) {
          STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(agg_info.update_min_or_max(datum))) {
        LOG_WARN("fail to update_min_or_max", K(ret), K(i), K(row_idx), K(datum), K(agg_info));
      } else {
        LOG_DEBUG("update min/max", K(i), K(row_idx), K(datum), K(agg_info));
      }
    }
  }
  return ret;
}

int ObMicroBlockReader::get_aggregate_result(
    const int64_t *row_ids,
    const int64_t row_cap,
    ObDatumRow &row_buf,
    common::ObIArray<storage::ObAggCell*> &agg_cells)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ ||
                  nullptr == read_info_ ||
                  row_cap > header_->row_count_ ||
                  !row_buf.is_valid()) ||
                  agg_cells.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KPC(header_), KPC(read_info_), K(row_cap), K(row_buf));
  } else if (OB_FAIL(row_buf.reserve(read_info_->get_request_count()))) {
    LOG_WARN("Failed to reserve row buf", K(ret), K(row_buf), KPC(read_info_));
  } else {
    int64_t row_idx = common::OB_INVALID_INDEX;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < row_cap; ++idx) {
      row_idx = row_ids[idx];
      if (OB_UNLIKELY(row_idx < 0 || row_idx >= header_->row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Uexpected row idx", K(ret), K(row_idx), KPC(header_));
      } else if (OB_FAIL(flat_row_reader_.read_row(
          data_begin_ + index_data_[row_idx],
          index_data_[row_idx + 1] - index_data_[row_idx],
          read_info_,
          row_buf))) {
        LOG_WARN("Fail to read row", K(ret), K(idx), K(row_idx), K(row_cap), KPC_(header));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < agg_cells.count(); ++i) {
          int32_t col_idx = agg_cells.at(i)->get_col_idx();
          if (OB_FAIL(agg_cells.at(i)->process(row_buf))) {
            LOG_WARN("Failed to process agg cell", K(ret), K(row_buf));
          }
        }
      }
    }
  }
  return ret;
}

}
}
