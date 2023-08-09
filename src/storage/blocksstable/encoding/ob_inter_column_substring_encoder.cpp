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

#include "ob_inter_column_substring_encoder.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_force_print_log.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
const ObColumnHeader::Type ObInterColSubStrEncoder::type_;

ObInterColSubStrEncoder::ObInterColSubStrEncoder()
    : ref_col_idx_(-1), ref_ctx_(NULL), same_start_pos_(-1),
      fix_data_size_(-1), start_pos_byte_(0),
      val_len_byte_(0)
{
  start_pos_array_.set_attr(ObMemAttr(MTL_ID(), "IntColSubStrEnc"));
  exc_row_ids_.set_attr(ObMemAttr(MTL_ID(), "IntColSubStrEnc"));
}

ObInterColSubStrEncoder::~ObInterColSubStrEncoder()
{
}

void ObInterColSubStrEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  ref_col_idx_ = -1;
  ref_ctx_ = NULL;
  same_start_pos_ = -1;
  fix_data_size_ = -1;
  start_pos_byte_ = 0;
  val_len_byte_ = 0;
  start_pos_array_.reuse();
  exc_row_ids_.reuse();
  meta_writer_.reset();
}

int ObInterColSubStrEncoder::init(
    const ObColumnEncodingCtx &ctx,
    const int64_t column_index,
    const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_index, rows))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx), K(column_index), "row_count", rows.count());
  } else if (OB_FAIL(start_pos_array_.reserve(rows.count()))) {
    LOG_WARN("failed to reserve start position array", K(ret), "count", rows.count());
  } else if (OB_FAIL(exc_row_ids_.reserve(rows.count()))) {
    LOG_WARN("failed to reserve exception row id array", K(ret), "count", rows.count());
  } else {
    const ObObjTypeStoreClass sc = get_store_class_map()[
        ob_obj_type_class(column_type_.get_type())];
    if (OB_UNLIKELY(!is_string_encoding_valid(sc))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type for inter column substring", K(ret), K(sc), K_(column_index));
    } else {
      column_header_.type_ = type_;
    }
  }
  return ret;
}

int ObInterColSubStrEncoder::set_ref_col_idx(const int64_t ref_col_idx,
    const ObColumnEncodingCtx &ref_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 > ref_col_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ref_col_idx));
  } else {
    const ObObjMeta &ref_col_type = ctx_->encoding_ctx_->col_descs_->at(ref_col_idx).col_type_;
    const ObObjTypeStoreClass ref_sc = get_store_class_map()[
        ob_obj_type_class(ref_col_type.get_type())];
    if (OB_UNLIKELY(!is_string_encoding_valid(ref_sc))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type for inter column substring", K(ret), K(ref_sc), K(ref_col_idx));
    } else {
      ref_col_idx_ = ref_col_idx;
      ref_ctx_ = &ref_ctx;
    }
  }
  return ret;
}

int64_t ObInterColSubStrEncoder::is_substring(const ObDatum &cell, const ObDatum &refed_cell)
{
  char *found = NULL;
  int64_t start_pos = 0;
  const bool is_cell_ext_store = cell.is_null() || cell.is_nop();
  const bool is_ref_ext_store = refed_cell.is_null() || refed_cell.is_nop();

  if (is_cell_ext_store) {
    if (is_ref_ext_store) {
      // equal, both are ext
      start_pos = EXT_START_POS;
    } else {
      // ext exception
      start_pos = EXCEPTION_START_POS;
    }
  } else if (is_ref_ext_store) {
    start_pos = EXCEPTION_START_POS;
  } else if (cell.len_ <= refed_cell.len_ && (nullptr != (found = static_cast<char *>(
      memmem(refed_cell.ptr_, refed_cell.len_, cell.ptr_, cell.len_))))) {
    // substring
    start_pos = found - refed_cell.ptr_;
  } else {
    // exception
    start_pos = EXCEPTION_START_POS;
  }

  LOG_DEBUG("start pos", K(start_pos), KP(found), K(cell.len_));
  return start_pos;
}

int ObInterColSubStrEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_type_ != ctx_->encoding_ctx_->col_descs_->at(ref_col_idx_).col_type_)) {
    suitable = false;
  } else {
    suitable = true;
    bool is_same_start_pos = true;
    same_start_pos_ = -1;
    // in exception meta, fix_data_cnt_ is uint8_t,
    // to avoid overflow, we have to limit the max excepction count
    const int64_t max_exc_cnt = std::min(MAX_EXC_CNT, rows_->count() * EXC_THRESHOLD_PCT / 100 + 1);
    int64_t start_pos = 0;
    int64_t max_start_pos = 0;
    int64_t max_val_len = 0;
    bool var_data = false;
    fix_data_size_ = -1;

    const int64_t avg_len = ctx_->fix_data_size_ > 0 ?
        ctx_->fix_data_size_ : ctx_->var_data_size_ / rows_->count();
    const int64_t ref_avg_len = ref_ctx_->fix_data_size_ > 0 ?
        ref_ctx_->fix_data_size_ : ref_ctx_->var_data_size_ / rows_->count();

    if (avg_len > ref_avg_len || avg_len <= sizeof(uint16_t) * 2) { // fast check
      suitable = false;
    } else {
      // get all exception row ids
      for (int64_t i = 0; OB_SUCC(ret) && i < rows_->count()
          && exc_row_ids_.count() <= max_exc_cnt; ++i) {
        const ObDatum &cell = ctx_->col_datums_->at(i);
        const ObDatum &refed_cell = ref_ctx_->col_datums_->at(i);
        max_val_len = (STORED_NOT_EXT == get_stored_ext_value(cell) && cell.len_ > max_val_len)
            ? cell.len_
            : max_val_len;

        if (EXCEPTION_START_POS == (start_pos = is_substring(cell, refed_cell))) {
          if (OB_FAIL(exc_row_ids_.push_back(i))) {
            LOG_WARN("failed to push back row id", K(ret), K(i));
          } else if (OB_FAIL(start_pos_array_.push_back(start_pos))) {
            LOG_WARN("failed to push back", K(ret), K(start_pos));
          }
        } else if (EXT_START_POS == start_pos) { // both are ext
          // calc same start pos should exclude extend value
          if (OB_FAIL(start_pos_array_.push_back(0))) {
            LOG_WARN("failed to push back start pos", K(ret));
          }
        } else {
          if (OB_FAIL(start_pos_array_.push_back(start_pos))) { // start pos
            LOG_WARN("failed to push back start pos", K(ret));
          } else {
            max_start_pos = start_pos > max_start_pos ? start_pos : max_start_pos;
            // whether all substrings are at the same start pos
            if (is_same_start_pos) {
              if (-1 == same_start_pos_) {
                same_start_pos_ = start_pos;
              } else if (!(is_same_start_pos = (same_start_pos_ == start_pos))) {
                same_start_pos_ = -1;
              }
            }
            // whether all substrings have the same length
            if (!var_data) {
              if (fix_data_size_ < 0) {
                fix_data_size_ = cell.len_;
              } else if (cell.len_ != fix_data_size_) {
                fix_data_size_ = -1;
                var_data = true;
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (max_start_pos >= UINT16_MAX || max_val_len >= UINT16_MAX) {
        suitable = false;
        FLOG_INFO("Too large ref sub string", K(max_start_pos), K(max_val_len));
      } else if (exc_row_ids_.count() > max_exc_cnt) {
        suitable = false;
      } else if (exc_row_ids_.count() > 0) {
        // init exception writer if has exc
        if (OB_FAIL(meta_writer_.init(&exc_row_ids_, ctx_->col_datums_, column_type_))) {
          LOG_WARN("init meta writer failed", K(ret), K_(exc_row_ids), K_(column_type));
        } else if (OB_FAIL(meta_writer_.traverse_exc(suitable))) {
          LOG_WARN("meta writer traverse failed", K(ret));
        }
      }
      // calc parameters for row store
      if (OB_SUCC(ret)) {
        if (0 < fix_data_size_ && 0 <= same_start_pos_) { // not need row store
          desc_.need_data_store_ = false;
        } else {
          desc_.need_data_store_ = true;
          desc_.fix_data_length_ = 0;
          if (0 > same_start_pos_) {
            start_pos_byte_ = max_start_pos <= UINT8_MAX ? 1 : 2;
          }
          if (0 > fix_data_size_) {
            val_len_byte_ = max_val_len <= UINT8_MAX ? 1 : 2;
          }
        }
        desc_.fix_data_length_ = start_pos_byte_ + val_len_byte_;
        desc_.need_extend_value_bit_store_ = false;
        desc_.is_var_data_ = false;
        desc_.has_null_ = 0 != ctx_->null_cnt_;
        desc_.has_nope_ = 0 != ctx_->nope_cnt_;
        column_header_.set_fix_lenght_attr();
      }
    }
  }
  return ret;
}

int64_t ObInterColSubStrEncoder::calc_size() const
{
  int64_t size = sizeof(ObInterColSubStrMetaHeader);
  if (0 < exc_row_ids_.count()) {
    size += meta_writer_.size();
  }
  if (0 > fix_data_size_) {
    size += rows_->count() * val_len_byte_;
  }
  if (0 > same_start_pos_) {
    size += rows_->count() * start_pos_byte_;
  }
  return size;
}

int ObInterColSubStrEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    char *buf = buf_writer.current();
    // calc meta size
    int64_t size = sizeof(ObInterColSubStrMetaHeader);
    if (0 < exc_row_ids_.count()) {
      size += meta_writer_.size();
    }

    // write meta
    ObInterColSubStrMetaHeader *meta_header = NULL;
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("failed to advance buf_writer", K(ret), K(size));
    } else {
      meta_header = reinterpret_cast<ObInterColSubStrMetaHeader *>(buf);
      meta_header->reset();
      meta_header->ref_col_idx_ = static_cast<uint16_t>(ref_col_idx_);
      if (0 <= same_start_pos_) {
        meta_header->start_pos_ = static_cast<uint16_t>(same_start_pos_);
        meta_header->set_same_start_pos();
      } else {
        meta_header->set_start_pos_byte(start_pos_byte_);
      }
      if (0 <= fix_data_size_) {
        meta_header->length_ = static_cast<uint16_t>(fix_data_size_);
        meta_header->set_fix_length();
      } else {
        meta_header->set_val_len_byte(val_len_byte_);
      }
    }

    if (OB_SUCC(ret) && 0 < exc_row_ids_.count()) {
      buf += sizeof(ObInterColSubStrMetaHeader);
      if (OB_FAIL(meta_writer_.write(buf))) {
        LOG_WARN("write meta failed", K(ret), KP(buf));
      }
    }
    LOG_DEBUG("meta_header", K(*meta_header));
  }
  return ret;
}

int ObInterColSubStrEncoder::get_row_checksum(int64_t &checksum) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    checksum = 0;
    FOREACH(r, *rows_) {
      checksum += r->get_datum(column_index_).checksum(0);
    }
  }
  return ret;
}

int64_t ObInterColSubStrEncoder::get_ref_col_idx() const
{
  return ref_col_idx_;
}

struct ObInterColSubStrEncoder::ColumnDataSetter
{
  struct {} INCLUDE_EXT_CELL[0]; // call filler with extend cells
  STATIC_ASSERT(HAS_MEMBER(ColumnDataSetter, INCLUDE_EXT_CELL),
      "InterColSubStr column data setter should be called for extend value cell");

  ColumnDataSetter(ObInterColSubStrEncoder &enc) : enc_(enc) {}

  inline int operator()(
      const int64_t row_id,
      const common::ObDatum &datum,
      char *buf,
      const int64_t) const
  {
    if (enc_.val_len_byte_ > 0) {
      if (enc_.start_pos_byte_ > 0) {
        int64_t start_pos = enc_.start_pos_array_.at(row_id);
        MEMCPY(buf, &start_pos, enc_.start_pos_byte_);
      }
      // ext value's value len could be any value, force set it to 0 to avoid checksum error
      int64_t val_len = STORED_NOT_EXT != get_stored_ext_value(datum) ? 0 : datum.len_;
      MEMCPY(buf + enc_.start_pos_byte_, &val_len, enc_.val_len_byte_);
    } else { // only fix data size
      int64_t start_pos = enc_.start_pos_array_.at(row_id);
      MEMCPY(buf, &start_pos, enc_.start_pos_byte_);
    }
    return OB_SUCCESS;
  }

  ObInterColSubStrEncoder &enc_;
};

int ObInterColSubStrEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_fix_encoder())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(desc));
  } else {
    EmptyGetter getter;
    ColumnDataSetter setter(*this);
    if (OB_FAIL(fill_column_store(buf_writer, *ctx_->col_datums_, getter, setter))) {
      LOG_WARN("fill inter column substr column store failed", K(ret));
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
