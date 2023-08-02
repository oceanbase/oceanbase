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

#include "lib/oblog/ob_log.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "ob_lob_meta.h"
#include "ob_lob_seq.h"
#include "ob_lob_manager.h"

namespace oceanbase
{
namespace storage
{

int ObLobMetaScanIter::open(ObLobAccessParam &param, ObILobApator* lob_adatper)
{
  int ret = OB_SUCCESS;
  lob_adatper_ = lob_adatper;
  param_ = param;
  cur_pos_ = 0;
  cur_byte_pos_ = 0;
  if (OB_FAIL(lob_adatper->scan_lob_meta(param, scan_param_, meta_iter_))) {
    LOG_WARN("failed to open iter", K(ret));
  }
  return ret;
}

ObLobMetaScanIter::ObLobMetaScanIter()
  : lob_adatper_(nullptr), meta_iter_(nullptr), param_(), scan_param_(), cur_pos_(0), cur_byte_pos_(0) {}

int ObLobMetaScanIter::get_next_row(ObLobMetaInfo &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_iter_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("meta_iter is null.", K(ret));
  } else if (cur_byte_pos_ > param_.byte_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan get lob meta byte len is bigger than byte size", K(ret), K(*this), K(param_));
  } else if (cur_byte_pos_ == param_.byte_size_) {
    ret = OB_ITER_END;
  } else {
    bool has_found = false;
    bool is_char = param_.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
    while (OB_SUCC(ret) && !has_found) {
      common::ObNewRow* new_row = NULL;
      ret = meta_iter_->get_next_row(new_row);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
          row.byte_len_ = 0;
          row.char_len_ = 0;
          row.seq_id_ = ObString();
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else if(OB_ISNULL(new_row)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("row is null.", K(ret));
      } else if (OB_FAIL(ObLobMetaUtil::transform(new_row, row))) {
        LOG_WARN("get meta info from row failed.", K(ret), K(new_row));
      } else {
        cur_info_ = row;
        if (is_range_over(row)) {
          // if row cur_pos > offset + len, need break;
          ret = OB_ITER_END;
        } else if (/*param_.scan_backward_ ||*/ is_in_range(row)) {
          has_found = true;
        } else {
          // TODO
          // ret = OB_EAGAIN;
        }
        // update sum(len)
        cur_pos_ += (is_char) ? row.char_len_ : row.byte_len_;
        cur_byte_pos_ += row.byte_len_;
      }
    }
  }
  return ret;
}

int ObLobMetaScanIter::get_next_row(ObLobMetaScanResult &result)
{
  int ret = OB_SUCCESS;
  bool is_char = param_.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  ret = get_next_row(result.info_);
  if (ret == OB_ITER_END) {
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to get next row.", K(ret));
  } else {
    uint32_t cur_len = is_char ? result.info_.char_len_ : result.info_.byte_len_;
    if (cur_len > cur_pos_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cal cur pos.", K(ret), K(cur_pos_), K(cur_len));
    } else {
      uint64_t cur_pos = cur_pos_ - cur_len;
      result.st_ = 0;
      result.len_ = cur_len;
      // if scan backward, do full scan, not support range
      // if (!param_.scan_backward_) {
        // 3种场景，meta_info与左右边界相交，或者被包含
        if (cur_pos < param_.offset_) { // 越过左边界,
          if (cur_pos + cur_len < param_.offset_) {
            ret = OB_ERR_INTERVAL_INVALID;
            LOG_WARN("Invalid query result at left edge.", K(ret), K(cur_pos), K(cur_len), K(param_.offset_), K(param_.len_));
          } else {
            if (!param_.scan_backward_) {
              result.st_ = param_.offset_ - cur_pos;
              result.len_ -= result.st_;
            } else {
              result.len_ = cur_pos_ - param_.offset_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (cur_pos + cur_len > param_.offset_ + param_.len_) { // 越过右边界
            if (cur_pos > param_.offset_ + param_.len_) {
              ret = OB_ERR_INTERVAL_INVALID;
              LOG_WARN("Invalid query result at right edge.", K(ret), K(cur_pos), K(cur_len), K(param_.offset_), K(param_.len_));
            } else {
              if (!param_.scan_backward_) {
                result.len_ = param_.offset_ + param_.len_ - cur_pos - result.st_;
              } else {
                // []
                result.st_ = cur_pos_ - (param_.offset_ + param_.len_);
                result.len_ -= result.st_;
              }
            }

          }
        }
      // }
    }
  }
  return ret;
}

int ObLobMetaUtil::transform_lob_id(common::ObNewRow* row, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  ObString buf;
  if (OB_FAIL(row->cells_[ObLobMetaUtil::LOB_ID_COL_ID].get_varchar(buf))) {
    LOG_WARN("fail to get string from obj", K(ret), K(row->cells_[ObLobMetaUtil::LOB_ID_COL_ID]));
  } else if (buf.length() != sizeof(ObLobId)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to transform lob id.", K(ret), KPC(row));
  } else {
    info.lob_id_ = *reinterpret_cast<ObLobId*>(buf.ptr());
  }
  return ret;
}

int ObLobMetaUtil::transform_seq_id(common::ObNewRow* row, ObLobMetaInfo &info)
{
  return row->cells_[ObLobMetaUtil::SEQ_ID_COL_ID].get_varchar(info.seq_id_);
}

int ObLobMetaUtil::transform_byte_len(common::ObNewRow* row, ObLobMetaInfo &info)
{
  return row->cells_[ObLobMetaUtil::BYTE_LEN_COL_ID].get_uint32(info.byte_len_);
}

int ObLobMetaUtil::transform_char_len(common::ObNewRow* row, ObLobMetaInfo &info)
{
  return row->cells_[ObLobMetaUtil::CHAR_LEN_COL_ID].get_uint32(info.char_len_);
}

int ObLobMetaUtil::transform_piece_id(common::ObNewRow* row, ObLobMetaInfo &info)
{
  return row->cells_[ObLobMetaUtil::PIECE_ID_COL_ID].get_uint64(info.piece_id_);
}

int ObLobMetaUtil::transform_lob_data(common::ObNewRow* row, ObLobMetaInfo &info)
{
  return row->cells_[ObLobMetaUtil::LOB_DATA_COL_ID].get_varchar(info.lob_data_);
}

int ObLobMetaUtil::transform(common::ObNewRow* row, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null.", K(ret));
  } else if (!row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row));
  } else if (row->get_count() != LOB_META_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_lob_id(row, info))) {
    LOG_WARN("get lob id from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_seq_id(row, info))) {
    LOG_WARN("get seq id from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_byte_len(row, info))) {
    LOG_WARN("get byte len from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_char_len(row, info))) {
    LOG_WARN("get char len from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_piece_id(row, info))) {
    LOG_WARN("get macro id from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_lob_data(row, info))) {
    LOG_WARN("get macro id from row failed.", K(ret), KPC(row));
  }
  return ret;
}

bool ObLobMetaScanIter::is_in_range(const ObLobMetaInfo& info)
{
  bool bool_ret = false;
  bool is_char = param_.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint64_t cur_end = cur_pos_ + ((is_char) ? info.char_len_ : info.byte_len_);
  uint64_t range_end = param_.offset_ + param_.len_;
  if (std::max(cur_pos_, param_.offset_) < std::min(cur_end, range_end)) {
    bool_ret = true;
  }
  return bool_ret;
}

void ObLobMetaScanIter::reset()
{
  if (meta_iter_ != NULL) {
    meta_iter_->reset();
    if (lob_adatper_ != NULL) {
      (void)lob_adatper_->revert_scan_iter(meta_iter_);
    }
  }
  lob_adatper_ = nullptr;
  meta_iter_ = nullptr;
  cur_pos_ = 0;
  cur_byte_pos_ = 0;
}

// called after 
bool ObLobMetaScanIter::is_range_begin(const ObLobMetaInfo& info)
{
  bool is_char = param_.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t meta_len = (is_char) ? info.char_len_ : info.byte_len_;
  uint64_t range_begin = param_.offset_;
  return (range_begin < cur_pos_) && (cur_pos_ - meta_len <= range_begin);
}

bool ObLobMetaScanIter::is_range_end(const ObLobMetaInfo& info)
{
  bool is_char = param_.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t meta_len = (is_char) ? info.char_len_ : info.byte_len_;
  uint64_t range_end = param_.offset_ + param_.len_;
  return (cur_pos_ >= range_end) && (cur_pos_ - meta_len < range_end);
}

bool ObLobMetaScanIter::is_range_over(const ObLobMetaInfo& info)
{
  return cur_pos_ >= param_.offset_ + param_.len_;
}

ObLobMetaWriteIter::ObLobMetaWriteIter(
    const ObString& data, 
    ObIAllocator* allocator, 
    uint32_t piece_block_size
    ) : seq_id_(allocator),
    lob_id_(),
    coll_type_(CS_TYPE_BINARY),
    scan_iter_(),
    padding_size_(0),
    seq_id_end_(allocator),
    post_data_(),
    remain_buf_(),
    inner_buffer_(),
    allocator_(allocator),
    last_info_(),
    iter_(nullptr)
{
  data_ = data;
  offset_ = 0;
  piece_id_ = 0;
  piece_block_size_ = piece_block_size;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             uint64_t padding_size,
                             ObString &post_data,
                             ObString &remain_buf,
                             ObString &seq_id_st,
                             ObString &seq_id_end)
{
  int ret = OB_SUCCESS;
  coll_type_ = param.coll_type_;
  lob_id_ = param.lob_data_->id_;
  piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
  padding_size_ = padding_size;
  seq_id_.set_seq_id(seq_id_st);
  seq_id_end_.set_seq_id(seq_id_end);
  post_data_ = post_data;
  remain_buf_ = remain_buf;
  char *buf = reinterpret_cast<char*>(allocator_->alloc(piece_block_size_));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed.", K(piece_block_size_));
  } else {
    inner_buffer_.assign_ptr(buf, piece_block_size_);
  }
  return ret;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param, ObILobApator* adatper)
{
  int ret = OB_SUCCESS;
  bool is_empty_lob = (param.byte_size_ == 0);
  if (!is_empty_lob && OB_FAIL(scan_iter_.open(param, adatper))) {
    LOG_WARN("failed open scan meta open iter.", K(ret));
  } else {
    coll_type_ = param.coll_type_;
    lob_id_ = param.lob_data_->id_;
    piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    padding_size_ = 0;
    inner_buffer_.assign_ptr(nullptr, 0);
    
    // locate first piece
    if (!is_empty_lob) {
      ObLobMetaScanResult scan_res;
      ret = scan_iter_.get_next_row(scan_res);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
          // empty table
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else {
        last_info_ = scan_res.info_;
        seq_id_.set_seq_id(scan_res.info_.seq_id_);
      }
    }
  }
  return ret;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             void *iter, // ObLobQueryIter
                             ObString &read_buf,
                             uint64_t padding_size,
                             ObString &post_data,
                             ObString &remain_buf,
                             ObString &seq_id_st,
                             ObString &seq_id_end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null query iter", K(ret));
  } else {
    coll_type_ = param.coll_type_;
    lob_id_ = param.lob_data_->id_;
    piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    padding_size_ = padding_size;
    seq_id_.set_seq_id(seq_id_st);
    seq_id_end_.set_seq_id(seq_id_end);
    post_data_ = post_data;
    remain_buf_ = remain_buf;
    iter_ = iter;
    data_.assign_buffer(read_buf.ptr(), piece_block_size_);
    char *buf = reinterpret_cast<char*>(allocator_->alloc(piece_block_size_));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buffer failed.", K(piece_block_size_));
    } else {
      inner_buffer_.assign_ptr(buf, piece_block_size_);
    }
  }
  return ret;
}

int ObLobMetaWriteIter::try_fill_data(
    ObLobMetaWriteResult& row,
    bool &use_inner_buffer,
    bool &fill_full)
{
  int ret = OB_SUCCESS;
  ObLobQueryIter *iter = reinterpret_cast<ObLobQueryIter*>(iter_);
  int64_t by_len = 0;
  int64_t char_len = 0;
  int64_t max_bytes_in_char = 16;
  if (use_inner_buffer) {
    // prepare tmp read buffer
    ObString tmp_read_buf;
    tmp_read_buf.assign_buffer(data_.ptr(), row.data_.remain());
    if (OB_FAIL(iter->get_next_row(tmp_read_buf))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next read buff", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (tmp_read_buf.length() == 0) {
      LOG_DEBUG("[LOB] get empty result");
    } else {
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            tmp_read_buf.ptr(),
                                            tmp_read_buf.length(),
                                            tmp_read_buf.length(),
                                            char_len);
      by_len = ob_lob_writer_length_validation(coll_type_, tmp_read_buf.length(), by_len, char_len);
      if (row.data_.write(tmp_read_buf.ptr(), by_len) != by_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write data to inner buffer", K(ret), K(row.data_), K(by_len));
      } else {
        fill_full = (row.data_.remain() <= max_bytes_in_char);
      }
    }
  } else {
    ObString tmp_read_buf;
    tmp_read_buf.assign_buffer(data_.ptr(), piece_block_size_);
    if (OB_FAIL(iter->get_next_row(tmp_read_buf))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next read buff", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (tmp_read_buf.length() + max_bytes_in_char >= piece_block_size_ || inner_buffer_.ptr() == nullptr) {
      // calc len and assign ptr
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            tmp_read_buf.ptr(),
                                            tmp_read_buf.length(),
                                            tmp_read_buf.length(),
                                            char_len);
      by_len = ob_lob_writer_length_validation(coll_type_, tmp_read_buf.length(), by_len, char_len);
      row.data_.assign_ptr(tmp_read_buf.ptr(), by_len);
      fill_full = true;
    } else {
      // use inner buffer
      use_inner_buffer = true;
      row.data_.assign_buffer(inner_buffer_.ptr(), inner_buffer_.length());
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            tmp_read_buf.ptr(),
                                            tmp_read_buf.length(),
                                            tmp_read_buf.length(),
                                            char_len);
      by_len = ob_lob_writer_length_validation(coll_type_, tmp_read_buf.length(), by_len, char_len);
      if (row.data_.write(tmp_read_buf.ptr(), by_len) != by_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write data to inner buffer", K(ret), K(row.data_), K(by_len));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row.info_.byte_len_ += by_len;
    row.info_.char_len_ += char_len;
    OB_ASSERT(row.info_.byte_len_ >= row.info_.char_len_);
  }
  return ret;
}

int ObLobMetaWriteIter::try_fill_data(
    ObLobMetaWriteResult& row,
    ObString &data,
    uint64_t base,
    bool is_padding,
    bool &use_inner_buffer,
    bool &fill_full)
{
  int ret = OB_SUCCESS;
  uint64_t offset = offset_ - base;
  uint32_t left_size = is_padding ? (padding_size_ - offset) : (data.length() - offset);
  int64_t by_len = 0;
  int64_t char_len = 0;
  if (use_inner_buffer) {
    // try write data to buffer
    fill_full = (left_size >= row.data_.remain());
    by_len = (row.data_.remain() > left_size) ? left_size : row.data_.remain();
    if (is_padding) {
      char_len = by_len;
      if (coll_type_ == CS_TYPE_BINARY) {
        MEMSET(row.data_.ptr() + row.data_.length(), '0x00', by_len);
      } else {
        MEMSET(row.data_.ptr() + row.data_.length(), ' ', by_len);
      }
      row.data_.set_length(row.data_.length() + by_len);
    } else {
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            data.ptr() + offset,
                                            data.length() - offset,
                                            by_len,
                                            char_len);
      by_len = ob_lob_writer_length_validation(coll_type_, data.length() - offset, by_len, char_len);
      if (row.data_.write(data_.ptr() + offset, by_len) != by_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write data to inner buffer", K(ret), K(row.data_), K(by_len));
      }
    }
  } else {
    // check need use inner buffer
    if ((left_size >= piece_block_size_ && !is_padding) || inner_buffer_.ptr() == nullptr) {
      // calc len and assign ptr
      int64_t max_bytes = piece_block_size_;
      if (max_bytes > data.length() - offset) {
        max_bytes = (data.length() - offset);
      }
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            data.ptr() + offset,
                                            data.length() - offset,
                                            max_bytes,
                                            char_len);
      by_len = ob_lob_writer_length_validation(coll_type_, data.length() - offset, by_len, char_len);
      row.data_.assign_ptr(data_.ptr() + offset, by_len);
      fill_full = true;
    } else {
      // use inner buffer
      use_inner_buffer = true;
      row.data_.assign_buffer(inner_buffer_.ptr(), inner_buffer_.length());
      if (is_padding) {
        by_len = (row.data_.remain() > left_size) ? left_size : row.data_.remain();
        char_len = by_len;
        fill_full = (left_size >= row.data_.remain());
        if (coll_type_ == CS_TYPE_BINARY) {
          MEMSET(row.data_.ptr(), '0x00', by_len);
        } else {
          MEMSET(row.data_.ptr(), ' ', by_len);
        }
        row.data_.set_length(by_len);
      } else {
        by_len = ObCharset::max_bytes_charpos(coll_type_,
                                              data.ptr() + offset,
                                              data.length() - offset,
                                              left_size,
                                              char_len);
        by_len = ob_lob_writer_length_validation(coll_type_, data.length() - offset, by_len, char_len);
        if (row.data_.write(data.ptr() + offset, by_len) != by_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to write data to inner buffer", K(ret), K(row.data_), K(by_len));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row.info_.byte_len_ += by_len;
    row.info_.char_len_ += char_len;
    OB_ASSERT(row.info_.byte_len_ >= row.info_.char_len_);
    offset_ += by_len;
  }
  return ret;
}

int ObLobMetaWriteIter::try_update_last_info(ObLobMetaWriteResult &row)
{
  int ret = OB_SUCCESS;
  row.old_info_ = last_info_;
  row.is_update_ = false;
  bool is_fill_full = false;
  if (last_info_.seq_id_.ptr() != nullptr) {
    if (last_info_.byte_len_ < piece_block_size_) {
      int64_t by_len = piece_block_size_ - last_info_.byte_len_;
      int64_t char_len = 0;
      int64_t max_bytes = by_len > data_.length() ? data_.length() : by_len;
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            data_.ptr(),
                                            data_.length(),
                                            max_bytes,
                                            char_len);
      if (by_len > 0) {
        // prepare buffer
        ObString buffer;
        char *buf = reinterpret_cast<char*>(allocator_->alloc(piece_block_size_));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buffer failed.", K(piece_block_size_));
        } else {
          buffer.assign_ptr(buf, piece_block_size_);
          MEMCPY(buf, last_info_.lob_data_.ptr(), last_info_.lob_data_.length());
          MEMCPY(buf + last_info_.lob_data_.length(), data_.ptr(), by_len);
          row.data_.assign_ptr(buf, by_len + last_info_.lob_data_.length());
          row.info_ = last_info_;
          row.is_update_ = true;
          row.info_.byte_len_ += by_len;
          row.info_.char_len_ += char_len;
          offset_ += by_len;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // Reset last info, only try to update last info once
      last_info_.seq_id_.reset();
    }
  }
  return ret;
}

int ObLobMetaWriteIter::get_next_row(ObLobMetaWriteResult &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_update_last_info(row))) {
    LOG_WARN("fail to do try update last info.", K(ret));
  } else if (!row.is_update_) {
    // 1. init common info for ObLobMetaWriteResult
    row.need_alloc_macro_id_ = false;
    row.info_.lob_id_ = lob_id_;
    row.info_.piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    row.info_.lob_data_ = ObString();
    row.info_.byte_len_ = 0;
    row.info_.char_len_ = 0;
    ObString end_seq_id;
    seq_id_end_.get_seq_id(end_seq_id);
    if (OB_ISNULL(end_seq_id.ptr())) {
      // not set end
      if (OB_FAIL(seq_id_.get_next_seq_id(row.info_.seq_id_))) {
        LOG_WARN("failed to next seq id.", K(ret));
      }
    } else {
      // has set seq end
      if (OB_FAIL(seq_id_.get_next_seq_id(row.info_.seq_id_, seq_id_end_))) {
        LOG_WARN("failed to next seq id.", K(ret));
      }
    }

    // 2. fill data to ObLobMetaWriteResult
    //    in sequenced by [post_data][padding][data][remain_buf]
    bool use_inner_buffer = false;
    bool fill_full = false;
    while (OB_SUCC(ret) && !fill_full) {
      if (iter_ == nullptr) {
        if (post_data_.length() > offset_) {
          ret = try_fill_data(row, post_data_, 0, false, use_inner_buffer, fill_full);
        } else if (padding_size_ > offset_ - post_data_.length()) {
          ret = try_fill_data(row, post_data_, post_data_.length(), true, use_inner_buffer, fill_full);
        } else if (data_.length() > offset_ - post_data_.length() - padding_size_) {
          ret = try_fill_data(row, data_, post_data_.length() + padding_size_, false, use_inner_buffer, fill_full);
        } else if (remain_buf_.length() > offset_ - post_data_.length() - padding_size_ - data_.length()) {
          ret = try_fill_data(row, remain_buf_, post_data_.length() + padding_size_ + data_.length(), false, use_inner_buffer, fill_full);
        } else {
          ret = OB_ITER_END;
        }
      } else {
        ObLobQueryIter *iter = reinterpret_cast<ObLobQueryIter*>(iter_);
        if (post_data_.length() > offset_) {
          ret = try_fill_data(row, post_data_, 0, false, use_inner_buffer, fill_full);
        } else if (padding_size_ > offset_ - post_data_.length()) {
          ret = try_fill_data(row, post_data_, post_data_.length(), true, use_inner_buffer, fill_full);
        } else if (!iter->is_end()) {
          ret = try_fill_data(row, use_inner_buffer, fill_full);
        } else if (remain_buf_.length() > offset_ - post_data_.length() - padding_size_) {
          ret = try_fill_data(row, remain_buf_, post_data_.length() + padding_size_, false, use_inner_buffer, fill_full);
        } else {
          ret = OB_ITER_END;
        }
      }
    }
    if (ret == OB_ITER_END && row.info_.byte_len_ > 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobMetaWriteIter::close()
{
  int ret = OB_SUCCESS;
  scan_iter_.reset();
  seq_id_.reset();
  seq_id_end_.reset();
  if (inner_buffer_.ptr() != nullptr) {
    allocator_->free(inner_buffer_.ptr());
  }
  inner_buffer_.reset();
  return ret;
}

int ObLobMetaManager::write(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.write_lob_meta(param, in_row))) {
    LOG_WARN("write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// append
int ObLobMetaManager::append(ObLobAccessParam& param, ObLobMetaWriteIter& iter)
{
  int ret = OB_SUCCESS;
  param.scan_backward_ = true;
  
  if (OB_FAIL(iter.open(param, &persistent_lob_adapter_))) {
    LOG_WARN("fail open write iter.", K(ret), K(param));
  }
  return ret;
}

// generate LobMetaRow at specified range on demands
int ObLobMetaManager::insert(ObLobAccessParam& param, ObLobMetaWriteIter& iter)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  UNUSED(iter);
  return ret;
}
// rebuild specified range
int ObLobMetaManager::rebuild(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  return ret;
}
// get specified range LobMeta info
int ObLobMetaManager::scan(ObLobAccessParam& param, ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  // TODO check if per lob or tmp lob
  ObILobApator *apator = &persistent_lob_adapter_;
  if (OB_FAIL(iter.open(param, apator))) {
    LOG_WARN("open lob scan iter failed.", K(ret), K(param));
  }
  return ret;
}
// erase specified range
int ObLobMetaManager::erase(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.erase_lob_meta(param, in_row))) {
    LOG_WARN("erase lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// update specified range
int ObLobMetaManager::update(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.update_lob_meta(param, old_row, new_row))) {
    LOG_WARN("update lob meta failed.");
  }
  return ret;
}

int ObLobMetaManager::fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.fetch_lob_id(param, lob_id))) {
    LOG_WARN("fetch lob id failed.", K(ret), K(param));
  }
  return ret;
}

}
}
