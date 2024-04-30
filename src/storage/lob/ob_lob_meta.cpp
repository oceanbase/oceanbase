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
#include "ob_lob_meta.h"
#include "lib/oblog/ob_log.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/lob/ob_lob_seq.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_write_buffer.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"
namespace oceanbase
{
namespace storage
{

int ObLobMetaScanIter::open(ObLobAccessParam &param, ObILobApator* lob_adatper)
{
  int ret = OB_SUCCESS;
  lob_adatper_ = lob_adatper;
  byte_size_ = param.byte_size_;
  offset_ = param.offset_;
  len_ = param.len_;
  coll_type_ = param.coll_type_;
  scan_backward_ = param.scan_backward_;
  allocator_ = param.allocator_;
  access_ctx_ = param.access_ctx_;
  cur_pos_ = 0;
  cur_byte_pos_ = 0;
  if (OB_FAIL(lob_adatper->scan_lob_meta(param, scan_param_, meta_iter_))) {
    LOG_WARN("failed to open iter", K(ret));
  }
  return ret;
}

ObLobMetaScanIter::ObLobMetaScanIter()
  : lob_adatper_(nullptr), meta_iter_(nullptr),
    byte_size_(0), offset_(0), len_(0), coll_type_(ObCollationType::CS_TYPE_INVALID), scan_backward_(false),
    allocator_(nullptr), access_ctx_(nullptr), scan_param_(), cur_pos_(0), cur_byte_pos_(0), not_calc_char_len_(false),
    not_need_last_info_(false) {}

int ObLobMetaScanIter::get_next_row(ObLobMetaInfo &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_iter_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("meta_iter is null.", K(ret));
  } else if (cur_byte_pos_ > byte_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan get lob meta byte len is bigger than byte size", K(ret), K(*this), K(byte_size_));
  } else if (cur_byte_pos_ == byte_size_) {
    ret = OB_ITER_END;
  } else {
    bool has_found = false;
    bool is_char = coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
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
        if (is_char && row.char_len_ == UINT32_MAX) {
          if (row.byte_len_ != byte_size_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected situation", K(ret), K(byte_size_), KPC(this), K(row));
          } else if (not_calc_char_len()) {
            LOG_DEBUG("not_calc_char_len", KPC(this), K(cur_info_));
          } else {
            // char len has not been calc, just calc char len here
            row.char_len_ = ObCharset::strlen_char(coll_type_, row.lob_data_.ptr(), row.lob_data_.length());
          }
        }
        if (is_range_over(row)) {
          // if row cur_pos > offset + len, need break;
          ret = OB_ITER_END;
        } else if (/*scan_backward_ ||*/ is_in_range(row)) {
          has_found = true;
        } else {
          // TODO
          // ret = OB_EAGAIN;
        }
        // update sum(len)
        cur_pos_ += (is_char) ? row.char_len_ : row.byte_len_;
        cur_byte_pos_ += row.byte_len_;
        if (OB_SUCC(ret) && ! not_need_last_info() && cur_byte_pos_ == byte_size_) {
          ObLobMetaInfo info = cur_info_;
          if (OB_FAIL(cur_info_.deep_copy(*allocator_, info))) {
            LOG_WARN("fail to do deep copy for cur info", K(ret), K(info), K(row), KPC(this));
          } else {
            LOG_DEBUG("deep_copy last info", K(cur_info_), KPC(this));
          }
        }
      }
    }
  }
  return ret;
}

int ObLobMetaScanIter::get_next_row(ObLobMetaScanResult &result)
{
  int ret = OB_SUCCESS;
  bool is_char = coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
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
      // if (!scan_backward_) {
        // 3种场景，meta_info与左右边界相交，或者被包含
        if (cur_pos < offset_) { // 越过左边界,
          if (cur_pos + cur_len < offset_) {
            ret = OB_ERR_INTERVAL_INVALID;
            LOG_WARN("Invalid query result at left edge.", K(ret), K(cur_pos), K(cur_len), K(offset_), K(len_));
          } else {
            if (!scan_backward_) {
              result.st_ = offset_ - cur_pos;
              result.len_ -= result.st_;
            } else {
              result.len_ = cur_pos_ - offset_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (cur_pos + cur_len > offset_ + len_) { // 越过右边界
            if (cur_pos > offset_ + len_) {
              ret = OB_ERR_INTERVAL_INVALID;
              LOG_WARN("Invalid query result at right edge.", K(ret), K(cur_pos), K(cur_len), K(offset_), K(len_));
            } else {
              if (!scan_backward_) {
                result.len_ = offset_ + len_ - cur_pos - result.st_;
              } else {
                result.st_ = cur_pos_ - (offset_ + len_);
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
  bool is_char = coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint64_t cur_end = cur_pos_ + ((is_char) ? info.char_len_ : info.byte_len_);
  uint64_t range_end = offset_ + len_;
  if (std::max(cur_pos_, offset_) < std::min(cur_end, range_end)) {
    bool_ret = true;
  }
  return bool_ret;
}

void ObLobMetaScanIter::reset()
{
  if (meta_iter_ != NULL && access_ctx_ == NULL) {
    meta_iter_->reset();
    if (lob_adatper_ != NULL) {
      (void)lob_adatper_->revert_scan_iter(meta_iter_);
    }
  }
  lob_adatper_ = nullptr;
  meta_iter_ = nullptr;
  access_ctx_ = nullptr;
  cur_pos_ = 0;
  cur_byte_pos_ = 0;
}

// called after 
bool ObLobMetaScanIter::is_range_begin(const ObLobMetaInfo& info)
{
  bool is_char = coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t meta_len = (is_char) ? info.char_len_ : info.byte_len_;
  uint64_t range_begin = offset_;
  return (range_begin < cur_pos_) && (cur_pos_ - meta_len <= range_begin);
}

bool ObLobMetaScanIter::is_range_end(const ObLobMetaInfo& info)
{
  bool is_char = coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t meta_len = (is_char) ? info.char_len_ : info.byte_len_;
  uint64_t range_end = offset_ + len_;
  return (cur_pos_ >= range_end) && (cur_pos_ - meta_len < range_end);
}

bool ObLobMetaScanIter::is_range_over(const ObLobMetaInfo& info)
{
  return cur_pos_ >= offset_ + len_;
}

ObLobMetaWriteIter::ObLobMetaWriteIter(
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
    iter_(nullptr),
    iter_fill_size_(0),
    is_store_char_len_(true)
{
  offset_ = 0;
  piece_id_ = 0;
  piece_block_size_ = piece_block_size;
}

int ObLobMetaWriteIter::get_last_meta_info(ObLobAccessParam &param, ObLobMetaManager* meta_manager)
{
  int ret = OB_SUCCESS;
  // do reverse scan
  bool save_is_reverse = param.scan_backward_;
  param.scan_backward_ = true;
  if (OB_ISNULL(meta_manager)) {
    LOG_DEBUG("not scan last info", K(ret), K(param));
  } else if (OB_FAIL(meta_manager->scan(param, scan_iter_))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param));
  } else {
    ObLobMetaScanResult scan_res;
    if (OB_FAIL(scan_iter_.get_next_row(scan_res))) {
      if (ret == OB_ITER_END) {
        LOG_WARN("no found lob_meta_info", K(ret), K(param), K(scan_iter_));
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        LOG_WARN("failed to get next row.", K(ret), K(param), K(scan_iter_));
      }
    } else {
      last_info_ = scan_res.info_;
      seq_id_.set_seq_id(last_info_.seq_id_);
    }
  }
  param.scan_backward_ = save_is_reverse;
  return ret;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             ObString &data,
                             uint64_t padding_size,
                             ObString &post_data,
                             ObString &remain_buf,
                             ObString &seq_id_st,
                             ObString &seq_id_end,
                             ObLobMetaManager* meta_manager)
{
  int ret = OB_SUCCESS;
  coll_type_ = param.coll_type_;
  lob_id_ = param.lob_data_->id_;
  piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
  padding_size_ = padding_size;
  seq_id_.set_seq_id(seq_id_st);
  seq_id_end_.set_seq_id(seq_id_end);
  post_data_ = post_data;
  data_ = data;
  remain_buf_ = remain_buf;
  is_store_char_len_ = param.is_store_char_len_;

  if (OB_FAIL(get_last_meta_info(param, meta_manager))) {
    LOG_WARN("get_last_meta_info fail", K(ret), K(param));
  } else if (0 == padding_size_ && post_data_.empty() && remain_buf_.empty() && last_info_.byte_len_ > 0) {
    // need concat data, so alloc buffer
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

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             void *iter, // ObLobQueryIter for data
                             ObString &read_buf, // ObLobQueryIter read buffer
                             uint64_t padding_size,
                             ObString &post_data,
                             ObString &remain_buf,
                             ObString &seq_id_st,
                             ObString &seq_id_end,
                             ObLobMetaManager* meta_manager)
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
    is_store_char_len_ = param.is_store_char_len_;
    data_.assign_buffer(read_buf.ptr(), piece_block_size_);

    char *buf = nullptr;
    if (OB_FAIL(get_last_meta_info(param, meta_manager))) {
      LOG_WARN("get_last_meta_info fail", K(ret), K(param));
    } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_->alloc(piece_block_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buffer failed.", K(piece_block_size_));
    } else {
      inner_buffer_.assign_ptr(buf, piece_block_size_);
    }
  }
  return ret;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             ObString &data,
                             ObLobMetaManager* meta_manager)
{
  ObString post_data;
  ObString remain_buf;
  ObString seq_id_st;
  ObString seq_id_end;
  return open(param, data, 0/*padding_size*/, post_data, remain_buf, seq_id_st, seq_id_end, meta_manager);
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param,
                             void *iter, // ObLobQueryIter
                             ObString &read_buf,
                             ObLobMetaManager* meta_manager)
{
  ObString post_data;
  ObString remain_buf;
  ObString seq_id_st;
  ObString seq_id_end;
  return open(param, iter, read_buf, 0/*padding_size*/, post_data, remain_buf, seq_id_st, seq_id_end, meta_manager);
}

int ObLobMetaWriteIter::try_fill_data(ObLobWriteBuffer &write_buffer, ObLobQueryIter *iter)
{
  int ret = OB_SUCCESS;
  int64_t by_len = 0;
  int64_t max_bytes_in_char = 16;

  // prepare read buffer
  ObString data;
  if (write_buffer.use_buffer()) {
    data.assign_buffer(data_.ptr(), write_buffer.remain());
  } else {
    data.assign_buffer(data_.ptr(), piece_block_size_);
  }

  // read data and prepare write buffer mode
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(iter->get_next_row(data))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next read buff", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (data.length() == 0) {
    LOG_INFO("[LOB] get empty result");
  } else if (write_buffer.use_buffer()) {
    // already use buffer
  } else if (data.length() + max_bytes_in_char >= piece_block_size_ || inner_buffer_.ptr() == nullptr) {
    // not use buffer
  } else {
    // use buffer because need concat data
    if (inner_buffer_.length() != piece_block_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_buffer_ length not equal piece_block_size", K(ret), K(piece_block_size_), "inner_buffer_length", inner_buffer_.length());
    } else if (OB_FAIL(write_buffer.set_buffer(inner_buffer_.ptr(), piece_block_size_))) {
      LOG_WARN("set_buffer fail", K(ret));
    }
  }

  // write data
  if (OB_FAIL(ret)) {
  } else if (iter->is_end()) {
  } else if (OB_FAIL(write_buffer.append(data.ptr(), data.length(), by_len))) {
    LOG_WARN("set buffer data fail", K(ret));
  }

  if (OB_SUCC(ret)) {
    offset_ += by_len;
    iter_fill_size_ += by_len;
  }
  return ret;
}

int ObLobMetaWriteIter::try_fill_data(
    ObLobWriteBuffer &write_buffer,
    ObString &data,
    uint64_t base,
    bool is_padding)
{
  int ret = OB_SUCCESS;
  uint64_t offset = offset_ - base;
  uint32_t left_size = is_padding ? (padding_size_ - offset) : (data.length() - offset);
  int64_t by_len = 0;

  // prepare buffer mode and max_write_byte_len
  if (write_buffer.use_buffer()) {
    // already use buffer
  } else if ((left_size >= piece_block_size_ && !is_padding) || inner_buffer_.ptr() == nullptr) {
    // not use buffer
  } else {
    // use buffer
    if (inner_buffer_.length() != piece_block_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_buffer_ length not equal piece_block_size", K(ret), K(piece_block_size_), "inner_buffer_length", inner_buffer_.length());
    } else if (OB_FAIL(write_buffer.set_buffer(inner_buffer_.ptr(), piece_block_size_))) {
      LOG_WARN("set_buffer fail", K(ret));
    }
  }

  // write data
  if (OB_FAIL(ret)) {
  } else if (is_padding) {
    // there is a trouble if charset is utf16
    if (OB_FAIL(write_buffer.padding(left_size, by_len))) {
      LOG_WARN("padding fail", K(ret), K(left_size));
    }
  } else if (OB_FAIL(write_buffer.append(data.ptr() + offset, left_size, by_len))) {
    LOG_WARN("set buffer data fail", K(ret), K(left_size));
  }

  if (OB_SUCC(ret)) {
    offset_ += by_len;
  }
  return ret;
}

int ObLobMetaWriteIter::try_update_last_info(
    ObLobMetaWriteResult &row,
    ObLobWriteBuffer&write_buffer)
{
  int ret = OB_SUCCESS;
  row.old_info_ = last_info_;
  row.is_update_ = false;
  bool is_fill_full = false;
  if (last_info_.seq_id_.ptr() != nullptr) {
    if (last_info_.byte_len_ > 0 && last_info_.byte_len_ < piece_block_size_) {
      int64_t by_len = 0;
      if (inner_buffer_.length() != piece_block_size_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner_buffer_ length not equal piece_block_size", K(ret), K(piece_block_size_), "inner_buffer_length", inner_buffer_.length());
      } else if (OB_FAIL(write_buffer.set_buffer(inner_buffer_.ptr(), piece_block_size_))) {
        LOG_WARN("set_buffer fail", K(ret));
      } else if (OB_FAIL(write_buffer.append(last_info_.lob_data_.ptr(), last_info_.lob_data_.length(), by_len))) {
        LOG_WARN("set buffer data fail", K(ret), K(last_info_), K(write_buffer));
      } else if (by_len != last_info_.lob_data_.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should write all data", K(ret), K(by_len), K(last_info_), K(write_buffer));
      } else {
        row.is_update_ = true;
        row.info_ = last_info_;
        // reset except lob_id, seq_id, piece_id
        row.info_.lob_data_.reset();
        row.info_.byte_len_ = 0;
        row.info_.char_len_ = 0;
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
  ObLobWriteBuffer write_buffer(coll_type_, piece_block_size_, is_store_char_len_);
  if (OB_FAIL(try_update_last_info(row, write_buffer))) {
    LOG_WARN("fail to do try update last info.", K(ret));
  } else if (!row.is_update_) {
    // 1. init common info for ObLobMetaWriteResult
    row.need_alloc_macro_id_ = false;
    row.info_.lob_id_ = lob_id_;
    row.info_.piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    row.info_.lob_data_.reset();
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
  }
  if (OB_SUCC(ret)) {
    // 2. fill data to ObLobMetaWriteResult
    //    in sequenced by [post_data][padding][data][remain_buf]
    while (OB_SUCC(ret) && ! write_buffer.is_full()) {
      if (iter_ == nullptr) {
        if (post_data_.length() > offset_) {
          ret = try_fill_data(write_buffer, post_data_, 0/*base*/, false/*is_padding**/);
        } else if (padding_size_ > offset_ - post_data_.length()) {
          ret = try_fill_data(write_buffer, post_data_, post_data_.length(), true/*is_padding**/);
        } else if (data_.length() > offset_ - post_data_.length() - padding_size_) {
          ret = try_fill_data(write_buffer, data_, post_data_.length() + padding_size_, false/*is_padding**/);
        } else if (remain_buf_.length() > offset_ - post_data_.length() - padding_size_ - data_.length()) {
          ret = try_fill_data(write_buffer, remain_buf_, post_data_.length() + padding_size_ + data_.length(), false/*is_padding**/);
        } else {
          ret = OB_ITER_END;
        }
      } else {
        ObLobQueryIter *iter = reinterpret_cast<ObLobQueryIter*>(iter_);
        if (post_data_.length() > offset_) {
          ret = try_fill_data(write_buffer, post_data_, 0/*base*/, false/*is_padding**/);
        } else if (padding_size_ > offset_ - post_data_.length()) {
          ret = try_fill_data(write_buffer, post_data_, post_data_.length(), true/*is_padding**/);
        } else if (!iter->is_end()) {
          ret = try_fill_data(write_buffer, iter);
        } else if (remain_buf_.length() > offset_ - post_data_.length() - padding_size_ - iter_fill_size_) {
          ret = try_fill_data(write_buffer, remain_buf_, post_data_.length() + padding_size_ + iter_fill_size_, false/*is_padding**/);
        } else {
          ret = OB_ITER_END;
        }
      }
    }
    if (OB_SUCC(ret) || (ret == OB_ITER_END && write_buffer.byte_length() > 0)) {
      if (OB_FAIL(write_buffer.to_lob_meta_info(row.info_))) {
        LOG_WARN("to_lob_meta_info fail", K(ret));
      } else if (row.info_.lob_data_.length() == 0
          || row.info_.lob_data_.length() > piece_block_size_
          || row.info_.lob_data_.length() != row.info_.byte_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result data over piece_block_size", K(ret), K(piece_block_size_), K(row.info_), K(row.is_update_));
      } else {
        row.data_ = row.info_.lob_data_;
        LOG_DEBUG("to_lob_meta_info", K(row.info_), K(write_buffer), K(offset_));
      }
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

int ObLobMetaManager::batch_insert(ObLobAccessParam& param, ObNewRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.write_lob_meta(param, iter))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

int ObLobMetaManager::batch_delete(ObLobAccessParam& param, ObNewRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.erase_lob_meta(param, iter))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// append
int ObLobMetaManager::append(ObLobAccessParam& param, ObLobMetaWriteIter& iter)
{
  int ret = OB_SUCCESS;
  UNUSED(param);
  UNUSED(iter);
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

int ObLobMetaManager::open(ObLobAccessParam &param, ObLobMetaSingleGetter* getter)
{
  int ret = OB_SUCCESS;
  ObILobApator *apator = &persistent_lob_adapter_;
  if (OB_FAIL(getter->open(param, apator))) {
    LOG_WARN("open lob scan iter failed.", K(ret), K(param));
  } else {
    getter->lob_adatper_ = apator;
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

ObLobMetaSingleGetter::~ObLobMetaSingleGetter()
{
  if (OB_NOT_NULL(row_objs_)) {
    param_->allocator_->free(row_objs_);
    row_objs_ = nullptr;
  }
  if (OB_NOT_NULL(scan_iter_)) {
    scan_iter_->reset();
    if (lob_adatper_ != NULL) {
      lob_adatper_->revert_scan_iter(scan_iter_);
    }
    scan_iter_ = nullptr;
  }
}

int ObLobMetaSingleGetter::open(ObLobAccessParam &param, ObILobApator* lob_adatper)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_FAIL(lob_adatper->prepare_single_get(param, scan_param_, table_id_))) {
    LOG_WARN("failed to open iter", K(ret));
  } else if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(ObObj) * 4))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc range obj failed.", K(ret));
  } else {
    row_objs_ = reinterpret_cast<ObObj*>(buf);
    param_ = &param;
  }
  return ret;
}

int ObLobMetaSingleGetter::get_next_row(ObString &seq_id, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  common::ObNewRowIterator *iter = nullptr;
  oceanbase::common::ObNewRow *row = nullptr;
  ObObj *row_objs = row_objs_;
  const char *lob_id_ptr = reinterpret_cast<char*>(&param_->lob_data_->id_);
  row_objs[0].reset();
  row_objs[0].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  row_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  row_objs[1].reset();
  row_objs[1].set_varchar(seq_id.ptr(), seq_id.length()); // lob_id
  row_objs[1].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey min_row_key(row_objs, 2);

  row_objs[2].reset();
  row_objs[2].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  row_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  row_objs[3].reset();
  row_objs[3].set_varchar(seq_id.ptr(), seq_id.length()); // lob_id
  row_objs[3].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey max_row_key(row_objs + 2, 2);

  common::ObNewRange range;
  range.table_id_ = table_id_;
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  scan_param_.key_ranges_.reset();

  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("failed to push key range.", K(ret), K(scan_param_), K(range));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret));
  } else if (OB_NOT_NULL(scan_iter_)) {
    scan_iter_->reuse();
    if (OB_FAIL(scan_iter_->rescan(scan_param_))) {
      LOG_WARN("rescan fali", K(ret), K(scan_param_));
    }
  } else if (OB_FAIL(oas->table_scan(scan_param_, iter))) {
    LOG_WARN("do table scan fali", K(ret), K(scan_param_));
  } else {
    scan_iter_ = static_cast<ObTableScanIterator*>(iter);
    iter = nullptr;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scan_iter_->get_next_row(row))) {
    LOG_WARN("get next row failed.", K(ret));
  } else if (OB_FAIL(ObLobMetaUtil::transform(row, info))) {
    LOG_WARN("failed to transform row.", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (iter != nullptr) {
      iter->reset();
      if (lob_adatper_ != nullptr) {
        lob_adatper_->revert_scan_iter(iter);
      }
      iter = nullptr;
      scan_iter_ = nullptr;
    }
  }
  return ret;
}

}
}
