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
#include "ob_lob_iterator.h"

namespace oceanbase
{
namespace storage
{
/*************ObLobQueryIter*****************/
int ObLobQueryIter::fill_buffer(ObString& buffer, const ObString &data, ObString &remain_data)
{
  int ret = OB_SUCCESS;
  int64_t actual_write_size = 0;
  int64_t write_char_len = 0;
  if (data.length() > 0) {
    if (data.length() > buffer.remain()) {
      // buffer is not enough to write all data
      if (is_reverse_) {
        if (cs_type_ != CS_TYPE_BINARY) {
          // calc remain data size that is not writed to buffer
          int64_t left_size = data.length() - buffer.remain();
          int64_t remain_size = ObCharset::max_bytes_charpos(cs_type_, data.ptr(), data.length(), left_size, write_char_len);
          // if remain_size is not equal left_size, means char is splited
          if (remain_size < left_size) {
            // means char is splited, so add one char size
            remain_size += ObCharset::charpos(cs_type_, data.ptr() + remain_size, data.length() - remain_size, 1);
          }
          actual_write_size = data.length() - remain_size;
        } else {
          actual_write_size = buffer.remain();
        }
      } else if (cs_type_ != CS_TYPE_BINARY) {
        // adjust size to make sure one char is not splited
        actual_write_size = ObCharset::max_bytes_charpos(cs_type_, data.ptr(), data.length(), buffer.remain(), write_char_len);
      } else {
        actual_write_size = buffer.remain();
      }
    } else {
      // do full write, we can regard with char or byte
      actual_write_size = data.length();
    }

    // write data to buffer
    if (OB_FAIL(ret)) {
    } else if (actual_write_size < 0 || actual_write_size > data.length() || actual_write_size > buffer.remain()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("actual_write_size invalid", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
    } else if (actual_write_size == 0) {
      remain_data.assign_ptr(data.ptr(), data.length());
      LOG_TRACE("no data to write", K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
          K(buffer.remain()), K(buffer.length()), K(buffer.size()));
    } else if (is_reverse_) {
      // write the last of data to the begin of buffer if is reverve
      if (actual_write_size != buffer.write_front(data.ptr() + data.length() - actual_write_size, actual_write_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("write front fail", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
            K(buffer.remain()), K(buffer.length()), K(buffer.size()));
      } else {
        LOG_DEBUG("write front success", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
            K(buffer.remain()), K(buffer.length()), K(buffer.size()));
        remain_data.assign_ptr(data.ptr(), data.length() - actual_write_size);
      }
    } else {
      if (actual_write_size != buffer.write(data.ptr(), actual_write_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("write data fail", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
            K(buffer.remain()), K(buffer.length()), K(buffer.size()));
      } else {
        LOG_DEBUG("write success", K(ret), K_(cs_type), K_(is_reverse), K(actual_write_size), K(data.length()),
            K(buffer.remain()), K(buffer.length()), K(buffer.size()));
        remain_data.assign_ptr(data.ptr() + actual_write_size, data.length() - actual_write_size);
      }
    }
  } else {
    LOG_DEBUG("no data to fill", K(data.length()), K(buffer.remain()), K(buffer.length()), K(buffer.size()));
  }
  return ret;
}


/*************ObLobInRowQueryIter*****************/

int ObLobInRowQueryIter::open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse)
{
  int ret = OB_SUCCESS;
  orgin_data_.assign_ptr(data.ptr() + byte_offset, byte_len);
  remain_data_ = orgin_data_;
  is_reverse_ = is_reverse;
  cs_type_ = cs;
  is_inited_ = true;
  return ret;
}

int ObLobInRowQueryIter::get_next_row(ObString& buffer)
{
  int ret = OB_SUCCESS;
  ObString new_remain_data;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else if (remain_data_.length() == 0) {
    ret = OB_ITER_END;
    LOG_DEBUG("not data to write", KPC(this));
  } else if (OB_FAIL(fill_buffer(buffer, remain_data_, new_remain_data))) {
    LOG_WARN("fill buffer fail", K(ret), KPC(this));
  } else {
    LOG_DEBUG("fill full", K(buffer.length()), K(buffer.size()), K(new_remain_data.length()), K(remain_data_.length()), KPC(this));
    remain_data_ = new_remain_data;
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);
  return ret;
}

void ObLobInRowQueryIter::reset()
{
  orgin_data_.reset();
  remain_data_.reset();
  is_reverse_ = false;
  is_end_ = false;
  is_inited_ = false;
}

/********** ObLobOutRowQueryIter ****************/

int ObLobOutRowQueryIter::open(ObLobAccessParam &param, ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lob_meta_mngr->scan(param, meta_iter_))) {
    LOG_WARN("fail to do lob query with retry", K(ret), K(param_));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("assign fail", K(ret), K(param));
  } else {
    is_reverse_ = param.scan_backward_;
    cs_type_ = param.coll_type_;
    is_inited_ = true;
  }
  return ret;
}


int ObLobOutRowQueryIter::get_next_row(ObString& buffer)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid.", K(ret));
  } else {
    bool has_fill_full = false;
    uint64_t st_len = buffer.length();
    ObString block_data;
    ObString remain_data;
    while (OB_SUCC(ret) && !has_fill_full) {
      // first try fill buffer remain data to output
      if (OB_FAIL(fill_buffer(buffer, last_data_, remain_data))) {
        LOG_WARN("fill buffer fail", K(ret) ,K(buffer.length()), K(buffer.size()), K(remain_data.length()), K(last_data_.length()));
      } else if (buffer.remain() == 0 || remain_data.length() > 0) {
        LOG_DEBUG("fill full", K(buffer.length()), K(buffer.size()), K(remain_data.length()), K(last_data_.length()));
        has_fill_full = true;
        last_data_ = remain_data;
      } else if (OB_FALSE_IT(last_data_.reset())) {
      } else if (OB_FAIL(meta_iter_.get_next_row(block_data))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("get next query result failed.", K(ret), K(param_));
        }
      } else {
        last_data_ = block_data;
      }
    }
    // if is OB_ITER_END, means no next row from meta_iter_ 
    // if data.length() is larger than st_len, that means last_data_ may be not empty
    // so can not return OB_ITER_END
    if (ret == OB_ITER_END && buffer.length() > st_len) {
      ret = OB_SUCCESS;
    }
  }
  is_end_ = is_end_ || (ret == OB_ITER_END);
  return ret;
}

void ObLobOutRowQueryIter::reset()
{
  meta_iter_.reset();
  last_data_.reset();
  is_reverse_ = false;
  is_end_ = false;
  is_inited_ = false;
}

/********** ObLobPartialUpdateRowIter ****************/

ObLobPartialUpdateRowIter::~ObLobPartialUpdateRowIter()
{
}

int ObLobPartialUpdateRowIter::open(ObLobAccessParam &param, ObLobLocatorV2 &delta_lob, ObLobDiffHeader *diff_header)
{
  int ret = OB_SUCCESS;
  param_ = &param;
  delta_lob_ = delta_lob;
  char *buf = diff_header->data_;
  int64_t data_len = diff_header->persist_loc_size_;
  int64_t pos = 0;
  if (OB_FAIL(partial_data_.init())) {
    LOG_WARN("map create fail", K(ret));
  } else if (OB_FAIL(partial_data_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize partial data fail", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(partial_data_.sort_index())) {
    LOG_WARN("sort_index fail", K(ret), K(data_len), K(partial_data_));
  }
  return ret;  
}

int ObLobPartialUpdateRowIter::get_next_row(int64_t &offset, ObLobMetaInfo *&old_info, ObLobMetaInfo *&new_info)
{
  int ret = OB_SUCCESS;
  bool found_row = false;
  for(; OB_SUCC(ret) && ! found_row && chunk_iter_ < partial_data_.index_.count(); ++chunk_iter_) {
    ObLobChunkIndex &idx =  partial_data_.index_[chunk_iter_];
    if (1 == idx.is_modified_ || 1 == idx.is_add_) {
      ObLobChunkData &chunk_data = partial_data_.data_[idx.data_idx_];
      found_row = true;
      ObString old_data;
      if (! idx.is_add_ && idx.old_data_idx_ >= 0) {
        old_data = partial_data_.old_data_[idx.old_data_idx_].data_;
      }
      if (! idx.is_add_) {
        if (OB_FAIL(ObLobMetaUtil::construct(
            *param_, param_->lob_data_->id_, idx.seq_id_,
            old_data.length(), old_data.length(), old_data,
            old_meta_info_))) {
          LOG_WARN("construct old lob_meta_info fail", K(ret), K(idx), K(old_data));
        } else if (OB_FAIL(ObLobMetaUtil::construct(
            *param_, param_->lob_data_->id_, idx.seq_id_,
            idx.byte_len_, idx.byte_len_,
            ObString(idx.byte_len_, chunk_data.data_.ptr() + idx.pos_),
            new_meta_info_))) {
          LOG_WARN("construct new lob_meta_info fail", K(ret), K(idx), K(chunk_data));
        } else {
          offset = idx.offset_;
          old_info = &old_meta_info_;
          new_info = &new_meta_info_;
        }
      } else if (OB_FAIL(ObLobMetaUtil::construct(
          *param_, param_->lob_data_->id_, ObString(),
          idx.byte_len_, idx.byte_len_,
          ObString(idx.byte_len_, chunk_data.data_.ptr() + idx.pos_),
          new_meta_info_))) {
        LOG_WARN("construct new lob_meta_info fail", K(ret), K(idx), K(chunk_data));
      } else {
        offset = idx.offset_;
        new_info = &new_meta_info_;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found_row) {
  } else if (chunk_iter_ == partial_data_.index_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}
/********** ObLobPartialUpdateRowIter ****************/

} // storage
} // oceanbase