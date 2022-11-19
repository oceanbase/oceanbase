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

namespace oceanbase
{
namespace storage
{

int ObLobMetaScanIter::open(ObLobAccessParam &param, ObILobApator* lob_adatper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lob_adatper->scan_lob_meta(param, scan_param_, meta_iter_))) {
    LOG_WARN("failed to open iter", K(ret));
  } else {
    lob_adatper_ = lob_adatper;
    param_ = param;
    cur_pos_ = 0;
  }
  return ret;
}

ObLobMetaScanIter::ObLobMetaScanIter()
  : lob_adatper_(nullptr), meta_iter_(nullptr), param_(), scan_param_(), cur_pos_(0) {}

int ObLobMetaScanIter::get_next_row(ObLobMetaInfo &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_iter_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("meta_iter is null.", K(ret));
  } else {
    bool has_found = false;
    uint64_t old_cur_pos = cur_pos_;
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
        if (is_range_over(row)) {
          // if row cur_pos > offset + len, need break;
          ret = OB_ITER_END;
        } else if (param_.scan_backward_ || is_in_range(row)) {
          has_found = true;
        } else {
          // TODO
          // ret = OB_EAGAIN;
        }
        // update sum(len)
        cur_pos_ += (is_char) ? row.char_len_ : row.byte_len_;
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
      if (!param_.scan_backward_) {
        // 3种场景，meta_info与左右边界相交，或者被包含
        if (cur_pos < param_.offset_) { // 越过左边界,
          if (cur_pos + cur_len < param_.offset_) {
            ret = OB_ERR_INTERVAL_INVALID;
            LOG_WARN("Invalid query result at left edge.", K(ret), K(cur_pos), K(cur_len), K(param_.offset_), K(param_.len_));
          } else {
            result.st_ = param_.offset_ - cur_pos;
            result.len_ -= result.st_;
          }
        }
        if (OB_SUCC(ret)) {
          if (cur_pos + cur_len > param_.offset_ + param_.len_) { // 越过右边界
            if (cur_pos > param_.offset_ + param_.len_) {
              ret = OB_ERR_INTERVAL_INVALID;
              LOG_WARN("Invalid query result at right edge.", K(ret), K(cur_pos), K(cur_len), K(param_.offset_), K(param_.len_));
            }
            result.len_ = param_.offset_ + param_.len_ - cur_pos - result.st_;
          }
        }
      }
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
    scan_iter_()
{
  data_ = data;
  offset_ = 0;
  piece_id_ = 0;
  piece_block_size_ = piece_block_size;
}

int ObLobMetaWriteIter::open(ObLobAccessParam &param, ObILobApator* adatper)
{
  int ret = OB_SUCCESS;
  
  if (OB_FAIL(scan_iter_.open(param, adatper))) {
    LOG_WARN("failed open scan meta open iter.", K(ret));
  } else {
    coll_type_ = param.coll_type_;
    lob_id_ = param.lob_data_->id_;
    piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    ObLobMetaScanResult scan_res;
    
    // locate first piece 
    ret = scan_iter_.get_next_row(scan_res);
    if (OB_FAIL(ret)) {
      if (ret == OB_ITER_END) {
        // empty table
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next row.", K(ret));
      }
    } else {
      seq_id_.set_seq_id(scan_res.info_.seq_id_);
    }
  }
  return ret;
}

int ObLobMetaWriteIter::get_next_row(ObLobMetaWriteResult &row)
{
  int ret = OB_SUCCESS;
  if (data_.length() > offset_) {
    uint32_t left_size = data_.length() - offset_;
    int64_t by_len = std::min(piece_block_size_, left_size);
    int64_t char_len = by_len;

    if (coll_type_ != common::ObCollationType::CS_TYPE_BINARY) {
      char_len = offset_;
      by_len = ObCharset::max_bytes_charpos(coll_type_,
                                            data_.ptr() + offset_, 
                                            data_.length() - offset_, 
                                            by_len, 
                                            char_len);
    }

    row.data_.assign_ptr(data_.ptr() + offset_, by_len);
    offset_ += by_len;
    row.need_alloc_macro_id_ = true;

    row.info_.byte_len_ = by_len;
    row.info_.lob_id_ = lob_id_;
    row.info_.char_len_ = char_len;
    row.info_.lob_data_ = ObString();

    // TODO: weiyouchao.wyc
    // use get auto increment interface;
    row.info_.piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    
    if (OB_FAIL(seq_id_.get_next_seq_id(row.info_.seq_id_))) {
      LOG_WARN("failed to next seq id.", K(ret));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObLobMetaWriteIter::close()
{
  int ret = OB_SUCCESS;
  scan_iter_.reset();
  seq_id_.reset();
  return ret;
}

int ObLobMetaManager::write(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.write_lob_meta_tablet(param, in_row))) {
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
  if (OB_FAIL(persistent_lob_adapter_.erase_lob_meta_tablet(param, in_row))) {
    LOG_WARN("erase lob meta failed.", K(ret), K(param));
  }
  return ret;
}

// update specified range
int ObLobMetaManager::update(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.update_lob_meta_tablet(param, old_row, new_row))) {
    LOG_WARN("update lob meta failed.");
  }
  return ret;
}

int ObLobMetaManager::fetch_lob_id(const ObLobAccessParam& param, uint64_t &lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.fetch_lob_id(param, lob_id))) {
    LOG_WARN("fetch lob id failed.", K(ret), K(param));
  }
  return ret;
}

}
}
