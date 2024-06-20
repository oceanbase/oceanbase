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

#include "ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{


int ObLobPersistWriteIter::update_seq_no()
{
  int ret = OB_SUCCESS;
  if (param_->seq_no_st_.is_valid()) {
    if (param_->used_seq_cnt_ < param_->total_seq_cnt_) {
      param_->dml_base_param_->spec_seq_no_ = param_->seq_no_st_ + param_->used_seq_cnt_;
      param_->used_seq_cnt_++;
      LOG_DEBUG("dml lob meta with seq no", K(param_->dml_base_param_->spec_seq_no_));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get seq no from param.", K(ret), KPC(param_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seq no from param.", K(ret), KPC(param_));
  }
  return ret;
}

int ObLobPersistWriteIter::inc_lob_size(ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  param_->lob_data_->byte_size_ += info.byte_len_;
  param_->byte_size_ = param_->lob_data_->byte_size_;
  if (ObLobManager::lob_handle_has_char_len(*param_)) {
    int64_t *len = ObLobManager::get_char_len_ptr(*param_);
    *len = *len + info.char_len_;
    OB_ASSERT(*len >= 0);
  }
  return ret;
}

int ObLobPersistWriteIter::dec_lob_size(ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  param_->lob_data_->byte_size_ -= info.byte_len_;
  if (ObLobManager::lob_handle_has_char_len(*param_)) {
    int64_t *len = ObLobManager::get_char_len_ptr(*param_);
    *len = *len - info.char_len_;
    OB_ASSERT(*len >= 0);
  }
  param_->byte_size_ = param_->lob_data_->byte_size_;
  return ret;
}

int ObLobPersistUpdateSingleRowIter::init(ObLobAccessParam *param, ObNewRow *old_row, ObNewRow *new_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(old_row) || OB_ISNULL(new_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(old_row), KP(new_row));
  } else {
    param_ = param;
    old_row_ = old_row;
    new_row_ = new_row;
  }
  return ret;
}

int ObLobPersistUpdateSingleRowIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_row_) || OB_ISNULL(new_row_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get next row failed, null val.", K(ret), K(old_row_), K(new_row_));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (!got_old_row_) {
    row = old_row_;
    got_old_row_ = true;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = new_row_;
    got_old_row_ = false;
    is_iter_end_ = true;
  }
  return ret;
}

int ObLobPersistInsertSingleRowIter::init(ObLobAccessParam *param, ObNewRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(row));
  } else {
    param_ = param;
    row_ = row;
  }
  return ret;
}

int ObLobPersistInsertSingleRowIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(param_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::init(ObLobAccessParam *param, ObNewRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(row));
  } else {
    param_ = param;
    row_ = row;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(param_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

int ObLobPersistInsertIter::init(ObLobAccessParam *param, ObLobMetaWriteIter *meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or meta_iter is null", K(ret), KP(param), KP(meta_iter));
  } else {
    param_ = param;
    meta_iter_ = meta_iter;
  }
  return ret;
}

int ObLobPersistInsertIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_iter_->get_next_row(result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next meta info failed.", K(ret));
    }
  } else if (OB_FALSE_IT(result_.info_.lob_data_.assign_ptr(result_.data_.ptr(), result_.data_.length()))) {
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else if (OB_FAIL(ObLobManager::update_out_ctx(*param_, nullptr, result_.info_))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else if (OB_FAIL(inc_lob_size(result_.info_))) {
    LOG_WARN("inc_lob_size fail", K(ret));
  } else {
    ObPersistentLobApator::set_lob_meta_row(row_cell_, new_row_, result_.info_);
    row = &new_row_;
  }
  return ret;
}

int ObLobPersistDeleteIter::init(ObLobAccessParam *param, ObLobMetaScanIter *meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or meta_iter is null", K(ret), KP(param), KP(meta_iter));
  } else {
    param_ = param;
    meta_iter_ = meta_iter;
  }
  return ret;
}

int ObLobPersistDeleteIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_iter_->get_next_row(result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next meta info failed.", K(ret));
    }
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else if (OB_FAIL(ObLobManager::update_out_ctx(*param_, nullptr, result_.info_))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else if (OB_FAIL(dec_lob_size(result_.info_))) {
    LOG_WARN("dec_lob_size fail", K(ret));
  } else {
    ObPersistentLobApator::set_lob_meta_row(row_cell_, new_row_, result_.info_);
    row = &new_row_;
  }
  return ret;
}


} // storage
} // oceanbase
