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
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_locator_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

int ObLobDiskLocatorWrapper::get_char_len(const ObLobLocatorV2 &locator, uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  ObString disk_locator;
  if (OB_FAIL(locator.get_disk_locator(disk_locator))) {
    LOG_WARN("failed to get lob common from lob locator", K(ret), K(locator));
  } else if (disk_locator.length() != ObLobManager::LOB_OUTROW_FULL_SIZE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk locator invalid", K(ret), K(locator));
  } else {
    ObLobCommon *lob_common = reinterpret_cast<ObLobCommon*>(disk_locator.ptr());
    ObLobData *lob_data = reinterpret_cast<ObLobData*>(lob_common->buffer_);
    ObLobDataOutRowCtx *outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(lob_data->buffer_);
    uint64_t *char_len_ptr = reinterpret_cast<uint64_t*>(outrow_ctx + 1);
    char_len = *char_len_ptr;
  }
  return ret;
}

bool ObLobDiskLocatorWrapper::is_valid() const
{
  return nullptr != lob_common_ && nullptr != lob_data_ && nullptr != outrow_ctx_ && nullptr != char_len_ptr_;
}

int ObLobDiskLocatorWrapper::init(char *ptr, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_disk_locator_length(len))) {
    LOG_WARN("check_disk_locator_length fail", K(ret), K(len), KP(ptr));
  } else if (OB_ISNULL(lob_common_ = reinterpret_cast<ObLobCommon*>(ptr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob common is null", K(ret), K(len), KP(ptr));
  } else  if (lob_common_->in_row_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not be inrow", K(ret), KPC_(lob_common), K(len), KP(ptr));
  } else {
    lob_data_ = reinterpret_cast<ObLobData*>(lob_common_->buffer_);
    outrow_ctx_ = reinterpret_cast<ObLobDataOutRowCtx*>(lob_data_->buffer_);
    char_len_ptr_ = reinterpret_cast<uint64_t*>(outrow_ctx_ + 1);
  }
  return ret;
}

int ObLobDiskLocatorWrapper::reset_for_dml()
{
  int ret = OB_SUCCESS;

  lob_data_->byte_size_ = 0;

  outrow_ctx_->seq_no_st_ = 0;
  outrow_ctx_->seq_no_cnt_ = 0;
  outrow_ctx_->del_seq_no_cnt_ = 0;

  uint64_t char_len = *char_len_ptr_;
  if (char_len != UINT64_MAX) {
    *char_len_ptr_ = 0;
  }

  return ret;
}


int ObLobDiskLocatorWrapper::check_disk_locator_length(const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (len != ObLobManager::LOB_OUTROW_FULL_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk locator length", K(ret), K(len));
  }
  return ret;
}


int ObLobDiskLocatorWrapper::check_for_dml(ObLobDiskLocatorWrapper &other) const
{
  int ret = OB_SUCCESS;
  if (! is_valid() || ! other.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", K(ret), K(other), KPC(this));
  } else if (lob_common_->in_row_ || ! lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_common is not outrow", K(ret), K(other), KPC(this));
  } else if (get_lob_id() != other.get_lob_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob id is not match", K(ret), K(other), KPC(this));
  } else if (get_byte_size() != other.get_byte_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob byte size is not match", K(ret), K(other), KPC(this));
  } else if (get_char_len() != other.get_char_len()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob char len is not match", K(ret), K(other), KPC(this));
  // if op is sql, means not register ext info log callback
  // so seq no shuold be same
  } else if (ObLobDataOutRowCtx::OpType::SQL == outrow_ctx_->op_ && outrow_ctx_->op_ == other.outrow_ctx_->op_ && 
      (outrow_ctx_->seq_no_st_ != other.outrow_ctx_->seq_no_st_ || outrow_ctx_->seq_no_cnt_ != other.outrow_ctx_->seq_no_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("seq no is not match", K(ret), K(other), KPC(this));
  }
  return ret;
}

int ObLobDiskLocatorBuilder::init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t locator_len = ObLobManager::LOB_OUTROW_FULL_SIZE;
  char *ptr = nullptr;
  if (OB_ISNULL(ptr = static_cast<char*>(allocator.alloc(locator_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf fail", K(ret), K(locator_len));
  } else {
    ptr_ = ptr;
    len_ = locator_len;
    lob_common_ = new(ptr)ObLobCommon();
    lob_common_->in_row_ = 0;
    lob_common_->is_init_ = 1;
    lob_data_ = new(lob_common_->buffer_)ObLobData();
    outrow_ctx_ = new(lob_data_->buffer_)ObLobDataOutRowCtx();    
    outrow_ctx_->is_full_ = 1;
    char_len_ptr_ = reinterpret_cast<uint64_t*>(outrow_ctx_ + 1);
    *char_len_ptr_ = 0;
  }
  return ret;
}

int ObLobDiskLocatorBuilder::set_lob_id(const ObLobId& lob_id)
{
  int ret = OB_SUCCESS;
  lob_data_->id_ = lob_id;
  return ret;
}

int ObLobDiskLocatorBuilder::set_byte_len(const uint64_t &byte_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_data_) || 0 == byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data is invalid", K(ret), K(byte_len), KPC(this));
  } else {
    lob_data_->byte_size_ = byte_len;
  }
  return ret;
}

int ObLobDiskLocatorBuilder::set_char_len(const uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  *char_len_ptr_ = char_len;
  return ret;
}

int ObLobDiskLocatorBuilder::set_seq_no(const ObLobDataOutRowCtx::OpType type, transaction::ObTxSEQ &seq_no_st, const uint32_t seq_no_cnt)
{
  int ret = OB_SUCCESS;
  outrow_ctx_->op_ = type;
  outrow_ctx_->seq_no_st_ = seq_no_st.cast_to_int();
  outrow_ctx_->seq_no_cnt_ = seq_no_cnt;
  return ret;
}

int ObLobDiskLocatorBuilder::set_chunk_size(const int64_t lob_chunk_size)
{
  int ret = OB_SUCCESS;
  outrow_ctx_->chunk_size_ = lob_chunk_size / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
  return ret;
}
int ObLobDiskLocatorBuilder::set_ext_info_log_length(const int64_t len)
{
  int ret = OB_SUCCESS;
  outrow_ctx_->modified_len_ = len;
  return ret;
}

int ObLobDiskLocatorBuilder::to_locator(ObLobLocatorV2 &locator) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_) || 0 == len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is empty", K(ret), KPC(this));
  } else {
    locator.assign_buffer(ptr_, len_);
  }
  return ret;
}

}; // end namespace storage
}; // end namespace oceanbase