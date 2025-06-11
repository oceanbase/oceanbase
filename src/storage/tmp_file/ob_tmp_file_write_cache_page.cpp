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

#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"

namespace oceanbase
{
namespace tmp_file
{

void ObTmpFilePageId::reset()
{
  page_index_in_block_ = -1;
  block_index_ = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
}

bool ObTmpFilePageId::operator==(const ObTmpFilePageId &other) const
{
  return page_index_in_block_ == other.page_index_in_block_
      && block_index_ == other.block_index_;
}

bool ObTmpFileWriteCacheKey::is_valid() const
{
  return ObTmpFileGlobal::INVALID_TMP_FILE_FD != fd_;
}

ObTmpFileWriteCacheKey& ObTmpFileWriteCacheKey::operator=(const ObTmpFileWriteCacheKey &other)
{
  type_ = other.type_;
  fd_ = other.fd_;
  virtual_page_id_ = other.virtual_page_id_;
  return *this;
}

void ObTmpFileWriteCacheKey::reset()
{
  type_ = PageType::INVALID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  virtual_page_id_ = 0;
}

bool ObTmpFileWriteCacheKey::operator==(const ObTmpFileWriteCacheKey &other) const
{
  return fd_ == other.fd_
      && type_ == other.type_
      && virtual_page_id_ == other.virtual_page_id_;
}

int ObTmpFileWriteCacheKey::hash(uint64_t &hash_val) const
{
  hash_val = murmurhash(&type_, sizeof(PageType), 0);
  hash_val = murmurhash(&fd_, sizeof(int64_t), hash_val);
  hash_val = murmurhash(&virtual_page_id_, sizeof(int64_t), hash_val);
  return OB_SUCCESS;
}

ObTmpFilePage::ObTmpFilePage()
  : flag_(0), buf_(nullptr), list_node_(*this), page_id_(), page_key_()
{
}

ObTmpFilePage::ObTmpFilePage(char *buf, const uint32_t idx)
  : flag_(0), buf_(buf), list_node_(*this), page_id_(), page_key_()
{
  set_array_index(idx);
}

bool ObTmpFilePage::is_valid()
{
  return get_ref() > 0;
}

void ObTmpFilePage::set_is_full(bool is_full)
{
  if (is_full) {
    __atomic_fetch_or(&flag_, PG_FULL_MASK, __ATOMIC_SEQ_CST);
  } else {
    __atomic_fetch_and(&flag_, ~PG_FULL_MASK, __ATOMIC_SEQ_CST);
  }
}

void ObTmpFilePage::set_loading(bool is_loading)
{
  if (is_loading) {
    __atomic_fetch_or(&flag_, PG_LOADING_MASK, __ATOMIC_SEQ_CST);
  } else {
    __atomic_fetch_and(&flag_, ~PG_LOADING_MASK, __ATOMIC_SEQ_CST);
  }
}

void ObTmpFilePage::set_ref(uint32_t ref) {
  uint64_t old_val = 0;
  uint64_t new_val = 0;
  const uint64_t val = (static_cast<uint64_t>(ref) << 16) & PG_REF_MASK;
  do {
    old_val = ATOMIC_LOAD(&flag_);
    new_val = (old_val & ~PG_REF_MASK) | val;
  } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_val, new_val));
}

void ObTmpFilePage::set_array_index(uint32_t index) {
  uint64_t old_val = 0;
  uint64_t new_val = 0;
  const uint64_t val = (static_cast<uint64_t>(index) << 32) & PG_ARRAY_MASK;
  do {
    old_val = ATOMIC_LOAD(&flag_);
    new_val = (old_val & ~PG_ARRAY_MASK) | val;
  } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_val, new_val));
}

void ObTmpFilePage::set_page_key(const ObTmpFileWriteCacheKey &key)
{
  page_key_ = key;
}

void ObTmpFilePage::set_page_id(const ObTmpFilePageId &page_id)
{
  page_id_ = page_id;
}

bool ObTmpFilePage::is_full() const
{
  return ATOMIC_LOAD(&flag_) & PG_FULL_MASK;
}

bool ObTmpFilePage::is_loading() const
{
  return ATOMIC_LOAD(&flag_) & PG_LOADING_MASK;
}

uint32_t ObTmpFilePage::inc_ref() {
  uint64_t old_val = 0;
  uint64_t new_val = 0;
  uint16_t new_ref = 0;
  uint16_t current_ref = 0;
  do {
    old_val = ATOMIC_LOAD(&flag_);
    current_ref = (old_val & PG_REF_MASK) >> 16;
    new_ref = current_ref + 1;
    if (OB_UNLIKELY(UINT16_MAX == current_ref)) { // OVERFLOW
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("page ref overflow", KR(ret), K(current_ref), KPC(this));
      break;
    }
    new_val = (old_val & ~PG_REF_MASK) | ((static_cast<uint64_t>(new_ref) << 16) & PG_REF_MASK);
  } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_val, new_val));
  return new_ref;
}

uint32_t ObTmpFilePage::dec_ref() {
  int ret = OB_SUCCESS;
  uint64_t old_val = 0;
  uint64_t new_val = 0;
  uint16_t new_ref = 0;
  uint16_t current_ref = 0;
  do {
    old_val = ATOMIC_LOAD(&flag_);
    current_ref = (old_val & PG_REF_MASK) >> 16;
    new_ref = current_ref - 1;
    if (OB_UNLIKELY(current_ref == 0)) { // UNDERFLOW
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("page ref underflow", KR(ret), KPC(this));
      break;
    }
    new_val = (old_val & ~PG_REF_MASK) | ((static_cast<uint64_t>(new_ref) << 16) & PG_REF_MASK);
  } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_val, new_val));

  if (OB_SUCC(ret) && new_ref == 0) {
    ObTmpFileWriteCache &write_cache = MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_write_cache();
    if (OB_FAIL(write_cache.add_free_page_(this))) {
      LOG_ERROR("failed to add page to write cache free list", KR(ret), KPC(this));
    }
  }
  return new_ref;
}

uint32_t ObTmpFilePage::get_ref() const
{
  return (ATOMIC_LOAD(&flag_) & PG_REF_MASK) >> 16;
}

uint32_t ObTmpFilePage::get_array_index() const
{
  return (ATOMIC_LOAD(&flag_) & PG_ARRAY_MASK) >> 32;
}

char *ObTmpFilePage::get_buffer() const
{
  return buf_;
}

ObTmpFilePageId ObTmpFilePage::get_page_id() const
{
  return page_id_;
}

ObTmpFileWriteCacheKey ObTmpFilePage::get_page_key() const
{
  return page_key_;
}

ObTmpFilePage::PageListNode &ObTmpFilePage::get_list_node()
{
  return list_node_;
}

void ObTmpFilePage::replace_node(ObTmpFilePage &other)
{
  list_node_.replace_by(&other.get_list_node());
}

void ObTmpFilePage::reset()
{
  // do not reset array_index_
  set_is_full(false);
  set_loading(false);
  ref_ = 0;
  page_id_.reset();
  page_key_.reset();
}

int ObTmpFilePage::assign(const ObTmpFilePage &other)
{
  set_is_full(other.is_full());
  set_loading(other.is_loading());
  set_array_index(other.get_array_index());
  set_ref(other.get_ref());
  buf_ = other.buf_;
  page_id_ = other.page_id_;
  page_key_ = other.page_key_;

  ObTmpFilePage &other_page = const_cast<ObTmpFilePage &>(other);
  other_page.replace_node(*this);
  return OB_SUCCESS;
}

ObTmpFilePageHandle::ObTmpFilePageHandle() : page_(nullptr)
{
}

ObTmpFilePageHandle::ObTmpFilePageHandle(ObTmpFilePage *page)
{
  if (OB_NOT_NULL(page)) {
    page_ = page;
    page_->inc_ref();
  }
}

ObTmpFilePageHandle::ObTmpFilePageHandle(const ObTmpFilePageHandle &other)
  : page_(nullptr)
{
  operator=(other);
}

ObTmpFilePageHandle::~ObTmpFilePageHandle()
{
  reset();
}

int ObTmpFilePageHandle::reset()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(page_)) {
    page_->dec_ref();
  }
  page_ = nullptr;
  return ret;
}

int ObTmpFilePageHandle::init(ObTmpFilePage *page)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(page_) && OB_FAIL(reset())) {
    LOG_WARN("failed to reset old page", KR(ret), KPC(page_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(page)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("page ptr is null", KR(ret));
  } else {
    page_ = page;
    page_->inc_ref();
  }
  return ret;
}

int ObTmpFilePageHandle::assign(const ObTmpFilePageHandle &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_NOT_NULL(page_) && OB_FAIL(reset())) {
      LOG_WARN("failed to reset old page", KR(ret), KPC(page_));
    }

    if (OB_NOT_NULL(other.page_)) {
      page_ = other.page_;
      page_->inc_ref();
    }
  }
  return ret;
}

ObTmpFilePageHandle& ObTmpFilePageHandle::operator=(const ObTmpFilePageHandle &other)
{
  if (this != &other) {
    if (OB_NOT_NULL(page_)) {
      reset();
    }

    if (OB_NOT_NULL(other.page_)) {
      page_ = other.page_;
      page_->inc_ref();
    }
  }
  return *this;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
