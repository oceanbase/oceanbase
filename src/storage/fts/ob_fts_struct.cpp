/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_struct.h"
#include "lib/charset/ob_charset.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace storage
{

int ObFTToken::init(
    const char *ptr,
    const int64_t length,
    const ObObjMeta &meta,
    const sql::ObExprHashFuncType hash_func,
    const ObDatumCmpFuncType cmp_func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ptr || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments",
        K(ret), KP(ptr), K(length), K(meta), KP(hash_func), KP(cmp_func));
  } else {
    token_.set_string(ptr, length);
    meta_ = meta;
    hash_func_ = hash_func;
    cmp_func_ = cmp_func;
    is_calc_hash_val_ = false;
    hash_val_ = 0;
  }
  return ret;
}

int ObFTToken::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  if (is_calc_hash_val_) {
    hash_val = hash_val_;
  } else {
    sql::ObExprHashFuncType hash_func = nullptr;
    if (OB_UNLIKELY(nullptr == hash_func_)) {
      sql::ObExprBasicFuncs *funcs = ObDatumFuncs::get_basic_func(meta_.get_type(), meta_.get_collation_type());
      if (OB_UNLIKELY(nullptr == funcs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get basic funcs", K(ret));
      } else if (OB_UNLIKELY(nullptr == funcs->default_hash_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get default hash func", K(ret));
      } else {
        hash_func = funcs->default_hash_;
      }
    } else {
      hash_func = hash_func_;
    }
    if (FAILEDx(hash_func(token_, 0, hash_val_))) {
      LOG_WARN("fail to calc hash", K(ret));
    } else {
      is_calc_hash_val_ = true;
      hash_val = hash_val_;
    }
  }
  return ret;
}

bool ObFTToken::operator==(const ObFTToken &other) const
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_FAIL(do_compare(other, is_equal))) {
    LOG_WARN("fail to do token compapre", K(ret));
    ob_abort();
  }
  return is_equal;
}

int ObFTToken::do_compare(const ObFTToken &other, bool &is_equal) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  is_equal = false;
  if (OB_LIKELY(is_calc_hash_val_ && other.is_calc_hash_val_) && (hash_val_ != other.hash_val_)) {
    // not equal
  } else {
    ObDatumCmpFuncType cmp_func = cmp_func_ == nullptr ? get_datum_cmp_func(meta_, other.meta_) : cmp_func_;
    if (OB_UNLIKELY(nullptr == cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the cmp func is null", K(ret));
    } else if (OB_FAIL(cmp_func(token_, other.token_, cmp_ret))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to compare", K(ret));
    } else {
      is_equal = (0 == cmp_ret);
    }
  }
  return ret;
}

ObFTTokenInfo::~ObFTTokenInfo()
{
  release_position_list_();
}

ObFTTokenInfo::ObFTTokenInfo(const ObFTTokenInfo &other)
    : allocator_(other.allocator_),
      count_(other.count_),
      position_list_(other.position_list_),
      position_list_holder_(other.position_list_holder_)
{
  retain_position_list_();
}

ObFTTokenInfo &ObFTTokenInfo::operator=(const ObFTTokenInfo &other)
{
  if (this != &other) {
    release_position_list_();
    allocator_ = other.allocator_;
    count_ = other.count_;
    position_list_ = other.position_list_;
    position_list_holder_ = other.position_list_holder_;
    retain_position_list_();
  }
  return *this;
}

void ObFTTokenInfo::retain_position_list_()
{
  if (OB_NOT_NULL(position_list_holder_)) {
    ++position_list_holder_->ref_cnt_;
  }
}

void ObFTTokenInfo::release_position_list_()
{
  if (OB_NOT_NULL(position_list_holder_)) {
    if (0 == --position_list_holder_->ref_cnt_) {
      position_list_holder_->~ObFTPositionListHolder();
      allocator_->free(position_list_holder_);
      allocator_ = nullptr;
    }
  }
  position_list_holder_ = nullptr;
  position_list_ = nullptr;
}
int ObFTTokenInfo::update_one_position(ObIAllocator &allocator, const int64_t position)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(position < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (count_ >= share::ObFTSConstants::MAX_POSITION_LIST_COUNT) {
    // only support maximum 512 position list
    // don't retrun failure on purpose
  } else {
    if (OB_ISNULL(position_list_)) {
      allocator_ = &allocator;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObFTPositionListHolder)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for position list", K(ret));
      } else {
        position_list_holder_ = new (buf) ObFTPositionListHolder();
        position_list_ = &position_list_holder_->position_list_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (count_ > 0 && OB_UNLIKELY(position_list_->at(count_ - 1) >= position)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("position is not in order", K(ret), K(position), K(position_list_));
    } else if (OB_FAIL(position_list_->push_back(position))) {
      LOG_WARN("fail to push back position", K(ret), K(position));
    }
  }

  if (OB_SUCC(ret)) {
    ++count_;
  }
  return ret;
}

int ObFTTokenInfo::update_without_pos_list()
{
  ++count_;
  return OB_SUCCESS;
}
} // namespace storage
} // namespace oceanbase