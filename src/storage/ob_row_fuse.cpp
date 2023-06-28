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

#include "storage/ob_row_fuse.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace storage
{
int ObNopPos::init(ObIAllocator &allocator, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(capacity));
  } else {
    if (capacity <= capacity_) {
    } else {
      free();
      if (NULL == (nops_ = static_cast<int16_t*>(allocator.alloc(
                sizeof(int16_t) * capacity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "no memory", K(ret), K(capacity));
      } else {
        allocator_ = &allocator;
        capacity_ = capacity;
      }
    }
  }
  return ret;
}

void ObNopPos::free()
{
  if (nullptr != nops_ && nullptr != allocator_) {
    allocator_->free(nops_);
    nops_ = nullptr;
  }
  capacity_ = 0;
  count_ = 0;
}

void ObNopPos::destroy()
{
  free();
  allocator_ = NULL;
}

int ObNopPos::get_nop_pos(const int64_t idx, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else {
    pos = nops_[idx];
  }
  return ret;
}

int ObNopPos::set_count(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (capacity_ <= 0 || count < 0 || count > capacity_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "wrong nop count", K(ret), K(count), K_(capacity));
  } else {
    count_ = count;
  }
  return ret;
}

int ObNopPos::set_nop_pos(const int64_t idx, const int64_t pos)
{
  int ret = OB_SUCCESS;
  if (capacity_ <= 0 || idx < 0 || idx >= capacity_ || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "wrong nop count", K(ret), K(idx), K_(capacity), K(pos));
  } else {
    nops_[idx] = static_cast<int16_t>(pos);
  }
  return ret;
}

ObObjShallowCopy ObRowFuse::shallow_copy_;


template<typename T>
OB_INLINE static int simple_fuse_row(
    const ObStoreRow &former,
    ObStoreRow &result,
    ObNopPos &nop_pos,
    bool &final_result,
    T &obj_copy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!former.is_valid()) || OB_ISNULL(result.row_val_.cells_) || !nop_pos.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments, ", K(former), K(former.is_valid()), KP(result.row_val_.cells_), K(nop_pos.count()), K(nop_pos.capacity()), K(ret));
  } else if (result.flag_.is_delete() || former.flag_.is_not_exist()) {
    // do nothing
  } else {
    final_result = false;
    bool first_val = (0 == result.row_val_.count_ || result.flag_.is_not_exist());
    int64_t column_cnt = 0;

    if (first_val) {
      nop_pos.reset();
      result.flag_ = former.flag_;
      result.from_base_ = former.from_base_;
      column_cnt = former.row_val_.count_;
    } else {
      column_cnt = nop_pos.count_;
    }

    if (former.flag_.is_delete()) {
      final_result = true;
      if (first_val) { // copy rowkey
        result.row_val_.count_ = former.row_val_.count_;
        for (int i = 0; OB_SUCC(ret) && i < former.row_val_.count_; ++i) {
          if (OB_FAIL(obj_copy(former.row_val_.cells_[i], result.row_val_.cells_[i]))) {
            STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(i), K(former.row_val_.cells_[i]));
          }
        }
      }
    } else if (former.flag_.is_exist_without_delete()) {
      if (OB_UNLIKELY(result.flag_.is_exist_without_delete()
                         && result.row_val_.is_valid()
                         && nop_pos.count() > result.row_val_.count_
                         && result.row_val_.count_ != former.row_val_.count_)) {
        ret = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(former), K(result), K(nop_pos.count()));
      } else {
        STORAGE_LOG(DEBUG, "start to fuse", K(former), K(result), K(nop_pos.count()));
        int64_t idx = -1;
        int64_t left_cnt = 0;
        bool is_former_nop = true;

        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
          idx = first_val ? i : nop_pos.nops_[i];
          is_former_nop = former.row_val_.cells_[idx].is_nop_value();
          if (is_former_nop) {
            nop_pos.nops_[left_cnt++] = static_cast<int16_t>(idx);
          }
          if (is_former_nop && !first_val) {
            // do nothing
          } else if (OB_FAIL(obj_copy(former.row_val_.cells_[idx], result.row_val_.cells_[idx]))) {
            STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(idx), K(former.row_val_.cells_[idx]));
          }
        }
        final_result = (0 == left_cnt);
        result.row_val_.count_ = former.row_val_.count_;
        nop_pos.count_ = left_cnt;
      }
    } else {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "wrong row flag", K(ret), K(former));
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const ObStoreRow &former,
                          ObStoreRow &result,
                          ObNopPos &nop_pos,
                          bool &final_result,
                          ObObjDeepCopy *obj_copy)
{
  int ret = OB_SUCCESS;
  if (nullptr == obj_copy) {
    if (OB_FAIL(simple_fuse_row(former, result, nop_pos, final_result, shallow_copy_))) {
      STORAGE_LOG(WARN, "fail to fuse simple row with shallow copy", K(ret), K(former), K(result));
    }
  } else {
    if (OB_FAIL(simple_fuse_row(former, result, nop_pos, final_result, *obj_copy))) {
      STORAGE_LOG(WARN, "fail to fuse simple row with deep copy", K(ret), K(former), K(result));
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const blocksstable::ObDatumRow &former,
                        blocksstable::ObDatumRow &result,
                        ObNopPos &nop_pos,
                        bool &final_result,
                        ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!former.is_valid() || !result.is_valid() || !nop_pos.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments, ", K(former), K(result), K(nop_pos.count()), K(nop_pos.capacity()), K(ret));
  } else if (result.row_flag_.is_delete() || former.row_flag_.is_not_exist()) {
    // do nothing
  } else {
    final_result = false;
    bool first_val = (0 == result.count_ || result.row_flag_.is_not_exist());
    int64_t column_cnt = 0;
    // add by zimiao, When result.trans_info_ is nullptr,
    // it means that the current row does not have any transaction information,
    // so set the first transaction information obtained by the current row
    if (OB_ISNULL(result.trans_info_)) {
      result.trans_info_ = former.trans_info_;
    }
    if (first_val) {
      nop_pos.reset();
      result.row_flag_ = former.row_flag_;
      column_cnt = former.count_;
    } else {
      column_cnt = nop_pos.count_;
    }

    if (former.row_flag_.is_delete()) {
      final_result = true;
      if (first_val) { // copy rowkey
        result.count_ = former.count_;
        for (int i = 0; OB_SUCC(ret) && i < former.count_; ++i) {
          if (OB_ISNULL(allocator)) {
            result.storage_datums_[i] = former.storage_datums_[i];
          } else if (OB_FAIL(result.storage_datums_[i].deep_copy(former.storage_datums_[i], *allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy storage datum", K(ret));
          }
        }
      }
    } else if (former.row_flag_.is_exist_without_delete()) {
      if (OB_UNLIKELY(result.row_flag_.is_exist_without_delete()
                         && result.is_valid()
                         && nop_pos.count() > result.count_
                         && result.count_ != former.count_)) {
        ret = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(former), K(result), K(nop_pos.count()));
      } else {
        STORAGE_LOG(DEBUG, "start to fuse", K(former), K(result), K(nop_pos.count()));
        int64_t idx = -1;
        int64_t left_cnt = 0;
        bool is_former_nop = true;

        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
          idx = first_val ? i : nop_pos.nops_[i];
          is_former_nop = former.storage_datums_[idx].is_nop();
          if (is_former_nop) {
            nop_pos.nops_[left_cnt++] = static_cast<int16_t>(idx);
          }
          if (is_former_nop && !first_val) {
            // do nothing
          } else if (OB_ISNULL(allocator)) {
            result.storage_datums_[idx] = former.storage_datums_[idx];
          } else if (OB_FAIL(result.storage_datums_[idx].deep_copy(former.storage_datums_[idx], *allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy storage datum", K(ret), K(idx));
          }
        }
        final_result = (0 == left_cnt);
        result.count_ = MAX(result.count_, former.count_);
        nop_pos.count_ = left_cnt;
        STORAGE_LOG(DEBUG, "fuse row", K(ret), K(former), K(result));
      }
    } else {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "wrong row flag", K(ret), K(former));
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
