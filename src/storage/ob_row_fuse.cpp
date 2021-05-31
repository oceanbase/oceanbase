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

namespace oceanbase {
using namespace oceanbase::common;
namespace storage {
int ObNopPos::init(ObIAllocator& allocator, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(capacity));
  } else {
    if (capacity <= capacity_) {
    } else {
      free();
      if (NULL == (nops_ = static_cast<int16_t*>(allocator.alloc(sizeof(int16_t) * capacity)))) {
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

int ObNopPos::get_nop_pos(const int64_t idx, int64_t& pos)
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

template <typename T>
OB_INLINE static int complex_fuse_row(const ObStoreRow& former, ObStoreRow& result, ObNopPos& nop_pos,
    bool& final_result, const common::ObIArray<int32_t>* col_idxs, bool* column_changed, const int64_t column_count,
    T& obj_copy)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!former.is_valid()) || OB_ISNULL(result.row_val_.cells_) || !nop_pos.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments, ", K(former), K(result), K(nop_pos.count()), K(ret));
  } else if (common::ObActionFlag::OP_DEL_ROW != result.flag_) {
    final_result = false;
    bool first_val = (0 == result.row_val_.count_ || common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == result.flag_);
    int64_t cnt = 0;

    if (first_val) {
      nop_pos.reset();
      result.flag_ = former.flag_;
      result.from_base_ = former.from_base_;
      cnt = (NULL == col_idxs) ? former.row_val_.count_ : col_idxs->count();
    } else {
      cnt = nop_pos.count_;
    }

    if (cnt > column_count && NULL != column_changed) {
      ret = common::OB_ERR_SYS;
      STORAGE_LOG(ERROR, "column count not match", K(ret), K(cnt), K(column_count));
    } else if (common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == former.flag_) {
      // skip, do nothing
    } else if (common::ObActionFlag::OP_DEL_ROW == former.flag_) {
      final_result = true;
    } else if (common::ObActionFlag::OP_ROW_EXIST == former.flag_) {
      if (OB_UNLIKELY(common::ObActionFlag::OP_ROW_EXIST == result.flag_ && result.row_val_.is_valid() &&
                      nop_pos.count() > result.row_val_.count_ &&
                      (NULL == col_idxs && result.row_val_.count_ != former.row_val_.count_)) &&
          (NULL != col_idxs && result.row_val_.count_ != col_idxs->count())) {
        ret = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid arguments", K(ret), K(former), K(result), K(nop_pos.count()), K(*col_idxs));
      } else {
        int64_t column_cnt = (NULL == col_idxs) ? former.row_val_.count_ : col_idxs->count();
        int64_t idx = -1;
        int64_t left_cnt = 0;
        int64_t former_idx = -1;
        bool is_former_nop = true;

        for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
          idx = first_val ? i : nop_pos.nops_[i];
          former_idx = (NULL == col_idxs) ? idx : col_idxs->at(idx);
          if (former_idx == -1) {
            // skip the not exist column
            continue;
          }
          is_former_nop = former.row_val_.cells_[former_idx].is_nop_value();

          if (first_val) {
            if (OB_FAIL(obj_copy(former.row_val_.cells_[former_idx], result.row_val_.cells_[idx]))) {
              STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(former_idx), K(idx));
            } else {
              if (is_former_nop) {
                nop_pos.nops_[left_cnt++] = static_cast<int16_t>(idx);
              } else if (NULL != column_changed && !former.from_base_) {
                column_changed[idx] = true;
              }
            }
          } else {
            if (is_former_nop) {
              nop_pos.nops_[left_cnt++] = static_cast<int16_t>(idx);
            } else {
              if (OB_FAIL(obj_copy(former.row_val_.cells_[former_idx], result.row_val_.cells_[idx]))) {
                STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(former_idx), K(idx));
              } else {
                if (NULL != column_changed && !former.from_base_) {
                  column_changed[idx] = true;
                }
              }
            }
          }
        }
        final_result = (0 == left_cnt);
        result.row_val_.count_ = column_cnt;
        nop_pos.count_ = left_cnt;
      }
    } else {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "wrong row flag", K(ret));
    }
  }
  return ret;
}

template <typename T>
OB_INLINE static int simple_fuse_row(
    const ObStoreRow& former, ObStoreRow& result, ObNopPos& nop_pos, bool& final_result, T& obj_copy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!former.is_valid()) || OB_ISNULL(result.row_val_.cells_) || !nop_pos.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid arguments, ",
        K(former),
        K(former.is_valid()),
        KP(result.row_val_.cells_),
        K(nop_pos.count()),
        K(nop_pos.capacity()),
        K(ret));
  } else if (common::ObActionFlag::OP_DEL_ROW == result.flag_ ||
             common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == former.flag_) {
    // do nothing
  } else {
    final_result = false;
    bool first_val = (0 == result.row_val_.count_ || common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == result.flag_);
    int64_t column_cnt = 0;

    if (first_val) {
      nop_pos.reset();
      result.flag_ = former.flag_;
      result.from_base_ = former.from_base_;
      column_cnt = former.row_val_.count_;
    } else {
      column_cnt = nop_pos.count_;
    }

    if (common::ObActionFlag::OP_DEL_ROW == former.flag_) {
      final_result = true;
      if (first_val) {  // copy rowkey
        for (int i = 0; OB_SUCC(ret) && i < former.row_val_.count_; ++i) {
          if (OB_FAIL(obj_copy(former.row_val_.cells_[i], result.row_val_.cells_[i]))) {
            STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(i), K(former.row_val_.cells_[i]));
          }
        }
      }
    } else if (common::ObActionFlag::OP_ROW_EXIST == former.flag_) {
      if (OB_UNLIKELY(common::ObActionFlag::OP_ROW_EXIST == result.flag_ && result.row_val_.is_valid() &&
                      nop_pos.count() > result.row_val_.count_ && result.row_val_.count_ != former.row_val_.count_)) {
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
      STORAGE_LOG(WARN, "wrong row flag", K(ret));
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const ObStoreRow& former, ObStoreRow& result, ObNopPos& nop_pos, bool& final_result,
    const common::ObIArray<int32_t>* col_idxs, bool* column_changed, const int64_t column_count,
    ObObjDeepCopy* obj_copy)
{
  int ret = OB_SUCCESS;
  if (nullptr == obj_copy) {
    if (OB_FAIL(complex_fuse_row(
            former, result, nop_pos, final_result, col_idxs, column_changed, column_count, shallow_copy_))) {
      STORAGE_LOG(WARN,
          "fail to fuse complex row with shallow copy",
          K(ret),
          K(former),
          K(result),
          KP(col_idxs),
          KP(column_changed),
          K(column_count));
    }
  } else {
    if (OB_FAIL(complex_fuse_row(
            former, result, nop_pos, final_result, col_idxs, column_changed, column_count, *obj_copy))) {
      STORAGE_LOG(WARN,
          "fail to fuse complex row with deep copy",
          K(ret),
          K(former),
          K(result),
          KP(col_idxs),
          KP(column_changed),
          K(column_count));
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(
    const ObStoreRow& former, ObStoreRow& result, ObNopPos& nop_pos, bool& final_result, ObObjDeepCopy* obj_copy)
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

template <typename T>
OB_INLINE static int simple_fuse_sparse_row(const ObStoreRow& former, ObStoreRow& result,
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, bool& final_result, T& obj_copy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!former.is_valid()) || OB_ISNULL(result.row_val_.cells_) || OB_ISNULL(result.column_ids_)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid arguments, ", K(former), K(former.is_valid()), K(result), K(ret));
  } else if (common::ObActionFlag::OP_DEL_ROW == result.flag_ ||
             common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == former.flag_) {
    // do nothing
  } else {
    result.is_sparse_row_ = true;  // set sparse row flag
    if (0 == result.row_val_.count_ || common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == result.flag_) {
      result.flag_ = former.flag_;
      result.from_base_ = former.from_base_;
      bit_set.reset();
    }

    if (common::ObActionFlag::OP_DEL_ROW == former.flag_) {
      final_result = true;
      if (0 == result.row_val_.count_) {  // copy rowkey
        for (int i = 0; OB_SUCC(ret) && i < former.row_val_.count_; ++i) {
          if (OB_FAIL(obj_copy(former.row_val_.cells_[i], result.row_val_.cells_[i]))) {
            STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(i), K(former.row_val_.cells_[i]));
          } else {
            result.column_ids_[i] = former.column_ids_[i];
          }
        }
        if (OB_SUCC(ret)) {
          result.row_val_.count_ = former.row_val_.count_;
        }
      }
    } else {
      int index_cnt = result.row_val_.count_;  // next write index in result row
      bool found = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < former.row_val_.count_; i++) {  // loop former row
        found = bit_set.has_member(former.column_ids_[i]);
        if (!found) {  // not found in result row
          if (index_cnt > result.capacity_) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN,
                "input row count is not enough",
                K(ret),
                K(index_cnt),
                K(result.row_val_.count_),
                K(result.capacity_));
          } else if (OB_FAIL(obj_copy(former.row_val_.cells_[i], result.row_val_.cells_[index_cnt]))) {
            STORAGE_LOG(WARN, "failed to copy obj", K(ret), K(i), K(index_cnt));
          } else if (OB_FAIL(bit_set.add_member(former.column_ids_[i]))) {  // add column id into set
            STORAGE_LOG(WARN, "add column id into set failed", K(ret), K(i), K(former.column_ids_[i]));
          } else {
            result.column_ids_[index_cnt] = former.column_ids_[i];
            STORAGE_LOG(DEBUG,
                "add col",
                K(index_cnt),
                K(result.column_ids_[index_cnt]),
                K(result.column_ids_[index_cnt]),
                K(former.row_val_.cells_[i]),
                K(result.row_val_.cells_[index_cnt]));
            index_cnt++;  // move forward
          }
        } else {  // found in result row
          STORAGE_LOG(DEBUG, "already had col", K(former.column_ids_[i]), K(i));
        }
      }
      if (OB_SUCC(ret)) {  // set result row count
        result.row_val_.count_ = index_cnt;
      }
    }
  }
  return ret;
}

int ObRowFuse::fuse_sparse_row(const ObStoreRow& former, ObStoreRow& result,
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, bool& final_result, ObObjDeepCopy* obj_copy)
{
  int ret = OB_SUCCESS;
  if (nullptr == obj_copy) {
    if (OB_FAIL(simple_fuse_sparse_row(former, result, bit_set, final_result, shallow_copy_))) {
      STORAGE_LOG(WARN, "fail to fuse simple row with shallow copy", K(ret), K(former), K(result));
    }
  } else {
    if (OB_FAIL(simple_fuse_sparse_row(former, result, bit_set, final_result, *obj_copy))) {
      STORAGE_LOG(WARN, "fail to fuse simple row with deep copy", K(ret), K(former), K(result));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
