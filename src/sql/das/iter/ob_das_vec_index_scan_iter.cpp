/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASVecIndexScanCtDef, ObDASAttachCtDef), vec_index_param_, query_param_, vector_index_param_, algorithm_type_, vec_type_, dim_, sort_expr_);
OB_SERIALIZE_MEMBER((ObDASVecIndexHNSWScanCtDef, ObDASVecIndexScanCtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexScanRtDef, ObDASAttachRtDef));
OB_SERIALIZE_MEMBER((ObDASVecIndexHNSWScanRtDef, ObDASVecIndexScanRtDef));


int ObVecIndexBitmap::init(uint64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (capacity == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("capacity is 0", K(ret));
  } else {
    int64_t *tmp_vids = static_cast<int64_t *>(allocator_->alloc(sizeof(int64_t) * capacity));
    if (OB_ISNULL(tmp_vids)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vids", K(ret), K(capacity));
    } else {
      capacity_ = capacity;
      type_ = VIDS;
      min_vid_ = INT64_MAX;
      max_vid_ = INT64_MIN;
      valid_cnt_ = 0;
      vids_ = tmp_vids;
    }
  }

  return ret;
}

int ObVecIndexBitmap::add_vid(int64_t vid)
{
  int ret = OB_SUCCESS;
  if (type_ == VIDS) {
    if (OB_ISNULL(vids_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vids_ is null", K(ret));
    } else if (valid_cnt_ >= capacity_) {
      if (OB_FAIL(upgrade_to_byte_array())) {
        LOG_WARN("failed to upgrade to byte array", K(ret));
      }

      if (FAILEDx(add_vid(vid))) {
        LOG_WARN("failed to add vid after upgrade", K(ret));
      }
    } else {
      vids_[valid_cnt_] = vid;
      valid_cnt_++;
      min_vid_ = min(min_vid_, vid);
      max_vid_ = max(max_vid_, vid);
    }
  } else if (type_ == BYTE_ARRAY) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap_ is null", K(ret));
    } else if (vid < min_vid_ || vid > max_vid_) {
      if (OB_FAIL(upgrade_to_roaring_bitmap())) {
        LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
      } else if (OB_FAIL(add_vid(vid))) {
        LOG_WARN("failed to add vid after upgrade", K(ret));
      }
    } else {
      int64_t real_idx = vid - min_vid_;
      bitmap_[real_idx >> 3] |= uint8_t(0x1 << (real_idx & 0x7));
      valid_cnt_++;
    }
  } else if (type_ == ROARING_BITMAP) {
    if (OB_ISNULL(roaring_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("roaring_bitmap_ is null", K(ret));
    } else {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(roaring_bitmap_, vid));
      if (OB_SUCC(ret)) {
        valid_cnt_++;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter type", K(ret), K(type_));
  }
  return ret;
}

int ObVecIndexBitmap::upgrade_to_byte_array()
{
  int ret = OB_SUCCESS;

  if (type_ != VIDS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can only upgrade from VIDS type", K(ret), K(type_));
  } else if (OB_ISNULL(vids_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vec index bitmap", K(ret));
  } else if (valid_cnt_ == 0) {
    allocator_->free(vids_);
    vids_ = nullptr;
    type_ = BYTE_ARRAY;
    bitmap_ = nullptr;
    capacity_ = 0;
  } else if (max_vid_ < min_vid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vid range", K(ret), K(min_vid_), K(max_vid_));
  } else {
    int64_t vid_range = max_vid_ - min_vid_ + 1;
    if (OB_UNLIKELY(vid_range <= 0)) {
      if (OB_FAIL(upgrade_to_roaring_bitmap())) {
        LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
      }
    } else {
      uint64_t bitmap_size = static_cast<uint64_t>(vid_range + 7) / 8;

      if (bitmap_size > NORMAL_BITMAP_MAX_SIZE) {
        if (OB_FAIL(upgrade_to_roaring_bitmap())) {
          LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
        }
      } else {
        uint8_t *new_bitmap = nullptr;
        if (OB_ISNULL(new_bitmap = static_cast<uint8_t *>(allocator_->alloc(sizeof(uint8_t) * bitmap_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc bitmap", K(ret), K(bitmap_size));
        } else {
          memset(new_bitmap, 0, sizeof(uint8_t) * bitmap_size);
          for (uint64_t i = 0; OB_SUCC(ret) && i < valid_cnt_; i++) {
            int64_t vid = vids_[i];
            int64_t real_idx = vid - min_vid_;
            if (OB_UNLIKELY(real_idx < 0 || static_cast<uint64_t>(real_idx >> 3) >= bitmap_size)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("vid out of bitmap range", K(ret), K(vid), K(min_vid_), K(max_vid_), K(real_idx), K(bitmap_size));
            } else {
              new_bitmap[real_idx >> 3] |= uint8_t(0x1 << (real_idx & 0x7));
            }
          }

          if (OB_SUCC(ret)) {
            capacity_ = vid_range;
            allocator_->free(vids_);
            vids_ = nullptr;
            bitmap_ = new_bitmap;
            type_ = BYTE_ARRAY;
          } else if (OB_NOT_NULL(new_bitmap)) {
            allocator_->free(new_bitmap);
            new_bitmap = nullptr;
          }
        }
      }
    }
  }

  return ret;
}

int ObVecIndexBitmap::upgrade_to_roaring_bitmap()
{
  int ret = OB_SUCCESS;

  roaring::api::roaring64_bitmap_t *new_bitmap = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
  ROARING_TRY_CATCH(new_bitmap = roaring::api::roaring64_bitmap_create());
  if (OB_SUCC(ret) && OB_ISNULL(new_bitmap)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create insert bitmap", K(ret));
  } else if (ret == OB_ALLOCATE_MEMORY_FAILED) {
    new_bitmap = nullptr;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (type_ == VIDS && OB_NOT_NULL(vids_)) {
    for (uint64_t i = 0; i < valid_cnt_ && OB_SUCC(ret); i++) {
      ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(new_bitmap, vids_[i]));
    }
  } else if (type_ == BYTE_ARRAY && OB_NOT_NULL(bitmap_)) {
    for (uint64_t i = 0; i < capacity_ / 8 && OB_SUCC(ret); i++) {
      if (bitmap_[i]) {
        for (uint64_t j = 0; j < 8 && OB_SUCC(ret); j++) {
          if (bitmap_[i] & (1 << j)) {
            uint64_t val = i * 8 + j + min_vid_;
            ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(new_bitmap, val));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_bitmap)) {
    roaring::api::roaring64_bitmap_free(new_bitmap);
    new_bitmap = nullptr;
  }

  if (OB_SUCC(ret)) {
    if (type_ == VIDS && OB_NOT_NULL(vids_)) {
      allocator_->free(vids_);
      vids_ = nullptr;
    } else if (type_ == BYTE_ARRAY && OB_NOT_NULL(bitmap_)) {
      allocator_->free(bitmap_);
      bitmap_ = nullptr;
    }
    roaring_bitmap_ = new_bitmap;
    type_ = ROARING_BITMAP;
    capacity_ = 0;
  }

  return ret;
}

bool ObVecIndexBitmap::test(int64_t vid)
{
  bool bret = false;
  if (type_ == VIDS) {
    // do nothing
  } else if (type_ == BYTE_ARRAY) {
    if (vid >= min_vid_ && vid <= max_vid_) {
      int64_t real_idx = vid - min_vid_;
      bret = ((bitmap_[real_idx >> 3] & (0x1 << (real_idx & 0x7))) != 0);
    }
  } else if (type_ == ROARING_BITMAP) {
    bret = roaring::api::roaring64_bitmap_contains(roaring_bitmap_, vid);
  }
  return bret;
}

void ObVecIndexBitmap::reset()
{
  if (type_ == VIDS) {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(vids_)) {
      allocator_->free(vids_);
      vids_ = nullptr;
    }
  } else if (type_ == BYTE_ARRAY) {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(bitmap_)) {
      allocator_->free(bitmap_);
      bitmap_ = nullptr;
    }
  } else if (type_ == ROARING_BITMAP) {
    if (OB_NOT_NULL(roaring_bitmap_)) {
      lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmapADPV"));
      roaring::api::roaring64_bitmap_free(roaring_bitmap_);
      roaring_bitmap_ = nullptr;
    }
  }
  type_ = VIDS;
  capacity_ = 0;
  valid_cnt_ = 0;
  min_vid_ = INT64_MAX;
  max_vid_ = INT64_MIN;
}

int ObVecIndexBitmapIter::init(ObVecIndexBitmap *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (bitmap->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("bitmap iterator only supports ROARING_BITMAP type", K(ret), K(bitmap->type_));
  } else if (OB_ISNULL(bitmap->roaring_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("roaring bitmap is null", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iterator already inited", K(ret));
  } else {
    ROARING_TRY_CATCH(iter_ = roaring::api::roaring64_iterator_create(bitmap->roaring_bitmap_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to create roaring bitmap iterator", K(ret));
    } else if (OB_ISNULL(iter_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create roaring bitmap iterator", K(ret));
    } else if (!roaring::api::roaring64_iterator_has_value(iter_)) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("roaring bitmap iterator has no value", K(ret));
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
      bitmap_ = bitmap;
      inited_ = true;
    }
  }
  return ret;
}

int ObVecIndexBitmapIter::advance_to(int64_t target_vid, int64_t &return_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_ISNULL(bitmap_) || bitmap_->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null or not ROARING_BITMAP type", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is null", K(ret));
  } else if (target_vid > curr_vid_) {
    bool has_value = false;
    ROARING_TRY_CATCH(has_value = roaring::api::roaring64_iterator_move_equalorlarger(iter_, target_vid));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to advance iterator", K(ret), K(target_vid));
    } else if (!has_value) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
    }
  } else {
    bool has_next = false;
    ROARING_TRY_CATCH(has_next = roaring::api::roaring64_iterator_advance(iter_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to advance iterator", K(ret), K(target_vid), K(curr_vid_));
    } else if (!has_next) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
      if (curr_vid_ < target_vid) {
        bool has_value = false;
        ROARING_TRY_CATCH(has_value = roaring::api::roaring64_iterator_move_equalorlarger(iter_, target_vid));
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to advance iterator", K(ret), K(target_vid));
        } else if (!has_value) {
          ret = OB_ITER_END;
          curr_vid_ = OB_INVALID_INDEX;
        } else {
          curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
        }
      }
    }
  }

  return_vid = curr_vid_;
  return ret;
}

int ObVecIndexBitmapIter::get_curr_vid(int64_t &curr_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_UNLIKELY(curr_vid_ == OB_INVALID_INDEX)) {
    ret = OB_ITER_END;
  } else {
    curr_vid = curr_vid_;
  }
  return ret;
}

int ObVecIndexBitmapIter::next_vid(int64_t &next_vid)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator not inited", K(ret));
  } else if (OB_ISNULL(bitmap_) || bitmap_->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null or not ROARING_BITMAP type", K(ret));
  } else if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator is null", K(ret));
  } else {
    bool has_next = false;
    ROARING_TRY_CATCH(has_next = roaring::api::roaring64_iterator_advance(iter_));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get next vid", K(ret));
    } else if (!has_next) {
      ret = OB_ITER_END;
      curr_vid_ = OB_INVALID_INDEX;
    } else {
      curr_vid_ = roaring::api::roaring64_iterator_value(iter_);
    }
    next_vid = curr_vid_;
  }
  return ret;
}

void ObVecIndexBitmapIter::reset()
{
  if (OB_NOT_NULL(iter_)) {
    roaring::api::roaring64_iterator_free(iter_);
    iter_ = nullptr;
  }
  bitmap_ = nullptr;
  inited_ = false;
  curr_vid_ = OB_INVALID_INDEX;
}

} // namespace sql
} // namespace oceanbase
