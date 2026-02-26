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
#include "sql/engine/ob_batch_rows.h"
#include "share/ob_batch_selector.h"
#include "lib/container/ob_array_wrap.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

void ObBatchSelector::set_batch_rows(const sql::ObBatchRows &brs)
{
  if (brs.all_rows_active_) {
    type_ = CONTINIOUS_LENGTH;
    offset_ = 0;
  } else {
    type_ = SKIP_BITMAP;
    skip_ = brs.skip_;
  }
  count_ = brs.size_;
  cursor_ = 0;
}

void ObBatchSelector::set_continous_rows(const int64_t offset, const int64_t row_count)
{
  type_ = CONTINIOUS_LENGTH;
  offset_ = offset;
  count_ = row_count;
  cursor_ = 0;
}

void ObBatchSelector::set_active_array(const uint16_t *active_array, const int64_t row_count)
{
  type_ = ACTIVE_ARRAY_U16;
  u16_array_ = active_array;
  count_ = row_count;
  cursor_ = 0;
}

void ObBatchSelector::set_active_array(const uint32_t *active_array, const int64_t row_count)
{
  type_ = ACTIVE_ARRAY_U32;
  u32_array_ = active_array;
  count_ = row_count;
  cursor_ = 0;
}

void ObBatchSelector::set_active_array(const uint64_t *active_array, const int64_t row_count)
{
  type_ = ACTIVE_ARRAY_U64;
  u64_array_ = active_array;
  count_ = row_count;
  cursor_ = 0;
}

int64_t ObBatchSelector::get_end() const
{
  int64_t max = 0;
  switch (type_) {
    case CONTINIOUS_LENGTH:
      max = offset_ + count_;
      break;
    case SKIP_BITMAP:
      max = count_;
      break;
    case ACTIVE_ARRAY_U16:
      if (count_ > 0) {
        max = u16_array_[count_ - 1] + 1;
      }
      break;
    case ACTIVE_ARRAY_U32:
      if (count_ > 0) {
        max = u32_array_[count_ - 1] + 1;
      }
      break;
    case ACTIVE_ARRAY_U64:
      if (count_ > 0) {
        max = u64_array_[count_ - 1] + 1;
      }
      break;
    default:
      break;
  }
  return max;
}

int ObBatchSelector::get_next(int64_t &offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(cursor_ >= count_)) {
    ret = OB_ITER_END;
  } else {
    switch (type_) {
      case CONTINIOUS_LENGTH:
      {
        offset = offset_ + cursor_++;
        break;
      }
      case SKIP_BITMAP:
      {
        while (cursor_ < count_ && skip_->at(cursor_)) {
          ++cursor_;
        }
        offset = cursor_++;
        break;
      }
      case ACTIVE_ARRAY_U16:
      {
        offset = u16_array_[cursor_++];
        break;
      }
      case ACTIVE_ARRAY_U32:
      {
        offset = u32_array_[cursor_++];
        break;
      }
      case ACTIVE_ARRAY_U64:
      {
        offset = u64_array_[cursor_++];
        break;
      }
      default:
      {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support selector type", K(ret), K(type_));
        break;
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObBatchSelector)
{
  int64_t pos = 0;
  J_OBJ_START();
  switch (type_) {
    case CONTINIOUS_LENGTH:
    {
      J_KV("type", "CONTINIOUS_LENGTH", K_(offset), K_(count), K_(cursor));
      break;
    }
    case SKIP_BITMAP:
    {
      J_KV("type", "SKIP_BITMAP",
           "skip_bit_vec", ObLogPrintHex(reinterpret_cast<const char *>(skip_), NULL == skip_ ? 0 : sql::ObBitVector::memory_size(count_)),
           K_(count), K_(cursor));
      break;
    }
    case ACTIVE_ARRAY_U16:
    {
      J_KV("type", "ACTIVE_ARRAY_U16", "u16_array", common::ObArrayWrap<uint16_t>(u16_array_, count_), K_(count), K_(cursor));
      break;
    }
    case ACTIVE_ARRAY_U32:
    {
      J_KV("type", "ACTIVE_ARRAY_U32", "u16_array", common::ObArrayWrap<uint32_t>(u32_array_, count_), K_(count), K_(cursor));
      break;
    }
    case ACTIVE_ARRAY_U64:
    {
      J_KV("type", "ACTIVE_ARRAY_U64", "u16_array", common::ObArrayWrap<uint64_t>(u64_array_, count_), K_(count), K_(cursor));
      break;
    }
    default:
    {
      J_KV(K_(type), "ptr", (void *)skip_, K_(count), K_(cursor));
      break;
    }
  }
  J_OBJ_END();
  return pos;
}
