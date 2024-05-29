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

#include "storage/tx_table/ob_tx_data_hash_map.h"
#include "storage/tx/ob_tx_data_define.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace storage {

void ObTxDataHashMap::destroy()
{
  if (OB_NOT_NULL(buckets_)) {
    ObTxData *curr = nullptr;
    ObTxData *next = nullptr;
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      curr = buckets_[i].next_;
      buckets_[i].next_ = nullptr;
      while (OB_NOT_NULL(curr)) {
        next = curr->hash_node_.next_;
        curr->dec_ref();
        curr = next;
      }
    }
    allocator_.free(buckets_);
    total_cnt_ = 0;
  }
}

int ObTxDataHashMap::init()
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = BUCKETS_CNT * sizeof(ObTxDataHashHeader);
  void *ptr = allocator_.alloc(alloc_size);
  if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory failed when init tx data hash map", KR(ret), K(alloc_size), K(BUCKETS_CNT));
  } else {
    buckets_ = new (ptr) ObTxDataHashHeader[BUCKETS_CNT];
    for (int i = 0; i < BUCKETS_CNT; i++) {
      buckets_[i].reset();
    }
    total_cnt_ = 0;
  }
  return ret;
}

int ObTxDataHashMap::insert(const transaction::ObTransID &key, ObTxData *value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid() || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(key), KP(value));
  } else {
    int64_t pos = get_pos(key);

    if (OB_UNLIKELY(ObTxData::ExclusiveType::NORMAL != value->exclusive_flag_)) {
      if (ObTxData::ExclusiveType::EXCLUSIVE != value->exclusive_flag_) {
        STORAGE_LOG(ERROR, "invalid exclusive flag", KPC(value));
      } else {
        ObTxData *iter = buckets_[pos].next_;
        while (OB_NOT_NULL(iter)) {
          if (iter->contain(key)) {
            iter->exclusive_flag_ = ObTxData::ExclusiveType::DELETED;
          }
          iter = iter->hash_node_.next_;
        }
      }
    }

    // atomic insert this value
    while (true) {
      ObTxData *next_value = ATOMIC_LOAD(&buckets_[pos].next_);
      value->hash_node_.next_ = next_value;
      if (next_value == ATOMIC_CAS(&buckets_[pos].next_, next_value, value)) {
        if (value->inc_ref() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected ref cnt on tx data", KR(ret), KPC(value));
          ob_abort();
        }
        ATOMIC_INC(&total_cnt_);
        break;
      }
    }
  }
  return ret;
}

int ObTxDataHashMap::get(const transaction::ObTransID &key, ObTxDataGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTxData *value = nullptr;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(key));
  } else {
    const int64_t pos = get_pos(key);
    ObTxData *cache_val = buckets_[pos].hot_cache_val_;
    if (OB_NOT_NULL(cache_val) && cache_val->contain(key)) {
      value = cache_val;
    } else {
      ObTxData *tmp_value = nullptr;
      tmp_value = buckets_[pos].next_;
      while (OB_NOT_NULL(tmp_value)) {
        if (tmp_value->contain(key)) {
          value = tmp_value;
          buckets_[pos].hot_cache_val_ = value;
          break;
        } else {
          tmp_value = tmp_value->hash_node_.next_;
        }
      }
    }
  }

  if (OB_ISNULL(value)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(guard.init(value))) {
    STORAGE_LOG(WARN, "get tx data from tx data hash map failed", KR(ret), KP(this), KPC(value));
  }
  return ret;
}

int ObTxDataHashMap::Iterator::get_next(ObTxDataGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTxData *next_val = nullptr;

  while (OB_SUCC(ret) && OB_ISNULL(next_val)) {
    if (OB_NOT_NULL(val_)) {
      next_val = val_;
      val_ = val_->hash_node_.next_;
    } else if (bucket_idx_ >= tx_data_map_.BUCKETS_CNT) {
      ret = OB_ITER_END;
    } else {
      while (++bucket_idx_ < tx_data_map_.BUCKETS_CNT) {
        val_ = tx_data_map_.buckets_[bucket_idx_].next_;

        if (OB_NOT_NULL(val_) && (ObTxData::ExclusiveType::DELETED != val_->exclusive_flag_)) {
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(next_val) && OB_FAIL(guard.init(next_val))) {
    STORAGE_LOG(WARN, "init tx data guard failed when get next from iterator", KR(ret), KPC(next_val));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
