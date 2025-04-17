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

template <typename EncodingItem>
ObEncodingPool<EncodingItem>::ObEncodingPool(const int64_t item_size, const char *label)
  : free_items_(), free_cnt_(0),
    pool_(item_size, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
      common::ObMalloc(label))
{
}

template <typename EncodingItem>
ObEncodingPool<EncodingItem>::~ObEncodingPool()
{
  while (free_cnt_ > 0) {
    free_items_[free_cnt_ - 1]->~EncodingItem();
    --free_cnt_;
  }
}

template <typename EncodingItem>
template <typename T>
inline int ObEncodingPool<EncodingItem>::alloc(T *&item)
{
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(free_cnt_ <= 0)) {
    void *p = pool_.alloc();
    if (NULL == p) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      item = new (p) T();
    }
  } else {
    item = static_cast<T *>(free_items_[free_cnt_ - 1]);
    item->reuse();
    --free_cnt_;
  }
  return ret;
}


template <typename EncodingItem>
inline void ObEncodingPool<EncodingItem>::free(EncodingItem *item)
{
  if (NULL != item) {
    if (free_cnt_ < MAX_FREE_ITEM_CNT) {
      free_items_[free_cnt_] = item;
      ++free_cnt_;
    } else {
      item->~EncodingItem();
      pool_.free(item);
      item = NULL;
    }
  }
}

template <typename EncodingItem>
ObEncodingAllocator<EncodingItem>::ObEncodingAllocator(const int64_t *size_array, const char *label)
  : inited_(false),
    size_index_(0),
    raw_pool_(size_array[size_index_++], label),
    dict_pool_(size_array[size_index_++], label),
    rle_pool_(size_array[size_index_++], label),
    const_pool_(size_array[size_index_++], label),
    int_diff_pool_(size_array[size_index_++], label),
    str_diff_pool_(size_array[size_index_++], label),
    hex_str_pool_(size_array[size_index_++], label),
    str_prefix_pool_(size_array[size_index_++], label),
    column_equal_pool_(size_array[size_index_++], label),
    column_substr_pool_(size_array[size_index_++], label),
    pool_cnt_(0)
{
  for (int64_t i = 0; i < ObColumnHeader::MAX_TYPE; i++) {
    pools_[i] = NULL;
  }
}

template <typename EncodingItem>
int ObEncodingAllocator<EncodingItem>::init()
{
  int ret = common::OB_SUCCESS;
  // can init twice
  if (!inited_) {
    MEMSET(pools_, 0, sizeof(pools_));
    if (OB_FAIL(add_pool(&raw_pool_))
        || OB_FAIL(add_pool(&dict_pool_))
        || OB_FAIL(add_pool(&rle_pool_))
        || OB_FAIL(add_pool(&const_pool_))
        || OB_FAIL(add_pool(&int_diff_pool_))
        || OB_FAIL(add_pool(&str_diff_pool_))
        || OB_FAIL(add_pool(&hex_str_pool_))
        || OB_FAIL(add_pool(&str_prefix_pool_))
        || OB_FAIL(add_pool(&column_equal_pool_))
        || OB_FAIL(add_pool(&column_substr_pool_))) {
      STORAGE_LOG(WARN, "add_pool failed", K(ret));
    } else if (pool_cnt_ != size_index_) {
      ret = common::OB_INNER_STAT_ERROR;
      STORAGE_LOG(WARN, "pool_cnt and size_index not same", K(ret), K_(pool_cnt), K_(size_index));
    } else if (pool_cnt_ != ObColumnHeader::MAX_TYPE) {
      ret = common::OB_INNER_STAT_ERROR;
      STORAGE_LOG(WARN, "not all encoder has pool", K(ret), K_(pool_cnt),
          "encoder cnt", ObColumnHeader::MAX_TYPE);
    } else {
      inited_ = true;
    }
  }
  return ret;
}

template <typename EncodingItem>
template<typename T>
inline int ObEncodingAllocator<EncodingItem>::alloc(T *&item)
{
  int ret = common::OB_SUCCESS;
  item = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    // performance critical, don't check params
    if (OB_FAIL(pools_[T::type_]->alloc(item))) {
      STORAGE_LOG(WARN, "allocate failed", K(ret));
    }
  }
  return ret;
}

template <typename EncodingItem>
inline void ObEncodingAllocator<EncodingItem>::free(EncodingItem *item)
{
  if (NULL != item) {
    // performance critical, don't check params
    pools_[item->get_type()]->free(item);
    item = NULL;
  }
}

template <typename EncodingItem>
int ObEncodingAllocator<EncodingItem>::add_pool(Pool *pool)
{
  int ret = common::OB_SUCCESS;
  if (NULL == pool) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "pool is null", K(ret));
  } else if (pool_cnt_ >= ObColumnHeader::MAX_TYPE) {
    ret = common::OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "pool array size overflow", K(ret), K_(pool_cnt),
        "pool array size", ObColumnHeader::MAX_TYPE);
  } else {
    pools_[pool_cnt_] = pool;
    ++pool_cnt_;
  }
  return ret;
}
