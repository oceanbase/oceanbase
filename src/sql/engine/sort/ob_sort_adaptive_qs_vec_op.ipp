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

namespace oceanbase
{
namespace sql
{
/*********************************** start ObAdaptiveQS **********************************/
template <typename Store_Row>
ObAdaptiveQS<Store_Row>::ObAdaptiveQS(common::ObIArray<Store_Row *> &sort_rows,
                                      const RowMeta &row_meta, common::ObIAllocator &alloc) :
  row_meta_(row_meta),
  orig_sort_rows_(sort_rows), alloc_(alloc)
{}

template <typename Store_Row>
int ObAdaptiveQS<Store_Row>::init(common::ObIArray<Store_Row *> &sort_rows,
                                  common::ObIAllocator &alloc, int64_t rows_begin, int64_t rows_end,
                                  bool &can_encode)
{
  int ret = OB_SUCCESS;
  can_encode = true;
  sort_rows_.set_allocator(&alloc);
  if (rows_end - rows_begin <= 0) {
    // do nothing
  } else if (rows_begin < 0 || rows_end > sort_rows.count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(rows_begin), K(rows_end), K(sort_rows.count()), K(ret));
  } else if (OB_FAIL(sort_rows_.prepare_allocate(rows_end - rows_begin))) {
    SQL_ENG_LOG(WARN, "failed to init", K(ret));
  } else {
    for (int64_t i = 0; can_encode && i < rows_end - rows_begin; i++) {
      AQSItem &item = sort_rows_[i];
      const Store_Row *row = sort_rows.at(i + rows_begin);
      if (row->is_null(0)) {
        can_encode = false;
        break;
      }
      item.key_ptr_ = (unsigned char *)row->get_cell_payload(row_meta_, 0);
      item.len_ = row->get_length(row_meta_, 0);
      item.row_ptr_ = sort_rows.at(i + rows_begin);
      if (item.len_ > 0)
        item.sub_cache_[0] = item.key_ptr_[0];
      if (item.len_ > 1)
        item.sub_cache_[1] = item.key_ptr_[1];
    }
  }
  return ret;
}

/*
 * AQS sort performance as follows:
 *
 *  step1:         | datas |<---------------------------+
 *   quicksort         |                                |
 *      +--------------+-------------+                  |
 *      |              |             |                  |
 *  less than(lt)   equal to(eq)    great than(gt)      |
 *      |                                               |
 *step2: do radix sort and distribute buckets           |
 *       for each buckets redo this process             |
 *  |bucket0| ... |bucket255|                           |
 *            |                                         |
 *            +-----------------------------------------+
 */
template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::aqs_cps_qs(int64_t l, int64_t r, int64_t common_prefix,
                                         int64_t depth_limit, int64_t cache_offset)
{
  int64_t lt = l + 1, gt = r, m = (l - r) / 2 + r;
  int64_t differ_at = INT64_MAX, lt_cp = INT64_MAX, gt_cp = INT64_MAX;
  if ((r - l) < 16) {
    insertion_sort(l, r, common_prefix, cache_offset);
    // return;
  } else {
    // choose best pivot
    if (compare_vals(m, l, differ_at, common_prefix, cache_offset) > 0)
      swap(m, l);
    if (compare_vals(l, r - 1, differ_at, common_prefix, cache_offset) > 0)
      swap(l, r - 1);
    if (compare_vals(m, l, differ_at, common_prefix, cache_offset) > 0)
      swap(m, l);

    for (uint64_t i = l + 1; i < gt; i++) {
      int compare_res = compare_vals(i, l, differ_at, common_prefix, cache_offset);
      if (compare_res < 0) {
        if (i + 1 < gt) {
          __builtin_prefetch(sort_rows_.at(i + 1).key_ptr_);
        }
        lt_cp = min(differ_at, lt_cp);
        swap(i, lt);
        lt++;
      } else if (compare_res == 0) {
        if (i + 1 < gt) {
          __builtin_prefetch(sort_rows_.at(i + 1).key_ptr_);
        }
      } else {
        gt_cp = min(differ_at, gt_cp);
        gt--;
        swap(i, gt);
        i--;
      }
    }
    lt--;
    swap(lt, l);
    depth_limit--;
    if (lt != l)
      aqs_radix(l, lt, lt_cp, cache_offset, depth_limit);
    if (gt != r)
      aqs_radix(gt, r, gt_cp, cache_offset, depth_limit);
  }
}

template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::insertion_sort(int64_t l, int64_t r, int64_t common_prefix,
                                             int64_t cache_offset)
{
  for (int i = l + 1; i < r; i++) {
    int64_t idx = i;
    int64_t differ_at = 0;
    while ((idx - 1) >= l
           && compare_vals(idx, idx - 1, differ_at, common_prefix, cache_offset) < 0) {
      swap(idx, idx - 1);
      idx--;
    }
  }
}

template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::aqs_radix(int64_t l, int64_t r, int64_t common_prefix, int64_t offset,
                                        int64_t depth_limit)
{
  int more_pos = l, done_pos = l;
  int cache_offset = offset;

  for (int i = l; i < r; i++) {
    if (sort_rows_.at(i).len_ == common_prefix) {
      swap(i, more_pos);
      swap(more_pos, done_pos);
      more_pos++;
      done_pos++;
      continue;
    }

    /*
     * Update cache policy:
     * we can use following model to interpret key str:
     *  | common prefix | key value | remians str |
     *  key values means the first byte after common prefix.
     *
     *  For the cache which size is 2, there are three scenarioes:
     *  1. Values in the cache are totally ineffective
     *      We just upate it
     *  2. only last byte of the cache is effective
     *     we also needs to update it, since the last byte will be used to do
     * radix sort.
     *  3. first and last byte of the cache is effective
     *     we do not update it, since the last byte is useful for next quick
     * sort.
     */
    // NOTES: we will use sub_cache_[0] to do radix sort
    int val = common_prefix - offset;
    // first and last byte of cache is effective
    if (val == 0) {
      // only last byte of the cache is effective
    } else if (val == 1) {
      sort_rows_.at(i).sub_cache_[0] = sort_rows_.at(i).sub_cache_[1];
      cache_offset = common_prefix + 1;
      // values in the cache are totally ineffective
    } else {
      unsigned char *x = ((unsigned char *)(sort_rows_.at(i).key_ptr_)) + common_prefix;
      sort_rows_.at(i).sub_cache_[0] = *x;
      cache_offset = common_prefix + 1;
    }

    if (sort_rows_.at(i).len_ == common_prefix + 1) {
      swap(i, more_pos);
      more_pos++;
    } else {
    }
  }
  inplace_radixsort_more_bucket(done_pos, r, 7, common_prefix, depth_limit, cache_offset,
                                (common_prefix - offset) != 0);
}

// use dfs to do radix sort
// reference: https://en.wikipedia.org/wiki/Radix_sort
template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::inplace_radixsort_more_bucket(int64_t l, int64_t r, int64_t div_val,
                                                            int64_t common_prefix,
                                                            int64_t depth_limit,
                                                            int64_t cache_offset, bool update)
{
  if (l >= r || l + 1 == r) {
    // do nothing
  } else {
    if (div_val == -1) {
      int more_l = l;
      for (int i = l; i < r; i++) {
        if (sort_rows_.at(i).len_ == common_prefix + 1) {
          swap(more_l, i);
          more_l++;
        }
      }

      // update cache
      if (update) {
        if (more_l < r) {
          __builtin_prefetch((&sort_rows_.at(more_l).len_));
          __builtin_prefetch(((unsigned char *)(sort_rows_.at(more_l).key_ptr_)) + common_prefix);
        }
        for (int i = more_l; i < r; i++) {
          unsigned char *x = ((unsigned char *)(sort_rows_.at(i).key_ptr_)) + common_prefix;
          if (i + 1 < r) {
            __builtin_prefetch((&sort_rows_.at(i + 1).len_));
            __builtin_prefetch(((unsigned char *)(sort_rows_.at(i + 1).key_ptr_)) + common_prefix);
          }
          sort_rows_.at(i).sub_cache_[0] = *(x + 1);
          sort_rows_.at(i).sub_cache_[1] =
            (common_prefix + 2 == sort_rows_.at(i).len_) ? 0x00 : *(x + 2);
        }
      }
      aqs_cps_qs(more_l, r, common_prefix + 1, depth_limit, cache_offset);
      return;
    }

    int divide_line = l;
    __builtin_prefetch((&sort_rows_.at(l).sub_cache_[0]));
    for (int i = l; i < r; i++) {
      // byte b = index_bytes[i];
      if (i + 1 < r)
        __builtin_prefetch((&sort_rows_.at(i + 1).sub_cache_[0]));
      if ((sort_rows_.at(i).sub_cache_[0] & masks[div_val]) == 0) {
        swap(i, divide_line);
        divide_line++;
      }
    }
    inplace_radixsort_more_bucket(l, divide_line, div_val - 1, common_prefix, depth_limit,
                                  cache_offset, update);
    inplace_radixsort_more_bucket(divide_line, r, div_val - 1, common_prefix, depth_limit,
                                  cache_offset, update);
  }
}

template <typename Store_Row>
int ObAdaptiveQS<Store_Row>::fast_cmp_normal(const unsigned char *s, const unsigned char *t,
                                             int64_t length, int64_t &differ_at, int64_t cache_ends)
{
  int cmp_ret = 0;
  for (int i = 0; (cmp_ret == 0) && i < length; i++) {
    if (s[i] != t[i]) {
      differ_at = i + cache_ends;
      cmp_ret = s[i] - t[i];
    }
  }
  return cmp_ret;
}

/*
 * For comparsion:
 *  we orgnized each entry as follows:
 *    | len_ | cache_ | key_ptr_ | row_ptr_ |
 *                       |           |
 *                      key         row
 *  we will use those entry to do comparison:
 *      if cache cannot distinguish those two entry, we will use key_ptr
 *      to index key and use key to do comparison.
 */
template <typename Store_Row>
int ObAdaptiveQS<Store_Row>::compare_cache(AQSItem &l, AQSItem &r, int64_t &differ_at,
                                           int64_t common_prefix, int64_t cache_offset)
{
  int64_t cache_ends = cache_offset + 2;
  int64_t res = 0;
  for (int64_t i = common_prefix; res == 0 && i < cache_ends; i++) {
    int64_t idx = i - cache_offset;
    if (l.sub_cache_[idx] != r.sub_cache_[idx]) {
      differ_at = i;
      res = l.sub_cache_[idx] - r.sub_cache_[idx];
    }
  }

  if (res != 0) {
    // do nothing
  } else {
    unsigned char *item_b = l.key_ptr_;
    unsigned char *pivot_b = r.key_ptr_;
    int64_t len = min(static_cast<int64_t>(l.len_), static_cast<int64_t>(r.len_));
    int64_t stride = 16;
    __builtin_prefetch(pivot_b + stride);
    for (int64_t j = cache_ends; res == 0 && j < len; j += stride) {
      unsigned char *pivot_key = pivot_b + j;
      unsigned char *item_key = item_b + j;

      __builtin_prefetch(pivot_key + stride);
      if (__builtin_expect((j + stride) > len, 0)) {
        for (int i = 0; res == 0 && i < len - j; i++) {
          if (pivot_key[i] != item_key[i]) {
            differ_at = i + j;
            res = item_key[i] - pivot_key[i];
          }
        }
      } else {
        res = cmp_byte_func(item_key, pivot_key, stride, differ_at, j);
      }
    }

    if (res != 0) {
      // do nothing
    } else {
      differ_at = min(static_cast<int64_t>(l.len_), static_cast<int64_t>(r.len_));
      res = l.len_ - r.len_;
    }
  }
  return res;
}

template <typename Store_Row>
int ObAdaptiveQS<Store_Row>::compare_vals(int64_t l, int64_t r, int64_t &differ_at,
                                          int64_t common_prefix, int64_t cache_offset)
{
  return compare_cache(sort_rows_.at(l), sort_rows_.at(r), differ_at, common_prefix, cache_offset);
}

/*********************************** end ObAdaptiveQS **********************************/

template <typename StoreRow, typename SortingItem>
ObFixedKeySort<StoreRow, SortingItem>::ObFixedKeySort(
	common::ObIArray<StoreRow *> &sort_rows, const RowMeta &row_meta,
	common::ObIAllocator &alloc)
		: row_meta_(row_meta), orig_sort_rows_(sort_rows), alloc_(alloc),
			buf_(nullptr), sorting_items_(nullptr), tmp_items_(nullptr),
			item_cnt_(0), key_size_(0), item_size_(0), buckets_(nullptr)
{}

template <typename StoreRow, typename SortingItem>
int ObFixedKeySort<StoreRow, SortingItem>::init(common::ObIArray<StoreRow *> &sort_rows,
    common::ObIAllocator &alloc, int64_t rows_begin, int64_t rows_end, bool &can_encode)
{
  int ret = OB_SUCCESS;
  can_encode = true;
  if (rows_end - rows_begin <= 0) {
    // do nothing
  } else if (rows_begin < 0 || rows_end > sort_rows.count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(rows_begin), K(rows_end), K(sort_rows.count()), K(ret));
  } else if (OB_FAIL(prepare_sorting_items(rows_begin, rows_end))) {
    SQL_ENG_LOG(WARN, "failed to prepare items", K(ret));
  } else {
    for (int64_t i = 0; can_encode && i < item_cnt_; i++) {
      SortingItem &item = sorting_items_[i];
      StoreRow *row = sort_rows.at(i + rows_begin);
      if (row->is_null(0)) {
        can_encode = false;
        break;
      }
      item.init(row, row_meta_);
    }
  }
  return ret;
}

template <typename StoreRow, typename SortingItem>
int ObFixedKeySort<StoreRow, SortingItem>::prepare_sorting_items(
    int64_t rows_begin, int64_t rows_end)
{
  int ret = OB_SUCCESS;
  key_size_ = SortingItem::KeyType::get_key_size();
  item_size_ = SortingItem::get_item_size();
  item_cnt_ = rows_end - rows_begin;
  SQL_ENG_LOG(DEBUG, "prepare sorting items", K(key_size_), K(item_size_), K(item_cnt_));
  int64_t items_size = item_size_ * item_cnt_;
  int64_t buckets_size = RADIX_LOCATIONS * sizeof(int64_t) * key_size_;
  buf_ = reinterpret_cast<DataPtr>(alloc_.alloc(items_size * 2 + buckets_size));
  if (OB_ISNULL(buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc memory", K(ret));
  } else {
    memset(buf_, 0, items_size * 2 + buckets_size);
    sorting_items_ = reinterpret_cast<SortingItem *>(buf_);
    tmp_items_ = reinterpret_cast<SortingItem *>(buf_ + items_size);
    buckets_ = reinterpret_cast<int64_t *>(buf_ + items_size * 2);
  }
  return ret;
}

template <typename StoreRow, typename SortingItem>
void ObFixedKeySort<StoreRow, SortingItem>::reset()
{
  if (buf_ != nullptr) {
    alloc_.free(buf_);
    buf_ = nullptr;
    sorting_items_ = nullptr;
    tmp_items_ = nullptr;
  }
  item_cnt_ = 0;
  key_size_ = 0;
  item_size_ = 0;
}

template <typename StoreRow, typename SortingItem>
void ObFixedKeySort<StoreRow, SortingItem>::insertion_sort(const DataPtr orig_ptr,
    const int64_t count)
{
  const SortingItem *source_ptr = reinterpret_cast<const SortingItem *>(orig_ptr);
  if (count > 1) {
    for (int64_t i = 1; i < count; i++) {
      int64_t j = i;
      SortingItem insert_item = source_ptr[i];
      while (j > 0) {
        const SortingItem &cur_item = source_ptr[j - 1];
        if (cur_item.key_.key_cmp(insert_item.key_) > 0) {
          memcpy((void *)(source_ptr + j), (void *)(source_ptr + (j - 1)), item_size_);
          j--;
        } else {
          break;
        }
      }
      memcpy((void *)(source_ptr + j), (void *)(&insert_item), item_size_);
    }
  }
}
template <typename StoreRow, typename SortingItem>
void ObFixedKeySort<StoreRow, SortingItem>::radix_sort(const DataPtr orig_ptr,
    const DataPtr tmp_ptr, const int64_t count, const int64_t offset, int64_t *locations, bool swap)
{
  if (0 == count || offset == key_size_) {
    // if swap is true, orig_ptr and tmp_ptr is swaped
    if (swap) {
      memcpy(tmp_ptr, orig_ptr, count * item_size_);
    }
  } else if (count <= INSERTION_SORT_THRESHOLD) {
    insertion_sort(orig_ptr, count);
    if (swap) {
      memcpy(tmp_ptr, orig_ptr, count * item_size_);
    }
  } else {
    int64_t real_offset;
    real_offset = offset;
    const DataPtr source_ptr = orig_ptr;
    const DataPtr target_ptr = tmp_ptr;
    // init counts to 0
    memset(locations, 0, RADIX_LOCATIONS * sizeof(int64_t));
    int64_t *counts = locations + 1;
    // collect counts
    DataPtr offset_ptr = source_ptr + real_offset;
    for (int64_t i = 0; i < count; i++) {
      counts[*offset_ptr]++;
      offset_ptr += item_size_;
    }
    // compute locations from buckets
    int64_t max_count = 0;
    for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
      max_count = std::max(max_count, counts[radix]);
      counts[radix] += locations[radix];
    }
    if (max_count != count) {
      // reorder items into tmp array
      DataPtr item_ptr = source_ptr;
      for (int64_t i = 0; i < count; i++) {
        const int64_t &radix_offset = locations[*(item_ptr + real_offset)]++;
        memcpy(target_ptr + radix_offset * item_size_, item_ptr, item_size_);
        item_ptr += item_size_;
      }
      swap = !swap;
    }
    if (max_count == count) {
      radix_sort(orig_ptr, tmp_ptr, count, offset + 1, locations + RADIX_LOCATIONS, swap);
    } else {
      int64_t radix_count = locations[0];
      for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        if (radix_count != 0) {
          const int64_t loc = (locations[radix] - radix_count) * item_size_;
          radix_sort(tmp_ptr + loc,
              orig_ptr + loc,
              radix_count,
              offset + 1,
              locations + RADIX_LOCATIONS,
              swap);
        }
        radix_count = locations[radix + 1] - locations[radix];
      }
    }
  }
}

} // namespace sql
} // namespace oceanbase