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
  tmp_sort_rows_.set_allocator(&alloc);
  if (rows_end - rows_begin <= 0) {
    // do nothing
  } else if (rows_begin < 0 || rows_end > sort_rows.count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(rows_begin), K(rows_end), K(sort_rows.count()), K(ret));
  } else if (OB_FAIL(sort_rows_.prepare_allocate(rows_end - rows_begin))) {
    SQL_ENG_LOG(WARN, "failed to init sort_rows_", K(ret));
  } else if (OB_FAIL(tmp_sort_rows_.prepare_allocate(rows_end - rows_begin))) {
    SQL_ENG_LOG(WARN, "failed to init tmp_sort_rows_", K(ret));
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
 * =============================================
 * AQS is a hybrid sorting algorithm that combines 3-way partitioning quicksort
 * with radix sort, optimized for sorting variable-length byte strings.
 *
 * Key Features:
 * 1. Uses two arrays (sort_rows_ and tmp_sort_rows_) that swap roles during sorting
 * 2. Equal elements stay in source array (already sorted, no further processing needed)
 * 3. Less-than and greater-than elements go to target array for recursive sorting
 * 4. Uses 2-byte cache (sub_cache_) to accelerate comparisons
 * 5. Small arrays (< 16 elements): Use insertion sort
 *
 * Algorithm Flow:
 * ===============
 *
 * Phase 1: 3-Way Partitioning (aqs_cps_qs)
 * ----------------------------------------
 * Input:  source array [a1, a2, pivot, a3, a4, ...]
 *                           ↓
 *         Select pivot (median-of-three), compare all elements
 *                           ↓
 * Output: ┌─────────────────────────────────────────────────┐
 *         │ source: [   eq   ] (done, no more processing)  │
 *         │ target: [lt][lt]  ...gap...  [gt][gt][gt]       │
 *         └─────────────────────────────────────────────────┘
 *         Note: gt elements are written backwards and need reverse
 *
 * Phase 2: Radix Distribution (aqs_radix → radixsort_by_byte)
 * -----------------------------------------------------------
 * For elements that need further sorting (lt and gt parts):
 *
 * 1. Update cache with next byte after common_prefix
 * 2. Count frequency of each byte value (0-255)
 * 3. Distribute into 256 buckets based on byte value
 *
 *    source: [e1,e2,e3,e4,e5,...]
 *              ↓   ↓  ↓  ↓  ↓
 *    target: [bucket₀][bucket₁]...[bucket₂₅₅]
 *              ↓
 *    Each non-empty bucket recursively goes to Phase 1
 *
 * Dual-Array Swapping Mechanism:
 * ==============================
 * The 'swap' parameter controls which array is source/target:
 *
 *   swap=false:  source=sort_rows_,     target=tmp_sort_rows_
 *   swap=true:   source=tmp_sort_rows_, target=sort_rows_
 *
 * Data Flow Example (3 levels of recursion):
 * ------------------------------------------
 *   Level 0 (swap=false):
 *     Read from:  sort_rows_     (source)
 *     Write to:   tmp_sort_rows_ (target - lt/gt parts)
 *     eq stays in: sort_rows_    (source - eq part)
 *
 *   Level 1 (swap=true):
 *     Read from:  tmp_sort_rows_ (source)
 *     Write to:   sort_rows_     (target - lt/gt parts)
 *     eq stays in: tmp_sort_rows_(source - eq part)
 *
 *   Level 2 (swap=false):
 *     Read from:  sort_rows_     (source)
 *     Write to:   tmp_sort_rows_ (target - lt/gt parts)
 *     eq stays in: sort_rows_    (source - eq part)
 *
 * Finally: Copy all row_ptr_ back to sort_rows_ to get final sorted result
 */
template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::aqs_cps_qs(int64_t l, int64_t r, int64_t common_prefix,
                                         int64_t cache_offset, bool swap)
{
  common::ObFixedArray<AQSItem, common::ObIAllocator> &source = swap ? tmp_sort_rows_ : sort_rows_;
  common::ObFixedArray<AQSItem, common::ObIAllocator> &target = swap ? sort_rows_ : tmp_sort_rows_;
  if (OB_UNLIKELY(l >= r)) {
  } else if ((r - l) < 16) {
    insertion_sort(l, r, common_prefix, cache_offset, swap);
    if (swap) {
      for (int64_t i = l; i < r; i++) {
        // todo: optimize this
        sort_rows_.at(i).row_ptr_ = tmp_sort_rows_.at(i).row_ptr_;
      }
    }
  } else {
    int64_t differ_at = INT64_MAX;

    // 三数取中选pivot
    AQSItem pivot;
    int64_t m = l + (r - l) / 2;
    int cmp_lm = compare_vals(source.at(l), source.at(m), differ_at, common_prefix, cache_offset);
    int cmp_mr = compare_vals(source.at(m), source.at(r - 1), differ_at, common_prefix, cache_offset);
    int cmp_lr = compare_vals(source.at(l), source.at(r - 1), differ_at, common_prefix, cache_offset);
    if ((cmp_lm <= 0 && cmp_mr <= 0) || (cmp_lm >= 0 && cmp_mr >= 0)) {
      pivot = source.at(m);
    } else if ((cmp_lm >= 0 && cmp_lr <= 0) || (cmp_lm <= 0 && cmp_lr >= 0)) {
      pivot = source.at(l);
    } else {
      pivot = source.at(r - 1);
    }

    // 划分lt/eq/gt三个部分
    int64_t lt_count = 0, eq_count = 0, gt_count = 0;
    int64_t lt_cp = INT64_MAX, gt_cp = INT64_MAX;

    for (int64_t i = l; i < r; i++) {
      int cmp = compare_vals(source.at(i), pivot, differ_at,
                             common_prefix, cache_offset);
      if (cmp < 0) {
        target.at(l + lt_count++) = source.at(i);
        lt_cp = std::min(differ_at, lt_cp);
      } else if (cmp == 0) {
        source.at(l + eq_count++) = source.at(i);
      } else {
        target.at(r - gt_count - 1) = source.at(i);
        gt_count++;
        gt_cp = std::min(differ_at, gt_cp);
      }
    }

    // gt need to be reversed, to keep the original order
    std::reverse(target.begin() + (r - gt_count), target.begin() + r);

    for (int64_t i = eq_count - 1; i >= 0; i--) {
      source.at(l + lt_count + i).row_ptr_ = source.at(l + i).row_ptr_;
    }

    if (swap) {
      for (int64_t i = 0; i < eq_count; i++) {
        sort_rows_.at(l + lt_count + i).row_ptr_ = tmp_sort_rows_.at(l + lt_count + i).row_ptr_;
      }
    }

    // 递归处理 lt 和 gt
    if (lt_count > 0) {
      aqs_radix(l, l + lt_count, lt_cp, cache_offset, !swap);
    }
    if (gt_count > 0) {
      aqs_radix(l + lt_count + eq_count, r, gt_cp, cache_offset, !swap);
    }
  }
}

template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::insertion_sort(int64_t l, int64_t r, int64_t common_prefix,
                                             int64_t cache_offset, bool swap)
{
  common::ObFixedArray<AQSItem, common::ObIAllocator> &arr = swap ? tmp_sort_rows_ : sort_rows_;
  for (int64_t i = l + 1; i < r; i++) {
    int64_t idx = i;
    int64_t differ_at = 0;
    while ((idx - 1) >= l
           && compare_vals(arr.at(idx), arr.at(idx - 1), differ_at, common_prefix, cache_offset) < 0) {
      std::swap(arr.at(idx), arr.at(idx - 1));
      idx--;
    }
  }
}

template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::aqs_radix(int64_t l, int64_t r, int64_t common_prefix, int64_t offset, bool swap)
{
  common::ObFixedArray<AQSItem, common::ObIAllocator> &source = swap ? tmp_sort_rows_ : sort_rows_;
  common::ObFixedArray<AQSItem, common::ObIAllocator> &target = swap ? sort_rows_ : tmp_sort_rows_;

  int done_pos = l;
  int cache_offset = offset;

  for (int64_t i = l; i < r; i++) {
    if (source.at(i).len_ == common_prefix) {
      std::swap(source[i], source[done_pos++]);
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
      // cache 有效，不更新
    } else if (val == 1) {
      // only last byte of the cache is effective
      source.at(i).sub_cache_[0] = source.at(i).sub_cache_[1];
      cache_offset = common_prefix;
    } else {
      unsigned char *x = ((unsigned char *)(source.at(i).key_ptr_)) + common_prefix;
      source.at(i).sub_cache_[0] = *x;
      cache_offset = common_prefix;
    }
  }

  radixsort_by_byte(done_pos, r, common_prefix, cache_offset, swap);
  if (swap) {
    for (int64_t i = l; i < done_pos; i++) {
      sort_rows_.at(i).row_ptr_ = tmp_sort_rows_.at(i).row_ptr_;
    }
  }
}

template <typename Store_Row>
void ObAdaptiveQS<Store_Row>::radixsort_by_byte(int64_t l, int64_t r,
                                                 int64_t common_prefix,
                                                 int64_t cache_offset,
                                                 bool swap)
{
  common::ObFixedArray<AQSItem, common::ObIAllocator> &source = swap ? tmp_sort_rows_ : sort_rows_;
  common::ObFixedArray<AQSItem, common::ObIAllocator> &target = swap ? sort_rows_ : tmp_sort_rows_;

  if (l >= r) {
  } else if (l + 1 == r) {
    if (swap) {
      sort_rows_.at(l).row_ptr_ = tmp_sort_rows_.at(l).row_ptr_;
    }
  } else {
    int64_t locations[RADIX_LOCATIONS] = {0};
    int64_t *counts = locations + 1;
    for (int64_t i = l; i < r; i++) {
      unsigned char byte_val = source.at(i).sub_cache_[0];
      counts[byte_val]++;
    }
    int64_t max_count = 0;
    for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
      max_count = std::max(max_count, counts[radix]);
      counts[radix] += locations[radix];
    }

    if (max_count != (r - l)) {
      // 基数排序到辅助数组
      for (int64_t i = l; i < r; i++) {
        unsigned char byte_val = source.at(i).sub_cache_[0];
        target.at(l + locations[byte_val]) = source.at(i);
        locations[byte_val]++;
      }
      swap = !swap;

      // 对每一个基数做aqs
      int64_t radix_count = locations[0];
      for (int64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        if (radix_count != 0) {
          // update cache
          int64_t new_l = (locations[radix] - radix_count) + l;
          int64_t new_r = new_l + radix_count;
          int more_l = new_l;
          for (int64_t i = new_l; i < new_r; i++) {
            if (target.at(i).len_ == common_prefix + 1) {
              std::swap(target.at(i), target.at(more_l++));
            } else {
              unsigned char *x = ((unsigned char *)(target.at(i).key_ptr_)) + common_prefix;
              target.at(i).sub_cache_[0] = *(x + 1);
              target.at(i).sub_cache_[1] = (common_prefix + 2 == target.at(i).len_) ? 0x00 : *(x + 2);
            }
          }
          if (more_l < new_r) {
            aqs_cps_qs(more_l, new_r, common_prefix + 1, cache_offset + 1, swap);
          }
          if (swap) {
            for (int64_t i = new_l; i < more_l; i++) {
              sort_rows_.at(i).row_ptr_ = tmp_sort_rows_.at(i).row_ptr_;
            }
          }
        }
        radix_count = locations[radix + 1] - locations[radix];
      }
    } else {
      // update cache
      int more_l = l;
      for (int64_t i = l; i < r; i++) {
        if (source.at(i).len_ == common_prefix + 1) {
          std::swap(source.at(i), source.at(more_l++));
        } else {
          unsigned char *x = ((unsigned char *)(source.at(i).key_ptr_)) + common_prefix;
          source.at(i).sub_cache_[0] = *(x + 1);
          source.at(i).sub_cache_[1] = (common_prefix + 2 == source.at(i).len_) ? 0x00 : *(x + 2);
        }
      }
      if (swap) {
        for (int64_t i = l; i < more_l; i++) {
          sort_rows_.at(i).row_ptr_ = tmp_sort_rows_.at(i).row_ptr_;
        }
      }
      aqs_cps_qs(more_l, r, common_prefix + 1, cache_offset + 1, swap);
    }
  }
  return;
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
int ObAdaptiveQS<Store_Row>::compare_vals(AQSItem &l, AQSItem &r, int64_t &differ_at,
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