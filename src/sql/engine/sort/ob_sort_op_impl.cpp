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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_sort_op_impl.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

/************************************* start ObSortOpImpl *********************************/
ObSortOpImpl::ObAdaptiveQS::ObAdaptiveQS(common::ObIArray<ObChunkDatumStore::StoredRow *> &sort_rows,
                                         common::ObIAllocator &alloc)
  : orig_sort_rows_(sort_rows),
    alloc_(alloc)
{
}

int ObSortOpImpl::ObAdaptiveQS::init(common::ObIArray<ObChunkDatumStore::StoredRow *> &sort_rows,
                                         common::ObIAllocator &alloc, int64_t rows_begin,
                                         int64_t rows_end, bool &can_encode)
{
  int ret = OB_SUCCESS;
  can_encode = true;
  sort_rows_.set_allocator(&alloc);
  if (rows_end - rows_begin <= 0) {
    // do nothing
  } else if (rows_begin < 0 || rows_end > sort_rows.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rows_begin), K(rows_end), K(sort_rows.count()), K(ret));
  } else if (OB_FAIL(sort_rows_.prepare_allocate(rows_end - rows_begin))) {
    LOG_WARN("failed to init", K(ret));
  } else {
    for (int64_t i = 0; can_encode && i < rows_end - rows_begin; i++) {
      AQSItem &item = sort_rows_[i];
      ObDatum cell = sort_rows.at(i + rows_begin)->cells()[0];
      if (cell.is_null()) {
        can_encode=false;
        break;
      }
      item.key_ptr_ = (unsigned char *)cell.ptr_;
      item.len_ = cell.len_;
      item.row_ptr_ = sort_rows.at(i + rows_begin);
      if (item.len_>0) item.sub_cache_[0] = item.key_ptr_[0];
      if (item.len_>1) item.sub_cache_[1] = item.key_ptr_[1];
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
void ObSortOpImpl::ObAdaptiveQS::aqs_cps_qs(int64_t l, int64_t r,
                                            int64_t common_prefix,
                                            int64_t depth_limit,
                                            int64_t cache_offset)
{
  int64_t lt = l + 1, gt = r, m = (l - r) / 2 + r;
  int64_t differ_at = INT64_MAX, lt_cp = INT64_MAX, gt_cp = INT64_MAX;
  if ((r - l) < 16) {
    insertion_sort(l, r, common_prefix, cache_offset);
    //return;
  } else {
    // choose best pivot
    if (compare_vals(m, l, differ_at, common_prefix, cache_offset) > 0) swap(m, l);
    if (compare_vals(l, r-1, differ_at, common_prefix, cache_offset) > 0) swap(l, r-1);
    if (compare_vals(m, l, differ_at, common_prefix, cache_offset) > 0) swap(m, l);

    for (uint64_t i = l+1; i < gt; i++) {
      int compare_res = compare_vals(i, l, differ_at, common_prefix, cache_offset);
      if (compare_res < 0) {
        if (i+1 < gt) {
          __builtin_prefetch(sort_rows_.at(i+1).key_ptr_);
        }
        lt_cp = min(differ_at, lt_cp);
        swap(i, lt);
        lt++;
      } else if (compare_res == 0) {
        if (i+1 < gt) {
          __builtin_prefetch(sort_rows_.at(i+1).key_ptr_);
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
    if (lt != l) aqs_radix(l, lt, lt_cp, cache_offset, depth_limit);
    if (gt != r) aqs_radix(gt, r, gt_cp, cache_offset, depth_limit);
  }
}

void ObSortOpImpl::ObAdaptiveQS::insertion_sort(int64_t l, int64_t r,
                                                  int64_t common_prefix,
                                                  int64_t cache_offset)
{
  for (int i = l + 1; i < r; i++)
  {
    int64_t idx = i;
    int64_t differ_at = 0;
    while ((idx - 1) >= l && compare_vals(idx, idx - 1, differ_at, common_prefix, cache_offset) < 0)
    {
      swap(idx, idx - 1);
      idx--;
    }
  }
}

void ObSortOpImpl::ObAdaptiveQS::aqs_radix(int64_t l, int64_t r,
                                            int64_t common_prefix,
                                            int64_t offset,
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
     *     we also needs to update it, since the last byte will be used to do radix sort.
     *  3. first and last byte of the cache is effective
     *     we do not update it, since the last byte is useful for next quick sort.
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
  inplace_radixsort_more_bucket(done_pos, r, 7, 
                                common_prefix, depth_limit,
                                cache_offset, (common_prefix - offset) != 0);
}

// use dfs to do radix sort
// reference: https://en.wikipedia.org/wiki/Radix_sort
void ObSortOpImpl::ObAdaptiveQS::inplace_radixsort_more_bucket(int64_t l, int64_t r,
                                                                int64_t div_val,
                                                                int64_t common_prefix,
                                                                int64_t depth_limit,
                                                                int64_t cache_offset,
                                                                bool update)
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
        if(more_l < r) {
          __builtin_prefetch((&sort_rows_.at(more_l).len_));
          __builtin_prefetch(((unsigned char *)(sort_rows_.at(more_l).key_ptr_)) + common_prefix);
        }
        for (int i = more_l; i < r; i++) {
          unsigned char *x = ((unsigned char *)(sort_rows_.at(i).key_ptr_)) + common_prefix;
          if ( i+1 < r ) {
            __builtin_prefetch((&sort_rows_.at(i+1).len_));
            __builtin_prefetch(((unsigned char *)(sort_rows_.at(i+1).key_ptr_)) + common_prefix);
          }
          sort_rows_.at(i).sub_cache_[0] = *(x + 1);
          sort_rows_.at(i).sub_cache_[1] = (common_prefix + 2 == sort_rows_.at(i).len_) ? 0x00 : *(x+2);
        }
      }
      aqs_cps_qs(more_l, r, common_prefix + 1, depth_limit, cache_offset);
      return;
    }

    int divide_line = l;
    __builtin_prefetch((&sort_rows_.at(l).sub_cache_[0]));
    for (int i = l; i < r; i++) {
      // byte b = index_bytes[i];
      if (i+1 < r) __builtin_prefetch((&sort_rows_.at(i+1).sub_cache_[0]));
      if ((sort_rows_.at(i).sub_cache_[0] & masks[div_val]) == 0) {
        swap(i, divide_line);
        divide_line++;
      }
    }
    inplace_radixsort_more_bucket(l, divide_line,
                                  div_val - 1, common_prefix,
                                  depth_limit, cache_offset, update);
    inplace_radixsort_more_bucket(divide_line, r,
                                  div_val - 1, common_prefix,
                                  depth_limit, cache_offset, update);
  }
}

typedef int (*CompareByteFunc)(const unsigned char *s, const unsigned char *t,
                        int64_t length, int64_t &differ_at, int64_t cache_ends);
extern int fast_compare_simd(const unsigned char *s, const unsigned char *t,
                        int64_t length, int64_t &differ_at, int64_t cache_ends);

int fast_compare_normal(const unsigned char *s, const unsigned char *t,
                        int64_t length, int64_t &differ_at, int64_t cache_ends)
{
  int cmp_ret = 0;
  for (int i = 0; (cmp_ret == 0)  && i < length; i++) {
    if (s[i] != t[i]) {
      differ_at = i + cache_ends;
      cmp_ret = s[i] - t[i];
    }
  }
  return cmp_ret;
}

CompareByteFunc get_fast_compare_func()
{
  return blocksstable::is_avx512_valid()
      ? fast_compare_simd
      : fast_compare_normal;
}

CompareByteFunc cmp_byte_func = get_fast_compare_func();

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
int ObSortOpImpl::ObAdaptiveQS::compare_cache(AQSItem &l,
                                  AQSItem &r, int64_t &differ_at,
                                  int64_t common_prefix,
                                  int64_t cache_offset)
{
  int64_t cache_ends = cache_offset + 2;
  int64_t res = 0;
  for (int64_t i = common_prefix;
          res == 0 && i < cache_ends; i++) {
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
    for (int64_t j = cache_ends;
          res == 0 && j < len; j += stride)
    {
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

int ObSortOpImpl::ObAdaptiveQS::compare_vals(int64_t l, int64_t r,
                                             int64_t &differ_at, int64_t common_prefix,
                                             int64_t cache_offset) {
  return compare_cache(sort_rows_.at(l), sort_rows_.at(r), differ_at, common_prefix, cache_offset);
}

ObSortOpImpl::Compare::Compare()
  : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr),
    exec_ctx_(nullptr), cmp_count_(0), cmp_start_(0), cmp_end_(0)
{
}

int ObSortOpImpl::Compare::init(
    const ObIArray<ObSortFieldCollation> *sort_collations,
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
    ObExecContext *exec_ctx,
    bool enable_encode_sortkey)
{
  int ret = OB_SUCCESS;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs || nullptr == exec_ctx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret),
      K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    exec_ctx_ = exec_ctx;
    cnt_ = sort_cmp_funs_->count();
    cmp_start_ = 0;
    cmp_end_ = sort_cmp_funs_->count();
    enable_encode_sortkey_ = enable_encode_sortkey;
  }
  return ret;
}

int ObSortOpImpl::Compare::fast_check_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((cmp_count_++ & 8191) == 8191)) {
    ret = exec_ctx_->check_status();
  }
  return ret;
}

bool ObSortOpImpl::Compare::operator()(
    const ObChunkDatumStore::StoredRow *l,
    const ObChunkDatumStore::StoredRow *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else if (OB_FAIL(fast_check_status())) {
    LOG_WARN("fast check failed", K(ret));
  } else if (enable_encode_sortkey_) {
    const ObDatum l_cell = l->cells()[0];
    const ObDatum r_cell = r->cells()[0];
    int cmp = 0;
    cmp = MEMCMP(l_cell.ptr_, r_cell.ptr_, min(l_cell.len_, r_cell.len_));
    less = cmp != 0 ? (cmp < 0) : (l_cell.len_ - r_cell.len_) < 0;
  } else {
    const ObDatum *lcells = l->cells();
    const ObDatum *rcells = r->cells();
    int cmp = 0;
    for (int64_t i = cmp_start_; 0 == cmp && i < cmp_end_ && OB_SUCC(ret); i++) {
      const ObSortFieldCollation& sort_collation = sort_collations_->at(i);
      const int64_t idx = sort_collation.field_idx_;
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp < 0) {
        less = sort_collation.is_ascending_;
      } else if (cmp > 0) {
        less = !sort_collation.is_ascending_;
      }
    }
  }
  return less;
}

bool ObSortOpImpl::Compare::operator()(
    const common::ObIArray<ObExpr*> *l,
    const ObChunkDatumStore::StoredRow *r,
    ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else if (OB_FAIL(fast_check_status())) {
    LOG_WARN("fast check failed", K(ret));
  } else {
    const ObDatum *rcells = r->cells();
    ObDatum *other_datum = nullptr;
    int cmp = 0;
    const int64_t cnt = sort_cmp_funs_->count();
    for (int64_t i = 0; 0 == cmp && i < cnt && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(l->at(idx)->eval(eval_ctx, other_datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(*other_datum, rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        if (cmp < 0) {
          less = sort_collations_->at(i).is_ascending_;
        } else if (cmp > 0) {
          less = !sort_collations_->at(i).is_ascending_;
        }
      }
    }
  }
  return less;
}

int ObSortOpImpl::Compare::with_ties_cmp(const common::ObIArray<ObExpr*> *l,
                                         const ObChunkDatumStore::StoredRow *r,
                                         ObEvalCtx &eval_ctx)
{
  int cmp = 0;
  
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum *rcells = r->cells();
    ObDatum *other_datum = nullptr;
    const int64_t cnt = sort_cmp_funs_->count();
    for (int64_t i = 0; 0 == cmp && i < cnt && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(l->at(idx)->eval(eval_ctx, other_datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(*other_datum, rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        cmp = sort_collations_->at(i).is_ascending_ ? -cmp : cmp;
      }
    }
  }
  return cmp;
}

int ObSortOpImpl::Compare::with_ties_cmp(const ObChunkDatumStore::StoredRow *l,
                                         const ObChunkDatumStore::StoredRow *r)
{
  int cmp = 0;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum *rcells = r->cells();
    const ObDatum *lcells = l->cells();
    const int64_t cnt = sort_cmp_funs_->count();
    for (int64_t i = 0; 0 == cmp && i < cnt && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        cmp = sort_collations_->at(i).is_ascending_ ? -cmp : cmp;
      }
    }
  }
  return cmp;
}

// compare function for external merge sort
bool ObSortOpImpl::Compare::operator()(const ObSortOpChunk *l, const ObSortOpChunk *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(r->row_, l->row_);
  }
  return less;
}

bool ObSortOpImpl::Compare::operator()(
    ObChunkDatumStore::StoredRow **l,
    ObChunkDatumStore::StoredRow **r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

ObSortOpImpl::ObSortOpImpl(ObMonitorNode &op_monitor_info)
  : inited_(false), local_merge_sort_(false), need_rewind_(false),
    got_first_row_(false), sorted_(false), enable_encode_sortkey_(false), mem_context_(NULL),
    mem_entify_guard_(mem_context_), tenant_id_(OB_INVALID_ID), sort_collations_(nullptr),
    sort_cmp_funs_(nullptr), eval_ctx_(nullptr), datum_store_(ObModIds::OB_SQL_SORT_ROW), inmem_row_size_(0), mem_check_interval_mask_(1),
    row_idx_(0), heap_iter_begin_(false), imms_heap_(NULL), ems_heap_(NULL),
    next_stored_row_func_(&ObSortOpImpl::array_next_stored_row),
    input_rows_(OB_INVALID_ID), input_width_(OB_INVALID_ID),
    profile_(ObSqlWorkAreaType::SORT_WORK_AREA), op_monitor_info_(op_monitor_info), sql_mem_processor_(profile_, op_monitor_info_),
    op_type_(PHY_INVALID), op_id_(UINT64_MAX), exec_ctx_(nullptr), stored_rows_(nullptr),
    io_event_observer_(nullptr), buckets_(NULL), max_bucket_cnt_(0), part_hash_nodes_(NULL),
    max_node_cnt_(0), part_cnt_(0), topn_cnt_(INT64_MAX), outputted_rows_cnt_(0),
    is_fetch_with_ties_(false), topn_heap_(NULL), ties_array_pos_(0), ties_array_(),
    last_ties_row_(NULL), rows_(NULL)
{
}

ObSortOpImpl::~ObSortOpImpl()
{
  reset();
}

// Set the note in ObPrefixSortImpl::init(): %sort_columns may be zero, to compatible with
// the wrong generated prefix sort.
int ObSortOpImpl::init(
  const uint64_t tenant_id,
  const ObIArray<ObSortFieldCollation> *sort_collations,
  const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
  ObEvalCtx *eval_ctx,
  ObExecContext *exec_ctx,
  const bool enable_encode_sortkey /* = false*/,
  const bool in_local_order /* = false */,
  const bool need_rewind /* = false */,
  const int64_t part_cnt /* = 0 */,
  const int64_t topn_cnt /* = INT64_MAX */,
  const bool is_fetch_with_ties /* = false */,
  const int64_t default_block_size /* = 64KB */)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funs)
             || OB_ISNULL(eval_ctx) || OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: argument is null", K(ret),
              K(tenant_id), K(sort_collations), K(sort_cmp_funs), K(eval_ctx));
  } else if (OB_FAIL(comp_.init(sort_collations, sort_cmp_funs,
                      exec_ctx, enable_encode_sortkey && !(part_cnt > 0)))) {
    LOG_WARN("failed to init compare functions", K(ret));
  } else {
    local_merge_sort_ = in_local_order;
    need_rewind_ = need_rewind;
    enable_encode_sortkey_ = enable_encode_sortkey;
    tenant_id_ = tenant_id;
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    eval_ctx_ = eval_ctx;
    exec_ctx_ = exec_ctx;
    part_cnt_ = part_cnt;
    topn_cnt_ = topn_cnt;
    use_heap_sort_ = is_topn_sort();
    is_fetch_with_ties_ = is_fetch_with_ties;
    int64_t batch_size = eval_ctx_->max_batch_size_;
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (NULL == mem_context_ && OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (NULL == mem_context_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(datum_store_.init(
        INT64_MAX /* mem limit, big enough to hold all rows in memory */,
        tenant_id_, ObCtxIds::WORK_AREA, ObModIds::OB_SQL_SORT_ROW,
        false /*+ disable dump */,
        0, /* row_extra_size */
        default_block_size))) {
      LOG_WARN("init row store failed", K(ret));
    } else if (is_topn_sort()
               && OB_ISNULL(topn_heap_ = OB_NEWx(TopnHeap, (&mem_context_->get_malloc_allocator()),
                            comp_, &mem_context_->get_malloc_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (batch_size > 0
               && OB_ISNULL(stored_rows_ = static_cast<ObChunkDatumStore::StoredRow **>(
                       mem_context_->get_malloc_allocator().alloc(
                           sizeof(*stored_rows_) * batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      quick_sort_array_.set_block_allocator(
        ModulePageAllocator(mem_context_->get_malloc_allocator(), "SortOpRows"));
      datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
      datum_store_.set_allocator(mem_context_->get_malloc_allocator());
      datum_store_.set_io_event_observer(io_event_observer_);
      profile_.set_exec_ctx(exec_ctx);
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SORT_MERGE_SORT_ROUND;
      op_monitor_info_.otherstat_2_value_ = 1;
      ObPhysicalPlanCtx *plan_ctx = NULL;
      const ObPhysicalPlan *phy_plan = nullptr;
      if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("deserialized exec ctx without phy plan ctx set. Unexpected", K(ret));
      } else if (OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, phy plan must not be nullptr", K(ret));
      } else if (phy_plan->get_ddl_task_id() > 0) {
        op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
        op_monitor_info_.otherstat_5_value_ = phy_plan->get_ddl_task_id();
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
      if (!is_topn_sort()) {
        rows_ = &quick_sort_array_;
      } else {
        rows_ = &(const_cast<common::ObIArray<ObChunkDatumStore::StoredRow *> &>
                                (topn_heap_->get_heap_data()));
      }
    }
  }

  return ret;
}

void ObSortOpImpl::reuse()
{
  sorted_ = false;
  iter_.reset();
  quick_sort_array_.reuse();
  datum_store_.reset();
  inmem_row_size_ = 0;
  mem_check_interval_mask_ = 1;
  row_idx_ = 0;
  next_stored_row_func_ = &ObSortOpImpl::array_next_stored_row;
  ties_array_pos_ = 0;
  if (0 != ties_array_.count()) {
    for (int64_t i = 0; i < ties_array_.count(); ++i) {
      sql_mem_processor_.alloc(-1 * ties_array_[i]->get_max_size());
      mem_context_->get_malloc_allocator().free(ties_array_[i]);
      ties_array_[i] = NULL;
    }
  }
  ties_array_.reset();
  while (!sort_chunks_.is_empty()) {
    ObSortOpChunk *chunk = sort_chunks_.remove_first();
    chunk->~ObSortOpChunk();
    if (NULL != mem_context_) {
      mem_context_->get_malloc_allocator().free(chunk);
    }
  }
  if (NULL != imms_heap_) {
    imms_heap_->reset();
  }
  heap_iter_begin_ = false;
  if (NULL != ems_heap_) {
    ems_heap_->reset();
  }
  if (NULL != topn_heap_) {
    for (int64_t i = 0; i < topn_heap_->count(); ++i) {
      sql_mem_processor_.alloc(-1 *
        static_cast<SortStoredRow *>(topn_heap_->at(i))->get_max_size());
      mem_context_->get_malloc_allocator().free(static_cast<SortStoredRow *>(topn_heap_->at(i)));
      topn_heap_->at(i) = NULL;
    }
    topn_heap_->reset();
  }
}

void ObSortOpImpl::unregister_profile()
{
  sql_mem_processor_.unregister_profile();
}

void ObSortOpImpl::unregister_profile_if_necessary()
{
  sql_mem_processor_.unregister_profile_if_necessary();
}

void ObSortOpImpl::reset()
{
  sql_mem_processor_.unregister_profile();
  iter_.reset();
  reuse();
  quick_sort_array_.reset();
  datum_store_.reset();
  inmem_row_size_ = 0;
  local_merge_sort_ = false;
  need_rewind_ = false;
  sorted_ = false;
  got_first_row_ = false;
  comp_.reset();
  max_bucket_cnt_ = 0;
  max_node_cnt_ = 0;
  part_cnt_ = 0;
  topn_cnt_ = INT64_MAX;
  outputted_rows_cnt_ = 0;
  is_fetch_with_ties_ = false;
  rows_ = NULL;
  ties_array_pos_ = 0;
  if (0 != ties_array_.count()) {
    for (int64_t i = 0; i < ties_array_.count(); ++i) {
      mem_context_->get_malloc_allocator().free(ties_array_[i]);
      ties_array_[i] = NULL;
    }
  }
  ties_array_.reset();
  if (NULL != mem_context_) {
    if (NULL != imms_heap_) {
      imms_heap_->~IMMSHeap();
      mem_context_->get_malloc_allocator().free(imms_heap_);
      imms_heap_ = NULL;
    }
    if (NULL != ems_heap_) {
      ems_heap_->~EMSHeap();
      mem_context_->get_malloc_allocator().free(ems_heap_);
      ems_heap_ = NULL;
    }
    if (NULL != stored_rows_) {
      mem_context_->get_malloc_allocator().free(stored_rows_);
      stored_rows_ = NULL;
    }
    if (NULL != buckets_) {
      mem_context_->get_malloc_allocator().free(buckets_);
      buckets_ = NULL;
    }
    if (NULL != part_hash_nodes_) {
      mem_context_->get_malloc_allocator().free(part_hash_nodes_);
      part_hash_nodes_ = NULL;
    }
    if (NULL != topn_heap_) {
      for (int64_t i = 0; i < topn_heap_->count(); ++i) {
        mem_context_->get_malloc_allocator().free(static_cast<SortStoredRow *>(topn_heap_->at(i)));
        topn_heap_->at(i) = NULL;
      }
      topn_heap_->~TopnHeap();
      mem_context_->get_malloc_allocator().free(topn_heap_);
      topn_heap_ = NULL;
    }
    if (NULL != last_ties_row_) {
      mem_context_->get_malloc_allocator().free(last_ties_row_);
      last_ties_row_ = NULL;
    }
    // can not destroy mem_entify here, the memory may hold by %iter_ or %datum_store_
  }
  inited_ = false;
  io_event_observer_ = nullptr;
}

template <typename Input>
int ObSortOpImpl::build_chunk(const int64_t level, Input &input, int64_t extra_size)
{
  int ret = OB_SUCCESS;
  const int64_t curr_time = ObTimeUtility::fast_current_time();
  int64_t stored_row_cnt = 0;
  ObChunkDatumStore *datum_store = NULL;
  const ObChunkDatumStore::StoredRow *src_store_row = NULL;
  ObChunkDatumStore::StoredRow *dst_store_row = NULL;
  ObSortOpChunk *chunk = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(chunk = OB_NEWx(ObSortOpChunk,
      (&mem_context_->get_malloc_allocator()), level))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(chunk->datum_store_.init(1/*+ mem limit, small limit for dump immediately */,
                        tenant_id_, ObCtxIds::WORK_AREA, ObModIds::OB_SQL_SORT_ROW,
                        true/*+ enable dump */, extra_size/* for InMemoryTopnSort */))) {
    LOG_WARN("init row store failed", K(ret));
  } else {
    chunk->datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
    chunk->datum_store_.set_allocator(mem_context_->get_malloc_allocator());
    chunk->datum_store_.set_callback(&sql_mem_processor_);
    chunk->datum_store_.set_io_event_observer(io_event_observer_);
    while (OB_SUCC(ret)) {
      if (!is_fetch_with_ties_ && stored_row_cnt >= topn_cnt_) {
        break;
      } else if (OB_FAIL(input(datum_store, src_store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get input row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(chunk->datum_store_.add_row(*src_store_row, &dst_store_row))) {
        LOG_WARN("copy row to row store failed");
      } else {
        stored_row_cnt++;
        op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
        op_monitor_info_.otherstat_1_value_ += 1;
      }
    }

    // 必须强制先dump，然后finish dump才有效
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(chunk->datum_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else if (OB_FAIL(chunk->datum_store_.finish_add_row(true/*+ need dump */))) {
      LOG_WARN("finish add row failed", K(ret));
    } else {
      const int64_t sort_io_time = ObTimeUtility::fast_current_time() - curr_time;
      op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
      op_monitor_info_.otherstat_4_value_ += sort_io_time;
      LOG_TRACE("dump sort file",
          "level", level,
          "rows", chunk->datum_store_.get_row_cnt(),
          "file_size", chunk->datum_store_.get_file_size(),
          "memory_hold", chunk->datum_store_.get_mem_hold(),
          "mem_used", mem_context_->used());

    }
  }

  if (OB_SUCC(ret)) {
    // In increase sort, chunk->level_ may less than the last of sort chunks.
    // insert the chunk to the upper bound the level.
    ObSortOpChunk *pos = sort_chunks_.get_last();
    for ( ; pos != sort_chunks_.get_header() && pos->level_ > level; pos = pos->get_prev()) {
    }
    pos = pos->get_next();
    if (!sort_chunks_.add_before(pos, chunk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add link node to list failed", K(ret));
    }
  }
  if (OB_SUCCESS != ret && NULL != chunk) {
    chunk->~ObSortOpChunk();
    mem_context_->get_malloc_allocator().free(chunk);
    chunk = NULL;
  }

  return ret;
}

// 如果发现需要dump，则
// 1 重新获取可用内存大小
// 2 检查是否还需要dump
// 3 如果需要dump，分三种情况
//   3.0 cache_size <= mem_bound 全内存(这里表示之前预估不准确，同时有足够内存可用)
//       申请是否有更多内存可用，决定是否需要dump
//       3.0.1 申请内存大于等于cache size，则不dump
//       3.0.2 申请内存小于cache size，则dump，返回的是算one-pass size
//   3.1 未超过cache size，则直接dump
//   3.2 超过了cache size，则采用2*size方式申请内存，one-pass内存
//       然后继续，和之前逻辑一样
//       所以这里会导致最开始dump的partition one-pass内存较少，后面倍数cache size关系的one-pass更大
int ObSortOpImpl::preprocess_dump(bool &dumped)
{
  int ret = OB_SUCCESS;
  dumped = false;
  if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(
      &mem_context_->get_malloc_allocator()))) {
    LOG_WARN("failed to get max available memory size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
    LOG_WARN("failed to update used memory size", K(ret));
  } else {
    dumped = need_dump();
    if (dumped) {
      if (!sql_mem_processor_.is_auto_mgr()) {
        // 如果dump在非auto管理模式也需要注册到workarea
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              UNUSED(max_memory_size);
              return need_dump();
            },
            dumped, mem_context_->used()))) {
          LOG_WARN("failed to extend memory size", K(ret));
        }
      } else if (profile_.get_cache_size() < profile_.get_global_bound_size()) {
        // in-memory：所有数据都可以缓存，即global bound size比较大，则继续看是否有更多内存可用
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              UNUSED(max_memory_size);
              return need_dump();
            },
            dumped, mem_context_->used()))) {
          LOG_WARN("failed to extend memory size", K(ret));
        }
        LOG_TRACE("trace sort need dump", K(dumped), K(mem_context_->used()),
          K(get_memory_limit()), K(profile_.get_cache_size()), K(profile_.get_expect_size()));
      } else {
        // one-pass
        if (profile_.get_cache_size() <=
                                  datum_store_.get_mem_hold() + datum_store_.get_file_size()) {
          // 总体数据量超过cache size，说明估算的cache不准确，需要重新估算one-pass size，按照2*cache_size处理
          if (OB_FAIL(sql_mem_processor_.update_cache_size(&mem_context_->get_malloc_allocator(),
            profile_.get_cache_size() * EXTEND_MULTIPLE))) {
            LOG_WARN("failed to update cache size", K(ret), K(profile_.get_cache_size()));
          } else {
            dumped = need_dump();
          }
        } else { }
      }
      LOG_INFO("trace sort need dump", K(dumped), K(mem_context_->used()), K(get_memory_limit()),
        K(profile_.get_cache_size()), K(profile_.get_expect_size()),
        K(sql_mem_processor_.get_data_size()));
    }
  }
  return ret;
}

int ObSortOpImpl::before_add_row()
{
  int ret = OB_SUCCESS;
  int64_t sort_force_dump_rows = - EVENT_CALL(EventTable::EN_SORT_IMPL_FORCE_DO_DUMP);

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!got_first_row_)) {
    if (!comp_.is_inited() && OB_FAIL(comp_.init(sort_collations_, sort_cmp_funs_,
                              exec_ctx_, enable_encode_sortkey_ && !(part_cnt_ > 0)))) {
      LOG_WARN("init compare failed", K(ret));
    } else {
      got_first_row_ = true;
      int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
      if (OB_FAIL(sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  tenant_id_,
                  size, op_monitor_info_.op_type_, op_monitor_info_.op_id_, exec_ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else {
        datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        datum_store_.set_callback(&sql_mem_processor_);
        datum_store_.set_io_event_observer(io_event_observer_);
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sort_force_dump_rows > 0 && rows_->count() >= sort_force_dump_rows) {
    if (OB_FAIL(do_dump())) {
      LOG_WARN("dump failed", K(ret));
    }
  } else if (!rows_->empty()) {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return rows_->count() > cur_cnt; },
      updated))) {
      LOG_WARN("failed to update max available mem size periodically", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled()) {
      if (rows_->count() >= MAX_ROW_CNT) {
        // 最大2G，超过2G会扩容到4G，4G申请会失败
        if (OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      } else if (need_dump()) {
        bool dumped = false;
        if (OB_FAIL(preprocess_dump(dumped))) {
          LOG_WARN("failed preprocess dump", K(ret));
        } else if (dumped && OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && sorted_) {
    if (!need_rewind_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not add row after sort if no need rewind", K(ret));
    } else {
      sorted_ = false;
      // add null sentry row
      if (!rows_->empty() && NULL != rows_->at(rows_->count() - 1)) {
        if (OB_FAIL(rows_->push_back(NULL))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSortOpImpl::after_add_row(ObChunkDatumStore::StoredRow *sr)
{
  int ret = OB_SUCCESS;
  inmem_row_size_ += sr->row_size_;
  if (local_merge_sort_ && rows_->count() > 0 && NULL != rows_->at(rows_->count() - 1)) {
    const bool less = comp_(sr, rows_->at(rows_->count() - 1));
    if (OB_SUCCESS != comp_.ret_) {
      ret = comp_.ret_;
      LOG_WARN("compare failed", K(ret));
    } else if (less) {
      // If new is less than previous row, add NULL to separate different local order rows.
      if (OB_FAIL(rows_->push_back(NULL))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rows_->push_back(sr))) {
      LOG_WARN("array push back failed", K(ret), K(rows_->count()));
    }
  }
  return ret;
}

int ObSortOpImpl::add_quick_sort_row(const common::ObIArray<ObExpr*> &exprs,
                                     const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_row(exprs, eval_ctx_, &sr))) {
    LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else if (OB_FAIL(after_add_row(sr))) {
    LOG_WARN("after add row process failed", K(ret));
  } else {
    store_row = sr;
  }
  return ret;
}

int ObSortOpImpl::add_row(const common::ObIArray<ObExpr*> &exprs,
                          const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_heap_sort_ && need_dump())) {
    bool dumped = false;
    if (OB_FAIL(preprocess_dump(dumped))) {
      LOG_WARN("failed preprocess dump", K(ret));
    } else if (dumped && OB_FAIL(do_dump())) {
      LOG_WARN("failed to do topn dump", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_heap_sort_) {
    ret = add_heap_sort_row(exprs, store_row);
  } else {
    ret = add_quick_sort_row(exprs, store_row);
  }
  return ret;
}

int ObSortOpImpl::add_quick_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                                       const ObBitVector &skip,
                                       const int64_t batch_size,
                                       const int64_t start_pos /* 0 */,
                                       int64_t *append_row_count)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_cnt = 0;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_batch(exprs, *eval_ctx_, skip, batch_size,
                                            stored_rows_cnt, stored_rows_, start_pos))) {
    LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_rows_cnt; i++) {
      if (OB_FAIL(after_add_row(stored_rows_[i]))) {
        LOG_WARN("after add row process failed", K(ret));
      }
    }
    if (OB_NOT_NULL(append_row_count)) {
      *append_row_count = stored_rows_cnt;
    }
  }
  return ret;
}

int ObSortOpImpl::add_batch(const common::ObIArray<ObExpr *> &exprs,
                            const ObBitVector &skip,
                            const int64_t batch_size,
                            const int64_t start_pos /* 0 */,
                            int64_t *append_row_count = nullptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_heap_sort_ && need_dump())) {
    bool dumped = false;
    if (OB_FAIL(preprocess_dump(dumped))) {
      LOG_WARN("failed preprocess dump", K(ret));
    } else if (dumped && OB_FAIL(do_dump())) {
      LOG_WARN("failed to do topn dump", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_heap_sort_) {
    ret = add_heap_sort_batch(exprs, skip, batch_size, start_pos, append_row_count);
  } else {
    ret = add_quick_sort_batch(exprs, skip, batch_size, start_pos, append_row_count);
  }
  return ret;
}

int ObSortOpImpl::add_quick_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                                       const ObBitVector &skip,
                                       const int64_t batch_size,
                                       const uint16_t selector[],
                                       const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_cnt = size;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_batch(exprs, *eval_ctx_, skip, batch_size,
                                            selector, size, stored_rows_))) {
    LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_rows_cnt; i++) {
      if (OB_FAIL(after_add_row(stored_rows_[i]))) {
        LOG_WARN("after add row process failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSortOpImpl::add_batch(const common::ObIArray<ObExpr *> &exprs,
                            const ObBitVector &skip, const int64_t batch_size,
                            const uint16_t selector[], const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_heap_sort_ && need_dump())) {
    if (OB_FAIL(do_dump())) {
      LOG_WARN("failed to do topn dump", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_heap_sort_) {
    ret = add_heap_sort_batch(exprs, skip, batch_size, selector, size);
  } else {
    ret = add_quick_sort_batch(exprs, skip, batch_size, selector, size);
  }
  return ret;
}

int ObSortOpImpl::add_stored_row(const ObChunkDatumStore::StoredRow &input_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed", K(ret));
  } else if (OB_FAIL(datum_store_.add_row(input_row, &sr))) {
    LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else if (OB_FAIL(after_add_row(sr))) {
    LOG_WARN("after add row process failed", K(ret));
  }
  return ret;
}

int ObSortOpImpl::is_equal_part(const ObChunkDatumStore::StoredRow *l,
                                 const ObChunkDatumStore::StoredRow *r,
                                 bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_ISNULL(l) && OB_ISNULL(r)) {
    // do nothing
  } else if (OB_ISNULL(l) || OB_ISNULL(r)
             || (l->cells()[sort_collations_->at(0).field_idx_].get_uint64()
                 != r->cells()[sort_collations_->at(0).field_idx_].get_uint64())) {
    is_equal = false; // offest 0 is hash value.
  } else {
    int cmp_ret = 0;
    for (int64_t i = 1; is_equal && i <= part_cnt_; ++i) {
      int64_t idx = sort_collations_->at(i).field_idx_;
      const ObDatum &ld = l->cells()[idx];
      const ObDatum &rd = r->cells()[idx];
      if (ld.pack_ == rd.pack_ && 0 == memcmp(ld.ptr_, rd.ptr_, ld.len_)) {
        // do nothing
      } else if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(ld, rd, cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        is_equal = (0 == cmp_ret);
      }
    }
  }
  return ret;
}

int ObSortOpImpl::do_partition_sort(common::ObIArray<ObChunkDatumStore::StoredRow *> &rows,
                                    const int64_t rows_begin, const int64_t rows_end)
{
  int ret = OB_SUCCESS;
  CK(part_cnt_ > 0);
  int64_t hash_expr_cnt = 1;
  ObIAllocator &allocator = mem_context_->get_malloc_allocator();
  uint64_t node_cnt = rows_end - rows_begin;
  uint64_t bucket_cnt = next_pow2(std::max(16L, rows.count()));
  uint64_t shift_right = __builtin_clzll(bucket_cnt) + 1;

  if (OB_SUCC(ret)) {
    if (rows_end - rows_begin <= 0) {
      // do nothing
    } else if (rows_begin < 0 || rows_end > rows.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(rows_begin), K(rows_end), K(rows.count()), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (max_bucket_cnt_ < bucket_cnt) {
      if (NULL != buckets_) {
        allocator.free(buckets_);
        buckets_ = NULL;
        max_bucket_cnt_ = 0;
      }
      buckets_ = (PartHashNode **)allocator.alloc(sizeof(PartHashNode *) * bucket_cnt);
      if (OB_ISNULL(buckets_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        max_bucket_cnt_ = bucket_cnt;
        MEMSET(buckets_, 0, sizeof(PartHashNode *) * bucket_cnt);
      }
    } else {
      MEMSET(buckets_, 0, sizeof(PartHashNode *) * bucket_cnt);
    }
  }

  if (OB_SUCC(ret)) {
    if (max_node_cnt_ < node_cnt) {
      if (NULL != part_hash_nodes_) {
        allocator.free(part_hash_nodes_);
        part_hash_nodes_ = NULL;
        max_node_cnt_ = 0;
      }
      part_hash_nodes_ = (PartHashNode *)allocator.alloc(sizeof(PartHashNode) * node_cnt);
      if (OB_ISNULL(part_hash_nodes_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        max_node_cnt_ = node_cnt;
      }
    }
  }

  for (int64_t i = rows_begin; OB_SUCC(ret) && i < rows_end; ++i) {
    if (OB_ISNULL(rows.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get rows", K(ret));
    } else {
      int64_t hash_idx = sort_collations_->at(0).field_idx_;
      const uint64_t hash_value = rows.at(i)->cells()[hash_idx].get_uint64();
      uint64_t pos = hash_value >> shift_right; // high n bit
      PartHashNode &insert_node = part_hash_nodes_[i - rows_begin];
      PartHashNode *&bucket = buckets_[pos];
      insert_node.store_row_ = rows.at(i);
      PartHashNode *exist = bucket;
      bool equal = false;
      while (NULL != exist && OB_SUCC(ret)) {
        if (OB_FAIL(is_equal_part(exist->store_row_, rows.at(i), equal))) {
          LOG_WARN("failed to check equal", K(ret));
        } else if (equal) {
          break;
        } else {
          exist = exist->hash_node_next_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (NULL == exist) { // insert at first node with hash_node_next.
        insert_node.part_row_next_ = NULL;
        insert_node.hash_node_next_ = bucket;
        bucket = &insert_node;
      } else { // insert at second node with part_row_next.
        insert_node.part_row_next_ = exist->part_row_next_;
        exist->part_row_next_ = &insert_node;
      }
    }
  }

  int64_t rows_idx = rows_begin;
  ObArray<PartHashNode *> bucket_nodes;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(bucket_nodes.prepare_allocate(16))) {
      LOG_WARN("failed to prepare allocate bucket nodes", K(ret));
    }
  }
  for (int64_t bucket_idx = 0; OB_SUCC(ret) && bucket_idx < bucket_cnt; ++bucket_idx) {
    int64_t bucket_part_cnt = 0;
    PartHashNode *bucket_node = buckets_[bucket_idx];
    if (NULL == bucket_node) {
      continue; // no rows add here
    }
    while (OB_SUCC(ret) && NULL != bucket_node) {
      if (OB_LIKELY(bucket_part_cnt < bucket_nodes.count())) {
        bucket_nodes.at(bucket_part_cnt) = bucket_node;
      } else {
        if (OB_FAIL(bucket_nodes.push_back(bucket_node))) {
          LOG_WARN("failed to push back bucket node", K(ret));
        }
      }
      bucket_node = bucket_node->hash_node_next_;
      bucket_part_cnt++;
    }
    comp_.set_cmp_range(0, part_cnt_ + hash_expr_cnt);
    std::sort(&bucket_nodes.at(0), &bucket_nodes.at(0) + bucket_part_cnt, HashNodeComparer(comp_));
    comp_.set_cmp_range(part_cnt_ + hash_expr_cnt, comp_.get_cnt());
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_part_cnt; ++i) {
      int64_t rows_last = rows_idx;
      PartHashNode *part_node = bucket_nodes.at(i);
      while (NULL != part_node) {
        rows.at(rows_idx++) = part_node->store_row_;
        part_node = part_node->part_row_next_;
      }
      if (comp_.cmp_start_ != comp_.cmp_end_) {
        if (enable_encode_sortkey_) {
          bool can_encode = true;
          ObAdaptiveQS aqs(rows, allocator);
          if (OB_FAIL(aqs.init(rows, allocator, rows_last, rows_idx, can_encode))) {
            LOG_WARN("failed to init aqs", K(ret));
          } else if (can_encode) {
            aqs.sort(rows_last, rows_idx);
          } else {
            enable_encode_sortkey_ = false;
            comp_.enable_encode_sortkey_ = false;
            std::sort(&rows.at(0) + rows_last, &rows.at(0) + rows_idx, CopyableComparer(comp_));
          }
        } else {
          std::sort(&rows.at(0) + rows_last, &rows.at(0) + rows_idx, CopyableComparer(comp_));
        }
      }
    }
    comp_.set_cmp_range(0, comp_.get_cnt());
  }
  return ret;
}

int ObSortOpImpl::do_dump()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (rows_->empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sort_inmem_data())) {
    LOG_WARN("sort in-memory data failed", K(ret));
  } else {
    const int64_t level = 0;
    if (!need_imms()) {
      int64_t row_pos = 0;
      int64_t ties_array_pos = 0;
      auto input = [&](ObChunkDatumStore *&rs, const ObChunkDatumStore::StoredRow *&row) {
        int ret = OB_SUCCESS;
        if (row_pos >= rows_->count()
            && ties_array_pos >= ties_array_.count()) {
          ret = OB_ITER_END;
        } else if (row_pos < rows_->count()) {
          row = rows_->at(row_pos);
          rs = &datum_store_;
          row_pos += 1;
        } else {
          row = ties_array_.at(ties_array_pos);
          rs = &datum_store_;
          ties_array_pos += 1;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        LOG_WARN("build chunk failed", K(ret));
      }
    } else {
      auto input = [&](ObChunkDatumStore *&rs, const ObChunkDatumStore::StoredRow *&row) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(imms_heap_next(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from memory heap failed", K(ret));
          }
        } else {
          rs = &datum_store_;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        LOG_WARN("build chunk failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && use_heap_sort_) {
      if (NULL != mem_context_ && NULL != topn_heap_) {
        for (int64_t i = 0; i < topn_heap_->count(); ++i) {
          sql_mem_processor_.alloc(-1 *
            static_cast<SortStoredRow *>(topn_heap_->at(i))->get_max_size());
          mem_context_->get_malloc_allocator().free(
            static_cast<SortStoredRow *>(topn_heap_->at(i)));
          topn_heap_->at(i) = NULL;
        }
        topn_heap_->~TopnHeap();
        mem_context_->get_malloc_allocator().free(topn_heap_);
        topn_heap_ = NULL;
      }
      if (0 != ties_array_.count()) {
        for (int64_t i = 0; i < ties_array_.count(); ++i) {
          sql_mem_processor_.alloc(-1 * ties_array_[i]->get_max_size());
          mem_context_->get_malloc_allocator().free(ties_array_[i]);
          ties_array_[i] = NULL;
        }
      }
      ties_array_.reset();
      got_first_row_ = false;
      use_heap_sort_ = false;
      rows_ = &quick_sort_array_;
    }

    if (OB_SUCC(ret)) {
      heap_iter_begin_ = false;
      row_idx_ = 0;
      quick_sort_array_.reset();
      datum_store_.reset();
      inmem_row_size_ = 0;
      mem_check_interval_mask_ = 1;
      sql_mem_processor_.set_number_pass(level + 1);
      sql_mem_processor_.reset();
    }
  }
  return ret;
}

int ObSortOpImpl::build_ems_heap(int64_t &merge_ways)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sort_chunks_.get_size() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty or one way, merge sort not needed", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(
    &mem_context_->get_malloc_allocator()))) {
    LOG_WARN("failed to get max available memory size", K(ret));
  } else {
    ObSortOpChunk *first = sort_chunks_.get_first();
    if (first->level_ != first->get_next()->level_) {
      LOG_TRACE("only one chunk in current level, move to next level directly",
          K(first->level_));
      first->level_ = first->get_next()->level_;
    }
    int64_t max_ways = 1;
    ObSortOpChunk *c = first->get_next();
    // get max merge ways in same level
    for (int64_t i = 0;
        first->level_ == c->level_
        && i < std::min(sort_chunks_.get_size(), (int32_t)MAX_MERGE_WAYS) - 1;
        i++) {
      max_ways += 1;
      c = c->get_next();
    }

    if (NULL == ems_heap_) {
      if (OB_ISNULL(ems_heap_ = OB_NEWx(EMSHeap, (&mem_context_->get_malloc_allocator()),
          comp_, &mem_context_->get_malloc_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else {
      ems_heap_->reset();
    }
    if (OB_SUCC(ret)) {
      merge_ways = get_memory_limit() / ObChunkDatumStore::BLOCK_SIZE;
      merge_ways = std::max(2L, merge_ways);
      if (merge_ways < max_ways) {
        bool dumped = false;
        int64_t need_size = max_ways * ObChunkDatumStore::BLOCK_SIZE;
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return max_memory_size < need_size;
            },
            dumped, mem_context_->used()))) {
          LOG_WARN("failed to extend memory size", K(ret));
        }
        merge_ways = std::max(merge_ways, get_memory_limit() / ObChunkDatumStore::BLOCK_SIZE);
      }
      merge_ways = std::min(merge_ways, max_ways);
      LOG_TRACE("do merge sort ", K(first->level_), K(merge_ways), K(sort_chunks_.get_size()), K(get_memory_limit()), K(sql_mem_processor_.get_profile()));
    }

    if (OB_SUCC(ret)) {
      ObSortOpChunk *chunk = sort_chunks_.get_first();
      for (int64_t i = 0; i < merge_ways && OB_SUCC(ret); i++) {
        chunk->iter_.reset();
        if (OB_FAIL(chunk->iter_.init(&chunk->datum_store_))) {
          LOG_WARN("init iterator failed", K(ret));
        } else if (OB_FAIL(chunk->iter_.get_next_row(chunk->row_))
            || NULL == chunk->row_) {
          if (OB_ITER_END == ret || OB_SUCCESS == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row store is not empty, iterate end is unexpected",
                K(ret), KP(chunk->row_));
          }
          LOG_WARN("get next row failed", K(ret));
        } else if (OB_FAIL(ems_heap_->push(chunk))) {
          LOG_WARN("heap push failed", K(ret));
        } else {
          chunk = chunk->get_next();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    heap_iter_begin_ = false;
  }
  return ret;
}

template <typename Heap, typename NextFunc, typename Item>
int ObSortOpImpl::heap_next(Heap &heap, const NextFunc &func, Item &item)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (heap_iter_begin_) {
      if (!heap.empty()) {
        Item it = heap.top();
        bool is_end = false;
        if (OB_FAIL(func(it, is_end))) {
          LOG_WARN("get next item fail");
        } else {
          if (is_end) {
            if (OB_FAIL(heap.pop())) {
              LOG_WARN("heap pop failed", K(ret));
            }
          } else {
            if (OB_FAIL(heap.replace_top(it))) {
              LOG_WARN("heap replace failed", K(ret));
            }
          }
        }
      }
    } else {
      heap_iter_begin_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (heap.empty()) {
      ret = OB_ITER_END;
    } else {
      item = heap.top();
    }
  }
  return ret;
}

int ObSortOpImpl::ems_heap_next(ObSortOpChunk *&chunk)
{
  const auto f = [](ObSortOpChunk *&c, bool &is_end) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(c->iter_.get_next_row(c->row_))) {
      if (OB_ITER_END == ret) {
        is_end = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    }
    return ret;
  };
  return heap_next(*ems_heap_, f, chunk);
}

int ObSortOpImpl::imms_heap_next(const ObChunkDatumStore::StoredRow *&store_row)
{
  ObChunkDatumStore::StoredRow **sr = NULL;
  const auto f = [](ObChunkDatumStore::StoredRow **&r, bool &is_end) {
    r += 1;
    is_end = (NULL == *r);
    return OB_SUCCESS;
  };

  int ret = heap_next(*imms_heap_, f, sr);
  if (OB_SUCC(ret)) {
    store_row = *sr;
  }
  return ret;
}

int ObSortOpImpl::sort_inmem_data()
{
  int ret = OB_SUCCESS;
  const int64_t curr_time = ObTimeUtility::fast_current_time();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!rows_->empty()) {
    if (!use_heap_sort_ && (local_merge_sort_ || sorted_)) {
      // row already in order, do nothing.
    } else {
      int64_t begin = 0;
      if (need_imms()) {
        // is increment sort (rows add after sort()), sort the last add rows
        for (int64_t i = rows_->count() - 1; i >= 0; i--) {
          if (NULL == rows_->at(i)) {
            begin = i + 1;
            break;
          }
        }
      }
      if (part_cnt_ > 0) {
        do_partition_sort(*rows_, begin, rows_->count());
      } else if (enable_encode_sortkey_) {
        bool can_encode = true;
        ObAdaptiveQS aqs(*rows_, mem_context_->get_malloc_allocator());
        if (OB_FAIL(aqs.init(*rows_, mem_context_->get_malloc_allocator(), begin, rows_->count(), can_encode))) {
          LOG_WARN("failed to init aqs", K(ret));
        } else if (can_encode) {
          aqs.sort(begin, rows_->count());
        } else {
          enable_encode_sortkey_ = false;
          comp_.enable_encode_sortkey_ = false;
          std::sort(&rows_->at(begin), &rows_->at(0) + rows_->count(), CopyableComparer(comp_));
        }
      } else {
        std::sort(&rows_->at(begin), &rows_->at(0) + rows_->count(), CopyableComparer(comp_));
      }
      if (OB_SUCCESS != comp_.ret_) {
        ret = comp_.ret_;
        LOG_WARN("compare failed", K(ret));
      }
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
      op_monitor_info_.otherstat_1_value_ += rows_->count();
    }
    if (OB_SUCC(ret) && need_imms()) {
      if (NULL == imms_heap_) {
        if (OB_ISNULL(imms_heap_ = OB_NEWx(IMMSHeap, (&mem_context_->get_malloc_allocator()),
            comp_, &mem_context_->get_malloc_allocator()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      } else {
        imms_heap_->reset();
      }
      // add null sentry row first
      if (OB_FAIL(ret)) {
      } else if (NULL != rows_->at(rows_->count() - 1)
          && OB_FAIL(rows_->push_back(NULL))) {
        LOG_WARN("array push back failed", K(ret));
      } else {
        int64_t merge_ways = rows_->count() - datum_store_.get_row_cnt();
        LOG_TRACE("do local merge sort ways",
            K(merge_ways), K(rows_->count()), K(datum_store_.get_row_cnt()));
        if (merge_ways > INMEMORY_MERGE_SORT_WARN_WAYS) {
          // only log warning msg
          LOG_WARN("too many merge ways", K(ret),
              K(merge_ways), K(rows_->count()), K(datum_store_.get_row_cnt()));
        }
        ObChunkDatumStore::StoredRow **prev = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < rows_->count(); i++) {
          if (NULL == prev || NULL == *prev) {
            if (OB_FAIL(imms_heap_->push(&rows_->at(i)))) {
              LOG_WARN("heap push back failed", K(ret));
            }
          }
          op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
          op_monitor_info_.otherstat_1_value_ += 1;
          prev = &rows_->at(i);
        }
        heap_iter_begin_ = false;
      }
    }
    const int64_t sort_cpu_time = ObTimeUtility::fast_current_time() - curr_time;
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::SORT_INMEM_SORT_TIME;
    op_monitor_info_.otherstat_3_value_ += sort_cpu_time;
  }
  return ret;
}

int ObSortOpImpl::sort()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!rows_->empty()) {
    // in memory sort
    if (sort_chunks_.is_empty()) {
      iter_.reset();
      if (OB_FAIL(sort_inmem_data())) {
        LOG_WARN("sort in-memory data failed", K(ret));
      } else if (OB_FAIL(iter_.init(&datum_store_))) {
        LOG_WARN("init iterator failed", K(ret));
      } else {
        if (!need_imms()) {
          row_idx_ = 0;
          next_stored_row_func_ = &ObSortOpImpl::array_next_stored_row;
        } else {
          next_stored_row_func_ = &ObSortOpImpl::imms_heap_next_stored_row;
        }
      }
    } else if (OB_FAIL(do_dump())) {
      LOG_WARN("dump failed");
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sort_chunks_.get_size() >= 2) {
    blk_holder_.release();
    set_blk_holder(nullptr);
    // do merge sort
    int64_t ways = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(build_ems_heap(ways))) {
        LOG_WARN("build heap failed", K(ret));
      } else {
        // last merge round,
        if (ways == sort_chunks_.get_size()) {
          break;
        }
        auto input = [&](ObChunkDatumStore *&rs, const ObChunkDatumStore::StoredRow *&row) {
          int ret = OB_SUCCESS;
          ObSortOpChunk *chunk = NULL;
          if (OB_FAIL(ems_heap_next(chunk))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next heap row failed", K(ret));
            }
          } else if (NULL == chunk) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get chunk from heap is NULL", K(ret));
          } else {
            rs = &chunk->datum_store_;
            row = chunk->row_;
          }
          return ret;
        };
        const int64_t level = sort_chunks_.get_first()->level_ + 1;
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SORT_MERGE_SORT_ROUND;
        op_monitor_info_.otherstat_2_value_ = level;
        if (OB_FAIL(build_chunk(level, input))) {
          LOG_WARN("build chunk failed", K(ret));
        } else {
          sql_mem_processor_.set_number_pass(level + 1);
          for (int64_t i = 0; i < ways; i++) {
            ObSortOpChunk *c = sort_chunks_.remove_first();
            c->~ObSortOpChunk();
            mem_context_->get_malloc_allocator().free(c);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      set_blk_holder(&blk_holder_);
      next_stored_row_func_ = &ObSortOpImpl::ems_heap_next_stored_row;
    }
  }
  return ret;
}

int ObSortOpImpl::array_next_stored_row(
  const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (row_idx_ >= rows_->count()
      && ties_array_pos_ >= ties_array_.count()) {
    ret = OB_ITER_END;
  } else if (row_idx_ < rows_->count()) {
    sr = rows_->at(row_idx_);
    row_idx_ += 1;
  } else {
    sr = ties_array_.at(ties_array_pos_);
    ties_array_pos_ += 1;
  }
  return ret;
}

int ObSortOpImpl::imms_heap_next_stored_row(
  const ObChunkDatumStore::StoredRow *&sr)
{
  return imms_heap_next(sr);
}

int ObSortOpImpl::ems_heap_next_stored_row(
  const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  ObSortOpChunk *chunk = NULL;
  if (OB_FAIL(ems_heap_next(chunk))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next heap row failed", K(ret));
    }
  } else if (NULL == chunk || NULL == chunk->row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL chunk or store row", K(ret));
  } else {
    sr = chunk->row_;
  }
  return ret;
}

int ObSortOpImpl::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_rewind_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inited with non rewind support", K(ret));
  } else {
    if (&ObSortOpImpl::array_next_stored_row == next_stored_row_func_) {
      row_idx_ = 0;
    } else {
      if (OB_FAIL(sort())) {
        LOG_WARN("sort rows failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSortOpImpl::get_next_batch_stored_rows(int64_t max_cnt, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("get next batch failed", K(ret));
  } else {
    read_rows = 0;
    blk_holder_.release();
    for (int64_t i = 0; OB_SUCC(ret) && i < max_cnt; i++) {
      const ObChunkDatumStore::StoredRow *sr = NULL;
      if (OB_FAIL((this->*next_stored_row_func_)(sr))) {
        // next_stored_row_func_ is safe to return OB_ITER_END twice.
        if (OB_ITER_END == ret) {
          if (read_rows > 0) {
            ret = OB_SUCCESS;
          }
          break;
        } else {
          LOG_WARN("get stored rows failed", K(ret));
        }
      } else {
        stored_rows_[read_rows++] = const_cast<ObChunkDatumStore::StoredRow *>(sr);
      }
    }
    if (OB_ITER_END == ret && !need_rewind_) {
      reuse();
    }
  }
  return ret;
}

int ObSortOpImpl::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                                 const int64_t max_cnt, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_batch_stored_rows(max_cnt, read_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next batch stored rows", K(ret));
    }
  } else if (read_rows > 0 && OB_FAIL(adjust_topn_read_rows(stored_rows_, read_rows))) {
    LOG_WARN("failed to adjust read rows with ties", K(ret));
  } else {
    ObChunkDatumStore::Iterator::attach_rows(exprs, *eval_ctx_,
        const_cast<const ObChunkDatumStore::StoredRow **>(stored_rows_), read_rows);
  }
  return ret;
}

// if less than heap size or replace heap top, store row is new row
// otherwise, store row will not change as last result obtained.
int ObSortOpImpl::add_heap_sort_row(const common::ObIArray<ObExpr*> &exprs,
                                    const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_) || OB_ISNULL(topn_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_context or heap is not initialized", K(ret));
  } else if (!got_first_row_) {
    got_first_row_ = true;
    // heap sort will extend rowsize twice to reuse the space
    int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_ * 2;
    if (OB_FAIL(sql_mem_processor_.init(
               &mem_context_->get_malloc_allocator(),
               tenant_id_, size, op_monitor_info_.op_type_, op_monitor_info_.op_id_, &eval_ctx_->exec_ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    }
  } else {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
                                &mem_context_->get_malloc_allocator(),
                                [&](int64_t cur_cnt){ return topn_heap_->count() > cur_cnt; },
                                updated))) {
        LOG_WARN("failed to get max available memory size", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (topn_heap_->count() == topn_cnt_ - outputted_rows_cnt_) {
    if (is_fetch_with_ties_ && OB_FAIL(adjust_topn_heap_with_ties(exprs, store_row))) {
      LOG_WARN("failed to adjust topn heap with ties", K(ret));
    } else if (!is_fetch_with_ties_ && OB_FAIL(adjust_topn_heap(exprs, store_row))) {
      LOG_WARN("failed to adjust topn heap", K(ret));
    }
  } else { // push back array
    SortStoredRow *new_row = NULL;
    ObIAllocator &alloc = mem_context_->get_malloc_allocator();
    int64_t topn_heap_size = topn_heap_->count();
    if (OB_FAIL(copy_to_row(exprs, alloc, new_row))) {
      LOG_WARN("failed to generate new row", K(ret));
    } else if (OB_FAIL(topn_heap_->push(new_row))) {
      LOG_WARN("failed to push back row", K(ret));
      if (topn_heap_->count() == topn_heap_size) {
        mem_context_->get_malloc_allocator().free(new_row);
        new_row = NULL;
      }
    } else {
      store_row = new_row;
      LOG_DEBUG("in memory topn sort check add row", KPC(new_row));
    }
  }

  return ret;
}

int ObSortOpImpl::add_heap_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                                      const ObBitVector &skip,
                                      const int64_t batch_size,
                                      const int64_t start_pos /* 0 */,
                                      int64_t *append_row_count)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(batch_size);
  for (int64_t i = start_pos; OB_SUCC(ret) && i < batch_size; i++) {
    if (skip.at(i)) {
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(add_heap_sort_row(exprs, store_row))) {
      LOG_WARN("failed to add topn row", K(ret));
    }
    row_count++;
  }
  if (OB_NOT_NULL(append_row_count)) {
    *append_row_count = row_count;
  }
  return ret;
}

// if less than heap size or replace heap top, store row is new row
// otherwise, store row will not change as last result obtained.
// here, strored_rows_ only used to fetch prev_row in prefix sort batch.
int ObSortOpImpl::add_heap_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                                      const ObBitVector &skip,
                                      const int64_t batch_size,
                                      const uint16_t selector[],
                                      const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(batch_size);
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t idx = selector[i];
    batch_info_guard.set_batch_idx(idx);
    if (OB_FAIL(add_heap_sort_row(exprs, store_row))) {
      LOG_WARN("check need sort failed", K(ret));
    } else if (OB_NOT_NULL(store_row)) {
      stored_rows_[i] = const_cast<ObChunkDatumStore::StoredRow *>(store_row);
    } else if (OB_NOT_NULL(topn_heap_) && OB_NOT_NULL(topn_heap_->top())) {
      stored_rows_[i] = const_cast<ObChunkDatumStore::StoredRow *>(topn_heap_->top());
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObSortOpImpl::adjust_topn_heap(const common::ObIArray<ObExpr*> &exprs,
                                   const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_) || OB_ISNULL(topn_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_context or heap is not initialized", K(ret));
  } else if (OB_ISNULL(topn_heap_->top())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error.top of the heap is NULL", K(ret), K(topn_heap_->count()));
  } else if (!topn_heap_->empty()) {
    if (comp_(&exprs, topn_heap_->top(), *eval_ctx_)) {
      ObIAllocator &alloc = mem_context_->get_malloc_allocator();
      SortStoredRow* new_row = NULL;
      if (OB_FAIL(copy_to_topn_row(exprs, alloc, new_row))) {
        LOG_WARN("failed to generate new row", K(ret));
      } else if (OB_FAIL(topn_heap_->replace_top(new_row))) {
        LOG_WARN("failed to replace top", K(ret));
      } else {
        store_row = new_row;
      }
    } else {
      ret = comp_.ret_;
    }
  }
  return ret;
}

// for order by c1 desc fetch next 5 rows with ties:
//  row < heap.top: add row to ties_array_
//  row = heap.top: add row to ties_array_
//  row > heap.top: 1. replace heap top use row;
//                  2. if previous heap.top = new heap.top, add previous heap.top to ties_array_
//                     else reset ties_array_.
int ObSortOpImpl::adjust_topn_heap_with_ties(const common::ObIArray<ObExpr*> &exprs,
                                             const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_) || OB_ISNULL(topn_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_context or heap is not initialized", K(ret));
  } else if (OB_ISNULL(topn_heap_->top())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error.top of the heap is NULL", K(ret), K(topn_heap_->count()));
  } else if (!topn_heap_->empty()) {
    int cmp = comp_.with_ties_cmp(&exprs, topn_heap_->top(), *eval_ctx_);
    bool is_alloced = false;
    bool add_ties_array = false;
    SortStoredRow *new_row = NULL;
    SortStoredRow *copy_pre_heap_top_row = NULL;
    ObIAllocator &alloc = mem_context_->get_malloc_allocator();
    SortStoredRow *pre_heap_top_row = static_cast<SortStoredRow *>(topn_heap_->top());
    if (OB_FAIL(comp_.ret_) || cmp < 0) {
      /* do nothing */
    } else if (0 == cmp) {
      // equal to heap top, add row to ties array
      int64_t ties_array_size = ties_array_.count();
      if (OB_FAIL(copy_to_row(exprs, alloc, new_row))) {
        LOG_WARN("failed to generate new row", K(ret));
      } else if (OB_FAIL(ties_array_.push_back(new_row))) {
        LOG_WARN("failed to push back ties array", K(ret));
        if (ties_array_size == ties_array_.count()) {
          mem_context_->get_malloc_allocator().free(new_row);
          new_row = NULL;
        }
      } else {
        store_row = new_row;
        LOG_DEBUG("in memory topn sort with ties add ties array", KPC(new_row));
      }
    } else if (OB_FAIL(generate_new_row(pre_heap_top_row, alloc, copy_pre_heap_top_row))) {
      LOG_WARN("failed to generate new row", K(ret));
    } else if (OB_FAIL(copy_to_topn_row(exprs, alloc, new_row))) {
      LOG_WARN("failed to generate new row", K(ret));
    } else if (OB_FAIL(topn_heap_->replace_top(new_row))) {
      LOG_WARN("failed to replace top", K(ret));
    } else if (OB_FALSE_IT(cmp = comp_.with_ties_cmp(copy_pre_heap_top_row, topn_heap_->top()))) {
    } else if (OB_FAIL(comp_.ret_)) {
      /* do nothing */
    } else if (0 != cmp) {
      // previous heap top not equal to new heap top, clear ties array
      LOG_DEBUG("in memory topn sort with ties clear ties array",
                  KPC(new_row), KPC(copy_pre_heap_top_row));
      if (0 != ties_array_.count()) {
        for (int64_t i = 0; i < ties_array_.count(); ++i) {
          sql_mem_processor_.alloc(-1 * ties_array_[i]->get_max_size());
          inmem_row_size_ -= ties_array_[i]->get_max_size();
          mem_context_->get_malloc_allocator().free(ties_array_[i]);
          ties_array_[i] = NULL;
        }
      }
      ties_array_.reset();
      store_row = new_row;
    } else if (OB_FAIL(ties_array_.push_back(copy_pre_heap_top_row))) {
      LOG_WARN("failed to push back ties array", K(ret));
    } else {
      // previous heap top equal to new heap top, add previous heap top to ties array
      store_row = new_row;
      add_ties_array = true;
      LOG_DEBUG("in memory topn sort with ties add ties array",
                  KPC(new_row), KPC(copy_pre_heap_top_row));
    }
    if (!add_ties_array && OB_NOT_NULL(copy_pre_heap_top_row)) {
      sql_mem_processor_.alloc(-1 * copy_pre_heap_top_row->get_max_size());
      inmem_row_size_ -= copy_pre_heap_top_row->get_max_size();
      mem_context_->get_malloc_allocator().free(copy_pre_heap_top_row);
      copy_pre_heap_top_row = NULL;
    }
  }
  return ret;
}

// copy exprs values to topn heap top row.
int ObSortOpImpl::copy_to_topn_row(const common::ObIArray<ObExpr*> &exprs,
                                   ObIAllocator &alloc,
                                   SortStoredRow *&new_row)
{
  int ret = OB_SUCCESS;
  SortStoredRow *top_row = static_cast<SortStoredRow *>(topn_heap_->top());
  if (OB_FAIL(copy_to_row(exprs, alloc, top_row))) {
    LOG_WARN("failed to copy to row", K(ret));
  } else {
    new_row = top_row;
    topn_heap_->top() = static_cast<ObChunkDatumStore::StoredRow *>(top_row);
  }
  return ret;
}

// copy exprs values to row.
// if row space is enough reuse the space, else use the alloc get new space.
int ObSortOpImpl::copy_to_row(const common::ObIArray<ObExpr*> &exprs,
                              ObIAllocator &alloc,
                              SortStoredRow *&row)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t row_size = 0;
  int64_t buffer_len = 0;
  SortStoredRow *dst = NULL;
  SortStoredRow *reclaim_row = NULL;
  //check to see whether this old row's space is adequate for new one
  if (OB_FAIL(ObChunkDatumStore::Block::row_store_size(exprs,
                                                       *eval_ctx_,
                                                       row_size,
                                                       STORE_ROW_EXTRA_SIZE))) {
    LOG_WARN("failed to calc copy size", K(ret));
  } else if (NULL != row && row->get_max_size() >= row_size) {
    buf = reinterpret_cast<char*>(row);
    buffer_len = row->get_max_size();
    dst = row;
  } else {
    buffer_len = row_size * 2;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else {
      sql_mem_processor_.alloc(buffer_len);
      inmem_row_size_ += buffer_len;
      dst = reinterpret_cast<SortStoredRow *>(buf);
      reclaim_row = row;
    }
  }
  if (OB_SUCC(ret)) {
    ObChunkDatumStore::StoredRow *sr = static_cast<ObChunkDatumStore::StoredRow *>(dst);
    if (OB_FAIL(ObChunkDatumStore::StoredRow::build(
          sr, exprs, *eval_ctx_, buf, buffer_len, STORE_ROW_EXTRA_SIZE))) {
      LOG_WARN("build stored row failed", K(ret));
      if (row != dst) {
        reclaim_row = dst;
      }
    } else {
      row = dst;
      row->set_max_size(buffer_len);
    }
  }
  if (NULL != reclaim_row) {
    sql_mem_processor_.alloc(-1 * reclaim_row->get_max_size());
    inmem_row_size_ -= reclaim_row->get_max_size();
    mem_context_->get_malloc_allocator().free(reclaim_row);
    reclaim_row = NULL;
  }
  return ret;
}

//deep copy orign_row use the alloc
int ObSortOpImpl::generate_new_row(SortStoredRow *orign_row,
                                   ObIAllocator &alloc,
                                   SortStoredRow *&new_row)
{
  int ret = OB_SUCCESS;
  new_row = NULL;
  char *buf = NULL;
  if (OB_ISNULL(orign_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc.alloc(orign_row->get_max_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buf failed", K(ret));
  } else if (OB_ISNULL(new_row = new(buf) SortStoredRow())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new row", K(ret));
  } else if (OB_FAIL(new_row->assign(orign_row))) {
    LOG_WARN("stored row assign failed", K(ret));
  } else {
    new_row->set_max_size(orign_row->get_max_size());
    sql_mem_processor_.alloc(new_row->get_max_size());
    inmem_row_size_ += new_row->get_max_size();
  }
  return ret;
}

//deep copy orign_row to last_ties_row_
int ObSortOpImpl::generate_last_ties_row(const ObChunkDatumStore::StoredRow *orign_row)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  CK(NULL == last_ties_row_);
  ObIAllocator &alloc = mem_context_->get_malloc_allocator();
  if (OB_ISNULL(orign_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc.alloc(orign_row->row_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc buf failed", K(ret));
  } else if (OB_ISNULL(last_ties_row_ = new(buf) ObChunkDatumStore::StoredRow())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to new last_tie_row", K(ret));
  } else if (OB_FAIL(last_ties_row_->assign(orign_row))) {
    LOG_WARN("stored row assign failed", K(ret));
  } else {
    sql_mem_processor_.alloc(orign_row->row_size_);
    inmem_row_size_ += last_ties_row_->row_size_;
  }
  return ret;
}

int ObSortOpImpl::adjust_topn_read_rows(ObChunkDatumStore::StoredRow **stored_rows,
                                        int64_t &read_cnt)
{
  int ret = OB_SUCCESS;
  int64_t start_check_pos = -1;

  if (outputted_rows_cnt_ >= topn_cnt_ && !is_fetch_with_ties_) {
    read_cnt = 0;
  } else if (outputted_rows_cnt_ >= topn_cnt_ && is_fetch_with_ties_) {
    start_check_pos = 0;
  } else if (outputted_rows_cnt_ < topn_cnt_ && !is_fetch_with_ties_) {
    read_cnt = min(read_cnt, topn_cnt_ - outputted_rows_cnt_);
  } else if (outputted_rows_cnt_ < topn_cnt_ && is_fetch_with_ties_) {
    if (read_cnt >= topn_cnt_ - outputted_rows_cnt_) {
      start_check_pos = topn_cnt_ - outputted_rows_cnt_;
      if (OB_FAIL(generate_last_ties_row(stored_rows[start_check_pos - 1]))) {
        LOG_WARN("failed to generate last ties row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (start_check_pos >= 0) {
      int64_t with_ties_row_cnt = 0;
      for (int64_t i = start_check_pos; i < read_cnt; ++i) {
        if (0 == comp_.with_ties_cmp(stored_rows[i], last_ties_row_)) {
          with_ties_row_cnt++;
        } else {
          break;
        }
      }
      read_cnt = start_check_pos + with_ties_row_cnt;
    }
    if (OB_UNLIKELY(OB_FAIL(comp_.ret_))) {
      ret = comp_.ret_;
    } else if (0 == read_cnt) {
      ret = OB_ITER_END;
    } else {
      outputted_rows_cnt_ += read_cnt;
    }
  }

  return ret;
}

void ObSortOpImpl::set_blk_holder(ObChunkDatumStore::IteratedBlockHolder *blk_holder)
{
  DLIST_FOREACH_NORET(chunk, sort_chunks_) {
    chunk->iter_.set_blk_holder_ptr(blk_holder);
  }
}

/************************************* end ObSortOpImpl ********************************/

/*********************************** start ObPrefixSortImpl *****************************/
ObPrefixSortImpl::ObPrefixSortImpl(ObMonitorNode &op_monitor_info) : ObSortOpImpl(op_monitor_info), prefix_pos_(0),
    full_sort_collations_(nullptr), full_sort_cmp_funs_(nullptr),
    base_sort_collations_(), base_sort_cmp_funs_(),
    prev_row_(nullptr), next_prefix_row_store_(), next_prefix_row_(nullptr),
    child_(nullptr), self_op_(nullptr), sort_row_count_(nullptr),
    selector_(nullptr), selector_size_(0), sort_prefix_rows_(0), immediate_prefix_store_(ObModIds::OB_SQL_SORT_ROW),
    immediate_prefix_rows_(nullptr), immediate_prefix_pos_(0), brs_(NULL)
{
}

void ObPrefixSortImpl::reset()
{
  prefix_pos_ = 0;
  full_sort_collations_ = nullptr;
  base_sort_collations_.reset();
  base_sort_cmp_funs_.reset();
  brs_holder_.reset();
  next_prefix_row_store_.reset();
  next_prefix_row_ = nullptr;
  prev_row_ = nullptr;
  child_ = nullptr;
  self_op_ = nullptr;
  sort_row_count_ = nullptr;

  selector_size_ = 0;
  sort_prefix_rows_ = 0;
  immediate_prefix_store_.reset();
  immediate_prefix_rows_ = 0;
  immediate_prefix_pos_ = 0;
  brs_ = NULL;

  ObSortOpImpl::reset();
}

int ObPrefixSortImpl::init(const int64_t tenant_id,
    const int64_t prefix_pos,
    const common::ObIArray<ObExpr *> &all_exprs,
    const ObIArray<ObSortFieldCollation> *sort_collations,
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
    ObEvalCtx *eval_ctx,
    ObOperator *child_op,
    ObOperator *self_op,
    ObExecContext &exec_ctx,
    bool enable_encode_sortkey,
    int64_t &sort_row_cnt,
    int64_t topn_cnt,
    bool is_fetch_with_ties)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funs)
      || OB_ISNULL(eval_ctx) || OB_ISNULL(child_op) || OB_ISNULL(self_op)
      || prefix_pos <= 0 || prefix_pos > sort_collations->count()
      || sort_collations->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(prefix_pos));
  } else {
    int64_t batch_size = eval_ctx->max_batch_size_;
    prefix_pos_ = prefix_pos;
    full_sort_collations_ = sort_collations;
    full_sort_cmp_funs_ = sort_cmp_funs;
    // NOTE: %cnt may be zero, some plan is wrong generated with prefix sort:
    // %prefix_pos == %sort_columns.count(), the sort operator should be eliminated but not.
    //
    // To be compatible with this plan, we keep this behavior.
    const int64_t cnt = sort_collations->count() - prefix_pos;
    base_sort_collations_.init(cnt,
        const_cast<ObSortFieldCollation*>(&sort_collations->at(0) + prefix_pos), cnt);
    base_sort_cmp_funs_.init(cnt,
        const_cast<ObSortCmpFunc*>(&sort_cmp_funs->at(0) + prefix_pos), cnt);
    prev_row_ = nullptr;
    next_prefix_row_ = nullptr;
    child_ = child_op;
    self_op_ = self_op;
    exec_ctx_ = &exec_ctx;
    sort_row_count_ = &sort_row_cnt;
    if (OB_FAIL(ObSortOpImpl::init(tenant_id, &base_sort_collations_, &base_sort_cmp_funs_,
                                   eval_ctx, &exec_ctx, enable_encode_sortkey, false, false,
                                   0, topn_cnt, is_fetch_with_ties))) {
      LOG_WARN("sort impl init failed", K(ret));
    } else if (batch_size <= 0) {
      if (OB_FAIL(next_prefix_row_store_.init(mem_context_->get_malloc_allocator(),
                                              all_exprs.count()))) {
        LOG_WARN("failed to init next prefix row store", K(ret));
      } else if (OB_FAIL(fetch_rows(all_exprs))) {
        LOG_WARN("fetch rows failed");
      }
    } else {
      selector_ = (typeof(selector_))eval_ctx->exec_ctx_.get_allocator().alloc(
          batch_size * sizeof(*selector_));
      immediate_prefix_rows_ = (typeof(immediate_prefix_rows_))
          eval_ctx->exec_ctx_.get_allocator().alloc(batch_size * sizeof(*immediate_prefix_rows_));
      if (NULL == selector_ || NULL == immediate_prefix_rows_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed",
                 K(ret), K(batch_size), KP(selector_), KP(immediate_prefix_rows_));
      } else if (OB_FAIL(immediate_prefix_store_.init(
                  INT64_MAX /* mem limit, big enough to hold all rows in memory */,
                  tenant_id_, ObCtxIds::WORK_AREA, ObModIds::OB_SQL_SORT_ROW,
                  false /*+ disable dump */))) {
        LOG_WARN("init row store failed", K(ret));
      } else if (OB_FAIL(brs_holder_.init(all_exprs, *eval_ctx))) {
        LOG_WARN("init batch result holder failed", K(ret));
      } else if (OB_FAIL(fetch_rows_batch(all_exprs))) {
        LOG_WARN("fetch rows in batch manner failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::fetch_rows(const common::ObIArray<ObExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSortOpImpl::reuse();
    int64_t row_count = 0;
    bool sort_need_dump = false;
    prev_row_ = NULL;
    if (NULL != next_prefix_row_) {
      // Restore next_prefix_row_ to expressions, to make sure no overwrite of expression's value
      // when get row from child.
      row_count += 1;
      if (OB_FAIL(next_prefix_row_store_.restore(all_exprs, *eval_ctx_))) {
        LOG_WARN("restore expr values failed", K(ret));
      } else if (OB_FAIL(add_row(all_exprs, prev_row_))) {
        LOG_WARN("add row to sort impl failed", K(ret));
      } else if (OB_ISNULL(prev_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add stored row is NULL", K(ret));
      } else {
        next_prefix_row_ = NULL;
        LOG_DEBUG("trace restore row", K(ObToStringExprRow(*eval_ctx_, all_exprs)));
      }
    }
    while (OB_SUCC(ret)) {
      self_op_->clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END == ret) {
          // Set %next_prefix_row_ to NULL to indicate that all rows are fetched.
          next_prefix_row_ = NULL;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
        break;
      } else {
        *sort_row_count_ += 1;
        row_count += 1;
        // check the prefix is the same with previous row
        bool same_prefix = true;
        if (NULL != prev_row_) {
          const ObDatum *rcells = prev_row_->cells();
          ObDatum *l_datum = nullptr;
          int cmp_ret = 0;
          for (int64_t i = 0; same_prefix && i < prefix_pos_ && OB_SUCC(ret); i++) {
            const int64_t idx = full_sort_collations_->at(i).field_idx_;
            if (OB_FAIL(all_exprs.at(idx)->eval(*eval_ctx_, l_datum))) {
              LOG_WARN("failed to eval expr", K(ret));
            } else if (OB_FAIL(full_sort_cmp_funs_->at(i).cmp_func_(*l_datum, rcells[idx], cmp_ret))) {
              LOG_WARN("failed to compare", K(ret));
            } else {
              same_prefix = (0 == cmp_ret);
            }
          }
        }
        if (!same_prefix) {
          // row are saved in %next_prefix_row_, will be added in the next call
          if (OB_FAIL(next_prefix_row_store_.shadow_copy(all_exprs, *eval_ctx_))) {
            LOG_WARN("failed to add datum row", K(ret));
          } else {
            next_prefix_row_ = next_prefix_row_store_.get_store_row();
          }
          break;
        }
        if (OB_FAIL(add_row(all_exprs, prev_row_))) {
          LOG_WARN("add row to sort impl failed", K(ret));
        } else if (OB_ISNULL(prev_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add stored row is NULL", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && row_count > 0) {
      if (OB_FAIL(ObSortOpImpl::sort())) {
        LOG_WARN("sort failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::get_next_row(const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(ObSortOpImpl::get_next_row(exprs))) {
      if (OB_ITER_END == ret) {
        if (NULL != next_prefix_row_ && outputted_rows_cnt_ < topn_cnt_) {
          if (OB_FAIL(fetch_rows(exprs))) {
            LOG_WARN("fetch rows failed", K(ret));
          } else if (OB_FAIL(ObSortOpImpl::get_next_row(exprs))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("sort impl get next row failed", K(ret));
            }
          }
        }
      } else {
        LOG_WARN("sort impl get next row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::is_same_prefix(const ObChunkDatumStore::StoredRow *store_row,
                                      const common::ObIArray<ObExpr *> &all_exprs,
                                      const int64_t datum_idx,
                                      bool &same)
{
  int ret = OB_SUCCESS;
  same = true;
  int cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix_pos_ && same; i++) {
    const int64_t idx = full_sort_collations_->at(i).field_idx_;
    ObExpr *e = all_exprs.at(idx);
    // for non batch result expression, datum should always be the same.
    if (e->is_batch_result()) {
      if (OB_FAIL(full_sort_cmp_funs_->at(i).cmp_func_(store_row->cells()[idx],
                                                       e->locate_batch_datums(*eval_ctx_)[datum_idx],
                                                       cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        same = (0 == cmp_ret);
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::is_same_prefix(const common::ObIArray<ObExpr *> &all_exprs,
                                      const int64_t datum_idx1,
                                      const int64_t datum_idx2,
                                      bool &same)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  same = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix_pos_ && same; i++) {
    const int64_t idx = full_sort_collations_->at(i).field_idx_;
    ObExpr *e = all_exprs.at(idx);
    if (e->is_batch_result()) {
      if (OB_FAIL(full_sort_cmp_funs_->at(i).cmp_func_(
              e->locate_batch_datums(*eval_ctx_)[datum_idx1],
              e->locate_batch_datums(*eval_ctx_)[datum_idx2],
              cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else {
        same = (0 == cmp_ret);
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::add_immediate_prefix(const common::ObIArray<ObExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  int64_t pos = immediate_prefix_store_.get_row_cnt();
  if (OB_FAIL(immediate_prefix_store_.add_batch(
              all_exprs, *eval_ctx_, *brs_->skip_, brs_->size_, selector_, selector_size_,
              immediate_prefix_rows_ + pos))) {
    LOG_WARN("add batch failed", K(ret));
  } else if (!comp_.is_inited()
             && OB_FAIL(comp_.init(sort_collations_, sort_cmp_funs_,
                 exec_ctx_, enable_encode_sortkey_ && !(part_cnt_ > 0)))) {
    LOG_WARN("init compare failed", K(ret));
  } else {
    std::sort(immediate_prefix_rows_ + pos, immediate_prefix_rows_ + pos + selector_size_,
              CopyableComparer(comp_));
    if (OB_SUCCESS != comp_.ret_) {
      ret = comp_.ret_;
      LOG_WARN("compare failed", K(ret));
    }
  }
  return ret;
}

// Fetch rows from child until new prefix found or iterate end.
// One batch rows form child are split into three part:
// 1. Sort prefix: same prefix with rows in ObSortOpImpl
// 2. Immediate prefixes: prefix which boundaries in current batch, added to
//    %immediate_prefix_store_ and sorted by std::sort.
// 3. Next prefix: last prefix of current batch, rows keep in expression, added to
//    ObSortOpImpl in next fetch_rows_batch().
//
// E.g.:
//
//    1 <-- sort prefix start
//    1
//    1 <-- sort prefix end
//    2 <-- immediate prefixes start
//    3
//    3
//    3 <-- immediate prefixes end
//    4 <-- next prefix start
//    4
//    4 <-- next prefix end
//
int ObPrefixSortImpl::fetch_rows_batch(const common::ObIArray<ObExpr *> &all_exprs)
{
  int ret = OB_SUCCESS;
  ObSortOpImpl::reuse();
  sort_prefix_rows_ = 0;
  if (OB_FAIL(brs_holder_.restore())) {
    LOG_WARN("restore batch result failed", K(ret));
  } else if (selector_size_ > 0) {
    // next prefix rows in previous fetch_rows_batch().
    if (OB_FAIL(add_batch(all_exprs, *brs_->skip_, brs_->size_,
                          selector_, selector_size_))) {
      LOG_WARN("add batch failed", K(ret));
    } else {
      sort_prefix_rows_ += selector_size_;
      prev_row_ = stored_rows_[selector_size_ - 1];
    }
    selector_size_ = 0;
  }
  immediate_prefix_pos_ = 0;
  if (immediate_prefix_store_.get_row_cnt() > 0) {
    immediate_prefix_store_.reset();
  }

  bool found_new_prefix = false;
  while (OB_SUCC(ret) && !found_new_prefix) {
    self_op_->clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(self_op_->get_spec().max_batch_size_, brs_))) {
      LOG_WARN("get next batch failed", K(ret));
    } else {
      // evaluate all expression and set projected, no need to evaluate any more.
      for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); i++) {
        ObExpr *e = all_exprs.at(i);
        if (OB_FAIL(e->eval_batch(*eval_ctx_, *brs_->skip_, brs_->size_))) {
          LOG_WARN("eval batch failed", K(ret));
        } else {
          e->get_eval_info(*eval_ctx_).projected_ = true;
        }
      }
      selector_size_ = 0;
      int64_t new_prefix = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_->size_; i++) {
        if (brs_->skip_->at(i)) {
          continue;
        }
        *sort_row_count_ += 1;
        if (new_prefix < 0) {
          bool is_same = false;
          if (NULL != prev_row_ && OB_FAIL(is_same_prefix(prev_row_, all_exprs, i, is_same))) {
            LOG_WARN("check same prefix failed", K(ret));
          } else if (NULL == prev_row_ && OB_FAIL(is_same_prefix(all_exprs, 0, i, is_same))) {
            LOG_WARN("check same prefix failed", K(ret));
          } else if (is_same) {
            selector_[selector_size_++] = i;
          } else {
            if (0 == selector_size_) {
              // do nothing
            } else if (OB_FAIL(add_batch(all_exprs, *brs_->skip_, brs_->size_,
                                         selector_, selector_size_))) {
              LOG_WARN("add batch failed", K(ret));
            } else {
              sort_prefix_rows_ += selector_size_;
              prev_row_ = stored_rows_[selector_size_ - 1];
            }
            new_prefix = i;
            selector_size_ = 1;
            selector_[0] = i;
          }
          continue;
        }
        if (new_prefix >= 0) {
          bool is_same = false;
          if (OB_FAIL(is_same_prefix(all_exprs, new_prefix, i, is_same))) {
            LOG_WARN("check same prefix failed", K(ret));
          } else if (!is_same) {
            if (OB_FAIL(add_immediate_prefix(all_exprs))) {
              LOG_WARN("add immediate prefix failed", K(ret));
            } else {
              new_prefix = i;
              selector_size_ = 1;
              selector_[0] = i;
            }
          } else {
            selector_[selector_size_++] = i;
          }
        }
      } // end for

      if (selector_size_ > 0 && OB_SUCC(ret)) {
        if (new_prefix < 0) {
          if (OB_FAIL(add_batch(all_exprs, *brs_->skip_, brs_->size_,
                                selector_, selector_size_))) {
            LOG_WARN("add batch failed", K(ret));
          } else {
            sort_prefix_rows_ += selector_size_;
            prev_row_ = stored_rows_[selector_size_ - 1];
          }
          selector_size_ = 0;
        } else {
          if (brs_->end_) {
            // add last immediate prefix rows
            if (OB_FAIL(add_immediate_prefix(all_exprs))) {
              LOG_WARN("add immediate prefix failed", K(ret));
            }
            selector_size_ = 0;
          }
        }
      }
      found_new_prefix = new_prefix >= 0 || brs_->end_;
      if (found_new_prefix && OB_SUCC(ret)) {
        // child not iterate end, need backup child expression datums
        const int64_t cnt = std::min(sort_prefix_rows_ + immediate_prefix_store_.get_row_cnt(),
                                     (int64_t)self_op_->get_spec().max_batch_size_);
        OZ(brs_holder_.save(cnt));
      }
    }
  } // end while

  if (OB_SUCC(ret) && sort_prefix_rows_ > 0) {
    ret = ObSortOpImpl::sort();
  }

  return ret;
}

int ObPrefixSortImpl::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                                     const int64_t max_cnt, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    read_rows = 0;
    // Read rows from sort prefix or immediate prefixes,
    // fetch_rows_batch() and try again if no row read
    const int64_t max_loop_cnt = 2;
    for (int64_t loop = 0; OB_SUCC(ret) && loop < max_loop_cnt && 0 == read_rows; loop++) {
      if (sort_prefix_rows_ > 0) {
        if (OB_FAIL(get_next_batch_stored_rows(max_cnt, read_rows))) {
          if (OB_ITER_END == ret) {
            sort_prefix_rows_ = 0;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get next batch stored rows failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && immediate_prefix_pos_ < immediate_prefix_store_.get_row_cnt()) {
        int64_t cnt = std::min(max_cnt - read_rows,
                               immediate_prefix_store_.get_row_cnt() - immediate_prefix_pos_);
        MEMCPY(stored_rows_ + read_rows,
               immediate_prefix_rows_ + immediate_prefix_pos_,
               cnt * sizeof(*stored_rows_));
        immediate_prefix_pos_ += cnt;
        read_rows += cnt;
      }

      if (OB_SUCC(ret)) {
        if (read_rows > 0 && OB_FAIL(adjust_topn_read_rows(stored_rows_, read_rows))) {
          LOG_WARN("adjust read rows with ties failed", K(ret));
        } else if (read_rows > 0) {
          ObChunkDatumStore::Iterator::attach_rows(exprs, *eval_ctx_,
              const_cast<const ObChunkDatumStore::StoredRow **>(stored_rows_), read_rows);
        } else {
          if (0 == loop && (NULL == brs_ || !brs_->end_) && outputted_rows_cnt_ < topn_cnt_) {
            if (OB_FAIL(fetch_rows_batch(exprs))) {
              LOG_WARN("fetch rows in batch manner failed", K(ret));
            }
          } else {
            ret = OB_ITER_END;
          }
        }
      }
    }
  }
  return ret;
}

/*********************************** end ObPrefixSortImpl *****************************/

/*********************************** start ObUniqueSortImpl *****************************/
int ObUniqueSortImpl::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                                     const int64_t max_cnt, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  int64_t tmp_read_rows = 0;
  while (OB_SUCC(ret) && 0 == tmp_read_rows) {
    if (OB_FAIL(get_next_batch_stored_rows(max_cnt, read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch stored_rows", K(ret));
      }
    } else {
      for (int64_t nth_row = 0; OB_SUCC(ret) && nth_row < read_rows; ++nth_row) {
        ObChunkDatumStore::StoredRow *cur_row = stored_rows_[nth_row];
        if (NULL != prev_row_ || 0 < nth_row) {
          const ObDatum *lcells = NULL != prev_row_
                                  ? prev_row_->cells()
                                  : stored_rows_[nth_row - 1]->cells();
          const ObDatum *rcells = cur_row->cells();
          int cmp = 0;
          for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_cmp_funs_->count(); i++) {
            const int64_t idx = sort_collations_->at(i).field_idx_;
            if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
              LOG_WARN("compare failed", K(ret));
            }
          }
          LOG_DEBUG("debug cmp unique key", K(cmp));
          if (OB_FAIL(ret)) {
          } else if (0 == cmp) {
          } else {
            free_prev_row();
            if (nth_row != tmp_read_rows) {
              // move to previous row to data dense
              stored_rows_[tmp_read_rows] = stored_rows_[nth_row];
            }
            ++tmp_read_rows;
          }
        } else {
          if (nth_row != tmp_read_rows) {
            // move to previous row to data dense
            stored_rows_[tmp_read_rows] = stored_rows_[nth_row];
          }
          ++tmp_read_rows;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 < tmp_read_rows || OB_ISNULL(prev_row_)) {
        if (OB_FAIL(save_prev_row(*(stored_rows_[read_rows - 1])))) {
          LOG_WARN("save prev row failed", K(ret));
        } else {
          ObChunkDatumStore::Iterator::attach_rows(exprs, *eval_ctx_,
              const_cast<const ObChunkDatumStore::StoredRow **>(stored_rows_), tmp_read_rows);
          read_rows = tmp_read_rows;
        }
      }
    }
  }
  return ret;
}

int ObUniqueSortImpl::get_next_row(const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObSortOpImpl::get_next_row(exprs, sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
      break;
    } else if (NULL == sr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL store row returned", K(ret));
    } else {
      if (NULL != prev_row_) {
        const ObDatum *lcells = prev_row_->cells();
        const ObDatum *rcells = sr->cells();
        int cmp = 0;
        for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_cmp_funs_->count(); i++) {
          const int64_t idx = sort_collations_->at(i).field_idx_;
          if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
            LOG_WARN("compare failed", K(ret));
          }
        }
        if (0 == cmp) {
          continue;
        }
      }
      if (OB_FAIL(save_prev_row(*sr))) {
        LOG_WARN("save prev row failed", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObUniqueSortImpl::get_next_stored_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObSortOpImpl::get_next_row(sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
      break;
    } else if (NULL == sr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL store row returned", K(ret));
    } else {
      if (NULL != prev_row_) {
        const ObDatum *lcells = prev_row_->cells();
        const ObDatum *rcells = sr->cells();
        int cmp = 0;
        for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < sort_cmp_funs_->count(); i++) {
          const int64_t idx = sort_collations_->at(i).field_idx_;
          if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx], cmp))) {
            LOG_WARN("compare failed", K(ret));
          }
        }
        if (0 == cmp) {
          continue;
        }
      }
      if (OB_FAIL(save_prev_row(*sr))) {
        LOG_WARN("save prev row failed", K(ret));
      }
      break;
    }
  }
  return ret;
}

void ObUniqueSortImpl::free_prev_row()
{
  if (NULL != prev_row_ && NULL != mem_context_) {
    mem_context_->get_malloc_allocator().free(prev_row_);
    prev_row_ = NULL;
    prev_buf_size_ = 0;
  }
}

void ObUniqueSortImpl::reuse()
{
  free_prev_row();
  ObSortOpImpl::reuse();
}

void ObUniqueSortImpl::reset()
{
  free_prev_row();
  ObSortOpImpl::reset();
}

int ObUniqueSortImpl::save_prev_row(const ObChunkDatumStore::StoredRow &sr)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_UNLIKELY(sr.row_size_ > prev_buf_size_)) {
      free_prev_row();
      // allocate more memory to avoid too much memory alloc times.
      const int64_t size = sr.row_size_ * 2;
      prev_row_ = static_cast<ObChunkDatumStore::StoredRow *>(
          mem_context_->get_malloc_allocator().alloc(size));
      if (NULL == prev_row_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        prev_buf_size_ = size;
        prev_row_ = new (prev_row_) ObChunkDatumStore::StoredRow();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prev_row_->assign(&sr))) {
        LOG_WARN("store row assign failed", K(ret));
      }
    }
  }
  return ret;
}
/*********************************** end ObUniqueSortImpl *****************************/
} // end namespace sql
} // end namespace oceanbase
