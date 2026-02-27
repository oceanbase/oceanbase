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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_STRATEGY_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_STRATEGY_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_i_sort_vec_op_impl.h"
#include "sql/engine/sort/ob_sort_adaptive_qs_vec_op.h"
#include "sql/engine/sort/ob_sort_compare_vec_op.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_key_fetcher_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_eager_filter.h"
#include "sql/engine/sort/ob_sort_vec_op_store_row_factory.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/sort/ob_pd_topn_sort_filter.h"
#include "sql/engine/sort/ob_partition_topn_sort_vec_op.h"

namespace oceanbase
{
namespace sql
{

// Forward declarations to avoid heavy includes
template <typename Compare, typename Store_Row, bool has_addon>
class ObSortVecOpImpl;

struct RowMeta;

template<typename Compare, typename Store_Row, bool has_addon>
class ObISortStrategy
{
public:
  virtual ~ObISortStrategy() {}
  virtual int sort_inmem_data(const int64_t begin, const int64_t end, common::ObIArray<Store_Row *> *rows) = 0;
};

template<typename Compare, typename Store_Row, bool has_addon>
class ObFullSortStrategy : public ObISortStrategy<Compare, Store_Row, has_addon>
{
public:
  ObFullSortStrategy(Compare &comp,
      const RowMeta *sk_row_meta,
      lib::MemoryContext *mem_context,
      const bool enable_encode_sortkey)
  :
    enable_encode_sortkey_(enable_encode_sortkey),
    is_fixed_key_sort_enabled_(false),
    fixed_sort_key_len_(0),
    comp_(comp),
    sk_row_meta_(sk_row_meta),
    mem_context_(mem_context)
  {}
  virtual ~ObFullSortStrategy() {}
  virtual int sort_inmem_data(const int64_t begin, const int64_t end, common::ObIArray<Store_Row *> *rows) override;

private:
  int do_fixed_key_sort(int64_t begin, common::ObIArray<Store_Row *> *rows);

private:
  class CopyableComparer
  {
  public:
    explicit CopyableComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const Store_Row *l, const Store_Row *r)
    {
      return compare_(l, r);
    }
    Compare &compare_;
  };
  bool enable_encode_sortkey_;
  bool is_fixed_key_sort_enabled_;
  int64_t fixed_sort_key_len_;
  Compare &comp_;
  const RowMeta *sk_row_meta_;
  lib::MemoryContext *mem_context_;
};

template <typename Compare, typename Store_Row, bool has_addon>
int ObFullSortStrategy<Compare, Store_Row, has_addon>::do_fixed_key_sort(int64_t begin, common::ObIArray<Store_Row *> *rows)
{
  #define FIXED_KEY_SORT(sort_key_len)                                          \
    case (sort_key_len): {                                                       \
      FixedKeySort<Store_Row, sort_key_len> fixed_key_sort(                   \
          *rows, *sk_row_meta_, mem_context_->ref_context()->get_malloc_allocator());          \
      if (OB_FAIL(fixed_key_sort.init(*rows,                                   \
                                      mem_context_->ref_context()->get_malloc_allocator(),     \
                                      begin, rows->count(), can_encode))) {    \
        SQL_ENG_LOG(WARN, "failed to init fixed_key_sort", K(ret));             \
      } else if (can_encode) {                                                   \
        fixed_key_sort.sort(begin, rows->count());                             \
      }                                                                          \
      break;                                                                     \
    }
  int ret = OB_SUCCESS;
  bool can_encode = true;
  switch (fixed_sort_key_len_) {
    FIXED_KEY_SORT(2)
    FIXED_KEY_SORT(3)
    FIXED_KEY_SORT(4)
    FIXED_KEY_SORT(5)
    FIXED_KEY_SORT(6)
    FIXED_KEY_SORT(7)
    FIXED_KEY_SORT(8)
    FIXED_KEY_SORT(9)
    FIXED_KEY_SORT(10)
    FIXED_KEY_SORT(11)
    FIXED_KEY_SORT(12)
    FIXED_KEY_SORT(13)
    FIXED_KEY_SORT(14)
    FIXED_KEY_SORT(15)
    FIXED_KEY_SORT(16)
    FIXED_KEY_SORT(17)
    FIXED_KEY_SORT(18)
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected encode sort key len", K(ret), K(fixed_sort_key_len_));
    }
  }
  if (OB_SUCC(ret) && !can_encode) {
    enable_encode_sortkey_ = false;
    comp_.fallback_to_disable_encode_sortkey();
    lib::ob_sort(&rows->at(begin), &rows->at(0) + rows->count(), CopyableComparer(comp_));
  }
  return ret;
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObFullSortStrategy<Compare, Store_Row, has_addon>::sort_inmem_data(const int64_t begin, const int64_t end, common::ObIArray<Store_Row *> *rows)
{
  int ret = OB_SUCCESS;
  UNUSED(end);
  if (OB_ISNULL(rows) || rows->count() == 0 || begin >= rows->count()) {
    // empty array or invalid range, no need to sort
  } else if (enable_encode_sortkey_) {
    if (is_fixed_key_sort_enabled_) {
      if (OB_FAIL(do_fixed_key_sort(begin, rows))) {
        SQL_ENG_LOG(WARN, "failed to do fixed key sort", K(ret));
      }
    } else {
      bool can_encode = true;
      ObAdaptiveQS<Store_Row> aqs(*rows, *sk_row_meta_,
                                  mem_context_->ref_context()->get_malloc_allocator());
      if (OB_FAIL(aqs.init(*rows, mem_context_->ref_context()->get_malloc_allocator(), begin,
                           rows->count(), can_encode))) {
        SQL_ENG_LOG(WARN, "failed to init aqs", K(ret));
      } else if (can_encode) {
        aqs.sort(begin, rows->count());
      } else {
        enable_encode_sortkey_ = false;
        comp_.fallback_to_disable_encode_sortkey();
        lib::ob_sort(&rows->at(begin), &rows->at(0) + rows->count(),
                     CopyableComparer(comp_));
      }
    }
  } else {
    lib::ob_sort(&rows->at(begin), &rows->at(0) + rows->count(),
                 CopyableComparer(comp_));
  }
  return ret;
}

template<typename Compare, typename Store_Row, bool has_addon>
class ObPartitionSortStrategy : public ObISortStrategy<Compare, Store_Row, has_addon>
{
public:
  struct PartHashNode
  {
    PartHashNode() : hash_node_next_(nullptr), part_row_next_(nullptr), store_row_(nullptr)
    {}
    ~PartHashNode()
    {}
  public:
    PartHashNode *hash_node_next_;
    PartHashNode *part_row_next_;
    Store_Row *store_row_;
    TO_STRING_EMPTY();
  };
  using BucketArray = common::ObSegmentArray<PartHashNode *,
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      common::ModulePageAllocator>;
  using BucketNodeArray = common::ObSegmentArray<PartHashNode,
      OB_MALLOC_MIDDLE_BLOCK_SIZE,
      common::ModulePageAllocator>;
  static const int64_t FIXED_PART_NODE_SIZE = sizeof(PartHashNode);
  static const int64_t FIXED_PART_BKT_SIZE = sizeof(PartHashNode *);
  static const int64_t MAX_ROW_CNT = 268435456; // (2G / 8)
  ObPartitionSortStrategy(
    Compare &comp,
    const RowMeta &row_meta,
    int64_t part_cnt,
    const common::ObIArray<ObExpr *> *sk_exprs,
    const ObIArray<ObSortFieldCollation> *sk_collations,
    ObIAllocator &allocator)
  :
    comp_(comp),
    row_meta_(row_meta),
    part_cnt_(part_cnt),
    buckets_(nullptr),
    part_hash_nodes_(nullptr),
    max_bucket_cnt_(0),
    max_node_cnt_(0),
    enable_encode_sortkey_(false),
    sk_exprs_(sk_exprs),
    sk_collations_(sk_collations),
    allocator_(allocator),
    page_allocator_("PartSortBucket", MTL_ID(), ObCtxIds::WORK_AREA)
  {
  }
  virtual ~ObPartitionSortStrategy()
  {
    if (nullptr != buckets_) {
      buckets_->~BucketArray();
      allocator_.free(buckets_);
      buckets_ = nullptr;
    }
    if (nullptr != part_hash_nodes_) {
      part_hash_nodes_->~BucketNodeArray();
      allocator_.free(part_hash_nodes_);
      part_hash_nodes_ = nullptr;
    }
  }
  virtual int sort_inmem_data(const int64_t rows_begin, const int64_t rows_end, common::ObIArray<Store_Row *> *rows) override;
  void resue();
private:
  class HashNodeComparer
  {
  public:
    explicit HashNodeComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const PartHashNode *l, const PartHashNode *r)
    {
      return compare_(l->store_row_, r->store_row_);
    }
    Compare &compare_;
  };
  class CopyableComparer
  {
  public:
    explicit CopyableComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const Store_Row *l, const Store_Row *r)
    {
      return compare_(l, r);
    }
    Compare &compare_;
  };
private:
  Compare &comp_;
  const RowMeta &row_meta_;
  int64_t part_cnt_;
  BucketArray *buckets_;
  BucketNodeArray *part_hash_nodes_;
  int64_t max_bucket_cnt_;
  int64_t max_node_cnt_;
  bool enable_encode_sortkey_;
  const common::ObIArray<ObExpr *> *sk_exprs_;
  const ObIArray<ObSortFieldCollation> *sk_collations_;
  ObIAllocator &allocator_;
  common::ModulePageAllocator page_allocator_;
private:
  // Compare part key equality using comp_. Equal iff l !< r and r !< l within partition key range.
  int is_equal_part(const Store_Row *l, const Store_Row *r, const RowMeta &row_meta, bool &is_equal)
  {
    int ret = OB_SUCCESS;
    is_equal = true;
    if (OB_ISNULL(l) && OB_ISNULL(r)) {
      // do nothing
    } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
      is_equal = false;
    } else {
      int64_t hash_idx = sk_collations_->at(0).field_idx_;
      const uint64_t l_hash_value =
        *(reinterpret_cast<const uint64_t *>(l->get_cell_payload(row_meta, hash_idx)));
      const uint64_t r_hash_value =
        *(reinterpret_cast<const uint64_t *>(r->get_cell_payload(row_meta, hash_idx)));
      if (l_hash_value != r_hash_value) {
        is_equal = false;
      } else {
        int cmp_ret = 0;
        ObLength l_len = 0;
        ObLength r_len = 0;
        bool l_null = false;
        bool r_null = false;
        const char *l_data = nullptr;
        const char *r_data = nullptr;
        for (int64_t i = 1; is_equal && i <= part_cnt_; ++i) {
          const int64_t idx = sk_collations_->at(i).field_idx_;
          const ObExpr *e = sk_exprs_->at(idx);
          auto &sort_cmp_fun = NULL_FIRST == sk_collations_->at(i).null_pos_ ?
                                 e->basic_funcs_->row_null_first_cmp_ :
                                 e->basic_funcs_->row_null_last_cmp_;
          l_null = l->is_null(idx);
          r_null = r->is_null(idx);
          if (l_null != r_null) {
            is_equal = false;
          } else if (l_null && r_null) {
            is_equal = true;
          } else {
            l->get_cell_payload(row_meta, idx, l_data, l_len);
            r->get_cell_payload(row_meta, idx, r_data, r_len);
            if (l_len == r_len && (0 == memcmp(l_data, r_data, l_len))) {
              is_equal = true;
            } else if (OB_FAIL(sort_cmp_fun(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data,
                                            r_len, r_null, cmp_ret))) {
              SQL_ENG_LOG(WARN, "failed to compare", K(ret));
            } else {
              is_equal = (0 == cmp_ret);
            }
          }
        }
      }
    }
    return ret;
  }
  template <typename ArrayType>
  int prepare_bucket_array(ArrayType *&arr, uint64_t cnt)
  {
    int ret = OB_SUCCESS;
    if (nullptr == arr) {
      ArrayType *tmp = static_cast<ArrayType *>(allocator_.alloc(sizeof(ArrayType)));
      if (nullptr == tmp) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate array", K(ret));
      } else {
        arr = new (tmp) ArrayType(page_allocator_);
      }
    } else {
      arr->reuse();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(arr->prepare_allocate(cnt))) {
        SQL_ENG_LOG(WARN, "failed to prepare allocate array", K(ret), K(cnt));
      }
    }
    return ret;
  }
};

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionSortStrategy<Compare, Store_Row, has_addon>::sort_inmem_data(const int64_t rows_begin, const int64_t rows_end,
   common::ObIArray<Store_Row *> *rows)
{
  int ret = OB_SUCCESS;
  CK(part_cnt_ > 0);
  int64_t hash_expr_cnt = 1;
  uint64_t node_cnt = rows_end - rows_begin;
  uint64_t bucket_cnt = next_pow2(std::max(16L, rows->count()));
  uint64_t shift_right = __builtin_clzll(bucket_cnt) + 1;

  if (OB_SUCC(ret)) {
    if (rows_end - rows_begin <= 0) {
      // do nothing
    } else if (rows_begin < 0 || rows_end > rows->count()) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(rows_begin), K(rows_end), K(rows->count()));
    } else if (OB_FAIL(prepare_bucket_array<BucketArray>(buckets_, bucket_cnt))) {
      LOG_WARN("failed to create bucket array", K(ret));
    } else if (OB_FAIL(prepare_bucket_array<BucketNodeArray>(part_hash_nodes_, node_cnt))) {
      LOG_WARN("failed to create bucket node array", K(ret));
    } else {
      buckets_->set_all(nullptr);
      max_bucket_cnt_ = bucket_cnt;
      max_node_cnt_ = node_cnt;
    }
  }

  for (int64_t i = rows_begin; OB_SUCC(ret) && i < rows_end; ++i) {
    if (OB_ISNULL(rows->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to get rows", K(ret));
    } else {
      int64_t hash_idx = sk_collations_->at(0).field_idx_;
      const uint64_t hash_value =
        *(reinterpret_cast<const uint64_t *>(rows->at(i)->get_cell_payload(row_meta_, hash_idx)));
      uint64_t pos = hash_value >> shift_right; // high n bit
      PartHashNode &insert_node = part_hash_nodes_->at(i - rows_begin);
      PartHashNode *&bucket = buckets_->at(pos);
      insert_node.store_row_ = rows->at(i);
      PartHashNode *exist = bucket;
      bool equal = false;
      while (nullptr != exist && OB_SUCC(ret)) {
        if (OB_FAIL(is_equal_part(exist->store_row_, rows->at(i), row_meta_, equal))) {
          SQL_ENG_LOG(WARN, "failed to check equal", K(ret));
        } else if (equal) {
          break;
        } else {
          exist = exist->hash_node_next_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (nullptr == exist) { // insert at first node with hash_node_next.
        insert_node.part_row_next_ = nullptr;
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
      SQL_ENG_LOG(WARN, "failed to prepare allocate bucket nodes", K(ret));
    }
  }
  for (int64_t bucket_idx = 0; OB_SUCC(ret) && bucket_idx < bucket_cnt; ++bucket_idx) {
    int64_t bucket_part_cnt = 0;
    PartHashNode *bucket_node = buckets_->at(bucket_idx);
    if (nullptr == bucket_node) {
      continue; // no rows add here
    }
    while (OB_SUCC(ret) && nullptr != bucket_node) {
      if (OB_LIKELY(bucket_part_cnt < bucket_nodes.count())) {
        bucket_nodes.at(bucket_part_cnt) = bucket_node;
      } else {
        if (OB_FAIL(bucket_nodes.push_back(bucket_node))) {
          SQL_ENG_LOG(WARN, "failed to push back bucket node", K(ret));
        }
      }
      bucket_node = bucket_node->hash_node_next_;
      bucket_part_cnt++;
    }
    comp_.set_cmp_range(0, part_cnt_ + hash_expr_cnt);
    lib::ob_sort(&bucket_nodes.at(0), &bucket_nodes.at(0) + bucket_part_cnt, HashNodeComparer(comp_));
    comp_.set_cmp_range(part_cnt_ + hash_expr_cnt, comp_.get_cnt());
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_part_cnt; ++i) {
      int64_t rows_last = rows_idx;
      PartHashNode *part_node = bucket_nodes.at(i);
      while (nullptr != part_node) {
        rows->at(rows_idx++) = part_node->store_row_;
        part_node = part_node->part_row_next_;
      }
      if (comp_.cmp_start_ != comp_.cmp_end_) {
        if (enable_encode_sortkey_) {
          bool can_encode = true;
          ObAdaptiveQS<Store_Row> aqs(*rows, row_meta_, allocator_);
          if (OB_FAIL(aqs.init(*rows, allocator_, rows_last, rows_idx, can_encode))) {
            SQL_ENG_LOG(WARN, "failed to init aqs", K(ret));
          } else if (can_encode) {
            aqs.sort(rows_last, rows_idx);
          } else {
            enable_encode_sortkey_ = false;
            comp_.fallback_to_disable_encode_sortkey();
            lib::ob_sort(&rows->at(0) + rows_last, &rows->at(0) + rows_idx, CopyableComparer(comp_));
          }
        } else {
          lib::ob_sort(&rows->at(0) + rows_last, &rows->at(0) + rows_idx, CopyableComparer(comp_));
        }
      }
    }
    comp_.set_cmp_range(0, comp_.get_cnt());
  }
  return ret;
}

template<typename Compare, typename Store_Row, bool has_addon>
class ObPartitionTopNSortStrategy : public ObISortStrategy<Compare, Store_Row, has_addon>
{
public:
  ObPartitionTopNSortStrategy(
    ObIAllocator &allocator,
    lib::MemoryContext &mem_context,
    ObSqlWorkAreaProfile &profile,
    Compare &comp,
    ObSortKeyFetcher &sort_exprs_getter,
    ObSqlMemMgrProcessor &sql_mem_processor,
    int64_t &inmem_row_size,
    int64_t &outputted_rows_cnt)
  : topn_sort_(allocator, mem_context, profile, comp, sort_exprs_getter, sql_mem_processor, inmem_row_size, outputted_rows_cnt)
  {}
  int init(ObSortVecOpContext &ctx, ObIAllocator *page_allocator, ObIArray<ObExpr *> *all_exprs,
      const RowMeta *sk_row_meta, const RowMeta *addon_row_meta);
  int sort_inmem_data(const int64_t rows_begin, const int64_t rows_end, common::ObIArray<Store_Row *> *rows) override;
  void reset();
  void reuse();
  int add_batch(const ObBatchRows &input_brs,
      const int64_t start_pos /* 0 */,
      int64_t *append_row_count,
      bool need_load_data,
      common::ObIArray<Store_Row *> *&rows);
  int add_batch(const ObBatchRows &input_brs,
      const uint16_t selector[],
      const int64_t size,
      common::ObIArray<Store_Row *> *&rows,
      Store_Row **sk_rows);
  int do_sort();
  int next_stored_row(const Store_Row *&sk_row);
  int part_topn_next_stored_row(const Store_Row *&sk_row);
  int part_topn_node_next(int64_t &cur_topn_node_array_idx, int64_t &cur_topn_node_idx,
  const Store_Row *&store_row, const Store_Row *&addon_row);
  void reset_row_idx();
  int64_t get_ht_bucket_size() const;
  int64_t get_need_extra_mem_size() const;
private:
  ObPartitionTopNSort<Compare, Store_Row, has_addon> topn_sort_;
};

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::init(ObSortVecOpContext &ctx, ObIAllocator *page_allocator, ObIArray<ObExpr *> *all_exprs,
      const RowMeta *sk_row_meta, const RowMeta *addon_row_meta)
{
  return topn_sort_.init(ctx, page_allocator, all_exprs, sk_row_meta, addon_row_meta);
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::sort_inmem_data(const int64_t rows_begin, const int64_t rows_end, common::ObIArray<Store_Row *> *rows)
{
  UNUSEDx(rows_begin, rows_end, rows);
  return topn_sort_.do_sort();
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
      const int64_t start_pos /* 0 */,
      int64_t *append_row_count,
      bool need_load_data,
      common::ObIArray<Store_Row *> *&rows)
{
  return topn_sort_.add_batch(input_brs, start_pos, append_row_count, need_load_data, rows);
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
    const uint16_t selector[],
    const int64_t size,
    common::ObIArray<Store_Row *> *&rows,
    Store_Row **sk_rows)
{
  return topn_sort_.add_batch(input_brs, selector, size, rows, sk_rows);
}

template<typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::reset_row_idx()
{
  topn_sort_.reset_row_idx();
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::do_sort()
{
  return topn_sort_.do_sort();
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::next_stored_row(const Store_Row *&sk_row)
{
  return topn_sort_.next_stored_row(sk_row);
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::part_topn_next_stored_row(const Store_Row *&sk_row)
{
  return topn_sort_.part_topn_next_stored_row(sk_row);
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::part_topn_node_next(
    int64_t &cur_node_idx,
    int64_t &row_idx,
    const Store_Row *&sk_row,
    const Store_Row *&addon_row)
{
  return topn_sort_.part_topn_node_next(cur_node_idx, row_idx, sk_row, addon_row);
}

template<typename Compare, typename Store_Row, bool has_addon>
int64_t ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::get_ht_bucket_size() const
{
  return topn_sort_.get_ht_bucket_size();
}

template<typename Compare, typename Store_Row, bool has_addon>
int64_t ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::get_need_extra_mem_size() const
{
  return topn_sort_.get_need_extra_mem_size();
}

template<typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::reset()
{
  topn_sort_.reset();
}

template<typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSortStrategy<Compare, Store_Row, has_addon>::reuse()
{
  topn_sort_.reuse();
}

} // namespace sql
} // namespace oceanbase

#endif
