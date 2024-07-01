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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_IMPL_H_

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
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/sort/ob_pd_topn_sort_filter.h"

namespace oceanbase {
namespace sql {
#define SK_RC_DOWNCAST_P(row) const_cast<Store_Row *>(row)
#define SK_DOWNCAST_P(store_row) static_cast<Store_Row *>(store_row)
#define SK_CONST_UPCAST_P(store_row) reinterpret_cast<const ObCompactRow *>(store_row)
#define SK_UPCAST_PP(store_row) reinterpret_cast<ObCompactRow **>(store_row)
#define SK_DOWNCAST_PP(store_row) static_cast<Store_Row **>(store_row)
#define SK_UPCAST_CONST_PP(store_row)                                                              \
  const_cast<const ObCompactRow **>(reinterpret_cast<ObCompactRow **>(store_row))

// Vectorization 2.0 sort implementation class
template <typename Compare, typename Store_Row, bool has_addon>
class ObSortVecOpImpl : public ObISortVecOpImpl
{
  using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;

public:
  explicit ObSortVecOpImpl(ObMonitorNode &op_monitor_info, lib::MemoryContext &mem_context) :
    ObISortVecOpImpl(op_monitor_info, mem_context), flag_(0), tenant_id_(OB_INVALID_ID),
    allocator_(mem_context->get_malloc_allocator()), sk_collations_(nullptr),
    addon_collations_(nullptr), cmp_sort_collations_(nullptr), sk_row_meta_(nullptr),
    addon_row_meta_(nullptr), sk_exprs_(nullptr), addon_exprs_(nullptr), cmp_sk_exprs_(nullptr),
    all_exprs_(allocator_), sk_vec_ptrs_(allocator_), addon_vec_ptrs_(allocator_),
    eval_ctx_(nullptr), comp_(allocator_), sk_store_(&allocator_), addon_store_(&allocator_),
    inmem_row_size_(0), mem_check_interval_mask_(1), row_idx_(0), quick_sort_array_(),
    heap_iter_begin_(false), imms_heap_(nullptr), ems_heap_(nullptr),
    next_stored_row_func_(&ObSortVecOpImpl::array_next_stored_row), exec_ctx_(nullptr),
    sk_rows_(nullptr), addon_rows_(nullptr), buckets_(nullptr), max_bucket_cnt_(0),
    part_hash_nodes_(nullptr), max_node_cnt_(0), part_cnt_(0), topn_cnt_(INT64_MAX),
    outputted_rows_cnt_(0), use_heap_sort_(false), is_fetch_with_ties_(false), topn_heap_(nullptr),
    ties_array_pos_(0), ties_array_(), sorted_dumped_rows_ptrs_(), last_ties_row_(nullptr), rows_(nullptr),
    sort_exprs_getter_(allocator_),
    store_row_factory_(allocator_, sql_mem_processor_, sk_row_meta_, addon_row_meta_, inmem_row_size_, topn_cnt_),
    topn_filter_(nullptr), is_topn_filter_enabled_(false), compress_type_(NONE_COMPRESSOR)
  {}
  virtual ~ObSortVecOpImpl()
  {
    reset();
  }
  virtual void reset() override;
  virtual int init(ObSortVecOpContext &context) override;
  virtual int add_batch(const ObBatchRows &input_brs, bool &sort_need_dump) override;
  // get next batch rows, %max_cnt should equal or smaller than max batch size.
  // return OB_ITER_END for EOF
  virtual int get_next_batch(const int64_t max_cnt, int64_t &read_rows) override;
  virtual int sort() override;
  virtual int add_batch_stored_row(int64_t &row_size, const ObCompactRow **sk_stored_rows,
                                   const ObCompactRow **addon_stored_rows) override;
  virtual int64_t get_extra_size(bool is_sort_key) override
  {
    return Store_Row::get_extra_size(is_sort_key);
  }
  // keep initialized, can sort same rows (same cell type, cell count,
  // projector) after reuse.
  void reuse();
  // reset to state before init
  void destroy()
  {
    reset();
  }
  int init_pd_topn_filter_msg(ObSortVecOpContext &ctx);
  int init_vec_ptrs(const common::ObIArray<ObExpr *> &exprs,
                    common::ObFixedArray<ObIVector *, common::ObIAllocator> &vec_ptrs,
                    ObEvalCtx *eval_ctx);
  int merge_sk_addon_exprs(const common::ObIArray<ObExpr *> *sk_exprs,
                           const common::ObIArray<ObExpr *> *addon_exprs);

  int init_temp_row_store(const common::ObIArray<ObExpr *> &exprs, const int64_t mem_limit,
                          const int64_t batch_size, const bool need_callback,
                          const bool enable_dump, const int64_t extra_size, ObCompressorType compress_type,
                          ObTempRowStore &row_store);
  int init_sort_temp_row_store(const int64_t batch_size);
  int init_store_row_factory();
  int init_eager_topn_filter(const common::ObIArray<Store_Row *> *dumped_rows, const int64_t max_batch_size);
  int add_batch(const ObBatchRows &input_brs, const int64_t start_pos, int64_t *append_row_count);
  int add_batch(const ObBatchRows &input_brs, const int64_t start_pos, bool &sort_need_dump,
                int64_t *append_row_count)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(add_batch(input_brs, start_pos, append_row_count))) {
      SQL_ENG_LOG(WARN, "failed to add batch", K(ret));
    } else if (use_heap_sort_) {
      sort_need_dump = false;
    } else {
      sort_need_dump = need_dump();
    }
    return ret;
  }
  int add_batch(const ObBatchRows &input_brs, const uint16_t selector[], const int64_t size);
  int add_stored_row(const ObCompactRow *store_row);
  int add_stored_row(const ObCompactRow *sk_row, const ObCompactRow *addon_row);
  // rewind get_next_row() iterator to begin.
  int rewind();
  OB_INLINE int64_t get_memory_limit()
  {
    return sql_mem_processor_.get_mem_bound();
  }
  OB_INLINE int64_t get_memory_used()
  {
    return mem_context_->used();
  }
  bool is_inited() const
  {
    return inited_;
  }
  bool is_topn_sort() const
  {
    return INT64_MAX != topn_cnt_;
  }

  OB_INLINE bool is_topn_filter_enabled()
  {
    return is_topn_filter_enabled_;
  }

  class CopyableComparer
  {
  public:
    CopyableComparer(Compare &compare) : compare_(compare)
    {}
    bool operator()(const Store_Row *l, const Store_Row *r)
    {
      return compare_(l, r);
    }
    Compare &compare_;
  };

  struct PartHashNode
  {
    PartHashNode() : hash_node_next_(nullptr), part_row_next_(nullptr), store_row_(nullptr)
    {}
    ~PartHashNode()
    {
      hash_node_next_ = nullptr;
      part_row_next_ = nullptr;
      store_row_ = nullptr;
    }
    PartHashNode *hash_node_next_;
    PartHashNode *part_row_next_;
    Store_Row *store_row_;
    TO_STRING_EMPTY();
  };

  class HashNodeComparer
  {
  public:
    HashNodeComparer(Compare &compare) : compare_(compare)
    {}
    bool operator()(const PartHashNode *l, const PartHashNode *r)
    {
      return compare_(l->store_row_, r->store_row_);
    }
    Compare &compare_;
  };

protected:
  typedef int (ObSortVecOpImpl::*NextStoredRowFunc)(const Store_Row *&sr);
  int sort_inmem_data();
  int do_dump();
  int build_ems_heap(int64_t &merge_ways);
  template <typename Heap, typename NextFunc, typename Item>
  int heap_next(Heap &heap, const NextFunc &func, Item &item);
  int ems_heap_next(SortVecOpChunk *&chunk);
  int imms_heap_next(const Store_Row *&sk_row);
  int array_next_stored_row(const Store_Row *&sk_row);
  int imms_heap_next_stored_row(const Store_Row *&sr);
  int ems_heap_next_stored_row(const Store_Row *&sr);
  int build_row(const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta,
                const int64_t row_size, ObEvalCtx &ctx, ObCompactRow *&stored_row);
  bool need_dump()
  {
    return sql_mem_processor_.get_data_size() > get_tmp_buffer_mem_bound()
           || mem_context_->used() >= profile_.get_max_bound();
  }
  inline int64_t get_tmp_buffer_mem_bound() {
    // The memory reserved for ObSortVecOpEagerFilter should be deducted when topn filter is enabled.
    return (is_topn_filter_enabled() && is_topn_sort())
               ? sql_mem_processor_.get_mem_bound() *
                     (1.0 - ObSortVecOpEagerFilter<Compare, Store_Row,
                                            has_addon>::FILTER_RATIO)
               : sql_mem_processor_.get_mem_bound();
  }
  int preprocess_dump(bool &dumped);
  // before add row process: update date used memory, try dump ...
  int before_add_row();
  // after add row process: check local merge sort order and add %sr to %rows_
  int after_add_row(Store_Row *sr);
  int add_quick_sort_batch(const ObBatchRows &input_brs, const int64_t start_pos /* 0 */,
                           int64_t *append_row_count = nullptr);
  int add_quick_sort_batch(const uint16_t selector[], const int64_t size);
  // partition sort for window function
  int is_equal_part(const Store_Row *l, const Store_Row *r, const RowMeta &row_meta,
                    bool &is_equal);
  int do_partition_sort(const RowMeta &row_meta, common::ObIArray<Store_Row *> &rows,
                        const int64_t rows_begin, const int64_t rows_end);
  void set_blk_holder(ObTempRowStore::BlockHolder *blk_holder);
  // for topn sort
  int add_heap_sort_row(const Store_Row *&store_row);
  // load data to comp_
  int load_data_to_comp(const ObBatchRows &input_brs);
  int add_heap_sort_batch(const ObBatchRows &input_brs, const int64_t start_pos /* 0 */,
                          int64_t *append_row_count = nullptr,
                          bool need_load_data = true);
  int add_heap_sort_batch(const ObBatchRows &input_brs, const uint16_t selector[],
                          const int64_t size);
  int adjust_topn_heap(const Store_Row *&store_row);
  int adjust_topn_heap_with_ties(const Store_Row *&store_row);
  int copy_to_topn_row(Store_Row *&new_row);
  // row is in parameter and out parameter.
  // if row is null will alloc new memory, otherwise reuse in place if memory is
  // enough. ensure row pointer not change when an error occurs.
  int copy_to_row(const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta,
                  Store_Row *&row);
  int copy_to_row(Store_Row *&sk_row);
  int generate_last_ties_row(const Store_Row *orign_row);
  int adjust_topn_read_rows(Store_Row **stored_rows, int64_t &read_cnt);
  int batch_eval_vector(const common::ObIArray<ObExpr *> &exprs, const ObBatchRows &input_brs);
  // fetch next batch stored rows to stored_rows_
  int get_next_batch_stored_rows(int64_t max_cnt, int64_t &read_rows);
  int attach_rows(const ObExprPtrIArray &exprs, ObEvalCtx &ctx, const RowMeta &row_meta,
                  const ObCompactRow **srows, const int64_t read_rows);
  int attach_rows(const int64_t read_rows);
  bool need_imms() const
  {
    return !use_heap_sort_ && rows_->count() > sk_store_.get_row_cnt();
  }
  OB_INLINE bool is_separate_encode_sk() const
  {
    return 1 == sk_exprs_->count() && T_FUN_SYS_ENCODE_SORTKEY == sk_exprs_->at(0)->type_;
  }
  int add_sort_batch_row(const ObBatchRows &input_brs, const int64_t start_pos,
                         int64_t &append_row_count);
  int add_sort_batch_row(const uint16_t selector[], const int64_t row_size);
  int update_max_available_mem_size_periodically();
  int eager_topn_filter(common::ObIArray<Store_Row *> *sorted_dumped_rows);
  int eager_topn_filter_update(const common::ObIArray<Store_Row *> *sorted_dumped_rows);
  template <typename Input>
  int build_chunk(const int64_t level, Input &input)
  {
    int ret = OB_SUCCESS;
    const int64_t curr_time = ObTimeUtility::fast_current_time();
    int64_t stored_row_cnt = 0;
    const Store_Row *sort_key_row = nullptr;
    const Store_Row *addon_field_row = nullptr;
    ObCompactRow *dst_sk_row = nullptr;
    ObCompactRow *dst_addon_row = nullptr;
    SortVecOpChunk *chunk = nullptr;
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not init", K(ret));
    } else if (OB_ISNULL(
                 chunk = OB_NEWx(SortVecOpChunk, (&mem_context_->get_malloc_allocator()), level))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else if (OB_FAIL(init_temp_row_store(*sk_exprs_, 1, eval_ctx_->max_batch_size_, true,
                                           true /*enable dump*/, Store_Row::get_extra_size(true), compress_type_,
                                           chunk->sk_store_))) {
      SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
    } else if (has_addon
               && OB_FAIL(init_temp_row_store(
                    *addon_exprs_, 1, eval_ctx_->max_batch_size_, true, true /*enable dump*/,
                    Store_Row::get_extra_size(false), compress_type_, chunk->addon_store_))) {
      SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (!is_fetch_with_ties_ && stored_row_cnt >= topn_cnt_) {
          break;
        } else if (OB_FAIL(input(sort_key_row, addon_field_row))) {
          if (OB_ITER_END != ret) {
            SQL_ENG_LOG(WARN, "get input row failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
          break;
        } else if (OB_FAIL(chunk->sk_store_.add_row(SK_CONST_UPCAST_P(sort_key_row), dst_sk_row))) {
          SQL_ENG_LOG(WARN, "copy row to row store failed");
        } else if (has_addon
                   && OB_FAIL(chunk->addon_store_.add_row(SK_CONST_UPCAST_P(addon_field_row),
                                                          dst_addon_row))) {
          SQL_ENG_LOG(WARN, "copy row to row store failed");
        } else {
          stored_row_cnt++;
          op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
          op_monitor_info_.otherstat_1_value_ += 1;
        }
      }

      // 必须强制先dump，然后finish dump才有效
      if (OB_FAIL(ret)) {
      } else if (has_addon
                 && (chunk->sk_store_.get_row_cnt() != chunk->addon_store_.get_row_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "the number of rows in sort key store and addon store is expected to be equal",
          K(chunk->sk_store_.get_row_cnt()), K(chunk->addon_store_.get_row_cnt()), K(ret));
      } else if (OB_FAIL(chunk->sk_store_.dump(false, true))) {
        SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
      } else if (OB_FAIL(chunk->sk_store_.finish_add_row(true /*+ need dump */))) {
        SQL_ENG_LOG(WARN, "finish add row failed", K(ret));
      } else if (has_addon && OB_FAIL(chunk->addon_store_.dump(false, true))) {
        SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
      } else if (has_addon && OB_FAIL(chunk->addon_store_.finish_add_row(true /*+ need dump */))) {
        SQL_ENG_LOG(WARN, "finish add row failed", K(ret));
      } else {
        const int64_t sort_io_time = ObTimeUtility::fast_current_time() - curr_time;
        const int64_t file_size =
          has_addon ? (chunk->sk_store_.get_file_size() + chunk->addon_store_.get_file_size()) :
                      chunk->sk_store_.get_file_size();
        const int64_t mem_hold =
          has_addon ? (chunk->sk_store_.get_mem_hold() + chunk->addon_store_.get_mem_hold()) :
                      chunk->sk_store_.get_mem_hold();
        op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
        op_monitor_info_.otherstat_4_value_ += sort_io_time;
        LOG_TRACE("dump sort file", "level", level, "rows", chunk->sk_store_.get_row_cnt(),
                  "file_size", file_size, "memory_hold", mem_hold, "mem_used",
                  mem_context_->used());
      }
    }

    if (OB_SUCC(ret)) {
      // In increase sort, chunk->level_ may less than the last of sort chunks.
      // insert the chunk to the upper bound the level.
      SortVecOpChunk *pos = sort_chunks_.get_last();
      for (; pos != sort_chunks_.get_header() && pos->level_ > level; pos = pos->get_prev()) {}
      pos = pos->get_next();
      if (!sort_chunks_.add_before(pos, chunk)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "add link node to list failed", K(ret));
      }
    }
    if (OB_SUCCESS != ret && nullptr != chunk) {
      chunk->~SortVecOpChunk();
      mem_context_->get_malloc_allocator().free(chunk);
      chunk = nullptr;
    }

    return ret;
  }

  DISALLOW_COPY_AND_ASSIGN(ObSortVecOpImpl);

protected:
  static const int64_t MAX_ROW_CNT = 268435456; // (2G / 8)
  static const int64_t EXTEND_MULTIPLE = 2;
  static const int64_t MAX_MERGE_WAYS = 256;
  static const int64_t INMEMORY_MERGE_SORT_WARN_WAYS = 10000;
  typedef common::ObBinaryHeap<Store_Row **, Compare, 16> IMMSHeap;
  typedef common::ObBinaryHeap<SortVecOpChunk *, Compare, MAX_MERGE_WAYS> EMSHeap;
  typedef common::ObBinaryHeap<Store_Row *, Compare> TopnHeap;

  union
  {
    struct
    {
      uint32_t inited_ : 1;
      uint32_t no_addon_field_ : 1;
      uint32_t local_merge_sort_ : 1;
      uint32_t need_rewind_ : 1;
      uint32_t got_first_row_ : 1;
      uint32_t sorted_ : 1;
      uint32_t enable_encode_sortkey_ : 1;
      uint32_t reserved_ : 25;
    };
    uint32_t flag_;
  };
  int64_t tenant_id_;
  ObIAllocator &allocator_;
  const ObIArray<ObSortFieldCollation> *sk_collations_;
  const ObIArray<ObSortFieldCollation> *addon_collations_;
  const ObIArray<ObSortFieldCollation> *cmp_sort_collations_;
  const RowMeta *sk_row_meta_;
  const RowMeta *addon_row_meta_;
  const common::ObIArray<ObExpr *> *sk_exprs_;
  const common::ObIArray<ObExpr *> *addon_exprs_;
  const common::ObIArray<ObExpr *> *cmp_sk_exprs_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> all_exprs_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> sk_vec_ptrs_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> addon_vec_ptrs_;
  ObEvalCtx *eval_ctx_;
  Compare comp_;
  ObTempRowStore sk_store_;
  ObTempRowStore addon_store_;
  int64_t inmem_row_size_;
  int64_t mem_check_interval_mask_;
  int64_t row_idx_; // for iterate rows_
  common::ObArray<Store_Row *> quick_sort_array_;
  common::ObDList<SortVecOpChunk> sort_chunks_;
  bool heap_iter_begin_;
  // heap for in-memory merge sort local order rows
  IMMSHeap *imms_heap_;
  // heap for external merge sort
  EMSHeap *ems_heap_;
  NextStoredRowFunc next_stored_row_func_;
  ObExecContext *exec_ctx_;
  Store_Row **sk_rows_;
  Store_Row **addon_rows_;
  PartHashNode **buckets_;
  uint64_t max_bucket_cnt_;
  PartHashNode *part_hash_nodes_;
  uint64_t max_node_cnt_;
  int64_t part_cnt_;
  // for limit topn sort change to simple sort
  int64_t topn_cnt_;
  int64_t outputted_rows_cnt_;
  // for topn sort
  bool use_heap_sort_;
  bool is_fetch_with_ties_;
  TopnHeap *topn_heap_;
  int64_t ties_array_pos_;
  common::ObArray<Store_Row *> ties_array_;
  common::ObArray<Store_Row *> sorted_dumped_rows_ptrs_;
  Store_Row *last_ties_row_;
  common::ObIArray<Store_Row *> *rows_;
  ObTempRowStore::BlockHolder blk_holder_;
  ObSortKeyFetcher sort_exprs_getter_;
  ObSortVecOpStoreRowFactory<Store_Row, has_addon> store_row_factory_;
  ObSortVecOpEagerFilter<Compare, Store_Row, has_addon> *topn_filter_;
  bool is_topn_filter_enabled_;
  ObCompressorType compress_type_;
  ObPushDownTopNFilter pd_topn_filter_;
};

} // end namespace sql
} // end namespace oceanbase

#include "ob_sort_vec_op_impl.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_IMPL_H_ */
