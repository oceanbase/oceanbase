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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_OP_IMPL_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{

struct ObSortOpChunk : public common::ObDLinkBase<ObSortOpChunk>
{
  explicit ObSortOpChunk(const int64_t level): level_(level), datum_store_(ObModIds::OB_SQL_SORT_ROW), row_(NULL) {}

  int64_t level_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator iter_;
  const ObChunkDatumStore::StoredRow *row_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSortOpChunk);
};

/*
 * Sort rows, do in memory sort if memory can hold all rows, otherwise do disk sort.
 * Prefix sorting is not supported it can be implemented by by simply wrapping ObSortOpImpl.
 * Rows in local/partial order (e.g.: 1,3,5,7,2,4,6,8) are sorted by in-memory merge sort first.
 * (init with true %in_local_order parameter).
 * Interface:
 * ObSortOpImpl sort_impl;
 * sort_impl.init();
 * while (OB_ITER_END != get_row(row)) {
 *   sort_impl.add(row);
 * }
 * sort_impl.sort();
 * ObIArray<ObExpr*> &exprs;
 * while (OB_ITER_END != ret) {
 *  ret = sort_impl.get_next_row(exprs);
 *  ...
 * }
 * after sort and iteration, you can add row and sort again. (not efficient)
 * NOTE: need_rewind must be true.
 * while () {
 *   sort_impl.add(row);
 * }
 * sort_impl.sort();
 * while () {
 *   sort_impl.get_next_row();
 * }
 * Abbrev:
 * imms/IMMS: in-memory merge sort
 * ems/EMS: external merge sort
 */
class ObSortOpImpl
{
public:
  static const int64_t EXTEND_MULTIPLE = 2;
  static const int64_t MAX_MERGE_WAYS = 256;
  static const int64_t INMEMORY_MERGE_SORT_WARN_WAYS = 10000;

  explicit ObSortOpImpl(ObMonitorNode &op_monitor_info);
  virtual ~ObSortOpImpl();

  // if rewind id not needed, we will release the resource after iterate end.
  int init(const uint64_t tenant_id,
      const ObIArray<ObSortFieldCollation> *sort_collations,
      const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
      ObEvalCtx *eval_ctx,
      ObExecContext *exec_ctx,
      bool enable_encode_sortkey,
      const bool in_local_order = false,
      const bool need_rewind = false,
      const int64_t part_cnt = 0,
      const int64_t topn_cnt = INT64_MAX,
      const bool is_fetch_with_ties = false,
      const int64_t default_block_size = ObChunkDatumStore::BLOCK_SIZE);

  virtual int64_t get_prefix_pos() const { return 0;  }
  // keep initialized, can sort same rows (same cell type, cell count, projector) after reuse.
  void reuse();
  // reset to state before init
  void reset();
  void destroy() { reset(); }

  // Add row and return the stored row.
  int add_row(const common::ObIArray<ObExpr*> &expr,
              const ObChunkDatumStore::StoredRow *&store_row);
  int add_row(const common::ObIArray<ObExpr*> &expr,
              const ObChunkDatumStore::StoredRow *&store_row,
              bool &sort_need_dump)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(add_row(expr, store_row))) {
      SQL_ENG_LOG(WARN, "failed to add row", K(ret));
    } else if (use_heap_sort_) {
      sort_need_dump = false;
    } else {
      sort_need_dump = need_dump();
    }
    return ret;
  }
  int add_row(const common::ObIArray<ObExpr*> &exprs)
  {
    const ObChunkDatumStore::StoredRow *store_row = NULL;
    return add_row(exprs, store_row);
  }
  int add_row(const common::ObIArray<ObExpr*> &exprs, bool &sort_need_dump)
  {
    const ObChunkDatumStore::StoredRow *store_row = NULL;
    return add_row(exprs, store_row, sort_need_dump);
  }

  // add rows by skip vector
  int add_batch(const common::ObIArray<ObExpr *> &exprs,
                const ObBitVector &skip,
                const int64_t batch_size,
                const int64_t start_pos,
                int64_t *append_row_count);
  int add_batch(const common::ObIArray<ObExpr *> &exprs,
                const ObBitVector &skip,
                const int64_t batch_size,
                const int64_t start_pos,
                bool &sort_need_dump,
                int64_t *append_row_count)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(add_batch(exprs, skip, batch_size, start_pos, append_row_count))) {
      SQL_ENG_LOG(WARN, "failed to add batch", K(ret));
    } else if (use_heap_sort_) {
      sort_need_dump = false;
    } else {
      sort_need_dump = need_dump();
    }
    return ret;
  }
  int add_batch(const common::ObIArray<ObExpr *> &exprs,
                const ObBitVector &skip,
                const int64_t batch_size,
                bool &sort_need_dump)
  {
    return add_batch(exprs, skip, batch_size, 0, sort_need_dump, nullptr);
  }
  // add batch rows by selector
  int add_batch(const common::ObIArray<ObExpr *> &exprs,
                const ObBitVector &skip,
                const int64_t batch_size,
                const uint16_t selector[],
                const int64_t size);

  int add_stored_row(const ObChunkDatumStore::StoredRow &input_row);

  int sort();

  int get_next_row(const common::ObIArray<ObExpr*> &exprs)
  {
    int ret = OB_SUCCESS;
    const ObChunkDatumStore::StoredRow *sr = NULL;
    ret = get_next_row(exprs, sr);
    return ret;
  }
  int get_next_row(const ObChunkDatumStore::StoredRow *&sr)
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited()) {
      ret = common::OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not init", K(ret));
    } else if (outputted_rows_cnt_ >= topn_cnt_ && !is_fetch_with_ties_) {
      ret = OB_ITER_END;
    } else {
      blk_holder_.release();
      ret = (this->*next_stored_row_func_)(sr);
      if (OB_UNLIKELY(common::OB_ITER_END == ret) && !need_rewind_) {
        reuse();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (NULL != last_ties_row_ && 0 != comp_.with_ties_cmp(sr, last_ties_row_)) {
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(OB_FAIL(comp_.ret_))) {
      ret = comp_.ret_;
    } else {
      outputted_rows_cnt_++;
      if (is_fetch_with_ties_ && outputted_rows_cnt_ == topn_cnt_
          && OB_FAIL(generate_last_ties_row(sr))) {
        SQL_ENG_LOG(WARN, "failed to generate last_ties_row", K(ret));
      }
    }
    return ret;
  }

  // get next batch rows, %max_cnt should equal or smaller than max batch size.
  // return OB_ITER_END for EOF
  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     const int64_t max_cnt, int64_t &read_rows);

  // rewind get_next_row() iterator to begin.
  int rewind();

  OB_INLINE int64_t get_memory_limit() { return sql_mem_processor_.get_mem_bound(); }

  bool is_inited() const { return inited_; }
  bool is_topn_sort() const { return INT64_MAX != topn_cnt_; }

  void set_input_rows(int64_t input_rows) { input_rows_ = input_rows; }
  void set_input_width(int64_t input_width) { input_width_ = input_width; }
  void set_operator_type(ObPhyOperatorType op_type) { op_type_ = op_type; }
  void set_operator_id(uint64_t op_id) { op_id_ = op_id; }
  void collect_memory_dump_info(ObMonitorNode &info)
  {
    info.otherstat_1_id_ = op_monitor_info_.otherstat_1_id_;
    info.otherstat_1_value_ = op_monitor_info_.otherstat_1_value_;
    info.otherstat_2_id_ = op_monitor_info_.otherstat_2_id_;
    info.otherstat_2_value_ = op_monitor_info_.otherstat_2_value_;
    info.otherstat_3_id_ = op_monitor_info_.otherstat_3_id_;
    info.otherstat_3_value_ = op_monitor_info_.otherstat_3_value_;
    info.otherstat_4_id_ = op_monitor_info_.otherstat_4_id_;
    info.otherstat_4_value_ = op_monitor_info_.otherstat_4_value_;
    info.otherstat_6_id_ = op_monitor_info_.otherstat_6_id_;
    info.otherstat_6_value_ = op_monitor_info_.otherstat_6_value_;
  }
  inline void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }
  void unregister_profile();
  void unregister_profile_if_necessary();

  class Compare
  {
  public:
    Compare();
    int init(const ObIArray<ObSortFieldCollation> *sort_collations,
        const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
        ObExecContext *exec_ctx,
        bool enable_encode_sortkey);

    // compare function for quick sort.
    bool operator()(const ObChunkDatumStore::StoredRow *l, const ObChunkDatumStore::StoredRow *r);

    // compare function for in-memory merge sort
    bool operator()(ObChunkDatumStore::StoredRow **l, ObChunkDatumStore::StoredRow **r);
    // compare function for external merge sort
    bool operator()(const ObSortOpChunk *l, const ObSortOpChunk *r);

    bool operator()(
        const common::ObIArray<ObExpr*> *l,
        const ObChunkDatumStore::StoredRow *r,
        ObEvalCtx &eval_ctx);

    int with_ties_cmp(const common::ObIArray<ObExpr*> *l,
                      const ObChunkDatumStore::StoredRow *r,
                      ObEvalCtx &eval_ctx);

    int with_ties_cmp(const ObChunkDatumStore::StoredRow *l,
                      const ObChunkDatumStore::StoredRow *r);

    bool is_inited() const { return NULL != sort_collations_; }
    // interface required by ObBinaryHeap
    int get_error_code() { return ret_; }

    void reset() { this->~Compare(); new (this)Compare(); }

    int fast_check_status();

    int64_t get_cnt() { return cnt_; }

    // for hash_based sort of partition by in window function
    void set_cmp_range(const int64_t cmp_start, const int64_t cmp_end)
    {
      cmp_start_ = cmp_start;
      cmp_end_ = cmp_end;
    }

  public:
    int ret_;
    const ObIArray<ObSortFieldCollation> *sort_collations_;
    const ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
    ObExecContext *exec_ctx_;
    bool enable_encode_sortkey_;
    int64_t cmp_count_;
    int64_t cmp_start_;
    int64_t cmp_end_;
  private:
    int64_t cnt_;
    DISALLOW_COPY_AND_ASSIGN(Compare);
  };

  class CopyableComparer
  {
  public:
    CopyableComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const ObChunkDatumStore::StoredRow *l, const ObChunkDatumStore::StoredRow *r)
    {
      return compare_(l, r);
    }
    Compare &compare_;
  };
  struct PartHashNode
  {
    PartHashNode(): hash_node_next_(NULL), part_row_next_(NULL), store_row_(NULL) {}
    ~PartHashNode() { hash_node_next_ = NULL; part_row_next_ = NULL; store_row_ = NULL; }
    // hash_node_next_ 为 buckets 中的某个 bucket 中的多个数据块之间的联系，
    // 多个数据块之间满足：hash_value 的高 n 位相同，但 hash_value、partition by value  不相同。
    // part_row_next_  为 buckets 中的某个 bucket 中的单个数据块内部的联系，
    // 单个数据块内部满足：hash_value 的高 n 位相同，且 hash_value、partition by value 完全相同。
    PartHashNode *hash_node_next_;
    PartHashNode *part_row_next_;
    ObChunkDatumStore::StoredRow *store_row_;
    TO_STRING_EMPTY();
  };
  
  class HashNodeComparer
  {
  public:
    HashNodeComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const PartHashNode *l, const PartHashNode *r)
    {
      return compare_(l->store_row_, r->store_row_);
    }
    Compare &compare_;
  };

  struct AQSItem {
    unsigned char *key_ptr_;
    ObChunkDatumStore::StoredRow *row_ptr_;
    uint32_t len_;
    unsigned char sub_cache_[2];
    AQSItem() : key_ptr_(NULL),
                row_ptr_(NULL),
                len_(0),
                sub_cache_()
    {
    }
    TO_STRING_KV(K_(len), K_(sub_cache), K_(key_ptr), KP(row_ptr_));
  };
  class ObAdaptiveQS {
    public:
      ObAdaptiveQS(common::ObIArray<ObChunkDatumStore::StoredRow *> &sort_rows, common::ObIAllocator &alloc);
      int init(common::ObIArray<ObChunkDatumStore::StoredRow *> &sort_rows,
                   common::ObIAllocator &alloc, int64_t rows_begin, int64_t rows_end, bool &can_encode);
      ~ObAdaptiveQS() {
        reset();
      }
      void sort(int64_t rows_begin, int64_t rows_end)
      {
        aqs_cps_qs(0, sort_rows_.count(), 0, 0, 0);
        for (int64_t i = 0; i < sort_rows_.count() && i < (rows_end - rows_begin); ++i) {
          orig_sort_rows_.at(i + rows_begin) = sort_rows_.at(i).row_ptr_;
        }
      }
      void reset()
      {
        sort_rows_.reset();
      }
      void aqs_cps_qs(int64_t l, int64_t r, int64_t common_prefix,
                        int64_t depth_limit, int64_t cache_offset);
      void aqs_radix(int64_t l, int64_t r, int64_t common_prefix,
                        int64_t offset, int64_t depth_limit);
      void inplace_radixsort_more_bucket(int64_t l, int64_t r,
                                          int64_t div_val,
                                          int64_t common_prefix,
                                          int64_t depth_limit,
                                          int64_t cache_offset,
                                          bool update);
      void insertion_sort(int64_t l, int64_t r,
                            int64_t common_prefix,
                            int64_t cache_offset);
      inline void swap(int64_t l, int64_t r) {
        std::swap(sort_rows_[r], sort_rows_[l]);
      }
      inline int compare_vals(int64_t l, int64_t r, int64_t &differ_at,
                              int64_t common_prefix, int64_t cache_offset);
      inline int compare_cache(AQSItem &l, AQSItem &r, int64_t &differ_at,
                                int64_t common_prefix, int64_t cache_offset);
    public:
      unsigned char masks[8]{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
      common::ObIArray<ObChunkDatumStore::StoredRow *> &orig_sort_rows_;
      common::ObFixedArray<AQSItem, common::ObIAllocator> sort_rows_;
      common::ObIAllocator &alloc_;
  };

protected:
  class MemEntifyFreeGuard
  {
  public:
    explicit MemEntifyFreeGuard(lib::MemoryContext &entify) : entify_(entify) {}
    ~MemEntifyFreeGuard()
    {
      if (NULL != entify_) {
        DESTROY_CONTEXT(entify_);
        entify_ = NULL;
      }
    }
    lib::MemoryContext &entify_;
  };
  //Optimize mem usage/performance of top-n sort:
  //
  //Record buf_len of each allocated row. When old row pop-ed out of the heap
  //and has enough space for new row, use the space of old row to store new row
  //instead of allocating space for new row.
  //Note that this is not perfect solution, it cannot handle the case that row size
  //keeps going up. However, this can cover most cases.
  struct SortStoredRow : public ObChunkDatumStore::StoredRow
  {
    struct ExtraInfo
    {
      uint64_t max_size_;
    };
    ExtraInfo &get_extra_info()
    {
      static_assert(sizeof(SortStoredRow) == sizeof(sql::ObChunkDatumStore::StoredRow),
          "sizeof StoredJoinRow must be the save with StoredRow");
      return *reinterpret_cast<ExtraInfo *>(get_extra_payload());
    }
    const ExtraInfo &get_extra_info() const
    { return *reinterpret_cast<const ExtraInfo *>(get_extra_payload()); }

    inline uint64_t get_max_size() const
    { return get_extra_info().max_size_; }
    inline void set_max_size(const uint64_t max_size)
    { get_extra_info().max_size_ = max_size; }
  };

  int get_next_row(const common::ObIArray<ObExpr*> &exprs, const ObChunkDatumStore::StoredRow *&sr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(get_next_row(sr))) {
      if (common::OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "fail to get_next_row", K(ret));
      }
    } else if (OB_ISNULL(sr)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: store row is null", K(ret));
    } else if (OB_FAIL(sr->to_expr(exprs, *eval_ctx_))) {
      SQL_ENG_LOG(WARN, "convert store row to expr value failed", K(ret));
    }
    return ret;
  }

  // fetch next batch stored rows to stored_rows_
  int get_next_batch_stored_rows(int64_t max_cnt, int64_t &read_rows);

  typedef int (ObSortOpImpl::*NextStoredRowFunc)(const ObChunkDatumStore::StoredRow *&sr);

  bool need_imms() const
  {
    return !use_heap_sort_ && rows_->count() > datum_store_.get_row_cnt();
  }
  int sort_inmem_data();
  int do_dump();

  template <typename Input>
    int build_chunk(const int64_t level, Input &input, int64_t extra_size = 0);

  int build_ems_heap(int64_t &merge_ways);
  template <typename Heap, typename NextFunc, typename Item>
  int heap_next(Heap &heap, const NextFunc &func, Item &item);
  int ems_heap_next(ObSortOpChunk *&chunk);
  int imms_heap_next(const ObChunkDatumStore::StoredRow *&store_row);

  int array_next_stored_row(
      const ObChunkDatumStore::StoredRow *&sr);
  int imms_heap_next_stored_row(
      const ObChunkDatumStore::StoredRow *&sr);
  int ems_heap_next_stored_row(
      const ObChunkDatumStore::StoredRow *&sr);

  // 这里need dump外加两个条件: 1) data_size > expect_size 2) mem_used > global_bound
  // 为什么如此，原因在于expect size可能是one pass size，所以数据大于expect size，
  // 而总内存不能超过global bound size，否则总体内存会超限
  // 基于此，看后面是否统一考虑采用这种方案，也就是分两部分：data size和total mem used size来判断是否dump
  bool need_dump()
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound()
        || mem_context_->used() >= profile_.get_max_bound();
  }
  int preprocess_dump(bool &dumped);
  // before add row process: update date used memory, try dump ...
  int before_add_row();
  // after add row process: check local merge sort order and add %sr to %rows_
  int after_add_row(ObChunkDatumStore::StoredRow *sr);
  int add_quick_sort_row(const common::ObIArray<ObExpr*> &exprs,
                         const ObChunkDatumStore::StoredRow *&store_row);
  int add_quick_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                           const ObBitVector &skip,
                           const int64_t batch_size,
                           const int64_t start_pos /* 0 */,
                           int64_t *append_row_count = nullptr);
  int add_quick_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                           const ObBitVector &skip,
                           const int64_t batch_size,
                           const uint16_t selector[],
                           const int64_t size);
  // partition sort for window function
  int is_equal_part(const ObChunkDatumStore::StoredRow *l, const ObChunkDatumStore::StoredRow *r, bool &is_equal);
  int do_partition_sort(common::ObIArray<ObChunkDatumStore::StoredRow *> &rows,
                        const int64_t rows_begin, const int64_t rows_end);
  void set_blk_holder(ObChunkDatumStore::IteratedBlockHolder *blk_holder);
  // for topn sort
  int add_heap_sort_row(const common::ObIArray<ObExpr*> &exprs,
                        const ObChunkDatumStore::StoredRow *&store_row);
  int add_heap_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                          const ObBitVector &skip,
                          const int64_t batch_size,
                          const int64_t start_pos /* 0 */,
                          int64_t *append_row_count = nullptr);
  int add_heap_sort_batch(const common::ObIArray<ObExpr *> &exprs,
                          const ObBitVector &skip,
                          const int64_t batch_size,
                          const uint16_t selector[],
                          const int64_t size);
  int adjust_topn_heap(const common::ObIArray<ObExpr*> &exprs,
                       const ObChunkDatumStore::StoredRow *&store_row);
  int adjust_topn_heap_with_ties(const common::ObIArray<ObExpr*> &exprs,
                                 const ObChunkDatumStore::StoredRow *&store_row);
  //
  int copy_to_topn_row(const common::ObIArray<ObExpr*> &exprs,
                       ObIAllocator &alloc,
                       SortStoredRow *&top_row);
  // row is in parameter and out parameter.
  // if row is null will alloc new memory, otherwise reuse in place if memory is enough.
  // ensure row pointer not change when an error occurs.
  int copy_to_row(const common::ObIArray<ObExpr*> &exprs,
                  ObIAllocator &alloc,
                  SortStoredRow *&row);
  int generate_new_row(SortStoredRow *orign_row,
                       ObIAllocator &alloc,
                       SortStoredRow *&new_row);
  int generate_last_ties_row(const ObChunkDatumStore::StoredRow *orign_row);
  int adjust_topn_read_rows(ObChunkDatumStore::StoredRow **stored_rows, int64_t &read_cnt);

  DISALLOW_COPY_AND_ASSIGN(ObSortOpImpl);

protected:
  typedef common::ObBinaryHeap<ObChunkDatumStore::StoredRow **, Compare, 16> IMMSHeap;
  typedef common::ObBinaryHeap<ObSortOpChunk *, Compare, MAX_MERGE_WAYS> EMSHeap;
  typedef common::ObBinaryHeap<ObChunkDatumStore::StoredRow *, Compare> TopnHeap;
  static const int64_t MAX_ROW_CNT = 268435456; // (2G / 8)
  static const int64_t STORE_ROW_HEADER_SIZE = sizeof(SortStoredRow);
  static const int64_t STORE_ROW_EXTRA_SIZE = sizeof(uint64_t);
  bool inited_;
  bool local_merge_sort_;
  bool need_rewind_;
  bool got_first_row_;
  bool sorted_;
  bool enable_encode_sortkey_;
  lib::MemoryContext mem_context_;
  MemEntifyFreeGuard mem_entify_guard_;
  int64_t tenant_id_;
  const ObIArray<ObSortFieldCollation> *sort_collations_;
  const ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
  ObEvalCtx *eval_ctx_;
  Compare comp_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator iter_;
  int64_t inmem_row_size_;
  int64_t mem_check_interval_mask_;
  int64_t row_idx_; // for iterate rows_
  common::ObArray<ObChunkDatumStore::StoredRow *> quick_sort_array_;
  common::ObDList<ObSortOpChunk> sort_chunks_;
  bool heap_iter_begin_;
  // heap for in-memory merge sort local order rows
  IMMSHeap *imms_heap_;
  // heap for external merge sort
  EMSHeap *ems_heap_;
  NextStoredRowFunc next_stored_row_func_;
  int64_t input_rows_;
  int64_t input_width_;
  ObSqlWorkAreaProfile profile_;
  ObMonitorNode &op_monitor_info_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
  ObExecContext *exec_ctx_;
  ObChunkDatumStore::StoredRow **stored_rows_;
  ObIOEventObserver *io_event_observer_;
  // for window function partition sort
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
  common::ObArray<SortStoredRow *> ties_array_;
  ObChunkDatumStore::StoredRow *last_ties_row_;
  common::ObIArray<ObChunkDatumStore::StoredRow *> *rows_;
  ObChunkDatumStore::IteratedBlockHolder blk_holder_;
};

class ObInMemoryTopnSortImpl;
class ObPrefixSortImpl : public ObSortOpImpl
{
public:
  explicit ObPrefixSortImpl(ObMonitorNode &op_monitor_info);

  // init && start fetch %op rows
  int init(const int64_t tenant_id,
      const int64_t prefix_pos,
      const common::ObIArray<ObExpr *> &all_exprs,
      const ObIArray<ObSortFieldCollation> *sort_collations,
      const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
      ObEvalCtx *eval_ctx,
      ObOperator *child,
      ObOperator *self_op,
      ObExecContext &exec_ctx,
      bool enable_encode_sortkey,
      int64_t &sort_row_cnt,
      int64_t topn_cnt = INT64_MAX,
      bool is_fetch_with_ties = false);

  int64_t get_prefix_pos() const { return prefix_pos_;  }
  int get_next_row(const common::ObIArray<ObExpr*> &exprs);

  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     const int64_t max_cnt, int64_t &read_rows);

  void reuse();
  void reset();
private:
  // fetch rows in same prefix && do sort, set %next_prefix_row_ to NULL
  // when all child rows are fetched.
  int fetch_rows(const common::ObIArray<ObExpr *> &all_exprs);
  int fetch_rows_batch(const common::ObIArray<ObExpr *> &all_exprs);

  int is_same_prefix(const ObChunkDatumStore::StoredRow *store_row,
                      const common::ObIArray<ObExpr *> &all_exprs,
                      const int64_t datum_idx,
                      bool &same);

  int is_same_prefix(const common::ObIArray<ObExpr *> &all_exprs,
                      const int64_t datum_idx1, const int64_t datum_idx2,
                      bool &same);

  int add_immediate_prefix(const common::ObIArray<ObExpr *> &all_exprs);

  // disable call ObSortOpImpl interface directly.
  using ObSortOpImpl::init;
  using ObSortOpImpl::add_row;
  using ObSortOpImpl::add_batch;
  using ObSortOpImpl::get_next_row;
  using ObSortOpImpl::get_next_batch;

private:
  int64_t prefix_pos_;
  const ObIArray<ObSortFieldCollation> *full_sort_collations_;
  const ObIArray<ObSortCmpFunc> *full_sort_cmp_funs_;
  common::ObArrayHelper<ObSortFieldCollation> base_sort_collations_;
  common::ObArrayHelper<ObSortCmpFunc> base_sort_cmp_funs_;

  const ObChunkDatumStore::StoredRow *prev_row_;
  // when got new prefix, save the row to to %next_prefix_row_
  ObChunkDatumStore::ShadowStoredRow next_prefix_row_store_;
  ObChunkDatumStore::StoredRow *next_prefix_row_;

  ObOperator *child_;
  ObOperator *self_op_;
  int64_t *sort_row_count_;

  // members for batch interface
  uint16_t *selector_;
  uint16_t selector_size_;

  int64_t sort_prefix_rows_;

  ObChunkDatumStore immediate_prefix_store_;
  ObChunkDatumStore::StoredRow **immediate_prefix_rows_;
  int64_t immediate_prefix_pos_;

  const ObBatchRows *brs_;
  ObBatchResultHolder brs_holder_;
};

class ObUniqueSortImpl : public ObSortOpImpl
{
public:
  explicit ObUniqueSortImpl(ObMonitorNode &op_monitor_info) : ObSortOpImpl(op_monitor_info), prev_row_(NULL), prev_buf_size_(0)
  {
  }

  virtual ~ObUniqueSortImpl()
  {
    free_prev_row();
  }

  int init(const uint64_t tenant_id,
      const ObIArray<ObSortFieldCollation> *sort_collations,
      const ObIArray<ObSortCmpFunc> *sort_cmp_funs,
      ObEvalCtx *eval_ctx,
      ObExecContext *exec_ctx,
      const bool need_rewind,
      const int64_t default_block_size = ObChunkDatumStore::BLOCK_SIZE)
  {
    return ObSortOpImpl::init(tenant_id,
        sort_collations,
        sort_cmp_funs,
        eval_ctx,
        exec_ctx,
        false,
        false /* local order */,
        need_rewind,
        0, /* part_cnt */
        INT64_MAX, /* topn_cnt */
        false, /* is_fetch_with_ties */
        default_block_size);
  }

  int get_next_row(const common::ObIArray<ObExpr*> &exprs);
  int get_next_stored_row(const ObChunkDatumStore::StoredRow *&sr);

  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     const int64_t max_cnt, int64_t &read_rows);

  void reuse();
  void reset();

  int sort()
  {
    free_prev_row();
    return ObSortOpImpl::sort();
  }

  int rewind()
  {
    free_prev_row();
    return ObSortOpImpl::rewind();
  }
private:
  int save_prev_row(const ObChunkDatumStore::StoredRow &sr);
  void free_prev_row();

private:
  ObChunkDatumStore::StoredRow *prev_row_;
  int64_t prev_buf_size_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_OP_IMPL_H_ */
