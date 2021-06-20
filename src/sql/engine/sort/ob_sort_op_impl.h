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

namespace oceanbase {
namespace sql {

struct ObSortOpChunk : public common::ObDLinkBase<ObSortOpChunk> {
  explicit ObSortOpChunk(const int64_t level) : level_(level), row_(NULL)
  {}

  int64_t level_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator iter_;
  const ObChunkDatumStore::StoredRow* row_;

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
class ObSortOpImpl {
public:
  static const int64_t EXTEND_MULTIPLE = 2;
  static const int64_t MAX_MERGE_WAYS = 256;
  static const int64_t INMEMORY_MERGE_SORT_WARN_WAYS = 10000;

  ObSortOpImpl();
  virtual ~ObSortOpImpl();

  // if rewind id not needed, we will release the resource after iterate end.
  int init(const uint64_t tenant_id, const ObIArray<ObSortFieldCollation>* sort_collations,
      const ObIArray<ObSortCmpFunc>* sort_cmp_funs, ObEvalCtx* eval_ctx, const bool in_local_order = false,
      const bool need_rewind = false);

  // keep initialized, can sort same rows (same cell type, cell count, projector) after reuse.
  void reuse();
  // reset to state before init
  void reset();
  void destroy()
  {
    reset();
  }

  // Add row and return the stored row.
  template <typename T>
  int add_row(const T& exprs, const ObChunkDatumStore::StoredRow*& store_row);

  int add_row(const common::ObIArray<ObExpr*>& exprs)
  {
    const ObChunkDatumStore::StoredRow* store_row = NULL;
    return add_row(exprs, store_row);
  }
  int add_stored_row(const ObChunkDatumStore::StoredRow& input_row);

  int sort();
  int get_next_row(const common::ObIArray<ObExpr*>& exprs)
  {
    const ObChunkDatumStore::StoredRow* sr = NULL;
    return get_next_row(exprs, sr);
  }
  int get_next_row(const ObChunkDatumStore::StoredRow*& sr)
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited()) {
      ret = common::OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not init", K(ret));
    } else {
      ret = (this->*next_stored_row_func_)(sr);
      if (OB_UNLIKELY(common::OB_ITER_END == ret) && !need_rewind_) {
        reuse();
      }
    }
    return ret;
  }

  // rewind get_next_row() iterator to begin.
  int rewind();

  OB_INLINE int64_t get_memory_limit()
  {
    return sql_mem_processor_.get_mem_bound();
  }

  bool is_inited() const
  {
    return inited_;
  }

  void set_input_rows(int64_t input_rows)
  {
    input_rows_ = input_rows;
  }
  void set_input_width(int64_t input_width)
  {
    input_width_ = input_width;
  }

  void set_operator_type(ObPhyOperatorType op_type)
  {
    op_type_ = op_type;
  }
  void set_operator_id(uint64_t op_id)
  {
    op_id_ = op_id;
  }
  void set_exec_ctx(ObExecContext* exec_ctx)
  {
    exec_ctx_ = exec_ctx;
    profile_.set_exec_ctx(exec_ctx);
  }

  void unregister_profile();

  class Compare {
  public:
    Compare();
    int init(const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs);

    // compare function for quick sort.
    bool operator()(const ObChunkDatumStore::StoredRow* l, const ObChunkDatumStore::StoredRow* r);

    // compare function for in-memory merge sort
    bool operator()(ObChunkDatumStore::StoredRow** l, ObChunkDatumStore::StoredRow** r);
    // compare function for external merge sort
    bool operator()(const ObSortOpChunk* l, const ObSortOpChunk* r);

    bool operator()(const common::ObIArray<ObExpr*>* l, const ObChunkDatumStore::StoredRow* r, ObEvalCtx& eval_ctx);

    bool is_inited() const
    {
      return NULL != sort_collations_;
    }
    // interface required by ObBinaryHeap
    int get_error_code()
    {
      return ret_;
    }

    void reset()
    {
      this->~Compare();
      new (this) Compare();
    }

  public:
    int ret_;
    const ObIArray<ObSortFieldCollation>* sort_collations_;
    const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Compare);
  };

  class CopyableComparer {
  public:
    CopyableComparer(Compare& compare) : compare_(compare)
    {}
    bool operator()(const ObChunkDatumStore::StoredRow* l, const ObChunkDatumStore::StoredRow* r)
    {
      return compare_(l, r);
    }
    Compare& compare_;
  };

protected:
  class MemEntifyFreeGuard {
  public:
    explicit MemEntifyFreeGuard(lib::MemoryContext*& entify) : entify_(entify)
    {}
    ~MemEntifyFreeGuard()
    {
      if (NULL != entify_) {
        DESTROY_CONTEXT(entify_);
        entify_ = NULL;
      }
    }
    lib::MemoryContext*& entify_;
  };

  int get_next_row(const common::ObIArray<ObExpr*>& exprs, const ObChunkDatumStore::StoredRow*& sr)
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

  typedef int (ObSortOpImpl::*NextStoredRowFunc)(const ObChunkDatumStore::StoredRow*& sr);

  bool need_imms() const
  {
    return rows_.count() > datum_store_.get_row_cnt();
  }
  int sort_inmem_data();
  int do_dump();
  template <typename Input>
  int build_chunk(const int64_t level, Input& input);

  int build_ems_heap(int64_t& merge_ways);
  template <typename Heap, typename NextFunc, typename Item>
  int heap_next(Heap& heap, const NextFunc& func, Item& item);
  int ems_heap_next(ObSortOpChunk*& chunk);
  int imms_heap_next(const ObChunkDatumStore::StoredRow*& store_row);

  int array_next_stored_row(const ObChunkDatumStore::StoredRow*& sr);
  int imms_heap_next_stored_row(const ObChunkDatumStore::StoredRow*& sr);
  int ems_heap_next_stored_row(const ObChunkDatumStore::StoredRow*& sr);

  // here need dump add two conditions: 1) data_size> expect_size 2) mem_used> global_bound
  // the reason is that the expected size may be one pass size, so the data is larger than the expected size,
  // the total memory cannot exceed the global bound size, otherwise the total memory will exceed the limit
  bool need_dump()
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound() ||
           mem_context_->used() >= profile_.get_max_bound();
  }
  int preprocess_dump(bool& dumped);

  DISALLOW_COPY_AND_ASSIGN(ObSortOpImpl);

protected:
  typedef common::ObBinaryHeap<ObChunkDatumStore::StoredRow**, Compare, 16> IMMSHeap;
  typedef common::ObBinaryHeap<ObSortOpChunk*, Compare, MAX_MERGE_WAYS> EMSHeap;
  static const int64_t MAX_ROW_CNT = 268435456;  // (2G / 8)
  bool inited_;
  bool local_merge_sort_;
  bool need_rewind_;
  bool got_first_row_;
  bool sorted_;
  lib::MemoryContext* mem_context_;
  MemEntifyFreeGuard mem_entify_guard_;
  int64_t tenant_id_;
  const ObIArray<ObSortFieldCollation>* sort_collations_;
  const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;
  ObEvalCtx* eval_ctx_;
  Compare comp_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator iter_;
  int64_t inmem_row_size_;
  int64_t mem_check_interval_mask_;
  int64_t row_idx_;  // for iterate rows_
  common::ObArray<ObChunkDatumStore::StoredRow*> rows_;
  common::ObDList<ObSortOpChunk> sort_chunks_;
  bool heap_iter_begin_;
  // heap for in-memory merge sort local order rows
  IMMSHeap* imms_heap_;
  // heap for external merge sort
  EMSHeap* ems_heap_;
  NextStoredRowFunc next_stored_row_func_;
  int64_t input_rows_;
  int64_t input_width_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
  ObExecContext* exec_ctx_;
};

class ObPrefixSortImpl : public ObSortOpImpl {
public:
  ObPrefixSortImpl();

  // init && start fetch %op rows
  int init(const int64_t tenant_id, const int64_t prefix_pos, const common::ObIArray<ObExpr*>& all_exprs,
      const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs,
      ObEvalCtx* eval_ctx, ObOperator* child, ObOperator* self_op, ObExecContext& exec_ctx, int64_t& sort_row_cnt);

  int get_next_row(const common::ObIArray<ObExpr*>& exprs);

  void reuse();
  void reset();

private:
  // fetch rows in same prefix && do sort, set %next_prefix_row_ to NULL
  // when all child rows are fetched.
  int fetch_rows(const common::ObIArray<ObExpr*>& all_exprs);
  using ObSortOpImpl::init;

private:
  int64_t prefix_pos_;
  const ObIArray<ObSortFieldCollation>* full_sort_collations_;
  const ObIArray<ObSortCmpFunc>* full_sort_cmp_funs_;
  common::ObArrayHelper<ObSortFieldCollation> base_sort_collations_;
  common::ObArrayHelper<ObSortCmpFunc> base_sort_cmp_funs_;

  const ObChunkDatumStore::StoredRow* prev_row_;
  // when got new prefix, save the row to to %next_prefix_row_
  ObChunkDatumStore::ShadowStoredRow<> next_prefix_row_store_;
  ObChunkDatumStore::StoredRow* next_prefix_row_;

  ObOperator* child_;
  ObOperator* self_op_;
  int64_t* sort_row_count_;
};

class ObUniqueSortImpl : public ObSortOpImpl {
public:
  ObUniqueSortImpl() : prev_row_(NULL), prev_buf_size_(0)
  {}

  virtual ~ObUniqueSortImpl()
  {
    free_prev_row();
  }

  int init(const uint64_t tenant_id, const ObIArray<ObSortFieldCollation>* sort_collations,
      const ObIArray<ObSortCmpFunc>* sort_cmp_funs, ObEvalCtx* eval_ctx, const bool need_rewind)
  {
    return ObSortOpImpl::init(
        tenant_id, sort_collations, sort_cmp_funs, eval_ctx, false /* local order */, need_rewind);
  }

  int get_next_row(const common::ObIArray<ObExpr*>& exprs);
  int get_next_stored_row(const ObChunkDatumStore::StoredRow*& sr);

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
  int save_prev_row(const ObChunkDatumStore::StoredRow& sr);
  void free_prev_row();

private:
  ObChunkDatumStore::StoredRow* prev_row_;
  int64_t prev_buf_size_;
};

class ObInMemoryTopnSortImpl {
public:
  ObInMemoryTopnSortImpl();
  virtual ~ObInMemoryTopnSortImpl();
  int init(const int64_t tenant_id, int64_t prefix_pos_, const ObIArray<ObSortFieldCollation>* sort_collations,
      const ObIArray<ObSortCmpFunc>* sort_cmp_funs, ObEvalCtx* eval_ctx);
  virtual void reset();
  virtual void reuse();
  virtual int add_row(const common::ObIArray<ObExpr*>& exprs, bool& need_sort);
  virtual int sort_rows();
  virtual int get_next_row(const common::ObIArray<ObExpr*>& exprs);
  inline int64_t get_row_count() const;
  void set_topn(int64_t topn)
  {
    topn_cnt_ = topn;
  }
  void set_iter_end()
  {
    iter_end_ = true;
  }
  bool is_iter_end()
  {
    return iter_end_;
  }
  int64_t get_topn_cnt()
  {
    return topn_cnt_;
  }
  inline void set_fetch_with_ties(bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }
  // TO_STRING_KV(K_(sort_array_pos));
private:
  // Optimize mem usage/performance of top-n sort:
  // Record buf_len of each allocated row. When old row pop-ed out of the heap
  // and has enough space for new row, use the space of old row to store new row
  // instead of allocating space for new row.
  // Note that this is not perfect solution, it cannot handle the case that row size
  // keeps going up. However, this can cover most cases.
  struct SortStoredRow : public ObChunkDatumStore::StoredRow {
    struct ExtraInfo {
      uint64_t max_size_;
    };
    ExtraInfo& get_extra_info()
    {
      static_assert(sizeof(SortStoredRow) == sizeof(sql::ObChunkDatumStore::StoredRow),
          "sizeof StoredJoinRow must be the save with StoredRow");
      return *reinterpret_cast<ExtraInfo*>(get_extra_payload());
    }
    const ExtraInfo& get_extra_info() const
    {
      return *reinterpret_cast<const ExtraInfo*>(get_extra_payload());
    }

    inline uint64_t get_max_size() const
    {
      return get_extra_info().max_size_;
    }
    inline void set_max_size(const uint64_t max_size)
    {
      get_extra_info().max_size_ = max_size;
    }
  };

private:
  int adjust_topn_heap(const common::ObIArray<ObExpr*>& exprs);
  int convert_row(const SortStoredRow* sr, const common::ObIArray<ObExpr*>& exprs);
  int check_block_row(const common::ObIArray<ObExpr*>& exprs, const SortStoredRow* last_row, bool& is_cur_block);
  bool has_prefix_pos()
  {
    return prefix_pos_ > 0;
  }

private:
  static const int64_t STORE_ROW_HEADER_SIZE = sizeof(SortStoredRow);
  static const int64_t STORE_ROW_EXTRA_SIZE = sizeof(uint64_t);
  // data members
  int64_t prefix_pos_;
  int64_t topn_cnt_;
  int64_t topn_sort_array_pos_;
  bool is_fetch_with_ties_;
  bool iter_end_;
  SortStoredRow* last_row_;
  const ObIArray<ObSortFieldCollation>* sort_collations_;
  const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;
  ObEvalCtx* eval_ctx_;
  ObSortOpImpl::Compare cmp_;
  common::ObArenaAllocator cur_alloc_;  // deep copy current block row
  common::ObBinaryHeap<SortStoredRow*, ObSortOpImpl::Compare> heap_;
  DISALLOW_COPY_AND_ASSIGN(ObInMemoryTopnSortImpl);
};

inline int64_t ObInMemoryTopnSortImpl::get_row_count() const
{
  return heap_.count();
}

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_OP_IMPL_H_ */
