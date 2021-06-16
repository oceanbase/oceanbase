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

#ifndef OCEANBASE_SORT_OB_SORT_IMPL_H_
#define OCEANBASE_SORT_OB_SORT_IMPL_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase {
namespace sql {

struct ObSortChunk : public common::ObDLinkBase<ObSortChunk> {
  explicit ObSortChunk(const int64_t level) : level_(level), row_(NULL)
  {}

  int64_t level_;
  ObChunkRowStore row_store_;
  ObChunkRowStore::Iterator iter_;
  const ObChunkRowStore::StoredRow* row_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSortChunk);
};

//
// Sort rows, do in memory sort if memory can hold all rows, otherwise do disk sort.
// Prefix sorting is not supported it can be implemented by by simply wrapping ObSortImpl.
//
// Rows in local/partial order (e.g.: 1,3,5,7,2,4,6,8) are sorted by in-memory merge sort first.
// (init with true %in_local_order parameter).
//
// Interface:
//    ObSortImpl sort_impl;
//    sort_impl.init();
//    while (OB_ITER_END != get_row(row)) {
//      sort_impl.add(row);
//    }
//    sort_impl.sort();
//    ObNewRow *row_ptr = NULL;
//    while (OB_ITER_END != ret) {
//      ret = sort_impl.get_next_row(row_ptr);
//      ...
//    }
//
//    // after sort and iteration, you can add row and sort again. (not efficient)
//    // NOTE: need_rewind must be true.
//    while () {
//      sort_impl.add(row);
//    }
//    sort_impl.sort();
//    while () {
//      sort_impl.get_next_row();
//    }
//
// Abbrev:
//  imms/IMMS: in-memory merge sort
//  ems/EMS: external merge sort
//
class ObSortImpl {
public:
  static const int64_t EXTEND_MULTIPLE = 2;
  static const int64_t MAX_MERGE_WAYS = 256;
  static const int64_t INMEMORY_MERGE_SORT_WARN_WAYS = 10000;
  typedef common::ObIArray<ObSortColumn> SortColumns;
  typedef common::ObIArray<ObOpSchemaObj> SortExtraInfos;

  ObSortImpl();
  virtual ~ObSortImpl();

  // if rewind id not needed, we will release the resource after iterate end.
  int init(const uint64_t tenant_id, const SortColumns& sort_columns, const SortExtraInfos* extra_infos,
      const bool in_local_order = false, const bool need_rewind = false);

  // keep initialized, can sort same rows (same cell type, cell count, projector) after reuse.
  void reuse();
  // reset to state before init
  void reset();
  void destroy()
  {
    reset();
  }

  // Add row and return the stored row.
  int add_row(const common::ObNewRow& row, const ObChunkRowStore::StoredRow*& store_row);
  int add_row(const common::ObNewRow& row)
  {
    const ObChunkRowStore::StoredRow* store_row = NULL;
    return add_row(row, store_row);
  }
  int sort();
  int get_next_row(const common::ObNewRow*& row)
  {
    const ObChunkRowStore::StoredRow* sr = NULL;
    return get_next_row(row, sr);
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
  int get_sort_columns(common::ObIArray<ObSortColumn>& sort_columns);

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
  class Compare {
  public:
    Compare();
    int init(common::ObArenaAllocator& alloc, const common::ObNewRow& row, const SortColumns* sort_columns,
        const SortExtraInfos* extra_infos);

    // compare function for quick sort.
    bool operator()(const ObChunkRowStore::StoredRow* l, const ObChunkRowStore::StoredRow* r);

    // compare function for in-memory merge sort
    bool operator()(ObChunkRowStore::StoredRow** l, ObChunkRowStore::StoredRow** r);
    // compare function for external merge sort
    bool operator()(const ObSortChunk* l, const ObSortChunk* r);

    bool is_inited() const
    {
      return NULL != sort_columns_;
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
    const SortColumns* sort_columns_;
    int32_t* indexes_;
    ObCmpNullPos* null_poses_;
    obj_cmp_func_nullsafe* cmp_funcs_;
    ObCompareCtx cmp_ctx_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Compare);
  };

  class CopyableComparer {
  public:
    CopyableComparer(Compare& compare) : compare_(compare)
    {}
    bool operator()(const ObChunkRowStore::StoredRow* l, const ObChunkRowStore::StoredRow* r)
    {
      return compare_(l, r);
    }
    Compare& compare_;
  };

  typedef int (ObSortImpl::*NextRowFunc)(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr);
  int get_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr)
  {
    int ret = common::OB_SUCCESS;
    if (!is_inited()) {
      ret = common::OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not init", K(ret));
    } else {
      ret = (this->*next_row_func_)(row, sr);
      if (OB_UNLIKELY(common::OB_ITER_END == ret) && !need_rewind_) {
        reuse();
      }
    }
    return ret;
  }

  bool need_imms() const
  {
    return rows_.count() > row_store_.get_row_cnt();
  }
  int sort_inmem_data();
  int do_dump();
  template <typename Input>
  int build_chunk(const int64_t level, Input& input);

  int build_ems_heap(int64_t& merge_ways);
  template <typename Heap, typename NextFunc, typename Item>
  int heap_next(Heap& heap, const NextFunc& func, Item& item);
  int ems_heap_next(ObSortChunk*& chunk);
  int imms_heap_next(const ObChunkRowStore::StoredRow*& store_row);

  int array_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr);
  int imms_heap_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr);
  int ems_heap_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr);

  // here need dump add two conditions: 1) data_size> expect_size 2) mem_used> global_bound
  // the reason is that the expected size may be one pass size, so the data is larger than the expected size,
  // the total memory cannot exceed the global bound size, otherwise the total memory will exceed the limit
  bool need_dump()
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound() ||
           mem_context_->used() >= profile_.get_max_bound();
  }
  int preprocess_dump(bool& dumped);

  DISALLOW_COPY_AND_ASSIGN(ObSortImpl);

protected:
  typedef common::ObBinaryHeap<ObChunkRowStore::StoredRow**, Compare, 16> IMMSHeap;
  typedef common::ObBinaryHeap<ObSortChunk*, Compare, MAX_MERGE_WAYS> EMSHeap;
  static const int64_t MAX_ROW_CNT = 268435456;  // (2G / 8)
  bool inited_;
  bool local_merge_sort_;
  bool need_rewind_;
  bool got_first_row_;
  bool sorted_;
  lib::MemoryContext* mem_context_;
  MemEntifyFreeGuard mem_entify_guard_;
  int64_t tenant_id_;
  const SortColumns* sort_columns_;
  const SortExtraInfos* extra_infos_;
  Compare comp_;
  ObChunkRowStore row_store_;
  ObChunkRowStore::Iterator iter_;
  int64_t inmem_row_size_;
  int64_t mem_check_interval_mask_;
  int64_t row_idx_;  // for iterate rows_
  common::ObArray<ObChunkRowStore::StoredRow*> rows_;
  common::ObDList<ObSortChunk> sort_chunks_;
  bool heap_iter_begin_;
  // heap for in-memory merge sort local order rows
  IMMSHeap* imms_heap_;
  // heap for external merge sort
  EMSHeap* ems_heap_;
  NextRowFunc next_row_func_;
  int64_t input_rows_;
  int64_t input_width_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
  ObExecContext* exec_ctx_;
};

// Simply wrap for unique sort.
// NOTE:
//  Can not be accessed by base class pointer, since methods are not virtual.
class ObUniqueSort : public ObSortImpl {
public:
  ObUniqueSort() : prev_row_(NULL), prev_buf_size_(0)
  {}

  virtual ~ObUniqueSort()
  {
    free_prev_row();
  }

  int init(const uint64_t tenant_id, const SortColumns& sort_columns, const bool need_rewind)
  {
    return ObSortImpl::init(tenant_id, sort_columns, NULL /* extra info */, false /* local order */, need_rewind);
  }

  int get_next_row(const common::ObNewRow*& row);

  void reuse();
  void reset();

  int sort()
  {
    free_prev_row();
    return ObSortImpl::sort();
  }

  int rewind()
  {
    free_prev_row();
    return ObSortImpl::rewind();
  }

private:
  int save_prev_row(const ObChunkRowStore::StoredRow& sr);
  void free_prev_row();

private:
  ObChunkRowStore::StoredRow* prev_row_;
  int64_t prev_buf_size_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif  // OCEANBASE_SORT_OB_SORT_IMPL_H_
