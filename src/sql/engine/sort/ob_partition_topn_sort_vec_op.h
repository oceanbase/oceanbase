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

#ifndef OCEANBASE_SQL_ENGINE_SORT_PARTITION_TOPN_SORT_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_PARTITION_TOPN_SORT_VEC_OP_H_

#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/sort/ob_i_sort_vec_op_impl.h"
#include "sql/engine/sort/ob_sort_adaptive_qs_vec_op.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_sort_compare_vec_op.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_key_fetcher_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_context.h"

namespace oceanbase {
namespace sql {
class ObPartTopnRowBuffer {
  public:
    const static int64_t MAX_BUFFER_SIZE = 1024 * 1024 * 1024;
    const static int64_t MIN_BUFFER_SIZE = 2 * 1024;
    ObPartTopnRowBuffer(ObSqlWorkAreaProfile &profile,
                        common::ObIAllocator &allocator,
                        ObSqlMemMgrProcessor &sql_mem_processor,
                        int64_t &inmem_row_size)
      : profile_(profile), sql_mem_processor_(sql_mem_processor), allocator_(allocator),
        inmem_row_size_(inmem_row_size), row_buffers_(allocator_) {}
    ~ObPartTopnRowBuffer() {
      reset();
    }

    struct RowBuffer {
      char *buffer_{nullptr};
      int64_t buffer_size_{0};
      int64_t buffer_offset_{0};
    };

    void init(int64_t est_rows) {
      est_rows_ = est_rows;
    }

    int reset() {
      int ret = OB_SUCCESS;
      OZ(reuse());
      row_buffers_.reset();
      return ret;
    }
    int reuse() {
      int ret = OB_SUCCESS;
      while (OB_SUCC(ret) && !row_buffers_.empty()) {
        RowBuffer row_buf;
        if (OB_FAIL(row_buffers_.pop_front(row_buf))) {
          SQL_ENG_LOG(WARN, "failed to pop back row buffer", K(ret));
        } else if (OB_NOT_NULL(row_buf.buffer_)) {
          sql_mem_processor_.alloc(-1 * row_buf.buffer_size_);
          inmem_row_size_ -= row_buf.buffer_size_;
          allocator_.free(row_buf.buffer_);
        } else {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "row buffer is null", K(ret));
        }
      }
      return ret;
    }
    int64_t max_mem_bound() const {
      return 0.8 * profile_.get_global_bound_size();
    }
    int get_mem(int64_t size, char *&buffer) {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(row_buffers_.empty())) {
        RowBuffer row_buf;
        int64_t buffer_size = min(next_pow2(size * est_rows_ / 10), MAX_BUFFER_SIZE);
        buffer_size = max(buffer_size, MIN_BUFFER_SIZE);
        buffer_size = min(buffer_size, max_mem_bound());
        row_buf.buffer_size_ = buffer_size;
        row_buf.buffer_offset_ = 0;
        row_buf.buffer_ = reinterpret_cast<char *>(allocator_.alloc(buffer_size));
        if (OB_ISNULL(row_buf.buffer_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "alloc buf failed", K(ret), K(buffer_size));
        } else {
          sql_mem_processor_.alloc(buffer_size);
          inmem_row_size_ += buffer_size;
          row_buffers_.push_back(row_buf);
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        RowBuffer &row_buf = row_buffers_.get_last();
        if (OB_ISNULL(row_buf.buffer_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "row buffer is null", K(ret));
        } else if (row_buf.buffer_offset_ + size > row_buf.buffer_size_) {
            RowBuffer new_row_buf;
            new_row_buf.buffer_size_ = min(2 * row_buf.buffer_size_, max_mem_bound()); // double the buffer size
            new_row_buf.buffer_size_ = max(new_row_buf.buffer_size_, size);
            new_row_buf.buffer_offset_ = 0;
            new_row_buf.buffer_ = reinterpret_cast<char *>(allocator_.alloc(new_row_buf.buffer_size_));
            if (OB_ISNULL(new_row_buf.buffer_)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SQL_ENG_LOG(WARN, "alloc buf failed", K(ret), K(new_row_buf.buffer_size_));
            } else {
              sql_mem_processor_.alloc(new_row_buf.buffer_size_);
              inmem_row_size_ += new_row_buf.buffer_size_;
              buffer = new_row_buf.buffer_;
              new_row_buf.buffer_offset_ += size;
              row_buffers_.push_back(new_row_buf);
            }
        } else {
          buffer = row_buf.buffer_ + row_buf.buffer_offset_;
          row_buf.buffer_offset_ += size;
        }
      }
      return ret;
    }
  private:
    ObSqlWorkAreaProfile &profile_;
    ObSqlMemMgrProcessor &sql_mem_processor_;
    common::ObIAllocator &allocator_;
    int64_t &inmem_row_size_;
    common::ObList<RowBuffer, ObIAllocator> row_buffers_;
    int64_t est_rows_;

  };
// Vectorization 2.0 partition topn sort implementation class
template <typename Compare, typename Store_Row, bool has_addon>
class ObPartitionTopNSort
{
public:
  ObPartitionTopNSort(ObIAllocator &allocator,
                      lib::MemoryContext &mem_context,
                      ObSqlWorkAreaProfile &profile,
                      Compare &comp,
                      ObSortKeyFetcher &sort_exprs_getter,
                      ObSqlMemMgrProcessor &sql_mem_processor,
                      int64_t &inmem_row_size,
                      int64_t &outputted_rows_cnt)
    : allocator_(allocator),
      sql_mem_processor_(sql_mem_processor),
      mem_context_(mem_context),
      comp_(comp),
      sort_exprs_getter_(sort_exprs_getter),
      outputted_rows_cnt_(outputted_rows_cnt),
      pt_row_buffer_(profile, allocator, sql_mem_processor, inmem_row_size) {}
  ~ObPartitionTopNSort() {
    reset();
  }
  void reset();
  void reuse();
  int init(ObSortVecOpContext &ctx, ObIAllocator *page_allocator, ObIArray<ObExpr *> *all_exprs,
            const RowMeta *sk_row_meta, const RowMeta *addon_row_meta);
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
  void reset_row_idx() {
    row_idx_ = 0;
  }

private:
  struct TopnNode
  {
    TopnNode(Compare &cmp, common::ObIAllocator *allocator = NULL):
      reuse_idx_(-1), top_row_(nullptr),
      rows_array_(64, ModulePageAllocator(*allocator, "SortOpRows")),
      ties_array_(64, ModulePageAllocator(*allocator, "SortOpRows")) {
    }
    ~TopnNode() {
      rows_array_.reset();
      ties_array_.reset();
    }
    Store_Row *get_top() const { return top_row_ != nullptr ? top_row_ : rows_array_.at(0); }
    TO_STRING_EMPTY();
    uint64_t hash_val_{0};
    int64_t reuse_idx_{-1};
    Store_Row *top_row_{nullptr};
    common::ObSEArray<Store_Row *, 16> rows_array_;
    common::ObSEArray<Store_Row *, 8> ties_array_;
  };
  struct PartTopnNode
  {
    PartTopnNode(Compare &cmp, common::ObIAllocator *allocator = NULL) :
      hash_node_next_(NULL),
      topn_node_(cmp, allocator) {}
    ~PartTopnNode() {hash_node_next_ = NULL; topn_node_.~TopnNode(); }
    PartTopnNode *hash_node_next_;
    TopnNode topn_node_;
    TO_STRING_EMPTY();
  };
  class TopnNodeComparer
  {
  public:
    TopnNodeComparer(Compare &compare) : compare_(compare) {}
    bool operator()(const TopnNode *l, const TopnNode *r)
    {
      return compare_(l->get_top(), r->get_top());
    }
    Compare &compare_;
  };
  class CopyableComparer
  {
  public:
    CopyableComparer(Compare &compare) : compare_(compare)
    {}
    OB_INLINE bool operator()(const Store_Row *l, const Store_Row *r)
    {
      return compare_(l, r);
    }
    Compare &compare_;
  };
  class ObPartitionTopnVecMemChecker
  {
  public:
    ObPartitionTopnVecMemChecker(int64_t row_cnt):
      cur_row_cnt_(row_cnt)
      {}
    bool operator()(int64_t max_row_cnt)
    {
      return cur_row_cnt_ > max_row_cnt;
    }
    int64_t cur_row_cnt_;
  };
  // general func
  int update_max_available_mem_size_periodically()
  {
    int ret = OB_SUCCESS;
    if (!got_first_row_) {
      got_first_row_ = true;
    } else {
      bool updated = false;
      ObPartitionTopnVecMemChecker checker(row_count_);
      if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
            &mem_context_->get_malloc_allocator(), checker, updated))) {
        SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
      } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
        SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
      }
    }
    return ret;
  }
  int batch_eval_vector(const common::ObIArray<ObExpr *> &exprs, const ObBatchRows &input_brs)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      ObExpr *expr = exprs.at(i);
      if (OB_FAIL(expr->eval_vector(*eval_ctx_, input_brs))) {
        SQL_ENG_LOG(WARN, "eval failed", K(ret));
      }
    }
    return ret;
  }
  int before_add_batch(const ObBatchRows &input_brs)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(update_max_available_mem_size_periodically())) {
      SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
    } else if (OB_FAIL(batch_eval_vector(*all_exprs_, input_brs))) {
      SQL_ENG_LOG(WARN, "failed to eval vector", K(ret));
    } else if (OB_FAIL(sort_exprs_getter_.fetch_payload(input_brs))) {
      SQL_ENG_LOG(WARN, "failed to batch fetch sort key payload", K(ret));
    } else if(FALSE_IT(comp_.set_sort_key_col_result_list(
                    sort_exprs_getter_.get_sk_col_result_list()))) {
    }
    return ret;
  }
  int before_add_batch(const uint16_t selector[], const int64_t size)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(update_max_available_mem_size_periodically())) {
      SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
    } else if (OB_FAIL(sort_exprs_getter_.fetch_payload(selector, size))) {
      SQL_ENG_LOG(WARN, "failed to batch fetch sort key payload", K(ret));
    } else if(FALSE_IT(comp_.set_sort_key_col_result_list(
                    sort_exprs_getter_.get_sk_col_result_list()))) {
    }
    return ret;
  }

  // partition topn sort func
  int is_equal_part(const Store_Row *l, const ObEvalCtx &eval_ctx, const RowMeta &row_meta,
    bool &is_equal);
  int imms_partition_topn_next(const Store_Row *&store_row);
  int init_topn();
  int init_bucket_array(const int64_t est_rows);
  int resize_buckets();
  void reuse_part_topn_node();
  int locate_current_topn_node();
  int locate_current_topn_node_in_bucket(
                uint64_t hash_value,
                PartTopnNode *first_node,
                PartTopnNode *&exist);
  int add_part_topn_sort_row(const Store_Row *&store_row);
  int add_part_topn_sort_batch(const ObBatchRows &input_brs,
                const int64_t start_pos /* 0 */,
                int64_t *append_row_count,
                bool need_load_data);
  int add_part_topn_sort_batch(const ObBatchRows &input_brs,
                const uint16_t selector[],
                const int64_t size);
  typedef int (ObPartitionTopNSort::*NextStoredRowFunc)(const Store_Row *&sr);
  typedef int (ObPartitionTopNSort::*AddTopNRowFunc)(const Store_Row *&sr);
  typedef int (ObPartitionTopNSort::*TopNSortFunc)(TopnNode *node);
  int build_row(const common::ObIArray<ObExpr *> &exprs,
                const RowMeta &row_meta,
                const int64_t row_size,
                ObEvalCtx &ctx,
                ObCompactRow *&stored_row);
  int copy_to_row(Store_Row *&sk_row);
  int copy_to_row(const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, bool is_sort_key, Store_Row *&row);
  // for quick select sort
  int do_quick_select(common::ObIArray<Store_Row *> &rows, int64_t row_begin, int64_t row_end, int64_t k);
  int do_quick_select(TopnNode *node, int64_t k);
  int do_quick_select_with_ties(TopnNode *node, int64_t k);
  int add_quick_select_row(const Store_Row *&sr);
  int add_quick_select_row_with_ties(const Store_Row *&sr);
  template <bool with_ties>
  int add_quick_select_row_impl(const Store_Row *&sr);
  int finish_in_quick_select(TopnNode *node);

  using BucketArray = common::ObSegmentArray<PartTopnNode *,
                                             OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                             common::ModulePageAllocator>;
  int prepare_bucket_array(uint64_t bucket_num, BucketArray *&buckets)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buckets)) {
      BucketArray *array_ptr = nullptr;
      void *buckets_buf = nullptr;
      if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else if (FALSE_IT(array_ptr = new (buckets_buf) BucketArray(
                          *reinterpret_cast<common::ModulePageAllocator *>(page_allocator_)))) {
      } else if (OB_FAIL(array_ptr->init(bucket_num))) {
        SQL_ENG_LOG(WARN, "failed to init bucket", K(ret), K(bucket_num));
      } else {
        array_ptr->set_all(nullptr);
        buckets = array_ptr;
      }
    } else {
      buckets->reuse();
      if (OB_FAIL(buckets->init(bucket_num))) {
        SQL_ENG_LOG(WARN, "failed to init bucket", K(ret), K(bucket_num));
      } else {
        buckets->set_all(nullptr);
      }
    }
    return ret;
  }
private:

  static const int64_t MIN_BUCKET_COUNT = 1L << 14;  //16384;
  static const int64_t MAX_BUCKET_COUNT = 1L << 21; //2097152;
  static constexpr double ENLARGE_BUCKET_NUM_FACTOR = 0.75;
  common::ObIAllocator &allocator_;
  common::ObIAllocator *page_allocator_;
  ObSqlMemMgrProcessor &sql_mem_processor_;
  lib::MemoryContext &mem_context_;
  const ObIArray<ObSortFieldCollation> *sk_collations_{nullptr};
  const RowMeta *sk_row_meta_{nullptr};
  const common::ObIArray<ObExpr *> *sk_exprs_{nullptr};
  const RowMeta *addon_row_meta_{nullptr};
  const common::ObIArray<ObExpr *> *addon_exprs_{nullptr};
  common::ObIArray<ObExpr *> *all_exprs_{nullptr};

  ObEvalCtx *eval_ctx_;
  Compare &comp_;
  ObSortKeyFetcher &sort_exprs_getter_;
  int64_t &outputted_rows_cnt_;
  int64_t max_bucket_cnt_{0};
  int64_t part_cnt_{0};
  int64_t topn_cnt_{INT64_MAX};
  bool is_fetch_with_ties_{false};
  int64_t row_count_{0};
  int64_t row_idx_{0};
  TopnNode *topn_node_{nullptr};
  BucketArray *pt_buckets_{nullptr};
  ObSEArray<TopnNode*, 16> topn_nodes_;
  int64_t cur_topn_node_idx_{0};
  int64_t part_group_cnt_{0};
  int64_t hash_collision_cnt_{0};
  AddTopNRowFunc add_topn_row_func_{nullptr};
  TopNSortFunc topn_sort_func_{nullptr};
  ObPartTopnRowBuffer pt_row_buffer_;
  bool got_first_row_{false};
};

} // end namespace sql
} // end namespace oceanbase

#include "ob_partition_topn_sort_vec_op.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_PARTITION_TOPN_SORT_VEC_OP_H_ */
