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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_H_

#include "lib/string/ob_string.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/sort/ob_merge_sort.h"
#include "sql/engine/sort/ob_in_memory_topn_sort.h"
#include "sql/engine/sort/ob_specific_columns_sort.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/sort/ob_local_merge_sort.h"
#include "sql/engine/sort/ob_sort_impl.h"
#include "lib/container/ob_array_helper.h"

namespace oceanbase {
namespace sql {
class ObExecContext;

class ObPrefixSort : public ObSortImpl {
public:
  ObPrefixSort();

  // init && start fetch %op rows
  int init(const int64_t tenant_id, const int64_t prefix_pos, const SortColumns& sort_columns,
      const SortExtraInfos& extra_infos, const ObPhyOperator& op, ObExecContext& exec_ctx, int64_t& sort_row_cnt);

  int get_next_row(const common::ObNewRow*& row);

  void reuse();
  void reset();

private:
  // fetch rows in same prefix && do sort, set %cur_row_ to NULL all child rows are fetched.
  int fetch_rows();
  using ObSortImpl::init;

private:
  int64_t prefix_pos_;
  const SortColumns* full_sort_columns_;
  common::ObArrayHelper<ObSortColumn> base_sort_columns_;
  common::ObArrayHelper<ObOpSchemaObj> base_extra_infos_;

  const ObChunkRowStore::StoredRow* prev_row_;
  const common::ObNewRow* cur_row_;

  const ObPhyOperator* op_;
  int64_t* sort_row_count_;
};

class ObSort : public ObSingleChildPhyOperator, public ObSortableTrait {
  OB_UNIS_VERSION_V(1);
  static const int64_t MEM_LIMIT_SIZE = 128 * 1024 * 1024;  // 128M
private:
  class ObSortCtx : public ObPhyOperatorCtx {
    friend class ObSort;

  public:
    explicit ObSortCtx(ObExecContext& ctx);
    virtual ~ObSortCtx()
    {}
    virtual void destroy();

  private:
    ObSortImpl sort_impl_;
    ObPrefixSort prefix_sort_impl_;
    ObInMemoryTopnSort topn_sort_;
    int (ObSort::*read_func_)(ObExecContext& exec_ctx, ObSortCtx& sort_ctx, const ObNewRow*& row) const;
    int64_t sort_row_count_;
    bool is_first_;
    int64_t ret_row_count_;
  };

public:
  explicit ObSort(common::ObIAllocator& alloc);
  virtual ~ObSort()
  {}
  virtual void reset();
  virtual void reuse();
  void set_mem_limit(const int64_t limit);
  inline void set_topn_expr(ObSqlExpression* expr)
  {
    topn_expr_ = expr;
  }
  virtual int rescan(ObExecContext& exec_ctx) const;
  int64_t get_mem_size_limit() const;
  int get_sort_row_count(ObExecContext& exec_ctx, int64_t& sort_row_count) const;
  int get_topn_count(ObExecContext& exec_ctx, int64_t& topn) const;
  int set_topk_params(
      ObSqlExpression* limit, ObSqlExpression* offset, int64_t minimum_row_count, int64_t topk_precision);
  inline void set_prefix_pos(const int64_t prefix_pos)
  {
    prefix_pos_ = prefix_pos;
  }
  inline int64_t get_prefix_pos() const
  {
    return prefix_pos_;
  }
  inline bool is_prefix_sort() const
  {
    return 0 != prefix_pos_;
  }
  inline void set_local_merge_sort(bool is_local_merge_sort)
  {
    is_local_merge_sort_ = is_local_merge_sort;
  }
  inline void set_fetch_with_ties(bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }
  // TODO():minimum_row_count and topk_precision should be defined as N_XXX after library
  // is  merged
  TO_STRING_KV(N_ID, id_, N_COLUMN_COUNT, column_count_, N_PROJECTOR,
      common::ObArrayWrap<int32_t>(projector_, projector_size_), N_FILTER_EXPRS, filter_exprs_, N_CALC_EXPRS,
      calc_exprs_, N_ORDER_BY, sort_columns_, N_LIMIT, mem_limit_, "minimum_row_count", minimum_row_count_,
      "topk_precision", topk_precision_);

private:
  // function members
  int process_sort(ObExecContext& exec_ctx, ObSortCtx& sort_ctx) const;
  int get_int_value(ObExecContext& ctx, const ObSqlExpression* in_val, int64_t& out_val) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& exec_ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& exec_ctx) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;

  template <typename T>
  int sort_component_next(T& component, common::ObNewRow& row) const
  {
    const common::ObNewRow* r = NULL;
    int ret = component.get_next_row(r);
    if (common::OB_SUCCESS == ret) {
      if (NULL == r) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "NULL row returned", K(ret));
      }
      ret = ObBaseSort::attach_row(*r, row);
    }
    return ret;
  }

  int sort_impl_next(ObExecContext&, ObSortCtx& sort_ctx, const ObNewRow*& row) const
  {
    row = &sort_ctx.cur_row_;
    return sort_component_next(sort_ctx.sort_impl_, sort_ctx.cur_row_);
  }

  int prefix_sort_impl_next(ObExecContext&, ObSortCtx& sort_ctx, const ObNewRow*& row) const
  {
    row = &sort_ctx.cur_row_;
    return sort_component_next(sort_ctx.prefix_sort_impl_, sort_ctx.cur_row_);
  }

  int topn_sort_next(ObExecContext& exec_ctx, ObSortCtx& sort_ctx, const ObNewRow*& row) const;

private:
  int64_t mem_limit_;  // unused, use GCONF.__sort_area_size as mem limit instead
  ObSqlExpression* topn_expr_;
  // used for topk
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  ObSqlExpression* topk_limit_expr_;
  ObSqlExpression* topk_offset_expr_;
  int64_t prefix_pos_;  // for prefix_sort
  bool is_local_merge_sort_;
  bool is_fetch_with_ties_;  // for fetch with ties
  DISALLOW_COPY_AND_ASSIGN(ObSort);
};

inline int64_t ObSort::get_mem_size_limit() const
{
  return mem_limit_;
}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_H_ */
