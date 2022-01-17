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

#ifndef OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_DOMAIN_INDEX_H_
#define OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_DOMAIN_INDEX_H_
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/container/ob_fixed_array.h"
#include "storage/ob_i_partition_storage.h"

namespace oceanbase {
namespace sql {
class ObQueryRange;
class ObIterExprOperator;
class ObDomainIndex : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObDomainIndexCtx : public ObPhyOperatorCtx {
  public:
    explicit ObDomainIndexCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx),
          index_scan_iters_(ctx.get_allocator()),
          index_scan_params_(ctx.get_allocator()),
          index_allocator_(common::ObModIds::OB_SQL_TABLE_SCAN_CTX),
          search_tree_(NULL),
          iter_expr_ctx_(ctx, ctx.get_allocator())
    {}
    virtual ~ObDomainIndexCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }

  private:
    common::ObFixedArray<common::ObNewRowIterator*, common::ObIAllocator> index_scan_iters_;
    common::ObFixedArray<storage::ObTableScanParam*, common::ObIAllocator> index_scan_params_;
    common::ObArenaAllocator index_allocator_;
    ObIterExprOperator* search_tree_;
    common::ObSEArray<common::ObObj, 4> keywords_;
    ObIterExprCtx iter_expr_ctx_;
    friend class ObDomainIndex;
  };

  //  struct ObIndexScanInfo
  //  {
  //    OB_UNIS_VERSION(1);
  //  public:
  //    inline void reset()
  //    {
  //      fulltext_filter_ = NULL;
  //      index_range_ = NULL;
  //    }
  //    TO_STRING_KV(K_(*fulltext_filter),
  //                 K_(*index_range));
  //    ObSqlExpression *fulltext_filter_;
  //    ObQueryRange *index_range_;
  //  };
public:
  explicit ObDomainIndex(common::ObIAllocator& allocator);
  virtual ~ObDomainIndex();

  inline void set_table_op_id(uint64_t table_op_id)
  {
    table_op_id_ = table_op_id;
  }
  inline int init_output_column_array(int64_t column_count)
  {
    return output_column_ids_.init(column_count);
  }
  inline int add_output_column(uint64_t column_id)
  {
    return output_column_ids_.push_back(column_id);
  }
  //  inline int init_index_scan_array(int64_t iter_count) { return index_scan_info_.prepare_allocate(iter_count); }
  inline void set_flags(const int64_t flags)
  {
    flags_ = flags;
  }
  inline int set_index_range(const ObQueryRange& index_range)
  {
    return index_range_.deep_copy(index_range);
  }
  inline void set_fulltext_key_idx(int64_t fulltext_key_idx)
  {
    fulltext_key_idx_ = fulltext_key_idx;
  }
  inline void set_fulltext_mode_flag(ObMatchAgainstMode mode_flag)
  {
    fulltext_mode_flag_ = mode_flag;
  }
  void set_fulltext_filter(ObSqlExpression* expr)
  {
    fulltext_filter_ = expr;
  }
  // inline void set_search_tree(ObIterExprOperator *search_tree) { search_tree_ = search_tree; }
  inline int set_search_key_param(const common::ObObj& param)
  {
    return common::ob_write_obj(allocator_, param, search_key_param_);
  }
  inline void set_index_id(uint64_t index_id)
  {
    index_id_ = index_id;
  }
  inline void set_index_projector(int32_t* index_projector, int64_t projector_size)
  {
    index_projector_ = index_projector;
    index_projector_size_ = projector_size;
  }
  int rescan(ObExecContext& ctx) const;

private:
  /**
   * @brief get next row, call get_next to get a row,
   * if filters exist, call each filter in turn to filter the row,
   * if all rows are filtered, return OB_ITER_END,
   * if compute exprs exist, call each expr in turn to calculate
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  int do_domain_index_scan(ObExecContext& ctx) const;
  int do_domain_index_rescan(ObExecContext& ctx) const;
  int do_range_extract(ObExecContext& ctx, bool is_rescan) const;
  int prepare_index_scan_param(ObExecContext& ctx) const;
  virtual bool need_filter_row() const
  {
    return false;
  }
  int wrap_expr_ctx(ObExecContext& exec_ctx, common::ObExprCtx& expr_ctx) const;
  inline int get_search_keyword_text(ObExecContext& exec_ctx, common::ObObj& keyword_text) const;
  inline int get_final_fulltext_filter(
      ObExecContext& ctx, const common::ObObj& keyword, ObSqlExpression*& fulltext_filter, bool need_deep_copy) const;
  inline int replace_key_range(common::ObNewRange& key_range, const common::ObObj& keyword) const;

private:
  uint64_t table_op_id_;
  common::ObIAllocator& allocator_;
  // common::ObFixedArray<ObIndexScanInfo, common::ObIAllocator> index_scan_info_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> output_column_ids_;
  int64_t flags_;
  uint64_t index_id_;
  int32_t* index_projector_;
  int64_t index_projector_size_;
  // ObIterExprOperator *search_tree_;
  common::ObObj search_key_param_;
  ObMatchAgainstMode fulltext_mode_flag_;
  ObQueryRange index_range_;
  int64_t fulltext_key_idx_;
  ObSqlExpression* fulltext_filter_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_TABLE_OB_DOMAIN_INDEX_H_ */
