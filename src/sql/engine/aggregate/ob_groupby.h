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

#ifndef _OB_GROUPBY_H
#define _OB_GROUPBY_H
#include "lib/container/ob_array.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/aggregate/ob_aggregate_function.h"

namespace oceanbase {
namespace sql {
typedef common::ObColumnInfo ObGroupColumn;
// struct ObGroupColumn
//{
//  int64_t index_;
//  common::ObCollationType cs_id_;
//  ObGroupColumn() : index_(common::OB_INVALID_INDEX), cs_id_(common::COLLATION_TYPE_NONE) {}
//  NEED_SERIALIZE_AND_DESERIALIZE;
//  TO_STRING_KV2(N_INDEX_ID, index_,
//                N_COLLATION_TYPE, common::ObCharset::collation_name(cs_id_));
//};

class ObGroupBy : public ObSingleChildPhyOperator {
public:
  static const int64_t MIN_BUCKET_COUNT = 100;
  static const int64_t MAX_BUCKET_COUNT = 100000;
  static const int64_t LOCAL_COLUMN_COUNT = 16;

protected:
  class ObGroupByCtx : public ObPhyOperatorCtx {
  public:
    explicit ObGroupByCtx(ObExecContext& exec_ctx) : ObPhyOperatorCtx(exec_ctx), aggr_func_(), child_column_count_(0)
    {}
    virtual ~ObGroupByCtx()
    {}
    virtual void destroy()
    {
      aggr_func_.~ObAggregateFunction();
      ObPhyOperatorCtx::destroy_base();
    }
    inline ObAggregateFunction& get_aggr_func()
    {
      return aggr_func_;
    }
    inline int64_t get_child_column_count() const
    {
      return child_column_count_;
    }
    inline void set_child_column_count(int64_t child_column_count)
    {
      child_column_count_ = child_column_count;
    }

  protected:
    ObAggregateFunction aggr_func_;
    int64_t child_column_count_;
  };
  OB_UNIS_VERSION_V(1);

public:
  explicit ObGroupBy(common::ObIAllocator& alloc);
  virtual ~ObGroupBy();
  virtual void reset();
  virtual void reuse();
  virtual int add_aggr_column(ObAggregateExpression* expr);
  /// set memory limit
  void set_mem_size_limit(const int64_t limit);
  // set rollup statement
  void set_rollup(const bool has_rollup);
  /**
   * @brief add group column index
   * @param column_idx[in], the group column index in the row from child operator,
   *        such as select c1, c2 group by c2, c3,
   *        the row  is ROW(c1, c2, c3) from child operator,
   *        column_idx of c2 is 1, column_idx of c3 is 2
   */
  inline virtual int add_group_column_idx(
      int64_t column_idx, common::ObCollationType cs_type /* = common::COLLATION_TYPE_NONE*/)
  {
    ObGroupColumn group_column;
    group_column.index_ = column_idx;
    group_column.cs_type_ = cs_type;
    return group_col_idxs_.push_back(group_column);
  }
  /**
   * @brief add rolup column index
   */
  inline virtual int add_rollup_column_idx(
      int64_t column_idx, common::ObCollationType cs_type /* = common::COLLATION_TYPE_NONE*/)
  {
    ObGroupColumn group_column;
    group_column.index_ = column_idx;
    group_column.cs_type_ = cs_type;
    return rollup_col_idxs_.push_back(group_column);
  }
  inline virtual const ObAggrExprList& get_aggr_columns() const
  {
    return aggr_columns_;
  }
  int init(int64_t count)
  {
    return init_array_size<>(group_col_idxs_, count);
  }
  int init_rollup(int64_t count)
  {
    return init_array_size<>(rollup_col_idxs_, count);
  }

  int add_udf_meta(ObAggUdfMeta& meta)
  {
    return agg_udf_meta_.push_back(meta);
  }

  void set_est_group_cnt(const int64_t cnt)
  {
    est_group_cnt_ = cnt;
  }

protected:
  int is_same_group(const common::ObRowStore::StoredRow& row1, const common::ObNewRow& row2, bool& result,
      int64_t& first_diff_pos) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const = 0;

  int init_group_by(ObExecContext& ctx) const;
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const = 0;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGroupBy);

protected:
  ObAggrExprList aggr_columns_;
  int64_t mem_size_limit_;  // discarded, never assigned
  int32_t prepare_row_num_;
  int32_t distinct_set_bucket_num_;
  common::ObFixedArray<ObGroupColumn, common::ObIAllocator> group_col_idxs_;
  bool has_rollup_;
  common::ObSEArray<ObAggUdfMeta, 16> agg_udf_meta_;
  int64_t est_group_cnt_;
  common::ObFixedArray<ObGroupColumn, common::ObIAllocator> rollup_col_idxs_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_GROUPBY_H */
