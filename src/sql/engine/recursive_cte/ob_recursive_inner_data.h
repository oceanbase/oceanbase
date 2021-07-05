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

#ifndef OB_RECURSIVE_INNER_DATA_
#define OB_RECURSIVE_INNER_DATA_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_double_children_phy_operator.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/sort/ob_specific_columns_sort.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "ob_search_method.h"
#include "ob_fake_cte_table.h"

namespace oceanbase {
namespace sql {
class ObRecursiveInnerData {
  using ObTreeNode = ObSearchMethod::ObTreeNode;
  friend class ObRecursiveUnionAllOperatorCtx;
  friend class ObRecursiveUnionAll;

public:
  struct RowComparer;
  enum RecursiveUnionState { R_UNION_BEGIN, R_UNION_READ_LEFT, R_UNION_READ_RIGHT, R_UNION_END, R_UNION_STATE_COUNT };
  enum SearchStrategyType { DEPTH_FRIST, BREADTH_FRIST };

public:
  explicit ObRecursiveInnerData(common::ObIAllocator& alloc)
      : state_(RecursiveUnionState::R_UNION_READ_LEFT),
        stored_row_buf_(ObModIds::OB_SQL_CTE_ROW),
        pump_operator_(nullptr),
        left_op_(nullptr),
        right_op_(nullptr),
        search_type_(SearchStrategyType::BREADTH_FRIST),
        search_by_col_lists_(),
        cycle_by_col_lists_(),
        result_output_(stored_row_buf_),
        cte_pseudo_column_row_desc_(alloc),
        cur_row_(nullptr),
        cycle_value_(),
        non_cycle_value_(),
        ordering_column_(1),
        dfs_pump_(stored_row_buf_),
        bfs_pump_(stored_row_buf_),
        calc_buf_(NULL)
  {}
  ~ObRecursiveInnerData() = default;

  inline void set_left_child(ObPhyOperator* op)
  {
    left_op_ = op;
  };
  inline void set_right_child(ObPhyOperator* op)
  {
    right_op_ = op;
  };
  inline void set_fake_cte_table(const ObFakeCTETable* cte_table)
  {
    pump_operator_ = cte_table;
  };
  inline void set_search_strategy(ObRecursiveInnerData::SearchStrategyType strategy)
  {
    search_type_ = strategy;
  };
  int add_search_column(ObSortColumn col);
  int add_cycle_column(common::ObColumnInfo);
  int get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row);
  int rescan(ObExecContext& ctx);
  int set_fake_cte_table_empty(ObExecContext& ctx);
  int init(const common::ObIArray<int64_t>& cte_pseudo_column_row_desc);
  void set_op_schema_objs(const common::ObIArray<ObOpSchemaObj>& op_schema_objs)
  {
    dfs_pump_.set_op_schema_objs(op_schema_objs);
  }
  void set_calc_buf(common::ObIAllocator* calc_buf)
  {
    calc_buf_ = calc_buf;
  }

private:
  void destroy();
  int add_pseudo_column(const ObNewRow* row, bool cycle = false);
  int try_get_left_rows(ObExecContext& exec_ctx, const ObNewRow*& row);
  int try_get_right_rows(ObExecContext& exec_ctx, const ObNewRow*& row);
  int try_format_output_row(const ObNewRow*& output_row);
  /**
   * left child of recursive union is called plan a, and right child is called plan b
   * plan a generate initial data, plan b is executed recursively.
   */
  int get_all_data_from_left_child(ObExecContext& exec_ctx);
  int get_all_data_from_right_child(ObExecContext& exec_ctx);
  int depth_first_union(ObExecContext& exec_ctx, const bool sort = true);
  int breadth_first_union(ObExecContext& exec_ctx, bool left_branch, bool& continue_search);
  int start_new_level(ObExecContext& exec_ctx, bool left_branch);
  // output one row to fake_cte_table operator,use it as next intput of plan b.
  int fake_cte_table_add_row(ObExecContext& exec_ctx, ObTreeNode& node);
  int assign_to_cur_row(ObNewRow& cur_row, const ObNewRow& input_row);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRecursiveInnerData);

private:
  RecursiveUnionState state_;
  common::ObArenaAllocator stored_row_buf_;
  const ObFakeCTETable* pump_operator_;
  ObPhyOperator* left_op_;
  ObPhyOperator* right_op_;
  // BFS or DFS
  SearchStrategyType search_type_;
  common::ObSEArray<ObSortColumn, 32> search_by_col_lists_;
  common::ObSEArray<common::ObColumnInfo, 32> cycle_by_col_lists_;
  common::ObList<ObTreeNode, common::ObIAllocator> result_output_;
  common::ObFixedArray<int64_t, common::ObIAllocator> cte_pseudo_column_row_desc_;
  // cur row
  common::ObNewRow* cur_row_;
  // cycle value
  common::ObObj cycle_value_;
  // non-cycle value
  common::ObObj non_cycle_value_;
  /**
   * Oracle explaination:
   * The ordering_column is automatically added to the column list for the query name.
   * The query that selects from query_name can include an ORDER BY on ordering_column to return the rows in the order
   * that was specified by the SEARCH clause.
   */
  int64_t ordering_column_;
  ObDepthFisrtSearch dfs_pump_;
  ObBreadthFisrtSearch bfs_pump_;
  common::ObIAllocator* calc_buf_;
};
}  // end namespace sql
}  // end namespace oceanbase

#endif
