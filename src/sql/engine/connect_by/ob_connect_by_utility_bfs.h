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

#ifndef __OB_SQL_CONNECT_BY_UTILITY2_h_
#define __OB_SQL_CONNECT_BY_UTILITY2_h_

#include "sql/engine/connect_by/ob_connect_by_utility.h"

namespace oceanbase {
namespace sql {
class ObSortColumn;

class ObConnectByWithIndex;
class ObConnectByPumpBFS : public ObConnectByPumpBase {
  friend ObConnectByWithIndex;

private:
  class PathNode {
  public:
    PathNode() : prior_exprs_result_(NULL), paths_(), level_(0)
    {}
    ~PathNode()
    {}
    void reset()
    {
      prior_exprs_result_ = NULL;
      for (int64_t i = 0; i < paths_.count(); ++i) {
        paths_.at(i).reset();
      }
      paths_.reset();
      level_ = 0;
    }
    int init_path_array(const int64_t size);

    TO_STRING_KV(KPC(prior_exprs_result_), K(paths_), K(level_));
    const ObNewRow* prior_exprs_result_;
    // results of sys_connect_by_path functions in a connect by query.
    ObSEArray<common::ObString, 16> paths_;
    int64_t level_;
  };

  class PumpNode {
  public:
    PumpNode() : pump_row_(NULL), output_row_(NULL), path_node_()
    {}
    ~PumpNode()
    {}
    void reset()
    {
      pump_row_ = NULL;
      output_row_ = NULL;
      path_node_.reset();
    }
    TO_STRING_KV(KPC(pump_row_), KPC(output_row_), K(path_node_));
    const ObNewRow* pump_row_;
    const ObNewRow* output_row_;
    PathNode path_node_;
  };

  class RowComparer {
  public:
    explicit RowComparer(const ObIArray<ObSortColumn>& sort_columns, int& ret) : sort_columns_(sort_columns), ret_(ret)
    {}
    ~RowComparer()
    {}
    bool operator()(const PumpNode& pump_node1, const PumpNode& pump_node2);

  private:
    const ObIArray<ObSortColumn>& sort_columns_;
    int& ret_;
  };

public:
  ObConnectByPumpBFS()
      : ObConnectByPumpBase(),
        pump_stack_(),
        path_stack_(),
        sort_stack_(),
        free_record_(),
        sort_columns_(NULL),
        need_sort_siblings_(false)
  {}
  ~ObConnectByPumpBFS()
  {
    free_memory();
  }
  int get_next_row(const ObNewRow*& pump_row, const ObNewRow*& ouptput_row);
  int append_row(const ObNewRow* right_row, const ObNewRow* joined_row);
  int add_root_row(const ObNewRow* root_row, const ObNewRow* output_row);
  int sort_sibling_rows();
  int init(const ObConnectByWithIndex& connect_by, common::ObExprCtx* expr_ctx);
  int set_connect_by_root_row(const ObNewRow* root_row);
  int get_parent_path(int64_t idx, ObString& p_path) const;
  int set_cur_node_path(int64_t idx, const ObString& path);
  void reset();
  void free_memory();
  void free_memory_for_rescan();

private:
  int push_back_row_to_stack(const ObNewRow& left_row, const ObNewRow& output_row);
  int free_record_rows();
  int add_path_stack(PathNode& path_node);
  int calc_path_node(const ObNewRow& left_row, PumpNode& pump_nodek6);
  int add_path_node(const ObNewRow& row, PumpNode& pump_node);
  int check_cycle_path(const ObNewRow& row);
  int free_path_stack();
  int free_pump_node_stack(ObIArray<PumpNode>& stack);

private:
  static const int64_t CONNECT_BY_MAX_NODE_NUM = (2L << 30) / sizeof(PumpNode);
  ObArray<PumpNode> pump_stack_;
  ObArray<PathNode> path_stack_;
  ObArray<PumpNode> sort_stack_;
  ObSEArray<const ObNewRow*, 2> free_record_;
  const ObIArray<ObSortColumn>* sort_columns_;
  bool need_sort_siblings_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_CONNECT_BY_UTILITY2_h_ */
