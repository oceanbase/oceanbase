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

#ifndef SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_BFS_H_
#define SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_BFS_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/connect_by/ob_cnnt_by_pump.h"

namespace oceanbase
{
namespace sql
{

class ObNLConnectByWithIndexOp;
class ObConnectByOpBFSPump : public ObConnectByOpPumpBase
{
  friend ObNLConnectByWithIndexOp;
private:
  class PathNode
  {
  public:
  PathNode(): prior_exprs_result_(NULL), paths_(), level_(0) {}
    ~PathNode() { }
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
    const ObChunkDatumStore::StoredRow *prior_exprs_result_;
    // 一棵树高为16已经很大很大了，应该够用
    common::ObSEArray<common::ObString, 16> paths_;
    int64_t level_;
  };

  class PumpNode
  {
  public:
    PumpNode(): pump_row_(NULL),output_row_(NULL), path_node_(), is_cycle_(false) { }
    ~PumpNode() { }
    void reset()
    {
      pump_row_ = NULL;
      output_row_ = NULL;
      path_node_.reset();
    }
    TO_STRING_KV(KPC(pump_row_), KPC(output_row_), K(path_node_));
    const ObChunkDatumStore::StoredRow *pump_row_;
    const ObChunkDatumStore::StoredRow *output_row_;
    PathNode path_node_;
    bool is_cycle_;
  };

  class RowComparer
  {
  public:
    explicit RowComparer(
      const common::ObIArray<ObSortFieldCollation> *sort_collations,
      const common::ObIArray<ObSortCmpFunc> *sort_cmp_funs,
      int &ret)
    : sort_collations_(sort_collations),
      sort_cmp_funs_(sort_cmp_funs),
      ret_(ret)
    {
    }
    ~RowComparer() {}
    bool operator()(const PumpNode &pump_node1, const PumpNode &pump_node2);
  private:
    const common::ObIArray<ObSortFieldCollation> *sort_collations_;
    const common::ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
    int &ret_;
  };

public:
  ObConnectByOpBFSPump()
    : ObConnectByOpPumpBase(),
      pump_stack_(),
      path_stack_(),
      sort_stack_(),
      free_record_(),
      connect_by_(nullptr),
      cmp_funcs_(nullptr),
      sort_collations_(NULL),
      sort_cmp_funs_(nullptr),
      pump_row_(nullptr),
      output_row_(nullptr),
      is_nocycle_(false)
      {}
  ~ObConnectByOpBFSPump() { free_memory(); }
  int get_next_row(const ObChunkDatumStore::StoredRow *&pump_row,
      const ObChunkDatumStore::StoredRow *&ouptput_row);
  int append_row(const common::ObIArray<ObExpr*> &right_row, const common::ObIArray<ObExpr*> &joined_row);
  int add_root_row(const common::ObIArray<ObExpr*> &root_row, const common::ObIArray<ObExpr*> &output_row);
  int sort_sibling_rows();
  int init(ObNLConnectByWithIndexSpec &connect_by, ObNLConnectByWithIndexOp &connect_by_op,
            ObEvalCtx &eval_ctx);
  inline int64_t get_current_level() const { return cur_level_; }
  bool is_root_node();
  int get_parent_path(int64_t idx, common::ObString &p_path) const;
  int set_cur_node_path(int64_t idx, const common::ObString &path);
  void reset();
  void free_memory();
  void free_memory_for_rescan();
private:
  int push_back_row_to_stack(const common::ObIArray<ObExpr*> &root_exprs,
      const common::ObIArray<ObExpr*> &cur_output_exprs);
  int free_record_rows();
  int add_path_stack(PathNode &path_node);
  int calc_path_node(PumpNode &pump_nodek6);
  int add_path_node(PumpNode &pump_node);
  int check_cycle_path();
  int free_path_stack();
  int free_pump_node_stack(ObIArray<PumpNode> &stack);
  void set_allocator(common::ObIAllocator &alloc) { allocator_.set_allocator(alloc); }
private:
  common::ObArray<PumpNode> pump_stack_;
  common::ObArray<PathNode> path_stack_;
  common::ObArray<PumpNode> sort_stack_;
  common::ObSEArray<const ObChunkDatumStore::StoredRow *, 2> free_record_;
  //记录connect by后除去prior 常量表达式(如prior 0)的所有表达式
  ObNLConnectByWithIndexOp *connect_by_;
  const common::ObIArray<ObCmpFunc> *cmp_funcs_;
  const common::ObIArray<ObSortFieldCollation> *sort_collations_;
  const common::ObIArray<ObSortCmpFunc> *sort_cmp_funs_;
  const ObChunkDatumStore::StoredRow *pump_row_;
  const ObChunkDatumStore::StoredRow *output_row_;
  bool is_nocycle_;
};

}//sql
}//oceanbase
#endif /* SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_BFS_H_ */
