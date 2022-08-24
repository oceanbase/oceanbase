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

#ifndef SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_H_
#define SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_H_

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObConnectByOpPumpBase {
private:
  class MallocWrapper : public common::ObMalloc {
  public:
    explicit MallocWrapper(const char* label) : allocator_(label), alloc_cnt_(0)
    {}
    virtual ~MallocWrapper()
    {
      if (OB_UNLIKELY(alloc_cnt_ != 0)) {
        OB_LOG(WARN, "memory expanded in cby execution.", K(alloc_cnt_));
      }
    }
    void* alloc(const int64_t sz)
    {
      void* mem = allocator_.alloc(sz);
      alloc_cnt_ = nullptr == mem ? alloc_cnt_ : alloc_cnt_ + 1;
      return mem;
    }
    void* alloc(const int64_t sz, const common::ObMemAttr& attr)
    {
      void* mem = allocator_.alloc(sz, attr);
      alloc_cnt_ = nullptr == mem ? alloc_cnt_ : alloc_cnt_ + 1;
      return mem;
    }
    void free(void* ptr)
    {
      --alloc_cnt_;
      allocator_.free(ptr);
    }
    void set_tenant_id(int64_t tenant_id)
    {
      allocator_.set_tenant_id(tenant_id);
    }

  private:
    common::ObArenaAllocator allocator_;
    int64_t alloc_cnt_;
  };

public:
  ObConnectByOpPumpBase()
      :  // shallow_row_(),
         // pump_row_desc_(NULL),
         // pseudo_column_row_desc_(NULL),
        connect_by_prior_exprs_(NULL),
        left_prior_exprs_(NULL),
        right_prior_exprs_(NULL),
        eval_ctx_(NULL),
        //      connect_by_root_row_(NULL),
        allocator_(ObModIds::OB_CONNECT_BY_PUMP),
        is_inited_(false),
        cur_level_(1),
        never_meet_cycle_(true),
        connect_by_path_count_(0)
  {}
  ~ObConnectByOpPumpBase()
  {}
  inline int64_t get_current_level() const
  {
    return cur_level_;
  }
  //  virtual int set_connect_by_root_row(const common::ObNewRow *root_row) = 0;

protected:
  int deep_copy_row(const common::ObIArray<ObExpr*>& exprs, const ObChunkDatumStore::StoredRow*& dst_row);

protected:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  // for init hash set for loop check
  static const int64_t CONNECT_BY_TREE_HEIGHT = 16;
  // record all prior expressions in connect by conditions except prior const
  const common::ObIArray<ObExpr*>* connect_by_prior_exprs_;
  const common::ObIArray<ObExpr*>* left_prior_exprs_;
  const common::ObIArray<ObExpr*>* right_prior_exprs_;
  ObEvalCtx* eval_ctx_;
  MallocWrapper allocator_;
  bool is_inited_;
  // for assign node.level when append row, equal to level of left row + 1
  int64_t cur_level_;
  bool never_meet_cycle_;
  int64_t connect_by_path_count_;
};

class ObNLConnectByOp;
class ObConnectByOpPump : public ObConnectByOpPumpBase {
  friend ObNLConnectByOp;

private:
  class ObHashColumn {
  public:
    ObHashColumn() : row_(NULL), exprs_(NULL), hash_val_(0)
    {}
    ObHashColumn(const ObChunkDatumStore::StoredRow* row, const common::ObIArray<ObExpr*>* exprs)
        : row_(row), exprs_(exprs), hash_val_(0)
    {}

    ~ObHashColumn()
    {}

    uint64_t hash() const
    {
      if (hash_val_ == 0) {
        hash_val_ = inner_hash();
      }
      return hash_val_;
    }

    uint64_t inner_hash() const;

    bool operator==(const ObHashColumn& other) const;

  public:
    const ObChunkDatumStore::StoredRow* row_;
    const common::ObIArray<ObExpr*>* exprs_;
    mutable uint64_t hash_val_;
  };

  class PumpNode {
  public:
    PumpNode()
        : pump_row_(NULL),
          output_row_(NULL),
          prior_exprs_result_(NULL),
          is_cycle_(false),
          is_leaf_(false),
          level_(0),
          sys_path_length_(),
          iter_(nullptr),
          first_child_(NULL)
    {}
    ~PumpNode()
    {}

    TO_STRING_KV(K(is_cycle_), K(is_leaf_), K(level_));
    // left_row or right shallow row
    const ObChunkDatumStore::StoredRow* pump_row_;
    const ObChunkDatumStore::StoredRow* output_row_;
    const ObChunkDatumStore::StoredRow* prior_exprs_result_;
    bool is_cycle_;
    bool is_leaf_;
    uint64_t level_;
    ObSEArray<uint64_t, 8> sys_path_length_;
    ObChunkDatumStore::Iterator* iter_;
    // When output contains connect_by_isleaf, need to search for first child.
    // If there is no rownum in connect by conditions,
    // we can output first_child_ at first when start to output children nodes.
    const ObChunkDatumStore::StoredRow* first_child_;
  };
  typedef common::hash::ObHashSet<ObHashColumn, common::hash::NoPthreadDefendMode> RowMap;

public:
  ObConnectByOpPump()
      : ObConnectByOpPumpBase(),
        //      prior_exprs_result_row_(),
        pump_stack_(),
        datum_store_(),
        cur_output_exprs_(NULL),
        hash_filter_rows_(),
        connect_by_(NULL),
        sys_path_buffer_(),
        is_nocycle_(false),
        datum_store_constructed_(false)
  {}
  ~ObConnectByOpPump()
  {
    free_memory();
    if (hash_filter_rows_.created()) {
      hash_filter_rows_.destroy();
    }
  }
  int join_right_table(PumpNode& node, bool& matched);
  int get_next_row();
  int add_root_row();
  int init(const ObNLConnectBySpec& connect_by, ObNLConnectByOp& connect_by_op, ObEvalCtx& eval_ctx);

  void reset();
  void free_memory();
  int push_back_store_row();
  int get_top_pump_node(PumpNode*& node);
  int get_sys_path(uint64_t sys_connect_by_path_id, ObString& parent_path);
  int concat_sys_path(uint64_t sys_connect_by_path_id, const ObString& cur_path);

private:
  //  int alloc_prior_row_cells(uint64_t row_count);
  int push_back_node_to_stack(PumpNode& node);
  int calc_prior_and_check_cycle(PumpNode& node, bool set_refactored, PumpNode* left_node);
  int check_cycle(const ObChunkDatumStore::StoredRow* row, bool set_refactored);
  int check_child_cycle(PumpNode& node, PumpNode* left_node);
  int free_pump_node(PumpNode& node);
  int alloc_iter(PumpNode& node);
  int free_pump_node_stack(ObIArray<PumpNode>& stack);
  void set_row_store_constructed()
  {
    datum_store_constructed_ = true;
  }
  bool get_row_store_constructed()
  {
    return datum_store_constructed_;
  }

private:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  static const int64_t CONNECT_BY_MAX_NODE_NUM = (2L << 30) / sizeof(PumpNode);
  ObArray<PumpNode> pump_stack_;
  ObChunkDatumStore datum_store_;
  const ObIArray<ObExpr*>* cur_output_exprs_;
  RowMap hash_filter_rows_;
  ObNLConnectByOp* connect_by_;
  ObSEArray<common::ObString, 16> sys_path_buffer_;
  bool is_nocycle_;
  bool datum_store_constructed_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_H_ */
