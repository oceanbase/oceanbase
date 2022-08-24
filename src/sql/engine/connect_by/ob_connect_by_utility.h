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

#ifndef __OB_SQL_CONNECT_BY_UTILITY_h_
#define __OB_SQL_CONNECT_BY_UTILITY_h_

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
//#include "sql/engine/ob_exec_context.h"
#include "common/row/ob_row.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObSortColumn;
enum ObConnectByPseudoColumn {
  LEVEL = 0,
  CONNECT_BY_ISCYCLE = 1,
  CONNECT_BY_ISLEAF = 2,
  CONNECT_BY_PSEUDO_COLUMN_CNT = 3
};

typedef common::ObFixedArray<int64_t, common::ObIAllocator> ConnectByRowDesc;
class ObConnectByPumpBase {
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
  ObConnectByPumpBase()
      : shallow_row_(),
        pump_row_desc_(NULL),
        pseudo_column_row_desc_(NULL),
        connect_by_prior_exprs_(NULL),
        expr_ctx_(NULL),
        connect_by_root_row_(NULL),
        allocator_(ObModIds::OB_CONNECT_BY_PUMP),
        is_inited_(false),
        cur_level_(1),
        is_output_level_(false),
        is_output_cycle_(false),
        is_output_leaf_(false),
        never_meet_cycle_(true),
        connect_by_path_count_(0)
  {}
  ~ObConnectByPumpBase()
  {}
  inline int64_t get_current_level() const
  {
    return cur_level_;
  }
  virtual int set_connect_by_root_row(const common::ObNewRow* root_row) = 0;

protected:
  int deep_copy_row(const common::ObNewRow& src_row, const common::ObNewRow*& dst_row);
  int alloc_shallow_row_cells(const ObPhyOperator& left_op);
  int alloc_shallow_row_projector(const ObPhyOperator& left_op);
  int construct_pump_row(const common::ObNewRow* row);
  int check_output_pseudo_columns();
  int check_pump_row_desc();

protected:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  // for init hash set for loop check
  static const int64_t CONNECT_BY_TREE_HEIGHT = 16;
  common::ObNewRow shallow_row_;
  const ConnectByRowDesc* pump_row_desc_;
  const ConnectByRowDesc* pseudo_column_row_desc_;
  // record all prior expressions in connect by conditions except prior const
  const common::ObDList<ObSqlExpression>* connect_by_prior_exprs_;
  common::ObExprCtx* expr_ctx_;
  const common::ObNewRow* connect_by_root_row_;
  MallocWrapper allocator_;
  bool is_inited_;
  // for assign node.level when append row, equal to level of left row + 1
  int64_t cur_level_;
  bool is_output_level_;
  bool is_output_cycle_;
  bool is_output_leaf_;
  bool never_meet_cycle_;
  int64_t connect_by_path_count_;
};

class ObConnectBy;
class ObConnectByPump : public ObConnectByPumpBase {
  friend ObConnectBy;

private:
  class ObHashColumn {
  public:
    ObHashColumn() : row_(NULL), expr_ctx_(NULL), hash_val_(0)
    {}
    ObHashColumn(const common::ObNewRow* row, common::ObExprCtx* expr_ctx)
        : row_(row), expr_ctx_(expr_ctx), hash_val_(0)
    {}

    ~ObHashColumn()
    {}

    int init(const common::ObNewRow* row, common::ObExprCtx* expr_ctx, const uint64_t hash_val = 0)
    {
      row_ = row;
      expr_ctx_ = expr_ctx;
      hash_val_ = hash_val;
      return common::OB_SUCCESS;
    }

    uint64_t hash() const
    {
      if (hash_val_ == 0) {
        hash_val_ = inner_hash();
      }
      return hash_val_;
    }

    uint64_t inner_hash() const;

    bool operator==(const ObHashColumn& other) const;

    TO_STRING_KV(K_(row), K_(hash_val));

  public:
    const common::ObNewRow* row_;
    common::ObExprCtx* expr_ctx_;
    mutable uint64_t hash_val_;
  };

  struct HashTableCell {
    HashTableCell() = default;
    const ObChunkRowStore::StoredRow* stored_row_;
    HashTableCell* next_tuple_;

    TO_STRING_KV(K_(stored_row), K(static_cast<void*>(next_tuple_)));
  };

  struct ConnectByHashTable {
    ConnectByHashTable()
        : buckets_(nullptr),
          all_cells_(nullptr),
          collision_cnts_(nullptr),
          nbuckets_(0),
          row_count_(0),
          inited_(false),
          ht_alloc_(nullptr)
    {}
    void reset()
    {
      if (nullptr != buckets_) {
        buckets_->reset();
      }
      if (nullptr != collision_cnts_) {
        collision_cnts_->reset();
      }
      if (nullptr != all_cells_) {
        all_cells_->reset();
      }
      nbuckets_ = 0;
    }
    int init(ObIAllocator& alloc);
    void free(ObIAllocator* alloc)
    {
      reset();
      if (OB_NOT_NULL(buckets_)) {
        buckets_->destroy();
        if (!OB_ISNULL(alloc)) {
          alloc->free(buckets_);
        }
        buckets_ = nullptr;
      }
      if (OB_NOT_NULL(collision_cnts_)) {
        collision_cnts_->destroy();
        if (!OB_ISNULL(alloc)) {
          alloc->free(collision_cnts_);
        }
        collision_cnts_ = nullptr;
      }
      if (OB_NOT_NULL(all_cells_)) {
        all_cells_->destroy();
        if (!OB_ISNULL(alloc)) {
          alloc->free(all_cells_);
        }
        all_cells_ = nullptr;
      }
      if (OB_NOT_NULL(ht_alloc_)) {
        ht_alloc_->reset();
        ht_alloc_->~ModulePageAllocator();
        if (!OB_ISNULL(alloc)) {
          alloc->free(ht_alloc_);
        }
        ht_alloc_ = nullptr;
      }
      inited_ = false;
    }
    void inc_collision(int64_t bucket_id)
    {
      if (255 > collision_cnts_->at(bucket_id)) {
        ++collision_cnts_->at(bucket_id);
      }
    }
    void get_collision_info(uint8_t& min_cnt, uint8_t& max_cnt, int64_t& total_cnt, int64_t& used_bucket_cnt)
    {
      min_cnt = 255;
      max_cnt = 0;
      total_cnt = 0;
      used_bucket_cnt = 0;
      if (collision_cnts_->count() != nbuckets_) {
        SQL_ENG_LOG(
            WARN, "unexpected: collision_cnts_->count() != nbuckets_", K(collision_cnts_->count()), K(nbuckets_));
      } else {
        for (int64_t i = 0; i < nbuckets_; ++i) {
          int64_t cnt = collision_cnts_->at(i);
          if (0 < cnt) {
            ++used_bucket_cnt;
            total_cnt += cnt;
            if (min_cnt > cnt) {
              min_cnt = cnt;
            }
            if (max_cnt < cnt) {
              max_cnt = cnt;
            }
          }
        }
      }
      if (0 == used_bucket_cnt) {
        min_cnt = 0;
      }
    }
    void reverse_bucket_cells();
    using BucketArray = common::ObSegmentArray<HashTableCell*, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    BucketArray* buckets_;
    using AllCellArray = common::ObSegmentArray<HashTableCell, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    AllCellArray* all_cells_;
    using CollisionCntArray = common::ObSegmentArray<uint8_t, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    CollisionCntArray* collision_cnts_;  // not used yet
    int64_t nbuckets_;
    int64_t row_count_;
    bool inited_;
    ModulePageAllocator* ht_alloc_;
  };

  struct RowFetcher {
    RowFetcher() : iterator_(nullptr), tuple_(nullptr), use_hash_(false)
    {}
    ObChunkRowStore::Iterator* iterator_;
    HashTableCell* tuple_;
    bool use_hash_;  // tuple is valid if use_hash_
    int init(
        ObConnectByPump& connect_by_pump, const ObArray<ObSqlExpression*>& hash_probe_exprs, const ObNewRow* left_row);
    int get_next_row(ObNewRow*& row);
  };

  class PumpNode {
  public:
    PumpNode()
        : pump_row_(NULL),
          output_row_(NULL),
          prior_exprs_result_(NULL),
          right_row_(NULL),
          is_cycle_(false),
          is_leaf_(false),
          level_(0),
          sys_path_length_(),
          row_fetcher_(),
          first_child_(NULL)
    {}
    ~PumpNode()
    {}

    TO_STRING_KV(KPC(pump_row_), KPC(output_row_), K(is_cycle_), K(is_leaf_), K(level_));
    // left_row or right shallow row
    const ObNewRow* pump_row_;
    const ObNewRow* output_row_;
    const ObNewRow* prior_exprs_result_;
    const ObNewRow* right_row_;
    bool is_cycle_;
    bool is_leaf_;
    uint64_t level_;
    ObSEArray<uint64_t, 8> sys_path_length_;
    RowFetcher row_fetcher_;
    // When output contains connect_by_isleaf, need to search for first child.
    // If there is no rownum in connect by conditions,
    // we can output first_child_ at first when start to output children nodes.
    const ObNewRow* first_child_;
  };

  typedef common::hash::ObHashSet<ObHashColumn, common::hash::NoPthreadDefendMode> RowMap;

public:
  ObConnectByPump()
      : ObConnectByPumpBase(),
        prior_exprs_result_row_(),
        pump_stack_(),
        row_store_(),
        hash_filter_rows_(),
        connect_by_(NULL),
        sys_path_buffer_(),
        is_nocycle_(false),
        row_store_constructed_(false),
        hash_table_(),
        use_hash_(false)
  {}
  ~ObConnectByPump()
  {
    free_memory();
    if (hash_filter_rows_.created()) {
      hash_filter_rows_.destroy();
    }
  }
  int join_right_table(PumpNode& node, bool& matched, ObPhyOperator::ObPhyOperatorCtx* phy_ctx);
  int get_next_row(ObPhyOperator::ObPhyOperatorCtx* phy_ctx);
  int add_root_row(const ObNewRow* root_row, const ObNewRow& mock_right_row, const ObNewRow* output_row);
  int init(const ObConnectBy& connect_by, common::ObExprCtx* expr_ctx);
  int set_connect_by_root_row(const ObNewRow* root_row)
  {
    return deep_copy_row(*root_row, connect_by_root_row_);
  }
  void reset();
  void close(ObIAllocator* allocator);
  void free_memory();
  int push_back_store_row(const ObNewRow& row)
  {
    return row_store_.add_row(row);
  }
  int get_top_pump_node(PumpNode*& node);
  int get_sys_path(uint64_t sys_connect_by_path_id, ObString& parent_path);
  int concat_sys_path(uint64_t sys_connect_by_path_id, const ObString& cur_path);
  int build_hash_table(ObIAllocator& alloc);
  int calc_hash_value(const ObArray<ObSqlExpression*>& hash_exprs, const ObNewRow& row, ObExprCtx& expr_ctx,
      uint64_t& hash_value) const;

private:
  int alloc_prior_row_cells(uint64_t row_count);
  int alloc_iter(PumpNode& node);
  int push_back_node_to_stack(PumpNode& node);
  int calc_prior_and_check_cycle(PumpNode& node, bool set_refactored);
  int check_cycle(const ObNewRow& row, bool set_refactored);
  int check_child_cycle(const ObNewRow& right_row, PumpNode& node);
  int free_pump_node(PumpNode& node);
  int free_pump_node_stack(ObIArray<PumpNode>& stack);
  void set_row_store_constructed()
  {
    row_store_constructed_ = true;
  }
  bool get_row_store_constructed()
  {
    return row_store_constructed_;
  }

private:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  static const int64_t CONNECT_BY_MAX_NODE_NUM = (2L << 30) / sizeof(PumpNode);
  // record results of connect by prior expressions
  ObNewRow prior_exprs_result_row_;
  ObArray<PumpNode> pump_stack_;
  ObChunkRowStore row_store_;
  RowMap hash_filter_rows_;
  const ObConnectBy* connect_by_;
  ObSEArray<common::ObString, 16> sys_path_buffer_;
  bool is_nocycle_;
  bool row_store_constructed_;
  ConnectByHashTable hash_table_;
  bool use_hash_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_CONNECT_BY_UTILITY_h_ */
