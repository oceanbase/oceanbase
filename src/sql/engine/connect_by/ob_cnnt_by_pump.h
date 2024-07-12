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

namespace oceanbase
{
namespace sql
{
class ObConnectByOpPumpBase
{
private:
  class MallocWrapper: public common::ObMalloc
  {
  public:
    explicit MallocWrapper(): allocator_(NULL), alloc_cnt_(0) {}
    virtual ~MallocWrapper()
    {
      if (OB_UNLIKELY(alloc_cnt_ != 0)) {
        OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "memory expanded in cby execution.", K(alloc_cnt_));
      }
    }
    void *alloc(const int64_t sz)
    {
      void *mem = allocator_->alloc(sz);
      alloc_cnt_ = nullptr == mem ? alloc_cnt_ : alloc_cnt_ + 1;
      return mem;
    }
    void *alloc(const int64_t sz, const common::ObMemAttr &attr)
    {
      void *mem = allocator_->alloc(sz, attr);
      alloc_cnt_ = nullptr == mem ? alloc_cnt_ : alloc_cnt_ + 1;
      return mem;
    }
    void free(void *ptr)
    {
      --alloc_cnt_;
      allocator_->free(ptr);
    }
    void set_allocator(common::ObIAllocator &alloc) {
      allocator_ = &alloc;
    }
  private:
    common::ObIAllocator *allocator_;
    int64_t alloc_cnt_;
  };

public:
  ObConnectByOpPumpBase()
      ://shallow_row_(),
      //pump_row_desc_(NULL),
      //pseudo_column_row_desc_(NULL),
      connect_by_prior_exprs_(NULL),
      left_prior_exprs_(NULL),
      right_prior_exprs_(NULL),
      eval_ctx_(NULL),
//      connect_by_root_row_(NULL),
      allocator_(),
      is_inited_(false),
      cur_level_(1),
      never_meet_cycle_(true),
      connect_by_path_count_(0)
      {}
  ~ObConnectByOpPumpBase() {
  }
  inline int64_t get_current_level() const { return cur_level_; }
//  virtual int set_connect_by_root_row(const common::ObNewRow *root_row) = 0;

protected:
  int deep_copy_row(const common::ObIArray<ObExpr*> &exprs,
    const ObChunkDatumStore::StoredRow *&dst_row);

protected:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  //用于初始化检测环的hash_set
  static const int64_t CONNECT_BY_TREE_HEIGHT = 16;
//  common::ObNewRow shallow_row_;//用来初步构建pump的内容, 为deep copy做准备
//  const ConnectByRowDesc *pseudo_column_row_desc_;
  //记录connect by后除去prior 常量表达式(如prior 0)的所有表达式
  const common::ObIArray<ObExpr*> *connect_by_prior_exprs_;
  const common::ObIArray<ObExpr*> *left_prior_exprs_;
  const common::ObIArray<ObExpr*> *right_prior_exprs_;
  ObEvalCtx *eval_ctx_;
//  const ObChunkDatumStore::StoredRow *connect_by_root_row_;//用来记录当前root的root_row
  MallocWrapper allocator_;
  bool is_inited_;
  int64_t cur_level_;//记录append_row时应该使用的level，为left row' level + 1
  bool never_meet_cycle_;
  int64_t connect_by_path_count_;
};

class ObNLConnectByOp;
class ObConnectByOpPump : public ObConnectByOpPumpBase
{
  friend ObNLConnectByOp;
private:
  class ObHashColumn
  {
  public:
    ObHashColumn()
      : row_(NULL),
        exprs_(NULL),
        hash_val_(0)
    {
    }
    ObHashColumn(const ObChunkDatumStore::StoredRow *row,
                const common::ObIArray<ObExpr *> *exprs)
      : row_(row),
        exprs_(exprs),
        hash_val_(0)
    {
    }

    ~ObHashColumn()
    {}

    int hash(uint64_t &hash_val) const
    {
      int ret = OB_SUCCESS;
      if (hash_val_ == 0) {
        if (OB_FAIL(inner_hash(hash_val_))) {
          LOG_WARN("fail to do hash", K(ret));
        }
      }
      hash_val = hash_val_;
      return ret;
    }

    int inner_hash(uint64_t &result) const;

    bool operator ==(const ObHashColumn &other) const;

  public:
    const ObChunkDatumStore::StoredRow *row_;
    const common::ObIArray<ObExpr *> *exprs_;
    mutable uint64_t hash_val_;
  };
	struct HashTableCell
  {
    HashTableCell() = default;
    const ObChunkDatumStore::StoredRow *stored_row_;
    HashTableCell *next_tuple_;
    TO_STRING_KV(K_(stored_row), K(static_cast<void*>(next_tuple_)));
  };
	struct RowFetcher
  {
    RowFetcher() :
      iterator_(nullptr), tuple_(nullptr), use_hash_(false)
    { }
    ObChunkDatumStore::Iterator *iterator_;
    HashTableCell *tuple_;
    bool use_hash_;     // tuple is valid if use_hash_
    int init(ObConnectByOpPump &connect_by_pump, const ExprFixedArray &hash_probe_exprs);
    int get_next_row(const ObChunkDatumStore::StoredRow *&row);
    int get_next_row(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx);
  };

  class PumpNode
  {
  public:
    PumpNode(): pump_row_(NULL), output_row_(NULL), prior_exprs_result_(NULL),
                is_cycle_(false), is_leaf_(false), level_(0), sys_path_length_(),
                row_fetcher_(), first_child_(NULL)
                { }
    ~PumpNode() { }

    TO_STRING_KV(K(is_cycle_), K(is_leaf_), K(level_));
    //left_row or right shallow row
    const ObChunkDatumStore::StoredRow *pump_row_;
    const ObChunkDatumStore::StoredRow *output_row_;
    const ObChunkDatumStore::StoredRow *prior_exprs_result_;
    bool is_cycle_;
    bool is_leaf_;
    uint64_t level_;
    ObSEArray<uint64_t, 8> sys_path_length_;
    RowFetcher row_fetcher_;
    //在输出包含isleaf伪列时，为了产生当前行的isleaf，会查找它的第一个子节点
    //如果connect by不含rownum，那么可以保存这个子节点，开始查找子节点时直接返回这个子节点
    const ObChunkDatumStore::StoredRow *first_child_;
  };
  struct ConnectByHashTable
  {
    ConnectByHashTable()
        : buckets_(nullptr),
          all_cells_(nullptr),
          nbuckets_(0),
          row_count_(0),
          inited_(false),
          ht_alloc_(nullptr)
    {
    }
    // The state after reuse should be same as that after init
    void reuse()
    {
      if (nullptr != buckets_) {
        buckets_->reset();
      }
      if (nullptr != all_cells_) {
        all_cells_->reset();
      }
      nbuckets_ = 0;
      row_count_ = 0;
    }
    int init(ObIAllocator &alloc);
    void free(ObIAllocator *alloc)
    {
      reuse();
      if (OB_NOT_NULL(buckets_)) {
        buckets_->destroy();
        if (!OB_ISNULL(alloc)) {
          alloc->free(buckets_);
        }
        buckets_ = nullptr;
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
    void reverse_bucket_cells();
    using BucketArray = common::ObSegmentArray<HashTableCell*,
                    OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    BucketArray *buckets_;
    using AllCellArray = common::ObSegmentArray<HashTableCell,
                    OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    AllCellArray *all_cells_;
    int64_t nbuckets_;
    int64_t row_count_;
    bool inited_;
    ModulePageAllocator *ht_alloc_;
  };
  typedef common::hash::ObHashSet<ObHashColumn, common::hash::NoPthreadDefendMode> RowMap;
public:
  ObConnectByOpPump()
      : ObConnectByOpPumpBase(),
//      prior_exprs_result_row_(),
      pump_stack_() ,
      datum_store_(ObModIds::OB_CONNECT_BY_PUMP),
      cur_output_exprs_(NULL),
      hash_filter_rows_(),
      connect_by_(NULL),
      sys_path_buffer_(),
      is_nocycle_(false),
      datum_store_constructed_(false),
      hash_table_(),
      use_hash_(false)
      {}
  ~ObConnectByOpPump() {
    free_memory();
    if (hash_filter_rows_.created()) {
      hash_filter_rows_.destroy();
    }
  }
  int join_right_table(PumpNode &node, bool &matched);
  int get_next_row();
  int add_root_row();
  int init(const ObNLConnectBySpec &connect_by, ObNLConnectByOp &connect_by_op,
    ObEvalCtx &eval_ctx);

  void reset();
  void close(ObIAllocator *allocator);
  void free_memory();
  int push_back_store_row();
  int get_top_pump_node(PumpNode *&node);
  int get_sys_path(uint64_t sys_connect_by_path_id, ObString &parent_path);
  int concat_sys_path(uint64_t sys_connect_by_path_id, const ObString &cur_path);
  int calc_hash_value(const ExprFixedArray &exprs, uint64_t &hash_value);
  int build_hash_table(ObIAllocator &alloc);

private:
//  int alloc_prior_row_cells(uint64_t row_count);
  int push_back_node_to_stack(PumpNode &node, bool &is_push);
  int calc_prior_and_check_cycle(PumpNode &node, bool set_refactored, PumpNode *left_node);
  int check_cycle(const ObChunkDatumStore::StoredRow *row, bool set_refactored);
  int check_child_cycle(PumpNode &node, PumpNode *left_node);
  void free_pump_node(PumpNode &node);
  int alloc_iter(PumpNode &node);
  int free_pump_node_stack(ObSegmentArray<PumpNode> &stack);
  void set_row_store_constructed() { datum_store_constructed_ = true; }
  bool get_row_store_constructed() { return datum_store_constructed_; }
  void set_allocator(common::ObIAllocator &alloc) { allocator_.set_allocator(alloc); }

private:
  static const int64_t SYS_PATH_BUFFER_INIT_SIZE = 128;
  static const int64_t CONNECT_BY_MAX_NODE_NUM = (2L << 30) / sizeof(PumpNode);
  //用于初始化检测环的hash_set
  static const int64_t CONNECT_BY_TREE_HEIGHT = 16;
//  ObNewRow prior_exprs_result_row_;//用来存放计算connect by prior表达式的结果
  ObSegmentArray<PumpNode> pump_stack_;
  ObChunkDatumStore datum_store_;
  const ObIArray<ObExpr *> *cur_output_exprs_;
  RowMap hash_filter_rows_;
  ObNLConnectByOp *connect_by_;
  ObSEArray<common::ObString, 16> sys_path_buffer_;
  bool is_nocycle_;
  bool datum_store_constructed_;
  ConnectByHashTable hash_table_;
  bool use_hash_;
};

}//sql
}//oceanbase
#endif /* SRC_SQL_ENGINE_OB_CONNECT_BY_OP_PUMP_H_ */
