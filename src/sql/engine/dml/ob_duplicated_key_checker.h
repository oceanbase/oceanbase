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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_DUPLICATED_KEY_CHECKER_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_DUPLICATED_KEY_CHECKER_H_
#include "sql/executor/ob_mini_task_executor.h"
#include "storage/ob_i_partition_storage.h"
#include "sql/engine/ob_operator.h"
namespace oceanbase {
namespace sql {
typedef common::ObRowStore DupKeyCheckerRowStore;
typedef ObChunkDatumStore DupKeyCheckerDatumRowStore;
typedef common::ObRowStore::Iterator DupKeyCheckerRowIter;
typedef ObChunkDatumStore::Iterator DupKeyCheckerDatumRowIter;
class ObIRowkeyIterator {
public:
  virtual ~ObIRowkeyIterator()
  {}
  virtual int get_next_conflict_row(common::ObNewRow& row) = 0;
  virtual int get_next_conflict_row() = 0;
  virtual const ObExprPtrIArray* get_output() = 0;
};

struct ObUniqueIndexScanInfo {
  ObUniqueIndexScanInfo()
      : table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        part_cnt_(0),
        index_location_(NULL),
        calc_index_part_id_expr_(NULL),
        conflict_column_cnt_(0),
        index_conflict_exprs_(),
        index_fetcher_op_(NULL),
        index_fetcher_spec_(NULL)
  {}
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    index_tid_ = common::OB_INVALID_ID;
    part_cnt_ = 0;
    index_location_ = NULL;
    calc_index_part_id_expr_ = NULL;
    calc_exprs_.reset();
    conflict_column_cnt_ = 0;
    index_conflict_exprs_.reset();
    se_index_conflict_exprs_.reset();
    index_fetcher_op_ = NULL;
    index_fetcher_spec_ = NULL;
  }
  TO_STRING_KV(K_(table_id), K_(index_tid), K_(part_cnt), KPC_(index_location), K_(conflict_column_cnt),
      K_(index_conflict_exprs), KPC_(index_fetcher_op));

  uint64_t table_id_;
  uint64_t index_tid_;               // for basic table,table_id_ is same with index_tid_
  int64_t part_cnt_;                 // partition_cnt_ in schema,used to build partition key
  ObTableLocation* index_location_;  // calculate index partition
  ObExpr* calc_index_part_id_expr_;
  ExprFixedArray calc_exprs_;  // Before calculating calc_index_part_id_expr_, clear the evalated_ mark
  // There may be conflicting columns. For the basic table, rowkey+local unique index column
  int64_t conflict_column_cnt_;
  common::ObDList<ObSqlExpression> index_conflict_exprs_;  // projector conflict row
  ExprFixedArray se_index_conflict_exprs_;                 // projector conflict row
  ObPhyOperator* index_fetcher_op_;                        // subplan root for index check
  ObOpSpec* index_fetcher_spec_;                           // subplan root for index check
};

struct ObPhyUniqueConstraintInfo {
  TO_STRING_KV(K_(constraint_name), K_(constraint_columns), K_(se_constraint_columns), K_(calc_exprs));
  common::ObString constraint_name_;
  common::ObDList<ObSqlExpression> constraint_columns_;
  // Record expressions based on table column
  ExprFixedArray se_constraint_columns_;
  ExprFixedArray calc_exprs_;
};

struct ObConstraintKey {
  ObConstraintKey()
      : duplicated_row_(NULL),
        datums_(NULL),
        eval_ctx_(NULL),
        constraint_column_depend_exprs_(NULL),
        constraint_info_(NULL),
        is_primary_index_(false)
  {}
  int init_constraint_row(ObIAllocator& alloc);
  uint64_t hash() const;
  bool operator==(const ObConstraintKey& other) const;
  TO_STRING_KV(KP(datums_), KPC_(duplicated_row), KPC_(constraint_info), K_(is_primary_index));
  const common::ObNewRow* duplicated_row_;
  // Call init_constraint_row to initialize, and perform other non-set operations on the map
  // No need to initialize this value, when performing equivalent/hash calculation,
  // it will be calculated through constraint expr
  // In this way, when performing equivalent comparisons,
  // you can compare the result of the constraint expr calculation with the calculated datums_
  // to avoid the situation where the same constraint exprs memory
  // needs to store the data calculated twice for equivalent calculation;
  ObDatum* datums_;
  ObEvalCtx* eval_ctx_;
  // Here you need the user to display the settings,
  // mainly to let the upper user know, the hash value calculation of the key behind, etc.,
  // It relies on these columns, so users don't use them indiscriminately
  const ObExprPtrIArray* constraint_column_depend_exprs_;
  const ObPhyUniqueConstraintInfo* constraint_info_;
  bool is_primary_index_;
};

struct ObConstraintValue {
  ObConstraintValue() : baseline_row_(NULL), current_row_(NULL), baseline_datum_row_(NULL), current_datum_row_(NULL)
  {}
  // During check_duplicate_rowkey, the conflicting rows will be deduplicated (add_var_to_array_no_dup)
  // Need to use this operator
  bool operator==(const ObConstraintValue& other) const;
  TO_STRING_KV(KPC_(baseline_row), KPC_(current_row), KP(baseline_datum_row_), KP(current_datum_row_));
  const common::ObNewRow* baseline_row_;
  const common::ObNewRow* current_row_;
  const ObChunkDatumStore::StoredRow* baseline_datum_row_;
  const ObChunkDatumStore::StoredRow* current_datum_row_;
};

typedef common::hash::ObHashMap<ObConstraintKey, ObConstraintValue, common::hash::NoPthreadDefendMode>
    ObUniqueConstraintCtx;
class ObDupKeyCheckerCtx {
public:
  friend class ObDuplicatedKeyChecker;
  static const int64_t ROW_BATCH_SIZE = 512;
  struct PartRowStore {
    PartRowStore() : partition_id_(0), row_store_(NULL), datum_store_(NULL)
    {}
    TO_STRING_KV(K_(partition_id));
    int64_t partition_id_;
    common::ObRowStore* row_store_;
    ObChunkDatumStore* datum_store_;
  };

public:
  ObDupKeyCheckerCtx(common::ObIAllocator& allocator, common::ObExprCtx* expr_ctx,
      ObDMLMiniTaskExecutor& index_executor, DupKeyCheckerRowStore* checker_row_store, ObEvalCtx* eval_ctx = NULL,
      DupKeyCheckerDatumRowStore* checker_datum_row_store = NULL);
  int init(int64_t constraint_cnt, bool has_gui);
  void destroy();
  int get_unique_index_part_info(const ObUniqueIndexScanInfo& index_info, ObExecContext& ctx,
      ObIRowkeyIterator& rowkey_iter, common::ObIArray<PartRowStore>& part_row_store, bool is_static_engine = false);
  int get_or_create_scan_task_info(common::ObAddr& runner_server, ObTaskInfo*& task_info, bool is_static_engine);
  void set_mini_job(const ObPhysicalPlan* phy_plan, const ObPhyOperator* plan_root, const ObPhyOperator* extend_root)
  {
    mini_job_.set_phy_plan(phy_plan);
    mini_job_.set_root_op(plan_root);
    mini_job_.set_extend_op(extend_root);
    if (OB_NOT_NULL(plan_root)) {
      ob_job_id_.set_job_id(plan_root->get_id());
    }
  }
  void set_mini_job(const ObPhysicalPlan* phy_plan, const ObOpSpec* plan_root, const ObOpSpec* extend_root)
  {
    mini_job_.set_phy_plan(phy_plan);
    mini_job_.set_root_spec(plan_root);
    mini_job_.set_extend_spec(extend_root);
    if (OB_NOT_NULL(plan_root)) {
      ob_job_id_.set_job_id(plan_root->id_);
    }
  }

  // Used to get all the rowkey information corresponding to the unique index,
  // As an optimization, when obtaining the main table and local unique index information,
  // the entire row information of the conflicting row will also be brought back
  // Avoid double lookup
  int fetch_unique_index_rowkey(ObExecContext& ctx, common::ObRowStore::Iterator& row_iter);
  int fetch_unique_index_rowkey(
      ObExecContext& ctx, const ObExprPtrIArray*& iter_output, DupKeyCheckerDatumRowIter& row_iter);
  int fetch_gui_lookup_data(
      ObExecContext& ctx, const ObExprPtrIArray*& iter_output, ObChunkDatumStore::Iterator& datum_iter);
  int fetch_gui_lookup_data(ObExecContext& ctx, common::ObRowStore::Iterator& row_iter);
  common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }
  common::ObExprCtx* get_expr_ctx()
  {
    return expr_ctx_;
  }
  const common::ObScanner& get_primary_rowkey_scanner()
  {
    return unique_rowkey_result_.get_task_result();
  }
  void reset_task_info();
  int check_rowkey_exists(const common::ObNewRow& rowkey, bool& is_exists);
  int check_rowkey_exists(const ObExprPtrIArray& row, bool& is_exists);
  int get_checker_store_begin_iter(DupKeyCheckerRowIter& row_iter);
  int get_checker_store_begin_iter(DupKeyCheckerDatumRowIter& datum_iter);
  bool update_incremental_row() const
  {
    return update_incremental_row_;
  }
  DupKeyCheckerDatumRowStore* get_checker_datum_store()
  {
    return checker_datum_store_;
  }
  template <class ShuffleOp>
  int shuffle_final_data(ObExecContext& ctx, const ObExprPtrIArray& output_exprs, ShuffleOp& shuffle_op);

private:
  int create_mini_task_info(ObTaskInfo*& task_info);

private:
  common::ObIAllocator& allocator_;
  common::ObExprCtx* expr_ctx_;
  ObEvalCtx* eval_ctx_;
  DupKeyCheckerRowStore* checker_row_store_;         // store the which need check
  DupKeyCheckerDatumRowStore* checker_datum_store_;  // store the which need check
  ObDMLMiniTaskExecutor& index_executor_;
  ObMiniJob mini_job_;
  ObMiniTaskResult unique_rowkey_result_;
  ObMiniTaskResult gui_lookup_result_;
  int64_t unique_index_count_;    // all unique index num,contain local unique index
  int64_t primary_rowkey_count_;  // rowkey size os basic table
  bool has_global_unique_index_;
  bool update_incremental_row_;
  common::ObSEArray<ObTaskInfo*, 4> scan_tasks_;
  common::ObArrayWrap<ObUniqueConstraintCtx> constraint_ctxs_;
  ObJobID ob_job_id_;
};

/**
 *             +--------------------------------+
 *             |                                |
 *             |   ObDuplicateKeyChecker        |
 *             |   table_scan_subplan_          |
 *             |   global_unique_index_scan_    |
 *             |   global_unique_index_lookup_  |
 *             |                                |
 *             +---+-------------------------+--+
 *                 |                         |
 *          fetch conflict rowkey       fetch conflict rowkey
 *                 &                         |
 *          fetch needed column data         |
 *  +--------------v--------------+ +--------v----------------+
 *  |   ObConflictRowFetcher      | |  ObConflictRowFetcher   |
 *  |                             | |                         |
 *  | primary_conflict_row_fetcher| |  global_unique_index    |
 *  +-----------------------------+ +----------+--------------+
 *                                             |
 *                                    look up needed column data
 *                                             |
 *                                             |
 *                                  +----------v------------------+
 *                                  |  ObConflictRowFetcher       |
 *                                  |                             |
 *                                  |  global_unique_index_lookup |
 *                                  +-----------------------------+
 *
 *  The conflict check of ObDuplicateKeyChecker is mainly divided into two steps:
 *  1. Obtain the primary key information corresponding to the conflicting row according to
 *  the information of the primary key or unique index key to be checked
 *  2. Back-check the main table according to the obtained conflicting primary key,
 *  and obtain the information of other columns of the conflicting row that the query needs to obtain
 *  Therefore, the duplicate key checker relies on obtaining the conflict information of the row data
 *  to be written from the storage layer.
 *  Here, the storage layer provides a corresponding interface to obtain this information.
 *  If a table has both a primary key and a local unique index, it also contains a global unique index,
 *  As shown in the figure above,
 *  ObDuplicateKeyChecker will generate two plans in the step of obtaining the conflicting row rowkey:
 *  1. primary_conflict_row_fetcher: used to get the rowkey of primary key + local unique index conflict,
 *  Since the OB has an IOT structure, the primary index&local unique index and the table data are in the same
 * partition, Therefore, in this case, in order to optimize the number of RPC calls, we will get the primary key rowkey
 * and bring back the other column data that needs to be read to the duplicate key checker.
 *  2. global_unique_index_fetcher: Because the global index does not store other column data of the table,
 *  Therefore, when obtaining the conflict information of the global unique index,
 *  only the rowkey of the primary table can be obtained.
 *  When the duplicate key checker receives this part of the conflicting rowkey planned to be obtained,
 *  if the conflict with the primary key is not completely overlapped,
 *  another table lookup needs to be sent.
 *  Task to get other column data corresponding to this part of rowkey
 */
class ObDuplicatedKeyChecker {
private:
  class ObUniqueIndexRowkeyIter : public ObIRowkeyIterator {
  public:
    ObUniqueIndexRowkeyIter(common::ObIAllocator& allocator, common::ObExprCtx* expr_ctx,
        DupKeyCheckerRowIter checker_row_iter, const ObUniqueIndexScanInfo& index_info)
        : allocator_(allocator),
          expr_ctx_(expr_ctx),
          index_info_(index_info),
          checker_row_iter_(checker_row_iter),
          iter_output_(NULL),
          eval_ctx_(NULL)
    {}

    ObUniqueIndexRowkeyIter(common::ObIAllocator& allocator, const ObExprPtrIArray* iter_output,
        const ObUniqueIndexScanInfo& index_info, ObEvalCtx* eval_ctx)
        : allocator_(allocator),
          expr_ctx_(NULL),
          index_info_(index_info),
          checker_datum_row_iter_(),
          iter_output_(iter_output),
          eval_ctx_(eval_ctx)

    {}

    virtual ~ObUniqueIndexRowkeyIter()
    {}
    virtual int get_next_conflict_row(common::ObNewRow& row) override;
    virtual int get_next_conflict_row() override;
    int init(ObChunkDatumStore* datum_store);
    virtual const ObExprPtrIArray* get_output() override
    {
      return iter_output_;
    };

  private:
    common::ObIAllocator& allocator_;
    common::ObExprCtx* expr_ctx_;
    const ObUniqueIndexScanInfo& index_info_;
    DupKeyCheckerRowIter checker_row_iter_;

    DupKeyCheckerDatumRowIter checker_datum_row_iter_;
    const ObExprPtrIArray* iter_output_;
    ObEvalCtx* eval_ctx_;
  };

  class ObPrimaryRowkeyIter : public ObIRowkeyIterator {
  public:
    ObPrimaryRowkeyIter(common::ObRowStore::Iterator rowkey_iter, ObDupKeyCheckerCtx& checker_ctx)
        : rowkey_iter_(rowkey_iter), checker_ctx_(checker_ctx), iter_output_(NULL), eval_ctx_(NULL)
    {}
    ObPrimaryRowkeyIter(const ObExprPtrIArray* iter_output, ObDupKeyCheckerCtx& checker_ctx, ObEvalCtx* eval_ctx)
        : checker_ctx_(checker_ctx), iter_output_(iter_output), datum_rowkey_iter_(), eval_ctx_(eval_ctx)
    {}
    virtual ~ObPrimaryRowkeyIter()
    {}
    virtual int get_next_conflict_row(common::ObNewRow& row) override;
    virtual int get_next_conflict_row() override;
    virtual const ObExprPtrIArray* get_output() override
    {
      return iter_output_;
    };
    int init(ObChunkDatumStore* datum_store);

  private:
    common::ObRowStore::Iterator rowkey_iter_;
    ObDupKeyCheckerCtx& checker_ctx_;
    // for static engine
    const ObExprPtrIArray* iter_output_;  // rowkey column of the basic table
    DupKeyCheckerDatumRowIter datum_rowkey_iter_;
    ObEvalCtx* eval_ctx_;
  };

public:
  ObDuplicatedKeyChecker()
      : unique_index_cnt_(0),
        phy_plan_(NULL),
        table_scan_info_(),
        gui_scan_root_(NULL),
        gui_scan_root_spec_(NULL),
        gui_lookup_info_(),
        gui_scan_infos_(),
        dml_flag_(storage::INSERT_RETURN_ALL_DUP),
        table_columns_(NULL),
        is_static_engine_(false)
  {}
  virtual ~ObDuplicatedKeyChecker()
  {}

  int init_checker_ctx(ObDupKeyCheckerCtx& ctx) const;
  void set_unique_index_cnt(int64_t unique_index_cnt)
  {
    unique_index_cnt_ = unique_index_cnt;
  }
  void set_physical_plan(const ObPhysicalPlan* phy_plan)
  {
    phy_plan_ = phy_plan;
  }
  void set_gui_scan_root(const ObPhyOperator* checker_root)
  {
    gui_scan_root_ = checker_root;
  }
  void set_gui_scan_root(const ObOpSpec* checker_spec)
  {
    gui_scan_root_spec_ = checker_spec;
  }
  void set_is_static_engine(bool is_static_engine)
  {
    is_static_engine_ = is_static_engine;
  }
  void set_table_columns(const ObExprPtrIArray* table_columns)
  {
    table_columns_ = table_columns;
  }
  int build_duplicate_rowkey_map(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const;
  int check_duplicate_rowkey(ObDupKeyCheckerCtx& checker_ctx, const common::ObNewRow& row,
      common::ObIArray<ObConstraintValue>& constraint_values) const;
  int check_duplicate_rowkey(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs,
      common::ObIArray<ObConstraintValue>& constraint_values) const;
  inline ObUniqueIndexScanInfo& get_table_scan_info()
  {
    return table_scan_info_;
  }
  inline common::ObArrayWrap<ObUniqueIndexScanInfo>& get_gui_scan_infos()
  {
    return gui_scan_infos_;
  }
  inline common::ObArrayWrap<ObPhyUniqueConstraintInfo>& get_unique_constraint_infos()
  {
    return constraint_infos_;
  }
  ObUniqueIndexScanInfo& get_gui_lookup_info()
  {
    return gui_lookup_info_;
  }
  void set_dml_flag(storage::ObInsertFlag dml_flag)
  {
    dml_flag_ = dml_flag;
  }
  int delete_old_row(ObDupKeyCheckerCtx& checker_ctx, const common::ObNewRow& row) const;
  int delete_old_row(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs) const;
  int insert_new_row(ObDupKeyCheckerCtx& checker_ctx, const common::ObNewRow& new_row) const;
  int insert_new_row(ObDupKeyCheckerCtx& checker_ctx, const ObExprPtrIArray& row_exprs,
      const ObChunkDatumStore::StoredRow& old_row) const;
  template <class ShuffleOp>
  int shuffle_final_data(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx, const ShuffleOp& shuffle_op) const;

private:
  int build_conflict_row_task_info(const ObUniqueIndexScanInfo& index_info, ObExecContext& ctx,
      ObIRowkeyIterator& rowkey_iter, ObDupKeyCheckerCtx& checker_ctx) const;
  int build_conflict_row_task_info_list(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const;
  int rebuild_gui_lookup_task_info_list(ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx) const;
  bool need_gui_lookup(ObDupKeyCheckerCtx& checker_ctx) const;
  int build_base_line_constraint_infos(ObDupKeyCheckerCtx& checker_ctx, ObEvalCtx* eval_ctx,
      const ObExprPtrIArray* iter_output, ObChunkDatumStore::Iterator& datum_iter) const;
  int build_base_line_constraint_infos(ObDupKeyCheckerCtx& checker_ctx, common::ObRowStore::Iterator& row_iter) const;
  int extract_rowkey_info(const ObPhyUniqueConstraintInfo& constraint_info, const common::ObNewRow& table_row,
      char* buf, int64_t buf_len, const ObTimeZoneInfo* tz_info) const;
  int extract_rowkey_info(const ObPhyUniqueConstraintInfo& constraint_info, ObEvalCtx& eval_ctx, char* buf,
      int64_t buf_len, const ObTimeZoneInfo* tz_info) const;

private:
  int64_t unique_index_cnt_;
  const ObPhysicalPlan* phy_plan_;
  // primary and local unique index scan plan root
  ObUniqueIndexScanInfo table_scan_info_;
  // global unique index scan plan root
  const ObPhyOperator* gui_scan_root_;
  const ObOpSpec* gui_scan_root_spec_;  // append
  ObUniqueIndexScanInfo gui_lookup_info_;
  common::ObArrayWrap<ObUniqueIndexScanInfo> gui_scan_infos_;
  storage::ObInsertFlag dml_flag_;
  common::ObArrayWrap<ObPhyUniqueConstraintInfo> constraint_infos_;
  // All columns of the dependent basic table
  const ObExprPtrIArray* table_columns_;
  bool is_static_engine_;
};

template <class ShuffleOp>
int ObDuplicatedKeyChecker::shuffle_final_data(
    ObExecContext& ctx, ObDupKeyCheckerCtx& checker_ctx, const ShuffleOp& shuffle_op) const
{
  int ret = common::OB_SUCCESS;
  ObUniqueConstraintCtx& primary_ctx = checker_ctx.constraint_ctxs_.at(0);
  ObUniqueConstraintCtx::iterator row_iter = primary_ctx.begin();
  for (; OB_SUCC(ret) && row_iter != primary_ctx.end(); ++row_iter) {
    ObConstraintValue& constraint_value = row_iter->second;
    if (constraint_value.baseline_row_ == constraint_value.current_row_) {
      // no changed, ignore it
    } else {
      if (NULL != constraint_value.baseline_row_) {
        // baseline row is not empty, delete it
        if (OB_FAIL(shuffle_op.shuffle_final_delete_row(ctx, *constraint_value.baseline_row_))) {
          SQL_ENG_LOG(WARN, "shuffle delete row failed", K(ret), K(constraint_value));
        }
        SQL_ENG_LOG(DEBUG, "shuffle final delete row", K(ret), KPC(constraint_value.baseline_row_));
      }
      if (OB_SUCC(ret) && NULL != constraint_value.current_row_) {
        // current row is not empty, insert new row
        if (OB_FAIL(shuffle_op.shuffle_final_insert_row(ctx, *constraint_value.current_row_))) {
          SQL_ENG_LOG(WARN, "shuffle insert row failed", K(ret), K(constraint_value));
        }
        SQL_ENG_LOG(DEBUG, "shuffle final insert row", K(ret), KPC(constraint_value.current_row_));
      }
    }
  }
  return ret;
}

template <class ShuffleOp>
int ObDupKeyCheckerCtx::shuffle_final_data(
    ObExecContext& ctx, const ObExprPtrIArray& output_exprs, ShuffleOp& shuffle_op)
{
  int ret = common::OB_SUCCESS;
  ObUniqueConstraintCtx& primary_ctx = constraint_ctxs_.at(0);
  ObUniqueConstraintCtx::iterator row_iter = primary_ctx.begin();
  for (; OB_SUCC(ret) && row_iter != primary_ctx.end(); ++row_iter) {
    ObConstraintValue& constraint_value = row_iter->second;
    if (constraint_value.baseline_datum_row_ == constraint_value.current_datum_row_) {
      // no changed, ignore it
    } else {
      if (NULL != constraint_value.baseline_datum_row_) {
        // baseline row is not empty, delete it
        if (OB_ISNULL(ctx.get_eval_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          SQL_ENG_LOG(WARN, "eval ctx is null", K(ret));
        } else if (OB_FAIL(constraint_value.baseline_datum_row_->to_expr(output_exprs, *ctx.get_eval_ctx()))) {
          SQL_ENG_LOG(WARN, "stored row to expr failed", K(ret));
        } else if (OB_FAIL(shuffle_op.shuffle_final_delete_row(ctx, output_exprs))) {
          SQL_ENG_LOG(WARN, "shuffle delete row failed", K(ret), K(constraint_value));
        }
      }
      if (OB_SUCC(ret) && NULL != constraint_value.current_datum_row_) {
        // current row is not empty, insert new row
        if (OB_ISNULL(ctx.get_eval_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          SQL_ENG_LOG(WARN, "eval ctx is null", K(ret));
        } else if (OB_FAIL(constraint_value.current_datum_row_->to_expr(output_exprs, *ctx.get_eval_ctx()))) {
          SQL_ENG_LOG(WARN, "stored row to expr failed", K(ret));
        } else if (OB_FAIL(shuffle_op.shuffle_final_insert_row(ctx, output_exprs))) {
          SQL_ENG_LOG(WARN, "shuffle insert row failed", K(ret), K(constraint_value));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_DUPLICATED_KEY_CHECKER_H_ */
