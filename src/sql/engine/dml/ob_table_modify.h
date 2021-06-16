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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_MODIFY_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_MODIFY_H_
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_2d_array.h"
#include "common/row/ob_row_iterator.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/engine/ob_operator.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/schema/ob_table_dml_param.h"
namespace oceanbase {
namespace storage {
class ObDMLBaseParam;
}
namespace common {
struct ObPartitionKey;
}
}  // namespace oceanbase
namespace oceanbase {
namespace sql {
class ObTableModifySpec;
class ObPhyTableLocation;
typedef Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false> ParamStore;

struct ObColumnConvInfo {
  OB_UNIS_VERSION(1);

public:
  ObColumnConvInfo() : type_(), column_flags_(0), column_info_(), str_values_()
  {}
  ObColumnConvInfo(common::ObIAllocator& allocator)
      : type_(allocator), column_flags_(0), column_info_(), str_values_(allocator)
  {}
  inline void set_allocator(common::ObIAllocator* alloc)
  {
    str_values_.set_allocator(alloc);
  }

  ObExprResType type_;
  uint64_t column_flags_;
  common::ObString column_info_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> str_values_;

  TO_STRING_KV(K_(type), K_(column_flags), K_(column_info), K_(str_values));
};

class ObTableModify;
class ObTableLocation;
struct DMLSubPlan {
  DMLSubPlan()
  {
    memset(this, 0, sizeof(*this));
  }
  ObTableModify* subplan_root_;
  int32_t* value_projector_;
  int64_t value_projector_size_;
  TO_STRING_KV(
      KPC_(subplan_root), "value_projector", common::ObArrayWrap<int32_t>(value_projector_, value_projector_size_));
};

struct SeDMLSubPlan {
  SeDMLSubPlan() : subplan_root_(NULL), access_exprs_()
  {}
  ObTableModifySpec* subplan_root_;
  ObFixedArray<ObExpr*, common::ObIAllocator> access_exprs_;
};
typedef common::ObArrayWrap<ObTableLocation*> TableLocationArray;
typedef common::ObArrayWrap<DMLSubPlan> DMLSubPlanArray;
typedef common::ObArrayWrap<SeDMLSubPlan> SeDMLSubPlanArray;
typedef common::ObArrayWrap<ObExpr*> ExprArrayWrap;

struct ObGlobalIndexDMLInfo {
  ObGlobalIndexDMLInfo()
      : table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        part_cnt_(0),
        table_locs_(),
        dml_subplans_(),
        calc_exprs_()
  {}
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    index_tid_ = common::OB_INVALID_ID;
    part_cnt_ = 0;
    table_locs_.reset();
    dml_subplans_.reset();
    calc_part_id_exprs_.reset();
    calc_exprs_.reset();
    se_subplans_.reset();
    hint_part_ids_.reset();
  }
  uint64_t table_id_;
  uint64_t index_tid_;
  int64_t part_cnt_;
  TableLocationArray table_locs_;
  // Any DML operation of each table can be decomposed into one or more of insert/delete/update.
  // So here, let CG generate the sub-plans of these three operators for the DML of each table,
  // and dynamically fill in the data for these sub-plans during execution.
  DMLSubPlanArray dml_subplans_;
  ExprArrayWrap calc_part_id_exprs_;
  ExprFixedArray calc_exprs_;
  SeDMLSubPlanArray se_subplans_;
  common::ObFixedArray<int64_t, common::ObIAllocator> hint_part_ids_;

  TO_STRING_KV(K_(table_id), K_(index_tid), K_(part_cnt), K_(table_locs), K_(dml_subplans));
};

struct ObGlobalIndexDMLCtx {
  ObGlobalIndexDMLCtx()
      : table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        part_cnt_(0),
        is_table_(false),
        dml_subplans_(),
        partition_ids_()
  {}
  ~ObGlobalIndexDMLCtx()
  {}
  uint64_t table_id_;
  uint64_t index_tid_;
  int64_t part_cnt_;
  bool is_table_;
  DMLSubPlanArray dml_subplans_;
  SeDMLSubPlanArray se_subplans_;
  common::ObSEArray<int64_t, 4> partition_ids_;
  TO_STRING_KV(K_(table_id), K_(index_tid), K_(part_cnt), K_(is_table), K_(dml_subplans), K_(partition_ids));
};

struct ObAssignColumns {
  ObAssignColumns()
      : old_projector_(NULL),
        old_projector_size_(0),
        new_projector_(NULL),
        new_projector_size_(0),
        inner_alloc_("AssignColRow"),
        old_row_(&inner_alloc_),
        new_row_(&inner_alloc_),
        assign_columns_()
  {}

  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(assign_columns_.allocate_array(allocator, count))) {
      SQL_ENG_LOG(WARN, "allocate array failed", K(ret), K(count));
    } else {
      old_row_.set_allocator(&allocator);
      new_row_.set_allocator(&allocator);
    }
    return ret;
  }

  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp)
  {
    UNUSED(column_id);
    int ret = common::OB_SUCCESS;
    ColumnContent column;
    column.projector_index_ = projector_index;
    column.auto_filled_timestamp_ = auto_filled_timestamp;
    if (OB_UNLIKELY(array_index < 0) || OB_UNLIKELY(array_index >= assign_columns_.count())) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid array_index", K(ret), K(array_index), K(assign_columns_.count()));
    } else {
      assign_columns_.at(array_index) = column;
    }
    return ret;
  }

  void set_updated_projector(int32_t* projector, int64_t projector_size)
  {
    new_projector_ = projector;
    new_projector_size_ = projector_size;
  }

  void project_old_and_new_row(
      const common::ObNewRow& full_row, common::ObNewRow& old_row, common::ObNewRow& new_row) const
  {
    new_row.cells_ = full_row.cells_;
    new_row.count_ = full_row.count_;
    new_row.projector_ = new_projector_;
    new_row.projector_size_ = new_projector_size_;
    old_row.cells_ = full_row.cells_;
    old_row.count_ = full_row.count_;
    old_row.projector_ = old_projector_;
    old_row.projector_size_ = old_projector_size_;
  }

  bool check_row_whether_changed(const common::ObNewRow& new_row) const
  {
    bool bret = false;
    if (assign_columns_.count() > 0 && new_row.is_valid()) {
      int64_t projector_index = assign_columns_.at(0).projector_index_;
      if (projector_index >= 0 && projector_index < new_row.get_count()) {
        const ObObj& updated_value = new_row.get_cell(projector_index);
        bret = !(updated_value.is_ext() && ObActionFlag::OP_LOCK_ROW == updated_value.get_ext());
      }
    }
    return bret;
  }

  const common::ObIArrayWrap<ColumnContent>& get_assign_columns() const
  {
    return assign_columns_;
  }

  TO_STRING_KV("old_projector", common::ObArrayWrap<int32_t>(old_projector_, old_projector_size_), "new_projector",
      common::ObArrayWrap<int32_t>(new_projector_, new_projector_size_), K_(assign_columns));
  int32_t* old_projector_;
  int64_t old_projector_size_;
  int32_t* new_projector_;
  int64_t new_projector_size_;
  common::ModulePageAllocator inner_alloc_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
  common::ObArrayWrap<ColumnContent> assign_columns_;
};

struct ObTableDMLInfo {
  ObTableDMLInfo()
      : distinct_algo_(), index_infos_(), is_enable_row_movement_(false), rowkey_cnt_(0), need_check_filter_null_(false)
  {}
  inline void reset()
  {
    distinct_algo_ = T_DISTINCT_NONE;
    index_infos_.reset();
    is_enable_row_movement_ = false;
    rowkey_cnt_ = 0;
    need_check_filter_null_ = false;
  }
  DistinctType distinct_algo_;
  common::ObArrayWrap<ObGlobalIndexDMLInfo> index_infos_;
  ObAssignColumns assign_columns_;
  bool is_enable_row_movement_;
  int64_t rowkey_cnt_;
  bool need_check_filter_null_;

  inline void set_enable_row_movement(bool b)
  {
    is_enable_row_movement_ = b;
  }
  inline bool get_enable_row_movement() const
  {
    return is_enable_row_movement_;
  }

  TO_STRING_KV(K_(distinct_algo), K_(index_infos), K_(assign_columns), K_(is_enable_row_movement), K_(rowkey_cnt),
      K_(need_check_filter_null));
};

struct RowkeyItem {
  inline bool operator==(const RowkeyItem& other) const
  {
    bool bret = true;
    if (rowkey_.get_count() != other.rowkey_.get_count()) {
      bret = false;
    }
    for (int64_t i = 0; bret && i < rowkey_.get_count(); ++i) {
      if (rowkey_.get_cell(i) != other.rowkey_.get_cell(i)) {
        bret = false;
      }
    }
    return bret;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    for (int64_t i = 0; i < rowkey_.get_count(); ++i) {
      hash_ret = rowkey_.get_cell(i).hash(hash_ret);
    }
    return hash_ret;
  }

  common::ObNewRow rowkey_;
};
typedef common::hash::ObHashSet<RowkeyItem, common::hash::NoPthreadDefendMode> RowkeyDistCtx;

// for check_rowkey_whether_distinct
// to check if each rowkey of ObOpSpec is distinct. same as RowkeyItem
struct SeRowkeyItem {
  SeRowkeyItem() : row_(NULL), datums_(NULL), cnt_(0)
  {}
  int init(const ObExprPtrIArray& row, ObEvalCtx& eval_ctx, ObIAllocator& alloc, const int64_t rowkey_cnt);
  bool operator==(const SeRowkeyItem& other) const;
  uint64_t hash() const;
  int copy_datum_data(ObIAllocator& alloc);

  const ObExpr* const* row_;
  ObDatum* datums_;
  int64_t cnt_;
};

typedef common::hash::ObHashSet<SeRowkeyItem, common::hash::NoPthreadDefendMode> SeRowkeyDistCtx;

struct ObTableDMLCtx {
  ObTableDMLCtx() : index_ctxs_(), rowkey_dist_ctx_(NULL), se_rowkey_dist_ctx_(NULL), cur_row_()
  {}
  virtual ~ObTableDMLCtx()
  {
    if (rowkey_dist_ctx_ != nullptr) {
      rowkey_dist_ctx_->destroy();
    }
    if (se_rowkey_dist_ctx_ != nullptr) {
      se_rowkey_dist_ctx_->destroy();
    }
    index_ctxs_.release_array();
  }
  common::ObArrayWrap<ObGlobalIndexDMLCtx> index_ctxs_;
  RowkeyDistCtx* rowkey_dist_ctx_;
  SeRowkeyDistCtx* se_rowkey_dist_ctx_;
  common::ObNewRow cur_row_;
};

struct DMLPartInfo {
  OB_UNIS_VERSION(1);

public:
  TO_STRING_KV(K_(partition_key), K_(part_row_cnt));

  common::ObPartitionKey partition_key_;
  int64_t part_row_cnt_;
};

class ObTableModifyInput : public ObIPhyOperatorInput {
  friend class ObTableModify;
  OB_UNIS_VERSION_V(1);

public:
  ObTableModifyInput() : location_idx_(common::OB_INVALID_INDEX), is_single_part_(false), part_infos_()
  {}
  virtual ~ObTableModifyInput()
  {}
  virtual void reset() override
  {
    location_idx_ = common::OB_INVALID_INDEX;
    is_single_part_ = false;
    part_infos_.reset();
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  inline int64_t get_location_idx() const
  {
    return location_idx_;
  }
  inline void set_location_idx(int64_t location_idx)
  {
    location_idx_ = location_idx;
  }
  virtual bool need_serialized() const
  {
    return !is_single_part_;
  }
  /**
   * @brief set allocator which is used for deserialize, but not all objects will use allocator
   * while deserializing, so you can override it if you need.
   */
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator)
  {
    part_infos_.set_allocator(allocator);
  }
  TO_STRING_KV(K_(location_idx), K_(is_single_part), K_(part_infos));

private:
  int64_t location_idx_;
  bool is_single_part_;
  common::ObFixedArray<DMLPartInfo, common::ObIAllocator> part_infos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableModifyInput);
};

typedef common::ObFixedArray<ObForeignKeyArg, common::ObIAllocator> ObForeignKeyArgArray;

class ObTableModify : public ObSingleChildPhyOperator {
protected:
  class ObTableModifyCtx : public ObPhyOperatorCtx {
  public:
    explicit ObTableModifyCtx(ObExecContext& exec_ctx)
        : ObPhyOperatorCtx(exec_ctx),
          exec_ctx_(exec_ctx),
          sql_proxy_(NULL),
          inner_conn_(NULL),
          tenant_id_(0),
          saved_conn_(),
          is_nested_session_(false),
          foreign_key_checks_(false),
          need_close_conn_(false),
          rowkey_dist_ctx_(NULL),
          iter_end_(false),
          saved_session_(NULL)
    {}
    virtual ~ObTableModifyCtx()
    {
      destroy();
    }
    virtual void destroy();
    int open_inner_conn();
    int close_inner_conn();
    int begin_nested_session(bool skip_cur_stmt_tables);
    int end_nested_session();
    int set_foreign_key_cascade(bool is_cascade);
    int get_foreign_key_cascade(bool& is_cascade) const;
    int set_foreign_key_check_exist(bool is_check_exist);
    int get_foreign_key_check_exist(bool& is_check_exist) const;
    int execute_write(const char* sql);
    int execute_read(const char* sql, common::ObMySQLProxy::MySQLResult& res);
    bool is_nested_session()
    {
      return is_nested_session_;
    }
    void set_foreign_key_checks()
    {
      foreign_key_checks_ = true;
    }
    bool need_foreign_key_checks()
    {
      return foreign_key_checks_;
    }
    const ObTimeZoneInfo* get_tz_info()
    {
      return TZ_INFO(exec_ctx_.get_my_session());
    }
    const ObObjPrintParams get_obj_print_params()
    {
      return CREATE_OBJ_PRINT_PARAM(exec_ctx_.get_my_session());
    }
    int check_stack();

  public:
    ObExecContext& exec_ctx_;
    common::ObMySQLProxy* sql_proxy_;
    observer::ObInnerSQLConnection* inner_conn_;
    uint64_t tenant_id_;
    sql::ObSQLSessionInfo::StmtSavedValue& get_saved_session()
    {
      if (NULL == saved_session_) {
        saved_session_ = new (saved_session_buf_) sql::ObSQLSessionInfo::StmtSavedValue();
      }
      return *saved_session_;
    }
    observer::ObInnerSQLConnection::SavedValue saved_conn_;
    bool is_nested_session_;
    bool foreign_key_checks_;
    bool need_close_conn_;
    RowkeyDistCtx* rowkey_dist_ctx_;
    bool iter_end_;

  private:
    ObSQLSessionInfo::StmtSavedValue* saved_session_;
    char* saved_session_buf_[sizeof(ObSQLSessionInfo::StmtSavedValue)];
  };
  class ObDMLRowIterator : public common::ObNewRowIterator {
  public:
    explicit ObDMLRowIterator(ObExecContext& ctx, const ObTableModify& op) : ctx_(ctx), op_(op), project_row_()
    {}
    ~ObDMLRowIterator()
    {
      reset();
    }
    int init();
    int get_next_row(common::ObNewRow*& row);
    int get_next_rows(common::ObNewRow*& row, int64_t& row_count);
    void reset();

  private:
    ObExecContext& ctx_;
    const ObTableModify& op_;
    common::ObNewRow project_row_;
  };
  class ForeignKeyHandle {
  public:
    static int do_handle_old_row(
        ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const common::ObNewRow& old_row);
    static int do_handle_old_row(
        ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args, const common::ObNewRow& old_row);
    static int do_handle_new_row(
        ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const common::ObNewRow& new_row);
    static int do_handle_new_row(
        ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args, const common::ObNewRow& new_row);
    static int do_handle(ObTableModify* modify_op, ObTableModifyCtx& modify_ctx, const common::ObNewRow& old_row,
        const common::ObNewRow& new_row);
    static int do_handle(ObTableModifyCtx& modify_ctx, const ObForeignKeyArgArray& fk_args,
        const common::ObNewRow& old_row, const common::ObNewRow& new_row);

  private:
    static int value_changed(const common::ObIArray<ObForeignKeyColumn>& columns, const common::ObNewRow& old_row,
        const common::ObNewRow& new_row, bool& has_changed);
    static int check_exist(
        ObTableModifyCtx& modify_ctx, const ObForeignKeyArg& fk_arg, const common::ObNewRow& row, bool expect_zero);
    static int cascade(ObTableModifyCtx& modify_ctx, const ObForeignKeyArg& fk_arg, const common::ObNewRow& old_row,
        const common::ObNewRow& new_row);
    static int gen_set(char* buf, int64_t len, int64_t& pos, const common::ObIArray<ObForeignKeyColumn>& columns,
        const common::ObNewRow& row, const common::ObObjPrintParams& print_params);
    static int gen_where(char* buf, int64_t len, int64_t& pos, const common::ObIArray<ObForeignKeyColumn>& columns,
        const common::ObNewRow& row, const common::ObObjPrintParams& print_params);
    static int gen_column_value(char* buf, int64_t len, int64_t& pos,
        const common::ObIArray<ObForeignKeyColumn>& columns, const common::ObNewRow& row, const char* delimiter,
        const common::ObObjPrintParams& print_params, bool forbid_null);
    static bool is_self_ref_row(const ObNewRow& row, const ObForeignKeyArg& fk_arg);
  };

private:
  // The multi-table semantics of update and delete, such as udpate t1, t2, the data source
  // is the Cartesian product of t1 and t2, if there are duplicate rows, only the first row
  // is processed each time, and it needs to be filtered out later. Here, a constant is
  // defined to represent the bucket of the hashset The number is currently 1M. The size
  // of the application is estimated to be about several hundred megabytes. This value
  // needs to be considered. If it is too small, the conflict will be serious and the
  // performance will be affected. If it is too large, the application memory will fail.
  static const int64_t MIN_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024;
  static const int64_t MAX_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024 * 1024;

  OB_UNIS_VERSION(1);

public:
  explicit ObTableModify(common::ObIAllocator& alloc);
  ~ObTableModify();

  void reset();
  void reuse();
  /**
   * @brief the table id specify which table will be modified data
   */
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  void set_index_tid(uint64_t index_tid)
  {
    index_tid_ = index_tid;
  }
  //  void set_child_table_id(uint64_t table_id) { child_table_id_ = table_id; }
  //  void set_parent_table_id(uint64_t table_id) { parent_table_id_ = table_id; }
  int add_column_info(const ColumnContent& column);
  virtual int create_operator_input(ObExecContext& ctx) const;
  /**
   * @brief table_modify must tell storage every cell's column id in the row
   * @param column_id[in], the order of column_id must correspond with the index of column in the row
   */
  int add_column_id(uint64_t column_id);
  // the row which needs to lock should promise primary key is in the front; Besides, it should not
  // read datas based on projector
  int lock_row(ObExecContext& ctx, const common::ObNewRow& row, storage::ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey) const;
  int set_primary_key_ids(const common::ObIArray<uint64_t>& column_ids);
  void set_ignore(bool is_ignore)
  {
    is_ignore_ = is_ignore;
  }
  int init_primary_key_ids(int64_t rowkey_cnt)
  {
    return primary_key_ids_.init(rowkey_cnt);
  }
  int add_primary_key_id(uint64_t rowkey_id)
  {
    return primary_key_ids_.push_back(rowkey_id);
  }
  int primary_key_count() const
  {
    return primary_key_ids_.count();
  }
  int init_column_ids_count(int64_t count)
  {
    return init_array_size<>(column_ids_, count);
  }
  int init_column_infos_count(int64_t count)
  {
    return init_array_size<>(column_infos_, count);
  }
  int init_column_conv_info_count(int64_t count)
  {
    return init_array_size<>(column_conv_infos_, count);
  }
  int add_column_conv_info(const ObExprResType& res_type, const uint64_t column_flags, common::ObIAllocator& allocator,
      const common::ObIArray<common::ObString>* str_values = NULL, const common::ObString* column_info_str = NULL);
  int add_returning_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(returning_exprs_, expr);
  }
  bool is_returning() const
  {
    return !returning_exprs_.is_empty();
  }
  int add_check_constraint_expr(ObSqlExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(check_constraint_exprs_, expr);
  }
  bool need_filter_null_row() const
  {
    return need_filter_null_row_;
  }
  void set_need_filter_null_row(bool need_filter)
  {
    need_filter_null_row_ = need_filter;
  }
  bool need_skip_log_user_error() const
  {
    return need_skip_log_user_error_;
  }
  void set_need_skip_log_user_error(bool need_skip_log_user_error)
  {
    need_skip_log_user_error_ = need_skip_log_user_error;
  }
  void set_distinct_algo(DistinctType distinct_type)
  {
    distinct_algo_ = distinct_type;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  uint64_t get_index_tid() const
  {
    return index_tid_;
  }
  virtual bool is_dml_operator() const
  {
    return true;
  }
  virtual bool is_dml_without_output() const
  {
    return !is_returning();
  }
  static int extend_dml_stmt(ObExecContext& ctx, const common::ObIArrayWrap<ObTableDMLInfo>& dml_table_infos,
      const common::ObIArrayWrap<ObTableDMLCtx>& dml_table_ctxs);
  inline void set_from_multi_table_dml(bool from_multi_table_dml)
  {
    from_multi_table_dml_ = from_multi_table_dml;
  }
  inline bool from_multi_table_dml() const
  {
    return from_multi_table_dml_;
  }
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const;
  static common::ObString get_duplicated_rowkey_buffer(const common::ObIArray<uint64_t>& rowkey_ids,
      const common::ObNewRow& row, const common::ObTimeZoneInfo* tz_info = NULL);
  static int init_dml_param(ObExecContext& ctx, const uint64_t table_id, const ObPhyOperator& phy_op,
      const bool only_data_table, const share::schema::ObTableDMLParam* table_param,
      storage::ObDMLBaseParam& dml_param);
  static int init_dml_param_se(ObExecContext& ctx, const uint64_t table_id, const bool only_data_table,
      const share::schema::ObTableDMLParam* table_param, storage::ObDMLBaseParam& dml_param);
  template <class UpdateCtx, class UpdateOp>
  int check_updated_value(UpdateCtx& update_ctx, const UpdateOp& update_op, const common::ObNewRow& old_row,
      common::ObNewRow& new_row, bool& is_updated) const;
  /**
   * @brief check the updated row value whether nullable, whether be updated, whether need casted
   * @param updated_row
   * @param is_updated
   */
  static int check_row_value(const common::ObIArrayWrap<ColumnContent>& update_column_infos,
      const common::ObNewRow& old_row, common::ObNewRow& new_row);
  static int mark_lock_row_flag(
      const common::ObIArrayWrap<ColumnContent>& update_column_infos, common::ObNewRow& new_row);
  virtual int switch_iterator(ObExecContext& ctx) const override;
  void set_stmt_id_idx(int64_t stmt_id_idx)
  {
    stmt_id_idx_ = stmt_id_idx;
  }

public:
  int init_foreign_key_args(int64_t fk_count);
  int add_foreign_key_arg(const ObForeignKeyArg& fk_arg);
  int init_foreign_key_operation(ObExecContext& ctx) const;
  virtual bool has_foreign_key() const;
  OB_INLINE const ObForeignKeyArgArray& get_fk_args() const
  {
    return fk_args_;
  }
  int32_t get_column_idx(uint64_t column_id);

  OB_INLINE const common::ObIArray<ColumnContent>& get_column_infos() const
  {
    return column_infos_;
  }
  OB_INLINE const common::ObDList<ObSqlExpression>& check_constraint_exprs() const
  {
    return check_constraint_exprs_;
  }

  OB_INLINE share::schema::ObTableDMLParam& get_table_param()
  {
    return table_param_;
  }
  OB_INLINE const common::ObIArray<uint64_t>& get_column_ids() const
  {
    return column_ids_;
  }
  int check_rowkey_whether_distinct(ObExecContext& ctx, const ObNewRow& dml_row, int64_t rowkey_cnt,
      DistinctType distinct_algo, RowkeyDistCtx*& rowkey_dist_ctx, bool& is_dist) const;
  void set_gi_above(bool gi_above)
  {
    gi_above_ = gi_above;
  }
  void set_is_returning(bool v)
  {
    is_returning_ = v;
  }
  void set_is_pdml_index_maintain(bool v)
  {
    is_pdml_index_maintain_ = v;
  }
  void set_need_check_pk_is_null(bool v)
  {
    need_check_pk_is_null_ = v;
  }
  bool need_check_pk_is_null() const
  {
    return need_check_pk_is_null_;
  }
  void set_table_location_uncertain(bool v)
  {
    table_location_uncertain_ = v;
  }
  bool is_table_location_uncertain() const
  {
    return table_location_uncertain_;
  }
  virtual bool is_multi_dml() const
  {
    return false;
  }

protected:
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

  int calculate_virtual_column(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row, int64_t row_num) const;
  int calc_returning_row(ObExprCtx& expr_ctx, const ObNewRow& cur_row, ObNewRow& return_row) const;
  int save_returning_row(ObExprCtx& expr_ctx, const ObNewRow& row, ObNewRow& return_row, ObRowStore& store) const;
  int validate_row(common::ObExprCtx& expr_ctx, common::ObCastCtx& column_conv_ctx, common::ObNewRow& calc_row) const;
  int validate_normal_column(
      common::ObExprCtx& expr_ctx, common::ObCastCtx& column_conv_ctx, common::ObNewRow& calc_row) const;
  int validate_virtual_column(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row, int64_t row_num) const;
  int validate_row(common::ObExprCtx& expr_ctx, common::ObCastCtx& column_conv_ctx, common::ObNewRow& calc_row,
      bool check_normal_column, bool check_virtual_column) const;
  int check_row_null(
      ObExecContext& ctx, const common::ObNewRow& calc_row, const common::ObIArray<ColumnContent>& column_infos) const;
  int set_autoinc_param_pkey(ObExecContext& ctx, const common::ObPartitionKey& pkey) const;
  int get_part_location(ObExecContext& ctx, const ObPhyTableLocation& table_location,
      const share::ObPartitionReplicaLocation*& out) const;
  int get_part_location(ObExecContext& ctx, common::ObIArray<DMLPartInfo>& part_keys) const;
  // for checking the rowkey whether null, the head of the row must be rowkey
  int check_rowkey_is_null(const ObNewRow& row, int64_t rowkey_cnt, bool& is_null) const;
  int get_gi_task(ObExecContext& ctx) const;
  void log_user_error_inner(int ret, int64_t col_idx, int64_t row_num, ObExecContext& ctx) const;
  int calc_row_for_pdml(ObExecContext& ctx, ObNewRow& cur_row) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableModify);

protected:
  uint64_t table_id_;
  uint64_t index_tid_;
  bool is_ignore_;
  // to identify whether is a multi partition dml or a single partition dml
  bool from_multi_table_dml_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> primary_key_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> column_infos_;
  common::ObFixedArray<ObColumnConvInfo, common::ObIAllocator> column_conv_infos_;
  common::ObDList<ObSqlExpression> returning_exprs_;
  common::ObDList<ObSqlExpression> check_constraint_exprs_;
  ObForeignKeyArgArray fk_args_;
  uint64_t tg_event_;
  share::schema::ObTableDMLParam table_param_;
  bool need_filter_null_row_;
  DistinctType distinct_algo_;
  bool gi_above_;
  // if current operator is pdml, this represents whether need to return rows
  bool is_returning_;
  // the index of stmt_id column in the row
  int64_t stmt_id_idx_;
  // no need to check pk is null for heap table
  bool need_check_pk_is_null_;
  ObSqlExpression* old_row_rowid_;
  ObSqlExpression* new_row_rowid_;
  bool need_skip_log_user_error_;
  // just for compatible with 22x, no use now
  common::ObDList<ObSqlExpression> tsc_virtual_column_exprs_;
  bool is_pdml_index_maintain_;
  bool table_location_uncertain_;
};

template <class UpdateCtx, class UpdateOp>
int ObTableModify::check_updated_value(UpdateCtx& update_ctx, const UpdateOp& update_op,
    const common::ObNewRow& old_row, common::ObNewRow& new_row, bool& is_updated) const
{
  int ret = common::OB_SUCCESS;
  is_updated = false;
  if (OB_FAIL(check_row_value(update_op.get_assign_columns(), old_row, new_row))) {
    SQL_ENG_LOG(WARN, "check row value failed", K(ret), K(old_row), K(new_row));
  } else if (update_op.check_row_whether_changed(new_row)) {
    is_updated = true;
    update_ctx.inc_changed_rows();
    update_ctx.inc_affected_rows();
  }
  update_ctx.inc_found_rows();
  return ret;
}

class ObMultiDMLCtx;
class ObMultiDMLInfo {
public:
  class ObIsMultiDMLGuard {
  public:
    explicit ObIsMultiDMLGuard(ObPhysicalPlanCtx& plan_ctx) : is_multi_dml_(plan_ctx.get_is_multi_dml())
    {
      is_multi_dml_ = true;
    }
    ~ObIsMultiDMLGuard()
    {
      is_multi_dml_ = false;
    }

  private:
    bool& is_multi_dml_;
  };

public:
  ObMultiDMLInfo(common::ObIAllocator& alloc)
      : allocator_(alloc), table_dml_infos_(), subplan_root_(NULL), se_subplan_root_(NULL)
  {}
  ~ObMultiDMLInfo()
  {}

  int init_table_dml_info_array(int64_t count)
  {
    return table_dml_infos_.allocate_array(allocator_, count);
  }
  int add_table_dml_info(int64_t idx, const ObTableDMLInfo& table_info);
  void set_subplan_root(const ObPhyOperator* subplan_root)
  {
    subplan_root_ = subplan_root;
  }
  void set_subplan_root(const ObOpSpec* se_subplan_root)
  {
    se_subplan_root_ = se_subplan_root;
  }
  int shuffle_dml_row(
      ObExecContext& ctx, ObMultiDMLCtx& multi_dml_ctx, const ObExprPtrIArray& row, int64_t dml_op) const;
  int shuffle_dml_row(ObExecContext& ctx, common::ObPartMgr& part_mgr, ObMultiDMLCtx& multi_dml_ctx,
      const common::ObNewRow& row, int64_t dml_op) const;
  bool subplan_has_foreign_key() const;
  // for engine 3.0
  bool sesubplan_has_foreign_key() const;
  int wait_all_task(ObMultiDMLCtx* dml_ctx, ObPhysicalPlanCtx* plan_ctx) const;

public:
  common::ObIAllocator& allocator_;
  common::ObArrayWrap<ObTableDMLInfo> table_dml_infos_;
  const ObPhyOperator* subplan_root_;
  const ObOpSpec* se_subplan_root_;
};

class ObMultiDMLCtx {
  friend class ObMultiDMLInfo;

public:
  explicit ObMultiDMLCtx(common::ObIAllocator& allocator)
      : table_dml_ctxs_(),
        multi_dml_plan_mgr_(allocator),
        mini_task_executor_(allocator),
        returning_row_(),
        returning_row_store_(allocator, ObModIds::OB_SQL_ROW_STORE, common::OB_SERVER_TENANT_ID, false),
        returning_row_iterator_(),
        allocator_(allocator)
  {}
  ~ObMultiDMLCtx()
  {}
  void destroy_ctx()
  {
    returning_row_store_.reset();
    returning_row_iterator_.reset();
    multi_dml_plan_mgr_.reset();
    mini_task_executor_.destroy();
    table_dml_ctxs_.release_array();
  }
  int init_multi_dml_ctx(ObExecContext& ctx, const common::ObIArrayWrap<ObTableDMLInfo>& table_dml_infos,
      const ObPhysicalPlan* phy_plan, const ObPhyOperator* subplan_root, const ObOpSpec* se_subplan_root = NULL);
  void release_multi_part_shuffle_info();

public:
  common::ObArrayWrap<ObTableDMLCtx> table_dml_ctxs_;
  ObMultiDMLPlanMgr multi_dml_plan_mgr_;
  ObDMLMiniTaskExecutor mini_task_executor_;
  common::ObNewRow returning_row_;
  common::ObRowStore returning_row_store_;
  common::ObRowStore::Iterator returning_row_iterator_;
  common::ObIAllocator& allocator_;
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_MODIFY_H_ */
