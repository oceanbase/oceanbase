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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_MODIFY_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_MODIFY_OP_

#include "sql/engine/ob_operator.h"
#include "sql/engine/dml/ob_table_modify.h"

namespace oceanbase {
namespace sql {

class ObTableModifyOp;
class ObTableModifySpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableModifySpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  virtual ~ObTableModifySpec()
  {}

  inline void set_from_multi_table_dml(bool from_multi_table_dml)
  {
    from_multi_table_dml_ = from_multi_table_dml;
  }
  inline bool from_multi_table_dml() const
  {
    return from_multi_table_dml_;
  }

  virtual bool is_dml_operator() const override
  {
    return true;
  }

  int init_column_ids_count(int64_t count)
  {
    return column_ids_.init(count);
  }

  int init_primary_key_ids(int64_t rowkey_cnt)
  {
    return primary_key_ids_.init(rowkey_cnt);
  }
  int add_primary_key_id(uint64_t rowkey_id)
  {
    return primary_key_ids_.push_back(rowkey_id);
  }
  int set_primary_key_ids(const ObIArray<uint64_t>& pri_col_ids)
  {
    return primary_key_ids_.assign(pri_col_ids);
  }
  int primary_key_count() const
  {
    return primary_key_ids_.count();
  }

  int init_column_infos_count(int64_t count)
  {
    return column_infos_.init(count);
  }
  int add_column_info(const ColumnContent& column);

  int init_column_conv_info_count(int64_t count)
  {
    return column_conv_infos_.init(count);
  }

  int add_column_conv_info(const ObExprResType& res_type, const uint64_t column_flags, common::ObIAllocator& allocator,
      const common::ObIArray<common::ObString>* str_values = NULL);
  int init_foreign_key_args(int64_t fk_count);
  int add_foreign_key_arg(const ObForeignKeyArg& fk_arg);

  const ObForeignKeyArgArray& get_fk_args() const
  {
    return fk_args_;
  }

  int32_t get_column_idx(uint64_t column_id);
  int add_column_id(uint64_t column_id);

  uint64_t get_table_id() const
  {
    return table_id_;
  }
  uint64_t get_index_tid() const
  {
    return index_tid_;
  }
  share::schema::ObTableDMLParam& get_table_param()
  {
    return table_param_;
  }
  const common::ObIArray<uint64_t>& get_column_ids()
  {
    return column_ids_;
  }

  bool need_skip_log_user_error() const
  {
    return need_skip_log_user_error_;
  }
  void set_need_skip_log_user_error(bool need_skip_log_user_error)
  {
    need_skip_log_user_error_ = need_skip_log_user_error;
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

public:
  virtual bool has_foreign_key() const;

public:
  uint64_t table_id_;
  uint64_t index_tid_;
  bool is_ignore_;
  bool from_multi_table_dml_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> primary_key_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> column_infos_;
  common::ObFixedArray<ObColumnConvInfo, common::ObIAllocator> column_conv_infos_;
  // output row for storage process, for prepare_next_storage_row() interface.
  ExprFixedArray storage_row_output_;
  ExprFixedArray returning_exprs_;
  ExprFixedArray check_constraint_exprs_;
  ObForeignKeyArgArray fk_args_;
  uint64_t tg_event_;
  share::schema::ObTableDMLParam table_param_;
  bool need_filter_null_row_;
  DistinctType distinct_algo_;
  bool gi_above_;
  bool is_returning_;
  ObExpr* lock_row_flag_expr_;
  bool is_pdml_index_maintain_;
  bool need_skip_log_user_error_;
  bool table_location_uncertain_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableModifySpec);
};

class ObTableModifyOpInput : public ObOpInput {
public:
  friend class ObTableModifyOp;
  OB_UNIS_VERSION_V(1);

public:
  ObTableModifyOpInput(ObExecContext& ctx, const ObOpSpec& spec)
      : ObOpInput(ctx, spec), location_idx_(common::OB_INVALID_INDEX), part_infos_()
  {}
  virtual ~ObTableModifyOpInput()
  {}
  virtual void reset()
  {
    location_idx_ = common::OB_INVALID_INDEX;
    part_infos_.reset();
  }
  virtual int init(ObTaskInfo& task_info) override;
  inline int64_t get_location_idx() const
  {
    return location_idx_;
  }
  inline void set_location_idx(int64_t location_idx)
  {
    location_idx_ = location_idx;
  }
  /**
   * @brief set allocator which is used for deserialize, but not all objects will use allocator
   * while deserializing, so you can override it if you need.
   */
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator)
  {
    part_infos_.set_allocator(allocator);
  }
  const ObTableModifySpec& get_spec() const
  {
    return static_cast<const ObTableModifySpec&>(spec_);
  }
  TO_STRING_KV(K_(location_idx), K_(part_infos));

private:
  int64_t location_idx_;
  common::ObFixedArray<DMLPartInfo, common::ObIAllocator> part_infos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableModifyOpInput);
};

class ObTableModifyOp : public ObOperator {
public:
  class DMLRowIterator : public common::ObNewRowIterator {
  public:
    DMLRowIterator(ObExecContext& ctx, ObTableModifyOp& op) : ctx_(ctx), op_(op)
    {}
    virtual ~DMLRowIterator()
    {
      reset();
    }
    int init();
    int get_next_row(common::ObNewRow*& row);
    virtual int get_next_rows(common::ObNewRow*& row, int64_t& row_count);
    virtual void reset();

    // create project_row_ cells.
    int setup_project_row(const int64_t cnt);

  protected:
    ObExecContext& ctx_;
    ObTableModifyOp& op_;
    common::ObNewRow project_row_;
  };

  class ForeignKeyHandle {
  public:
    struct ObFkRowResInfo {
      ObExpr* rt_expr_;
      ObDatum ori_datum_;
      TO_STRING_KV(K_(rt_expr), K_(ori_datum));
    };
    static int do_handle_old_row(
        ObTableModifyOp& modify_op, const ObForeignKeyArgArray& fk_args, const ObExprPtrIArray& old_row);
    static int do_handle_new_row(
        ObTableModifyOp& modify_op, const ObForeignKeyArgArray& fk_args, const ObExprPtrIArray& new_row);
    static int do_handle(ObTableModifyOp& modify_op, const ObForeignKeyArgArray& fk_args,
        const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row);

  private:
    static int value_changed(ObTableModifyOp& op, const common::ObIArray<ObForeignKeyColumn>& columns,
        const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row, bool& has_changed);
    static int check_exist(
        ObTableModifyOp& modify_op, const ObForeignKeyArg& fk_arg, const ObExprPtrIArray& row, bool expect_zero);
    static int cascade(ObTableModifyOp& modify_op, const ObForeignKeyArg& fk_arg, const ObExprPtrIArray& old_row,
        const ObExprPtrIArray& new_row);
    static int gen_set(ObEvalCtx& eval_ctx, char* buf, int64_t len, int64_t& pos,
        const common::ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row,
        const common::ObObjPrintParams& print_params);
    static int gen_where(ObEvalCtx& eval_ctx, char* buf, int64_t len, int64_t& pos,
        const common::ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row,
        const common::ObObjPrintParams& print_params);
    static int gen_column_value(ObEvalCtx& ctx, char* buf, int64_t len, int64_t& pos,
        const common::ObIArray<ObForeignKeyColumn>& columns, const ObExprPtrIArray& row, const char* delimiter,
        const common::ObObjPrintParams& print_params, bool forbid_null);
    static int is_self_ref_row(
        ObEvalCtx& ctx, const ObExprPtrIArray& row, const ObForeignKeyArg& fk_arg, bool& is_self_ref);
  };

public:
  ObTableModifyOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObTableModifyOp()
  {}

  virtual int switch_iterator() override;

  int is_valid();

  sql::ObSQLSessionInfo::StmtSavedValue& get_saved_session()
  {
    if (NULL == saved_session_) {
      saved_session_ = new (saved_session_buf_) sql::ObSQLSessionInfo::StmtSavedValue();
    }
    return *saved_session_;
  }

  const ObTableModifySpec& get_spec() const
  {
    return static_cast<const ObTableModifySpec&>(spec_);
  }
  ObTableModifyOpInput* get_input() const
  {
    return static_cast<ObTableModifyOpInput*>(input_);
  }

  virtual void destroy() override
  {
    if (rowkey_dist_ctx_ != nullptr) {
      rowkey_dist_ctx_->~SeRowkeyDistCtx();
      rowkey_dist_ctx_ = nullptr;
    }
    returning_datum_iter_.reset();
    returning_datum_store_.reset();
    ObOperator::destroy();
  }

public:
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
  int check_stack();
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
  const ObObjPrintParams get_obj_print_params()
  {
    return CREATE_OBJ_PRINT_PARAM(ctx_.get_my_session());
  }
  int init_foreign_key_operation();
  int check_rowkey_is_null(const ObExprPtrIArray& row, int64_t rowkey_cnt, bool& is_null) const;
  void log_user_error_inner(
      int ret, int64_t col_idx, int64_t row_num, const ObIArray<ColumnContent>& column_infos) const;
  // first pk count cell of %row is pk.
  int check_rowkey_whether_distinct(const ObExprPtrIArray& row, int64_t rowkey_cnt, DistinctType distinct_algo,
      SeRowkeyDistCtx*& rowkey_dist_ctx, bool& is_dist);

  int calc_part_id(const ObExpr* calc_part_id_expr, ObIArray<int64_t>& part_ids, int64_t& part_idx);
  // Prepare next row for storage process, used in DMLRowIterator to supply rows for storage.
  // The old engine get rows from operator get_next_row() interface, but the operator has no
  // output expressions, we can not do this in static typing engine.
  //
  // %output may point to MY_SPEC.storage_row_output_ or not.
  // Update operator need output old row and new row here, %output changes.
  virtual int prepare_next_storage_row(const ObExprPtrIArray*& output)
  {
    // the default implement same with get_next_row()
    output = &get_spec().storage_row_output_;
    return ObOperator::get_next_row();
  }

protected:
  OperatorOpenOrder get_operator_open_order() const;
  virtual int inner_open();
  virtual int inner_close();

  // project expressions to old style row, allocate cells from ctx_.get_allocator() if needed.
  int project_row(ObExpr* const* exprs, const int64_t cnt, common::ObNewRow& row) const;
  int project_row(const ObExprPtrIArray& expr_row, common::ObNewRow& row) const
  {
    return project_row(expr_row.get_data(), expr_row.count(), row);
  }
  // project datum to old style row.
  int project_row(const ObDatum* datums, ObExpr* const* exprs, const int64_t cnt, common::ObNewRow& row) const;
  int lock_row(
      const ObExprPtrIArray& row, storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey) const;

  template <class UpdateOp>
  int check_updated_value(UpdateOp& update_op, const common::ObIArrayWrap<ColumnContent>& assign_columns,
      const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row, bool& is_updated);

  int check_row_value(bool& updated, const common::ObIArrayWrap<ColumnContent>& update_column_infos,
      const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row);

  int mark_lock_row_flag(int64_t flag);

  int check_row_null(const ObExprPtrIArray& row, const common::ObIArray<ColumnContent>& column_infos) const;
  int set_autoinc_param_pkey(const common::ObPartitionKey& pkey) const;
  int get_part_location(const ObPhyTableLocation& table_location, const share::ObPartitionReplicaLocation*& out);
  int get_part_location(common::ObIArray<DMLPartInfo>& part_keys);

  int get_gi_task();
  // filtered if filter return false value. (move from ObPhyOperator).
  // eval all exprs in [%beg_idx, %end_idx) of %check_constraint_exprs,
  // if both %beg_idx and %end_idx are OB_INVALID_ID, eval all exprs in
  // %check_constraint_exprs.
  int filter_row_for_check_cst(const ExprFixedArray& check_constraint_exprs, bool& filtered,
      int64_t beg_idx = OB_INVALID_ID, int64_t end_idx = OB_INVALID_ID) const;

  // datum will be resotred according to %fk_self_ref_row_res_infos_.
  // resotre is necessary when there are forgien key self reference.
  // see ObTableModifyOp::ForgienKeyHandle::do_handle().
  // for now, this func is only needed in update/multi update operator.
  int restore_and_reset_fk_res_info();

  bool init_returning_store();

public:
  common::ObMySQLProxy* sql_proxy_;
  observer::ObInnerSQLConnection* inner_conn_;
  uint64_t tenant_id_;
  observer::ObInnerSQLConnection::SavedValue saved_conn_;
  bool is_nested_session_;
  bool foreign_key_checks_;
  bool need_close_conn_;
  SeRowkeyDistCtx* rowkey_dist_ctx_;

  // trigger
  ObNewRow tg_old_row_;
  ObNewRow tg_new_row_;
  common::ObArrayHelper<common::ObObjParam> tg_when_point_params_;
  common::ObArrayHelper<common::ObObjParam> tg_stmt_point_params_;
  common::ObArrayHelper<common::ObObjParam> tg_row_point_params_;
  common::ObObjParam* tg_all_params_;

  // row for locking, used in lock_row() to convert ObExpr array to old style row.
  mutable common::ObNewRow lock_row_;
  bool iter_end_;

  ObChunkDatumStore returning_datum_store_;
  ObChunkDatumStore::Iterator returning_datum_iter_;

private:
  ObSQLSessionInfo::StmtSavedValue* saved_session_;
  char* saved_session_buf_[sizeof(ObSQLSessionInfo::StmtSavedValue)];
  // when got forgien key self reference, need to change row.
  // see ObTableModifyOp::ForeignKeyHandle::do_handle()
  ObSEArray<ForeignKeyHandle::ObFkRowResInfo, 32> fk_self_ref_row_res_infos_;

  // trigger
  static const int64_t ALL_PARAM_COUNT;
  static const int64_t PARAM_OLD_IDX;
  static const int64_t PARAM_NEW_IDX;
  static const int64_t PARAM_EVENT_IDX;
  static const int64_t WHEN_POINT_PARAM_OFFSET;
  static const int64_t WHEN_POINT_PARAM_COUNT;
  static const int64_t STMT_POINT_PARAM_OFFSET;
  static const int64_t STMT_POINT_PARAM_COUNT;
  static const int64_t ROW_POINT_PARAM_OFFSET;
  static const int64_t ROW_POINT_PARAM_COUNT;
  // used by check_rowkey_whether_distinct
  static const int64_t MIN_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024;
  static const int64_t MAX_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024 * 1024;
};

template <class UpdateOp>
int ObTableModifyOp::check_updated_value(UpdateOp& update_op, const common::ObIArrayWrap<ColumnContent>& assign_columns,
    const ObExprPtrIArray& old_row, const ObExprPtrIArray& new_row, bool& is_updated)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_row_value(is_updated, assign_columns, old_row, new_row))) {
    SQL_ENG_LOG(WARN, "check row value failed", K(ret));
  } else if (is_updated) {
    update_op.inc_changed_rows();
    update_op.inc_affected_rows();
  }
  update_op.inc_found_rows();
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif
