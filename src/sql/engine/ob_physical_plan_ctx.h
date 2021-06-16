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

#ifndef OCEANBASE_SQL_OB_PHYSICAL_PLAN_CTX_H
#define OCEANBASE_SQL_OB_PHYSICAL_PLAN_CTX_H
#include "share/ob_autoincrement_param.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "common/ob_hint.h"
#include "common/ob_field.h"
#include "common/ob_clock_generator.h"
#include "storage/transaction/ob_trans_define.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_2d_array.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/engine/user_defined_function/ob_udf_ctx_mgr.h"
#include "sql/engine/expr/ob_expr.h"
namespace oceanbase {
namespace sql {
class ObResultSet;
class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObExecContext;
class ObUdfCtxMgr;
class ObPxDmlRowInfo;

struct PartParamIdxArray {
  PartParamIdxArray() : part_id_(-1), part_param_idxs_()
  {}
  TO_STRING_KV(K_(part_id), K_(part_param_idxs));
  int64_t part_id_;
  ObFixedArray<int64_t, common::ObIAllocator> part_param_idxs_;
};
typedef common::ObFixedArray<PartParamIdxArray, common::ObIAllocator> BatchParamIdxArray;
typedef common::ObFixedArray<ObImplicitCursorInfo, common::ObIAllocator> ImplicitCursorInfoArray;

struct ObRemoteSqlInfo {
  ObRemoteSqlInfo() : use_ps_(false), is_batched_stmt_(false), remote_sql_(), ps_params_(nullptr)
  {}

  TO_STRING_KV(K_(use_ps), K_(is_batched_stmt), K_(remote_sql), KPC_(ps_params));

  bool use_ps_;
  bool is_batched_stmt_;
  common::ObString remote_sql_;
  ParamStore* ps_params_;
};

class ObPhysicalPlanCtx {
  OB_UNIS_VERSION(1);

public:
  typedef common::Ob2DArray<common::ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false>
      ParamStore;
  explicit ObPhysicalPlanCtx(common::ObIAllocator& allocator);
  virtual ~ObPhysicalPlanCtx();
  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline void set_show_seed(bool show_seed)
  {
    is_show_seed_ = show_seed;
  }
  inline uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  inline bool get_show_seed() const
  {
    return is_show_seed_;
  }
  inline void set_tenant_schema_version(const int64_t version)
  {
    tenant_schema_version_ = version;
  }
  inline int64_t get_tenant_schema_version() const
  {
    return tenant_schema_version_;
  }
  /**
   * @brief: set the timestamp when the execution of this plan should time out
   * @param: ts_timeout_us [in] the microseconds timeout
   */
  inline void set_timeout_timestamp(const int64_t ts_timeout_us)
  {
    ts_timeout_us_ = ts_timeout_us;
  }
  inline int64_t get_timeout_timestamp() const
  {
    return ts_timeout_us_;
  }
  inline int64_t get_ps_timeout_timestamp() const
  {
    return ts_timeout_us_ - ESTIMATE_PS_RESERVE_TIME;
  }
  inline int64_t get_trans_timeout_timestamp() const
  {
    return ts_timeout_us_ - ESTIMATE_TRANS_RESERVE_TIME;
  }
  inline bool is_timeout(int64_t* remain_us = NULL) const
  {
    int64_t now = common::ObClockGenerator::getClock();
    if (remain_us != NULL) {
      if (OB_LIKELY(ts_timeout_us_ > 0)) {
        *remain_us = ts_timeout_us_ - now;
      } else {
        *remain_us = INT64_MAX;  // no timeout
      }
    }
    return (ts_timeout_us_ > 0 && now > ts_timeout_us_);
  }
  // snapshot timestamp used for transaction set consistency check
  inline int64_t get_tsc_timestamp() const
  {
    return tsc_snapshot_timestamp_;
  }
  inline void set_tsc_timestamp(int64_t ts)
  {
    tsc_snapshot_timestamp_ = ts;
  }
  inline void set_consistency_level(const common::ObConsistencyLevel consistency)
  {
    consistency_level_ = consistency;
  }
  inline common::ObConsistencyLevel get_consistency_level() const
  {
    return consistency_level_;
  }
  // param store
  int reserve_param_space(int64_t param_count);
  const ParamStore& get_param_store() const
  {
    return param_store_;
  }
  ParamStore& get_param_store_for_update()
  {
    return param_store_;
  }
  const common::ObObjParam* get_param_mem() const
  {
    if (param_store_.count() <= 0) {
      return nullptr;
    } else {
      return &param_store_.at(0);
    }
  }
  DatumParamStore& get_datum_param_store()
  {
    return datum_param_store_;
  }
  const DatumParamStore& get_datum_param_store() const
  {
    return datum_param_store_;
  }
  const ObIArray<char*>& get_param_frame_ptrs() const
  {
    return param_frame_ptrs_;
  }
  int init_datum_param_store();
  void reset_datum_param_store()
  {
    param_store_.reuse();
    datum_param_store_.reuse();
    param_frame_ptrs_.reuse();
    param_frame_capacity_ = 0;
  }
  int extend_datum_param_store(DatumParamStore& ext_datum_store);
  ObRemoteSqlInfo& get_remote_sql_info()
  {
    return remote_sql_info_;
  }
  bool is_terminate(int& ret) const;
  void set_cur_time(const int64_t& session_val)
  {
    cur_time_.set_timestamp(session_val);
  }
  void set_cur_time(const int64_t& session_val, const ObSQLSessionInfo& session);
  const common::ObObj& get_cur_time() const
  {
    return cur_time_;
  }
  int64_t get_cur_time_tardy_value() const
  {
    return cur_time_.get_datetime() + DELTA_TARDY_TIME_US;
  }
  bool has_cur_time() const
  {
    return common::ObTimestampType == cur_time_.get_type();
  }
  void set_merging_frozen_time(const common::ObPreciseDateTime& val)
  {
    merging_frozen_time_.set_timestamp(val);
  }
  const common::ObObj& get_merging_frozen_time() const
  {
    return merging_frozen_time_;
  }
  bool has_merging_frozen_time() const
  {
    return common::ObTimestampType == merging_frozen_time_.get_type();
  }
  char* get_message() const
  {
    return const_cast<char*>(message_);
  }
  void set_message(const char* message)
  {
    snprintf(message_, common::MSG_SIZE, "%s", message);
  }
  int64_t get_affected_rows() const
  {
    return affected_rows_;
  }
  void set_affected_rows(int64_t affected_rows)
  {
    affected_rows_ = affected_rows;
  }
  inline void add_affected_rows(int64_t affected_rows)
  {
    affected_rows_ += affected_rows;
  }
  int64_t get_found_rows() const
  {
    return found_rows_;
  }
  void set_found_rows(int64_t found_rows)
  {
    found_rows_ = found_rows;
  }
  /*void set_start_trans_param(const transaction::ObStartTransParam &start_trans_param)
  {
    start_trans_param_ = start_trans_param;
  }*/
  transaction::ObStartTransParam& get_start_trans_param()
  {
    return start_trans_param_;
  }

  inline void set_autoinc_cache_handle(share::CacheHandle* handle)
  {
    autoinc_cache_handle_ = handle;
  }
  inline share::CacheHandle* get_autoinc_cache_handle() const
  {
    return autoinc_cache_handle_;
  }
  inline int set_autoinc_params(common::ObIArray<share::AutoincParam>& autoinc_params)
  {
    return autoinc_params_.assign(autoinc_params);
  }
  inline common::ObIArray<share::AutoincParam>& get_autoinc_params()
  {
    return autoinc_params_;
  }
  inline uint64_t calc_last_insert_id_session()
  {
    if (last_insert_id_cur_stmt_ > 0) {
      last_insert_id_session_ = last_insert_id_cur_stmt_;
      last_insert_id_changed_ = true;
    }
    return last_insert_id_session_;
  }
  inline void set_last_insert_id_session(const uint64_t last_insert_id)
  {
    last_insert_id_session_ = last_insert_id;
  }
  inline uint64_t get_last_insert_id_session() const
  {
    return last_insert_id_session_;
  }
  inline uint64_t calc_last_insert_id_to_client()
  {
    if (0 == last_insert_id_to_client_) {
      last_insert_id_to_client_ = last_insert_id_cur_stmt_ > 0
                                      ? last_insert_id_cur_stmt_
                                      : (last_insert_id_with_expr_ ? last_insert_id_session_ : autoinc_col_value_);
    }
    return last_insert_id_to_client_;
  }
  inline void set_last_insert_id_to_client(const uint64_t last_insert_id)
  {
    last_insert_id_to_client_ = last_insert_id;
  }
  inline uint64_t get_last_insert_id_to_client()
  {
    return last_insert_id_to_client_;
  }
  inline void record_last_insert_id_cur_stmt()
  {
    if (0 == last_insert_id_cur_stmt_ && autoinc_id_tmp_ > 0) {
      last_insert_id_cur_stmt_ = autoinc_id_tmp_;
    }
  }
  inline void set_last_insert_id_cur_stmt(const uint64_t last_insert_id)
  {
    last_insert_id_cur_stmt_ = last_insert_id;
  }
  inline uint64_t get_last_insert_id_cur_stmt()
  {
    return last_insert_id_cur_stmt_;
  }
  inline void set_autoinc_id_tmp(const uint64_t autoinc_id)
  {
    autoinc_id_tmp_ = autoinc_id;
  }
  inline uint64_t get_autoinc_id_tmp()
  {
    return autoinc_id_tmp_;
  }
  inline void set_autoinc_col_value(const uint64_t autoinc_value)
  {
    autoinc_col_value_ = autoinc_value;
  }
  inline uint64_t get_autoinc_col_value()
  {
    return autoinc_col_value_;
  }
  inline void set_last_insert_id_with_expr(const bool with_expr)
  {
    last_insert_id_with_expr_ = with_expr;
  }
  inline bool get_last_insert_id_with_expr()
  {
    return last_insert_id_with_expr_;
  }
  inline void set_last_insert_id_changed(const bool changed)
  {
    last_insert_id_changed_ = changed;
  }
  inline bool get_last_insert_id_changed() const
  {
    return last_insert_id_changed_;
  }
  void set_phy_plan(const ObPhysicalPlan* phy_plan);
  inline const ObPhysicalPlan* get_phy_plan() const
  {
    return phy_plan_;
  }
  inline void set_or_expand_transformed(const bool is_or_expand_transformed)
  {
    is_or_expand_transformed_ = is_or_expand_transformed;
  }
  inline bool get_or_expand_transformed() const
  {
    return is_or_expand_transformed_;
  }
  inline void reset_for_quick_retry()
  {
    affected_rows_ = 0;
    row_matched_count_ = 0;
    row_duplicated_count_ = 0;
    row_deleted_count_ = 0;
  }
  inline void set_bind_array_count(int64_t bind_array_count)
  {
    bind_array_count_ = bind_array_count;
  }
  inline int64_t get_bind_array_count() const
  {
    return bind_array_count_;
  }

  // current index for array binding parameters.
  // CAUTION: this index only used in static typing engine's operators && expressions,
  // the old engine get it from ObExprCtx::cur_array_index_
  void set_bind_array_idx(const int64_t idx)
  {
    bind_array_idx_ = idx;
  }
  int64_t get_bind_array_idx() const
  {
    return bind_array_idx_;
  }
  void inc_bind_array_idx()
  {
    bind_array_idx_++;
  }

  void set_worker_count(int64_t worker_count)
  {
    unsed_worker_count_since_222rel_ = worker_count;
  }
  inline void set_exec_ctx(const ObExecContext* exec_ctx)
  {
    exec_ctx_ = exec_ctx;
  }
  void set_error_ignored(bool ignored)
  {
    is_error_ignored_ = ignored;
  }
  bool is_error_ignored() const
  {
    return is_error_ignored_;
  }
  void set_select_into(bool is_select_into)
  {
    is_select_into_ = is_select_into;
  }
  bool is_select_into() const
  {
    return is_select_into_;
  }
  void set_is_result_accurate(bool is_accurate)
  {
    is_result_accurate_ = is_accurate;
  }
  bool is_result_accurate() const
  {
    return is_result_accurate_;
  }
  void set_foreign_key_checks(bool foreign_key_checks)
  {
    foreign_key_checks_ = foreign_key_checks;
  }
  bool need_foreign_key_checks() const
  {
    return foreign_key_checks_;
  }
  inline bool is_affect_found_row() const
  {
    return is_affect_found_row_;
  }
  inline void set_is_affect_found_row(bool is_affect_found_row)
  {
    is_affect_found_row_ = is_affect_found_row;
  }
  int sync_last_value_local();
  int sync_last_value_global();
  int set_row_matched_count(int64_t row_count);
  inline void add_row_matched_count(int64_t row_count)
  {
    row_matched_count_ += row_count;
  }
  int64_t get_row_matched_count() const
  {
    return row_matched_count_;
  }
  int set_row_duplicated_count(int64_t row_count);
  inline void add_row_duplicated_count(int64_t row_count)
  {
    row_duplicated_count_ += row_count;
  }
  int64_t get_row_duplicated_count() const
  {
    return row_duplicated_count_;
  }
  int set_row_deleted_count(int64_t row_count);
  inline void add_row_deleted_count(int64_t row_count)
  {
    row_deleted_count_ += row_count;
  }
  int64_t get_row_deleted_count() const
  {
    return row_deleted_count_;
  }
  void set_expr_op_size(int64_t expr_op_size)
  {
    expr_op_size_ = expr_op_size;
  }
  int64_t get_expr_op_size() const
  {
    return expr_op_size_;
  }
  void set_ignore_stmt(bool is_ignore)
  {
    is_ignore_stmt_ = is_ignore;
  }
  bool is_ignore_stmt() const
  {
    return is_ignore_stmt_;
  }
  bool is_plain_select_stmt() const;
  ObTableScanStat& get_table_scan_stat()
  {
    return table_scan_stat_;
  }
  void set_table_row_count_list_capacity(uint32_t capacity)
  {
    table_row_count_list_.set_capacity(capacity);
  }
  ObIArray<ObTableRowCount>& get_table_row_count_list()
  {
    return table_row_count_list_;
  }
  inline bool& get_is_multi_dml()
  {
    return is_multi_dml_;
  }
  inline bool is_new_engine()
  {
    return is_new_engine_;
  }
  int set_batched_stmt_partition_ids(ObIArray<int64_t>& partition_ids);
  int assign_batch_stmt_param_idxs(const BatchParamIdxArray& param_idxs);
  inline const BatchParamIdxArray& get_batched_stmt_param_idxs() const
  {
    return batched_stmt_param_idxs_;
  }
  inline int create_implicit_cursor_infos(int64_t cursor_count)
  {
    return implicit_cursor_infos_.prepare_allocate(cursor_count);
  }
  const ImplicitCursorInfoArray& get_implicit_cursor_infos() const
  {
    return implicit_cursor_infos_;
  }
  int merge_implicit_cursor_info(const ObImplicitCursorInfo& implicit_cursor_info);
  int merge_implicit_cursors(const common::ObIArray<ObImplicitCursorInfo>& implicit_cursors);
  void reset_cursor_info();
  void set_cur_stmt_id(int64_t cur_stmt_id)
  {
    cur_stmt_id_ = cur_stmt_id;
  }
  int64_t get_cur_stmt_id() const
  {
    return cur_stmt_id_;
  }
  int switch_implicit_cursor();
  const ObIArray<int64_t>* get_part_param_idxs(int64_t part_id) const;
  void set_large_query(bool v)
  {
    is_large_query_ = v;
  }
  bool is_large_query()
  {
    return is_large_query_;
  }
  void add_px_dml_row_info(const ObPxDmlRowInfo& dml_row_info);
  TO_STRING_KV("tenant_id", tenant_id_);

private:
  int extend_param_frame(const int64_t old_size);
  int reserve_param_frame(const int64_t capacity);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalPlanCtx);

private:
  static const int64_t ESTIMATE_PS_RESERVE_TIME = 100 * 1000;
  static const int64_t ESTIMATE_TRANS_RESERVE_TIME = 70 * 1000;
  // oracle calc time during running, not before running.
  // oracle datetime func has two categoris: sysdate/systimestamp, current_date/current_timestamp/localtimestamp
  // so we use `cur_time_` for first used-category, `cur_time_ + DELTA_TARDY_TIME_US` for second used-category.
  static const int64_t DELTA_TARDY_TIME_US = 5;
  common::ObIAllocator& allocator_;

private:
  /**
   * @note these member need serialize
   */
  int64_t tenant_id_;
  // used for TRANSACTION SET CONSISTENCY check
  int64_t tsc_snapshot_timestamp_;
  // only used when the sql contains fun like current_time
  common::ObObj cur_time_;  // used for session
  common::ObObj merging_frozen_time_;
  // execution timeout for this round of execution
  int64_t ts_timeout_us_;
  common::ObConsistencyLevel consistency_level_;
  ParamStore param_store_;
  DatumParamStore datum_param_store_;
  common::ObSEArray<char*, 1, common::ModulePageAllocator, true> param_frame_ptrs_;
  int64_t param_frame_capacity_;
  // used for function sys_view_bigint_param(idx), @note unused anymore
  common::ObSEArray<common::ObObj, 1, common::ModulePageAllocator, true> sys_view_bigint_params_;
  ObSQLMode sql_mode_;
  common::ObFixedArray<share::AutoincParam, common::ObIAllocator> autoinc_params_;
  // from session to remote; last_insert_id in session;
  uint64_t last_insert_id_session_;
  // error or warning message
  char message_[common::MSG_SIZE];
  // only for serialize expr_op_size_ in ObExecContext, which is still using old serialize macro,
  // so we can't add any member in its serialize func because of compat problem.
  // fortunately, there is no compat problem for ObPhysicalPlanCtx.
  int64_t expr_op_size_;
  bool is_ignore_stmt_;
  int64_t bind_array_count_;
  // current index for array binding parameters.
  // CAUTION: this index only used in static typing engine's operators && expressions,
  // the old engine get it from ObExprCtx::cur_array_index_
  int64_t bind_array_idx_;
  int64_t tenant_schema_version_;

private:
  /**
   * @note these member not need serialize
   */
  transaction::ObStartTransParam start_trans_param_;
  int64_t affected_rows_;
  // specified this plan whether affect found_row() result
  bool is_affect_found_row_;
  int64_t found_rows_;
  const ObPhysicalPlan* phy_plan_;
  // if current expr is in insert values, this is the expr's index in value row
  int64_t curr_col_index_;
  share::CacheHandle* autoinc_cache_handle_;
  // last_insert_id return to client
  uint64_t last_insert_id_to_client_;
  // first auto-increment value in current stmt
  uint64_t last_insert_id_cur_stmt_;
  // auto-increment value
  uint64_t autoinc_id_tmp_;
  // auto-increment column value
  uint64_t autoinc_col_value_;
  // set if last_insert_id(expr) called
  bool last_insert_id_with_expr_;
  // if updated, last_insert_id should update in session too
  bool last_insert_id_changed_;
  // use for mysql_info
  int64_t row_matched_count_;
  // duplicated or changed
  int64_t row_duplicated_count_;
  int64_t row_deleted_count_;
  bool is_error_ignored_;
  bool is_select_into_;
  bool is_result_accurate_;
  bool foreign_key_checks_;
  int64_t unsed_worker_count_since_222rel_;  // deprecated
  const ObExecContext* exec_ctx_;
  ObTableScanStat table_scan_stat_;
  common::ObFixedArray<ObTableRowCount, common::ObIAllocator> table_row_count_list_;  // (table_id, table_row_count)
                                                                                      // pairs
  BatchParamIdxArray batched_stmt_param_idxs_;
  ImplicitCursorInfoArray implicit_cursor_infos_;
  // implicit cursor current stmt id
  int64_t cur_stmt_id_;

  bool is_or_expand_transformed_;
  bool is_show_seed_;

  bool is_multi_dml_;

  bool is_new_engine_;
  ObRemoteSqlInfo remote_sql_info_;
  bool is_large_query_;
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_PHYSICAL_PLAN_CTX_H
