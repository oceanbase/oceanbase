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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_physical_plan.h"
#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"
#include "lib/alloc/malloc_hook.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "sql/engine/ob_phy_operator_factory.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_operator_factory.h"
#include "sql/ob_sql_mock_schema_utils.h"

namespace oceanbase {
using namespace common;
using namespace common::serialization;
using namespace share::schema;
using namespace lib;
namespace sql {

ObPhysicalPlan::ObPhysicalPlan(MemoryContext& mem_context /* = CURRENT_CONTEXT */)
    : ObCacheObject(T_CO_SQL_CRSR, mem_context),
      query_hint_(),
      main_query_(NULL),
      root_op_spec_(NULL),
      param_count_(0),
      signature_(0),
      field_columns_(mem_context.get_arena_allocator()),
      param_columns_(mem_context.get_arena_allocator()),
      autoinc_params_(allocator_),
      stmt_need_privs_(allocator_),
      stmt_ora_need_privs_(allocator_),
      vars_(allocator_),
      op_factory_(allocator_),
      sql_expression_factory_(allocator_),
      expr_op_factory_(allocator_),
      literal_stmt_type_(stmt::T_NONE),
      plan_type_(OB_PHY_PLAN_UNINITIALIZED),
      location_type_(OB_PHY_PLAN_UNINITIALIZED),
      require_local_execution_(false),
      use_px_(false),
      px_dop_(0),
      next_phy_operator_id_(0),
      next_expr_operator_id_(0),
      regexp_op_count_(0),
      like_op_count_(0),
      px_exchange_out_op_count_(0),
      is_sfu_(false),
      is_contains_assignment_(false),
      affected_last_insert_id_(false),
      is_affect_found_row_(false),
      has_top_limit_(false),
      is_wise_join_(false),
      contain_table_scan_(false),
      has_nested_sql_(false),
      session_id_(0),
      contain_oracle_trx_level_temporary_table_(false),
      contain_oracle_session_level_temporary_table_(false),
      concurrent_num_(0),
      max_concurrent_num_(ObMaxConcurrentParam::UNLIMITED),
      row_param_map_(allocator_),
      is_update_uniq_index_(false),
      is_contain_global_index_(false),
      base_constraints_(allocator_),
      strict_constrinats_(allocator_),
      non_strict_constrinats_(allocator_),
      expr_frame_info_(allocator_),
      stat_(),
      op_stats_(),
      is_returning_(false),
      is_late_materialized_(false),
      is_dep_base_table_(false),
      is_insert_select_(false),
      contain_paramed_column_field_(false),
      first_array_index_(OB_INVALID_INDEX),
      need_consistent_snapshot_(true),
      is_batched_multi_stmt_(false),
      is_new_engine_(false),
      use_pdml_(false),
      use_temp_table_(false),
      has_link_table_(false),
      mock_rowid_tables_(allocator_),
      need_serial_exec_(false),
      temp_sql_can_prepare_(false)
{}

ObPhysicalPlan::~ObPhysicalPlan()
{
  destroy();
}

void ObPhysicalPlan::reset()
{
  query_hint_.reset();
  main_query_ = NULL;
  root_op_spec_ = NULL;
  param_count_ = 0;
  signature_ = 0;
  field_columns_.reset();
  param_columns_.reset();
  autoinc_params_.reset();
  stmt_need_privs_.reset();
  stmt_ora_need_privs_.reset();
  vars_.reset();
  op_factory_.destroy();
  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
  literal_stmt_type_ = stmt::T_NONE;
  plan_type_ = OB_PHY_PLAN_UNINITIALIZED;
  location_type_ = OB_PHY_PLAN_UNINITIALIZED;
  require_local_execution_ = false;
  use_px_ = false;
  px_dop_ = 0;
  next_phy_operator_id_ = 0;
  next_expr_operator_id_ = 0;
  regexp_op_count_ = 0;
  like_op_count_ = 0;
  px_exchange_out_op_count_ = 0;
  is_sfu_ = false;
  is_contain_virtual_table_ = false;
  is_contain_inner_table_ = false;
  is_contains_assignment_ = false;
  affected_last_insert_id_ = false;
  is_affect_found_row_ = false;
  has_top_limit_ = false;
  is_wise_join_ = false;
  contain_table_scan_ = false;
  has_nested_sql_ = false;
  session_id_ = 0;
  contain_oracle_trx_level_temporary_table_ = false;
  contain_oracle_session_level_temporary_table_ = false;
  concurrent_num_ = 0;
  max_concurrent_num_ = ObMaxConcurrentParam::UNLIMITED;
  is_update_uniq_index_ = false;
  is_contain_global_index_ = false;
  loc_sensitive_hint_.reset();
  ObCacheObject::reset();
  is_returning_ = false;
  is_late_materialized_ = false;
  is_dep_base_table_ = false;
  is_insert_select_ = false;
  base_constraints_.reset();
  strict_constrinats_.reset();
  non_strict_constrinats_.reset();
  contain_paramed_column_field_ = false;
  first_array_index_ = OB_INVALID_INDEX;
  need_consistent_snapshot_ = true;
  is_batched_multi_stmt_ = false;
  temp_sql_can_prepare_ = false;
  is_new_engine_ = false;
#ifndef NDEBUG
  bit_set_.reset();
#endif
  use_pdml_ = false;
  use_temp_table_ = false;
  has_link_table_ = false;
  mock_rowid_tables_.reset();
  need_serial_exec_ = false;
}

void ObPhysicalPlan::destroy()
{
#ifndef NDEBUG
  bit_set_.reset();
#endif
  op_factory_.destroy();
  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
}

int ObPhysicalPlan::copy_common_info(ObPhysicalPlan& src)
{
  int ret = OB_SUCCESS;
  ObSqlExpressionFactory* factory = get_sql_expression_factory();
  if (OB_ISNULL(factory)) {
    LOG_WARN("expression factory is NULL", K(ret));
  } else {
    ObSqlExpression* expr = NULL;
    DLIST_FOREACH(node, src.get_pre_calc_exprs())
    {
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null");
      } else if (OB_FAIL(get_sql_expression_factory()->alloc(expr))) {
        LOG_WARN("fail to alloc sql expression", K(ret));
      } else if (OB_UNLIKELY(NULL == expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr allocated is null", K(ret));
      } else if (OB_FAIL(expr->assign(*node))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_FAIL(add_calculable_expr(expr))) {
        LOG_WARN("failed to add column");
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret)) {
      // copy regexp_op_count_
      set_regexp_op_count(src.regexp_op_count_);
      set_like_op_count(src.like_op_count_);
      set_px_exchange_out_op_count(src.px_exchange_out_op_count_);
      // copy others
      set_fetch_cur_time(src.get_fetch_cur_time());
      set_stmt_type(src.get_stmt_type());
      set_literal_stmt_type(src.get_literal_stmt_type());
      // copy plan_id/hint/privs
      object_id_ = src.object_id_;
      if (OB_FAIL(set_query_hint(src.get_query_hint()))) {
        LOG_WARN("Failed to copy query hint", K(ret));
      } else if (OB_FAIL(set_stmt_need_privs(src.get_stmt_need_privs()))) {
        LOG_WARN("Failed to deep copy", K(src.get_stmt_need_privs()), K(ret));
      } else {
      }  // do nothing
    }
  }
  return ret;
}

int ObPhysicalPlan::set_vars(const common::ObIArray<ObVarInfo>& vars)
{
  int ret = OB_SUCCESS;
  int64_t N = vars.count();
  if (N > 0 && OB_FAIL(vars_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve vars", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObVarInfo& var_info = vars.at(i);
    ObVarInfo clone_var_info;
    if (OB_FAIL(var_info.deep_copy(allocator_, clone_var_info))) {
      LOG_WARN("fail to deep copy var info", K(ret), K(var_info));
    } else if (OB_FAIL(vars_.push_back(clone_var_info))) {
      LOG_WARN("fail to push back vars", K(ret), K(clone_var_info));
    }
  }
  return ret;
}

int ObPhysicalPlan::add_phy_query(ObPhyOperator* phy_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_query)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(phy_query));
  } else {
    main_query_ = phy_query;
    LOG_DEBUG("add main query", K(phy_query));
  }
  return ret;
}

ObPhyOperator* ObPhysicalPlan::get_main_query() const
{
  return main_query_;
}

void ObPhysicalPlan::set_main_query(ObPhyOperator* query)
{
  main_query_ = query;
}

int ObPhysicalPlan::add_calculable_expr(ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  if (!pre_calc_exprs_.add_last(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add column");
  } else {
  }
  return ret;
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len) const
{
  return to_string(buf, buf_len, main_query_);
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len, ObPhyOperator* start_query) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_PARAM_NUM,
      param_count_,
      N_TABLE_SCHEMA_VERSION,
      dependency_tables_,
      K_(pre_calc_exprs),
      K_(plan_type),
      K_(location_type));

  // print main query first
  J_COMMA();
  BUF_PRINTF("\n");
  J_NAME(N_MAIN_QUERY);
  J_COLON();
  print_tree(buf, buf_len, pos, start_query);

  J_OBJ_END();
  return pos;
}

void ObPhysicalPlan::print_tree(char* buf, const int64_t buf_len, int64_t& pos, const ObPhyOperator* op)
{
  if (op != NULL) {
    BUF_PRINTF("\n");
    J_OBJ_START();
    // 1. operator content
    J_NAME(ob_phy_operator_type_str(op->get_type()));
    J_COLON();
    BUF_PRINTO(*op);
    // 3. children
    if (op->get_child_num() > 0) {
      J_COMMA();
      J_NAME(N_CHILDREN_OPS);
      J_COLON();
      J_ARRAY_START();
    }

    for (int32_t i = 0; i < op->get_child_num(); ++i) {
      if (i > 0) {
        J_COMMA();
      }
      print_tree(buf, buf_len, pos, op->get_child(i));
    }

    if (op->get_child_num() > 0) {
      J_ARRAY_END();
    }
    J_OBJ_END();
  } else {
    J_EMPTY_OBJ();
  }
}

int ObPhysicalPlan::init_params_info_str()
{
  int ret = common::OB_SUCCESS;
  int64_t N = params_info_.count();
  int64_t buf_len = N * ObParamInfo::MAX_STR_DES_LEN + 1;
  int64_t pos = 0;
  char* buf = (char*)allocator_.alloc(buf_len);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_PC_LOG(WARN, "fail to alloc memory for param info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (N - 1 != i) {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "{%d,%d,%d,%d,%d},",
                params_info_.at(i).flag_.need_to_check_type_,
                params_info_.at(i).flag_.need_to_check_bool_value_,
                params_info_.at(i).flag_.expected_bool_value_,
                params_info_.at(i).scale_,
                params_info_.at(i).type_))) {
          SQL_PC_LOG(WARN, "fail to buff_print param info", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "{%d,%d,%d,%d,%d}",
                params_info_.at(i).flag_.need_to_check_type_,
                params_info_.at(i).flag_.need_to_check_bool_value_,
                params_info_.at(i).flag_.expected_bool_value_,
                params_info_.at(i).scale_,
                params_info_.at(i).type_))) {
          SQL_PC_LOG(WARN, "fail to buff_print param info", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator_, ObString(pos, buf), stat_.param_infos_))) {
      SQL_PC_LOG(WARN, "fail to deep copy param infos", K(ret));
    }
  }

  return ret;
}

int ObPhysicalPlan::set_field_columns(const ColumnsFieldArray& fields)
{
  int ret = OB_SUCCESS;
  ObField field;
  WITH_CONTEXT(&mem_context_)
  {
    int64_t N = fields.count();
    if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
      OB_LOG(WARN, "fail to reserve field column", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObField& ofield = fields.at(i);
      LOG_DEBUG("ofield info", K(ofield));
      if (!contain_paramed_column_field_ && ofield.is_paramed_select_item_) {
        if (OB_ISNULL(ofield.paramed_ctx_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid paramed ctx", K(ofield.paramed_ctx_), K(i));
        } else if (ofield.paramed_ctx_->param_idxs_.count() > 0) {
          contain_paramed_column_field_ = true;
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(field.deep_copy(ofield, &allocator_))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(field_columns_.push_back(field))) {
        LOG_WARN("push back field columns failed", K(ret));
      } else {
        LOG_DEBUG("succ to push back field columns", K(field));
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::set_param_fields(const common::ParamsFieldArray& params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  WITH_CONTEXT(&mem_context_)
  {
    if (N > 0 && OB_FAIL(param_columns_.reserve(N))) {
      LOG_WARN("failed to reserved param field", K(ret));
    }
    ObField tmp_field;
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObField& param_field = params.at(i);
      if (OB_FAIL(tmp_field.deep_copy(param_field, &allocator_))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(param_columns_.push_back(tmp_field))) {
        LOG_WARN("push back field columns failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::set_autoinc_params(const ObIArray<share::AutoincParam>& autoinc_params)
{
  return autoinc_params_.assign(autoinc_params);
}

int ObPhysicalPlan::set_stmt_need_privs(const ObStmtNeedPrivs& stmt_need_privs)
{
  int ret = OB_SUCCESS;
  stmt_need_privs_.reset();
  if (OB_FAIL(stmt_need_privs_.deep_copy(stmt_need_privs, allocator_))) {
    LOG_WARN("Failed to deep copy ObStmtNeedPrivs", K_(stmt_need_privs));
  }
  return ret;
}

int ObPhysicalPlan::set_stmt_ora_need_privs(const ObStmtOraNeedPrivs& stmt_ora_need_privs)
{
  int ret = OB_SUCCESS;
  stmt_ora_need_privs_.reset();
  if (OB_FAIL(stmt_ora_need_privs_.deep_copy(stmt_ora_need_privs, allocator_))) {
    LOG_WARN("Failed to deep copy ObStmtNeedPrivs", K_(stmt_ora_need_privs));
  }
  return ret;
}

void ObPhysicalPlan::inc_large_querys()
{
  ATOMIC_INC(&(stat_.large_querys_));
}

void ObPhysicalPlan::inc_delayed_large_querys()
{
  ATOMIC_INC(&(stat_.delayed_large_querys_));
}

void ObPhysicalPlan::inc_delayed_px_querys()
{
  ATOMIC_INC(&(stat_.delayed_px_querys_));
}
int ObPhysicalPlan::init_operator_stats()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_stats_.init(&allocator_, next_phy_operator_id_))) {
    LOG_WARN("fail to init op_stats", K(ret));
  }
  return ret;
}

void ObPhysicalPlan::update_plan_stat(const ObAuditRecordData& record, const bool is_first, const bool is_evolution,
    const ObIArray<ObTableRowCount>* table_row_count_list)
{
  const int64_t current_time = ObTimeUtility::current_time();
  int64_t execute_count = 0;
  if (record.is_timeout()) {
    ATOMIC_INC(&(stat_.timeout_count_));
    ATOMIC_AAF(&(stat_.total_process_time_), record.get_process_time());
  } else {
    execute_count = ATOMIC_AAF(&stat_.execute_times_, 1);
    ATOMIC_AAF(&(stat_.total_process_time_), record.get_process_time());
    ATOMIC_AAF(&(stat_.disk_reads_), record.exec_record_.get_io_read_count());
    ATOMIC_AAF(&(stat_.direct_writes_), record.exec_record_.get_io_write_count());
    ATOMIC_AAF(&(stat_.buffer_gets_),
        2 * record.exec_record_.get_row_cache_hit() + 2 * record.exec_record_.get_bloom_filter_filts() +
            record.exec_record_.get_block_index_cache_hit() + record.exec_record_.get_block_cache_hit() +
            record.exec_record_.get_io_read_count());
    ATOMIC_AAF(&(stat_.application_wait_time_), record.exec_record_.get_application_time());
    ATOMIC_AAF(&(stat_.concurrency_wait_time_), record.exec_record_.get_concurrency_time());
    ATOMIC_AAF(&(stat_.user_io_wait_time_), record.exec_record_.get_user_io_time());
    ATOMIC_AAF(&(stat_.rows_processed_), record.return_rows_ + record.affected_rows_);
    ATOMIC_AAF(&(stat_.elapsed_time_), record.get_elapsed_time());
    ATOMIC_AAF(&(stat_.cpu_time_),
        record.get_elapsed_time() - record.exec_record_.wait_time_end_ -
            (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
    // ATOMIC_STORE(&(stat_.expected_worker_count_), record.expected_worker_cnt_);

    if (is_first) {
      ATOMIC_STORE(&(stat_.hit_count_), 0);
    } else {
      ATOMIC_INC(&(stat_.hit_count_));
    }

    if (record.get_elapsed_time() > GCONF.trace_log_slow_query_watermark) {
      ATOMIC_INC(&(stat_.slow_count_));
    }
    int64_t slowest_usec = ATOMIC_LOAD(&stat_.slowest_exec_usec_);
    if (slowest_usec < record.get_elapsed_time()) {
      ATOMIC_STORE(&(stat_.slowest_exec_usec_), record.get_elapsed_time());
      ATOMIC_STORE(&(stat_.slowest_exec_time_), current_time);
    }

    if (stat_.table_row_count_first_exec_ != NULL && table_row_count_list != NULL) {
      int64_t access_table_num = stat_.access_table_num_;
      int64_t max_index =
          std::min(access_table_num, std::min(table_row_count_list->count(), OB_MAX_TABLE_NUM_PER_STMT));
      if (is_first) {
        for (int64_t i = 0; i < max_index; ++i) {
          ATOMIC_STORE(&(stat_.table_row_count_first_exec_[i].op_id_), table_row_count_list->at(i).op_id_);
          ATOMIC_STORE(&(stat_.table_row_count_first_exec_[i].row_count_), table_row_count_list->at(i).row_count_);
          LOG_DEBUG("first add row stat", K(table_row_count_list->at(i)));
        }  // for end
      } else if (record.get_elapsed_time() > SLOW_QUERY_TIME_FOR_PLAN_EXPIRE) {
        for (int64_t i = 0; !is_expired() && i < max_index; ++i) {
          for (int64_t j = 0; !is_expired() && j < max_index; ++j) {
            // In some scenarios, such as parallel execution,
            // the order of row information storage in different execution tables may be different
            if (table_row_count_list->at(i).op_id_ == stat_.table_row_count_first_exec_[j].op_id_) {
              int64_t first_exec_row_count = ATOMIC_LOAD(&stat_.table_row_count_first_exec_[j].row_count_);
              if (first_exec_row_count == -1) {
                // do nothing
              } else if (check_if_is_expired(first_exec_row_count, table_row_count_list->at(i).row_count_)) {
                set_is_expired(true);
                LOG_INFO("plan is expired",
                    K(first_exec_row_count),
                    K(table_row_count_list->at(i)),
                    "current_elapsed_time",
                    record.get_elapsed_time(),
                    "plan_stat",
                    stat_);
              }
            }
          }  // for max_index end
        }    // for max_index end
      }
    }
  }
  ATOMIC_STORE(&(stat_.last_active_time_), current_time);
  if (is_evolution) {  // for spm
    ATOMIC_INC(&(stat_.bl_info_.executions_));
    ATOMIC_AAF(&(stat_.bl_info_.cpu_time_),
        record.get_elapsed_time() - record.exec_record_.wait_time_end_ -
            (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
  }
  if (stat_.is_bind_sensitive_ || stat_.enable_plan_expiration_) {
    int64_t pos = execute_count % ObPlanStat::MAX_SCAN_STAT_SIZE;
    ATOMIC_STORE(&(stat_.table_scan_stat_[pos].query_range_row_count_), record.table_scan_stat_.query_range_row_count_);
    ATOMIC_STORE(&(stat_.table_scan_stat_[pos].indexback_row_count_), record.table_scan_stat_.indexback_row_count_);
    ATOMIC_STORE(&(stat_.table_scan_stat_[pos].output_row_count_), record.table_scan_stat_.output_row_count_);
    if (is_first) {
      ATOMIC_STORE(&(stat_.first_exec_row_count_), record.table_scan_stat_.query_range_row_count_);
    }
  }

  if (!is_expired() && stat_.enable_plan_expiration_) {
    // if the first request is timeout, the execute_count is zero, the avg cpu time should be the cpu time
    if (record.is_timeout() || record.status_ == OB_SESSION_KILLED) {
      set_is_expired(true);
      LOG_INFO("query plan is expired due to execution timeout", K(stat_));
    } else if (execute_count == 0 || (execute_count % SLOW_QUERY_SAMPLE_SIZE) != 0) {
      // do nothing when query execution samples are not enough
    } else if (stat_.cpu_time_ <= SLOW_QUERY_TIME_FOR_PLAN_EXPIRE * stat_.execute_times_) {
      // do nothing for fast query
    } else if (is_plan_unstable()) {
      set_is_expired(true);
    }
  }
}

bool ObPhysicalPlan::is_plan_unstable()
{
  bool bret = false;
  int64_t exec_count = 0;
  int64_t total_index_back_rows = 0;
  int64_t total_query_range_rows = 0;
  int64_t first_query_range_rows = ATOMIC_LOAD(&stat_.first_exec_row_count_);
  if (first_query_range_rows != -1) {
    for (int64_t i = 0; i < SLOW_QUERY_SAMPLE_SIZE; ++i) {
      int64_t query_range_rows = ATOMIC_LOAD(&(stat_.table_scan_stat_[i].query_range_row_count_));
      int64_t index_back_rows = ATOMIC_LOAD(&(stat_.table_scan_stat_[i].indexback_row_count_));
      if (query_range_rows != -1) {
        exec_count++;
        total_query_range_rows += query_range_rows;
        total_index_back_rows += index_back_rows;
      }
    }
    int64_t total_access_cost = (total_query_range_rows + 10 * total_index_back_rows);
    if (total_access_cost <= SLOW_QUERY_ROW_COUNT_THRESOLD * exec_count) {
      // the query plan does not accesses too many rows in the average
    } else if (total_query_range_rows / exec_count > first_query_range_rows * 10) {
      // the average query range row count increases great
      bret = true;
      LOG_INFO("query plan is expired due to unstable performance",
          K(bret),
          K(stat_.execute_times_),
          K(exec_count),
          K(first_query_range_rows),
          K(total_query_range_rows),
          K(total_index_back_rows));
    }
  }
  return bret;
}

int64_t ObPhysicalPlan::get_evo_perf() const
{
  int64_t v = 0;
  if (0 == stat_.bl_info_.executions_) {
    v = 0;
  } else {
    v = stat_.bl_info_.cpu_time_ / stat_.bl_info_.executions_;
  }

  return v;
}

/**
 * At present, 3 indicators are used to eliminate the plan.
 * Only when these three conditions are met at the same time,will eliminate this plan
 *     1. The current number of rows exceeds the threshold (100 rows)
 *     2. Execution time exceeds the threshold (5ms)
 *     3. The ratio of the number of table scan rows to the number of original scan rows
 *        exceeds the threshold (2 times)
 *
 *     1. The reason for setting the threshold of the current number of rows is
 *        because the table scan function is not guaranteed to increase.
 *        In the case of frequent insertions and deletions, the original table scan function
 *        may be kept at a low value. At this time, the planned elimination will be very frequent.
 *     2. Setting a threshold can greatly alleviate the frequency of planned elimination
 */
inline bool ObPhysicalPlan::check_if_is_expired(
    const int64_t first_exec_row_count, const int64_t current_row_count) const
{
  bool ret_bool = false;
  if (current_row_count <= EXPIRED_PLAN_TABLE_ROW_THRESHOLD) {  // 100 rows
    ret_bool = false;
  } else {
    ret_bool = ((first_exec_row_count == 0 && current_row_count > 0) ||
                (first_exec_row_count > 0 && current_row_count / first_exec_row_count > TABLE_ROW_CHANGE_THRESHOLD));
  }
  return ret_bool;
}

int ObPhysicalPlan::inc_concurrent_num()
{
  int ret = OB_SUCCESS;
  int64_t concurrent_num = 0;
  int64_t new_num = 0;
  bool is_succ = false;
  if (max_concurrent_num_ == ObMaxConcurrentParam::UNLIMITED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current physical plan is unlimit", K(ret), K(max_concurrent_num_));
  } else {
    while (OB_SUCC(ret) && false == is_succ) {
      concurrent_num = ATOMIC_LOAD(&concurrent_num_);
      if (concurrent_num >= max_concurrent_num_) {
        ret = OB_REACH_MAX_CONCURRENT_NUM;
      } else {
        new_num = concurrent_num + 1;
        is_succ = ATOMIC_BCAS(&concurrent_num_, concurrent_num, new_num);
      }
    }
  }
  return ret;
}
void ObPhysicalPlan::dec_concurrent_num()
{
  ATOMIC_DEC(&concurrent_num_);
}

int ObPhysicalPlan::set_max_concurrent_num(int64_t max_concurrent_num)
{
  int ret = OB_SUCCESS;
  if (max_concurrent_num < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max_concurrent_num", K(ret), K(max_concurrent_num));
  } else {
    ATOMIC_STORE(&max_concurrent_num_, max_concurrent_num);
  }
  return ret;
}

int64_t ObPhysicalPlan::get_max_concurrent_num()
{
  return ATOMIC_LOAD(&max_concurrent_num_);
}

OB_SERIALIZE_MEMBER(FlashBackQueryItem, table_id_, time_val_);

// Because I haven't seen the remote execution of the force_trace_log processing,
// so it is temporarily not serialized
OB_SERIALIZE_MEMBER(ObPhysicalPlan,
    tenant_schema_version_,  // The field is not used during the execution period
    query_hint_.query_timeout_, query_hint_.read_consistency_, query_hint_.dummy_, is_sfu_, dependency_tables_,
    param_count_, plan_type_, signature_, stmt_type_, regexp_op_count_, literal_stmt_type_, like_op_count_,
    is_ignore_stmt_, object_id_, stat_.sql_id_, is_contain_inner_table_, is_update_uniq_index_, is_returning_,
    location_type_, use_px_, vars_, px_dop_, has_nested_sql_, stat_.enable_early_lock_release_, mock_rowid_tables_,
    use_pdml_, is_new_engine_, use_temp_table_);

int ObPhysicalPlan::set_table_locations(const ObTablePartitionInfoArray& infos)
{
  int ret = OB_SUCCESS;
  table_locations_.reset();
  table_locations_.set_allocator(&allocator_);
  if (OB_FAIL(table_locations_.init(infos.count()))) {
    LOG_WARN("fail to init table location count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
    ObTableLocation& tl = infos.at(i)->get_table_location();
    if (OB_FAIL(table_locations_.push_back(tl))) {
      LOG_WARN("fail to push table location", K(ret), K(i));
    }
  }

  return ret;
}

int ObPhysicalPlan::set_location_constraints(const ObIArray<LocationConstraint>& base_constraints,
    const ObIArray<ObPwjConstraint*>& strict_constraints, const ObIArray<ObPwjConstraint*>& non_strict_constraints)
{
  // deep copy location constraints
  int ret = OB_SUCCESS;
  if (base_constraints.count() > 0) {
    base_constraints_.reset();
    base_constraints_.set_allocator(&allocator_);
    if (OB_FAIL(base_constraints_.init(base_constraints.count()))) {
      LOG_WARN("failed to init base constraints", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < base_constraints.count(); ++i) {
      if (OB_FAIL(base_constraints_.push_back(base_constraints.at(i)))) {
        LOG_WARN("failed to push back element", K(ret), K(base_constraints.at(i)));
      } else { /*do nothing*/
      }
    }
  }

  if (OB_SUCC(ret) && strict_constraints.count() > 0) {
    strict_constrinats_.reset();
    strict_constrinats_.set_allocator(&allocator_);
    if (OB_FAIL(strict_constrinats_.init(strict_constraints.count()))) {
      LOG_WARN("failed to init strict constraints", K(ret));
    } else if (OB_FAIL(strict_constrinats_.prepare_allocate(strict_constraints.count()))) {
      LOG_WARN("failed to prepare allocate location constraints", K(ret));
    } else {
      ObPwjConstraint* cur_cons;
      for (int64_t i = 0; OB_SUCC(ret) && i < strict_constraints.count(); ++i) {
        if (OB_ISNULL(cur_cons = strict_constraints.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i));
        } else if (cur_cons->count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty array", K(ret));
        } else {
          strict_constrinats_.at(i).reset();
          strict_constrinats_.at(i).set_allocator(&allocator_);
          if (OB_FAIL(strict_constrinats_.at(i).init(cur_cons->count()))) {
            LOG_WARN("failed to init fixed array", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_cons->count(); ++j) {
            if (OB_FAIL(strict_constrinats_.at(i).push_back(cur_cons->at(j)))) {
              LOG_WARN("failed to push back element", K(ret), K(cur_cons->at(j)));
            } else { /*do nothing*/
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && non_strict_constraints.count() > 0) {
    non_strict_constrinats_.reset();
    non_strict_constrinats_.set_allocator(&allocator_);
    if (OB_FAIL(non_strict_constrinats_.init(non_strict_constraints.count()))) {
      LOG_WARN("failed to init strict constraints", K(ret));
    } else if (OB_FAIL(non_strict_constrinats_.prepare_allocate(non_strict_constraints.count()))) {
      LOG_WARN("failed to prepare allocate location constraints", K(ret));
    } else {
      ObPwjConstraint* cur_cons;
      for (int64_t i = 0; OB_SUCC(ret) && i < non_strict_constraints.count(); ++i) {
        if (OB_ISNULL(cur_cons = non_strict_constraints.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(i));
        } else if (cur_cons->count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty array", K(ret));
        } else {
          non_strict_constrinats_.at(i).reset();
          non_strict_constrinats_.at(i).set_allocator(&allocator_);
          if (OB_FAIL(non_strict_constrinats_.at(i).init(cur_cons->count()))) {
            LOG_WARN("failed to init fixed array", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_cons->count(); ++j) {
            if (OB_FAIL(non_strict_constrinats_.at(i).push_back(cur_cons->at(j)))) {
              LOG_WARN("failed to push back element", K(ret), K(cur_cons->at(j)));
            } else { /*do nothing*/
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    base_constraints_.reset();
    strict_constrinats_.reset();
    non_strict_constrinats_.reset();
  } else {
    LOG_DEBUG(
        "deep copied location constraints", K(base_constraints_), K(strict_constrinats_), K(non_strict_constrinats_));
  }

  return ret;
}

int ObPhysicalPlan::generate_mock_rowid_tables(share::schema::ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  mock_rowid_tables_.reset();
  ObArray<uint64_t> mock_rowid_tables;
  const ObTableSchema* table_schema = NULL;

  // generate table ids mocking rowid columns
  for (int i = 0; OB_SUCC(ret) && i < dependency_tables_.count(); i++) {
    if (DEPENDENCY_TABLE != dependency_tables_.at(i).object_type_) {
      // do nothing
    } else if (OB_FAIL(schema_guard.get_table_schema(dependency_tables_.at(i).object_id_, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table schema", K(ret));
    } else if (NULL == table_schema->get_column_schema(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)) {
      // do nothing
    } else if (OB_FAIL(mock_rowid_tables.push_back(dependency_tables_.at(i).object_id_))) {
      LOG_WARN("failed to push back element", K(ret));
    }
  }  // for end
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mock_rowid_tables_.assign(mock_rowid_tables))) {
      LOG_WARN("failed to assign to array", K(ret));
    } else {
      LOG_TRACE("mocked rowid tables", K(mock_rowid_tables_));
    }
  }
  return ret;
}

DEF_TO_STRING(FlashBackQueryItem)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id));
  J_KV(K_(time_val));
  J_OBJ_END();
  return pos;
}

int ObPhysicalPlan::alloc_op_spec(
    const ObPhyOperatorType type, const int64_t child_cnt, ObOpSpec*& op, const uint64_t op_id)
{
  int ret = OB_SUCCESS;
  UNUSED(op_id);
  if (type >= PHY_END || child_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), K(child_cnt));
  } else if (OB_FAIL(ObOperatorFactory::alloc_op_spec(allocator_, type, child_cnt, op))) {
    LOG_WARN("allocate operator spec failed", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL operator spec returned", K(ret));
  } else {
    uint32_t tmp_op_id = UINT32_MAX;
    if (OB_INVALID_ID != op_id) {
      tmp_op_id = op_id;
    } else {
      tmp_op_id = next_phy_operator_id_++;
    }
    op->id_ = tmp_op_id;
    op->plan_ = this;
  }
  return ret;
}

int64_t ObPhysicalPlan::get_pre_expr_ref_count() const
{
  int64_t ret = 0;
  if (OB_ISNULL(stat_.pre_cal_expr_handler_)) {
  } else {
    ret = stat_.pre_cal_expr_handler_->get_ref_count();
  }
  return ret;
}
void ObPhysicalPlan::inc_pre_expr_ref_count()
{
  if (OB_ISNULL(stat_.pre_cal_expr_handler_)) {
    LOG_WARN("pre-calcuable expression handler has not been initalized.");
  } else {
    stat_.pre_cal_expr_handler_->inc_ref_cnt();
  }
}

void ObPhysicalPlan::dec_pre_expr_ref_count()
{
  if (OB_ISNULL(stat_.pre_cal_expr_handler_)) {
  } else {
    PreCalcExprHandler* handler = stat_.pre_cal_expr_handler_;
    int64_t ref_cnt = handler->dec_ref_cnt();
    if (ref_cnt == 0) {
      common::ObIAllocator* alloc = stat_.pre_cal_expr_handler_->pc_alloc_;
      stat_.pre_cal_expr_handler_->~PreCalcExprHandler();
      alloc->free(stat_.pre_cal_expr_handler_);
      stat_.pre_cal_expr_handler_ = NULL;
    }
  }
}

void ObPhysicalPlan::set_pre_calc_expr_handler(PreCalcExprHandler* handler)
{
  stat_.pre_cal_expr_handler_ = handler;
}

PreCalcExprHandler* ObPhysicalPlan::get_pre_calc_expr_handler()
{
  return stat_.pre_cal_expr_handler_;
}

}  // namespace sql
}  // namespace oceanbase
