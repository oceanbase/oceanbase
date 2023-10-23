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
#include "sql/engine/ob_phy_operator_type.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_operator_factory.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_truncated_string.h"
#include "sql/spm/ob_spm_evolution_plan.h"
#include "sql/engine/ob_exec_feedback_info.h"

namespace oceanbase
{
using namespace common;
using namespace common::serialization;
using namespace share::schema;
using namespace lib;
namespace sql
{

ObPhysicalPlan::ObPhysicalPlan(MemoryContext &mem_context /* = CURRENT_CONTEXT */)
  : ObPlanCacheObject(ObLibCacheNameSpace::NS_CRSR, mem_context),
    phy_hint_(),
    root_op_spec_(NULL),
    param_count_(0),
    signature_(0),
    field_columns_(mem_context->get_arena_allocator()),
    param_columns_(mem_context->get_arena_allocator()),
    returning_param_columns_(mem_context->get_arena_allocator()),
    autoinc_params_(allocator_),
    stmt_need_privs_(allocator_),
    stmt_ora_need_privs_(allocator_),
    audit_units_(allocator_),
    vars_(allocator_),
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
    gtt_session_scope_ids_(allocator_),
    gtt_trans_scope_ids_(allocator_),
    concurrent_num_(0),
    max_concurrent_num_(ObMaxConcurrentParam::UNLIMITED),
    table_locations_(allocator_),
    das_table_locations_(allocator_),
    row_param_map_(allocator_),
    is_update_uniq_index_(false),
    contain_index_location_(false),
    base_constraints_(allocator_),
    strict_constrinats_(allocator_),
    non_strict_constrinats_(allocator_),
    expr_frame_info_(allocator_),
    stat_(),
    op_stats_(),
    need_drive_dml_query_(false),
    tx_id_(-1),
    tm_sessid_(-1),
    var_init_exprs_(allocator_),
    is_returning_(false),
    is_late_materialized_(false),
    is_dep_base_table_(false),
    is_insert_select_(false),
    is_plain_insert_(false),
    flashback_query_items_(allocator_),
    contain_paramed_column_field_(false),
    first_array_index_(OB_INVALID_INDEX),
    need_consistent_snapshot_(true),
    is_batched_multi_stmt_(false),
    encrypt_meta_array_(allocator_),
    is_new_engine_(false),
    use_pdml_(false),
    use_temp_table_(false),
    has_link_table_(false),
    has_link_sfd_(false),
    need_serial_exec_(false),
    temp_sql_can_prepare_(false),
    is_need_trans_(false),
    batch_size_(0),
    contain_pl_udf_or_trigger_(false),
    ddl_schema_version_(0),
    ddl_table_id_(0),
    ddl_execution_id_(-1),
    ddl_task_id_(0),
    is_packed_(false),
    has_instead_of_trigger_(false),
    min_cluster_version_(GET_MIN_CLUSTER_VERSION()),
    need_record_plan_info_(false),
    enable_append_(false),
    append_table_id_(0),
    logical_plan_(),
    is_enable_px_fast_reclaim_(false)
{
}

ObPhysicalPlan::~ObPhysicalPlan()
{
  destroy();
}

void ObPhysicalPlan::reset()
{
  ObPlanCacheObject::reset();
  phy_hint_.reset();
  root_op_spec_ = NULL;
  param_count_ = 0;
  signature_ = 0;
  field_columns_.reset();
  param_columns_.reset();
  returning_param_columns_.reset();
  autoinc_params_.reset();
  stmt_need_privs_.reset();
  stmt_ora_need_privs_.reset();
  audit_units_.reset();
  vars_.reset();
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
  gtt_session_scope_ids_.reset();
  gtt_trans_scope_ids_.reset();
  concurrent_num_ = 0;
  max_concurrent_num_ = ObMaxConcurrentParam::UNLIMITED;
  is_update_uniq_index_ = false;
  contain_index_location_ = false;
  ObPlanCacheObject::reset();
  is_returning_ = false;
  is_late_materialized_ = false;
  is_dep_base_table_ = false;
  is_insert_select_ = false;
  is_plain_insert_ = false;
  base_constraints_.reset();
  strict_constrinats_.reset();
  non_strict_constrinats_.reset();
  flashback_query_items_.reset();
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
  has_link_sfd_ = false;
  encrypt_meta_array_.reset();
  need_serial_exec_ = false;
  batch_size_ = 0;
  contain_pl_udf_or_trigger_ = false;
  is_packed_ = false;
  has_instead_of_trigger_ = false;
  enable_append_ = false;
  append_table_id_ = 0;
  stat_.expected_worker_map_.destroy();
  stat_.minimal_worker_map_.destroy();
  tx_id_ = -1;
  tm_sessid_ = -1;
  var_init_exprs_.reset();
  need_record_plan_info_ = false;
  logical_plan_.reset();
  is_enable_px_fast_reclaim_ = false;
}

void ObPhysicalPlan::destroy()
{
#ifndef NDEBUG
  bit_set_.reset();
#endif
  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
  stat_.expected_worker_map_.destroy();
  stat_.minimal_worker_map_.destroy();
}

int ObPhysicalPlan::copy_common_info(ObPhysicalPlan &src)
{
  int ret = OB_SUCCESS;
  //copy regexp_op_count_
  set_regexp_op_count(src.regexp_op_count_);
  set_like_op_count(src.like_op_count_);
  set_px_exchange_out_op_count(src.px_exchange_out_op_count_);
  //copy others
  set_fetch_cur_time(src.get_fetch_cur_time());
  set_stmt_type(src.get_stmt_type());
  set_literal_stmt_type(src.get_literal_stmt_type());
  //copy plan_id/hint/privs
  object_id_ = src.object_id_;
  min_cluster_version_ = src.min_cluster_version_;
  if (OB_FAIL(set_phy_plan_hint(src.get_phy_plan_hint()))) {
    LOG_WARN("Failed to copy query hint", K(ret));
  } else if (OB_FAIL(set_stmt_need_privs(src.get_stmt_need_privs()))) {
    LOG_WARN("Failed to deep copy", K(src.get_stmt_need_privs()), K(ret));
  } else { }//do nothing
  return ret;
}

int ObPhysicalPlan::set_vars(const common::ObIArray<ObVarInfo> &vars)
{
  int ret = OB_SUCCESS;
  int64_t N = vars.count();
  if (N > 0 && OB_FAIL(vars_.reserve(N))) {
    OB_LOG(WARN, "fail to reserve vars", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObVarInfo &var_info = vars.at(i);
    ObVarInfo clone_var_info;
    if (OB_FAIL(var_info.deep_copy(allocator_, clone_var_info))) {
      LOG_WARN("fail to deep copy var info", K(ret), K(var_info));
    } else if (OB_FAIL(vars_.push_back(clone_var_info))) {
      // deep_copy时只写了ObString对象，ObString对象可以在allocator_析构的时候完全释放掉，因此这里不用调ObString的析构函数
      LOG_WARN("fail to push back vars", K(ret), K(clone_var_info));
    }
  }
  return ret;
}

int ObPhysicalPlan::init_params_info_str()
{
  int ret = common::OB_SUCCESS;
  int64_t N = params_info_.count();
  int64_t buf_len = N * ObParamInfo::MAX_STR_DES_LEN + 1;
  int64_t pos = 0;
  char *buf = (char *)allocator_.alloc(buf_len);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_PC_LOG(WARN, "fail to alloc memory for param info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (N - 1 != i) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{%d,%d,%d,%d,%d},",
                                    params_info_.at(i).flag_.need_to_check_type_,
                                    params_info_.at(i).flag_.need_to_check_bool_value_,
                                    params_info_.at(i).flag_.expected_bool_value_,
                                    params_info_.at(i).scale_,
                                    params_info_.at(i).type_))) {
          SQL_PC_LOG(WARN, "fail to buff_print param info", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{%d,%d,%d,%d,%d}",
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

int ObPhysicalPlan::set_field_columns(const ColumnsFieldArray &fields)
{
  int ret = OB_SUCCESS;
  ObField field;
  WITH_CONTEXT(mem_context_) {
    int64_t N = fields.count();
    if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
      OB_LOG(WARN, "fail to reserve field column", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObField &ofield = fields.at(i);
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

int ObPhysicalPlan::set_param_fields(const common::ParamsFieldArray &params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  WITH_CONTEXT(mem_context_) {
    if(N > 0 && OB_FAIL(param_columns_.reserve(N))) {
      LOG_WARN("failed to reserved param field", K(ret));
    }
    ObField tmp_field;
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObField &param_field = params.at(i);
      if (OB_FAIL(tmp_field.deep_copy(param_field, &allocator_))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(param_columns_.push_back(tmp_field))) {
        LOG_WARN("push back field columns failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::set_returning_param_fields(const common::ParamsFieldArray &params)
{
  int ret = OB_SUCCESS;
  int64_t N = params.count();
  WITH_CONTEXT(mem_context_) {
    if(N > 0 && OB_FAIL(returning_param_columns_.reserve(N))) {
      LOG_WARN("failed to reserved returning param field", K(ret));
    }
    ObField tmp_field;
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObField &param_field = params.at(i);
      if (OB_FAIL(tmp_field.deep_copy(param_field, &allocator_))) {
        LOG_WARN("deep copy field failed", K(ret));
      } else if (OB_FAIL(returning_param_columns_.push_back(tmp_field))) {
        LOG_WARN("push back field columns failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::set_autoinc_params(const ObIArray<share::AutoincParam> &autoinc_params)
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

int ObPhysicalPlan::set_audit_units(const common::ObIArray<ObAuditUnit>& audit_units)
{
  return audit_units_.assign(audit_units);
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

bool ObPhysicalPlan::is_stmt_modify_trans() const
{
  return is_sfu_ || ObStmt::is_dml_write_stmt(stmt_type_);
}

int ObPhysicalPlan::init_operator_stats()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_stats_.init(&allocator_, next_phy_operator_id_))) {
    LOG_WARN("fail to init op_stats", K(ret));
  }
  return ret;
}

void ObPhysicalPlan::update_plan_stat(const ObAuditRecordData &record,
                                      const bool is_first,
                                      const bool is_evolution,
                                      const ObIArray<ObTableRowCount> *table_row_count_list)
{
  const int64_t current_time = ObClockGenerator::getClock();
  int64_t execute_count = 0;
  if (record.is_timeout()) {
    ATOMIC_INC(&(stat_.timeout_count_));
    ATOMIC_AAF(&(stat_.total_process_time_), record.get_process_time());
  } else if (!GCONF.enable_perf_event) { // short route
    ATOMIC_AAF(&(stat_.elapsed_time_), record.get_elapsed_time());
    ATOMIC_AAF(&(stat_.cpu_time_), record.get_elapsed_time() - record.exec_record_.wait_time_end_
                                   - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));

    if (is_first) {
      ATOMIC_STORE(&(stat_.hit_count_), 0);
    } else {
      ATOMIC_INC(&(stat_.hit_count_));
    }
  } else { // long route stat begin
    execute_count = ATOMIC_AAF(&stat_.execute_times_, 1);
    ATOMIC_AAF(&(stat_.total_process_time_), record.get_process_time());
    ATOMIC_AAF(&(stat_.disk_reads_), record.exec_record_.get_io_read_count());
    ATOMIC_AAF(&(stat_.direct_writes_), record.exec_record_.get_io_write_count());
    ATOMIC_AAF(&(stat_.buffer_gets_), 2 * record.exec_record_.get_row_cache_hit()
                                      + 2 * record.exec_record_.get_fuse_row_cache_hit()
                                      + 2 * record.exec_record_.get_bloom_filter_filts()
                                      + record.exec_record_.get_block_cache_hit()
                                      + record.exec_record_.get_io_read_count());
    ATOMIC_AAF(&(stat_.application_wait_time_), record.exec_record_.get_application_time());
    ATOMIC_AAF(&(stat_.concurrency_wait_time_), record.exec_record_.get_concurrency_time());
    ATOMIC_AAF(&(stat_.user_io_wait_time_), record.exec_record_.get_user_io_time());
    ATOMIC_AAF(&(stat_.rows_processed_), record.return_rows_ + record.affected_rows_);
    ATOMIC_AAF(&(stat_.elapsed_time_), record.get_elapsed_time());
    ATOMIC_AAF(&(stat_.cpu_time_), record.get_elapsed_time() - record.exec_record_.wait_time_end_
                                   - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
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
      int64_t max_index = std::min(access_table_num,
                                   std::min(table_row_count_list->count(),
                                   OB_MAX_TABLE_NUM_PER_STMT));
      if (is_first) {
        for (int64_t i = 0; i < max_index; ++i) {
          ATOMIC_STORE(&(stat_.table_row_count_first_exec_[i].op_id_),
                       table_row_count_list->at(i).op_id_);
          ATOMIC_STORE(&(stat_.table_row_count_first_exec_[i].row_count_),
                       table_row_count_list->at(i).row_count_);
          LOG_DEBUG("first add row stat", K(table_row_count_list->at(i)));
        } // for end
      } else if (record.get_elapsed_time() > SLOW_QUERY_TIME_FOR_PLAN_EXPIRE) {
        for (int64_t i = 0; !is_expired() && i < max_index; ++i) {
          for (int64_t j = 0; !is_expired() && j < max_index; ++j) {
            // 一些场景比如并行执行时，不同次执行表的行信息存储的顺序可能不同
            if (table_row_count_list->at(i).op_id_ ==
                stat_.table_row_count_first_exec_[j].op_id_) {
              int64_t first_exec_row_count = ATOMIC_LOAD(&stat_.table_row_count_first_exec_[j]
                                                               .row_count_);
              if (first_exec_row_count == -1) {
                // do nothing
              } else if (check_if_is_expired(first_exec_row_count,
                                             table_row_count_list->at(i).row_count_)) {
                set_is_expired(true);
                LOG_INFO("plan is expired", K(first_exec_row_count),
                                            K(table_row_count_list->at(i)),
                                            "current_elapsed_time", record.get_elapsed_time(),
                                            "plan_stat", stat_);
              }
            }
          } // for max_index end
        } // for max_index end
      }
    }
    ATOMIC_STORE(&(stat_.last_active_time_), current_time);
    if (ATOMIC_LOAD(&stat_.is_evolution_)) { //for spm
      ATOMIC_INC(&(stat_.evolution_stat_.executions_));
      // ATOMIC_AAF(&(stat_.evolution_stat_.cpu_time_),
      //            record.get_elapsed_time() - record.exec_record_.wait_time_end_
      //            - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
      ATOMIC_AAF(&(stat_.evolution_stat_.cpu_time_), record.exec_timestamp_.executor_t_);
      ATOMIC_AAF(&(stat_.evolution_stat_.elapsed_time_), record.get_elapsed_time());
      ATOMIC_STORE(&(stat_.evolution_stat_.last_exec_ts_), record.exec_timestamp_.executor_end_ts_);
    }
    if (stat_.is_bind_sensitive_ && execute_count > 0) {
      int64_t pos = execute_count % ObPlanStat::MAX_SCAN_STAT_SIZE;
      ATOMIC_STORE(&(stat_.table_scan_stat_[pos].query_range_row_count_),
                   record.table_scan_stat_.query_range_row_count_);
      ATOMIC_STORE(&(stat_.table_scan_stat_[pos].indexback_row_count_),
                   record.table_scan_stat_.indexback_row_count_);
      ATOMIC_STORE(&(stat_.table_scan_stat_[pos].output_row_count_),
                   record.table_scan_stat_.output_row_count_);
    }
  } // long route stat ends

  if (!is_expired() && stat_.enable_plan_expiration_) {
    if (record.is_timeout() || record.status_ == OB_SESSION_KILLED) {
      set_is_expired(true);
      LOG_INFO("query plan is expired due to execution timeout", K(stat_));
    } else if (is_first) {
      ATOMIC_STORE(&(stat_.sample_times_), 0);
      ATOMIC_STORE(&(stat_.first_exec_row_count_),
                    record.exec_record_.get_memstore_read_row_count() +
                    record.exec_record_.get_ssstore_read_row_count());
      ATOMIC_STORE(&(stat_.first_exec_usec_), record.get_elapsed_time() - record.exec_record_.wait_time_end_
                                  - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
    } else if (0 == stat_.sample_times_) { // first sample query
      ATOMIC_INC(&(stat_.sample_times_));
      ATOMIC_STORE(&(stat_.sample_exec_row_count_),
                   record.exec_record_.get_memstore_read_row_count() +
                   record.exec_record_.get_ssstore_read_row_count());
      ATOMIC_STORE(&(stat_.sample_exec_usec_), record.get_elapsed_time() - record.exec_record_.wait_time_end_
                                   - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
    } else {
      int64_t sample_count = ATOMIC_AAF(&(stat_.sample_times_), 1);
      int64_t sample_exec_row_count = ATOMIC_AAF(&(stat_.sample_exec_row_count_),
                                        record.exec_record_.get_memstore_read_row_count() +
                                        record.exec_record_.get_ssstore_read_row_count());
      int64_t sample_exec_usec = ATOMIC_AAF(&(stat_.sample_exec_usec_),
                                    record.get_elapsed_time() - record.exec_record_.wait_time_end_
                                    - (record.exec_timestamp_.run_ts_ - record.exec_timestamp_.receive_ts_));
      if (sample_count < SLOW_QUERY_SAMPLE_SIZE) {
        // do nothing when query execution samples are not enough
      } else {
        if (stat_.cpu_time_ <= SLOW_QUERY_TIME_FOR_PLAN_EXPIRE * stat_.execute_times_) {
        // do nothing for fast query
        } else if (is_plan_unstable(sample_count, sample_exec_row_count, sample_exec_usec)) {
          set_is_expired(true);
        }
        ATOMIC_STORE(&(stat_.sample_times_), 0);
      }
    }
  }
}

bool ObPhysicalPlan::is_plan_unstable(const int64_t sample_count,
                                      const int64_t sample_exec_row_count,
                                      const int64_t sample_exec_usec)
{
  bool bret = false;
  if (sample_exec_usec <= SLOW_QUERY_TIME_FOR_PLAN_EXPIRE * sample_count) {
    // sample query is fast query in the average
  } else if (OB_PHY_PLAN_LOCAL == plan_type_) {
    int64_t first_query_range_rows = ATOMIC_LOAD(&stat_.first_exec_row_count_);
    if (sample_exec_row_count <= SLOW_QUERY_ROW_COUNT_THRESOLD * sample_count) {
      // the sample query does not accesses too many rows in the average
    } else if (sample_exec_row_count / sample_count > first_query_range_rows * 10) {
      // the average sample query range row count increases great
      bret = true;
      LOG_INFO("local query plan is expired due to unstable performance",
               K(bret), K(stat_.execute_times_),
               K(first_query_range_rows), K(sample_exec_row_count), K(sample_count));
    }
  } else if ( OB_PHY_PLAN_DISTRIBUTED == plan_type_) {
    int64_t first_exec_usec = ATOMIC_LOAD(&stat_.first_exec_usec_);
    if (sample_exec_usec / sample_count > first_exec_usec * 2) {
      // the average sample query execute time increases great
      bret = true;
      LOG_INFO("distribute query plan is expired due to unstable performance",
               K(bret), K(stat_.execute_times_), K(first_exec_usec),
               K(sample_exec_usec), K(sample_count));
    }
  } else {
    // do nothing
  }
  return bret;
}

int64_t ObPhysicalPlan::get_evo_perf() const {
  int64_t v = 0;
  if (0 == stat_.evolution_stat_.executions_) {
    v = 0;
  } else {
    v = stat_.evolution_stat_.cpu_time_/ stat_.evolution_stat_.executions_;
  }

  return v;
}

/**
 * 目前采3个指标来淘汰计划，只有同时满足这3个条件，才会触发计划的淘汰：
 *     1. 当前行数超过阈值（100行）
 *     2. 执行时间超过阈值（5ms）
 *     3. 表扫描行数与原扫描行数比值超过阈值（2倍）
 *
 *     设置当前行数阈值的原因是因为表扫描函数并不能保证递增，在频繁插入删除的情况下，原来的表扫描函数
 *     可能会保持在一个较低的值，这时候计划淘汰会很频繁，
 *     设置一个阈值的可以在很大程度上缓解计划淘汰的频率
 */
inline bool ObPhysicalPlan::check_if_is_expired(const int64_t first_exec_row_count,
                                                const int64_t current_row_count) const
{
  bool ret_bool = false;
  if (current_row_count <= EXPIRED_PLAN_TABLE_ROW_THRESHOLD) { // 100 行
    ret_bool = false;
  } else {
    ret_bool =  ((first_exec_row_count == 0  && current_row_count > 0)
                 || (first_exec_row_count > 0 && current_row_count / first_exec_row_count > TABLE_ROW_CHANGE_THRESHOLD));
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
    while(OB_SUCC(ret) && false == is_succ) {
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

OB_SERIALIZE_MEMBER(FlashBackQueryItem,
                    table_id_,
                    time_val_);

//因为还没看到远程执行对force_trace_log的处理，所以暂时不序列化
OB_SERIALIZE_MEMBER(ObPhysicalPlan,
                    tenant_schema_version_, //该字段执行期没被使用
                    phy_hint_.query_timeout_,
                    phy_hint_.read_consistency_,
                    is_sfu_,
                    dependency_tables_,
                    param_count_,
                    plan_type_,
                    signature_,
                    stmt_type_,
                    regexp_op_count_,
                    literal_stmt_type_,
                    like_op_count_,
                    is_ignore_stmt_,
                    object_id_,
                    stat_.sql_id_,
                    is_contain_inner_table_,
                    is_update_uniq_index_,
                    dummy_string_,
                    is_returning_,
                    location_type_,
                    use_px_,
                    vars_,
                    px_dop_,
                    has_nested_sql_,
                    flashback_query_items_,
                    stat_.enable_early_lock_release_,
                    encrypt_meta_array_,
                    use_pdml_,
                    is_new_engine_,
                    use_temp_table_,
                    batch_size_,
                    need_drive_dml_query_,
                    is_need_trans_,
                    contain_oracle_trx_level_temporary_table_,
                    ddl_schema_version_,
                    ddl_table_id_,
                    phy_hint_.monitor_,
                    need_serial_exec_,
                    contain_pl_udf_or_trigger_,
                    is_packed_,
                    has_instead_of_trigger_,
                    is_plain_insert_,
                    ddl_execution_id_,
                    ddl_task_id_,
                    stat_.plan_id_,
                    min_cluster_version_,
                    need_record_plan_info_,
                    enable_append_,
                    append_table_id_,
                    is_enable_px_fast_reclaim_,
                    gtt_session_scope_ids_,
                    gtt_trans_scope_ids_);

int ObPhysicalPlan::set_table_locations(const ObTablePartitionInfoArray &infos,
                                        ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  table_locations_.reset();
  das_table_locations_.reset();
  if (OB_FAIL(table_locations_.prepare_allocate_and_keep_count(infos.count(),
                                                 allocator_))) {
    LOG_WARN("fail to init table location count", K(ret));
  } else if (OB_FAIL(das_table_locations_.prepare_allocate_and_keep_count(infos.count(),
                                                            allocator_))) {
    LOG_WARN("fail to init das table location count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
    ObTableLocation &tl = infos.at(i)->get_table_location();
    const ObTableSchema *table_schema = nullptr;
    if (tl.use_das()) {
      if (OB_FAIL(das_table_locations_.push_back(tl))) {
        LOG_WARN("fail to push das table location", K(ret), K(i));
      }
    } else if (OB_FAIL(table_locations_.push_back(tl))) {
      LOG_WARN("fail to push table location", K(ret), K(i));
    } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), tl.get_ref_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(tl.get_ref_table_id()));
    } else {
      contain_index_location_ |= table_schema->is_index_table();
    }
    LOG_DEBUG("set table location", K(tl), K(tl.use_das()));
  }

  return ret;
}

int ObPhysicalPlan::set_location_constraints(const ObIArray<LocationConstraint> &base_constraints,
                                             const ObIArray<ObPwjConstraint *> &strict_constraints,
                                             const ObIArray<ObPwjConstraint *> &non_strict_constraints,
                                             const ObIArray<ObDupTabConstraint> &dup_table_replica_cons)
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
      } else { /*do nothing*/ }
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
      ObPwjConstraint *cur_cons;
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
            } else { /*do nothing*/ }
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
      ObPwjConstraint *cur_cons;
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
            } else { /*do nothing*/ }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && dup_table_replica_cons.count() > 0) {
    dup_table_replica_cons_.reset();
    dup_table_replica_cons_.set_allocator(&allocator_);
    if (OB_FAIL(dup_table_replica_cons_.init(dup_table_replica_cons.count()))) {
      LOG_WARN("failed to init duplicate table constraints", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dup_table_replica_cons.count(); ++i) {
        if(OB_FAIL(dup_table_replica_cons_.push_back(dup_table_replica_cons.at(i)))) {
          LOG_WARN("failed to assign element", K(ret), K(dup_table_replica_cons.at(i)));
        } else { /*do nothing*/ }
      }
    }
  }

  if (OB_FAIL(ret)) {
    base_constraints_.reset();
    strict_constrinats_.reset();
    non_strict_constrinats_.reset();
    dup_table_replica_cons_.reset();
  } else {
    LOG_TRACE("deep copied location constraints", K(base_constraints_), K(strict_constrinats_),
                            K(non_strict_constrinats_), K(dup_table_replica_cons_));
  }

  return ret;
}

bool ObPhysicalPlan::has_same_location_constraints(const ObPhysicalPlan &r) const
{
  bool is_same = true;
  const ObIArray<LocationConstraint>& l_base_cons = get_base_constraints();
  const ObIArray<LocationConstraint>& r_base_cons = r.get_base_constraints();
  const ObIArray<ObPlanPwjConstraint>& l_non_strict_cons = get_non_strict_constraints();
  const ObIArray<ObPlanPwjConstraint>& r_non_strict_cons = r.get_non_strict_constraints();
  const ObIArray<ObPlanPwjConstraint>& l_strict_cons = get_strict_constraints();
  const ObIArray<ObPlanPwjConstraint>& r_strict_cons = r.get_strict_constraints();
  const ObIArray<ObDupTabConstraint>& l_dup_rep_cons = get_dup_table_replica_constraints();
  const ObIArray<ObDupTabConstraint>& r_dup_rep_cons = r.get_dup_table_replica_constraints();
  if (l_base_cons.count() != r_base_cons.count() ||
      l_strict_cons.count() != r_strict_cons.count() ||
      l_non_strict_cons.count() != r_non_strict_cons.count()||
      l_dup_rep_cons.count() != r_dup_rep_cons.count()) {
    is_same = false;
  } else {
    for (int64_t i = 0; is_same && i < l_base_cons.count(); i++) {
      is_same = is_same && (l_base_cons.at(i) == r_base_cons.at(i));
    }
    for (int64_t i = 0; is_same && i < l_strict_cons.count(); i++) {
      if (l_strict_cons.at(i).count() != r_strict_cons.at(i).count()) {
        is_same = false;
      } else {
        for (int64_t j = 0; is_same && j < l_strict_cons.at(i).count(); j++) {
          is_same = (l_strict_cons.at(i).at(j) == (r_strict_cons.at(i)).at(j));
        }
      }
    }
    for (int64_t i = 0; is_same && i < l_non_strict_cons.count(); i++) {
      if (l_non_strict_cons.at(i).count() != r_non_strict_cons.at(i).count()) {
        is_same = false;
      } else {
        for (int64_t j = 0; is_same && j < l_non_strict_cons.at(i).count(); j++) {
          is_same = (l_non_strict_cons.at(i).at(j) == r_non_strict_cons.at(i).at(j));
        }
      }
    }
    for(int64_t i = 0; is_same && i < l_dup_rep_cons.count(); i++) {
      is_same = is_same && (l_dup_rep_cons.at(i) == r_dup_rep_cons.at(i));
    }
  }
  return is_same;
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


int ObPhysicalPlan::alloc_op_spec(const ObPhyOperatorType type,
                                  const int64_t child_cnt,
                                  ObOpSpec *&op,
                                  const uint64_t op_id)
{
  int ret = OB_SUCCESS;
  UNUSED(op_id);
  if (type >= PHY_END || child_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), K(child_cnt));
  } else if (OB_FAIL(ObOperatorFactory::alloc_op_spec(
              allocator_, type, child_cnt, op))) {
    LOG_WARN("allocate operator spec failed", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL operator spec returned", K(ret));
  } else {
    // 这里是直接将log operator id赋值给 spec
    uint32_t tmp_op_id = UINT32_MAX;
    if (OB_INVALID_ID != op_id) {
      tmp_op_id = op_id;
    } else {
      tmp_op_id = next_phy_operator_id_++;
    }
    op->id_ = tmp_op_id;
    op->plan_ = this;
    op->max_batch_size_ = (ObOperatorFactory::is_vectorized(type)) ? batch_size_ : 0;
  }
  return ret;
}

int ObPhysicalPlan::get_encrypt_meta(const uint64_t table_id,
                                     ObIArray<transaction::ObEncryptMetaCache> &metas,
                                     const ObIArray<transaction::ObEncryptMetaCache> *&ret_ptr) const
{
  int ret = OB_SUCCESS;
  metas.reset();
  ARRAY_FOREACH_N(encrypt_meta_array_, i, cnt) {
    if (encrypt_meta_array_.at(i).table_id_ == table_id) {
      ret = metas.push_back(encrypt_meta_array_.at(i));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get table encrypt meta fail", KR(ret), K(table_id));
  } else {
    ret_ptr = &metas;
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
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "pre-calcuable expression handler has not been initalized.");
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

int ObPhysicalPlan::before_cache_evicted()
{
  int ret = OB_SUCCESS;
  dec_pre_expr_ref_count();
  return ret;
}

void ObPhysicalPlan::set_pre_calc_expr_handler(PreCalcExprHandler* handler)
{
  stat_.pre_cal_expr_handler_ = handler;
}

PreCalcExprHandler* ObPhysicalPlan::get_pre_calc_expr_handler()
{
  return stat_.pre_cal_expr_handler_;
}

void ObPhysicalPlan::calc_whether_need_trans()
{
  // 这只是一种实现方式，通过看这个plan是否有table参与来决定是否要走事务接口
  bool bool_ret = false;
  if (OB_UNLIKELY(stmt::T_EXPLAIN == stmt_type_)) {
    // false
  } else if (get_dependency_table_size() <= 0) {
    // false
  } else {
    for (int64_t i = 0; i < get_dependency_table_size(); ++i) {
      if (get_dependency_table().at(i).is_base_table()) {
        const share::schema::ObSchemaObjVersion &table_version = get_dependency_table().at(i);
        uint64_t table_id = table_version.get_object_id();
        if(table_version.is_existed()
            && (!common::is_virtual_table(table_id) || share::is_oracle_mapping_real_virtual_table(table_id))
            && !common::is_cte_table(table_id)) {
          bool_ret = true;
          break;
        }
      }
    }
  }
  // mysql允许select udf中有dml，需要保证select 整体原子性
  if (!bool_ret && contain_pl_udf_or_trigger() && lib::is_mysql_mode() && stmt::T_EXPLAIN != stmt_type_) {
    bool_ret = true;
  }
  is_need_trans_ = bool_ret;
}

int ObPhysicalPlan::set_expected_worker_map(const common::hash::ObHashMap<ObAddr, int64_t> &c)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign_worker_map(stat_.expected_worker_map_, c))) {
    LOG_WARN("set expected worker map failed", K(ret));
  }
  return ret;
}
const ObPlanStat::AddrMap& ObPhysicalPlan:: get_expected_worker_map() const
{
  return stat_.expected_worker_map_;
}

int ObPhysicalPlan::set_minimal_worker_map(const common::hash::ObHashMap<ObAddr, int64_t> &c)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign_worker_map(stat_.minimal_worker_map_, c))) {
    LOG_WARN("set minimal worker map failed", K(ret));
  }
  return ret;
}

int ObPhysicalPlan::assign_worker_map(ObPlanStat::AddrMap &worker_map, const common::hash::ObHashMap<ObAddr, int64_t> &c)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "WorkerMap");
  if (worker_map.created()) {
    worker_map.clear();
  } else if (OB_FAIL(worker_map.create(common::hash::cal_next_prime(100), attr, attr))){
    LOG_WARN("create hash map failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    for (common::hash::ObHashMap<ObAddr, int64_t>::const_iterator it = c.begin();
        OB_SUCC(ret) && it != c.end(); ++it) {
      if (OB_FAIL(worker_map.set_refactored(it->first, it->second))){
        SQL_PC_LOG(WARN, "set refactored failed", K(ret), K(it->first), K(it->second));
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::update_cache_obj_stat(ObILibCacheCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObPlanCacheCtx &pc_ctx = static_cast<ObPlanCacheCtx&>(ctx);
  if (OB_ISNULL(pc_ctx.sql_ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "session is null", K(ret));
  } else  {
    stat_.plan_id_ = get_plan_id();
    stat_.plan_hash_value_ = get_signature();
    stat_.gen_time_ = ObTimeUtility::current_time();
    stat_.schema_version_ = get_tenant_schema_version();
    stat_.last_active_time_ = stat_.gen_time_;
    stat_.hit_count_ = 0;
    stat_.mem_used_ = get_mem_size();
    stat_.slow_count_ = 0;
    stat_.slowest_exec_time_ = 0;
    stat_.slowest_exec_usec_ = 0;
    if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
      ObTruncatedString trunc_stmt(pc_ctx.raw_sql_, OB_MAX_SQL_LENGTH);
      if (OB_FAIL(ob_write_string(get_allocator(),
                                  trunc_stmt.string(),
                                  stat_.stmt_))) {
        SQL_PC_LOG(WARN, "fail to set truncate string", K(ret));
      }
      stat_.ps_stmt_id_ = pc_ctx.fp_result_.pc_key_.key_id_;
    } else {
      ObTruncatedString trunc_stmt(pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_, OB_MAX_SQL_LENGTH);
      if (OB_FAIL(ob_write_string(get_allocator(),
                                  trunc_stmt.string(),
                                  stat_.stmt_))) {
        SQL_PC_LOG(WARN, "fail to set truncate string", K(ret));
      }
    }
    stat_.large_querys_= 0;
    stat_.delayed_large_querys_= 0;
    stat_.delayed_px_querys_= 0;
    stat_.outline_version_ = get_outline_state().outline_version_.version_;
    stat_.outline_id_ = get_outline_state().outline_version_.object_id_;
    // Truncate the raw sql to avoid the plan memory being too large due to the long raw sql
    ObTruncatedString trunc_raw_sql(pc_ctx.raw_sql_, OB_MAX_SQL_LENGTH);
    if (OB_FAIL(pc_ctx.get_not_param_info_str(get_allocator(), stat_.sp_info_str_))) {
      SQL_PC_LOG(WARN, "fail to get special param info string", K(ret));
    } else if (OB_FAIL(ob_write_string(get_allocator(),
                                       pc_ctx.fp_result_.pc_key_.sys_vars_str_,
                                       stat_.sys_vars_str_))) {
      SQL_PC_LOG(DEBUG, "succeed to add plan statistic", "plan_id", get_plan_id(), K(ret));
    } else if (OB_FAIL(ob_write_string(get_allocator(),
                                       pc_ctx.fp_result_.pc_key_.config_str_,
                                       stat_.config_str_))) {
      SQL_PC_LOG(DEBUG, "failed to add plan statistic", "plan_id", get_plan_id(), K(ret));
    } else if (OB_FAIL(init_params_info_str())) {
      SQL_PC_LOG(DEBUG, "fail to gen param info str", K(ret));
    } else if (OB_FAIL(ob_write_string(get_allocator(),
                                       trunc_raw_sql.string(),
                                       stat_.raw_sql_))) {
      SQL_PC_LOG(DEBUG, "fail to copy raw sql", "plan_id", get_plan_id(), K(ret));
    } else {
      stat_.sql_cs_type_ = pc_ctx.sql_ctx_.session_info_->get_local_collation_connection();
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (get_access_table_num() > 0) {
      if (OB_ISNULL(stat_.table_row_count_first_exec_
                    = static_cast<ObTableRowCount *>(
                    get_allocator().alloc(get_access_table_num() * sizeof(ObTableRowCount))))) {
        // @banliu.zyd: 这块内存存放计划涉及的表的行数，用于统计信息已经过期的计划的淘汰，分配失败时
        //              不报错，走原来不淘汰计划的逻辑
        LOG_WARN("allocate memory for table row count list failed", K(get_access_table_num()));
      } else {
        for (int64_t i = 0; i < get_access_table_num(); ++i) {
          stat_.table_row_count_first_exec_[i].op_id_ = OB_INVALID_ID;
          stat_.table_row_count_first_exec_[i].row_count_ = -1;
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (pc_ctx.tmp_table_names_.count() > 0) {
      LOG_DEBUG("set tmp table name str", K(pc_ctx.tmp_table_names_));
      stat_.sessid_ = pc_ctx.sql_ctx_.session_info_->get_sessid();\
      int64_t pos = 0;
      // fill temporary table name
      for (int64_t i = 0; OB_SUCC(ret) && i < pc_ctx.tmp_table_names_.count(); i++) {
        if (OB_ISNULL(stat_.plan_tmp_tbl_name_str_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_DEBUG("null plan tmp tbl id str", K(ret));
        } else if (OB_FAIL(databuff_printf(stat_.plan_tmp_tbl_name_str_,
                                           ObPlanStat::STMT_MAX_LEN,
                                           pos,
                                           "%.*s%s",
                                           pc_ctx.tmp_table_names_.at(i).length(),
                                           pc_ctx.tmp_table_names_.at(i).ptr(),
                                           ((i == pc_ctx.tmp_table_names_.count() - 1) ? "" : ", ")))) {
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to write plan tmp tbl name info",
                     K(pc_ctx.tmp_table_names_.at(i)), K(i), K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        stat_.plan_tmp_tbl_name_str_[pos] = '\0';
        pos += 1;
        stat_.plan_tmp_tbl_name_str_len_ = static_cast<int32_t>(pos);
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::set_logical_plan(ObLogicalPlanRawData &logical_plan)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = (char*)allocator_.alloc(logical_plan.logical_plan_len_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    if (NULL != logical_plan_.logical_plan_) {
      allocator_.free(logical_plan_.logical_plan_);
    }
    MEMCPY(buf, logical_plan.logical_plan_, logical_plan.logical_plan_len_);
    logical_plan_ = logical_plan;
    logical_plan_.logical_plan_ = buf;
  }
  return ret;
}

int ObPhysicalPlan::set_feedback_info(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t plan_open_time = 0;
  ObSEArray<ObSqlPlanItem*, 4> plan_items;
  ObExecFeedbackInfo &feedback_info = ctx.get_feedback_info();
  const common::ObIArray<ObExecFeedbackNode> &feedback_nodes = feedback_info.get_feedback_nodes();
  if (OB_FAIL(logical_plan_.uncompress_logical_plan(ctx.get_allocator(), plan_items))) {
    LOG_WARN("failed to uncompress logical plan", K(ret));
  } else if (feedback_nodes.count() != plan_items.count()) {
    ret = OB_SUCCESS;
    LOG_WARN("unexpect feedback node count", K(ret));
  } else if (!feedback_nodes.empty()) {
    plan_open_time = feedback_nodes.at(0).op_open_time_;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < plan_items.count(); ++i) {
    const ObExecFeedbackNode &feedback_node = feedback_nodes.at(i);
    ObSqlPlanItem *plan_item = plan_items.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else if (feedback_node.op_id_ != plan_item->id_) {
       ret = OB_SUCCESS;
      LOG_WARN("unexpect feedback node info", K(ret));
    } else {
      int64_t real_cost = 0;
      if (0 != feedback_node.output_row_count_ &&
          0 != feedback_node.op_last_row_time_) {
        real_cost = feedback_node.op_last_row_time_ - plan_open_time;
      } else if (0 != feedback_node.op_close_time_) {
        real_cost = feedback_node.op_close_time_ - plan_open_time;
      }
      plan_item->real_cardinality_ = feedback_node.output_row_count_;
      plan_item->real_cost_ = real_cost;
      plan_item->cpu_cost_ = feedback_node.db_time_;
      plan_item->io_cost_ = feedback_node.block_time_;
      plan_item->search_columns_ = feedback_node.worker_count_;
    }
  }
  if (OB_SUCC(ret)) {
    ObLogicalPlanRawData new_logical_plan;
    if (OB_FAIL(new_logical_plan.compress_logical_plan(ctx.get_allocator(), plan_items))) {
      LOG_WARN("failed to compress logical plan", K(ret));
    } else if (OB_FAIL(set_logical_plan(new_logical_plan))) {
      LOG_WARN("failed to set logical plan", K(ret));
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
